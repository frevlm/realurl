#include "realurl/stream_pump_pool.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <sys/resource.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#ifdef __linux__
#include <sched.h>
#endif

namespace realurl
{

namespace
{

std::atomic<size_t> configured_size{ 0 };
std::atomic<size_t> configured_queue{ 0 };
std::atomic<long long> configured_idle_ms{ -1 };
constexpr long long kDefaultIdleMs = 60'000;

size_t environment_size(const char *name)
{
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0')
        return 0;
    char *end = nullptr;
    errno = 0;
    const long parsed = std::strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed <= 0)
        return 0;
    return static_cast<size_t>(parsed);
}

long long environment_idle_ms()
{
    const char *value = std::getenv("REALURL_PUMP_IDLE_MS");
    if (value == nullptr || *value == '\0')
        return kDefaultIdleMs;
    char *end = nullptr;
    errno = 0;
    const long long parsed = std::strtoll(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 0)
        return kDefaultIdleMs;
    return parsed;
}

/// Read a single unsigned value from a sysfs/procfs limit file. "max", a
/// negative value, a missing file, or an unparseable value all mean unlimited.
size_t read_limit(const char *path)
{
    std::ifstream input(path);
    std::string text;
    if (!(input >> text) || text == "max" || text[0] == '-')
        return 0;
    errno = 0;
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0')
        return 0;
    return static_cast<size_t>(std::min<unsigned long long>(parsed, std::numeric_limits<size_t>::max()));
}

size_t current_thread_count()
{
#ifdef __linux__
    std::ifstream input("/proc/self/status");
    std::string key;
    while (input >> key) {
        if (key == "Threads:") {
            size_t count = 0;
            if (input >> count)
                return count;
            return 0;
        }
        input.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
#endif
    return 0;
}

/// CPUs this process may actually run on: affinity mask intersected with the
/// cgroup CPU quota.  std::thread::hardware_concurrency() alone ignores both.
size_t available_cpus()
{
    size_t count = 0;
#ifdef __linux__
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int i = 0; i < CPU_SETSIZE; ++i) {
            if (CPU_ISSET(i, &set))
                ++count;
        }
    }

    std::ifstream cgroup_v2("/sys/fs/cgroup/cpu.max");
    std::string quota;
    unsigned long long period = 0;
    if (cgroup_v2 >> quota >> period) {
        if (quota != "max") {
            errno = 0;
            char *end = nullptr;
            const unsigned long long limit = std::strtoull(quota.c_str(), &end, 10);
            if (errno == 0 && end != quota.c_str() && *end == '\0' && limit > 0 && period > 0) {
                const size_t cpus = static_cast<size_t>((limit + period - 1) / period);
                count = count == 0 ? cpus : std::min(count, cpus);
            }
        }
    } else {
        const size_t limit = read_limit("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
        const size_t quota_period = read_limit("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
        if (limit > 0 && quota_period > 0) {
            const size_t cpus = (limit + quota_period - 1) / quota_period;
            count = count == 0 ? cpus : std::min(count, cpus);
        }
    }
#endif
    if (count == 0) {
        const unsigned hw = std::thread::hardware_concurrency();
        count = hw == 0 ? 1 : static_cast<size_t>(hw);
    }
    return std::max<size_t>(1, count);
}

/// Threads this process may still create.  pids.max (cgroup v2/v1) and
/// RLIMIT_NPROC are per-process/per-user; threads-max and pid_max are
/// kernel-wide fallbacks.  Half of what remains is reserved for io threads,
/// client loops, feeders, and application threads.
size_t thread_budget()
{
    size_t limit = 0;
    const auto take = [&limit](size_t value) {
        if (value > 0)
            limit = limit == 0 ? value : std::min(limit, value);
    };
    take(read_limit("/sys/fs/cgroup/pids.max"));
    take(read_limit("/sys/fs/cgroup/pids/pids.max"));

    rlimit processes{};
    if (getrlimit(RLIMIT_NPROC, &processes) == 0 && processes.rlim_cur != RLIM_INFINITY)
        take(static_cast<size_t>(processes.rlim_cur));

#ifdef __linux__
    take(read_limit("/proc/sys/kernel/threads-max"));
    take(read_limit("/proc/sys/kernel/pid_max"));
#endif

    if (limit == 0)
        return 0;

    const size_t current = current_thread_count();
    const size_t available = limit > current ? limit - current : 0;
    return std::max<size_t>(1, available / 2);
}

/// Every streaming response holds one client connection fd, so the fd budget is
/// also a ceiling on concurrent streams.  Keep a quarter for listeners,
/// upstream sockets, and housekeeping.
size_t fd_budget()
{
    rlimit files{};
    if (getrlimit(RLIMIT_NOFILE, &files) != 0 || files.rlim_cur == RLIM_INFINITY)
        return 0;
    const size_t limit = static_cast<size_t>(files.rlim_cur);
    return limit - limit / 4;
}

/// When RLIMIT_AS is set, every thread reserves its stack up front from the
/// address-space budget.
size_t address_space_budget()
{
#ifdef RLIMIT_AS
    rlimit address{};
    if (getrlimit(RLIMIT_AS, &address) != 0 || address.rlim_cur == RLIM_INFINITY)
        return 0;

    rlimit stack{};
    if (getrlimit(RLIMIT_STACK, &stack) != 0 || stack.rlim_cur == RLIM_INFINITY)
        return 0;

    const long minimum = sysconf(_SC_THREAD_STACK_MIN);
    const size_t stack_floor = minimum > 0 ? static_cast<size_t>(minimum) : 1;
    const size_t stack_bytes = std::max<size_t>(static_cast<size_t>(stack.rlim_cur), stack_floor);
    return std::max<size_t>(1, static_cast<size_t>(address.rlim_cur / 2) / stack_bytes);
#else
    return 0;
#endif
}

size_t auto_capacity()
{
    size_t capacity = 0;
    const auto take = [&capacity](size_t value) {
        if (value > 0)
            capacity = capacity == 0 ? value : std::min(capacity, value);
    };
    take(thread_budget());
    take(fd_budget());
    take(address_space_budget());
    if (capacity == 0)
        capacity = available_cpus();
    return std::max<size_t>(1, capacity);
}

} // namespace


StreamPumpPool &stream_pump_pool()
{
    return StreamPumpPool::instance();
}

StreamPumpPool &StreamPumpPool::instance()
{
    static StreamPumpPool pool;
    return pool;
}

void StreamPumpPool::configure(size_t count)
{
    configured_size.store(count, std::memory_order_relaxed);
}

void StreamPumpPool::configure_queue(size_t limit)
{
    configured_queue.store(limit, std::memory_order_relaxed);
}

void StreamPumpPool::configure_idle(std::chrono::milliseconds timeout)
{
    configured_idle_ms.store(std::max<long long>(0, timeout.count()), std::memory_order_relaxed);
}

StreamPumpPool::StreamPumpPool()
{
    size_t count = configured_size.load(std::memory_order_relaxed);
    if (count == 0)
        count = environment_size("REALURL_PUMP_THREADS");
    if (count == 0)
        count = auto_capacity();
    capacity_.store(std::max<size_t>(1, count), std::memory_order_relaxed);

    size_t queue = configured_queue.load(std::memory_order_relaxed);
    if (queue == 0)
        queue = environment_size("REALURL_PUMP_QUEUE");
    if (queue == 0) {
        // Running and queued streams both hold a client fd. The capacity
        // already spends part of the fd budget, so the default queue only
        // uses what is left, capped at one extra wave.
        const size_t capacity = capacity_.load(std::memory_order_relaxed);
        const size_t files = fd_budget();
        if (files == 0)
            queue = capacity;
        else if (files > capacity)
            queue = std::min(capacity, files - capacity);
        else
            queue = 0;
    }
    queue_limit_.store(queue, std::memory_order_relaxed);

    long long idle = configured_idle_ms.load(std::memory_order_relaxed);
    if (idle < 0)
        idle = environment_idle_ms();
    idle_timeout_ = std::chrono::milliseconds(idle);

    const size_t initial = std::min(capacity_.load(std::memory_order_relaxed), available_cpus());
    min_workers_ = initial;
    try {
        for (size_t i = 0; i < initial; ++i)
            threads_.emplace_back([this] { worker_loop(); });
    } catch (...) {
        stop_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
        for (auto &thread : threads_) {
            if (thread.joinable())
                thread.join();
        }
        throw;
    }
    size_.store(threads_.size(), std::memory_order_relaxed);
}

StreamPumpPool::~StreamPumpPool()
{
    shutdown();
}

void StreamPumpPool::worker_loop()
{
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mu_);

            if (idle_timeout_.count() > 0 && threads_.size() > min_workers_) {
                const bool woken = cv_.wait_for(lock, idle_timeout_, [this] {
                    return stop_.load(std::memory_order_relaxed) || !tasks_.empty();
                });
                if (!woken && !stop_.load(std::memory_order_relaxed) && tasks_.empty() &&
                    threads_.size() > min_workers_) {
                    retire_worker_locked();
                    return;
                }
            } else {
                cv_.wait(lock, [this] { return stop_.load(std::memory_order_relaxed) || !tasks_.empty(); });
            }

            if (tasks_.empty()) {
                if (stop_.load(std::memory_order_relaxed))
                    break;
                continue;
            }
            task = std::move(tasks_.front());
            tasks_.pop_front();
            ++busy_;
        }
        task();
        {
            std::lock_guard<std::mutex> lock(mu_);
            --busy_;
        }
    }
}

void StreamPumpPool::retire_worker_locked()
{
    const auto self = std::this_thread::get_id();
    for (auto it = threads_.begin(); it != threads_.end(); ++it) {
        if (it->get_id() == self) {
            retired_.splice(retired_.end(), threads_, it);
            size_.store(threads_.size(), std::memory_order_relaxed);
            return;
        }
    }
}

bool StreamPumpPool::spawn_worker_locked()
{
    try {
        threads_.emplace_back([this] { worker_loop(); });
    } catch (const std::system_error &error) {
        capacity_.store(threads_.size(), std::memory_order_relaxed);
        queue_limit_.store(std::min(queue_limit_.load(std::memory_order_relaxed), threads_.size()),
                           std::memory_order_relaxed);
        min_workers_ = std::min(min_workers_, threads_.size());
        std::cerr << "pump pool: cannot create worker (" << error.what() << "); capacity limited to "
                  << threads_.size() << '\n';
        return false;
    }
    size_.store(threads_.size(), std::memory_order_relaxed);
    return true;
}

void StreamPumpPool::shutdown()
{
    if (stop_.exchange(true, std::memory_order_relaxed))
        return;

    std::list<std::thread> workers;
    {
        std::lock_guard<std::mutex> lock(mu_);
        workers.swap(threads_);
        workers.splice(workers.end(), retired_);
        cv_.notify_all();
    }
    for (auto &worker : workers) {
        if (worker.joinable())
            worker.join();
    }
    size_.store(0, std::memory_order_relaxed);
}

std::shared_ptr<StreamGate> StreamPumpPool::submit(std::function<void()> task, boost::asio::any_io_executor io_ex)
{
    auto gate = StreamGate::create(std::move(io_ex));
    bool grew = false;
    std::list<std::thread> retired;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stop_.load(std::memory_order_relaxed))
            return nullptr;

        if (threads_.size() >= capacity_.load(std::memory_order_relaxed) &&
            busy_ >= threads_.size() &&
            tasks_.size() >= queue_limit_.load(std::memory_order_relaxed)) {
            rejected_.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }

        tasks_.push_back([gate, task = std::move(task)]() mutable {
            try {
                task();
            } catch (const std::exception &err) {
                std::cerr << "pump task failed: " << err.what() << '\n';
            } catch (...) {
                std::cerr << "pump task failed: unknown\n";
            }
            gate->finish();
        });

        if (!stop_.load(std::memory_order_relaxed) &&
            threads_.size() < capacity_.load(std::memory_order_relaxed) &&
            busy_ + tasks_.size() > threads_.size()) {
            grew = spawn_worker_locked();
        }

        retired.swap(retired_);
    }
    for (auto &worker : retired) {
        if (worker.joinable())
            worker.join();
    }
    if (grew)
        cv_.notify_all();
    else
        cv_.notify_one();
    return gate;
}

} // namespace realurl
