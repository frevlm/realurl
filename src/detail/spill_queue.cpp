#include "realurl/detail/spill_queue.hpp"

#include "realurl/detail/event_loop.hpp"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <mutex>
#include <system_error>
#include <thread>
#include <utility>

namespace realurl::detail
{

namespace
{

std::atomic<std::uint64_t> g_spilled{ 0 };

/// Count `n` more bytes against the process-wide cap; false if they do not
/// fit. A limit of 0 only counts.
bool reserve(std::uint64_t n, std::uint64_t limit) noexcept
{
    std::uint64_t current = g_spilled.load(std::memory_order_relaxed);
    do {
        if (limit > 0 && current + n > limit)
            return false;
    } while (!g_spilled.compare_exchange_weak(current, current + n, std::memory_order_relaxed));
    return true;
}

void release(std::uint64_t n) noexcept
{
    if (n > 0)
        g_spilled.fetch_sub(n, std::memory_order_relaxed);
}

/// Anonymous temporary file: created on the first append, unlinked at once,
/// truncated back to zero on reset so a long-lived stream does not grow it
/// without bound. Plain blocking pread/pwrite on page-cache backed storage.
class FileSpillStore final : public SpillStore {
public:
    explicit FileSpillStore(std::string dir)
        : dir_(std::move(dir))
    {
    }

    ~FileSpillStore() override
    {
        if (fd_ >= 0)
            ::close(fd_);
    }

    bool append(std::string_view bytes) override
    {
        if (fd_ < 0 && !open_file())
            return false;
        // A failed write leaves write_offset_ alone, so whatever part did land
        // is overwritten by the next append.
        std::size_t done = 0;
        while (done < bytes.size()) {
            const ssize_t r = ::pwrite(fd_, bytes.data() + done, bytes.size() - done,
                                       static_cast<off_t>(write_offset_ + done));
            if (r < 0 && errno == EINTR)
                continue;
            if (r <= 0)
                return false;
            done += static_cast<std::size_t>(r);
        }
        write_offset_ += done;
        return true;
    }

    bool read(std::span<char> out) override
    {
        std::size_t done = 0;
        while (done < out.size()) {
            const ssize_t r = ::pread(fd_, out.data() + done, out.size() - done,
                                      static_cast<off_t>(read_offset_ + done));
            if (r < 0 && errno == EINTR)
                continue;
            if (r <= 0)
                return false;
            done += static_cast<std::size_t>(r);
        }
#if defined(__linux__)
        (void)::posix_fadvise(fd_, static_cast<off_t>(read_offset_), static_cast<off_t>(done), POSIX_FADV_DONTNEED);
#endif
        read_offset_ += done;
        return true;
    }

    void reset() override
    {
        read_offset_ = 0;
        write_offset_ = 0;
        if (fd_ >= 0)
            (void)::ftruncate(fd_, 0);
    }

private:
    bool open_file()
    {
        std::string dir = dir_;
        if (dir.empty()) {
            const char *tmp = std::getenv("TMPDIR");
            dir = tmp != nullptr && *tmp != '\0' ? tmp : "/tmp";
        }

#ifdef O_TMPFILE
        fd_ = ::open(dir.c_str(), O_TMPFILE | O_RDWR | O_CLOEXEC, 0600);
        if (fd_ >= 0)
            return true;
#endif

        if (dir.back() != '/')
            dir.push_back('/');
        dir.append("realurl-spill-XXXXXX");
        fd_ = ::mkostemp(dir.data(), O_CLOEXEC);
        if (fd_ < 0)
            return false;
        ::unlink(dir.c_str());
        return true;
    }

    std::string dir_;
    int fd_ = -1;
    std::uint64_t read_offset_ = 0;
    std::uint64_t write_offset_ = 0;
};

/// FIFO of store jobs served by detached threads shared by every offloaded
/// queue in the process.
class SpillHelper {
public:
    /// The first call starts `threads` threads; later counts are ignored.
    static SpillHelper &instance(int threads = 1)
    {
        // Leaked on purpose: jobs may still run during static destruction.
        static SpillHelper *helper = new SpillHelper(std::max(1, threads));
        return *helper;
    }

    void post(std::function<void()> job)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            jobs_.push_back(std::move(job));
        }
        wake_.notify_one();
    }

private:
    explicit SpillHelper(int threads)
    {
        for (int i = 0; i < threads; ++i)
            std::thread([this] { run(); }).detach();
    }

    void run()
    {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                wake_.wait(lock, [this] { return !jobs_.empty(); });
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            job();
        }
    }

    std::mutex mutex_;
    std::condition_variable wake_;
    std::deque<std::function<void()>> jobs_;
};

} // namespace

std::unique_ptr<SpillStore> make_file_spill_store(std::string dir)
{
    return std::make_unique<FileSpillStore>(std::move(dir));
}

/// One store and the route its job results take back to the queue. Jobs hold
/// a reference, so the store outlives a queue that abandons it mid-job.
struct SpillQueue::Shared {
    std::unique_ptr<SpillStore> store;
    /// Loop thread only. Null once the queue abandoned this store, which
    /// drops results that are already posted.
    SpillQueue *owner = nullptr;
    std::mutex mutex;
    /// Guarded by mutex. Null once abandoned, so the helper stops posting to
    /// a loop that may be gone.
    EventLoop *loop = nullptr;
};

struct SpillQueue::Job {
    /// Write: the payload, and on failure the bytes handed back. Read: the
    /// result.
    std::string bytes;
    std::size_t size = 0;
    bool write = false;
    /// Read: the store is empty afterwards, so reset it.
    bool reset = false;
    bool ok = false;
};

SpillQueue::SpillQueue(std::shared_ptr<const SpillConfig> config, EventLoop *loop) noexcept
    : memory_limit_(config != nullptr ? config->threshold : 0)
    , config_(std::move(config))
    , loop_(config_ != nullptr && config_->offload ? loop : nullptr)
{
}

SpillQueue::~SpillQueue()
{
    release(stored_ + writing_);
    abandon();
}

std::uint64_t SpillQueue::total_spilled() noexcept
{
    return g_spilled.load(std::memory_order_relaxed);
}

void SpillQueue::append(std::string_view bytes)
{
    if (bytes.empty())
        return;

    if (size() == head_.size() && (memory_limit_ == 0 || head_.size() + bytes.size() <= memory_limit_)) {
        head_.append(bytes);
        return;
    }
    pending_.append(bytes);
    pump();
}

std::size_t SpillQueue::read(std::span<char> out)
{
    std::size_t total = 0;
    while (total < out.size()) {
        if (head_.empty()) {
            pump();
            if (head_.empty())
                break;
        }
        total += head_.read(out.subspan(total));
    }
    if (total == 0)
        throw_if_failed();
    pump();
    if (refused_ && size() < memory_limit_)
        refused_ = false;
    return total;
}

void SpillQueue::drain_to(std::string &out)
{
    out.reserve(out.size() + size());
    do {
        head_.drain_to(out);
        pump();
    } while (!head_.empty());
    throw_if_failed();
    if (refused_ && size() < memory_limit_)
        refused_ = false;
}

void SpillQueue::clear() noexcept
{
    head_.clear();
    std::string().swap(pending_);
    release(stored_ + writing_);
    stored_ = 0;
    writing_ = 0;
    abandon();
    busy_ = false;
    error_ = 0;
    refused_ = false;
}

void SpillQueue::pump()
{
    while (!busy_ && error_ == 0) {
        if ((failed_ || refused_) && head_.empty() && stored_ == 0 && writing_ == 0 && !pending_.empty()) {
            // Bytes that cannot be spilled reach the reader once nothing is
            // ahead of them.
            head_.append(pending_);
            std::string().swap(pending_);
        }

        // Synchronous queues fetch once the head is empty. Offloaded ones
        // fetch ahead, while half of a chunk or of the memory limit is left,
        // to hide the helper round trip.
        const std::size_t refill_at = loop_ != nullptr ? std::min(memory_limit_, kChunk) / 2 : 0;
        Job job;
        if (stored_ > 0 && head_.size() <= refill_at) {
            job.size = static_cast<std::size_t>(std::min<std::uint64_t>(kChunk, stored_));
            job.reset = job.size == stored_;
        } else if (!pending_.empty() && !failed_) {
            if (shared_ == nullptr) {
                std::unique_ptr<SpillStore> store;
                try {
                    store = config_->store ? config_->store() : make_file_spill_store(config_->dir);
                } catch (...) {
                }
                if (store == nullptr) {
                    failed_ = true;
                    return;
                }
                shared_ = std::make_shared<Shared>();
                shared_->store = std::move(store);
                shared_->owner = this;
                shared_->loop = loop_;
            }
            if (!reserve(pending_.size(), config_->total_limit)) {
                refused_ = true;
                return;
            }
            job.write = true;
            job.size = pending_.size();
            job.bytes = std::move(pending_);
            pending_.clear();
            writing_ = job.size;
        } else {
            return;
        }
        run(std::move(job));
    }
}

void SpillQueue::run(Job job)
{
    // Runs where the store lives: inline, or on the helper.
    const auto execute = [](SpillStore &store, Job &job) {
        if (job.write) {
            job.ok = store.append(job.bytes);
            if (job.ok)
                std::string().swap(job.bytes);
            return;
        }
        job.bytes.resize(job.size);
        job.ok = store.read({ job.bytes.data(), job.size });
        if (job.ok && job.reset)
            store.reset();
    };

    busy_ = true;
    if (loop_ == nullptr) {
        execute(*shared_->store, job);
        finish(std::move(job));
        return;
    }

    SpillHelper::instance(config_->threads).post([execute, shared = shared_, job = std::move(job)]() mutable {
        execute(*shared->store, job);
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->loop == nullptr)
            return;
        shared->loop->post([shared, job = std::move(job)]() mutable {
            SpillQueue *queue = shared->owner;
            if (queue == nullptr)
                return;
            queue->finish(std::move(job));
            queue->pump();
            if (queue->ready_ != nullptr)
                queue->ready_(queue->ready_context_);
        });
    });
}

void SpillQueue::finish(Job job)
{
    busy_ = false;
    if (job.write) {
        writing_ = 0;
        if (job.ok) {
            stored_ += job.size;
            return;
        }
        // The store refused the bytes: they stay in memory, ahead of anything
        // that arrived while the job ran, and spilling stops for this queue.
        failed_ = true;
        release(job.size);
        job.bytes.append(pending_);
        pending_ = std::move(job.bytes);
        return;
    }

    if (!job.ok) {
        // The bytes exist only in the store; there is nothing to fall back to.
        failed_ = true;
        error_ = EIO;
        return;
    }
    head_.append(job.bytes);
    stored_ -= job.size;
    release(job.size);
}

void SpillQueue::throw_if_failed() const
{
    if (error_ != 0)
        throw std::system_error(error_, std::generic_category(), "spill read");
}

void SpillQueue::abandon() noexcept
{
    if (shared_ == nullptr)
        return;
    shared_->owner = nullptr;
    if (loop_ != nullptr) {
        {
            std::lock_guard<std::mutex> lock(shared_->mutex);
            shared_->loop = nullptr;
        }
        // The last reference may be this one; a store's destructor can block
        // (closing a file or a database handle), so drop it on the helper.
        SpillHelper::instance(config_->threads).post([shared = std::move(shared_)] {});
    }
    shared_.reset();
}

} // namespace realurl::detail
