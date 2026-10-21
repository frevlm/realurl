#include "realurl/http2.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace realurl
{
namespace
{

struct GroupJob {
    EventLoop *target_loop = nullptr;
    EventLoop *caller_loop = nullptr;
    std::coroutine_handle<> caller{};
    Client *client = nullptr;

    std::string method;
    std::string url;
    Headers headers;
    std::string body;
    std::chrono::milliseconds timeout{};

    std::optional<Response> response;
    std::exception_ptr error;
};

Task<void> run_group_job(std::shared_ptr<GroupJob> job)
{
    try {
        job->response = co_await job->client->request(std::move(job->method), std::move(job->url),
                                                      std::move(job->headers), std::move(job->body), job->timeout);
    } catch (...) {
        job->error = std::current_exception();
    }

    job->caller_loop->post([job] {
        if (job->caller)
            job->caller.resume();
    });
    co_return;
}

class CrossLoopAwaiter {
public:
    explicit CrossLoopAwaiter(std::shared_ptr<GroupJob> job) noexcept
        : job_(std::move(job))
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        job_->caller = handle;
        EventLoop *target = job_->target_loop;
        target->post([job = job_, target] { target->spawn(run_group_job(job)); });
    }

    Response await_resume()
    {
        if (job_->error)
            std::rethrow_exception(job_->error);
        if (!job_->response)
            throw std::runtime_error("client group: request completed without a response");
        return std::move(*job_->response);
    }

private:
    std::shared_ptr<GroupJob> job_;
};

} // namespace

struct ClientGroup::Impl {
    struct Worker {
        std::thread thread;
        std::mutex mutex;
        std::condition_variable cv;
        bool ready = false;
        std::unique_ptr<EventLoop> loop_owner;
        std::unique_ptr<Client> client_owner;
        EventLoop *loop = nullptr;
        Client *client = nullptr;
    };

    Impl(unsigned count, Config config)
        : config_(std::move(config))
    {
        if (count == 0)
            count = 1;

        workers_.reserve(count);
        for (unsigned i = 0; i < count; ++i) {
            auto worker = std::make_unique<Worker>();
            Worker *raw = worker.get();
            raw->thread = std::thread([this, raw] {
                {
                    std::lock_guard<std::mutex> lock(raw->mutex);
                    raw->loop_owner = std::make_unique<EventLoop>();
                    raw->client_owner = std::make_unique<Client>(*raw->loop_owner, config_);
                    raw->loop = raw->loop_owner.get();
                    raw->client = raw->client_owner.get();
                    raw->ready = true;
                }
                raw->cv.notify_one();
                raw->loop_owner->run();
            });
            workers_.push_back(std::move(worker));
        }

        for (const auto &worker : workers_) {
            std::unique_lock<std::mutex> lock(worker->mutex);
            worker->cv.wait(lock, [&worker] { return worker->ready; });
        }
    }

    ~Impl()
    {
        shutdown();
    }

    Worker &pick()
    {
        const std::size_t index = next_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        return *workers_[index];
    }

    void begin_job()
    {
        inflight_.fetch_add(1, std::memory_order_acq_rel);
    }

    void end_job()
    {
        if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(job_mutex_);
            job_cv_.notify_all();
        }
    }

    Task<void> run_spawned(std::shared_ptr<Task<void>> task)
    {
        try {
            co_await std::move(*task);
        } catch (...) {
        }
        end_job();
        co_return;
    }

    void spawn_task(std::size_t index, Task<void> &&task)
    {
        if (!task.valid())
            return;
        if (closed_.load(std::memory_order_acquire))
            throw std::runtime_error("ClientGroup is closed");

        Worker &worker = *workers_[index % workers_.size()];
        auto holder = std::make_shared<Task<void>>(std::move(task));
        begin_job();
        EventLoop *target = worker.loop;
        target->post([this, holder, target] { target->spawn(run_spawned(std::move(holder))); });
    }

    void close_clients()
    {
        for (const auto &worker : workers_) {
            Worker *raw = worker.get();
            raw->loop->post([raw] { raw->client->close(); });
        }
    }

    void shutdown()
    {
        if (closed_.exchange(true, std::memory_order_acq_rel))
            return;

        {
            std::unique_lock<std::mutex> lock(job_mutex_);
            job_cv_.wait_for(lock, std::chrono::seconds(5), [this] {
                return inflight_.load(std::memory_order_acquire) == 0;
            });
        }

        for (const auto &worker : workers_) {
            Worker *raw = worker.get();
            raw->loop->post([raw] {
                raw->client->close();
                raw->loop->stop();
            });
        }
        for (auto &worker : workers_) {
            if (worker->thread.joinable())
                worker->thread.join();
        }
    }

    Config config_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<std::size_t> next_{ 0 };
    std::atomic<int> inflight_{ 0 };
    std::atomic<bool> closed_{ false };
    std::mutex job_mutex_;
    std::condition_variable job_cv_;
};

ClientGroup::ClientGroup(unsigned workers, Config config)
    : impl_(std::make_unique<Impl>(workers, std::move(config)))
{
}

ClientGroup::~ClientGroup() = default;

std::size_t ClientGroup::workers() const noexcept
{
    return impl_->workers_.size();
}

Task<Response> ClientGroup::get(std::string url, std::chrono::milliseconds timeout)
{
    co_return co_await request("GET", std::move(url), {}, {}, timeout);
}

Task<Response> ClientGroup::request(std::string method, std::string url, Headers headers, std::string body,
                                    std::chrono::milliseconds timeout)
{
    EventLoop *caller = EventLoop::current_or_null();
    if (caller == nullptr)
        throw std::logic_error("ClientGroup::request() requires an EventLoop on the calling thread");

    for (const auto &worker : impl_->workers_) {
        if (worker->loop != caller)
            continue;

        impl_->begin_job();
        try {
            Response response = co_await worker->client->request(std::move(method), std::move(url),
                                                                 std::move(headers), std::move(body), timeout);
            impl_->end_job();
            co_return response;
        } catch (...) {
            impl_->end_job();
            throw;
        }
    }

    auto job = std::make_shared<GroupJob>();
    Impl::Worker &worker = impl_->pick();
    job->target_loop = worker.loop;
    job->caller_loop = caller;
    job->client = worker.client;
    job->method = std::move(method);
    job->url = std::move(url);
    job->headers = std::move(headers);
    job->body = std::move(body);
    job->timeout = timeout;

    impl_->begin_job();
    try {
        Response response = co_await CrossLoopAwaiter(std::move(job));
        impl_->end_job();
        co_return response;
    } catch (...) {
        impl_->end_job();
        throw;
    }
}

void ClientGroup::spawn(Task<void> &&task)
{
    impl_->spawn_task(impl_->next_.fetch_add(1, std::memory_order_relaxed), std::move(task));
}

void ClientGroup::spawn_on(std::size_t worker, Task<void> &&task)
{
    impl_->spawn_task(worker, std::move(task));
}

void ClientGroup::close()
{
    impl_->close_clients();
}

} // namespace realurl
