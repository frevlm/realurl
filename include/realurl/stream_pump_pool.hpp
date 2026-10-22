#pragma once

#include <cstddef>
#include <sys/types.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace realurl
{

/// Completion-gate signalling that a pump-pool task has finished.
///
/// A streaming request is offloaded to a pump-pool thread; the connection
/// coroutine registers a completion via async_wait() (through the
/// async_wait_gate awaitable adapter), which suspends the coroutine and frees
/// its io thread for the whole stream duration.  The pump thread calls
/// finish() when the stream work is done; the completion is posted back onto
/// the io executor.
class StreamGate : public std::enable_shared_from_this<StreamGate> {
public:
    static std::shared_ptr<StreamGate> create(boost::asio::any_io_executor io_ex)
    {
        return std::shared_ptr<StreamGate>(new StreamGate(std::move(io_ex)));
    }

    /// Called from the pump thread when the stream work is done (safe to call
    /// once; further calls are no-ops).
    void finish()
    {
        std::function<void()> callback;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (finished_.exchange(true, std::memory_order_acq_rel))
                return;
            callback = std::move(callback_);
        }
        if (callback)
            boost::asio::post(io_ex_, std::move(callback));
    }

    /// Register a completion handler, invoked exactly once (on the io
    /// executor) when the gate finishes.  If the gate already finished, the
    /// handler is posted immediately.
    void async_wait(std::function<void()> callback)
    {
        bool already_finished = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            already_finished = finished_.load(std::memory_order_acquire);
            if (!already_finished)
                callback_ = std::move(callback);
        }
        if (already_finished)
            boost::asio::post(io_ex_, std::move(callback));
    }

private:
    explicit StreamGate(boost::asio::any_io_executor io_ex)
        : io_ex_(std::move(io_ex))
    {
    }

    boost::asio::any_io_executor io_ex_;
    std::mutex mu_;
    std::function<void()> callback_;
    std::atomic<bool> finished_{ false };
};

/// asio-awaitable adapter: co_await async_wait_gate(gate, net::use_awaitable)
/// suspends the coroutine until gate->finish() is called.
template <typename CompletionToken> auto async_wait_gate(std::shared_ptr<StreamGate> gate, CompletionToken &&token)
{
    return boost::asio::async_initiate<CompletionToken, void()>(
        [gate = std::move(gate)](auto handler) mutable {
            // std::function requires a copyable callable; the asio handler may
            // be move-only, so box it in a shared_ptr.
            auto boxed = std::make_shared<std::decay_t<decltype(handler)>>(std::move(handler));
            gate->async_wait([boxed]() mutable { std::move (*boxed)(); });
        },
        token);
}

/// Bounded pool of long-lived pump threads.
///
/// Tasks share one global FIFO queue and run on whichever worker is free: no
/// client or IP is pinned to a thread or serialized behind another stream.
/// A task blocks its thread until it returns, so capacity() is the ceiling on
/// concurrently running streams; excess submissions wait in the queue.
///
/// Workers are created on demand: the pool starts with min(capacity(), CPUs)
/// threads and grows while tasks are backlogged.  The automatic capacity is
/// derived from the process resource budgets - the smallest of the thread
/// budget (cgroup pids.max, RLIMIT_NPROC, kernel threads-max/pid_max), the
/// file-descriptor budget (RLIMIT_NOFILE minus headroom), and the address
/// space budget (RLIMIT_AS divided by the thread stack) - and is never below
/// one.  configure() or REALURL_PUMP_THREADS overrides the computed ceiling.
///
/// Waiting work is bounded too: once every worker is busy at capacity() and
/// queue_limit() tasks are already waiting, submit() refuses the task instead
/// of growing an unbounded backlog.  The queue defaults to what remains of the
/// fd budget after the workers, capped at capacity(), and can be overridden
/// with configure_queue() or REALURL_PUMP_QUEUE.
class StreamPumpPool {
public:
    static StreamPumpPool &instance();

    /// Set the capacity used when the singleton is constructed. 0 restores the
    /// automatic calculation.  Ignored once the pool has started.
    static void configure(size_t count);

    /// Set the queue limit used when the singleton is constructed. 0 restores
    /// the automatic value.  Ignored once the pool has started.
    static void configure_queue(size_t limit);

    /// Idle time before workers above the initial size exit. 0 disables
    /// shrinking.  Ignored once the pool has started.
    static void configure_idle(std::chrono::milliseconds timeout);

    /// Run `task` on a pump thread.  Returns a gate that the connection
    /// coroutine co_awaits; the gate is finished automatically when the task
    /// returns or throws.  Returns nullptr when the pool is at capacity and
    /// its queue is full; the caller should shed load (the server returns 503).
    std::shared_ptr<StreamGate> submit(std::function<void()> task, boost::asio::any_io_executor io_ex);

    /// Current number of worker threads (grows on demand up to capacity()).
    size_t size() const
    {
        return size_.load(std::memory_order_relaxed);
    }

    /// Ceiling on concurrent workers; see the class comment.
    size_t capacity() const
    {
        return capacity_.load(std::memory_order_relaxed);
    }

    /// Maximum number of tasks allowed to wait for a worker.
    size_t queue_limit() const
    {
        return queue_limit_.load(std::memory_order_relaxed);
    }

    /// Total submissions refused because the queue was full.
    size_t rejected() const
    {
        return rejected_.load(std::memory_order_relaxed);
    }

    /// Configured idle timeout; 0 means workers are never reclaimed.
    std::chrono::milliseconds idle_timeout() const
    {
        return idle_timeout_;
    }

    void shutdown();

private:
    StreamPumpPool();
    ~StreamPumpPool();
    StreamPumpPool(const StreamPumpPool &) = delete;
    StreamPumpPool &operator=(const StreamPumpPool &) = delete;

    void worker_loop();
    void retire_worker_locked();
    bool spawn_worker_locked();

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> tasks_;
    std::list<std::thread> threads_;
    std::list<std::thread> retired_;
    size_t busy_ = 0;       // guarded by mu_
    size_t min_workers_ = 0; // guarded by mu_
    std::chrono::milliseconds idle_timeout_{ 0 };
    std::atomic<size_t> size_{ 0 };
    std::atomic<size_t> capacity_{ 0 };
    std::atomic<size_t> queue_limit_{ 0 };
    std::atomic<size_t> rejected_{ 0 };
    std::atomic<bool> stop_{ false };
};

StreamPumpPool &stream_pump_pool();

} // namespace realurl
