#pragma once

/// Per-thread event loop: epoll on Linux, kqueue on macOS.
///
/// One loop per thread. Connections are bound to the loop that created them.
/// The hot path (socket readiness, timers, coroutine resumption) runs entirely
/// on the loop thread and takes no locks. Only post() crosses threads.

#include "realurl/detail/task.hpp"

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

namespace realurl::detail
{

#ifdef REALURL_WITH_IO_URING
class Uring;
#endif

using TimerId = std::uint64_t;

enum : std::uint32_t {
    kIoRead = 1,
    kIoWrite = 2,
    kIoError = 4,
};

using IoCallback = void (*)(void *ctx, std::uint32_t events);

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop &) = delete;
    EventLoop &operator=(const EventLoop &) = delete;

    static EventLoop *current_or_null() noexcept;
    static EventLoop &current();

    /// Run until stop(). Each iteration polls, fires due timers, and resumes
    /// ready coroutines.
    void run();

    /// One poll iteration. Returns false once stopped.
    bool run_once();

    void stop();
    bool stopped() const noexcept;

    /// Thread-safe: enqueue a callback on the loop thread.
    void post(std::function<void()> fn);

    /// Loop-thread only. Returns an id usable with cancel().
    TimerId schedule(std::chrono::nanoseconds delay, std::function<void()> fn);
    void cancel(TimerId id);

    /// Start a detached top-level coroutine. The frame destroys itself when
    /// the coroutine completes.
    void spawn(Task<void> &&task);

    /// Suspend the current coroutine for `delay`. Loop-thread only.
    Task<void> sleep_for(std::chrono::nanoseconds delay);

    /// Loop-thread only. Register interest in fd readiness. A second call for
    /// the same fd replaces the callback and the event mask.
    void add_io(int fd, std::uint32_t events, IoCallback callback, void *ctx);
    void del_io(int fd);
    void del_io(int fd, std::uint32_t direction);

    /// Queue a coroutine for resumption after the current poll batch. This is
    /// the only way IO completions resume coroutines, which keeps poller
    /// callbacks from mutating registrations mid-iteration.
    void resume_later(std::coroutine_handle<> handle);

    void wakeup();

#ifdef REALURL_WITH_IO_URING
    /// Lazily create the per-loop io_uring. Returns nullptr when the kernel or
    /// sandbox does not allow io_uring.
    Uring *uring();
#endif

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    EventLoop *previous_ = nullptr;
};

/// Run a task to completion on a private EventLoop. Only callable from a
/// thread that is not already running an event loop.
template <class T>
T sync_wait(Task<T> task)
{
    if (EventLoop::current_or_null() != nullptr)
        throw std::logic_error("sync_wait() cannot run inside an EventLoop");
    if (!task.valid())
        throw std::logic_error("sync_wait() on an empty task");

    EventLoop loop;
    std::exception_ptr error;
    using Stored = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
    std::optional<Stored> value;
    bool done = false;

    auto wrapper = [&]() -> Task<void> {
        try {
            if constexpr (std::is_void_v<T>) {
                co_await std::move(task);
            } else {
                value.emplace(co_await std::move(task));
            }
        } catch (...) {
            error = std::current_exception();
        }
        done = true;
    };

    loop.spawn(wrapper());
    while (!done)
        loop.run_once();

    if (error)
        std::rethrow_exception(error);
    if constexpr (!std::is_void_v<T>)
        return std::move(*value);
}

} // namespace realurl::detail

namespace realurl
{

using detail::EventLoop;
using detail::sync_wait;
using detail::Task;

} // namespace realurl
