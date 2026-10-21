#include "realurl/detail/event_loop.hpp"

#include "poller.hpp"

#ifdef REALURL_WITH_IO_URING
#include "uring.hpp"
#endif

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace realurl::detail
{
namespace
{

thread_local EventLoop *t_current = nullptr;

using Clock = std::chrono::steady_clock;

} // namespace

struct EventLoop::Impl {
    struct Slot {
        IoCallback callback = nullptr;
        void *ctx = nullptr;
    };

    struct FdState {
        Slot read;
        Slot write;
    };

    struct Timer {
        Clock::time_point when;
        TimerId id = 0;
        std::function<void()> fn;
    };

    struct TimerLater {
        bool operator()(const Timer &a, const Timer &b) const
        {
            return a.when > b.when;
        }
    };

    std::unique_ptr<Poller> poller = Poller::create();

    std::unordered_map<int, FdState> io;
    std::vector<IoEvent> events;

    std::mutex post_mutex;
    std::vector<std::function<void()>> posts;
    std::vector<std::function<void()>> local_posts;

    std::priority_queue<Timer, std::vector<Timer>, TimerLater> timers;
    std::unordered_set<TimerId> active_timers;
    TimerId next_timer_id = 1;

    std::vector<std::coroutine_handle<>> ready;
    std::vector<std::coroutine_handle<>> batch;
    std::atomic<bool> stopped{ false };

#ifdef REALURL_WITH_IO_URING
    bool uring_attempted = false;
    std::unique_ptr<Uring> uring;
#endif

    static std::uint32_t mask_of(const FdState &state)
    {
        return (state.read.callback ? kIoRead : 0U) | (state.write.callback ? kIoWrite : 0U);
    }
};

EventLoop::EventLoop()
    : impl_(std::make_unique<Impl>())
    , previous_(t_current)
{
    t_current = this;
}

EventLoop::~EventLoop()
{
    if (t_current == this)
        t_current = previous_;
}

EventLoop *EventLoop::current_or_null() noexcept
{
    return t_current;
}

EventLoop &EventLoop::current()
{
    if (t_current == nullptr)
        throw std::logic_error("no EventLoop on this thread");
    return *t_current;
}

void EventLoop::run()
{
    while (!impl_->stopped.load(std::memory_order_relaxed))
        (void)run_once();
}

bool EventLoop::run_once()
{
    Impl &impl = *impl_;

    {
        std::lock_guard<std::mutex> lock(impl.post_mutex);
        impl.local_posts.swap(impl.posts);
    }
    for (auto &fn : impl.local_posts)
        fn();
    impl.local_posts.clear();

    const Clock::time_point now = Clock::now();
    while (!impl.timers.empty() && impl.timers.top().when <= now) {
        Impl::Timer timer = impl.timers.top();
        impl.timers.pop();
        if (impl.active_timers.erase(timer.id) > 0)
            timer.fn();
    }

    int timeout_ms = 1000;
    if (!impl.timers.empty()) {
        const auto delta =
            std::chrono::duration_cast<std::chrono::milliseconds>(impl.timers.top().when - Clock::now()).count();
        timeout_ms = delta < 0 ? 0 : static_cast<int>(std::min<long long>(delta, 1000));
    }
    if (!impl.ready.empty())
        timeout_ms = 0;

    if (!impl.stopped.load(std::memory_order_relaxed))
        impl.poller->wait(impl.events, timeout_ms);
    else
        impl.events.clear();

    for (const IoEvent &event : impl.events) {
        const auto it = impl.io.find(event.fd);
        if (it == impl.io.end())
            continue;

        const Impl::Slot read_slot = it->second.read;
        const Impl::Slot write_slot = it->second.write;
        if ((event.events & kIoRead) != 0 && read_slot.callback != nullptr)
            read_slot.callback(read_slot.ctx, event.events);
        if ((event.events & kIoWrite) != 0 && write_slot.callback != nullptr)
            write_slot.callback(write_slot.ctx, event.events);
    }

    while (!impl.ready.empty()) {
        impl.batch.swap(impl.ready);
        for (std::coroutine_handle<> handle : impl.batch)
            handle.resume();
        impl.batch.clear();
    }

    return !impl.stopped.load(std::memory_order_relaxed);
}

void EventLoop::stop()
{
    impl_->stopped.store(true, std::memory_order_relaxed);
    impl_->poller->wakeup();
}

bool EventLoop::stopped() const noexcept
{
    return impl_->stopped.load(std::memory_order_relaxed);
}

void EventLoop::post(std::function<void()> fn)
{
    {
        std::lock_guard<std::mutex> lock(impl_->post_mutex);
        impl_->posts.push_back(std::move(fn));
    }
    impl_->poller->wakeup();
}

TimerId EventLoop::schedule(std::chrono::nanoseconds delay, std::function<void()> fn)
{
    const TimerId id = impl_->next_timer_id++;
    impl_->active_timers.insert(id);
    impl_->timers.push({ Clock::now() + delay, id, std::move(fn) });
    return id;
}

void EventLoop::cancel(TimerId id)
{
    if (id != 0)
        impl_->active_timers.erase(id);
}

void EventLoop::spawn(Task<void> &&task)
{
    std::coroutine_handle<TaskPromise<void>> handle = task.release();
    if (handle)
        handle.resume();
}

Task<void> EventLoop::sleep_for(std::chrono::nanoseconds delay)
{
    struct SleepAwaiter {
        bool await_ready() const noexcept
        {
            return false;
        }

        void await_suspend(std::coroutine_handle<> handle)
        {
            handle_ = handle;
            timer_ = loop_->schedule(delay_, [this] { loop_->resume_later(handle_); });
        }

        void await_resume() noexcept
        {
            if (timer_ != 0)
                loop_->cancel(timer_);
        }

        EventLoop *loop_;
        std::chrono::nanoseconds delay_;
        std::coroutine_handle<> handle_{};
        TimerId timer_ = 0;
    };

    co_await SleepAwaiter{ this, delay };
}

void EventLoop::add_io(int fd, std::uint32_t events, IoCallback callback, void *ctx)
{
    Impl::FdState &state = impl_->io[fd];
    if ((events & kIoRead) != 0)
        state.read = { callback, ctx };
    if ((events & kIoWrite) != 0)
        state.write = { callback, ctx };

    const std::uint32_t mask = Impl::mask_of(state);
    if (mask == 0) {
        impl_->io.erase(fd);
        impl_->poller->del(fd);
        return;
    }
    impl_->poller->set(fd, mask);
}

void EventLoop::del_io(int fd)
{
    del_io(fd, kIoRead | kIoWrite);
}

void EventLoop::del_io(int fd, std::uint32_t direction)
{
    const auto it = impl_->io.find(fd);
    if (it == impl_->io.end())
        return;

    if ((direction & kIoRead) != 0)
        it->second.read = {};
    if ((direction & kIoWrite) != 0)
        it->second.write = {};

    const std::uint32_t mask = Impl::mask_of(it->second);
    if (mask == 0) {
        impl_->poller->del(fd);
        impl_->io.erase(it);
    } else {
        impl_->poller->set(fd, mask);
    }
}

void EventLoop::resume_later(std::coroutine_handle<> handle)
{
    impl_->ready.push_back(handle);
}

void EventLoop::wakeup()
{
    impl_->poller->wakeup();
}

#ifdef REALURL_WITH_IO_URING
Uring *EventLoop::uring()
{
    if (!impl_->uring && !impl_->uring_attempted) {
        impl_->uring_attempted = true;
        auto ring = std::make_unique<Uring>(*this);
        if (ring->valid())
            impl_->uring = std::move(ring);
    }
    return impl_->uring.get();
}
#endif

} // namespace realurl::detail
