#pragma once

/// Minimal C++20 task type. A Task is cold: it starts when awaited or spawned
/// onto an EventLoop. Sub-tasks resume their parent by symmetric transfer;
/// a top-level Task destroys its own frame on completion.

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace realurl::detail
{

class EventLoop;

template <class T>
class Task;

struct TaskPromiseBase {
    std::coroutine_handle<> continuation{};
    std::exception_ptr error{};
    EventLoop *loop = nullptr;

    std::suspend_always initial_suspend() noexcept
    {
        return {};
    }

    struct FinalAwaiter {
        bool await_ready() noexcept
        {
            return false;
        }

        template <class Promise>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> handle) noexcept
        {
            const std::coroutine_handle<> next = handle.promise().continuation;
            if (next)
                return next;
            handle.destroy();
            return std::noop_coroutine();
        }

        void await_resume() noexcept
        {
        }
    };

    FinalAwaiter final_suspend() noexcept
    {
        return {};
    }

    void unhandled_exception() noexcept
    {
        error = std::current_exception();
    }
};

template <class T>
struct TaskPromise : TaskPromiseBase {
    std::optional<T> value;

    Task<T> get_return_object() noexcept;

    template <class U>
    void return_value(U &&result)
    {
        value.emplace(std::forward<U>(result));
    }
};

template <>
struct TaskPromise<void> : TaskPromiseBase {
    Task<void> get_return_object() noexcept;

    void return_void() noexcept
    {
    }
};

template <class T = void>
class Task {
public:
    using promise_type = TaskPromise<T>;

    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept
        : handle_(handle)
    {
    }

    Task(Task &&other) noexcept
        : handle_(std::exchange(other.handle_, {}))
    {
    }

    Task &operator=(Task &&other) noexcept
    {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    Task(const Task &) = delete;
    Task &operator=(const Task &) = delete;

    ~Task()
    {
        if (handle_)
            handle_.destroy();
    }

    bool valid() const noexcept
    {
        return static_cast<bool>(handle_);
    }

    /// Give up ownership of the frame. Used by EventLoop::spawn().
    std::coroutine_handle<promise_type> release() noexcept
    {
        return std::exchange(handle_, {});
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) noexcept
    {
        handle_.promise().continuation = continuation;
        return handle_;
    }

    T await_resume()
    {
        auto &promise = handle_.promise();
        if (promise.error)
            std::rethrow_exception(promise.error);
        if constexpr (!std::is_void_v<T>)
            return std::move(*promise.value);
    }

private:
    std::coroutine_handle<promise_type> handle_{};
};

template <class T>
Task<T> TaskPromise<T>::get_return_object() noexcept
{
    return Task<T>{ std::coroutine_handle<TaskPromise<T>>::from_promise(*this) };
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept
{
    return Task<void>{ std::coroutine_handle<TaskPromise<void>>::from_promise(*this) };
}

} // namespace realurl::detail
