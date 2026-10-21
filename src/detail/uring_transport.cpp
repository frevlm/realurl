#ifdef REALURL_WITH_IO_URING

#include "uring_transport.hpp"

#include <cerrno>
#include <coroutine>
#include <system_error>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace realurl::detail
{
namespace
{

class RecvOp final : public UringOp {
public:
    RecvOp(Uring &ring, int fd, std::span<char> buffer) noexcept
        : ring_(ring)
        , fd_(fd)
        , buffer_(buffer)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        if (!ring_.submit_recv(fd_, buffer_, this)) {
            result_ = -EIO;
            ring_.loop().resume_later(handle_);
        }
    }

    int await_resume() const noexcept
    {
        return result_;
    }

    void complete(int result) override
    {
        result_ = result;
        if (handle_)
            ring_.loop().resume_later(handle_);
    }

private:
    Uring &ring_;
    int fd_;
    std::span<char> buffer_;
    std::coroutine_handle<> handle_{};
    int result_ = 0;
};

class SendOp final : public UringOp {
public:
    SendOp(Uring &ring, int fd, std::string_view data) noexcept
        : ring_(ring)
        , fd_(fd)
        , data_(data)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        if (!ring_.submit_send(fd_, data_, this)) {
            result_ = -EIO;
            ring_.loop().resume_later(handle_);
        }
    }

    int await_resume() const noexcept
    {
        return result_;
    }

    void complete(int result) override
    {
        result_ = result;
        if (handle_)
            ring_.loop().resume_later(handle_);
    }

private:
    Uring &ring_;
    int fd_;
    std::string_view data_;
    std::coroutine_handle<> handle_{};
    int result_ = 0;
};

} // namespace

UringTransport::UringTransport(EventLoop &loop, Uring &ring, int fd) noexcept
    : loop_(loop)
    , ring_(ring)
    , fd_(fd)
{
}

UringTransport::~UringTransport()
{
    close();
}

std::unique_ptr<Transport> UringTransport::create(EventLoop &loop, int fd) noexcept
{
    if (fd < 0)
        return nullptr;

    Uring *ring = loop.uring();
    if (ring == nullptr || !ring->valid()) {
        ::close(fd);
        return nullptr;
    }

    return std::unique_ptr<Transport>(new UringTransport(loop, *ring, fd));
}

Task<std::size_t> UringTransport::read(std::span<char> buffer, std::chrono::milliseconds)
{
    for (;;) {
        if (fd_ < 0)
            throw std::system_error(EBADF, std::generic_category(), "uring recv");
        const int result = co_await RecvOp(ring_, fd_, buffer);
        if (result == -EINTR)
            continue;
        if (result < 0)
            throw std::system_error(-result, std::generic_category(), "uring recv");
        co_return static_cast<std::size_t>(result);
    }
}

Task<void> UringTransport::write_all(std::string_view data, std::chrono::milliseconds)
{
    while (!data.empty()) {
        if (fd_ < 0)
            throw std::system_error(EBADF, std::generic_category(), "uring send");
        const int result = co_await SendOp(ring_, fd_, data);
        if (result == -EINTR)
            continue;
        if (result < 0)
            throw std::system_error(-result, std::generic_category(), "uring send");
        if (result == 0)
            throw std::system_error(EPIPE, std::generic_category(), "uring send");
        data.remove_prefix(static_cast<std::size_t>(result));
    }
    co_return;
}

Task<void> UringTransport::shutdown_write()
{
    if (fd_ >= 0)
        (void)::shutdown(fd_, SHUT_WR);
    co_return;
}

void UringTransport::close() noexcept
{
    if (fd_ < 0)
        return;
    (void)::shutdown(fd_, SHUT_RDWR);
    ::close(fd_);
    fd_ = -1;
}

} // namespace realurl::detail

#endif
