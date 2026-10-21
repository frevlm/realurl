#include "realurl/detail/socket.hpp"

#include <cerrno>
#include <cstring>
#include <system_error>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

namespace realurl::detail
{
namespace
{

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

[[noreturn]] void throw_errno(const char *what)
{
    throw std::system_error(errno, std::generic_category(), what);
}

class IoAwaiter {
public:
    IoAwaiter(EventLoop &loop, int fd, std::uint32_t events, std::chrono::milliseconds timeout)
        : loop_(&loop)
        , fd_(fd)
        , events_(events)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        loop_->add_io(fd_, events_, &IoAwaiter::on_ready, this);
        if (timeout_.count() > 0)
            timer_ = loop_->schedule(timeout_, [this] { finish(0, ETIMEDOUT); });
    }

    std::uint32_t await_resume()
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "socket wait");
        return revents_;
    }

private:
    static void on_ready(void *ctx, std::uint32_t events)
    {
        static_cast<IoAwaiter *>(ctx)->finish(events, 0);
    }

    void finish(std::uint32_t events, int error)
    {
        if (done_)
            return;
        done_ = true;
        revents_ = events;
        error_ = error;
        loop_->del_io(fd_, events_);
        if (timer_ != 0) {
            loop_->cancel(timer_);
            timer_ = 0;
        }
        loop_->resume_later(handle_);
    }

    EventLoop *loop_;
    int fd_;
    std::uint32_t events_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerId timer_ = 0;
    std::uint32_t revents_ = 0;
    int error_ = 0;
    bool done_ = false;
};

int accept_nonblocking(int fd, sockaddr *address, socklen_t *length)
{
#ifdef __linux__
    return ::accept4(fd, address, length, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    const int client = ::accept(fd, address, length);
    if (client >= 0) {
        set_nonblocking(client);
        set_cloexec(client);
    }
    return client;
#endif
}

} // namespace

void set_nonblocking(int fd)
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        (void)::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void set_cloexec(int fd)
{
    const int flags = ::fcntl(fd, F_GETFD, 0);
    if (flags >= 0)
        (void)::fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

TcpStream::TcpStream(EventLoop &loop, int fd) noexcept
    : loop_(&loop)
    , fd_(fd)
{
}

TcpStream::~TcpStream()
{
    close();
}

TcpStream::TcpStream(TcpStream &&other) noexcept
    : loop_(other.loop_)
    , fd_(std::exchange(other.fd_, -1))
{
}

TcpStream &TcpStream::operator=(TcpStream &&other) noexcept
{
    if (this != &other) {
        close();
        loop_ = other.loop_;
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

void TcpStream::close() noexcept
{
    if (fd_ >= 0) {
        loop_->del_io(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

void TcpStream::set_nodelay(bool enabled) noexcept
{
    const int value = enabled ? 1 : 0;
    (void)::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
}

Task<std::size_t> TcpStream::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    for (;;) {
        const ssize_t n = ::recv(fd_, buffer.data(), buffer.size(), 0);
        if (n > 0)
            co_return static_cast<std::size_t>(n);
        if (n == 0)
            co_return 0;
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            throw_errno("recv");
        (void)co_await IoAwaiter(*loop_, fd_, kIoRead, idle);
    }
}

Task<void> TcpStream::write_all(std::string_view data, std::chrono::milliseconds timeout)
{
    while (!data.empty()) {
        const ssize_t n = ::send(fd_, data.data(), data.size(), kSendFlags);
        if (n > 0) {
            data.remove_prefix(static_cast<std::size_t>(n));
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            (void)co_await IoAwaiter(*loop_, fd_, kIoWrite, timeout);
            continue;
        }
        throw_errno("send");
    }
}

Task<void> TcpStream::wait_readable(std::chrono::milliseconds timeout)
{
    (void)co_await IoAwaiter(*loop_, fd_, kIoRead, timeout);
}

Task<void> TcpStream::wait_writable(std::chrono::milliseconds timeout)
{
    (void)co_await IoAwaiter(*loop_, fd_, kIoWrite, timeout);
}

Task<void> TcpStream::shutdown_write()
{
    if (fd_ >= 0)
        (void)::shutdown(fd_, SHUT_WR);
    co_return;
}

Task<TcpStream> TcpStream::connect(EventLoop &loop, const sockaddr *address, socklen_t length,
                                   std::chrono::milliseconds timeout)
{
    const int fd = ::socket(address->sa_family, SOCK_STREAM, 0);
    if (fd < 0)
        throw_errno("socket");

    set_nonblocking(fd);
    set_cloexec(fd);

    if (::connect(fd, address, length) != 0) {
        if (errno != EINPROGRESS && errno != EWOULDBLOCK) {
            const int error = errno;
            ::close(fd);
            throw std::system_error(error, std::generic_category(), "connect");
        }

        try {
            (void)co_await IoAwaiter(loop, fd, kIoWrite, timeout);
        } catch (...) {
            ::close(fd);
            throw;
        }

        int error = 0;
        socklen_t error_length = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_length) != 0) {
            const int saved = errno;
            ::close(fd);
            throw std::system_error(saved, std::generic_category(), "getsockopt");
        }
        if (error != 0) {
            ::close(fd);
            throw std::system_error(error, std::generic_category(), "connect");
        }
    }

    TcpStream stream(loop, fd);
    stream.set_nodelay(true);
    co_return stream;
}

TcpListener::TcpListener(EventLoop &loop, std::uint16_t port, int backlog)
    : loop_(&loop)
{
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0)
        throw_errno("socket");

    set_nonblocking(fd_);
    set_cloexec(fd_);

    int reuse = 1;
    (void)::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::bind(fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        const int error = errno;
        ::close(fd_);
        fd_ = -1;
        throw std::system_error(error, std::generic_category(), "bind");
    }
    if (::listen(fd_, backlog) != 0) {
        const int error = errno;
        ::close(fd_);
        fd_ = -1;
        throw std::system_error(error, std::generic_category(), "listen");
    }

    socklen_t length = sizeof(address);
    if (::getsockname(fd_, reinterpret_cast<sockaddr *>(&address), &length) == 0)
        port_ = ntohs(address.sin_port);
}

TcpListener::~TcpListener()
{
    close();
}

void TcpListener::close() noexcept
{
    if (fd_ >= 0) {
        loop_->del_io(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

Task<std::pair<int, sockaddr_storage>> TcpListener::accept()
{
    for (;;) {
        sockaddr_storage address{};
        socklen_t length = sizeof(address);
        const int client = accept_nonblocking(fd_, reinterpret_cast<sockaddr *>(&address), &length);
        if (client >= 0)
            co_return std::pair<int, sockaddr_storage>{ client, address };
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            throw_errno("accept");
        (void)co_await IoAwaiter(*loop_, fd_, kIoRead, {});
    }
}

} // namespace realurl::detail
