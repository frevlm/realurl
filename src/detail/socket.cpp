#include "realurl/detail/socket.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>

#include <fcntl.h>
#include <netdb.h>
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

void set_nosigpipe(int fd) noexcept
{
#ifdef SO_NOSIGPIPE
    const int on = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#else
    (void)fd;
#endif
}

int accept_nonblocking(int fd, sockaddr *address, socklen_t *length)
{
#ifdef __linux__
    const int client = ::accept4(fd, address, length, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    const int client = ::accept(fd, address, length);
    if (client >= 0) {
        set_nonblocking(client);
        set_cloexec(client);
    }
#endif
    if (client >= 0)
        set_nosigpipe(client);
    return client;
}

int bind_listener(const ListenerOptions &options, std::uint16_t &port, int &family)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    const bool wildcard = options.host.empty() || options.host == "*";
    const std::string service = std::to_string(options.port);

    addrinfo *results = nullptr;
    const int rc = ::getaddrinfo(wildcard ? nullptr : options.host.c_str(), service.c_str(), &hints, &results);
    if (rc != 0)
        throw std::runtime_error(std::string("getaddrinfo: ") + ::gai_strerror(rc));

    int last_error = 0;
    int listener = -1;
    for (addrinfo *entry = results; entry != nullptr; entry = entry->ai_next) {
        const int fd = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (fd < 0) {
            last_error = errno;
            continue;
        }

        set_nonblocking(fd);
        set_cloexec(fd);

        int reuse = 1;
        (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

#ifdef SO_REUSEPORT
        if (options.reuse_port) {
            if (::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) != 0) {
                last_error = errno;
                ::close(fd);
                continue;
            }
        }
#endif

        if (entry->ai_family == AF_INET6) {
            const int v6only = options.dual_stack ? 0 : 1;
            (void)::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only));
        }

#ifdef __linux__
        if (options.defer_accept) {
#ifdef TCP_DEFER_ACCEPT
            (void)::setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &reuse, sizeof(reuse));
#endif
        }
        if (options.tcp_fastopen) {
#ifdef TCP_FASTOPEN
            const int qlen = options.backlog;
            (void)::setsockopt(fd, IPPROTO_TCP, TCP_FASTOPEN, &qlen, sizeof(qlen));
#endif
        }
#endif

        if (::bind(fd, entry->ai_addr, static_cast<socklen_t>(entry->ai_addrlen)) != 0) {
            last_error = errno;
            ::close(fd);
            continue;
        }
        if (::listen(fd, options.backlog) != 0) {
            last_error = errno;
            ::close(fd);
            continue;
        }

        sockaddr_storage bound{};
        socklen_t bound_length = sizeof(bound);
        if (::getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &bound_length) == 0) {
            port = bound.ss_family == AF_INET6
                ? ntohs(reinterpret_cast<sockaddr_in6 *>(&bound)->sin6_port)
                : ntohs(reinterpret_cast<sockaddr_in *>(&bound)->sin_port);
        } else {
            port = options.port;
        }
        family = entry->ai_family;
        listener = fd;
        break;
    }

    ::freeaddrinfo(results);

    if (listener < 0) {
        throw std::system_error(last_error != 0 ? last_error : EADDRNOTAVAIL, std::generic_category(),
                                "bind");
    }
    return listener;
}

} // namespace

/// One parked socket wait. The fd stays registered with the loop for its whole
/// lifetime; this object only tells the owning stream/listener which coroutine
/// to resume when the edge arrives.
class IoWaiter {
public:
    IoWaiter(EventLoop &loop, TcpStream &stream, std::uint32_t events, std::chrono::milliseconds timeout)
        : loop_(&loop)
        , kind_(Kind::Stream)
        , stream_(&stream)
        , events_(events)
        , timeout_(timeout)
    {
    }

    IoWaiter(EventLoop &loop, TcpListener &listener, std::chrono::milliseconds timeout)
        : loop_(&loop)
        , kind_(Kind::Listener)
        , listener_(&listener)
        , events_(kIoRead)
        , timeout_(timeout)
    {
    }

    ~IoWaiter()
    {
        if (done_)
            return;
        if (kind_ == Kind::Stream)
            stream_->stop_wait(events_, this);
        else
            listener_->stop_wait(this);
        loop_->cancel(timer_);
    }

    static void on_timeout(void *context)
    {
        static_cast<IoWaiter *>(context)->complete(0, ETIMEDOUT);
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        if (timeout_.count() > 0)
            loop_->schedule(timer_, timeout_, &IoWaiter::on_timeout, this);
        if (kind_ == Kind::Stream)
            stream_->start_wait(events_, this);
        else
            listener_->start_wait(this);
    }

    std::uint32_t await_resume()
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "socket wait");
        return revents_;
    }

    void complete(std::uint32_t events, int error)
    {
        if (done_)
            return;
        done_ = true;
        revents_ = events;
        error_ = error;
        if (kind_ == Kind::Stream)
            stream_->stop_wait(events_, this);
        else
            listener_->stop_wait(this);
        loop_->cancel(timer_);
        loop_->resume_later(handle_);
    }

private:
    enum class Kind { Stream, Listener };

    EventLoop *loop_;
    Kind kind_;
    TcpStream *stream_ = nullptr;
    TcpListener *listener_ = nullptr;
    std::uint32_t events_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerNode timer_;
    std::uint32_t revents_ = 0;
    int error_ = 0;
    bool done_ = false;
};

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
    if (fd_ >= 0)
        set_nosigpipe(fd_);
    register_io();
}

TcpStream::~TcpStream()
{
    close();
}

TcpStream::TcpStream(TcpStream &&other) noexcept
    : loop_(other.loop_)
    , fd_(std::exchange(other.fd_, -1))
{
    other.read_waiter_ = nullptr;
    other.write_waiter_ = nullptr;
    register_io();
}

TcpStream &TcpStream::operator=(TcpStream &&other) noexcept
{
    if (this != &other) {
        close();
        loop_ = other.loop_;
        fd_ = std::exchange(other.fd_, -1);
        other.read_waiter_ = nullptr;
        other.write_waiter_ = nullptr;
        register_io();
    }
    return *this;
}

void TcpStream::register_io() noexcept
{
    if (fd_ >= 0 && loop_ != nullptr)
        loop_->add_io(fd_, kIoRead | kIoWrite, &TcpStream::on_io, this);
}

void TcpStream::unregister_io() noexcept
{
    if (fd_ >= 0 && loop_ != nullptr)
        loop_->del_io(fd_);
    read_waiter_ = nullptr;
    write_waiter_ = nullptr;
}

void TcpStream::on_io(void *ctx, std::uint32_t events)
{
    static_cast<TcpStream *>(ctx)->handle_io(events);
}

void TcpStream::handle_io(std::uint32_t events)
{
    if ((events & (kIoRead | kIoError)) != 0)
        readable_ = true;
    if (read_waiter_ != nullptr && (events & (kIoRead | kIoError)) != 0)
        read_waiter_->complete(events, 0);
    if (write_waiter_ != nullptr && (events & (kIoWrite | kIoError)) != 0)
        write_waiter_->complete(events, 0);
}

void TcpStream::start_wait(std::uint32_t events, IoWaiter *waiter)
{
    if (fd_ < 0) {
        waiter->complete(kIoError, EBADF);
        return;
    }
    if ((events & kIoRead) != 0)
        read_waiter_ = waiter;
    if ((events & kIoWrite) != 0)
        write_waiter_ = waiter;
}

void TcpStream::stop_wait(std::uint32_t events, IoWaiter *waiter) noexcept
{
    if ((events & kIoRead) != 0 && read_waiter_ == waiter)
        read_waiter_ = nullptr;
    if ((events & kIoWrite) != 0 && write_waiter_ == waiter)
        write_waiter_ = nullptr;
}

void TcpStream::close() noexcept
{
    if (fd_ >= 0) {
        unregister_io();
        ::close(fd_);
        fd_ = -1;
    }
}

void TcpStream::set_nodelay(bool enabled) noexcept
{
    const int value = enabled ? 1 : 0;
    (void)::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
}

int TcpStream::release() noexcept
{
    if (fd_ < 0)
        return -1;
    unregister_io();
    const int fd = fd_;
    fd_ = -1;
    return fd;
}

Task<std::size_t> TcpStream::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    for (;;) {
        if (!readable_)
            (void)co_await IoWaiter(*loop_, *this, kIoRead, idle);
        const ssize_t n = ::recv(fd_, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            // A stream recv returns everything queued up to the buffer size,
            // so a short read means the socket is drained.
            readable_ = static_cast<std::size_t>(n) == buffer.size();
            co_return static_cast<std::size_t>(n);
        }
        if (n == 0)
            co_return 0;
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            throw_errno("recv");
        readable_ = false;
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
            (void)co_await IoWaiter(*loop_, *this, kIoWrite, timeout);
            continue;
        }
        throw_errno("send");
    }
}

Task<void> TcpStream::wait_readable(std::chrono::milliseconds timeout)
{
    (void)co_await IoWaiter(*loop_, *this, kIoRead, timeout);
}

Task<void> TcpStream::wait_writable(std::chrono::milliseconds timeout)
{
    (void)co_await IoWaiter(*loop_, *this, kIoWrite, timeout);
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
    set_nosigpipe(fd);

    TcpStream stream(loop, fd);

    if (::connect(fd, address, length) != 0) {
        if (errno != EINPROGRESS && errno != EWOULDBLOCK) {
            const int error = errno;
            stream.close();
            throw std::system_error(error, std::generic_category(), "connect");
        }

        try {
            (void)co_await stream.wait_writable(timeout);
        } catch (...) {
            stream.close();
            throw;
        }

        int error = 0;
        socklen_t error_length = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_length) != 0) {
            const int saved = errno;
            stream.close();
            throw std::system_error(saved, std::generic_category(), "getsockopt");
        }
        if (error != 0) {
            stream.close();
            throw std::system_error(error, std::generic_category(), "connect");
        }
    }

    stream.set_nodelay(true);
    co_return stream;
}

TcpListener::TcpListener(EventLoop &loop, const ListenerOptions &options)
    : loop_(&loop)
{
    fd_ = bind_listener(options, port_, family_);
    register_io();
}

TcpListener::TcpListener(EventLoop &loop, std::uint16_t port, int backlog)
    : TcpListener(loop, ListenerOptions{ "127.0.0.1", port, backlog })
{
}

TcpListener::~TcpListener()
{
    close();
}

void TcpListener::register_io() noexcept
{
    if (fd_ >= 0 && loop_ != nullptr)
        loop_->add_io(fd_, kIoRead, &TcpListener::on_io, this);
}

void TcpListener::unregister_io() noexcept
{
    if (fd_ >= 0 && loop_ != nullptr)
        loop_->del_io(fd_);
    waiter_ = nullptr;
}

void TcpListener::on_io(void *ctx, std::uint32_t events)
{
    static_cast<TcpListener *>(ctx)->handle_io(events);
}

void TcpListener::handle_io(std::uint32_t events)
{
    if (waiter_ != nullptr && (events & (kIoRead | kIoError)) != 0)
        waiter_->complete(events, 0);
}

void TcpListener::start_wait(IoWaiter *waiter)
{
    if (fd_ < 0) {
        waiter->complete(kIoError, EBADF);
        return;
    }
    waiter_ = waiter;
}

void TcpListener::stop_wait(IoWaiter *waiter) noexcept
{
    if (waiter_ == waiter)
        waiter_ = nullptr;
}

void TcpListener::close() noexcept
{
    if (fd_ >= 0) {
        unregister_io();
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
        (void)co_await IoWaiter(*loop_, *this, {});
    }
}

} // namespace realurl::detail
