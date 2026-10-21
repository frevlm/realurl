#pragma once

#include "realurl/detail/event_loop.hpp"
#include "realurl/detail/transport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <utility>

namespace realurl::detail
{

/// Non-blocking TCP stream bound to one event loop. All methods must be called
/// from that loop's thread.
class TcpStream : public Transport {
public:
    TcpStream(EventLoop &loop, int fd) noexcept;
    ~TcpStream() override;

    TcpStream(TcpStream &&other) noexcept;
    TcpStream &operator=(TcpStream &&other) noexcept;
    TcpStream(const TcpStream &) = delete;
    TcpStream &operator=(const TcpStream &) = delete;

    int fd() const noexcept override
    {
        return fd_;
    }

    EventLoop &loop() noexcept
    {
        return *loop_;
    }

    /// Returns bytes read, or 0 on EOF. Throws std::system_error on failure;
    /// errno == ETIMEDOUT when idle elapses.
    Task<std::size_t> read(std::span<char> buffer, std::chrono::milliseconds idle = {}) override;

    /// Writes the whole buffer. Each wait is bounded by timeout; errno ==
    /// ETIMEDOUT when it elapses.
    Task<void> write_all(std::string_view data, std::chrono::milliseconds timeout = {}) override;

    Task<void> shutdown_write() override;

    void close() noexcept override;
    void set_nodelay(bool enabled) noexcept;

    Task<void> wait_readable(std::chrono::milliseconds timeout = {});
    Task<void> wait_writable(std::chrono::milliseconds timeout = {});

    /// Non-blocking connect. timeout applies to the connect wait.
    static Task<TcpStream> connect(EventLoop &loop, const sockaddr *address, socklen_t length,
                                   std::chrono::milliseconds timeout);

private:
    EventLoop *loop_ = nullptr;
    int fd_ = -1;
};

class TcpListener {
public:
    TcpListener(EventLoop &loop, std::uint16_t port, int backlog = 128);
    ~TcpListener();

    TcpListener(const TcpListener &) = delete;
    TcpListener &operator=(const TcpListener &) = delete;

    Task<std::pair<int, sockaddr_storage>> accept();

    std::uint16_t port() const noexcept
    {
        return port_;
    }

    int fd() const noexcept
    {
        return fd_;
    }

    void close() noexcept;

private:
    EventLoop *loop_ = nullptr;
    int fd_ = -1;
    std::uint16_t port_ = 0;
};

void set_nonblocking(int fd);
void set_cloexec(int fd);

} // namespace realurl::detail
