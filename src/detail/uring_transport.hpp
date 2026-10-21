#pragma once

/// Completion-based TCP transport for h2c on Linux. TLS stays on the
/// readiness-based TcpStream/TlsStream path because BoringSSL owns the fd.
///
/// The fd is switched to blocking mode and every recv/send is submitted to the
/// loop's io_uring with IOSQE_ASYNC; completions wake the loop through the
/// ring's eventfd registered with the poller.

#ifdef REALURL_WITH_IO_URING

#include "realurl/detail/socket.hpp"
#include "uring.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>

namespace realurl::detail
{

class UringTransport final : public Transport {
public:
    /// Takes ownership of fd. Returns nullptr when the loop has no usable ring.
    static std::unique_ptr<Transport> create(EventLoop &loop, int fd) noexcept;

    ~UringTransport() override;

    Task<std::size_t> read(std::span<char> buffer, std::chrono::milliseconds idle = {}) override;
    Task<void> write_all(std::string_view data, std::chrono::milliseconds timeout = {}) override;
    Task<void> shutdown_write() override;

    int fd() const noexcept override
    {
        return fd_;
    }

    void close() noexcept override;

private:
    UringTransport(EventLoop &loop, Uring &ring, int fd) noexcept;

    EventLoop &loop_;
    Uring &ring_;
    int fd_ = -1;
};

} // namespace realurl::detail

#endif
