#pragma once

/// Completion-based TCP transport for h2c on Linux.
///
/// Recv uses IORING_OP_RECV + IORING_RECV_MULTISHOT with a provided-buffer
/// ring when the kernel supports it; otherwise it falls back to one-shot recv.
/// Send remains one SQE per write. TLS stays on the readiness-based
/// TcpStream/TlsServerStream path.

#ifdef REALURL_WITH_IO_URING

#include "realurl/detail/socket.hpp"
#include "uring.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>

namespace realurl::detail
{

struct RecvState;

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
    UringTransport(Uring &ring, int fd) noexcept;

    Uring &ring_;
    int fd_ = -1;
    std::shared_ptr<RecvState> recv_;
    bool multishot_ = false;
};

} // namespace realurl::detail

#endif
