#pragma once

/// TLS 1.3 stream built on BoringSSL. The BoringSSL types stay inside
/// src/detail/tls.cpp; this header only forwards them.

#include "realurl/detail/resolver.hpp"
#include "realurl/detail/socket.hpp"

#include <memory>
#include <span>
#include <string>
#include <string_view>

struct ssl_ctx_st;
struct ssl_st;

namespace realurl::detail
{

using SSL_CTX = ::ssl_ctx_st;
using SSL = ::ssl_st;

/// Process-wide TLS configuration. One instance is shared by every loop
/// thread; SSL_CTX is thread-safe, SSL objects are not.
class TlsContext {
public:
    /// `ca_file` may be empty, in which case the system trust store is used.
    /// `offer_native` adds realurl/1 to the ALPN list ahead of h2.
    /// Throws std::runtime_error when the context cannot be created.
    static std::shared_ptr<TlsContext> create(std::string_view ca_file, bool offer_native);

    ~TlsContext();

    TlsContext(const TlsContext &) = delete;
    TlsContext &operator=(const TlsContext &) = delete;

    SSL_CTX *native() const noexcept
    {
        return ctx_;
    }

private:
    explicit TlsContext(SSL_CTX *ctx) noexcept
        : ctx_(ctx)
    {
    }

    SSL_CTX *ctx_;
};

/// TLS stream: a TcpStream plus one SSL object. Only the connection's reader
/// and writer coroutines touch it, both on the owning loop thread.
class TlsStream : public Transport {
public:
    /// Connects, performs the TLS handshake, and requires ALPN to select h2.
    static Task<std::unique_ptr<Transport>> connect(EventLoop &loop, const SocketAddress &address,
                                                     std::string_view server_name,
                                                     const std::shared_ptr<TlsContext> &context,
                                                     std::chrono::milliseconds timeout);

    ~TlsStream() override;

    Task<std::size_t> read(std::span<char> buffer, std::chrono::milliseconds idle = {}) override;
    Task<void> write_all(std::string_view data, std::chrono::milliseconds timeout = {}) override;
    Task<void> shutdown_write() override;

    int fd() const noexcept override
    {
        return socket_.fd();
    }

    void close() noexcept override;

    std::string_view alpn() const noexcept override
    {
        return alpn_;
    }

private:
    TlsStream(TcpStream socket, SSL *ssl, std::string alpn);

    TcpStream socket_;
    SSL *ssl_ = nullptr;
    std::string alpn_;
};

} // namespace realurl::detail
