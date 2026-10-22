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

class TlsSessionCache;

/// Client TLS configuration, one per Client. SSL_CTX itself is thread-safe,
/// but the session cache is not locked: a TlsContext must only be used by the
/// loop thread that owns its Client.
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

    /// Binds `ssl` to the cache key (host:port) and offers the cached session
    /// for that key, if any, for resumption.
    void prepare(SSL *ssl, std::string key) const;

private:
    TlsContext(SSL_CTX *ctx, std::unique_ptr<TlsSessionCache> cache) noexcept;

    SSL_CTX *ctx_;
    std::unique_ptr<TlsSessionCache> cache_;
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

    /// True when the handshake resumed a cached session.
    bool resumed() const noexcept
    {
        return resumed_;
    }

private:
    TlsStream(TcpStream socket, SSL *ssl, std::string alpn, bool resumed);

    TcpStream socket_;
    SSL *ssl_ = nullptr;
    std::string alpn_;
    bool resumed_ = false;
};

} // namespace realurl::detail
