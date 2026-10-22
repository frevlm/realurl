#pragma once

/// Server-side TLS 1.2/1.3 transport on the realurl EventLoop.
///
/// OpenSSL owns the handshake and record layer. On Linux the context enables
/// SSL_OP_ENABLE_KTLS; whether the kernel actually takes over is reported by
/// ktls_send()/ktls_recv() and checked again after the first read/write.

#include "realurl/detail/socket.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>

struct ssl_ctx_st;
struct ssl_st;

namespace realurl::detail
{

using ServerSSL_CTX = ::ssl_ctx_st;
using ServerSSL = ::ssl_st;

class TlsServerContext {
public:
    static std::shared_ptr<TlsServerContext> create(std::string cert_file, std::string key_file,
                                                    bool enable_ktls, bool native_enabled);
    ~TlsServerContext();

    TlsServerContext(const TlsServerContext &) = delete;
    TlsServerContext &operator=(const TlsServerContext &) = delete;

    ServerSSL_CTX *native() const noexcept
    {
        return ctx_;
    }

    bool ktls_requested() const noexcept
    {
        return ktls_requested_;
    }

private:
    TlsServerContext(ServerSSL_CTX *ctx, bool ktls_requested, bool native_enabled) noexcept
        : ctx_(ctx)
        , ktls_requested_(ktls_requested)
        , native_enabled_(native_enabled)
    {
    }

    ServerSSL_CTX *ctx_;
    bool ktls_requested_ = false;
    bool native_enabled_ = true;
};

class TlsServerStream final : public Transport {
public:
    static Task<std::unique_ptr<Transport>> accept(EventLoop &loop, TcpStream &&socket,
                                                   const std::shared_ptr<TlsServerContext> &context,
                                                   std::chrono::milliseconds timeout);

    ~TlsServerStream() override;

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

    bool ktls_send() const noexcept
    {
        return ktls_send_;
    }

    bool ktls_recv() const noexcept
    {
        return ktls_recv_;
    }

private:
    TlsServerStream(TcpStream socket, ServerSSL *ssl, std::string alpn);

    void refresh_ktls() noexcept;

    TcpStream socket_;
    ServerSSL *ssl_ = nullptr;
    std::string alpn_;
    bool ktls_send_ = false;
    bool ktls_recv_ = false;
};

} // namespace realurl::detail
