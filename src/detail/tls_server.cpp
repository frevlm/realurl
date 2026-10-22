#include "realurl/detail/tls_server.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>

namespace realurl::detail
{
namespace
{

void describe_error(std::string &message)
{
    message = "tls server handshake failed";
    char buffer[256] = {};
    ERR_error_string_n(ERR_peek_last_error(), buffer, sizeof(buffer));
    if (buffer[0] != '\0')
        message.append(": ").append(buffer);
}

int select_alpn(SSL *, const unsigned char **out, unsigned char *out_len,
                const unsigned char *in, unsigned int in_len, void *)
{
    const unsigned char *h2 = nullptr;
    const unsigned char *http11 = nullptr;

    for (unsigned int i = 0; i < in_len;) {
        const unsigned int length = in[i++];
        if (length == 0 || i + length > in_len)
            break;
        if (length == 2 && in[i] == 'h' && in[i + 1] == '2' && h2 == nullptr)
            h2 = in + i;
        if (length == 8 && std::memcmp(in + i, "http/1.1", 8) == 0 && http11 == nullptr)
            http11 = in + i;
        i += length;
    }

    if (h2 != nullptr) {
        *out = h2;
        *out_len = 2;
        return SSL_TLSEXT_ERR_OK;
    }
    if (http11 != nullptr) {
        *out = http11;
        *out_len = 8;
        return SSL_TLSEXT_ERR_OK;
    }
    return SSL_TLSEXT_ERR_NOACK;
}

} // namespace

std::shared_ptr<TlsServerContext> TlsServerContext::create(std::string cert_file,
                                                           std::string key_file, bool enable_ktls)
{
    static std::once_flag once;
    std::call_once(once, [] { OPENSSL_init_ssl(0, nullptr); });

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (ctx == nullptr)
        throw std::runtime_error("SSL_CTX_new(server) failed");

    if (SSL_CTX_use_certificate_chain_file(ctx, cert_file.c_str()) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        std::string message;
        describe_error(message);
        SSL_CTX_free(ctx);
        throw std::runtime_error(message);
    }

    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_alpn_select_cb(ctx, select_alpn, nullptr);
    SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_SERVER);
    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    (void)SSL_CTX_set_cipher_list(ctx, "ECDHE+AESGCM:ECDHE+CHACHA20");
    SSL_CTX_set_num_tickets(ctx, 2);

    bool ktls_requested = false;
#if defined(__linux__) && defined(SSL_OP_ENABLE_KTLS)
    if (enable_ktls) {
        SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS);
#ifdef SSL_OP_ENABLE_KTLS_TX_ZEROCOPY_SENDFILE
        SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS_TX_ZEROCOPY_SENDFILE);
#endif
        ktls_requested = true;
    }
#else
    (void)enable_ktls;
#endif

    return std::shared_ptr<TlsServerContext>(new TlsServerContext(ctx, ktls_requested));
}

TlsServerContext::~TlsServerContext()
{
    if (ctx_ != nullptr)
        SSL_CTX_free(ctx_);
}

TlsServerStream::TlsServerStream(TcpStream socket, ServerSSL *ssl, std::string alpn)
    : socket_(std::move(socket))
    , ssl_(ssl)
    , alpn_(std::move(alpn))
{
    refresh_ktls();
}

TlsServerStream::~TlsServerStream()
{
    if (ssl_ != nullptr)
        SSL_free(ssl_);
}

void TlsServerStream::refresh_ktls() noexcept
{
#if defined(__linux__) && OPENSSL_VERSION_NUMBER >= 0x30000000L
    if (ssl_ != nullptr) {
        ktls_send_ = BIO_get_ktls_send(SSL_get_wbio(ssl_)) != 0;
        ktls_recv_ = BIO_get_ktls_recv(SSL_get_rbio(ssl_)) != 0;
    }
#endif
}

Task<std::unique_ptr<Transport>> TlsServerStream::accept(
    EventLoop &, TcpStream &&socket, const std::shared_ptr<TlsServerContext> &context,
    std::chrono::milliseconds timeout)
{
    SSL *ssl = SSL_new(context->native());
    if (ssl == nullptr)
        throw std::runtime_error("SSL_new failed");

    if (SSL_set_fd(ssl, socket.fd()) != 1) {
        SSL_free(ssl);
        throw std::runtime_error("SSL_set_fd failed");
    }
    SSL_set_accept_state(ssl);

    for (;;) {
        const int rc = SSL_accept(ssl);
        if (rc == 1)
            break;

        const int error = SSL_get_error(ssl, rc);
        if (error == SSL_ERROR_WANT_READ) {
            co_await socket.wait_readable(timeout);
        } else if (error == SSL_ERROR_WANT_WRITE) {
            co_await socket.wait_writable(timeout);
        } else {
            std::string message;
            describe_error(message);
            SSL_free(ssl);
            throw std::runtime_error(message);
        }
    }

    const unsigned char *selected = nullptr;
    unsigned int selected_len = 0;
    SSL_get0_alpn_selected(ssl, &selected, &selected_len);
    std::string alpn;
    if (selected != nullptr && selected_len != 0)
        alpn.assign(reinterpret_cast<const char *>(selected), selected_len);

    co_return std::unique_ptr<Transport>(new TlsServerStream(std::move(socket), ssl, std::move(alpn)));
}

Task<std::size_t> TlsServerStream::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    if (buffer.empty())
        co_return 0;

    for (;;) {
        const int n = SSL_read(ssl_, buffer.data(), static_cast<int>(std::min<std::size_t>(
                                                       buffer.size(), static_cast<std::size_t>(INT_MAX))));
        if (n > 0) {
            refresh_ktls();
            co_return static_cast<std::size_t>(n);
        }

        const int error = SSL_get_error(ssl_, n);
        if (error == SSL_ERROR_WANT_READ) {
            co_await socket_.wait_readable(idle);
            continue;
        }
        if (error == SSL_ERROR_WANT_WRITE) {
            co_await socket_.wait_writable(idle);
            continue;
        }
        if (error == SSL_ERROR_ZERO_RETURN || (error == SSL_ERROR_SYSCALL && n == 0))
            co_return 0;

        throw std::system_error(ECONNRESET, std::generic_category(), "tls server read");
    }
}

Task<void> TlsServerStream::write_all(std::string_view data, std::chrono::milliseconds timeout)
{
    while (!data.empty()) {
        const int chunk = static_cast<int>(
            std::min<std::size_t>(data.size(), static_cast<std::size_t>(INT_MAX)));
        const int n = SSL_write(ssl_, data.data(), chunk);
        if (n > 0) {
            refresh_ktls();
            data.remove_prefix(static_cast<std::size_t>(n));
            continue;
        }

        const int error = SSL_get_error(ssl_, n);
        if (error == SSL_ERROR_WANT_READ) {
            co_await socket_.wait_readable(timeout);
        } else if (error == SSL_ERROR_WANT_WRITE) {
            co_await socket_.wait_writable(timeout);
        } else {
            throw std::system_error(ECONNRESET, std::generic_category(), "tls server write");
        }
    }
    co_return;
}

Task<void> TlsServerStream::shutdown_write()
{
    if (ssl_ != nullptr) {
        const int rc = SSL_shutdown(ssl_);
        (void)rc;
    }
    co_await socket_.shutdown_write();
}

void TlsServerStream::close() noexcept
{
    socket_.close();
}

} // namespace realurl::detail
