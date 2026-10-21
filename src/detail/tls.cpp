#include "realurl/detail/tls.hpp"

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>

#include <mutex>

#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>

namespace realurl::detail
{
namespace
{

void describe_last_error(std::string &message)
{
    message = "tls handshake failed";
    char buffer[256] = {};
    ERR_error_string_n(ERR_peek_last_error(), buffer, sizeof(buffer));
    if (buffer[0] != '\0')
        message.append(": ").append(buffer);
}

class Wants {
public:
    explicit Wants(int code) noexcept
        : code_(code)
    {
    }

    bool read() const noexcept
    {
        return code_ == SSL_ERROR_WANT_READ;
    }

    bool write() const noexcept
    {
        return code_ == SSL_ERROR_WANT_WRITE;
    }

private:
    int code_;
};

} // namespace

std::shared_ptr<TlsContext> TlsContext::create(std::string_view ca_file)
{
    static std::once_flag once;
    std::call_once(once, [] { SSL_library_init(); });

    SSL_CTX *ctx = SSL_CTX_new(TLS_method());
    if (ctx == nullptr)
        throw std::runtime_error("SSL_CTX_new failed");

    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);

    if (!ca_file.empty()) {
        if (SSL_CTX_load_verify_locations(ctx, std::string(ca_file).c_str(), nullptr) != 1) {
            SSL_CTX_free(ctx);
            throw std::runtime_error("SSL_CTX_load_verify_locations failed");
        }
    } else {
        SSL_CTX_set_default_verify_paths(ctx);
    }

    // Client: only h2.
    static const unsigned char kAlpnProtocols[] = { 2, 'h', '2' };
    SSL_CTX_set_alpn_protos(ctx, kAlpnProtocols, sizeof(kAlpnProtocols));

    return std::shared_ptr<TlsContext>(new TlsContext(ctx));
}

TlsContext::~TlsContext()
{
    if (ctx_ != nullptr)
        SSL_CTX_free(ctx_);
}

TlsStream::TlsStream(TcpStream socket, SSL *ssl)
    : socket_(std::move(socket))
    , ssl_(ssl)
{
}

TlsStream::~TlsStream()
{
    if (ssl_ != nullptr)
        SSL_free(ssl_);
}

Task<std::unique_ptr<Transport>> TlsStream::connect(EventLoop &loop, const SocketAddress &address,
                                                    std::string_view server_name,
                                                    const std::shared_ptr<TlsContext> &context,
                                                    std::chrono::milliseconds timeout)
{
    TcpStream socket = co_await TcpStream::connect(loop, reinterpret_cast<const sockaddr *>(&address.storage),
                                                   address.length, timeout);

    SSL *ssl = SSL_new(context->native());
    if (ssl == nullptr)
        throw std::runtime_error("SSL_new failed");

    SSL_set_fd(ssl, socket.fd());
    SSL_set_tlsext_host_name(ssl, std::string(server_name).c_str());
    SSL_set1_host(ssl, std::string(server_name).c_str());
    SSL_set_connect_state(ssl);

    for (;;) {
        const int rc = SSL_connect(ssl);
        if (rc == 1)
            break;

        const Wants wants(SSL_get_error(ssl, rc));
        if (wants.read()) {
            co_await socket.wait_readable(timeout);
        } else if (wants.write()) {
            co_await socket.wait_writable(timeout);
        } else {
            std::string message;
            describe_last_error(message);
            const long verify = SSL_get_verify_result(ssl);
            if (verify != X509_V_OK)
                message.append(": certificate verification failed");
            SSL_free(ssl);
            throw std::runtime_error(message);
        }
    }

    const unsigned char *selected = nullptr;
    unsigned int selected_len = 0;
    SSL_get0_alpn_selected(ssl, &selected, &selected_len);
    const bool h2 = selected != nullptr && selected_len == 2 && selected[0] == 'h' && selected[1] == '2';
    if (!h2) {
        SSL_free(ssl);
        throw std::runtime_error("TLS server did not negotiate h2");
    }

    co_return std::unique_ptr<Transport>(new TlsStream(std::move(socket), ssl));
}

Task<std::size_t> TlsStream::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    for (;;) {
        const int n = SSL_read(ssl_, buffer.data(), static_cast<int>(buffer.size()));
        if (n > 0)
            co_return static_cast<std::size_t>(n);

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

        throw std::system_error(ECONNRESET, std::generic_category(), "tls read");
    }
}

Task<void> TlsStream::write_all(std::string_view data, std::chrono::milliseconds timeout)
{
    while (!data.empty()) {
        const int n = SSL_write(ssl_, data.data(), static_cast<int>(data.size()));
        if (n > 0) {
            data.remove_prefix(static_cast<std::size_t>(n));
            continue;
        }

        const Wants wants(SSL_get_error(ssl_, n));
        if (wants.read()) {
            co_await socket_.wait_readable(timeout);
        } else if (wants.write()) {
            co_await socket_.wait_writable(timeout);
        } else {
            throw std::system_error(ECONNRESET, std::generic_category(), "tls write");
        }
    }
    co_return;
}

Task<void> TlsStream::shutdown_write()
{
    co_await socket_.shutdown_write();
}

void TlsStream::close() noexcept
{
    socket_.close();
}

} // namespace realurl::detail
