#include "realurl/detail/tls.hpp"

#include "realurl/detail/protocol.hpp"

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>

#include <mutex>

#include <cstdint>
#include <cstring>
#include <iterator>
#include <list>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>

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

constexpr std::size_t kMaxCachedSessions = 256;

struct SslDeleter {
    void operator()(SSL *ssl) const noexcept
    {
        SSL_free(ssl);
    }
};

/// Owns an SSL until it is handed to a TlsStream, so a handshake that throws
/// or whose coroutine is destroyed still frees it.
using SslPtr = std::unique_ptr<SSL, SslDeleter>;

std::uint16_t port_of(const SocketAddress &address) noexcept
{
    const auto *base = reinterpret_cast<const sockaddr *>(&address.storage);
    if (base->sa_family == AF_INET)
        return ntohs(reinterpret_cast<const sockaddr_in *>(base)->sin_port);
    if (base->sa_family == AF_INET6)
        return ntohs(reinterpret_cast<const sockaddr_in6 *>(base)->sin6_port);
    return 0;
}

void free_cache_key(void *, void *data, CRYPTO_EX_DATA *, int, long, void *)
{
    delete static_cast<std::string *>(data);
}

/// SSL ex_data slot that carries the session cache key for one connection.
int cache_key_index()
{
    static const int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, free_cache_key);
    return index;
}

} // namespace

/// Most recent session per key, oldest evicted first. Owns one reference to
/// every stored SSL_SESSION. Not thread-safe.
class TlsSessionCache {
public:
    ~TlsSessionCache()
    {
        for (const Entry &entry : entries_)
            SSL_SESSION_free(entry.session);
    }

    /// Takes over the caller's reference to `session`.
    void put(const std::string &key, SSL_SESSION *session)
    {
        erase(key);
        entries_.push_back({ key, session });
        index_[key] = std::prev(entries_.end());
        if (entries_.size() > kMaxCachedSessions)
            erase(std::string(entries_.front().key));
    }

    /// Borrowed pointer, valid until the next put() or erase().
    SSL_SESSION *find(const std::string &key) const
    {
        const auto it = index_.find(key);
        return it == index_.end() ? nullptr : it->second->session;
    }

    void erase(const std::string &key)
    {
        const auto it = index_.find(key);
        if (it == index_.end())
            return;
        SSL_SESSION_free(it->second->session);
        entries_.erase(it->second);
        index_.erase(it);
    }

private:
    struct Entry {
        std::string key;
        SSL_SESSION *session;
    };

    std::list<Entry> entries_;
    std::unordered_map<std::string, std::list<Entry>::iterator> index_;
};

namespace
{

/// Runs on the loop thread from SSL_connect/SSL_read. TLS 1.3 tickets arrive
/// after the handshake, so this fires during the first reads. Returning 1
/// hands our reference to the cache.
int store_session(SSL *ssl, SSL_SESSION *session)
{
    auto *cache = static_cast<TlsSessionCache *>(SSL_CTX_get_app_data(SSL_get_SSL_CTX(ssl)));
    const auto *key = static_cast<const std::string *>(SSL_get_ex_data(ssl, cache_key_index()));
    if (cache == nullptr || key == nullptr || SSL_SESSION_is_resumable(session) != 1)
        return 0;
    cache->put(*key, session);
    return 1;
}

} // namespace

TlsContext::TlsContext(SSL_CTX *ctx, std::unique_ptr<TlsSessionCache> cache) noexcept
    : ctx_(ctx)
    , cache_(std::move(cache))
{
}

std::shared_ptr<TlsContext> TlsContext::create(std::string_view ca_file, bool offer_native)
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

    if (offer_native) {
        static const unsigned char kAlpnNative[] = {
            9, 'r', 'e', 'a', 'l', 'u', 'r', 'l', '/', '1', 2, 'h', '2',
        };
        SSL_CTX_set_alpn_protos(ctx, kAlpnNative, sizeof(kAlpnNative));
    } else {
        static const unsigned char kAlpnH2[] = { 2, 'h', '2' };
        SSL_CTX_set_alpn_protos(ctx, kAlpnH2, sizeof(kAlpnH2));
    }

    auto cache = std::make_unique<TlsSessionCache>();
    SSL_CTX_set_app_data(ctx, cache.get());
    SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
    SSL_CTX_sess_set_new_cb(ctx, store_session);

    return std::shared_ptr<TlsContext>(new TlsContext(ctx, std::move(cache)));
}

TlsContext::~TlsContext()
{
    if (ctx_ == nullptr)
        return;
    // Live SSL objects keep the SSL_CTX alive; make sure they stop reaching
    // for the cache we are about to destroy.
    SSL_CTX_sess_set_new_cb(ctx_, nullptr);
    SSL_CTX_set_app_data(ctx_, nullptr);
    SSL_CTX_free(ctx_);
}

void TlsContext::prepare(SSL *ssl, std::string key) const
{
    SSL_SESSION *session = cache_->find(key);
    auto *owned = new std::string(std::move(key));
    if (SSL_set_ex_data(ssl, cache_key_index(), owned) != 1) {
        delete owned;
        return;
    }
    if (session == nullptr)
        return;
    if (SSL_SESSION_is_resumable(session) == 1)
        SSL_set_session(ssl, session);
    else
        cache_->erase(*owned);
}

TlsStream::TlsStream(TcpStream socket, SSL *ssl, std::string alpn, bool resumed)
    : socket_(std::move(socket))
    , ssl_(ssl)
    , alpn_(std::move(alpn))
    , resumed_(resumed)
{
}

TlsStream::~TlsStream()
{
    if (ssl_ == nullptr)
        return;
    // SSL_free() invalidates the session of a connection that did not shut
    // down cleanly, which would make every cached ticket unusable. Dropping a
    // pooled connection without close_notify is routine, so mark it done.
    SSL_set_shutdown(ssl_, SSL_SENT_SHUTDOWN | SSL_RECEIVED_SHUTDOWN);
    SSL_free(ssl_);
}

Task<std::unique_ptr<Transport>> TlsStream::connect(EventLoop &loop, const SocketAddress &address,
                                                    std::string_view server_name,
                                                    const std::shared_ptr<TlsContext> &context,
                                                    std::chrono::milliseconds timeout)
{
    TcpStream socket = co_await TcpStream::connect(loop, reinterpret_cast<const sockaddr *>(&address.storage),
                                                   address.length, timeout);

    SslPtr ssl(SSL_new(context->native()));
    if (ssl == nullptr)
        throw std::runtime_error("SSL_new failed");

    SSL_set_fd(ssl.get(), socket.fd());
    SSL_set_tlsext_host_name(ssl.get(), std::string(server_name).c_str());
    SSL_set1_host(ssl.get(), std::string(server_name).c_str());
    SSL_set_connect_state(ssl.get());
    context->prepare(ssl.get(), std::string(server_name) + ':' + std::to_string(port_of(address)));

    for (;;) {
        const int rc = SSL_connect(ssl.get());
        if (rc == 1)
            break;

        const Wants wants(SSL_get_error(ssl.get(), rc));
        if (wants.read()) {
            co_await socket.wait_readable(timeout);
        } else if (wants.write()) {
            co_await socket.wait_writable(timeout);
        } else {
            std::string message;
            describe_last_error(message);
            const long verify = SSL_get_verify_result(ssl.get());
            if (verify != X509_V_OK)
                message.append(": certificate verification failed");
            throw std::runtime_error(message);
        }
    }

    const unsigned char *selected = nullptr;
    unsigned int selected_len = 0;
    SSL_get0_alpn_selected(ssl.get(), &selected, &selected_len);
    std::string alpn;
    if (selected != nullptr && selected_len != 0)
        alpn.assign(reinterpret_cast<const char *>(selected), selected_len);
    if (alpn != kNativeAlpn && alpn != "h2")
        throw std::runtime_error("TLS server did not negotiate realurl/1 or h2");

    const bool resumed = SSL_session_reused(ssl.get()) == 1;
    co_return std::unique_ptr<Transport>(new TlsStream(std::move(socket), ssl.release(), std::move(alpn), resumed));
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
