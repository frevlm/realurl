#include "realurl/http2.hpp"

#include "realurl/detail/connection.hpp"
#include "realurl/detail/resolver.hpp"
#include "realurl/detail/socket.hpp"
#include "realurl/detail/tls.hpp"
#include "realurl/detail/transport.hpp"
#include "realurl/detail/url.hpp"

#ifdef REALURL_WITH_IO_URING
#include "uring_transport.hpp"
#endif

#include <algorithm>
#include <cerrno>
#include <coroutine>
#include <cstring>
#include <exception>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace realurl
{
namespace detail
{
namespace
{

/// Parks a request until the in-flight connection attempt for its origin
/// finishes. Without this, a cold pool under concurrency opens one connection
/// per request.
class ConnectWaiter {
public:
    explicit ConnectWaiter(std::vector<std::coroutine_handle<>> &waiters) noexcept
        : waiters_(waiters)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        waiters_.push_back(handle);
    }

    void await_resume() const noexcept
    {
    }

private:
    std::vector<std::coroutine_handle<>> &waiters_;
};

} // namespace

class ClientImpl {
public:
    ClientImpl(EventLoop &loop, Config config)
        : loop_(&loop)
        , config_(std::move(config))
        , resolver_(loop)
        , tls_context_(TlsContext::create(config_.ca_file))
    {
    }

    Task<std::shared_ptr<H2Connection>> connection(const UrlParts &url, std::chrono::milliseconds timeout)
    {
        const std::string key = origin_key(url);

        OriginPool *pool = nullptr;
        for (;;) {
            pool = &pool_for(key);
            auto &list = pool->conns;
            for (auto it = list.begin(); it != list.end();) {
                if (!(*it)->usable())
                    it = list.erase(it);
                else
                    ++it;
            }

            std::shared_ptr<H2Connection> best_with_room;
            std::shared_ptr<H2Connection> best_overall;
            for (const auto &conn : list) {
                if (best_overall == nullptr || conn->active_streams() < best_overall->active_streams())
                    best_overall = conn;

                const std::size_t limit = config_.max_streams_per_conn > 0
                    ? std::min(conn->max_streams(), static_cast<std::size_t>(config_.max_streams_per_conn))
                    : conn->max_streams();
                if (conn->active_streams() < limit) {
                    if (best_with_room == nullptr || conn->active_streams() < best_with_room->active_streams())
                        best_with_room = conn;
                }
            }

            if (best_with_room != nullptr)
                co_return best_with_room;

            const int max_conns = std::max(1, config_.max_conns_per_origin);
            if (best_overall != nullptr && static_cast<int>(list.size()) >= max_conns)
                co_return best_overall;

            if (pool->connecting) {
                co_await ConnectWaiter(pool->waiters);
                if (pool->last_error)
                    std::rethrow_exception(pool->last_error);
                continue;
            }

            pool->connecting = true;
            pool->last_error = nullptr;
            break;
        }

        std::shared_ptr<H2Connection> conn;
        try {
            conn = co_await dial(url, timeout);
        } catch (...) {
            finish_attempt(*pool, nullptr, std::current_exception());
            throw;
        }
        finish_attempt(*pool, conn);
        co_return conn;
    }

    void close()
    {
        for (auto &entry : pools_) {
            for (const auto &conn : entry.second->conns)
                conn->close();
            entry.second->waiters.clear();
        }
        pools_.clear();
    }

    EventLoop &loop() noexcept
    {
        return *loop_;
    }

private:
    struct OriginPool {
        std::vector<std::shared_ptr<H2Connection>> conns;
        std::vector<std::coroutine_handle<>> waiters;
        std::exception_ptr last_error;
        bool connecting = false;
    };

    static std::string origin_key(const UrlParts &url)
    {
        std::string key;
        key.reserve(url.scheme.size() + url.host.size() + 16);
        key.append(url.scheme).append("://").append(url.authority());
        return key;
    }

    OriginPool &pool_for(const std::string &key)
    {
        auto it = pools_.find(key);
        if (it == pools_.end())
            it = pools_.emplace(key, std::make_unique<OriginPool>()).first;
        return *it->second;
    }

    void finish_attempt(OriginPool &pool, const std::shared_ptr<H2Connection> &conn,
                        std::exception_ptr error = {})
    {
        if (conn)
            pool.conns.push_back(conn);
        pool.connecting = false;
        pool.last_error = std::move(error);

        std::vector<std::coroutine_handle<>> waiters = std::move(pool.waiters);
        pool.waiters.clear();
        for (std::coroutine_handle<> handle : waiters)
            loop_->resume_later(handle);
    }

    Task<std::shared_ptr<H2Connection>> dial(const UrlParts &url, std::chrono::milliseconds timeout)
    {
        const std::string connect_host = config_.pinned_ip.empty() ? url.host : config_.pinned_ip;

        std::chrono::milliseconds connect_timeout = config_.connect_timeout;
        if (timeout.count() > 0)
            connect_timeout = std::min(timeout, config_.connect_timeout);

        const SocketAddress address = co_await resolver_.resolve(connect_host, url.port);

        std::unique_ptr<Transport> transport;
        if (url.tls) {
            transport = co_await TlsStream::connect(*loop_, address, url.host, tls_context_, connect_timeout);
        } else {
            TcpStream socket =
                co_await TcpStream::connect(*loop_, reinterpret_cast<const sockaddr *>(&address.storage),
                                            address.length, connect_timeout);
#ifdef REALURL_WITH_IO_URING
            if (loop_->uring() != nullptr)
                transport = UringTransport::create(*loop_, socket.release());
#endif
            if (transport == nullptr)
                transport = std::make_unique<TcpStream>(std::move(socket));
        }

        co_return co_await H2Connection::connect(*loop_, std::move(transport), url.authority(), url.scheme);
    }

    EventLoop *loop_;
    Config config_;
    Resolver resolver_;
    std::shared_ptr<TlsContext> tls_context_;
    std::unordered_map<std::string, std::unique_ptr<OriginPool>> pools_;
};

} // namespace detail

Stream::~Stream()
{
    if (impl_ != nullptr)
        impl_->cancel();
}

Task<ResponseHead> Stream::head(std::chrono::milliseconds timeout)
{
    if (impl_ == nullptr)
        throw std::system_error(ENOTCONN, std::generic_category(), "empty stream");
    co_return co_await impl_->head(timeout);
}

Task<std::size_t> Stream::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    if (impl_ == nullptr)
        throw std::system_error(ENOTCONN, std::generic_category(), "empty stream");
    co_return co_await impl_->read(buffer, idle);
}

Task<void> Stream::write(std::string data, std::chrono::milliseconds timeout)
{
    if (impl_ == nullptr)
        throw std::system_error(ENOTCONN, std::generic_category(), "empty stream");
    co_await impl_->write(data, timeout);
}

Task<void> Stream::end(std::chrono::milliseconds timeout)
{
    if (impl_ == nullptr)
        co_return;
    co_await impl_->end(timeout);
}

void Stream::cancel()
{
    if (impl_ != nullptr)
        impl_->cancel();
}

Client::Client(EventLoop &loop, Config config)
    : impl_(std::make_unique<detail::ClientImpl>(loop, std::move(config)))
{
}

Client::~Client()
{
    // Shut every connection down so peer sockets see EOF before this object
    // dies; connection reader/writer coroutines may outlive the pool.
    impl_->close();
}

Task<Response> Client::get(std::string url, std::chrono::milliseconds timeout)
{
    co_return co_await request("GET", std::move(url), {}, {}, timeout);
}

Task<Response> Client::request(std::string method, std::string url, Headers headers, std::string body,
                               std::chrono::milliseconds timeout)
{
    if (method.empty())
        throw std::invalid_argument("h2 request: empty method");
    const bool has_body = !body.empty();
    const detail::UrlParts parts = detail::parse_url(url);
    std::shared_ptr<detail::H2Connection> connection = co_await impl_->connection(parts, timeout);
    std::shared_ptr<detail::H2StreamImpl> stream_impl =
        connection->open(method, parts.path, std::move(headers), !has_body);
    Stream stream(std::move(stream_impl));

    if (has_body) {
        co_await stream.write(std::move(body), timeout);
        co_await stream.end(timeout);
    }

    Response response;
    response.head = co_await stream.head(timeout);

    char buffer[16384];
    for (;;) {
        const std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, timeout);
        if (n == 0)
            break;
        response.body.append(buffer, n);
    }

    co_return response;
}

Task<Stream> Client::open(std::string method, std::string url, Headers headers,
                          std::chrono::milliseconds timeout)
{
    if (method.empty())
        throw std::invalid_argument("h2 open: empty method");
    const detail::UrlParts parts = detail::parse_url(url);
    std::shared_ptr<detail::H2Connection> connection = co_await impl_->connection(parts, timeout);
    std::shared_ptr<detail::H2StreamImpl> stream =
        connection->open(method, parts.path, std::move(headers), false);
    co_return Stream(std::move(stream));
}

void Client::close()
{
    impl_->close();
}

} // namespace realurl
