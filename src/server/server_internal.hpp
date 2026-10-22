#pragma once

#include "http1_parser.hpp"
#include "router.hpp"

#include "realurl/detail/protocol.hpp"
#include "realurl/detail/socket.hpp"
#include "realurl/detail/tls_server.hpp"
#include "realurl/server.hpp"

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace realurl::detail
{

using server::AsyncClientWriter;
using server::BodyMode;
using server::BodyReader;
using server::ClientWriter;
using server::HandlerResponse;
using server::Request;
using server::Response;
using server::StreamStrategy;
using server::StreamWriter;

struct ServerContext
{
    Router router;

    Server::PreRoutingHandler pre_routing;
    std::size_t payload_max = 0;
    std::size_t header_limit = 1 << 20;
    std::unordered_map<int, std::pair<std::string, std::string>> error_bodies;

    std::chrono::milliseconds read_header_timeout{ 5000 };
    std::chrono::milliseconds request_idle_timeout{ 60000 };
    std::chrono::milliseconds client_write_timeout{ 60000 };

    bool path_decoding = true;
    bool keep_alive = true;
    bool native_enabled = true;
    bool native_compression = false;
    StreamStrategy default_strategy = StreamStrategy::Auto;
    std::size_t queue_high = 64 * 1024;
    std::size_t queue_low = 32 * 1024;
    std::size_t max_connections = 0;

    bool has_upstream = false;
    Config upstream_config;

    std::string tls_cert;
    std::string tls_key;
    bool ktls = true;
    bool cpu_affinity = false;
    std::shared_ptr<TlsServerContext> tls_context;

    /// Asio context used only by the legacy take_over_async compatibility
    /// layer. The native path never touches it.
    boost::asio::io_context *compat_io = nullptr;

    std::atomic<std::uint64_t> requests{ 0 };
    std::atomic<std::uint64_t> responses{ 0 };
    std::atomic<std::uint64_t> errors{ 0 };
    std::atomic<std::uint64_t> bytes_in{ 0 };
    std::atomic<std::uint64_t> bytes_out{ 0 };
    std::atomic<std::uint64_t> h1_connections{ 0 };
    std::atomic<std::uint64_t> h2_connections{ 0 };
    std::atomic<std::uint64_t> native_connections{ 0 };

    void apply_error_body(Response &response, int status) const;
    std::string error_response(int status) const;
};

bool status_allows_body(int status) noexcept;

/// Serialize a fixed (non-takeover) HTTP/1.1 response.
void serialize_fixed_response(const Response &response, bool head_only, bool keep_alive,
                              std::string &out);

/// Cached RFC 7231 IMF-fixdate for the current second.
std::string_view current_date() noexcept;

std::shared_ptr<BodySource> make_memory_source(std::shared_ptr<std::string> data);

class H1StreamWriter;

class H1Connection
{
public:
    H1Connection(ServerContext &context, EventLoop &loop,
                 std::unique_ptr<Transport> transport, std::string initial, int family,
                 sockaddr_storage peer, Client *upstream);
    H1Connection(ServerContext &context, EventLoop &loop, TcpStream &&stream, std::string initial,
                 int family, sockaddr_storage peer, Client *upstream);
    H1Connection(ServerContext &context, EventLoop &loop, int fd, int family,
                 sockaddr_storage peer, Client *upstream);
    ~H1Connection();

    H1Connection(const H1Connection &) = delete;
    H1Connection &operator=(const H1Connection &) = delete;

    Task<void> run();

    // BodyReader support.
    Task<bool> fill(std::chrono::milliseconds idle);
    std::string_view available() const noexcept
    {
        return std::string_view{ buffer_ }.substr(offset_);
    }
    void consume(std::size_t count) noexcept;
    int fd() const noexcept
    {
        return transport_ != nullptr ? transport_->fd() : -1;
    }

private:
    Task<bool> read_head();
    Task<bool> read_body(std::shared_ptr<std::string> &data);
    Task<void> write_error(int status, bool head_only = false);
    Task<bool> dispatch_handler(const Router::Route &route);
    Task<void> legacy_takeover(const Response &response);
    Task<void> async_takeover(const Response &response);

    void build_request();
    void compact();
    void reset_request();

    ServerContext &context_;
    EventLoop &loop_;
    std::unique_ptr<Transport> transport_;
    int family_ = AF_INET;
    std::string remote_addr_;
    int remote_port_ = 0;
    Client *upstream_ = nullptr;

    std::string buffer_;
    std::size_t offset_ = 0;
    std::string head_storage_;

    ParsedHead head_;
    Request request_;
    Response response_;
    std::unique_ptr<H1StreamWriter> writer_;
    std::unique_ptr<BodyReader> body_reader_;

    bool close_ = false;
};

} // namespace realurl::detail
