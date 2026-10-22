#pragma once

/// Minimal HTTP/1.1 + HTTP/2 server on the realurl EventLoop.
///
/// One EventLoop per io thread, one SO_REUSEPORT listener per loop, and
/// coroutine handlers. The public routing/Request/Response surface stays close
/// to the previous Beast-based server; the new native streaming entry point is
/// Response::take_over_task().

#include "realurl/server_types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace realurl
{

using ClientWriter = server::ClientWriter;
using AsyncClientWriter = server::AsyncClientWriter;

class Server
{
public:
    struct Impl;

    using Header = server::Header;
    using BodyMode = server::BodyMode;
    using HandlerResponse = server::HandlerResponse;
    using StreamStrategy = server::StreamStrategy;
    using Request = server::Request;
    using Response = server::Response;
    using RouteOptions = server::RouteOptions;

    using Handler = std::function<void(const Request &, Response &)>;
    using PreRoutingHandler = std::function<HandlerResponse(Request &, Response &)>;
    using TakeOverTask = Response::TakeOverTask;
    using AsyncTakeOverTask = Response::AsyncTakeOverTask;
    using TaskTakeOver = Response::TaskTakeOver;

    struct Stats
    {
        std::uint64_t connections_accepted = 0;
        std::uint64_t active_connections = 0;
        std::uint64_t requests = 0;
        std::uint64_t responses = 0;
        std::uint64_t errors = 0;
        std::uint64_t bytes_in = 0;
        std::uint64_t bytes_out = 0;
        std::uint64_t h1_connections = 0;
        std::uint64_t h2_connections = 0;
        std::uint64_t native_connections = 0;
        EventLoopStats loop;
    };

    Server();
    ~Server();

    Server(const Server &) = delete;
    Server &operator=(const Server &) = delete;

    Server &Get(std::string pattern, Handler handler, RouteOptions options = {});
    Server &Post(std::string pattern, Handler handler, RouteOptions options = {});
    Server &Put(std::string pattern, Handler handler, RouteOptions options = {});
    Server &Delete(std::string pattern, Handler handler, RouteOptions options = {});

    Server &GetRegex(std::string pattern, Handler handler, RouteOptions options = {});
    Server &PostRegex(std::string pattern, Handler handler, RouteOptions options = {});
    Server &PutRegex(std::string pattern, Handler handler, RouteOptions options = {});
    Server &DeleteRegex(std::string pattern, Handler handler, RouteOptions options = {});

    Server &set_pre_routing_handler(PreRoutingHandler handler);
    Server &set_payload_max_length(std::size_t bytes);
    Server &set_header_limit(std::size_t bytes);
    Server &set_error_body(int status, std::string content_type, std::string body);

    /// 0 = no timeout.
    Server &set_read_header_timeout(std::chrono::milliseconds timeout);
    Server &set_request_idle_timeout(std::chrono::milliseconds timeout);
    Server &set_client_write_timeout(std::chrono::milliseconds timeout);

    /// Number of event-loop threads. 0 = hardware_concurrency().
    Server &set_io_threads(unsigned count);

    /// Decode percent escapes in Request::path. Default true.
    Server &set_path_decoding(bool enabled);

    /// HTTP/1.1 keep-alive. Default true.
    Server &set_keep_alive(bool enabled);

    /// Per-stream queue high/low watermarks (bytes). Defaults 64 KiB / 32 KiB.
    Server &set_queue_watermark(std::size_t high, std::size_t low);

    /// Maximum connections per loop. 0 = unlimited.
    Server &set_max_connections(std::size_t count);

    /// Enable the per-loop upstream client used by proxy handlers.
    Server &set_upstream(Config config);

    /// Enable the realurl native transport (ALPN realurl/1 and the plaintext
    /// REALURL/1 preface). Enabled by default; call before listen().
    Server &set_native(bool enabled = true);

    /// Default strategy for routes that do not override it.
    Server &set_strategy(StreamStrategy strategy);

    /// TLS listener (certificate chain + private key PEM).
    Server &set_tls(std::string cert_file, std::string key_file);

    /// Linux only; kTLS is best-effort and falls back to user-space TLS.
    Server &set_ktls(bool enabled);

    /// Linux only; pin loop threads to the available CPUs.
    Server &set_cpu_affinity(bool enabled);

    /// Deprecated compatibility knobs. The new server has no pump threads.
    Server &set_pump_threads(unsigned count);
    Server &set_pump_queue_limit(unsigned count);
    Server &set_pump_idle_timeout(std::chrono::milliseconds timeout);

    /// Bind and serve until stop(). Blocks the calling thread.
    bool listen(const std::string &host, int port);

    void stop();
    bool is_running() const;

    /// In-memory dispatch: parses one raw HTTP/1.1 request and returns the raw
    /// response bytes. take_over_task runs on a private EventLoop.
    std::string process(std::string_view raw_request, std::string_view remote_addr = "127.0.0.1");

    Stats stats() const;

private:
    std::unique_ptr<Impl> impl_;
};

} // namespace realurl
