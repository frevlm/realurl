#pragma once

#include <realurl/body_source.hpp>
#include <realurl/fd_writer.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace realurl
{

/// Minimal HTTP/1.1 server with an httplib-like routing API.
///
/// Requests carry either a fully buffered body (management JSON endpoints) or
/// a pull-based BodySource (large streaming uploads).  Responses are either a
/// fixed body (set_content) or a raw connection takeover (take_over), where
/// the handler writes the status line, headers, and body through a
/// ClientWriter.  Every response closes the connection.
class Server
{
public:
    enum class BodyMode { Buffered, Streamed };
    enum class HandlerResponse { Handled, Unhandled };

    using TakeOverTask = std::function<void(ClientWriter &)>;

    struct Request
    {
        std::string method;
        std::string target; // original request-target, including the query
        std::string path;   // target without query; decoded when enabled
        std::string version;
        std::vector<std::pair<std::string, std::string>> headers;
        std::unordered_map<std::string, std::string> path_params;

        std::string body;                        // Buffered mode
        std::shared_ptr<BodySource> body_source; // Streamed mode
        BodyMode body_mode = BodyMode::Buffered;
        long long content_length = -1;

        std::string remote_addr;
        int remote_port = 0;

        /// Case-insensitive header lookup.  Returns the first matching value.
        std::string_view get_header(std::string_view name) const;
        bool has_header(std::string_view name) const;
    };

    struct Response
    {
        int status = 200;
        std::vector<std::pair<std::string, std::string>> headers;
        std::string body;
        std::string content_type;

        void set_status(int code)
        {
            status = code;
        }

        /// Replace an existing header (case-insensitive) or append a new one.
        void set_header(std::string name, std::string value);

        /// Append a header without replacing duplicates (for Set-Cookie).
        void add_header(std::string name, std::string value);

        std::string get_header(std::string_view name) const;

        void set_content(std::string body, std::string_view content_type = {});

        /// Take over the raw connection.  The task writes the response head and
        /// body through the ClientWriter.  The server runs it on a pump thread
        /// and keeps the connection open until it returns.
        void take_over(TakeOverTask task);

        bool taken_over() const
        {
            return static_cast<bool>(take_over_);
        }

        const TakeOverTask &take_over_task() const
        {
            return take_over_;
        }

    private:
        TakeOverTask take_over_;
    };

    using Handler = std::function<void(const Request &, Response &)>;
    using PreRoutingHandler = std::function<HandlerResponse(Request &, Response &)>;

    struct RouteOptions {
        BodyMode body_mode;
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

    /// Maximum request body size.  0 = unlimited.  Buffered routes are rejected
    /// during the read; Streamed routes stop the feeder and return 413 when no
    /// response has been written yet.
    Server &set_payload_max_length(std::size_t bytes);

    /// Maximum total request-header size.  0 = library default (1 MiB).
    Server &set_header_limit(std::size_t bytes);

    /// Body used for generated error responses (400/413/431/500/...).
    Server &set_error_body(int status, std::string content_type, std::string body);

    /// 0 = no timeout.
    Server &set_read_header_timeout(std::chrono::milliseconds timeout);
    Server &set_client_write_timeout(std::chrono::milliseconds timeout);

    /// 0 = std::thread::hardware_concurrency().
    Server &set_io_threads(unsigned count);

    /// Decode percent escapes in Request::path.  Default true; streaming
    /// listeners that must preserve the raw path can disable it.
    Server &set_path_decoding(bool enabled);

    /// Bind and serve until stop().  Blocks the calling thread.
    bool listen(const std::string &host, int port);

    /// Stop accepting, wait for active connections (bounded), then join.
    void stop();

    bool is_running() const;

    /// In-memory dispatch: parses one raw request and returns the raw response
    /// bytes.  No sockets or worker threads are used; take_over tasks run on
    /// the calling thread.  Returns an empty string when parsing fails.
    std::string process(std::string_view raw_request, std::string_view remote_addr = "127.0.0.1");

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace realurl
