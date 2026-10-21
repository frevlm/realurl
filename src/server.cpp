#include "realurl/server.hpp"

#include "realurl/body_feeder.hpp"
#include "realurl/buffer_pool.hpp"
#include "realurl/stream_pump_pool.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <mutex>
#include <regex>
#include <sstream>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace realurl
{

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

namespace
{

std::string_view reason_phrase(int status)
{
    switch (status) {
    case 100:
        return "Continue";
    case 200:
        return "OK";
    case 201:
        return "Created";
    case 202:
        return "Accepted";
    case 204:
        return "No Content";
    case 301:
        return "Moved Permanently";
    case 302:
        return "Found";
    case 304:
        return "Not Modified";
    case 400:
        return "Bad Request";
    case 401:
        return "Unauthorized";
    case 402:
        return "Payment Required";
    case 403:
        return "Forbidden";
    case 404:
        return "Not Found";
    case 405:
        return "Method Not Allowed";
    case 408:
        return "Request Timeout";
    case 409:
        return "Conflict";
    case 413:
        return "Payload Too Large";
    case 415:
        return "Unsupported Media Type";
    case 422:
        return "Unprocessable Entity";
    case 429:
        return "Too Many Requests";
    case 431:
        return "Request Header Fields Too Large";
    case 500:
        return "Internal Server Error";
    case 501:
        return "Not Implemented";
    case 502:
        return "Bad Gateway";
    case 503:
        return "Service Unavailable";
    case 504:
        return "Gateway Timeout";
    default:
        return "Status";
    }
}

bool iequals(std::string_view left, std::string_view right)
{
    if (left.size() != right.size())
        return false;
    for (std::size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) != std::tolower(static_cast<unsigned char>(right[i])))
            return false;
    }
    return true;
}

int hex_value(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

std::string percent_decode(std::string_view input)
{
    std::string out;
    out.reserve(input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '%' && i + 2 < input.size()) {
            const int hi = hex_value(input[i + 1]);
            const int lo = hex_value(input[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>(hi * 16 + lo));
                i += 2;
                continue;
            }
        }
        out.push_back(input[i]);
    }
    return out;
}

std::string target_path(std::string_view target)
{
    const auto end = target.find_first_of("?#");
    return std::string{ target.substr(0, end == std::string_view::npos ? target.size() : end) };
}

long long parse_content_length(std::string_view text)
{
    if (text.empty())
        return -1;
    long long value = 0;
    for (char c : text) {
        if (c < '0' || c > '9')
            return -1;
        if (value > (std::numeric_limits<long long>::max() - (c - '0')) / 10)
            return -1;
        value = value * 10 + (c - '0');
    }
    return value;
}

std::vector<std::string> split_path(std::string_view path)
{
    std::vector<std::string> parts;
    std::size_t start = 0;
    for (;;) {
        const auto slash = path.find('/', start);
        if (slash == std::string_view::npos) {
            parts.emplace_back(path.substr(start));
            break;
        }
        parts.emplace_back(path.substr(start, slash - start));
        start = slash + 1;
    }
    return parts;
}

bool match_params(const std::vector<std::string> &segments, std::string_view path,
                  std::unordered_map<std::string, std::string> &params)
{
    const std::vector<std::string> parts = split_path(path);
    if (parts.size() != segments.size())
        return false;

    std::unordered_map<std::string, std::string> captured;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        const std::string &segment = segments[i];
        if (segment.size() > 1 && segment[0] == ':') {
            if (parts[i].empty())
                return false;
            captured.emplace(segment.substr(1), parts[i]);
        } else if (segment != parts[i]) {
            return false;
        }
    }
    params = std::move(captured);
    return true;
}

std::string serialize_response(const Server::Response &response, bool head_only)
{
    std::ostringstream out;
    out << "HTTP/1.1 " << response.status << ' ' << reason_phrase(response.status) << "\r\n";
    for (const auto &header : response.headers) {
        if (iequals(header.first, "Content-Length") || iequals(header.first, "Connection") ||
            iequals(header.first, "Transfer-Encoding"))
            continue;
        out << header.first << ": " << header.second << "\r\n";
    }
    if (!response.content_type.empty())
        out << "Content-Type: " << response.content_type << "\r\n";
    out << "Content-Length: " << response.body.size() << "\r\n"
        << "Connection: close\r\n\r\n";
    if (!head_only)
        out << response.body;
    return out.str();
}

/// In-memory read stream for Server::process().
struct MemoryStream {
    std::string_view data;
    std::size_t pos = 0;

    template <typename MutableBufferSequence>
    std::size_t read_some(const MutableBufferSequence &buffers, boost::system::error_code &ec)
    {
        if (pos >= data.size()) {
            ec = http::error::end_of_stream;
            return 0;
        }
        const std::size_t count = net::buffer_copy(buffers, net::buffer(data.data() + pos, data.size() - pos));
        pos += count;
        ec.clear();
        return count;
    }

    template <typename MutableBufferSequence>
    std::size_t read_some(const MutableBufferSequence &buffers)
    {
        boost::system::error_code ec;
        const std::size_t count = read_some(buffers, ec);
        if (ec)
            throw boost::system::system_error(ec);
        return count;
    }
};

std::shared_ptr<BodySource> make_memory_source(std::shared_ptr<std::string> data)
{
    auto offset = std::make_shared<std::size_t>(0);
    auto source = std::make_shared<BodySource>();
    source->read = [data, offset]() -> BodyReadResult {
        if (*offset >= data->size())
            return { {}, true, false };
        return { std::string_view{ *data }.substr(*offset), false, false };
    };
    source->consume = [offset](std::size_t count) { *offset += count; };
    source->peek = [data]() { return data->substr(0, 65536); };
    source->wake = [] {};
    return source;
}

std::shared_ptr<BodySource> make_empty_source()
{
    auto source = std::make_shared<BodySource>();
    source->read = []() -> BodyReadResult {
        return { {}, true, false };
    };
    source->consume = [](std::size_t) {};
    source->peek = []() { return std::string{}; };
    source->wake = [] {};
    return source;
}

} // namespace

// ============================================================================
// Request / Response
// ============================================================================

std::string_view Server::Request::get_header(std::string_view name) const
{
    for (const auto &header : headers) {
        if (iequals(header.first, name))
            return header.second;
    }
    return {};
}

bool Server::Request::has_header(std::string_view name) const
{
    for (const auto &header : headers) {
        if (iequals(header.first, name))
            return true;
    }
    return false;
}

void Server::Response::set_header(std::string name, std::string value)
{
    if (iequals(name, "Content-Type")) {
        content_type = std::move(value);
        return;
    }
    for (auto &header : headers) {
        if (iequals(header.first, name)) {
            header.first = std::move(name);
            header.second = std::move(value);
            return;
        }
    }
    headers.emplace_back(std::move(name), std::move(value));
}

void Server::Response::add_header(std::string name, std::string value)
{
    headers.emplace_back(std::move(name), std::move(value));
}

std::string Server::Response::get_header(std::string_view name) const
{
    if (iequals(name, "Content-Type"))
        return content_type;
    for (const auto &header : headers) {
        if (iequals(header.first, name))
            return header.second;
    }
    return {};
}

void Server::Response::set_content(std::string body, std::string_view content_type)
{
    this->body = std::move(body);
    if (!content_type.empty())
        this->content_type = std::string{ content_type };
    else if (this->content_type.empty() && !this->body.empty())
        this->content_type = "text/plain";
}

void Server::Response::take_over(TakeOverTask task)
{
    take_over_ = std::move(task);
}

// ============================================================================
// Server::Impl
// ============================================================================

struct Server::Impl {
    enum class RouteKind { Exact, Params, Regex };

    struct Route {
        std::string method;
        RouteKind kind = RouteKind::Exact;
        std::string pattern;
        std::vector<std::string> segments;
        std::regex regex;
        Handler handler;
        BodyMode body_mode = BodyMode::Buffered;
    };

    std::vector<Route> routes;
    PreRoutingHandler pre_routing;
    std::size_t payload_max = 0;
    std::size_t header_limit = 1 << 20;
    std::unordered_map<int, std::pair<std::string, std::string>> error_bodies;
    std::chrono::milliseconds read_header_timeout{ 5000 };
    std::chrono::milliseconds client_write_timeout{ 60000 };
    unsigned io_threads = 0;
    bool path_decoding = true;
    BufferPool buffer_pool{ 64 };

    net::io_context ioc;
    std::unique_ptr<tcp::acceptor> acceptor;
    std::vector<std::thread> threads;
    std::atomic<bool> running{ false };
    std::atomic<int> active{ 0 };
    std::mutex conn_mu;
    std::condition_variable conn_cv;
    std::mutex wait_mu;
    std::condition_variable wait_cv;

    void add_route(std::string method, RouteKind kind, std::string pattern, Handler handler, RouteOptions options)
    {
        Route route;
        route.method = std::move(method);
        route.kind = kind;
        route.pattern = std::move(pattern);
        route.handler = std::move(handler);
        route.body_mode = options.body_mode;
        if (kind == RouteKind::Params)
            route.segments = split_path(route.pattern);
        if (kind == RouteKind::Regex)
            route.regex = std::regex{ route.pattern };
        routes.push_back(std::move(route));
    }

    const Route *find_route(std::string_view method, std::string_view path, Request &request) const
    {
        auto method_matches = [&](const Route &route) {
            return route.method == method || (method == "HEAD" && route.method == "GET");
        };

        for (const Route &route : routes) {
            if (route.kind == RouteKind::Exact && method_matches(route) && route.pattern == path)
                return &route;
        }
        for (const Route &route : routes) {
            if (route.kind != RouteKind::Params || !method_matches(route))
                continue;
            std::unordered_map<std::string, std::string> params;
            if (match_params(route.segments, path, params)) {
                request.path_params = std::move(params);
                return &route;
            }
        }
        for (const Route &route : routes) {
            if (route.kind == RouteKind::Regex && method_matches(route) &&
                std::regex_match(std::string{ path }, route.regex))
                return &route;
        }
        return nullptr;
    }

    void apply_error_body(Response &response, int status) const
    {
        const auto it = error_bodies.find(status);
        if (it == error_bodies.end())
            return;
        response.content_type = it->second.first;
        response.body = it->second.second;
    }

    Request make_request(const http::request<http::string_body> &message, std::string remote_addr, int remote_port) const
    {
        Request request;
        request.method = std::string{ message.method_string() };
        request.target = std::string{ message.target() };
        request.version = message.version() == 11 ? "HTTP/1.1" : "HTTP/1.0";
        request.path = target_path(request.target);
        if (path_decoding)
            request.path = percent_decode(request.path);
        for (const auto &field : message)
            request.headers.emplace_back(std::string{ field.name_string() }, std::string{ field.value() });
        if (const auto it = message.find(http::field::content_length); it != message.end())
            request.content_length = parse_content_length(it->value());
        request.remote_addr = std::move(remote_addr);
        request.remote_port = remote_port;
        return request;
    }

    std::string error_response(int status, bool head_only = false) const
    {
        Response response;
        response.status = status;
        apply_error_body(response, status);
        return serialize_response(response, head_only);
    }

    std::string process(std::string_view raw_request, std::string_view remote_addr)
    {
        MemoryStream stream{ raw_request, 0 };
        beast::flat_buffer buffer;
        http::request_parser<http::string_body> parser;
        parser.header_limit(static_cast<std::uint32_t>(header_limit));
        parser.body_limit(payload_max > 0 ? payload_max : std::numeric_limits<std::uint64_t>::max());

        boost::system::error_code ec;
        http::read_header(stream, buffer, parser, ec);
        if (ec) {
            if (ec == http::error::header_limit)
                return error_response(431);
            if (ec == http::error::body_limit)
                return error_response(413);
            return error_response(400);
        }

        Request request = make_request(parser.get(), std::string{ remote_addr }, 0);
        if (request.has_header("Content-Length") && request.content_length < 0)
            return error_response(400);
        if (request.target.empty() || request.path.empty() ||
            (request.path[0] != '/' && !(request.method == "OPTIONS" && request.target == "*")))
            return error_response(400);

        if (pre_routing) {
            Response response;
            if (pre_routing(request, response) == HandlerResponse::Handled)
                return serialize_response(response, request.method == "HEAD");
        }

        const Route *route = find_route(request.method, request.path, request);
        if (route == nullptr)
            return error_response(404, request.method == "HEAD");

        request.body_mode = route->body_mode;
        if (route->body_mode == BodyMode::Buffered) {
            http::read(stream, buffer, parser, ec);
            if (ec == http::error::body_limit)
                return error_response(413);
            if (ec)
                return error_response(400);
            request.body = std::move(parser.get().body());
            request.content_length = static_cast<long long>(request.body.size());
        } else {
            const auto transfer_encoding = request.get_header("Transfer-Encoding");
            if (!transfer_encoding.empty())
                return error_response(400);
            if (request.content_length < 0)
                return error_response(400);
            if (payload_max > 0 && request.content_length > static_cast<long long>(payload_max))
                return error_response(413);
            if (request.content_length > 0) {
                auto data = std::make_shared<std::string>(static_cast<const char *>(buffer.data().data()), buffer.size());
                data->append(stream.data.substr(stream.pos));
                request.body_source = make_memory_source(std::move(data));
            } else {
                request.body_source = make_empty_source();
            }
        }

        Response response;
        try {
            route->handler(request, response);
        } catch (const std::exception &) {
            response = Response{};
            response.status = 500;
            apply_error_body(response, 500);
        }

        if (response.taken_over()) {
            std::string output;
            ClientWriter writer = [&output](std::string_view data) {
                output.append(data);
                return true;
            };
            response.take_over_task()(writer);
            return output;
        }
        return serialize_response(response, request.method == "HEAD");
    }

    static net::awaitable<void> write_response(beast::tcp_stream &stream, std::string bytes)
    {
        boost::system::error_code ec;
        co_await net::async_write(stream, net::buffer(bytes), net::redirect_error(net::use_awaitable, ec));
    }

    static net::awaitable<void> write_error(beast::tcp_stream &stream, Impl *impl, int status, bool head_only)
    {
        co_await write_response(stream, impl->error_response(status, head_only));
    }

    static net::awaitable<void> send_continue(beast::tcp_stream &stream)
    {
        static constexpr std::string_view kContinue = "HTTP/1.1 100 Continue\r\n\r\n";
        boost::system::error_code ec;
        co_await net::async_write(stream, net::buffer(kContinue), net::redirect_error(net::use_awaitable, ec));
    }

    struct StreamedBody {
        std::shared_ptr<BodyFeeder> feeder;
        std::thread thread;
        int fd = -1;

        StreamedBody() = default;
        StreamedBody(const StreamedBody &) = delete;
        StreamedBody &operator=(const StreamedBody &) = delete;

        ~StreamedBody()
        {
            stop();
        }

        std::shared_ptr<BodySource> start(Impl *impl, int client_fd, std::string_view initial, long long limit)
        {
            fd = client_fd;
            feeder = std::make_shared<BodyFeeder>(&impl->buffer_pool);
            feeder->set_body_limit(limit);
            auto source = feeder->make_source();
            feeder->feed_initial(initial);
            thread = std::thread([feeder = feeder, client_fd] { feeder->run(client_fd); });
            return source;
        }

        void stop()
        {
            if (!feeder)
                return;
            feeder->cancel();
            if (fd >= 0)
                ::shutdown(fd, SHUT_RD);
            if (thread.joinable())
                thread.join();
            feeder.reset();
            fd = -1;
        }
    };

    static net::awaitable<void> handle_connection(Impl *impl, tcp::socket socket)
    {
        struct Guard {
            Impl *impl;
            ~Guard()
            {
                impl->active.fetch_sub(1, std::memory_order_acq_rel);
                impl->conn_cv.notify_all();
            }
        } guard{ impl };

        beast::tcp_stream stream(std::move(socket));
        boost::system::error_code ec;
        const auto endpoint = stream.socket().remote_endpoint(ec);
        const std::string remote_addr = ec ? std::string{} : endpoint.address().to_string();
        const int remote_port = ec ? 0 : static_cast<int>(endpoint.port());

        beast::flat_buffer buffer;
        http::request_parser<http::string_body> parser;
        parser.header_limit(static_cast<std::uint32_t>(impl->header_limit));
        parser.body_limit(impl->payload_max > 0 ? impl->payload_max : std::numeric_limits<std::uint64_t>::max());

        if (impl->read_header_timeout.count() > 0)
            stream.expires_after(impl->read_header_timeout);
        else
            stream.expires_never();

        co_await http::async_read_header(stream, buffer, parser, net::redirect_error(net::use_awaitable, ec));
        if (ec) {
            if (ec == http::error::header_limit)
                co_await write_error(stream, impl, 431, false);
            else if (ec == http::error::body_limit)
                co_await write_error(stream, impl, 413, false);
            else if (ec != net::error::eof && ec != net::error::connection_reset && ec != net::error::operation_aborted)
                co_await write_error(stream, impl, 400, false);
            co_return;
        }

        Request request = impl->make_request(parser.get(), remote_addr, remote_port);
        if (request.has_header("Content-Length") && request.content_length < 0) {
            co_await write_error(stream, impl, 400, false);
            co_return;
        }
        if (request.target.empty() || request.path.empty() ||
            (request.path[0] != '/' && !(request.method == "OPTIONS" && request.target == "*"))) {
            co_await write_error(stream, impl, 400, false);
            co_return;
        }

        if (impl->pre_routing) {
            Response pre_response;
            if (impl->pre_routing(request, pre_response) == HandlerResponse::Handled) {
                co_await write_response(stream, serialize_response(pre_response, request.method == "HEAD"));
                co_return;
            }
        }

        const Route *route = impl->find_route(request.method, request.path, request);
        if (route == nullptr) {
            co_await write_error(stream, impl, 404, request.method == "HEAD");
            co_return;
        }
        request.body_mode = route->body_mode;

        if (iequals(request.get_header("Expect"), "100-continue"))
            co_await send_continue(stream);

        StreamedBody streamed;
        if (route->body_mode == BodyMode::Buffered) {
            stream.expires_after(std::chrono::seconds(60));
            co_await http::async_read(stream, buffer, parser, net::redirect_error(net::use_awaitable, ec));
            if (ec == http::error::body_limit) {
                co_await write_error(stream, impl, 413, false);
                co_return;
            }
            if (ec) {
                if (ec != net::error::eof && ec != net::error::connection_reset && ec != net::error::operation_aborted)
                    co_await write_error(stream, impl, 400, false);
                co_return;
            }
            request.body = std::move(parser.get().body());
            request.content_length = static_cast<long long>(request.body.size());
        } else {
            const auto transfer_encoding = request.get_header("Transfer-Encoding");
            if (!transfer_encoding.empty()) {
                co_await write_error(stream, impl, 400, false);
                co_return;
            }
            if (request.content_length < 0) {
                co_await write_error(stream, impl, 400, false);
                co_return;
            }
            if (impl->payload_max > 0 && request.content_length > static_cast<long long>(impl->payload_max)) {
                co_await write_error(stream, impl, 413, false);
                co_return;
            }
            if (request.content_length > 0) {
                const std::string_view initial{ static_cast<const char *>(buffer.data().data()), buffer.size() };
                request.body_source = streamed.start(impl, stream.socket().native_handle(), initial,
                                                     static_cast<long long>(impl->payload_max));
                buffer.consume(buffer.size());
            } else {
                request.body_source = make_empty_source();
            }
        }

        Response response;
        try {
            route->handler(request, response);
        } catch (const std::exception &) {
            if (!response.taken_over()) {
                response = Response{};
                response.status = 500;
                impl->apply_error_body(response, 500);
            }
        }

        if (response.taken_over()) {
            auto response_started = std::make_shared<std::atomic_bool>(false);
            ClientWriter writer = client_writer_from_fd(stream.socket().native_handle(), impl->client_write_timeout,
                                                        response_started);
            auto gate = stream_pump_pool().submit(
                request.remote_addr,
                [task = response.take_over_task(), &writer]() mutable { task(writer); },
                stream.get_executor());
            co_await async_wait_gate(gate, net::use_awaitable);

            if (streamed.feeder && streamed.feeder->limit_exceeded() &&
                !response_started->load(std::memory_order_acquire)) {
                co_await write_error(stream, impl, 413, false);
            }
        } else {
            co_await write_response(stream, serialize_response(response, request.method == "HEAD"));
        }

        streamed.stop();
        boost::system::error_code ignored;
        stream.socket().shutdown(net::socket_base::shutdown_send, ignored);
    }

    static net::awaitable<void> accept_loop(Impl *impl)
    {
        for (;;) {
            auto [ec, socket] = co_await impl->acceptor->async_accept(net::as_tuple(net::use_awaitable));
            if (ec) {
                if (ec == net::error::operation_aborted || !impl->running.load())
                    co_return;
                continue;
            }
            impl->active.fetch_add(1, std::memory_order_acq_rel);
            net::co_spawn(impl->acceptor->get_executor(), handle_connection(impl, std::move(socket)), net::detached);
        }
    }

    bool listen(const std::string &host, int port)
    {
        if (running.exchange(true))
            return false;

        boost::system::error_code ec;
        tcp::resolver resolver(ioc);
        const auto endpoints = resolver.resolve(host, std::to_string(port), ec);
        if (ec || endpoints.empty()) {
            running.store(false);
            return false;
        }

        acceptor = std::make_unique<tcp::acceptor>(ioc);
        acceptor->open(endpoints.begin()->endpoint().protocol(), ec);
        if (ec) {
            running.store(false);
            return false;
        }
        acceptor->set_option(tcp::acceptor::reuse_address(true), ec);
        acceptor->bind(*endpoints.begin(), ec);
        if (ec) {
            running.store(false);
            return false;
        }
        acceptor->listen(net::socket_base::max_listen_connections, ec);
        if (ec) {
            running.store(false);
            return false;
        }

        net::co_spawn(ioc, accept_loop(this), net::detached);

        const unsigned count = io_threads > 0 ? io_threads : std::max(1u, std::thread::hardware_concurrency());
        threads.reserve(count);
        for (unsigned i = 0; i < count; ++i)
            threads.emplace_back([this] { ioc.run(); });

        std::unique_lock<std::mutex> lock(wait_mu);
        wait_cv.wait(lock, [this] { return !running.load(); });
        return true;
    }

    void stop()
    {
        if (!running.exchange(false))
            return;

        net::post(ioc, [this] {
            boost::system::error_code ec;
            if (acceptor)
                acceptor->close(ec);
        });

        {
            std::unique_lock<std::mutex> lock(conn_mu);
            conn_cv.wait_for(lock, std::chrono::seconds(30), [this] { return active.load() == 0; });
        }

        ioc.stop();
        for (auto &thread : threads) {
            if (thread.joinable())
                thread.join();
        }
        threads.clear();
        wait_cv.notify_all();
    }
};

// ============================================================================
// Server
// ============================================================================

Server::Server()
    : impl_(std::make_unique<Impl>())
{
}

Server::~Server()
{
    stop();
}

Server &Server::Get(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Impl::RouteKind::Exact : Impl::RouteKind::Params;
    impl_->add_route("GET", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::Post(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Impl::RouteKind::Exact : Impl::RouteKind::Params;
    impl_->add_route("POST", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::Put(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Impl::RouteKind::Exact : Impl::RouteKind::Params;
    impl_->add_route("PUT", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::Delete(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Impl::RouteKind::Exact : Impl::RouteKind::Params;
    impl_->add_route("DELETE", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::GetRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->add_route("GET", Impl::RouteKind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::PostRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->add_route("POST", Impl::RouteKind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::PutRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->add_route("PUT", Impl::RouteKind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::DeleteRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->add_route("DELETE", Impl::RouteKind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::set_pre_routing_handler(PreRoutingHandler handler)
{
    impl_->pre_routing = std::move(handler);
    return *this;
}

Server &Server::set_payload_max_length(std::size_t bytes)
{
    impl_->payload_max = bytes;
    return *this;
}

Server &Server::set_header_limit(std::size_t bytes)
{
    impl_->header_limit = bytes > 0 ? bytes : (1 << 20);
    return *this;
}

Server &Server::set_error_body(int status, std::string content_type, std::string body)
{
    impl_->error_bodies[status] = { std::move(content_type), std::move(body) };
    return *this;
}

Server &Server::set_read_header_timeout(std::chrono::milliseconds timeout)
{
    impl_->read_header_timeout = timeout;
    return *this;
}

Server &Server::set_client_write_timeout(std::chrono::milliseconds timeout)
{
    impl_->client_write_timeout = timeout;
    return *this;
}

Server &Server::set_io_threads(unsigned count)
{
    impl_->io_threads = count;
    return *this;
}

Server &Server::set_path_decoding(bool enabled)
{
    impl_->path_decoding = enabled;
    return *this;
}

bool Server::listen(const std::string &host, int port)
{
    return impl_->listen(host, port);
}

void Server::stop()
{
    impl_->stop();
}

bool Server::is_running() const
{
    return impl_->running.load();
}

std::string Server::process(std::string_view raw_request, std::string_view remote_addr)
{
    return impl_->process(raw_request, remote_addr);
}

} // namespace realurl
