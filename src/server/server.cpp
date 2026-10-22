#include "server_internal.hpp"
#include "h2_server.hpp"

#include "realurl/fd_writer.hpp"
#include "realurl/stream_pump_pool.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace realurl
{

using detail::H1Connection;
using detail::HeadParseResult;
using detail::ListenerOptions;
using detail::ParsedHead;
using detail::Router;
using detail::ServerContext;
using detail::TcpListener;
using detail::TcpStream;
using detail::TlsServerContext;
using detail::TlsServerStream;
using detail::Transport;
using detail::run_h2_server;
using server::AsyncClientWriter;
using server::BodyMode;
using server::BodyReader;
using server::ClientWriter;
using server::HandlerResponse;
using server::Request;
using server::StreamWriter;

namespace
{

using namespace std::chrono_literals;
using server::Header;

std::string_view trim_ws(std::string_view value) noexcept
{
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
        value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
        value.remove_suffix(1);
    return value;
}

int hex_digit(char c) noexcept
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

bool decode_chunked(std::string_view data, std::string &out)
{
    std::size_t offset = 0;
    for (;;) {
        const auto line_end = data.find("\r\n", offset);
        if (line_end == std::string_view::npos)
            return false;

        std::string_view line = data.substr(offset, line_end - offset);
        const auto extension = line.find(';');
        if (extension != std::string_view::npos)
            line = line.substr(0, extension);
        line = trim_ws(line);
        if (line.empty())
            return false;

        std::uint64_t size = 0;
        for (char c : line) {
            const int digit = hex_digit(c);
            if (digit < 0)
                return false;
            if (size > (UINT64_MAX - static_cast<std::uint64_t>(digit)) / 16)
                return false;
            size = size * 16 + static_cast<std::uint64_t>(digit);
        }

        offset = line_end + 2;
        if (size == 0) {
            for (;;) {
                const auto trailer_end = data.find("\r\n", offset);
                if (trailer_end == std::string_view::npos)
                    return false;
                if (trailer_end == offset)
                    return true;
                offset = trailer_end + 2;
            }
        }

        if (size > data.size() - offset)
            return false;
        out.append(data.data() + offset, static_cast<std::size_t>(size));
        offset += static_cast<std::size_t>(size);
        if (data.substr(offset, 2) != "\r\n")
            return false;
        offset += 2;
    }
}

class MemoryStreamWriter final : public StreamWriter
{
public:
    explicit MemoryStreamWriter(std::string &output)
        : output_(&output)
    {
    }

    Task<void> write_head(int status, std::span<const Header> headers,
                          std::optional<std::uint64_t> content_length) override
    {
        if (head_sent_)
            co_return;
        output_->append("HTTP/1.1 ");
        output_->append(std::to_string(status));
        output_->push_back(' ');
        output_->append(detail::reason_phrase(status));
        output_->append("\r\n");
        for (const Header &header : headers) {
            if (detail::iequals(header.first, "Content-Length") ||
                detail::iequals(header.first, "Connection") ||
                detail::iequals(header.first, "Transfer-Encoding"))
                continue;
            output_->append(header.first);
            output_->append(": ");
            output_->append(header.second);
            output_->append("\r\n");
        }
        if (content_length.has_value()) {
            output_->append("Content-Length: ");
            output_->append(std::to_string(*content_length));
            output_->append("\r\n");
        }
        output_->append("Connection: close\r\n\r\n");
        head_sent_ = true;
        co_return;
    }

    Task<bool> write(std::string_view data) override
    {
        if (!head_sent_)
            co_await write_head(200, {}, {});
        if (closed_)
            co_return false;
        output_->append(data);
        co_return true;
    }

    Task<void> end() override
    {
        ended_ = true;
        co_return;
    }

    void cancel() override
    {
        closed_ = true;
    }

    bool closed() const noexcept override
    {
        return closed_;
    }

private:
    std::string *output_;
    bool head_sent_ = false;
    bool ended_ = false;
    bool closed_ = false;
};

} // namespace

struct Server::Impl
{
    struct Worker
    {
        std::thread thread;
        std::mutex mutex;
        std::condition_variable cv;
        bool ready = false;
        bool failed = false;
        std::unique_ptr<EventLoop> loop;
        std::unique_ptr<Client> client;
        std::unique_ptr<TcpListener> listener;
    };

    ServerContext context;
    std::atomic<bool> running{ false };
    std::atomic<bool> stop_requested{ false };
    std::atomic<bool> listen_failed{ false };
    std::atomic<std::uint64_t> connections_accepted{ 0 };
    std::atomic<std::uint64_t> active_connections{ 0 };

    unsigned requested_threads = 0;
    unsigned worker_count = 0;
    std::string listen_host;
    int listen_port = 0;

    std::mutex wait_mutex;
    std::condition_variable wait_cv;
    std::mutex stop_mutex;
    std::atomic<bool> stop_complete{ false };

    std::vector<std::unique_ptr<Worker>> workers;
    boost::asio::io_context compat_io;
    using CompatWork = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    std::unique_ptr<CompatWork> compat_work;
    std::thread compat_thread;

    ~Impl()
    {
        stop();
    }

    static void worker_main(Impl *impl, Worker *worker);

    bool start();
    void stop();
    Stats stats() const;

    std::string process(std::string_view raw_request, std::string_view remote_addr);
};

namespace
{

Task<void> handle_client(Server::Impl *impl, EventLoop &loop, int fd, sockaddr_storage peer,
                         Client *client)
{
    struct Guard {
        Server::Impl *impl;
        ~Guard()
        {
            impl->active_connections.fetch_sub(1, std::memory_order_acq_rel);
            impl->wait_cv.notify_all();
        }
    } guard{ impl };

    TcpStream socket(loop, fd);
    std::unique_ptr<Transport> transport;
    if (impl->context.tls_context != nullptr) {
        try {
            transport = co_await TlsServerStream::accept(loop, std::move(socket),
                                                         impl->context.tls_context, 10s);
        } catch (...) {
            co_return;
        }
    } else {
        transport = std::make_unique<TcpStream>(std::move(socket));
    }

    std::string initial;
    static constexpr std::string_view kH2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

    char magic[24] = {};
    std::size_t magic_length = 0;
    bool h2 = true;
    while (magic_length < kH2Preface.size()) {
        std::size_t n = 0;
        bool failed = false;
        try {
            n = co_await transport->read({ magic + magic_length, kH2Preface.size() - magic_length }, 5s);
        } catch (...) {
            failed = true;
        }
        if (failed || n == 0) {
            h2 = false;
            break;
        }
        magic_length += n;
        if (std::string_view{ magic, magic_length } != kH2Preface.substr(0, magic_length)) {
            h2 = false;
            break;
        }
    }
    initial.assign(magic, magic_length);

    if (h2) {
        co_await run_h2_server(impl->context, loop, std::move(transport), peer, client);
    } else {
        H1Connection connection(impl->context, loop, std::move(transport), std::move(initial),
                                peer.ss_family, peer, client);
        co_await connection.run();
    }
}

Task<void> accept_loop(Server::Impl *impl, EventLoop &loop, TcpListener &listener, Client *client)
{
    for (;;) {
        if (impl->stop_requested.load(std::memory_order_acquire))
            co_return;

        int fd = -1;
        sockaddr_storage peer{};
        bool failed = false;
        try {
            auto accepted = co_await listener.accept();
            fd = accepted.first;
            peer = accepted.second;
        } catch (...) {
            failed = true;
        }

        if (failed) {
            if (impl->stop_requested.load(std::memory_order_acquire))
                co_return;
            co_await loop.sleep_for(1ms);
            continue;
        }

        if (impl->stop_requested.load(std::memory_order_acquire)) {
            ::close(fd);
            co_return;
        }

        impl->connections_accepted.fetch_add(1, std::memory_order_relaxed);
        impl->active_connections.fetch_add(1, std::memory_order_acq_rel);
        loop.spawn(handle_client(impl, loop, fd, peer, client));
    }
}

} // namespace

void Server::Impl::worker_main(Impl *impl, Worker *worker)
{
    worker->loop = std::make_unique<EventLoop>();

    if (impl->context.has_upstream)
        worker->client = std::make_unique<Client>(*worker->loop, impl->context.upstream_config);

    ListenerOptions options;
    options.host = impl->listen_host;
    options.port = static_cast<std::uint16_t>(impl->listen_port);
    options.backlog = 4096;
    options.reuse_port = impl->worker_count > 1;
    options.dual_stack = true;

    try {
        worker->listener = std::make_unique<TcpListener>(*worker->loop, options);
    } catch (...) {
        worker->failed = true;
        impl->listen_failed.store(true, std::memory_order_release);
        impl->running.store(false, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(worker->mutex);
            worker->ready = true;
        }
        worker->cv.notify_all();
        impl->wait_cv.notify_all();
        return;
    }

#ifdef __linux__
    if (impl->context.cpu_affinity) {
        static std::atomic<unsigned> next_cpu{ 0 };
        const unsigned cpu = next_cpu.fetch_add(1, std::memory_order_relaxed);
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu % static_cast<unsigned>(std::thread::hardware_concurrency()), &set);
        (void)::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
    }
#endif

    {
        std::lock_guard<std::mutex> lock(worker->mutex);
        worker->ready = true;
    }
    worker->cv.notify_all();

    worker->loop->spawn(accept_loop(impl, *worker->loop, *worker->listener, worker->client.get()));
    worker->loop->run();

    worker->listener.reset();
    worker->client.reset();
}

bool Server::Impl::start()
{
    if (running.load(std::memory_order_acquire) || !workers.empty())
        return false;

    running.store(false, std::memory_order_release);
    stop_requested.store(false, std::memory_order_release);
    stop_complete.store(false, std::memory_order_release);
    listen_failed.store(false, std::memory_order_release);
    context.compat_io = &compat_io;

    if (!context.tls_cert.empty()) {
        try {
            context.tls_context =
                TlsServerContext::create(context.tls_cert, context.tls_key, context.ktls);
        } catch (const std::exception &error) {
            std::fprintf(stderr, "realurl::Server: TLS setup failed: %s\n", error.what());
            listen_failed.store(true, std::memory_order_release);
            running.store(false, std::memory_order_release);
            return false;
        }
    }

    compat_io.restart();
    compat_work = std::make_unique<CompatWork>(boost::asio::make_work_guard(compat_io));
    compat_thread = std::thread([this] { compat_io.run(); });

    unsigned count = requested_threads;
    if (count == 0)
        count = std::max(1u, std::thread::hardware_concurrency());
    if (listen_port == 0 && count > 1)
        count = 1;

    worker_count = count;
    workers.reserve(count);
    for (unsigned i = 0; i < count; ++i) {
        auto worker = std::make_unique<Worker>();
        Worker *raw = worker.get();
        raw->thread = std::thread([this, raw] { worker_main(this, raw); });
        workers.push_back(std::move(worker));
    }

    for (auto &worker : workers) {
        std::unique_lock<std::mutex> lock(worker->mutex);
        worker->cv.wait(lock, [&worker] { return worker->ready; });
    }

    if (listen_failed.load(std::memory_order_acquire)) {
        stop();
        return false;
    }

    running.store(true, std::memory_order_release);

    std::unique_lock<std::mutex> lock(wait_mutex);
    wait_cv.wait(lock, [this] { return stop_requested.load(std::memory_order_acquire); });
    lock.unlock();

    stop();
    return !listen_failed.load(std::memory_order_acquire);
}

void Server::Impl::stop()
{
    std::lock_guard<std::mutex> stop_lock(stop_mutex);
    if (stop_complete.exchange(true, std::memory_order_acq_rel))
        return;

    stop_requested.store(true, std::memory_order_release);
    running.store(false, std::memory_order_release);

    if (workers.empty()) {
        if (compat_thread.joinable()) {
            compat_io.stop();
            compat_thread.join();
        }
        return;
    }

    for (auto &worker : workers) {
        if (worker->loop != nullptr) {
            Worker *raw = worker.get();
            worker->loop->post([raw] {
                if (raw->listener != nullptr)
                    raw->listener->close();
            });
        }
    }

    {
        std::unique_lock<std::mutex> lock(wait_mutex);
        wait_cv.wait_for(lock, 2s, [this] {
            return active_connections.load(std::memory_order_acquire) == 0;
        });
    }

    for (auto &worker : workers) {
        if (worker->loop != nullptr) {
            Worker *raw = worker.get();
            worker->loop->post([raw] {
                if (raw->loop != nullptr)
                    raw->loop->stop();
            });
        }
    }

    compat_io.stop();
    if (compat_thread.joinable())
        compat_thread.join();

    for (auto &worker : workers) {
        if (worker->thread.joinable())
            worker->thread.join();
    }
    workers.clear();
    wait_cv.notify_all();
}

Server::Stats Server::Impl::stats() const
{
    Stats result;
    result.connections_accepted = connections_accepted.load(std::memory_order_relaxed);
    result.active_connections = active_connections.load(std::memory_order_relaxed);
    result.requests = context.requests.load(std::memory_order_relaxed);
    result.responses = context.responses.load(std::memory_order_relaxed);
    result.errors = context.errors.load(std::memory_order_relaxed);
    result.bytes_in = context.bytes_in.load(std::memory_order_relaxed);
    result.bytes_out = context.bytes_out.load(std::memory_order_relaxed);

    for (const auto &worker : workers) {
        if (worker != nullptr && worker->loop != nullptr) {
            const EventLoopStats stats = worker->loop->stats();
            result.loop.posts += stats.posts;
            result.loop.timer_callbacks += stats.timer_callbacks;
            result.loop.io_events += stats.io_events;
            result.loop.resumes += stats.resumes;
            result.loop.poll_iterations += stats.poll_iterations;
        }
    }
    return result;
}

std::string Server::Impl::process(std::string_view raw_request, std::string_view remote_addr)
{
    ParsedHead head;
    const HeadParseResult parsed = detail::parse_request_head(
        raw_request, 0, context.header_limit, context.path_decoding, head);
    if (parsed != HeadParseResult::Ok)
        return context.error_response(parsed == HeadParseResult::Error ? head.error_status : 400);

    Request request;
    request.method = head.method;
    request.target = head.target;
    request.path = head.path;
    request.version = head.version;
    request.headers = { head.headers.data(), head.header_count };
    request.content_length = head.content_length;
    request.remote_addr = remote_addr;
    request.body_mode = BodyMode::Buffered;

    const std::string_view rest = raw_request.substr(head.consumed);
    std::string body;
    if (head.chunked) {
        if (!decode_chunked(rest, body))
            return context.error_response(400);
        if (context.payload_max != 0 && body.size() > context.payload_max)
            return context.error_response(413);
    } else if (head.content_length > 0) {
        if (static_cast<std::size_t>(head.content_length) > rest.size())
            return context.error_response(400);
        if (context.payload_max != 0 && static_cast<std::size_t>(head.content_length) > context.payload_max)
            return context.error_response(413);
        body.assign(rest.data(), static_cast<std::size_t>(head.content_length));
    }

    if (context.pre_routing) {
        Response pre_response;
        if (context.pre_routing(request, pre_response) == HandlerResponse::Handled) {
            std::string out;
            detail::serialize_fixed_response(pre_response, head.head_only, false, out);
            return out;
        }
    }

    const Router::Route *route = context.router.find(request.method, request.path, request);
    if (route == nullptr)
        return context.error_response(404);

    request.body_mode = route->options.body_mode;
    if (request.body_mode == BodyMode::Streamed) {
        if (head.chunked)
            return context.error_response(400);
        if (head.content_length < 0)
            return context.error_response(400);
    }
    if (request.body_mode == BodyMode::Buffered) {
        request.body = std::move(body);
        request.content_length = static_cast<long long>(request.body.size());
    } else {
        const std::size_t body_size = body.size();
        request.body_source = detail::make_memory_source(std::make_shared<std::string>(std::move(body)));
        request.content_length = static_cast<long long>(body_size);
    }

    Server::Response response;
    try {
        route->handler(request, response);
    } catch (...) {
        response = Server::Response{};
        response.status = 500;
        context.apply_error_body(response, 500);
    }

    if (response.task_taken_over()) {
        std::string out;
        MemoryStreamWriter writer(out);
        EventLoop private_loop;
        request.loop = &private_loop;
        try {
            detail::sync_wait(response.task_take_over_fn()(writer));
        } catch (...) {
            if (out.empty())
                return context.error_response(500);
        }
        return out;
    }

    if (response.async_taken_over()) {
        std::string out;
        boost::asio::io_context io;
        AsyncClientWriter writer([&out](std::string_view data) -> boost::asio::awaitable<bool> {
            out.append(data);
            co_return true;
        });
        boost::asio::co_spawn(
            io,
            [&response, &writer]() -> boost::asio::awaitable<void> {
                try {
                    co_await response.async_take_over_task()(writer);
                } catch (...) {
                }
                co_return;
            },
            boost::asio::detached);
        io.run();
        return out;
    }

    if (response.taken_over()) {
        std::string out;
        ClientWriter writer = [&out](std::string_view data) {
            out.append(data);
            return true;
        };
        response.take_over_task_fn()(writer);
        return out;
    }

    std::string out;
    detail::serialize_fixed_response(response, head.head_only, false, out);
    return out;
}

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
    const auto kind = pattern.find("/:") == std::string::npos ? Router::Kind::Exact : Router::Kind::Params;
    impl_->context.router.add("GET", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::Post(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Router::Kind::Exact : Router::Kind::Params;
    impl_->context.router.add("POST", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::Put(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Router::Kind::Exact : Router::Kind::Params;
    impl_->context.router.add("PUT", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::Delete(std::string pattern, Handler handler, RouteOptions options)
{
    const auto kind = pattern.find("/:") == std::string::npos ? Router::Kind::Exact : Router::Kind::Params;
    impl_->context.router.add("DELETE", kind, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::GetRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->context.router.add("GET", Router::Kind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::PostRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->context.router.add("POST", Router::Kind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::PutRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->context.router.add("PUT", Router::Kind::Regex, std::move(pattern), std::move(handler), options);
    return *this;
}

Server &Server::DeleteRegex(std::string pattern, Handler handler, RouteOptions options)
{
    impl_->context.router.add("DELETE", Router::Kind::Regex, std::move(pattern), std::move(handler),
                              options);
    return *this;
}

Server &Server::set_pre_routing_handler(PreRoutingHandler handler)
{
    impl_->context.pre_routing = std::move(handler);
    return *this;
}

Server &Server::set_payload_max_length(std::size_t bytes)
{
    impl_->context.payload_max = bytes;
    return *this;
}

Server &Server::set_header_limit(std::size_t bytes)
{
    impl_->context.header_limit = bytes > 0 ? bytes : (1 << 20);
    return *this;
}

Server &Server::set_error_body(int status, std::string content_type, std::string body)
{
    impl_->context.error_bodies[status] = { std::move(content_type), std::move(body) };
    return *this;
}

Server &Server::set_read_header_timeout(std::chrono::milliseconds timeout)
{
    impl_->context.read_header_timeout = timeout;
    return *this;
}

Server &Server::set_request_idle_timeout(std::chrono::milliseconds timeout)
{
    impl_->context.request_idle_timeout = timeout;
    return *this;
}

Server &Server::set_client_write_timeout(std::chrono::milliseconds timeout)
{
    impl_->context.client_write_timeout = timeout;
    return *this;
}

Server &Server::set_io_threads(unsigned count)
{
    impl_->requested_threads = count;
    return *this;
}

Server &Server::set_path_decoding(bool enabled)
{
    impl_->context.path_decoding = enabled;
    return *this;
}

Server &Server::set_keep_alive(bool enabled)
{
    impl_->context.keep_alive = enabled;
    return *this;
}

Server &Server::set_queue_watermark(std::size_t high, std::size_t low)
{
    impl_->context.queue_high = high;
    impl_->context.queue_low = low;
    return *this;
}

Server &Server::set_max_connections(std::size_t count)
{
    impl_->context.max_connections = count;
    return *this;
}

Server &Server::set_upstream(Config config)
{
    impl_->context.has_upstream = true;
    impl_->context.upstream_config = std::move(config);
    return *this;
}

Server &Server::set_strategy(StreamStrategy strategy)
{
    impl_->context.default_strategy = strategy;
    return *this;
}

Server &Server::set_tls(std::string cert_file, std::string key_file)
{
    impl_->context.tls_cert = std::move(cert_file);
    impl_->context.tls_key = std::move(key_file);
    return *this;
}

Server &Server::set_ktls(bool enabled)
{
    impl_->context.ktls = enabled;
    return *this;
}

Server &Server::set_cpu_affinity(bool enabled)
{
    impl_->context.cpu_affinity = enabled;
    return *this;
}

Server &Server::set_pump_threads(unsigned count)
{
    StreamPumpPool::configure(count);
    return *this;
}

Server &Server::set_pump_queue_limit(unsigned count)
{
    StreamPumpPool::configure_queue(count);
    return *this;
}

Server &Server::set_pump_idle_timeout(std::chrono::milliseconds timeout)
{
    StreamPumpPool::configure_idle(timeout);
    return *this;
}

bool Server::listen(const std::string &host, int port)
{
    impl_->listen_host = host;
    impl_->listen_port = port;
    return impl_->start();
}

void Server::stop()
{
    impl_->stop();
}

bool Server::is_running() const
{
    return impl_->running.load(std::memory_order_acquire);
}

std::string Server::process(std::string_view raw_request, std::string_view remote_addr)
{
    return impl_->process(raw_request, remote_addr);
}

Server::Stats Server::stats() const
{
    return impl_->stats();
}

} // namespace realurl
