#include "server_internal.hpp"

#include "realurl/fd_writer.hpp"
#include "realurl/stream_pump_pool.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <memory>
#include <netdb.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

namespace realurl::detail
{
namespace
{

struct PayloadTooLarge {
};

struct BadRequest {
};

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

class H1BodyReader final : public BodyReader
{
public:
    H1BodyReader(H1Connection &connection, bool chunked, long long length, std::size_t limit,
                 std::chrono::milliseconds idle)
        : connection_(&connection)
        , chunked_(chunked)
        , remaining_(chunked ? 0 : static_cast<std::uint64_t>(std::max<long long>(length, 0)))
        , limit_(limit)
        , idle_(idle)
    {
    }

    Task<std::size_t> read(std::span<char> out) override
    {
        if (out.empty())
            co_return 0;

        if (!chunked_) {
            if (remaining_ == 0)
                co_return 0;
            if (connection_->available().empty()) {
                if (!co_await connection_->fill(idle_))
                    throw std::system_error(ECONNRESET, std::generic_category(), "http body");
            }
            const std::string_view available = connection_->available();
            const std::size_t n = static_cast<std::size_t>(
                std::min<std::uint64_t>({ available.size(), out.size(), remaining_ }));
            if (n == 0)
                throw BadRequest{};
            std::memcpy(out.data(), available.data(), n);
            connection_->consume(n);
            remaining_ -= n;
            total_ += n;
            check_limit();
            co_return n;
        }

        for (;;) {
            switch (state_) {
            case State::Size: {
                const std::string_view available = connection_->available();
                const auto line_end = available.find("\r\n");
                if (line_end == std::string_view::npos) {
                    if (available.size() > 4096)
                        throw BadRequest{};
                    if (!co_await connection_->fill(idle_))
                        throw BadRequest{};
                    continue;
                }

                std::string_view line = available.substr(0, line_end);
                const auto extension = line.find(';');
                if (extension != std::string_view::npos)
                    line = line.substr(0, extension);
                line = trim_ws(line);
                if (line.empty())
                    throw BadRequest{};

                std::uint64_t size = 0;
                for (char c : line) {
                    const int digit = hex_digit(c);
                    if (digit < 0)
                        throw BadRequest{};
                    if (size > (UINT64_MAX - static_cast<std::uint64_t>(digit)) / 16)
                        throw BadRequest{};
                    size = size * 16 + static_cast<std::uint64_t>(digit);
                }

                connection_->consume(line_end + 2);
                if (size == 0) {
                    state_ = State::Trailers;
                    continue;
                }
                remaining_ = size;
                total_ += size;
                check_limit();
                state_ = State::Data;
                continue;
            }

            case State::Data: {
                if (remaining_ == 0) {
                    state_ = State::Crlf;
                    continue;
                }
                if (connection_->available().empty()) {
                    if (!co_await connection_->fill(idle_))
                        throw BadRequest{};
                }
                const std::string_view available = connection_->available();
                const std::size_t n = static_cast<std::size_t>(
                    std::min<std::uint64_t>({ available.size(), out.size(), remaining_ }));
                if (n == 0)
                    throw BadRequest{};
                std::memcpy(out.data(), available.data(), n);
                connection_->consume(n);
                remaining_ -= n;
                if (remaining_ == 0)
                    state_ = State::Crlf;
                co_return n;
            }

            case State::Crlf: {
                if (connection_->available().size() < 2) {
                    if (!co_await connection_->fill(idle_))
                        throw BadRequest{};
                }
                const std::string_view available = connection_->available();
                if (available.size() < 2 || available[0] != '\r' || available[1] != '\n')
                    throw BadRequest{};
                connection_->consume(2);
                state_ = State::Size;
                continue;
            }

            case State::Trailers: {
                const std::string_view available = connection_->available();
                const auto line_end = available.find("\r\n");
                if (line_end == std::string_view::npos) {
                    if (available.size() > 65536)
                        throw BadRequest{};
                    if (!co_await connection_->fill(idle_))
                        throw BadRequest{};
                    continue;
                }
                if (line_end == 0) {
                    connection_->consume(2);
                    state_ = State::Done;
                    co_return 0;
                }
                connection_->consume(line_end + 2);
                continue;
            }

            case State::Done:
                co_return 0;
            }
        }
    }

    std::uint64_t remaining() const noexcept override
    {
        return chunked_ ? UINT64_MAX : remaining_;
    }

private:
    enum class State { Size, Data, Crlf, Trailers, Done };

    void check_limit() const
    {
        if (limit_ != 0 && total_ > limit_)
            throw PayloadTooLarge{};
    }

    H1Connection *connection_;
    bool chunked_ = false;
    std::uint64_t remaining_ = 0;
    std::uint64_t total_ = 0;
    std::size_t limit_ = 0;
    std::chrono::milliseconds idle_{};
    State state_ = State::Size;
};

} // namespace

class H1StreamWriter final : public StreamWriter
{
public:
    H1StreamWriter(Transport &stream, bool head_only, bool keep_alive,
                   std::chrono::milliseconds timeout, int status)
        : stream_(&stream)
        , head_only_(head_only)
        , keep_alive_(keep_alive)
        , timeout_(timeout)
        , status_(status)
    {
    }

    Task<void> write_head(int status, std::span<const Header> headers,
                          std::optional<std::uint64_t> content_length) override
    {
        if (head_sent_)
            co_return;

        status_ = status;
        head_.clear();
        head_.reserve(256);
        head_.append("HTTP/1.1 ");
        head_.append(std::to_string(status));
        head_.push_back(' ');
        head_.append(reason_phrase(status));
        head_.append("\r\n");

        for (const Header &header : headers) {
            if (iequals(header.first, "Content-Length") || iequals(header.first, "Connection") ||
                iequals(header.first, "Transfer-Encoding"))
                continue;
            head_.append(header.first);
            head_.append(": ");
            head_.append(header.second);
            head_.append("\r\n");
        }

        head_.append("Date: ");
        head_.append(current_date());
        head_.append("\r\n");

        const bool body_allowed = status_allows_body(status);
        if (content_length.has_value() && body_allowed) {
            head_.append("Content-Length: ");
            head_.append(std::to_string(*content_length));
            head_.append("\r\n");
        } else if (body_allowed && !head_only_) {
            head_.append("Transfer-Encoding: chunked\r\n");
            chunked_ = true;
        }

        head_.append("Connection: ");
        head_.append(keep_alive_ ? "keep-alive" : "close");
        head_.append("\r\n\r\n");

        head_sent_ = true;
        co_await write_raw(head_);
    }

    Task<bool> write(std::string_view data) override
    {
        if (!head_sent_)
            co_await write_head(status_, {}, {});
        if (head_only_ || ended_ || closed_)
            co_return !closed_;

        if (!chunked_)
            co_return co_await write_raw(data);

        std::string framed;
        framed.reserve(data.size() + 24);
        char size_line[24];
        const int length = std::snprintf(size_line, sizeof(size_line), "%zx\r\n", data.size());
        if (length > 0)
            framed.append(size_line, static_cast<std::size_t>(length));
        framed.append(data);
        framed.append("\r\n");
        co_return co_await write_raw(framed);
    }

    Task<void> end() override
    {
        if (ended_ || closed_)
            co_return;
        ended_ = true;
        if (chunked_ && head_sent_ && !head_only_)
            co_await write_raw("0\r\n\r\n");
    }

    void cancel() override
    {
        closed_ = true;
        if (stream_ != nullptr)
            stream_->close();
    }

    bool closed() const noexcept override
    {
        return closed_ || stream_ == nullptr || stream_->fd() < 0;
    }

    bool head_sent() const noexcept
    {
        return head_sent_;
    }

    bool ended() const noexcept
    {
        return ended_;
    }

private:
    Task<bool> write_raw(std::string_view data)
    {
        if (closed_ || stream_ == nullptr)
            co_return false;
        if (data.empty())
            co_return true;
        try {
            co_await stream_->write_all(data, timeout_);
        } catch (...) {
            closed_ = true;
            co_return false;
        }
        co_return true;
    }

    Transport *stream_;
    bool head_only_ = false;
    bool keep_alive_ = false;
    bool chunked_ = false;
    bool head_sent_ = false;
    bool ended_ = false;
    bool closed_ = false;
    std::chrono::milliseconds timeout_{};
    int status_ = 200;
    std::string head_;
};

namespace
{

class GateAwaiter
{
public:
    GateAwaiter(std::shared_ptr<StreamGate> gate, EventLoop &loop)
        : gate_(std::move(gate))
        , loop_(&loop)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        gate_->async_wait([this] {
            loop_->post([this] {
                auto handle = handle_;
                handle_ = {};
                handle.resume();
            });
        });
    }

    void await_resume() const noexcept
    {
    }

private:
    std::shared_ptr<StreamGate> gate_;
    EventLoop *loop_;
    std::coroutine_handle<> handle_{};
};

} // namespace

H1Connection::H1Connection(ServerContext &context, EventLoop &loop,
                           std::unique_ptr<Transport> transport, std::string initial, int family,
                           sockaddr_storage peer, Client *upstream)
    : context_(context)
    , loop_(loop)
    , transport_(std::move(transport))
    , family_(family)
    , upstream_(upstream)
    , buffer_(std::move(initial))
{
    char host[NI_MAXHOST] = {};
    char service[NI_MAXSERV] = {};
    if (::getnameinfo(reinterpret_cast<sockaddr *>(&peer), sizeof(peer), host, sizeof(host), service,
                      sizeof(service), NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
        remote_addr_ = host;
        remote_port_ = std::atoi(service);
    }

    buffer_.reserve(std::max<std::size_t>(65536, buffer_.size()));
}

H1Connection::H1Connection(ServerContext &context, EventLoop &loop, TcpStream &&stream,
                           std::string initial, int family, sockaddr_storage peer, Client *upstream)
    : H1Connection(context, loop, std::make_unique<TcpStream>(std::move(stream)), std::move(initial),
                   family, peer, upstream)
{
}

H1Connection::H1Connection(ServerContext &context, EventLoop &loop, int fd, int family,
                           sockaddr_storage peer, Client *upstream)
    : H1Connection(context, loop, std::make_unique<TcpStream>(loop, fd), {}, family, peer, upstream)
{
}

H1Connection::~H1Connection() = default;

void H1Connection::compact()
{
    if (offset_ == 0)
        return;
    buffer_.erase(0, offset_);
    offset_ = 0;
}

void H1Connection::consume(std::size_t count) noexcept
{
    offset_ += count;
    if (offset_ >= buffer_.size()) {
        buffer_.clear();
        offset_ = 0;
    } else if (offset_ > 65536 && offset_ > buffer_.size() / 2) {
        buffer_.erase(0, offset_);
        offset_ = 0;
    }
}

Task<bool> H1Connection::fill(std::chrono::milliseconds idle)
{
    if (offset_ > 0)
        compact();

    if (buffer_.size() == buffer_.capacity()) {
        if (buffer_.capacity() >= (1u << 20))
            throw BadRequest{};
        buffer_.reserve(buffer_.capacity() * 2);
    }

    const std::size_t space = buffer_.capacity() - buffer_.size();
    const std::size_t old_size = buffer_.size();
    buffer_.resize(old_size + space);

    std::size_t n = 0;
    try {
        n = co_await transport_->read({ buffer_.data() + old_size, space }, idle);
    } catch (...) {
        buffer_.resize(old_size);
        throw;
    }

    if (n == 0) {
        buffer_.resize(old_size);
        co_return false;
    }
    buffer_.resize(old_size + n);
    context_.bytes_in.fetch_add(n, std::memory_order_relaxed);
    co_return true;
}

Task<bool> H1Connection::read_head()
{
    for (;;) {
        const std::string_view available = this->available();
        const auto terminator = available.find("\r\n\r\n");
        if (terminator != std::string_view::npos) {
            const std::size_t head_length = terminator + 4;
            if (head_length > context_.header_limit) {
                head_ = ParsedHead{};
                head_.error_status = 431;
                co_return true;
            }
            head_storage_.assign(available.data(), head_length);
            consume(head_length);

            const HeadParseResult result = parse_request_head(head_storage_, 0, context_.header_limit,
                                                              context_.path_decoding, head_);
            if (result == HeadParseResult::Error && head_.error_status == 0)
                head_.error_status = 400;
            co_return true;
        }

        if (available.size() > context_.header_limit) {
            head_ = ParsedHead{};
            head_.error_status = 431;
            co_return true;
        }

        if (!co_await fill(context_.read_header_timeout))
            co_return false;
    }
}

void H1Connection::build_request()
{
    request_ = Request{};
    request_.method = head_.method;
    request_.target = head_.target;
    request_.path = head_.path;
    request_.version = head_.version;
    request_.headers = { head_.headers.data(), head_.header_count };
    request_.content_length = head_.content_length;
    request_.remote_addr = remote_addr_;
    request_.remote_port = remote_port_;
    request_.loop = &loop_;
    request_.upstream = upstream_;
}

Task<bool> H1Connection::read_body(std::shared_ptr<std::string> &data)
{
    data = std::make_shared<std::string>();
    if (head_.content_length > 0 && head_.content_length < (1 << 20))
        data->reserve(static_cast<std::size_t>(head_.content_length));

    H1BodyReader reader(*this, head_.chunked, head_.content_length, context_.payload_max,
                        context_.request_idle_timeout);
    char buffer[16384];
    for (;;) {
        const std::size_t n = co_await reader.read({ buffer, sizeof(buffer) });
        if (n == 0)
            break;
        data->append(buffer, n);
    }
    co_return true;
}

Task<void> H1Connection::write_error(int status, bool head_only)
{
    Response response;
    response.status = status;
    context_.apply_error_body(response, status);

    std::string out;
    serialize_fixed_response(response, head_only, false, out);
    context_.errors.fetch_add(1, std::memory_order_relaxed);
    context_.bytes_out.fetch_add(out.size(), std::memory_order_relaxed);
    try {
        co_await transport_->write_all(out, context_.client_write_timeout);
    } catch (...) {
    }
    close_ = true;
}

Task<bool> H1Connection::dispatch_handler(const Router::Route &route)
{
    response_ = Response{};
    try {
        route.handler(request_, response_);
    } catch (...) {
        if (!response_.taken_over()) {
            response_ = Response{};
            response_.status = 500;
            context_.apply_error_body(response_, 500);
        } else {
            close_ = true;
            co_return false;
        }
    }

    if (response_.task_taken_over()) {
        writer_ = std::make_unique<H1StreamWriter>(*transport_, head_.head_only, false,
                                                   context_.client_write_timeout, response_.status);
        bool failed = false;
        try {
            co_await response_.task_take_over_fn()(*writer_);
        } catch (...) {
            failed = true;
        }
        if (failed) {
            if (!writer_->head_sent())
                co_await write_error(500, head_.head_only);
            else
                close_ = true;
            co_return false;
        }
        if (!writer_->ended())
            co_await writer_->end();
        close_ = true;
        co_return false;
    }

    if (response_.async_taken_over()) {
        if (context_.tls_context != nullptr) {
            co_await write_error(501, head_.head_only);
            co_return false;
        }
        co_await async_takeover(response_);
        co_return false;
    }

    if (response_.taken_over()) {
        if (context_.tls_context != nullptr) {
            co_await write_error(501, head_.head_only);
            co_return false;
        }
        co_await legacy_takeover(response_);
        co_return false;
    }

    const bool keep_alive = head_.keep_alive && context_.keep_alive;
    std::string out;
    serialize_fixed_response(response_, head_.head_only, keep_alive, out);
    context_.responses.fetch_add(1, std::memory_order_relaxed);
    context_.bytes_out.fetch_add(out.size(), std::memory_order_relaxed);
    try {
        co_await transport_->write_all(out, context_.client_write_timeout);
    } catch (...) {
        close_ = true;
        co_return false;
    }
    co_return keep_alive;
}

Task<void> H1Connection::legacy_takeover(const Response &response)
{
    auto response_started = std::make_shared<std::atomic_bool>(false);
    ClientWriter writer =
        client_writer_from_fd(transport_->fd(), context_.client_write_timeout, response_started);

    auto task = response.take_over_task_fn();
    auto gate = stream_pump_pool().submit(
        [task = std::move(task), writer = std::move(writer)]() mutable { task(writer); },
        context_.compat_io->get_executor());

    if (gate == nullptr) {
        Response unavailable;
        unavailable.status = 503;
        unavailable.set_header("Retry-After", "1");
        context_.apply_error_body(unavailable, 503);

        std::string out;
        serialize_fixed_response(unavailable, head_.head_only, false, out);
        context_.errors.fetch_add(1, std::memory_order_relaxed);
        context_.bytes_out.fetch_add(out.size(), std::memory_order_relaxed);
        try {
            co_await transport_->write_all(out, context_.client_write_timeout);
        } catch (...) {
        }
        close_ = true;
        co_return;
    }

    co_await GateAwaiter(std::move(gate), loop_);
    close_ = true;
}

Task<void> H1Connection::async_takeover(const Response &response)
{
    auto response_started = std::make_shared<std::atomic_bool>(false);
    auto error = std::make_shared<std::exception_ptr>();
    ClientWriter blocking =
        client_writer_from_fd(transport_->fd(), context_.client_write_timeout, response_started);

    AsyncClientWriter writer([blocking = std::move(blocking)](std::string_view data)
                                 -> boost::asio::awaitable<bool> {
        if (data.empty())
            co_return true;
        co_return blocking(data);
    });

    auto gate = StreamGate::create(context_.compat_io->get_executor());
    auto task = response.async_take_over_task();
    boost::asio::co_spawn(
        *context_.compat_io,
        [task = std::move(task), &writer, gate, error]() -> boost::asio::awaitable<void> {
            try {
                co_await task(writer);
            } catch (...) {
                *error = std::current_exception();
            }
            gate->finish();
            co_return;
        },
        boost::asio::detached);

    co_await GateAwaiter(gate, loop_);

    if (*error && !response_started->load(std::memory_order_acquire))
        co_await write_error(500, head_.head_only);
    close_ = true;
}

void H1Connection::reset_request()
{
    request_ = Request{};
    response_ = Response{};
    writer_.reset();
    body_reader_.reset();
    head_ = ParsedHead{};
}

Task<void> H1Connection::run()
{
    int error_status = 0;
    try {
        for (;;) {
            if (!co_await read_head())
                break;

            if (head_.error_status != 0) {
                co_await write_error(head_.error_status);
                break;
            }

            build_request();
            context_.requests.fetch_add(1, std::memory_order_relaxed);

            if (context_.pre_routing) {
                Response pre_response;
                if (context_.pre_routing(request_, pre_response) == HandlerResponse::Handled) {
                    const bool keep_alive = head_.keep_alive && context_.keep_alive;
                    std::string out;
                    serialize_fixed_response(pre_response, head_.head_only, keep_alive, out);
                    try {
                        co_await transport_->write_all(out, context_.client_write_timeout);
                    } catch (...) {
                        break;
                    }
                    if (!keep_alive)
                        break;
                    reset_request();
                    continue;
                }
            }

            const Router::Route *route =
                context_.router.find(request_.method, request_.path, request_);
            if (route == nullptr) {
                co_await write_error(404, head_.head_only);
                break;
            }
            request_.body_mode = route->options.body_mode;
            if (request_.body_mode == BodyMode::Streamed) {
                if (head_.chunked || head_.content_length < 0) {
                    co_await write_error(400, head_.head_only);
                    break;
                }
            }

            if (context_.payload_max != 0 && head_.content_length > 0 &&
                static_cast<std::size_t>(head_.content_length) > context_.payload_max) {
                co_await write_error(413, head_.head_only);
                break;
            }

            const bool has_body = head_.chunked || head_.content_length > 0;
            if (head_.expect_continue && has_body) {
                static constexpr std::string_view kContinue = "HTTP/1.1 100 Continue\r\n\r\n";
                try {
                    co_await transport_->write_all(kContinue, context_.client_write_timeout);
                } catch (...) {
                    break;
                }
            }

            if (request_.body_mode == BodyMode::StreamedNative) {
                body_reader_ = std::make_unique<H1BodyReader>(
                    *this, head_.chunked, head_.content_length, context_.payload_max,
                    context_.request_idle_timeout);
                request_.body_reader = body_reader_.get();
                request_.content_length = head_.content_length;
                close_ = true;
            } else {
                std::shared_ptr<std::string> body;
                if (has_body)
                    co_await read_body(body);
                else
                    body = std::make_shared<std::string>();

                if (request_.body_mode == BodyMode::Buffered) {
                    request_.body = std::move(*body);
                    request_.content_length = static_cast<long long>(request_.body.size());
                } else {
                    request_.body_source = make_memory_source(body);
                    request_.content_length = static_cast<long long>(body->size());
                }
            }

            const bool keep_alive = co_await dispatch_handler(*route);
            if (!keep_alive || !head_.keep_alive || !context_.keep_alive || close_)
                break;

            reset_request();
        }
    } catch (const PayloadTooLarge &) {
        error_status = 413;
    } catch (const BadRequest &) {
        error_status = 400;
    } catch (const std::system_error &error) {
        if (error.code().value() == ETIMEDOUT)
            error_status = 408;
        else if (error.code().value() != ECONNRESET)
            error_status = 400;
    } catch (...) {
        if (!writer_ || !writer_->head_sent())
            error_status = 500;
    }

    if (error_status != 0)
        co_await write_error(error_status, head_.head_only);

    transport_->close();
}

} // namespace realurl::detail
