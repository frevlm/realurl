#pragma once

/// Public request/response surface for realurl::Server.
///
/// Header views point into the connection's read buffer and stay valid until
/// the handler (including a take_over_task) returns. Copy anything that must
/// survive the request.

#include "realurl/body_source.hpp"
#include "realurl/detail/task.hpp"
#include "realurl/http2.hpp"

#include <boost/asio/awaitable.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace realurl::detail
{

inline bool iequals(std::string_view left, std::string_view right) noexcept
{
    if (left.size() != right.size())
        return false;
    for (std::size_t i = 0; i < left.size(); ++i) {
        auto a = static_cast<unsigned char>(left[i]);
        auto b = static_cast<unsigned char>(right[i]);
        if (a >= 'A' && a <= 'Z')
            a = static_cast<unsigned char>(a - 'A' + 'a');
        if (b >= 'A' && b <= 'Z')
            b = static_cast<unsigned char>(b - 'A' + 'a');
        if (a != b)
            return false;
    }
    return true;
}

} // namespace realurl::detail

namespace realurl
{
namespace server
{

using Header = std::pair<std::string_view, std::string_view>;

using realurl::detail::iequals;

enum class BodyMode { Buffered, Streamed, StreamedNative };
enum class HandlerResponse { Handled, Unhandled };
enum class StreamStrategy { Auto, Latency, Throughput, Bandwidth, Tunnel };

/// Blocking writer used by the legacy take_over path.
using ClientWriter = std::function<bool(std::string_view)>;

/// Sink behind AsyncClientWriter; provided by the server's compatibility layer.
using AsyncWriterSink = std::function<boost::asio::awaitable<bool>(std::string_view)>;

/// Awaitable writer used by the legacy take_over_async() path.
class AsyncClientWriter
{
public:
    explicit AsyncClientWriter(AsyncWriterSink sink)
        : sink_(std::move(sink))
    {
    }

    boost::asio::awaitable<bool> write(std::string_view data)
    {
        return sink_(data);
    }

private:
    AsyncWriterSink sink_;
};

class BodyReader
{
public:
    virtual ~BodyReader() = default;

    /// Read up to buffer.size() bytes. Returns 0 at end of body.
    virtual Task<std::size_t> read(std::span<char> buffer) = 0;

    /// Bytes still expected, or UINT64_MAX when unknown (chunked).
    virtual std::uint64_t remaining() const noexcept = 0;
};

/// Raw response writer handed to take_over_task handlers.
class StreamWriter
{
public:
    virtual ~StreamWriter() = default;

    /// Send the response head. If content_length is absent the writer chooses
    /// chunked framing (HTTP/1.1) or no Content-Length (HTTP/2).
    virtual Task<void> write_head(int status, std::span<const Header> headers,
                                  std::optional<std::uint64_t> content_length = {}) = 0;

    /// Queue response body bytes. Returns false when the client is gone.
    virtual Task<bool> write(std::string_view data) = 0;

    /// Finish the response body.
    virtual Task<void> end() = 0;

    /// Abort the response direction (HTTP/1.1 close, HTTP/2 RST_STREAM).
    virtual void cancel() = 0;

    virtual bool closed() const noexcept = 0;
};

struct Request
{
    std::string_view method;
    std::string_view target;
    std::string_view path;
    std::string_view version;
    std::span<const Header> headers;
    std::unordered_map<std::string_view, std::string_view> path_params;

    std::string body;                        // Buffered mode
    BodyReader *body_reader = nullptr;       // Streamed mode (native)
    std::shared_ptr<BodySource> body_source; // Streamed mode (legacy shim)
    BodyMode body_mode = BodyMode::Buffered;
    long long content_length = -1;

    std::string_view remote_addr;
    int remote_port = 0;
    EventLoop *loop = nullptr;
    Client *upstream = nullptr;

    std::string_view get_header(std::string_view name) const
    {
        for (const Header &header : headers) {
            if (iequals(header.first, name))
                return header.second;
        }
        return {};
    }

    bool has_header(std::string_view name) const
    {
        for (const Header &header : headers) {
            if (iequals(header.first, name))
                return true;
        }
        return false;
    }

    Task<std::size_t> read_body(std::span<char> buffer) const
    {
        if (body_reader == nullptr)
            co_return 0;
        co_return co_await body_reader->read(buffer);
    }
};

struct Response
{
    int status = 200;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
    std::string_view body_view;
    std::string content_type;

    using TakeOverTask = std::function<void(ClientWriter &)>;
    using AsyncTakeOverTask = std::function<boost::asio::awaitable<void>(AsyncClientWriter &)>;
    using TaskTakeOver = std::function<Task<void>(StreamWriter &)>;

    void set_status(int code)
    {
        status = code;
    }

    void set_header(std::string name, std::string value)
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

    void add_header(std::string name, std::string value)
    {
        headers.emplace_back(std::move(name), std::move(value));
    }

    std::string get_header(std::string_view name) const
    {
        if (iequals(name, "Content-Type"))
            return content_type;
        for (const auto &header : headers) {
            if (iequals(header.first, name))
                return header.second;
        }
        return {};
    }

    void set_content(std::string content, std::string_view type = {})
    {
        body = std::move(content);
        body_view = {};
        if (!type.empty())
            content_type = std::string{ type };
        else if (content_type.empty() && !body.empty())
            content_type = "text/plain";
    }

    /// Zero-copy fast path: the storage must outlive the response write.
    void set_content(std::string_view content, std::string_view type = {})
    {
        body.clear();
        body_view = content;
        if (!type.empty())
            content_type = std::string{ type };
        else if (content_type.empty() && !content.empty())
            content_type = "text/plain";
    }

    void set_content(const char *content, std::string_view type = {})
    {
        set_content(std::string_view{ content != nullptr ? content : "" }, type);
    }

    std::uint64_t body_size() const noexcept
    {
        return body_view.empty() ? body.size() : body_view.size();
    }

    void take_over(TakeOverTask task)
    {
        take_over_ = std::move(task);
        async_take_over_ = nullptr;
        task_take_over_ = nullptr;
    }

    void take_over_async(AsyncTakeOverTask task)
    {
        async_take_over_ = std::move(task);
        take_over_ = nullptr;
        task_take_over_ = nullptr;
    }

    void take_over_task(TaskTakeOver task)
    {
        task_take_over_ = std::move(task);
        take_over_ = nullptr;
        async_take_over_ = nullptr;
    }

    bool taken_over() const
    {
        return static_cast<bool>(take_over_) || static_cast<bool>(async_take_over_) ||
               static_cast<bool>(task_take_over_);
    }

    bool async_taken_over() const
    {
        return static_cast<bool>(async_take_over_);
    }

    bool task_taken_over() const
    {
        return static_cast<bool>(task_take_over_);
    }

    const TakeOverTask &take_over_task_fn() const
    {
        return take_over_;
    }

    const AsyncTakeOverTask &async_take_over_task() const
    {
        return async_take_over_;
    }

    const TaskTakeOver &task_take_over_fn() const
    {
        return task_take_over_;
    }

private:
    TakeOverTask take_over_;
    AsyncTakeOverTask async_take_over_;
    TaskTakeOver task_take_over_;
};

struct RouteOptions
{
    BodyMode body_mode = BodyMode::Buffered;
    StreamStrategy strategy = StreamStrategy::Auto;
};

} // namespace server
} // namespace realurl
