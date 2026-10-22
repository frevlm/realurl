#include "realurl/detail/connection.hpp"

#include "realurl/detail/native_codec.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <system_error>
#include <utility>

namespace realurl::detail
{
using namespace h2;

namespace
{

constexpr std::uint32_t kMaxWindow = 2147483647u;

std::uint32_t read_be32(const char *data)
{
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(data[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[2])) << 8) |
           static_cast<std::uint32_t>(static_cast<unsigned char>(data[3]));
}

std::uint16_t read_be16(const char *data)
{
    return static_cast<std::uint16_t>((static_cast<unsigned char>(data[0]) << 8) |
                                      static_cast<unsigned char>(data[1]));
}

bool is_connection_header(std::string_view name)
{
    return name == "connection" || name == "keep-alive" || name == "proxy-connection" ||
           name == "transfer-encoding" || name == "upgrade";
}

bool iequals(std::string_view left, std::string_view right) noexcept
{
    if (left.size() != right.size())
        return false;
    for (std::size_t i = 0; i < left.size(); ++i) {
        const auto a = static_cast<unsigned char>(left[i]);
        const auto b = static_cast<unsigned char>(right[i]);
        const auto lower = [](unsigned char c) {
            return c >= 'A' && c <= 'Z' ? static_cast<unsigned char>(c - 'A' + 'a') : c;
        };
        if (lower(a) != lower(b))
            return false;
    }
    return true;
}

bool parse_uint(std::string_view text, std::uint64_t &out) noexcept
{
    if (text.empty())
        return false;
    std::uint64_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9')
            return false;
        const std::uint64_t digit = static_cast<std::uint64_t>(c - '0');
        if (value > (UINT64_MAX - digit) / 10)
            return false;
        value = value * 10 + digit;
    }
    out = value;
    return true;
}

Task<void> read_exact(Transport &transport, std::span<char> buffer,
                      std::chrono::milliseconds timeout)
{
    std::size_t offset = 0;
    while (offset < buffer.size()) {
        const std::size_t n = co_await transport.read(buffer.subspan(offset), timeout);
        if (n == 0)
            throw NativeUnsupported();
        offset += n;
    }
}

Task<void> expect_native_hello(Transport &transport, std::chrono::milliseconds timeout)
{
    char header[9] = {};
    try {
        co_await read_exact(transport, { header, sizeof(header) }, timeout);
    } catch (const std::system_error &) {
        throw NativeUnsupported();
    }

    const auto *raw = reinterpret_cast<const unsigned char *>(header);
    const std::uint32_t length = (static_cast<std::uint32_t>(raw[0]) << 16) |
                                 (static_cast<std::uint32_t>(raw[1]) << 8) |
                                 static_cast<std::uint32_t>(raw[2]);
    const std::uint32_t stream_id = (static_cast<std::uint32_t>(raw[5]) << 24) |
                                    (static_cast<std::uint32_t>(raw[6]) << 16) |
                                    (static_cast<std::uint32_t>(raw[7]) << 8) |
                                    static_cast<std::uint32_t>(raw[8]);
    if (raw[3] != kNativeHelloType || length != kNativeHelloPayloadSize ||
        (stream_id & 0x7fffffffu) != 0)
        throw NativeUnsupported();

    char payload[kNativeHelloPayloadSize] = {};
    try {
        co_await read_exact(transport, { payload, sizeof(payload) }, timeout);
    } catch (const std::system_error &) {
        throw NativeUnsupported();
    }

    if (std::memcmp(payload, kNativeHelloMagic.data(), kNativeHelloMagic.size()) != 0 ||
        static_cast<unsigned char>(payload[8]) != kNativeVersionMajor ||
        static_cast<unsigned char>(payload[9]) != kNativeVersionMinor)
        throw NativeUnsupported();
}

} // namespace

class H2StreamImpl::HeadAwaiter {
public:
    HeadAwaiter(H2StreamImpl &stream, std::chrono::milliseconds timeout)
        : stream_(stream)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return stream_.head_ready_;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_head(this, handle, timeout_);
    }

    ResponseHead await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 head");
        return stream_.head_result_;
    }

private:
    friend class H2StreamImpl;

    static void on_timeout(void *context)
    {
        auto *waiter = static_cast<HeadAwaiter *>(context);
        if (!waiter->done_) {
            waiter->error_ = ETIMEDOUT;
            waiter->stream_.complete_head(waiter);
        }
    }

    H2StreamImpl &stream_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerNode timer_;
    int error_ = 0;
    bool done_ = false;
};

class H2StreamImpl::ReadAwaiter {
public:
    ReadAwaiter(H2StreamImpl &stream, std::span<char> buffer, std::chrono::milliseconds idle)
        : stream_(stream)
        , buffer_(buffer)
        , idle_(idle)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_read(this, handle);
    }

    std::size_t await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 read");
        return n_;
    }

private:
    friend class H2StreamImpl;

    static void on_timeout(void *context)
    {
        auto *waiter = static_cast<ReadAwaiter *>(context);
        if (!waiter->done_) {
            waiter->error_ = ETIMEDOUT;
            waiter->stream_.complete_read(waiter);
        }
    }

    H2StreamImpl &stream_;
    std::span<char> buffer_;
    std::chrono::milliseconds idle_;
    std::coroutine_handle<> handle_{};
    TimerNode timer_;
    std::size_t n_ = 0;
    int error_ = 0;
    bool done_ = false;
};

class H2StreamImpl::WriteAwaiter {
public:
    WriteAwaiter(H2StreamImpl &stream, std::chrono::milliseconds timeout)
        : stream_(stream)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_write(this, handle, timeout_);
    }

    void await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 write");
    }

private:
    friend class H2StreamImpl;

    static void on_timeout(void *context)
    {
        auto *waiter = static_cast<WriteAwaiter *>(context);
        if (!waiter->done_) {
            waiter->error_ = ETIMEDOUT;
            waiter->stream_.complete_write(waiter);
        }
    }

    H2StreamImpl &stream_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerNode timer_;
    int error_ = 0;
    bool done_ = false;
};

class H2StreamImpl::EndAwaiter {
public:
    EndAwaiter(H2StreamImpl &stream, std::chrono::milliseconds timeout)
        : stream_(stream)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return stream_.end_sent_;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_end(this, handle, timeout_);
    }

    void await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 end");
    }

private:
    friend class H2StreamImpl;

    static void on_timeout(void *context)
    {
        auto *waiter = static_cast<EndAwaiter *>(context);
        if (!waiter->done_) {
            waiter->error_ = ETIMEDOUT;
            waiter->stream_.complete_end(waiter);
        }
    }

    H2StreamImpl &stream_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerNode timer_;
    int error_ = 0;
    bool done_ = false;
};

H2StreamImpl::H2StreamImpl(H2Connection &connection, std::uint32_t id, std::uint32_t initial_window) noexcept
    : connection_(&connection)
    , loop_(&connection.loop())
    , id_(id)
    , send_window_(initial_window)
{
}

void H2StreamImpl::start_head(HeadAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout)
{
    if (closed_ || error_ != 0) {
        waiter->error_ = error_ != 0 ? error_ : ECANCELED;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    head_waiter_ = waiter;
    waiter->handle_ = handle;
    if (timeout.count() > 0) {
        loop_->schedule(waiter->timer_, timeout, &HeadAwaiter::on_timeout, waiter);
    }
}

void H2StreamImpl::complete_head(HeadAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    loop_->cancel(waiter->timer_);
    if (head_waiter_ == waiter)
        head_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2StreamImpl::start_read(ReadAwaiter *waiter, std::coroutine_handle<> handle)
{
    if (closed_ || error_ != 0) {
        waiter->error_ = error_ != 0 ? error_ : ECANCELED;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    read_waiter_ = waiter;
    waiter->handle_ = handle;
    if (waiter->idle_.count() > 0) {
        loop_->schedule(waiter->timer_, waiter->idle_, &ReadAwaiter::on_timeout, waiter);
    }
}

void H2StreamImpl::complete_read(ReadAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    loop_->cancel(waiter->timer_);
    if (read_waiter_ == waiter)
        read_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2StreamImpl::start_write(WriteAwaiter *waiter, std::coroutine_handle<> handle,
                               std::chrono::milliseconds timeout)
{
    if (closed_ || local_closed_) {
        waiter->error_ = error_ != 0 ? error_ : EPIPE;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    write_waiter_ = waiter;
    waiter->handle_ = handle;
    if (timeout.count() > 0) {
        loop_->schedule(waiter->timer_, timeout, &WriteAwaiter::on_timeout, waiter);
    }
}

void H2StreamImpl::complete_write(WriteAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    loop_->cancel(waiter->timer_);
    if (write_waiter_ == waiter)
        write_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2StreamImpl::start_end(EndAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout)
{
    if (end_sent_ || closed_) {
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    end_waiter_ = waiter;
    waiter->handle_ = handle;
    if (timeout.count() > 0) {
        loop_->schedule(waiter->timer_, timeout, &EndAwaiter::on_timeout, waiter);
    }
}

void H2StreamImpl::complete_end(EndAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    loop_->cancel(waiter->timer_);
    if (end_waiter_ == waiter)
        end_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

Task<ResponseHead> H2StreamImpl::head(std::chrono::milliseconds timeout)
{
    if (head_ready_)
        co_return head_result_;
    if (error_ != 0)
        throw std::system_error(error_, std::generic_category(), "h2 head");
    if (closed_)
        throw std::system_error(ECANCELED, std::generic_category(), "h2 stream closed");
    co_return co_await HeadAwaiter(*this, timeout);
}

Task<std::size_t> H2StreamImpl::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    if (buffer.empty())
        throw std::invalid_argument("h2 read: empty buffer");

    if (!recv_queue_.empty()) {
        const std::size_t n = recv_queue_.read(buffer);
        flush_window();
        co_return n;
    }

    if (error_ != 0)
        throw std::system_error(error_, std::generic_category(), "h2 read");
    if (remote_closed_ || closed_)
        co_return 0;

    co_return co_await ReadAwaiter(*this, buffer, idle);
}

Task<void> H2StreamImpl::read_all(std::string &out, std::chrono::milliseconds idle)
{
    for (;;) {
        if (!recv_queue_.empty()) {
            recv_queue_.drain_to(out);
            flush_window();
        }
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 read");
        if (remote_closed_ || closed_)
            co_return;
        // An empty buffer parks until data, END_STREAM or an error arrives;
        // on_data queues the bytes for the next drain.
        co_await ReadAwaiter(*this, {}, idle);
    }
}

Task<void> H2StreamImpl::write(std::string_view data, std::chrono::milliseconds timeout)
{
    if (closed_ || local_closed_)
        throw std::system_error(EPIPE, std::generic_category(), "h2 write");

    while (!data.empty()) {
        const std::size_t pending = send_queue_.size();
        const std::size_t space = pending < kSendHighWater ? kSendHighWater - pending : 0;
        if (space == 0) {
            if (connection_ != nullptr && !in_write_queue_ && !blocked_on_window_) {
                connection_->enqueue_write(id_);
            }
            co_await WriteAwaiter(*this, timeout);
            if (closed_ || local_closed_)
                throw std::system_error(error_ != 0 ? error_ : EPIPE, std::generic_category(), "h2 write");
            continue;
        }

        const std::size_t n = std::min(space, data.size());
        send_queue_.append(data.substr(0, n));
        data.remove_prefix(n);

        if (connection_ != nullptr && !in_write_queue_ && !blocked_on_window_) {
            connection_->enqueue_write(id_);
        }
    }

    if (connection_ != nullptr) {
        if (!in_write_queue_ && !blocked_on_window_) {
            connection_->enqueue_write(id_);
        } else {
            connection_->schedule_flush();
        }
    }
    co_return;
}

Task<void> H2StreamImpl::end(std::chrono::milliseconds timeout)
{
    if (end_sent_ || closed_)
        co_return;

    end_pending_ = true;
    if (connection_ != nullptr) {
        if (!in_write_queue_ && !blocked_on_window_) {
            connection_->enqueue_write(id_);
        } else {
            connection_->schedule_flush();
        }
    }

    if (end_sent_)
        co_return;
    co_await EndAwaiter(*this, timeout);
}

void H2StreamImpl::cancel()
{
    if (closed_)
        return;
    if (connection_ != nullptr) {
        connection_->send_rst(id_, ErrorCode::Cancel);
        connection_->schedule_flush();
    }
    fail(ECANCELED);
}

void H2StreamImpl::on_head(ResponseHead &&head, bool end_stream)
{
    if (closed_)
        return;

    if (head_ready_) {
        // Trailers: ignored in this version.
    } else {
        head_result_ = std::move(head);
        head_ready_ = true;
        if (head_waiter_ != nullptr)
            complete_head(head_waiter_);
    }

    if (end_stream || head_result_.status == 204 || head_result_.status == 304) {
        remote_closed_ = true;
        if (read_waiter_ != nullptr && recv_queue_.empty()) {
            if (read_waiter_->n_ == 0)
                complete_read(read_waiter_);
        }
        check_closed();
    }
}

void H2StreamImpl::on_data(std::string_view data, bool end_stream, std::uint32_t flow_bytes)
{
    if (closed_)
        return;

    recv_unacked_ += flow_bytes;

    if (read_waiter_ != nullptr) {
        ReadAwaiter *waiter = read_waiter_;
        const std::size_t n = std::min(data.size(), waiter->buffer_.size());
        if (n > 0)
            std::memcpy(waiter->buffer_.data(), data.data(), n);
        waiter->n_ = n;
        recv_queue_.append(data.substr(n));
        complete_read(waiter);
    } else {
        recv_queue_.append(data);
    }

    if (end_stream)
        remote_closed_ = true;

    flush_window();
    check_closed();
}

void H2StreamImpl::on_rst(ErrorCode code)
{
    (void)code;
    fail(ECONNRESET);
}

void H2StreamImpl::on_window_update(std::uint32_t increment)
{
    if (send_window_ + static_cast<std::int64_t>(increment) > static_cast<std::int64_t>(kMaxWindow)) {
        fail(EPROTO);
        return;
    }
    send_window_ += increment;
    if (blocked_on_window_ && connection_ != nullptr && send_window_ > 0) {
        blocked_on_window_ = false;
        if (!in_write_queue_) {
            connection_->enqueue_write(id_);
        }
    } else if (connection_ != nullptr) {
        connection_->schedule_flush();
    }
}

void H2StreamImpl::on_connection_closed(int error)
{
    fail(error != 0 ? error : ECONNRESET);
}

void H2StreamImpl::on_settings_initial_window(std::int32_t delta)
{
    send_window_ += delta;
    if (blocked_on_window_ && connection_ != nullptr && send_window_ > 0) {
        blocked_on_window_ = false;
        if (!in_write_queue_) {
            connection_->enqueue_write(id_);
        }
    } else if (connection_ != nullptr) {
        connection_->schedule_flush();
    }
}

void H2StreamImpl::flush_window()
{
    if (connection_ == nullptr || closed_)
        return;
    if (recv_queue_.size() >= kRecvHighWater)
        return;
    if (recv_unacked_ >= kWindowUpdateThreshold) {
        connection_->send_window_update(id_, recv_unacked_);
        recv_unacked_ = 0;
    }
}

PumpResult H2StreamImpl::pump_one_frame()
{
    if (connection_ == nullptr || closed_)
        return PumpResult::NoWork;

    if (send_queue_.empty() && !end_pending_)
        return PumpResult::NoWork;

    std::string_view chunk;
    if (!send_queue_.empty()) {
        const std::int64_t window = std::min<std::int64_t>(connection_->send_window(), send_window_);
        if (window <= 0)
            return PumpResult::WindowFull;
        chunk = send_queue_.front(std::min<std::size_t>(static_cast<std::size_t>(window),
                                                        connection_->max_frame_size()));
    }

    // END_STREAM rides on the frame that carries the last queued byte.
    const bool end_stream = end_pending_ && chunk.size() == send_queue_.size();
    connection_->append_data(id_, chunk, end_stream);
    connection_->consume_send_window(chunk.size());
    send_window_ -= static_cast<std::int64_t>(chunk.size());
    send_queue_.consume(chunk.size());

    if (send_queue_.size() <= kSendLowWater && write_waiter_ != nullptr)
        complete_write(write_waiter_);

    if (end_stream) {
        end_pending_ = false;
        end_sent_ = true;
        local_closed_ = true;
        if (end_waiter_ != nullptr)
            complete_end(end_waiter_);
    }
    check_closed();
    return PumpResult::SentFrame;
}

bool H2StreamImpl::pump()
{
    bool queued = false;
    while (pump_one_frame() == PumpResult::SentFrame)
        queued = true;
    return queued;
}

void H2StreamImpl::fail(int error)
{
    if (closed_)
        return;

    closed_ = true;
    error_ = error;
    in_write_queue_ = false;
    blocked_on_window_ = false;

    if (head_waiter_ != nullptr) {
        head_waiter_->error_ = error;
        complete_head(head_waiter_);
    }
    if (read_waiter_ != nullptr) {
        read_waiter_->error_ = error;
        complete_read(read_waiter_);
    }
    if (write_waiter_ != nullptr) {
        write_waiter_->error_ = error;
        complete_write(write_waiter_);
    }
    if (end_waiter_ != nullptr) {
        end_waiter_->error_ = error;
        complete_end(end_waiter_);
    }

    send_queue_.clear();
    recv_queue_.clear();

    if (connection_ != nullptr) {
        connection_->on_stream_closed(id_);
        connection_ = nullptr;
    }
}

void H2StreamImpl::check_closed()
{
    if (closed_ || !(local_closed_ && remote_closed_))
        return;

    closed_ = true;
    if (connection_ != nullptr) {
        connection_->on_stream_closed(id_);
        connection_ = nullptr;
    }
    if (read_waiter_ != nullptr && recv_queue_.empty())
        complete_read(read_waiter_);
}

H2Connection::H2Connection(EventLoop &loop, std::unique_ptr<Transport> transport, std::string authority,
                           std::string scheme, WireMode mode)
    : loop_(loop)
    , transport_(std::move(transport))
    , authority_(std::move(authority))
    , scheme_(std::move(scheme))
    , mode_(mode)
    , flush_signal_(loop)
{
}

Task<std::shared_ptr<H2Connection>> H2Connection::connect(
    EventLoop &loop, std::unique_ptr<Transport> transport, std::string authority, std::string scheme,
    WireMode mode, bool probe_native, std::chrono::milliseconds probe_timeout)
{
    std::shared_ptr<H2Connection> connection(new H2Connection(
        loop, std::move(transport), std::move(authority), std::move(scheme), mode));
    connection->send_preface();

    if (probe_native) {
        try {
            co_await connection->transport_->write_all(connection->write_buffer_, probe_timeout);
            connection->write_buffer_.clear();
            co_await expect_native_hello(*connection->transport_, probe_timeout);
        } catch (...) {
            connection->transport_->close();
            throw;
        }
    }

    loop.spawn(connection->reader_loop());
    loop.spawn(connection->writer_loop());
    co_return connection;
}

void H2Connection::send_preface()
{
    if (mode_ == WireMode::Native) {
        write_buffer_.append(kNativePreface);
        const Setting settings[] = {
            { kSettingInitialWindowSize, kStreamRecvWindow },
            { kSettingMaxFrameSize, kDefaultMaxFrameSize },
        };
        FrameWriter writer;
        writer.settings(settings);
        write_buffer_.append(writer.buffer());
    } else {
        write_buffer_.append(kH2Preface);
        const Setting settings[] = {
            { kSettingEnablePush, 0 },
            { kSettingInitialWindowSize, kStreamRecvWindow },
            { kSettingMaxFrameSize, kDefaultMaxFrameSize },
        };
        FrameWriter writer;
        writer.settings(settings);
        write_buffer_.append(writer.buffer());
    }

    FrameWriter update;
    update.window_update(0, kConnRecvWindow - kDefaultInitialWindowSize);
    write_buffer_.append(update.buffer());
}

std::shared_ptr<H2StreamImpl> H2Connection::open(std::string_view method, std::string_view path,
                                                 HeaderList headers, bool end_stream,
                                                 std::optional<std::uint64_t> content_length)
{
    if (method.empty())
        throw std::invalid_argument("h2 open: empty method");
    if (path.empty() || path[0] != '/')
        throw std::invalid_argument("h2 open: path must begin with /");
    if (!usable_)
        throw std::system_error(ENOTCONN, std::generic_category(), "h2 connection is not usable");
    if (streams_.size() >= max_streams_)
        throw std::system_error(EBUSY, std::generic_category(), "h2 connection is at max concurrent streams");

    const std::uint32_t id = next_stream_id_;
    next_stream_id_ += 2;

    auto stream = std::make_shared<H2StreamImpl>(*this, id, peer_initial_window_);
    if (end_stream) {
        stream->local_closed_ = true;
        stream->end_sent_ = true;
    }

    if (mode_ == WireMode::Native) {
        if (!native::encode_request(method, path, std::span<const HeaderField>(headers), content_length,
                                    native_head_scratch_, true)) {
            throw std::invalid_argument("native open: invalid request head");
        }
        streams_.emplace(id, stream);
        send_headers(id, native_head_scratch_, end_stream);
        return stream;
    }

    HeaderList block;
    block.reserve(4 + headers.size());
    block.push_back({ ":method", std::string(method) });
    block.push_back({ ":scheme", scheme_ });
    block.push_back({ ":authority", authority_ });
    block.push_back({ ":path", std::string(path) });
    for (HeaderField &header : headers) {
        if (header.name.empty() || header.name[0] == ':')
            continue;
        std::string lower_name;
        lower_name.reserve(header.name.size());
        for (char c : header.name) {
            if (c == '\r' || c == '\n' || c == '\0')
                throw std::invalid_argument("h2 open: header name contains control character");
            lower_name.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
        if (is_connection_header(lower_name))
            continue;
        for (char c : header.value) {
            if (c == '\r' || c == '\n' || c == '\0')
                throw std::invalid_argument("h2 open: header value contains control character");
        }
        block.push_back({ std::move(lower_name), std::move(header.value) });
    }

    std::string encoded;
    encoder_.encode(block, encoded);
    streams_.emplace(id, stream);
    send_headers(id, encoded, end_stream);
    return stream;
}

void H2Connection::send_headers(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    append_headers(write_buffer_, stream_id, block, end_stream, max_frame_size_);
    schedule_flush();
}

void H2Connection::append_data(std::uint32_t stream_id, std::string_view data, bool end_stream)
{
    append_frame_header(write_buffer_, FrameType::Data, end_stream ? kFlagEndStream : 0, stream_id, data.size());
    write_buffer_.append(data);
}

void H2Connection::send_rst(std::uint32_t stream_id, ErrorCode code)
{
    char h[13];
    h[0] = 0;
    h[1] = 0;
    h[2] = 4;
    h[3] = static_cast<char>(FrameType::RstStream);
    h[4] = 0;
    h[5] = static_cast<char>((stream_id >> 24) & 0x7f);
    h[6] = static_cast<char>((stream_id >> 16) & 0xff);
    h[7] = static_cast<char>((stream_id >> 8) & 0xff);
    h[8] = static_cast<char>(stream_id & 0xff);
    const auto c = static_cast<std::uint32_t>(code);
    h[9] = static_cast<char>((c >> 24) & 0xff);
    h[10] = static_cast<char>((c >> 16) & 0xff);
    h[11] = static_cast<char>((c >> 8) & 0xff);
    h[12] = static_cast<char>(c & 0xff);
    write_buffer_.append(h, sizeof(h));
}

void H2Connection::send_window_update(std::uint32_t stream_id, std::uint32_t increment)
{
    char h[13];
    h[0] = 0;
    h[1] = 0;
    h[2] = 4;
    h[3] = static_cast<char>(FrameType::WindowUpdate);
    h[4] = 0;
    h[5] = static_cast<char>((stream_id >> 24) & 0x7f);
    h[6] = static_cast<char>((stream_id >> 16) & 0xff);
    h[7] = static_cast<char>((stream_id >> 8) & 0xff);
    h[8] = static_cast<char>(stream_id & 0xff);
    const std::uint32_t inc = increment & 0x7fffffffu;
    h[9] = static_cast<char>((inc >> 24) & 0xff);
    h[10] = static_cast<char>((inc >> 16) & 0xff);
    h[11] = static_cast<char>((inc >> 8) & 0xff);
    h[12] = static_cast<char>(inc & 0xff);
    write_buffer_.append(h, sizeof(h));
    schedule_flush();
}

void H2Connection::schedule_flush()
{
    flush_signal_.notify();
}

void H2Connection::enqueue_write(std::uint32_t stream_id)
{
    auto it = streams_.find(stream_id);
    if (it != streams_.end() && it->second != nullptr) {
        if (it->second->in_write_queue_)
            return;
        it->second->in_write_queue_ = true;
        it->second->blocked_on_window_ = false;
    }
    write_queue_.push_back(stream_id);
    schedule_flush();
}

void H2Connection::on_stream_closed(std::uint32_t stream_id)
{
    pending_removals_.push_back(stream_id);
}

void H2Connection::reap()
{
    if (pending_removals_.empty())
        return;
    for (std::uint32_t id : pending_removals_)
        streams_.erase(id);
    pending_removals_.clear();
}

Task<void> H2Connection::reader_loop()
{
    auto self = shared_from_this();
    std::vector<char> buffer(65536);

    for (;;) {
        std::size_t n = 0;
        try {
            n = co_await transport_->read({ buffer.data(), buffer.size() });
        } catch (...) {
            close_with_error(ECONNRESET);
            co_return;
        }
        if (n == 0) {
            close_with_error(ECONNRESET);
            co_return;
        }

        read_buffer_.append(buffer.data(), n);
        process_frames();
        reap();
        if (closed_)
            co_return;
    }
}

Task<void> H2Connection::writer_loop()
{
    auto self = shared_from_this();

    for (;;) {
        if (closed_)
            co_return;

        reap();

        constexpr std::size_t kMaxBatchWriteSize = 512 * 1024;
        while (!write_queue_.empty() && write_buffer_.size() < kMaxBatchWriteSize) {
            const std::uint32_t id = write_queue_.front();

            auto it = streams_.find(id);
            if (it == streams_.end() || it->second == nullptr) {
                write_queue_.pop_front();
                continue;
            }

            auto &stream = it->second;
            // 0-byte DATA frames (such as empty frames carrying END_STREAM) are not subject
            // to flow control per RFC 7540 §6.1. Only break if this stream has actual data payload
            // to send and the connection send window is depleted.
            if (stream->send_pending() > 0 && send_window_ <= 0)
                break;

            write_queue_.pop_front();
            stream->in_write_queue_ = false;

            if (stream->done())
                continue;

            const PumpResult res = stream->pump_one_frame();
            if (res == PumpResult::SentFrame) {
                if (stream->has_pending_work()) {
                    stream->in_write_queue_ = true;
                    write_queue_.push_back(id);
                }
            } else if (res == PumpResult::WindowFull) {
                stream->blocked_on_window_ = true;
                blocked_streams_.push_back(id);
            }
        }

        if (!write_buffer_.empty()) {
            out_buffer_.clear();
            out_buffer_.swap(write_buffer_);
            try {
                co_await transport_->write_all(out_buffer_);
            } catch (...) {
                close_with_error(ECONNRESET);
                co_return;
            }
            continue;
        }

        if (closed_)
            co_return;
        co_await flush_signal_;
    }
}

void H2Connection::process_frames()
{
    std::size_t offset = 0;

    for (;;) {
        Frame frame;
        const ParseResult result = parser_.next(read_buffer_, offset, frame);
        if (result == ParseResult::NeedMore)
            break;
        if (result == ParseResult::Error) {
            send_goaway(ErrorCode::FrameSize);
            close_with_error(EPROTO);
            return;
        }

        handle_frame(frame);
        if (closed_)
            return;
    }

    if (offset > 0)
        read_buffer_.erase(0, offset);
}

void H2Connection::handle_frame(const Frame &frame)
{
    if (expecting_continuation_ && frame.header.type != FrameType::Continuation) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    switch (frame.header.type) {
    case FrameType::Data:
        handle_data(frame);
        break;

    case FrameType::Headers:
        handle_headers(frame);
        break;

    case FrameType::Priority:
        break;

    case FrameType::RstStream: {
        if (frame.header.stream_id == 0 || frame.payload.size() != 4) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const auto it = streams_.find(frame.header.stream_id);
        if (it != streams_.end())
            it->second->on_rst(static_cast<ErrorCode>(read_be32(frame.payload.data())));
        break;
    }

    case FrameType::Settings:
        handle_settings(frame.payload, (frame.header.flags & kFlagAck) != 0);
        break;

    case FrameType::PushPromise:
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        break;

    case FrameType::Ping: {
        if (frame.header.stream_id != 0 || frame.payload.size() != 8) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        if ((frame.header.flags & kFlagAck) == 0) {
            FrameWriter writer;
            writer.ping(true, read_be32(frame.payload.data()) * 0x100000000ull +
                                  read_be32(frame.payload.data() + 4));
            write_buffer_.append(writer.buffer());
            schedule_flush();
        }
        break;
    }

    case FrameType::GoAway:
        handle_goaway(frame.payload);
        break;

    case FrameType::WindowUpdate:
        handle_window_update(frame);
        break;

    case FrameType::Continuation: {
        if (!expecting_continuation_ || frame.header.stream_id != continuation_stream_) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        continuation_block_.append(frame.payload);
        if ((frame.header.flags & kFlagEndHeaders) != 0) {
            expecting_continuation_ = false;
            const std::uint32_t stream_id = continuation_stream_;
            const bool end_stream = continuation_end_stream_;
            std::string block = std::move(continuation_block_);
            continuation_block_.clear();
            finish_headers(stream_id, block, end_stream);
        }
        break;
    }

    default:
        break;
    }
}

void H2Connection::handle_settings(std::string_view payload, bool ack)
{
    if (ack) {
        if (!payload.empty()) {
            send_goaway(ErrorCode::FrameSize);
            close_with_error(EPROTO);
        }
        return;
    }

    if (payload.size() % 6 != 0) {
        send_goaway(ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    for (std::size_t offset = 0; offset < payload.size(); offset += 6) {
        const std::uint16_t id = read_be16(payload.data() + offset);
        const std::uint32_t value = read_be32(payload.data() + offset + 2);

        switch (id) {
        case kSettingInitialWindowSize: {
            if (value > kMaxWindow) {
                send_goaway(ErrorCode::FlowControl);
                close_with_error(EPROTO);
                return;
            }
            const auto delta = static_cast<std::int32_t>(value) - static_cast<std::int32_t>(peer_initial_window_);
            peer_initial_window_ = value;
            for (auto &entry : streams_)
                entry.second->on_settings_initial_window(delta);
            break;
        }
        case kSettingMaxFrameSize:
            if (value < kDefaultMaxFrameSize || value > 16777215) {
                send_goaway(ErrorCode::Protocol);
                close_with_error(EPROTO);
                return;
            }
            max_frame_size_ = value;
            break;
        case kSettingMaxConcurrentStreams:
            max_streams_ = value == 0 ? 1 : value;
            break;
        case kSettingHeaderTableSize:
            decoder_.set_max_allowed_table_size(value);
            break;
        default:
            break;
        }
    }

    FrameWriter writer;
    writer.settings_ack();
    write_buffer_.append(writer.buffer());
    schedule_flush();
}

void H2Connection::handle_headers(const Frame &frame)
{
    if (frame.header.stream_id == 0) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    std::string_view payload = frame.payload;
    if ((frame.header.flags & kFlagPadded) != 0) {
        if (payload.empty()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const std::size_t pad = static_cast<unsigned char>(payload.front());
        payload.remove_prefix(1);
        if (pad > payload.size()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        payload.remove_suffix(pad);
    }
    if ((frame.header.flags & kFlagPriority) != 0) {
        if (payload.size() < 5) {
            send_goaway(ErrorCode::FrameSize);
            close_with_error(EPROTO);
            return;
        }
        payload.remove_prefix(5);
    }

    const bool end_stream = (frame.header.flags & kFlagEndStream) != 0;
    if ((frame.header.flags & kFlagEndHeaders) != 0) {
        finish_headers(frame.header.stream_id, payload, end_stream);
        return;
    }

    expecting_continuation_ = true;
    continuation_stream_ = frame.header.stream_id;
    continuation_end_stream_ = end_stream;
    continuation_block_.assign(payload);
}

void H2Connection::finish_headers(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    ResponseHead head;

    if (mode_ == WireMode::Native) {
        std::vector<native::HeaderView> &views = native_views_;
        native::ResponseHead decoded;
        if (!native::decode_response(block, views, decoded, native::kMaxHeaders, native::kMaxBlock)) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }

        head.status = decoded.status;
        head.content_length = decoded.content_length;
        head.protocol = Protocol::Native;
        head.headers.reserve(views.size() + (decoded.content_length.has_value() ? 1 : 0));

        bool has_content_length = false;
        for (const native::HeaderView &view : views) {
            if (iequals(view.first, "content-length"))
                has_content_length = true;
            head.headers.push_back(HeaderField{ std::string(view.first), std::string(view.second) });
        }
        if (decoded.content_length.has_value() && !has_content_length)
            head.headers.push_back(HeaderField{ "content-length", std::to_string(*decoded.content_length) });
    } else {
        HeaderList headers;
        if (!decoder_.decode(block, headers)) {
            send_goaway(ErrorCode::Compression);
            close_with_error(EPROTO);
            return;
        }

        head.protocol = Protocol::Http2;
        for (HeaderField &header : headers) {
            if (header.name == ":status") {
                try {
                    head.status = std::stoi(header.value);
                } catch (...) {
                    head.status = 0;
                }
            } else if (!header.name.empty() && header.name[0] != ':') {
                if (iequals(header.name, "content-length")) {
                    std::uint64_t length = 0;
                    if (parse_uint(header.value, length))
                        head.content_length = length;
                }
                head.headers.push_back(std::move(header));
            }
        }
    }

    const auto it = streams_.find(stream_id);
    if (it == streams_.end())
        return;
    it->second->on_head(std::move(head), end_stream);
}

void H2Connection::handle_data(const Frame &frame)
{
    if (frame.header.stream_id == 0) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    std::string_view data = frame.payload;
    if ((frame.header.flags & kFlagPadded) != 0) {
        if (data.empty()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const std::size_t pad = static_cast<unsigned char>(data.front());
        data.remove_prefix(1);
        if (pad > data.size()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        data.remove_suffix(pad);
    }

    const auto it = streams_.find(frame.header.stream_id);
    if (it != streams_.end())
        it->second->on_data(data, (frame.header.flags & kFlagEndStream) != 0, frame.header.length);

    recv_unacked_ += frame.header.length;
    if (recv_unacked_ >= 32768) {
        send_window_update(0, recv_unacked_);
        recv_unacked_ = 0;
    }
}

void H2Connection::handle_goaway(std::string_view payload)
{
    if (payload.size() < 8) {
        send_goaway(ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    const std::uint32_t last_stream_id = read_be32(payload.data()) & 0x7fffffffu;
    usable_ = false;

    std::vector<std::shared_ptr<H2StreamImpl>> failed;
    for (const auto &entry : streams_) {
        if (entry.first > last_stream_id)
            failed.push_back(entry.second);
    }
    for (const auto &stream : failed)
        stream->on_connection_closed(ECONNRESET);
}

void H2Connection::handle_window_update(const Frame &frame)
{
    if (frame.payload.size() != 4) {
        send_goaway(ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    const std::uint32_t increment = read_be32(frame.payload.data()) & 0x7fffffffu;
    if (increment == 0) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    if (frame.header.stream_id == 0) {
        if (send_window_ + static_cast<std::int64_t>(increment) > static_cast<std::int64_t>(kMaxWindow)) {
            send_goaway(ErrorCode::FlowControl);
            close_with_error(EPROTO);
            return;
        }
        send_window_ += increment;
        if (send_window_ > 0 && !blocked_streams_.empty()) {
            std::vector<std::uint32_t> unblocked;
            unblocked.swap(blocked_streams_);
            for (std::uint32_t sid : unblocked) {
                auto it = streams_.find(sid);
                if (it != streams_.end() && it->second != nullptr) {
                    if (it->second->send_window_ > 0) {
                        it->second->blocked_on_window_ = false;
                        if (!it->second->in_write_queue_ && it->second->has_pending_work()) {
                            it->second->in_write_queue_ = true;
                            write_queue_.push_back(sid);
                        }
                    } else {
                        blocked_streams_.push_back(sid);
                    }
                }
            }
        }
    } else {
        const auto it = streams_.find(frame.header.stream_id);
        if (it != streams_.end())
            it->second->on_window_update(increment);
    }

    schedule_flush();
}

void H2Connection::send_goaway(ErrorCode code)
{
    FrameWriter writer;
    writer.goaway(next_stream_id_ - 1, code);
    write_buffer_.append(writer.buffer());
}

void H2Connection::close_with_error(int error)
{
    if (closed_)
        return;

    closed_ = true;
    usable_ = false;

    std::vector<std::shared_ptr<H2StreamImpl>> streams;
    streams.reserve(streams_.size());
    for (auto &entry : streams_)
        streams.push_back(entry.second);
    streams_.clear();
    write_queue_.clear();
    blocked_streams_.clear();
    pending_removals_.clear();

    for (const auto &stream : streams)
        stream->on_connection_closed(error);

    if (transport_->fd() >= 0)
        (void)::shutdown(transport_->fd(), SHUT_RDWR);

    flush_signal_.notify();
}

void H2Connection::close()
{
    if (closed_)
        return;
    send_goaway(ErrorCode::NoError);
    close_with_error(0);
}

} // namespace realurl::detail
