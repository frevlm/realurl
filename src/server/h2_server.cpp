#include "h2_server.hpp"

#include "realurl/detail/native_codec.hpp"
#include "realurl/detail/protocol.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <netdb.h>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace realurl::detail
{
namespace
{

constexpr std::uint32_t kStreamRecvWindow = 1024 * 1024;
constexpr std::uint32_t kConnRecvWindow = 16 * 1024 * 1024;
constexpr std::size_t kSendHighWater = 256 * 1024;
constexpr std::size_t kSendLowWater = 128 * 1024;
constexpr std::size_t kRecvHighWater = 1024 * 1024;
constexpr std::uint32_t kWindowUpdateThreshold = 32 * 1024;
constexpr std::size_t kMaxHeaderBlock = 1024 * 1024;
constexpr std::uint32_t kMaxStreams = 1000;
constexpr std::uint32_t kMaxWindow = 2147483647u;

enum class H2PumpResult { NoWork, WindowFull, SentFrame };

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

bool forbidden_header(std::string_view name) noexcept
{
    return iequals(name, "connection") || iequals(name, "keep-alive") ||
           iequals(name, "proxy-connection") || iequals(name, "transfer-encoding") ||
           iequals(name, "upgrade");
}

long long parse_content_length(std::string_view value) noexcept
{
    if (value.empty())
        return -1;
    long long result = 0;
    for (char c : value) {
        if (c < '0' || c > '9')
            return -1;
        if (result > (LLONG_MAX - (c - '0')) / 10)
            return -1;
        result = result * 10 + (c - '0');
    }
    return result;
}

struct RequestMetadata
{
    std::string method;
    std::string path;
    std::string authority;
    long long content_length = -1;
};

bool validate_headers(const h2::HeaderList &headers, RequestMetadata &out)
{
    bool regular_seen = false;
    bool method_seen = false;
    bool path_seen = false;
    bool scheme_seen = false;
    bool authority_seen = false;
    bool content_length_seen = false;

    for (const h2::HeaderField &field : headers) {
        if (field.name.empty())
            return false;

        if (field.name[0] == ':') {
            if (regular_seen)
                return false;
            if (field.name == ":method") {
                if (method_seen)
                    return false;
                method_seen = true;
                out.method = field.value;
            } else if (field.name == ":path") {
                if (path_seen)
                    return false;
                path_seen = true;
                out.path = field.value;
            } else if (field.name == ":scheme") {
                if (scheme_seen)
                    return false;
                scheme_seen = true;
            } else if (field.name == ":authority") {
                if (authority_seen)
                    return false;
                authority_seen = true;
                out.authority = field.value;
            } else {
                return false;
            }
            continue;
        }

        regular_seen = true;
        for (char c : field.name) {
            if (c >= 'A' && c <= 'Z')
                return false;
        }
        if (forbidden_header(field.name))
            return false;
        if (field.name == "content-length") {
            const long long value = parse_content_length(field.value);
            if (value < 0 || (content_length_seen && value != out.content_length))
                return false;
            out.content_length = value;
            content_length_seen = true;
        }
    }

    if (!method_seen)
        return false;
    if (out.method == "CONNECT")
        return authority_seen;
    if (!path_seen || out.path.empty() || (out.path[0] != '/' && out.path != "*"))
        return false;
    (void)scheme_seen;
    return true;
}

} // namespace

class H2ServerConnection::FlushSignal
{
public:
    explicit FlushSignal(EventLoop &loop)
        : loop_(&loop)
    {
    }

    bool await_ready() const noexcept
    {
        return notified_;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept
    {
        waiter_ = handle;
    }

    void await_resume() noexcept
    {
        notified_ = false;
    }

    void notify()
    {
        notified_ = true;
        if (waiter_) {
            const std::coroutine_handle<> handle = waiter_;
            waiter_ = {};
            loop_->resume_later(handle);
        }
    }

private:
    EventLoop *loop_;
    bool notified_ = false;
    std::coroutine_handle<> waiter_{};
};

class H2ServerStream final : public StreamWriter,
                             public BodyReader,
                             public std::enable_shared_from_this<H2ServerStream>
{
public:
    H2ServerStream(H2ServerConnection &connection, std::uint32_t id)
        : connection_(&connection)
        , loop_(&connection.loop())
        , id_(id)
        , send_window_(h2::kDefaultInitialWindowSize)
    {
    }

    std::uint32_t id() const noexcept
    {
        return id_;
    }

    bool done() const noexcept
    {
        return closed_;
    }

    bool has_pending_work() const noexcept
    {
        return send_pending_ > 0 || (end_pending_ && !end_sent_);
    }

    void on_headers(h2::HeaderList &&headers, bool end_stream);
    void on_native_headers(std::string block, bool end_stream);
    void on_data(std::string_view data, bool end_stream, std::uint32_t flow_bytes);
    void on_rst();
    void on_window_update(std::uint32_t increment);
    void on_settings_initial_window(std::int32_t delta);
    H2PumpResult pump_one_frame();
    bool pump();
    Task<void> run_handler();

    // StreamWriter
    Task<void> write_head(int status, std::span<const Header> headers,
                          std::optional<std::uint64_t> content_length) override;
    Task<bool> write(std::string_view data) override;
    Task<void> end() override;
    void cancel() override;
    bool closed() const noexcept override
    {
        return closed_;
    }

    // BodyReader
    Task<std::size_t> read(std::span<char> buffer) override;
    std::uint64_t remaining() const noexcept override
    {
        return remote_closed_ ? 0 : UINT64_MAX;
    }

    bool in_write_queue_ = false;
    bool blocked_on_window_ = false;

private:
    friend class H2ServerConnection;

    class ReadAwaiter;
    class WriteAwaiter;
    class EndAwaiter;

    friend class ReadAwaiter;
    friend class WriteAwaiter;
    friend class EndAwaiter;

    void start_read(ReadAwaiter *waiter, std::coroutine_handle<> handle);
    void complete_read(ReadAwaiter *waiter);
    void start_write(WriteAwaiter *waiter, std::coroutine_handle<> handle);
    void complete_write(WriteAwaiter *waiter);
    void start_end(EndAwaiter *waiter, std::coroutine_handle<> handle);
    void complete_end(EndAwaiter *waiter);

    Task<void> send_fixed_response(const Response &response);
    Task<void> send_error(int status);
    void finish_request_head(std::string_view method, std::string_view target,
                             long long content_length, bool end_stream,
                             std::string_view version);
    void flush_window();
    void fail(int error);
    void check_closed();
    std::size_t send_pending() const noexcept
    {
        return send_pending_;
    }

    H2ServerConnection *connection_;
    EventLoop *loop_;
    std::uint32_t id_ = 0;

    std::string method_storage_;
    std::string path_storage_;
    std::string decoded_path_storage_;
    std::string native_head_storage_;
    std::string content_length_storage_;
    std::string head_block_;
    h2::HeaderList headers_;
    std::vector<Header> request_header_views_;
    Request request_;
    bool head_ready_ = false;

    bool head_sent_ = false;
    bool end_pending_ = false;
    bool end_sent_ = false;
    bool local_closed_ = false;
    bool remote_closed_ = false;
    bool closed_ = false;
    bool head_only_ = false;
    int error_ = 0;

    std::deque<std::string> recv_queue_;
    std::size_t recv_chunk_offset_ = 0;
    std::size_t recv_pending_ = 0;
    std::uint32_t recv_unacked_ = 0;
    std::int64_t recv_window_ = kStreamRecvWindow;

    std::deque<std::string> send_queue_;
    std::size_t send_chunk_offset_ = 0;
    std::size_t send_pending_ = 0;
    std::int64_t send_window_ = h2::kDefaultInitialWindowSize;

    ReadAwaiter *read_waiter_ = nullptr;
    WriteAwaiter *write_waiter_ = nullptr;
    EndAwaiter *end_waiter_ = nullptr;
};

class H2ServerStream::ReadAwaiter
{
public:
    ReadAwaiter(H2ServerStream &stream, std::span<char> buffer)
        : stream_(stream)
        , buffer_(buffer)
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
            throw std::system_error(error_, std::generic_category(), "h2 server read");
        return n_;
    }

private:
    friend class H2ServerStream;

    H2ServerStream &stream_;
    std::span<char> buffer_;
    std::coroutine_handle<> handle_{};
    std::size_t n_ = 0;
    int error_ = 0;
    bool done_ = false;
};

class H2ServerStream::WriteAwaiter
{
public:
    explicit WriteAwaiter(H2ServerStream &stream)
        : stream_(stream)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_write(this, handle);
    }

    void await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 server write");
    }

private:
    friend class H2ServerStream;

    H2ServerStream &stream_;
    std::coroutine_handle<> handle_{};
    int error_ = 0;
    bool done_ = false;
};

class H2ServerStream::EndAwaiter
{
public:
    explicit EndAwaiter(H2ServerStream &stream)
        : stream_(stream)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_end(this, handle);
    }

    void await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 server end");
    }

private:
    friend class H2ServerStream;

    H2ServerStream &stream_;
    std::coroutine_handle<> handle_{};
    int error_ = 0;
    bool done_ = false;
};

H2ServerConnection::H2ServerConnection(ServerContext &context, EventLoop &loop,
                                       std::unique_ptr<Transport> transport, sockaddr_storage peer,
                                       Client *upstream, WireMode mode)
    : context_(context)
    , loop_(loop)
    , transport_(std::move(transport))
    , upstream_(upstream)
    , mode_(mode)
    , peer_(peer)
    , flush_signal_(std::make_unique<FlushSignal>(loop))
    , close_signal_(std::make_unique<FlushSignal>(loop))
{
    char host[NI_MAXHOST] = {};
    char service[NI_MAXSERV] = {};
    if (::getnameinfo(reinterpret_cast<sockaddr *>(&peer_), sizeof(peer_), host, sizeof(host), service,
                      sizeof(service), NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
        remote_addr_ = host;
        remote_port_ = std::atoi(service);
    }
}

H2ServerConnection::~H2ServerConnection() = default;

Task<void> H2ServerConnection::serve()
{
    auto self = shared_from_this();
    (void)self;
    send_preface();
    loop_.spawn(reader_loop());
    loop_.spawn(writer_loop());
    co_await *close_signal_;
    transport_->close();
}

void H2ServerConnection::send_preface()
{
    if (mode_ == WireMode::Native) {
        char hello[kNativeHelloPayloadSize] = {};
        std::memcpy(hello, kNativeHelloMagic.data(), kNativeHelloMagic.size());
        hello[8] = static_cast<char>(kNativeVersionMajor);
        hello[9] = static_cast<char>(kNativeVersionMinor);

        h2::FrameWriter hello_writer;
        hello_writer.extension(kNativeHelloType, 0, 0, { hello, sizeof(hello) });
        write_buffer_.append(hello_writer.buffer());

        const h2::Setting settings[] = {
            { h2::kSettingInitialWindowSize, kStreamRecvWindow },
            { h2::kSettingMaxFrameSize, h2::kDefaultMaxFrameSize },
            { h2::kSettingMaxConcurrentStreams, kMaxStreams },
        };
        h2::FrameWriter writer;
        writer.settings(settings);
        write_buffer_.append(writer.buffer());
    } else {
        const h2::Setting settings[] = {
            { h2::kSettingEnablePush, 0 },
            { h2::kSettingInitialWindowSize, kStreamRecvWindow },
            { h2::kSettingMaxFrameSize, h2::kDefaultMaxFrameSize },
            { h2::kSettingMaxConcurrentStreams, kMaxStreams },
        };
        h2::FrameWriter writer;
        writer.settings(settings);
        write_buffer_.append(writer.buffer());
    }

    h2::FrameWriter update;
    update.window_update(0, kConnRecvWindow - h2::kDefaultInitialWindowSize);
    write_buffer_.append(update.buffer());
    schedule_flush();
}

Task<void> H2ServerConnection::reader_loop()
{
    auto self = shared_from_this();
    (void)self;
    char buffer[65536];

    for (;;) {
        if (closed_)
            co_return;
        std::size_t n = 0;
        try {
            n = co_await transport_->read({ buffer, sizeof(buffer) });
        } catch (...) {
            close_with_error(ECONNRESET);
            co_return;
        }
        if (n == 0) {
            close_with_error(ECONNRESET);
            co_return;
        }
        read_buffer_.append(buffer, n);
        process_frames();
        if (closed_)
            co_return;
    }
}

void H2ServerConnection::process_frames()
{
    std::size_t offset = 0;
    for (;;) {
        h2::Frame frame;
        const h2::ParseResult result = parser_.next(read_buffer_, offset, frame);
        if (result == h2::ParseResult::NeedMore)
            break;
        if (result == h2::ParseResult::Error) {
            send_goaway(h2::ErrorCode::FrameSize);
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

void H2ServerConnection::handle_frame(const h2::Frame &frame)
{
    if (expecting_continuation_ && frame.header.type != h2::FrameType::Continuation) {
        send_goaway(h2::ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    switch (frame.header.type) {
    case h2::FrameType::Data:
        handle_data(frame);
        break;
    case h2::FrameType::Headers:
        handle_headers(frame);
        break;
    case h2::FrameType::Priority:
        break;
    case h2::FrameType::RstStream: {
        if (frame.header.stream_id == 0 || frame.payload.size() != 4) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const auto it = streams_.find(frame.header.stream_id);
        if (it != streams_.end())
            it->second->on_rst();
        break;
    }
    case h2::FrameType::Settings:
        handle_settings(frame.payload, (frame.header.flags & h2::kFlagAck) != 0);
        break;
    case h2::FrameType::PushPromise:
        send_goaway(h2::ErrorCode::Protocol);
        close_with_error(EPROTO);
        break;
    case h2::FrameType::Ping: {
        if (frame.header.stream_id != 0 || frame.payload.size() != 8) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        if ((frame.header.flags & h2::kFlagAck) == 0) {
            h2::FrameWriter writer;
            writer.ping(true, read_be32(frame.payload.data()) * 0x100000000ull +
                                  read_be32(frame.payload.data() + 4));
            write_buffer_.append(writer.buffer());
            schedule_flush();
        }
        break;
    }
    case h2::FrameType::GoAway:
        handle_goaway(frame.payload);
        break;
    case h2::FrameType::WindowUpdate:
        handle_window_update(frame);
        break;
    case h2::FrameType::Continuation: {
        if (!expecting_continuation_ || frame.header.stream_id != continuation_stream_) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        continuation_block_.append(frame.payload);
        if (continuation_block_.size() > kMaxHeaderBlock) {
            send_goaway(h2::ErrorCode::Compression);
            close_with_error(EPROTO);
            return;
        }
        if ((frame.header.flags & h2::kFlagEndHeaders) != 0) {
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

void H2ServerConnection::handle_settings(std::string_view payload, bool ack)
{
    if (ack) {
        if (!payload.empty()) {
            send_goaway(h2::ErrorCode::FrameSize);
            close_with_error(EPROTO);
        }
        return;
    }
    if (payload.size() % 6 != 0) {
        send_goaway(h2::ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    for (std::size_t offset = 0; offset < payload.size(); offset += 6) {
        const std::uint16_t id = read_be16(payload.data() + offset);
        const std::uint32_t value = read_be32(payload.data() + offset + 2);
        switch (id) {
        case h2::kSettingInitialWindowSize: {
            if (value > kMaxWindow) {
                send_goaway(h2::ErrorCode::FlowControl);
                close_with_error(EPROTO);
                return;
            }
            const auto delta = static_cast<std::int32_t>(value) -
                               static_cast<std::int32_t>(peer_initial_window_);
            peer_initial_window_ = value;
            for (auto &entry : streams_)
                entry.second->on_settings_initial_window(delta);
            break;
        }
        case h2::kSettingMaxFrameSize:
            if (value < h2::kDefaultMaxFrameSize || value > 16777215) {
                send_goaway(h2::ErrorCode::Protocol);
                close_with_error(EPROTO);
                return;
            }
            max_frame_size_ = value;
            break;
        default:
            break;
        }
    }

    h2::FrameWriter writer;
    writer.settings_ack();
    write_buffer_.append(writer.buffer());
    schedule_flush();
}

void H2ServerConnection::handle_headers(const h2::Frame &frame)
{
    if (frame.header.stream_id == 0 || (frame.header.stream_id & 1u) == 0) {
        send_goaway(h2::ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    std::string_view payload = frame.payload;
    if ((frame.header.flags & h2::kFlagPadded) != 0) {
        if (payload.empty()) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const std::size_t pad = static_cast<unsigned char>(payload.front());
        payload.remove_prefix(1);
        if (pad > payload.size()) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        payload.remove_suffix(pad);
    }
    if ((frame.header.flags & h2::kFlagPriority) != 0) {
        if (payload.size() < 5) {
            send_goaway(h2::ErrorCode::FrameSize);
            close_with_error(EPROTO);
            return;
        }
        payload.remove_prefix(5);
    }

    if (streams_.find(frame.header.stream_id) == streams_.end() &&
        frame.header.stream_id <= max_stream_id_) {
        send_goaway(h2::ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }
    max_stream_id_ = std::max(max_stream_id_, frame.header.stream_id);
    const bool end_stream = (frame.header.flags & h2::kFlagEndStream) != 0;

    if ((frame.header.flags & h2::kFlagEndHeaders) != 0) {
        finish_headers(frame.header.stream_id, payload, end_stream);
        return;
    }

    expecting_continuation_ = true;
    continuation_stream_ = frame.header.stream_id;
    continuation_end_stream_ = end_stream;
    continuation_block_.assign(payload);
}

void H2ServerConnection::finish_headers(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    if (block.size() > kMaxHeaderBlock) {
        send_goaway(h2::ErrorCode::Compression);
        close_with_error(EPROTO);
        return;
    }

    auto it = streams_.find(stream_id);

    if (mode_ == WireMode::Native) {
        if (it == streams_.end()) {
            if (active_streams_ >= kMaxStreams) {
                send_rst(stream_id, h2::ErrorCode::RefusedStream);
                return;
            }

            auto stream = std::make_shared<H2ServerStream>(*this, stream_id);
            streams_.emplace(stream_id, stream);
            ++active_streams_;
            stream->on_native_headers(std::string(block), end_stream);
            if (stream->done())
                return;
            loop_.spawn(stream->run_handler());
            return;
        }

        it->second->on_native_headers(std::string(block), end_stream);
        return;
    }

    h2::HeaderList headers;
    if (!decoder_.decode(block, headers, 65536)) {
        send_goaway(h2::ErrorCode::Compression);
        close_with_error(EPROTO);
        return;
    }

    if (it == streams_.end()) {
        if (active_streams_ >= kMaxStreams) {
            send_rst(stream_id, h2::ErrorCode::RefusedStream);
            return;
        }

        auto stream = std::make_shared<H2ServerStream>(*this, stream_id);
        streams_.emplace(stream_id, stream);
        ++active_streams_;
        stream->on_headers(std::move(headers), end_stream);
        if (stream->done())
            return;
        loop_.spawn(stream->run_handler());
        return;
    }

    it->second->on_headers(std::move(headers), end_stream);
}

void H2ServerConnection::handle_data(const h2::Frame &frame)
{
    if (frame.header.stream_id == 0) {
        send_goaway(h2::ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    std::string_view data = frame.payload;
    if ((frame.header.flags & h2::kFlagPadded) != 0) {
        if (data.empty()) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const std::size_t pad = static_cast<unsigned char>(data.front());
        data.remove_prefix(1);
        if (pad > data.size()) {
            send_goaway(h2::ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        data.remove_suffix(pad);
    }

    const auto it = streams_.find(frame.header.stream_id);
    if (it == streams_.end()) {
        send_rst(frame.header.stream_id, h2::ErrorCode::StreamClosed);
        return;
    }
    it->second->on_data(data, (frame.header.flags & h2::kFlagEndStream) != 0, frame.header.length);

    recv_unacked_ += frame.header.length;
    if (recv_unacked_ >= kWindowUpdateThreshold) {
        send_window_update(0, recv_unacked_);
        recv_unacked_ = 0;
    }
}

void H2ServerConnection::handle_window_update(const h2::Frame &frame)
{
    if (frame.payload.size() != 4) {
        send_goaway(h2::ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }
    const std::uint32_t increment = read_be32(frame.payload.data()) & 0x7fffffffu;
    if (increment == 0) {
        send_goaway(h2::ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    if (frame.header.stream_id == 0) {
        if (send_window_ + static_cast<std::int64_t>(increment) > static_cast<std::int64_t>(kMaxWindow)) {
            send_goaway(h2::ErrorCode::FlowControl);
            close_with_error(EPROTO);
            return;
        }
        send_window_ += increment;
        std::vector<std::uint32_t> unblocked;
        unblocked.swap(blocked_streams_);
        for (std::uint32_t id : unblocked) {
            const auto it = streams_.find(id);
            if (it == streams_.end() || it->second == nullptr)
                continue;
            if (it->second->send_window_ > 0) {
                it->second->blocked_on_window_ = false;
                if (!it->second->in_write_queue_ && it->second->has_pending_work()) {
                    it->second->in_write_queue_ = true;
                    write_queue_.push_back(id);
                }
            } else {
                blocked_streams_.push_back(id);
            }
        }
        schedule_flush();
        return;
    }

    const auto it = streams_.find(frame.header.stream_id);
    if (it != streams_.end())
        it->second->on_window_update(increment);
}

void H2ServerConnection::handle_goaway(std::string_view payload)
{
    if (payload.size() < 8) {
        send_goaway(h2::ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }
    close_with_error(0);
}

void H2ServerConnection::send_goaway(h2::ErrorCode code)
{
    h2::FrameWriter writer;
    writer.goaway(max_stream_id_, code);
    write_buffer_.append(writer.buffer());
    schedule_flush();
}

void H2ServerConnection::queue_headers(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    h2::FrameWriter writer;
    if (block.size() <= max_frame_size_) {
        writer.headers(stream_id, block, true, end_stream);
        write_buffer_.append(writer.buffer());
    } else {
        writer.headers(stream_id, block.substr(0, max_frame_size_), false, end_stream);
        write_buffer_.append(writer.buffer());
        std::size_t offset = max_frame_size_;
        while (offset < block.size()) {
            const std::size_t n = std::min<std::size_t>(max_frame_size_, block.size() - offset);
            const bool last = offset + n == block.size();
            h2::FrameWriter continuation;
            continuation.continuation(stream_id, block.substr(offset, n), last);
            write_buffer_.append(continuation.buffer());
            offset += n;
        }
    }
    schedule_flush();
}

bool H2ServerConnection::encode_response_head(int status, std::span<const Header> headers,
                                              std::optional<std::uint64_t> content_length,
                                              std::string &out)
{
    if (mode_ == WireMode::Native)
        return native::encode_response(status, headers, content_length, out, true, true);

    out.clear();
    h2::HeaderList fields;
    fields.push_back({ ":status", std::to_string(status) });
    for (const Header &header : headers) {
        if (forbidden_header(header.first) || iequals(header.first, "content-length"))
            continue;
        fields.push_back({ std::string(header.first), std::string(header.second) });
    }
    if (content_length.has_value())
        fields.push_back({ "content-length", std::to_string(*content_length) });
    encoder_.encode(fields, out);
    return true;
}

void H2ServerConnection::append_data(std::uint32_t stream_id, std::string_view data, bool end_stream)
{
    char header[9];
    const std::size_t length = data.size();
    header[0] = static_cast<char>((length >> 16) & 0xff);
    header[1] = static_cast<char>((length >> 8) & 0xff);
    header[2] = static_cast<char>(length & 0xff);
    header[3] = static_cast<char>(h2::FrameType::Data);
    header[4] = static_cast<char>(end_stream ? h2::kFlagEndStream : 0);
    header[5] = static_cast<char>((stream_id >> 24) & 0x7f);
    header[6] = static_cast<char>((stream_id >> 16) & 0xff);
    header[7] = static_cast<char>((stream_id >> 8) & 0xff);
    header[8] = static_cast<char>(stream_id & 0xff);
    write_buffer_.append(header, sizeof(header));
    if (!data.empty())
        write_buffer_.append(data);
}

void H2ServerConnection::send_rst(std::uint32_t stream_id, h2::ErrorCode code)
{
    h2::FrameWriter writer;
    writer.rst_stream(stream_id, code);
    write_buffer_.append(writer.buffer());
    schedule_flush();
}

void H2ServerConnection::send_window_update(std::uint32_t stream_id, std::uint32_t increment)
{
    h2::FrameWriter writer;
    writer.window_update(stream_id, increment);
    write_buffer_.append(writer.buffer());
    schedule_flush();
}

void H2ServerConnection::schedule_flush()
{
    if (flush_signal_)
        flush_signal_->notify();
}

void H2ServerConnection::enqueue_write(std::uint32_t stream_id)
{
    const auto it = streams_.find(stream_id);
    if (it != streams_.end() && it->second != nullptr) {
        if (it->second->in_write_queue_)
            return;
        it->second->in_write_queue_ = true;
        it->second->blocked_on_window_ = false;
    }
    write_queue_.push_back(stream_id);
    schedule_flush();
}

void H2ServerConnection::on_stream_closed(std::uint32_t stream_id)
{
    pending_removals_.push_back(stream_id);
    schedule_flush();
}

Task<void> H2ServerConnection::writer_loop()
{
    auto self = shared_from_this();
    (void)self;

    for (;;) {
        if (closed_)
            co_return;

        if (!pending_removals_.empty()) {
            for (std::uint32_t id : pending_removals_) {
                const auto it = streams_.find(id);
                if (it != streams_.end()) {
                    streams_.erase(it);
                    if (active_streams_ > 0)
                        --active_streams_;
                }
            }
            pending_removals_.clear();
        }

        constexpr std::size_t kMaxBatch = 512 * 1024;
        while (!write_queue_.empty() && write_buffer_.size() < kMaxBatch) {
            const std::uint32_t id = write_queue_.front();
            const auto it = streams_.find(id);
            if (it == streams_.end() || it->second == nullptr) {
                write_queue_.pop_front();
                continue;
            }

            auto &stream = it->second;
            if (stream->send_pending() > 0 && send_window_ <= 0)
                break;

            write_queue_.pop_front();
            stream->in_write_queue_ = false;

            if (stream->done())
                continue;

            const H2PumpResult result = stream->pump_one_frame();
            if (result == H2PumpResult::SentFrame) {
                if (stream->has_pending_work()) {
                    stream->in_write_queue_ = true;
                    write_queue_.push_back(id);
                }
            } else if (result == H2PumpResult::WindowFull) {
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
        co_await *flush_signal_;
    }
}

void H2ServerConnection::close_with_error(int error)
{
    if (closed_)
        return;
    closed_ = true;

    std::vector<std::shared_ptr<H2ServerStream>> streams;
    streams.reserve(streams_.size());
    for (auto &entry : streams_)
        streams.push_back(entry.second);
    streams_.clear();
    write_queue_.clear();
    blocked_streams_.clear();
    pending_removals_.clear();

    for (const auto &stream : streams)
        stream->on_rst();

    if (transport_->fd() >= 0)
        (void)::shutdown(transport_->fd(), SHUT_RDWR);

    if (flush_signal_)
        flush_signal_->notify();
    if (close_signal_)
        close_signal_->notify();
    (void)error;
}

void H2ServerStream::finish_request_head(std::string_view method, std::string_view target,
                                         long long content_length, bool end_stream,
                                         std::string_view version)
{
    request_ = Request{};
    request_.method = method;
    request_.target = target;
    request_.path = target;
    const auto query = target.find_first_of("?#");
    if (query != std::string_view::npos)
        request_.path = target.substr(0, query);
    if (connection_->context().path_decoding && request_.path.find('%') != std::string_view::npos) {
        decoded_path_storage_ = percent_decode(request_.path);
        request_.path = decoded_path_storage_;
    }
    request_.version = version;
    request_.content_length = content_length;
    request_.remote_addr = connection_->remote_addr_;
    request_.remote_port = connection_->remote_port_;
    request_.loop = loop_;
    request_.upstream = connection_->upstream_;
    request_.headers = { request_header_views_.data(), request_header_views_.size() };

    head_only_ = method == "HEAD";
    head_ready_ = true;
    if (end_stream)
        remote_closed_ = true;
}

void H2ServerStream::on_headers(h2::HeaderList &&headers, bool end_stream)
{
    if (head_ready_) {
        remote_closed_ = remote_closed_ || end_stream;
        if (end_stream) {
            if (read_waiter_ != nullptr && recv_pending_ == 0)
                complete_read(read_waiter_);
        }
        check_closed();
        return;
    }

    RequestMetadata metadata;
    if (!validate_headers(headers, metadata)) {
        connection_->send_rst(id_, h2::ErrorCode::Protocol);
        fail(EPROTO);
        return;
    }

    method_storage_ = std::move(metadata.method);
    path_storage_ = std::move(metadata.path);
    headers_ = std::move(headers);

    request_header_views_.clear();
    request_header_views_.reserve(headers_.size());
    for (const h2::HeaderField &field : headers_) {
        if (!field.name.empty() && field.name[0] != ':')
            request_header_views_.emplace_back(field.name, field.value);
    }

    finish_request_head(method_storage_, path_storage_, metadata.content_length, end_stream,
                        "HTTP/2");
}

void H2ServerStream::on_native_headers(std::string block, bool end_stream)
{
    if (head_ready_) {
        std::vector<native::HeaderView> trailers;
        if (!native::decode_trailers(block, trailers, native::kMaxHeaders, kMaxHeaderBlock)) {
            connection_->send_rst(id_, h2::ErrorCode::Protocol);
            fail(EPROTO);
            return;
        }

        remote_closed_ = remote_closed_ || end_stream;
        if (end_stream) {
            if (read_waiter_ != nullptr && recv_pending_ == 0)
                complete_read(read_waiter_);
        }
        check_closed();
        return;
    }

    native_head_storage_ = std::move(block);
    native::RequestHead decoded;
    if (!native::decode_request(native_head_storage_, request_header_views_, decoded,
                                native::kMaxHeaders,
                                std::min<std::size_t>(connection_->context().header_limit,
                                                      native::kMaxBlock))) {
        connection_->send_rst(id_, h2::ErrorCode::Protocol);
        fail(EPROTO);
        return;
    }

    long long content_length = -1;
    if (decoded.content_length.has_value()) {
        if (*decoded.content_length > static_cast<std::uint64_t>(LLONG_MAX)) {
            connection_->send_rst(id_, h2::ErrorCode::Protocol);
            fail(EPROTO);
            return;
        }
        content_length = static_cast<long long>(*decoded.content_length);
    }

    bool has_content_length_header = false;
    for (const Header &header : request_header_views_) {
        if (iequals(header.first, "content-length")) {
            has_content_length_header = true;
            break;
        }
    }
    if (!has_content_length_header && decoded.content_length.has_value()) {
        content_length_storage_ = std::to_string(*decoded.content_length);
        request_header_views_.emplace_back("content-length", content_length_storage_);
    }

    finish_request_head(decoded.method, decoded.target, content_length, end_stream, "realurl/1");
}

void H2ServerStream::on_data(std::string_view data, bool end_stream, std::uint32_t flow_bytes)
{
    if (closed_)
        return;

    recv_window_ -= static_cast<std::int64_t>(flow_bytes);
    if (recv_window_ < 0) {
        connection_->close_with_error(EPROTO);
        return;
    }
    recv_unacked_ += flow_bytes;

    if (read_waiter_ != nullptr) {
        ReadAwaiter *waiter = read_waiter_;
        const std::size_t n = std::min(data.size(), waiter->buffer_.size());
        if (n > 0)
            std::memcpy(waiter->buffer_.data(), data.data(), n);
        waiter->n_ = n;
        if (n < data.size()) {
            recv_queue_.push_back(std::string(data.substr(n)));
            recv_pending_ += data.size() - n;
        }
        complete_read(waiter);
    } else {
        recv_queue_.push_back(std::string(data));
        recv_pending_ += data.size();
    }

    if (end_stream)
        remote_closed_ = true;

    flush_window();
    if (end_stream && read_waiter_ != nullptr && recv_pending_ == 0)
        complete_read(read_waiter_);
    check_closed();
}

void H2ServerStream::on_rst()
{
    fail(ECONNRESET);
}

void H2ServerStream::on_window_update(std::uint32_t increment)
{
    if (send_window_ + static_cast<std::int64_t>(increment) > static_cast<std::int64_t>(kMaxWindow)) {
        fail(EPROTO);
        return;
    }
    send_window_ += increment;
    if (blocked_on_window_ && send_window_ > 0) {
        blocked_on_window_ = false;
        if (!in_write_queue_)
            connection_->enqueue_write(id_);
    } else {
        connection_->schedule_flush();
    }
}

void H2ServerStream::on_settings_initial_window(std::int32_t delta)
{
    send_window_ += delta;
    if (blocked_on_window_ && send_window_ > 0) {
        blocked_on_window_ = false;
        if (!in_write_queue_)
            connection_->enqueue_write(id_);
    }
}

void H2ServerStream::start_read(ReadAwaiter *waiter, std::coroutine_handle<> handle)
{
    if (closed_ || error_ != 0) {
        waiter->error_ = error_ != 0 ? error_ : ECANCELED;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }
    read_waiter_ = waiter;
    waiter->handle_ = handle;
}

void H2ServerStream::complete_read(ReadAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (read_waiter_ == waiter)
        read_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2ServerStream::start_write(WriteAwaiter *waiter, std::coroutine_handle<> handle)
{
    if (closed_) {
        waiter->error_ = error_ != 0 ? error_ : EPIPE;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }
    write_waiter_ = waiter;
    waiter->handle_ = handle;
}

void H2ServerStream::complete_write(WriteAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (write_waiter_ == waiter)
        write_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2ServerStream::start_end(EndAwaiter *waiter, std::coroutine_handle<> handle)
{
    if (end_sent_ || closed_) {
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }
    end_waiter_ = waiter;
    waiter->handle_ = handle;
}

void H2ServerStream::complete_end(EndAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (end_waiter_ == waiter)
        end_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

Task<std::size_t> H2ServerStream::read(std::span<char> buffer)
{
    if (buffer.empty())
        throw std::invalid_argument("h2 server read: empty buffer");

    if (recv_pending_ > 0) {
        std::size_t copied = 0;
        while (!recv_queue_.empty() && copied < buffer.size()) {
            auto &front = recv_queue_.front();
            const std::size_t available = front.size() - recv_chunk_offset_;
            const std::size_t n = std::min(available, buffer.size() - copied);
            std::memcpy(buffer.data() + copied, front.data() + recv_chunk_offset_, n);
            copied += n;
            recv_chunk_offset_ += n;
            recv_pending_ -= n;
            if (recv_chunk_offset_ == front.size()) {
                recv_queue_.pop_front();
                recv_chunk_offset_ = 0;
            }
        }
        flush_window();
        co_return copied;
    }

    if (error_ != 0)
        throw std::system_error(error_, std::generic_category(), "h2 server read");
    if (remote_closed_ || closed_)
        co_return 0;

    co_return co_await ReadAwaiter(*this, buffer);
}

Task<void> H2ServerStream::write_head(int status, std::span<const Header> headers,
                                      std::optional<std::uint64_t> content_length)
{
    if (head_sent_ || closed_)
        co_return;

    if (!connection_->encode_response_head(status, headers, content_length, head_block_)) {
        cancel();
        co_return;
    }

    const bool no_body = head_only_ || !status_allows_body(status) ||
                         (content_length.has_value() && *content_length == 0);
    connection_->queue_headers(id_, head_block_, no_body);
    head_sent_ = true;
    if (no_body) {
        end_sent_ = true;
        local_closed_ = true;
        check_closed();
    }
    co_return;
}

Task<bool> H2ServerStream::write(std::string_view data)
{
    if (!head_sent_)
        co_await write_head(200, {}, {});
    if (closed_ || head_only_ || end_pending_)
        co_return false;
    if (data.empty())
        co_return true;

    send_queue_.push_back(std::string(data));
    send_pending_ += data.size();
    connection_->enqueue_write(id_);

    if (send_pending_ >= kSendHighWater)
        co_await WriteAwaiter(*this);
    co_return !closed_;
}

Task<void> H2ServerStream::end()
{
    if (end_sent_ || closed_)
        co_return;
    end_pending_ = true;
    connection_->enqueue_write(id_);
    if (end_sent_)
        co_return;
    co_await EndAwaiter(*this);
}

void H2ServerStream::cancel()
{
    if (closed_)
        return;
    connection_->send_rst(id_, h2::ErrorCode::Cancel);
    fail(ECANCELED);
}

Task<void> H2ServerStream::send_error(int status)
{
    Response response;
    response.status = status;
    connection_->context().apply_error_body(response, status);
    co_await send_fixed_response(response);
}

Task<void> H2ServerStream::send_fixed_response(const Response &response)
{
    if (closed_ || connection_ == nullptr)
        co_return;

    const bool body_allowed = status_allows_body(response.status);
    const std::optional<std::uint64_t> content_length =
        body_allowed ? std::optional<std::uint64_t>(response.body_size()) : std::nullopt;

    std::vector<Header> views;
    views.reserve(response.headers.size() + 1);
    for (const auto &header : response.headers)
        views.emplace_back(header.first, header.second);
    if (!response.content_type.empty())
        views.emplace_back("content-type", response.content_type);

    if (!connection_->encode_response_head(response.status, views, content_length, head_block_)) {
        cancel();
        co_return;
    }

    const bool no_body = head_only_ || !body_allowed;
    connection_->queue_headers(id_, head_block_, no_body);
    head_sent_ = true;

    if (no_body) {
        end_sent_ = true;
        local_closed_ = true;
        check_closed();
        co_return;
    }

    const std::uint64_t size = response.body_size();
    if (size > 0) {
        if (!response.body_view.empty())
            send_queue_.push_back(std::string(response.body_view));
        else
            send_queue_.push_back(response.body);
        send_pending_ = send_queue_.front().size();
    }
    end_pending_ = true;
    connection_->enqueue_write(id_);
    if (end_sent_)
        co_return;
    co_await EndAwaiter(*this);
}

Task<void> H2ServerStream::run_handler()
{
    auto self = shared_from_this();
    (void)self;

    if (!head_ready_ || closed_)
        co_return;

    Response response;
    const Router::Route *route = nullptr;

    if (connection_->context().pre_routing) {
        Response pre_response;
        if (connection_->context().pre_routing(request_, pre_response) == HandlerResponse::Handled) {
            co_await send_fixed_response(pre_response);
            co_return;
        }
    }

    route = connection_->context().router.find(request_.method, request_.path, request_);
    if (route == nullptr) {
        co_await send_error(404);
        co_return;
    }
    request_.body_mode = route->options.body_mode;

    if (request_.content_length >= 0 && connection_->context().payload_max != 0 &&
        static_cast<std::uint64_t>(request_.content_length) > connection_->context().payload_max) {
        co_await send_error(413);
        co_return;
    }

    if (request_.body_mode == BodyMode::Buffered) {
        char buffer[16384];
        for (;;) {
            const std::size_t n = co_await read({ buffer, sizeof(buffer) });
            if (n == 0)
                break;
            if (connection_->context().payload_max != 0 &&
                request_.body.size() + n > connection_->context().payload_max) {
                co_await send_error(413);
                co_return;
            }
            request_.body.append(buffer, n);
        }
        request_.content_length = static_cast<long long>(request_.body.size());
    } else {
        request_.body_reader = this;
    }

    try {
        route->handler(request_, response);
    } catch (...) {
        if (!response.taken_over()) {
            response = Response{};
            response.status = 500;
            connection_->context().apply_error_body(response, 500);
        } else {
            cancel();
            co_return;
        }
    }

    if (response.task_taken_over()) {
        bool failed = false;
        try {
            co_await response.task_take_over_fn()(*this);
        } catch (...) {
            failed = true;
        }
        if (failed) {
            if (!head_sent_)
                co_await send_error(500);
            else
                cancel();
            co_return;
        }
        if (!head_sent_) {
            co_await send_error(500);
            co_return;
        }
        if (!end_sent_)
            co_await end();
        co_return;
    }

    if (response.taken_over()) {
        co_await send_error(501);
        co_return;
    }

    co_await send_fixed_response(response);
}

H2PumpResult H2ServerStream::pump_one_frame()
{
    if (closed_)
        return H2PumpResult::NoWork;

    if (send_pending_ > 0) {
        const std::int64_t window = std::min<std::int64_t>(connection_->send_window(), send_window_);
        if (window <= 0)
            return H2PumpResult::WindowFull;

        const std::size_t allowed = std::min<std::size_t>(
            { send_pending_, static_cast<std::size_t>(window), connection_->max_frame_size() });

        auto &front = send_queue_.front();
        const std::size_t available = front.size() - send_chunk_offset_;
        const std::size_t to_send = std::min(allowed, available);

        connection_->append_data(id_, std::string_view(front).substr(send_chunk_offset_, to_send), false);
        send_chunk_offset_ += to_send;
        send_pending_ -= to_send;
        connection_->consume_send_window(to_send);
        send_window_ -= static_cast<std::int64_t>(to_send);

        if (send_chunk_offset_ == front.size()) {
            send_queue_.pop_front();
            send_chunk_offset_ = 0;
        }

        if (send_pending_ <= kSendLowWater && write_waiter_ != nullptr)
            complete_write(write_waiter_);

        return H2PumpResult::SentFrame;
    }

    if (send_pending_ == 0 && end_pending_ && !end_sent_) {
        connection_->append_data(id_, {}, true);
        end_pending_ = false;
        end_sent_ = true;
        local_closed_ = true;
        if (end_waiter_ != nullptr)
            complete_end(end_waiter_);
        check_closed();
        return H2PumpResult::SentFrame;
    }

    return H2PumpResult::NoWork;
}

bool H2ServerStream::pump()
{
    bool queued = false;
    while (pump_one_frame() == H2PumpResult::SentFrame)
        queued = true;
    return queued;
}

void H2ServerStream::flush_window()
{
    if (closed_)
        return;
    if (recv_pending_ >= kRecvHighWater)
        return;
    if (recv_unacked_ >= kWindowUpdateThreshold) {
        connection_->send_window_update(id_, recv_unacked_);
        recv_window_ += recv_unacked_;
        recv_unacked_ = 0;
    }
}

void H2ServerStream::fail(int error)
{
    if (closed_)
        return;
    closed_ = true;
    error_ = error;
    in_write_queue_ = false;
    blocked_on_window_ = false;

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
    send_pending_ = 0;
    send_chunk_offset_ = 0;
    recv_queue_.clear();
    recv_pending_ = 0;
    recv_chunk_offset_ = 0;

    if (connection_ != nullptr) {
        connection_->on_stream_closed(id_);
        connection_ = nullptr;
    }
}

void H2ServerStream::check_closed()
{
    if (closed_ || !(local_closed_ && remote_closed_))
        return;
    closed_ = true;
    if (connection_ != nullptr) {
        connection_->on_stream_closed(id_);
        connection_ = nullptr;
    }
    if (read_waiter_ != nullptr && recv_pending_ == 0)
        complete_read(read_waiter_);
}

Task<void> run_h2_server(ServerContext &context, EventLoop &loop,
                         std::unique_ptr<Transport> transport, sockaddr_storage peer,
                         Client *upstream, WireMode mode)
{
    auto connection = std::make_shared<H2ServerConnection>(context, loop, std::move(transport),
                                                           peer, upstream, mode);
    co_await connection->serve();
}

} // namespace realurl::detail
