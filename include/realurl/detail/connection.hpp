#pragma once

#include "realurl/detail/byte_queue.hpp"
#include "realurl/detail/event_loop.hpp"
#include "realurl/detail/frame.hpp"
#include "realurl/detail/hpack.hpp"
#include "realurl/detail/native_compression.hpp"
#include "realurl/detail/protocol.hpp"
#include "realurl/detail/socket.hpp"
#include "realurl/detail/transport.hpp"
#include "realurl/http2.hpp"

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace realurl::detail
{

using h2::ErrorCode;
using h2::Frame;
using h2::FrameParser;
using h2::FrameWriter;
using h2::HeaderField;
using h2::HeaderList;
using h2::HpackDecoder;
using h2::HpackEncoder;
using h2::kDefaultInitialWindowSize;
using h2::kDefaultMaxFrameSize;

class H2Connection;
class H2StreamImpl;

/// Thrown by the plaintext native probe when the peer does not speak
/// realurl/1. The client uses it to fall back to h2c.
class NativeUnsupported : public std::runtime_error {
public:
    NativeUnsupported()
        : std::runtime_error("peer does not speak realurl/1")
    {
    }
};

enum class PumpResult {
    NoWork,
    WindowFull,
    SentFrame,
};

/// One-shot wakeup primitive used by the connection writer coroutine.
class FlushSignal {
public:
    explicit FlushSignal(EventLoop &loop) noexcept
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

class H2StreamImpl : public std::enable_shared_from_this<H2StreamImpl> {
public:
    H2StreamImpl(H2Connection &connection, std::uint32_t id, std::uint32_t initial_window) noexcept;

    std::uint32_t id() const noexcept
    {
        return id_;
    }

    Task<ResponseHead> head(std::chrono::milliseconds timeout);
    Task<std::size_t> read(std::span<char> buffer, std::chrono::milliseconds idle);
    /// Append the rest of the body to `out` until END_STREAM.
    Task<void> read_all(std::string &out, std::chrono::milliseconds idle);
    Task<void> write(std::string_view data, std::chrono::milliseconds timeout);
    Task<void> end(std::chrono::milliseconds timeout);
    void cancel();

    void on_head(ResponseHead &&head, bool end_stream);
    void on_data(std::string_view payload, std::size_t decoded, bool compressed, bool end_stream,
                 std::uint32_t flow_bytes);
    void on_rst(ErrorCode code);
    void on_window_update(std::uint32_t increment);
    void on_connection_closed(int error);
    void on_settings_initial_window(std::int32_t delta);
    PumpResult pump_one_frame();
    bool pump();
    bool done() const noexcept
    {
        return closed_;
    }
    bool has_pending_work() const noexcept
    {
        return !send_queue_.empty() || (end_pending_ && !end_sent_);
    }

private:
    friend class H2Connection;
    class HeadAwaiter;
    class ReadAwaiter;
    class WriteAwaiter;
    class EndAwaiter;

    friend class HeadAwaiter;
    friend class ReadAwaiter;
    friend class WriteAwaiter;
    friend class EndAwaiter;

    void start_head(HeadAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout);
    void complete_head(HeadAwaiter *waiter);
    void start_read(ReadAwaiter *waiter, std::coroutine_handle<> handle);
    void complete_read(ReadAwaiter *waiter);
    void start_write(WriteAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout);
    void complete_write(WriteAwaiter *waiter);
    void start_end(EndAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout);
    void complete_end(EndAwaiter *waiter);

    void fail(int error);
    void check_closed();
    void flush_window();
    bool wake_reader();
    void reject_data();
    std::size_t send_pending() const noexcept
    {
        return send_queue_.size();
    }

    static constexpr std::size_t kSendHighWater = 256 * 1024;
    static constexpr std::size_t kSendLowWater = 128 * 1024;
    static constexpr std::size_t kRecvHighWater = 1024 * 1024;
    static constexpr std::size_t kWindowUpdateThreshold = 32 * 1024;

    H2Connection *connection_ = nullptr;
    EventLoop *loop_ = nullptr;
    std::uint32_t id_ = 0;

    bool head_ready_ = false;
    bool local_closed_ = false;
    bool remote_closed_ = false;
    bool closed_ = false;
    bool end_pending_ = false;
    bool end_sent_ = false;
    bool in_write_queue_ = false;
    bool blocked_on_window_ = false;
    /// The request head carried content-encoding; its body is sent as is.
    bool body_encoded_ = false;
    int error_ = 0;

    ResponseHead head_result_;

    native::RecvQueue recv_queue_;
    std::size_t recv_unacked_ = 0;

    ByteQueue send_queue_;
    std::int64_t send_window_ = kDefaultInitialWindowSize;

    HeadAwaiter *head_waiter_ = nullptr;
    ReadAwaiter *read_waiter_ = nullptr;
    WriteAwaiter *write_waiter_ = nullptr;
    EndAwaiter *end_waiter_ = nullptr;
};

class H2Connection : public std::enable_shared_from_this<H2Connection> {
public:
    static Task<std::shared_ptr<H2Connection>> connect(
        EventLoop &loop, std::unique_ptr<Transport> transport, std::string authority,
        std::string scheme, WireMode mode = WireMode::Http2, bool probe_native = false,
        std::chrono::milliseconds probe_timeout = std::chrono::milliseconds{ 1000 },
        bool native_compression = false);

    std::shared_ptr<H2StreamImpl> open(std::string_view method, std::string_view path, HeaderList headers,
                                       bool end_stream,
                                       std::optional<std::uint64_t> content_length = std::nullopt);

    bool usable() const noexcept
    {
        return usable_ && !closed_;
    }

    std::size_t active_streams() const noexcept
    {
        return streams_.size();
    }

    std::size_t max_streams() const noexcept
    {
        return max_streams_;
    }

    EventLoop &loop() noexcept
    {
        return loop_;
    }

    void schedule_flush();
    void enqueue_write(std::uint32_t stream_id);
    /// Queue one DATA frame and return its wire length. Compressible payloads
    /// are compressed once the native peer has advertised support.
    std::size_t append_data(std::uint32_t stream_id, std::string_view data, bool end_stream, bool compressible);
    void send_rst(std::uint32_t stream_id, ErrorCode code);
    void send_window_update(std::uint32_t stream_id, std::uint32_t increment);
    void on_stream_closed(std::uint32_t stream_id);

    std::int64_t send_window() const noexcept
    {
        return send_window_;
    }

    void consume_send_window(std::size_t bytes) noexcept
    {
        send_window_ -= static_cast<std::int64_t>(bytes);
    }

    std::uint32_t max_frame_size() const noexcept
    {
        return max_frame_size_;
    }

    void close();

private:
    H2Connection(EventLoop &loop, std::unique_ptr<Transport> transport, std::string authority,
                 std::string scheme, WireMode mode, bool native_compression);

    Task<void> reader_loop();
    Task<void> writer_loop();

    void send_preface();
    void process_frames();
    void handle_frame(const Frame &frame);
    void handle_settings(std::string_view payload, bool ack);
    void handle_headers(const Frame &frame);
    void finish_headers(std::uint32_t stream_id, std::string_view block, bool end_stream);
    void handle_data(const Frame &frame);
    void handle_goaway(std::string_view payload);
    void handle_window_update(const Frame &frame);
    void send_goaway(ErrorCode code);
    void send_headers(std::uint32_t stream_id, std::string_view block, bool end_stream);
    void close_with_error(int error);
    void reap();

    static constexpr std::uint32_t kConnRecvWindow = 16 * 1024 * 1024;
    static constexpr std::uint32_t kStreamRecvWindow = 4 * 1024 * 1024;

    EventLoop &loop_;
    std::unique_ptr<Transport> transport_;
    std::string authority_;
    std::string scheme_;
    WireMode mode_ = WireMode::Http2;

    FrameParser parser_;
    HpackDecoder decoder_;
    HpackEncoder encoder_;
    native::DataCodec codec_;

    std::string native_head_scratch_;
    std::vector<std::pair<std::string_view, std::string_view>> native_views_;
    std::string read_buffer_;
    std::string write_buffer_;
    std::string out_buffer_;
    std::string continuation_block_;
    std::uint32_t continuation_stream_ = 0;
    bool continuation_end_stream_ = false;
    bool expecting_continuation_ = false;

    std::unordered_map<std::uint32_t, std::shared_ptr<H2StreamImpl>> streams_;
    std::deque<std::uint32_t> write_queue_;
    std::vector<std::uint32_t> blocked_streams_;
    std::vector<std::uint32_t> pending_removals_;

    FlushSignal flush_signal_;

    std::uint32_t next_stream_id_ = 1;
    std::uint32_t max_streams_ = 100;
    std::uint32_t max_frame_size_ = kDefaultMaxFrameSize;
    /// SETTINGS_MAX_FRAME_SIZE we advertise and parse with. The peer
    /// compresses at most this much input per DATA frame, so it also bounds
    /// one decompressed frame.
    std::uint32_t local_max_frame_size_ = kDefaultMaxFrameSize;
    std::uint32_t peer_initial_window_ = kDefaultInitialWindowSize;
    std::int64_t send_window_ = kDefaultInitialWindowSize;
    std::uint32_t recv_unacked_ = 0;

    /// We advertised kSettingNativeCompression and accept compressed DATA.
    bool compression_offered_ = false;
    /// The peer advertised zstd decoding; outgoing DATA may be compressed.
    bool peer_compression_ = false;
    bool usable_ = true;
    bool closed_ = false;
};

} // namespace realurl::detail
