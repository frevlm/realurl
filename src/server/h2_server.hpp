#pragma once

#include "server_internal.hpp"

#include "realurl/detail/frame.hpp"
#include "realurl/detail/hpack.hpp"
#include "realurl/detail/native_compression.hpp"

#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace realurl::detail
{

class H2ServerConnection;
class H2ServerStream;

class H2ServerConnection : public std::enable_shared_from_this<H2ServerConnection>
{
public:
    H2ServerConnection(ServerContext &context, EventLoop &loop,
                       std::unique_ptr<Transport> transport, sockaddr_storage peer, Client *upstream,
                       WireMode mode);
    ~H2ServerConnection();

    H2ServerConnection(const H2ServerConnection &) = delete;
    H2ServerConnection &operator=(const H2ServerConnection &) = delete;

    Task<void> serve();

    EventLoop &loop() noexcept
    {
        return loop_;
    }

    ServerContext &context() noexcept
    {
        return context_;
    }

    /// Queue one DATA frame and return its wire length. Compressible payloads
    /// are compressed once the native peer has advertised support.
    std::size_t append_data(std::uint32_t stream_id, std::string_view data, bool end_stream, bool compressible);
    void send_rst(std::uint32_t stream_id, h2::ErrorCode code);
    void send_window_update(std::uint32_t stream_id, std::uint32_t increment);
    void enqueue_write(std::uint32_t stream_id);
    void schedule_flush();
    void on_stream_closed(std::uint32_t stream_id);
    void close_with_error(int error);

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

    /// Encode an HTTP/2 or native response head and queue its HEADERS
    /// frames. Returns false when the head cannot be encoded.
    bool send_response_head(std::uint32_t stream_id, int status, std::span<const Header> headers,
                            std::optional<std::uint64_t> content_length, bool end_stream);

private:
    friend class H2ServerStream;

    class FlushSignal;

    Task<void> reader_loop();
    Task<void> writer_loop();
    void send_preface();
    void process_frames();
    void handle_frame(const h2::Frame &frame);
    void handle_settings(std::string_view payload, bool ack);
    void handle_headers(const h2::Frame &frame);
    void finish_headers(std::uint32_t stream_id, std::string_view block, bool end_stream);
    void open_stream(std::uint32_t stream_id, std::string_view block, h2::HeaderList &&headers, bool end_stream);
    void deliver_headers(H2ServerStream &stream, std::string_view block, h2::HeaderList &&headers, bool end_stream);
    void handle_data(const h2::Frame &frame);
    void handle_window_update(const h2::Frame &frame);
    void handle_goaway(std::string_view payload);
    void send_goaway(h2::ErrorCode code);
    bool encode_response_head(int status, std::span<const Header> headers,
                              std::optional<std::uint64_t> content_length, std::string &out);

    ServerContext &context_;
    EventLoop &loop_;
    std::unique_ptr<Transport> transport_;
    Client *upstream_ = nullptr;
    WireMode mode_ = WireMode::Http2;
    sockaddr_storage peer_{};
    std::string remote_addr_;
    int remote_port_ = 0;

    h2::FrameParser parser_;
    h2::HpackDecoder decoder_;
    h2::HpackEncoder encoder_;
    native::DataCodec codec_;

    std::string read_buffer_;
    std::string write_buffer_;
    std::string out_buffer_;
    std::string continuation_block_;
    std::string head_scratch_;
    std::vector<Header> header_scratch_;
    std::uint32_t continuation_stream_ = 0;
    bool continuation_end_stream_ = false;
    bool expecting_continuation_ = false;

    std::unordered_map<std::uint32_t, std::shared_ptr<H2ServerStream>> streams_;
    std::deque<std::uint32_t> write_queue_;
    std::vector<std::uint32_t> blocked_streams_;
    std::vector<std::uint32_t> pending_removals_;

    std::unique_ptr<FlushSignal> flush_signal_;
    std::unique_ptr<FlushSignal> close_signal_;

    std::uint32_t max_stream_id_ = 0;
    std::uint32_t max_frame_size_ = h2::kDefaultMaxFrameSize;
    /// SETTINGS_MAX_FRAME_SIZE we advertise and parse with. The peer
    /// compresses at most this much input per DATA frame, so it also bounds
    /// one decompressed frame.
    std::uint32_t local_max_frame_size_ = h2::kDefaultMaxFrameSize;
    std::uint32_t peer_initial_window_ = h2::kDefaultInitialWindowSize;
    std::int64_t send_window_ = h2::kDefaultInitialWindowSize;
    std::uint32_t recv_unacked_ = 0;

    std::size_t active_streams_ = 0;
    /// We advertised kSettingNativeCompression and accept compressed DATA.
    bool compression_offered_ = false;
    /// The peer advertised zstd decoding; outgoing DATA may be compressed.
    bool peer_compression_ = false;
    bool closed_ = false;
};

Task<void> run_h2_server(ServerContext &context, EventLoop &loop,
                         std::unique_ptr<Transport> transport, sockaddr_storage peer,
                         Client *upstream, WireMode mode = WireMode::Http2);

} // namespace realurl::detail
