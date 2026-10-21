#pragma once

/// HTTP/2 frame header parsing and serialization (RFC 9113 section 4).

#include "realurl/detail/http2_types.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace realurl::detail::h2
{

struct FrameHeader {
    std::uint32_t length = 0;
    FrameType type = FrameType::Data;
    std::uint8_t flags = 0;
    std::uint32_t stream_id = 0;
};

struct Frame {
    FrameHeader header;
    std::string_view payload;
};

enum class ParseResult {
    Ok,
    NeedMore,
    Error,
};

class FrameParser {
public:
    explicit FrameParser(std::uint32_t max_frame_size = kDefaultMaxFrameSize)
        : max_frame_size_(max_frame_size)
    {
    }

    void set_max_frame_size(std::uint32_t value)
    {
        max_frame_size_ = value;
    }

    /// Parse one frame starting at offset. On Ok, offset advances past the
    /// frame and `out.payload` points into `data`. On NeedMore, offset is
    /// unchanged. On Error, the connection must be closed with FRAME_SIZE_ERROR.
    ParseResult next(std::string_view data, std::size_t &offset, Frame &out) const;

private:
    std::uint32_t max_frame_size_;
};

class FrameWriter {
public:
    void data(std::uint32_t stream_id, std::string_view payload, bool end_stream);
    void headers(std::uint32_t stream_id, std::string_view block, bool end_headers, bool end_stream);
    void continuation(std::uint32_t stream_id, std::string_view block, bool end_headers);
    void rst_stream(std::uint32_t stream_id, ErrorCode code);
    void settings(std::span<const Setting> settings);
    void settings_ack();
    void ping(bool ack, std::uint64_t opaque);
    void goaway(std::uint32_t last_stream_id, ErrorCode code, std::string_view debug = {});
    void window_update(std::uint32_t stream_id, std::uint32_t increment);

    std::string &buffer() noexcept
    {
        return buffer_;
    }

    void clear() noexcept
    {
        buffer_.clear();
    }

private:
    void write_header(FrameType type, std::uint8_t flags, std::uint32_t stream_id, std::size_t length);

    std::string buffer_;
};

} // namespace realurl::detail::h2
