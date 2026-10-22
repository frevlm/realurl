#pragma once

/// Per-frame zstd compression of realurl native DATA frames.
///
/// Each compressed DATA frame carries kFlagNativeCompressed and exactly one
/// standalone zstd frame that declares its content size. There is no context
/// shared across frames, so frames decode independently and in any order.

#include "realurl/detail/byte_queue.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <span>
#include <string>
#include <string_view>

struct ZSTD_CCtx_s;

namespace realurl::detail::native
{

/// Payloads smaller than this are always sent uncompressed.
inline constexpr std::size_t kCompressMinBytes = 1024;

inline constexpr int kCompressionLevel = 1;

/// zstd compression context owned by one connection. It is created on first
/// use and reused for every frame.
class DataCodec {
public:
    DataCodec() noexcept = default;
    ~DataCodec();

    DataCodec(const DataCodec &) = delete;
    DataCodec &operator=(const DataCodec &) = delete;

    /// True when realurl was built with zstd.
    static bool available() noexcept;

    /// Compress `data` into one zstd frame. Returns an empty view when the
    /// result would not be smaller than `data`. The view is valid until the
    /// next compress().
    std::string_view compress(std::string_view data);

private:
    // Unused when built without zstd.
    [[maybe_unused]] ZSTD_CCtx_s *cctx_ = nullptr;
    std::string compressed_;
};

/// Check the header of one compressed DATA payload and return its declared
/// content size. Fails on a missing size, a size above `max_size`, or bytes
/// after the zstd frame.
bool content_size(std::string_view frame, std::size_t max_size, std::size_t &size);

/// Decode one zstd frame into exactly `size` bytes at `out`, using a decoder
/// context shared by the calling thread.
bool decompress(std::string_view frame, char *out, std::size_t size);

/// Append `data` as one DATA frame. With a codec, payloads of at least
/// kCompressMinBytes are sent compressed when that makes them smaller.
/// Returns the frame length, which is what flow control counts.
std::size_t append_data_frame(std::string &out, std::uint32_t stream_id, std::string_view data, bool end_stream,
                              DataCodec *codec);

/// Received DATA of one stream. Compressed payloads stay compressed until
/// the reader pulls them, so the queue holds the wire bytes that flow control
/// admitted plus the undelivered rest of at most one decoded frame.
class RecvQueue {
public:
    /// Bytes held in memory.
    std::size_t size() const noexcept
    {
        return wire_.size() + decoded_left();
    }

    bool empty() const noexcept
    {
        return size() == 0;
    }

    /// Bytes the reader will receive.
    std::size_t decoded_size() const noexcept
    {
        return pending_ + decoded_left();
    }

    /// Queue one DATA payload that decodes to `decoded` bytes. A compressed
    /// payload must have passed content_size().
    void push(std::string_view payload, std::size_t decoded, bool compressed);

    /// Move up to out.size() decoded bytes into `out` and set `n` to the
    /// count. Fails when a compressed payload does not decode.
    bool read(std::span<char> out, std::size_t &n);

    /// Append every queued byte, decoded, to `out`.
    bool drain_to(std::string &out);

    void clear() noexcept;

private:
    /// One compressed payload, located by its offset in the received wire
    /// bytes. Bytes between records are plain.
    struct Chunk {
        std::uint64_t offset;
        std::uint32_t wire;
        std::uint32_t decoded;
    };

    std::size_t decoded_left() const noexcept
    {
        return decoded_.size() - decoded_head_;
    }

    /// Plain bytes queued before the next compressed payload.
    std::size_t plain_front() const noexcept
    {
        if (chunks_.empty())
            return wire_.size();
        return static_cast<std::size_t>(chunks_.front().offset - (received_ - wire_.size()));
    }

    /// Copy the rest of the last decoded payload into `out`.
    std::size_t take_decoded(std::span<char> out) noexcept;
    /// Decode the front payload into `out` when it fits, else into decoded_.
    bool decode_front(std::span<char> out, std::size_t &n);

    ByteQueue wire_;
    std::deque<Chunk> chunks_;
    /// Total wire bytes ever queued; the end offset of wire_.
    std::uint64_t received_ = 0;
    /// Decoded size of the bytes in wire_.
    std::size_t pending_ = 0;
    std::string decoded_;
    std::size_t decoded_head_ = 0;
};

} // namespace realurl::detail::native
