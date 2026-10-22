#include "realurl/detail/native_compression.hpp"

#include "realurl/detail/frame.hpp"
#include "realurl/detail/protocol.hpp"

#include <algorithm>
#include <cstring>
#include <memory>

#ifdef REALURL_HAVE_ZSTD
#include <zstd.h>
#endif

namespace realurl::detail::native
{

#ifdef REALURL_HAVE_ZSTD

namespace
{

struct DCtxDeleter {
    void operator()(ZSTD_DCtx *ctx) const noexcept
    {
        ZSTD_freeDCtx(ctx);
    }
};

/// Decoding is stateless per frame, so one context serves every connection
/// on a loop thread, including streams whose connection is gone.
ZSTD_DCtx *thread_dctx()
{
    thread_local std::unique_ptr<ZSTD_DCtx, DCtxDeleter> ctx(ZSTD_createDCtx());
    return ctx.get();
}

} // namespace

DataCodec::~DataCodec()
{
    ZSTD_freeCCtx(cctx_);
}

bool DataCodec::available() noexcept
{
    return true;
}

std::string_view DataCodec::compress(std::string_view data)
{
    if (data.size() < 2)
        return {};
    if (cctx_ == nullptr && (cctx_ = ZSTD_createCCtx()) == nullptr)
        return {};

    // A destination one byte short of the input makes zstd fail instead of
    // producing output that is not smaller.
    compressed_.resize(data.size() - 1);
    const std::size_t n =
        ZSTD_compressCCtx(cctx_, compressed_.data(), compressed_.size(), data.data(), data.size(), kCompressionLevel);
    if (ZSTD_isError(n))
        return {};
    return { compressed_.data(), n };
}

bool content_size(std::string_view frame, std::size_t max_size, std::size_t &size)
{
    const unsigned long long declared = ZSTD_getFrameContentSize(frame.data(), frame.size());
    if (declared == ZSTD_CONTENTSIZE_UNKNOWN || declared == ZSTD_CONTENTSIZE_ERROR || declared > max_size)
        return false;
    if (ZSTD_findFrameCompressedSize(frame.data(), frame.size()) != frame.size())
        return false;
    size = static_cast<std::size_t>(declared);
    return true;
}

bool decompress(std::string_view frame, char *out, std::size_t size)
{
    ZSTD_DCtx *ctx = thread_dctx();
    if (ctx == nullptr)
        return false;
    const std::size_t n = ZSTD_decompressDCtx(ctx, out, size, frame.data(), frame.size());
    return !ZSTD_isError(n) && n == size;
}

#else

DataCodec::~DataCodec() = default;

bool DataCodec::available() noexcept
{
    return false;
}

std::string_view DataCodec::compress(std::string_view)
{
    return {};
}

bool content_size(std::string_view, std::size_t, std::size_t &)
{
    return false;
}

bool decompress(std::string_view, char *, std::size_t)
{
    return false;
}

#endif

std::size_t append_data_frame(std::string &out, std::uint32_t stream_id, std::string_view data, bool end_stream,
                              DataCodec *codec)
{
    std::uint8_t flags = end_stream ? h2::kFlagEndStream : 0;
    if (codec != nullptr && data.size() >= kCompressMinBytes) {
        const std::string_view compressed = codec->compress(data);
        if (!compressed.empty()) {
            data = compressed;
            flags |= kFlagNativeCompressed;
        }
    }
    h2::append_frame_header(out, h2::FrameType::Data, flags, stream_id, data.size());
    out.append(data);
    return data.size();
}

void RecvQueue::push(std::string_view payload, std::size_t decoded, bool compressed)
{
    if (decoded == 0)
        return;
    if (compressed)
        chunks_.push_back(
            { received_, static_cast<std::uint32_t>(payload.size()), static_cast<std::uint32_t>(decoded) });
    received_ += payload.size();
    pending_ += decoded;
    wire_.append(payload);
}

bool RecvQueue::read(std::span<char> out, std::size_t &n)
{
    n = take_decoded(out);
    while (n < out.size() && !wire_.empty()) {
        const std::size_t plain = plain_front();
        if (plain == 0) {
            if (!decode_front(out.subspan(n), n))
                return false;
            continue;
        }
        const std::size_t count = wire_.read(out.subspan(n, std::min(out.size() - n, plain)));
        pending_ -= count;
        n += count;
    }
    return true;
}

bool RecvQueue::drain_to(std::string &out)
{
    const std::size_t base = out.size();
    out.resize(base + decoded_size());
    std::size_t n = 0;
    const bool ok = read({ out.data() + base, out.size() - base }, n);
    out.resize(base + n);
    return ok;
}

void RecvQueue::clear() noexcept
{
    wire_.clear();
    chunks_.clear();
    pending_ = 0;
    decoded_.clear();
    decoded_head_ = 0;
}

std::size_t RecvQueue::take_decoded(std::span<char> out) noexcept
{
    const std::size_t n = std::min(out.size(), decoded_left());
    if (n > 0)
        std::memcpy(out.data(), decoded_.data() + decoded_head_, n);
    decoded_head_ += n;
    if (decoded_head_ == decoded_.size()) {
        decoded_.clear();
        decoded_head_ = 0;
    }
    return n;
}

bool RecvQueue::decode_front(std::span<char> out, std::size_t &n)
{
    const Chunk chunk = chunks_.front();
    const bool direct = chunk.decoded <= out.size();
    if (!direct)
        decoded_.resize(chunk.decoded);
    const bool ok = decompress(wire_.front(chunk.wire), direct ? out.data() : decoded_.data(), chunk.decoded);
    wire_.consume(chunk.wire);
    chunks_.pop_front();
    pending_ -= chunk.decoded;
    if (!ok) {
        clear();
        return false;
    }
    n += direct ? chunk.decoded : take_decoded(out);
    return true;
}

} // namespace realurl::detail::native
