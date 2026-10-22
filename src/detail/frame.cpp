#include "realurl/detail/frame.hpp"

#include <cstring>

namespace realurl::detail::h2
{
namespace
{

void append_u32(std::string &out, std::uint32_t value)
{
    out.push_back(static_cast<char>((value >> 24) & 0xff));
    out.push_back(static_cast<char>((value >> 16) & 0xff));
    out.push_back(static_cast<char>((value >> 8) & 0xff));
    out.push_back(static_cast<char>(value & 0xff));
}

void append_u16(std::string &out, std::uint16_t value)
{
    out.push_back(static_cast<char>((value >> 8) & 0xff));
    out.push_back(static_cast<char>(value & 0xff));
}

std::uint32_t read_u32(const char *data)
{
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(data[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[2])) << 8) |
           static_cast<std::uint32_t>(static_cast<unsigned char>(data[3]));
}

} // namespace

ParseResult FrameParser::next(std::string_view data, std::size_t &offset, Frame &out) const
{
    if (data.size() - offset < 9)
        return ParseResult::NeedMore;

    const char *raw = data.data() + offset;
    const std::uint32_t length =
        (static_cast<std::uint32_t>(static_cast<unsigned char>(raw[0])) << 16) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(raw[1])) << 8) |
        static_cast<std::uint32_t>(static_cast<unsigned char>(raw[2]));
    const auto type = static_cast<FrameType>(static_cast<unsigned char>(raw[3]));
    const std::uint8_t flags = static_cast<std::uint8_t>(raw[4]);
    const std::uint32_t stream_id = read_u32(raw + 5) & 0x7fffffffu;

    if (length > max_frame_size_)
        return ParseResult::Error;
    if (data.size() - offset - 9 < length)
        return ParseResult::NeedMore;

    out.header = { length, type, flags, stream_id };
    out.payload = data.substr(offset + 9, length);
    offset += 9 + length;
    return ParseResult::Ok;
}

void append_frame_header(std::string &out, FrameType type, std::uint8_t flags, std::uint32_t stream_id,
                         std::size_t length)
{
    const char header[9] = {
        static_cast<char>((length >> 16) & 0xff),
        static_cast<char>((length >> 8) & 0xff),
        static_cast<char>(length & 0xff),
        static_cast<char>(type),
        static_cast<char>(flags),
        static_cast<char>((stream_id >> 24) & 0x7f),
        static_cast<char>((stream_id >> 16) & 0xff),
        static_cast<char>((stream_id >> 8) & 0xff),
        static_cast<char>(stream_id & 0xff),
    };
    out.append(header, sizeof(header));
}

void append_headers(std::string &out, std::uint32_t stream_id, std::string_view block, bool end_stream,
                    std::size_t max_frame_size)
{
    FrameType type = FrameType::Headers;
    std::uint8_t flags = end_stream ? kFlagEndStream : 0;
    do {
        const std::string_view chunk = block.substr(0, max_frame_size);
        block.remove_prefix(chunk.size());
        if (block.empty())
            flags |= kFlagEndHeaders;
        append_frame_header(out, type, flags, stream_id, chunk.size());
        out.append(chunk);
        type = FrameType::Continuation;
        flags = 0;
    } while (!block.empty());
}

void FrameWriter::write_header(FrameType type, std::uint8_t flags, std::uint32_t stream_id, std::size_t length)
{
    append_frame_header(buffer_, type, flags, stream_id, length);
}

void FrameWriter::data(std::uint32_t stream_id, std::string_view payload, bool end_stream)
{
    std::uint8_t flags = 0;
    if (end_stream)
        flags |= kFlagEndStream;
    write_header(FrameType::Data, flags, stream_id, payload.size());
    buffer_.append(payload);
}

void FrameWriter::headers(std::uint32_t stream_id, std::string_view block, bool end_headers, bool end_stream)
{
    std::uint8_t flags = 0;
    if (end_headers)
        flags |= kFlagEndHeaders;
    if (end_stream)
        flags |= kFlagEndStream;
    write_header(FrameType::Headers, flags, stream_id, block.size());
    buffer_.append(block);
}

void FrameWriter::continuation(std::uint32_t stream_id, std::string_view block, bool end_headers)
{
    write_header(FrameType::Continuation, end_headers ? kFlagEndHeaders : 0, stream_id, block.size());
    buffer_.append(block);
}

void FrameWriter::rst_stream(std::uint32_t stream_id, ErrorCode code)
{
    write_header(FrameType::RstStream, 0, stream_id, 4);
    append_u32(buffer_, static_cast<std::uint32_t>(code));
}

void FrameWriter::settings(std::span<const Setting> settings)
{
    write_header(FrameType::Settings, 0, 0, settings.size() * 6);
    for (const Setting &setting : settings) {
        append_u16(buffer_, setting.id);
        append_u32(buffer_, setting.value);
    }
}

void FrameWriter::settings_ack()
{
    write_header(FrameType::Settings, kFlagAck, 0, 0);
}

void FrameWriter::ping(bool ack, std::uint64_t opaque)
{
    write_header(FrameType::Ping, ack ? kFlagAck : 0, 0, 8);
    append_u32(buffer_, static_cast<std::uint32_t>(opaque >> 32));
    append_u32(buffer_, static_cast<std::uint32_t>(opaque));
}

void FrameWriter::goaway(std::uint32_t last_stream_id, ErrorCode code, std::string_view debug)
{
    write_header(FrameType::GoAway, 0, 0, 8 + debug.size());
    append_u32(buffer_, last_stream_id & 0x7fffffffu);
    append_u32(buffer_, static_cast<std::uint32_t>(code));
    buffer_.append(debug);
}

void FrameWriter::window_update(std::uint32_t stream_id, std::uint32_t increment)
{
    write_header(FrameType::WindowUpdate, 0, stream_id, 4);
    append_u32(buffer_, increment & 0x7fffffffu);
}

void FrameWriter::extension(std::uint8_t type, std::uint32_t stream_id, std::uint8_t flags,
                            std::string_view payload)
{
    write_header(static_cast<FrameType>(type), flags, stream_id, payload.size());
    buffer_.append(payload);
}

} // namespace realurl::detail::h2
