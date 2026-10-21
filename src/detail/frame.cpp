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

void FrameWriter::write_header(FrameType type, std::uint8_t flags, std::uint32_t stream_id, std::size_t length)
{
    buffer_.push_back(static_cast<char>((length >> 16) & 0xff));
    buffer_.push_back(static_cast<char>((length >> 8) & 0xff));
    buffer_.push_back(static_cast<char>(length & 0xff));
    buffer_.push_back(static_cast<char>(type));
    buffer_.push_back(static_cast<char>(flags));
    append_u32(buffer_, stream_id & 0x7fffffffu);
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

} // namespace realurl::detail::h2
