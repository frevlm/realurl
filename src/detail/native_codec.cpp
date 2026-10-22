#include "realurl/detail/native_codec.hpp"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace realurl::detail::native
{
namespace
{

constexpr std::string_view kMethods[] = {
    "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE",
};

/// Sorted by byte value; header_code() binary-searches this table.
constexpr std::string_view kHeaderNames[] = {
    "accept",
    "accept-encoding",
    "accept-language",
    "authorization",
    "cache-control",
    "connection",
    "content-encoding",
    "content-length",
    "content-type",
    "cookie",
    "date",
    "etag",
    "expect",
    "host",
    "if-modified-since",
    "if-none-match",
    "location",
    "origin",
    "pragma",
    "range",
    "referer",
    "server",
    "set-cookie",
    "te",
    "traceparent",
    "tracestate",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "user-agent",
    "vary",
    "via",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-request-id",
};

constexpr std::size_t kMethodCount = std::size(kMethods);
constexpr std::size_t kHeaderNameCount = std::size(kHeaderNames);

constexpr std::uint8_t kRequestTag = 0x01;
constexpr std::uint8_t kTrailersTag = 0x02;
constexpr std::uint8_t kResponseTag = 0x11;
constexpr std::uint8_t kLiteral = 0xff;
constexpr std::uint8_t kFlagContentLength = 0x01;

unsigned char lower(unsigned char c) noexcept
{
    return c >= 'A' && c <= 'Z' ? static_cast<unsigned char>(c - 'A' + 'a') : c;
}

int compare_ci(std::string_view left, std::string_view right) noexcept
{
    const std::size_t count = std::min(left.size(), right.size());
    for (std::size_t i = 0; i < count; ++i) {
        const unsigned char a = lower(static_cast<unsigned char>(left[i]));
        const unsigned char b = lower(static_cast<unsigned char>(right[i]));
        if (a != b)
            return a < b ? -1 : 1;
    }
    if (left.size() == right.size())
        return 0;
    return left.size() < right.size() ? -1 : 1;
}

bool iequals(std::string_view left, std::string_view right) noexcept
{
    return compare_ci(left, right) == 0;
}

bool control(char c) noexcept
{
    return c == '\r' || c == '\n' || c == '\0';
}

bool valid_name(std::string_view name) noexcept
{
    if (name.empty() || name.front() == ':')
        return false;
    for (char c : name) {
        if (control(c) || c == ' ')
            return false;
    }
    return true;
}

bool valid_value(std::string_view value) noexcept
{
    for (char c : value) {
        if (control(c))
            return false;
    }
    return true;
}

bool token_char(unsigned char c) noexcept
{
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
        return true;
    switch (c) {
    case '!':
    case '#':
    case '$':
    case '%':
    case '&':
    case '\'':
    case '*':
    case '+':
    case '-':
    case '.':
    case '^':
    case '_':
    case '`':
    case '|':
    case '~':
        return true;
    default:
        return false;
    }
}

bool valid_method(std::string_view method) noexcept
{
    if (method.empty())
        return false;
    for (unsigned char c : method) {
        if (!token_char(c))
            return false;
    }
    return true;
}

bool valid_target(std::string_view target) noexcept
{
    if (target.empty() || (target.front() != '/' && target.front() != '*'))
        return false;
    for (char c : target) {
        if (control(c) || c == ' ')
            return false;
    }
    return true;
}

bool hop_by_hop(std::string_view name) noexcept
{
    return iequals(name, "connection") || iequals(name, "keep-alive") || iequals(name, "proxy-connection") ||
           iequals(name, "transfer-encoding") || iequals(name, "upgrade");
}

void append_varint(std::string &out, std::uint64_t value)
{
    while (value >= 0x80) {
        out.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

bool read_varint(std::string_view data, std::size_t &offset, std::uint64_t max, std::uint64_t &out)
{
    std::uint64_t result = 0;
    unsigned shift = 0;
    for (unsigned i = 0; i < 10; ++i) {
        if (offset >= data.size())
            return false;
        const auto byte = static_cast<std::uint8_t>(data[offset++]);
        const std::uint64_t part = byte & 0x7f;
        if (shift >= 64 || part > (std::numeric_limits<std::uint64_t>::max() >> shift))
            return false;
        result |= part << shift;
        if ((byte & 0x80) == 0) {
            if (result > max)
                return false;
            out = result;
            return true;
        }
        shift += 7;
    }
    return false;
}

bool read_bytes(std::string_view data, std::size_t &offset, std::uint64_t length, std::string_view &out)
{
    if (offset > data.size())
        return false;
    const std::uint64_t available = data.size() - offset;
    if (length > available)
        return false;
    out = data.substr(offset, static_cast<std::size_t>(length));
    offset += static_cast<std::size_t>(length);
    return true;
}

std::uint8_t method_code(std::string_view method) noexcept
{
    for (std::size_t i = 0; i < kMethodCount; ++i) {
        if (kMethods[i] == method)
            return static_cast<std::uint8_t>(i);
    }
    return kLiteral;
}

std::uint8_t header_code(std::string_view name) noexcept
{
    const auto begin = std::begin(kHeaderNames);
    const auto end = std::end(kHeaderNames);
    const auto it = std::lower_bound(begin, end, name, [](std::string_view left, std::string_view right) {
        return compare_ci(left, right) < 0;
    });
    if (it == end || compare_ci(*it, name) != 0)
        return kLiteral;
    return static_cast<std::uint8_t>(std::distance(begin, it));
}

std::string_view header_name(std::uint8_t code) noexcept
{
    if (code >= kHeaderNameCount)
        return {};
    return kHeaderNames[code];
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
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10)
            return false;
        value = value * 10 + digit;
    }
    out = value;
    return true;
}

bool append_header(std::string &out, std::string_view name, std::string_view value)
{
    if (!valid_name(name) || !valid_value(value))
        return false;
    const std::uint8_t code = header_code(name);
    out.push_back(static_cast<char>(code));
    if (code == kLiteral) {
        append_varint(out, name.size());
        out.append(name);
    }
    append_varint(out, value.size());
    out.append(value);
    return out.size() <= kMaxBlock;
}

bool append_flags(std::string &out, std::optional<std::uint64_t> content_length)
{
    out.push_back(static_cast<char>(content_length.has_value() ? kFlagContentLength : 0));
    if (content_length.has_value())
        append_varint(out, *content_length);
    return out.size() <= kMaxBlock;
}

bool decode_header_list(std::string_view block, std::size_t &offset, std::uint64_t count, std::size_t max_headers,
                        std::vector<HeaderView> &headers, std::optional<std::uint64_t> &content_length)
{
    if (count > max_headers)
        return false;

    headers.clear();
    headers.reserve(static_cast<std::size_t>(count));

    for (std::uint64_t i = 0; i < count; ++i) {
        if (offset >= block.size())
            return false;
        const auto code = static_cast<std::uint8_t>(block[offset++]);

        std::string_view name;
        if (code == kLiteral) {
            std::uint64_t length = 0;
            if (!read_varint(block, offset, block.size(), length))
                return false;
            if (!read_bytes(block, offset, length, name))
                return false;
            if (!valid_name(name))
                return false;
        } else {
            name = header_name(code);
            if (name.empty())
                return false;
        }

        std::uint64_t length = 0;
        if (!read_varint(block, offset, block.size(), length))
            return false;
        std::string_view value;
        if (!read_bytes(block, offset, length, value))
            return false;
        if (!valid_value(value))
            return false;

        if (iequals(name, "content-length")) {
            std::uint64_t parsed = 0;
            if (!parse_uint(value, parsed))
                return false;
            if (content_length.has_value() && *content_length != parsed)
                return false;
            content_length = parsed;
        }

        headers.emplace_back(name, value);
    }
    return true;
}

template <class HeaderRange, class NameOf, class ValueOf>
bool encode_request_impl(std::string_view method, std::string_view target, const HeaderRange &headers, NameOf name_of,
                         ValueOf value_of, std::optional<std::uint64_t> content_length, std::string &out,
                         bool skip_hop_by_hop)
{
    if (!valid_method(method) || !valid_target(target))
        return false;

    std::size_t count = 0;
    for (const auto &header : headers) {
        const std::string_view name = name_of(header);
        if (skip_hop_by_hop && (hop_by_hop(name) || (!name.empty() && name.front() == ':') ||
                                (content_length.has_value() && iequals(name, "content-length")))) {
            continue;
        }
        if (!valid_name(name) || !valid_value(value_of(header)))
            return false;
        ++count;
    }
    if (count > kMaxHeaders)
        return false;

    out.clear();
    out.reserve(64);
    out.push_back(static_cast<char>(kRequestTag));

    const std::uint8_t method_id = method_code(method);
    out.push_back(static_cast<char>(method_id));
    if (method_id == kLiteral) {
        append_varint(out, method.size());
        out.append(method);
    }

    append_varint(out, target.size());
    out.append(target);
    if (!append_flags(out, content_length))
        return false;

    append_varint(out, count);
    for (const auto &header : headers) {
        const std::string_view name = name_of(header);
        if (skip_hop_by_hop && (hop_by_hop(name) || (!name.empty() && name.front() == ':') ||
                                (content_length.has_value() && iequals(name, "content-length")))) {
            continue;
        }
        if (!append_header(out, name, value_of(header)))
            return false;
    }
    return out.size() <= kMaxBlock;
}

} // namespace

bool encode_request(std::string_view method, std::string_view target, std::span<const HeaderView> headers,
                    std::optional<std::uint64_t> content_length, std::string &out, bool skip_hop_by_hop)
{
    return encode_request_impl(
        method, target, headers, [](const HeaderView &header) { return header.first; },
        [](const HeaderView &header) { return header.second; }, content_length, out, skip_hop_by_hop);
}

bool encode_request(std::string_view method, std::string_view target, std::span<const h2::HeaderField> headers,
                    std::optional<std::uint64_t> content_length, std::string &out, bool skip_hop_by_hop)
{
    return encode_request_impl(
        method, target, headers, [](const h2::HeaderField &header) -> std::string_view { return header.name; },
        [](const h2::HeaderField &header) -> std::string_view { return header.value; }, content_length, out,
        skip_hop_by_hop);
}

bool encode_response(int status, std::span<const HeaderView> headers, std::optional<std::uint64_t> content_length,
                     std::string &out, bool skip_hop_by_hop, bool skip_content_length)
{
    if (status < 100 || status > 599)
        return false;

    std::size_t count = 0;
    for (const HeaderView &header : headers) {
        if (skip_hop_by_hop && hop_by_hop(header.first))
            continue;
        if (skip_content_length && iequals(header.first, "content-length"))
            continue;
        if (!valid_name(header.first) || !valid_value(header.second))
            return false;
        ++count;
    }
    if (count > kMaxHeaders)
        return false;

    out.clear();
    out.reserve(64);
    out.push_back(static_cast<char>(kResponseTag));
    append_varint(out, static_cast<std::uint64_t>(status));
    if (!append_flags(out, content_length))
        return false;

    append_varint(out, count);
    for (const HeaderView &header : headers) {
        if (skip_hop_by_hop && hop_by_hop(header.first))
            continue;
        if (skip_content_length && iequals(header.first, "content-length"))
            continue;
        if (!append_header(out, header.first, header.second))
            return false;
    }
    return out.size() <= kMaxBlock;
}

bool encode_trailers(std::span<const HeaderView> headers, std::string &out)
{
    if (headers.size() > kMaxHeaders)
        return false;

    out.clear();
    out.reserve(64);
    out.push_back(static_cast<char>(kTrailersTag));
    append_varint(out, headers.size());
    for (const HeaderView &header : headers) {
        if (!append_header(out, header.first, header.second))
            return false;
    }
    return out.size() <= kMaxBlock;
}

bool decode_request(std::string_view block, std::vector<HeaderView> &headers, RequestHead &out, std::size_t max_headers,
                    std::size_t max_block)
{
    if (block.empty() || block.size() > max_block)
        return false;

    std::size_t offset = 0;
    if (static_cast<std::uint8_t>(block[offset++]) != kRequestTag)
        return false;

    std::string_view method;
    if (offset >= block.size())
        return false;
    const auto method_id = static_cast<std::uint8_t>(block[offset++]);
    if (method_id == kLiteral) {
        std::uint64_t length = 0;
        if (!read_varint(block, offset, block.size(), length))
            return false;
        if (!read_bytes(block, offset, length, method))
            return false;
    } else {
        if (method_id >= kMethodCount)
            return false;
        method = kMethods[method_id];
    }
    if (!valid_method(method))
        return false;

    std::uint64_t target_length = 0;
    if (!read_varint(block, offset, block.size(), target_length))
        return false;
    std::string_view target;
    if (!read_bytes(block, offset, target_length, target))
        return false;
    if (!valid_target(target))
        return false;

    if (offset >= block.size())
        return false;
    const auto flags = static_cast<std::uint8_t>(block[offset++]);
    if ((flags & ~kFlagContentLength) != 0)
        return false;

    std::optional<std::uint64_t> content_length;
    if ((flags & kFlagContentLength) != 0) {
        std::uint64_t value = 0;
        if (!read_varint(block, offset, std::numeric_limits<std::uint64_t>::max(), value))
            return false;
        content_length = value;
    }

    std::uint64_t count = 0;
    if (!read_varint(block, offset, max_headers, count))
        return false;

    std::optional<std::uint64_t> header_length;
    if (!decode_header_list(block, offset, count, max_headers, headers, header_length))
        return false;
    if (offset != block.size())
        return false;
    if (content_length.has_value() && header_length.has_value() && *content_length != *header_length)
        return false;
    if (!content_length.has_value())
        content_length = header_length;

    out.method = method;
    out.target = target;
    out.content_length = content_length;
    out.headers = { headers.data(), headers.size() };
    return true;
}

bool decode_response(std::string_view block, std::vector<HeaderView> &headers, ResponseHead &out,
                     std::size_t max_headers, std::size_t max_block)
{
    if (block.empty() || block.size() > max_block)
        return false;

    std::size_t offset = 0;
    if (static_cast<std::uint8_t>(block[offset++]) != kResponseTag)
        return false;

    std::uint64_t status = 0;
    if (!read_varint(block, offset, 599, status) || status < 100)
        return false;

    if (offset >= block.size())
        return false;
    const auto flags = static_cast<std::uint8_t>(block[offset++]);
    if ((flags & ~kFlagContentLength) != 0)
        return false;

    std::optional<std::uint64_t> content_length;
    if ((flags & kFlagContentLength) != 0) {
        std::uint64_t value = 0;
        if (!read_varint(block, offset, std::numeric_limits<std::uint64_t>::max(), value))
            return false;
        content_length = value;
    }

    std::uint64_t count = 0;
    if (!read_varint(block, offset, max_headers, count))
        return false;

    std::optional<std::uint64_t> header_length;
    if (!decode_header_list(block, offset, count, max_headers, headers, header_length))
        return false;
    if (offset != block.size())
        return false;
    if (content_length.has_value() && header_length.has_value() && *content_length != *header_length)
        return false;
    if (!content_length.has_value())
        content_length = header_length;

    out.status = static_cast<int>(status);
    out.content_length = content_length;
    out.headers = { headers.data(), headers.size() };
    return true;
}

bool decode_trailers(std::string_view block, std::vector<HeaderView> &headers, std::size_t max_headers,
                     std::size_t max_block)
{
    if (block.empty() || block.size() > max_block)
        return false;

    std::size_t offset = 0;
    if (static_cast<std::uint8_t>(block[offset++]) != kTrailersTag)
        return false;

    std::uint64_t count = 0;
    if (!read_varint(block, offset, max_headers, count))
        return false;

    std::optional<std::uint64_t> content_length;
    if (!decode_header_list(block, offset, count, max_headers, headers, content_length))
        return false;
    return offset == block.size();
}

} // namespace realurl::detail::native
