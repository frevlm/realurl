#pragma once

/// Compact request/response head codec for the realurl native transport.
///
/// A native HEADERS payload carries one tag byte followed by varints and raw
/// bytes. Header names use a static table; values are raw. Decoded views point
/// into the input block, which must outlive them.

#include "realurl/detail/http2_types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace realurl::detail::native
{

using HeaderView = std::pair<std::string_view, std::string_view>;

inline constexpr std::size_t kMaxHeaders = 256;
inline constexpr std::size_t kMaxBlock = 1 << 20;

struct RequestHead {
    std::string_view method;
    std::string_view target;
    std::optional<std::uint64_t> content_length;
    std::span<const HeaderView> headers;
};

struct ResponseHead {
    int status = 0;
    std::optional<std::uint64_t> content_length;
    std::span<const HeaderView> headers;
};

/// Encode a request head. Returns false on invalid method/target/headers or
/// when the encoded block exceeds kMaxBlock. `out` is cleared on entry.
/// skip_hop_by_hop also drops pseudo-headers and, when content_length is
/// present, a content-length header.
bool encode_request(std::string_view method, std::string_view target, std::span<const HeaderView> headers,
                    std::optional<std::uint64_t> content_length, std::string &out, bool skip_hop_by_hop = false);

bool encode_request(std::string_view method, std::string_view target, std::span<const h2::HeaderField> headers,
                    std::optional<std::uint64_t> content_length, std::string &out, bool skip_hop_by_hop = false);

/// skip_content_length drops a content-length header; the value should be
/// carried by the content_length field instead.
bool encode_response(int status, std::span<const HeaderView> headers, std::optional<std::uint64_t> content_length,
                     std::string &out, bool skip_hop_by_hop = false, bool skip_content_length = false);

bool encode_trailers(std::span<const HeaderView> headers, std::string &out);

/// Decode a request head into views over `block`. `headers` is cleared and
/// filled; `out.headers` points into it.
bool decode_request(std::string_view block, std::vector<HeaderView> &headers, RequestHead &out,
                    std::size_t max_headers = kMaxHeaders, std::size_t max_block = kMaxBlock);

bool decode_response(std::string_view block, std::vector<HeaderView> &headers, ResponseHead &out,
                     std::size_t max_headers = kMaxHeaders, std::size_t max_block = kMaxBlock);

bool decode_trailers(std::string_view block, std::vector<HeaderView> &headers, std::size_t max_headers = kMaxHeaders,
                     std::size_t max_block = kMaxBlock);

} // namespace realurl::detail::native
