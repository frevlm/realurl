#pragma once

#include "realurl/server_types.hpp"

#include <array>
#include <cstddef>
#include <string>
#include <string_view>

namespace realurl::detail
{

using Header = server::Header;

enum class HeadParseResult { Ok, NeedMore, Error };

inline constexpr std::size_t kMaxH1Headers = 128;

struct ParsedHead
{
    std::string_view method;
    std::string_view target;
    std::string_view path;
    std::string_view version;
    std::string path_storage;

    std::array<Header, kMaxH1Headers> headers{};
    std::size_t header_count = 0;

    std::size_t consumed = 0;
    long long content_length = -1;
    bool chunked = false;
    bool keep_alive = true;
    bool expect_continue = false;
    bool head_only = false;

    int error_status = 0;
};

/// Parse one HTTP/1.1 request head. `last_len` is the previous buffer size (0
/// for a fresh buffer) so picohttpparser can short-circuit partial parses.
HeadParseResult parse_request_head(std::string_view buffer, std::size_t last_len,
                                   std::size_t header_limit, bool decode_path, ParsedHead &out);

} // namespace realurl::detail
