#include "http1_parser.hpp"

#include "router.hpp"

#include "picohttpparser.h"

#include <algorithm>
#include <cctype>
#include <cstring>

namespace realurl::detail
{
namespace
{

std::string_view trim(std::string_view value) noexcept
{
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
        value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
        value.remove_suffix(1);
    return value;
}

bool header_has_token(std::string_view value, std::string_view token) noexcept
{
    std::size_t start = 0;
    while (start <= value.size()) {
        const auto comma = value.find(',', start);
        const std::string_view part = trim(value.substr(
            start, comma == std::string_view::npos ? value.size() - start : comma - start));
        if (iequals(part, token))
            return true;
        if (comma == std::string_view::npos)
            break;
        start = comma + 1;
    }
    return false;
}

bool parse_transfer_encoding(std::string_view value, bool &chunked) noexcept
{
    bool saw_chunked = false;
    std::size_t start = 0;
    std::string_view last;
    while (start <= value.size()) {
        const auto comma = value.find(',', start);
        const std::string_view part = trim(value.substr(
            start, comma == std::string_view::npos ? value.size() - start : comma - start));
        if (part.empty())
            return false;
        if (!iequals(part, "chunked"))
            return false;
        last = part;
        saw_chunked = true;
        if (comma == std::string_view::npos)
            break;
        start = comma + 1;
    }
    if (!saw_chunked || !iequals(last, "chunked"))
        return false;
    chunked = true;
    return true;
}

} // namespace

HeadParseResult parse_request_head(std::string_view buffer, std::size_t last_len,
                                   std::size_t header_limit, bool decode_path, ParsedHead &out)
{
    out = ParsedHead{};

    if (header_limit == 0)
        header_limit = 1 << 20;

    const char *method = nullptr;
    std::size_t method_length = 0;
    const char *path = nullptr;
    std::size_t path_length = 0;
    int minor_version = 0;
    phr_header headers[kMaxH1Headers];
    std::size_t header_count = kMaxH1Headers;

    const int parsed = phr_parse_request(buffer.data(), buffer.size(), &method, &method_length, &path,
                                         &path_length, &minor_version, headers, &header_count, last_len);
    if (parsed == -2) {
        if (buffer.size() > header_limit) {
            out.error_status = 431;
            return HeadParseResult::Error;
        }
        return HeadParseResult::NeedMore;
    }
    if (parsed < 0) {
        out.error_status = header_count >= kMaxH1Headers ? 431 : 400;
        return HeadParseResult::Error;
    }
    if (static_cast<std::size_t>(parsed) > header_limit) {
        out.error_status = 431;
        return HeadParseResult::Error;
    }

    out.method = std::string_view{ method, method_length };
    out.target = std::string_view{ path, path_length };
    out.version = minor_version == 1 ? "HTTP/1.1" : "HTTP/1.0";
    out.consumed = static_cast<std::size_t>(parsed);
    out.keep_alive = minor_version == 1;

    if (out.method.empty() || out.target.empty()) {
        out.error_status = 400;
        return HeadParseResult::Error;
    }
    if (minor_version != 0 && minor_version != 1) {
        out.error_status = 400;
        return HeadParseResult::Error;
    }

    const std::string_view raw_path = target_path(out.target);
    if (raw_path.empty() || (raw_path.front() != '/' && !(out.method == "OPTIONS" && out.target == "*"))) {
        out.error_status = 400;
        return HeadParseResult::Error;
    }

    if (decode_path && raw_path.find('%') != std::string_view::npos) {
        out.path_storage = percent_decode(raw_path);
        out.path = out.path_storage;
    } else {
        out.path = raw_path;
    }

    out.header_count = header_count;
    for (std::size_t i = 0; i < header_count; ++i) {
        if (headers[i].name == nullptr) {
            // obs-fold is rejected; request smuggling stays unambiguous.
            out.error_status = 400;
            return HeadParseResult::Error;
        }
        out.headers[i] = Header{ std::string_view{ headers[i].name, headers[i].name_len },
                                 std::string_view{ headers[i].value, headers[i].value_len } };
    }

    bool have_content_length = false;
    bool have_transfer_encoding = false;
    bool chunked = false;

    for (std::size_t i = 0; i < out.header_count; ++i) {
        const Header &header = out.headers[i];
        if (iequals(header.first, "Content-Length")) {
            const long long value = parse_content_length(trim(header.second));
            if (value < 0) {
                out.error_status = 400;
                return HeadParseResult::Error;
            }
            if (have_content_length && value != out.content_length) {
                out.error_status = 400;
                return HeadParseResult::Error;
            }
            out.content_length = value;
            have_content_length = true;
        } else if (iequals(header.first, "Transfer-Encoding")) {
            if (!parse_transfer_encoding(header.second, chunked)) {
                out.error_status = 400;
                return HeadParseResult::Error;
            }
            have_transfer_encoding = true;
        } else if (iequals(header.first, "Connection")) {
            if (header_has_token(header.second, "close"))
                out.keep_alive = false;
            if (header_has_token(header.second, "keep-alive") && minor_version == 0)
                out.keep_alive = true;
        } else if (iequals(header.first, "Expect")) {
            if (iequals(trim(header.second), "100-continue"))
                out.expect_continue = true;
        }
    }

    if (have_content_length && have_transfer_encoding) {
        out.error_status = 400;
        return HeadParseResult::Error;
    }

    out.chunked = chunked;
    if (chunked)
        out.content_length = -1;
    out.head_only = out.method == "HEAD";

    return HeadParseResult::Ok;
}

} // namespace realurl::detail
