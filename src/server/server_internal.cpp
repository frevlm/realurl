#include "server_internal.hpp"

#include <ctime>
#include <string>

namespace realurl::detail
{
namespace
{

void append_header(std::string &out, std::string_view name, std::string_view value)
{
    out.append(name);
    out.append(": ");
    out.append(value);
    out.append("\r\n");
}

bool is_skipped_header(std::string_view name) noexcept
{
    return iequals(name, "Content-Length") || iequals(name, "Connection") ||
           iequals(name, "Transfer-Encoding");
}

} // namespace

bool status_allows_body(int status) noexcept
{
    return status >= 200 && status != 204 && status != 304;
}

void ServerContext::apply_error_body(Response &response, int status) const
{
    const auto it = error_bodies.find(status);
    if (it == error_bodies.end())
        return;
    response.content_type = it->second.first;
    response.body = it->second.second;
    response.body_view = {};
}

std::string ServerContext::error_response(int status) const
{
    Response response;
    response.status = status;
    apply_error_body(response, status);
    std::string out;
    serialize_fixed_response(response, false, false, out);
    return out;
}

void serialize_fixed_response(const Response &response, bool head_only, bool keep_alive,
                              std::string &out)
{
    out.clear();
    out.reserve(256 + response.body.size());
    out.append("HTTP/1.1 ");
    out.append(std::to_string(response.status));
    out.push_back(' ');
    out.append(reason_phrase(response.status));
    out.append("\r\n");

    for (const auto &header : response.headers) {
        if (is_skipped_header(header.first))
            continue;
        append_header(out, header.first, header.second);
    }
    if (!response.content_type.empty())
        append_header(out, "Content-Type", response.content_type);

    append_header(out, "Date", current_date());

    const bool body_allowed = status_allows_body(response.status);
    if (body_allowed)
        append_header(out, "Content-Length", std::to_string(response.body_size()));

    append_header(out, "Connection", keep_alive ? "keep-alive" : "close");
    out.append("\r\n");

    if (!head_only && body_allowed) {
        if (!response.body_view.empty())
            out.append(response.body_view);
        else
            out.append(response.body);
    }
}

std::string_view current_date() noexcept
{
    thread_local char buffer[40] = {};
    thread_local std::time_t cached_at = 0;
    thread_local bool initialized = false;

    const std::time_t now = std::time(nullptr);
    if (!initialized || now != cached_at) {
        std::tm tm{};
        if (::gmtime_r(&now, &tm) != nullptr)
            std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S GMT", &tm);
        cached_at = now;
        initialized = true;
    }
    return std::string_view{ buffer };
}

std::shared_ptr<BodySource> make_memory_source(std::shared_ptr<std::string> data)
{
    auto offset = std::make_shared<std::size_t>(0);
    auto source = std::make_shared<BodySource>();
    source->read = [data, offset]() -> BodyReadResult {
        if (*offset >= data->size())
            return { {}, true, false };
        return { std::string_view{ *data }.substr(*offset), false, false };
    };
    source->consume = [offset](std::size_t count) { *offset += count; };
    source->peek = [data]() { return data->substr(0, 65536); };
    source->wake = [] {};
    return source;
}

} // namespace realurl::detail
