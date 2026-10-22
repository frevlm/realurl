#include "realurl/proxy.hpp"

#include <optional>
#include <string>
#include <vector>

namespace realurl::server
{
namespace
{

bool hop_by_hop(std::string_view name) noexcept
{
    return iequals(name, "connection") || iequals(name, "keep-alive") ||
           iequals(name, "proxy-connection") || iequals(name, "transfer-encoding") ||
           iequals(name, "upgrade") || iequals(name, "te") || iequals(name, "trailer");
}

std::optional<std::uint64_t> parse_length(std::string_view text) noexcept
{
    if (text.empty())
        return std::nullopt;
    std::uint64_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9')
            return std::nullopt;
        if (value > (UINT64_MAX - static_cast<std::uint64_t>(c - '0')) / 10)
            return std::nullopt;
        value = value * 10 + static_cast<std::uint64_t>(c - '0');
    }
    return value;
}

Task<void> bad_gateway(StreamWriter &downstream)
{
    static constexpr std::string_view body = "Bad Gateway";
    try {
        co_await downstream.write_head(502, {}, body.size());
        co_await downstream.write(body);
        co_await downstream.end();
    } catch (...) {
        downstream.cancel();
    }
}

} // namespace

Task<void> proxy_stream(Client &client, std::string url, const Request &request,
                        StreamWriter &downstream, ProxyOptions options)
{
    Headers headers;
    headers.reserve(request.headers.size() + 1);

    std::string content_length_storage;
    for (const Header &header : request.headers) {
        if (hop_by_hop(header.first) || iequals(header.first, "host") ||
            iequals(header.first, "content-length"))
            continue;
        headers.push_back(realurl::detail::h2::HeaderField{ std::string(header.first), std::string(header.second) });
    }

    if (request.body_mode == BodyMode::Buffered) {
        content_length_storage = std::to_string(request.body.size());
        headers.push_back(realurl::detail::h2::HeaderField{ "content-length", content_length_storage });
    } else if (request.body_mode == BodyMode::StreamedNative && request.content_length >= 0) {
        content_length_storage = std::to_string(request.content_length);
        headers.push_back(realurl::detail::h2::HeaderField{ "content-length", content_length_storage });
    }

    Stream upstream;
    bool failed = false;
    bool head_sent = false;

    try {
        upstream = co_await client.open(std::string(request.method), std::move(url), std::move(headers),
                                        options.connect_timeout);

        if (request.body_mode == BodyMode::Buffered && !request.body.empty()) {
            co_await upstream.write(std::string(request.body), options.write_timeout);
        } else if (request.body_mode == BodyMode::StreamedNative && request.body_reader != nullptr) {
            char buffer[16384];
            for (;;) {
                const std::size_t n = co_await request.read_body({ buffer, sizeof(buffer) });
                if (n == 0)
                    break;
                co_await upstream.write(std::string(buffer, n), options.write_timeout);
            }
        }

        co_await upstream.end(options.write_timeout);

        ResponseHead head = co_await upstream.head(options.header_timeout);

        std::vector<Header> response_headers;
        response_headers.reserve(head.headers.size());
        std::optional<std::uint64_t> content_length;
        for (const auto &field : head.headers) {
            if (iequals(field.name, "content-length")) {
                content_length = parse_length(field.value);
                continue;
            }
            if (hop_by_hop(field.name))
                continue;
            response_headers.emplace_back(field.name, field.value);
        }

        co_await downstream.write_head(head.status, response_headers, content_length);
        head_sent = true;

        char buffer[16384];
        for (;;) {
            const std::size_t n = co_await upstream.read({ buffer, sizeof(buffer) }, options.read_idle);
            if (n == 0)
                break;
            if (!co_await downstream.write({ buffer, n })) {
                upstream.cancel();
                co_return;
            }
        }

        co_await downstream.end();
    } catch (...) {
        failed = true;
    }

    if (failed) {
        upstream.cancel();
        if (head_sent)
            downstream.cancel();
        else
            co_await bad_gateway(downstream);
    }
}

} // namespace realurl::server
