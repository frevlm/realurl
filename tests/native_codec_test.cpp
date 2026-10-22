#include "realurl/detail/native_codec.hpp"

#include "test_harness.hpp"

#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace realurl::detail::native;

namespace
{

std::vector<HeaderView> headers_of(const std::vector<std::pair<std::string, std::string>> &source)
{
    std::vector<HeaderView> result;
    result.reserve(source.size());
    for (const auto &entry : source)
        result.emplace_back(entry.first, entry.second);
    return result;
}

void check_request(std::string_view method, std::string_view target,
                   const std::vector<std::pair<std::string, std::string>> &source,
                   std::optional<std::uint64_t> content_length)
{
    const std::vector<HeaderView> views = headers_of(source);
    std::string block;
    CHECK(encode_request(method, target, views, content_length, block));
    CHECK(!block.empty());

    std::vector<HeaderView> decoded;
    RequestHead head;
    CHECK(decode_request(block, decoded, head));
    CHECK(head.method == method);
    CHECK(head.target == target);
    CHECK(head.content_length == content_length);
    CHECK(head.headers.size() == source.size());
    for (std::size_t i = 0; i < source.size(); ++i) {
        CHECK(head.headers[i].first == source[i].first);
        CHECK(head.headers[i].second == source[i].second);
    }
}

void test_requests()
{
    const char *methods[] = { "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE" };
    for (const char *method : methods)
        check_request(method, "/", {}, std::nullopt);

    check_request("PROPFIND", "/dav/file", {}, std::nullopt);
    check_request("POST", "/v1/chat?stream=1",
                  { { "content-type", "application/json" },
                    { "authorization", "Bearer abcdefghijklmnopqrstuvwxyz" },
                    { "x-custom-header", "custom" },
                    { "x-empty", "" } },
                  2 * 1024 * 1024);
    check_request("GET", "/zero", { { "accept", "*/*" } }, 0);

    std::vector<std::pair<std::string, std::string>> many;
    for (int i = 0; i < 64; ++i)
        many.emplace_back(i % 2 == 0 ? "x-request-id" : "x-custom", std::to_string(i));
    check_request("GET", "/many", many, std::nullopt);
}

void check_response(int status, const std::vector<std::pair<std::string, std::string>> &source,
                    std::optional<std::uint64_t> content_length)
{
    const std::vector<HeaderView> views = headers_of(source);
    std::string block;
    CHECK(encode_response(status, views, content_length, block));
    CHECK(!block.empty());

    std::vector<HeaderView> decoded;
    ResponseHead head;
    CHECK(decode_response(block, decoded, head));
    CHECK(head.status == status);
    CHECK(head.content_length == content_length);
    CHECK(head.headers.size() == source.size());
    for (std::size_t i = 0; i < source.size(); ++i) {
        CHECK(head.headers[i].first == source[i].first);
        CHECK(head.headers[i].second == source[i].second);
    }
}

void test_responses()
{
    const int statuses[] = { 100, 200, 204, 304, 404, 500, 599 };
    for (int status : statuses)
        check_response(status, {}, std::nullopt);

    check_response(200, { { "content-type", "text/plain" }, { "x-request-id", "abc" } }, 5);
    check_response(404, { { "content-type", "application/json" } }, 0);
    check_response(200, { { "content-length", "42" } }, 42);
}

void test_trailers()
{
    const std::vector<HeaderView> views = { { "x-trace", "done" }, { "x-checksum", "123" } };
    std::string block;
    CHECK(encode_trailers(views, block));

    std::vector<HeaderView> decoded;
    CHECK(decode_trailers(block, decoded));
    CHECK(decoded.size() == views.size());
    for (std::size_t i = 0; i < views.size(); ++i) {
        CHECK(decoded[i].first == views[i].first);
        CHECK(decoded[i].second == views[i].second);
    }
}

void test_limits()
{
    std::vector<std::pair<std::string, std::string>> many;
    for (int i = 0; i < static_cast<int>(kMaxHeaders) + 1; ++i)
        many.emplace_back("x-custom", std::to_string(i));

    std::string block;
    CHECK(!encode_request("GET", "/", headers_of(many), std::nullopt, block));

    const std::string near_value(kMaxBlock - 64, 'v');
    const std::vector<HeaderView> near = { { "x-custom", near_value } };
    CHECK(encode_request("POST", "/", near, std::nullopt, block));

    const std::string over_value(kMaxBlock, 'v');
    const std::vector<HeaderView> over = { { "x-custom", over_value } };
    CHECK(!encode_request("POST", "/", over, std::nullopt, block));

    std::vector<HeaderView> two = { { "accept", "*/*" }, { "host", "example.com" } };
    CHECK(encode_request("GET", "/", two, std::nullopt, block));
    std::vector<HeaderView> decoded;
    RequestHead head;
    CHECK(!decode_request(block, decoded, head, 1));
}

void test_filtering()
{
    std::string block;
    std::vector<HeaderView> headers = {
        { ":authority", "example.com" },
        { "connection", "keep-alive" },
        { "transfer-encoding", "chunked" },
        { "x-ok", "1" },
    };
    CHECK(encode_request("GET", "/", headers, std::nullopt, block, true));

    std::vector<HeaderView> decoded;
    RequestHead request;
    CHECK(decode_request(block, decoded, request));
    CHECK(request.headers.size() == 1);
    CHECK(request.headers[0].first == "x-ok");
    CHECK(request.headers[0].second == "1");

    std::vector<HeaderView> request_with_length = {
        { "content-length", "7" },
        { "x-ok", "2" },
    };
    CHECK(encode_request("POST", "/", request_with_length, 7, block, true));
    CHECK(decode_request(block, decoded, request));
    CHECK(request.content_length == 7);
    CHECK(request.headers.size() == 1);
    CHECK(request.headers[0].first == "x-ok");

    std::vector<HeaderView> response_headers = {
        { "content-length", "7" },
        { "connection", "close" },
        { "content-type", "text/plain" },
    };
    CHECK(encode_response(200, response_headers, 7, block, true, true));
    ResponseHead response;
    CHECK(decode_response(block, decoded, response));
    CHECK(response.status == 200);
    CHECK(response.content_length == 7);
    CHECK(response.headers.size() == 1);
    CHECK(response.headers[0].first == "content-type");
}

void test_invalid_input()
{
    std::vector<HeaderView> decoded;
    RequestHead request;
    ResponseHead response;
    std::string block;

    CHECK(!decode_request("", decoded, request));
    CHECK(!decode_response("", decoded, response));
    CHECK(!decode_trailers("", decoded));

    CHECK(!encode_request("", "/", std::span<const HeaderView>{}, std::nullopt, block));
    CHECK(!encode_request("GE T", "/", std::span<const HeaderView>{}, std::nullopt, block));
    CHECK(!encode_request("GET", "relative", std::span<const HeaderView>{}, std::nullopt, block));
    CHECK(!encode_request("GET", "/bad\r\ntarget", std::span<const HeaderView>{}, std::nullopt, block));
    CHECK(!encode_response(99, std::span<const HeaderView>{}, std::nullopt, block));
    CHECK(!encode_response(600, std::span<const HeaderView>{}, std::nullopt, block));

    std::vector<HeaderView> bad_name = { { ":authority", "x" } };
    CHECK(!encode_request("GET", "/", bad_name, std::nullopt, block));
    std::vector<HeaderView> bad_value = { { "x", "a\nb" } };
    CHECK(!encode_request("GET", "/", bad_value, std::nullopt, block));
    CHECK(!encode_response(200, bad_value, std::nullopt, block));
}

void test_truncated()
{
    std::vector<HeaderView> headers = { { "content-type", "text/plain" }, { "x-request-id", "abc" } };
    std::string block;
    CHECK(encode_request("POST", "/v1/chat", headers, 123, block));

    std::vector<HeaderView> decoded;
    RequestHead request;
    for (std::size_t length = 0; length < block.size(); ++length)
        CHECK(!decode_request(std::string_view{ block }.substr(0, length), decoded, request));

    std::string response_block;
    CHECK(encode_response(200, headers, 123, response_block));
    ResponseHead response;
    for (std::size_t length = 0; length < response_block.size(); ++length)
        CHECK(!decode_response(std::string_view{ response_block }.substr(0, length), decoded, response));
}

void test_corrupted_fields()
{
    std::vector<HeaderView> headers = { { "content-type", "text/plain" } };
    std::string block;
    CHECK(encode_request("GET", "/x", headers, std::nullopt, block));

    // Request layout: tag, method code, target len, target, flags, header count...
    std::string bad = block;
    bad[0] = 0x02;
    std::vector<HeaderView> decoded;
    RequestHead request;
    CHECK(!decode_request(bad, decoded, request));

    bad = block;
    const std::size_t flags_offset = 1 + 1 + 1 + 2; // tag, GET, target len, "/x"
    CHECK(block.size() > flags_offset);
    bad[flags_offset] = static_cast<char>(0x80);
    CHECK(!decode_request(bad, decoded, request));

    // Content-length header conflicts with the explicit field.
    std::vector<HeaderView> conflicting = { { "content-length", "7" } };
    CHECK(encode_request("POST", "/x", conflicting, 8, block));
    CHECK(!decode_request(block, decoded, request));

    // Trailing garbage after a valid response.
    std::string response_block;
    CHECK(encode_response(200, {}, std::nullopt, response_block));
    response_block.push_back(0x7f);
    ResponseHead response;
    CHECK(!decode_response(response_block, decoded, response));

    // Varint with too many continuation bytes.
    std::string varint_bomb;
    varint_bomb.push_back(0x01); // request tag
    varint_bomb.push_back(static_cast<char>(0xff)); // literal method
    for (int i = 0; i < 12; ++i)
        varint_bomb.push_back(static_cast<char>(0x80));
    CHECK(!decode_request(varint_bomb, decoded, request));
}

void test_fuzz()
{
    std::mt19937 rng(0x5eed1234);
    std::uniform_int_distribution<int> length_dist(0, 96);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    std::vector<HeaderView> decoded;
    RequestHead request;
    ResponseHead response;
    for (int i = 0; i < 100000; ++i) {
        const int length = length_dist(rng);
        std::string block;
        block.reserve(static_cast<std::size_t>(length));
        for (int j = 0; j < length; ++j)
            block.push_back(static_cast<char>(byte_dist(rng)));
        (void)decode_request(block, decoded, request);
        (void)decode_response(block, decoded, response);
        (void)decode_trailers(block, decoded);
    }
}

} // namespace

int main()
{
    test_requests();
    test_responses();
    test_trailers();
    test_limits();
    test_filtering();
    test_invalid_input();
    test_truncated();
    test_corrupted_fields();
    test_fuzz();
    return test::summary();
}
