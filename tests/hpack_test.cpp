#include "realurl/detail/hpack.hpp"
#include "test_harness.hpp"

#include <cstddef>
#include <cstdio>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>

using realurl::detail::h2::HeaderField;
using realurl::detail::h2::HeaderList;
using realurl::detail::h2::HpackDecoder;
using realurl::detail::h2::HpackEncoder;

namespace
{

using Expected = std::initializer_list<std::pair<std::string_view, std::string_view>>;

bool matches(const HeaderList &headers, Expected expected)
{
    if (headers.size() != expected.size())
        return false;

    std::size_t index = 0;
    for (const auto &entry : expected) {
        if (headers[index].name != entry.first || headers[index].value != entry.second)
            return false;
        ++index;
    }
    return true;
}

void dump(const HeaderList &headers)
{
    for (const HeaderField &header : headers)
        std::fprintf(stderr, "  %s: %s\n", header.name.c_str(), header.value.c_str());
}

void check(const HeaderList &headers, Expected expected, const char *label)
{
    if (!matches(headers, expected)) {
        std::fprintf(stderr, "%s mismatch:\n", label);
        dump(headers);
    }
    CHECK(matches(headers, expected));
}

} // namespace

int main()
{
    {
        HpackDecoder decoder;
        HeaderList headers;

        CHECK(decoder.decode("\x82\x86\x84\x41\x0f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6f\x6d",
                             headers));
        check(headers,
              { { ":method", "GET" }, { ":scheme", "http" }, { ":path", "/" }, { ":authority", "www.example.com" } },
              "C.3.1");

        headers.clear();
        CHECK(decoder.decode("\x82\x86\x84\xbe\x58\x08\x6e\x6f\x2d\x63\x61\x63\x68\x65", headers));
        check(headers,
              { { ":method", "GET" },
                { ":scheme", "http" },
                { ":path", "/" },
                { ":authority", "www.example.com" },
                { "cache-control", "no-cache" } },
              "C.3.2");

        headers.clear();
        CHECK(decoder.decode("\x82\x87\x85\xbf\x40\x0a\x63\x75\x73\x74\x6f\x6d\x2d\x6b\x65\x79\x0c\x63\x75\x73\x74"
                             "\x6f\x6d\x2d\x76\x61\x6c\x75\x65",
                             headers));
        check(headers,
              { { ":method", "GET" },
                { ":scheme", "https" },
                { ":path", "/index.html" },
                { ":authority", "www.example.com" },
                { "custom-key", "custom-value" } },
              "C.3.3");
    }

    {
        HpackDecoder decoder;
        HeaderList headers;

        CHECK(decoder.decode(
            "\x82\x86\x84\x41\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff", headers));
        check(headers,
              { { ":method", "GET" }, { ":scheme", "http" }, { ":path", "/" }, { ":authority", "www.example.com" } },
              "C.4.1");

        headers.clear();
        CHECK(decoder.decode("\x82\x86\x84\xbe\x58\x86\xa8\xeb\x10\x64\x9c\xbf", headers));
        check(headers,
              { { ":method", "GET" },
                { ":scheme", "http" },
                { ":path", "/" },
                { ":authority", "www.example.com" },
                { "cache-control", "no-cache" } },
              "C.4.2");

        headers.clear();
        CHECK(decoder.decode("\x82\x87\x85\xbf\x40\x88\x25\xa8\x49\xe9\x5b\xa9\x7d\x7f\x89\x25\xa8\x49\xe9\x5b\xb8\xe8"
                             "\xb4\xbf",
                             headers));
        check(headers,
              { { ":method", "GET" },
                { ":scheme", "https" },
                { ":path", "/index.html" },
                { ":authority", "www.example.com" },
                { "custom-key", "custom-value" } },
              "C.4.3");
    }

    {
        HpackDecoder decoder;
        HeaderList headers;

        CHECK(decoder.decode(
            "\x48\x82\x64\x02\x58\x85\xae\xc3\x77\x1a\x4b\x61\x96\xd0\x7a\xbe\x94\x10\x54\xd4\x44\xa8\x20\x05\x95"
            "\x04\x0b\x81\x66\xe0\x82\xa6\x2d\x1b\xff\x6e\x91\x9d\x29\xad\x17\x18\x63\xc7\x8f\x0b\x97\xc8\xe9\xae"
            "\x82\xae\x43\xd3",
            headers));
        check(headers,
              { { ":status", "302" },
                { "cache-control", "private" },
                { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
                { "location", "https://www.example.com" } },
              "C.6.1");

        headers.clear();
        CHECK(decoder.decode("\x48\x83\x64\x0e\xff\xc1\xc0\xbf", headers));
        check(headers,
              { { ":status", "307" },
                { "cache-control", "private" },
                { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
                { "location", "https://www.example.com" } },
              "C.6.2");

        headers.clear();
        CHECK(decoder.decode(
            "\x88\xc1\x61\x96\xd0\x7a\xbe\x94\x10\x54\xd4\x44\xa8\x20\x05\x95\x04\x0b\x81\x66\xe0\x84\xa6\x2d\x1b"
            "\xff\xc0\x5a\x83\x9b\xd9\xab\x77\xad\x94\xe7\x82\x1d\xd7\xf2\xe6\xc7\xb3\x35\xdf\xdf\xcd\x5b\x39\x60"
            "\xd5\xaf\x27\x08\x7f\x36\x72\xc1\xab\x27\x0f\xb5\x29\x1f\x95\x87\x31\x60\x65\xc0\x03\xed\x4e\xe5\xb1"
            "\x06\x3d\x50\x07",
            headers));
        check(headers,
              { { ":status", "200" },
                { "cache-control", "private" },
                { "date", "Mon, 21 Oct 2013 20:13:22 GMT" },
                { "location", "https://www.example.com" },
                { "content-encoding", "gzip" },
                { "set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1" } },
              "C.6.3");
    }

    {
        HeaderList input = {
            { ":method", "POST" },
            { ":scheme", "https" },
            { ":path", "/upload" },
            { ":authority", "example.com" },
            { "content-type", "application/octet-stream" },
            { "authorization", "Bearer token" },
            { "x-custom", "value with spaces" },
        };

        HpackEncoder encoder;
        std::string block;
        encoder.encode(input, block);

        HpackDecoder decoder;
        HeaderList output;
        CHECK(decoder.decode(block, output));
        CHECK(matches(output, { { ":method", "POST" },
                                { ":scheme", "https" },
                                { ":path", "/upload" },
                                { ":authority", "example.com" },
                                { "content-type", "application/octet-stream" },
                                { "authorization", "Bearer token" },
                                { "x-custom", "value with spaces" } }));
    }

    {
        HpackDecoder decoder;
        HeaderList headers;
        CHECK(!decoder.decode("\xff\xff\xff\xff", headers)); // invalid index
        headers.clear();
        CHECK(!decoder.decode("\x82\x86\x84\x41\x0f\x77", headers)); // truncated string
    }

    return test::summary();
}
