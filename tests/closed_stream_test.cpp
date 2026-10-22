#include "realurl/detail/frame.hpp"
#include "realurl/detail/hpack.hpp"
#include "realurl/detail/native_codec.hpp"
#include "realurl/detail/protocol.hpp"
#include "realurl/server.hpp"

#include "support/raw_connection.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <map>
#include <netinet/in.h>
#include <optional>
#include <span>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
namespace h2 = realurl::detail::h2;
namespace native = realurl::detail::native;
using test::RawConnection;
using test::RawFrame;

namespace
{

int free_port()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    int port = -1;
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0 &&
        ::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) == 0)
        port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

/// Response body of /big; fits the default connection window.
constexpr std::size_t kBigBody = 32 * 1024;

/// A block no native decoder accepts: the tag byte is undefined.
const std::string kBadNativeBlock = "\xee";

using Resets = std::map<std::uint32_t, std::vector<std::uint32_t>>;

std::uint32_t code(h2::ErrorCode error)
{
    return static_cast<std::uint32_t>(error);
}

std::string headers_frame(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    std::string out;
    h2::append_headers(out, stream_id, block, end_stream, h2::kDefaultMaxFrameSize);
    return out;
}

std::string native_request(std::uint32_t stream_id, std::string_view method, std::string_view target, bool end_stream)
{
    std::string block;
    CHECK(native::encode_request(method, target, std::span<const native::HeaderView>(), std::nullopt, block));
    return headers_frame(stream_id, block, end_stream);
}

/// Response of one stream plus every RST_STREAM seen before it ended.
struct Outcome {
    Resets resets;
    int status = 0;
    std::string body;
};

/// Read frames until `stream_id` ends. `status_of` decodes a HEADERS payload.
template <typename StatusOf>
Outcome read_until_end(RawConnection &connection, std::uint32_t stream_id, StatusOf status_of)
{
    Outcome outcome;
    RawFrame frame;
    while (connection.next(frame)) {
        const std::uint32_t id = frame.header.stream_id;
        if (frame.header.type == h2::FrameType::RstStream)
            outcome.resets[id].push_back(test::read_be32(frame.payload));
        if (frame.header.type == h2::FrameType::Headers && id == stream_id)
            outcome.status = status_of(frame.payload);
        if (frame.header.type == h2::FrameType::Data && id == stream_id)
            outcome.body += frame.payload;
        if (test::end_of(frame, stream_id))
            break;
    }
    return outcome;
}

int native_status(std::string_view payload)
{
    std::vector<native::HeaderView> views;
    native::ResponseHead head;
    return native::decode_response(payload, views, head) ? head.status : -1;
}

Outcome native_hello(int port, std::uint32_t stream_id, std::string prefix)
{
    RawConnection connection(test::connect_loopback(port));
    h2::FrameWriter settings;
    settings.settings({});
    std::string request = std::string(realurl::detail::kNativePreface) + settings.buffer() + std::move(prefix);
    request += native_request(stream_id, "GET", "/hello", true);
    CHECK(connection.send(request));
    return read_until_end(connection, stream_id, native_status);
}

void start(realurl::Server &server, std::thread &listener, int port)
{
    server.set_io_threads(1);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });
    server.Post("/echo", [](const auto &request, auto &response) { response.set_content(request.body); });
    server.Get("/big", [](const auto &, auto &response) { response.set_content(std::string(kBigBody, 'b')); });
    listener = std::thread([&server, port] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
}

/// A native stream reset by an undecodable head, or by trailers that do not
/// decode, stays closed: more HEADERS on it get RST_STREAM(STREAM_CLOSED).
/// The connection keeps serving, and so does the server.
void test_native_headers_on_closed_stream()
{
    const int port = free_port();
    CHECK(port > 0);
    realurl::Server server;
    std::thread listener;
    start(server, listener, port);

    // One write, so the server sees every frame before it reaps the closed
    // streams.
    std::string frames = headers_frame(1, kBadNativeBlock, false);
    frames += native_request(1, "GET", "/hello", true);
    frames += native_request(3, "POST", "/echo", false);
    frames += headers_frame(3, kBadNativeBlock, true);
    frames += native_request(3, "GET", "/hello", true);
    const Outcome same = native_hello(port, 5, frames);

    const Resets expected = {
        { 1, { code(h2::ErrorCode::Protocol), code(h2::ErrorCode::StreamClosed) } },
        { 3, { code(h2::ErrorCode::Protocol), code(h2::ErrorCode::StreamClosed) } },
    };
    CHECK(same.resets == expected);
    CHECK(same.status == 200);
    CHECK(same.body == "world");

    const Outcome fresh = native_hello(port, 1, {});
    CHECK(fresh.resets.size() == 0);
    CHECK(fresh.status == 200);
    CHECK(fresh.body == "world");

    server.stop();
    listener.join();
}

std::string h2_request(h2::HpackEncoder &encoder, std::uint32_t stream_id, h2::HeaderList headers)
{
    std::string block;
    encoder.encode(headers, block);
    return headers_frame(stream_id, block, true);
}

h2::HeaderList h2_get(std::string path)
{
    return { { ":method", "GET" }, { ":scheme", "http" }, { ":authority", "127.0.0.1" }, { ":path", std::move(path) } };
}

/// Send `request` on a new h2c connection and read until `stream_id` ends.
template <typename StatusOf>
Outcome h2_exchange(int port, std::string_view request, std::uint32_t stream_id, StatusOf status_of)
{
    RawConnection connection(test::connect_loopback(port));
    CHECK(connection.send(request));
    return read_until_end(connection, stream_id, status_of);
}

/// The HTTP/2 counterpart: a stream reset for a malformed request head stays
/// closed, and its HPACK blocks still decode, so later requests on the same
/// connection and on new ones are served.
void test_h2_headers_on_closed_stream()
{
    const int port = free_port();
    CHECK(port > 0);
    realurl::Server server;
    std::thread listener;
    start(server, listener, port);

    h2::HpackEncoder encoder;
    h2::HpackDecoder decoder;
    const auto status_of = [&decoder](std::string_view payload) {
        h2::HeaderList headers;
        if (!decoder.decode(payload, headers))
            return -1;
        for (const h2::HeaderField &field : headers) {
            if (field.name == ":status")
                return std::stoi(field.value);
        }
        return 0;
    };

    h2::FrameWriter settings;
    settings.settings({});
    std::string request = std::string(realurl::detail::kH2Preface) + settings.buffer();
    request += h2_request(encoder, 1, { { ":scheme", "http" }, { ":path", "/hello" } });
    request += h2_request(encoder, 1, h2_get("/hello"));
    request += h2_request(encoder, 3, h2_get("/hello"));
    const Outcome same = h2_exchange(port, request, 3, status_of);

    const Resets expected = { { 1, { code(h2::ErrorCode::Protocol), code(h2::ErrorCode::StreamClosed) } } };
    CHECK(same.resets == expected);
    CHECK(same.status == 200);
    CHECK(same.body == "world");

    h2::HpackEncoder fresh_encoder;
    std::string fresh_request = std::string(realurl::detail::kH2Preface) + settings.buffer();
    fresh_request += h2_request(fresh_encoder, 1, h2_get("/hello"));
    decoder = h2::HpackDecoder();
    const Outcome fresh = h2_exchange(port, fresh_request, 1, status_of);
    CHECK(fresh.resets.size() == 0);
    CHECK(fresh.status == 200);
    CHECK(fresh.body == "world");

    server.stop();
    listener.join();
}

/// A new server stream starts its send window at the client's
/// SETTINGS_INITIAL_WINDOW_SIZE: the first DATA frame is exactly that size.
void test_stream_initial_window()
{
    const int port = free_port();
    CHECK(port > 0);
    realurl::Server server;
    std::thread listener;
    start(server, listener, port);

    constexpr std::uint32_t kInitialWindow = 1000;
    const h2::Setting window[] = { { h2::kSettingInitialWindowSize, kInitialWindow } };
    h2::FrameWriter settings;
    settings.settings(window);
    RawConnection connection(test::connect_loopback(port));
    std::string request = std::string(realurl::detail::kNativePreface) + settings.buffer();
    request += native_request(1, "GET", "/big", true);
    CHECK(connection.send(request));

    std::size_t first_frame = 0;
    std::string body;
    RawFrame frame;
    while (connection.next(frame)) {
        if (frame.header.type == h2::FrameType::Data && frame.header.stream_id == 1) {
            body += frame.payload;
            if (first_frame == 0) {
                first_frame = frame.payload.size();
                h2::FrameWriter update;
                update.window_update(1, kBigBody);
                CHECK(connection.send(update.buffer()));
            }
        }
        if (test::end_of(frame, 1))
            break;
    }

    CHECK(first_frame == kInitialWindow);
    CHECK(body == std::string(kBigBody, 'b'));

    server.stop();
    listener.join();
}

} // namespace

int main()
{
    test_native_headers_on_closed_stream();
    test_h2_headers_on_closed_stream();
    test_stream_initial_window();
    return test::summary();
}
