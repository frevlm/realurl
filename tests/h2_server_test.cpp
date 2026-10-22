#include "realurl/detail/frame.hpp"
#include "realurl/detail/hpack.hpp"
#include "realurl/detail/protocol.hpp"
#include "realurl/http2.hpp"
#include "realurl/server.hpp"

#include "test_harness.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <netinet/in.h>
#include <map>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

namespace
{


struct StreamFetch
{
    realurl::EventLoop *loop = nullptr;
    realurl::Client *client = nullptr;
    std::string url;
    bool *ok = nullptr;
};

realurl::Task<void> fetch_stream(StreamFetch fetch)
{
    try {
        realurl::Stream stream = co_await fetch.client->open("GET", fetch.url);
        co_await stream.end(5s);
        const realurl::ResponseHead head = co_await stream.head(5s);
        std::string body;
        char buffer[32];
        for (;;) {
            const std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, 5s);
            if (n == 0)
                break;
            body.append(buffer, n);
        }
        *fetch.ok = head.status == 200 && head.protocol == realurl::Protocol::Http2 &&
                    body == "onetwo";
    } catch (...) {
        *fetch.ok = false;
    }
    fetch.loop->stop();
    co_return;
}

struct ConcurrentFetch
{
    realurl::EventLoop *loop = nullptr;
    realurl::Client *client = nullptr;
    std::string url;
    int total = 0;
    std::atomic<int> *completed = nullptr;
    std::atomic<int> *succeeded = nullptr;
};

realurl::Task<void> fetch_one(ConcurrentFetch fetch)
{
    try {
        const realurl::Response response = co_await fetch.client->get(fetch.url, 10s);
        if (response.head.status == 200 && response.body == "world")
            fetch.succeeded->fetch_add(1, std::memory_order_relaxed);
    } catch (...) {
    }
    if (fetch.completed->fetch_add(1, std::memory_order_acq_rel) + 1 == fetch.total)
        fetch.loop->stop();
    co_return;
}

int free_port()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return -1;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) != 0) {
        ::close(fd);
        return -1;
    }
    const int port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

struct WireFrame
{
    realurl::detail::h2::FrameType type;
    std::uint8_t flags = 0;
    std::string payload;
};

/// Speak raw h2c to the server and record the frames of each response, so
/// the test sees exactly what goes on the wire.
void test_unary_response_frames()
{
    namespace h2 = realurl::detail::h2;

    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });
    server.Get("/empty", [](const auto &, auto &response) { response.status = 200; });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 500 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    CHECK(::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0);
    timeval timeout{ 5, 0 };
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    h2::HpackEncoder encoder;
    h2::FrameWriter writer;
    writer.settings({});
    for (const auto &[id, path] : { std::pair<std::uint32_t, const char *>{ 1, "/hello" }, { 3, "/empty" } }) {
        std::string block;
        encoder.encode({ { ":method", "GET" }, { ":scheme", "http" }, { ":authority", "127.0.0.1" },
                         { ":path", path } },
                       block);
        writer.headers(id, block, true, true);
    }
    std::string out(realurl::detail::kH2Preface);
    out += writer.buffer();
    CHECK(::send(fd, out.data(), out.size(), 0) == static_cast<ssize_t>(out.size()));

    std::map<std::uint32_t, std::vector<WireFrame>> streams;
    std::string input;
    std::size_t ended = 0;
    h2::FrameParser parser;
    char buffer[4096];
    while (ended < 2) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0)
            break;
        input.append(buffer, static_cast<std::size_t>(n));
        std::size_t offset = 0;
        h2::Frame frame;
        while (parser.next(input, offset, frame) == h2::ParseResult::Ok) {
            if (frame.header.stream_id == 0)
                continue;
            streams[frame.header.stream_id].push_back(
                { frame.header.type, frame.header.flags, std::string(frame.payload) });
            if ((frame.header.flags & h2::kFlagEndStream) != 0)
                ++ended;
        }
        input.erase(0, offset);
    }
    ::close(fd);

    // A body response is HEADERS + one DATA frame that also carries END_STREAM.
    const std::vector<WireFrame> &hello = streams[1];
    CHECK(hello.size() == 2);
    if (hello.size() == 2) {
        CHECK(hello[0].type == h2::FrameType::Headers);
        CHECK(hello[0].flags == h2::kFlagEndHeaders);
        CHECK(hello[1].type == h2::FrameType::Data);
        CHECK(hello[1].flags == h2::kFlagEndStream);
        CHECK(hello[1].payload == "world");
    }

    // An empty body ends the stream on HEADERS.
    const std::vector<WireFrame> &empty = streams[3];
    CHECK(empty.size() == 1);
    if (empty.size() == 1) {
        CHECK(empty[0].type == h2::FrameType::Headers);
        CHECK(empty[0].flags == (h2::kFlagEndHeaders | h2::kFlagEndStream));
    }

    server.stop();
    listener.join();
}

void test_unary_and_streaming()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_payload_max_length(4 * 1024 * 1024);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });
    server.Post("/echo", [](const auto &request, auto &response) {
        response.set_content(request.body);
    });
    server.Get("/stream", [](const auto &, auto &response) {
        response.take_over_task([](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            const realurl::server::Header headers[] = { { "content-type", "text/plain" } };
            co_await writer.write_head(200, headers, std::nullopt);
            co_await writer.write("one");
            co_await writer.write("two");
            co_await writer.end();
        });
    });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 500 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);

    realurl::Config config;
    config.protocol = realurl::Protocol::Http2;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 128;
    realurl::ClientGroup group(2, config);
    const std::string base = "http://127.0.0.1:" + std::to_string(port);

    const realurl::Response hello = realurl::sync_wait(group.get(base + "/hello"));
    CHECK(hello.head.status == 200);
    CHECK(hello.body == "world");
    CHECK(hello.head.protocol == realurl::Protocol::Http2);

    const realurl::Response echo =
        realurl::sync_wait(group.request("POST", base + "/echo", {}, "ping"));
    CHECK(echo.head.status == 200);
    CHECK(echo.body == "ping");

    const realurl::Response missing = realurl::sync_wait(group.get(base + "/missing"));
    CHECK(missing.head.status == 404);

    const std::string large(2 * 1024 * 1024, 'x');
    const realurl::Response large_echo =
        realurl::sync_wait(group.request("POST", base + "/echo", {}, large));
    CHECK(large_echo.head.status == 200);
    CHECK(large_echo.body == large);

    realurl::EventLoop loop;
    realurl::Client client(loop, config);
    bool streamed_ok = false;
    loop.spawn(fetch_stream({ &loop, &client, base + "/stream", &streamed_ok }));
    loop.run();
    CHECK(streamed_ok);

    server.stop();
    listener.join();
}

void test_concurrent_streams()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 500 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);

    realurl::Config config;
    config.protocol = realurl::Protocol::Http2;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 128;

    realurl::EventLoop loop;
    realurl::Client client(loop, config);
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + "/hello";

    constexpr int kRequests = 64;
    std::atomic<int> completed{ 0 };
    std::atomic<int> succeeded{ 0 };

    for (int i = 0; i < kRequests; ++i) {
        loop.spawn(fetch_one({ &loop, &client, url, kRequests, &completed, &succeeded }));
    }

    loop.run();
    CHECK(completed.load() == kRequests);
    CHECK(succeeded.load() == kRequests);

    server.stop();
    listener.join();
}

} // namespace

int main()
{
    test_unary_and_streaming();
    test_unary_response_frames();
    test_concurrent_streams();
    return test::summary();
}
