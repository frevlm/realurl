#include "realurl/http2.hpp"
#include "realurl/proxy.hpp"
#include "realurl/server.hpp"

#include "support/h2_test_server.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#ifndef REALURL_CERT_DIR
#define REALURL_CERT_DIR "../../tests/certs"
#endif

using namespace std::chrono_literals;
using realurl::Protocol;

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

void wait_running(const realurl::Server &server)
{
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
}

realurl::Config config_with(Protocol protocol)
{
    realurl::Config config;
    config.protocol = protocol;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 128;
    config.connect_timeout = 1s;
    return config;
}

std::string base_url(int port)
{
    return "http://127.0.0.1:" + std::to_string(port);
}

bool has_header(const realurl::Headers &headers, std::string_view name, std::string_view value)
{
    for (const auto &header : headers) {
        if (header.name == name && header.value == value)
            return true;
    }
    return false;
}

struct StreamCheck {
    realurl::EventLoop *loop = nullptr;
    realurl::Client *client = nullptr;
    std::string url;
    bool *ok = nullptr;
};

realurl::Task<void> check_stream(StreamCheck check)
{
    try {
        realurl::Stream stream = co_await check.client->open("GET", check.url);
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
        *check.ok = head.status == 200 && head.protocol == Protocol::Native && body == "onetwo";
    } catch (...) {
        *check.ok = false;
    }
    check.loop->stop();
    co_return;
}

struct ConcurrentCheck {
    realurl::EventLoop *loop = nullptr;
    realurl::Client *client = nullptr;
    std::string url;
    int total = 0;
    std::atomic<int> *completed = nullptr;
    std::atomic<int> *succeeded = nullptr;
};

realurl::Task<void> fetch_one(ConcurrentCheck check)
{
    try {
        const realurl::Response response = co_await check.client->get(check.url, 10s);
        if (response.head.status == 200 && response.body == "world" && response.head.protocol == Protocol::Native) {
            check.succeeded->fetch_add(1, std::memory_order_relaxed);
        }
    } catch (...) {
    }
    if (check.completed->fetch_add(1, std::memory_order_acq_rel) + 1 == check.total)
        check.loop->stop();
    co_return;
}

void test_native_unary_streaming_and_large_headers()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_payload_max_length(4 * 1024 * 1024);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });
    server.Post("/echo", [](const auto &request, auto &response) { response.set_content(request.body); });
    server.Get("/headers", [](const auto &request, auto &response) {
        response.set_content(std::string(request.get_header("x-large")));
    });
    server.Post("/length", [](const auto &request, auto &response) {
        response.set_content(std::to_string(request.content_length));
    });
    server.Get("/bad-header", [](const auto &, auto &response) {
        response.set_header("x-bad", "line\nbreak");
        response.set_content("x");
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
    wait_running(server);

    realurl::ClientGroup group(1, config_with(Protocol::Native));
    const std::string base = base_url(port);

    const realurl::Response hello = realurl::sync_wait(group.get(base + "/hello"));
    CHECK(hello.head.status == 200);
    CHECK(hello.body == "world");
    CHECK(hello.head.protocol == Protocol::Native);
    CHECK(hello.head.content_length == 5);
    CHECK(has_header(hello.head.headers, "content-length", "5"));
    CHECK(server.stats().native_connections == 1);

    const realurl::Response echo = realurl::sync_wait(group.request("POST", base + "/echo", {}, "ping"));
    CHECK(echo.head.status == 200);
    CHECK(echo.body == "ping");
    CHECK(echo.head.protocol == Protocol::Native);
    CHECK(echo.head.content_length == 4);

    const std::string large(2 * 1024 * 1024, 'x');
    const realurl::Response large_echo = realurl::sync_wait(group.request("POST", base + "/echo", {}, large));
    CHECK(large_echo.head.status == 200);
    CHECK(large_echo.body == large);
    CHECK(large_echo.head.protocol == Protocol::Native);
    CHECK(large_echo.head.content_length == large.size());

    const realurl::Response length = realurl::sync_wait(group.request("POST", base + "/length", {}, "12345"));
    CHECK(length.head.status == 200);
    CHECK(length.body == "5");

    const std::string large_header(32 * 1024, 'h');
    const realurl::Headers headers = { { "x-large", large_header } };
    const realurl::Response header_echo = realurl::sync_wait(group.request("GET", base + "/headers", headers, {}));
    CHECK(header_echo.head.status == 200);
    CHECK(header_echo.body == large_header);
    CHECK(header_echo.head.content_length == large_header.size());

    const realurl::Response missing = realurl::sync_wait(group.get(base + "/missing"));
    CHECK(missing.head.status == 404);
    CHECK(missing.head.protocol == Protocol::Native);

    bool bad_header_failed = false;
    try {
        (void)realurl::sync_wait(group.get(base + "/bad-header"));
    } catch (...) {
        bad_header_failed = true;
    }
    CHECK(bad_header_failed);

    const realurl::Response after_bad = realurl::sync_wait(group.get(base + "/hello"));
    CHECK(after_bad.head.status == 200);
    CHECK(after_bad.body == "world");

    realurl::EventLoop loop;
    realurl::Client client(loop, config_with(Protocol::Native));
    bool streamed_ok = false;
    loop.spawn(check_stream({ &loop, &client, base + "/stream", &streamed_ok }));
    loop.run();
    CHECK(streamed_ok);

    client.close();
    group.close();
    server.stop();
    listener.join();
}

void test_native_concurrency()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    realurl::EventLoop loop;
    realurl::Client client(loop, config_with(Protocol::Native));
    const std::string url = base_url(port) + "/hello";

    constexpr int kRequests = 64;
    std::atomic<int> completed{ 0 };
    std::atomic<int> succeeded{ 0 };
    for (int i = 0; i < kRequests; ++i)
        loop.spawn(fetch_one({ &loop, &client, url, kRequests, &completed, &succeeded }));
    loop.run();

    CHECK(completed.load() == kRequests);
    CHECK(succeeded.load() == kRequests);
    CHECK(server.stats().native_connections == 1);

    client.close();
    server.stop();
    listener.join();
}

void test_auto_plaintext_negotiation_and_fallback()
{
    {
        const int port = free_port();
        CHECK(port > 0);

        realurl::Server server;
        server.set_io_threads(1);
        server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });

        std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
        wait_running(server);

        realurl::ClientGroup group(1, config_with(Protocol::Auto));
        const realurl::Response response = realurl::sync_wait(group.get(base_url(port) + "/hello"));
        CHECK(response.head.status == 200);
        CHECK(response.head.protocol == Protocol::Native);

        group.close();
        server.stop();
        listener.join();
    }

    {
        realurl::test::H2TestServer upstream([](const realurl::test::H2TestRequest &) {
            realurl::test::H2TestResponse response;
            response.body = "h2";
            return response;
        });

        realurl::Config config = config_with(Protocol::Auto);
        config.connect_timeout = 300ms;
        realurl::ClientGroup group(1, config);
        const std::string url = "http://127.0.0.1:" + std::to_string(upstream.port()) + "/x";
        const realurl::Response response = realurl::sync_wait(group.get(url));
        CHECK(response.head.status == 200);
        CHECK(response.body == "h2");
        CHECK(response.head.protocol == Protocol::Http2);
    }
}

void test_native_disabled()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_native(false);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    {
        realurl::Config config = config_with(Protocol::Auto);
        config.connect_timeout = 300ms;
        realurl::ClientGroup group(1, config);
        const realurl::Response response = realurl::sync_wait(group.get(base_url(port) + "/hello"));
        CHECK(response.head.status == 200);
        CHECK(response.head.protocol == Protocol::Http2);
        group.close();
    }

    {
        realurl::Config config = config_with(Protocol::Native);
        config.connect_timeout = 300ms;
        realurl::ClientGroup group(1, config);
        bool threw = false;
        try {
            (void)realurl::sync_wait(group.get(base_url(port) + "/hello"));
        } catch (...) {
            threw = true;
        }
        CHECK(threw);
        group.close();
    }

    server.stop();
    listener.join();
}

void test_tls_native()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_tls(REALURL_CERT_DIR "/localhost.pem", REALURL_CERT_DIR "/localhost-key.pem");
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("secure"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    const std::string url = "https://localhost:" + std::to_string(port) + "/hello";

    {
        realurl::Config config = config_with(Protocol::Native);
        config.ca_file = REALURL_CERT_DIR "/localhost.pem";
        realurl::ClientGroup group(1, config);
        const realurl::Response response = realurl::sync_wait(group.get(url));
        CHECK(response.head.status == 200);
        CHECK(response.body == "secure");
        CHECK(response.head.protocol == Protocol::Native);
        group.close();
    }

    {
        realurl::Config config = config_with(Protocol::Auto);
        config.ca_file = REALURL_CERT_DIR "/localhost.pem";
        realurl::ClientGroup group(1, config);
        const realurl::Response response = realurl::sync_wait(group.get(url));
        CHECK(response.head.status == 200);
        CHECK(response.head.protocol == Protocol::Native);
        group.close();
    }

    {
        realurl::Config config = config_with(Protocol::Http2);
        config.ca_file = REALURL_CERT_DIR "/localhost.pem";
        realurl::ClientGroup group(1, config);
        const realurl::Response response = realurl::sync_wait(group.get(url));
        CHECK(response.head.status == 200);
        CHECK(response.head.protocol == Protocol::Http2);
        group.close();
    }

    server.stop();
    listener.join();
}

void test_tls_native_disabled()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_native(false);
    server.set_tls(REALURL_CERT_DIR "/localhost.pem", REALURL_CERT_DIR "/localhost-key.pem");
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("secure"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    realurl::Config config = config_with(Protocol::Auto);
    config.ca_file = REALURL_CERT_DIR "/localhost.pem";
    realurl::ClientGroup group(1, config);
    const realurl::Response response =
        realurl::sync_wait(group.get("https://localhost:" + std::to_string(port) + "/hello"));
    CHECK(response.head.status == 200);
    CHECK(response.head.protocol == Protocol::Http2);

    group.close();
    server.stop();
    listener.join();
}

void test_native_proxy()
{
    const int upstream_port = free_port();
    CHECK(upstream_port > 0);

    realurl::Server upstream;
    upstream.set_io_threads(1);
    upstream.Post("/echo", [](const auto &request, auto &response) { response.set_content(request.body); });

    std::thread upstream_listener([&] { (void)upstream.listen("127.0.0.1", upstream_port); });
    wait_running(upstream);

    const int proxy_port = free_port();
    CHECK(proxy_port > 0);

    realurl::Server proxy;
    proxy.set_io_threads(1);
    realurl::Config upstream_config = config_with(Protocol::Native);
    proxy.set_upstream(upstream_config);

    const std::string upstream_url = base_url(upstream_port) + "/echo";
    proxy.Post("/proxy", [upstream_url](const realurl::Server::Request &request, realurl::Server::Response &response) {
        response.take_over_task([&request, upstream_url](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            co_await realurl::server::proxy_stream(*request.upstream, upstream_url, request, writer);
        });
    });

    std::thread proxy_listener([&] { (void)proxy.listen("127.0.0.1", proxy_port); });
    wait_running(proxy);

    realurl::ClientGroup group(1, config_with(Protocol::Native));
    const realurl::Response response =
        realurl::sync_wait(group.request("POST", base_url(proxy_port) + "/proxy", {}, "payload"));
    CHECK(response.head.status == 200);
    CHECK(response.body == "payload");
    CHECK(response.head.protocol == Protocol::Native);

    group.close();
    proxy.stop();
    proxy_listener.join();
    upstream.stop();
    upstream_listener.join();
}

} // namespace

int main()
{
    test_native_unary_streaming_and_large_headers();
    test_native_concurrency();
    test_auto_plaintext_negotiation_and_fallback();
    test_native_disabled();
    test_tls_native();
    test_tls_native_disabled();
    test_native_proxy();
    return test::summary();
}
