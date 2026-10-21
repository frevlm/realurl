#include "realurl/server.hpp"
#include "support/http1_client.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

namespace
{

using namespace std::chrono_literals;

bool starts_with(const std::string &value, std::string_view prefix)
{
    return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
}

void test_process_basics()
{
    realurl::Server server;
    server.Get("/hello", [](const auto &, auto &res) { res.set_content("world", "text/plain"); });
    server.Get("/users/:id", [](const auto &req, auto &res) {
        res.set_content(req.path_params.at("id"), "text/plain");
    });
    server.GetRegex("/regex/.*", [](const auto &, auto &res) { res.set_content("regex", "text/plain"); });
    server.Post("/echo", [](const auto &req, auto &res) { res.set_content(req.body, "text/plain"); });

    const std::string hello = server.process("GET /hello HTTP/1.1\r\nHost: test\r\n\r\n");
    CHECK(starts_with(hello, "HTTP/1.1 200 OK\r\n"));
    CHECK(hello.find("Content-Length: 5\r\n") != std::string::npos);
    CHECK(hello.find("Connection: close\r\n") != std::string::npos);
    CHECK(hello.find("\r\n\r\nworld") != std::string::npos);

    const std::string param = server.process("GET /users/42 HTTP/1.1\r\nHost: test\r\n\r\n");
    if (param.find("\r\n\r\n42") == std::string::npos)
        std::cerr << "param response: [" << param << "]\n";
    CHECK(param.find("\r\n\r\n42") != std::string::npos);

    const std::string regex = server.process("GET /regex/anything HTTP/1.1\r\nHost: test\r\n\r\n");
    CHECK(regex.find("\r\n\r\nregex") != std::string::npos);

    const std::string echo =
        server.process("POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\nping");
    CHECK(echo.find("\r\n\r\nping") != std::string::npos);

    const std::string missing = server.process("GET /missing HTTP/1.1\r\nHost: test\r\n\r\n");
    CHECK(starts_with(missing, "HTTP/1.1 404 Not Found\r\n"));
}

void test_process_take_over()
{
    realurl::Server server;
    server.Get("/raw", [](const auto &, auto &res) {
        res.take_over([](realurl::ClientWriter &writer) {
            (void)writer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n");
            (void)writer("data: one\n\n");
        });
    });

    const std::string raw = server.process("GET /raw HTTP/1.1\r\nHost: test\r\n\r\n");
    CHECK(starts_with(raw, "HTTP/1.1 200 OK\r\n"));
    CHECK(raw.find("data: one\n\n") != std::string::npos);
}

void test_process_limits()
{
    realurl::Server server;
    server.set_payload_max_length(3);
    server.set_error_body(413, "text/plain", "too large");
    server.Post("/echo", [](const auto &req, auto &res) { res.set_content(req.body); });

    const std::string too_large =
        server.process("POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\nxxxx");
    CHECK(starts_with(too_large, "HTTP/1.1 413 Payload Too Large\r\n"));
    CHECK(too_large.find("\r\n\r\ntoo large") != std::string::npos);

    const std::string chunked =
        server.process("POST /echo HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n"
                       "4\r\nping\r\n0\r\n\r\n");
    CHECK(starts_with(chunked, "HTTP/1.1 413 Payload Too Large\r\n"));
}

void test_process_streamed_chunked_rejected()
{
    realurl::Server server;
    server.Post("/stream", [](const auto &, auto &res) { res.set_content("ok"); },
                { .body_mode = realurl::Server::BodyMode::Streamed });

    const std::string response =
        server.process("POST /stream HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n"
                       "4\r\nping\r\n0\r\n\r\n");
    CHECK(starts_with(response, "HTTP/1.1 400 Bad Request\r\n"));
}

void test_content_type_header()
{
    realurl::Server server;
    server.Get("/ct", [](const auto &, auto &res) {
        res.set_header("Content-Type", "application/json");
        res.set_content("{}");
    });

    const std::string response = server.process("GET /ct HTTP/1.1\r\nHost: test\r\n\r\n");
    CHECK(response.find("Content-Type: application/json\r\n") != std::string::npos);
    const auto first = response.find("Content-Type:");
    CHECK(first != std::string::npos);
    CHECK(response.find("Content-Type:", first + 1) == std::string::npos);
}

int free_port()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t length = sizeof(address);
    ::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length);
    const int port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

int connect_local(int port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    timeval timeout{ 2, 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool send_raw(int fd, std::string_view data)
{
    std::size_t sent = 0;
    while (sent < data.size()) {
#ifdef MSG_NOSIGNAL
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
#else
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
#endif
        if (n <= 0)
            return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

std::string read_until_close(int fd)
{
    std::string out;
    char buffer[4096];
    for (;;) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0)
            break;
        out.append(buffer, static_cast<std::size_t>(n));
    }
    return out;
}

std::string read_until(int fd, std::string_view marker)
{
    std::string out;
    char buffer[4096];
    while (out.find(marker) == std::string::npos) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0)
            break;
        out.append(buffer, static_cast<std::size_t>(n));
    }
    return out;
}

void test_live_expect_continue(int port)
{
    const int fd = connect_local(port);
    CHECK(fd >= 0);
    if (fd < 0)
        return;

    CHECK(send_raw(fd, "POST /echo HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n"));
    const std::string interim = read_until(fd, "\r\n\r\n");
    CHECK(interim.find("HTTP/1.1 100 Continue") != std::string::npos);

    CHECK(send_raw(fd, "ping"));
    const std::string response = read_until_close(fd);
    CHECK(response.find("HTTP/1.1 200 OK") != std::string::npos);
    CHECK(response.find("\r\n\r\nping") != std::string::npos);
    CHECK(::close(fd) == 0);
}

void test_live_requests()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(2);
    server.Get("/hello", [](const auto &, auto &res) { res.set_content("world", "text/plain"); });
    server.Post("/echo", [](const auto &req, auto &res) { res.set_content(req.body, "text/plain"); });
    server.Post("/upload", [](const auto &req, auto &res) {
        const std::size_t expected = static_cast<std::size_t>(req.content_length);
        std::string received;
        while (received.size() < expected) {
            auto result = req.body_source->read();
            if (result.eof)
                break;
            if (result.wait) {
                std::this_thread::sleep_for(1ms);
                continue;
            }
            const std::size_t take = std::min(result.chunk.size(), expected - received.size());
            received.append(result.chunk.substr(0, take));
            req.body_source->consume(take);
        }
        res.set_content(std::to_string(received.size()), "text/plain");
    }, { .body_mode = realurl::Server::BodyMode::Streamed });
    server.Get("/events", [](const auto &, auto &res) {
        res.take_over([](realurl::ClientWriter &writer) {
            (void)writer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n");
            (void)writer("data: one\n\n");
            (void)writer("data: two\n\n");
        });
    });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 200 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);

    auto get_response = test::http1_request(port, "GET", "/hello");
    CHECK(get_response.status == 200);
    CHECK(get_response.body == "world");

    auto post_response = test::http1_request(port, "POST", "/echo", "ping");
    CHECK(post_response.status == 200);
    CHECK(post_response.body == "ping");

    const std::string large(256 * 1024, 'x');
    auto upload_response = test::http1_request(port, "POST", "/upload", large);
    CHECK(upload_response.status == 200);
    CHECK(upload_response.body == std::to_string(large.size()));

    auto events_response = test::http1_request(port, "GET", "/events");
    CHECK(events_response.status == 200);
    CHECK(events_response.body == "data: one\n\ndata: two\n\n");

    test_live_expect_continue(port);

    server.stop();
    listener.join();
}

} // namespace

int main()
{
    test_process_basics();
    test_process_take_over();
    test_process_limits();
    test_process_streamed_chunked_rejected();
    test_content_type_header();
    test_live_requests();
    return test::summary();
}
