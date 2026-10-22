#include "realurl/proxy.hpp"
#include "realurl/server.hpp"

#include "support/h2_test_server.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using realurl::Server;

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

int connect_local(int port)
{
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

    for (int attempt = 0; attempt < 100; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return -1;
        if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0)
            return fd;
        ::close(fd);
        std::this_thread::sleep_for(5ms);
    }
    return -1;
}

std::string dechunk(std::string_view data)
{
    std::string out;
    std::size_t offset = 0;
    for (;;) {
        const auto line_end = data.find("\r\n", offset);
        if (line_end == std::string_view::npos)
            break;
        std::string_view line = data.substr(offset, line_end - offset);
        const auto extension = line.find(';');
        if (extension != std::string_view::npos)
            line = line.substr(0, extension);
        std::size_t size = 0;
        for (char c : line) {
            int digit = -1;
            if (c >= '0' && c <= '9')
                digit = c - '0';
            else if (c >= 'a' && c <= 'f')
                digit = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F')
                digit = c - 'A' + 10;
            if (digit < 0)
                return out;
            size = size * 16 + static_cast<std::size_t>(digit);
        }
        offset = line_end + 2;
        if (size == 0)
            break;
        if (offset + size > data.size())
            break;
        out.append(data.data() + offset, size);
        offset += size;
        if (data.substr(offset, 2) != "\r\n")
            break;
        offset += 2;
    }
    return out;
}

struct TimedResponse
{
    int status = 0;
    std::string body;
    double ttfb_ms = 0;
    double total_ms = 0;
};

TimedResponse timed_request(int port, std::string_view method, std::string_view path,
                            std::string_view payload = {})
{
    TimedResponse result;
    const int fd = connect_local(port);
    if (fd < 0)
        return result;

    timeval timeout{ 10, 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    std::string request;
    request.append(method).append(" ").append(path).append(" HTTP/1.1\r\n");
    request.append("Host: 127.0.0.1\r\nConnection: close\r\n");
    if (!payload.empty())
        request.append("Content-Length: ").append(std::to_string(payload.size())).append("\r\n");
    request.append("\r\n").append(payload);

    const auto start = std::chrono::steady_clock::now();
    std::size_t sent = 0;
    while (sent < request.size()) {
        const ssize_t n = ::send(fd, request.data() + sent, request.size() - sent, 0);
        if (n <= 0) {
            ::close(fd);
            return result;
        }
        sent += static_cast<std::size_t>(n);
    }

    std::string raw;
    char buffer[16384];
    bool first_body = false;
    for (;;) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0)
            break;
        raw.append(buffer, static_cast<std::size_t>(n));
        if (!first_body) {
            const auto head_end = raw.find("\r\n\r\n");
            if (head_end != std::string::npos && raw.size() > head_end + 4) {
                first_body = true;
                result.ttfb_ms = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - start).count();
            }
        }
    }
    const auto end = std::chrono::steady_clock::now();
    ::close(fd);

    result.total_ms = std::chrono::duration<double, std::milli>(end - start).count();
    const auto head_end = raw.find("\r\n\r\n");
    if (head_end != std::string::npos) {
        if (raw.size() >= 12)
            result.status = std::atoi(raw.substr(9, 3).c_str());
        const std::string_view body = std::string_view{ raw }.substr(head_end + 4);
        if (raw.find("Transfer-Encoding: chunked") != std::string::npos)
            result.body = dechunk(body);
        else
            result.body.assign(body);
    }
    return result;
}

void test_streaming_proxy()
{
    realurl::test::H2TestServer upstream([](const realurl::test::H2TestRequest &) {
        realurl::test::H2TestResponse response;
        response.body = "0123456789";
        response.chunks = 5;
        response.chunk_delay = 50ms;
        return response;
    });

    const int port = free_port();
    CHECK(port > 0);

    Server server;
    server.set_io_threads(1);
    realurl::Config config;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 64;
    server.set_upstream(config);

    const std::string url = "http://127.0.0.1:" + std::to_string(upstream.port()) + "/stream";
    server.Get("/stream", [url](const Server::Request &request, Server::Response &response) {
        response.take_over_task([&request, url](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            co_await realurl::server::proxy_stream(*request.upstream, url, request, writer);
        });
    });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 500 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);

    const TimedResponse response = timed_request(port, "GET", "/stream");
    CHECK(response.status == 200);
    CHECK(response.body == "0123456789");
    CHECK(response.ttfb_ms < 150.0);
    CHECK(response.total_ms > 200.0);

    server.stop();
    listener.join();
}

void test_upload_proxy()
{
    realurl::test::H2TestServer upstream([](const realurl::test::H2TestRequest &request) {
        realurl::test::H2TestResponse response;
        response.body = std::to_string(request.body.size());
        return response;
    });

    const int port = free_port();
    CHECK(port > 0);

    Server server;
    server.set_io_threads(1);
    server.set_payload_max_length(4 * 1024 * 1024);
    realurl::Config config;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 64;
    server.set_upstream(config);

    const std::string url = "http://127.0.0.1:" + std::to_string(upstream.port()) + "/upload";
    server.Post("/upload", [url](const Server::Request &request, Server::Response &response) {
        response.take_over_task([&request, url](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            co_await realurl::server::proxy_stream(*request.upstream, url, request, writer);
        });
    }, { .body_mode = Server::BodyMode::StreamedNative });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 500 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);

    const std::string payload(2 * 1024 * 1024, 'x');
    const TimedResponse response = timed_request(port, "POST", "/upload", payload);
    CHECK(response.status == 200);
    CHECK(response.body == std::to_string(payload.size()));

    server.stop();
    listener.join();
}

} // namespace

int main()
{
    test_streaming_proxy();
    test_upload_proxy();
    return test::summary();
}
