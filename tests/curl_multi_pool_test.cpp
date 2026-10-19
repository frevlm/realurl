#include "realurl/curl_multi_pool.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cctype>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace
{

using namespace std::chrono_literals;

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

bool send_all(int fd, std::string_view data)
{
    size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, kSendFlags);
        if (n <= 0)
            return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

size_t content_length_of(std::string_view request)
{
    const std::string lower = [&] {
        std::string out;
        out.reserve(request.size());
        for (char c : request)
            out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        return out;
    }();
    const auto pos = lower.find("content-length:");
    if (pos == std::string::npos)
        return 0;
    const auto line_end = lower.find("\r\n", pos);
    return static_cast<size_t>(std::stoull(lower.substr(pos + 15, line_end - pos - 15)));
}

/// One-shot loopback HTTP/1.1 server.  Sends a fixed body, optionally split
/// into two writes with a delay between them.
class LoopbackServer {
public:
    LoopbackServer(std::string body, std::chrono::milliseconds delay_before_body = 0ms, bool split_body = false)
        : body_(std::move(body))
        , delay_before_body_(delay_before_body)
        , split_body_(split_body)
    {
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listener_ < 0)
            throw std::runtime_error("socket failed");

        int yes = 1;
        ::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 || ::listen(listener_, 1) != 0)
            throw std::runtime_error("bind/listen failed");

        socklen_t length = sizeof(address);
        ::getsockname(listener_, reinterpret_cast<sockaddr *>(&address), &length);
        port_ = ntohs(address.sin_port);

        thread_ = std::thread([this] { serve(); });
    }

    ~LoopbackServer()
    {
        if (thread_.joinable())
            thread_.join();
        if (listener_ >= 0)
            ::close(listener_);
    }

    int port() const
    {
        return port_;
    }

    const std::string &request() const
    {
        return request_;
    }

private:
    void serve()
    {
        pollfd pfd{};
        pfd.fd = listener_;
        pfd.events = POLLIN;
        if (::poll(&pfd, 1, 3000) <= 0)
            return;

        const int client = ::accept(listener_, nullptr, nullptr);
        if (client < 0)
            return;

        read_request(client);

        const std::string headers = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Test: yes\r\nContent-Length: " +
                                    std::to_string(body_.size()) + "\r\nConnection: close\r\n\r\n";
        send_all(client, headers);

        if (delay_before_body_.count() > 0)
            std::this_thread::sleep_for(delay_before_body_);

        if (split_body_ && body_.size() > 1) {
            const size_t half = body_.size() / 2;
            send_all(client, std::string_view{ body_ }.substr(0, half));
            std::this_thread::sleep_for(20ms);
            send_all(client, std::string_view{ body_ }.substr(half));
        } else {
            send_all(client, body_);
        }

        ::shutdown(client, SHUT_WR);
        ::close(client);
    }

    void read_request(int client)
    {
        char buffer[4096];
        while (request_.find("\r\n\r\n") == std::string::npos) {
            const ssize_t n = ::recv(client, buffer, sizeof(buffer), 0);
            if (n <= 0)
                return;
            request_.append(buffer, static_cast<size_t>(n));
        }

        const auto header_end = request_.find("\r\n\r\n") + 4;
        const size_t content_length = content_length_of(request_);
        while (request_.size() - header_end < content_length) {
            const ssize_t n = ::recv(client, buffer, sizeof(buffer), 0);
            if (n <= 0)
                return;
            request_.append(buffer, static_cast<size_t>(n));
        }
    }

    int listener_ = -1;
    int port_ = 0;
    std::string body_;
    std::chrono::milliseconds delay_before_body_;
    bool split_body_ = false;
    std::string request_;
    std::thread thread_;
};

std::string read_stream(realurl::CurlMultiPool::StreamResult &stream)
{
    std::string body = stream.initial_body;
    char buffer[64];
    for (;;) {
        const ssize_t n = stream.stream_read(buffer, sizeof(buffer), 1000);
        if (n == 0)
            break;
        if (n < 0)
            throw std::runtime_error("stream read failed");
        body.append(buffer, static_cast<size_t>(n));
    }
    return body;
}

void test_get()
{
    LoopbackServer server("hello");
    realurl::CurlMultiPool pool;

    realurl::CurlRequest request;
    request.url = "http://127.0.0.1:" + std::to_string(server.port()) + "/get";
    request.method = "GET";
    request.content_length = 0;

    const realurl::CurlResponse response = pool.execute(request);
    CHECK(response.status_code == 200);
    CHECK(response.body == "hello");
    CHECK(response.headers.size() >= 2);

    bool saw_test_header = false;
    for (const auto &header : response.headers) {
        if (header.name == "X-Test" && header.value == "yes")
            saw_test_header = true;
    }
    CHECK(saw_test_header);
}

void test_post_body()
{
    LoopbackServer server("pong");
    realurl::CurlMultiPool pool;

    realurl::CurlRequest request;
    request.url = "http://127.0.0.1:" + std::to_string(server.port()) + "/upload";
    request.method = "POST";
    request.initial_body_chunk = "ping";

    const realurl::CurlResponse response = pool.execute(request);
    CHECK(response.status_code == 200);
    CHECK(response.body == "pong");
    CHECK(server.request().find("POST /upload") != std::string::npos);
    CHECK(server.request().find("ping") != std::string::npos);
}

void test_stream()
{
    LoopbackServer server("streaming-body", 50ms, true);
    realurl::CurlMultiPool pool;

    realurl::CurlRequest request;
    request.url = "http://127.0.0.1:" + std::to_string(server.port()) + "/stream";
    request.method = "GET";
    request.content_length = 0;
    request.header_timeout_s = 5;

    auto stream = pool.execute_stream(request);
    CHECK(stream.status_code == 200);
    CHECK(!stream.stream_error());

    const std::string body = read_stream(stream);
    CHECK(body == "streaming-body");
    CHECK(!stream.stream_error());

    stream.stream_close();
}

void test_write_abort()
{
    LoopbackServer server("will-be-aborted");
    realurl::CurlMultiPool pool;

    realurl::CurlRequest request;
    request.url = "http://127.0.0.1:" + std::to_string(server.port()) + "/abort";
    request.method = "GET";
    request.content_length = 0;
    request.on_response_chunk = [](std::string_view) { return false; };

    bool threw = false;
    try {
        (void)pool.execute(request);
    } catch (const std::runtime_error &) {
        threw = true;
    }
    CHECK(threw);
}

} // namespace

int main()
{
    curl_global_init(CURL_GLOBAL_DEFAULT);

    test_get();
    test_post_body();
    test_stream();
    test_write_abort();

    curl_global_cleanup();
    return test::summary();
}
