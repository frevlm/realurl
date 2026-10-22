#include "realurl/detail/tls.hpp"
#include "realurl/http2.hpp"
#include "realurl/server.hpp"

#include "test_harness.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using realurl::detail::SocketAddress;
using realurl::detail::TlsContext;
using realurl::detail::TlsStream;

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
    socklen_t length = sizeof(address);
    int port = -1;
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0 &&
        ::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) == 0) {
        port = ntohs(address.sin_port);
    }
    ::close(fd);
    return port;
}

SocketAddress loopback(int port)
{
    SocketAddress address;
    auto *in = reinterpret_cast<sockaddr_in *>(&address.storage);
    in->sin_family = AF_INET;
    in->sin_port = htons(static_cast<std::uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &in->sin_addr);
    address.length = sizeof(sockaddr_in);
    return address;
}

/// Client preface plus an empty SETTINGS frame.
constexpr std::string_view kH2Hello("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                                    "\x00\x00\x00\x04\x00\x00\x00\x00\x00",
                                    24 + 9);

/// Opens one TLS connection and reads the server's SETTINGS frame, which also
/// delivers the session ticket. Returns -1 on failure, else whether the
/// handshake resumed.
realurl::Task<int> h2_hello(std::shared_ptr<TlsContext> context, int port)
{
    try {
        auto transport = co_await TlsStream::connect(realurl::EventLoop::current(), loopback(port), "localhost",
                                                     context, 5s);
        auto &stream = static_cast<TlsStream &>(*transport);
        co_await stream.write_all(kH2Hello, 5s);

        char frame[9];
        std::size_t have = 0;
        while (have < sizeof(frame)) {
            const std::size_t n = co_await stream.read({ frame + have, sizeof(frame) - have }, 5s);
            if (n == 0)
                co_return -1;
            have += n;
        }
        if (frame[3] != 0x04)
            co_return -1;
        co_return stream.resumed() ? 1 : 0;
    } catch (...) {
        co_return -1;
    }
}

realurl::Task<void> resume_twice(std::shared_ptr<TlsContext> context, int port, int *first, int *second)
{
    *first = co_await h2_hello(context, port);
    *second = co_await h2_hello(context, port);
}

/// Handshake against `port` with a short timeout. Returns the errno of the
/// failure, or 0 when it succeeds.
realurl::Task<int> handshake_error(std::shared_ptr<TlsContext> context, int port)
{
    try {
        (void)co_await TlsStream::connect(realurl::EventLoop::current(), loopback(port), "localhost", context, 100ms);
    } catch (const std::system_error &error) {
        co_return error.code().value();
    } catch (...) {
        co_return -1;
    }
    co_return 0;
}

realurl::Task<void> timeout_then_resume(std::shared_ptr<TlsContext> context, int silent_port, int port, int *timed_out,
                                        int *resumed)
{
    *timed_out = co_await handshake_error(context, silent_port);
    *resumed = co_await h2_hello(context, port);
}

/// Listening socket that completes TCP handshakes but never answers.
int silent_listener(int &port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 || ::listen(fd, 4) != 0 ||
        ::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) != 0) {
        ::close(fd);
        return -1;
    }
    port = ntohs(address.sin_port);
    return fd;
}

realurl::Task<int> get_status(realurl::Client &client, std::string url)
{
    try {
        const realurl::Response response = co_await client.get(std::move(url), 5s);
        co_return response.body == "secure" ? response.head.status : -1;
    } catch (...) {
        co_return -1;
    }
}

realurl::Task<void> client_reconnects(realurl::Config config, std::string url, int *first, int *second)
{
    realurl::Client client(realurl::EventLoop::current(), config);
    *first = co_await get_status(client, url);
    client.close();
    *second = co_await get_status(client, url);
}

} // namespace

int main()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_tls(REALURL_CERT_DIR "/localhost.pem", REALURL_CERT_DIR "/localhost-key.pem");
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("secure"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    CHECK(server.is_running());

    const auto context = TlsContext::create(REALURL_CERT_DIR "/localhost.pem", false);
    int first = -1;
    int second = -1;
    realurl::sync_wait(resume_twice(context, port, &first, &second));
    CHECK(first == 0);
    CHECK(second == 1);

    // A handshake that times out releases its SSL, and the cached session
    // still resumes the next connection.
    int silent_port = 0;
    const int silent_fd = silent_listener(silent_port);
    CHECK(silent_fd >= 0);
    int timed_out = -1;
    int resumed = -1;
    realurl::sync_wait(timeout_then_resume(context, silent_port, port, &timed_out, &resumed));
    ::close(silent_fd);
    CHECK(timed_out == ETIMEDOUT);
    CHECK(resumed == 1);

    realurl::Config config;
    config.ca_file = REALURL_CERT_DIR "/localhost.pem";
    config.protocol = realurl::Protocol::Http2;
    config.max_conns_per_origin = 1;
    const std::string url = "https://localhost:" + std::to_string(port) + "/hello";
    first = second = -1;
    realurl::sync_wait(client_reconnects(config, url, &first, &second));
    CHECK(first == 200);
    CHECK(second == 200);

    server.stop();
    listener.join();
    return test::summary();
}
