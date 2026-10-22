#include "realurl/http2.hpp"
#include "realurl/server.hpp"

#include "test_harness.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

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

std::string tls_h1_request(int port)
{
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == nullptr)
        return {};

    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    static const unsigned char kAlpn[] = { 8, 'h', 't', 't', 'p', '/', '1', '.', '1' };
    SSL_CTX_set_alpn_protos(ctx, kAlpn, sizeof(kAlpn));

    const int raw = connect_local(port);
    if (raw < 0) {
        SSL_CTX_free(ctx);
        return {};
    }

    SSL *ssl = SSL_new(ctx);
    SSL_set_fd(ssl, raw);
    SSL_set_tlsext_host_name(ssl, "localhost");
    std::string response;
    if (SSL_connect(ssl) == 1) {
        static constexpr std::string_view request =
            "GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        (void)SSL_write(ssl, request.data(), static_cast<int>(request.size()));
        char buffer[4096];
        for (;;) {
            const int n = SSL_read(ssl, buffer, sizeof(buffer));
            if (n <= 0)
                break;
            response.append(buffer, static_cast<std::size_t>(n));
        }
    }

    SSL_free(ssl);
    ::close(raw);
    SSL_CTX_free(ctx);
    return response;
}

} // namespace

int main()
{
    const int port = free_port();
    CHECK(port > 0);

    realurl::Server server;
    server.set_io_threads(1);
    server.set_tls(REALURL_CERT_DIR "/localhost.pem", REALURL_CERT_DIR "/localhost-key.pem");
    server.set_ktls(true);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("secure"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    CHECK(server.is_running());

    realurl::Config config;
    config.ca_file = REALURL_CERT_DIR "/localhost.pem";
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 32;

    realurl::ClientGroup group(1, config);
    const std::string url = "https://localhost:" + std::to_string(port) + "/hello";
    try {
        const realurl::Response response = realurl::sync_wait(group.get(url));
        CHECK(response.head.status == 200);
        CHECK(response.body == "secure");
    } catch (const std::exception &error) {
        std::fprintf(stderr, "tls h2 failed: %s\n", error.what());
        CHECK(false);
    }

    const std::string h1 = tls_h1_request(port);
    CHECK(h1.find("HTTP/1.1 200 OK") == 0);
    CHECK(h1.find("\r\n\r\nsecure") != std::string::npos);

    server.stop();
    listener.join();
    return test::summary();
}
