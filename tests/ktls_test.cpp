#include "realurl/detail/tls_server.hpp"

#include "test_harness.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

namespace
{

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

void client_thread(std::uint16_t port, bool *ok)
{
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == nullptr) {
        *ok = false;
        return;
    }
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);

    const int fd = connect_local(port);
    if (fd < 0) {
        SSL_CTX_free(ctx);
        *ok = false;
        return;
    }

    SSL *ssl = SSL_new(ctx);
    SSL_set_fd(ssl, fd);
    SSL_set_tlsext_host_name(ssl, "localhost");
    if (SSL_connect(ssl) != 1) {
        *ok = false;
    } else {
        (void)SSL_write(ssl, "ping", 4);
        char buffer[8];
        *ok = SSL_read(ssl, buffer, sizeof(buffer)) == 2;
    }

    SSL_free(ssl);
    ::close(fd);
    SSL_CTX_free(ctx);
}


struct KtlsServerState
{
    realurl::detail::EventLoop *loop = nullptr;
    realurl::detail::TcpListener *listener = nullptr;
    std::shared_ptr<realurl::detail::TlsServerContext> context;
    bool *ktls_send = nullptr;
    bool *ktls_recv = nullptr;
    bool *server_ok = nullptr;
};

realurl::detail::Task<void> serve_ktls(KtlsServerState state)
{
    try {
        auto [fd, peer] = co_await state.listener->accept();
        (void)peer;
        realurl::detail::TcpStream socket(*state.loop, fd);
        auto transport = co_await realurl::detail::TlsServerStream::accept(
            *state.loop, std::move(socket), state.context, 5s);
        auto *tls = dynamic_cast<realurl::detail::TlsServerStream *>(transport.get());
        if (tls != nullptr) {
            char buffer[8];
            const std::size_t n = co_await tls->read({ buffer, sizeof(buffer) }, 5s);
            *state.server_ok = n == 4 && std::string_view(buffer, n) == "ping";
            co_await tls->write_all("ok", 5s);
            *state.ktls_send = tls->ktls_send();
            *state.ktls_recv = tls->ktls_recv();
        }
    } catch (...) {
        *state.server_ok = false;
    }
    state.loop->stop();
    co_return;
}

} // namespace

int main()
{
    auto context = realurl::detail::TlsServerContext::create(REALURL_CERT_DIR "/localhost.pem",
                                                             REALURL_CERT_DIR "/localhost-key.pem",
                                                             true, true);

    realurl::detail::EventLoop loop;
    realurl::detail::TcpListener listener(loop, 0);
    const std::uint16_t port = listener.port();

    bool ktls_send = false;
    bool ktls_recv = false;
    bool server_ok = false;
    bool client_ok = false;

    loop.spawn(serve_ktls({ &loop, &listener, context, &ktls_send, &ktls_recv, &server_ok }));

    std::thread client([&] { client_thread(port, &client_ok); });
    loop.run();
    client.join();

    CHECK(client_ok);
    CHECK(server_ok);
    std::printf("ktls: send=%d recv=%d requested=%d\n", ktls_send ? 1 : 0, ktls_recv ? 1 : 0,
                context->ktls_requested() ? 1 : 0);
    return test::summary();
}
