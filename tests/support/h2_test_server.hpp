#pragma once

#include "realurl/detail/http2_types.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>

namespace realurl::test
{

struct H2TestRequest {
    std::string method;
    std::string path;
    std::string authority;
    detail::h2::HeaderList headers;
    std::string body;
};

struct H2TestResponse {
    int status = 200;
    detail::h2::HeaderList headers;
    std::string body;
    std::chrono::milliseconds delay{ 0 };
    /// Keep the stream open: do not send a response, let the request hang.
    bool hold = false;
    bool goaway_after = false;
    bool reset_after = false;
    /// If chunks > 0, the body is divided into `chunks` pieces, sent with
    /// `chunk_delay` between them.
    int chunks = 0;
    std::chrono::milliseconds chunk_delay{ 0 };
};

using Handler = std::function<H2TestResponse(const H2TestRequest &)>;

/// Minimal h2c (prior knowledge) server built on the same frame and HPACK
/// code as the client. Blocking, one connection at a time. Test-only.
class H2TestServer {
public:
    explicit H2TestServer(Handler handler);
    ~H2TestServer();

    H2TestServer(const H2TestServer &) = delete;
    H2TestServer &operator=(const H2TestServer &) = delete;

    std::uint16_t port() const noexcept
    {
        return port_;
    }

    int connections() const noexcept
    {
        return connections_.load(std::memory_order_relaxed);
    }

private:
    void serve();
    void handle_connection(int fd);

    Handler handler_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::thread thread_;
    std::atomic<bool> stop_{ false };
    std::atomic<int> connections_{ 0 };
};

/// Same as H2TestServer, but over TLS. Requires the client to pass
/// tests/certs/localhost.pem as its ca_file.
class H2TlsTestServer {
public:
    H2TlsTestServer(std::string cert_file, std::string key_file, Handler handler);
    ~H2TlsTestServer();

    H2TlsTestServer(const H2TlsTestServer &) = delete;
    H2TlsTestServer &operator=(const H2TlsTestServer &) = delete;

    std::uint16_t port() const noexcept
    {
        return port_;
    }

    int connections() const noexcept
    {
        return connections_.load(std::memory_order_relaxed);
    }

private:
    void serve();
    void handle_connection(int fd);

    std::string cert_file_;
    std::string key_file_;
    Handler handler_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::thread thread_;
    std::atomic<bool> stop_{ false };
    std::atomic<int> connections_{ 0 };
};

} // namespace realurl::test
