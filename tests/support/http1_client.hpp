#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace test
{

struct Http1Response {
    int status = 0;
    std::string body;
};

/// Blocking HTTP/1.1 client used to exercise the HTTP/1.1 server in tests.
/// Reads until the peer closes the connection (the server sends
/// `Connection: close` on every response).
inline Http1Response http1_request(int port, std::string_view method, std::string_view path,
                                   std::string_view body = {})
{
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

    int fd = -1;
    for (int retry = 0; retry < 100; ++retry) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            break;
        if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0)
            break;
        ::close(fd);
        fd = -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (fd < 0)
        throw std::runtime_error("connect failed");

    timeval timeout{ 5, 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    std::string request;
    request.append(method).append(" ").append(path).append(" HTTP/1.1\r\n");
    request.append("Host: 127.0.0.1\r\nConnection: close\r\n");
    if (!body.empty()) {
        request.append("Content-Length: ").append(std::to_string(body.size())).append("\r\n");
    }
    request.append("\r\n");
    request.append(body);

    std::size_t sent = 0;
    while (sent < request.size()) {
#ifdef MSG_NOSIGNAL
        const ssize_t n = ::send(fd, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
#else
        const ssize_t n = ::send(fd, request.data() + sent, request.size() - sent, 0);
#endif
        if (n <= 0) {
            ::close(fd);
            throw std::runtime_error("send failed");
        }
        sent += static_cast<std::size_t>(n);
    }

    std::string raw;
    char buffer[16384];
    for (;;) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0)
            break;
        raw.append(buffer, static_cast<std::size_t>(n));
    }
    ::close(fd);

    Http1Response response;
    const std::size_t line_end = raw.find("\r\n");
    if (line_end == std::string::npos || raw.size() < 12)
        throw std::runtime_error("malformed response");
    response.status = std::stoi(raw.substr(9, 3));

    const std::size_t body_start = raw.find("\r\n\r\n");
    if (body_start != std::string::npos)
        response.body = raw.substr(body_start + 4);
    return response;
}

} // namespace test
