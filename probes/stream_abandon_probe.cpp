// Test-only: compare abandoning StreamResult with explicitly closing it.
// A loopback fixture sends headers now and a body after execute_stream returns.
#include "realurl/curl_multi_pool.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>

int main(int argc, char **argv)
{
    const bool close_stream = argc == 2 && std::string(argv[1]) == "--close";
    curl_global_init(CURL_GLOBAL_DEFAULT);
    const int listener = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (listener < 0 || bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        listen(listener, 1) != 0)
        throw std::runtime_error("fixture listen failed");
    socklen_t length = sizeof(address);
    getsockname(listener, reinterpret_cast<sockaddr *>(&address), &length);
    std::thread fixture([listener] {
        const int client = accept(listener, nullptr, nullptr);
        if (client < 0)
            return;
        const int enabled = 1;
        setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
        std::string request;
        char buffer[1024];
        while (request.find("\r\n\r\n") == std::string::npos) {
            const auto n = recv(client, buffer, sizeof(buffer), 0);
            if (n <= 0)
                break;
            request.append(buffer, n);
        }
        const std::string headers = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n";
        send(client, headers.data(), headers.size(), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        send(client, "hello", 5, 0);
        close(client);
        close(listener);
    });
    {
        realurl::CurlMultiPool pool;
        realurl::CurlRequest request;
        request.url = "http://127.0.0.1:" + std::to_string(ntohs(address.sin_port)) + "/";
        request.method = "GET";
        request.content_length = 0;
        {
            auto stream = pool.execute_stream(request);
            if (stream.status_code != 200)
                throw std::runtime_error("fixture response failed");
            std::fprintf(stderr, "headers received; %s StreamResult\n", close_stream ? "closing" : "abandoning");
            if (close_stream)
                stream.stream_close();
        }
        for (int tick = 0; tick < 100; ++tick) {
            pool.perform();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    fixture.join();
    curl_global_cleanup();
    std::puts("completed without ASan failure");
}
