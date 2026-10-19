#include "realurl/server.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

namespace
{

std::atomic_bool running{ true };

void on_signal(int)
{
    running.store(false);
}

} // namespace

int main(int argc, char **argv)
{
    const int port = argc > 1 ? std::atoi(argv[1]) : 8080;

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    realurl::Server server;
    server.set_io_threads(2);

    server.Get("/", [](const auto &, auto &res) { res.set_content("hello\n", "text/plain"); });

    server.Get("/events", [](const auto &, auto &res) {
        res.take_over([](realurl::ClientWriter &writer) {
            (void)writer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n");
            for (int i = 0; i < 5; ++i) {
                const std::string event = "data: " + std::to_string(i) + "\n\n";
                if (!writer(event))
                    return;
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
        });
    });

    server.Post("/echo", [](const auto &req, auto &res) { res.set_content(req.body, "text/plain"); });

    std::printf("listening on http://127.0.0.1:%d\n", port);
    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });

    while (running.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    server.stop();
    listener.join();
    return 0;
}
