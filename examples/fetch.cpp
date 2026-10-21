#include <realurl/http2.hpp>

#include <chrono>
#include <cstdio>
#include <exception>
#include <string>

using namespace std::chrono_literals;

int main(int argc, char **argv)
{
    if (argc != 2) {
        std::fprintf(stderr, "usage: %s URL\n", argv[0]);
        return 2;
    }

    realurl::EventLoop loop;
    realurl::Client client(loop);
    const std::string url = argv[1];

    int exit_code = 0;
    auto run = [&]() -> realurl::Task<void> {
        try {
            const realurl::Response response = co_await client.get(url, 30s);
            std::fprintf(stderr, "status: %d\n", response.head.status);
            std::fwrite(response.body.data(), 1, response.body.size(), stdout);
            std::fflush(stdout);
        } catch (const std::exception &error) {
            std::fprintf(stderr, "request failed: %s\n", error.what());
            exit_code = 1;
        }
        loop.stop();
    };

    loop.spawn(run());
    loop.run();
    return exit_code;
}
