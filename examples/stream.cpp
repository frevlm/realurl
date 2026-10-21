#include <realurl/http2.hpp>

#include <chrono>
#include <cstdio>
#include <exception>
#include <string>

/// Opens one bidirectional HTTP/2 stream, uploads a body in chunks, then
/// streams the response back out.
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
            realurl::Headers headers = { { "content-type", "text/plain" } };
            realurl::Stream stream = co_await client.open("POST", url, std::move(headers), 30s);

            for (int i = 0; i < 4; ++i) {
                co_await stream.write("chunk\n", 30s);
            }
            co_await stream.end(30s);

            const realurl::ResponseHead head = co_await stream.head(30s);
            std::fprintf(stderr, "status: %d\n", head.status);

            char buffer[65536];
            for (;;) {
                const std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, 30s);
                if (n == 0)
                    break;
                std::fwrite(buffer, 1, n, stdout);
                std::fflush(stdout);
            }
        } catch (const std::exception &error) {
            std::fprintf(stderr, "stream failed: %s\n", error.what());
            exit_code = 1;
        }
        loop.stop();
    };

    loop.spawn(run());
    loop.run();
    return exit_code;
}
