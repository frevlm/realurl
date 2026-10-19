#include "realurl/curl_multi_pool.hpp"

#include <cstdio>
#include <exception>
#include <string>

int main(int argc, char **argv)
{
    if (argc != 2) {
        std::fprintf(stderr, "usage: %s URL\n", argv[0]);
        return 2;
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);

    try {
        realurl::CurlMultiPool pool;
        realurl::CurlRequest request;
        request.url = argv[1];
        request.method = "GET";
        request.content_length = 0;
        request.header_timeout_s = 30;

        auto stream = pool.execute_stream(request);
        std::fprintf(stderr, "status: %d\n", stream.status_code);

        if (!stream.initial_body.empty())
            std::fwrite(stream.initial_body.data(), 1, stream.initial_body.size(), stdout);

        char buffer[65536];
        for (;;) {
            const ssize_t n = stream.stream_read(buffer, sizeof(buffer), 30000);
            if (n == 0)
                break;
            if (n < 0) {
                std::fprintf(stderr, "stream read failed\n");
                stream.stream_close();
                curl_global_cleanup();
                return 1;
            }
            std::fwrite(buffer, 1, static_cast<size_t>(n), stdout);
            std::fflush(stdout);
        }

        stream.stream_close();
    } catch (const std::exception &err) {
        std::fprintf(stderr, "stream failed: %s\n", err.what());
        curl_global_cleanup();
        return 1;
    }

    curl_global_cleanup();
    return 0;
}
