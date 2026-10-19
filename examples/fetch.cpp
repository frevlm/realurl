#include "realurl/curl_multi_pool.hpp"

#include <cstdio>
#include <exception>
#include <string>
#include <string_view>

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

        const realurl::CurlResponse response = pool.execute(request);
        std::printf("status: %d\n", response.status_code);
        for (const auto &header : response.headers)
            std::printf("%s: %s\n", header.name.c_str(), header.value.c_str());
        std::printf("\n%s", response.body.c_str());
    } catch (const std::exception &err) {
        std::fprintf(stderr, "request failed: %s\n", err.what());
        curl_global_cleanup();
        return 1;
    }

    curl_global_cleanup();
    return 0;
}
