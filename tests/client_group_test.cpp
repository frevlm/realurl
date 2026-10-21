#include "realurl/http2.hpp"
#include "support/h2_test_server.hpp"
#include "test_harness.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace
{

std::string url_for(const realurl::test::H2TestServer &server, std::string_view path)
{
    return "http://127.0.0.1:" + std::to_string(server.port()) + std::string(path);
}

realurl::Task<void> group_request(realurl::ClientGroup &group, const std::string &url, int index,
                                  std::vector<std::string> &bodies, int &remaining, int &failures,
                                  realurl::EventLoop &loop)
{
    try {
        realurl::Response response = co_await group.get(url + "?i=" + std::to_string(index), 5s);
        bodies[static_cast<std::size_t>(index)] = std::move(response.body);
    } catch (const std::exception &error) {
        if (failures == 0)
            std::fprintf(stderr, "first error: %s\n", error.what());
        ++failures;
    }
    if (--remaining == 0)
        loop.stop();
}

} // namespace

int main()
{
    realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &request) {
        realurl::test::H2TestResponse response;
        response.body = request.path;
        return response;
    });

    realurl::EventLoop loop;
    realurl::Config config;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = 32;
    config.connect_timeout = 2s;
    realurl::ClientGroup group(4, config);
    CHECK(group.workers() == 4);

    constexpr int kRequests = 128;
    std::vector<std::string> bodies(kRequests);
    int remaining = kRequests;
    int failures = 0;
    const std::string url = url_for(server, "/group");

    for (int i = 0; i < kRequests; ++i)
        loop.spawn(group_request(group, url, i, bodies, remaining, failures, loop));
    loop.run();

    CHECK(failures == 0);
    for (int i = 0; i < kRequests; ++i) {
        const std::string expected = "/group?i=" + std::to_string(i);
        CHECK(bodies[static_cast<std::size_t>(i)] == expected);
    }
    CHECK(server.connections() >= 2);

    return test::summary();
}
