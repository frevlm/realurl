#include "realurl/http2.hpp"
#include "support/h2_test_server.hpp"
#include "test_harness.hpp"

#ifndef REALURL_CERT_DIR
#define REALURL_CERT_DIR "../../tests/certs"
#endif

#include <chrono>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

namespace
{

std::string url_for(const realurl::test::H2TestServer &server, std::string_view path)
{
    return "http://127.0.0.1:" + std::to_string(server.port()) + std::string(path);
}

/// Keeps the coroutine closure alive until run() returns. A temporary lambda
/// passed straight to spawn() would leave the coroutine's `this` dangling.
template <class F>
void run_task(realurl::EventLoop &loop, F closure)
{
    auto task = std::move(closure);
    loop.spawn(task());
    loop.run();
}

realurl::Task<void> mux_one(realurl::Client &client, const realurl::test::H2TestServer &server, int index,
                            std::vector<std::string> &bodies, int &remaining, int &failures,
                            realurl::EventLoop &loop)
{
    try {
        const std::string path = "/mux/" + std::to_string(index);
        realurl::Response response = co_await client.get(url_for(server, path), 2s);
        bodies[static_cast<std::size_t>(index)] = response.body;
    } catch (...) {
        ++failures;
    }
    if (--remaining == 0)
        loop.stop();
}

} // namespace

int main()
{
    {
        realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &request) {
            realurl::test::H2TestResponse response;
            response.status = 200;
            response.body = "hello " + request.path;
            response.headers.push_back({ "x-test", "yes" });
            return response;
        });

        realurl::EventLoop loop;
        realurl::Client client(loop);

        int status = 0;
        std::string body;
        std::string error;
        const std::string url = url_for(server, "/x");

        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                realurl::Response response = co_await client.get(url, 2s);
                status = response.head.status;
                body = response.body;
            } catch (const std::exception &exception) {
                error = exception.what();
            }
            loop.stop();
        });

        CHECK(error.empty());
        if (!error.empty())
            std::fprintf(stderr, "get failed: %s\n", error.c_str());
        CHECK(status == 200);
        CHECK(body == "hello /x");
    }

    {
        realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &request) {
            realurl::test::H2TestResponse response;
            response.body = request.body;
            return response;
        });

        realurl::EventLoop loop;
        realurl::Client client(loop);

        std::string body;
        std::string error;
        const std::string url = url_for(server, "/echo");

        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                realurl::Headers headers = { { "content-type", "text/plain" } };
                realurl::Response response =
                    co_await client.request("POST", url, std::move(headers), "ping", 2s);
                body = response.body;
            } catch (const std::exception &exception) {
                error = exception.what();
            }
            loop.stop();
        });

        CHECK(error.empty());
        CHECK(body == "ping");
    }

    {
        realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &request) {
            realurl::test::H2TestResponse response;
            response.body = request.path;
            return response;
        });

        realurl::EventLoop loop;
        realurl::Client client(loop);
        const std::string url = url_for(server, "/warmup");

        const int count = 10;
        std::vector<std::string> bodies(static_cast<std::size_t>(count));
        int remaining = count;
        int failures = 0;
        int status = 0;

        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                (void)co_await client.get(url, 2s);
                status = 200;
            } catch (...) {
                ++failures;
            }

            for (int i = 0; i < count; ++i)
                loop.spawn(mux_one(client, server, i, bodies, remaining, failures, loop));
        });

        CHECK(status == 200);
        CHECK(failures == 0);
        for (int i = 0; i < count; ++i) {
            const std::string expected = "/mux/" + std::to_string(i);
            CHECK(bodies[static_cast<std::size_t>(i)] == expected);
        }
        CHECK(server.connections() == 1);
    }

    {
        realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &request) {
            realurl::test::H2TestResponse response;
            response.body = request.body;
            return response;
        });

        realurl::EventLoop loop;
        realurl::Client client(loop);
        const std::string url = url_for(server, "/duplex");

        int status = 0;
        std::string body;
        std::string error;

        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                realurl::Stream stream = co_await client.open("POST", url, {}, 2s);
                co_await stream.write("hello", 2s);
                co_await stream.write(" world", 2s);
                co_await stream.end(2s);
                const realurl::ResponseHead head = co_await stream.head(2s);
                status = head.status;
                char buffer[32];
                for (;;) {
                    const std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, 2s);
                    if (n == 0)
                        break;
                    body.append(buffer, n);
                }
            } catch (const std::exception &exception) {
                error = exception.what();
            }
            loop.stop();
        });

        CHECK(error.empty());
        if (!error.empty())
            std::fprintf(stderr, "duplex failed: %s\n", error.c_str());
        CHECK(status == 200);
        CHECK(body == "hello world");
    }

    {
        realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &) {
            realurl::test::H2TestResponse response;
            response.body = "late";
            response.delay = 200ms;
            return response;
        });

        realurl::EventLoop loop;
        realurl::Client client(loop);
        const std::string url = url_for(server, "/slow");

        bool timed_out = false;
        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                realurl::Stream stream = co_await client.open("GET", url, {}, 2s);
                (void)co_await stream.head(50ms);
            } catch (const std::system_error &exception) {
                timed_out = exception.code().value() == ETIMEDOUT;
            } catch (...) {
            }
            loop.stop();
        });

        CHECK(timed_out);
    }

    {
        const std::string large(256 * 1024, 'x');
        realurl::test::H2TestServer server([large](const realurl::test::H2TestRequest &) {
            realurl::test::H2TestResponse response;
            response.body = large;
            return response;
        });

        realurl::EventLoop loop;
        realurl::Client client(loop);
        const std::string url = url_for(server, "/large");

        std::size_t received = 0;
        std::string error;

        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                realurl::Response response = co_await client.get(url, 5s);
                received = response.body.size();
            } catch (const std::exception &exception) {
                error = exception.what();
            }
            loop.stop();
        });

        CHECK(error.empty());
        CHECK(received == large.size());
    }

    {
        realurl::test::H2TlsTestServer server(REALURL_CERT_DIR "/localhost.pem",
                                              REALURL_CERT_DIR "/localhost-key.pem",
                                              [](const realurl::test::H2TestRequest &request) {
                                                  realurl::test::H2TestResponse response;
                                                  response.body = "tls " + request.path;
                                                  return response;
                                              });

        realurl::EventLoop loop;
        realurl::Config config;
        config.ca_file = REALURL_CERT_DIR "/localhost.pem";
        realurl::Client client(loop, config);
        const std::string url = "https://localhost:" + std::to_string(server.port()) + "/tls";

        int status = 0;
        std::string body;
        std::string error;

        run_task(loop, [&]() -> realurl::Task<void> {
            try {
                realurl::Response response = co_await client.get(url, 5s);
                status = response.head.status;
                body = response.body;
            } catch (const std::exception &exception) {
                error = exception.what();
            }
            loop.stop();
        });

        CHECK(error.empty());
        if (!error.empty())
            std::fprintf(stderr, "tls get failed: %s\n", error.c_str());
        CHECK(status == 200);
        CHECK(body == "tls /tls");
    }

    return test::summary();
}
