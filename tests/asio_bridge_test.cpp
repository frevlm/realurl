#include "realurl/asio_bridge.hpp"

#include "support/h2_test_server.hpp"
#include "test_harness.hpp"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <string>
#include <thread>
#include <vector>

namespace
{

namespace net = boost::asio;
using namespace std::chrono_literals;
using realurl::test::H2TestRequest;
using realurl::test::H2TestResponse;
using realurl::test::H2TestServer;

std::string upstream_url(const H2TestServer &server, std::string_view path)
{
    return "http://127.0.0.1:" + std::to_string(server.port()) + std::string(path);
}

void test_single_request()
{
    H2TestServer upstream([](const H2TestRequest &) {
        H2TestResponse response;
        response.body = "upstream-body";
        return response;
    });

    realurl::ClientGroup group(1);
    const std::string url = upstream_url(upstream, "/single");

    net::io_context io;
    std::string body;
    std::exception_ptr error;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            try {
                auto response = co_await realurl::async_request(group, "GET", url);
                body = std::move(response.body);
            } catch (...) {
                error = std::current_exception();
            }
        },
        net::detached);
    io.run();

    if (error) {
        try {
            std::rethrow_exception(error);
        } catch (const std::exception &exception) {
            std::fprintf(stderr, "single request error: %s\n", exception.what());
        } catch (...) {
            std::fprintf(stderr, "single request error: unknown\n");
        }
    }
    CHECK(!error);
    CHECK(body == "upstream-body");
}

void test_concurrent_requests_do_not_block()
{
    H2TestServer upstream([](const H2TestRequest &) {
        H2TestResponse response;
        response.body = "x";
        response.delay = 200ms;
        return response;
    });

    realurl::ClientGroup group(1);
    const std::string url = upstream_url(upstream, "/concurrent");

    net::io_context io;
    constexpr int kRequests = 64;
    std::atomic<int> completed{ 0 };
    std::atomic<int> failed{ 0 };
    const auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kRequests; ++i) {
        net::co_spawn(
            io,
            [&]() -> net::awaitable<void> {
                try {
                    auto response = co_await realurl::async_request(group, "GET", url, {}, {}, 30s);
                    if (response.body == "x")
                        completed.fetch_add(1, std::memory_order_relaxed);
                    else
                        failed.fetch_add(1, std::memory_order_relaxed);
                } catch (...) {
                    failed.fetch_add(1, std::memory_order_relaxed);
                }
            },
            net::detached);
    }

    std::thread first([&] { io.run(); });
    std::thread second([&] { io.run(); });
    first.join();
    second.join();
    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    // Serialized on two io threads this takes 64/2 * 200 ms = 6.4 s.
    std::printf("asio bridge: %d concurrent requests in %.3fs\n", kRequests, elapsed);
    CHECK(completed.load() == kRequests);
    CHECK(failed.load() == 0);
    CHECK(elapsed < 2.0);
}

void test_sync_wait_has_no_poll_tail()
{
    H2TestServer upstream([](const H2TestRequest &) {
        H2TestResponse response;
        response.body = "sync";
        return response;
    });

    realurl::ClientGroup group(1);
    const std::string url = upstream_url(upstream, "/sync-wait");

    const auto start = std::chrono::steady_clock::now();
    auto response = realurl::sync_wait(group.request("GET", url, {}, {}, 5s));
    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    std::printf("sync_wait request in %.3fs\n", elapsed);
    CHECK(response.body == "sync");
    CHECK(elapsed < 0.8);
}

void test_error_propagates()
{
    realurl::ClientGroup group(1);

    net::io_context io;
    bool caught = false;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            try {
                auto response = co_await realurl::async_request(group, "GET", "http://127.0.0.1:1/", {}, {}, 500ms);
                (void)response;
            } catch (...) {
                caught = true;
            }
        },
        net::detached);
    io.run();

    CHECK(caught);
}

void test_completion_token()
{
    H2TestServer upstream([](const H2TestRequest &) {
        H2TestResponse response;
        response.body = "token-body";
        return response;
    });

    realurl::ClientGroup group(1);
    const std::string url = upstream_url(upstream, "/token");

    net::io_context io;
    bool called = false;
    std::string body;
    std::exception_ptr error;
    realurl::async_request(group, "GET", url, {}, {}, 30s,
                           net::bind_executor(io.get_executor(), [&](std::exception_ptr result, realurl::Response response) {
                               error = result;
                               body = std::move(response.body);
                               called = true;
                           }));
    io.run();

    CHECK(called);
    CHECK(!error);
    CHECK(body == "token-body");
}

} // namespace

int main()
{
    test_single_request();
    test_concurrent_requests_do_not_block();
    test_sync_wait_has_no_poll_tail();
    test_error_propagates();
    test_completion_token();
    return test::summary();
}
