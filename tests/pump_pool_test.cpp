#include "realurl/stream_pump_pool.hpp"
#include "test_harness.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>

namespace
{

using namespace std::chrono_literals;

void test_configured_size()
{
    ::setenv("REALURL_PUMP_THREADS", "1", 1);
    realurl::StreamPumpPool::configure(3);
    CHECK(realurl::stream_pump_pool().capacity() == 3);
    CHECK(realurl::stream_pump_pool().size() >= 1);
    CHECK(realurl::stream_pump_pool().size() <= 3);
}

void test_completion()
{
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    std::atomic<int> completed{ 0 };

    auto gate = realurl::stream_pump_pool().submit(
        [] { std::this_thread::sleep_for(10ms); }, io.get_executor());

    boost::asio::steady_timer timeout(io, 2s);
    gate->async_wait([&] {
        ++completed;
        timeout.cancel();
        work.reset();
    });

    timeout.async_wait([&](const boost::system::error_code &) { work.reset(); });

    io.run();
    CHECK(completed.load() == 1);
}

void test_concurrency()
{
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    std::atomic<int> active{ 0 };
    std::atomic<int> peak{ 0 };
    std::atomic<int> completed{ 0 };
    boost::asio::steady_timer timeout(io, 2s);

    for (int i = 0; i < 4; ++i) {
        auto gate = realurl::stream_pump_pool().submit(
            [&] {
                const int now = ++active;
                int previous = peak.load();
                while (previous < now && !peak.compare_exchange_weak(previous, now)) {
                }
                std::this_thread::sleep_for(20ms);
                --active;
            },
            io.get_executor());

        gate->async_wait([&] {
            if (++completed == 4) {
                timeout.cancel();
                work.reset();
            }
        });
    }

    timeout.async_wait([&](const boost::system::error_code &) { work.reset(); });
    io.run();

    CHECK(completed.load() == 4);
    CHECK(peak.load() > 1);
    CHECK(realurl::stream_pump_pool().size() > 1);
    CHECK(realurl::stream_pump_pool().size() <= realurl::stream_pump_pool().capacity());
}

} // namespace

int main()
{
    test_configured_size();
    test_completion();
    test_concurrency();
    return test::summary();
}
