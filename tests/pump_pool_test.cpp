#include "realurl/stream_pump_pool.hpp"
#include "test_harness.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

namespace
{

using namespace std::chrono_literals;

void test_completion()
{
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    std::atomic<int> completed{ 0 };

    auto gate = realurl::stream_pump_pool().submit(
        "127.0.0.1", [] { std::this_thread::sleep_for(10ms); }, io.get_executor());

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

void test_affinity()
{
    constexpr int kTasks = 64;
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    std::atomic<int> active{ 0 };
    std::atomic<int> peak{ 0 };
    std::atomic<int> completed{ 0 };
    boost::asio::steady_timer timeout(io, 5s);

    for (int i = 0; i < kTasks; ++i) {
        const bool same_ip = i < kTasks / 2;
        const std::string ip = same_ip ? "127.0.0.1" : "10.0.0." + std::to_string(i);

        auto gate = realurl::stream_pump_pool().submit(
            ip,
            [&] {
                const int now = ++active;
                int previous = peak.load();
                while (previous < now && !peak.compare_exchange_weak(previous, now)) {
                }
                std::this_thread::sleep_for(5ms);
                --active;
            },
            io.get_executor());

        gate->async_wait([&] {
            if (++completed == kTasks) {
                timeout.cancel();
                work.reset();
            }
        });
    }

    timeout.async_wait([&](const boost::system::error_code &) { work.reset(); });

    io.run();
    CHECK(completed.load() == kTasks);
    // Same-IP tasks are serialized by design, but distinct IPs can run in
    // parallel on the pool's worker threads.
    CHECK(peak.load() > 1);
}

} // namespace

int main()
{
    test_completion();
    test_affinity();
    return test::summary();
}
