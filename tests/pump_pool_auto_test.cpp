#include "realurl/stream_pump_pool.hpp"
#include "test_harness.hpp"

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>

namespace
{

using namespace std::chrono_literals;

void test_auto_capacity()
{
    ::unsetenv("REALURL_PUMP_THREADS");
    realurl::StreamPumpPool::configure(0);

    auto &pool = realurl::stream_pump_pool();
    CHECK(pool.capacity() >= 1);
    CHECK(pool.size() >= 1);
    CHECK(pool.size() <= pool.capacity());

    if (pool.capacity() <= pool.size())
        return;

    boost::asio::io_context io;
    std::atomic<int> started{ 0 };
    std::atomic<int> completed{ 0 };
    std::atomic<bool> release{ false };
    const int initial = static_cast<int>(pool.size());

    auto blocking_task = [&] {
        started.fetch_add(1, std::memory_order_release);
        while (!release.load(std::memory_order_acquire))
            std::this_thread::sleep_for(1ms);
        completed.fetch_add(1, std::memory_order_release);
    };

    for (int i = 0; i < initial; ++i)
        (void)pool.submit(blocking_task, io.get_executor());

    for (int i = 0; i < 2000 && started.load(std::memory_order_acquire) < initial; ++i)
        std::this_thread::sleep_for(1ms);
    CHECK(started.load(std::memory_order_acquire) == initial);

    (void)pool.submit(blocking_task, io.get_executor());
    CHECK(pool.size() == std::min(pool.capacity(), static_cast<std::size_t>(initial) + 1));

    release.store(true, std::memory_order_release);
    for (int i = 0; i < 2000 && completed.load(std::memory_order_acquire) < initial + 1; ++i)
        std::this_thread::sleep_for(1ms);
    CHECK(completed.load(std::memory_order_acquire) == initial + 1);
}

} // namespace

int main()
{
    test_auto_capacity();
    return test::summary();
}
