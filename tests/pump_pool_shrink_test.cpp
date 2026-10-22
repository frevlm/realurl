#include "realurl/stream_pump_pool.hpp"
#include "test_harness.hpp"

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <thread>

namespace
{

using namespace std::chrono_literals;

bool wait_for(const std::atomic<int> &value, int target)
{
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (value.load(std::memory_order_acquire) < target && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    return value.load(std::memory_order_acquire) >= target;
}

bool wait_for_size(const realurl::StreamPumpPool &pool, std::size_t target)
{
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (pool.size() > target && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    return pool.size() <= target;
}

void test_idle_shrink()
{
    realurl::StreamPumpPool::configure(64);
    realurl::StreamPumpPool::configure_queue(64);
    realurl::StreamPumpPool::configure_idle(100ms);

    auto &pool = realurl::stream_pump_pool();
    CHECK(pool.idle_timeout() == 100ms);

    const auto initial = pool.size();
    if (initial >= pool.capacity())
        return;

    boost::asio::io_context io;
    std::atomic<bool> release{ false };
    std::atomic<int> started{ 0 };
    std::atomic<int> completed{ 0 };
    const int tasks = static_cast<int>(initial) + 2;

    auto blocking_task = [&] {
        started.fetch_add(1, std::memory_order_release);
        while (!release.load(std::memory_order_acquire))
            std::this_thread::sleep_for(1ms);
        completed.fetch_add(1, std::memory_order_release);
    };

    for (int i = 0; i < tasks; ++i)
        (void)pool.submit(blocking_task, io.get_executor());

    CHECK(wait_for(started, tasks));
    CHECK(pool.size() == static_cast<std::size_t>(tasks));
    CHECK(pool.rejected() == 0);

    release.store(true, std::memory_order_release);
    CHECK(wait_for(completed, tasks));
    CHECK(wait_for_size(pool, initial));
}

} // namespace

int main()
{
    test_idle_shrink();
    return test::summary();
}
