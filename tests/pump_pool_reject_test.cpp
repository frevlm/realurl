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

bool wait_for(const std::atomic<int> &value, int target)
{
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (value.load(std::memory_order_acquire) < target && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    return value.load(std::memory_order_acquire) >= target;
}

void test_queue_rejection()
{
    ::unsetenv("REALURL_PUMP_THREADS");
    ::unsetenv("REALURL_PUMP_QUEUE");
    realurl::StreamPumpPool::configure(1);
    realurl::StreamPumpPool::configure_queue(1);

    auto &pool = realurl::stream_pump_pool();
    CHECK(pool.capacity() == 1);
    CHECK(pool.queue_limit() == 1);
    CHECK(pool.size() == 1);

    boost::asio::io_context io;
    std::atomic<bool> release{ false };
    std::atomic<int> started{ 0 };
    std::atomic<int> completed{ 0 };

    auto blocking_task = [&] {
        started.fetch_add(1, std::memory_order_release);
        while (!release.load(std::memory_order_acquire))
            std::this_thread::sleep_for(1ms);
        completed.fetch_add(1, std::memory_order_release);
    };

    auto first = pool.submit(blocking_task, io.get_executor());
    CHECK(first != nullptr);
    CHECK(wait_for(started, 1));

    auto queued = pool.submit(blocking_task, io.get_executor());
    CHECK(queued != nullptr);

    auto rejected = pool.submit(blocking_task, io.get_executor());
    CHECK(rejected == nullptr);
    CHECK(pool.rejected() == 1);

    release.store(true, std::memory_order_release);
    CHECK(wait_for(completed, 2));
    CHECK(pool.size() == 1);
}

} // namespace

int main()
{
    test_queue_rejection();
    return test::summary();
}
