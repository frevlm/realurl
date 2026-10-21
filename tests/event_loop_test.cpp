#include "realurl/detail/event_loop.hpp"
#include "test_harness.hpp"

#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace
{

realurl::Task<int> answer()
{
    co_return 42;
}

realurl::Task<int> add_one(int value)
{
    co_return value + 1;
}

realurl::Task<int> nested()
{
    co_return co_await add_one(41);
}

realurl::Task<void> fail()
{
    throw std::runtime_error("boom");
    co_return;
}

} // namespace

int main()
{
    CHECK(realurl::sync_wait(answer()) == 42);
    CHECK(realurl::sync_wait(nested()) == 42);

    bool caught = false;
    try {
        realurl::sync_wait(fail());
    } catch (const std::runtime_error &error) {
        caught = std::string(error.what()) == "boom";
    }
    CHECK(caught);

    {
        realurl::EventLoop loop;
        int count = 0;
        auto poster = [&]() -> realurl::Task<void> {
            for (int i = 0; i < 10000; ++i) {
                loop.post([&] {
                    if (++count == 10000)
                        loop.stop();
                });
            }
            co_return;
        };
        loop.spawn(poster());
        loop.run();
        CHECK(count == 10000);
    }

    {
        realurl::EventLoop loop;
        std::vector<int> order;
        loop.schedule(20ms, [&] {
            order.push_back(2);
            loop.stop();
        });
        loop.schedule(5ms, [&] { order.push_back(1); });
        loop.run();
        CHECK(order.size() == 2);
        if (order.size() == 2) {
            CHECK(order[0] == 1);
            CHECK(order[1] == 2);
        }
    }

    return test::summary();
}
