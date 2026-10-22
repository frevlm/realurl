#include "realurl/detail/timer_wheel.hpp"

#include "test_harness.hpp"

#include <chrono>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

using Wheel = realurl::detail::TimerWheel;
using Node = realurl::detail::TimerNode;

void push_int(void *context)
{
    static_cast<std::vector<int> *>(context)->push_back(1);
}

void test_order_and_cascade()
{
    Wheel wheel;
    std::vector<int> fired;

    Node late;
    Node early;
    Node coarse;

    wheel.schedule(late, 30ms, [](void *context) {
        static_cast<std::vector<int> *>(context)->push_back(30);
    }, &fired);
    wheel.schedule(early, 10ms, [](void *context) {
        static_cast<std::vector<int> *>(context)->push_back(10);
    }, &fired);
    wheel.schedule(coarse, 100ms, [](void *context) {
        static_cast<std::vector<int> *>(context)->push_back(100);
    }, &fired);

    CHECK(wheel.active() == 3);

    const auto start = Wheel::Clock::now();
    wheel.advance(start + 50ms);
    CHECK(fired.size() == 2);
    if (fired.size() == 2) {
        CHECK(fired[0] == 10);
        CHECK(fired[1] == 30);
    }
    CHECK(wheel.active() == 1);

    wheel.advance(start + 200ms);
    CHECK(fired.size() == 3);
    if (fired.size() == 3)
        CHECK(fired[2] == 100);
    CHECK(wheel.active() == 0);
}

void test_immediate_and_next_deadline()
{
    Wheel wheel;
    std::vector<int> fired;

    Node node;
    wheel.schedule(node, 0ms, push_int, &fired);
    CHECK(wheel.next_deadline().has_value());

    wheel.advance(Wheel::Clock::now());
    CHECK(fired.size() == 1);
    CHECK(wheel.active() == 0);
    CHECK(!wheel.next_deadline().has_value());
}

void test_cancel()
{
    Wheel wheel;
    std::vector<int> fired;

    Node node;
    wheel.schedule(node, 10ms, push_int, &fired);
    wheel.cancel(node);
    CHECK(wheel.active() == 0);

    wheel.advance(Wheel::Clock::now() + 50ms);
    CHECK(fired.empty());
}

namespace
{

struct RepeatContext {
    Wheel *wheel = nullptr;
    Node node;
    int fired = 0;
};

void repeat_timer(void *context)
{
    auto *repeat = static_cast<RepeatContext *>(context);
    ++repeat->fired;
    if (repeat->fired < 3)
        repeat->wheel->schedule(repeat->node, 10ms, repeat_timer, repeat);
}

} // namespace

void test_reschedule_from_callback()
{
    Wheel wheel;
    RepeatContext context;
    context.wheel = &wheel;

    wheel.schedule(context.node, 10ms, repeat_timer, &context);

    for (int i = 0; i < 3; ++i) {
        std::this_thread::sleep_for(15ms);
        wheel.advance(Wheel::Clock::now());
        CHECK(context.fired == i + 1);
    }
    CHECK(wheel.active() == 0);
}

void test_many_timers()
{
    Wheel wheel;
    std::vector<Node> nodes(1000);
    std::vector<int> fired(1000, 0);

    for (std::size_t i = 0; i < nodes.size(); ++i) {
        wheel.schedule(nodes[i], std::chrono::milliseconds(static_cast<int>(i % 50)),
                       [](void *context) { *static_cast<int *>(context) = 1; }, &fired[i]);
    }
    CHECK(wheel.active() == nodes.size());

    wheel.advance(Wheel::Clock::now() + 100ms);
    CHECK(wheel.active() == 0);

    std::size_t count = 0;
    for (int value : fired)
        count += value;
    CHECK(count == nodes.size());
}

} // namespace

int main()
{
    test_order_and_cascade();
    test_immediate_and_next_deadline();
    test_cancel();
    test_reschedule_from_callback();
    test_many_timers();
    return test::summary();
}
