#include "realurl/rate_limit.hpp"
#include "test_harness.hpp"

int main()
{
    auto &limiter = realurl::upload_rate_limiter();

    limiter.configure(0, 5 * 1024 * 1024);
    CHECK(!limiter.enabled());
    CHECK(limiter.pipe_bps() == 0);

    limiter.configure(1000000, 5 * 1024 * 1024);
    CHECK(limiter.enabled());
    CHECK(limiter.pipe_bps() == 1000000);

    auto limited = limiter.join(10 * 1024 * 1024);
    CHECK(limited->desired_cap_bps() > 0);

    auto normal = limiter.join(1024);
    CHECK(normal->desired_cap_bps() == 0);

    limiter.tick(limited, 0);
    limiter.tick(normal, 0);
    limiter.tick_now();

    CHECK(limited->desired_cap_bps() > 0);
    CHECK(limited->desired_cap_bps() <= 1000000);

    limiter.finish(limited);
    limiter.finish(normal);

    limiter.configure(0, 5 * 1024 * 1024);
    CHECK(!limiter.enabled());

    return test::summary();
}
