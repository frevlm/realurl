#include "realurl/detail/event_loop.hpp"

#include "test_harness.hpp"

#include <cstdio>

int main()
{
#ifdef REALURL_WITH_IO_URING
    realurl::EventLoop loop;
    realurl::detail::Uring *ring = loop.uring();
    const bool multishot = loop.uring_multishot_available();
    std::printf("uring: available=%d multishot=%d\n", ring != nullptr ? 1 : 0,
                multishot ? 1 : 0);
    if (ring == nullptr)
        std::printf("uring: unavailable in this sandbox; readiness fallback active\n");
    if (ring != nullptr && !multishot)
        std::printf("uring: provided-buffer ring unavailable; one-shot recv fallback active\n");
#else
    std::printf("uring: disabled at build time\n");
#endif
    return test::summary();
}
