#include "realurl/buffer_pool.hpp"
#include "test_harness.hpp"

#include <memory>
#include <vector>

int main()
{
    realurl::BufferPool pool(4);
    CHECK(pool.capacity() == 4);
    CHECK(pool.available() == 4);
    CHECK(realurl::BufferPool::kBufferSize == 65536);

    std::vector<realurl::BufferPool::Buffer> buffers;
    buffers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        buffers.push_back(pool.borrow());
        CHECK(buffers.back() != nullptr);
    }
    CHECK(pool.available() == 0);

    buffers.pop_back();
    CHECK(pool.available() == 1);

    // Exhausted pool falls back to an emergency allocation.
    auto emergency = pool.borrow();
    CHECK(emergency != nullptr);
    CHECK(pool.available() == 0);

    return test::summary();
}
