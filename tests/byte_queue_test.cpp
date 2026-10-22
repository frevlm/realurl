#include "realurl/detail/byte_queue.hpp"
#include "test_harness.hpp"

#include <string>

using realurl::detail::ByteQueue;

int main()
{
    {
        ByteQueue queue;
        CHECK(queue.empty());
        queue.append("hello ");
        queue.append("world");
        CHECK(queue.size() == 11);
        CHECK(queue.front(5) == "hello");
        CHECK(queue.front(100) == "hello world");

        queue.consume(6);
        CHECK(queue.size() == 5);
        CHECK(queue.front(100) == "world");

        char buffer[3];
        CHECK(queue.read(buffer) == 3);
        CHECK(std::string(buffer, 3) == "wor");
        CHECK(queue.read(buffer) == 2);
        CHECK(std::string(buffer, 2) == "ld");
        CHECK(queue.empty());
        CHECK(queue.read(buffer) == 0);
    }

    {
        // A dead prefix at least as large as the live bytes is compacted on
        // the next append; byte order is preserved across the move.
        ByteQueue queue;
        queue.append("abcdef");
        queue.consume(4);
        queue.append("gh");
        CHECK(queue.size() == 4);
        CHECK(queue.front(100) == "efgh");
    }

    {
        ByteQueue queue;
        std::string out = "head:";
        queue.append("0123456789");
        queue.consume(2);
        queue.drain_to(out);
        CHECK(out == "head:23456789");
        CHECK(queue.empty());

        queue.append("again");
        CHECK(queue.front(100) == "again");
    }

    {
        // Large payloads round-trip; a drained queue starts over.
        ByteQueue queue;
        const std::string big(256 * 1024, 'x');
        queue.append(big);
        queue.append("tail");
        queue.consume(big.size());
        CHECK(queue.front(100) == "tail");
        queue.clear();
        CHECK(queue.empty());
        queue.append("y");
        CHECK(queue.size() == 1);
    }

    return test::summary();
}
