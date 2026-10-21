#include "realurl/body_feeder.hpp"
#include "realurl/buffer_pool.hpp"
#include "test_harness.hpp"

#include <chrono>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace
{

bool wait_for_chunk(realurl::BodySource &source, std::string &out, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto result = source.read();
        if (!result.chunk.empty()) {
            out.append(result.chunk);
            source.consume(result.chunk.size());
            return true;
        }
        if (result.eof)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}

void test_initial_bytes()
{
    realurl::BufferPool pool(4);
    auto feeder = std::make_shared<realurl::BodyFeeder>(&pool);
    auto source = feeder->make_source();

    feeder->feed_initial("hello");
    auto result = source->read();
    CHECK(result.chunk == "hello");
    source->consume(result.chunk.size());
    CHECK(feeder->total_bytes.load() == 5);

    feeder->cancel();
    auto after_cancel = source->read();
    CHECK(after_cancel.eof);
}

void test_socket_feed()
{
    int fds[2] = { -1, -1 };
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    realurl::BufferPool pool(4);
    auto feeder = std::make_shared<realurl::BodyFeeder>(&pool);
    auto source = feeder->make_source();

    std::thread reader([feeder, fd = fds[0]] { feeder->run(fd); });

    const std::string payload = "streamed-body";
    CHECK(::write(fds[1], payload.data(), payload.size()) == static_cast<ssize_t>(payload.size()));
    CHECK(::close(fds[1]) == 0);

    std::string received;
    CHECK(wait_for_chunk(*source, received, std::chrono::seconds(2)));
    CHECK(received == payload);

    reader.join();
    CHECK(::close(fds[0]) == 0);
    CHECK(feeder->total_bytes.load() == static_cast<long long>(payload.size()));
}

void test_body_limit()
{
    realurl::BufferPool pool(4);
    auto feeder = std::make_shared<realurl::BodyFeeder>(&pool);
    auto source = feeder->make_source();
    feeder->set_body_limit(4);

    feeder->feed_initial("12345");
    CHECK(feeder->limit_exceeded());
    auto result = source->read();
    CHECK(result.chunk.empty());
    CHECK(!result.eof);
}

void test_idle_timeout()
{
    int fds[2] = { -1, -1 };
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    realurl::BufferPool pool(4);
    auto feeder = std::make_shared<realurl::BodyFeeder>(&pool);
    feeder->set_idle_timeout(std::chrono::milliseconds(250));
    auto source = feeder->make_source();

    std::thread reader([feeder, fd = fds[0]] { feeder->run(fd); });

    // Leave socket open without sending anything: feeder should detect inactivity and terminate!
    reader.join();

    auto result = source->read();
    CHECK(result.eof);

    CHECK(::close(fds[0]) == 0);
    CHECK(::close(fds[1]) == 0);
}

} // namespace

int main()
{
    test_initial_bytes();
    test_socket_feed();
    test_body_limit();
    test_idle_timeout();
    return test::summary();
}
