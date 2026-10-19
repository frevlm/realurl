#include "realurl/fd_writer.hpp"
#include "test_harness.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace
{

using namespace std::chrono_literals;

std::string read_exactly(int fd, size_t size)
{
    std::string out;
    std::array<char, 64> buffer{};
    while (out.size() < size) {
        const ssize_t n = ::read(fd, buffer.data(), buffer.size());
        if (n <= 0)
            break;
        out.append(buffer.data(), static_cast<size_t>(n));
    }
    return out;
}

} // namespace

int main()
{
    int fds[2] = { -1, -1 };
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    CHECK(realurl::send_all_fd(fds[0], "hello", 1s));
    CHECK(read_exactly(fds[1], 5) == "hello");

    auto started = std::make_shared<std::atomic_bool>(false);
    auto writer = realurl::client_writer_from_fd(fds[0], 1s, started);
    CHECK(writer("world"));
    CHECK(started->load());
    CHECK(read_exactly(fds[1], 5) == "world");

    CHECK(::close(fds[1]) == 0);
    CHECK(!writer("after-close"));

    CHECK(::close(fds[0]) == 0);
    return test::summary();
}
