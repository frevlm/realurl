#include "realurl/detail/resolver.hpp"
#include "realurl/detail/socket.hpp"
#include "test_harness.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::chrono_literals;
using namespace realurl::detail;

int main()
{
    {
        EventLoop loop;
        TcpListener listener(loop, 0);
        const std::uint16_t port = listener.port();
        CHECK(port != 0);

        std::string received;
        bool failed = false;
        std::string error_message;

        auto server_task = [&]() -> Task<void> {
            try {
                auto [fd, address] = co_await listener.accept();
                (void)address;
                TcpStream stream(loop, fd);
                char buffer[64];
                const std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, 2s);
                co_await stream.write_all({ buffer, n }, 2s);
                co_await stream.shutdown_write();
            } catch (const std::exception &error) {
                failed = true;
                error_message = error.what();
            }
        };
        loop.spawn(server_task());

        auto client_task = [&]() -> Task<void> {
            try {
                Resolver resolver(loop);
                const SocketAddress address = co_await resolver.resolve("127.0.0.1", port);
                TcpStream client =
                    co_await TcpStream::connect(loop, reinterpret_cast<const sockaddr *>(&address.storage),
                                                address.length, 2s);
                co_await client.write_all("hello", 2s);
                char buffer[64];
                const std::size_t n = co_await client.read({ buffer, sizeof(buffer) }, 2s);
                received.assign(buffer, n);
            } catch (const std::exception &error) {
                failed = true;
                error_message = error.what();
            }
            loop.stop();
        };
        loop.spawn(client_task());

        loop.run();
        CHECK(!failed);
        if (failed)
            std::fprintf(stderr, "echo failed: %s\n", error_message.c_str());
        CHECK(received == "hello");
    }

    {
        int pair[2] = { -1, -1 };
        CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0);
        set_nonblocking(pair[0]);
        set_nonblocking(pair[1]);

        EventLoop loop;
        TcpStream stream(loop, pair[0]);
        bool timed_out = false;

        auto timeout_task = [&]() -> Task<void> {
            char buffer[1];
            try {
                (void)co_await stream.read({ buffer, 1 }, 50ms);
            } catch (const std::system_error &error) {
                timed_out = error.code().value() == ETIMEDOUT;
            }
            loop.stop();
        };
        loop.spawn(timeout_task());

        loop.run();
        ::close(pair[1]);
        CHECK(timed_out);
    }

    {
        EventLoop loop;
        Resolver resolver(loop);
        bool resolved = false;
        auto resolve_task = [&]() -> Task<void> {
            const SocketAddress address = co_await resolver.resolve("localhost", 80);
            resolved = address.length > 0;
            loop.stop();
        };
        loop.spawn(resolve_task());
        loop.run();
        CHECK(resolved);
    }

    {
        EventLoop loop;
        ListenerOptions options;
        options.host = "127.0.0.1";
        options.port = 0;
        options.reuse_port = true;

        TcpListener first(loop, options);
        CHECK(first.port() != 0);
        CHECK(first.family() == AF_INET);

        options.port = first.port();
        TcpListener second(loop, options);
        CHECK(second.port() == first.port());
    }

    {
        EventLoop loop;
        ListenerOptions options;
        options.host = "::1";
        options.port = 0;
        try {
            TcpListener listener(loop, options);
            CHECK(listener.family() == AF_INET6);
            CHECK(listener.port() != 0);
        } catch (const std::exception &) {
            // IPv6 may be unavailable in constrained containers.
        }
    }

    return test::summary();
}
