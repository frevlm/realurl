#include "realurl/detail/spill_queue.hpp"
#include "realurl/http2.hpp"
#include "realurl/server.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

using namespace std::chrono_literals;
using realurl::detail::SpillQueue;

namespace
{

constexpr std::uint64_t kBody = 4 * 1024 * 1024;
constexpr std::size_t kWrite = 64 * 1024;
constexpr int kStreams = 4;

int free_port()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    int port = -1;
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0 &&
        ::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) == 0)
        port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

/// pattern[k] == k % 251, long enough that any offset's next kWrite bytes are
/// pattern.substr(offset % 251, kWrite).
const std::string &pattern()
{
    static const std::string bytes = [] {
        std::string out(kWrite + 251, '\0');
        for (std::size_t i = 0; i < out.size(); ++i)
            out[i] = static_cast<char>(i % 251);
        return out;
    }();
    return bytes;
}

/// In-memory store that sleeps on every call, so offloaded reads really wait.
class SlowStore final : public realurl::SpillStore {
public:
    bool append(std::string_view bytes) override
    {
        std::this_thread::sleep_for(1ms);
        log_.append(bytes);
        return true;
    }

    bool read(std::span<char> out) override
    {
        std::this_thread::sleep_for(1ms);
        if (out.size() > log_.size() - head_)
            return false;
        std::memcpy(out.data(), log_.data() + head_, out.size());
        head_ += out.size();
        return true;
    }

    void reset() override
    {
        log_.clear();
        head_ = 0;
    }

private:
    std::string log_;
    std::size_t head_ = 0;
};

struct Run {
    realurl::Client *client = nullptr;
    realurl::EventLoop *loop = nullptr;
    std::string url;
    std::atomic<int> *sent = nullptr;
    int done = 0;
    int ok = 0;
    /// Streams that were still well behind when every response was sent.
    int behind = 0;
};

/// Reads the body in 16 KiB steps with a pause every 64 KiB, checking every
/// byte.
realurl::Task<void> slow_read(Run &run)
{
    bool ok = false;
    bool behind = false;
    try {
        realurl::Stream stream = co_await run.client->open("GET", run.url, {}, 10s);
        co_await stream.end(10s);
        const realurl::ResponseHead head = co_await stream.head(10s);
        ok = head.status == 200;
        std::uint64_t received = 0;
        std::size_t since_pause = 0;
        char buffer[16384];
        while (ok) {
            const std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, 10s);
            if (n == 0)
                break;
            ok = received + n <= kBody && std::memcmp(buffer, pattern().data() + received % 251, n) == 0;
            received += n;
            if (run.sent->load() >= kStreams && received < kBody / 2)
                behind = true;
            since_pause += n;
            if (since_pause >= 64 * 1024) {
                since_pause = 0;
                co_await run.loop->sleep_for(2ms);
            }
        }
        ok = ok && received == kBody;
    } catch (const std::exception &) {
        ok = false;
    }
    run.ok += ok ? 1 : 0;
    run.behind += behind ? 1 : 0;
    if (++run.done == kStreams + 1)
        run.loop->stop();
}

/// The buffered path: Client::get drains through read_all.
realurl::Task<void> buffered_get(Run &run)
{
    bool ok = false;
    try {
        const realurl::Response response = co_await run.client->get(run.url, 10s);
        ok = response.head.status == 200 && response.body.size() == kBody;
        for (std::uint64_t offset = 0; ok && offset < kBody; offset += kWrite)
            ok = std::memcmp(response.body.data() + offset, pattern().data() + offset % 251, kWrite) == 0;
    } catch (const std::exception &) {
        ok = false;
    }
    run.ok += ok ? 1 : 0;
    if (++run.done == kStreams + 1)
        run.loop->stop();
}

/// kStreams slow readers plus one buffered get against `config`. Returns the
/// number of slow readers that were still behind when the upstream finished.
int run_case(int port, std::atomic<int> &sent, realurl::Config config)
{
    sent = 0;
    realurl::EventLoop loop;
    config.protocol = realurl::Protocol::Native;
    config.max_conns_per_origin = 1;
    realurl::Client client(loop, config);
    Run run;
    run.client = &client;
    run.loop = &loop;
    run.url = "http://127.0.0.1:" + std::to_string(port) + "/big";
    run.sent = &sent;
    for (int i = 0; i < kStreams; ++i)
        loop.spawn(slow_read(run));
    loop.spawn(buffered_get(run));
    const realurl::detail::TimerId guard = loop.schedule(30s, [&] { loop.stop(); });
    loop.run();
    loop.cancel(guard);
    CHECK(run.done == kStreams + 1);
    CHECK(run.ok == kStreams + 1);
    client.close();
    return run.behind;
}

} // namespace

int main()
{
    const int port = free_port();
    CHECK(port > 0);

    std::atomic<int> sent{ 0 };
    realurl::Server server;
    server.set_io_threads(1);
    server.Get("/big", [&](const realurl::Server::Request &, realurl::Server::Response &response) {
        response.take_over_task([&](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            co_await writer.write_head(200, {}, kBody);
            for (std::uint64_t offset = 0; offset < kBody; offset += kWrite) {
                if (!co_await writer.write(std::string_view(pattern()).substr(offset % 251, kWrite)))
                    co_return;
            }
            co_await writer.end();
            ++sent;
        });
    });
    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    CHECK(server.is_running());

    {
        // No spilling: the backlog stays in memory and the peer is throttled.
        realurl::Config config;
        run_case(port, sent, config);
    }

    for (const bool offload : { false, true }) {
        // A 64 KiB threshold lets the upstream finish long before the slow
        // readers do.
        realurl::Config config;
        config.spill_threshold = 64 * 1024;
        config.spill_offload = offload;
        CHECK(run_case(port, sent, config) > 0);
        CHECK(SpillQueue::total_spilled() == 0);
    }

    {
        // A slow custom store behind the helper and the smallest window:
        // reads park until fetches land, and credit waits for the store.
        realurl::Config config;
        config.spill_threshold = 16 * 1024;
        config.spill_offload = true;
        config.spill_window = 64 * 1024;
        config.spill_store = [] { return std::make_unique<SlowStore>(); };
        run_case(port, sent, config);
        CHECK(SpillQueue::total_spilled() == 0);
    }

    for (const bool offload : { false, true }) {
        // A 1 MiB process-wide cap refuses most spills; streams throttle
        // instead and stay byte-exact.
        realurl::Config config;
        config.spill_threshold = 64 * 1024;
        config.spill_offload = offload;
        config.spill_total_limit = 1024 * 1024;
        run_case(port, sent, config);
        CHECK(SpillQueue::total_spilled() == 0);
    }

    server.stop();
    listener.join();
    return test::summary();
}
