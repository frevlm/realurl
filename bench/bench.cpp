#include "resource.hpp"
#include "support/h2_test_server.hpp"
#include "realurl/asio_bridge.hpp"
#include "realurl/detail/tls.hpp"
#include "realurl/http2.hpp"
#include "realurl/proxy.hpp"
#include "realurl/server.hpp"
#include "realurl/stream_pump_pool.hpp"

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

namespace net = boost::asio;

double megabytes(std::uint64_t bytes)
{
    return static_cast<double>(bytes) / (1024.0 * 1024.0);
}

std::vector<double> percentiles(std::vector<double> values)
{
    std::sort(values.begin(), values.end());
    if (values.empty())
        return {};
    const auto pick = [&](double p) {
        const std::size_t index = static_cast<std::size_t>(p / 100.0 * static_cast<double>(values.size() - 1));
        return values[index];
    };
    return { pick(50), pick(90), pick(95), pick(99), pick(99.9), values.back() };
}

/// Fixed-size worker: claims one request quota at a time, records per-request
/// latency, and stops the loop once the quota is exhausted.
realurl::Task<void> bench_worker(realurl::Client &client, const std::string &url, std::atomic<int> &remaining,
                                 std::vector<double> &latencies_ms, realurl::EventLoop &loop)
{
    std::vector<double> local;
    for (;;) {
        int expected = remaining.load(std::memory_order_relaxed);
        bool claimed = false;
        while (expected > 0) {
            if (remaining.compare_exchange_weak(expected, expected - 1, std::memory_order_relaxed)) {
                claimed = true;
                break;
            }
        }
        if (!claimed) {
            latencies_ms.insert(latencies_ms.end(), local.begin(), local.end());
            loop.stop();
            co_return;
        }

        const auto start = std::chrono::steady_clock::now();
        (void)co_await client.get(url, 10s);
        local.push_back(
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
    }
}

struct MtBenchState {
    /// One quota per worker. Only that worker's loop thread touches it, so the
    /// per-request claim is a plain decrement instead of a contended CAS.
    std::vector<int> remaining;
    std::atomic<int> requesters{ 0 };
    std::atomic<int> failures{ 0 };
    std::vector<std::vector<double>> latencies;
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
};

realurl::Task<void> mt_requester(realurl::ClientGroup &group, const std::string &url, MtBenchState &state, int slot)
{
    std::vector<double> local;
    for (;;) {
        if (state.remaining[static_cast<std::size_t>(slot)] <= 0)
            break;
        --state.remaining[static_cast<std::size_t>(slot)];

        const auto start = std::chrono::steady_clock::now();
        try {
            (void)co_await group.get(url, 10s);
        } catch (...) {
            state.failures.fetch_add(1, std::memory_order_relaxed);
        }
        local.push_back(
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
    }

    auto &target = state.latencies[static_cast<std::size_t>(slot)];
    target.insert(target.end(), local.begin(), local.end());

    if (state.requesters.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        std::lock_guard<std::mutex> lock(state.mutex);
        state.done = true;
        state.cv.notify_all();
    }
}

realurl::Task<void> mt_worker(realurl::ClientGroup &group, const std::string &url, MtBenchState &state, int slot,
                              int window)
{
    for (int i = 0; i < window; ++i)
        realurl::EventLoop::current().spawn(mt_requester(group, url, state, slot));
    co_return;
}

realurl::Task<void> mt_warmup(realurl::ClientGroup &group, const std::string &url, std::atomic<int> &warmed,
                              int target, std::mutex &mutex, std::condition_variable &cv)
{
    try {
        (void)co_await group.get(url, 10s);
    } catch (...) {
    }
    if (warmed.fetch_add(1, std::memory_order_acq_rel) + 1 == target) {
        std::lock_guard<std::mutex> lock(mutex);
        cv.notify_all();
    }
    co_return;
}

int run_requests_mt(int total, int concurrency, int workers, int connections = 1)
{
    total = std::max(1, total);
    concurrency = std::max(1, concurrency);
    workers = std::max(1, workers);
    connections = std::max(1, connections);

    realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &) {
        realurl::test::H2TestResponse response;
        response.body = "bench";
        return response;
    });

    realurl::Config config;
    config.max_conns_per_origin = connections;
    config.max_streams_per_conn =
        std::max(1, (concurrency + workers * connections - 1) / (workers * connections));
    realurl::ClientGroup group(static_cast<unsigned>(workers), config);
    const std::string url = "http://127.0.0.1:" + std::to_string(server.port()) + "/bench";

    // Open one pooled connection per worker before the timed burst.
    std::atomic<int> warmed{ 0 };
    std::mutex warm_mutex;
    std::condition_variable warm_cv;
    const int warm_target = workers * connections;
    for (int i = 0; i < warm_target; ++i)
        group.spawn_on(static_cast<std::size_t>(i % workers),
                       mt_warmup(group, url, warmed, warm_target, warm_mutex, warm_cv));
    {
        std::unique_lock<std::mutex> lock(warm_mutex);
        warm_cv.wait_for(lock, 30s, [&] { return warmed.load(std::memory_order_acquire) >= warm_target; });
    }

    MtBenchState state;
    state.remaining.assign(static_cast<std::size_t>(workers), total / workers);
    for (int i = 0; i < total % workers; ++i)
        ++state.remaining[static_cast<std::size_t>(i)];
    state.latencies.resize(static_cast<std::size_t>(workers));
    const int window = std::max(1, concurrency / workers);
    state.requesters.store(workers * window);
    const bench::CpuTime cpu_start = bench::cpu_time();
    const auto wall_start = std::chrono::steady_clock::now();

    for (int i = 0; i < workers; ++i)
        group.spawn_on(static_cast<std::size_t>(i), mt_worker(group, url, state, i, window));

    {
        std::unique_lock<std::mutex> lock(state.mutex);
        state.cv.wait(lock, [&] { return state.done; });
    }
    const auto wall_end = std::chrono::steady_clock::now();
    const bench::CpuTime cpu_end = bench::cpu_time();

    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<std::size_t>(total));
    for (auto &local : state.latencies)
        latencies_ms.insert(latencies_ms.end(), local.begin(), local.end());

    const double wall = std::chrono::duration<double>(wall_end - wall_start).count();
    const std::vector<double> pct = percentiles(std::move(latencies_ms));
    std::printf("mode=requests_mt requests=%d concurrency=%d workers=%d connections=%d\n", total, concurrency,
                workers, server.connections());
    std::printf("  throughput: %.0f req/s (wall %.2fs)\n", total / wall, wall);
    if (!pct.empty())
        std::printf("  latency ms: p50=%.3f p90=%.3f p95=%.3f p99=%.3f p99.9=%.3f max=%.3f\n", pct[0], pct[1],
                    pct[2], pct[3], pct[4], pct[5]);
    std::printf("  failures: %d\n", state.failures.load(std::memory_order_relaxed));
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (%.3f ms/req)\n", cpu_end.user - cpu_start.user,
                cpu_end.system - cpu_start.system, cpu_end.total() - cpu_start.total(),
                (cpu_end.total() - cpu_start.total()) * 1000.0 / total);
    std::printf("  rss: peak=%.1fMB current=%.1fMB\n", megabytes(bench::peak_rss_bytes()),
                megabytes(bench::current_rss_bytes()));
    return 0;
}

int run_requests(int total, int concurrency, int connections = 1)
{
    realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &) {
        realurl::test::H2TestResponse response;
        response.body = "bench";
        return response;
    });

    realurl::EventLoop loop;
    realurl::Config config;
    config.max_conns_per_origin = connections;
    config.max_streams_per_conn = (concurrency + connections - 1) / connections;
    realurl::Client client(loop, config);
    const std::string url = "http://127.0.0.1:" + std::to_string(server.port()) + "/bench";

    std::atomic<int> remaining{ total };
    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<std::size_t>(total));

    std::chrono::steady_clock::time_point wall_start{};
    bench::CpuTime cpu_start{};

    // One driver coroutine: warm up first (absorbing connection setup), then
    // spawn the workers and start the clocks.
    auto driver = [&]() -> realurl::Task<void> {
        (void)co_await client.get(url, 10s);
        wall_start = std::chrono::steady_clock::now();
        cpu_start = bench::cpu_time();

        for (int i = 0; i < concurrency; ++i)
            loop.spawn(bench_worker(client, url, remaining, latencies_ms, loop));
        co_return;
    };
    loop.spawn(driver());
    loop.run();

    const auto wall_start_copy = wall_start;
    const double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_start_copy).count();
    const bench::CpuTime cpu_end = bench::cpu_time();

    const std::vector<double> pct = percentiles(std::move(latencies_ms));
    std::printf("mode=requests requests=%d concurrency=%d connections=%d\n", total, concurrency,
                server.connections());
    std::printf("  throughput: %.0f req/s (wall %.2fs)\n", total / wall, wall);
    if (!pct.empty())
        std::printf("  latency ms: p50=%.3f p90=%.3f p95=%.3f p99=%.3f p99.9=%.3f max=%.3f\n", pct[0], pct[1],
                    pct[2], pct[3], pct[4], pct[5]);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (%.3f ms/req)\n", cpu_end.user - cpu_start.user,
                cpu_end.system - cpu_start.system, cpu_end.total() - cpu_start.total(),
                (cpu_end.total() - cpu_start.total()) * 1000.0 / total);
    std::printf("  rss: peak=%.1fMB current=%.1fMB\n", megabytes(bench::peak_rss_bytes()),
                megabytes(bench::current_rss_bytes()));
    return 0;
}


int free_port();

struct H2ServerBenchState
{
    realurl::ClientGroup *group = nullptr;
    std::string url;
    int total = 0;
    std::atomic<int> *next = nullptr;
    std::atomic<int> *done = nullptr;
    std::atomic<int> *failures = nullptr;
    std::vector<double> *latencies = nullptr;
};

realurl::Task<void> h2_server_bench_worker(H2ServerBenchState state)
{
    for (;;) {
        const int index = state.next->fetch_add(1, std::memory_order_relaxed);
        if (index >= state.total)
            break;
        const auto start = std::chrono::steady_clock::now();
        try {
            const realurl::Response response = co_await state.group->get(state.url);
            if (response.head.status != 200)
                state.failures->fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
            state.failures->fetch_add(1, std::memory_order_relaxed);
        }
        const auto end = std::chrono::steady_clock::now();
        (*state.latencies)[static_cast<std::size_t>(index)] =
            std::chrono::duration<double, std::milli>(end - start).count();
        state.done->fetch_add(1, std::memory_order_acq_rel);
    }
    co_return;
}

int run_h2_server_unary(int total, int concurrency, int workers, realurl::Protocol protocol)
{
    total = std::max(1, total);
    concurrency = std::max(1, concurrency);
    workers = std::max(1, workers);

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "h2_unary: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    server.set_io_threads(1);
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running()) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "h2_unary: server failed to start\n");
        return 1;
    }

    realurl::Config config;
    config.protocol = protocol;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = concurrency;
    realurl::ClientGroup group(static_cast<unsigned>(workers), config);
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + "/hello";

    try {
        (void)realurl::sync_wait(group.get(url));
    } catch (const std::exception &error) {
        std::fprintf(stderr, "h2_unary: warmup failed: %s\n", error.what());
        server.stop();
        listener.join();
        return 1;
    }

    std::atomic<int> next{ 0 };
    std::atomic<int> done{ 0 };
    std::atomic<int> failures{ 0 };
    std::vector<double> latencies(static_cast<std::size_t>(total));

    const std::uint64_t rss_before = bench::current_rss_bytes();
    const bench::CpuTime cpu_start = bench::cpu_time();
    const auto wall_start = std::chrono::steady_clock::now();

    H2ServerBenchState state{ &group, url, total, &next, &done, &failures, &latencies };
    for (int i = 0; i < concurrency; ++i)
        group.spawn(h2_server_bench_worker(state));

    while (done.load(std::memory_order_acquire) < total)
        std::this_thread::sleep_for(1ms);

    const auto wall_end = std::chrono::steady_clock::now();
    const bench::CpuTime cpu_end = bench::cpu_time();
    const double wall = std::chrono::duration<double>(wall_end - wall_start).count();

    const std::vector<double> pct = percentiles(latencies);
    const int failed = failures.load(std::memory_order_relaxed);

    std::printf("mode=h2_unary protocol=%s requests=%d concurrency=%d workers=%d\n",
                protocol == realurl::Protocol::Native ? "native" : "h2", total, concurrency, workers);
    std::printf("  status: %d ok, %d failed in %.2fs (%.0f req/s)\n", total - failed, failed, wall,
                total / wall);
    if (!pct.empty())
        std::printf("  latency ms: p50=%.3f p90=%.3f p95=%.3f p99=%.3f p99.9=%.3f max=%.3f\n",
                    pct[0], pct[1], pct[2], pct[3], pct[4], pct[5]);
    const double cpu_total = cpu_end.total() - cpu_start.total();
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (%.4f ms/req)\n",
                cpu_end.user - cpu_start.user, cpu_end.system - cpu_start.system, cpu_total,
                cpu_total * 1000.0 / total);
    std::printf("  rss: before=%.1fMB peak=%.1fMB current=%.1fMB\n", megabytes(rss_before),
                megabytes(bench::peak_rss_bytes()), megabytes(bench::current_rss_bytes()));
    const auto stats = server.stats();
    std::printf("  connections: native=%llu h2=%llu h1=%llu\n",
                static_cast<unsigned long long>(stats.native_connections),
                static_cast<unsigned long long>(stats.h2_connections),
                static_cast<unsigned long long>(stats.h1_connections));

    group.close();
    server.stop();
    listener.join();
    return failed == 0 ? 0 : 1;
}


struct H1BenchState
{
    int port = 0;
    int total = 0;
    std::atomic<int> *next = nullptr;
    std::atomic<int> *failures = nullptr;
    std::vector<double> *latencies = nullptr;
};

bool h1_read_response(int fd, std::string &buffer)
{
    for (;;) {
        const auto header_end = buffer.find("\r\n\r\n");
        if (header_end != std::string::npos) {
            std::size_t length = 0;
            const auto length_pos = buffer.find("Content-Length:");
            if (length_pos != std::string::npos && length_pos < header_end) {
                const std::size_t value_start = length_pos + 15;
                const auto value_end = buffer.find("\r\n", value_start);
                if (value_end != std::string::npos)
                    length = static_cast<std::size_t>(std::atoi(buffer.substr(value_start, value_end - value_start).c_str()));
            }
            if (buffer.size() >= header_end + 4 + length) {
                buffer.erase(0, header_end + 4 + length);
                return true;
            }
        }

        char temp[16384];
        const ssize_t n = ::recv(fd, temp, sizeof(temp), 0);
        if (n <= 0)
            return false;
        buffer.append(temp, static_cast<std::size_t>(n));
    }
}

void h1_bench_worker(H1BenchState state)
{
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(state.port));
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        state.failures->fetch_add(state.total, std::memory_order_relaxed);
        return;
    }
    if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        state.failures->fetch_add(state.total, std::memory_order_relaxed);
        return;
    }

    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    timeval timeout{ 10, 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    static constexpr std::string_view request =
        "GET /hello HTTP/1.1\r\nHost: bench\r\n\r\n";
    std::string buffer;

    for (;;) {
        const int index = state.next->fetch_add(1, std::memory_order_relaxed);
        if (index >= state.total)
            break;

        const auto start = std::chrono::steady_clock::now();
        std::size_t sent = 0;
        bool ok = true;
        while (sent < request.size()) {
            const ssize_t n = ::send(fd, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                ok = false;
                break;
            }
            sent += static_cast<std::size_t>(n);
        }
        if (!ok || !h1_read_response(fd, buffer)) {
            state.failures->fetch_add(1, std::memory_order_relaxed);
            break;
        }
        const auto end = std::chrono::steady_clock::now();
        (*state.latencies)[static_cast<std::size_t>(index)] =
            std::chrono::duration<double, std::milli>(end - start).count();
    }

    ::close(fd);
}

int run_h1_server_unary(int total, int concurrency, int threads)
{
    total = std::max(1, total);
    concurrency = std::max(1, concurrency);
    threads = std::max(1, threads);

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "h1_unary: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    server.set_io_threads(std::max(1, threads));
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running()) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "h1_unary: server failed to start\n");
        return 1;
    }

    std::atomic<int> next{ 0 };
    std::atomic<int> failures{ 0 };
    std::vector<double> latencies(static_cast<std::size_t>(total));
    H1BenchState state{ port, total, &next, &failures, &latencies };

    const std::uint64_t rss_before = bench::current_rss_bytes();
    const bench::CpuTime cpu_start = bench::cpu_time();
    const auto wall_start = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(threads));
    for (int i = 0; i < threads; ++i)
        workers.emplace_back([state] { h1_bench_worker(state); });
    for (auto &worker : workers)
        worker.join();

    const auto wall_end = std::chrono::steady_clock::now();
    const bench::CpuTime cpu_end = bench::cpu_time();
    const double wall = std::chrono::duration<double>(wall_end - wall_start).count();

    const std::vector<double> pct = percentiles(latencies);
    const int failed = failures.load(std::memory_order_relaxed);
    std::printf("mode=h1_unary requests=%d connections=%d\n", total, threads);
    std::printf("  status: %d ok, %d failed in %.2fs (%.0f req/s)\n", total - failed, failed, wall,
                total / wall);
    if (!pct.empty())
        std::printf("  latency ms: p50=%.3f p90=%.3f p95=%.3f p99=%.3f p99.9=%.3f max=%.3f\n",
                    pct[0], pct[1], pct[2], pct[3], pct[4], pct[5]);
    const double cpu_total = cpu_end.total() - cpu_start.total();
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (%.4f ms/req)\n",
                cpu_end.user - cpu_start.user, cpu_end.system - cpu_start.system, cpu_total,
                cpu_total * 1000.0 / total);
    std::printf("  rss: before=%.1fMB peak=%.1fMB current=%.1fMB\n", megabytes(rss_before),
                megabytes(bench::peak_rss_bytes()), megabytes(bench::current_rss_bytes()));

    server.stop();
    listener.join();
    return failed == 0 ? 0 : 1;
}

int run_hold(int stream_count)
{
    realurl::test::H2TestServer server([](const realurl::test::H2TestRequest &) {
        realurl::test::H2TestResponse response;
        response.hold = true;
        return response;
    });

    realurl::EventLoop loop;
    realurl::Config config;
    config.max_streams_per_conn = stream_count;
    realurl::Client client(loop, config);
    const std::string url = "http://127.0.0.1:" + std::to_string(server.port()) + "/hold";

    const std::uint64_t rss_before = bench::current_rss_bytes();

    auto driver = [&]() -> realurl::Task<void> {
        try {
            std::vector<realurl::Stream> streams;
            streams.reserve(static_cast<std::size_t>(stream_count));
            const auto open_start = std::chrono::steady_clock::now();
            for (int i = 0; i < stream_count; ++i) {
                realurl::Stream stream = co_await client.open("POST", url, {}, 30s);
                co_await stream.end(30s);
                streams.push_back(std::move(stream));
            }
            const double open_seconds =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - open_start).count();

            // Steady state: nothing moves for a few seconds. Idle keepalive is
            // a 30s PING, so this window measures pure per-stream overhead.
            const bench::CpuTime cpu_start = bench::cpu_time();
            co_await loop.sleep_for(5s);
            const bench::CpuTime cpu_end = bench::cpu_time();

            const std::uint64_t rss_after = bench::current_rss_bytes();
            const std::int64_t delta_bytes =
                static_cast<std::int64_t>(rss_after) - static_cast<std::int64_t>(rss_before);
            const double per_stream = delta_bytes > 0 ? static_cast<double>(delta_bytes) / stream_count : 0.0;
            const double delta_mb = delta_bytes > 0 ? megabytes(static_cast<std::uint64_t>(delta_bytes)) : 0.0;
            std::printf("mode=hold streams=%d connections=%d\n", stream_count, server.connections());
            std::printf("  open: %d streams in %.2fs (%.0f streams/s)\n", stream_count, open_seconds,
                        stream_count / open_seconds);
            std::printf("  rss: before=%.1fMB held=%.1fMB delta=%.1fMB per-stream=%.2fKB\n",
                        megabytes(rss_before), megabytes(rss_after), delta_mb, per_stream / 1024.0);
            std::printf("  cpu during 5s idle hold: total=%.3fs (user=%.3fs sys=%.3fs)\n",
                        cpu_end.total() - cpu_start.total(), cpu_end.user - cpu_start.user,
                        cpu_end.system - cpu_start.system);
            std::printf("  rss: peak=%.1fMB\n", megabytes(bench::peak_rss_bytes()));

            for (realurl::Stream &stream : streams)
                stream.cancel();
        } catch (const std::exception &error) {
            std::fprintf(stderr, "hold failed: %s\n", error.what());
        }
        loop.stop();
    };

    loop.spawn(driver());
    loop.run();
    return 0;
}

realurl::Task<void> llm_worker(realurl::Client &client, const std::string &url,
                               const std::string &body_2mb,
                               int total,
                               std::atomic<int> &remaining,
                               std::atomic<int> &completed,
                               std::vector<double> &latencies_total_ms,
                               std::vector<double> &latencies_ttft_ms,
                               std::atomic<std::uint64_t> &total_uploaded,
                               std::atomic<std::uint64_t> &total_downloaded,
                               std::atomic<int> &failed_count,
                               realurl::EventLoop &loop)
{
    std::vector<double> local_total;
    std::vector<double> local_ttft;
    for (;;) {
        int expected = remaining.load(std::memory_order_relaxed);
        bool claimed = false;
        while (expected > 0) {
            if (remaining.compare_exchange_weak(expected, expected - 1, std::memory_order_relaxed)) {
                claimed = true;
                break;
            }
        }
        if (!claimed) {
            latencies_total_ms.insert(latencies_total_ms.end(), local_total.begin(), local_total.end());
            latencies_ttft_ms.insert(latencies_ttft_ms.end(), local_ttft.begin(), local_ttft.end());
            co_return;
        }

        const auto start = std::chrono::steady_clock::now();
        try {
            realurl::Stream stream = co_await client.open("POST", url, {}, 60s);
            co_await stream.write(body_2mb, 60s);
            co_await stream.end(60s);
            total_uploaded.fetch_add(body_2mb.size(), std::memory_order_relaxed);

            (void)co_await stream.head(60s);
            char buffer[16384];
            std::size_t n = co_await stream.read({ buffer, sizeof(buffer) }, 60s);
            const auto first_chunk = std::chrono::steady_clock::now();
            local_ttft.push_back(
                std::chrono::duration<double, std::milli>(first_chunk - start).count());
            total_downloaded.fetch_add(n, std::memory_order_relaxed);

            while (n > 0) {
                n = co_await stream.read({ buffer, sizeof(buffer) }, 60s);
                total_downloaded.fetch_add(n, std::memory_order_relaxed);
            }

            local_total.push_back(
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
        } catch (const std::exception &e) {
            failed_count.fetch_add(1, std::memory_order_relaxed);
        }

        if (completed.fetch_add(1, std::memory_order_relaxed) + 1 == total) {
            latencies_total_ms.insert(latencies_total_ms.end(), local_total.begin(), local_total.end());
            latencies_ttft_ms.insert(latencies_ttft_ms.end(), local_ttft.begin(), local_ttft.end());
            local_total.clear();
            local_ttft.clear();
            loop.stop();
            co_return;
        }
    }
}

int run_llm(int total, int concurrency, int duration_seconds, bool streaming, int connections = 1)
{
    realurl::test::H2TestServer server([duration_seconds, streaming](const realurl::test::H2TestRequest &) {
        realurl::test::H2TestResponse response;
        response.body = std::string(1024, 'T'); // ~1024 bytes (representing ~1k tokens)
        if (streaming && duration_seconds > 0) {
            response.chunks = duration_seconds;
            response.chunk_delay = 1000ms;
        } else {
            response.delay = std::chrono::seconds(duration_seconds);
        }
        return response;
    });

    realurl::EventLoop loop;
    realurl::Config config;
    config.max_conns_per_origin = connections;
    config.max_streams_per_conn = (concurrency + connections - 1) / connections;
    realurl::Client client(loop, config);
    const std::string url = "http://127.0.0.1:" + std::to_string(server.port()) + "/llm";

    // 2MB LLM prompt payload (~100k input tokens)
    const std::string prompt_2mb(2 * 1024 * 1024, 'P');

    std::atomic<int> remaining{ total };
    std::atomic<int> completed{ 0 };
    std::vector<double> latencies_total_ms;
    latencies_total_ms.reserve(static_cast<std::size_t>(total));
    std::vector<double> latencies_ttft_ms;
    latencies_ttft_ms.reserve(static_cast<std::size_t>(total));
    std::atomic<std::uint64_t> total_uploaded{ 0 };
    std::atomic<std::uint64_t> total_downloaded{ 0 };
    std::atomic<int> failed_count{ 0 };

    std::chrono::steady_clock::time_point wall_start{};
    bench::CpuTime cpu_start{};
    const std::uint64_t rss_before = bench::current_rss_bytes();

    auto driver = [&]() -> realurl::Task<void> {
        // Warm up connection first with a small request
        (void)co_await client.get(url, 30s);
        wall_start = std::chrono::steady_clock::now();
        cpu_start = bench::cpu_time();

        for (int i = 0; i < concurrency; ++i) {
            loop.spawn(llm_worker(client, url, prompt_2mb, total, remaining, completed,
                                  latencies_total_ms, latencies_ttft_ms, total_uploaded, total_downloaded,
                                  failed_count, loop));
        }
        co_return;
    };

    loop.spawn(driver());
    loop.run();

    const double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_start).count();
    const bench::CpuTime cpu_end = bench::cpu_time();
    const std::uint64_t rss_after = bench::current_rss_bytes();
    const std::vector<double> pct_ttft = percentiles(std::move(latencies_ttft_ms));
    const std::vector<double> pct_total = percentiles(std::move(latencies_total_ms));

    std::printf("mode=llm requests=%d concurrency=%d duration=%ds streaming=%s connections=%d\n",
                total, concurrency, duration_seconds, streaming ? "true" : "false", server.connections());
    std::printf("  status: %d passed, %d failed in %.2fs (%.2f req/s, %.1f RPM)\n",
                total - failed_count.load(), failed_count.load(), wall,
                total / wall, (total / wall) * 60.0);
    std::printf("  data: %.1f MB uploaded (%.1f MB/s), %.1f KB downloaded\n",
                total_uploaded.load() / (1024.0 * 1024.0),
                (total_uploaded.load() / (1024.0 * 1024.0)) / wall,
                total_downloaded.load() / 1024.0);
    if (!pct_ttft.empty())
        std::printf("  ttft ms:   p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n",
                    pct_ttft[0], pct_ttft[1], pct_ttft[2], pct_ttft[3], pct_ttft[5]);
    if (!pct_total.empty())
        std::printf("  total s:   p50=%.2f p90=%.2f p95=%.2f p99=%.2f max=%.2f\n",
                    pct_total[0] / 1000.0, pct_total[1] / 1000.0, pct_total[2] / 1000.0,
                    pct_total[3] / 1000.0, pct_total[5] / 1000.0);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (%.3f ms/req)\n",
                cpu_end.user - cpu_start.user,
                cpu_end.system - cpu_start.system,
                cpu_end.total() - cpu_start.total(),
                (cpu_end.total() - cpu_start.total()) * 1000.0 / total);
    std::printf("  rss: before=%.1fMB peak=%.1fMB after=%.1fMB delta=%.1fMB per-inflight=%.2fMB\n",
                megabytes(rss_before), megabytes(bench::peak_rss_bytes()), megabytes(rss_after),
                megabytes(bench::peak_rss_bytes() - rss_before),
                (concurrency > 0) ? megabytes(bench::peak_rss_bytes() - rss_before) / concurrency : 0.0);
    return 0;
}

int free_port()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return 0;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) != 0) {
        ::close(fd);
        return 0;
    }
    const int port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

/// Client preface plus an empty SETTINGS frame.
constexpr std::string_view kH2Hello("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                                    "\x00\x00\x00\x04\x00\x00\x00\x00\x00",
                                    24 + 9);

realurl::detail::SocketAddress tls_loopback(int port)
{
    realurl::detail::SocketAddress address;
    auto *in = reinterpret_cast<sockaddr_in *>(&address.storage);
    in->sin_family = AF_INET;
    in->sin_port = htons(static_cast<std::uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &in->sin_addr);
    address.length = sizeof(sockaddr_in);
    return address;
}

/// One TLS connection: TCP connect, handshake, h2 preface, and the server's
/// SETTINGS frame, which also delivers the session ticket. Returns -1 on
/// failure, else whether the handshake resumed.
realurl::Task<int> tls_hello(std::shared_ptr<realurl::detail::TlsContext> context, int port)
{
    try {
        auto transport = co_await realurl::detail::TlsStream::connect(
            realurl::EventLoop::current(), tls_loopback(port), "localhost", context, 5s);
        auto &stream = static_cast<realurl::detail::TlsStream &>(*transport);
        co_await stream.write_all(kH2Hello, 5s);
        char frame[9];
        std::size_t have = 0;
        while (have < sizeof(frame)) {
            const std::size_t n = co_await stream.read({ frame + have, sizeof(frame) - have }, 5s);
            if (n == 0)
                co_return -1;
            have += n;
        }
        co_return stream.resumed() ? 1 : 0;
    } catch (...) {
        co_return -1;
    }
}

struct TlsConnectResult {
    std::vector<double> latencies;
    int resumed = 0;
    int failed = 0;
};

realurl::Task<void> tls_connect_all(const std::vector<std::shared_ptr<realurl::detail::TlsContext>> &contexts,
                                    int port, TlsConnectResult &result)
{
    for (const auto &context : contexts) {
        const auto start = std::chrono::steady_clock::now();
        const int outcome = co_await tls_hello(context, port);
        result.latencies.push_back(
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
        if (outcome < 0)
            ++result.failed;
        else
            result.resumed += outcome;
    }
}

/// Sequential new TLS connections to a realurl Server. `resume` shares one
/// warmed TlsContext, so every handshake resumes its cached session; `full`
/// gives each connection a fresh context and a full handshake. Contexts are
/// created before timing starts.
int run_tls_connect(int total, bool resume, const std::string &cert, const std::string &key)
{
    using realurl::detail::TlsContext;
    total = std::max(1, total);

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "tls_connect: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    server.set_io_threads(1);
    server.set_tls(cert, key);
    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running()) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "tls_connect: server failed to start\n");
        return 1;
    }

    std::vector<std::shared_ptr<TlsContext>> contexts;
    const std::vector<std::shared_ptr<TlsContext>> warmup{ TlsContext::create(cert, false) };
    if (resume)
        contexts.assign(static_cast<std::size_t>(total), warmup.front());
    for (int i = 0; !resume && i < total; ++i)
        contexts.push_back(TlsContext::create(cert, false));
    TlsConnectResult warm;
    realurl::sync_wait(tls_connect_all(warmup, port, warm));

    TlsConnectResult result;
    const std::uint32_t wire_start = bench::loopback_tx_bytes();
    const bench::CpuTime cpu_start = bench::cpu_time();
    const auto wall_start = std::chrono::steady_clock::now();
    realurl::sync_wait(tls_connect_all(contexts, port, result));
    const double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_start).count();
    const bench::CpuTime cpu_end = bench::cpu_time();
    const std::uint32_t wire = bench::loopback_tx_bytes() - wire_start;

    const std::vector<double> pct = percentiles(result.latencies);
    std::printf("mode=tls_connect handshake=%s connections=%d\n", resume ? "resume" : "full", total);
    std::printf("  status: %d ok, %d failed, %d resumed in %.2fs (%.0f conn/s)\n", total - result.failed,
                result.failed, result.resumed, wall, total / wall);
    std::printf("  latency ms: p50=%.3f p90=%.3f p99=%.3f max=%.3f\n", pct[0], pct[1], pct[3], pct[5]);
    std::printf("  cpu: total=%.2fs (%.1f us/conn, client + server)\n", cpu_end.total() - cpu_start.total(),
                (cpu_end.total() - cpu_start.total()) * 1e6 / total);
    std::printf("  wire: %.0f B/conn on loopback\n", static_cast<double>(wire) / total);

    server.stop();
    listener.join();
    return result.failed == 0 ? 0 : 1;
}

/// Same workload as run_llm(), but the endpoint is a realurl Server so both
/// the HTTP/2 and native transports can be compared against identical server
/// code. The server streams 1 KiB over duration_seconds chunks.
int run_server_llm(int total, int concurrency, int duration_seconds, bool streaming, int connections,
                   realurl::Protocol protocol, std::size_t body_bytes = 2 * 1024 * 1024, bool bulk = false)
{
    total = std::max(1, total);
    concurrency = std::max(1, concurrency);
    connections = std::max(1, connections);
    duration_seconds = std::max(0, duration_seconds);

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "native_llm: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    server.set_io_threads(1);
    server.set_payload_max_length(std::max<std::size_t>(4 * 1024 * 1024, body_bytes * 2));
    server.set_read_header_timeout(0ms);
    server.set_client_write_timeout(std::chrono::seconds(duration_seconds + 60));

    server.Get("/llm", [](const auto &, auto &response) { response.set_content("ok"); });
    server.Post("/llm", [duration_seconds, streaming, bulk, body_bytes](const realurl::Server::Request &request,
                                                       realurl::Server::Response &response) {
        response.take_over_task([duration_seconds, streaming, bulk, body_bytes, &request](
                                    realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            const realurl::server::Header headers[] = { { "content-type", "text/plain" } };
            co_await writer.write_head(200, headers, std::nullopt);

            if (streaming && duration_seconds > 0) {
                constexpr std::size_t kBody = 1024;
                const std::string body(kBody, 'T');
                std::size_t offset = 0;
                for (int i = 0; i < duration_seconds; ++i) {
                    const std::size_t remaining = kBody - offset;
                    const std::size_t chunk =
                        (i + 1 == duration_seconds)
                            ? remaining
                            : std::max<std::size_t>(1, kBody / static_cast<std::size_t>(duration_seconds));
                    const std::size_t n = std::min(chunk, remaining);
                    co_await writer.write(std::string_view{ body }.substr(offset, n));
                    offset += n;
                    if (i + 1 < duration_seconds)
                        co_await request.loop->sleep_for(1000ms);
                }
            } else {
                if (duration_seconds > 0)
                    co_await request.loop->sleep_for(std::chrono::seconds(duration_seconds));
                co_await writer.write(std::string(bulk ? body_bytes : 1024, 'T'));
            }
            co_await writer.end();
        });
    });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running()) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "native_llm: server failed to start\n");
        return 1;
    }

    realurl::EventLoop loop;
    realurl::Config config;
    config.protocol = protocol;
    config.max_conns_per_origin = connections;
    config.max_streams_per_conn = (concurrency + connections - 1) / connections;
    realurl::Client client(loop, config);
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + "/llm";
    const std::string prompt_2mb(body_bytes, 'P');

    std::atomic<int> remaining{ total };
    std::atomic<int> completed{ 0 };
    std::vector<double> latencies_total_ms;
    latencies_total_ms.reserve(static_cast<std::size_t>(total));
    std::vector<double> latencies_ttft_ms;
    latencies_ttft_ms.reserve(static_cast<std::size_t>(total));
    std::atomic<std::uint64_t> total_uploaded{ 0 };
    std::atomic<std::uint64_t> total_downloaded{ 0 };
    std::atomic<int> failed_count{ 0 };

    std::chrono::steady_clock::time_point wall_start{};
    bench::CpuTime cpu_start{};
    const std::uint64_t rss_before = bench::current_rss_bytes();

    auto driver = [&]() -> realurl::Task<void> {
        (void)co_await client.get(url, 30s);
        wall_start = std::chrono::steady_clock::now();
        cpu_start = bench::cpu_time();

        for (int i = 0; i < concurrency; ++i) {
            loop.spawn(llm_worker(client, url, prompt_2mb, total, remaining, completed,
                                  latencies_total_ms, latencies_ttft_ms, total_uploaded,
                                  total_downloaded, failed_count, loop));
        }
        co_return;
    };

    loop.spawn(driver());
    loop.run();

    const double wall =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_start).count();
    const bench::CpuTime cpu_end = bench::cpu_time();
    const std::uint64_t rss_after = bench::current_rss_bytes();
    const std::vector<double> pct_ttft = percentiles(std::move(latencies_ttft_ms));
    const std::vector<double> pct_total = percentiles(std::move(latencies_total_ms));

    const auto stats = server.stats();
    std::printf("mode=native_llm protocol=%s requests=%d concurrency=%d duration=%ds streaming=%s "
                "connections=%llu\n",
                protocol == realurl::Protocol::Native ? "native" : "h2", total, concurrency,
                duration_seconds, streaming ? "true" : "false",
                static_cast<unsigned long long>(stats.native_connections + stats.h2_connections));
    std::printf("  status: %d passed, %d failed in %.2fs (%.2f req/s, %.1f RPM)\n",
                total - failed_count.load(), failed_count.load(), wall, total / wall,
                (total / wall) * 60.0);
    std::printf("  data: %.1f MB uploaded (%.1f MB/s), %.1f KB downloaded (%.1f MB/s)\n",
                total_uploaded.load() / (1024.0 * 1024.0),
                (total_uploaded.load() / (1024.0 * 1024.0)) / wall,
                total_downloaded.load() / 1024.0,
                (total_downloaded.load() / (1024.0 * 1024.0)) / wall);
    if (!pct_ttft.empty())
        std::printf("  ttft ms:   p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_ttft[0],
                    pct_ttft[1], pct_ttft[2], pct_ttft[3], pct_ttft[5]);
    if (!pct_total.empty())
        std::printf("  total s:   p50=%.2f p90=%.2f p95=%.2f p99=%.2f max=%.2f\n",
                    pct_total[0] / 1000.0, pct_total[1] / 1000.0, pct_total[2] / 1000.0,
                    pct_total[3] / 1000.0, pct_total[5] / 1000.0);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (%.3f ms/req)\n",
                cpu_end.user - cpu_start.user, cpu_end.system - cpu_start.system,
                cpu_end.total() - cpu_start.total(),
                (cpu_end.total() - cpu_start.total()) * 1000.0 / total);
    std::printf("  rss: before=%.1fMB peak=%.1fMB after=%.1fMB delta=%.1fMB per-inflight=%.2fMB\n",
                megabytes(rss_before), megabytes(bench::peak_rss_bytes()), megabytes(rss_after),
                megabytes(bench::peak_rss_bytes() - rss_before),
                (concurrency > 0) ? megabytes(bench::peak_rss_bytes() - rss_before) / concurrency
                                  : 0.0);

    client.close();
    server.stop();
    listener.join();
    return failed_count.load() == 0 ? 0 : 1;
}

struct PumpClientResult {
    double ttft_ms = 0;
    double total_ms = 0;
    int events = 0;
    bool ok = false;
};

struct PumpBenchState {
    std::vector<PumpClientResult> results;
    int connect_failures = 0;
    int send_failures = 0;
    int read_failures = 0;
    int first_stage = 0;
    int first_errno = 0;
    int first_recv = 0;

    void record_failure(int stage, int error = 0, int received = 0)
    {
        if (first_stage == 0) {
            first_stage = stage;
            first_errno = error;
        }
        first_recv = received;
    }
};

struct PollSlot {
    int fd = -1;
    bool connected = false;
    bool done = false;
    bool first_byte = false;
    int match = 0;
    int events = 0;
    std::chrono::steady_clock::time_point start{};
    std::chrono::steady_clock::time_point first{};
};

struct PollRunResult {
    double wall_seconds = 0;
    bench::CpuTime cpu_start{};
    bench::CpuTime cpu_end{};
    double thread_cpu_seconds = 0; // CPU burned by the client thread itself
};

double thread_cpu_seconds()
{
    timespec now{};
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now);
    return static_cast<double>(now.tv_sec) + static_cast<double>(now.tv_nsec) / 1e9;
}

sockaddr_in loopback_address(int port)
{
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}

bool set_nonblocking(int fd)
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

/// Single-threaded poll client. One thread per connection would cap the bench
/// at the process thread limit long before the server's concurrency limit.
/// count_events tallies "data: " occurrences per stream into results[i].events.
PollRunResult run_poll_clients(int port, int concurrency, int timeout_ms, PumpBenchState &state,
                               std::string_view path = "/stream", std::string_view body = {},
                               bool count_events = false)
{
    const std::string head =
        body.empty() ? "GET " + std::string(path) + " HTTP/1.1\r\nHost: bench\r\n\r\n"
                     : "POST " + std::string(path) + " HTTP/1.1\r\nHost: bench\r\nContent-Length: " +
                           std::to_string(body.size()) + "\r\n\r\n";
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    const sockaddr_in address = loopback_address(port);

    std::vector<PollSlot> slots(static_cast<std::size_t>(concurrency));
    std::vector<pollfd> pollfds;
    std::vector<int> indices;
    pollfds.reserve(slots.size());
    indices.reserve(slots.size());

    // Establish in batches so a connect burst does not overflow the listen
    // backlog (SOMAXCONN is 128 on macOS). All requests are sent together
    // afterwards, so connection setup is outside the measured window.
    constexpr int kBatch = 64;
    for (int base = 0; base < concurrency; base += kBatch) {
        const int end = std::min(concurrency, base + kBatch);
        for (int i = base; i < end; ++i) {
            PollSlot &slot = slots[static_cast<std::size_t>(i)];
            slot.fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (slot.fd < 0 || !set_nonblocking(slot.fd)) {
                if (slot.fd >= 0)
                    ::close(slot.fd);
                slot.fd = -1;
                slot.done = true;
                ++state.connect_failures;
                state.record_failure(1, errno);
                continue;
            }
            if (::connect(slot.fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0 &&
                errno != EINPROGRESS) {
                ::close(slot.fd);
                slot.fd = -1;
                slot.done = true;
                ++state.connect_failures;
                state.record_failure(1, errno);
            }
        }

        int pending = 0;
        for (int i = base; i < end; ++i) {
            const PollSlot &slot = slots[static_cast<std::size_t>(i)];
            if (!slot.done && !slot.connected)
                ++pending;
        }

        while (pending > 0 && std::chrono::steady_clock::now() < deadline) {
            pollfds.clear();
            indices.clear();
            for (int i = base; i < end; ++i) {
                PollSlot &slot = slots[static_cast<std::size_t>(i)];
                if (!slot.done && !slot.connected) {
                    pollfds.push_back({ slot.fd, POLLOUT, 0 });
                    indices.push_back(i);
                }
            }
            const int rc = ::poll(pollfds.data(), static_cast<nfds_t>(pollfds.size()), 100);
            if (rc < 0 && errno != EINTR)
                break;
            if (rc <= 0)
                continue;

            for (std::size_t k = 0; k < pollfds.size(); ++k) {
                if (!(pollfds[k].revents & (POLLOUT | POLLERR | POLLHUP)))
                    continue;
                PollSlot &slot = slots[static_cast<std::size_t>(indices[k])];
                if (slot.done || slot.connected)
                    continue;

                int error = 0;
                socklen_t length = sizeof(error);
                if (::getsockopt(slot.fd, SOL_SOCKET, SO_ERROR, &error, &length) != 0 || error != 0) {
                    ::close(slot.fd);
                    slot.fd = -1;
                    slot.done = true;
                    ++state.connect_failures;
                    state.record_failure(1, error);
                } else {
                    slot.connected = true;
                }
                --pending;
            }
        }

        for (int i = base; i < end; ++i) {
            PollSlot &slot = slots[static_cast<std::size_t>(i)];
            if (slot.done || slot.connected)
                continue;
            ::close(slot.fd);
            slot.fd = -1;
            slot.done = true;
            ++state.connect_failures;
            state.record_failure(1, ETIMEDOUT);
        }
    }

    PollRunResult run;
    run.cpu_start = bench::cpu_time();
    const double thread_cpu_start = thread_cpu_seconds();
    const auto start = std::chrono::steady_clock::now();
    int active = 0;

    for (PollSlot &slot : slots) {
        if (!slot.connected)
            continue;

        const auto send_bytes = [&](std::string_view data) {
            std::size_t sent = 0;
            while (sent < data.size()) {
                const ssize_t n = ::send(slot.fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
                if (n > 0) {
                    sent += static_cast<std::size_t>(n);
                    continue;
                }
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    pollfd pfd{ slot.fd, POLLOUT, 0 };
                    if (::poll(&pfd, 1, 100) <= 0 && std::chrono::steady_clock::now() >= deadline)
                        return false;
                    continue;
                }
                if (n < 0 && errno == EINTR)
                    continue;
                return false;
            }
            return true;
        };

        if (!send_bytes(head) || (!body.empty() && !send_bytes(body))) {
            ::close(slot.fd);
            slot.fd = -1;
            slot.done = true;
            ++state.send_failures;
            state.record_failure(2, errno);
            continue;
        }

        slot.start = std::chrono::steady_clock::now();
        ++active;
    }

    char buffer[16384];
    while (active > 0 && std::chrono::steady_clock::now() < deadline) {
        pollfds.clear();
        indices.clear();
        for (int i = 0; i < concurrency; ++i) {
            PollSlot &slot = slots[static_cast<std::size_t>(i)];
            if (slot.fd >= 0 && slot.connected && !slot.done) {
                pollfds.push_back({ slot.fd, POLLIN, 0 });
                indices.push_back(i);
            }
        }
        if (pollfds.empty())
            break;

        const int rc = ::poll(pollfds.data(), static_cast<nfds_t>(pollfds.size()), 100);
        if (rc < 0 && errno != EINTR)
            break;
        if (rc <= 0)
            continue;

        for (std::size_t k = 0; k < pollfds.size(); ++k) {
            if (!(pollfds[k].revents & (POLLIN | POLLHUP | POLLERR)))
                continue;

            const int index = indices[k];
            PollSlot &slot = slots[static_cast<std::size_t>(index)];
            for (;;) {
                const ssize_t n = ::recv(slot.fd, buffer, sizeof(buffer), 0);
                if (n > 0) {
                    if (!slot.first_byte) {
                        slot.first_byte = true;
                        slot.first = std::chrono::steady_clock::now();
                    }
                    if (count_events) {
                        // "data: " has no proper border, so a mismatch restarts at 0 or 1.
                        static constexpr std::string_view kMarker = "data: ";
                        for (ssize_t b = 0; b < n; ++b) {
                            const char c = buffer[b];
                            if (c == kMarker[static_cast<std::size_t>(slot.match)])
                                ++slot.match;
                            else
                                slot.match = (c == kMarker[0]) ? 1 : 0;
                            if (slot.match == static_cast<int>(kMarker.size())) {
                                slot.match = 0;
                                ++slot.events;
                            }
                        }
                    }
                    continue;
                }
                if (n == 0) {
                    const auto end = std::chrono::steady_clock::now();
                    auto &out = state.results[static_cast<std::size_t>(index)];
                    out.ok = true;
                    out.events = slot.events;
                    out.ttft_ms = std::chrono::duration<double, std::milli>(slot.first - slot.start).count();
                    out.total_ms = std::chrono::duration<double, std::milli>(end - slot.start).count();
                    ::close(slot.fd);
                    slot.fd = -1;
                    slot.done = true;
                    --active;
                    break;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;
                if (errno == EINTR)
                    continue;

                ::close(slot.fd);
                slot.fd = -1;
                slot.done = true;
                ++state.read_failures;
                state.record_failure(4, errno, static_cast<int>(n));
                --active;
                break;
            }
        }
    }

    const auto end = std::chrono::steady_clock::now();
    for (int i = 0; i < concurrency; ++i) {
        PollSlot &slot = slots[static_cast<std::size_t>(i)];
        if (slot.done)
            continue;
        if (slot.fd >= 0)
            ::close(slot.fd);
        slot.done = true;
        ++state.read_failures;
        state.record_failure(5, ETIMEDOUT);
    }

    run.cpu_end = bench::cpu_time();
    run.thread_cpu_seconds = thread_cpu_seconds() - thread_cpu_start;
    run.wall_seconds = std::chrono::duration<double>(end - start).count();
    return run;
}

int run_pump(int concurrency, int chunks, int interval_ms, int timeout_ms, bool async_mode)
{
    concurrency = std::max(1, concurrency);
    chunks = std::max(0, chunks);
    interval_ms = std::max(0, interval_ms);
    timeout_ms = std::max(1, timeout_ms);

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "pump: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    server.set_io_threads(2);
    server.Get("/stream", [chunks, interval_ms, async_mode](const realurl::Server::Request &,
                                                            realurl::Server::Response &response) {
        if (async_mode) {
            response.take_over_async([chunks, interval_ms](realurl::AsyncClientWriter &writer) -> net::awaitable<void> {
                if (!(co_await writer.write("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")))
                    co_return;
                net::steady_timer timer(co_await net::this_coro::executor);
                for (int i = 0; i < chunks; ++i) {
                    if (!(co_await writer.write("data: one\n\n")))
                        co_return;
                    if (interval_ms > 0) {
                        timer.expires_after(std::chrono::milliseconds(interval_ms));
                        co_await timer.async_wait(net::use_awaitable);
                    }
                }
            });
            return;
        }

        response.take_over([chunks, interval_ms](realurl::ClientWriter &writer) {
            if (!writer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"))
                return;
            for (int i = 0; i < chunks; ++i) {
                if (!writer("data: one\n\n"))
                    return;
                if (interval_ms > 0)
                    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            }
        });
    });

    std::size_t pump_capacity = 0;
    std::size_t pump_initial = 0;
    std::size_t pump_queue = 0;
    if (!async_mode) {
        // Measure queueing rather than load shedding: give the pool room for
        // the whole burst. Production defaults to a queue equal to capacity.
        realurl::StreamPumpPool::configure_queue(static_cast<std::size_t>(concurrency));
        pump_capacity = realurl::stream_pump_pool().capacity();
        pump_initial = realurl::stream_pump_pool().size();
        pump_queue = realurl::stream_pump_pool().queue_limit();
    }
    const std::uint64_t rss_before = bench::current_rss_bytes();

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running()) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "pump: server failed to start\n");
        return 1;
    }

    PumpBenchState state;
    state.results.resize(static_cast<std::size_t>(concurrency));
    const PollRunResult run = run_poll_clients(port, concurrency, timeout_ms, state);

    server.stop();
    listener.join();

    std::vector<double> ttft_ms;
    std::vector<double> total_ms;
    ttft_ms.reserve(static_cast<std::size_t>(concurrency));
    total_ms.reserve(static_cast<std::size_t>(concurrency));
    for (const PumpClientResult &result : state.results) {
        if (!result.ok)
            continue;
        ttft_ms.push_back(result.ttft_ms);
        total_ms.push_back(result.total_ms);
    }

    const double wall = run.wall_seconds;
    const bench::CpuTime cpu_start = run.cpu_start;
    const bench::CpuTime cpu_end = run.cpu_end;
    const int connect_failures = state.connect_failures;
    const int send_failures = state.send_failures;
    const int read_failures = state.read_failures;
    const int failures = connect_failures + send_failures + read_failures;
    const std::vector<double> pct_ttft = percentiles(ttft_ms);
    const std::vector<double> pct_total = percentiles(total_ms);

    double waves = 1.0;
    if (async_mode) {
        std::printf("mode=async concurrency=%d io_threads=2 stream_ms=%d timeout_ms=%d\n", concurrency,
                    chunks * interval_ms, timeout_ms);
    } else {
        const std::size_t effective = std::min(static_cast<std::size_t>(concurrency), pump_capacity);
        const std::size_t pump_rejected = realurl::stream_pump_pool().rejected();
        waves = static_cast<double>(concurrency) / static_cast<double>(effective);
        std::printf("mode=pump concurrency=%d pump_capacity=%zu pump_initial=%zu pump_queue=%zu "
                    "pump_rejected=%zu stream_ms=%d timeout_ms=%d\n",
                    concurrency, pump_capacity, pump_initial, pump_queue, pump_rejected, chunks * interval_ms,
                    timeout_ms);
    }
    std::printf("  status: %d ok, %d failed in %.2fs (%.2f waves)", concurrency - failures, failures, wall,
                waves);
    if (failures > 0)
        std::printf(" [connect=%d send=%d read=%d first: stage=%d errno=%d recv=%d]", connect_failures,
                    send_failures, read_failures, state.first_stage, state.first_errno, state.first_recv);
    std::printf("\n");
    if (!pct_ttft.empty())
        std::printf("  ttft ms:  p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_ttft[0], pct_ttft[1],
                    pct_ttft[2], pct_ttft[3], pct_ttft[5]);
    if (!pct_total.empty())
        std::printf("  total ms: p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_total[0], pct_total[1],
                    pct_total[2], pct_total[3], pct_total[5]);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs\n", cpu_end.user - cpu_start.user,
                cpu_end.system - cpu_start.system, cpu_end.total() - cpu_start.total());
    std::printf("  rss: before=%.1fMB peak=%.1fMB current=%.1fMB\n", megabytes(rss_before),
                megabytes(bench::peak_rss_bytes()), megabytes(bench::current_rss_bytes()));
    return 0;
}

/// Server::is_running() flips before bind/listen; probe until the port accepts.
bool wait_until_accepting(int port)
{
    for (int attempt = 0; attempt < 200; ++attempt) {
        const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
        if (probe >= 0) {
            sockaddr_in address = loopback_address(port);
            const bool connected =
                ::connect(probe, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == 0;
            ::close(probe);
            if (connected)
                return true;
        }
        std::this_thread::sleep_for(5ms);
    }
    return false;
}

std::string proxy_head(const realurl::Response &response)
{
    return "HTTP/1.1 " + std::to_string(response.head.status) + " Proxy\r\nContent-Length: " +
           std::to_string(response.body.size()) + "\r\nConnection: close\r\n\r\n";
}

/// HTTP/1.1 proxy in front of the h2c test server. `bridged` runs the upstream
/// request through realurl::async_request (ClientGroup worker, completion
/// back on the io thread); otherwise sync_wait blocks the io thread, which is
/// the pattern the bridge replaces.
int run_proxy(int concurrency, int chunks, int interval_ms, int timeout_ms, int workers, bool bridged)
{
    concurrency = std::max(1, concurrency);
    chunks = std::max(0, chunks);
    interval_ms = std::max(0, interval_ms);
    timeout_ms = std::max(1, timeout_ms);
    workers = std::max(1, workers);

    realurl::test::H2TestServer upstream([chunks, interval_ms](const realurl::test::H2TestRequest &) {
        realurl::test::H2TestResponse response;
        response.body = "data: one\n\n";
        response.chunks = chunks;
        response.chunk_delay = std::chrono::milliseconds(interval_ms);
        return response;
    });

    realurl::Config config;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = concurrency;
    config.connect_timeout = 5s;
    realurl::ClientGroup group(static_cast<unsigned>(workers), config);
    const std::string upstream_url =
        "http://127.0.0.1:" + std::to_string(upstream.port()) + "/upstream";

    // Warm the pooled upstream connection outside the measured window.
    try {
        (void)realurl::sync_wait(group.request("GET", upstream_url, {}, {}, 30s));
    } catch (const std::exception &error) {
        std::fprintf(stderr, "proxy: upstream warmup failed: %s\n", error.what());
        return 1;
    }

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "proxy: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    server.set_io_threads(2);
    // The client connects every stream before sending requests. Blocking
    // handlers would otherwise let the 5 s header timer expire for connections
    // that are merely queued behind two blocked io threads.
    server.set_read_header_timeout(0ms);
    server.Get("/proxy", [&group, upstream_url, bridged](const realurl::Server::Request &,
                                                         realurl::Server::Response &response) {
        if (bridged) {
            response.take_over_async([&group, upstream_url](realurl::AsyncClientWriter &writer) -> net::awaitable<void> {
                realurl::Response upstream_response;
                bool failed = false;
                try {
                    upstream_response = co_await realurl::async_request(group, "GET", upstream_url, {}, {}, 120s);
                } catch (...) {
                    failed = true;
                }
                if (failed) {
                    co_await writer.write(
                        "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    co_return;
                }
                const std::string head = proxy_head(upstream_response);
                if (!(co_await writer.write(head)))
                    co_return;
                co_await writer.write(upstream_response.body);
            });
            return;
        }

        response.take_over_async([&group, upstream_url](realurl::AsyncClientWriter &writer) -> net::awaitable<void> {
            realurl::Response upstream_response;
            bool failed = false;
            try {
                upstream_response = realurl::sync_wait(group.request("GET", upstream_url, {}, {}, 120s));
            } catch (...) {
                failed = true;
            }
            if (failed) {
                co_await writer.write("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                co_return;
            }
            const std::string head = proxy_head(upstream_response);
            if (!(co_await writer.write(head)))
                co_return;
            co_await writer.write(upstream_response.body);
        });
    });

    const std::uint64_t rss_before = bench::current_rss_bytes();
    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running()) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "proxy: server failed to start\n");
        return 1;
    }
    if (!wait_until_accepting(port)) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "proxy: server did not start accepting\n");
        return 1;
    }

    PumpBenchState state;
    state.results.resize(static_cast<std::size_t>(concurrency));
    const PollRunResult run = run_poll_clients(port, concurrency, timeout_ms, state, "/proxy");

    server.stop();
    listener.join();

    std::vector<double> ttft_ms;
    std::vector<double> total_ms;
    ttft_ms.reserve(static_cast<std::size_t>(concurrency));
    total_ms.reserve(static_cast<std::size_t>(concurrency));
    for (const PumpClientResult &result : state.results) {
        if (!result.ok)
            continue;
        ttft_ms.push_back(result.ttft_ms);
        total_ms.push_back(result.total_ms);
    }

    const double wall = run.wall_seconds;
    const int failures = state.connect_failures + state.send_failures + state.read_failures;
    const std::vector<double> pct_ttft = percentiles(ttft_ms);
    const std::vector<double> pct_total = percentiles(total_ms);

    std::printf("mode=proxy bridge=%d concurrency=%d workers=%d upstream_chunks=%d upstream_interval_ms=%d "
                "timeout_ms=%d\n",
                bridged ? 1 : 0, concurrency, workers, chunks, interval_ms, timeout_ms);
    std::printf("  status: %d ok, %d failed in %.2fs", concurrency - failures, failures, wall);
    if (failures > 0)
        std::printf(" [connect=%d send=%d read=%d first: stage=%d errno=%d recv=%d]", state.connect_failures,
                    state.send_failures, state.read_failures, state.first_stage, state.first_errno,
                    state.first_recv);
    std::printf("\n");
    if (!pct_ttft.empty())
        std::printf("  ttft ms:  p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_ttft[0], pct_ttft[1],
                    pct_ttft[2], pct_ttft[3], pct_ttft[5]);
    if (!pct_total.empty())
        std::printf("  total ms: p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_total[0], pct_total[1],
                    pct_total[2], pct_total[3], pct_total[5]);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs\n", run.cpu_end.user - run.cpu_start.user,
                run.cpu_end.system - run.cpu_start.system, run.cpu_end.total() - run.cpu_start.total());
    std::printf("  rss: before=%.1fMB peak=%.1fMB current=%.1fMB\n", megabytes(rss_before),
                megabytes(bench::peak_rss_bytes()), megabytes(bench::current_rss_bytes()));
    return 0;
}

/// Proxy in front of a 20 s SSE-style upstream, with a 2 MB request body.
/// Measures the buffered bridge against the direct-client llm baseline.
int run_proxy_llm(int concurrency, int duration_s, int body_bytes, int workers, bool bridged,
                  bool inline_proxy = false)
{
    concurrency = std::max(1, concurrency);
    duration_s = std::max(1, duration_s);
    body_bytes = std::max(1, body_bytes);
    workers = std::max(1, workers);
    constexpr int kChunks = 20;
    const int interval_ms = std::max(1, duration_s * 1000 / kChunks);
    const std::string upstream_body(static_cast<std::size_t>(kChunks) * 100, 'r');

    std::atomic<std::size_t> upstream_bytes{ 0 };
    realurl::test::H2TestServer upstream([&](const realurl::test::H2TestRequest &request) {
        realurl::test::H2TestResponse response;
        if (request.path == "/warmup") {
            response.body = "ok";
            return response;
        }
        upstream_bytes.fetch_add(request.body.size(), std::memory_order_relaxed);
        response.body = upstream_body;
        response.chunks = kChunks;
        response.chunk_delay = std::chrono::milliseconds(interval_ms);
        return response;
    });

    realurl::Config config;
    config.max_conns_per_origin = 1;
    config.max_streams_per_conn = concurrency;
    config.connect_timeout = 5s;
    realurl::ClientGroup group(static_cast<unsigned>(workers), config);
    const std::string base = "http://127.0.0.1:" + std::to_string(upstream.port());
    const std::string upstream_url = base + "/stream";

    try {
        (void)realurl::sync_wait(group.request("GET", base + "/warmup", {}, {}, 30s));
    } catch (const std::exception &error) {
        std::fprintf(stderr, "proxy_llm: upstream warmup failed: %s\n", error.what());
        return 1;
    }

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "proxy_llm: cannot reserve a local port\n");
        return 1;
    }

    realurl::Server server;
    if (inline_proxy) {
        server.set_io_threads(1);
        server.set_upstream(config);
    } else {
        server.set_io_threads(2);
    }
    server.set_read_header_timeout(0ms);
    server.set_client_write_timeout(std::chrono::seconds(duration_s + 60));
    auto proxy_handler = [&group, upstream_url, bridged, inline_proxy](
                             const realurl::Server::Request &request,
                             realurl::Server::Response &response) {
        std::string body = request.body;
        if (inline_proxy) {
            response.take_over_task([&request, upstream_url](realurl::server::StreamWriter &writer)
                                        -> realurl::Task<void> {
                co_await realurl::server::proxy_stream(*request.upstream, upstream_url, request, writer);
            });
            return;
        }
        if (bridged) {
            response.take_over_async([&group, upstream_url, body = std::move(body)](
                                         realurl::AsyncClientWriter &writer) mutable -> net::awaitable<void> {
                realurl::Response upstream_response;
                bool failed = false;
                try {
                    upstream_response = co_await realurl::async_request(
                        group, "POST", upstream_url,
                        realurl::Headers{ { "content-type", "application/octet-stream" } }, std::move(body), 300s);
                } catch (...) {
                    failed = true;
                }
                if (failed) {
                    co_await writer.write(
                        "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    co_return;
                }
                const std::string head = proxy_head(upstream_response);
                if (!(co_await writer.write(head)))
                    co_return;
                co_await writer.write(upstream_response.body);
            });
            return;
        }

        response.take_over_async([&group, upstream_url, body = std::move(body)](
                                     realurl::AsyncClientWriter &writer) mutable -> net::awaitable<void> {
            realurl::Response upstream_response;
            bool failed = false;
            try {
                upstream_response = realurl::sync_wait(group.request(
                    "POST", upstream_url, realurl::Headers{ { "content-type", "application/octet-stream" } },
                    std::move(body), 300s));
            } catch (...) {
                failed = true;
            }
            if (failed) {
                co_await writer.write("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                co_return;
            }
            const std::string head = proxy_head(upstream_response);
            if (!(co_await writer.write(head)))
                co_return;
            co_await writer.write(upstream_response.body);
        });
    };

    if (inline_proxy) {
        server.Post("/proxy", proxy_handler,
                    { .body_mode = realurl::Server::BodyMode::StreamedNative });
    } else {
        server.Post("/proxy", proxy_handler);
    }

    const std::uint64_t rss_before = bench::current_rss_bytes();
    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running() || !wait_until_accepting(port)) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "proxy_llm: server failed to start\n");
        return 1;
    }

    const std::string client_body(static_cast<std::size_t>(body_bytes), 'u');
    const int timeout_ms = (duration_s + 60) * 1000;
    PumpBenchState state;
    state.results.resize(static_cast<std::size_t>(concurrency));
    const PollRunResult run =
        run_poll_clients(port, concurrency, timeout_ms, state, "/proxy", client_body);

    server.stop();
    listener.join();

    std::vector<double> ttft_ms;
    std::vector<double> total_ms;
    ttft_ms.reserve(static_cast<std::size_t>(concurrency));
    total_ms.reserve(static_cast<std::size_t>(concurrency));
    for (const PumpClientResult &result : state.results) {
        if (!result.ok)
            continue;
        ttft_ms.push_back(result.ttft_ms);
        total_ms.push_back(result.total_ms);
    }

    const double wall = run.wall_seconds;
    const int failures = state.connect_failures + state.send_failures + state.read_failures;
    const std::vector<double> pct_ttft = percentiles(ttft_ms);
    const std::vector<double> pct_total = percentiles(total_ms);
    const std::size_t expected_bytes = static_cast<std::size_t>(concurrency) * static_cast<std::size_t>(body_bytes);

    std::printf("mode=proxy_llm bridge=%d inline=%d concurrency=%d duration=%ds body=%d workers=%d\n",
                bridged ? 1 : 0, inline_proxy ? 1 : 0, concurrency, duration_s, body_bytes, workers);
    std::printf("  status: %d ok, %d failed in %.2fs", concurrency - failures, failures, wall);
    if (failures > 0)
        std::printf(" [connect=%d send=%d read=%d first: stage=%d errno=%d recv=%d]", state.connect_failures,
                    state.send_failures, state.read_failures, state.first_stage, state.first_errno,
                    state.first_recv);
    std::printf("\n");
    if (!pct_ttft.empty())
        std::printf("  ttft ms:  p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_ttft[0], pct_ttft[1],
                    pct_ttft[2], pct_ttft[3], pct_ttft[5]);
    if (!pct_total.empty())
        std::printf("  total ms: p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_total[0], pct_total[1],
                    pct_total[2], pct_total[3], pct_total[5]);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs\n", run.cpu_end.user - run.cpu_start.user,
                run.cpu_end.system - run.cpu_start.system, run.cpu_end.total() - run.cpu_start.total());
    std::printf("  rss: before=%.1fMB peak=%.1fMB current=%.1fMB\n", megabytes(rss_before),
                megabytes(bench::peak_rss_bytes()), megabytes(bench::current_rss_bytes()));
    std::printf("  upstream: received=%zu expected=%zu\n", upstream_bytes.load(std::memory_order_relaxed),
                expected_bytes);
    return 0;
}

/// Upstream half of the gateway shape: a native realurl server answering every
/// POST /events with `events` SSE events, one per interval_ms. Runs until killed.
int run_sse_upstream(int port, int events, int interval_ms, int event_bytes, int io_threads)
{
    events = std::max(1, events);
    interval_ms = std::max(0, interval_ms);
    event_bytes = std::max(1, event_bytes);
    io_threads = std::max(1, io_threads);

    const auto event = std::make_shared<const std::string>("data: " + std::string(static_cast<std::size_t>(event_bytes), 'e') + "\n\n");

    realurl::Server server;
    server.set_io_threads(static_cast<unsigned>(io_threads));
    server.set_read_header_timeout(0ms);
    server.set_client_write_timeout(std::chrono::hours(1));
    server.Get("/meta", [events](const realurl::Server::Request &, realurl::Server::Response &response) {
        response.set_content(std::to_string(events), "text/plain");
    });
    server.Post("/events", [event, events, interval_ms](const realurl::Server::Request &request,
                                                        realurl::Server::Response &response) {
        response.take_over_task([event, events, interval_ms, &request](
                                    realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            const realurl::server::Header headers[] = { { "content-type", "text/event-stream" } };
            co_await writer.write_head(200, headers, std::nullopt);
            for (int i = 0; i < events; ++i) {
                if (i > 0 && interval_ms > 0)
                    co_await request.loop->sleep_for(std::chrono::milliseconds(interval_ms));
                if (!(co_await writer.write(*event)))
                    co_return;
            }
            co_await writer.end();
        });
    }, { .body_mode = realurl::Server::BodyMode::Buffered });

    std::printf("mode=sse_upstream port=%d events=%d interval_ms=%d event_bytes=%d io_threads=%d\n", port, events,
                interval_ms, event_bytes, io_threads);
    std::fflush(stdout);
    return server.listen("127.0.0.1", port) ? 0 : 1;
}

/// Ask the upstream how many events a stream carries (GET /meta over plain
/// HTTP/1.1). Returns -1 when the upstream is unreachable.
int fetch_upstream_events(int port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    const sockaddr_in address = loopback_address(port);
    if (::connect(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return -1;
    }
    constexpr std::string_view request = "GET /meta HTTP/1.1\r\nHost: bench\r\nConnection: close\r\n\r\n";
    std::string reply;
    char buffer[1024];
    if (::send(fd, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size())) {
        for (ssize_t n; (n = ::recv(fd, buffer, sizeof(buffer), 0)) > 0;)
            reply.append(buffer, static_cast<std::size_t>(n));
    }
    ::close(fd);
    const std::size_t split = reply.find("\r\n\r\n");
    return split == std::string::npos ? -1 : std::atoi(reply.c_str() + split + 4);
}

/// Gateway shape: H1 clients -> proxy Server (StreamedNative, proxy_stream) ->
/// native upstream (run in another process via sse_upstream). Reports per-stream
/// memory and per-event CPU while every stream is held open.
int run_gateway(int upstream_port, int concurrency, int body_bytes, int io_threads, int timeout_ms)
{
    concurrency = std::max(1, concurrency);
    body_bytes = std::max(1, body_bytes);
    io_threads = std::max(1, io_threads);
    timeout_ms = std::max(1, timeout_ms);

    const int events = fetch_upstream_events(upstream_port);
    if (events <= 0) {
        std::fprintf(stderr, "gateway: no sse_upstream on port %d\n", upstream_port);
        return 1;
    }

    realurl::Config config;
    config.protocol = realurl::Protocol::Native;
    config.max_streams_per_conn = 1000;
    config.max_conns_per_origin = (concurrency + 999) / 1000 + 1;
    config.connect_timeout = 5s;

    const int port = free_port();
    if (port <= 0) {
        std::fprintf(stderr, "gateway: cannot reserve a local port\n");
        return 1;
    }

    const std::string upstream_url = "http://127.0.0.1:" + std::to_string(upstream_port) + "/events";
    realurl::Server server;
    server.set_io_threads(static_cast<unsigned>(io_threads));
    server.set_upstream(config);
    server.set_read_header_timeout(0ms);
    server.set_client_write_timeout(std::chrono::milliseconds(timeout_ms));
    server.Post("/proxy", [upstream_url](const realurl::Server::Request &request,
                                         realurl::Server::Response &response) {
        response.take_over_task([&request, upstream_url](realurl::server::StreamWriter &writer)
                                    -> realurl::Task<void> {
            co_await realurl::server::proxy_stream(*request.upstream, upstream_url, request, writer);
        });
    }, { .body_mode = realurl::Server::BodyMode::StreamedNative });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
    if (!server.is_running() || !wait_until_accepting(port)) {
        server.stop();
        listener.join();
        std::fprintf(stderr, "gateway: server failed to start\n");
        return 1;
    }

    const std::string client_body(static_cast<std::size_t>(body_bytes), 'u');
    PumpBenchState state;
    state.results.resize(static_cast<std::size_t>(concurrency));

    std::atomic<bool> sampling{ true };
    std::atomic<std::uint64_t> sampled_rss_max{ 0 };
    std::thread sampler([&] {
        auto next = std::chrono::steady_clock::now();
        while (sampling.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= next) {
                const std::uint64_t rss = bench::current_rss_bytes();
                if (rss > sampled_rss_max.load(std::memory_order_relaxed))
                    sampled_rss_max.store(rss, std::memory_order_relaxed);
                next += 200ms;
            }
            std::this_thread::sleep_for(10ms);
        }
    });

    const std::uint64_t rss_before = bench::current_rss_bytes();
    const PollRunResult run =
        run_poll_clients(port, concurrency, timeout_ms, state, "/proxy", client_body, true);
    sampling.store(false, std::memory_order_release);
    sampler.join();
    const std::uint64_t rss_peak = bench::peak_rss_bytes();

    server.stop();
    listener.join();

    std::vector<double> ttft_ms;
    std::vector<double> total_ms;
    int short_streams = 0;
    int first_short_events = -1;
    for (const PumpClientResult &result : state.results) {
        if (!result.ok)
            continue;
        if (result.events != events) {
            ++short_streams;
            if (first_short_events < 0)
                first_short_events = result.events;
            continue;
        }
        ttft_ms.push_back(result.ttft_ms);
        total_ms.push_back(result.total_ms);
    }

    const int ok = static_cast<int>(ttft_ms.size());
    const int failures = concurrency - ok;
    const std::vector<double> pct_ttft = percentiles(ttft_ms);
    const std::vector<double> pct_total = percentiles(total_ms);
    const bench::CpuTime cpu_total{ run.cpu_end.user - run.cpu_start.user,
                                    run.cpu_end.system - run.cpu_start.system };
    const double proxy_cpu = cpu_total.total() - run.thread_cpu_seconds;
    const double event_count = static_cast<double>(ok) * static_cast<double>(events);
    const double peak_kb_per_stream =
        static_cast<double>(rss_peak > rss_before ? rss_peak - rss_before : 0) / 1024.0 / concurrency;

    std::printf("mode=gateway upstream_port=%d concurrency=%d events=%d body=%d io_threads=%d timeout_ms=%d\n",
                upstream_port, concurrency, events, body_bytes, io_threads, timeout_ms);
    std::printf("  status: %d ok, %d failed in %.2fs", ok, failures, run.wall_seconds);
    if (failures > 0)
        std::printf(" [connect=%d send=%d read=%d short=%d first: stage=%d errno=%d recv=%d events=%d]",
                    state.connect_failures, state.send_failures, state.read_failures, short_streams,
                    state.first_stage, state.first_errno, state.first_recv, first_short_events);
    std::printf("\n");
    if (!pct_ttft.empty())
        std::printf("  ttft ms:  p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_ttft[0], pct_ttft[1],
                    pct_ttft[2], pct_ttft[3], pct_ttft[5]);
    if (!pct_total.empty())
        std::printf("  total ms: p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct_total[0], pct_total[1],
                    pct_total[2], pct_total[3], pct_total[5]);
    std::printf("  cpu: user=%.2fs sys=%.2fs total=%.2fs (clients=%.2fs proxy=%.2fs)\n", cpu_total.user,
                cpu_total.system, cpu_total.total(), run.thread_cpu_seconds, proxy_cpu);
    if (event_count > 0)
        std::printf("  cpu per event: total=%.2fus proxy-only=%.2fus (%d events)\n",
                    cpu_total.total() * 1e6 / event_count, proxy_cpu * 1e6 / event_count,
                    static_cast<int>(event_count));
    std::printf("  rss: before=%.1fMB peak=%.1fMB current=%.1fMB sampled-max=%.1fMB\n", megabytes(rss_before),
                megabytes(rss_peak), megabytes(bench::current_rss_bytes()),
                megabytes(sampled_rss_max.load(std::memory_order_relaxed)));
    std::printf("  peak per stream: %.1fKB\n", peak_kb_per_stream);
    return failures == 0 ? 0 : 1;
}

} // namespace

int main(int argc, char **argv)
{
    const std::string mode = argc > 1 ? argv[1] : "requests";

    if (mode == "requests") {
        const int total = argc > 2 ? std::atoi(argv[2]) : 20000;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : 100;
        const int connections = argc > 4 ? std::atoi(argv[4]) : 1;
        return run_requests(total, concurrency, connections);
    }
    if (mode == "requests_mt") {
        const int total = argc > 2 ? std::atoi(argv[2]) : 20000;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : 100;
        const int workers = argc > 4 ? std::atoi(argv[4]) : 4;
        const int connections = argc > 5 ? std::atoi(argv[5]) : 1;
        return run_requests_mt(total, concurrency, workers, connections);
    }
    if (mode == "h1_unary") {
        const int total = argc > 2 ? std::atoi(argv[2]) : 100000;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : 8;
        const int threads = argc > 4 ? std::atoi(argv[4]) : concurrency;
        return run_h1_server_unary(total, concurrency, threads);
    }
    if (mode == "h2_unary" || mode == "native_unary") {
        const int total = argc > 2 ? std::atoi(argv[2]) : 20000;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : 100;
        const int workers = argc > 4 ? std::atoi(argv[4]) : 1;
        const bool native = mode == "native_unary" ||
                            (argc > 5 && (std::string(argv[5]) == "native" || std::string(argv[5]) == "1"));
        return run_h2_server_unary(total, concurrency, workers,
                                   native ? realurl::Protocol::Native : realurl::Protocol::Http2);
    }
    if (mode == "hold") {
        const int streams = argc > 2 ? std::atoi(argv[2]) : 1000;
        return run_hold(streams);
    }
    if (mode == "llm") {
        const int total = argc > 2 ? std::atoi(argv[2]) : 10;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : total;
        const int duration = argc > 4 ? std::atoi(argv[4]) : 20;
        const bool streaming = argc > 5 ? (std::string(argv[5]) == "1" || std::string(argv[5]) == "true") : false;
        const int connections = argc > 6 ? std::atoi(argv[6]) : 1;
        return run_llm(total, concurrency, duration, streaming, connections);
    }
    if (mode == "native_llm") {
        const int total = argc > 2 ? std::atoi(argv[2]) : 10;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : total;
        const int duration = argc > 4 ? std::atoi(argv[4]) : 20;
        const bool streaming =
            argc > 5 ? (std::string(argv[5]) == "1" || std::string(argv[5]) == "true") : false;
        const int connections = argc > 6 ? std::atoi(argv[6]) : 1;
        const bool native =
            argc > 7 ? (std::string(argv[7]) == "native" || std::string(argv[7]) == "1") : true;
        return run_server_llm(total, concurrency, duration, streaming, connections,
                              native ? realurl::Protocol::Native : realurl::Protocol::Http2);
    }
    if (mode == "tls_connect") {
        // tls_connect <connections> [full|resume] [cert] [key]
        const int total = argc > 2 ? std::atoi(argv[2]) : 2000;
        const bool resume = argc > 3 ? std::string(argv[3]) == "resume" : true;
        const std::string cert = argc > 4 ? argv[4] : REALURL_CERT_DIR "/localhost.pem";
        const std::string key = argc > 5 ? argv[5] : REALURL_CERT_DIR "/localhost-key.pem";
        return run_tls_connect(total, resume, cert, key);
    }
    if (mode == "native_bulk") {
        // native_bulk <requests> <concurrency> <body_kib> <connections> <protocol>
        const int total = argc > 2 ? std::atoi(argv[2]) : 200;
        const int concurrency = argc > 3 ? std::atoi(argv[3]) : 8;
        const int body_kib = argc > 4 ? std::atoi(argv[4]) : 2048;
        const int connections = argc > 5 ? std::atoi(argv[5]) : 1;
        const bool native =
            argc > 6 ? (std::string(argv[6]) == "native" || std::string(argv[6]) == "1") : true;
        return run_server_llm(total, concurrency, 0, false, connections,
                              native ? realurl::Protocol::Native : realurl::Protocol::Http2,
                              static_cast<std::size_t>(std::max(1, body_kib)) * 1024, true);
    }
    if (mode == "pump" || mode == "async") {
        const int concurrency = argc > 2 ? std::atoi(argv[2]) : 128;
        const int chunks = argc > 3 ? std::atoi(argv[3]) : 10;
        const int interval_ms = argc > 4 ? std::atoi(argv[4]) : 100;
        const int timeout_ms = argc > 5 ? std::atoi(argv[5]) : 30000;
        return run_pump(concurrency, chunks, interval_ms, timeout_ms, mode == "async");
    }
    if (mode == "proxy") {
        const int concurrency = argc > 2 ? std::atoi(argv[2]) : 128;
        const int chunks = argc > 3 ? std::atoi(argv[3]) : 10;
        const int interval_ms = argc > 4 ? std::atoi(argv[4]) : 100;
        const int timeout_ms = argc > 5 ? std::atoi(argv[5]) : 30000;
        const int workers = argc > 6 ? std::atoi(argv[6]) : 1;
        const bool bridged = argc > 7 ? std::string(argv[7]) != "block" : true;
        return run_proxy(concurrency, chunks, interval_ms, timeout_ms, workers, bridged);
    }
    if (mode == "proxy_llm") {
        const int concurrency = argc > 2 ? std::atoi(argv[2]) : 100;
        const int duration_s = argc > 3 ? std::atoi(argv[3]) : 20;
        const int body_bytes = argc > 4 ? std::atoi(argv[4]) : 2 * 1024 * 1024;
        const int workers = argc > 5 ? std::atoi(argv[5]) : 1;
        const bool bridged = argc > 6 ? std::string(argv[6]) != "block" : true;
        return run_proxy_llm(concurrency, duration_s, body_bytes, workers, bridged);
    }
    if (mode == "proxy_inline") {
        const int concurrency = argc > 2 ? std::atoi(argv[2]) : 100;
        const int duration_s = argc > 3 ? std::atoi(argv[3]) : 20;
        const int body_bytes = argc > 4 ? std::atoi(argv[4]) : 2 * 1024 * 1024;
        return run_proxy_llm(concurrency, duration_s, body_bytes, 1, false, true);
    }
    if (mode == "sse_upstream") {
        // sse_upstream <port> <events> <interval_ms> <event_bytes> [io_threads]
        if (argc < 6) {
            std::fprintf(stderr, "usage: %s sse_upstream <port> <events> <interval_ms> <event_bytes> [io_threads]\n",
                         argv[0]);
            return 2;
        }
        return run_sse_upstream(std::atoi(argv[2]), std::atoi(argv[3]), std::atoi(argv[4]), std::atoi(argv[5]),
                                argc > 6 ? std::atoi(argv[6]) : 1);
    }
    if (mode == "gateway") {
        // gateway <upstream_port> <concurrency> [body_bytes] [io_threads] [timeout_ms]
        if (argc < 4) {
            std::fprintf(stderr,
                         "usage: %s gateway <upstream_port> <concurrency> [body_bytes] [io_threads] [timeout_ms]\n",
                         argv[0]);
            return 2;
        }
        return run_gateway(std::atoi(argv[2]), std::atoi(argv[3]), argc > 4 ? std::atoi(argv[4]) : 2000,
                           argc > 5 ? std::atoi(argv[5]) : 1, argc > 6 ? std::atoi(argv[6]) : 120000);
    }

    std::fprintf(stderr,
                 "usage: %s requests <N> <concurrency> [connections] | requests_mt <N> <concurrency> <workers> "
                 "[connections] | h2_unary|native_unary <N> <concurrency> <workers> [h2|native] | hold <streams> | llm <total> <concurrency> [duration_s] [streaming] [connections] "
                 "| native_llm <total> <concurrency> [duration_s] [streaming] [connections] [h2|native] "
                 "| native_bulk <N> <concurrency> [body_kib] [connections] [h2|native] "
                 "| tls_connect <N> [full|resume] [cert] [key] "
                 "| pump <concurrency> [chunks] [interval_ms] [timeout_ms] "
                 "| async <concurrency> [chunks] [interval_ms] [timeout_ms] "
                 "| proxy <concurrency> [chunks] [interval_ms] [timeout_ms] [workers] [bridge|block] "
                 "| proxy_llm <concurrency> [duration_s] [body_bytes] [workers] [bridge|block] "
                 "| proxy_inline <concurrency> [duration_s] [body_bytes] "
                 "| sse_upstream <port> <events> <interval_ms> <event_bytes> [io_threads] "
                 "| gateway <upstream_port> <concurrency> [body_bytes] [io_threads] [timeout_ms]\n",
                 argv[0]);
    return 2;
}
