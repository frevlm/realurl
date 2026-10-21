#include "resource.hpp"
#include "support/h2_test_server.hpp"
#include "realurl/http2.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace
{

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

    std::fprintf(stderr,
                 "usage: %s requests <N> <concurrency> [connections] | requests_mt <N> <concurrency> <workers> "
                 "[connections] | hold <streams> | llm <total> <concurrency> [duration_s] [streaming] [connections]\n",
                 argv[0]);
    return 2;
}
