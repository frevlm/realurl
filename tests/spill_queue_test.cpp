#include "realurl/detail/event_loop.hpp"
#include "realurl/detail/spill_queue.hpp"
#include "test_harness.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using realurl::EventLoop;
using realurl::SpillStore;
using realurl::detail::SpillConfig;
using realurl::detail::SpillQueue;

namespace
{

std::string pattern(std::size_t offset, std::size_t n)
{
    std::string out(n, '\0');
    for (std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<char>((offset + i) % 251);
    return out;
}

std::shared_ptr<const SpillConfig> config(std::size_t threshold, bool offload,
                                          realurl::SpillStoreFactory store = {},
                                          std::uint64_t total_limit = 0, std::string dir = {})
{
    auto out = std::make_shared<SpillConfig>();
    out->threshold = threshold;
    out->dir = std::move(dir);
    out->store = std::move(store);
    out->offload = offload;
    out->total_limit = total_limit;
    return out;
}

/// Run `loop` until `done()` or about 5 s pass. Each poll is woken at once:
/// a posted result runs at the start of the next iteration, and helper-side
/// effects such as a store's destruction never wake the loop.
template <class Pred>
bool run_until(EventLoop &loop, Pred done)
{
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        loop.wakeup();
        loop.run_once();
    }
    return true;
}

/// Read up to `n` bytes, waiting out offloaded fetches.
std::string read_n(SpillQueue &queue, EventLoop &loop, std::size_t n)
{
    std::string out(n, '\0');
    std::size_t got = 0;
    while (got < n && !queue.empty()) {
        const std::size_t r = queue.read({ out.data() + got, n - got });
        got += r;
        if (r == 0 && !run_until(loop, [&] { return !queue.busy(); }))
            break;
    }
    out.resize(got);
    return out;
}

void drain_all(SpillQueue &queue, EventLoop &loop, std::string &out)
{
    for (;;) {
        queue.drain_to(out);
        if (queue.empty() || !run_until(loop, [&] { return !queue.busy(); }))
            return;
    }
}

/// Counters a MemoryStore shares with the test.
struct StoreStats {
    std::atomic<int> appends{ 0 };
    std::atomic<int> reads{ 0 };
    std::atomic<int> resets{ 0 };
    std::atomic<int> destroyed{ 0 };
    /// Overlapping calls, or a reset before the reader caught up.
    std::atomic<int> violations{ 0 };
    /// Appends and reads after this many calls fail; -1 never.
    int fail_appends_after = -1;
    int fail_reads_after = -1;
    std::chrono::milliseconds delay{ 0 };
};

/// In-memory SpillStore that checks the caller keeps to the contract.
class MemoryStore final : public SpillStore {
public:
    explicit MemoryStore(std::shared_ptr<StoreStats> stats)
        : stats_(std::move(stats))
    {
    }

    ~MemoryStore() override
    {
        ++stats_->destroyed;
    }

    bool append(std::string_view bytes) override
    {
        const Guard guard(*this);
        const int n = stats_->appends++;
        if (stats_->fail_appends_after >= 0 && n >= stats_->fail_appends_after)
            return false;
        log_.append(bytes);
        return true;
    }

    bool read(std::span<char> out) override
    {
        const Guard guard(*this);
        const int n = stats_->reads++;
        if ((stats_->fail_reads_after >= 0 && n >= stats_->fail_reads_after) || out.size() > log_.size() - head_)
            return false;
        std::copy_n(log_.data() + head_, out.size(), out.data());
        head_ += out.size();
        return true;
    }

    void reset() override
    {
        const Guard guard(*this);
        ++stats_->resets;
        if (head_ != log_.size())
            ++stats_->violations;
        log_.clear();
        head_ = 0;
    }

private:
    /// Flags overlapping calls and applies the configured delay.
    struct Guard {
        explicit Guard(MemoryStore &store)
            : store_(store)
        {
            if (store_.busy_.exchange(true))
                ++store_.stats_->violations;
            if (store_.stats_->delay.count() > 0)
                std::this_thread::sleep_for(store_.stats_->delay);
        }
        ~Guard()
        {
            store_.busy_.store(false);
        }
        MemoryStore &store_;
    };

    std::shared_ptr<StoreStats> stats_;
    std::atomic<bool> busy_{ false };
    std::string log_;
    std::size_t head_ = 0;
};

realurl::SpillStoreFactory memory_store(const std::shared_ptr<StoreStats> &stats)
{
    return [stats] { return std::make_unique<MemoryStore>(stats); };
}

/// Random appends, reads and drains against a string model. The threshold is
/// small so the head/store boundary is crossed constantly.
void model_test(EventLoop &loop, std::shared_ptr<const SpillConfig> cfg)
{
    SpillQueue queue(std::move(cfg), &loop);
    std::mt19937 rng(20261023);
    std::string model;
    std::size_t model_head = 0;
    std::size_t written = 0;
    bool ok = true;

    for (int op = 0; op < 4000 && ok; ++op) {
        const unsigned kind = rng() % 10;
        if (kind < 5) {
            const std::size_t n = 1 + rng() % 20000;
            const std::string bytes = pattern(written, n);
            written += n;
            queue.append(bytes);
            model.append(bytes);
        } else if (kind < 9) {
            const std::size_t n = 1 + rng() % 30000;
            const std::string got = read_n(queue, loop, n);
            const std::size_t expect = std::min(n, model.size() - model_head);
            ok = got.size() == expect && model.compare(model_head, expect, got) == 0;
            model_head += expect;
        } else {
            std::string got = "x";
            drain_all(queue, loop, got);
            ok = got.size() == 1 + model.size() - model_head && got.compare(1, std::string::npos, model, model_head) == 0;
            model_head = model.size();
        }
        ok = ok && queue.size() == model.size() - model_head;
    }
    CHECK(ok);
    CHECK(queue.has_store());
}

/// 20 MiB through a 64 KiB threshold, reading at a third of the append rate
/// so the store keeps growing, round-trips byte-exact.
void bulk_test(EventLoop &loop, std::shared_ptr<const SpillConfig> cfg)
{
    constexpr std::size_t kTotal = 20 * 1024 * 1024;
    SpillQueue queue(std::move(cfg), &loop);
    std::size_t appended = 0;
    std::size_t consumed = 0;
    bool ok = true;
    while (appended < kTotal) {
        const std::size_t n = std::min<std::size_t>(37 * 1024 + 11, kTotal - appended);
        queue.append(pattern(appended, n));
        appended += n;
        if ((appended / n) % 3 == 0) {
            const std::string got = read_n(queue, loop, 16 * 1024);
            ok = ok && got == pattern(consumed, got.size());
            consumed += got.size();
        }
    }
    CHECK(queue.stored() > kTotal / 4);
    for (;;) {
        const std::string got = read_n(queue, loop, 16 * 1024);
        if (got.empty())
            break;
        ok = ok && got == pattern(consumed, got.size());
        consumed += got.size();
    }
    CHECK(ok);
    CHECK(consumed == kTotal);
    CHECK(queue.empty());
    CHECK(SpillQueue::total_spilled() == 0);
}

} // namespace

int main()
{
    EventLoop loop;

    for (const bool offload : { false, true }) {
        model_test(loop, config(4096, offload));
        auto stats = std::make_shared<StoreStats>();
        model_test(loop, config(4096, offload, memory_store(stats)));
        CHECK(stats->violations == 0);
        CHECK(run_until(loop, [&] { return stats->destroyed == 1; }));
        CHECK(SpillQueue::total_spilled() == 0);
        bulk_test(loop, config(64 * 1024, offload));
    }

    {
        // Bytes past the threshold go to the store; reading past the head
        // fetches them back, and an emptied store is reset.
        auto stats = std::make_shared<StoreStats>();
        SpillQueue queue(config(16, false, memory_store(stats)), &loop);
        queue.append(std::string(16, 'a'));
        CHECK(!queue.has_store());
        queue.append(pattern(0, 100));
        CHECK(queue.has_store());
        CHECK(queue.stored() == 100);
        CHECK(queue.size() == 116);
        CHECK(SpillQueue::total_spilled() == 100);

        CHECK(read_n(queue, loop, 16) == std::string(16, 'a'));
        CHECK(read_n(queue, loop, 10) == pattern(0, 10));
        CHECK(queue.stored() == 0);
        CHECK(stats->resets == 1);
        CHECK(SpillQueue::total_spilled() == 0);
        CHECK(read_n(queue, loop, 1000) == pattern(10, 90));
        CHECK(queue.empty());
    }

    {
        // Once the store holds bytes, later appends follow it even when they
        // would fit in memory; FIFO order holds across head, store and
        // pending bytes.
        SpillQueue queue(config(8, false), &loop);
        queue.append("12345678");
        queue.append(pattern(0, 200000));
        queue.append("z");
        CHECK(queue.stored() == 200001);
        CHECK(read_n(queue, loop, 8) == "12345678");
        CHECK(read_n(queue, loop, 200000) == pattern(0, 200000));
        CHECK(read_n(queue, loop, 10) == "z");
        CHECK(queue.empty());
    }

    {
        // Offload: appends past the threshold are in flight, counted by
        // size() and as awaiting the store until it has them. Appends made
        // while a write runs go to the store as one batch. An empty head
        // reads 0 until the fetch lands.
        auto stats = std::make_shared<StoreStats>();
        stats->delay = 20ms;
        SpillQueue queue(config(8, true, memory_store(stats)), &loop);
        queue.append("12345678");
        queue.append(pattern(0, 1000));
        CHECK(queue.busy());
        CHECK(queue.stored() == 0);
        CHECK(queue.size() == 1008);
        queue.append("tail");
        CHECK(queue.size() == 1012);
        CHECK(queue.awaiting_store() == 1004);
        char buffer[16];
        CHECK(queue.read(buffer) == 8);
        CHECK(queue.read(buffer) == 0);
        CHECK(queue.size() == 1004);
        CHECK(read_n(queue, loop, 2000) == pattern(0, 1000) + "tail");
        CHECK(queue.empty());
        CHECK(queue.awaiting_store() == 0);
        CHECK(stats->appends == 2);
        CHECK(stats->violations == 0);
    }

    {
        // The ready callback fires on the loop after each offloaded job, and
        // the reader retries until the fetch has landed.
        int calls = 0;
        SpillQueue queue(config(8, true), &loop);
        queue.set_ready_callback([](void *context) { ++*static_cast<int *>(context); }, &calls);
        queue.append(pattern(0, 100000));
        char buffer[100];
        CHECK(queue.read(buffer) == 0);
        int seen = 0;
        std::size_t got = 0;
        CHECK(run_until(loop, [&] {
            if (calls == seen)
                return false;
            seen = calls;
            got = queue.read(buffer);
            return got > 0;
        }));
        CHECK(calls >= 2);
        CHECK(got == sizeof(buffer));
        CHECK(std::string_view(buffer, sizeof(buffer)) == pattern(0, 100));
    }

    {
        // Threshold 0 is a plain in-memory queue.
        SpillQueue queue;
        queue.append(std::string(8 * 1024 * 1024, 'm'));
        queue.append("end");
        CHECK(!queue.has_store());
        CHECK(queue.size() == 8 * 1024 * 1024 + 3);
        std::string out;
        queue.drain_to(out);
        CHECK(out.size() == 8 * 1024 * 1024 + 3);
        CHECK(out.substr(out.size() - 3) == "end");
    }

    for (const bool offload : { false, true }) {
        // An unusable spill directory keeps everything in memory.
        SpillQueue queue(config(16, offload, {}, 0, "/nonexistent/realurl-spill-dir"), &loop);
        queue.append(pattern(0, 10));
        queue.append(pattern(10, 50000));
        queue.append(pattern(50010, 70000));
        CHECK(queue.size() == 120010);
        CHECK(read_n(queue, loop, 200000) == pattern(0, 120010));
        CHECK(SpillQueue::total_spilled() == 0);
    }

    for (const bool offload : { false, true }) {
        // A factory returning nullptr keeps everything in memory.
        SpillQueue queue(config(16, offload, [] { return std::unique_ptr<SpillStore>(); }), &loop);
        queue.append(pattern(0, 40000));
        CHECK(!queue.has_store());
        CHECK(read_n(queue, loop, 50000) == pattern(0, 40000));
    }

    for (const bool offload : { false, true }) {
        // A store that fails after the first append: what it holds is read
        // back, everything later stays in memory, in order.
        auto stats = std::make_shared<StoreStats>();
        stats->fail_appends_after = 1;
        SpillQueue queue(config(16, offload, memory_store(stats)), &loop);
        std::size_t appended = 0;
        for (int i = 0; i < 10; ++i) {
            queue.append(pattern(appended, 3000));
            appended += 3000;
        }
        CHECK(read_n(queue, loop, appended) == pattern(0, appended));
        CHECK(queue.empty());
        CHECK(SpillQueue::total_spilled() == 0);
    }

    for (const bool offload : { false, true }) {
        // A failed store read is an error: the bytes exist nowhere else.
        auto stats = std::make_shared<StoreStats>();
        stats->fail_reads_after = 0;
        SpillQueue queue(config(16, offload, memory_store(stats)), &loop);
        queue.append(pattern(0, 16));
        queue.append(pattern(16, 1000));
        CHECK(read_n(queue, loop, 16) == pattern(0, 16));
        bool threw = false;
        try {
            read_n(queue, loop, 1000);
            char byte;
            queue.read({ &byte, 1 });
        } catch (const std::system_error &error) {
            threw = error.code().value() == EIO;
        }
        CHECK(threw);
        threw = false;
        try {
            std::string out;
            queue.drain_to(out);
        } catch (const std::system_error &) {
            threw = true;
        }
        CHECK(threw);
        queue.clear();
        CHECK(queue.empty());
        CHECK(SpillQueue::total_spilled() == 0);
    }

    for (const bool offload : { false, true }) {
        // Process-wide cap: the second queue's spill does not fit, so it keeps
        // the bytes in memory and is throttled until it drains below its
        // threshold; then it spills again.
        constexpr std::size_t kThreshold = 16 * 1024;
        SpillQueue first(config(kThreshold, offload, {}, 100 * 1024), &loop);
        SpillQueue second(config(kThreshold, offload, {}, 100 * 1024), &loop);
        first.append(pattern(0, kThreshold));
        first.append(pattern(kThreshold, 90 * 1024));
        CHECK(run_until(loop, [&] { return !first.busy(); }));
        CHECK(first.stored() == 90 * 1024);
        CHECK(SpillQueue::total_spilled() == 90 * 1024);

        second.append(pattern(0, kThreshold));
        second.append(pattern(kThreshold, 20 * 1024));
        CHECK(!second.busy());
        CHECK(second.stored() == 0);
        CHECK(second.throttled());
        CHECK(!first.throttled());
        CHECK(SpillQueue::total_spilled() == 90 * 1024);

        // Draining the first queue frees the cap.
        CHECK(read_n(first, loop, kThreshold + 90 * 1024) == pattern(0, kThreshold + 90 * 1024));
        CHECK(SpillQueue::total_spilled() == 0);

        // Reading the second below its threshold lifts the throttle.
        CHECK(read_n(second, loop, 10 * 1024) == pattern(0, 10 * 1024));
        CHECK(second.throttled());
        CHECK(read_n(second, loop, 20 * 1024) == pattern(10 * 1024, 20 * 1024));
        CHECK(second.size() < kThreshold);
        CHECK(!second.throttled());

        second.append(pattern(kThreshold + 20 * 1024, 64 * 1024));
        CHECK(second.busy() || second.stored() > 0);
        const std::size_t rest = second.size();
        CHECK(read_n(second, loop, rest) == pattern(30 * 1024, rest));
        CHECK(SpillQueue::total_spilled() == 0);
    }

    {
        // Destroying or clearing a queue with a job in flight: the store
        // lives until the job ends, late results are dropped, and the cap
        // is released at once.
        auto stats = std::make_shared<StoreStats>();
        stats->delay = 50ms;
        {
            SpillQueue queue(config(16, true, memory_store(stats)), &loop);
            queue.append(pattern(0, 16));
            queue.append(pattern(16, 5000));
            CHECK(queue.busy());
        }
        CHECK(SpillQueue::total_spilled() == 0);
        CHECK(run_until(loop, [&] { return stats->destroyed == 1; }));

        SpillQueue queue(config(16, true, memory_store(stats)), &loop);
        queue.append(pattern(0, 16));
        queue.append(pattern(16, 5000));
        CHECK(run_until(loop, [&] { return !queue.busy(); }));
        char buffer[16];
        CHECK(queue.read(buffer) == 16);
        CHECK(queue.busy());
        queue.clear();
        CHECK(queue.empty());
        CHECK(!queue.busy());

        // The cleared queue spills into a fresh store while the old one is
        // still finishing its read.
        queue.append(pattern(100, 16));
        queue.append(pattern(116, 3000));
        CHECK(read_n(queue, loop, 4000) == pattern(100, 3016));
        CHECK(run_until(loop, [&] { return stats->destroyed == 2; }));
        CHECK(stats->violations == 0);
    }

    CHECK(SpillQueue::total_spilled() == 0);
    return test::summary();
}
