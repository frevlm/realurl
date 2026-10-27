// Self-test and throughput probe for the database spill backends.
//
//   spill_backends_test <backend> [arg]
//
// Creates 8 stores and drives them from 2 threads (4 stores each). Every store
// moves ~20 MiB through random-sized appends and reads (fixed seed), checked
// byte for byte against a std::string model, with a few resets in between.

#include "spill_backends.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace
{

constexpr size_t kTotalPerStore = 20u << 20;
constexpr size_t kMaxBacklog = 6u << 20;
constexpr int kStores = 8;
constexpr int kThreads = 2;

struct Rng {
    uint64_t s;
    uint64_t next()
    {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        return s;
    }
    size_t upto(size_t n) { return static_cast<size_t>(next() % n); }
};

/// Unread bytes the store should still hold.
struct Model {
    std::string buf;
    size_t head = 0;
    size_t size() const { return buf.size() - head; }
    void push(std::string_view b) { buf.append(b); }
    const char* front() const { return buf.data() + head; }
    void pop(size_t n)
    {
        head += n;
        if (head > (1u << 20) && head * 2 > buf.size()) {
            buf.erase(0, head);
            head = 0;
        }
    }
    void clear()
    {
        buf.clear();
        head = 0;
    }
};

struct Result {
    double append_s = 0, read_s = 0;
    size_t appended = 0, read = 0;
    bool ok = true;
    std::string why;
};

using Clock = std::chrono::steady_clock;

double since(Clock::time_point t)
{
    return std::chrono::duration<double>(Clock::now() - t).count();
}

void run_store(realurl::SpillStore& st, uint64_t seed, Result& r)
{
    Rng rng{seed * 0x9E3779B97F4A7C15ull + 1};
    Model m;
    std::string chunk, got;
    size_t appended = 0;
    int resets = 0;

    auto fail = [&](const char* why) {
        r.ok = false;
        r.why = why;
    };
    auto do_read = [&](size_t n) {
        got.resize(n);
        const auto t = Clock::now();
        const bool ok = st.read(std::span<char>(got.data(), n));
        r.read_s += since(t);
        if (!ok)
            return fail("read returned false");
        if (std::memcmp(got.data(), m.front(), n) != 0)
            return fail("read data mismatch");
        m.pop(n);
        r.read += n;
    };

    while (r.ok && (appended < kTotalPerStore || m.size() > 0)) {
        const bool can_append = appended < kTotalPerStore && m.size() < kMaxBacklog;
        const size_t pick = rng.upto(100);
        if (can_append && (pick < 55 || m.size() == 0)) {
            // log-uniform size, 1 byte .. 128 KiB
            const size_t cap = size_t{1} << rng.upto(18);
            size_t n = 1 + rng.upto(cap);
            n = std::min(n, kTotalPerStore - appended);
            chunk.resize(n);
            for (size_t i = 0; i < n; i += 8) {
                const uint64_t v = rng.next();
                std::memcpy(chunk.data() + i, &v, std::min<size_t>(8, n - i));
            }
            const auto t = Clock::now();
            const bool ok = st.append(chunk);
            r.append_s += since(t);
            if (!ok)
                return fail("append returned false");
            m.push(chunk);
            appended += n;
            r.appended += n;
        } else if (m.size() > 0) {
            if (pick > 97 && resets < 3 && appended > kTotalPerStore / 4) {
                st.reset();
                m.clear();
                ++resets;
                continue;
            }
            const size_t cap = size_t{1} << rng.upto(18);
            do_read(1 + std::min(m.size() - 1, rng.upto(cap)));
        }
    }
    if (r.ok && resets == 0)
        fail("no reset exercised");
    // The store must be usable again after draining.
    if (r.ok) {
        st.reset();
        m.clear();
        const std::string probe = "hello, spill";
        if (!st.append(probe))
            return fail("append after reset failed");
        m.push(probe);
        do_read(probe.size());
    }
}

} // namespace

int main(int argc, char** argv)
{
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <sqlite|sqlite_mt|postgres> [arg]\n", argv[0]);
        return 2;
    }
    const std::string name = argv[1];
    const std::string arg = argc > 2 ? argv[2] : "";
    auto factory = bench::make_spill_store(name, arg);
    if (!factory) {
        std::fprintf(stderr, "backend '%s' unavailable (not compiled in, or connection failed)\n", name.c_str());
        return 2;
    }

    std::vector<std::unique_ptr<realurl::SpillStore>> stores;
    for (int i = 0; i < kStores; ++i) {
        stores.push_back(factory());
        if (!stores.back()) {
            std::puts("FAIL: factory returned null store");
            return 1;
        }
    }

    std::vector<Result> res(kStores);
    std::vector<std::thread> threads;
    const auto t0 = Clock::now();
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int i = t; i < kStores; i += kThreads)
                run_store(*stores[i], 1000 + i, res[i]);
        });
    for (auto& th : threads)
        th.join();
    const double wall = since(t0);

    // Aggregate: bytes over the busiest thread's time in that operation.
    double app_t[kThreads] = {}, rd_t[kThreads] = {};
    size_t app_b = 0, rd_b = 0;
    bool ok = true;
    for (int i = 0; i < kStores; ++i) {
        app_t[i % kThreads] += res[i].append_s;
        rd_t[i % kThreads] += res[i].read_s;
        app_b += res[i].appended;
        rd_b += res[i].read;
        if (!res[i].ok) {
            ok = false;
            std::printf("store %d: %s\n", i, res[i].why.c_str());
        }
    }
    const double at = *std::max_element(app_t, app_t + kThreads);
    const double rt = *std::max_element(rd_t, rd_t + kThreads);
    const double mb = 1024.0 * 1024.0;
    std::printf("%s: append %.1f MiB/s (%.0f MiB), read %.1f MiB/s (%.0f MiB), wall %.2fs\n", name.c_str(),
                static_cast<double>(app_b) / mb / at, static_cast<double>(app_b) / mb,
                static_cast<double>(rd_b) / mb / rt, static_cast<double>(rd_b) / mb, wall);
    stores.clear();
    std::puts(ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
