#include "realurl/rate_limit.hpp"

#include <algorithm>
#include <chrono>

namespace realurl
{
namespace
{

constexpr std::chrono::milliseconds kTickDuration{ 200 }; // coordinator tick
constexpr long long kMinFlowCapBps = 32 * 1024; // per-flow cap floor
constexpr long long kBudgetFloorFrac = 10; // % of P the limited pool never drops below
constexpr long long kHeadroomFrac = 5; // % of P reserved as headroom

} // namespace

UploadRateLimiter &UploadRateLimiter::instance()
{
    static UploadRateLimiter limiter;
    return limiter;
}

UploadRateLimiter &upload_rate_limiter()
{
    return UploadRateLimiter::instance();
}

void UploadRateLimiter::configure(long long pipe_bps, long long classify_bytes)
{
    pipe_bps_.store(pipe_bps, std::memory_order_relaxed);
    if (classify_bytes > 0) {
        classify_bytes_.store(classify_bytes, std::memory_order_relaxed);
    }
    if (pipe_bps > 0) {
        last_tick_ = {};
    }
}

std::shared_ptr<UploadRateFlow> UploadRateLimiter::join(long long content_length)
{
    auto flow = std::make_shared<UploadRateFlow>(classify_bytes_.load(std::memory_order_relaxed), content_length);
    // Warm curl's MAX_SEND_SPEED window from byte 0: cap = P at join.  A cap
    // applied mid-flight stalls 6.5–10.9 s (measured) because curl anchors
    // the window near transfer start; a pre-warmed window cuts at most ~1
    // tick.  Desired caps are only ever applied by the flow's owner thread.
    if (flow->limited_) {
        flow->desired_cap_bps_.store(pipe_bps_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    } else {
        flow->desired_cap_bps_.store(0, std::memory_order_relaxed); // normal pool: no cap
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        flows_.emplace(flow.get(), flow);
    }
    tick_now(); // allocate immediately — cap must be set before the first byte
    return flow;
}

void UploadRateLimiter::finish(const std::shared_ptr<UploadRateFlow> &flow)
{
    {
        std::lock_guard<std::mutex> lock(mu_);
        flows_.erase(flow.get());
    }
    tick_now(); // redistribute the freed share
}

void UploadRateLimiter::tick(const std::shared_ptr<UploadRateFlow> &flow, long long uploaded)
{
    if (pipe_bps_.load(std::memory_order_relaxed) <= 0)
        return;
    flow->uploaded_.store(uploaded, std::memory_order_relaxed);

    const auto now = std::chrono::steady_clock::now();
    bool due = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (last_tick_ == std::chrono::steady_clock::time_point{}) {
            last_tick_ = now;
            due = true;
        } else if (now - last_tick_ >= kTickDuration) {
            last_tick_ = now;
            due = true;
        }
    }
    if (due) {
        allocate();
    }
}

void UploadRateLimiter::tick_now()
{
    if (pipe_bps_.load(std::memory_order_relaxed) <= 0)
        return;
    std::lock_guard<std::mutex> lock(mu_);
    last_tick_ = std::chrono::steady_clock::now();
    allocate();
}

void UploadRateLimiter::allocate()
{
    const long long P = pipe_bps_.load(std::memory_order_relaxed);
    if (P <= 0)
        return;

    // 1) normal-pool demand = Σ remaining bytes of active normal flows.
    //    Classification is by Content-Length, so demand is MEASURED, not
    //    estimated — no EWMA, no decay knobs (probe finding 4).
    long long backlog = 0;
    int n_limited = 0;
    for (auto &[ptr, flow] : flows_) {
        const long long uploaded = flow->uploaded_.load(std::memory_order_relaxed);
        const long long prev = flow->prev_uploaded_.load(std::memory_order_relaxed);
        // Progress since last tick (used to skip cuts mid catch-down).
        flow->tick_progress_.store(uploaded > prev, std::memory_order_relaxed);
        flow->prev_uploaded_.store(uploaded, std::memory_order_relaxed);

        if (flow->limited_) {
            ++n_limited;
        } else if (flow->content_length_ > 0) {
            backlog += flow->content_length_ - uploaded;
            if (backlog < 0)
                backlog = 0;
        }
    }
    if (n_limited == 0)
        return;

    // 2) budget: pipe minus what the normal pool consumes over the 1 s
    //    horizon, minus fixed headroom.
    const long long claim = std::min(backlog, P); // backlog / 1 s
    const long long headroom = P / (100 / kHeadroomFrac);
    long long budget = P - claim - headroom;
    const long long floor = P / (100 / kBudgetFloorFrac);
    const long long ceiling = P - headroom;
    budget = std::clamp(budget, floor, ceiling);

    const long long target = std::max(budget / n_limited, kMinFlowCapBps);

    // 3) apply caps: raise freely; cut at most 2× per tick, and never cut a
    //    flow that made no progress last tick — consecutive cuts while curl
    //    is already catching down chain into one long stall (8.4 s measured).
    for (auto &[ptr, flow] : flows_) {
        if (!flow->limited_)
            continue;
        long long next = target;
        const long long cap = flow->desired_cap_bps_.load(std::memory_order_relaxed);
        if (next < cap) {
            if (flow->tick_progress_.load(std::memory_order_relaxed)) {
                next = std::max(next, cap / 2);
            } else {
                continue; // mid catch-down: leave it alone
            }
        }
        if (next != cap) {
            flow->desired_cap_bps_.store(next, std::memory_order_release);
        }
    }
}

} // namespace realurl
