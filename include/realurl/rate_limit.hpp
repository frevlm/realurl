#pragma once

/// UploadRateLimiter — two-pool upload bandwidth coordinator.
///
/// Production wiring of the algorithm validated in
/// tests/rate_limit_two_pool.cc (8 rounds, all checks green).
///
/// Model:
///   - Streams are classified ONCE at join (pure function of Content-Length):
///       length >= classify_mb  → limited pool
///       length <  classify_mb  → normal pool (latency priority)
///       length unknown         → limited pool (from byte 0 — a client
///                                 streaming without a declared length is the
///                                 agent-big-request signature)
///   - Every limited flow carries a cap from its FIRST byte (cap = P initially,
///     keeping curl's MAX_SEND_SPEED window warm — a cap applied mid-flight
///     stalls 6.5–10.9 s because curl anchors the window at transfer start).
///   - Every 200 ms tick (driven by the stream owner thread's event loop):
///       backlog = Σ normal flows (Content-Length − uploaded)   // measured
///       claim   = min(backlog / 1 s, P)
///       budget  = clamp(P − claim − 5%·P, 10%·P, P − 5%·P)
///       target  = max(budget / N_limited, 32 KB/s)
///     Down-scaling cuts at most 2× per tick and never cuts a flow that made
///     no progress last tick (consecutive halvings chain into one long
///     stall — measured 8.4 s).
///   - Upstream sockets are bounded via SO_SNDBUF = 64 KB (see
///     curl_multi_pool setup) — without it `uploaded` (CURLINFO_SIZE_UPLOAD_T)
///     leads the wire by MBs and the backlog signal is blind.
///
/// Threading: each stream is driven by ONE thread (the pump-pool thread that
/// owns its curl_multi).  The owner calls tick() from its pump loop; the
/// coordinator's mutex serialises the (cheap) bookkeeping.  CURLOPT cap
/// writes happen only in tick()/join()/finish() — i.e. on the owner thread —
/// which keeps easy-handle access thread-affine.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace realurl
{

/// One registered upload flow.  Create via limiter.join(), destroy via
/// limiter.finish().  The owner thread may read `desired_cap_bps` to apply
/// the latest cap to its curl handle; all coordinator bookkeeping is
/// internal to UploadRateLimiter.
class UploadRateFlow {
public:
    /// Constructed by UploadRateLimiter::join().  Public because
    /// std::make_shared constructs through the allocator, which is not a
    /// friend — do not call directly.
    explicit UploadRateFlow(long long classify_bytes, long long content_length)
        : classify_bytes_(classify_bytes)
        , content_length_(content_length)
        , limited_(content_length < 0 || content_length >= classify_bytes)
    {
    }

    /// Cap (bytes/s) the coordinator currently wants applied.  Reads are
    /// lock-free; the owner applies it with curl_easy_setopt when it changes.
    long long desired_cap_bps() const
    {
        return desired_cap_bps_.load(std::memory_order_acquire);
    }

private:
    friend class UploadRateLimiter;

    std::atomic<long long> uploaded_{ 0 };
    std::atomic<long long> desired_cap_bps_{ 0 };
    std::atomic<long long> prev_uploaded_{ 0 };
    std::atomic<bool> tick_progress_{ false };

    long long classify_bytes_ = 0;
    long long content_length_ = 0;
    bool limited_ = false;
};

/// Global two-pool upload coordinator.  When disabled (pipe_bps == 0) all
/// methods are cheap no-ops, so callers pay nothing unless the feature is
/// configured.
class UploadRateLimiter {
public:
    static UploadRateLimiter &instance();

    /// Configure the pipe rate.  Called once at startup from config; 0
    /// disables limiting.
    void configure(long long pipe_bps, long long classify_bytes);

    bool enabled() const
    {
        return pipe_bps_.load(std::memory_order_relaxed) > 0;
    }

    /// Total pipe rate in bytes/s (0 = disabled).
    long long pipe_bps() const
    {
        return pipe_bps_.load(std::memory_order_relaxed);
    }

    /// Register a stream.  content_length < 0 means unknown (→ limited).
    /// Returns the flow handle; the owner keeps it for the stream lifetime.
    std::shared_ptr<UploadRateFlow> join(long long content_length);

    /// Mark a stream finished; its share is redistributed on the next tick.
    void finish(const std::shared_ptr<UploadRateFlow> &flow);

    /// Owner-thread hook: report `uploaded` bytes (CURLINFO_SIZE_UPLOAD_T)
    /// and run a coordinator tick if one is due (200 ms).  Called from the
    /// pump loop on the stream's owning thread.
    void tick(const std::shared_ptr<UploadRateFlow> &flow, long long uploaded);

    /// Force an immediate allocation (join/finish events) regardless of the
    /// tick interval — caps must be set before the first byte is sent.
    void tick_now();

private:
    UploadRateLimiter() = default;

    void allocate();

    std::atomic<long long> pipe_bps_{ 0 };
    std::atomic<long long> classify_bytes_{ 5LL * 1024 * 1024 };
    std::mutex mu_;
    std::unordered_map<UploadRateFlow *, std::shared_ptr<UploadRateFlow>> flows_;
    std::chrono::steady_clock::time_point last_tick_{};
};

UploadRateLimiter &upload_rate_limiter();

} // namespace realurl
