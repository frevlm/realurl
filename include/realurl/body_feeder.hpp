#pragma once

#include "realurl/buffer_pool.hpp"
#include "realurl/body_source.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <unistd.h>

namespace realurl
{

/// Bounded sliding-window feeder for request bodies read from a client socket.
///
/// A dedicated feeder thread reads chunks from the socket and pushes them
/// into a small bounded queue (≤ kMaxQueuedChunks × kChunkSize bytes).  The
/// curl pool pulls from it via the BodySource (read/consume); when the queue
/// is full the feeder stops reading, so TCP backpressure propagates to the
/// client.  The first kPeekBytes are retained for billing-model extraction.
///
/// Memory is O(window) regardless of body size — a 500 MB body flows through
/// this ~192 KB window without ever existing as a whole object.
class BodyFeeder : public std::enable_shared_from_this<BodyFeeder> {
public:
    static constexpr size_t kChunkSize = BufferPool::kBufferSize; // 64 KB
    static constexpr size_t kMaxQueuedChunks = 2; // ≤ 128 KB in flight
    static constexpr size_t kPeekBytes = 65536; // billing-model peek window

    /// `pool` — shared buffer pool to borrow blocks from (design doc §2:
    /// one public pool instead of per-connection buffers; a connection
    /// waiting for readability holds 0 bytes).  When null, a small private
    /// pool is created per feeder (used by standalone probes).
    explicit BodyFeeder(BufferPool *pool = nullptr);

    /// Feeder thread entry: read body chunks from fd into the bounded queue.
    /// Blocks until EOF/error or cancel(); never touches the io thread.
    void run(int fd);

    /// Seed the queue with bytes that were over-read off the socket into the
    /// HTTP parser's buffer (async_read_header reads past the header
    /// terminator).  Call before starting run(); not thread-safe by design.
    void feed_initial(std::string_view bytes);

    /// Stop feeding (from any thread); unblocks run() and read().
    void cancel();

    /// Total body bytes read so far.  Used to stream-count against an
    /// optional proxy-side body limit (see `http_max_body_bytes`).
    std::atomic<long long> total_bytes{ 0 };

    /// Optional proxy-side body cap.  When > 0, run() stops reading once
    /// total_bytes exceeds it and marks limit_exceeded().  0 = unlimited
    /// (the default — oversized bodies pass through; the upstream's 413
    /// reaches the client).
    void set_body_limit(long long bytes);

    /// True once the body exceeded the configured limit.
    bool limit_exceeded() const
    {
        return limit_exceeded_.load(std::memory_order_acquire);
    }

    /// Build a BodySource backed by this feeder.  Call before starting run().
    std::shared_ptr<BodySource> make_source();

private:
    /// One queued body chunk: a pool-borrowed block + the number of valid
    /// bytes.  Destroying the Chunk returns the block to the pool.
    struct Chunk {
        BufferPool::Buffer buf;
        size_t size = 0;
    };

    BodyReadResult pull();
    void consume(size_t n);
    std::string peek_copy();

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Chunk> chunks_;
    std::string prefix_;
    BufferPool *pool_ = nullptr; // shared public pool (borrowed)
    std::unique_ptr<BufferPool> private_pool_; // own pool when pool_ is null
    size_t offset_ = 0;
    bool eof_ = false;
    bool cancelled_ = false;
    long long body_limit_bytes_ = 0;
    std::atomic<bool> limit_exceeded_{ false };
    std::shared_ptr<BodySource> source_;
};

inline BodyFeeder::BodyFeeder(BufferPool *pool)
    : pool_(pool)
{
    if (pool_ == nullptr) {
        // Standalone use (probes): a small private pool keeps the same
        // borrow/return discipline without a shared global.
        private_pool_ = std::make_unique<BufferPool>(kMaxQueuedChunks + 1);
        pool_ = private_pool_.get();
    }
}

inline std::shared_ptr<BodySource> BodyFeeder::make_source()
{
    auto source = std::make_shared<BodySource>();
    auto self = shared_from_this();
    source->read = [self]() { return self->pull(); };
    source->consume = [self](size_t n) { self->consume(n); };
    source->peek = [self]() { return self->peek_copy(); };
    source_ = source;
    return source;
}

inline void BodyFeeder::set_body_limit(long long bytes)
{
    body_limit_bytes_ = bytes;
}

inline BodyReadResult BodyFeeder::pull()
{
    std::lock_guard<std::mutex> lock(mu_);
    while (!chunks_.empty()) {
        const Chunk &front = chunks_.front();
        if (offset_ >= front.size) {
            chunks_.pop_front(); // returns the block to the pool
            offset_ = 0;
            continue;
        }
        return { std::string_view{ front.buf.get() + offset_, front.size - offset_ }, false, false };
    }
    if (eof_ || cancelled_)
        return { {}, true, false };
    return { {}, false, true };
}

inline void BodyFeeder::consume(size_t n)
{
    std::lock_guard<std::mutex> lock(mu_);
    offset_ += n;
    if (!chunks_.empty() && offset_ >= chunks_.front().size) {
        chunks_.pop_front(); // returns the block to the pool
        offset_ = 0;
        cv_.notify_all(); // queue has space again — resume reading the socket
    }
}

inline std::string BodyFeeder::peek_copy()
{
    std::lock_guard<std::mutex> lock(mu_);
    return prefix_;
}

inline void BodyFeeder::cancel()
{
    std::lock_guard<std::mutex> lock(mu_);
    cancelled_ = true;
    cv_.notify_all();
}

inline void BodyFeeder::feed_initial(std::string_view bytes)
{
    if (bytes.empty())
        return;
    // Count over-read bytes against the body limit too (streaming count is
    // global, not per-read).
    const long long limit = body_limit_bytes_;
    if (limit > 0 && total_bytes.load(std::memory_order_relaxed) + static_cast<long long>(bytes.size()) > limit) {
        limit_exceeded_.store(true, std::memory_order_release);
        return;
    }
    total_bytes.fetch_add(static_cast<long long>(bytes.size()), std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(mu_);
    if (prefix_.size() < kPeekBytes) {
        const size_t take = std::min(bytes.size(), kPeekBytes - prefix_.size());
        prefix_.append(bytes.data(), take);
    }
    // The over-read may exceed one block; chain borrowed blocks.
    size_t off = 0;
    while (off < bytes.size()) {
        Chunk chunk;
        chunk.buf = pool_->borrow();
        const size_t take = std::min(bytes.size() - off, BufferPool::kBufferSize);
        std::memcpy(chunk.buf.get(), bytes.data() + off, take);
        chunk.size = take;
        chunks_.push_back(std::move(chunk));
        off += take;
    }
    cv_.notify_all();
}

inline void BodyFeeder::run(int fd)
{
    for (;;) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [&] {
                return cancelled_ || limit_exceeded_.load(std::memory_order_acquire) ||
                       chunks_.size() < kMaxQueuedChunks;
            });
            if (cancelled_ || limit_exceeded_.load(std::memory_order_acquire))
                break;
        }

        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN;
        const int rc = ::poll(&pfd, 1, 200);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (rc == 0)
            continue; // poll timeout — re-check cancel

        {
            std::lock_guard<std::mutex> lock(mu_);
            if (cancelled_)
                break;
        }

        // Borrow a block from the pool and read directly into it — no
        // per-connection copy, no per-connection buffer.
        Chunk chunk;
        chunk.buf = pool_->borrow();
        const ssize_t n = ::read(fd, chunk.buf.get(), BufferPool::kBufferSize);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
            continue;
        if (n <= 0)
            break; // EOF or read error (the block returns to the pool)
        chunk.size = static_cast<size_t>(n);

        // Streaming body-size accounting: the count is global across
        // over-read initial bytes and socket reads, so a body larger than
        // the configured limit aborts mid-stream.  0 = unlimited.
        const long long limit = body_limit_bytes_;
        if (limit > 0 && total_bytes.load(std::memory_order_relaxed) + static_cast<long long>(n) > limit) {
            limit_exceeded_.store(true, std::memory_order_release);
            break;
        }
        total_bytes.fetch_add(static_cast<long long>(n), std::memory_order_relaxed);

        std::function<void()> wake;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (cancelled_)
                break;
            if (prefix_.size() < kPeekBytes) {
                const size_t take = std::min(chunk.size, kPeekBytes - prefix_.size());
                prefix_.append(chunk.buf.get(), take);
            }
            chunks_.push_back(std::move(chunk));
            cv_.notify_all();
            wake = source_ ? source_->wake : std::function<void()>{};
        }
        // Wake the curl owner thread; it performs the actual unpause on its
        // own easy handle.  This callback is a no-op once the stream closes.
        if (wake)
            wake();
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        eof_ = true;
        cv_.notify_all();
    }
}

} // namespace realurl
