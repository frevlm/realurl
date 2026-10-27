#pragma once

/// Byte FIFO that keeps a bounded head in memory and spills the rest to a
/// SpillStore.
///
/// Layout, front to back:
///
///   [ head ] [ stored ] [ writing ] [ pending ]
///
/// `head` is what readers consume. `stored` counts bytes the store holds.
/// `writing` counts bytes inside the one write job in flight. `pending` holds
/// bytes waiting for the next write job, or bytes that cannot be spilled
/// (store failed, process-wide cap reached). Appends go to the head while
/// nothing sits behind it and the head stays within the memory limit;
/// otherwise they join `pending`, which the next write job hands to the store.
/// A memory limit of 0 never spills, which makes the queue a plain ByteQueue.
///
/// Store calls run as jobs, at most one per queue at a time, so a store never
/// sees overlapping calls and always sees them in queue order. Synchronous
/// queues run each job inline. Offloaded queues run them on a process-wide
/// helper thread and apply the result in a callback posted back to the loop;
/// readers then find the head empty while size() > 0 and wait for the
/// ready callback. A job keeps its store alive, so clearing or destroying the
/// queue while one is in flight abandons the store to the job instead of
/// waiting for it.

#include "realurl/detail/byte_queue.hpp"
#include "realurl/spill_store.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace realurl::detail
{

class EventLoop;

/// Per-client receive spill settings, copied from Config and shared by every
/// stream that spills.
struct SpillConfig {
    /// Bytes kept in memory before spilling; 0 disables spilling.
    std::size_t threshold = 0;
    /// Total queued bytes per stream at which the reader stops crediting
    /// flow-control window.
    std::uint64_t limit = 256ull * 1024 * 1024;
    /// Directory for the built-in spill file. Empty uses $TMPDIR, else /tmp.
    std::string dir;
    /// Creates each stream's store. Empty uses the built-in temporary file.
    SpillStoreFactory store;
    /// Run store calls on the helper thread instead of the loop thread.
    bool offload = false;
    /// Bytes held in stores across the process; 0 is unlimited.
    std::uint64_t total_limit = 0;
    /// Per-stream receive window advertised while spilling.
    std::uint32_t window = 256 * 1024;
    /// Helper threads; the first offloaded job in the process fixes the count.
    int threads = 2;
};

/// Built-in store: an anonymous temporary file in `dir` (empty uses $TMPDIR,
/// else /tmp), created on the first append.
std::unique_ptr<SpillStore> make_file_spill_store(std::string dir);

class SpillQueue {
public:
    SpillQueue() noexcept = default;
    /// `loop` receives job completions when config->offload is set.
    SpillQueue(std::shared_ptr<const SpillConfig> config, EventLoop *loop) noexcept;
    ~SpillQueue();

    SpillQueue(const SpillQueue &) = delete;
    SpillQueue &operator=(const SpillQueue &) = delete;

    std::size_t size() const noexcept
    {
        return head_.size() + static_cast<std::size_t>(stored_) + writing_ + pending_.size();
    }

    bool empty() const noexcept
    {
        return size() == 0;
    }

    /// Never throws for store errors: bytes that cannot be spilled stay in
    /// memory.
    void append(std::string_view bytes);

    /// Copy up to out.size() bytes into `out` and consume them. Returns 0 with
    /// size() > 0 while an offloaded job is still fetching the next bytes.
    /// Throws std::system_error once spilled bytes cannot be read back.
    std::size_t read(std::span<char> out);

    /// Append every byte available now to `out`. Synchronous queues always
    /// empty; offloaded ones stop where the store has to be read. Throws like
    /// read().
    void drain_to(std::string &out);

    void clear() noexcept;

    /// Called on the loop thread after each offloaded job, so a parked reader
    /// can retry and the stream can credit bytes the store accepted.
    void set_ready_callback(void (*callback)(void *), void *context) noexcept
    {
        ready_ = callback;
        ready_context_ = context;
    }

    /// Bytes past the head that the store has not accepted: waiting for or
    /// inside a write job, or kept in memory because spilling failed or was
    /// refused. The reader withholds window credit for them, so the peer can
    /// only grow this backlog by one window.
    std::size_t awaiting_store() const noexcept
    {
        return pending_.size() + writing_;
    }

    /// True after the process-wide cap refused a spill, until the backlog
    /// drains below the memory limit. The reader stops crediting window
    /// meanwhile.
    bool throttled() const noexcept
    {
        return refused_;
    }

    /// True once a store exists. Exposed for tests.
    bool has_store() const noexcept
    {
        return shared_ != nullptr;
    }

    /// Bytes the store holds. Exposed for tests.
    std::uint64_t stored() const noexcept
    {
        return stored_;
    }

    /// True while a job is in flight. Exposed for tests.
    bool busy() const noexcept
    {
        return busy_;
    }

    /// Bytes held in stores across the process. Exposed for tests and benches.
    static std::uint64_t total_spilled() noexcept;

    /// Largest fetch from the store.
    static constexpr std::size_t kChunk = 64 * 1024;

private:
    struct Shared;
    struct Job;

    /// Start jobs until one is in flight or none is needed.
    void pump();
    void run(Job job);
    void finish(Job job);
    void throw_if_failed() const;
    /// Release the current store to whatever jobs still reference it.
    void abandon() noexcept;

    ByteQueue head_;
    std::string pending_;
    std::uint64_t stored_ = 0;
    std::size_t writing_ = 0;
    std::size_t memory_limit_ = 0;
    std::shared_ptr<const SpillConfig> config_;
    EventLoop *loop_ = nullptr;
    std::shared_ptr<Shared> shared_;
    void (*ready_)(void *) = nullptr;
    void *ready_context_ = nullptr;
    int error_ = 0;
    bool busy_ = false;
    bool failed_ = false;
    bool refused_ = false;
};

} // namespace realurl::detail
