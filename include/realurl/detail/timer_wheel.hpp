#pragma once

/// Intrusive hashed timer wheel.
///
/// Timers are embedded in the object they belong to (connection, stream,
/// awaiter). Scheduling and cancellation are O(1); firing is bounded by the
/// number of slots crossed rather than by the wall-clock delay. No
/// std::function or heap node is allocated per timer.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace realurl::detail
{

struct TimerNode {
    TimerNode *next = nullptr;
    TimerNode *prev = nullptr;
    std::uint64_t deadline_ms = 0;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    std::uint8_t level = 0;
    bool active = false;
    void (*callback)(void *) = nullptr;
    void *context = nullptr;
};

class TimerWheel {
public:
    using Clock = std::chrono::steady_clock;

    explicit TimerWheel(std::size_t slots_per_level = 64) noexcept;
    ~TimerWheel();

    TimerWheel(const TimerWheel &) = delete;
    TimerWheel &operator=(const TimerWheel &) = delete;

    void schedule(TimerNode &node, std::chrono::nanoseconds delay, void (*callback)(void *),
                  void *context);
    void schedule(TimerNode &node, Clock::time_point when, void (*callback)(void *), void *context);

    void cancel(TimerNode &node) noexcept;

    /// Fire every timer whose deadline is at or before `now`, and cascade
    /// timers from coarser levels.
    void advance(Clock::time_point now);

    std::optional<Clock::time_point> next_deadline() const;

    std::size_t active() const noexcept
    {
        return active_;
    }

    /// Number of callbacks fired since construction (loop-thread read).
    std::uint64_t fired_total() const noexcept
    {
        return fired_total_;
    }

    bool empty() const noexcept
    {
        return active_ == 0;
    }

private:
    static constexpr std::size_t kLevels = 4;

    struct Slot {
        TimerNode *head = nullptr;
    };

    void insert(TimerNode &node) noexcept;
    void unlink(TimerNode &node) noexcept;
    void process_slot(std::size_t slot_index, std::uint64_t now_ms) noexcept;
    void recompute_min() noexcept;
    std::uint32_t index_for(std::uint64_t deadline_ms, std::size_t level) const noexcept;

    std::size_t slots_per_level_;
    std::size_t shift_per_level_ = 0;
    std::size_t mask_;
    std::uint64_t mask64_;
    std::vector<Slot> slots_;
    std::uint64_t last_tick_ms_ = 0;
    std::uint64_t min_deadline_ms_ = UINT64_MAX;
    std::uint64_t generation_ = 0;
    std::uint64_t fired_total_ = 0;
    std::size_t active_ = 0;
    bool initialized_ = false;
};

} // namespace realurl::detail
