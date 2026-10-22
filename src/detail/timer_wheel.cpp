#include "realurl/detail/timer_wheel.hpp"

#include <algorithm>
#include <cassert>

namespace realurl::detail
{
namespace
{

std::uint64_t to_ms(TimerWheel::Clock::time_point when) noexcept
{
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(when.time_since_epoch()).count());
}

} // namespace

TimerWheel::TimerWheel(std::size_t slots_per_level) noexcept
    : slots_per_level_(slots_per_level)
{
    if (slots_per_level_ < 2 || (slots_per_level_ & (slots_per_level_ - 1)) != 0)
        slots_per_level_ = 64;

    mask_ = slots_per_level_ - 1;
    mask64_ = static_cast<std::uint64_t>(mask_);
    for (std::size_t n = slots_per_level_; n > 1; n >>= 1)
        ++shift_per_level_;
    slots_.resize(slots_per_level_ * kLevels);

    last_tick_ms_ = to_ms(Clock::now());
    initialized_ = true;
}

TimerWheel::~TimerWheel() = default;

std::uint32_t TimerWheel::index_for(std::uint64_t deadline_ms, std::size_t level) const noexcept
{
    return static_cast<std::uint32_t>((deadline_ms >> (shift_per_level_ * level)) & mask64_);
}

void TimerWheel::insert(TimerNode &node) noexcept
{
    // Pick the highest level whose bucket boundary is strictly in the future
    // relative to the wheel's current tick. This keeps timers that fall inside
    // the current coarse bucket in a finer level, where advance() will see
    // them instead of skipping the bucket.
    std::size_t level = 0;
    for (std::size_t candidate = 1; candidate < kLevels; ++candidate) {
        const std::size_t shift = shift_per_level_ * candidate;
        if ((node.deadline_ms >> shift) > (last_tick_ms_ >> shift))
            level = candidate;
        else
            break;
    }
    const std::uint32_t index = level == 0 && node.deadline_ms <= last_tick_ms_
        ? static_cast<std::uint32_t>(last_tick_ms_ & mask64_)
        : index_for(node.deadline_ms, level);
    const std::size_t slot_index = level * slots_per_level_ + index;

    Slot &slot = slots_[slot_index];
    node.level = static_cast<std::uint8_t>(level);
    node.slot = static_cast<std::uint32_t>(slot_index);

    // Keep each bucket ordered by deadline so a cascade to level 0 preserves
    // the firing order (FIFO for equal deadlines).
    TimerNode *next = slot.head;
    TimerNode *prev = nullptr;
    while (next != nullptr && next->deadline_ms <= node.deadline_ms) {
        prev = next;
        next = next->next;
    }
    node.prev = prev;
    node.next = next;
    if (prev != nullptr)
        prev->next = &node;
    else
        slot.head = &node;
    if (next != nullptr)
        next->prev = &node;
}

void TimerWheel::unlink(TimerNode &node) noexcept
{
    Slot &slot = slots_[node.slot];
    if (node.prev != nullptr)
        node.prev->next = node.next;
    else
        slot.head = node.next;
    if (node.next != nullptr)
        node.next->prev = node.prev;
    node.next = nullptr;
    node.prev = nullptr;
}

void TimerWheel::schedule(TimerNode &node, std::chrono::nanoseconds delay, void (*callback)(void *),
                          void *context)
{
    cancel(node);

    std::uint64_t delta_ms =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(delay).count());
    if (delay > std::chrono::nanoseconds::zero() &&
        std::chrono::duration_cast<std::chrono::milliseconds>(delay) < delay)
        ++delta_ms;

    node.deadline_ms = last_tick_ms_ + delta_ms;
    node.callback = callback;
    node.context = context;
    node.generation = ++generation_;
    node.active = true;

    insert(node);
    ++active_;
    min_deadline_ms_ = std::min(min_deadline_ms_, node.deadline_ms);
}

void TimerWheel::schedule(TimerNode &node, Clock::time_point when, void (*callback)(void *),
                          void *context)
{
    const auto now = Clock::now();
    if (when <= now) {
        schedule(node, std::chrono::nanoseconds::zero(), callback, context);
        return;
    }
    schedule(node, std::chrono::duration_cast<std::chrono::nanoseconds>(when - now), callback, context);
}

void TimerWheel::cancel(TimerNode &node) noexcept
{
    if (!node.active)
        return;

    const std::uint64_t deadline = node.deadline_ms;
    unlink(node);
    node.active = false;
    node.callback = nullptr;
    node.context = nullptr;

    if (active_ > 0)
        --active_;
    if (deadline == min_deadline_ms_)
        min_deadline_ms_ = UINT64_MAX;
}

void TimerWheel::process_slot(std::size_t slot_index, std::uint64_t now_ms) noexcept
{
    Slot &slot = slots_[slot_index];
    TimerNode *node = slot.head;
    slot.head = nullptr;

    const bool finest = slot_index < slots_per_level_;

    while (node != nullptr) {
        TimerNode *next = node->next;
        node->next = nullptr;
        node->prev = nullptr;
        node->active = false;

        if (finest && node->deadline_ms <= now_ms) {
            if (active_ > 0)
                --active_;
            if (node->deadline_ms == min_deadline_ms_)
                min_deadline_ms_ = UINT64_MAX;

            void (*callback)(void *) = node->callback;
            void *context = node->context;
            node->callback = nullptr;
            node->context = nullptr;
            if (callback != nullptr) {
                callback(context);
                ++fired_total_;
            }
        } else {
            node->active = true;
            insert(*node);
        }

        node = next;
    }
}

void TimerWheel::recompute_min() noexcept
{
    std::uint64_t minimum = UINT64_MAX;
    for (const Slot &slot : slots_) {
        for (const TimerNode *node = slot.head; node != nullptr; node = node->next)
            minimum = std::min(minimum, node->deadline_ms);
    }
    min_deadline_ms_ = minimum;
}

void TimerWheel::advance(Clock::time_point now)
{
    const std::uint64_t now_ms = to_ms(now);
    if (!initialized_) {
        last_tick_ms_ = now_ms;
        initialized_ = true;
    }
    if (now_ms < last_tick_ms_) {
        last_tick_ms_ = now_ms;
        return;
    }

    const std::uint64_t old_tick = last_tick_ms_;
    last_tick_ms_ = now_ms;

    // Coarse-to-fine: anything cascaded into level 0 for the current tick is
    // fired by the level-0 pass below. Callbacks that schedule new timers see
    // `now` as the wheel's current time.
    for (std::size_t level = kLevels - 1; level >= 1; --level) {
        const std::size_t shift = shift_per_level_ * level;
        const std::uint64_t old_boundary = old_tick >> shift;
        const std::uint64_t new_boundary = now_ms >> shift;
        if (new_boundary <= old_boundary)
            continue;

        const std::uint64_t span = new_boundary - old_boundary;
        const std::uint64_t base = static_cast<std::uint64_t>(level * slots_per_level_);
        if (span >= slots_per_level_) {
            for (std::size_t i = 0; i < slots_per_level_; ++i)
                process_slot(static_cast<std::size_t>(base) + i, now_ms);
        } else {
            for (std::uint64_t boundary = old_boundary + 1; boundary <= new_boundary; ++boundary) {
                const std::size_t index = static_cast<std::size_t>(boundary & mask64_);
                process_slot(static_cast<std::size_t>(base) + index, now_ms);
            }
        }
    }

    const std::uint64_t span = now_ms - old_tick;
    if (span >= slots_per_level_) {
        for (std::size_t i = 0; i < slots_per_level_; ++i)
            process_slot(i, now_ms);
    } else {
        for (std::uint64_t tick = old_tick; tick <= now_ms; ++tick)
            process_slot(static_cast<std::size_t>(tick & mask64_), now_ms);
    }

    if (min_deadline_ms_ <= now_ms)
        recompute_min();
}

std::optional<TimerWheel::Clock::time_point> TimerWheel::next_deadline() const
{
    if (active_ == 0)
        return std::nullopt;

    if (min_deadline_ms_ == UINT64_MAX)
        const_cast<TimerWheel *>(this)->recompute_min();

    if (min_deadline_ms_ == UINT64_MAX)
        return std::nullopt;

    return Clock::time_point(std::chrono::milliseconds(min_deadline_ms_));
}

} // namespace realurl::detail
