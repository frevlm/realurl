#pragma once

/// Byte FIFO backed by one contiguous buffer.
///
/// Stream send/receive queues hold small, short-lived payloads on the unary
/// path and up to a few MiB on streaming paths. A flat buffer avoids the
/// per-stream node allocations of a chunk list. Consumed bytes are reclaimed
/// when the queue drains, or by compaction once the dead prefix is at least as
/// large as the live bytes, which keeps appends amortized O(1).

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>
#include <string_view>

namespace realurl::detail
{

class ByteQueue {
public:
    std::size_t size() const noexcept
    {
        return data_.size() - head_;
    }

    bool empty() const noexcept
    {
        return head_ == data_.size();
    }

    /// Up to `max` unconsumed bytes, starting at the front.
    std::string_view front(std::size_t max) const noexcept
    {
        return std::string_view(data_).substr(head_, max);
    }

    void append(std::string_view bytes)
    {
        if (head_ > 0 && head_ >= size()) {
            data_.erase(0, head_);
            head_ = 0;
        }
        data_.append(bytes);
    }

    void consume(std::size_t n) noexcept
    {
        head_ += std::min(n, size());
        if (head_ == data_.size())
            reset();
    }

    /// Copy up to out.size() bytes into `out` and consume them.
    std::size_t read(std::span<char> out) noexcept
    {
        const std::size_t n = std::min(out.size(), size());
        if (n > 0)
            std::memcpy(out.data(), data_.data() + head_, n);
        consume(n);
        return n;
    }

    /// Append every unconsumed byte to `out` and empty the queue.
    void drain_to(std::string &out)
    {
        out.append(data_, head_);
        reset();
    }

    void clear() noexcept
    {
        reset();
    }

private:
    /// Buffers above this capacity are released once drained, so an idle
    /// long-lived stream does not pin the peak of an earlier burst.
    static constexpr std::size_t kRetainCapacity = 64 * 1024;

    void reset() noexcept
    {
        head_ = 0;
        if (data_.capacity() > kRetainCapacity)
            std::string().swap(data_);
        else
            data_.clear();
    }

    std::string data_;
    std::size_t head_ = 0;
};

} // namespace realurl::detail
