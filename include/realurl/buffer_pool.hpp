#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace realurl
{

/// Pre-allocated buffer pool for streaming proxy I/O.
///
/// Each buffer is 64 KB. The pool pre-allocates max_buffers blocks and
/// recycles them via borrow(). If the free list is exhausted (should never
/// happen in a correctly-sized pool), borrow() allocates an emergency buffer
/// that is freed directly rather than returned to the pool.
///
/// Thread-safe: all public methods acquire an internal mutex.
class BufferPool {
public:
    static constexpr size_t kBufferSize = 65536; // 64 KB

    /// A borrowed buffer. The custom deleter returns the buffer to the pool
    /// when the unique_ptr is destroyed.
    using Buffer = std::unique_ptr<char[], std::function<void(char *)>>;

    /// Pre-allocate max_buffers blocks of kBufferSize bytes each.
    explicit BufferPool(size_t max_buffers = 64);

    ~BufferPool();

    BufferPool(const BufferPool &) = delete;
    BufferPool &operator=(const BufferPool &) = delete;

    /// Borrow a buffer. Returns immediately — pops from the free list, or
    /// allocates an emergency buffer (with warning) if the pool is exhausted.
    ///
    /// IMPORTANT: The BufferPool MUST outlive all borrowed Buffers. If the
    /// pool is destroyed while Buffers are still outstanding, returning them
    /// via the custom deleter will be use-after-free.
    Buffer borrow();

    /// Number of buffers currently available in the free list.
    size_t available() const;

    /// Total number of buffers managed by this pool (capacity).
    size_t capacity() const;

private:
    void return_buffer(char *buf) noexcept;

    std::vector<char *> free_list_;
    mutable std::mutex mu_;
    size_t capacity_;
};

} // namespace realurl
