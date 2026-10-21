#pragma once

/// Per-thread size-class cache for coroutine frames.
///
/// Coroutine frames are allocated by the promise's operator new before the
/// coroutine body runs. Every co_await on a helper Task therefore costs one
/// allocation on the request hot path. Frames are short-lived and almost
/// always freed on the same loop thread, so a thread-local free list removes
/// most of the malloc/free traffic without locking.
///
/// Frames whose type requires over-alignment, or whose size exceeds the
/// largest class, fall back to the global allocator.

#include <cstddef>
#include <cstdint>
#include <new>

namespace realurl::detail::coro_pool
{

inline constexpr std::size_t kHeaderSize = 16;
inline constexpr std::size_t kMaxPooled = 32768;
inline constexpr std::uint32_t kNumClasses = 10;
inline constexpr std::uint32_t kOverSize = 0xFFFFFFFFu;

inline constexpr std::size_t kClassSize[kNumClasses] = {
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
};

struct Block {
    Block *next = nullptr;
    std::uint32_t class_index = 0;
    std::uint32_t reserved = 0;
};

static_assert(sizeof(Block) == kHeaderSize);

inline std::uint32_t class_for(std::size_t need) noexcept
{
    for (std::uint32_t i = 0; i < kNumClasses; ++i) {
        if (need <= kClassSize[i])
            return i;
    }
    return kOverSize;
}

struct Cache {
    Block *lists[kNumClasses]{};
    std::uint32_t counts[kNumClasses]{};

    ~Cache()
    {
        for (std::uint32_t i = 0; i < kNumClasses; ++i) {
            while (Block *block = lists[i]) {
                lists[i] = block->next;
                ::operator delete(block);
            }
        }
    }
};

inline Cache &cache() noexcept
{
    static thread_local Cache value;
    return value;
}

inline std::uint32_t cache_limit(std::uint32_t cls) noexcept
{
    return kClassSize[cls] >= 4096 ? 256u : 4096u;
}

inline void *allocate(std::size_t size)
{
    const std::size_t need = size + kHeaderSize;
    const std::uint32_t cls = class_for(need);
    Block *block = nullptr;

    if (cls != kOverSize) {
        Cache &cached = cache();
        block = cached.lists[cls];
        if (block != nullptr) {
            cached.lists[cls] = block->next;
            if (cached.counts[cls] > 0)
                --cached.counts[cls];
            return reinterpret_cast<char *>(block) + kHeaderSize;
        }
        block = static_cast<Block *>(::operator new(kClassSize[cls]));
    } else {
        block = static_cast<Block *>(::operator new(need));
    }

    block->class_index = cls;
    return reinterpret_cast<char *>(block) + kHeaderSize;
}

inline void deallocate(void *pointer) noexcept
{
    if (pointer == nullptr)
        return;

    Block *block = reinterpret_cast<Block *>(static_cast<char *>(pointer) - kHeaderSize);
    const std::uint32_t cls = block->class_index;
    if (cls >= kNumClasses) {
        ::operator delete(block);
        return;
    }

    Cache &cached = cache();
    if (cached.counts[cls] >= cache_limit(cls)) {
        ::operator delete(block);
        return;
    }

    block->next = cached.lists[cls];
    cached.lists[cls] = block;
    ++cached.counts[cls];
}

} // namespace realurl::detail::coro_pool
