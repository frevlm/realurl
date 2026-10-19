#include "realurl/buffer_pool.hpp"

#include <cstdio>
#include <mutex>

namespace realurl
{

BufferPool::BufferPool(size_t max_buffers)
    : capacity_(max_buffers)
{
    free_list_.reserve(max_buffers);
    for (size_t i = 0; i < max_buffers; ++i) {
        free_list_.push_back(new char[kBufferSize]);
    }
}

BufferPool::~BufferPool()
{
    // Free all pool-owned buffers. Emergency buffers (still outstanding at
    // destruction time) have their own delete[] deleter and don't touch us.
    std::lock_guard<std::mutex> lock(mu_);
    for (char *buf : free_list_) {
        delete[] buf;
    }
}

BufferPool::Buffer BufferPool::borrow()
{
    std::lock_guard<std::mutex> lock(mu_);
    if (!free_list_.empty()) {
        char *buf = free_list_.back();
        free_list_.pop_back();
        return Buffer(buf, [this](char *p) { this->return_buffer(p); });
    }

    // Emergency: pool was undersized for the current load. Allocate a
    // one-off buffer that is deleted directly (not returned to the pool).
    std::fprintf(stderr,
                 "WARNING: BufferPool free list exhausted (capacity=%zu), "
                 "allocating emergency buffer\n",
                 capacity_);
    return Buffer(new char[kBufferSize], [](char *p) { delete[] p; });
}

size_t BufferPool::available() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return free_list_.size();
}

size_t BufferPool::capacity() const
{
    return capacity_;
}

void BufferPool::return_buffer(char *buf) noexcept
{
    std::lock_guard<std::mutex> lock(mu_);
    free_list_.push_back(buf);
}

} // namespace realurl
