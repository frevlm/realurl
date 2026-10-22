#pragma once

/// Minimal raw io_uring completion core (no liburing dependency).
/// Only compiled on Linux with REALURL_WITH_IO_URING. One ring per EventLoop;
/// an eventfd registered with the ring wakes the loop when CQEs are posted.
///
/// Plaintext transports use a provided-buffer ring and IORING_OP_RECV with
/// IORING_RECV_MULTISHOT when the kernel exposes it. The one-shot path stays as
/// a fallback for kernels/sandboxes that reject the ring registration.

#ifdef REALURL_WITH_IO_URING

#include "realurl/detail/event_loop.hpp"

#include <linux/io_uring.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace realurl::detail
{

class UringOp {
public:
    virtual ~UringOp() = default;
    virtual void complete(int result, unsigned flags) = 0;
};

class Uring {
public:
    explicit Uring(EventLoop &loop);
    ~Uring();

    Uring(const Uring &) = delete;
    Uring &operator=(const Uring &) = delete;

    bool valid() const noexcept
    {
        return ring_fd_ >= 0;
    }

    EventLoop &loop() noexcept
    {
        return loop_;
    }

    bool submit_recv(int fd, std::span<char> buffer, UringOp *op);
    bool submit_send(int fd, std::string_view data, UringOp *op);

    /// Queue the multishot receive. Returns false when provided buffers are
    /// unavailable or the SQ is full.
    bool submit_recv_multishot(int fd, UringOp *op);

    /// Return a provided buffer to the kernel's receive ring.
    void return_buffer(std::uint32_t bid) noexcept;

    /// Data pointer for a provided buffer id.
    char *buffer_data(std::uint32_t bid) noexcept;

    bool pbuf_available() const noexcept
    {
        return pbuf_;
    }

    static constexpr std::uint32_t kBufCount = 256;
    static constexpr std::uint32_t kBufSize = 65536;
    static constexpr std::uint16_t kBufGroup = 0;

    void cancel(int fd);

    void drain();

private:
    static void on_event(void *ctx, std::uint32_t events);

    io_uring_sqe *get_sqe();
    bool submit_one(io_uring_sqe *sqe);
    void neutralise(io_uring_sqe *sqe) noexcept;
    bool setup_buffer_ring();
    void release_buffer_ring() noexcept;

    EventLoop &loop_;
    int ring_fd_ = -1;
    int event_fd_ = -1;

    void *sq_ptr_ = nullptr;
    void *cq_ptr_ = nullptr;
    std::size_t sq_map_size_ = 0;
    std::size_t cq_map_size_ = 0;
    std::size_t sqes_size_ = 0;
    bool single_mmap_ = false;

    io_uring_sqe *sqes_ = nullptr;
    io_uring_cqe *cqes_ = nullptr;

    unsigned *sq_head_ = nullptr;
    unsigned *sq_tail_ = nullptr;
    unsigned *sq_ring_mask_ = nullptr;
    unsigned *sq_ring_entries_ = nullptr;
    unsigned *sq_array_ = nullptr;

    unsigned *cq_head_ = nullptr;
    unsigned *cq_tail_ = nullptr;
    unsigned *cq_ring_mask_ = nullptr;
    unsigned *cq_ring_entries_ = nullptr;

    io_uring_buf *buf_ring_ = nullptr;
    std::uint16_t *buf_tail_ptr_ = nullptr;
    std::size_t buf_ring_size_ = 0;
    void *buf_memory_ = nullptr;
    std::size_t buf_memory_size_ = 0;
    std::uint32_t buf_tail_ = 0;
    bool pbuf_ = false;
};

} // namespace realurl::detail

#endif
