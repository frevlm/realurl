#pragma once

/// Minimal raw io_uring completion core (no liburing dependency).
/// Only compiled on Linux with REALURL_WITH_IO_URING. One ring per EventLoop;
/// an eventfd registered with the ring wakes the loop when CQEs are posted.

#ifdef REALURL_WITH_IO_URING

#include "realurl/detail/event_loop.hpp"

#include <linux/io_uring.h>

#include <cstdint>
#include <span>
#include <string_view>

namespace realurl::detail
{

class UringOp {
public:
    virtual ~UringOp() = default;
    virtual void complete(int result) = 0;
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

    void drain();

private:
    static void on_event(void *ctx, std::uint32_t events);

    io_uring_sqe *get_sqe();
    bool submit_one(io_uring_sqe *sqe);
    void neutralise(io_uring_sqe *sqe) noexcept;

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
};

} // namespace realurl::detail

#endif
