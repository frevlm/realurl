#ifdef REALURL_WITH_IO_URING

#include "uring.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cerrno>
#include <cstdint>
#include <cstring>

#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace realurl::detail
{
namespace
{

constexpr unsigned kQueueDepth = 4096;

unsigned load_acquire(unsigned *value) noexcept
{
    return std::atomic_ref<unsigned>(*value).load(std::memory_order_acquire);
}

void store_release(unsigned *value, unsigned next) noexcept
{
    std::atomic_ref<unsigned>(*value).store(next, std::memory_order_release);
}

} // namespace

Uring::Uring(EventLoop &loop)
    : loop_(loop)
{
    io_uring_params params{};
    ring_fd_ = static_cast<int>(::syscall(__NR_io_uring_setup, kQueueDepth, &params));
    if (ring_fd_ < 0)
        return;

    const std::size_t sq_ring_size =
        params.sq_off.array + params.sq_entries * sizeof(unsigned);
    const std::size_t cq_ring_size =
        params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);

    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0) {
        const std::size_t size = std::max(sq_ring_size, cq_ring_size);
        void *ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_,
                           IORING_OFF_SQ_RING);
        if (ptr == MAP_FAILED) {
            ::close(ring_fd_);
            ring_fd_ = -1;
            return;
        }
        sq_ptr_ = ptr;
        cq_ptr_ = ptr;
        sq_map_size_ = size;
        cq_map_size_ = size;
        single_mmap_ = true;
    } else {
        sq_ptr_ = ::mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_,
                         IORING_OFF_SQ_RING);
        cq_ptr_ = ::mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_,
                         IORING_OFF_CQ_RING);
        if (sq_ptr_ == MAP_FAILED || cq_ptr_ == MAP_FAILED) {
            if (sq_ptr_ != MAP_FAILED)
                ::munmap(sq_ptr_, sq_ring_size);
            if (cq_ptr_ != MAP_FAILED)
                ::munmap(cq_ptr_, cq_ring_size);
            sq_ptr_ = nullptr;
            cq_ptr_ = nullptr;
            ::close(ring_fd_);
            ring_fd_ = -1;
            return;
        }
        sq_map_size_ = sq_ring_size;
        cq_map_size_ = cq_ring_size;
    }

    sqes_size_ = params.sq_entries * sizeof(io_uring_sqe);
    void *sqes = ::mmap(nullptr, sqes_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_,
                        IORING_OFF_SQES);
    if (sqes == MAP_FAILED) {
        if (single_mmap_) {
            ::munmap(sq_ptr_, sq_map_size_);
        } else {
            ::munmap(sq_ptr_, sq_map_size_);
            ::munmap(cq_ptr_, cq_map_size_);
        }
        sq_ptr_ = nullptr;
        cq_ptr_ = nullptr;
        ::close(ring_fd_);
        ring_fd_ = -1;
        return;
    }
    sqes_ = static_cast<io_uring_sqe *>(sqes);

    char *sq = static_cast<char *>(sq_ptr_);
    sq_head_ = reinterpret_cast<unsigned *>(sq + params.sq_off.head);
    sq_tail_ = reinterpret_cast<unsigned *>(sq + params.sq_off.tail);
    sq_ring_mask_ = reinterpret_cast<unsigned *>(sq + params.sq_off.ring_mask);
    sq_ring_entries_ = reinterpret_cast<unsigned *>(sq + params.sq_off.ring_entries);
    sq_array_ = reinterpret_cast<unsigned *>(sq + params.sq_off.array);

    char *cq = static_cast<char *>(cq_ptr_);
    cq_head_ = reinterpret_cast<unsigned *>(cq + params.cq_off.head);
    cq_tail_ = reinterpret_cast<unsigned *>(cq + params.cq_off.tail);
    cq_ring_mask_ = reinterpret_cast<unsigned *>(cq + params.cq_off.ring_mask);
    cq_ring_entries_ = reinterpret_cast<unsigned *>(cq + params.cq_off.ring_entries);
    cqes_ = reinterpret_cast<io_uring_cqe *>(cq + params.cq_off.cqes);

    event_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd_ < 0) {
        ::munmap(sqes_, sqes_size_);
        if (single_mmap_)
            ::munmap(sq_ptr_, sq_map_size_);
        else {
            ::munmap(sq_ptr_, sq_map_size_);
            ::munmap(cq_ptr_, cq_map_size_);
        }
        sqes_ = nullptr;
        sq_ptr_ = nullptr;
        cq_ptr_ = nullptr;
        ::close(ring_fd_);
        ring_fd_ = -1;
        return;
    }

    if (::syscall(__NR_io_uring_register, ring_fd_, IORING_REGISTER_EVENTFD, &event_fd_, 1) < 0) {
        ::close(event_fd_);
        event_fd_ = -1;
        ::munmap(sqes_, sqes_size_);
        if (single_mmap_)
            ::munmap(sq_ptr_, sq_map_size_);
        else {
            ::munmap(sq_ptr_, sq_map_size_);
            ::munmap(cq_ptr_, cq_map_size_);
        }
        sqes_ = nullptr;
        sq_ptr_ = nullptr;
        cq_ptr_ = nullptr;
        ::close(ring_fd_);
        ring_fd_ = -1;
        return;
    }

    (void)setup_buffer_ring();
    loop_.add_io(event_fd_, kIoRead, &Uring::on_event, this);
}

Uring::~Uring()
{
    release_buffer_ring();
    if (event_fd_ >= 0) {
        loop_.del_io(event_fd_);
        ::close(event_fd_);
        event_fd_ = -1;
    }
    if (sqes_ != nullptr)
        ::munmap(sqes_, sqes_size_);
    if (sq_ptr_ != nullptr) {
        if (single_mmap_) {
            ::munmap(sq_ptr_, sq_map_size_);
        } else {
            ::munmap(sq_ptr_, sq_map_size_);
            ::munmap(cq_ptr_, cq_map_size_);
        }
    }
    if (ring_fd_ >= 0) {
        ::close(ring_fd_);
        ring_fd_ = -1;
    }
}

io_uring_sqe *Uring::get_sqe()
{
    const unsigned tail = load_acquire(sq_tail_);
    const unsigned head = load_acquire(sq_head_);
    if (tail - head >= *sq_ring_entries_)
        return nullptr;

    const unsigned index = tail & *sq_ring_mask_;
    io_uring_sqe *sqe = &sqes_[index];
    std::memset(sqe, 0, sizeof(*sqe));
    sq_array_[index] = index;
    return sqe;
}

bool Uring::submit_one(io_uring_sqe *sqe)
{
    const unsigned tail = load_acquire(sq_tail_) + 1;
    store_release(sq_tail_, tail);

    int rc = 0;
    do {
        rc = static_cast<int>(::syscall(__NR_io_uring_enter, ring_fd_, 1, 0, 0, nullptr, 0));
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        neutralise(sqe);
        return false;
    }
    return true;
}

void Uring::neutralise(io_uring_sqe *sqe) noexcept
{
    sqe->opcode = IORING_OP_NOP;
    sqe->flags = 0;
    sqe->user_data = 0;
}

bool Uring::submit_recv(int fd, std::span<char> buffer, UringOp *op)
{
    if (ring_fd_ < 0)
        return false;

    io_uring_sqe *sqe = get_sqe();
    if (sqe == nullptr)
        return false;

    sqe->opcode = IORING_OP_RECV;
    sqe->fd = fd;
    sqe->addr = reinterpret_cast<__u64>(buffer.data());
    sqe->len = static_cast<__u32>(buffer.size());
    sqe->msg_flags = 0;
    sqe->user_data = reinterpret_cast<__u64>(op);
    return submit_one(sqe);
}

bool Uring::submit_send(int fd, std::string_view data, UringOp *op)
{
    if (ring_fd_ < 0)
        return false;

    io_uring_sqe *sqe = get_sqe();
    if (sqe == nullptr)
        return false;

    sqe->opcode = IORING_OP_SEND;
    sqe->fd = fd;
    sqe->addr = reinterpret_cast<__u64>(data.data());
    sqe->len = static_cast<__u32>(data.size());
    sqe->msg_flags = MSG_NOSIGNAL;
    sqe->user_data = reinterpret_cast<__u64>(op);
    return submit_one(sqe);
}

bool Uring::setup_buffer_ring()
{
    buf_ring_size_ = static_cast<std::size_t>(kBufCount) * sizeof(io_uring_buf);
    buf_memory_size_ = static_cast<std::size_t>(kBufCount) * kBufSize;

    buf_ring_ = static_cast<io_uring_buf *>(::mmap(nullptr, buf_ring_size_, PROT_READ | PROT_WRITE,
                                                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (buf_ring_ == reinterpret_cast<io_uring_buf *>(MAP_FAILED)) {
        buf_ring_ = nullptr;
        return false;
    }
    buf_memory_ = ::mmap(nullptr, buf_memory_size_, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buf_memory_ == MAP_FAILED) {
        buf_memory_ = nullptr;
        release_buffer_ring();
        return false;
    }

    buf_tail_ptr_ = reinterpret_cast<std::uint16_t *>(
        reinterpret_cast<char *>(buf_ring_) + offsetof(io_uring_buf, resv));
    for (std::uint32_t i = 0; i < kBufCount; ++i) {
        io_uring_buf &buf = buf_ring_[i];
        buf.addr = reinterpret_cast<__u64>(static_cast<char *>(buf_memory_) + i * kBufSize);
        buf.len = kBufSize;
        buf.bid = i;
    }
    buf_tail_ = kBufCount;
    __atomic_store_n(buf_tail_ptr_, static_cast<std::uint16_t>(buf_tail_), __ATOMIC_RELEASE);

    io_uring_buf_reg reg{};
    reg.ring_addr = reinterpret_cast<__u64>(buf_ring_);
    reg.ring_entries = kBufCount;
    reg.bgid = kBufGroup;
    if (::syscall(__NR_io_uring_register, ring_fd_, IORING_REGISTER_PBUF_RING, &reg, 1) < 0) {
        release_buffer_ring();
        return false;
    }

    pbuf_ = true;
    return true;
}

void Uring::release_buffer_ring() noexcept
{
    if (pbuf_) {
        io_uring_buf_reg reg{};
        reg.bgid = kBufGroup;
        (void)::syscall(__NR_io_uring_register, ring_fd_, IORING_UNREGISTER_PBUF_RING, &reg, 1);
        pbuf_ = false;
    }
    if (buf_ring_ != nullptr) {
        ::munmap(buf_ring_, buf_ring_size_);
        buf_ring_ = nullptr;
    }
    if (buf_memory_ != nullptr) {
        ::munmap(buf_memory_, buf_memory_size_);
        buf_memory_ = nullptr;
    }
}

void Uring::return_buffer(std::uint32_t bid) noexcept
{
    if (!pbuf_ || bid >= kBufCount || buf_ring_ == nullptr)
        return;

    const std::uint32_t index = buf_tail_ & (kBufCount - 1);
    io_uring_buf &buf = buf_ring_[index];
    buf.addr = reinterpret_cast<__u64>(static_cast<char *>(buf_memory_) + bid * kBufSize);
    buf.len = kBufSize;
    buf.bid = bid;
    ++buf_tail_;
    __atomic_store_n(buf_tail_ptr_, static_cast<std::uint16_t>(buf_tail_), __ATOMIC_RELEASE);
}

char *Uring::buffer_data(std::uint32_t bid) noexcept
{
    return static_cast<char *>(buf_memory_) + static_cast<std::size_t>(bid) * kBufSize;
}

bool Uring::submit_recv_multishot(int fd, UringOp *op)
{
    if (ring_fd_ < 0 || !pbuf_)
        return false;

    io_uring_sqe *sqe = get_sqe();
    if (sqe == nullptr)
        return false;

    sqe->opcode = IORING_OP_RECV;
    sqe->fd = fd;
    sqe->addr = 0;
    sqe->len = 0;
    sqe->msg_flags = 0;
    sqe->buf_group = kBufGroup;
    sqe->flags = IOSQE_BUFFER_SELECT;
    sqe->ioprio = IORING_RECV_MULTISHOT;
    sqe->user_data = reinterpret_cast<__u64>(op);
    return submit_one(sqe);
}

void Uring::cancel(int fd)
{
    if (ring_fd_ < 0 || fd < 0)
        return;
    io_uring_sqe *sqe = get_sqe();
    if (sqe == nullptr)
        return;
    sqe->opcode = IORING_OP_ASYNC_CANCEL;
    sqe->fd = -1;
    sqe->addr = static_cast<__u64>(fd);
    sqe->len = 0;
    sqe->user_data = 0;
    (void)submit_one(sqe);
}

void Uring::drain()
{
    if (ring_fd_ < 0)
        return;

    for (;;) {
        unsigned head = load_acquire(cq_head_);
        const unsigned tail = load_acquire(cq_tail_);

        while (head != tail) {
            io_uring_cqe *cqe = &cqes_[head & *cq_ring_mask_];
            auto *op = reinterpret_cast<UringOp *>(cqe->user_data);
            if (op != nullptr)
                op->complete(cqe->res, cqe->flags);
            ++head;
        }
        store_release(cq_head_, head);

        if (event_fd_ >= 0) {
            std::uint64_t value = 0;
            const ssize_t drained = ::read(event_fd_, &value, sizeof(value));
            (void)drained;
        }

        // A completion can land between draining the CQ and clearing the
        // eventfd. Re-check before sleeping so that edge does not get lost.
        if (load_acquire(cq_head_) == load_acquire(cq_tail_))
            break;
    }
}

void Uring::on_event(void *ctx, std::uint32_t)
{
    static_cast<Uring *>(ctx)->drain();
}

} // namespace realurl::detail

#endif
