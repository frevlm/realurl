#ifdef REALURL_WITH_IO_URING

#include "uring_transport.hpp"

#include <cerrno>
#include <coroutine>
#include <cstring>
#include <deque>
#include <memory>
#include <system_error>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace realurl::detail
{

struct RecvBuffer {
    std::uint32_t bid = 0;
    std::uint32_t len = 0;
    std::uint32_t offset = 0;
};

class ReadAwaiter;

struct RecvState {
    Uring *ring = nullptr;
    std::deque<RecvBuffer> queue;
    ReadAwaiter *waiter = nullptr;
    bool eof = false;
    bool done = false;
    int error = 0;

    void on_data(std::uint32_t bid, std::uint32_t len);
    void on_eof();
    void on_error(int value);
    void on_done();
    void complete_waiter(std::size_t n, int value);
};

class ReadAwaiter {
public:
    ReadAwaiter(RecvState &state, std::span<char> buffer)
        : state_(&state)
        , buffer_(buffer)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        state_->waiter = this;
    }

    std::size_t await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "uring recv");
        return n_;
    }

    RecvState *state_;
    std::span<char> buffer_;
    std::coroutine_handle<> handle_{};
    std::size_t n_ = 0;
    int error_ = 0;
    bool done_ = false;
};

void RecvState::complete_waiter(std::size_t n, int value)
{
    if (waiter == nullptr)
        return;
    ReadAwaiter *current = waiter;
    waiter = nullptr;
    current->n_ = n;
    current->error_ = value;
    current->done_ = true;
    ring->loop().resume_later(current->handle_);
}

void RecvState::on_data(std::uint32_t bid, std::uint32_t len)
{
    if (done) {
        ring->return_buffer(bid);
        return;
    }

    if (waiter != nullptr) {
        ReadAwaiter *current = waiter;
        const std::size_t n = std::min<std::size_t>(len, current->buffer_.size());
        if (n > 0)
            std::memcpy(current->buffer_.data(), ring->buffer_data(bid), n);
        if (n < len)
            queue.push_back({ bid, len, static_cast<std::uint32_t>(n) });
        else
            ring->return_buffer(bid);
        complete_waiter(n, 0);
        return;
    }

    queue.push_back({ bid, len, 0 });
}

void RecvState::on_eof()
{
    eof = true;
    if (waiter != nullptr)
        complete_waiter(0, 0);
}

void RecvState::on_error(int value)
{
    error = value != 0 ? value : ECONNRESET;
    if (waiter != nullptr)
        complete_waiter(0, error);
}

void RecvState::on_done()
{
    done = true;
    for (const RecvBuffer &buffer : queue)
        ring->return_buffer(buffer.bid);
    queue.clear();
    if (waiter != nullptr) {
        const int value = error != 0 ? error : ECANCELED;
        error = value;
        complete_waiter(0, value);
    }
}

namespace
{

class MultishotRecvOp final : public UringOp {
public:
    MultishotRecvOp(Uring &ring, std::shared_ptr<RecvState> state) noexcept
        : state_(std::move(state))
    {
        (void)ring;
    }

    void complete(int result, unsigned flags) override
    {
        const bool more = (flags & IORING_CQE_F_MORE) != 0;
        if (result > 0 && (flags & IORING_CQE_F_BUFFER) != 0) {
            const std::uint32_t bid = flags >> IORING_CQE_BUFFER_SHIFT;
            state_->on_data(bid, static_cast<std::uint32_t>(result));
        } else if (result == 0) {
            state_->on_eof();
        } else if (result < 0) {
            state_->on_error(-result);
        }

        if (!more) {
            state_->on_done();
            delete this;
        }
    }

private:
    std::shared_ptr<RecvState> state_;
};

class RecvOp final : public UringOp {
public:
    RecvOp(Uring &ring, int fd, std::span<char> buffer) noexcept
        : ring_(ring)
        , fd_(fd)
        , buffer_(buffer)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        if (!ring_.submit_recv(fd_, buffer_, this)) {
            result_ = -EIO;
            ring_.loop().resume_later(handle_);
        }
    }

    int await_resume() const noexcept
    {
        return result_;
    }

    void complete(int result, unsigned) override
    {
        result_ = result;
        if (handle_)
            ring_.loop().resume_later(handle_);
    }

private:
    Uring &ring_;
    int fd_;
    std::span<char> buffer_;
    std::coroutine_handle<> handle_{};
    int result_ = 0;
};

class SendOp final : public UringOp {
public:
    SendOp(Uring &ring, int fd, std::string_view data) noexcept
        : ring_(ring)
        , fd_(fd)
        , data_(data)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        if (!ring_.submit_send(fd_, data_, this)) {
            result_ = -EIO;
            ring_.loop().resume_later(handle_);
        }
    }

    int await_resume() const noexcept
    {
        return result_;
    }

    void complete(int result, unsigned) override
    {
        result_ = result;
        if (handle_)
            ring_.loop().resume_later(handle_);
    }

private:
    Uring &ring_;
    int fd_;
    std::string_view data_;
    std::coroutine_handle<> handle_{};
    int result_ = 0;
};

} // namespace

UringTransport::UringTransport(Uring &ring, int fd) noexcept
    : ring_(ring)
    , fd_(fd)
{
    recv_ = std::make_shared<RecvState>();
    recv_->ring = &ring_;

    if (ring_.pbuf_available()) {
        auto *op = new MultishotRecvOp(ring_, recv_);
        if (ring_.submit_recv_multishot(fd_, op))
            multishot_ = true;
        else
            delete op;
    }
}

UringTransport::~UringTransport()
{
    close();
}

std::unique_ptr<Transport> UringTransport::create(EventLoop &loop, int fd) noexcept
{
    if (fd < 0)
        return nullptr;

    Uring *ring = loop.uring();
    if (ring == nullptr || !ring->valid()) {
        ::close(fd);
        return nullptr;
    }

    return std::unique_ptr<Transport>(new UringTransport(*ring, fd));
}

Task<std::size_t> UringTransport::read(std::span<char> buffer, std::chrono::milliseconds)
{
    if (buffer.empty())
        co_return 0;

    if (!multishot_) {
        for (;;) {
            if (fd_ < 0)
                throw std::system_error(EBADF, std::generic_category(), "uring recv");
            const int result = co_await RecvOp(ring_, fd_, buffer);
            if (result == -EINTR)
                continue;
            if (result < 0)
                throw std::system_error(-result, std::generic_category(), "uring recv");
            co_return static_cast<std::size_t>(result);
        }
    }

    for (;;) {
        if (!recv_->queue.empty()) {
            RecvBuffer &front = recv_->queue.front();
            const std::size_t available = front.len - front.offset;
            const std::size_t n = std::min(available, buffer.size());
            std::memcpy(buffer.data(), ring_.buffer_data(front.bid) + front.offset, n);
            front.offset += static_cast<std::uint32_t>(n);
            if (front.offset == front.len) {
                ring_.return_buffer(front.bid);
                recv_->queue.pop_front();
            }
            co_return n;
        }

        if (recv_->error != 0)
            throw std::system_error(recv_->error, std::generic_category(), "uring recv");
        if (recv_->eof)
            co_return 0;

        const std::size_t n = co_await ReadAwaiter(*recv_, buffer);
        if (n > 0)
            co_return n;
        if (recv_->error != 0)
            throw std::system_error(recv_->error, std::generic_category(), "uring recv");
        if (recv_->eof)
            co_return 0;
    }
}

Task<void> UringTransport::write_all(std::string_view data, std::chrono::milliseconds)
{
    while (!data.empty()) {
        if (fd_ < 0)
            throw std::system_error(EBADF, std::generic_category(), "uring send");
        const int result = co_await SendOp(ring_, fd_, data);
        if (result == -EINTR)
            continue;
        if (result < 0)
            throw std::system_error(-result, std::generic_category(), "uring send");
        if (result == 0)
            throw std::system_error(EPIPE, std::generic_category(), "uring send");
        data.remove_prefix(static_cast<std::size_t>(result));
    }
    co_return;
}

Task<void> UringTransport::shutdown_write()
{
    if (fd_ >= 0)
        (void)::shutdown(fd_, SHUT_WR);
    co_return;
}

void UringTransport::close() noexcept
{
    if (fd_ < 0)
        return;

    if (multishot_ && recv_ != nullptr) {
        if (recv_->waiter != nullptr) {
            ReadAwaiter *current = recv_->waiter;
            recv_->waiter = nullptr;
            current->error_ = ECANCELED;
            current->done_ = true;
            ring_.loop().resume_later(current->handle_);
        }
        ring_.cancel(fd_);
    }

    (void)::shutdown(fd_, SHUT_RDWR);
    ::close(fd_);
    fd_ = -1;
}

} // namespace realurl::detail

#endif
