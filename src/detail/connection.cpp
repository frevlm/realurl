#include "realurl/detail/connection.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <system_error>
#include <utility>

namespace realurl::detail
{
using namespace h2;

namespace
{

constexpr std::uint32_t kMaxWindow = 2147483647u;

std::uint32_t read_be32(const char *data)
{
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(data[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[2])) << 8) |
           static_cast<std::uint32_t>(static_cast<unsigned char>(data[3]));
}

std::uint16_t read_be16(const char *data)
{
    return static_cast<std::uint16_t>((static_cast<unsigned char>(data[0]) << 8) |
                                      static_cast<unsigned char>(data[1]));
}

bool is_connection_header(std::string_view name)
{
    return name == "connection" || name == "keep-alive" || name == "proxy-connection" ||
           name == "transfer-encoding" || name == "upgrade";
}

} // namespace

class H2StreamImpl::HeadAwaiter {
public:
    HeadAwaiter(H2StreamImpl &stream, std::chrono::milliseconds timeout)
        : stream_(stream)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return stream_.head_ready_;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_head(this, handle, timeout_);
    }

    ResponseHead await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 head");
        return stream_.head_result_;
    }

private:
    friend class H2StreamImpl;

    H2StreamImpl &stream_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerId timer_ = 0;
    int error_ = 0;
    bool done_ = false;
};

class H2StreamImpl::ReadAwaiter {
public:
    ReadAwaiter(H2StreamImpl &stream, std::span<char> buffer, std::chrono::milliseconds idle)
        : stream_(stream)
        , buffer_(buffer)
        , idle_(idle)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_read(this, handle);
    }

    std::size_t await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 read");
        return n_;
    }

private:
    friend class H2StreamImpl;

    H2StreamImpl &stream_;
    std::span<char> buffer_;
    std::chrono::milliseconds idle_;
    std::coroutine_handle<> handle_{};
    TimerId timer_ = 0;
    std::size_t n_ = 0;
    int error_ = 0;
    bool done_ = false;
};

class H2StreamImpl::WriteAwaiter {
public:
    WriteAwaiter(H2StreamImpl &stream, std::chrono::milliseconds timeout)
        : stream_(stream)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_write(this, handle, timeout_);
    }

    void await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 write");
    }

private:
    friend class H2StreamImpl;

    H2StreamImpl &stream_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerId timer_ = 0;
    int error_ = 0;
    bool done_ = false;
};

class H2StreamImpl::EndAwaiter {
public:
    EndAwaiter(H2StreamImpl &stream, std::chrono::milliseconds timeout)
        : stream_(stream)
        , timeout_(timeout)
    {
    }

    bool await_ready() const noexcept
    {
        return stream_.end_sent_;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        stream_.start_end(this, handle, timeout_);
    }

    void await_resume() const
    {
        if (error_ != 0)
            throw std::system_error(error_, std::generic_category(), "h2 end");
    }

private:
    friend class H2StreamImpl;

    H2StreamImpl &stream_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_{};
    TimerId timer_ = 0;
    int error_ = 0;
    bool done_ = false;
};

H2StreamImpl::H2StreamImpl(H2Connection &connection, std::uint32_t id, std::uint32_t initial_window) noexcept
    : connection_(&connection)
    , loop_(&connection.loop())
    , id_(id)
    , send_window_(initial_window)
{
}

void H2StreamImpl::start_head(HeadAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout)
{
    if (closed_ || error_ != 0) {
        waiter->error_ = error_ != 0 ? error_ : ECANCELED;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    head_waiter_ = waiter;
    waiter->handle_ = handle;
    if (timeout.count() > 0) {
        waiter->timer_ = loop_->schedule(timeout, [this, waiter] {
            if (!waiter->done_) {
                waiter->error_ = ETIMEDOUT;
                complete_head(waiter);
            }
        });
    }
}

void H2StreamImpl::complete_head(HeadAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (waiter->timer_ != 0) {
        loop_->cancel(waiter->timer_);
        waiter->timer_ = 0;
    }
    if (head_waiter_ == waiter)
        head_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2StreamImpl::start_read(ReadAwaiter *waiter, std::coroutine_handle<> handle)
{
    if (closed_ || error_ != 0) {
        waiter->error_ = error_ != 0 ? error_ : ECANCELED;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    read_waiter_ = waiter;
    waiter->handle_ = handle;
    if (waiter->idle_.count() > 0) {
        waiter->timer_ = loop_->schedule(waiter->idle_, [this, waiter] {
            if (!waiter->done_) {
                waiter->error_ = ETIMEDOUT;
                complete_read(waiter);
            }
        });
    }
}

void H2StreamImpl::complete_read(ReadAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (waiter->timer_ != 0) {
        loop_->cancel(waiter->timer_);
        waiter->timer_ = 0;
    }
    if (read_waiter_ == waiter)
        read_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2StreamImpl::start_write(WriteAwaiter *waiter, std::coroutine_handle<> handle,
                               std::chrono::milliseconds timeout)
{
    if (closed_ || local_closed_) {
        waiter->error_ = error_ != 0 ? error_ : EPIPE;
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    write_waiter_ = waiter;
    waiter->handle_ = handle;
    if (timeout.count() > 0) {
        waiter->timer_ = loop_->schedule(timeout, [this, waiter] {
            if (!waiter->done_) {
                waiter->error_ = ETIMEDOUT;
                complete_write(waiter);
            }
        });
    }
}

void H2StreamImpl::complete_write(WriteAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (waiter->timer_ != 0) {
        loop_->cancel(waiter->timer_);
        waiter->timer_ = 0;
    }
    if (write_waiter_ == waiter)
        write_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

void H2StreamImpl::start_end(EndAwaiter *waiter, std::coroutine_handle<> handle, std::chrono::milliseconds timeout)
{
    if (end_sent_ || closed_) {
        waiter->done_ = true;
        loop_->resume_later(handle);
        return;
    }

    end_waiter_ = waiter;
    waiter->handle_ = handle;
    if (timeout.count() > 0) {
        waiter->timer_ = loop_->schedule(timeout, [this, waiter] {
            if (!waiter->done_) {
                waiter->error_ = ETIMEDOUT;
                complete_end(waiter);
            }
        });
    }
}

void H2StreamImpl::complete_end(EndAwaiter *waiter)
{
    if (waiter == nullptr || waiter->done_)
        return;
    waiter->done_ = true;
    if (waiter->timer_ != 0) {
        loop_->cancel(waiter->timer_);
        waiter->timer_ = 0;
    }
    if (end_waiter_ == waiter)
        end_waiter_ = nullptr;
    loop_->resume_later(waiter->handle_);
}

Task<ResponseHead> H2StreamImpl::head(std::chrono::milliseconds timeout)
{
    if (head_ready_)
        co_return head_result_;
    if (error_ != 0)
        throw std::system_error(error_, std::generic_category(), "h2 head");
    if (closed_)
        throw std::system_error(ECANCELED, std::generic_category(), "h2 stream closed");
    co_return co_await HeadAwaiter(*this, timeout);
}

Task<std::size_t> H2StreamImpl::read(std::span<char> buffer, std::chrono::milliseconds idle)
{
    if (buffer.empty())
        throw std::invalid_argument("h2 read: empty buffer");

    if (recv_pending_ > 0) {
        std::size_t total_copied = 0;
        while (!recv_queue_.empty() && total_copied < buffer.size()) {
            auto &front = recv_queue_.front();
            const std::size_t available = front.size() - recv_chunk_offset_;
            const std::size_t n = std::min(available, buffer.size() - total_copied);
            std::memcpy(buffer.data() + total_copied, front.data() + recv_chunk_offset_, n);
            total_copied += n;
            recv_chunk_offset_ += n;
            recv_pending_ -= n;
            if (recv_chunk_offset_ == front.size()) {
                recv_queue_.pop_front();
                recv_chunk_offset_ = 0;
            }
        }
        flush_window();
        co_return total_copied;
    }

    if (error_ != 0)
        throw std::system_error(error_, std::generic_category(), "h2 read");
    if (remote_closed_ || closed_)
        co_return 0;

    co_return co_await ReadAwaiter(*this, buffer, idle);
}

Task<void> H2StreamImpl::write(std::string_view data, std::chrono::milliseconds timeout)
{
    if (closed_ || local_closed_)
        throw std::system_error(EPIPE, std::generic_category(), "h2 write");

    while (!data.empty()) {
        const std::size_t pending = send_pending_;
        const std::size_t space = pending < kSendHighWater ? kSendHighWater - pending : 0;
        if (space == 0) {
            if (connection_ != nullptr && !in_write_queue_ && !blocked_on_window_) {
                connection_->enqueue_write(id_);
            }
            co_await WriteAwaiter(*this, timeout);
            if (closed_ || local_closed_)
                throw std::system_error(error_ != 0 ? error_ : EPIPE, std::generic_category(), "h2 write");
            continue;
        }

        const std::size_t n = std::min(space, data.size());
        if (!send_queue_.empty() && send_queue_.back().size() < 16384) {
            const std::size_t append_n = std::min(16384 - send_queue_.back().size(), n);
            send_queue_.back().append(data.substr(0, append_n));
            send_pending_ += append_n;
            data.remove_prefix(append_n);
        } else {
            send_queue_.push_back(std::string(data.substr(0, n)));
            send_pending_ += n;
            data.remove_prefix(n);
        }

        if (connection_ != nullptr && !in_write_queue_ && !blocked_on_window_) {
            connection_->enqueue_write(id_);
        }
    }

    if (connection_ != nullptr) {
        if (!in_write_queue_ && !blocked_on_window_) {
            connection_->enqueue_write(id_);
        } else {
            connection_->schedule_flush();
        }
    }
    co_return;
}

Task<void> H2StreamImpl::end(std::chrono::milliseconds timeout)
{
    if (end_sent_ || closed_)
        co_return;

    end_pending_ = true;
    if (connection_ != nullptr) {
        if (!in_write_queue_ && !blocked_on_window_) {
            connection_->enqueue_write(id_);
        } else {
            connection_->schedule_flush();
        }
    }

    if (end_sent_)
        co_return;
    co_await EndAwaiter(*this, timeout);
}

void H2StreamImpl::cancel()
{
    if (closed_)
        return;
    if (connection_ != nullptr) {
        connection_->send_rst(id_, ErrorCode::Cancel);
        connection_->schedule_flush();
    }
    fail(ECANCELED);
}

void H2StreamImpl::on_headers(HeaderList &&headers, bool end_stream)
{
    if (closed_)
        return;

    if (head_ready_) {
        // Trailers: ignored in this version.
    } else {
        ResponseHead result;
        for (HeaderField &header : headers) {
            if (header.name == ":status") {
                try {
                    result.status = std::stoi(header.value);
                } catch (...) {
                    result.status = 0;
                }
            } else if (!header.name.empty() && header.name[0] != ':') {
                result.headers.push_back(std::move(header));
            }
        }
        head_result_ = std::move(result);
        head_ready_ = true;
        if (head_waiter_ != nullptr)
            complete_head(head_waiter_);
    }

    if (end_stream) {
        remote_closed_ = true;
        if (read_waiter_ != nullptr && recv_pending_ == 0) {
            if (read_waiter_->n_ == 0)
                complete_read(read_waiter_);
        }
        check_closed();
    }
}

void H2StreamImpl::on_data(std::string_view data, bool end_stream, std::uint32_t flow_bytes)
{
    if (closed_)
        return;

    recv_unacked_ += flow_bytes;

    if (read_waiter_ != nullptr) {
        ReadAwaiter *waiter = read_waiter_;
        const std::size_t n = std::min(data.size(), waiter->buffer_.size());
        if (n > 0)
            std::memcpy(waiter->buffer_.data(), data.data(), n);
        waiter->n_ = n;
        if (n < data.size()) {
            recv_queue_.push_back(std::string(data.substr(n)));
            recv_pending_ += (data.size() - n);
        }
        complete_read(waiter);
    } else {
        recv_queue_.push_back(std::string(data));
        recv_pending_ += data.size();
    }

    if (end_stream)
        remote_closed_ = true;

    flush_window();
    check_closed();
}

void H2StreamImpl::on_rst(ErrorCode code)
{
    (void)code;
    fail(ECONNRESET);
}

void H2StreamImpl::on_window_update(std::uint32_t increment)
{
    if (send_window_ + static_cast<std::int64_t>(increment) > static_cast<std::int64_t>(kMaxWindow)) {
        fail(EPROTO);
        return;
    }
    send_window_ += increment;
    if (blocked_on_window_ && connection_ != nullptr && send_window_ > 0) {
        blocked_on_window_ = false;
        if (!in_write_queue_) {
            connection_->enqueue_write(id_);
        }
    } else if (connection_ != nullptr) {
        connection_->schedule_flush();
    }
}

void H2StreamImpl::on_connection_closed(int error)
{
    fail(error != 0 ? error : ECONNRESET);
}

void H2StreamImpl::on_settings_initial_window(std::int32_t delta)
{
    send_window_ += delta;
    if (blocked_on_window_ && connection_ != nullptr && send_window_ > 0) {
        blocked_on_window_ = false;
        if (!in_write_queue_) {
            connection_->enqueue_write(id_);
        }
    } else if (connection_ != nullptr) {
        connection_->schedule_flush();
    }
}

void H2StreamImpl::flush_window()
{
    if (connection_ == nullptr || closed_)
        return;
    if (recv_pending_ >= kRecvHighWater)
        return;
    if (recv_unacked_ >= kWindowUpdateThreshold) {
        connection_->send_window_update(id_, recv_unacked_);
        recv_unacked_ = 0;
    }
}

PumpResult H2StreamImpl::pump_one_frame()
{
    if (connection_ == nullptr || closed_)
        return PumpResult::NoWork;

    if (send_pending_ > 0) {
        const std::int64_t window = std::min<std::int64_t>(connection_->send_window(), send_window_);
        if (window <= 0)
            return PumpResult::WindowFull;

        const std::size_t max_frame = connection_->max_frame_size();
        const std::size_t allowed =
            std::min<std::size_t>({ send_pending_, static_cast<std::size_t>(window), max_frame });

        auto &front = send_queue_.front();
        const std::size_t avail = front.size() - send_chunk_offset_;
        const std::size_t to_send = std::min(allowed, avail);

        connection_->append_data(id_, std::string_view(front).substr(send_chunk_offset_, to_send), false);
        send_chunk_offset_ += to_send;
        send_pending_ -= to_send;
        connection_->consume_send_window(to_send);
        send_window_ -= static_cast<std::int64_t>(to_send);

        if (send_chunk_offset_ == front.size()) {
            send_queue_.pop_front();
            send_chunk_offset_ = 0;
        }

        if (send_pending_ <= kSendLowWater && write_waiter_ != nullptr)
            complete_write(write_waiter_);

        check_closed();
        return PumpResult::SentFrame;
    }

    if (send_pending_ == 0 && end_pending_ && !end_sent_) {
        connection_->append_data(id_, {}, true);
        end_pending_ = false;
        end_sent_ = true;
        local_closed_ = true;
        if (end_waiter_ != nullptr)
            complete_end(end_waiter_);
        check_closed();
        return PumpResult::SentFrame;
    }

    return PumpResult::NoWork;
}

bool H2StreamImpl::pump()
{
    bool queued = false;
    while (pump_one_frame() == PumpResult::SentFrame)
        queued = true;
    return queued;
}

void H2StreamImpl::fail(int error)
{
    if (closed_)
        return;

    closed_ = true;
    error_ = error;
    in_write_queue_ = false;
    blocked_on_window_ = false;

    if (head_waiter_ != nullptr) {
        head_waiter_->error_ = error;
        complete_head(head_waiter_);
    }
    if (read_waiter_ != nullptr) {
        read_waiter_->error_ = error;
        complete_read(read_waiter_);
    }
    if (write_waiter_ != nullptr) {
        write_waiter_->error_ = error;
        complete_write(write_waiter_);
    }
    if (end_waiter_ != nullptr) {
        end_waiter_->error_ = error;
        complete_end(end_waiter_);
    }

    send_queue_.clear();
    send_pending_ = 0;
    send_chunk_offset_ = 0;
    recv_queue_.clear();
    recv_pending_ = 0;
    recv_chunk_offset_ = 0;

    if (connection_ != nullptr) {
        connection_->on_stream_closed(id_);
        connection_ = nullptr;
    }
}

void H2StreamImpl::check_closed()
{
    if (closed_ || !(local_closed_ && remote_closed_))
        return;

    closed_ = true;
    if (connection_ != nullptr) {
        connection_->on_stream_closed(id_);
        connection_ = nullptr;
    }
    if (read_waiter_ != nullptr && recv_pending_ == 0)
        complete_read(read_waiter_);
}

H2Connection::H2Connection(EventLoop &loop, std::unique_ptr<Transport> transport, std::string authority,
                                 std::string scheme)
    : loop_(loop)
    , transport_(std::move(transport))
    , authority_(std::move(authority))
    , scheme_(std::move(scheme))
    , flush_signal_(loop)
{
}

Task<std::shared_ptr<H2Connection>> H2Connection::connect(EventLoop &loop, std::unique_ptr<Transport> transport,
                                                          std::string authority, std::string scheme)
{
    std::shared_ptr<H2Connection> connection(
        new H2Connection(loop, std::move(transport), std::move(authority), std::move(scheme)));
    connection->send_preface();
    loop.spawn(connection->reader_loop());
    loop.spawn(connection->writer_loop());
    co_return connection;
}

void H2Connection::send_preface()
{
    write_buffer_.append("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

    const Setting settings[] = {
        { kSettingEnablePush, 0 },
        { kSettingInitialWindowSize, kStreamRecvWindow },
        { kSettingMaxFrameSize, kDefaultMaxFrameSize },
    };
    FrameWriter writer;
    writer.settings(settings);
    write_buffer_.append(writer.buffer());

    FrameWriter update;
    update.window_update(0, kConnRecvWindow - kDefaultInitialWindowSize);
    write_buffer_.append(update.buffer());
}

std::shared_ptr<H2StreamImpl> H2Connection::open(std::string_view method, std::string_view path, HeaderList headers,
                                                 bool end_stream)
{
    if (!usable_)
        throw std::system_error(ENOTCONN, std::generic_category(), "h2 connection is not usable");
    if (streams_.size() >= max_streams_)
        throw std::system_error(EBUSY, std::generic_category(), "h2 connection is at max concurrent streams");

    const std::uint32_t id = next_stream_id_;
    next_stream_id_ += 2;

    auto stream = std::make_shared<H2StreamImpl>(*this, id, peer_initial_window_);
    streams_.emplace(id, stream);

    HeaderList block;
    block.reserve(4 + headers.size());
    block.push_back({ ":method", std::string(method) });
    block.push_back({ ":scheme", scheme_ });
    block.push_back({ ":authority", authority_ });
    block.push_back({ ":path", std::string(path) });
    for (HeaderField &header : headers) {
        if (header.name.empty() || header.name[0] == ':')
            continue;
        if (is_connection_header(header.name))
            continue;
        block.push_back(std::move(header));
    }

    std::string encoded;
    encoder_.encode(block, encoded);
    send_headers(id, encoded, end_stream);
    return stream;
}

void H2Connection::send_headers(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    FrameWriter writer;
    if (block.size() <= max_frame_size_) {
        writer.headers(stream_id, block, true, end_stream);
        write_buffer_.append(writer.buffer());
    } else {
        writer.headers(stream_id, block.substr(0, max_frame_size_), false, end_stream);
        write_buffer_.append(writer.buffer());

        std::size_t offset = max_frame_size_;
        while (offset < block.size()) {
            const std::size_t n = std::min<std::size_t>(max_frame_size_, block.size() - offset);
            const bool last = offset + n == block.size();
            FrameWriter continuation;
            continuation.continuation(stream_id, block.substr(offset, n), last);
            write_buffer_.append(continuation.buffer());
            offset += n;
        }
    }
    schedule_flush();
}

void H2Connection::append_data(std::uint32_t stream_id, std::string_view data, bool end_stream)
{
    char h[9];
    const std::size_t length = data.size();
    h[0] = static_cast<char>((length >> 16) & 0xff);
    h[1] = static_cast<char>((length >> 8) & 0xff);
    h[2] = static_cast<char>(length & 0xff);
    h[3] = static_cast<char>(FrameType::Data);
    h[4] = static_cast<char>(end_stream ? kFlagEndStream : 0);
    h[5] = static_cast<char>((stream_id >> 24) & 0x7f);
    h[6] = static_cast<char>((stream_id >> 16) & 0xff);
    h[7] = static_cast<char>((stream_id >> 8) & 0xff);
    h[8] = static_cast<char>(stream_id & 0xff);

    write_buffer_.append(h, sizeof(h));
    if (!data.empty())
        write_buffer_.append(data);
}

void H2Connection::send_rst(std::uint32_t stream_id, ErrorCode code)
{
    char h[13];
    h[0] = 0;
    h[1] = 0;
    h[2] = 4;
    h[3] = static_cast<char>(FrameType::RstStream);
    h[4] = 0;
    h[5] = static_cast<char>((stream_id >> 24) & 0x7f);
    h[6] = static_cast<char>((stream_id >> 16) & 0xff);
    h[7] = static_cast<char>((stream_id >> 8) & 0xff);
    h[8] = static_cast<char>(stream_id & 0xff);
    const auto c = static_cast<std::uint32_t>(code);
    h[9] = static_cast<char>((c >> 24) & 0xff);
    h[10] = static_cast<char>((c >> 16) & 0xff);
    h[11] = static_cast<char>((c >> 8) & 0xff);
    h[12] = static_cast<char>(c & 0xff);
    write_buffer_.append(h, sizeof(h));
}

void H2Connection::send_window_update(std::uint32_t stream_id, std::uint32_t increment)
{
    char h[13];
    h[0] = 0;
    h[1] = 0;
    h[2] = 4;
    h[3] = static_cast<char>(FrameType::WindowUpdate);
    h[4] = 0;
    h[5] = static_cast<char>((stream_id >> 24) & 0x7f);
    h[6] = static_cast<char>((stream_id >> 16) & 0xff);
    h[7] = static_cast<char>((stream_id >> 8) & 0xff);
    h[8] = static_cast<char>(stream_id & 0xff);
    const std::uint32_t inc = increment & 0x7fffffffu;
    h[9] = static_cast<char>((inc >> 24) & 0xff);
    h[10] = static_cast<char>((inc >> 16) & 0xff);
    h[11] = static_cast<char>((inc >> 8) & 0xff);
    h[12] = static_cast<char>(inc & 0xff);
    write_buffer_.append(h, sizeof(h));
    schedule_flush();
}

void H2Connection::schedule_flush()
{
    flush_signal_.notify();
}

void H2Connection::enqueue_write(std::uint32_t stream_id)
{
    auto it = streams_.find(stream_id);
    if (it != streams_.end() && it->second != nullptr) {
        if (it->second->in_write_queue_)
            return;
        it->second->in_write_queue_ = true;
        it->second->blocked_on_window_ = false;
    }
    write_queue_.push_back(stream_id);
    schedule_flush();
}

void H2Connection::on_stream_closed(std::uint32_t stream_id)
{
    pending_removals_.push_back(stream_id);
}

void H2Connection::reap()
{
    if (pending_removals_.empty())
        return;
    for (std::uint32_t id : pending_removals_)
        streams_.erase(id);
    pending_removals_.clear();
}

Task<void> H2Connection::reader_loop()
{
    auto self = shared_from_this();
    std::vector<char> buffer(65536);

    for (;;) {
        std::size_t n = 0;
        try {
            n = co_await transport_->read({ buffer.data(), buffer.size() });
        } catch (...) {
            close_with_error(ECONNRESET);
            co_return;
        }
        if (n == 0) {
            close_with_error(ECONNRESET);
            co_return;
        }

        read_buffer_.append(buffer.data(), n);
        process_frames();
        reap();
        if (closed_)
            co_return;
    }
}

Task<void> H2Connection::writer_loop()
{
    auto self = shared_from_this();

    for (;;) {
        if (closed_)
            co_return;

        reap();

        constexpr std::size_t kMaxBatchWriteSize = 512 * 1024;
        while (!write_queue_.empty() && send_window_ > 0 && write_buffer_.size() < kMaxBatchWriteSize) {
            const std::uint32_t id = write_queue_.front();
            write_queue_.pop_front();

            auto it = streams_.find(id);
            if (it == streams_.end() || it->second == nullptr)
                continue;

            auto &stream = it->second;
            stream->in_write_queue_ = false;

            if (stream->done())
                continue;

            const PumpResult res = stream->pump_one_frame();
            if (res == PumpResult::SentFrame) {
                if (stream->has_pending_work()) {
                    stream->in_write_queue_ = true;
                    write_queue_.push_back(id);
                }
            } else if (res == PumpResult::WindowFull) {
                stream->blocked_on_window_ = true;
                blocked_streams_.push_back(id);
            }
        }

        if (!write_buffer_.empty()) {
            out_buffer_.clear();
            out_buffer_.swap(write_buffer_);
            try {
                co_await transport_->write_all(out_buffer_);
            } catch (...) {
                close_with_error(ECONNRESET);
                co_return;
            }
            continue;
        }

        if (closed_)
            co_return;
        co_await flush_signal_;
    }
}

void H2Connection::process_frames()
{
    std::size_t offset = 0;

    for (;;) {
        Frame frame;
        const ParseResult result = parser_.next(read_buffer_, offset, frame);
        if (result == ParseResult::NeedMore)
            break;
        if (result == ParseResult::Error) {
            send_goaway(ErrorCode::FrameSize);
            close_with_error(EPROTO);
            return;
        }

        handle_frame(frame);
        if (closed_)
            return;
    }

    if (offset > 0)
        read_buffer_.erase(0, offset);
}

void H2Connection::handle_frame(const Frame &frame)
{
    if (expecting_continuation_ && frame.header.type != FrameType::Continuation) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    switch (frame.header.type) {
    case FrameType::Data:
        handle_data(frame);
        break;

    case FrameType::Headers:
        handle_headers(frame);
        break;

    case FrameType::Priority:
        break;

    case FrameType::RstStream: {
        if (frame.header.stream_id == 0 || frame.payload.size() != 4) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const auto it = streams_.find(frame.header.stream_id);
        if (it != streams_.end())
            it->second->on_rst(static_cast<ErrorCode>(read_be32(frame.payload.data())));
        break;
    }

    case FrameType::Settings:
        handle_settings(frame.payload, (frame.header.flags & kFlagAck) != 0);
        break;

    case FrameType::PushPromise:
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        break;

    case FrameType::Ping: {
        if (frame.header.stream_id != 0 || frame.payload.size() != 8) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        if ((frame.header.flags & kFlagAck) == 0) {
            FrameWriter writer;
            writer.ping(true, read_be32(frame.payload.data()) * 0x100000000ull +
                                  read_be32(frame.payload.data() + 4));
            write_buffer_.append(writer.buffer());
            schedule_flush();
        }
        break;
    }

    case FrameType::GoAway:
        handle_goaway(frame.payload);
        break;

    case FrameType::WindowUpdate:
        handle_window_update(frame);
        break;

    case FrameType::Continuation: {
        if (!expecting_continuation_ || frame.header.stream_id != continuation_stream_) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        continuation_block_.append(frame.payload);
        if ((frame.header.flags & kFlagEndHeaders) != 0) {
            expecting_continuation_ = false;
            const std::uint32_t stream_id = continuation_stream_;
            const bool end_stream = continuation_end_stream_;
            std::string block = std::move(continuation_block_);
            continuation_block_.clear();
            finish_headers(stream_id, block, end_stream);
        }
        break;
    }

    default:
        break;
    }
}

void H2Connection::handle_settings(std::string_view payload, bool ack)
{
    if (ack) {
        if (!payload.empty()) {
            send_goaway(ErrorCode::FrameSize);
            close_with_error(EPROTO);
        }
        return;
    }

    if (payload.size() % 6 != 0) {
        send_goaway(ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    for (std::size_t offset = 0; offset < payload.size(); offset += 6) {
        const std::uint16_t id = read_be16(payload.data() + offset);
        const std::uint32_t value = read_be32(payload.data() + offset + 2);

        switch (id) {
        case kSettingInitialWindowSize: {
            if (value > kMaxWindow) {
                send_goaway(ErrorCode::FlowControl);
                close_with_error(EPROTO);
                return;
            }
            const auto delta = static_cast<std::int32_t>(value) - static_cast<std::int32_t>(peer_initial_window_);
            peer_initial_window_ = value;
            for (auto &entry : streams_)
                entry.second->on_settings_initial_window(delta);
            break;
        }
        case kSettingMaxFrameSize:
            if (value < kDefaultMaxFrameSize || value > 16777215) {
                send_goaway(ErrorCode::Protocol);
                close_with_error(EPROTO);
                return;
            }
            max_frame_size_ = value;
            break;
        case kSettingMaxConcurrentStreams:
            max_streams_ = value == 0 ? 1 : value;
            break;
        case kSettingHeaderTableSize:
            decoder_.set_max_allowed_table_size(value);
            break;
        default:
            break;
        }
    }

    FrameWriter writer;
    writer.settings_ack();
    write_buffer_.append(writer.buffer());
    schedule_flush();
}

void H2Connection::handle_headers(const Frame &frame)
{
    if (frame.header.stream_id == 0) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    std::string_view payload = frame.payload;
    if ((frame.header.flags & kFlagPadded) != 0) {
        if (payload.empty()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const std::size_t pad = static_cast<unsigned char>(payload.front());
        payload.remove_prefix(1);
        if (pad > payload.size()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        payload.remove_suffix(pad);
    }
    if ((frame.header.flags & kFlagPriority) != 0) {
        if (payload.size() < 5) {
            send_goaway(ErrorCode::FrameSize);
            close_with_error(EPROTO);
            return;
        }
        payload.remove_prefix(5);
    }

    const bool end_stream = (frame.header.flags & kFlagEndStream) != 0;
    if ((frame.header.flags & kFlagEndHeaders) != 0) {
        finish_headers(frame.header.stream_id, payload, end_stream);
        return;
    }

    expecting_continuation_ = true;
    continuation_stream_ = frame.header.stream_id;
    continuation_end_stream_ = end_stream;
    continuation_block_.assign(payload);
}

void H2Connection::finish_headers(std::uint32_t stream_id, std::string_view block, bool end_stream)
{
    HeaderList headers;
    if (!decoder_.decode(block, headers)) {
        send_goaway(ErrorCode::Compression);
        close_with_error(EPROTO);
        return;
    }

    const auto it = streams_.find(stream_id);
    if (it == streams_.end())
        return;
    it->second->on_headers(std::move(headers), end_stream);
}

void H2Connection::handle_data(const Frame &frame)
{
    if (frame.header.stream_id == 0) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    std::string_view data = frame.payload;
    if ((frame.header.flags & kFlagPadded) != 0) {
        if (data.empty()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        const std::size_t pad = static_cast<unsigned char>(data.front());
        data.remove_prefix(1);
        if (pad > data.size()) {
            send_goaway(ErrorCode::Protocol);
            close_with_error(EPROTO);
            return;
        }
        data.remove_suffix(pad);
    }

    const auto it = streams_.find(frame.header.stream_id);
    if (it != streams_.end())
        it->second->on_data(data, (frame.header.flags & kFlagEndStream) != 0, frame.header.length);

    recv_unacked_ += frame.header.length;
    if (recv_unacked_ >= 32768) {
        send_window_update(0, recv_unacked_);
        recv_unacked_ = 0;
    }
}

void H2Connection::handle_goaway(std::string_view payload)
{
    if (payload.size() < 8) {
        send_goaway(ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    const std::uint32_t last_stream_id = read_be32(payload.data()) & 0x7fffffffu;
    usable_ = false;

    std::vector<std::shared_ptr<H2StreamImpl>> failed;
    for (const auto &entry : streams_) {
        if (entry.first > last_stream_id)
            failed.push_back(entry.second);
    }
    for (const auto &stream : failed)
        stream->on_connection_closed(ECONNRESET);
}

void H2Connection::handle_window_update(const Frame &frame)
{
    if (frame.payload.size() != 4) {
        send_goaway(ErrorCode::FrameSize);
        close_with_error(EPROTO);
        return;
    }

    const std::uint32_t increment = read_be32(frame.payload.data()) & 0x7fffffffu;
    if (increment == 0) {
        send_goaway(ErrorCode::Protocol);
        close_with_error(EPROTO);
        return;
    }

    if (frame.header.stream_id == 0) {
        if (send_window_ + static_cast<std::int64_t>(increment) > static_cast<std::int64_t>(kMaxWindow)) {
            send_goaway(ErrorCode::FlowControl);
            close_with_error(EPROTO);
            return;
        }
        send_window_ += increment;
        if (send_window_ > 0 && !blocked_streams_.empty()) {
            std::vector<std::uint32_t> unblocked;
            unblocked.swap(blocked_streams_);
            for (std::uint32_t sid : unblocked) {
                auto it = streams_.find(sid);
                if (it != streams_.end() && it->second != nullptr) {
                    if (it->second->send_window_ > 0) {
                        it->second->blocked_on_window_ = false;
                        if (!it->second->in_write_queue_ && it->second->has_pending_work()) {
                            it->second->in_write_queue_ = true;
                            write_queue_.push_back(sid);
                        }
                    } else {
                        blocked_streams_.push_back(sid);
                    }
                }
            }
        }
    } else {
        const auto it = streams_.find(frame.header.stream_id);
        if (it != streams_.end())
            it->second->on_window_update(increment);
    }

    schedule_flush();
}

void H2Connection::send_goaway(ErrorCode code)
{
    FrameWriter writer;
    writer.goaway(next_stream_id_ - 1, code);
    write_buffer_.append(writer.buffer());
}

void H2Connection::close_with_error(int error)
{
    if (closed_)
        return;

    closed_ = true;
    usable_ = false;

    std::vector<std::shared_ptr<H2StreamImpl>> streams;
    streams.reserve(streams_.size());
    for (auto &entry : streams_)
        streams.push_back(entry.second);
    streams_.clear();
    write_queue_.clear();
    blocked_streams_.clear();
    pending_removals_.clear();

    for (const auto &stream : streams)
        stream->on_connection_closed(error);

    if (transport_->fd() >= 0)
        (void)::shutdown(transport_->fd(), SHUT_RDWR);

    flush_signal_.notify();
}

void H2Connection::close()
{
    if (closed_)
        return;
    send_goaway(ErrorCode::NoError);
    close_with_error(0);
}

} // namespace realurl::detail
