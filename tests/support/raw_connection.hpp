#pragma once

#include "realurl/detail/frame.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace test
{

struct RawFrame {
    realurl::detail::h2::FrameHeader header;
    std::string payload;
};

/// Blocking frame-level peer over a plain socket.
class RawConnection {
public:
    explicit RawConnection(int fd)
        : fd_(fd)
    {
        timeval timeout{ 5, 0 };
        (void)::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    }

    ~RawConnection()
    {
        if (fd_ >= 0)
            ::close(fd_);
    }

    RawConnection(const RawConnection &) = delete;
    RawConnection &operator=(const RawConnection &) = delete;

    bool send(std::string_view bytes)
    {
        while (!bytes.empty()) {
            const ssize_t n = ::send(fd_, bytes.data(), bytes.size(), 0);
            if (n <= 0)
                return false;
            bytes.remove_prefix(static_cast<std::size_t>(n));
        }
        return true;
    }

    bool read_exact(std::size_t size, std::string &out)
    {
        while (buffer_.size() < size) {
            if (!fill())
                return false;
        }
        out = buffer_.substr(0, size);
        buffer_.erase(0, size);
        return true;
    }

    bool next(RawFrame &out)
    {
        namespace h2 = realurl::detail::h2;
        for (;;) {
            std::size_t offset = 0;
            h2::Frame frame;
            const h2::ParseResult result = parser_.next(buffer_, offset, frame);
            if (result == h2::ParseResult::Error)
                return false;
            if (result == h2::ParseResult::Ok) {
                out.header = frame.header;
                out.payload.assign(frame.payload);
                buffer_.erase(0, offset);
                return true;
            }
            if (!fill())
                return false;
        }
    }

private:
    bool fill()
    {
        char chunk[65536];
        const ssize_t n = ::recv(fd_, chunk, sizeof(chunk), 0);
        if (n <= 0)
            return false;
        buffer_.append(chunk, static_cast<std::size_t>(n));
        return true;
    }

    int fd_;
    std::string buffer_;
    realurl::detail::h2::FrameParser parser_{ 16777215 };
};

inline int connect_loopback(int port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

/// True when `frame` ends `stream_id`.
inline bool end_of(const RawFrame &frame, std::uint32_t stream_id)
{
    namespace h2 = realurl::detail::h2;
    return frame.header.stream_id == stream_id && (frame.header.flags & h2::kFlagEndStream) != 0 &&
           (frame.header.type == h2::FrameType::Data || frame.header.type == h2::FrameType::Headers);
}

/// Big-endian u32 at the front of `bytes`, as RST_STREAM and WINDOW_UPDATE
/// carry it.
inline std::uint32_t read_be32(std::string_view bytes)
{
    const auto *raw = reinterpret_cast<const unsigned char *>(bytes.data());
    return (std::uint32_t{ raw[0] } << 24) | (std::uint32_t{ raw[1] } << 16) | (std::uint32_t{ raw[2] } << 8) |
           std::uint32_t{ raw[3] };
}

} // namespace test
