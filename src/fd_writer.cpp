#include "realurl/fd_writer.hpp"

#include <cerrno>
#include <poll.h>
#include <sys/socket.h>
#include <utility>

namespace realurl
{

bool send_all_fd(int fd, std::string_view data, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Socket buffer full (slow client): wait for writability, bounded
            // by the deadline so a stuck client cannot hold the writer forever.
            for (;;) {
                if (std::chrono::steady_clock::now() >= deadline)
                    return false;
                pollfd pfd{};
                pfd.fd = fd;
                pfd.events = POLLOUT;
                const int rc = ::poll(&pfd, 1, 50);
                if (rc > 0)
                    break;
                if (rc < 0 && errno != EINTR)
                    return false;
            }
            continue;
        }
        return false; // closed / error
    }
    return true;
}

ClientWriter client_writer_from_fd(int fd, std::chrono::milliseconds timeout,
                                   std::shared_ptr<std::atomic_bool> response_started)
{
    return [fd, timeout, response_started = std::move(response_started)](std::string_view data) {
        const bool sent = send_all_fd(fd, data, timeout);
        if (sent && response_started)
            response_started->store(true, std::memory_order_release);
        return sent;
    };
}

} // namespace realurl
