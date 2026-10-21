#include "poller.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/eventfd.h>

#include <unordered_map>

namespace realurl::detail
{
namespace
{

std::uint32_t to_epoll(std::uint32_t mask)
{
    std::uint32_t events = EPOLLET;
    if (mask & kIoRead)
        events |= EPOLLIN;
    if (mask & kIoWrite)
        events |= EPOLLOUT;
    return events;
}

class EpollPoller final : public Poller {
public:
    EpollPoller()
    {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = wake_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &event);
    }

    ~EpollPoller() override
    {
        if (wake_fd_ >= 0)
            ::close(wake_fd_);
        if (epoll_fd_ >= 0)
            ::close(epoll_fd_);
    }

    void set(int fd, std::uint32_t mask) override
    {
        if (mask == 0) {
            del(fd);
            return;
        }

        epoll_event event{};
        event.events = to_epoll(mask) | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
        event.data.fd = fd;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == 0)
            return;
        if (errno == EEXIST) {
            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) == 0)
                return;
        }
        std::fprintf(stderr, "realurl: epoll_ctl(fd=%d) failed: %s\n", fd, std::strerror(errno));
    }

    void del(int fd) override
    {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    }

    int wait(std::vector<IoEvent> &events, int timeout_ms) override
    {
        epoll_event buffer[256];
        const int ready = ::epoll_wait(epoll_fd_, buffer, 256, timeout_ms);
        if (ready <= 0)
            return ready;

        events.clear();
        for (int i = 0; i < ready; ++i) {
            if (buffer[i].data.fd == wake_fd_) {
                std::uint64_t value = 0;
                (void)::read(wake_fd_, &value, sizeof(value));
                continue;
            }

            std::uint32_t mask = 0;
            if (buffer[i].events & EPOLLIN)
                mask |= kIoRead;
            if (buffer[i].events & EPOLLOUT)
                mask |= kIoWrite;
            if (buffer[i].events & (EPOLLERR | EPOLLHUP))
                mask |= kIoError;
            events.push_back({ buffer[i].data.fd, mask });
        }
        return static_cast<int>(events.size());
    }

    void wakeup() override
    {
        const std::uint64_t value = 1;
        (void)::write(wake_fd_, &value, sizeof(value));
    }

private:
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
};

} // namespace

std::unique_ptr<Poller> Poller::create()
{
    return std::make_unique<EpollPoller>();
}

} // namespace realurl::detail
