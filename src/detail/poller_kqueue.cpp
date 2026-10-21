#include "poller.hpp"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>

#include <unordered_map>

namespace realurl::detail
{
namespace
{

constexpr std::uintptr_t kWakeIdent = 0;

class KqueuePoller final : public Poller {
public:
    KqueuePoller()
    {
        kq_ = ::kqueue();
        if (kq_ < 0) {
            std::fprintf(stderr, "realurl: kqueue() failed: %s\n", std::strerror(errno));
            return;
        }

        struct kevent event;
        EV_SET(&event, kWakeIdent, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        (void)::kevent(kq_, &event, 1, nullptr, 0, nullptr);
    }

    ~KqueuePoller() override
    {
        if (kq_ >= 0)
            ::close(kq_);
    }

    void set(int fd, std::uint32_t mask) override
    {
        if (kq_ < 0)
            return;

        auto it = masks_.find(fd);
        const std::uint32_t current = it == masks_.end() ? 0 : it->second;
        if (current == mask)
            return;

        update_filter(fd, EVFILT_READ, kIoRead, current, mask);
        update_filter(fd, EVFILT_WRITE, kIoWrite, current, mask);

        if (mask == 0)
            masks_.erase(fd);
        else
            masks_[fd] = mask;
    }

    void del(int fd) override
    {
        set(fd, 0);
    }

    int wait(std::vector<IoEvent> &events, int timeout_ms) override
    {
        if (kq_ < 0)
            return -1;

        struct kevent buffer[256];
        struct timespec timeout{};
        struct timespec *timeout_ptr = nullptr;
        if (timeout_ms >= 0) {
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
            timeout_ptr = &timeout;
        }

        const int ready = ::kevent(kq_, nullptr, 0, buffer, 256, timeout_ptr);
        if (ready <= 0)
            return ready;

        events.clear();
        for (int i = 0; i < ready; ++i) {
            if (buffer[i].filter == EVFILT_USER)
                continue;

            std::uint32_t mask = buffer[i].filter == EVFILT_READ ? kIoRead : kIoWrite;
            if (buffer[i].flags & EV_ERROR)
                mask |= kIoError;
            events.push_back({ static_cast<int>(reinterpret_cast<std::intptr_t>(buffer[i].udata)), mask });
        }
        return static_cast<int>(events.size());
    }

    void wakeup() override
    {
        if (kq_ < 0)
            return;
        struct kevent event;
        EV_SET(&event, kWakeIdent, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
        (void)::kevent(kq_, &event, 1, nullptr, 0, nullptr);
    }

private:
    void update_filter(int fd, short filter, std::uint32_t flag, std::uint32_t current, std::uint32_t next)
    {
        if ((next & flag) && !(current & flag)) {
            struct kevent event;
            EV_SET(&event, static_cast<std::uintptr_t>(fd), filter, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0,
                   reinterpret_cast<void *>(static_cast<std::intptr_t>(fd)));
            (void)::kevent(kq_, &event, 1, nullptr, 0, nullptr);
        } else if (!(next & flag) && (current & flag)) {
            struct kevent event;
            EV_SET(&event, static_cast<std::uintptr_t>(fd), filter, EV_DELETE, 0, 0, nullptr);
            (void)::kevent(kq_, &event, 1, nullptr, 0, nullptr);
        }
    }

    int kq_ = -1;
    std::unordered_map<int, std::uint32_t> masks_;
};

} // namespace

std::unique_ptr<Poller> Poller::create()
{
    return std::make_unique<KqueuePoller>();
}

} // namespace realurl::detail
