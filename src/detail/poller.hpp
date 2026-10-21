#pragma once

#include "realurl/detail/event_loop.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace realurl::detail
{

struct IoEvent {
    int fd = -1;
    std::uint32_t events = 0;
};

/// Platform readiness backend. set() installs the exact interest mask for fd;
/// a zero mask removes it.
class Poller {
public:
    virtual ~Poller() = default;

    virtual void set(int fd, std::uint32_t mask) = 0;
    virtual void del(int fd) = 0;
    virtual int wait(std::vector<IoEvent> &events, int timeout_ms) = 0;
    virtual void wakeup() = 0;

    static std::unique_ptr<Poller> create();
};

} // namespace realurl::detail
