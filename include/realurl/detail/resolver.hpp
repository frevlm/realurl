#pragma once

#include "realurl/detail/event_loop.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <sys/socket.h>

namespace realurl::detail
{

struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t length = 0;
};

/// Asynchronous resolver. getaddrinfo runs on one dedicated thread; results
/// are posted back to the owning event loop. IP literals are resolved inline
/// without touching the worker thread.
class Resolver {
public:
    struct Impl;

    explicit Resolver(EventLoop &loop);
    ~Resolver();

    Resolver(const Resolver &) = delete;
    Resolver &operator=(const Resolver &) = delete;

    Task<SocketAddress> resolve(std::string host, std::uint16_t port);

private:
    std::unique_ptr<Impl> impl_;
};

} // namespace realurl::detail
