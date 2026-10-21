#pragma once

#include "realurl/detail/event_loop.hpp"

#include <chrono>
#include <cstddef>
#include <span>
#include <string_view>

namespace realurl::detail
{

/// Byte transport used by the HTTP/2 connection. Implemented by plain TCP and
/// by TLS. Every method runs on the connection's event loop thread.
class Transport {
public:
    virtual ~Transport() = default;

    virtual Task<std::size_t> read(std::span<char> buffer, std::chrono::milliseconds idle = {}) = 0;
    virtual Task<void> write_all(std::string_view data, std::chrono::milliseconds timeout = {}) = 0;
    virtual Task<void> shutdown_write() = 0;
    virtual int fd() const = 0;
    virtual void close() = 0;
};

} // namespace realurl::detail
