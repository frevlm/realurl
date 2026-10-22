#pragma once

/// In-loop HTTP proxy helper.
///
/// The upstream Client and the downstream StreamWriter belong to the same
/// EventLoop, so the relay is a plain coroutine: no bridge, no thread hop, and
/// backpressure comes from the downstream writer suspending the upstream read
/// loop.

#include "realurl/server_types.hpp"

#include <chrono>
#include <string>

namespace realurl::server
{

struct ProxyOptions
{
    std::chrono::milliseconds connect_timeout{ 5000 };
    std::chrono::milliseconds header_timeout{ 60000 };
    std::chrono::milliseconds read_idle{ 60000 };
    std::chrono::milliseconds write_timeout{ 60000 };
};

/// Relay `request` to `url` and stream the response into `downstream`.
/// Errors before the response head produce a 502; errors after it close the
/// downstream direction (HTTP/1.1) or reset the stream (HTTP/2).
Task<void> proxy_stream(Client &client, std::string url, const Request &request,
                        StreamWriter &downstream, ProxyOptions options = {});

} // namespace realurl::server
