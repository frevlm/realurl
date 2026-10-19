#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string_view>

namespace realurl
{

/// Writes response bytes to a client connection.  Returns false when the
/// client has disconnected or the write deadline expired.
using ClientWriter = std::function<bool(std::string_view)>;

/// Write all of `data` to `fd`, waiting for writability up to `timeout` when
/// the socket buffer is full.  Returns false on timeout or socket error.
bool send_all_fd(int fd, std::string_view data, std::chrono::milliseconds timeout);

/// Build a ClientWriter around a raw socket fd.  When `response_started` is
/// non-null, it is set to true after the first successful write.
ClientWriter client_writer_from_fd(int fd, std::chrono::milliseconds timeout,
                                   std::shared_ptr<std::atomic_bool> response_started = {});

} // namespace realurl
