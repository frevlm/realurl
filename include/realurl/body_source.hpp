#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

namespace realurl
{

/// Result of pulling one request-body chunk from a sliding-window source.
///
/// Exactly one of the three flags is meaningful per call:
///   - chunk  (non-empty): bytes available to send.  The view is only valid
///     until the source is touched again (the consumer copies it synchronously).
///   - wait   (true):      no data available yet — the transfer must pause and
///     be woken (via BodySource::wake) when the producer pushes more bytes.
///   - eof    (true):      end of body — no further chunks.
struct BodyReadResult {
    std::string_view chunk;
    bool eof = false;
    bool wait = false;
};

/// Sliding-window request-body source shared between the HTTP server (which
/// feeds bytes read from the client socket) and the curl pool (which pulls
/// them in its READFUNCTION).  Kept in a shared_ptr so the two ends never
/// copy the body; memory is bounded by the producer's window regardless of
/// body size.
struct BodySource {
    /// Non-blocking pull.  Returns the next un-consumed bytes, or
    /// {wait:true} / {eof:true}.  Does NOT advance the consume cursor.
    std::function<BodyReadResult()> read;

    /// Advance the consume cursor by n bytes (the bytes returned by read()).
    /// The consumer must call this after copying, mirroring a read cursor.
    std::function<void(size_t)> consume;

    /// Bounded peek of everything pushed so far (used for billing-model
    /// extraction before the body is fully consumed).  Returns a copy so the
    /// producer may keep appending.
    std::function<std::string()> peek;

    /// Registered by the curl pool once the easy/multi handles exist: call
    /// after pushing new bytes to wake a paused transfer.  The producer only
    /// wakes the multi handle; the curl owner thread performs
    /// curl_easy_pause(CURLPAUSE_CONT).  Thread-safe; a no-op once the
    /// transfer is closed.
    std::function<void()> wake;
};

} // namespace realurl
