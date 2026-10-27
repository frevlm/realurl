#pragma once

/// Pluggable backing storage for stream receive spilling (experimental).
///
/// A stream that falls behind its peer keeps a bounded head of unread bytes
/// in memory and hands the rest to a SpillStore. The built-in store is an
/// anonymous temporary file; a Config::spill_store factory replaces it.

#include <functional>
#include <memory>
#include <span>
#include <string_view>

namespace realurl
{

/// Append-only byte log for one stream, read back in order.
///
/// Calls on one store never overlap, but they may run on a helper thread
/// instead of the loop thread (Config::spill_offload), so a store must not
/// touch loop state.
class SpillStore {
public:
    virtual ~SpillStore() = default;

    /// Append `bytes` after everything already stored. False on failure; the
    /// caller then keeps the bytes in memory.
    virtual bool append(std::string_view bytes) = 0;

    /// Fill `out` with the oldest out.size() unread bytes and drop them. The
    /// caller never asks for more than it appended. False on failure.
    virtual bool read(std::span<char> out) = 0;

    /// Drop everything; the reader has caught up with the writer.
    virtual void reset() = 0;
};

/// Creates the store for one stream the first time it spills. Runs on the
/// stream's loop thread. Returning nullptr disables spilling for that stream.
using SpillStoreFactory = std::function<std::unique_ptr<SpillStore>()>;

} // namespace realurl
