#pragma once

/// realurl public API: HTTP/2 client with single requests and multiplexed
/// bidirectional streams.
///
/// One EventLoop per thread. A Client belongs to the loop that created it and
/// must only be used from that loop's thread. Connections are pooled per
/// origin and shared by every stream opened through the client.

#include "realurl/detail/event_loop.hpp"
#include "realurl/detail/http2_types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace realurl
{

using Header = detail::h2::HeaderField;
using Headers = detail::h2::HeaderList;

/// Wire protocol selected for a client connection.
enum class Protocol { Auto, Http2, Native };

struct ResponseHead {
    int status = 0;
    Headers headers;
    /// Negotiated wire protocol; Auto means unknown.
    Protocol protocol = Protocol::Auto;
    /// Response body length when known. Zero-length bodies are value 0.
    std::optional<std::uint64_t> content_length;
};

struct Response {
    ResponseHead head;
    std::string body;
};

struct Config {
    /// PEM bundle for server verification. Empty uses the system trust store.
    std::string ca_file;

    /// Connect to this IP instead of resolving the URL host. Host header and
    /// SNI still use the URL host. Empty disables pinning.
    std::string pinned_ip;

    int max_conns_per_origin = 4;
    int max_streams_per_conn = 100;
    std::chrono::milliseconds connect_timeout{ 5000 };
    std::chrono::milliseconds idle_timeout{ 60000 };

    /// Auto prefers the realurl native transport and falls back to HTTP/2.
    /// Http2 disables native negotiation entirely. Native requires it.
    Protocol protocol = Protocol::Auto;
};

namespace detail
{
class ClientImpl;
class H2StreamImpl;
} // namespace detail

/// Move-only handle to one HTTP/2 stream. Destroying a stream without calling
/// end() or cancel() resets it.
class Stream {
public:
    Stream() noexcept = default;
    Stream(Stream &&) noexcept = default;
    Stream &operator=(Stream &&) noexcept = default;
    ~Stream();

    Stream(const Stream &) = delete;
    Stream &operator=(const Stream &) = delete;

    bool valid() const noexcept
    {
        return impl_ != nullptr;
    }

    /// Wait for the response headers. Idempotent: repeated calls return the
    /// same head. timeout is the maximum wait for this call; zero uses the
    /// client's header default.
    Task<ResponseHead> head(std::chrono::milliseconds timeout = {});

    /// Read response body bytes. Returns 0 at END_STREAM. idle bounds the wait
    /// for data; zero means no per-read limit.
    Task<std::size_t> read(std::span<char> buffer, std::chrono::milliseconds idle = {});

    /// Queue request body bytes. Returns once the bytes are accepted into the
    /// stream's bounded send buffer (backpressure when the peer is slow).
    /// Takes ownership so a temporary string is safe to pass.
    Task<void> write(std::string data, std::chrono::milliseconds timeout = {});

    /// Half-close: send END_STREAM after the queued body. Returns once the
    /// frame has been written to the socket.
    Task<void> end(std::chrono::milliseconds timeout = {});

    /// Send RST_STREAM. Idempotent. Wakes every pending operation.
    void cancel();

private:
    friend class Client;
    friend class detail::ClientImpl;

    explicit Stream(std::shared_ptr<detail::H2StreamImpl> impl) noexcept
        : impl_(std::move(impl))
    {
    }

    std::shared_ptr<detail::H2StreamImpl> impl_;
};

/// Multi-loop client: owns `workers` EventLoops, each running on its own
/// thread, and spreads requests across them. Call it from any EventLoop
/// thread; the result is delivered back to the caller's loop.
///
/// Unlike Client, this type does not expose open(): a Stream is bound to the
/// loop that owns its connection, so streaming handles cannot cross loops.
class ClientGroup {
public:
    explicit ClientGroup(unsigned workers, Config config = {});
    ~ClientGroup();

    ClientGroup(const ClientGroup &) = delete;
    ClientGroup &operator=(const ClientGroup &) = delete;

    std::size_t workers() const noexcept;

    Task<Response> get(std::string url, std::chrono::milliseconds timeout = {});

    Task<Response> request(std::string method, std::string url, Headers headers = {}, std::string body = {},
                           std::chrono::milliseconds timeout = {});

    /// Run one task on a worker loop (round-robin). Thread-safe. Tasks that
    /// call get()/request() from inside a worker loop use that worker's Client
    /// directly, without a cross-thread round trip.
    void spawn(Task<void> &&task);

    /// Run one task on a specific worker (index modulo worker count).
    void spawn_on(std::size_t worker, Task<void> &&task);

    /// Drop idle connections on every worker. Worker loops keep running.
    void close();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class Client {
public:
    explicit Client(EventLoop &loop, Config config = {});
    ~Client();

    Client(const Client &) = delete;
    Client &operator=(const Client &) = delete;

    /// One request, full response body buffered.
    Task<Response> get(std::string url, std::chrono::milliseconds timeout = {});

    Task<Response> request(std::string method, std::string url, Headers headers = {}, std::string body = {},
                           std::chrono::milliseconds timeout = {});

    /// One bidirectional stream. The stream is opened immediately (HEADERS are
    /// queued); the response head arrives asynchronously via Stream::head().
    Task<Stream> open(std::string method, std::string url, Headers headers = {},
                      std::chrono::milliseconds timeout = {});

    /// Drop idle connections. Active streams keep their connection alive.
    void close();

private:
    std::unique_ptr<detail::ClientImpl> impl_;
};

} // namespace realurl
