# realurl

HTTP transport primitives for high-concurrency streaming proxies.

realurl is the networking layer extracted from [revlm](https://github.com/FlowerRealm/revlm). It focuses on the part of a proxy that usually becomes the bottleneck: keeping thousands of long-lived upstream streams alive without one thread and one connection per request.

The design is deliberately narrow. realurl does not route, authenticate, bill, or parse SSE. It moves bytes between a client socket and an upstream HTTP/2 connection with bounded memory, connection reuse, and explicit timeout segments.

## Components

| Component | Header | Purpose |
| --- | --- | --- |
| `CurlMultiPool` | `realurl/curl_multi_pool.hpp` | Per-thread `curl_multi` client with connection reuse, streaming reads, sliding-window uploads, DNS pinning, and four-segment timeouts |
| `BodySource` | `realurl/body_source.hpp` | Pull-based request-body window shared between a producer and the curl read callback |
| `BodyFeeder` | `realurl/body_feeder.hpp` | Bounded sliding-window feeder for bytes read from a client socket |
| `BufferPool` | `realurl/buffer_pool.hpp` | Pre-allocated 64 KiB blocks recycled across requests |
| `UploadRateLimiter` | `realurl/rate_limit.hpp` | Two-pool upload bandwidth coordinator (latency-sensitive flows first) |
| `StreamPumpPool` | `realurl/stream_pump_pool.hpp` | Bounded worker pool with client-IP affinity, plus an asio completion gate |
| `ClientWriter` | `realurl/fd_writer.hpp` | Deadline-bounded writes to a raw client fd |
| `ResponseSink` | `realurl/response_sink.hpp` | Server-agnostic response abstraction for non-streaming and chunked responses |
| `Server` | `realurl/server.hpp` | HTTP/1.1 server with httplib-like routes, buffered or streamed request bodies, and raw response takeover |
| `BeastResponseSink` | `realurl/beast_response_sink.hpp` | Boost.Beast adapter for `ResponseSink` |
| `HttplibResponseSink` | `realurl/httplib_response_sink.hpp` | Optional cpp-httplib adapter |

## Design highlights

- **One multi handle per thread, not one per request.** Upstream connections are reused across every stream the thread processes, including HTTP/2 multiplexing.
- **Streaming is driven on the calling thread.** `execute_stream()` performs the transfer on the thread that owns the multi handle; there is no per-stream worker thread and no per-stream curl handle.
- **Bounded upload memory.** `BodyFeeder` keeps at most two 64 KiB chunks in flight; a 500 MB body never exists as a single object.
- **Four timeout segments.** Connect, upload-stall (low-speed), response-header wait, and inter-event idle are configured independently.
- **Backpressure is explicit.** The read callback returns `CURL_READFUNC_PAUSE` when the body producer has no data, and the producer wakes the multi handle with `curl_multi_wakeup`.
- **Server side is pluggable.** `ResponseSink` decouples gateway code from cpp-httplib and Beast, and `realurl::Server` provides an httplib-like routing API with raw streaming takeover.

## Build

Requirements: CMake 3.20+, a C++20 compiler, libcurl, Boost 1.83+ headers (Asio; Beast for the server adapter). POSIX only (Linux and macOS); the body feeder and fd writer use POSIX sockets.

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Options:

| Option | Default | Meaning |
| --- | --- | --- |
| `REALURL_BUILD_TESTS` | `ON` | Build unit tests |
| `REALURL_BUILD_EXAMPLES` | `ON` | Build `fetch` and `stream` examples |
| `REALURL_BUILD_PROBES` | `OFF` | Build the known-defect probes |
| `REALURL_WITH_HTTPLIB` | `OFF` | Provide the `realurl::httplib` adapter target |

## Quick start

```cpp
#include <realurl/curl_multi_pool.hpp>

curl_global_init(CURL_GLOBAL_DEFAULT);

realurl::CurlMultiPool pool;
realurl::CurlRequest request;
request.url = "https://example.com/";
request.method = "GET";
request.content_length = 0;

const realurl::CurlResponse response = pool.execute(request);
```

Streaming:

```cpp
auto stream = pool.execute_stream(request);

std::string body = stream.initial_body;
char buffer[65536];
for (;;) {
    const ssize_t n = stream.stream_read(buffer, sizeof(buffer), 30000);
    if (n == 0)
        break;
    if (n < 0)
        throw std::runtime_error("stream read failed");
    body.append(buffer, static_cast<size_t>(n));
}
stream.stream_close();
```

Server:

```cpp
#include <realurl/server.hpp>

realurl::Server server;
server.Get("/hello", [](const auto &, auto &res) {
    res.set_content("world", "text/plain");
});
server.Get("/events", [](const auto &, auto &res) {
    res.take_over([](realurl::ClientWriter &writer) {
        (void)writer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                     "Connection: close\r\n\r\n");
        (void)writer("data: one\n\n");
    });
});
server.listen("0.0.0.0", 8080); // blocks until stop()
```

See `docs/server.md` for routing, body modes, limits, and `Server::process`.

Uploading a body that is larger than memory:

```cpp
realurl::BufferPool buffers(64);
auto feeder = std::make_shared<realurl::BodyFeeder>(&buffers);
auto source = feeder->make_source();

// A dedicated thread reads the client socket; the curl pool pulls from
// the bounded window through BodySource.
std::thread reader([feeder, fd] { feeder->run(fd); });

request.body_source = source;
request.content_length = declared_length; // -1 when unknown
```

## Threading rules

- A `CurlMultiPool` is single-threaded. Never share one across threads.
- `execute()`, `execute_stream()`, `stream_read()`, and `stream_close()` must run on the thread that owns the pool.
- `BodyFeeder::run()` blocks; run it on a dedicated thread and call `cancel()` before joining.
- `StreamPumpPool::submit()` hashes the client IP to one worker; streams from the same IP are serialized by design.
- `BeastResponseSink` must be used on the connection's `io_context` thread.

## Known limitations

This is version 0.2.1: the transport code was extracted from revlm; the server
was added afterwards. The following defects are known and documented rather
than fixed:

1. **`StreamResult` requires an explicit `stream_close()`.** Destroying the result without closing leaves the easy handle attached to the multi handle; the abandon probe reproduces a heap-use-after-free under AddressSanitizer.
2. **`StreamPumpPool` serializes streams per client IP.** This is intentional for upstream connection reuse, but one client's long streams block its other streams.
3. **libcurl below 8.4 uses `select()`.** On systems without a poll-based libcurl, the process is limited to `FD_SETSIZE` (1024) descriptors.
4. **Beast chunked uploads are not covered by the test suite.** The revlm performance report records a 12-second timeout in that path.
5. **Streamed routes require `Content-Length`.** Chunked request bodies are rejected with 400; only buffered routes decode chunked input.

The probe sources in `probes/` exist so these can be reproduced and fixed in a later version.

## License

[MIT](LICENSE)
