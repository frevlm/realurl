# Design

The 0.3 architecture. The behavior of the HTTP/2 client is specified by
`include/realurl/http2.hpp`; this document describes the layers underneath.

## Threading model

One `EventLoop` per thread. Everything the hot path touches — socket
readiness, timers, coroutine resumption, HPACK state, flow-control windows —
lives on the loop thread and takes no locks. `EventLoop::post()` is the only
cross-thread entry point (mutex + swap + eventfd/EVFILT_USER wakeup) and it is
not on the request path.

A `Client` and every connection it creates belong to one loop. Connections are
never shared between loops; `SSL_CTX` (via `TlsContext`) is the only shared
TLS object because BoringSSL guarantees its thread safety.

## Coroutine core

`Task<T>` is a cold coroutine with symmetric transfer: awaiting a child stores
the parent handle in the child's promise, and the child's final suspend
transfers straight back to the parent. A top-level task (spawned via
`EventLoop::spawn`) destroys its own frame on completion.

IO awaits use an in-frame awaiter registered with the poller as a plain
callback pointer. Completions are never resumed inside the poll batch: the
callback pushes the handle into a ready list, and the loop resumes the batch
after polling. That keeps poller registrations from mutating mid-iteration and
gives one well-defined resume point per await.

Timeouts are loop timers captured by the awaiter; whichever of IO/timer fires
first completes the awaiter and both paths are idempotent (`done` flag).

## Event loop

| Platform | Backend | Wakeup |
| --- | --- | --- |
| Linux | `epoll_create1` + `EPOLLONESHOT`-free level trigger | `eventfd` |
| macOS | `kqueue` + `EVFILT_READ/WRITE` | `EVFILT_USER` |

Timer wheel is a `priority_queue` with lazy cancellation (`TimerId` set).
`run_once()` order: posted callbacks → due timers → poll (timeout clamped to
the nearest timer, 0 when coroutines are already ready) → dispatch events →
resume ready batch.

## HTTP/2 engine

### Frames

`FrameParser` validates the 9-byte header, enforces `max_frame_size`, and
returns views into the read buffer (zero-copy framing). `FrameWriter` appends
encoded frames to the connection's outbound buffer. HEADERS exceeding the
frame limit are split into CONTINUATION frames; the reader enforces that no
other frame type may appear inside a header sequence.

### HPACK

The decoder is complete: 61-entry static table, dynamic table with size
updates, Huffman decoding (tree built once from the RFC 7541 Appendix B
table), padding/EOS validation, and a header-list size cap. The encoder uses
static-table indexes and literal-without-indexing representations; sensitive
fields (`authorization`, `cookie`, `set-cookie`) use never-indexed. This is
valid HPACK with predictable CPU; Huffman emission is a later optimization.

The RFC 7541 C.3, C.4, and C.6 example sequences run verbatim in
`tests/hpack_test.cpp`; the interop test (`tests/interop/h2server.go`) then
cross-checks the decoder against a real Go server, which emits Huffman-coded
headers.

### Streams

Each stream is a connection-owned object with:

- a bounded send buffer (256 KiB high water) compacted by offset,
- receive window accounting: consumed bytes are returned with WINDOW_UPDATE
  only while the receive buffer is below 1 MiB, which is the TCP-style
  backpressure for slow readers,
- one suspended reader/writer/head/end awaiter each (single-consumer per
  direction), completed at most once.

### Connection

Two coroutines per connection:

- **Reader**: fills a 64 KiB buffer, parses frames, dispatches to streams,
  returns connection-level flow-control credit. On any protocol violation the
  connection sends GOAWAY with the matching error code and tears down.
- **Writer**: waits on a flush signal, pumps every stream's send buffer into
  DATA frames bounded by both windows, and issues one socket write per batch.

`SETTINGS` deltas (`INITIAL_WINDOW_SIZE`) are applied to every stream,
including negative deltas. `GOAWAY` fails streams above `last_stream_id` and
drains the rest. RST_STREAM and stream completion always return flow-control
credit correctly and wake every suspended waiter exactly once.

### Pool

`ClientImpl` keeps, per origin key (`scheme://authority` plus pinned IP), up
to `Config::max_conns_per_origin` connections. Selection prefers the
connection with the fewest active streams under the peer's
`MAX_CONCURRENT_STREAMS`; unusable connections (GOAWAY, error) are dropped
lazily. `Client::close()` (also run by the destructor) sends GOAWAY and shuts
down every socket so peers observe EOF promptly.

## What is deliberately absent

HTTP/1.1 client fallback, HTTP/3, proxies, priority trees (parsed, ignored),
server push (`ENABLE_PUSH=0`, PUSH_PROMISE is a connection error), trailers
emission, DNS beyond a thread-pool `getaddrinfo`, and io_uring. Each has a
seam: `Transport` for socket backends, `Resolver` for name resolution,
`ErrorCode`/`FrameType` for protocol versions.
