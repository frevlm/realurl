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
never shared between loops. Each `Client` owns its `TlsContext`; the client
keeps the most recent TLS session per `host:port` (up to 256, oldest evicted)
and offers it on reconnect, so a new connection to a known origin resumes
instead of redoing the key exchange. The cache is unlocked because only the
owning loop thread touches it. Only sessions from a verified handshake are
cached and they are never offered to another host name; 0-RTT is not used.

`ClientGroup` owns N loop/client pairs, one per worker thread. A task submitted
with `spawn()`/`spawn_on()` runs on a worker loop, and `get()`/`request()`
called from inside that loop uses the worker's `Client` directly. Calls from a
different loop are dispatched through `post()` and the result is posted back;
that path is convenient but costs a thread hop per request, so throughput
sensitive callers should keep their work on the worker loops.

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

Coroutine frames are allocated by `TaskPromiseBase::operator new`. A
thread-local size-class cache (`detail/coro_pool.hpp`) serves frames up to
32 KiB; over-aligned or larger frames fall back to the global allocator. This
removes the malloc/free pair that every helper `co_await`
(`head`/`read`/`write`/`end`) would otherwise pay.

Every local that lives across a `co_await` is stored in the frame. Classes of
4 KiB and above cache only 256 frames per thread, so a hot-path coroutine must
not hold a large stack buffer across an await: at high concurrency the frame
falls out of the cache and every call pays a large malloc/free. Buffered
request and response bodies therefore drain straight from the stream's queue
into the destination string instead of going through a local buffer.

## Event loop

| Platform | Backend | Wakeup |
| --- | --- | --- |
| Linux | `epoll_create1` + `EPOLLET` | `eventfd` |
| macOS | `kqueue` + `EVFILT_READ/WRITE` + `EV_CLEAR` | `EVFILT_USER` |

Sockets register read/write interest once, when the `TcpStream` is created,
and never unregister it per operation. `TcpStream` keeps at most one parked
reader and one parked writer; the poller callback resumes the parked coroutine
(or drops the edge when nothing is waiting). Writes try the syscall first.
Reads track readiness instead: a stream `recv` returns everything queued up to
the buffer size, so a short read means the socket is drained, and the next
`read()` parks without a `recv` that would only return EAGAIN. Every read edge
marks the stream readable, whether or not a reader is parked, so an edge that
arrives while nothing waits is never lost. This removes the
`epoll_ctl`/`kevent` pair that a per-wait registration would cost, and the
wasted `recv` per wakeup. TLS streams read through `SSL_read` and keep the
try-first loop.

Timer wheel is a `priority_queue` with lazy cancellation (`TimerId` set).
`run_once()` order: posted callbacks → due timers → poll (timeout clamped to
the nearest timer, 0 when coroutines are already ready) → dispatch events →
resume ready batch.

## HTTP/2 engine

### Frames

`FrameParser` validates the 9-byte header, enforces `max_frame_size`, and
returns views into the read buffer (zero-copy framing). `append_frame_header`
and `append_headers` write frames straight into the connection's outbound
buffer; `FrameWriter` builds the rare control frames. HEADERS exceeding the
frame limit are split into CONTINUATION frames; the reader enforces that no
other frame type may appear inside a header sequence.

Each stream buffers its outgoing and incoming body bytes in a `ByteQueue`, one
contiguous buffer with a read cursor. The writer sends one frame per stream
turn, and END_STREAM rides on the frame that carries the last queued byte, so
a unary response is HEADERS + one DATA frame, and an empty body ends on
HEADERS.

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

`ClientImpl` keeps, per origin key (`scheme://host:port`, built in a reused
buffer so a lookup does not allocate), up to `Config::max_conns_per_origin` connections. Selection prefers the
connection with the fewest active streams under the peer's
`MAX_CONCURRENT_STREAMS`; unusable connections (GOAWAY, error) are dropped
lazily. A cold pool coalesces connection attempts: the first request starts
the connect and later requests park on the origin's waiter list, then retry
selection when the attempt completes. Without this, a burst of N requests
opened N connections. `Client::close()` (also run by the destructor) sends
GOAWAY and shuts down every socket so peers observe EOF promptly.

## Native transport (realurl/1)

When both ends are realurl, the connection can switch to a native head codec.
It is not a second transport stack: the 9-byte frame header, DATA,
CONTINUATION, SETTINGS, PING, GOAWAY, WINDOW_UPDATE, RST_STREAM, stream state,
flow control, writer scheduling and `Transport` layer are the same objects used
by HTTP/2. Only the HEADERS/CONTINUATION payload changes, from HPACK to a
compact binary block:

```
tag (request/response/trailers)
method code or literal, target
flags + optional content-length varint
header count, then (name code | literal, value) pairs
```

Header names use a static, case-insensitive table of common names; unknown
names are literal. Values are raw. There is no dynamic table and no Huffman.
The decoder produces views into the head block, so the server builds `Request`
without per-header `std::string` allocations. `content-length` has a dedicated
field and is mapped back to both `content_length` and a synthesized header view
on the receiving side.

Negotiation is per connection:

- TLS prefers ALPN `realurl/1`, then `h2`, then `http/1.1`;
- plaintext uses the `REALURL/1\r\n` preamble, detected before the h2 preface;
- a native server sends a `NATIVE_HELLO` extension frame before SETTINGS;
- a plaintext `Protocol::Auto` client probes native and validates
  `NATIVE_HELLO`; if it sees an h2 server's SETTINGS instead it reconnects with
  h2c and caches the result per origin.

The server dispatcher reads 11 bytes, matches the native preamble, continues to
24 bytes for the h2 preface, or hands the bytes to H1. `Server::set_native(false)`
removes `realurl/1` from ALPN and disables the preamble match. `Server::Stats`
counts H1, H2 and native connections.

Native HEADERS blocks are bounded by 1 MiB (`set_header_limit()` can lower it)
and 256 headers; method, target and header values reject CR/LF/NUL, and all
varints are overflow-checked. A malformed block resets the stream; an oversized
block closes the connection with GOAWAY.

## Server runtime

`realurl::Server` runs on the same EventLoop as the client. One thread owns one
loop, one `SO_REUSEPORT` listener and one upstream `Client`; the raft of
per-connection state (read buffer, H1 parser, H2 streams, HPACK, flow-control
windows, timers) never leaves that thread.

The connection dispatcher reads the first 24 bytes once. A client preface
(`PRI * HTTP/2.0...`) selects `H2ServerConnection`; anything else is handed to
`H1Connection` together with the bytes already read. TLS uses ALPN for the same
choice.

`H1Connection` keeps a stable header arena, parses with picohttpparser,
supports keep-alive, pipelining, chunked bodies and `Expect: 100-continue`.
`H2ServerConnection` reuses `FrameParser`/`FrameWriter`/HPACK and implements
stream state, SETTINGS, per-stream and connection flow control, RST/GOAWAY and
a round-robin writer. Both frontends share the router, `Request`/`Response` and
the `StreamWriter` contract.

`take_over_task()` is the native streaming path: the handler coroutine writes
`StreamWriter` chunks and suspends when the client is slow. The legacy
`take_over()`/`take_over_async()` adapters are kept for source compatibility
and are not on the new path.

`proxy_stream()` composes the downstream writer with a per-loop upstream
`Client`: request bodies are streamed with `StreamedNative`, upstream DATA is
forwarded as it arrives, and a suspended downstream write stops the upstream
read loop. No bridge, post or thread hop is involved.

Timers use an intrusive hashed wheel per loop. Coroutine framing and buffer
ownership stay on the loop thread.

### io_uring (optional)

`REALURL_WITH_IO_URING=ON` switches plaintext (h2c) client connections to
`UringTransport`, a completion-based transport on a per-loop raw io_uring ring
(kernel UAPI, no liburing dependency). recv uses `IORING_OP_RECV` with
`IORING_RECV_MULTISHOT` and a registered provided-buffer ring (256 x 64 KiB);
send is one SQE per write. Completions are drained from the CQ into the normal
ready list, and the ring's eventfd wakes the loop. Kernels or sandboxes that
reject the buffer ring fall back to one-shot recv; a loop without a usable ring
falls back to the edge-triggered `TcpStream`. TLS stays on
`TcpStream`/`TlsServerStream` because OpenSSL owns the fd. The backend is
opt-in and Linux-only.

## What is deliberately absent

HTTP/1.1 client fallback, HTTP/3, priority trees (parsed, ignored), server push
(`ENABLE_PUSH=0`, PUSH_PROMISE is a connection error), trailers emission, and
DNS beyond a thread-pool `getaddrinfo`. Each has a seam:
`Transport` for socket backends, `Resolver` for name resolution,
`ErrorCode`/`FrameType` for protocol versions. io_uring is available behind
`REALURL_WITH_IO_URING` for h2c only.
