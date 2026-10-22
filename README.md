# realurl

A from-scratch C++20 networking runtime: its own event loop, coroutines, HPACK,
HTTP/2 framing/flow control, HTTP/1.1 parser, connection pool, TLS server/client
and task scheduling. OpenSSL 3.x is the only dependency delegated to a library.

realurl is the networking layer extracted from [revlm](https://github.com/FlowerRealm/revlm).
It provides an HTTP/2 client (the original 0.3 core) and, in 0.4, an HTTP/1.1 +
HTTP/2 server with an in-loop streaming proxy. In 0.5, two realurl peers can
negotiate a native transport (`realurl/1`) that keeps the HTTP/2 framing,
streams and flow control but replaces HPACK with a compact binary head codec;
peers that do not support it automatically use HTTP/2. HTTP/3 is not
supported.

## Public API

Everything a caller needs lives in one header, `realurl/http2.hpp`:

```cpp
realurl::EventLoop loop;                 // one loop per thread
realurl::Client client(loop);            // pooled, zero-config

auto run = [&]() -> realurl::Task<void> {
    // One request, full body buffered:
    realurl::Response r = co_await client.get("https://example.com/");

    // One bidirectional stream on a pooled connection:
    realurl::Stream s = co_await client.open("POST", "https://api.example.com/x");
    co_await s.write("hello", 5s);
    co_await s.end(5s);
    realurl::ResponseHead head = co_await s.head(5s);
    char buffer[65536];
    for (;;) {
        std::size_t n = co_await s.read({buffer, sizeof(buffer)}, 30s);
        if (n == 0) break;
        /* ... */
    }
};
loop.spawn(run());
loop.run();
```

That is the whole surface: `Client`, `ClientGroup`, `Stream`, `Response`,
`Header`, `Config`, `Task`, `EventLoop`, and the `Protocol` enum. No connection
handles, no session objects, no request builders. A `Stream` is move-only;
destroying one without `end()`/`cancel()` sends RST_STREAM.

`Config::protocol` selects the wire transport. The default, `Protocol::Auto`,
prefers the realurl native transport and silently falls back to HTTP/2;
`Protocol::Http2` disables native negotiation, and `Protocol::Native` requires
it. `ResponseHead::protocol` reports what was negotiated and
`ResponseHead::content_length` carries the body length when known.

`ClientGroup` runs N loop/client pairs on N threads and spreads work across
them. Tasks run on a worker loop use that worker's client directly (no
cross-thread hop); callers on another loop get the result posted back:

```cpp
realurl::Task<void> fetch(realurl::ClientGroup &group)
{
    realurl::Response r = co_await group.get("https://example.com/");
    /* ... */
}

realurl::ClientGroup group(4);
group.spawn(fetch(group));   // runs on one worker loop
```

Rules:

- A `Client` belongs to the `EventLoop` that created it. Use it only from that
  loop's thread. Connections are pooled per origin and shared by every stream
  opened through the client.
- A `ClientGroup` owns its worker loops and must outlive every task submitted
  to it. `get()`/`request()` need an `EventLoop` on the calling thread; tasks
  passed to `spawn()` run on a worker loop and use that worker's client
  without a cross-thread hop.
- `sync_wait(task)` runs a task on a private loop from any other thread.
- Errors are `std::system_error` (`errc::timed_out`, `errc::connection_reset`,
  ...). Non-2xx responses are not errors; check `Response::head.status`.
- `read()` returning 0 means END_STREAM. Its `idle` timeout aborts the wait
  but does not reset the stream.

## Components

| Layer | Files | Notes |
| --- | --- | --- |
| Coroutine core | `detail/task.hpp`, `detail/coro_pool.hpp` | `Task<T>`, symmetric transfer, `sync_wait`, per-thread frame cache |
| Event loop | `detail/event_loop.hpp`, `src/detail/poller_{epoll,kqueue}.cpp` | epoll on Linux, kqueue on macOS, edge-triggered persistent registration, timer heap |
| Sockets | `detail/socket.hpp` | Non-blocking connect/read/write, listener, resolver |
| TLS | `detail/tls.cpp`, OpenSSL 3.x | TLS 1.2/1.3, ALPN (realurl/1, h2, http1.1), SNI, session tickets, kTLS hooks |
| Server | `realurl/server.hpp`, `src/server/*` | H1 + H2 + native on the EventLoop, keep-alive, coroutine takeover, in-loop proxy |
| io_uring | `detail/uring*.cpp` | multishot recv + provided buffers, Linux only, `REALURL_WITH_IO_URING` |
| HTTP/2 | `detail/frame.hpp`, `detail/hpack.hpp`, `detail/connection.hpp` | Full HPACK (Huffman + dynamic table), flow control, GOAWAY/PING/RST |
| Native codec | `detail/native_codec.hpp`, `detail/protocol.hpp` | `realurl/1` head blocks, ALPN/preface negotiation, native response heads |
| Client | `realurl/http2.hpp`, `detail/client.hpp`, `detail/client_group.cpp` | Pool per origin with coalesced connection attempts, sharded `ClientGroup` |


## Build

Requirements: CMake 3.20+, Clang 17+ (macOS AppleClang or Linux clang),
OpenSSL 3.x development headers. macOS: `brew install openssl@3` and pass
`-DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3`. POSIX only (Linux and macOS).
GCC is not supported: its coroutine frontend ICEs on this codebase, including
the current release.

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Options:

| Option | Default | Meaning |
| --- | --- | --- |
| `REALURL_BUILD_TESTS` | `ON` | Unit and integration tests |
| `REALURL_BUILD_EXAMPLES` | `ON` | `fetch` and `stream` examples |
| `REALURL_BUILD_BENCH` | `OFF` | Loopback h2c benchmark |
| `REALURL_WITH_HTTPLIB` | `OFF` | cpp-httplib response-sink adapter |
| `REALURL_WITH_IO_URING` | `OFF` | io_uring transport on Linux: multishot recv + provided buffers, one-shot send fallback |

OpenSSL types stay inside `src/detail/tls.cpp` and
`src/detail/tls_server.cpp`. BoringSSL is no longer used because it does not
expose kTLS or `SSL_sendfile`.

## Design notes

See `docs/design.md` for the threading model and `docs/performance.md` for
measured numbers. The short version:

- **One thread, one loop, no locks on the hot path.** IO completions, timers,
  and coroutine resumption all run on the loop thread. Only `post()` crosses
  threads. `ClientGroup` adds worker loops rather than sharing one.
- **One poller registration per socket.** Sockets register edge-triggered
  read/write interest once and awaiters park in the stream; steady-state
  traffic makes no `epoll_ctl`/`kevent` calls.
- **Coroutine frames are pooled per thread.** `Task` frames come from a
  size-class free list instead of malloc/free on every `co_await`. Hot-path
  coroutines keep no large buffers across an await, so their frames stay in
  the cached small classes.
- **Two frames per unary response.** END_STREAM rides on the last DATA frame
  (or on HEADERS for an empty body), and a drained socket is not re-polled
  with a `recv` that would only return EAGAIN.
- **Zero-copy read path when the caller keeps up.** DATA payload is memcpy'd
  straight into the suspended reader's buffer; a bounded 1 MiB receive buffer
  plus paused WINDOW_UPDATEs provide backpressure when it does not.
- **One writer coroutine per connection.** Stream writes queue into a bounded
  per-stream buffer and are flushed by the connection writer under
  connection+stream flow control.
- **HPACK is the correctness single point.** The decoder passes the RFC 7541
  C.3/C.4/C.6 example suites and is cross-checked against Go's `net/http` in
  the interop tests.
- **Native transport reuses the engine.** `realurl/1` runs on the same frame
  header, stream state, flow control and writer, and swaps only the
  HEADERS/CONTINUATION codec: a compact binary block with a static name table
  instead of HPACK. Native peers are found by TLS ALPN or the
  `REALURL/1\r\n` preface; a plaintext `Auto` client probes native and falls
  back to h2c once per origin. See `docs/native.md` and the A/B numbers in
  `docs/performance.md`.

## License

[MIT](LICENSE)
