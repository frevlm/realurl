# realurl

A from-scratch HTTP/2 client in C++20: its own event loop, coroutines, HPACK,
flow control, connection pool, and task scheduling. TLS 1.3 is the only
dependency delegated to a library (BoringSSL).

realurl is the networking layer extracted from [revlm](https://github.com/FlowerRealm/revlm),
rewritten in 0.3 to drop libcurl entirely. It speaks HTTP/2 only — no HTTP/1.1
fallback, no HTTP/3 — and multiplexes every request over a small pool of
long-lived connections.

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

That is the whole surface: `Client`, `Stream`, `Response`, `Header`, `Config`,
`Task`, `EventLoop`. No connection handles, no session objects, no request
builders. A `Stream` is move-only; destroying one without `end()`/`cancel()`
sends RST_STREAM.

Rules:

- A `Client` belongs to the `EventLoop` that created it. Use it only from that
  loop's thread. Connections are pooled per origin and shared by every stream
  opened through the client.
- `sync_wait(task)` runs a task on a private loop from any other thread.
- Errors are `std::system_error` (`errc::timed_out`, `errc::connection_reset`,
  ...). Non-2xx responses are not errors; check `Response::head.status`.
- `read()` returning 0 means END_STREAM. Its `idle` timeout aborts the wait
  but does not reset the stream.

## Components

| Layer | Files | Notes |
| --- | --- | --- |
| Coroutine core | `detail/task.hpp` | `Task<T>`, symmetric transfer, `sync_wait` |
| Event loop | `detail/event_loop.hpp`, `src/detail/poller_{epoll,kqueue}.cpp` | epoll on Linux, kqueue on macOS, timer heap, lock-free hot path |
| Sockets | `detail/socket.hpp` | Non-blocking connect/read/write, listener, resolver |
| TLS | `detail/tls.hpp`, BoringSSL | TLS 1.3, ALPN must be h2, SNI, certificate verification |
| HTTP/2 | `detail/frame.hpp`, `detail/hpack.hpp`, `detail/connection.hpp` | Full HPACK (Huffman + dynamic table), flow control, GOAWAY/PING/RST |
| Client | `realurl/http2.hpp`, `detail/client.hpp` | Pool per origin, `get`/`request`/`open` |

## Build

Requirements: CMake 3.20+, a C++20 compiler, Go (BoringSSL builds codegen
during configure). POSIX only (Linux and macOS).

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

BoringSSL is fetched at a pinned commit on first configure (needs Go on
`PATH`); all BoringSSL types stay inside `src/detail/tls.cpp`.

## Design notes

See `docs/design.md` for the threading model and `docs/performance.md` for
measured numbers. The short version:

- **One thread, one loop, no locks on the hot path.** IO completions, timers,
  and coroutine resumption all run on the loop thread. Only `post()` crosses
  threads.
- **Zero-copy read path when the caller keeps up.** DATA payload is memcpy'd
  straight into the suspended reader's buffer; a bounded 1 MiB receive buffer
  plus paused WINDOW_UPDATEs provide backpressure when it does not.
- **One writer coroutine per connection.** Stream writes queue into a bounded
  per-stream buffer and are flushed by the connection writer under
  connection+stream flow control.
- **HPACK is the correctness single point.** The decoder passes the RFC 7541
  C.3/C.4/C.6 example suites and is cross-checked against Go's `net/http` in
  the interop tests.

## License

[MIT](LICENSE)
