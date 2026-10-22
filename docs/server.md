# Server

`realurl::Server` is an HTTP/1.1 and HTTP/2 server built on the realurl
EventLoop. One event loop runs per io thread; each loop owns its listener
(`SO_REUSEPORT`), connections, stream queues and upstream client. Handlers are
coroutines, and a takeover handler can stream a response without crossing a
thread or a second scheduler.

The public routing API stays close to the 0.3 Beast server; the native streaming
entry point is `Response::take_over_task()`.

## Quick start

```cpp
#include <realurl/server.hpp>

realurl::Server server;
server.set_io_threads(4);          // 4 event loops / SO_REUSEPORT listeners

server.Get("/hello", [](const auto &, auto &res) {
    res.set_content("world", "text/plain");
});

server.Post("/echo", [](const auto &req, auto &res) {
    res.set_content(req.body, "text/plain");
});

server.Get("/events", [](const auto &, auto &res) {
    res.take_over_task([](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
        const realurl::server::Header headers[] = {
            { "content-type", "text/event-stream" },
        };
        co_await writer.write_head(200, headers, std::nullopt); // chunked H1 / DATA H2
        co_await writer.write("data: one\n\n");
        co_await writer.end();
    });
});

server.listen("0.0.0.0", 8080); // blocks until stop()
```

`stop()` closes the listeners, waits up to 2 seconds for active connections,
then stops the loops. The request/response types are in
`include/realurl/server_types.hpp`.

## Routing and requests

| API | Match |
| --- | --- |
| `Get/Post/Put/Delete(pattern, handler)` | exact path, or `:name` segments (`/users/:id`) |
| `GetRegex/...` | `std::regex_match` on the decoded path |
| `set_pre_routing_handler(handler)` | before routing; return `Handled` to short-circuit |

Precedence is exact, then `:param`, then regex. `HEAD` falls back to a `GET`
route and suppresses the body. Unknown paths return 404 with the body from
`set_error_body(404, ...)`.

`Request` header/path fields are `std::string_view` views into the connection's
head buffer. They stay valid until the handler (including a
`take_over_task`) returns; copy anything that must outlive the request.

```cpp
struct Request {
    std::string_view method, target, path, version;
    std::span<const server::Header> headers;
    std::unordered_map<std::string_view, std::string_view> path_params;
    std::string body;                  // Buffered
    BodyReader *body_reader;           // StreamedNative
    std::shared_ptr<BodySource> body_source; // Streamed (legacy)
    BodyMode body_mode;
    long long content_length;
    std::string_view remote_addr;
    int remote_port;
    EventLoop *loop;
    Client *upstream;                  // set_upstream()
    std::string_view get_header(std::string_view) const;
    bool has_header(std::string_view) const;
    Task<std::size_t> read_body(std::span<char>) const; // 0 = EOF
};
```

`path` has the query removed. Percent decoding is enabled by default
(`set_path_decoding(false)` preserves raw escapes).

## Body modes

The body mode is a property of the route, decided before body bytes are read:

```cpp
server.Post("/login", handler);                       // Buffered (default)
server.Post("/upload", handler,
            { .body_mode = realurl::Server::BodyMode::StreamedNative });
```

- **Buffered**: `Request::body` holds the whole body. Chunked bodies are
  decoded. Use this for management JSON and small unary requests.
- **StreamedNative**: `Request::body_reader` is a coroutine `BodyReader`;
  call `co_await req.read_body(buffer)` to pull chunks. Memory is bounded by the
  reader window and the downstream flow-control window. This is the path for
  proxy uploads and large bodies.
- **Streamed**: compatibility shim for the 0.3 `BodySource` API. The body is
  buffered and exposed through `Request::body_source`; new code should use
  `StreamedNative`.

`Expect: 100-continue` is honored after routing and the payload limit are known.
`Content-Length` is required for `Streamed`; chunked is accepted by
`Buffered`/`StreamedNative`.

## Responses

Fixed response:

```cpp
res.set_status(200);
res.set_header("X-Request-Id", id);
res.set_content(body, "application/json; charset=utf-8");
```

`set_content(std::string_view)` stores a view and is zero-copy; the storage must
outlive the response write. Use `set_content(std::string)` for owned bodies.

Native streaming:

```cpp
res.take_over_task([](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
    const realurl::server::Header headers[] = { { "content-type", "text/plain" } };
    co_await writer.write_head(200, headers, std::nullopt);
    co_await writer.write("one");
    co_await writer.write("two");
    co_await writer.end();
});
```

`StreamWriter` methods:

| Method | Meaning |
| --- | --- |
| `write_head(status, headers, content_length)` | Send the response head. Without `content_length`, HTTP/1.1 uses chunked and HTTP/2 omits it. |
| `write(string_view)` | Queue body bytes; returns false when the client is gone. |
| `end()` | Finish the body (chunk terminator / END_STREAM). |
| `cancel()` | Abort the stream (H1 close, H2 RST_STREAM). |
| `closed()` | True once the client or connection is gone. |

HTTP/1.1 responses use `Content-Length` when known, otherwise chunked. HTTP/2
responses use HEADERS + DATA; one stream never blocks another.

### Legacy takeover

`take_over()` (blocking pump thread) and `take_over_async()` (Boost.Asio
awaitable) are kept for source compatibility and run through a bounded compat
path. They are not used by the native server path. Over TLS both return 501;
use `take_over_task()` instead. `set_pump_threads/queue_limit/idle_timeout`
still configure the legacy path.

## Keep-alive, limits and errors

HTTP/1.1 keep-alive is on by default (`set_keep_alive(false)` for the old
one-request-per-connection behavior). A response with `Connection: close` or an
HTTP/1.0 request without `keep-alive` closes the connection. Pipelined requests
are parsed from the same buffer, up to 16 outstanding.

| Setting | Default | Behavior |
| --- | --- | --- |
| `set_payload_max_length(n)` | 0 (unlimited) | 413 before or during the body read |
| `set_header_limit(n)` | 1 MiB | 431 |
| `set_error_body(status, type, body)` | empty | Generated 400/404/408/413/431/500/503 body |
| `set_read_header_timeout(ms)` | 5 s | Header wait; 0 disables |
| `set_request_idle_timeout(ms)` | 60 s | Idle wait between body chunks |
| `set_client_write_timeout(ms)` | 60 s | Per-write wait; not a total stream deadline |
| `set_io_threads(n)` | hardware concurrency | Number of EventLoop/listener threads |
| `set_max_connections(n)` | 0 (unlimited) | Per loop |
| `set_queue_watermark(high, low)` | 64 KiB / 32 KiB | Per-stream output queue |
| `set_strategy(...)` | Auto | `Latency`, `Throughput`, `Bandwidth`, `Tunnel` |
| `set_keep_alive(bool)` | true | HTTP/1.1 connection reuse |

Errors use the canonical reason phrase and `Connection: close`. A header/body
read timeout is 408. A handler exception before any response byte is 500.

## In-loop upstream proxy

```cpp
realurl::Config upstream;
upstream.max_conns_per_origin = 1;
upstream.max_streams_per_conn = 1000;
server.set_upstream(upstream);   // one Client per loop

server.Post("/v1/chat", [url](const realurl::Server::Request &req,
                              realurl::Server::Response &res) {
    res.take_over_task([&req, url](realurl::server::StreamWriter &out)
                           -> realurl::Task<void> {
        co_await realurl::server::proxy_stream(*req.upstream, url, req, out);
    });
}, { .body_mode = realurl::Server::BodyMode::StreamedNative });
```

`proxy_stream()`:

- forwards request headers (hop-by-hop removed) and streams the request body;
- waits for upstream HEADERS, writes the downstream head with the same status;
- reads upstream DATA and writes each chunk downstream, then ends the stream;
- returns 502 only before the downstream head; after it, the stream is closed;
- cancels the upstream H2 stream when the downstream writer fails.

The upstream `Client` and downstream connection are on the same EventLoop. A
slow downstream makes `writer.write()` suspend, which stops the upstream read
loop and closes the H2 flow-control window; no bridge or thread hop is involved.

## HTTP/2 downstream

Plaintext h2c uses the client preface (`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`) to
select the H2 frontend; anything else goes to HTTP/1.1. TLS uses ALPN (`h2` or
`http/1.1`). H2 supports SETTINGS/ACK, HEADERS/CONTINUATION, DATA, WINDOW_UPDATE,
RST_STREAM, PING, GOAWAY, trailers and `MAX_CONCURRENT_STREAMS`; inbound and
outbound flow control are tracked per stream and per connection. The H2 writer
round-robins streams so one slow stream cannot exhaust the shared connection
window.

## Native downstream (realurl/1)

The server also accepts the realurl native transport from realurl clients. It
is enabled by default:

```cpp
server.set_native(false);   // before listen(): HTTP/1.1 + HTTP/2 only
```

- TLS clients that offer ALPN `realurl/1` get the native codec; clients that
  offer only `h2` or `http/1.1` are unaffected.
- Plaintext clients are detected by the 11-byte `REALURL/1\r\n` preamble,
  before the h2 preface; anything else is HTTP/1.1.
- Handlers and takeover writers are unchanged. In native requests,
  `Request::version` is `"realurl/1"`; `:authority` and `:scheme` are not
  transmitted, so `host` is only present if the caller set it as a regular
  header.
- `Server::Stats` reports `h1_connections`, `h2_connections` and
  `native_connections`.

The upstream client configured with `set_upstream()` takes the same
`realurl::Config`, so a gateway can proxy native downstream to native upstream:

```cpp
realurl::Config upstream;
upstream.protocol = realurl::Protocol::Auto;   // or Protocol::Native
server.set_upstream(upstream);
```

See `docs/native.md` for the wire format and negotiation.

## TLS and kTLS

```cpp
server.set_tls("cert.pem", "key.pem");
server.set_ktls(true);   // Linux best-effort
```

OpenSSL 3.x performs the handshake (TLS 1.2/1.3, ALPN, session tickets). On
Linux the context sets `SSL_OP_ENABLE_KTLS`; `TlsServerStream` reports whether
the kernel took over TX/RX via `BIO_get_ktls_send/recv`. If the kernel/cipher
does not support kTLS, the connection falls back to user-space TLS. TLS
takeover must use `take_over_task()`; the legacy fd writer cannot speak TLS.

## In-memory dispatch

`Server::process(raw_request)` parses one HTTP/1.1 request and returns the raw
response bytes without sockets or worker threads. Fixed responses and
`take_over_task` are supported; the task runs on a private EventLoop.
