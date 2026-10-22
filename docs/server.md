# Server

`realurl::Server` is a small HTTP/1.1 server with an httplib-like routing API.
It exists so the HTTP entry point can live in realurl while the application
keeps only route handlers.

## Quick start

```cpp
#include <realurl/server.hpp>

realurl::Server server;

server.Get("/hello", [](const auto &, auto &res) {
    res.set_content("world", "text/plain");
});

server.Post("/echo", [](const auto &req, auto &res) {
    res.set_content(req.body, "text/plain");
});

server.Get("/events", [](const auto &, auto &res) {
    res.take_over([](realurl::ClientWriter &writer) {
        (void)writer("HTTP/1.1 200 OK\r\n"
                     "Content-Type: text/event-stream\r\n"
                     "Connection: close\r\n\r\n");
        (void)writer("data: one\n\n");
    });
});

if (port == 0) { /* ... */ }
server.listen("0.0.0.0", 8080); // blocks until stop()
```

`stop()` may be called from another thread. It closes the acceptor, waits up to
30 seconds for active connections, then stops the io threads.

## Routing

| API | Match |
| --- | --- |
| `Get/Post/Put/Delete(pattern, handler)` | exact path, or `:name` segments such as `/users/:id` |
| `GetRegex/PostRegex/PutRegex/DeleteRegex(pattern, handler)` | `std::regex_match` on the path |
| `set_pre_routing_handler(handler)` | runs before routing; return `Handled` to short-circuit |

Match precedence is exact, then `:param`, then regex. `HEAD` falls back to a
`GET` route and suppresses the body. Unknown paths return 404 with the body
registered by `set_error_body(404, ...)`.

`Request::path` has the query removed. Percent decoding is enabled by default
(`set_path_decoding(false)` keeps the raw path, which streaming proxies that
care about `%2F` should use).

## Body modes

The body mode is a property of the route, decided before any body bytes are
read:

```cpp
server.Post("/api/login", handler); // Buffered (default)
server.Post("/v1/chat/completions", handler,
            { .body_mode = realurl::Server::BodyMode::Streamed });
```

- **Buffered**: `Request::body` contains the whole body. Chunked request bodies
  are decoded. This is the management/JSON mode.
- **Streamed**: `Request::body_source` is a `BodySource`; `Request::body` is
  empty. The server starts a `BodyFeeder` over the client socket, so memory is
  bounded by the feeder window. A `Content-Length` is required; chunked bodies
  are rejected with 400. Handlers must consume at most
  `Request::content_length` bytes.

## Responses

Fixed response:

```cpp
res.set_status(200);
res.set_header("X-Request-Id", id);
res.set_content(body, "application/json; charset=utf-8");
```

Raw streaming response:

```cpp
res.take_over([request = std::move(request)](realurl::ClientWriter &writer) {
    // The task runs on a pump-pool thread. Write the head and body yourself.
    if (!writer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                "Connection: close\r\n\r\n"))
        return;
    // ... pump events ...
});
```

The handler returns immediately; the connection coroutine waits for the pump
task to finish. `Response::status` is used for access logging and for the
generated 413 when the body limit was exceeded before any byte was written.
The task must not outlive its invocation; the `ClientWriter` is a reference
into the connection frame. This path uses one pump thread per active stream,
so it is bounded by the pump capacity and queue.

Async takeover, for thousands of concurrent streams:

```cpp
res.take_over_async([](realurl::AsyncClientWriter &writer)
    -> boost::asio::awaitable<void> {
    if (!(co_await writer.write("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                                "Connection: close\r\n\r\n")))
        co_return;
    co_await writer.write("data: one\n\n");
});
```

The handler is a Boost.Asio coroutine on an io thread. `write()` suspends it
until the socket accepts the bytes and returns false on disconnect or the
client write timeout, so one io thread can drive many streams. The handler must
not block; use asio timers/sockets or post work to another executor. This path
does not consume a pump thread and is not limited by the pump capacity or
queue; concurrency is bounded by io threads, fds, and memory. The buffer passed
to `write()` must stay alive until the awaitable completes.

There is intentionally no `set_chunked_content_provider`. Streaming responses
use raw takeover so the application controls the exact wire bytes.

### Upstream requests from an async takeover

`take_over_async` handlers run on an io thread and must not block. Use
`realurl::async_request()` from `<realurl/asio_bridge.hpp>` to call the
HTTP/2 client without blocking that thread:

```cpp
#include <realurl/asio_bridge.hpp>

realurl::ClientGroup upstream(2);

res.take_over_async([&upstream](realurl::AsyncClientWriter &writer)
    -> boost::asio::awaitable<void> {
    realurl::Response upstream_response;
    bool failed = false;
    try {
        upstream_response =
            co_await realurl::async_request(upstream, "GET", "http://127.0.0.1:8080/v1/chat");
    } catch (...) {
        failed = true;
    }
    if (failed) {
        co_await writer.write("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
        co_return;
    }
    const std::string head = "HTTP/1.1 200 OK\r\nContent-Length: " +
        std::to_string(upstream_response.body.size()) + "\r\n\r\n";
    if (!(co_await writer.write(head)))
        co_return;
    co_await writer.write(upstream_response.body);
});
```

The request runs on one of the group's EventLoop workers; the result is posted
back to the handler's executor, so the io thread is free while the upstream is
in flight. One worker multiplexes every stream over its pooled h2 connection;
raise `Config::max_streams_per_conn` when you expect thousands of concurrent
upstream streams. The `ClientGroup` must outlive the operation. The bridge
buffers the upstream response before completing; it does not relay chunks as
they arrive, and cancellation of the waiting handler is not propagated
upstream.

The completion-token form is available for callbacks and futures:
`async_request(group, method, url, headers, body, timeout, token)` completes as
`void(std::exception_ptr, Response)`. With `boost::asio::use_awaitable` the
exception is rethrown at the `co_await` site.

## Limits and errors

| Setting | Default | Behavior |
| --- | --- | --- |
| `set_payload_max_length(n)` | 0 (unlimited) | Buffered: 413 during parse. Streamed: 413 before the handler or after the task if nothing was written. |
| `set_header_limit(n)` | 1 MiB | 431 |
| `set_error_body(status, type, body)` | empty | Body used for generated 400/404/413/431/500/503 responses |
| `set_read_header_timeout(ms)` | 5 s | 0 disables |
| `set_client_write_timeout(ms)` | 60 s | Used by `take_over` writers |
| `set_io_threads(n)` | hardware concurrency | 0 selects the default |
| `set_pump_threads(n)` | automatic | Ceiling on `take_over` workers; 0 selects the automatic value. Workers start at `min(capacity, CPUs)` and grow on demand. `REALURL_PUMP_THREADS` sets the same ceiling. Must be called before the first streaming response. |
| `set_pump_queue_limit(n)` | fd-budget remainder, capped at capacity | Maximum `take_over` tasks waiting for a worker; 0 selects the automatic value. When the queue is full, new streaming requests get 503 + `Retry-After: 1`. `REALURL_PUMP_QUEUE` sets the same limit. Must be called before the first streaming response. |
| `set_pump_idle_timeout(ms)` | 60 s | Workers above the initial size exit after this idle time; 0 disables shrinking. `REALURL_PUMP_IDLE_MS` sets the same value. Must be called before the first streaming response. |

Every response carries `Connection: close`; the server handles one request per
connection. The status line always uses the canonical reason phrase.

## In-memory dispatch

`Server::process(raw_request)` parses one request and returns the raw response
bytes without opening a socket or starting worker threads. Buffered routes and
`take_over` are both supported; `take_over` writes into the returned string.
This is the replacement for httplib's in-memory `Server::process` in tests.

```cpp
const std::string response =
    server.process("GET /hello HTTP/1.1\r\nHost: test\r\n\r\n");
```

An empty return value means parsing failed; callers usually map that to 400.

## Threading

| Thread | Work |
| --- | --- |
| io threads | accept, header/body read, routing, buffered handlers, response writes |
| feeder thread (one per Streamed request) | reads the client body into the bounded window |
| pump threads | blocking `take_over` tasks |
| io threads | `take_over_async` coroutines (no pump worker) |
| ClientGroup worker threads (application-owned) | upstream requests issued through `realurl::async_request` |

Buffered handlers run on an io thread, so long blocking work blocks one io
thread. Streamed handlers should return quickly and put the real work in
`take_over`.

A `take_over` task blocks its pump thread for the stream's whole lifetime, so
the pump capacity is also the ceiling on concurrently running streams. Waiting
work is bounded: at most `queue_limit()` tasks wait. The automatic queue limit
is what remains of the fd budget after reserving one fd per worker (running and
queued streams both hold a client fd), capped at the capacity. If every worker
is busy at capacity and the queue is full, the request is not submitted; the
server writes `503 Service Unavailable` with `Retry-After: 1` and closes the
connection. The 503 body comes from `set_error_body(503, ...)` when configured.

The queue rules above apply to blocking `take_over`; `take_over_async` bypasses
the pump entirely and is bounded by the io threads, file descriptors, and
memory instead.

Workers are elastic. The pool starts with `min(capacity, CPUs)` threads and
adds a worker only while tasks are backlogged, up to the capacity. Workers
above the initial size exit after `set_pump_idle_timeout` (default 60 s), so a
burst's extra threads are reclaimed. The automatic capacity is the smallest
budget exposed by the process environment:

- thread budget: `pids.max` (cgroup v2/v1), `RLIMIT_NPROC`, kernel
  `threads-max`/`pid_max`, minus the current thread count, with half reserved
  for io threads and application threads;
- fd budget: `RLIMIT_NOFILE` minus a quarter for listeners, upstream sockets,
  and housekeeping (each streaming response holds one client fd);
- address-space budget: `RLIMIT_AS` divided by the thread stack size, when a
  virtual-memory limit is set.

`set_pump_threads(n)`/`REALURL_PUMP_THREADS` override the computed ceiling;
`set_pump_queue_limit(n)`/`REALURL_PUMP_QUEUE` override the queue.
`StreamPumpPool::capacity()` reports the ceiling, `size()` the current worker
count, `queue_limit()` the waiting bound, and `rejected()` the number of
refused submissions. This keeps the default useful on a 2-core container and
on a 64-core host without hardcoding a thread count; it also follows the cgroup
limits that `hardware_concurrency()` ignores.
