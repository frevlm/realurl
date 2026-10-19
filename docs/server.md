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
into the connection frame.

There is intentionally no `set_chunked_content_provider`. Streaming responses
use raw takeover so the application controls the exact wire bytes.

## Limits and errors

| Setting | Default | Behavior |
| --- | --- | --- |
| `set_payload_max_length(n)` | 0 (unlimited) | Buffered: 413 during parse. Streamed: 413 before the handler or after the task if nothing was written. |
| `set_header_limit(n)` | 1 MiB | 431 |
| `set_error_body(status, type, body)` | empty | Body used for generated 400/404/413/431/500 responses |
| `set_read_header_timeout(ms)` | 5 s | 0 disables |
| `set_client_write_timeout(ms)` | 60 s | Used by `take_over` writers |
| `set_io_threads(n)` | hardware concurrency | 0 selects the default |

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
| pump threads | `take_over` tasks |

Buffered handlers run on an io thread, so long blocking work blocks one io
thread. Streamed handlers should return quickly and put the real work in
`take_over`.
