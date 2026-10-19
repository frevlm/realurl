# Design

This document describes the data structures and threading model. The behavior is intentionally the same as the implementation extracted from revlm; the performance report in `performance.md` explains why each piece exists.

## Data flow

A streaming proxy request involves four moving parts:

```text
client socket ──► BodyFeeder ──► BodySource ──► curl READFUNCTION ──► upstream
                                                                        │
client socket ◄── ClientWriter ◄── ResponseSink ◄── curl WRITEFUNCTION ◄┘
```

- `BodyFeeder` reads the client body on its own thread into a bounded queue of 64 KiB blocks borrowed from `BufferPool`.
- `BodySource` exposes `read()`, `consume()`, `peek()`, and `wake()` to the curl read callback.
- `CurlMultiPool` owns one `CURLM*` per thread and attaches one easy handle per in-flight stream.
- `ClientWriter` writes response bytes to the client fd with a deadline.
- `ResponseSink` lets the same gateway code write to a Beast stream, a cpp-httplib response, or any other server.

## Core data structures

### `BodySource`

A pull-based window with four callbacks:

| Callback | Semantics |
| --- | --- |
| `read()` | Return the next unconsumed bytes, or `{wait:true}`, or `{eof:true}`. Does not advance the cursor. |
| `consume(n)` | Advance the cursor after the consumer copied `n` bytes. |
| `peek()` | Copy the retained prefix (used for request inspection before the body is fully consumed). |
| `wake()` | Called by the producer after appending bytes; the curl owner thread performs `curl_easy_pause(CURLPAUSE_CONT)`. |

Keeping `read` and `consume` separate means the curl callback can copy directly out of the pool block without an intermediate buffer or a second allocation.

### `CurlRequest` / `CurlResponse`

`CurlRequest` is a value object. The pool never retains it; every callback and the body source must stay alive for the duration of `execute()` or `execute_stream()`. The fields are grouped by concern:

- request line and headers,
- body production (`initial_body_chunk`, `content_length`, `body_source`),
- response consumption (`on_headers`, `on_response_chunk`),
- four timeouts (connect, upload-stall, header, total),
- security (`dns_pin`),
- optional upload rate flow.

### `CurlMultiPool::StreamResult`

The result of `execute_stream()` contains the status, headers, any bytes that arrived before headers completed (`initial_body`), and four callable handles: `stream_read`, `stream_error`, `stream_error_message`, `stream_close`.

The returned object is a *view* over shared state. It is copyable, but all copies refer to the same transfer. `stream_close()` must be called on the owning thread before the last copy is destroyed; see the known limitations in the README.

### `BufferPool`

A fixed-capacity free list of 64 KiB blocks. `borrow()` returns a `unique_ptr<char[]>` whose deleter returns the block to the pool. When the free list is empty, `borrow()` allocates an emergency block that is deleted directly. The pool must outlive every borrowed buffer.

### `UploadRateLimiter`

A process-wide coordinator implementing a two-pool bandwidth allocation:

- Flows are classified once at join time by `Content-Length`: at or above `classify_bytes` → limited pool; below → normal pool; unknown → limited.
- Every limited flow receives a cap from its first byte, because applying `CURLOPT_MAX_SEND_SPEED_LARGE` mid-transfer stalls curl for several seconds.
- A 200 ms tick computes `backlog = Σ remaining normal bytes`, then allocates the remaining pipe budget across limited flows.
- Down-scaling is capped at 2× per tick and skips flows that made no progress, avoiding cascading stalls.

The owner thread applies the cap; the coordinator only stores an atomic target.

### `StreamPumpPool`

A bounded pool of long-lived worker threads. Tasks are hashed by client IP to a fixed worker so a client's streams reuse the same thread-local `CurlMultiPool` and therefore the same upstream connections.

`StreamGate` is the completion primitive: the pump thread calls `finish()`, which posts the registered callback back onto the connection's asio executor. The connection coroutine awaits the gate instead of blocking an io thread for the stream duration.

## Threading model

| Thread | Owns | May touch |
| --- | --- | --- |
| Connection io thread | `BeastResponseSink`, asio socket | server-side read/write, gate registration |
| Body feeder thread | client fd read side | `BodyFeeder` queue, `BodySource::wake` |
| Pump worker thread | `CurlMultiPool` (thread-local), easy handles | curl handles, rate flow cap, `ClientWriter` |
| Rate coordinator | mutex-protected flow map | atomic targets only |

No easy handle is touched from more than one thread. The only cross-thread curl operation is `curl_multi_wakeup()`, which is explicitly thread-safe.

## Timeout model

Four independent segments prevent one failure mode from masking another:

| Segment | curl option / mechanism | Default |
| --- | --- | --- |
| Connect | `CURLOPT_CONNECTTIMEOUT` | 5 s |
| Upload stall | `CURLOPT_LOW_SPEED_LIMIT` + `CURLOPT_LOW_SPEED_TIME` | 10 KB/s for 30 s |
| Response headers | `drive_until_headers()` deadline | 120 s |
| Inter-event idle | `stream_read(..., idle_timeout_ms)` | caller-provided |
| Total transfer | `CURLOPT_TIMEOUT` | disabled (0) |

A wall-clock total timeout is disabled by default because a large body that keeps making progress should not be killed. The upload-stall and idle segments handle stuck transfers instead.

## Why per-thread multi handles

A `curl_multi` handle caches connections. Creating one multi handle per stream prevents reuse and forces a new TCP/TLS handshake for every request. Creating one shared multi handle requires serializing access, because easy handles and multi handles are not thread-safe.

Per-thread multi handles split the difference: each thread reuses its own connections, and streams never contend on a shared curl lock. `StreamPumpPool` then pins a client's streams to one thread, which keeps connection reuse stable across requests.
