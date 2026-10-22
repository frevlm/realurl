# Performance

All numbers measured on Apple M5 / macOS, loopback, h2c prior knowledge,
Release build, one pooled HTTP/2 connection (`connections=1` in every run).
The benchmark (`bench/realurl_bench`) warms up the connection before starting
the clocks, so percentiles reflect steady state. RSS/CPU include the in-tree
test server that runs in the same process. The tables below the architecture
pass are the original 0.3 baseline.

## TLS session resumption

`tls_connect` opens sequential new TLS connections to a realurl `Server`:
TCP connect, handshake, h2 preface, and the server's first SETTINGS frame.
`full` gives every connection an empty session cache; `resume` shares one
warmed `TlsContext`, so every handshake resumes (2000/2000 in every run).
Apple M5 / macOS, loopback, Release build, OpenSSL 3, TLS 1.3, 2000
connections, median of 3 runs (spread under 1%). CPU is client plus server
in the same process; wire bytes come from the loopback interface counter and
include TCP/IP headers.

| Server certificate | Handshake | conn/s | p50 | p99 | CPU/conn | Wire/conn |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ECDSA P-256 (`tests/certs`) | full | 2,378 | 0.417 ms | 0.451 ms | 461 µs | 5,290 B |
| ECDSA P-256 | resume | 2,789 | 0.355 ms | 0.401 ms | 386 µs | 4,670 B |
| RSA 2048 | full | 1,274 | 0.786 ms | 0.952 ms | 716 µs | 5,766 B |
| RSA 2048 | resume | 2,796 | 0.354 ms | 0.443 ms | 382 µs | 4,668 B |

Resumption skips the certificate signature and its verification, so its cost
does not depend on the certificate: about 0.35 ms either way. That saves 15%
of handshake latency and 16% of CPU with ECDSA, and 55% of latency and 47% of
CPU with RSA 2048, plus the certificate bytes. Without 0-RTT a TLS 1.3
resumption still takes one round trip, the same as a full handshake, so on a
real network the win is CPU and bytes, not round trips.

```sh
./build-release/bench/realurl_bench tls_connect 2000 full
./build-release/bench/realurl_bench tls_connect 2000 resume
./build-release/bench/realurl_bench tls_connect 2000 resume rsa.pem rsa-key.pem
```

## Two-end hot path (0.5.x)

Measured on Apple M5 / macOS, loopback, Release build. `base` is commit
`0a4cffa` (the native transport release, the code behind the 0.5 tables
below); `new` is the hot-path pass described here. The two binaries ran
interleaved, five rounds per row, and the table reports the median of each
column. The host had a background process holding 90-260% CPU during these
runs, so absolute numbers sit below the 0.5 tables; compare rows against each
other, not across sections.

### Unary: 200,000 requests, 1,000 streams, 1 connection

| Protocol | Build | req/s | p50 | p99 | CPU/req | Peak RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| h2 | base | 528,888 | 1.582 ms | 4.310 ms | 2.0 µs | 87.0 MB |
| h2 | new | 914,461 | 1.012 ms | 1.700 ms | 1.7 µs | 16.7 MB |
| native | base | 721,061 | 1.166 ms | 2.940 ms | 1.5 µs | 86.5 MB |
| native | new | **2,026,461** | **0.419 ms** | **1.198 ms** | **0.7 µs** | **16.3 MB** |

Native: **2.8x throughput, -64% p50, -59% p99, -53% CPU/req, -81% peak RSS**.
h2 shares the stream engine and gains 1.7x.

### Single stream: 40,000 requests, concurrency 1

| Protocol | Build | req/s | p50 | p99 | CPU/req |
| --- | --- | ---: | ---: | ---: | ---: |
| h2 | base | 48,826 | 0.017 ms | 0.075 ms | 16.3 µs |
| h2 | new | 52,229 | 0.015 ms | 0.067 ms | 14.8 µs |
| native | base | 52,775 | 0.016 ms | 0.068 ms | 14.9 µs |
| native | new | 56,520 | 0.015 ms | 0.062 ms | 13.1 µs |

Native: +7% throughput, -12% CPU/req. This path is bound by the socket round
trip; the gain is the dropped EAGAIN `recv` on each side.

### 2 MiB upload + 20 s SSE: 100 concurrency, 1 connection

Three interleaved native runs per build:

| Build | TTFT p50 | CPU/req | Peak RSS |
| --- | ---: | ---: | ---: |
| base | 141.7 / 158.2 / 153.0 ms | 1.94 / 1.82 / 2.21 ms | 568 / 539 / 567 MB |
| new | 146.8 / 132.9 / 144.0 ms | 2.16 / 2.08 / 2.01 ms | 555 / 573 / 556 MB |

Parity within noise. Wall time (19.15 s) is set by the 20 s stream; the
upload and long-lived streams do not exercise the per-request costs removed
here. The flat stream buffer does not raise peak RSS.

### Where the time went

Profiles (`sample`, 1,000-way native unary) of `base` showed the client and
server loops idle 35-50% of the time: the two ends alternate, so per-request
CPU on either side sets throughput. Header coding was minor. The costs were:

1. **16 KiB buffers inside coroutine frames.** `Client::request` and the
   server's `run_handler` each kept `char buffer[16384]` across a `co_await`,
   so the buffer lived in the frame and pushed it into the 32 KiB pool class.
   That class caches 256 frames per thread; at 1,000 in flight every request
   paid a 32 KiB malloc/free. Bodies now drain straight from the stream queue
   into `Response::body` / `Request::body`, and a request with no body never
   enters the read path. This is also the RSS drop.
2. **Per-stream `std::deque<std::string>` queues.** The first push on each
   stream allocated a ~4 KiB deque block and freed it at stream close: ~19% of
   server CPU and ~14% of client CPU. Both ends now use `ByteQueue`, one
   contiguous buffer with amortized compaction.
3. **A separate END_STREAM frame.** A unary response was HEADERS + DATA +
   empty DATA(END_STREAM). END_STREAM now rides on the frame carrying the last
   byte, and an empty body ends on HEADERS. Request bodies get the same.
4. **EAGAIN `recv` after every read.** `TcpStream` now remembers that a short
   read drained the socket and parks until the next read edge.
5. **Small per-request allocations:** HEADERS built in a temporary
   `FrameWriter`, the response head encoded into a per-stream string, a header
   view vector per response on both ends, and an origin key that exceeded the
   short-string buffer on every request. All of them now reuse
   connection-owned buffers.

### Reproduce

```sh
./build/bench/realurl_bench h2_unary 200000 1000 1 h2
./build/bench/realurl_bench h2_unary 200000 1000 1 native
./build/bench/realurl_bench h2_unary 40000 1 1 h2
./build/bench/realurl_bench h2_unary 40000 1 1 native
./build/bench/realurl_bench native_llm 100 100 20 1 1 native
```

## Native transport A/B (realurl/1, 0.5)

Measured on Apple M5 / macOS, loopback, Release build, OpenSSL 3.6.5. Both
protocols use the same realurl `Server` and the same handler; only
`Config::protocol` changes (`Protocol::Http2` vs `Protocol::Native`). Runs are
interleaved on the same machine, one event loop and one connection. The
1,000-way table reports every run plus the median; the first h2 run is an
outlier and is kept rather than hidden.

### Unary: 50,000 requests, 1,000 streams, 1 connection

| Protocol | Run | req/s | p50 | p99 | CPU/req | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| h2 | 1 | 477,837 | 1.787 ms | 5.961 ms | 3.1 µs | 81.8 MB |
| h2 | 2 | 711,702 | 1.308 ms | 3.466 ms | 1.5 µs | 82.1 MB |
| h2 | 3 | 686,534 | 1.368 ms | 3.483 ms | 1.6 µs | 81.8 MB |
| native | 1 | 903,495 | 0.964 ms | 2.882 ms | 1.3 µs | 82.1 MB |
| native | 2 | 926,813 | 0.976 ms | 3.000 ms | 1.2 µs | 82.1 MB |
| native | 3 | 921,024 | 0.993 ms | 3.021 ms | 1.2 µs | 82.4 MB |
| **median h2** | | **686,534** | **1.368 ms** | **3.483 ms** | **1.6 µs** | **81.8 MB** |
| **median native** | | **921,024** | **0.976 ms** | **3.021 ms** | **1.2 µs** | **82.1 MB** |

Native vs h2 on the medians: **+34% throughput, -29% p50, -25% cpu/req**, with
no RSS change.

### Single stream: 20,000 requests, concurrency 1

| Protocol | Run | req/s | p50 | p99 | CPU/req |
| --- | ---: | ---: | ---: | ---: | ---: |
| h2 | 1 | 55,508 | 0.017 ms | 0.037 ms | 14.4 µs |
| h2 | 2 | 58,619 | 0.017 ms | 0.024 ms | 14.1 µs |
| h2 | 3 | 58,411 | 0.017 ms | 0.025 ms | 14.2 µs |
| native | 1 | 61,582 | 0.016 ms | 0.023 ms | 12.8 µs |
| native | 2 | 61,528 | 0.016 ms | 0.023 ms | 12.9 µs |
| native | 3 | 61,996 | 0.016 ms | 0.023 ms | 12.7 µs |

Native: **+5.5% throughput, -9% cpu/req**, p50 0.016 vs 0.017 ms. The 10%
single-stream p50 gate was not met: the latency difference is one clock tick
at this resolution, so cpu/req and throughput are the meaningful figures.
This path is dominated by the socket round trip, not header codec work.

### 2 MiB upload + 20 s SSE: 100 concurrency, 1 connection

| Protocol | Wall | TTFT p50 | TTFT p99 | CPU/req | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| h2 | 19.09 s | 78.3 ms | 86.5 ms | 1.415 ms | 576.2 MB |
| native | 19.08 s | 70.1 ms | 79.5 ms | 1.238 ms | 582.9 MB |

Wall time is identical, TTFT is 10% lower, cpu/req is 12.5% lower, and peak
RSS is +1.2% (within the +5% gate). `content-length` in the native response
head lets the client reserve the body, and the head codec removes per-request
header allocations.

### Large body + delayed response: 2 MiB upload, 5 s delay, 20 concurrency

| Protocol | Wall | TTFT p50 | Peak RSS |
| --- | ---: | ---: | ---: |
| h2 | 5.02 s | 5016.8 ms | 152.1 MB |
| native | 5.02 s | 5021.0 ms | 140.5 MB |

The artificial delay dominates; the expected parity holds. Native peak RSS was
lower in this run (140.5 vs 152.1 MB).

### Reproduce

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DREALURL_BUILD_BENCH=ON
cmake --build build --target realurl_bench

./build/bench/realurl_bench h2_unary 50000 1000 1 h2
./build/bench/realurl_bench h2_unary 50000 1000 1 native
./build/bench/realurl_bench h2_unary 20000 1 1 h2
./build/bench/realurl_bench h2_unary 20000 1 1 native
./build/bench/realurl_bench native_llm 100 100 20 1 1 h2
./build/bench/realurl_bench native_llm 100 100 20 1 1 native
```

`native_llm` uses a realurl `Server` endpoint for both protocols; the
historical `llm` mode still uses the in-tree h2-only test server and is not
directly comparable to these A/B numbers.

## Frame size and TCP_NOTSENT_LOWAT (not adopted)

`native_bulk <requests> <concurrency> <body_kib> <connections> <protocol>` does
a POST of `body_kib` and reads the same number of bytes back through a realurl
`Server`. Apple M5 / macOS, loopback, Release build, medians of 3 interleaved
runs, `native_bulk 1000 8 2048 1 native` (other agents were loading the host, so
run-to-run spread is about +/-15%).

| Native SETTINGS_MAX_FRAME_SIZE | 2 MiB bulk MB/s (each way) | 64 KiB bulk MB/s | `native_llm` TTFT p50 |
|---|---:|---:|---:|
| 16 KiB (current) | 2028 | 2263 | 93 ms |
| 64 KiB | 2013 | 2138 | 116 ms |
| 256 KiB | 2121 | 1798 | 104 ms |

No measurable win, so native mode keeps 16 KiB. `TCP_NOTSENT_LOWAT = 16 KiB` on
both ends gave a bulk median of 770 MB/s against 850 MB/s without (6 interleaved
runs) and an unresolved TTFT difference (150 ms vs 160 ms, 3 runs), so it is not
applied either.

## Single-runtime server (0.4)

Measured on Apple M5 / macOS, loopback, Release build, OpenSSL 3.6.5. The
client, upstream h2c test server and downstream server run in the same process,
so CPU/RSS include all three. `proxy_inline` uses one loop, one per-loop
upstream Client, and `BodyMode::StreamedNative` (2 MB request bodies are
forwarded chunk by chunk, not buffered).

### Inline proxy vs buffered bridge (2 MB POST + 20 s SSE)

| Mode | Concurrency | TTFT p50 | Total p50 | CPU | Peak RSS |
|---|---:|---:|---:|---:|---:|
| inline proxy | 100 | **22.2 ms** | 20.25 s | 0.75 s | **34.6 MB** |
| buffered bridge | 100 | 20,235.0 ms | 20.24 s | 1.51 s | 445.0 MB |
| inline proxy | 1,000 | **248.8 ms** | 20.24 s | 9.73 s | 170.3 MB |

- 1,000 concurrent streams uploaded 2,097,152,000 bytes (2 MB each) with
  `received == expected` and zero failures.
- TTFT includes the upstream 2 MB upload; the first response chunk is written
  as soon as the upstream emits it. The buffered bridge waits for END_STREAM,
  which is why its TTFT equals the full 20 s stream.
- Streamed request bodies keep RSS bounded: 100 streams used 34.6 MB instead
  of 445 MB for the buffered bridge; 1,000 streams peaked at 170 MB.

### H2 downstream unary

| Requests | Streams / connection | req/s | p50 | p99 | CPU/req | Peak RSS |
|---:|---:|---:|---:|---:|---:|---:|
| 20,000 | 100 | 354,659 | 0.268 ms | 0.544 ms | 2.8 µs | 15.3 MB |
| 50,000 | 1,000 | **685,068** | 1.259 ms | 5.104 ms | 1.5 µs | 82.4 MB |

One EventLoop and one downstream H2 connection; the client and server share the
process. The 1,000-stream run completes in 0.07 s with zero failures.

### H1 downstream keep-alive

| Requests | Connections | req/s | p50 | p99 | CPU/req |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 8 | 205,539 | 0.037 ms | 0.083 ms | 12.4 µs |
| 200,000 | 32 | 208,773 | 0.148 ms | 0.305 ms | 13.0 µs |

One server loop. CPU includes the in-process load generator. HTTP/1.1 pays a
recv, a parse and a send per request; H2 multiplexing is the scaling path.

### Linux validation

Ubuntu 24.04 container, 2 cores / 3 GB, clang 18.1.3, OpenSSL 3.0.13,
`REALURL_WITH_IO_URING=ON`:

- 22/22 tests pass, including `h2_server_test`, `proxy_test`,
  `tls_server_test`, `ktls_test` and `uring_test`.
- `uring: available=1 multishot=1`: the provided-buffer ring registered and
  `IORING_OP_RECV + IORING_RECV_MULTISHOT` is active. This requires
  `--security-opt seccomp=unconfined`; Docker's default seccomp profile blocks
  `io_uring_setup`, in which case the readiness fallback runs and all tests
  still pass.
- kTLS: `SSL_OP_ENABLE_KTLS` is requested, but the linuxkit kernel used by
  Docker Desktop has no TLS ULP (`/proc/net/tls_stat` is absent), so
  `BIO_get_ktls_send/recv` are 0 and the connection correctly falls back to
  user-space TLS. A bare-metal kernel with kTLS is required to measure the
  offload.

### io_uring

`REALURL_WITH_IO_URING=ON` builds the completion transport (raw kernel UAPI,
no liburing). Plaintext recv uses `IORING_OP_RECV` with
`IORING_RECV_MULTISHOT` and a registered provided-buffer ring (256 x 64 KiB);
send remains one SQE per batch. Kernels or sandboxes that reject the buffer
ring fall back to one-shot recv, and a loop with no usable ring falls back to
the edge-triggered `TcpStream` path.

## Current architecture pass

Measured on the same machine before and after the persistent-poll, frame-pool
and connection-coalescing changes:

| Run | Result |
| --- | --- |
| `requests 50000 1` | 39,042 req/s, p50 0.025 ms, 16.0 MB peak |
| `requests 200000 1000 1` | 401-463k req/s over 5 runs, p50 2.37 ms, p99 3.73 ms, 4.6 µs CPU/req |
| `requests_mt 200000 1000 2 1` | 543-575k req/s across two loops, p50 1.80 ms |
| `requests_mt 200000 1000 4 1` | 211,266 req/s (server-limited, see note) |
| `hold 10000` | 435-459k streams/s to open, 0.56 KB/stream, ~0 CPU while idle |

Before/after on the same machine: single-stream 37k -> 39k req/s, 1,000-way
392k -> 401-463k req/s, 1,000-way peak RSS ~60-70 MB (from 69 MB; the step
measurement after the poll change was 51 MB, and allocator behaviour varies
between runs), and stream-open rate 289k -> 435-459k/s.

The multi-loop run is limited by the in-tree test server above two workers:
it uses blocking sockets and two `send()` calls per response on a
per-connection thread, and the host has 4 performance + 6 efficiency cores.
Treat `requests_mt` as a client-scaling check, not a server benchmark; use an
external load peer for absolute multi-loop numbers.

### io_uring (0.3 baseline)

The 0.3 one-shot transport is still available as a fallback; the 0.4
multishot/provided-buffer path is described above.

### Stream pump scaling (`pump`)

`take_over` streams run one per pump worker, so the worker capacity is the
ceiling on concurrently running streams. Workers start at `min(capacity, CPUs)`
and grow on demand; the automatic capacity comes from the process thread, fd,
and address-space budgets rather than a core multiple (see `docs/server.md`).
This sweep uses a 1 s SSE-style response (10 chunks, 100 ms apart), all requests
released together, with an in-process single-threaded poll client on the same
M5 (10 cores). `REALURL_PUMP_THREADS` pins the capacity for the comparison.

| Capacity | Concurrency | Waves | Wall | TTFT p50 | TTFT p99 | Total p50 | Peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 40 | 40 | 1.0 | 1.06 s | 0.7 ms | 0.8 ms | 1.05 s | 3.5 MB |
| 40 | 128 | 3.2 | 4.14 s | 1.04 s | 3.09 s | 2.07 s | 4.0 MB |
| 40 | 512 | 12.8 | 13.51 s | 6.21 s | 12.44 s | 7.25 s | 6.1 MB |
| 64 | 512 | 8.0 | 8.35 s | 3.15 s | 7.29 s | 4.20 s | 6.4 MB |
| 128 | 512 | 4.0 | 4.22 s | 1.07 s | 3.12 s | 2.12 s | 7.7 MB |
| 256 | 512 | 2.0 | 2.13 s | 5.2 ms | 1.05 s | 1.06 s | 9.8 MB |

Reading:

- TTFT is quantized into waves: once concurrency exceeds the pool size, a
  stream waits `ceil(C / P) - 1` stream durations before it starts. Raising the
  pool divides that wait proportionally (12.8 -> 8 -> 4 -> 2 waves).
- CPU stays under 1 s for 512 streams because pump threads sleep between chunks
  or block on the client socket. On this host there is no CPU knee up to 256
  threads: the cost is one OS thread per concurrent stream (~14 KB touched per
  idle thread, plus stack address space), not scheduler throughput.
- The benchmark raises the queue limit to the concurrency so it measures
  queueing instead of load shedding. In production the automatic queue is the
  fd-budget remainder capped at capacity; once every worker is busy and the
  queue is full, new streams receive 503 with `Retry-After: 1` instead of
  waiting without bound. The 12 s tail above is therefore a deliberate
  worst-case measurement, not the steady-state overload behavior.

With no override the pool starts at `min(capacity, CPUs)` and grows on demand.
On the M5 the automatic capacity is 1333 (`RLIMIT_NPROC / 2`), so the
512-stream run grows to 512 workers and finishes one wave in 1.06 s
(TTFT p50 4.0 ms). In a 2-core / 2 GB container the automatic capacity is
15810 (`threads-max / 2`); 512 streams still complete in one wave (1.02 s,
0.15 s CPU), while pinning the old-style capacity of 16 takes 8 waves (8.02 s).
The ceiling follows the process budget, the actual worker count follows the
load; workers above the initial size are reclaimed after the idle timeout.

Reproduce:

```sh
./build/bench/realurl_bench pump 512 10 100 30000                       # automatic capacity
REALURL_PUMP_THREADS=128 ./build/bench/realurl_bench pump 512 10 100 30000  # pinned capacity
```

The poll client is single-threaded, so the RSS column reflects the server plus
sockets rather than one client thread per stream.

### Async takeover (`async`)

`take_over_async` runs one coroutine per stream on the io threads, with no pump
worker. Same 1 s SSE shape and single-threaded poll client:

| Host | Concurrency | Wall | TTFT p50 | TTFT p99 | CPU | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| M5, 2 io threads | 4096 | 1.09 s | 16.9 ms | 23.4 ms | 0.70 s | 28.6 MB |
| 2-core / 2 GB container | 4096 | 1.03 s | 12.6 ms | 24.1 ms | 0.44 s | 24.9 MB |
| 2-core / 2 GB container | 8192 | 1.05 s | 25.7 ms | 48.6 ms | 0.85 s | 45.3 MB |

For contrast, the blocking pump pinned to 40 workers and given 4096 streams
hits the 60 s client timeout with 1736 unfinished streams. Async removes the
thread-per-stream ceiling; concurrency is bounded by fds, memory, and the io
threads instead.

```sh
./build/bench/realurl_bench async 8192 10 100 60000
```

### Proxy bridge (`proxy`)

`proxy` puts the HTTP/1.1 server in front of the in-process h2c test server.
The handler either co_awaits `realurl::async_request()` (`bridge`) or calls
`realurl::sync_wait(group.request(...))` on the io thread (`block`). Both use
2 io threads, one `ClientGroup` worker, and `max_streams_per_conn` set to the
concurrency. `chunks`/`interval_ms` shape the upstream stream; the test server
sleeps in 20 ms steps, so 10 x 100 ms measures ~0.68 s.

M5 / macOS (10 cores):

| Mode | Concurrency | Upstream | Wall | TTFT p50 | CPU | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| bridge | 16 | 10 x 100 ms | 0.71 s | 678.6 ms | 0.02 s | 6.0 MB |
| block | 16 | 10 x 100 ms | 5.39 s | 2699.3 ms | 0.03 s | 5.5 MB |
| bridge | 2048 | 1 x 10 ms | 0.05 s | 38.1 ms | 0.20 s | 122.4 MB |
| block | 2048 | 1 x 10 ms | 26.43 s | 13267.6 ms | 5.05 s | 47.2 MB |
| bridge | 2048 | 10 x 100 ms | 0.77 s | 691.0 ms | 1.04 s | 121.9 MB |

2-core / 2 GB Linux container:

| Mode | Concurrency | Upstream | Wall | TTFT p50 | CPU | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| bridge | 2048 | 1 x 10 ms | 0.13 s | 64.1 ms | 0.14 s | 59.0 MB |
| block | 2048 | 1 x 10 ms | 43.50 s | 21850.7 ms | 2.73 s | 31.0 MB |
| bridge | 2048 | 10 x 100 ms | 0.69 s | 658.6 ms | 0.37 s | 60.1 MB |

Reading:

- Blocking on the io thread leaves only `io_threads` upstream requests in
  flight. Wall tracks `ceil(C / 2) x upstream_duration`: 16/2 x 0.68 s = 5.4 s
  and 2048/2 x 0.026 s = 26.5 s on macOS, 2048/2 x 0.043 s = 44 s in the
  container. Measured values match.
- The bridge finishes in about one upstream duration regardless of C:
  2048 concurrent ~0.68 s upstream streams complete in 0.69-0.77 s with zero
  failures on both hosts. One worker multiplexes all streams over one h2
  connection; the limit is fds, memory, and `max_streams_per_conn`.
- RSS is not a bridge-vs-block comparison: the test upstream spawns one thread
  per delayed response, so the bridge runs hold 2048 helper threads while the
  blocking runs hold two. CPU/RSS include that test server.
- The 2048-connection blocking runs also hit the server's 5 s read-header
  timeout because the connections queued behind two blocked io threads; the
  benchmark disables it. The same head-of-line timeout is a real production
  symptom of blocking an io thread.
- `sync_wait` used to poll for its full timeout after the awaited task
  completed, adding a 1 s tail to every serialized request. That is fixed;
  without the fix the blocking column would be 1 s per request higher.

Reproduce:

```sh
./build/bench/realurl_bench proxy 2048 1 10 120000 1 bridge
./build/bench/realurl_bench proxy 2048 1 10 120000 1 block
./build/bench/realurl_bench proxy 2048 10 100 120000 1 bridge
```

### Proxy LLM (`proxy_llm`)

`proxy_llm` sends a 2 MB POST body through the HTTP/1.1 proxy, forwards it to
the h2c test server, and waits for a 20 s SSE-style response (20 chunks,
100 ms apart). 100 concurrent, one ClientGroup worker, 2 io threads:

| Host | Wall | TTFT p50 | CPU | Peak RSS | Upload verified |
| --- | ---: | ---: | ---: | ---: | --- |
| M5 | 20.75 s | 20250.8 ms | 2.23 s | 228.2 MB | 200 MB / 200 MB |
| 2-core / 2 GB | 20.82 s | 20726.0 ms | 1.65 s | 425.5 MB | 200 MB / 200 MB |

Direct-client baseline from the `llm` section (100 concurrent, 20 s streaming,
2 MB uploads): total 20.45 s, TTFT p50 1138.8 ms, CPU 1.22 s, peak RSS
557.5 MB.

Reading:

- The buffered bridge preserves completion time and upload correctness, but
  TTFT becomes the whole upstream duration (~20 s): `async_request()` returns
  only after the response body is complete. It cannot be used for token-level
  SSE relaying.
- CPU grows by about 1 s per 100 requests (proxy hop plus 2 MB forwarding);
  the extra RSS is the in-flight request bodies.
- A streaming bridge (bounded chunk queue with backpressure) is required to
  restore the ~1 s TTFT. Until then, use the direct client for streaming
  responses.

Reproduce:

```sh
./build/bench/realurl_bench proxy_llm 100 20 2097152 1 bridge
```

## 0.3 baseline: `realurl_bench requests <N> <concurrency>`

20,000 `client.get()` requests over one multiplexed connection:

| Concurrency | Throughput | p50 | p90 | p95 | p99 | p99.9 | max | RSS | CPU/req |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 36,957 req/s | 0.026 ms | 0.029 ms | 0.031 ms | 0.079 ms | 0.109 ms | 0.652 ms | 13.7 MB | 0.019 ms |
| 100 | 371,743 req/s | 0.269 ms | 0.314 ms | 0.330 ms | 0.380 ms | 0.496 ms | 0.531 ms | 13.6 MB | 0.005 ms |
| 1,000 | 520,335 req/s | 1.804 ms | 2.125 ms | 2.343 ms | 5.403 ms | 6.780 ms | 6.910 ms | 56.1 MB | 0.004 ms |
| 10,000 | 276,546 req/s | 28.954 ms | 56.745 ms | 63.572 ms | 79.871 ms | 85.582 ms | 86.955 ms | 376.8 MB | 0.005 ms |

Reading:

- Single-stream loopback round trip is ~26 µs at the p50; a caller keeps a
  connection pinned at ~37k req/s per stream without any concurrency.
- At 1,000 concurrency, throughput breaks **520k req/s** on a single HTTP/2 connection
  on a single thread (up from ~300k req/s baseline, a +72% improvement), with p50 = 1.80 ms
  and p90 = 2.13 ms.
- At 10,000 concurrency, 10,000 in-flight requests on one connection complete 50,000 requests
  in 0.18s at **276k req/s**, maintaining 100% success rate with p50 = 28.95 ms and max = 86.96 ms.
- CPU per request stays flat at 4–5 µs across all concurrency tiers up to 10,000.
- RSS scales linearly with active in-flight streams (56 MB at 1,000 concurrency, 376 MB at 10,000 concurrency).

## Hold mode: `realurl_bench hold <streams>`

`N` bidirectional streams opened on one connection and left half-open
(HEADERS+END_STREAM sent, server never responds). Steady state is measured
over a 5 s idle window; client keepalive PING is 30 s, so this is pure
per-stream overhead.

| Streams | Open rate | RSS delta | Per stream | CPU during 5 s idle |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 110k streams/s | 0.9 MB | 0.96 KB | 0.002 s |
| 10,000 | 362k streams/s | 5.5 MB | 0.56 KB | 0.002 s |

Reading: a parked stream costs ~0.56 KB and effectively zero CPU. Opening
10,000 streams takes 0.03 s (362k streams/s); parked streams do not participate
in `write_queue_` polling, eliminating idle stream overhead completely.

The same run under AddressSanitizer is clean (11/11 tests, hold included);
ASan's own overhead inflates per-stream memory, so the table above is the
Release number.

## LLM large-request mode: `realurl_bench llm <total> <concurrency> [duration_s] [streaming]`

Simulates large prompt LLM requests: 2 MB request payload (~100k input tokens),
~1 KB response payload (~1k output tokens), holding/streaming for 20 seconds
over 1 multiplexed HTTP/2 connection on a single EventLoop thread.

| Concurrency | Requests | Success | Wall Time | Throughput | p50 Latency | p95 Latency | p99 Latency | Uploaded | Peak RSS | CPU/req |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1/1 (100%) | 20.03 s | 3.0 RPM | 20.03 s | 20.03 s | 20.03 s | 2 MB | 12.5 MB | 31.6 ms |
| 10 | 10 | 10/10 (100%) | 20.06 s | 29.9 RPM | 20.04 s | 20.06 s | 20.06 s | 20 MB | 46.1 MB | 17.8 ms |
| 50 | 50 | 50/50 (100%) | 20.12 s | 149.1 RPM | 20.06 s | 20.11 s | 20.11 s | 100 MB | 147.0 MB | 12.5 ms |
| 100 | 100 | 100/100 (100%) | 20.24 s | 296.5 RPM | 20.12 s | 20.21 s | 20.23 s | 200 MB | 271.1 MB | 12.2 ms |
| 200 | 200 | 200/200 (100%) | 20.45 s | 586.7 RPM | 20.26 s | 20.43 s | 20.44 s | 400 MB | 498.8 MB | 11.8 ms |
| 500 | 500 | 500/500 (100%) | 20.98 s | 1,429.7 RPM | 20.47 s | 20.92 s | 20.97 s | 1,000 MB | 902.7 MB | 11.2 ms |
| 1,000 | 1,000 | 1,000/1,000 (100%) | 21.88 s | 2,741.7 RPM | 20.88 s | 21.76 s | 21.86 s | 2,000 MB | 1,209.4 MB | 11.0 ms |

Streaming mode (100 concurrency, tokens streamed in 20 chunks over 20 s):
- 100/100 passed in 20.45 s (293.3 RPM)
- **TTFT (Time to First Token)**: p50 = 1138.8 ms, p90 = 1161.4 ms, p95 = 1163.7 ms, p99 = 1168.7 ms, max = 1169.1 ms
- **Total Duration**: p50 = 20.35 s, p90 = 20.41 s, p95 = 20.42 s, p99 = 20.43 s, max = 20.43 s
- **Fair Interleaving**: All 100 streams received their initial token within a narrow ~30 ms spread (1138 ms to 1169 ms), eliminating head-of-line starvation.
- Peak RSS = 557.5 MB, CPU = 1.22 s total (12.1 ms/req)

Reading:
- **Fair Round-Robin Data Interleaving**: Multi-stream sending uses deficit/round-robin interleaving where each active stream sends up to 1 DATA frame per cycle, preventing the first stream from monopolizing the connection send window and reducing TTFT variance to near zero.
- **Amortized Buffer Queue**: Send and receive paths use `ByteQueue`, one contiguous buffer with a read cursor. The consumed prefix is reclaimed when the queue drains, or compacted once it is at least as large as the live bytes, so no `std::string::erase(0, offset)` runs per read. (0.3 used chunked `std::deque<std::string>` queues; the per-stream deque block allocation they cost is gone, see "Two-end hot path".)
- **Zero failure rate up to 1,000 concurrent 2 MB streams**: Unlike the legacy 0.2 libcurl stack which hit `FD_SETSIZE` and collapsed (759/1,000 with 241 HTTP 502 errors under 20 s streams), realurl maintains 100% success rate at 2,741.7 RPM (1,000 concurrent 20 s streams).
- **Linear memory scaling**: In-flight RSS scales directly with the active payloads. At 1,000 concurrency, 2 GB of payload data was concurrently uploaded with a peak RSS of only ~1.2 GB, thanks to dynamic buffer compaction and flow control backpressure.
- **Low CPU footprint**: CPU per request stays flat at ~11–12 ms despite transferring 2 MB of data per request, with total CPU during the 20-second window consuming only ~11 seconds across 1,000 requests (~5% of overall capacity).

## Context: what the 0.2 libcurl stack measured

The 0.2 report (kept in git history) recorded the limits that motivated this
rewrite: system libcurl 8.7.1 capped at `FD_SETSIZE`, per-stream easy handles,
and thread-per-request scaling walls (759/1,000 at 1,000 RPM for 20 s
streams). Those numbers are obsolete as of 0.3; the library no longer links
libcurl.

## Design decisions that show up in the numbers

- Read path: DATA payload is copied once, directly into the suspended
  reader's buffer. No per-frame queue when the caller keeps up.
- Write path: active `write_queue_` containing only streams with pending writes,
  eliminating O(N) stream scans across idle streams waiting for server responses.
- Batching: double-buffered `out_buffer_` swap flushes up to 512 KB per cycle in a single socket write.
- Inline frame serialization: `append_frame_header` / `append_headers` write
  DATA, HEADERS and CONTINUATION frames straight into the connection's
  outbound buffer, without `FrameWriter` heap allocations.
- END_STREAM rides on the DATA frame that carries the last queued byte; an
  empty body ends on HEADERS. A unary response is two frames.
- No large buffers in hot-path coroutine frames: buffered bodies drain from the
  stream queue into the destination string, keeping frames in the pooled small
  size classes.
- Flow control: fair deficit/round-robin interleaving among active streams in `write_queue_`,
  with automatic suspension to `blocked_streams_` on window exhaustion and re-enqueue on WINDOW_UPDATE.
- Connection pooling: least-loaded connection selection using `max_conns_per_origin` and
  `max_streams_per_conn` with concurrency gates.
- `Stream` is a move-only handle over a connection-owned object; no
  `shared_ptr` traffic on request hot paths beyond the stream handle itself.
- HPACK encoding uses static-table indexes plus literal-without-indexing —
  no Huffman on the send path, full Huffman on the receive path.

## Measurement notes

- The in-tree test server is intentionally simple; it is a protocol peer, not
  a load engine. End-to-end proxy numbers depend on revlm's gateway paths and
  are out of scope here.
- TLS handshake and bulk transfer were validated functionally
  (`client_test` TLS case); a dedicated TLS throughput benchmark is still
  planned.

## Reproduce

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DREALURL_BUILD_BENCH=ON
cmake --build build
./build/bench/realurl_bench requests 20000 100
./build/bench/realurl_bench requests 20000 1000
./build/bench/realurl_bench requests_mt 200000 1000 2 1
./build/bench/realurl_bench hold 10000
./build/bench/realurl_bench llm 100 100 20
./build/bench/realurl_bench llm 1000 1000 20
```

## Known limitations

1. Timers still use a `std::function` priority queue with lazy cancellation;
   a long-lived process accumulates expired nodes until they reach the heap
   top, and every timed wait pays a function-object construction. An
   intrusive timer wheel is the next allocation win.
2. `writev` batching exists only implicitly through the writer coroutine; a
   user-provided `write_owned(std::string&&)` zero-copy path is reserved.
3. The io_uring transport submits one recv or send per `io_uring_enter`.
   Multishot recv with provided buffers and SQPOLL are the next steps for that
   backend.
4. `realurl::async_request()` buffers the upstream response before completing.
   A streaming bridge with a bounded chunk queue and backpressure is still
   missing, so token-level SSE relaying through the proxy keeps TTFT at the full
   upstream duration.
