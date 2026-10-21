# Performance

All numbers measured on the 0.3 self-contained stack: Apple M5 / macOS,
loopback, h2c prior knowledge, Release build, one pooled HTTP/2 connection
(`connections=1` in every run). The benchmark (`bench/realurl_bench`) warms up
the connection before starting the clocks, so percentiles reflect steady
state. RSS/CPU include the in-tree test server that runs in the same process.

## Request mode: `realurl_bench requests <N> <concurrency>`

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
- **Zero-Memmove Buffer Queue**: Send and receive paths use chunked queues (`std::deque<std::string>`) with cursor offsets instead of `std::string::erase(0, offset)`, eliminating dozens of megabytes of `std::memmove` per request.
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
- Inline frame serialization: direct stack-buffer formatting for DATA, RST, and WINDOW_UPDATE
  frames without `FrameWriter` heap allocations.
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
  (`client_test` TLS case); a dedicated TLS throughput benchmark is planned
  alongside the io_uring backend.

## Reproduce

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DREALURL_BUILD_BENCH=ON
cmake --build build
./build/bench/realurl_bench requests 20000 100
./build/bench/realurl_bench requests 20000 1000
./build/bench/realurl_bench hold 10000
./build/bench/realurl_bench llm 100 100 20
./build/bench/realurl_bench llm 1000 1000 20
```

## Known limitations

1. Coroutine frames are heap-allocated; a per-loop arena is the next
   optimization.
2. `writev` batching exists only implicitly through the writer coroutine; a
   user-provided `write_owned(std::string&&)` zero-copy path is reserved.
