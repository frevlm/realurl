# Performance report

This report is the evidence behind the design. It was produced while the code
lived in revlm; the component-only findings apply directly to realurl, while
the end-to-end numbers depend on revlm's gateway, database, and billing paths.

- Date: 2026-10-19
- Backend baseline: revlm `2da7483`
- Environment: Apple M5 / macOS, isolated MySQL, local mock upstream
- Load: fixed arrival rate (open loop), 1,000 RPM, 200 ms / 5 s / 20 s streams

## Results

| Configuration | 200 ms streams | 5 s streams | 20 s streams |
| --- | ---: | ---: | ---: |
| Default: 36 httplib threads + system curl | 1,000/1,000 | 540/1,000 | 72/1,000 |
| 512 threads + system curl | 1,000/1,000 | 1,000/1,000 | **759/1,000** (241×502) |
| 512 threads + Homebrew curl 8.22.0 | — | — | **1,000/1,000** |

### Interpretation

- The httplib thread pool caps at 36 threads by default. Sustainable rate is
  roughly `36 / stream_duration`: about 432 RPM for 5 s streams and 108 RPM for
  20 s streams.
- With 512 threads and the system curl, 20 s streams stall at about 254
  concurrent transfers. The 241 HTTP 502s are system libcurl 8.7.1 hitting
  `select()` / `FD_SETSIZE=1024`, not a thread, mock, or fd-count ceiling.
- With 512 threads and Homebrew curl 8.22.0 (poll-based), the run reaches
  1,000/1,000: peak 334 active streams, 383 threads, RSS growth 417 MiB, queue
  p95 6.3 ms, usage records 1,000/1,000, zero late starts.
- Beast `StreamPumpPool` hashes by remote IP, so long streams from one IP are
  serialized. On 200 ms streams at 1,000 RPM the Beast path achieved only
  137/1,000. This is a design property of per-IP affinity, independent of
  httplib.
- In the default-build overload rounds, the gateway started upstream requests
  after the client had disconnected: 244 late starts in the 5 s round and
  490–495 in the 20 s round. Usage records were 756/957 and 144/621, so the
  overload path can miss upstream usage.

## Component findings

### StreamResult lifetime

The `stream_abandon_probe` fixture sends response headers immediately and the
body 250 ms later. It destroys the `StreamResult` after headers without calling
`stream_close()`, then drives the same multi handle:

```text
ERROR: AddressSanitizer: heap-use-after-free
READ in stream_writefn (curl_multi_pool.cpp:263)
freed by CurlMultiPool::StreamResult::~StreamResult()
```

The control run with an explicit `stream_close()` passes. A production crash
with the same stack appeared once under 1,000 RPM / 20 s streams (SIGSEGV in
`stream_writefn` after about 72 s), but was not reproduced on two subsequent
runs. The lifetime defect is real even though the full production trigger chain
was not proven.

The probe is preserved in `probes/stream_abandon_probe.cpp`. Fixing it is out
of scope for 0.1.0.

### BodyFeeder / BodySource ownership

`component_probe` reports:

```text
{"probe":"body_ownership","feeder_expired":false,"source_expired":false}
```

`make_source()` captures a strong reference to the feeder, and the feeder keeps
a `shared_ptr` to the source, so the pair is never released. Under 1,000
consecutive 64 KiB requests, Beast RSS grew from 21.75 MiB to 105.72 MiB; the
httplib paths grew by about 2 MiB.

### Pump-pool concurrency

`component_probe` submits 8 tasks of 100 ms each:

- Same IP: peak active 1, elapsed about 800 ms (serialized by design).
- Distinct IPs: peak active equal to the worker count, elapsed about 100 ms.

This is the expected trade-off: per-IP affinity keeps upstream connection reuse
stable, at the cost of serializing one client's streams.

### Other reproduced issues

- Beast chunked uploads time out after 12 s; the upstream never receives the
  complete request. The httplib paths work.
- Large-body windows are effective: a 64 MiB request peaked at 660.6 MiB with
  the old path and 22.4 MiB with the Beast path. The problem is releasing the
  objects after the request, not the window itself.
- Empty-database schema: `requests.id` is `BIGINT NOT NULL` without
  `AUTO_INCREMENT` or a default, while `ensure_schema` marks migration 0011 as
  applied.
- `sql_exec` only checks the first result set, so errors in later statements are
  lost. `ScopedTransaction` does not reuse the current ODB connection, so
  leaving the scope does not commit the row.

The database findings belong to revlm, not realurl, and are recorded here only
because they were part of the same measurement round.

## Reproduce

The realurl probes build with:

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DREALURL_BUILD_PROBES=ON
cmake --build build
./build/probes/realurl_probe_component
```

For the lifetime defect under AddressSanitizer:

```sh
cmake -S . -B build-asan -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DREALURL_BUILD_PROBES=ON \
  -DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer"
cmake --build build-asan
./build-asan/probes/realurl_probe_stream_abandon
```

The end-to-end rate test (`scripts/perf/rate_load.py`) stays in revlm because it
starts the gateway binary and verifies MySQL rows.

## Next steps

The original report's priority list still applies:

1. Depend on a poll-based libcurl (8.4+) instead of relying on `select()`.
2. Make stream lifetime RAII-scoped so abandoning a `StreamResult` closes it.
3. Add capacity rejection instead of accepting work the process cannot finish.
4. Only then consider a multi-handle event loop. Adding threads or swapping
   curl alone does not address the queueing and ownership problems.
