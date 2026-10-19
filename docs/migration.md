# Migration from revlm

realurl 0.1.0 is the networking layer of revlm branch
`feat/native-streaming-v1`, moved without behavioral changes. This document is
the mapping used by the extraction and by revlm's switch to the library.

## Moved

| revlm | realurl |
| --- | --- |
| `backend/include/streaming/body_source.hpp` | `include/realurl/body_source.hpp` |
| `backend/include/streaming/buffer_pool.hpp` | `include/realurl/buffer_pool.hpp` |
| `backend/src/streaming/buffer_pool.cpp` | `src/buffer_pool.cpp` |
| `backend/include/streaming/body_feeder.hpp` | `include/realurl/body_feeder.hpp` |
| `backend/include/streaming/curl_multi_pool.hpp` | `include/realurl/curl_multi_pool.hpp` |
| `backend/src/streaming/curl_multi_pool.cpp` | `src/curl_multi_pool.cpp` |
| `backend/include/streaming/rate_limit.hpp` | `include/realurl/rate_limit.hpp` |
| `backend/src/streaming/rate_limit.cpp` | `src/rate_limit.cpp` |
| `backend/include/streaming/response_sink.hpp` | `include/realurl/response_sink.hpp` |
| `backend/include/streaming/beast_response_sink.hpp` | `include/realurl/beast_response_sink.hpp` |
| `backend/include/streaming/httplib_response_sink.hpp` | `include/realurl/httplib_response_sink.hpp` |
| `backend/include/streaming/stream_pump_pool.hpp` | `include/realurl/stream_pump_pool.hpp` |
| `backend/src/streaming/stream_pump_pool.cpp` | `src/stream_pump_pool.cpp` |
| `backend/src/proxy/gateway.cpp`: `send_all_fd`, `client_writer_from_fd` | `include/realurl/fd_writer.hpp`, `src/fd_writer.cpp` |
| `scripts/perf/stream_abandon_probe.cpp` | `probes/stream_abandon_probe.cpp` |
| `scripts/perf/component_probe.cpp` | `probes/component_probe.cpp` |

## Deliberately not moved

| revlm | Why |
| --- | --- |
| `backend/include/streaming/transport.hpp` | The public API takes `ProxyRequest` and `GatewayStreamKind`; it is a gateway facade, not a transport primitive. |
| `backend/src/streaming/curl_window_transport.cpp` | Wires the gateway, billing callback, and global config. |
| `backend/src/streaming/beast_server.cpp` | Contains v1 routing, authentication, quota checks, and usage commit. |
| `backend/include/streaming/beast_proxy_dispatch.hpp` | Converts Beast requests into revlm's `ProxyRequest`. |
| SSRF validation, channel selection, pricing, SSE parsing | Business policy. |
| `scripts/perf/rate_load.py` | Starts the revlm binary and verifies MySQL rows. |

## Dependency boundary changes

Two changes were unavoidable to make the code independent:

1. Namespace: `revlm` → `realurl`.
2. `client_writer_from_fd` now takes an explicit timeout:

   ```cpp
   // realurl
   ClientWriter client_writer_from_fd(int fd, std::chrono::milliseconds timeout,
                                      std::shared_ptr<std::atomic_bool> response_started = {});
   ```

   revlm keeps a thin wrapper that reads `config().proxy_client_write_timeout_seconds`
   and forwards to the library, so call sites and behavior stay the same.

## How revlm consumes realurl

The root `CMakeLists.txt` sets `REALURL_SOURCE_DIR` to a local checkout, or falls
back to `FetchContent` from `https://github.com/frevlm/realurl` at tag `v0.1.0`.
`backend/CMakeLists.txt` links:

```cmake
realurl::core
realurl::beast
realurl::httplib
```

The duplicated headers and sources listed under "Moved" are deleted from
`backend/`; includes change from `streaming/<name>.hpp` to
`realurl/<name>.hpp`, and symbols are qualified with `realurl::`.
