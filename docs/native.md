# Native transport (realurl/1)

`realurl/1` is a wire protocol used when both ends of a connection are
realurl. It keeps the HTTP/2 frame header, stream state machine, flow control,
writer scheduling and transport layer, and replaces HPACK with a compact
binary head codec. The handler surface stays exactly the same: `Router`,
`Request`, `Response`, `take_over_task()`, `proxy_stream()` and the body modes
do not change.

It is not a replacement for HTTP/2. A realurl client still talks to ordinary
HTTP/2 servers, and a realurl server still serves ordinary HTTP/1.1 and HTTP/2
clients. The native protocol is chosen only when both peers support it.

## Negotiation

### TLS

The client offers two ALPN protocols, in this order:

```
realurl/1
h2
```

The server prefers `realurl/1`, then `h2`, then `http/1.1`. The selected ALPN
is the connection mode; no extra round trip is involved. A realurl client that
offers `realurl/1` to a non-realurl TLS server still gets `h2` if the server
supports it.

### Plaintext

The client sends this 11-byte preamble before SETTINGS:

```
REALURL/1\r\n
```

The server reads 11 bytes and dispatches:

- exact `REALURL/1\r\n` and native is enabled -> native;
- a prefix of `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` -> read the remaining bytes and
  use HTTP/2;
- anything else -> HTTP/1.1, with the bytes already read preserved.

A native server sends `NATIVE_HELLO` as its first frame, before SETTINGS. A
plaintext client using `Protocol::Auto` reads and validates that frame. An
ordinary h2c server sends SETTINGS first, so the probe fails cleanly and the
client reconnects with the HTTP/2 preface. The result is cached per origin
(`scheme://authority`); the failed probe happens at most once per origin.

### DATA compression

Native SETTINGS may carry one extra entry:

| Setting | Id | Value |
| --- | ---: | --- |
| `NATIVE_COMPRESSION` | 0x10 | bitmask of DATA codecs the sender can decode; bit0 = zstd |

A native peer sends it when built with zstd and compression is enabled.
Compression is off by default on both sides; opt in with
`Config::native_compression = true` and `Server::set_native_compression(true)`.
A peer
compresses outgoing DATA only after it has received the other side's SETTINGS
with bit0 set, so DATA sent before that arrives is plain. Peers that never
send the entry (older realurl, or builds without zstd) ignore it as an unknown
setting and only ever see plain DATA. The entry is never sent in HTTP/2 mode.

## Frames

The 9-byte HTTP/2 frame header is reused unchanged:

```
24-bit length | 8-bit type | 8-bit flags | 31-bit stream id
```

| Type | Value | Use |
| --- | ---: | --- |
| DATA | 0 | Request/response body |
| HEADERS | 1 | Native head block |
| RST_STREAM | 3 | Stream abort |
| SETTINGS | 4 | Same 6-byte setting entries as HTTP/2 |
| PING | 6 | Keepalive |
| GOAWAY | 7 | Connection shutdown |
| WINDOW_UPDATE | 8 | Connection/stream flow control |
| CONTINUATION | 9 | Head block larger than `max_frame_size` |
| NATIVE_HELLO | 0x10 | Server's first frame; version check |

`NATIVE_HELLO` payload is 12 bytes:

```
u8[8] magic = "REALURL1"
u8    major = 1
u8    minor = 0
u16   capabilities = 0   (big-endian, reserved)
```

Native mode does not use the `PADDED` or `PRIORITY` flags; native peers never
set them. `ENABLE_PUSH` is not sent in native SETTINGS.

DATA frames have one native-only flag:

| Flag | Value | Meaning |
| --- | ---: | --- |
| END_STREAM | 0x1 | unchanged |
| COMPRESSED | 0x20 | payload is one complete, standalone zstd frame |

Each compressed frame is independent: no dictionary or window is shared
across frames, and the zstd frame header must declare the content size. The
sender compresses a payload only when it is at least 1024 bytes, the stream's
head did not carry `content-encoding`, and the result is smaller than the
input; otherwise the frame goes out plain. Compression runs on chunks of at
most `max_frame_size` input bytes (the receiver's advertised
`SETTINGS_MAX_FRAME_SIZE`), so a compressed frame always fits that frame size
and decompresses to at most that many bytes. Flow control and `max_frame_size` count wire bytes, the
frame length after compression, exactly as HTTP/2 counts bytes on the wire.

A receiver that did not advertise zstd treats the flag as a connection error
(`GOAWAY PROTOCOL_ERROR`). An HTTP/2 connection ignores it, like any flag that
is undefined for the frame type.

## Head block

All integers are unsigned LEB128. All strings are length-prefixed raw bytes.
Decoded views point into the head block, which the receiver owns until the
handler or response completes.

### Request (HEADERS payload)

```
u8     tag = 0x01
u8     method_code            // 0..8, or 0xff for a literal
       [varint len + bytes]   // literal method only
varint target_len + target bytes
u8     flags                  // bit0: content_length present
       [varint content_length]
varint header_count
repeat:
  u8     name_code            // 0..N-1, or 0xff for a literal
         [varint len + name]  // literal name only
  varint value_len + value bytes
```

### Trailers (a second HEADERS on the same stream)

```
u8 tag = 0x02
varint header_count
repeat: same name/value encoding
```

Request trailers are validated and ignored, matching the current HTTP/2
behavior. Response trailers are not emitted.

### Response

```
u8     tag = 0x11
varint status                 // 100..599
u8     flags                  // bit0: content_length present
       [varint content_length]
varint header_count
repeat: same name/value encoding
```

When the sender knows the body length, `content-length` is carried in the
flags/varint field rather than as a header. The receiving side maps it to
`Request::content_length` / `ResponseHead::content_length` and synthesizes a
`content-length` header view for API compatibility. Callers that do not know
the length can still send a regular `content-length` header.

### Static tables

Methods (case-sensitive):

```
GET POST PUT DELETE PATCH HEAD OPTIONS CONNECT TRACE
```

Header names are looked up case-insensitively in this sorted table; any other
name is sent literally:

```
accept, accept-encoding, accept-language, authorization, cache-control,
connection, content-encoding, content-length, content-type, cookie, date, etag,
expect, host, if-modified-since, if-none-match, location, origin, pragma,
range, referer, server, set-cookie, te, traceparent, tracestate, trailer,
transfer-encoding, upgrade, user-agent, vary, via, x-forwarded-for,
x-forwarded-host, x-forwarded-proto, x-request-id
```

There is no HPACK dynamic table and no Huffman coding. The existing HTTP/2
encoder also uses static indexes plus literal-without-indexing, so this does
not lose the current compression behavior.

## Semantics

| HTTP/2 | Native |
| --- | --- |
| `:method` | `method_code` / literal |
| `:path` | `target` |
| `:scheme`, `:authority` | not transmitted; `host` is a regular header if set |
| `:status` | varint status |
| `content-length` header | flags + varint field |
| `Request::version` | `"realurl/1"` |
| `ResponseHead.protocol` | `Protocol::Native` |
| Body modes, `StreamWriter`, `BodyReader` | unchanged |
| 100-continue, push, priority, response trailers | not supported (same as the H2 server) |

`Method`, path, header names/values and content lengths are validated. On
requests, header names beginning with `:` and hop-by-hop headers are skipped;
on responses, hop-by-hop and `content-length` headers are skipped and the
length travels in the flags/varint field instead.

## Limits

| Limit | Value |
| --- | --- |
| Head block | 1 MiB (server `set_header_limit()` can lower it); client 1 MiB |
| Header count | 256 |
| Method / target / header bytes | no CR, LF or NUL; target starts with `/` or `*` |
| Status | 100..599 |
| Varint | unsigned LEB128, max 10 bytes, overflow rejected; lengths/counts are also bounded by the limits above |
| `content-length` | must fit `uint64`; the server also enforces `set_payload_max_length()` before reading the body |
| Compressed DATA frame | one zstd frame whose declared content size is at most the receiver's advertised `SETTINGS_MAX_FRAME_SIZE`; no trailing bytes |

A malformed block resets the stream; an oversized head block closes the
connection with `GOAWAY`. The connection survives a stream-level parse error.
A compressed DATA frame that does not decode, has no content size, or
declares more than the receiver's advertised `SETTINGS_MAX_FRAME_SIZE` resets
its stream with `PROTOCOL_ERROR`; its bytes still count against the
connection window, and the connection keeps serving. A legitimate sender never
exceeds that size, so one compressed frame never inflates beyond one frame's
worth of plain DATA.

A receiver checks the zstd frame header when the frame arrives and keeps the
payload compressed until the application reads it, so the DATA a stream
buffers is bounded by its flow-control window in wire bytes, plus at most one
decoded frame. A payload whose header checks out but whose body does not
decode resets its stream when it is read.

## API

```cpp
realurl::Config config;
config.protocol = realurl::Protocol::Auto;   // default
// Protocol::Http2  -> never offer realurl/1
// Protocol::Native -> require realurl/1
config.native_compression = true;            // opt in: zstd DATA when the server opted in too
```

`ResponseHead` now reports the negotiated transport and the body length:

```cpp
realurl::Response response = co_await client.get("http://127.0.0.1:8080/");
if (response.head.protocol == realurl::Protocol::Native) {
    // native fast path
}
if (response.head.content_length) {
    response.body.reserve(*response.head.content_length);
}
```

Server-side:

```cpp
realurl::Server server;
server.set_native(false);   // opt out of ALPN realurl/1 and the native preface
server.set_native_compression(true);    // opt in to zstd DATA (default off)
```

`Server::Stats` counts connections per protocol:

```cpp
auto stats = server.stats();
stats.h1_connections;
stats.h2_connections;
stats.native_connections;
```

The upstream client used by `proxy_stream()` takes the same `Config`, so a
realurl gateway can use the native transport on both hops:

```cpp
realurl::Config upstream;
upstream.protocol = realurl::Protocol::Auto;
server.set_upstream(upstream);
```

## Operational notes

- Native is enabled by default. It is a distinct protocol, not HTTP; L7
  middleboxes and WAFs will not understand it. Disable it with
  `set_native(false)` where that matters. TLS clients that do not offer
  `realurl/1` are unaffected by the ALPN preference.
- A plaintext `Protocol::Auto` client probes native first. A non-realurl peer
  costs one failed connection on first contact, cached per origin; set
  `Protocol::Http2` to skip the probe entirely.
- `set_native(false)` and `set_native_compression()` must be called before
  `listen()`.
- DATA compression is built when CMake finds zstd (`REALURL_WITH_ZSTD`,
  default `ON`) and is off at runtime until both sides opt in. It trades CPU
  for bandwidth: it wins only when the link is slower than about 5 Gbit/s per
  server core (JSON, level 1); on loopback and fast LANs it lowers throughput
  (see `docs/performance.md`). Leave it off where CPU is the bottleneck or
  bodies are already compressed without a `content-encoding` header.
- The native transport sends `Request::version = "realurl/1"`; handlers that
  switch on the version string should account for it.

## Measurements

See `docs/performance.md` for the A/B numbers against HTTP/2 over the same
realurl server, including 1,000-way unary, single-stream unary, 2 MiB uploads
and 20 s streaming responses.
