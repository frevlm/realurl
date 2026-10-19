#pragma once

/// CurlMultiPool — per-thread libcurl multi-handle HTTP client.
///
/// Replaces internal httplib::Client usage for upstream proxying.
/// - One CURLM* per instance, intended for single-thread use.
/// - Streaming (execute_stream) drives the transfer on the CALLING thread's
///   multi handle — no per-stream worker thread, no per-stream curl_multi.
///   Connection reuse therefore works: the multi handle caches upstream
///   connections across the streams it processes.
/// - Forced HTTP/2 (CURL_HTTP_VERSION_2) to avoid h3 negotiation.
/// - Four-segment timeout: connect, upload-stall (LOW_SPEED), response-header
///   wait, inter-event idle (enforced by the caller via stream_read's timeout).
/// - Sliding-window upload via CURLOPT_READFUNCTION + BodySource with
///   CURL_READFUNC_PAUSE backpressure.

#include <curl/curl.h>

#include "realurl/body_source.hpp"
#include "realurl/rate_limit.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <vector>

namespace realurl
{

struct CurlHeader {
    std::string name;
    std::string value;
};

/// Describes one upstream HTTP request.  The pool never copies or retains
/// the request — callbacks (body_source, on_response_chunk, …) must remain
/// valid for the lifetime of execute() / execute_stream().
struct CurlRequest {
    /// Full URL including scheme, host, port, path, and query.
    std::string url;

    /// HTTP method (default "POST").
    std::string method = "POST";

    /// Request headers.  "Host", "Content-Length", and "Connection" are
    /// stripped automatically by the pool.
    std::vector<CurlHeader> headers;

    /// First chunk of the request body, sent before the body source is ever
    /// consulted.  For non-streaming requests this is typically the entire
    /// body.  When a body_source is present, curl falls back to it after this
    /// chunk is exhausted.
    std::string initial_body_chunk;

    /// Declared request body size in bytes, or -1 when unknown.  When >= 0,
    /// curl sends a real Content-Length (instead of chunked framing) while
    /// still streaming the bytes from initial_body_chunk / body_source.
    long long content_length = -1;

    // ---- body production (sliding window) ----

    /// Sliding-window body source.  When set, the READFUNCTION pulls from it
    /// (with CURL_READFUNC_PAUSE when it reports {wait:true}); the producer
    /// wakes the transfer via body_source->wake.  When null, the request body
    /// is exactly initial_body_chunk.
    std::shared_ptr<BodySource> body_source;

    // ---- response consumption ----

    /// Called for each response body chunk.  Return false to abort the
    /// transfer early (curl will see a write error).
    std::function<bool(std::string_view)> on_response_chunk;

    /// Called once when the full set of response headers has been received,
    /// before the first body chunk.
    std::function<void(int status_code, const std::vector<CurlHeader> &headers)> on_headers;

    // ---- timeouts (four segments) ----

    /// TCP / TLS handshake timeout (seconds).  Default 5 s.
    long connect_timeout_s = 5;

    /// Upload stall detection: if the average upload speed drops below this
    /// many bytes per second for upload_low_speed_time seconds the transfer
    /// is aborted.  Default 10 KB/s.
    long upload_low_speed_bytes = 10240;

    /// Upload stall window (seconds).  Default 30 s.
    long upload_low_speed_time = 30;

    /// How long execute_stream waits for the response headers (seconds).
    /// Default 120 s.
    long header_timeout_s = 120;

    /// Hard total-transfer timeout including connect, upload, and download
    /// (seconds).  0 = disabled (the default — big bodies may legitimately
    /// take minutes as long as they are making progress).
    long total_timeout_s = 0;

    /// CURLOPT_RESOLVE pin string: "host:port:address".
    /// When non-empty, curl resolves the named host:port to the given
    /// address, eliminating the DNS rebinding window between SSRF
    /// validation and upstream connection.
    std::string dns_pin;

    // ---- upload rate limiting (design doc: two-pool-bandwidth-allocation) ----

    /// Rate-limiter flow handle.  When set, the pool applies the flow's
    /// desired cap to this easy handle (CURLOPT_MAX_SEND_SPEED_LARGE) at
    /// setup and re-applies it whenever the pump loop notices a change.
    /// All cap writes happen on the stream's owning thread.
    std::shared_ptr<UploadRateFlow> rate_flow;
};

/// Result of a completed (non-streaming) request.
struct CurlResponse {
    int status_code = 0;
    std::vector<CurlHeader> headers;
    std::string body;
};

/// Per-thread libcurl multi-handle pool.
///
/// Usage:
///   CurlMultiPool pool;
///   CurlResponse r = pool.execute(req);            // blocks on this thread
///   auto s = pool.execute_stream(req);             // drives until headers
///   ssize_t n;
///   while ((n = s.stream_read(buf, sz, idle_ms)) > 0) {}   // consume
///   s.stream_close();                              // remove + cleanup handle
///
/// execute_stream drives the transfer on the calling thread (the thread that
/// owns this pool's multi handle).  The caller must keep calling stream_read
/// until it returns 0 (clean EOF) or < 0 (error; errno == ETIMEDOUT is an
/// inter-event idle timeout, not a transport failure).
class CurlMultiPool {
public:
    CurlMultiPool();
    ~CurlMultiPool();

    CurlMultiPool(const CurlMultiPool &) = delete;
    CurlMultiPool &operator=(const CurlMultiPool &) = delete;

    // ---- non-streaming ----

    /// Execute a request and block until the full response is received.
    /// Throws std::runtime_error on transport / protocol error.
    CurlResponse execute(const CurlRequest &request);

    // ---- streaming ----

    struct StreamResult {
        /// HTTP status code.
        int status_code = 0;

        /// Response headers.
        std::vector<CurlHeader> headers;

        /// Any body bytes that arrived before headers were complete.
        std::string initial_body;

        /// Drive the transfer and read the next chunk of the response body.
        /// Returns:
        ///   > 0  bytes read into buffer
        ///   0    transfer finished cleanly (EOF)
        ///   < 0  error; errno is set.  ETIMEDOUT means no response bytes
        ///        arrived within idle_timeout_ms (inter-event idle timeout) —
        ///        NOT a transport failure.
        /// Blocks on the calling thread, driving curl_multi until data or
        /// EOF.  Must be called from the same thread that called
        /// execute_stream (the multi handle is not thread-safe).
        std::function<ssize_t(char *, size_t, int idle_timeout_ms)> stream_read;

        /// True once the underlying transfer hit a transport error.
        std::function<bool()> stream_error;

        /// Transport error description (empty when no error).
        std::function<std::string()> stream_error_message;

        /// Close the stream: removes the easy handle from the multi and
        /// cleans it up.  Must be called on the owning thread.  Safe to call
        /// multiple times; no-op after first.
        std::function<void()> stream_close;
    };

    /// Launch a streaming request and block until response headers arrive
    /// (or the header timeout elapses).  Throws std::runtime_error on
    /// connect / header timeout / transport error.
    StreamResult execute_stream(const CurlRequest &request);

    /// Drive the pool's internal multi handle for one tick.  Returns the
    /// number of still-running transfers.
    int perform();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace realurl
