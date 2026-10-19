#include "realurl/curl_multi_pool.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <utility>

namespace realurl
{
namespace
{

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

constexpr char kEmptyAcceptEncoding[] = "";

void strip_crlf(std::string_view &line)
{
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
        line.remove_suffix(1);
}

int parse_status_code(std::string_view line)
{
    // skip "HTTP/x.y " or "HTTP/2 "
    const char *p = line.data();
    const char *end = p + line.size();
    while (p < end && *p != ' ')
        ++p;
    while (p < end && *p == ' ')
        ++p;
    int code = 0;
    while (p < end && *p >= '0' && *p <= '9') {
        code = code * 10 + static_cast<int>(*p - '0');
        ++p;
    }
    return code > 0 ? code : 0;
}

std::pair<std::string, std::string> parse_header_line(std::string_view line)
{
    const auto colon = line.find(':');
    if (colon == std::string_view::npos)
        return {};
    std::string name{ line.substr(0, colon) };
    std::string_view raw_value = line.substr(colon + 1);
    const auto start = raw_value.find_first_not_of(" \t");
    std::string value = (start != std::string_view::npos) ? std::string{ raw_value.substr(start) } : std::string{};
    return { std::move(name), std::move(value) };
}

void tick_rate(CURL *easy, const std::shared_ptr<UploadRateFlow> &flow, long long &applied_cap_bps)
{
    if (!flow)
        return;

    curl_off_t uploaded = 0;
    curl_easy_getinfo(easy, CURLINFO_SIZE_UPLOAD_T, &uploaded);
    upload_rate_limiter().tick(flow, static_cast<long long>(uploaded));
    const long long cap = flow->desired_cap_bps();
    if (cap > 0 && cap != applied_cap_bps) {
        curl_easy_setopt(easy, CURLOPT_MAX_SEND_SPEED_LARGE, static_cast<curl_off_t>(cap));
        applied_cap_bps = cap;
    }
}

struct RateFlowGuard {
    std::shared_ptr<UploadRateFlow> flow;

    void release()
    {
        flow.reset();
    }

    ~RateFlowGuard()
    {
        if (flow)
            upload_rate_limiter().finish(flow);
    }
};

/// Guards the multi wake-up state the body producer may call concurrently.
/// The producer only calls curl_multi_wakeup; easy-handle operations stay on
/// the owner thread and detach is serialized with wake().
struct StreamWakeState {
    std::mutex mu;
    CURL *easy = nullptr;
    CURLM *multi = nullptr;
    bool resume_requested = false;

    void wake()
    {
        std::lock_guard<std::mutex> lock(mu);
        if (!multi)
            return;
        // The producer thread may wake the multi handle, but only the
        // owner thread may touch the easy handle.  resume_if_requested()
        // performs CURLPAUSE_CONT on the owner thread before perform().
        resume_requested = true;
        curl_multi_wakeup(multi);
    }

    void resume_if_requested(CURL *owner_easy)
    {
        bool resume = false;
        {
            std::lock_guard<std::mutex> lock(mu);
            if (easy == owner_easy && resume_requested) {
                resume_requested = false;
                resume = true;
            }
        }
        if (resume)
            curl_easy_pause(owner_easy, CURLPAUSE_CONT);
    }

    void detach()
    {
        std::lock_guard<std::mutex> lock(mu);
        easy = nullptr;
        multi = nullptr;
        resume_requested = false;
    }
};

// ---------------------------------------------------------------------------
// non-streaming request context
// ---------------------------------------------------------------------------

struct RequestContext {
    CurlRequest request;
    CurlResponse response;

    std::shared_ptr<StreamWakeState> wake_state;
    size_t initial_offset = 0; // read cursor within initial_body_chunk
    long long applied_cap_bps = 0;
};

size_t readfn(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *ctx = static_cast<RequestContext *>(userdata);
    const size_t max_bytes = size * nmemb;
    if (max_bytes == 0)
        return 0;

    // 1. initial_body_chunk.
    if (ctx->initial_offset < ctx->request.initial_body_chunk.size()) {
        const size_t n = std::min(ctx->request.initial_body_chunk.size() - ctx->initial_offset, max_bytes);
        std::memcpy(ptr, ctx->request.initial_body_chunk.data() + ctx->initial_offset, n);
        ctx->initial_offset += n;
        return n;
    }

    // 2. sliding-window body source.
    if (ctx->request.body_source) {
        BodyReadResult r = ctx->request.body_source->read();
        if (r.wait)
            return CURL_READFUNC_PAUSE;
        if (r.eof)
            return 0;
        const size_t n = std::min(r.chunk.size(), max_bytes);
        std::memcpy(ptr, r.chunk.data(), n);
        ctx->request.body_source->consume(n);
        return n;
    }

    return 0; // EOF
}

size_t writefn(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *ctx = static_cast<RequestContext *>(userdata);
    const size_t n = size * nmemb;
    if (n == 0)
        return 0;

    if (ctx->request.on_response_chunk) {
        if (!ctx->request.on_response_chunk(std::string_view{ ptr, n }))
            return 0; // signals error to curl
    }

    ctx->response.body.append(ptr, n);
    return n;
}

size_t headerfn(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *ctx = static_cast<RequestContext *>(userdata);
    const size_t n = size * nmemb;
    std::string_view line{ ptr, n };
    strip_crlf(line);

    if (line.empty()) {
        if (ctx->request.on_headers && ctx->response.status_code > 0) {
            ctx->request.on_headers(ctx->response.status_code, ctx->response.headers);
        }
        return n;
    }

    if (ctx->response.status_code == 0) {
        const int code = parse_status_code(line);
        if (code > 0) {
            ctx->response.status_code = code;
            return n;
        }
    }

    auto [name, value] = parse_header_line(line);
    if (!name.empty())
        ctx->response.headers.push_back({ std::move(name), std::move(value) });

    return n;
}

// ---------------------------------------------------------------------------
// streaming state — driven on the owning thread; no worker thread
// ---------------------------------------------------------------------------

struct StreamState {
    CurlRequest request;

    CURL *easy = nullptr;
    CURLM *multi = nullptr;
    std::shared_ptr<StreamWakeState> wake_state = std::make_shared<StreamWakeState>();
    int running = 0;

    // response
    int status_code = 0;
    std::vector<CurlHeader> headers;
    std::string pending; // response bytes not yet consumed by the pump
    bool headers_ready = false;

    // body upload
    size_t initial_offset = 0;
    bool paused_on_body = false; // READFUNCTION returned PAUSE
    bool body_sent = false; // full request body handed to curl

    // lifecycle
    bool done = false;
    bool error = false;
    bool closed = false;
    std::string error_msg;
    std::chrono::steady_clock::time_point paused_since{}; // body producer wait start

    // last cap applied to the easy handle (rate limiting); 0 = none yet
    long long applied_cap_bps = 0;
};

size_t stream_writefn(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *state = static_cast<StreamState *>(userdata);
    const size_t n = size * nmemb;
    if (n == 0)
        return 0;
    state->pending.append(ptr, n);
    return n;
}

size_t stream_headerfn(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *state = static_cast<StreamState *>(userdata);
    const size_t n = size * nmemb;
    std::string_view line{ ptr, n };
    strip_crlf(line);

    if (line.empty()) {
        // Only treat the blank line as end-of-headers once a REAL (non-1xx)
        // status has been seen.  Interim responses (e.g. "100 Continue"
        // before the final response) must not release the pump early.
        if (state->status_code != 0)
            state->headers_ready = true;
        return n;
    }

    if (state->status_code == 0) {
        const int code = parse_status_code(line);
        if (code > 0) {
            if (code >= 100 && code < 200)
                return n; // interim response — skip entirely
            state->status_code = code;
            return n;
        }
    }

    auto [name, value] = parse_header_line(line);
    if (!name.empty())
        state->headers.push_back({ std::move(name), std::move(value) });

    return n;
}

size_t stream_readfn(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *state = static_cast<StreamState *>(userdata);
    const size_t max_bytes = size * nmemb;
    if (max_bytes == 0)
        return 0;

    // 1. initial_body_chunk (before the sliding-window source).
    if (state->initial_offset < state->request.initial_body_chunk.size()) {
        const size_t n = std::min(state->request.initial_body_chunk.size() - state->initial_offset, max_bytes);
        std::memcpy(ptr, state->request.initial_body_chunk.data() + state->initial_offset, n);
        state->initial_offset += n;
        return n;
    }

    // 2. sliding-window body source.
    if (state->request.body_source) {
        BodyReadResult r = state->request.body_source->read();
        if (r.wait) {
            state->paused_on_body = true;
            return CURL_READFUNC_PAUSE;
        }
        if (r.eof) {
            state->paused_on_body = false;
            state->paused_since = {};
            state->body_sent = true;
            return 0; // EOF
        }
        const size_t n = std::min(r.chunk.size(), max_bytes);
        std::memcpy(ptr, r.chunk.data(), n);
        state->request.body_source->consume(n);
        state->paused_on_body = false;
        state->paused_since = {};
        return n;
    }

    state->body_sent = true;
    return 0; // EOF
}

void drain_messages(StreamState *state)
{
    int msgs_left = 0;
    CURLMsg *msg = nullptr;
    while ((msg = curl_multi_info_read(state->multi, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE && msg->data.result != CURLE_OK) {
            state->error = true;
            if (state->error_msg.empty())
                state->error_msg = std::string{ curl_easy_strerror(msg->data.result) };
        }
    }
}

void perform_once(StreamState *state)
{
    if (state->closed) {
        state->done = true;
        return;
    }
    state->wake_state->resume_if_requested(state->easy);
    curl_multi_perform(state->multi, &state->running);
    if (state->running == 0) {
        drain_messages(state);
        state->done = true;
    }
}

/// Wait for socket activity or a curl internal timer (connect / low-speed /
/// header deadlines).  Returns when the transfer has something to do; the
/// caller must follow up with perform_once().
void wait_for_multi(StreamState *state, long wait_ms)
{
    long curl_ms = 0;
    curl_multi_timeout(state->multi, &curl_ms);
    long effective_ms = wait_ms;
    if (curl_ms > 0)
        effective_ms = std::min(effective_ms, curl_ms);
    if (effective_ms <= 0)
        effective_ms = 1;
    curl_multi_poll(state->multi, nullptr, 0, effective_ms, nullptr);
}

// ---------------------------------------------------------------------------
// curl easy setup shared by execute() and execute_stream()
// ---------------------------------------------------------------------------

/// Bounded upstream socket buffers (design doc: two-pool-bandwidth-allocation
/// finding 3).  With auto-tuned kernel buffers, CURLINFO_SIZE_UPLOAD_T leads
/// the wire by megabytes — `written` ≠ `delivered` — and the rate-limiter's
/// backlog signal goes blind.  64 KB bounds the discrepancy to one window.
int upstream_sockopt_cb(void *clientp, curl_socket_t fd, curlsocktype purpose)
{
    (void)clientp;
    if (purpose != CURLSOCKTYPE_IPCXN)
        return CURL_SOCKOPT_OK;
    constexpr int kSockBufBytes = 64 * 1024;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &kSockBufBytes, sizeof(kSockBufBytes));
    return CURL_SOCKOPT_OK;
}

void setup_common_options(CURL *easy, const CurlRequest &request)
{
    curl_easy_setopt(easy, CURLOPT_URL, request.url.c_str());

    // HTTP/2 only — deployment curl has no h3 support.
    curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2);

    // No signal handling (threaded usage).
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);

    // TLS.
    curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST, 2L);

    // Disable transparent decompression — the proxy uses Accept-Encoding: identity.
    curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, kEmptyAcceptEncoding);

    // Connection reuse (curl_multi caches connections per multi handle).
    curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, 1L);

    // Bounded send buffer on the upstream socket (see upstream_sockopt_cb).
    curl_easy_setopt(easy, CURLOPT_SOCKOPTFUNCTION, upstream_sockopt_cb);

    // Upload rate limiting: apply the coordinator's desired cap.  The flow
    // was pre-warmed at join (cap = P from byte 0), so the window is never
    // cold when a down-scale arrives.
    if (request.rate_flow) {
        const long long cap = request.rate_flow->desired_cap_bps();
        if (cap > 0)
            curl_easy_setopt(easy, CURLOPT_MAX_SEND_SPEED_LARGE, static_cast<curl_off_t>(cap));
    }

    // Four-segment timeouts.  connect + upload-stall apply to every transfer;
    // the total timeout is optional (0 = disabled) so big-body uploads that
    // make progress are never killed by a wall-clock cap.
    curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT, request.connect_timeout_s);
    curl_easy_setopt(easy, CURLOPT_LOW_SPEED_LIMIT, request.upload_low_speed_bytes);
    curl_easy_setopt(easy, CURLOPT_LOW_SPEED_TIME, request.upload_low_speed_time);
    if (request.total_timeout_s > 0)
        curl_easy_setopt(easy, CURLOPT_TIMEOUT, request.total_timeout_s);
    else
        curl_easy_setopt(easy, CURLOPT_TIMEOUT, 0L);

    // Method.
    const std::string &method = request.method;
    if (method == "POST") {
        curl_easy_setopt(easy, CURLOPT_POST, 1L);
    } else if (method == "GET") {
        curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    } else {
        curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, method.c_str());
    }
}

void setup_body_options(CURL *easy, const CurlRequest &request)
{
    const bool has_source = static_cast<bool>(request.body_source);
    const bool has_initial = !request.initial_body_chunk.empty();

    // Keep the request method selected by setup_common_options().  In
    // particular, CURLOPT_UPLOAD changes a POST into PUT semantics, which
    // breaks every JSON API endpoint that expects POST + READFUNCTION.
    // CURLOPT_POST with a read callback is the streaming POST path.
    if (has_initial && !has_source) {
        // Entire body is known upfront.
        curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.initial_body_chunk.size()));
    } else if (request.content_length >= 0) {
        // Sliding-window body with a declared size: keep the upstream framing
        // (real Content-Length) instead of falling back to chunked.
        curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.content_length));
    }
    // Otherwise curl uses a streaming request body with no known total size.
}

struct CurlSlistGuard {
    struct curl_slist *list = nullptr;
    ~CurlSlistGuard()
    {
        if (list)
            curl_slist_free_all(list);
    }
    CurlSlistGuard() = default;
    CurlSlistGuard(const CurlSlistGuard &) = delete;
    CurlSlistGuard &operator=(const CurlSlistGuard &) = delete;
};

void setup_headers(CURL *easy, const CurlRequest &request, CurlSlistGuard &guard)
{
    for (const CurlHeader &header : request.headers) {
        // Let curl manage these internally.
        if (header.name == "Host" || header.name == "Content-Length" || header.name == "Connection")
            continue;
        std::string line = header.name + ": " + header.value;
        guard.list = curl_slist_append(guard.list, line.c_str());
    }
    if (guard.list)
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, guard.list);
}

void setup_dns_pin(CURL *easy, const CurlRequest &request, CurlSlistGuard &guard)
{
    // DNS pin: prevent rebinding between SSRF validation and curl's own
    // getaddrinfo call.  Only set when upstream.cpp provides a pin.
    if (!request.dns_pin.empty()) {
        guard.list = curl_slist_append(guard.list, request.dns_pin.c_str());
        if (guard.list)
            curl_easy_setopt(easy, CURLOPT_RESOLVE, guard.list);
    }
}

// ---------------------------------------------------------------------------
// multi-handle event loop (non-streaming)
// ---------------------------------------------------------------------------

void drive_multi_to_completion(CURLM *multi, CURL *easy, RequestContext &ctx)
{
    int running = 0;
    do {
        ctx.wake_state->resume_if_requested(easy);
        CURLMcode mc = curl_multi_perform(multi, &running);
        if (mc != CURLM_OK)
            throw std::runtime_error("curl_multi_perform failed");
        tick_rate(easy, ctx.request.rate_flow, ctx.applied_cap_bps);

        if (running > 0) {
            mc = curl_multi_wait(multi, nullptr, 0, 100, nullptr);
            if (mc != CURLM_OK)
                throw std::runtime_error("curl_multi_wait failed");
        }
    } while (running > 0);

    // Check completion status.
    int msgs_left = 0;
    CURLMsg *msg = curl_multi_info_read(multi, &msgs_left);
    if (msg && msg->msg == CURLMSG_DONE) {
        if (msg->data.result != CURLE_OK) {
            throw std::runtime_error(std::string{ "upstream request failed: " } + curl_easy_strerror(msg->data.result));
        }
    }
}

// ---------------------------------------------------------------------------
// streaming: drive until headers / pump read
// ---------------------------------------------------------------------------

void drive_until_headers(const std::shared_ptr<StreamState> &state, long header_timeout_s)
{
    const long header_timeout_ms = header_timeout_s > 0 ? header_timeout_s * 1000 : 60000;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(header_timeout_ms);
    const auto upload_stall = std::chrono::seconds(state->request.upload_low_speed_time);
    std::chrono::steady_clock::time_point paused_since{};

    for (;;) {
        perform_once(state.get());

        // Report progress and apply any new cap on the multi owner thread.
        tick_rate(state->easy, state->request.rate_flow, state->applied_cap_bps);

        if (state->headers_ready)
            return;
        if (state->done) {
            if (!state->error && state->error_msg.empty())
                state->error_msg = "upstream closed before response headers";
            state->error = true;
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        long remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
        if (remaining <= 0) {
            state->error = true;
            state->error_msg = "response headers timeout";
            return;
        }

        if (state->paused_on_body) {
            // Body producer has no data yet.  The upload-stall window governs
            // this phase (curl's low-speed timer may also fire via the wait).
            if (paused_since == std::chrono::steady_clock::time_point{})
                paused_since = now;
            else if (now - paused_since >= upload_stall) {
                state->error = true;
                state->error_msg = "upload stalled";
                return;
            }
            wait_for_multi(state.get(), std::min(remaining, 500L));
            continue;
        }
        paused_since = {};
        wait_for_multi(state.get(), std::min(remaining, 1000L));
    }
}

ssize_t stream_read(const std::shared_ptr<StreamState> &state, char *out, size_t size, int idle_timeout_ms)
{
    const std::chrono::milliseconds idle_timeout(idle_timeout_ms > 0 ? idle_timeout_ms : 60000);
    auto last_progress = std::chrono::steady_clock::now();

    for (;;) {
        // Serve buffered response bytes first.
        if (!state->pending.empty()) {
            const size_t n = std::min(size, state->pending.size());
            std::memcpy(out, state->pending.data(), n);
            state->pending.erase(0, n);
            last_progress = std::chrono::steady_clock::now();
            return static_cast<ssize_t>(n);
        }

        // Rate-limiter progress is sampled only by the multi owner thread.
        tick_rate(state->easy, state->request.rate_flow, state->applied_cap_bps);

        if (state->done) {
            if (state->error) {
                errno = EIO;
                return -1;
            }
            return 0; // clean EOF
        }

        const auto now = std::chrono::steady_clock::now();

        if (state->paused_on_body) {
            // No sockets are active while the body producer is paused; wait
            // for its wake (curl_multi_wakeup) or curl's internal timers.
            // The upload-stall window bounds this phase, so a client that
            // stops mid-body cannot hold the thread forever.
            const auto upload_stall = std::chrono::seconds(std::max(state->request.upload_low_speed_time, 1L));
            if (state->paused_since == std::chrono::steady_clock::time_point{}) {
                state->paused_since = now;
            } else if (now - state->paused_since >= upload_stall) {
                state->error = true;
                state->error_msg = "upload stalled";
                errno = EIO;
                return -1;
            }
            wait_for_multi(state.get(), 500);
            perform_once(state.get());
            continue;
        }

        // Normal wait: bounded by the inter-event idle timeout once the
        // request body has been fully sent.  During upload the upload-stall
        // timer governs, so a slow-but-progressing big body is not killed.
        const long elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_progress).count();
        long wait_ms = state->body_sent ? std::max(static_cast<long>(idle_timeout.count()) - elapsed, 1L) : 5000;
        if (wait_ms <= 0) {
            errno = ETIMEDOUT;
            return -1;
        }
        wait_for_multi(state.get(), wait_ms);
        perform_once(state.get());
        if (!state->pending.empty() || state->done)
            continue;
        if (state->body_sent &&
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_progress)
                    .count() >= idle_timeout.count()) {
            errno = ETIMEDOUT;
            return -1;
        }
    }
}

void stream_close(const std::shared_ptr<StreamState> &state)
{
    if (state->closed)
        return;
    state->closed = true;
    // Release the rate-limiter share (its redistribution happens on the next
    // tick of any remaining flow).  The owner thread may finish() itself.
    if (state->request.rate_flow) {
        upload_rate_limiter().finish(state->request.rate_flow);
    }
    state->wake_state->detach();
    if (state->easy) {
        if (state->multi)
            curl_multi_remove_handle(state->multi, state->easy);
        curl_easy_cleanup(state->easy);
        state->easy = nullptr;
    }
}

} // namespace

// ============================================================================
// CurlMultiPool::Impl
// ============================================================================

struct CurlMultiPool::Impl {
    CURLM *multi = nullptr;

    Impl()
        : multi(curl_multi_init())
    {
        if (!multi)
            throw std::runtime_error("curl_multi_init failed");
    }

    ~Impl()
    {
        if (multi)
            curl_multi_cleanup(multi);
    }

    Impl(const Impl &) = delete;
    Impl &operator=(const Impl &) = delete;
};

// ============================================================================
// CurlMultiPool
// ============================================================================

CurlMultiPool::CurlMultiPool()
    : impl_(std::make_unique<Impl>())
{
}

CurlMultiPool::~CurlMultiPool() = default;

CurlResponse CurlMultiPool::execute(const CurlRequest &request)
{
    RateFlowGuard rate_guard{ request.rate_flow };
    CURL *easy = curl_easy_init();
    if (!easy)
        throw std::runtime_error("curl_easy_init failed");

    auto wake_state = std::make_shared<StreamWakeState>();
    {
        std::lock_guard<std::mutex> lock(wake_state->mu);
        wake_state->easy = easy;
        wake_state->multi = impl_->multi;
    }

    // Ensure cleanup on early exit.
    struct EasyGuard {
        CURL *e = nullptr;
        CURLM *m = nullptr;
        bool removed = false;
        std::shared_ptr<StreamWakeState> wake_state;
        ~EasyGuard()
        {
            if (wake_state)
                wake_state->detach();
            if (!e)
                return;
            if (m && !removed)
                curl_multi_remove_handle(m, e);
            curl_easy_cleanup(e);
        }
    } guard{ easy, impl_->multi, false, wake_state };

    RequestContext ctx;
    ctx.request = request;
    ctx.wake_state = wake_state;

    setup_common_options(easy, request);
    setup_body_options(easy, request);
    CurlSlistGuard dns_guard;
    setup_dns_pin(easy, request, dns_guard);
    CurlSlistGuard header_guard;
    setup_headers(easy, request, header_guard);

    // Callbacks.
    curl_easy_setopt(easy, CURLOPT_READFUNCTION, readfn);
    curl_easy_setopt(easy, CURLOPT_READDATA, &ctx);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, writefn);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &ctx);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, headerfn);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, &ctx);

    if (request.body_source && !request.body_source->wake) {
        request.body_source->wake = [wake_state]() { wake_state->wake(); };
    }

    CURLMcode mc = curl_multi_add_handle(impl_->multi, easy);
    if (mc != CURLM_OK)
        throw std::runtime_error("curl_multi_add_handle failed");

    drive_multi_to_completion(impl_->multi, easy, ctx);

    // Extract the accumulated response *before* removing + cleaning up easy.
    CurlResponse response = std::move(ctx.response);

    curl_multi_remove_handle(impl_->multi, easy);
    guard.removed = true;
    // EasyGuard still owns cleanup; only removal has already happened.

    return response;
}

CurlMultiPool::StreamResult CurlMultiPool::execute_stream(const CurlRequest &request)
{
    RateFlowGuard rate_guard{ request.rate_flow };
    CURL *easy = curl_easy_init();
    if (!easy)
        throw std::runtime_error("curl_easy_init failed");

    auto state = std::make_shared<StreamState>();
    state->request = request;
    state->easy = easy;
    state->multi = impl_->multi;
    {
        std::lock_guard<std::mutex> lock(state->wake_state->mu);
        state->wake_state->easy = easy;
        state->wake_state->multi = impl_->multi;
    }

    setup_common_options(easy, request);
    setup_body_options(easy, request);
    CurlSlistGuard dns_guard;
    setup_dns_pin(easy, request, dns_guard);
    CurlSlistGuard header_guard;
    setup_headers(easy, request, header_guard);

    curl_easy_setopt(easy, CURLOPT_READFUNCTION, stream_readfn);
    curl_easy_setopt(easy, CURLOPT_READDATA, state.get());
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, stream_writefn);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, state.get());
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, stream_headerfn);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, state.get());

    if (request.body_source && !request.body_source->wake) {
        request.body_source->wake = [wake_state = state->wake_state]() { wake_state->wake(); };
    }

    CURLMcode mc = curl_multi_add_handle(impl_->multi, easy);
    if (mc != CURLM_OK) {
        rate_guard.release();
        stream_close(state);
        throw std::runtime_error("curl_multi_add_handle failed");
    }
    // The returned stream now owns the flow and releases it in stream_close.
    rate_guard.release();

    drive_until_headers(state, request.header_timeout_s);

    if (!state->headers_ready) {
        const std::string message = state->error_msg.empty() ? "upstream stream failed" : state->error_msg;
        stream_close(state);
        throw std::runtime_error(message);
    }

    StreamResult result;
    result.status_code = state->status_code;
    result.headers = std::move(state->headers);
    result.initial_body = std::move(state->pending);

    result.stream_read = [state](char *out, size_t size, int idle_timeout_ms) -> ssize_t {
        return stream_read(state, out, size, idle_timeout_ms);
    };
    result.stream_error = [state]() { return state->error; };
    result.stream_error_message = [state]() { return state->error_msg; };
    result.stream_close = [state]() { stream_close(state); };

    return result;
}

int CurlMultiPool::perform()
{
    if (!impl_->multi)
        return 0;
    int running = 0;
    CURLMcode mc = curl_multi_perform(impl_->multi, &running);
    if (mc != CURLM_OK)
        return 0;
    return running;
}

} // namespace realurl
