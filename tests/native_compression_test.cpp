#include "realurl/detail/frame.hpp"
#include "realurl/detail/native_codec.hpp"
#include "realurl/detail/native_compression.hpp"
#include "realurl/detail/protocol.hpp"
#include "realurl/http2.hpp"
#include "realurl/server.hpp"

#include "support/raw_connection.hpp"
#include "test_harness.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <future>
#include <map>
#include <netinet/in.h>
#include <optional>
#include <span>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#ifdef REALURL_HAVE_ZSTD
#include <zstd.h>
#endif

#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__GLIBC__)
#include <malloc.h>
#endif

using namespace std::chrono_literals;
using realurl::Protocol;
namespace h2 = realurl::detail::h2;
namespace native = realurl::detail::native;
using test::RawConnection;
using test::RawFrame;
using test::connect_loopback;
using test::end_of;
using test::read_be32;

namespace
{

constexpr std::size_t kLargeBody = 256 * 1024;

int free_port()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(fd);
        return -1;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length) != 0) {
        ::close(fd);
        return -1;
    }
    const int port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

void wait_running(const realurl::Server &server)
{
    for (int i = 0; i < 1000 && !server.is_running(); ++i)
        std::this_thread::sleep_for(1ms);
}

realurl::Config native_config(bool compression)
{
    realurl::Config config;
    config.protocol = Protocol::Native;
    config.max_conns_per_origin = 1;
    config.connect_timeout = 1s;
    config.native_compression = compression;
    return config;
}

std::string base_url(int port)
{
    return "http://127.0.0.1:" + std::to_string(port);
}

/// Deterministic JSON-like text that zstd compresses well.
std::string json_body(std::size_t size)
{
    std::string out = "[";
    for (int i = 0; out.size() < size; ++i) {
        out += "{\"id\":" + std::to_string(i) + ",\"name\":\"user-" + std::to_string(i * 7919 % 1000) +
               "\",\"active\":" + (i % 3 == 0 ? "true" : "false") + ",\"score\":" + std::to_string(i * 37 % 101) + "},";
    }
    out.resize(size);
    return out;
}

void add_routes(realurl::Server &server, const std::string &json)
{
    server.set_payload_max_length(4 * 1024 * 1024);
    server.Post("/echo", [](const auto &request, auto &response) { response.set_content(request.body); });
    server.Get("/json", [&json](const auto &, auto &response) { response.set_content(json); });
    server.Get("/small", [](const auto &, auto &response) { response.set_content(std::string(1023, 's')); });
    server.Get("/encoded", [&json](const auto &, auto &response) {
        response.set_header("content-encoding", "identity");
        response.set_content(json);
    });
    server.Get("/hello", [](const auto &, auto &response) { response.set_content("world"); });
}

/// Large and small bodies in both directions, with compression enabled on
/// either side or both.
void test_round_trip(bool client_compression, bool server_compression)
{
    const int port = free_port();
    CHECK(port > 0);

    const std::string json = json_body(kLargeBody);
    realurl::Server server;
    server.set_io_threads(1);
    server.set_native_compression(server_compression);
    add_routes(server, json);

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    realurl::ClientGroup group(1, native_config(client_compression));
    const std::string base = base_url(port);

    const realurl::Response upload = realurl::sync_wait(group.request("POST", base + "/echo", {}, json));
    CHECK(upload.head.status == 200);
    CHECK(upload.head.protocol == Protocol::Native);
    CHECK(upload.body.size() == kLargeBody);
    CHECK(upload.body == json);

    const realurl::Response download = realurl::sync_wait(group.get(base + "/json"));
    CHECK(download.head.status == 200);
    CHECK(download.head.content_length == kLargeBody);
    CHECK(download.body == json);

    const realurl::Response small = realurl::sync_wait(group.get(base + "/small"));
    CHECK(small.head.status == 200);
    CHECK(small.body == std::string(1023, 's'));

    const realurl::Response ping = realurl::sync_wait(group.request("POST", base + "/echo", {}, "ping"));
    CHECK(ping.head.status == 200);
    CHECK(ping.body == "ping");

    CHECK(server.stats().native_connections == 1);

    group.close();
    server.stop();
    listener.join();
}

/// Frame size a peer may send to a raw connection, which never advertises
/// SETTINGS_MAX_FRAME_SIZE.
constexpr std::size_t kFrameSize = h2::kDefaultMaxFrameSize;

std::string settings_frame(bool offer_compression, std::uint32_t initial_window = 8u << 20)
{
    std::vector<h2::Setting> settings = { { h2::kSettingInitialWindowSize, initial_window } };
    if (offer_compression)
        settings.push_back({ realurl::detail::kSettingNativeCompression, realurl::detail::kNativeCompressionZstd });
    h2::FrameWriter writer;
    writer.settings(settings);
    writer.window_update(0, 16u << 20);
    return writer.buffer();
}

std::string request_headers(std::uint32_t stream_id, std::string_view method, std::string_view target, bool end_stream)
{
    std::string block;
    CHECK(native::encode_request(method, target, std::span<const native::HeaderView>(), std::nullopt, block));
    std::string out;
    h2::append_headers(out, stream_id, block, end_stream, kFrameSize);
    return out;
}

/// A new server stream starts its send window at the client's
/// SETTINGS_INITIAL_WINDOW_SIZE: the first DATA frame is exactly that size.
void test_server_stream_initial_window()
{
    const int port = free_port();
    CHECK(port > 0);

    const std::string json = json_body(kLargeBody);
    realurl::Server server;
    server.set_io_threads(1);
    add_routes(server, json);

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    constexpr std::uint32_t kInitialWindow = 1000;
    RawConnection connection(connect_loopback(port));
    std::string request = std::string(realurl::detail::kNativePreface) + settings_frame(false, kInitialWindow);
    request += request_headers(1, "GET", "/json", true);
    CHECK(connection.send(request));

    std::size_t first_frame = 0;
    std::string body;
    RawFrame frame;
    while (connection.next(frame)) {
        if (frame.header.type == h2::FrameType::Data && frame.header.stream_id == 1) {
            body += frame.payload;
            if (first_frame == 0) {
                first_frame = frame.payload.size();
                h2::FrameWriter update;
                update.window_update(1, 8u << 20);
                CHECK(connection.send(update.buffer()));
            }
        }
        if (end_of(frame, 1))
            break;
    }

    CHECK(first_frame == kInitialWindow);
    CHECK(body == json);

    server.stop();
    listener.join();
}

#ifdef REALURL_HAVE_ZSTD

std::string data_frame(std::uint32_t stream_id, std::string_view payload, std::uint8_t flags)
{
    std::string out;
    h2::append_frame_header(out, h2::FrameType::Data, flags, stream_id, payload.size());
    out.append(payload);
    return out;
}

std::string zstd_decode(std::string_view frame)
{
    const unsigned long long size = ZSTD_getFrameContentSize(frame.data(), frame.size());
    if (size == ZSTD_CONTENTSIZE_UNKNOWN || size == ZSTD_CONTENTSIZE_ERROR)
        return {};
    std::string out(static_cast<std::size_t>(size), '\0');
    const std::size_t n = ZSTD_decompress(out.data(), out.size(), frame.data(), frame.size());
    if (ZSTD_isError(n))
        return {};
    out.resize(n);
    return out;
}

/// Compressed frame without a content size, as a streaming encoder writes it.
std::string zstd_without_size(std::string_view data)
{
    ZSTD_CCtx *cctx = ZSTD_createCCtx();
    (void)ZSTD_CCtx_setParameter(cctx, ZSTD_c_contentSizeFlag, 0);
    std::string out(ZSTD_compressBound(data.size()), '\0');
    const std::size_t n = ZSTD_compress2(cctx, out.data(), out.size(), data.data(), data.size());
    ZSTD_freeCCtx(cctx);
    out.resize(ZSTD_isError(n) ? 0 : n);
    return out;
}

std::string zstd_with_size(std::string_view data)
{
    std::string out(ZSTD_compressBound(data.size()), '\0');
    const std::size_t n = ZSTD_compress(out.data(), out.size(), data.data(), data.size(), 1);
    out.resize(ZSTD_isError(n) ? 0 : n);
    return out;
}

/// One zstd frame holding a single RLE block of `block` copies of `byte`,
/// declaring `content` bytes (256..65791). A block longer than the declared
/// size passes content_size() but fails to decode.
std::string zstd_rle(std::size_t content, std::size_t block, char byte)
{
    // Magic, then a single-segment descriptor with a 2-byte content size.
    std::string out("\x28\xb5\x2f\xfd\x60", 5);
    const std::size_t declared = content - 256;
    out += static_cast<char>(declared & 0xff);
    out += static_cast<char>(declared >> 8);
    // Last block, type RLE.
    const std::size_t header = 1 | (1 << 1) | (block << 3);
    out += static_cast<char>(header & 0xff);
    out += static_cast<char>((header >> 8) & 0xff);
    out += static_cast<char>((header >> 16) & 0xff);
    out += byte;
    return out;
}

constexpr std::uint64_t kFnvBasis = 14695981039346656037ull;

std::uint64_t fnv1a(std::uint64_t hash, std::string_view bytes)
{
    for (char c : bytes) {
        hash ^= static_cast<unsigned char>(c);
        hash *= 1099511628211ull;
    }
    return hash;
}

/// Bytes the process holds in malloc, or 0 where the allocator does not say.
std::size_t heap_in_use()
{
#if defined(__APPLE__)
    malloc_statistics_t stats{};
    malloc_zone_statistics(nullptr, &stats);
    return stats.size_in_use;
#elif defined(__GLIBC__) && __GLIBC_PREREQ(2, 33)
    const struct mallinfo2 info = mallinfo2();
    return info.uordblks + info.hblkhd;
#else
    return 0;
#endif
}

struct WireBody {
    std::string body;
    int data_frames = 0;
    int compressed_frames = 0;
};

/// Append one DATA frame to `out`, decoding it when it carries the flag.
void collect_data(const RawFrame &frame, WireBody &out)
{
    if (frame.payload.empty())
        return;
    ++out.data_frames;
    if ((frame.header.flags & realurl::detail::kFlagNativeCompressed) != 0) {
        ++out.compressed_frames;
        out.body += zstd_decode(frame.payload);
    } else {
        out.body += frame.payload;
    }
}

/// GET `target` over a raw native connection and return the DATA frames as
/// they appeared on the wire.
WireBody raw_get(int port, std::string_view target, bool offer_compression)
{
    RawConnection connection(connect_loopback(port));
    std::string request = std::string(realurl::detail::kNativePreface) + settings_frame(offer_compression);
    request += request_headers(1, "GET", target, true);
    h2::FrameWriter update;
    update.window_update(1, 8u << 20);
    request += update.buffer();
    CHECK(connection.send(request));

    WireBody wire;
    RawFrame frame;
    while (connection.next(frame)) {
        if (frame.header.type == h2::FrameType::Data && frame.header.stream_id == 1)
            collect_data(frame, wire);
        if (end_of(frame, 1))
            break;
    }
    return wire;
}

/// A server with compression enabled compresses every full-size frame for a
/// peer that advertises zstd, and sends plain frames to a peer that does not
/// or when the response already carries content-encoding.
void test_server_wire_frames()
{
    const int port = free_port();
    CHECK(port > 0);

    const std::string json = json_body(kLargeBody);
    realurl::Server server;
    server.set_io_threads(1);
    server.set_native_compression(true);
    add_routes(server, json);

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    constexpr int kFrames = static_cast<int>(kLargeBody / kFrameSize);

    const WireBody compressed = raw_get(port, "/json", true);
    CHECK(compressed.body == json);
    CHECK(compressed.data_frames == kFrames);
    CHECK(compressed.compressed_frames == kFrames);

    const WireBody plain = raw_get(port, "/json", false);
    CHECK(plain.body == json);
    CHECK(plain.data_frames == kFrames);
    CHECK(plain.compressed_frames == 0);

    const WireBody encoded = raw_get(port, "/encoded", true);
    CHECK(encoded.body == json);
    CHECK(encoded.data_frames == kFrames);
    CHECK(encoded.compressed_frames == 0);

    server.stop();
    listener.join();
}

/// Compression is opt-in: a default server sends plain DATA even to a peer
/// that advertises zstd.
void test_server_default_plain()
{
    const int port = free_port();
    CHECK(port > 0);

    const std::string json = json_body(kLargeBody);
    realurl::Server server;
    server.set_io_threads(1);
    add_routes(server, json);

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    const WireBody wire = raw_get(port, "/json", true);
    CHECK(wire.body == json);
    CHECK(wire.data_frames == static_cast<int>(kLargeBody / kFrameSize));
    CHECK(wire.compressed_frames == 0);

    server.stop();
    listener.join();
}

/// The server's advertised SETTINGS_MAX_FRAME_SIZE, read from its preface.
std::uint32_t read_server_max_frame_size(RawConnection &connection)
{
    RawFrame frame;
    while (connection.next(frame)) {
        if (frame.header.type != h2::FrameType::Settings || (frame.header.flags & h2::kFlagAck) != 0)
            continue;
        std::uint32_t value = h2::kDefaultMaxFrameSize;
        for (std::size_t offset = 0; offset + 6 <= frame.payload.size(); offset += 6) {
            const auto id = static_cast<std::uint16_t>((static_cast<unsigned char>(frame.payload[offset]) << 8) |
                                                       static_cast<unsigned char>(frame.payload[offset + 1]));
            if (id == h2::kSettingMaxFrameSize)
                value = read_be32(std::string_view(frame.payload).substr(offset + 2));
        }
        return value;
    }
    return 0;
}

/// A compressed DATA frame without a content size, one that inflates past
/// the receiver's advertised max_frame_size, or one that fails to decode when
/// the handler reads it, resets its stream. A frame that inflates to exactly
/// that size is accepted, and the connection keeps serving.
void test_bad_compressed_frames()
{
    const int port = free_port();
    CHECK(port > 0);

    const std::string json = json_body(kLargeBody);
    realurl::Server server;
    server.set_io_threads(1);
    server.set_native_compression(true);
    add_routes(server, json);

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    RawConnection connection(connect_loopback(port));
    CHECK(connection.send(std::string(realurl::detail::kNativePreface) + settings_frame(true)));
    const std::uint32_t limit = read_server_max_frame_size(connection);
    CHECK(limit >= h2::kDefaultMaxFrameSize);

    const std::uint8_t flags = realurl::detail::kFlagNativeCompressed | h2::kFlagEndStream;
    const std::string over(limit + 1, '\0');
    const std::string exact = json.substr(0, limit);

    std::string request = request_headers(1, "POST", "/echo", false);
    request += data_frame(1, zstd_without_size(json.substr(0, 4096)), flags);
    request += request_headers(3, "POST", "/echo", false);
    request += data_frame(3, zstd_with_size(over), flags);
    request += request_headers(5, "GET", "/hello", true);
    request += request_headers(7, "POST", "/echo", false);
    request += data_frame(7, zstd_with_size(exact), flags);
    request += request_headers(9, "POST", "/echo", false);
    request += data_frame(9, zstd_rle(1000, 2000, 'x'), flags);
    CHECK(connection.send(request));

    std::map<std::uint32_t, std::uint32_t> resets;
    int status_5 = 0;
    std::string body_5;
    WireBody echo_7;
    bool ended_5 = false;
    bool ended_7 = false;
    RawFrame frame;
    while (!(ended_5 && ended_7 && resets.size() == 3) && connection.next(frame)) {
        const std::uint32_t id = frame.header.stream_id;
        if (frame.header.type == h2::FrameType::RstStream && frame.payload.size() == 4)
            resets[id] = read_be32(frame.payload);
        if (frame.header.type == h2::FrameType::Headers && id == 5) {
            std::vector<native::HeaderView> views;
            native::ResponseHead head;
            if (native::decode_response(frame.payload, views, head))
                status_5 = head.status;
        }
        if (frame.header.type == h2::FrameType::Data && id == 5)
            body_5 += frame.payload;
        if (frame.header.type == h2::FrameType::Data && id == 7)
            collect_data(frame, echo_7);
        ended_5 = ended_5 || end_of(frame, 5);
        ended_7 = ended_7 || end_of(frame, 7);
    }

    const auto protocol_error = static_cast<std::uint32_t>(h2::ErrorCode::Protocol);
    const std::map<std::uint32_t, std::uint32_t> expected_resets = { { 1, protocol_error },
                                                                     { 3, protocol_error },
                                                                     { 9, protocol_error } };
    CHECK(resets == expected_resets);
    CHECK(status_5 == 200);
    CHECK(body_5 == "world");
    CHECK(echo_7.body == exact);

    server.stop();
    listener.join();
}

/// Listening loopback socket; `port` receives its port.
int listen_loopback(int &port)
{
    const int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(::bind(listen_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0);
    CHECK(::listen(listen_fd, 1) == 0);
    socklen_t length = sizeof(address);
    CHECK(::getsockname(listen_fd, reinterpret_cast<sockaddr *>(&address), &length) == 0);
    port = ntohs(address.sin_port);
    return listen_fd;
}

/// What a native server sends first: the hello frame and SETTINGS that
/// advertise zstd.
std::string native_server_hello()
{
    char hello[realurl::detail::kNativeHelloPayloadSize] = {};
    std::memcpy(hello, realurl::detail::kNativeHelloMagic.data(), realurl::detail::kNativeHelloMagic.size());
    hello[8] = static_cast<char>(realurl::detail::kNativeVersionMajor);
    hello[9] = static_cast<char>(realurl::detail::kNativeVersionMinor);
    h2::FrameWriter writer;
    writer.extension(realurl::detail::kNativeHelloType, 0, 0, { hello, sizeof(hello) });
    return writer.buffer() + settings_frame(true);
}

/// The client compresses request DATA once the server's SETTINGS advertise
/// zstd. A raw native server records the upload frames.
void test_client_wire_frames()
{
    int port = 0;
    const int listen_fd = listen_loopback(port);

    const std::string json = json_body(kLargeBody);
    WireBody upload;
    std::string preface;

    std::thread peer([&] {
        RawConnection connection(::accept(listen_fd, nullptr, nullptr));
        if (!connection.read_exact(realurl::detail::kNativePreface.size(), preface))
            return;
        if (!connection.send(native_server_hello()))
            return;

        std::string response;
        CHECK(native::encode_response(200, std::span<const native::HeaderView>(), 0, response));
        RawFrame frame;
        while (connection.next(frame)) {
            const std::uint32_t id = frame.header.stream_id;
            if (frame.header.type == h2::FrameType::Data && id == 3)
                collect_data(frame, upload);
            if (!end_of(frame, id))
                continue;
            std::string out;
            h2::append_headers(out, id, response, true, kFrameSize);
            (void)connection.send(out);
            if (id == 3)
                break;
        }
    });

    realurl::ClientGroup group(1, native_config(true));
    const std::string base = base_url(port);
    const realurl::Response warmup = realurl::sync_wait(group.get(base + "/warmup"));
    CHECK(warmup.head.status == 200);
    const realurl::Response posted = realurl::sync_wait(group.request("POST", base + "/upload", {}, json));
    CHECK(posted.head.status == 200);

    peer.join();
    group.close();
    ::close(listen_fd);

    constexpr int kFrames = static_cast<int>(kLargeBody / kFrameSize);
    CHECK(preface == realurl::detail::kNativePreface);
    CHECK(upload.body == json);
    CHECK(upload.data_frames == kFrames);
    CHECK(upload.compressed_frames == kFrames);
}

/// A receive queue keeps compressed frames compressed: it holds their wire
/// bytes plus the rest of at most one decoded frame, and a slow reader gets
/// the exact decoded bytes back.
void test_recv_queue_holds_wire_bytes()
{
    const std::string zeros(kFrameSize, '\0');
    const std::string json = json_body(kFrameSize);
    const std::string packed_zeros = zstd_rle(kFrameSize, kFrameSize, '\0');
    const std::string packed_json = zstd_with_size(json);
    constexpr int kFrames = 256;

    native::RecvQueue queue;
    std::string expected;
    std::size_t wire = 0;
    for (int i = 0; i < kFrames; ++i) {
        const std::string &packed = i % 8 == 7 ? packed_json : packed_zeros;
        std::size_t decoded = 0;
        CHECK(native::content_size(packed, kFrameSize, decoded));
        queue.push(packed, decoded, true);
        queue.push("plain", 5, false);
        expected += i % 8 == 7 ? json : zeros;
        expected += "plain";
        wire += packed.size() + 5;
    }
    CHECK(queue.size() == wire);
    CHECK(queue.decoded_size() == expected.size());

    char buffer[1000];
    std::size_t n = 0;
    CHECK(queue.read(buffer, n));
    CHECK(n == sizeof(buffer));
    CHECK(queue.size() == wire - packed_zeros.size() + kFrameSize - sizeof(buffer));

    std::string body(buffer, n);
    while (body.size() < expected.size() / 2 && queue.read(buffer, n))
        body.append(buffer, n);
    CHECK(queue.drain_to(body));
    CHECK(body == expected);
    CHECK(queue.size() == 0);

    queue.push(zstd_rle(1000, 2000, 'x'), 1000, true);
    CHECK(queue.read(buffer, n) == false);
    CHECK(queue.size() == 0);
}

/// Frames of `kFrameSize` decoded bytes: mostly zeros, with a JSON frame every
/// 64th so the body is not uniform.
struct CompressibleBody {
    std::vector<std::string> frames;
    std::size_t size = 0;
    std::uint64_t hash = kFnvBasis;
};

CompressibleBody compressible_body(int frames)
{
    const std::string zeros(kFrameSize, '\0');
    const std::string json = json_body(kFrameSize);
    const std::string packed_zeros = zstd_rle(kFrameSize, kFrameSize, '\0');
    const std::string packed_json = zstd_with_size(json);

    CompressibleBody body;
    for (int i = 0; i < frames; ++i) {
        const bool is_json = i % 64 == 63;
        body.frames.push_back(is_json ? packed_json : packed_zeros);
        body.hash = fnv1a(body.hash, is_json ? json : zeros);
        body.size += kFrameSize;
    }
    return body;
}

/// A handler that does not read while 64 MiB of highly compressible upload
/// arrives leaves the server holding about the wire bytes, not the decoded
/// size. The stream is not reset, and the body arrives byte-identical once the
/// handler reads.
void test_slow_reader_compressed_upload()
{
    const int port = free_port();
    CHECK(port > 0);

    std::atomic<std::size_t> held{ 0 };
    realurl::Server server;
    server.set_io_threads(1);
    server.set_native_compression(true);
    server.Post("/slow", [&held](const realurl::Server::Request &request, realurl::Server::Response &response) {
        response.take_over_task([&request, &held](realurl::server::StreamWriter &writer) -> realurl::Task<void> {
            co_await request.loop->sleep_for(300ms);
            held = heap_in_use();
            std::string buffer(64 * 1024, '\0');
            std::uint64_t hash = kFnvBasis;
            std::size_t size = 0;
            for (;;) {
                const std::size_t n = co_await request.body_reader->read(buffer);
                if (n == 0)
                    break;
                hash = fnv1a(hash, { buffer.data(), n });
                size += n;
            }
            const std::string body = std::to_string(size) + ":" + std::to_string(hash);
            co_await writer.write_head(200, {}, body.size());
            co_await writer.write(body);
            co_await writer.end();
        });
    }, { .body_mode = realurl::Server::BodyMode::StreamedNative });

    std::thread listener([&] { (void)server.listen("127.0.0.1", port); });
    wait_running(server);

    const CompressibleBody upload = compressible_body(4096);
    std::string request = std::string(realurl::detail::kNativePreface) + settings_frame(false);
    request += request_headers(1, "POST", "/slow", false);
    for (const std::string &frame : upload.frames)
        request += data_frame(1, frame, realurl::detail::kFlagNativeCompressed);
    request += data_frame(1, {}, h2::kFlagEndStream);

    std::size_t baseline = 0;
    std::uint32_t reset = 0;
    WireBody response;
    {
        RawConnection connection(connect_loopback(port));
        baseline = heap_in_use();
        CHECK(connection.send(request));
        RawFrame frame;
        while (connection.next(frame)) {
            if (frame.header.type == h2::FrameType::RstStream && frame.header.stream_id == 1)
                reset = read_be32(frame.payload);
            if (frame.header.type == h2::FrameType::Data && frame.header.stream_id == 1)
                collect_data(frame, response);
            if (end_of(frame, 1) || reset != 0)
                break;
        }
    }

    CHECK(upload.size == 64u << 20);
    CHECK(reset == 0);
    CHECK(response.body == std::to_string(upload.size) + ":" + std::to_string(upload.hash));
    // The decoded upload is 64 MiB; its wire bytes are about 300 KiB.
    if (baseline != 0)
        CHECK(held.load() < baseline + (8u << 20));

    server.stop();
    listener.join();
}

/// The client keeps a highly compressible response compressed until the
/// caller reads it, and delivers it byte-identical.
void test_client_compressed_download()
{
    int port = 0;
    const int listen_fd = listen_loopback(port);
    const CompressibleBody download = compressible_body(2048);
    std::promise<void> received;

    std::thread peer([&] {
        RawConnection connection(::accept(listen_fd, nullptr, nullptr));
        std::string preface;
        if (!connection.read_exact(realurl::detail::kNativePreface.size(), preface) ||
            !connection.send(native_server_hello()))
            return;
        RawFrame frame;
        while (connection.next(frame) && !end_of(frame, 1)) {
        }

        std::string head;
        CHECK(native::encode_response(200, std::span<const native::HeaderView>(), std::nullopt, head));
        std::string out;
        h2::append_headers(out, 1, head, false, kFrameSize);
        for (const std::string &packed : download.frames)
            out += data_frame(1, packed, realurl::detail::kFlagNativeCompressed);
        out += data_frame(1, {}, h2::kFlagEndStream);
        (void)connection.send(out);
        received.get_future().wait();
    });

    realurl::ClientGroup group(1, native_config(true));
    const realurl::Response response = realurl::sync_wait(group.get(base_url(port) + "/zeros"));
    received.set_value();
    group.close();
    peer.join();
    ::close(listen_fd);

    CHECK(response.head.status == 200);
    CHECK(response.body.size() == download.size);
    CHECK(fnv1a(kFnvBasis, response.body) == download.hash);
}

#endif

} // namespace

int main()
{
    test_round_trip(true, true);
    test_round_trip(false, true);
    test_round_trip(true, false);
    test_server_stream_initial_window();
#ifdef REALURL_HAVE_ZSTD
    test_server_wire_frames();
    test_server_default_plain();
    test_bad_compressed_frames();
    test_client_wire_frames();
    test_recv_queue_holds_wire_bytes();
    test_slow_reader_compressed_upload();
    test_client_compressed_download();
#endif
    return test::summary();
}
