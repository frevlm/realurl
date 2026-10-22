#include "support/h2_test_server.hpp"

#include <openssl/ssl.h>

#include "realurl/detail/frame.hpp"
#include "realurl/detail/hpack.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace realurl::test
{
namespace
{

using namespace realurl::detail::h2;

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

std::uint32_t read_be24(const char *data)
{
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(data[0])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[1])) << 8) |
           static_cast<std::uint32_t>(static_cast<unsigned char>(data[2]));
}

std::uint32_t read_be32(const char *data)
{
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(data[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(data[2])) << 8) |
           static_cast<std::uint32_t>(static_cast<unsigned char>(data[3]));
}

bool send_all(int fd, std::string_view data, SSL *ssl)
{
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = 0;
        if (ssl != nullptr) {
            n = SSL_write(ssl, data.data() + sent, static_cast<int>(data.size() - sent));
        } else {
            n = ::send(fd, data.data() + sent, data.size() - sent, kSendFlags);
        }
        if (n <= 0)
            return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

int make_listener()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        throw std::runtime_error("h2 test server: socket failed");

    int yes = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 || ::listen(fd, 16) != 0) {
        ::close(fd);
        throw std::runtime_error("h2 test server: bind/listen failed");
    }
    return fd;
}

std::uint16_t listener_port(int fd)
{
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    (void)::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &length);
    return ntohs(address.sin_port);
}

bool recv_exact(int fd, void *data, std::size_t size, SSL *ssl, const std::atomic<bool> &stop)
{
    auto *cursor = static_cast<char *>(data);
    std::size_t remaining = size;

    while (remaining > 0) {
        ssize_t n = 0;
        if (ssl != nullptr) {
            n = SSL_read(ssl, cursor, static_cast<int>(remaining));
        } else {
            n = ::recv(fd, cursor, remaining, 0);
        }
        if (n > 0) {
            cursor += n;
            remaining -= static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0)
            return false;
        if (errno == EINTR)
            continue;
        if ((errno == EAGAIN || errno == EWOULDBLOCK) && !stop.load(std::memory_order_relaxed))
            continue;
        return false;
    }
    return true;
}

void apply_headers(H2TestRequest &request, HeaderList &&headers)
{
    for (HeaderField &header : headers) {
        if (header.name == ":method")
            request.method = std::move(header.value);
        else if (header.name == ":path")
            request.path = std::move(header.value);
        else if (header.name == ":authority")
            request.authority = std::move(header.value);
        else if (!header.name.empty() && header.name[0] != ':')
            request.headers.push_back(std::move(header));
    }
}

/// Shared frame loop for the plaintext and TLS test servers. `ssl` is null
/// for plaintext connections.
void run_frames(int fd, SSL *ssl, const Handler &handler, const std::atomic<bool> &stop)
{
    char preface[24];
    if (!recv_exact(fd, preface, sizeof(preface), ssl, stop))
        return;
    if (std::memcmp(preface, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", sizeof(preface)) != 0)
        return;

    {
        FrameWriter settings;
        const Setting settings_list[] = {
            { kSettingMaxConcurrentStreams, 200000 },
            { kSettingInitialWindowSize, 1024 * 1024 },
        };
        settings.settings(settings_list);
        if (!send_all(fd, settings.buffer(), ssl))
            return;

        FrameWriter update;
        update.window_update(0, 16 * 1024 * 1024 - 65535);
        if (!send_all(fd, update.buffer(), ssl))
            return;
    }

    HpackDecoder decoder;
    HpackEncoder encoder;
    std::unordered_map<std::uint32_t, H2TestRequest> requests;
    std::unordered_map<std::uint32_t, std::string> pending_blocks;
    std::mutex send_mtx;
    std::atomic<bool> connection_closed{ false };
    std::vector<std::thread> workers;

    struct WorkerGuard {
        std::atomic<bool> &connection_closed;
        std::vector<std::thread> &workers;
        ~WorkerGuard()
        {
            connection_closed.store(true, std::memory_order_relaxed);
            for (auto &t : workers) {
                if (t.joinable())
                    t.join();
            }
        }
    } worker_guard{ connection_closed, workers };

    auto do_send_response = [&](std::uint32_t stream_id, const H2TestResponse &response) {
        if (response.chunks > 0) {
            {
                std::lock_guard lock(send_mtx);
                if (stop.load(std::memory_order_relaxed) || connection_closed.load(std::memory_order_relaxed))
                    return;
                HeaderList headers;
                headers.push_back({ ":status", std::to_string(response.status) });
                for (const HeaderField &header : response.headers)
                    headers.push_back(header);

                std::string block;
                encoder.encode(headers, block);

                FrameWriter head_writer;
                head_writer.headers(stream_id, block, true, response.body.empty());
                if (!send_all(fd, head_writer.buffer(), ssl))
                    return;
            }

            const std::size_t total = response.body.size();
            const std::size_t n_chunks = std::max(1, response.chunks);
            const std::size_t chunk_size = (total + n_chunks - 1) / n_chunks;
            std::size_t offset = 0;
            for (std::size_t c = 0; c < n_chunks && offset < total; ++c) {
                if (response.chunk_delay.count() > 0) {
                    const auto start_chunk = std::chrono::steady_clock::now();
                    while (!stop.load(std::memory_order_relaxed) &&
                           !connection_closed.load(std::memory_order_relaxed) &&
                           std::chrono::steady_clock::now() - start_chunk < response.chunk_delay) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    }
                }
                if (stop.load(std::memory_order_relaxed) || connection_closed.load(std::memory_order_relaxed))
                    return;

                const std::size_t n = std::min(chunk_size, total - offset);
                const bool last = (offset + n == total);
                FrameWriter data_writer;
                data_writer.data(stream_id, std::string_view(response.body).substr(offset, n), last);
                std::lock_guard lock(send_mtx);
                if (!send_all(fd, data_writer.buffer(), ssl))
                    return;
                offset += n;
            }
        } else {
            if (response.delay.count() > 0) {
                const auto start_delay = std::chrono::steady_clock::now();
                while (!stop.load(std::memory_order_relaxed) &&
                       !connection_closed.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() - start_delay < response.delay) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
            }
            if (stop.load(std::memory_order_relaxed) || connection_closed.load(std::memory_order_relaxed))
                return;

            std::lock_guard lock(send_mtx);
            HeaderList headers;
            headers.push_back({ ":status", std::to_string(response.status) });
            for (const HeaderField &header : response.headers)
                headers.push_back(header);

            std::string block;
            encoder.encode(headers, block);

            FrameWriter head_writer;
            head_writer.headers(stream_id, block, true, response.body.empty());
            if (!send_all(fd, head_writer.buffer(), ssl))
                return;

            std::size_t offset = 0;
            while (offset < response.body.size()) {
                const std::size_t n = std::min<std::size_t>(16384, response.body.size() - offset);
                const bool last = offset + n == response.body.size();
                FrameWriter data_writer;
                data_writer.data(stream_id, std::string_view(response.body).substr(offset, n), last);
                if (!send_all(fd, data_writer.buffer(), ssl))
                    return;
                offset += n;
            }
        }

        std::lock_guard lock(send_mtx);
        if (response.reset_after) {
            FrameWriter reset;
            reset.rst_stream(stream_id, ErrorCode::Cancel);
            (void)send_all(fd, reset.buffer(), ssl);
        }
        if (response.goaway_after) {
            FrameWriter goaway;
            goaway.goaway(stream_id, ErrorCode::NoError);
            (void)send_all(fd, goaway.buffer(), ssl);
        }
    };

    auto respond = [&](std::uint32_t stream_id, const H2TestRequest &request) {
        const H2TestResponse response = handler(request);
        if (response.hold)
            return;
        if (response.delay.count() > 0 || response.chunks > 0) {
            workers.emplace_back([&, stream_id, resp = std::move(response)]() {
                do_send_response(stream_id, resp);
            });
        } else {
            do_send_response(stream_id, response);
        }
    };

    for (;;) {
        if (stop.load(std::memory_order_relaxed))
            return;

        char header_bytes[9];
        if (!recv_exact(fd, header_bytes, sizeof(header_bytes), ssl, stop))
            return;

        const std::uint32_t length = read_be24(header_bytes);
        if (length > 16384)
            return;

        const auto type = static_cast<FrameType>(static_cast<unsigned char>(header_bytes[3]));
        const auto flags = static_cast<std::uint8_t>(header_bytes[4]);
        const std::uint32_t stream_id = read_be32(header_bytes + 5) & 0x7fffffffu;

        std::string payload(length, '\0');
        if (length > 0 && !recv_exact(fd, payload.data(), length, ssl, stop))
            return;

        switch (type) {
        case FrameType::Settings: {
            if ((flags & kFlagAck) == 0) {
                FrameWriter ack;
                ack.settings_ack();
                std::lock_guard lock(send_mtx);
                if (!send_all(fd, ack.buffer(), ssl))
                    return;
            }
            break;
        }

        case FrameType::Ping: {
            if ((flags & kFlagAck) == 0 && payload.size() == 8) {
                FrameWriter pong;
                pong.ping(true, read_be32(payload.data()) * 0x100000000ull + read_be32(payload.data() + 4));
                std::lock_guard lock(send_mtx);
                if (!send_all(fd, pong.buffer(), ssl))
                    return;
            }
            break;
        }

        case FrameType::Headers: {
            std::string_view block = payload;
            if ((flags & kFlagPadded) != 0) {
                if (block.empty())
                    return;
                const std::size_t pad = static_cast<unsigned char>(block.front());
                block.remove_prefix(1);
                if (pad > block.size())
                    return;
                block.remove_suffix(pad);
            }
            if ((flags & kFlagPriority) != 0) {
                if (block.size() < 5)
                    return;
                block.remove_prefix(5);
            }

            if ((flags & kFlagEndHeaders) == 0) {
                pending_blocks[stream_id].assign(block);
                break;
            }

            HeaderList headers;
            if (!decoder.decode(block, headers))
                return;
            H2TestRequest &request = requests[stream_id];
            apply_headers(request, std::move(headers));
            if ((flags & kFlagEndStream) != 0) {
                H2TestRequest req = std::move(request);
                requests.erase(stream_id);
                respond(stream_id, req);
            }
            break;
        }

        case FrameType::Continuation: {
            auto pending = pending_blocks.find(stream_id);
            if (pending == pending_blocks.end())
                return;
            pending->second.append(payload);
            if ((flags & kFlagEndHeaders) != 0) {
                HeaderList headers;
                if (!decoder.decode(pending->second, headers))
                    return;
                H2TestRequest &request = requests[stream_id];
                apply_headers(request, std::move(headers));
                pending_blocks.erase(pending);
                if ((flags & kFlagEndStream) != 0) {
                    H2TestRequest req = std::move(request);
                    requests.erase(stream_id);
                    respond(stream_id, req);
                }
            }
            break;
        }

        case FrameType::Data: {
            auto it = requests.find(stream_id);
            if (it == requests.end())
                break;
            std::string_view data = payload;
            if ((flags & kFlagPadded) != 0) {
                if (data.empty())
                    return;
                const std::size_t pad = static_cast<unsigned char>(data.front());
                data.remove_prefix(1);
                if (pad > data.size())
                    return;
                data.remove_suffix(pad);
            }
            it->second.body.append(data);

            if (length > 0) {
                FrameWriter update;
                update.window_update(stream_id, length);
                update.window_update(0, length);
                std::lock_guard lock(send_mtx);
                (void)send_all(fd, update.buffer(), ssl);
            }

            if ((flags & kFlagEndStream) != 0) {
                H2TestRequest req = std::move(it->second);
                requests.erase(it);
                respond(stream_id, req);
            }
            break;
        }

        case FrameType::RstStream:
            requests.erase(stream_id);
            pending_blocks.erase(stream_id);
            break;

        case FrameType::WindowUpdate:
            break;

        case FrameType::GoAway:
            return;

        default:
            break;
        }
    }
}

} // namespace

H2TestServer::H2TestServer(Handler handler)
    : handler_(std::move(handler))
{
    listen_fd_ = make_listener();
    port_ = listener_port(listen_fd_);
    thread_ = std::thread([this] { serve(); });
}

H2TestServer::~H2TestServer()
{
    stop_.store(true, std::memory_order_relaxed);
    if (listen_fd_ >= 0)
        (void)::shutdown(listen_fd_, SHUT_RDWR);
    if (thread_.joinable())
        thread_.join();
    if (listen_fd_ >= 0)
        ::close(listen_fd_);
}

void H2TestServer::serve()
{
    struct ClientWorker {
        std::thread th;
        std::atomic<bool> done{ false };
    };
    std::vector<std::unique_ptr<ClientWorker>> client_threads;

    while (!stop_.load(std::memory_order_relaxed)) {
        pollfd descriptor{};
        descriptor.fd = listen_fd_;
        descriptor.events = POLLIN;
        const int ready = ::poll(&descriptor, 1, 200);
        if (ready <= 0)
            continue;

        const int client = ::accept(listen_fd_, nullptr, nullptr);
        if (client < 0)
            continue;

        timeval timeout{ 60, 0 };
        (void)::setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        timeval recv_timeout{ 0, 200000 };
        (void)::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &recv_timeout, sizeof(recv_timeout));

        connections_.fetch_add(1, std::memory_order_relaxed);

        auto worker = std::make_unique<ClientWorker>();
        auto *w = worker.get();
        w->th = std::thread([this, client, w]() {
            run_frames(client, nullptr, handler_, stop_);
            ::close(client);
            w->done.store(true, std::memory_order_release);
        });
        client_threads.push_back(std::move(worker));

        std::erase_if(client_threads, [](const std::unique_ptr<ClientWorker> &item) {
            if (item->done.load(std::memory_order_acquire)) {
                if (item->th.joinable())
                    item->th.join();
                return true;
            }
            return false;
        });
    }

    for (auto &w : client_threads) {
        if (w->th.joinable())
            w->th.join();
    }
}

} // namespace realurl::test

#include <openssl/ssl.h>
#include <openssl/err.h>

namespace realurl::test
{
namespace
{

int select_h2_alpn(SSL *, const unsigned char **out, unsigned char *out_len,
                   const unsigned char *in, unsigned int in_len, void *)
{
    for (unsigned int i = 0; i < in_len;) {
        const unsigned int length = in[i++];
        if (length == 0 || i + length > in_len)
            break;
        if (length == 2 && in[i] == 'h' && in[i + 1] == '2') {
            *out = in + i;
            *out_len = 2;
            return SSL_TLSEXT_ERR_OK;
        }
        i += length;
    }
    return SSL_TLSEXT_ERR_NOACK;
}

} // namespace

void H2TlsTestServer::serve()
{
    SSL_CTX *ctx = SSL_CTX_new(TLS_method());
    if (ctx == nullptr)
        return;
    if (SSL_CTX_use_certificate_chain_file(ctx, cert_file_.c_str()) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, key_file_.c_str(), SSL_FILETYPE_PEM) != 1) {
        SSL_CTX_free(ctx);
        return;
    }
    SSL_CTX_set_alpn_select_cb(ctx, select_h2_alpn, nullptr);
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);

    struct ClientWorker {
        std::thread th;
        std::atomic<bool> done{ false };
    };
    std::vector<std::unique_ptr<ClientWorker>> client_threads;

    while (!stop_.load(std::memory_order_relaxed)) {
        pollfd descriptor{};
        descriptor.fd = listen_fd_;
        descriptor.events = POLLIN;
        if (::poll(&descriptor, 1, 200) <= 0)
            continue;

        const int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0)
            continue;

        auto worker = std::make_unique<ClientWorker>();
        auto *w = worker.get();
        w->th = std::thread([this, ctx, fd, w]() {
            SSL *ssl = SSL_new(ctx);
            SSL_set_fd(ssl, fd);
            SSL_set_accept_state(ssl);
            if (SSL_accept(ssl) == 1) {
                const unsigned char *proto = nullptr;
                unsigned int len = 0;
                SSL_get0_alpn_selected(ssl, &proto, &len);
                if (len == 2 && proto[0] == 'h' && proto[1] == '2')
                    connections_.fetch_add(1, std::memory_order_relaxed);
                run_frames(fd, ssl, handler_, stop_);
            }
            SSL_free(ssl);
            ::close(fd);
            w->done.store(true, std::memory_order_release);
        });
        client_threads.push_back(std::move(worker));

        std::erase_if(client_threads, [](const std::unique_ptr<ClientWorker> &item) {
            if (item->done.load(std::memory_order_acquire)) {
                if (item->th.joinable())
                    item->th.join();
                return true;
            }
            return false;
        });
    }

    for (auto &w : client_threads) {
        if (w->th.joinable())
            w->th.join();
    }

    SSL_CTX_free(ctx);
}

H2TlsTestServer::H2TlsTestServer(std::string cert_file, std::string key_file, Handler handler)
    : cert_file_(std::move(cert_file))
    , key_file_(std::move(key_file))
    , handler_(std::move(handler))
{
    listen_fd_ = make_listener();
    port_ = listener_port(listen_fd_);
    thread_ = std::thread([this] { serve(); });
}

H2TlsTestServer::~H2TlsTestServer()
{
    stop_.store(true, std::memory_order_relaxed);
    if (listen_fd_ >= 0)
        (void)::shutdown(listen_fd_, SHUT_RDWR);
    if (thread_.joinable())
        thread_.join();
    if (listen_fd_ >= 0)
        ::close(listen_fd_);
}

} // namespace realurl::test
