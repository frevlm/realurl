#include "realurl/detail/resolver.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <system_error>
#include <thread>
#include <utility>

namespace realurl::detail
{
namespace
{

struct ResolveResult {
    SocketAddress address;
    int error = 0;
    std::coroutine_handle<> handle{};
    std::atomic<int> state{ 0 }; // 0 pending, 1 worker, 2 timeout
};

bool parse_literal(const std::string &host, std::uint16_t port, SocketAddress &out)
{
    sockaddr_in v4{};
    if (::inet_pton(AF_INET, host.c_str(), &v4.sin_addr) == 1) {
        v4.sin_family = AF_INET;
        v4.sin_port = htons(port);
        std::memcpy(&out.storage, &v4, sizeof(v4));
        out.length = sizeof(v4);
        return true;
    }

    sockaddr_in6 v6{};
    if (::inet_pton(AF_INET6, host.c_str(), &v6.sin6_addr) == 1) {
        v6.sin6_family = AF_INET6;
        v6.sin6_port = htons(port);
        std::memcpy(&out.storage, &v6, sizeof(v6));
        out.length = sizeof(v6);
        return true;
    }

    return false;
}

} // namespace

struct Resolver::Impl {
    struct Job {
        std::string host;
        std::uint16_t port = 0;
        std::shared_ptr<ResolveResult> result;
    };

    EventLoop *loop = nullptr;
    std::thread worker;
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<Job> jobs;
    bool stopping = false;

    void start()
    {
        worker = std::thread([this] { run(); });
    }

    ~Impl()
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
        }
        cv.notify_all();
        if (worker.joinable())
            worker.join();
    }

    void submit(std::string host, std::uint16_t port, std::shared_ptr<ResolveResult> result)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            jobs.push_back({ std::move(host), port, std::move(result) });
        }
        cv.notify_one();
    }

    void run()
    {
        for (;;) {
            Job job;
            {
                std::unique_lock<std::mutex> lock(mutex);
                cv.wait(lock, [this] { return stopping || !jobs.empty(); });
                if (stopping)
                    return;
                job = std::move(jobs.front());
                jobs.pop_front();
            }
            execute(job);
        }
    }

    void execute(Job &job)
    {
        SocketAddress address{};
        int error = 0;

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        addrinfo *list = nullptr;
        const int rc = ::getaddrinfo(job.host.c_str(), nullptr, &hints, &list);
        if (rc != 0) {
            error = rc == EAI_AGAIN ? EAGAIN : (rc == EAI_NONAME ? ENOENT : EIO);
        } else {
            const addrinfo *chosen = nullptr;
            for (const addrinfo *it = list; it != nullptr; it = it->ai_next) {
                if (it->ai_family == AF_INET) {
                    chosen = it;
                    break;
                }
                if (chosen == nullptr && it->ai_family == AF_INET6)
                    chosen = it;
            }
            if (chosen == nullptr || chosen->ai_addrlen > sizeof(sockaddr_storage)) {
                error = EIO;
            } else {
                std::memcpy(&address.storage, chosen->ai_addr, chosen->ai_addrlen);
                address.length = static_cast<socklen_t>(chosen->ai_addrlen);
                if (address.storage.ss_family == AF_INET)
                    reinterpret_cast<sockaddr_in *>(&address.storage)->sin_port = htons(job.port);
                else if (address.storage.ss_family == AF_INET6)
                    reinterpret_cast<sockaddr_in6 *>(&address.storage)->sin6_port = htons(job.port);
            }
            ::freeaddrinfo(list);
        }

        job.result->address = address;
        job.result->error = error;

        if (job.result->state.exchange(1, std::memory_order_acq_rel) == 0) {
            EventLoop *loop_copy = loop;
            auto result = job.result;
            loop_copy->post([result] {
                if (result->handle)
                    result->handle.resume();
            });
        }
    }
};

namespace
{

class ResolveAwaiter {
public:
    ResolveAwaiter(Resolver::Impl *impl, std::string host, std::uint16_t port, std::shared_ptr<ResolveResult> result)
        : impl_(impl)
        , host_(std::move(host))
        , port_(port)
        , result_(std::move(result))
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        result_->handle = handle;
        impl_->submit(std::move(host_), port_, result_);
    }

    SocketAddress await_resume()
    {
        if (result_->error != 0)
            throw std::system_error(result_->error, std::generic_category(), "resolve");
        return result_->address;
    }

private:
    Resolver::Impl *impl_;
    std::string host_;
    std::uint16_t port_;
    std::shared_ptr<ResolveResult> result_;
};

} // namespace

Resolver::Resolver(EventLoop &loop)
    : impl_(std::make_unique<Impl>())
{
    impl_->loop = &loop;
    impl_->start();
}

Resolver::~Resolver() = default;

Task<SocketAddress> Resolver::resolve(std::string host, std::uint16_t port)
{
    SocketAddress literal{};
    if (parse_literal(host, port, literal))
        co_return literal;

    auto result = std::make_shared<ResolveResult>();
    const SocketAddress address =
        co_await ResolveAwaiter(impl_.get(), std::move(host), port, std::move(result));
    co_return address;
}

} // namespace realurl::detail
