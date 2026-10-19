#include "realurl/stream_pump_pool.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <utility>

namespace realurl
{
namespace
{

size_t hash_client_ip(std::string_view ip)
{
    std::size_t h = 1469598103934665603ull; // FNV-1a
    for (char c : ip) {
        h ^= static_cast<unsigned char>(c);
        h *= 1099511628211ull;
    }
    return h;
}

} // namespace

StreamPumpPool &stream_pump_pool()
{
    return StreamPumpPool::instance();
}

StreamPumpPool &StreamPumpPool::instance()
{
    static StreamPumpPool pool;
    return pool;
}

StreamPumpPool::StreamPumpPool()
{
    const unsigned hw = std::thread::hardware_concurrency();
    const size_t count = static_cast<size_t>(std::clamp<unsigned>(hw == 0 ? 4 : hw * 4, 4, 64));
    size_ = count;
    workers_.reserve(count);
    threads_.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        auto worker = std::make_unique<Worker>();
        threads_.emplace_back([worker = worker.get(), this] {
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(worker->mu);
                    worker->cv.wait(lock,
                                    [&] { return stop_.load(std::memory_order_relaxed) || !worker->tasks.empty(); });
                    if (worker->tasks.empty()) {
                        if (stop_.load(std::memory_order_relaxed))
                            break;
                        continue;
                    }
                    task = std::move(worker->tasks.front());
                    worker->tasks.pop_front();
                }
                task();
            }
        });
        workers_.push_back(std::move(worker));
    }
}

StreamPumpPool::~StreamPumpPool()
{
    shutdown();
}

void StreamPumpPool::shutdown()
{
    if (stop_.exchange(true, std::memory_order_relaxed))
        return;
    for (const auto &worker : workers_) {
        std::lock_guard<std::mutex> lock(worker->mu);
        worker->cv.notify_all();
    }
    for (auto &thread : threads_) {
        if (thread.joinable())
            thread.join();
    }
    threads_.clear();
}

std::shared_ptr<StreamGate> StreamPumpPool::submit(std::string_view client_ip, std::function<void()> task,
                                                   boost::asio::any_io_executor io_ex)
{
    auto gate = StreamGate::create(std::move(io_ex));
    Worker &worker = *workers_[hash_client_ip(client_ip) % size_];
    {
        std::lock_guard<std::mutex> lock(worker.mu);
        worker.tasks.push_back([gate, task = std::move(task)]() mutable {
            try {
                task();
            } catch (const std::exception &err) {
                std::cerr << "pump task failed: " << err.what() << '\n';
            } catch (...) {
                std::cerr << "pump task failed: unknown\n";
            }
            gate->finish();
        });
    }
    worker.cv.notify_one();
    return gate;
}

} // namespace realurl
