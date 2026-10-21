#include "realurl/stream_pump_pool.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <utility>

namespace realurl
{


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
    threads_.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        threads_.emplace_back([this] {
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(mu_);
                    cv_.wait(lock, [&] { return stop_.load(std::memory_order_relaxed) || !tasks_.empty(); });
                    if (tasks_.empty()) {
                        if (stop_.load(std::memory_order_relaxed))
                            break;
                        continue;
                    }
                    task = std::move(tasks_.front());
                    tasks_.pop_front();
                }
                task();
            }
        });
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
    {
        std::lock_guard<std::mutex> lock(mu_);
        cv_.notify_all();
    }
    for (auto &thread : threads_) {
        if (thread.joinable())
            thread.join();
    }
    threads_.clear();
}

std::shared_ptr<StreamGate> StreamPumpPool::submit(std::function<void()> task, boost::asio::any_io_executor io_ex)
{
    auto gate = StreamGate::create(std::move(io_ex));
    {
        std::lock_guard<std::mutex> lock(mu_);
        tasks_.push_back([gate, task = std::move(task)]() mutable {
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
    cv_.notify_one();
    return gate;
}

} // namespace realurl
