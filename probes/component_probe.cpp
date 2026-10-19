// Observational probes against unchanged production classes, not replacement implementations.
#include "realurl/body_feeder.hpp"
#include "realurl/stream_pump_pool.hpp"
#include <boost/asio/io_context.hpp>
#include <chrono>
#include <iostream>
#include <thread>

int main()
{
    realurl::BufferPool buffers(4);
    std::weak_ptr<realurl::BodyFeeder> weak_feeder;
    std::weak_ptr<realurl::BodySource> weak_source;
    {
        auto feeder = std::make_shared<realurl::BodyFeeder>(&buffers);
        auto source = feeder->make_source();
        weak_feeder = feeder;
        weak_source = source;
        feeder->feed_initial("payload");
        auto data = source->read();
        source->consume(data.chunk.size());
        feeder->cancel();
    }
    std::cout << "{\"probe\":\"body_ownership\",\"feeder_expired\":" << weak_feeder.expired()
              << ",\"source_expired\":" << weak_source.expired() << "}\n";
    // Explicitly break the observed cycle in this test process only.
    if (auto source = weak_source.lock()) {
        source->read = {};
        source->consume = {};
        source->peek = {};
    }

    auto &pool = realurl::stream_pump_pool();
    for (bool same_ip : {true, false}) {
        boost::asio::io_context io;
        auto work = boost::asio::make_work_guard(io);
        std::atomic<int> active{0}, peak{0}, completed{0};
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < 8; ++i) {
            auto gate = pool.submit(same_ip ? "127.0.0.1" : "127.0.0." + std::to_string(i + 1), [&] {
                int n = ++active;
                int previous = peak.load();
                while (previous < n && !peak.compare_exchange_weak(previous, n)) {}
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                --active;
            }, io.get_executor());
            gate->async_wait([&] { if (++completed == 8) work.reset(); });
        }
        io.run();
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now()-start).count();
        std::cout << "{\"probe\":\"pump_pool\",\"same_ip\":" << same_ip << ",\"tasks\":8,\"task_ms\":100,\"peak_active\":"
                  << peak << ",\"elapsed_ms\":" << ms << "}\n";
    }
    pool.shutdown();
}
