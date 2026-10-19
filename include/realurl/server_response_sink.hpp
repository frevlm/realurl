#pragma once

#include <realurl/response_sink.hpp>
#include <realurl/server.hpp>

#include <stdexcept>
#include <string>
#include <string_view>

namespace realurl
{

/// Adapts Server::Response to the ResponseSink interface used by gateway code.
/// Only fixed responses are supported; raw streaming must use
/// Server::Response::take_over().
class ServerResponseSink final : public ResponseSink
{
public:
    explicit ServerResponseSink(Server::Response &res)
        : res_(res)
    {
    }

    void set_status(int code) override
    {
        res_.set_status(code);
    }

    int status() const override
    {
        return res_.status;
    }

    /// The wire format always uses the canonical reason phrase, matching
    /// cpp-httplib.  Custom reasons are intentionally ignored.
    void set_reason(std::string_view) override
    {
    }

    std::string reason() const override
    {
        return {};
    }

    void set_header(std::string_view name, std::string_view value) override
    {
        res_.set_header(std::string{ name }, std::string{ value });
    }

    std::string get_header(std::string_view name) const override
    {
        return res_.get_header(name);
    }

    void set_content(std::string body, std::string_view content_type) override
    {
        res_.set_content(std::move(body), content_type);
    }

    void set_chunked_provider(std::string_view, std::function<void(ChunkedSink &)>) override
    {
        throw std::logic_error("realurl::Server supports only Response::take_over for streaming");
    }

private:
    Server::Response &res_;
};

} // namespace realurl
