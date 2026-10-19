#pragma once

#include "realurl/response_sink.hpp"

#include <httplib.h>
#include <string>
#include <string_view>

namespace realurl
{

/// Adapts ChunkedSink to httplib::DataSink.
class HttplibChunkedSink : public ChunkedSink {
public:
    explicit HttplibChunkedSink(::httplib::DataSink &sink)
        : sink_(sink)
    {
    }

    bool write(std::string_view data) override
    {
        return sink_.write(data.data(), data.size());
    }

    void done() override
    {
        sink_.done();
    }

private:
    ::httplib::DataSink &sink_;
};

/// Adapts ResponseSink to httplib::Response.
class HttplibResponseSink : public ResponseSink {
public:
    explicit HttplibResponseSink(::httplib::Response &res)
        : res_(res)
    {
    }

    void set_status(int code) override
    {
        res_.status = code;
    }

    int status() const override
    {
        return res_.status;
    }

    void set_reason(std::string_view r) override
    {
        res_.reason = std::string{ r };
    }

    std::string reason() const override
    {
        return res_.reason;
    }

    void set_header(std::string_view name, std::string_view value) override
    {
        res_.set_header(std::string{ name }, std::string{ value });
    }

    std::string get_header(std::string_view name) const override
    {
        return res_.get_header_value(std::string{ name });
    }

    void set_content(std::string body, std::string_view content_type) override
    {
        res_.set_content(std::move(body), std::string{ content_type });
    }

    void set_chunked_provider(std::string_view content_type, std::function<void(ChunkedSink &)> provider) override
    {
        res_.set_chunked_content_provider(std::string{ content_type },
                                          [provider](size_t offset, ::httplib::DataSink &ds) -> bool {
                                              if (offset != 0)
                                                  return false;
                                              HttplibChunkedSink wrapped(ds);
                                              provider(wrapped);
                                              return true;
                                          });
    }

private:
    ::httplib::Response &res_;
};

} // namespace realurl
