#pragma once

#include <functional>
#include <string>
#include <string_view>

namespace realurl
{

/// Abstract sink for writing streaming response body chunks.
/// Replaces httplib::DataSink so gateway/proxy code does not depend on cpp-httplib types.
class ChunkedSink {
public:
    virtual ~ChunkedSink() = default;

    /// Write a chunk of response body data.
    /// @return true on success, false if the client has disconnected.
    virtual bool write(std::string_view data) = 0;

    /// Signal that all chunks have been written.
    virtual void done() = 0;
};

/// Abstract sink for writing an HTTP response (status, headers, body).
/// Replaces httplib::Response& so gateway/proxy code does not depend on cpp-httplib types.
class ResponseSink {
public:
    virtual ~ResponseSink() = default;

    virtual void set_status(int code) = 0;
    virtual int status() const = 0;

    virtual void set_reason(std::string_view reason) = 0;
    virtual std::string reason() const = 0;

    virtual void set_header(std::string_view name, std::string_view value) = 0;
    virtual std::string get_header(std::string_view name) const = 0;

    /// Set a fixed-size response body.
    virtual void set_content(std::string body, std::string_view content_type) = 0;

    /// Set a chunked transfer-encoding provider for streaming responses.
    /// The provider callback receives a ChunkedSink& to write chunks into.
    /// Unlike httplib's raw callback, there is no offset parameter and no bool return —
    /// the adapter handles the offset guard internally.
    virtual void set_chunked_provider(std::string_view content_type, std::function<void(ChunkedSink &)> provider) = 0;
};

} // namespace realurl
