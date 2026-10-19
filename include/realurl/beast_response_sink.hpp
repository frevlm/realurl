#pragma once

/// BeastResponseSink — adapts ResponseSink to a beast::tcp_stream.
///
/// Non-streaming: set_content() writes a complete HTTP/1.1 response
/// (status line, headers, Content-Length, body) to the socket.
///
/// Streaming: set_chunked_provider() writes the status line and headers
/// (with Transfer-Encoding: chunked), then synchronously invokes the
/// provider callback.  The provider writes body chunks through
/// BeastChunkedSink, which formats them as HTTP chunked encoding and
/// writes directly to the socket.
///
/// All methods must be called from the connection's io_context thread.
/// The provider callback runs synchronously inside set_chunked_provider()
/// and may block (e.g. reading from an upstream CURL stream).

#include "realurl/response_sink.hpp"

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>

#include <array>
#include <cstdio>
#include <functional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace realurl
{

namespace net = boost::asio;
namespace beast = boost::beast;

/// Writes HTTP chunked body data to a beast::tcp_stream.
/// Thread-commentary: must be called from the connection thread.
class BeastChunkedSink : public ChunkedSink {
public:
    explicit BeastChunkedSink(beast::tcp_stream &stream)
        : stream_(stream)
    {
    }

    /// Write one HTTP chunk: hex_size CRLF data CRLF.
    /// Returns false if the client has disconnected (write error).
    bool write(std::string_view data) override
    {
        // HTTP chunked encoding: <hex-size>\r\n<data>\r\n
        std::array<char, 32> size_buf{};
        int size_len = std::snprintf(size_buf.data(), size_buf.size(), "%zx\r\n", data.size());
        boost::system::error_code ec;

        net::write(stream_.socket(), net::buffer(size_buf.data(), static_cast<size_t>(size_len)), ec);
        if (ec)
            return false;

        net::write(stream_.socket(), net::buffer(data), ec);
        if (ec)
            return false;

        net::write(stream_.socket(), net::buffer("\r\n", 2), ec);
        return !ec;
    }

    /// Write the final chunk (0 CRLF CRLF) to signal end of body.
    void done() override
    {
        boost::system::error_code ec;
        net::write(stream_.socket(), net::buffer("0\r\n\r\n", 5), ec);
    }

private:
    beast::tcp_stream &stream_;
};

/// Adapts ResponseSink to a Beast TCP stream.
/// Accumulates status, reason, and headers; flushes on set_content()
/// or set_chunked_provider().
class BeastResponseSink : public ResponseSink {
public:
    explicit BeastResponseSink(beast::tcp_stream &stream)
        : stream_(stream)
    {
    }

    void set_status(int code) override
    {
        status_ = code;
    }

    int status() const override
    {
        return status_;
    }

    void set_reason(std::string_view r) override
    {
        reason_ = std::string{ r };
    }

    std::string reason() const override
    {
        return reason_;
    }

    void set_header(std::string_view name, std::string_view value) override
    {
        headers_.emplace_back(name, value);
    }

    std::string get_header(std::string_view name) const override
    {
        for (const auto &h : headers_) {
            if (h.first == name)
                return h.second;
        }
        return {};
    }

    /// Write a complete HTTP response (status line + headers + body) and
    /// close the connection.
    void set_content(std::string body, std::string_view content_type) override
    {
        std::ostringstream out;
        write_status_and_headers(out, content_type, "Content-Length", std::to_string(body.size()));
        out << "\r\n" << body;

        boost::system::error_code ec;
        net::write(stream_.socket(), net::buffer(out.str()), ec);
        sent_ = true;
    }

    /// Initiate chunked transfer encoding: write status line and headers,
    /// then synchronously invoke the provider.  The provider receives a
    /// BeastChunkedSink it can use to push body chunks.
    void set_chunked_provider(std::string_view content_type, std::function<void(ChunkedSink &)> provider) override
    {
        std::ostringstream out;
        write_status_and_headers(out, content_type, "Transfer-Encoding", "chunked");
        out << "\r\n";

        boost::system::error_code ec;
        net::write(stream_.socket(), net::buffer(out.str()), ec);
        if (ec)
            return;
        sent_ = true;

        BeastChunkedSink chunk_sink(stream_);
        provider(chunk_sink);
    }

private:
    /// Write the status line and accumulated headers, plus one final header
    /// (Content-Type / Content-Length / Transfer-Encoding) and
    /// Connection: close.
    void write_status_and_headers(std::ostringstream &out, std::string_view content_type, std::string_view final_name,
                                  std::string_view final_value)
    {
        out << "HTTP/1.1 " << status_ << ' ' << reason_ << "\r\n";
        for (const auto &h : headers_) {
            out << h.first << ": " << h.second << "\r\n";
        }
        out << "Content-Type: " << content_type << "\r\n"
            << final_name << ": " << final_value << "\r\n"
            << "Connection: close\r\n";
    }

    beast::tcp_stream &stream_;
    int status_ = 200;
    std::string reason_ = "OK";
    std::vector<std::pair<std::string, std::string>> headers_;
    bool sent_ = false;
};

} // namespace realurl
