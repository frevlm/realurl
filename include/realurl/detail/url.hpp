#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace realurl::detail
{

struct UrlParts {
    std::string scheme;
    std::string host;
    std::uint16_t port = 0;
    std::string path = "/";
    bool tls = false;

    /// host[:port], omitting the port when it is the scheme default.
    std::string authority() const;
};

/// Parse an absolute http:// or https:// URL. Throws std::invalid_argument.
UrlParts parse_url(std::string_view url);

} // namespace realurl::detail
