#include "realurl/detail/url.hpp"

#include <cctype>
#include <stdexcept>

namespace realurl::detail
{
namespace
{

std::uint16_t default_port(std::string_view scheme)
{
    return scheme == "https" ? 443 : 80;
}

} // namespace

std::string UrlParts::authority() const
{
    const bool ipv6 = host.find(':') != std::string::npos;
    std::string out;
    if (ipv6)
        out.append("[").append(host).append("]");
    else
        out.append(host);

    if (port != default_port(scheme))
        out.append(":").append(std::to_string(port));
    return out;
}

UrlParts parse_url(std::string_view url)
{
    const std::size_t scheme_end = url.find("://");
    if (scheme_end == std::string_view::npos)
        throw std::invalid_argument("url: missing scheme");

    UrlParts parts;
    parts.scheme.assign(url.substr(0, scheme_end));
    if (parts.scheme != "http" && parts.scheme != "https")
        throw std::invalid_argument("url: unsupported scheme");
    parts.tls = parts.scheme == "https";

    std::size_t pos = scheme_end + 3;
    const std::size_t authority_end = url.find_first_of("/?#", pos);
    const std::string_view authority = url.substr(pos, authority_end == std::string_view::npos
                                                           ? std::string_view::npos
                                                           : authority_end - pos);
    if (authority.empty())
        throw std::invalid_argument("url: missing host");

    std::string_view host_port = authority;
    const std::size_t at = host_port.rfind('@');
    if (at != std::string_view::npos)
        host_port.remove_prefix(at + 1);

    if (!host_port.empty() && host_port.front() == '[') {
        const std::size_t close = host_port.find(']');
        if (close == std::string_view::npos)
            throw std::invalid_argument("url: malformed ipv6 host");
        parts.host.assign(host_port.substr(1, close - 1));
        if (close + 1 < host_port.size()) {
            if (host_port[close + 1] != ':')
                throw std::invalid_argument("url: malformed authority");
            const std::string_view port_text = host_port.substr(close + 2);
            const int value = std::stoi(std::string(port_text));
            if (value <= 0 || value > 65535)
                throw std::invalid_argument("url: bad port");
            parts.port = static_cast<std::uint16_t>(value);
        }
    } else {
        const std::size_t colon = host_port.rfind(':');
        if (colon != std::string_view::npos) {
            parts.host.assign(host_port.substr(0, colon));
            const std::string_view port_text = host_port.substr(colon + 1);
            const int value = std::stoi(std::string(port_text));
            if (value <= 0 || value > 65535)
                throw std::invalid_argument("url: bad port");
            parts.port = static_cast<std::uint16_t>(value);
        } else {
            parts.host.assign(host_port);
        }
    }

    if (parts.host.empty())
        throw std::invalid_argument("url: missing host");
    if (parts.port == 0)
        parts.port = default_port(parts.scheme);

    pos = authority_end == std::string_view::npos ? url.size() : authority_end;
    if (pos < url.size() && url[pos] != '/')
        parts.path = "/" + std::string(url.substr(pos));
    else if (pos < url.size())
        parts.path.assign(url.substr(pos));
    else
        parts.path = "/";

    const std::size_t fragment = parts.path.find('#');
    if (fragment != std::string::npos)
        parts.path.erase(fragment);

    if (parts.path.empty())
        parts.path = "/";

    return parts;
}

} // namespace realurl::detail
