#include "router.hpp"

#include <limits>
#include <utility>

namespace realurl::detail
{
namespace
{

int hex_value(char c) noexcept
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

std::vector<std::string> split_pattern(std::string_view path)
{
    std::vector<std::string> parts;
    std::size_t start = 0;
    for (;;) {
        const auto slash = path.find('/', start);
        if (slash == std::string_view::npos) {
            parts.emplace_back(path.substr(start));
            break;
        }
        parts.emplace_back(path.substr(start, slash - start));
        start = slash + 1;
    }
    return parts;
}

std::vector<std::string_view> split_path(std::string_view path)
{
    std::vector<std::string_view> parts;
    std::size_t start = 0;
    for (;;) {
        const auto slash = path.find('/', start);
        if (slash == std::string_view::npos) {
            parts.push_back(path.substr(start));
            break;
        }
        parts.push_back(path.substr(start, slash - start));
        start = slash + 1;
    }
    return parts;
}

bool match_params(const std::vector<std::string> &segments, std::string_view path,
                  std::unordered_map<std::string_view, std::string_view> &params)
{
    const std::vector<std::string_view> parts = split_path(path);
    if (parts.size() != segments.size())
        return false;

    std::unordered_map<std::string_view, std::string_view> captured;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        const std::string &segment = segments[i];
        if (segment.size() > 1 && segment[0] == ':') {
            if (parts[i].empty())
                return false;
            captured.emplace(std::string_view{ segment }.substr(1), parts[i]);
        } else if (segment != parts[i]) {
            return false;
        }
    }
    params = std::move(captured);
    return true;
}

} // namespace

void Router::add(std::string method, Kind kind, std::string pattern, Server::Handler handler,
                 Server::RouteOptions options)
{
    Route route;
    route.method = std::move(method);
    route.kind = kind;
    route.pattern = std::move(pattern);
    route.handler = std::move(handler);
    route.options = options;
    if (kind == Kind::Params)
        route.segments = split_pattern(route.pattern);
    if (kind == Kind::Regex)
        route.regex = std::regex{ route.pattern };
    routes_.push_back(std::move(route));
}

const Router::Route *Router::find(std::string_view method, std::string_view path,
                                  Server::Request &request) const
{
    auto method_matches = [&](const Route &route) {
        return route.method == method || (method == "HEAD" && route.method == "GET");
    };

    for (const Route &route : routes_) {
        if (route.kind == Kind::Exact && method_matches(route) && route.pattern == path)
            return &route;
    }
    for (const Route &route : routes_) {
        if (route.kind != Kind::Params || !method_matches(route))
            continue;
        std::unordered_map<std::string_view, std::string_view> params;
        if (match_params(route.segments, path, params)) {
            request.path_params = std::move(params);
            return &route;
        }
    }
    for (const Route &route : routes_) {
        if (route.kind == Kind::Regex && method_matches(route) &&
            std::regex_match(std::string{ path }, route.regex))
            return &route;
    }
    return nullptr;
}

std::string_view reason_phrase(int status) noexcept
{
    switch (status) {
    case 100:
        return "Continue";
    case 200:
        return "OK";
    case 201:
        return "Created";
    case 202:
        return "Accepted";
    case 204:
        return "No Content";
    case 206:
        return "Partial Content";
    case 301:
        return "Moved Permanently";
    case 302:
        return "Found";
    case 304:
        return "Not Modified";
    case 400:
        return "Bad Request";
    case 401:
        return "Unauthorized";
    case 402:
        return "Payment Required";
    case 403:
        return "Forbidden";
    case 404:
        return "Not Found";
    case 405:
        return "Method Not Allowed";
    case 408:
        return "Request Timeout";
    case 409:
        return "Conflict";
    case 413:
        return "Payload Too Large";
    case 415:
        return "Unsupported Media Type";
    case 422:
        return "Unprocessable Entity";
    case 426:
        return "Upgrade Required";
    case 429:
        return "Too Many Requests";
    case 431:
        return "Request Header Fields Too Large";
    case 500:
        return "Internal Server Error";
    case 501:
        return "Not Implemented";
    case 502:
        return "Bad Gateway";
    case 503:
        return "Service Unavailable";
    case 504:
        return "Gateway Timeout";
    default:
        return "Status";
    }
}

std::string_view target_path(std::string_view target) noexcept
{
    const auto end = target.find_first_of("?#");
    return target.substr(0, end == std::string_view::npos ? target.size() : end);
}

std::string percent_decode(std::string_view input)
{
    std::string out;
    out.reserve(input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '%' && i + 2 < input.size()) {
            const int hi = hex_value(input[i + 1]);
            const int lo = hex_value(input[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>(hi * 16 + lo));
                i += 2;
                continue;
            }
        }
        out.push_back(input[i]);
    }
    return out;
}

long long parse_content_length(std::string_view text) noexcept
{
    if (text.empty())
        return -1;
    long long value = 0;
    for (char c : text) {
        if (c < '0' || c > '9')
            return -1;
        if (value > (std::numeric_limits<long long>::max() - (c - '0')) / 10)
            return -1;
        value = value * 10 + (c - '0');
    }
    return value;
}

} // namespace realurl::detail
