#pragma once

#include "realurl/server.hpp"

#include <regex>
#include <string>
#include <string_view>
#include <vector>

namespace realurl::detail
{

class Router
{
public:
    enum class Kind { Exact, Params, Regex };

    struct Route
    {
        std::string method;
        Kind kind = Kind::Exact;
        std::string pattern;
        std::vector<std::string> segments;
        std::regex regex;
        Server::Handler handler;
        Server::RouteOptions options;
    };

    void add(std::string method, Kind kind, std::string pattern, Server::Handler handler,
             Server::RouteOptions options);

    const Route *find(std::string_view method, std::string_view path, Server::Request &request) const;

    const std::vector<Route> &routes() const noexcept
    {
        return routes_;
    }

private:
    std::vector<Route> routes_;
};

std::string_view reason_phrase(int status) noexcept;
std::string_view target_path(std::string_view target) noexcept;
std::string percent_decode(std::string_view input);
long long parse_content_length(std::string_view text) noexcept;

} // namespace realurl::detail
