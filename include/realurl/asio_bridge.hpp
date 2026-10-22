#pragma once

/// Bridge between Boost.Asio and the realurl HTTP/2 client.
///
/// realurl::Client and ClientGroup run their coroutines on realurl
/// EventLoops; a take_over_async handler runs on a Boost.Asio io thread.
/// async_request() dispatches the upstream request to a ClientGroup worker
/// and completes on the caller's associated executor, so the io thread is
/// never blocked while the upstream request is in flight.
///
/// The ClientGroup must outlive the operation. The upstream response is
/// buffered before completion; cancellation of the waiting handler is not
/// propagated to the worker, which finishes the request and then discards
/// the result.

#include "realurl/http2.hpp"

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace realurl
{
namespace detail
{

/// Runs on a ClientGroup worker; the coroutine frame owns every argument, so
/// no lambda closure has to outlive the task.
template <class Finish>
Task<void> bridge_request_job(ClientGroup &group, std::string method, std::string url, Headers headers,
                              std::string body, std::chrono::milliseconds timeout, Finish finish)
{
    std::exception_ptr error;
    Response response;
    try {
        response = co_await group.request(std::move(method), std::move(url), std::move(headers), std::move(body),
                                          timeout);
    } catch (...) {
        error = std::current_exception();
    }
    finish(error, std::move(response));
    co_return;
}

} // namespace detail

/// Completion-token form. The completion signature is
/// void(std::exception_ptr, Response); with use_awaitable the exception is
/// rethrown at the co_await site.
template <class CompletionToken>
auto async_request(ClientGroup &group, std::string method, std::string url, Headers headers, std::string body,
                   std::chrono::milliseconds timeout, CompletionToken &&token)
{
    return boost::asio::async_initiate<CompletionToken, void(std::exception_ptr, Response)>(
        [&group, method = std::move(method), url = std::move(url), headers = std::move(headers),
         body = std::move(body), timeout](auto handler) mutable {
            auto boxed = std::make_shared<std::decay_t<decltype(handler)>>(std::move(handler));
            auto executor = boost::asio::get_associated_executor(*boxed);
            // The request runs on a realurl EventLoop, so asio has no
            // outstanding operation of its own. Keep the target executor alive
            // until the completion has run, otherwise io_context::run() can
            // return while the bridge is still in flight.
            auto work = std::make_shared<decltype(boost::asio::make_work_guard(executor))>(executor);

            auto finish = [boxed, executor, work](std::exception_ptr error, Response response) mutable {
                boost::asio::post(executor, [boxed, work, error, response = std::move(response)]() mutable {
                    (*boxed)(error, std::move(response));
                });
            };

            try {
                // Pass a copy of finish: if spawn() throws (closed group), the
                // coroutine frame is destroyed and the moved-from captures in
                // this scope would no longer be usable for the error post.
                group.spawn(detail::bridge_request_job(group, std::move(method), std::move(url),
                                                       std::move(headers), std::move(body), timeout, finish));
            } catch (...) {
                finish(std::current_exception(), Response{});
            }
        },
        token);
}

/// Coroutine form: co_await async_request(group, "GET", url).
inline boost::asio::awaitable<Response> async_request(ClientGroup &group, std::string method, std::string url,
                                                      Headers headers = {}, std::string body = {},
                                                      std::chrono::milliseconds timeout = {})
{
    co_return co_await async_request(group, std::move(method), std::move(url), std::move(headers),
                                     std::move(body), timeout, boost::asio::use_awaitable);
}

} // namespace realurl
