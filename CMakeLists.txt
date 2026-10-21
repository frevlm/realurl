cmake_minimum_required(VERSION 3.20)

project(realurl VERSION 0.3.0 LANGUAGES CXX)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  message(FATAL_ERROR "realurl requires Clang (GCC's coroutine frontend ICEs on this codebase)")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/CompilerWarnings.cmake")

option(REALURL_BUILD_TESTS "Build unit tests" ON)
option(REALURL_BUILD_EXAMPLES "Build examples" ON)
option(REALURL_BUILD_BENCH "Build benchmarks" OFF)
option(REALURL_WITH_HTTPLIB "Provide the cpp-httplib response-sink adapter target" OFF)
option(REALURL_WITH_IO_URING "Use the io_uring completion transport for h2c (Linux)" OFF)

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# BoringSSL: TLS 1.3 + ALPN only. Fetched at a pinned commit because BoringSSL
# has no stable ABI and no system package.
# ---------------------------------------------------------------------------

include(FetchContent)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_Declare(boringssl
  GIT_REPOSITORY https://github.com/google/boringssl.git
  GIT_TAG b50cad27ee8013f241648ef6a7adf7f6f6b3d25b)
FetchContent_MakeAvailable(boringssl)

# ---------------------------------------------------------------------------
# core: transport, event loop, HTTP/2 client. No Boost, no libcurl.
# ---------------------------------------------------------------------------

add_library(realurl_core
  src/buffer_pool.cpp
  src/fd_writer.cpp
  src/rate_limit.cpp
  src/detail/event_loop.cpp
  src/detail/socket.cpp
  src/detail/resolver.cpp
  src/detail/frame.cpp
  src/detail/hpack.cpp
  src/detail/url.cpp
  src/detail/connection.cpp
  src/detail/client.cpp
  src/detail/client_group.cpp
  src/detail/tls.cpp
)

if(APPLE)
  target_sources(realurl_core PRIVATE src/detail/poller_kqueue.cpp)
elseif(UNIX)
  target_sources(realurl_core PRIVATE src/detail/poller_epoll.cpp)
endif()
add_library(realurl::core ALIAS realurl_core)

target_include_directories(realurl_core PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include>")
target_link_libraries(realurl_core PUBLIC Threads::Threads)
target_link_libraries(realurl_core PRIVATE ssl crypto realurl_warnings)

if(REALURL_WITH_IO_URING)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "REALURL_WITH_IO_URING is only supported on Linux")
  endif()
  target_sources(realurl_core PRIVATE src/detail/uring.cpp src/detail/uring_transport.cpp)
  target_compile_definitions(realurl_core PRIVATE REALURL_WITH_IO_URING=1)
endif()

# ---------------------------------------------------------------------------
# server: HTTP/1.1 server built on Boost.Beast
# ---------------------------------------------------------------------------

find_package(Boost 1.83 REQUIRED CONFIG)

add_library(realurl_server
  src/server.cpp
  src/stream_pump_pool.cpp
)
add_library(realurl::server ALIAS realurl_server)
target_include_directories(realurl_server PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include>")
target_link_libraries(realurl_server PUBLIC realurl::core Boost::headers Threads::Threads)
target_link_libraries(realurl_server PRIVATE realurl_warnings)

# ---------------------------------------------------------------------------
# optional cpp-httplib adapter (header only)
# ---------------------------------------------------------------------------

if(REALURL_WITH_HTTPLIB)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(REALURL_CPPHTTPLIB IMPORTED_TARGET cpp-httplib)

  add_library(realurl_httplib INTERFACE)
  add_library(realurl::httplib ALIAS realurl_httplib)
  target_include_directories(realurl_httplib INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:include>")

  if(REALURL_CPPHTTPLIB_FOUND)
    target_link_libraries(realurl_httplib INTERFACE realurl::core PkgConfig::REALURL_CPPHTTPLIB)
  else()
    find_package(httplib REQUIRED CONFIG)
    target_link_libraries(realurl_httplib INTERFACE realurl::core httplib::httplib)
  endif()
endif()

# ---------------------------------------------------------------------------
# tests, examples, benchmarks
# ---------------------------------------------------------------------------


if(REALURL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(REALURL_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(REALURL_BUILD_BENCH)
  add_subdirectory(bench)
endif()
