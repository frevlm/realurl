cmake_minimum_required(VERSION 3.20)

project(realurl VERSION 0.3.0 LANGUAGES C CXX)

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
option(REALURL_WITH_ZSTD "zstd compression of realurl/1 DATA frames" ON)

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# OpenSSL 3.x: TLS 1.3, ALPN, and (Linux) kernel TLS. BoringSSL is not used
# because it does not expose kTLS or SSL_sendfile.
# ---------------------------------------------------------------------------

if(APPLE AND NOT OPENSSL_ROOT_DIR)
  foreach(_openssl_prefix /opt/homebrew/opt/openssl@3 /usr/local/opt/openssl@3)
    if(EXISTS "${_openssl_prefix}/include/openssl/ssl.h")
      set(OPENSSL_ROOT_DIR "${_openssl_prefix}" CACHE PATH "" FORCE)
      break()
    endif()
  endforeach()
endif()

find_package(OpenSSL 3.0 REQUIRED)

if(OPENSSL_VERSION VERSION_LESS 3.0)
  message(FATAL_ERROR "OpenSSL 3.0+ is required (found ${OPENSSL_VERSION})")
endif()

if(EXISTS "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h")
  file(READ "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h" _realurl_opensslv)
  if(_realurl_opensslv MATCHES "LIBRESSL_VERSION_NUMBER")
    message(FATAL_ERROR "LibreSSL is not supported; install OpenSSL 3.x and set OPENSSL_ROOT_DIR")
  endif()
endif()

# ---------------------------------------------------------------------------
# zstd (optional): per-frame compression of native DATA frames. Without it the
# native transport never advertises compression and sends DATA unchanged.
# ---------------------------------------------------------------------------

set(REALURL_HAVE_ZSTD OFF)
if(REALURL_WITH_ZSTD)
  find_path(REALURL_ZSTD_INCLUDE_DIR zstd.h PATHS /opt/homebrew/include /usr/local/include)
  find_library(REALURL_ZSTD_LIBRARY NAMES zstd PATHS /opt/homebrew/lib /usr/local/lib)
  if(REALURL_ZSTD_INCLUDE_DIR AND REALURL_ZSTD_LIBRARY)
    add_library(realurl::zstd UNKNOWN IMPORTED)
    set_target_properties(realurl::zstd PROPERTIES
      IMPORTED_LOCATION "${REALURL_ZSTD_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${REALURL_ZSTD_INCLUDE_DIR}")
    set(REALURL_HAVE_ZSTD ON)
    message(STATUS "realurl: zstd found at ${REALURL_ZSTD_LIBRARY}")
  else()
    message(WARNING "realurl: zstd not found; building without native DATA compression")
  endif()
endif()

# ---------------------------------------------------------------------------
# core: transport, event loop, HTTP/2 client. No Boost, no libcurl.
# ---------------------------------------------------------------------------

add_library(realurl_core
  src/buffer_pool.cpp
  src/fd_writer.cpp
  src/rate_limit.cpp
  src/detail/event_loop.cpp
  src/detail/timer_wheel.cpp
  src/detail/socket.cpp
  src/detail/resolver.cpp
  src/detail/frame.cpp
  src/detail/hpack.cpp
  src/detail/native_codec.cpp
  src/detail/native_compression.cpp
  src/detail/url.cpp
  src/detail/connection.cpp
  src/detail/client.cpp
  src/detail/client_group.cpp
  src/detail/tls.cpp
  src/detail/tls_server.cpp
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
target_link_libraries(realurl_core PRIVATE OpenSSL::SSL OpenSSL::Crypto realurl_warnings)

if(REALURL_HAVE_ZSTD)
  target_link_libraries(realurl_core PRIVATE realurl::zstd)
  target_compile_definitions(realurl_core PRIVATE REALURL_HAVE_ZSTD=1)
endif()

if(REALURL_WITH_IO_URING)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "REALURL_WITH_IO_URING is only supported on Linux")
  endif()
  target_sources(realurl_core PRIVATE src/detail/uring.cpp src/detail/uring_transport.cpp)
  target_compile_definitions(realurl_core PRIVATE REALURL_WITH_IO_URING=1)
endif()

# ---------------------------------------------------------------------------
# server: HTTP/1.1 + HTTP/2 server on the realurl EventLoop
# ---------------------------------------------------------------------------

find_package(Boost 1.83 REQUIRED CONFIG)

add_library(realurl_picohttpparser STATIC
  third_party/picohttpparser/picohttpparser.c)
target_include_directories(realurl_picohttpparser PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/picohttpparser")

add_library(realurl_server
  src/server/server.cpp
  src/server/server_internal.cpp
  src/server/router.cpp
  src/server/http1_parser.cpp
  src/server/http1_connection.cpp
  src/server/proxy.cpp
  src/server/h2_server.cpp
  src/stream_pump_pool.cpp
)
add_library(realurl::server ALIAS realurl_server)
target_include_directories(realurl_server PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include>")
target_link_libraries(realurl_server PUBLIC realurl::core Boost::headers Threads::Threads)
target_link_libraries(realurl_server PRIVATE realurl_warnings realurl_picohttpparser)

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
