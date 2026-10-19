cmake_minimum_required(VERSION 3.20)

project(realurl VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerWarnings)

option(REALURL_BUILD_TESTS "Build unit tests" ON)
option(REALURL_BUILD_EXAMPLES "Build examples" ON)
option(REALURL_BUILD_PROBES "Build measurement probes" OFF)
option(REALURL_WITH_HTTPLIB "Provide the cpp-httplib response-sink adapter target" OFF)

find_package(CURL REQUIRED)
find_package(Boost 1.83 REQUIRED CONFIG)
find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# core: transport primitives with no HTTP-server dependency
# ---------------------------------------------------------------------------

add_library(realurl_core
  src/buffer_pool.cpp
  src/curl_multi_pool.cpp
  src/fd_writer.cpp
  src/rate_limit.cpp
  src/stream_pump_pool.cpp
)
add_library(realurl::core ALIAS realurl_core)

target_include_directories(realurl_core PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include>")

target_link_libraries(realurl_core PUBLIC
  CURL::libcurl
  Boost::headers
  Threads::Threads
)
target_link_libraries(realurl_core PRIVATE realurl_warnings)

# ---------------------------------------------------------------------------
# beast adapter (header only)
# ---------------------------------------------------------------------------

add_library(realurl_beast INTERFACE)
add_library(realurl::beast ALIAS realurl_beast)
target_include_directories(realurl_beast INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include>")
target_link_libraries(realurl_beast INTERFACE realurl::core Boost::headers)

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
# tests, examples, probes
# ---------------------------------------------------------------------------

if(REALURL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(REALURL_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(REALURL_BUILD_PROBES)
  add_subdirectory(probes)
endif()
