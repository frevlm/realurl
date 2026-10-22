add_executable(realurl_bench bench.cpp)
target_include_directories(realurl_bench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../tests")
target_link_libraries(realurl_bench PRIVATE realurl::server realurl_test_support realurl_warnings)
