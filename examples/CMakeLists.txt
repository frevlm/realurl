add_executable(realurl_fetch fetch.cpp)
target_link_libraries(realurl_fetch PRIVATE realurl::core realurl_warnings)

add_executable(realurl_stream stream.cpp)
target_link_libraries(realurl_stream PRIVATE realurl::core realurl_warnings)

add_executable(realurl_server_example server.cpp)
target_link_libraries(realurl_server_example PRIVATE realurl::server realurl_warnings)
