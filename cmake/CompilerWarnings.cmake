add_library(realurl_warnings INTERFACE)
target_compile_options(realurl_warnings INTERFACE
  -Wall
  -Wextra
  -Wpedantic
)
