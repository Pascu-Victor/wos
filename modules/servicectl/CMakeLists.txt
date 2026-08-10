cmake_minimum_required(VERSION 3.28)
project(servicectl LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(servicectl src/main.cpp)

target_compile_options(servicectl PRIVATE
    -Wall
    -Wextra
    -Werror
    -fno-sanitize=safe-stack
)

set_target_properties(servicectl PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(servicectl PRIVATE c++abi c++ c unwind)
