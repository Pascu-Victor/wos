cmake_minimum_required(VERSION 3.28)

project(strace LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

include_directories(
    ${CMAKE_SOURCE_DIR}/modules/defines
    ${CMAKE_SOURCE_DIR}/modules/kern/src
    ${CMAKE_SOURCE_DIR}/modules/kern/src/abi
    ${CMAKE_SOURCE_DIR}/modules/stdlib/src
)

add_executable(strace src/main.cpp)

set_target_properties(strace PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(strace c++abi c++ c unwind)
