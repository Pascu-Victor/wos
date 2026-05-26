cmake_minimum_required(VERSION 3.20)

project(testd LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -Wall -Wextra -fno-sanitize=safe-stack")

include_directories(
    ${CMAKE_SOURCE_DIR}/modules/defines
    ${CMAKE_SOURCE_DIR}/modules/kern/src
    ${CMAKE_SOURCE_DIR}/modules/kern/src/abi
    ${CMAKE_SOURCE_DIR}/modules/stdlib/src
)

add_executable(testd src/main.cpp)

set_target_properties(testd PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(testd c++abi c++ c unwind)
