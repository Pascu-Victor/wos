cmake_minimum_required(VERSION 3.28)

project(renderbench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native -flto -Wall -Wextra -Werror -fno-sanitize=safe-stack")

include_directories(
    ${CMAKE_SOURCE_DIR}/modules/defines
    ${CMAKE_SOURCE_DIR}/modules/kern/src
    ${CMAKE_SOURCE_DIR}/modules/kern/src/abi
    ${CMAKE_SOURCE_DIR}/modules/stdlib/src
    ${CMAKE_SOURCE_DIR}/modules/testprog/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

add_executable(renderbench
    src/main.cpp
    src/render_core.cpp
    ${CMAKE_SOURCE_DIR}/modules/testprog/src/mandelbench/lodepng.cpp
    ${CMAKE_SOURCE_DIR}/modules/testprog/src/mandelbench/tinycthread.cpp
)

set_target_properties(renderbench PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(renderbench c++abi c++ c unwind)
