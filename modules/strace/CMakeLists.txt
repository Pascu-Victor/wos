cmake_minimum_required(VERSION 3.28)

project(strace LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

set(STRACE_SOURCES
    src/cli.cpp
    src/decode.cpp
    src/main.cpp
    src/output.cpp
    src/ptrace_client.cpp
    src/remote.cpp
    src/time_format.cpp
    src/trace.cpp
)

add_executable(strace ${STRACE_SOURCES})

target_include_directories(strace PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

set_target_properties(strace PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(strace c++abi c++ c unwind)
