cmake_minimum_required(VERSION 3.20)

project(testd LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -Wall -Wextra -fno-sanitize=safe-stack")

include_directories(
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

set(SOURCES
    src/harness.cpp
    src/main.cpp
    src/registry.cpp
    src/remote_helper.cpp
    src/suite_ipc.cpp
    src/suite_journal.cpp
    src/suite_memory.cpp
    src/suite_misc.cpp
    src/suite_net.cpp
    src/suite_process.cpp
    src/suite_remote_ipc.cpp
    src/suite_vfs.cpp
    src/suite_wki.cpp
)

add_executable(testd ${SOURCES})

set_target_properties(testd PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(testd c++abi c++ c unwind)
