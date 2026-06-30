cmake_minimum_required(VERSION 3.28)

project(wkictl LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(WKICTL_SOURCES
    src/cli.cpp
    src/dispatch.cpp
    src/main.cpp
    src/perf.cpp
    src/target.cpp
    src/vfs.cpp
    src/wosid.cpp
)

add_executable(wkictl ${WKICTL_SOURCES})

target_compile_options(wkictl PRIVATE
    -Wall
    -Wextra
    -Werror
    -fno-sanitize=safe-stack
)

target_include_directories(wkictl PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

set_target_properties(wkictl PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(wkictl PRIVATE c++abi c++ c unwind)
