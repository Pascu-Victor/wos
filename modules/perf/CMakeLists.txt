cmake_minimum_required(VERSION 3.28)

project(perf LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

# Include paths
include_directories(
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

# Source files
set(SOURCES
    src/args.cpp
    src/commands.cpp
    src/data.cpp
    src/main.cpp
    src/map.cpp
    src/run.cpp
    src/sched.cpp
    src/support.cpp
    src/wki.cpp
)

# Create executable
add_executable(perf ${SOURCES})

# Linker settings
set_target_properties(perf PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(perf c++abi c++ c unwind)
