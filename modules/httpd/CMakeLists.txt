cmake_minimum_required(VERSION 3.28)

project(httpd LANGUAGES CXX)

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
    src/main.cpp
)

# Create executable
add_executable(httpd ${SOURCES})

# Build as dynamic PIE (override parent's -static)
set_target_properties(httpd PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION ON
)
set_target_properties(httpd PROPERTIES LINK_OPTIONS "")

target_link_options(httpd PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-Wl,-z,now"
)

target_link_libraries(httpd PRIVATE journal_lib c++abi c++ c unwind)
