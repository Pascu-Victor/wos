cmake_minimum_required(VERSION 3.28)

project(httpd LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

# Include paths
include_directories(
    ${CMAKE_SOURCE_DIR}/modules/defines
    ${CMAKE_SOURCE_DIR}/modules/kern/src
    ${CMAKE_SOURCE_DIR}/modules/kern/src/abi
    ${CMAKE_SOURCE_DIR}/modules/stdlib/src
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

target_link_libraries(httpd PRIVATE c++abi c++ c unwind)
