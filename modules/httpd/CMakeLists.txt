cmake_minimum_required(VERSION 3.28)

project(httpd LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

# Source files
set(SOURCES
    src/filesystem.cpp
    src/main.cpp
    src/request.cpp
    src/response.cpp
    src/routes.cpp
    src/server.cpp
    src/socket_io.cpp
    src/time.cpp
)

# Create executable
add_executable(httpd ${SOURCES})

target_include_directories(httpd PRIVATE
    include
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

# Build as dynamic PIE (override parent's -static)
set_target_properties(httpd PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION ON
)
set_target_properties(httpd PROPERTIES LINK_OPTIONS "")

target_link_options(httpd PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${WOS_SYSROOT_LIB_DIR}"
    "-Wl,-z,now"
)

target_link_libraries(httpd PRIVATE journal_lib c++abi c++ c unwind)
