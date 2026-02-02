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

# Linker settings
set_target_properties(httpd PROPERTIES
    LINK_FLAGS "-Wl,-T,${CMAKE_CURRENT_SOURCE_DIR}/linker.ld -static -Wl,-z,now"
)

target_link_libraries(httpd c++abi c++ c unwind)
