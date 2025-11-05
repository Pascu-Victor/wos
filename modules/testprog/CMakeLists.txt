cmake_minimum_required(VERSION 3.28)

project(testprog LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffreestanding -fno-stack-protector")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions -fno-rtti")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mno-red-zone -mcmodel=kernel")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mno-80387 -mno-mmx -mno-sse -mno-sse2")

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
add_executable(testprog ${SOURCES})

# # Linker settings
set_target_properties(testprog PROPERTIES
    LINK_FLAGS "-Wl,-T,${CMAKE_CURRENT_SOURCE_DIR}/linker.ld -static"
)

target_link_libraries(testprog)
