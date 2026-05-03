cmake_minimum_required(VERSION 3.28)

project(journal LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

include_directories(
    ${CMAKE_SOURCE_DIR}/modules/defines
    ${CMAKE_SOURCE_DIR}/modules/kern/src
    ${CMAKE_SOURCE_DIR}/modules/kern/src/abi
)

add_executable(journal src/main.cpp)

set_target_properties(journal PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION ON
)
set_target_properties(journal PROPERTIES LINK_OPTIONS "")

target_link_options(journal PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-Wl,-z,now"
)

target_link_libraries(journal PRIVATE c++abi c++ c unwind)
