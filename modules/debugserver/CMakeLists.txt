cmake_minimum_required(VERSION 3.28)
project(debugserver LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

include_directories(
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

add_executable(debugserver src/main.cpp)

set_target_properties(debugserver PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION ON
)
set_target_properties(debugserver PROPERTIES LINK_OPTIONS "")

target_link_options(debugserver PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${WOS_SYSROOT_LIB_DIR}"
    "-Wl,-z,now"
)

target_link_libraries(debugserver PRIVATE journal_lib c++abi c++ c unwind)
