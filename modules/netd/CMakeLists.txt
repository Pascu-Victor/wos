cmake_minimum_required(VERSION 3.10)
project(netd_service CXX)

set(TARGET netd)
set(CXXFILES
    src/main.cpp
    ../journal/src/journal_lib.cpp
)

add_executable(${TARGET} ${CXXFILES})

target_compile_options(${TARGET} PRIVATE
    -O3
    -std=c++23
    -Wall
    -mcmodel=small
)

include_directories(src ../extern ../ ../extern/limine)

set_target_properties(${TARGET} PROPERTIES
    POSITION_INDEPENDENT_CODE OFF
    INTERPROCEDURAL_OPTIMIZATION ON
)

set_target_properties(${TARGET} PROPERTIES LINK_OPTIONS "")

target_link_options(${TARGET} PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-static"
    "-Wl,--no-pie"
    "-mcmodel=small"
    "-Wl,--no-dynamic-linker"
    "-Wl,-z,now"
)

target_link_libraries(${TARGET} PRIVATE
    c++abi
    c++
    c
    unwind
)
