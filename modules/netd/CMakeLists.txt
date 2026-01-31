cmake_minimum_required(VERSION 3.10)
project(netd_service CXX)

set(TARGET netd)
set(CXXFILES src/main.cpp)

add_executable(${TARGET} ${CXXFILES})

target_compile_options(${TARGET} PRIVATE
    -g
    -O2
    -std=c++23
    -Wall
    -mcmodel=small
)

include_directories(src ../extern ../ ../extern/limine)

set_target_properties(${TARGET} PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION OFF
)

set_target_properties(${TARGET} PROPERTIES LINK_OPTIONS "")

target_link_options(${TARGET} PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-static"
    "-mcmodel=small"
    "-Wl,-z,now"
    "-Wl,-T,${CMAKE_SOURCE_DIR}/modules/netd/linker.ld"
)

target_link_libraries(${TARGET} PRIVATE
    c++abi
    c++
    c
    unwind
)
