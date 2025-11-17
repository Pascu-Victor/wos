cmake_minimum_required(VERSION 3.10)
project(init_service CXX)

set(TARGET init)
# Build only the entry source; exclude any local ABI shims
set(CXXFILES src/init.cpp)

add_executable(${TARGET} ${CXXFILES})

# Set target-specific compile options to ensure -O0 is respected
target_compile_options(${TARGET} PRIVATE
    -g
    -O0
    -std=c++23
    -Wall
    -mcmodel=small
)

include_directories(src ../extern ../ ../extern/limine)

# Do not include kernel source headers; rely exclusively on toolchain/sysroot headers

set_target_properties(${TARGET} PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION OFF
)

# Clear the global link options and set our own
set_target_properties(${TARGET} PROPERTIES LINK_OPTIONS "")

# Use static linking with custom linker script
target_link_options(${TARGET} PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-static"
    "-mcmodel=small"
    "-Wl,-z,now"        # bind-now: resolve all PLT entries at load time
    "-Wl,-T,${CMAKE_SOURCE_DIR}/modules/init/service.ld"
)

target_link_libraries(${TARGET} PRIVATE
    c++abi
    c++
    c
    unwind
)
