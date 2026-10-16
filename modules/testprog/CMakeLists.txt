cmake_minimum_required(VERSION 3.28)
project(testprog LANGUAGES CXX)


option(WOS_MANDELBENCH_DEBUG "Enable verbose mandelbench CPU instrumentation" OFF)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -flto -Wall -Wextra -Werror -fno-sanitize=safe-stack")

# Include paths
include_directories(
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

# Source files
set(SOURCES
    src/aslr_hardening.cpp
    src/asan_crasher.cpp
    src/blockbench.cpp
    src/cowbench.cpp
    src/credentials.cpp
    src/fsbench.cpp
    src/ipv6_net.cpp
    src/ipv6_ra.cpp
    src/main.cpp
    src/memory_pressure.cpp
    src/netbench.cpp
    src/numa_locality.cpp
    src/numa_bench.cpp
    src/perfbench.cpp
    src/resource_limits.cpp
    src/thread_exit_stress.cpp
    src/tests/blockbench_mmap.cpp
    src/usercopy_negative.cpp
    src/vfs_coherency.cpp
    src/vfs_coherency_recovery.cpp
    src/wki_chaos_workload.cpp
    src/xattr_matrix.cpp
    src/mandelbench/lodepng.cpp
    src/mandelbench/mandelbench.cpp
    src/mandelbench/mandelbench_wki.cpp
    src/mandelbench/util.cpp
    src/mandelbench/tinycthread.cpp
)

# Create executable
add_executable(testprog ${SOURCES})

# A static entrypoint is needed to test exec with RLIMIT_NOFILE=0: a dynamic
# linker's own library opens correctly consume user descriptors.
foreach(probe IN ITEMS resource_exec_probe resource_core_probe resource_stack_probe)
    add_executable(${probe} src/${probe}.cpp)
    target_compile_options(${probe} PRIVATE -ffreestanding -fno-builtin -fno-exceptions -fno-rtti -fno-stack-protector -fno-pie)
    target_link_options(${probe} PRIVATE -nostdlib -static -Wl,-no-pie -Wl,-e,_start -Wl,--no-dynamic-linker)
    add_dependencies(testprog ${probe})
endforeach()

if(WOS_MANDELBENCH_DEBUG)
    target_compile_definitions(testprog PRIVATE MANDELBENCH_DEBUG=1)
else()
    target_compile_definitions(testprog PRIVATE MANDELBENCH_DEBUG=0)
endif()

# Linker settings
set_target_properties(testprog PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,-z,relro -Wl,--no-dynamic-linker"
)

target_link_libraries(testprog c++abi c++ c unwind)
