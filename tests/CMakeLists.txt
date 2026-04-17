cmake_minimum_required(VERSION 3.20)
project(wos_tests CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------

# WOS kernel source (the code under test)
set(WOS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." CACHE PATH "WOS repository root")
set(WOS_KERN_SRC "${WOS_ROOT}/modules/kern/src")

# Host shim headers (shadow kernel headers with host-compatible implementations)
set(KERN_SHIM_DIR "${CMAKE_CURRENT_SOURCE_DIR}/host/kern_shim")

# Include order is critical: shim headers first, then kernel source.
# This way, #include <platform/sys/spinlock.hpp> resolves to the shim,
# while #include <net/wki/wire.hpp> resolves to the real kernel header.
set(WOS_TEST_INCLUDES
    ${KERN_SHIM_DIR}      # 1st: shim overrides (spinlock, kmalloc, dbg, etc.)
    ${WOS_KERN_SRC}       # 2nd: real kernel headers (wire.hpp, wki.hpp, etc.)
    ${WOS_KERN_SRC}/std   # 3rd: kernel's internal std supplements
    ${WOS_ROOT}/modules   # 4th: inter-module headers (defines/)
)

# ---------------------------------------------------------------------------
# Google Test (fetched via FetchContent)
# ---------------------------------------------------------------------------

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
)
# Prevent gtest from overriding our compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# ---------------------------------------------------------------------------
# Common compile options for all test targets
# ---------------------------------------------------------------------------

# Check if compiler supports fuzzer sanitizer (Clang only).
# We can't use check_cxx_compiler_flag because -fsanitize=fuzzer provides
# its own main(), conflicting with the test program.  Instead, check for
# the fuzzer runtime library directly.
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_FLAGS "-fsanitize=fuzzer-no-link")
check_cxx_source_compiles("int main() { return 0; }" HAS_FUZZER_SANITIZER)
unset(CMAKE_REQUIRED_FLAGS)

# ---------------------------------------------------------------------------
# Helper: add a unit test target
# ---------------------------------------------------------------------------

function(wos_add_unit_test name)
    cmake_parse_arguments(ARG "" "" "SOURCES;KERN_SOURCES" ${ARGN})

    add_executable(${name} ${ARG_SOURCES} ${ARG_KERN_SOURCES})
    target_include_directories(${name} PRIVATE ${WOS_TEST_INCLUDES})
    target_link_libraries(${name} PRIVATE GTest::gtest_main)
    target_compile_options(${name} PRIVATE
        -g -O1 -fno-omit-frame-pointer
        -fsanitize=address,undefined
        -fno-sanitize=function,vptr
        -Wall -Wextra
    )
    target_link_options(${name} PRIVATE -fsanitize=address,undefined)

    add_test(NAME ${name} COMMAND ${name})
endfunction()

# ---------------------------------------------------------------------------
# Helper: add a LibFuzzer fuzz target
# ---------------------------------------------------------------------------

function(wos_add_fuzz_target name)
    if(NOT HAS_FUZZER_SANITIZER)
        message(STATUS "Skipping fuzz target ${name} — compiler lacks -fsanitize=fuzzer")
        return()
    endif()

    cmake_parse_arguments(ARG "" "" "SOURCES;KERN_SOURCES" ${ARGN})

    add_executable(${name} ${ARG_SOURCES} ${ARG_KERN_SOURCES})
    target_include_directories(${name} PRIVATE ${WOS_TEST_INCLUDES})
    target_compile_options(${name} PRIVATE
        -g -O1 -fno-omit-frame-pointer
        -fsanitize=address,undefined,fuzzer
        -fno-sanitize=function,vptr
        -Wall -Wextra
    )
    target_link_options(${name} PRIVATE -fsanitize=address,undefined,fuzzer)
endfunction()

# ---------------------------------------------------------------------------
# Enable CTest
# ---------------------------------------------------------------------------

enable_testing()

# ---------------------------------------------------------------------------
# Subdirectories
# ---------------------------------------------------------------------------

add_subdirectory(host)
