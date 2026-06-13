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
if(NOT HAS_FUZZER_SANITIZER)
    message(FATAL_ERROR "Host fuzz smoke tests require compiler support for -fsanitize=fuzzer-no-link")
endif()

# ---------------------------------------------------------------------------
# Helper: add a unit test target
# ---------------------------------------------------------------------------

option(WOS_HOST_TEST_DISABLE_LSAN "Disable LeakSanitizer for host CTest runs that execute under ptrace-style wrappers" ON)
set(WOS_HOST_FUZZ_SMOKE_RUNS "16" CACHE STRING "Number of LibFuzzer iterations for host CTest smoke runs")
set(WOS_HOST_FUZZ_SMOKE_MAX_LEN "256" CACHE STRING "Maximum generated input size for host CTest fuzz smoke runs")

function(wos_add_unit_test name)
    cmake_parse_arguments(ARG "" "" "SOURCES;KERN_SOURCES" ${ARGN})

    add_executable(${name} ${ARG_SOURCES} ${ARG_KERN_SOURCES})
    target_include_directories(${name} PRIVATE ${WOS_TEST_INCLUDES})
    target_link_libraries(${name} PRIVATE GTest::gtest_main)
    target_compile_options(${name} PRIVATE
        -g -O1 -fno-omit-frame-pointer
        -fsanitize=address,undefined
        -fno-sanitize=vptr
        -Wall -Wextra
    )
    target_link_options(${name} PRIVATE -fsanitize=address,undefined)

    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES LABELS "host;unit")
    if(WOS_HOST_TEST_DISABLE_LSAN)
        set_tests_properties(${name} PROPERTIES
            ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0;LSAN_OPTIONS=detect_leaks=0"
        )
    endif()
endfunction()

# ---------------------------------------------------------------------------
# Helper: add a LibFuzzer fuzz target
# ---------------------------------------------------------------------------

function(wos_add_fuzz_target name)
    cmake_parse_arguments(ARG "" "" "SOURCES;KERN_SOURCES" ${ARGN})

    add_executable(${name} ${ARG_SOURCES} ${ARG_KERN_SOURCES})
    target_include_directories(${name} PRIVATE ${WOS_TEST_INCLUDES})
    target_compile_options(${name} PRIVATE
        -g -O1 -fno-omit-frame-pointer
        -fsanitize=address,undefined,fuzzer
        -fno-sanitize=vptr
        -Wall -Wextra
    )
    target_link_options(${name} PRIVATE -fsanitize=address,undefined,fuzzer)

    set(fuzz_artifact_dir "${CMAKE_CURRENT_BINARY_DIR}/fuzz-artifacts")
    file(MAKE_DIRECTORY "${fuzz_artifact_dir}")

    add_test(
        NAME ${name}_smoke
        COMMAND ${name}
                -runs=${WOS_HOST_FUZZ_SMOKE_RUNS}
                -max_len=${WOS_HOST_FUZZ_SMOKE_MAX_LEN}
                -artifact_prefix=${fuzz_artifact_dir}/${name}-
    )
    set_tests_properties(${name}_smoke PROPERTIES
        LABELS "host;fuzz;smoke"
        TIMEOUT 20
    )
    if(WOS_HOST_TEST_DISABLE_LSAN)
        set_tests_properties(${name}_smoke PROPERTIES
            ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0;LSAN_OPTIONS=detect_leaks=0"
        )
    endif()
endfunction()

# ---------------------------------------------------------------------------
# Enable CTest
# ---------------------------------------------------------------------------

enable_testing()

# ---------------------------------------------------------------------------
# Subdirectories
# ---------------------------------------------------------------------------

add_subdirectory(host)
