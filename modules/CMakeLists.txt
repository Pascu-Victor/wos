cmake_minimum_required(VERSION 3.20)

set(CMAKE_C_COMPILER_WORKS ON)
set(CMAKE_CXX_COMPILER_WORKS ON)

# Set paths to host tools and target sysroot. This file is used both from the
# repo root and directly via `cmake -S modules`, so derive paths from this file.
get_filename_component(WOS_REPO_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(WOS_MODULES_DIR "${CMAKE_CURRENT_LIST_DIR}")
set(HOST_PATH ${WOS_REPO_ROOT}/toolchain/host)
set(WOS_REPO_SYSROOT_PATH "${WOS_REPO_ROOT}/toolchain/sysroot")
set(WOS_NATIVE_INSTALLED_SYSROOT_PATH "/")

set(WOS_REPO_SYSROOT_READY OFF)
if(EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libc.a" AND
   EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libc++.a" AND
   EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libc++abi.a" AND
   EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libunwind.a" AND
   EXISTS "${WOS_REPO_SYSROOT_PATH}/include/c++/v1")
    set(WOS_REPO_SYSROOT_READY ON)
endif()

set(WOS_NATIVE_SYSTEM_BUILD OFF CACHE BOOL "Build modules with the native WOS system toolchain")
execute_process(
    COMMAND uname -s
    OUTPUT_VARIABLE WOS_HOST_UNAME
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if((DEFINED ENV{WOS_NATIVE_HOST} AND "$ENV{WOS_NATIVE_HOST}" STREQUAL "1") OR CMAKE_HOST_SYSTEM_NAME STREQUAL "WOS" OR
   WOS_HOST_UNAME STREQUAL "WOS")
    set(WOS_NATIVE_SYSTEM_BUILD ON CACHE BOOL "Build modules with the native WOS system toolchain" FORCE)
endif()

option(WOS_NATIVE_USE_INSTALLED_SYSROOT
       "For native WOS builds, prefer the installed / sysroot over the repo toolchain/sysroot"
       OFF)

if(WOS_NATIVE_SYSTEM_BUILD AND (WOS_NATIVE_USE_INSTALLED_SYSROOT OR NOT WOS_REPO_SYSROOT_READY))
    set(WOS_DEFAULT_SYSROOT_PATH "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
else()
    set(WOS_DEFAULT_SYSROOT_PATH "${WOS_REPO_SYSROOT_PATH}")
endif()
set(WOS_SYSROOT_PATH "${WOS_DEFAULT_SYSROOT_PATH}" CACHE PATH "WOS target sysroot")
if(WOS_NATIVE_SYSTEM_BUILD AND NOT WOS_NATIVE_USE_INSTALLED_SYSROOT AND WOS_REPO_SYSROOT_READY AND
   WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(WOS_SYSROOT_PATH "${WOS_REPO_SYSROOT_PATH}" CACHE PATH "WOS target sysroot" FORCE)
endif()
set(SYSROOT_PATH ${WOS_SYSROOT_PATH})
set(WOS_SYSROOT_LIB_DIR "${SYSROOT_PATH}")
cmake_path(APPEND WOS_SYSROOT_LIB_DIR "lib")
if(WOS_NATIVE_SYSTEM_BUILD)
    find_program(WOS_SYSTEM_C_COMPILER NAMES clang cc REQUIRED)
    find_program(WOS_SYSTEM_CXX_COMPILER NAMES clang++ c++ REQUIRED)
    find_program(WOS_SYSTEM_LINKER NAMES ld.lld lld REQUIRED)
    set(CMAKE_C_COMPILER "${WOS_SYSTEM_C_COMPILER}" CACHE FILEPATH "Path to native WOS C compiler" FORCE)
    set(CMAKE_CXX_COMPILER "${WOS_SYSTEM_CXX_COMPILER}" CACHE FILEPATH "Path to native WOS C++ compiler" FORCE)
else()
    set(CMAKE_C_COMPILER "${HOST_PATH}/bin/clang" CACHE INTERNAL "Path to clang" FORCE)
    set(CMAKE_CXX_COMPILER "${HOST_PATH}/bin/clang++" CACHE INTERNAL "Path to clang++" FORCE)
endif()
if(NOT WOS_NATIVE_SYSTEM_BUILD)
    set(CMAKE_LINKER_TYPE "LLD")
endif()
option(WOS_USE_CCACHE "Use ccache/cccache as a compiler launcher when available" ON)
set(WOS_CCACHE_PROGRAM "" CACHE FILEPATH "Path to ccache/cccache")
if(WOS_USE_CCACHE AND NOT WOS_CCACHE_PROGRAM)
    find_program(WOS_CCACHE_PROGRAM NAMES ccache cccache)
endif()
if(WOS_USE_CCACHE AND WOS_CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${WOS_CCACHE_PROGRAM}" CACHE STRING "C compiler launcher" FORCE)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${WOS_CCACHE_PROGRAM}" CACHE STRING "C++ compiler launcher" FORCE)
endif()

# Configure sysroot for cross-compilation. Native WOS builds can also reuse the
# checked-out repo sysroot when it is populated with the required runtime libs.
set(CMAKE_SYSROOT "${SYSROOT_PATH}")
if(NOT WOS_NATIVE_SYSTEM_BUILD OR NOT WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(CMAKE_FIND_ROOT_PATH "${SYSROOT_PATH}")
    set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
    set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
    set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(wos_modules C CXX ASM_NASM)

# Disable C++ modules support which requires experimental compiler features
set(CMAKE_CXX_SCAN_FOR_MODULES OFF CACHE BOOL "Disable C++ modules" FORCE)

option(WOS_UASAN "Enable AddressSanitizer instrumentation for WOS userspace modules" OFF)
set(WOS_UASAN_RUNTIME_DIR "" CACHE PATH "Clang runtime directory containing WOS AddressSanitizer libraries")
option(WOS_USERSPACE_COVERAGE "Enable LLVM source-based coverage instrumentation for WOS userspace modules" OFF)
option(WOS_USERSPACE_NO_AVX "Build WOS userspace without AVX for FPU-state diagnostics" OFF)
option(WOS_DYNAMIC_USERSPACE_PIE "Build dynamic WOS userspace executables as PIE" OFF)
option(WOS_DYNAMIC_USERSPACE_NO_PLT "Use GOT calls instead of PLT stubs for dynamic WOS userspace executables" OFF)

set(WOS_SYSROOT_COMPILE_OPTIONS "--sysroot=${SYSROOT_PATH}")
if(WOS_NATIVE_SYSTEM_BUILD AND WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(WOS_C_STDLIB_INCLUDE "/usr/include")
    set(WOS_CXX_STDLIB_INCLUDE "/usr/include/c++/v1")
    set(WOS_STDLIB_LINK_DIR "/usr/lib")
else()
    set(WOS_C_STDLIB_INCLUDE "${SYSROOT_PATH}/include")
    set(WOS_CXX_STDLIB_INCLUDE "${SYSROOT_PATH}/include/c++/v1")
    set(WOS_STDLIB_LINK_DIR "${WOS_SYSROOT_LIB_DIR}")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(WOS_USERSPACE_CPU_OPTIONS -march=native)
if(WOS_USERSPACE_NO_AVX)
    list(APPEND WOS_USERSPACE_CPU_OPTIONS -mno-avx -mno-avx2 -mno-fma -mno-f16c -fno-vectorize -fno-slp-vectorize)
endif()
add_compile_options(
    # Common flags for C and C++
    "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-O3;-g;-flto;-fdiagnostics-color=always;-fno-stack-protector;-fno-stack-check;-fno-sanitize=safe-stack;-pipe;-Wall;-Wextra;-pedantic;-Wno-gnu-anonymous-struct;-Wno-c99-extensions;-Wno-zero-length-array;-m64;-Wformat;-Wformat-security;-Werror=format-security;${WOS_SYSROOT_COMPILE_OPTIONS};-fno-omit-frame-pointer>"
    # C-specific flags
    "$<$<COMPILE_LANGUAGE:C>:-std=gnu11>"
    "$<$<COMPILE_LANGUAGE:C>:SHELL:-isystem ${WOS_C_STDLIB_INCLUDE}>"
    # C++-specific flags
    "$<$<COMPILE_LANGUAGE:CXX>:-std=c++23>"
    "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-isystem ${WOS_CXX_STDLIB_INCLUDE}>"
    "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-isystem ${WOS_C_STDLIB_INCLUDE}>"
    # NASM flags
    "$<$<COMPILE_LANGUAGE:ASM_NASM>:-g;-Wall;-felf64;-Isrc;-w-reloc-abs-qword;-w-reloc-rel-dword>"
)

# Linker flags to use sysroot libraries
set(WOS_BASE_LINK_OPTIONS
    "--sysroot=${SYSROOT_PATH}"
    "-fuse-ld=lld"
    "-L${WOS_STDLIB_LINK_DIR}"
    "-std=c++23"
    "-flto"
)
add_link_options(${WOS_BASE_LINK_OPTIONS})

# Set the linker to use from the selected toolchain
if(WOS_NATIVE_SYSTEM_BUILD)
    set(CMAKE_LINKER "${WOS_SYSTEM_LINKER}" CACHE FILEPATH "Path to native WOS lld linker" FORCE)
else()
    set(CMAKE_LINKER "${HOST_PATH}/bin/ld.lld" CACHE INTERNAL "Path to lld linker" FORCE)
endif()

#set target
set(TARGET wos_modules)

#build wos related subprojects
add_subdirectory(kern)

set(WOS_USERSPACE_TARGETS
    init
    testprog
    testd
    netd
    httpd
    debugserver
    perf
    top
    memacc
    journal
    journal_lib
    wkictl
    powerctl
    renderbench
    strace
    sftpserver
)
set(WOS_USERSPACE_TARGETS ${WOS_USERSPACE_TARGETS} PARENT_SCOPE)
set(WOS_USERSPACE_LOGGING_TARGETS
    init
)
set(WOS_DYNAMIC_USERSPACE_EXECUTABLE_TARGETS
    testprog
    testd
    httpd
    debugserver
    perf
    top
    memacc
    journal
    wkictl
    powerctl
    renderbench
    strace
    sftpserver
)

if(WOS_UASAN)
    if(NOT WOS_UASAN_RUNTIME_DIR)
        execute_process(
            COMMAND ${CMAKE_C_COMPILER} -print-runtime-dir
            OUTPUT_VARIABLE WOS_UASAN_RUNTIME_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY
        )
    endif()

    foreach(runtime_lib IN ITEMS libclang_rt.asan_static.a libclang_rt.asan.a libclang_rt.asan_cxx.a)
        if(NOT EXISTS "${WOS_UASAN_RUNTIME_DIR}/${runtime_lib}")
            message(FATAL_ERROR
                "WOS_UASAN requires ${runtime_lib} in ${WOS_UASAN_RUNTIME_DIR}. "
                "The current WOS toolchain must build and install the ASAN compiler-rt runtime first.")
        endif()
    endforeach()
endif()

add_subdirectory(init)
add_subdirectory(testprog)
add_subdirectory(testd)
add_subdirectory(netd)
add_subdirectory(httpd)
add_subdirectory(debugserver)
add_subdirectory(perf)
add_subdirectory(top)
add_subdirectory(memacc)
add_subdirectory(journal)
add_subdirectory(wkictl)
add_subdirectory(powerctl)
add_subdirectory(renderbench)
add_subdirectory(strace)
add_subdirectory(sftpserver)

foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
    if(TARGET ${user_target})
        target_compile_options(${user_target} PRIVATE
            "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:${WOS_USERSPACE_CPU_OPTIONS}>"
        )
    endif()
endforeach()

function(wos_strip_link_flag out_value in_value flag)
    string(REPLACE "${flag}" "" stripped_value "${in_value}")
    string(REGEX REPLACE "[ \t]+" " " stripped_value "${stripped_value}")
    string(STRIP "${stripped_value}" stripped_value)
    set(${out_value} "${stripped_value}" PARENT_SCOPE)
endfunction()

function(wos_make_target_dynamic_user_executable user_target)
    get_target_property(existing_link_flags ${user_target} LINK_FLAGS)
    if(NOT existing_link_flags)
        set(existing_link_flags "")
    endif()
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-static")
    if(WOS_DYNAMIC_USERSPACE_PIE)
        wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-no-pie")
    endif()
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-Wl,--no-dynamic-linker")
    set_target_properties(${user_target} PROPERTIES
        LINK_FLAGS "${existing_link_flags}"
        POSITION_INDEPENDENT_CODE ${WOS_DYNAMIC_USERSPACE_PIE}
        SKIP_BUILD_RPATH ON
        BUILD_RPATH ""
        INSTALL_RPATH ""
    )

    get_target_property(existing_link_options ${user_target} LINK_OPTIONS)
    if(existing_link_options)
        list(REMOVE_ITEM existing_link_options
            "-static"
            "-Wl,--no-dynamic-linker"
            "-Wl,-T,${WOS_MODULES_DIR}/init/service.ld"
        )
        if(WOS_DYNAMIC_USERSPACE_PIE)
            list(REMOVE_ITEM existing_link_options "-no-pie")
        endif()
        set_target_properties(${user_target} PROPERTIES LINK_OPTIONS "${existing_link_options}")
    endif()

    if(NOT WOS_DYNAMIC_USERSPACE_PIE)
        target_link_options(${user_target} PRIVATE "-Wl,--no-pie")
    endif()

    if(WOS_DYNAMIC_USERSPACE_NO_PLT)
        target_compile_options(${user_target} PRIVATE "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fno-plt>")
    endif()
endfunction()

foreach(user_target IN LISTS WOS_DYNAMIC_USERSPACE_EXECUTABLE_TARGETS)
    if(TARGET ${user_target})
        wos_make_target_dynamic_user_executable(${user_target})
    endif()
endforeach()

if(WOS_UASAN)
    foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
        if(TARGET ${user_target})
            wos_make_target_dynamic_user_executable(${user_target})
            target_compile_options(${user_target} PRIVATE
                "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize=address;-fno-omit-frame-pointer;-fno-optimize-sibling-calls>"
            )
            target_link_options(${user_target} PRIVATE -fsanitize=address)
            target_compile_definitions(${user_target} PRIVATE WOS_UASAN=1)
        endif()
    endforeach()

    foreach(user_target IN LISTS WOS_USERSPACE_LOGGING_TARGETS)
        if(TARGET ${user_target} AND TARGET journal_lib)
            set_property(TARGET ${user_target} APPEND PROPERTY LINK_LIBRARIES journal_lib)
        endif()
    endforeach()
endif()

if(WOS_USERSPACE_COVERAGE)
    foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
        if(TARGET ${user_target})
            set_target_properties(${user_target} PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
            target_compile_options(${user_target} PRIVATE
                "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fprofile-instr-generate;-fcoverage-mapping;-mllvm;-runtime-counter-relocation;-fno-omit-frame-pointer;-fno-lto>"
            )
            target_link_options(${user_target} PRIVATE
                -fprofile-instr-generate
                -fno-lto
            )
            target_compile_definitions(${user_target} PRIVATE WOS_USERSPACE_COVERAGE=1)
        endif()
    endforeach()
endif()

# Aggregate target for all WOS modules
add_custom_target(wos_modules)
add_dependencies(wos_modules wos journal_lib init testprog testd netd httpd debugserver perf top memacc journal wkictl powerctl renderbench strace sftpserver)
