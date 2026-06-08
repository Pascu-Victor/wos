cmake_minimum_required(VERSION 3.20)

set(CMAKE_C_COMPILER_WORKS ON)
set(CMAKE_CXX_COMPILER_WORKS ON)

# Set paths to host tools and target sysroot
set(HOST_PATH ${CMAKE_SOURCE_DIR}/toolchain/host)
set(WOS_SYSROOT_PATH "${CMAKE_SOURCE_DIR}/toolchain/sysroot" CACHE PATH "WOS target sysroot")
set(SYSROOT_PATH ${WOS_SYSROOT_PATH})
set(CMAKE_C_COMPILER "${HOST_PATH}/bin/clang" CACHE INTERNAL "Path to clang")
set(CMAKE_CXX_COMPILER "${HOST_PATH}/bin/clang++" CACHE INTERNAL "Path to clang++")
set(CMAKE_LINKER_TYPE "LLD")
option(WOS_USE_CCACHE "Use ccache/cccache as a compiler launcher when available" ON)
set(WOS_CCACHE_PROGRAM "" CACHE FILEPATH "Path to ccache/cccache")
if(WOS_USE_CCACHE AND NOT WOS_CCACHE_PROGRAM)
    find_program(WOS_CCACHE_PROGRAM NAMES ccache cccache)
endif()
if(WOS_USE_CCACHE AND WOS_CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${WOS_CCACHE_PROGRAM}" CACHE STRING "C compiler launcher" FORCE)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${WOS_CCACHE_PROGRAM}" CACHE STRING "C++ compiler launcher" FORCE)
endif()

# Configure sysroot for cross-compilation
set(CMAKE_SYSROOT "${SYSROOT_PATH}")
set(CMAKE_FIND_ROOT_PATH "${SYSROOT_PATH}")
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(wos_modules C CXX ASM_NASM)

# Disable C++ modules support which requires experimental compiler features
set(CMAKE_CXX_SCAN_FOR_MODULES OFF CACHE BOOL "Disable C++ modules" FORCE)

option(WOS_UASAN "Enable AddressSanitizer instrumentation for WOS userspace modules" ON)
set(WOS_UASAN_RUNTIME_DIR "" CACHE PATH "Clang runtime directory containing WOS AddressSanitizer libraries")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(
    # Common flags for C and C++
    "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-O3;-g;-flto;-fdiagnostics-color=always;-fno-stack-protector;-fno-stack-check;-fno-sanitize=safe-stack;-pipe;-Wall;-Wextra;-pedantic;-Wno-gnu-anonymous-struct;-Wno-c99-extensions;-Wno-zero-length-array;-m64;-march=native;-Wformat;-Wformat-security;-Werror=format-security;--sysroot=${SYSROOT_PATH};-fno-omit-frame-pointer>"
    # C-specific flags
    "$<$<COMPILE_LANGUAGE:C>:-std=gnu11>"
    # C++-specific flags
    "$<$<COMPILE_LANGUAGE:CXX>:-std=c++23;-isystem;${SYSROOT_PATH}/include/c++/v1>"
    # NASM flags
    "$<$<COMPILE_LANGUAGE:ASM_NASM>:-g;-Wall;-felf64;-Isrc;-w-reloc-abs-qword;-w-reloc-rel-dword>"
)

# Linker flags to use sysroot libraries
set(WOS_BASE_LINK_OPTIONS
    "--sysroot=${SYSROOT_PATH}"
    "-fuse-ld=lld"
    "-L${SYSROOT_PATH}/lib"
    "-std=c++23"
    "-flto"
)
if(NOT WOS_UASAN)
    list(APPEND WOS_BASE_LINK_OPTIONS "-static")
endif()
add_link_options(${WOS_BASE_LINK_OPTIONS})

# Set the linker to use from host tools
set(CMAKE_LINKER "${HOST_PATH}/bin/ld.lld" CACHE INTERNAL "Path to lld linker")

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
    renderbench
    strace
    sftpserver
)
set(WOS_USERSPACE_LOGGING_TARGETS
    init
    testprog
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
add_subdirectory(renderbench)
add_subdirectory(strace)
add_subdirectory(sftpserver)

function(wos_strip_link_flag out_value in_value flag)
    string(REPLACE "${flag}" "" stripped_value "${in_value}")
    string(REGEX REPLACE "[ \t]+" " " stripped_value "${stripped_value}")
    string(STRIP "${stripped_value}" stripped_value)
    set(${out_value} "${stripped_value}" PARENT_SCOPE)
endfunction()

function(wos_make_target_dynamic_for_asan user_target)
    get_target_property(existing_link_flags ${user_target} LINK_FLAGS)
    if(NOT existing_link_flags)
        set(existing_link_flags "")
    endif()
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-static")
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-no-pie")
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-Wl,--no-dynamic-linker")
    set_target_properties(${user_target} PROPERTIES
        LINK_FLAGS "${existing_link_flags}"
        POSITION_INDEPENDENT_CODE ON
    )

    get_target_property(existing_link_options ${user_target} LINK_OPTIONS)
    if(existing_link_options)
        list(REMOVE_ITEM existing_link_options
            "-static"
            "-no-pie"
            "-Wl,--no-dynamic-linker"
            "-Wl,-T,${CMAKE_SOURCE_DIR}/modules/init/service.ld"
        )
        set_target_properties(${user_target} PROPERTIES LINK_OPTIONS "${existing_link_options}")
    endif()
endfunction()

if(WOS_UASAN)
    foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
        if(TARGET ${user_target})
            wos_make_target_dynamic_for_asan(${user_target})
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

# Aggregate target for all WOS modules
add_custom_target(wos_modules)
add_dependencies(wos_modules wos journal_lib init testprog testd netd httpd debugserver perf top memacc journal wkictl renderbench strace sftpserver)
