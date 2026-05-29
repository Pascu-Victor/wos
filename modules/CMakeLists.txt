cmake_minimum_required(VERSION 3.20)

set(CMAKE_C_COMPILER_WORKS ON)
set(CMAKE_CXX_COMPILER_WORKS ON)

# Set paths to host tools and target sysroot
set(HOST_PATH ${CMAKE_SOURCE_DIR}/toolchain/host)
set(SYSROOT_PATH ${CMAKE_SOURCE_DIR}/toolchain/sysroot)
set(CMAKE_C_COMPILER "${HOST_PATH}/bin/clang" CACHE INTERNAL "Path to clang")
set(CMAKE_CXX_COMPILER "${HOST_PATH}/bin/clang++" CACHE INTERNAL "Path to clang++")
set(CMAKE_LINKER_TYPE "LLD")

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
add_link_options(
    "--sysroot=${SYSROOT_PATH}"
    "-fuse-ld=lld"
    "-L${SYSROOT_PATH}/lib"
    "-static"
    "-std=c++23"
    "-flto"
)

# Set the linker to use from host tools
set(CMAKE_LINKER "${HOST_PATH}/bin/ld.lld" CACHE INTERNAL "Path to lld linker")

#set target
set(TARGET wos_modules)

#build wos related subprojects
add_subdirectory(kern)
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

# Aggregate target for all WOS modules
add_custom_target(wos_modules)
add_dependencies(wos_modules wos journal_lib init testprog testd netd httpd debugserver perf top memacc journal wkictl renderbench strace sftpserver)
