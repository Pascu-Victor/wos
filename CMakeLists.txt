cmake_minimum_required(VERSION 3.20)

set(CMAKE_C_COMPILER_WORKS ON)
set(CMAKE_CXX_COMPILER_WORKS ON)

# Set path to sysroot toolchain
set(SYSROOT_PATH ${CMAKE_SOURCE_DIR}/toolchain/target1)
set(CMAKE_C_COMPILER "${SYSROOT_PATH}/bin/clang" CACHE INTERNAL "Path to clang")
set(CMAKE_CXX_COMPILER "${SYSROOT_PATH}/bin/clang++" CACHE INTERNAL "Path to clang++")
set(CMAKE_LINKER_TYPE "LLD")

# Configure sysroot for cross-compilation
set(CMAKE_SYSROOT "${SYSROOT_PATH}")
set(CMAKE_FIND_ROOT_PATH "${SYSROOT_PATH}")
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(wos C CXX ASM_NASM)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(
    # Common flags for C and C++
    "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-O1;-g;-fdiagnostics-color=always;-pipe;-Wall;-Wextra;-fno-stack-protector;-fno-stack-check;-fno-lto;-mno-sse;-m64;-march=x86-64;-mno-red-zone;-Wformat;-Wformat-security;-Werror=format-security;--sysroot=${SYSROOT_PATH};-fno-sanitize=safe-stack;-fno-omit-frame-pointer>"
    # C-specific flags
    "$<$<COMPILE_LANGUAGE:C>:-std=gnu11>"
    # C++-specific flags
    "$<$<COMPILE_LANGUAGE:CXX>:-std=c++23>"
    # NASM flags
    "$<$<COMPILE_LANGUAGE:ASM_NASM>:-g;-Wall;-felf64;-Isrc;-w-reloc-abs-qword;-w-reloc-rel-dword>"
)

# Linker flags to use sysroot libraries
add_link_options(
    "--sysroot=${SYSROOT_PATH}"
    "-fuse-ld=lld"
    "-L${SYSROOT_PATH}/lib"
    "-static"
)

# Set the linker to use from sysroot
set(CMAKE_LINKER "${SYSROOT_PATH}/bin/ld.lld" CACHE INTERNAL "Path to lld linker")

#set target
set(TARGET wos)

#build subprojects
add_subdirectory(modules/kern)
add_subdirectory(modules/init)
