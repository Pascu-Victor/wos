cmake_minimum_required(VERSION 3.20)

set(CMAKE_C_COMPILER_WORKS ON)
set(CMAKE_CXX_COMPILER_WORKS ON)

#llvm in tools/llvm-build/bin
#set path to llvm
set(LLVM_PATH ${CMAKE_SOURCE_DIR}/tools/llvm-build/bin)
set(CMAKE_C_COMPILER "${LLVM_PATH}/clang" CACHE INTERNAL "Path to clang")
set(CMAKE_CXX_COMPILER "${LLVM_PATH}/clang++" CACHE INTERNAL "Path to clang++")
set(CMAKE_LINKER_TYPE "LLD")

project(wos C CXX ASM_NASM)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(
    # Common flags for C and C++
    "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-O3;-fdiagnostics-color=always;-pipe;-Wall;-Wextra;-nostdinc;-ffreestanding;-fno-stack-protector;-fno-stack-check;-fno-lto;-fPIE;-m64;-march=x86-64;-mno-80387;-mno-mmx;-mno-sse;-mno-sse2;-mno-red-zone;-Wformat;-Wformat-security;-Werror=format-security>"
    # C-specific flags
    "$<$<COMPILE_LANGUAGE:C>:-std=gnu11>"
    # C++-specific flags
    "$<$<COMPILE_LANGUAGE:CXX>:-std=c++23;-fomit-frame-pointer>"
    # NASM flags
    "$<$<COMPILE_LANGUAGE:ASM_NASM>:-g;-Wall;-felf64;-Isrc;-w-reloc-abs-qword;-w-reloc-rel-dword>"
)

#set target
set(TARGET wos)

#build subprojects
add_subdirectory(modules/stdlib)
add_subdirectory(modules/kern)
add_subdirectory(modules/init)
