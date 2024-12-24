cmake_minimum_required(VERSION 3.20)

#llvm in tools/llvm-build/bin
#set path to llvm
set(LLVM_PATH ${CMAKE_SOURCE_DIR}/tools/llvm-build/bin)
set(CMAKE_C_COMPILER "${LLVM_PATH}/clang" CACHE INTERNAL "Path to clang")
set(CMAKE_CXX_COMPILER "${LLVM_PATH}/clang++" CACHE INTERNAL "Path to clang++")
set(CMAKE_LINKER_TYPE "LLD")

project(wos C CXX ASM_NASM)

#set target
set(TARGET wos)

#build subprojects
add_subdirectory(modules/stdlib)
add_subdirectory(modules/kern)
add_subdirectory(modules/init)
