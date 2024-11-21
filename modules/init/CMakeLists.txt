cmake_minimum_required(VERSION 3.10)
project(init_service CXX)


set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_LINKER ld.lld)

set(TARGET init)
file(GLOB_RECURSE CFILES RELATIVE ${CMAKE_SOURCE_DIR} "*.cpp")
list(FILTER CFILES EXCLUDE REGEX "CMakeFiles/.*")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ffreestanding -O2 -std=c++23 -mno-sse -mno-sse2 -Wall")

add_executable(${TARGET} ${CFILES})

target_link_directories(${TARGET} PRIVATE ../stdlib/bin)
target_link_libraries(${TARGET} std)

target_include_directories(${TARGET} PRIVATE ../stdlib/src)
target_include_directories(${TARGET} PRIVATE ../kern/src)
target_include_directories(${TARGET} PRIVATE ../)

set_target_properties(${TARGET} PROPERTIES LINK_FLAGS "-T../service.ld -static -nostdlib")
