cmake_minimum_required(VERSION 3.10)
project(init_service CXX)

set(TARGET init)
file(GLOB_RECURSE CXXFILES src/*.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffreestanding -g -O0 -std=c++23 -mno-sse -mno-sse2 -Wall")

add_executable(${TARGET} ${CXXFILES})

target_link_libraries(${TARGET} std)

target_include_directories(${TARGET} PRIVATE ../stdlib/src)
target_include_directories(${TARGET} PRIVATE ../kern/src)
target_include_directories(${TARGET} PRIVATE ../)

set_target_properties(${TARGET} PROPERTIES LINK_FLAGS "-T${CMAKE_SOURCE_DIR}/modules/init/service.ld -static -nostdlib")

