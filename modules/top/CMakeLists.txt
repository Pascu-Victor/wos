cmake_minimum_required(VERSION 3.28)
project(top LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

set(SOURCES
    src/app.cpp
    src/io.cpp
    src/main.cpp
    src/parse.cpp
    src/procfs_reader.cpp
    src/render.cpp
    src/terminal.cpp
    src/types.cpp
)

add_executable(top ${SOURCES})

target_include_directories(top PRIVATE
    include
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

set_target_properties(top PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(top c++abi c++ c unwind)
