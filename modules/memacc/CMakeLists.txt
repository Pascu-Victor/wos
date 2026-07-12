cmake_minimum_required(VERSION 3.28)
project(memacc LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

include_directories(
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
    ${WOS_MODULES_DIR}/stdlib/src
)

set(SOURCES
    src/commands.cpp
    src/main.cpp
    src/options.cpp
    src/processes.cpp
    src/procfs_io.cpp
    src/reports.cpp
    src/rows.cpp
)

add_executable(memacc ${SOURCES})

set_target_properties(memacc PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(memacc c++abi c++ c unwind)
