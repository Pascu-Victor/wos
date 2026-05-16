cmake_minimum_required(VERSION 3.28)

project(sftpserver LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -Werror -fno-sanitize=safe-stack")

add_executable(sftpserver src/main.cpp)
set_target_properties(sftpserver PROPERTIES OUTPUT_NAME "sftp-server")
set_target_properties(sftpserver PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(sftpserver c++abi c++ c unwind)
