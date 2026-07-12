cmake_minimum_required(VERSION 3.28)
project(powerctl LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

add_executable(powerctl src/main.cpp)

set_target_properties(powerctl PROPERTIES
    LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
)

target_link_libraries(powerctl c++abi c++ c unwind)
