cmake_minimum_required(VERSION 3.28)

project(journal LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fno-sanitize=safe-stack")

include_directories(
    ${WOS_MODULES_DIR}/defines
    ${WOS_MODULES_DIR}/kern/src
    ${WOS_MODULES_DIR}/kern/src/abi
)

add_executable(journal src/main.cpp)

set_target_properties(journal PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INTERPROCEDURAL_OPTIMIZATION ON
)
set_target_properties(journal PROPERTIES LINK_OPTIONS "")

target_link_options(journal PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-Wl,-z,now"
)

target_link_libraries(journal PRIVATE c++abi c++ c unwind)

add_library(journal_lib SHARED src/journal_lib.cpp)
set_target_properties(journal_lib PROPERTIES
    OUTPUT_NAME journal
    POSITION_INDEPENDENT_CODE ON
)
set_target_properties(journal_lib PROPERTIES LINK_OPTIONS "")

target_link_options(journal_lib PRIVATE
    "--sysroot=${CMAKE_SYSROOT}"
    "-fuse-ld=lld"
    "-L${CMAKE_SYSROOT}/lib"
    "-Wl,-z,now"
)

target_link_libraries(journal_lib PRIVATE c)

add_custom_command(TARGET journal_lib POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy "$<TARGET_FILE:journal_lib>" "${CMAKE_SYSROOT}/lib/libjournal.so"
    COMMENT "Installing libjournal.so into sysroot"
)
