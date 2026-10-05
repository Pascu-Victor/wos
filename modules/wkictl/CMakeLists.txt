cmake_minimum_required(VERSION 3.28)
project(wkictl LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(WKICTL_LAUNCH_SOURCES
    src/cli.cpp
    src/forward.cpp
    src/launch.cpp
    src/launch_dispatch.cpp
    src/wosid.cpp
)

set(WKICTL_SOURCES
    src/auth.cpp
    src/chaos.cpp
    src/dispatch.cpp
    src/main.cpp
    src/peer_resolver.cpp
    src/perf.cpp
    src/target.cpp
    src/vfs.cpp
)

add_executable(wkictl ${WKICTL_SOURCES} ${WKICTL_LAUNCH_SOURCES})
add_executable(wki_run src/launch_main.cpp ${WKICTL_LAUNCH_SOURCES})
set_target_properties(wki_run PROPERTIES OUTPUT_NAME "wki-run")

foreach(wkictl_target IN ITEMS wkictl wki_run)
    target_compile_options(${wkictl_target} PRIVATE
        -Wall
        -Wextra
        -Werror
        -fno-sanitize=safe-stack
    )

    target_include_directories(${wkictl_target} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${WOS_MODULES_DIR}/defines
        ${WOS_MODULES_DIR}/kern/src
        ${WOS_MODULES_DIR}/kern/src/abi
        ${WOS_MODULES_DIR}/stdlib/src
    )

    set_target_properties(${wkictl_target} PROPERTIES
        LINK_FLAGS "-Wl,-z,now -Wl,--no-dynamic-linker"
    )
endforeach()

target_link_libraries(wkictl PRIVATE c++abi c++ c unwind)
# Shared launch handlers use header-only C++ types and libc; do not load the
# management utility's C++ runtime merely to set policy and exec a command.
target_link_options(wki_run PRIVATE -nostdlib++ --unwindlib=none)
target_link_libraries(wki_run PRIVATE c)

if(WOS_UASAN)
    # The diagnostic C++ ASan runtime can need the exception/unwind libraries.
    target_link_libraries(wki_run PRIVATE c++abi c++ unwind)
endif()
