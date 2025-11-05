cmake_minimum_required(VERSION 3.20)

# Reset environment variables that may have been set by wos-env.sh
# to prevent interference with system toolchain detection
unset(ENV{CC})
unset(ENV{CXX})
unset(ENV{LD})
unset(ENV{CFLAGS})
unset(ENV{CXXFLAGS})
unset(ENV{LDFLAGS})

# If the WOS environment saved the original PATH in WOS_ORIGINAL_PATH, restore it
# so this CMake run uses the user's original PATH rather than the WOS toolchain overlay.
if(DEFINED ENV{WOS_ORIGINAL_PATH})
    set(ENV{PATH} "$ENV{WOS_ORIGINAL_PATH}")
endif()

# Find clang/clang++ in system PATH only
find_program(CLANG_C_COMPILER clang)
find_program(CLANG_CXX_COMPILER clang++)

if(NOT CLANG_C_COMPILER OR NOT CLANG_CXX_COMPILER)
    message(FATAL_ERROR "clang and clang++ must be installed to build tools")
endif()

# Set Clang as the compiler using paths from find_program()
set(CMAKE_C_COMPILER ${CLANG_C_COMPILER} CACHE FILEPATH "C compiler" FORCE)
set(CMAKE_CXX_COMPILER ${CLANG_CXX_COMPILER} CACHE FILEPATH "C++ compiler" FORCE)

project(wos_tools
    VERSION 1.0
    DESCRIPTION "WOS development tools"
    LANGUAGES CXX)

# Enable color output for clang
add_compile_options(-fdiagnostics-color=always)

# Set the runtime output directory for all targets to build/bin directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_SOURCE_DIR}/build/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_SOURCE_DIR}/build/bin)

# Include all subdirectories that contain CMakeLists.txt files
# Exclude any directories named "build"
file(GLOB SUBDIRS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *)

foreach(SUBDIR ${SUBDIRS})
    if(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${SUBDIR})
        # Skip build directories
        if(NOT ${SUBDIR} STREQUAL "build")
            if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${SUBDIR}/CMakeLists.txt)
                message(STATUS "Adding subdirectory: ${SUBDIR}")
                add_subdirectory(${SUBDIR})
            endif()
        endif()
    endif()
endforeach()
