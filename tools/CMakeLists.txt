cmake_minimum_required(VERSION 3.20)

# Reset environment variables that may have been set by wos-env.sh
# to prevent interference with system toolchain detection
unset(ENV{CC})
unset(ENV{CXX})
unset(ENV{LD})
unset(ENV{CFLAGS})
unset(ENV{CXXFLAGS})
unset(ENV{LDFLAGS})

# Resolve workspace root and path entries to remove from host compiler discovery.
get_filename_component(WOS_WORKSPACE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
set(WOS_TARGET_TOOLCHAIN_BIN "${WOS_WORKSPACE_ROOT}/toolchain/target1/bin")
set(WOS_HOST_TOOLS_BIN "${WOS_WORKSPACE_ROOT}/tools/build/bin")

# Reconstruct a clean host PATH so find_program() does not pick the target compiler
# from nearby overlaid paths.
if(DEFINED ENV{WOS_ORIGINAL_PATH} AND NOT "$ENV{WOS_ORIGINAL_PATH}" STREQUAL "")
    set(_wos_path_source "$ENV{WOS_ORIGINAL_PATH}")
else()
    set(_wos_path_source "$ENV{PATH}")
endif()

string(REPLACE ":" ";" _wos_path_entries "${_wos_path_source}")
set(_wos_clean_path_entries "")
foreach(_entry IN LISTS _wos_path_entries)
    if(NOT _entry STREQUAL ""
       AND NOT _entry STREQUAL "${WOS_TARGET_TOOLCHAIN_BIN}"
       AND NOT _entry STREQUAL "${WOS_HOST_TOOLS_BIN}")
        list(APPEND _wos_clean_path_entries "${_entry}")
    endif()
endforeach()

if(_wos_clean_path_entries)
    list(JOIN _wos_clean_path_entries ":" _wos_clean_path)
    set(ENV{PATH} "${_wos_clean_path}")
endif()

# Find host clang/clang++ from the cleaned PATH.
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

