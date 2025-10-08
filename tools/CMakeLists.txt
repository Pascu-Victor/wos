cmake_minimum_required(VERSION 3.20)

# Set Clang as the compiler
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

project(wos_tools
    VERSION 1.0
    DESCRIPTION "WOS development tools"
    LANGUAGES CXX)

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
