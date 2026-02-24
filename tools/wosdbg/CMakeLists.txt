# WOS Debugger - Execution Log Viewer & Coredump Analyzer
cmake_minimum_required(VERSION 3.16)

project(wosdbg VERSION 2.0.0 LANGUAGES C CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

# Set compiler flags based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native -flto")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-flto -s")
elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG -march=native -mtune=native")
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG -Wall -Wextra -Wpedantic")
endif()

# Enable automatic MOC for Qt
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

# Find required Qt components
find_package(Qt6 REQUIRED COMPONENTS Core Widgets Network)

# Find other required libraries
find_package(PkgConfig REQUIRED)

# Find BFD library using pkg-config first
pkg_check_modules(BFD libbfd)

# Fallback to manual find if pkg-config fails
if(NOT BFD_FOUND)
    find_library(BFD_LIBRARY bfd)
    find_path(BFD_INCLUDE_DIR bfd.h)

    if(NOT BFD_LIBRARY OR NOT BFD_INCLUDE_DIR)
        message(FATAL_ERROR "BFD library not found. Please install binutils-dev or binutils-devel")
    endif()
else()
    set(BFD_LIBRARY ${BFD_LIBRARIES})
    set(BFD_INCLUDE_DIR ${BFD_INCLUDE_DIRS})
endif()

# Find Capstone library
pkg_check_modules(CAPSTONE REQUIRED capstone>=4.0)

# Add main executable
add_executable(wosdbg
    main.cpp
    wosdbg.h
    wosdbg.cpp
    log_processor.cpp
    log_server.cpp
    log_client.cpp
    config.h
    config.cpp
    virtual_table.h
    virtual_table.cpp
    virtual_table_integration.h
    virtual_table_integration.cpp
    coredump_parser.h
    coredump_parser.cpp
    elf_symbol_resolver.h
    elf_symbol_resolver.cpp
    coredump_memory.h
    coredump_memory.cpp
    coredump_browser.h
    coredump_browser.cpp
    coredump_register_panel.h
    coredump_register_panel.cpp
    coredump_segment_panel.h
    coredump_segment_panel.cpp
    coredump_memory_panel.h
    coredump_memory_panel.cpp
    coredump_elf_panel.h
    coredump_elf_panel.cpp
    capstone_disasm.h
    capstone_disasm.cpp
)


# Find LLVM for demangling support
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")

# Add worker executable
add_executable(wosdbg_worker
    log_worker.cpp
    config.cpp
    config.h
    capstone_disasm.h
    capstone_disasm.cpp
)

# Link libraries for main executable
target_link_libraries(wosdbg
    Qt6::Core
    Qt6::Widgets
    Qt6::Network
    ${CAPSTONE_LIBRARIES}
    LLVMDemangle
    iberty
    z
    dl
)

# Link libraries for worker executable
target_link_libraries(wosdbg_worker
    Qt6::Core
    ${BFD_LIBRARY}
    ${CAPSTONE_LIBRARIES}
    LLVMDemangle
    iberty
    z
    dl
)

# Include directories for main executable
target_include_directories(wosdbg PRIVATE
    ${CAPSTONE_INCLUDE_DIRS}
    ${LLVM_INCLUDE_DIRS}
)

# Include directories for worker executable
target_include_directories(wosdbg_worker PRIVATE
    ${BFD_INCLUDE_DIR}
    ${CAPSTONE_INCLUDE_DIRS}
    ${LLVM_INCLUDE_DIRS}
)

# Compiler definitions for main executable
target_compile_definitions(wosdbg PRIVATE
    ${CAPSTONE_CFLAGS_OTHER}
)

# Compiler definitions for worker executable
target_compile_definitions(wosdbg_worker PRIVATE
    ${CAPSTONE_CFLAGS_OTHER}
)

# Common compiler options for all targets
set(COMMON_COMPILE_OPTIONS
    -Wall
    -Wextra
    -Wformat=2
    -Wunused
    -Wcast-align
    -Wpointer-arith
    -Winit-self
    -Wredundant-decls
)

# Debug-specific options
set(DEBUG_COMPILE_OPTIONS
    ${COMMON_COMPILE_OPTIONS}
    -Wshadow
    -Wconversion
    -Wpedantic
    -fstack-protector-strong
)

# Release-specific options
set(RELEASE_COMPILE_OPTIONS
    ${COMMON_COMPILE_OPTIONS}
    -ffast-math
    -fomit-frame-pointer
)

# Apply compiler options based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(wosdbg PRIVATE ${DEBUG_COMPILE_OPTIONS})
    target_compile_options(wosdbg_worker PRIVATE ${DEBUG_COMPILE_OPTIONS})
else()
    target_compile_options(wosdbg PRIVATE ${RELEASE_COMPILE_OPTIONS})
    target_compile_options(wosdbg_worker PRIVATE ${RELEASE_COMPILE_OPTIONS})
endif()

# Enable interprocedural optimization for Release builds
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TARGET wosdbg PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    set_property(TARGET wosdbg_worker PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# Install targets
install(TARGETS wosdbg wosdbg_worker
    DESTINATION bin
)

# Create a desktop entry (optional)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/wosdbg.desktop.in"
    "${CMAKE_CURRENT_BINARY_DIR}/wosdbg.desktop"
    @ONLY
)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/wosdbg.desktop"
    DESTINATION share/applications
    OPTIONAL
)
