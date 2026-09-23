# WOS Debugger - Execution Log Viewer & Coredump Analyzer
cmake_minimum_required(VERSION 3.16)

project(wosdbg VERSION 2.0.0 LANGUAGES C CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
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
pkg_check_modules(LIBARCHIVE REQUIRED libarchive)

# Add main executable
add_library(wosdbg_core STATIC
    debugger_workspace.h
    debugger_workspace.cpp
    wosdbg.h
    wosdbg.cpp
    log_processor.cpp
    telemetry_log.h
    telemetry_log.cpp
    log_server.cpp
    log_client.cpp
    debug_analysis_service.h
    debug_analysis_service.cpp
    live_protocol.h
    live_protocol.cpp
    live_session_manager.h
    live_session_manager.cpp
    debug_cli.h
    debug_cli.cpp
    debug_tool_panel.h
    debug_tool_panel.cpp
    tool_arguments_form.h
    tool_arguments_form.cpp
    tool_result_view.h
    tool_result_view.cpp
    tool_presentation.h
    tool_presentation.cpp
    mcp_http_server.h
    mcp_http_server.cpp
    config.h
    config.cpp
    virtual_table.h
    virtual_table.cpp
    virtual_table_integration.h
    virtual_table_integration.cpp
    coredump_parser.h
    coredump_parser.cpp
    incident_bundle.h
    incident_bundle.cpp
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
    coredump_disasm_panel.h
    coredump_disasm_panel.cpp
    capstone_disasm.h
    capstone_disasm.cpp
)

add_executable(wosdbg main.cpp)
target_link_libraries(wosdbg PRIVATE wosdbg_core)


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
target_link_libraries(wosdbg_core PUBLIC
    Qt6::Core
    Qt6::Widgets
    Qt6::Network
    wos::telemetry
    ${CAPSTONE_LIBRARIES}
    ${LIBARCHIVE_LIBRARIES}
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
target_include_directories(wosdbg_core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../modules/kern/src
)
target_include_directories(wosdbg_core PUBLIC
    ${CAPSTONE_INCLUDE_DIRS}
    ${LIBARCHIVE_INCLUDE_DIRS}
    ${LLVM_INCLUDE_DIRS}
)

# Include directories for worker executable
target_include_directories(wosdbg_worker PRIVATE
    ${BFD_INCLUDE_DIR}
    ${CAPSTONE_INCLUDE_DIRS}
    ${LLVM_INCLUDE_DIRS}
)

# Compiler definitions for main executable
target_compile_definitions(wosdbg_core PUBLIC
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
    target_compile_options(wosdbg_core PRIVATE ${DEBUG_COMPILE_OPTIONS})
    target_compile_options(wosdbg_worker PRIVATE ${DEBUG_COMPILE_OPTIONS})
else()
    target_compile_options(wosdbg PRIVATE ${RELEASE_COMPILE_OPTIONS})
    target_compile_options(wosdbg_core PRIVATE ${RELEASE_COMPILE_OPTIONS})
    target_compile_options(wosdbg_worker PRIVATE ${RELEASE_COMPILE_OPTIONS})
endif()

# Enable interprocedural optimization for Release builds
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TARGET wosdbg PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    set_property(TARGET wosdbg_core PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
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

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    add_executable(wosdbg_workspace_semantic_test tests/workspace_semantic_test.cpp)
    target_include_directories(wosdbg_workspace_semantic_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(wosdbg_workspace_semantic_test PRIVATE wosdbg_core)
    add_test(NAME wosdbg_workspace_semantic_test COMMAND wosdbg_workspace_semantic_test)
    set_tests_properties(wosdbg_workspace_semantic_test PROPERTIES TIMEOUT 60 ENVIRONMENT "QT_QPA_PLATFORM=offscreen;QT_FORCE_STDERR_LOGGING=1")
    add_executable(wosdbg_ui_semantic_test
        tests/ui_semantic_test.cpp
    )
    target_include_directories(wosdbg_ui_semantic_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(wosdbg_ui_semantic_test PRIVATE wosdbg_core)
    add_test(NAME wosdbg_ui_semantic_test COMMAND wosdbg_ui_semantic_test $<TARGET_FILE:wosdbg>)
    set_tests_properties(wosdbg_ui_semantic_test PROPERTIES TIMEOUT 60 ENVIRONMENT "QT_QPA_PLATFORM=offscreen;QT_FORCE_STDERR_LOGGING=1")
    add_test(
        NAME wosdbg_incident_cli_semantic_test
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/incident_cli_semantic_test.py
                --wosdbg $<TARGET_FILE:wosdbg>
                --require-incident-tools
    )
    set_tests_properties(wosdbg_incident_cli_semantic_test PROPERTIES TIMEOUT 180)
    add_test(
        NAME wosdbg_telemetry_jsonl_semantic_test
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/telemetry_jsonl_semantic_test.py
                --wosdbg $<TARGET_FILE:wosdbg>
    )
    set_tests_properties(wosdbg_telemetry_jsonl_semantic_test PROPERTIES TIMEOUT 120)
    add_test(
        NAME wosdbg_live_cli_semantic_test
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/live_cli_semantic_test.py
                --wosdbg $<TARGET_FILE:wosdbg>
                --require-live-tools
    )
    set_tests_properties(wosdbg_live_cli_semantic_test PROPERTIES TIMEOUT 120)
endif()

option(WOSDBG_BUILD_FUZZERS "Build bounded WOSDBG parser fuzz targets" OFF)
if(WOSDBG_BUILD_FUZZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "WOSDBG_BUILD_FUZZERS requires Clang/libFuzzer")
    endif()
    add_executable(wosdbg_coredump_parser_fuzz EXCLUDE_FROM_ALL
        tests/coredump_parser_fuzz.cpp
        coredump_parser.cpp
    )
    target_include_directories(wosdbg_coredump_parser_fuzz PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(wosdbg_coredump_parser_fuzz PRIVATE Qt6::Core)
    target_compile_options(wosdbg_coredump_parser_fuzz PRIVATE
        -fsanitize=fuzzer,address,undefined
        -fno-omit-frame-pointer
    )
    target_link_options(wosdbg_coredump_parser_fuzz PRIVATE
        -fsanitize=fuzzer,address,undefined
        -fno-omit-frame-pointer
    )
endif()
