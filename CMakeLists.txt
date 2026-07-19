cmake_minimum_required(VERSION 3.20)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "WOS build type" FORCE)
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)

option(WOS_BUILD_LINUX_BENCHMARKS "Build Linux host benchmark binaries alongside WOS artifacts" OFF)
set(WOS_TOOLS_BUILD_TYPE Release CACHE STRING "Build type for host-side WOS tools")
set_property(CACHE WOS_TOOLS_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
set(WOS_NATIVE_SYSTEM_BUILD OFF CACHE BOOL "Build modules with the native WOS system toolchain")
set(WOS_HOST_UNAME "")
if(NOT ((DEFINED ENV{WOS_NATIVE_HOST} AND "$ENV{WOS_NATIVE_HOST}" STREQUAL "1") OR CMAKE_HOST_SYSTEM_NAME STREQUAL "WOS"))
    execute_process(
        COMMAND uname -s
        OUTPUT_VARIABLE WOS_HOST_UNAME
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
endif()
if((DEFINED ENV{WOS_NATIVE_HOST} AND "$ENV{WOS_NATIVE_HOST}" STREQUAL "1") OR CMAKE_HOST_SYSTEM_NAME STREQUAL "WOS" OR
   WOS_HOST_UNAME STREQUAL "WOS")
    set(WOS_NATIVE_SYSTEM_BUILD ON CACHE BOOL "Build modules with the native WOS system toolchain" FORCE)
endif()

set(WOS_USE_CCACHE_DEFAULT ON)
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_USE_CCACHE_DEFAULT OFF)
endif()
option(WOS_USE_CCACHE "Use ccache/cccache as a compiler launcher when available" ${WOS_USE_CCACHE_DEFAULT})
set(WOS_CCACHE_PROGRAM "" CACHE FILEPATH "Path to ccache/cccache")

set(WOS_PORT_BUILD_DEFAULT ON)
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_PORT_BUILD_DEFAULT OFF)
endif()
set(WOS_BUILD_CMAKE_FOR_HOST_DEFAULT ${WOS_PORT_BUILD_DEFAULT})
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_BUILD_CMAKE_FOR_HOST_DEFAULT OFF)
endif()

set(WOS_BUILD_DISK_IMAGES_DEFAULT ON)
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_BUILD_DISK_IMAGES_DEFAULT OFF)
endif()

set(WOS_BUILD_WOSDBG_DEFAULT ON)
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_BUILD_WOSDBG_DEFAULT OFF)
endif()
option(WOS_BUILD_WOSDBG "Build the Qt6-based wosdbg host tool" ${WOS_BUILD_WOSDBG_DEFAULT})
option(WOS_BUILD_DISK_IMAGES "Build boot/rootfs qcow2 images with qemu-img, mtools, and libguestfs" ${WOS_BUILD_DISK_IMAGES_DEFAULT})

if(WOS_NATIVE_SYSTEM_BUILD AND NOT WOS_BUILD_DISK_IMAGES)
    message(STATUS "Native WOS build: disk image packaging is disabled; pass -DWOS_BUILD_DISK_IMAGES=ON when qemu-img, mtools, and guestfish are available.")
endif()

set(WOS_BUILD_HOST_TOOLS_DEFAULT ON)
if(NOT WOS_BUILD_WOSDBG AND NOT WOS_BUILD_LINUX_BENCHMARKS)
    set(WOS_BUILD_HOST_TOOLS_DEFAULT OFF)
endif()
option(WOS_BUILD_HOST_TOOLS "Configure and build host-side WOS tools" ${WOS_BUILD_HOST_TOOLS_DEFAULT})

if(WOS_USE_CCACHE AND NOT WOS_CCACHE_PROGRAM)
    find_program(WOS_CCACHE_PROGRAM NAMES ccache cccache)
endif()

set(WOS_TOOLS_CCACHE_CMAKE_ARGS -DWOS_USE_CCACHE=${WOS_USE_CCACHE})
if(WOS_CCACHE_PROGRAM)
    list(APPEND WOS_TOOLS_CCACHE_CMAKE_ARGS -DWOS_CCACHE_PROGRAM=${WOS_CCACHE_PROGRAM})
endif()

set(WOS_TOOLS_BUILD_PARALLEL_ARGS --parallel)
if(DEFINED ENV{WOS_BUILD_JOBS} AND "$ENV{WOS_BUILD_JOBS}" MATCHES "^[1-9][0-9]*$")
    set(WOS_TOOLS_BUILD_PARALLEL_ARGS --parallel $ENV{WOS_BUILD_JOBS})
elseif(DEFINED ENV{CMAKE_BUILD_PARALLEL_LEVEL} AND "$ENV{CMAKE_BUILD_PARALLEL_LEVEL}" MATCHES "^[1-9][0-9]*$")
    set(WOS_TOOLS_BUILD_PARALLEL_ARGS --parallel $ENV{CMAKE_BUILD_PARALLEL_LEVEL})
endif()

if(WOS_BUILD_HOST_TOOLS)
    include(ExternalProject)
    ExternalProject_Add(wos_tools_build
        SOURCE_DIR ${CMAKE_SOURCE_DIR}/tools
        BINARY_DIR ${CMAKE_BINARY_DIR}/tools
        CMAKE_ARGS
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
            -DCMAKE_BUILD_TYPE=${WOS_TOOLS_BUILD_TYPE}
            -DWOS_BUILD_LINUX_BENCHMARKS=${WOS_BUILD_LINUX_BENCHMARKS}
            -DWOS_BUILD_WOSDBG=${WOS_BUILD_WOSDBG}
            ${WOS_TOOLS_CCACHE_CMAKE_ARGS}
        BUILD_COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/tools ${WOS_TOOLS_BUILD_PARALLEL_ARGS}
        INSTALL_COMMAND ""
    )

    # Always run the build step
    ExternalProject_Add_StepTargets(wos_tools_build build)
    add_custom_target(wos_tools_build_always ALL
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/tools ${WOS_TOOLS_BUILD_PARALLEL_ARGS}
        DEPENDS wos_tools_build-build
        COMMENT "Building WOS tools..."
    )
else()
    add_custom_target(wos_tools_build_always
        COMMENT "Host-side WOS tools disabled."
    )
endif()

project(wos NONE)

# WOS modules (kernel, init, testprog, netd, httpd, perf, top, memacc, journal, wkictl, renderbench)
add_subdirectory(modules)

# ---------------------------------------------------------------------------
# Toolchain / external build targets
# ---------------------------------------------------------------------------

set(WOS_SYSROOT_PATH "${CMAKE_SOURCE_DIR}/toolchain/sysroot" CACHE PATH "WOS target sysroot")
set(WOS_MLIBC_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/mlibc-build" CACHE PATH "mlibc build directory")
set(WOS_LIBCXX_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/libcxx-build" CACHE PATH "libc++ build directory")
set(WOS_BUSYBOX_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/busybox-build" CACHE PATH "BusyBox build directory")
set(WOS_BUSYBOX_INSTALL_DIR "${CMAKE_SOURCE_DIR}/toolchain/busybox-install" CACHE PATH "BusyBox install staging directory")
set(WOS_BUSYBOX_CONFIG "${CMAKE_SOURCE_DIR}/configs/busybox/wos_full.config" CACHE FILEPATH "Resolved BusyBox WOS config")
set(WOS_DROPBEAR_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/dropbear-build" CACHE PATH "Dropbear build directory")
set(WOS_MAKE_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/make-build" CACHE PATH "GNU make build directory")
set(WOS_MAKE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/make" CACHE PATH "GNU make source directory")
set(WOS_BASH_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/bash-build" CACHE PATH "Bash build directory")
set(WOS_BASH_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/bash" CACHE PATH "Bash source directory")
set(WOS_ZLIB_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/zlib-build" CACHE PATH "zlib build directory")
set(WOS_ZLIB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/zlib" CACHE PATH "zlib source directory")
set(WOS_OPENSSL_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/openssl-build" CACHE PATH "OpenSSL build directory")
set(WOS_OPENSSL_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/openssl" CACHE PATH "OpenSSL source directory")
set(WOS_CURL_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/curl-build" CACHE PATH "curl build directory")
set(WOS_CURL_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/curl" CACHE PATH "curl source directory")
set(WOS_GIT_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/git-build" CACHE PATH "Native WOS Git build directory")
set(WOS_GIT_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/git" CACHE PATH "Git source directory")
set(WOS_CLANG_FOR_WOS_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/clang-wos-build" CACHE PATH "Native WOS clang build directory")
set(WOS_NINJA_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/ninja-build" CACHE PATH "Native WOS Ninja build directory")
set(WOS_CMAKE_FOR_HOST_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/cmake-host-build" CACHE PATH "Host CMake build directory")
set(WOS_CMAKE_FOR_HOST_INSTALL_DIR "${CMAKE_SOURCE_DIR}/toolchain/host" CACHE PATH "Host CMake install prefix")
set(WOS_CMAKE_FOR_WOS_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/cmake-wos-build" CACHE PATH "Native WOS CMake build directory")
set(WOS_PYTHON_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/python" CACHE PATH "CPython source directory")
set(WOS_PYTHON_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/python-build" CACHE PATH "Native WOS Python build directory")
set(WOS_MESON_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/meson" CACHE PATH "Meson source directory")
set(WOS_MESON_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/meson-build" CACHE PATH "Native WOS Meson staging directory")
set(WOS_NASM_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/nasm" CACHE PATH "NASM source directory")
set(WOS_NASM_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/nasm-build" CACHE PATH "Native WOS NASM build directory")
set(WOS_NCURSES_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/ncurses" CACHE PATH "ncurses source directory")
set(WOS_NCURSES_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/ncurses-build" CACHE PATH "Native WOS ncurses build directory")
set(WOS_NANO_SOURCE_DIR "${CMAKE_SOURCE_DIR}/toolchain/src/nano" CACHE PATH "GNU nano source directory")
set(WOS_NANO_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/nano-build" CACHE PATH "Native WOS nano build directory")
set(WOS_BOOT_DISK "${CMAKE_SOURCE_DIR}/disk.qcow2" CACHE FILEPATH "WOS boot disk image")
set(WOS_ROOTFS_DISK "${CMAKE_SOURCE_DIR}/mountfs.qcow2" CACHE FILEPATH "WOS rootfs disk image")
option(WOS_SKIP_LIBCXX_INSTALL "Skip the libc++ install target; useful when an isolated sysroot is pre-seeded" OFF)
option(WOS_BUILD_CLANG_FOR_WOS "Build a native WOS clang/lld toolchain and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_NINJA_FOR_WOS "Build a native WOS Ninja and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_CMAKE_FOR_HOST "Build host-side CMake from the WOS fork and install it into toolchain/host" ${WOS_BUILD_CMAKE_FOR_HOST_DEFAULT})
option(WOS_BUILD_CMAKE_FOR_WOS "Build native WOS CMake tools and stage them into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_PYTHON_FOR_WOS "Build native WOS Python and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_MESON_FOR_WOS "Stage native WOS Meson into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_NASM_FOR_WOS "Build native WOS NASM and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_BASH_FOR_WOS "Build native WOS Bash and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_NCURSES_FOR_WOS "Build static WOS ncursesw for terminal applications" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_NANO_FOR_WOS "Build native WOS nano and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_OPENSSL_FOR_WOS "Build static WOS OpenSSL-compatible TLS libraries for curl and HTTPS-capable ports" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_CURL_FOR_WOS "Build native WOS curl and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
option(WOS_BUILD_GIT_FOR_WOS "Build native WOS Git and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})
set(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN_DEFAULT OFF)
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN_DEFAULT ON)
endif()
option(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN "Reuse artifacts already produced by tools/bootstrap.sh instead of rerunning external toolchain/userland port builders" ${WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN_DEFAULT})
option(WOS_USERSPACE_NO_AVX "Build WOS userspace and HTTPS ports without AVX for FPU-state diagnostics" OFF)
set(WOS_USERSPACE_NO_AVX_ENV 0)
if(WOS_USERSPACE_NO_AVX)
    set(WOS_USERSPACE_NO_AVX_ENV 1)
endif()

set(SYSROOT ${WOS_SYSROOT_PATH})
set(MLIBC_BUILD_DIR ${WOS_MLIBC_BUILD_DIR})
set(LIBCXX_BUILD_DIR ${WOS_LIBCXX_BUILD_DIR})
set(WOS_BUILD_SCRIPTS_DIR ${CMAKE_SOURCE_DIR}/scripts/build)
set(WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX "/(\\.git|\\.github|debian|docs?|testsuite|test|fuzz|manpages)(/|$)")

set(WOS_RECURSIVE_GLOB_OPTIONS)
if(NOT WOS_NATIVE_SYSTEM_BUILD)
    list(APPEND WOS_RECURSIVE_GLOB_OPTIONS CONFIGURE_DEPENDS)
endif()

function(wos_glob_recurse output_var)
    file(GLOB_RECURSE WOS_GLOB_RECURSE_OUTPUT ${WOS_RECURSIVE_GLOB_OPTIONS} ${ARGN})
    set(${output_var} ${WOS_GLOB_RECURSE_OUTPUT} PARENT_SCOPE)
endfunction()

function(wos_collect_external_sources output_var)
    wos_glob_recurse(WOS_COLLECTED_EXTERNAL_SOURCES ${ARGN})
    list(FILTER WOS_COLLECTED_EXTERNAL_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
    set(${output_var} ${WOS_COLLECTED_EXTERNAL_SOURCES} PARENT_SCOPE)
endfunction()

function(wos_add_preseeded_artifact_target target_name stamp label)
    add_custom_command(
        OUTPUT ${stamp}
        COMMAND ${WOS_BUILD_SCRIPTS_DIR}/verify_preseeded_artifacts.sh ${label} ${stamp} ${ARGN}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Using preseeded ${label} artifacts..."
        DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/verify_preseeded_artifacts.sh
    )
    add_custom_target(${target_name} DEPENDS ${stamp})
endfunction()

# --- check_headers: validate #pragma once in kern and init headers ----------
set(WOS_HEADER_CHECK_FILES)
if(NOT WOS_NATIVE_SYSTEM_BUILD)
    wos_glob_recurse(WOS_HEADER_CHECK_FILES
        ${CMAKE_SOURCE_DIR}/modules/kern/*.hpp
        ${CMAKE_SOURCE_DIR}/modules/init/*.hpp
    )
endif()
set(CHECK_HEADERS_STAMP ${CMAKE_BINARY_DIR}/stamps/check_headers.stamp)
add_custom_command(
    OUTPUT ${CHECK_HEADERS_STAMP}
    COMMAND sh ${WOS_BUILD_SCRIPTS_DIR}/check_headers.sh ${CMAKE_SOURCE_DIR}/modules/kern
    COMMAND sh ${WOS_BUILD_SCRIPTS_DIR}/check_headers.sh ${CMAKE_SOURCE_DIR}/modules/init
    COMMAND ${CMAKE_COMMAND} -E touch ${CHECK_HEADERS_STAMP}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking #pragma once in kernel/init headers..."
    DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/check_headers.sh ${WOS_HEADER_CHECK_FILES}
)
add_custom_target(check_headers DEPENDS ${CHECK_HEADERS_STAMP})

# --- mlibc: build and install the C library ---------------------------------
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/stamps)
set(MLIBC_STAMP ${CMAKE_BINARY_DIR}/stamps/mlibc.stamp)
if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    wos_add_preseeded_artifact_target(mlibc ${MLIBC_STAMP} mlibc
        ${WOS_SYSROOT_PATH}/lib/libc.so
        ${WOS_SYSROOT_PATH}/lib/Scrt1.o
        ${WOS_SYSROOT_PATH}/include/stdio.h
    )
else()
    # Glob mlibc only when the port is actually rebuilt. Preseeded self-host
    # configures validate artifacts and should not walk external source trees.
    wos_glob_recurse(MLIBC_SOURCES
        ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.cpp
        ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.c
        ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.hpp
        ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.h
        ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.S
        ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/meson.build
    )
    add_custom_command(
        OUTPUT ${MLIBC_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                WOS_MLIBC_BUILD_DIR=${WOS_MLIBC_BUILD_DIR}
                ${WOS_BUILD_SCRIPTS_DIR}/build_mlibc.sh ${CMAKE_BUILD_TYPE}
        COMMAND ${CMAKE_COMMAND} -E touch ${MLIBC_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building mlibc..."
        DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_mlibc.sh ${MLIBC_SOURCES}
    )
    add_custom_target(mlibc DEPENDS ${MLIBC_STAMP})
endif()

# --- libcxx: build and install libc++ ---------------------------------------
set(LIBCXX_STAMP ${CMAKE_BINARY_DIR}/stamps/libcxx.stamp)
if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    wos_add_preseeded_artifact_target(libcxx ${LIBCXX_STAMP} libcxx
        ${WOS_SYSROOT_PATH}/lib/libc++.so
        ${WOS_SYSROOT_PATH}/lib/libc++abi.so
        ${WOS_SYSROOT_PATH}/include/c++/v1/__config
    )
elseif(WOS_SKIP_LIBCXX_INSTALL)
    add_custom_command(
        OUTPUT ${LIBCXX_STAMP}
        COMMAND ${CMAKE_COMMAND} -E echo "Skipping libc++ install using pre-seeded sysroot ${WOS_SYSROOT_PATH}"
        COMMAND ${CMAKE_COMMAND} -E touch ${LIBCXX_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Using pre-seeded libc++ in isolated sysroot..."
        DEPENDS ${MLIBC_STAMP}
    )
else()
    add_custom_command(
        OUTPUT ${LIBCXX_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_LIBCXX_BUILD_DIR=${LIBCXX_BUILD_DIR}
                ${WOS_BUILD_SCRIPTS_DIR}/build_libcxx_for_wos.sh
        COMMAND ${CMAKE_COMMAND} -E touch ${LIBCXX_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building libc++..."
        DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_libcxx_for_wos.sh ${MLIBC_STAMP}
    )
endif()
if(NOT WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    add_custom_target(libcxx DEPENDS ${LIBCXX_STAMP})
endif()

# --- busybox: cross-compile busybox for WOS ---------------------------------
set(BUSYBOX_STAMP ${CMAKE_BINARY_DIR}/stamps/busybox.stamp)
if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    wos_add_preseeded_artifact_target(busybox ${BUSYBOX_STAMP} busybox
        ${WOS_BUSYBOX_INSTALL_DIR}/bin/busybox
    )
else()
    wos_glob_recurse(BUSYBOX_SOURCES
        ${CMAKE_SOURCE_DIR}/toolchain/src/busybox/*
    )
    list(FILTER BUSYBOX_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
    add_custom_command(
        OUTPUT ${BUSYBOX_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                WOS_BUSYBOX_BUILD_DIR=${WOS_BUSYBOX_BUILD_DIR}
                WOS_BUSYBOX_INSTALL_DIR=${WOS_BUSYBOX_INSTALL_DIR}
                WOS_BUSYBOX_CONFIG=${WOS_BUSYBOX_CONFIG}
                ${WOS_BUILD_SCRIPTS_DIR}/build_busybox.sh
        COMMAND ${CMAKE_COMMAND} -E touch ${BUSYBOX_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building busybox..."
        DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_busybox.sh ${WOS_BUSYBOX_CONFIG} ${BUSYBOX_SOURCES} ${MLIBC_STAMP} ${LIBCXX_STAMP}
    )
    add_custom_target(busybox DEPENDS ${BUSYBOX_STAMP})
endif()

# --- dropbear: cross-compile dropbear SSH for WOS ---------------------------
set(DROPBEAR_STAMP ${CMAKE_BINARY_DIR}/stamps/dropbear.stamp)
if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    wos_add_preseeded_artifact_target(dropbear ${DROPBEAR_STAMP} dropbear
        ${WOS_SYSROOT_PATH}/bin/dropbearmulti
    )
else()
    wos_glob_recurse(DROPBEAR_SOURCES
        ${CMAKE_SOURCE_DIR}/toolchain/src/dropbear/*
    )
    list(FILTER DROPBEAR_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
    add_custom_command(
        OUTPUT ${DROPBEAR_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                WOS_DROPBEAR_BUILD_DIR=${WOS_DROPBEAR_BUILD_DIR}
                ${WOS_BUILD_SCRIPTS_DIR}/build_dropbear.sh
        COMMAND ${CMAKE_COMMAND} -E touch ${DROPBEAR_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building dropbear..."
        DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_dropbear.sh ${DROPBEAR_SOURCES} ${MLIBC_STAMP} ${LIBCXX_STAMP}
    )
    add_custom_target(dropbear DEPENDS ${DROPBEAR_STAMP})
endif()

# --- gnu_make: cross-compile GNU make for WOS -------------------------------
set(GNU_MAKE_STAMP ${CMAKE_BINARY_DIR}/stamps/gnu_make.stamp)
if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    wos_add_preseeded_artifact_target(gnu_make ${GNU_MAKE_STAMP} gnu_make
        ${WOS_SYSROOT_PATH}/bin/make
    )
else()
    wos_glob_recurse(GNU_MAKE_SOURCES
        ${WOS_MAKE_SOURCE_DIR}/*
    )
    list(FILTER GNU_MAKE_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
    add_custom_command(
        OUTPUT ${GNU_MAKE_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                WOS_MAKE_BUILD_DIR=${WOS_MAKE_BUILD_DIR}
                WOS_MAKE_SOURCE_DIR=${WOS_MAKE_SOURCE_DIR}
                ${WOS_BUILD_SCRIPTS_DIR}/build_make.sh
        COMMAND ${CMAKE_COMMAND} -E touch ${GNU_MAKE_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building GNU make..."
        DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_make.sh ${GNU_MAKE_SOURCES} ${MLIBC_STAMP} ${LIBCXX_STAMP}
    )
    add_custom_target(gnu_make DEPENDS ${GNU_MAKE_STAMP})
endif()

# --- bash_for_wos: cross-compile Bash to run inside WOS ---------------------
set(BASH_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/bash_for_wos.stamp)
set(WOS_BASH_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_BASH_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(bash_for_wos ${BASH_FOR_WOS_STAMP} bash
            ${WOS_SYSROOT_PATH}/bin/bash
        )
    else()
        wos_collect_external_sources(BASH_FOR_WOS_SOURCES
            ${WOS_BASH_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${BASH_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_BASH_BUILD_DIR=${WOS_BASH_BUILD_DIR}
                    WOS_BASH_SOURCE_DIR=${WOS_BASH_SOURCE_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_bash_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${BASH_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS Bash..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_bash_for_wos.sh
                ${BASH_FOR_WOS_SOURCES}
                ${MLIBC_STAMP}
        )
        add_custom_target(bash_for_wos DEPENDS ${BASH_FOR_WOS_STAMP})
    endif()
    list(APPEND WOS_BASH_FOR_WOS_ROOTFS_INPUTS ${BASH_FOR_WOS_STAMP})
endif()

# --- zlib_for_wos: static compression library for native Git ----------------
set(ZLIB_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/zlib_for_wos.stamp)
if(WOS_BUILD_CURL_FOR_WOS OR WOS_BUILD_GIT_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(zlib_for_wos ${ZLIB_FOR_WOS_STAMP} zlib
            ${WOS_SYSROOT_PATH}/lib/libz.a
            ${WOS_SYSROOT_PATH}/include/zlib.h
        )
    else()
        wos_collect_external_sources(ZLIB_FOR_WOS_SOURCES
            ${WOS_ZLIB_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${ZLIB_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_ZLIB_BUILD_DIR=${WOS_ZLIB_BUILD_DIR}
                    WOS_ZLIB_SOURCE_DIR=${WOS_ZLIB_SOURCE_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_zlib_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${ZLIB_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building static zlib for WOS..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_zlib_for_wos.sh
                ${ZLIB_FOR_WOS_SOURCES}
                ${MLIBC_STAMP}
        )
        add_custom_target(zlib_for_wos DEPENDS ${ZLIB_FOR_WOS_STAMP})
    endif()
endif()

# --- openssl_for_wos: static OpenSSL-compatible TLS libraries for curl ------
set(OPENSSL_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/openssl_for_wos.stamp)
if(WOS_BUILD_OPENSSL_FOR_WOS OR WOS_BUILD_CURL_FOR_WOS OR WOS_BUILD_GIT_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(openssl_for_wos ${OPENSSL_FOR_WOS_STAMP} openssl
            ${WOS_SYSROOT_PATH}/lib/libssl.a
            ${WOS_SYSROOT_PATH}/lib/libcrypto.a
            ${WOS_SYSROOT_PATH}/include/openssl/ssl.h
        )
    else()
        wos_collect_external_sources(OPENSSL_FOR_WOS_SOURCES
            ${WOS_OPENSSL_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${OPENSSL_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_OPENSSL_BUILD_DIR=${WOS_OPENSSL_BUILD_DIR}
                    WOS_OPENSSL_SOURCE_DIR=${WOS_OPENSSL_SOURCE_DIR}
                    WOS_USERSPACE_NO_AVX=${WOS_USERSPACE_NO_AVX_ENV}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_openssl_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${OPENSSL_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building static OpenSSL-compatible TLS libraries for WOS..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_openssl_for_wos.sh
                ${OPENSSL_FOR_WOS_SOURCES}
                ${MLIBC_STAMP}
        )
        add_custom_target(openssl_for_wos DEPENDS ${OPENSSL_FOR_WOS_STAMP})
    endif()
endif()

# --- curl_for_wos: HTTPS client and static libcurl for Git -----------------
set(CURL_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/curl_for_wos.stamp)
set(WOS_CURL_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_CURL_FOR_WOS OR WOS_BUILD_GIT_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(curl_for_wos ${CURL_FOR_WOS_STAMP} curl
            ${WOS_SYSROOT_PATH}/bin/curl
            ${WOS_SYSROOT_PATH}/lib/libcurl.a
        )
    else()
        wos_collect_external_sources(CURL_FOR_WOS_SOURCES
            ${WOS_CURL_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${CURL_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_CURL_BUILD_DIR=${WOS_CURL_BUILD_DIR}
                    WOS_CURL_SOURCE_DIR=${WOS_CURL_SOURCE_DIR}
                    WOS_USERSPACE_NO_AVX=${WOS_USERSPACE_NO_AVX_ENV}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_curl_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${CURL_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS curl..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_curl_for_wos.sh
                ${CURL_FOR_WOS_SOURCES}
                ${ZLIB_FOR_WOS_STAMP}
                ${OPENSSL_FOR_WOS_STAMP}
                ${MLIBC_STAMP}
        )
        add_custom_target(curl_for_wos DEPENDS ${CURL_FOR_WOS_STAMP})
        add_dependencies(curl_for_wos zlib_for_wos openssl_for_wos)
    endif()
endif()
if(WOS_BUILD_CURL_FOR_WOS)
    list(APPEND WOS_CURL_FOR_WOS_ROOTFS_INPUTS ${CURL_FOR_WOS_STAMP})
endif()

# --- git_for_wos: cross-compile Git to run inside WOS ----------------------
set(GIT_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/git_for_wos.stamp)
set(WOS_GIT_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_GIT_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(git_for_wos ${GIT_FOR_WOS_STAMP} git
            ${WOS_SYSROOT_PATH}/bin/git
            ${WOS_SYSROOT_PATH}/bin/git-shell
            ${WOS_SYSROOT_PATH}/bin/scalar
            ${WOS_SYSROOT_PATH}/libexec/git-core/git-remote-https
        )
    else()
        wos_collect_external_sources(GIT_FOR_WOS_SOURCES
            ${WOS_GIT_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${GIT_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_GIT_BUILD_DIR=${WOS_GIT_BUILD_DIR}
                    WOS_GIT_SOURCE_DIR=${WOS_GIT_SOURCE_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_git_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${GIT_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS Git..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_git_for_wos.sh
                ${GIT_FOR_WOS_SOURCES}
                ${CURL_FOR_WOS_STAMP}
                ${MLIBC_STAMP}
        )
        add_custom_target(git_for_wos DEPENDS ${GIT_FOR_WOS_STAMP})
        add_dependencies(git_for_wos bash_for_wos curl_for_wos)
    endif()
    list(APPEND WOS_GIT_FOR_WOS_ROOTFS_INPUTS ${GIT_FOR_WOS_STAMP})
endif()

# --- cmake_for_host: build WOS-aware CMake for the Linux host ---------------
set(CMAKE_FOR_HOST_STAMP ${CMAKE_BINARY_DIR}/stamps/cmake_for_host.stamp)
set(WOS_CMAKE_FOR_HOST_DEPENDS)
set(WOS_SCAN_CMAKE_PORT_SOURCES OFF)
if(WOS_BUILD_CMAKE_FOR_HOST)
    set(WOS_SCAN_CMAKE_PORT_SOURCES ON)
elseif(WOS_BUILD_CMAKE_FOR_WOS AND NOT WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
    set(WOS_SCAN_CMAKE_PORT_SOURCES ON)
endif()
if(WOS_SCAN_CMAKE_PORT_SOURCES)
    wos_collect_external_sources(CMAKE_FOR_WOS_SOURCES
        ${CMAKE_SOURCE_DIR}/toolchain/src/cmake/*
    )
endif()
if(WOS_BUILD_CMAKE_FOR_HOST)
    add_custom_command(
        OUTPUT ${CMAKE_FOR_HOST_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_CMAKE_FOR_HOST_BUILD_DIR=${WOS_CMAKE_FOR_HOST_BUILD_DIR}
                WOS_CMAKE_FOR_HOST_INSTALL_DIR=${WOS_CMAKE_FOR_HOST_INSTALL_DIR}
                ${WOS_BUILD_SCRIPTS_DIR}/build_cmake_for_host.sh
        COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_FOR_HOST_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building host WOS-aware CMake tools..."
        DEPENDS
            ${WOS_BUILD_SCRIPTS_DIR}/build_cmake_for_host.sh
            ${CMAKE_SOURCE_DIR}/toolchain/src/cmake/CMakeLists.txt
            ${CMAKE_FOR_WOS_SOURCES}
    )
    add_custom_target(cmake_for_host DEPENDS ${CMAKE_FOR_HOST_STAMP})
    list(APPEND WOS_CMAKE_FOR_HOST_DEPENDS ${CMAKE_FOR_HOST_STAMP})
endif()

# --- clang_for_wos: cross-compile clang/lld to run inside WOS ---------------
set(CLANG_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/clang_for_wos.stamp)
set(WOS_CLANG_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_CLANG_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(clang_for_wos ${CLANG_FOR_WOS_STAMP} clang_for_wos
            ${WOS_SYSROOT_PATH}/bin/clang
            ${WOS_SYSROOT_PATH}/bin/lld
            ${WOS_SYSROOT_PATH}/bin/ld.lld
            ${WOS_SYSROOT_PATH}/bin/llvm-ar
            ${WOS_SYSROOT_PATH}/bin/llvm-ranlib
            ${WOS_SYSROOT_PATH}/bin/llvm-nm
            ${WOS_SYSROOT_PATH}/bin/llvm-objcopy
            ${WOS_SYSROOT_PATH}/bin/llvm-strip
            ${WOS_SYSROOT_PATH}/bin/llvm-readelf
            ${WOS_SYSROOT_PATH}/bin/llvm-objdump
            ${WOS_SYSROOT_PATH}/bin/llvm-symbolizer
            ${WOS_SYSROOT_PATH}/bin/llvm-as
            ${WOS_SYSROOT_PATH}/bin/llvm-dis
            ${WOS_SYSROOT_PATH}/bin/llvm-link
            ${WOS_SYSROOT_PATH}/bin/llvm-size
            ${WOS_SYSROOT_PATH}/bin/llvm-strings
            ${WOS_SYSROOT_PATH}/bin/llvm-dwarfdump
            ${WOS_SYSROOT_PATH}/bin/llc
            ${WOS_SYSROOT_PATH}/bin/opt
            ${WOS_SYSROOT_PATH}/bin/obj2yaml
            ${WOS_SYSROOT_PATH}/bin/yaml2obj
            ${WOS_SYSROOT_PATH}/bin/llvm-tblgen
            ${WOS_SYSROOT_PATH}/bin/clang-tblgen
            ${WOS_SYSROOT_PATH}/bin/x86_64-pc-wos.cfg
            ${WOS_SYSROOT_PATH}/lib/clang/*/lib/x86_64-pc-wos/libclang_rt.builtins.a
        )
    else()
        wos_collect_external_sources(CLANG_FOR_WOS_SOURCES
            ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/llvm/*
            ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/clang/*
            ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/lld/*
        )
        add_custom_command(
            OUTPUT ${CLANG_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_CLANG_FOR_WOS_BUILD_DIR=${WOS_CLANG_FOR_WOS_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_clang_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${CLANG_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS clang/lld toolchain..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_clang_for_wos.sh
                ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/llvm/CMakeLists.txt
                ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/clang/CMakeLists.txt
                ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/lld/CMakeLists.txt
                ${CLANG_FOR_WOS_SOURCES}
                ${WOS_CMAKE_FOR_HOST_DEPENDS}
                ${MLIBC_STAMP}
                ${LIBCXX_STAMP}
        )
        add_custom_target(clang_for_wos DEPENDS ${CLANG_FOR_WOS_STAMP})
        if(WOS_BUILD_CMAKE_FOR_HOST)
            add_dependencies(clang_for_wos cmake_for_host)
        endif()
    endif()
    list(APPEND WOS_CLANG_FOR_WOS_ROOTFS_INPUTS ${CLANG_FOR_WOS_STAMP})
endif()

# --- ninja_for_wos: cross-compile Ninja to run inside WOS -------------------
set(NINJA_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/ninja_for_wos.stamp)
set(WOS_NINJA_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_NINJA_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(ninja_for_wos ${NINJA_FOR_WOS_STAMP} ninja
            ${WOS_SYSROOT_PATH}/bin/ninja
        )
    else()
        wos_collect_external_sources(NINJA_FOR_WOS_SOURCES
            ${CMAKE_SOURCE_DIR}/toolchain/src/ninja/*
        )
        add_custom_command(
            OUTPUT ${NINJA_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_NINJA_BUILD_DIR=${WOS_NINJA_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_ninja_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${NINJA_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS Ninja..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_ninja_for_wos.sh
                ${CMAKE_SOURCE_DIR}/toolchain/src/ninja/CMakeLists.txt
                ${NINJA_FOR_WOS_SOURCES}
                ${WOS_CMAKE_FOR_HOST_DEPENDS}
                ${MLIBC_STAMP}
                ${LIBCXX_STAMP}
        )
        add_custom_target(ninja_for_wos DEPENDS ${NINJA_FOR_WOS_STAMP})
        if(WOS_BUILD_CMAKE_FOR_HOST)
            add_dependencies(ninja_for_wos cmake_for_host)
        endif()
    endif()
    list(APPEND WOS_NINJA_FOR_WOS_ROOTFS_INPUTS ${NINJA_FOR_WOS_STAMP})
endif()

# --- cmake_for_wos: cross-compile CMake to run inside WOS ------------------
set(CMAKE_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/cmake_for_wos.stamp)
set(WOS_CMAKE_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_CMAKE_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(cmake_for_wos ${CMAKE_FOR_WOS_STAMP} cmake
            ${WOS_SYSROOT_PATH}/bin/cmake
            ${WOS_SYSROOT_PATH}/bin/ctest
            ${WOS_SYSROOT_PATH}/bin/cpack
            ${WOS_SYSROOT_PATH}/share/cmake-*
        )
    else()
        add_custom_command(
            OUTPUT ${CMAKE_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_CMAKE_FOR_WOS_BUILD_DIR=${WOS_CMAKE_FOR_WOS_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_cmake_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS CMake tools..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_cmake_for_wos.sh
                ${CMAKE_SOURCE_DIR}/toolchain/src/cmake/CMakeLists.txt
                ${CMAKE_FOR_WOS_SOURCES}
                ${WOS_CMAKE_FOR_HOST_DEPENDS}
                ${MLIBC_STAMP}
                ${LIBCXX_STAMP}
        )
        add_custom_target(cmake_for_wos DEPENDS ${CMAKE_FOR_WOS_STAMP})
        if(WOS_BUILD_CMAKE_FOR_HOST)
            add_dependencies(cmake_for_wos cmake_for_host)
        endif()
    endif()
    list(APPEND WOS_CMAKE_FOR_WOS_ROOTFS_INPUTS ${CMAKE_FOR_WOS_STAMP})
endif()

# --- python_for_wos: cross-compile CPython to run inside WOS ----------------
set(PYTHON_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/python_for_wos.stamp)
set(WOS_PYTHON_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_PYTHON_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(python_for_wos ${PYTHON_FOR_WOS_STAMP} python
            ${WOS_SYSROOT_PATH}/bin/python3*
            ${WOS_SYSROOT_PATH}/lib/python*
        )
    else()
        wos_collect_external_sources(PYTHON_FOR_WOS_SOURCES
            ${WOS_PYTHON_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${PYTHON_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_PYTHON_BUILD_DIR=${WOS_PYTHON_BUILD_DIR}
                    WOS_PYTHON_SOURCE_DIR=${WOS_PYTHON_SOURCE_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_python_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${PYTHON_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS Python..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_python_for_wos.sh
                ${PYTHON_FOR_WOS_SOURCES}
                ${MLIBC_STAMP}
                ${LIBCXX_STAMP}
        )
        add_custom_target(python_for_wos ALL DEPENDS ${PYTHON_FOR_WOS_STAMP})
    endif()
    list(APPEND WOS_PYTHON_FOR_WOS_ROOTFS_INPUTS ${PYTHON_FOR_WOS_STAMP})
endif()

# --- meson_for_wos: stage Meson to run inside WOS --------------------------
set(MESON_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/meson_for_wos.stamp)
set(WOS_MESON_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_MESON_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(meson_for_wos ${MESON_FOR_WOS_STAMP} meson
            ${WOS_SYSROOT_PATH}/bin/meson
        )
    else()
        wos_collect_external_sources(MESON_FOR_WOS_SOURCES
            ${WOS_MESON_SOURCE_DIR}/*
        )
        set(WOS_MESON_FOR_WOS_DEPENDS
            ${WOS_BUILD_SCRIPTS_DIR}/build_meson_for_wos.sh
            ${MESON_FOR_WOS_SOURCES}
            ${MLIBC_STAMP}
            ${LIBCXX_STAMP}
        )
        if(WOS_BUILD_PYTHON_FOR_WOS)
            list(APPEND WOS_MESON_FOR_WOS_DEPENDS ${PYTHON_FOR_WOS_STAMP})
        endif()
        add_custom_command(
            OUTPUT ${MESON_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_MESON_SOURCE_DIR=${WOS_MESON_SOURCE_DIR}
                    WOS_MESON_BUILD_DIR=${WOS_MESON_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_meson_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${MESON_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Staging native WOS Meson..."
            DEPENDS ${WOS_MESON_FOR_WOS_DEPENDS}
        )
        add_custom_target(meson_for_wos DEPENDS ${MESON_FOR_WOS_STAMP})
        if(WOS_BUILD_PYTHON_FOR_WOS)
            add_dependencies(meson_for_wos python_for_wos)
        endif()
    endif()
    list(APPEND WOS_MESON_FOR_WOS_ROOTFS_INPUTS ${MESON_FOR_WOS_STAMP})
endif()

# --- nasm_for_wos: cross-compile NASM to run inside WOS --------------------
set(NASM_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/nasm_for_wos.stamp)
set(WOS_NASM_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_NASM_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(nasm_for_wos ${NASM_FOR_WOS_STAMP} nasm
            ${WOS_SYSROOT_PATH}/bin/nasm
            ${WOS_SYSROOT_PATH}/bin/ndisasm
        )
    else()
        wos_collect_external_sources(NASM_FOR_WOS_SOURCES
            ${WOS_NASM_SOURCE_DIR}/*
        )
        list(FILTER NASM_FOR_WOS_SOURCES EXCLUDE REGEX "/(configure|config/config\\.h\\.in|autoconf/aclocal\\.m4|autoconf/clean\\.sh|autom4te\\.cache)(/|$)")
        add_custom_command(
            OUTPUT ${NASM_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_NASM_SOURCE_DIR=${WOS_NASM_SOURCE_DIR}
                    WOS_NASM_BUILD_DIR=${WOS_NASM_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_nasm_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${NASM_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS NASM..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_nasm_for_wos.sh
                ${NASM_FOR_WOS_SOURCES}
                ${MLIBC_STAMP}
        )
        add_custom_target(nasm_for_wos DEPENDS ${NASM_FOR_WOS_STAMP})
    endif()
    list(APPEND WOS_NASM_FOR_WOS_ROOTFS_INPUTS ${NASM_FOR_WOS_STAMP})
endif()

# --- ncurses_for_wos: static terminal library for native text UI ports ------
set(NCURSES_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/ncurses_for_wos.stamp)
set(WOS_NCURSES_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_NCURSES_FOR_WOS OR WOS_BUILD_NANO_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(ncurses_for_wos ${NCURSES_FOR_WOS_STAMP} ncurses
            ${WOS_SYSROOT_PATH}/lib/libncursesw.a
            ${WOS_SYSROOT_PATH}/include/curses.h
            ${WOS_SYSROOT_PATH}/include/term.h
        )
    else()
        wos_collect_external_sources(NCURSES_FOR_WOS_SOURCES
            ${WOS_NCURSES_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${NCURSES_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_NCURSES_SOURCE_DIR=${WOS_NCURSES_SOURCE_DIR}
                    WOS_NCURSES_BUILD_DIR=${WOS_NCURSES_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_ncurses_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${NCURSES_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building static WOS ncursesw..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_ncurses_for_wos.sh
                ${NCURSES_FOR_WOS_SOURCES}
                ${MLIBC_STAMP}
        )
        add_custom_target(ncurses_for_wos DEPENDS ${NCURSES_FOR_WOS_STAMP})
    endif()
    list(APPEND WOS_NCURSES_FOR_WOS_ROOTFS_INPUTS ${NCURSES_FOR_WOS_STAMP})
endif()

if(TARGET python_for_wos AND TARGET ncurses_for_wos)
    add_dependencies(python_for_wos ncurses_for_wos)
endif()
if(TARGET python_for_wos AND TARGET openssl_for_wos)
    add_dependencies(python_for_wos openssl_for_wos)
endif()

# --- nano_for_wos: cross-compile GNU nano to run inside WOS -----------------
set(NANO_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/nano_for_wos.stamp)
set(WOS_NANO_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_NANO_FOR_WOS)
    if(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)
        wos_add_preseeded_artifact_target(nano_for_wos ${NANO_FOR_WOS_STAMP} nano
            ${WOS_SYSROOT_PATH}/bin/nano
            ${WOS_SYSROOT_PATH}/bin/rnano
            ${WOS_SYSROOT_PATH}/share/nano
        )
    else()
        wos_collect_external_sources(NANO_FOR_WOS_SOURCES
            ${WOS_NANO_SOURCE_DIR}/*
        )
        add_custom_command(
            OUTPUT ${NANO_FOR_WOS_STAMP}
            COMMAND ${CMAKE_COMMAND} -E env
                    WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                    WOS_NANO_SOURCE_DIR=${WOS_NANO_SOURCE_DIR}
                    WOS_NANO_BUILD_DIR=${WOS_NANO_BUILD_DIR}
                    ${WOS_BUILD_SCRIPTS_DIR}/build_nano_for_wos.sh
            COMMAND ${CMAKE_COMMAND} -E touch ${NANO_FOR_WOS_STAMP}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building native WOS nano..."
            DEPENDS
                ${WOS_BUILD_SCRIPTS_DIR}/build_nano_for_wos.sh
                ${NANO_FOR_WOS_SOURCES}
                ${NCURSES_FOR_WOS_STAMP}
                ${MLIBC_STAMP}
        )
        add_custom_target(nano_for_wos DEPENDS ${NANO_FOR_WOS_STAMP})
        add_dependencies(nano_for_wos ncurses_for_wos)
    endif()
    list(APPEND WOS_NANO_FOR_WOS_ROOTFS_INPUTS ${NANO_FOR_WOS_STAMP})
endif()

if(WOS_BUILD_DISK_IMAGES)
    # --- mountfs_disk: create or delta-sync the XFS rootfs image ------------
    # If mountfs.qcow2 doesn't exist, create it from scratch.
    # If it already exists, delta-sync changed binaries and libraries.
    wos_glob_recurse(WOS_ROOTFS_CONFIG_INPUTS
        ${CMAKE_SOURCE_DIR}/configs/rootfs/*
        ${CMAKE_SOURCE_DIR}/configs/drive/srv/*
    )
    set(WOS_MODULE_TARGET_FILES
        $<TARGET_FILE:wos>
        $<TARGET_FILE:init>
        $<TARGET_FILE:testprog>
        $<TARGET_FILE:testd>
        $<TARGET_FILE:netd>
        $<TARGET_FILE:httpd>
        $<TARGET_FILE:debugserver>
        $<TARGET_FILE:perf>
        $<TARGET_FILE:top>
        $<TARGET_FILE:memacc>
        $<TARGET_FILE:journal>
        $<TARGET_FILE:journal_lib>
        $<TARGET_FILE:wkictl>
        $<TARGET_FILE:powerctl>
        $<TARGET_FILE:renderbench>
        $<TARGET_FILE:strace>
        $<TARGET_FILE:sftpserver>
    )
    set(MOUNTFS_STAMP ${CMAKE_BINARY_DIR}/stamps/mountfs_disk.stamp)
    add_custom_command(
        OUTPUT ${WOS_ROOTFS_DISK} ${MOUNTFS_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_BUILD_DIR=${CMAKE_BINARY_DIR}
                WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                WOS_BUSYBOX_INSTALL_DIR=${WOS_BUSYBOX_INSTALL_DIR}
                WOS_ROOTFS_DISK=${WOS_ROOTFS_DISK}
                bash -c "if [ -f '${WOS_ROOTFS_DISK}' ]\; then '${WOS_BUILD_SCRIPTS_DIR}/sync_rootfs.sh'\; else '${WOS_BUILD_SCRIPTS_DIR}/create_mountfs_disk.sh'\; fi"
        COMMAND ${CMAKE_COMMAND} -E touch ${MOUNTFS_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Syncing rootfs disk..."
        DEPENDS
            ${WOS_BUILD_SCRIPTS_DIR}/create_mountfs_disk.sh
            ${WOS_BUILD_SCRIPTS_DIR}/qcow_common.sh
            ${WOS_BUILD_SCRIPTS_DIR}/rootfs_common.sh
            ${WOS_BUILD_SCRIPTS_DIR}/rootfs_delta.py
            ${WOS_BUILD_SCRIPTS_DIR}/rootfs_qcow_xfs_delta.py
            ${WOS_BUILD_SCRIPTS_DIR}/sync_rootfs.sh
            ${CMAKE_SOURCE_DIR}/configs/system.conf
            ${CMAKE_SOURCE_DIR}/configs/vfstab
            ${WOS_ROOTFS_CONFIG_INPUTS}
            ${WOS_MODULE_TARGET_FILES}
            ${BUSYBOX_STAMP}
            ${DROPBEAR_STAMP}
            ${GNU_MAKE_STAMP}
            ${LIBCXX_STAMP}
            ${WOS_CLANG_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_NINJA_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_CMAKE_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_PYTHON_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_MESON_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_NASM_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_BASH_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_NCURSES_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_NANO_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_CURL_FOR_WOS_ROOTFS_INPUTS}
            ${WOS_GIT_FOR_WOS_ROOTFS_INPUTS}
    )
    add_custom_target(mountfs_disk DEPENDS ${WOS_ROOTFS_DISK} ${MOUNTFS_STAMP})
    if(WOS_BUILD_CLANG_FOR_WOS)
        add_dependencies(mountfs_disk clang_for_wos)
    endif()
    if(WOS_BUILD_NINJA_FOR_WOS)
        add_dependencies(mountfs_disk ninja_for_wos)
    endif()
    if(WOS_BUILD_CMAKE_FOR_WOS)
        add_dependencies(mountfs_disk cmake_for_wos)
    endif()
    if(WOS_BUILD_PYTHON_FOR_WOS)
        add_dependencies(mountfs_disk python_for_wos)
    endif()
    if(WOS_BUILD_MESON_FOR_WOS)
        add_dependencies(mountfs_disk meson_for_wos)
    endif()
    if(WOS_BUILD_NASM_FOR_WOS)
        add_dependencies(mountfs_disk nasm_for_wos)
    endif()
    if(WOS_BUILD_BASH_FOR_WOS)
        add_dependencies(mountfs_disk bash_for_wos)
    endif()
    if(WOS_BUILD_NCURSES_FOR_WOS OR WOS_BUILD_NANO_FOR_WOS)
        add_dependencies(mountfs_disk ncurses_for_wos)
    endif()
    if(WOS_BUILD_NANO_FOR_WOS)
        add_dependencies(mountfs_disk nano_for_wos)
    endif()
    if(WOS_BUILD_CURL_FOR_WOS)
        add_dependencies(mountfs_disk curl_for_wos)
    endif()
    if(WOS_BUILD_GIT_FOR_WOS)
        add_dependencies(mountfs_disk git_for_wos)
    endif()

    # --- boot_image: create the boot disk + initramfs ----------------------
    set(INITRAMFS_OUTPUT ${CMAKE_BINARY_DIR}/initramfs.cpio)
    set(BOOT_IMAGE_STAMP ${CMAKE_BINARY_DIR}/stamps/boot_image.stamp)
    add_custom_command(
        OUTPUT ${WOS_BOOT_DISK} ${INITRAMFS_OUTPUT} ${BOOT_IMAGE_STAMP}
        COMMAND ${CMAKE_COMMAND} -E env
                WOS_BUILD_DIR=${CMAKE_BINARY_DIR}
                WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
                WOS_BUSYBOX_INSTALL_DIR=${WOS_BUSYBOX_INSTALL_DIR}
                WOS_BOOT_DISK=${WOS_BOOT_DISK}
                WOS_ROOTFS_DISK=${WOS_ROOTFS_DISK}
                ${WOS_BUILD_SCRIPTS_DIR}/make_image.sh
        COMMAND ${CMAKE_COMMAND} -E touch ${BOOT_IMAGE_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Creating boot image..."
        DEPENDS
            ${WOS_BUILD_SCRIPTS_DIR}/make_image.sh
            ${WOS_BUILD_SCRIPTS_DIR}/make_initramfs.sh
            ${WOS_BUILD_SCRIPTS_DIR}/extract_partuuid.py
            ${WOS_BUILD_SCRIPTS_DIR}/qcow_common.sh
            ${CMAKE_SOURCE_DIR}/configs/disks.conf
            ${CMAKE_SOURCE_DIR}/configs/netdevs.conf
            ${CMAKE_SOURCE_DIR}/configs/system.conf
            ${CMAKE_SOURCE_DIR}/configs/vfstab
            ${CMAKE_SOURCE_DIR}/modules/kern/limine.conf
            $<TARGET_FILE:wos>
            $<TARGET_FILE:init>
            ${WOS_ROOTFS_DISK}
            ${MOUNTFS_STAMP}
            ${LIBCXX_STAMP}
    )
    add_custom_target(boot_image DEPENDS ${WOS_BOOT_DISK} ${INITRAMFS_OUTPUT} ${BOOT_IMAGE_STAMP})

    # busybox, dropbear, GNU make, Bash, curl, Git, Ninja, CMake, Python, Meson, NASM, ncurses, and nano are only needed for image creation, not compilation.
    add_dependencies(boot_image busybox dropbear gnu_make)
    if(WOS_BUILD_NINJA_FOR_WOS)
        add_dependencies(boot_image ninja_for_wos)
    endif()
    if(WOS_BUILD_CMAKE_FOR_WOS)
        add_dependencies(boot_image cmake_for_wos)
    endif()
    if(WOS_BUILD_PYTHON_FOR_WOS)
        add_dependencies(boot_image python_for_wos)
    endif()
    if(WOS_BUILD_MESON_FOR_WOS)
        add_dependencies(boot_image meson_for_wos)
    endif()
    if(WOS_BUILD_NASM_FOR_WOS)
        add_dependencies(boot_image nasm_for_wos)
    endif()
    if(WOS_BUILD_BASH_FOR_WOS)
        add_dependencies(boot_image bash_for_wos)
    endif()
    if(WOS_BUILD_NCURSES_FOR_WOS OR WOS_BUILD_NANO_FOR_WOS)
        add_dependencies(boot_image ncurses_for_wos)
    endif()
    if(WOS_BUILD_NANO_FOR_WOS)
        add_dependencies(boot_image nano_for_wos)
    endif()
    if(WOS_BUILD_CURL_FOR_WOS)
        add_dependencies(boot_image curl_for_wos)
    endif()
    if(WOS_BUILD_GIT_FOR_WOS)
        add_dependencies(boot_image git_for_wos)
    endif()
endif()

if(NOT WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_SYSROOT_DEPENDENT_TARGETS wos)
    list(APPEND WOS_SYSROOT_DEPENDENT_TARGETS ${WOS_USERSPACE_TARGETS})
    foreach(mod IN LISTS WOS_SYSROOT_DEPENDENT_TARGETS)
        if(TARGET ${mod})
            add_dependencies(${mod} mlibc libcxx)
        endif()
    endforeach()
endif()

# --- wos_full: top-level target that builds everything ---------------------
add_custom_target(wos_full
    COMMENT "WOS full build complete."
)
add_dependencies(wos_full
    check_headers
    wos_modules
    busybox
    dropbear
    gnu_make
    wos_tools_build_always
)
if(WOS_BUILD_DISK_IMAGES)
    add_dependencies(wos_full
        mountfs_disk
        boot_image
    )
endif()
if(WOS_BUILD_CLANG_FOR_WOS)
    add_dependencies(wos_full clang_for_wos)
endif()
if(WOS_BUILD_NINJA_FOR_WOS)
    add_dependencies(wos_full ninja_for_wos)
endif()
if(WOS_BUILD_CMAKE_FOR_WOS)
    add_dependencies(wos_full cmake_for_wos)
endif()
if(WOS_BUILD_CMAKE_FOR_HOST)
    add_dependencies(wos_full cmake_for_host)
endif()
if(WOS_BUILD_PYTHON_FOR_WOS)
    add_dependencies(wos_full python_for_wos)
endif()
if(WOS_BUILD_MESON_FOR_WOS)
    add_dependencies(wos_full meson_for_wos)
endif()
if(WOS_BUILD_NASM_FOR_WOS)
    add_dependencies(wos_full nasm_for_wos)
endif()
if(WOS_BUILD_BASH_FOR_WOS)
    add_dependencies(wos_full bash_for_wos)
endif()
if(WOS_BUILD_NCURSES_FOR_WOS OR WOS_BUILD_NANO_FOR_WOS)
    add_dependencies(wos_full ncurses_for_wos)
endif()
if(WOS_BUILD_NANO_FOR_WOS)
    add_dependencies(wos_full nano_for_wos)
endif()
if(WOS_BUILD_OPENSSL_FOR_WOS)
    add_dependencies(wos_full openssl_for_wos)
endif()
if(WOS_BUILD_CURL_FOR_WOS)
    add_dependencies(wos_full curl_for_wos)
endif()
if(WOS_BUILD_GIT_FOR_WOS)
    add_dependencies(wos_full git_for_wos)
endif()
