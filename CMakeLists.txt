cmake_minimum_required(VERSION 3.20)

include(ExternalProject)

option(WOS_BUILD_LINUX_BENCHMARKS "Build Linux host benchmark binaries alongside WOS artifacts" OFF)
option(WOS_USE_CCACHE "Use ccache/cccache as a compiler launcher when available" ON)
set(WOS_CCACHE_PROGRAM "" CACHE FILEPATH "Path to ccache/cccache")

if(WOS_USE_CCACHE AND NOT WOS_CCACHE_PROGRAM)
    find_program(WOS_CCACHE_PROGRAM NAMES ccache cccache)
endif()

set(WOS_TOOLS_CCACHE_CMAKE_ARGS -DWOS_USE_CCACHE=${WOS_USE_CCACHE})
if(WOS_CCACHE_PROGRAM)
    list(APPEND WOS_TOOLS_CCACHE_CMAKE_ARGS -DWOS_CCACHE_PROGRAM=${WOS_CCACHE_PROGRAM})
endif()

ExternalProject_Add(wos_tools_build
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/tools
    BINARY_DIR ${CMAKE_BINARY_DIR}/tools
    CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
        -DCMAKE_BUILD_TYPE=Release
        -DWOS_BUILD_LINUX_BENCHMARKS=${WOS_BUILD_LINUX_BENCHMARKS}
        ${WOS_TOOLS_CCACHE_CMAKE_ARGS}
    INSTALL_COMMAND ""
    USES_TERMINAL_CONFIGURE TRUE
    USES_TERMINAL_BUILD TRUE
)

# Always run the build step
ExternalProject_Add_StepTargets(wos_tools_build build)
add_custom_target(wos_tools_build_always ALL
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/tools
    DEPENDS wos_tools_build-build
    COMMENT "Building WOS tools..."
)

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
set(WOS_DROPBEAR_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/dropbear-build" CACHE PATH "Dropbear build directory")
set(WOS_CLANG_FOR_WOS_BUILD_DIR "${CMAKE_SOURCE_DIR}/toolchain/clang-wos-build" CACHE PATH "Native WOS clang build directory")
set(WOS_BOOT_DISK "${CMAKE_SOURCE_DIR}/disk.qcow2" CACHE FILEPATH "WOS boot disk image")
set(WOS_ROOTFS_DISK "${CMAKE_SOURCE_DIR}/mountfs.qcow2" CACHE FILEPATH "WOS rootfs disk image")
option(WOS_SKIP_LIBCXX_INSTALL "Skip the libc++ install target; useful when an isolated sysroot is pre-seeded" OFF)
option(WOS_BUILD_CLANG_FOR_WOS "Build a native WOS clang/lld toolchain and stage it into the rootfs" ON)

set(SYSROOT ${WOS_SYSROOT_PATH})
set(MLIBC_BUILD_DIR ${WOS_MLIBC_BUILD_DIR})
set(LIBCXX_BUILD_DIR ${WOS_LIBCXX_BUILD_DIR})
set(WOS_BUILD_SCRIPTS_DIR ${CMAKE_SOURCE_DIR}/scripts/build)
set(WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX "/(\\.git|\\.github|debian|docs?|testsuite|test|fuzz|manpages)(/|$)")

# --- check_headers: validate #pragma once in kern and init headers ----------
file(GLOB_RECURSE WOS_HEADER_CHECK_FILES CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/modules/kern/*.hpp
    ${CMAKE_SOURCE_DIR}/modules/init/*.hpp
)
set(CHECK_HEADERS_STAMP ${CMAKE_BINARY_DIR}/stamps/check_headers.stamp)
add_custom_command(
    OUTPUT ${CHECK_HEADERS_STAMP}
    COMMAND sh ${WOS_BUILD_SCRIPTS_DIR}/check_headers.sh ${CMAKE_SOURCE_DIR}/modules/kern
    COMMAND sh ${WOS_BUILD_SCRIPTS_DIR}/check_headers.sh ${CMAKE_SOURCE_DIR}/modules/init
    COMMAND ${CMAKE_COMMAND} -E touch ${CHECK_HEADERS_STAMP}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking #pragma once in kernel/init headers..."
    DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/check_headers.sh ${WOS_HEADER_CHECK_FILES}
    USES_TERMINAL
)
add_custom_target(check_headers DEPENDS ${CHECK_HEADERS_STAMP})

# --- mlibc: build and install the C library ---------------------------------
# Glob all mlibc sources so the stamp invalidates when any file changes,
# including source additions discovered by Ninja's CMake regeneration check.
file(GLOB_RECURSE MLIBC_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.cpp
    ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.c
    ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.hpp
    ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.h
    ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/*.S
    ${CMAKE_SOURCE_DIR}/toolchain/src/mlibc/meson.build
)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/stamps)
set(MLIBC_STAMP ${CMAKE_BINARY_DIR}/stamps/mlibc.stamp)
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
    USES_TERMINAL
)
add_custom_target(mlibc DEPENDS ${MLIBC_STAMP})

# --- libcxx: build and install libc++ ---------------------------------------
set(LIBCXX_STAMP ${CMAKE_BINARY_DIR}/stamps/libcxx.stamp)
if(WOS_SKIP_LIBCXX_INSTALL)
    add_custom_command(
        OUTPUT ${LIBCXX_STAMP}
        COMMAND ${CMAKE_COMMAND} -E echo "Skipping libc++ install using pre-seeded sysroot ${WOS_SYSROOT_PATH}"
        COMMAND ${CMAKE_COMMAND} -E touch ${LIBCXX_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Using pre-seeded libc++ in isolated sysroot..."
        DEPENDS ${MLIBC_STAMP}
        USES_TERMINAL
    )
else()
    add_custom_command(
        OUTPUT ${LIBCXX_STAMP}
        COMMAND ninja -C ${LIBCXX_BUILD_DIR}
        COMMAND ninja -C ${LIBCXX_BUILD_DIR} install
        COMMAND ${CMAKE_COMMAND} -E touch ${LIBCXX_STAMP}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building libc++..."
        DEPENDS ${MLIBC_STAMP}
        USES_TERMINAL
    )
endif()
add_custom_target(libcxx DEPENDS ${LIBCXX_STAMP})

# --- busybox: cross-compile busybox for WOS ---------------------------------
file(GLOB_RECURSE BUSYBOX_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/toolchain/src/busybox/*
)
list(FILTER BUSYBOX_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
set(BUSYBOX_STAMP ${CMAKE_BINARY_DIR}/stamps/busybox.stamp)
add_custom_command(
    OUTPUT ${BUSYBOX_STAMP}
    COMMAND ${CMAKE_COMMAND} -E env
            WOS_SYSROOT_PATH=${WOS_SYSROOT_PATH}
            WOS_BUSYBOX_BUILD_DIR=${WOS_BUSYBOX_BUILD_DIR}
            WOS_BUSYBOX_INSTALL_DIR=${WOS_BUSYBOX_INSTALL_DIR}
            ${WOS_BUILD_SCRIPTS_DIR}/build_busybox.sh
    COMMAND ${CMAKE_COMMAND} -E touch ${BUSYBOX_STAMP}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Building busybox..."
    DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_busybox.sh ${BUSYBOX_SOURCES} ${MLIBC_STAMP} ${LIBCXX_STAMP}
    USES_TERMINAL
)
add_custom_target(busybox DEPENDS ${BUSYBOX_STAMP})

# --- dropbear: cross-compile dropbear SSH for WOS ---------------------------
file(GLOB_RECURSE DROPBEAR_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/toolchain/src/dropbear/*
)
list(FILTER DROPBEAR_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
set(DROPBEAR_STAMP ${CMAKE_BINARY_DIR}/stamps/dropbear.stamp)
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
    USES_TERMINAL
)
add_custom_target(dropbear DEPENDS ${DROPBEAR_STAMP})

# --- clang_for_wos: cross-compile clang/lld to run inside WOS ---------------
file(GLOB_RECURSE CLANG_FOR_WOS_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/llvm/*
    ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/clang/*
    ${CMAKE_SOURCE_DIR}/toolchain/src/llvm-project/lld/*
)
list(FILTER CLANG_FOR_WOS_SOURCES EXCLUDE REGEX "${WOS_EXTERNAL_SOURCE_EXCLUDE_REGEX}")
set(CLANG_FOR_WOS_STAMP ${CMAKE_BINARY_DIR}/stamps/clang_for_wos.stamp)
set(WOS_CLANG_FOR_WOS_ROOTFS_INPUTS)
if(WOS_BUILD_CLANG_FOR_WOS)
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
            ${MLIBC_STAMP}
            ${LIBCXX_STAMP}
        USES_TERMINAL
    )
    add_custom_target(clang_for_wos DEPENDS ${CLANG_FOR_WOS_STAMP})
    list(APPEND WOS_CLANG_FOR_WOS_ROOTFS_INPUTS ${CLANG_FOR_WOS_STAMP})
endif()

# --- mountfs_disk: create or delta-sync the XFS rootfs image ----------------
# If mountfs.qcow2 doesn't exist, create it from scratch.
# If it already exists, delta-sync changed binaries and libraries.
file(GLOB_RECURSE WOS_ROOTFS_CONFIG_INPUTS CONFIGURE_DEPENDS
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
        ${WOS_BUILD_SCRIPTS_DIR}/rootfs_common.sh
        ${WOS_BUILD_SCRIPTS_DIR}/sync_rootfs.sh
        ${CMAKE_SOURCE_DIR}/configs/system.conf
        ${CMAKE_SOURCE_DIR}/configs/vfstab
        ${WOS_ROOTFS_CONFIG_INPUTS}
        ${WOS_MODULE_TARGET_FILES}
        ${BUSYBOX_STAMP}
        ${DROPBEAR_STAMP}
        ${LIBCXX_STAMP}
        ${WOS_CLANG_FOR_WOS_ROOTFS_INPUTS}
    USES_TERMINAL
)
add_custom_target(mountfs_disk DEPENDS ${WOS_ROOTFS_DISK} ${MOUNTFS_STAMP})
if(WOS_BUILD_CLANG_FOR_WOS)
    add_dependencies(mountfs_disk clang_for_wos)
endif()

# --- boot_image: create the boot disk + initramfs --------------------------
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
    USES_TERMINAL
)
add_custom_target(boot_image DEPENDS ${WOS_BOOT_DISK} ${INITRAMFS_OUTPUT} ${BOOT_IMAGE_STAMP})

foreach(mod wos init testprog testd netd httpd perf top memacc journal wkictl powerctl renderbench sftpserver)
    add_dependencies(${mod} mlibc libcxx)
endforeach()
# busybox and dropbear are only needed for image creation, not compilation.
add_dependencies(boot_image busybox dropbear)

# --- wos_full: top-level target that builds everything ---------------------
add_custom_target(wos_full
    COMMENT "WOS full build complete."
)
add_dependencies(wos_full
    check_headers
    wos_modules
    mountfs_disk
    boot_image
    wos_tools_build_always
)
if(WOS_BUILD_CLANG_FOR_WOS)
    add_dependencies(wos_full clang_for_wos)
endif()
