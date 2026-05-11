cmake_minimum_required(VERSION 3.20)

include(ExternalProject)
ExternalProject_Add(wos_tools_build
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/tools
    BINARY_DIR ${CMAKE_BINARY_DIR}/tools
    CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
        -DCMAKE_BUILD_TYPE=Release
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

# WOS modules (kernel, init, testprog, netd, httpd, perf, journal, wkictl, renderbench)
add_subdirectory(modules)

# ---------------------------------------------------------------------------
# Toolchain / external build targets
# ---------------------------------------------------------------------------

set(SYSROOT ${CMAKE_SOURCE_DIR}/toolchain/sysroot)
set(MLIBC_BUILD_DIR ${CMAKE_SOURCE_DIR}/toolchain/mlibc-build)
set(LIBCXX_BUILD_DIR ${CMAKE_SOURCE_DIR}/toolchain/libcxx-build)

# --- check_headers: validate #pragma once in kern and init headers ----------
add_custom_target(check_headers
    COMMAND sh ${CMAKE_SOURCE_DIR}/scripts/check_headers.sh ${CMAKE_SOURCE_DIR}/modules/kern
    COMMAND sh ${CMAKE_SOURCE_DIR}/scripts/check_headers.sh ${CMAKE_SOURCE_DIR}/modules/init
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking #pragma once in kernel/init headers..."
    USES_TERMINAL
)

# --- mlibc: build and install the C library ---------------------------------
# Glob all mlibc sources so the stamp invalidates when any file changes.
# New files require a CMake reconfigure (cmake -B build).
file(GLOB_RECURSE MLIBC_SOURCES
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
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_mlibc.sh ${CMAKE_BUILD_TYPE}
    COMMAND ${CMAKE_COMMAND} -E touch ${MLIBC_STAMP}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Building mlibc..."
    DEPENDS ${CMAKE_SOURCE_DIR}/scripts/build_mlibc.sh ${MLIBC_SOURCES}
    USES_TERMINAL
)
add_custom_target(mlibc DEPENDS ${MLIBC_STAMP})

# --- libcxx: build and install libc++ ---------------------------------------
set(LIBCXX_STAMP ${CMAKE_BINARY_DIR}/stamps/libcxx.stamp)
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
add_custom_target(libcxx DEPENDS ${LIBCXX_STAMP})

# --- busybox: cross-compile busybox for WOS ---------------------------------
add_custom_target(busybox
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_busybox.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Building busybox..."
    USES_TERMINAL
)
add_dependencies(busybox mlibc libcxx)

# --- dropbear: cross-compile dropbear SSH for WOS ---------------------------
add_custom_target(dropbear
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_dropbear.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Building dropbear..."
    USES_TERMINAL
)
add_dependencies(dropbear mlibc libcxx)

# --- mountfs_disk: create or delta-sync the XFS rootfs image ----------------
# If mountfs.qcow2 doesn't exist, create it from scratch.
# If it already exists, delta-sync changed binaries and libraries.
add_custom_target(mountfs_disk
    COMMAND bash -c "if [ -f '${CMAKE_SOURCE_DIR}/mountfs.qcow2' ]\; then '${CMAKE_SOURCE_DIR}/scripts/sync_rootfs.sh'\; else '${CMAKE_SOURCE_DIR}/scripts/create_mountfs_disk.sh'\; fi"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Syncing rootfs disk..."
    USES_TERMINAL
)
# Needs testprog and init binaries from the modules build
add_dependencies(mountfs_disk wos_modules)

# --- boot_image: create the boot disk + initramfs --------------------------
add_custom_target(boot_image
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/make_image.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Creating boot image..."
    USES_TERMINAL
)
add_dependencies(boot_image wos_modules mountfs_disk)

foreach(mod wos init testprog testd netd httpd perf journal wkictl renderbench)
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
