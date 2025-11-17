cmake_minimum_required(VERSION 3.20)
project(wos C CXX ASM_NASM)

# Set the output name for the final kernel executable
set(KERNEL wos)

# Internal flags
set(CMAKE_LINK_FLAGS "LINKER:-m,elf_x86_64" "SHELL:-nostdlib" "SHELL:-z text" "SHELL:-z max-page-size=0x1000" "SHELL:-Wl,-T,${CMAKE_SOURCE_DIR}/modules/kern/linker.ld")

# Include directories

# Find source files
file(GLOB_RECURSE CFILES src/*.c)
file(GLOB_RECURSE ASFILES src/*.S)
file(GLOB_RECURSE NASMFILES src/*.asm)
file(GLOB_RECURSE CXXFILES src/*.cpp)

# Add executable
add_executable(${KERNEL} ${CFILES} ${ASFILES} ${NASMFILES} ${CXXFILES})

# Kernel-specific compile options: freestanding, no exceptions, no RTTI.
target_compile_options(${KERNEL} PRIVATE
    $<$<COMPILE_LANGUAGE:C>:-fno-exceptions>
    $<$<COMPILE_LANGUAGE:CXX>:-fno-exceptions;-fno-rtti>
)

# Target-scoped includes for kernel only (do NOT include userspace stdlib src here)
target_include_directories(${KERNEL} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/std
    # Avoid adding the project root here. Adding the root lets the
    # compiler find headers under modules/stdlib/src (for example
    # <std/...>) which collides with the toolchain libc++ headers.
    # Instead, add only the small public include sets the kernel needs.
    ${CMAKE_SOURCE_DIR}/modules
)

# Add external headers as SYSTEM so their C headers (e.g. extern/clang/stddef.h)
# don't shadow the toolchain's standard library headers. This ensures
# libc++'s <stddef.h> from the sysroot is found first.
## Do not add extern headers globally to kernel. If a specific extern header
## is required by a kernel source file, add a targeted include or forwarder.
target_include_directories(${KERNEL} SYSTEM PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/limine
)

target_link_options(${KERNEL} PRIVATE ${CMAKE_LINK_FLAGS})

# target_link_libraries(${KERNEL} stdkern)


# Custom command to modify the ELF file type
add_custom_command(TARGET ${KERNEL} POST_BUILD
    COMMAND printf '\\003' | dd of=$<TARGET_FILE:${KERNEL}> bs=1 count=1 seek=16 conv=notrunc 2>/dev/null
)
