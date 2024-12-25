cmake_minimum_required(VERSION 3.20)
project(wos C CXX ASM_NASM)

# Set the output name for the final kernel executable
set(KERNEL wos)

# Internal flags
set(CMAKE_LINK_FLAGS "LINKER:-m,elf_x86_64" "SHELL:-nostdlib" "SHELL:-pie" "SHELL:-z text" "SHELL:-z max-page-size=0x1000" "SHELL:-T${CMAKE_SOURCE_DIR}/modules/kern/linker.ld")

# Include directories
include_directories(src ../extern ../ ../stdlib/src ../extern/clang)

# Find source files
file(GLOB_RECURSE CFILES src/*.c)
file(GLOB_RECURSE ASFILES src/*.S)
file(GLOB_RECURSE NASMFILES src/*.asm)
file(GLOB_RECURSE CXXFILES src/*.cpp)

# Add executable
add_executable(${KERNEL} ${CFILES} ${ASFILES} ${NASMFILES} ${CXXFILES})

target_link_options(${KERNEL} PRIVATE ${CMAKE_LINK_FLAGS})

# Link libraries
target_link_libraries(${KERNEL} stdkern)


# Custom command to modify the ELF file type
add_custom_command(TARGET ${KERNEL} POST_BUILD
    COMMAND printf '\\003' | dd of=$<TARGET_FILE:${KERNEL}> bs=1 count=1 seek=16 conv=notrunc 2>/dev/null
)

