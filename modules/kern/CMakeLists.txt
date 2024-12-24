cmake_minimum_required(VERSION 3.16)
project(wos C CXX ASM_NASM)

# Set the output name for the final kernel executable
set(KERNEL wos)

# Set compilers
# set(CMAKE_C_COMPILER clang)
# set(CMAKE_CXX_COMPILER clang++)
# set(CMAKE_ASM_NASM_COMPILER nasm)
# # Set linker

# Internal flags
set(CMAKE_C_FLAGS "-O0 -g -pipe -Wall -Wextra -std=gnu11 -ffreestanding -nostdinc -fno-stack-protector -fno-stack-check -fno-exceptions -fno-lto -fPIE -m64 -march=x86-64 -mno-80387 -mno-mmx -mno-sse -mno-sse2 -mno-red-zone -Wformat -Wformat-security -Werror=format-security")
set(CMAKE_CXX_FLAGS "-O0 -g -pipe -Wall -Wextra -std=c++23 -nostdinc -ffreestanding -fno-stack-protector -fno-stack-check -fomit-frame-pointer -fno-lto -fPIE -m64 -march=x86-64 -mno-80387 -mno-mmx -mno-sse -mno-sse2 -mno-red-zone -Wformat -Wformat-security -Werror=format-security")
set(NASM_FLAGS "-Wall -f elf64 -I src -w-reloc-abs-qword -w-reloc-rel-dword")
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

target_link_libraries(${KERNEL} stdkern)

# Set compiler and linker flags
set_source_files_properties(${CFILES} PROPERTIES COMPILE_FLAGS ${CMAKE_C_FLAGS})
set_source_files_properties(${CXXFILES} PROPERTIES COMPILE_FLAGS ${CMAKE_CXX_FLAGS})
set_source_files_properties(${NASMFILES} PROPERTIES LANGUAGE ASM_NASM COMPILE_FLAGS ${NASM_FLAGS})
target_link_options(${KERNEL} PRIVATE ${CMAKE_LINK_FLAGS})

# Set NASM flags

# Custom command to modify the ELF file type
add_custom_command(TARGET ${KERNEL} POST_BUILD
    COMMAND printf '\\003' | dd of=$<TARGET_FILE:${KERNEL}> bs=1 count=1 seek=16 conv=notrunc 2>/dev/null
)

