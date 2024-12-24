cmake_minimum_required(VERSION 3.10)
project(stdlib)

set(CMAKE_C_FLAGS
	"-nostdlib \
	-std=c11 \
	-fPIC \
	-nostdinc \
	-g \
	-Wall \
	-Wextra \
	-Werror \
	-Wformat \
	-Wformat-security"
)
set(CMAKE_CXX_FLAGS
	"-nostdlib \
	-std=c++23 \
	-fPIC \
	-nostdinc \
	-g \
	-Wall \
	-Wextra \
	-Werror \
	-Wformat \
	-Wformat-security"
)

set(KERNEL_C_FLAGS
	"-nostdlib \
	-std=c11 \
	-fno-pic \
	-nostdinc \
	-g \
	-Wall \
	-Wextra \
	-Werror \
	-Wformat \
	-Wformat-security \
	-ffreestanding \
	-fno-stack-protector \
    -fno-threadsafe-statics \
	-fno-stack-check \
	-fno-exceptions \
	-fno-lto \
	-fPIE \
	-m64 \
	-march=x86-64 \
	-mno-80387 \
	-mno-mmx \
	-mno-sse \
	-mno-sse2 \
	-mno-red-zone"
)

set(KERNEL_CXX_FLAGS
	"-nostdlib \
	-std=c++23 \
	-fno-pic \
	-nostdinc \
	-g \
	-Wall \
	-Wextra \
	-Werror \
	-Wformat \
	-Wformat-security \
	-ffreestanding \
	-fno-stack-protector \
	-fno-stack-check \
	-fomit-frame-pointer \
    -fno-threadsafe-statics \
	-fno-lto \
	-fPIE \
	-m64 \
	-march=x86-64 \
	-mno-80387 \
	-mno-mmx \
	-mno-sse \
	-mno-sse2 \
	-mno-red-zone"
)

include_directories(src ../kern/src ../ ../extern ../extern/clang)

file(GLOB_RECURSE CFILES src/*.c)
file(GLOB_RECURSE ASFILES src/*.S)
file(GLOB_RECURSE NASMFILES src/*.asm)
file(GLOB_RECURSE CXXFILES src/*.cpp)

set(OBJ_FILES "")
foreach(file ${CFILES})
	set(OBJ_FILES ${OBJ_FILES} ${file})
endforeach()
foreach(file ${ASFILES})
	set(OBJ_FILES ${OBJ_FILES} ${file})
endforeach()
foreach(file ${NASMFILES})
	set(OBJ_FILES ${OBJ_FILES} ${file})
endforeach()
foreach(file ${CXXFILES})
	set(OBJ_FILES ${OBJ_FILES} ${file})
endforeach()

enable_language(ASM_NASM)
set_source_files_properties(${NASMFILES} PROPERTIES LANGUAGE ASM_NASM)

add_library(std STATIC ${OBJ_FILES})
target_compile_options(std PRIVATE ${C_FLAGS} ${CXX_FLAGS})

add_library(stdkern STATIC ${OBJ_FILES})
target_compile_options(stdkern PRIVATE ${COMBINED_C_FLAGS} ${COMBINED_CXX_FLAGS})
