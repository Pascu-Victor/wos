cmake_minimum_required(VERSION 3.20)

set(CMAKE_C_COMPILER_WORKS ON)
set(CMAKE_CXX_COMPILER_WORKS ON)

# Set paths to host tools and target sysroot. This file is used both from the
# repo root and directly via `cmake -S modules`, so derive paths from this file.
get_filename_component(WOS_REPO_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(WOS_MODULES_DIR "${CMAKE_CURRENT_LIST_DIR}")
set(WOS_HOST_TOOLCHAIN_PATH "${WOS_REPO_ROOT}/toolchain/host" CACHE PATH
    "Host toolchain used for Linux cross-builds")
set(HOST_PATH ${WOS_HOST_TOOLCHAIN_PATH})
set(WOS_REPO_SYSROOT_PATH "${WOS_REPO_ROOT}/toolchain/sysroot")
set(WOS_NATIVE_INSTALLED_SYSROOT_PATH "/")

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

option(WOS_NATIVE_USE_INSTALLED_SYSROOT
       "For native WOS builds, prefer the installed / sysroot over the repo toolchain/sysroot"
       OFF)

set(WOS_REPO_SYSROOT_READY OFF)
if(EXISTS "${WOS_REPO_SYSROOT_PATH}")
    if(EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libc.a" AND
       EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libc++.a" AND
       EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libc++abi.a" AND
       EXISTS "${WOS_REPO_SYSROOT_PATH}/lib/libunwind.a" AND
       EXISTS "${WOS_REPO_SYSROOT_PATH}/include/c++/v1")
        set(WOS_REPO_SYSROOT_READY ON)
    endif()
endif()

if(WOS_NATIVE_SYSTEM_BUILD AND (WOS_NATIVE_USE_INSTALLED_SYSROOT OR NOT WOS_REPO_SYSROOT_READY))
    set(WOS_DEFAULT_SYSROOT_PATH "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
else()
    set(WOS_DEFAULT_SYSROOT_PATH "${WOS_REPO_SYSROOT_PATH}")
endif()
set(WOS_SYSROOT_PATH "${WOS_DEFAULT_SYSROOT_PATH}" CACHE PATH "WOS target sysroot")
if(WOS_NATIVE_SYSTEM_BUILD AND NOT WOS_NATIVE_USE_INSTALLED_SYSROOT AND WOS_REPO_SYSROOT_READY AND
   WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(WOS_SYSROOT_PATH "${WOS_REPO_SYSROOT_PATH}" CACHE PATH "WOS target sysroot" FORCE)
endif()
set(SYSROOT_PATH ${WOS_SYSROOT_PATH})
set(WOS_SYSROOT_LIB_DIR "${SYSROOT_PATH}")
cmake_path(APPEND WOS_SYSROOT_LIB_DIR "lib")
if(WOS_NATIVE_SYSTEM_BUILD AND WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(WOS_SYSTEM_C_COMPILER "/usr/bin/clang" CACHE FILEPATH "Path to native WOS C compiler" FORCE)
    set(WOS_SYSTEM_CXX_COMPILER "/usr/bin/clang++" CACHE FILEPATH "Path to native WOS C++ compiler" FORCE)
    set(WOS_SYSTEM_LINKER "/usr/bin/ld.lld" CACHE FILEPATH "Path to native WOS linker" FORCE)
    set(WOS_SYSTEM_AR "/usr/bin/llvm-ar" CACHE FILEPATH "Path to native WOS archiver" FORCE)
    set(WOS_SYSTEM_RANLIB "/usr/bin/llvm-ranlib" CACHE FILEPATH "Path to native WOS ranlib" FORCE)
    set(WOS_SYSTEM_STRIP "/usr/bin/llvm-strip" CACHE FILEPATH "Path to native WOS strip" FORCE)
    set(WOS_SYSTEM_NM "/usr/bin/llvm-nm" CACHE FILEPATH "Path to native WOS nm" FORCE)
    set(WOS_SYSTEM_OBJDUMP "/usr/bin/llvm-objdump" CACHE FILEPATH "Path to native WOS objdump" FORCE)
    set(WOS_SYSTEM_OBJCOPY "/usr/bin/llvm-objcopy" CACHE FILEPATH "Path to native WOS objcopy" FORCE)
    set(WOS_SYSTEM_READELF "/usr/bin/llvm-readelf" CACHE FILEPATH "Path to native WOS readelf" FORCE)
    set(WOS_SYSTEM_DLLTOOL "/usr/bin/llvm-dlltool" CACHE FILEPATH "Path to native WOS dlltool" FORCE)
    set(WOS_SYSTEM_ADDR2LINE "/usr/bin/llvm-addr2line" CACHE FILEPATH "Path to native WOS addr2line" FORCE)
    set(WOS_SYSTEM_NASM "/usr/bin/nasm" CACHE FILEPATH "Path to native WOS NASM" FORCE)
elseif(WOS_NATIVE_SYSTEM_BUILD)
    find_program(WOS_SYSTEM_C_COMPILER NAMES clang cc REQUIRED)
    find_program(WOS_SYSTEM_CXX_COMPILER NAMES clang++ c++ REQUIRED)
    find_program(WOS_SYSTEM_LINKER NAMES ld.lld lld REQUIRED)
    find_program(WOS_SYSTEM_AR NAMES llvm-ar ar REQUIRED)
    find_program(WOS_SYSTEM_RANLIB NAMES llvm-ranlib ranlib REQUIRED)
    find_program(WOS_SYSTEM_STRIP NAMES llvm-strip strip REQUIRED)
    find_program(WOS_SYSTEM_NM NAMES llvm-nm nm REQUIRED)
    find_program(WOS_SYSTEM_OBJDUMP NAMES llvm-objdump objdump REQUIRED)
    find_program(WOS_SYSTEM_OBJCOPY NAMES llvm-objcopy objcopy REQUIRED)
    find_program(WOS_SYSTEM_READELF NAMES llvm-readelf readelf REQUIRED)
    find_program(WOS_SYSTEM_DLLTOOL NAMES llvm-dlltool dlltool REQUIRED)
    find_program(WOS_SYSTEM_ADDR2LINE NAMES llvm-addr2line addr2line REQUIRED)
    find_program(WOS_SYSTEM_NASM NAMES nasm REQUIRED)
endif()
if(WOS_NATIVE_SYSTEM_BUILD)
    set(CMAKE_C_COMPILER "${WOS_SYSTEM_C_COMPILER}" CACHE FILEPATH "Path to native WOS C compiler" FORCE)
    set(CMAKE_CXX_COMPILER "${WOS_SYSTEM_CXX_COMPILER}" CACHE FILEPATH "Path to native WOS C++ compiler" FORCE)
    set(CMAKE_ASM_NASM_COMPILER "${WOS_SYSTEM_NASM}" CACHE FILEPATH "Path to native WOS NASM" FORCE)
    set(CMAKE_AR "${WOS_SYSTEM_AR}" CACHE FILEPATH "Path to native WOS archiver" FORCE)
    set(CMAKE_RANLIB "${WOS_SYSTEM_RANLIB}" CACHE FILEPATH "Path to native WOS ranlib" FORCE)
    set(CMAKE_LINKER "${WOS_SYSTEM_LINKER}" CACHE FILEPATH "Path to native WOS linker" FORCE)
    set(CMAKE_STRIP "${WOS_SYSTEM_STRIP}" CACHE FILEPATH "Path to native WOS strip" FORCE)
    set(CMAKE_NM "${WOS_SYSTEM_NM}" CACHE FILEPATH "Path to native WOS nm" FORCE)
    set(CMAKE_OBJDUMP "${WOS_SYSTEM_OBJDUMP}" CACHE FILEPATH "Path to native WOS objdump" FORCE)
    set(CMAKE_OBJCOPY "${WOS_SYSTEM_OBJCOPY}" CACHE FILEPATH "Path to native WOS objcopy" FORCE)
    set(CMAKE_READELF "${WOS_SYSTEM_READELF}" CACHE FILEPATH "Path to native WOS readelf" FORCE)
    set(CMAKE_DLLTOOL "${WOS_SYSTEM_DLLTOOL}" CACHE FILEPATH "Path to native WOS dlltool" FORCE)
    set(CMAKE_ADDR2LINE "${WOS_SYSTEM_ADDR2LINE}" CACHE FILEPATH "Path to native WOS addr2line" FORCE)
else()
    set(CMAKE_C_COMPILER "${HOST_PATH}/bin/clang" CACHE INTERNAL "Path to clang" FORCE)
    set(CMAKE_CXX_COMPILER "${HOST_PATH}/bin/clang++" CACHE INTERNAL "Path to clang++" FORCE)
endif()
if(NOT WOS_NATIVE_SYSTEM_BUILD)
    set(CMAKE_LINKER_TYPE "LLD")
endif()

if(WOS_NATIVE_SYSTEM_BUILD AND WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(WOS_NATIVE_CLANG_VERSION "22.0.0")
    set(WOS_NATIVE_CLANG_RESOURCE_DIR "/usr/lib/clang/22")
    set(WOS_NATIVE_CLANG_RUNTIME_DIR "${WOS_NATIVE_CLANG_RESOURCE_DIR}/lib/x86_64-pc-wos")

    foreach(WOS_NATIVE_LANG IN ITEMS C CXX)
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_FORCED TRUE CACHE INTERNAL "Native WOS compiler metadata is provided by WOS" FORCE)
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_ID_RUN TRUE CACHE INTERNAL "Native WOS compiler identity is provided by WOS" FORCE)
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_ID "Clang")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_VERSION "${WOS_NATIVE_CLANG_VERSION}")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_VERSION_INTERNAL "")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_WRAPPER "")
        set(CMAKE_${WOS_NATIVE_LANG}_PLATFORM_ID "")
        set(CMAKE_${WOS_NATIVE_LANG}_SIMULATE_ID "")
        set(CMAKE_${WOS_NATIVE_LANG}_SIMULATE_VERSION "")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_FRONTEND_VARIANT "GNU")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_ARCHITECTURE_ID "x86_64")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_WORKS TRUE)
        set(CMAKE_${WOS_NATIVE_LANG}_ABI_COMPILED TRUE)
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_AR "${WOS_SYSTEM_AR}")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_RANLIB "${WOS_SYSTEM_RANLIB}")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_LINKER "${WOS_SYSTEM_LINKER}")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_LINKER_ARCHITECTURE_FLAGS "")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_LINKER_ID "LLD")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_LINKER_VERSION "${WOS_NATIVE_CLANG_VERSION}")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_LINKER_FRONTEND_VARIANT "GNU")
        set(CMAKE_${WOS_NATIVE_LANG}_LINKER_DEPFILE_SUPPORTED TRUE)
        set(CMAKE_${WOS_NATIVE_LANG}_LINKER_PUSHPOP_STATE_SUPPORTED TRUE)
        set(CMAKE_${WOS_NATIVE_LANG}_SIZEOF_DATA_PTR "8")
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILER_ABI "ELF")
        set(CMAKE_${WOS_NATIVE_LANG}_BYTE_ORDER "LITTLE_ENDIAN")
        set(CMAKE_${WOS_NATIVE_LANG}_LIBRARY_ARCHITECTURE "")
        set(CMAKE_${WOS_NATIVE_LANG}_IMPLICIT_LINK_DIRECTORIES "${WOS_NATIVE_CLANG_RUNTIME_DIR};/lib")
        set(CMAKE_${WOS_NATIVE_LANG}_IMPLICIT_LINK_FRAMEWORK_DIRECTORIES "")
        set(_CMAKE_${WOS_NATIVE_LANG}_IPO_SUPPORTED_BY_CMAKE YES CACHE INTERNAL "Native WOS Clang IPO support is known" FORCE)
        set(_CMAKE_${WOS_NATIVE_LANG}_IPO_MAY_BE_SUPPORTED_BY_COMPILER YES CACHE INTERNAL "Native WOS Clang IPO support is known" FORCE)
        set(CMAKE_${WOS_NATIVE_LANG}_COMPILE_OPTIONS_IPO "-flto=thin" CACHE INTERNAL "Native WOS Clang IPO compile options" FORCE)
        set(CMAKE_${WOS_NATIVE_LANG}_ARCHIVE_CREATE_IPO "\"${WOS_SYSTEM_AR}\" qc <TARGET> <LINK_FLAGS> <OBJECTS>" CACHE INTERNAL
            "Native WOS Clang IPO archive create rule" FORCE)
        set(CMAKE_${WOS_NATIVE_LANG}_ARCHIVE_APPEND_IPO "\"${WOS_SYSTEM_AR}\" q <TARGET> <LINK_FLAGS> <OBJECTS>" CACHE INTERNAL
            "Native WOS Clang IPO archive append rule" FORCE)
        set(CMAKE_${WOS_NATIVE_LANG}_ARCHIVE_FINISH_IPO "\"${WOS_SYSTEM_RANLIB}\" <TARGET>" CACHE INTERNAL
            "Native WOS Clang IPO archive finish rule" FORCE)
    endforeach()
    set(CMAKE_LINKER_PUSHPOP_STATE_SUPPORTED TRUE)

    set(CMAKE_C_STANDARD_COMPUTED_DEFAULT "17")
    set(CMAKE_C_EXTENSIONS_COMPUTED_DEFAULT "ON")
    set(CMAKE_C_STANDARD_LATEST "23")
    set(CMAKE_C_IMPLICIT_INCLUDE_DIRECTORIES "${WOS_NATIVE_CLANG_RESOURCE_DIR}/include;/usr/include")
    set(CMAKE_C_IMPLICIT_LINK_LIBRARIES "unwind;c")
    set(CMAKE_C_COMPILE_FEATURES c_std_90 c_function_prototypes c_std_99 c_restrict c_variadic_macros c_std_11 c_static_assert c_std_17 c_std_23)
    set(CMAKE_C90_COMPILE_FEATURES c_std_90 c_function_prototypes)
    set(CMAKE_C99_COMPILE_FEATURES c_std_99 c_restrict c_variadic_macros)
    set(CMAKE_C11_COMPILE_FEATURES c_std_11 c_static_assert)
    set(CMAKE_C17_COMPILE_FEATURES c_std_17)
    set(CMAKE_C23_COMPILE_FEATURES c_std_23)

    set(CMAKE_CXX_STANDARD_COMPUTED_DEFAULT "17")
    set(CMAKE_CXX_EXTENSIONS_COMPUTED_DEFAULT "ON")
    set(CMAKE_CXX_STANDARD_LATEST "26")
    set(CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES "/usr/include/c++/v1;${WOS_NATIVE_CLANG_RESOURCE_DIR}/include;/usr/include")
    set(CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "c++;c++abi;m;unwind;c")
    set(CMAKE_CXX_COMPILER_CLANG_RESOURCE_DIR "${WOS_NATIVE_CLANG_RESOURCE_DIR}")
    set(CMAKE_CXX_COMPILE_FEATURES cxx_std_98 cxx_template_template_parameters cxx_std_11 cxx_alias_templates cxx_alignas cxx_alignof cxx_attributes cxx_auto_type cxx_constexpr cxx_decltype cxx_decltype_incomplete_return_types cxx_default_function_template_args cxx_defaulted_functions cxx_defaulted_move_initializers cxx_delegating_constructors cxx_deleted_functions cxx_enum_forward_declarations cxx_explicit_conversions cxx_extended_friend_declarations cxx_extern_templates cxx_final cxx_func_identifier cxx_generalized_initializers cxx_inheriting_constructors cxx_inline_namespaces cxx_lambdas cxx_local_type_template_args cxx_long_long_type cxx_noexcept cxx_nonstatic_member_init cxx_nullptr cxx_override cxx_range_for cxx_raw_string_literals cxx_reference_qualified_functions cxx_right_angle_brackets cxx_rvalue_references cxx_sizeof_member cxx_static_assert cxx_strong_enums cxx_thread_local cxx_trailing_return_types cxx_unicode_literals cxx_uniform_initialization cxx_unrestricted_unions cxx_user_literals cxx_variadic_macros cxx_variadic_templates cxx_std_14 cxx_aggregate_default_initializers cxx_attribute_deprecated cxx_binary_literals cxx_contextual_conversions cxx_decltype_auto cxx_digit_separators cxx_generic_lambdas cxx_lambda_init_captures cxx_relaxed_constexpr cxx_return_type_deduction cxx_variable_templates cxx_std_17 cxx_std_20 cxx_std_23 cxx_std_26)
    set(CMAKE_CXX98_COMPILE_FEATURES cxx_std_98 cxx_template_template_parameters)
    set(CMAKE_CXX11_COMPILE_FEATURES cxx_std_11 cxx_alias_templates cxx_alignas cxx_alignof cxx_attributes cxx_auto_type cxx_constexpr cxx_decltype cxx_decltype_incomplete_return_types cxx_default_function_template_args cxx_defaulted_functions cxx_defaulted_move_initializers cxx_delegating_constructors cxx_deleted_functions cxx_enum_forward_declarations cxx_explicit_conversions cxx_extended_friend_declarations cxx_extern_templates cxx_final cxx_func_identifier cxx_generalized_initializers cxx_inheriting_constructors cxx_inline_namespaces cxx_lambdas cxx_local_type_template_args cxx_long_long_type cxx_noexcept cxx_nonstatic_member_init cxx_nullptr cxx_override cxx_range_for cxx_raw_string_literals cxx_reference_qualified_functions cxx_right_angle_brackets cxx_rvalue_references cxx_sizeof_member cxx_static_assert cxx_strong_enums cxx_thread_local cxx_trailing_return_types cxx_unicode_literals cxx_uniform_initialization cxx_unrestricted_unions cxx_user_literals cxx_variadic_macros cxx_variadic_templates)
    set(CMAKE_CXX14_COMPILE_FEATURES cxx_std_14 cxx_aggregate_default_initializers cxx_attribute_deprecated cxx_binary_literals cxx_contextual_conversions cxx_decltype_auto cxx_digit_separators cxx_generic_lambdas cxx_lambda_init_captures cxx_relaxed_constexpr cxx_return_type_deduction cxx_variable_templates)
    set(CMAKE_CXX17_COMPILE_FEATURES cxx_std_17)
    set(CMAKE_CXX20_COMPILE_FEATURES cxx_std_20)
    set(CMAKE_CXX23_COMPILE_FEATURES cxx_std_23)
    set(CMAKE_CXX26_COMPILE_FEATURES cxx_std_26)

    set(CMAKE_ASM_NASM_COMPILER_ID "NASM")
    set(CMAKE_ASM_NASM_COMPILER_VERSION "")
    set(CMAKE_ASM_NASM_COMPILER_FORCED TRUE CACHE INTERNAL "Native WOS NASM metadata is provided by WOS" FORCE)
    set(CMAKE_ASM_NASM_COMPILER_ID_RUN TRUE CACHE INTERNAL "Native WOS NASM identity is provided by WOS" FORCE)
    set(CMAKE_ASM_NASM_COMPILER_WORKS TRUE CACHE INTERNAL "Native WOS NASM compiler is known-good" FORCE)
    set(CMAKE_ASM_NASM_COMPILER_ID_VENDOR_MATCH [==[NASM version]==])
    set(CMAKE_ASM_NASM_COMPILER_ARCHITECTURE_ID "")
    set(CMAKE_ASM_NASM_COMPILER_ENV_VAR "ASM_NASM")
    set(CMAKE_ASM_NASM_IGNORE_EXTENSIONS h H o O obj OBJ def DEF rc RC)
    set(CMAKE_ASM_NASM_LINKER_PREFERENCE 0)
    set(CMAKE_ASM_NASM_LINKER_DEPFILE_SUPPORTED "")
    set(CMAKE_ASM_NASM_LINKER_PUSHPOP_STATE_SUPPORTED "")
endif()

set(WOS_USE_CCACHE_DEFAULT ON)
if(WOS_NATIVE_SYSTEM_BUILD)
    set(WOS_USE_CCACHE_DEFAULT OFF)
endif()
option(WOS_USE_CCACHE "Use ccache/cccache as a compiler launcher when available" ${WOS_USE_CCACHE_DEFAULT})
set(WOS_CCACHE_PROGRAM "" CACHE FILEPATH "Path to ccache/cccache")
if(WOS_USE_CCACHE AND NOT WOS_CCACHE_PROGRAM)
    find_program(WOS_CCACHE_PROGRAM NAMES ccache cccache)
endif()
if(WOS_USE_CCACHE AND WOS_CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${WOS_CCACHE_PROGRAM}" CACHE STRING "C compiler launcher" FORCE)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${WOS_CCACHE_PROGRAM}" CACHE STRING "C++ compiler launcher" FORCE)
endif()

# Configure sysroot for cross-compilation. Native WOS builds can also reuse the
# checked-out repo sysroot when it is populated with the required runtime libs.
set(CMAKE_SYSROOT "${SYSROOT_PATH}")
if(NOT WOS_NATIVE_SYSTEM_BUILD OR NOT WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(CMAKE_FIND_ROOT_PATH "${SYSROOT_PATH}")
    set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
    set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
    set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(wos_modules C CXX ASM_NASM)

# Disable C++ modules support which requires experimental compiler features
set(CMAKE_CXX_SCAN_FOR_MODULES OFF CACHE BOOL "Disable C++ modules" FORCE)

option(WOS_UASAN "Enable AddressSanitizer instrumentation for WOS userspace modules" OFF)
set(WOS_UASAN_RUNTIME_DIR "" CACHE PATH "Clang runtime directory containing WOS AddressSanitizer libraries")
option(WOS_USERSPACE_COVERAGE "Enable LLVM source-based coverage instrumentation for WOS userspace modules" OFF)
option(WOS_USERSPACE_NO_AVX "Build WOS userspace without AVX for FPU-state diagnostics" OFF)
option(WOS_DYNAMIC_USERSPACE_PIE "Build dynamic WOS userspace executables as PIE" OFF)
option(WOS_DYNAMIC_USERSPACE_NO_PLT "Use GOT calls instead of PLT stubs for dynamic WOS userspace executables" OFF)

set(WOS_SYSROOT_COMPILE_OPTIONS "--sysroot=${SYSROOT_PATH}")
if(WOS_NATIVE_SYSTEM_BUILD AND WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")
    set(WOS_C_STDLIB_INCLUDE "/usr/include")
    set(WOS_CXX_STDLIB_INCLUDE "/usr/include/c++/v1")
    set(WOS_STDLIB_LINK_DIR "/usr/lib")
else()
    set(WOS_C_STDLIB_INCLUDE "${SYSROOT_PATH}/include")
    set(WOS_CXX_STDLIB_INCLUDE "${SYSROOT_PATH}/include/c++/v1")
    set(WOS_STDLIB_LINK_DIR "${WOS_SYSROOT_LIB_DIR}")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(WOS_USERSPACE_CPU_OPTIONS -march=native)
if(WOS_USERSPACE_NO_AVX)
    list(APPEND WOS_USERSPACE_CPU_OPTIONS -mno-avx -mno-avx2 -mno-fma -mno-f16c -fno-vectorize -fno-slp-vectorize)
endif()
add_compile_options(
    # Common flags for C and C++
    "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-O3;-g;-flto;-fdiagnostics-color=always;-fno-stack-protector;-fno-stack-check;-fno-sanitize=safe-stack;-pipe;-Wall;-Wextra;-pedantic;-Wno-gnu-anonymous-struct;-Wno-c99-extensions;-Wno-zero-length-array;-m64;-Wformat;-Wformat-security;-Werror=format-security;${WOS_SYSROOT_COMPILE_OPTIONS};-fno-omit-frame-pointer>"
    # C-specific flags
    "$<$<COMPILE_LANGUAGE:C>:-std=gnu11>"
    "$<$<COMPILE_LANGUAGE:C>:SHELL:-isystem ${WOS_C_STDLIB_INCLUDE}>"
    # C++-specific flags
    "$<$<COMPILE_LANGUAGE:CXX>:-std=c++23>"
    "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-isystem ${WOS_CXX_STDLIB_INCLUDE}>"
    "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-isystem ${WOS_C_STDLIB_INCLUDE}>"
    # NASM flags
    "$<$<COMPILE_LANGUAGE:ASM_NASM>:-g;-Wall;-felf64;-Isrc;-w-reloc-abs-qword;-w-reloc-rel-dword>"
)

# Linker flags to use sysroot libraries
set(WOS_BASE_LINK_OPTIONS
    "--sysroot=${SYSROOT_PATH}"
    "-fuse-ld=lld"
    "-L${WOS_STDLIB_LINK_DIR}"
    "-std=c++23"
    "-flto"
)
add_link_options(${WOS_BASE_LINK_OPTIONS})

# Set the linker to use from the selected toolchain
if(WOS_NATIVE_SYSTEM_BUILD)
    set(CMAKE_LINKER "${WOS_SYSTEM_LINKER}" CACHE FILEPATH "Path to native WOS lld linker" FORCE)
else()
    set(CMAKE_LINKER "${HOST_PATH}/bin/ld.lld" CACHE INTERNAL "Path to lld linker" FORCE)
endif()

#set target
set(TARGET wos_modules)

#build wos related subprojects
add_subdirectory(kern)

set(WOS_USERSPACE_TARGETS
    init
    testprog
    testd
    netd
    httpd
    debugserver
    perf
    top
    memacc
    journal
    journal_lib
    wkictl
    powerctl
    renderbench
    strace
    sftpserver
)
set(WOS_USERSPACE_TARGETS ${WOS_USERSPACE_TARGETS} PARENT_SCOPE)
set(WOS_USERSPACE_LOGGING_TARGETS
    init
)
set(WOS_DYNAMIC_USERSPACE_EXECUTABLE_TARGETS
    testprog
    testd
    httpd
    debugserver
    perf
    top
    memacc
    journal
    wkictl
    powerctl
    renderbench
    strace
    sftpserver
)

if(WOS_UASAN)
    if(NOT WOS_UASAN_RUNTIME_DIR)
        execute_process(
            COMMAND ${CMAKE_C_COMPILER} -print-runtime-dir
            OUTPUT_VARIABLE WOS_UASAN_RUNTIME_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY
        )
    endif()

    foreach(runtime_lib IN ITEMS libclang_rt.asan_static.a libclang_rt.asan.a libclang_rt.asan_cxx.a)
        if(NOT EXISTS "${WOS_UASAN_RUNTIME_DIR}/${runtime_lib}")
            message(FATAL_ERROR
                "WOS_UASAN requires ${runtime_lib} in ${WOS_UASAN_RUNTIME_DIR}. "
                "The current WOS toolchain must build and install the ASAN compiler-rt runtime first.")
        endif()
    endforeach()
endif()

add_subdirectory(init)
add_subdirectory(testprog)
add_subdirectory(testd)
add_subdirectory(netd)
add_subdirectory(httpd)
add_subdirectory(debugserver)
add_subdirectory(perf)
add_subdirectory(top)
add_subdirectory(memacc)
add_subdirectory(journal)
add_subdirectory(wkictl)
add_subdirectory(powerctl)
add_subdirectory(renderbench)
add_subdirectory(strace)
add_subdirectory(sftpserver)

foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
    if(TARGET ${user_target})
        target_compile_options(${user_target} PRIVATE
            "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:${WOS_USERSPACE_CPU_OPTIONS}>"
        )
    endif()
endforeach()

function(wos_strip_link_flag out_value in_value flag)
    string(REPLACE "${flag}" "" stripped_value "${in_value}")
    string(REGEX REPLACE "[ \t]+" " " stripped_value "${stripped_value}")
    string(STRIP "${stripped_value}" stripped_value)
    set(${out_value} "${stripped_value}" PARENT_SCOPE)
endfunction()

function(wos_make_target_dynamic_user_executable user_target)
    get_target_property(existing_link_flags ${user_target} LINK_FLAGS)
    if(NOT existing_link_flags)
        set(existing_link_flags "")
    endif()
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-static")
    if(WOS_DYNAMIC_USERSPACE_PIE)
        wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-no-pie")
    endif()
    wos_strip_link_flag(existing_link_flags "${existing_link_flags}" "-Wl,--no-dynamic-linker")
    set_target_properties(${user_target} PROPERTIES
        LINK_FLAGS "${existing_link_flags}"
        POSITION_INDEPENDENT_CODE ${WOS_DYNAMIC_USERSPACE_PIE}
        SKIP_BUILD_RPATH ON
        BUILD_RPATH ""
        INSTALL_RPATH ""
    )

    get_target_property(existing_link_options ${user_target} LINK_OPTIONS)
    if(existing_link_options)
        list(REMOVE_ITEM existing_link_options
            "-static"
            "-Wl,--no-dynamic-linker"
            "-Wl,-T,${WOS_MODULES_DIR}/init/service.ld"
        )
        if(WOS_DYNAMIC_USERSPACE_PIE)
            list(REMOVE_ITEM existing_link_options "-no-pie")
        endif()
        set_target_properties(${user_target} PROPERTIES LINK_OPTIONS "${existing_link_options}")
    endif()

    if(NOT WOS_DYNAMIC_USERSPACE_PIE)
        target_link_options(${user_target} PRIVATE "-Wl,--no-pie")
    endif()

    if(WOS_DYNAMIC_USERSPACE_NO_PLT)
        target_compile_options(${user_target} PRIVATE "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fno-plt>")
    endif()
endfunction()

foreach(user_target IN LISTS WOS_DYNAMIC_USERSPACE_EXECUTABLE_TARGETS)
    if(TARGET ${user_target})
        wos_make_target_dynamic_user_executable(${user_target})
    endif()
endforeach()

if(WOS_UASAN)
    foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
        if(TARGET ${user_target})
            wos_make_target_dynamic_user_executable(${user_target})
            target_compile_options(${user_target} PRIVATE
                "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize=address;-fno-omit-frame-pointer;-fno-optimize-sibling-calls>"
            )
            target_link_options(${user_target} PRIVATE -fsanitize=address)
            target_compile_definitions(${user_target} PRIVATE WOS_UASAN=1)
        endif()
    endforeach()

    foreach(user_target IN LISTS WOS_USERSPACE_LOGGING_TARGETS)
        if(TARGET ${user_target} AND TARGET journal_lib)
            set_property(TARGET ${user_target} APPEND PROPERTY LINK_LIBRARIES journal_lib)
        endif()
    endforeach()
endif()

if(WOS_USERSPACE_COVERAGE)
    foreach(user_target IN LISTS WOS_USERSPACE_TARGETS)
        if(TARGET ${user_target})
            set_target_properties(${user_target} PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
            target_compile_options(${user_target} PRIVATE
                "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fprofile-instr-generate;-fcoverage-mapping;-mllvm;-runtime-counter-relocation;-fno-omit-frame-pointer;-fno-lto>"
            )
            target_link_options(${user_target} PRIVATE
                -fprofile-instr-generate
                -fno-lto
            )
            target_compile_definitions(${user_target} PRIVATE WOS_USERSPACE_COVERAGE=1)
        endif()
    endforeach()
endif()

# Aggregate target for all WOS modules
add_custom_target(wos_modules)
add_dependencies(wos_modules wos journal_lib init testprog testd netd httpd debugserver perf top memacc journal wkictl powerctl renderbench strace sftpserver)
