cmake_minimum_required(VERSION 3.20)
project(wos C CXX ASM_NASM)

# Set the output name for the final kernel executable
set(KERNEL wos)

# ---------------------------------------------------------------------------
# Sanitizer / instrumentation options (all default OFF for release builds)
# ---------------------------------------------------------------------------
option(WOS_KCFI     "Enable Clang Kernel Control-Flow Integrity"            ON)
option(WOS_KUBSAN   "Enable Kernel UndefinedBehaviorSanitizer"              ON)
option(WOS_KASAN    "Enable Kernel AddressSanitizer (requires shadow map)"  ON)
option(WOS_KCOV     "Enable kernel coverage instrumentation (for Syzkaller)" ON)
option(WOS_SELFTEST "Enable kernel self-test mode (--selftest cmdline flag)" ON)

# Internal flags
set(CMAKE_LINK_FLAGS "LINKER:-m,elf_x86_64" "SHELL:-nostdlib" "SHELL:-z max-page-size=0x1000" "SHELL:-Wl,-T,${CMAKE_SOURCE_DIR}/modules/kern/linker.ld")

# Include directories

# Find source files
file(GLOB_RECURSE CFILES src/*.c)
file(GLOB_RECURSE ASFILES src/*.S)
file(GLOB_RECURSE NASMFILES src/*.asm)
file(GLOB_RECURSE CXXFILES src/*.cpp)

# Exclude test sources unless self-test mode is explicitly enabled.
# GLOB_RECURSE picks up src/test/*.cpp; strip them in production builds.
if(NOT WOS_SELFTEST)
    file(GLOB_RECURSE KTEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/src/test/*.cpp)
    list(REMOVE_ITEM CXXFILES ${KTEST_SRCS})
endif()

# Add executable
add_executable(${KERNEL} ${CFILES} ${ASFILES} ${NASMFILES} ${CXXFILES})

# Kernel-specific compile options: freestanding, no exceptions, no RTTI.
target_compile_options(${KERNEL} PRIVATE
    $<$<COMPILE_LANGUAGE:C>:-fno-exceptions;-mcmodel=kernel;-mno-sse;-mno-red-zone;-fno-stack-protector;-fno-stack-check;-fno-lto;-fno-sanitize=safe-stack>
    $<$<COMPILE_LANGUAGE:CXX>:-fno-exceptions;-fno-rtti;-mcmodel=kernel;-mno-sse;-mno-red-zone;-fno-stack-protector;-fno-stack-check;-fno-lto;-fno-sanitize=safe-stack>
)
target_link_options(${KERNEL} PRIVATE -fno-lto)

# ---------------------------------------------------------------------------
# Sanitizer flags (applied on top of the base kernel options)
# ---------------------------------------------------------------------------
if(WOS_KCFI)
    target_compile_options(${KERNEL} PRIVATE
        $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize=kcfi>
    )
    target_link_options(${KERNEL} PRIVATE -fsanitize=kcfi)
    target_compile_definitions(${KERNEL} PRIVATE WOS_KCFI=1)
    message(STATUS "[WOS] KCFI enabled")
endif()

if(WOS_KUBSAN)
    # Use trap mode: UBSan calls __ubsan_handle_* which we implement in sanitizer/ubsan.cpp
    target_compile_options(${KERNEL} PRIVATE
        $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize=undefined;-fno-sanitize-recover=all;-fno-sanitize=function,vptr>
    )
    target_compile_definitions(${KERNEL} PRIVATE WOS_KUBSAN=1)
    message(STATUS "[WOS] KUBSan enabled")
endif()

if(WOS_KASAN)
    # -fsanitize=kernel-address applies to C/CXX only (not NASM/ASM).
    target_compile_options(${KERNEL} PRIVATE
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=kernel-address>
    )
    # -mllvm flags must be passed as SHELL: pairs so CMake doesn't strip the
    # repeated -mllvm tokens (deduplication). They also must be language-gated
    # so NASM doesn't see them.
    target_compile_options(${KERNEL} PRIVATE
        "$<$<COMPILE_LANGUAGE:C,CXX>:SHELL:-mllvm -asan-mapping-offset=0xdffffc0000000000>"
        "$<$<COMPILE_LANGUAGE:C,CXX>:SHELL:-mllvm -asan-globals=0>"
        "$<$<COMPILE_LANGUAGE:C,CXX>:SHELL:-mllvm -asan-instrumentation-with-call-threshold=0>"
    )
    target_compile_definitions(${KERNEL} PRIVATE WOS_KASAN=1)

    # ---------------------------------------------------------------------------
    # Files that must NOT be KASan-instrumented because they execute before the
    # shadow memory is mapped (PHASE_0 early boot, MM init, interrupt/fault
    # infrastructure, and the KASan runtime itself).
    #
    # Instrumenting these causes the compiler-inserted shadow checks to fault on
    # unmapped shadow pages before the page-fault handler is ready — instant
    # triple fault or infinite recursion.
    # ---------------------------------------------------------------------------
    set(KASAN_EXCLUDED_SRCS
        # Early boot / logging (runs before any shadow pages exist)
        src/platform/dbg/dbg.cpp
        src/platform/dbg/journal.cpp
        src/platform/dbg/coredump.cpp
        src/mod/io/serial/serial.cpp
        src/mod/gfx/fb.cpp
        src/mod/gfx/fb_font.cpp
        # Memory management init (sets up the very heap KASan annotates)
        src/platform/mm/phys.opt.cpp
        src/platform/mm/virt.opt.cpp
        src/platform/mm/mm.opt.cpp
        src/platform/mm/addr.opt.cpp
        src/platform/mm/paging.opt.cpp
        src/platform/mm/page_alloc.cpp
        src/platform/mm/dyn/kmalloc.opt.cpp
        src/platform/mm/dyn/slab.opt.cpp
        src/minimalist_malloc/mini_malloc.cpp
        # Interrupt/exception infrastructure (fault handler runs before kasan::init)
        src/platform/interrupt/gates.cpp
        src/platform/interrupt/gdt.cpp
        src/platform/interrupt/idt.cpp
        src/platform/interrupt/interrupt.cpp
        src/platform/interrupt/kmem.cpp
        # KASan/UBSan/KCOV runtimes (must not instrument themselves)
        src/sanitizer/kasan.cpp
        src/sanitizer/ubsan.cpp
        src/sanitizer/kcov.cpp
        # Init infrastructure itself
        src/main.cpp
        src/platform/init/init_wrappers.cpp
        src/platform/init/init_validation.cpp
        src/platform/init/limine_requests.cpp
        # Low-level CPU/ASM helpers called from exception paths
        src/platform/asm/cpu.cpp
        src/platform/asm/tlb.cpp
        src/util/hcf.hpp
        # Synchronization primitives — called from excluded early-boot code
        # (dbg::log -> logLock.lock() runs before IDT; shadow fault = triple fault)
        src/platform/sys/spinlock.cpp
        src/platform/sys/mutex.cpp
        src/platform/sys/rwlock.cpp
        # String/memory primitives — called transitively from excluded files
        src/util/string.opt.cpp
        src/util/mem.opt.cpp
        src/util/abi_stubs.opt.cpp
    )
    foreach(src IN LISTS KASAN_EXCLUDED_SRCS)
        set_source_files_properties(
            ${CMAKE_CURRENT_SOURCE_DIR}/${src}
            PROPERTIES COMPILE_OPTIONS "-fno-sanitize=kernel-address"
        )
    endforeach()

    message(STATUS "[WOS] KASan enabled (requires shadow memory setup)")
endif()

if(WOS_KCOV)
    target_compile_options(${KERNEL} PRIVATE
        $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize-coverage=trace-pc>
    )
    target_compile_definitions(${KERNEL} PRIVATE WOS_KCOV=1)
    message(STATUS "[WOS] KCOV enabled (for Syzkaller coverage)")
endif()

if(WOS_SELFTEST)
    target_compile_definitions(${KERNEL} PRIVATE WOS_SELFTEST=1)
    message(STATUS "[WOS] Self-test mode enabled (boot with --selftest to run)")
endif()

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

# Inject builder identity (user@hostname) into the kernel binary
execute_process(COMMAND whoami OUTPUT_VARIABLE _WOS_USER OUTPUT_STRIP_TRAILING_WHITESPACE)
cmake_host_system_information(RESULT _WOS_HOST QUERY HOSTNAME)
target_compile_definitions(${KERNEL} PRIVATE "WOS_BUILDER=\"${_WOS_USER}@${_WOS_HOST}\"")

# Network hot-path instrumentation (prints cycle-count stats every 1000 pkts)
# Remove this line to disable tracing.
# target_compile_definitions(${KERNEL} PRIVATE NET_TRACE)

target_link_options(${KERNEL} PRIVATE ${CMAKE_LINK_FLAGS})

# target_link_libraries(${KERNEL} stdkern)


