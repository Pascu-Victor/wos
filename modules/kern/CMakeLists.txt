cmake_minimum_required(VERSION 3.20)
project(wos C CXX ASM_NASM)

# Set the output name for the final kernel executable
set(KERNEL wos)

# ---------------------------------------------------------------------------
# Kernel build-time diagnostics, sanitizers, and instrumentation.
# ---------------------------------------------------------------------------
option(WOS_KCFI                    "Enable Clang Kernel Control-Flow Integrity"                         OFF)
option(WOS_KUBSAN                  "Enable Kernel UndefinedBehaviorSanitizer"                           OFF)
option(WOS_KERNEL_UBSAN_TRAP       "Enable runtime-free trap-mode kernel UBSan/integer/bounds checks"   OFF)
option(WOS_KASAN                   "Enable Kernel AddressSanitizer (requires shadow map)"               OFF)
option(WOS_KCOV                    "Enable kernel coverage instrumentation (for Syzkaller)"             OFF)
option(WOS_KCOV_NO_PRUNE           "Disable KCOV coverage pruning to instrument all eligible trace-pc blocks" ON)
option(WOS_KCOV_SOURCE_FRIENDLY    "Tune KCOV-enabled kernel codegen for source-level coverage fidelity" OFF)
option(WOS_KCOV_PANIC_TRACE        "Record a recent-PC ring for panic dumps using KCOV callbacks"       OFF)
option(WOS_KERNEL_STRICT_WARNINGS  "Enable strict kernel warning audit flags with Werror"               OFF)
option(WOS_KERNEL_EVERYTHING_WARNINGS "Enable Clang -Weverything kernel warning audit flags"            OFF)
option(WOS_SELFTEST                "Enable kernel self-test mode (--selftest cmdline flag)"             OFF)
option(WOS_NET_TRACE               "Enable network hot-path cycle-count trace spans"                    OFF)
option(WOS_NET_PACKET_DEBUG        "Track packet buffer allocation provenance and TX-refusal snapshots" OFF)
option(WOS_PHYS_ALLOC_CALLER_STATS "Track physical page allocation call sites for OOM reports"          OFF)
option(WOS_PHYS_LOCK_DEBUG         "Track physical allocator memlock holder CR3/CPU/RIP"                OFF)
option(WOS_KMALLOC_DEBUG_INFO      "Track kmalloc allocation call sites/tags for OOM/corruption dumps"  OFF)
option(WOS_MEMACC_FULL_DEFAULT     "Compile and default-enable full memacc allocation provenance"       OFF)
option(WOS_MM_RECLAIM_MAGIC_PROBES "Probe UNKNOWN reclaim frames for allocator magic in diagnostic builds" OFF)
option(WOS_MM_PROVENANCE_PERF_CONTROL "Disable expensive MM provenance diagnostics for control builds"  ON)
set(WOS_KERNEL_LIBCXX_HARDENING_MODE fast CACHE STRING "Kernel libc++ hardening mode: none, fast, extensive, debug")
set_property(CACHE WOS_KERNEL_LIBCXX_HARDENING_MODE PROPERTY STRINGS none fast extensive debug)

set(WOS_KERNEL_LIBCXX_HARDENING_MODES none fast extensive debug)
if(NOT "${WOS_KERNEL_LIBCXX_HARDENING_MODE}" IN_LIST WOS_KERNEL_LIBCXX_HARDENING_MODES)
    message(FATAL_ERROR
        "Unsupported WOS_KERNEL_LIBCXX_HARDENING_MODE='${WOS_KERNEL_LIBCXX_HARDENING_MODE}'. "
        "Use one of: none, fast, extensive, debug.")
endif()
string(TOUPPER "${WOS_KERNEL_LIBCXX_HARDENING_MODE}" WOS_KERNEL_LIBCXX_HARDENING_MODE_UPPER)

if(WOS_KUBSAN AND WOS_KERNEL_UBSAN_TRAP)
    message(FATAL_ERROR "WOS_KUBSAN and WOS_KERNEL_UBSAN_TRAP are separate UBSan profiles; enable only one")
endif()

if(WOS_MM_PROVENANCE_PERF_CONTROL)
    set(WOS_MEMACC_FULL_DEFAULT OFF CACHE BOOL "Compile and default-enable full memacc allocation provenance" FORCE)
    set(WOS_PHYS_ALLOC_CALLER_STATS OFF CACHE BOOL "Track physical page allocation call sites for OOM reports" FORCE)
    set(WOS_PHYS_LOCK_DEBUG OFF CACHE BOOL "Track physical allocator memlock holder CR3/CPU/RIP" FORCE)
    set(WOS_KMALLOC_DEBUG_INFO OFF CACHE BOOL "Track kmalloc allocation call sites/tags for OOM/corruption dumps" FORCE)
    set(WOS_MM_RECLAIM_MAGIC_PROBES OFF CACHE BOOL "Probe UNKNOWN reclaim frames for allocator magic in diagnostic builds" FORCE)
endif()

if(WOS_MEMACC_FULL_DEFAULT)
    set(WOS_PHYS_ALLOC_CALLER_STATS ON CACHE BOOL "Track physical page allocation call sites for OOM reports" FORCE)
    set(WOS_KMALLOC_DEBUG_INFO ON CACHE BOOL "Track kmalloc allocation call sites/tags for OOM/corruption dumps" FORCE)
endif()

if(WOS_KCOV_PANIC_TRACE AND NOT WOS_KCOV)
    message(FATAL_ERROR "WOS_KCOV_PANIC_TRACE requires WOS_KCOV=ON")
endif()

if(WOS_KCOV_SOURCE_FRIENDLY AND NOT WOS_KCOV)
    message(FATAL_ERROR "WOS_KCOV_SOURCE_FRIENDLY requires WOS_KCOV=ON")
endif()

# Internal flags
set(CMAKE_LINK_FLAGS "LINKER:-m,elf_x86_64" "SHELL:-nostdlib" "SHELL:-z max-page-size=0x1000" "SHELL:-Wl,-T,${WOS_MODULES_DIR}/kern/linker.ld")

# Include directories

# Find source files
file(GLOB_RECURSE CFILES CONFIGURE_DEPENDS src/*.c)
file(GLOB_RECURSE ASFILES CONFIGURE_DEPENDS src/*.S)
file(GLOB_RECURSE NASMFILES CONFIGURE_DEPENDS src/*.asm)
file(GLOB_RECURSE CXXFILES CONFIGURE_DEPENDS src/*.cpp)

# Exclude test sources unless self-test mode is explicitly enabled.
# GLOB_RECURSE picks up src/test/*.cpp; strip them in production builds.
if(NOT WOS_SELFTEST)
    file(GLOB_RECURSE KTEST_SRCS CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/test/*.cpp)
    list(REMOVE_ITEM CXXFILES ${KTEST_SRCS})
endif()

# Add executable
add_executable(${KERNEL} ${CFILES} ${ASFILES} ${NASMFILES} ${CXXFILES})

set(WOS_KERNEL_C_CXX_OPTIONS
    -ffreestanding
    -fno-builtin
    -m64
    -mno-red-zone
    -mno-mmx
    -mno-sse
    -mno-sse2
    -mno-avx
    -msoft-float
    -fno-omit-frame-pointer
    -fno-optimize-sibling-calls
    -fno-delete-null-pointer-checks
    -fno-strict-aliasing
    -fstrict-flex-arrays=3
    -ftrivial-auto-var-init=pattern
    -fdiagnostics-color=always
    -fdiagnostics-show-option
    -pipe
    -mcmodel=kernel
    -fno-stack-protector
    -fno-stack-check
    -fno-lto
    -fno-sanitize=safe-stack
)
if(NOT WOS_KERNEL_UBSAN_TRAP)
    list(APPEND WOS_KERNEL_C_CXX_OPTIONS
        -fno-strict-overflow
        -fwrapv
        -fwrapv-pointer
    )
endif()

set(WOS_KERNEL_CXX_OPTIONS
    -fno-exceptions
    -fno-rtti
    -fno-threadsafe-statics
    -fno-use-cxa-atexit
    -fno-c++-static-destructors
    -Wglobal-constructors
    -Wexit-time-destructors
    -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_${WOS_KERNEL_LIBCXX_HARDENING_MODE_UPPER}
)

# Kernel-specific compile options: freestanding, no hosted C++ runtime, no red zone.
target_compile_options(${KERNEL} PRIVATE
    "$<$<COMPILE_LANGUAGE:C>:${WOS_KERNEL_C_CXX_OPTIONS};-fno-exceptions>"
    "$<$<COMPILE_LANGUAGE:CXX>:${WOS_KERNEL_C_CXX_OPTIONS};${WOS_KERNEL_CXX_OPTIONS}>"
)
target_link_options(${KERNEL} PRIVATE -static -Wl,--no-dynamic-linker -fno-lto)

set(WOS_KERNEL_STRICT_C_CXX_WARNING_OPTIONS
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    -Wconversion
    -Wsign-conversion
    -Wimplicit-int-conversion
    -Wimplicit-int-float-conversion
    -Wshorten-64-to-32
    -Wfloat-conversion
    -Wdouble-promotion
    -Wshadow-all
    -Wundef
    -Wunreachable-code-aggressive
    -Wuninitialized
    -Wconditional-uninitialized
    -Wreturn-type
    -Wmissing-noreturn
    -Wcast-align
    -Wcast-qual
    -Wnull-dereference
    -Wformat=2
    -Wformat-security
    -Werror=format-security
    -Wswitch
    -Wswitch-enum
    -Wimplicit-fallthrough
    -Wextra-semi
    -Wcomma
    -Wunsafe-buffer-usage
)
set(WOS_KERNEL_STRICT_CXX_WARNING_OPTIONS
    -Wold-style-cast
    -Wzero-as-null-pointer-constant
    -Wnon-virtual-dtor
    -Woverloaded-virtual
    -Winconsistent-missing-override
    -Wsuggest-override
    -Wreorder-ctor
    -Wextra-semi-stmt
    -Wrange-loop-analysis
    -Wmove
    -Wself-move
    -Wredundant-move
    -Wpessimizing-move
    -Wthread-safety
)
set(WOS_KERNEL_EVERYTHING_WARNING_OPTIONS
    -Weverything
    -Wno-c++98-compat
    -Wno-c++98-compat-pedantic
    -Wno-pre-c++20-compat
    -Wno-pre-c++20-compat-pedantic
    -Wno-padded
)

if(WOS_KERNEL_STRICT_WARNINGS)
    target_compile_options(${KERNEL} PRIVATE
        "$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:${WOS_KERNEL_STRICT_C_CXX_WARNING_OPTIONS}>"
        "$<$<COMPILE_LANGUAGE:CXX>:${WOS_KERNEL_STRICT_CXX_WARNING_OPTIONS}>"
    )
    message(STATUS "[WOS] Kernel strict warning profile enabled")
endif()

if(WOS_KERNEL_EVERYTHING_WARNINGS)
    target_compile_options(${KERNEL} PRIVATE
        "$<$<COMPILE_LANGUAGE:CXX>:${WOS_KERNEL_EVERYTHING_WARNING_OPTIONS}>"
    )
    message(STATUS "[WOS] Kernel -Weverything warning audit profile enabled")
endif()

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
    # Report/panic mode: UBSan calls __ubsan_handle_* implemented in sanitizer/ubsan.cpp.
    target_compile_options(${KERNEL} PRIVATE
        $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize=undefined;-fno-sanitize-recover=all;-fno-sanitize=function,vptr>
    )
    target_compile_definitions(${KERNEL} PRIVATE WOS_KUBSAN=1)
    message(STATUS "[WOS] KUBSan enabled")
endif()

if(WOS_KERNEL_UBSAN_TRAP)
    target_compile_options(${KERNEL} PRIVATE
        $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize=undefined;-fsanitize=integer;-fsanitize=implicit-conversion;-fsanitize=bounds;-fsanitize=local-bounds;-fsanitize=nullability;-fsanitize=float-divide-by-zero;-fsanitize=float-cast-overflow;-fno-sanitize=function,vptr;-fno-sanitize-recover=all;-fsanitize-trap=all;-fno-sanitize-merge>
    )
    target_compile_definitions(${KERNEL} PRIVATE WOS_KERNEL_UBSAN_TRAP=1)
    message(STATUS "[WOS] Kernel UBSan trap profile enabled")
endif()

if(WOS_KASAN)
    set(KERNEL_SANITIZER_IGNORELIST ${CMAKE_CURRENT_SOURCE_DIR}/sancov.ignore)

    # -fsanitize=kernel-address applies to C/CXX only (not NASM/ASM).
    target_compile_options(${KERNEL} PRIVATE
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=kernel-address>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize-ignorelist=${KERNEL_SANITIZER_IGNORELIST}>
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
        # Early ACPI/APIC discovery is used while firmware tables and CPU/APIC
        # state are still being brought up. Keep this path out of KASAN so
        # compiler-emitted stack/object shadow poisoning does not recurse into
        # unmapped-shadow faults before the boot fault path is fully safe.
        src/platform/acpi/acpi.cpp
        src/platform/acpi/apic/apic.cpp
        src/platform/acpi/hpet/hpet.cpp
        src/platform/acpi/ioapic/ioapic.cpp
        src/platform/acpi/madt/madt.cpp
        src/platform/acpi/tables/rsdp.cpp
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
    message(STATUS "[WOS] KASan ignorelist enabled: ${KERNEL_SANITIZER_IGNORELIST}")
endif()

if(WOS_KCOV)
    target_compile_options(${KERNEL} PRIVATE
        $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-fsanitize-coverage=trace-pc>
    )
    if(WOS_KCOV_NO_PRUNE)
        target_compile_options(${KERNEL} PRIVATE
            $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-Xclang -fsanitize-coverage-no-prune>
        )
    endif()
    target_compile_definitions(${KERNEL} PRIVATE WOS_KCOV=1)
    message(STATUS "[WOS] KCOV enabled (for Syzkaller coverage)")
    if(WOS_KCOV_NO_PRUNE)
        message(STATUS "[WOS] KCOV no-prune enabled (instrument all eligible trace-pc blocks)")
    endif()
    if(WOS_KCOV_SOURCE_FRIENDLY)
        # The repository-wide module flags add -O3. Override that for the
        # kernel in KCOV-focused builds so LLVM keeps more source statements,
        # call sites, and basic-block boundaries visible to addr2line/LCOV.
        target_compile_options(${KERNEL} PRIVATE
            $<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-O0;-fno-inline;-fno-inline-functions;-fno-optimize-sibling-calls>
        )
        message(STATUS "[WOS] KCOV source-friendly codegen enabled (O0, reduced inlining, no tail-call folding)")
    endif()

    if(WOS_KCOV_PANIC_TRACE)
        target_compile_definitions(${KERNEL} PRIVATE WOS_KCOV_PANIC_TRACE=1)
        message(STATUS "[WOS] KCOV panic recent-PC trace enabled")
    endif()
endif()

if(WOS_SELFTEST)
    target_compile_definitions(${KERNEL} PRIVATE WOS_SELFTEST=1)
    message(STATUS "[WOS] Self-test mode enabled (boot with --selftest to run)")
endif()

if(WOS_MM_PROVENANCE_PERF_CONTROL)
    message(STATUS "[WOS] MM provenance perf-control mode enabled")
endif()

if(WOS_MEMACC_FULL_DEFAULT)
    target_compile_definitions(${KERNEL} PRIVATE WOS_MEMACC_FULL_DEFAULT=1)
    message(STATUS "[WOS] memacc full provenance default enabled")
endif()

if(WOS_NET_TRACE)
    target_compile_definitions(${KERNEL} PRIVATE NET_TRACE=1)
    message(STATUS "[WOS] Network hot-path trace spans enabled")
endif()

if(WOS_NET_PACKET_DEBUG)
    target_compile_definitions(${KERNEL} PRIVATE WOS_NET_PACKET_DEBUG=1)
    message(STATUS "[WOS] Packet buffer provenance debug enabled")
endif()

if(WOS_PHYS_ALLOC_CALLER_STATS)
    target_compile_definitions(${KERNEL} PRIVATE WOS_PHYS_ALLOC_CALLER_STATS=1)
    message(STATUS "[WOS] Physical page allocation caller stats enabled")
endif()

if(WOS_PHYS_LOCK_DEBUG)
    target_compile_definitions(${KERNEL} PRIVATE WOS_PHYS_LOCK_DEBUG=1)
    message(STATUS "[WOS] Physical allocator memlock holder tracking enabled")
endif()

if(WOS_KMALLOC_DEBUG_INFO)
    target_compile_definitions(${KERNEL} PRIVATE WOS_KMALLOC_DEBUG_INFO=1)
    message(STATUS "[WOS] kmalloc allocation provenance debug enabled")
endif()

if(WOS_MM_RECLAIM_MAGIC_PROBES)
    target_compile_definitions(${KERNEL} PRIVATE WOS_MM_RECLAIM_MAGIC_PROBES=1)
    message(STATUS "[WOS] MM UNKNOWN-frame reclaim magic probes enabled")
endif()

# Target-scoped includes for kernel only (do NOT include userspace stdlib src here)
target_include_directories(${KERNEL} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/std
    # Avoid adding the project root here. Adding the root lets the
    # compiler find headers under modules/stdlib/src (for example
    # <std/...>) which collides with the toolchain libc++ headers.
    # Instead, add only the small public include sets the kernel needs.
    ${WOS_MODULES_DIR}
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

target_link_options(${KERNEL} PRIVATE ${CMAKE_LINK_FLAGS})

# target_link_libraries(${KERNEL} stdkern)
