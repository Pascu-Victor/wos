// WOS Kernel Coverage (KCOV) - Syzkaller-compatible coverage tracing.
//
// The compiler (-fsanitize-coverage=trace-pc) inserts a call to
// __sanitizer_cov_trace_pc() at every basic-block entry.  We record the
// return address (the PC of the instrumented block) into a per-task ring
// buffer.
//
// Userspace (syz-executor) enables coverage via a procfs node, executes
// syscalls, then reads the buffer to extract coverage PCs.  Syzkaller uses
// these to guide mutation.
//
// Gated behind cmake -DWOS_KCOV=ON. When disabled, the compiler does not emit
// coverage callbacks and the panic/selftest paths compile without KCOV output.

#ifdef WOS_KCOV

#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <new>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <sanitizer/kcov.hpp>

#ifdef WOS_KCOV_PANIC_TRACE
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#endif

namespace {
#ifdef WOS_KCOV_PANIC_TRACE
using log = ker::mod::dbg::logger<"kcov">;

constexpr size_t KCOV_PANIC_TRACE_ENTRIES = 128;
constexpr size_t KCOV_PANIC_TRACE_CPU_COUNT = ker::mod::desc::gdt::MAX_CPUS;

struct PanicTraceRing {
    uint64_t pcs[KCOV_PANIC_TRACE_ENTRIES]{};
    uint32_t write_seq = 0;
    uint32_t padding = 0;
};

static_assert(KCOV_PANIC_TRACE_ENTRIES == 128, "update kcov_trace_pc.asm entry count");
static_assert(KCOV_PANIC_TRACE_CPU_COUNT == 256, "update kcov_trace_pc.asm cpu count");
static_assert(offsetof(PanicTraceRing, pcs) == 0, "panic ring pcs offset changed");
static_assert(offsetof(PanicTraceRing, write_seq) == 1024, "panic ring write_seq offset changed");
static_assert(sizeof(PanicTraceRing) == 1032, "panic ring size changed");
#endif

// Per-CPU KCOV buffer pointer.  In a full implementation this would be
// stored in the Task struct (per-task).  For the initial bring-up we use
// a single global buffer — sufficient for single-threaded syz-executor.
extern "C" ker::sanitizer::kcov::KcovBuffer* wos_kcov_current_buffer = nullptr;
static_assert(offsetof(ker::sanitizer::kcov::KcovBuffer, pcs) == 0, "kcov buffer pcs offset changed");
static_assert(offsetof(ker::sanitizer::kcov::KcovBuffer, count) == 8, "kcov buffer count offset changed");
static_assert(offsetof(ker::sanitizer::kcov::KcovBuffer, capacity) == 16, "kcov buffer capacity offset changed");
static_assert(offsetof(ker::sanitizer::kcov::KcovBuffer, enabled) == 24, "kcov buffer enabled offset changed");
static_assert(offsetof(ker::sanitizer::kcov::KcovBuffer, truncated) == 25, "kcov buffer truncated offset changed");
#ifdef WOS_KCOV_PANIC_TRACE
extern "C" PanicTraceRing wos_kcov_panic_rings[KCOV_PANIC_TRACE_CPU_COUNT]{};
#endif

constexpr uint64_t KCOV_AUTO_RESERVE_BYTES = 128ULL * 1024 * 1024;

auto round_up_to_page(uint64_t bytes) -> uint64_t {
    constexpr uint64_t PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
    if (bytes == 0 || bytes > UINT64_MAX - (PAGE_SIZE - 1)) {
        return 0;
    }
    return (bytes + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
}

auto entries_to_allocation_bytes(size_t num_entries) -> uint64_t {
    if (num_entries == 0 || num_entries > UINT64_MAX / sizeof(uint64_t)) {
        return 0;
    }
    return round_up_to_page(static_cast<uint64_t>(num_entries) * sizeof(uint64_t));
}

auto highest_power_of_two_at_most(uint64_t value) -> uint64_t {
    uint64_t power = 1;
    while (power <= value / 2) {
        power <<= 1;
    }
    return power;
}

auto auto_reserve_bytes(uint64_t free_bytes) -> uint64_t {
    if (free_bytes > KCOV_AUTO_RESERVE_BYTES) {
        return KCOV_AUTO_RESERVE_BYTES;
    }
    return free_bytes / 4;
}

auto select_auto_allocation_bytes() -> uint64_t {
    constexpr uint64_t PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t const DEFAULT_BYTES = entries_to_allocation_bytes(ker::sanitizer::kcov::KCOV_DEFAULT_ENTRIES);
    uint64_t const FREE_BYTES = ker::mod::mm::phys::get_free_mem_bytes();
    uint64_t const RESERVE_BYTES = auto_reserve_bytes(FREE_BYTES);
    uint64_t const TARGET_BYTES = FREE_BYTES > RESERVE_BYTES ? FREE_BYTES - RESERVE_BYTES : 0;
    uint64_t candidate = highest_power_of_two_at_most(TARGET_BYTES);
    if (candidate < DEFAULT_BYTES) {
        candidate = DEFAULT_BYTES;
    }

    while (candidate >= PAGE_SIZE) {
        if (ker::mod::mm::phys::page_alloc_can_satisfy(candidate, RESERVE_BYTES)) {
            return candidate;
        }
        candidate /= 2;
    }
    return 0;
}

}  // namespace

namespace ker::sanitizer::kcov {

__attribute__((no_sanitize("coverage"))) void init() {
    // Nothing to do at boot — buffers are allocated on demand.
}

__attribute__((no_sanitize("coverage"))) int alloc_buffer(size_t num_entries) {
    uint64_t const ALLOC_BYTES = num_entries == 0 ? select_auto_allocation_bytes() : entries_to_allocation_bytes(num_entries);
    if (ALLOC_BYTES == 0) {
        return -1;
    }

    auto* buf = new (std::nothrow) KcovBuffer{};
    if (buf == nullptr) {
        return -1;  // ENOMEM
    }

    buf->pcs = static_cast<uint64_t*>(ker::mod::mm::phys::page_alloc_may_fail(ALLOC_BYTES, "kcov_buffer"));
    if (buf->pcs == nullptr) {
        delete buf;
        return -1;
    }

    buf->count = 0;
    buf->capacity = ALLOC_BYTES / sizeof(uint64_t);
    buf->enabled = false;
    buf->truncated = false;
    wos_kcov_current_buffer = buf;
    return 0;
}

__attribute__((no_sanitize("coverage"))) void enable() {
    if (wos_kcov_current_buffer != nullptr) {
        wos_kcov_current_buffer->enabled = true;
    }
}

__attribute__((no_sanitize("coverage"))) void disable() {
    if (wos_kcov_current_buffer != nullptr) {
        wos_kcov_current_buffer->enabled = false;
    }
}

__attribute__((no_sanitize("coverage"))) void reset() {
    if (wos_kcov_current_buffer != nullptr) {
        wos_kcov_current_buffer->count = 0;
        wos_kcov_current_buffer->truncated = false;
    }
}

__attribute__((no_sanitize("coverage"))) KcovBuffer* current_buffer() { return wos_kcov_current_buffer; }

#ifdef WOS_KCOV_PANIC_TRACE
__attribute__((no_sanitize("coverage"))) void dump_panic_trace_for_cpu(uint64_t cpu_id) {
    if (cpu_id >= KCOV_PANIC_TRACE_CPU_COUNT) {
        log::panic("Recent PC trace: invalid cpu=%lu", cpu_id);
        return;
    }

    PanicTraceRing const& ring = wos_kcov_panic_rings[cpu_id];
    uint32_t const WRITE_SEQ = __atomic_load_n(&ring.write_seq, __ATOMIC_ACQUIRE);
    if (WRITE_SEQ == 0) {
        log::panic("Recent PC trace (cpu=%lu): <empty>", cpu_id);
        return;
    }

    uint32_t const COUNT = WRITE_SEQ < KCOV_PANIC_TRACE_ENTRIES ? WRITE_SEQ : static_cast<uint32_t>(KCOV_PANIC_TRACE_ENTRIES);
    log::panic("Recent PC trace (cpu=%lu, entries=%u):", cpu_id, COUNT);

    for (uint32_t i = 0; i < COUNT; ++i) {
        uint32_t const SEQ = WRITE_SEQ - COUNT + i;
        uint32_t const SLOT = SEQ % static_cast<uint32_t>(KCOV_PANIC_TRACE_ENTRIES);
        log::panic("  [%u] pc=0x%lx", i, ring.pcs[SLOT]);
    }
}
#endif

}  // namespace ker::sanitizer::kcov

// --------------------------------------------------------------------------
// Compiler callback lives in kcov_trace_pc.asm so its boot-time stack usage is
// independent of the C++ optimization level.

#endif  // WOS_KCOV
