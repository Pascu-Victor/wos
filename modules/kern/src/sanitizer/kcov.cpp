// WOS Kernel Coverage (KCOV) — Syzkaller-compatible coverage tracing.
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
// Gated behind cmake -DWOS_KCOV=ON.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#ifdef WOS_KCOV

#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <sanitizer/kcov.hpp>

namespace {
using log = ker::mod::dbg::logger<"kcov">;

constexpr size_t KCOV_PANIC_TRACE_ENTRIES = 128;

struct PanicTraceRing {
    std::array<uint64_t, KCOV_PANIC_TRACE_ENTRIES> pcs{};
    std::atomic<uint32_t> write_seq{0};
};

// Per-CPU KCOV buffer pointer.  In a full implementation this would be
// stored in the Task struct (per-task).  For the initial bring-up we use
// a single global buffer — sufficient for single-threaded syz-executor.
ker::sanitizer::kcov::KcovBuffer* s_current_buffer = nullptr;
std::array<PanicTraceRing, ker::mod::desc::gdt::MAX_CPUS> s_panic_rings{};

}  // namespace

namespace ker::sanitizer::kcov {

void init() {
    // Nothing to do at boot — buffers are allocated on demand.
}

int alloc_buffer(size_t num_entries) {
    if (num_entries == 0 || num_entries > KCOV_MAX_ENTRIES) {
        num_entries = KCOV_MAX_ENTRIES;
    }

    auto* buf = new (std::nothrow) KcovBuffer{};
    if (buf == nullptr) {
        return -1;  // ENOMEM
    }

    buf->pcs = new (std::nothrow) uint64_t[num_entries];
    if (buf->pcs == nullptr) {
        delete buf;
        return -1;
    }

    buf->count = 0;
    buf->capacity = num_entries;
    buf->enabled = false;
    s_current_buffer = buf;
    return 0;
}

void enable() {
    if (s_current_buffer != nullptr) {
        s_current_buffer->enabled = true;
    }
}

void disable() {
    if (s_current_buffer != nullptr) {
        s_current_buffer->enabled = false;
    }
}

void reset() {
    if (s_current_buffer != nullptr) {
        s_current_buffer->count = 0;
    }
}

KcovBuffer* current_buffer() { return s_current_buffer; }

void dump_panic_trace_for_cpu(uint64_t cpu_id) {
    if (cpu_id >= s_panic_rings.size()) {
        log::panic("Recent PC trace: invalid cpu=%lu", cpu_id);
        return;
    }

    PanicTraceRing const& ring = s_panic_rings.at(cpu_id);
    uint32_t const WRITE_SEQ = ring.write_seq.load(std::memory_order_acquire);
    if (WRITE_SEQ == 0) {
        log::panic("Recent PC trace (cpu=%lu): <empty>", cpu_id);
        return;
    }

    uint32_t const COUNT = WRITE_SEQ < KCOV_PANIC_TRACE_ENTRIES ? WRITE_SEQ : static_cast<uint32_t>(KCOV_PANIC_TRACE_ENTRIES);
    log::panic("Recent PC trace (cpu=%lu, entries=%u):", cpu_id, COUNT);

    for (uint32_t i = 0; i < COUNT; ++i) {
        uint32_t const SEQ = WRITE_SEQ - COUNT + i;
        uint32_t const SLOT = SEQ % static_cast<uint32_t>(KCOV_PANIC_TRACE_ENTRIES);
        log::panic("  [%u] pc=0x%lx", i, ring.pcs.at(SLOT));
    }
}

}  // namespace ker::sanitizer::kcov

// --------------------------------------------------------------------------
// Compiler callback — called at every basic-block entry
// --------------------------------------------------------------------------
// NOLINTNEXTLINE(readability-identifier-naming)
extern "C" void __sanitizer_cov_trace_pc() {
    auto const PC = reinterpret_cast<uint64_t>(__builtin_return_address(0));
    if (ker::mod::cpu::is_per_cpu_ready()) {
        uint64_t const CPU_ID = ker::mod::cpu::current_cpu();
        if (CPU_ID < s_panic_rings.size()) {
            auto& ring = s_panic_rings.at(CPU_ID);
            uint32_t const SEQ = ring.write_seq.fetch_add(1, std::memory_order_relaxed);
            ring.pcs.at(SEQ % static_cast<uint32_t>(KCOV_PANIC_TRACE_ENTRIES)) = PC;
        }
    }

    auto* buf = s_current_buffer;
    if ((buf == nullptr) || !buf->enabled) {
        return;
    }

    uint64_t const IDX = buf->count;
    if (IDX < buf->capacity) {
        buf->pcs[IDX] = PC;
        buf->count = IDX + 1;
    }
    // If buffer is full, silently drop (Syzkaller handles partial coverage)
}

#endif  // WOS_KCOV
