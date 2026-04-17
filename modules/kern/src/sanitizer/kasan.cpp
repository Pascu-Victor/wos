// WOS Kernel AddressSanitizer (KASan) runtime.
//
// This file provides:
//   1. Shadow memory demand-paging (handle_shadow_fault)
//   2. Shadow memory poison/unpoison helpers
//   3. Heap allocation hooks (on_kmalloc / on_kfree)
//   4. __asan_load/store callbacks emitted by -fsanitize=kernel-address
//   5. Miscellaneous ASan support callbacks (__asan_option_detect_stack_use_after_return, etc.)
//
// Gated behind WOS_KASAN; when the macro is not defined the file is still
// compiled but everything inside the #ifdef is stripped by the preprocessor.

#ifdef WOS_KASAN

#include "kasan.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::kasan {

// ---------------------------------------------------------------------------
// Shadow region bounds
// Shadow covers [SHADOW_BASE, SHADOW_END).
// SHADOW_OFFSET is defined in kasan.hpp and must match -asan-mapping-offset.
//
// The shadow region covers 1/8 of the 64-bit address space.  For a kernel
// that uses the top 128 TiB (0xffff800000000000 .. 0xffffffffffffffff), the
// corresponding shadow occupies a sub-range of the region starting at
// SHADOW_OFFSET.  We never need to map the full 16 EiB of shadow — only the
// pages that are actually touched are demand-faulted in.
// ---------------------------------------------------------------------------

// Lower bound of the shadow region we consider valid for demand-paging.
// Derived: shadow_of(0x0) = SHADOW_OFFSET >> 3 * 0 + SHADOW_OFFSET.
// For safety just use SHADOW_OFFSET as the bottom.
static constexpr uint64_t SHADOW_REGION_BASE = SHADOW_OFFSET;

// Upper bound: shadow for the top of canonical address space.
// shadow_of(0xffffffffffffffff) = (0xffffffffffffffff >> 3) + SHADOW_OFFSET
//                                = 0x1fffffffffffffff + 0xdffffc0000000000
//                                ≈ 0xfffffc0000000000 (wraps in 64 bits, fine)
// Use a generous end that covers all kernel VA.
static constexpr uint64_t SHADOW_REGION_END = 0xfffffc2000000000ULL;

// ---------------------------------------------------------------------------
// Shadow page fault handler
// ---------------------------------------------------------------------------

// Per-CPU re-entrancy guard for shadow fault handling.
// pageAlloc and mapToKernelPageTable themselves are KASan-instrumented and can
// trigger further shadow faults. We must prevent recursion on the *same* CPU
// but allow different CPUs to handle shadow faults concurrently.
// Uses an atomic bitmask indexed by LAPIC ID (read via GS-base per-CPU data
// or CPUID). Supports up to 64 CPUs.
static std::atomic<uint64_t> s_shadow_fault_cpus{0};  // NOLINT

// Serialise concurrent mapToKernelPageTable calls so two CPUs don't race
// to create the same PML entry and leak a page.
static std::atomic_flag s_shadow_map_lock = ATOMIC_FLAG_INIT;  // NOLINT

// Get current CPU index — reads LAPIC ID via x2APIC MSR 0x802.
// Before x2APIC is enabled (early boot), we're single-threaded on the BSP
// so returning 0 is safe. We detect this by checking IA32_APIC_BASE (MSR 0x1B)
// bit 10 (x2APIC enable).
static inline uint64_t shadow_cpu_index() {
    uint32_t apic_base_lo, apic_base_hi;
    asm volatile("rdmsr" : "=a"(apic_base_lo), "=d"(apic_base_hi) : "c"(0x1Bu));
    if ((apic_base_lo & (1 << 10)) == 0) {
        return 0;  // x2APIC not yet enabled — single-threaded BSP
    }
    uint32_t lo, hi;
    asm volatile("rdmsr" : "=a"(lo), "=d"(hi) : "c"(0x802u));
    return lo & 63;
}

bool handle_shadow_fault(uint64_t cr2) {
    if (cr2 < SHADOW_REGION_BASE || cr2 >= SHADOW_REGION_END) {
        return false;  // Not our fault
    }

    uint64_t cpu = shadow_cpu_index();
    uint64_t cpu_bit = 1ULL << (cpu & 63);

    // Check for re-entrancy on THIS cpu
    if ((s_shadow_fault_cpus.load(std::memory_order_relaxed) & cpu_bit) != 0) {
        return false;  // Recursive shadow fault on same CPU — let it panic
    }
    s_shadow_fault_cpus.fetch_or(cpu_bit, std::memory_order_acquire);

    // Allocate one zeroed physical page (shadow is 0x00 = accessible by default).
    void* phys_page = mm::phys::pageAlloc(mm::paging::PAGE_SIZE);
    if (phys_page == nullptr) {
        dbg::log("[KASan] OOM allocating shadow page for cr2=0x%lx", cr2);
        s_shadow_fault_cpus.fetch_and(~cpu_bit, std::memory_order_release);
        return false;
    }
    memset(phys_page, 0x00, mm::paging::PAGE_SIZE);

    auto shadow_vaddr = static_cast<mm::addr::vaddr_t>(cr2 & ~(mm::paging::PAGE_SIZE - 1));
    auto shadow_paddr = (mm::addr::paddr_t)mm::addr::get_phys_pointer((mm::addr::vaddr_t)phys_page);

    // Serialise page-table manipulation so two CPUs faulting on the same
    // shadow page don't both create a mapping and leak the second page.
    while (s_shadow_map_lock.test_and_set(std::memory_order_acquire)) {
        asm volatile("pause");
    }

    // Another CPU may have already mapped this shadow page while we waited.
    auto* kernel_pt = mm::virt::getKernelPagemap();
    if (mm::virt::translate(kernel_pt, shadow_vaddr) != mm::virt::PADDR_INVALID) {
        // Already mapped — free our page and return success.
        s_shadow_map_lock.clear(std::memory_order_release);
        // Note: we leak the phys_page here since there's no pageFree().
        // This is harmless — it only happens on rare concurrent shadow faults
        // to the exact same page.
        s_shadow_fault_cpus.fetch_and(~cpu_bit, std::memory_order_release);
        return true;
    }

    mm::virt::mapToKernelPageTable(shadow_vaddr, shadow_paddr, mm::paging::pageTypes::KERNEL);
    s_shadow_map_lock.clear(std::memory_order_release);

    s_shadow_fault_cpus.fetch_and(~cpu_bit, std::memory_order_release);
    return true;
}

// ---------------------------------------------------------------------------
// Shadow memory manipulation
// ---------------------------------------------------------------------------

void poison_range(const void* ptr, size_t size, int8_t value) {
    auto addr = reinterpret_cast<uintptr_t>(ptr);

    // Poison whole-granule (8-byte) units first
    size_t full_units = size / 8;
    int8_t* shadow = addr_to_shadow(addr);
    if (full_units > 0) {
        memset(shadow, value, full_units);
    }

    // Partial last granule: if poisoning, mark as partially accessible
    // (only the leading bytes are valid — same as ASan partial poisoning).
    size_t remainder = size % 8;
    if (remainder != 0) {
        // For partial poisoning we store the count of accessible bytes
        // (ASan convention: shadow byte = N means first N bytes accessible).
        // When unpoisoning (value == 0) we clear it fully.
        shadow[full_units] = (value == SHADOW_ACCESSIBLE) ? 0 : static_cast<int8_t>(remainder);
    }
}

void unpoison_range(const void* ptr, size_t size) { poison_range(ptr, size, SHADOW_ACCESSIBLE); }

// ---------------------------------------------------------------------------
// KASan early init — unpoison the kernel static image
// ---------------------------------------------------------------------------

// Runtime enable flag — false until enable() is called after the IDT
// is live (so shadow page demand-faults work).
static volatile bool s_active = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void init() {
    // The compiler's shadow accesses for global variables and kernel static
    // code will fault into handle_shadow_fault() and get demand-zeroed pages.
    // For global redzones (emitted by -asan-globals=1), Clang calls
    // __asan_register_globals() which we stub out below — we rely on the
    // demand-fault mechanism instead of pre-populating global shadows.
    dbg::log("[KASan] initialized (shadow base=0x%lx)", SHADOW_OFFSET);
}

void enable() { s_active = true; }

bool is_enabled() {
    return s_active;
    dbg::log("[KASan] runtime shadow checks enabled");
}

bool in_shadow_fault() {
    uint64_t cpu = shadow_cpu_index();
    uint64_t cpu_bit = 1ULL << (cpu & 63);
    return (s_shadow_fault_cpus.load(std::memory_order_relaxed) & cpu_bit) != 0;
}

}  // namespace ker::mod::kasan

// ---------------------------------------------------------------------------
// ASan ABI — callbacks emitted by the compiler.
// Names must be exactly as the compiler generates them (no namespace).
// ---------------------------------------------------------------------------

extern "C" {

// ---- Report functions (called when a violation is detected) ---------------
// These are the functions Clang emits calls to on detected violations.
// We log and panic rather than trying to recover.

namespace {

namespace ser = ker::mod::io::serial;

void kasan_write_hex(uint64_t val) {
    char buf[19];
    buf[0] = '0';
    buf[1] = 'x';
    constexpr const char* hex = "0123456789abcdef";
    for (int i = 15; i >= 0; i--) {
        buf[2 + (15 - i)] = hex[(val >> (i * 4)) & 0xf];
    }
    buf[18] = '\0';
    ser::writeUnlocked(buf);
}

void kasan_write_dec(uint64_t val) {
    char buf[21];
    int pos = 20;
    buf[pos] = '\0';
    if (val == 0) {
        buf[--pos] = '0';
    } else {
        while (val > 0) {
            buf[--pos] = static_cast<char>('0' + (val % 10));
            val /= 10;
        }
    }
    ser::writeUnlocked(buf + pos);
}

// Describe a shadow byte value (ASan convention)
const char* kasan_shadow_desc(int8_t val) {
    if (val == 0x00) return "accessible";
    if (val >= 0x01 && val <= 0x07) return "partial";
    switch (static_cast<uint8_t>(val)) {
        case 0xf1:
            return "heap-left-redzone";
        case 0xf2:
            return "heap-mid-redzone";
        case 0xf3:
            return "heap-right-redzone";
        case 0xf5:
            return "stack-left-redzone";
        case 0xf8:
            return "freed";
        case 0xf9:
            return "global-redzone";
        case 0xfa:
            return "stack-mid-redzone";
        case 0xfb:
            return "stack-partial";
        case 0xfc:
            return "stack-right-redzone";
        case 0xfd:
            return "stack-use-after-scope";
        case 0xfe:
            return "stack-use-after-return";
        case 0xff:
            return "poisoned";
        default:
            return "unknown";
    }
}

// Print hex dump of memory around the faulting address (8 rows of 16 bytes)
void kasan_hexdump(uintptr_t addr, size_t size) {
    uintptr_t start = (addr - 64) & ~0xFULL;
    uintptr_t end = start + 128;
    if (start < 0xffff000000000000ULL) return;

    ser::writeUnlocked("\n--- Memory around ");
    kasan_write_hex(addr);
    ser::writeUnlocked(" ---\n");

    constexpr const char* hex = "0123456789abcdef";
    for (uintptr_t row = start; row < end; row += 16) {
        if (addr >= row && addr < row + 16) {
            ser::writeUnlocked("=>");
        } else {
            ser::writeUnlocked("  ");
        }
        kasan_write_hex(row);
        ser::writeUnlocked(": ");

        auto* p = reinterpret_cast<const uint8_t*>(row);
        for (int i = 0; i < 16; i++) {
            bool is_target = ((row + static_cast<uintptr_t>(i)) >= addr && (row + static_cast<uintptr_t>(i)) < addr + size);
            if (is_target) ser::writeUnlocked("[");
            char h[3];
            h[0] = hex[p[i] >> 4];
            h[1] = hex[p[i] & 0xf];
            h[2] = '\0';
            ser::writeUnlocked(h);
            if (is_target) {
                ser::writeUnlocked("]");
            } else {
                ser::writeUnlocked(" ");
            }
            if (i == 7) ser::writeUnlocked(" ");
        }

        ser::writeUnlocked(" |");
        for (int i = 0; i < 16; i++) {
            char c = (p[i] >= 0x20 && p[i] < 0x7f) ? static_cast<char>(p[i]) : '.';
            ser::writeUnlocked(c);
        }
        ser::writeUnlocked("|\n");
    }
}

// Print ASan-style shadow memory map around the faulting shadow byte
void kasan_shadow_map(uintptr_t addr) {
    int8_t* shadow = ker::mod::kasan::addr_to_shadow(addr);
    auto shadow_addr = reinterpret_cast<uintptr_t>(shadow);

    // Print 9 rows of 16 shadow bytes = 144 bytes of shadow
    // Each shadow byte covers 8 application bytes, so 144*8 = 1152 bytes of app memory
    uintptr_t start = (shadow_addr - 64) & ~0xFULL;  // align to 16 bytes
    uintptr_t end = start + 144;

    ser::writeUnlocked("\n--- Shadow memory around ");
    kasan_write_hex(shadow_addr);
    ser::writeUnlocked(" (each byte covers 8 app bytes) ---\n");

    constexpr const char* hex = "0123456789abcdef";
    for (uintptr_t row = start; row < end; row += 16) {
        // Arrow on the row containing the faulting shadow byte
        if (shadow_addr >= row && shadow_addr < row + 16) {
            ser::writeUnlocked("=>");
        } else {
            ser::writeUnlocked("  ");
        }
        kasan_write_hex(row);
        ser::writeUnlocked(": ");

        auto* p = reinterpret_cast<const int8_t*>(row);
        for (int i = 0; i < 16; i++) {
            bool is_target = (row + static_cast<uintptr_t>(i) == shadow_addr);
            if (is_target) ser::writeUnlocked("[");
            char h[3];
            auto byte = static_cast<uint8_t>(p[i]);
            h[0] = hex[byte >> 4];
            h[1] = hex[byte & 0xf];
            h[2] = '\0';
            ser::writeUnlocked(h);
            if (is_target) {
                ser::writeUnlocked("]");
            } else {
                ser::writeUnlocked(" ");
            }
            if (i == 7) ser::writeUnlocked(" ");
        }
        ser::writeUnlocked("\n");
    }

    // Legend
    ser::writeUnlocked("  Shadow byte legend:\n");
    ser::writeUnlocked("    00=accessible  01-07=partial  f1=heap-left-rz  f2=heap-mid-rz\n");
    ser::writeUnlocked("    f3=heap-right-rz  f5=stack-left-rz  f8=freed  f9=global-rz\n");
    ser::writeUnlocked("    ff=poisoned\n");
}

}  // namespace

[[noreturn]] static void kasan_report(uintptr_t addr, size_t size, bool is_write, uintptr_t ret_addr) {
    ser::enterPanicMode();
    ker::mod::smt::halt_other_cores();

    int8_t* shadow = ker::mod::kasan::addr_to_shadow(addr);
    int8_t s = *shadow;
    auto shadow_addr = reinterpret_cast<uintptr_t>(shadow);

    // Header — mimic LLVM ASan format
    ser::writeUnlocked("\n==================================================================\n");
    ser::writeUnlocked("ERROR: KernelAddressSanitizer: ");
    ser::writeUnlocked(kasan_shadow_desc(s));
    ser::writeUnlocked(" on address ");
    kasan_write_hex(addr);
    ser::writeUnlocked("\n");

    // Access details
    ser::writeUnlocked(is_write ? "WRITE" : "READ");
    ser::writeUnlocked(" of size ");
    kasan_write_dec(size);
    ser::writeUnlocked(" at ");
    kasan_write_hex(addr);
    ser::writeUnlocked("\n");

    // Shadow details
    ser::writeUnlocked("  Shadow addr: ");
    kasan_write_hex(shadow_addr);
    ser::writeUnlocked("  Shadow byte: ");
    {
        constexpr const char* hex = "0123456789abcdef";
        char h[5];
        h[0] = '0';
        h[1] = 'x';
        h[2] = hex[(static_cast<uint8_t>(s)) >> 4];
        h[3] = hex[(static_cast<uint8_t>(s)) & 0xf];
        h[4] = '\0';
        ser::writeUnlocked(h);
    }
    ser::writeUnlocked(" (");
    ser::writeUnlocked(kasan_shadow_desc(s));
    ser::writeUnlocked(")\n");

    // Adjacent shadow bytes
    ser::writeUnlocked("  Adjacent: prev=");
    {
        constexpr const char* hex = "0123456789abcdef";
        char h[5];
        h[0] = '0';
        h[1] = 'x';
        h[2] = hex[(static_cast<uint8_t>(shadow[-1])) >> 4];
        h[3] = hex[(static_cast<uint8_t>(shadow[-1])) & 0xf];
        h[4] = '\0';
        ser::writeUnlocked(h);
    }
    ser::writeUnlocked("(");
    ser::writeUnlocked(kasan_shadow_desc(shadow[-1]));
    ser::writeUnlocked(")  next=");
    {
        constexpr const char* hex = "0123456789abcdef";
        char h[5];
        h[0] = '0';
        h[1] = 'x';
        h[2] = hex[(static_cast<uint8_t>(shadow[1])) >> 4];
        h[3] = hex[(static_cast<uint8_t>(shadow[1])) & 0xf];
        h[4] = '\0';
        ser::writeUnlocked(h);
    }
    ser::writeUnlocked("(");
    ser::writeUnlocked(kasan_shadow_desc(shadow[1]));
    ser::writeUnlocked(")\n");

    // Caller
    ser::writeUnlocked("  Caller: ");
    kasan_write_hex(ret_addr);
    ser::writeUnlocked("\n");

    // Hex dump of application memory
    kasan_hexdump(addr, size);

    // Shadow memory map
    kasan_shadow_map(addr);

    ser::writeUnlocked("==================================================================\n");

    ker::mod::dbg::panic_handler("KASan: memory access violation (see above)");
    __builtin_unreachable();
}

// NOLINTBEGIN(readability-identifier-naming)

void __asan_report_load1(uintptr_t addr) { kasan_report(addr, 1, false, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_load2(uintptr_t addr) { kasan_report(addr, 2, false, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_load4(uintptr_t addr) { kasan_report(addr, 4, false, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_load8(uintptr_t addr) { kasan_report(addr, 8, false, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_load16(uintptr_t addr) { kasan_report(addr, 16, false, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_load_n(uintptr_t addr, size_t size) { kasan_report(addr, size, false, (uintptr_t)__builtin_return_address(0)); }

void __asan_report_store1(uintptr_t addr) { kasan_report(addr, 1, true, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_store2(uintptr_t addr) { kasan_report(addr, 2, true, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_store4(uintptr_t addr) { kasan_report(addr, 4, true, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_store8(uintptr_t addr) { kasan_report(addr, 8, true, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_store16(uintptr_t addr) { kasan_report(addr, 16, true, (uintptr_t)__builtin_return_address(0)); }
void __asan_report_store_n(uintptr_t addr, size_t size) { kasan_report(addr, size, true, (uintptr_t)__builtin_return_address(0)); }

// ---- Outline shadow check -------------------------------------------------
// With -asan-instrumentation-with-call-threshold=0, the compiler calls
// __asan_loadN / __asan_storeN for EVERY memory access (outline mode).
// These must check the shadow byte and only report on actual violations.
//
// Shadow byte encoding (8-byte granule):
//   0x00       = all 8 bytes accessible
//   0x01..0x07 = first N bytes accessible
//   negative   = sentinel (redzone / freed / poisoned)

extern "C" char __kernel_end[];

static inline auto check_shadow(uintptr_t addr, size_t size) -> bool {
    if (__builtin_expect(static_cast<long>(!ker::mod::kasan::s_active), 0) != 0) {
        return true;  // KASan not yet live — skip
    }

    // Skip userspace addresses — their shadow falls in the non-canonical hole
    if (__builtin_expect(addr < 0xffff800000000000ULL, 0)) return true;

    if (__builtin_expect(addr >= reinterpret_cast<uint64_t>(__kernel_end), 0)) return true;

    int8_t* shadow = ker::mod::kasan::addr_to_shadow(addr);
    int8_t s = *shadow;

    if (__builtin_expect(s == 0, 1)) {
        // First granule fully accessible.
        // If the access fits within one granule we're done.
        if (size <= 8 - (addr & 7)) return true;
        // Crosses into next granule — check it.
        int8_t s2 = shadow[1];
        if (__builtin_expect(s2 == 0, 1)) return true;
        if (s2 < 0) return false;
        // Partial second granule: remaining bytes must fit.
        size_t remaining = (addr & 7) + size - 8;
        return remaining <= static_cast<size_t>(s2);
    }

    if (s < 0) return false;  // Sentinel (redzone / freed / poisoned)

    // Partial granule: first s bytes accessible from granule start.
    return (addr & 7) + size <= static_cast<size_t>(s);
}

// ---- Load/store instrumentation callbacks ---------------------------------
// With -asan-instrumentation-with-call-threshold=0, Clang emits calls to
// __asan_loadN / __asan_storeN for every memory access (outline mode).
// These perform the shadow check and only report on violations.

void __asan_load1(uintptr_t addr) {
    if (!check_shadow(addr, 1)) kasan_report(addr, 1, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load2(uintptr_t addr) {
    if (!check_shadow(addr, 2)) kasan_report(addr, 2, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load4(uintptr_t addr) {
    if (!check_shadow(addr, 4)) kasan_report(addr, 4, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load8(uintptr_t addr) {
    if (!check_shadow(addr, 8)) kasan_report(addr, 8, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load16(uintptr_t addr) {
    if (!check_shadow(addr, 16)) kasan_report(addr, 16, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load_n(uintptr_t addr, size_t n) {
    if (!check_shadow(addr, n)) kasan_report(addr, n, false, (uintptr_t)__builtin_return_address(0));
}

void __asan_store1(uintptr_t addr) {
    if (!check_shadow(addr, 1)) kasan_report(addr, 1, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store2(uintptr_t addr) {
    if (!check_shadow(addr, 2)) kasan_report(addr, 2, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store4(uintptr_t addr) {
    if (!check_shadow(addr, 4)) kasan_report(addr, 4, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store8(uintptr_t addr) {
    if (!check_shadow(addr, 8)) kasan_report(addr, 8, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store16(uintptr_t addr) {
    if (!check_shadow(addr, 16)) kasan_report(addr, 16, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store_n(uintptr_t addr, size_t n) {
    if (!check_shadow(addr, n)) kasan_report(addr, n, true, (uintptr_t)__builtin_return_address(0));
}

// Noabort variants — emitted when -fsanitize-recover=address is in effect.
// We still treat violations as fatal in the kernel.
void __asan_load1_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 1)) kasan_report(addr, 1, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load2_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 2)) kasan_report(addr, 2, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load4_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 4)) kasan_report(addr, 4, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load8_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 8)) kasan_report(addr, 8, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_load16_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 16)) kasan_report(addr, 16, false, (uintptr_t)__builtin_return_address(0));
}
void __asan_loadN_noabort(uintptr_t addr, size_t n) {
    if (!check_shadow(addr, n)) kasan_report(addr, n, false, (uintptr_t)__builtin_return_address(0));
}

void __asan_store1_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 1)) kasan_report(addr, 1, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store2_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 2)) kasan_report(addr, 2, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store4_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 4)) kasan_report(addr, 4, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store8_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 8)) kasan_report(addr, 8, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_store16_noabort(uintptr_t addr) {
    if (!check_shadow(addr, 16)) kasan_report(addr, 16, true, (uintptr_t)__builtin_return_address(0));
}
void __asan_storeN_noabort(uintptr_t addr, size_t n) {
    if (!check_shadow(addr, n)) kasan_report(addr, n, true, (uintptr_t)__builtin_return_address(0));
}

// ---- Stack / scope helpers ------------------------------------------------
void __asan_stack_malloc_0(size_t) {}
void __asan_stack_malloc_1(size_t) {}
void __asan_stack_malloc_2(size_t) {}
void __asan_stack_malloc_3(size_t) {}
void __asan_stack_malloc_4(size_t) {}
void __asan_stack_malloc_5(size_t) {}
void __asan_stack_malloc_6(size_t) {}
void __asan_stack_malloc_7(size_t) {}
void __asan_stack_malloc_8(size_t) {}
void __asan_stack_malloc_9(size_t) {}
void __asan_stack_malloc_10(size_t) {}

void __asan_stack_free_0(uintptr_t, size_t) {}
void __asan_stack_free_1(uintptr_t, size_t) {}
void __asan_stack_free_2(uintptr_t, size_t) {}
void __asan_stack_free_3(uintptr_t, size_t) {}
void __asan_stack_free_4(uintptr_t, size_t) {}
void __asan_stack_free_5(uintptr_t, size_t) {}
void __asan_stack_free_6(uintptr_t, size_t) {}
void __asan_stack_free_7(uintptr_t, size_t) {}
void __asan_stack_free_8(uintptr_t, size_t) {}
void __asan_stack_free_9(uintptr_t, size_t) {}
void __asan_stack_free_10(uintptr_t, size_t) {}

void __asan_poison_stack_memory(uintptr_t addr, size_t size) {
    ker::mod::kasan::poison_range(reinterpret_cast<void*>(addr), size, ker::mod::kasan::SHADOW_POISONED);
}
void __asan_unpoison_stack_memory(uintptr_t addr, size_t size) { ker::mod::kasan::unpoison_range(reinterpret_cast<void*>(addr), size); }

// NOLINTEND(readability-identifier-naming)

// ---- Global variable registration -----------------------------------------
// -asan-globals=1 causes the compiler to emit descriptors for each global and
// call __asan_register_globals at module init time.  We stub this out —
// shadow pages for globals are demand-faulted in as zeroed (accessible).

struct AsanGlobalDescriptor {
    uintptr_t beg;
    size_t size;
    size_t size_with_redzone;
    const char* name;
    const char* module_name;
    uintptr_t has_dynamic_init;
    void* source_location;
    uintptr_t odr_indicator;
};

void __asan_register_globals(AsanGlobalDescriptor* globals, size_t n) {
    for (size_t i = 0; i < n; i++) {
        // Unpoison the object body first — demand-paged shadow is zeroed (accessible)
        // but an earlier registration may have poisoned a shared shadow granule.
        ker::mod::kasan::unpoison_range(reinterpret_cast<void*>(globals[i].beg), globals[i].size);
        // Poison the trailing redzone (bytes after the object up to size_with_redzone).
        size_t redzone = globals[i].size_with_redzone - globals[i].size;
        if (redzone > 0) {
            ker::mod::kasan::poison_range(reinterpret_cast<void*>(globals[i].beg + globals[i].size), redzone,
                                          ker::mod::kasan::SHADOW_GLOBAL_REDZONE);
        }
    }
}

void __asan_unregister_globals(AsanGlobalDescriptor*, size_t) {}
void __asan_register_image_globals(uintptr_t) {}
void __asan_unregister_image_globals(uintptr_t) {}
void __asan_register_elf_globals(uintptr_t, void*, void*) {}
void __asan_unregister_elf_globals(uintptr_t, void*, void*) {}

// ---- Shadow bulk-set helpers ----------------------------------------------
// Emitted by the compiler to fill shadow memory with a constant byte value
// (e.g. 0x00 to unpoison, 0xf8 to mark freed).
void __asan_set_shadow_00(uintptr_t addr, size_t size) { memset(reinterpret_cast<void*>(addr), 0x00, size); }
void __asan_set_shadow_f1(uintptr_t addr, size_t size) { memset(reinterpret_cast<void*>(addr), 0xf1, size); }
void __asan_set_shadow_f2(uintptr_t addr, size_t size) { memset(reinterpret_cast<void*>(addr), 0xf2, size); }
void __asan_set_shadow_f3(uintptr_t addr, size_t size) { memset(reinterpret_cast<void*>(addr), 0xf3, size); }
void __asan_set_shadow_f5(uintptr_t addr, size_t size) { memset(reinterpret_cast<void*>(addr), 0xf5, size); }
void __asan_set_shadow_f8(uintptr_t addr, size_t size) { memset(reinterpret_cast<void*>(addr), 0xf8, size); }

// ---- Dynamic init guards --------------------------------------------------
// Called around dynamic initializers of globals when -asan-globals=1.
// We use demand-paged zeroed shadow so no explicit action is needed.
void __asan_before_dynamic_init(const char* /*module_name*/) {}
void __asan_after_dynamic_init() {}

// ---- Misc callbacks -------------------------------------------------------
// Called at function exit when the function may have modified the stack or
// longjmp'd past the instrumented frame.
void __asan_handle_no_return() {}

// ---- Sanitizer container annotation stubs ---------------------------------
// Used by libc++ hardened containers to annotate ASan shadow for partial
// buffer access detection.  Not applicable to kernel code.
void __sanitizer_annotate_contiguous_container(const void*, const void*, const void*, const void*) {}
void __sanitizer_annotate_double_ended_contiguous_container(const void*, const void*, const void*, const void*, const void*, const void*) {}

// ---- Misc runtime options -------------------------------------------------
int __asan_option_detect_stack_use_after_return = 0;

// Called by the ASan runtime init — we use our own init().
void __asan_init() { ker::mod::kasan::init(); }
void __asan_version_mismatch_check_v8() {}

// ---- memset/memcpy/memmove interceptors -----------------------------------
// The compiler emits calls to these instead of the libc versions when ASan is
// active.  We just delegate to the kernel's existing implementations.

void* __asan_memset(void* dst, int c, size_t n) { return memset(dst, c, n); }
void* __asan_memcpy(void* dst, const void* src, size_t n) { return memcpy(dst, src, n); }
void* __asan_memmove(void* dst, const void* src, size_t n) { return memmove(dst, src, n); }

// ---- Poison/unpoison helpers called by the runtime ------------------------
void __asan_poison_memory_region(void const volatile* addr, size_t size) {
    ker::mod::kasan::poison_range((void*)addr, size, ker::mod::kasan::SHADOW_POISONED);  // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
}
void __asan_unpoison_memory_region(void const volatile* addr, size_t size) {
    ker::mod::kasan::unpoison_range((void*)addr, size);  // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
}

int __asan_address_is_poisoned(void const volatile* addr) {
    auto a = (uintptr_t)addr;  // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
    int8_t* shadow = ker::mod::kasan::addr_to_shadow(a);
    int8_t shadow_val = *shadow;
    if (shadow_val == 0) return 0;
    // Partial poisoning: check if the specific byte within the granule is bad
    size_t offset = a & 7;
    return (shadow_val < 0) || (static_cast<size_t>(shadow_val) <= offset) ? 1 : 0;
}

uintptr_t __asan_region_is_poisoned(uintptr_t beg, size_t size) {
    for (uintptr_t addr = beg; addr < beg + size; addr += 8) {
        if (__asan_address_is_poisoned(reinterpret_cast<void*>(addr))) {
            return addr;
        }
    }
    return 0;
}

}  // extern "C"

#endif  // WOS_KASAN
