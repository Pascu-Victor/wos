#pragma once

// WOS Kernel AddressSanitizer (KASan) — public interface.
//
// Gated behind cmake -DWOS_KASAN=ON.  When WOS_KASAN is not defined all
// inline helpers below compile to no-ops so call sites have zero overhead.
//
// Shadow memory layout (1 shadow byte covers 8 application bytes):
//   shadow_addr = (app_addr >> 3) + KASAN_SHADOW_OFFSET
//   KASAN_SHADOW_OFFSET = 0xdffffc0000000000  (matches the -asan-mapping-offset passed to Clang)
//
// Shadow byte values (matching ASan convention):
//   0x00        = all 8 bytes accessible
//   0x01..0x07  = first N bytes accessible, rest are redzone
//   0xf1        = heap left redzone
//   0xf2        = heap right redzone (mid-allocation redzone for large allocs)
//   0xf3        = heap right redzone (after allocation)
//   0xf5        = stack left redzone
//   0xf8        = heap freed (use-after-free)
//   0xff        = poisoned (generic)

#include <cstddef>
#include <cstdint>

namespace ker::mod::kasan {

// Shadow memory base.  Must match -asan-mapping-offset passed to Clang.
constexpr uint64_t SHADOW_OFFSET = 0xdffffc0000000000ULL;

// Shadow byte sentinels
constexpr int8_t SHADOW_ACCESSIBLE = 0x00;
constexpr int8_t SHADOW_HEAP_LREDZONE = static_cast<int8_t>(0xf1);
constexpr int8_t SHADOW_HEAP_RREDZONE = static_cast<int8_t>(0xf3);
constexpr int8_t SHADOW_HEAP_FREED = static_cast<int8_t>(0xf8);
constexpr int8_t SHADOW_GLOBAL_REDZONE = static_cast<int8_t>(0xf9);
constexpr int8_t SHADOW_POISONED = static_cast<int8_t>(0xff);

// Convert an application address to its shadow address.
inline auto addr_to_shadow(uintptr_t addr) -> int8_t* { return reinterpret_cast<int8_t*>((addr >> 3) + SHADOW_OFFSET); }

// Handle a shadow-region page fault (cr2 in shadow range).
// Called from the page-fault handler in gates.cpp / pagefault_handler().
// Allocates a zeroed (accessible) page for the shadow on demand.
// Returns true if the fault was handled (shadow page allocated), false otherwise.
auto handle_shadow_fault(uint64_t cr2) -> bool;

// Poison/unpoison a byte range in the shadow map.
// value == SHADOW_ACCESSIBLE (0x00) = unpoison.
void poison_range(const void* ptr, size_t size, int8_t value);
void unpoison_range(const void* ptr, size_t size);

// Called from early kernel init to pre-map the shadow region for the
// kernel static image (text/data/bss).  Everything else is demand-faulted.
void init();

// Enable runtime shadow checking.  Must be called after the IDT is live
// so that shadow demand-faulting works.  Before this call, all outline
// __asan_load/store checks are no-ops.
void enable();

// Returns true if runtime shadow checking is active.
auto is_enabled() -> bool;

// Returns true if the current CPU is inside handle_shadow_fault.
// Used by pageAlloc to skip unpoison_range and avoid recursive shadow faults.
auto in_shadow_fault() -> bool;

}  // namespace ker::mod::kasan
