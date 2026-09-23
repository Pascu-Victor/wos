#pragma once
#include <extern/limine.h>

#include <cstddef>
#include <cstdint>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::mm {
void init();
// Boot-only immutable firmware metadata. Consumers must bound entry_count and
// finish before any future bootloader-memory reclamation. Do not retain entries.
[[nodiscard]] auto boot_memory_map() -> const limine_memmap_response*;
inline constexpr size_t KERNEL_STACK_SIZE = 0x80000;  // 512KB
inline constexpr size_t KERNEL_STACK_POOL_CAPACITY = 256;
inline constexpr size_t KERNEL_STACK_GUARD_SIZE = 0x1000;
inline constexpr size_t KERNEL_STACK_SLOT_STRIDE = KERNEL_STACK_SIZE + (2 * KERNEL_STACK_GUARD_SIZE);
// This range is outside KASan's reserved shadow interval and the linked kernel
// image. Guard pages and usable stack pages occupy one fixed slot each.
inline constexpr uintptr_t GUARDED_KERNEL_STACK_ARENA_BASE = 0xfffffd0000000000ULL;
inline constexpr uintptr_t GUARDED_KERNEL_STACK_ARENA_END =
    GUARDED_KERNEL_STACK_ARENA_BASE + (KERNEL_STACK_POOL_CAPACITY * KERNEL_STACK_SLOT_STRIDE);
inline constexpr uintptr_t LEGACY_KERNEL_STACK_MIN = 0xffff800000000000ULL;
inline constexpr uintptr_t LEGACY_KERNEL_STACK_MAX = 0xffff900000000000ULL;

[[nodiscard]] constexpr auto is_kernel_stack_address(uintptr_t address) -> bool {
    if (address >= LEGACY_KERNEL_STACK_MIN && address < LEGACY_KERNEL_STACK_MAX) {
        return true;
    }
    if (address < GUARDED_KERNEL_STACK_ARENA_BASE || address >= GUARDED_KERNEL_STACK_ARENA_END) {
        return false;
    }
    uintptr_t const IN_SLOT = (address - GUARDED_KERNEL_STACK_ARENA_BASE) % KERNEL_STACK_SLOT_STRIDE;
    // The one-past-end stack top is a valid saved RSP; guard-page interiors are
    // not valid stack addresses even though they belong to the arena.
    return IN_SLOT >= KERNEL_STACK_GUARD_SIZE && IN_SLOT <= KERNEL_STACK_GUARD_SIZE + KERNEL_STACK_SIZE;
}
inline constexpr size_t USER_STACK_SIZE = 0x4000000;  // 64MiB
inline constexpr size_t TLS_STATIC_SIZE = 0x16000;    // 64KB

template <size_t RequestedStackSize = KERNEL_STACK_SIZE>
struct Stack {
    static_assert(RequestedStackSize <= KERNEL_STACK_SIZE);
    // Even short-lived AP handoff stacks use a complete guarded pool slot.
    // The template argument remains source-compatible with legacy callers but
    // is only a minimum usable-size request, never an unguarded allocation.
    static constexpr size_t SIZE = KERNEL_STACK_SIZE;
    uint64_t* sp{};
    uint64_t* base{};

    Stack() : base(static_cast<uint64_t*>(phys::kernel_stack_alloc("cpu_handoff_stack"))) {
        sp = base != nullptr ? base + (KERNEL_STACK_SIZE / sizeof(uint64_t)) : nullptr;
    }

    void free() { phys::page_free(base); }
};
}  // namespace ker::mod::mm
