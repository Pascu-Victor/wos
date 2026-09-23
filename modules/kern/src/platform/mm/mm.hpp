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
inline constexpr size_t USER_STACK_SIZE = 0x4000000;  // 64MiB
inline constexpr size_t TLS_STATIC_SIZE = 0x16000;    // 64KB

template <size_t StackSize = 4096>
struct Stack {
    static constexpr size_t SIZE = StackSize;
    uint64_t* sp{};
    uint64_t* base{};

    Stack() : base(static_cast<uint64_t*>(phys::page_alloc(PhysicalPageOwner::KERNEL_STACK, StackSize, "kernel_stack"))) {
        sp = base + (StackSize / sizeof(uint64_t));
    }

    void free() { phys::page_free(base); }
};
}  // namespace ker::mod::mm
