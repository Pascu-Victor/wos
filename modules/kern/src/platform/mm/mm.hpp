#pragma once
#include <extern/limine.h>

#include <cstddef>
#include <cstdint>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::mm {
void init();
inline constexpr size_t KERNEL_STACK_SIZE = 0x80000;  // 512KB
inline constexpr size_t USER_STACK_SIZE = 0x800000;   // 8MiB
inline constexpr size_t TLS_STATIC_SIZE = 0x16000;    // 64KB

template <size_t StackSize = 4096>
struct Stack {
    static constexpr size_t SIZE = StackSize;
    uint64_t* sp{};
    uint64_t* base{};

    Stack() : base(static_cast<uint64_t*>(phys::page_alloc(StackSize))) { sp = base + (StackSize / sizeof(uint64_t)); }

    void free() { phys::page_free(base); }
};
}  // namespace ker::mod::mm
