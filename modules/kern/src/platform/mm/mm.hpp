#pragma once
#include <limine.h>

#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::mm {
void init(void);

template <size_t StackSize = 4096>
struct Stack {
    static const size_t size = StackSize;
    uint64_t *sp;
    uint64_t *base;

    Stack() {
        base = (uint64_t *)phys::pageAlloc(StackSize);
        sp = base + StackSize / sizeof(uint64_t);
    }

    void free() { phys::pageFree(base); }
};
}  // namespace ker::mod::mm
