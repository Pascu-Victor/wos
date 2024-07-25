#pragma once

#include <limine.h>
#include <defines/defines.hpp>
#include <util/funcs.hpp>

namespace ker::mod::mm::addr {
    typedef uint64_t vaddr_t, paddr_t;

    void setHHDMOffset(uint64_t offset);
    void setMMAPSize(uint64_t size);

    paddr_t* getVirtPointer(paddr_t paddr);
    vaddr_t* getPhysPointer(vaddr_t vaddr);

    void init(limine_hhdm_response* hhdmResponse);
}