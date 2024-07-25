#pragma once

#include <limine.h>
#include <defines/defines.hpp>
#include <util/funcs.hpp>

namespace ker::mod::mm::addr {
    typedef uint64_t vaddr_t, paddr_t;

    void setHHDMOffset(uint64_t offset);
    void setMMAPSize(uint64_t size);

    paddr_t* getPhysAddr(vaddr_t vaddr);
    vaddr_t* getVirtAddr(paddr_t paddr);

    void init(limine_hhdm_response* hhdmResponse);
}