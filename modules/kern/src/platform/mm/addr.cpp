#include "addr.hpp"

namespace ker::mod::mm::addr {
    static uint64_t hhdmOffset = 0;
    static uint64_t mmapSize = 0;

    void setHHDMOffset(uint64_t offset) {
        hhdmOffset = offset;
    }

    void setMMAPSize(uint64_t size) {
        mmapSize = size;
    }

    paddr_t* getVirtPointer(paddr_t vaddr) {
        return reinterpret_cast<paddr_t*>(vaddr + hhdmOffset);
    }

    vaddr_t* getPhysPointer(vaddr_t paddr) {
        return reinterpret_cast<vaddr_t*>(paddr - hhdmOffset);
    }

    void init(limine_hhdm_response* hhdmResponse) {
        if (hhdmResponse == nullptr) {
            hcf();
        }

        setHHDMOffset((uint64_t)hhdmResponse->offset);
    }
}