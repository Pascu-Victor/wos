#include "addr.hpp"

namespace ker::mod::mm::addr {
static uint64_t hhdmOffset = 0;
static uint64_t mmapSize = 0;

void setHHDMOffset(uint64_t offset) { hhdmOffset = offset; }

uint64_t getHHDMOffset() { return hhdmOffset; }

void setMMAPSize(uint64_t size) { mmapSize = size; }

auto getVirtPointer(vaddr_t vaddr) -> vaddr_t* { return reinterpret_cast<vaddr_t*>(vaddr + hhdmOffset); }

auto getPhysPointer(paddr_t paddr) -> paddr_t* { return reinterpret_cast<paddr_t*>(paddr - hhdmOffset); }

void init(limine_hhdm_response* hhdmResponse) {
    if (hhdmResponse == nullptr) {
        hcf();
    }

    setHHDMOffset((uint64_t)hhdmResponse->offset);
}
}  // namespace ker::mod::mm::addr
