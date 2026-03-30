#include "addr.hpp"

#include <extern/limine.h>

#include <cstdint>

#include "util/hcf.hpp"

namespace ker::mod::mm::addr {
static uint64_t hhdm_offset = 0;
static uint64_t mmap_size = 0;

void set_hhdm_offset(uint64_t offset) { hhdm_offset = offset; }

auto get_hhdm_offset() -> uint64_t { return hhdm_offset; }

void set_mmap_size(uint64_t size) { mmap_size = size; }

auto get_virt_pointer(vaddr_t vaddr) -> vaddr_t* { return reinterpret_cast<vaddr_t*>(vaddr + hhdm_offset); }

auto get_phys_pointer(paddr_t paddr) -> paddr_t* { return reinterpret_cast<paddr_t*>(paddr - hhdm_offset); }

void init(limine_hhdm_response* hhdm_response) {
    if (hhdm_response == nullptr) {
        hcf();
    }

    set_hhdm_offset(hhdm_response->offset);
}
}  // namespace ker::mod::mm::addr
