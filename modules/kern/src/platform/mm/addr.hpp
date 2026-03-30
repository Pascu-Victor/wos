#pragma once

#include <extern/limine.h>

#include <cstdint>
#include <defines/defines.hpp>
#include <util/hcf.hpp>

namespace ker::mod::mm::addr {
using vaddr_t = uint64_t;
using paddr_t = uint64_t;

void set_hhdm_offset(uint64_t offset);
auto get_hhdm_offset() -> uint64_t;
void set_mmap_size(uint64_t size);

auto get_virt_pointer(vaddr_t vaddr) -> vaddr_t*;
auto get_phys_pointer(paddr_t paddr) -> paddr_t*;

void init(limine_hhdm_response* hhdm_response);
}  // namespace ker::mod::mm::addr
