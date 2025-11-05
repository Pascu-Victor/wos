#pragma once

#include <limine.h>

#include <cstdint>
#include <defines/defines.hpp>
#include <util/hcf.hpp>

namespace ker::mod::mm::addr {
using vaddr_t = uint64_t;
using paddr_t = uint64_t;

void setHHDMOffset(uint64_t offset);
auto getHHDMOffset() -> uint64_t;
void setMMAPSize(uint64_t size);

auto getVirtPointer(vaddr_t paddr) -> vaddr_t*;
auto getPhysPointer(paddr_t vaddr) -> paddr_t*;

void init(limine_hhdm_response* hhdmResponse);
}  // namespace ker::mod::mm::addr
