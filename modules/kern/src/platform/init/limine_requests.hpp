#pragma once

#include <limine.h>

#include <cstdint>

namespace ker::init {

// Access the kernel module request from limine
auto get_kernel_module_request() -> volatile limine_module_request&;

// Access/set the captured kernel stack pointer
auto get_kernel_rsp() -> uint64_t;
void set_kernel_rsp(uint64_t rsp);

}  // namespace ker::init
