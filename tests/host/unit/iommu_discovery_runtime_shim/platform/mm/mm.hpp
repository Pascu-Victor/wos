#pragma once

#include <extern/limine.h>

#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::mm {
[[nodiscard]] auto boot_memory_map() -> const limine_memmap_response*;
}
