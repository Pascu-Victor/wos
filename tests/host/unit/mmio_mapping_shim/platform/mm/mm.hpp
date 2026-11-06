#pragma once
#include <extern/limine.h>
namespace ker::mod::mm {
[[nodiscard]] auto boot_memory_map() -> const limine_memmap_response*;
}
