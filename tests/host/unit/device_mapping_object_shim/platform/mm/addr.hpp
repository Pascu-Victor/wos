#pragma once
#include <cstdint>
namespace ker::mod::mm::addr {
inline auto get_phys_pointer(uint64_t pointer) -> void* { return reinterpret_cast<void*>(pointer); }
inline auto get_virt_pointer(uint64_t pointer) -> void* { return reinterpret_cast<void*>(pointer); }
}  // namespace ker::mod::mm::addr
