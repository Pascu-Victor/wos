#pragma once
#include <cstdint>

namespace ker::abi::vmem {
enum class ops : uint64_t {
    anon_allocate,
    anon_free,
};

// Protection flags (matching Linux mmap)
constexpr uint64_t PROT_NONE = 0x0;
constexpr uint64_t PROT_READ = 0x1;
constexpr uint64_t PROT_WRITE = 0x2;
constexpr uint64_t PROT_EXEC = 0x4;

// Flags (matching Linux mmap)
constexpr uint64_t MAP_SHARED = 0x01;
constexpr uint64_t MAP_PRIVATE = 0x02;
constexpr uint64_t MAP_FIXED = 0x10;
constexpr uint64_t MAP_ANONYMOUS = 0x20;

// Error codes
constexpr int VMEM_SUCCESS = 0;
constexpr int VMEM_ENOMEM = 12;     // Out of memory
constexpr int VMEM_EINVAL = 22;     // Invalid argument
constexpr int VMEM_EFAULT = 14;     // Bad address
constexpr int VMEM_EOVERFLOW = 75;  // Value too large

}  // namespace ker::abi::vmem
