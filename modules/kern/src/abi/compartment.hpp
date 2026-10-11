#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::abi::compartment {
inline constexpr uint64_t VERSION = 1;
inline constexpr uint64_t CREATE_OPERATION = 54;
inline constexpr size_t LIMIT_COUNT = 16;
inline constexpr size_t ASSET_MAX = 64;
inline constexpr size_t DESCRIPTOR_MAX = 256;
inline constexpr size_t VECTOR_MAX = 256;
inline constexpr size_t PATH_BYTES_MAX = 512;
inline constexpr size_t ARGUMENT_BYTES_MAX = 2 * 1024 * 1024;
inline constexpr uint64_t ROOT_BYTES_MAX = 32 * 1024 * 1024;
enum class Network : uint32_t { DENIED = 0, SHARED_HOST = 1 };

// Addresses refer to executing-process memory. Strings exclude a terminating
// NUL; embedded NULs are rejected. Empty argument strings are permitted.
struct String {
    uint64_t address{};
    uint64_t bytes{};
};
struct Limit {
    uint64_t soft = UINT64_MAX;
    uint64_t hard = UINT64_MAX;
};
// Assets are copied into the fresh root. Descriptors retain the same open File
// and its rights/offset; no ambient descriptors are inherited.
struct Asset {
    int32_t source_fd = -1;
    uint32_t mode = 0600;
    String destination;
    uint64_t reserved{};
};
struct Descriptor {
    int32_t source_fd = -1;
    uint32_t destination{};
    uint64_t reserved{};
};
struct Spec {
    uint64_t size = sizeof(Spec);
    uint64_t version = VERSION;
    uint64_t flags{};
    uint64_t root_bytes = ROOT_BYTES_MAX;
    uint64_t authority_ceiling{};
    uint64_t service_privileges{};
    Network network{Network::DENIED};
    uint32_t dumpable{};
    std::array<Limit, LIMIT_COUNT> limits;
    String executable;
    uint64_t assets{};
    uint64_t asset_count{};
    uint64_t descriptors{};
    uint64_t descriptor_count{};
    uint64_t argv{};
    uint64_t argc{};
    uint64_t envp{};
    uint64_t envc{};
    std::array<uint64_t, 2> reserved{};
};
static_assert(sizeof(String) == 16 && sizeof(Limit) == 16);
static_assert(sizeof(Asset) == 32 && sizeof(Descriptor) == 16);
static_assert(sizeof(Spec) == 408 && alignof(Spec) == 8);
static_assert(offsetof(Spec, limits) == 56 && offsetof(Spec, executable) == 312);
}  // namespace ker::abi::compartment
