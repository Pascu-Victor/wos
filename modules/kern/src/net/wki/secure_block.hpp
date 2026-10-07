#pragma once

#include <array>
#include <cstdint>
#include <net/wki/secure_rdma.hpp>
#include <net/wki/wire.hpp>

namespace ker::net::wki {

struct DevServerBinding;
struct BlockSecureContext;
constexpr uint32_t WKI_SECURE_BLOCK_BYTES = 2 * 1024 * 1024;
constexpr uint16_t WKI_SECURE_BLOCK_VERSION = 1;

// Exact authenticated attach, independent of the legacy shared-ring flags.
struct BlockSecurePrepare {
    uint16_t version = WKI_SECURE_BLOCK_VERSION;
    uint8_t attach_cookie = 0;
    uint8_t reserved = 0;
    uint32_t resource_id = 0;
    ResourceIncarnationToken incarnation{};
} __attribute__((packed));
static_assert(sizeof(BlockSecurePrepare) == 16);

struct BlockSecureIntent {
    uint16_t version = WKI_SECURE_BLOCK_VERSION;
    uint8_t opcode = 0;
    uint8_t attach_cookie = 0;
    uint32_t resource_id = 0;
    ResourceIncarnationToken incarnation{};
    uint64_t binding_generation = 0;
    uint64_t lba = 0;
    uint32_t count = 0;
    uint32_t block_size = 0;
    uint64_t sequence = 0;
    std::array<uint8_t, 16> reserved{};
} __attribute__((packed));
static_assert(sizeof(BlockSecureIntent) == rdma_control::NATIVE_INTENT_BYTES);

// Called only by an accepted task worker retaining the exact BLOCK binding.
auto wki_secure_block_prepare(DevServerBinding* binding, const BlockSecurePrepare& request, rdma_protocol::RegionDescriptor& output) -> int;
void wki_secure_block_close_context(BlockSecureContext* context);
void wki_secure_block_release_context(BlockSecureContext* context);

}  // namespace ker::net::wki
