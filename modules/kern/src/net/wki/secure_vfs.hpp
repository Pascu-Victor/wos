#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <net/wki/secure_rdma.hpp>

namespace ker::net::wki {

// Opaque native intent authenticated in ARM/RESULT. Numeric fd is only a lookup
// hint; the ordinary-open token, binding and incarnation are native authority.
struct VfsSecureIoIntent {
    uint16_t version = 1;
    uint8_t opcode = 0;
    uint8_t reserved0 = 0;
    int32_t fd = -1;
    int64_t file_offset = 0;
    uint32_t length = 0;
    uint32_t reserved1 = 0;
    uint64_t binding_id = 0;
    uint64_t open_token = 0;
    uint32_t owner_boot_epoch = 0;
    uint32_t resource_incarnation = 0;
    uint32_t resource_id = 0;
    uint32_t reserved2 = 0;
    uint64_t reserved3 = 0;
} __attribute__((packed));
static_assert(sizeof(VfsSecureIoIntent) == rdma_control::NATIVE_INTENT_BYTES);

struct VfsSecureIoResult {
    int32_t status = 0;
    uint32_t bytes = 0;
    uint8_t outcome = static_cast<uint8_t>(WkiSecureRdmaOutcome::NO_EFFECT);
    std::array<uint8_t, 7> reserved{};
} __attribute__((packed));
static_assert(sizeof(VfsSecureIoResult) == 16);

inline auto wki_vfs_secure_intent_valid(const VfsSecureIoIntent& intent) -> bool {
    return intent.version == 1 &&
           (intent.opcode == static_cast<uint8_t>(rdma_protocol::Opcode::READ) ||
            intent.opcode == static_cast<uint8_t>(rdma_protocol::Opcode::WRITE)) &&
           intent.reserved0 == 0 && intent.reserved1 == 0 && intent.reserved2 == 0 && intent.reserved3 == 0 && intent.fd >= 0 &&
           intent.length > 0 && intent.length <= rdma_protocol::MAX_TRANSFER_BYTES && intent.binding_id != 0 && intent.open_token != 0 &&
           intent.owner_boot_epoch != 0 && intent.resource_incarnation != 0 && intent.resource_id != 0 && intent.file_offset >= 0 &&
           static_cast<uint64_t>(intent.file_offset) <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - intent.length;
}

inline auto wki_vfs_secure_decode_intent(const WkiSecureRdmaRequest& request, VfsSecureIoIntent& output) -> bool {
    if (request.intent_length != sizeof(output) || request.offset != 0 || request.length == 0 ||
        request.length > rdma_protocol::MAX_TRANSFER_BYTES || request.operation_id == 0 || request.receive_ticket == 0)
        return false;
    VfsSecureIoIntent intent{};
    __builtin_memcpy(&intent, request.native_intent.data(), sizeof(intent));
    if (!wki_vfs_secure_intent_valid(intent) || intent.opcode != static_cast<uint8_t>(request.opcode) || intent.length != request.length)
        return false;
    output = intent;
    return true;
}

inline auto wki_vfs_secure_message_result_valid(const VfsSecureIoResult& result, bool write, uint32_t requested, size_t payload) -> bool {
    for (auto byte : result.reserved)
        if (byte != 0) return false;
    if (result.bytes > requested || result.outcome > static_cast<uint8_t>(WkiSecureRdmaOutcome::UNCERTAIN)) return false;
    if (result.outcome == static_cast<uint8_t>(WkiSecureRdmaOutcome::EXACT)) {
        if (result.status != 0 || (write && result.bytes == 0)) return false;
    } else if (result.status >= 0 || result.bytes != 0) {
        return false;
    }
    return payload == sizeof(result) + (write ? 0 : result.bytes);
}

}  // namespace ker::net::wki
