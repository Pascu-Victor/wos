#pragma once

#include <cstdint>
#include <net/wki/rdma_protocol.hpp>

namespace ker::net::wki::rdma_control {

constexpr size_t NATIVE_INTENT_BYTES = 64;

// These payloads travel only on the ordinary authenticated reliable channel4.
// They do not modify HELLO or reinterpret any of its occupied capability bits.
struct Confirm {
    rdma_protocol::SessionId session_id{};
    auth_crypto::Digest negotiation{};
    uint16_t version = rdma_protocol::VERSION;
    uint8_t transports = 0;
    uint8_t services = 0;
    uint32_t reserved = 0;
} __attribute__((packed));
static_assert(sizeof(Confirm) == 56);

struct Arm {
    rdma_protocol::RegionDescriptor grant{};
    uint64_t operation_id = 0;
    uint64_t receive_ticket = 0;
    uint64_t offset = 0;
    uint32_t length = 0;
    uint16_t flags = 0;
    uint8_t opcode = 0;
    uint8_t reserved = 0;
    uint16_t version = rdma_protocol::VERSION;
    uint16_t reserved2 = 0;
    uint16_t intent_length = 0;
    uint16_t reserved3 = 0;
    // Service-specific authenticated native intent. Unused bytes are zero;
    // exact ARM_ACK/CANCEL comparison includes the complete fixed encoding.
    std::array<uint8_t, NATIVE_INTENT_BYTES> native_intent{};
} __attribute__((packed));
static_assert(sizeof(Arm) == 168);

struct ArmAck {
    Arm request{};
    int32_t status = 0;
} __attribute__((packed));
static_assert(sizeof(ArmAck) == 172);

// Native completion can report an exact partial VFS write without promising a
// full successful WRITE_END. It is trusted only after exact request/context
// matching and retirement of the sender's local TX work.
struct Result {
    Arm request{};
    int32_t status = 0;
    uint32_t bytes = 0;
    uint8_t outcome = 0;
    std::array<uint8_t, 7> reserved{};
} __attribute__((packed));
static_assert(sizeof(Result) == 184);

}  // namespace ker::net::wki::rdma_control
