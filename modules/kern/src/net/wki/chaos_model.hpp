#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/blk_ring.hpp>
#include <net/wki/wire.hpp>

namespace ker::net::wki {

constexpr uint32_t WKI_CHAOS_SCHEMA_VERSION = 1;
constexpr size_t WKI_CHAOS_MAX_RULES = 32;
constexpr size_t WKI_CHAOS_MAX_QUEUED_FRAMES = 8;
constexpr size_t WKI_CHAOS_MAX_QUEUED_BLOCK_EVENTS = 8;
constexpr size_t WKI_CHAOS_MAX_TRACE_ROWS = 256;
constexpr size_t WKI_CHAOS_MAX_STREAMS = 128;
constexpr size_t WKI_CHAOS_MAX_FRAME_SIZE = WKI_HEADER_SIZE + WKI_ETH_MAX_PAYLOAD;
// Server-side completion notifications do not retain the proxy's waiter
// deadline, so their copied metadata receives this conservative finite hold
// window. Proxy submissions carry the exact operation deadline instead.
constexpr uint64_t WKI_CHAOS_BLOCK_DOORBELL_MAX_HOLD_US = 400'000;
constexpr uint16_t WKI_CHAOS_BLOCK_SQE_BYTES = sizeof(BlkSqEntry);

enum class WkiChaosDirection : uint8_t {
    TX = 0,
    RX = 1,
};

enum class WkiChaosSurface : uint8_t {
    FRAME = 0,
    BLOCK_DOORBELL = 1,
    BLOCK_SQE = 2,
};

enum class WkiChaosBlockOrigin : uint8_t {
    PROXY = 0,
    SERVER = 1,
};

enum class WkiChaosBlockLane : uint8_t {
    IVSHMEM = 0,
    ROCE = 1,
};

enum class WkiChaosAction : uint8_t {
    PASS = 0,
    DROP = 1,
    DUPLICATE = 2,
    DELAY = 3,
    REORDER = 4,
    CORRUPT = 5,
    FAIL = 6,
    PARTITION = 7,
    RELEASE = 8,
};

enum class WkiChaosCorruptMode : uint8_t {
    CHECKSUM = 0,
    PAYLOAD_XOR = 1,
};

enum class WkiChaosOutcome : uint8_t {
    PASSED = 0,
    DROPPED = 1,
    DUPLICATED = 2,
    QUEUED = 3,
    FAILED = 4,
    RELEASED = 5,
    QUEUE_OVERFLOW = 6,
    STREAM_OVERFLOW = 7,
    TRANSPORT_REMOVED = 8,
    CORRUPTED = 9,
    COALESCED = 10,
    HEALED = 11,
};

struct WkiChaosBlockKey {
    WkiChaosSurface surface = WkiChaosSurface::BLOCK_DOORBELL;
    WkiChaosDirection direction = WkiChaosDirection::TX;
    WkiChaosBlockOrigin origin = WkiChaosBlockOrigin::PROXY;
    WkiChaosBlockLane lane = WkiChaosBlockLane::IVSHMEM;
    uint16_t transport_id = 0;
    uint16_t neighbor = WKI_NODE_INVALID;
    uint32_t zone_id = 0;
    uint32_t resource_id = 0;
    uint32_t ring_generation = 0;
    uint32_t ring_index = 0;
    uint32_t operation_cookie = 0;
    uint8_t block_opcode = 0;
    uint8_t attach_cookie = 0;
    uint32_t channel_generation = 0;
    uint32_t owner_boot_epoch = 0;
    uint32_t resource_incarnation = 0;
    uint16_t descriptor_len = 0;
    // Local-only expiry metadata. It is deliberately excluded from matching
    // and deterministic hashes; it bounds deferred delivery without changing
    // the block-ring or WKI wire layouts.
    uint64_t delivery_deadline_us = 0;
    uint64_t stream_occurrence = 0;
    uint64_t stable_hash = 0;
};

constexpr auto wki_chaos_block_ring_generation(uint32_t channel_generation, uint8_t attach_cookie, uint32_t owner_boot_epoch,
                                               uint32_t resource_incarnation) -> uint32_t {
    uint32_t value = channel_generation ^ (static_cast<uint32_t>(attach_cookie) << 24U);
    value ^= owner_boot_epoch * 0x9E3779B1U;
    value ^= resource_incarnation * 0x85EBCA77U;
    value ^= value >> 16U;
    return value == 0 ? 1 : value;
}

constexpr auto wki_chaos_block_identity_equal(const WkiChaosBlockKey& lhs, const WkiChaosBlockKey& rhs) -> bool {
    return lhs.surface == rhs.surface && lhs.direction == rhs.direction && lhs.origin == rhs.origin && lhs.lane == rhs.lane &&
           lhs.transport_id == rhs.transport_id && lhs.neighbor == rhs.neighbor && lhs.zone_id == rhs.zone_id &&
           lhs.resource_id == rhs.resource_id && lhs.ring_generation == rhs.ring_generation && lhs.ring_index == rhs.ring_index &&
           lhs.operation_cookie == rhs.operation_cookie && lhs.block_opcode == rhs.block_opcode && lhs.attach_cookie == rhs.attach_cookie &&
           lhs.channel_generation == rhs.channel_generation && lhs.owner_boot_epoch == rhs.owner_boot_epoch &&
           lhs.resource_incarnation == rhs.resource_incarnation;
}

struct WkiChaosFrameKey {
    WkiChaosDirection direction = WkiChaosDirection::TX;
    uint16_t transport_id = 0;
    uint16_t neighbor = WKI_NODE_INVALID;
    uint16_t src_node = WKI_NODE_INVALID;
    uint16_t dst_node = WKI_NODE_INVALID;
    uint16_t channel_id = 0;
    uint8_t msg_type = 0;
    uint32_t seq_num = 0;
    uint32_t ack_num = 0;
    uint16_t frame_len = 0;
    uint16_t payload_len = 0;
    bool frame_valid = false;
    bool dev_op_valid = false;
    uint16_t dev_op_id = 0;
    uint64_t stream_occurrence = 0;
    uint64_t stable_hash = 0;
};

struct WkiChaosRule {
    uint32_t id = 0;
    bool active = false;
    WkiChaosSurface surface = WkiChaosSurface::FRAME;
    WkiChaosDirection direction = WkiChaosDirection::TX;
    WkiChaosAction action = WkiChaosAction::PASS;
    bool match_transport = false;
    uint16_t transport_id = 0;
    bool match_neighbor = false;
    uint16_t neighbor = WKI_NODE_INVALID;
    bool match_src = false;
    uint16_t src_node = WKI_NODE_INVALID;
    bool match_dst = false;
    uint16_t dst_node = WKI_NODE_INVALID;
    bool match_channel = false;
    uint16_t channel_id = 0;
    bool match_type = false;
    uint8_t msg_type = 0;
    bool match_op = false;
    uint16_t op_id = 0;
    bool match_block_origin = false;
    WkiChaosBlockOrigin block_origin = WkiChaosBlockOrigin::PROXY;
    bool match_block_lane = false;
    WkiChaosBlockLane block_lane = WkiChaosBlockLane::IVSHMEM;
    bool match_zone = false;
    uint32_t zone_id = 0;
    bool match_resource = false;
    uint32_t resource_id = 0;
    bool match_ring_generation = false;
    uint32_t ring_generation = 0;
    bool match_ring_index = false;
    uint32_t ring_index = 0;
    bool match_cookie = false;
    uint32_t operation_cookie = 0;
    bool match_block_opcode = false;
    uint8_t block_opcode = 0;
    bool match_seq = false;
    uint32_t seq_num = 0;
    bool match_occurrence = false;
    uint64_t occurrence = 0;
    uint64_t after = 0;
    uint64_t every = 1;
    uint64_t limit = 0;
    uint32_t chance_permyriad = 10'000;
    WkiChaosCorruptMode corrupt_mode = WkiChaosCorruptMode::CHECKSUM;
    uint16_t payload_offset = 0;
    uint8_t payload_xor = 0;
    uint16_t sqe_offset = 0;
    uint8_t sqe_xor = 0;
    uint64_t applied = 0;
};

struct WkiChaosCorruptionTrace {
    WkiChaosCorruptMode mode = WkiChaosCorruptMode::CHECKSUM;
    uint16_t payload_offset = 0;
    uint8_t payload_xor = 0;
    uint8_t byte_before = 0;
    uint8_t byte_after = 0;
    bool payload_changed = false;
};

struct WkiChaosBlockCorruptionTrace {
    uint16_t sqe_offset = 0;
    uint8_t sqe_xor = 0;
    uint8_t byte_before = 0;
    uint8_t byte_after = 0;
    bool changed = false;
};

struct WkiChaosTraceRow {
    uint64_t event_id = 0;
    uint64_t release_order = 0;
    uint64_t queued_event_id = 0;
    WkiChaosFrameKey key = {};
    WkiChaosSurface surface = WkiChaosSurface::FRAME;
    WkiChaosBlockKey block_key = {};
    uint32_t rule_id = 0;
    WkiChaosAction action = WkiChaosAction::PASS;
    WkiChaosOutcome outcome = WkiChaosOutcome::PASSED;
    uint32_t checksum_before = 0;
    uint32_t checksum_after = 0;
    uint32_t local_boot_epoch = 0;
    uint32_t peer_boot_epoch = 0;
    uint32_t peer_channel_epoch = 0;
    int32_t transport_result = 0;
    uint64_t coalesced_count = 0;
    WkiChaosCorruptionTrace corruption = {};
    WkiChaosBlockCorruptionTrace block_corruption = {};
};

struct WkiChaosCounters {
    uint64_t observed = 0;
    uint64_t matched = 0;
    uint64_t passed = 0;
    uint64_t dropped = 0;
    uint64_t duplicated = 0;
    uint64_t delayed = 0;
    uint64_t reordered = 0;
    uint64_t corrupted = 0;
    uint64_t failed = 0;
    uint64_t released = 0;
    uint64_t coalesced = 0;
};

struct WkiChaosInterceptResult {
    WkiChaosAction action = WkiChaosAction::PASS;
    WkiChaosOutcome outcome = WkiChaosOutcome::PASSED;
    uint32_t rule_id = 0;
    uint64_t event_id = 0;
    bool matched = false;
    bool queued_ready = false;
};

struct WkiChaosDeliveryView {
    enum class Kind : uint8_t {
        FRAME = 0,
        BLOCK_DOORBELL = 1,
    };

    size_t slot = WKI_CHAOS_MAX_QUEUED_FRAMES;
    Kind kind = Kind::FRAME;
    WkiChaosDirection direction = WkiChaosDirection::TX;
    WkiChaosAction action = WkiChaosAction::PASS;
    uintptr_t transport_cookie = 0;
    uint64_t ingress_cookie = 0;
    uint16_t neighbor = WKI_NODE_INVALID;
    const uint8_t* data = nullptr;
    uint16_t len = 0;
    WkiChaosBlockKey block_key = {};
};

class WkiChaosModel {
   public:
    WkiChaosModel() = default;

    auto clear() -> bool;
    void enable(uint64_t seed);
    void disable();
    [[nodiscard]] auto enabled() const -> bool;
    [[nodiscard]] auto seed() const -> uint64_t;

    auto upsert_rule(const WkiChaosRule& rule) -> bool;
    auto heal_partition(uint32_t rule_id) -> bool;
    auto release(uint32_t rule_id, uint32_t count) -> size_t;

    auto intercept(WkiChaosDirection direction, uint16_t transport_id, uintptr_t transport_cookie, uint16_t neighbor, const void* data,
                   uint16_t len, uint32_t local_boot_epoch = 0, uint32_t peer_boot_epoch = 0, uint32_t peer_channel_epoch = 0,
                   uint64_t ingress_cookie = 0) -> WkiChaosInterceptResult;
    auto intercept_block_doorbell(WkiChaosBlockKey key) -> WkiChaosInterceptResult;
    auto intercept_block_sqe(WkiChaosBlockKey key, void* descriptor, uint16_t descriptor_len) -> WkiChaosInterceptResult;
    auto transport_removed(WkiChaosDirection direction, uint16_t transport_id, uint16_t neighbor, const void* data, uint16_t len,
                           uint32_t local_boot_epoch = 0, uint32_t peer_boot_epoch = 0, uint32_t peer_channel_epoch = 0)
        -> WkiChaosInterceptResult;
    auto claim_ready(WkiChaosDeliveryView* out) -> bool;
    void finish_delivery(size_t slot, int32_t transport_result = 0);
    void discard_transport(uintptr_t transport_cookie, uint16_t transport_id = 0);

    [[nodiscard]] auto counters() const -> const WkiChaosCounters&;
    [[nodiscard]] auto active_rule_count() const -> size_t;
    [[nodiscard]] auto queued_frame_count() const -> size_t;
    [[nodiscard]] auto queued_block_event_count() const -> size_t;
    [[nodiscard]] auto trace_count() const -> size_t;
    [[nodiscard]] auto trace_first_event_id() const -> uint64_t;
    [[nodiscard]] auto trace_last_event_id() const -> uint64_t;
    [[nodiscard]] auto queue_overflow() const -> bool;
    [[nodiscard]] auto trace_overflow() const -> bool;
    [[nodiscard]] auto stream_overflow() const -> bool;
    [[nodiscard]] auto invalid() const -> bool;
    [[nodiscard]] auto has_ready() const -> bool;
    auto rule_snapshot(WkiChaosRule* out, size_t max_rows) const -> size_t;
    auto trace_snapshot(WkiChaosTraceRow* out, size_t max_rows, uint64_t after_event_id) const -> size_t;

   private:
    enum class QueueState : uint8_t {
        FREE = 0,
        HELD = 1,
        READY = 2,
        IN_FLIGHT = 3,
    };

    enum class QueueFrameResult : uint8_t {
        QUEUED = 0,
        OVERFLOW = 1,
        INVALID_CORRUPTION = 2,
    };

    struct StreamRow {
        bool active = false;
        WkiChaosFrameKey key = {};
        uint64_t occurrence = 0;
    };

    struct BlockStreamRow {
        bool active = false;
        WkiChaosBlockKey key = {};
        uint64_t occurrence = 0;
    };

    struct QueuedFrame {
        QueueState state = QueueState::FREE;
        WkiChaosDirection direction = WkiChaosDirection::TX;
        WkiChaosAction action = WkiChaosAction::PASS;
        uintptr_t transport_cookie = 0;
        uint64_t ingress_cookie = 0;
        uint16_t neighbor = WKI_NODE_INVALID;
        uint16_t len = 0;
        uint32_t rule_id = 0;
        uint64_t queued_event_id = 0;
        uint64_t release_order = 0;
        uint64_t coalesced_count = 0;
        WkiChaosFrameKey key = {};
        WkiChaosCorruptionTrace corruption = {};
        std::array<uint8_t, WKI_CHAOS_MAX_FRAME_SIZE> frame = {};
    };

    struct QueuedBlockDoorbell {
        QueueState state = QueueState::FREE;
        WkiChaosAction action = WkiChaosAction::PASS;
        uint32_t rule_id = 0;
        uint64_t queued_event_id = 0;
        uint64_t release_order = 0;
        WkiChaosBlockKey key = {};
    };

    auto next_occurrence(WkiChaosFrameKey& key) -> bool;
    auto next_block_occurrence(WkiChaosBlockKey& key) -> bool;
    [[nodiscard]] auto rule_matches(const WkiChaosRule& rule, const WkiChaosFrameKey& key) const -> bool;
    [[nodiscard]] auto block_rule_matches(const WkiChaosRule& rule, const WkiChaosBlockKey& key) const -> bool;
    [[nodiscard]] auto chance_matches(const WkiChaosRule& rule, const WkiChaosFrameKey& key) const -> bool;
    [[nodiscard]] auto block_chance_matches(const WkiChaosRule& rule, const WkiChaosBlockKey& key) const -> bool;
    auto append_trace(const WkiChaosFrameKey& key, uint32_t rule_id, WkiChaosAction action, WkiChaosOutcome outcome,
                      uint64_t queued_event_id, uint64_t release_order, uint32_t checksum_before, uint32_t checksum_after,
                      uint32_t local_boot_epoch, uint32_t peer_boot_epoch, uint32_t peer_channel_epoch, int32_t transport_result = 0,
                      const WkiChaosCorruptionTrace& corruption = {}, uint64_t coalesced_count = 0) -> uint64_t;
    auto append_block_trace(const WkiChaosBlockKey& key, uint32_t rule_id, WkiChaosAction action, WkiChaosOutcome outcome,
                            uint64_t queued_event_id, uint64_t release_order, int32_t transport_result = 0,
                            const WkiChaosBlockCorruptionTrace& corruption = {}) -> uint64_t;
    auto queue_frame(const WkiChaosFrameKey& key, uintptr_t transport_cookie, uint64_t ingress_cookie, uint16_t neighbor, const void* data,
                     uint16_t len, const WkiChaosRule& rule, uint64_t queued_event_id, uint32_t* checksum_before, uint32_t* checksum_after,
                     WkiChaosCorruptionTrace* corruption) -> QueueFrameResult;
    auto coalesce_held_frame(const WkiChaosFrameKey& key, uintptr_t transport_cookie, uint64_t ingress_cookie, uint16_t neighbor,
                             const void* data, uint16_t len, WkiChaosInterceptResult* result) -> bool;

    bool enabled_ = false;
    uint64_t seed_ = 0;
    uint64_t next_event_id_ = 1;
    uint64_t next_release_order_ = 1;
    std::array<WkiChaosRule, WKI_CHAOS_MAX_RULES> rules_ = {};
    std::array<StreamRow, WKI_CHAOS_MAX_STREAMS> streams_ = {};
    std::array<BlockStreamRow, WKI_CHAOS_MAX_STREAMS> block_streams_ = {};
    std::array<QueuedFrame, WKI_CHAOS_MAX_QUEUED_FRAMES> queued_ = {};
    std::array<QueuedBlockDoorbell, WKI_CHAOS_MAX_QUEUED_BLOCK_EVENTS> queued_block_ = {};
    std::array<WkiChaosTraceRow, WKI_CHAOS_MAX_TRACE_ROWS> trace_ = {};
    size_t trace_count_ = 0;
    WkiChaosCounters counters_ = {};
    bool queue_overflow_ = false;
    bool trace_overflow_ = false;
    bool stream_overflow_ = false;
    bool invalid_ = false;
};

}  // namespace ker::net::wki
