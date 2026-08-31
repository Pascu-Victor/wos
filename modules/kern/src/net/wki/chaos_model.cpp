#include "chaos_model.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/wire.hpp>

namespace ker::net::wki {

namespace {

constexpr uint64_t FNV64_OFFSET = 14695981039346656037ULL;
constexpr uint64_t FNV64_PRIME = 1099511628211ULL;
constexpr std::array<uint32_t, 256> CRC32_TABLE = [] {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < table.size(); ++i) {
        uint32_t crc = i;
        for (uint8_t bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) != 0 ? (crc >> 1U) ^ 0xEDB88320U : crc >> 1U;
        }
        table.at(i) = crc;
    }
    return table;
}();

auto hash_byte(uint64_t hash, uint8_t value) -> uint64_t { return (hash ^ value) * FNV64_PRIME; }

template <typename T>
auto hash_value(uint64_t hash, T value) -> uint64_t {
    auto raw = static_cast<uint64_t>(value);
    for (size_t i = 0; i < sizeof(value); ++i) {
        hash = hash_byte(hash, static_cast<uint8_t>(raw & 0xFFU));
        raw >>= 8U;
    }
    return hash;
}

auto stable_key_hash(const WkiChaosFrameKey& key) -> uint64_t {
    uint64_t hash = FNV64_OFFSET;
    hash = hash_value(hash, static_cast<uint8_t>(key.direction));
    hash = hash_value(hash, key.transport_id);
    hash = hash_value(hash, key.neighbor);
    hash = hash_value(hash, key.src_node);
    hash = hash_value(hash, key.dst_node);
    hash = hash_value(hash, key.channel_id);
    hash = hash_value(hash, key.msg_type);
    hash = hash_value(hash, key.seq_num);
    hash = hash_value(hash, key.ack_num);
    hash = hash_value(hash, key.frame_len);
    return hash;
}

auto stream_key_hash(const WkiChaosFrameKey& key) -> uint64_t {
    uint64_t hash = FNV64_OFFSET;
    hash = hash_value(hash, static_cast<uint8_t>(key.direction));
    hash = hash_value(hash, key.transport_id);
    hash = hash_value(hash, key.neighbor);
    hash = hash_value(hash, key.src_node);
    hash = hash_value(hash, key.dst_node);
    hash = hash_value(hash, key.channel_id);
    hash = hash_value(hash, key.msg_type);
    return hash;
}

auto stable_block_key_hash(const WkiChaosBlockKey& key) -> uint64_t {
    uint64_t hash = FNV64_OFFSET;
    hash = hash_value(hash, static_cast<uint8_t>(key.surface));
    hash = hash_value(hash, static_cast<uint8_t>(key.direction));
    hash = hash_value(hash, static_cast<uint8_t>(key.origin));
    hash = hash_value(hash, static_cast<uint8_t>(key.lane));
    hash = hash_value(hash, key.transport_id);
    hash = hash_value(hash, key.neighbor);
    hash = hash_value(hash, key.zone_id);
    hash = hash_value(hash, key.resource_id);
    hash = hash_value(hash, key.ring_generation);
    hash = hash_value(hash, key.ring_index);
    hash = hash_value(hash, key.operation_cookie);
    hash = hash_value(hash, key.block_opcode);
    hash = hash_value(hash, key.attach_cookie);
    hash = hash_value(hash, key.channel_generation);
    hash = hash_value(hash, key.owner_boot_epoch);
    hash = hash_value(hash, key.resource_incarnation);
    return hash;
}

auto block_stream_key_hash(const WkiChaosBlockKey& key) -> uint64_t {
    uint64_t hash = FNV64_OFFSET;
    hash = hash_value(hash, static_cast<uint8_t>(key.surface));
    hash = hash_value(hash, static_cast<uint8_t>(key.direction));
    hash = hash_value(hash, static_cast<uint8_t>(key.origin));
    hash = hash_value(hash, static_cast<uint8_t>(key.lane));
    hash = hash_value(hash, key.transport_id);
    hash = hash_value(hash, key.neighbor);
    hash = hash_value(hash, key.zone_id);
    hash = hash_value(hash, key.resource_id);
    hash = hash_value(hash, key.ring_generation);
    hash = hash_value(hash, key.block_opcode);
    return hash;
}

auto mix64(uint64_t value) -> uint64_t {
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return value;
}

auto stream_keys_equal(const WkiChaosFrameKey& lhs, const WkiChaosFrameKey& rhs) -> bool {
    return lhs.direction == rhs.direction && lhs.transport_id == rhs.transport_id && lhs.neighbor == rhs.neighbor &&
           lhs.src_node == rhs.src_node && lhs.dst_node == rhs.dst_node && lhs.channel_id == rhs.channel_id && lhs.msg_type == rhs.msg_type;
}

auto block_stream_keys_equal(const WkiChaosBlockKey& lhs, const WkiChaosBlockKey& rhs) -> bool {
    return lhs.surface == rhs.surface && lhs.direction == rhs.direction && lhs.origin == rhs.origin && lhs.lane == rhs.lane &&
           lhs.transport_id == rhs.transport_id && lhs.neighbor == rhs.neighbor && lhs.zone_id == rhs.zone_id &&
           lhs.resource_id == rhs.resource_id && lhs.ring_generation == rhs.ring_generation && lhs.block_opcode == rhs.block_opcode;
}

// Reliability is a message-class property in WKI rather than a header flag.
// Keep this list aligned with the reliable dispatch cases in wki.cpp. Unknown
// types fail closed for retransmit coalescing.
auto reliable_message_type(uint8_t raw_type) -> bool {
    switch (static_cast<MsgType>(raw_type)) {
        case MsgType::HELLO:
        case MsgType::HELLO_ACK:
        case MsgType::HEARTBEAT:
        case MsgType::HEARTBEAT_ACK:
        case MsgType::PEER_GOODBYE:
            return false;
        case MsgType::LSA:
        case MsgType::LSA_ACK:
        case MsgType::FENCE_NOTIFY:
        case MsgType::RECONCILE_REQ:
        case MsgType::RECONCILE_ACK:
        case MsgType::RESOURCE_ADVERT:
        case MsgType::RESOURCE_WITHDRAW:
        case MsgType::ZONE_CREATE_REQ:
        case MsgType::ZONE_CREATE_ACK:
        case MsgType::ZONE_DESTROY:
        case MsgType::ZONE_NOTIFY_PRE:
        case MsgType::ZONE_NOTIFY_POST:
        case MsgType::ZONE_READ_REQ:
        case MsgType::ZONE_READ_RESP:
        case MsgType::ZONE_WRITE_REQ:
        case MsgType::ZONE_WRITE_ACK:
        case MsgType::ZONE_NOTIFY_PRE_ACK:
        case MsgType::ZONE_NOTIFY_POST_ACK:
        case MsgType::EVENT_SUBSCRIBE:
        case MsgType::EVENT_UNSUBSCRIBE:
        case MsgType::EVENT_PUBLISH:
        case MsgType::EVENT_ACK:
        case MsgType::DEV_ATTACH_REQ:
        case MsgType::DEV_ATTACH_ACK:
        case MsgType::DEV_DETACH:
        case MsgType::DEV_OP_REQ:
        case MsgType::DEV_OP_RESP:
        case MsgType::DEV_IRQ_FWD:
        case MsgType::CHANNEL_OPEN:
        case MsgType::CHANNEL_OPEN_ACK:
        case MsgType::CHANNEL_CLOSE:
        case MsgType::TASK_SUBMIT:
        case MsgType::TASK_ACCEPT:
        case MsgType::TASK_REJECT:
        case MsgType::TASK_COMPLETE:
        case MsgType::TASK_CANCEL:
        case MsgType::LOAD_REPORT:
        case MsgType::TASK_SUBMIT_FRAGMENT:
            return true;
    }
    return false;
}

struct ParsedFrame {
    WkiHeader header = {};
    bool valid = false;
    bool dev_op_valid = false;
    uint16_t dev_op_id = 0;
};

auto parse_frame(const void* data, uint16_t len) -> ParsedFrame {
    ParsedFrame parsed = {};
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return parsed;
    }
    std::memcpy(&parsed.header, data, sizeof(parsed.header));
    size_t const FRAME_DATA_LEN = static_cast<size_t>(len) - WKI_HEADER_SIZE;
    if (wki_version(parsed.header.version_flags) != WKI_VERSION || parsed.header.payload_len > WKI_ETH_MAX_PAYLOAD ||
        parsed.header.payload_len > FRAME_DATA_LEN) {
        return parsed;
    }
    parsed.valid = true;

    const auto* payload = static_cast<const uint8_t*>(data) + WKI_HEADER_SIZE;
    if (parsed.header.msg_type == static_cast<uint8_t>(MsgType::DEV_OP_REQ) && parsed.header.payload_len >= sizeof(DevOpReqPayload)) {
        DevOpReqPayload request = {};
        std::memcpy(&request, payload, sizeof(request));
        parsed.dev_op_valid = static_cast<size_t>(sizeof(request)) + request.data_len == parsed.header.payload_len;
        parsed.dev_op_id = request.op_id;
    } else if (parsed.header.msg_type == static_cast<uint8_t>(MsgType::DEV_OP_RESP) &&
               parsed.header.payload_len >= sizeof(DevOpRespPayload)) {
        DevOpRespPayload response = {};
        std::memcpy(&response, payload, sizeof(response));
        parsed.dev_op_valid = static_cast<size_t>(sizeof(response)) + response.data_len == parsed.header.payload_len;
        parsed.dev_op_id = response.op_id;
    }
    return parsed;
}

auto frame_key(WkiChaosDirection direction, uint16_t transport_id, uint16_t neighbor, const void* data, uint16_t len) -> WkiChaosFrameKey {
    WkiChaosFrameKey key = {};
    key.direction = direction;
    key.transport_id = transport_id;
    key.neighbor = neighbor;
    key.frame_len = len;
    if (data != nullptr && len >= WKI_HEADER_SIZE) {
        ParsedFrame const PARSED = parse_frame(data, len);
        key.src_node = PARSED.header.src_node;
        key.dst_node = PARSED.header.dst_node;
        key.channel_id = PARSED.header.channel_id;
        key.msg_type = PARSED.header.msg_type;
        key.seq_num = PARSED.header.seq_num;
        key.ack_num = PARSED.header.ack_num;
        key.payload_len = PARSED.header.payload_len;
        key.frame_valid = PARSED.valid;
        key.dev_op_valid = PARSED.dev_op_valid;
        key.dev_op_id = PARSED.dev_op_id;
    }
    key.stable_hash = stable_key_hash(key);
    return key;
}

auto frame_checksum(const void* data, uint16_t len) -> uint32_t {
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return 0;
    }
    WkiHeader header = {};
    std::memcpy(&header, data, sizeof(header));
    return header.checksum;
}

auto crc32_continue(uint32_t crc, const uint8_t* data, size_t len) -> uint32_t {
    for (size_t i = 0; i < len; ++i) {
        crc = CRC32_TABLE.at((crc ^ data[i]) & 0xFFU) ^ (crc >> 8U);
    }
    return crc;
}

auto valid_frame_checksum(const uint8_t* data, uint16_t len, const WkiHeader& header) -> uint32_t {
    WkiHeader clean = header;
    clean.checksum = 0;
    uint32_t crc = crc32_continue(UINT32_MAX, reinterpret_cast<const uint8_t*>(&clean), sizeof(clean));
    if (header.payload_len > 0 && static_cast<size_t>(WKI_HEADER_SIZE) + header.payload_len <= len) {
        crc = crc32_continue(crc, data + WKI_HEADER_SIZE, header.payload_len);
    }
    return crc ^ UINT32_MAX;
}

void corrupt_frame_checksum(uint8_t* data, uint16_t len) {
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return;
    }
    WkiHeader header = {};
    std::memcpy(&header, data, sizeof(header));
    // Force core validation to reject the frame even for a direct path whose
    // normal WKI checksum is zero. The wire layout and original retransmit copy
    // remain unchanged.
    uint32_t const VALID_CHECKSUM = valid_frame_checksum(data, len, header);
    header.checksum = VALID_CHECKSUM == UINT32_MAX ? 1U : VALID_CHECKSUM + 1U;
    std::memcpy(data, &header, sizeof(header));
}

auto corrupt_frame_payload(uint8_t* data, uint16_t len, uint16_t payload_offset, uint8_t payload_xor, WkiChaosCorruptionTrace* trace)
    -> bool {
    if (trace == nullptr || payload_xor == 0) {
        return false;
    }
    ParsedFrame const PARSED = parse_frame(data, len);
    if (!PARSED.valid || payload_offset >= PARSED.header.payload_len) {
        return false;
    }

    auto* payload_byte = data + WKI_HEADER_SIZE + payload_offset;
    trace->mode = WkiChaosCorruptMode::PAYLOAD_XOR;
    trace->payload_offset = payload_offset;
    trace->payload_xor = payload_xor;
    trace->byte_before = *payload_byte;
    *payload_byte ^= payload_xor;
    trace->byte_after = *payload_byte;
    trace->payload_changed = true;

    WkiHeader header = PARSED.header;
    header.checksum = valid_frame_checksum(data, len, header);
    if (header.checksum == 0) {
        *payload_byte ^= payload_xor;
        *trace = {};
        return false;
    }
    std::memcpy(data, &header, sizeof(header));
    return true;
}

}  // namespace

auto WkiChaosModel::clear() -> bool {
    if (std::ranges::any_of(queued_, [](const QueuedFrame& frame) { return frame.state == QueueState::IN_FLIGHT; }) ||
        std::ranges::any_of(queued_block_, [](const QueuedBlockDoorbell& event) { return event.state == QueueState::IN_FLIGHT; })) {
        return false;
    }
    enabled_ = false;
    seed_ = 0;
    next_event_id_ = 1;
    next_release_order_ = 1;
    rules_ = {};
    streams_ = {};
    block_streams_ = {};
    queued_ = {};
    queued_block_ = {};
    trace_ = {};
    trace_count_ = 0;
    counters_ = {};
    queue_overflow_ = false;
    trace_overflow_ = false;
    stream_overflow_ = false;
    invalid_ = false;
    return true;
}

void WkiChaosModel::enable(uint64_t seed) {
    seed_ = seed;
    enabled_ = true;
}

void WkiChaosModel::disable() { enabled_ = false; }

auto WkiChaosModel::enabled() const -> bool { return enabled_; }

auto WkiChaosModel::seed() const -> uint64_t { return seed_; }

auto WkiChaosModel::upsert_rule(const WkiChaosRule& rule) -> bool {
    if (rule.id == 0 || rule.action == WkiChaosAction::RELEASE || rule.every == 0 || rule.chance_permyriad > 10'000) {
        return false;
    }
    bool const VALID_SURFACE = rule.surface == WkiChaosSurface::FRAME || rule.surface == WkiChaosSurface::BLOCK_DOORBELL ||
                               rule.surface == WkiChaosSurface::BLOCK_SQE;
    bool const HAS_BLOCK_SELECTOR = rule.match_block_origin || rule.match_block_lane || rule.match_zone || rule.match_resource ||
                                    rule.match_ring_generation || rule.match_ring_index || rule.match_cookie || rule.match_block_opcode;
    bool const HAS_FRAME_SELECTOR =
        rule.match_src || rule.match_dst || rule.match_channel || rule.match_type || rule.match_op || rule.match_seq;
    bool const HAS_FRAME_CORRUPTION =
        rule.corrupt_mode != WkiChaosCorruptMode::CHECKSUM || rule.payload_offset != 0 || rule.payload_xor != 0;
    bool const HAS_SQE_CORRUPTION = rule.sqe_offset != 0 || rule.sqe_xor != 0;
    bool valid = VALID_SURFACE;
    if (rule.surface == WkiChaosSurface::FRAME) {
        bool const DEV_OP_TYPE = !rule.match_type || rule.msg_type == static_cast<uint8_t>(MsgType::DEV_OP_REQ) ||
                                 rule.msg_type == static_cast<uint8_t>(MsgType::DEV_OP_RESP);
        valid = valid && !HAS_BLOCK_SELECTOR && !HAS_SQE_CORRUPTION && (!rule.match_op || DEV_OP_TYPE) &&
                (rule.corrupt_mode == WkiChaosCorruptMode::CHECKSUM || rule.corrupt_mode == WkiChaosCorruptMode::PAYLOAD_XOR) &&
                (rule.action == WkiChaosAction::CORRUPT || !HAS_FRAME_CORRUPTION) &&
                (rule.action != WkiChaosAction::CORRUPT || rule.corrupt_mode != WkiChaosCorruptMode::CHECKSUM || !HAS_FRAME_CORRUPTION) &&
                (rule.action != WkiChaosAction::CORRUPT || rule.corrupt_mode != WkiChaosCorruptMode::PAYLOAD_XOR || rule.payload_xor != 0);
    } else if (rule.surface == WkiChaosSurface::BLOCK_DOORBELL) {
        bool const VALID_ACTION = rule.action == WkiChaosAction::PASS || rule.action == WkiChaosAction::DROP ||
                                  rule.action == WkiChaosAction::DUPLICATE || rule.action == WkiChaosAction::DELAY;
        valid = valid && rule.direction == WkiChaosDirection::TX && !HAS_FRAME_SELECTOR && !HAS_FRAME_CORRUPTION && !HAS_SQE_CORRUPTION &&
                VALID_ACTION;
    } else if (rule.surface == WkiChaosSurface::BLOCK_SQE) {
        valid = valid && rule.direction == WkiChaosDirection::TX && !HAS_FRAME_SELECTOR && !HAS_FRAME_CORRUPTION &&
                rule.action == WkiChaosAction::CORRUPT && rule.sqe_offset < WKI_CHAOS_BLOCK_SQE_BYTES && rule.sqe_xor != 0;
    }
    if (!valid) {
        return false;
    }
    for (auto& row : rules_) {
        if (row.active && row.id == rule.id) {
            bool const FRAME_QUEUED = std::ranges::any_of(
                queued_, [&rule](const QueuedFrame& frame) { return frame.state != QueueState::FREE && frame.rule_id == rule.id; });
            bool const BLOCK_QUEUED = std::ranges::any_of(queued_block_, [&rule](const QueuedBlockDoorbell& event) {
                return event.state != QueueState::FREE && event.rule_id == rule.id;
            });
            if (FRAME_QUEUED || BLOCK_QUEUED) {
                // A queue record owns the rule identity until release, heal,
                // transport discard, or clear. Rebinding that ID would make
                // release semantics and applied evidence ambiguous.
                return false;
            }
            row = rule;
            row.active = true;
            row.applied = 0;
            return true;
        }
    }
    for (auto& row : rules_) {
        if (!row.active) {
            row = rule;
            row.active = true;
            row.applied = 0;
            return true;
        }
    }
    return false;
}

auto WkiChaosModel::heal_partition(uint32_t rule_id) -> bool {
    for (auto& rule : rules_) {
        if (rule.active && rule.id == rule_id && rule.action == WkiChaosAction::PARTITION) {
            rule.active = false;
            for (auto& frame : queued_) {
                if (frame.state == QueueState::FREE || frame.rule_id != rule_id || frame.action != WkiChaosAction::PARTITION) {
                    continue;
                }
                static_cast<void>(append_trace(frame.key, frame.rule_id, WkiChaosAction::PARTITION, WkiChaosOutcome::HEALED,
                                               frame.queued_event_id, 0, frame_checksum(frame.frame.data(), frame.len),
                                               frame_checksum(frame.frame.data(), frame.len), 0, 0, 0, 0, frame.corruption,
                                               frame.coalesced_count));
                frame = {};
            }
            return true;
        }
    }
    return false;
}

auto WkiChaosModel::release(uint32_t rule_id, uint32_t count) -> size_t {
    if (count == 0) {
        count = UINT32_MAX;
    }
    size_t released = 0;
    while (released < count) {
        QueuedFrame* frame_candidate = nullptr;
        for (auto& frame : queued_) {
            if (frame.state != QueueState::HELD || frame.action == WkiChaosAction::PARTITION ||
                (rule_id != 0 && frame.rule_id != rule_id)) {
                continue;
            }
            bool const REVERSE = frame.action == WkiChaosAction::REORDER;
            if (frame_candidate == nullptr || (REVERSE && frame.queued_event_id > frame_candidate->queued_event_id) ||
                (!REVERSE && frame.queued_event_id < frame_candidate->queued_event_id)) {
                frame_candidate = &frame;
            }
        }
        QueuedBlockDoorbell* block_candidate = nullptr;
        for (auto& event : queued_block_) {
            if (event.state != QueueState::HELD || (rule_id != 0 && event.rule_id != rule_id)) {
                continue;
            }
            if (block_candidate == nullptr || event.queued_event_id < block_candidate->queued_event_id) {
                block_candidate = &event;
            }
        }
        if (frame_candidate == nullptr && block_candidate == nullptr) {
            break;
        }
        bool const RELEASE_BLOCK = block_candidate != nullptr &&
                                   (frame_candidate == nullptr || block_candidate->queued_event_id < frame_candidate->queued_event_id);
        if (RELEASE_BLOCK) {
            block_candidate->state = QueueState::READY;
            block_candidate->release_order = next_release_order_++;
        } else {
            frame_candidate->state = QueueState::READY;
            frame_candidate->release_order = next_release_order_++;
        }
        ++released;
    }
    return released;
}

auto WkiChaosModel::next_occurrence(WkiChaosFrameKey& key) -> bool {
    auto const START = static_cast<size_t>(stream_key_hash(key) % streams_.size());
    for (size_t probe = 0; probe < streams_.size(); ++probe) {
        auto& row = streams_.at((START + probe) % streams_.size());
        if (row.active && stream_keys_equal(row.key, key)) {
            key.stream_occurrence = row.occurrence++;
            return true;
        }
        if (!row.active) {
            row.active = true;
            row.key = key;
            row.occurrence = 1;
            key.stream_occurrence = 0;
            return true;
        }
    }
    stream_overflow_ = true;
    invalid_ = true;
    return false;
}

auto WkiChaosModel::next_block_occurrence(WkiChaosBlockKey& key) -> bool {
    key.stable_hash = stable_block_key_hash(key);
    auto const START = static_cast<size_t>(block_stream_key_hash(key) % block_streams_.size());
    for (size_t probe = 0; probe < block_streams_.size(); ++probe) {
        auto& row = block_streams_.at((START + probe) % block_streams_.size());
        if (row.active && block_stream_keys_equal(row.key, key)) {
            key.stream_occurrence = row.occurrence++;
            return true;
        }
        if (!row.active) {
            row.active = true;
            row.key = key;
            row.occurrence = 1;
            key.stream_occurrence = 0;
            return true;
        }
    }
    stream_overflow_ = true;
    invalid_ = true;
    return false;
}

auto WkiChaosModel::rule_matches(const WkiChaosRule& rule, const WkiChaosFrameKey& key) const -> bool {
    if (!rule.active || rule.surface != WkiChaosSurface::FRAME || rule.direction != key.direction ||
        (rule.limit != 0 && rule.applied >= rule.limit)) {
        return false;
    }
    if ((rule.match_transport && rule.transport_id != key.transport_id) || (rule.match_neighbor && rule.neighbor != key.neighbor) ||
        (rule.match_src && rule.src_node != key.src_node) || (rule.match_dst && rule.dst_node != key.dst_node) ||
        (rule.match_channel && rule.channel_id != key.channel_id) || (rule.match_type && rule.msg_type != key.msg_type) ||
        (rule.match_op && (!key.dev_op_valid || rule.op_id != key.dev_op_id)) ||
        (rule.action == WkiChaosAction::CORRUPT && rule.corrupt_mode == WkiChaosCorruptMode::PAYLOAD_XOR &&
         (!key.frame_valid || rule.payload_offset >= key.payload_len)) ||
        (rule.match_seq && rule.seq_num != key.seq_num) || (rule.match_occurrence && rule.occurrence != key.stream_occurrence) ||
        key.stream_occurrence < rule.after || ((key.stream_occurrence - rule.after) % rule.every) != 0) {
        return false;
    }
    return chance_matches(rule, key);
}

auto WkiChaosModel::block_rule_matches(const WkiChaosRule& rule, const WkiChaosBlockKey& key) const -> bool {
    if (!rule.active || rule.surface != key.surface || rule.direction != key.direction || (rule.limit != 0 && rule.applied >= rule.limit)) {
        return false;
    }
    if ((rule.match_transport && rule.transport_id != key.transport_id) || (rule.match_neighbor && rule.neighbor != key.neighbor) ||
        (rule.match_block_origin && rule.block_origin != key.origin) || (rule.match_block_lane && rule.block_lane != key.lane) ||
        (rule.match_zone && rule.zone_id != key.zone_id) || (rule.match_resource && rule.resource_id != key.resource_id) ||
        (rule.match_ring_generation && rule.ring_generation != key.ring_generation) ||
        (rule.match_ring_index && rule.ring_index != key.ring_index) ||
        (rule.match_cookie && rule.operation_cookie != key.operation_cookie) ||
        (rule.match_block_opcode && rule.block_opcode != key.block_opcode) ||
        (key.surface == WkiChaosSurface::BLOCK_SQE && rule.sqe_offset >= key.descriptor_len) ||
        (rule.match_occurrence && rule.occurrence != key.stream_occurrence) || key.stream_occurrence < rule.after ||
        ((key.stream_occurrence - rule.after) % rule.every) != 0) {
        return false;
    }
    return block_chance_matches(rule, key);
}

auto WkiChaosModel::chance_matches(const WkiChaosRule& rule, const WkiChaosFrameKey& key) const -> bool {
    if (rule.chance_permyriad >= 10'000) {
        return true;
    }
    uint64_t value = seed_ ^ key.stable_hash ^ mix64(key.stream_occurrence) ^ (static_cast<uint64_t>(rule.id) << 32U);
    return (mix64(value) % 10'000U) < rule.chance_permyriad;
}

auto WkiChaosModel::block_chance_matches(const WkiChaosRule& rule, const WkiChaosBlockKey& key) const -> bool {
    if (rule.chance_permyriad >= 10'000) {
        return true;
    }
    uint64_t value = seed_ ^ key.stable_hash ^ mix64(key.stream_occurrence) ^ (static_cast<uint64_t>(rule.id) << 32U);
    return (mix64(value) % 10'000U) < rule.chance_permyriad;
}

auto WkiChaosModel::append_trace(const WkiChaosFrameKey& key, uint32_t rule_id, WkiChaosAction action, WkiChaosOutcome outcome,
                                 uint64_t queued_event_id, uint64_t release_order, uint32_t checksum_before, uint32_t checksum_after,
                                 uint32_t local_boot_epoch, uint32_t peer_boot_epoch, uint32_t peer_channel_epoch, int32_t transport_result,
                                 const WkiChaosCorruptionTrace& corruption, uint64_t coalesced_count) -> uint64_t {
    uint64_t const EVENT_ID = next_event_id_++;
    if (trace_count_ >= trace_.size()) {
        trace_overflow_ = true;
        invalid_ = true;
        return EVENT_ID;
    }
    trace_.at(trace_count_++) = WkiChaosTraceRow{
        .event_id = EVENT_ID,
        .release_order = release_order,
        .queued_event_id = queued_event_id,
        .key = key,
        .rule_id = rule_id,
        .action = action,
        .outcome = outcome,
        .checksum_before = checksum_before,
        .checksum_after = checksum_after,
        .local_boot_epoch = local_boot_epoch,
        .peer_boot_epoch = peer_boot_epoch,
        .peer_channel_epoch = peer_channel_epoch,
        .transport_result = transport_result,
        .coalesced_count = coalesced_count,
        .corruption = corruption,
    };
    return EVENT_ID;
}

auto WkiChaosModel::append_block_trace(const WkiChaosBlockKey& key, uint32_t rule_id, WkiChaosAction action, WkiChaosOutcome outcome,
                                       uint64_t queued_event_id, uint64_t release_order, int32_t transport_result,
                                       const WkiChaosBlockCorruptionTrace& corruption) -> uint64_t {
    uint64_t const EVENT_ID = next_event_id_++;
    if (trace_count_ >= trace_.size()) {
        trace_overflow_ = true;
        invalid_ = true;
        return EVENT_ID;
    }
    trace_.at(trace_count_++) = WkiChaosTraceRow{
        .event_id = EVENT_ID,
        .release_order = release_order,
        .queued_event_id = queued_event_id,
        .surface = key.surface,
        .block_key = key,
        .rule_id = rule_id,
        .action = action,
        .outcome = outcome,
        .transport_result = transport_result,
        .block_corruption = corruption,
    };
    return EVENT_ID;
}

auto WkiChaosModel::queue_frame(const WkiChaosFrameKey& key, uintptr_t transport_cookie, uint64_t ingress_cookie, uint16_t neighbor,
                                const void* data, uint16_t len, const WkiChaosRule& rule, uint64_t queued_event_id,
                                uint32_t* checksum_before, uint32_t* checksum_after, WkiChaosCorruptionTrace* corruption)
    -> QueueFrameResult {
    if (data == nullptr || len > WKI_CHAOS_MAX_FRAME_SIZE || checksum_before == nullptr || checksum_after == nullptr ||
        corruption == nullptr) {
        queue_overflow_ = true;
        invalid_ = true;
        return QueueFrameResult::OVERFLOW;
    }
    for (auto& frame : queued_) {
        if (frame.state != QueueState::FREE) {
            continue;
        }
        frame.state = QueueState::HELD;
        frame.direction = key.direction;
        frame.action = rule.action;
        frame.transport_cookie = transport_cookie;
        frame.ingress_cookie = ingress_cookie;
        frame.neighbor = neighbor;
        frame.len = len;
        frame.rule_id = rule.id;
        frame.queued_event_id = queued_event_id;
        frame.release_order = 0;
        frame.coalesced_count = 0;
        frame.key = key;
        std::memcpy(frame.frame.data(), data, len);
        *checksum_before = frame_checksum(frame.frame.data(), len);
        if (rule.action == WkiChaosAction::CORRUPT) {
            corruption->mode = rule.corrupt_mode;
            if (rule.corrupt_mode == WkiChaosCorruptMode::PAYLOAD_XOR) {
                if (!corrupt_frame_payload(frame.frame.data(), len, rule.payload_offset, rule.payload_xor, corruption)) {
                    frame = {};
                    invalid_ = true;
                    return QueueFrameResult::INVALID_CORRUPTION;
                }
            } else {
                corrupt_frame_checksum(frame.frame.data(), len);
            }
        }
        frame.corruption = *corruption;
        *checksum_after = frame_checksum(frame.frame.data(), len);
        return QueueFrameResult::QUEUED;
    }
    queue_overflow_ = true;
    invalid_ = true;
    return QueueFrameResult::OVERFLOW;
}

auto WkiChaosModel::coalesce_held_frame(const WkiChaosFrameKey& key, uintptr_t transport_cookie, uint64_t ingress_cookie, uint16_t neighbor,
                                        const void* data, uint16_t len, WkiChaosInterceptResult* result) -> bool {
    if (data == nullptr || result == nullptr || !key.frame_valid || !reliable_message_type(key.msg_type)) {
        return false;
    }
    for (auto& frame : queued_) {
        bool const SUPPRESSING_ACTION =
            frame.action == WkiChaosAction::DELAY || frame.action == WkiChaosAction::REORDER || frame.action == WkiChaosAction::PARTITION;
        if (frame.state == QueueState::FREE || !SUPPRESSING_ACTION || frame.direction != key.direction ||
            frame.transport_cookie != transport_cookie || frame.ingress_cookie != ingress_cookie || frame.neighbor != neighbor ||
            frame.len != len || frame.key.transport_id != key.transport_id || frame.key.src_node != key.src_node ||
            frame.key.dst_node != key.dst_node || frame.key.channel_id != key.channel_id || frame.key.msg_type != key.msg_type ||
            frame.key.seq_num != key.seq_num || std::memcmp(frame.frame.data(), data, len) != 0) {
            continue;
        }
        ++frame.coalesced_count;
        ++counters_.coalesced;
        result->action = frame.action;
        result->outcome = WkiChaosOutcome::COALESCED;
        result->rule_id = frame.rule_id;
        result->event_id = frame.queued_event_id;
        result->matched = true;
        return true;
    }
    return false;
}

auto WkiChaosModel::intercept(WkiChaosDirection direction, uint16_t transport_id, uintptr_t transport_cookie, uint16_t neighbor,
                              const void* data, uint16_t len, uint32_t local_boot_epoch, uint32_t peer_boot_epoch,
                              uint32_t peer_channel_epoch, uint64_t ingress_cookie) -> WkiChaosInterceptResult {
    WkiChaosInterceptResult result = {};
    if (!enabled_) {
        return result;
    }
    ++counters_.observed;
    WkiChaosFrameKey key = frame_key(direction, transport_id, neighbor, data, len);
    if (coalesce_held_frame(key, transport_cookie, ingress_cookie, neighbor, data, len, &result)) {
        return result;
    }
    if (!next_occurrence(key)) {
        result.outcome = WkiChaosOutcome::STREAM_OVERFLOW;
        result.event_id = append_trace(key, 0, WkiChaosAction::PASS, result.outcome, 0, 0, frame_checksum(data, len),
                                       frame_checksum(data, len), local_boot_epoch, peer_boot_epoch, peer_channel_epoch);
        return result;
    }

    WkiChaosRule* matched = nullptr;
    for (auto& rule : rules_) {
        if (rule_matches(rule, key)) {
            matched = &rule;
            break;
        }
    }
    if (matched == nullptr) {
        ++counters_.passed;
        return result;
    }

    ++matched->applied;
    ++counters_.matched;
    result.action = matched->action;
    result.rule_id = matched->id;
    result.matched = true;
    uint32_t checksum_before = frame_checksum(data, len);
    uint32_t checksum_after = checksum_before;
    uint64_t queued_event_id = 0;
    WkiChaosCorruptionTrace corruption = {};

    switch (matched->action) {
        case WkiChaosAction::PASS:
            ++counters_.passed;
            result.outcome = WkiChaosOutcome::PASSED;
            break;
        case WkiChaosAction::DROP:
            ++counters_.dropped;
            result.outcome = WkiChaosOutcome::DROPPED;
            break;
        case WkiChaosAction::PARTITION: {
            bool const NEEDS_RETRANSMIT_RECORD = key.frame_valid && reliable_message_type(key.msg_type);
            if (NEEDS_RETRANSMIT_RECORD) {
                uint64_t const QUEUED_EVENT_ID = next_event_id_;
                QueueFrameResult const QUEUED = queue_frame(key, transport_cookie, ingress_cookie, neighbor, data, len, *matched,
                                                            QUEUED_EVENT_ID, &checksum_before, &checksum_after, &corruption);
                if (QUEUED != QueueFrameResult::QUEUED) {
                    ++counters_.dropped;
                    result.action = WkiChaosAction::DROP;
                    result.outcome = WkiChaosOutcome::QUEUE_OVERFLOW;
                    break;
                }
                queued_event_id = QUEUED_EVENT_ID;
            }
            ++counters_.dropped;
            result.outcome = WkiChaosOutcome::DROPPED;
            break;
        }
        case WkiChaosAction::FAIL:
            ++counters_.failed;
            result.outcome = WkiChaosOutcome::FAILED;
            break;
        case WkiChaosAction::DUPLICATE:
        case WkiChaosAction::DELAY:
        case WkiChaosAction::REORDER:
        case WkiChaosAction::CORRUPT: {
            // Reserve the event id before queuing so explicit release has a
            // stable reference even when producers race on different CPUs.
            uint64_t const QUEUED_EVENT_ID = next_event_id_;
            QueueFrameResult const QUEUED = queue_frame(key, transport_cookie, ingress_cookie, neighbor, data, len, *matched,
                                                        QUEUED_EVENT_ID, &checksum_before, &checksum_after, &corruption);
            if (QUEUED != QueueFrameResult::QUEUED) {
                // Storage exhaustion and the impossible CRC32==0 payload case
                // share one fail-closed policy: suppress physical delivery.
                result.action = WkiChaosAction::DROP;
                ++counters_.dropped;
                result.outcome = QUEUED == QueueFrameResult::OVERFLOW ? WkiChaosOutcome::QUEUE_OVERFLOW : WkiChaosOutcome::FAILED;
                break;
            }
            queued_event_id = QUEUED_EVENT_ID;
            if (matched->action == WkiChaosAction::DUPLICATE || matched->action == WkiChaosAction::CORRUPT) {
                static_cast<void>(release(matched->id, 1));
                result.queued_ready = true;
            }
            if (matched->action == WkiChaosAction::DUPLICATE) {
                ++counters_.duplicated;
            } else if (matched->action == WkiChaosAction::DELAY) {
                ++counters_.delayed;
            } else if (matched->action == WkiChaosAction::REORDER) {
                ++counters_.reordered;
            } else {
                ++counters_.corrupted;
            }
            result.outcome = matched->action == WkiChaosAction::DUPLICATE ? WkiChaosOutcome::DUPLICATED : WkiChaosOutcome::QUEUED;
            break;
        }
        case WkiChaosAction::RELEASE:
            result.action = WkiChaosAction::PASS;
            result.outcome = WkiChaosOutcome::PASSED;
            break;
    }

    result.event_id = append_trace(key, matched->id, matched->action, result.outcome, queued_event_id, 0, checksum_before, checksum_after,
                                   local_boot_epoch, peer_boot_epoch, peer_channel_epoch, 0, corruption);
    return result;
}

auto WkiChaosModel::intercept_block_doorbell(WkiChaosBlockKey key) -> WkiChaosInterceptResult {
    WkiChaosInterceptResult result = {};
    if (!enabled_) {
        return result;
    }
    key.surface = WkiChaosSurface::BLOCK_DOORBELL;
    key.descriptor_len = 0;
    ++counters_.observed;
    if (!next_block_occurrence(key)) {
        result.outcome = WkiChaosOutcome::STREAM_OVERFLOW;
        result.event_id = append_block_trace(key, 0, WkiChaosAction::PASS, result.outcome, 0, 0);
        return result;
    }

    WkiChaosRule* matched = nullptr;
    for (auto& rule : rules_) {
        if (block_rule_matches(rule, key)) {
            matched = &rule;
            break;
        }
    }
    if (matched == nullptr) {
        ++counters_.passed;
        return result;
    }

    ++matched->applied;
    ++counters_.matched;
    result.action = matched->action;
    result.rule_id = matched->id;
    result.matched = true;
    uint64_t queued_event_id = 0;
    switch (matched->action) {
        case WkiChaosAction::PASS:
            ++counters_.passed;
            result.outcome = WkiChaosOutcome::PASSED;
            break;
        case WkiChaosAction::DROP:
            ++counters_.dropped;
            result.outcome = WkiChaosOutcome::DROPPED;
            break;
        case WkiChaosAction::DUPLICATE:
            ++counters_.duplicated;
            result.outcome = WkiChaosOutcome::DUPLICATED;
            break;
        case WkiChaosAction::DELAY: {
            QueuedBlockDoorbell* queued = nullptr;
            for (auto& event : queued_block_) {
                if (event.state == QueueState::FREE) {
                    queued = &event;
                    break;
                }
            }
            if (queued == nullptr) {
                queue_overflow_ = true;
                invalid_ = true;
                ++counters_.dropped;
                result.action = WkiChaosAction::DROP;
                result.outcome = WkiChaosOutcome::QUEUE_OVERFLOW;
                break;
            }
            queued_event_id = next_event_id_;
            *queued = QueuedBlockDoorbell{
                .state = QueueState::HELD,
                .action = matched->action,
                .rule_id = matched->id,
                .queued_event_id = queued_event_id,
                .key = key,
            };
            ++counters_.delayed;
            result.outcome = WkiChaosOutcome::QUEUED;
            break;
        }
        case WkiChaosAction::REORDER:
        case WkiChaosAction::CORRUPT:
        case WkiChaosAction::FAIL:
        case WkiChaosAction::PARTITION:
        case WkiChaosAction::RELEASE:
            // upsert_rule() rejects these actions for the doorbell surface.
            ++counters_.failed;
            result.action = WkiChaosAction::DROP;
            result.outcome = WkiChaosOutcome::FAILED;
            break;
    }
    result.event_id = append_block_trace(key, matched->id, matched->action, result.outcome, queued_event_id, 0);
    return result;
}

auto WkiChaosModel::intercept_block_sqe(WkiChaosBlockKey key, void* descriptor, uint16_t descriptor_len) -> WkiChaosInterceptResult {
    WkiChaosInterceptResult result = {};
    if (!enabled_) {
        return result;
    }
    key.surface = WkiChaosSurface::BLOCK_SQE;
    key.descriptor_len = descriptor_len;
    ++counters_.observed;
    if (!next_block_occurrence(key)) {
        result.outcome = WkiChaosOutcome::STREAM_OVERFLOW;
        result.event_id = append_block_trace(key, 0, WkiChaosAction::PASS, result.outcome, 0, 0);
        return result;
    }

    WkiChaosRule* matched = nullptr;
    for (auto& rule : rules_) {
        if (block_rule_matches(rule, key)) {
            matched = &rule;
            break;
        }
    }
    if (matched == nullptr) {
        ++counters_.passed;
        return result;
    }

    ++matched->applied;
    ++counters_.matched;
    result.action = matched->action;
    result.rule_id = matched->id;
    result.matched = true;
    WkiChaosBlockCorruptionTrace corruption = {
        .sqe_offset = matched->sqe_offset,
        .sqe_xor = matched->sqe_xor,
    };
    if (descriptor == nullptr || matched->sqe_offset >= descriptor_len || matched->sqe_xor == 0) {
        ++counters_.failed;
        result.action = WkiChaosAction::PASS;
        result.outcome = WkiChaosOutcome::FAILED;
    } else {
        auto* byte = static_cast<uint8_t*>(descriptor) + matched->sqe_offset;
        corruption.byte_before = *byte;
        *byte ^= matched->sqe_xor;
        corruption.byte_after = *byte;
        corruption.changed = true;
        ++counters_.corrupted;
        result.outcome = WkiChaosOutcome::CORRUPTED;
    }
    result.event_id = append_block_trace(key, matched->id, matched->action, result.outcome, 0, 0, 0, corruption);
    return result;
}

auto WkiChaosModel::transport_removed(WkiChaosDirection direction, uint16_t transport_id, uint16_t neighbor, const void* data, uint16_t len,
                                      uint32_t local_boot_epoch, uint32_t peer_boot_epoch, uint32_t peer_channel_epoch)
    -> WkiChaosInterceptResult {
    WkiChaosInterceptResult result = {};
    result.action = direction == WkiChaosDirection::TX ? WkiChaosAction::FAIL : WkiChaosAction::DROP;
    result.outcome = WkiChaosOutcome::TRANSPORT_REMOVED;
    if (!enabled_) {
        return result;
    }
    ++counters_.observed;
    WkiChaosFrameKey key = frame_key(direction, transport_id, neighbor, data, len);
    static_cast<void>(next_occurrence(key));
    if (direction == WkiChaosDirection::TX) {
        ++counters_.failed;
    } else {
        ++counters_.dropped;
    }
    result.event_id = append_trace(key, 0, result.action, result.outcome, 0, 0, frame_checksum(data, len), frame_checksum(data, len),
                                   local_boot_epoch, peer_boot_epoch, peer_channel_epoch, -1);
    return result;
}

auto WkiChaosModel::claim_ready(WkiChaosDeliveryView* out) -> bool {
    if (out == nullptr) {
        return false;
    }
    QueuedFrame* oldest = nullptr;
    size_t oldest_slot = queued_.size();
    for (size_t i = 0; i < queued_.size(); ++i) {
        auto& frame = queued_.at(i);
        if (frame.state != QueueState::READY) {
            continue;
        }
        if (oldest == nullptr || frame.release_order < oldest->release_order) {
            oldest = &frame;
            oldest_slot = i;
        }
    }
    QueuedBlockDoorbell* oldest_block = nullptr;
    size_t oldest_block_slot = queued_block_.size();
    for (size_t i = 0; i < queued_block_.size(); ++i) {
        auto& event = queued_block_.at(i);
        if (event.state != QueueState::READY) {
            continue;
        }
        if (oldest_block == nullptr || event.release_order < oldest_block->release_order) {
            oldest_block = &event;
            oldest_block_slot = i;
        }
    }
    if (oldest == nullptr && oldest_block == nullptr) {
        return false;
    }
    if (oldest_block != nullptr && (oldest == nullptr || oldest_block->release_order < oldest->release_order)) {
        oldest_block->state = QueueState::IN_FLIGHT;
        *out = WkiChaosDeliveryView{
            .slot = WKI_CHAOS_MAX_QUEUED_FRAMES + oldest_block_slot,
            .kind = WkiChaosDeliveryView::Kind::BLOCK_DOORBELL,
            .direction = oldest_block->key.direction,
            .action = oldest_block->action,
            .neighbor = oldest_block->key.neighbor,
            .block_key = oldest_block->key,
        };
        return true;
    }
    oldest->state = QueueState::IN_FLIGHT;
    *out = WkiChaosDeliveryView{
        .slot = oldest_slot,
        .kind = WkiChaosDeliveryView::Kind::FRAME,
        .direction = oldest->direction,
        .action = oldest->action,
        .transport_cookie = oldest->transport_cookie,
        .ingress_cookie = oldest->ingress_cookie,
        .neighbor = oldest->neighbor,
        .data = oldest->frame.data(),
        .len = oldest->len,
    };
    return true;
}

void WkiChaosModel::finish_delivery(size_t slot, int32_t transport_result) {
    if (slot >= WKI_CHAOS_MAX_QUEUED_FRAMES) {
        size_t const BLOCK_SLOT = slot - WKI_CHAOS_MAX_QUEUED_FRAMES;
        if (BLOCK_SLOT < queued_block_.size() && queued_block_.at(BLOCK_SLOT).state == QueueState::IN_FLIGHT) {
            auto& event = queued_block_.at(BLOCK_SLOT);
            ++counters_.released;
            WkiChaosOutcome const OUTCOME = transport_result < 0 ? WkiChaosOutcome::FAILED : WkiChaosOutcome::RELEASED;
            static_cast<void>(append_block_trace(event.key, event.rule_id, WkiChaosAction::RELEASE, OUTCOME, event.queued_event_id,
                                                 event.release_order, transport_result));
            event = {};
        }
        return;
    }
    if (slot < queued_.size() && queued_.at(slot).state == QueueState::IN_FLIGHT) {
        auto& frame = queued_.at(slot);
        ++counters_.released;
        WkiChaosOutcome const OUTCOME = transport_result < 0 ? WkiChaosOutcome::FAILED : WkiChaosOutcome::RELEASED;
        static_cast<void>(append_trace(frame.key, frame.rule_id, WkiChaosAction::RELEASE, OUTCOME, frame.queued_event_id,
                                       frame.release_order, frame_checksum(frame.frame.data(), frame.len),
                                       frame_checksum(frame.frame.data(), frame.len), 0, 0, 0, transport_result, frame.corruption,
                                       frame.coalesced_count));
        frame = {};
    }
}

void WkiChaosModel::discard_transport(uintptr_t transport_cookie, uint16_t transport_id) {
    for (auto& frame : queued_) {
        if (frame.state == QueueState::FREE || frame.transport_cookie != transport_cookie) {
            continue;
        }
        if (frame.state == QueueState::IN_FLIGHT) {
            // Runtime teardown holds a transport lease until this callback
            // finishes. The in-flight row is retired by finish_delivery().
            continue;
        }
        static_cast<void>(append_trace(frame.key, frame.rule_id, frame.action, WkiChaosOutcome::TRANSPORT_REMOVED, frame.queued_event_id,
                                       frame.release_order, frame_checksum(frame.frame.data(), frame.len),
                                       frame_checksum(frame.frame.data(), frame.len), 0, 0, 0, 0, frame.corruption, frame.coalesced_count));
        frame = {};
    }
    if (transport_id == 0) {
        return;
    }
    for (auto& event : queued_block_) {
        if (event.state == QueueState::FREE || event.key.transport_id != transport_id) {
            continue;
        }
        if (event.state == QueueState::IN_FLIGHT) {
            // Deferred block delivery owns no transport pointer, but its exact
            // binding retain remains responsible for completing the row.
            continue;
        }
        static_cast<void>(append_block_trace(event.key, event.rule_id, event.action, WkiChaosOutcome::TRANSPORT_REMOVED,
                                             event.queued_event_id, event.release_order));
        event = {};
    }
}

auto WkiChaosModel::counters() const -> const WkiChaosCounters& { return counters_; }

auto WkiChaosModel::active_rule_count() const -> size_t {
    return static_cast<size_t>(std::ranges::count_if(rules_, [](const WkiChaosRule& rule) { return rule.active; }));
}

auto WkiChaosModel::queued_frame_count() const -> size_t {
    return static_cast<size_t>(std::ranges::count_if(queued_, [](const QueuedFrame& frame) { return frame.state != QueueState::FREE; }));
}

auto WkiChaosModel::queued_block_event_count() const -> size_t {
    return static_cast<size_t>(
        std::ranges::count_if(queued_block_, [](const QueuedBlockDoorbell& event) { return event.state != QueueState::FREE; }));
}

auto WkiChaosModel::trace_count() const -> size_t { return trace_count_; }

auto WkiChaosModel::trace_first_event_id() const -> uint64_t { return trace_count_ == 0 ? 0 : trace_.at(0).event_id; }

auto WkiChaosModel::trace_last_event_id() const -> uint64_t { return trace_count_ == 0 ? 0 : trace_.at(trace_count_ - 1).event_id; }

auto WkiChaosModel::queue_overflow() const -> bool { return queue_overflow_; }

auto WkiChaosModel::trace_overflow() const -> bool { return trace_overflow_; }

auto WkiChaosModel::stream_overflow() const -> bool { return stream_overflow_; }

auto WkiChaosModel::invalid() const -> bool { return invalid_; }

auto WkiChaosModel::has_ready() const -> bool {
    return std::ranges::any_of(queued_, [](const QueuedFrame& frame) { return frame.state == QueueState::READY; }) ||
           std::ranges::any_of(queued_block_, [](const QueuedBlockDoorbell& event) { return event.state == QueueState::READY; });
}

auto WkiChaosModel::rule_snapshot(WkiChaosRule* out, size_t max_rows) const -> size_t {
    if (out == nullptr || max_rows == 0) {
        return 0;
    }
    size_t count = 0;
    for (const auto& rule : rules_) {
        if (rule.active && count < max_rows) {
            out[count++] = rule;
        }
    }
    return count;
}

auto WkiChaosModel::trace_snapshot(WkiChaosTraceRow* out, size_t max_rows, uint64_t after_event_id) const -> size_t {
    if (out == nullptr || max_rows == 0) {
        return 0;
    }
    size_t count = 0;
    for (size_t i = 0; i < trace_count_ && count < max_rows; ++i) {
        if (trace_.at(i).event_id > after_event_id) {
            out[count++] = trace_.at(i);
        }
    }
    return count;
}

}  // namespace ker::net::wki
