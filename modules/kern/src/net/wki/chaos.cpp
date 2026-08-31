#include "chaos.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/packet.hpp>
#include <net/wki/chaos_model.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

namespace {

constexpr size_t WKI_CHAOS_COMMAND_MAX = 512;
constexpr uint64_t WKI_CHAOS_INGRESS_MAC_PRESENT = 1ULL << 63U;

mod::sys::Spinlock s_chaos_lock;                                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
WkiChaosModel s_chaos;                                                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_chaos_enabled{false};                                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_runtime_control_allowed{false};                       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint16_t s_next_transport_id = 1;                                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<WkiTransport*, WKI_MAX_TRANSPORTS> s_registered_transports{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct ChaosPeerEpochRow {
    uint16_t node_id = WKI_NODE_INVALID;
    uint32_t remote_boot_epoch = 0;
    uint32_t remote_channel_epoch = 0;
};

std::array<ChaosPeerEpochRow, WKI_MAX_PEERS> s_peer_epochs;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

#ifdef WOS_SELFTEST
std::atomic<WkiChaosRxDeliveryHook> s_rx_delivery_hook{nullptr};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<WkiChaosBlockDoorbellDeliveryHook> s_block_doorbell_delivery_hook{
    nullptr};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
#endif

struct TokenCursor {
    char* cursor = nullptr;

    auto next() -> char* {
        while (cursor != nullptr && (*cursor == ' ' || *cursor == '\t' || *cursor == '\r' || *cursor == '\n')) {
            ++cursor;
        }
        if (cursor == nullptr || *cursor == '\0') {
            return nullptr;
        }
        char* token = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && *cursor != '\r' && *cursor != '\n') {
            ++cursor;
        }
        if (*cursor != '\0') {
            *cursor++ = '\0';
        }
        return token;
    }
};

auto parse_u64(const char* text, uint64_t* out) -> bool {
    if (text == nullptr || out == nullptr || *text == '\0') {
        return false;
    }
    uint32_t base = 10;
    if (text[0] == '0' && (text[1] == 'x' || text[1] == 'X')) {
        base = 16;
        text += 2;
        if (*text == '\0') {
            return false;
        }
    }
    uint64_t value = 0;
    for (; *text != '\0'; ++text) {
        uint8_t digit = 0;
        if (*text >= '0' && *text <= '9') {
            digit = static_cast<uint8_t>(*text - '0');
        } else if (base == 16 && *text >= 'a' && *text <= 'f') {
            digit = static_cast<uint8_t>(*text - 'a' + 10);
        } else if (base == 16 && *text >= 'A' && *text <= 'F') {
            digit = static_cast<uint8_t>(*text - 'A' + 10);
        } else {
            return false;
        }
        if (digit >= base || value > (UINT64_MAX - digit) / base) {
            return false;
        }
        value = (value * base) + digit;
    }
    *out = value;
    return true;
}

constexpr auto next_transport_id(uint16_t id) -> uint16_t { return id == UINT16_MAX ? 1 : static_cast<uint16_t>(id + 1U); }

template <typename T>
auto parse_unsigned(const char* text, T* out) -> bool {
    uint64_t value = 0;
    if (!parse_u64(text, &value) || value > static_cast<uint64_t>(static_cast<T>(-1))) {
        return false;
    }
    *out = static_cast<T>(value);
    return true;
}

auto split_key_value(char* token, char** key, char** value) -> bool {
    if (token == nullptr || key == nullptr || value == nullptr) {
        return false;
    }
    char* equals = token;
    while (*equals != '\0' && *equals != '=') {
        ++equals;
    }
    if (*equals != '=' || equals == token || equals[1] == '\0') {
        return false;
    }
    *equals = '\0';
    *key = token;
    *value = equals + 1;
    return true;
}

auto parse_direction(const char* value, WkiChaosDirection* out) -> bool {
    if (std::strcmp(value, "tx") == 0) {
        *out = WkiChaosDirection::TX;
        return true;
    }
    if (std::strcmp(value, "rx") == 0) {
        *out = WkiChaosDirection::RX;
        return true;
    }
    return false;
}

auto parse_surface(const char* value, WkiChaosSurface* out) -> bool {
    if (std::strcmp(value, "frame") == 0) {
        *out = WkiChaosSurface::FRAME;
        return true;
    }
    if (std::strcmp(value, "blk_doorbell") == 0) {
        *out = WkiChaosSurface::BLOCK_DOORBELL;
        return true;
    }
    if (std::strcmp(value, "blk_sqe") == 0) {
        *out = WkiChaosSurface::BLOCK_SQE;
        return true;
    }
    return false;
}

auto parse_block_origin(const char* value, bool* match, WkiChaosBlockOrigin* out) -> bool {
    if (std::strcmp(value, "*") == 0) {
        *match = false;
        return true;
    }
    *match = true;
    if (std::strcmp(value, "proxy") == 0) {
        *out = WkiChaosBlockOrigin::PROXY;
        return true;
    }
    if (std::strcmp(value, "server") == 0) {
        *out = WkiChaosBlockOrigin::SERVER;
        return true;
    }
    return false;
}

auto parse_block_lane(const char* value, bool* match, WkiChaosBlockLane* out) -> bool {
    if (std::strcmp(value, "*") == 0) {
        *match = false;
        return true;
    }
    *match = true;
    if (std::strcmp(value, "ivshmem") == 0) {
        *out = WkiChaosBlockLane::IVSHMEM;
        return true;
    }
    if (std::strcmp(value, "roce") == 0) {
        *out = WkiChaosBlockLane::ROCE;
        return true;
    }
    return false;
}

auto parse_action(const char* value, WkiChaosAction* out) -> bool {
    struct ActionName {
        const char* name;
        WkiChaosAction action;
    };
    constexpr std::array ACTIONS{
        ActionName{.name = "pass", .action = WkiChaosAction::PASS},
        ActionName{.name = "drop", .action = WkiChaosAction::DROP},
        ActionName{.name = "duplicate", .action = WkiChaosAction::DUPLICATE},
        ActionName{.name = "delay", .action = WkiChaosAction::DELAY},
        ActionName{.name = "reorder", .action = WkiChaosAction::REORDER},
        ActionName{.name = "corrupt", .action = WkiChaosAction::CORRUPT},
        ActionName{.name = "fail", .action = WkiChaosAction::FAIL},
        ActionName{.name = "partition", .action = WkiChaosAction::PARTITION},
    };
    const auto* const MATCH =
        std::ranges::find_if(ACTIONS, [value](const ActionName& candidate) { return std::strcmp(value, candidate.name) == 0; });
    if (MATCH == ACTIONS.end()) {
        return false;
    }
    *out = MATCH->action;
    return true;
}

auto parse_corrupt_mode(const char* value, WkiChaosCorruptMode* out) -> bool {
    if (std::strcmp(value, "checksum") == 0) {
        *out = WkiChaosCorruptMode::CHECKSUM;
        return true;
    }
    if (std::strcmp(value, "payload") == 0) {
        *out = WkiChaosCorruptMode::PAYLOAD_XOR;
        return true;
    }
    return false;
}

template <typename T>
auto parse_optional_match(const char* value, bool* match, T* out) -> bool {
    if (std::strcmp(value, "*") == 0) {
        *match = false;
        return true;
    }
    *match = true;
    return parse_unsigned(value, out);
}

enum class RuleField : uint8_t {
    ID,
    SURFACE,
    DIRECTION,
    ACTION,
    TRANSPORT,
    NEIGHBOR,
    SRC,
    DST,
    CHANNEL,
    TYPE,
    OP,
    ORIGIN,
    LANE,
    ZONE,
    RESOURCE,
    RING_GENERATION,
    RING_INDEX,
    COOKIE,
    BLOCK_OPCODE,
    SEQUENCE,
    OCCURRENCE,
    AFTER,
    EVERY,
    LIMIT,
    CHANCE,
    CORRUPT_MODE,
    PAYLOAD_OFFSET,
    PAYLOAD_XOR,
    SQE_OFFSET,
    SQE_XOR,
};

constexpr auto rule_field_mask(RuleField field) -> uint64_t { return 1ULL << static_cast<uint8_t>(field); }

auto claim_rule_field(uint64_t* seen, RuleField field) -> bool {
    uint64_t const MASK = rule_field_mask(field);
    if ((*seen & MASK) != 0) {
        return false;
    }
    *seen |= MASK;
    return true;
}

auto parse_rule(TokenCursor& tokens, WkiChaosRule* rule) -> bool {
    uint64_t seen = 0;
    bool have_id = false;
    bool have_direction = false;
    bool have_action = false;
    bool have_corrupt_mode = false;
    bool have_payload_offset = false;
    bool have_payload_xor = false;
    bool have_sqe_offset = false;
    bool have_sqe_xor = false;
    for (char* token = tokens.next(); token != nullptr; token = tokens.next()) {
        char* key = nullptr;
        char* value = nullptr;
        if (!split_key_value(token, &key, &value)) {
            return false;
        }
        if (std::strcmp(key, "id") == 0) {
            have_id = claim_rule_field(&seen, RuleField::ID) && parse_unsigned(value, &rule->id);
        } else if (std::strcmp(key, "surface") == 0) {
            if (!claim_rule_field(&seen, RuleField::SURFACE) || !parse_surface(value, &rule->surface)) {
                return false;
            }
        } else if (std::strcmp(key, "dir") == 0) {
            have_direction = claim_rule_field(&seen, RuleField::DIRECTION) && parse_direction(value, &rule->direction);
        } else if (std::strcmp(key, "action") == 0) {
            have_action = claim_rule_field(&seen, RuleField::ACTION) && parse_action(value, &rule->action);
        } else if (std::strcmp(key, "transport") == 0) {
            if (!claim_rule_field(&seen, RuleField::TRANSPORT) ||
                !parse_optional_match(value, &rule->match_transport, &rule->transport_id)) {
                return false;
            }
        } else if (std::strcmp(key, "neighbor") == 0) {
            if (!claim_rule_field(&seen, RuleField::NEIGHBOR) || !parse_optional_match(value, &rule->match_neighbor, &rule->neighbor)) {
                return false;
            }
        } else if (std::strcmp(key, "src") == 0) {
            if (!claim_rule_field(&seen, RuleField::SRC) || !parse_optional_match(value, &rule->match_src, &rule->src_node)) {
                return false;
            }
        } else if (std::strcmp(key, "dst") == 0) {
            if (!claim_rule_field(&seen, RuleField::DST) || !parse_optional_match(value, &rule->match_dst, &rule->dst_node)) {
                return false;
            }
        } else if (std::strcmp(key, "channel") == 0) {
            if (!claim_rule_field(&seen, RuleField::CHANNEL) || !parse_optional_match(value, &rule->match_channel, &rule->channel_id)) {
                return false;
            }
        } else if (std::strcmp(key, "type") == 0) {
            if (!claim_rule_field(&seen, RuleField::TYPE) || !parse_optional_match(value, &rule->match_type, &rule->msg_type)) {
                return false;
            }
        } else if (std::strcmp(key, "op") == 0) {
            if (!claim_rule_field(&seen, RuleField::OP) || !parse_optional_match(value, &rule->match_op, &rule->op_id)) {
                return false;
            }
        } else if (std::strcmp(key, "origin") == 0) {
            if (!claim_rule_field(&seen, RuleField::ORIGIN) || !parse_block_origin(value, &rule->match_block_origin, &rule->block_origin)) {
                return false;
            }
        } else if (std::strcmp(key, "lane") == 0) {
            if (!claim_rule_field(&seen, RuleField::LANE) || !parse_block_lane(value, &rule->match_block_lane, &rule->block_lane)) {
                return false;
            }
        } else if (std::strcmp(key, "zone") == 0) {
            if (!claim_rule_field(&seen, RuleField::ZONE) || !parse_optional_match(value, &rule->match_zone, &rule->zone_id)) {
                return false;
            }
        } else if (std::strcmp(key, "resource") == 0) {
            if (!claim_rule_field(&seen, RuleField::RESOURCE) || !parse_optional_match(value, &rule->match_resource, &rule->resource_id)) {
                return false;
            }
        } else if (std::strcmp(key, "ring_generation") == 0) {
            if (!claim_rule_field(&seen, RuleField::RING_GENERATION) ||
                !parse_optional_match(value, &rule->match_ring_generation, &rule->ring_generation)) {
                return false;
            }
        } else if (std::strcmp(key, "ring_index") == 0) {
            if (!claim_rule_field(&seen, RuleField::RING_INDEX) ||
                !parse_optional_match(value, &rule->match_ring_index, &rule->ring_index)) {
                return false;
            }
        } else if (std::strcmp(key, "cookie") == 0) {
            if (!claim_rule_field(&seen, RuleField::COOKIE) || !parse_optional_match(value, &rule->match_cookie, &rule->operation_cookie)) {
                return false;
            }
        } else if (std::strcmp(key, "blk_op") == 0) {
            if (!claim_rule_field(&seen, RuleField::BLOCK_OPCODE) ||
                !parse_optional_match(value, &rule->match_block_opcode, &rule->block_opcode)) {
                return false;
            }
        } else if (std::strcmp(key, "seq") == 0) {
            if (!claim_rule_field(&seen, RuleField::SEQUENCE) || !parse_optional_match(value, &rule->match_seq, &rule->seq_num)) {
                return false;
            }
        } else if (std::strcmp(key, "occurrence") == 0) {
            if (!claim_rule_field(&seen, RuleField::OCCURRENCE) ||
                !parse_optional_match(value, &rule->match_occurrence, &rule->occurrence)) {
                return false;
            }
        } else if (std::strcmp(key, "after") == 0) {
            if (!claim_rule_field(&seen, RuleField::AFTER) || !parse_u64(value, &rule->after)) {
                return false;
            }
        } else if (std::strcmp(key, "every") == 0) {
            if (!claim_rule_field(&seen, RuleField::EVERY) || !parse_u64(value, &rule->every) || rule->every == 0) {
                return false;
            }
        } else if (std::strcmp(key, "limit") == 0) {
            if (!claim_rule_field(&seen, RuleField::LIMIT) || !parse_u64(value, &rule->limit)) {
                return false;
            }
        } else if (std::strcmp(key, "chance") == 0) {
            if (!claim_rule_field(&seen, RuleField::CHANCE) || !parse_unsigned(value, &rule->chance_permyriad) ||
                rule->chance_permyriad > 10'000) {
                return false;
            }
        } else if (std::strcmp(key, "corrupt") == 0) {
            if (!claim_rule_field(&seen, RuleField::CORRUPT_MODE) || !parse_corrupt_mode(value, &rule->corrupt_mode)) {
                return false;
            }
            have_corrupt_mode = true;
        } else if (std::strcmp(key, "payload_offset") == 0) {
            if (!claim_rule_field(&seen, RuleField::PAYLOAD_OFFSET) || !parse_unsigned(value, &rule->payload_offset)) {
                return false;
            }
            have_payload_offset = true;
        } else if (std::strcmp(key, "payload_xor") == 0) {
            if (!claim_rule_field(&seen, RuleField::PAYLOAD_XOR) || !parse_unsigned(value, &rule->payload_xor) || rule->payload_xor == 0) {
                return false;
            }
            have_payload_xor = true;
        } else if (std::strcmp(key, "sqe_offset") == 0) {
            if (!claim_rule_field(&seen, RuleField::SQE_OFFSET) || !parse_unsigned(value, &rule->sqe_offset)) {
                return false;
            }
            have_sqe_offset = true;
        } else if (std::strcmp(key, "sqe_xor") == 0) {
            if (!claim_rule_field(&seen, RuleField::SQE_XOR) || !parse_unsigned(value, &rule->sqe_xor) || rule->sqe_xor == 0) {
                return false;
            }
            have_sqe_xor = true;
        } else {
            return false;
        }
    }
    rule->active = true;
    if (!have_id || rule->id == 0 || !have_direction || !have_action) {
        return false;
    }
    if (rule->surface == WkiChaosSurface::BLOCK_SQE) {
        return rule->action == WkiChaosAction::CORRUPT && !have_corrupt_mode && !have_payload_offset && !have_payload_xor &&
               have_sqe_offset && have_sqe_xor;
    }
    if (rule->surface == WkiChaosSurface::BLOCK_DOORBELL) {
        return !have_corrupt_mode && !have_payload_offset && !have_payload_xor && !have_sqe_offset && !have_sqe_xor;
    }
    if (rule->action != WkiChaosAction::CORRUPT) {
        return !have_corrupt_mode && !have_payload_offset && !have_payload_xor && !have_sqe_offset && !have_sqe_xor;
    }
    if (!have_corrupt_mode || rule->corrupt_mode == WkiChaosCorruptMode::CHECKSUM) {
        return !have_payload_offset && !have_payload_xor && !have_sqe_offset && !have_sqe_xor;
    }
    return have_payload_offset && have_payload_xor && !have_sqe_offset && !have_sqe_xor;
}

auto parse_id_count(TokenCursor& tokens, uint32_t* id, uint32_t* count, bool allow_count) -> bool {
    bool have_id = false;
    bool have_count = false;
    for (char* token = tokens.next(); token != nullptr; token = tokens.next()) {
        char* key = nullptr;
        char* value = nullptr;
        if (!split_key_value(token, &key, &value)) {
            return false;
        }
        if (std::strcmp(key, "id") == 0) {
            if (have_id || !parse_unsigned(value, id)) {
                return false;
            }
            have_id = true;
        } else if (allow_count && std::strcmp(key, "count") == 0) {
            if (have_count || !parse_unsigned(value, count)) {
                return false;
            }
            have_count = true;
        } else {
            return false;
        }
    }
    return have_id && *id != 0;
}

auto pack_ingress_metadata(const WkiRxMetadata* metadata) -> uint64_t {
    if (metadata == nullptr || !metadata->has_src_mac) {
        return 0;
    }
    uint64_t packed = WKI_CHAOS_INGRESS_MAC_PRESENT;
    for (size_t i = 0; i < proto::MacAddress::SIZE_BYTES; ++i) {
        packed |= static_cast<uint64_t>(metadata->src_mac.at(i)) << (i * 8U);
    }
    return packed;
}

auto unpack_ingress_metadata(uint64_t packed, WkiRxMetadata* metadata) -> const WkiRxMetadata* {
    if ((packed & WKI_CHAOS_INGRESS_MAC_PRESENT) == 0 || metadata == nullptr) {
        return nullptr;
    }
    metadata->has_src_mac = true;
    for (size_t i = 0; i < proto::MacAddress::SIZE_BYTES; ++i) {
        metadata->src_mac.at(i) = static_cast<uint8_t>((packed >> (i * 8U)) & 0xFFU);
    }
    return metadata;
}

void frame_epochs_locked(WkiChaosDirection direction, const void* data, uint16_t len, uint32_t* local_boot, uint32_t* peer_boot,
                         uint32_t* peer_channel) {
    *local_boot = g_wki.local_boot_epoch;
    *peer_boot = 0;
    *peer_channel = 0;
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return;
    }
    WkiHeader header = {};
    std::memcpy(&header, data, sizeof(header));
    uint16_t const PEER_NODE = direction == WkiChaosDirection::TX ? header.dst_node : header.src_node;
    for (auto const& row : s_peer_epochs) {
        if (row.node_id == PEER_NODE) {
            *peer_boot = row.remote_boot_epoch;
            *peer_channel = row.remote_channel_epoch;
            break;
        }
    }
}

void deliver_rx(WkiTransport* transport, const void* data, uint16_t len, const WkiRxMetadata* metadata) {
#ifdef WOS_SELFTEST
    if (auto* hook = s_rx_delivery_hook.load(std::memory_order_acquire); hook != nullptr) {
        hook(transport, data, len, metadata);
        return;
    }
#endif
    wki_rx(transport, data, len, metadata);
}

auto deliver_block_doorbell(const WkiChaosBlockKey& key) -> bool {
    if (key.delivery_deadline_us != 0 && wki_now_us() >= key.delivery_deadline_us) {
        return false;
    }
#ifdef WOS_SELFTEST
    if (auto* hook = s_block_doorbell_delivery_hook.load(std::memory_order_acquire); hook != nullptr) {
        return hook(key);
    }
#endif
    if (key.origin == WkiChaosBlockOrigin::PROXY) {
        return wki_dev_proxy_chaos_deliver_doorbell(key);
    }
    return wki_dev_server_chaos_deliver_doorbell(key);
}

struct ChaosRuntimeIntercept {
    WkiChaosInterceptResult decision = {};
    bool transport_leased = false;
};

auto chaos_intercept(WkiChaosDirection direction, WkiTransport* transport, uint16_t neighbor, const void* data, uint16_t len,
                     uint64_t ingress_cookie) -> ChaosRuntimeIntercept {
    uint32_t local_boot = 0;
    uint32_t peer_boot = 0;
    uint32_t peer_channel = 0;
    ChaosRuntimeIntercept result = {};
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    // Epoch writers publish into this fixed cache while peer_lock is held.
    // Looking it up under only the chaos lock avoids channel.lock -> peer_lock
    // inversion and keeps the injected RX/TX boundary allocation-free.
    frame_epochs_locked(direction, data, len, &local_boot, &peer_boot, &peer_channel);
    uint16_t const TRANSPORT_ID = transport != nullptr ? transport->chaos_id : 0;
    if (transport == nullptr || TRANSPORT_ID == 0 || transport->chaos_unregistering.load(std::memory_order_relaxed)) {
        result.decision = s_chaos.transport_removed(direction, TRANSPORT_ID, neighbor, data, len, local_boot, peer_boot, peer_channel);
    } else {
        // This lease spans the immediate callback or suppression decision. It
        // closes the enqueue/callback gap against concurrent unregister.
        transport->chaos_in_flight.fetch_add(1, std::memory_order_relaxed);
        result.transport_leased = true;
        result.decision = s_chaos.intercept(direction, TRANSPORT_ID, reinterpret_cast<uintptr_t>(transport), neighbor, data, len,
                                            local_boot, peer_boot, peer_channel, ingress_cookie);
    }
    s_chaos_lock.unlock_irqrestore(FLAGS);
    return result;
}

void release_transport_lease(WkiTransport* transport, bool leased) {
    if (transport != nullptr && leased) {
        transport->chaos_in_flight.fetch_sub(1, std::memory_order_release);
    }
}

auto deliver_ready_once() -> int {
    WkiChaosDeliveryView delivery = {};
    WkiTransport* transport = nullptr;
    bool leased = false;
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    bool const PRESENT = s_chaos.claim_ready(&delivery);
    if (PRESENT && delivery.kind == WkiChaosDeliveryView::Kind::FRAME) {
        transport = reinterpret_cast<WkiTransport*>(delivery.transport_cookie);
        if (transport != nullptr && transport->chaos_id != 0 && !transport->chaos_unregistering.load(std::memory_order_relaxed)) {
            transport->chaos_in_flight.fetch_add(1, std::memory_order_relaxed);
            leased = true;
        } else {
            s_chaos.finish_delivery(delivery.slot, WKI_ERR_TX_FAILED);
        }
    }
    s_chaos_lock.unlock_irqrestore(FLAGS);
    if (!PRESENT) {
        return WKI_OK;
    }

    if (delivery.kind == WkiChaosDeliveryView::Kind::BLOCK_DOORBELL) {
        int const RESULT = deliver_block_doorbell(delivery.block_key) ? WKI_OK : WKI_ERR_NOT_FOUND;
        uint64_t const FINISH_FLAGS = s_chaos_lock.lock_irqsave();
        s_chaos.finish_delivery(delivery.slot, RESULT);
        s_chaos_lock.unlock_irqrestore(FINISH_FLAGS);
        return RESULT;
    }
    if (!leased) {
        return WKI_OK;
    }

    int result = WKI_ERR_TX_FAILED;
    if (transport != nullptr && delivery.direction == WkiChaosDirection::TX && transport->tx != nullptr) {
        result = transport->tx(transport, delivery.neighbor, delivery.data, delivery.len);
    } else if (transport != nullptr && delivery.direction == WkiChaosDirection::RX) {
        WkiRxMetadata metadata = {};
        deliver_rx(transport, delivery.data, delivery.len, unpack_ingress_metadata(delivery.ingress_cookie, &metadata));
        result = WKI_OK;
    }

    uint64_t const FINISH_FLAGS = s_chaos_lock.lock_irqsave();
    s_chaos.finish_delivery(delivery.slot, result);
    transport->chaos_in_flight.fetch_sub(1, std::memory_order_release);
    s_chaos_lock.unlock_irqrestore(FINISH_FLAGS);
    return result;
}

auto drain_ready() -> int {
    int result = WKI_OK;
    for (size_t delivered = 0; delivered < WKI_CHAOS_MAX_QUEUED_FRAMES + WKI_CHAOS_MAX_QUEUED_BLOCK_EVENTS; ++delivered) {
        bool ready = false;
        uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
        ready = s_chaos.has_ready();
        s_chaos_lock.unlock_irqrestore(FLAGS);
        if (!ready) {
            return result;
        }
        result = deliver_ready_once();
    }
    return result;
}

}  // namespace

void wki_chaos_allow_runtime_control(bool allowed) {
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    s_runtime_control_allowed.store(allowed, std::memory_order_release);
    s_peer_epochs = {};
    if (!allowed) {
        s_chaos.disable();
        s_chaos_enabled.store(false, std::memory_order_release);
    }
    s_chaos_lock.unlock_irqrestore(FLAGS);
}

auto wki_chaos_configure(const char* command, size_t len) -> int {
    if (command == nullptr || len == 0 || len >= WKI_CHAOS_COMMAND_MAX) {
        return WKI_ERR_INVALID;
    }
    for (size_t i = 0; i < len; ++i) {
        if (command[i] == '\0') {
            return WKI_ERR_INVALID;
        }
    }
    std::array<char, WKI_CHAOS_COMMAND_MAX> copy{};
    std::memcpy(copy.data(), command, len);
    copy.at(len) = '\0';
    TokenCursor tokens{.cursor = copy.data()};
    char* verb = tokens.next();
    if (verb == nullptr) {
        return WKI_ERR_INVALID;
    }

    bool notify_release = false;
    int result = WKI_OK;
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    bool const ALLOWED = s_runtime_control_allowed.load(std::memory_order_relaxed);
    if (std::strcmp(verb, "clear") == 0) {
        if (tokens.next() != nullptr) {
            result = WKI_ERR_INVALID;
        } else if (!s_chaos.clear()) {
            result = WKI_ERR_BUSY;
        }
    } else if (std::strcmp(verb, "disable") == 0) {
        if (tokens.next() != nullptr) {
            result = WKI_ERR_INVALID;
        } else {
            s_chaos.disable();
        }
    } else if (ALLOWED && std::strcmp(verb, "enable") == 0) {
        char* token = tokens.next();
        char* key = nullptr;
        char* value = nullptr;
        uint64_t seed = 0;
        if (!split_key_value(token, &key, &value) || std::strcmp(key, "seed") != 0 || !parse_u64(value, &seed) ||
            tokens.next() != nullptr) {
            result = WKI_ERR_INVALID;
        } else {
            s_chaos.enable(seed);
        }
    } else if (ALLOWED && std::strcmp(verb, "rule") == 0) {
        WkiChaosRule rule = {};
        if (!parse_rule(tokens, &rule) || !s_chaos.upsert_rule(rule)) {
            result = WKI_ERR_INVALID;
        }
    } else if (ALLOWED && std::strcmp(verb, "heal") == 0) {
        uint32_t id = 0;
        uint32_t ignored = 0;
        if (!parse_id_count(tokens, &id, &ignored, false)) {
            result = WKI_ERR_INVALID;
        } else if (!s_chaos.heal_partition(id)) {
            result = WKI_ERR_NOT_FOUND;
        }
    } else if (ALLOWED && std::strcmp(verb, "release") == 0) {
        uint32_t id = 0;
        uint32_t count = 0;
        if (!parse_id_count(tokens, &id, &count, true)) {
            result = WKI_ERR_INVALID;
        } else if (s_chaos.release(id, count) == 0) {
            result = WKI_ERR_NOT_FOUND;
        } else {
            notify_release = true;
        }
    } else {
        result = WKI_ERR_INVALID;
    }
    s_chaos_enabled.store(s_chaos.enabled(), std::memory_order_release);
    s_chaos_lock.unlock_irqrestore(FLAGS);

    if (notify_release) {
        wki_deferred_work_notify();
    }
    return result;
}

auto wki_chaos_snapshot(WkiChaosSnapshot* out) -> bool {
    size_t rule_count = 0;
    size_t trace_count = 0;
    return wki_chaos_capture(out, nullptr, 0, &rule_count, nullptr, 0, &trace_count);
}

auto wki_chaos_capture(WkiChaosSnapshot* out, WkiChaosRuleRow* rules, size_t max_rules, size_t* rule_count, WkiChaosTraceRow* trace,
                       size_t max_trace, size_t* trace_count, uint64_t after_event_id) -> bool {
    if (out == nullptr || rule_count == nullptr || trace_count == nullptr || (max_rules != 0 && rules == nullptr) ||
        (max_trace != 0 && trace == nullptr)) {
        return false;
    }
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    *out = WkiChaosSnapshot{
        .schema_version = WKI_CHAOS_SCHEMA_VERSION,
        .supported = true,
        .runtime_control_allowed = s_runtime_control_allowed.load(std::memory_order_relaxed),
        .enabled = s_chaos.enabled(),
        .seed = s_chaos.seed(),
        .active_rule_count = s_chaos.active_rule_count(),
        .queued_frame_count = s_chaos.queued_frame_count(),
        .queued_block_event_count = s_chaos.queued_block_event_count(),
        .trace_count = s_chaos.trace_count(),
        .trace_first_event_id = s_chaos.trace_first_event_id(),
        .trace_last_event_id = s_chaos.trace_last_event_id(),
        .counters = s_chaos.counters(),
        .queue_overflow = s_chaos.queue_overflow(),
        .trace_overflow = s_chaos.trace_overflow(),
        .stream_overflow = s_chaos.stream_overflow(),
        .invalid = s_chaos.invalid(),
    };
    *rule_count = s_chaos.rule_snapshot(rules, max_rules);
    *trace_count = s_chaos.trace_snapshot(trace, max_trace, after_event_id);
    s_chaos_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto wki_chaos_rule_snapshot(WkiChaosRuleRow* out, size_t max_rows) -> size_t {
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    size_t const COUNT = s_chaos.rule_snapshot(out, max_rows);
    s_chaos_lock.unlock_irqrestore(FLAGS);
    return COUNT;
}

auto wki_chaos_trace_snapshot(WkiChaosTraceRow* out, size_t max_rows, uint64_t after_event_id) -> size_t {
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    size_t const COUNT = s_chaos.trace_snapshot(out, max_rows, after_event_id);
    s_chaos_lock.unlock_irqrestore(FLAGS);
    return COUNT;
}

void wki_chaos_transport_register(WkiTransport* transport) {
    if (transport == nullptr) {
        return;
    }
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    WkiTransport** free_slot = nullptr;
    for (auto& active : s_registered_transports) {
        if (active == transport) {
            // Registration is idempotent. In particular, never reset a lease
            // counter or cancel teardown for a concurrently unregistering
            // transport.
            s_chaos_lock.unlock_irqrestore(FLAGS);
            return;
        }
        if (free_slot == nullptr && active == nullptr) {
            free_slot = &active;
        }
    }
    if (free_slot == nullptr) {
        // WKI_MAX_TRANSPORTS is the subsystem contract. Exceeding it must not
        // alias an existing chaos identity; injections fail closed instead.
        transport->chaos_id = 0;
        transport->chaos_unregistering.store(true, std::memory_order_relaxed);
        s_chaos_lock.unlock_irqrestore(FLAGS);
        return;
    }

    uint16_t candidate = s_next_transport_id == 0 ? 1 : s_next_transport_id;
    bool allocated = false;
    // At most WKI_MAX_TRANSPORTS-1 IDs are live when a free slot exists, so a
    // collision-free nonzero candidate is found within this bounded scan even
    // when the uint16_t allocator wraps.
    for (size_t attempt = 0; attempt <= s_registered_transports.size(); ++attempt) {
        bool collision = false;
        for (auto* active : s_registered_transports) {
            if (active != nullptr && active->chaos_id == candidate) {
                collision = true;
                break;
            }
        }
        if (!collision) {
            allocated = true;
            break;
        }
        candidate = next_transport_id(candidate);
    }
    if (!allocated) {
        transport->chaos_id = 0;
        transport->chaos_unregistering.store(true, std::memory_order_relaxed);
        s_chaos_lock.unlock_irqrestore(FLAGS);
        return;
    }

    transport->chaos_in_flight.store(0, std::memory_order_relaxed);
    transport->chaos_unregistering.store(false, std::memory_order_relaxed);
    transport->chaos_id = candidate;
    *free_slot = transport;
    s_next_transport_id = next_transport_id(candidate);
    s_chaos_lock.unlock_irqrestore(FLAGS);
}

void wki_chaos_transport_unregister(WkiTransport* transport) {
    if (transport == nullptr) {
        return;
    }
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    transport->chaos_unregistering.store(true, std::memory_order_relaxed);
    s_chaos.discard_transport(reinterpret_cast<uintptr_t>(transport), transport->chaos_id);
    s_chaos_lock.unlock_irqrestore(FLAGS);

    // Deferred callbacks are bounded TX/RX operations and run without the
    // chaos or transport-registry lock. Waiting here prevents the caller from
    // freeing transport/private storage while one such callback is active.
    while (transport->chaos_in_flight.load(std::memory_order_acquire) != 0) {
        if (mod::sched::can_query_current_task() && mod::sched::preemptible() && mod::sched::interrupts_enabled()) {
            mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }

    uint64_t const FINISH_FLAGS = s_chaos_lock.lock_irqsave();
    for (auto& active : s_registered_transports) {
        if (active == transport) {
            active = nullptr;
            break;
        }
    }
    transport->chaos_id = 0;
    s_chaos_lock.unlock_irqrestore(FINISH_FLAGS);
}

void wki_chaos_peer_epoch_update(uint16_t node_id, uint32_t remote_boot_epoch, uint32_t remote_channel_epoch) {
    if (node_id == WKI_NODE_INVALID || node_id == WKI_NODE_BROADCAST) {
        return;
    }
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    ChaosPeerEpochRow* empty = nullptr;
    for (auto& row : s_peer_epochs) {
        if (row.node_id == node_id) {
            row.remote_boot_epoch = remote_boot_epoch;
            row.remote_channel_epoch = remote_channel_epoch;
            s_chaos_lock.unlock_irqrestore(FLAGS);
            return;
        }
        if (empty == nullptr && row.node_id == WKI_NODE_INVALID) {
            empty = &row;
        }
    }
    if (empty != nullptr) {
        *empty = ChaosPeerEpochRow{
            .node_id = node_id,
            .remote_boot_epoch = remote_boot_epoch,
            .remote_channel_epoch = remote_channel_epoch,
        };
    }
    s_chaos_lock.unlock_irqrestore(FLAGS);
}

auto wki_transport_send(WkiTransport* transport, uint16_t neighbor, const void* data, uint16_t len) -> int {
    if (transport == nullptr || transport->tx == nullptr) {
        return WKI_ERR_TX_FAILED;
    }
    if (!s_chaos_enabled.load(std::memory_order_relaxed)) {
        return transport->tx(transport, neighbor, data, len);
    }

    auto const INTERCEPT = chaos_intercept(WkiChaosDirection::TX, transport, neighbor, data, len, 0);
    int result = WKI_ERR_TX_FAILED;
    switch (INTERCEPT.decision.action) {
        case WkiChaosAction::PASS:
            result = transport->tx(transport, neighbor, data, len);
            break;
        case WkiChaosAction::DROP:
        case WkiChaosAction::PARTITION:
        case WkiChaosAction::DELAY:
        case WkiChaosAction::REORDER:
            result = WKI_OK;
            break;
        case WkiChaosAction::FAIL:
            if (INTERCEPT.decision.outcome != WkiChaosOutcome::TRANSPORT_REMOVED) {
                wki_eth_note_injected_tx_failure(transport, data, len);
            }
            result = WKI_ERR_TX_FAILED;
            break;
        case WkiChaosAction::DUPLICATE:
            result = transport->tx(transport, neighbor, data, len);
            wki_deferred_work_notify();
            break;
        case WkiChaosAction::CORRUPT:
            wki_deferred_work_notify();
            result = WKI_OK;
            break;
        case WkiChaosAction::RELEASE:
            result = transport->tx(transport, neighbor, data, len);
            break;
    }
    release_transport_lease(transport, INTERCEPT.transport_leased);
    return result;
}

auto wki_transport_send_pkt(WkiTransport* transport, uint16_t neighbor, ker::net::PacketBuffer* pkt) -> int {
    if (pkt == nullptr) {
        return WKI_ERR_TX_FAILED;
    }
    if (transport == nullptr || transport->tx_pkt == nullptr) {
        ker::net::pkt_free(pkt);
        return WKI_ERR_TX_FAILED;
    }
    if (!s_chaos_enabled.load(std::memory_order_relaxed)) {
        return transport->tx_pkt(transport, neighbor, pkt);
    }

    auto const INTERCEPT = chaos_intercept(WkiChaosDirection::TX, transport, neighbor, pkt->data, static_cast<uint16_t>(pkt->len), 0);
    int result = WKI_ERR_TX_FAILED;
    switch (INTERCEPT.decision.action) {
        case WkiChaosAction::PASS:
            result = transport->tx_pkt(transport, neighbor, pkt);
            break;
        case WkiChaosAction::DROP:
        case WkiChaosAction::PARTITION:
        case WkiChaosAction::DELAY:
        case WkiChaosAction::REORDER:
            ker::net::pkt_free(pkt);
            result = WKI_OK;
            break;
        case WkiChaosAction::FAIL:
            if (INTERCEPT.decision.outcome != WkiChaosOutcome::TRANSPORT_REMOVED) {
                wki_eth_note_injected_tx_failure(transport, pkt->data, static_cast<uint16_t>(pkt->len));
            }
            ker::net::pkt_free(pkt);
            result = WKI_ERR_TX_FAILED;
            break;
        case WkiChaosAction::DUPLICATE:
            result = transport->tx_pkt(transport, neighbor, pkt);
            wki_deferred_work_notify();
            break;
        case WkiChaosAction::CORRUPT:
            ker::net::pkt_free(pkt);
            wki_deferred_work_notify();
            result = WKI_OK;
            break;
        case WkiChaosAction::RELEASE:
            result = transport->tx_pkt(transport, neighbor, pkt);
            break;
    }
    release_transport_lease(transport, INTERCEPT.transport_leased);
    return result;
}

void wki_chaos_rx_ingress(WkiTransport* transport, const void* data, uint16_t len, const WkiRxMetadata* metadata) {
    if (!s_chaos_enabled.load(std::memory_order_relaxed)) {
        deliver_rx(transport, data, len, metadata);
        return;
    }
    uint16_t neighbor = WKI_NODE_INVALID;
    if (data != nullptr && len >= WKI_HEADER_SIZE) {
        WkiHeader header = {};
        std::memcpy(&header, data, sizeof(header));
        neighbor = header.src_node;
    }
    auto const INTERCEPT = chaos_intercept(WkiChaosDirection::RX, transport, neighbor, data, len, pack_ingress_metadata(metadata));
    switch (INTERCEPT.decision.action) {
        case WkiChaosAction::PASS:
            deliver_rx(transport, data, len, metadata);
            break;
        case WkiChaosAction::DUPLICATE:
            deliver_rx(transport, data, len, metadata);
            wki_deferred_work_notify();
            break;
        case WkiChaosAction::CORRUPT:
            wki_deferred_work_notify();
            break;
        case WkiChaosAction::DROP:
        case WkiChaosAction::DELAY:
        case WkiChaosAction::REORDER:
        case WkiChaosAction::FAIL:
        case WkiChaosAction::PARTITION:
        case WkiChaosAction::RELEASE:
            break;
    }
    release_transport_lease(transport, INTERCEPT.transport_leased);
}

auto wki_chaos_block_doorbell(const WkiChaosBlockKey& key) -> WkiChaosAction {
    if (!s_chaos_enabled.load(std::memory_order_relaxed)) {
        return WkiChaosAction::PASS;
    }
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    WkiChaosInterceptResult const RESULT = s_chaos.intercept_block_doorbell(key);
    s_chaos_lock.unlock_irqrestore(FLAGS);
    return RESULT.action;
}

void wki_chaos_block_sqe(const WkiChaosBlockKey& key, void* descriptor, uint16_t descriptor_len) {
    if (!s_chaos_enabled.load(std::memory_order_relaxed)) {
        return;
    }
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    static_cast<void>(s_chaos.intercept_block_sqe(key, descriptor, descriptor_len));
    s_chaos_lock.unlock_irqrestore(FLAGS);
}

void wki_chaos_drain_ready() { static_cast<void>(drain_ready()); }

#ifdef WOS_SELFTEST
void wki_chaos_selftest_set_rx_delivery_hook(WkiChaosRxDeliveryHook hook) { s_rx_delivery_hook.store(hook, std::memory_order_release); }

void wki_chaos_selftest_set_block_doorbell_delivery_hook(WkiChaosBlockDoorbellDeliveryHook hook) {
    s_block_doorbell_delivery_hook.store(hook, std::memory_order_release);
}

void wki_chaos_selftest_set_next_transport_id(uint16_t next_id) {
    uint64_t const FLAGS = s_chaos_lock.lock_irqsave();
    s_next_transport_id = next_id == 0 ? 1 : next_id;
    s_chaos_lock.unlock_irqrestore(FLAGS);
}
#endif

}  // namespace ker::net::wki
