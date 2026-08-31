#pragma once

#include <cstddef>
#include <cstdint>
#include <net/wki/chaos_model.hpp>

namespace ker::net {
struct PacketBuffer;
}

namespace ker::net::wki {

struct WkiTransport;
struct WkiRxMetadata;

struct WkiChaosSnapshot {
    uint32_t schema_version = WKI_CHAOS_SCHEMA_VERSION;
    bool supported = true;
    bool runtime_control_allowed = false;
    bool enabled = false;
    uint64_t seed = 0;
    size_t active_rule_count = 0;
    size_t queued_frame_count = 0;
    size_t queued_block_event_count = 0;
    size_t trace_count = 0;
    uint64_t trace_first_event_id = 0;
    uint64_t trace_last_event_id = 0;
    WkiChaosCounters counters = {};
    bool queue_overflow = false;
    bool trace_overflow = false;
    bool stream_overflow = false;
    bool invalid = false;
};

using WkiChaosRuleRow = WkiChaosRule;

// Runtime mutation is fail-closed. Boot initialization explicitly opts in
// after finding the `wki.chaos` kernel-command-line token.
void wki_chaos_allow_runtime_control(bool allowed);

// Bounded ASCII command parser. Commands are documented and parsed in the
// implementation; no caller-owned pointer is retained.
auto wki_chaos_configure(const char* command, size_t len) -> int;
auto wki_chaos_snapshot(WkiChaosSnapshot* out) -> bool;
auto wki_chaos_rule_snapshot(WkiChaosRuleRow* out, size_t max_rows) -> size_t;
auto wki_chaos_trace_snapshot(WkiChaosTraceRow* out, size_t max_rows, uint64_t after_event_id = 0) -> size_t;
// Copies summary, active rules, and trace rows under one lock acquisition so
// procfs consumers never observe rows from a different generation than the
// summary counts.
auto wki_chaos_capture(WkiChaosSnapshot* out, WkiChaosRuleRow* rules, size_t max_rules, size_t* rule_count, WkiChaosTraceRow* trace,
                       size_t max_trace, size_t* trace_count, uint64_t after_event_id = 0) -> bool;

// Internal message-boundary hooks. The disabled path performs one atomic load
// and calls the original transport callback directly.
void wki_chaos_transport_register(WkiTransport* transport);
// Teardown is task-context: it may yield until a bounded immediate/deferred
// callback that already owns the transport lease has returned.
void wki_chaos_transport_unregister(WkiTransport* transport);
// Called while peer state is serialized. The chaos subsystem keeps its own
// fixed snapshot so injected send paths never acquire peer_lock while holding
// a channel lock.
void wki_chaos_peer_epoch_update(uint16_t node_id, uint32_t remote_boot_epoch, uint32_t remote_channel_epoch);
auto wki_transport_send(WkiTransport* transport, uint16_t neighbor, const void* data, uint16_t len) -> int;
auto wki_transport_send_pkt(WkiTransport* transport, uint16_t neighbor, ker::net::PacketBuffer* pkt) -> int;
void wki_chaos_rx_ingress(WkiTransport* transport, const void* data, uint16_t len, const WkiRxMetadata* metadata);
// Block-ring semantic hooks. Callers publish descriptors or invoke their
// original notification path only after consulting the returned action. The
// disabled path is one relaxed atomic load and returns PASS without touching
// the descriptor or retaining any identity/object pointer.
auto wki_chaos_block_doorbell(const WkiChaosBlockKey& key) -> WkiChaosAction;
void wki_chaos_block_sqe(const WkiChaosBlockKey& key, void* descriptor, uint16_t descriptor_len);
void wki_chaos_drain_ready();

#ifdef WOS_SELFTEST
using WkiChaosRxDeliveryHook = void (*)(WkiTransport* transport, const void* data, uint16_t len, const WkiRxMetadata* metadata);
using WkiChaosBlockDoorbellDeliveryHook = bool (*)(const WkiChaosBlockKey& key);
void wki_chaos_selftest_set_rx_delivery_hook(WkiChaosRxDeliveryHook hook);
void wki_chaos_selftest_set_block_doorbell_delivery_hook(WkiChaosBlockDoorbellDeliveryHook hook);
void wki_chaos_selftest_set_next_transport_id(uint16_t next_id);
#endif

}  // namespace ker::net::wki
