#pragma once

#include <cstddef>
#include <cstdint>
#include <net/wki/rdma_control.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <span>

namespace ker::net::wki {

struct WkiTransport;
struct WkiRxMetadata;
struct WkiPeer;
struct WkiHeader;
struct WkiChannelIdentity;

enum class WkiSecureRdmaOutcome : uint8_t { NO_EFFECT, EXACT, UNCERTAIN };

struct WkiSecureRdmaResult {
    // Native negative errno; transport-specific WKI status never escapes here.
    int32_t status = 0;
    uint32_t bytes = 0;
    WkiSecureRdmaOutcome outcome = WkiSecureRdmaOutcome::NO_EFFECT;
};

// Immutable request presented to the registered native consumer. A capability
// authorizes a region, while this full ticket authorizes one native operation.
struct WkiSecureRdmaRequest {
    rdma_protocol::RegionDescriptor grant{};
    uint64_t operation_id = 0;
    uint64_t receive_ticket = 0;
    uint64_t offset = 0;
    uint32_t length = 0;
    uint16_t peer_node = 0;
    uint16_t flags = 0;
    rdma_protocol::Opcode opcode = rdma_protocol::Opcode::READ;
    uint16_t intent_length = 0;
    std::array<uint8_t, rdma_control::NATIVE_INTENT_BYTES> native_intent{};
};

struct WkiSecureRdmaHooks {
    // Task context, outside core gates. Checks the native allocation/policy and
    // reserves an exact ticket and overlap/credit window. No native effect yet.
    int (*arm)(void* context, const WkiSecureRdmaRequest& request, uint64_t& consumer_generation) = nullptr;
    // Task context, outside core gates, after full coverage and registry claim.
    // Recheck native authority; WRITE input is immutable, READ fills output and
    // returns its actual prefix. Never call a synchronous secure transfer here.
    WkiSecureRdmaResult (*effect)(void* context, const WkiSecureRdmaRequest& request, std::span<uint8_t> private_backing,
                                  std::span<const uint8_t> input, std::span<uint8_t> output) = nullptr;
    // Exactly once for every successful arm, after effect/TX retirement. Frees
    // native reservations/pins. Context remains owned until unregister succeeds.
    void (*release)(void* context, const WkiSecureRdmaRequest& request, const WkiSecureRdmaResult& result) = nullptr;
};

// Initialization retains only boot-private capability entropy. The default
// service mask remains zero until each consumer's qualification is complete;
// the diagnostic command-line profile negotiates explicit service masks.
auto wki_secure_rdma_init() -> bool;
void wki_secure_rdma_worker_start();
void wki_secure_rdma_tick();
// Called under exclusive peer lifecycle before control keys are replaced or
// erased. Ends admission synchronously; task worker drains claimed effects.
void wki_secure_rdma_retire_peer(WkiPeer* peer);
// Close before provider registration is removed; queued references release
// without a peer gate. Caller drains them before freeing provider storage.
void wki_secure_rdma_retire_transport(WkiTransport* transport, uint64_t generation);
auto wki_secure_rdma_shutdown() -> bool;

auto wki_secure_rdma_available(const WkiChannelIdentity& identity, rdma_protocol::Service service) -> bool;
// Diagnostic authority snapshot; NONE includes try-only pressure. This never
// substitutes for the transfer's own exact session/grant admission.
auto wki_secure_rdma_transport(const WkiChannelIdentity& identity, rdma_protocol::Service service) -> rdma_protocol::Transport;
auto wki_secure_rdma_register_region(const WkiChannelIdentity& identity, rdma_protocol::Service service, void* private_backing,
                                     uint32_t size, uint64_t binding_generation, uint8_t permissions, const WkiSecureRdmaHooks& hooks,
                                     void* context, rdma_protocol::RegionDescriptor& output) -> int;
// Nonblocking close/reclaim. On BUSY the exact backing/context must be retained
// and retried; neither timeout nor failed drain permits freeing native memory.
auto wki_secure_rdma_unregister_region(const rdma_protocol::RegionDescriptor& local) -> int;
// 0 means this exact local descriptor is current; -EAGAIN is try-only pressure,
// -ESTALE means the allocation/session/provider binding has retired. Pressure
// never authorizes closing or replacing a consumer's current context.
auto wki_secure_rdma_local_region_current(const WkiChannelIdentity& identity, rdma_protocol::Service service,
                                          const rdma_protocol::RegionDescriptor& local) -> int;
// Full descriptors arrive only through authenticated control. This lookup also
// checks the exact local channel, provider and currently confirmed session.
auto wki_secure_rdma_remote_grant(const WkiChannelIdentity& identity, rdma_protocol::Service service, uint32_t rkey,
                                  rdma_protocol::RegionDescriptor& output, uint8_t required_permissions = 0) -> bool;

// Task-only synchronous transfers copy WRITE input before the first wait and
// retain no caller buffer across waits. READ performs a finite final copy from
// retained private result storage. NO_EFFECT may permit consumer-specific
// message fallback; UNCERTAIN WRITE never does. Identities/tickets never wrap.
auto wki_secure_rdma_write(const WkiChannelIdentity& identity, const rdma_protocol::RegionDescriptor& remote, uint64_t offset,
                           std::span<const uint8_t> input, uint16_t flags = 0, std::span<const uint8_t> native_intent = {})
    -> WkiSecureRdmaResult;
auto wki_secure_rdma_read(const WkiChannelIdentity& identity, const rdma_protocol::RegionDescriptor& remote, uint64_t offset,
                          std::span<uint8_t> output, std::span<const uint8_t> native_intent = {}) -> WkiSecureRdmaResult;

// Reliable authenticated controls enter a bounded private queue. The dispatch
// identity and current control session are captured before receive storage dies.
void wki_secure_rdma_control_ingress(const WkiHeader* header, const void* payload, uint16_t size, const WkiChannelIdentity& identity);
enum class WkiSecureRdmaControlRxAdmission : uint8_t { NOT_APPLICABLE, RETRY, DISCARDED, QUEUED };
// Core reliable RX only: caller owns the authenticated, authorized peer's
// lifecycle admission and the validated exact channel guard. Copy before
// advancing sequence/ACK or unlinking a reorder entry; RETRY changes neither.
// No channel re-lock, allocation, scheduler wake, network or native effects.
auto wki_secure_rdma_admit_control_rx(const WkiHeader* header, const void* payload, uint16_t size, const WkiChannelIdentity& identity,
                                      WkiPeer* admitted_peer) -> WkiSecureRdmaControlRxAdmission;

struct WkiSecureRdmaDiagnostics {
    std::array<uint64_t, static_cast<size_t>(rdma_protocol::Drop::COUNT)> drops{};
    uint32_t queued_records = 0;
    uint32_t live_regions = 0;
    uint32_t region_references = 0;
    uint32_t region_claims = 0;
    uint32_t live_operations = 0;
    uint32_t live_keys = 0;
    uint32_t replay_windows = 0;
    uint32_t consumer_pins = 0;
    uint32_t pending_revocations = 0;
    // Saturating, non-secret final client EXACT completions and actual bytes.
    // Index: (typed transport - 1) * 4 + (service - 1); both enums are checked.
    std::array<uint64_t, 8> client_completions{};
    std::array<uint64_t, 8> client_bytes{};
    // Same indices, separated by validated operation direction so metadata
    // READs cannot qualify an ordinary-fallback payload WRITE.
    std::array<uint64_t, 8> client_read_completions{};
    std::array<uint64_t, 8> client_read_bytes{};
    std::array<uint64_t, 8> client_write_completions{};
    std::array<uint64_t, 8> client_write_bytes{};
};
auto wki_secure_rdma_diagnostics(WkiSecureRdmaDiagnostics& output) -> bool;

// Adapter boundary only: callers own a stable private packet/ring snapshot.
// The common engine copies at most one complete secure record into a fixed
// queue and owns authentication, replay, grant and operation admission. RX is
// bounded, allocation-free, nonblocking, and never calls native service code.
// Unnegotiated/retired providers and all legacy raw records fail closed.
void wki_secure_rdma_rx_ingress(WkiTransport* transport, const void* record, uint16_t size, const WkiRxMetadata* metadata);

}  // namespace ker::net::wki
