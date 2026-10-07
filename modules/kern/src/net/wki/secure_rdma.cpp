#include "secure_rdma.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <net/wki/chaos.hpp>
#include <net/wki/rdma_control.hpp>
#include <net/wki/rdma_operations.hpp>
#include <net/wki/rdma_registry.hpp>
#include <net/wki/rdma_session.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/mm/phys.hpp>
#include <platform/random/entropy.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <utility>

namespace ker::net::wki {

namespace {
namespace proto = rdma_protocol;
namespace ops = rdma_operations;
namespace sched = ker::mod::sched;
constexpr size_t RECORD_CELLS = 64;
constexpr size_t CONTROL_CELLS = 64;
constexpr size_t REGIONS = rdma_registry::REGION_SLOTS;
constexpr uint64_t RETRY_US = 100'000;
constexpr uint64_t WORKER_US = 1'000;
constexpr size_t WORK_BUDGET = 8;
constexpr uint64_t OPERATION_US = 5'000'000;
constexpr uint64_t NODE_SEQUENCE_MAX = (uint64_t{1} << 48) - 1;

class EngineGuard final {
   public:
    explicit EngineGuard(ker::mod::sys::Spinlock& lock) : lock_(lock), held_(lock.try_lock()) {}
    ~EngineGuard() {
        if (held_) lock_.unlock();
    }
    explicit operator bool() const { return held_; }
    EngineGuard(const EngineGuard&) = delete;
    auto operator=(const EngineGuard&) -> EngineGuard& = delete;

   private:
    ker::mod::sys::Spinlock& lock_;
    bool held_;
};
class OwnerReset final {
   public:
    explicit OwnerReset(std::atomic<bool>& owner) : owner_(owner) {}
    ~OwnerReset() { owner_.store(false, std::memory_order_release); }
    OwnerReset(const OwnerReset&) = delete;
    auto operator=(const OwnerReset&) -> OwnerReset& = delete;

   private:
    std::atomic<bool>& owner_;
};
class PeerAdmission final {
   public:
    ~PeerAdmission() { reset(); }
    auto acquire(WkiPeer* peer) -> bool {
        if (peer_ != nullptr || !wki_peer_lifecycle_rx_try_acquire(peer)) return false;
        peer_ = peer;
        return true;
    }
    void reset() {
        if (peer_ != nullptr) wki_peer_lifecycle_rx_release(std::exchange(peer_, nullptr));
    }

   private:
    WkiPeer* peer_ = nullptr;
};
// Compare registry addresses before dereference. Teardown waits on the existing
// provider reference; release needs neither registry nor peer admission.
class ProviderPin final {
   public:
    ProviderPin() = default;
    ~ProviderPin() { reset(); }
    ProviderPin(const ProviderPin&) = delete;
    auto operator=(const ProviderPin&) -> ProviderPin& = delete;
    ProviderPin(ProviderPin&& other) noexcept { *this = std::move(other); }
    auto operator=(ProviderPin&& other) noexcept -> ProviderPin& {
        if (this != &other) {
            reset();
            transport = std::exchange(other.transport, nullptr);
            generation = std::exchange(other.generation, 0);
            kind = std::exchange(other.kind, proto::Transport::NONE);
        }
        return *this;
    }
    auto acquire(WkiTransport* expected, uint64_t expected_generation = 0) -> bool {
        if (transport != nullptr || expected == nullptr || !g_wki.transport_lock.try_lock()) return false;
        bool retained = false;
        for (auto* candidate = g_wki.transports; candidate != nullptr; candidate = candidate->next) {
            if (candidate != expected) continue;
            if (!candidate->chaos_unregistering.load(std::memory_order_acquire) && candidate->registration_generation != 0 &&
                (expected_generation == 0 || candidate->registration_generation == expected_generation) &&
                candidate->secure_tx != nullptr && proto::transport_bit(candidate->secure_kind) != 0 &&
                candidate->mtu >= proto::RECORD_BYTES) {
                uint32_t count = candidate->chaos_in_flight.load(std::memory_order_relaxed);
                if (count != UINT32_MAX &&
                    candidate->chaos_in_flight.compare_exchange_strong(count, count + 1, std::memory_order_acq_rel)) {
                    transport = candidate;
                    generation = candidate->registration_generation;
                    kind = candidate->secure_kind;
                    retained = true;
                }
            }
            break;
        }
        g_wki.transport_lock.unlock();
        return retained;
    }
    auto status() const -> int {
        if (transport == nullptr) return -ESTALE;
        if (!g_wki.transport_lock.try_lock()) return -EAGAIN;
        bool live = false;
        for (auto* candidate = g_wki.transports; candidate != nullptr; candidate = candidate->next) {
            if (candidate != transport) continue;
            live = candidate->registration_generation == generation && !candidate->chaos_unregistering.load(std::memory_order_acquire) &&
                   candidate->secure_kind == kind && candidate->secure_tx != nullptr;
            break;
        }
        g_wki.transport_lock.unlock();
        return live ? 0 : -ESTALE;
    }
    auto current() const -> bool { return status() == 0; }
    void reset() {
        if (transport != nullptr) std::exchange(transport, nullptr)->chaos_in_flight.fetch_sub(1, std::memory_order_release);
        generation = 0;
        kind = proto::Transport::NONE;
    }
    WkiTransport* transport = nullptr;
    uint64_t generation = 0;
    proto::Transport kind = proto::Transport::NONE;
};
struct PeerContext {
    rdma_session::Session session;
    std::atomic<bool> closed{true};
    std::atomic<uintptr_t> provider_cookie{0};
    std::atomic<uint64_t> provider_generation{0};
    // Immutable enqueue identity; effect authority still requires the worker's
    // fully confirmed context under the engine gate.
    std::atomic<uint64_t> record_incarnation{0};
    bool begun = false, installed = false, confirmed = false, confirm_sent = false;
    uint16_t node = 0;
    uint32_t auth_generation = 0;
    uint64_t incarnation = 0, retry_at = 0;
    proto::SessionId sid{};
    proto::Offer local{}, remote{};
    auth_crypto::Digest negotiation{};
    WkiChannelIdentity control{};
    proto::ReplayWindow arm_replay{};
    proto::SessionId remote_history_sid{};
    uint64_t remote_history_incarnation = 0;
};
struct Region {
    bool live = false, closing = false, grant_pending = false, revoke_pending = false, revoked = false;
    uint32_t references = 0;
    rdma_registry::Binding binding{};
    proto::RegionDescriptor descriptor{};
    WkiSecureRdmaHooks hooks{};
    void* context = nullptr;
};
struct RemoteRegion {
    bool live = false;
    rdma_registry::Binding binding{};
    proto::RegionDescriptor descriptor{};
};
enum class JobState : uint8_t {
    FREE,
    RESERVED,
    ARM_SEND,
    ARM_WAIT,
    OUT_PREPARE,
    OUT_SEND,
    OUT_WAIT,
    IN_ARM,
    IN_WAIT,
    IN_FINISH,
    IN_REPLY,
    CANCEL,
    COMPLETE
};
struct Job {
    std::atomic<JobState> state{JobState::FREE};
    bool outgoing = false, frontend = false, arm_succeeded = false, ack_pending = false, ack_seen = false, effect_done = false,
         end_sent = false;
    bool native_result_seen = false, result_sent = false, timeout_logged = false;
    std::atomic<bool> cancel{false}, done{false};
    rdma_registry::Binding binding{};
    rdma_control::Arm arm{};
    int32_t ack_status = 0;
    uint64_t deadline = 0, retry_at = 0, consumer_generation = 0;
    uint32_t position = 0;
    size_t region = REGIONS;
    sched::task::Task* prepare_owner = nullptr;
    WkiSecureRdmaResult result{};
    ops::Handle operation;
    ops::Work work;
    ProviderPin provider;
    std::array<uint8_t, proto::RECORD_BYTES> tx_record{};
};
enum class CellState : uint8_t { FREE, QUEUED, PROCESSING };
struct RecordCell {
    CellState state = CellState::FREE;
    bool retry = false, decoded = false;
    size_t peer = WKI_MAX_PEERS;
    uint32_t auth_generation = 0;
    proto::SessionId sid{};
    uint64_t secure_incarnation = 0;
    rdma_registry::Binding binding{};
    proto::Header header{};
    uint16_t size = 0;
    ProviderPin provider;
    std::array<uint8_t, proto::RECORD_BYTES> record{};
};
struct ControlCell {
    CellState state = CellState::FREE;
    bool retry = false;
    size_t peer = WKI_MAX_PEERS;
    uint32_t auth_generation = 0;
    proto::SessionId sid{};
    uint64_t secure_incarnation = 0, provider_generation = 0;
    uintptr_t provider_cookie = 0;
    WkiChannelIdentity identity{};
    MsgType type{};
    uint16_t size = 0;
    std::array<uint8_t, sizeof(rdma_control::Result)> payload{};
};
ker::mod::sys::Spinlock s_lock;
ker::mod::sys::Spinlock s_record_lock;
std::atomic<bool> s_initialized{false}, s_stopping{false}, s_service_owner{false}, s_worker_stopped{true};
bool s_initialized_once = false, s_worker_starting = false;
uint8_t s_services = 0;
uint64_t s_next_incarnation = 1, s_next_operation = 1, s_next_ticket = 1;
sched::task::Task* s_worker = nullptr;
uint8_t* s_scratch = nullptr;
std::array<PeerContext, WKI_MAX_PEERS> s_peers{};
std::array<Region, REGIONS> s_regions{};
std::array<RemoteRegion, REGIONS> s_remote{};
std::array<Job, ops::OPERATION_SLOTS> s_jobs{};
std::array<RecordCell, RECORD_CELLS> s_records{};
// Only the service_pass owner decodes records. Session::receive may erase its
// input on pressure, so preserve the queue-owned ciphertext until it succeeds.
// Static private scratch avoids a frame-sized object on the kernel task stack.
std::array<uint8_t, proto::RECORD_BYTES> s_record_decode{};
std::array<ControlCell, CONTROL_CELLS> s_controls{};
std::array<size_t, CONTROL_CELLS> s_control_order{};
std::array<size_t, RECORD_CELLS> s_record_order{};
size_t s_control_head = 0, s_control_count = 0, s_record_head = 0, s_record_count = 0;
std::array<std::atomic<uint64_t>, static_cast<size_t>(proto::Drop::COUNT)> s_drops{};
std::array<std::atomic<uint64_t>, 8> s_client_completions{}, s_client_bytes{};
std::array<std::atomic<uint64_t>, 8> s_client_read_completions{}, s_client_read_bytes{}, s_client_write_completions{},
    s_client_write_bytes{};
std::atomic<uint32_t> s_task_gate_warnings{0};
std::atomic<uint32_t> s_timeout_warnings{0};
rdma_registry::Registry s_registry;
ops::Operations s_operations;
ops::EraseWork s_eraser;
bool s_eraser_wiped = false;
size_t s_peer_cursor = 0;

void dropped(proto::Drop reason) {
    auto& count = s_drops[static_cast<size_t>(reason)];
    uint64_t previous = count.load(std::memory_order_relaxed);
    while (previous != UINT64_MAX && !count.compare_exchange_weak(previous, previous + 1, std::memory_order_relaxed)) {
    }
}
void diagnostic_add(std::atomic<uint64_t>& target, uint64_t value) {
    uint64_t previous = target.load(std::memory_order_relaxed);
    while (previous != UINT64_MAX) {
        uint64_t const NEXT = value > UINT64_MAX - previous ? UINT64_MAX : previous + value;
        if (target.compare_exchange_weak(previous, NEXT, std::memory_order_relaxed)) return;
    }
}
void count_client_result(const rdma_registry::Binding& binding, proto::Opcode opcode, const WkiSecureRdmaResult& result) {
    auto const TRANSPORT = static_cast<uint8_t>(binding.transport), SERVICE = static_cast<uint8_t>(binding.service);
    if (result.status != 0 || result.outcome != WkiSecureRdmaOutcome::EXACT || TRANSPORT < 1 || TRANSPORT > 2 || SERVICE < 1 || SERVICE > 4)
        return;
    size_t const INDEX = static_cast<size_t>(TRANSPORT - 1) * 4 + SERVICE - 1;
    diagnostic_add(s_client_completions[INDEX], 1);
    diagnostic_add(s_client_bytes[INDEX], result.bytes);
    if (opcode == proto::Opcode::READ) {
        diagnostic_add(s_client_read_completions[INDEX], 1);
        diagnostic_add(s_client_read_bytes[INDEX], result.bytes);
    } else if (opcode == proto::Opcode::WRITE) {
        diagnostic_add(s_client_write_completions[INDEX], 1);
        diagnostic_add(s_client_write_bytes[INDEX], result.bytes);
    }
}
auto peer_index(const WkiPeer* peer) -> size_t {
    uintptr_t const ADDRESS = reinterpret_cast<uintptr_t>(peer), BEGIN = reinterpret_cast<uintptr_t>(g_wki.peers.data());
    if (ADDRESS < BEGIN || ADDRESS - BEGIN >= sizeof(g_wki.peers) || (ADDRESS - BEGIN) % sizeof(WkiPeer) != 0) return WKI_MAX_PEERS;
    return (ADDRESS - BEGIN) / sizeof(WkiPeer);
}
auto same_identity(const WkiChannelIdentity& a, const WkiChannelIdentity& b) -> bool {
    return a.channel == b.channel && a.peer_node_id == b.peer_node_id && a.channel_id == b.channel_id && a.generation == b.generation;
}
auto channel_status(const WkiChannelIdentity& identity) -> int {
    if (identity.channel == nullptr || identity.generation == 0) return -ESTALE;
    if (!identity.channel->lock.try_lock()) return -EAGAIN;
    bool const LIVE = identity.channel->active && identity.channel->peer_node_id == identity.peer_node_id &&
                      identity.channel->channel_id == identity.channel_id && identity.channel->generation == identity.generation;
    identity.channel->lock.unlock();
    return LIVE ? 0 : -ESTALE;
}
auto channel_current(const WkiChannelIdentity& identity) -> bool { return channel_status(identity) == 0; }
auto capture_channel(uint16_t node, uint16_t channel, bool create = false) -> WkiChannelIdentity {
    auto* value = create ? wki_channel_get(node, channel) : wki_channel_lookup(node, channel);
    if (value == nullptr || !value->lock.try_lock()) return {};
    WkiChannelIdentity output{};
    if (value->active && value->peer_node_id == node && value->channel_id == channel && value->generation != 0)
        output = {value, node, channel, value->generation};
    value->lock.unlock();
    return output;
}
auto same_descriptor(const proto::RegionDescriptor& a, const proto::RegionDescriptor& b) -> bool {
    return auth_crypto::constant_time_equal(&a, &b, sizeof(a));
}
auto same_binding(const rdma_registry::Binding& a, const rdma_registry::Binding& b) -> bool {
    return a.owner_node == b.owner_node && a.peer_node == b.peer_node && a.session_id == b.session_id &&
           a.secure_incarnation == b.secure_incarnation && a.transport_cookie == b.transport_cookie &&
           a.transport_generation == b.transport_generation && a.channel_cookie == b.channel_cookie &&
           a.channel_generation == b.channel_generation && a.channel_id == b.channel_id && a.transport == b.transport &&
           a.service == b.service;
}
auto binding_identity(const rdma_registry::Binding& binding) -> WkiChannelIdentity {
    return {reinterpret_cast<WkiChannel*>(binding.channel_cookie), binding.peer_node, binding.channel_id, binding.channel_generation};
}
auto binding_peer(const rdma_registry::Binding& binding) -> WkiPeer* {
    auto* peer = wki_peer_find(binding.peer_node);
    return peer_index(peer) != WKI_MAX_PEERS ? peer : nullptr;
}
auto peer_current(const WkiPeer* peer) -> bool {
    return peer != nullptr && peer_index(peer) != WKI_MAX_PEERS && peer->node_id != 0 && peer->node_id != UINT16_MAX &&
           peer->state == PeerState::CONNECTED && peer->is_direct && peer->auth_session.active &&
           !peer->vfs_reset_rebind_pending.load(std::memory_order_acquire);
}
auto policy_allows(const WkiPeer& peer, proto::Service service, proto::Opcode opcode, uint16_t flags = 0) -> bool {
    uint64_t required = 0;
    switch (service) {
        case proto::Service::ZONE:
            required = wki_policy_bits(WkiPolicy::ZONE_RDMA);
            break;
        case proto::Service::IPC:
            required = wki_policy_bits(WkiPolicy::IPC);
            break;
        case proto::Service::VFS:
            required = wki_policy_bits(opcode == proto::Opcode::READ || (flags & proto::FLAG_RESPONSE) != 0 ? WkiPolicy::VFS_READ
                                                                                                            : WkiPolicy::VFS_WRITE);
            break;
        case proto::Service::BLOCK:
            required = wki_policy_bits(opcode == proto::Opcode::READ ? WkiPolicy::BLOCK_READ : WkiPolicy::BLOCK_WRITE);
            break;
        default:
            return false;
    }
    return (peer.auth_session.policy & required) == required;
}
auto context_current(const PeerContext& context, const WkiPeer& peer, bool confirmed) -> bool {
    return !context.closed.load(std::memory_order_acquire) && context.begun && context.node == peer.node_id &&
           context.auth_generation == peer.auth_session.generation && context.sid == peer.auth_session.session_id &&
           (!confirmed || (context.installed && context.confirmed && context.confirm_sent));
}
auto make_binding(const WkiChannelIdentity& identity, proto::Service service, WkiPeer& peer, const ProviderPin& provider,
                  rdma_registry::Binding& output) -> bool {
    EngineGuard const GUARD(s_lock);
    if (!GUARD || s_stopping.load(std::memory_order_acquire) || !context_current(s_peers[peer_index(&peer)], peer, true)) return false;
    auto& context = s_peers[peer_index(&peer)];
    if (context.provider_cookie.load(std::memory_order_acquire) != reinterpret_cast<uintptr_t>(provider.transport) ||
        context.provider_generation.load(std::memory_order_acquire) != provider.generation ||
        (context.local.services & context.remote.services & proto::service_bit(service)) == 0 ||
        (context.local.transports & context.remote.transports & proto::transport_bit(provider.kind)) == 0)
        return false;
    output = {g_wki.my_node_id,
              peer.node_id,
              context.sid,
              context.incarnation,
              reinterpret_cast<uintptr_t>(provider.transport),
              provider.generation,
              reinterpret_cast<uintptr_t>(identity.channel),
              identity.generation,
              identity.channel_id,
              provider.kind,
              service};
    return true;
}
auto binding_status(const rdma_registry::Binding& binding, WkiPeer& peer, const ProviderPin& provider) -> int {
    if (!peer_current(&peer) || peer.node_id != binding.peer_node || peer.transport != provider.transport ||
        provider.kind != binding.transport || provider.generation != binding.transport_generation)
        return -ESTALE;
    int const PROVIDER = provider.status();
    if (PROVIDER != 0) return PROVIDER;
    int const CHANNEL = channel_status(binding_identity(binding));
    if (CHANNEL != 0) return CHANNEL;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return -EAGAIN;
    return !s_stopping.load(std::memory_order_acquire) && context_current(s_peers[peer_index(&peer)], peer, true) &&
                   s_peers[peer_index(&peer)].incarnation == binding.secure_incarnation &&
                   binding.session_id == peer.auth_session.session_id
               ? 0
               : -ESTALE;
}
auto binding_current(const rdma_registry::Binding& binding, WkiPeer& peer, const ProviderPin& provider) -> bool {
    return binding_status(binding, peer, provider) == 0;
}
void wake_worker() {
    sched::task::Task* worker = nullptr;
    {
        EngineGuard const GUARD(s_lock);
        if (GUARD && s_worker != nullptr && s_worker->try_acquire()) worker = s_worker;
    }
    if (worker != nullptr) {
        sched::kern_wake(worker);
        worker->release();
    }
}
auto deadline_after(uint64_t now, uint64_t interval) -> uint64_t { return interval > UINT64_MAX - now ? UINT64_MAX : now + interval; }
auto command_token(const char* command, const char* wanted) -> bool {
    if (command == nullptr) return false;
    size_t const LENGTH = std::strlen(wanted);
    while (*command != '\0') {
        while (*command == ' ' || *command == '\t') ++command;
        auto* begin = command;
        while (*command != '\0' && *command != ' ' && *command != '\t') ++command;
        if (static_cast<size_t>(command - begin) == LENGTH && std::memcmp(begin, wanted, LENGTH) == 0) return true;
    }
    return false;
}
auto control_size(MsgType type) -> size_t {
    switch (type) {
        case MsgType::SECURE_RDMA_OFFER:
            return sizeof(proto::Offer);
        case MsgType::SECURE_RDMA_CONFIRM:
            return sizeof(rdma_control::Confirm);
        case MsgType::SECURE_RDMA_GRANT:
        case MsgType::SECURE_RDMA_REVOKE:
            return sizeof(proto::RegionDescriptor);
        case MsgType::SECURE_RDMA_ARM:
        case MsgType::SECURE_RDMA_CANCEL:
            return sizeof(rdma_control::Arm);
        case MsgType::SECURE_RDMA_ARM_ACK:
            return sizeof(rdma_control::ArmAck);
        case MsgType::SECURE_RDMA_RESULT:
            return sizeof(rdma_control::Result);
        default:
            return 0;
    }
}
auto request_from(const rdma_control::Arm& arm, uint16_t peer) -> WkiSecureRdmaRequest {
    WkiSecureRdmaRequest output{};
    output.grant = arm.grant;
    output.operation_id = arm.operation_id;
    output.receive_ticket = arm.receive_ticket;
    output.offset = arm.offset;
    output.length = arm.length;
    output.peer_node = peer;
    output.flags = arm.flags;
    output.opcode = static_cast<proto::Opcode>(arm.opcode);
    output.intent_length = arm.intent_length;
    output.native_intent = arm.native_intent;
    return output;
}
auto header_from(const rdma_control::Arm& arm, const rdma_registry::Binding& binding, proto::Opcode opcode, bool outgoing,
                 uint32_t fragment = 0, uint32_t payload = 0, uint32_t total = UINT32_MAX, int32_t status = 0) -> proto::Header {
    proto::Header header{};
    header.transport = static_cast<uint8_t>(binding.transport);
    header.service = static_cast<uint8_t>(binding.service);
    header.opcode = static_cast<uint8_t>(opcode);
    header.src_node = outgoing ? binding.owner_node : binding.peer_node;
    header.dst_node = outgoing ? binding.peer_node : binding.owner_node;
    header.channel_id = binding.channel_id;
    header.flags = opcode == proto::Opcode::READ_DATA || opcode == proto::Opcode::READ_END || opcode == proto::Opcode::WRITE_END
                       ? proto::FLAG_RESPONSE
                       : arm.flags;
    header.session_id = binding.session_id;
    header.region_generation = arm.grant.generation;
    header.capability = arm.grant.capability;
    header.region_offset = arm.offset;
    header.operation_id = arm.operation_id;
    header.binding_generation = arm.grant.binding_generation;
    header.receive_ticket = arm.receive_ticket;
    header.fragment_offset = fragment;
    header.total_length = total == UINT32_MAX ? arm.length : total;
    header.payload_length = payload;
    header.rkey = arm.grant.rkey;
    header.status = status;
    return header;
}
auto valid_arm(const rdma_control::Arm& arm, const rdma_registry::Binding& binding) -> bool {
    if (arm.version != proto::VERSION || arm.reserved != 0 || arm.reserved2 != 0 || arm.reserved3 != 0 ||
        arm.intent_length > arm.native_intent.size())
        return false;
    for (size_t i = arm.intent_length; i < arm.native_intent.size(); ++i)
        if (arm.native_intent[i] != 0) return false;
    auto const OPCODE = static_cast<proto::Opcode>(arm.opcode);
    if (OPCODE != proto::Opcode::READ && OPCODE != proto::Opcode::WRITE && OPCODE != proto::Opcode::NOTIFY) return false;
    auto header =
        header_from(arm, binding, OPCODE, false, 0, OPCODE == proto::Opcode::WRITE ? std::min(arm.length, proto::FRAGMENT_BYTES) : 0);
    header.counter = 1;  // Provenance is the fully authenticated ARM control, not a synthetic AEAD/replay admission.
    return proto::validate_header(header, proto::HEADER_BYTES + header.payload_length) == proto::Drop::NONE &&
           proto::validate_grant(header, arm.grant) == proto::Drop::NONE;
}
auto model_result(const WkiSecureRdmaResult& result) -> ops::Result {
    return {result.status, result.bytes, static_cast<ops::Outcome>(result.outcome)};
}
auto public_result(const ops::Result& result) -> WkiSecureRdmaResult {
    return {result.status, result.bytes, static_cast<WkiSecureRdmaOutcome>(result.outcome)};
}
auto send_control(size_t index, MsgType type, const void* payload, size_t size) -> bool {
    if (index >= s_peers.size() || size != control_size(type)) return false;
    auto* peer = &g_wki.peers[index];
    PeerAdmission admission;
    if (!admission.acquire(peer) || !peer_current(peer)) return false;
    WkiChannelIdentity identity{};
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD || !context_current(s_peers[index], *peer, false)) return false;
        identity = s_peers[index].control;
    }
    if (!channel_current(identity)) return false;
    admission.reset();  // Reliable send reacquires its short origin admission.
    return wki_send_on_channel_identity(identity, type, payload, static_cast<uint16_t>(size)) == WKI_OK;
}
auto native_status(proto::Drop reason) -> int {
    switch (reason) {
        case proto::Drop::NONE:
            return 0;
        case proto::Drop::PRESSURE:
            return -EAGAIN;
        case proto::Drop::RETIRED:
        case proto::Drop::SESSION:
            return -ESTALE;
        case proto::Drop::PERMISSION:
        case proto::Drop::GRANT:
            return -EACCES;
        case proto::Drop::COUNTER:
            return -EOVERFLOW;
        default:
            return -EINVAL;
    }
}
auto provider_status(WkiTransport* expected, uint64_t generation = 0) -> int {
    if (!g_wki.transport_lock.try_lock()) return -EAGAIN;
    int status = -ESTALE;
    for (auto* candidate = g_wki.transports; candidate != nullptr; candidate = candidate->next) {
        if (candidate != expected) continue;
        if (candidate->chaos_unregistering.load(std::memory_order_acquire) || candidate->registration_generation == 0 ||
            (generation != 0 && generation != candidate->registration_generation))
            status = -ESTALE;
        else if (candidate->secure_tx == nullptr || proto::transport_bit(candidate->secure_kind) == 0 ||
                 candidate->mtu < proto::RECORD_BYTES)
            status = -ENOTSUP;
        else
            status = 0;
        break;
    }
    g_wki.transport_lock.unlock();
    return status;
}
auto capture_authority(const WkiChannelIdentity& identity, proto::Service service, proto::Opcode opcode, uint16_t flags,
                       rdma_registry::Binding& binding, ProviderPin& pin) -> int {
    if (!s_initialized.load(std::memory_order_acquire) || (s_services & proto::service_bit(service)) == 0) return -ENOTSUP;
    if (s_stopping.load(std::memory_order_acquire)) return -ENOTCONN;
    auto* peer = wki_peer_find(identity.peer_node_id);
    PeerAdmission admission;
    if (peer_index(peer) == WKI_MAX_PEERS) return -ENOTCONN;
    if (!admission.acquire(peer)) return -EAGAIN;
    if (!peer_current(peer)) return -ENOTCONN;
    int const CHANNEL = channel_status(identity);
    if (CHANNEL != 0) return CHANNEL;
    if (!policy_allows(*peer, service, opcode, flags)) return -EACCES;
    if (!pin.acquire(peer->transport)) {
        int const STATUS = provider_status(peer->transport);
        return STATUS == 0 ? -EAGAIN : STATUS;
    }
    if (!make_binding(identity, service, *peer, pin, binding)) {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) return -EAGAIN;
        auto const& context = s_peers[peer_index(peer)];
        if (context.begun && !context_current(context, *peer, false)) return -ESTALE;
        if (context_current(context, *peer, true) &&
            (context.local.services & context.remote.services & proto::service_bit(service)) != 0 &&
            (context.local.transports & context.remote.transports & proto::transport_bit(pin.kind)) != 0)
            return -EAGAIN;
        return -ENOTSUP;
    }
    return 0;
}
auto reserve_node_id(uint64_t& next) -> uint64_t {
    if (next == 0 || next > NODE_SEQUENCE_MAX || g_wki.my_node_id == 0 || g_wki.my_node_id == UINT16_MAX) return 0;
    uint64_t const SEQUENCE = next;
    next = next == NODE_SEQUENCE_MAX ? 0 : next + 1;
    return (static_cast<uint64_t>(g_wki.my_node_id) << 48) | SEQUENCE;
}
auto sender_id(uint64_t value, uint16_t node) -> bool {
    return static_cast<uint16_t>(value >> 48) == node && (value & NODE_SEQUENCE_MAX) != 0;
}
auto same_arm(const rdma_control::Arm& a, const rdma_control::Arm& b) -> bool {
    return auth_crypto::constant_time_equal(&a, &b, sizeof(a));
}
auto valid_native_result(const WkiSecureRdmaResult& result, const rdma_control::Arm& arm) -> bool {
    if (result.status > 0 || result.bytes > arm.length || result.outcome > WkiSecureRdmaOutcome::UNCERTAIN) return false;
    if (result.outcome == WkiSecureRdmaOutcome::NO_EFFECT) return result.bytes == 0;
    if (result.outcome == WkiSecureRdmaOutcome::EXACT) {
        return result.status == 0 && (result.bytes == arm.length || arm.grant.service == static_cast<uint8_t>(proto::Service::VFS) ||
                                      (arm.opcode == static_cast<uint8_t>(proto::Opcode::WRITE) &&
                                       arm.grant.service == static_cast<uint8_t>(proto::Service::IPC)));
    }
    return true;
}
auto model_spec(const Job& job, ops::Role role) -> ops::Spec {
    return {role,           job.binding,    job.arm.grant, job.arm.operation_id, job.arm.receive_ticket,
            job.arm.offset, job.arm.length, job.arm.flags, job.deadline};
}
auto job_find(const rdma_registry::Binding& binding, const rdma_control::Arm& arm) -> Job* {
    for (auto& job : s_jobs) {
        if (job.state.load(std::memory_order_acquire) != JobState::FREE && same_binding(binding, job.binding) &&
            job.arm.operation_id == arm.operation_id && job.arm.receive_ticket == arm.receive_ticket)
            return &job;
    }
    return nullptr;
}
void job_clear_locked(Job& job) {
    // No native callback or network I/O here. Every local Work has retired.
    job.work.reset();
    job.operation.reset();
    job.provider.reset();
    if (job.prepare_owner != nullptr) std::exchange(job.prepare_owner, nullptr)->release();
    job.binding = {};
    job.arm = {};
    job.result = {};
    job.region = REGIONS;
    job.outgoing = false;
    job.frontend = false;
    job.arm_succeeded = false;
    job.ack_pending = false;
    job.ack_seen = false;
    job.effect_done = false;
    job.end_sent = false;
    job.native_result_seen = false;
    job.result_sent = false;
    job.timeout_logged = false;
    job.ack_status = 0;
    job.deadline = 0;
    job.retry_at = 0;
    job.consumer_generation = 0;
    job.position = 0;
    job.cancel.store(false, std::memory_order_relaxed);
    job.done.store(false, std::memory_order_relaxed);
    job.state.store(JobState::FREE, std::memory_order_release);
}
void service_pass();
void worker_entry();
void maintain_peer(size_t index, uint64_t now) {
    auto& context = s_peers[index];
    auto* peer = &g_wki.peers[index];
    if (context.closed.load(std::memory_order_acquire)) {
        bool busy = false;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) return;
            for (auto& region : s_regions)
                if (region.live && region.binding.peer_node == context.node && region.binding.session_id == context.sid) {
                    region.closing = true;
                    region.grant_pending = false;
                }
            for (auto& remote : s_remote)
                if (remote.live && remote.binding.peer_node == context.node && remote.binding.session_id == context.sid)
                    remote.live = false;
            for (auto& job : s_jobs)
                if (job.state.load(std::memory_order_acquire) != JobState::FREE && job.binding.peer_node == context.node &&
                    job.binding.secure_incarnation == context.incarnation) {
                    job.cancel.store(true, std::memory_order_release);
                    busy = true;
                }
        }
        if (busy) return;
        if (context.session.retire() != proto::Drop::NONE) return;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) return;
            context.begun = false;
            context.installed = false;
            context.confirmed = false;
            context.confirm_sent = false;
            context.sid = {};
            context.local = {};
            context.remote = {};
            context.negotiation = {};
            context.arm_replay = {};
            context.control = {};
            context.provider_cookie.store(0, std::memory_order_release);
            context.provider_generation.store(0, std::memory_order_release);
        }
    }
    if (s_stopping.load(std::memory_order_acquire)) return;
    PeerAdmission admission;
    ProviderPin provider;
    if (!admission.acquire(peer) || !peer_current(peer) || !provider.acquire(peer->transport)) return;
    auto identity = capture_channel(peer->node_id, WKI_CHAN_SECURE_RDMA, true);
    if (identity.channel == nullptr) return;
    bool begun = false;
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) return;
        begun = context.begun;
        if (begun && (!context_current(context, *peer, false) ||
                      context.provider_cookie.load(std::memory_order_acquire) != reinterpret_cast<uintptr_t>(provider.transport) ||
                      context.provider_generation.load(std::memory_order_acquire) != provider.generation)) {
            context.closed.store(true, std::memory_order_release);
            return;
        }
    }
    if (!begun) {
        // Pipe exports can publish GRANT before TASK_SUBMIT imports their FDs.
        // Establish this fixed receiver channel before advertising support;
        // GRANT binding itself remains lookup-only and generation-qualified.
        if ((s_services & proto::service_bit(proto::Service::IPC)) != 0 &&
            capture_channel(peer->node_id, WKI_CHAN_IPC_DATA, true).channel == nullptr)
            return;
        proto::Offer local{};
        local.transports = proto::transport_bit(provider.kind);
        local.services = s_services;
        local.record_bytes = proto::RECORD_BYTES;
        local.session_id = peer->auth_session.session_id;
        uint64_t incarnation = 0;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) return;
            incarnation = proto::reserve(s_next_incarnation);
        }
        if (incarnation == 0 || !ker::mod::random::entropy::get_bytes(local.nonce.data() + 8, 8)) return;
        for (size_t byte = 0; byte < 8; ++byte) local.nonce[byte] = static_cast<uint8_t>(incarnation >> (byte * 8));
        auto const BEGUN = context.session.begin(g_wki.my_node_id, peer->node_id, incarnation, local);
        if (BEGUN != proto::Drop::NONE) {
            dropped(BEGUN);
            return;
        }
        // Provider unregister publishes its close under this same gate before
        // scanning the atomic ownership pair. Either it observes this context,
        // or publication observes the already-closed provider and fails.
        EngineGuard const TRANSPORT_GUARD(g_wki.transport_lock);
        if (!TRANSPORT_GUARD || provider.transport->chaos_unregistering.load(std::memory_order_acquire) ||
            provider.transport->registration_generation != provider.generation) {
            context.closed.store(true, std::memory_order_release);
            return;
        }
        EngineGuard const GUARD(s_lock);
        if (!GUARD) {
            context.closed.store(true, std::memory_order_release);
            return;
        }
        context.node = peer->node_id;
        context.auth_generation = peer->auth_session.generation;
        context.sid = local.session_id;
        context.local = local;
        context.incarnation = incarnation;
        context.control = identity;
        if (context.remote_history_sid != local.session_id) {
            context.remote_history_sid = local.session_id;
            context.remote_history_incarnation = 0;
        }
        context.retry_at = 0;
        context.begun = true;
        context.provider_generation.store(provider.generation, std::memory_order_release);
        context.provider_cookie.store(reinterpret_cast<uintptr_t>(provider.transport), std::memory_order_release);
        context.record_incarnation.store(incarnation, std::memory_order_release);
        context.closed.store(false, std::memory_order_release);
    }
    proto::Offer local{};
    rdma_control::Confirm confirm{};
    bool installed = false, due = false;
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD || !context_current(context, *peer, false)) return;
        due = !(context.confirmed && context.confirm_sent) && (context.retry_at == 0 || now >= context.retry_at);
        if (due) {
            context.retry_at = deadline_after(now, RETRY_US);
            local = context.local;
            installed = context.installed;
            confirm.session_id = context.sid;
            confirm.negotiation = context.negotiation;
            confirm.transports = context.local.transports & context.remote.transports;
            confirm.services = context.local.services & context.remote.services;
        }
    }
    admission.reset();
    if (!due) return;
    static_cast<void>(send_control(index, MsgType::SECURE_RDMA_OFFER, &local, sizeof(local)));
    if (installed && send_control(index, MsgType::SECURE_RDMA_CONFIRM, &confirm, sizeof(confirm))) {
        EngineGuard const GUARD(s_lock);
        if (GUARD && context.sid == confirm.session_id && context.negotiation == confirm.negotiation) context.confirm_sent = true;
    }
}
void process_offer(ControlCell& cell, WkiPeer& peer) {
    auto& context = s_peers[cell.peer];
    proto::Offer offer{};
    std::memcpy(&offer, cell.payload.data(), sizeof(offer));
    if (offer.session_id != cell.sid || !proto::valid_offer(offer)) {
        dropped(proto::Drop::SESSION);
        return;
    }
    uint64_t remote_incarnation = 0;
    for (size_t byte = 0; byte < 8; ++byte) remote_incarnation |= static_cast<uint64_t>(offer.nonce[byte]) << (byte * 8);
    if (remote_incarnation == 0) {
        dropped(proto::Drop::SESSION);
        return;
    }
    proto::Offer local{};
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) {
            cell.retry = true;
            return;
        }
        if (!context_current(context, peer, false)) return;
        // A retired secure incarnation cannot reinstall an old remote offer
        // under the same control SID. Delayed older offers do not close the
        // newer context; exact live duplicates retain all counters/replay.
        if (context.remote_history_sid == cell.sid && context.remote_history_incarnation != 0) {
            if (remote_incarnation < context.remote_history_incarnation ||
                (remote_incarnation == context.remote_history_incarnation && !context.installed)) {
                dropped(proto::Drop::TOO_OLD);
                return;
            }
            if (remote_incarnation == context.remote_history_incarnation &&
                !auth_crypto::constant_time_equal(&offer, &context.remote, sizeof(offer))) {
                context.closed.store(true, std::memory_order_release);
                dropped(proto::Drop::SESSION);
                return;
            }
            // The reliable control channel already owns delivery/retransmit.
            // An exact installed duplicate must not rearm negotiation or
            // reset directional keys, counters, replay or native bindings.
            if (remote_incarnation == context.remote_history_incarnation && context.installed) return;
        }
        local = context.local;
    }
    auto const INSTALLED = context.session.install(offer, peer.auth_session.tx_key, peer.auth_session.rx_key);
    if (INSTALLED != proto::Drop::NONE) {
        dropped(INSTALLED);
        if (INSTALLED == proto::Drop::PRESSURE)
            cell.retry = true;
        else
            context.closed.store(true, std::memory_order_release);
        return;
    }
    auth_crypto::Digest digest{};
    if (!proto::negotiation_digest(g_wki.my_node_id, peer.node_id, local, offer, digest)) return;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) {
        cell.retry = true;
        return;
    }
    if (!context_current(context, peer, false)) return;
    context.remote = offer;
    context.remote_history_sid = cell.sid;
    context.remote_history_incarnation = remote_incarnation;
    context.negotiation = digest;
    context.installed = true;
    context.retry_at = 0;
}
void process_confirm(ControlCell& cell, WkiPeer& peer) {
    rdma_control::Confirm confirm{};
    std::memcpy(&confirm, cell.payload.data(), sizeof(confirm));
    EngineGuard const GUARD(s_lock);
    if (!GUARD) {
        cell.retry = true;
        return;
    }
    auto& context = s_peers[cell.peer];
    if (!context_current(context, peer, false) || !context.installed || confirm.version != proto::VERSION || confirm.reserved != 0 ||
        confirm.session_id != context.sid || confirm.negotiation != context.negotiation ||
        confirm.transports != (context.local.transports & context.remote.transports) ||
        confirm.services != (context.local.services & context.remote.services)) {
        dropped(proto::Drop::SESSION);
        return;
    }
    context.confirmed = true;
}
auto control_binding(ControlCell& cell, WkiPeer& peer, const proto::RegionDescriptor& descriptor, ProviderPin& provider,
                     rdma_registry::Binding& binding) -> bool {
    auto* channel = wki_channel_lookup(peer.node_id, descriptor.channel_id);
    if (channel == nullptr) return false;
    WkiChannelIdentity identity{};
    {
        EngineGuard const GUARD(channel->lock);
        if (!GUARD) {
            cell.retry = true;
            return false;
        }
        if (!channel->active || channel->generation == 0 || channel->peer_node_id != peer.node_id ||
            channel->channel_id != descriptor.channel_id)
            return false;
        identity = {channel, peer.node_id, descriptor.channel_id, channel->generation};
    }
    if (!provider.acquire(peer.transport)) {
        int const STATUS = provider_status(peer.transport);
        if (STATUS == 0 || STATUS == -EAGAIN) cell.retry = true;
        return false;
    }
    if (static_cast<uint8_t>(provider.kind) != descriptor.transport || descriptor.session_id != cell.sid) return false;
    auto const SERVICE = static_cast<proto::Service>(descriptor.service);
    if (make_binding(identity, SERVICE, peer, provider, binding)) return true;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) {
        cell.retry = true;
        return false;
    }
    auto const& context = s_peers[cell.peer];
    if (context_current(context, peer, false) && context.installed && context.confirmed &&
        context.provider_cookie.load(std::memory_order_acquire) == reinterpret_cast<uintptr_t>(provider.transport) &&
        context.provider_generation.load(std::memory_order_acquire) == provider.generation &&
        (context.local.services & context.remote.services & proto::service_bit(SERVICE)) != 0 &&
        (context.local.transports & context.remote.transports & proto::transport_bit(provider.kind)) != 0 &&
        !s_stopping.load(std::memory_order_acquire))
        cell.retry = true;
    return false;
}
auto canonical_descriptor(const proto::RegionDescriptor& descriptor, const rdma_registry::Binding& binding) -> bool {
    rdma_control::Arm probe{};
    probe.grant = descriptor;
    probe.operation_id = 1;
    probe.receive_ticket = 1;
    auto const OPCODE = (descriptor.permissions & proto::PERMIT_READ) != 0    ? proto::Opcode::READ
                        : (descriptor.permissions & proto::PERMIT_WRITE) != 0 ? proto::Opcode::WRITE
                                                                              : proto::Opcode::NOTIFY;
    probe.opcode = static_cast<uint8_t>(OPCODE);
    probe.length = OPCODE == proto::Opcode::NOTIFY ? 0 : 1;
    return valid_arm(probe, binding);
}
void process_grant(ControlCell& cell, WkiPeer& peer, bool revoke) {
    proto::RegionDescriptor descriptor{};
    std::memcpy(&descriptor, cell.payload.data(), sizeof(descriptor));
    ProviderPin provider;
    rdma_registry::Binding binding{};
    if (!control_binding(cell, peer, descriptor, provider, binding) || !canonical_descriptor(descriptor, binding)) {
        dropped(cell.retry ? proto::Drop::PRESSURE : proto::Drop::GRANT);
        return;
    }
    EngineGuard const GUARD(s_lock);
    if (!GUARD) {
        cell.retry = true;
        dropped(proto::Drop::PRESSURE);
        return;
    }
    RemoteRegion* available = nullptr;
    for (auto& region : s_remote) {
        if (!region.live && available == nullptr) available = &region;
        if (!region.live || !same_binding(binding, region.binding) || region.descriptor.rkey != descriptor.rkey) continue;
        if (!same_descriptor(descriptor, region.descriptor)) {
            dropped(proto::Drop::GRANT);
            return;
        }
        if (revoke) {
            region.live = false;
            for (auto& job : s_jobs)
                if (job.state.load(std::memory_order_acquire) != JobState::FREE && job.outgoing && same_binding(binding, job.binding) &&
                    same_descriptor(descriptor, job.arm.grant))
                    job.cancel.store(true, std::memory_order_release);
        }
        return;
    }
    if (revoke) return;
    if (available == nullptr) {
        dropped(proto::Drop::PRESSURE);
        return;
    }
    available->binding = binding;
    available->descriptor = descriptor;
    available->live = true;
}
auto process_arm(ControlCell& cell, WkiPeer& peer, uint64_t now) -> int {
    rdma_control::Arm arm{};
    std::memcpy(&arm, cell.payload.data(), sizeof(arm));
    ProviderPin provider;
    rdma_registry::Binding binding{};
    if (!sender_id(arm.operation_id, peer.node_id) || !sender_id(arm.receive_ticket, peer.node_id) ||
        !control_binding(cell, peer, arm.grant, provider, binding) || !valid_arm(arm, binding))
        return -EACCES;
    if (!policy_allows(peer, binding.service, static_cast<proto::Opcode>(arm.opcode), arm.flags)) return -EACCES;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return -EAGAIN;
    auto* duplicate = job_find(binding, arm);
    if (duplicate != nullptr) {
        if (duplicate->outgoing || !same_arm(duplicate->arm, arm)) return -EACCES;
        if (duplicate->arm_succeeded || duplicate->ack_status != 0) duplicate->ack_pending = true;
        return 0;
    }
    auto replay = s_peers[cell.peer].arm_replay;
    auto const REPLAY = proto::replay_admit(replay, arm.operation_id);
    if (REPLAY != proto::Drop::NONE) return -ESTALE;
    auto region = std::find_if(s_regions.begin(), s_regions.end(), [&](auto const& value) {
        return value.live && !value.closing && same_binding(value.binding, binding) && same_descriptor(value.descriptor, arm.grant);
    });
    if (region == s_regions.end() || region->references == UINT32_MAX) return -EACCES;
    auto job = std::find_if(s_jobs.begin(), s_jobs.end(),
                            [](auto const& value) { return value.state.load(std::memory_order_acquire) == JobState::FREE; });
    if (job == s_jobs.end()) return -EAGAIN;
    auto header = header_from(arm, binding, static_cast<proto::Opcode>(arm.opcode), false, 0,
                              arm.opcode == static_cast<uint8_t>(proto::Opcode::WRITE) ? std::min(arm.length, proto::FRAGMENT_BYTES) : 0);
    header.counter = 1;
    rdma_registry::Lease lease;
    auto const ADMITTED = s_registry.admit(binding, header, lease);
    if (ADMITTED != proto::Drop::NONE) return native_status(ADMITTED);
    job->binding = binding;
    job->arm = arm;
    job->deadline = deadline_after(now, OPERATION_US);
    auto const ROLE = arm.opcode == static_cast<uint8_t>(proto::Opcode::READ)    ? ops::Role::SERVE_READ
                      : arm.opcode == static_cast<uint8_t>(proto::Opcode::WRITE) ? ops::Role::RECEIVE_WRITE
                                                                                 : ops::Role::RECEIVE_NOTIFY;
    auto const PREPARED = s_operations.prepare(model_spec(*job, ROLE), std::move(lease), job->operation);
    if (PREPARED != proto::Drop::NONE) {
        job_clear_locked(*job);
        return native_status(PREPARED);
    }
    job->region = static_cast<size_t>(region - s_regions.begin());
    job->provider = std::move(provider);
    ++region->references;
    s_peers[cell.peer].arm_replay = replay;
    job->state.store(JobState::IN_ARM, std::memory_order_release);
    return 0;
}
void process_ack_or_result(ControlCell& cell, WkiPeer& peer) {
    rdma_control::Arm arm{};
    std::memcpy(&arm, cell.payload.data(), sizeof(arm));
    ProviderPin provider;
    rdma_registry::Binding binding{};
    if (!control_binding(cell, peer, arm.grant, provider, binding)) return;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) {
        cell.retry = true;
        return;
    }
    auto* job = job_find(binding, arm);
    if (job == nullptr || !same_arm(job->arm, arm)) {
        dropped(proto::Drop::OPERATION);
        return;
    }
    if (cell.type == MsgType::SECURE_RDMA_CANCEL) {
        if (!job->outgoing) job->cancel.store(true, std::memory_order_release);
        return;
    }
    if (!job->outgoing) return;
    if (cell.type == MsgType::SECURE_RDMA_ARM_ACK) {
        rdma_control::ArmAck ack{};
        std::memcpy(&ack, cell.payload.data(), sizeof(ack));
        if (ack.status > 0 || (job->ack_seen && ack.status != job->ack_status)) {
            dropped(proto::Drop::OPERATION);
            return;
        }
        job->ack_seen = true;
        job->ack_status = ack.status;
    } else {
        rdma_control::Result result{};
        std::memcpy(&result, cell.payload.data(), sizeof(result));
        WkiSecureRdmaResult native{result.status, result.bytes, static_cast<WkiSecureRdmaOutcome>(result.outcome)};
        if (arm.opcode != static_cast<uint8_t>(proto::Opcode::WRITE) || !valid_native_result(native, arm) ||
            std::any_of(result.reserved.begin(), result.reserved.end(), [](auto byte) { return byte != 0; }) ||
            (job->state.load(std::memory_order_acquire) != JobState::OUT_SEND &&
             job->state.load(std::memory_order_acquire) != JobState::OUT_WAIT)) {
            dropped(proto::Drop::OPERATION);
            return;
        }
        if (job->native_result_seen) {
            if (job->result.status != native.status || job->result.bytes != native.bytes || job->result.outcome != native.outcome)
                dropped(proto::Drop::OPERATION);
            return;
        }
        job->result = native;
        job->native_result_seen = true;
    }
}
void process_control_cell(ControlCell& cell, uint64_t now) {
    auto* peer = &g_wki.peers[cell.peer];
    PeerAdmission admission;
    if (!admission.acquire(peer)) {
        cell.retry = true;
        return;
    }
    int const CHANNEL = channel_status(cell.identity);
    if (CHANNEL == -EAGAIN) {
        cell.retry = true;
        return;
    }
    if (!peer_current(peer) || peer->auth_session.generation != cell.auth_generation || peer->auth_session.session_id != cell.sid ||
        CHANNEL != 0) {
        dropped(proto::Drop::SESSION);
        return;
    }
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) {
            cell.retry = true;
            return;
        }
        if (s_peers[cell.peer].begun && !same_identity(s_peers[cell.peer].control, cell.identity)) {
            dropped(proto::Drop::SESSION);
            return;
        }
        if (cell.type != MsgType::SECURE_RDMA_OFFER && cell.type != MsgType::SECURE_RDMA_CONFIRM &&
            (s_peers[cell.peer].incarnation != cell.secure_incarnation ||
             s_peers[cell.peer].provider_cookie.load(std::memory_order_acquire) != cell.provider_cookie ||
             s_peers[cell.peer].provider_generation.load(std::memory_order_acquire) != cell.provider_generation)) {
            dropped(proto::Drop::SESSION);
            return;
        }
    }
    int arm_status = 0;
    switch (cell.type) {
        case MsgType::SECURE_RDMA_OFFER:
            process_offer(cell, *peer);
            break;
        case MsgType::SECURE_RDMA_CONFIRM:
            process_confirm(cell, *peer);
            break;
        case MsgType::SECURE_RDMA_GRANT:
            process_grant(cell, *peer, false);
            break;
        case MsgType::SECURE_RDMA_REVOKE:
            process_grant(cell, *peer, true);
            break;
        case MsgType::SECURE_RDMA_ARM:
            arm_status = process_arm(cell, *peer, now);
            // ARM pressure is an explicit no-effect negative acknowledgement;
            // the sender may retry this exact ticket without native re-arming.
            if (cell.retry) {
                cell.retry = false;
                arm_status = -EAGAIN;
            }
            break;
        case MsgType::SECURE_RDMA_ARM_ACK:
        case MsgType::SECURE_RDMA_RESULT:
        case MsgType::SECURE_RDMA_CANCEL:
            process_ack_or_result(cell, *peer);
            break;
        default:
            dropped(proto::Drop::FIELD);
            break;
    }
    admission.reset();
    if (cell.type == MsgType::SECURE_RDMA_ARM && arm_status != 0) {
        rdma_control::ArmAck ack{};
        std::memcpy(&ack.request, cell.payload.data(), sizeof(ack.request));
        ack.status = arm_status;
        static_cast<void>(send_control(cell.peer, MsgType::SECURE_RDMA_ARM_ACK, &ack, sizeof(ack)));
    }
}
// The retained peer admission protects lookup; channel storage remains in its
// fixed pool. Capture one allocation under its guard and never recapture a
// successor after pressure. Operations::admit still checks the full native grant.
auto record_binding_status(RecordCell& cell, WkiPeer& peer) -> int {
    if (cell.binding.channel_cookie == 0) {
        proto::Header hint{};
        std::memcpy(&hint, cell.record.data(), sizeof(hint));
        auto* channel = wki_channel_lookup(peer.node_id, hint.channel_id);
        if (channel == nullptr) return -ESTALE;
        EngineGuard const GUARD(channel->lock);
        if (!GUARD) return -EAGAIN;
        if (!channel->active || channel->peer_node_id != peer.node_id || channel->channel_id != hint.channel_id || channel->generation == 0)
            return -ESTALE;
        cell.binding = {g_wki.my_node_id,
                        peer.node_id,
                        cell.sid,
                        cell.secure_incarnation,
                        reinterpret_cast<uintptr_t>(cell.provider.transport),
                        cell.provider.generation,
                        reinterpret_cast<uintptr_t>(channel),
                        channel->generation,
                        channel->channel_id,
                        cell.provider.kind,
                        static_cast<proto::Service>(hint.service)};
    }
    return binding_status(cell.binding, peer, cell.provider);
}
void retry_record(RecordCell& cell) {
    cell.retry = true;
    dropped(proto::Drop::PRESSURE);
}
class RecordDecodeReset final {
   public:
    ~RecordDecodeReset() { auth_crypto::secure_erase(s_record_decode.data(), s_record_decode.size()); }
};
void process_record_cell(RecordCell& cell) {
    // These atomic close gates need no peer/provider admission. Teardown must
    // drain retained ciphertext/plaintext even while those admissions are shut.
    if (s_stopping.load(std::memory_order_acquire) || s_peers[cell.peer].closed.load(std::memory_order_acquire) ||
        cell.provider.transport->chaos_unregistering.load(std::memory_order_acquire)) {
        dropped(proto::Drop::RETIRED);
        return;
    }
    auto* peer = &g_wki.peers[cell.peer];
    PeerAdmission admission;
    if (!admission.acquire(peer)) {
        retry_record(cell);
        return;
    }
    if (!peer_current(peer) || peer->auth_session.generation != cell.auth_generation || peer->auth_session.session_id != cell.sid ||
        peer->transport != cell.provider.transport) {
        dropped(proto::Drop::SESSION);
        return;
    }
    int const PROVIDER = cell.provider.status();
    if (PROVIDER == -EAGAIN) {
        retry_record(cell);
        return;
    }
    if (PROVIDER != 0) {
        dropped(proto::Drop::RETIRED);
        return;
    }
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) {
            retry_record(cell);
            return;
        }
        auto const& context = s_peers[cell.peer];
        if (!context_current(context, *peer, true) || context.incarnation != cell.secure_incarnation ||
            context.provider_cookie.load(std::memory_order_acquire) != reinterpret_cast<uintptr_t>(cell.provider.transport) ||
            context.provider_generation.load(std::memory_order_acquire) != cell.provider.generation) {
            dropped(proto::Drop::SESSION);
            return;
        }
    }
    int const BINDING = record_binding_status(cell, *peer);
    if (BINDING == -EAGAIN) {
        retry_record(cell);
        return;
    }
    if (BINDING != 0) {
        dropped(proto::Drop::RETIRED);
        return;
    }
    if (!cell.decoded) {
        // The sole service owner uses this fixed private copy. Preserve original
        // ciphertext on Session pressure; its established failure wipe remains.
        RecordDecodeReset const RESET;
        std::memcpy(s_record_decode.data(), cell.record.data(), cell.size);
        proto::Header header{};
        auto const DECODED = s_peers[cell.peer].session.receive(cell.provider.kind, s_record_decode.data(), cell.size, header);
        if (DECODED == proto::Drop::PRESSURE) {
            retry_record(cell);
            return;
        }
        if (DECODED != proto::Drop::NONE) {
            dropped(DECODED);
            return;
        }
        std::memcpy(cell.record.data(), s_record_decode.data(), cell.size);
        cell.header = header;
        cell.decoded = true;
    }
    // A retained decoded record has already committed replay. Revalidate its
    // exact captured allocation and admit that private plaintext without a
    // second authentication/replay turn or any prior native effect.
    int const CURRENT = binding_status(cell.binding, *peer, cell.provider);
    if (CURRENT == -EAGAIN) {
        retry_record(cell);
        return;
    }
    if (CURRENT != 0) {
        dropped(proto::Drop::RETIRED);
        return;
    }
    auto const ACCEPTED =
        s_operations.admit(cell.binding, cell.header, {cell.record.data() + proto::HEADER_BYTES, cell.header.payload_length});
    if (ACCEPTED == proto::Drop::PRESSURE)
        retry_record(cell);
    else if (ACCEPTED != proto::Drop::NONE)
        dropped(ACCEPTED);
}
void drain_rx(uint64_t now) {
    for (size_t count = 0; count < WORK_BUDGET; ++count) {
        ControlCell* cell = nullptr;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD || s_control_count == 0) break;
            cell = &s_controls[s_control_order[s_control_head]];
            s_control_head = (s_control_head + 1) % CONTROL_CELLS;
            --s_control_count;
            cell->state = CellState::PROCESSING;
            cell->retry = false;
        }
        process_control_cell(*cell, now);
        if (cell->retry) {
            s_lock.lock();
            s_control_head = (s_control_head + CONTROL_CELLS - 1) % CONTROL_CELLS;
            s_control_order[s_control_head] = static_cast<size_t>(cell - s_controls.data());
            ++s_control_count;
            cell->state = CellState::QUEUED;
            s_lock.unlock();
            break;
        }
        auth_crypto::secure_erase(cell->payload.data(), cell->payload.size());
        s_lock.lock();
        cell->state = CellState::FREE;
        s_lock.unlock();
    }
    for (size_t count = 0; count < WORK_BUDGET; ++count) {
        RecordCell* cell = nullptr;
        {
            EngineGuard const GUARD(s_record_lock);
            if (!GUARD || s_record_count == 0) break;
            cell = &s_records[s_record_order[s_record_head]];
            s_record_head = (s_record_head + 1) % RECORD_CELLS;
            --s_record_count;
            cell->state = CellState::PROCESSING;
            cell->retry = false;
        }
        process_record_cell(*cell);
        if (cell->retry) {
            // Reuse the existing final queue gate after all admission guards
            // unwind. Preserve FIFO and consume only this bounded worker turn.
            s_record_lock.lock();
            s_record_head = (s_record_head + RECORD_CELLS - 1) % RECORD_CELLS;
            s_record_order[s_record_head] = static_cast<size_t>(cell - s_records.data());
            ++s_record_count;
            cell->state = CellState::QUEUED;
            s_record_lock.unlock();
            break;
        }
        auth_crypto::secure_erase(cell->record.data(), cell->record.size());
        auth_crypto::secure_erase(&cell->header, sizeof(cell->header));
        auth_crypto::secure_erase(&cell->binding, sizeof(cell->binding));
        cell->provider.reset();
        cell->secure_incarnation = 0;
        cell->sid = {};
        cell->auth_generation = 0;
        cell->peer = WKI_MAX_PEERS;
        cell->size = 0;
        cell->decoded = false;
        cell->retry = false;
        s_record_lock.lock();
        cell->state = CellState::FREE;
        s_record_lock.unlock();
    }
}
auto send_record(Job& job, proto::Opcode opcode, uint32_t fragment, uint32_t payload, uint32_t total, int32_t status,
                 const uint8_t* plaintext) -> int {
    auto* peer = binding_peer(job.binding);
    PeerAdmission admission;
    if (peer == nullptr) return -ESTALE;
    if (!admission.acquire(peer)) return -EAGAIN;
    int const BINDING = binding_status(job.binding, *peer, job.provider);
    if (BINDING != 0) return BINDING;
    auto header = header_from(job.arm, job.binding, opcode, true, fragment, payload, total, status);
    auto const ENCODED = s_peers[peer_index(peer)].session.send(header, plaintext, job.tx_record.data(), job.tx_record.size());
    if (ENCODED != proto::Drop::NONE) return native_status(ENCODED);
    // A WKI transport error is not native errno and is never EBADF. Once
    // publication was attempted, a WRITE cannot safely use message fallback.
    return wki_transport_send_secure(job.provider.transport, peer->node_id, job.tx_record.data(),
                                     static_cast<uint16_t>(proto::HEADER_BYTES + payload)) == WKI_OK
               ? 0
               : -EIO;
}
void finish_job(Job& job) {
    if (job.work) return;
    if (job.arm_succeeded && job.region < s_regions.size()) {
        WkiSecureRdmaHooks hooks{};
        void* context = nullptr;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) return;
            hooks = s_regions[job.region].hooks;
            context = s_regions[job.region].context;
        }
        // Exact release is independent of the retiring peer/channel gates.
        hooks.release(context, request_from(job.arm, job.binding.peer_node), job.result);
        job.arm_succeeded = false;
    }
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return;
    if (job.region < s_regions.size()) {
        --s_regions[job.region].references;
        job.region = REGIONS;
    }
    job.provider.reset();
    job.state.store(JobState::COMPLETE, std::memory_order_release);
    job.done.store(true, std::memory_order_release);
}
void cancel_job(Job& job, int status) {
    if (job.state.load(std::memory_order_acquire) == JobState::IN_FINISH && job.effect_done) return;
    if (job.work) {
        // SEND work only. EFFECT is finished by the same task before reaching
        // here; a cancellation cannot abandon actual asynchronous ownership.
        auto const FINISHED = s_operations.finish_send(job.work, status);
        if (FINISHED == proto::Drop::PRESSURE) return;
        if (FINISHED != proto::Drop::NONE) return;
    }
    auto const CANCELLED = s_operations.cancel(job.operation, status);
    if (CANCELLED == proto::Drop::PRESSURE) return;
    ops::Result result{};
    if (s_operations.observe(job.operation, result) != proto::Drop::NONE) return;
    if (!job.native_result_seen) {
        job.result = public_result(result);
        if (job.outgoing && job.arm.opcode == static_cast<uint8_t>(proto::Opcode::WRITE) && result.outcome != ops::Outcome::NO_EFFECT) {
            // A successful transport END cannot replace the exact native
            // RESULT (including POSIX partial bytes). Missing RESULT remains
            // uncertain even if the record model reached terminal success.
            job.result = {status, 0, WkiSecureRdmaOutcome::UNCERTAIN};
        }
    }
    finish_job(job);
}
void progress_arm(Job& job) {
    if (!job.arm_succeeded) {
        WkiSecureRdmaHooks hooks{};
        void* context = nullptr;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) return;
            if (job.region >= s_regions.size() || !s_regions[job.region].live || s_regions[job.region].closing) {
                job.cancel.store(true, std::memory_order_release);
                return;
            }
            hooks = s_regions[job.region].hooks;
            context = s_regions[job.region].context;
        }
        uint64_t generation = 0;
        int status = hooks.arm(context, request_from(job.arm, job.binding.peer_node), generation);
        if (status > 0) status = -EIO;
        job.arm_succeeded = status == 0;
        job.consumer_generation = generation;
        if (status == 0 && generation == 0) status = -EINVAL;
        job.ack_status = status;
        if (status != 0) {
            job.ack_pending = true;
            job.result = {status, 0, WkiSecureRdmaOutcome::NO_EFFECT};
            job.cancel.store(true, std::memory_order_release);
            return;
        }
    }
    auto const ARMED = s_operations.arm(job.operation, job.consumer_generation);
    if (ARMED == proto::Drop::PRESSURE) return;
    if (ARMED != proto::Drop::NONE) {
        job.ack_status = native_status(ARMED);
        job.ack_pending = true;
        job.cancel.store(true, std::memory_order_release);
        return;
    }
    job.ack_pending = true;
    job.state.store(JobState::IN_WAIT, std::memory_order_release);
}
void progress_effect(Job& job) {
    auto* peer = binding_peer(job.binding);
    PeerAdmission admission;
    if (peer == nullptr) {
        job.cancel.store(true, std::memory_order_release);
        return;
    }
    if (!admission.acquire(peer)) return;
    int const BINDING = binding_status(job.binding, *peer, job.provider);
    if (BINDING == -EAGAIN) return;
    if (BINDING != 0 || !policy_allows(*peer, job.binding.service, static_cast<proto::Opcode>(job.arm.opcode), job.arm.flags)) {
        job.cancel.store(true, std::memory_order_release);
        return;
    }
    WkiSecureRdmaHooks hooks{};
    void* context = nullptr;
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) return;
        auto& region = s_regions[job.region];
        if (region.closing || !region.live || !context_current(s_peers[peer_index(peer)], *peer, true)) {
            job.cancel.store(true, std::memory_order_release);
            return;
        }
        auto const CLAIMED = s_operations.take_effect(job.operation, job.work);
        if (CLAIMED != proto::Drop::NONE) {
            if (CLAIMED != proto::Drop::PRESSURE && CLAIMED != proto::Drop::OPERATION) job.cancel.store(true, std::memory_order_release);
            return;
        }
        hooks = region.hooks;
        context = region.context;
    }
    admission.reset();
    auto const BACKING = job.work.backing();
    bool const READ = job.arm.opcode == static_cast<uint8_t>(proto::Opcode::READ);
    job.result =
        hooks.effect(context, request_from(job.arm, job.binding.peer_node), {static_cast<uint8_t*>(BACKING.backing), BACKING.size},
                     READ ? std::span<const uint8_t>{} : job.work.staging(), READ ? job.work.prepare_buffer() : std::span<uint8_t>{});
    if (!valid_native_result(job.result, job.arm)) job.result = {-EIO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    job.effect_done = true;
    job.state.store(JobState::IN_FINISH, std::memory_order_release);
}
void progress_finish_effect(Job& job) {
    auto const FINISHED = s_operations.finish_effect(job.work, model_result(job.result));
    if (FINISHED == proto::Drop::PRESSURE) return;
    if (FINISHED != proto::Drop::NONE) {
        dropped(FINISHED);
        return;
    }
    job.position = 0;
    job.state.store(JobState::IN_REPLY, std::memory_order_release);
}
void progress_reply(Job& job) {
    if (job.end_sent) {
        finish_job(job);
        return;
    }
    size_t const INDEX = peer_index(binding_peer(job.binding));
    bool const READ = job.arm.opcode == static_cast<uint8_t>(proto::Opcode::READ);
    if (!READ && !job.result_sent) {
        rdma_control::Result result{};
        result.request = job.arm;
        result.status = job.result.status;
        result.bytes = job.result.bytes;
        result.outcome = static_cast<uint8_t>(job.result.outcome);
        if (!send_control(INDEX, MsgType::SECURE_RDMA_RESULT, &result, sizeof(result))) return;
        job.result_sent = true;
    }
    if (READ && !job.work) {
        ops::Result result{};
        if (s_operations.take_result(job.operation, job.work, result) != proto::Drop::NONE) return;
    }
    if (READ && job.position < job.result.bytes) {
        uint32_t const SIZE = std::min(proto::FRAGMENT_BYTES, job.result.bytes - job.position);
        int const SENT =
            send_record(job, proto::Opcode::READ_DATA, job.position, SIZE, job.result.bytes, 0, job.work.staging().data() + job.position);
        if (SENT == -EAGAIN) return;
        if (SENT != 0) {
            job.work.reset();
            job.end_sent = true;
            finish_job(job);
            return;
        }
        job.position += SIZE;
        return;
    }
    if (job.arm.opcode == static_cast<uint8_t>(proto::Opcode::NOTIFY)) {
        finish_job(job);
        return;
    }
    int32_t const STATUS = !READ && (job.result.status != 0 || job.result.bytes != job.arm.length)
                               ? (job.result.status != 0 ? job.result.status : -EIO)
                               : job.result.status;
    int const SENT = send_record(job, READ ? proto::Opcode::READ_END : proto::Opcode::WRITE_END, 0, 0,
                                 READ ? job.result.bytes : job.arm.length, STATUS, nullptr);
    if (SENT == -EAGAIN) return;
    job.work.reset();
    job.end_sent = true;
    finish_job(job);
}
void progress_outgoing(Job& job, JobState state, uint64_t now) {
    size_t const INDEX = peer_index(binding_peer(job.binding));
    if (state == JobState::ARM_SEND || state == JobState::ARM_WAIT) {
        if (job.ack_seen) {
            if (job.ack_status != 0) {
                cancel_job(job, job.ack_status);
                return;
            }
            job.state.store(JobState::OUT_SEND, std::memory_order_release);
            return;
        }
        if (job.retry_at == 0 || now >= job.retry_at) {
            job.retry_at = deadline_after(now, RETRY_US);
            static_cast<void>(send_control(INDEX, MsgType::SECURE_RDMA_ARM, &job.arm, sizeof(job.arm)));
            job.state.store(JobState::ARM_WAIT, std::memory_order_release);
        }
        return;
    }
    if (state == JobState::OUT_SEND) {
        if (job.end_sent) {
            if (s_operations.finish_send(job.work, 0) == proto::Drop::NONE) job.state.store(JobState::OUT_WAIT, std::memory_order_release);
            return;
        }
        if (!job.work) {
            auto const BEGUN = s_operations.begin_send(job.operation, job.work);
            if (BEGUN == proto::Drop::PRESSURE) return;
            if (BEGUN != proto::Drop::NONE) {
                cancel_job(job, native_status(BEGUN));
                return;
            }
        }
        bool const READ = job.arm.opcode == static_cast<uint8_t>(proto::Opcode::READ);
        uint32_t const SIZE = READ ? 0 : std::min(proto::FRAGMENT_BYTES, job.arm.length - job.position);
        int const SENT = send_record(job, READ ? proto::Opcode::READ : proto::Opcode::WRITE, READ ? 0 : job.position, SIZE, job.arm.length,
                                     0, READ ? nullptr : job.work.staging().data() + job.position);
        if (SENT == -EAGAIN) return;
        if (SENT != 0) {
            cancel_job(job, SENT);
            return;
        }
        job.position += SIZE;
        if (READ || job.position == job.arm.length) {
            auto const FINISHED = s_operations.finish_send(job.work, 0);
            if (FINISHED == proto::Drop::PRESSURE) {
                job.end_sent = true;
                return;
            }
            if (FINISHED != proto::Drop::NONE) {
                cancel_job(job, -EIO);
                return;
            }
            job.state.store(JobState::OUT_WAIT, std::memory_order_release);
        }
        return;
    }
    if (state == JobState::OUT_WAIT) {
        bool const WRITE = job.arm.opcode == static_cast<uint8_t>(proto::Opcode::WRITE);
        if (WRITE && !job.native_result_seen) return;
        if (WRITE) {
            // Native RESULT is authoritative only after local SEND retirement.
            auto const CLOSED = s_operations.cancel(job.operation, -ECANCELED);
            if (CLOSED == proto::Drop::PRESSURE) return;
        }
        ops::Result result{};
        if (s_operations.observe(job.operation, result) != proto::Drop::NONE) return;
        if (!WRITE) job.result = public_result(result);
        finish_job(job);
    }
}
void diagnose_job_timeout(Job& job, JobState state, int provider_status) {
    auto const* command = ker::init::get_kernel_cmdline();
    if (job.timeout_logged || !command_token(command, "wki.chaos") || !command_token(command, "wki.secure_rdma")) return;
    job.timeout_logged = true;
    uint32_t count = s_timeout_warnings.load(std::memory_order_relaxed);
    bool reserved = false;
    for (uint32_t attempt = 0; attempt < 8 && count < 32; ++attempt) {
        if (s_timeout_warnings.compare_exchange_weak(count, count + 1, std::memory_order_relaxed)) {
            reserved = true;
            break;
        }
    }
    if (!reserved) return;
    ker::mod::dbg::logger<"wki">::warn(
        "secure job timeout peer=%u channel=%u service=%u transport=%u opcode=%u state=%u outgoing=%u length=%u position=%u "
        "ack_seen=%u ack_status=%d arm_succeeded=%u ack_pending=%u effect_done=%u end_sent=%u native_result=%u result_sent=%u "
        "result_status=%d result_bytes=%u outcome=%u provider_status=%d cancel=%u",
        static_cast<unsigned>(job.binding.peer_node), static_cast<unsigned>(job.binding.channel_id),
        static_cast<unsigned>(job.binding.service), static_cast<unsigned>(job.binding.transport), static_cast<unsigned>(job.arm.opcode),
        static_cast<unsigned>(state), job.outgoing ? 1U : 0U, job.arm.length, job.position, job.ack_seen ? 1U : 0U, job.ack_status,
        job.arm_succeeded ? 1U : 0U, job.ack_pending ? 1U : 0U, job.effect_done ? 1U : 0U, job.end_sent ? 1U : 0U,
        job.native_result_seen ? 1U : 0U, job.result_sent ? 1U : 0U, job.result.status, job.result.bytes,
        static_cast<unsigned>(job.result.outcome), provider_status, job.cancel.load(std::memory_order_acquire) ? 1U : 0U);
}
void progress_job(Job& job, uint64_t now) {
    auto const STATE = job.state.load(std::memory_order_acquire);
    if (STATE == JobState::FREE) return;
    if (STATE == JobState::RESERVED) {
        sched::task::Task* owner = nullptr;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD || job.state.load(std::memory_order_acquire) != JobState::RESERVED) return;
            owner = job.prepare_owner;
            if (owner == nullptr || owner->state.load(std::memory_order_acquire) != sched::task::TaskState::DEAD ||
                owner->death_epoch.load(std::memory_order_acquire) == 0 ||
                !sched::EpochManager::is_safe_to_reclaim(owner->death_epoch.load(std::memory_order_acquire)) || !sched::has_run_queues() ||
                !owner->try_acquire_lifetime_ref())
                return;
        }
        // Match scheduler GC's epoch plus all-CPU current-task exclusion. DEAD
        // alone does not prove that an interrupted finite copy has retired.
        bool executing = false;
        for (uint64_t cpu = 0; cpu < ker::mod::smt::get_core_count(); ++cpu) {
            auto const STATE = sched::get_scheduler_cpu_state(cpu);
            if (STATE.current_pid == owner->pid || STATE.handoff_pid == owner->pid) {
                executing = true;
                break;
            }
        }
        owner->release();
        if (executing) return;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD || job.state.load(std::memory_order_acquire) != JobState::RESERVED || job.prepare_owner != owner) return;
            job.frontend = false;
            job.state.store(JobState::CANCEL, std::memory_order_release);
        }
        job.work.reset();
        cancel_job(job, -ECANCELED);
        return;
    }
    if (STATE == JobState::COMPLETE) {
        EngineGuard const GUARD(s_lock);
        if (GUARD && (!job.frontend || now >= job.deadline)) job_clear_locked(job);
        return;
    }
    if (job.ack_pending) {
        rdma_control::ArmAck ack{};
        ack.request = job.arm;
        ack.status = job.ack_status;
        if (send_control(peer_index(binding_peer(job.binding)), MsgType::SECURE_RDMA_ARM_ACK, &ack, sizeof(ack))) job.ack_pending = false;
    }
    if (STATE == JobState::IN_FINISH) {
        progress_finish_effect(job);
        return;
    }
    size_t const PEER = peer_index(binding_peer(job.binding));
    int const PROVIDER = job.provider.status();
    if (now >= job.deadline) diagnose_job_timeout(job, STATE, PROVIDER);
    if (PROVIDER == -EAGAIN && !job.cancel.load(std::memory_order_acquire) && !s_stopping.load(std::memory_order_acquire) &&
        now < job.deadline && PEER != WKI_MAX_PEERS && !s_peers[PEER].closed.load(std::memory_order_acquire))
        return;
    if (job.cancel.load(std::memory_order_acquire) || s_stopping.load(std::memory_order_acquire) || now >= job.deadline || PROVIDER != 0 ||
        PEER == WKI_MAX_PEERS || s_peers[PEER].closed.load(std::memory_order_acquire)) {
        // RESULT/READ reply loans are not active native work; release before
        // cancellation. A finished native effect is never invoked again.
        if (STATE == JobState::IN_REPLY) {
            job.work.reset();
            finish_job(job);
            return;
        }
        cancel_job(job, now >= job.deadline ? -ETIMEDOUT : -ESTALE);
        return;
    }
    if (STATE == JobState::IN_ARM)
        progress_arm(job);
    else if (STATE == JobState::IN_WAIT)
        progress_effect(job);
    else if (STATE == JobState::IN_REPLY)
        progress_reply(job);
    else
        progress_outgoing(job, STATE, now);
}
void maintain_regions() {
    for (size_t index = 0; index < s_regions.size(); ++index) {
        rdma_registry::Binding binding{};
        proto::RegionDescriptor descriptor{};
        bool closing = false, publish = false, tombstone = false;
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) return;
            auto const& region = s_regions[index];
            if (!region.live && !region.revoke_pending) continue;
            binding = region.binding;
            descriptor = region.descriptor;
            closing = region.closing;
            publish = region.grant_pending;
            tombstone = !region.live;
            if (tombstone) {
                size_t const PEER = peer_index(binding_peer(binding));
                if (PEER == WKI_MAX_PEERS || s_peers[PEER].closed.load(std::memory_order_acquire) || !s_peers[PEER].begun ||
                    s_peers[PEER].sid != binding.session_id || s_peers[PEER].incarnation != binding.secure_incarnation) {
                    s_regions[index] = {};
                    continue;
                }
            }
        }
        if (tombstone) {
            ProviderPin pin;
            auto* peer = binding_peer(binding);
            PeerAdmission admission;
            if (!admission.acquire(peer) ||
                !pin.acquire(reinterpret_cast<WkiTransport*>(binding.transport_cookie), binding.transport_generation) ||
                !binding_current(binding, *peer, pin))
                continue;
            admission.reset();
            if (send_control(peer_index(peer), MsgType::SECURE_RDMA_REVOKE, &descriptor, sizeof(descriptor))) {
                EngineGuard const GUARD(s_lock);
                if (GUARD && !s_regions[index].live && s_regions[index].revoke_pending && same_binding(binding, s_regions[index].binding) &&
                    same_descriptor(descriptor, s_regions[index].descriptor))
                    s_regions[index] = {};
            }
            continue;
        }
        if (!closing) {
            ProviderPin pin;
            auto* peer = binding_peer(binding);
            PeerAdmission admission;
            if (peer == nullptr)
                closing = true;
            else {
                if (!admission.acquire(peer)) continue;
                if (!pin.acquire(reinterpret_cast<WkiTransport*>(binding.transport_cookie), binding.transport_generation)) {
                    int const STATUS =
                        provider_status(reinterpret_cast<WkiTransport*>(binding.transport_cookie), binding.transport_generation);
                    if (STATUS == 0 || STATUS == -EAGAIN) continue;
                    closing = true;
                } else {
                    int const STATUS = binding_status(binding, *peer, pin);
                    if (STATUS == -EAGAIN) continue;
                    closing = STATUS != 0;
                }
            }
        }
        {
            EngineGuard const GUARD(s_lock);
            if (!GUARD) continue;
            auto& region = s_regions[index];
            if (!region.live || !same_descriptor(descriptor, region.descriptor)) continue;
            if (closing) {
                region.closing = true;
                region.grant_pending = false;
                if (s_registry.revoke(descriptor.rkey, descriptor.generation) == proto::Drop::NONE) region.revoked = true;
                for (auto& job : s_jobs)
                    if (job.state.load(std::memory_order_acquire) != JobState::FREE && job.region == index)
                        job.cancel.store(true, std::memory_order_release);
            }
        }
        if (!closing && publish &&
            send_control(peer_index(binding_peer(binding)), MsgType::SECURE_RDMA_GRANT, &descriptor, sizeof(descriptor))) {
            EngineGuard const GUARD(s_lock);
            if (GUARD && s_regions[index].live && same_descriptor(descriptor, s_regions[index].descriptor))
                s_regions[index].grant_pending = false;
        }
    }
}
auto drained_locked() -> bool {
    if (s_control_count != 0) return false;
    {
        EngineGuard const RECORDS(s_record_lock);
        if (!RECORDS || s_record_count != 0) return false;
    }
    for (auto const& job : s_jobs)
        if (job.state.load(std::memory_order_acquire) != JobState::FREE) return false;
    for (auto const& peer : s_peers)
        if (peer.begun) return false;
    for (auto const& region : s_regions)
        if (!region.live && region.revoke_pending) return false;
    if (s_eraser) return false;
    // Clearing a COMPLETE job releases its last handle before the bounded
    // one-slot eraser has necessarily retired every terminal operation. Keep
    // the worker until the table proves all scratch and registry loans gone.
    ops::Statistics operations{};
    return s_operations.statistics(operations) == proto::Drop::NONE && operations.live == 0;
}
void service_pass() {
    bool expected = false;
    if (!s_initialized.load(std::memory_order_acquire) || s_services == 0 ||
        !s_service_owner.compare_exchange_strong(expected, true, std::memory_order_acquire))
        return;
    OwnerReset const OWNER(s_service_owner);
    uint64_t const NOW = wki_now_us();
    if (s_stopping.load(std::memory_order_acquire))
        for (auto& peer : s_peers) peer.closed.store(true, std::memory_order_release);
    for (size_t count = 0; count < WORK_BUDGET; ++count) {
        maintain_peer(s_peer_cursor, NOW);
        s_peer_cursor = (s_peer_cursor + 1) % WKI_MAX_PEERS;
    }
    drain_rx(NOW);
    maintain_regions();
    for (auto& job : s_jobs) progress_job(job, NOW);
    static_cast<void>(s_operations.expire(NOW, -ETIMEDOUT));
    if (!s_eraser) {
        s_eraser_wiped = false;
        static_cast<void>(s_operations.begin_erase(s_eraser));
    }
    if (s_eraser) {
        if (!s_eraser_wiped) {
            auto bytes = s_eraser.buffer();
            auth_crypto::secure_erase(bytes.data(), bytes.size());
            s_eraser_wiped = true;
        }
        if (s_operations.finish_erase(s_eraser) == proto::Drop::NONE) s_eraser_wiped = false;
    }
}
void worker_body() {
    auto* current = sched::get_current_task();
    while (true) {
        auto const HOLD = sched::hold_kernel_shutdown();
        if (sched::kernel_threads_shutdown_requested()) s_stopping.store(true, std::memory_order_release);
        service_pass();
        bool stop = false, busy = false;
        {
            EngineGuard const GUARD(s_lock);
            if (GUARD) {
                stop = s_stopping.load(std::memory_order_acquire) && drained_locked();
                if (stop) {
                    s_worker = nullptr;
                } else
                    for (auto const& job : s_jobs)
                        if (job.state.load(std::memory_order_acquire) != JobState::FREE) {
                            busy = true;
                            break;
                        }
            }
        }
        if (stop) {
            current->release();
            // Final engine publication after every queue/table access. The
            // trampoline exit is explicit, after HOLD and worker RAII unwind.
            s_worker_stopped.store(true, std::memory_order_release);
            return;
        }
        if (busy)
            sched::kern_yield();
        else
            sched::kern_sleep_us(WORKER_US);
    }
}
void worker_entry() {
    worker_body();
    sched::exit_current_kernel_thread();
}
}  // namespace

auto wki_secure_rdma_init() -> bool {
    if (s_initialized_once) return s_initialized.load(std::memory_order_acquire);
    s_initialized_once = true;
    auto const* command = ker::init::get_kernel_cmdline();
    if (command_token(command, "wki.secure_rdma")) {
        s_services = proto::SERVICES;
        if (command_token(command, "wki.secure_rdma_zone_off"))
            s_services &= static_cast<uint8_t>(~proto::service_bit(proto::Service::ZONE));
        if (command_token(command, "wki.secure_rdma_vfs_off")) s_services &= static_cast<uint8_t>(~proto::service_bit(proto::Service::VFS));
        if (command_token(command, "wki.secure_rdma_ipc_off")) s_services &= static_cast<uint8_t>(~proto::service_bit(proto::Service::IPC));
        if (command_token(command, "wki.secure_rdma_block_off"))
            s_services &= static_cast<uint8_t>(~proto::service_bit(proto::Service::BLOCK));
    }
    rdma_crypto::Key root{};
    bool const ENTROPY = ker::mod::random::entropy::get_bytes(root.data(), root.size());
    auto const INITIALIZED = ENTROPY ? s_registry.initialize(g_wki.my_node_id, root) : proto::Drop::AUTHENTICATION;
    auth_crypto::secure_erase(root.data(), root.size());
    if (INITIALIZED != proto::Drop::NONE) return false;
    if (s_services != 0) {
        s_scratch = static_cast<uint8_t*>(
            ker::mod::mm::phys::page_alloc(ker::mod::mm::PhysicalPageOwner::WKI_ZONE, ops::SCRATCH_BYTES, "wki_secure_rdma"));
        if (s_scratch == nullptr) {
            static_cast<void>(s_registry.shutdown());
            return false;
        }
        std::memset(s_scratch, 0, ops::SCRATCH_BYTES);
        if (s_operations.initialize({s_scratch, ops::SCRATCH_BYTES}) != proto::Drop::NONE) {
            auth_crypto::secure_erase(s_scratch, ops::SCRATCH_BYTES);
            ker::mod::mm::phys::page_free(std::exchange(s_scratch, nullptr));
            static_cast<void>(s_registry.shutdown());
            return false;
        }
    }
    s_initialized.store(true, std::memory_order_release);
    return true;
}
void wki_secure_rdma_worker_start() {
    if (!s_initialized.load(std::memory_order_acquire) || s_services == 0 || s_stopping.load(std::memory_order_acquire)) return;
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD || s_worker != nullptr || s_worker_starting) return;
        s_worker_starting = true;
    }
    auto const HOLD = sched::hold_kernel_shutdown();
    auto* worker = sched::task::Task::create_kernel_thread("wki_secure_rdma", worker_entry);
    if (worker != nullptr && !worker->try_acquire_lifetime_ref()) {
        sched::task::destroy_unpublished_user_thread(worker);
        worker = nullptr;
    }
    s_lock.lock();
    s_worker = worker;
    s_worker_stopped.store(worker == nullptr, std::memory_order_release);
    s_lock.unlock();
    bool const POSTED = worker != nullptr && sched::post_task_balanced(worker);
    s_lock.lock();
    s_worker_starting = false;
    if (!POSTED) {
        s_worker = nullptr;
        s_worker_stopped.store(true, std::memory_order_release);
    }
    s_lock.unlock();
    if (worker != nullptr && !POSTED) {
        worker->release();
        sched::task::destroy_unpublished_user_thread(worker);
    }
}
void wki_secure_rdma_tick() { wake_worker(); }
void wki_secure_rdma_retire_peer(WkiPeer* peer) {
    size_t const INDEX = peer_index(peer);
    if (INDEX == WKI_MAX_PEERS) return;
    // Admission closes regardless of model/table tryguard contention.
    // Auth invokes this while holding its replay lock. No engine/scheduler
    // gate is acquired here; the worker's bounded idle timer performs drain.
    s_peers[INDEX].closed.store(true, std::memory_order_release);
}
void wki_secure_rdma_retire_transport(WkiTransport* transport, uint64_t generation) {
    if (transport == nullptr || generation == 0) return;
    // Core has synchronously published chaos_unregistering before this call.
    // Close session admission even when the private table gate is contended.
    // Publication pairs with the core's unregistering transport gate above.
    for (auto& context : s_peers)
        if (context.provider_cookie.load(std::memory_order_acquire) == reinterpret_cast<uintptr_t>(transport) &&
            context.provider_generation.load(std::memory_order_acquire) == generation)
            context.closed.store(true, std::memory_order_release);
    {
        EngineGuard const GUARD(s_lock);
        if (GUARD) {
            for (auto& region : s_regions)
                if (region.live && region.binding.transport_cookie == reinterpret_cast<uintptr_t>(transport) &&
                    region.binding.transport_generation == generation)
                    region.closing = true;
            for (auto& remote : s_remote)
                if (remote.live && remote.binding.transport_cookie == reinterpret_cast<uintptr_t>(transport) &&
                    remote.binding.transport_generation == generation)
                    remote.live = false;
            for (auto& job : s_jobs)
                if (job.state != JobState::FREE && job.binding.transport_cookie == reinterpret_cast<uintptr_t>(transport) &&
                    job.binding.transport_generation == generation)
                    job.cancel.store(true, std::memory_order_release);
        }
    }
    wake_worker();
}
auto wki_secure_rdma_admit_control_rx(const WkiHeader* header, const void* payload, uint16_t size, const WkiChannelIdentity& identity,
                                      WkiPeer* admitted_peer) -> WkiSecureRdmaControlRxAdmission {
    using Admission = WkiSecureRdmaControlRxAdmission;
    if (header == nullptr || sizeof(WkiHeader) > UINTPTR_MAX - reinterpret_cast<uintptr_t>(header)) return Admission::NOT_APPLICABLE;
    auto const TYPE = static_cast<MsgType>(header->msg_type);
    if (control_size(TYPE) == 0) return Admission::NOT_APPLICABLE;
    if (!s_initialized.load(std::memory_order_acquire) || s_services == 0 || s_stopping.load(std::memory_order_acquire)) {
        dropped(proto::Drop::RETIRED);
        return Admission::DISCARDED;
    }
    size_t const INDEX = peer_index(admitted_peer);
    // Caller holds this exact channel guard and the authenticated peer admission.
    // No nested channel/peer gate or scheduler notification is permitted here.
    if (payload == nullptr || size > UINTPTR_MAX - reinterpret_cast<uintptr_t>(payload) || control_size(TYPE) != size ||
        size > sizeof(rdma_control::Result) || INDEX == WKI_MAX_PEERS || !peer_current(admitted_peer) || identity.channel == nullptr ||
        identity.generation == 0 || identity.channel_id != WKI_CHAN_SECURE_RDMA || header->channel_id != identity.channel_id ||
        header->src_node != identity.peer_node_id || header->dst_node != g_wki.my_node_id ||
        admitted_peer->node_id != identity.peer_node_id || !identity.channel->active ||
        identity.channel->peer_node_id != identity.peer_node_id || identity.channel->channel_id != identity.channel_id ||
        identity.channel->generation != identity.generation) {
        dropped(proto::Drop::FIELD);
        return Admission::DISCARDED;
    }
    EngineGuard const GUARD(s_lock);
    if (!GUARD) {
        dropped(proto::Drop::PRESSURE);
        return Admission::RETRY;
    }
    if (s_stopping.load(std::memory_order_acquire)) return Admission::DISCARDED;
    auto available = std::find_if(s_controls.begin(), s_controls.end(), [](auto const& cell) { return cell.state == CellState::FREE; });
    if (available == s_controls.end()) {
        dropped(proto::Drop::PRESSURE);
        return Admission::RETRY;
    }
    available->peer = INDEX;
    available->auth_generation = admitted_peer->auth_session.generation;
    available->sid = admitted_peer->auth_session.session_id;
    available->secure_incarnation = s_peers[INDEX].incarnation;
    available->provider_cookie = s_peers[INDEX].provider_cookie.load(std::memory_order_acquire);
    available->provider_generation = s_peers[INDEX].provider_generation.load(std::memory_order_acquire);
    available->identity = identity;
    available->type = TYPE;
    available->size = size;
    available->retry = false;
    std::memcpy(available->payload.data(), payload, size);
    available->state = CellState::QUEUED;
    s_control_order[(s_control_head + s_control_count) % CONTROL_CELLS] = static_cast<size_t>(available - s_controls.begin());
    ++s_control_count;
    return Admission::QUEUED;
}
void wki_secure_rdma_control_ingress(const WkiHeader* header, const void* payload, uint16_t size, const WkiChannelIdentity& identity) {
    if (header == nullptr || sizeof(WkiHeader) > UINTPTR_MAX - reinterpret_cast<uintptr_t>(header) || identity.channel == nullptr) return;
    auto* peer = wki_peer_find(identity.peer_node_id);
    PeerAdmission admission;
    if (!admission.acquire(peer)) {
        dropped(proto::Drop::PRESSURE);
        return;
    }
    WkiSecureRdmaControlRxAdmission result;
    {
        EngineGuard const CHANNEL(identity.channel->lock);
        if (!CHANNEL) {
            dropped(proto::Drop::PRESSURE);
            return;
        }
        result = wki_secure_rdma_admit_control_rx(header, payload, size, identity, peer);
    }
    admission.reset();
    if (result == WkiSecureRdmaControlRxAdmission::QUEUED) wake_worker();
}
void wki_secure_rdma_rx_ingress(WkiTransport* transport, const void* record, uint16_t size, const WkiRxMetadata* metadata) {
    if (!s_initialized.load(std::memory_order_acquire) || s_stopping.load(std::memory_order_acquire) || s_services == 0) {
        dropped(proto::Drop::RETIRED);
        return;
    }
    if (record == nullptr || metadata == nullptr || size < proto::HEADER_BYTES || size > proto::RECORD_BYTES ||
        size > UINTPTR_MAX - reinterpret_cast<uintptr_t>(record)) {
        dropped(proto::Drop::LENGTH);
        return;
    }
    proto::Header hint{};
    std::memcpy(&hint, record, sizeof(hint));
    auto* peer = wki_peer_find(hint.src_node);
    PeerAdmission admission;
    size_t const INDEX = peer_index(peer);
    ProviderPin pin;
    if (INDEX == WKI_MAX_PEERS || !admission.acquire(peer) || !peer_current(peer) || peer->transport != transport ||
        !pin.acquire(transport) || hint.dst_node != g_wki.my_node_id || hint.session_id != peer->auth_session.session_id ||
        (pin.kind == proto::Transport::ROCE && (!metadata->has_src_mac || metadata->src_mac != peer->mac)) ||
        (pin.kind == proto::Transport::IVSHMEM && (!metadata->has_direct_peer || metadata->direct_peer != peer->node_id))) {
        dropped(proto::Drop::IDENTITY);
        return;
    }
    uint64_t const INCARNATION = s_peers[INDEX].record_incarnation.load(std::memory_order_acquire);
    {
        EngineGuard const GUARD(s_record_lock);
        if (!GUARD) {
            dropped(proto::Drop::PRESSURE);
            return;
        }
        if (s_stopping.load(std::memory_order_acquire) || s_peers[INDEX].closed.load(std::memory_order_acquire) || INCARNATION == 0) {
            dropped(proto::Drop::SESSION);
            return;
        }
        auto available = std::find_if(s_records.begin(), s_records.end(), [](auto const& cell) { return cell.state == CellState::FREE; });
        if (available == s_records.end()) {
            dropped(proto::Drop::PRESSURE);
            return;
        }
        available->peer = INDEX;
        available->auth_generation = peer->auth_session.generation;
        available->sid = peer->auth_session.session_id;
        available->secure_incarnation = INCARNATION;
        available->binding = {};
        available->header = {};
        available->decoded = false;
        available->retry = false;
        available->size = size;
        available->provider = std::move(pin);
        std::memcpy(available->record.data(), record, size);
        available->state = CellState::QUEUED;
        s_record_order[(s_record_head + s_record_count) % RECORD_CELLS] = static_cast<size_t>(available - s_records.begin());
        ++s_record_count;
    }
    wake_worker();
}

auto wki_secure_rdma_available(const WkiChannelIdentity& identity, proto::Service service) -> bool {
    ProviderPin pin;
    rdma_registry::Binding binding{};
    return capture_authority(identity, service, proto::Opcode::READ, 0, binding, pin) == 0 ||
           capture_authority(identity, service, proto::Opcode::WRITE, 0, binding, pin) == 0;
}
auto wki_secure_rdma_transport(const WkiChannelIdentity& identity, proto::Service service) -> proto::Transport {
    rdma_registry::Binding binding{};
    ProviderPin pin;
    if (capture_authority(identity, service, proto::Opcode::READ, 0, binding, pin) == 0) return pin.kind;
    pin.reset();
    if (capture_authority(identity, service, proto::Opcode::WRITE, 0, binding, pin) == 0) return pin.kind;
    return proto::Transport::NONE;
}
auto wki_secure_rdma_local_region_current(const WkiChannelIdentity& identity, proto::Service service,
                                          const proto::RegionDescriptor& descriptor) -> int {
    if (!s_initialized.load(std::memory_order_acquire) || s_stopping.load(std::memory_order_acquire)) return -ESTALE;
    rdma_registry::Binding binding{};
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) return -EAGAIN;
        auto found = std::find_if(s_regions.begin(), s_regions.end(), [&](auto const& region) {
            return region.live && !region.closing && same_descriptor(descriptor, region.descriptor);
        });
        if (found == s_regions.end()) return -ESTALE;
        binding = found->binding;
    }
    if (!same_identity(identity, binding_identity(binding)) || service != binding.service) return -ESTALE;
    auto* peer = wki_peer_find(identity.peer_node_id);
    PeerAdmission admission;
    ProviderPin provider;
    if (peer == nullptr || peer_index(peer) == WKI_MAX_PEERS) return -ESTALE;
    if (!admission.acquire(peer)) return -EAGAIN;
    if (!peer_current(peer) || reinterpret_cast<uintptr_t>(peer->transport) != binding.transport_cookie) return -ESTALE;
    if (identity.channel == nullptr || !identity.channel->lock.try_lock()) return -EAGAIN;
    bool const LIVE = identity.channel->active && identity.channel->generation == identity.generation &&
                      identity.channel->peer_node_id == identity.peer_node_id && identity.channel->channel_id == identity.channel_id;
    identity.channel->lock.unlock();
    if (!LIVE) return -ESTALE;
    if (!provider.acquire(peer->transport, binding.transport_generation)) {
        int const STATUS = provider_status(peer->transport, binding.transport_generation);
        return STATUS == 0 || STATUS == -EAGAIN ? -EAGAIN : -ESTALE;
    }
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return -EAGAIN;
    auto const& context = s_peers[peer_index(peer)];
    if (!context_current(context, *peer, true) || context.sid != binding.session_id || context.incarnation != binding.secure_incarnation)
        return -ESTALE;
    for (auto const& region : s_regions)
        if (region.live && !region.closing && same_binding(binding, region.binding) && same_descriptor(descriptor, region.descriptor))
            return 0;
    return -ESTALE;
}
auto wki_secure_rdma_register_region(const WkiChannelIdentity& identity, proto::Service service, void* backing, uint32_t size,
                                     uint64_t binding_generation, uint8_t permissions, const WkiSecureRdmaHooks& hooks, void* context,
                                     proto::RegionDescriptor& output) -> int {
    // Native -errno only. A nonzero result retains no backing/context ownership.
    if (backing == nullptr || size == 0 || size > UINTPTR_MAX - reinterpret_cast<uintptr_t>(backing) || hooks.arm == nullptr ||
        hooks.effect == nullptr || hooks.release == nullptr || binding_generation == 0)
        return -EINVAL;
    ProviderPin pin;
    rdma_registry::Binding binding{};
    auto const OPCODE = (permissions & proto::PERMIT_WRITE) != 0 ? proto::Opcode::WRITE : proto::Opcode::READ;
    int const AUTHORITY = capture_authority(identity, service, OPCODE, 0, binding, pin);
    if (AUTHORITY != 0) return AUTHORITY;
    auto* peer = binding_peer(binding);
    PeerAdmission admission;
    if (!admission.acquire(peer)) return -EAGAIN;
    int const BINDING = binding_status(binding, *peer, pin);
    if (BINDING != 0) return BINDING;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return -EAGAIN;
    if (s_stopping.load(std::memory_order_acquire)) return -ESTALE;
    auto available =
        std::find_if(s_regions.begin(), s_regions.end(), [](auto const& region) { return !region.live && !region.revoke_pending; });
    if (available == s_regions.end()) return -ENOMEM;
    proto::RegionDescriptor descriptor{};
    auto const REGISTERED = s_registry.register_region({binding, backing, size, binding_generation, permissions}, descriptor);
    if (REGISTERED != proto::Drop::NONE) return native_status(REGISTERED);
    available->binding = binding;
    available->descriptor = descriptor;
    available->hooks = hooks;
    available->context = context;
    available->references = 0;
    available->closing = false;
    available->revoked = false;
    available->revoke_pending = false;
    available->grant_pending = true;
    available->live = true;
    output = descriptor;
    // Publication is retried by the worker; network failure cannot change the
    // successful owner's responsibility to close/reclaim this exact descriptor.
    return 0;
}
auto wki_secure_rdma_unregister_region(const proto::RegionDescriptor& descriptor) -> int {
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return -EAGAIN;
    auto found = std::find_if(s_regions.begin(), s_regions.end(),
                              [&](auto const& region) { return region.live && same_descriptor(region.descriptor, descriptor); });
    if (found == s_regions.end()) return -ESTALE;
    found->closing = true;
    found->grant_pending = false;
    found->revoke_pending = true;
    for (auto& job : s_jobs)
        if (job.state.load(std::memory_order_acquire) != JobState::FREE && job.region == static_cast<size_t>(found - s_regions.begin()))
            job.cancel.store(true, std::memory_order_release);
    auto const REVOKED = s_registry.revoke(descriptor.rkey, descriptor.generation);
    if (REVOKED == proto::Drop::PRESSURE) return -EAGAIN;
    if (REVOKED != proto::Drop::NONE) return native_status(REVOKED);
    found->revoked = true;
    if (found->references != 0) return -EBUSY;
    rdma_registry::ReleasedRegion released{};
    auto const RECLAIMED = s_registry.reclaim(descriptor.rkey, descriptor.generation, released);
    if (RECLAIMED != proto::Drop::NONE) return RECLAIMED == proto::Drop::PRESSURE ? -EBUSY : native_status(RECLAIMED);
    // Reclaimed native storage is no longer retained. Only the immutable
    // descriptor/binding remains until accepted reliable REVOKE publication
    // or retirement of this exact secure incarnation.
    found->live = false;
    found->hooks = {};
    found->context = nullptr;
    size_t const PEER = peer_index(binding_peer(found->binding));
    if (PEER == WKI_MAX_PEERS || s_peers[PEER].closed.load(std::memory_order_acquire) || !s_peers[PEER].begun ||
        s_peers[PEER].sid != found->binding.session_id || s_peers[PEER].incarnation != found->binding.secure_incarnation)
        *found = {};
    return 0;
}
auto wki_secure_rdma_remote_grant(const WkiChannelIdentity& identity, proto::Service service, uint32_t rkey,
                                  proto::RegionDescriptor& output, uint8_t required_permissions) -> bool {
    ProviderPin pin;
    rdma_registry::Binding binding{};
    auto const OPCODE = (required_permissions & proto::PERMIT_WRITE) != 0 ? proto::Opcode::WRITE : proto::Opcode::READ;
    if ((required_permissions & static_cast<uint8_t>(~(proto::PERMIT_READ | proto::PERMIT_WRITE | proto::PERMIT_NOTIFY))) != 0 ||
        capture_authority(identity, service, OPCODE, 0, binding, pin) != 0)
        return false;
    EngineGuard const GUARD(s_lock);
    if (!GUARD) return false;
    const RemoteRegion* selected = nullptr;
    for (auto const& region : s_remote) {
        if (!region.live || !same_binding(binding, region.binding) || (rkey != 0 && region.descriptor.rkey != rkey) ||
            (region.descriptor.permissions & required_permissions) != required_permissions)
            continue;
        if (selected != nullptr) return false;
        selected = &region;
    }
    if (selected == nullptr) return false;
    output = selected->descriptor;
    return true;
}

namespace {
auto frontend_process_can_wait(sched::task::Task* task) -> bool {
    if (task == nullptr || task != sched::get_current_task() || task->type != sched::task::TaskType::PROCESS ||
        task->scheduler_transition_active.load(std::memory_order_acquire) || task->wki_proxy_task)
        return false;
    if (wki_current_process_wait_can_park()) return true;
    // An earlier same-CPU wake may request a yield at syscall return. This
    // exact channel-free pair does not own an exec or deferred-wait handoff.
    // Preserve both markers; the existing PROCESS safe point owns its IF and
    // transient wait metadata, without changing the syscall return request.
    return task->syscall_account_start_us != 0 && task->deferred_task_switch && task->yield_switch && task->wait_channel == nullptr &&
           task->wait_channel_kind == sched::task::WaitChannelKind::NONE && !task->wants_block && !task->is_voluntary_blocked() &&
           task->wki_proxy_task_id == 0 && sched::preempt_count() == 0 && !task->has_interrupting_signal_pending() &&
           !wki_current_wait_must_drive_progress();
}
void diagnose_task_gate(sched::task::Task* task) {
    auto const* command = ker::init::get_kernel_cmdline();
    if (!command_token(command, "wki.chaos") || !command_token(command, "wki.secure_rdma")) return;
    uint32_t count = s_task_gate_warnings.load(std::memory_order_relaxed);
    bool reserved = false;
    for (uint32_t attempt = 0; attempt < 8 && count < 8; ++attempt) {
        if (s_task_gate_warnings.compare_exchange_weak(count, count + 1, std::memory_order_relaxed)) {
            reserved = true;
            break;
        }
    }
    if (!reserved) return;
    unsigned const PREEMPT = sched::preempt_count();
    auto const* reason = task == nullptr    ? "no-task"
                         : task == s_worker ? "service-worker"
                         : PREEMPT != 0     ? "preempt-disabled"
                                            : "process-wait-ineligible";
    std::array<char, 33> channel{};
    if (task != nullptr && task->wait_channel != nullptr)
        for (size_t index = 0; index + 1 < channel.size() && task->wait_channel[index] != '\0'; ++index)
            channel[index] = task->wait_channel[index];
    ker::mod::dbg::logger<"wki">::warn(
        "secure frontend task gate reason=%s pid=%llu type=%u preempt=%u deferred=%u yield=%u wait_kind=%u wait=%s account=%llu "
        "wants=%u voluntary=%u proxy=%u signal=%u transition=%u",
        reason, task != nullptr ? static_cast<unsigned long long>(task->pid) : 0ULL,
        task != nullptr ? static_cast<unsigned>(task->type) : 0U, PREEMPT, task != nullptr && task->deferred_task_switch ? 1U : 0U,
        task != nullptr && task->yield_switch ? 1U : 0U, task != nullptr ? static_cast<unsigned>(task->wait_channel_kind) : 0U,
        channel.data(), task != nullptr ? static_cast<unsigned long long>(task->syscall_account_start_us) : 0ULL,
        task != nullptr && task->wants_block ? 1U : 0U, task != nullptr && task->is_voluntary_blocked() ? 1U : 0U,
        task != nullptr ? task->wki_proxy_task_id : 0U, task != nullptr && task->has_interrupting_signal_pending() ? 1U : 0U,
        task != nullptr && task->scheduler_transition_active.load(std::memory_order_acquire) ? 1U : 0U);
}
void frontend_pause(const WkiChannelIdentity& identity) {
    wake_worker();
    if (frontend_process_can_wait(sched::get_current_task()))
        sched::preemptible_syscall_park("secure_rdma", deadline_after(wki_now_us(), WORKER_US));
    else {
        wki_spin_yield_channel_identity(identity);
        auto* task = sched::get_current_task();
        if (task != nullptr && task->type == sched::task::TaskType::DAEMON)
            sched::kern_sleep_us(WORKER_US);
        else
            sched::kern_yield();
    }
}
auto synchronous_transfer(const WkiChannelIdentity& identity, const proto::RegionDescriptor& remote, uint64_t offset,
                          std::span<const uint8_t> input, std::span<uint8_t> output, proto::Opcode opcode, uint16_t flags,
                          std::span<const uint8_t> intent) -> WkiSecureRdmaResult {
    size_t const SIZE = opcode == proto::Opcode::WRITE ? input.size() : output.size();
    auto const SERVICE = static_cast<proto::Service>(remote.service);
    auto failure = [](int status) { return WkiSecureRdmaResult{status, 0, WkiSecureRdmaOutcome::NO_EFFECT}; };
    if (SIZE == 0 || SIZE > proto::MAX_TRANSFER_BYTES || intent.size() > rdma_control::NATIVE_INTENT_BYTES ||
        (intent.size() != 0 && (intent.data() == nullptr || intent.size() > UINTPTR_MAX - reinterpret_cast<uintptr_t>(intent.data()))) ||
        (opcode == proto::Opcode::WRITE && (input.data() == nullptr || SIZE > UINTPTR_MAX - reinterpret_cast<uintptr_t>(input.data()))) ||
        (opcode == proto::Opcode::READ && (output.data() == nullptr || SIZE > UINTPTR_MAX - reinterpret_cast<uintptr_t>(output.data()))))
        return failure(-EINVAL);
    auto* task = sched::get_current_task();
    if (task == nullptr || task == s_worker || sched::preempt_count() != 0 ||
        (task->type != sched::task::TaskType::DAEMON && !frontend_process_can_wait(task))) {
        diagnose_task_gate(task);
        return failure(-EAGAIN);
    }
    auto const HOLD = sched::hold_kernel_shutdown();
    ProviderPin provider;
    rdma_registry::Binding binding{};
    int const AUTHORITY = capture_authority(identity, SERVICE, opcode, flags, binding, provider);
    if (AUTHORITY != 0) return failure(AUTHORITY);
    Job* job = nullptr;
    uint64_t token = 0;
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) return failure(-EAGAIN);
        size_t const PEER = peer_index(binding_peer(binding));
        if (s_stopping.load(std::memory_order_acquire) || PEER == WKI_MAX_PEERS || s_peers[PEER].closed.load(std::memory_order_acquire) ||
            s_peers[PEER].sid != binding.session_id || s_peers[PEER].incarnation != binding.secure_incarnation)
            return failure(-ESTALE);
        auto grant = std::find_if(s_remote.begin(), s_remote.end(), [&](auto const& value) {
            return value.live && same_binding(value.binding, binding) && same_descriptor(value.descriptor, remote);
        });
        if (grant == s_remote.end()) return failure(-ESTALE);
        auto available = std::find_if(s_jobs.begin(), s_jobs.end(),
                                      [](auto const& value) { return value.state.load(std::memory_order_acquire) == JobState::FREE; });
        if (available == s_jobs.end()) return failure(-EAGAIN);
        rdma_control::Arm arm{};
        arm.grant = remote;
        arm.offset = offset;
        arm.length = static_cast<uint32_t>(SIZE);
        arm.flags = flags;
        arm.opcode = static_cast<uint8_t>(opcode);
        arm.intent_length = static_cast<uint16_t>(intent.size());
        if (!intent.empty()) std::memcpy(arm.native_intent.data(), intent.data(), intent.size());
        arm.operation_id = reserve_node_id(s_next_operation);
        arm.receive_ticket = reserve_node_id(s_next_ticket);
        if (arm.operation_id == 0 || arm.receive_ticket == 0) return failure(-EOVERFLOW);
        // Reversing source/destination in this synthetic validation is harmless:
        // this is a local request check, never admission of an incoming record.
        if (!valid_arm(arm, binding)) return failure(-EACCES);
        if (!task->try_acquire()) return failure(-ECANCELED);
        job = &*available;
        job->outgoing = true;
        job->frontend = true;
        job->prepare_owner = task;
        job->arm = arm;
        job->binding = binding;
        job->deadline = deadline_after(wki_now_us(), OPERATION_US);
        job->provider = std::move(provider);
        token = arm.operation_id;
        auto const PREPARED = s_operations.prepare(
            model_spec(*job, opcode == proto::Opcode::WRITE ? ops::Role::SEND_WRITE : ops::Role::RECEIVE_READ), {}, job->operation);
        if (PREPARED != proto::Drop::NONE) {
            job_clear_locked(*job);
            return failure(native_status(PREPARED));
        }
        job->state.store(JobState::RESERVED, std::memory_order_release);
        if (opcode == proto::Opcode::WRITE) {
            auto const LOAN = s_operations.begin_prepare(job->operation, job->work);
            if (LOAN != proto::Drop::NONE) {
                std::exchange(job->prepare_owner, nullptr)->release();
                job->frontend = false;
                job->cancel.store(true, std::memory_order_release);
                job->state.store(JobState::CANCEL, std::memory_order_release);
                return failure(native_status(LOAN));
            }
        }
    }
    // No caller memory escapes this finite, nonyielding prepublication copy.
    proto::Drop sealed = proto::Drop::NONE;
    if (opcode == proto::Opcode::WRITE) {
        auto staging = job->work.prepare_buffer();
        std::memcpy(staging.data(), input.data(), SIZE);
        sealed = s_operations.seal(job->work);
        if (sealed != proto::Drop::NONE) job->work.reset();
    }
    {
        // This finite publication must complete while the preparing task still
        // executes. The worker's abandoned-copy collector requires GC's full
        // DEAD+epoch+CPU retirement proof before taking RESERVED ownership.
        s_lock.lock();
        std::exchange(job->prepare_owner, nullptr)->release();
        if (sealed != proto::Drop::NONE) {
            job->frontend = false;
            job->cancel.store(true, std::memory_order_release);
            job->state.store(JobState::CANCEL, std::memory_order_release);
            s_lock.unlock();
            wake_worker();
            return failure(native_status(sealed));
        }
        job->state.store(JobState::ARM_SEND, std::memory_order_release);
        s_lock.unlock();
    }
    wake_worker();
    while (true) {
        ops::Work result_loan;
        WkiSecureRdmaResult result{};
        proto::Opcode result_opcode = opcode;
        bool collected = false;
        {
            EngineGuard const GUARD(s_lock);
            if (GUARD) {
                if (job->state.load(std::memory_order_acquire) == JobState::FREE || job->arm.operation_id != token)
                    return {-ETIMEDOUT, 0,
                            opcode == proto::Opcode::WRITE ? WkiSecureRdmaOutcome::UNCERTAIN : WkiSecureRdmaOutcome::NO_EFFECT};
                if (job->done.load(std::memory_order_acquire)) {
                    ops::Result model{};
                    if (s_operations.take_result(job->operation, result_loan, model) == proto::Drop::NONE) {
                        result = job->result;
                        result_opcode = static_cast<proto::Opcode>(job->arm.opcode);
                        job->frontend = false;
                        collected = true;
                    }
                }
            }
        }
        if (collected) {
            if (opcode == proto::Opcode::READ && result.status == 0 && result.outcome == WkiSecureRdmaOutcome::EXACT) {
                if (result.bytes > output.size() || result.bytes > result_loan.staging().size())
                    return {-EIO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
                if (result.bytes != 0) std::memcpy(output.data(), result_loan.staging().data(), result.bytes);
            }
            // Loan retirement precedes return and cannot call native code or wait.
            result_loan.reset();
            count_client_result(binding, result_opcode, result);
            wake_worker();
            return result;
        }
        frontend_pause(identity);
    }
}
}  // namespace
auto wki_secure_rdma_write(const WkiChannelIdentity& identity, const proto::RegionDescriptor& remote, uint64_t offset,
                           std::span<const uint8_t> input, uint16_t flags, std::span<const uint8_t> native_intent) -> WkiSecureRdmaResult {
    return synchronous_transfer(identity, remote, offset, input, {}, proto::Opcode::WRITE, flags, native_intent);
}
auto wki_secure_rdma_read(const WkiChannelIdentity& identity, const proto::RegionDescriptor& remote, uint64_t offset,
                          std::span<uint8_t> output, std::span<const uint8_t> native_intent) -> WkiSecureRdmaResult {
    return synchronous_transfer(identity, remote, offset, {}, output, proto::Opcode::READ, 0, native_intent);
}
auto wki_secure_rdma_diagnostics(WkiSecureRdmaDiagnostics& output) -> bool {
    WkiSecureRdmaDiagnostics snapshot{};
    rdma_registry::Statistics regions{};
    ops::Statistics operations{};
    if (s_registry.statistics(regions) != proto::Drop::NONE ||
        (s_services != 0 && s_operations.statistics(operations) != proto::Drop::NONE))
        return false;
    {
        EngineGuard const GUARD(s_record_lock);
        if (!GUARD) return false;
        for (auto const& cell : s_records)
            if (cell.state != CellState::FREE) ++snapshot.queued_records;
    }
    {
        EngineGuard const GUARD(s_lock);
        if (!GUARD) return false;
        for (auto const& region : s_regions)
            if (region.live)
                snapshot.consumer_pins += region.references;
            else if (region.revoke_pending)
                ++snapshot.pending_revocations;
    }
    for (auto& context : s_peers) {
        rdma_session::Statistics session{};
        if (context.session.statistics(session) != proto::Drop::NONE) return false;
        snapshot.live_keys += session.live_keys;
        snapshot.replay_windows += session.replay_windows;
    }
    for (size_t index = 0; index < s_drops.size(); ++index) snapshot.drops[index] = s_drops[index].load(std::memory_order_relaxed);
    for (size_t index = 0; index < s_client_completions.size(); ++index) {
        snapshot.client_completions[index] = s_client_completions[index].load(std::memory_order_relaxed);
        snapshot.client_bytes[index] = s_client_bytes[index].load(std::memory_order_relaxed);
        snapshot.client_read_completions[index] = s_client_read_completions[index].load(std::memory_order_relaxed);
        snapshot.client_read_bytes[index] = s_client_read_bytes[index].load(std::memory_order_relaxed);
        snapshot.client_write_completions[index] = s_client_write_completions[index].load(std::memory_order_relaxed);
        snapshot.client_write_bytes[index] = s_client_write_bytes[index].load(std::memory_order_relaxed);
    }
    snapshot.live_regions = regions.live;
    snapshot.region_references = regions.references;
    snapshot.region_claims = regions.claims;
    snapshot.live_operations = operations.live;
    output = snapshot;
    return true;
}
auto wki_secure_rdma_shutdown() -> bool {
    if (!s_initialized.load(std::memory_order_acquire)) return true;
    s_stopping.store(true, std::memory_order_release);
    for (auto& peer : s_peers) peer.closed.store(true, std::memory_order_release);
    wake_worker();
    if (!s_worker_stopped.load(std::memory_order_acquire)) return false;
    bool expected = false;
    if (!s_service_owner.compare_exchange_strong(expected, true, std::memory_order_acquire)) return false;
    OwnerReset const OWNER(s_service_owner);
    if (s_services != 0) {
        EngineGuard const GUARD(s_lock);
        if (!GUARD || !drained_locked()) return false;
        if (s_operations.shutdown() != proto::Drop::NONE) return false;
    }
    // Owner reclaim is mandatory even when all keys/jobs are gone. Failed
    // teardown retains private root, region backing and the enclosing core.
    if (s_registry.shutdown() != proto::Drop::NONE) return false;
    if (s_scratch != nullptr) {
        auth_crypto::secure_erase(s_scratch, ops::SCRATCH_BYTES);
        ker::mod::mm::phys::page_free(std::exchange(s_scratch, nullptr));
    }
    s_initialized.store(false, std::memory_order_release);
    return true;
}

}  // namespace ker::net::wki
