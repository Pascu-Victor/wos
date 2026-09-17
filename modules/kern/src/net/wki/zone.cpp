#include "zone.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/wki/event.hpp>
#ifdef WOS_SELFTEST
#include <net/wki/dev_server.hpp>
#endif
#include <net/wki/transport_ivshmem.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <utility>
#include <vector>

#include "platform/mm/addr.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

// -----------------------------------------------------------------------------
// Zone table - static storage for all zones on this node
// -----------------------------------------------------------------------------

namespace {

auto destroy_zone(uint32_t zone_id, const WkiZone* expected) -> int;

std::array<WkiZone, WKI_MAX_ZONES> s_zone_table;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_zone_table_lock;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_zone_initialized = false;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

enum class ZoneCreateWorkKind : uint8_t {
    REQUEST,
    ACK,
};

struct ZoneCreateWork {
    ZoneCreateWorkKind kind = ZoneCreateWorkKind::REQUEST;
    WkiHeader header{};
    WkiChannelIdentity channel_identity{};
    ZoneCreateReqPayload request{};
    ZoneCreateAckPayload ack{};
};

void process_zone_create_request(const WkiHeader& header, const ZoneCreateReqPayload& request, const WkiChannelIdentity& identity);
void process_zone_create_ack(const WkiHeader& header, const ZoneCreateAckPayload& ack, const WkiChannelIdentity& identity);

// At most one request-side and one ACK-side setup can exist for each zone
// slot. Keeping both populations in fixed storage makes RX admission bounded
// without retaining a transport-owned payload pointer.
constexpr size_t WKI_ZONE_CREATE_WORK_CAPACITY = WKI_MAX_ZONES * 2;
constexpr uint32_t WKI_ZONE_WORK_SLICE_NS = 2'000'000;
constexpr int WKI_ZONE_WORK_NICE = -5;
std::array<ZoneCreateWork, WKI_ZONE_CREATE_WORK_CAPACITY>
    s_zone_create_work{};                                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_zone_create_work_head = 0;                           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_zone_create_work_tail = 0;                           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_zone_create_work_count = 0;                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_zone_create_work_lock;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_zone_create_work_pending{false};          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
mod::sched::task::Task* s_zone_create_worker_task = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto enqueue_zone_create_work(const ZoneCreateWork& work) -> bool {
    uint64_t const FLAGS = s_zone_create_work_lock.lock_irqsave();
    if (s_zone_create_work_count == s_zone_create_work.size()) {
        s_zone_create_work_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    s_zone_create_work.at(s_zone_create_work_tail) = work;
    s_zone_create_work_tail = (s_zone_create_work_tail + 1) % s_zone_create_work.size();
    ++s_zone_create_work_count;
    s_zone_create_work_pending.store(true, std::memory_order_release);
    auto* const WORKER = s_zone_create_worker_task;
    s_zone_create_work_lock.unlock_irqrestore(FLAGS);

    if (WORKER != nullptr) {
        mod::sched::wake_task_from_event(WORKER);
    }
    return true;
}

auto dequeue_zone_create_work(ZoneCreateWork* work) -> bool {
    if (work == nullptr) {
        return false;
    }

    uint64_t const FLAGS = s_zone_create_work_lock.lock_irqsave();
    if (s_zone_create_work_count == 0) {
        s_zone_create_work_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    *work = s_zone_create_work.at(s_zone_create_work_head);
    s_zone_create_work_head = (s_zone_create_work_head + 1) % s_zone_create_work.size();
    --s_zone_create_work_count;
    s_zone_create_work_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto zone_channel_live(const WkiChannelIdentity& identity) -> bool {
    return identity.channel_id == WKI_CHAN_ZONE_MGMT && identity.peer_node_id != WKI_NODE_INVALID &&
           identity.peer_node_id != WKI_NODE_BROADCAST &&
           wki_channel_generation_is_live(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation);
}

auto same_zone_channel(const WkiChannelIdentity& left, const WkiChannelIdentity& right) -> bool {
    return left.channel != nullptr && left.channel == right.channel && left.peer_node_id == right.peer_node_id &&
           left.channel_id == right.channel_id && left.generation == right.generation;
}

struct ZonePostNotification {
    WkiChannelIdentity channel{};
    ZoneNotifyPayload payload{};
};

// Caller retains this exact ACTIVE allocation. Its ID and channel identity
// stay immutable until the last pin releases; retirement only ends admission.
auto prepare_zone_post_notification(const WkiZone* zone, uint8_t op_type, ZonePostNotification& output) -> int {
    if (op_type > 1) return WKI_ERR_INVALID;  // rkey exchange is a separate setup operation
    if (zone == nullptr || zone->retiring.load(std::memory_order_acquire) || !zone_channel_live(zone->channel_identity) ||
        zone->retiring.load(std::memory_order_acquire))
        return WKI_ERR_ZONE_INACTIVE;
    ZonePostNotification prepared{};
    prepared.channel = zone->channel_identity;
    prepared.payload.zone_id = zone->zone_id;
    prepared.payload.op_type = op_type;
    output = prepared;
    return WKI_OK;
}

// Task-context local setup only. Release before waiting for a peer response.
class ZoneSetupLease {
   public:
    ZoneSetupLease() = default;
    ZoneSetupLease(const ZoneSetupLease&) = delete;
    auto operator=(const ZoneSetupLease&) -> ZoneSetupLease& = delete;
    ~ZoneSetupLease() { release(); }
    auto acquire(uint16_t node) -> bool {
        if (peer != nullptr) return false;
        WkiPeer* candidate = wki_peer_find(node);
        if (!wki_peer_lifecycle_acquire(candidate)) return false;
        peer = candidate;
        if (peer->node_id != node || peer->state != PeerState::CONNECTED ||
            peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
            release();
            return false;
        }
        return true;
    }
    auto try_acquire(uint16_t node) -> bool {
        if (peer != nullptr) return false;
        WkiPeer* candidate = wki_peer_find(node);
        if (!wki_peer_lifecycle_try_acquire(candidate)) return false;
        peer = candidate;
        if (peer->node_id != node || peer->state != PeerState::CONNECTED ||
            peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
            release();
            return false;
        }
        return true;
    }
    void release() {
        if (peer != nullptr) {
            wki_peer_lifecycle_release(peer);
            peer = nullptr;
        }
    }

   private:
    WkiPeer* peer = nullptr;
};

void process_zone_create_work(const ZoneCreateWork& work) {
    ZoneSetupLease lifecycle;
    if (!lifecycle.acquire(work.header.src_node) || work.channel_identity.peer_node_id != work.header.src_node ||
        !zone_channel_live(work.channel_identity))
        return;
    if (work.kind == ZoneCreateWorkKind::REQUEST) {
        process_zone_create_request(work.header, work.request, work.channel_identity);
    } else {
        process_zone_create_ack(work.header, work.ack, work.channel_identity);
    }
}

[[noreturn]] void zone_create_worker_thread() {
    for (;;) {
        if (!s_zone_create_work_pending.exchange(false, std::memory_order_acq_rel)) {
            mod::sched::kern_block();
            continue;
        }

        ZoneCreateWork work{};
        while (dequeue_zone_create_work(&work)) {
            process_zone_create_work(work);
        }
    }
}

auto find_zone_slot(uint32_t zone_id) -> WkiZone* {
    for (auto& zone : s_zone_table) {
        if (zone.state != ZoneState::NONE && zone.zone_id == zone_id) {
            return &zone;
        }
    }
    return nullptr;
}

auto alloc_zone_slot() -> WkiZone* {
    for (auto& zone : s_zone_table) {
        if (zone.state == ZoneState::NONE && !zone.retiring.load(std::memory_order_acquire) &&
            zone.refs.load(std::memory_order_acquire) == 0) {
            return &zone;
        }
    }
    return nullptr;
}

void free_zone_backing(WkiZone* zone) {
    if (zone->local_vaddr != nullptr) {
        if (zone->is_rdma && !zone->is_roce) {
            // ivshmem RDMA-backed: free from ivshmem RDMA pool
            auto offset = static_cast<int64_t>(zone->local_phys_addr);
            wki_ivshmem_rdma_free(offset, zone->size);
        } else {
            // RoCE-backed or non-RDMA: regular kernel pages
            ker::mod::mm::phys::page_free(zone->local_vaddr);
        }
    }
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->rdma_transport = nullptr;
}

auto allocate_zone_backing(uint32_t size) -> void* {
    // Allocate physically contiguous pages from buddy allocator
    return ker::mod::mm::phys::page_alloc(ker::mod::mm::PhysicalPageOwner::WKI_ZONE, size, "wki_zone");
}

auto zone_range_valid(uint32_t offset, uint32_t len, uint32_t size) -> bool { return offset <= size && len <= size - offset; }

auto allocate_zone_op_cookie_locked(WkiZone* zone) -> uint32_t {
    uint32_t cookie = zone->next_op_cookie;
    if (cookie == 0) {
        cookie = 1;
    }
    zone->next_op_cookie = cookie + 1U;
    if (zone->next_op_cookie == 0) {
        zone->next_op_cookie = 1;
    }
    return cookie;
}

auto zone_read_response_matches_pending(const WkiZone& zone, const ZoneReadRespPayload& resp) -> bool {
    return resp.op_cookie == zone.read_expected_cookie && resp.offset == zone.read_expected_offset && resp.length == zone.read_expected_len;
}

auto zone_write_ack_matches_pending(const WkiZone& zone, const ZoneWriteAckPayload& ack) -> bool {
    return ack.op_cookie == zone.write_expected_cookie && ack.offset == zone.write_expected_offset && ack.length == zone.write_expected_len;
}

void reset_zone_metadata(WkiZone* zone) {
    zone->zone_id = 0;
    zone->peer_node_id = WKI_NODE_INVALID;
    zone->channel_identity = {};
    zone->state = ZoneState::NONE;
    zone->size = 0;
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->access_policy = 0;
    zone->notify_mode = ZoneNotifyMode::NONE;
    zone->type_hint = ZoneTypeHint::BUFFER;
    zone->is_rdma = false;
    zone->is_roce = false;
    zone->remote_rkey = 0;
    zone->remote_phys_addr = 0;
    zone->rdma_transport = nullptr;
    zone->is_initiator = false;
    zone->pre_handler = nullptr;
    zone->post_handler = nullptr;
    zone->next_op_cookie = 1;
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_result_len = 0;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->read_status = 0;
    zone->read_wait_entry = nullptr;
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->write_status = 0;
    zone->write_wait_entry = nullptr;
}

void finalize_retired_zone_if_idle(WkiZone* zone) {
    if (zone == nullptr || !zone->retiring.load(std::memory_order_acquire) || zone->refs.load(std::memory_order_acquire) != 0) {
        return;
    }

    s_zone_table_lock.lock();
    if (zone->retiring.load(std::memory_order_acquire) && zone->refs.load(std::memory_order_acquire) == 0) {
        free_zone_backing(zone);
        reset_zone_metadata(zone);
        zone->retiring.store(false, std::memory_order_release);
    }
    s_zone_table_lock.unlock();
}

void retain_zone(WkiZone* zone) {
    if (zone != nullptr) {
        zone->refs.fetch_add(1, std::memory_order_acq_rel);
    }
}

void release_zone(WkiZone* zone) {
    if (zone == nullptr) {
        return;
    }
    uint32_t const PREV = zone->refs.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 1) {
        finalize_retired_zone_if_idle(zone);
    }
}

auto acquire_zone_slot(uint32_t zone_id) -> WkiZone* {
    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(zone_id);
    if (zone != nullptr && !zone->retiring.load(std::memory_order_acquire)) {
        retain_zone(zone);
    } else {
        zone = nullptr;
    }
    s_zone_table_lock.unlock();
    return zone;
}

void mark_zone_retiring_locked(WkiZone* zone) {
    if (zone == nullptr || zone->retiring.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    zone->state = ZoneState::NONE;
}

void retire_zone_after_timeout(WkiZone* zone) {
    s_zone_table_lock.lock();
    mark_zone_retiring_locked(zone);
    s_zone_table_lock.unlock();
}

// Caller must hold WkiZone::lock.
auto claim_and_clear_waiter_locked(WkiWaitEntry*& waiter_slot) -> WkiWaitEntry* {
    WkiWaitEntry* waiter = waiter_slot;
    waiter_slot = nullptr;
    if (!wki_claim_op(waiter)) {
        return nullptr;
    }
    return waiter;
}

void finish_claimed_waiter(WkiWaitEntry* waiter, int result) {
    if (waiter != nullptr) {
        wki_finish_claimed_op(waiter, result);
    }
}

void finish_or_wait_for_cancelled_waiter(WkiWaitEntry& wait, bool claimed, int result) {
    if (claimed) {
        wki_finish_claimed_op(&wait, result);
        return;
    }

    while (wait.state.load(std::memory_order_acquire) != static_cast<uint8_t>(WkiWaitEntry::DONE)) {
        ker::mod::sched::kern_yield();
    }
}

void cancel_read_waiter(WkiZone* zone, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    zone->lock.lock();
    if (zone->read_wait_entry == &wait) {
        zone->read_wait_entry = nullptr;
        zone->read_status = result;
        zone->read_pending.store(false, std::memory_order_release);
        zone->read_dest_buf = nullptr;
        zone->read_expected_offset = 0;
        zone->read_expected_len = 0;
        zone->read_expected_cookie = 0;
    }
    claimed = wki_claim_op(&wait);
    zone->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void cancel_write_waiter(WkiZone* zone, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    zone->lock.lock();
    if (zone->write_wait_entry == &wait) {
        zone->write_wait_entry = nullptr;
        zone->write_status = result;
        zone->write_pending.store(false, std::memory_order_release);
        zone->write_expected_offset = 0;
        zone->write_expected_len = 0;
        zone->write_expected_cookie = 0;
    }
    claimed = wki_claim_op(&wait);
    zone->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void clear_read_waiter_after_wait(WkiZone* zone, WkiWaitEntry& wait, int wait_result) {
    zone->lock.lock();
    if (zone->read_wait_entry == &wait) {
        zone->read_wait_entry = nullptr;
    }
    if (wait_result == WKI_ERR_TIMEOUT) {
        zone->read_status = WKI_ERR_ZONE_TIMEOUT;
        zone->read_pending.store(false, std::memory_order_release);
        zone->read_dest_buf = nullptr;
        zone->read_expected_offset = 0;
        zone->read_expected_len = 0;
        zone->read_expected_cookie = 0;
    }
    zone->lock.unlock();
}

void clear_write_waiter_after_wait(WkiZone* zone, WkiWaitEntry& wait, int wait_result) {
    zone->lock.lock();
    if (zone->write_wait_entry == &wait) {
        zone->write_wait_entry = nullptr;
    }
    if (wait_result == WKI_ERR_TIMEOUT) {
        zone->write_status = WKI_ERR_ZONE_TIMEOUT;
        zone->write_pending.store(false, std::memory_order_release);
        zone->write_expected_offset = 0;
        zone->write_expected_len = 0;
        zone->write_expected_cookie = 0;
    }
    zone->lock.unlock();
}

// Check if a peer has any RDMA-capable transport (ivshmem or RoCE)
auto peer_has_rdma(uint16_t node_id) -> bool {
    WkiPeer const* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return false;
    }
    // Check dedicated RDMA transport (RoCE or ivshmem set during HELLO)
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        return true;
    }
    // Fallback: check primary message transport (ivshmem has rdma_capable=true)
    if (peer->transport != nullptr && peer->transport->rdma_capable) {
        return true;
    }
    return false;
}

// Get the RDMA transport for a peer (prefers rdma_transport, falls back to transport)
auto peer_rdma_transport(uint16_t node_id) -> WkiTransport* {
    WkiPeer const* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return nullptr;
    }
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        return peer->rdma_transport;
    }
    if (peer->transport != nullptr && peer->transport->rdma_capable) {
        return peer->transport;
    }
    return nullptr;
}

// Check if the peer's RDMA transport is RoCE (not ivshmem shared memory)
[[maybe_unused]]
auto peer_rdma_is_roce(uint16_t node_id) -> bool {
    WkiPeer const* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return false;
    }
    // If rdma_transport is set and it's NOT the same as the primary transport,
    // it's the RoCE overlay. Also check if the primary transport is not rdma_capable.
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        // If the primary transport is also rdma_capable, it's ivshmem (preferred)
        return peer->transport == nullptr || !peer->transport->rdma_capable;
    }
    return false;
}

// Allocate RDMA-backed zone memory from ivshmem shared region.
// Returns the local virtual pointer and sets out_offset to the RDMA offset (used as rkey).
// Returns nullptr if RDMA allocation fails.
auto allocate_rdma_zone_backing(uint32_t size, int64_t& out_offset) -> void* {
    int64_t const OFFSET = wki_ivshmem_rdma_alloc(size);
    if (OFFSET < 0) {
        return nullptr;
    }
    out_offset = OFFSET;
    return wki_ivshmem_rdma_ptr(OFFSET);
}

// Allocate RoCE-backed zone memory: kernel pages registered with the RoCE transport.
// Each side has separate local memory; RDMA write/read must be used to synchronize.
// Returns the local virtual pointer and sets out_rkey to the registered RDMA key.
auto allocate_roce_zone_backing(WkiTransport* transport, uint32_t size, uint32_t& out_rkey) -> void* {
    if (transport == nullptr || transport->rdma_register_region == nullptr) {
        return nullptr;
    }
    void* backing = ker::mod::mm::phys::page_alloc(ker::mod::mm::PhysicalPageOwner::WKI_ZONE, size, "wki_zone");
    if (backing == nullptr) {
        return nullptr;
    }
    uint32_t rkey = 0;
    int const RET = transport->rdma_register_region(transport, reinterpret_cast<uint64_t>(backing), size, &rkey);
    if (RET != 0) {
        ker::mod::mm::phys::page_free(backing);
        return nullptr;
    }
    out_rkey = rkey;
    return backing;
}

struct PreparedZoneBacking {
    void* ptr = nullptr;
    uint64_t phys_addr = 0;
    uint32_t rkey = 0;
    WkiTransport* rdma_transport = nullptr;
    bool is_rdma = false;
    bool is_roce = false;
};

constexpr size_t WKI_ZONE_ZERO_YIELD_BYTES = static_cast<size_t>(8) * 1024 * 1024;

void zero_ivshmem_zone_backing(void* backing, uint32_t size) {
    auto* cursor = static_cast<uint64_t*>(backing);
    size_t remaining_words = size / sizeof(uint64_t);
    constexpr size_t WORDS_PER_SLICE = WKI_ZONE_ZERO_YIELD_BYTES / sizeof(uint64_t);

    while (remaining_words != 0) {
        size_t words = std::min(remaining_words, WORDS_PER_SLICE);
        remaining_words -= words;
        // BAR2 is mapped as device memory. Wide stores avoid turning a large
        // security clear into millions of byte MMIO transactions.
        asm volatile(
            "cld\n\t"
            "rep stosq\n\t"
            : "+D"(cursor), "+c"(words)
            : "a"(uint64_t{0})
            : "memory", "cc");  // NOLINT(hicpp-no-assembler)
        if (remaining_words != 0) {
            mod::sched::kern_yield();
        }
    }
}

void zero_prepared_zone_backing(const PreparedZoneBacking& backing, uint32_t size) {
    if (backing.is_rdma && !backing.is_roce) {
        zero_ivshmem_zone_backing(backing.ptr, size);
        return;
    }
    memset(backing.ptr, 0, size);
}

auto prepare_zone_backing(uint16_t peer, uint32_t size, bool allow_rdma) -> PreparedZoneBacking {
    PreparedZoneBacking result{};
    int64_t rdma_offset = -1;

    if (allow_rdma && peer_has_rdma(peer)) {
        result.ptr = allocate_rdma_zone_backing(size, rdma_offset);
        if (result.ptr != nullptr) {
            result.is_rdma = true;
            result.phys_addr = static_cast<uint64_t>(rdma_offset);
            result.rkey = static_cast<uint32_t>(rdma_offset);
        }

        if (result.ptr == nullptr) {
            WkiTransport* const ROCE = peer_rdma_transport(peer);
            if (ROCE != nullptr) {
                result.ptr = allocate_roce_zone_backing(ROCE, size, result.rkey);
                if (result.ptr != nullptr) {
                    result.is_rdma = true;
                    result.is_roce = true;
                    result.rdma_transport = ROCE;
                    result.phys_addr =
                        reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(result.ptr)));
                }
            }
        }
    }

    if (result.ptr == nullptr) {
        result.ptr = allocate_zone_backing(size);
        if (result.ptr != nullptr) {
            result.phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(result.ptr)));
        }
    }

    if (result.ptr != nullptr) {
        zero_prepared_zone_backing(result, size);
    }
    return result;
}

void release_prepared_zone_backing(PreparedZoneBacking* backing, uint32_t size) {
    if (backing == nullptr || backing->ptr == nullptr) {
        return;
    }
    if (backing->is_rdma && !backing->is_roce) {
        wki_ivshmem_rdma_free(static_cast<int64_t>(backing->phys_addr), size);
    } else {
        ker::mod::mm::phys::page_free(backing->ptr);
    }
    *backing = {};
}

}  // namespace

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_zone_init() {
    if (s_zone_initialized) {
        return;
    }

    for (auto& zone : s_zone_table) {
        zone.state = ZoneState::NONE;
        zone.zone_id = 0;
        zone.refs.store(0, std::memory_order_release);
        zone.retiring.store(false, std::memory_order_release);
    }

    s_zone_initialized = true;
    log::info("Zone subsystem initialized");
}

void wki_zone_worker_start() {
    if (s_zone_create_worker_task != nullptr) {
        return;
    }

    auto* task = mod::sched::task::Task::create_kernel_thread("wki_zone_setup", zone_create_worker_thread);
    if (task == nullptr) {
        log::error("Failed to create WKI zone setup worker");
        return;
    }
    task->slice_ns = WKI_ZONE_WORK_SLICE_NS;
    mod::sched::set_task_nice(task, WKI_ZONE_WORK_NICE);
    s_zone_create_worker_task = task;
    mod::sched::post_task_balanced(task);
    log::info("Zone setup worker started (PID %d)", task->pid);
}

// -----------------------------------------------------------------------------
// Public API - Zone creation
// -----------------------------------------------------------------------------

auto wki_zone_create(uint16_t peer, uint32_t zone_id, uint32_t size, uint8_t access_policy, ZoneNotifyMode notify, ZoneTypeHint hint,
                     WkiZoneRef* local_ref) -> int {
    if (!s_zone_initialized) {
        return WKI_ERR_INVALID;
    }

    uint8_t const LOCAL_ACCESS = access_policy & (ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE);
    if (local_ref != nullptr && (*local_ref || LOCAL_ACCESS == 0)) return WKI_ERR_INVALID;

    // Size must be page-aligned
    if (size == 0 || (size & 0xFFF) != 0) {
        return WKI_ERR_INVALID;
    }

    ZoneSetupLease lifecycle;
    // Device setup callers can retain a binding that peer cleanup must drain.
    // Never wait for that cleanup's lifecycle writer while holding its pin.
    if (!lifecycle.try_acquire(peer)) return WKI_ERR_BUSY;
    WkiChannel* channel = wki_channel_get(peer, WKI_CHAN_ZONE_MGMT);
    if (channel == nullptr) return WKI_ERR_NO_MEM;
    WkiChannelIdentity identity{};
    channel->lock.lock();
    if (channel->active && channel->peer_node_id == peer && channel->channel_id == WKI_CHAN_ZONE_MGMT) {
        identity = {channel, peer, WKI_CHAN_ZONE_MGMT, channel->generation};
    }
    channel->lock.unlock();
    if (!zone_channel_live(identity)) return WKI_ERR_NOT_FOUND;

    s_zone_table_lock.lock();

    // Check zone_id doesn't already exist
    if (find_zone_slot(zone_id) != nullptr) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_EXISTS;
    }

    // Allocate a slot
    WkiZone* zone = alloc_zone_slot();
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_NO_MEM;
    }

    // Set up zone in NEGOTIATING state
    zone->zone_id = zone_id;
    zone->peer_node_id = peer;
    zone->channel_identity = identity;
    zone->state = ZoneState::NEGOTIATING;
    zone->size = size;
    zone->access_policy = access_policy;
    zone->notify_mode = notify;
    zone->type_hint = hint;
    zone->is_initiator = true;
    zone->is_rdma = false;
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->remote_rkey = 0;
    zone->remote_phys_addr = 0;
    zone->pre_handler = nullptr;
    zone->post_handler = nullptr;
    zone->next_op_cookie = 1;
    zone->read_pending = false;
    zone->read_dest_buf = nullptr;
    zone->read_result_len = 0;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->read_wait_entry = nullptr;
    zone->read_status = 0;
    zone->write_pending = false;
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->write_wait_entry = nullptr;
    zone->write_status = 0;
    zone->retiring.store(false, std::memory_order_release);
    retain_zone(zone);

    s_zone_table_lock.unlock();

    // Send ZONE_CREATE_REQ
    ZoneCreateReqPayload req = {};
    req.zone_id = zone_id;
    req.size = size;
    req.access_policy = access_policy;
    req.notify_mode = static_cast<uint8_t>(notify);
    req.zone_type_hint = static_cast<uint8_t>(hint);

    // Set up wait entry before send (temporarily use read_wait_entry;
    // zone is NEGOTIATING so no reads are possible)
    WkiWaitEntry create_wait = {};
    zone->lock.lock();
    zone->read_wait_entry = &create_wait;
    zone->read_status = 0;
    zone->lock.unlock();

    int const RET = wki_send_on_channel_identity(identity, MsgType::ZONE_CREATE_REQ, &req, sizeof(req));
    if (RET != WKI_OK) {
        cancel_read_waiter(zone, create_wait, RET);
        s_zone_table_lock.lock();
        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();
        release_zone(zone);
        return RET;
    }

    lifecycle.release();

    // Wait for ACK via async wait queue
    int const WAIT_RC = wki_wait_for_op(&create_wait, WKI_OP_TIMEOUT_US);
    clear_read_waiter_after_wait(zone, create_wait, WAIT_RC);
    if (WAIT_RC == WKI_ERR_TIMEOUT) {
        // Timeout - clean up
        s_zone_table_lock.lock();
        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();
        release_zone(zone);
        return WKI_ERR_ZONE_TIMEOUT;
    }

    if (local_ref != nullptr) {
        // Transfer ownership from the exact initiating slot before releasing
        // its waiter pin. Numeric lookup after return could select a successor.
        *local_ref = WkiZoneRef::retain(zone, peer, LOCAL_ACCESS);
        if (!*local_ref) {
            static_cast<void>(destroy_zone(zone_id, zone));
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        release_zone(zone);
        return WKI_OK;
    }
    s_zone_table_lock.lock();
    bool const ACTIVE = zone->state == ZoneState::ACTIVE && !zone->retiring.load(std::memory_order_acquire);
    s_zone_table_lock.unlock();
    if (ACTIVE) {
        release_zone(zone);
        return WKI_OK;
    }

    // Zone was rejected - slot already cleaned up by ACK handler
    release_zone(zone);
    return WKI_ERR_ZONE_REJECTED;
}

// -----------------------------------------------------------------------------
// Public API - Zone destruction
// -----------------------------------------------------------------------------

namespace {

auto destroy_zone(uint32_t zone_id, const WkiZone* expected) -> int {
    WkiWaitEntry* read_waiter = nullptr;
    WkiWaitEntry* write_waiter = nullptr;
    s_zone_table_lock.lock();

    WkiZone* zone = find_zone_slot(zone_id);
    if (zone == nullptr || (expected != nullptr && zone != expected)) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_NOT_FOUND;
    }

    WkiChannelIdentity const IDENTITY = zone->channel_identity;

    mark_zone_retiring_locked(zone);
    zone->lock.lock();
    zone->read_status = -1;
    read_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->write_status = -1;
    write_waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->lock.unlock();

    s_zone_table_lock.unlock();

    finish_claimed_waiter(read_waiter, -1);
    finish_claimed_waiter(write_waiter, -1);
    finalize_retired_zone_if_idle(zone);

    // Notify peer
    ZoneDestroyPayload destroy = {};
    destroy.zone_id = zone_id;
    wki_send_on_channel_identity(IDENTITY, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy));

    log::info("Zone 0x%08x destroyed", zone_id);
    wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_DESTROYED, &zone_id, sizeof(zone_id));
    return WKI_OK;
}

}  // namespace

auto wki_zone_destroy(uint32_t zone_id) -> int { return destroy_zone(zone_id, nullptr); }

// -----------------------------------------------------------------------------
// Public API - Zone lookup
// -----------------------------------------------------------------------------

auto wki_zone_find(uint32_t zone_id) -> WkiZone* {
    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(zone_id);
    s_zone_table_lock.unlock();
    return zone;
}

// -----------------------------------------------------------------------------
// Public API - Zone read (message-based, blocking)
// -----------------------------------------------------------------------------

auto wki_zone_read(uint32_t zone_id, uint32_t offset, void* buf, uint32_t len) -> int {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr) {
        return WKI_ERR_ZONE_NOT_FOUND;
    }
    if (zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return WKI_ERR_ZONE_INACTIVE;
    }

    // Check access policy - we need REMOTE_READ on the peer's zone
    // (The remote side validates this too, but checking early avoids network round-trip)
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_READ) == 0) {
        release_zone(zone);
        return WKI_ERR_ZONE_ACCESS;
    }

    if (!zone_range_valid(offset, len, zone->size)) {
        release_zone(zone);
        return WKI_ERR_INVALID;
    }

    // For RDMA zones, the caller must retain a local WkiZoneRef
    if (zone->is_rdma) {
        if (zone->local_vaddr == nullptr) {
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        memcpy(buf, static_cast<uint8_t*>(zone->local_vaddr) + offset, len);
        release_zone(zone);
        return WKI_OK;
    }

    // Message-based: chunk if necessary
    auto* dest = static_cast<uint8_t*>(buf);
    uint32_t remaining = len;
    uint32_t cur_offset = offset;

    while (remaining > 0) {
        uint32_t chunk = remaining;
        chunk = std::min<size_t>(chunk, WKI_ZONE_MAX_MSG_DATA);

        // Set up pending read state
        WkiWaitEntry read_wait = {};
        zone->lock.lock();
        if (zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE) {
            zone->lock.unlock();
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        if (zone->read_pending.load(std::memory_order_acquire)) {
            zone->lock.unlock();
            release_zone(zone);
            return WKI_ERR_BUSY;
        }
        uint32_t const OP_COOKIE = allocate_zone_op_cookie_locked(zone);
        zone->read_wait_entry = &read_wait;
        zone->read_dest_buf = dest;
        zone->read_result_len = 0;
        zone->read_expected_offset = cur_offset;
        zone->read_expected_len = chunk;
        zone->read_expected_cookie = OP_COOKIE;
        zone->read_status = 0;
        zone->read_pending.store(true, std::memory_order_release);
        zone->lock.unlock();

        // Send ZONE_READ_REQ
        ZoneReadReqPayload req = {};
        req.zone_id = zone_id;
        req.offset = cur_offset;
        req.length = chunk;
        req.op_cookie = OP_COOKIE;

        int const RET = wki_send_on_channel_identity(zone->channel_identity, MsgType::ZONE_READ_REQ, &req, sizeof(req));
        if (RET != WKI_OK) {
            cancel_read_waiter(zone, read_wait, RET);
            release_zone(zone);
            return RET;
        }

        // Wait for response via async wait queue
        int const WAIT_RC = wki_wait_for_op(&read_wait, WKI_OP_TIMEOUT_US);
        clear_read_waiter_after_wait(zone, read_wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            retire_zone_after_timeout(zone);
            release_zone(zone);
            return WKI_ERR_ZONE_TIMEOUT;
        }

        if (zone->read_status != 0) {
            int const STATUS = zone->read_status;
            release_zone(zone);
            return STATUS;
        }

        dest += chunk;
        cur_offset += chunk;
        remaining -= chunk;
    }

    release_zone(zone);
    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API - Zone write (message-based, blocking)
// -----------------------------------------------------------------------------

auto wki_zone_write(uint32_t zone_id, uint32_t offset, const void* buf, uint32_t len) -> int {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr) {
        return WKI_ERR_ZONE_NOT_FOUND;
    }
    if (zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return WKI_ERR_ZONE_INACTIVE;
    }

    if ((zone->access_policy & ZONE_ACCESS_REMOTE_WRITE) == 0) {
        release_zone(zone);
        return WKI_ERR_ZONE_ACCESS;
    }

    if (!zone_range_valid(offset, len, zone->size)) {
        release_zone(zone);
        return WKI_ERR_INVALID;
    }

    // For RDMA zones, the caller must retain a local WkiZoneRef
    if (zone->is_rdma) {
        if (zone->local_vaddr == nullptr) {
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        memcpy(static_cast<uint8_t*>(zone->local_vaddr) + offset, buf, len);
        release_zone(zone);
        return WKI_OK;
    }

    // Message-based: chunk if necessary
    const auto* src = static_cast<const uint8_t*>(buf);
    uint32_t remaining = len;
    uint32_t cur_offset = offset;

    while (remaining > 0) {
        uint32_t chunk = remaining;
        chunk = std::min<size_t>(chunk, WKI_ZONE_MAX_MSG_DATA);

        // Build variable-length ZONE_WRITE_REQ: header + data
        auto msg_len = static_cast<uint16_t>(sizeof(ZoneWriteReqPayload) + chunk);
        auto* msg_buf = new (std::nothrow) uint8_t[msg_len];
        if (msg_buf == nullptr) {
            release_zone(zone);
            return WKI_ERR_ZONE_NO_MEM;
        }

        auto* req = reinterpret_cast<ZoneWriteReqPayload*>(msg_buf);
        req->zone_id = zone_id;
        req->offset = cur_offset;
        req->length = chunk;
        memcpy(msg_buf + sizeof(ZoneWriteReqPayload), src, chunk);

        // Set up pending write state
        WkiWaitEntry write_wait = {};
        zone->lock.lock();
        if (zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE) {
            zone->lock.unlock();
            delete[] msg_buf;
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        if (zone->write_pending.load(std::memory_order_acquire)) {
            zone->lock.unlock();
            delete[] msg_buf;
            release_zone(zone);
            return WKI_ERR_BUSY;
        }
        uint32_t const OP_COOKIE = allocate_zone_op_cookie_locked(zone);
        zone->write_wait_entry = &write_wait;
        zone->write_expected_offset = cur_offset;
        zone->write_expected_len = chunk;
        zone->write_expected_cookie = OP_COOKIE;
        zone->write_status = 0;
        zone->write_pending.store(true, std::memory_order_release);
        zone->lock.unlock();
        req->op_cookie = OP_COOKIE;

        int const RET = wki_send_on_channel_identity(zone->channel_identity, MsgType::ZONE_WRITE_REQ, msg_buf, msg_len);
        delete[] msg_buf;

        if (RET != WKI_OK) {
            cancel_write_waiter(zone, write_wait, RET);
            release_zone(zone);
            return RET;
        }

        // Wait for ACK via async wait queue
        int const WAIT_RC = wki_wait_for_op(&write_wait, WKI_OP_TIMEOUT_US);
        clear_write_waiter_after_wait(zone, write_wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            retire_zone_after_timeout(zone);
            release_zone(zone);
            return WKI_ERR_ZONE_TIMEOUT;
        }

        if (zone->write_status != 0) {
            int const STATUS = zone->write_status;
            release_zone(zone);
            return STATUS;
        }

        src += chunk;
        cur_offset += chunk;
        remaining -= chunk;
    }

    release_zone(zone);
    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API - RDMA direct access
// -----------------------------------------------------------------------------

WkiZoneRef::WkiZoneRef(WkiZoneRef&& other) noexcept
    : zone_(std::exchange(other.zone_, nullptr)), memory_(std::exchange(other.memory_, {})) {}

auto WkiZoneRef::operator=(WkiZoneRef&& other) noexcept -> WkiZoneRef& {
    if (this != &other) {
        reset();
        zone_ = std::exchange(other.zone_, nullptr);
        memory_ = std::exchange(other.memory_, {});
    }
    return *this;
}

WkiZoneRef::~WkiZoneRef() { reset(); }

void WkiZoneRef::reset() {
    WkiZone* old = std::exchange(zone_, nullptr);
    memory_ = {};
    release_zone(old);
}

auto WkiZoneRef::live() const -> bool {
    // The pin keeps identity immutable. Never nest the channel lock under the
    // zone/table lock: channel retirement may drive zone cleanup.
    return zone_ != nullptr && !zone_->retiring.load(std::memory_order_acquire) && zone_channel_live(zone_->channel_identity) &&
           !zone_->retiring.load(std::memory_order_acquire);
}

auto WkiZoneRef::snapshot_memory(WkiZoneMemory& output) const -> bool {
    if (!live()) return false;
    zone_->lock.lock();
    bool const ACTIVE = !zone_->retiring.load(std::memory_order_acquire);
    WkiZoneMemory const MEMORY{zone_->local_vaddr, zone_->size, zone_->is_roce, zone_->remote_rkey, zone_->rdma_transport};
    zone_->lock.unlock();
    if (!ACTIVE || !live()) return false;
    output = MEMORY;
    return true;
}

auto WkiZoneRef::retain(WkiZone* zone, uint16_t peer, uint8_t local_access) -> WkiZoneRef {
    WkiZoneRef result;
    constexpr uint8_t LOCAL_MASK = ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE;
    if (zone == nullptr || local_access == 0 || (local_access & ~LOCAL_MASK) != 0) return result;
    // Caller already owns a pin, so this exact slot cannot be recycled.
    s_zone_table_lock.lock();
    zone->lock.lock();
    if (!zone->retiring.load(std::memory_order_acquire) && zone->state == ZoneState::ACTIVE && zone->peer_node_id == peer &&
        zone->local_vaddr != nullptr && (zone->access_policy & local_access) == local_access) {
        retain_zone(zone);
        result.zone_ = zone;
        result.memory_ = {zone->local_vaddr, zone->size, zone->is_roce, zone->remote_rkey, zone->rdma_transport};
    }
    zone->lock.unlock();
    s_zone_table_lock.unlock();
    if (!result.live()) result.reset();
    return result;
}

auto WkiZoneRef::acquire(uint32_t zone_id, uint16_t peer, uint8_t local_access) -> WkiZoneRef {
    WkiZone* zone = acquire_zone_slot(zone_id);
    WkiZoneRef result = retain(zone, peer, local_access);
    release_zone(zone);
    return result;
}

auto WkiZoneRef::destroy() const -> int { return zone_ != nullptr ? destroy_zone(zone_->zone_id, zone_) : WKI_ERR_ZONE_NOT_FOUND; }

auto WkiZoneRef::notify_post(uint8_t op_type) const -> int {
    ZonePostNotification prepared{};
    int const RESULT = prepare_zone_post_notification(zone_, op_type, prepared);
    if (RESULT != WKI_OK) return RESULT;
    // Reliable enqueue rechecks this exact allocation under its channel lock.
    // A reset after preparation cannot redirect the wake to a new generation.
    return wki_send_on_channel_identity(prepared.channel, MsgType::ZONE_NOTIFY_POST, &prepared.payload, sizeof(prepared.payload));
}

auto WkiZoneRef::set_handlers(ZoneNotifyHandler pre, ZoneNotifyHandler post) const -> bool {
    if (!live()) return false;
    zone_->lock.lock();
    bool const ACTIVE = !zone_->retiring.load(std::memory_order_acquire);
    if (ACTIVE) {
        zone_->pre_handler = pre;
        zone_->post_handler = post;
    }
    zone_->lock.unlock();
    return ACTIVE;
}

// -----------------------------------------------------------------------------
// Public API - Notification handlers
// -----------------------------------------------------------------------------

void wki_zone_set_handlers(uint32_t zone_id, ZoneNotifyHandler pre, ZoneNotifyHandler post) {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr) {
        return;
    }
    zone->lock.lock();
    if (zone->state == ZoneState::ACTIVE && !zone->retiring.load(std::memory_order_acquire)) {
        zone->pre_handler = pre;
        zone->post_handler = post;
    }
    zone->lock.unlock();
    release_zone(zone);
}

// -----------------------------------------------------------------------------
// Public API - Fencing cleanup
// -----------------------------------------------------------------------------

void wki_zones_destroy_for_peer(uint16_t node_id) {
    while (true) {
        WkiZone* zone = nullptr;
        uint32_t zone_id = 0;
        WkiWaitEntry* read_waiter = nullptr;
        WkiWaitEntry* write_waiter = nullptr;

        s_zone_table_lock.lock();
        for (auto& candidate : s_zone_table) {
            if (candidate.state == ZoneState::NONE || candidate.peer_node_id != node_id) {
                continue;
            }

            zone = &candidate;
            zone_id = candidate.zone_id;
            mark_zone_retiring_locked(zone);

            zone->lock.lock();
            zone->read_status = -1;
            read_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
            zone->read_pending.store(false, std::memory_order_release);
            zone->read_dest_buf = nullptr;
            zone->read_expected_offset = 0;
            zone->read_expected_len = 0;
            zone->read_expected_cookie = 0;
            zone->write_status = -1;
            write_waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
            zone->write_pending.store(false, std::memory_order_release);
            zone->write_expected_offset = 0;
            zone->write_expected_len = 0;
            zone->write_expected_cookie = 0;
            zone->lock.unlock();
            break;
        }

        s_zone_table_lock.unlock();

        if (zone == nullptr) {
            break;
        }

        log::info("Destroying zone 0x%08x during peer cleanup (peer 0x%04x)", zone_id, node_id);
        finish_claimed_waiter(read_waiter, -1);
        finish_claimed_waiter(write_waiter, -1);
        finalize_retired_zone_if_idle(zone);
    }
}

// -----------------------------------------------------------------------------
// Public API - Zone listing
// -----------------------------------------------------------------------------

auto wki_zones_list() -> auto {
    s_zone_table_lock.lock();
    auto zones = std::vector<WkiZone*>{};
    for (auto& zone : s_zone_table) {
        if (zone.state != ZoneState::NONE) {
            zones.push_back(&zone);
        }
    }
    s_zone_table_lock.unlock();
    return zones;
}

#ifdef WOS_SELFTEST
auto wki_zone_selftest_timeout_retirement_fences_stale_completion() -> bool {
    WkiZone read_zone = {};
    WkiWaitEntry read_wait = {};

    read_zone.zone_id = 0xABCD;
    read_zone.peer_node_id = 0x1234;
    read_zone.state = ZoneState::ACTIVE;
    read_zone.read_pending.store(true, std::memory_order_release);
    read_zone.read_wait_entry = &read_wait;

    clear_read_waiter_after_wait(&read_zone, read_wait, WKI_ERR_TIMEOUT);
    retire_zone_after_timeout(&read_zone);

    bool const READ_FENCED = read_zone.retiring.load(std::memory_order_acquire) && read_zone.state == ZoneState::NONE &&
                             !read_zone.read_pending.load(std::memory_order_acquire) && read_zone.read_wait_entry == nullptr &&
                             read_zone.read_status == WKI_ERR_ZONE_TIMEOUT;

    WkiZone write_zone = {};
    WkiWaitEntry write_wait = {};

    write_zone.zone_id = 0xBCDE;
    write_zone.peer_node_id = 0x1234;
    write_zone.state = ZoneState::ACTIVE;
    write_zone.write_pending.store(true, std::memory_order_release);
    write_zone.write_wait_entry = &write_wait;

    clear_write_waiter_after_wait(&write_zone, write_wait, WKI_ERR_TIMEOUT);
    retire_zone_after_timeout(&write_zone);

    bool const WRITE_FENCED = write_zone.retiring.load(std::memory_order_acquire) && write_zone.state == ZoneState::NONE &&
                              !write_zone.write_pending.load(std::memory_order_acquire) && write_zone.write_wait_entry == nullptr &&
                              write_zone.write_status == WKI_ERR_ZONE_TIMEOUT;

    return READ_FENCED && WRITE_FENCED;
}

auto wki_zone_selftest_range_and_read_response_validation() -> bool {
    if (!zone_range_valid(0, 0, 4096) || !zone_range_valid(4096, 0, 4096) || !zone_range_valid(1024, 2048, 4096)) {
        return false;
    }
    if (zone_range_valid(4097, 0, 4096) || zone_range_valid(4000, 128, 4096) || zone_range_valid(0xFFFFF000U, 0x2000U, 0xFFFFFFFFU)) {
        return false;
    }

    WkiZone zone = {};
    zone.read_expected_offset = 128;
    zone.read_expected_len = 64;
    zone.read_expected_cookie = 77;

    ZoneReadRespPayload resp = {};
    resp.offset = 128;
    resp.length = 64;
    resp.op_cookie = 77;
    if (!zone_read_response_matches_pending(zone, resp)) {
        return false;
    }

    resp.op_cookie = 78;
    if (zone_read_response_matches_pending(zone, resp)) {
        return false;
    }

    resp.op_cookie = 77;
    resp.length = 65;
    if (zone_read_response_matches_pending(zone, resp)) {
        return false;
    }

    resp.length = 64;
    resp.offset = 129;
    return !zone_read_response_matches_pending(zone, resp);
}

auto wki_zone_selftest_waiter_slots_and_cookies() -> bool {
    WkiZone cookie_zone = {};
    cookie_zone.next_op_cookie = 0;
    if (allocate_zone_op_cookie_locked(&cookie_zone) != 1 || cookie_zone.next_op_cookie != 2) {
        return false;
    }

    cookie_zone.next_op_cookie = 0xFFFFFFFFU;
    if (allocate_zone_op_cookie_locked(&cookie_zone) != 0xFFFFFFFFU || cookie_zone.next_op_cookie != 1) {
        return false;
    }

    WkiZone read_zone = {};
    read_zone.read_expected_offset = 32;
    read_zone.read_expected_len = 16;
    read_zone.read_expected_cookie = 1234;

    ZoneReadRespPayload read_resp = {};
    read_resp.offset = 32;
    read_resp.length = 16;
    read_resp.op_cookie = 1235;
    if (zone_read_response_matches_pending(read_zone, read_resp)) {
        return false;
    }

    read_resp.op_cookie = 1234;
    if (!zone_read_response_matches_pending(read_zone, read_resp)) {
        return false;
    }

    WkiZone write_zone = {};
    write_zone.write_expected_offset = 64;
    write_zone.write_expected_len = 24;
    write_zone.write_expected_cookie = 4321;

    ZoneWriteAckPayload write_ack = {};
    write_ack.offset = 64;
    write_ack.length = 24;
    write_ack.op_cookie = 4322;
    if (zone_write_ack_matches_pending(write_zone, write_ack)) {
        return false;
    }

    write_ack.op_cookie = 4321;
    write_ack.offset = 65;
    if (zone_write_ack_matches_pending(write_zone, write_ack)) {
        return false;
    }

    write_ack.offset = 64;
    return zone_write_ack_matches_pending(write_zone, write_ack);
}
#endif

// -----------------------------------------------------------------------------
// RX Handlers - Zone negotiation
// -----------------------------------------------------------------------------

namespace {

void process_zone_create_request(const WkiHeader& header, const ZoneCreateReqPayload& request, const WkiChannelIdentity& identity) {
    uint16_t const SRC_NODE = header.src_node;

    // Validate size is page-aligned and non-zero
    if (request.size == 0 || (request.size & 0xFFF) != 0) {
        ZoneCreateAckPayload ack = {};
        ack.zone_id = request.zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_POLICY);
        static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack)));
        return;
    }

    s_zone_table_lock.lock();

    if (find_zone_slot(request.zone_id) != nullptr) {
        s_zone_table_lock.unlock();
        ZoneCreateAckPayload ack = {};
        ack.zone_id = request.zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_POLICY);
        static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack)));
        return;
    }

    WkiZone* zone = alloc_zone_slot();
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        ZoneCreateAckPayload ack = {};
        ack.zone_id = request.zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_NO_MEM);
        static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack)));
        return;
    }

    // Reserve the table identity before dropping the lock. A destroy or peer
    // fence can retire this NEGOTIATING row while allocation is in flight;
    // the retained reference prevents slot reuse until publication aborts.
    reset_zone_metadata(zone);
    zone->zone_id = request.zone_id;
    zone->peer_node_id = SRC_NODE;
    zone->channel_identity = identity;
    zone->state = ZoneState::NEGOTIATING;
    zone->size = request.size;
    zone->access_policy = request.access_policy;
    zone->notify_mode = static_cast<ZoneNotifyMode>(request.notify_mode);
    zone->type_hint = static_cast<ZoneTypeHint>(request.zone_type_hint);
    zone->is_initiator = false;
    zone->retiring.store(false, std::memory_order_release);
    retain_zone(zone);
    s_zone_table_lock.unlock();

    PreparedZoneBacking backing = prepare_zone_backing(SRC_NODE, request.size, true);
    bool published = false;
    bool reply = false;
    auto status = ZoneCreateStatus::REJECTED_NO_MEM;

    s_zone_table_lock.lock();
    bool const IDENTITY_LIVE = zone->state == ZoneState::NEGOTIATING && !zone->retiring.load(std::memory_order_acquire) &&
                               !zone->is_initiator && zone->zone_id == request.zone_id && zone->peer_node_id == SRC_NODE;
    if (IDENTITY_LIVE && backing.ptr != nullptr) {
        zone->local_vaddr = backing.ptr;
        zone->local_phys_addr = backing.phys_addr;
        zone->is_rdma = backing.is_rdma;
        zone->is_roce = backing.is_roce;
        zone->local_rkey = backing.rkey;
        zone->rdma_transport = backing.rdma_transport;
        zone->state = ZoneState::ACTIVE;
        published = true;
        reply = true;
        status = ZoneCreateStatus::ACCEPTED;
    } else if (IDENTITY_LIVE) {
        mark_zone_retiring_locked(zone);
        reply = true;
    }
    s_zone_table_lock.unlock();

    if (!published) {
        release_prepared_zone_backing(&backing, request.size);
    }

    ZoneCreateAckPayload ack = {};
    ack.zone_id = request.zone_id;
    ack.status = static_cast<uint8_t>(status);
    ack.phys_addr = published ? backing.phys_addr : 0;
    ack.rkey = published ? backing.rkey : 0;
    if (reply) {
        static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack)));
    }

    if (published) {
        log::info("Zone 0x%08x created (responder, peer 0x%04x, %u bytes, rdma=%d, roce=%d)", request.zone_id, SRC_NODE, request.size,
                  backing.is_rdma ? 1 : 0, backing.is_roce ? 1 : 0);
        wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_CREATED, &request.zone_id, sizeof(request.zone_id));
    }
    release_zone(zone);
}

void process_zone_create_ack(const WkiHeader& header, const ZoneCreateAckPayload& ack_payload, const WkiChannelIdentity& identity) {
    auto const STATUS = static_cast<ZoneCreateStatus>(ack_payload.status);
    WkiWaitEntry* create_waiter = nullptr;
    WkiZone* zone = nullptr;
    uint32_t zone_size = 0;

    s_zone_table_lock.lock();
    zone = find_zone_slot(ack_payload.zone_id);
    if (zone == nullptr || zone->state != ZoneState::NEGOTIATING || !zone->is_initiator || zone->peer_node_id != header.src_node ||
        !same_zone_channel(zone->channel_identity, identity) || zone->retiring.load(std::memory_order_acquire)) {
        s_zone_table_lock.unlock();
        if (STATUS == ZoneCreateStatus::ACCEPTED) {
            ZoneDestroyPayload destroy = {};
            destroy.zone_id = ack_payload.zone_id;
            static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy)));
        }
        return;
    }

    retain_zone(zone);
    zone_size = zone->size;
    zone->lock.lock();
    zone->read_status = 0;
    create_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    zone->read_pending.store(false, std::memory_order_release);
    zone->lock.unlock();

    if (create_waiter == nullptr) {
        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();
        if (STATUS == ZoneCreateStatus::ACCEPTED) {
            ZoneDestroyPayload destroy = {};
            destroy.zone_id = ack_payload.zone_id;
            static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy)));
        }
        release_zone(zone);
        return;
    }
    s_zone_table_lock.unlock();

    if (STATUS == ZoneCreateStatus::ACCEPTED) {
        PreparedZoneBacking backing = prepare_zone_backing(header.src_node, zone_size, ack_payload.rkey != 0);
        bool published = false;

        s_zone_table_lock.lock();
        bool const IDENTITY_LIVE = zone->state == ZoneState::NEGOTIATING && !zone->retiring.load(std::memory_order_acquire) &&
                                   zone->is_initiator && zone->zone_id == ack_payload.zone_id && zone->peer_node_id == header.src_node;
        if (IDENTITY_LIVE && backing.ptr != nullptr) {
            zone->local_vaddr = backing.ptr;
            zone->local_phys_addr = backing.phys_addr;
            zone->is_rdma = backing.is_rdma;
            zone->is_roce = backing.is_roce;
            zone->local_rkey = backing.rkey;
            zone->rdma_transport = backing.rdma_transport;
            zone->remote_phys_addr = ack_payload.phys_addr;
            zone->remote_rkey = ack_payload.rkey;
            zone->state = ZoneState::ACTIVE;
            published = true;
        } else if (IDENTITY_LIVE) {
            mark_zone_retiring_locked(zone);
        }
        s_zone_table_lock.unlock();

        if (!published) {
            release_prepared_zone_backing(&backing, zone_size);
            ZoneDestroyPayload destroy = {};
            destroy.zone_id = ack_payload.zone_id;
            static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy)));
        } else if (backing.is_roce && backing.rkey != 0) {
            // RoCE has separate local memory. Publish our key only after that
            // backing is active under the exact retained zone identity.
            ZoneNotifyPayload rkey_notify = {};
            rkey_notify.zone_id = ack_payload.zone_id;
            rkey_notify.offset = backing.rkey;
            rkey_notify.length = 0;
            rkey_notify.op_type = 0xFE;
            static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_NOTIFY_POST, &rkey_notify, sizeof(rkey_notify)));
        }

        if (published) {
            log::info("Zone 0x%08x active (initiator, peer 0x%04x, %u bytes, rdma=%d, roce=%d)", ack_payload.zone_id, header.src_node,
                      zone_size, backing.is_rdma ? 1 : 0, backing.is_roce ? 1 : 0);
            wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_CREATED, &ack_payload.zone_id, sizeof(ack_payload.zone_id));
        }
    } else {
        log::warn("Zone 0x%08x rejected by peer 0x%04x (status=%u)", ack_payload.zone_id, header.src_node, ack_payload.status);
        s_zone_table_lock.lock();
        if (zone->state == ZoneState::NEGOTIATING && zone->zone_id == ack_payload.zone_id && zone->peer_node_id == header.src_node) {
            mark_zone_retiring_locked(zone);
        }
        s_zone_table_lock.unlock();
    }

    finish_claimed_waiter(create_waiter, 0);
    release_zone(zone);
}

}  // namespace

namespace detail {

void handle_zone_create_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(ZoneCreateReqPayload)) {
        return;
    }

    ZoneCreateWork work{};
    work.kind = ZoneCreateWorkKind::REQUEST;
    work.header = *hdr;
    work.channel_identity = identity;
    std::memcpy(&work.request, payload, sizeof(work.request));
    if (!enqueue_zone_create_work(work)) {
        ZoneCreateAckPayload ack = {};
        ack.zone_id = work.request.zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_NO_MEM);
        static_cast<void>(wki_send_on_channel_identity(identity, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack)));
    }
}

void handle_zone_create_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(ZoneCreateAckPayload)) {
        return;
    }

    ZoneCreateWork work{};
    work.kind = ZoneCreateWorkKind::ACK;
    work.header = *hdr;
    work.channel_identity = identity;
    std::memcpy(&work.ack, payload, sizeof(work.ack));
    if (!enqueue_zone_create_work(work)) {
        log::error("Zone create ACK work queue exhausted: peer=0x%04x zone=0x%08x", hdr->src_node, work.ack.zone_id);
    }
}

void handle_zone_destroy(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneDestroyPayload)) {
        return;
    }

    const auto* destroy = reinterpret_cast<const ZoneDestroyPayload*>(payload);
    WkiWaitEntry* read_waiter = nullptr;
    WkiWaitEntry* write_waiter = nullptr;

    s_zone_table_lock.lock();

    WkiZone* zone = find_zone_slot(destroy->zone_id);
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        return;
    }

    // Only the peer that shares this zone can destroy it
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity)) {
        s_zone_table_lock.unlock();
        return;
    }

    log::info("Zone 0x%08x destroyed by peer 0x%04x", destroy->zone_id, hdr->src_node);

    mark_zone_retiring_locked(zone);
    zone->lock.lock();
    zone->read_status = -1;
    read_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->write_status = -1;
    write_waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->lock.unlock();

    s_zone_table_lock.unlock();

    finish_claimed_waiter(read_waiter, -1);
    finish_claimed_waiter(write_waiter, -1);
    finalize_retired_zone_if_idle(zone);
}

// -----------------------------------------------------------------------------
// RX Handlers - Zone notifications
// -----------------------------------------------------------------------------

void handle_zone_notify_pre(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneNotifyPayload)) {
        return;
    }

    const auto* notify = reinterpret_cast<const ZoneNotifyPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(notify->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity)) {
        release_zone(zone);
        return;
    }

    ZoneNotifyHandler handler = nullptr;
    zone->lock.lock();
    if (!zone->retiring.load(std::memory_order_acquire) && zone->state == ZoneState::ACTIVE) {
        handler = zone->pre_handler;
    }
    zone->lock.unlock();

    // Invoke pre-notification handler if registered
    if (handler != nullptr) {
        handler(notify->zone_id, notify->offset, notify->length, notify->op_type);
    }

    // Send ACK back to notifier
    ZoneNotifyAckPayload ack = {};
    ack.zone_id = notify->zone_id;
    wki_send_on_channel_identity(identity, MsgType::ZONE_NOTIFY_PRE_ACK, &ack, sizeof(ack));
    release_zone(zone);
}

void handle_zone_notify_post(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneNotifyPayload)) {
        return;
    }

    const auto* notify = reinterpret_cast<const ZoneNotifyPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(notify->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity)) {
        release_zone(zone);
        return;
    }

    // op_type=0xFE: rkey-exchange - the initiator is telling us its RDMA rkey
    // so we can write/read its zone memory.  Store it in remote_rkey.
    if (notify->op_type == 0xFE) {
        zone->lock.lock();
        zone->remote_rkey = notify->offset;  // rkey encoded in offset field
        zone->lock.unlock();
        // No ACK needed for rkey-exchange - the initiator doesn't wait.
        release_zone(zone);
        return;
    }

    ZoneNotifyHandler handler = nullptr;
    zone->lock.lock();
    if (!zone->retiring.load(std::memory_order_acquire) && zone->state == ZoneState::ACTIVE) {
        handler = zone->post_handler;
    }
    zone->lock.unlock();

    // Invoke post-notification handler if registered
    if (handler != nullptr) {
        handler(notify->zone_id, notify->offset, notify->length, notify->op_type);
    }

    // Send ACK back to notifier
    ZoneNotifyAckPayload ack = {};
    ack.zone_id = notify->zone_id;
    wki_send_on_channel_identity(identity, MsgType::ZONE_NOTIFY_POST_ACK, &ack, sizeof(ack));
    release_zone(zone);
}

// -----------------------------------------------------------------------------
// RX Handlers - Zone read (message-based)
// -----------------------------------------------------------------------------

void handle_zone_read_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneReadReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneReadReqPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(req->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity)) {
        release_zone(zone);
        return;
    }

    // Check access policy - peer wants to read our local data
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_READ) == 0) {
        release_zone(zone);
        return;
    }

    // Bounds check
    if (req->length > WKI_ZONE_MAX_MSG_DATA || !zone_range_valid(req->offset, req->length, zone->size)) {
        release_zone(zone);
        return;
    }

    // Build response: ZoneReadRespPayload + data
    uint32_t const DATA_LEN = req->length;
    auto resp_len = static_cast<uint16_t>(sizeof(ZoneReadRespPayload) + DATA_LEN);
    auto* resp_buf = new (std::nothrow) uint8_t[resp_len];
    if (resp_buf == nullptr) {
        release_zone(zone);
        return;
    }

    auto* resp = reinterpret_cast<ZoneReadRespPayload*>(resp_buf);
    resp->zone_id = req->zone_id;
    resp->offset = req->offset;
    resp->length = DATA_LEN;
    resp->op_cookie = req->op_cookie;

    // Copy data from local zone backing
    memcpy(resp_buf + sizeof(ZoneReadRespPayload), static_cast<uint8_t*>(zone->local_vaddr) + req->offset, DATA_LEN);

    wki_send_on_channel_identity(identity, MsgType::ZONE_READ_RESP, resp_buf, resp_len);
    delete[] resp_buf;
    release_zone(zone);
}

void handle_zone_read_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneReadRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const ZoneReadRespPayload*>(payload);
    WkiWaitEntry* waiter = nullptr;

    WkiZone* zone = acquire_zone_slot(resp->zone_id);
    if (zone == nullptr) {
        return;
    }

    zone->lock.lock();
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity) ||
        zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE ||
        !zone->read_pending.load(std::memory_order_acquire)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    if (resp->length > payload_len - sizeof(ZoneReadRespPayload) || !zone_read_response_matches_pending(*zone, *resp)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    if (waiter == nullptr) {
        zone->read_pending.store(false, std::memory_order_release);
        zone->read_dest_buf = nullptr;
        zone->read_expected_offset = 0;
        zone->read_expected_len = 0;
        zone->read_expected_cookie = 0;
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    // Copy data to the waiting buffer
    if (zone->read_dest_buf != nullptr && resp->length > 0) {
        memcpy(zone->read_dest_buf, payload + sizeof(ZoneReadRespPayload), resp->length);
    }

    zone->read_result_len = resp->length;
    zone->read_status = 0;
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->lock.unlock();
    finish_claimed_waiter(waiter, 0);
    release_zone(zone);
}

// -----------------------------------------------------------------------------
// RX Handlers - Zone write (message-based)
// -----------------------------------------------------------------------------

void handle_zone_write_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneWriteReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneWriteReqPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(req->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity)) {
        release_zone(zone);
        return;
    }

    // Check access policy - peer wants to write to our local data
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_WRITE) == 0) {
        ZoneWriteAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.offset = req->offset;
        ack.length = req->length;
        ack.status = WKI_ERR_ZONE_ACCESS;
        ack.op_cookie = req->op_cookie;
        wki_send_on_channel_identity(identity, MsgType::ZONE_WRITE_ACK, &ack, sizeof(ack));
        release_zone(zone);
        return;
    }

    // Validate data
    if (req->length > payload_len - sizeof(ZoneWriteReqPayload)) {
        release_zone(zone);
        return;
    }
    if (req->length > WKI_ZONE_MAX_MSG_DATA || !zone_range_valid(req->offset, req->length, zone->size)) {
        release_zone(zone);
        return;
    }

    // Copy data into local zone backing
    memcpy(static_cast<uint8_t*>(zone->local_vaddr) + req->offset, payload + sizeof(ZoneWriteReqPayload), req->length);

    // Send ACK
    ZoneWriteAckPayload ack = {};
    ack.zone_id = req->zone_id;
    ack.offset = req->offset;
    ack.length = req->length;
    ack.status = 0;  // success
    ack.op_cookie = req->op_cookie;
    wki_send_on_channel_identity(identity, MsgType::ZONE_WRITE_ACK, &ack, sizeof(ack));
    release_zone(zone);
}

void handle_zone_write_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity) {
    if (hdr == nullptr || payload == nullptr || hdr->channel_id != WKI_CHAN_ZONE_MGMT || identity.peer_node_id != hdr->src_node ||
        !zone_channel_live(identity))
        return;
    if (payload_len < sizeof(ZoneWriteAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const ZoneWriteAckPayload*>(payload);
    WkiWaitEntry* waiter = nullptr;

    WkiZone* zone = acquire_zone_slot(ack->zone_id);
    if (zone == nullptr) {
        return;
    }

    zone->lock.lock();
    if (zone->peer_node_id != hdr->src_node || !same_zone_channel(zone->channel_identity, identity) ||
        zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE ||
        !zone->write_pending.load(std::memory_order_acquire)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    if (!zone_write_ack_matches_pending(*zone, *ack)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
    if (waiter == nullptr) {
        zone->write_pending.store(false, std::memory_order_release);
        zone->write_expected_offset = 0;
        zone->write_expected_len = 0;
        zone->write_expected_cookie = 0;
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    zone->write_status = ack->status;
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->lock.unlock();
    finish_claimed_waiter(waiter, 0);
    release_zone(zone);
}

}  // namespace detail

#ifdef WOS_SELFTEST
constexpr uint32_t SELFTEST_LOCAL_ZONE = 0x7A530101;

auto wki_zone_selftest_with_local_backing(uint32_t size, bool (*test)(WkiZoneRef&)) -> bool {
    constexpr uint16_t PEER = 0x7A53;
    constexpr uint32_t ZONE = SELFTEST_LOCAL_ZONE;
    if (g_wki.initialized || s_zone_create_worker_task != nullptr || find_zone_slot(ZONE) != nullptr || test == nullptr) return false;
    void* memory = allocate_zone_backing(size);
    if (memory == nullptr) return false;
    std::memset(memory, 0, size);
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = PEER;
    channel.channel_id = WKI_CHAN_ZONE_MGMT;
    channel.generation = 27;
    s_zone_table_lock.lock();
    WkiZone* zone = alloc_zone_slot();
    if (zone != nullptr) {
        reset_zone_metadata(zone);
        zone->zone_id = ZONE;
        zone->peer_node_id = PEER;
        zone->channel_identity = {&channel, PEER, WKI_CHAN_ZONE_MGMT, channel.generation};
        zone->size = size;
        zone->local_vaddr = memory;
        zone->access_policy = ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE;
        zone->state = ZoneState::ACTIVE;
        retain_zone(zone);
    }
    s_zone_table_lock.unlock();
    if (zone == nullptr) {
        ker::mod::mm::phys::page_free(memory);
        return false;
    }
    WkiZoneRef ref = WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE);
    // The original snapshot stays unchanged; a locked refresh observes late
    // metadata on this allocation without another ID lookup.
    zone->lock.lock();
    zone->remote_rkey = 123;
    zone->lock.unlock();
    WkiZoneMemory snapshot{};
    bool valid = ref.memory().remote_rkey == 0 && ref.snapshot_memory(snapshot) && snapshot.remote_rkey == 123 && snapshot.data == memory;
    valid = (ref && test(ref)) && valid;
    wki_zones_destroy_for_peer(PEER);
    WkiZoneMemory sentinel{.data = memory, .size = 19, .remote_rkey = 456};
    valid = valid && !ref.snapshot_memory(sentinel) && sentinel.data == memory && sentinel.size == 19 && sentinel.remote_rkey == 456;
    ref.reset();
    release_zone(zone);
    return valid && zone->refs.load(std::memory_order_acquire) == 0 && zone->local_vaddr == nullptr && find_zone_slot(ZONE) == nullptr;
}

auto wki_zone_selftest_post_notification_authority() -> bool {
    return wki_zone_selftest_with_local_backing(4096, [](WkiZoneRef& ref) -> bool {
        WkiZone* zone = acquire_zone_slot(SELFTEST_LOCAL_ZONE);
        if (zone == nullptr) return false;
        ZonePostNotification prepared{};
        bool valid = prepare_zone_post_notification(zone, 1, prepared) == WKI_OK &&
                     same_zone_channel(prepared.channel, zone->channel_identity) && prepared.payload.zone_id == SELFTEST_LOCAL_ZONE &&
                     prepared.payload.op_type == 1 && prepared.payload.offset == 0 && prepared.payload.length == 0;
        ZonePostNotification const ORIGINAL = prepared;
        valid = prepare_zone_post_notification(zone, 0, prepared) == WKI_OK && prepared.payload.op_type == 0 && valid;
        prepared.payload.zone_id = 99;
        valid = prepare_zone_post_notification(zone, 0xFE, prepared) == WKI_ERR_INVALID && prepared.payload.zone_id == 99 && valid;
        // A live grant reaches the ordinary uninitialized-transport check.
        // The fixture never enables WKI or transmits a network frame.
        valid = ref.notify_post(1) == WKI_ERR_INVALID && valid;
        WkiChannel* channel = ORIGINAL.channel.channel;
        valid = wki_channel_close_generation(channel, ORIGINAL.channel.peer_node_id, ORIGINAL.channel.channel_id,
                                             ORIGINAL.channel.generation) &&
                valid;
        valid = ref.notify_post(1) == WKI_ERR_ZONE_INACTIVE && valid;
        ++channel->generation;
        channel->active = true;
        auto const CREDITS = channel->tx_credits;
        auto const SEQUENCE = channel->tx_seq;
        valid = prepare_zone_post_notification(zone, 1, prepared) == WKI_ERR_ZONE_INACTIVE && prepared.payload.zone_id == 99 && valid;
        valid = ref.notify_post(0) == WKI_ERR_ZONE_INACTIVE && channel->tx_credits == CREDITS && channel->tx_seq == SEQUENCE &&
                ORIGINAL.channel.generation != channel->generation && ORIGINAL.payload.zone_id == SELFTEST_LOCAL_ZONE && valid;
        valid = ref.destroy() == WKI_OK && valid;
        valid = ref.notify_post(1) == WKI_ERR_ZONE_INACTIVE && valid;
        release_zone(zone);
        return valid;
    });
}

auto wki_zone_selftest_local_backing_retention() -> bool {
    constexpr uint16_t PEER = 0x7A52;
    constexpr uint32_t ZONE = 0x7A520101;
    if (g_wki.initialized || s_zone_create_worker_task != nullptr || find_zone_slot(ZONE) != nullptr) return false;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = PEER;
    channel.channel_id = WKI_CHAN_ZONE_MGMT;
    channel.generation = 17;
    auto prepare = [&](uint32_t size) -> WkiZone* {
        void* memory = allocate_zone_backing(size);
        if (memory == nullptr) return nullptr;
        std::memset(memory, 0, size);
        s_zone_table_lock.lock();
        WkiZone* zone = alloc_zone_slot();
        if (zone != nullptr) {
            reset_zone_metadata(zone);
            zone->zone_id = ZONE;
            zone->peer_node_id = PEER;
            zone->channel_identity = {&channel, PEER, WKI_CHAN_ZONE_MGMT, channel.generation};
            zone->size = size;
            zone->local_vaddr = memory;
            zone->access_policy = ZONE_ACCESS_LOCAL_READ;
            zone->state = ZoneState::ACTIVE;
            retain_zone(zone);
        }
        s_zone_table_lock.unlock();
        if (zone == nullptr) ker::mod::mm::phys::page_free(memory);
        return zone;
    };
    WkiZone* zone = prepare(blk_ring_default_zone_size());
    if (zone == nullptr) return false;
    bool valid = !WkiZoneRef::acquire(ZONE, PEER + 1, ZONE_ACCESS_LOCAL_READ) &&
                 !WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_WRITE) && !WkiZoneRef::acquire(ZONE, PEER, 0) &&
                 !WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_REMOTE_READ) && zone->refs.load(std::memory_order_acquire) == 1;
    WkiZoneRef read = WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ);
    valid = valid && read.live() && read.memory().data == zone->local_vaddr && read.memory().size == zone->size;
    read.reset();
    zone->lock.lock();
    zone->access_policy |= ZONE_ACCESS_LOCAL_WRITE;
    zone->lock.unlock();
    WkiZoneRef ring = WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE);
    release_zone(zone);
    valid = valid && ring.live() && zone->refs.load(std::memory_order_acquire) == 1;
    if (ring) {
        // Actual binding moves and poll path: one valid FLUSH completes, then
        // retirement leaves the next SQE and CQ state untouched.
        valid = wki_dev_server_selftest_zone_retention(ring) && valid;
        valid = valid && !ring.live() && ring.memory().data == zone->local_vaddr && zone->local_vaddr != nullptr &&
                !WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ) && !ring.set_handlers(nullptr, nullptr);
        WkiZone* replacement = prepare(4096);
        valid = valid && replacement != nullptr && replacement != zone;
        if (replacement != nullptr) {
            // Retained old storage cannot destroy a new zone with the same ID.
            int const OLD_DESTROY = ring.destroy();
            valid = valid && OLD_DESTROY == WKI_ERR_ZONE_NOT_FOUND && replacement->state == ZoneState::ACTIVE;
            WkiZoneRef fresh = WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ);
            valid = valid && fresh.live() && fresh.memory().data == replacement->local_vaddr;
            valid = wki_channel_close_generation(&channel, PEER, WKI_CHAN_ZONE_MGMT, channel.generation) && valid;
            valid = valid && !fresh.live() && !WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ);
            ++channel.generation;
            channel.active = true;
            valid = valid && !fresh.live() && !WkiZoneRef::acquire(ZONE, PEER, ZONE_ACCESS_LOCAL_READ);
            static_cast<void>(fresh.destroy());
            fresh.reset();
            release_zone(replacement);
            valid = valid && replacement->local_vaddr == nullptr && replacement->refs.load(std::memory_order_acquire) == 0;
        }
        ring.reset();
    } else {
        wki_zones_destroy_for_peer(PEER);
    }
    valid = valid && zone->local_vaddr == nullptr && zone->refs.load(std::memory_order_acquire) == 0 && find_zone_slot(ZONE) == nullptr;
    return valid;
}

auto wki_zone_selftest_deferred_channel_retirement() -> bool {
    constexpr uint16_t PEER = 0x7A51;
    constexpr uint32_t ZONE = 0x7A510101;
    if (g_wki.initialized || s_zone_create_worker_task != nullptr || s_zone_create_work_count != 0 || wki_peer_find(PEER) != nullptr ||
        find_zone_slot(ZONE) != nullptr)
        return false;
    auto const OLD_HASH = g_wki.peer_hash;
    auto const OLD_COUNT = g_wki.peer_count;
    auto* peer = wki_peer_alloc(PEER);
    if (peer == nullptr) return false;
    peer->state = PeerState::CONNECTED;
    bool const WAS_INITIALIZED = s_zone_initialized;
    s_zone_initialized = true;
    bool const CLAIMED = wki_peer_lifecycle_acquire(peer);
    int const CONTENDED_CREATE = wki_zone_create(PEER, ZONE, 4096, ZONE_ACCESS_REMOTE_READ, ZoneNotifyMode::NONE);
    if (CLAIMED) wki_peer_lifecycle_release(peer);
    s_zone_initialized = WAS_INITIALIZED;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = PEER;
    channel.channel_id = WKI_CHAN_ZONE_MGMT;
    channel.generation = 7;
    WkiChannelIdentity const OLD{&channel, PEER, WKI_CHAN_ZONE_MGMT, 7};
    WkiHeader header{};
    header.src_node = PEER;
    header.channel_id = WKI_CHAN_ZONE_MGMT;
    ZoneCreateReqPayload request{};
    request.zone_id = ZONE;
    request.size = 4096;
    request.access_policy = ZONE_ACCESS_REMOTE_READ | ZONE_ACCESS_REMOTE_WRITE;

    detail::handle_zone_create_req(&header, reinterpret_cast<const uint8_t*>(&request), sizeof(request), OLD);
    ZoneCreateWork popped{};
    bool const POPPED = dequeue_zone_create_work(&popped);
    bool valid = CLAIMED && CONTENDED_CREATE == WKI_ERR_BUSY && POPPED && same_zone_channel(popped.channel_identity, OLD);
    valid = wki_channel_close_generation(&channel, PEER, WKI_CHAN_ZONE_MGMT, OLD.generation) && valid;
    process_zone_create_work(popped);
    valid = valid && find_zone_slot(ZONE) == nullptr;
    // Closing makes the slot inactive; channel_init advances generation only
    // when a subsequent allocation republishes that slot.
    ++channel.generation;
    channel.active = true;
    WkiChannelIdentity const CURRENT{&channel, PEER, WKI_CHAN_ZONE_MGMT, channel.generation};
    process_zone_create_work(popped);
    valid = valid && find_zone_slot(ZONE) == nullptr;

    // Current work is also refused while connected-epoch cleanup owns admission.
    popped.channel_identity = CURRENT;
    peer->vfs_reset_rebind_pending.store(true, std::memory_order_release);
    process_zone_create_work(popped);
    valid = valid && find_zone_slot(ZONE) == nullptr;
    peer->vfs_reset_rebind_pending.store(false, std::memory_order_release);
    process_zone_create_work(popped);
    WkiZone* zone = acquire_zone_slot(ZONE);
    valid = valid && zone != nullptr && zone->state == ZoneState::ACTIVE && same_zone_channel(zone->channel_identity, CURRENT);
    if (zone != nullptr) {
        struct Write {
            ZoneWriteReqPayload request;
            std::array<uint8_t, 4> data;
        } __attribute__((packed));
        Write write{};
        write.request.zone_id = ZONE;
        write.request.length = write.data.size();
        write.request.op_cookie = 1;
        write.data = {1, 2, 3, 4};
        detail::handle_zone_write_req(&header, reinterpret_cast<const uint8_t*>(&write), sizeof(write), CURRENT);
        valid = valid && std::memcmp(zone->local_vaddr, write.data.data(), write.data.size()) == 0;
        valid = wki_channel_close_generation(&channel, PEER, WKI_CHAN_ZONE_MGMT, CURRENT.generation) && valid;
        ++channel.generation;
        channel.active = true;
        WkiChannelIdentity const REPLACEMENT{&channel, PEER, WKI_CHAN_ZONE_MGMT, channel.generation};
        write.data = {5, 6, 7, 8};
        detail::handle_zone_write_req(&header, reinterpret_cast<const uint8_t*>(&write), sizeof(write), REPLACEMENT);
        std::array<uint8_t, 4> const ORIGINAL{1, 2, 3, 4};
        valid = valid && std::memcmp(zone->local_vaddr, ORIGINAL.data(), ORIGINAL.size()) == 0;
        wki_zones_destroy_for_peer(PEER);
        valid = valid && zone->retiring.load(std::memory_order_acquire) && zone->local_vaddr != nullptr;
        release_zone(zone);

        // An old queued ACK cannot claim the new incarnation's stack waiter.
        WkiWaitEntry waiter{};
        s_zone_table_lock.lock();
        zone = alloc_zone_slot();
        if (zone != nullptr) {
            reset_zone_metadata(zone);
            zone->zone_id = ZONE;
            zone->peer_node_id = PEER;
            zone->channel_identity = REPLACEMENT;
            zone->state = ZoneState::NEGOTIATING;
            zone->is_initiator = true;
            zone->size = 4096;
            zone->read_wait_entry = &waiter;
            retain_zone(zone);
        }
        s_zone_table_lock.unlock();
        valid = valid && zone != nullptr;
        if (zone != nullptr) {
            ZoneCreateWork ack{};
            ack.kind = ZoneCreateWorkKind::ACK;
            ack.header = header;
            ack.channel_identity = CURRENT;
            ack.ack.zone_id = ZONE;
            ack.ack.status = static_cast<uint8_t>(ZoneCreateStatus::ACCEPTED);
            process_zone_create_work(ack);
            valid = valid && zone->read_wait_entry == &waiter && zone->state == ZoneState::NEGOTIATING &&
                    waiter.state.load(std::memory_order_acquire) != WkiWaitEntry::DONE;
            ack.channel_identity = REPLACEMENT;
            process_zone_create_work(ack);
            valid = valid && zone->state == ZoneState::ACTIVE && zone->read_wait_entry == nullptr && zone->local_vaddr != nullptr &&
                    waiter.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE;
            wki_zones_destroy_for_peer(PEER);
            release_zone(zone);
        }
    }
    wki_zones_destroy_for_peer(PEER);
    valid = valid && find_zone_slot(ZONE) == nullptr && peer->lifecycle_state.load(std::memory_order_acquire) == 0;
    s_zone_create_work_pending.store(false, std::memory_order_release);
    peer->~WkiPeer();
    new (peer) WkiPeer();
    g_wki.peer_hash = OLD_HASH;
    g_wki.peer_count = OLD_COUNT;
    return valid;
}
#endif

}  // namespace ker::net::wki
