#include "dev_server.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <list>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/transport_roce.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <utility>
#include <vfs/mount.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {
constexpr uint32_t WKI_VFS_DAEMON_SLICE_NS = 2'000'000;
constexpr int WKI_VFS_DAEMON_NICE = -5;

std::list<DevServerBinding> g_bindings;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_dev_server_initialized = false;   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_server_lock;   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct DeferredVfsOp {
    WkiHeader hdr{};
    uint16_t op_id = 0;
    uint16_t req_data_len = 0;
    uint8_t* req_data = nullptr;
    DeferredVfsOp* next = nullptr;
};

constexpr size_t VFS_OP_WORKER_COUNT = 16;

struct VfsOpWorkerShard {
    ker::mod::sys::Spinlock lock;
    DeferredVfsOp* head = nullptr;
    DeferredVfsOp* tail = nullptr;
    ker::mod::sched::task::Task* task = nullptr;
    std::atomic<uint32_t> pending{0};
};

std::array<VfsOpWorkerShard, VFS_OP_WORKER_COUNT> s_vfs_op_workers;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void promote_vfs_worker(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = WKI_VFS_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, WKI_VFS_DAEMON_NICE);
}

struct CachedVfsWriteResponse {
    bool found = false;
    uint16_t cookie = 0;
    int16_t status = 0;
    uint32_t bytes_written = 0;
};

// s_server_lock must be held by caller
auto find_binding_by_channel(uint16_t consumer_node, uint16_t channel_id) -> DevServerBinding* {
    for (auto& b : g_bindings) {
        if (b.active && !b.retiring.load(std::memory_order_acquire) && b.consumer_node == consumer_node &&
            b.assigned_channel == channel_id) {
            return &b;
        }
    }
    return nullptr;
}

// s_server_lock must be held by caller
auto find_binding_by_resource(uint16_t consumer_node, ResourceType resource_type, uint32_t resource_id) -> DevServerBinding* {
    for (auto& b : g_bindings) {
        if (b.active && !b.retiring.load(std::memory_order_acquire) && b.consumer_node == consumer_node &&
            b.resource_type == resource_type && b.resource_id == resource_id) {
            return &b;
        }
    }
    return nullptr;
}

// s_server_lock must be held by caller
auto find_binding_by_zone_id(uint32_t zone_id) -> DevServerBinding* {
    for (auto& b : g_bindings) {
        if (b.active && !b.retiring.load(std::memory_order_acquire) && b.blk_rdma_active && b.blk_zone_id == zone_id) {
            return &b;
        }
    }
    return nullptr;
}

// s_server_lock must be held by caller
auto retain_binding_locked(DevServerBinding* binding) -> bool {
    if (binding == nullptr || !binding->active || binding->retiring.load(std::memory_order_acquire)) {
        return false;
    }
    binding->refs.fetch_add(1, std::memory_order_acq_rel);
    return true;
}

void release_binding(DevServerBinding* binding) {
    if (binding == nullptr) {
        return;
    }
    uint32_t const PREV = binding->refs.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 0) {
        binding->refs.store(0, std::memory_order_release);
    }
}

void wait_for_binding_refs_to_drain(DevServerBinding* binding) {
    while (binding != nullptr && binding->refs.load(std::memory_order_acquire) != 0) {
        ker::mod::sched::kern_yield();
    }
}

// s_server_lock must be held by caller
void mark_binding_retiring_locked(DevServerBinding& binding) {
    binding.active = false;
    binding.retiring.store(true, std::memory_order_release);
}

// s_server_lock must be held by caller
auto erase_retired_binding_locked(DevServerBinding* binding) -> bool {
    if (binding == nullptr || binding->refs.load(std::memory_order_acquire) != 0) {
        return false;
    }
    for (auto it = g_bindings.begin(); it != g_bindings.end(); ++it) {
        if (&*it == binding && !it->active) {
            g_bindings.erase(it);
            return true;
        }
    }
    return false;
}

auto find_block_device_by_resource_id(uint32_t resource_id) -> ker::dev::BlockDevice* {
    return ker::dev::block_device_at(static_cast<size_t>(resource_id));
}

auto find_net_device_by_resource_id(uint32_t resource_id) -> ker::net::NetDevice* {
    size_t const COUNT = ker::net::netdev_count();
    for (size_t i = 0; i < COUNT; i++) {
        ker::net::NetDevice* ndev = ker::net::netdev_at(i);
        if (ndev != nullptr && ndev->ifindex == resource_id) {
            return ndev;
        }
    }
    return nullptr;
}

// s_server_lock must be held by caller
auto block_has_remote_writer_locked(const ker::dev::BlockDevice* dev) -> bool {
    if (dev == nullptr) {
        return false;
    }
    return std::ranges::any_of(g_bindings, [dev](const DevServerBinding& binding) {
        return binding.active && !binding.retiring.load(std::memory_order_acquire) && binding.resource_type == ResourceType::BLOCK &&
               !binding.block_read_only && ker::dev::block_devices_overlap(binding.block_dev, dev);
    });
}

auto is_vfs_op(uint16_t op_id) -> bool { return op_id >= OP_VFS_OPEN && op_id <= OP_VFS_READ_BULK; }

auto transport_is_roce(const WkiTransport* transport) -> bool {
    return transport != nullptr && transport->name != nullptr && std::strcmp(transport->name, "wki-roce") == 0;
}

auto req_cookie_from_header(const WkiHeader* hdr) -> uint16_t {
    return hdr != nullptr ? static_cast<uint16_t>(hdr->seq_num & UINT16_MAX) : 0;
}

void send_vfs_error_response(uint16_t dst_node, uint16_t channel_id, uint16_t op_id, int16_t status, uint16_t req_cookie) {
    DevOpRespPayload resp = {};
    resp.op_id = op_id;
    resp.status = status;
    resp.data_len = 0;
    resp.reserved = req_cookie;
    wki_send(dst_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
}

void send_cached_vfs_write_response(uint16_t dst_node, uint16_t channel_id, const CachedVfsWriteResponse& cached) {
    std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> resp_buf{};
    auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
    resp->op_id = OP_VFS_WRITE_RDMA;
    resp->status = cached.status;
    resp->reserved = cached.cookie;
    if (cached.status >= 0) {
        resp->data_len = sizeof(uint32_t);
        std::memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &cached.bytes_written, sizeof(uint32_t));
        wki_send(dst_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
        return;
    }

    resp->data_len = 0;
    wki_send(dst_node, channel_id, MsgType::DEV_OP_RESP, resp, sizeof(*resp));
}

void clear_vfs_write_active(uint16_t consumer_node, uint16_t channel_id, uint16_t req_cookie) {
    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    DevServerBinding* binding = find_binding_by_channel(consumer_node, channel_id);
    if (binding != nullptr && binding->resource_type == ResourceType::VFS && binding->vfs_rdma_write_active &&
        binding->vfs_rdma_write_active_cookie == req_cookie) {
        binding->vfs_rdma_write_active = false;
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
}

void run_deferred_vfs_op(void* arg) {
    auto* op = static_cast<DeferredVfsOp*>(arg);
    if (op == nullptr) {
        return;
    }

    std::array<char, sizeof(DevServerBinding::vfs_export_path)> export_path{};
    std::array<char, sizeof(DevServerBinding::vfs_export_name)> export_name{};
    DevServerBinding* retained_binding = nullptr;

    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        DevServerBinding* binding = find_binding_by_channel(op->hdr.src_node, op->hdr.channel_id);
        if (binding != nullptr && binding->resource_type == ResourceType::VFS && retain_binding_locked(binding)) {
            std::memcpy(export_path.data(), static_cast<const void*>(binding->vfs_export_path), export_path.size());
            std::memcpy(export_name.data(), static_cast<const void*>(binding->vfs_export_name), export_name.size());
            export_path.at(export_path.size() - 1) = '\0';
            export_name.at(export_name.size() - 1) = '\0';
            retained_binding = binding;
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    if (retained_binding != nullptr) {
        detail::handle_vfs_op(&op->hdr, op->hdr.channel_id, export_path.data(), export_name.data(), op->op_id, op->req_data,
                              op->req_data_len);
    } else {
        send_vfs_error_response(op->hdr.src_node, op->hdr.channel_id, op->op_id, -ENOTCONN, req_cookie_from_header(&op->hdr));
    }
    release_binding(retained_binding);

    delete[] op->req_data;
    delete op;
}

auto vfs_worker_index(uint16_t src_node, uint16_t channel_id) -> size_t {
    auto hash = static_cast<uint32_t>(src_node) ^ (static_cast<uint32_t>(channel_id) * 0x9e3779b9U);
    hash ^= hash >> 16U;
    hash *= 0x7feb352dU;
    hash ^= hash >> 15U;
    hash *= 0x846ca68bU;
    hash ^= hash >> 16U;
    return static_cast<size_t>(hash % VFS_OP_WORKER_COUNT);
}

auto vfs_worker_dequeue(size_t index) -> DeferredVfsOp* {
    auto& shard = s_vfs_op_workers.at(index);
    uint64_t const FLAGS = shard.lock.lock_irqsave();
    DeferredVfsOp* op = shard.head;
    if (op != nullptr) {
        shard.head = op->next;
        if (shard.head == nullptr) {
            shard.tail = nullptr;
        }
        op->next = nullptr;
        shard.pending.fetch_sub(1, std::memory_order_relaxed);
    }
    shard.lock.unlock_irqrestore(FLAGS);
    return op;
}

void vfs_op_worker_loop(size_t index) {
    while (true) {
        DeferredVfsOp* op = vfs_worker_dequeue(index);
        if (op != nullptr) {
            run_deferred_vfs_op(op);
            continue;
        }
        ker::mod::sched::kern_block();
    }
}

void vfs_op_worker_0() { vfs_op_worker_loop(0); }
void vfs_op_worker_1() { vfs_op_worker_loop(1); }
void vfs_op_worker_2() { vfs_op_worker_loop(2); }
void vfs_op_worker_3() { vfs_op_worker_loop(3); }
void vfs_op_worker_4() { vfs_op_worker_loop(4); }
void vfs_op_worker_5() { vfs_op_worker_loop(5); }
void vfs_op_worker_6() { vfs_op_worker_loop(6); }
void vfs_op_worker_7() { vfs_op_worker_loop(7); }
void vfs_op_worker_8() { vfs_op_worker_loop(8); }
void vfs_op_worker_9() { vfs_op_worker_loop(9); }
void vfs_op_worker_10() { vfs_op_worker_loop(10); }
void vfs_op_worker_11() { vfs_op_worker_loop(11); }
void vfs_op_worker_12() { vfs_op_worker_loop(12); }
void vfs_op_worker_13() { vfs_op_worker_loop(13); }
void vfs_op_worker_14() { vfs_op_worker_loop(14); }
void vfs_op_worker_15() { vfs_op_worker_loop(15); }

auto vfs_worker_for(const WkiHeader* hdr) -> VfsOpWorkerShard* {
    if (hdr == nullptr) {
        return nullptr;
    }

    size_t const START = vfs_worker_index(hdr->src_node, hdr->channel_id);
    for (size_t attempt = 0; attempt < VFS_OP_WORKER_COUNT; ++attempt) {
        size_t const INDEX = (START + attempt) % VFS_OP_WORKER_COUNT;
        auto& shard = s_vfs_op_workers.at(INDEX);
        if (shard.task != nullptr) {
            return &shard;
        }
    }
    return nullptr;
}

auto queue_vfs_op(const WkiHeader* hdr, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len) -> bool {
    auto* shard = vfs_worker_for(hdr);
    if (shard == nullptr) {
        return false;
    }

    auto* op = new (std::nothrow) DeferredVfsOp{};
    if (op == nullptr) {
        return false;
    }

    if (req_data_len > 0) {
        op->req_data = new (std::nothrow) uint8_t[req_data_len];
        if (op->req_data == nullptr) {
            delete op;
            return false;
        }
        std::memcpy(op->req_data, req_data, req_data_len);
    }

    op->hdr = *hdr;
    op->op_id = op_id;
    op->req_data_len = req_data_len;
    op->next = nullptr;

    uint64_t const FLAGS = shard->lock.lock_irqsave();
    if (shard->tail != nullptr) {
        shard->tail->next = op;
    } else {
        shard->head = op;
    }
    shard->tail = op;
    shard->pending.fetch_add(1, std::memory_order_relaxed);
    shard->lock.unlock_irqrestore(FLAGS);

    ker::mod::sched::wake_task_from_event(shard->task);
    return true;
}

struct ExistingNetBindingInfo {
    bool found = false;
    uint16_t assigned_channel = 0;
    uint8_t attach_cookie = 0;
    ker::net::NetDevice* net_dev = nullptr;
};

struct NetStateSnapshot {
    uint32_t ipv4_addr = 0;
    uint32_t ipv4_mask = 0;
    proto::MacAddress real_mac;
    uint16_t link_state = 0;
    uint32_t mtu = 1500;
};

auto has_net_binding_for_dev(ker::net::NetDevice* dev) -> bool;

auto find_existing_net_binding(uint16_t consumer_node, uint32_t resource_id) -> ExistingNetBindingInfo {
    ExistingNetBindingInfo info = {};

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    if (auto* binding = find_binding_by_resource(consumer_node, ResourceType::NET, resource_id); binding != nullptr) {
        info.found = true;
        info.assigned_channel = binding->assigned_channel;
        info.attach_cookie = binding->attach_cookie;
        info.net_dev = binding->net_dev;
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);

    return info;
}

auto forwarded_frame_is_wki(const ker::net::PacketBuffer* pkt) -> bool {
    if (pkt == nullptr || pkt->len < ker::net::proto::ETH_HLEN) {
        return false;
    }

    const auto* eth = reinterpret_cast<const ker::net::proto::EthernetHeader*>(pkt->data);
    uint16_t const ETHERTYPE = ntohs(eth->ethertype);
    return ETHERTYPE == ker::net::proto::ETH_TYPE_WKI || ETHERTYPE == ker::net::proto::ETH_TYPE_WKI_ROCE;
}

auto capture_net_state(ker::net::NetDevice* ndev) -> NetStateSnapshot {
    NetStateSnapshot snap = {};
    if (ndev == nullptr) {
        return snap;
    }

    snap.real_mac = ndev->mac;
    snap.link_state = ndev->state;
    snap.mtu = ndev->mtu;

    auto* nif = ker::net::netif_get(ndev);
    if (nif != nullptr && nif->ipv4_addr_count > 0) {
        const auto& primary_ipv4 = nif->ipv4_addrs.front();
        snap.ipv4_addr = primary_ipv4.addr;
        snap.ipv4_mask = primary_ipv4.netmask;
    }

    return snap;
}

void fill_net_state_payload(NetStateNotifyPayload& payload, const NetStateSnapshot& snap) {
    payload.ipv4_addr = snap.ipv4_addr;
    payload.ipv4_mask = snap.ipv4_mask;
    payload.real_mac = snap.real_mac;
    payload.link_state = snap.link_state;
    payload.mtu = snap.mtu;
}

void record_binding_net_state(DevServerBinding& binding, const NetStateSnapshot& snap) {
    binding.net_state_valid = true;
    binding.net_last_ipv4_addr = snap.ipv4_addr;
    binding.net_last_ipv4_mask = snap.ipv4_mask;
    binding.net_last_real_mac = snap.real_mac;
    binding.net_last_link_state = snap.link_state;
    binding.net_last_mtu = snap.mtu;
}

void build_net_attach_ack(DevAttachAckNetPayload& ack, ker::net::NetDevice* ndev, uint32_t resource_id, uint16_t assigned_channel) {
    ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
    ack.assigned_channel = assigned_channel;
    ack.resource_id = resource_id;
    ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));
    NetStateSnapshot const SNAP = capture_net_state(ndev);
    ack.real_mac = SNAP.real_mac;
    ack.link_state = SNAP.link_state;
    ack.ipv4_addr = SNAP.ipv4_addr;
    ack.ipv4_mask = SNAP.ipv4_mask;
    ack.mtu = SNAP.mtu;
}

void rollback_net_binding(uint16_t consumer_node, uint32_t resource_id, ker::net::NetDevice* ndev, WkiChannel* ch) {
    bool uninstall_rx_forward = false;

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto it = g_bindings.begin(); it != g_bindings.end(); ++it) {
        if (!it->active || it->consumer_node != consumer_node || it->resource_type != ResourceType::NET || it->resource_id != resource_id) {
            continue;
        }

        g_bindings.erase(it);
        uninstall_rx_forward = !has_net_binding_for_dev(ndev);
        break;
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);

    if (ndev != nullptr && ndev->remotable != nullptr) {
        ndev->remotable->on_remote_detach(consumer_node);
    }
    if (ch != nullptr) {
        wki_channel_close(ch);
    }
    if (uninstall_rx_forward && ndev != nullptr) {
        ndev->wki_rx_forward = nullptr;
    }
}

void rollback_attach_ack_failure(uint16_t consumer_node, ResourceType resource_type, uint32_t resource_id, uint16_t assigned_channel) {
    struct RollbackInfo {
        bool found = false;
        DevServerBinding* binding = nullptr;
        uint32_t blk_zone_id = 0;
        bool blk_rdma_active = false;
        ker::dev::BlockDevice* block_dev = nullptr;
        uint16_t consumer_node = 0;
        uint16_t assigned_channel = 0;
        uint8_t* vfs_rdma_write_buf = nullptr;
        uint8_t* vfs_rdma_read_staging_buf = nullptr;
        uint8_t* vfs_rdma_bulk_staging_buf = nullptr;
    };

    RollbackInfo info = {};
    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        for (auto& binding : g_bindings) {
            if (!binding.active || binding.retiring.load(std::memory_order_acquire) || binding.consumer_node != consumer_node ||
                binding.resource_type != resource_type || binding.resource_id != resource_id ||
                binding.assigned_channel != assigned_channel) {
                continue;
            }

            info.found = true;
            info.binding = &binding;
            info.blk_zone_id = binding.blk_zone_id;
            info.blk_rdma_active = binding.blk_rdma_active;
            info.block_dev = binding.block_dev;
            info.consumer_node = binding.consumer_node;
            info.assigned_channel = binding.assigned_channel;
            info.vfs_rdma_write_buf = binding.vfs_rdma_write_buf;
            info.vfs_rdma_read_staging_buf = binding.vfs_rdma_read_staging_buf;
            info.vfs_rdma_bulk_staging_buf = binding.vfs_rdma_bulk_staging_buf;
            binding.vfs_rdma_write_buf = nullptr;
            binding.vfs_rdma_read_staging_buf = nullptr;
            binding.vfs_rdma_bulk_staging_buf = nullptr;
            mark_binding_retiring_locked(binding);
            break;
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    if (!info.found) {
        return;
    }

    wait_for_binding_refs_to_drain(info.binding);

    delete[] info.vfs_rdma_write_buf;
    delete[] info.vfs_rdma_read_staging_buf;
    delete[] info.vfs_rdma_bulk_staging_buf;

    if (info.blk_rdma_active && info.blk_zone_id != 0) {
        wki_zone_destroy(info.blk_zone_id);
    }
    if (info.block_dev != nullptr && info.block_dev->remotable != nullptr) {
        info.block_dev->remotable->on_remote_detach(info.consumer_node);
    }
    WkiChannel* ch = wki_channel_lookup(info.consumer_node, info.assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    {
        uint64_t const ERASE_FLAGS = s_server_lock.lock_irqsave();
        erase_retired_binding_locked(info.binding);
        s_server_lock.unlock_irqrestore(ERASE_FLAGS);
    }
}

auto reserve_attach_channel(uint16_t requester_node, uint16_t requested_channel, PriorityClass priority) -> WkiChannel* {
    if (wki_requester_controls_dynamic_channel(requester_node, g_wki.my_node_id)) {
        if (requested_channel < WKI_CHAN_DYNAMIC_BASE) {
            return nullptr;
        }
        return wki_channel_reserve(requester_node, requested_channel, priority);
    }

    return wki_channel_alloc(requester_node, priority);
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_dev_server_init() {
    if (g_dev_server_initialized) {
        return;
    }

    using WorkerEntry = void (*)();
    constexpr std::array<WorkerEntry, VFS_OP_WORKER_COUNT> VFS_OP_WORKER_ENTRIES = {
        vfs_op_worker_0,  vfs_op_worker_1,  vfs_op_worker_2,  vfs_op_worker_3,  vfs_op_worker_4,  vfs_op_worker_5,
        vfs_op_worker_6,  vfs_op_worker_7,  vfs_op_worker_8,  vfs_op_worker_9,  vfs_op_worker_10, vfs_op_worker_11,
        vfs_op_worker_12, vfs_op_worker_13, vfs_op_worker_14, vfs_op_worker_15,
    };
    constexpr std::array<const char*, VFS_OP_WORKER_COUNT> VFS_OP_WORKER_NAMES = {
        "wki_vfs_srv0",  "wki_vfs_srv1",  "wki_vfs_srv2",  "wki_vfs_srv3",  "wki_vfs_srv4",  "wki_vfs_srv5",
        "wki_vfs_srv6",  "wki_vfs_srv7",  "wki_vfs_srv8",  "wki_vfs_srv9",  "wki_vfs_srv10", "wki_vfs_srv11",
        "wki_vfs_srv12", "wki_vfs_srv13", "wki_vfs_srv14", "wki_vfs_srv15",
    };

    bool any_vfs_worker = false;
    for (size_t i = 0; i < VFS_OP_WORKER_COUNT; ++i) {
        auto* worker = ker::mod::sched::task::Task::create_kernel_thread(VFS_OP_WORKER_NAMES.at(i), VFS_OP_WORKER_ENTRIES.at(i));
        s_vfs_op_workers.at(i).task = worker;
        if (worker != nullptr) {
            promote_vfs_worker(worker);
            ker::mod::sched::post_task_balanced(worker);
            any_vfs_worker = true;
        } else {
            ker::mod::dbg::log("[WKI] Dev server failed to create VFS worker: index=%u", static_cast<unsigned>(i));
        }
    }

    if (!any_vfs_worker) {
        ker::mod::dbg::log("[WKI] Dev server failed to create any VFS workers");
    }
    g_dev_server_initialized = true;
    ker::mod::dbg::log("[WKI] Dev server subsystem initialized");
}

auto wki_dev_server_block_has_remote_writer(const dev::BlockDevice* dev) -> bool {
    uint64_t const FLAGS = s_server_lock.lock_irqsave();
    bool const HAS_WRITER = block_has_remote_writer_locked(dev);
    s_server_lock.unlock_irqrestore(FLAGS);
    return HAS_WRITER;
}

// -----------------------------------------------------------------------------
// D11: Check if any active NET binding still references a given device
// -----------------------------------------------------------------------------

namespace {

// s_server_lock must be held by caller
auto has_net_binding_for_dev(ker::net::NetDevice* dev) -> bool {
    return std::ranges::any_of(
        g_bindings, [dev](const DevServerBinding& b) { return b.active && b.resource_type == ResourceType::NET && b.net_dev == dev; });
}

auto net_request_cookie_from_payload(const uint8_t* req_data, uint16_t req_data_len, uint16_t fallback) -> uint16_t {
    if (req_data == nullptr || req_data_len < sizeof(uint16_t)) {
        return fallback;
    }
    uint16_t cookie = 0;
    memcpy(&cookie, req_data, sizeof(cookie));
    return cookie != 0 ? cookie : fallback;
}

}  // namespace

// -----------------------------------------------------------------------------
// D11: RX forward - called from netdev_rx() on the owner's NIC
// -----------------------------------------------------------------------------

void wki_dev_server_forward_net_rx(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt) {
    if (dev == nullptr || pkt == nullptr || pkt->data == nullptr || pkt->len == 0) {
        return;
    }

    // Remote NIC sharing is for guest-visible Ethernet traffic, not for WKI's
    // own transport frames. Forwarding WKI or RoCE packets into proxy NICs can
    // make them re-enter the WKI stack and explode traffic fanout with many peers.
    if (forwarded_frame_is_wki(pkt)) {
        return;
    }
    if (pkt->len < ker::net::proto::ETH_HLEN) {
        return;
    }

    const auto* eth = reinterpret_cast<const ker::net::proto::EthernetHeader*>(pkt->data);
    ker::net::proto::MacAddress const DST = eth->dst;
    bool const IS_BROADCAST = DST.is_broadcast();
    bool const IS_MULTICAST = !IS_BROADCAST && DST.is_multicast();
    bool const IS_OWNER_UNICAST = !IS_BROADCAST && !IS_MULTICAST && DST == dev->mac;
    if (IS_OWNER_UNICAST) {
        return;
    }

    auto pkt_data_len = static_cast<uint16_t>(pkt->len);
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + sizeof(NetNotifyHeader) + pkt_data_len);
    if (req_total > WKI_ETH_MAX_PAYLOAD) {
        return;  // Packet too large for a single WKI message
    }

    // Collect matching bindings under the lock, then send outside
    struct RxTarget {
        uint16_t consumer_node;
        uint16_t assigned_channel;
        uint8_t attach_cookie;
    };
    constexpr size_t MAX_RX_TARGETS = 32;
    std::array<RxTarget, MAX_RX_TARGETS> targets = {};
    size_t target_count = 0;

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto& b : g_bindings) {
        if (!b.active || b.resource_type != ResourceType::NET || b.net_dev != dev) {
            continue;
        }
        if (!b.net_nic_opened) {
            continue;
        }
        if (IS_BROADCAST && !b.net_rx_filter.accept_broadcast) {
            continue;
        }
        if (IS_MULTICAST && !b.net_rx_filter.accept_multicast) {
            continue;
        }
        if (b.net_rx_credits == 0) {
            b.net_dev->rx_dropped++;
            continue;
        }
        if (target_count < MAX_RX_TARGETS) {
            targets.at(target_count++) = {
                .consumer_node = b.consumer_node,
                .assigned_channel = b.assigned_channel,
                .attach_cookie = b.attach_cookie,
            };
            b.net_rx_credits--;
        }
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);

    // Build and send OP_NET_RX_NOTIFY outside the lock (fire-and-forget)
    for (size_t i = 0; i < target_count; i++) {
        const auto& target = targets.at(i);
        auto* req_buf = new (std::nothrow) uint8_t[req_total];
        if (req_buf == nullptr) {
            continue;
        }
        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
        req->op_id = OP_NET_RX_NOTIFY;
        req->data_len = static_cast<uint16_t>(sizeof(NetNotifyHeader) + pkt_data_len);
        auto* notify = reinterpret_cast<NetNotifyHeader*>(req_buf + sizeof(DevOpReqPayload));
        notify->magic = WKI_NET_NOTIFY_MAGIC;
        notify->attach_cookie = target.attach_cookie;
        notify->reserved = 0;
        notify->data_len = pkt_data_len;
        memcpy(req_buf + sizeof(DevOpReqPayload) + sizeof(NetNotifyHeader), pkt->data, pkt_data_len);
        wki_send(target.consumer_node, target.assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
        delete[] req_buf;
    }
}

void wki_dev_server_mark_net_opened(uint16_t consumer_node, uint16_t channel_id, ker::net::NetDevice* dev, bool opened) {
    if (dev == nullptr) {
        return;
    }

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    DevServerBinding* binding = find_binding_by_channel(consumer_node, channel_id);
    if (binding != nullptr && binding->active && binding->resource_type == ResourceType::NET && binding->net_dev == dev) {
        binding->net_nic_opened = opened;
        if (opened) {
            binding->net_rx_filter.accept_broadcast = true;
            binding->net_rx_filter.accept_multicast = true;
        }
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
}

void wki_dev_server_add_net_rx_credits(uint16_t consumer_node, uint16_t channel_id, ker::net::NetDevice* dev, uint16_t credits) {
    if (dev == nullptr || credits == 0) {
        return;
    }

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    DevServerBinding* binding = find_binding_by_channel(consumer_node, channel_id);
    if (binding != nullptr && binding->active && binding->resource_type == ResourceType::NET && binding->net_dev == dev) {
        binding->net_rx_credits += credits;
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
}

void wki_dev_server_notify_net_changed(ker::net::NetDevice* dev) {
    if (dev == nullptr) {
        return;
    }

    struct NotifyTarget {
        uint16_t consumer_node;
        uint16_t assigned_channel;
        uint8_t attach_cookie;
    };
    constexpr size_t MAX_NOTIFY_TARGETS = 32;
    std::array<NotifyTarget, MAX_NOTIFY_TARGETS> targets = {};
    size_t target_count = 0;

    NetStateSnapshot const SNAP = capture_net_state(dev);

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto& b : g_bindings) {
        if (!b.active || b.resource_type != ResourceType::NET || b.net_dev != dev) {
            continue;
        }

        bool const CHANGED = !b.net_state_valid || b.net_last_ipv4_addr != SNAP.ipv4_addr || b.net_last_ipv4_mask != SNAP.ipv4_mask ||
                             b.net_last_real_mac != SNAP.real_mac || b.net_last_link_state != SNAP.link_state || b.net_last_mtu != SNAP.mtu;
        if (!CHANGED) {
            continue;
        }

        record_binding_net_state(b, SNAP);
        if (target_count < MAX_NOTIFY_TARGETS) {
            targets.at(target_count++) = {
                .consumer_node = b.consumer_node,
                .assigned_channel = b.assigned_channel,
                .attach_cookie = b.attach_cookie,
            };
        }
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);

    if (target_count == 0) {
        return;
    }

    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(NetNotifyHeader) + sizeof(NetStateNotifyPayload)> msg{};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_NET_STATE_NOTIFY;
    req->data_len = sizeof(NetNotifyHeader) + sizeof(NetStateNotifyPayload);
    auto* notify = reinterpret_cast<NetNotifyHeader*>(msg.data() + sizeof(DevOpReqPayload));
    notify->magic = WKI_NET_NOTIFY_MAGIC;
    notify->data_len = sizeof(NetStateNotifyPayload);
    auto* state = reinterpret_cast<NetStateNotifyPayload*>(msg.data() + sizeof(DevOpReqPayload) + sizeof(NetNotifyHeader));
    fill_net_state_payload(*state, SNAP);

    for (size_t i = 0; i < target_count; i++) {
        const auto& target = targets.at(i);
        notify->attach_cookie = target.attach_cookie;
        wki_send(target.consumer_node, target.assigned_channel, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
    }
}

void wki_dev_server_refresh_vfs_binding(uint32_t resource_id, const char* export_path, const char* export_name) {
    if (export_path == nullptr || export_name == nullptr) {
        return;
    }

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto& binding : g_bindings) {
        if (!binding.active || binding.resource_type != ResourceType::VFS || binding.resource_id != resource_id) {
            continue;
        }

        if (std::strncmp(static_cast<const char*>(binding.vfs_export_path), export_path, sizeof(binding.vfs_export_path)) == 0 &&
            std::strncmp(static_cast<const char*>(binding.vfs_export_name), export_name, sizeof(binding.vfs_export_name)) == 0) {
            continue;
        }

        size_t path_len = std::strlen(export_path);
        if (path_len >= sizeof(binding.vfs_export_path)) {
            path_len = sizeof(binding.vfs_export_path) - 1;
        }
        std::memcpy(static_cast<void*>(binding.vfs_export_path), export_path, path_len);
        binding.vfs_export_path[path_len] = '\0';

        size_t name_len = std::strlen(export_name);
        if (name_len >= sizeof(binding.vfs_export_name)) {
            name_len = sizeof(binding.vfs_export_name) - 1;
        }
        std::memcpy(static_cast<void*>(binding.vfs_export_name), export_name, name_len);
        binding.vfs_export_name[name_len] = '\0';

        ker::mod::dbg::log("[WKI] Refreshed VFS binding: node=0x%04x res_id=%u ch=%u path='%s' name='%s'", binding.consumer_node,
                           resource_id, binding.assigned_channel, static_cast<const char*>(binding.vfs_export_path),
                           static_cast<const char*>(binding.vfs_export_name));
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
}

// -----------------------------------------------------------------------------
// Fencing cleanup
// -----------------------------------------------------------------------------

void wki_dev_server_detach_all_for_peer(uint16_t node_id) {
    // Collect work items and cleanup info under the lock
    struct DetachWork {
        DevServerBinding* binding;
        uint32_t blk_zone_id;
        bool blk_rdma_active;
        ker::dev::BlockDevice* block_dev;
        ker::net::NetDevice* net_dev;
        uint16_t consumer_node;
        uint16_t assigned_channel;
        uint8_t* vfs_rdma_write_buf;
        uint8_t* vfs_rdma_read_staging_buf;
        uint8_t* vfs_rdma_bulk_staging_buf;
    };
    constexpr size_t MAX_DETACH = 32;

    constexpr size_t MAX_NET_DEVS = 16;
    bool detached_any = false;

    while (true) {
        std::array<DetachWork, MAX_DETACH> work = {};
        size_t work_count = 0;
        std::array<ker::net::NetDevice*, MAX_NET_DEVS> uninstall_devs = {};
        size_t uninstall_count = 0;

        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        for (auto& b : g_bindings) {
            if (!b.active || b.consumer_node != node_id) {
                continue;
            }
            if (work_count >= MAX_DETACH) {
                break;
            }
            work.at(work_count++) = {.binding = &b,
                                     .blk_zone_id = b.blk_zone_id,
                                     .blk_rdma_active = b.blk_rdma_active,
                                     .block_dev = b.block_dev,
                                     .net_dev = b.net_dev,
                                     .consumer_node = b.consumer_node,
                                     .assigned_channel = b.assigned_channel,
                                     .vfs_rdma_write_buf = b.vfs_rdma_write_buf,
                                     .vfs_rdma_read_staging_buf = b.vfs_rdma_read_staging_buf,
                                     .vfs_rdma_bulk_staging_buf = b.vfs_rdma_bulk_staging_buf};
            b.vfs_rdma_write_buf = nullptr;
            b.vfs_rdma_read_staging_buf = nullptr;
            b.vfs_rdma_bulk_staging_buf = nullptr;
            mark_binding_retiring_locked(b);
        }

        // Determine which NET devices need their RX forward hook uninstalled.
        for (size_t i = 0; i < work_count; i++) {
            const auto& item = work.at(i);
            if (item.net_dev == nullptr) {
                continue;
            }
            bool dup = false;
            for (size_t j = 0; j < uninstall_count; j++) {
                if (uninstall_devs.at(j) == item.net_dev) {
                    dup = true;
                    break;
                }
            }
            if (!dup && uninstall_count < MAX_NET_DEVS && !has_net_binding_for_dev(item.net_dev)) {
                uninstall_devs.at(uninstall_count++) = item.net_dev;
            }
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);

        if (work_count == 0) {
            break;
        }
        detached_any = true;

        // Perform cleanup outside the lock.
        for (size_t i = 0; i < work_count; i++) {
            const auto& item = work.at(i);
            wait_for_binding_refs_to_drain(item.binding);
            delete[] item.vfs_rdma_write_buf;
            delete[] item.vfs_rdma_read_staging_buf;
            delete[] item.vfs_rdma_bulk_staging_buf;
            if (item.blk_rdma_active && item.blk_zone_id != 0) {
                wki_zone_destroy(item.blk_zone_id);
            }
            if (item.block_dev != nullptr && item.block_dev->remotable != nullptr) {
                item.block_dev->remotable->on_remote_fault(node_id);
            }
            if (item.net_dev != nullptr && item.net_dev->remotable != nullptr) {
                item.net_dev->remotable->on_remote_fault(node_id);
            }
            WkiChannel* ch = wki_channel_get(item.consumer_node, item.assigned_channel);
            if (ch != nullptr) {
                wki_channel_close(ch);
            }
        }

        {
            uint64_t const ERASE_FLAGS = s_server_lock.lock_irqsave();
            for (size_t i = 0; i < work_count; i++) {
                erase_retired_binding_locked(work.at(i).binding);
            }
            s_server_lock.unlock_irqrestore(ERASE_FLAGS);
        }

        // D11: Uninstall RX forward hooks for NET devices that no longer have bindings.
        for (size_t i = 0; i < uninstall_count; i++) {
            uninstall_devs.at(i)->wki_rx_forward = nullptr;
        }
    }

    if (detached_any) {
        wki_resource_advertise_all();
    }
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const DevAttachReqPayload*>(payload);

    // Prepare response
    DevAttachAckPayload ack = {};
    ack.reserved = req->attach_cookie;
    ack.resource_id = req->resource_id;

    auto res_type = static_cast<ResourceType>(req->resource_type);

    if (res_type == ResourceType::BLOCK) {
        // Find the block device
        ker::dev::BlockDevice* bdev = find_block_device_by_resource_id(req->resource_id);
        if (bdev == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Check remotable
        if (bdev->remotable == nullptr || !bdev->remotable->can_remote()) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_REMOTABLE);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        bool const REQUEST_READ = dev_attach_requests_read(req->attach_mode);
        bool const REQUEST_WRITE = dev_attach_requests_write(req->attach_mode);
        bool const OWNER_MOUNTED = ker::vfs::mounted_block_device_overlaps(bdev);
        bool read_only_attach = false;
        {
            uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
            bool const WRITE_UNAVAILABLE =
                ker::dev::block_device_is_read_only(bdev) || OWNER_MOUNTED || block_has_remote_writer_locked(bdev);
            read_only_attach = !REQUEST_WRITE || (REQUEST_WRITE && WRITE_UNAVAILABLE);
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
        }
        if (read_only_attach && !REQUEST_READ) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // The lower node ID is the channel allocator for the peer pair.
        WkiChannel* ch = reserve_attach_channel(hdr->src_node, req->requested_channel, PriorityClass::THROUGHPUT);
        if (ch == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Call on_remote_attach
        int const ATTACH_RET = bdev->remotable->on_remote_attach(hdr->src_node);
        if (ATTACH_RET != 0) {
            wki_channel_close(ch);
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Create binding
        DevServerBinding binding;
        binding.active = true;
        binding.consumer_node = hdr->src_node;
        binding.assigned_channel = ch->channel_id;
        binding.resource_type = ResourceType::BLOCK;
        binding.resource_id = req->resource_id;
        binding.attach_cookie = req->attach_cookie;
        binding.block_dev = bdev;
        binding.block_read_only = read_only_attach;

        // Compute the RDMA zone ID for the block ring.  Zone creation is
        // deferred to the timer tick (wki_dev_server_process_pending_zones)
        // because wki_zone_create() blocks on a spin-wait for the zone ACK,
        // and we are currently inside the NAPI poll handler. Inline NAPI
        // draining cannot re-enter that handler, so the ACK can never be
        // received here. Instead, send the ACK optimistically with the
        // zone_id; the consumer already has a timeout loop waiting for the
        // zone to appear.
        uint32_t const BLK_ZONE_ID = (static_cast<uint32_t>(hdr->src_node) << 16) | req->resource_id;
        binding.blk_zone_id = BLK_ZONE_ID;
        binding.blk_zone_pending = true;  // processed by wki_dev_server_process_pending_zones()
        binding.blk_rdma_active = false;  // not yet - set when deferred creation succeeds

        {
            bool const OWNER_MOUNTED_NOW = ker::vfs::mounted_block_device_overlaps(bdev);
            uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
            if (REQUEST_WRITE && !read_only_attach &&
                (ker::dev::block_device_is_read_only(bdev) || OWNER_MOUNTED_NOW || block_has_remote_writer_locked(bdev))) {
                s_server_lock.unlock_irqrestore(SRV_FLAGS);
                bdev->remotable->on_remote_detach(hdr->src_node);
                wki_channel_close(ch);
                ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
                wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
                return;
            }
            g_bindings.push_back(std::move(binding));
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
        }

        // Send success ACK with the proposed RDMA zone info.
        // The consumer will wait for the zone to appear + server_ready.
        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));
        ack.rdma_flags = DEV_ATTACH_RDMA_BLK_RING;
        if (read_only_attach) {
            ack.rdma_flags |= DEV_ATTACH_READ_ONLY;
        }
        ack.blk_zone_id = BLK_ZONE_ID;

        // Advertise streaming bulk RDMA transfer support when the peer has an
        // RDMA-capable transport (ivshmem or RoCE).  The consumer will allocate
        // and register a staging buffer for direct large-range transfers.
        {
            WkiPeer const* peer = wki_peer_find(hdr->src_node);
            if (peer != nullptr && peer->rdma_transport != nullptr) {
                ack.rdma_flags |= DEV_ATTACH_RDMA_BULK;
            }
        }

        ker::mod::dbg::log("[WKI] Dev attach: node=0x%04x res_id=%u ch=%u rdma=deferred zone=0x%08x access=%s", hdr->src_node,
                           req->resource_id, ch->channel_id, BLK_ZONE_ID, read_only_attach ? "ro" : "rw");

        // Send ACK on WKI_CHAN_RESOURCE (the same channel the request arrived on) so that
        // the piggybacked ACK properly drains the client's retransmit queue for channel 3.
        // The dynamic channel ID is communicated via ack.assigned_channel in the payload.
        int const ACK_RET = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
        if (ACK_RET != WKI_OK) {
            ker::mod::dbg::log("[WKI] Dev attach ACK send failed: node=0x%04x err=%d", hdr->src_node, ACK_RET);
            rollback_attach_ack_failure(hdr->src_node, ResourceType::BLOCK, req->resource_id, ch->channel_id);
            wki_resource_advertise_all();
        } else {
            wki_resource_advertise_all();
        }
    } else if (res_type == ResourceType::VFS) {
        // Find the VFS export
        VfsExport exp = {};
        if (!wki_remote_vfs_find_export_snapshot(req->resource_id, &exp)) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // The lower node ID is the channel allocator for the peer pair. VFS
        // write RPCs can spend milliseconds in the server after the control
        // frame is received, so ACK them immediately instead of waiting for a
        // response piggyback and tripping the sender retransmit timer.
        WkiChannel const* ch = reserve_attach_channel(hdr->src_node, req->requested_channel, PriorityClass::LATENCY);
        if (ch == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Create binding with VFS export path
        DevServerBinding binding;
        binding.active = true;
        binding.consumer_node = hdr->src_node;
        binding.assigned_channel = ch->channel_id;
        binding.resource_type = ResourceType::VFS;
        binding.resource_id = req->resource_id;
        binding.attach_cookie = req->attach_cookie;
        memcpy(static_cast<void*>(binding.vfs_export_path), static_cast<const void*>(exp.export_path),
               std::min(sizeof(binding.vfs_export_path), sizeof(exp.export_path)));
        memcpy(static_cast<void*>(binding.vfs_export_name), static_cast<const void*>(exp.name),
               std::min(sizeof(binding.vfs_export_name), sizeof(exp.name)));
        binding.vfs_export_path[sizeof(binding.vfs_export_path) - 1] = '\0';
        binding.vfs_export_name[sizeof(binding.vfs_export_name) - 1] = '\0';

        // RDMA-backed VFS I/O: pre-register server-side buffers.
        // rdma_register_region is a local-only operation - safe to call in NAPI context.
        {
            WkiPeer const* peer = wki_peer_find(hdr->src_node);
            if (peer != nullptr && peer->rdma_transport != nullptr && peer->rdma_transport->rdma_register_region != nullptr) {
                // Write receive buffer: consumer rdma_writes file data here.
                constexpr uint32_t VFS_WRITE_BUF = VFS_RDMA_WRITE_SIZE;
                auto* wbuf = new (std::nothrow) uint8_t[VFS_WRITE_BUF];
                if (wbuf != nullptr) {
                    uint32_t rkey = 0;
                    int const REG_RET = peer->rdma_transport->rdma_register_region(peer->rdma_transport, reinterpret_cast<uint64_t>(wbuf),
                                                                                   VFS_WRITE_BUF, &rkey);
                    if (REG_RET == 0 && rkey != 0) {
                        binding.vfs_rdma_write_buf = wbuf;
                        binding.vfs_rdma_write_rkey = rkey;
                        binding.vfs_rdma_write_transport = peer->rdma_transport;
                        ack.rdma_flags |= DEV_ATTACH_RDMA_VFS;
                        ack.blk_zone_id = rkey;  // carry server write-recv rkey to consumer
                    } else {
                        delete[] wbuf;
                    }
                }

                // Read staging buffer (RoCE pull mode only): server reads file data here;
                // consumer rdma_reads to pull. Not needed for ivshmem (push is safe there).
                bool const PEER_IS_ROCE =
                    (peer->rdma_transport->name != nullptr && std::strcmp(peer->rdma_transport->name, "wki-roce") == 0);
                if (PEER_IS_ROCE) {
                    auto* rbuf = new (std::nothrow) uint8_t[VFS_RDMA_BOUNCE_SIZE];
                    if (rbuf != nullptr) {
                        uint32_t rrkey = 0;
                        int const REG_RET = peer->rdma_transport->rdma_register_region(
                            peer->rdma_transport, reinterpret_cast<uint64_t>(rbuf), VFS_RDMA_BOUNCE_SIZE, &rrkey);
                        if (REG_RET == 0 && rrkey != 0) {
                            binding.vfs_rdma_read_staging_buf = rbuf;
                            binding.vfs_rdma_read_staging_rkey = rrkey;
                            ack.rdma_flags |= DEV_ATTACH_RDMA_VFS_READ;
                            ack.rdma_read_staging_rkey = rrkey;
                        } else {
                            delete[] rbuf;
                        }
                    }

                    // Bulk staging buffer (RoCE bulk pull mode): server reads
                    // transport-sized VFS chunks here, then the consumer pulls
                    // them via RDMA. RoCE already caps each pull burst, so avoid
                    // reserving a 2 MiB buffer for a 128 KiB transfer window.
                    auto* bbuf = new (std::nothrow) uint8_t[VFS_RDMA_ROCE_BULK_SIZE];
                    if (bbuf != nullptr) {
                        uint32_t brkey = 0;
                        int const BREG_RET = peer->rdma_transport->rdma_register_region(
                            peer->rdma_transport, reinterpret_cast<uint64_t>(bbuf), VFS_RDMA_ROCE_BULK_SIZE, &brkey);
                        if (BREG_RET == 0 && brkey != 0) {
                            binding.vfs_rdma_bulk_staging_buf = bbuf;
                            binding.vfs_rdma_bulk_staging_rkey = brkey;
                            ack.rdma_flags |= DEV_ATTACH_RDMA_BULK_PULL;
                            ack.rdma_bulk_staging_rkey = brkey;
                        } else {
                            delete[] bbuf;
                            ker::mod::dbg::log("[WKI] VFS attach: bulk staging registration failed node=0x%04x ret=%d", hdr->src_node,
                                               BREG_RET);
                        }
                    } else {
                        ker::mod::dbg::log("[WKI] VFS attach: bulk staging allocation failed node=0x%04x size=%u", hdr->src_node,
                                           VFS_RDMA_ROCE_BULK_SIZE);
                    }
                }
            }
        }

        {
            uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
            g_bindings.push_back(std::move(binding));
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
        }

        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));

        ker::mod::dbg::log("[WKI] VFS attach: node=0x%04x res_id=%u ch=%u path=%s rdma_write=%s read_staging_rkey=%u bulk_staging_rkey=%u",
                           hdr->src_node, req->resource_id, ch->channel_id, static_cast<const char*>(exp.name),
                           ((ack.rdma_flags & DEV_ATTACH_RDMA_VFS) != 0) ? "yes" : "no", ack.rdma_read_staging_rkey,
                           ack.rdma_bulk_staging_rkey);

        // Send ACK on WKI_CHAN_RESOURCE (the same channel the request arrived on) so that
        // the piggybacked ACK properly drains the client's retransmit queue for channel 3.
        // The dynamic channel ID is communicated via ack.assigned_channel in the payload.
        int const ACK_RET = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
        if (ACK_RET != WKI_OK) {
            ker::mod::dbg::log("[WKI] VFS attach ACK send failed: node=0x%04x err=%d", hdr->src_node, ACK_RET);
            rollback_attach_ack_failure(hdr->src_node, ResourceType::VFS, req->resource_id, ch->channel_id);
        }
    } else if (res_type == ResourceType::NET) {
        // Find the net device
        ker::net::NetDevice* ndev = find_net_device_by_resource_id(req->resource_id);
        if (ndev == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Check remotable
        if (ndev->remotable == nullptr || !ndev->remotable->can_remote()) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_REMOTABLE);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        if (auto existing = find_existing_net_binding(hdr->src_node, req->resource_id); existing.found && existing.net_dev != nullptr) {
            DevAttachAckNetPayload existing_ack = {};
            build_net_attach_ack(existing_ack, existing.net_dev, req->resource_id, existing.assigned_channel);
            existing_ack.attach_cookie = existing.attach_cookie;

            int const ACK_RET = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &existing_ack, sizeof(existing_ack));
            if (ACK_RET != WKI_OK) {
                ker::mod::dbg::log("[WKI] NET attach ACK resend failed: node=0x%04x err=%d", hdr->src_node, ACK_RET);
            }
            return;
        }

        // The lower node ID is the channel allocator for the peer pair.
        WkiChannel* ch = reserve_attach_channel(hdr->src_node, req->requested_channel, PriorityClass::THROUGHPUT);
        if (ch == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Call on_remote_attach
        int const ATTACH_RET = ndev->remotable->on_remote_attach(hdr->src_node);
        if (ATTACH_RET != 0) {
            wki_channel_close(ch);
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Create binding
        DevServerBinding binding;
        binding.active = true;
        binding.consumer_node = hdr->src_node;
        binding.assigned_channel = ch->channel_id;
        binding.resource_type = ResourceType::NET;
        binding.resource_id = req->resource_id;
        binding.attach_cookie = req->attach_cookie;
        binding.net_dev = ndev;
        record_binding_net_state(binding, capture_net_state(ndev));

        {
            uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
            g_bindings.push_back(std::move(binding));
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
        }

        // D11: Install RX forward hook on the NIC so received packets are forwarded
        ndev->wki_rx_forward = wki_dev_server_forward_net_rx;

        // V2: Send extended NET attach ACK with owner NIC info
        DevAttachAckNetPayload net_ack = {};
        build_net_attach_ack(net_ack, ndev, req->resource_id, ch->channel_id);
        net_ack.attach_cookie = req->attach_cookie;
        uint32_t netmask = net_ack.ipv4_mask;
        uint32_t bit_count = 0;
        while (netmask) {
            bit_count += netmask & 1;
            netmask >>= 1;
        }
        ker::mod::dbg::log("[WKI] NET attach: node=0x%04x res_id=%u ch=%u ip=%d.%d.%d.%d/%d", hdr->src_node, req->resource_id,
                           ch->channel_id, (net_ack.ipv4_addr >> 24) & 0xFF, (net_ack.ipv4_addr >> 16) & 0xFF,
                           (net_ack.ipv4_addr >> 8) & 0xFF, net_ack.ipv4_addr & 0xFF, bit_count);

        int const ACK_RET = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &net_ack, sizeof(net_ack));
        if (ACK_RET != WKI_OK) {
            ker::mod::dbg::log("[WKI] NET attach ACK send failed: node=0x%04x err=%d", hdr->src_node, ACK_RET);
            rollback_net_binding(hdr->src_node, req->resource_id, ndev, ch);
        }
    } else {
        ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
    }
}

void handle_dev_detach(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevDetachPayload)) {
        return;
    }

    const auto* det = reinterpret_cast<const DevDetachPayload*>(payload);
    uint8_t const DETACH_COOKIE = wki_dev_detach_cookie_from_payload(payload, payload_len);

    // Collect binding info under the lock, erase from deque, then cleanup outside
    struct DetachInfo {
        DevServerBinding* binding;
        uint32_t blk_zone_id;
        bool blk_rdma_active;
        ker::dev::BlockDevice* block_dev;
        ker::net::NetDevice* net_dev;
        uint16_t consumer_node;
        uint16_t assigned_channel;
        bool found;
        bool uninstall_rx_forward;
        uint8_t* vfs_rdma_write_buf;
        uint8_t* vfs_rdma_read_staging_buf;
        uint8_t* vfs_rdma_bulk_staging_buf;
    };
    DetachInfo info = {};

    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        for (auto& binding : g_bindings) {
            if (!binding.active ||
                !wki_dev_detach_matches_binding(binding.consumer_node, binding.resource_type, binding.resource_id, hdr->src_node, *det) ||
                !wki_dev_detach_cookie_matches_binding(binding.attach_cookie, DETACH_COOKIE)) {
                continue;
            }
            info.found = true;
            info.binding = &binding;
            info.blk_zone_id = binding.blk_zone_id;
            info.blk_rdma_active = binding.blk_rdma_active;
            info.block_dev = binding.block_dev;
            info.net_dev = binding.net_dev;
            info.consumer_node = binding.consumer_node;
            info.assigned_channel = binding.assigned_channel;
            info.vfs_rdma_write_buf = binding.vfs_rdma_write_buf;
            info.vfs_rdma_read_staging_buf = binding.vfs_rdma_read_staging_buf;
            info.vfs_rdma_bulk_staging_buf = binding.vfs_rdma_bulk_staging_buf;
            binding.vfs_rdma_write_buf = nullptr;
            binding.vfs_rdma_read_staging_buf = nullptr;
            binding.vfs_rdma_bulk_staging_buf = nullptr;
            mark_binding_retiring_locked(binding);
            if (info.net_dev != nullptr) {
                info.uninstall_rx_forward = !has_net_binding_for_dev(info.net_dev);
            }
            break;
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    if (!info.found) {
        return;
    }

    // Cleanup outside the lock

    wait_for_binding_refs_to_drain(info.binding);

    delete[] info.vfs_rdma_write_buf;

    delete[] info.vfs_rdma_read_staging_buf;

    delete[] info.vfs_rdma_bulk_staging_buf;

    if (info.blk_rdma_active && info.blk_zone_id != 0) {
        wki_zone_destroy(info.blk_zone_id);
    }
    if (info.block_dev != nullptr && info.block_dev->remotable != nullptr) {
        info.block_dev->remotable->on_remote_detach(hdr->src_node);
    }
    if (info.net_dev != nullptr && info.net_dev->remotable != nullptr) {
        info.net_dev->remotable->on_remote_detach(hdr->src_node);
    }
    WkiChannel* ch = wki_channel_get(info.consumer_node, info.assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }
    ker::mod::dbg::log("[WKI] Dev detach: node=0x%04x type=%u res_id=%u", hdr->src_node, det->resource_type, det->resource_id);
    if (info.uninstall_rx_forward) {
        info.net_dev->wki_rx_forward = nullptr;
    }

    {
        uint64_t const ERASE_FLAGS = s_server_lock.lock_irqsave();
        erase_retired_binding_locked(info.binding);
        s_server_lock.unlock_irqrestore(ERASE_FLAGS);
    }
    wki_resource_advertise_all();
}

void handle_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const DevOpReqPayload*>(payload);
    const uint8_t* req_data = payload + sizeof(DevOpReqPayload);
    uint16_t const REQ_DATA_LEN = req->data_len;

    // Verify data fits
    if (sizeof(DevOpReqPayload) + REQ_DATA_LEN > payload_len) {
        return;
    }

    uint16_t const REQUEST_COOKIE = req_cookie_from_header(hdr);
    bool const IS_VFS_OP = is_vfs_op(req->op_id);

    // Snapshot binding identity under the lock. Detach/fence may erase the
    // binding after this point, so code below must not dereference it.
    bool binding_found = false;
    ResourceType binding_resource_type = ResourceType::BLOCK;
    ker::net::NetDevice* binding_net_dev = nullptr;
    ker::dev::BlockDevice* binding_block_dev = nullptr;
    bool binding_block_read_only = false;
    uint32_t vfs_write_rkey = 0;
    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        DevServerBinding* binding = find_binding_by_channel(hdr->src_node, hdr->channel_id);
        if (binding != nullptr && binding->resource_type == ResourceType::VFS) {
            vfs_write_rkey = binding->vfs_rdma_write_rkey;
        }
        if (binding != nullptr) {
            binding_found = true;
            binding_resource_type = binding->resource_type;
            binding_net_dev = binding->net_dev;
            binding_block_dev = binding->block_dev;
            binding_block_read_only = binding->block_read_only;
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    if (!binding_found) {
        // D11: OP_NET_RX_NOTIFY is sent server->consumer on the dynamic channel.
        // The consumer has a proxy (not a server binding), so route to the
        // consumer-side handler instead of returning an error.
        if (req->op_id == OP_VFS_INVALIDATE) {
            detail::handle_vfs_invalidate_notify(hdr, req_data, REQ_DATA_LEN);
            return;
        }
        if (req->op_id == OP_NET_RX_NOTIFY) {
            detail::handle_net_rx_notify(hdr, req_data, REQ_DATA_LEN);
            return;
        }
        if (req->op_id == OP_NET_STATE_NOTIFY) {
            detail::handle_net_state_notify(hdr, req_data, REQ_DATA_LEN);
            return;
        }

        // IPC pipe/socket ops (0x0700-0x07FF) are routed to the IPC subsystem.
        // These don't use device bindings — they use resource_ids.
        if (req->op_id >= 0x0700 && req->op_id <= 0x07FF) {
            wki_ipc_handle_dev_op_req(hdr->src_node, hdr->channel_id, payload, payload_len);
            return;
        }

        if (IS_VFS_OP) {
            send_vfs_error_response(hdr->src_node, hdr->channel_id, req->op_id, -ENOTCONN, req_cookie_from_header(hdr));
            return;
        }

        // Send error response
        uint16_t const RESPONSE_COOKIE = (req->op_id >= OP_NET_XMIT && req->op_id <= OP_NET_STATE_NOTIFY)
                                             ? net_request_cookie_from_payload(req_data, REQ_DATA_LEN, REQUEST_COOKIE)
                                             : REQUEST_COOKIE;
        DevOpRespPayload resp = {};
        resp.op_id = req->op_id;
        resp.status = -1;
        resp.data_len = 0;
        resp.reserved = RESPONSE_COOKIE;
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
        return;
    }

    // Dispatch VFS operations to a worker thread. Local VFS/XFS operations can
    // block for milliseconds or longer; keeping them out of RX preserves ACK,
    // heartbeat, and fence progress for the peer set.
    // Upper bound is OP_VFS_READ_BULK (0x0413), the highest VFS op code.
    // OP_VFS_READ_RDMA (0x0410), OP_VFS_WRITE_RDMA (0x0411),
    // OP_VFS_READDIR_BATCH (0x0412), and OP_VFS_READ_BULK (0x0413) all
    // sit above OP_VFS_SEEK_END (0x040E).
    if (IS_VFS_OP) {
        if (req->op_id == OP_VFS_WRITE_RDMA) {
            uint16_t const REQ_COOKIE = req_cookie_from_header(hdr);
            CachedVfsWriteResponse cached = {};
            bool duplicate_active = false;
            bool prepare_region = false;
            {
                uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
                DevServerBinding* vfs_binding = find_binding_by_channel(hdr->src_node, hdr->channel_id);
                if (vfs_binding != nullptr && vfs_binding->resource_type == ResourceType::VFS) {
                    if (vfs_binding->vfs_rdma_write_resp_valid && vfs_binding->vfs_rdma_write_resp_cookie == REQ_COOKIE) {
                        cached.found = true;
                        cached.cookie = vfs_binding->vfs_rdma_write_resp_cookie;
                        cached.status = vfs_binding->vfs_rdma_write_resp_status;
                        cached.bytes_written = vfs_binding->vfs_rdma_write_resp_bytes;
                    } else if (vfs_binding->vfs_rdma_write_active && vfs_binding->vfs_rdma_write_active_cookie == REQ_COOKIE) {
                        duplicate_active = true;
                    } else {
                        vfs_binding->vfs_rdma_write_active = true;
                        vfs_binding->vfs_rdma_write_active_cookie = REQ_COOKIE;
                        prepare_region = transport_is_roce(vfs_binding->vfs_rdma_write_transport);
                        vfs_write_rkey = vfs_binding->vfs_rdma_write_rkey;
                    }
                }
                s_server_lock.unlock_irqrestore(SRV_FLAGS);
            }

            if (cached.found) {
                send_cached_vfs_write_response(hdr->src_node, hdr->channel_id, cached);
                return;
            }
            if (duplicate_active) {
                return;
            }
            if (prepare_region && !wki_roce_region_prepare_tagged_write(vfs_write_rkey, REQ_COOKIE)) {
                clear_vfs_write_active(hdr->src_node, hdr->channel_id, REQ_COOKIE);
                send_vfs_error_response(hdr->src_node, hdr->channel_id, req->op_id, -EIO, REQ_COOKIE);
                return;
            }
        }
        if (!queue_vfs_op(hdr, req->op_id, req_data, REQ_DATA_LEN)) {
            if (req->op_id == OP_VFS_WRITE_RDMA) {
                clear_vfs_write_active(hdr->src_node, hdr->channel_id, req_cookie_from_header(hdr));
            }
            send_vfs_error_response(hdr->src_node, hdr->channel_id, req->op_id, -ENOMEM, req_cookie_from_header(hdr));
        }
        return;
    }

    // IPC pipe/socket ops (0x0700-0x07FF) use resource_ids, not bindings
    if (req->op_id >= 0x0700 && req->op_id <= 0x07FF) {
        wki_ipc_handle_dev_op_req(hdr->src_node, hdr->channel_id, payload, payload_len);
        return;
    }

    // Dispatch NET operations to remote_net handler
    if (req->op_id >= OP_NET_XMIT && req->op_id <= OP_NET_STATE_NOTIFY) {
        if (binding_resource_type != ResourceType::NET || binding_net_dev == nullptr) {
            DevOpRespPayload resp = {};
            resp.op_id = req->op_id;
            resp.status = -1;
            resp.data_len = 0;
            resp.reserved = net_request_cookie_from_payload(req_data, REQ_DATA_LEN, REQUEST_COOKIE);
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            return;
        }
        detail::handle_net_op(hdr, hdr->channel_id, binding_net_dev, req->op_id, req_data, REQ_DATA_LEN);
        return;
    }

    // Block device operations require block_dev
    if (binding_block_dev == nullptr) {
        DevOpRespPayload resp = {};
        resp.op_id = req->op_id;
        resp.status = -1;
        resp.data_len = 0;
        resp.reserved = REQUEST_COOKIE;
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
        return;
    }

    ker::dev::BlockDevice* bdev = binding_block_dev;

    switch (req->op_id) {
        case OP_BLOCK_INFO: {
            // Response: {block_size:u64, total_blocks:u64}
            constexpr uint16_t INFO_DATA_LEN = 16;
            std::array<uint8_t, sizeof(DevOpRespPayload) + INFO_DATA_LEN> buf = {};

            auto* resp = reinterpret_cast<DevOpRespPayload*>(buf.data());
            resp->op_id = OP_BLOCK_INFO;
            resp->status = 0;
            resp->data_len = INFO_DATA_LEN;
            resp->reserved = REQUEST_COOKIE;

            auto* info_data = buf.data() + sizeof(DevOpRespPayload);
            uint64_t bs = bdev->block_size;
            uint64_t tb = bdev->total_blocks;
            memcpy(info_data, &bs, sizeof(uint64_t));
            memcpy(info_data + sizeof(uint64_t), &tb, sizeof(uint64_t));

            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, buf.data(),
                     static_cast<uint16_t>(sizeof(DevOpRespPayload) + INFO_DATA_LEN));
            break;
        }

        case OP_BLOCK_READ: {
            // Request data: {lba:u64, count:u32} = 12 bytes
            if (REQ_DATA_LEN < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_READ;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQUEST_COOKIE;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint64_t lba = 0;
            uint32_t count = 0;
            memcpy(&lba, req_data, sizeof(uint64_t));
            memcpy(&count, req_data + sizeof(uint64_t), sizeof(uint32_t));

            // Allocate read buffer
            auto data_bytes = static_cast<uint32_t>(count * bdev->block_size);
            auto max_resp_data = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));

            // Clamp to max payload
            if (data_bytes > max_resp_data) {
                data_bytes = max_resp_data;
                count = data_bytes / static_cast<uint32_t>(bdev->block_size);
                if (count == 0) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_BLOCK_READ;
                    resp.status = -1;
                    resp.data_len = 0;
                    resp.reserved = REQUEST_COOKIE;
                    wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                    break;
                }
                data_bytes = count * static_cast<uint32_t>(bdev->block_size);
            }

            auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + data_bytes);
            auto* buf = new (std::nothrow) uint8_t[resp_total];
            if (buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_READ;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQUEST_COOKIE;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(buf);
            uint8_t* read_buf = buf + sizeof(DevOpRespPayload);

            int const RET = ker::dev::block_read(bdev, lba, count, read_buf);

            resp->op_id = OP_BLOCK_READ;
            resp->status = static_cast<int16_t>(RET);
            resp->data_len = (RET == 0) ? static_cast<uint16_t>(data_bytes) : 0;
            resp->reserved = REQUEST_COOKIE;

            uint16_t const SEND_LEN = (RET == 0) ? resp_total : static_cast<uint16_t>(sizeof(DevOpRespPayload));
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, buf, SEND_LEN);

            delete[] buf;
            break;
        }

        case OP_BLOCK_WRITE: {
            // Request data: {lba:u64, count:u32, data[...]} = 12 + data bytes
            if (REQ_DATA_LEN < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQUEST_COOKIE;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint64_t lba = 0;
            uint32_t count = 0;
            memcpy(&lba, req_data, sizeof(uint64_t));
            memcpy(&count, req_data + sizeof(uint64_t), sizeof(uint32_t));

            const uint8_t* write_data = req_data + 12;
            auto write_data_len = static_cast<uint16_t>(REQ_DATA_LEN - 12);

            if (binding_block_read_only) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_WRITE;
                resp.status = -EROFS;
                resp.data_len = 0;
                resp.reserved = REQUEST_COOKIE;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Validate data length
            auto expected = static_cast<uint32_t>(count * bdev->block_size);
            if (write_data_len < expected) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = REQUEST_COOKIE;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int const RET = ker::dev::block_write(bdev, lba, count, write_data);

            DevOpRespPayload resp = {};
            resp.op_id = OP_BLOCK_WRITE;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            resp.reserved = REQUEST_COOKIE;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_BLOCK_FLUSH: {
            int const RET = ker::dev::block_flush(bdev);

            DevOpRespPayload resp = {};
            resp.op_id = OP_BLOCK_FLUSH;
            resp.status = static_cast<int16_t>(RET);
            resp.data_len = 0;
            resp.reserved = REQUEST_COOKIE;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        default: {
            // Unknown op
            mod::dbg::log("[WKI] Unknown block op: node=0x%04x ch=%u op_id=0x%04x", hdr->src_node, hdr->channel_id, req->op_id);
            DevOpRespPayload resp = {};
            resp.op_id = req->op_id;
            resp.status = -1;
            resp.data_len = 0;
            resp.reserved = REQUEST_COOKIE;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }
    }
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Block RDMA ring - tiered signaling (server -> consumer)
// -----------------------------------------------------------------------------

namespace {

void blk_ring_signal_consumer(DevServerBinding* binding) {
    WkiPeer const* peer = wki_peer_find(binding->consumer_node);
    if (peer == nullptr) {
        return;
    }

    // Tier 1: ivshmem doorbell (near-zero latency)
    if (peer->transport != nullptr && peer->transport->doorbell != nullptr) {
        peer->transport->doorbell(peer->transport, binding->consumer_node, binding->blk_zone_id);
        return;
    }

    // Tier 2: RoCE doorbell (if RDMA overlay transport has doorbell)
    if (peer->rdma_transport != nullptr && peer->rdma_transport->doorbell != nullptr) {
        peer->rdma_transport->doorbell(peer->rdma_transport, binding->consumer_node, binding->blk_zone_id);
    }

    // Reliable notify as fallback/backup for RoCE doorbell loss. The consumer
    // watches the pushed CQ state directly, so duplicate notifications are fine.
    ZoneNotifyPayload notify = {};
    notify.zone_id = binding->blk_zone_id;
    notify.op_type = 0;  // completion notification
    wki_send(binding->consumer_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &notify, sizeof(notify));
}

}  // namespace

// -----------------------------------------------------------------------------
// Block RDMA ring - SQ poll (server side)
// -----------------------------------------------------------------------------

namespace {

void roce_write_consumer_header_u32(DevServerBinding* binding, uint64_t offset, uint32_t value) {
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, offset, &value,
                                            sizeof(value));
}

void roce_push_server_indices(DevServerBinding* binding, const BlkRingHeader* hdr) {
    // sq_tail and cq_head are server-owned.  Push cq_head last because the
    // consumer treats it as the completion-availability signal.
    roce_write_consumer_header_u32(binding, __builtin_offsetof(BlkRingHeader, sq_tail), hdr->sq_tail);
    roce_write_consumer_header_u32(binding, __builtin_offsetof(BlkRingHeader, cq_head), hdr->cq_head);
}

// RoCE helper: push CQ entries, data slots, and server-owned indices to proxy.
// Only the single new CQ entry is pushed instead of the entire CQ region to
// minimise RDMA bytes and frame count.
void roce_push_completions(DevServerBinding* binding, uint32_t data_slot, uint32_t data_bytes, uint32_t cq_idx) {
    if (!binding->blk_roce || binding->blk_rdma_transport == nullptr) {
        return;
    }
    auto* hdr = blk_ring_header(binding->blk_zone_ptr);

    // Push the data slot (block data for reads) to the proxy
    if (data_bytes > 0 && data_slot < hdr->data_slot_count) {
        uint32_t const SLOT_OFFSET = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (data_slot * hdr->data_slot_size);
        binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, SLOT_OFFSET,
                                                blk_data_slot(binding->blk_zone_ptr, hdr, data_slot), data_bytes);
    }

    // Push only the single new CQ entry (16 bytes) instead of the full CQ region
    uint32_t const CQ_BASE = blk_ring_cq_offset(hdr->sq_depth);
    uint32_t const ENTRY_OFF = CQ_BASE + (cq_idx * static_cast<uint32_t>(sizeof(BlkCqEntry)));
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, ENTRY_OFF,
                                            static_cast<uint8_t*>(binding->blk_zone_ptr) + ENTRY_OFF, sizeof(BlkCqEntry));

    // Push only server-owned indices; do not overwrite the consumer's sq_head/cq_tail.
    roce_push_server_indices(binding, hdr);
}

// Track accumulated RoCE completions for batch push after draining the SQ loop.
struct BatchCqPush {
    uint32_t data_slot;
    uint32_t data_bytes;
    uint32_t cq_idx;
};

// RoCE helper: push all accumulated completions in a single burst.
// Data slots are pushed individually (each may be large), but CQ entries and
// the server-owned indices are pushed once at the end - amortizing per-completion frame overhead.
void roce_push_completions_batch(DevServerBinding* binding, const BatchCqPush* entries, uint32_t count) {
    if (!binding->blk_roce || binding->blk_rdma_transport == nullptr || count == 0) {
        return;
    }
    auto* hdr = blk_ring_header(binding->blk_zone_ptr);

    // 1. Push all data slots (each slot may be up to 64KB - individual writes)
    for (uint32_t i = 0; i < count; i++) {
        if (entries[i].data_bytes > 0 && entries[i].data_slot < hdr->data_slot_count) {
            uint32_t const SLOT_OFFSET = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (entries[i].data_slot * hdr->data_slot_size);
            binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey,
                                                    SLOT_OFFSET, blk_data_slot(binding->blk_zone_ptr, hdr, entries[i].data_slot),
                                                    entries[i].data_bytes);
        }
    }

    // 2. Push the entire CQ region in one write (64 entries * 16 bytes = 1024 bytes)
    //    This is cheaper than count individual 16-byte writes when count > 1.
    uint32_t const CQ_BASE = blk_ring_cq_offset(hdr->sq_depth);
    uint32_t const CQ_TOTAL = blk_ring_cq_size(hdr->cq_depth);
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, CQ_BASE,
                                            static_cast<uint8_t*>(binding->blk_zone_ptr) + CQ_BASE, CQ_TOTAL);

    // 3. Push server-owned indices last (cq_head is the availability signal).
    roce_push_server_indices(binding, hdr);
}

void blk_ring_server_poll(DevServerBinding* binding) {
    if (!binding->blk_rdma_active || binding->blk_zone_ptr == nullptr) {
        return;
    }

    // Guard against concurrent poll from timer thread + post_handler on different CPUs.
    // Use atomic test-and-set to ensure only one caller processes the ring at a time.
    bool expected = false;
    if (!binding->blk_poll_active.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        return;  // another CPU is already polling this ring
    }

    auto* hdr = blk_ring_header(binding->blk_zone_ptr);
    if (hdr->server_ready == 0) {
        binding->blk_poll_active.store(false, std::memory_order_release);
        return;
    }

    // For RoCE zones the consumer already pushes the SQ region into our local
    // copy via rdma_write (roce_push_sq) before sending the doorbell/notification.
    // No need to do an RDMA_READ here - the data is already in blk_zone_ptr.

    bool posted_cqe = false;

    // Accumulate RoCE completions for batch push (avoids per-CQE RDMA writes)
    constexpr uint32_t MAX_BATCH_CQ = 64;
    std::array<BatchCqPush, MAX_BATCH_CQ> batch_pushes = {};
    uint32_t batch_count = 0;

    while (!blk_sq_empty(hdr)) {
        // Check CQ has space before processing
        if (blk_cq_full(hdr)) {
            break;
        }

        uint32_t const SQ_IDX = hdr->sq_tail % hdr->sq_depth;
        auto* sqe = &blk_sq_entries(binding->blk_zone_ptr)[SQ_IDX];

        BlkCqEntry cqe = {};
        cqe.tag = sqe->tag;
        cqe.data_slot = sqe->data_slot;
        uint32_t data_bytes = 0;

        switch (static_cast<BlkOpcode>(sqe->opcode)) {
            case BlkOpcode::READ: {
                if (sqe->data_slot >= hdr->data_slot_count) {
                    cqe.status = -1;
                    cqe.bytes_transferred = 0;
                    break;
                }
                uint8_t* dest = blk_data_slot(binding->blk_zone_ptr, hdr, sqe->data_slot);
                int const RET = ker::dev::block_read(binding->block_dev, sqe->lba, sqe->block_count, dest);
                cqe.status = RET;
                cqe.bytes_transferred = (RET == 0) ? sqe->block_count * hdr->block_size : 0;
                data_bytes = cqe.bytes_transferred;
                break;
            }
            case BlkOpcode::WRITE: {
                if (sqe->data_slot >= hdr->data_slot_count) {
                    cqe.status = -1;
                    cqe.bytes_transferred = 0;
                    break;
                }
                if (binding->block_read_only) {
                    cqe.status = -EROFS;
                    cqe.bytes_transferred = 0;
                    break;
                }
                // For RoCE writes: pull the data slot from proxy before writing to disk
                if (binding->blk_roce && binding->blk_rdma_transport != nullptr) {
                    uint32_t const SLOT_OFFSET =
                        blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (sqe->data_slot * hdr->data_slot_size);
                    uint32_t const SLOT_BYTES = sqe->block_count * hdr->block_size;
                    binding->blk_rdma_transport->rdma_read(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey,
                                                           SLOT_OFFSET, blk_data_slot(binding->blk_zone_ptr, hdr, sqe->data_slot),
                                                           SLOT_BYTES);
                }
                const uint8_t* src = blk_data_slot(binding->blk_zone_ptr, hdr, sqe->data_slot);
                int const RET = ker::dev::block_write(binding->block_dev, sqe->lba, sqe->block_count, src);
                cqe.status = RET;
                cqe.bytes_transferred = 0;
                break;
            }
            case BlkOpcode::FLUSH: {
                cqe.status = ker::dev::block_flush(binding->block_dev);
                cqe.bytes_transferred = 0;
                break;
            }
            case BlkOpcode::BULK_READ: {
                // Streaming bulk read: read blocks from device into a temporary
                // staging buffer, then RDMA-write the entire range to the
                // consumer's pre-registered staging buffer (rkey in data_slot).
                uint32_t const CONSUMER_RKEY = sqe->data_slot;
                uint32_t const TOTAL_BYTES = sqe->block_count * hdr->block_size;
                auto* staging = new (std::nothrow) uint8_t[TOTAL_BYTES];
                if (staging == nullptr) {
                    cqe.status = -12;  // ENOMEM
                    cqe.bytes_transferred = 0;
                    break;
                }

                int const RET = ker::dev::block_read(binding->block_dev, sqe->lba, sqe->block_count, staging);
                if (RET == 0 && binding->blk_rdma_transport != nullptr) {
                    // RDMA-write directly into consumer's staging buffer at offset 0
                    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, CONSUMER_RKEY, 0, staging,
                                                            TOTAL_BYTES);
                    cqe.bytes_transferred = TOTAL_BYTES;
                } else {
                    cqe.bytes_transferred = 0;
                }
                cqe.status = RET;
                delete[] staging;
                // Bulk ops push data directly via RDMA - no ring data slot involved
                data_bytes = 0;
                break;
            }
            case BlkOpcode::BULK_WRITE: {
                // Streaming bulk write: RDMA-read entire range from consumer's
                // registered staging buffer, then write blocks to device.
                if (binding->block_read_only) {
                    cqe.status = -EROFS;
                    cqe.bytes_transferred = 0;
                    data_bytes = 0;
                    break;
                }
                uint32_t const CONSUMER_RKEY = sqe->data_slot;
                uint32_t const TOTAL_BYTES = sqe->block_count * hdr->block_size;
                auto* staging = new (std::nothrow) uint8_t[TOTAL_BYTES];
                if (staging == nullptr) {
                    cqe.status = -12;  // ENOMEM
                    cqe.bytes_transferred = 0;
                    break;
                }

                int ret = 0;
                if (binding->blk_rdma_transport != nullptr) {
                    // RDMA-read from consumer's staging buffer at offset 0
                    binding->blk_rdma_transport->rdma_read(binding->blk_rdma_transport, binding->consumer_node, CONSUMER_RKEY, 0, staging,
                                                           TOTAL_BYTES);
                    ret = ker::dev::block_write(binding->block_dev, sqe->lba, sqe->block_count, staging);
                } else {
                    ret = -1;
                }
                cqe.status = ret;
                cqe.bytes_transferred = 0;
                delete[] staging;
                data_bytes = 0;
                break;
            }
            default: {
                cqe.status = -1;
                cqe.bytes_transferred = 0;
                break;
            }
        }

        // Advance SQ tail (consume entry)
        asm volatile("" ::: "memory");
        hdr->sq_tail = (hdr->sq_tail + 1) % hdr->sq_depth;

        // Post CQ entry
        uint32_t const CQ_IDX = hdr->cq_head % hdr->cq_depth;
        blk_cq_entries(binding->blk_zone_ptr, hdr)[CQ_IDX] = cqe;
        asm volatile("" ::: "memory");
        hdr->cq_head = (hdr->cq_head + 1) % hdr->cq_depth;

        // Accumulate for batch RoCE push (instead of per-CQE push)
        if (batch_count < MAX_BATCH_CQ) {
            batch_pushes.at(batch_count++) = {.data_slot = sqe->data_slot, .data_bytes = data_bytes, .cq_idx = CQ_IDX};
        }

        posted_cqe = true;
    }

    // Batch-push all accumulated RoCE completions in one burst:
    // - Data slots are pushed individually (each up to 64KB)
    // - CQ region + server-owned indices pushed once at the end
    // For single completions this falls back to the per-CQE path
    // (equivalent cost: N data + 1 CQ region + indices, vs N * (1 data + 1 CQ entry + indices)).
    if (batch_count == 1) {
        const auto& push = batch_pushes.front();
        roce_push_completions(binding, push.data_slot, push.data_bytes, push.cq_idx);
    } else if (batch_count > 1) {
        roce_push_completions_batch(binding, batch_pushes.data(), batch_count);
    }

    // Release poll guard so other CPUs can poll this ring
    binding->blk_poll_active.store(false, std::memory_order_release);

    if (posted_cqe) {
        blk_ring_signal_consumer(binding);
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Zone post_handler - called when the consumer sends ZONE_NOTIFY_POST
// Triggers immediate ring polling instead of waiting for the timer tick.
// -----------------------------------------------------------------------------

namespace {

void blk_zone_post_handler(uint32_t zone_id, uint32_t /*offset*/, uint32_t /*length*/, uint8_t /*op_type*/) {
    DevServerBinding* binding = nullptr;
    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        binding = find_binding_by_zone_id(zone_id);
        if (retain_binding_locked(binding)) {
            binding->blk_sq_notified = true;
        } else {
            binding = nullptr;
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    // blk_ring_server_poll may do block I/O - call outside the lock
    if (binding != nullptr) {
        blk_ring_server_poll(binding);
        release_binding(binding);
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Deferred zone creation - runs from wki_timer_tick, outside NAPI poll context
// -----------------------------------------------------------------------------

void wki_dev_server_process_pending_zones() {
    // Collect pending bindings under the lock, then process outside
    struct PendingBinding {
        DevServerBinding* binding;
        uint16_t consumer_node;
        uint32_t blk_zone_id;
        ker::dev::BlockDevice* block_dev;
    };
    constexpr size_t MAX_PENDING = 32;
    std::array<PendingBinding, MAX_PENDING> pending = {};
    size_t pending_count = 0;

    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        for (auto& b : g_bindings) {
            if (!b.active || !b.blk_zone_pending) {
                continue;
            }
            if (pending_count < MAX_PENDING) {
                if (!retain_binding_locked(&b)) {
                    continue;
                }
                b.blk_zone_pending = false;  // only attempt once
                pending.at(pending_count++) = {
                    .binding = &b, .consumer_node = b.consumer_node, .blk_zone_id = b.blk_zone_id, .block_dev = b.block_dev};
            }
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    // Process each pending binding outside the lock (wki_zone_create may block)
    for (size_t pi = 0; pi < pending_count; pi++) {
        const auto& item = pending.at(pi);
        DevServerBinding* binding = item.binding;

        uint32_t const ZONE_SZ = blk_ring_default_zone_size();
        uint8_t const ZONE_ACCESS = ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE | ZONE_ACCESS_REMOTE_READ | ZONE_ACCESS_REMOTE_WRITE;

        int const ZONE_RET =
            wki_zone_create(item.consumer_node, item.blk_zone_id, ZONE_SZ, ZONE_ACCESS, ZoneNotifyMode::POST_ONLY, ZoneTypeHint::MSG_QUEUE);
        if (ZONE_RET != WKI_OK) {
            ker::mod::dbg::log("[WKI] Deferred block RDMA ring creation failed (err=%d) for zone 0x%08x - consumer falls back to msg path",
                               ZONE_RET, item.blk_zone_id);
            {
                uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
                if (binding->active && !binding->retiring.load(std::memory_order_acquire) && binding->blk_zone_id == item.blk_zone_id) {
                    binding->blk_zone_id = 0;
                }
                s_server_lock.unlock_irqrestore(SRV_FLAGS);
            }
            release_binding(binding);
            continue;
        }

        void* zone_ptr = wki_zone_get_ptr(item.blk_zone_id);
        bool blk_rdma_active = (zone_ptr != nullptr);

        if (!blk_rdma_active) {
            ker::mod::dbg::log("[WKI] Deferred block RDMA zone ptr null for zone 0x%08x", item.blk_zone_id);
            {
                uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
                if (binding->active && !binding->retiring.load(std::memory_order_acquire) && binding->blk_zone_id == item.blk_zone_id) {
                    binding->blk_zone_id = 0;
                }
                s_server_lock.unlock_irqrestore(SRV_FLAGS);
            }
            wki_zone_destroy(item.blk_zone_id);
            release_binding(binding);
            continue;
        }

        // Check if the zone is RoCE-backed (needs explicit rdma_write/read sync)
        WkiZone const* blk_zone = wki_zone_find(item.blk_zone_id);
        bool blk_roce = false;
        uint32_t blk_remote_rkey = 0;
        WkiTransport* blk_rdma_transport = nullptr;
        if (blk_zone != nullptr && blk_zone->is_roce) {
            blk_roce = true;
            blk_remote_rkey = blk_zone->remote_rkey;
            blk_rdma_transport = blk_zone->rdma_transport;
        }

        bool binding_still_active = false;
        {
            uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
            if (binding->active && !binding->retiring.load(std::memory_order_acquire) && binding->blk_zone_id == item.blk_zone_id) {
                binding->blk_zone_ptr = zone_ptr;
                binding->blk_rdma_active = true;
                binding->blk_roce = blk_roce;
                binding->blk_remote_rkey = blk_remote_rkey;
                binding->blk_rdma_transport = blk_rdma_transport;
                binding_still_active = true;
            }
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
        }

        if (!binding_still_active) {
            wki_zone_destroy(item.blk_zone_id);
            release_binding(binding);
            continue;
        }

        // Initialize the ring header in local memory
        auto* ring_hdr = blk_ring_header(zone_ptr);
        ring_hdr->sq_head = 0;
        ring_hdr->sq_tail = 0;
        ring_hdr->cq_head = 0;
        ring_hdr->cq_tail = 0;
        ring_hdr->sq_depth = BLK_RING_DEFAULT_SQ_DEPTH;
        ring_hdr->cq_depth = BLK_RING_DEFAULT_CQ_DEPTH;
        ring_hdr->data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS;
        ring_hdr->data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE;
        ring_hdr->block_size = item.block_dev != nullptr ? static_cast<uint32_t>(item.block_dev->block_size) : 0;
        ring_hdr->total_blocks = item.block_dev != nullptr ? item.block_dev->total_blocks : 0;
        // Compiler barrier: ensure all fields are visible before server_ready
        asm volatile("" ::: "memory");
        ring_hdr->server_ready = 1;

        // For RoCE zones: push the entire ring header to the proxy so it
        // can see server_ready and device parameters
        if (blk_roce && blk_rdma_transport != nullptr && blk_remote_rkey != 0) {
            blk_rdma_transport->rdma_write(blk_rdma_transport, item.consumer_node, blk_remote_rkey, 0, ring_hdr, sizeof(BlkRingHeader));
        }

        ker::mod::dbg::log("[WKI] Deferred block RDMA ring created: zone=0x%08x size=%u roce=%d", item.blk_zone_id, ZONE_SZ,
                           blk_roce ? 1 : 0);

        // Register a zone post_handler so that ZONE_NOTIFY_POST from the consumer
        // triggers immediate ring polling instead of waiting for the ~10ms timer tick.
        wki_zone_set_handlers(item.blk_zone_id, nullptr, blk_zone_post_handler);
        release_binding(binding);
    }
}

// -----------------------------------------------------------------------------
// Block RDMA ring - periodic poll (called from wki_timer_tick)
// -----------------------------------------------------------------------------

auto wki_dev_server_get_vfs_write_buf(uint16_t consumer_node, uint16_t channel_id) -> uint8_t* {
    return wki_dev_server_get_vfs_write_region(consumer_node, channel_id).buf;
}

auto wki_dev_server_get_vfs_write_region(uint16_t consumer_node, uint16_t channel_id) -> VfsWriteRegionInfo {
    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto& b : g_bindings) {
        if (b.active && b.resource_type == ResourceType::VFS && b.consumer_node == consumer_node && b.assigned_channel == channel_id) {
            VfsWriteRegionInfo info = {};
            info.buf = b.vfs_rdma_write_buf;
            info.rkey = b.vfs_rdma_write_rkey;
            info.transport = b.vfs_rdma_write_transport;
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
            return info;
        }
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
    return {};
}

void wki_dev_server_complete_vfs_write(uint16_t consumer_node, uint16_t channel_id, uint16_t req_cookie, int16_t status,
                                       uint32_t bytes_written) {
    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    DevServerBinding* binding = find_binding_by_channel(consumer_node, channel_id);
    if (binding != nullptr && binding->resource_type == ResourceType::VFS) {
        if (binding->vfs_rdma_write_active && binding->vfs_rdma_write_active_cookie == req_cookie) {
            binding->vfs_rdma_write_active = false;
        }
        binding->vfs_rdma_write_resp_valid = true;
        binding->vfs_rdma_write_resp_cookie = req_cookie;
        binding->vfs_rdma_write_resp_status = status;
        binding->vfs_rdma_write_resp_bytes = bytes_written;
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
}

void wki_dev_server_send_vfs_notify(uint32_t resource_id, uint16_t op_id, const uint8_t* data, uint16_t data_len) {
    if (resource_id == 0 || data == nullptr) {
        return;
    }

    std::array<uint8_t, sizeof(DevOpReqPayload) + 516> req{};
    size_t const TOTAL = sizeof(DevOpReqPayload) + data_len;
    if (TOTAL > req.size()) {
        return;
    }

    auto* req_hdr = reinterpret_cast<DevOpReqPayload*>(req.data());
    req_hdr->op_id = op_id;
    req_hdr->data_len = data_len;
    std::memcpy(req.data() + sizeof(DevOpReqPayload), data, data_len);

    struct Target {
        uint16_t consumer_node = WKI_NODE_INVALID;
        uint16_t channel_id = 0;
    };
    std::deque<Target> targets;

    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (const auto& binding : g_bindings) {
        if (!binding.active || binding.resource_type != ResourceType::VFS || binding.resource_id != resource_id) {
            continue;
        }
        targets.push_back({.consumer_node = binding.consumer_node, .channel_id = binding.assigned_channel});
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);

    for (const auto& target : targets) {
        static_cast<void>(wki_send(target.consumer_node, target.channel_id, MsgType::DEV_OP_REQ, req.data(), static_cast<uint16_t>(TOTAL)));
    }
}

auto wki_dev_server_get_vfs_read_staging_buf(uint16_t consumer_node, uint16_t channel_id) -> uint8_t* {
    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto& b : g_bindings) {
        if (b.active && b.resource_type == ResourceType::VFS && b.consumer_node == consumer_node && b.assigned_channel == channel_id) {
            uint8_t* buf = b.vfs_rdma_read_staging_buf;
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
            return buf;
        }
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
    return nullptr;
}

auto wki_dev_server_get_vfs_bulk_staging_buf(uint16_t consumer_node, uint16_t channel_id) -> uint8_t* {
    uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
    for (auto& b : g_bindings) {
        if (b.active && b.resource_type == ResourceType::VFS && b.consumer_node == consumer_node && b.assigned_channel == channel_id) {
            uint8_t* buf = b.vfs_rdma_bulk_staging_buf;
            s_server_lock.unlock_irqrestore(SRV_FLAGS);
            return buf;
        }
    }
    s_server_lock.unlock_irqrestore(SRV_FLAGS);
    return nullptr;
}

void wki_dev_server_poll_rings() {
    // Collect bindings to poll under the lock, then poll outside
    constexpr size_t MAX_RINGS = 32;
    std::array<DevServerBinding*, MAX_RINGS> rings = {};
    size_t ring_count = 0;

    {
        uint64_t const SRV_FLAGS = s_server_lock.lock_irqsave();
        for (auto& b : g_bindings) {
            if (!b.active || !b.blk_rdma_active) {
                continue;
            }
            // For RoCE bindings, only poll when the consumer has signalled new work.
            // The consumer pushes the SQ via RDMA_WRITE then sends a doorbell;
            // there is nothing to do until that notification arrives.
            // Non-RoCE (ivshmem) bindings use shared memory - polling is cheap.
            if (b.blk_roce && !b.blk_sq_notified) {
                continue;
            }
            if (ring_count < MAX_RINGS) {
                if (!retain_binding_locked(&b)) {
                    continue;
                }
                b.blk_sq_notified = false;
                rings.at(ring_count++) = &b;
            }
        }
        s_server_lock.unlock_irqrestore(SRV_FLAGS);
    }

    // blk_ring_server_poll may do block I/O - call outside the lock
    for (size_t i = 0; i < ring_count; i++) {
        DevServerBinding* binding = rings.at(i);
        blk_ring_server_poll(binding);
        release_binding(binding);
    }
}

#ifdef WOS_SELFTEST
auto wki_dev_server_selftest_binding_lifecycle_flags() -> bool {
    DevServerBinding binding;
    binding.active = true;
    binding.refs.store(2, std::memory_order_relaxed);
    binding.retiring.store(true, std::memory_order_relaxed);

    DevServerBinding moved(std::move(binding));
    return moved.active && moved.refs.load(std::memory_order_relaxed) == 2 && moved.retiring.load(std::memory_order_relaxed);
}

auto wki_dev_server_selftest_attach_ack_failure_rolls_back_binding() -> bool {
    auto* write_buf = new (std::nothrow) uint8_t[1];
    auto* read_buf = new (std::nothrow) uint8_t[1];
    auto* bulk_buf = new (std::nothrow) uint8_t[1];
    if (write_buf == nullptr || read_buf == nullptr || bulk_buf == nullptr) {
        delete[] write_buf;
        delete[] read_buf;
        delete[] bulk_buf;
        return false;
    }

    constexpr uint16_t SURVIVOR_NODE = 0x701;
    constexpr uint16_t BLOCK_NODE = 0x702;
    constexpr uint16_t VFS_NODE = 0x703;
    constexpr uint16_t SURVIVOR_CHANNEL = 41;
    constexpr uint16_t BLOCK_CHANNEL = 42;
    constexpr uint16_t VFS_CHANNEL = 43;
    constexpr uint32_t SURVIVOR_RESOURCE = 11;
    constexpr uint32_t BLOCK_RESOURCE = 12;
    constexpr uint32_t VFS_RESOURCE = 13;

    uint64_t flags = s_server_lock.lock_irqsave();
    size_t const START_SIZE = g_bindings.size();

    auto& survivor = g_bindings.emplace_back();
    survivor.active = true;
    survivor.consumer_node = SURVIVOR_NODE;
    survivor.assigned_channel = SURVIVOR_CHANNEL;
    survivor.resource_type = ResourceType::BLOCK;
    survivor.resource_id = SURVIVOR_RESOURCE;

    auto& block = g_bindings.emplace_back();
    block.active = true;
    block.consumer_node = BLOCK_NODE;
    block.assigned_channel = BLOCK_CHANNEL;
    block.resource_type = ResourceType::BLOCK;
    block.resource_id = BLOCK_RESOURCE;

    auto& vfs = g_bindings.emplace_back();
    vfs.active = true;
    vfs.consumer_node = VFS_NODE;
    vfs.assigned_channel = VFS_CHANNEL;
    vfs.resource_type = ResourceType::VFS;
    vfs.resource_id = VFS_RESOURCE;
    vfs.vfs_rdma_write_buf = write_buf;
    vfs.vfs_rdma_read_staging_buf = read_buf;
    vfs.vfs_rdma_bulk_staging_buf = bulk_buf;

    s_server_lock.unlock_irqrestore(flags);

    rollback_attach_ack_failure(BLOCK_NODE, ResourceType::BLOCK, BLOCK_RESOURCE, BLOCK_CHANNEL);
    rollback_attach_ack_failure(VFS_NODE, ResourceType::VFS, VFS_RESOURCE, VFS_CHANNEL);

    flags = s_server_lock.lock_irqsave();
    bool survivor_found = false;
    bool block_removed = true;
    bool vfs_removed = true;
    for (auto it = g_bindings.begin(); it != g_bindings.end();) {
        if (it->consumer_node == SURVIVOR_NODE && it->resource_type == ResourceType::BLOCK && it->resource_id == SURVIVOR_RESOURCE &&
            it->assigned_channel == SURVIVOR_CHANNEL) {
            survivor_found = true;
            it = g_bindings.erase(it);
            continue;
        }
        if (it->consumer_node == BLOCK_NODE && it->resource_type == ResourceType::BLOCK && it->resource_id == BLOCK_RESOURCE &&
            it->assigned_channel == BLOCK_CHANNEL) {
            block_removed = false;
            it = g_bindings.erase(it);
            continue;
        }
        if (it->consumer_node == VFS_NODE && it->resource_type == ResourceType::VFS && it->resource_id == VFS_RESOURCE &&
            it->assigned_channel == VFS_CHANNEL) {
            vfs_removed = false;
            delete[] it->vfs_rdma_write_buf;
            delete[] it->vfs_rdma_read_staging_buf;
            delete[] it->vfs_rdma_bulk_staging_buf;
            it = g_bindings.erase(it);
            continue;
        }
        ++it;
    }
    bool const SIZE_RESTORED = g_bindings.size() == START_SIZE;
    s_server_lock.unlock_irqrestore(flags);

    return survivor_found && block_removed && vfs_removed && SIZE_RESTORED;
}
#endif

}  // namespace ker::net::wki
