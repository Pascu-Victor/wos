#include "virtio_net.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mod/io/port/port.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/wire.hpp>
#include <new>
#include <platform/acpi/ioapic/ioapic.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

#include "dev/pci.hpp"
#include "dev/virtio/virtio.hpp"
#include "net/address.hpp"
#include "platform/dbg/dbg.hpp"
#include "util/hcf.hpp"

namespace ker::dev::virtio {

namespace {
using net_log = ker::mod::dbg::logger<"net">;

auto remotable_can_remote() -> bool { return true; }
auto remotable_can_share() -> bool { return true; }
auto remotable_can_passthrough() -> bool { return false; }
auto remotable_on_attach(uint16_t node_id) -> int {
    net_log::trace("remote attach from 0x%04x", node_id);
    return 0;
}
void remotable_on_detach(uint16_t node_id) { net_log::trace("remote detach from 0x%04x", node_id); }
void remotable_on_fault(uint16_t node_id) { net_log::trace("remote fault for 0x%04x", node_id); }
const ker::net::wki::RemotableOps S_REMOTABLE_OPS = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

constexpr size_t MAX_VIRTIO_NET_DEVICES = 4;
std::array<VirtIONetDevice*, MAX_VIRTIO_NET_DEVICES> devices = {};
size_t device_count = 0;

constexpr uint8_t SINGLE_QUEUE_PAIRS = 1;
constexpr uint8_t MIN_MQ_QUEUE_PAIRS = 2;
constexpr uint8_t WKI_CONTROL_TX_RETRY_POLLS = 8;

void init_queue_pair_contexts(VirtIONetDevice* dev) {
    for (uint8_t i = 0; i < VIRTIO_NET_MAX_QUEUE_PAIRS; i++) {
        auto& pair = dev->queue_pairs.at(i);
        pair.dev = dev;
        pair.index = i;
    }
}

auto rx_queue_index(uint8_t pair) -> uint16_t { return static_cast<uint16_t>(pair * 2U); }
auto tx_queue_index(uint8_t pair) -> uint16_t { return static_cast<uint16_t>((pair * 2U) + 1U); }

auto net_cpu_for_pair(uint64_t core_count, uint8_t pair) -> uint64_t {
    if (core_count == 0) {
        return 0;
    }
    return (core_count - 1U) - (static_cast<uint64_t>(pair) % core_count);
}

auto desired_queue_pairs(uint16_t device_max_pairs, uint64_t core_count) -> uint8_t {
    uint16_t const CPU_LIMIT = static_cast<uint16_t>(std::max<uint64_t>(core_count, 1U));
    uint16_t const CAPPED = std::min({device_max_pairs, CPU_LIMIT, static_cast<uint16_t>(VIRTIO_NET_MAX_QUEUE_PAIRS)});
    if (CAPPED < MIN_MQ_QUEUE_PAIRS) {
        return SINGLE_QUEUE_PAIRS;
    }
    return static_cast<uint8_t>(CAPPED);
}

auto queue_pair_for_napi(VirtIONetDevice* dev, ker::net::NapiStruct* napi) -> VirtIONetQueuePair* {
    uint8_t const CONFIGURED = std::min<uint8_t>(dev->configured_queue_pairs, VIRTIO_NET_MAX_QUEUE_PAIRS);
    for (uint8_t i = 0; i < CONFIGURED; i++) {
        auto& pair = dev->queue_pairs.at(i);
        if (&pair.napi == napi) {
            return &pair;
        }
    }
    return nullptr;
}

auto active_tx_pair_count(const VirtIONetDevice* dev) -> uint8_t {
    if (dev == nullptr) {
        return 0;
    }
    return std::min<uint8_t>(std::max<uint8_t>(dev->num_queue_pairs, SINGLE_QUEUE_PAIRS), VIRTIO_NET_MAX_QUEUE_PAIRS);
}

auto virtio_net_hdr_size_for_features(uint32_t features) -> uint8_t {
    return static_cast<uint8_t>((features & VIRTIO_NET_F_MRG_RXBUF) != 0 ? VIRTIO_NET_HDR_SIZE_MRG_RXBUF : VIRTIO_NET_HDR_SIZE);
}

auto virtio_net_rx_buffer_count(const VirtIONetDevice* dev, const ker::net::PacketBuffer* pkt) -> uint16_t {
    if (dev == nullptr || pkt == nullptr || (dev->negotiated_features & VIRTIO_NET_F_MRG_RXBUF) == 0) {
        return 1;
    }

    const auto* hdr = reinterpret_cast<const VirtIONetHeaderMergeableRx*>(pkt->storage.data());
    return hdr->num_buffers;
}

void virtio_net_drop_rx_buffer(VirtIONetDevice* dev, Virtqueue* rxq, uint16_t desc_idx) {
    auto* pkt = rxq->pkt_map.at(desc_idx);
    rxq->pkt_map.at(desc_idx) = nullptr;
    if (pkt != nullptr) {
        ker::net::pkt_free(pkt);
    }
    dev->netdev.rx_dropped++;
}

auto is_wki_control_liveness_frame(const ker::net::PacketBuffer* pkt) -> bool {
    if (pkt == nullptr || pkt->data == nullptr || pkt->len < ker::net::proto::ETH_HLEN + ker::net::wki::WKI_HEADER_SIZE) {
        return false;
    }

    const auto* eth = reinterpret_cast<const ker::net::proto::EthernetHeader*>(pkt->data);
    if (ker::net::ntohs(eth->ethertype) != ker::net::proto::ETH_TYPE_WKI) {
        return false;
    }

    const auto* hdr = reinterpret_cast<const ker::net::wki::WkiHeader*>(pkt->data + ker::net::proto::ETH_HLEN);
    if (ker::net::wki::wki_version(hdr->version_flags) != ker::net::wki::WKI_VERSION) {
        return false;
    }
    if (ker::net::proto::ETH_HLEN + ker::net::wki::WKI_HEADER_SIZE + hdr->payload_len > pkt->len) {
        return false;
    }

    auto const TYPE = static_cast<ker::net::wki::MsgType>(hdr->msg_type);
    switch (TYPE) {
        case ker::net::wki::MsgType::HELLO:
        case ker::net::wki::MsgType::HELLO_ACK:
            return hdr->channel_id == ker::net::wki::WKI_CHAN_CONTROL && hdr->payload_len <= sizeof(ker::net::wki::HelloPayload);
        case ker::net::wki::MsgType::HEARTBEAT:
        case ker::net::wki::MsgType::HEARTBEAT_ACK:
            return hdr->payload_len <= sizeof(ker::net::wki::HeartbeatPayload);
        case ker::net::wki::MsgType::FENCE_NOTIFY:
            return hdr->channel_id == ker::net::wki::WKI_CHAN_CONTROL && hdr->payload_len <= sizeof(ker::net::wki::FenceNotifyPayload);
        default:
            return (ker::net::wki::wki_flags(hdr->version_flags) & ker::net::wki::WKI_FLAG_PRIORITY) != 0 &&
                   hdr->payload_len <= sizeof(ker::net::wki::HeartbeatPayload);
    }
}

// Handle both kernel static and HHDM mappings.
auto virt_to_phys(void* vaddr) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(vaddr);
    if (addr >= mod::mm::addr::get_hhdm_offset()) {
        uint64_t const PHYS = ker::mod::mm::virt::translate(ker::mod::mm::virt::get_kernel_pagemap(), addr);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            net_log::error("virt_to_phys: failed for kernel address 0x%lx", addr);
            hcf();
        }
        return PHYS;
    }
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(addr));
}

void fill_rx_queue_for(VirtIONetDevice* dev, Virtqueue* rxq) {
    const char* const DEV_NAME = dev != nullptr ? dev->netdev.name.data() : "?";
    if (rxq->num_free > rxq->size) {
        net_log::error("virtio rx refill: dev=%s q=%u corrupt free count free=%u size=%u", DEV_NAME, rxq->queue_index, rxq->num_free,
                       rxq->size);
        rxq->num_free = 0;
        rxq->free_head = VIRTQ_NO_BUF;
        return;
    }

    size_t target_free = ker::net::pkt_pool_free_count();
    if (rxq->num_free > 0) {
        target_free = std::max(target_free, static_cast<size_t>(rxq->num_free) + ker::net::PKT_POOL_TX_RESERVE);
        ker::net::pkt_pool_ensure_free(target_free);
    }

    size_t const BEFORE_FREE = ker::net::pkt_pool_free_count();
    uint16_t filled = 0;
    while (rxq->num_free > 0) {
        if (ker::net::pkt_pool_free_count() <= ker::net::PKT_POOL_TX_RESERVE) {
            break;
        }
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            break;
        }
        pkt->data = pkt->storage.data();
        pkt->len = 0;

        uint64_t const PHYS = virt_to_phys(pkt->storage.data());
        uint32_t const BUF_LEN = ker::net::PKT_BUF_SIZE;

        if (virtq_add_buf(rxq, PHYS, BUF_LEN, VRING_DESC_F_WRITE, pkt) < 0) {
            ker::net::pkt_free(pkt);
            break;
        }
        filled++;
    }

    if (rxq->num_free != 0) {
        net_log::warn("virtio rx refill short: dev=%s q=%u filled=%u remaining_slots=%u pool_free_before=%zu pool_free=%zu", DEV_NAME,
                      rxq->queue_index, filled, rxq->num_free, BEFORE_FREE, ker::net::pkt_pool_free_count());
    } else if (filled >= (rxq->size / 2U)) {
        net_log::trace("virtio rx refill: dev=%s q=%u filled=%u pool_free_before=%zu pool_free=%zu", DEV_NAME, rxq->queue_index, filled,
                       BEFORE_FREE, ker::net::pkt_pool_free_count());
    }
    virtq_kick(rxq);
}

auto process_rx_budget_for(VirtIONetDevice* dev, Virtqueue* rxq, int budget) -> int {
    int processed = 0;
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while (processed < budget) {
        desc_idx = virtq_get_buf(rxq, &len);
        if (desc_idx == VIRTQ_NO_BUF) {
            break;
        }
        processed++;
        if (desc_idx == VIRTQ_BAD_BUF) {
            dev->netdev.rx_dropped++;
            continue;
        }

        auto* pkt = rxq->pkt_map.at(desc_idx);
        rxq->pkt_map.at(desc_idx) = nullptr;

        if (pkt == nullptr || len <= static_cast<uint32_t>(dev->hdr_size) || len > ker::net::PKT_BUF_SIZE) {
            if (pkt != nullptr) {
                ker::net::pkt_free(pkt);
            }
            dev->netdev.rx_dropped++;
            continue;
        }

        uint16_t const RX_BUFFERS = virtio_net_rx_buffer_count(dev, pkt);
        if (RX_BUFFERS == 0 || RX_BUFFERS > 1) {
            ker::net::pkt_free(pkt);
            dev->netdev.rx_dropped++;

            // We provide jumbo-sized receive buffers, so normal packets should
            // fit in one buffer.  If the device still returns a merged packet,
            // drain the remaining fragments so they are not misdelivered as
            // independent Ethernet frames.
            for (uint16_t consumed = 1; consumed < RX_BUFFERS; consumed++) {
                uint16_t const EXTRA_DESC = virtq_get_buf(rxq, nullptr);
                if (EXTRA_DESC == VIRTQ_NO_BUF) {
                    break;
                }
                processed++;
                if (EXTRA_DESC == VIRTQ_BAD_BUF) {
                    dev->netdev.rx_dropped++;
                    continue;
                }
                virtio_net_drop_rx_buffer(dev, rxq, EXTRA_DESC);
            }
            continue;
        }

        pkt->data = pkt->storage.data() + dev->hdr_size;
        pkt->len = len - dev->hdr_size;
        pkt->dev = &dev->netdev;

        ker::net::netdev_rx(&dev->netdev, pkt);
    }

    fill_rx_queue_for(dev, rxq);

    return processed;
}

// Collect completed TX buffers from a specific queue; caller frees after releasing lock.
auto collect_tx_for(Virtqueue* txq) -> ker::net::PacketBuffer* {
    ker::net::PacketBuffer* head = nullptr;
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while ((desc_idx = virtq_get_buf(txq, &len)) != VIRTQ_NO_BUF) {
        if (desc_idx == VIRTQ_BAD_BUF) {
            continue;
        }

        auto* pkt = txq->pkt_map.at(desc_idx);
        txq->pkt_map.at(desc_idx) = nullptr;
        if (pkt != nullptr) {
            pkt->next = head;
            head = pkt;
        }
    }
    return head;
}

void free_pkt_list(ker::net::PacketBuffer* head) {
    while (head != nullptr) {
        auto* next = head->next;
        ker::net::pkt_free(head);
        head = next;
    }
}

void fill_virtqueue_diag(Virtqueue* vq, VirtqueueDiagSnapshot& out) {
    out = {};
    if (vq == nullptr) {
        return;
    }

    uint64_t const FLAGS = vq->lock.lock_irqsave();
    out.size = vq->size;
    out.num_free = vq->num_free;
    out.pending = virtq_pending_count(vq);
    out.avail_idx = vq->avail->idx;
    out.used_idx = vq->used->idx;
    out.last_used_idx = vq->last_used_idx;
    uint16_t mapped = 0;
    for (uint16_t i = 0; i < vq->size && i < vq->pkt_map.size(); ++i) {
        if (vq->pkt_map.at(i) != nullptr) {
            ++mapped;
        }
    }
    out.mapped = mapped;
    vq->lock.unlock_irqrestore(FLAGS);
}

// Per-queue-pair MSI-X vector control. The MSI-X entry index matches the
// queue-pair index.
void virtio_net_irq_disable_pair(VirtIONetDevice* dev, uint8_t pair) {
    if (!dev->msix_enabled) {
        return;
    }
    auto const RX_Q = static_cast<uint16_t>(pair * 2);
    auto const TX_Q = static_cast<uint16_t>((pair * 2) + 1);
    uint64_t const FLAGS = dev->irq_lock.lock_irqsave();
    if (dev->modern_cfg != nullptr) {
        dev->modern_cfg->queue_select = RX_Q;
        dev->modern_cfg->queue_msix_vector = VIRTIO_MSI_NO_VECTOR;
        dev->modern_cfg->queue_select = TX_Q;
        dev->modern_cfg->queue_msix_vector = VIRTIO_MSI_NO_VECTOR;
    } else {
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, RX_Q);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, TX_Q);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
    }
    dev->irq_lock.unlock_irqrestore(FLAGS);
}

void virtio_net_irq_enable_pair(VirtIONetDevice* dev, uint8_t pair) {
    if (!dev->msix_enabled) {
        return;
    }
    auto const RX_Q = static_cast<uint16_t>(pair * 2);
    auto const TX_Q = static_cast<uint16_t>((pair * 2) + 1);
    uint16_t const ENTRY = pair;
    uint64_t const FLAGS = dev->irq_lock.lock_irqsave();
    if (dev->modern_cfg != nullptr) {
        dev->modern_cfg->queue_select = RX_Q;
        dev->modern_cfg->queue_msix_vector = ENTRY;
        dev->modern_cfg->queue_select = TX_Q;
        dev->modern_cfg->queue_msix_vector = ENTRY;
    } else {
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, RX_Q);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, ENTRY);
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, TX_Q);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, ENTRY);
    }
    dev->irq_lock.unlock_irqrestore(FLAGS);
}

int virtio_net_poll(ker::net::NapiStruct* napi, int budget) {
    NET_TRACE_SPAN(SPAN_NAPI_POLL);
    auto* dev = static_cast<VirtIONetDevice*>(napi->dev->private_data);
    auto* pair = queue_pair_for_napi(dev, napi);
    if (pair == nullptr || pair->rxq == nullptr || pair->txq == nullptr) {
        net_log::error("virtio NAPI poll for unknown queue pair napi=%p", napi);
        ker::net::napi_complete(napi);
        return 0;
    }
    int processed = 0;

    // Reclaim TX buffers early to return PacketBuffers to the pool.
    {
        NET_TRACE_SPAN(SPAN_TX_RECLAIM);
        uint64_t const TXFLAGS = pair->txq->lock.lock_irqsave();
        auto* reclaimed = collect_tx_for(pair->txq);
        pair->txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
    }

    {
        NET_TRACE_SPAN(SPAN_RX_BUDGET);
        processed = process_rx_budget_for(dev, pair->rxq, budget);
    }

    // If MQ activation timed out, the device may still process the control
    // command late and start routing RX packets to the extra queues we already
    // configured.  Pair 0 owns emergency draining for those inactive queues so
    // packets do not strand forever without an enabled NAPI worker.
    if (pair->index == 0 && dev->configured_queue_pairs > dev->num_queue_pairs) {
        for (uint8_t i = dev->num_queue_pairs; i < dev->configured_queue_pairs && processed < budget; i++) {
            auto* rxq = dev->queue_pairs.at(i).rxq;
            if (rxq != nullptr) {
                processed += process_rx_budget_for(dev, rxq, budget - processed);
            }
        }
    }

    if (processed < budget) {
        ker::net::napi_complete(napi);
        virtio_net_irq_enable_pair(dev, pair->index);

        // Handle missed notification races across IRQ re-enable.
        bool has_pending = virtq_has_pending(pair->rxq);
        if (pair->index == 0 && !has_pending && dev->configured_queue_pairs > dev->num_queue_pairs) {
            for (uint8_t i = dev->num_queue_pairs; i < dev->configured_queue_pairs; i++) {
                auto* rxq = dev->queue_pairs.at(i).rxq;
                if (rxq != nullptr && virtq_has_pending(rxq)) {
                    has_pending = true;
                    break;
                }
            }
        }
        if (has_pending) {
            virtio_net_irq_disable_pair(dev, pair->index);
            ker::net::napi_schedule(napi);
        }
    }

    return processed;
}

void virtio_net_irq(uint8_t vector, void* private_data) {
    (void)vector;

    auto* pair = static_cast<VirtIONetQueuePair*>(private_data);
    if (pair == nullptr || pair->dev == nullptr) {
        return;
    }
    auto* dev = pair->dev;

    if (!dev->msix_enabled && pair->index == 0) {
        uint8_t const ISR_STATUS = ::inb(dev->io_base + VIRTIO_REG_ISR_STATUS);
        if (ISR_STATUS == 0) {
            return;
        }
    }

    // Always defer packet processing to NAPI worker context.
    virtio_net_irq_disable_pair(dev, pair->index);
    ker::net::napi_schedule(&pair->napi);
}

// Send VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET over the control queue to activate the
// requested number of queue pairs.  Busy-polls the used ring for the ACK.  Must
// be called after VIRTIO_STATUS_DRIVER_OK is written to the device.  Returns
// true on success.
auto send_mq_ctrl_cmd(VirtIONetDevice* dev, uint16_t queue_pairs) -> bool {
    if (dev->ctrlq == nullptr) {
        return false;
    }

    // Allocate a DMA-accessible page: [class][cmd][pairs_lo][pairs_hi][ack]
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::phys::page_alloc(4096));
    if (buf == nullptr) {
        return false;
    }
    buf[0] = VIRTIO_NET_CTRL_CLASS_MQ;
    buf[1] = VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET;
    buf[2] = static_cast<uint8_t>(queue_pairs & 0xFFU);
    buf[3] = static_cast<uint8_t>((queue_pairs >> 8U) & 0xFFU);
    buf[4] = 0xFF;  // ack placeholder - device writes VIRTIO_NET_OK (0) on success

    uint64_t const PHYS = virt_to_phys(buf);

    // Chain three descriptors: [header(2B) | data(2B) | ack(1B,writable)]
    auto* ctrlq = dev->ctrlq;
    ctrlq->desc[0].addr = PHYS;
    ctrlq->desc[0].len = 2;
    ctrlq->desc[0].flags = VRING_DESC_F_NEXT;
    ctrlq->desc[0].next = 1;
    ctrlq->desc[1].addr = PHYS + 2;
    ctrlq->desc[1].len = 2;
    ctrlq->desc[1].flags = VRING_DESC_F_NEXT;
    ctrlq->desc[1].next = 2;
    ctrlq->desc[2].addr = PHYS + 4;
    ctrlq->desc[2].len = 1;
    ctrlq->desc[2].flags = VRING_DESC_F_WRITE;
    ctrlq->desc[2].next = 0;

    ctrlq->avail->ring[0] = 0;  // descriptor chain head
    __atomic_thread_fence(__ATOMIC_RELEASE);
    ctrlq->avail->idx = 1;

    virtq_kick(ctrlq);
    // Use TSC (rdtsc, no VM-exits) so the tight spin does not cause a flood of
    // MMIO VM-exits that would starve the QEMU vhost thread we are waiting on.
    // HPET reads would cause VM-exits on every iteration and prevent QEMU from
    // processing our ctrl-queue kick, creating a self-inflicted timeout.
    // 5 s covers any host scheduler latency during early boot.
    constexpr uint64_t CTRL_ACK_TIMEOUT_MS = 5000;
    uint64_t const DEADLINE_MS = ker::mod::time::get_ms() + CTRL_ACK_TIMEOUT_MS;
    while (ker::mod::time::get_ms() < DEADLINE_MS) {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        if (ctrlq->used->idx != 0) {
            return buf[4] == VIRTIO_NET_OK;  // buf leaked intentionally (init-time, 1 page)
        }
        asm volatile("pause" ::: "memory");
    }
    net_log::warn("ctrl-queue MQ ack timeout");
    return false;
}

auto virtio_net_open(ker::net::NetDevice* netdev) -> int {
    netdev->state = 1;
    return 0;
}

void virtio_net_close(ker::net::NetDevice* netdev) { netdev->state = 0; }

enum class TxAttemptResult : uint8_t {
    SENT,
    FULL,
    FAILED,
};

auto virtio_net_try_xmit_pair(ker::net::NetDevice* netdev, VirtIONetDevice* dev, ker::net::PacketBuffer* pkt, uint8_t pair_idx)
    -> TxAttemptResult {
    auto* txq = dev->queue_pairs.at(pair_idx).txq;
    if (txq == nullptr) {
        return TxAttemptResult::FULL;
    }

    uint64_t const TXFLAGS = txq->lock.lock_irqsave();

    // Reap completed TX buffers opportunistically on every send. This keeps
    // TX descriptors available even if completion interrupts are delayed.
    ker::net::PacketBuffer* reclaimed = collect_tx_for(txq);
    if (txq->num_free == 0) {
        virtq_kick(txq);
        txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
        return TxAttemptResult::FULL;
    }

    auto* hdr = reinterpret_cast<VirtIONetHeader*>(pkt->push(dev->hdr_size));
    std::memset(hdr, 0, dev->hdr_size);

    uint64_t const PHYS = virt_to_phys(pkt->data);
    auto total_len = static_cast<uint32_t>(pkt->len);

    int const RET = virtq_add_buf(txq, PHYS, total_len, 0, pkt);
    if (RET < 0) {
        txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
        return TxAttemptResult::FAILED;
    }

    virtq_kick(txq);
    netdev->tx_packets++;
    netdev->tx_bytes += total_len - dev->hdr_size;

    txq->lock.unlock_irqrestore(TXFLAGS);
    free_pkt_list(reclaimed);
    return TxAttemptResult::SENT;
}

auto virtio_net_try_xmit_active_pairs(ker::net::NetDevice* netdev, VirtIONetDevice* dev, ker::net::PacketBuffer* pkt, uint8_t start_pair)
    -> TxAttemptResult {
    uint8_t const PAIR_COUNT = active_tx_pair_count(dev);
    if (PAIR_COUNT == 0) {
        return TxAttemptResult::FAILED;
    }

    for (uint8_t offset = 0; offset < PAIR_COUNT; offset++) {
        auto const PAIR = static_cast<uint8_t>((start_pair + offset) % PAIR_COUNT);
        TxAttemptResult const RESULT = virtio_net_try_xmit_pair(netdev, dev, pkt, PAIR);
        if (RESULT != TxAttemptResult::FULL) {
            return RESULT;
        }
    }

    return TxAttemptResult::FULL;
}

auto virtio_net_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) -> int {
    NET_TRACE_SPAN(SPAN_START_XMIT);
    if (pkt == nullptr) {
        return -1;
    }
    if (netdev == nullptr) {
        ker::net::pkt_free(pkt);
        return -1;
    }
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    if (dev == nullptr) {
        ker::net::pkt_free(pkt);
        return -1;
    }
    uint8_t const PAIR_COUNT = active_tx_pair_count(dev);
    if (PAIR_COUNT == 0) {
        ker::net::pkt_free(pkt);
        return -1;
    }

    bool const WKI_CONTROL = is_wki_control_liveness_frame(pkt);
    auto start_pair = static_cast<uint8_t>(dev->tx_rr.fetch_add(1, std::memory_order_relaxed) % PAIR_COUNT);
    TxAttemptResult result = virtio_net_try_xmit_active_pairs(netdev, dev, pkt, start_pair);
    if (result == TxAttemptResult::SENT) {
        return 0;
    }
    if (result == TxAttemptResult::FAILED) {
        netdev->tx_dropped++;
        ker::net::pkt_free(pkt);
        return -1;
    }

    if (WKI_CONTROL) {
        for (uint8_t retry = 0; retry < WKI_CONTROL_TX_RETRY_POLLS; retry++) {
            ker::net::napi_poll_all_pending();
            start_pair = static_cast<uint8_t>(dev->tx_rr.fetch_add(1, std::memory_order_relaxed) % PAIR_COUNT);
            result = virtio_net_try_xmit_active_pairs(netdev, dev, pkt, start_pair);
            if (result == TxAttemptResult::SENT) {
                return 0;
            }
            if (result == TxAttemptResult::FAILED) {
                netdev->tx_dropped++;
                ker::net::pkt_free(pkt);
                return -1;
            }
            asm volatile("pause" ::: "memory");
        }
    }

    netdev->tx_dropped++;
    net_log::warn("TX ring full: dev=%s drop #%lu pool_free=%zu pairs=%u wki_ctrl=%u", netdev->name.data(),
                  static_cast<unsigned long>(netdev->tx_dropped), ker::net::pkt_pool_free_count(), PAIR_COUNT, WKI_CONTROL ? 1U : 0U);
    ker::net::pkt_free(pkt);
    return -1;
}

auto virtio_net_set_queue_cpu(ker::net::NetDevice* net, uint32_t pair_idx, uint64_t cpu) -> int {
    auto* dev = static_cast<VirtIONetDevice*>(net->private_data);
    if (pair_idx >= dev->num_queue_pairs) {
        return -EINVAL;
    }
    auto& pair = dev->queue_pairs.at(pair_idx);
    ker::net::napi_set_worker_cpu(&pair.napi, cpu);
    if (dev->msix_enabled && pair.irq_vector != 0) {
        ker::dev::pci::pci_configure_msix_entry(dev->pci, pair_idx, pair.irq_vector, cpu);
    }
    return 0;
}

void virtio_net_set_mac(ker::net::NetDevice* netdev, const uint8_t* mac) {
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    if (dev == nullptr || mac == nullptr) {
        return;
    }

    if ((dev->negotiated_features & VIRTIO_NET_F_MAC) == 0) {
        return;
    }

    uint16_t const MAC_OFF = dev->msix_enabled ? VIRTIO_NET_CFG_MAC_MSIX : VIRTIO_NET_CFG_MAC;
    for (size_t i = 0; i < netdev->mac.size(); i++) {
        ::outb(dev->io_base + MAC_OFF + static_cast<uint16_t>(i), mac[i]);
        netdev->mac.at(i) = mac[i];
    }
}

void write_status(uint16_t io_base, uint8_t status) { ::outb(io_base + VIRTIO_REG_DEVICE_STATUS, status); }

ker::net::NetDeviceOps virtio_net_ops = {
    .open = virtio_net_open,
    .close = virtio_net_close,
    .start_xmit = virtio_net_start_xmit,
    .set_mac = virtio_net_set_mac,
    .set_queue_cpu = virtio_net_set_queue_cpu,
};

// Walk the PCI vendor capability list (cap_id=9) for a virtio cfg_type entry.
auto virtio_find_cap(ker::dev::pci::PCIDevice* dev, uint8_t cfg_type) -> uint8_t {
    uint8_t off = ker::dev::pci::pci_config_read8(dev->bus, dev->slot, dev->function, ker::dev::pci::PCI_CAP_PTR);
    for (int limit = 48; off >= 0x40 && limit-- > 0;) {
        if (ker::dev::pci::pci_config_read8(dev->bus, dev->slot, dev->function, off) == VIRTIO_PCI_CAP_VNDR) {
            if (ker::dev::pci::pci_config_read8(dev->bus, dev->slot, dev->function, static_cast<uint8_t>(off + 3)) == cfg_type) {
                return off;
            }
        }
        off = ker::dev::pci::pci_config_read8(dev->bus, dev->slot, dev->function, static_cast<uint8_t>(off + 1));
    }
    return 0;
}

// Map a virtio PCI cap's BAR region: reads bar+offset from the cap header,
// maps the BAR, and returns (volatile uint8_t*)bar_va + offset.
auto map_virtio_cap_region(ker::dev::pci::PCIDevice* dev, uint8_t cap_off) -> uint8_t* {
    auto bar_idx = static_cast<int>(ker::dev::pci::pci_config_read8(dev->bus, dev->slot, dev->function, static_cast<uint8_t>(cap_off + 4)));
    uint32_t const OFFSET = ker::dev::pci::pci_config_read32(dev->bus, dev->slot, dev->function, static_cast<uint8_t>(cap_off + 8));
    auto* bar = ker::dev::pci::pci_map_bar(dev, bar_idx);
    if (bar == nullptr) {
        return nullptr;
    }
    return static_cast<uint8_t*>(bar) + OFFSET;
}

// Modern virtio init (virtio 1.0).  Returns 0 on success, -1 if modern caps
// are absent or the device rejects VERSION_1 (caller falls back to legacy).
auto init_device_modern(ker::dev::pci::PCIDevice* pci_dev) -> int {
    uint8_t const COMMON_CAP = virtio_find_cap(pci_dev, VIRTIO_PCI_CAP_COMMON_CFG);
    uint8_t const NOTIFY_CAP = virtio_find_cap(pci_dev, VIRTIO_PCI_CAP_NOTIFY_CFG);
    if (COMMON_CAP == 0 || NOTIFY_CAP == 0) {
        return -1;
    }

    auto* common_va = map_virtio_cap_region(pci_dev, COMMON_CAP);
    if (common_va == nullptr) {
        return -1;
    }
    auto* cfg = reinterpret_cast<VirtioModernCfg*>(common_va);

    cfg->device_feature_select = 1;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    if ((cfg->device_feature & VIRTIO_F_VERSION_1) == 0) {
        return -1;
    }

    uint32_t notify_off_mult =
        ker::dev::pci::pci_config_read32(pci_dev->bus, pci_dev->slot, pci_dev->function, static_cast<uint8_t>(NOTIFY_CAP + 16));
    auto* notify_va = map_virtio_cap_region(pci_dev, NOTIFY_CAP);
    if (notify_va == nullptr) {
        return -1;
    }

    uint8_t const DEVCFG_CAP = virtio_find_cap(pci_dev, VIRTIO_PCI_CAP_DEVICE_CFG);

    ker::dev::pci::pci_enable_memory_space(pci_dev);
    ker::dev::pci::pci_enable_bus_master(pci_dev);

    volatile uint8_t* devcfg_va = (DEVCFG_CAP != 0) ? map_virtio_cap_region(pci_dev, DEVCFG_CAP) : nullptr;

    auto* dev = new (std::nothrow) VirtIONetDevice{};
    if (dev == nullptr) {
        return -1;
    }
    init_queue_pair_contexts(dev);
    dev->pci = pci_dev;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): PCI BAR0 is fixed config-space layout.
    dev->io_base = static_cast<uint16_t>(pci_dev->bar[0] & ~0x3U);
    dev->hdr_size = static_cast<uint8_t>(VIRTIO_NET_HDR_SIZE);
    dev->modern_cfg = cfg;
    dev->notify_base = notify_va;
    dev->notify_off_multiplier = notify_off_mult;
    dev->device_cfg_base = devcfg_va;

    uint64_t const CORE_COUNT = ker::mod::smt::get_core_count();

    // Reset
    cfg->device_status = 0;
    for (int i = 0; i < 1000 && cfg->device_status != 0; i++) {
        asm volatile("pause");
    }
    cfg->device_status = VIRTIO_STATUS_ACKNOWLEDGE;
    cfg->device_status = static_cast<uint8_t>(VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER);

    cfg->device_feature_select = 0;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    uint32_t const FEAT_LO = cfg->device_feature;

    uint32_t our_lo = 0;
    if ((FEAT_LO & VIRTIO_NET_F_MAC) != 0) {
        our_lo |= VIRTIO_NET_F_MAC;
    }
    if ((FEAT_LO & VIRTIO_NET_F_STATUS) != 0) {
        our_lo |= VIRTIO_NET_F_STATUS;
    }
    if ((FEAT_LO & VIRTIO_NET_F_MRG_RXBUF) != 0) {
        our_lo |= VIRTIO_NET_F_MRG_RXBUF;
    }
    bool want_mq = (CORE_COUNT >= 2) && ((FEAT_LO & VIRTIO_NET_F_CTRL_VQ) != 0) && ((FEAT_LO & VIRTIO_NET_F_MQ) != 0);
    if (want_mq) {
        our_lo |= VIRTIO_NET_F_CTRL_VQ | VIRTIO_NET_F_MQ;
    }

    cfg->driver_feature_select = 0;
    cfg->driver_feature = our_lo;
    cfg->driver_feature_select = 1;
    cfg->driver_feature = VIRTIO_F_VERSION_1;

    cfg->device_status = static_cast<uint8_t>(VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_FEATURES_OK);
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    if ((cfg->device_status & VIRTIO_STATUS_FEATURES_OK) == 0) {
        net_log::error("modern FEATURES_OK rejected");
        cfg->device_status = VIRTIO_STATUS_FAILED;
        delete dev;
        return -1;
    }
    dev->negotiated_features = our_lo;
    dev->hdr_size = virtio_net_hdr_size_for_features(our_lo);

    // Set up one queue: select, size, alloc, assign addresses, notify addr, enable.
    auto setup_queue = [&](uint16_t q_idx, uint16_t max_size) -> Virtqueue* {
        cfg->queue_select = q_idx;
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        uint16_t const HW_SIZE = cfg->queue_size;
        if (HW_SIZE == 0) {
            return nullptr;
        }
        uint16_t const SIZE = std::min({HW_SIZE, max_size, VIRTQ_MAX_SIZE});
        cfg->queue_size = SIZE;

        auto* vq = virtq_alloc(SIZE);
        if (vq == nullptr) {
            return nullptr;
        }
        vq->queue_index = q_idx;
        uint16_t const NOTIFY_OFF = cfg->queue_notify_off;
        vq->notify_addr = reinterpret_cast<volatile uint16_t*>(notify_va + (static_cast<size_t>(NOTIFY_OFF) * notify_off_mult));

        cfg->queue_desc = virt_to_phys(vq->desc);
        cfg->queue_avail = virt_to_phys(vq->avail);
        cfg->queue_used = virt_to_phys(vq->used);
        cfg->queue_enable = 1;
        return vq;
    };

    auto& pair0 = dev->queue_pairs.at(0);
    pair0.rxq = setup_queue(rx_queue_index(0), VIRTQ_MAX_SIZE);
    if (pair0.rxq == nullptr) {
        net_log::error("modern RX queue failed");
        cfg->device_status = VIRTIO_STATUS_FAILED;
        delete dev;
        return -1;
    }
    pair0.txq = setup_queue(tx_queue_index(0), VIRTQ_MAX_SIZE);
    if (pair0.txq == nullptr) {
        net_log::error("modern TX queue failed");
        cfg->device_status = VIRTIO_STATUS_FAILED;
        delete dev;
        return -1;
    }

    uint8_t target_queue_pairs = SINGLE_QUEUE_PAIRS;
    uint16_t ctrlq_idx = 0;
    if (want_mq) {
        uint16_t max_vq_pairs = 1;
        if (devcfg_va != nullptr) {
            max_vq_pairs = *reinterpret_cast<volatile const uint16_t*>(devcfg_va + 8);
            if (max_vq_pairs >= MIN_MQ_QUEUE_PAIRS) {
                ctrlq_idx = static_cast<uint16_t>(2U * max_vq_pairs);
            } else {
                want_mq = false;
            }
        } else {
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            uint16_t const NQ = cfg->num_queues;
            ctrlq_idx = (NQ >= 5) ? static_cast<uint16_t>(NQ - 1U) : uint16_t{0};
            max_vq_pairs = (ctrlq_idx != 0) ? static_cast<uint16_t>(ctrlq_idx / 2U) : uint16_t{1};
            if (ctrlq_idx == 0) {
                want_mq = false;
            }
        }
        if (want_mq) {
            target_queue_pairs = desired_queue_pairs(max_vq_pairs, CORE_COUNT);
            want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
        }
    }
    if (want_mq) {
        uint8_t configured_pairs = SINGLE_QUEUE_PAIRS;
        for (uint8_t pair_idx = 1; pair_idx < target_queue_pairs; pair_idx++) {
            auto& pair = dev->queue_pairs.at(pair_idx);
            pair.rxq = setup_queue(rx_queue_index(pair_idx), VIRTQ_MAX_SIZE);
            pair.txq = setup_queue(tx_queue_index(pair_idx), VIRTQ_MAX_SIZE);
            if (pair.rxq == nullptr || pair.txq == nullptr) {
                net_log::warn("modern queue pair %u setup failed, using %u pair(s)", pair_idx, configured_pairs);
                break;
            }
            configured_pairs++;
        }
        target_queue_pairs = configured_pairs;
        want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
    }
    if (want_mq) {
        dev->ctrlq = setup_queue(ctrlq_idx, 32);
        if (dev->ctrlq == nullptr) {
            want_mq = false;
        }
    }

    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector == 0) {
        net_log::error("no free IRQ vector");
        cfg->device_status = VIRTIO_STATUS_FAILED;
        delete dev;
        return -1;
    }
    pair0.irq_vector = vector;

    if (want_mq) {
        for (uint8_t pair_idx = 1; pair_idx < target_queue_pairs; pair_idx++) {
            auto& pair = dev->queue_pairs.at(pair_idx);
            uint8_t const PAIR_VECTOR = ker::mod::gates::allocate_vector();
            if (PAIR_VECTOR == 0) {
                net_log::warn("no IRQ vector for queue pair %u, using %u pair(s)", pair_idx, pair_idx);
                target_queue_pairs = pair_idx;
                break;
            }
            pair.irq_vector = PAIR_VECTOR;
        }
        want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
    }

    int const MSIX_RET = ker::dev::pci::pci_enable_msix(pci_dev, vector, net_cpu_for_pair(CORE_COUNT, 0));
    if (MSIX_RET == 0) {
        dev->msix_enabled = true;
        if (want_mq) {
            for (uint8_t pair_idx = 1; pair_idx < target_queue_pairs; pair_idx++) {
                auto& pair = dev->queue_pairs.at(pair_idx);
                if (ker::dev::pci::pci_configure_msix_entry(pci_dev, pair_idx, pair.irq_vector, net_cpu_for_pair(CORE_COUNT, pair_idx)) !=
                    0) {
                    net_log::warn("MSI-X entry %u failed, using %u pair(s)", pair_idx, pair_idx);
                    target_queue_pairs = pair_idx;
                    break;
                }
            }
            want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
        }
    }

    if (dev->msix_enabled) {
        cfg->config_msix_vector = VIRTIO_MSI_NO_VECTOR;

        auto assign_vec = [&](uint16_t q, uint16_t entry) -> bool {
            cfg->queue_select = q;
            cfg->queue_msix_vector = entry;
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            return cfg->queue_msix_vector != VIRTIO_MSI_NO_VECTOR;
        };

        for (uint8_t pair_idx = 0; pair_idx < target_queue_pairs; pair_idx++) {
            if (!assign_vec(rx_queue_index(pair_idx), pair_idx)) {
                net_log::warn("MSI-X RX%u vector rejected", pair_idx);
                if (pair_idx == 0) {
                    dev->msix_enabled = false;
                } else {
                    target_queue_pairs = pair_idx;
                    want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
                }
                break;
            }
            if (!assign_vec(tx_queue_index(pair_idx), pair_idx)) {
                net_log::warn("MSI-X TX%u vector rejected", pair_idx);
                if (pair_idx == 0) {
                    dev->msix_enabled = false;
                } else {
                    target_queue_pairs = pair_idx;
                    want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
                }
                break;
            }
        }
        if (dev->ctrlq != nullptr) {
            cfg->queue_select = ctrlq_idx;
            cfg->queue_msix_vector = VIRTIO_MSI_NO_VECTOR;
        }
    }

    if (!dev->msix_enabled) {
        want_mq = false;
        int const MSI_RET = ker::dev::pci::pci_enable_msi(pci_dev, vector, net_cpu_for_pair(CORE_COUNT, 0));
        if (MSI_RET != 0) {
            vector = pci_dev->interrupt_line + 32;
            pair0.irq_vector = vector;
            ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
        }
    }

    dev->configured_queue_pairs = (want_mq && dev->msix_enabled) ? target_queue_pairs : SINGLE_QUEUE_PAIRS;
    dev->num_queue_pairs = dev->configured_queue_pairs;

    if ((our_lo & VIRTIO_NET_F_MAC) != 0 && devcfg_va != nullptr) {
        for (size_t i = 0; i < ker::net::proto::MacAddress::SIZE_BYTES; i++) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): virtio device config is MMIO bytes.
            dev->netdev.mac.at(i) = devcfg_va[i];
        }
    }

    for (uint8_t pair_idx = 0; pair_idx < dev->num_queue_pairs; pair_idx++) {
        auto& pair = dev->queue_pairs.at(pair_idx);
        if (pair.irq_vector != 0) {
            ker::mod::gates::request_irq(pair.irq_vector, virtio_net_irq, &pair, pair_idx == 0 ? "virtio-net" : "virtio-net-mq");
        }
    }

    cfg->device_status =
        static_cast<uint8_t>(VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_FEATURES_OK | VIRTIO_STATUS_DRIVER_OK);

    fill_rx_queue_for(dev, pair0.rxq);
    for (uint8_t pair_idx = 1; pair_idx < dev->configured_queue_pairs; pair_idx++) {
        auto* rxq = dev->queue_pairs.at(pair_idx).rxq;
        if (rxq != nullptr) {
            fill_rx_queue_for(dev, rxq);
        }
    }
    if (dev->num_queue_pairs >= MIN_MQ_QUEUE_PAIRS) {
        if (!send_mq_ctrl_cmd(dev, dev->num_queue_pairs)) {
            net_log::warn("MQ activation failed, downgrading to single-queue");
            for (uint8_t pair_idx = 1; pair_idx < dev->num_queue_pairs; pair_idx++) {
                virtio_net_irq_disable_pair(dev, pair_idx);
            }
            dev->num_queue_pairs = SINGLE_QUEUE_PAIRS;
        }
    }

    dev->netdev.ops = &virtio_net_ops;
    dev->netdev.mtu = 9000;
    dev->netdev.state = 1;
    dev->netdev.private_data = dev;
    dev->netdev.name.at(0) = '\0';
    dev->netdev.remotable = &S_REMOTABLE_OPS;

    ker::net::netdev_register(&dev->netdev);
    for (uint8_t pair_idx = 0; pair_idx < dev->num_queue_pairs; pair_idx++) {
        auto& pair = dev->queue_pairs.at(pair_idx);
        ker::net::napi_init(&pair.napi, &dev->netdev, virtio_net_poll, 64);
        ker::net::napi_enable(&pair.napi, net_cpu_for_pair(CORE_COUNT, pair_idx));

        // Re-arm MSI-X vectors in case an interrupt fired between requestIrq
        // and napi_enable (e.g. during the ctrl-queue spin).  virtio_net_irq
        // disables the pair on entry but napi_schedule silently fails while
        // NAPI is DISABLED, so vectors stay at NO_VECTOR unless restored here.
        virtio_net_irq_enable_pair(dev, pair_idx);
    }

    devices.at(device_count++) = dev;

    net_log::info("%s MAC=%02x:%02x:%02x:%02x:%02x:%02x vec=0x%02x%s pairs=%u hdr=%u mrg=%u napi ready", dev->netdev.name.data(),
                  dev->netdev.mac.at(0), dev->netdev.mac.at(1), dev->netdev.mac.at(2), dev->netdev.mac.at(3), dev->netdev.mac.at(4),
                  dev->netdev.mac.at(5), vector, dev->msix_enabled ? " modern msix" : " modern msi/intx", dev->num_queue_pairs,
                  dev->hdr_size, (dev->negotiated_features & VIRTIO_NET_F_MRG_RXBUF) != 0 ? 1U : 0U);

    return 0;
}

auto init_device(ker::dev::pci::PCIDevice* pci_dev) -> int {
    if (device_count >= MAX_VIRTIO_NET_DEVICES) {
        return -1;
    }

    if (init_device_modern(pci_dev) == 0) {
        return 0;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): PCI BAR0 is fixed config-space layout.
    auto io_base = static_cast<uint16_t>(pci_dev->bar[0] & ~0x3U);
    if (io_base == 0) {
        net_log::error("BAR0 is zero");
        return -1;
    }

    ker::dev::pci::pci_enable_bus_master(pci_dev);
    uint16_t cmd = ker::dev::pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND);
    cmd |= ker::dev::pci::PCI_COMMAND_IO_SPACE;
    ker::dev::pci::pci_config_write16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND, cmd);

    write_status(io_base, 0);

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE);

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER);

    uint32_t const DEV_FEATURES = ::inl(io_base + VIRTIO_REG_DEVICE_FEATURES);
    uint64_t const CORE_COUNT = ker::mod::smt::get_core_count();

    // Negotiate features: MAC, status, and mergeable RX header support.
    uint32_t our_features = 0;
    if ((DEV_FEATURES & VIRTIO_NET_F_MAC) != 0) {
        our_features |= VIRTIO_NET_F_MAC;
    }
    if ((DEV_FEATURES & VIRTIO_NET_F_STATUS) != 0) {
        our_features |= VIRTIO_NET_F_STATUS;
    }
    if ((DEV_FEATURES & VIRTIO_NET_F_MRG_RXBUF) != 0) {
        our_features |= VIRTIO_NET_F_MRG_RXBUF;
    }
    ::outl(io_base + VIRTIO_REG_GUEST_FEATURES, our_features);

    auto* dev = new (std::nothrow) VirtIONetDevice{};
    if (dev == nullptr) {
        write_status(io_base, VIRTIO_STATUS_FAILED);
        return -1;
    }
    init_queue_pair_contexts(dev);
    dev->pci = pci_dev;
    dev->io_base = io_base;
    dev->hdr_size = virtio_net_hdr_size_for_features(our_features);
    dev->negotiated_features = our_features;
    auto& pair0 = dev->queue_pairs.at(0);

    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
    uint16_t rxq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (rxq_size == 0) {
        net_log::error("RX queue size is 0");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        delete dev;
        return -1;
    }
    rxq_size = std::min(rxq_size, VIRTQ_MAX_SIZE);

    pair0.rxq = virtq_alloc(rxq_size);
    if (pair0.rxq == nullptr) {
        net_log::error("failed to alloc RX queue");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        delete dev;
        return -1;
    }
    pair0.rxq->io_base = io_base;
    pair0.rxq->queue_index = rx_queue_index(0);

    uint64_t const RXQ_PHYS = virt_to_phys(pair0.rxq->desc);
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(RXQ_PHYS / 4096));

    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
    uint16_t txq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (txq_size == 0) {
        net_log::error("TX queue size is 0");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        delete dev;
        return -1;
    }
    txq_size = std::min(txq_size, VIRTQ_MAX_SIZE);

    pair0.txq = virtq_alloc(txq_size);
    if (pair0.txq == nullptr) {
        net_log::error("failed to alloc TX queue");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        delete dev;
        return -1;
    }
    pair0.txq->io_base = io_base;
    pair0.txq->queue_index = tx_queue_index(0);

    uint64_t const TXQ_PHYS = virt_to_phys(pair0.txq->desc);
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(TXQ_PHYS / 4096));

    // Try MSI-X first (needed for per-pair CPU steering), then MSI, then INTx.
    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector == 0) {
        net_log::error("no free IRQ vector");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        delete dev;
        return -1;
    }
    pair0.irq_vector = vector;

    int const MSIX_RET = ker::dev::pci::pci_enable_msix(pci_dev, vector, net_cpu_for_pair(CORE_COUNT, 0));
    if (MSIX_RET == 0) {
        dev->msix_enabled = true;

        // Config vector: disabled (we don't handle config-change interrupts).
        ::outw(io_base + VIRTIO_MSI_CONFIG_VECTOR, VIRTIO_MSI_NO_VECTOR);
    }

    // Assign MSI-X entry indices to individual queues.
    if (dev->msix_enabled) {
        // Pair 0: queues 0 (RX) and 1 (TX) -> entry 0
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            net_log::warn("MSI-X RX0 vector rejected");
            dev->msix_enabled = false;
        }
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            net_log::warn("MSI-X TX0 vector rejected");
            dev->msix_enabled = false;
        }
    }

    if (!dev->msix_enabled) {
        int const MSI_RET = ker::dev::pci::pci_enable_msi(pci_dev, vector, net_cpu_for_pair(CORE_COUNT, 0));
        if (MSI_RET != 0) {
            vector = pci_dev->interrupt_line + 32;
            pair0.irq_vector = vector;
            ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
        }
    }

    dev->configured_queue_pairs = SINGLE_QUEUE_PAIRS;
    dev->num_queue_pairs = SINGLE_QUEUE_PAIRS;

    // MAC offset depends on MSI-X vector fields.
    uint16_t const MAC_OFF = dev->msix_enabled ? VIRTIO_NET_CFG_MAC_MSIX : VIRTIO_NET_CFG_MAC;
    if ((our_features & VIRTIO_NET_F_MAC) != 0) {
        for (size_t i = 0; i < ker::net::proto::MacAddress::SIZE_BYTES; i++) {
            dev->netdev.mac.at(i) = ::inb(io_base + MAC_OFF + static_cast<uint16_t>(i));
        }
    }

    for (uint8_t pair_idx = 0; pair_idx < dev->num_queue_pairs; pair_idx++) {
        auto& pair = dev->queue_pairs.at(pair_idx);
        if (pair.irq_vector != 0) {
            ker::mod::gates::request_irq(pair.irq_vector, virtio_net_irq, &pair, pair_idx == 0 ? "virtio-net" : "virtio-net-mq");
        }
    }

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_DRIVER_OK);

    fill_rx_queue_for(dev, pair0.rxq);

    dev->netdev.ops = &virtio_net_ops;
    dev->netdev.mtu = 9000;
    dev->netdev.state = 1;
    dev->netdev.private_data = dev;
    dev->netdev.name.at(0) = '\0';
    dev->netdev.remotable = &S_REMOTABLE_OPS;

    ker::net::netdev_register(&dev->netdev);

    for (uint8_t pair_idx = 0; pair_idx < dev->num_queue_pairs; pair_idx++) {
        auto& pair = dev->queue_pairs.at(pair_idx);
        ker::net::napi_init(&pair.napi, &dev->netdev, virtio_net_poll, 64);
        ker::net::napi_enable(&pair.napi, net_cpu_for_pair(CORE_COUNT, pair_idx));
        virtio_net_irq_enable_pair(dev, pair_idx);
    }

    devices.at(device_count++) = dev;

    net_log::info("%s MAC=%02x:%02x:%02x:%02x:%02x:%02x rxq=%u txq=%u vec=0x%02x%s pairs=%u hdr=%u mrg=%u napi ready",
                  dev->netdev.name.data(), dev->netdev.mac.at(0), dev->netdev.mac.at(1), dev->netdev.mac.at(2), dev->netdev.mac.at(3),
                  dev->netdev.mac.at(4), dev->netdev.mac.at(5), rxq_size, txq_size, vector, dev->msix_enabled ? " msix" : " legacy",
                  dev->num_queue_pairs, dev->hdr_size, (dev->negotiated_features & VIRTIO_NET_F_MRG_RXBUF) != 0 ? 1U : 0U);

    return 0;
}
}  // namespace

auto virtio_net_diag_snapshot(VirtIONetDiagSnapshot* out, size_t max) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }

    size_t count = 0;
    for (size_t dev_idx = 0; dev_idx < device_count && count < max; ++dev_idx) {
        auto* dev = devices.at(dev_idx);
        if (dev == nullptr) {
            continue;
        }

        uint8_t const CONFIGURED = std::min<uint8_t>(dev->configured_queue_pairs, VIRTIO_NET_MAX_QUEUE_PAIRS);
        for (uint8_t pair_idx = 0; pair_idx < CONFIGURED && count < max; ++pair_idx) {
            auto& pair = dev->queue_pairs.at(pair_idx);
            auto& row = out[count++];
            row.name = dev->netdev.name;
            row.ifindex = dev->netdev.ifindex;
            row.negotiated_features = dev->negotiated_features;
            row.pair = pair_idx;
            row.num_queue_pairs = dev->num_queue_pairs;
            row.configured_queue_pairs = dev->configured_queue_pairs;
            row.hdr_size = dev->hdr_size;
            row.irq_vector = pair.irq_vector;
            row.msix_enabled = dev->msix_enabled;
            row.active = pair_idx < dev->num_queue_pairs;
            row.napi_state = static_cast<uint8_t>(pair.napi.state.load(std::memory_order_acquire));
            row.napi_has_work = pair.napi.has_work.load(std::memory_order_acquire);
            row.napi_polls = pair.napi.poll_count;
            row.napi_completes = pair.napi.complete_count;
            if (pair.napi.worker != nullptr) {
                row.napi_worker_pid = pair.napi.worker->pid;
                row.napi_worker_cpu = pair.napi.worker->cpu;
            }
            fill_virtqueue_diag(pair.rxq, row.rx);
            fill_virtqueue_diag(pair.txq, row.tx);
        }
    }
    return count;
}

auto virtio_net_init() -> int {
    int found = 0;

    size_t const COUNT = ker::dev::pci::pci_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = ker::dev::pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        if (dev->vendor_id != VIRTIO_VENDOR) {
            continue;
        }

        bool is_net = false;
        if (dev->device_id == VIRTIO_NET_LEGACY) {
            if (dev->class_code == ker::dev::pci::PCI_CLASS_NETWORK) {
                is_net = true;
            }
        } else if (dev->device_id == VIRTIO_NET_MODERN) {
            is_net = true;
        }

        if (!is_net) {
            continue;
        }

        net_log::info("found device at PCI %02x:%02x.%x", dev->bus, dev->slot, dev->function);

        if (init_device(dev) == 0) {
            found++;
        }
    }

    if (found == 0) {
        net_log::info("no devices found");
    }

    return found;
}

}  // namespace ker::dev::virtio
