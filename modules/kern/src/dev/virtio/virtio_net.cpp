#include "virtio_net.hpp"

#ifdef WOS_SELFTEST
#include "napi_irq_test.hpp"
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/iommu/domains.hpp>
#include <mod/io/port/port.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/wire.hpp>
#include <new>
#include <platform/acpi/ioapic/ioapic.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
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

class CallbackGuard {
    // Reverse destruction order releases the driver reference first. Capturing
    // the owning task also preserves the hold across migration/nested polling.
    ker::mod::sched::KernelShutdownHold shutdown_{ker::mod::sched::hold_kernel_shutdown()};
    lifecycle::Guard callback_;

   public:
    explicit CallbackGuard(lifecycle::Admission& admission) : callback_(admission) {}
    [[nodiscard]] explicit operator bool() const { return static_cast<bool>(callback_); }
};

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
// The dedicated procfs NAPI view must include every admitted queue pair.
static_assert(MAX_VIRTIO_NET_DEVICES * VIRTIO_NET_MAX_QUEUE_PAIRS <= VIRTIO_NET_DIAG_MAX_ROWS);
std::array<VirtIONetDevice*, MAX_VIRTIO_NET_DEVICES> devices = {};
size_t device_count = 0;
std::atomic<bool> shutdown_started{false};
VirtIONetDevice* failed_probes = nullptr;
auto retire_device(VirtIONetDevice* dev) -> int;
auto init_device(pci::PCIDevice* pci_dev) -> int;
#ifdef WOS_SELFTEST
SelftestProbeResult* probe_test = nullptr;
bool probe_test_withhold_reset = false;
auto selftest_live_rx(VirtIONetDevice* dev, ker::net::PacketBuffer* pkt) -> bool;
void selftest_live_irq(VirtIONetDevice* dev);
void selftest_live_start(VirtIONetDevice* dev);
void selftest_napi_start(VirtIONetDevice* dev);
auto selftest_napi_irq(VirtIONetQueuePair* pair) -> bool;
auto selftest_napi_rx(VirtIONetDevice* dev, Virtqueue* queue, ker::net::PacketBuffer* pkt, ker::net::NapiPollSource source, uint16_t desc)
    -> bool;
bool selftest_live_withhold_reset = false;
std::atomic<uint64_t> fault_override{0};
#endif

constexpr uint8_t SINGLE_QUEUE_PAIRS = 1;
constexpr uint8_t MIN_MQ_QUEUE_PAIRS = 2;
constexpr uint8_t DEFAULT_MQ_QUEUE_PAIR_LIMIT = 4;
static_assert(MAX_VIRTIO_NET_DEVICES * DEFAULT_MQ_QUEUE_PAIR_LIMIT * 2 * VIRTQ_MAX_SIZE + 2048 < dma::REGISTRY_CAPACITY);
constexpr uint8_t WKI_CONTROL_TX_RETRY_POLLS = 8;
constexpr uint8_t RX_EMPTY_PENDING_REARM_LIMIT = 1;

auto future_deadline_ms(uint64_t timeout_ms) -> uint64_t {
    uint64_t const NOW_MS = ker::mod::time::get_ms();
    if (UINT64_MAX - NOW_MS < timeout_ms) {
        return UINT64_MAX;
    }
    return NOW_MS + timeout_ms;
}

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
    uint16_t const DEFAULT_LIMIT = std::min<uint16_t>(DEFAULT_MQ_QUEUE_PAIR_LIMIT, VIRTIO_NET_MAX_QUEUE_PAIRS);
    uint16_t const CAPPED = std::min({device_max_pairs, CPU_LIMIT, DEFAULT_LIMIT});
    if (CAPPED < MIN_MQ_QUEUE_PAIRS) {
        return SINGLE_QUEUE_PAIRS;
    }
    return static_cast<uint8_t>(CAPPED);
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cur = cmdline;
    while (*cur != '\0') {
        while (*cur == ' ' || *cur == '\t' || *cur == '\n') {
            ++cur;
        }
        if (*cur == '\0') {
            break;
        }
        const char* start = cur;
        while (*cur != '\0' && *cur != ' ' && *cur != '\t' && *cur != '\n') {
            ++cur;
        }
        auto const LEN = static_cast<size_t>(cur - start);
        if (LEN == TOKEN_LEN && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto virtio_net_mq_disabled() -> bool {
    static std::atomic<int> cached{-1};
    int const VALUE = cached.load(std::memory_order_acquire);
    if (VALUE >= 0) {
        return VALUE != 0;
    }

    bool const DISABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "virtio_net.no_mq");
    cached.store(DISABLED ? 1 : 0, std::memory_order_release);
    return DISABLED;
}

auto virtio_net_mrg_rxbuf_disabled() -> bool {
    static std::atomic<int> cached{-1};
    int const VALUE = cached.load(std::memory_order_acquire);
    if (VALUE >= 0) {
        return VALUE != 0;
    }

    bool const DISABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "virtio_net.no_mrg_rxbuf");
    cached.store(DISABLED ? 1 : 0, std::memory_order_release);
    return DISABLED;
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

auto mix_tx_hash(uint32_t hash, uint32_t value) -> uint32_t {
    hash ^= value + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
    return hash;
}

auto tx_pair_for_packet(const VirtIONetDevice* dev, const ker::net::PacketBuffer* pkt) -> uint8_t {
    uint8_t const PAIR_COUNT = active_tx_pair_count(dev);
    if (PAIR_COUNT <= 1 || pkt == nullptr || pkt->data == nullptr || pkt->len < ker::net::proto::ETH_HLEN) {
        return 0;
    }

    const auto* eth = reinterpret_cast<const ker::net::proto::EthernetHeader*>(pkt->data);
    uint16_t const ETHERTYPE = ker::net::ntohs(eth->ethertype);
    uint32_t hash = ETHERTYPE;

    if (ETHERTYPE == ker::net::proto::ETH_TYPE_IPV4 && pkt->len >= ker::net::proto::ETH_HLEN + sizeof(ker::net::proto::IPv4Header)) {
        const auto* ip = reinterpret_cast<const ker::net::proto::IPv4Header*>(pkt->data + ker::net::proto::ETH_HLEN);
        uint8_t const IHL = static_cast<uint8_t>(ip->ihl_version & 0x0FU) * 4U;
        hash = ip->src_addr ^ ip->dst_addr ^ static_cast<uint32_t>(ip->protocol);
        if ((ip->protocol == ker::net::proto::IPPROTO_TCP || ip->protocol == ker::net::proto::IPPROTO_UDP) &&
            pkt->len >= ker::net::proto::ETH_HLEN + IHL + 4U) {
            const auto* ports = reinterpret_cast<const uint16_t*>(pkt->data + ker::net::proto::ETH_HLEN + IHL);
            hash ^= (static_cast<uint32_t>(ports[0]) << 16U) | static_cast<uint32_t>(ports[1]);
        }
    } else {
        for (size_t i = 0; i < ker::net::proto::MacAddress::SIZE_BYTES; ++i) {
            hash = mix_tx_hash(hash, static_cast<uint32_t>(eth->src.at(i)));
            hash = mix_tx_hash(hash, static_cast<uint32_t>(eth->dst.at(i)));
        }

        if ((ETHERTYPE == ker::net::proto::ETH_TYPE_WKI || ETHERTYPE == ker::net::proto::ETH_TYPE_WKI_ROCE) &&
            pkt->len >= ker::net::proto::ETH_HLEN + sizeof(ker::net::wki::WkiHeader)) {
            const auto* hdr = reinterpret_cast<const ker::net::wki::WkiHeader*>(pkt->data + ker::net::proto::ETH_HLEN);
            hash = mix_tx_hash(hash, static_cast<uint32_t>(hdr->src_node));
            hash = mix_tx_hash(hash, static_cast<uint32_t>(hdr->dst_node));
            hash = mix_tx_hash(hash, static_cast<uint32_t>(hdr->channel_id));
        }
    }

    hash *= 0x9e3779b9U;
    hash ^= hash >> 16U;
    return static_cast<uint8_t>(hash % PAIR_COUNT);
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

void fill_rx_queue_for(VirtIONetDevice* dev, Virtqueue* rxq) {
    if (rxq->failed.load(std::memory_order_acquire)) return;
    const char* const DEV_NAME = dev != nullptr ? dev->netdev.name.data() : "?";
    if (rxq->num_free > rxq->size) {
        net_log::error("virtio rx refill: dev=%s q=%u corrupt free count free=%u size=%u", DEV_NAME, rxq->queue_index, rxq->num_free,
                       rxq->size);
        rxq->failed.store(true, std::memory_order_release);
        return;
    }

    if (rxq->num_free > 0) {
        size_t const TARGET_FREE = static_cast<size_t>(rxq->num_free) + ker::net::PKT_POOL_RX_REFILL_RESERVE;
        ker::net::pkt_pool_ensure_free(TARGET_FREE);
    }

    size_t const BEFORE_FREE = ker::net::pkt_pool_free_count();
    uint16_t filled = 0;
    while (rxq->num_free > 0) {
        if (ker::net::pkt_pool_free_count() <= ker::net::PKT_POOL_RX_REFILL_RESERVE) {
            break;
        }
        auto* pkt = ker::net::pkt_alloc_rx();
        if (pkt == nullptr) {
            break;
        }
        pkt->data = pkt->storage.data();
        pkt->len = 0;

        auto buffer = std::span<std::byte>(reinterpret_cast<std::byte*>(pkt->storage.data()), pkt->storage.size());
        if (virtq_add_buf(rxq, buffer, pkt) < 0) {
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
    if (filled != 0) {
        virtq_kick(rxq);
    }
}

auto process_rx_budget_for(VirtIONetDevice* dev, Virtqueue* rxq, int budget,
                           ker::net::NapiPollSource source = ker::net::NapiPollSource::INLINE) -> int {
#ifndef WOS_SELFTEST
    (void)source;
#endif
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

#ifdef WOS_SELFTEST
        if (selftest_napi_rx(dev, rxq, pkt, source, desc_idx)) continue;
        if (selftest_live_rx(dev, pkt)) continue;
#endif
        ker::net::netdev_rx(&dev->netdev, pkt);
    }

    fill_rx_queue_for(dev, rxq);

    return processed;
}

// Collect completed TX buffers from a specific queue; caller frees after releasing lock.
auto collect_tx_for(Virtqueue* txq, uint64_t* reclaimed_count = nullptr) -> ker::net::PacketBuffer* {
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
            if (reclaimed_count != nullptr) ++*reclaimed_count;
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

// The available-ring flags are DMA-visible and may be updated concurrently by
// IRQ and poll contexts on different CPUs.  Store the whole aligned flags word
// atomically; EVENT_IDX is not negotiated, so bit 0 is the only active flag.
void virtio_net_set_queue_notifications(Virtqueue* vq, bool enabled) {
    if (vq == nullptr || vq->avail == nullptr) {
        return;
    }

    auto* flags = reinterpret_cast<uint16_t*>(vq->avail);
    enabled = enabled && !vq->failed.load(std::memory_order_acquire);
    uint16_t const VALUE = enabled ? uint16_t{0} : VIRTQ_AVAIL_F_NO_INTERRUPT;
    __atomic_store_n(flags, VALUE, __ATOMIC_RELEASE);
}

void virtio_net_set_pair_notifications(VirtIONetQueuePair* pair, bool enabled) {
    if (pair == nullptr) {
        return;
    }
    virtio_net_set_queue_notifications(pair->rxq, enabled);
    virtio_net_set_queue_notifications(pair->txq, enabled);
}

// Queue-vector mappings remain fixed for active pairs.  This helper is only
// used to permanently detach pairs left inactive by an MQ activation failure.
void virtio_net_unassign_pair_vectors(VirtIONetDevice* dev, uint8_t pair) {
    if (!dev->msix_enabled) {
        return;
    }
    auto const RX_Q = static_cast<uint16_t>(pair * 2U);
    auto const TX_Q = static_cast<uint16_t>((pair * 2U) + 1U);
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
}

struct VirtIONetPending {
    bool rx{};
    bool tx{};
};

auto virtio_net_pair_pending(VirtIONetDevice* dev, VirtIONetQueuePair* pair) -> VirtIONetPending {
    if (dev == nullptr || pair == nullptr || pair->rxq == nullptr) {
        return {};
    }

    VirtIONetPending pending{.rx = virtq_has_pending(pair->rxq)};
    if (pair->txq != nullptr) {
        uint64_t const TX_FLAGS = pair->txq->lock.lock_irqsave();
        pending.tx = virtq_has_pending(pair->txq);
        pair->txq->lock.unlock_irqrestore(TX_FLAGS);
    }
    if (pair->index == 0 && !pending.rx && dev->configured_queue_pairs > dev->num_queue_pairs) {
        for (uint8_t i = dev->num_queue_pairs; i < dev->configured_queue_pairs; i++) {
            auto* rxq = dev->queue_pairs.at(i).rxq;
            if (rxq != nullptr && virtq_has_pending(rxq)) {
                pending.rx = true;
                break;
            }
        }
    }
    return pending;
}

auto should_reschedule_after_notification_rearm(VirtIONetDevice* dev, VirtIONetQueuePair* pair, int rx_processed) -> bool {
    if (pair == nullptr) {
        return false;
    }

    VirtIONetPending const PENDING = virtio_net_pair_pending(dev, pair);
    if (rx_processed != 0) {
        pair->rx_empty_pending_rearms = 0;
    }

    // TX used entries are always consumable under txq->lock.  Reschedule them
    // without the defensive RX no-progress bound so transmit-side reclamation
    // cannot strand a completion posted while notifications were suppressed.
    if (PENDING.tx) {
        return true;
    }
    if (!PENDING.rx) {
        pair->rx_empty_pending_rearms = 0;
        return false;
    }

    if (rx_processed != 0) {
        return true;
    }

    if (pair->rx_empty_pending_rearms < RX_EMPTY_PENDING_REARM_LIMIT) {
        pair->rx_empty_pending_rearms++;
        return true;
    }

    return false;
}

// Caller holds pair->notification_lock.
auto virtio_net_rearm_pair_notifications_locked(VirtIONetDevice* dev, VirtIONetQueuePair* pair, int rx_processed) -> bool {
    virtio_net_set_pair_notifications(pair, true);

    // A full store->load barrier is required between making notifications
    // visible to the device and checking whether it posted work meanwhile.
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    bool const SHOULD_RESCHEDULE = should_reschedule_after_notification_rearm(dev, pair, rx_processed);
    bool const NAPI_IDLE = pair->napi.state.load(std::memory_order_acquire) == ker::net::NapiState::IDLE;

    // A duplicate IRQ or inline rescue poll may already own this pair.  Keep
    // notifications suppressed until that owner completes its own handshake.
    if (SHOULD_RESCHEDULE || !NAPI_IDLE) {
        virtio_net_set_pair_notifications(pair, false);
    }
    return SHOULD_RESCHEDULE;
}

void virtio_net_begin_poll(VirtIONetQueuePair* pair) {
    uint64_t const FLAGS = pair->notification_lock.lock_irqsave();
    virtio_net_set_pair_notifications(pair, false);
    pair->notification_lock.unlock_irqrestore(FLAGS);
}

void virtio_net_complete_poll(VirtIONetDevice* dev, VirtIONetQueuePair* pair, ker::net::NapiStruct* napi, int rx_processed) {
    uint64_t const FLAGS = pair->notification_lock.lock_irqsave();
    ker::net::napi_complete(napi);

    // Handle missed notification races across notification re-enable without
    // letting a stale/non-consumable pending bit spin an idle queue forever.
    bool const SHOULD_RESCHEDULE = virtio_net_rearm_pair_notifications_locked(dev, pair, rx_processed);
    pair->notification_lock.unlock_irqrestore(FLAGS);
    if (SHOULD_RESCHEDULE) {
        ker::net::napi_schedule(napi, ker::net::NapiScheduleSource::REARM);
    }
}

int virtio_net_poll(ker::net::NapiStruct* napi, int budget, ker::net::NapiPollSource source) {
    NET_TRACE_SPAN(SPAN_NAPI_POLL);
    auto* dev = static_cast<VirtIONetDevice*>(napi->dev->private_data);
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) {
        ker::net::napi_complete(napi);
        return 0;
    }
    auto* pair = queue_pair_for_napi(dev, napi);
    if (pair == nullptr || pair->rxq == nullptr || pair->txq == nullptr) {
        net_log::error("virtio NAPI poll for unknown queue pair napi=%p", napi);
        ker::net::napi_complete(napi);
        return 0;
    }
    // An inline rescue poll can claim POLLING while the prior owner is still
    // completing.  This short gate keeps it away from queue state until the
    // prior notification handshake has finished.
    virtio_net_begin_poll(pair);
    int processed = 0;

    // Reclaim TX buffers early to return PacketBuffers to the pool.
    {
        NET_TRACE_SPAN(SPAN_TX_RECLAIM);
        uint64_t reclaimed_count = 0;
        uint64_t const TXFLAGS = pair->txq->lock.lock_irqsave();
        auto* reclaimed = collect_tx_for(pair->txq, napi->diagnostics_enabled ? &reclaimed_count : nullptr);
        pair->txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
        ker::net::napi_diag_tx_reclaimed(napi, source, reclaimed_count);
    }

    {
        NET_TRACE_SPAN(SPAN_RX_BUDGET);
        processed = process_rx_budget_for(dev, pair->rxq, budget, source);
    }

    // If MQ activation timed out, the device may still process the control
    // command late and start routing RX packets to the extra queues we already
    // configured.  Pair 0 owns emergency draining for those inactive queues so
    // packets do not strand forever without an enabled NAPI worker.
    if (pair->index == 0 && dev->configured_queue_pairs > dev->num_queue_pairs) {
        for (uint8_t i = dev->num_queue_pairs; i < dev->configured_queue_pairs && processed < budget; i++) {
            auto* rxq = dev->queue_pairs.at(i).rxq;
            if (rxq != nullptr) {
                processed += process_rx_budget_for(dev, rxq, budget - processed, source);
            }
        }
    }

    if (processed < budget) {
        virtio_net_complete_poll(dev, pair, napi, processed);
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

    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) return;
#ifdef WOS_SELFTEST
    selftest_live_irq(dev);
#endif

    if (!dev->msix_enabled && pair->index == 0) {
        uint8_t const ISR_STATUS = ::inb(dev->io_base + VIRTIO_REG_ISR_STATUS);
        if (ISR_STATUS == 0) {
            return;
        }
    }

    // Always defer packet processing to NAPI worker context.
    uint64_t const FLAGS = pair->notification_lock.lock_irqsave();
    virtio_net_set_pair_notifications(pair, false);
#ifdef WOS_SELFTEST
    bool const SUPPRESSED = selftest_napi_irq(pair);
#endif
    pair->notification_lock.unlock_irqrestore(FLAGS);
#ifdef WOS_SELFTEST
    if (SUPPRESSED) return;
#endif
    ker::net::napi_schedule(&pair->napi, ker::net::NapiScheduleSource::IRQ);
}

// Send VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET over the control queue to activate the
// requested number of queue pairs.  Busy-polls the used ring for the ACK.  Must
// be called after VIRTIO_STATUS_DRIVER_OK is written to the device.  Returns
// true on success.
auto send_mq_ctrl_cmd(VirtIONetDevice* dev, uint16_t queue_pairs) -> bool {
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) return false;
    if (dev->ctrlq == nullptr) {
        return false;
    }

    // Preallocated before BME admission. Keep this coherent page until reset,
    // including an ACK timeout: the device may still process the command late.
    auto* buf = reinterpret_cast<uint8_t*>(dev->control_buffer.allocation.cpu);
    if (buf == nullptr || !dev->control_buffer.expose()) return false;
    buf[0] = VIRTIO_NET_CTRL_CLASS_MQ;
    buf[1] = VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET;
    buf[2] = static_cast<uint8_t>(queue_pairs & 0xFFU);
    buf[3] = static_cast<uint8_t>((queue_pairs >> 8U) & 0xFFU);
    buf[4] = 0xFF;  // ack placeholder - device writes VIRTIO_NET_OK (0) on success

    uint64_t const ADDRESS = dev->control_buffer.submission.address;

    // Chain three descriptors: [header(2B) | data(2B) | ack(1B,writable)]
    auto* ctrlq = dev->ctrlq;
    ctrlq->desc[0].addr = ADDRESS;
    ctrlq->desc[0].len = 2;
    ctrlq->desc[0].flags = VRING_DESC_F_NEXT;
    ctrlq->desc[0].next = 1;
    ctrlq->desc[1].addr = ADDRESS + 2;
    ctrlq->desc[1].len = 2;
    ctrlq->desc[1].flags = VRING_DESC_F_NEXT;
    ctrlq->desc[1].next = 2;
    ctrlq->desc[2].addr = ADDRESS + 4;
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
    uint64_t const DEADLINE_MS = future_deadline_ms(CTRL_ACK_TIMEOUT_MS);
    for (size_t spins = 0; spins < 100'000'000 && ker::mod::time::get_ms() < DEADLINE_MS; ++spins) {
        // The packed ABI understates alignment: used is page-aligned and idx
        // is at byte 2. One aligned x86 load, followed by the DMA acquire fence.
        auto const USED = static_cast<volatile VirtqUsed*>(ctrlq->used)->idx;
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        if (USED != 0) {
            return USED == 1 && ctrlq->used->ring[0].id == 0 && ctrlq->used->ring[0].len == 1 &&
                   *reinterpret_cast<volatile uint8_t*>(buf + 4) == VIRTIO_NET_OK;
        }
        asm volatile("pause" ::: "memory");
    }
    net_log::warn("ctrl-queue MQ ack timeout");
    return false;
}

auto virtio_net_open(ker::net::NetDevice* netdev) -> int {
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) return -ENODEV;
    netdev->state = 1;
    return 0;
}

void virtio_net_close(ker::net::NetDevice* netdev) {
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    CallbackGuard const GUARD(dev->admission);
    if (GUARD) netdev->state = 0;
}

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
    if (txq->failed.load(std::memory_order_acquire)) {
        txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
        return TxAttemptResult::FAILED;
    }
    if (txq->num_free == 0) {
        virtq_kick(txq);
        txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
        return TxAttemptResult::FULL;
    }

    auto const START = reinterpret_cast<uintptr_t>(pkt->storage.data());
    auto const DATA = reinterpret_cast<uintptr_t>(pkt->data);
    if (DATA < START || DATA - START < dev->hdr_size || DATA - START > pkt->storage.size() ||
        pkt->len > pkt->storage.size() - (DATA - START)) {
        txq->lock.unlock_irqrestore(TXFLAGS);
        free_pkt_list(reclaimed);
        return TxAttemptResult::FAILED;
    }
#ifdef WOS_SELFTEST
    if (pkt->len == 64 && std::memcmp(pkt->data + 14, "WOSDMAF1", 8) == 0)
        txq->selftest_address = fault_override.exchange(0, std::memory_order_acq_rel);
#endif
    auto* hdr = reinterpret_cast<VirtIONetHeader*>(pkt->push(dev->hdr_size));
    std::memset(hdr, 0, dev->hdr_size);

    auto total_len = static_cast<uint32_t>(pkt->len);
    auto buffer = std::span<std::byte>(reinterpret_cast<std::byte*>(pkt->data), pkt->len);
    int const RET = virtq_add_buf(txq, buffer, pkt);
#ifdef WOS_SELFTEST
    txq->selftest_address = 0;
#endif
    if (RET < 0) {
        pkt->pull(dev->hdr_size);
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
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) {
        ker::net::pkt_free(pkt);
        return -ENODEV;
    }
    uint8_t const PAIR_COUNT = active_tx_pair_count(dev);
    if (PAIR_COUNT == 0) {
        ker::net::pkt_free(pkt);
        return -1;
    }

    bool const WKI_CONTROL = is_wki_control_liveness_frame(pkt);
    auto start_pair = tx_pair_for_packet(dev, pkt);
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
            start_pair = tx_pair_for_packet(dev, pkt);
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
    if (dev == nullptr) return -ENODEV;
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) return -ENODEV;
    if (pair_idx >= dev->num_queue_pairs) {
        return -EINVAL;
    }
    auto& pair = dev->queue_pairs.at(pair_idx);
    if (!ker::net::napi_set_worker_cpu(&pair.napi, cpu)) {
        return -EBUSY;
    }
    if (dev->msix_enabled && pair.irq_vector != 0) {
        ker::dev::pci::pci_configure_msix_entry(dev->pci, pair_idx, pair.irq_vector, cpu);
    }
    return 0;
}

template <typename PinWorkers, typename SetInterruptCpu>
auto apply_queue_cpu_mask(VirtIONetDevice& dev, uint64_t mask, PinWorkers&& pin_workers, SetInterruptCpu&& set_interrupt_cpu) -> int {
    using ker::mod::sched::KernelWorkerCpuPin;
    std::array<KernelWorkerCpuPin, ker::mod::sched::MAX_KERNEL_WORKER_CPU_PINS> requests{};
    size_t count = 0;
    for (uint64_t remaining = mask; remaining != 0 && count < requests.size(); remaining &= remaining - 1) {
        if (count >= dev.num_queue_pairs) return -EINVAL;
        requests[count] = {.worker = dev.queue_pairs.at(count).napi.worker, .cpu = static_cast<uint64_t>(__builtin_ctzll(remaining))};
        ++count;
    }
    if (count == 0) return 0;
    for (auto const& request : std::span(requests).first(count)) {
        if (request.worker == nullptr) return -EBUSY;
    }
    if (!pin_workers(std::span<const KernelWorkerCpuPin>(requests).first(count))) return -EBUSY;
    for (size_t i = 0; i < count; ++i) {
        auto const& pair = dev.queue_pairs.at(i);
        if (dev.msix_enabled && pair.irq_vector != 0) set_interrupt_cpu(static_cast<uint32_t>(i), pair.irq_vector, requests[i].cpu);
    }
    return 0;
}

auto virtio_net_set_queue_cpu_mask(ker::net::NetDevice* net, uint64_t mask) -> int {
    auto* dev = static_cast<VirtIONetDevice*>(net->private_data);
    if (dev == nullptr) return -ENODEV;
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) return -ENODEV;
    return apply_queue_cpu_mask(
        *dev, mask, ker::mod::sched::pin_kernel_workers_to_cpus,
        [dev](uint32_t pair, uint8_t vector, uint64_t cpu) { ker::dev::pci::pci_configure_msix_entry(dev->pci, pair, vector, cpu); });
}

auto virtio_net_check_mac(ker::net::NetDevice* netdev, const uint8_t* mac) -> int {
    if (netdev == nullptr || mac == nullptr) return -EINVAL;
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    if (dev == nullptr) return -ENODEV;
    CallbackGuard const GUARD(dev->admission);
    if (!GUARD) return -ENODEV;
    if (std::equal(netdev->mac.bytes.begin(), netdev->mac.bytes.end(), mac)) return 0;
    // Modern config is read-only; runtime CTRL_MAC_ADDR is not negotiated yet.
    if (dev->modern_cfg != nullptr || dev->io_base == 0 || (dev->negotiated_features & VIRTIO_NET_F_MAC) == 0) return -ENOTSUP;
    return 0;
}

void virtio_net_set_mac(ker::net::NetDevice* netdev, const uint8_t* mac) {
    if (netdev == nullptr || netdev->private_data == nullptr) return;
    auto* guarded_dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    CallbackGuard const GUARD(guarded_dev->admission);
    if (!GUARD) return;
    if (virtio_net_check_mac(netdev, mac) != 0 || std::equal(netdev->mac.bytes.begin(), netdev->mac.bytes.end(), mac)) return;
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
    .check_mac = virtio_net_check_mac,
    .set_queue_cpu_mask = virtio_net_set_queue_cpu_mask,
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

// Serialized probe or terminal retirement after callback admission/drain.
auto reset_transport(VirtIONetDevice* dev) -> bool {
    (void)iommu::set_platform_dma_ready(dev->pci, false);
    struct ResetBackend {
        VirtIONetDevice* dev;
        uint64_t deadline;
        void write_reset() {
            if (dev->modern_cfg != nullptr)
                dev->modern_cfg->device_status = 0;
            else
                write_status(dev->io_base, 0);
        }
        auto read_status() -> uint8_t {
            if (dev->modern_cfg != nullptr) return dev->modern_cfg->device_status;
            return ::inb(dev->io_base + VIRTIO_REG_DEVICE_STATUS);
        }
        auto expired() -> bool { return ker::mod::time::get_ms() >= deadline; }
        void relax() { asm volatile("pause" ::: "memory"); }
    } backend{dev, future_deadline_ms(1000)};
    return managed::reset(backend, 10'000'000);
}

// Read back every source mask before releasing any owned interrupt vector.
auto mask_interrupts(VirtIONetDevice* dev) -> bool {
    auto* p = dev->pci;
    auto read = [p](uint8_t offset) { return pci::pci_config_read16(p->bus, p->slot, p->function, offset); };
    auto write = [p](uint8_t offset, uint16_t value) { pci::pci_config_write16(p->bus, p->slot, p->function, offset, value); };
    bool masked = true;
    uint8_t const MSI = pci::pci_find_capability(p, pci::PCI_CAP_ID_MSI);
    if (MSI != 0) {
        if (MSI > 0xfc) return false;
        auto const OFFSET = static_cast<uint8_t>(MSI + 2);
        write(OFFSET, static_cast<uint16_t>(read(OFFSET) & ~1U));
        masked = (read(OFFSET) & 1U) == 0;
    }
    uint8_t const MSIX = pci::pci_find_capability(p, pci::PCI_CAP_ID_MSIX);
    if (MSIX != 0) {
        if (MSIX > 0xfc) return false;
        auto const OFFSET = static_cast<uint8_t>(MSIX + 2);
        write(OFFSET, static_cast<uint16_t>((read(OFFSET) | 0x4000U) & ~0x8000U));
        masked = (read(OFFSET) & 0xc000U) == 0x4000U && masked;
    }
    write(pci::PCI_COMMAND, static_cast<uint16_t>(read(pci::PCI_COMMAND) | pci::PCI_COMMAND_INT_DISABLE));
    return (read(pci::PCI_COMMAND) & pci::PCI_COMMAND_INT_DISABLE) != 0 && masked;
}

void drain_interrupts(VirtIONetDevice* dev) {
    for (auto& pair : dev->queue_pairs) {
        if (!pair.irq_owned) continue;
        ker::mod::gates::free_irq(pair.irq_vector);
        pair.irq_owned = false;
    }
}

auto retire_regions(VirtIONetDevice* dev, bool quiesced) -> bool {
    bool released = true;
    for (auto& pair : dev->queue_pairs) {
        released = virtq_retire(pair.rxq, quiesced) && released;
        released = virtq_retire(pair.txq, quiesced) && released;
    }
    released = virtq_retire(dev->ctrlq, quiesced) && released;
    managed::DmaApi api;
    return dev->control_buffer.retire(api, quiesced) && released;
}

struct RetirementBackend {
    VirtIONetDevice* dev;
    bool reset_observed = false, interrupts_masked = false, bme_disabled = false;
    void close_admission() { dev->admission.close(); }
    auto drain_readers() -> bool {
        uint64_t const DEADLINE = future_deadline_ms(5000);
        for (size_t spins = 0; spins < 10'000'000 && ker::mod::time::get_ms() < DEADLINE; ++spins) {
            if (dev->admission.drained()) return true;
            if (ker::mod::sched::can_query_current_task())
                ker::mod::sched::kern_yield();
            else
                asm volatile("pause" ::: "memory");
        }
#ifdef WOS_SELFTEST
        if (!dev->admission.drained()) {
            net_log::warn("virtio live reader drain timeout readers=%u", dev->admission.readers());
            auto const COUNT = ker::mod::sched::get_active_task_count();
            for (uint32_t i = 0; i < COUNT; ++i) {
                auto* task = ker::mod::sched::get_active_task_at_safe(i);
                if (task == nullptr) continue;
                if (task->kernel_shutdown_ownership.held())
                    net_log::warn("virtio live held owner pid=%lu name=%s requested=%u", task->pid, task->name,
                                  task->kernel_shutdown_requested.load());
                task->release();
            }
        }
#endif
        return dev->admission.drained();
    }
    void unpublish() {
        ker::net::netdev_mark_control_retiring(&dev->netdev);
        ker::net::NetDeviceRetireToken token;
        (void)ker::net::netdev_unregister_begin(&dev->netdev, token);
        // Keep registry entries, NAPI objects and workers stable for snapshots
        // and raw WKI references. A late callback cannot pass the closed gate.
        // No wait for netdevice references: queued packet owners retire below.
        for (auto& pair : dev->queue_pairs) {
            pair.napi.state.store(ker::net::NapiState::DISABLED, std::memory_order_release);
            pair.napi.has_work.store(false, std::memory_order_release);
        }
    }
    auto mask_interrupts() -> bool { return interrupts_masked = virtio::mask_interrupts(dev); }
    auto reset() -> bool {
        reset_observed = reset_transport(dev);
#ifdef WOS_SELFTEST
        if (probe_test != nullptr) {
            probe_test->reset_observed = reset_observed;
            if (probe_test_withhold_reset) return false;
        }
        if (selftest_live_withhold_reset) return false;
#endif
        return reset_observed;
    }
    auto disable_bus_master() -> bool { return bme_disabled = pci::pci_set_bus_master(dev->pci, false); }
    void drain_interrupts() { virtio::drain_interrupts(dev); }
    void quarantine() { (void)retire_regions(dev, false); }
    auto release() -> bool { return retire_regions(dev, true); }
};

auto retire_device(VirtIONetDevice* dev) -> int {
    auto const SHUTDOWN_HOLD = ker::mod::sched::hold_kernel_shutdown();
    uint8_t expected = 0;
    if (!dev->retirement.compare_exchange_strong(expected, 1, std::memory_order_acq_rel, std::memory_order_acquire))
        return expected == 2 ? 0 : (expected == 1 ? -EBUSY : -EIO);
    auto const BEFORE = dma::stats(dev->pci);
    RetirementBackend backend{dev};
    auto const RESULT = lifecycle::retire(backend);
    auto const AFTER = dma::stats(dev->pci);
    dev->retirement.store(RESULT == lifecycle::Result::released ? 2 : 3, std::memory_order_release);
    net_log::info("virtio terminal retirement result=%u reset=%u irq_masked=%u bme_off=%u regions=%zu->%zu pages=%zu->%zu q=%zu",
                  static_cast<unsigned>(RESULT), backend.reset_observed, backend.interrupts_masked, backend.bme_disabled, BEFORE.regions,
                  AFTER.regions, BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
    return RESULT == lifecycle::Result::released ? 0 : -EIO;
}

auto fail_published_probe(VirtIONetDevice* dev) -> int {
    (void)retire_device(dev);
    dev->failed_probe_next = failed_probes;
    failed_probes = dev;
    return -1;
}

auto publish_irqs(VirtIONetDevice* dev) -> bool {
    dev->netdev.private_data = dev;
    dev->callbacks_published = true;
    for (uint8_t i = 0; i < dev->num_queue_pairs; ++i) {
        auto& pair = dev->queue_pairs[i];
        ker::net::napi_init(&pair.napi, &dev->netdev, virtio_net_poll, 64);
        if (pair.irq_vector == 0) return false;
        if (ker::mod::gates::request_irq(pair.irq_vector, virtio_net_irq, &pair, i == 0 ? "virtio-net" : "virtio-net-mq") != 0) {
            pair.irq_owned = false;  // A conflicting handler is never ours to free.
            return false;
        }
        pair.irq_owned = true;
    }
    if (dev->use_intx) {
        auto* p = dev->pci;
        auto const COMMAND = pci::pci_config_read16(p->bus, p->slot, p->function, pci::PCI_COMMAND);
        pci::pci_config_write16(p->bus, p->slot, p->function, pci::PCI_COMMAND,
                                static_cast<uint16_t>(COMMAND & ~pci::PCI_COMMAND_INT_DISABLE));
        if ((pci::pci_config_read16(p->bus, p->slot, p->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_INT_DISABLE) != 0) return false;
    }
    return true;
}

auto fail_probe(VirtIONetDevice* dev) -> int {
    if (dev->callbacks_published) {
        net_log::error("refusing probe unwind after callback publication");
        return -1;
    }
    bool const MASKED = mask_interrupts(dev);
    bool reset = reset_transport(dev);
#ifdef WOS_SELFTEST
    if (probe_test != nullptr) {
        probe_test->reset_observed = reset;
        if (probe_test_withhold_reset) reset = false;
    }
#endif
    bool const DISABLED = pci::pci_set_bus_master(dev->pci, false);
    bool const QUIESCED = reset && DISABLED && MASKED;
    if (MASKED) drain_interrupts(dev);
    bool const released = retire_regions(dev, QUIESCED);
    net_log::info("virtio probe unwind reset=%u bme_off=%u released=%u regions=%zu quarantined=%zu", reset, DISABLED, released,
                  dma::stats(dev->pci).regions, dma::stats(dev->pci).quarantined);
    if (released)
        delete dev;
    else {
        dev->failed_probe_next = failed_probes;
        failed_probes = dev;
    }
    return -1;
}

auto prepare_probe(VirtIONetDevice* dev) -> bool {
    bool const RESET = reset_transport(dev);
    bool const DISABLED = pci::pci_set_bus_master(dev->pci, false);
    if (!RESET || !DISABLED) return false;
    // The current coherent backend requires snooped, ordered x86 PCI DMA.
    uint8_t const PCIE = pci::pci_find_capability(dev->pci, 0x10);
    if (PCIE == 0) return true;
    if (PCIE > 0xf4) return false;
    auto const OFFSET = static_cast<uint8_t>(PCIE + 8);
    auto* pci_dev = dev->pci;
    uint16_t const CONTROL = pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, OFFSET);
    pci::pci_config_write16(pci_dev->bus, pci_dev->slot, pci_dev->function, OFFSET, static_cast<uint16_t>(CONTROL & ~0x810U));
    return (pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, OFFSET) & 0x810U) == 0;
}

#ifdef WOS_SELFTEST
// A private rootless fixture echoes EtherType 0x88b5 and flips request byte 22
// from 1 to 2. Distinct payload bytes and sizes detect stale/truncated DMA copies.
auto selftest_packet_byte(size_t offset, size_t frame_size, bool reply) -> uint8_t {
    constexpr std::array<uint8_t, 6> SOURCE{2, 0, 0, 0x20, 0, 1};
    constexpr char MAGIC[] = "WOSDMA20";
    if (offset < 6) return 0xff;
    if (offset < 12) return SOURCE[offset - 6];
    if (offset == 12) return 0x88;
    if (offset == 13) return 0xb5;
    if (offset < 22) return static_cast<uint8_t>(MAGIC[offset - 14]);
    if (offset == 22) return reply ? 2 : 1;
    if (offset == 23) return 0;
    if (offset < 28) return static_cast<uint8_t>(frame_size >> ((offset - 24) * 8));
    return static_cast<uint8_t>(offset * 17 + frame_size);
}

void selftest_packet_dma(VirtIONetDevice* dev) {
    auto& pair = dev->queue_pairs[0];
    ker::net::pkt_pool_init();
    ker::net::pkt_pool_reserve_for_rx_descriptors(pair.rxq->size);
    probe_test->packet_pool_before = ker::net::pkt_pool_free_count();
    fill_rx_queue_for(dev, pair.rxq);
    for (size_t frame_size : {size_t{64}, size_t{9014}}) {
        auto* tx = ker::net::pkt_alloc();
        if (tx == nullptr) return;
        tx->len = frame_size;
        for (size_t i = 0; i < frame_size; ++i) tx->data[i] = selftest_packet_byte(i, frame_size, false);
        if (virtio_net_try_xmit_pair(&dev->netdev, dev, tx, 0) != TxAttemptResult::SENT) {
            ker::net::pkt_free(tx);
            return;
        }
        bool received = false;
        uint64_t const DEADLINE = future_deadline_ms(5000);
        for (size_t spins = 0; spins < 100'000'000 && ker::mod::time::get_ms() < DEADLINE; ++spins) {
            // This unpublished probe has no IRQ/NAPI readers. Exercise the
            // normal queue completion/copy and refill functions directly.
            free_pkt_list(collect_tx_for(pair.txq));
            for (uint16_t n = 0; n < pair.rxq->size; ++n) {
                uint32_t length = 0;
                uint16_t const ID = virtq_get_buf(pair.rxq, &length);
                if (ID == VIRTQ_NO_BUF) break;
                if (ID == VIRTQ_BAD_BUF) return;
                auto* rx = pair.rxq->pkt_map[ID];
                pair.rxq->pkt_map[ID] = nullptr;
                bool matches = length == dev->hdr_size + frame_size && virtio_net_rx_buffer_count(dev, rx) == 1;
                if (matches) {
                    for (size_t i = 0; i < frame_size; ++i) {
                        if (rx->storage[dev->hdr_size + i] != selftest_packet_byte(i, frame_size, true)) {
                            matches = false;
                            break;
                        }
                    }
                }
                received = received || matches;
                ker::net::pkt_free(rx);
            }
            fill_rx_queue_for(dev, pair.rxq);
            if (received && pair.txq->num_free == pair.txq->size) break;
            asm volatile("pause" ::: "memory");
        }
        if (!received || pair.txq->num_free != pair.txq->size) return;
        ++probe_test->packets_verified;
    }
}
#endif

#ifdef WOS_SELFTEST
struct NapiIrqTest {
    std::atomic<VirtIONetDevice*> device{nullptr};
    std::atomic<uint64_t> expected_cookie{0}, armed_cookie{0}, received_cookie{0}, suppressed_cookie{0};
    std::atomic<uint64_t> suppressed{0}, bad{0}, duplicate{0}, foreign{0}, reflections{0};
    std::atomic<uint64_t> suppressed_us{0}, received_us{0};
    std::atomic<uint16_t> suppressed_idx{0}, suppressed_desc{0}, received_idx{0}, received_desc{0};
    std::atomic<uint32_t> suppressed_len{0};
    std::atomic<ker::net::NapiPollSource> source{ker::net::NapiPollSource::INLINE};
};
NapiIrqTest napi_irq_test;

// Existing notification lock is held, after masking this admitted real IRQ.
// An IDLE owner cannot consume RX until its begin-poll gate acquires that lock.
auto selftest_napi_irq(VirtIONetQueuePair* pair) -> bool {
    if (napi_irq_test.device.load(std::memory_order_acquire) != pair->dev || pair->index != 0) return false;
    uint64_t cookie = napi_irq_test.armed_cookie.load(std::memory_order_acquire);
    if (cookie == 0 || pair->napi.state.load(std::memory_order_acquire) != ker::net::NapiState::IDLE ||
        pair->napi.has_work.load(std::memory_order_acquire) || pair->rxq->failed.load(std::memory_order_acquire))
        return false;
    auto* queue = pair->rxq;
    uint16_t const USED = static_cast<volatile VirtqUsed*>(queue->used)->idx;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    if (static_cast<uint16_t>(USED - queue->last_used_idx) != 1) return false;
    volatile auto const& elem = queue->used->ring[queue->last_used_idx % queue->size];
    uint32_t const DESC = elem.id;
    uint32_t const LENGTH = elem.len;
    if (DESC >= queue->size || LENGTH != napi_test::FRAME_BYTES + pair->dev->hdr_size) return false;
    if (!napi_irq_test.armed_cookie.compare_exchange_strong(cookie, 0, std::memory_order_acq_rel)) return false;
    napi_irq_test.suppressed_idx.store(USED, std::memory_order_relaxed);
    napi_irq_test.suppressed_desc.store(static_cast<uint16_t>(DESC), std::memory_order_relaxed);
    napi_irq_test.suppressed_len.store(LENGTH, std::memory_order_relaxed);
    napi_irq_test.suppressed_us.store(ker::mod::time::get_us(), std::memory_order_relaxed);
    napi_irq_test.suppressed.fetch_add(1, std::memory_order_relaxed);
    napi_irq_test.suppressed_cookie.store(cookie, std::memory_order_release);
    return true;
}

auto selftest_napi_rx(VirtIONetDevice* dev, Virtqueue* queue, ker::net::PacketBuffer* pkt, ker::net::NapiPollSource source, uint16_t desc)
    -> bool {
    if (napi_irq_test.device.load(std::memory_order_acquire) != dev) return false;
    auto const BYTES = std::span<const uint8_t>(pkt->data, pkt->len);
    if (!napi_test::has_magic(BYTES)) {
        napi_irq_test.foreign.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    auto const FRAME = napi_test::inspect(BYTES);
    if (!FRAME || FRAME->cookie != napi_irq_test.expected_cookie.load(std::memory_order_acquire)) {
        napi_irq_test.bad.fetch_add(1, std::memory_order_relaxed);
    } else if (!FRAME->reply) {
        napi_irq_test.reflections.fetch_add(1, std::memory_order_relaxed);
    } else if (napi_irq_test.received_cookie.load(std::memory_order_acquire) != 0) {
        napi_irq_test.duplicate.fetch_add(1, std::memory_order_relaxed);
    } else {
        napi_irq_test.received_desc.store(desc, std::memory_order_relaxed);
        napi_irq_test.received_idx.store(queue->last_used_idx, std::memory_order_relaxed);
        napi_irq_test.received_us.store(ker::mod::time::get_us(), std::memory_order_relaxed);
        napi_irq_test.source.store(source, std::memory_order_relaxed);
        napi_irq_test.received_cookie.store(FRAME->cookie, std::memory_order_release);
    }
    ker::net::pkt_free(pkt);
    return true;
}

auto selftest_napi_quiet(VirtIONetDevice* dev) -> bool {
    auto& pair = dev->queue_pairs.at(0);
    uint64_t const FLAGS = pair.notification_lock.lock_irqsave();
    bool quiet =
        pair.napi.state.load(std::memory_order_acquire) == ker::net::NapiState::IDLE && !pair.napi.has_work.load(std::memory_order_acquire);
    if (quiet) {
        auto const PENDING = virtio_net_pair_pending(dev, &pair);
        quiet = !PENDING.rx && !PENDING.tx && (pair.rxq->avail->flags & VIRTQ_AVAIL_F_NO_INTERRUPT) == 0 &&
                (pair.txq->avail->flags & VIRTQ_AVAIL_F_NO_INTERRUPT) == 0;
    }
    pair.notification_lock.unlock_irqrestore(FLAGS);
    return quiet;
}

void selftest_napi_cleanup(VirtIONetDevice* dev) {
    napi_irq_test.armed_cookie.store(0, std::memory_order_release);
    auto& pair = dev->queue_pairs.at(0);
    uint64_t const FLAGS = pair.notification_lock.lock_irqsave();
    bool const PENDING = virtio_net_rearm_pair_notifications_locked(dev, &pair, 0);
    pair.notification_lock.unlock_irqrestore(FLAGS);
    if (PENDING) ker::net::napi_schedule(&pair.napi);
    napi_irq_test.device.store(nullptr, std::memory_order_release);
}

struct LiveDmaTest {
    std::atomic<VirtIONetDevice*> device{nullptr};
    std::atomic<uint64_t> small{0}, jumbo{0}, bad{0}, worker_rx{0}, irqs{0}, sent{0};
    std::atomic<uint64_t> sender_cpu{UINT64_MAX}, receive_cpus{0};
    std::atomic<bool> stop_sender{false}, holder_entered{false}, release_holder{false}, holder_released{false};
    std::atomic<bool> holder_returned_after_exit_point{false}, retire_started{false}, retire_done{false};
    std::atomic<uint32_t> protected_yields{0};
    std::atomic<int> retirement_result{-EBUSY};
};
LiveDmaTest live_test;

void selftest_live_irq(VirtIONetDevice* dev) {
    if (live_test.device.load(std::memory_order_acquire) == dev) live_test.irqs.fetch_add(1, std::memory_order_relaxed);
}

auto selftest_live_rx(VirtIONetDevice* dev, ker::net::PacketBuffer* pkt) -> bool {
    if (live_test.device.load(std::memory_order_acquire) != dev || pkt->len < 28 || pkt->data[12] != 0x88 || pkt->data[13] != 0xb5 ||
        pkt->data[14] != 'W' || pkt->data[22] != 2)
        return false;
    bool valid = pkt->len == 64 || pkt->len == 9014;
    for (size_t i = 0; valid && i < pkt->len; ++i) valid = pkt->data[i] == selftest_packet_byte(i, pkt->len, true);
    if (valid) {
        auto& count = pkt->len == 64 ? live_test.small : live_test.jumbo;
        count.fetch_add(1, std::memory_order_relaxed);
        uint64_t const CPU = ker::mod::cpu::current_cpu();
        if (CPU < 64) live_test.receive_cpus.fetch_or(uint64_t{1} << CPU, std::memory_order_relaxed);
        auto* current = ker::mod::sched::get_current_task();
        for (auto& pair : dev->queue_pairs)
            if (current != nullptr && current == pair.napi.worker) live_test.worker_rx.fetch_add(1, std::memory_order_relaxed);
    } else {
        live_test.bad.fetch_add(1, std::memory_order_relaxed);
    }
    ker::net::pkt_free(pkt);
    return true;
}

template <typename Predicate>
auto live_wait(Predicate predicate, uint64_t timeout_ms = 10000) -> bool {
    uint64_t const DEADLINE = future_deadline_ms(timeout_ms);
    while (!predicate()) {
        if (ker::mod::time::get_ms() >= DEADLINE) return false;
        ker::mod::sched::kern_sleep_us(1000);
    }
    return true;
}

void selftest_live_sender() {
    auto* dev = live_test.device.load(std::memory_order_acquire);
    live_test.sender_cpu.store(ker::mod::cpu::current_cpu(), std::memory_order_release);
    for (size_t n = 0; n < 20000 && !live_test.stop_sender.load(std::memory_order_acquire); ++n) {
        size_t const SIZE = (n & 1U) == 0 ? 64 : 9014;
        if (auto* pkt = ker::net::pkt_alloc_tx()) {
            pkt->len = SIZE;
            for (size_t i = 0; i < SIZE; ++i) pkt->data[i] = selftest_packet_byte(i, SIZE, false);
            if (virtio_net_start_xmit(&dev->netdev, pkt) == 0) live_test.sent.fetch_add(1, std::memory_order_relaxed);
        }
        ker::mod::sched::kern_sleep_us(1000);
    }
}

void selftest_live_holder() {
    auto* dev = live_test.device.load(std::memory_order_acquire);
    {
        CallbackGuard const GUARD(dev->admission);
        if (!GUARD) return;
        live_test.holder_entered.store(true, std::memory_order_release);
        uint64_t const DEADLINE = future_deadline_ms(20000);
        while (!live_test.release_holder.load(std::memory_order_acquire) && ker::mod::time::get_ms() < DEADLINE) {
            ker::mod::sched::kern_sleep_us(1000);
            auto* current = ker::mod::sched::get_current_task();
            if (ker::mod::sched::kernel_threads_shutdown_requested() && current->kernel_shutdown_requested.load(std::memory_order_acquire))
                live_test.protected_yields.fetch_add(1, std::memory_order_release);
        }
    }
    live_test.holder_released.store(true, std::memory_order_release);
    ker::mod::sched::kern_yield();
    // A pending cooperative exit must take effect after the ownership scope.
    live_test.holder_returned_after_exit_point.store(true, std::memory_order_release);
}

void selftest_live_retirer() {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    live_test.retire_started.store(true, std::memory_order_release);
    live_test.retirement_result.store(virtio_net_shutdown(), std::memory_order_release);
    live_test.retire_done.store(true, std::memory_order_release);
}

auto live_post(const char* name, void (*entry)(), uint64_t cpu) -> ker::mod::sched::task::Task* {
    auto* task = ker::mod::sched::task::Task::create_kernel_thread(name, entry);
    if (task == nullptr) return nullptr;
    if (!task->try_acquire()) return nullptr;
    ker::mod::sched::post_task_pinned_cpu(cpu, task);
    return task;  // Caller owns a reference, including after cooperative exit.
}

void selftest_fault_runner(VirtIONetDevice* dev, bool positive, ker::mod::sched::task::Task* sender) {
    auto test_flag = [](const char* flag) { return cmdline_has_token(ker::init::get_kernel_cmdline(), flag); };
    auto hold = ker::mod::sched::hold_kernel_shutdown();
    auto const DOMAIN = iommu::domain_info(dev->pci);
    bool const STALE = test_flag("--iommu-test-fault-stale");
    bool const WITHHELD = test_flag("--iommu-test-withhold-invalidation");
    live_test.stop_sender.store(true, std::memory_order_release);
    bool ok = positive && sender != nullptr && live_wait([sender] { return sender->exit_in_progress.load(std::memory_order_acquire); }) &&
              DOMAIN.mode == iommu::DomainMode::translated;
    auto const BEFORE = dma::stats(dev->pci);
    dma::Allocation allocation{};
    uint64_t address = 0;
    if (ok && WITHHELD) {
        ok = iommu::selftest_withhold_invalidation(dev->pci);
        auto result = dma::allocate_coherent(dev->pci, 4096);
        ok = ok && result.error == dma::Error::quarantined && result.allocation.handle.registry != nullptr;
        allocation = result.allocation;
    } else if (ok) {
        auto* owner = STALE ? dev->pci : pci::pci_get_device(0);
        ok = owner != nullptr && (STALE || owner != dev->pci);
        auto result = dma::allocate_coherent(owner, 4096);
        ok = ok && result.error == dma::Error::none && result.allocation.cpu != nullptr;
        allocation = result.allocation;
        if (ok) {
            std::memset(allocation.cpu, 0xa5, 4096);
            auto const SUBMITTED = dma::prepare(allocation.handle);
            ok = SUBMITTED.error == dma::Error::none;
            address = SUBMITTED.submission.address;
            // No hardware has seen this address yet. Revoke the stale case's
            // PTEs while retaining its registry/physical backing permanently.
            ok = ok && dma::complete(SUBMITTED.submission, 0, dma::Retirement::never_submitted) == dma::Error::none;
            if (ok && STALE) ok = iommu::unmap(dev->pci, address, address, 4096, iommu::vtd::Access::read_write);
            ok = ok && dma::quarantine(allocation.handle) == dma::Error::quarantined;
        }
        if (ok) {
            auto* packet = ker::net::pkt_alloc_tx();
            ok = packet != nullptr;
            if (packet != nullptr) {
                packet->len = 64;
                std::memset(packet->data, 0, packet->len);
                std::memcpy(packet->data + 14, "WOSDMAF1", 8);
                fault_override.store(address, std::memory_order_release);
                ok = virtio_net_start_xmit(&dev->netdev, packet) == 0;
            }
        }
    }
    bool const CLOSED = ok && live_wait([&] {
                            auto s = iommu::unit_status(DOMAIN.unit);
                            return s.failed && s.inhibited;
                        });
    auto const STATUS = iommu::unit_status(DOMAIN.unit);
    auto const* report = iommu::fault_report(DOMAIN.unit);
    bool matched = false;
    if (report != nullptr) {
        for (size_t i = 0; i < report->count; ++i) {
            auto const& r = report->records[i];
            matched |= r.stable && r.identified &&
                       r.source() == ((uint16_t{dev->pci->bus} << 8U) | (dev->pci->slot << 3U) | dev->pci->function) &&
                       r.address() == address && r.read() && r.reason() == 6 && r.domain == DOMAIN.id && r.domain_generation != 0;
        }
    }
    bool inhibited = CLOSED;
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto* device = pci::pci_get_device(i);
        if (iommu::domain_info(device).unit != DOMAIN.unit) continue;
        inhibited =
            inhibited && !iommu::bus_master_allowed(device) && !pci::pci_set_bus_master(device, true) &&
            (pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) == 0;
    }
    bool const REJECTED = CLOSED && dma::allocate_coherent(dev->pci, 4096).error == dma::Error::unavailable_device;
    int const RETIRED = virtio_net_shutdown();
    auto const AFTER = dma::stats(dev->pci);
    bool irq_owned = false;
    for (auto const& pair : dev->queue_pairs) irq_owned |= pair.irq_owned;
    bool guards = WITHHELD;
    if (!WITHHELD && allocation.cpu != nullptr && !irq_owned && dev->admission.drained()) {
        guards = true;
        for (size_t i = 0; i < 4096; ++i) guards &= allocation.cpu[i] == std::byte{0xa5};
    }
    size_t const EXTRA = STALE || WITHHELD ? 1 : 0;
    bool const PINNED = AFTER.regions == BEFORE.regions + EXTRA && AFTER.backing_pages == BEFORE.backing_pages + EXTRA &&
                        AFTER.quarantined == AFTER.regions;
    ok = ok && CLOSED && inhibited && REJECTED && RETIRED == -EIO && !irq_owned && dev->admission.drained() && guards && PINNED &&
         (WITHHELD ? report == nullptr && STATUS.irq_count == 0 : matched && STATUS.irq_count != 0 && fault_override.load() == 0);
    net_log::info(
        "virtio forbidden DMA %s case=%s address=0x%llx allocation_generation=%llu closed=%u inhibited=%u irq=%llu matched=%u guards=%u "
        "rejected=%u retired=%d irq_owned=%u regions=%zu->%zu pages=%zu->%zu q=%zu",
        ok ? "PASS" : "FAIL",
        WITHHELD ? "withheld-invalidation"
        : STALE  ? "stale"
                 : "foreign",
        static_cast<unsigned long long>(address), static_cast<unsigned long long>(allocation.handle.generation), CLOSED, inhibited,
        static_cast<unsigned long long>(STATUS.irq_count), matched, guards, REJECTED, RETIRED, irq_owned, BEFORE.regions, AFTER.regions,
        BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
    if (sender != nullptr) sender->release();
    // AHCI shares the failed unit. Do not run a filesystem shutdown that would
    // pretend this unit can still perform I/O. The private fixture stops by QMP.
    for (;;) ker::mod::sched::kern_sleep_us(100000);
}

void selftest_live_runner() {
    // Permit the normal init phases and scheduler handoff to finish. No BSP
    // selftest flag is used: IRQs, NAPI workers and other CPUs remain active.
    ker::mod::sched::kern_sleep_us(3'000'000);
    auto* dev = live_test.device.load(std::memory_order_acquire);
    auto const BEFORE = dma::stats(dev->pci);
    auto* sender = live_post("dma_live_tx", selftest_live_sender, 1);
    bool ok = sender != nullptr && live_wait([] {
                  return live_test.small.load(std::memory_order_acquire) >= 64 && live_test.jumbo.load(std::memory_order_acquire) >= 64 &&
                         live_test.worker_rx.load(std::memory_order_acquire) != 0 && live_test.irqs.load(std::memory_order_acquire) != 0;
              });
    auto const CMDLINE = ker::init::get_kernel_cmdline();
    if (cmdline_has_token(CMDLINE, "--iommu-test-fault-foreign") || cmdline_has_token(CMDLINE, "--iommu-test-fault-stale") ||
        cmdline_has_token(CMDLINE, "--iommu-test-withhold-invalidation"))
        selftest_fault_runner(dev, ok, sender);
    auto* holder = ok ? live_post("dma_live_owner", selftest_live_holder, 2) : nullptr;
    ok = holder != nullptr && live_wait([] { return live_test.holder_entered.load(std::memory_order_acquire); }) && ok;
    // Keep the coordinator that releases the held callback independently
    // runnable while retirement exercises its bounded reader wait.
    auto* retirer = ok ? live_post("dma_live_retire", selftest_live_retirer, 3) : nullptr;
    ok = retirer != nullptr && live_wait([dev] { return dev->admission.closed(); }) && ok;
    auto const HELD = dma::stats(dev->pci);
    bool const DRAIN_WAITED = ok && !dev->admission.drained() && !live_test.retire_done.load(std::memory_order_acquire) &&
                              HELD.regions == BEFORE.regions && HELD.backing_pages == BEFORE.backing_pages;
    // The coordinator is excluded. Owners and the retiring daemon must survive
    // scheduler exit checks while their scoped resources are still held.
    // A zero timeout only raises the global flag. A positive bound also marks
    // and wakes each target, which is required to exercise cooperative exit.
    auto const REQUEST = ker::mod::sched::request_kernel_threads_shutdown(1000);
    net_log::info("virtio live shutdown requested=%zu holder=%u retirer=%u", REQUEST.requested,
                  holder != nullptr && holder->kernel_shutdown_requested.load(std::memory_order_acquire),
                  retirer != nullptr && retirer->kernel_shutdown_requested.load(std::memory_order_acquire));
    bool const DEFERRED = ok && live_wait([] { return live_test.protected_yields.load(std::memory_order_acquire) >= 2; });
    live_test.stop_sender.store(true, std::memory_order_release);
    live_test.release_holder.store(true, std::memory_order_release);
    bool const RETIRED = retirer != nullptr && live_wait([] { return live_test.retire_done.load(std::memory_order_acquire); });
    bool const OWNER_EXITED = holder != nullptr && live_wait([holder] { return holder->exit_in_progress.load(std::memory_order_acquire); });
    if (!RETIRED) (void)virtio_net_shutdown();
    auto const AFTER = dma::stats(dev->pci);
    bool const ACCOUNTED = selftest_live_withhold_reset ? AFTER.regions == BEFORE.regions && AFTER.quarantined == BEFORE.regions &&
                                                              AFTER.backing_pages == BEFORE.backing_pages
                                                        : AFTER.regions == 0 && AFTER.backing_pages == 0 && AFTER.quarantined == 0;
    bool owners_detached = true, irqs_drained = true;
    for (auto& pair : dev->queue_pairs) {
        irqs_drained = irqs_drained && !pair.irq_owned;
        for (auto* queue : {pair.rxq, pair.txq})
            if (queue != nullptr)
                for (auto* pkt : queue->pkt_map) owners_detached = owners_detached && pkt == nullptr;
    }
    auto* late = ker::net::pkt_alloc();
    bool const LATE_REJECTED =
        late != nullptr && virtio_net_start_xmit(&dev->netdev, late) == -ENODEV && virtio_net_open(&dev->netdev) == -ENODEV;
    uint64_t const RX_CPUS = live_test.receive_cpus.load(std::memory_order_acquire);
    uint64_t const TX_CPU = live_test.sender_cpu.load(std::memory_order_acquire);
    bool const MULTICPU = TX_CPU < 64 && (RX_CPUS & ~(uint64_t{1} << TX_CPU)) != 0;
    int const EXPECTED = selftest_live_withhold_reset ? -EIO : 0;
    net_log::info("virtio live checks setup=%u released=%u returned=%u accounted=%u multicpu=%u bad=%lu result=%d expected=%d repeat=%d",
                  ok, live_test.holder_released.load(), live_test.holder_returned_after_exit_point.load(), ACCOUNTED, MULTICPU,
                  live_test.bad.load(), live_test.retirement_result.load(), EXPECTED, virtio_net_shutdown());
    ok = ok && DRAIN_WAITED && DEFERRED && RETIRED && OWNER_EXITED && live_test.holder_released.load(std::memory_order_acquire) &&
         !live_test.holder_returned_after_exit_point.load(std::memory_order_acquire) && ACCOUNTED && owners_detached && irqs_drained &&
         LATE_REJECTED && MULTICPU && live_test.bad.load(std::memory_order_acquire) == 0 &&
         live_test.retirement_result.load(std::memory_order_acquire) == EXPECTED && virtio_net_shutdown() == EXPECTED;
    net_log::info("virtio live DMA %s sent=%lu rx64=%lu rx9014=%lu worker_rx=%lu irq=%lu tx_cpu=%lu rx_cpus=0x%lx", ok ? "PASS" : "FAIL",
                  live_test.sent.load(), live_test.small.load(), live_test.jumbo.load(), live_test.worker_rx.load(), live_test.irqs.load(),
                  TX_CPU, RX_CPUS);
    net_log::info(
        "virtio live retirement waited=%u deferred=%u owner_exit=%u done=%u detached=%u irq_drained=%u late=%u regions=%zu->%zu "
        "pages=%zu->%zu q=%zu",
        DRAIN_WAITED, DEFERRED, OWNER_EXITED, RETIRED, owners_detached, irqs_drained, LATE_REJECTED, BEFORE.regions, AFTER.regions,
        BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
    if (sender != nullptr) sender->release();
    if (holder != nullptr) holder->release();
    if (retirer != nullptr) retirer->release();
    ker::mod::power::selftest_shutdown();
}

auto selftest_napi_wait_quiet(VirtIONetDevice* dev) -> bool {
    uint64_t quiet_since = 0;
    return live_wait(
        [&] {
            uint64_t const NOW = ker::mod::time::get_us();
            if (!selftest_napi_quiet(dev)) {
                quiet_since = 0;
                return false;
            }
            if (quiet_since == 0) quiet_since = NOW;
            return NOW - quiet_since >= 200000;
        },
        3000);
}

auto selftest_napi_fault_valid(uint64_t cookie, uint64_t suppressed_before, uint8_t hdr_size, const ker::net::NapiDiagSnapshot& before,
                               const ker::net::NapiDiagSnapshot& after) -> bool {
    if (!before.enabled || !after.enabled || before.schedules != after.schedules ||
        napi_irq_test.suppressed.load() - suppressed_before != 1 || napi_irq_test.suppressed_cookie.load() != cookie ||
        napi_irq_test.received_cookie.load() != cookie || napi_irq_test.source.load() != ker::net::NapiPollSource::RESCUE ||
        napi_irq_test.suppressed_idx.load() != napi_irq_test.received_idx.load() ||
        napi_irq_test.suppressed_desc.load() != napi_irq_test.received_desc.load() ||
        napi_irq_test.suppressed_len.load() != napi_test::FRAME_BYTES + hdr_size)
        return false;
    uint64_t const BEGIN = napi_irq_test.suppressed_us.load();
    uint64_t const END = napi_irq_test.received_us.load();
    return END >= BEGIN && END - BEGIN <= napi_test::RECOVERY_LIMIT_US;
}

void selftest_napi_runner() {
    auto* dev = napi_irq_test.device.load(std::memory_order_acquire);
    bool ok = false;
    uint64_t completed = 0, max_recovery_us = 0;
    std::array<uint64_t, 2> ordinary_irq{}, ordinary_worker_rx{};
    {
        CallbackGuard const GUARD(dev->admission);
        if (GUARD) {
            // Let normal boot traffic settle before the measured, private phase.
            ker::mod::sched::kern_sleep_us(3000000);
            ok = selftest_napi_wait_quiet(dev);
            uint64_t const BAD = napi_irq_test.bad.load();
            uint64_t const DUPLICATE = napi_irq_test.duplicate.load();
            uint64_t const FOREIGN = napi_irq_test.foreign.load();
            auto& napi = dev->queue_pairs.at(0).napi;
            for (uint64_t cookie = 1; ok && cookie <= 16; ++cookie) {
                bool const FAULT = cookie >= 5 && cookie <= 12;
                uint64_t const SUPPRESSED_BEFORE = napi_irq_test.suppressed.load();
                napi_irq_test.received_cookie.store(0, std::memory_order_release);
                napi_irq_test.expected_cookie.store(cookie, std::memory_order_release);
                auto before = ker::net::napi_diag_snapshot(&napi);
                auto* packet = ker::net::pkt_alloc_tx();
                ok = packet != nullptr;
                if (packet != nullptr) {
                    packet->len = napi_test::FRAME_BYTES;
                    for (size_t i = 0; i < packet->len; ++i) packet->data[i] = napi_test::packet_byte(i, cookie, false);
                    // start_xmit consumes the packet on success and failure.
                    ok = virtio_net_start_xmit(&dev->netdev, packet) == 0;
                }
                if (ok && FAULT) {
                    // The backend holds this cookie's reply until ARM. Drain
                    // request reflections and TX completions before suppressing.
                    ok = selftest_napi_wait_quiet(dev);
                    before = ker::net::napi_diag_snapshot(&napi);
                    if (ok) {
                        napi_irq_test.armed_cookie.store(cookie, std::memory_order_release);
                        net_log::info("napi irq ARM cookie=%lu", cookie);
                    }
                }
                if (ok) ok = live_wait([&] { return napi_irq_test.received_cookie.load(std::memory_order_acquire) == cookie; }, 2000);
                napi_irq_test.armed_cookie.store(0, std::memory_order_release);
                if (ok) ok = selftest_napi_wait_quiet(dev);
                auto const AFTER = ker::net::napi_diag_snapshot(&napi);
                uint64_t const BEGIN = napi_irq_test.suppressed_us.load();
                uint64_t const END = napi_irq_test.received_us.load();
                uint64_t const RECOVERY_US = END >= BEGIN ? END - BEGIN : UINT64_MAX;
                if (FAULT) {
                    ok = ok && selftest_napi_fault_valid(cookie, SUPPRESSED_BEFORE, dev->hdr_size, before, AFTER);
                    if (ok) max_recovery_us = std::max(max_recovery_us, RECOVERY_US);
                } else {
                    size_t const PHASE = cookie <= 4 ? 0 : 1;
                    ordinary_irq.at(PHASE) += AFTER.accepted.at(static_cast<size_t>(ker::net::NapiScheduleSource::IRQ)) -
                                              before.accepted.at(static_cast<size_t>(ker::net::NapiScheduleSource::IRQ));
                    ordinary_worker_rx.at(PHASE) += AFTER.rx_work.at(static_cast<size_t>(ker::net::NapiPollSource::WORKER)) -
                                                    before.rx_work.at(static_cast<size_t>(ker::net::NapiPollSource::WORKER));
                    ok = ok && napi_irq_test.suppressed.load() == SUPPRESSED_BEFORE;
                }
                ok = ok && napi_irq_test.bad.load() == BAD && napi_irq_test.duplicate.load() == DUPLICATE &&
                     napi_irq_test.foreign.load() == FOREIGN;
                net_log::info(
                    "napi irq CASE cookie=%lu fault=%u ok=%u source=%u suppressed=%lu idx=%u/%u desc=%u/%u len=%u dt_us=%lu "
                    "schedule=%lu/%lu/%lu/%lu bad=%lu duplicate=%lu foreign=%lu",
                    cookie, FAULT, ok, static_cast<unsigned>(napi_irq_test.source.load()),
                    napi_irq_test.suppressed.load() - SUPPRESSED_BEFORE, napi_irq_test.suppressed_idx.load(),
                    napi_irq_test.received_idx.load(), napi_irq_test.suppressed_desc.load(), napi_irq_test.received_desc.load(),
                    napi_irq_test.suppressed_len.load(), FAULT ? RECOVERY_US : 0, AFTER.schedules.at(0) - before.schedules.at(0),
                    AFTER.schedules.at(1) - before.schedules.at(1), AFTER.schedules.at(2) - before.schedules.at(2),
                    AFTER.schedules.at(3) - before.schedules.at(3), napi_irq_test.bad.load() - BAD,
                    napi_irq_test.duplicate.load() - DUPLICATE, napi_irq_test.foreign.load() - FOREIGN);
                if (ok) ++completed;
            }
            ok = ok && completed == 16 && ordinary_irq.at(0) != 0 && ordinary_irq.at(1) != 0 && ordinary_worker_rx.at(0) != 0 &&
                 ordinary_worker_rx.at(1) != 0;
            selftest_napi_cleanup(dev);
        }
    }  // Release the driver/shutdown hold before the ordinary global finalizer.
    net_log::info(
        "napi irq recovery %s cases=%lu suppressed=%lu max_dt_us=%lu bound_us=%lu slack_us=%lu "
        "ordinary_irq=%lu/%lu ordinary_worker_rx=%lu/%lu",
        ok ? "PASS" : "FAIL", completed, napi_irq_test.suppressed.load(), max_recovery_us, napi_test::RECOVERY_LIMIT_US,
        max_recovery_us > 100000 ? max_recovery_us - 100000 : 0, ordinary_irq.at(0), ordinary_irq.at(1), ordinary_worker_rx.at(0),
        ordinary_worker_rx.at(1));
    ker::mod::power::selftest_shutdown();
}

void selftest_napi_start(VirtIONetDevice* dev) {
    auto const* cmdline = ker::init::get_kernel_cmdline();
    if (ker::mod::smt::get_core_count() < 4 || device_count != 1 || dev->num_queue_pairs != 1 || dev->configured_queue_pairs != 1 ||
        dev->queue_pairs.at(0).napi.worker == nullptr || !dev->queue_pairs.at(0).napi.diagnostics_enabled ||
        cmdline_has_token(cmdline, "--selftest") || cmdline_has_token(cmdline, "--virtio-test-live")) {
        net_log::error("napi irq recovery FAIL configuration: require live SMP, four CPUs, one device/pair, net.napi_diag");
        return;
    }
    VirtIONetDevice* expected = nullptr;
    if (!napi_irq_test.device.compare_exchange_strong(expected, dev, std::memory_order_acq_rel)) return;
    auto* runner = live_post("napi_irq_test", selftest_napi_runner, 0);
    if (runner != nullptr) {
        runner->release();
    } else {
        napi_irq_test.device.store(nullptr, std::memory_order_release);
        net_log::error("napi irq recovery FAIL coordinator allocation");
    }
}

void selftest_live_start(VirtIONetDevice* dev) {
    if (ker::mod::smt::get_core_count() < 4 || dev->queue_pairs[0].napi.worker == nullptr) {
        net_log::error("virtio live DMA requires four CPUs and a NAPI worker");
        return;
    }
    VirtIONetDevice* expected = nullptr;
    if (!live_test.device.compare_exchange_strong(expected, dev, std::memory_order_acq_rel)) return;
    selftest_live_withhold_reset = cmdline_has_token(ker::init::get_kernel_cmdline(), "--virtio-test-live-withhold-reset");
    auto* runner = live_post("dma_live_test", selftest_live_runner, 0);
    if (runner != nullptr)
        runner->release();
    else
        net_log::error("virtio live DMA coordinator allocation failed");
}

auto selftest_terminal_retirement(VirtIONetDevice* dev) -> int {
    // Publish real netdevice and IRQ registry entries. BSP KTEST deliberately
    // has no running NAPI workers; concurrent execution needs a separate boot.
    auto& pair = dev->queue_pairs[0];
    dev->netdev.ops = &virtio_net_ops;
    dev->netdev.private_data = dev;
    dev->netdev.state = 1;
    dev->num_queue_pairs = 1;
    pair.irq_vector = ker::mod::gates::allocate_vector();
    pair.irq_owned = pair.irq_vector != 0;
    if (!pair.irq_owned || pci::pci_enable_msix(dev->pci, pair.irq_vector, 0) != 0) return fail_probe(dev);
    dev->msix_enabled = true;
    if (dev->modern_cfg != nullptr) {
        dev->modern_cfg->config_msix_vector = VIRTIO_MSI_NO_VECTOR;
        for (uint16_t q : {uint16_t{0}, uint16_t{1}}) {
            dev->modern_cfg->queue_select = q;
            dev->modern_cfg->queue_msix_vector = 0;
            if (dev->modern_cfg->queue_msix_vector != 0) return fail_probe(dev);
        }
    } else {
        ::outw(dev->io_base + VIRTIO_MSI_CONFIG_VECTOR, VIRTIO_MSI_NO_VECTOR);
        for (uint16_t q : {uint16_t{0}, uint16_t{1}}) {
            ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, q);
            ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
            if (::inw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR) != 0) return fail_probe(dev);
        }
    }
    if (!publish_irqs(dev)) return fail_published_probe(dev);
    if (ker::net::netdev_register(&dev->netdev) != 0) return fail_published_probe(dev);
    devices.at(device_count++) = dev;
    probe_test->terminal_published = true;
    int const RET = virtio_net_shutdown();
    probe_test->terminal_irq_drained = !pair.irq_owned;
    auto const AFTER = dma::stats(dev->pci);
    auto* late = ker::net::pkt_alloc();
    bool const TX_REJECTED = late != nullptr && virtio_net_start_xmit(&dev->netdev, late) == -ENODEV;
    virtio_net_irq(pair.irq_vector, &pair);
    bool const POLL_REJECTED = virtio_net_poll(&pair.napi, 64, ker::net::NapiPollSource::INLINE) == 0;
    probe_test->terminal_late_rejected =
        TX_REJECTED && POLL_REJECTED && virtio_net_open(&dev->netdev) == -ENODEV &&
        virtio_net_set_queue_cpu(&dev->netdev, 0, 0) == -ENODEV && virtio_net_set_queue_cpu_mask(&dev->netdev, 1) == -ENODEV &&
        virtio_net_check_mac(&dev->netdev, dev->netdev.mac.data()) == -ENODEV && !send_mq_ctrl_cmd(dev, 1) &&
        !ker::net::netdev_is_registered(&dev->netdev) && init_device(dev->pci) == -ENODEV;
    int const AGAIN = virtio_net_shutdown();
    auto const REPEATED = dma::stats(dev->pci);
    probe_test->terminal_repeat_stable = RET == AGAIN && RET == (probe_test_withhold_reset ? -EIO : 0) &&
                                         AFTER.regions == REPEATED.regions && AFTER.backing_pages == REPEATED.backing_pages &&
                                         AFTER.quarantined == REPEATED.quarantined;
    net_log::info("virtio terminal test published=%u late_rejected=%u irq_drained=%u repeat_stable=%u", probe_test->terminal_published,
                  probe_test->terminal_late_rejected, probe_test->terminal_irq_drained, probe_test->terminal_repeat_stable);
    return -1;
}

auto selftest_probe_boundary(VirtIONetDevice* dev, uint16_t pairs) -> int {
    probe_test->prepared = true;
    probe_test->mapped = dma::stats(dev->pci);
    probe_test->bme_enabled =
        (pci::pci_config_read16(dev->pci->bus, dev->pci->slot, dev->pci->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) != 0;
    // Queue/control DMA runs before IRQ handlers or NAPI workers are published.
    if (dev->modern_cfg != nullptr) {
        dev->modern_cfg->device_status =
            VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_FEATURES_OK | VIRTIO_STATUS_DRIVER_OK;
        probe_test->control_present = dev->ctrlq != nullptr;
        if (probe_test->control_present) probe_test->control_ack = send_mq_ctrl_cmd(dev, pairs);
    } else {
        write_status(dev->io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_DRIVER_OK);
    }
    if (probe_test->packets_requested) selftest_packet_dma(dev);
    int const RESULT = probe_test->terminal_requested ? selftest_terminal_retirement(dev) : fail_probe(dev);
    if (probe_test->packets_requested) probe_test->packet_pool_after = ker::net::pkt_pool_free_count();
    return RESULT;
}
#endif

// Only -ENOTSUP permits legacy fallback; owned probe failures are terminal.
auto init_device_modern(ker::dev::pci::PCIDevice* pci_dev) -> int {
    uint8_t const COMMON_CAP = virtio_find_cap(pci_dev, VIRTIO_PCI_CAP_COMMON_CFG);
    uint8_t const NOTIFY_CAP = virtio_find_cap(pci_dev, VIRTIO_PCI_CAP_NOTIFY_CFG);
    if (COMMON_CAP == 0 || NOTIFY_CAP == 0) {
        return -ENOTSUP;
    }

    pci::pci_enable_memory_space(pci_dev);
    auto* common_va = map_virtio_cap_region(pci_dev, COMMON_CAP);
    if (common_va == nullptr) {
        return -1;
    }
    auto* cfg = reinterpret_cast<VirtioModernCfg*>(common_va);

    cfg->device_feature_select = 1;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    if ((cfg->device_feature & VIRTIO_F_VERSION_1) == 0) {
        return -ENOTSUP;
    }

    uint32_t notify_off_mult =
        ker::dev::pci::pci_config_read32(pci_dev->bus, pci_dev->slot, pci_dev->function, static_cast<uint8_t>(NOTIFY_CAP + 16));
    auto* notify_va = map_virtio_cap_region(pci_dev, NOTIFY_CAP);
    if (notify_va == nullptr) {
        return -1;
    }

    uint8_t const DEVCFG_CAP = virtio_find_cap(pci_dev, VIRTIO_PCI_CAP_DEVICE_CFG);

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

    if (!prepare_probe(dev)) return fail_probe(dev);
    cfg->device_status = VIRTIO_STATUS_ACKNOWLEDGE;
    cfg->device_status = static_cast<uint8_t>(VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER);

    cfg->device_feature_select = 0;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    uint32_t const FEAT_LO = cfg->device_feature;
    cfg->device_feature_select = 1;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    bool const TRANSLATED = iommu::domain_info(pci_dev).mode == iommu::DomainMode::translated;
    auto const UPPER = platform_features(cfg->device_feature, TRANSLATED);
    if (!UPPER) {
        net_log::error("modern virtio requires VERSION_1 and translated DMA requires ACCESS_PLATFORM; probe refused");
        return fail_probe(dev);
    }

    uint32_t our_lo = 0;
    if ((FEAT_LO & VIRTIO_NET_F_MAC) != 0) {
        our_lo |= VIRTIO_NET_F_MAC;
    }
    if ((FEAT_LO & VIRTIO_NET_F_STATUS) != 0) {
        our_lo |= VIRTIO_NET_F_STATUS;
    }
    bool const MRG_RXBUF_DISABLED = virtio_net_mrg_rxbuf_disabled();
    if (!MRG_RXBUF_DISABLED && (FEAT_LO & VIRTIO_NET_F_MRG_RXBUF) != 0) {
        our_lo |= VIRTIO_NET_F_MRG_RXBUF;
    } else if (MRG_RXBUF_DISABLED && (FEAT_LO & VIRTIO_NET_F_MRG_RXBUF) != 0) {
        net_log::info("virtio-net mergeable RX buffers disabled by cmdline");
    }
    bool const MQ_DISABLED = virtio_net_mq_disabled();
    bool want_mq = !MQ_DISABLED && (CORE_COUNT >= 2) && ((FEAT_LO & VIRTIO_NET_F_CTRL_VQ) != 0) && ((FEAT_LO & VIRTIO_NET_F_MQ) != 0);
    if (want_mq) {
        our_lo |= VIRTIO_NET_F_CTRL_VQ | VIRTIO_NET_F_MQ;
    } else if (MQ_DISABLED && (FEAT_LO & VIRTIO_NET_F_MQ) != 0) {
        net_log::info("virtio-net multiqueue disabled by cmdline");
    }

    cfg->driver_feature_select = 0;
    cfg->driver_feature = our_lo;
    cfg->driver_feature_select = 1;
    cfg->driver_feature = *UPPER;

    cfg->device_status = static_cast<uint8_t>(VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_FEATURES_OK);
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    if ((cfg->device_status & VIRTIO_STATUS_FEATURES_OK) == 0) {
        net_log::error("modern FEATURES_OK rejected");
        return fail_probe(dev);
    }
    bool const PLATFORM = (*UPPER & VIRTIO_F_ACCESS_PLATFORM) != 0;
    if (!iommu::set_platform_dma_ready(pci_dev, PLATFORM)) return fail_probe(dev);
    net_log::info("virtio platform DMA negotiated=%u translated=%u upper=0x%x", PLATFORM, TRANSLATED, *UPPER);
    dev->negotiated_features = our_lo;
    dev->hdr_size = static_cast<uint8_t>(VIRTIO_NET_HDR_SIZE_MODERN);

    // Set up one queue: select, size, alloc, assign addresses, notify addr, enable.
    auto setup_queue = [&](uint16_t q_idx, uint16_t max_size, QueueKind kind) -> Virtqueue* {
        cfg->queue_select = q_idx;
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        uint16_t const HW_SIZE = cfg->queue_size;
        if (HW_SIZE == 0) {
            return nullptr;
        }
        uint16_t const SIZE = std::min({HW_SIZE, max_size, VIRTQ_MAX_SIZE});
        if (managed::layout(SIZE).bytes == 0) return nullptr;
        cfg->queue_size = SIZE;

        auto* vq = virtq_alloc(pci_dev, SIZE, kind);
        if (vq == nullptr) {
            return nullptr;
        }
        vq->queue_index = q_idx;
        virtio_net_set_queue_notifications(vq, false);
        uint16_t const NOTIFY_OFF = cfg->queue_notify_off;
        vq->notify_addr = reinterpret_cast<volatile uint16_t*>(notify_va + (static_cast<size_t>(NOTIFY_OFF) * notify_off_mult));

        // expose() precedes every hardware-visible address write.
        if (!vq->ring.expose()) {
            (void)virtq_retire(vq, false);
            return nullptr;
        }
        auto const LAYOUT = managed::layout(SIZE);
        cfg->queue_desc = vq->ring.submission.address;
        cfg->queue_avail = vq->ring.submission.address + LAYOUT.available;
        cfg->queue_used = vq->ring.submission.address + LAYOUT.used;
        cfg->queue_enable = 1;
        return vq;
    };

    auto& pair0 = dev->queue_pairs.at(0);
    pair0.rxq = setup_queue(rx_queue_index(0), VIRTQ_MAX_SIZE, QueueKind::receive);
    if (pair0.rxq == nullptr) {
        net_log::error("modern RX queue failed");
        return fail_probe(dev);
    }
    pair0.txq = setup_queue(tx_queue_index(0), VIRTQ_MAX_SIZE, QueueKind::transmit);
    if (pair0.txq == nullptr) {
        net_log::error("modern TX queue failed");
        return fail_probe(dev);
    }

    uint8_t target_queue_pairs = SINGLE_QUEUE_PAIRS;
    uint16_t ctrlq_idx = 0;
    if (want_mq) {
        uint16_t max_vq_pairs = 1;
        if (devcfg_va != nullptr) {
            max_vq_pairs = *reinterpret_cast<volatile const uint16_t*>(devcfg_va + 8);
            if (max_vq_pairs != 0 && max_vq_pairs < 0x8000U) {
                ctrlq_idx = static_cast<uint16_t>(2U * max_vq_pairs);
                want_mq = max_vq_pairs >= MIN_MQ_QUEUE_PAIRS;
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
            pair.rxq = setup_queue(rx_queue_index(pair_idx), VIRTQ_MAX_SIZE, QueueKind::receive);
            pair.txq = setup_queue(tx_queue_index(pair_idx), VIRTQ_MAX_SIZE, QueueKind::transmit);
            if (pair.rxq == nullptr || pair.txq == nullptr) {
                net_log::warn("modern queue pair %u setup failed", pair_idx);
                return fail_probe(dev);
            }
            configured_pairs++;
        }
        target_queue_pairs = configured_pairs;
        want_mq = target_queue_pairs >= MIN_MQ_QUEUE_PAIRS;
    }
    bool prepare_control = want_mq;
#ifdef WOS_SELFTEST
    // Rootless socket backends expose one pair. Still exercise the real MQ
    // command (pairs=1) and its DMA lifetime without inventing a second backend.
    if (probe_test != nullptr && ctrlq_idx != 0 && (our_lo & VIRTIO_NET_F_MQ) != 0) prepare_control = true;
#endif
    if (prepare_control) {
        dev->ctrlq = setup_queue(ctrlq_idx, 32, QueueKind::control);
        managed::DmaApi api;
        if (dev->ctrlq == nullptr || dev->ctrlq->size < 4 || !dev->control_buffer.allocate(api, pci_dev, managed::PAGE) ||
            !dev->control_buffer.prepare(api))
            return fail_probe(dev);
    }

    // Every ring, packet slot and control mapping is ready before DMA admission.
    // Passthrough is still explicit until translated domains are enabled.
    if (!pci::pci_set_bus_master(pci_dev, true)) return fail_probe(dev);
#ifdef WOS_SELFTEST
    if (probe_test != nullptr) return selftest_probe_boundary(dev, target_queue_pairs);
#endif

    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector == 0) {
        net_log::error("no free IRQ vector");
        return fail_probe(dev);
    }
    pair0.irq_vector = vector;
    pair0.irq_owned = true;
    uint8_t allocated_pair_vectors = 1;

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
            pair.irq_owned = true;
            allocated_pair_vectors = static_cast<uint8_t>(pair_idx + 1U);
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
        if (!mask_interrupts(dev)) return fail_probe(dev);
        want_mq = false;
        int const MSI_RET = ker::dev::pci::pci_enable_msi(pci_dev, vector, net_cpu_for_pair(CORE_COUNT, 0));
        if (MSI_RET != 0) {
            ker::mod::gates::free_irq(vector);
            vector = pci_dev->interrupt_line + 32;
            pair0.irq_vector = vector;
            pair0.irq_owned = false;
            dev->use_intx = true;
            ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
        }
    }

    uint8_t const RETAINED_PAIR_VECTORS = (want_mq && dev->msix_enabled) ? target_queue_pairs : SINGLE_QUEUE_PAIRS;
    for (uint8_t pair_idx = RETAINED_PAIR_VECTORS; pair_idx < allocated_pair_vectors; ++pair_idx) {
        auto& pair = dev->queue_pairs.at(pair_idx);
        virtio_net_unassign_pair_vectors(dev, pair_idx);
        ker::mod::gates::free_irq(pair.irq_vector);
        pair.irq_owned = false;
        pair.irq_vector = 0;
    }

    dev->configured_queue_pairs = RETAINED_PAIR_VECTORS;
    dev->num_queue_pairs = dev->configured_queue_pairs;

    if ((our_lo & VIRTIO_NET_F_MAC) != 0 && devcfg_va != nullptr) {
        for (size_t i = 0; i < ker::net::proto::MacAddress::SIZE_BYTES; i++) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): virtio device config is MMIO bytes.
            dev->netdev.mac.at(i) = devcfg_va[i];
        }
    }

    if (!publish_irqs(dev)) return fail_published_probe(dev);

    cfg->device_status =
        static_cast<uint8_t>(VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_FEATURES_OK | VIRTIO_STATUS_DRIVER_OK);

    size_t initial_rx_descriptors = 0;
    for (uint8_t pair_idx = 0; pair_idx < dev->configured_queue_pairs; pair_idx++) {
        auto* rxq = dev->queue_pairs.at(pair_idx).rxq;
        if (rxq != nullptr) {
            initial_rx_descriptors += rxq->num_free;
        }
    }
    ker::net::pkt_pool_reserve_for_rx_descriptors(initial_rx_descriptors);
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
                virtio_net_set_pair_notifications(&dev->queue_pairs.at(pair_idx), false);
                virtio_net_unassign_pair_vectors(dev, pair_idx);
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
        ker::net::napi_enable(&pair.napi, net_cpu_for_pair(CORE_COUNT, pair_idx));
        if (pair.napi.worker != nullptr) {
            // Keep notifications suppressed until the NAPI owner drains any
            // startup completions and performs the normal rearm handshake.
            ker::net::napi_schedule(&pair.napi, ker::net::NapiScheduleSource::STARTUP);
        }
    }

    devices.at(device_count++) = dev;

    net_log::info("%s MAC=%02x:%02x:%02x:%02x:%02x:%02x vec=0x%02x%s pairs=%u hdr=%u mrg=%u napi ready", dev->netdev.name.data(),
                  dev->netdev.mac.at(0), dev->netdev.mac.at(1), dev->netdev.mac.at(2), dev->netdev.mac.at(3), dev->netdev.mac.at(4),
                  dev->netdev.mac.at(5), vector, dev->msix_enabled ? " modern msix" : " modern msi/intx", dev->num_queue_pairs,
                  dev->hdr_size, (dev->negotiated_features & VIRTIO_NET_F_MRG_RXBUF) != 0 ? 1U : 0U);

    return 0;
}

auto init_device(ker::dev::pci::PCIDevice* pci_dev) -> int {
    if (shutdown_started.load(std::memory_order_acquire)) return -ENODEV;
    for (size_t i = 0; i < device_count; ++i) {
        if (devices[i]->pci == pci_dev) return devices[i]->admission.closed() ? -ENODEV : 0;
    }
    for (auto* failed = failed_probes; failed != nullptr; failed = failed->failed_probe_next) {
        if (failed->pci == pci_dev) return -1;
    }
    if (device_count >= MAX_VIRTIO_NET_DEVICES) {
        return -1;
    }

    int const MODERN = init_device_modern(pci_dev);
    if (MODERN != -ENOTSUP) return MODERN;
    if (iommu::domain_info(pci_dev).mode == iommu::DomainMode::translated) {
        net_log::error("legacy virtio refused in translated DMA mode: ACCESS_PLATFORM unavailable");
        return -1;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): PCI BAR0 is fixed config-space layout.
    auto io_base = static_cast<uint16_t>(pci_dev->bar[0] & ~0x3U);
    if (io_base == 0) {
        net_log::error("BAR0 is zero");
        return -1;
    }

    uint16_t cmd = ker::dev::pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND);
    cmd |= ker::dev::pci::PCI_COMMAND_IO_SPACE;
    ker::dev::pci::pci_config_write16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND, cmd);

    auto* dev = new (std::nothrow) VirtIONetDevice{};
    if (dev == nullptr) return -1;
    init_queue_pair_contexts(dev);
    dev->pci = pci_dev;
    dev->io_base = io_base;
    if (!prepare_probe(dev)) return fail_probe(dev);

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
    if ((DEV_FEATURES & VIRTIO_NET_F_MRG_RXBUF) != 0 && !virtio_net_mrg_rxbuf_disabled()) {
        our_features |= VIRTIO_NET_F_MRG_RXBUF;
    }
    ::outl(io_base + VIRTIO_REG_GUEST_FEATURES, our_features);

    dev->hdr_size = virtio_net_hdr_size_for_features(our_features);
    dev->negotiated_features = our_features;
    auto& pair0 = dev->queue_pairs.at(0);

    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
    uint16_t rxq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (rxq_size == 0) {
        net_log::error("RX queue size is 0");
        return fail_probe(dev);
    }
    if (managed::layout(rxq_size).bytes == 0) return fail_probe(dev);

    pair0.rxq = virtq_alloc(pci_dev, rxq_size, QueueKind::receive, managed::LEGACY_QUEUE_MASK);
    if (pair0.rxq == nullptr) {
        net_log::error("failed to alloc RX queue");
        return fail_probe(dev);
    }
    pair0.rxq->io_base = io_base;
    pair0.rxq->queue_index = rx_queue_index(0);
    virtio_net_set_queue_notifications(pair0.rxq, false);

    if (!pair0.rxq->ring.expose()) return fail_probe(dev);
    uint64_t const RXQ_PHYS = pair0.rxq->ring.submission.address;
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(RXQ_PHYS / 4096));

    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
    uint16_t txq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (txq_size == 0) {
        net_log::error("TX queue size is 0");
        return fail_probe(dev);
    }
    if (managed::layout(txq_size).bytes == 0) return fail_probe(dev);

    pair0.txq = virtq_alloc(pci_dev, txq_size, QueueKind::transmit, managed::LEGACY_QUEUE_MASK);
    if (pair0.txq == nullptr) {
        net_log::error("failed to alloc TX queue");
        return fail_probe(dev);
    }
    pair0.txq->io_base = io_base;
    pair0.txq->queue_index = tx_queue_index(0);
    virtio_net_set_queue_notifications(pair0.txq, false);

    if (!pair0.txq->ring.expose()) return fail_probe(dev);
    uint64_t const TXQ_PHYS = pair0.txq->ring.submission.address;
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(TXQ_PHYS / 4096));

    if (!pci::pci_set_bus_master(pci_dev, true)) return fail_probe(dev);
#ifdef WOS_SELFTEST
    if (probe_test != nullptr) return selftest_probe_boundary(dev, 1);
#endif

    // Try MSI-X first (needed for per-pair CPU steering), then MSI, then INTx.
    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector == 0) {
        net_log::error("no free IRQ vector");
        return fail_probe(dev);
    }
    pair0.irq_vector = vector;
    pair0.irq_owned = true;

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
        if (!mask_interrupts(dev)) return fail_probe(dev);
        int const MSI_RET = ker::dev::pci::pci_enable_msi(pci_dev, vector, net_cpu_for_pair(CORE_COUNT, 0));
        if (MSI_RET != 0) {
            ker::mod::gates::free_irq(vector);
            vector = pci_dev->interrupt_line + 32;
            pair0.irq_vector = vector;
            pair0.irq_owned = false;
            dev->use_intx = true;
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

    if (!publish_irqs(dev)) return fail_published_probe(dev);

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_DRIVER_OK);

    ker::net::pkt_pool_reserve_for_rx_descriptors(pair0.rxq->num_free);
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
        ker::net::napi_enable(&pair.napi, net_cpu_for_pair(CORE_COUNT, pair_idx));
        if (pair.napi.worker != nullptr) {
            // Keep notifications suppressed until the NAPI owner drains any
            // startup completions and performs the normal rearm handshake.
            ker::net::napi_schedule(&pair.napi, ker::net::NapiScheduleSource::STARTUP);
        }
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

        CallbackGuard const GUARD(dev->admission);
        if (!GUARD) continue;

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
            row.napi_polls = pair.napi.poll_count.load(std::memory_order_relaxed);
            row.napi_completes = pair.napi.complete_count.load(std::memory_order_relaxed);
            row.napi_diag = ker::net::napi_diag_snapshot(&pair.napi);
            if (pair.napi.worker != nullptr) {
                row.napi_worker_pid = pair.napi.worker->pid;
                row.napi_worker_cpu = pair.napi.worker->load_cpu();
            }
            fill_virtqueue_diag(pair.rxq, row.rx);
            fill_virtqueue_diag(pair.txq, row.tx);
        }
    }
    return count;
}

auto virtio_net_shutdown() -> int {
    if (ker::mod::sched::can_query_current_task() && !ker::mod::mm::phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    shutdown_started.store(true, std::memory_order_release);
    int result = 0;
    for (size_t i = 0; i < device_count; ++i) {
        int const RET = retire_device(devices[i]);
        if (RET != 0) result = RET;
    }
    return result;
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
#ifdef WOS_SELFTEST
    if (found != 0 && cmdline_has_token(ker::init::get_kernel_cmdline(), "--napi-irq-test")) selftest_napi_start(devices[0]);
    if (found != 0 && cmdline_has_token(ker::init::get_kernel_cmdline(), "--virtio-test-live")) selftest_live_start(devices[0]);
#endif

    return found;
}

#ifdef WOS_SELFTEST
auto virtio_selftest_probe_unwind() -> SelftestProbeResult {
    SelftestProbeResult result;
    bool const WITHHOLD = cmdline_has_token(ker::init::get_kernel_cmdline(), "--virtio-test-withhold-reset");
    result.reject_platform_requested = cmdline_has_token(ker::init::get_kernel_cmdline(), "--virtio-test-reject-platform");
    result.withhold_reset_proof = WITHHOLD;
    result.packets_requested = cmdline_has_token(ker::init::get_kernel_cmdline(), "--virtio-test-packets");
    result.terminal_requested = cmdline_has_token(ker::init::get_kernel_cmdline(), "--virtio-test-terminal");
    if (device_count != 0 || probe_test != nullptr) return result;
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr || dev->vendor_id != VIRTIO_VENDOR ||
            (dev->device_id != VIRTIO_NET_MODERN && !(dev->device_id == VIRTIO_NET_LEGACY && dev->class_code == pci::PCI_CLASS_NETWORK)))
            continue;
        result.found = true;
        result.before = dma::stats(dev);
        probe_test = &result;
        probe_test_withhold_reset = WITHHOLD;
        (void)init_device(dev);
        probe_test = nullptr;
        probe_test_withhold_reset = false;
        result.after = dma::stats(dev);
        result.platform_admission_closed = iommu::domain_info(dev).mode != iommu::DomainMode::translated ||
                                           (!iommu::bus_master_allowed(dev) && !pci::pci_set_bus_master(dev, true));
        result.bme_disabled =
            (pci::pci_config_read16(dev->bus, dev->slot, dev->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) == 0;
        net_log::info("virtio DMA probe prepared=%u ctrl=%u ack=%u reset_observed=%u withheld=%u regions=%zu->%zu pages=%zu->%zu q=%zu",
                      result.prepared, result.control_present, result.control_ack, result.reset_observed, WITHHOLD, result.mapped.regions,
                      result.after.regions, result.mapped.backing_pages, result.after.backing_pages, result.after.quarantined);
        net_log::info("virtio platform admission closed=%u rejection_requested=%u bme_off=%u", result.platform_admission_closed,
                      result.reject_platform_requested, result.bme_disabled);
        net_log::info("virtio packet DMA requested=%u verified=%u pool=%zu->%zu streaming=%zu", result.packets_requested,
                      result.packets_verified, result.packet_pool_before, result.packet_pool_after, result.mapped.streaming);
        break;
    }
    return result;
}
#endif

#ifdef WOS_SELFTEST
auto virtio_selftest_queue_affinity_batch() -> bool {
    auto* dev = new VirtIONetDevice{};
    auto* first = new ker::mod::sched::task::Task{};
    auto* second = new ker::mod::sched::task::Task{};
    dev->num_queue_pairs = 1;
    dev->msix_enabled = true;
    dev->queue_pairs[0].napi.worker = first;
    dev->queue_pairs[0].irq_vector = 40;
    dev->queue_pairs[1].irq_vector = 41;
    unsigned admissions = 0;
    unsigned interrupts = 0;
    bool accept = false;
    bool mapping_ok = true;
    auto pin = [&](std::span<const ker::mod::sched::KernelWorkerCpuPin> requests) {
        ++admissions;
        mapping_ok = requests.size() == 2 && requests[0].worker == first && requests[0].cpu == 0 && requests[1].worker == second &&
                     requests[1].cpu == 2 && mapping_ok;
        return accept;
    };
    auto irq = [&](uint32_t pair, uint8_t vector, uint64_t cpu) {
        mapping_ok = pair == interrupts && vector == 40 + pair && cpu == pair * 2 && mapping_ok;
        ++interrupts;
    };
    bool ok = apply_queue_cpu_mask(*dev, 5, pin, irq) == -EINVAL && admissions == 0 && interrupts == 0;
    dev->num_queue_pairs = 2;
    ok = apply_queue_cpu_mask(*dev, 5, pin, irq) == -EBUSY && admissions == 0 && interrupts == 0 && ok;
    dev->queue_pairs[1].napi.worker = second;
    ok = apply_queue_cpu_mask(*dev, 5, pin, irq) == -EBUSY && admissions == 1 && interrupts == 0 && ok;
    accept = true;
    ok = apply_queue_cpu_mask(*dev, 5, pin, irq) == 0 && admissions == 2 && interrupts == 2 && mapping_ok && ok;
    ok = apply_queue_cpu_mask(*dev, 0, pin, irq) == 0 && admissions == 2 && interrupts == 2 && ok;
    delete second;
    delete first;
    delete dev;
    return ok;
}
#endif

}  // namespace ker::dev::virtio
