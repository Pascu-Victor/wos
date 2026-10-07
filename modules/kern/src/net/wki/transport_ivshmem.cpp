#include "transport_ivshmem.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/ivshmem/callback.hpp>
#include <dev/ivshmem/ivshmem_net.hpp>
#include <dev/ivshmem/selftest.hpp>
#include <dev/ivshmem/shared_ring.hpp>
#include <dev/pci.hpp>
#include <memory>
#include <net/packet.hpp>
#include <net/wki/chaos.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <net/wki/secure_rdma.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/fw/qemu_fw_cfg.hpp>

#include "platform/ktime/ktime.hpp"

namespace ker::net::wki {

namespace {
namespace shared_ring = dev::ivshmem::shared_ring;

// -----------------------------------------------------------------------------
// BAR2 shared memory layout for WKI ivshmem transport
// -----------------------------------------------------------------------------
//
// [0..63]                WKI ivshmem header
// [64..64+64KB-1]        VM0->VM1 message ring
// [64+64KB..64+128KB-1]  VM1->VM0 message ring
// [64+128KB..end]        reserved legacy RDMA area (no service authority)
//

constexpr uint32_t WKI_IVSHMEM_MAGIC = 0x574B4944;  // "WKID"
constexpr uint32_t WKI_IVSHMEM_VERSION = 1;
constexpr size_t WKI_IVSHMEM_HEADER_SIZE = 64;
constexpr size_t WKI_IVSHMEM_RING_SIZE = static_cast<const size_t>(64 * 1024);  // 64KB per ring
constexpr size_t WKI_IVSHMEM_RING_AREA = 2 * WKI_IVSHMEM_RING_SIZE;
// Message frames include the WKI header and authentication trailer. VFS can
// use the complete common frame capacity even when direct RDMA is disabled.
using IvshmemFrame = std::array<uint8_t, WKI_MAX_FRAME_SIZE>;
static_assert(WKI_MAX_FRAME_SIZE <= UINT16_MAX);
static_assert(WKI_MAX_FRAME_SIZE + 8 < WKI_IVSHMEM_RING_SIZE);
constexpr size_t WKI_IVSHMEM_RDMA_OFFSET = WKI_IVSHMEM_HEADER_SIZE + WKI_IVSHMEM_RING_AREA;

constexpr size_t IVSHMEM_SHMEM_SIZE = static_cast<const size_t>(16 * 1024 * 1024);  // 16MB default
constexpr size_t WKI_RDMA_REGION_SIZE = IVSHMEM_SHMEM_SIZE - WKI_IVSHMEM_RDMA_OFFSET;

constexpr size_t RDMA_PAGE_SIZE = 4096;
constexpr size_t RDMA_MAX_PAGES = WKI_RDMA_REGION_SIZE / RDMA_PAGE_SIZE;

// -----------------------------------------------------------------------------
// Shared memory header (at offset 0 of BAR2)
// -----------------------------------------------------------------------------

struct WkiIvshmemHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t ring0_offset;
    uint32_t ring0_size;
    uint32_t ring1_offset;
    uint32_t ring1_size;
    uint32_t rdma_offset;
    uint32_t rdma_size;
    uint32_t vm_id;
    uint32_t peer_ready;
    std::array<uint8_t, 24> reserved;
} __attribute__((packed));

static_assert(sizeof(WkiIvshmemHeader) == WKI_IVSHMEM_HEADER_SIZE, "WkiIvshmemHeader must be 64 bytes");

// -----------------------------------------------------------------------------
// Ring buffer (reused from ivshmem_net design)
// -----------------------------------------------------------------------------

using WkiRing = shared_ring::View;

// -----------------------------------------------------------------------------
// Transport private state
// -----------------------------------------------------------------------------

struct IvshmemTransportPrivate {
    dev::ivshmem::PciResources resources{};
    dev::pci::PCIDevice* pci;
    volatile uint32_t* regs;  // BAR0
    uint8_t* shmem;           // BAR2
    size_t shmem_size;
    uint32_t my_vm_id;

    WkiRing tx_ring;
    WkiRing rx_ring;

    std::atomic<WkiRxHandler> rx_handler{nullptr};
    std::atomic_flag rx_drain_active = ATOMIC_FLAG_INIT;
    std::atomic_flag tx_active = ATOMIC_FLAG_INIT;
    std::atomic<bool> ring_failed{false};
    uint16_t direct_peer = WKI_NODE_INVALID;
};

WkiTransport s_ivshmem_transport;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
IvshmemTransportPrivate s_ivshmem_priv;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_ivshmem_initialized{false};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
#ifdef WOS_SELFTEST
auto selftest_late_calls() -> bool;
#endif

auto configured_vm_role() -> int32_t {
    char value = 0;
    int const READ = platform::fw::fw_cfg_read_file("opt/wos/wki-ivshmem-role", &value, sizeof(value));
    return READ == 1 && (value == '0' || value == '1') ? value - '0' : -1;
}

auto configured_direct_peer() -> uint16_t {
    std::array<char, 6> value{};
    int const READ = platform::fw::fw_cfg_read_file("opt/wos/wki-ivshmem-peer", value.data(), value.size() - 1);
    if (READ <= 0 || static_cast<size_t>(READ) >= value.size()) {
        return WKI_NODE_INVALID;
    }
    uint32_t peer = 0;
    for (int index = 0; index < READ; ++index) {
        if (value.at(static_cast<size_t>(index)) < '0' || value.at(static_cast<size_t>(index)) > '9') {
            return WKI_NODE_INVALID;
        }
        peer = (peer * 10U) + static_cast<uint32_t>(value.at(static_cast<size_t>(index)) - '0');
        if (peer >= WKI_NODE_BROADCAST) {
            return WKI_NODE_INVALID;
        }
    }
    return peer == 0 ? WKI_NODE_INVALID : static_cast<uint16_t>(peer);
}

// -----------------------------------------------------------------------------
// Ring operations
// -----------------------------------------------------------------------------

#ifdef WOS_SELFTEST
auto ring_write(WkiRing* ring, const uint8_t* data, size_t bytes) -> int {
    if (data == nullptr || bytes > WKI_MAX_FRAME_SIZE) return -1;
    return shared_ring::write(*ring, ring->size, std::span(data, bytes)).error == shared_ring::Error::none ? 0 : -1;
}
auto ring_read(WkiRing* ring, uint8_t* data, size_t bytes) -> uint16_t {
    auto const RESULT = shared_ring::read(*ring, ring->size, std::span(data, bytes));
    return RESULT.error == shared_ring::Error::none ? RESULT.bytes : 0;
}
#endif
auto transmit(IvshmemTransportPrivate* priv, const uint8_t* data, size_t bytes) -> int {
    if (data == nullptr || bytes == 0 || bytes > WKI_MAX_FRAME_SIZE || priv->ring_failed.load(std::memory_order_acquire) ||
        priv->tx_active.test_and_set(std::memory_order_acquire))
        return -1;
    auto const RESULT = shared_ring::write(priv->tx_ring, priv->tx_ring.size, std::span(data, bytes));
    priv->tx_active.clear(std::memory_order_release);
    if (RESULT.error == shared_ring::Error::index || RESULT.error == shared_ring::Error::geometry) {
        priv->ring_failed.store(true, std::memory_order_release);
        priv->resources.mask_vector(true);
    }
    if (RESULT.error != shared_ring::Error::none) return -1;
    priv->resources.ring(priv->my_vm_id == 0 ? 1 : 0);
    return 0;
}

// -----------------------------------------------------------------------------
// WkiTransport operations
// -----------------------------------------------------------------------------

auto ivshmem_wki_tx(WkiTransport* self, uint16_t /*neighbor_id*/, const void* data, uint16_t len) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);
    if (priv == nullptr) {
        return -1;
    }

    dev::ivshmem::CallbackGuard const GUARD(priv->resources);
    if (!GUARD) return -1;
    return transmit(priv, static_cast<const uint8_t*>(data), len);
}

auto ivshmem_secure_record(const void* data, uint16_t len) -> bool {
    if (data == nullptr || len < rdma_protocol::HEADER_BYTES || len > rdma_protocol::RECORD_BYTES) return false;
    uint32_t magic = 0;
    std::memcpy(&magic, data, sizeof(magic));
    return magic == rdma_protocol::MAGIC;
}

auto ivshmem_secure_tx(WkiTransport* self, uint16_t neighbor_id, const void* data, uint16_t len) -> int {
    if (self != &s_ivshmem_transport || !ivshmem_secure_record(data, len)) return -1;
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);
    if (priv != &s_ivshmem_priv || priv->direct_peer == WKI_NODE_INVALID || neighbor_id != priv->direct_peer ||
        self->secure_kind != rdma_protocol::Transport::IVSHMEM)
        return -1;
    dev::ivshmem::CallbackGuard const GUARD(priv->resources);
    if (!GUARD) return -1;
    return transmit(priv, static_cast<const uint8_t*>(data), len);
}

auto ivshmem_wki_tx_pkt(WkiTransport* self, uint16_t /*neighbor_id*/, net::PacketBuffer* pkt) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);
    if (priv == nullptr) {
        net::pkt_free(pkt);
        return -1;
    }

    dev::ivshmem::CallbackGuard const GUARD(priv->resources);
    if (!GUARD) {
        net::pkt_free(pkt);
        return -1;
    }
    int const RET = pkt != nullptr && pkt->len <= net::PKT_BUF_SIZE - pkt->headroom() ? transmit(priv, pkt->data, pkt->len) : -1;
    net::pkt_free(pkt);
    return RET;
}

void ivshmem_wki_set_rx_handler(WkiTransport* self, WkiRxHandler handler) {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);
    dev::ivshmem::CallbackGuard const GUARD(priv->resources);
    if (GUARD) priv->rx_handler.store(handler, std::memory_order_release);
}

// Legacy raw operations cannot authorize access to peer-writable BAR memory.
// Reject even before inspecting caller pointers or transport lifetime state.
auto ivshmem_wki_rdma_register_region(WkiTransport* /*self*/, uint64_t /*phys_addr*/, uint32_t /*size*/, uint32_t* /*rkey*/) -> int {
    return -1;
}

auto ivshmem_wki_rdma_read(WkiTransport* /*self*/, uint16_t /*neighbor_id*/, uint32_t /*rkey*/, uint64_t /*remote_offset*/,
                           void* /*local_buf*/, uint32_t /*len*/) -> int {
    return -1;
}

auto ivshmem_wki_rdma_write(WkiTransport* /*self*/, uint16_t /*neighbor_id*/, uint32_t /*rkey*/, uint64_t /*remote_offset*/,
                            const void* /*local_buf*/, uint32_t /*len*/) -> int {
    return -1;
}

auto ivshmem_wki_doorbell(WkiTransport* /*self*/, uint16_t /*neighbor_id*/, uint32_t /*value*/) -> int { return -1; }

// -----------------------------------------------------------------------------
// Task RX dispatch and minimal doorbell IRQ
// -----------------------------------------------------------------------------

auto drain_rx_ring(IvshmemTransportPrivate* priv, uint16_t budget) -> uint16_t {
    if (priv == nullptr) return 0;
    dev::ivshmem::CallbackGuard const GUARD(priv->resources);
    if (!GUARD || budget == 0 || priv->ring_failed.load(std::memory_order_acquire) ||
        priv->rx_drain_active.test_and_set(std::memory_order_acquire)) {
        return 0;
    }

    uint16_t processed = 0;
    // Reserved shared bytes are hostile input, never IRQ callback authority.
    // Both receive paths borrow only a complete private ring snapshot.
    IvshmemFrame buf{};
    while (processed < budget) {
        auto const RESULT = shared_ring::read(priv->rx_ring, priv->rx_ring.size, std::span(buf));
        if (RESULT.error != shared_ring::Error::none) {
            if (RESULT.error != shared_ring::Error::empty) priv->ring_failed.store(true, std::memory_order_release);
            break;
        }
        uint16_t const LEN = RESULT.bytes;

        WkiRxMetadata const metadata = {
            .has_direct_peer = priv->direct_peer != WKI_NODE_INVALID,
            .direct_peer = priv->direct_peer,
        };
        uint32_t magic = 0;
        if (LEN >= sizeof(magic)) std::memcpy(&magic, buf.data(), sizeof(magic));
        if (magic == rdma_protocol::MAGIC) {
            if (ivshmem_secure_record(buf.data(), LEN) && metadata.has_direct_peer)
                wki_chaos_secure_rx_ingress(&s_ivshmem_transport, buf.data(), LEN, &metadata);
        } else if (auto handler = priv->rx_handler.load(std::memory_order_acquire); handler != nullptr) {
            handler(&s_ivshmem_transport, buf.data(), LEN, &metadata);
        }
        ++processed;
    }
    priv->rx_drain_active.clear(std::memory_order_release);
    return processed;
}

void ivshmem_wki_irq(uint8_t /*vector*/, void* data) {
    auto* priv = static_cast<IvshmemTransportPrivate*>(data);
    if (priv == nullptr) {
        return;
    }

    dev::ivshmem::CallbackGuard const GUARD(priv->resources);
    if (!GUARD) return;
#ifdef WOS_SELFTEST
    priv->resources.test_irqs.fetch_add(1, std::memory_order_relaxed);
#endif
    // Only mask here. The existing peer task polls shared memory and re-arms
    // this vector; WKI protocol/ACK processing must never run in IRQ context.
    priv->resources.mask_vector(true);
}

}  // namespace

// -----------------------------------------------------------------------------
// Public legacy API - raw BAR authority is unavailable
// -----------------------------------------------------------------------------

auto wki_ivshmem_rdma_alloc(uint32_t /*size*/) -> int64_t { return -1; }

void wki_ivshmem_rdma_free(int64_t /*offset*/, uint32_t /*size*/) {}

auto wki_ivshmem_rdma_ptr(int64_t /*offset*/) -> void* { return nullptr; }

// -----------------------------------------------------------------------------
// Public legacy mailbox API - unauthenticated shared bytes cannot publish IRQs
// -----------------------------------------------------------------------------

void wki_ivshmem_irq_mailbox_write(WkiTransport* /*transport*/, uint16_t /*device_id*/, uint16_t /*irq_vector*/, uint32_t /*irq_status*/) {}

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_ivshmem_transport_init() {
#ifdef WOS_SELFTEST
    if (dev::ivshmem::pci_selftest_reserved()) return;
#endif
    if (s_ivshmem_priv.resources.callbacks.closed() || s_ivshmem_initialized || s_ivshmem_priv.resources.device != nullptr) {
        return;
    }

    // Probe PCI for an ivshmem device not claimed by ivshmem_net
    dev::pci::PCIDevice* found_dev = nullptr;
    size_t const COUNT = dev::pci::pci_device_count();

    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = dev::pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        if (dev->vendor_id == dev::ivshmem::IVSHMEM_VENDOR && dev->device_id == dev::ivshmem::IVSHMEM_DEVICE) {
            // Skip devices already claimed by the networking driver
            if (dev::ivshmem::ivshmem_net_is_claimed(dev)) {
                continue;
            }
            found_dev = dev;
            break;
        }
    }

    if (found_dev == nullptr) {
        ker::mod::dbg::log("[WKI] No unclaimed ivshmem device found for message transport");
        return;
    }

    if (!s_ivshmem_priv.resources.prepare(found_dev, IVSHMEM_SHMEM_SIZE)) {
        ker::mod::dbg::log("[WKI] ivshmem: PCI resource preparation failed");
        return;
    }
    auto* regs = s_ivshmem_priv.resources.regs;
    auto* shmem = s_ivshmem_priv.resources.shared;

    // Set up private state
    s_ivshmem_priv.pci = found_dev;
    s_ivshmem_priv.regs = regs;
    s_ivshmem_priv.shmem = shmem;
    s_ivshmem_priv.shmem_size = s_ivshmem_priv.resources.shared_size;
    s_ivshmem_priv.rx_handler = nullptr;
    s_ivshmem_priv.direct_peer = configured_direct_peer();

    // Publish magic last and retain a validated private layout. Attaching to
    // an existing valid header never resets a live peer's indices/mailbox.
    auto* hdr = reinterpret_cast<WkiIvshmemHeader*>(shmem);
    auto* magic = reinterpret_cast<uint32_t*>(shmem);
    int32_t const CONFIGURED_ROLE = configured_vm_role();
    auto const HEADER_DEADLINE = wki_future_deadline_us(ker::mod::time::get_us(), 5'000'000);
    shared_ring::Claim claim;
    do {
        claim = shared_ring::claim(magic, reinterpret_cast<uint32_t*>(shmem + offsetof(WkiIvshmemHeader, vm_id)), WKI_IVSHMEM_MAGIC,
                                   CONFIGURED_ROLE);
        if (claim.state != shared_ring::ClaimState::wait) break;
        asm volatile("pause" ::: "memory");
    } while (ker::mod::time::get_us() < HEADER_DEADLINE);
    if (claim.state == shared_ring::ClaimState::wait || claim.state == shared_ring::ClaimState::refused) {
        ker::mod::dbg::log("[WKI] ivshmem: shared header claim refused or timed out");
        return;
    }
    if (claim.state == shared_ring::ClaimState::initialize) {
        memset(shmem + 4, 0, WKI_IVSHMEM_HEADER_SIZE - 4);
        hdr->vm_id = claim.role;
        hdr->version = WKI_IVSHMEM_VERSION;
        hdr->ring0_offset = WKI_IVSHMEM_HEADER_SIZE;
        hdr->ring0_size = WKI_IVSHMEM_RING_SIZE;
        hdr->ring1_offset = WKI_IVSHMEM_HEADER_SIZE + WKI_IVSHMEM_RING_SIZE;
        hdr->ring1_size = WKI_IVSHMEM_RING_SIZE;
        hdr->rdma_offset = WKI_IVSHMEM_RDMA_OFFSET;
        hdr->rdma_size = WKI_RDMA_REGION_SIZE;
        memset(shmem + WKI_IVSHMEM_HEADER_SIZE, 0, 8);
        memset(shmem + WKI_IVSHMEM_HEADER_SIZE + WKI_IVSHMEM_RING_SIZE, 0, 8);
        shared_ring::store_word(magic, WKI_IVSHMEM_MAGIC);
    }
    auto const SHARED = std::span(shmem, s_ivshmem_priv.resources.shared_size);
    auto const LAYOUT = shared_ring::snapshot(SHARED, WKI_IVSHMEM_MAGIC, true);
    if (!LAYOUT || LAYOUT->extra.bytes < 2 * RDMA_PAGE_SIZE || LAYOUT->extra.bytes / RDMA_PAGE_SIZE > RDMA_MAX_PAGES) {
        ker::mod::dbg::log("[WKI] ivshmem: shared ring/RDMA layout refused");
        return;
    }
    s_ivshmem_priv.my_vm_id = claim.role;
    // Set up TX/RX rings from the validated copy only.
    auto const TX = shared_ring::bind(SHARED, LAYOUT->rings[claim.role]);
    auto const RX = shared_ring::bind(SHARED, LAYOUT->rings[1 - claim.role]);
    if (!TX || !RX) return;
    s_ivshmem_priv.tx_ring = *TX;
    s_ivshmem_priv.rx_ring = *RX;
    if (shared_ring::valid(TX->size, shared_ring::load_word(TX->head_ptr), shared_ring::load_word(TX->tail_ptr)) !=
            shared_ring::Error::none ||
        shared_ring::valid(RX->size, shared_ring::load_word(RX->head_ptr), shared_ring::load_word(RX->tail_ptr)) !=
            shared_ring::Error::none)
        return;
    auto* ready = reinterpret_cast<uint32_t*>(shmem + offsetof(WkiIvshmemHeader, peer_ready));
    if (claim.role == 1) shared_ring::store_word(ready, 1);

    // VM0: poll for peer_ready with 5s timeout
    if (s_ivshmem_priv.my_vm_id == 0) {
        constexpr uint64_t PEER_READY_TIMEOUT_US = 5'000'000;
        uint64_t const DEADLINE = wki_future_deadline_us(ker::mod::time::get_us(), PEER_READY_TIMEOUT_US);
        while (shared_ring::load_word(ready) == 0) {
            if (ker::mod::time::get_us() >= DEADLINE) {
                ker::mod::dbg::log("[WKI] ivshmem: peer_ready timeout - continuing without peer");
                break;
            }
            asm volatile("pause" ::: "memory");
        }
        if (shared_ring::load_word(ready) != 0) {
            ker::mod::dbg::log("[WKI] ivshmem: peer is ready");
        }
    }

    // Fill in transport struct
    s_ivshmem_transport.name = "wki-ivshmem";
    s_ivshmem_transport.mtu = static_cast<uint16_t>(WKI_MAX_FRAME_SIZE - WKI_HEADER_SIZE);
    s_ivshmem_transport.rdma_capable = false;
    bool const SECURE_PEER = s_ivshmem_priv.direct_peer != WKI_NODE_INVALID;
    s_ivshmem_transport.secure_kind = SECURE_PEER ? rdma_protocol::Transport::IVSHMEM : rdma_protocol::Transport::NONE;
    s_ivshmem_transport.secure_tx = SECURE_PEER ? ivshmem_secure_tx : nullptr;
    s_ivshmem_transport.private_data = &s_ivshmem_priv;
    s_ivshmem_transport.tx = ivshmem_wki_tx;
    s_ivshmem_transport.tx_pkt = ivshmem_wki_tx_pkt;
    s_ivshmem_transport.set_rx_handler = ivshmem_wki_set_rx_handler;
    s_ivshmem_transport.rdma_register_region = ivshmem_wki_rdma_register_region;
    // No raw BAR registration or revocation is supported. A future secure
    // adapter must own private backing and exact authenticated grants.
    s_ivshmem_transport.rdma_unregister_region = nullptr;
    s_ivshmem_transport.rdma_read = ivshmem_wki_rdma_read;
    s_ivshmem_transport.rdma_write = ivshmem_wki_rdma_write;
    s_ivshmem_transport.doorbell = ivshmem_wki_doorbell;
    s_ivshmem_transport.next = nullptr;

    // Private state is complete before any MSI-X source can call its handler.
    if (!s_ivshmem_priv.resources.activate(ivshmem_wki_irq, &s_ivshmem_priv, "wki-ivshmem")) {
        ker::mod::dbg::log("[WKI] ivshmem: PCI activation failed");
        return;
    }

    // Register with WKI core
    wki_transport_register(&s_ivshmem_transport);

    s_ivshmem_initialized = true;
    ker::mod::dbg::log("[WKI] ivshmem message transport initialized (vm_id=%u, raw RDMA disabled)", s_ivshmem_priv.my_vm_id);
#ifdef WOS_SELFTEST
    dev::ivshmem::selftest_start(&s_ivshmem_priv.resources, "wki", wki_ivshmem_transport_shutdown, selftest_late_calls, [](bool acquire) {
        if (acquire) {
            g_wki.transport_lock.lock();
            s_ivshmem_transport.chaos_in_flight.fetch_add(1, std::memory_order_acq_rel);
            g_wki.transport_lock.unlock();
        } else {
            s_ivshmem_transport.chaos_in_flight.fetch_sub(1, std::memory_order_release);
        }
    });
#endif
}

auto wki_ivshmem_transport_poll_required() -> bool {
    if (!s_ivshmem_initialized.load(std::memory_order_acquire)) return false;
    dev::ivshmem::CallbackGuard const GUARD(s_ivshmem_priv.resources);
    return GUARD && s_ivshmem_initialized.load(std::memory_order_acquire) && !s_ivshmem_priv.ring_failed.load(std::memory_order_acquire);
}

auto wki_ivshmem_transport_poll(uint16_t budget) -> uint16_t {
    dev::ivshmem::CallbackGuard const GUARD(s_ivshmem_priv.resources);
    if (!GUARD || !s_ivshmem_initialized.load(std::memory_order_acquire)) return 0;
    auto const PROCESSED = drain_rx_ring(&s_ivshmem_priv, budget);
    s_ivshmem_priv.resources.mask_vector(s_ivshmem_priv.ring_failed.load(std::memory_order_acquire));
    return PROCESSED;
}

auto wki_ivshmem_transport_shutdown() -> int {
    // Registry leases use this lock; chaos leases use the inner chaos lock.
    // Closing under both excludes a check-then-increment after a zero snapshot.
    g_wki.transport_lock.lock();
    wki_chaos_transport_close(&s_ivshmem_transport);
    g_wki.transport_lock.unlock();
    return s_ivshmem_priv.resources.retire(
        [](void*) {
            s_ivshmem_priv.rx_handler.store(nullptr, std::memory_order_release);
            if (s_ivshmem_initialized.exchange(false, std::memory_order_acq_rel)) wki_transport_unregister(&s_ivshmem_transport);
        },
        nullptr, [](void*) { return s_ivshmem_transport.chaos_in_flight.load(std::memory_order_acquire) == 0; });
}

#ifdef WOS_SELFTEST
namespace {
auto selftest_late_calls() -> bool {
    auto* transport = &s_ivshmem_transport;
    auto& priv = s_ivshmem_priv;
    std::array<uint8_t, 64> bytes{};
    std::array<uint8_t, 24> mailbox{};
    std::memcpy(mailbox.data(), priv.shmem + offsetof(WkiIvshmemHeader, reserved), mailbox.size());
    auto const HANDLER = priv.rx_handler.load();
    auto const IRQS = priv.resources.test_irqs.load();
    auto const POOL = net::pkt_pool_free_count();
    auto* pkt = net::pkt_alloc_tx();
    if (pkt == nullptr) return false;
    bool const TX = ivshmem_wki_tx_pkt(transport, 2, pkt) == -1 && ivshmem_wki_tx(transport, 2, bytes.data(), bytes.size()) == -1;
    uint32_t rkey = UINT32_MAX;
    bool const RDMA = ivshmem_wki_rdma_register_region(transport, 0, bytes.size(), &rkey) == -1 && rkey == UINT32_MAX &&
                      ivshmem_wki_rdma_read(transport, 2, 4096, 0, bytes.data(), bytes.size()) == -1 &&
                      ivshmem_wki_rdma_write(transport, 2, 4096, 0, bytes.data(), bytes.size()) == -1 &&
                      wki_ivshmem_rdma_alloc(4096) == -1 && wki_ivshmem_rdma_ptr(4096) == nullptr;
    wki_ivshmem_rdma_free(4096, 4096);
    wki_ivshmem_irq_mailbox_write(transport, 1, 2, 3);
    ivshmem_wki_set_rx_handler(transport, [](WkiTransport*, const void*, uint16_t, const WkiRxMetadata*) {});
    ivshmem_wki_irq(0, &priv);
    return TX && RDMA && ivshmem_wki_doorbell(transport, 2, 0) == -1 && wki_ivshmem_transport_poll(64) == 0 &&
           std::memcmp(mailbox.data(), priv.shmem + offsetof(WkiIvshmemHeader, reserved), mailbox.size()) == 0 &&
           priv.rx_handler.load() == HANDLER && priv.resources.test_irqs.load() == IRQS && net::pkt_pool_free_count() == POOL;
}
}  // namespace

auto wki_ivshmem_selftest_complete_frames() -> bool {
    std::unique_ptr<std::array<uint8_t, WKI_IVSHMEM_RING_SIZE>> storage(new (std::nothrow) std::array<uint8_t, WKI_IVSHMEM_RING_SIZE>{});
    std::unique_ptr<IvshmemFrame> sent(new (std::nothrow) IvshmemFrame{});
    std::unique_ptr<IvshmemFrame> received(new (std::nothrow) IvshmemFrame{});
    if (storage == nullptr || sent == nullptr || received == nullptr) return false;
    for (size_t i = 0; i < sent->size(); ++i) sent->at(i) = static_cast<uint8_t>((i * 37U + 19U) & 0xFFU);
    uint32_t head = 0, tail = 0;
    WkiRing ring{.head_ptr = &head, .tail_ptr = &tail, .data = storage->data(), .size = WKI_IVSHMEM_RING_SIZE - 8};
    // Exercise payload wrap as well as frames crossing the old 8192-byte cap.
    for (uint32_t start : {0U, ring.size - 4U, ring.size - 8192U}) {
        for (uint16_t length : {uint16_t{1}, uint16_t{8192}, uint16_t{8193}, static_cast<uint16_t>(WKI_MAX_FRAME_SIZE)}) {
            head = start;
            tail = start;
            received->fill(0);
            if (ring_write(&ring, sent->data(), length) != 0 || ring_read(&ring, received->data(), received->size()) != length ||
                std::memcmp(sent->data(), received->data(), length) != 0 || head != tail ||
                ring_read(&ring, received->data(), received->size()) != 0)
                return false;
        }
    }
    return true;
}
#endif

}  // namespace ker::net::wki
