#include "transport_ivshmem.hpp"

#include <array>
#include <cstddef>
#include <cstring>
#include <dev/ivshmem/ivshmem_net.hpp>
#include <dev/pci.hpp>
#include <net/wki/irq_fwd.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// BAR2 shared memory layout for WKI ivshmem transport
// -----------------------------------------------------------------------------
//
// [0..63]                WKI ivshmem header
// [64..64+64KB-1]        VM0→VM1 message ring
// [64+64KB..64+128KB-1]  VM1→VM0 message ring
// [64+128KB..end]        RDMA region pool (bitmap-allocated, 4KB granularity)
//

constexpr uint32_t WKI_IVSHMEM_MAGIC = 0x574B4944;  // "WKID"
constexpr uint32_t WKI_IVSHMEM_VERSION = 1;
constexpr size_t WKI_IVSHMEM_HEADER_SIZE = 64;
constexpr size_t WKI_IVSHMEM_RING_SIZE = static_cast<const size_t>(64 * 1024);  // 64KB per ring
constexpr size_t WKI_IVSHMEM_RING_AREA = 2 * WKI_IVSHMEM_RING_SIZE;
constexpr size_t WKI_IVSHMEM_RDMA_OFFSET = WKI_IVSHMEM_HEADER_SIZE + WKI_IVSHMEM_RING_AREA;

constexpr size_t IVSHMEM_SHMEM_SIZE = static_cast<const size_t>(16 * 1024 * 1024);  // 16MB default
constexpr size_t WKI_RDMA_REGION_SIZE = IVSHMEM_SHMEM_SIZE - WKI_IVSHMEM_RDMA_OFFSET;

constexpr size_t RDMA_PAGE_SIZE = 4096;
constexpr size_t RDMA_MAX_PAGES = WKI_RDMA_REGION_SIZE / RDMA_PAGE_SIZE;
constexpr size_t RDMA_BITMAP_SIZE = (RDMA_MAX_PAGES + 7) / 8;

// BAR0 register offsets (same as ivshmem_net)
constexpr uint32_t IVSHMEM_REG_INTRMASK = 0x00;
constexpr uint32_t IVSHMEM_REG_INTRSTATUS = 0x04;
constexpr uint32_t IVSHMEM_REG_DOORBELL = 0x0C;

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
// D4: IRQ forwarding mailbox — overlaid on the 24-byte reserved area
// Two 12-byte slots: [0]=VM0→VM1, [1]=VM1→VM0
// -----------------------------------------------------------------------------

struct IrqMailboxSlot {
    volatile uint32_t pending;  // 0 = empty, 1 = data ready
    uint16_t device_id;
    uint16_t irq_vector;
    uint32_t irq_status;
} __attribute__((packed));

static_assert(sizeof(IrqMailboxSlot) == 12, "IrqMailboxSlot must be 12 bytes");

// Offset of mailbox within the reserved area
constexpr size_t IRQ_MAILBOX_OFFSET = offsetof(WkiIvshmemHeader, reserved);

// -----------------------------------------------------------------------------
// Ring buffer (reused from ivshmem_net design)
// -----------------------------------------------------------------------------

struct WkiRing {
    volatile uint32_t* head_ptr;  // in shared memory
    volatile uint32_t* tail_ptr;  // in shared memory
    uint8_t* data;
    uint32_t size;
};

// -----------------------------------------------------------------------------
// Transport private state
// -----------------------------------------------------------------------------

struct IvshmemTransportPrivate {
    dev::pci::PCIDevice* pci;
    volatile uint32_t* regs;  // BAR0
    uint8_t* shmem;           // BAR2
    size_t shmem_size;
    uint32_t my_vm_id;

    WkiRing tx_ring;
    WkiRing rx_ring;

    // RDMA bitmap allocator
    std::array<uint8_t, RDMA_BITMAP_SIZE> rdma_bitmap;
    uint8_t* rdma_base;  // start of RDMA region in BAR2
    uint32_t rdma_size;

    WkiRxHandler rx_handler;
};

namespace {

WkiTransport s_ivshmem_transport;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
IvshmemTransportPrivate s_ivshmem_priv;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_ivshmem_initialized = false;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// -----------------------------------------------------------------------------
// Ring operations
// -----------------------------------------------------------------------------

auto ring_write(WkiRing* ring, const uint8_t* data_buf, uint16_t len) -> int {
    // Packet format: [len:u16][data][pad to 4 bytes]
    uint32_t pkt_size = 2 + len;
    uint32_t padded = (pkt_size + 3) & ~3U;

    uint32_t head = *ring->head_ptr;
    uint32_t tail = *ring->tail_ptr;
    uint32_t space = (head >= tail) ? (ring->size - head + tail) : (tail - head);

    if (padded + 1 > space) {
        return -1;  // ring full
    }

    uint32_t pos = head;
    ring->data[pos % ring->size] = static_cast<uint8_t>(len & 0xFF);
    ring->data[(pos + 1) % ring->size] = static_cast<uint8_t>(len >> 8);
    pos += 2;

    for (uint16_t i = 0; i < len; i++) {
        ring->data[(pos + i) % ring->size] = data_buf[i];
    }
    pos += len;

    while (((pos - head) & 3U) != 0U) {
        ring->data[pos % ring->size] = 0;
        pos++;
    }

    asm volatile("" ::: "memory");  // NOLINT(hicpp-no-assembler)
    *ring->head_ptr = pos % ring->size;
    return 0;
}

auto ring_read(WkiRing* ring, uint8_t* buf, uint16_t buf_size) -> uint16_t {
    uint32_t head = *ring->head_ptr;
    uint32_t tail = *ring->tail_ptr;
    asm volatile("" ::: "memory");  // NOLINT(hicpp-no-assembler)

    if (head == tail) {
        return 0;
    }

    auto len = static_cast<uint16_t>(ring->data[tail % ring->size] | (static_cast<uint16_t>(ring->data[(tail + 1) % ring->size]) << 8));

    uint32_t pkt_size = 2 + len;
    uint32_t padded = (pkt_size + 3) & ~3U;
    uint32_t pos = tail + 2;

    uint16_t copy_len = (len > buf_size) ? buf_size : len;
    for (uint16_t i = 0; i < copy_len; i++) {
        buf[i] = ring->data[(pos + i) % ring->size];
    }

    asm volatile("" ::: "memory");  // NOLINT(hicpp-no-assembler)
    *ring->tail_ptr = (tail + padded) % ring->size;
    return copy_len;
}

// -----------------------------------------------------------------------------
// RDMA bitmap allocator
// -----------------------------------------------------------------------------

auto rdma_bitmap_alloc(IvshmemTransportPrivate* priv, uint32_t size) -> int64_t {
    uint32_t pages_needed = (size + RDMA_PAGE_SIZE - 1) / RDMA_PAGE_SIZE;
    if (pages_needed == 0) {
        return -1;
    }

    // Simple first-fit scan
    uint32_t consecutive = 0;
    uint32_t start_page = 0;

    for (uint32_t page = 0; page < RDMA_MAX_PAGES; page++) {
        uint32_t byte_idx = page / 8;
        uint32_t bit_idx = page % 8;

        if ((priv->rdma_bitmap[byte_idx] & (1U << bit_idx)) != 0) {
            consecutive = 0;
            start_page = page + 1;
        } else {
            consecutive++;
            if (consecutive >= pages_needed) {
                // Mark pages as allocated
                for (uint32_t p = start_page; p <= page; p++) {
                    uint32_t bi = p / 8;
                    uint32_t bt = p % 8;
                    priv->rdma_bitmap[bi] |= static_cast<uint8_t>(1U << bt);
                }
                return static_cast<int64_t>(start_page) * static_cast<int64_t>(RDMA_PAGE_SIZE);
            }
        }
    }

    return -1;  // no space
}

void rdma_bitmap_free(IvshmemTransportPrivate* priv, int64_t offset, uint32_t size) {
    auto start_page = static_cast<uint32_t>(offset / RDMA_PAGE_SIZE);
    uint32_t pages = (size + RDMA_PAGE_SIZE - 1) / RDMA_PAGE_SIZE;

    for (uint32_t p = start_page; p < start_page + pages && p < RDMA_MAX_PAGES; p++) {
        uint32_t bi = p / 8;
        uint32_t bt = p % 8;
        priv->rdma_bitmap[bi] &= static_cast<uint8_t>(~(1U << bt));
    }
}

// -----------------------------------------------------------------------------
// WkiTransport operations
// -----------------------------------------------------------------------------

auto ivshmem_wki_tx(WkiTransport* self, uint16_t /*neighbor_id*/, const void* data, uint16_t len) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);
    if (priv == nullptr) {
        return -1;
    }

    int ret = ring_write(&priv->tx_ring, static_cast<const uint8_t*>(data), len);
    if (ret == 0) {
        // Ring doorbell to notify peer
        uint32_t peer_id = (priv->my_vm_id == 0) ? 1U : 0U;
        priv->regs[IVSHMEM_REG_DOORBELL / 4] = peer_id;
    }
    return ret;
}

void ivshmem_wki_set_rx_handler(WkiTransport* self, WkiRxHandler handler) {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);
    priv->rx_handler = handler;
}

auto ivshmem_wki_rdma_register_region(WkiTransport* self, uint64_t /*phys_addr*/, uint32_t size, uint32_t* rkey) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);

    int64_t offset = rdma_bitmap_alloc(priv, size);
    if (offset < 0) {
        return -1;
    }

    // The rkey is the offset within the RDMA region
    *rkey = static_cast<uint32_t>(offset);
    return 0;
}

auto ivshmem_wki_rdma_read(WkiTransport* self, uint16_t /*neighbor_id*/, uint32_t rkey, uint64_t remote_offset, void* local_buf,
                           uint32_t len) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);

    uint64_t src_offset = static_cast<uint64_t>(rkey) + remote_offset;
    if (src_offset + len > priv->rdma_size) {
        return -1;
    }

    memcpy(local_buf, priv->rdma_base + src_offset, len);
    return 0;
}

auto ivshmem_wki_rdma_write(WkiTransport* self, uint16_t /*neighbor_id*/, uint32_t rkey, uint64_t remote_offset, const void* local_buf,
                            uint32_t len) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);

    uint64_t dst_offset = static_cast<uint64_t>(rkey) + remote_offset;
    if (dst_offset + len > priv->rdma_size) {
        return -1;
    }

    memcpy(priv->rdma_base + dst_offset, local_buf, len);
    return 0;
}

auto ivshmem_wki_doorbell(WkiTransport* self, uint16_t /*neighbor_id*/, uint32_t value) -> int {
    auto* priv = static_cast<IvshmemTransportPrivate*>(self->private_data);

    // Encode value into doorbell register write
    // The peer receives this as an interrupt with the value recoverable from status
    uint32_t peer_id = (priv->my_vm_id == 0) ? 1U : 0U;
    // Doorbell register format: (peer_vector << 16) | peer_id
    priv->regs[IVSHMEM_REG_DOORBELL / 4] = (value << 16) | peer_id;
    return 0;
}

// -----------------------------------------------------------------------------
// IRQ handler — poll RX ring and deliver to WKI
// -----------------------------------------------------------------------------

void ivshmem_wki_irq(uint8_t /*vector*/, void* data) {
    auto* priv = static_cast<IvshmemTransportPrivate*>(data);
    if (priv == nullptr) {
        return;
    }

    // Acknowledge interrupt
    priv->regs[IVSHMEM_REG_INTRSTATUS / 4] = priv->regs[IVSHMEM_REG_INTRSTATUS / 4];

    // D4: Check IRQ forwarding mailbox before draining the ring.
    // Our RX mailbox: if we are VM0, the peer (VM1) writes to slot[1] (VM1→VM0).
    //                 if we are VM1, the peer (VM0) writes to slot[0] (VM0→VM1).
    auto* mailbox = reinterpret_cast<IrqMailboxSlot*>(priv->shmem + IRQ_MAILBOX_OFFSET);
    uint32_t rx_slot_idx = (priv->my_vm_id == 0) ? 1U : 0U;
    volatile auto* rx_slot = &mailbox[rx_slot_idx];

    if (rx_slot->pending != 0) {
        // Read mailbox data
        uint16_t dev_id = rx_slot->device_id;
        uint16_t vec = rx_slot->irq_vector;
        uint32_t status = rx_slot->irq_status;

        // Clear mailbox
        asm volatile("" ::: "memory");
        rx_slot->pending = 0;

        // Dispatch to IRQ forwarding subsystem
        wki_irq_fwd_doorbell_rx(0, dev_id, vec, status);
    }

    // Drain RX ring
    std::array<uint8_t, 8192> buf{};
    while (true) {
        uint16_t len = ring_read(&priv->rx_ring, buf.data(), static_cast<uint16_t>(buf.size()));
        if (len == 0) {
            break;
        }

        if (priv->rx_handler != nullptr) {
            priv->rx_handler(&s_ivshmem_transport, buf.data(), len);
        }
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Public API — RDMA allocator (used by zone.cpp for RDMA-backed zones)
// -----------------------------------------------------------------------------

auto wki_ivshmem_rdma_alloc(uint32_t size) -> int64_t {
    if (!s_ivshmem_initialized) {
        return -1;
    }
    return rdma_bitmap_alloc(&s_ivshmem_priv, size);
}

void wki_ivshmem_rdma_free(int64_t offset, uint32_t size) {
    if (!s_ivshmem_initialized) {
        return;
    }
    rdma_bitmap_free(&s_ivshmem_priv, offset, size);
}

auto wki_ivshmem_rdma_ptr(int64_t offset) -> void* {
    if (!s_ivshmem_initialized || offset < 0) {
        return nullptr;
    }
    return s_ivshmem_priv.rdma_base + offset;
}

// -----------------------------------------------------------------------------
// D4: IRQ forwarding mailbox write (called from irq_fwd.cpp before doorbell)
// -----------------------------------------------------------------------------

void wki_ivshmem_irq_mailbox_write(WkiTransport* transport, uint16_t device_id, uint16_t irq_vector, uint32_t irq_status) {
    if (transport == nullptr || transport->private_data == nullptr) {
        return;
    }
    auto* priv = static_cast<IvshmemTransportPrivate*>(transport->private_data);

    // Our TX mailbox slot: if we are VM0, we write to slot[0] (VM0→VM1).
    //                      if we are VM1, we write to slot[1] (VM1→VM0).
    auto* mailbox = reinterpret_cast<IrqMailboxSlot*>(priv->shmem + IRQ_MAILBOX_OFFSET);
    volatile auto* tx_slot = &mailbox[priv->my_vm_id];

    tx_slot->device_id = device_id;
    tx_slot->irq_vector = irq_vector;
    tx_slot->irq_status = irq_status;
    asm volatile("" ::: "memory");  // NOLINT(hicpp-no-assembler)
    tx_slot->pending = 1;
}

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_ivshmem_transport_init() {
    if (s_ivshmem_initialized) {
        return;
    }

    // Probe PCI for an ivshmem device not claimed by ivshmem_net
    dev::pci::PCIDevice* found_dev = nullptr;
    size_t count = dev::pci::pci_device_count();

    for (size_t i = 0; i < count; i++) {
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
        ker::mod::dbg::log("[WKI] No unclaimed ivshmem device found for RDMA transport");
        return;
    }

    // Enable PCI features
    dev::pci::pci_enable_bus_master(found_dev);
    dev::pci::pci_enable_memory_space(found_dev);

    // Map BAR0 (registers)
    auto* bar0_ptr = dev::pci::pci_map_bar(found_dev, 0);
    if (bar0_ptr == nullptr) {
        ker::mod::dbg::log("[WKI] ivshmem: failed to map BAR0");
        return;
    }
    auto* regs = reinterpret_cast<volatile uint32_t*>(bar0_ptr);

    // Map BAR2 (shared memory)
    auto* bar2_ptr = dev::pci::pci_map_bar(found_dev, 2);
    if (bar2_ptr == nullptr) {
        ker::mod::dbg::log("[WKI] ivshmem: failed to map BAR2");
        return;
    }
    auto* shmem = const_cast<uint8_t*>(reinterpret_cast<volatile uint8_t*>(bar2_ptr));  // NOLINT(cppcoreguidelines-pro-type-const-cast)

    // Set up private state
    s_ivshmem_priv.pci = found_dev;
    s_ivshmem_priv.regs = regs;
    s_ivshmem_priv.shmem = shmem;
    s_ivshmem_priv.shmem_size = IVSHMEM_SHMEM_SIZE;
    s_ivshmem_priv.rx_handler = nullptr;

    // Initialize or read shared memory header
    auto* hdr = reinterpret_cast<WkiIvshmemHeader*>(shmem);

    if (hdr->magic != WKI_IVSHMEM_MAGIC) {
        // First VM: initialize header and ring areas
        memset(hdr, 0, sizeof(WkiIvshmemHeader));
        hdr->magic = WKI_IVSHMEM_MAGIC;
        hdr->version = WKI_IVSHMEM_VERSION;
        hdr->ring0_offset = static_cast<uint32_t>(WKI_IVSHMEM_HEADER_SIZE);
        hdr->ring0_size = static_cast<uint32_t>(WKI_IVSHMEM_RING_SIZE);
        hdr->ring1_offset = static_cast<uint32_t>(WKI_IVSHMEM_HEADER_SIZE + WKI_IVSHMEM_RING_SIZE);
        hdr->ring1_size = static_cast<uint32_t>(WKI_IVSHMEM_RING_SIZE);
        hdr->rdma_offset = static_cast<uint32_t>(WKI_IVSHMEM_RDMA_OFFSET);
        hdr->rdma_size = static_cast<uint32_t>(WKI_RDMA_REGION_SIZE);
        hdr->vm_id = 0;
        s_ivshmem_priv.my_vm_id = 0;

        // Zero the ring head/tail pointers (first 8 bytes of each ring)
        memset(shmem + hdr->ring0_offset, 0, 8);
        memset(shmem + hdr->ring1_offset, 0, 8);
    } else {
        hdr->peer_ready = 1;
        s_ivshmem_priv.my_vm_id = 1;
    }

    // VM0: poll for peer_ready with 5s timeout
    if (s_ivshmem_priv.my_vm_id == 0) {
        constexpr uint64_t PEER_READY_TIMEOUT_US = 5'000'000;
        uint64_t deadline = ker::mod::time::getUs() + PEER_READY_TIMEOUT_US;
        while (hdr->peer_ready == 0) {
            if (ker::mod::time::getUs() >= deadline) {
                ker::mod::dbg::log("[WKI] ivshmem: peer_ready timeout — continuing without peer");
                break;
            }
            asm volatile("pause" ::: "memory");
        }
        if (hdr->peer_ready != 0) {
            ker::mod::dbg::log("[WKI] ivshmem: peer is ready");
        }
    }

    // Set up TX/RX rings
    // Each ring: first 8 bytes are [head:u32][tail:u32], rest is data
    if (s_ivshmem_priv.my_vm_id == 0) {
        // VM0 transmits on ring0, receives on ring1
        s_ivshmem_priv.tx_ring.head_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring0_offset);
        s_ivshmem_priv.tx_ring.tail_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring0_offset + 4);
        s_ivshmem_priv.tx_ring.data = shmem + hdr->ring0_offset + 8;
        s_ivshmem_priv.tx_ring.size = hdr->ring0_size - 8;

        s_ivshmem_priv.rx_ring.head_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring1_offset);
        s_ivshmem_priv.rx_ring.tail_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring1_offset + 4);
        s_ivshmem_priv.rx_ring.data = shmem + hdr->ring1_offset + 8;
        s_ivshmem_priv.rx_ring.size = hdr->ring1_size - 8;
    } else {
        // VM1 transmits on ring1, receives on ring0
        s_ivshmem_priv.tx_ring.head_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring1_offset);
        s_ivshmem_priv.tx_ring.tail_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring1_offset + 4);
        s_ivshmem_priv.tx_ring.data = shmem + hdr->ring1_offset + 8;
        s_ivshmem_priv.tx_ring.size = hdr->ring1_size - 8;

        s_ivshmem_priv.rx_ring.head_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring0_offset);
        s_ivshmem_priv.rx_ring.tail_ptr = reinterpret_cast<volatile uint32_t*>(shmem + hdr->ring0_offset + 4);
        s_ivshmem_priv.rx_ring.data = shmem + hdr->ring0_offset + 8;
        s_ivshmem_priv.rx_ring.size = hdr->ring0_size - 8;
    }

    // Set up RDMA region
    s_ivshmem_priv.rdma_base = shmem + WKI_IVSHMEM_RDMA_OFFSET;
    s_ivshmem_priv.rdma_size = static_cast<uint32_t>(WKI_RDMA_REGION_SIZE);
    s_ivshmem_priv.rdma_bitmap.fill(0);  // all free

    // Set up IRQ
    uint8_t vector = ker::mod::gates::allocateVector();
    if (vector != 0) {
        int msi_ret = dev::pci::pci_enable_msi(found_dev, vector);
        if (msi_ret != 0) {
            vector = found_dev->interrupt_line + 32;
        }
        ker::mod::gates::requestIrq(vector, ivshmem_wki_irq, &s_ivshmem_priv, "wki-ivshmem");
        regs[IVSHMEM_REG_INTRMASK / 4] = 0xFFFFFFFF;
    }

    // Fill in transport struct
    s_ivshmem_transport.name = "wki-ivshmem";
    s_ivshmem_transport.mtu = 8192;
    s_ivshmem_transport.rdma_capable = true;
    s_ivshmem_transport.private_data = &s_ivshmem_priv;
    s_ivshmem_transport.tx = ivshmem_wki_tx;
    s_ivshmem_transport.set_rx_handler = ivshmem_wki_set_rx_handler;
    s_ivshmem_transport.rdma_register_region = ivshmem_wki_rdma_register_region;
    s_ivshmem_transport.rdma_read = ivshmem_wki_rdma_read;
    s_ivshmem_transport.rdma_write = ivshmem_wki_rdma_write;
    s_ivshmem_transport.doorbell = ivshmem_wki_doorbell;
    s_ivshmem_transport.next = nullptr;

    // Register with WKI core
    wki_transport_register(&s_ivshmem_transport);

    s_ivshmem_initialized = true;
    ker::mod::dbg::log("[WKI] ivshmem RDMA transport initialized (vm_id=%u, rdma=%u KB)", s_ivshmem_priv.my_vm_id,
                       static_cast<uint32_t>(WKI_RDMA_REGION_SIZE / 1024));
}

}  // namespace ker::net::wki
