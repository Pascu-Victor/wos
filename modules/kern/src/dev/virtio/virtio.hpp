#pragma once

#include <cstddef>
#include <cstdint>
#include <net/packet.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::dev::virtio {

// VirtIO PCI vendor and device IDs
constexpr uint16_t VIRTIO_VENDOR = 0x1AF4;
constexpr uint16_t VIRTIO_NET_LEGACY = 0x1000;   // Transitional device
constexpr uint16_t VIRTIO_NET_MODERN = 0x1041;

// Legacy device register offsets (relative to BAR0 I/O base)
constexpr uint16_t VIRTIO_REG_DEVICE_FEATURES = 0x00;    // 32-bit R
constexpr uint16_t VIRTIO_REG_GUEST_FEATURES = 0x04;     // 32-bit R/W
constexpr uint16_t VIRTIO_REG_QUEUE_ADDR = 0x08;         // 32-bit R/W
constexpr uint16_t VIRTIO_REG_QUEUE_SIZE = 0x0C;         // 16-bit R
constexpr uint16_t VIRTIO_REG_QUEUE_SELECT = 0x0E;       // 16-bit R/W
constexpr uint16_t VIRTIO_REG_QUEUE_NOTIFY = 0x10;       // 16-bit R/W
constexpr uint16_t VIRTIO_REG_DEVICE_STATUS = 0x12;      // 8-bit  R/W
constexpr uint16_t VIRTIO_REG_ISR_STATUS = 0x13;         // 8-bit  R

// VirtIO-Net specific config registers (legacy, offset from BAR0 + 0x14)
constexpr uint16_t VIRTIO_NET_CFG_MAC = 0x14;            // 6 bytes
constexpr uint16_t VIRTIO_NET_CFG_STATUS = 0x1A;         // 16-bit

// Device status bits
constexpr uint8_t VIRTIO_STATUS_ACKNOWLEDGE = 1;
constexpr uint8_t VIRTIO_STATUS_DRIVER = 2;
constexpr uint8_t VIRTIO_STATUS_DRIVER_OK = 4;
constexpr uint8_t VIRTIO_STATUS_FEATURES_OK = 8;
constexpr uint8_t VIRTIO_STATUS_FAILED = 128;

// Feature bits
constexpr uint32_t VIRTIO_NET_F_MAC = (1u << 5);
constexpr uint32_t VIRTIO_NET_F_STATUS = (1u << 16);
constexpr uint32_t VIRTIO_NET_F_MRG_RXBUF = (1u << 15);
constexpr uint32_t VIRTIO_NET_F_CSUM = (1u << 0);
constexpr uint32_t VIRTIO_NET_F_GUEST_CSUM = (1u << 1);

// Virtqueue descriptor flags
constexpr uint16_t VRING_DESC_F_NEXT = 1;
constexpr uint16_t VRING_DESC_F_WRITE = 2;
constexpr uint16_t VRING_DESC_F_INDIRECT = 4;

// VirtIO net header (prepended to every packet)
struct VirtIONetHeader {
    uint8_t flags;
    uint8_t gso_type;
    uint16_t hdr_len;
    uint16_t gso_size;
    uint16_t csum_start;
    uint16_t csum_offset;
} __attribute__((packed));

constexpr size_t VIRTIO_NET_HDR_SIZE = sizeof(VirtIONetHeader);

// Virtqueue structures (packed to match hardware layout)
struct VirtqDesc {
    uint64_t addr;     // Physical address of buffer
    uint32_t len;      // Length of buffer
    uint16_t flags;    // VRING_DESC_F_* flags
    uint16_t next;     // Index of next descriptor (if VRING_DESC_F_NEXT)
} __attribute__((packed));

struct VirtqAvail {
    uint16_t flags;
    uint16_t idx;
    uint16_t ring[];   // Flexible array member
} __attribute__((packed));

struct VirtqUsedElem {
    uint32_t id;       // Index of start of used descriptor chain
    uint32_t len;      // Total bytes written to buffer
} __attribute__((packed));

struct VirtqUsed {
    uint16_t flags;
    uint16_t idx;
    VirtqUsedElem ring[];
} __attribute__((packed));

// Maximum virtqueue size
constexpr uint16_t VIRTQ_MAX_SIZE = 256;

struct Virtqueue {
    uint16_t size;           // Number of descriptors
    uint16_t num_free;       // Free descriptors count
    uint16_t free_head;      // Head of free descriptor list
    uint16_t last_used_idx;  // Last consumed used ring index

    VirtqDesc* desc;         // Descriptor table (physical-contiguous)
    VirtqAvail* avail;       // Available ring
    VirtqUsed* used;         // Used ring

    // Map descriptor index -> PacketBuffer for RX completion
    ker::net::PacketBuffer* pkt_map[VIRTQ_MAX_SIZE];

    uint16_t io_base;        // BAR0 for legacy notify
    uint16_t queue_index;    // Which queue (0=RX, 1=TX)

    ker::mod::sys::Spinlock lock;
};

// Virtqueue management
auto virtq_alloc(uint16_t size) -> Virtqueue*;
auto virtq_add_buf(Virtqueue* vq, uint64_t phys, uint32_t len, uint16_t flags,
                   ker::net::PacketBuffer* pkt) -> int;
auto virtq_get_buf(Virtqueue* vq, uint32_t* out_len) -> uint16_t;  // returns desc idx, 0xFFFF if empty
void virtq_kick(Virtqueue* vq);

// Calculate virtqueue memory layout sizes
auto virtq_desc_size(uint16_t qsz) -> size_t;
auto virtq_avail_size(uint16_t qsz) -> size_t;
auto virtq_used_size(uint16_t qsz) -> size_t;
auto virtq_total_size(uint16_t qsz) -> size_t;

}  // namespace ker::dev::virtio
