#pragma once

#include <cstdint>
#include <dev/pci.hpp>
#include <net/netdevice.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::dev::ivshmem {

// ivshmem-plain PCI device: vendor=0x1AF4 device=0x1110
constexpr uint16_t IVSHMEM_VENDOR = 0x1AF4;
constexpr uint16_t IVSHMEM_DEVICE = 0x1110;

// BAR layout:
// BAR0: register area (256 bytes) — IntrMask, IntrStatus, IVPosition, Doorbell
// BAR2: shared memory region

// Shared memory ring protocol:
// [0..63]       Header: magic, version, ring offsets
// [64..half]    VM0->VM1 ring
// [half..end]   VM1->VM0 ring
// Packets in ring: [len:uint16_t][data][pad to 4 bytes]

constexpr uint32_t IVSHMEM_MAGIC    = 0x574F534E;  // "WOSN"
constexpr uint32_t IVSHMEM_VERSION  = 1;

constexpr size_t RING_HEADER_SIZE   = 64;

struct IvshmemHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t ring0_offset;  // VM0->VM1 ring offset from shared memory base
    uint32_t ring0_size;
    uint32_t ring1_offset;  // VM1->VM0 ring offset
    uint32_t ring1_size;
    uint32_t vm_id;         // Set by each VM on init (0 or 1)
    uint32_t peer_ready;    // Set to 1 when peer has initialized
} __attribute__((packed));

struct RingBuffer {
    volatile uint32_t head;     // Written by producer
    volatile uint32_t tail;     // Written by consumer
    uint32_t size;              // Total ring data area size
    uint8_t* data;              // Points into shared memory
};

struct IvshmemNetDevice {
    ker::net::NetDevice netdev;
    pci::PCIDevice* pci;
    volatile uint32_t* regs;    // BAR0 registers
    uint8_t* shmem;             // BAR2 shared memory
    size_t shmem_size;
    uint32_t my_vm_id;
    RingBuffer tx_ring;         // We write, peer reads
    RingBuffer rx_ring;         // Peer writes, we read
    uint8_t irq_vector;
    bool active;
    ker::mod::sys::Spinlock tx_lock;
};

auto ivshmem_net_init() -> int;

}  // namespace ker::dev::ivshmem
