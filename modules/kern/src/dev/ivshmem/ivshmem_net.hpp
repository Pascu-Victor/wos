#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/ivshmem/pci_resources.hpp>
#include <dev/ivshmem/shared_ring.hpp>
#include <dev/pci.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::dev::ivshmem {

// Revision-1 ivshmem plain/doorbell PCI device.
constexpr uint16_t IVSHMEM_VENDOR = 0x1AF4;
constexpr uint16_t IVSHMEM_DEVICE = 0x1110;

// BAR layout:
// BAR0: register area (256 bytes) - IVPosition and Doorbell; mask/status reserved
// BAR1: MSI-X table/PBA in doorbell mode
// BAR2: shared memory region

// Shared memory ring protocol:
// [0..63]       Header: magic, version, ring offsets
// [64..half]    VM0->VM1 ring
// [half..end]   VM1->VM0 ring
// Packets in ring: [len:uint16_t][data][pad to 4 bytes]

constexpr uint32_t IVSHMEM_MAGIC = 0x574F534E;  // "WOSN"
constexpr uint32_t IVSHMEM_VERSION = 1;

constexpr size_t RING_HEADER_SIZE = 64;

struct IvshmemHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t ring0_offset;  // VM0->VM1 ring offset from shared memory base
    uint32_t ring0_size;
    uint32_t ring1_offset;  // VM1->VM0 ring offset
    uint32_t ring1_size;
    uint32_t vm_id;       // Initializer role, published before magic (0 or 1)
    uint32_t peer_ready;  // Set to 1 when peer has initialized
} __attribute__((packed));
static_assert(sizeof(IvshmemHeader) == 32);

using RingBuffer = shared_ring::View;

struct IvshmemNetDevice {
    PciResources resources{};
    ker::net::NetDevice netdev;
    pci::PCIDevice* pci;
    volatile uint32_t* regs;  // BAR0 registers
    uint8_t* shmem;           // BAR2 shared memory
    size_t shmem_size;
    uint32_t my_vm_id;
    RingBuffer tx_ring;  // We write, peer reads
    RingBuffer rx_ring;  // Peer writes, we read
    uint8_t irq_vector;
    std::atomic<bool> ring_failed{false};
    bool active;
    bool published = false;
    ker::mod::sys::Spinlock tx_lock;

    // NAPI state for deferred packet processing
    ker::net::NapiStruct napi{};
};

auto ivshmem_net_init() -> int;
auto ivshmem_net_shutdown() -> int;

// Check if a PCI device has already been claimed by ivshmem_net
auto ivshmem_net_is_claimed(pci::PCIDevice* dev) -> bool;

}  // namespace ker::dev::ivshmem
