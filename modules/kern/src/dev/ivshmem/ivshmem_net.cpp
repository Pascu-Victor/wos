#include "ivshmem_net.hpp"

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/ivshmem/callback.hpp>
#include <dev/ivshmem/selftest.hpp>
#include <net/packet.hpp>
#include <new>  // IWYU pragma: keep
#include <platform/dbg/dbg.hpp>
#include <platform/fw/qemu_fw_cfg.hpp>
#include <platform/ktime/ktime.hpp>
#include <span>

#include "dev/pci.hpp"
#include "net/netdevice.hpp"
#include "net/netpoll.hpp"

namespace ker::dev::ivshmem {

using log = ker::mod::dbg::logger<"ivsh">;

namespace {
// Forward declarations for NAPI
auto ivshmem_poll(ker::net::NapiStruct* napi, int budget, ker::net::NapiPollSource source) -> int;

constexpr size_t MAX_IVSHMEM_DEVICES = 2;
std::array<IvshmemNetDevice, MAX_IVSHMEM_DEVICES> dev_pool = {};
std::array<IvshmemNetDevice*, MAX_IVSHMEM_DEVICES> devices = {};
size_t device_count = 0;
size_t probe_count = 0;
std::atomic<bool> shutdown_started{false};
#ifdef WOS_SELFTEST
IvshmemNetDevice* test_device = nullptr;
auto selftest_late_calls() -> bool;
#endif

// Revision-1 BAR0 position register
constexpr uint32_t IVSHMEM_REG_IVPOSITION = 0x08;

// -- IRQ enable/disable helpers --
void ivshmem_irq_disable(IvshmemNetDevice* dev) { dev->resources.mask_vector(true); }

void ivshmem_irq_enable(IvshmemNetDevice* dev) { dev->resources.mask_vector(false); }

// -- NetDevice operations --
int ivshmem_open(ker::net::NetDevice* netdev) {
    auto* dev = static_cast<IvshmemNetDevice*>(netdev->private_data);
    if (dev == nullptr) return -ENODEV;
    CallbackGuard const GUARD(dev->resources);
    if (!GUARD || !dev->active || dev->ring_failed.load(std::memory_order_acquire)) return -ENODEV;
    netdev->state = 1;
    return 0;
}

void ivshmem_close(ker::net::NetDevice* netdev) {
    auto* dev = static_cast<IvshmemNetDevice*>(netdev->private_data);
    if (dev == nullptr) return;
    CallbackGuard const GUARD(dev->resources);
    if (GUARD) netdev->state = 0;
}

int ivshmem_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) {
    auto* dev = static_cast<IvshmemNetDevice*>(netdev->private_data);
    if (dev == nullptr || pkt == nullptr) {
        if (pkt != nullptr) {
            ker::net::pkt_free(pkt);
        }
        return -1;
    }
    CallbackGuard const GUARD(dev->resources);
    if (!GUARD || !dev->active || dev->ring_failed.load(std::memory_order_acquire) || pkt->len > ker::net::PKT_BUF_SIZE - pkt->headroom()) {
        ker::net::pkt_free(pkt);
        return -ENODEV;
    }

    dev->tx_lock.lock();
    auto const RESULT = shared_ring::write(dev->tx_ring, dev->tx_ring.size, std::span(pkt->data, pkt->len));
    int const RET = RESULT.error == shared_ring::Error::none ? 0 : -1;
    dev->tx_lock.unlock();
    if (RESULT.error == shared_ring::Error::index || RESULT.error == shared_ring::Error::geometry) {
        dev->ring_failed.store(true, std::memory_order_release);
        ivshmem_irq_disable(dev);
    }

    if (RET == 0) {
        netdev->tx_packets++;
        netdev->tx_bytes += pkt->len;
        // Ring doorbell to notify peer
        dev->resources.ring(dev->my_vm_id == 0 ? 1 : 0);
    } else {
        netdev->tx_dropped++;
    }

    ker::net::pkt_free(pkt);
    return RET;
}

void ivshmem_set_mac(ker::net::NetDevice* /*unused*/, const uint8_t* /*unused*/) {}

ker::net::NetDeviceOps ivshmem_ops = {
    .open = ivshmem_open,
    .close = ivshmem_close,
    .start_xmit = ivshmem_start_xmit,
    .set_mac = ivshmem_set_mac,
    .set_queue_cpu = nullptr,
    .check_mac = ker::net::netdev_check_fixed_mac,
};

// -- IRQ handler (minimal - just schedule NAPI) --
void ivshmem_irq(uint8_t /*unused*/, void* data) {
    auto* dev = static_cast<IvshmemNetDevice*>(data);
    if (dev == nullptr) {
        return;
    }
    CallbackGuard const GUARD(dev->resources);
    if (!GUARD || !dev->active) return;
#ifdef WOS_SELFTEST
    dev->resources.test_irqs.fetch_add(1, std::memory_order_relaxed);
#endif

    // Disable device interrupts
    ivshmem_irq_disable(dev);

    // Schedule NAPI poll (atomic, IRQ-safe)
    ker::net::napi_schedule(&dev->napi, ker::net::NapiScheduleSource::IRQ);
}

// -- Device init --
auto init_device(pci::PCIDevice* pci_dev) -> int {
    if (shutdown_started.load(std::memory_order_acquire) || ivshmem_net_is_claimed(pci_dev) || probe_count >= MAX_IVSHMEM_DEVICES) {
        return -1;
    }

    // Failed probe objects remain owned, including any unproven IRQ state.
    auto* idev = new (&dev_pool.at(probe_count++)) IvshmemNetDevice{};
    constexpr size_t SHMEM_SIZE = size_t{16} * 1024 * 1024;
    if (!idev->resources.prepare(pci_dev, SHMEM_SIZE)) {
        log::error("PCI shared-memory resource admission failed");
        return -1;
    }
    auto* regs = idev->resources.regs;
    auto* shmem = idev->resources.shared;

    // Determine VM position (0 or 1)
    uint32_t const IV_POS = regs[IVSHMEM_REG_IVPOSITION / 4];

    idev->pci = pci_dev;
    idev->regs = regs;
    idev->shmem = shmem;
    idev->shmem_size = idev->resources.shared_size;
    idev->my_vm_id = IV_POS;

    // Only a zero header may be initialized. A valid existing peer ring is
    // attached without resetting either side's indices or mailbox data.
    auto* hdr = reinterpret_cast<IvshmemHeader*>(shmem);
    auto* magic = reinterpret_cast<uint32_t*>(shmem);
    char configured = 0;
    int const READ_ROLE = platform::fw::fw_cfg_read_file("opt/wos/wki-ivshmem-role", &configured, 1);
    int32_t const ROLE = READ_ROLE == 1 && (configured == '0' || configured == '1') ? configured - '0' : -1;
    auto const START = mod::time::get_us();
    shared_ring::Claim claim;
    do {
        claim = shared_ring::claim(magic, reinterpret_cast<uint32_t*>(shmem + offsetof(IvshmemHeader, vm_id)), IVSHMEM_MAGIC, ROLE);
        if (claim.state != shared_ring::ClaimState::wait) break;
        asm volatile("pause" ::: "memory");
    } while (mod::time::get_us() - START < 5'000'000);
    if (claim.state == shared_ring::ClaimState::wait || claim.state == shared_ring::ClaimState::refused) {
        log::error("shared header claim refused or timed out");
        return -1;
    }
    if (claim.state == shared_ring::ClaimState::initialize) {
        std::memset(shmem + 4, 0, RING_HEADER_SIZE - 4);
        constexpr uint32_t HALF = (SHMEM_SIZE - RING_HEADER_SIZE) / 2;
        hdr->vm_id = claim.role;
        hdr->version = IVSHMEM_VERSION;
        hdr->ring0_offset = RING_HEADER_SIZE;
        hdr->ring0_size = HALF;
        hdr->ring1_offset = RING_HEADER_SIZE + HALF;
        hdr->ring1_size = HALF;
        std::memset(shmem + RING_HEADER_SIZE, 0, 8);
        std::memset(shmem + RING_HEADER_SIZE + HALF, 0, 8);
        shared_ring::store_word(magic, IVSHMEM_MAGIC);
    }
    auto const SHARED = std::span(shmem, idev->resources.shared_size);
    auto const LAYOUT = shared_ring::snapshot(SHARED, IVSHMEM_MAGIC, false);
    if (!LAYOUT) {
        log::error("shared ring layout refused");
        return -1;
    }
    idev->my_vm_id = claim.role;
    auto const TX = shared_ring::bind(SHARED, LAYOUT->rings[claim.role]);
    auto const RX = shared_ring::bind(SHARED, LAYOUT->rings[1 - claim.role]);
    if (!TX || !RX) return -1;
    if (shared_ring::valid(TX->size, shared_ring::load_word(TX->head_ptr), shared_ring::load_word(TX->tail_ptr)) !=
            shared_ring::Error::none ||
        shared_ring::valid(RX->size, shared_ring::load_word(RX->head_ptr), shared_ring::load_word(RX->tail_ptr)) !=
            shared_ring::Error::none)
        return -1;
    idev->tx_ring = *TX;
    idev->rx_ring = *RX;
    if (claim.role == 1) shared_ring::store_word(reinterpret_cast<uint32_t*>(shmem + offsetof(IvshmemHeader, peer_ready)), 1);

    // Generate MAC: locally administered, based on VM position
    idev->netdev.mac.at(0) = 0x02;
    idev->netdev.mac.at(1) = 0x44;  // 'D' for DMA
    idev->netdev.mac.at(2) = 0x4D;  // 'M'
    idev->netdev.mac.at(3) = 0x41;  // 'A'
    idev->netdev.mac.at(4) = 0x00;
    idev->netdev.mac.at(5) = static_cast<uint8_t>(idev->my_vm_id);

    // Register NetDevice as "dmaN"
    std::array<char, 16> name = {};
    name.at(0) = 'd';
    name.at(1) = 'm';
    name.at(2) = 'a';
    name.at(3) = static_cast<char>(static_cast<uint8_t>('0') + device_count);
    std::memcpy(idev->netdev.name.data(), name.data(), 5);

    idev->netdev.ops = &ivshmem_ops;
    idev->netdev.mtu = 9000;  // jumbo frames supported
    idev->netdev.state = 1;
    idev->netdev.private_data = idev;
    ker::net::napi_init(&idev->napi, &idev->netdev, ivshmem_poll, 64);
    if (!idev->resources.activate(ivshmem_irq, idev, "ivshmem-net")) return -1;
    idev->irq_vector = idev->resources.irq_vector;
    idev->active = true;

    if (ker::net::netdev_register(&idev->netdev) != 0) {
        idev->active = false;
        (void)idev->resources.stop_interrupts();
        return -1;
    }
    idev->published = true;

    // Initialize and enable NAPI for deferred packet processing
    ker::net::napi_enable(&idev->napi);

    devices.at(device_count++) = idev;

    log::info("%s vm_id=%u shmem=0x%lx ready", idev->netdev.name.data(), idev->my_vm_id, reinterpret_cast<uint64_t>(shmem));
#ifdef WOS_SELFTEST
    if (test_device == nullptr) {
        test_device = idev;
        selftest_start(&idev->resources, "net", ivshmem_net_shutdown, selftest_late_calls);
    }
#endif

    return 0;
}

// -- NAPI poll function (runs in worker thread context) --
int ivshmem_poll(ker::net::NapiStruct* napi, int budget, ker::net::NapiPollSource /*source*/) {
    auto* dev = static_cast<IvshmemNetDevice*>(napi->dev->private_data);
    if (dev == nullptr) return 0;
    CallbackGuard const GUARD(dev->resources);
    if (!GUARD || !dev->active) return 0;

    if (dev->ring_failed.load(std::memory_order_acquire)) {
        ker::net::napi_complete(napi);
        ivshmem_irq_disable(dev);
        return 0;
    }
    int processed = 0;
    while (processed < budget) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) break;
        auto const RESULT = shared_ring::read(dev->rx_ring, dev->rx_ring.size, std::span(pkt->data, pkt->tailroom()));
        if (RESULT.error != shared_ring::Error::none) {
            ker::net::pkt_free(pkt);
            if (RESULT.error != shared_ring::Error::empty) {
                dev->ring_failed.store(true, std::memory_order_release);
                dev->netdev.rx_dropped++;
            }
            break;
        }
        pkt->put(RESULT.bytes);
        pkt->dev = &dev->netdev;
        dev->netdev.rx_packets++;
        dev->netdev.rx_bytes += RESULT.bytes;
        ker::net::netdev_rx(&dev->netdev, pkt);
        processed++;
    }

    // If we processed less than budget, we're done - re-enable interrupts
    if (processed < budget) {
        ker::net::napi_complete(napi);
        if (!dev->ring_failed.load(std::memory_order_acquire))
            ivshmem_irq_enable(dev);
        else
            ivshmem_irq_disable(dev);
    }

    return processed;
}
#ifdef WOS_SELFTEST
auto selftest_late_calls() -> bool {
    auto* dev = test_device;
    auto const POOL = ker::net::pkt_pool_free_count();
    auto* pkt = ker::net::pkt_alloc_tx();
    if (pkt == nullptr) return false;
    auto const STATE = dev->netdev.state;
    auto const NAPI = dev->napi.state.load();
    auto const IRQS = dev->resources.test_irqs.load();
    bool const REJECTED = ivshmem_start_xmit(&dev->netdev, pkt) == -ENODEV && ivshmem_open(&dev->netdev) == -ENODEV;
    ivshmem_close(&dev->netdev);
    ivshmem_irq(0, dev);
    return REJECTED && ivshmem_poll(&dev->napi, 64, ker::net::NapiPollSource::INLINE) == 0 && dev->netdev.state == STATE &&
           dev->napi.state.load() == NAPI && dev->resources.test_irqs.load() == IRQS && ker::net::pkt_pool_free_count() == POOL;
}
#endif
}  // namespace

auto ivshmem_net_shutdown() -> int {
    shutdown_started.store(true, std::memory_order_release);
    int result = 0;
    for (size_t i = 0; i < probe_count; ++i) {
        auto& dev = dev_pool.at(i);
        int const RET = dev.resources.retire(
            [](void* data) {
                auto* owner = static_cast<IvshmemNetDevice*>(data);
                owner->active = false;
                if (owner->published) {
                    ker::net::netdev_mark_control_retiring(&owner->netdev);
                    ker::net::NetDeviceRetireToken token;
                    (void)ker::net::netdev_unregister_begin(&owner->netdev, token);
                    owner->published = false;
                }
                // Raw registry snapshots and worker epilogues can outlive drain;
                // retain all metadata and prevent new work without waiting on them.
                owner->napi.state.store(ker::net::NapiState::DISABLED, std::memory_order_release);
                owner->napi.has_work.store(false, std::memory_order_release);
            },
            &dev);
        if (RET != 0) result = RET;
    }
    return result;
}

auto ivshmem_net_is_claimed(pci::PCIDevice* dev) -> bool {
    for (size_t i = 0; i < probe_count; i++) {
        if (dev_pool.at(i).resources.device == dev) {
            return true;
        }
    }
    return false;
}

auto ivshmem_net_init() -> int {
    int found = 0;

    size_t const COUNT = pci::pci_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        if (dev->vendor_id == IVSHMEM_VENDOR && dev->device_id == IVSHMEM_DEVICE) {
            log::info("found device at PCI %02x:%02x.%x", dev->bus, dev->slot, dev->function);

            if (init_device(dev) == 0) {
                found++;
            }
        }
    }

    if (found == 0) {
        log::info("no devices found");
    }

    return found;
}

}  // namespace ker::dev::ivshmem
