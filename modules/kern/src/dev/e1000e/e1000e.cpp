#include "e1000e.hpp"

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/iommu/domains.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/wki/remotable.hpp>
#include <new>
#include <platform/acpi/ioapic/ioapic.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/cpu_maintenance_controller.hpp>
#include <platform/mm/cpu_maintenance_platform.hpp>
#include <platform/mm/phys.hpp>
#ifdef WOS_SELFTEST
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#endif
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>
#include <utility>

#include "dev/pci.hpp"

namespace ker::dev::e1000e {

using log = ker::mod::dbg::logger<"e1000e">;
namespace mmio = ker::mod::mm::mmio;
using Controller = ker::mod::mm::cpu_maintenance::detail::Controller;

namespace detail {
// This class is defined only in the actual consumer translation unit. Its
// method executes the hardware lifecycle before issuing the private witness.
class MmioRetirementAccess {
   public:
    static auto retire(E1000Device* dev, const Controller* controller) -> int;
};
}  // namespace detail

namespace {

// These disjoint BAR0 windows cover every register used by the actual driver.
// The retained function still owns the full BAR and its companion resources.
constexpr std::array<mmio::ViewRequest, 2> REGISTER_VIEWS = {{
    {.bar = 0, .offset = 0, .bytes = 0x1000},
    {.bar = 0, .offset = 0x2000, .bytes = 0x4000},
}};

// WKI remotable ops for E1000E devices
auto remotable_can_remote() -> bool { return true; }
auto remotable_can_share() -> bool { return true; }
auto remotable_can_passthrough() -> bool { return false; }
auto remotable_on_attach(uint16_t node_id) -> int {
    log::trace("remote attach from 0x%04x", node_id);
    return 0;
}
void remotable_on_detach(uint16_t node_id) { log::trace("remote detach from 0x%04x", node_id); }
void remotable_on_fault(uint16_t node_id) { log::trace("remote fault for 0x%04x", node_id); }
const ker::net::wki::RemotableOps S_REMOTABLE_OPS = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

constexpr size_t MAX_E1000_DEVICES = 4;
std::array<E1000Device*, MAX_E1000_DEVICES> devices = {};
size_t device_count = 0;
std::atomic<bool> shutdown_started{false};
managed::Api dma_api;
#ifdef WOS_SELFTEST
bool test_probing = false, test_withhold = false;
std::atomic<uint64_t> fault_override{0};
size_t test_allocations_left = SIZE_MAX;
DmaTestResult* test_result = nullptr;
auto test_rx(ker::net::PacketBuffer* pkt) -> bool;
auto selftest_live_rx(E1000Device* dev, ker::net::PacketBuffer* pkt) -> bool;
void selftest_live_irq(E1000Device* dev);
void selftest_live_start(E1000Device* dev);
auto test_flag(std::string_view token) -> bool {
    auto const* cmd = ker::init::get_kernel_cmdline();
    if (cmd == nullptr) return false;
    std::string_view text(cmd);
    for (size_t pos = text.find(token); pos != std::string_view::npos; pos = text.find(token, pos + 1)) {
        auto const END = pos + token.size();
        if ((pos == 0 || text[pos - 1] == ' ') && (END == text.size() || text[END] == ' ')) return true;
    }
    return false;
}
#endif

auto allow_allocation() -> bool {
#ifdef WOS_SELFTEST
    if (test_probing && test_allocations_left != SIZE_MAX) {
        if (test_allocations_left == 0) return false;
        --test_allocations_left;
    }
#endif
    return true;
}

class CallbackGuard {
    ker::mod::sched::KernelShutdownHold shutdown_{ker::mod::sched::hold_kernel_shutdown()};
    dma::lifecycle::Guard callback_;
    mmio::Borrow registers_;
    bool admitted_ = false;

   public:
    explicit CallbackGuard(E1000Device* dev) : callback_(dev->admission) {
        if (!callback_) return;
        if (dev->typed_mmio) {
            registers_ = dev->registers.borrow();
            if (!registers_.valid()) return;
        }
        admitted_ = true;
    }
    explicit operator bool() const { return admitted_; }
    [[nodiscard]] auto registers() const -> const mmio::Borrow& { return registers_; }
};

auto retire_device(E1000Device* dev, const Controller* controller = nullptr) -> int;

// Supported PCI device IDs (vendor = 0x8086)
struct DeviceID {
    uint16_t id;
    const char* name;
};

constexpr std::array<DeviceID, 5> SUPPORTED_DEVICES = {{
    {.id = 0x100E, .name = "82540EM (e1000)"},
    {.id = 0x10D3, .name = "82574L (e1000e)"},
    {.id = 0x1539, .name = "I211-AT"},
    {.id = 0x153A, .name = "I217-LM"},
    {.id = 0x15B8, .name = "I219-V"},
}};

// -- MMIO access ---------------------------------------------------------
inline auto reg_read(E1000Device* dev, const mmio::Borrow& registers, uint32_t offset) -> uint32_t {
    if (!dev->typed_mmio) return dev->mmio[offset / 4];
    if (offset < 0x1000) return registers.read32(0, offset);
    if (offset >= 0x2000 && offset < 0x6000) return registers.read32(1, offset - 0x2000);
    __builtin_trap();
}

inline void reg_write(E1000Device* dev, const mmio::Borrow& registers, uint32_t offset, uint32_t value) {
    if (!dev->typed_mmio) {
        dev->mmio[offset / 4] = value;
        return;
    }
    if (offset < 0x1000) {
        registers.write32(0, offset, value);
        return;
    }
    if (offset >= 0x2000 && offset < 0x6000) {
        registers.write32(1, offset - 0x2000, value);
        return;
    }
    __builtin_trap();
}

// A failed DMA transition closes software admission immediately. Teardown runs
// later in task context; IRQ/NAPI never unmap or free DMA backing.
void fail_io(E1000Device* dev, const mmio::Borrow& registers) {
    dev->admission.close();
    dev->driver_claim.close_data_admission();
    if (dev->typed_mmio) dev->registers.close();
    reg_write(dev, registers, REG_IMC, UINT32_MAX);
    (void)reg_read(dev, registers, REG_STATUS);
    (void)pci::pci_set_bus_master(dev->pci, false);
}

auto wait_clear(E1000Device* dev, const mmio::Borrow& registers, uint32_t reg, uint32_t mask, uint64_t timeout_ms) -> bool {
    auto const START = ker::mod::time::get_ms();
    for (size_t n = 0; n < 10'000'000; ++n) {
        auto const VALUE = reg_read(dev, registers, reg);
        if (VALUE != UINT32_MAX && (VALUE & mask) == 0) return true;
        if (ker::mod::time::get_ms() - START >= timeout_ms) break;
        asm volatile("pause" ::: "memory");
    }
    return false;
}

auto reset_device(E1000Device* dev, const mmio::Borrow& registers) -> bool {
    reg_write(dev, registers, REG_IMC, UINT32_MAX);
    reg_write(dev, registers, REG_RCTL, 0);
    reg_write(dev, registers, REG_TCTL, 0);
    (void)reg_read(dev, registers, REG_STATUS);
    reg_write(dev, registers, REG_CTRL, reg_read(dev, registers, REG_CTRL) | CTRL_RST);
    // 8254x CTRL.RST: allow at least ~1 us before any further device access.
    // Use two HPET microseconds to cover the counter's sampling phase.
    ker::mod::time::sleep_us(2);
    bool const RESET = wait_clear(dev, registers, REG_CTRL, CTRL_RST, 100);
    reg_write(dev, registers, REG_IMC, UINT32_MAX);
    (void)reg_read(dev, registers, REG_ICR);
    return RESET;
}

auto configure_coherence(E1000Device* dev, const mmio::Borrow& registers) -> bool {
    auto* p = dev->pci;
    uint8_t const CAP = pci::pci_find_capability(p, 0x10);  // PCI Express.
    if (CAP == 0) return p->device_id == 0x100e;            // Conventional snooped PCI.
    if (CAP > 0xf4) return false;
    auto const OFFSET = static_cast<uint8_t>(CAP + 8);
    auto value = pci::pci_config_read16(p->bus, p->slot, p->function, OFFSET);
    // PCIe Device Control: disable both No Snoop and relaxed ordering.
    value = static_cast<uint16_t>(value & ~0x0810U);
    pci::pci_config_write16(p->bus, p->slot, p->function, OFFSET, value);
    if ((pci::pci_config_read16(p->bus, p->slot, p->function, OFFSET) & 0x0810U) != 0) return false;
    if (p->device_id == 0x10d3) {
        // 82574 GCR: six No-Snoop clients off; required initialization bit 22.
        reg_write(dev, registers, 0x5b00, (reg_read(dev, registers, 0x5b00) & ~0x3fU) | (1U << 22));
        if ((reg_read(dev, registers, 0x5b00) & 0x3fU) != 0) return false;
    }
    return true;
}

// -- EEPROM read ---------------------------------------------------------
auto eeprom_read(E1000Device* dev, const mmio::Borrow& registers, uint8_t addr) -> uint16_t {
    // Write address and start bit
    reg_write(dev, registers, REG_EERD, (static_cast<uint32_t>(addr) << 8) | EERD_START);

    // Poll for completion
    uint32_t val = 0;
    for (int i = 0; i < 10000; i++) {
        val = reg_read(dev, registers, REG_EERD);
        if ((val & EERD_DONE) != 0) {
            return static_cast<uint16_t>(val >> 16);
        }
    }

    return 0;  // Timeout
}

// -- Read MAC address ----------------------------------------------------
void read_mac(E1000Device* dev, const mmio::Borrow& registers) {
    // Try reading from RAL/RAH first (already programmed by EEPROM autoload)
    uint32_t const RAL = reg_read(dev, registers, REG_RAL);
    uint32_t const RAH = reg_read(dev, registers, REG_RAH);

    if (RAL != 0 || (RAH & 0xFFFF) != 0) {
        dev->netdev.mac.at(0) = static_cast<uint8_t>(RAL);
        dev->netdev.mac.at(1) = static_cast<uint8_t>(RAL >> 8);
        dev->netdev.mac.at(2) = static_cast<uint8_t>(RAL >> 16);
        dev->netdev.mac.at(3) = static_cast<uint8_t>(RAL >> 24);
        dev->netdev.mac.at(4) = static_cast<uint8_t>(RAH);
        dev->netdev.mac.at(5) = static_cast<uint8_t>(RAH >> 8);
        return;
    }

    // Fallback: read from EEPROM
    uint16_t const W0 = eeprom_read(dev, registers, 0);
    uint16_t const W1 = eeprom_read(dev, registers, 1);
    uint16_t const W2 = eeprom_read(dev, registers, 2);

    dev->netdev.mac.at(0) = static_cast<uint8_t>(W0);
    dev->netdev.mac.at(1) = static_cast<uint8_t>(W0 >> 8);
    dev->netdev.mac.at(2) = static_cast<uint8_t>(W1);
    dev->netdev.mac.at(3) = static_cast<uint8_t>(W1 >> 8);
    dev->netdev.mac.at(4) = static_cast<uint8_t>(W2);
    dev->netdev.mac.at(5) = static_cast<uint8_t>(W2 >> 8);
}

auto allocate_ring(E1000Device* dev, managed::Buffer& ring, size_t bytes) -> bool {
    if (!allow_allocation()) return false;
    auto const RESULT = dma::allocate_coherent(dev->pci, bytes);
    if (RESULT.error != dma::Error::none) return false;
    ring.allocation = RESULT.allocation;
    std::memset(ring.allocation.cpu, 0, bytes);
    return ring.prepare(dma_api);
}

auto allocate_pool(E1000Device* dev, managed::Buffer& buffer, size_t bytes, dma::Direction direction) -> bool {
    if (!allow_allocation()) return false;
    auto const RESULT = dma::allocate_streaming(dev->pci, bytes, direction);
    if (RESULT.error != dma::Error::none) return false;
    buffer.allocation = RESULT.allocation;
    return true;
}

auto bind_rx(E1000Device* dev, size_t index, ker::net::PacketBuffer* pkt) -> bool {
    pkt->data = pkt->storage.data();
    pkt->len = 0;
    auto& buffer = dev->rx_dma[index];
    if (!buffer.bind(dma_api, {reinterpret_cast<std::byte*>(pkt->data), managed::RX_BYTES}, dma::Direction::from_device)) return false;
    dev->rx_bufs[index] = pkt;
    dev->rx_descs[index].addr = buffer.submission.address;
    dev->rx_descs[index].status = 0;
    return true;
}

// Probe prepares the entire ring; no partial ring is made hardware-visible.
auto init_rx(E1000Device* dev) -> bool {
    if (!allocate_ring(dev, dev->rx_ring, NUM_RX_DESC * sizeof(E1000RxDesc))) return false;
    dev->rx_descs = reinterpret_cast<E1000RxDesc*>(dev->rx_ring.allocation.cpu);
    for (size_t i = 0; i < NUM_RX_DESC; ++i) {
        if (!allocate_pool(dev, dev->rx_dma[i], managed::RX_BYTES, dma::Direction::from_device)) return false;
        auto* pkt = ker::net::pkt_alloc_rx();
        if (pkt == nullptr) return false;
        dev->rx_bufs[i] = pkt;
        if (!bind_rx(dev, i, pkt)) return false;
    }
    return true;
}

auto init_tx(E1000Device* dev) -> bool {
    if (!allocate_ring(dev, dev->tx_ring, NUM_TX_DESC * sizeof(E1000TxDesc))) return false;
    dev->tx_descs = reinterpret_cast<E1000TxDesc*>(dev->tx_ring.allocation.cpu);
    for (auto& buffer : dev->tx_dma)
        if (!allocate_pool(dev, buffer, ker::net::PKT_BUF_SIZE, dma::Direction::to_device)) return false;
    return true;
}

void program_rings(E1000Device* dev, const mmio::Borrow& registers) {
    (void)dev->rx_ring.expose();
    (void)dev->tx_ring.expose();
    for (auto& buffer : dev->rx_dma) (void)buffer.expose();
    uint64_t const RX = dev->rx_ring.submission.address;
    uint64_t const TX = dev->tx_ring.submission.address;
    reg_write(dev, registers, REG_RDBAL, static_cast<uint32_t>(RX));
    reg_write(dev, registers, REG_RDBAH, static_cast<uint32_t>(RX >> 32));
    reg_write(dev, registers, REG_RDLEN, NUM_RX_DESC * sizeof(E1000RxDesc));
    reg_write(dev, registers, REG_RDH, 0);
    reg_write(dev, registers, REG_RDT, NUM_RX_DESC - 1);
    reg_write(dev, registers, REG_TDBAL, static_cast<uint32_t>(TX));
    reg_write(dev, registers, REG_TDBAH, static_cast<uint32_t>(TX >> 32));
    reg_write(dev, registers, REG_TDLEN, NUM_TX_DESC * sizeof(E1000TxDesc));
    reg_write(dev, registers, REG_TDH, 0);
    reg_write(dev, registers, REG_TDT, 0);
    reg_write(dev, registers, REG_TIPG, (6U << 20) | (8U << 10) | 10U);
    asm volatile("mfence" ::: "memory");
    (void)reg_read(dev, registers, REG_STATUS);
}

int process_rx_budget(E1000Device* dev, const mmio::Borrow& registers, int budget) {
    int processed = 0;
    while (processed < budget && !dev->admission.closed()) {
        uint16_t const IDX = dev->rx_tail;
        volatile auto* desc = &dev->rx_descs[IDX];
        uint8_t const STATUS = desc->status;
        if ((STATUS & RX_STATUS_DD) == 0) break;
        asm volatile("lfence" ::: "memory");
        size_t const LENGTH = desc->length;
        bool const VALID = managed::rx_valid(STATUS, desc->errors, LENGTH, dev->rx_discarding);
        dev->rx_discarding = (STATUS & RX_STATUS_EOP) == 0;
        ++processed;  // Drops and fragments also consume the bounded budget.
        auto* pkt = dev->rx_bufs[IDX];
        if (pkt == nullptr || !dev->rx_dma[IDX].complete(dma_api, VALID ? LENGTH : 0)) {
            fail_io(dev, registers);
            break;
        }
        // Obtain the replacement BEFORE transferring the old owner. On pool
        // exhaustion drop this frame and reuse its valid packet/bounce slot.
        auto* replacement = VALID ? ker::net::pkt_alloc_rx() : nullptr;
        auto* repost = replacement != nullptr ? replacement : pkt;
        dev->rx_bufs[IDX] = repost;
        if (!bind_rx(dev, IDX, repost) || !dev->rx_dma[IDX].expose()) {
            if (replacement != nullptr) ker::net::pkt_free(pkt);
            fail_io(dev, registers);
            break;
        }
        asm volatile("mfence" ::: "memory");
        reg_write(dev, registers, REG_RDT, IDX);
        dev->rx_tail = (IDX + 1) % NUM_RX_DESC;
        if (replacement != nullptr) {
            pkt->data = pkt->storage.data();
            pkt->len = LENGTH;
            pkt->dev = &dev->netdev;
#ifdef WOS_SELFTEST
            if (test_probing && test_rx(pkt)) continue;
            if (selftest_live_rx(dev, pkt)) continue;
#endif
            ker::net::netdev_rx(&dev->netdev, pkt);
        } else {
            ++dev->netdev.rx_dropped;
        }
    }
    return processed;
}

void process_tx(E1000Device* dev, const mmio::Borrow& registers) {
    for (size_t i = 0; i < NUM_TX_DESC; ++i) {
        volatile auto* desc = &dev->tx_descs[i];
        if (dev->tx_bufs[i] == nullptr || (desc->status & TX_STATUS_DD) == 0) continue;
        asm volatile("lfence" ::: "memory");
        if (!dev->tx_dma[i].complete(dma_api, 0)) {
            fail_io(dev, registers);
            return;
        }
        auto* pkt = dev->tx_bufs[i];
        dev->tx_bufs[i] = nullptr;
        desc->status = 0;
        ker::net::pkt_free(pkt);
    }
}

// -- NAPI poll function - called from worker thread context --------------
int e1000_poll(ker::net::NapiStruct* napi, int budget, ker::net::NapiPollSource /*source*/) {
    auto* dev = reinterpret_cast<E1000Device*>(napi->dev);
    CallbackGuard const GUARD(dev);
    if (!GUARD) return 0;
    auto const& registers = GUARD.registers();
    int processed = 0;

    // TX reclamation is done lazily in start_xmit when the ring is full.
    // NAPI poll is the sole consumer of RX (mutual exclusion via NAPI CAS).
    processed = process_rx_budget(dev, registers, budget);

    // If we processed less than budget, we're done - re-enable IRQs
    if (processed < budget && !dev->admission.closed()) {
        ker::net::napi_complete(napi);
        // Re-enable interrupts
        reg_write(dev, registers, REG_IMS, ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_LSC | ICR_TXDW);
    }

    return processed;
}

// -- Minimal IRQ handler (NAPI model) ------------------------------------
void e1000_irq_handler(uint8_t /*vector*/, void* private_data) {
    auto* dev = static_cast<E1000Device*>(private_data);
    if (dev == nullptr) {
        return;
    }

    CallbackGuard const GUARD(dev);
    if (!GUARD) return;
    auto const& registers = GUARD.registers();

    // Read and acknowledge interrupt cause (auto-clears on read)
    uint32_t const ICR = reg_read(dev, registers, REG_ICR);
    if (ICR == 0) {
        return;  // Not our interrupt
    }

#ifdef WOS_SELFTEST
    selftest_live_irq(dev);
#endif
    // Handle link status change immediately (doesn't need deferral)
    if ((ICR & ICR_LSC) != 0) {
        uint32_t const STATUS = reg_read(dev, registers, REG_STATUS);
        if ((STATUS & 0x02) != 0) {
            log::info("link up");
        } else {
            log::info("link down");
        }
    }

    // For RX/TX, disable interrupts and schedule NAPI poll
    if ((ICR & (ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_TXDW | ICR_TXQE)) != 0) {
        // Disable interrupts until poll completes
        reg_write(dev, registers, REG_IMC, 0xFFFFFFFF);

        // Schedule NAPI poll - all real work happens in worker thread
        ker::net::napi_schedule(&dev->napi, ker::net::NapiScheduleSource::IRQ);
    }
}

// -- NetDevice operations ------------------------------------------------
int e1000_open(ker::net::NetDevice* ndev) {
    CallbackGuard const GUARD(reinterpret_cast<E1000Device*>(ndev));
    return GUARD ? 0 : -ENODEV;
}

void e1000_close(ker::net::NetDevice* /*ndev*/) {}

int e1000_start_xmit(ker::net::NetDevice* ndev, ker::net::PacketBuffer* pkt) {
    auto* dev = reinterpret_cast<E1000Device*>(ndev);

    CallbackGuard const GUARD(dev);
    if (!GUARD || pkt == nullptr ||
        !managed::tx_valid(reinterpret_cast<uintptr_t>(pkt->storage.data()), pkt->storage.size(), reinterpret_cast<uintptr_t>(pkt->data),
                           pkt->len)) {
        ker::net::pkt_free(pkt);
        return !GUARD ? -ENODEV : -EINVAL;
    }

    auto const& registers = GUARD.registers();

    // Use lock_irqsave to prevent preemption while holding the lock.
    // Without this, a DAEMON task holding the plain lock can be preempted,
    // and a non-preemptible PROCESS task on the same CPU will spin forever.
    uint64_t const TXFLAGS = dev->tx_lock.lock_irqsave();

    uint16_t const IDX = dev->tx_tail;
    uint16_t const NEXT = (IDX + 1) % NUM_TX_DESC;
    auto* desc = &dev->tx_descs[IDX];

    // Leave one descriptor unused: hardware interprets head == tail as empty.
    // Packet owners remain until DD reclamation, conservatively bounding space.
    if (dev->tx_bufs.at(IDX) != nullptr || dev->tx_bufs.at(NEXT) != nullptr) {
        // TX ring full - try to reclaim
        process_tx(dev, registers);
        if (dev->tx_bufs.at(IDX) != nullptr || dev->tx_bufs.at(NEXT) != nullptr) {
            dev->tx_lock.unlock_irqrestore(TXFLAGS);
            ker::net::pkt_free(pkt);
            return -1;  // Drop packet
        }
    }

    auto& buffer = dev->tx_dma[IDX];
    if (dev->admission.closed() || !buffer.bind(dma_api, {reinterpret_cast<std::byte*>(pkt->data), pkt->len}, dma::Direction::to_device)) {
        dev->tx_lock.unlock_irqrestore(TXFLAGS);
        ker::net::pkt_free(pkt);
        return -EIO;
    }
    (void)buffer.expose();
    // Set up descriptor
    desc->addr = buffer.submission.address;
#ifdef WOS_SELFTEST
    if (pkt->len == 64 && std::memcmp(pkt->data + 14, "WOSDMAF1", 8) == 0) {
        uint64_t const FORBIDDEN = fault_override.exchange(0, std::memory_order_acq_rel);
        if (FORBIDDEN != 0) desc->addr = FORBIDDEN;
    }
#endif
    desc->length = static_cast<uint16_t>(pkt->len);
    desc->cmd = TX_CMD_EOP | TX_CMD_IFCS | TX_CMD_RS;
    desc->status = 0;
    desc->cso = 0;
    desc->css = 0;
    desc->special = 0;

    dev->tx_bufs.at(IDX) = pkt;
    dev->tx_tail = NEXT;

    // Notify hardware only after all descriptor stores and DMA synchronization.
    asm volatile("mfence" ::: "memory");
    reg_write(dev, registers, REG_TDT, dev->tx_tail);

    ndev->tx_packets++;
    ndev->tx_bytes += pkt->len;

    dev->tx_lock.unlock_irqrestore(TXFLAGS);
    return 0;
}

void e1000_set_mac(ker::net::NetDevice* /*ndev*/, const uint8_t* /*mac*/) {
    // Not implemented - MAC is read from hardware
}

ker::net::NetDeviceOps e1000_netdev_ops = {
    .open = e1000_open,
    .close = e1000_close,
    .start_xmit = e1000_start_xmit,
    .set_mac = e1000_set_mac,
    .set_queue_cpu = nullptr,
    .check_mac = ker::net::netdev_check_fixed_mac,
};

// -- Check if PCI device is supported ------------------------------------
auto find_device_name(uint16_t device_id) -> const char* {
    for (auto supported_device : SUPPORTED_DEVICES) {
        if (supported_device.id == device_id) {
            return supported_device.name;
        }
    }
    return nullptr;
}

auto mask_interrupts(E1000Device* dev, const mmio::Borrow& registers) -> bool {
    bool masked = true;
    if (dev->typed_mmio || dev->mmio != nullptr) {
        reg_write(dev, registers, REG_IMC, UINT32_MAX);
        (void)reg_read(dev, registers, REG_STATUS);
        masked = reg_read(dev, registers, REG_IMS) == 0;
    }
    auto* p = dev->pci;
    auto read = [p](uint8_t offset) { return pci::pci_config_read16(p->bus, p->slot, p->function, offset); };
    auto write = [p](uint8_t offset, uint16_t value) { pci::pci_config_write16(p->bus, p->slot, p->function, offset, value); };
    auto const MSI = pci::pci_find_capability(p, pci::PCI_CAP_ID_MSI);
    if (MSI != 0) {
        if (MSI > 0xfc) return false;
        auto const OFFSET = static_cast<uint8_t>(MSI + 2);
        write(OFFSET, static_cast<uint16_t>(read(OFFSET) & ~1U));
        masked = (read(OFFSET) & 1U) == 0 && masked;
    }
    auto const MSIX = pci::pci_find_capability(p, pci::PCI_CAP_ID_MSIX);
    if (MSIX != 0) {
        if (MSIX > 0xfc) return false;
        auto const OFFSET = static_cast<uint8_t>(MSIX + 2);
        write(OFFSET, static_cast<uint16_t>((read(OFFSET) | 0x4000U) & ~0x8000U));
        masked = (read(OFFSET) & 0xc000U) == 0x4000U && masked;
    }
    write(pci::PCI_COMMAND, static_cast<uint16_t>(read(pci::PCI_COMMAND) | pci::PCI_COMMAND_INT_DISABLE));
    return (read(pci::PCI_COMMAND) & pci::PCI_COMMAND_INT_DISABLE) != 0 && masked;
}

auto stop_dma(E1000Device* dev, const mmio::Borrow& registers) -> bool {
    if (!dev->typed_mmio && dev->mmio == nullptr) return false;
    reg_write(dev, registers, REG_RCTL, reg_read(dev, registers, REG_RCTL) & ~RCTL_EN);
    reg_write(dev, registers, REG_TCTL, reg_read(dev, registers, REG_TCTL) & ~TCTL_EN);
    (void)reg_read(dev, registers, REG_STATUS);
    if (dev->pci->device_id != 0x10d3) {
        // Per-model release proof still needs qualification. Stop/reset but
        // never infer drained PCI transactions from reset or BME readback alone.
        (void)reset_device(dev, registers);
        return false;
    }
    // 82574 datasheet 3.1.3.10: block new requests and drain pending requests
    // before disabling BME. Do not reset afterward: reset reopens this gate.
    reg_write(dev, registers, REG_CTRL, reg_read(dev, registers, REG_CTRL) | (1U << 2));
    return wait_clear(dev, registers, REG_STATUS, 1U << 19, 100);
}

auto retire_buffers(E1000Device* dev, bool quiesced) -> bool {
    bool detached = true;
    for (size_t i = 0; i < NUM_RX_DESC; ++i) {
        if (dev->rx_dma[i].retire(dma_api, quiesced)) {
            ker::net::pkt_free(dev->rx_bufs[i]);
            dev->rx_bufs[i] = nullptr;
        } else
            detached = false;
    }
    for (size_t i = 0; i < NUM_TX_DESC; ++i) {
        if (dev->tx_dma[i].retire(dma_api, quiesced)) {
            ker::net::pkt_free(dev->tx_bufs[i]);
            dev->tx_bufs[i] = nullptr;
        } else
            detached = false;
    }
    detached = dev->rx_ring.retire(dma_api, quiesced) && detached;
    detached = dev->tx_ring.retire(dma_api, quiesced) && detached;
    if (dma::stats(dev->pci).regions == 0) {
        dev->rx_descs = nullptr;
        dev->tx_descs = nullptr;
    }
    return detached;
}

struct RetirementBackend {
    E1000Device* dev;
    mmio::Borrow registers{};
    bool stopped = false, masked = false, bme_off = false;
    void close_admission() {
        dev->admission.close();
        dev->driver_claim.close_data_admission();
        if (dev->typed_mmio) dev->registers.close();
    }
    auto acquire_terminal_if_drained() -> bool {
        if (!dev->admission.drained() || !dev->driver_claim.operations_drained()) return false;
        if (!dev->typed_mmio) return true;
        if (!dev->registers.borrows_drained()) return false;
        registers = dev->registers.terminal_borrow();
        return registers.valid();
    }
    auto drain_readers() -> bool {
        auto const START = ker::mod::time::get_ms();
        for (size_t n = 0; n < 10'000'000 && ker::mod::time::get_ms() - START < 5000; ++n) {
            if (acquire_terminal_if_drained()) return true;
            if (ker::mod::sched::can_query_current_task())
                ker::mod::sched::kern_yield();
            else
                asm volatile("pause" ::: "memory");
        }
        return acquire_terminal_if_drained();
    }
    void unpublish() {
        if (dev->published) {
            ker::net::netdev_mark_control_retiring(&dev->netdev);
            ker::net::NetDeviceRetireToken token;
            (void)ker::net::netdev_unregister_begin(&dev->netdev, token);
            dev->published = false;
        }
        dev->napi.state.store(ker::net::NapiState::DISABLED, std::memory_order_release);
        dev->napi.has_work.store(false, std::memory_order_release);
    }
    auto mask_interrupts() -> bool { return masked = e1000e::mask_interrupts(dev, registers); }
    // The shared lifecycle calls this hook 'reset'; e1000e uses a master drain
    // handshake which deliberately remains asserted instead of reopening it.
    auto reset() -> bool {
        stopped = stop_dma(dev, registers);
#ifdef WOS_SELFTEST
        if (test_withhold) return false;
#endif
        return stopped;
    }
    auto disable_bus_master() -> bool { return bme_off = pci::pci_set_bus_master(dev->pci, false); }
    void drain_interrupts() {
        if (dev->irq_owned) {
            ker::mod::gates::free_irq(dev->irq_vector);
            dev->irq_owned = false;
        }
    }
    void quarantine() { (void)retire_buffers(dev, false); }
    auto release() -> bool { return retire_buffers(dev, true) && dma::stats(dev->pci).regions == 0; }
};

auto retire_device(E1000Device* dev, const Controller* controller) -> int { return detail::MmioRetirementAccess::retire(dev, controller); }

// The probe borrow dies on every return, before init_device may retire.
auto initialize_registers(E1000Device* dev, const char* name) -> bool {
    auto* pci_dev = dev->pci;
    mmio::Borrow registers;
    if (dev->typed_mmio) {
        registers = dev->registers.borrow();
        if (!registers.valid()) return false;
    } else if (dev->mmio == nullptr) {
        return false;
    }
    if (!reset_device(dev, registers) || !configure_coherence(dev, registers)) return false;

    // Disable interrupt moderation/coalescing for low-latency VM-to-VM traffic.
    reg_write(dev, registers, REG_ITR, 0);
    reg_write(dev, registers, REG_RDTR, 0);
    reg_write(dev, registers, REG_RADV, 0);
    reg_write(dev, registers, REG_TIDV, 0);
    reg_write(dev, registers, REG_TADV, 0);

    // Set link up
    uint32_t ctrl = reg_read(dev, registers, REG_CTRL);
    ctrl |= CTRL_SLU;
    ctrl &= ~CTRL_PHY_RST;
    reg_write(dev, registers, REG_CTRL, ctrl);

    // Read MAC address
    read_mac(dev, registers);

    // Clear multicast table array
    for (int i = 0; i < 128; i++) {
        reg_write(dev, registers, REG_MTA + static_cast<uint32_t>(i * 4), 0);
    }

    // Program MAC into RAL/RAH
    uint32_t const RAL = static_cast<uint32_t>(dev->netdev.mac.at(0)) | (static_cast<uint32_t>(dev->netdev.mac.at(1)) << 8) |
                         (static_cast<uint32_t>(dev->netdev.mac.at(2)) << 16) | (static_cast<uint32_t>(dev->netdev.mac.at(3)) << 24);
    uint32_t const RAH = static_cast<uint32_t>(dev->netdev.mac.at(4)) | (static_cast<uint32_t>(dev->netdev.mac.at(5)) << 8) | RAH_AV;
    reg_write(dev, registers, REG_RAL, RAL);
    reg_write(dev, registers, REG_RAH, RAH);

    // Initialize RX and TX rings
    ker::net::pkt_pool_reserve_for_rx_descriptors(NUM_RX_DESC);
    if (!init_rx(dev) || !init_tx(dev)) {
        return false;
    }
    program_rings(dev, registers);
    if (!pci::pci_set_bus_master(pci_dev, true)) {
        return false;
    }

    // Set up interrupt
    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector == 0) {
        log::error("failed to allocate IRQ vector");
        return false;
    }

    dev->irq_vector = vector;
    dev->irq_owned = true;  // Reservation, then handler ownership.

    // Try MSI first, fall back to legacy interrupt
    int const MSI_RESULT = pci::pci_enable_msi(pci_dev, vector);
    if (MSI_RESULT != 0) {
        // Use legacy IRQ
        log::warn("MSI not available, using legacy IRQ %d", pci_dev->interrupt_line);
        ker::mod::gates::free_irq(vector);
        vector = pci_dev->interrupt_line + 32;  // IRQ line + ISA offset
        dev->irq_vector = vector;
        dev->irq_owned = false;
        if (pci_dev->interrupt_line == 0 || pci_dev->interrupt_line >= 16) {
            return false;
        }
        ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
    }

    ker::net::napi_init(&dev->napi, &dev->netdev, e1000_poll, 64);
    if (ker::mod::gates::request_irq(vector, e1000_irq_handler, dev, "e1000e") != 0) {
        return false;
    }
    dev->irq_owned = true;

    // Set up NetDevice
    dev->netdev.mtu = 1500;
    dev->netdev.state = 1;  // UP
    dev->netdev.ops = &e1000_netdev_ops;
    dev->netdev.private_data = dev;
    dev->netdev.remotable = &S_REMOTABLE_OPS;

    // Register with network stack
    if (ker::net::netdev_register(&dev->netdev) != 0) {
        return false;
    }
    dev->published = true;

    // Initialize and enable NAPI for deferred packet processing
#ifdef WOS_SELFTEST
    if (!test_probing)
#endif
    {
        ker::net::napi_enable(&dev->napi);
        if (dev->napi.worker == nullptr) {
            return false;
        }
    }
    if (MSI_RESULT != 0) {
        auto command = pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, pci::PCI_COMMAND);
        command = static_cast<uint16_t>(command & ~pci::PCI_COMMAND_INT_DISABLE);
        pci::pci_config_write16(pci_dev->bus, pci_dev->slot, pci_dev->function, pci::PCI_COMMAND, command);
    }

    reg_write(dev, registers, REG_RCTL, RCTL_EN | RCTL_BAM | RCTL_BSIZE_2048 | RCTL_SECRC);
    reg_write(dev, registers, REG_TCTL, TCTL_EN | TCTL_PSP | (15U << TCTL_CT_SHIFT) | (64U << TCTL_COLD_SHIFT));

    // Clear any pending interrupts before enabling them
    (void)reg_read(dev, registers, REG_ICR);

    // Enable interrupts: RX timer, RX overrun, link status change, TX done
#ifdef WOS_SELFTEST
    if (!test_probing)
#endif
        reg_write(dev, registers, REG_IMS, ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_LSC | ICR_TXDW);

    log::info("%s initialized, MAC=%02x:%02x:%02x:%02x:%02x:%02x, IRQ=%d (%s) napi", name, dev->netdev.mac.at(0), dev->netdev.mac.at(1),
              dev->netdev.mac.at(2), dev->netdev.mac.at(3), dev->netdev.mac.at(4), dev->netdev.mac.at(5), dev->irq_vector,
              MSI_RESULT == 0 ? "MSI" : "legacy");
    return true;
}

auto prepare_register_views(E1000Device* dev, const Controller& controller) -> mmio::Status {
    namespace platform = ker::mod::mm::cpu_maintenance::detail::platform;
    constexpr uint64_t NEXT_CALL_BUDGET_NS = 5'000'000'000;
    constexpr unsigned MAX_CALLS = 131072;
    uint64_t started = 0;
    if (!platform::clock_now_ns(started) || started > UINT64_MAX - NEXT_CALL_BUDGET_NS) return mmio::Status::clock_unavailable;
    uint64_t const DEADLINE = started + NEXT_CALL_BUDGET_NS;
    uint64_t previous = started;
    auto status = mmio::Status::pending_retained;
    unsigned calls = 0;
    for (; calls < MAX_CALLS;) {
        uint64_t now = 0;
        if (!platform::clock_now_ns(now) || now < previous) {
            status = mmio::Status::clock_unavailable;
            break;
        }
        previous = now;
        if (now >= DEADLINE) break;
        ++calls;
        status = pci::map_cold_uc_82574(dev->driver_claim, controller, REGISTER_VIEWS, dev->registers);
        // Ready has already activated and published the mapping. A deadline
        // cannot retrospectively turn that into an unexposed cold failure.
        if (status != mmio::Status::pending_retained) break;
        if (!platform::clock_now_ns(now) || now < previous) {
            status = mmio::Status::clock_unavailable;
            break;
        }
        previous = now;
        if (now >= DEADLINE) break;
        // The actual call's ConfigGuard, operation and MM PIN scopes have all
        // ended. Keep the same affine claim/plan/controller for the next step.
        asm volatile("pause" ::: "memory");
    }
    log::info("MMIO_BOOT_PREPARE attempts=%u status=%s exhausted=%u", calls, mmio::status_name(status),
              status == mmio::Status::pending_retained ? 1U : 0U);
    return status;
}

// Published objects have kernel lifetime, including failed probes with an IRQ
// reader or NAPI snapshot. Never reuse a retired PCI device in this boot.
void init_device(pci::PCIDevice* pci_dev, const char* name, const Controller* controller = nullptr) {
    if (shutdown_started.load(std::memory_order_acquire) || device_count >= MAX_E1000_DEVICES) return;
    for (size_t i = 0; i < device_count; ++i)
        if (devices[i]->pci == pci_dev) return;
    bool const TYPED = mmio::enabled() && pci_dev->device_id == 0x10d3;
    if (TYPED && controller == nullptr) {
        log::error("cold UC probe requires the actual init controller");
        return;
    }
    auto claim = pci::try_claim_driver(pci_dev, pci::DriverKind::e1000e);
    if (!claim) return;
    // Preserve the original legacy route. The typed factory must observe the
    // initial firmware BME state before any driver-side command write.
    if (!TYPED && !pci::pci_set_bus_master(pci_dev, false)) return;
    auto* dev = new (std::nothrow) E1000Device{};
    if (dev == nullptr) return;
    dev->pci = pci_dev;
    dev->typed_mmio = TYPED;
    dev->driver_claim = std::move(*claim);
    devices[device_count++] = dev;
    if (TYPED) {
        auto* actor = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
        log::info("MMIO_BOOT_PROBE device=%x controller=1 task_present=%u actor_pid=%llu actor_type=%u if=%u", pci_dev->device_id,
                  actor != nullptr, actor != nullptr ? actor->pid : UINT64_MAX,
                  actor != nullptr ? static_cast<unsigned>(actor->type) : UINT32_MAX, ker::mod::sched::interrupts_enabled());
        auto const STATUS = prepare_register_views(dev, *controller);
        if (STATUS != mmio::Status::ready) {
            // Private PCI preparation retains any physical/cache obligations.
            // No public mapping or register view exists and no raw fallback is
            // allowed, so this is not a hardware-quiescence continuation.
            dev->admission.close();
            dev->driver_claim.close_data_admission();
            dev->registers.close();
            dev->retirement.store(E1000Device::Retirement::cold_failed, std::memory_order_release);
            log::error("cold UC probe retained id=0x%x status=%s", pci_dev->device_id, mmio::status_name(STATUS));
            return;
        }
    } else {
        pci::pci_enable_memory_space(pci_dev);
        dev->mmio = static_cast<volatile uint32_t*>(pci::pci_map_bar(pci_dev, 0));
    }
    if (!initialize_registers(dev, name)) (void)retire_device(dev, controller);
}

#ifdef WOS_SELFTEST
uint8_t test_packet_byte(size_t offset, size_t bytes, bool reply) {
    if (offset < 6) return 0xff;
    if (offset < 12) return offset == 6 ? 2 : (offset == 11 ? 1 : 0);
    if (offset == 12) return 0x88;
    if (offset == 13) return 0xb5;
    constexpr std::string_view MARKER = "WOSDMA21";
    if (offset < 22) return static_cast<uint8_t>(MARKER[offset - 14]);
    if (offset == 22) return reply ? 2 : 1;
    if (offset == 23) return 0;
    if (offset < 28) return static_cast<uint8_t>(bytes >> ((offset - 24) * 8));
    return static_cast<uint8_t>((offset * 17 + bytes) & 255);
}

auto test_rx(ker::net::PacketBuffer* pkt) -> bool {
    if (pkt->len < 28 || pkt->data[12] != 0x88 || pkt->data[13] != 0xb5 || std::memcmp(pkt->data + 14, "WOSDMA21", 8) != 0 ||
        pkt->data[22] != 2)
        return false;
    bool valid = pkt->len == 64 || pkt->len == 1514;
    for (size_t i = 0; valid && i < pkt->len; ++i) valid = pkt->data[i] == test_packet_byte(i, pkt->len, true);
    if (valid && test_result != nullptr) {
        test_result->packets_verified |= pkt->len == 64 ? 1U : 2U;
        ++test_result->echoes;
    }
    ker::net::pkt_free(pkt);
    return true;
}

void test_packets(E1000Device* dev) {
    for (size_t iteration = 0; iteration < 300; ++iteration) {
        size_t const bytes = (iteration & 1U) == 0 ? 64 : 1514;
        size_t const expected_echoes = test_result->echoes + 1;
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) return;
        pkt->data = pkt->storage.data();
        pkt->len = bytes;
        for (size_t i = 0; i < bytes; ++i) pkt->data[i] = test_packet_byte(i, bytes, false);
        if (e1000_start_xmit(&dev->netdev, pkt) != 0) return;
        auto const START = ker::mod::time::get_ms();
        for (size_t n = 0; n < 100'000'000 && ker::mod::time::get_ms() - START < 5000; ++n) {
            CallbackGuard const GUARD(dev);
            if (!GUARD) return;
            auto const& registers = GUARD.registers();
            (void)process_rx_budget(dev, registers, 64);
            auto const FLAGS = dev->tx_lock.lock_irqsave();
            process_tx(dev, registers);
            dev->tx_lock.unlock_irqrestore(FLAGS);
            if (test_result->echoes == expected_echoes) break;
            asm volatile("pause" ::: "memory");
        }
    }
}
#endif

#ifdef WOS_SELFTEST
struct LiveDmaTest {
    std::atomic<E1000Device*> device{nullptr};
    std::atomic<uint64_t> small{0}, large{0}, bad{0}, worker_rx{0}, irqs{0}, sent{0};
    std::atomic<uint64_t> sender_cpu{UINT64_MAX}, receive_cpus{0};
    std::atomic<bool> stop_sender{false}, holder_entered{false}, release_holder{false}, holder_released{false};
    std::atomic<bool> holder_returned_after_exit_point{false}, retire_started{false}, retire_done{false};
    std::atomic<uint32_t> protected_yields{0};
    std::atomic<int> retirement_result{-EBUSY};
};
LiveDmaTest live_test;

void selftest_live_irq(E1000Device* dev) {
    if (live_test.device.load(std::memory_order_acquire) == dev) live_test.irqs.fetch_add(1, std::memory_order_relaxed);
}

auto selftest_live_rx(E1000Device* dev, ker::net::PacketBuffer* pkt) -> bool {
    if (live_test.device.load(std::memory_order_acquire) != dev || pkt->len < 28 || pkt->data[12] != 0x88 || pkt->data[13] != 0xb5 ||
        std::memcmp(pkt->data + 14, "WOSDMA21", 8) != 0 || pkt->data[22] != 2)
        return false;
    bool valid = pkt->len == 64 || pkt->len == 1514;
    for (size_t i = 0; valid && i < pkt->len; ++i) valid = pkt->data[i] == test_packet_byte(i, pkt->len, true);
    if (valid) {
        auto& count = pkt->len == 64 ? live_test.small : live_test.large;
        count.fetch_add(1, std::memory_order_relaxed);
        uint64_t const CPU = ker::mod::cpu::current_cpu();
        if (CPU < 64) live_test.receive_cpus.fetch_or(uint64_t{1} << CPU, std::memory_order_relaxed);
        auto* current = ker::mod::sched::get_current_task();
        if (current != nullptr && current == dev->napi.worker) live_test.worker_rx.fetch_add(1, std::memory_order_relaxed);
    } else {
        live_test.bad.fetch_add(1, std::memory_order_relaxed);
    }
    ker::net::pkt_free(pkt);
    return true;
}

template <typename Predicate>
auto live_wait(Predicate predicate, uint64_t timeout_ms = 10000) -> bool {
    uint64_t const DEADLINE = (ker::mod::time::get_ms() + timeout_ms);
    while (!predicate()) {
        if (ker::mod::time::get_ms() >= DEADLINE) return false;
        ker::mod::sched::kern_sleep_us(1000);
    }
    return true;
}

void selftest_live_sender_body() {
    auto* dev = live_test.device.load(std::memory_order_acquire);
    live_test.sender_cpu.store(ker::mod::cpu::current_cpu(), std::memory_order_release);
    for (size_t n = 0; n < 20000 && !live_test.stop_sender.load(std::memory_order_acquire); ++n) {
        size_t const SIZE = (n & 1U) == 0 ? 64 : 1514;
        if (auto* pkt = ker::net::pkt_alloc_tx()) {
            pkt->len = SIZE;
            for (size_t i = 0; i < SIZE; ++i) pkt->data[i] = test_packet_byte(i, SIZE, false);
            if (e1000_start_xmit(&dev->netdev, pkt) == 0) live_test.sent.fetch_add(1, std::memory_order_relaxed);
        }
        ker::mod::sched::kern_sleep_us(1000);
    }
}

[[noreturn]] void selftest_live_sender() {
    selftest_live_sender_body();
    ker::mod::sched::exit_current_kernel_thread();
}

void selftest_live_holder_body() {
    auto* dev = live_test.device.load(std::memory_order_acquire);
    {
        CallbackGuard const GUARD(dev);
        if (!GUARD) return;
        if (dev->typed_mmio && reg_read(dev, GUARD.registers(), REG_STATUS) == UINT32_MAX) return;
        live_test.holder_entered.store(true, std::memory_order_release);
        uint64_t const DEADLINE = (ker::mod::time::get_ms() + 20000);
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

[[noreturn]] void selftest_live_holder() {
    selftest_live_holder_body();
    ker::mod::sched::exit_current_kernel_thread();
}

void selftest_live_retirer_body() {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    live_test.retire_started.store(true, std::memory_order_release);
    live_test.retirement_result.store(e1000e_shutdown(), std::memory_order_release);
    live_test.retire_done.store(true, std::memory_order_release);
}

[[noreturn]] void selftest_live_retirer() {
    selftest_live_retirer_body();
    ker::mod::sched::exit_current_kernel_thread();
}

void discard_unpublished_helper(ker::mod::sched::task::Task* task) {
    using Task = ker::mod::sched::task::Task;
    if (!task->scheduler_published.load(std::memory_order_acquire) && task->ref_count.load(std::memory_order_acquire) == 1 &&
        task->sched_queue == Task::sched_queue::NONE && task->heap_index == -1 && task->sched_next == nullptr) {
        ker::mod::sched::task::destroy_unpublished_user_thread(task);
        return;
    }
    // PID/active publication rollback can leave an already admitted observer.
    // Keep the initial owner until reboot instead of deleting a borrowed Task.
    log::error("selftest helper retains uncertain unpublished lifetime pid=%lu refs=%u", task->pid,
               task->ref_count.load(std::memory_order_acquire));
}

auto live_post(const char* name, void (*entry)(), uint64_t cpu) -> ker::mod::sched::task::Task* {
    auto* task = ker::mod::sched::task::Task::create_kernel_thread(name, entry);
    if (task == nullptr) return nullptr;
    if (!task->try_acquire()) {
        discard_unpublished_helper(task);
        return nullptr;
    }
    if (!ker::mod::sched::post_task_pinned_cpu(cpu, task)) {
        task->release();
        discard_unpublished_helper(task);
        return nullptr;
    }
    return task;  // Caller owns a reference, including after cooperative exit.
}

void selftest_fault_runner(E1000Device* dev, bool positive, ker::mod::sched::task::Task* sender) {
    namespace iommu = ker::dev::iommu;
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
                ok = e1000_start_xmit(&dev->netdev, packet) == 0;
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
    int const RETIRED = e1000e_shutdown();
    auto const AFTER = dma::stats(dev->pci);
    bool guards = WITHHELD;
    if (!WITHHELD && allocation.cpu != nullptr && !dev->irq_owned && dev->admission.drained()) {
        guards = true;
        for (size_t i = 0; i < 4096; ++i) guards &= allocation.cpu[i] == std::byte{0xa5};
    }
    size_t const EXTRA = STALE || WITHHELD ? 1 : 0;
    bool const PINNED = AFTER.regions == BEFORE.regions + EXTRA && AFTER.backing_pages == BEFORE.backing_pages + EXTRA &&
                        AFTER.quarantined == AFTER.regions;
    ok = ok && CLOSED && inhibited && REJECTED && RETIRED == -EIO && !dev->irq_owned && dev->admission.drained() && guards && PINNED &&
         (WITHHELD ? report == nullptr && STATUS.irq_count == 0 : matched && STATUS.irq_count != 0 && fault_override.load() == 0);
    log::info(
        "forbidden DMA %s case=%s address=0x%llx allocation_generation=%llu closed=%u inhibited=%u irq=%llu matched=%u guards=%u "
        "rejected=%u retired=%d irq_owned=%u regions=%zu->%zu pages=%zu->%zu q=%zu",
        ok ? "PASS" : "FAIL",
        WITHHELD ? "withheld-invalidation"
        : STALE  ? "stale"
                 : "foreign",
        static_cast<unsigned long long>(address), static_cast<unsigned long long>(allocation.handle.generation), CLOSED, inhibited,
        static_cast<unsigned long long>(STATUS.irq_count), matched, guards, REJECTED, RETIRED, dev->irq_owned, BEFORE.regions,
        AFTER.regions, BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
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
    bool ok = BEFORE.regions == 514 && BEFORE.streaming == 512 && sender != nullptr && live_wait([] {
                  return live_test.small.load(std::memory_order_acquire) >= 160 && live_test.large.load(std::memory_order_acquire) >= 160 &&
                         live_test.worker_rx.load(std::memory_order_acquire) != 0 && live_test.irqs.load(std::memory_order_acquire) != 0;
              });
    if (test_flag("--iommu-test-fault-foreign") || test_flag("--iommu-test-fault-stale") || test_flag("--iommu-test-withhold-invalidation"))
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
    log::info("e1000 live shutdown requested=%zu holder=%u retirer=%u", REQUEST.requested,
              holder != nullptr && holder->kernel_shutdown_requested.load(std::memory_order_acquire),
              retirer != nullptr && retirer->kernel_shutdown_requested.load(std::memory_order_acquire));
    bool const DEFERRED = ok && live_wait([] { return live_test.protected_yields.load(std::memory_order_acquire) >= 2; });
    live_test.stop_sender.store(true, std::memory_order_release);
    live_test.release_holder.store(true, std::memory_order_release);
    bool const RETIRED = retirer != nullptr && live_wait([] { return live_test.retire_done.load(std::memory_order_acquire); });
    bool const OWNER_EXITED = holder != nullptr && live_wait([holder] { return holder->exit_in_progress.load(std::memory_order_acquire); });
    if (!RETIRED) (void)e1000e_shutdown();
    auto const AFTER = dma::stats(dev->pci);
    bool const QUARANTINE = test_withhold || dev->pci->device_id != 0x10d3;
    bool const ACCOUNTED =
        QUARANTINE ? AFTER.regions == BEFORE.regions && AFTER.quarantined == BEFORE.regions && AFTER.backing_pages == BEFORE.backing_pages
                   : AFTER.regions == 0 && AFTER.backing_pages == 0 && AFTER.quarantined == 0;
    bool owners_detached = RETIRED && dev->admission.drained();
    bool const irqs_drained = RETIRED && !dev->irq_owned;
    if (owners_detached) {
        for (auto* pkt : dev->rx_bufs) owners_detached = owners_detached && pkt == nullptr;
        for (auto* pkt : dev->tx_bufs) owners_detached = owners_detached && pkt == nullptr;
    }
    bool const BME_OFF =
        (pci::pci_config_read16(dev->pci->bus, dev->pci->slot, dev->pci->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) == 0;
    auto* late = ker::net::pkt_alloc();
    bool const LATE_REJECTED = late != nullptr && e1000_start_xmit(&dev->netdev, late) == -ENODEV && e1000_open(&dev->netdev) == -ENODEV;
    uint64_t const RX_CPUS = live_test.receive_cpus.load(std::memory_order_acquire);
    uint64_t const TX_CPU = live_test.sender_cpu.load(std::memory_order_acquire);
    bool const MULTICPU = TX_CPU < 64 && (RX_CPUS & ~(uint64_t{1} << TX_CPU)) != 0;
    int const EXPECTED = QUARANTINE ? -EIO : 0;
    log::info("e1000 live checks setup=%u released=%u returned=%u accounted=%u multicpu=%u bad=%lu result=%d expected=%d repeat=%d", ok,
              live_test.holder_released.load(), live_test.holder_returned_after_exit_point.load(), ACCOUNTED, MULTICPU,
              live_test.bad.load(), live_test.retirement_result.load(), EXPECTED, e1000e_shutdown());
    ok = ok && DRAIN_WAITED && DEFERRED && RETIRED && OWNER_EXITED && live_test.holder_released.load(std::memory_order_acquire) &&
         !live_test.holder_returned_after_exit_point.load(std::memory_order_acquire) && ACCOUNTED && owners_detached && irqs_drained &&
         LATE_REJECTED && BME_OFF && MULTICPU && live_test.bad.load(std::memory_order_acquire) == 0 &&
         live_test.retirement_result.load(std::memory_order_acquire) == EXPECTED && e1000e_shutdown() == EXPECTED;
    log::info("e1000 live DMA %s sent=%lu rx64=%lu rx1514=%lu worker_rx=%lu irq=%lu tx_cpu=%lu rx_cpus=0x%lx", ok ? "PASS" : "FAIL",
              live_test.sent.load(), live_test.small.load(), live_test.large.load(), live_test.worker_rx.load(), live_test.irqs.load(),
              TX_CPU, RX_CPUS);
    log::info(
        "e1000 live retirement waited=%u deferred=%u owner_exit=%u done=%u detached=%u irq_drained=%u late=%u bme_off=%u regions=%zu->%zu "
        "pages=%zu->%zu q=%zu",
        DRAIN_WAITED, DEFERRED, OWNER_EXITED, RETIRED, owners_detached, irqs_drained, LATE_REJECTED, BME_OFF, BEFORE.regions, AFTER.regions,
        BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
    if (sender != nullptr) sender->release();
    if (holder != nullptr) holder->release();
    if (retirer != nullptr) retirer->release();
    ker::mod::power::selftest_shutdown();
}

void selftest_live_start(E1000Device* dev) {
    if (ker::mod::smt::get_core_count() < 4 || !dev->published || dev->napi.worker == nullptr) {
        log::error("e1000 live DMA requires four CPUs and a NAPI worker");
        return;
    }
    E1000Device* expected = nullptr;
    if (!live_test.device.compare_exchange_strong(expected, dev, std::memory_order_acq_rel)) return;
    test_withhold = test_flag("--e1000-test-live-withhold-stop");
    auto* runner = live_post("dma_live_test", selftest_live_runner, 0);
    if (runner != nullptr)
        runner->release();
    else
        log::error("e1000 live DMA coordinator allocation failed");
}

#endif

}  // namespace

auto detail::MmioRetirementAccess::retire(E1000Device* dev, const Controller* controller) -> int {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    if (controller != nullptr && controller->context_status() != ker::mod::mm::cpu_maintenance::Status::ready) return -EWOULDBLOCK;
    using Retirement = E1000Device::Retirement;
    auto expected = Retirement::live;
    bool retry = false;
    if (!dev->retirement.compare_exchange_strong(expected, Retirement::in_progress, std::memory_order_acq_rel)) {
        if (expected != Retirement::withdrawal_pending)
            return expected == Retirement::released ? 0 : (expected == Retirement::in_progress ? -EBUSY : -EIO);
        if (!dev->retirement.compare_exchange_strong(expected, Retirement::in_progress, std::memory_order_acq_rel)) return -EBUSY;
        retry = true;
    }
    if (!retry) {
        auto const BEFORE = dma::stats(dev->pci);
        bool hardware_released = false;
        {
            // The unique terminal view survives the last register read AND
            // actual IRQ/DMA cleanup. Its destructor precedes witness issue.
            RetirementBackend backend{dev};
            auto const RESULT = dma::lifecycle::retire(backend);
            auto const AFTER = dma::stats(dev->pci);
            hardware_released = RESULT == dma::lifecycle::Result::released && backend.stopped && backend.masked && backend.bme_off &&
                                !dev->irq_owned && dev->admission.drained() && dev->driver_claim.operations_drained() && AFTER.regions == 0;
            log::info(
                "DMA retirement id=0x%x result=%u stopped=%u masked=%u bme_off=%u irq_drained=%u regions=%zu->%zu pages=%zu->%zu q=%zu",
                dev->pci->device_id, static_cast<unsigned>(RESULT), backend.stopped, backend.masked, backend.bme_off, !dev->irq_owned,
                BEFORE.regions, AFTER.regions, BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
        }
        if (!hardware_released) {
            dev->retirement.store(Retirement::failed, std::memory_order_release);
            return -EIO;
        }
        if (!dev->typed_mmio) {
            dev->retirement.store(Retirement::released, std::memory_order_release);
            return 0;
        }
    }
    // Retry reaches only this continuation: the prior real lifecycle already
    // released DMA backing. It never touches a BAR, repeats stop, or rebuilds a
    // maintenance ticket. Mapping owns the original withdrawal transaction.
    if (!dev->typed_mmio || dev->registers.empty() || !dev->admission.drained() || !dev->driver_claim.operations_drained() ||
        !dev->registers.borrows_drained() || dev->irq_owned || dma::stats(dev->pci).regions != 0) {
        dev->retirement.store(Retirement::failed, std::memory_order_release);
        return -EIO;
    }
    auto const BEFORE_WITHDRAWAL = dev->registers.report();
    if (BEFORE_WITHDRAWAL.terminal_active) {
        dev->retirement.store(Retirement::withdrawal_pending, std::memory_order_release);
        return -EAGAIN;
    }
    auto* actor = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    log::info("MMIO hardware released id=%llu retry=%u controller=%u actor_pid=%llu actor_type=%u terminal=0 readers=%llu ticket=%llu",
              BEFORE_WITHDRAWAL.id, retry, controller != nullptr, actor != nullptr ? actor->pid : UINT64_MAX,
              actor != nullptr ? static_cast<unsigned>(actor->type) : UINT32_MAX, BEFORE_WITHDRAWAL.normal_readers,
              BEFORE_WITHDRAWAL.withdrawal_ticket);
    mmio::QuiescedMapping const QUIESCED(dev->registers);
    auto const STATUS = controller != nullptr ? dev->registers.withdraw(*controller, QUIESCED) : dev->registers.withdraw(QUIESCED);
    auto const REPORT = dev->registers.report();
    bool const COMPLETE = STATUS == mmio::Status::complete;
    dev->retirement.store(COMPLETE ? Retirement::released : Retirement::withdrawal_pending, std::memory_order_release);
    log::info("MMIO retirement id=%llu retry=%u status=%s phase=%u ticket=%llu readers=%llu terminal=%u closed=%u timed_out=%u views=%zu",
              REPORT.id, retry, mmio::status_name(STATUS), static_cast<unsigned>(REPORT.phase), REPORT.withdrawal_ticket,
              REPORT.normal_readers, REPORT.terminal_active, REPORT.admission_closed, REPORT.timed_out_ever, REPORT.view_count);
    if (COMPLETE) return 0;
    return STATUS == mmio::Status::failed_retained || STATUS == mmio::Status::bad_hierarchy || STATUS == mmio::Status::decode_readback
               ? -EIO
               : -EAGAIN;
}

#ifdef WOS_SELFTEST
auto e1000e_mmio_runtime_check() -> bool {
    namespace sched = ker::mod::sched;
    using TaskState = sched::task::TaskState;
    auto const HOLD = sched::hold_kernel_shutdown();
    if (!mmio::enabled() || !ker::mod::mm::phys::can_wait_for_reclaim() || ker::mod::smt::get_core_count() < 4 ||
        test_flag("--e1000-test-live"))
        return false;
    E1000Device* dev = nullptr;
    for (size_t i = 0; i < device_count; ++i) {
        if (!devices[i]->typed_mmio) continue;
        if (dev != nullptr) return false;
        dev = devices[i];
    }
    if (dev == nullptr || dev->pci->device_id != 0x10d3 || !dev->published || dev->napi.worker == nullptr || dev->mmio != nullptr ||
        dev->retirement.load(std::memory_order_acquire) != E1000Device::Retirement::live)
        return false;
    auto const MAPPED = dev->registers.report();
    auto const DMA = dma::stats(dev->pci);
    if (MAPPED.phase != mmio::Phase::active || MAPPED.id == 0 || MAPPED.initial_ticket == 0 || MAPPED.view_count != REGISTER_VIEWS.size() ||
        MAPPED.admission_closed || MAPPED.terminal_active || DMA.regions != 514 || DMA.streaming != 512 ||
        MAPPED.resource_visits < MAPPED.physical_spans || MAPPED.resource_epoch == 0 || MAPPED.resource_root_frame == 0 ||
        (test_flag("--selftest-e1000-mmio-admission-steps") && MAPPED.resource_steps < MAPPED.physical_spans))
        return false;
    for (size_t i = 0; i < REGISTER_VIEWS.size(); ++i) {
        auto const& request = REGISTER_VIEWS[i];
        auto const& view = MAPPED.views[i];
        if (view.status != mmio::Status::ready || view.phase != mmio::Phase::active || view.bar != request.bar ||
            view.bar_offset != request.offset || view.physical != MAPPED.physical + request.offset || view.bytes != request.bytes ||
            view.initial_ticket == 0 || view.cohort_incarnation != MAPPED.cohort_incarnation)
            return false;
    }
    uint32_t status = UINT32_MAX;
    uint32_t secondary_gcr = UINT32_MAX;
    {
        auto borrow = dev->registers.borrow();
        if (!borrow.valid()) return false;
        mmio::Borrow moved{std::move(borrow)};
        if (borrow.valid() || !moved.valid()) return false;
        status = moved.read32(0, REG_STATUS);
        secondary_gcr = reg_read(dev, moved, 0x5b00);
        // The genuine whole-function Borrow keeps every observed child stable
        // while this read-only walk checks real leaves and unmapped guards.
        namespace virt = ker::mod::mm::virt;
        auto* const ROOT = virt::get_kernel_pagemap();
        if (ROOT == nullptr) return false;
        for (size_t i = 0; i < REGISTER_VIEWS.size(); ++i) {
            auto const& view = MAPPED.views[i];
            for (uint64_t offset = 0; offset < view.bytes; offset += 4096)
                if (virt::translate(ROOT, view.virtual_base + offset) != view.physical + offset) return false;
            if (virt::translate(ROOT, view.virtual_base - 4096) != virt::PADDR_INVALID ||
                virt::translate(ROOT, view.virtual_base + view.bytes) != virt::PADDR_INVALID)
                return false;
        }
        uint64_t const HHDM = ker::mod::mm::addr::get_hhdm_offset();
        if (virt::translate(ROOT, HHDM + MAPPED.physical + 0x1000) != virt::PADDR_INVALID ||
            virt::translate(ROOT, HHDM + MAPPED.physical + 0x6000) != virt::PADDR_INVALID)
            return false;
        moved.reset();
        if (moved.valid() || status == UINT32_MAX || secondary_gcr == UINT32_MAX) return false;
    }
    E1000Device* expected = nullptr;
    if (!live_test.device.compare_exchange_strong(expected, dev, std::memory_order_acq_rel)) return false;
    auto* sender = live_post("mmio_live_tx", selftest_live_sender, 1);
    auto* holder = live_post("mmio_live_borrow", selftest_live_holder, 2);
    bool const ENTERED =
        sender != nullptr && holder != nullptr && live_wait([] {
            return live_test.holder_entered.load(std::memory_order_acquire) && live_test.small.load(std::memory_order_acquire) >= 160 &&
                   live_test.large.load(std::memory_order_acquire) >= 160 && live_test.worker_rx.load(std::memory_order_acquire) != 0 &&
                   live_test.irqs.load(std::memory_order_acquire) != 0;
        });
    auto const HELD = dev->registers.report();
    bool const COUNTED = ENTERED && HELD.id == MAPPED.id && HELD.phase == mmio::Phase::active && HELD.normal_readers != 0 &&
                         !HELD.admission_closed && !HELD.terminal_active;
    live_test.stop_sender.store(true, std::memory_order_release);
    live_test.release_holder.store(true, std::memory_order_release);
    bool const JOINED = sender != nullptr && holder != nullptr && live_wait([sender, holder] {
                            return sender->state.load(std::memory_order_acquire) == TaskState::DEAD &&
                                   holder->state.load(std::memory_order_acquire) == TaskState::DEAD;
                        });
    // A failed bounded join preserves Task refs and device storage. It cannot
    // fall through to a shutdown claiming the helpers released their borrows.
    if (!JOINED) return false;
    sender->release();
    holder->release();
    auto const FINAL = dev->registers.report();
    auto const TX_CPU = live_test.sender_cpu.load(std::memory_order_acquire);
    auto const RX_CPUS = live_test.receive_cpus.load(std::memory_order_acquire);
    bool const OK = COUNTED && live_test.holder_released.load(std::memory_order_acquire) && live_test.bad.load() == 0 && TX_CPU < 64 &&
                    (RX_CPUS & ~(uint64_t{1} << TX_CPU)) != 0 && FINAL.id == MAPPED.id && FINAL.phase == mmio::Phase::active &&
                    FINAL.view_count == MAPPED.view_count && !FINAL.admission_closed && !FINAL.terminal_active &&
                    dev->retirement.load(std::memory_order_acquire) == E1000Device::Retirement::live;
    for (size_t i = 0; i < MAPPED.view_count; ++i) {
        auto const& view = MAPPED.views[i];
        log::info(
            "E1000_MMIO_RUNTIME_VIEW mapping=%llu view=%zu bar=%u bar_offset=%llx pa=%llx va=%llx bytes=%llu initial_ticket=%llu "
            "phase=%u pte_pages=%llu guards_absent=1",
            MAPPED.id, i, view.bar, view.bar_offset, view.physical, view.virtual_base, view.bytes, view.initial_ticket,
            static_cast<unsigned>(view.phase), view.bytes / 4096);
    }
    log::info(
        "E1000_MMIO_RUNTIME %s id=%llu pa=%llx va=%llx bytes=%llu initial_ticket=%llu status=%x raw_export=0 helpers_joined=%u "
        "held_readers=%llu rx64=%llu rx1514=%llu worker_rx=%llu irq=%llu tx_cpu=%llu rx_cpus=%llx regions=%zu first_retirement_pending=1 "
        "views=%zu sparse_pages=5 hhdm_gap_absent=1 hhdm_tail_absent=1 secondary_gcr=%x resource_steps=%llu resource_visits=%llu "
        "resource_restarts=%llu resource_epoch=%llu resource_root=%llx",
        OK ? "PASS" : "FAIL", MAPPED.id, MAPPED.physical, MAPPED.virtual_base, MAPPED.bytes, MAPPED.initial_ticket, status, JOINED,
        HELD.normal_readers, live_test.small.load(), live_test.large.load(), live_test.worker_rx.load(), live_test.irqs.load(), TX_CPU,
        RX_CPUS, DMA.regions, MAPPED.view_count, secondary_gcr, MAPPED.resource_steps, MAPPED.resource_visits, MAPPED.resource_restarts,
        MAPPED.resource_epoch, MAPPED.resource_root_frame);
    return OK;
}

auto e1000e_selftest() -> DmaTestResult {
    DmaTestResult result;
    auto* pci_dev = pci::pci_find_by_class(pci::PCI_CLASS_NETWORK, pci::PCI_SUBCLASS_ETHERNET);
    if (pci_dev == nullptr || pci_dev->vendor_id != 0x8086 || find_device_name(pci_dev->device_id) == nullptr) return result;
    result.found = true;
    result.release_supported = pci_dev->device_id == 0x10d3;
    ker::net::pkt_pool_init();
    result.pool_before = ker::net::pkt_pool_free_count();
    result.fail_prepare = test_flag("--e1000-test-fail-prepare");
    result.withheld = test_flag("--e1000-test-withhold-stop");
    result.packets_requested = test_flag("--e1000-test-packets");
    result.before = dma::stats(pci_dev);
    test_probing = true;
    test_withhold = result.withheld;
    test_result = &result;
    test_allocations_left = result.fail_prepare ? 17 : SIZE_MAX;
    auto const PREVIOUS = device_count;
    init_device(pci_dev, find_device_name(pci_dev->device_id));
    if (device_count > PREVIOUS) {
        auto* dev = devices[PREVIOUS];
        result.prepared = dev->published;
        result.mapped = dma::stats(pci_dev);
        if (result.prepared && result.packets_requested) test_packets(dev);
        result.retirement_result = e1000e_shutdown();
        auto* late = ker::net::pkt_alloc();
        result.late_rejected = late != nullptr && e1000_start_xmit(&dev->netdev, late) == -ENODEV && e1000_open(&dev->netdev) == -ENODEV;
        result.irq_drained = !dev->irq_owned;
        result.bme_disabled =
            (pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) == 0;
    }
    result.after = dma::stats(pci_dev);
    result.pool_after = ker::net::pkt_pool_free_count();
    int const AGAIN = e1000e_shutdown();
    auto const REPEATED = dma::stats(pci_dev);
    result.repeat_stable = AGAIN == result.retirement_result && REPEATED.regions == result.after.regions &&
                           REPEATED.backing_pages == result.after.backing_pages && REPEATED.quarantined == result.after.quarantined;
    log::info("DMA test echoes=%zu pool=%zu->%zu repeat=%u", result.echoes, result.pool_before, result.pool_after, result.repeat_stable);
    log::info(
        "DMA test prepared=%u fail_prepare=%u withheld=%u packets=%u verified=%u late=%u irq_drained=%u bme_off=%u "
        "regions=%zu->%zu pages=%zu->%zu q=%zu",
        result.prepared, result.fail_prepare, result.withheld, result.packets_requested, result.packets_verified, result.late_rejected,
        result.irq_drained, result.bme_disabled, result.mapped.regions, result.after.regions, result.mapped.backing_pages,
        result.after.backing_pages, result.after.quarantined);
    test_result = nullptr;
    test_probing = false;
    return result;
}
#endif

namespace {
auto shutdown_devices(const Controller* controller) -> int {
    if (ker::mod::sched::can_query_current_task() && !ker::mod::mm::phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    shutdown_started.store(true, std::memory_order_release);
    int result = 0;
    for (size_t i = 0; i < device_count; ++i) {
        int const RET = retire_device(devices[i], controller);
        if (RET != 0) result = RET;
    }
    return result;
}

void initialize_devices(const Controller* controller) {
    // Scan all PCI devices for supported Intel NICs
    size_t const COUNT = pci::pci_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }
        if (dev->vendor_id != 0x8086) {
            continue;
        }
        if (dev->class_code != pci::PCI_CLASS_NETWORK || dev->subclass_code != pci::PCI_SUBCLASS_ETHERNET) {
            continue;
        }
        const char* name = find_device_name(dev->device_id);
        if (name != nullptr) {
            log::info("found %s (device 0x%x) at %d:%d.%d", name, dev->device_id, dev->bus, dev->slot, dev->function);
            init_device(dev, name, controller);
        }
    }

    if (device_count == 0) {
        log::info("no supported Intel NIC found");
    }
#ifdef WOS_SELFTEST
    if (device_count != 0 && test_flag("--e1000-test-live")) selftest_live_start(devices[0]);
#endif
}
}  // namespace

auto e1000e_shutdown() -> int { return shutdown_devices(nullptr); }
auto e1000e_shutdown(const Controller& controller) -> int { return shutdown_devices(&controller); }
void e1000e_init() { initialize_devices(nullptr); }
void e1000e_init(const Controller& controller) { initialize_devices(&controller); }

}  // namespace ker::dev::e1000e
