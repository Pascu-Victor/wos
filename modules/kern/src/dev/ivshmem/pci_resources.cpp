#include "pci_resources.hpp"

#include <cerrno>
#include <dev/iommu/domains.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#ifdef WOS_SELFTEST
#include <platform/init/limine_requests.hpp>
#include <string_view>
#endif

namespace ker::dev::ivshmem {
#ifdef WOS_SELFTEST
auto selftest_flag(const char* token) -> bool {
    auto* cmd = ker::init::get_kernel_cmdline();
    if (cmd == nullptr) return false;
    std::string_view text(cmd);
    std::string_view const TOKEN(token);
    for (size_t pos = text.find(TOKEN); pos != std::string_view::npos; pos = text.find(TOKEN, pos + 1)) {
        auto const END = pos + TOKEN.size();
        if ((pos == 0 || text[pos - 1] == ' ') && (END == text.size() || text[END] == ' ')) return true;
    }
    return false;
}
auto pci_selftest_reserved() -> bool { return selftest_flag("--ivshmem-test-pci"); }
#endif
namespace {
using log = mod::dbg::logger<"ivshmem-pci">;
auto read16(pci::PCIDevice* p, uint8_t offset) -> uint16_t { return pci::pci_config_read16(p->bus, p->slot, p->function, offset); }
void write16(pci::PCIDevice* p, uint8_t offset, uint16_t value) { pci::pci_config_write16(p->bus, p->slot, p->function, offset, value); }
auto valid_bar(pci::PCIDevice* p, int index, uint64_t minimum) -> bool {
    if ((p->bar.at(static_cast<size_t>(index)) & 1U) != 0) return false;
    return model::bar(pci::pci_get_bar_addr(p, index), pci::pci_get_bar_size(p, index), minimum);
}
}  // namespace

auto PciResources::prepare(pci::PCIDevice* pci_device, size_t minimum_shared) -> bool {
    if (callbacks.closed() || device != nullptr || pci_device == nullptr) return false;
    device = pci_device;
    if (!pci::pci_set_bus_master(device, false) || !iommu::bus_master_allowed(device)) return false;
    auto const REVISION = pci::pci_config_read8(device->bus, device->slot, device->function, 8);
    msix_cap = pci::pci_find_capability(device, pci::PCI_CAP_ID_MSIX);
    mode = model::mode(REVISION, msix_cap != 0);
    if (mode == model::Mode::unsupported || (msix_cap != 0 && (msix_cap < 0x40 || msix_cap > 0xf4 || (msix_cap & 3U) != 0))) return false;
    if (!mask_interrupts()) return false;
    if (!valid_bar(device, 0, 256) || !valid_bar(device, 2, minimum_shared)) return false;
    if (mode == model::Mode::msix) {
        auto const TABLE = pci::pci_config_read32(device->bus, device->slot, device->function, static_cast<uint8_t>(msix_cap + 4));
        auto const PBA = pci::pci_config_read32(device->bus, device->slot, device->function, static_cast<uint8_t>(msix_cap + 8));
        if (!valid_bar(device, 1, 16) ||
            !model::msix_layout(msix_cap, read16(device, static_cast<uint8_t>(msix_cap + 2)), TABLE, PBA, pci::pci_get_bar_size(device, 1)))
            return false;
        auto* table_bar = static_cast<uint8_t*>(pci::pci_map_bar(device, 1));
        if (table_bar == nullptr) return false;
        vector_control = reinterpret_cast<volatile uint32_t*>(table_bar + (TABLE & ~uint32_t{7}) + 12);
    }
    pci::pci_enable_memory_space(device);
    if ((read16(device, pci::PCI_COMMAND) & pci::PCI_COMMAND_MEM_SPACE) == 0) return false;
    regs = static_cast<volatile uint32_t*>(pci::pci_map_bar(device, 0));
    shared = static_cast<uint8_t*>(pci::pci_map_bar(device, 2));
    shared_size = pci::pci_get_bar_size(device, 2);
    prepared = regs != nullptr && shared != nullptr;
    if (prepared)
        log::info("prepared PCI %02x:%02x.%x mode=%s BAR2=%zu BME=0 guest-DMA-regions=0", device->bus, device->slot, device->function,
                  mode == model::Mode::msix ? "msix" : "plain-poll", shared_size);
    return prepared;
}

auto PciResources::mask_interrupts() -> bool {
    if (device == nullptr) return true;
    bool masked = true;
    if (msix_cap != 0) {
        if (msix_cap < 0x40 || msix_cap > 0xf4) return false;
        auto const OFFSET = static_cast<uint8_t>(msix_cap + 2);
        write16(device, OFFSET, static_cast<uint16_t>((read16(device, OFFSET) | 0x4000U) & ~0x8000U));
        masked = (read16(device, OFFSET) & 0xc000U) == 0x4000U;
    }
    write16(device, pci::PCI_COMMAND, static_cast<uint16_t>(read16(device, pci::PCI_COMMAND) | pci::PCI_COMMAND_INT_DISABLE));
    return (read16(device, pci::PCI_COMMAND) & pci::PCI_COMMAND_INT_DISABLE) != 0 && masked;
}

auto PciResources::stop_interrupts() -> bool {
    bool const MASKED = mask_interrupts();
    bool const BME_OFF = device == nullptr || pci::pci_set_bus_master(device, false);
    bool proved = MASKED;
#ifdef WOS_SELFTEST
    if (state.load(std::memory_order_acquire) == State::retiring && selftest_flag("--ivshmem-test-withhold-stop")) proved = false;
#endif
    if (proved && irq_owned) {
        mod::gates::free_irq(irq_vector);
        irq_owned = false;
    }
    return proved && BME_OFF && !irq_owned;
}

auto PciResources::retire(void (*unpublish)(void*), void* data, bool (*other_drained)(void*)) -> int {
    if (mod::sched::can_query_current_task() && !mod::mm::phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    auto const HOLD = mod::sched::hold_kernel_shutdown();
    auto expected = State::active;
    if (!state.compare_exchange_strong(expected, State::retiring, std::memory_order_acq_rel))
        return expected == State::stopped ? 0 : expected == State::retiring ? -EBUSY : -EIO;
    struct Backend {
        PciResources& owner;
        void (*remove)(void*);
        void* data;
        bool (*other_drained)(void*);
        auto drained() -> bool { return owner.callbacks.drained() && (other_drained == nullptr || other_drained(data)); }
        void close_callbacks() { owner.callbacks.close(); }
        auto drain_callbacks() -> bool {
            auto const START = mod::time::get_ms();
            for (size_t n = 0; n < 10'000'000 && mod::time::get_ms() - START < 5000; ++n) {
                if (drained()) return true;
                if (mod::sched::can_query_current_task())
                    mod::sched::kern_yield();
                else
                    asm volatile("pause" ::: "memory");
            }
            return drained();
        }
        void inhibit_hardware() {
            // Existing callbacks can touch vector-control/doorbell MMIO, but
            // cannot undo this function mask or re-enable bus mastering.
            (void)owner.mask_interrupts();
            if (owner.device != nullptr) (void)pci::pci_set_bus_master(owner.device, false);
        }
        void unpublish() {
            if (remove != nullptr) remove(data);
        }
        auto stop_interrupts() -> bool { return owner.stop_interrupts(); }
    } backend{*this, unpublish, data, other_drained};
    auto const RESULT = model::retire(backend);
    state.store(RESULT == model::Retirement::stopped ? State::stopped : State::retained, std::memory_order_release);
    if (device != nullptr)
        log::info("retirement result=%u drained=%u BME=%u irq_owned=%u BAR-retained=1", static_cast<unsigned>(RESULT), backend.drained(),
                  (read16(device, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) != 0, irq_owned);
    return RESULT == model::Retirement::stopped ? 0 : -EIO;
}

auto PciResources::activate(mod::gates::irq_handler_fn handler, void* data, const char* name) -> bool {
    if (callbacks.closed() || !prepared || activation_attempted || irq_owned) return false;
    activation_attempted = true;
    struct Backend {
        PciResources& owner;
        mod::gates::irq_handler_fn handler;
        void* data;
        const char* name;
        auto disable_bus_master() -> bool { return pci::pci_set_bus_master(owner.device, false); }
        auto register_irq() -> bool {
            owner.irq_vector = mod::gates::allocate_vector();
            if (owner.irq_vector == 0) return false;
            if (mod::gates::request_irq(owner.irq_vector, handler, data, name) != 0) {
                mod::gates::free_irq(owner.irq_vector);
                return false;
            }
            owner.irq_owned = true;
            return true;
        }
        auto enable_msix() -> bool {
            return pci::pci_enable_msix(owner.device, owner.irq_vector) == 0 &&
                   (read16(owner.device, static_cast<uint8_t>(owner.msix_cap + 2)) & 0xc000U) == 0x8000U;
        }
        auto enable_bus_master() -> bool { return pci::pci_set_bus_master(owner.device, true); }
        void unwind_irq() { (void)owner.stop_interrupts(); }
    } backend{*this, handler, data, name};
    activated = model::activate(mode, backend);
    if (activated)
        log::info("active mode=%s irq_owned=%u vector=%u BME=%u guest-DMA-regions=0", mode == model::Mode::msix ? "msix" : "plain-poll",
                  irq_owned, irq_vector, (read16(device, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) != 0);
    return activated;
}

void PciResources::ring(uint16_t peer, uint16_t vector) const {
    if (mode == model::Mode::msix && activated && irq_owned && regs != nullptr) regs[3] = model::doorbell(peer, vector);
}
void PciResources::mask_vector(bool masked) const {
    if (vector_control != nullptr) *vector_control = masked ? *vector_control | 1U : *vector_control & ~uint32_t{1};
}
}  // namespace ker::dev::ivshmem
