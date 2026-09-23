#pragma once

#include <atomic>
#include <dev/dma/lifecycle.hpp>
#include <dev/ivshmem/pci_model.hpp>
#include <dev/pci.hpp>
#include <platform/interrupt/gates.hpp>

namespace ker::dev::ivshmem {

#ifdef WOS_SELFTEST
auto pci_selftest_reserved() -> bool;
auto selftest_flag(const char* token) -> bool;
#endif

// CPU BAR ownership, not a guest-RAM DMA allocation. No destructor unmaps BARs
// or frees an IRQ: raw shared-memory pointers and peer access have no revocation
// contract. Failed probes retain this object and cannot silently retry it.
struct PciResources {
    enum class State : uint8_t { active, retiring, stopped, retained };
    dma::lifecycle::Admission callbacks{};
    std::atomic<State> state{State::active};
#ifdef WOS_SELFTEST
    std::atomic<uint64_t> test_irqs{0};
#endif
    pci::PCIDevice* device = nullptr;
    volatile uint32_t* regs = nullptr;
    uint8_t* shared = nullptr;
    volatile uint32_t* vector_control = nullptr;
    size_t shared_size = 0;
    model::Mode mode = model::Mode::unsupported;
    uint8_t msix_cap = 0, irq_vector = 0;
    bool prepared = false, irq_owned = false, activated = false, activation_attempted = false;

    PciResources() = default;
    PciResources(const PciResources&) = delete;
    auto operator=(const PciResources&) -> PciResources& = delete;
    auto prepare(pci::PCIDevice* pci_device, size_t minimum_shared) -> bool;
    auto activate(mod::gates::irq_handler_fn handler, void* data, const char* name) -> bool;
    auto mask_interrupts() -> bool;
    // Probe unwind, or terminal callers AFTER closing/draining class callbacks.
    // Mask and drain IRQ ownership before considering private data reclaimable.
    auto stop_interrupts() -> bool;
    // Task/boot context, no driver or class locks held. Unpublication runs
    // only after callbacks drain. BARs and this owner stay pinned on all paths.
    auto retire(void (*unpublish)(void*), void* data, bool (*other_drained)(void*) = nullptr) -> int;
    // Only vector-control MMIO; allocation-free and safe in IRQ/NAPI.
    void mask_vector(bool masked) const;
    void ring(uint16_t peer, uint16_t vector = 0) const;
};
}  // namespace ker::dev::ivshmem
