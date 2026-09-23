#pragma once

#include <dev/ivshmem/pci_resources.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::dev::ivshmem {
// Cooperative task exit must not abandon the admitted BAR/IRQ owner on a
// callback's stack. Member destruction drops admission before shutdown hold.
class CallbackGuard {
    mod::sched::KernelShutdownHold shutdown_{mod::sched::hold_kernel_shutdown()};
    dma::lifecycle::Guard callback_;

   public:
    explicit CallbackGuard(PciResources& resources) : callback_(resources.callbacks) {}
    [[nodiscard]] explicit operator bool() const { return static_cast<bool>(callback_); }
};
}  // namespace ker::dev::ivshmem
