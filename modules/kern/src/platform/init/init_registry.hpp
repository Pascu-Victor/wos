#pragma once

#include <array>

#include "init_module.hpp"

namespace ker::init {

// Forward declarations for all init wrapper functions
namespace fns {
// PHASE 0: Early boot
void fb_init();
void serial_init();
void dbg_init();
void mm_init();
void fsgsbase_init();
void gdt_init();

// PHASE 1: Post-MM
void kmalloc_init();

// PHASE 2: Post-Interrupt (flattened from interrupt::init)
void pic_remap();
void acpi_init();
void apic_init();
void apic_mp_init();
void time_init();
void idt_init();
void sys_init();
void ioapic_init();

// PHASE 3: Scheduler Setup
void smt_init();
void epoch_manager_init();
void sched_init();

// PHASE 4: Subsystems
void dev_init();
void pci_enumerate();
void console_init();
void ahci_init();
void block_device_init();
void vfs_init();
void devfs_populate_partitions();
void net_init();

// PHASE 5: Drivers
void virtio_net_init();
void e1000e_init();
void cdc_ether_init();
void xhci_init();
void ivshmem_init();
void pkt_pool_expand();
void ndp_init();
void wki_init();
void devfs_populate_net();
void initramfs_init();

// PHASE 6: Post-Scheduler (flattened from sched::init)
void wki_eth_transport_init();
void wki_ivshmem_transport_init();
void ipv6_linklocal_init();
void sse_init();

// PHASE 7: Kernel Start
void kernel_start();
}  // namespace fns

// Runtime module registry - organized by phase for simple iteration
// The init order within each phase is determined by the array order
// Dependencies are documented in comments and enforced by phase structure

// PHASE 0: Early boot (no heap, no interrupts)
inline constexpr std::array<ModuleDesc, 7> PHASE_0_MODULES = {{
    {.name = "fb", .phase = BootPhase::PHASE_0_EARLY_BOOT, .init_fn = fns::fb_init},
    {.name = "serial", .phase = BootPhase::PHASE_0_EARLY_BOOT, .init_fn = fns::serial_init},
    {.name = "dbg", .phase = BootPhase::PHASE_0_EARLY_BOOT, .init_fn = fns::dbg_init},            // depends: serial
    {.name = "mm", .phase = BootPhase::PHASE_0_EARLY_BOOT, .init_fn = fns::mm_init},              // depends: dbg
    {.name = "fsgsbase", .phase = BootPhase::PHASE_0_EARLY_BOOT, .init_fn = fns::fsgsbase_init},  // depends: stack_capture
    {.name = "gdt", .phase = BootPhase::PHASE_0_EARLY_BOOT, .init_fn = fns::gdt_init},            // depends: fsgsbase
}};

// PHASE 1: Post-MM (kmalloc available)
inline constexpr std::array<ModuleDesc, 1> PHASE_1_MODULES = {{
    {.name = "kmalloc", .phase = BootPhase::PHASE_1_POST_MM, .init_fn = fns::kmalloc_init},  // depends: gdt
}};

// PHASE 2: Post-Interrupt (flattened from interrupt::init)
inline constexpr std::array<ModuleDesc, 8> PHASE_2_MODULES = {{
    {.name = "pic", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::pic_remap},         // depends: kmalloc
    {.name = "acpi", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::acpi_init},        // depends: pic
    {.name = "apic", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::apic_init},        // depends: acpi
    {.name = "apic_mp", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::apic_mp_init},  // depends: apic
    {.name = "time", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::time_init},        // depends: apic_mp
    {.name = "idt", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::idt_init},          // depends: time
    {.name = "sys", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::sys_init},          // depends: idt
    {.name = "ioapic", .phase = BootPhase::PHASE_2_POST_INTERRUPT, .init_fn = fns::ioapic_init},    // depends: idt
}};

// PHASE 3: Subsystems
inline constexpr std::array<ModuleDesc, 8> PHASE_3_MODULES = {{
    {.name = "dev", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::dev_init},                    // depends: ioapic
    {.name = "pci", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::pci_enumerate},               // depends: dev
    {.name = "console", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::console_init},            // depends: pci
    {.name = "ahci", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::ahci_init},                  // depends: pci
    {.name = "block_device", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::block_device_init},  // depends: ahci
    {.name = "vfs", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::vfs_init},                    // depends: block_device
    {.name = "devfs_partitions", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::devfs_populate_partitions},  // depends: vfs
    {.name = "net", .phase = BootPhase::PHASE_3_SUBSYSTEMS, .init_fn = fns::net_init},                                // depends: kmalloc
}};

// PHASE 4: Scheduler Setup
inline constexpr std::array<ModuleDesc, 4> PHASE_4_MODULES = {{
    {.name = "smt", .phase = BootPhase::PHASE_4_SCHEDULER_SETUP, .init_fn = fns::smt_init},                      // depends: gates, irqs
    {.name = "epoch_manager", .phase = BootPhase::PHASE_4_SCHEDULER_SETUP, .init_fn = fns::epoch_manager_init},  // depends: smt
    {.name = "initramfs", .phase = BootPhase::PHASE_4_SCHEDULER_SETUP, .init_fn = fns::initramfs_init},          // depends: vfs
    {.name = "sched", .phase = BootPhase::PHASE_4_SCHEDULER_SETUP, .init_fn = fns::sched_init},                  // depends: smt
}};

// PHASE 5: Drivers
inline constexpr std::array<ModuleDesc, 10> PHASE_5_MODULES = {{
    {.name = "virtio_net", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::virtio_net_init},       // depends: pci, net, smt
    {.name = "e1000e", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::e1000e_init},               // depends: pci, net, smt
    {.name = "cdc_ether", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::cdc_ether_init},         // depends: pci, net, smt
    {.name = "xhci", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::xhci_init},                   // depends: pci, cdc_ether, smt
    {.name = "ivshmem", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::ivshmem_init},             // depends: pci, net, smt
    {.name = "pkt_pool_expand", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::pkt_pool_expand},  // depends: virtio, e1000e, ivshmem
    {.name = "ndp", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::ndp_init},                     // depends: net
    {.name = "wki", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::wki_init},                     // depends: ndp
    {.name = "devfs_net", .phase = BootPhase::PHASE_5_DRIVERS, .init_fn = fns::devfs_populate_net},     // depends: vfs, virtio, e1000e
}};

// PHASE 6: Post-Scheduler (EpochManager required for packet transmission)
// This is the key phase that MUST come after all drivers!
// WKI transport and IPv6 linklocal send packets, which requires EpochManager
// smt and epoch_manager are now initialized in PHASE_3, so sched can use them here
inline constexpr std::array<ModuleDesc, 4> PHASE_6_MODULES = {{
    {.name = "wki_eth_transport",
     .phase = BootPhase::PHASE_6_POST_SCHEDULER,
     .init_fn = fns::wki_eth_transport_init},  // depends: sched, wki
    {.name = "wki_ivshmem_transport",
     .phase = BootPhase::PHASE_6_POST_SCHEDULER,
     .init_fn = fns::wki_ivshmem_transport_init},                                                                 // depends: sched, wki
    {.name = "ipv6_linklocal", .phase = BootPhase::PHASE_6_POST_SCHEDULER, .init_fn = fns::ipv6_linklocal_init},  // depends: sched, net
    {.name = "sse", .phase = BootPhase::PHASE_6_POST_SCHEDULER, .init_fn = fns::sse_init},                        // depends: sched
}};

// PHASE 7: Kernel Start (never returns)
// This phase hands off control to the scheduler
inline constexpr std::array<ModuleDesc, 1> PHASE_7_MODULES = {{
    {.name = "kernel_start", .phase = BootPhase::PHASE_7_KERNEL_START, .init_fn = fns::kernel_start},  // depends: sse, initramfs
}};

}  // namespace ker::init
