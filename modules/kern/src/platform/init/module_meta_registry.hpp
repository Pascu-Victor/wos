#pragma once

#include <array>

#include "init_module.hpp"

namespace ker::init {

// =============================================================================
// MODULE META REGISTRY - Compile-time metadata for dependency validation
// =============================================================================
// This registry contains ONLY compile-time metadata (no function pointers).
// It is used with static_assert to validate:
//   - No duplicate module IDs
//   - All hard dependencies exist
//   - No cross-phase violations (depending on later phases)
//   - No circular dependencies
//
// The runtime registry (init_registry.hpp) must be kept in sync with this.
// =============================================================================

inline constexpr size_t MODULE_COUNT = 43;

inline constexpr std::array<ModuleMeta, MODULE_COUNT> MODULE_META_REGISTRY = {{
    // =========================================================================
    // PHASE 0: Early boot (no heap, no interrupts)
    // =========================================================================
    make_meta("fb", BootPhase::PHASE_0_EARLY_BOOT),
    make_meta("serial", BootPhase::PHASE_0_EARLY_BOOT),
    make_meta("dbg", BootPhase::PHASE_0_EARLY_BOOT, Dependency{"serial"}),
    make_meta("mm", BootPhase::PHASE_0_EARLY_BOOT, Dependency{"dbg"}),
    make_meta("stack_capture", BootPhase::PHASE_0_EARLY_BOOT, Dependency{"mm"}),
    make_meta("fsgsbase", BootPhase::PHASE_0_EARLY_BOOT, Dependency{"stack_capture"}),
    make_meta("gdt", BootPhase::PHASE_0_EARLY_BOOT, Dependency{"fsgsbase"}),

    // =========================================================================
    // PHASE 1: Post-MM (kmalloc available)
    // =========================================================================
    make_meta("kmalloc", BootPhase::PHASE_1_POST_MM, Dependency{"gdt"}),

    // =========================================================================
    // PHASE 2: Post-Interrupt (flattened from interrupt::init)
    // =========================================================================
    make_meta("pic", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"kmalloc"}),
    make_meta("acpi", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"pic"}),
    make_meta("apic", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"acpi"}),
    make_meta("apic_mp", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"apic"}),
    make_meta("time", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"apic_mp"}),
    make_meta("idt", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"time"}),
    make_meta("sys", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"idt"}),
    make_meta("ioapic", BootPhase::PHASE_2_POST_INTERRUPT, Dependency{"idt"}),

    // =========================================================================
    // PHASE 3: Subsystems
    // =========================================================================
    make_meta("smt", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"sys"}),
    make_meta("epoch_manager", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"smt"}),
    make_meta("dev", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"ioapic"}),
    make_meta("pci", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"dev"}),
    make_meta("console", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"pci"}),
    make_meta("ahci", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"pci"}),
    make_meta("block_device", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"ahci"}, Dependency{"epoch_manager"}),
    make_meta("vfs", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"block_device"}),
    make_meta("devfs_partitions", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"vfs"}),
    make_meta("net", BootPhase::PHASE_3_SUBSYSTEMS, Dependency{"kmalloc"}),

    // =========================================================================
    // PHASE 4: Scheduler Setup (sched depends on smt + epoch_manager from PHASE_3)
    // =========================================================================
    make_meta("sched", BootPhase::PHASE_4_SCHEDULER_SETUP, Dependency{"epoch_manager"}, Dependency("smt"), Dependency("ioapic")),
    make_meta("initramfs", BootPhase::PHASE_4_SCHEDULER_SETUP, Dependency{"vfs"}),

    // =========================================================================
    // PHASE 5: Drivers (now have smt and epoch_manager for worker threads)
    // =========================================================================
    make_meta("virtio_net", BootPhase::PHASE_5_DRIVERS, Dependency{"pci"}, Dependency{"net"}, Dependency{"sched"}),
    make_meta("e1000e", BootPhase::PHASE_5_DRIVERS, Dependency{"pci"}, Dependency{"net"}, Dependency{"sched"}),
    make_meta("cdc_ether", BootPhase::PHASE_5_DRIVERS, Dependency{"pci"}, Dependency{"net"}, Dependency{"sched"}),
    make_meta("xhci", BootPhase::PHASE_5_DRIVERS, Dependency{"pci"}, Dependency{"cdc_ether"}, Dependency{"sched"}),
    make_meta("ivshmem", BootPhase::PHASE_5_DRIVERS, Dependency{"pci"}, Dependency{"net"}, Dependency{"sched"}),
    make_meta("pkt_pool_expand", BootPhase::PHASE_5_DRIVERS, Dependency{"virtio_net"}, Dependency{"e1000e"}, Dependency{"ivshmem"}),
    make_meta("ndp", BootPhase::PHASE_5_DRIVERS, Dependency{"net"}),
    make_meta("wki", BootPhase::PHASE_5_DRIVERS, Dependency{"ndp"}),
    make_meta("devfs_net", BootPhase::PHASE_5_DRIVERS, Dependency{"vfs"}, Dependency{"virtio_net"}, Dependency{"e1000e"}),

    // =========================================================================
    // PHASE 6: Post-Scheduler (EpochManager required for packet transmission)
    // WKI transport and IPv6 linklocal send packets, which requires EpochManager
    // =========================================================================
    make_meta("wki_eth_transport", BootPhase::PHASE_6_POST_SCHEDULER, Dependency{"sched"}, Dependency{"wki"}),
    make_meta("wki_ivshmem_transport", BootPhase::PHASE_6_POST_SCHEDULER, Dependency{"sched"}, Dependency{"wki"}),
    make_meta("ipv6_linklocal", BootPhase::PHASE_6_POST_SCHEDULER, Dependency{"sched"}, Dependency{"net"}),
    make_meta("sse", BootPhase::PHASE_6_POST_SCHEDULER, Dependency{"sched"}),

    // =========================================================================
    // PHASE 7: Kernel Start (never returns)
    // This phase contains only the final scheduler handoff
    // =========================================================================
    make_meta("kernel_start", BootPhase::PHASE_7_KERNEL_START, Dependency{"sse"}, Dependency{"initramfs"}),
}};

}  // namespace ker::init
