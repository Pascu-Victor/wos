#include "epoch.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/dbg/dbg.hpp"

namespace ker::mod::sched {

// Static member definitions
std::atomic<uint64_t> EpochManager::global_epoch{0};
CpuEpoch* EpochManager::cpu_epochs = nullptr;

void EpochManager::init() {
    uint64_t const CORE_COUNT = smt::get_core_count();

    // Manually allocate aligned memory for CpuEpoch array.
    // Each CpuEpoch needs 64-byte alignment to avoid false sharing.
    // Allocate with extra space to ensure we can align properly.
    size_t const ALLOC_SIZE = (CORE_COUNT + 1) * sizeof(CpuEpoch);
    auto* raw_ptr = new uint8_t[ALLOC_SIZE];

    // Align to 64 bytes
    auto const ADDR = reinterpret_cast<uintptr_t>(raw_ptr);
    uintptr_t const ALIGNED = (ADDR + 63) & ~63ULL;
    cpu_epochs = reinterpret_cast<CpuEpoch*>(ALIGNED);

    // Placement-new to construct each CpuEpoch
    for (uint64_t i = 0; i < CORE_COUNT; ++i) {
        new (&cpu_epochs[i]) CpuEpoch();
        cpu_epochs[i].local_epoch.store(0, std::memory_order_relaxed);
        cpu_epochs[i].in_critical_section.store(false, std::memory_order_relaxed);
    }

    // Ensure all initialization is visible
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

auto EpochManager::enter_critical() -> uint64_t {
    uint64_t const CPU_ID = cpu::current_cpu();
    enter_critical_for_cpu(CPU_ID);
    return CPU_ID;
}

void EpochManager::enter_critical_for_cpu(uint64_t cpu_id) {
    if (cpu_epochs == nullptr || cpu_id >= smt::get_core_count()) {
        return;
    }
    // Mark that we're entering a critical section
    cpu_epochs[cpu_id].in_critical_section.store(true, std::memory_order_relaxed);

    // Read the current global epoch and store it as our local epoch.
    // This must happen AFTER setting inCriticalSection to ensure proper ordering.
    uint64_t const EPOCH = global_epoch.load(std::memory_order_acquire);
    cpu_epochs[cpu_id].local_epoch.store(EPOCH, std::memory_order_release);

    // Full barrier to ensure all the above is visible before we access any task pointers
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

void EpochManager::enter_critical_apic() {
    uint64_t const CPU_ID = smt::get_cpu_index_from_apic_id(apic::get_apic_id());

    if (cpu_epochs == nullptr || CPU_ID >= smt::get_core_count()) {
        dbg::log("EpochManager::enter_criticalAPIC: cpu_epochs not initialized! CRITICAL FAILURE.");
        return;
    }
    enter_critical_for_cpu(CPU_ID);
}

void EpochManager::exit_critical() {
    uint64_t const CPU_ID = cpu::current_cpu();
    exit_critical_for_cpu(CPU_ID);
}

void EpochManager::exit_critical_for_cpu(uint64_t cpu_id) {
    if (cpu_epochs == nullptr || cpu_id >= smt::get_core_count()) {
        return;
    }
    // Full barrier to ensure all task pointer accesses complete before we exit
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

    // Mark that we're no longer in a critical section
    cpu_epochs[cpu_id].in_critical_section.store(false, std::memory_order_release);
}

auto EpochManager::current_epoch() -> uint64_t { return global_epoch.load(std::memory_order_acquire); }

void EpochManager::advance_epoch() { global_epoch.fetch_add(1, std::memory_order_acq_rel); }

auto EpochManager::is_safe_to_reclaim(uint64_t death_epoch) -> bool {
    uint64_t const CURRENT = global_epoch.load(std::memory_order_acquire);

    // Must be at least EPOCH_THRESHOLD epochs old
    if (CURRENT - death_epoch < EPOCH_THRESHOLD) {
        return false;
    }

    // Check that all CPUs have moved past the death epoch.
    // A CPU that's in a critical section with a localEpoch <= deathEpoch
    // might still be holding references to memory freed at deathEpoch.
    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t i = 0; i < CORE_COUNT; ++i) {
        if (cpu_epochs[i].in_critical_section.load(std::memory_order_acquire)) {
            uint64_t const CPU_EPOCH = cpu_epochs[i].local_epoch.load(std::memory_order_acquire);
            if (CPU_EPOCH <= death_epoch) {
                // This CPU might still be referencing memory from this epoch
                return false;
            }
        }
    }

    return true;
}

}  // namespace ker::mod::sched
