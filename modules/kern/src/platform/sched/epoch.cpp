#include "epoch.hpp"

#include <cstdint>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::sched {

// Static member definitions
std::atomic<uint64_t> EpochManager::globalEpoch{0};
CpuEpoch* EpochManager::cpuEpochs = nullptr;

void EpochManager::init() {
    uint64_t coreCount = smt::get_core_count();

    // Manually allocate aligned memory for CpuEpoch array.
    // Each CpuEpoch needs 64-byte alignment to avoid false sharing.
    // Allocate with extra space to ensure we can align properly.
    size_t allocSize = (coreCount + 1) * sizeof(CpuEpoch);
    auto* rawPtr = new uint8_t[allocSize];

    // Align to 64 bytes
    uintptr_t addr = reinterpret_cast<uintptr_t>(rawPtr);
    uintptr_t aligned = (addr + 63) & ~63ULL;
    cpuEpochs = reinterpret_cast<CpuEpoch*>(aligned);

    // Placement-new to construct each CpuEpoch
    for (uint64_t i = 0; i < coreCount; ++i) {
        new (&cpuEpochs[i]) CpuEpoch();
        cpuEpochs[i].localEpoch.store(0, std::memory_order_relaxed);
        cpuEpochs[i].inCriticalSection.store(false, std::memory_order_relaxed);
    }

    // Ensure all initialization is visible
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

auto EpochManager::enterCritical() -> uint64_t {
    uint64_t cpuId = cpu::current_cpu();
    enterCriticalForCpu(cpuId);
    return cpuId;
}

void EpochManager::enterCriticalForCpu(uint64_t cpuId) {
    if (cpuEpochs == nullptr || cpuId >= smt::get_core_count()) {
        return;
    }
    // Mark that we're entering a critical section
    cpuEpochs[cpuId].inCriticalSection.store(true, std::memory_order_relaxed);

    // Read the current global epoch and store it as our local epoch.
    // This must happen AFTER setting inCriticalSection to ensure proper ordering.
    uint64_t epoch = globalEpoch.load(std::memory_order_acquire);
    cpuEpochs[cpuId].localEpoch.store(epoch, std::memory_order_release);

    // Full barrier to ensure all the above is visible before we access any task pointers
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

void EpochManager::enterCriticalAPIC() {
    uint64_t cpuId = smt::get_cpu_index_from_apic_id(apic::get_apic_id());

    if (cpuEpochs == nullptr || cpuId >= smt::get_core_count()) {
        dbg::log("EpochManager::enterCriticalAPIC: cpuEpochs not initialized! CRITICAL FAILURE.");
        return;
    }
    enterCriticalForCpu(cpuId);
}

void EpochManager::exitCritical() {
    uint64_t cpuId = cpu::current_cpu();
    exitCriticalForCpu(cpuId);
}

void EpochManager::exitCriticalForCpu(uint64_t cpuId) {
    if (cpuEpochs == nullptr || cpuId >= smt::get_core_count()) {
        return;
    }
    // Full barrier to ensure all task pointer accesses complete before we exit
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

    // Mark that we're no longer in a critical section
    cpuEpochs[cpuId].inCriticalSection.store(false, std::memory_order_release);
}

auto EpochManager::currentEpoch() -> uint64_t { return globalEpoch.load(std::memory_order_acquire); }

void EpochManager::advanceEpoch() { globalEpoch.fetch_add(1, std::memory_order_acq_rel); }

auto EpochManager::isSafeToReclaim(uint64_t deathEpoch) -> bool {
    uint64_t current = globalEpoch.load(std::memory_order_acquire);

    // Must be at least EPOCH_THRESHOLD epochs old
    if (current - deathEpoch < EPOCH_THRESHOLD) {
        return false;
    }

    // Check that all CPUs have moved past the death epoch.
    // A CPU that's in a critical section with a localEpoch <= deathEpoch
    // might still be holding references to memory freed at deathEpoch.
    uint64_t coreCount = smt::get_core_count();
    for (uint64_t i = 0; i < coreCount; ++i) {
        if (cpuEpochs[i].inCriticalSection.load(std::memory_order_acquire)) {
            uint64_t cpuEpoch = cpuEpochs[i].localEpoch.load(std::memory_order_acquire);
            if (cpuEpoch <= deathEpoch) {
                // This CPU might still be referencing memory from this epoch
                return false;
            }
        }
    }

    return true;
}

}  // namespace ker::mod::sched
