#pragma once

#include <atomic>
#include <cstdint>

namespace ker::mod::sched {

// Per-CPU epoch state for lock-free epoch-based reclamation.
// Each CPU tracks whether it's in a critical section (holding task pointers)
// and what epoch it observed when entering the critical section.
struct CpuEpoch {
    std::atomic<uint64_t> localEpoch{0};
    std::atomic<bool> inCriticalSection{false};
    char padding[48];  // Pad to 64 bytes to avoid false sharing
} __attribute__((aligned(64)));

// Epoch-based memory reclamation manager.
// Provides lock-free read-side critical sections with deferred freeing.
// Objects can only be freed once all CPUs have passed through a quiescent point.
class EpochManager {
   public:
    // Number of epochs that must pass before memory can be reclaimed.
    // Higher values are safer but delay memory reclamation.
    // Reduced from 2 to 1 to speed up GC under memory pressure.
    static constexpr uint64_t EPOCH_THRESHOLD = 1;

    // Initialize the epoch system. Must be called once at scheduler init.
    static void init();

    // Enter a read-side critical section.
    // While in a critical section, the CPU promises not to access freed memory.
    // Call this before accessing task pointers outside of locks.
    static void enterCritical();

    // Enter a read-side critical section using APIC ID.
    // Used in contexts where CPU ID cannot be obtained normally.
    static void enterCriticalAPIC();

    // Exit a read-side critical section.
    // After this, the CPU no longer holds references to task pointers.
    static void exitCritical();

    // Get the current global epoch value.
    static auto currentEpoch() -> uint64_t;

    // Advance the global epoch. Called periodically (e.g., every N timer ticks).
    // This should only be called from one CPU (typically the BSP).
    static void advanceEpoch();

    // Check if it's safe to reclaim memory that was freed at the given epoch.
    // Returns true if all CPUs have advanced past the death epoch.
    static auto isSafeToReclaim(uint64_t deathEpoch) -> bool;

   private:
    static std::atomic<uint64_t> globalEpoch;
    static CpuEpoch* cpuEpochs;  // Per-CPU epoch state array
};

// RAII guard for epoch critical sections.
// Use this in scheduler functions that access task pointers.
class EpochGuard {
   public:
    EpochGuard() { EpochManager::enterCritical(); }
    ~EpochGuard() { EpochManager::exitCritical(); }

    // Non-copyable, non-movable
    EpochGuard(const EpochGuard&) = delete;
    EpochGuard(EpochGuard&&) = delete;
    auto operator=(const EpochGuard&) -> EpochGuard& = delete;
    auto operator=(EpochGuard&&) -> EpochGuard& = delete;
};

}  // namespace ker::mod::sched
