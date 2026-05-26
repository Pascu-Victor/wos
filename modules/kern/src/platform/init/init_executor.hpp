#pragma once

#include <array>
#include <cstddef>

#include "init_registry.hpp"

namespace ker::init {

// =============================================================================
// RUNTIME EXECUTOR
// =============================================================================
// Executes modules in phase order, with dependencies enforced by array ordering within each phase.
// The phase arrays in init_registry.hpp define the execution order.
// Compile-time validation is performed in init_validation.cpp via static_assert.

struct InitExecutor {
    template <size_t N>
    static void run_modules(const std::array<ModuleDesc, N>& modules) {
        for (const auto& mod : modules) {
            if (mod.init_fn != nullptr) {
                mod.init_fn();
            }
        }
    }

    // Execute all modules in a specific phase
    static void run_phase(BootPhase phase) {
        switch (phase) {
            case BootPhase::PHASE_0_EARLY_BOOT:
                run_modules(PHASE_0_MODULES);
                break;
            case BootPhase::PHASE_1_POST_MM:
                run_modules(PHASE_1_MODULES);
                break;
            case BootPhase::PHASE_2_POST_INTERRUPT:
                run_modules(PHASE_2_MODULES);
                break;
            case BootPhase::PHASE_3_SUBSYSTEMS:
                run_modules(PHASE_3_MODULES);
                break;
            case BootPhase::PHASE_4_SCHEDULER_SETUP:
                run_modules(PHASE_4_MODULES);
                break;
            case BootPhase::PHASE_5_DRIVERS:
                run_modules(PHASE_5_MODULES);
                break;
            case BootPhase::PHASE_6_POST_SCHEDULER:
                run_modules(PHASE_6_MODULES);
                break;
            case BootPhase::PHASE_7_KERNEL_START:
                run_modules(PHASE_7_MODULES);
                break;
        }
    }

    // Execute all phases up to and including the specified phase
    static void run_up_to_phase(BootPhase max_phase) {
        for (int p = 0; p <= static_cast<int>(max_phase); ++p) {
            run_phase(static_cast<BootPhase>(p));
        }
    }

    // Execute all modules in all phases (including PHASE_7 which never returns)
    [[noreturn]] static void run_all() {
        run_up_to_phase(BootPhase::PHASE_7_KERNEL_START);
        // PHASE_7 contains kernel_start which calls sched::startScheduler() and never returns
        // If we somehow get here, halt
        while (true) {
            asm volatile("hlt");
        }
    }
};

}  // namespace ker::init
