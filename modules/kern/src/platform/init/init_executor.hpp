#pragma once

#include "init_registry.hpp"

namespace ker::init {

// =============================================================================
// RUNTIME EXECUTOR
// =============================================================================
// Executes modules in phase order, with dependencies enforced by array ordering within each phase.
// The phase arrays in init_registry.hpp define the execution order.
// Compile-time validation is performed in init_validation.cpp via static_assert.

struct InitExecutor {
    // Execute all modules in a specific phase
    static void run_phase(BootPhase phase) {
        switch (phase) {
            case BootPhase::PHASE_0_EARLY_BOOT:
                for (const auto& mod : PHASE_0_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_1_POST_MM:
                for (const auto& mod : PHASE_1_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_2_POST_INTERRUPT:
                for (const auto& mod : PHASE_2_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_3_SUBSYSTEMS:
                for (const auto& mod : PHASE_3_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_4_SCHEDULER_SETUP:
                for (const auto& mod : PHASE_4_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_5_DRIVERS:
                for (const auto& mod : PHASE_5_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_6_POST_SCHEDULER:
                for (const auto& mod : PHASE_6_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
                break;
            case BootPhase::PHASE_7_KERNEL_START:
                for (const auto& mod : PHASE_7_MODULES) {
                    if (mod.init_fn != nullptr) {
                        mod.init_fn();
                    }
                }
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
