#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/mm/cpu_maintenance_controller.hpp>

#include "init_registry.hpp"

namespace ker::init {

// =============================================================================
// RUNTIME EXECUTOR
// =============================================================================
// Executes modules in phase order, with dependencies enforced by array ordering within each phase.
// The phase arrays in init_registry.hpp define the execution order.
// Compile-time validation is performed in init_validation.cpp via static_assert.

struct InitExecutor {
   private:
    friend class ker::mod::mm::cpu_maintenance::detail::Controller;
    inline static std::atomic<bool> started_{};
    uint64_t lineage_{};
    BootPhase phase_{BootPhase::PHASE_0_EARLY_BOOT};
    bool live_{true};
    bool scheduler_setup_complete_{};

    InitExecutor() : lineage_(started_.exchange(true, std::memory_order_acq_rel) ? 0 : 1) {
        if (lineage_ == 0) __builtin_trap();
    }
    InitExecutor(const InitExecutor&) = delete;
    InitExecutor(InitExecutor&&) = delete;
    auto operator=(const InitExecutor&) -> InitExecutor& = delete;
    auto operator=(InitExecutor&&) -> InitExecutor& = delete;

    template <size_t N>
    void run_modules(const std::array<ModuleDesc, N>& modules) {
        for (const auto& mod : modules) {
            if (mod.controller_init_fn != nullptr) __builtin_trap();
            if (mod.init_fn != nullptr) {
                mod.init_fn();
            }
        }
    }

    template <size_t N>
    void run_modules(const std::array<ModuleDesc, N>& modules, const ker::mod::mm::cpu_maintenance::detail::Controller& controller) {
        for (const auto& mod : modules) {
            if (mod.controller_init_fn != nullptr) {
                if (mod.init_fn != nullptr || mod.phase != BootPhase::PHASE_5_DRIVERS) __builtin_trap();
                mod.controller_init_fn(controller);
            } else if (mod.init_fn != nullptr) {
                mod.init_fn();
            }
        }
    }

    // Execute all modules in a specific phase
    void run_phase(BootPhase phase) {
        phase_ = phase;
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
                scheduler_setup_complete_ = true;
                break;
            case BootPhase::PHASE_5_DRIVERS: {
                const ker::mod::mm::cpu_maintenance::detail::Controller controller(*this);
                run_modules(PHASE_5_MODULES, controller);
                break;
            }
            case BootPhase::PHASE_6_POST_SCHEDULER:
                run_modules(PHASE_6_MODULES);
                break;
            case BootPhase::PHASE_7_KERNEL_START:
                run_modules(PHASE_7_MODULES);
                break;
        }
    }

    // Execute all phases up to and including the specified phase
    void run_up_to_phase(BootPhase max_phase) {
        for (int p = 0; p <= static_cast<int>(max_phase); ++p) {
            run_phase(static_cast<BootPhase>(p));
        }
    }

   public:
    // Only this real one-shot executor can enter the controlled phase5 path.
    [[noreturn]] static void run_all() {
        InitExecutor executor;
        executor.run_up_to_phase(BootPhase::PHASE_7_KERNEL_START);
        executor.live_ = false;
        // PHASE_7 contains kernel_start which calls sched::startScheduler() and never returns
        // If we somehow get here, halt
        while (true) {
            asm volatile("hlt");
        }
    }
};

}  // namespace ker::init
