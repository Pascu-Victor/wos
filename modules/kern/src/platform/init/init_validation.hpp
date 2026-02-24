#pragma once

#include <array>

#include "init_module.hpp"
#include "init_topo_sort.hpp"

namespace ker::init::validation {

// Validate all hard dependencies exist in the registry
template <size_t N>
constexpr bool validate_hard_deps_exist(const std::array<ModuleMeta, N>& registry) {
    for (size_t i = 0; i < N; ++i) {
        const auto& mod = registry[i];
        for (size_t d = 0; d < mod.dep_count; ++d) {
            const auto& dep = mod.deps[d];
            if (dep.type != DepType::HARD) {
                continue;
            }

            int dep_idx = find_module_index(registry, dep.target);
            if (dep_idx < 0) {
                // Hard dependency not found in registry
                return false;
            }
        }
    }
    return true;
}

// Validate no module depends on a module in a later boot phase
// (A PHASE_2 module cannot depend on a PHASE_4 module)
template <size_t N>
constexpr bool validate_phase_ordering(const std::array<ModuleMeta, N>& registry) {
    for (size_t i = 0; i < N; ++i) {
        const auto& mod = registry[i];
        for (size_t d = 0; d < mod.dep_count; ++d) {
            const auto& dep = mod.deps[d];
            if (dep.type != DepType::HARD) {
                continue;
            }

            int dep_idx = find_module_index(registry, dep.target);
            if (dep_idx < 0) {
                continue;  // Will be caught by validate_hard_deps_exist
            }

            // Module's phase must be >= dependency's phase
            if (static_cast<int>(mod.phase) < static_cast<int>(registry[dep_idx].phase)) {
                // Invalid: depending on a module in a later phase
                return false;
            }
        }
    }
    return true;
}

// Validate no duplicate module IDs
template <size_t N>
constexpr bool validate_no_duplicates(const std::array<ModuleMeta, N>& registry) {
    for (size_t i = 0; i < N; ++i) {
        for (size_t j = i + 1; j < N; ++j) {
            if (registry[i].id == registry[j].id) {
                return false;
            }
        }
    }
    return true;
}

// Combined validation - returns true if all checks pass
template <size_t N>
constexpr bool validate_registry(const std::array<ModuleMeta, N>& registry) {
    return validate_no_duplicates(registry) && validate_hard_deps_exist(registry) && validate_phase_ordering(registry) &&
           !detect_cycle(build_adj_matrix(registry));
}

// Constexpr function to run all validations with descriptive static_asserts
template <size_t N>
constexpr bool init_validate_registry(const std::array<ModuleMeta, N>& registry) {
    static_assert(validate_no_duplicates(registry), "Duplicate module ID detected in kernel init registry");
    static_assert(validate_hard_deps_exist(registry), "Missing hard dependency: a required module is not registered");
    static_assert(validate_phase_ordering(registry), "Invalid phase dependency: a module depends on a module in a later boot phase");
    static_assert(!detect_cycle(build_adj_matrix(registry)), "Circular dependency detected in kernel module initialization");
    return true;
}

}  // namespace ker::init::validation
