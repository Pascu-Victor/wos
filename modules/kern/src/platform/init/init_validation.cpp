// Compile-time validation of the kernel module registry
// This file triggers static_assert checks during compilation

#include "init_topo_sort.hpp"
#include "init_validation.hpp"
#include "module_meta_registry.hpp"

namespace ker::init {

// =============================================================================
// COMPILE-TIME VALIDATION
// =============================================================================
// These static_asserts validate the module registry at compile time.
// If any check fails, compilation will fail with a descriptive error message.

// Validate no duplicate module IDs
static_assert(validation::validate_no_duplicates(MODULE_META_REGISTRY),
              "Duplicate module ID detected in kernel init registry");

// Validate all hard dependencies exist
static_assert(validation::validate_hard_deps_exist(MODULE_META_REGISTRY),
              "Missing hard dependency: a required module is not registered");

// Validate no cross-phase violations (depending on later phases)
static_assert(validation::validate_phase_ordering(MODULE_META_REGISTRY),
              "Invalid phase dependency: a module depends on a module in a later boot phase");

// Validate no circular dependencies
static_assert(!detect_cycle(build_adj_matrix(MODULE_META_REGISTRY)),
              "Circular dependency detected in kernel module initialization");

// Compute and store the init order at compile time (for debugging/verification)
inline constexpr auto COMPUTED_INIT_ORDER = compute_init_order(MODULE_META_REGISTRY);

}  // namespace ker::init
