#include <platform/smt/smt.hpp>
#include <test/ktest.hpp>

namespace smt = ker::mod::smt;

// These invoke the actual out-of-line registry methods through private fixture
// instances. They never consume or reset the live CPU-domain registry.
KTEST(CpuDomainRegistry, OneShotRootAndGroupPublication) { KEXPECT_TRUE(smt::cpu_domain_registry_selftest_one_shot_roots()); }

KTEST(CpuDomainRegistry, LeafFieldsNamesAndStableSnapshots) { KEXPECT_TRUE(smt::cpu_domain_registry_selftest_leaf_snapshots()); }

KTEST(CpuDomainRegistry, InvalidLookupAndCpuMaskBoundaries) { KEXPECT_TRUE(smt::cpu_domain_registry_selftest_lookup_boundaries()); }

KTEST(CpuDomainRegistry, CapacityRefusalPreservesPublishedSlots) { KEXPECT_TRUE(smt::cpu_domain_registry_selftest_capacity_refusal()); }

KTEST(CpuDomainRegistry, UninitializedLookupAndAppendRefuse) { KEXPECT_TRUE(smt::cpu_domain_registry_selftest_uninitialized_refusal()); }
