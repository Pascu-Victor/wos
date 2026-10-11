#pragma once

#include <abi/service_resource.hpp>
#include <platform/sched/resource_model.hpp>
#include <platform/security/compartment_policy.hpp>
#include <platform/security/credential_store.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {

// Observations of an exact retained canonical creator, not namespace authority
// or a userspace grant. Creation owns the Task lifetime and this value across IO.
struct NativeCreatorPolicy {
    security::CredentialStore::Observation observed{};
    security::compartment::Profile parent{};
    security::compartment::Profile admitted{};
    uint64_t service_slot{abi::service_resource::NO_SELECTION};
    uint32_t creation_mask{0777};
};

// Ordinary ACTIVE canonical processes only. Initial native creation requires
// Host views and local service accounting. Output is unchanged on failure.
// No allocation/blocking; caller holds the source lifetime, with no locks held.
auto capture_native_creator_policy(task::Task& source, const security::compartment::Request& request, NativeCreatorPolicy& out) -> int;
auto revalidate_native_creator_policy(task::Task& source, const NativeCreatorPolicy& policy) -> int;

// Before image mapping; preserve aggregate affiliation without overwriting the
// already admitted per-process limits. Child is exclusively owned/unpublished.
auto attach_native_creation_service(task::Task& child, task::Task& source, const NativeCreatorPolicy& policy) -> int;

// Internal service-lock-held seam: credentials -> resource, never the reverse.
// No caller-supplied credential input; service_limits is an immutable locked
// selection observation, applied only for original init's selected child policy.
auto capture_native_creator_fields(task::Task& source, const security::compartment::Request& request, const State* service_limits,
                                   NativeCreatorPolicy& out) -> int;

}  // namespace ker::mod::sched::resource
