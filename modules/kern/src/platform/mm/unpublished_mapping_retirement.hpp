#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/mm/file_mapping_lifetime.hpp>

namespace ker::mod::mm::virt {
struct DestroyUserSpaceBudgetState;
}
namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::mm::file_mapping {

enum class UnpublishedRetirementState : uint8_t { PREPARED, RETIRING, WAITING_RETURN };
enum class UnpublishedRetirementMode : uint8_t { OWNED_SLOT, TRANSFERRED_SLOT, DEFERRED_TRANSFERRED };
enum class UnpublishedRetirementResult : uint8_t { REFUSED, PENDING, COMPLETE };

// A row is stable before exposure. Registry links own REGISTRY; lazy rows own
// LAZY; the real pin-lock index owns INDEX. The pointers here are bookkeeping
// borrows, never additional hidden family/binding/File owners.
struct UnpublishedMappingEntry {
    FileMappingLifetime* lifetime{};
    FileMappingBinding* binding{};
    paging::PageTable* root{};
    uint64_t start{}, end{}, file_offset{};
    UnpublishedMappingEntry* registry_next{};
    bool registered{};
    RetirementCompletion completion{};
};

struct UnpublishedMappingRetirement {
    static constexpr size_t CAPACITY = 8;
    std::array<UnpublishedMappingEntry, CAPACITY> entries{};
    size_t count{};
    virt::DestroyUserSpaceBudgetState* root_walk{};
    paging::PageTable* retired_root{};
    bool root_walk_started{};
    sched::task::Task* controller{};
    UnpublishedRetirementState state{UnpublishedRetirementState::PREPARED};
    UnpublishedRetirementMode mode{UnpublishedRetirementMode::OWNED_SLOT};
    int first_close_result{};
    sched::task::Task* deferred_subject{};  // Exact transferred owner; creator remains separate.
    UnpublishedMappingRetirement* deferred_next{};
    bool deferred_claimed{};              // Private single GC slice, outside its IRQ queue lock.
    sched::task::Task* active_subject{};  // Borrowed from creator plus exact slot/queue owner.
    UnpublishedMappingRetirement* active_next{};
};

}  // namespace ker::mod::mm::file_mapping
