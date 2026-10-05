#pragma once

#include <abi/resource.hpp>
#include <platform/mm/paging.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
// Count the union of lazy metadata and lower-half PTEs. The subject is the
// current task or an exclusively owned unpublished task. Its metadata is
// serialized by SharedVmemPublicationGuard (or private construction).
[[nodiscard]] auto count_address_space(task::Task& subject, mm::paging::PageTable* root, uint64_t start, uint64_t end, uint64_t& bytes)
    -> bool;
// Caller holds SharedVmemPublicationGuard or owns an unpublished root.
// Policy belongs to subject's process; added/replaced sizes are logical bytes.
[[nodiscard]] auto admit_address_space(task::Task* subject, mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start = 0,
                                       uint64_t replaced_size = 0) -> bool;
// Caller holds the publication guard or owns the unpublished task/root.
[[nodiscard]] auto observe_address_space(task::Task& subject) -> bool;
void copy_address_space_observation(task::Task& subject, abi::resource::Usage& usage);

class AddressSpaceObservation {
   public:
    explicit AddressSpaceObservation(task::Task* subject) : m_subject(subject) {}
    ~AddressSpaceObservation();
    AddressSpaceObservation(const AddressSpaceObservation&) = delete;
    auto operator=(const AddressSpaceObservation&) -> AddressSpaceObservation& = delete;
    // One admission and one complete lazy mapping under the same publication
    // guard. Every refusal, partial mapping, or ineligible root is recounted.
    [[nodiscard]] auto admit_mapping(mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start = 0, uint64_t replaced_size = 0)
        -> bool;
    void commit_mapping();

   private:
    [[nodiscard]] auto observe() -> bool;
    task::Task* m_subject;
    mm::paging::PageTable* m_admitted_root = nullptr;
    uint64_t m_admitted_bytes = 0;
    bool m_admission_attempted = false;
    bool m_committed = false;
};
}  // namespace ker::mod::sched::resource
