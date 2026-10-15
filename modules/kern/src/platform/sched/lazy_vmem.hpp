#pragma once

#include <platform/sched/task.hpp>

namespace ker::mod::sched::task {

// The caller protects an existing row owner. Retain never allocates or closes;
// release and vector cleanup require ordinary context outside publication/IRQ.
[[nodiscard]] auto try_retain_lazy_vmem_range(const LazyVmemRange& range) noexcept -> bool;
void release_lazy_vmem_range(const LazyVmemRange& range) noexcept;
void release_lazy_vmem_range_refs(LazyVmemRangeVec& ranges) noexcept;
[[nodiscard]] auto lazy_vmem_ranges_equal(const LazyVmemRange& left, const LazyVmemRange& right) noexcept -> bool;

// Prepare outside publication/root/IRQ guards. The owned immutable snapshot
// keeps rows and the source Task alive, never a root-reader lease. Caller must
// destroy/reset it after its publication guard has ended.
class LazyVmemClone {
   public:
    LazyVmemClone() = default;
    ~LazyVmemClone();
    LazyVmemClone(const LazyVmemClone&) = delete;
    auto operator=(const LazyVmemClone&) -> LazyVmemClone& = delete;

    [[nodiscard]] auto prepare(Task& source) -> int;
    // Caller holds SharedVmemPublicationGuard; no allocation or owner release.
    [[nodiscard]] auto matches(Task& source) const -> bool;
    // Transfer already-counted rows into an empty destination, under caller
    // publication. No new reference acquisition or allocation can fail here.
    [[nodiscard]] auto consume(Task& empty_destination) -> bool;
    [[nodiscard]] auto ranges() const noexcept -> const LazyVmemRangeVec& { return rows_; }
    [[nodiscard]] auto root() const noexcept -> mm::paging::PageTable* { return root_; }
    [[nodiscard]] auto root_incarnation() const noexcept -> uint64_t { return root_incarnation_; }
    void reset() noexcept;

   private:
    Task* source_{};
    mm::paging::PageTable* root_{};  // Identity only outside the capture guards.
    uint64_t root_incarnation_{};
    LazyVmemRangeVec rows_{};
    bool prepared_{};
};

}  // namespace ker::mod::sched::task
