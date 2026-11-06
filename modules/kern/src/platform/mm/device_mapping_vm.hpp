#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/device_mapping_object.hpp>
#include <memory>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/managed_huge.hpp>
#include <platform/mm/paging.hpp>
#include <span>

namespace ker::mod::mm::virt {
class BindingRootMutationLease;
}

namespace ker::mod::mm::device_vm {

class PreparedMap;
class PreparedChange;
class PreparedRevoke;
class PreparedFork;

// Admission limits, not truncation. Callers may supply a smaller policy budget.
// Arrays and all physical loans are prepared before publication/root guards.
struct Limits {
    uint64_t max_pages = 262144;
    size_t max_bindings = 4096;
};

namespace detail {
class BindingAccess {
   public:
    // Immutable borrowed metadata only. The caller owns this sealed snapshot;
    // its root field is an identity and never authorizes a root dereference.
    [[nodiscard]] static auto get(const file_mapping::FileBindingSnapshot&) noexcept -> const file_mapping::FileMappingBinding*;
};
struct VmAccess {
   private:
    friend class ::ker::mod::mm::device_vm::PreparedMap;
    friend class ::ker::mod::mm::device_vm::PreparedChange;
    friend class ::ker::mod::mm::device_vm::PreparedRevoke;
    friend class ::ker::mod::mm::device_vm::PreparedFork;
    static auto publish(PreparedMap&, sched::task::Task&, const sys::SyscallPreparationScope&,
                        const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
    static auto change(PreparedChange&, sched::task::Task&, const sys::SyscallPreparationScope&,
                       const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
    static auto revoke(PreparedRevoke&, virt::BindingRootMutationLease&, const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept
        -> int;
    static auto match_fork(PreparedFork&, sched::task::Task&, const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> bool;
    static auto attach_fork(PreparedFork&, sched::task::Task&, sched::task::Task&, paging::PageTable*,
                            const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
};
[[nodiscard]] auto count_current_bindings(sched::task::Task&, const ker::syscall::vmem::SharedVmemPublicationGuard&, size_t&,
                                          uint64_t&) noexcept -> int;
[[nodiscard]] auto capture_current_bindings(sched::task::Task&, const ker::syscall::vmem::SharedVmemPublicationGuard&,
                                            std::span<file_mapping::FileBindingSnapshot>, size_t&, uint64_t&) noexcept -> int;
}  // namespace detail

[[nodiscard]] auto current_device_overlap(sched::task::Task&, uint64_t start, uint64_t end,
                                          const ker::syscall::vmem::SharedVmemPublicationGuard&, bool& has_device) noexcept -> int;

class PreparedMap {
   public:
    PreparedMap() noexcept;
    ~PreparedMap();
    PreparedMap(const PreparedMap&) = delete;
    auto operator=(const PreparedMap&) -> PreparedMap& = delete;
    PreparedMap(PreparedMap&&) = delete;
    auto operator=(PreparedMap&&) -> PreparedMap& = delete;
    [[nodiscard]] auto prepare(file_mapping::FileMappingLifetime&, uint64_t start, uint64_t object_offset, uint64_t length,
                               uint64_t protections, Limits = {}) -> int;
    // Caller holds final VFS/AS/managed-layout admission under this actual PUB.
    // VM independently proves current root, cold index/PTEs and table geometry.
    [[nodiscard]] auto publish_current(sched::task::Task&, const sys::SyscallPreparationScope&,
                                       const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
    void reset() noexcept;

   private:
    friend struct detail::VmAccess;
    struct Storage {
        std::unique_ptr<ker::dev::mapping::PageLoan[]> loans;
        std::unique_ptr<paging::PageTable*[]> tables;
        std::unique_ptr<uint64_t[]> table_physical;
        file_mapping::FileMappingBinding* binding{};
        uint64_t start{}, end{}, offset{}, protections{};
        size_t pages{}, table_count{};
        bool consumed{};
    };
    std::unique_ptr<Storage> storage_;
};

enum class Change : uint8_t { Remove, Protect };
class PreparedChange {
   public:
    PreparedChange() noexcept;
    ~PreparedChange();
    PreparedChange(const PreparedChange&) = delete;
    auto operator=(const PreparedChange&) -> PreparedChange& = delete;
    PreparedChange(PreparedChange&&) = delete;
    auto operator=(PreparedChange&&) -> PreparedChange& = delete;
    [[nodiscard]] auto prepare_current(sched::task::Task&, uint64_t start, uint64_t end, Change, uint64_t protections = 0, Limits = {})
        -> int;
    [[nodiscard]] auto commit_current(sched::task::Task&, const sys::SyscallPreparationScope&,
                                      const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
    // Also releases successful old PTE/TLB owners. Always outside PUB/IRQ/root.
    void reset() noexcept;

   private:
    friend struct detail::VmAccess;
    struct Fragment {
        file_mapping::FileMappingBinding* binding{};
        uint64_t start{}, end{}, offset{};
    };
    struct Storage {
        std::unique_ptr<file_mapping::FileBindingSnapshot[]> old;
        std::unique_ptr<Fragment[]> fragments;
        std::unique_ptr<paging::PageTableEntry[]> removed;
        file_mapping::FileMappingTlbRetirement after_tlb;
        uint64_t start{}, end{}, protections{}, serial{};
        size_t count{}, fragment_count{}, removed_count{}, page_capacity{};
        Change change{Change::Remove};
        bool consumed{};
    };
    std::unique_ptr<Storage> storage_;
};

class PreparedRevoke {
   public:
    PreparedRevoke() noexcept;
    ~PreparedRevoke();
    PreparedRevoke(const PreparedRevoke&) = delete;
    auto operator=(const PreparedRevoke&) -> PreparedRevoke& = delete;
    PreparedRevoke(PreparedRevoke&&) = delete;
    auto operator=(PreparedRevoke&&) -> PreparedRevoke& = delete;
    // Caller keeps this exact sealed wrapper alive through the attempted commit.
    [[nodiscard]] auto prepare(const file_mapping::FileBindingSnapshot&, Limits = {}) -> int;
    [[nodiscard]] auto commit(virt::BindingRootMutationLease&, const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
    void reset() noexcept;

   private:
    friend struct detail::VmAccess;
    struct Storage {
        const file_mapping::FileBindingSnapshot* snapshot{};
        const file_mapping::FileMappingBinding* binding{};
        std::unique_ptr<paging::PageTableEntry[]> removed;
        file_mapping::FileMappingTlbRetirement after_tlb;
        size_t page_capacity{}, removed_count{};
        bool consumed{};
    };
    std::unique_ptr<Storage> storage_;
};

class PreparedFork {
   public:
    PreparedFork() noexcept;
    ~PreparedFork();
    PreparedFork(const PreparedFork&) = delete;
    auto operator=(const PreparedFork&) -> PreparedFork& = delete;
    PreparedFork(PreparedFork&&) = delete;
    auto operator=(PreparedFork&&) -> PreparedFork& = delete;
    [[nodiscard]] auto prepare_current(sched::task::Task&, const sys::SyscallPreparationScope&, Limits = {}) -> int;
    [[nodiscard]] auto matches_current(sched::task::Task&, const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> bool;
    [[nodiscard]] auto empty() const noexcept -> bool { return storage_ == nullptr || storage_->count == 0; }
    void reset() noexcept;

   private:
    friend struct detail::VmAccess;
    // Only the real copier may call this after its original child EMPTY proof
    // and source exclusive adoption. No public raw-child capture factory.
    friend auto huge::detail::copy_current_user_pagemap_exclusive(
        sched::task::Task&, paging::PageTable*, paging::PageTable*, const sys::SyscallPreparationScope&,
        const ker::syscall::vmem::SharedVmemPublicationGuard&, const huge::RangeSplitSurvey&, huge::PreparedTables&,
        numa::AllocationContext*, numa::AllocationResult*, virt::PreparedAnonymousSwapFork*, virt::ForkCopyRefusalWitness*,
        sched::task::Task*, virt::ForkPrivateChildRoot*, virt::ForkSourceOwner*, PreparedFork*) noexcept -> int;
    [[nodiscard]] auto attach_before_copy(sched::task::Task&, sched::task::Task&, paging::PageTable*,
                                          const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> int;
    struct Storage {
        std::unique_ptr<file_mapping::FileBindingSnapshot[]> source;
        std::unique_ptr<file_mapping::FileMappingBinding*[]> children;
        uint64_t serial{};
        size_t count{};
        bool consumed{};
    };
    std::unique_ptr<Storage> storage_;
};

}  // namespace ker::mod::mm::device_vm
