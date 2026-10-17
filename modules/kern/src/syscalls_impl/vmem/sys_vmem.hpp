#pragma once

#include <cstddef>
#include <cstdint>
#include <platform/mm/managed_huge_core.hpp>
#include <platform/mm/paging.hpp>

namespace ker::mod::sched::task {
struct LazyVmemRange;
class LazyVmemClone;
struct Task;
}  // namespace ker::mod::sched::task

namespace ker::mod::sys {
class SyscallPreparationScope;
}

namespace ker::mod::mm::numa {
struct AllocationContext;
}

namespace ker::vfs {
struct File;
}

#ifdef WOS_SELFTEST
namespace ker::test::numa_lifecycle {
struct RegistrySnapshot;
}
#endif

namespace ker::syscall::vmem {
#ifdef WOS_SELFTEST
// Exclusive prefaulted output. Busy/overflow never qualify a prefix.
void selftest_file_registry_snapshot(::ker::test::numa_lifecycle::RegistrySnapshot&) noexcept;
#endif

class SharedVmemPublicationGuard {
   public:
    SharedVmemPublicationGuard();
    ~SharedVmemPublicationGuard();

    SharedVmemPublicationGuard(const SharedVmemPublicationGuard&) = delete;
    SharedVmemPublicationGuard(SharedVmemPublicationGuard&&) = delete;
    auto operator=(const SharedVmemPublicationGuard&) -> SharedVmemPublicationGuard& = delete;
    auto operator=(SharedVmemPublicationGuard&&) -> SharedVmemPublicationGuard& = delete;
    // Borrows this live guard's exact singleton publication identity; no object bytes are read.
    [[nodiscard]] auto acquired() const noexcept -> bool;
};

// Try-only owner for short current-root admission. Construct in an instrumented
// caller before acquiring Task/root lifetimes; failure owns no publication lock.
class SharedVmemPublicationTryGuard {
   public:
    SharedVmemPublicationTryGuard() noexcept;
    ~SharedVmemPublicationTryGuard() noexcept;
    SharedVmemPublicationTryGuard(const SharedVmemPublicationTryGuard&) = delete;
    SharedVmemPublicationTryGuard(SharedVmemPublicationTryGuard&&) = delete;
    auto operator=(const SharedVmemPublicationTryGuard&) -> SharedVmemPublicationTryGuard& = delete;
    auto operator=(SharedVmemPublicationTryGuard&&) -> SharedVmemPublicationTryGuard& = delete;
    [[nodiscard]] auto acquired() const noexcept -> bool { return acquired_; }

   private:
    bool acquired_{};
};

// Counted PS-publication policy only, never root/Task lifetime ownership.
// prepare_current is reached solely from the owner-free public MREMAP entry.
// No borrowed preparation/PUB/root access survives that method or a yield.
class PendingMremapPsPolicy {
   public:
    PendingMremapPsPolicy() noexcept;
    ~PendingMremapPsPolicy() noexcept;
    PendingMremapPsPolicy(const PendingMremapPsPolicy&) = delete;
    PendingMremapPsPolicy(PendingMremapPsPolicy&&) = delete;
    auto operator=(const PendingMremapPsPolicy&) -> PendingMremapPsPolicy& = delete;
    auto operator=(PendingMremapPsPolicy&&) -> PendingMremapPsPolicy& = delete;
    [[nodiscard]] auto prepare_current(ker::mod::sched::task::Task&, uint64_t start, uint64_t end) noexcept -> int;
    void cancel() noexcept;

   private:
    // Non-owning executor identity/linkage, exactly like pending FD owners.
    ker::mod::sched::task::Task* executor_{};
    PendingMremapPsPolicy* previous_{};
    PendingMremapPsPolicy* next_{};
    ker::mod::mm::huge::RootKey key_{};
};
// Current executor only, before noreturn exit discards its kernel stack.
void cancel_mremap_ps_policies(ker::mod::sched::task::Task&) noexcept;

// Prepare outside publication. Owns exact parent binding snapshots and fresh
// unbound child bindings; destroy only after the publication guard has ended.
// Fork consumes prepared rows separately, copies checked PTE owners, then
// attaches these child-root identities before making the child visible.
class PublicFileForkPreparation {
   public:
    PublicFileForkPreparation() = default;
    ~PublicFileForkPreparation();
    PublicFileForkPreparation(const PublicFileForkPreparation&) = delete;
    auto operator=(const PublicFileForkPreparation&) -> PublicFileForkPreparation& = delete;
    [[nodiscard]] auto prepare(ker::mod::sched::task::Task& parent, const ker::mod::sched::task::LazyVmemClone& source) -> int;
    // Caller holds publication and has revalidated the exact source rows.
    [[nodiscard]] auto matches(ker::mod::sched::task::Task& parent) -> bool;
    // Child is still exclusively unpublished. Failure may attach a prefix;
    // ordinary child-root destruction must retire that prefix before deletion.
    [[nodiscard]] auto attach(ker::mod::sched::task::Task& child) -> bool;

   private:
    struct Entry;
    Entry* entries_{};
    size_t count_{};
    bool prepared_{};
};

// Unwired prepared legacy fork metadata. Caller retains exact actor/root and
// exclusively never-active child through commit, and destroys this holder only
// outside PUB/root/IF/preempt owners. Real checked File refs own every prefix.
class LegacyFileForkPreparation {
   public:
    LegacyFileForkPreparation() = default;
    ~LegacyFileForkPreparation();
    LegacyFileForkPreparation(const LegacyFileForkPreparation&) = delete;
    auto operator=(const LegacyFileForkPreparation&) -> LegacyFileForkPreparation& = delete;
    [[nodiscard]] auto prepare(ker::mod::sched::task::Task&, ker::mod::sched::task::Task&, const ker::mod::sys::SyscallPreparationScope&)
        -> int;
    [[nodiscard]] auto matches(ker::mod::sched::task::Task&, ker::mod::sched::task::Task&, const ker::mod::sys::SyscallPreparationScope&,
                               const SharedVmemPublicationGuard&) const -> int;
    [[nodiscard]] auto commit(ker::mod::sched::task::Task&, ker::mod::sched::task::Task&, const ker::mod::sys::SyscallPreparationScope&,
                              const SharedVmemPublicationGuard&) -> int;

   private:
    struct Storage;
    [[nodiscard]] auto context_matches(ker::mod::sched::task::Task&, ker::mod::sched::task::Task&,
                                       const ker::mod::sys::SyscallPreparationScope&) const -> bool;
    [[nodiscard]] auto rows_match_locked() const -> bool;
    Storage* storage_{};
    ker::mod::sched::task::Task* actor_{};
    ker::mod::sched::task::Task* child_{};
    ker::mod::mm::paging::PageTable* source_{};
    ker::mod::mm::paging::PageTable* destination_{};
    uint64_t source_serial_{}, destination_serial_{};
    bool prepared_{}, committed_{};
};

struct FileMmapCacheStats {
    uint64_t pages;
    uint64_t bytes;
    uint64_t capacity_pages;
};

// Caller retains the exact subject; user swap control is Host-only.
auto swap_control_for_subject(ker::mod::sched::task::Task* subject, uint64_t op, uint64_t address, uint64_t flags) -> uint64_t;

// Virtual memory syscall handler
// op: operation code (anon_allocate, anon_free)
// a1: hint address (for allocate) or address to free
// a2: size in bytes
// a3: protection flags (PROT_READ | PROT_WRITE | PROT_EXEC)
// a4: mapping flags (MAP_PRIVATE | MAP_ANONYMOUS, etc.)
auto sys_vmem(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t;
// Virtual memory mapping syscall handler
// hint: hint address
// size: size in bytes
// prot: protection flags
// flags: mapping flags
// fd: file descriptor (for file-backed mappings) or -1 for anonymous
// offset: offset in file (for file-backed mappings)
// addr: address to map (for MAP_FIXED) or 0
auto sys_vmem_map(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, uint64_t fd, uint64_t offset) -> uint64_t;
auto file_mmap_cache_stats() -> FileMmapCacheStats;
auto file_mmap_cache_reclaim(size_t max_pages) -> size_t;
void file_mmap_cache_register_shrinker();
// Production callers carry the initiating Task/CPU value across any worker.
auto materialize_lazy_file_page(ker::mod::sched::task::Task* task, const ker::mod::sched::task::LazyVmemRange& range, uint64_t page_vaddr,
                                const ker::mod::mm::paging::PageFault& fault, const ker::mod::mm::numa::AllocationContext& placement)
    -> bool;
// Internal/native compatibility capture before installer/root acquisition.
auto materialize_lazy_file_page(ker::mod::sched::task::Task* task, const ker::mod::sched::task::LazyVmemRange& range, uint64_t page_vaddr,
                                const ker::mod::mm::paging::PageFault& fault) -> bool;
// Caller must hold SharedVmemPublicationGuard. On failure, partial destination
// metadata remains owned by dst and must be released after dropping the guard.
auto clone_file_mmap_ranges_for_pagemap_locked(ker::mod::mm::paging::PageTable* src, ker::mod::mm::paging::PageTable* dst) -> bool;
// The pagemap must be unpublished or all Tasks that published it must have
// completed usercopy quiescence before teardown starts.
void release_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable* pagemap);

// Internal first native transaction: exact caller-owned unpublished process,
// private mapping only. No public mmap/loader/exec/fork admission is implied.
// Even preparation failure can leave a stable observed cleanup continuation;
// destroy the child through its ordinary unpublished cleanup protocol.
[[nodiscard]] auto prepare_unpublished_file_mapping(ker::mod::sched::task::Task& owner, ker::mod::sched::task::Task& child,
                                                    ker::vfs::File& file, uint64_t address, uint64_t length, uint64_t offset, uint64_t prot)
    -> int;
// Explicit bounded canonical TMPFS profile: registered mounted backend,
// READ/NONE only, fixed EOF and prepared canonical anchors. Still unpublished;
// generic VM mutation, public execution and fork remain unsupported.
[[nodiscard]] auto prepare_unpublished_read_shared_tmpfs_mapping(ker::mod::sched::task::Task& owner, ker::mod::sched::task::Task& child,
                                                                 ker::vfs::File& file, uint64_t address, uint64_t length, uint64_t offset,
                                                                 uint64_t prot) -> int;
// Enumerates actual lazy/registry/index holders. Unknown owners refuse before
// Task mutation; exact typed totals are checked only against that enumeration.
[[nodiscard]] auto validate_native_unpublished_mapping_owners(ker::mod::sched::task::Task& task) -> bool;

#ifdef WOS_SELFTEST
// Caller exclusively owns a never-active unpublished Task/root and address.
// Maps through the real anonymous zero reserve, not a synthetic registered page.
[[nodiscard]] auto selftest_map_anonymous_zero_page(ker::mod::sched::task::Task& subject, uint64_t address) -> bool;
// Bounded actual private helper calls on the same exclusive never-active
// target contract. These do not impersonate currentTask or exercise public
// syscall AS admission. A finite failure index is a controlled test operand,
// not a claim of physical pressure. Maximum length is sixteen pages.
[[nodiscard]] auto selftest_allocate_private_anonymous(ker::mod::sched::task::Task& subject, uint64_t address, uint64_t length,
                                                       uint64_t prot, uint64_t fail_before_page = UINT64_MAX) -> uint64_t;
// Exact absent/reserved leaf owned by the caller, with no swap/file owner.
// Failure operand only covers the private (non-NONE/no-WRITE) allocation.
[[nodiscard]] auto selftest_materialize_anonymous_reserved(ker::mod::sched::task::Task& subject, uint64_t address, uint64_t prot,
                                                           bool fail_private_allocation = false) -> bool;
struct SelftestFileCacheObservation {
    void* retained_frame{};  // Caller owns one actual frame ref on a hit.
    bool vacant_way{};       // Pre-SMP admission only, not a reservation.
};
[[nodiscard]] auto selftest_observe_file_mmap_cache(ker::vfs::File& file, uint64_t offset, SelftestFileCacheObservation& observation)
    -> bool;
[[nodiscard]] auto selftest_map_cached_private_file_page(ker::mod::sched::task::Task& subject, ker::vfs::File& file, uint64_t address,
                                                         uint64_t offset) -> bool;
[[nodiscard]] auto selftest_evict_file_mmap_cache_exact(ker::vfs::File& file, uint64_t offset, void* expected_frame) -> bool;
auto selftest_file_mmap_sync_preflight(ker::mod::mm::paging::PageTable* pagemap) -> bool;
auto selftest_file_mapping_admission(ker::mod::mm::paging::PageTable* pagemap) -> bool;
auto selftest_file_mapping_registry_growth(ker::mod::mm::paging::PageTable* pagemap) -> bool;
auto selftest_lazy_file_projection(ker::mod::mm::paging::PageTable* pagemap) -> bool;
auto selftest_file_writeback_policy() -> bool;
#endif

}  // namespace ker::syscall::vmem
