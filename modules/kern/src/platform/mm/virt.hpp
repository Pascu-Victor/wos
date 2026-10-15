#pragma once

#ifdef WOS_SELFTEST
#include <platform/mm/lifecycle_census.hpp>
#endif

#include <extern/limine.h>

#include <cstdint>
#include <platform/acpi/acpi.hpp>
#include <platform/asm/tlb.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/tlb_shootdown.hpp>
#include <platform/sched/task.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"

namespace ker::mod::mm::virt {
using namespace ker::mod::mm::addr;
using namespace ker::mod::mm::paging;

struct Range {
    uint64_t start;
    uint64_t end;
};

struct PageMapBatch {
    PageTable* root{};
    PageTable* pml3{};
    PageTable* pml2{};
    PageTable* pml1{};
    uint64_t cached_idx4 = UINT64_MAX;
    uint64_t cached_idx3 = UINT64_MAX;
    uint64_t cached_idx2 = UINT64_MAX;
    uint64_t table_flags{};
    bool dirty{};
};

class FileMappingInstallerLease {
   public:
    FileMappingInstallerLease() = default;
    ~FileMappingInstallerLease();
    FileMappingInstallerLease(const FileMappingInstallerLease&) = delete;
    auto operator=(const FileMappingInstallerLease&) -> FileMappingInstallerLease& = delete;
    [[nodiscard]] auto acquire(sched::task::Task& task, const sched::task::LazyVmemRange& range, uint64_t address) -> bool;
    [[nodiscard]] auto binding() const -> file_mapping::FileMappingBinding* { return binding_; }

   private:
    file_mapping::FileMappingBinding* binding_{};
    size_t access_gate_{static_cast<size_t>(-1)};
};

enum class PublicReadPrepareResult : uint8_t { NOT_TRACKED, READY, REFUSED };
// Ordinary pre-pin preparation only. No caller-owned root/pin/publication guard
// may span this potentially blocking demand backing operation.
[[nodiscard]] auto prepare_tmpfs_read_shared_page(sched::task::Task& task, uint64_t address, bool require_writable = false)
    -> PublicReadPrepareResult;
// Scalar lookup only: no root dereference or root lifetime acquisition. Caller
// holds actual Task/publication authority, or treats root only as an identity.
[[nodiscard]] auto capture_user_root_incarnation(PageTable* root, uint64_t& empty) noexcept -> bool;
[[nodiscard]] auto current_user_root_incarnation(sched::task::Task& task) -> uint64_t;
// Caller holds publication and the staged final cold-VA/lazy-row transaction.
[[nodiscard]] auto attach_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding*& prepared, uint64_t start,
                                               uint64_t end, uint64_t file_offset, uint64_t expected_serial) -> bool;
[[nodiscard]] auto query_current_file_binding(sched::task::Task& task, uint64_t start, uint64_t end,
                                              file_mapping::FileMappingBinding*& owned, bool& exact) -> bool;
// Exact current ACTIVE process only; publication remains held by caller.
// Preflight BEFORE changing any mirrored row; refusal has no metadata effects.
[[nodiscard]] auto preflight_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding) -> bool;
// Final operations follow successful preflight plus infallible mirrored-row
// withdrawal/rewrite. Contention retries; broken premises panic, never EAGAIN.
// Caller owns binding SNAPSHOT. after_tlb starts empty and must be released
// outside publication after successful retirement; its actual ACK is complete.
[[nodiscard]] auto retire_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding,
                                               file_mapping::FileMappingTlbRetirement& after_tlb) -> bool;
[[nodiscard]] auto protect_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding, uint64_t prot) -> bool;

[[nodiscard]] auto attach_unpublished_file_binding(file_mapping::FileMappingBinding*& prepared, PageTable* root, uint64_t start,
                                                   uint64_t end, uint64_t file_offset) -> bool;
// Caller owns the unpublished root. Bounded nonwaiting observation of all 256
// user PML4 slots; upper kernel mappings stay outside the first-native profile.
[[nodiscard]] auto unpublished_user_root_empty(PageTable* root) -> bool;
// Ordinary preparation only: the caller owns a stable, exclusively unpublished
// root and its retirement record. Failure may leave root-owned empty upper
// tables; no leaf is published. Do not hold publication/lazy/cow/MM guards.
[[nodiscard]] auto prepare_unpublished_file_mapping_tables(PageTable* root, uint64_t start, uint64_t length, uint64_t owner_pid) -> bool;
// Caller owns the actual root. Snapshot references must be released outside MM guards.
[[nodiscard]] auto snapshot_file_mapping_bindings(PageTable* root, file_mapping::FileMappingBinding** output, size_t capacity,
                                                  size_t& owned_count) -> bool;

struct UserMemoryStats {
    uint64_t virtual_pages;
    uint64_t resident_pages;
    uint64_t shared_pages;
    uint64_t page_table_pages;
    uint64_t swapped_pages;
    uint64_t swap_inflight_pages;
};

struct AnonymousSwapStatsSnapshot {
    uint64_t resident_candidates;
    uint64_t swapped_pages;
    uint64_t pageout_inflight;
    uint64_t pagein_inflight;
    uint64_t pageout_attempts;
    uint64_t pageout_successes;
    uint64_t pageout_failures;
    uint64_t pagein_attempts;
    uint64_t pagein_successes;
    uint64_t pagein_failures;
    uint64_t pagein_corruption;
    uint64_t pageout_latency_us;
    uint64_t pageout_latency_max_us;
    uint64_t pagein_latency_us;
    uint64_t pagein_latency_max_us;
};

struct UserPagePin {
    PageTable* pagemap{};
    vaddr_t user_page{};
    paddr_t physical_page{static_cast<paddr_t>(-1)};
    void* hhdm_page{};
    phys::PageLookupHint lookup_hint{};
    size_t access_gate_index{static_cast<size_t>(-1)};
    bool require_writable{};
    bool dirty{};
    file_mapping::FileMappingBinding* file_binding{};  // One exact PIN role.
    uint64_t root_incarnation{};
};

// A value snapshot from a live pin; it owns no Task, root or backing lease.
// Shared-frame identity deliberately excludes the requesting address space.
struct UserFutexIdentity {
    uint64_t root_incarnation{};
    vaddr_t virtual_word{};
    paddr_t physical_word{};
    bool shared{};
};

struct UserRootControlStats {
    uint64_t active_roots{};
    uint64_t control_pages{};
    uint64_t last_serial{};
};

struct DestroyUserSpaceStats {
    uint64_t calls;
    uint64_t collect_frames_us_total;
    uint64_t collect_frames_us_max;
    uint64_t free_data_us_total;
    uint64_t free_data_us_max;
    uint64_t free_pt_us_total;
    uint64_t free_pt_us_max;
    uint64_t tlb_flush_us_total;
    uint64_t tlb_flush_us_max;
    uint64_t data_leaf_entries_visited;
    uint64_t data_pages_ref_decremented;
    uint64_t data_pages_freed;
    uint64_t page_table_pages_ref_decremented;
    uint64_t page_table_pages_freed;
    uint64_t skipped_huge_pages;
    uint64_t skipped_unknown_frames;
    uint64_t skipped_slab_alloc_frames;
    uint64_t skipped_medium_alloc_frames;
    uint64_t skipped_kmalloc_large_alloc_frames;
    uint64_t skipped_page_table_aliases;
    uint64_t skipped_corrupt_entries;
    uint64_t magic_unknown_probe_reads;
    uint64_t magic_unknown_slab_hits;
    uint64_t magic_unknown_medium_hits;
    uint64_t magic_unknown_kmalloc_large_hits;
};

struct PageTablePoolStatsSnapshot {
    uint64_t capacity;
    uint64_t cached_pages;
    uint64_t alloc_hits;
    uint64_t alloc_misses;
    uint64_t releases;
    uint64_t rejects;
};

struct OwnedFrameStatsSnapshot {
    uint64_t capacity;
    uint64_t entries;
    uint64_t track_attempts;
    uint64_t track_added;
    uint64_t track_replaced;
    uint64_t track_skipped;
    uint64_t track_conflicts;
    uint64_t track_probe_failures;
    uint64_t untrack_attempts;
    uint64_t untrack_removed;
    uint64_t untrack_missed;
    uint64_t purge_calls;
    uint64_t purge_removed;
};

struct KernelVmapReclaimStats {
    size_t scanned_pages{};
    size_t reclaimed_pages{};
    bool has_more{};
    bool failed{};
    bool unsafe_context{};
};

struct KernelProtectionAudit {
    bool nx_supported{};
    bool nx_active{};
    bool write_protect_active{};
    bool layout_valid{};
    bool text_readonly{};
    bool text_executable{};
    bool rodata_readonly{};
    bool rodata_non_executable{};
    bool data_writable{};
    bool data_non_executable{};
    bool emergency_stack_guarded{};
    bool direct_alias_readonly{};
    bool direct_alias_non_executable{};
    bool debug_metadata_readonly{};
    bool debug_metadata_non_executable{};

    [[nodiscard]] constexpr auto enforced() const -> bool {
        return nx_supported && nx_active && write_protect_active && layout_valid && text_readonly && text_executable && rodata_readonly &&
               rodata_non_executable && data_writable && data_non_executable && emergency_stack_guarded && direct_alias_readonly &&
               direct_alias_non_executable && debug_metadata_readonly && debug_metadata_non_executable;
    }
};

enum class LazyFilePageInstallResult : uint8_t {
    MAPPED,
    ALREADY_MAPPED,
    STALE_RANGE,
};

struct DestroyUserSpaceBudgetState;

void init(limine_memmap_response* memmap_response, limine_executable_file_response* kernel_file_response,
          limine_executable_address_response* kernel_address_response);

static inline auto get_kernel_page_table() -> PageTable* { return reinterpret_cast<PageTable*>(rdcr3()); }

// Installs one mapping reference. Replacing a different present frame releases
// the displaced mapping reference only after the required TLB invalidation.
void map_page(PageTable* page_table, vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void init_page_map_batch(PageMapBatch* batch, PageTable* page_table, uint64_t flags);
// Batched replacement has the same ownership contract as map_page(); it flushes
// the current batch before releasing a displaced present frame.
void map_page_batched(PageMapBatch* batch, vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void flush_page_map_batch(PageMapBatch* batch);
void map_same_page_range(PageTable* page_table, vaddr_t vaddr, paddr_t paddr, uint64_t page_count, uint64_t flags);
void reserve_page_range(PageTable* page_table, vaddr_t vaddr, uint64_t page_count);
bool is_page_mapped(PageTable* page_table, vaddr_t vaddr);
bool is_page_reserved(PageTable* page_table, vaddr_t vaddr);
bool is_page_mapped_or_reserved(PageTable* page_table, vaddr_t vaddr);
// Caller retains the pagemap and serializes mapping publication. No allocation
// or page-table promotion; includes present PROT_NONE shared leaves.
[[nodiscard]] auto is_page_write_restricted(PageTable* page_table, vaddr_t vaddr) -> bool;
void unify_page_flags(PageTable* page_table, vaddr_t vaddr, uint64_t flags);
void map_range(PageTable* page_table, Range range, uint64_t flags, uint64_t offset = 0);
void unmap_page(PageTable* page_table, vaddr_t vaddr);
// Updates protection metadata for a non-resident anonymous leaf without
// faulting it in. Returns true when the page is tracked by anonymous swap.
[[nodiscard]] auto protect_anonymous_swap_page(PageTable* page_table, vaddr_t vaddr, uint64_t flags) -> bool;

void switch_to_kernel_pagemap();
PageTable* get_kernel_pagemap();
void init_tlb_shootdown();
void note_tlb_shootdown_cpu_online();
void init_pagemap();
[[nodiscard]] auto audit_kernel_protections() -> KernelProtectionAudit;
// Final supervisor mappings use these after init_pagemap() has established the
// CPU feature policy. The base page_types constants remain NX-free so the
// unsupported-CPU fallback never installs a reserved PTE bit.
[[nodiscard]] auto kernel_data_page_flags() -> uint64_t;
[[nodiscard]] auto kernel_mmio_page_flags() -> uint64_t;
// Prepare a bounded kernel virtual arena whose runtime allocations are backed
// by independent order-0 physical pages. Call after init_pagemap() and before
// task pagemaps copy the kernel-half PML4 entries.
void init_kernel_vmap();
// Permanently maps fixed-size stack slots through a dedicated virtual arena.
// Each usable stack has an unmapped page below and above it; slots are never
// remapped at runtime, so IRQ-constrained stack retirement needs no shootdown.
[[nodiscard]] auto init_guarded_kernel_stack_pool(void* backing, size_t capacity) -> bool;
[[nodiscard]] auto guarded_kernel_stack_slot(size_t index) -> void*;
[[nodiscard]] auto audit_guarded_kernel_stack(const void* stack_base) -> bool;
auto kernel_vmap_alloc(PhysicalPageOwner owner, uint64_t size, std::string_view name = {}) -> void*;
void kernel_vmap_free(void* ptr, uint64_t size);
[[nodiscard]] auto kernel_vmap_contains(const void* ptr) -> bool;
// Exact deferred backing pages that a safe-context drain can return.
[[nodiscard]] auto kernel_vmap_pending_free_pages() -> size_t;
// Release at most max_pages deferred mappings. The PTE invalidation, global
// TLB shootdown, and physical-reference release ordering is unchanged.
[[nodiscard]] auto drain_kernel_vmap_frees_bounded(size_t max_pages) -> KernelVmapReclaimStats;
// Complete frees deferred by IRQ-disabled callers. Safe contexts may call this
// opportunistically; it is a no-op while IRQs or preemption are disabled.
void drain_kernel_vmap_frees();
// Drain only when deferred mappings have reached the warm-pool cap. Callers
// that queue frees under an IRQ lock should retry this after releasing it.
void drain_kernel_vmap_frees_if_over_limit();
PageTable* create_pagemap();
// Recoverable private process construction; retains the page-table pool path.
auto create_pagemap_may_fail() -> PageTable*;
// owner_pid is diagnostic provenance only; it grants no teardown authority.
void release_pagemap(PageTable* pagemap, uint64_t owner_pid = 0);
void get_user_root_control_stats_snapshot(UserRootControlStats& out);
#ifdef WOS_SELFTEST
struct SelftestRootCensus {
    ker::mod::mm::lifecycle_census::Stamp stamp{};
    UserRootControlStats roots{};
    uint64_t indexed_bindings{};
    bool complete{};
};
[[nodiscard]] auto selftest_root_census() -> SelftestRootCensus;
#endif
void get_page_table_pool_stats_snapshot(PageTablePoolStatsSnapshot& out);
void get_owned_frame_stats_snapshot(OwnedFrameStatsSnapshot& out);
// Refresh only the kernel half using the current kernel root. Caller retains
// the root and serializes publication, or owns a private unpublished root.
void copy_kernel_mappings_to(PageTable* pagemap);
void copy_kernel_mappings(sched::task::Task* t);
void switch_pagemap(sched::task::Task* t);
bool pagefault_handler(uint64_t control_register, gates::InterruptFrame& frame, ker::mod::cpu::GPRegs& gpr);
void map_to_kernel_page_table(vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void map_range_to_kernel_page_table(Range range, uint64_t flags, uint64_t offset);
// assume hhdm as offset
void map_range_to_kernel_page_table(Range range, uint64_t flags);
[[nodiscard]] bool active_pagemap_is(PageTable* pagemap);
[[nodiscard]] bool user_page_mapped_now(PageTable* pagemap, vaddr_t vaddr);
[[nodiscard]] bool user_page_writable_now(PageTable* pagemap, vaddr_t vaddr);
// Sentinel returned by translate() when the virtual address is not mapped.
// Using (paddr_t)-1 so physical address 0 remains valid.
static constexpr paddr_t PADDR_INVALID = static_cast<paddr_t>(-1);

paddr_t translate(PageTable* page_table, vaddr_t vaddr);
[[nodiscard]] auto pin_user_page(PageTable* pagemap, vaddr_t vaddr, bool require_writable, UserPagePin& out, bool wait_for_teardown = false)
    -> bool;
// Acquires the shared pagemap lifetime gate before optional lazy/COW
// resolution, then transfers that read-side hold into out until unpin.
[[nodiscard]] auto pin_user_page_for_task(sched::task::Task* task, PageTable* expected_pagemap, vaddr_t vaddr, bool require_writable,
                                          bool fault_in, UserPagePin& out) -> bool;
[[nodiscard]] auto user_page_pin_still_mapped(const UserPagePin& pin) -> bool;
// Futex-only provenance. Caller keeps the original pin live; a busy output is
// unchanged on refusal. Capture/revalidation allocate nothing and never sleep.
[[nodiscard]] auto capture_user_futex_identity(const UserPagePin& pin, vaddr_t word, UserFutexIdentity& out) -> bool;
[[nodiscard]] auto user_futex_identity_still_mapped(const UserPagePin& pin, const UserFutexIdentity& identity) -> bool;
// Linearizes an HHDM write against leaf replacement and records the same PTE
// dirty state that a hardware write through the user mapping would set.
[[nodiscard]] auto user_page_pin_commit_write(const UserPagePin& pin) -> bool;
void unpin_user_page(UserPagePin& pin);
// Kernel-internal placement telemetry. Samples are independently monotonic
// scalars, not a globally atomic census. Only winning new private installs
// enter the locality denominator; software materialization is distinguished.
enum class DemandOrigin : uint8_t { HardwareFault, SoftwareEnsure };
enum class DemandBacking : uint8_t { Anonymous, ZeroCow, PrivateCowCopy };
enum class DemandExclusion : uint8_t { Eligible, Software, KernelFault, ExplicitPolicy, PrivateCopy, NoLocalRam };
struct PrivateDemandSnapshot {
    uint64_t candidates{};
    uint64_t allocation_failures{};
    uint64_t discarded{};
    uint64_t promotions{};
    uint64_t installed{};
    uint64_t anonymous{};
    uint64_t zero_cow{};
    uint64_t private_cow{};
    uint64_t software{};
    uint64_t hardware_user{};
    uint64_t hardware_kernel{};
    uint64_t eligible{};
    uint64_t local{};
    uint64_t locality_misses{};
    uint64_t fallbacks{};
    bool overflow{};
};
void get_private_demand_snapshot(PrivateDemandSnapshot& output) noexcept;
#ifdef WOS_SELFTEST
// Caller owns an actual live root, or uses the permanent kernel root. Only the
// scalar escapes; this is not an authority handle for a later raw release.
[[nodiscard]] auto selftest_user_root_incarnation(PageTable* owned_root) -> uint64_t;
enum class SelftestDemandAction : uint8_t {
    None,
    ChangePolicyAfterCapture,
    FailCowAllocation,
    DowngradeCowRead,
    DowngradeCowNone,
};
struct SelftestDemandObservation {
    numa::Policy captured_policy{};
    numa::NodeId local_node{numa::INVALID_NODE};
    numa::NodeId requested{numa::INVALID_NODE};
    numa::NodeId actual{numa::INVALID_NODE};
    DemandOrigin origin{DemandOrigin::SoftwareEnsure};
    DemandBacking backing{DemandBacking::Anonymous};
    DemandExclusion exclusion{DemandExclusion::Software};
    uint64_t cookie{};
    uint64_t captures{};
    uint64_t candidates{};
    uint64_t allocation_failures{};
    uint64_t installed{};
    uint64_t discarded{};
    uint64_t promotions{};
    bool admitted{};
    bool disarmed{};
    bool foreign_target{};
    bool action_consumed{};
    bool used_fallback{};
};
// Synchronous actual software ensure/copy on an exclusively owned unpublished
// Task/root. A hardware-origin classifier test does not synthesize a CPL3 trap.
[[nodiscard]] auto selftest_observe_private_demand(sched::task::Task& subject, uint64_t address, bool writable,
                                                   SelftestDemandObservation& output,
                                                   SelftestDemandAction action = SelftestDemandAction::None, numa::Policy replacement = {})
    -> bool;
[[nodiscard]] auto selftest_classify_private_demand(DemandOrigin origin, bool hardware_user, DemandBacking backing, numa::Policy policy,
                                                    bool local_has_ram) noexcept -> DemandExclusion;
enum class SelftestCowAction : uint8_t {
    FAIL_DESTINATION_ALLOCATION,
    DOWNGRADE_READ_BEFORE_COMMIT,
    DOWNGRADE_NONE_BEFORE_COMMIT,
};
struct SelftestCowObservation {
    bool admitted{};
    uint32_t consumed{};
    void* destination{};
    bool action_outside_spinlocks{};
    bool disarmed{};
};
// Scoped actual copyout on an owned, unpublished private COW leaf. The action
// runs once outside the resolver's locks; no allocator-wide failure or callback.
[[nodiscard]] auto selftest_copy_to_task_cow_action(sched::task::Task& subject, uint64_t address, const void* source, size_t size,
                                                    SelftestCowAction action, SelftestCowObservation& observation) -> bool;
// Native callers retain an actual Task-access hold for the exact expected root.
// Observation is local to this call; production pin signatures are unchanged.
struct UserPagePinPathObservation {
    uint32_t early_attempts{};
    uint32_t early_successes{};
    uint32_t writable_resolution_calls{};
};
[[nodiscard]] auto selftest_pin_user_page_for_task_observed(sched::task::Task* task, PageTable* expected_pagemap, vaddr_t vaddr,
                                                            bool require_writable, bool fault_in, UserPagePin& out,
                                                            UserPagePinPathObservation& observation) -> bool;
[[nodiscard]] auto selftest_user_pagemap_reader_count(PageTable* pagemap) -> uint32_t;
// Permission-only exchange in an exclusively owned, never-active native root.
[[nodiscard]] auto selftest_exchange_unpublished_user_entry(sched::task::Task& task, PageTable* expected_pagemap, vaddr_t vaddr,
                                                            unsigned level, uint64_t expected_raw, uint64_t replacement_raw) -> bool;
// Arms one exact recoverable kernel page fault. KTEST parks secondary CPUs;
// the caller must also mask local interrupts around the probe. A mismatched
// fault is deliberately not consumed and therefore follows the normal panic
// path.
[[nodiscard]] auto selftest_arm_expected_kernel_page_fault(vaddr_t fault_address, vaddr_t fault_rip, vaddr_t recovery_rip,
                                                           uint64_t expected_error, uint64_t error_mask) -> bool;
[[nodiscard]] auto selftest_finish_expected_kernel_page_fault() -> bool;
// Low-perturbation allocator guard. Before init_pagemap() completes this is a
// no-op; afterwards it reports the exact live page-table path for a missing or
// non-identity HHDM leaf without allocating or attempting a repair.
bool selftest_direct_map_contains(const void* ptr);
// Returns true for a page-table page that has been observed in the permanent
// kernel hierarchy. The registry is append-only so an illegal release remains
// detectable even after the frame has been returned to the buddy allocator.
bool selftest_kernel_page_table_frame(const void* ptr);
// Counts the actual append-only permanent-kernel table registry. Native owner
// baselines use its exact growth to distinguish KASan shadow hierarchy pages.
[[nodiscard]] auto selftest_kernel_page_table_count() -> uint64_t;
// Models the teardown/fork writer side of the shared-pagemap gate and proves
// an IRQ/scheduler-style mapped pin fails closed instead of waiting or walking.
bool selftest_user_pagemap_exclusive_rejects_mapped_pin(PageTable* pagemap);
// Deterministic task-context hooks used by KTEST to exercise the real PTE,
// refcount, swap-backend, and teardown paths without inducing allocator OOM.
bool selftest_anonymous_swap_pageout(PageTable* pagemap, vaddr_t vaddr);
bool selftest_anonymous_swap_pagein(PageTable* pagemap, vaddr_t vaddr);
bool selftest_anonymous_swap_is_swapped(PageTable* pagemap, vaddr_t vaddr);
#endif
auto install_lazy_file_page_if_current(sched::task::Task* task, const sched::task::LazyVmemRange& range, vaddr_t page_vaddr,
                                       paddr_t page_paddr, uint64_t page_flags,
                                       file_mapping::FileMappingBinding* expected_binding = nullptr) -> LazyFilePageInstallResult;
bool ensure_user_page_writable(sched::task::Task* task, vaddr_t vaddr);
bool ensure_user_page_mapped(sched::task::Task* task, vaddr_t vaddr);
auto collect_user_memory_stats(PageTable* page_table) -> UserMemoryStats;
// Nonwaiting MM lifetime-gate check for a count proven by a serialized mapping
// transaction. Caller retains the Task/root; no PTE walk or allocation.
[[nodiscard]] auto validate_user_pagemap_access(PageTable* page_table) -> bool;
// Logical user VA, including non-present reservations. Caller serializes
// mapping publication or owns an unpublished root. Bounds must be page aligned.
// A root teardown collision fails closed without blocking or allocating.
auto count_user_virtual_bytes(PageTable* page_table, uint64_t start, uint64_t end, uint64_t& bytes) -> bool;
// Same publication/root-lifetime contract as counting, but stops at the first
// present or reserved leaf. Invalid/empty ranges and teardown fail closed.
auto is_user_virtual_range_free(PageTable* page_table, uint64_t start, uint64_t end) -> bool;
void get_anonymous_swap_stats_snapshot(AnonymousSwapStatsSnapshot& out);
void register_anonymous_swap_shrinker();
void start_anonymous_swap_worker();
auto get_destroy_user_space_stats(uint64_t cpu_no) -> DestroyUserSpaceStats;
auto prepare_destroy_user_space_budget_state() -> DestroyUserSpaceBudgetState*;
[[nodiscard]] auto begin_prepared_destroy_user_space(DestroyUserSpaceBudgetState& state, PageTable* pagemap, uint64_t owner_pid,
                                                     const char* owner_name, const char* reason, bool wait_for_gate = true) -> bool;
auto create_destroy_user_space_budget_state(PageTable* pagemap, uint64_t owner_pid = 0, const char* owner_name = nullptr,
                                            const char* reason = nullptr) -> DestroyUserSpaceBudgetState*;
auto destroy_user_space_budgeted(DestroyUserSpaceBudgetState* state, uint32_t max_steps) -> bool;
void destroy_user_space_budget_state_destroy(DestroyUserSpaceBudgetState* state);
// Native continuation only: DONE and an actual held root-exclusive gate. Success
// consumes the walk and original sole root owner without dropping/reacquiring the
// striped gate, then retires completed-TLB bindings outside that gate. Refusal
// preserves state. Do not separately destroy the walk or release its root.
[[nodiscard]] auto finish_destroy_user_space_and_release_pagemap(DestroyUserSpaceBudgetState* state) -> bool;

// Free all user-space pages and page tables in a pagemap
// Only frees the lower half (user space), keeps kernel mappings intact
// After calling this, the root pagemap itself should be released with release_pagemap().
void destroy_user_space(PageTable* pagemap, uint64_t owner_pid = 0, const char* owner_name = nullptr, const char* reason = nullptr);

// Deep-copy user-space page tables from src to dst using COW.
// Both src and dst PML1 entries are marked read-only + COW bit.
// Physical data page refcounts are incremented.
// Page table structures (PML3/PML2/PML1) are freshly allocated for dst.
// Returns true on success, false on OOM.
bool deep_copy_user_pagemap_cow(PageTable* src, PageTable* dst);

// Debug helper: log active/dead user mappings that still reference a physical page.
// Intended for targeted fault-path diagnostics only.
bool debug_log_user_phys_mappings(uint64_t target_phys, const char* trigger, uint64_t owner_pid = 0, const char* owner_name = nullptr,
                                  bool log_when_empty = true);
}  // namespace ker::mod::mm::virt
