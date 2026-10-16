#pragma once

#include <platform/mm/managed_huge_core.hpp>
#include <platform/mm/numa_alloc.hpp>

namespace ker::mod::sched::task {
struct Task;
}
namespace ker::mod::sys {
class SyscallPreparationScope;
}
namespace ker::syscall::vmem {
class SharedVmemPublicationGuard;
class SharedVmemPublicationTryGuard;
}  // namespace ker::syscall::vmem

namespace ker::mod::mm::huge {
namespace detail {
struct PreparationAccess;
struct RangePreparationDriver;
// Actual virt adapters share user_mapping_pin_lock. Caller retains an actual
// root for capture; a copied RootKey is only an opaque comparison afterward.
[[nodiscard]] auto root_key(paging::PageTable*, RootKey& empty) noexcept -> Error;
[[nodiscard]] auto root_matches(RootKey) noexcept -> bool;
[[nodiscard]] auto initialize_caps(const CapsInput&) noexcept -> Error;
[[nodiscard]] auto reserve_leaf(RootKey, uint64_t va, uint16_t node, LeafHandle& empty) noexcept -> Error;
[[nodiscard]] auto cancel_leaf(LeafHandle) noexcept -> Error;
[[nodiscard]] auto reserve_plan(uint16_t pages, PlanHandle& empty) noexcept -> Error;
[[nodiscard]] auto release_plan(PlanHandle) noexcept -> Error;
void snapshot(Snapshot&) noexcept;
// Ordinary page-table pool only; every call occurs outside MM/root/PUB locks.
[[nodiscard]] auto allocate_table() noexcept -> paging::PageTable*;
void release_table(paging::PageTable*) noexcept;
}  // namespace detail

// BSP after immutable topology and successful C19 binding, before APs. The
// fixed scratch is exclusive bootstrap storage, not a concurrent query buffer.
[[nodiscard]] auto initialize_after_topology() noexcept -> Error;
void snapshot(Snapshot&) noexcept;

// Private managed publication and split keep their actual prepared owners
// outside the MM locks. No public huge admission exists. These holders require the
// accepted ordinary PROCESS preparation scope or exclusive unpublished root
// ownership, no incoming PUB/root-reader/MM/IRQ lock and addressable storage.
class PreparedSpan {
   public:
    PreparedSpan() = default;
    ~PreparedSpan();
    PreparedSpan(const PreparedSpan&) = delete;
    auto operator=(const PreparedSpan&) -> PreparedSpan& = delete;
    PreparedSpan(PreparedSpan&&) noexcept;
    auto operator=(PreparedSpan&&) noexcept -> PreparedSpan&;
    [[nodiscard]] auto prepare(RootKey, uint64_t va, const numa::AllocationContext&) noexcept -> Error;
    void reset() noexcept;
    [[nodiscard]] auto empty() const noexcept -> bool { return ticket_.generation == 0; }
    [[nodiscard]] auto data() const noexcept -> void* { return span_.data(); }
    [[nodiscard]] auto requested() const noexcept -> numa::NodeId { return requested_; }
    [[nodiscard]] auto actual() const noexcept -> numa::NodeId { return actual_; }

   private:
    friend struct detail::PreparationAccess;
    LeafHandle ticket_{};
    numa::OwnedSpan span_{};
    numa::NodeId requested_{numa::INVALID_NODE}, actual_{numa::INVALID_NODE};
};

struct SplitDescriptor {
    LeafKey key{};
    LeafHandle handle{};
    uint64_t semantic_word{};
};
struct GeometryStamp {
    RootKey root{};
    uint64_t epoch{};
};
// Plain VA-only progress. No table/control/Task/entry pointer persists between
// chunks. Storage is instrumented-caller constructed before any exclusion.
struct RangeSplitCursor {
    GeometryStamp stamp{};
    uint64_t start{}, end{}, next{};
    uint16_t count{};
    bool started{}, complete{}, epoch_changed{};
};
enum class RangeSurveyMode : uint8_t { Count, Fill, Verify };
class RangeSplitSurvey {
   public:
    RangeSplitSurvey() = default;
    [[nodiscard, gnu::always_inline]] auto complete() const noexcept -> bool { return complete_; }
    [[nodiscard, gnu::always_inline]] auto stamp() const noexcept -> const GeometryStamp& { return stamp_; }
    [[nodiscard, gnu::always_inline]] auto start() const noexcept -> uint64_t { return start_; }
    [[nodiscard, gnu::always_inline]] auto end() const noexcept -> uint64_t { return end_; }
    [[nodiscard, gnu::always_inline]] auto count() const noexcept -> uint16_t { return count_; }

   private:
    friend struct detail::RangePreparationDriver;
    GeometryStamp stamp_{};
    uint64_t start_{}, end_{};
    uint16_t count_{};
    bool complete_{};
};
enum class PlanPurpose : uint8_t { Unbound, Parents, Split, Cleanup };
struct SplitPlanStorage {
    SplitDescriptor descriptors[MAX_ROOT_LEAVES]{};
    paging::PageTable* tables[MAX_PLAN_TABLES]{};
    uint64_t table_physical[MAX_PLAN_TABLES]{};
    uint16_t descriptor_count{}, table_count{};
    RootKey root{};
    uint64_t start{}, end{}, geometry_epoch{};
    PlanPurpose purpose{};
    bool surveyed{};
};
static_assert(sizeof(SplitPlanStorage) <= 8 * 1024);

class PreparedTables {
   public:
    PreparedTables() = default;
    ~PreparedTables();
    PreparedTables(const PreparedTables&) = delete;
    auto operator=(const PreparedTables&) -> PreparedTables& = delete;
    PreparedTables(PreparedTables&&) noexcept;
    auto operator=(PreparedTables&&) noexcept -> PreparedTables&;
    [[nodiscard]] auto prepare(uint16_t count) noexcept -> Error;
    void reset() noexcept;
    [[nodiscard]] auto empty() const noexcept -> bool { return ticket_.generation == 0; }
    [[nodiscard]] auto count() const noexcept -> uint16_t { return storage_ != nullptr ? storage_->table_count : 0; }
    [[nodiscard]] auto table(uint16_t index) const noexcept -> paging::PageTable* {
        return storage_ != nullptr && index < storage_->table_count ? storage_->tables[index] : nullptr;
    }

   private:
    friend struct detail::PreparationAccess;
    [[nodiscard]] auto prepare_impl(uint16_t count
#ifdef WOS_SELFTEST
                                    ,
                                    uint16_t fail_before_table
#endif
                                    ) noexcept -> Error;
    PlanHandle ticket_{};
    SplitPlanStorage* storage_{};
};
// Fallible preparation owns no root/PUB/MM lease across allocations or yields.
// Caller owns ordinary PROCESS execution; no public syscall is wired here.
[[nodiscard]] auto prepare_current_range_split(sched::task::Task&, const sys::SyscallPreparationScope&, uint64_t start, uint64_t end,
                                               RangeSplitSurvey& empty, PreparedTables& empty_tables) noexcept -> Error;
class CurrentRangeSplitAdmission {
   public:
    CurrentRangeSplitAdmission(sched::task::Task&, const sys::SyscallPreparationScope&,
                               const ker::syscall::vmem::SharedVmemPublicationTryGuard&) noexcept;
    // Acquire blocking PUB in ordinary IF-on preparation BEFORE LegacyInterval.
    // This exact borrowed owner must outlive the admission.
    CurrentRangeSplitAdmission(sched::task::Task&, const sys::SyscallPreparationScope&,
                               const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept;
    ~CurrentRangeSplitAdmission() noexcept;
    CurrentRangeSplitAdmission(const CurrentRangeSplitAdmission&) = delete;
    CurrentRangeSplitAdmission(CurrentRangeSplitAdmission&&) = delete;
    auto operator=(const CurrentRangeSplitAdmission&) -> CurrentRangeSplitAdmission& = delete;
    auto operator=(CurrentRangeSplitAdmission&&) -> CurrentRangeSplitAdmission& = delete;
    [[nodiscard]] auto acquired() const noexcept -> bool { return root_ != nullptr; }
    [[nodiscard]] auto validate_split_and_arm(const RangeSplitSurvey&, PreparedTables&) noexcept -> Error;
    // Future metadata must call this in the SAME actual PUB/root/IF-off scope.
    [[nodiscard]] auto allows(const sched::task::Task&, uint64_t start, uint64_t end) const noexcept -> bool;

   private:
    [[nodiscard]] auto publication_acquired() const noexcept -> bool;
    sched::task::Task* subject_{};
    const sys::SyscallPreparationScope* preparation_{};
    const ker::syscall::vmem::SharedVmemPublicationTryGuard* publication_{};
    const ker::syscall::vmem::SharedVmemPublicationGuard* blocking_publication_{};
    paging::PageTable* root_{};
    size_t gate_{static_cast<size_t>(-1)};
    bool local_{}, armed_{};
    uint64_t start_{}, end_{};
};
namespace detail {
// Actual acquired lifetimes, never carried across a chunk's guard destruction.
struct CurrentRangeRootAccess {
    sched::task::Task* subject{};
    paging::PageTable* root{};
    size_t gate{static_cast<size_t>(-1)};
    bool local{};
};
[[nodiscard]] auto acquire_current_range_root(sched::task::Task&, CurrentRangeRootAccess& empty) noexcept -> Error;
void release_current_range_root(CurrentRangeRootAccess&) noexcept;
// At most16 cursor steps/48 raw hierarchy loads; owns COW->pin only internally.
[[nodiscard]] auto survey_range_chunk(paging::PageTable*, RangeSplitCursor&, RangeSurveyMode, SplitPlanStorage*) noexcept -> Error;
// Incoming actual lifetime/PUB authority; no nested reader or fallible work.
[[nodiscard]] auto commit_range_split(paging::PageTable*, const RangeSplitSurvey&, PreparedTables&) noexcept -> Error;
// Private legacy/native route: caller owns an exclusively unpublished root.
// No scheduled/public caller may use this in place of current preparation.
[[nodiscard]] auto prepare_unpublished_range_split(paging::PageTable*, uint64_t start, uint64_t end, RangeSplitSurvey& empty,
                                                   PreparedTables& empty_tables, uint16_t fail_before_table = UINT16_MAX) noexcept -> Error;
}  // namespace detail
namespace detail {
struct SpanPublicationView {
    LeafHandle handle{};
    uint64_t physical_2m{};
    uint16_t requested{}, actual{};
};
struct PreparationAccess {
    [[nodiscard]] static auto span_view(const PreparedSpan&, SpanPublicationView&) noexcept -> bool;
    static void span_transferred_after_publish(PreparedSpan&) noexcept;
    [[nodiscard]] static auto plan_storage(PreparedTables&) noexcept -> SplitPlanStorage*;
    static void tables_transferred_after_publish(PreparedTables&, uint64_t adopted_mask) noexcept;
#ifdef WOS_SELFTEST
    [[nodiscard]] static auto prepare_tables_for_test(PreparedTables&, uint16_t count, uint16_t fail_before_table) noexcept -> Error;
#endif
};
}  // namespace detail
}  // namespace ker::mod::mm::huge
