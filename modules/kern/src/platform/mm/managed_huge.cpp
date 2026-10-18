#include "managed_huge.hpp"

#include <cerrno>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>

namespace ker::mod::mm::huge {
namespace {
phys::NodeCounterSnapshot bootstrap_counts{};
phys::NodeCounterWorkspace bootstrap_workspace{};
}  // namespace

auto initialize_after_topology() noexcept -> Error {
    if (!phys::node_counters_ready() || phys::snapshot_node_counters(bootstrap_counts, bootstrap_workspace) != NodeCounterError::NONE ||
        !bootstrap_counts.complete || bootstrap_counts.node_count == 0 || bootstrap_counts.node_count > MAX_NODES)
        return Error::NotReady;
    CapsInput input{};
    input.node_count = bootstrap_counts.node_count;
    uint64_t sum = 0;
    for (size_t node = 0; node < input.node_count; ++node) {
        input.usable[node] = bootstrap_counts.nodes[node].usable;
        if (UINT64_MAX - sum < input.usable[node]) return Error::Invalid;
        sum += input.usable[node];
    }
    if (sum != bootstrap_counts.global.usable) return Error::Invalid;
    return detail::initialize_caps(input);
}

void snapshot(Snapshot& output) noexcept { detail::snapshot(output); }

PreparedSpan::~PreparedSpan() { reset(); }
PreparedSpan::PreparedSpan(PreparedSpan&& other) noexcept
    : ticket_(std::exchange(other.ticket_, {})),
      span_(std::move(other.span_)),
      requested_(std::exchange(other.requested_, numa::INVALID_NODE)),
      actual_(std::exchange(other.actual_, numa::INVALID_NODE)) {}
auto PreparedSpan::operator=(PreparedSpan&& other) noexcept -> PreparedSpan& {
    if (this != &other) {
        reset();
        ticket_ = std::exchange(other.ticket_, {});
        span_ = std::move(other.span_);
        requested_ = std::exchange(other.requested_, numa::INVALID_NODE);
        actual_ = std::exchange(other.actual_, numa::INVALID_NODE);
    }
    return *this;
}
void PreparedSpan::reset() noexcept {
    // The actual owner returns every frame before the scarce capacity credit.
    span_.reset();
    auto const TICKET = std::exchange(ticket_, {});
    if (TICKET.generation != 0 && detail::cancel_leaf(TICKET) != Error::None) __builtin_trap();
    requested_ = actual_ = numa::INVALID_NODE;
}
auto PreparedSpan::prepare(RootKey root, uint64_t va, const numa::AllocationContext& context) noexcept -> Error {
    if (!empty() || !span_.empty() || !RegistryCore::valid_root(root) || !RegistryCore::valid_va(va) || context.virtual_page_index != 0)
        return Error::Invalid;
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr) return Error::NotReady;
    auto const selection = numa::select_nodes(context.policy, *topology, context.local_node, va / LEAF_BYTES);
    if (!selection.valid()) return Error::Invalid;
    Error last = Error::Exhausted;
    for (size_t index = 0; index < selection.count; ++index) {
        auto const NODE = selection.nodes[index];
        LeafHandle ticket{};
        Error const RESERVED = detail::reserve_leaf(root, va, NODE, ticket);
        if (RESERVED != Error::None) {
            if (RESERVED == Error::Capacity) {
                last = RESERVED;
                continue;
            }
            return RESERVED;
        }
        auto single = context;
        single.policy = {uint64_t{1} << NODE, numa::Mode::BIND};
        single.virtual_page_index = va / LEAF_BYTES;
        auto allocation = numa::try_allocate_pages(single, LEAF_PAGES, LEAF_BYTES, PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                                   phys::PageInitialization::ZERO, numa::ReclaimMode::NEVER, "huge-prepared");
        Error result = allocation.span.empty() ? Error::Exhausted : Error::None;
        if (result == Error::None) {
            uint64_t const PHYSICAL =
                reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(allocation.span.data())));
            if (allocation.span.pages() != LEAF_PAGES || allocation.actual != NODE || (PHYSICAL & (LEAF_BYTES - 1)) != 0)
                result = Error::Invalid;
            for (uint64_t page = 0; result == Error::None && page < LEAF_PAGES; ++page) {
                auto* frame = static_cast<uint8_t*>(allocation.span.data()) + page * PAGE_BYTES;
                if (phys::page_kind_get(frame) != PageKind::NORMAL || phys::page_ref_get(frame) != 1 ||
                    numa::physical_node(PHYSICAL + page * PAGE_BYTES) != NODE)
                    result = Error::Invalid;
            }
            if (result == Error::None && !detail::root_matches(root)) result = Error::Stale;
        }
        if (result == Error::None) {
            ticket_ = ticket;
            span_ = std::move(allocation.span);
            requested_ = selection.requested;
            actual_ = NODE;
            return Error::None;
        }
        allocation.span.reset();
        if (detail::cancel_leaf(ticket) != Error::None) __builtin_trap();
        if (result != Error::Exhausted) return result;
        last = result;
    }
    return last;
}

PreparedTables::~PreparedTables() { reset(); }
PreparedTables::PreparedTables(PreparedTables&& other) noexcept
    : ticket_(std::exchange(other.ticket_, {})), storage_(std::exchange(other.storage_, nullptr)) {}
auto PreparedTables::operator=(PreparedTables&& other) noexcept -> PreparedTables& {
    if (this != &other) {
        reset();
        ticket_ = std::exchange(other.ticket_, {});
        storage_ = std::exchange(other.storage_, nullptr);
    }
    return *this;
}
void PreparedTables::reset() noexcept {
    if (storage_ != nullptr) {
        for (uint16_t i = 0; i < storage_->table_count; ++i) detail::release_table(storage_->tables[i]);
        delete std::exchange(storage_, nullptr);
    }
    auto const TICKET = std::exchange(ticket_, {});
    if (TICKET.generation != 0 && detail::release_plan(TICKET) != Error::None) __builtin_trap();
}
auto PreparedTables::prepare(uint16_t count, RangePrepareSite* failure_site) noexcept -> Error {
    return prepare_impl(count, failure_site
#ifdef WOS_SELFTEST
                        ,
                        UINT16_MAX
#endif
    );
}
auto PreparedTables::prepare_impl(uint16_t count, RangePrepareSite* failure_site
#ifdef WOS_SELFTEST
                                  ,
                                  uint16_t fail_before_table
#endif
                                  ) noexcept -> Error {
    if (!empty() || storage_ != nullptr || count == 0 || count > MAX_PLAN_TABLES) {
        if (failure_site != nullptr) *failure_site = RangePrepareSite::Input;
        return Error::Invalid;
    }
    PlanHandle ticket{};
    if (auto const error = detail::reserve_plan(count, ticket); error != Error::None) {
        if (failure_site != nullptr) *failure_site = RangePrepareSite::PlanReserve;
        return error;
    }
    ticket_ = ticket;
    storage_ = new (std::nothrow) SplitPlanStorage{};
    if (storage_ == nullptr) {
        if (failure_site != nullptr) *failure_site = RangePrepareSite::PlanAllocation;
        reset();
        return Error::Exhausted;
    }
    for (uint16_t i = 0; i < count; ++i) {
#ifdef WOS_SELFTEST
        if (i == fail_before_table) {
            if (failure_site != nullptr) *failure_site = RangePrepareSite::PlanAllocation;
            reset();
            return Error::Exhausted;
        }
#endif
        auto* table = detail::allocate_table();
        if (table == nullptr) {
            if (failure_site != nullptr) *failure_site = RangePrepareSite::PlanAllocation;
            reset();
            return Error::Exhausted;
        }
        storage_->table_physical[storage_->table_count] =
            reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(table)));
        storage_->tables[storage_->table_count++] = table;
    }
    return Error::None;
}
#ifdef WOS_SELFTEST
auto detail::PreparationAccess::prepare_tables_for_test(PreparedTables& owner, uint16_t count, uint16_t before,
                                                        RangePrepareSite* failure_site) noexcept -> Error {
    return owner.prepare_impl(count, failure_site, before);
}
#endif
auto detail::PreparationAccess::span_view(const PreparedSpan& owner, SpanPublicationView& empty) noexcept -> bool {
    if (empty.handle.generation != 0 || owner.ticket_.generation == 0 || owner.span_.empty() || owner.span_.pages() != LEAF_PAGES)
        return false;
    empty = {owner.ticket_, reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(owner.span_.data()))),
             owner.requested_, owner.actual_};
    return true;
}
void detail::PreparationAccess::span_transferred_after_publish(PreparedSpan& owner) noexcept {
    if (owner.ticket_.generation == 0 || owner.span_.empty()) __builtin_trap();
    (void)owner.span_.release();
    owner.ticket_ = {};
    owner.requested_ = owner.actual_ = numa::INVALID_NODE;
}
auto detail::PreparationAccess::plan_storage(PreparedTables& owner) noexcept -> SplitPlanStorage* { return owner.storage_; }
void detail::PreparationAccess::tables_transferred_after_publish(PreparedTables& owner, uint64_t adopted_mask) noexcept {
    if (owner.storage_ == nullptr || owner.ticket_.generation == 0 ||
        (owner.storage_->table_count < 64 && (adopted_mask >> owner.storage_->table_count) != 0))
        __builtin_trap();
    for (uint16_t i = 0; i < owner.storage_->table_count; ++i)
        if ((adopted_mask & (uint64_t{1} << i)) != 0) {
            if (owner.storage_->tables[i] == nullptr) __builtin_trap();
            owner.storage_->tables[i] = nullptr;
        }
}
// BEGIN whole-batch managed owners. All output rows are caller-born here.
PreparedHugeMapBatch::~PreparedHugeMapBatch() { reset(); }
PreparedHugeMapBatch::PreparedHugeMapBatch(PreparedHugeMapBatch&& other) noexcept
    : cleanup_(std::move(other.cleanup_)), parents_(std::move(other.parents_)), storage_(std::exchange(other.storage_, nullptr)) {}
auto PreparedHugeMapBatch::operator=(PreparedHugeMapBatch&& other) noexcept -> PreparedHugeMapBatch& {
    if (this != &other) {
        reset();
        cleanup_ = std::move(other.cleanup_);
        parents_ = std::move(other.parents_);
        storage_ = std::exchange(other.storage_, nullptr);
    }
    return *this;
}
void PreparedHugeMapBatch::reset() noexcept {
    if (storage_ != nullptr) {
        if (storage_->published && !storage_->acknowledged) __builtin_trap();
        for (uint16_t i = 0; i < storage_->count; ++i) storage_->leaves[i].span.reset();
        for (uint16_t i = 0; i < storage_->detached_count; ++i) detail::release_table(storage_->detached[i]);
        delete std::exchange(storage_, nullptr);
    }
    // The batch heap and every actual data/old-PT owner precede plan credits.
    cleanup_.reset();
    parents_.reset();
}
auto PreparedHugeMapBatch::prepare(RootKey root, uint64_t start, uint64_t length, uint64_t flags,
                                   const numa::AllocationContext& context) noexcept -> Error {
    constexpr uint64_t LIMIT = uint64_t{1} << 47;
    constexpr uint64_t PERMISSIONS = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_NX;
    if (!empty() || !cleanup_.empty() || !parents_.empty() || !RegistryCore::valid_root(root) || !RegistryCore::valid_va(start) ||
        length == 0 || (length & (LEAF_BYTES - 1)) != 0 || start >= LIMIT || length > LIMIT - start ||
        (flags & paging::PAGE_PRESENT) == 0 || (flags & ~PERMISSIONS) != 0 || context.virtual_page_index != 0)
        return Error::Invalid;
    if (length / LEAF_BYTES > MAX_ROOT_LEAVES) return Error::Capacity;
    auto const count = static_cast<uint16_t>(length / LEAF_BYTES);
    if (auto error = cleanup_.prepare(count); error != Error::None) return error;
    if (auto error = parents_.prepare(4); error != Error::None) {
        reset();
        return error;
    }
    storage_ = new (std::nothrow) detail::HugeMapBatchStorage{};
    if (storage_ == nullptr) {
        reset();
        return Error::Exhausted;
    }
    storage_->root = root;
    storage_->start = start;
    storage_->flags = flags;
    storage_->count = count;
    constexpr uint64_t HUGE_FRAME = uint64_t{0x000ffffffffff000} & ~(LEAF_BYTES - 1);
    for (uint16_t i = 0; i < count; ++i) {
        auto& row = storage_->leaves[i];
        uint64_t const va = start + static_cast<uint64_t>(i) * LEAF_BYTES;
        auto error = row.span.prepare(root, va, context);
        if (error == Error::None && !detail::PreparationAccess::span_view(row.span, row.view)) error = Error::Invalid;
        if (error == Error::None && (row.view.physical_2m & ~HUGE_FRAME) != 0) error = Error::Invalid;
        if (error != Error::None) {
            reset();
            return error;
        }
        row.key = {root, va, row.view.physical_2m};
        // Sorted owned physical spans: equality is a duplicate actual owner.
        uint16_t position = 0;
        while (position < i && storage_->span_physical[position] < row.view.physical_2m) ++position;
        if (position < i && storage_->span_physical[position] == row.view.physical_2m) {
            reset();
            return Error::Invalid;
        }
        for (uint16_t j = i; j > position; --j) storage_->span_physical[j] = storage_->span_physical[j - 1];
        storage_->span_physical[position] = row.view.physical_2m;
    }
    return Error::None;
}
auto detail::HugeMapBatchAccess::storage(PreparedHugeMapBatch& batch) noexcept -> HugeMapBatchStorage* { return batch.storage_; }
auto detail::HugeMapBatchAccess::parents(PreparedHugeMapBatch& batch) noexcept -> SplitPlanStorage* {
    return PreparationAccess::plan_storage(batch.parents_);
}
auto detail::HugeMapBatchAccess::cleanup(PreparedHugeMapBatch& batch) noexcept -> SplitPlanStorage* {
    return PreparationAccess::plan_storage(batch.cleanup_);
}
void detail::HugeMapBatchAccess::transfer_after_publish(PreparedHugeMapBatch& batch) noexcept {
    if (batch.storage_ == nullptr || !batch.storage_->published || batch.storage_->acknowledged) __builtin_trap();
    for (uint16_t i = 0; i < batch.storage_->count; ++i) PreparationAccess::span_transferred_after_publish(batch.storage_->leaves[i].span);
    PreparationAccess::tables_transferred_after_publish(batch.parents_, batch.storage_->adopted_mask);
}
// END whole-batch managed owners.
namespace {
auto range_current_context(sched::task::Task& target, const sys::SyscallPreparationScope& preparation, bool interrupts) noexcept -> bool {
    return preparation.ready() && sched::can_query_current_task() && sched::get_current_task() == &target &&
           target.type == sched::task::TaskType::PROCESS &&
           target.state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE &&
           !target.scheduler_transition_active.load(std::memory_order_acquire) &&
           !target.exit_in_progress.load(std::memory_order_acquire) && target.syscall_account_start_us != 0 &&
           sched::preempt_count() == 0 && sched::interrupts_enabled() == interrupts;
}
// Called only after each chunk has retired root/PUB/preempt owners.
void record_range_prepare_failure(RangePrepareWitness* witness, Error error, RangePrepareSite site, RangePreparePass pass, uint8_t attempt,
                                  const RangeSplitCursor* cursor = nullptr, uint16_t expected_count = 0) noexcept {
    if (witness == nullptr) return;
    *witness = {cursor != nullptr ? cursor->next : 0,
                cursor != nullptr ? cursor->count : uint16_t{0},
                expected_count,
                error,
                site,
                pass,
                attempt,
                true,
                cursor != nullptr && cursor->epoch_changed,
                false};
}
auto range_tables_valid(const SplitPlanStorage& storage, uint16_t count) noexcept -> bool {
    constexpr uint64_t FRAME = 0x000ffffffffff000ULL;
    if (storage.table_count != count || count == 0 || count > MAX_PLAN_TABLES) return false;
    for (uint16_t i = 0; i < count; ++i) {
        auto* table = storage.tables[i];
        if (table == nullptr || (reinterpret_cast<uint64_t>(table) & (PAGE_BYTES - 1)) != 0 ||
            phys::page_kind_get(table) != PageKind::PAGE_TABLE || phys::page_ref_get(table) != 1 ||
            (storage.table_physical[i] & ~FRAME) != 0 ||
            storage.table_physical[i] != reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(table))))
            return false;
        for (uint16_t j = 0; j < i; ++j)
            if (table == storage.tables[j]) return false;
    }
    return true;
}
}  // namespace
struct detail::RangePreparationDriver {
    static auto pass(sched::task::Task* target, const sys::SyscallPreparationScope* preparation, paging::PageTable* unpublished,
                     RangeSplitCursor& cursor, RangeSurveyMode mode, SplitPlanStorage* storage, RangePrepareWitness* witness,
                     RangePreparePass phase, uint8_t attempt, uint16_t expected_count = 0) noexcept -> Error {
        while (!cursor.complete) {
            if (target != nullptr && (preparation == nullptr || !range_current_context(*target, *preparation, true))) {
                record_range_prepare_failure(witness, Error::Invalid, RangePrepareSite::Context, phase, attempt, &cursor, expected_count);
                return Error::Invalid;
            }
            CurrentRangeRootAccess access;
            RangePrepareSite site = RangePrepareSite::PublicationBusy;
            Error error = Error::Again;
            {
                sched::PreemptGuard preempt;
                ker::syscall::vmem::SharedVmemPublicationTryGuard publication;
                if (publication.acquired()) {
                    site = RangePrepareSite::Unreported;
                    if (target == nullptr)
                        error = survey_range_chunk(unpublished, cursor, mode, storage, witness != nullptr ? &site : nullptr);
                    else {
                        error = acquire_current_range_root(*target, access, witness != nullptr ? &site : nullptr);
                        if (error == Error::None)
                            error = survey_range_chunk(access.root, cursor, mode, storage, witness != nullptr ? &site : nullptr);
                        // Even a post-acquire identity refusal owns the local lease.
                        release_current_range_root(access);
                    }
                }
            }
            // No actual Task/control/PT/PTE/root-reader/PUB/preempt owner remains.
            if (error != Error::None) {
                record_range_prepare_failure(witness, error, site, phase, attempt, &cursor, expected_count);
                return error;
            }
            if (!cursor.complete && target != nullptr) sched::kern_yield();
        }
        return Error::None;
    }
    static auto prepare(sched::task::Task* target, const sys::SyscallPreparationScope* preparation, paging::PageTable* unpublished,
                        uint64_t start, uint64_t end, RangeSplitSurvey& survey, PreparedTables& tables, uint16_t fail_before_table,
                        RangePrepareWitness* witness = nullptr) noexcept -> Error {
        constexpr uint64_t LIMIT = uint64_t{1} << 47;
        if (witness != nullptr) *witness = {};
        if (start >= end || end > LIMIT || ((start | end) & (PAGE_BYTES - 1)) != 0 || survey.complete_ ||
            survey.stamp_.root.incarnation != 0 || !tables.empty() || PreparationAccess::plan_storage(tables) != nullptr) {
            record_range_prepare_failure(witness, Error::Invalid, RangePrepareSite::Input, RangePreparePass::Entry, 0);
            return Error::Invalid;
        }
        for (unsigned attempt = 0; attempt < 3; ++attempt) {
            if (witness != nullptr) *witness = {};
            RangeSplitCursor count{};
            count.start = start;
            count.end = end;
            Error error = pass(target, preparation, unpublished, count, RangeSurveyMode::Count, nullptr, witness, RangePreparePass::Count,
                               static_cast<uint8_t>(attempt + 1));
            if (error != Error::None) {
                if (error == Error::Again && count.epoch_changed) continue;
                return error;  // Busy is immediate; no hidden contention retries.
            }
            if (count.count != 0) {
                RangePrepareSite site = RangePrepareSite::Unreported;
#ifdef WOS_SELFTEST
                error = PreparationAccess::prepare_tables_for_test(tables, count.count, fail_before_table,
                                                                   witness != nullptr ? &site : nullptr);
#else
                (void)fail_before_table;
                error = tables.prepare(count.count, witness != nullptr ? &site : nullptr);
#endif
                if (error != Error::None) {
                    record_range_prepare_failure(witness, error, site, RangePreparePass::Tables, static_cast<uint8_t>(attempt + 1), &count,
                                                 count.count);
                    return error;
                }
            }
            auto* storage = PreparationAccess::plan_storage(tables);
            if (count.count != 0 && (storage == nullptr || !range_tables_valid(*storage, count.count))) {
                tables.reset();
                record_range_prepare_failure(witness, Error::Invalid, RangePrepareSite::TableValidation, RangePreparePass::Tables,
                                             static_cast<uint8_t>(attempt + 1), &count, count.count);
                return Error::Invalid;
            }
            RangeSplitCursor fill{};
            fill.stamp = count.stamp;
            fill.start = start;
            fill.end = end;
            error = pass(target, preparation, unpublished, fill, count.count != 0 ? RangeSurveyMode::Fill : RangeSurveyMode::Count, storage,
                         witness, RangePreparePass::Fill, static_cast<uint8_t>(attempt + 1), count.count);
            if (error == Error::None && fill.count != count.count) {
                error = Error::Again;
                record_range_prepare_failure(witness, error, RangePrepareSite::CountMismatch, RangePreparePass::Fill,
                                             static_cast<uint8_t>(attempt + 1), &fill, count.count);
            }
            if (error == Error::None && storage != nullptr) {
                constexpr uint64_t PERMISSIONS = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_NX;
                constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
                for (uint16_t i = 0; i < count.count; ++i) {
                    auto const& descriptor = storage->descriptors[i];
                    auto* words = reinterpret_cast<uint64_t*>(storage->tables[i]);
                    for (uint64_t page = 0; page < LEAF_PAGES; ++page)
                        words[page] = (descriptor.key.pa_2m + page * PAGE_BYTES) | (descriptor.semantic_word & PERMISSIONS) | AD;
                }
            }
            // An independent complete final pass is required even for count0.
            RangeSplitCursor final{};
            final.stamp = count.stamp;
            final.start = start;
            final.end = end;
            if (error == Error::None)
                error = pass(target, preparation, unpublished, final, count.count != 0 ? RangeSurveyMode::Verify : RangeSurveyMode::Count,
                             storage, witness, RangePreparePass::Verify, static_cast<uint8_t>(attempt + 1), count.count);
            if (error == Error::None && final.count != count.count) {
                error = Error::Again;
                record_range_prepare_failure(witness, error, RangePrepareSite::CountMismatch, RangePreparePass::Verify,
                                             static_cast<uint8_t>(attempt + 1), &final, count.count);
            }
            if (error != Error::None) {
                tables.reset();  // All chunk guards ended before actual owner resets.
                if (error == Error::Again && (fill.epoch_changed || final.epoch_changed)) continue;
                return error;
            }
            if (storage != nullptr) {
                storage->root = count.stamp.root;
                storage->geometry_epoch = count.stamp.epoch;
                storage->start = start;
                storage->end = end;
                storage->descriptor_count = count.count;
                storage->purpose = PlanPurpose::Split;
                storage->surveyed = true;
            }
            survey.stamp_ = count.stamp;
            survey.start_ = start;
            survey.end_ = end;
            survey.count_ = count.count;
            survey.complete_ = true;
            if (witness != nullptr) *witness = {};
            return Error::None;
        }
        if (witness != nullptr && witness->valid) witness->exhausted = true;
        return Error::Again;
    }
};
auto detail::current_fork_context_valid(sched::task::Task& target, const sys::SyscallPreparationScope& preparation,
                                        const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept -> bool {
    return publication.acquired() && range_current_context(target, preparation, false);
}
auto prepare_current_range_split(sched::task::Task& target, const sys::SyscallPreparationScope& preparation, uint64_t start, uint64_t end,
                                 RangeSplitSurvey& survey, PreparedTables& tables, RangePrepareWitness* witness) noexcept -> Error {
    if (witness != nullptr) *witness = {};
    if (!range_current_context(target, preparation, true)) {
        record_range_prepare_failure(witness, Error::Invalid, RangePrepareSite::Context, RangePreparePass::Entry, 0);
        return Error::Invalid;
    }
    return detail::RangePreparationDriver::prepare(&target, &preparation, nullptr, start, end, survey, tables, UINT16_MAX, witness);
}
auto detail::prepare_unpublished_range_split(paging::PageTable* root, uint64_t start, uint64_t end, RangeSplitSurvey& survey,
                                             PreparedTables& tables, uint16_t fail_before_table) noexcept -> Error {
    if (root == nullptr) return Error::Invalid;
    return RangePreparationDriver::prepare(nullptr, nullptr, root, start, end, survey, tables, fail_before_table);
}
CurrentRangeSplitAdmission::CurrentRangeSplitAdmission(sched::task::Task& target, const sys::SyscallPreparationScope& preparation,
                                                       const ker::syscall::vmem::SharedVmemPublicationTryGuard& publication) noexcept
    : subject_(&target), preparation_(&preparation), publication_(&publication) {
    if (!publication.acquired() || !range_current_context(target, preparation, false)) return;
    detail::CurrentRangeRootAccess access;
    auto const error = detail::acquire_current_range_root(target, access);
    // Transfer both successful and partially acquired lifetimes immediately.
    local_ = access.local;
    gate_ = access.gate;
    if (error == Error::None) root_ = access.root;
}
CurrentRangeSplitAdmission::CurrentRangeSplitAdmission(sched::task::Task& target, const sys::SyscallPreparationScope& preparation,
                                                       const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept
    : subject_(&target), preparation_(&preparation), blocking_publication_(&publication) {
    if (!publication.acquired() || !range_current_context(target, preparation, false)) return;
    detail::CurrentRangeRootAccess access;
    auto const error = detail::acquire_current_range_root(target, access);
    local_ = access.local;
    gate_ = access.gate;
    if (error == Error::None) root_ = access.root;
}
auto CurrentRangeSplitAdmission::publication_acquired() const noexcept -> bool {
    if (publication_ != nullptr) return blocking_publication_ == nullptr && publication_->acquired();
    return blocking_publication_ != nullptr && blocking_publication_->acquired();
}
CurrentRangeSplitAdmission::~CurrentRangeSplitAdmission() noexcept {
    detail::CurrentRangeRootAccess access{subject_, root_, gate_, local_};
    detail::release_current_range_root(access);
}
auto CurrentRangeSplitAdmission::validate_split_and_arm(const RangeSplitSurvey& survey, PreparedTables& tables) noexcept -> Error {
    if (!acquired() || armed_ || !publication_acquired() || !range_current_context(*subject_, *preparation_, false)) return Error::Invalid;
    auto const error = detail::commit_range_split(root_, survey, tables);
    if (error != Error::None) return error;
    start_ = survey.start();
    end_ = survey.end();
    armed_ = true;
    return Error::None;
}
auto CurrentRangeSplitAdmission::allows(const sched::task::Task& target, uint64_t start, uint64_t end) const noexcept -> bool {
    return armed_ && subject_ == &target && root_ != nullptr && root_ == target.pagemap && start_ == start && end_ == end &&
           publication_acquired() && range_current_context(*subject_, *preparation_, false);
}

// BEGIN current huge-map admission. Named access/key/status storage is born in
// this instrumented constructor before any excluded adapter writes into it.
CurrentHugeMapAdmission::CurrentHugeMapAdmission(sched::task::Task& subject, const sys::SyscallPreparationScope& preparation,
                                                 const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept
    : subject_(&subject), preparation_(&preparation), publication_(&publication) {
    if (!context_valid()) return;
    access_.subject = &subject;
    access_.local = subject.try_acquire_usercopy_pagemap(access_.root);
    if (!access_.local) {
        status_ = Error::Again;
        return;
    }
    // Successful local access is recorded even if a later root/key check fails.
    // Deliberately never acquire a normal striped reader in this admission.
    if (access_.root == nullptr || access_.root == virt::get_kernel_pagemap() || access_.root != subject.pagemap) {
        status_ = Error::Stale;
        return;
    }
    status_ = detail::root_key(access_.root, key_);
    if (status_ == Error::None && (!RegistryCore::valid_root(key_) || key_.address_bits != reinterpret_cast<uint64_t>(access_.root)))
        status_ = Error::Stale;
    if (status_ == Error::None && !context_valid()) status_ = Error::Again;
}
CurrentHugeMapAdmission::~CurrentHugeMapAdmission() noexcept { detail::release_current_range_root(access_); }
auto CurrentHugeMapAdmission::status() const noexcept -> Error { return status_; }
auto CurrentHugeMapAdmission::root() const noexcept -> paging::PageTable* { return status_ == Error::None ? access_.root : nullptr; }
auto CurrentHugeMapAdmission::key() const noexcept -> const RootKey& { return key_; }
auto CurrentHugeMapAdmission::context_valid() const noexcept -> bool {
    return subject_ != nullptr && preparation_ != nullptr && publication_ != nullptr && publication_->acquired() &&
           range_current_context(*subject_, *preparation_, true) && !subject_->usercopy_pagemap_closing.load(std::memory_order_acquire);
}
auto CurrentHugeMapAdmission::publish(PreparedHugeMapBatch& batch) noexcept -> Error {
    if (status_ != Error::None) return status_;
    if (!context_valid()) return Error::Invalid;
    if (access_.subject != subject_ || !access_.local || access_.gate != static_cast<size_t>(-1) || access_.root == nullptr ||
        access_.root == virt::get_kernel_pagemap() || access_.root != subject_->pagemap)
        return Error::Stale;
    RootKey current{};
    if (auto const error = detail::root_key(access_.root, current); error != Error::None) return error;
    if (!RegistryCore::valid_root(key_) || !RegistryCore::same_root(current, key_)) return Error::Stale;
    auto const* storage = detail::HugeMapBatchAccess::storage(batch);
    if (storage == nullptr || !RegistryCore::same_root(storage->root, key_)) return Error::Invalid;
    // All new refusal checks precede the unchanged original exclusive/mapper.
    // Caller has ended every separate normal reader/split admission already.
    return detail::publish_prepared_huge_batch(access_.root, *publication_, batch);
}
// END current huge-map admission.

}  // namespace ker::mod::mm::huge

namespace ker::mod::mm::virt {
PreparedAnonymousSwapFork::PreparedAnonymousSwapFork() noexcept = default;
PreparedAnonymousSwapFork::~PreparedAnonymousSwapFork() { detail::reset_anonymous_swap_fork(*this); }
auto PreparedAnonymousSwapFork::prepare(sched::task::Task& actor, sched::task::Task& child, const sys::SyscallPreparationScope& preparation)
    -> int {
    return detail::prepare_anonymous_swap_fork(actor, child, preparation, *this);
}
namespace {
auto deep_copy_current_user_pagemap_prepared_impl(sched::task::Task& actor, PageTable* child,
                                                  const sys::SyscallPreparationScope& preparation,
                                                  const ker::syscall::vmem::SharedVmemPublicationGuard& publication,
                                                  const huge::RangeSplitSurvey& survey, huge::PreparedTables& tables,
                                                  numa::AllocationContext* eager_context, numa::AllocationResult* empty_result,
                                                  PreparedAnonymousSwapFork* prepared_swap) noexcept -> int {
    if (!huge::detail::current_fork_context_valid(actor, preparation, publication)) return -EWOULDBLOCK;
    constexpr uint64_t USER_END = uint64_t{1} << 47;
    if (child == nullptr || !survey.complete() || survey.start() != 0 || survey.end() != USER_END) return -EINVAL;
    // Instrumented caller-born storage: Task writes its result into this actual
    // addressable output. Deliberately do not acquire a striped reader here.
    huge::detail::CurrentRangeRootAccess access;
    access.subject = &actor;
    access.local = actor.try_acquire_usercopy_pagemap(access.root);
    int result = -EAGAIN;
    if (access.local && access.root != nullptr && access.root == actor.pagemap)
        result = huge::detail::copy_current_user_pagemap_exclusive(actor, access.root, child, preparation, publication, survey, tables,
                                                                   eager_context, empty_result, prepared_swap);
    // Record/release even a successful local lease followed by identity refusal.
    huge::detail::release_current_range_root(access);
    return result;
}
}  // namespace

auto deep_copy_current_user_pagemap_prepared(sched::task::Task& actor, PageTable* child, const sys::SyscallPreparationScope& preparation,
                                             const ker::syscall::vmem::SharedVmemPublicationGuard& publication,
                                             const huge::RangeSplitSurvey& survey, huge::PreparedTables& tables,
                                             numa::AllocationContext* eager_context, numa::AllocationResult* empty_result) noexcept -> int {
    return deep_copy_current_user_pagemap_prepared_impl(actor, child, preparation, publication, survey, tables, eager_context, empty_result,
                                                        nullptr);
}
auto deep_copy_current_user_pagemap_prepared(sched::task::Task& actor, PageTable* child, const sys::SyscallPreparationScope& preparation,
                                             const ker::syscall::vmem::SharedVmemPublicationGuard& publication,
                                             const huge::RangeSplitSurvey& survey, huge::PreparedTables& tables,
                                             numa::AllocationContext* eager_context, numa::AllocationResult* empty_result,
                                             PreparedAnonymousSwapFork& swap) noexcept -> int {
    return deep_copy_current_user_pagemap_prepared_impl(actor, child, preparation, publication, survey, tables, eager_context, empty_result,
                                                        &swap);
}

}  // namespace ker::mod::mm::virt
