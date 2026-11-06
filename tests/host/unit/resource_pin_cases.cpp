#include <gtest/gtest.h>

#include <barrier>
#include <net/wki/resource_contract_model.hpp>
#include <platform/sched/resource.hpp>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "resource_pin_ports.inc"

namespace {
namespace resource = ker::mod::sched::resource;
namespace wki = ker::net::wki;
using Task = ker::mod::sched::task::Task;
using Charge = resource::PinCharge;
constexpr unsigned MEMLOCK = static_cast<unsigned>(resource::Id::MEMLOCK);
constexpr uint64_t PAGE = 4096;
static_assert(MEMLOCK == 8 && ker::abi::resource::COUNT == 16 && ker::abi::resource::VERSION == 1);
static_assert(!std::is_copy_constructible_v<Charge> && std::is_nothrow_move_constructible_v<Charge>);
static_assert(!std::is_move_constructible_v<resource::PinAccountOwner>);

void process(Task& task, uint64_t pid = 77) {
    task.type = ker::mod::sched::task::TaskType::PROCESS;
    task.pid = pid;
}
auto usage(Task& task) -> resource::Usage {
    auto const flags = task.resource_lock.lock_irqsave();
    resource::refresh_pin_usage_locked(task);
    auto const result = task.resources.entries[MEMLOCK];
    task.resource_lock.unlock_irqrestore(flags);
    return result;
}
void coherent() {
    auto const s = resource::pin_accounting_snapshot();
    EXPECT_EQ(s.free_accounts + s.open_accounts + s.closed_accounts + s.exhausted_accounts, s.capacity);
    EXPECT_LE(s.closed_bytes, s.current_bytes);
    EXPECT_LE(s.current_bytes, s.peak_bytes);
}
void drained() {
    auto const s = resource::pin_accounting_snapshot();
    EXPECT_EQ(s.current_bytes, 0U);
    EXPECT_EQ(s.closed_bytes, 0U);
    EXPECT_EQ(s.charges, 0U);
    EXPECT_EQ(s.open_accounts, 0U);
    EXPECT_EQ(s.closed_accounts, 0U);
    coherent();
    EXPECT_EQ(pin_host::depth, 0U);
    EXPECT_EQ(pin_host::publication_depth, 0U);
}

TEST(PinAccounting, IssuedMovesAndRefundPreserveExactOwnership) {
    drained();
    {
        Task p;
        process(p);
        Charge first, second;
        EXPECT_TRUE(first.closed());
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, first), 0);
        ASSERT_EQ(resource::reserve_pin_charge(p, 2 * PAGE, second), 0);
        EXPECT_FALSE(first.closed());
        EXPECT_EQ(usage(p).current, 3 * PAGE);
        Charge moved(std::move(first));
        EXPECT_FALSE(first);
        EXPECT_TRUE(first.closed());
        EXPECT_EQ(moved.bytes(), PAGE);
        second = std::move(moved);
        EXPECT_FALSE(moved);
        EXPECT_EQ(second.bytes(), PAGE);
        EXPECT_EQ(usage(p).current, PAGE);
        second = std::move(second);
        EXPECT_EQ(second.bytes(), PAGE);
        second.reset();
        second.reset();
        EXPECT_EQ(usage(p).current, 0U);
        EXPECT_EQ(usage(p).peak, 3 * PAGE);
        EXPECT_EQ(p.ref_count.load(), 1U);
    }
    drained();
}

TEST(PinAccounting, InvalidRequestsCannotConsumePolicyOrMintCharges) {
    drained();
    {
        Task p;
        process(p);
        Charge charge;
        for (uint64_t bytes : {uint64_t{0}, uint64_t{1}, PAGE - 1, PAGE + 1, UINT64_MAX}) {
            EXPECT_EQ(resource::reserve_pin_charge(p, bytes, charge), -EINVAL);
            EXPECT_FALSE(charge);
        }
        EXPECT_EQ(usage(p).denied, 0U);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, charge), 0);
        EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, charge), -EINVAL);
        EXPECT_EQ(charge.bytes(), PAGE);
        EXPECT_EQ(usage(p).current, PAGE);
        EXPECT_EQ(usage(p).denied, 0U);
    }
    drained();
}

TEST(PinAccounting, CanonicalLimitsAndHistoricalDenialsSurviveIndependentRefunds) {
    drained();
    {
        Task p;
        process(p);
        auto& entry = p.resources.entries[MEMLOCK];
        entry.peak = 20 * PAGE;
        entry.denied = 9;
        entry.reason = resource::Denial::HARD_LIMIT;
        ASSERT_EQ(p.resources.set(MEMLOCK, {3 * PAGE, 5 * PAGE}, true, 256), 0);
        Charge a, b, denied;
        ASSERT_EQ(resource::reserve_pin_charge(p, 2 * PAGE, a), 0);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, b), 0);
        EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, denied), -ENOMEM);
        EXPECT_FALSE(denied);
        ASSERT_EQ(p.resources.set(MEMLOCK, {PAGE, 5 * PAGE}, false, 256), 0);
        EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, denied), -ENOMEM);
        a.reset();
        EXPECT_EQ(usage(p).current, PAGE);
        EXPECT_EQ(usage(p).peak, 20 * PAGE);
        EXPECT_EQ(usage(p).denied, 11U);
        EXPECT_EQ(usage(p).reason, resource::Denial::SOFT_LIMIT);
        b.reset();
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, denied), 0);
        EXPECT_EQ(usage(p).current, PAGE);
    }
    drained();
}

TEST(PinAccounting, RealOwnerBindingRejectsNumericAndStaleThreadFallbacks) {
    drained();
    {
        Task p, thread, unbound, daemon;
        process(p);
        process(thread, 78);
        process(unbound, 79);
        thread.is_thread = unbound.is_thread = true;
        thread.owner_pid = unbound.owner_pid = p.pid;
        Charge charge;
        EXPECT_EQ(resource::reserve_pin_charge(daemon, PAGE, charge), -ESRCH);
        EXPECT_EQ(resource::reserve_pin_charge(unbound, PAGE, charge), -ESRCH);
        ASSERT_TRUE(resource::bind_thread_owner(thread, p));
        thread.resources.entries[MEMLOCK].limit = {0, 0};
        ASSERT_EQ(p.resources.set(MEMLOCK, {PAGE, PAGE}, true, 256), 0);
        thread.owner_pid = 999;
        ASSERT_EQ(resource::reserve_pin_charge(thread, PAGE, charge), 0);
        EXPECT_EQ(usage(p).current, PAGE);
        EXPECT_EQ(thread.resources.entries[MEMLOCK].current, 0U);
        EXPECT_EQ(p.ref_count.load(), 2U);
        charge.reset();
        for (auto state : {ker::mod::sched::task::TaskState::EXITING, ker::mod::sched::task::TaskState::DEAD}) {
            p.state.store(state);
            EXPECT_EQ(resource::reserve_pin_charge(thread, PAGE, charge), -ESRCH);
            EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, charge), -ESRCH);
            EXPECT_FALSE(charge);
        }
        p.state.store(ker::mod::sched::task::TaskState::ACTIVE);
        p.exit_in_progress.store(true);
        EXPECT_EQ(resource::reserve_pin_charge(thread, PAGE, charge), -ESRCH);
        p.exit_in_progress.store(false);
    }
    drained();
}

TEST(PinAccounting, ForkPolicyAndExecBarrierDoNotCopyOrEraseLiveCharges) {
    drained();
    {
        Task p, child, thread;
        process(p);
        process(child, 78);
        process(thread, 79);
        thread.is_thread = true;
        thread.owner_pid = p.pid;
        ASSERT_TRUE(resource::bind_thread_owner(thread, p));
        ASSERT_EQ(p.resources.set(MEMLOCK, {4 * PAGE, 8 * PAGE}, true, 256), 0);
        Charge parent_charge, child_charge, after_exec;
        ASSERT_EQ(resource::reserve_pin_charge(thread, 2 * PAGE, parent_charge), 0);
        ASSERT_TRUE(resource::inherit(child, thread));
        EXPECT_EQ(usage(child).current, 0U);
        EXPECT_EQ(usage(child).limit.soft, 4 * PAGE);
        ASSERT_EQ(resource::reserve_pin_charge(child, PAGE, child_charge), 0);
        EXPECT_EQ(usage(p).current, 2 * PAGE);
        {
            resource::ExecAdmission exec;
            ASSERT_TRUE(exec.begin(thread));
            EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, after_exec), -ESRCH);
            EXPECT_FALSE(parent_charge.closed());
            EXPECT_EQ(usage(p).current, 2 * PAGE);
        }
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, after_exec), 0);
        thread.resource_owner.finish_cpu();
        EXPECT_FALSE(parent_charge.closed());
        EXPECT_EQ(usage(p).current, 3 * PAGE);
    }
    drained();
}

TEST(PinAccounting, WholeProcessClosePublishesToRetainedCharges) {
    drained();
    {
        Task p;
        process(p);
        Charge live, denied;
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, live), 0);
        ASSERT_TRUE(resource::close_thread_cpu_admission(p));
        EXPECT_TRUE(live.closed());
        EXPECT_EQ(resource::pin_accounting_snapshot().closed_bytes, PAGE);
        EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, denied), -ESRCH);
        EXPECT_FALSE(denied);
        EXPECT_EQ(usage(p).current, PAGE);
    }
    drained();
}

TEST(PinAccounting, CloseBeforeFirstReservationCannotCreateAnAccountLater) {
    drained();
    {
        Task p;
        process(p);
        auto const flags = p.resource_lock.lock_irqsave();
        resource::close_pin_account_locked(p);
        p.resource_lock.unlock_irqrestore(flags);
        Charge denied;
        EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, denied), -ESRCH);
        EXPECT_FALSE(denied);
    }
    drained();
}

TEST(PinAccounting, RefundReturnsActualGlobalCurrentAfterTaskDestruction) {
    drained();
    Charge orphan;
    {
        Task p;
        process(p);
        ASSERT_EQ(resource::reserve_pin_charge(p, 2 * PAGE, orphan), 0);
        EXPECT_EQ(p.ref_count.load(), 1U);
    }
    EXPECT_TRUE(orphan.closed());
    EXPECT_EQ(resource::pin_accounting_snapshot().closed_bytes, 2 * PAGE);
    EXPECT_EQ(resource::pin_accounting_snapshot().closed_accounts, 1U);
    orphan.reset();
    drained();
}

TEST(PinAccounting, ConcurrentAdmissionAndCloseNeverOversubscribeOrLoseRefunds) {
    drained();
    {
        Task p;
        process(p);
        ASSERT_EQ(p.resources.set(MEMLOCK, {4 * PAGE, 4 * PAGE}, true, 256), 0);
        std::barrier start(9);
        std::barrier halfway(9);
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < 8; ++i)
            threads.emplace_back([&] {
                start.arrive_and_wait();
                for (unsigned j = 0; j < 64; ++j) {
                    if (j == 32) halfway.arrive_and_wait();
                    Charge charge;
                    int const result = resource::reserve_pin_charge(p, PAGE, charge);
                    EXPECT_TRUE(result == 0 || result == -ENOMEM || result == -ESRCH);
                    EXPECT_EQ(static_cast<bool>(charge), result == 0);
                    EXPECT_LE(usage(p).current, 4 * PAGE);
                    std::this_thread::yield();
                }
            });
        start.arrive_and_wait();
        halfway.arrive_and_wait();
        ASSERT_TRUE(resource::close_thread_cpu_admission(p));
        for (auto& thread : threads) thread.join();
        EXPECT_EQ(usage(p).current, 0U);
        EXPECT_LE(usage(p).peak, 4 * PAGE);
        EXPECT_GT(usage(p).peak, 0U);
    }
    drained();
}

TEST(PinAccounting, LogicalOverflowRefusesWithoutOwningPhysicalPages) {
    drained();
    {
        Task p, other;
        process(p);
        process(other, 78);
        Charge huge, denied;
        constexpr uint64_t MAX_PAGES = UINT64_MAX & ~(PAGE - 1);
        ASSERT_EQ(resource::reserve_pin_charge(p, MAX_PAGES, huge), 0);
        EXPECT_EQ(resource::reserve_pin_charge(other, PAGE, denied), -EOVERFLOW);
        EXPECT_FALSE(denied);
        EXPECT_EQ(usage(other).current, 0U);
        EXPECT_EQ(usage(other).reason, resource::Denial::OVERFLOW);
        EXPECT_EQ(resource::pin_accounting_snapshot().current_bytes, MAX_PAGES);
    }
    drained();
}

TEST(PinAccounting, PoolCapacityRetainsOrphansThenReusesOnlyReturnedAccount) {
    drained();
    auto const capacity = resource::pin_accounting_snapshot().capacity;
    std::vector<Charge> held(capacity);
    for (unsigned i = 0; i < capacity; ++i) {
        Task p;
        process(p, 100 + i);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, held[i]), 0);
    }
    EXPECT_EQ(resource::pin_accounting_snapshot().closed_accounts, capacity);
    EXPECT_EQ(resource::pin_accounting_snapshot().closed_bytes, uint64_t{capacity} * PAGE);
    {
        Task full;
        process(full);
        Charge retry;
        EXPECT_EQ(resource::reserve_pin_charge(full, PAGE, retry), -ENOSPC);
        EXPECT_FALSE(retry);
        held.back().reset();
        ASSERT_EQ(resource::reserve_pin_charge(full, PAGE, retry), 0);
        EXPECT_FALSE(retry.closed());
    }
    held.clear();
    drained();
}

TEST(PinAccounting, SyscallUsageRefreshesRefundAndPreservesImportedHistory) {
    drained();
    {
        Task p;
        process(p);
        Charge a, b;
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, a), 0);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, b), 0);
        a.reset();
        resource::Usage observation;
        ASSERT_EQ(resource::limit_for_subject(&p, MEMLOCK, reinterpret_cast<uint64_t>(&observation), 1, false, true), 0U);
        EXPECT_EQ(observation.current, PAGE);
        EXPECT_EQ(observation.peak, 2 * PAGE);
        b.reset();
    }
    drained();
}

TEST(PinAccounting, ProcfsFreshSnapshotRefreshesWhileCachedViewStaysStable) {
    drained();
    {
        Task p;
        process(p);
        Charge a, b;
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, a), 0);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, b), 0);
        a.reset();
        pin_host::ProcFileData first;
        first.target = &p;
        std::array<char, 4096> text{};
        auto const count = pin_host::procfs_read_limits({}, p.pid, first, text.data(), text.size(), 0);
        ASSERT_GT(count, 0);
        std::string observed(text.data(), static_cast<size_t>(count));
        EXPECT_NE(observed.find("MEMLOCK infinity infinity 4096 8192 0 none bytes pending\n"), std::string::npos);
        b.reset();
        text.fill(0);
        EXPECT_EQ(pin_host::procfs_read_limits({}, p.pid, first, text.data(), text.size(), 0), count);
        EXPECT_EQ(std::string(text.data(), static_cast<size_t>(count)), observed);
        pin_host::ProcFileData fresh;
        fresh.target = &p;
        auto const refreshed = pin_host::procfs_read_limits({}, p.pid, fresh, text.data(), text.size(), 0);
        ASSERT_GT(refreshed, 0);
        EXPECT_NE(std::string(text.data(), static_cast<size_t>(refreshed)).find("MEMLOCK infinity infinity 0 8192 0 none bytes pending\n"),
                  std::string::npos);
    }
    drained();
}

TEST(PinAccounting, WkiSubmissionRefreshesRefundWithoutErasingLiteralHistory) {
    drained();
    {
        Task p;
        process(p);
        resource::State history;
        history.entries[MEMLOCK].peak = 9 * PAGE;
        history.entries[MEMLOCK].denied = 7;
        history.entries[MEMLOCK].reason = resource::Denial::SOFT_LIMIT;
        auto const imported = wki::resource_contract::snapshot(history);
        ASSERT_TRUE(wki::resource_contract::restore(imported, p.resources, 256));
        Charge a, b;
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, a), 0);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, b), 0);
        a.reset();
        wki::TaskResourceSnapshot result;
        ASSERT_TRUE(pin_host::snapshot_task_resources(&p, result, false));
        EXPECT_EQ(result.entries[MEMLOCK].current, PAGE);
        EXPECT_EQ(result.entries[MEMLOCK].peak, 9 * PAGE);
        EXPECT_EQ(result.entries[MEMLOCK].denied, 7U);
        EXPECT_EQ(result.entries[MEMLOCK].reason, resource::Denial::SOFT_LIMIT);
        resource::State receiver;
        ASSERT_TRUE(wki::resource_contract::restore(result, receiver, 256));
        EXPECT_EQ(receiver.entries[MEMLOCK].current, 0U);
        EXPECT_EQ(receiver.entries[MEMLOCK].peak, 9 * PAGE);
    }
    drained();
}

TEST(PinAccounting, WkiCompletionBranchRefreshesActualRetainedTaskUsage) {
    drained();
    {
        Task p;
        process(p);
        Charge a, b;
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, a), 0);
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, b), 0);
        a.reset();
        auto const result = pin_host::completion_resources(&p);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->entries[MEMLOCK].current, PAGE);
        EXPECT_EQ(result->entries[MEMLOCK].peak, 2 * PAGE);
    }
    drained();
}

#ifdef WOS_PIN_FINITE_GENERATION
TEST(PinAccounting, FiniteGenerationExhaustionNeverRecyclesOrWraps) {
    drained();
    auto const capacity = resource::pin_accounting_snapshot().capacity;
    for (unsigned i = 0; i < capacity * WOS_PIN_FINITE_GENERATION; ++i) {
        Task p;
        process(p);
        Charge charge;
        ASSERT_EQ(resource::reserve_pin_charge(p, PAGE, charge), 0);
    }
    auto const exhausted = resource::pin_accounting_snapshot();
    EXPECT_EQ(exhausted.exhausted_accounts, capacity);
    EXPECT_EQ(exhausted.free_accounts, 0U);
    Task p;
    process(p);
    Charge denied;
    EXPECT_EQ(resource::reserve_pin_charge(p, PAGE, denied), -EOVERFLOW);
    EXPECT_FALSE(denied);
    drained();
}
#endif
}  // namespace
