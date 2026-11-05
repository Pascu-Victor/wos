#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>

// Authorized Host-only fixture access for coherent reference saturation and
// misuse/death cases. No production setters or altered implementation bodies.
#define private public
#include <platform/mm/file_mapping_lifetime.hpp>
#undef private
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/paging.hpp>
#include <snapshot_fixture.hpp>

namespace fm = ker::mod::mm::file_mapping;
namespace fixture = snapshot_fixture;

namespace {
using State = fm::BindingRetirementState;
using Snapshot = fm::FileBindingSnapshot;
static_assert(!std::is_copy_constructible_v<Snapshot> && !std::is_copy_assignable_v<Snapshot>);
static_assert(std::is_nothrow_move_constructible_v<Snapshot> && std::is_nothrow_move_assignable_v<Snapshot>);

struct World {
    static constexpr uint64_t SERIAL = 17, START = 0x400000, PAGE = 4096;
    ker::vfs::File file;
    std::unique_ptr<ker::mod::mm::paging::PageTable> root{std::make_unique<ker::mod::mm::paging::PageTable>()};
    fm::FileMappingLifetime* family{fm::FileMappingLifetime::prepare(file)};
    fm::FileMappingIndex index;
    bool owns_file{true};
    ~World() {
        if (root) {
            fm::FileMappingTlbRetirement retirement;
            {
                fixture::IndexGuard guard;
                EXPECT_TRUE(index.detach_root_locked(root.get(), SERIAL, retirement));
            }
            retirement.release_after_completed_tlb();
        }
        if (family) family->release(fm::FamilyOwner::PREPARED);
        fixture::drain();
        if (owns_file) ker::vfs::vfs_put_file(&file);
    }
    auto add(size_t page = 0) -> fm::FileMappingBinding* {
        if (!family) return nullptr;
        auto* prepared =
            fm::FileMappingBinding::prepare(*family, root.get(), SERIAL, START + page * PAGE, START + (page + 1) * PAGE, page * PAGE);
        if (!prepared) return nullptr;
        auto* identity = prepared;
        bool attached = false;
        {
            fixture::IndexGuard guard;
            attached = index.attach_locked(prepared, SERIAL);
        }
        if (!attached) {
            prepared->release(fm::BindingOwner::PREPARED);
            return nullptr;
        }
        return identity;
    }
    auto capture(Snapshot& empty, size_t page = 0) -> bool {
        bool exact = false;
        fixture::IndexGuard guard;
        bool const captured = index.overlap_locked(root.get(), SERIAL, START + page * PAGE, START + (page + 1) * PAGE, empty, exact);
        EXPECT_TRUE(!captured || exact);
        return captured && !empty.empty();
    }
    void drop_family() { std::exchange(family, nullptr)->release(fm::FamilyOwner::PREPARED); }
    void drop_file() {
        owns_file = false;
        ker::vfs::vfs_put_file(&file);
    }
};

class BindingSnapshotTest : public ::testing::Test {
    void SetUp() override {
        ASSERT_TRUE(fixture::queue_empty());
        fixture::puts = fixture::closes = fixture::callbacks = 0;
        fixture::fail_reserve_after = -1;
        fixture::last_callback_result = 0;
    }
    void TearDown() override {
        fixture::drain();
        EXPECT_TRUE(fixture::queue_empty());
        EXPECT_EQ(fixture::lock_depth, 0U);
        EXPECT_EQ(fixture::index_depth, 0U);
    }
};

TEST_F(BindingSnapshotTest, EmptyResetAndMovesOwnNothing) {
    Snapshot empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.retirement_state(), State::EMPTY);
    empty.reset();
    Snapshot moved(std::move(empty));
    empty = std::move(moved);
    empty = std::move(empty);
    EXPECT_TRUE(empty.empty());
    EXPECT_TRUE(moved.empty());
    EXPECT_EQ(fixture::callbacks, 0U);
}

TEST_F(BindingSnapshotTest, CaptureAndMovesTransferOneActualSnapshotOwner) {
    World world;
    auto* binding = world.add();
    ASSERT_NE(binding, nullptr);
    Snapshot snapshot;
    ASSERT_TRUE(world.capture(snapshot));
    EXPECT_EQ(snapshot.retirement_state(), State::INDEXED);
    EXPECT_EQ(binding->owner_counts().count(fm::BindingOwner::SNAPSHOT), 1U);
    Snapshot moved(std::move(snapshot));
    EXPECT_TRUE(snapshot.empty());
    EXPECT_EQ(moved.retirement_state(), State::INDEXED);
    snapshot = std::move(moved);
    snapshot = std::move(snapshot);
    EXPECT_TRUE(moved.empty());
    EXPECT_EQ(binding->owner_counts().total, 2U);
    snapshot.reset();
    snapshot.reset();
    EXPECT_EQ(binding->owner_counts().total, 1U);
    EXPECT_EQ(fixture::callbacks, 0U);
}

TEST_F(BindingSnapshotTest, MoveAssignmentReleasesTheDestinationWithoutDuplicatingTheSource) {
    World world;
    auto* first = world.add();
    auto* second = world.add(1);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    Snapshot destination, source;
    ASSERT_TRUE(world.capture(destination));
    ASSERT_TRUE(world.capture(source, 1));
    destination = std::move(source);
    EXPECT_TRUE(source.empty());
    EXPECT_EQ(destination.binding_, second);
    EXPECT_EQ(first->owner_counts().total, 1U);
    EXPECT_EQ(second->owner_counts().total, 2U);
    destination.reset();
    EXPECT_EQ(second->owner_counts().total, 1U);
}

TEST_F(BindingSnapshotTest, DetachIsNotAckAndAckIsNotDestructiveCallbackReturn) {
    World world;
    auto* binding = world.add();
    ASSERT_NE(binding, nullptr);
    Snapshot snapshot;
    ASSERT_TRUE(world.capture(snapshot));
    world.file.close_result = -EIO;
    world.drop_family();
    world.drop_file();
    fm::FileMappingTlbRetirement retirement;
    {
        fixture::IndexGuard guard;
        ASSERT_TRUE(world.index.detach_root_locked(world.root.get(), World::SERIAL, retirement));
        EXPECT_TRUE(world.index.root_empty_locked(world.root.get()));
        EXPECT_EQ(snapshot.retirement_state(), State::TLB_PENDING);
        EXPECT_EQ(fixture::callbacks, 0U);
        EXPECT_EQ(fixture::closes, 0U);
    }
    EXPECT_FALSE(fixture::run_one());
    retirement.release_after_completed_tlb();  // Finite host ACK endpoint, not hardware evidence.
    world.root.reset();                        // Retained snapshot must never dereference the former root.
    EXPECT_EQ(snapshot.retirement_state(), State::TLB_COMPLETE);
    EXPECT_EQ(binding->owner_counts().total, 1U);
    EXPECT_FALSE(fixture::run_one());
    EXPECT_EQ(fixture::closes, 0U);
    snapshot.reset();
    EXPECT_TRUE(fixture::run_one());  // Actual binding finish queues family finish.
    EXPECT_EQ(fixture::callbacks, 1U);
    EXPECT_EQ(fixture::closes, 0U);
    EXPECT_TRUE(fixture::run_one());  // Actual family finish calls the finite VFS close.
    EXPECT_EQ(fixture::callbacks, 2U);
    EXPECT_EQ(fixture::closes, 1U);
    EXPECT_EQ(fixture::last_callback_result, -EIO);
    EXPECT_FALSE(fixture::run_one());  // Destructive close error is not retried.
}

TEST_F(BindingSnapshotTest, OccupiedOutputRefusalDoesNotResetOrReplaceAnOwner) {
    World world;
    auto* first = world.add();
    auto* second = world.add(1);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    Snapshot snapshot;
    ASSERT_TRUE(world.capture(snapshot));
    EXPECT_FALSE(world.capture(snapshot, 1));
    EXPECT_EQ(snapshot.binding_, first);
    EXPECT_EQ(first->owner_counts().total, 2U);
    EXPECT_EQ(second->owner_counts().total, 1U);
    bool exact = false;
    Snapshot absent;
    {
        fixture::IndexGuard guard;
        EXPECT_FALSE(
            world.index.overlap_locked(world.root.get(), World::SERIAL + 1, World::START, World::START + World::PAGE, absent, exact));
        EXPECT_FALSE(exact);
        EXPECT_TRUE(absent.empty());
        EXPECT_EQ(fixture::callbacks, 0U);
    }
}

TEST_F(BindingSnapshotTest, CapacityFailurePreservesOnlyTheActuallyAcquiredPrefix) {
    World world;
    ASSERT_NE(world.add(), nullptr);
    ASSERT_NE(world.add(1), nullptr);
    ASSERT_NE(world.add(2), nullptr);
    std::array<Snapshot, 2> snapshots;
    size_t count = 0;
    {
        fixture::IndexGuard guard;
        EXPECT_FALSE(world.index.snapshot_root_locked(world.root.get(), World::SERIAL, snapshots.data(), snapshots.size(), count));
        EXPECT_EQ(count, 2U);
        EXPECT_FALSE(snapshots[0].empty());
        EXPECT_FALSE(snapshots[1].empty());
        EXPECT_EQ(fixture::callbacks, 0U);
    }
    for (auto& snapshot : snapshots) snapshot.reset();
    EXPECT_EQ(fixture::callbacks, 0U);
}

TEST_F(BindingSnapshotTest, NonemptyArraySlotRefusesBeforeAcquiringAnyPrefix) {
    World world;
    auto* binding = world.add();
    ASSERT_NE(binding, nullptr);
    std::array<Snapshot, 2> snapshots;
    ASSERT_TRUE(world.capture(snapshots[1]));
    size_t count = 0;
    {
        fixture::IndexGuard guard;
        EXPECT_FALSE(world.index.snapshot_root_locked(world.root.get(), World::SERIAL, snapshots.data(), snapshots.size(), count));
        EXPECT_EQ(count, 0U);
        EXPECT_TRUE(snapshots[0].empty());
        EXPECT_EQ(snapshots[1].binding_, binding);
        EXPECT_EQ(binding->owner_counts().count(fm::BindingOwner::SNAPSHOT), 1U);
    }
}

TEST_F(BindingSnapshotTest, CoherentReferenceSaturationRefusesWithoutLosingPriorOwners) {
    World world;
    auto* saturated = world.add();
    auto* prefix = world.add(1);  // Intrusive index visits newest binding first.
    ASSERT_NE(saturated, nullptr);
    ASSERT_NE(prefix, nullptr);
    auto const saved = saturated->owners_;
    saturated->owners_.by_role.at(static_cast<size_t>(fm::BindingOwner::PIN)) = UINT64_MAX - saved.total;
    saturated->owners_.total = UINT64_MAX;
    std::array<Snapshot, 2> snapshots;
    size_t count = 0;
    {
        fixture::IndexGuard guard;
        EXPECT_FALSE(world.index.snapshot_root_locked(world.root.get(), World::SERIAL, snapshots.data(), snapshots.size(), count));
        EXPECT_EQ(count, 1U);
        EXPECT_EQ(snapshots[0].binding_, prefix);
        EXPECT_TRUE(snapshots[1].empty());
        Snapshot refused;
        bool exact = false;
        EXPECT_FALSE(world.index.overlap_locked(world.root.get(), World::SERIAL, World::START, World::START + World::PAGE, refused, exact));
        EXPECT_TRUE(refused.empty());
        EXPECT_EQ(saturated->owners_.total, UINT64_MAX);
    }
    saturated->owners_ = saved;  // Restore before any owner release/callback.
    snapshots[0].reset();
    EXPECT_EQ(prefix->owner_counts().total, 1U);
    EXPECT_EQ(saturated->owner_counts().total, 1U);
}

TEST_F(BindingSnapshotTest, BorrowedWrapperMisuseTrapsBeforeDroppingOwnership) {
    World world;
    auto* binding = world.add();
    ASSERT_NE(binding, nullptr);
    Snapshot snapshot;
    ASSERT_TRUE(world.capture(snapshot));
    snapshot.borrowed_ = true;  // Only the actual root lease may set this in production.
    EXPECT_DEATH(snapshot.reset(), "");
    EXPECT_DEATH({ Snapshot moved(std::move(snapshot)); }, "");
    EXPECT_DEATH(
        {
            Snapshot destination;
            destination = std::move(snapshot);
        },
        "");
    Snapshot other;
    ASSERT_TRUE(world.capture(other));
    EXPECT_DEATH(snapshot = std::move(other), "");
    EXPECT_FALSE(other.empty());
    snapshot.borrowed_ = false;
    EXPECT_EQ(binding->owner_counts().total, 3U);
    snapshot.reset();
    other.reset();
    EXPECT_EQ(binding->owner_counts().total, 1U);
}

TEST_F(BindingSnapshotTest, DistinctOwnedSnapshotsObserveActualRetirementConcurrently) {
    World world;
    ASSERT_NE(world.add(), nullptr);
    std::array<Snapshot, 4> snapshots;
    for (auto& snapshot : snapshots) ASSERT_TRUE(world.capture(snapshot));
    std::atomic<unsigned> indexed{0}, completed{0};
    std::atomic<bool> finish{false};
    std::array<std::thread, 4> observers;
    for (size_t i = 0; i < observers.size(); ++i) {
        observers[i] = std::thread([&, i] {
            EXPECT_EQ(snapshots[i].retirement_state(), State::INDEXED);
            indexed.fetch_add(1);
            while (!finish.load(std::memory_order_acquire)) std::this_thread::yield();
            if (snapshots[i].retirement_state() == State::TLB_COMPLETE)
                completed.fetch_add(1);
            else
                ADD_FAILURE() << "owned snapshot did not observe completed ACK publication";
        });
    }
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (indexed.load() != snapshots.size() && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    EXPECT_EQ(indexed.load(), snapshots.size());
    fm::FileMappingTlbRetirement retirement;
    {
        fixture::IndexGuard guard;
        EXPECT_TRUE(world.index.detach_root_locked(world.root.get(), World::SERIAL, retirement));
    }
    EXPECT_EQ(completed.load(), 0U);
    retirement.release_after_completed_tlb();
    finish.store(true, std::memory_order_release);
    for (auto& observer : observers) observer.join();
    EXPECT_EQ(completed.load(), snapshots.size());
    EXPECT_EQ(fixture::callbacks, 0U);
}

TEST_F(BindingSnapshotTest, PreparedCancellationAndReservationFailuresDoNotClaimAck) {
    World world;
    ASSERT_NE(world.family, nullptr);
    auto* prepared = fm::FileMappingBinding::prepare_unbound(*world.family);
    ASSERT_NE(prepared, nullptr);
    EXPECT_EQ(prepared->retirement_state(), State::PREPARED);
    prepared->release(fm::BindingOwner::PREPARED);
    EXPECT_TRUE(fixture::run_one());
    EXPECT_EQ(fixture::closes, 0U);
    fixture::fail_reserve_after = 0;
    EXPECT_EQ(fm::FileMappingBinding::prepare_unbound(*world.family), nullptr);
    EXPECT_EQ(world.family->owner_counts().total, 1U);
    EXPECT_EQ(world.file.refcount.load(), 2);
    EXPECT_EQ(fixture::closes, 0U);
    fixture::fail_reserve_after = -1;
}
}  // namespace
