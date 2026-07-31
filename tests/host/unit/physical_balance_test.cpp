#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/physical_balance.hpp>
#include <thread>
#include <vector>

namespace mm = ker::mod::mm;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;

namespace {

constexpr size_t TEST_ZONE_BYTES = 32 * 1024 * 1024;

struct FreeDeleter {
    void operator()(void* ptr) const { std::free(ptr); }
};

struct AllocatorBalanceSnapshot {
    phys::PhysicalBalanceComponents components{};
    std::array<uint64_t, mm::PHYSICAL_PAGE_OWNER_COUNT> owner_pages{};
    std::array<uint64_t, mm::PHYSICAL_PAGE_OWNER_COUNT> owner_objects{};
    uint64_t identity_pages = 0;
    uint64_t mismatch_pages = 0;
};

auto snapshot_allocator(mm::PageAllocator& allocator) -> AllocatorBalanceSnapshot {
    AllocatorBalanceSnapshot snapshot{};
    uint64_t const FLAGS = allocator.lock_irq();
    snapshot.components.managed_pages = allocator.total_pages;
    snapshot.components.free_pages = allocator.free_count;
    snapshot.components.allocator_metadata_pages = allocator.metadata_pages;
    snapshot.owner_pages = allocator.owner_pages;
    snapshot.owner_objects = allocator.owner_objects;
    for (size_t i = 1; i < snapshot.owner_pages.size(); ++i) {
        snapshot.components.owner_pages += snapshot.owner_pages.at(i);
    }
    allocator.unlock_irq(FLAGS);

    snapshot.identity_pages = phys::physical_balance_identity_pages(snapshot.components);
    snapshot.mismatch_pages = phys::physical_balance_mismatch_pages(snapshot.components);
    return snapshot;
}

class PhysicalBalanceTest : public ::testing::Test {
   protected:
    std::unique_ptr<void, FreeDeleter> storage{};
    mm::PageAllocator* allocator = nullptr;

    void SetUp() override {
        void* memory = std::aligned_alloc(paging::PAGE_SIZE, TEST_ZONE_BYTES);
        ASSERT_NE(memory, nullptr);
        storage.reset(memory);
        std::memset(memory, 0, TEST_ZONE_BYTES);
        allocator = ::new (memory) mm::PageAllocator{};
        allocator->init(reinterpret_cast<uint64_t>(memory), TEST_ZONE_BYTES);
        ASSERT_GT(allocator->usable_pages, 0U);
        ASSERT_EQ(snapshot_allocator(*allocator).mismatch_pages, 0U);
    }
};

TEST_F(PhysicalBalanceTest, AllocationTransferSplitAndFreeRemainExact) {
    auto const BEFORE = snapshot_allocator(*allocator);
    size_t const SELFTEST = static_cast<size_t>(mm::PhysicalPageOwner::SELFTEST);
    size_t const PRIVATE = static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING);

    uint64_t flags = allocator->lock_irq();
    void* allocation = allocator->alloc(3 * paging::PAGE_SIZE, mm::PhysicalPageOwner::SELFTEST);
    allocator->unlock_irq(flags);
    ASSERT_NE(allocation, nullptr);

    auto const LIVE = snapshot_allocator(*allocator);
    EXPECT_EQ(LIVE.mismatch_pages, 0U);
    EXPECT_EQ(LIVE.owner_pages.at(SELFTEST), BEFORE.owner_pages.at(SELFTEST) + 4);
    EXPECT_EQ(LIVE.owner_objects.at(SELFTEST), BEFORE.owner_objects.at(SELFTEST) + 1);

    flags = allocator->lock_irq();
    EXPECT_TRUE(allocator->reassign_allocated_block_owner(allocation, mm::PhysicalPageOwner::USER_PRIVATE_MAPPING));
    EXPECT_TRUE(allocator->split_allocated_block_to_order0(allocation));
    allocator->unlock_irq(flags);

    auto const TRANSFERRED = snapshot_allocator(*allocator);
    EXPECT_EQ(TRANSFERRED.mismatch_pages, 0U);
    EXPECT_EQ(TRANSFERRED.owner_pages.at(SELFTEST), BEFORE.owner_pages.at(SELFTEST));
    EXPECT_EQ(TRANSFERRED.owner_pages.at(PRIVATE), BEFORE.owner_pages.at(PRIVATE) + 4);
    EXPECT_EQ(TRANSFERRED.owner_objects.at(PRIVATE), BEFORE.owner_objects.at(PRIVATE) + 4);

    flags = allocator->lock_irq();
    for (size_t i = 0; i < 4; ++i) {
        auto* page = static_cast<std::byte*>(allocation) + (i * paging::PAGE_SIZE);
        EXPECT_EQ(allocator->free(page), paging::PAGE_SIZE);
    }
    allocator->unlock_irq(flags);

    auto const AFTER = snapshot_allocator(*allocator);
    EXPECT_EQ(AFTER.mismatch_pages, 0U);
    EXPECT_EQ(AFTER.components.free_pages, BEFORE.components.free_pages);
    EXPECT_EQ(AFTER.owner_pages, BEFORE.owner_pages);
    EXPECT_EQ(AFTER.owner_objects, BEFORE.owner_objects);
}

TEST_F(PhysicalBalanceTest, ConcurrentTransitionsHaveCoherentBoundedSnapshots) {
    constexpr size_t WORKER_COUNT = 6;
    constexpr size_t ITERATIONS = 1500;
    std::atomic<bool> start{false};
    std::atomic<size_t> workers_done{0};
    std::atomic<uint64_t> failures{0};
    std::atomic<uint64_t> snapshots{0};

    std::thread observer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (workers_done.load(std::memory_order_acquire) != WORKER_COUNT) {
            auto const snapshot = snapshot_allocator(*allocator);
            if (snapshot.mismatch_pages != 0 || snapshot.identity_pages != snapshot.components.managed_pages) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
            snapshots.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> workers;
    workers.reserve(WORKER_COUNT);
    for (size_t worker = 0; worker < WORKER_COUNT; ++worker) {
        workers.emplace_back([&, worker] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (size_t iteration = 0; iteration < ITERATIONS; ++iteration) {
                auto const OWNER =
                    ((worker + iteration) & 1U) == 0U ? mm::PhysicalPageOwner::SELFTEST : mm::PhysicalPageOwner::USER_PRIVATE_MAPPING;
                uint64_t const SIZE = ((worker + iteration) % 3U) == 0U ? 2 * paging::PAGE_SIZE : paging::PAGE_SIZE;
                uint64_t flags = allocator->lock_irq();
                void* allocation = allocator->alloc(SIZE, OWNER);
                allocator->unlock_irq(flags);
                if (allocation == nullptr) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                flags = allocator->lock_irq();
                bool const TRANSFERRED = allocator->reassign_allocated_block_owner(allocation, mm::PhysicalPageOwner::USER_FILE_CACHE);
                uint64_t const FREED = allocator->free(allocation);
                allocator->unlock_irq(flags);
                if (!TRANSFERRED || FREED == 0) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
            workers_done.fetch_add(1, std::memory_order_release);
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    observer.join();

    EXPECT_GT(snapshots.load(std::memory_order_relaxed), 0U);
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0U);
    EXPECT_EQ(snapshot_allocator(*allocator).mismatch_pages, 0U);
}

TEST_F(PhysicalBalanceTest, InjectedOwnerAndFreeCounterFaultsAreExact) {
    auto const CLEAN = snapshot_allocator(*allocator);
    size_t const OWNER = static_cast<size_t>(mm::PhysicalPageOwner::SELFTEST);

    uint64_t flags = allocator->lock_irq();
    allocator->owner_pages.at(OWNER)++;
    allocator->unlock_irq(flags);
    EXPECT_EQ(snapshot_allocator(*allocator).mismatch_pages, 1U);

    flags = allocator->lock_irq();
    allocator->owner_pages.at(OWNER)--;
    allocator->free_count--;
    allocator->unlock_irq(flags);
    EXPECT_EQ(snapshot_allocator(*allocator).mismatch_pages, 1U);

    flags = allocator->lock_irq();
    allocator->free_count++;
    allocator->unlock_irq(flags);
    auto const RESTORED = snapshot_allocator(*allocator);
    EXPECT_EQ(RESTORED.mismatch_pages, 0U);
    EXPECT_EQ(RESTORED.components.free_pages, CLEAN.components.free_pages);
}

TEST(PhysicalBalanceDescriptorTest, IncompleteAndResidualDescriptorsFailClosed) {
    EXPECT_TRUE(phys::physical_balance_descriptor_is_complete("user_file_cache", "cache lifetime", "bounded by cache capacity"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete(nullptr, "lifetime", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("", "lifetime", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("owner", "", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("owner", "lifetime", ""));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("unaccounted", "lifetime", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("unaccounted_estimate", "lifetime", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("unknown", "lifetime", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("other", "lifetime", "bound"));
    EXPECT_FALSE(phys::physical_balance_descriptor_is_complete("estimated", "lifetime", "bound"));
}

TEST(PhysicalBalanceEquationTest, OverflowCannotWrapIntoAnExactIdentity) {
    phys::PhysicalBalanceComponents const COMPONENTS{
        .managed_pages = 100,
        .free_pages = 80,
        .allocator_metadata_pages = 5,
        .zone_descriptor_pages = 2,
        .owner_pages = UINT64_MAX,
    };
    EXPECT_EQ(phys::physical_balance_identity_pages(COMPONENTS), UINT64_MAX);
    EXPECT_NE(phys::physical_balance_mismatch_pages(COMPONENTS), 0U);
}

}  // namespace
