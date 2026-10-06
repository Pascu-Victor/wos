#include <gtest/gtest.h>

#include <array>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

namespace reclaim = ker::mod::mm::reclaim;
namespace sched = ker::mod::sched;
namespace phys = ker::mod::mm::phys;

namespace {
struct Probe {
    unsigned calls{};
    bool may_block{};
    bool may_allocate{};
    bool may_io{};
};
Probe clean_probe;
Probe io_probe;
auto count(void*, const reclaim::ReclaimRequest&) -> reclaim::ReclaimCount { return {.reclaimable = 1}; }
auto scan(void* argument, const reclaim::ReclaimRequest& request) -> reclaim::ReclaimScanResult {
    auto& probe = *static_cast<Probe*>(argument);
    ++probe.calls;
    probe.may_block = request.may_block;
    probe.may_allocate = request.may_allocate;
    probe.may_io = request.may_io;
    return {.scanned = 1, .reclaimed = 1};
}
class ReclaimRuntime : public testing::Test {
   public:
    void SetUp() override {
        static bool const REGISTERED = [] {
            reclaim::Shrinker descriptor{"host-clean",
                                         reclaim::ReclaimUnit::PAGES,
                                         reclaim::ReclaimUnit::PAGES,
                                         0,
                                         reclaim::ReclaimPriority::LOW,
                                         reclaim::RECLAIM_MAY_BLOCK | reclaim::RECLAIM_MAY_ALLOCATE,
                                         1,
                                         1,
                                         count,
                                         scan,
                                         &clean_probe};
            if (!reclaim::register_shrinker(descriptor)) return false;
            descriptor.name = "host-io";
            descriptor.rank = 1;
            descriptor.capabilities |= reclaim::RECLAIM_MAY_IO;
            descriptor.opaque = &io_probe;
            return reclaim::register_shrinker(descriptor);
        }();
        ASSERT_TRUE(REGISTERED);
        clean_probe = {};
        io_probe = {};
        phys::block_test_reclaim_free_pages.store(0);
        sched::block_test_current_task = &first;
    }
    void TearDown() override {
        EXPECT_EQ(first.reclaim_no_io_depth, 0U);
        EXPECT_EQ(second.reclaim_no_io_depth, 0U);
        sched::block_test_current_task = nullptr;
        phys::block_test_reclaim_free_pages.store(1024 * 1024);
    }
    sched::task::Task first;
    sched::task::Task second;
};
}  // namespace

TEST_F(ReclaimRuntime, ScopedAllocationReclaimPreservesCleanProgressWithoutIo) {
    auto const BEFORE = reclaim::reclaim_for_allocation(0, 1);
    ASSERT_EQ(BEFORE.callbacks, 2U);
    {
        reclaim::NoIoScope const NO_IO;
        auto const SCOPED = reclaim::reclaim_for_allocation(0, 1);
        EXPECT_EQ(SCOPED.callbacks, 1U);
        EXPECT_TRUE(SCOPED.made_progress);
        EXPECT_EQ(clean_probe.calls, 2U);
        EXPECT_EQ(io_probe.calls, 1U);
        EXPECT_FALSE(clean_probe.may_io);
        EXPECT_TRUE(clean_probe.may_block);
        EXPECT_TRUE(clean_probe.may_allocate);
        EXPECT_EQ(first.preempt_disable_depth, 0U);
    }
    EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 2U);
    EXPECT_EQ(io_probe.calls, 2U);
    EXPECT_TRUE(io_probe.may_io);
}

TEST_F(ReclaimRuntime, NestedScopesStayBoundToTheirCreatingTaskAcrossSwitches) {
    {
        reclaim::NoIoScope const OUTER;
        EXPECT_EQ(first.reclaim_no_io_depth, 1U);
        {
            reclaim::NoIoScope const INNER;
            EXPECT_EQ(first.reclaim_no_io_depth, 2U);
            EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 1U);
            sched::block_test_current_task = &second;
            EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 2U);
            // Destructor follows its owner even if a platform seam switches
            // the reported current Task. It must never decrement second.
        }
        EXPECT_EQ(first.reclaim_no_io_depth, 1U);
        EXPECT_EQ(second.reclaim_no_io_depth, 0U);
        sched::block_test_current_task = &first;
        EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 1U);
    }
    EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 2U);
}

TEST_F(ReclaimRuntime, BootScopeDoesNotSuppressLaterTaskIoOrBypassUnsafeContext) {
    sched::block_test_current_task = nullptr;
    {
        reclaim::NoIoScope const BOOT;
        EXPECT_TRUE(reclaim::reclaim_for_allocation(0, 1).unsafe_context);
        EXPECT_EQ(clean_probe.calls, 0U);
        sched::block_test_current_task = &first;
        EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 2U);
    }
    first.preempt_disable_depth = 1;
    {
        reclaim::NoIoScope const NO_IO;
        EXPECT_TRUE(reclaim::reclaim_for_allocation(0, 1).unsafe_context);
    }
    first.preempt_disable_depth = 0;
    EXPECT_EQ(reclaim::reclaim_for_allocation(0, 1).callbacks, 2U);
}
