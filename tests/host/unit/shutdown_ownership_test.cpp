#include <gtest/gtest.h>

#include <dev/virtio/virtio_lifecycle.hpp>
#include <platform/sched/shutdown_ownership.hpp>

namespace sched = ker::mod::sched;
namespace life = ker::dev::virtio::lifecycle;

TEST(ShutdownOwnership, DeferralPreservesExistingExitRestrictions) {
    for (unsigned bits = 0; bits < 16; ++bits) {
        EXPECT_EQ(sched::cooperative_exit_deferred(bits & 1U, (bits & 2U) == 0, (bits & 4U) != 0, (bits & 8U) != 0), bits != 0);
    }
}
TEST(ShutdownOwnership, NestedResourcesDeferUntilEveryScopeReturns) {
    sched::ShutdownOwnership owner;
    EXPECT_FALSE(owner.held());
    {
        sched::KernelShutdownHold outer(&owner);
        EXPECT_TRUE(owner.held());
        {
            sched::KernelShutdownHold inner(&owner);
            EXPECT_TRUE(owner.held());
        }
        EXPECT_TRUE(sched::cooperative_exit_deferred(0, true, false, owner.held()));
    }
    EXPECT_FALSE(sched::cooperative_exit_deferred(0, true, false, owner.held()));
}
TEST(ShutdownOwnership, HoldRemainsBoundToOwnerWhenCurrentTaskChanges) {
    sched::ShutdownOwnership first, second;
    auto* current = &first;
    {
        sched::KernelShutdownHold hold(current);
        current = &second;
        EXPECT_TRUE(first.held());
        EXPECT_FALSE(current->held());
    }
    EXPECT_FALSE(first.held());
    EXPECT_FALSE(second.held());
}
TEST(ShutdownOwnership, BootWithoutTaskHasNoOwnershipSideEffects) {
    sched::KernelShutdownHold hold(nullptr);
    EXPECT_FALSE(sched::cooperative_exit_deferred(0, true, false, false));
}
TEST(ShutdownOwnership, CallbackRetiresBeforeCooperativeExitCanResume) {
    sched::ShutdownOwnership owner;
    life::Admission admission;
    {
        sched::KernelShutdownHold hold(&owner);
        {
            life::Guard callback(admission);
            ASSERT_TRUE(callback);
            admission.close();
            EXPECT_FALSE(admission.drained());
            EXPECT_TRUE(sched::cooperative_exit_deferred(0, true, false, owner.held()));
        }
        EXPECT_TRUE(admission.drained());
        EXPECT_TRUE(owner.held());
        life::Guard late(admission);
        EXPECT_FALSE(late);
    }
    EXPECT_TRUE(admission.drained());
    EXPECT_FALSE(sched::cooperative_exit_deferred(0, true, false, owner.held()));
}
