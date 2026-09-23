#include <gtest/gtest.h>

#include <dev/ivshmem/pci_model.hpp>
#include <string>

namespace model = ker::dev::ivshmem::model;
namespace {
struct Backend {
    unsigned failure = 0;
    std::string calls;
    auto disable_bus_master() -> bool {
        calls += 'D';
        return failure != 1;
    }
    auto register_irq() -> bool {
        calls += 'R';
        return failure != 1;
    }
    auto enable_msix() -> bool {
        calls += 'M';
        return failure != 2;
    }
    auto enable_bus_master() -> bool {
        calls += 'B';
        return failure != 3;
    }
    void unwind_irq() { calls += 'U'; }
};
}  // namespace

TEST(IvshmemPci, OnlyQualifiedRevisionAndCapabilitiesSelectMode) {
    for (unsigned revision = 0; revision < 256; ++revision) {
        EXPECT_EQ(model::mode(revision, false), revision == 1 ? model::Mode::plain : model::Mode::unsupported);
        EXPECT_EQ(model::mode(revision, true), revision == 1 ? model::Mode::msix : model::Mode::unsupported);
    }
}

TEST(IvshmemPci, PlainNeverRegistersIrqOrEnablesBusMaster) {
    for (unsigned failure = 0; failure < 2; ++failure) {
        Backend backend{failure};
        EXPECT_EQ(model::activate(model::Mode::plain, backend), failure == 0);
        EXPECT_EQ(backend.calls, "D");
    }
    Backend unsupported;
    EXPECT_FALSE(model::activate(model::Mode::unsupported, unsupported));
    EXPECT_TRUE(unsupported.calls.empty());
}

TEST(IvshmemPci, IrqOwnedBeforeActivationAndEveryLaterFailureUnwinds) {
    for (unsigned failure = 0; failure < 4; ++failure) {
        Backend backend{failure};
        EXPECT_EQ(model::activate(model::Mode::msix, backend), failure == 0);
        EXPECT_EQ(backend.calls, failure == 0 ? "RMB" : failure == 1 ? "R" : failure == 2 ? "RMU" : "RMBU");
    }
}

TEST(IvshmemPci, BarAndExtentRejectWrapMisalignmentAndUndersizedStorage) {
    EXPECT_TRUE(model::bar(0x10000000, 0x1000000, 0x1000000));
    EXPECT_FALSE(model::bar(0, 0x1000, 256));
    EXPECT_FALSE(model::bar(0x10000000, 0, 256));
    EXPECT_FALSE(model::bar(0x10000000, 0x1000, 0x1000000));
    EXPECT_FALSE(model::bar(0x10000001, 0x1000, 256));
    EXPECT_FALSE(model::bar(0x10000000, 0x3000, 256));
    EXPECT_FALSE(model::bar(UINT64_MAX - 4095, 4096, 256));
    EXPECT_TRUE(model::extent(4096, 4096, 0));
    EXPECT_FALSE(model::extent(4096, UINT64_MAX, 2));
    EXPECT_FALSE(model::extent(4096, 4095, 2));
}

TEST(IvshmemPci, MsixTableAndPendingArrayMustFitAndNeverOverlap) {
    EXPECT_TRUE(model::msix_layout(0x40, 0, 1, 0x801, 4096));
    EXPECT_TRUE(model::msix_layout(0xf4, 127, 1, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0x3c, 0, 1, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0xf8, 0, 1, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0x41, 0, 1, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0x40, 0, 0, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0x40, 0, 1, 0x800, 4096));
    EXPECT_FALSE(model::msix_layout(0x40, 0, 1, 9, 4096));
    EXPECT_FALSE(model::msix_layout(0x40, 128, 1, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0x40, 0, 0xfffffff9, 0x801, 4096));
    EXPECT_FALSE(model::msix_layout(0x40, 0, 1, 0x1001, 4096));
}

TEST(IvshmemPci, DoorbellSelectsPeerInHighHalfAndVectorInLowHalf) {
    EXPECT_EQ(model::doorbell(1), 0x10000U);
    EXPECT_EQ(model::doorbell(0, 1), 1U);
    EXPECT_EQ(model::doorbell(0x1234, 0xabcd), 0x1234abcdU);
    EXPECT_EQ(model::doorbell(0xffff, 0xffff), UINT32_MAX);
}

TEST(IvshmemPci, RetirementClosesBeforeDrainingAndNeverUnpublishesAnActiveBorrow) {
    struct RetirementBackend {
        bool drained, stopped;
        std::string calls;
        void close_callbacks() { calls += 'C'; }
        auto drain_callbacks() -> bool {
            calls += 'D';
            return drained;
        }
        void inhibit_hardware() { calls += 'I'; }
        void unpublish() { calls += 'U'; }
        auto stop_interrupts() -> bool {
            calls += 'S';
            return stopped;
        }
    };
    for (bool drained : {false, true}) {
        for (bool stopped : {false, true}) {
            RetirementBackend backend{drained, stopped};
            EXPECT_EQ(model::retire(backend), !drained  ? model::Retirement::callbacks_busy
                                              : stopped ? model::Retirement::stopped
                                                        : model::Retirement::interrupts_unproven);
            EXPECT_EQ(backend.calls, drained ? "CDUS" : "CDI");
        }
    }
}
