#include <gtest/gtest.h>

#include <dev/virtio/virtio_lifecycle.hpp>
#include <latch>
#include <thread>
#include <vector>

namespace life = ker::dev::virtio::lifecycle;
namespace {
struct Backend {
    life::Admission admission;
    bool readers = true, masked = true, reset_ok = true, bme = true, released = true;
    std::vector<int> events;
    void close_admission() {
        admission.close();
        events.push_back(1);
    }
    auto drain_readers() -> bool {
        events.push_back(2);
        return readers;
    }
    void unpublish() { events.push_back(3); }
    auto mask_interrupts() -> bool {
        events.push_back(4);
        return masked;
    }
    auto reset() -> bool {
        events.push_back(5);
        return reset_ok;
    }
    auto disable_bus_master() -> bool {
        events.push_back(6);
        return bme;
    }
    void drain_interrupts() { events.push_back(7); }
    void quarantine() { events.push_back(8); }
    auto release() -> bool {
        events.push_back(9);
        return released;
    }
};
}  // namespace
TEST(VirtioLifecycle, AllRetirementProofsPrecedeRelease) {
    Backend b;
    EXPECT_EQ(life::retire(b), life::Result::released);
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 3, 4, 5, 6, 7, 9}));
    EXPECT_TRUE(b.admission.drained());
    EXPECT_FALSE(b.admission.enter());
}
TEST(VirtioLifecycle, ReaderFailureRetainsQueueOwnershipAndCpuBorrows) {
    Backend b;
    b.readers = false;
    EXPECT_EQ(life::retire(b), life::Result::drain_failed);
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 6}));
}
TEST(VirtioLifecycle, MissingHardwareProofPinsBacking) {
    for (int failure = 0; failure != 3; ++failure) {
        Backend b;
        b.masked = failure != 0;
        b.reset_ok = failure != 1;
        b.bme = failure != 2;
        EXPECT_EQ(life::retire(b), life::Result::quiesce_failed);
        EXPECT_EQ(b.events, failure == 0 ? (std::vector<int>{1, 2, 3, 4, 5, 6, 8}) : (std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8}));
    }
}
TEST(VirtioLifecycle, ReleaseFailureIsNotReportedAsSuccess) {
    Backend b;
    b.released = false;
    EXPECT_EQ(life::retire(b), life::Result::release_failed);
}
TEST(VirtioLifecycle, ClosureDrainsAdmittedCallbackAndRejectsLateCallers) {
    life::Admission admission;
    std::latch entered{1}, finish{1};
    int published = 0;
    std::thread callback([&] {
        life::Guard guard(admission);
        EXPECT_TRUE(guard);
        entered.count_down();
        finish.wait();
        published = 42;
    });
    entered.wait();
    admission.close();
    EXPECT_TRUE(admission.closed());
    EXPECT_FALSE(admission.drained());
    {
        life::Guard late(admission);
        EXPECT_FALSE(late);
    }
    EXPECT_FALSE(admission.drained());
    finish.count_down();
    while (!admission.drained()) std::this_thread::yield();
    EXPECT_EQ(published, 42);  // acquire observes callback writes before release.
    callback.join();
    admission.close();
    EXPECT_TRUE(admission.drained());
    EXPECT_FALSE(admission.enter());
}
TEST(VirtioLifecycle, NestedReadersCannotReopenClosedGate) {
    life::Admission admission;
    {
        life::Guard outer(admission);
        {
            life::Guard inner(admission);
            EXPECT_TRUE(inner);
            admission.close();
        }
        EXPECT_FALSE(admission.drained());
        life::Guard late(admission);
        EXPECT_FALSE(late);
    }
    EXPECT_TRUE(admission.drained());
}
