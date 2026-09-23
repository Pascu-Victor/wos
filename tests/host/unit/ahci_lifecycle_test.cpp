#include <gtest/gtest.h>

#include <atomic>
#include <dev/ahci_lifecycle.hpp>
#include <latch>
#include <mutex>
#include <thread>
#include <vector>

namespace lifecycle = ker::dev::ahci::lifecycle;
namespace {
struct Backend {
    lifecycle::Admission admission;
    bool drained = true, durable = true, stopped = true, disabled = true, released = true;
    std::vector<int> events;
    void close_admission() {
        admission.close();
        events.push_back(1);
    }
    auto drain_readers() -> bool {
        events.push_back(2);
        return drained;
    }
    auto flush() -> bool {
        events.push_back(3);
        return durable;
    }
    auto stop_engines() -> bool {
        events.push_back(4);
        return stopped;
    }
    auto disable_bus_master() -> bool {
        events.push_back(5);
        return disabled;
    }
    void quarantine() { events.push_back(6); }
    auto release() -> bool {
        events.push_back(7);
        return released;
    }
};
}  // namespace
TEST(AhciLifecycle, FlushAndRetirementProofsPrecedeRelease) {
    Backend b;
    b.admission.open_for_probe();
    EXPECT_EQ(lifecycle::retire(b), lifecycle::Result::released);
    EXPECT_FALSE(b.admission.allows_command());
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 3, 4, 5, 7}));
}
TEST(AhciLifecycle, UnprovenReadersNeverPermitPortStateMutationOrRelease) {
    Backend b;
    b.drained = false;
    EXPECT_EQ(lifecycle::retire(b), lifecycle::Result::drain_failed);
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 5}));
}
TEST(AhciLifecycle, EitherMissingHardwareProofQuarantinesAllBacking) {
    for (bool stop : {false, true}) {
        Backend b;
        b.stopped = stop;
        b.disabled = !stop;
        EXPECT_EQ(lifecycle::retire(b), lifecycle::Result::quiesce_failed);
        EXPECT_EQ(b.events, (std::vector<int>{1, 2, 3, 4, 5, 6}));
    }
}
TEST(AhciLifecycle, FlushFailureNeverReportsDurabilityEvenAfterSafeRelease) {
    Backend b;
    b.durable = false;
    EXPECT_EQ(lifecycle::retire(b), lifecycle::Result::flush_failed);
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 3, 4, 5, 7}));
}
TEST(AhciLifecycle, InvalidationFailureNeverReportsSuccessfulRetirement) {
    Backend b;
    b.released = false;
    EXPECT_EQ(lifecycle::retire(b), lifecycle::Result::release_failed);
    EXPECT_EQ(b.events, (std::vector<int>{1, 2, 3, 4, 5, 7}));
}
TEST(AhciLifecycle, CloseDrainsAlreadyAdmittedCommandAndRejectsQueuedCommand) {
    lifecycle::Admission admission;
    admission.open_for_probe();
    std::mutex port;
    std::latch admitted{1}, closed{1}, finish_command{1};
    std::atomic<bool> completed{false};
    std::thread command([&] {
        std::lock_guard guard(port);
        EXPECT_TRUE(admission.allows_command());
        admitted.count_down();
        finish_command.wait();
        completed.store(true);
    });
    admitted.wait();
    struct ConcurrentBackend {
        lifecycle::Admission& admission;
        std::mutex& port;
        std::latch& closed;
        std::atomic<bool>& completed;
        std::unique_lock<std::mutex> guard;
        void close_admission() {
            admission.close();
            closed.count_down();
        }
        auto drain_readers() -> bool {
            guard = std::unique_lock(port);
            return true;
        }
        auto flush() -> bool {
            EXPECT_TRUE(completed.load());
            return true;
        }
        auto stop_engines() -> bool {
            EXPECT_TRUE(guard.owns_lock());
            return true;
        }
        auto disable_bus_master() -> bool { return true; }
        void quarantine() { ADD_FAILURE(); }
        auto release() -> bool {
            EXPECT_FALSE(admission.allows_command());
            return true;
        }
    };
    std::thread teardown([&] {
        ConcurrentBackend b{admission, port, closed, completed, {}};
        EXPECT_EQ(lifecycle::retire(b), lifecycle::Result::released);
    });
    closed.wait();
    EXPECT_FALSE(admission.allows_command());
    EXPECT_FALSE(completed.load());  // Closure did not forcibly retire this command.
    std::thread queued([&] {
        std::lock_guard guard(port);
        EXPECT_FALSE(admission.allows_command());
    });
    finish_command.count_down();
    command.join();
    teardown.join();
    queued.join();
}
