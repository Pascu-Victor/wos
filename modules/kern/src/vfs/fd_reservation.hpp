#pragma once

#include <cstdint>
#ifdef WOS_SELFTEST
#include <array>
namespace ker::test::lifecycle60 {
class CensusLease;
}
namespace ker::test::numa_lifecycle {
class CurrentCensusScope;
}
#endif

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {
struct File;
#ifdef WOS_SELFTEST
struct SelftestFdSlot {
    uint64_t file_identity{}, file_references{}, multiplicity{};
    bool cloexec{}, reserved{};
    auto operator==(const SelftestFdSlot&) const -> bool = default;
};
struct SelftestFdCensus {
    std::array<SelftestFdSlot, 256> slots{};
    uint64_t installed{}, reserved{}, distinct_files{}, executor_pending{}, recovery_presence{}, executor_cookie{};
    bool complete{};
    auto operator==(const SelftestFdCensus&) const -> bool = default;
};
// Requires the exact controller-issued executor ACK lease, including no live
// shared FD/table writer. A retained Task or caller bool is insufficient. Pending
// executor list is not protected by the driver's fd_table_lock (targets vary).
// No File retain/drop; file_identity is comparison-only, never dereference it.
[[nodiscard]] auto selftest_fd_census_parked(const ker::test::lifecycle60::CensusLease& lease) -> SelftestFdCensus;
void selftest_fd_census_current(const ker::test::numa_lifecycle::CurrentCensusScope&, SelftestFdCensus&);
#endif

// Reserve before backend effects. Slot publication cannot fail from EMFILE
// after reservation succeeds, even if another thread lowers the soft limit.
class FdReservation {
   public:
    explicit FdReservation(mod::sched::task::Task* target, uint64_t minimum = 0, bool dupfd = false);
    ~FdReservation();
    FdReservation(const FdReservation&) = delete;
    auto operator=(const FdReservation&) -> FdReservation& = delete;
    auto result() const -> int { return m_result; }
    auto install(File* file, bool cloexec = false) -> int;
    auto install_pair(FdReservation& second, File* first_file, File* second_file, bool cloexec) -> int;
    void cancel();

   private:
#ifdef WOS_SELFTEST
    friend auto selftest_fd_census_parked(const ker::test::lifecycle60::CensusLease&) -> SelftestFdCensus;
    friend void selftest_fd_census_current(const ker::test::numa_lifecycle::CurrentCensusScope&, SelftestFdCensus&);
    static void collect_selftest(mod::sched::task::Task&, uint64_t cookie, SelftestFdCensus&);
#endif
    mod::sched::task::Task* m_table = nullptr;
    mod::sched::task::Task* m_executor = nullptr;
    FdReservation* m_previous = nullptr;
    FdReservation* m_next = nullptr;
    int m_result = -1;
};

// Caller holds fd_table_lock. Order: fd_table_lock -> resource_lock.
auto fd_limit_locked(mod::sched::task::Task& task) -> uint64_t;
void fd_usage_changed_locked(mod::sched::task::Task& task);
void fd_limit_denied_locked(mod::sched::task::Task& task);
// Must run on the exiting task before its kernel stack is discarded.
void cancel_fd_reservations(mod::sched::task::Task& executor);
}  // namespace ker::vfs
