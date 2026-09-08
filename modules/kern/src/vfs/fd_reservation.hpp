#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {
struct File;

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
