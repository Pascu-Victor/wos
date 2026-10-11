#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::vfs {
// Caller retains subject; SYNC is global Host control, unlike explicit-File FSYNC.
auto sync_for_subject(ker::mod::sched::task::Task* subject) -> int64_t;
// Operation codes will be defined in abi when implemented further.
auto sys_vfs(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) -> int64_t;
auto wki_rules_for_subject(ker::mod::sched::task::Task* subject, uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4)
    -> int64_t;

}  // namespace ker::syscall::vfs
