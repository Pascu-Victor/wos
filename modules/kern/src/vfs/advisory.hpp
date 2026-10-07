#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {
struct File;
struct VfsFlockAbi;
struct RemoteAdvisoryCommand;
struct RemoteAdvisoryReply;
struct RemoteAdvisoryScopeResult;
enum class RemoteAdvisoryPhase : uint8_t;

struct AdvisoryDiag {
    uint64_t locks = 0;
    uint64_t associations = 0;
    uint64_t scopes = 0;
    uint64_t closed_scopes = 0;
    uint64_t wait_receipts = 0;
    uint64_t pending = 0;
    constexpr auto operator==(const AdvisoryDiag&) const -> bool = default;
};
// Task context only, called unlocked. Copies scalars under the native manager
// mutex; no retained File/Task pointers or network work escapes the snapshot.
auto vfs_advisory_diag_snapshot() -> AdvisoryDiag;

// Assigned by the authenticated peer's process owner, independently of PID
// reuse. Caller captures peer/boot from the exact admitted server binding.
struct RemoteAdvisoryOwner {
    uint64_t generation = 0;
    uint32_t boot_epoch = 0;
    int32_t pid = 0;
    uint16_t peer = UINT16_MAX;
    constexpr auto operator==(const RemoteAdvisoryOwner&) const -> bool = default;
    constexpr auto valid() const -> bool { return generation != 0 && boot_epoch != 0 && pid > 0 && peer != UINT16_MAX; }
};
enum class RemoteAdvisoryKind : uint8_t { PROCESS = 1, OPEN_FILE = 2, FLOCK = 3 };

// Caller retains the subject Task and its immutable filesystem owner. Threads
// share that owner's nonreused generation; zero means unbound/exhausted or an
// unassigned peek. Atomic-only; never resolves against get_current_task().
auto vfs_advisory_process_generation(ker::mod::sched::task::Task& subject, bool assign = true) -> uint64_t;

// Retained canonical inode identity; no version/cache freshness is granted.
auto vfs_advisory_remote_identity(File* file, uint64_t& output) -> int;
// Bounded process/inode scope, registered before any scoped POSIX effect.
// Registration replay stays terminal after close/ACK. Close cancels its pending
// POSIX waits and cannot affect successor tokens. ACK only frees closed storage.
auto vfs_advisory_remote_register_scope(File* witness, const RemoteAdvisoryOwner& owner, uint64_t operation,
                                        RemoteAdvisoryScopeResult& output) -> int;
auto vfs_advisory_remote_close_scope(const RemoteAdvisoryOwner& owner, uint64_t token, RemoteAdvisoryScopeResult& output) -> int;
auto vfs_advisory_remote_ack_scope(const RemoteAdvisoryOwner& owner, uint64_t token, RemoteAdvisoryScopeResult& output) -> int;

// Task context, caller retains an exact native TMPFS/XFS File. These APIs use
// the existing native advisory table; a contested acquisition returns EAGAIN.
// Range normalization/fstat precedes its sleeping manager mutex. Query output
// changes only on success. No network/RX or user pointers reach this interface.
auto vfs_advisory_remote_try(File* file, const RemoteAdvisoryOwner& owner, RemoteAdvisoryKind kind, bool query, VfsFlockAbi& request)
    -> int;
auto vfs_advisory_remote_close_process(File* file, const RemoteAdvisoryOwner& owner) -> int;

// Deferred owner request/poll/cancel/ACK. Maximum64 pending/terminal wait
// acquisition records and per-open monotonic IDs use the same native manager. A shared
// open can query/unlock while another operation waits. Workers never wait
// for another client's lock. Caller retains File; errors preserve output.
auto vfs_advisory_remote_request(File* file, uint64_t operation, RemoteAdvisoryPhase phase, const RemoteAdvisoryCommand& command,
                                 RemoteAdvisoryReply& output) -> int;

// Permanent admission closure is RX-safe and allocation-free. Task retirement
// releases locks/scopes under the manager before dropping the registry File
// reference; previously admitted I/O references may still keep the File alive.
void vfs_advisory_remote_close_admission(File* file);
void vfs_advisory_remote_retire(File* file);
}  // namespace ker::vfs
