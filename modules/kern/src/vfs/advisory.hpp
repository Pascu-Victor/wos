#pragma once

#include <cstdint>

namespace ker::vfs {
struct File;
struct VfsFlockAbi;
struct RemoteAdvisoryCommand;
struct RemoteAdvisoryReply;
enum class RemoteAdvisoryPhase : uint8_t;

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

// Task context, caller retains an exact native TMPFS/XFS File. These APIs use
// the existing native advisory table; a contested acquisition returns EAGAIN.
// Range normalization/fstat precedes its sleeping manager mutex. Query output
// changes only on success. No network/RX or user pointers reach this interface.
auto vfs_advisory_remote_try(File* file, const RemoteAdvisoryOwner& owner, RemoteAdvisoryKind kind, bool query, VfsFlockAbi& request)
    -> int;
auto vfs_advisory_remote_close_process(File* file, const RemoteAdvisoryOwner& owner) -> int;

// Deferred owner request/poll/cancel. One inline receipt and maximum64 pending
// operations use the same native manager; syscall/network workers never wait
// for another client's lock. Caller retains File; errors preserve output.
auto vfs_advisory_remote_request(File* file, uint64_t operation, RemoteAdvisoryPhase phase, const RemoteAdvisoryCommand& command,
                                 RemoteAdvisoryReply& output) -> int;

// Permanent admission closure is RX-safe and allocation-free. Task retirement
// releases locks/scopes under the manager before dropping the registry File
// reference; previously admitted I/O references may still keep the File alive.
void vfs_advisory_remote_close_admission(File* file);
void vfs_advisory_remote_retire(File* file);
}  // namespace ker::vfs
