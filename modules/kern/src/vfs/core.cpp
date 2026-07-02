#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <dev/gpt.hpp>
#include <memory>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wki.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>

#include "file.hpp"
#include "fs/devfs.hpp"
#include "fs/fat32.hpp"
#include "fs/tmpfs.hpp"
#include "net/wki/wire.hpp"
#include "platform/dbg/dbg.hpp"
#include "vfs.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs {

using log = ker::mod::dbg::logger<"vfs">;

// Keep in sync with userspace fcntl.h (Linux-compatible octal values)
constexpr int O_NONBLOCK = 04000;
constexpr int O_RDONLY_MODE = 0;
constexpr int O_WRONLY_MODE = 1;
constexpr int O_RDWR_MODE = 2;
constexpr int O_ACCMODE_MASK = 3;

namespace {
constexpr size_t MAX_PATH_LEN = 512;
constexpr int MAX_SYMLINK_DEPTH = 8;
constexpr size_t MAX_COMPONENTS = 64;
constexpr size_t MAX_VFSTAB_BYTES = 4096;
constexpr size_t WKI_PATH_PREFIX_LEN = 5;
constexpr int64_t VFS_NSEC_PER_SEC = 1000000000LL;
constexpr int64_t VFS_UTIME_NOW = (1LL << 30) - 1;
constexpr int64_t VFS_UTIME_OMIT = (1LL << 30) - 2;

auto make_absolute(const char* path, char* out, size_t outsize) -> int;
auto canonicalize_path(char* path, size_t bufsize) -> int;
auto normalize_task_path_inplace(char* path, size_t bufsize) -> int;
auto normalize_task_path_inplace_with_route(char* path, size_t bufsize, bool apply_task_route) -> int;
auto resolve_task_path_raw_impl(const char* path, char* out, size_t outsize, bool apply_task_route) -> int;
auto readlink_resolved(const char* abs_path, char* buf, size_t bufsize) -> ssize_t;
auto strip_mount_prefix(const MountPoint* mount, const char* path) -> const char*;
auto tmpfs_root_for_mount(const MountPoint* mount) -> ker::vfs::tmpfs::TmpNode*;

ker::util::SmallVec<ker::mod::sched::task::WkiVfsRule, 8> g_default_vfs_rules;

struct VfsRouteDecision {
    uint8_t route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
    size_t prefix_len = 0;
};

struct VfsResolvedTimes {
    Timespec atime{};
    Timespec mtime{};
    Timespec ctime{};
    bool set_atime = false;
    bool set_mtime = false;
};

constexpr size_t STREAM_CHUNK_SIZE = 65536;
constexpr size_t STREAM_ENTRY_BYTE_CAP = size_t{8} * 1024 * 1024;
constexpr size_t STREAM_DETACHED_REUSE_MAX = size_t{8} * 1024 * 1024;
constexpr size_t STREAM_DETACHED_PARTIAL_REUSE_MAX = size_t{256} * 1024;
constexpr size_t STREAM_LOCAL_SMALL_FILE_RETAIN_MAX = size_t{2} * 1024 * 1024;
constexpr size_t STREAM_DETACHED_GLOBAL_BYTE_CAP = size_t{256} * 1024 * 1024;
constexpr size_t STREAM_MAX_ACTIVE_ISLANDS = 4;
constexpr uint64_t STREAM_DETACHED_TTL_US = 5000000;
constexpr uint64_t STREAM_LOCAL_SMALL_FILE_DETACHED_TTL_US = 60000000;
constexpr uint64_t STREAM_SPLIT_DISTANCE_BYTES = uint64_t{2} * 1024 * 1024;
constexpr int STREAM_PREMATURE_EOF_RETRIES = 3;
constexpr size_t PIPE_WAKE_BATCH = 32;
constexpr size_t PIPE_WAITER_INLINE_CAPACITY = PIPE_WAKE_BATCH * 2;
constexpr size_t PIPE_DEFAULT_CAPACITY = 256UL * 1024UL;
constexpr size_t PIPE_DIRECT_MAX_CAPACITY = 256UL * 1024UL;
constexpr uint64_t ADVISORY_RANGE_EOF = UINT64_MAX;
// CMake/Ninja tree scans touch enough distinct paths that small metadata caches
// thrash mostly on set conflicts. Keep this static and bounded: 131072 entries
// gives LLVM-sized checkout/configure/build scans enough room without increasing
// the per-lookup way scan.
constexpr size_t METADATA_CACHE_SET_COUNT = 16384;
constexpr size_t METADATA_CACHE_WAYS = 8;
constexpr size_t METADATA_INVALIDATION_SET_COUNT = 8192;
constexpr size_t METADATA_INVALIDATION_WAYS = 4;
// Source-tree opens/stat calls repeatedly prove that hot ancestor directories
// are not symlinks. LLVM-sized checkouts also generate many one-shot final-path
// ENOENT probes, so keep enough negative readlink entries that those do not
// evict hot ancestor directory proofs immediately.
constexpr size_t SYMLINK_CACHE_SET_COUNT = 8192;
constexpr size_t SYMLINK_CACHE_WAYS = 4;
static_assert((METADATA_CACHE_SET_COUNT & (METADATA_CACHE_SET_COUNT - 1)) == 0);
static_assert((METADATA_INVALIDATION_SET_COUNT & (METADATA_INVALIDATION_SET_COUNT - 1)) == 0);
static_assert((SYMLINK_CACHE_SET_COUNT & (SYMLINK_CACHE_SET_COUNT - 1)) == 0);

struct MetadataCacheEntry {
    std::array<char, MAX_PATH_LEN> path{};
    Stat stat{};
    uint64_t hash = 0;
    uint64_t epoch = 0;
    uint64_t last_used = 0;
    uint64_t dev_id = 0;
    uint64_t invalidation_generation = 0;
    size_t path_len = 0;
    int result = 0;
    FSType fs_type = FSType::TMPFS;
    bool follow_final_symlink = false;
    bool require_directory = false;
    bool valid = false;
};

struct MetadataCacheSet {
    ker::mod::sys::Mutex lock;
    std::array<MetadataCacheEntry, METADATA_CACHE_WAYS> ways{};
    uint64_t clock = 0;
};

struct SymlinkCacheEntry {
    std::array<char, MAX_PATH_LEN> path{};
    std::array<char, MAX_PATH_LEN> target{};
    uint64_t hash = 0;
    uint64_t epoch = 0;
    uint64_t last_used = 0;
    uint64_t dev_id = 0;
    uint64_t invalidation_generation = 0;
    size_t path_len = 0;
    size_t target_len = 0;
    ssize_t result = 0;
    FSType fs_type = FSType::TMPFS;
    bool valid = false;
};

struct SymlinkCacheSet {
    ker::mod::sys::Mutex lock;
    std::array<SymlinkCacheEntry, SYMLINK_CACHE_WAYS> ways{};
    uint64_t clock = 0;
};

struct MetadataInvalidationEntry {
    uint64_t path_hash = 0;
    uint64_t generation = 0;
    bool valid = false;
};

struct MetadataInvalidationSet {
    std::array<MetadataInvalidationEntry, METADATA_INVALIDATION_WAYS> ways{};
    // If the set fills, invalidate the whole hash set instead of the whole cache.
    uint64_t overflow_generation = 0;
};

struct VfsFlockAbi {
    int16_t l_type = 0;
    int16_t l_whence = 0;
    off_t l_start = 0;
    off_t l_len = 0;
    int32_t l_pid = 0;
};
static_assert(sizeof(VfsFlockAbi) == 32);

constexpr int F_GETLK_CMD = 5;
constexpr int F_SETLK_CMD = 6;
constexpr int F_SETLKW_CMD = 7;
constexpr int F_OFD_GETLK_CMD = 36;
constexpr int F_OFD_SETLK_CMD = 37;
constexpr int F_OFD_SETLKW_CMD = 38;
constexpr int WOS_FLOCK_CMD = 0x5753464c;  // Private mlibc flock() request: 'WSFL'.
constexpr int16_t F_RDLCK_TYPE = 0;
constexpr int16_t F_WRLCK_TYPE = 1;
constexpr int16_t F_UNLCK_TYPE = 2;
constexpr int LOCK_SH_VALUE = 1;
constexpr int LOCK_EX_VALUE = 2;
constexpr int LOCK_NB_VALUE = 4;
constexpr int LOCK_UN_VALUE = 8;
constexpr int SEEK_SET_VALUE = 0;
constexpr int SEEK_CUR_VALUE = 1;
constexpr int SEEK_END_VALUE = 2;

enum class AdvisoryLockFamily : uint8_t {
    RECORD,
    FLOCK,
};

enum class AdvisoryOwnerKind : uint8_t {
    PROCESS,
    OPEN_FILE,
};

enum class AdvisoryLockType : uint8_t {
    READ,
    WRITE,
};

struct AdvisoryFileKey {
    uint64_t dev = 0;
    uint64_t ino = 0;
    uint64_t path_hash = 0;
    const File* anonymous_file = nullptr;
    FSType fs_type = FSType::TMPFS;
    std::array<char, MAX_PATH_LEN> path{};
    bool has_inode = false;
    bool has_path = false;
};

struct AdvisoryLock {
    AdvisoryFileKey key{};
    AdvisoryLockFamily family = AdvisoryLockFamily::RECORD;
    AdvisoryOwnerKind owner_kind = AdvisoryOwnerKind::PROCESS;
    uint64_t owner_pid = 0;
    const File* owner_file = nullptr;
    AdvisoryLockType type = AdvisoryLockType::READ;
    uint64_t start = 0;
    uint64_t end = ADVISORY_RANGE_EOF;
};

std::array<MetadataCacheSet, METADATA_CACHE_SET_COUNT> g_metadata_cache{};
// Path metadata/readlink caches use path-scoped invalidation generations.
// g_metadata_cache_epoch remains the conservative public epoch exposed for
// diagnostics and external cache consumers that need a simple global token.
std::atomic<uint64_t> g_metadata_cache_generation{1};
std::atomic<uint64_t> g_metadata_cache_epoch{1};
std::atomic<uint64_t> g_metadata_observation_epoch{1};

std::array<SymlinkCacheSet, SYMLINK_CACHE_SET_COUNT> g_symlink_cache{};

std::array<MetadataInvalidationSet, METADATA_INVALIDATION_SET_COUNT> g_metadata_subtree_invalidations{};
std::array<MetadataInvalidationSet, METADATA_INVALIDATION_SET_COUNT> g_metadata_exact_invalidations{};
ker::mod::sys::Mutex g_metadata_invalidation_lock;
std::atomic<uint64_t> g_metadata_invalidation_generation{1};
std::deque<AdvisoryLock> g_advisory_locks;
ker::mod::sys::Mutex g_advisory_lock_mutex;

std::atomic<uint64_t> g_vfs_metadata_hits{0};
std::atomic<uint64_t> g_vfs_metadata_misses{0};
std::atomic<uint64_t> g_vfs_metadata_stores{0};
std::atomic<uint64_t> g_vfs_metadata_miss_empty{0};
std::atomic<uint64_t> g_vfs_metadata_miss_invalidated{0};
std::atomic<uint64_t> g_vfs_metadata_miss_stale_generation{0};
std::atomic<uint64_t> g_vfs_metadata_miss_conflict{0};
std::atomic<uint64_t> g_vfs_metadata_path_invalidations{0};
std::atomic<uint64_t> g_vfs_metadata_generation_resets{0};
std::atomic<uint64_t> g_vfs_symlink_hits{0};
std::atomic<uint64_t> g_vfs_symlink_misses{0};
std::atomic<uint64_t> g_vfs_symlink_stores{0};
std::atomic<uint64_t> g_vfs_stream_hits{0};
std::atomic<uint64_t> g_vfs_stream_misses{0};
std::atomic<uint64_t> g_vfs_stream_backend_reads{0};
std::atomic<uint64_t> g_vfs_stream_backend_bytes{0};
std::atomic<uint64_t> g_vfs_stream_copied_bytes{0};
std::atomic<uint64_t> g_vfs_stream_invalidate_empty_skips{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_hits{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_misses{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_stores{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_uncacheable{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_bad_args{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_no_cache{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_pathless{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_fs{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_empty{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_generation{0};
std::atomic<uint64_t> g_vfs_fstat_snapshot_miss_invalidated{0};

auto open_flags_require_fs_write(int flags) -> bool {
    int const ACCMODE = flags & 3;
    return ACCMODE == 1 || ACCMODE == 2 || (flags & (ker::vfs::O_CREAT | ker::vfs::O_TRUNC)) != 0;
}

auto fcntl_setfl_flags(int current_flags, uint64_t requested_raw) -> int {
    constexpr int MUTABLE_STATUS_FLAGS = ker::vfs::O_APPEND | O_NONBLOCK;
    int const REQUESTED_FLAGS = static_cast<int>(requested_raw);
    return (current_flags & ~MUTABLE_STATUS_FLAGS) | (REQUESTED_FLAGS & MUTABLE_STATUS_FLAGS);
}

struct StreamCacheIdentity;
struct StreamFreshnessStamp;

void stream_detach_file(File* file);
void stream_invalidate_file(File* file);
void cache_notify_detach_file(File* file);
void cache_notify_file_data_changed_impl(File* file);
void cache_notify_file_metadata_changed_impl(File* file);
auto cache_notify_file_dirty_impl(File* file) -> bool;
void stream_invalidate_identity_locked(const StreamCacheIdentity& identity);
void stream_gc_locked(uint64_t now_us);
auto vfs_stream_cache_try_read(File* file, void* buf, size_t count, uint64_t start_offset, size_t* actual_size, ssize_t* result) -> bool;
auto vfs_stream_cache_get_file_stat(File* file, Stat* statbuf) -> int;
auto stream_build_identity(File* file, const Stat& statbuf, StreamCacheIdentity* identity, StreamFreshnessStamp* stamp,
                           bool* can_reuse_detached, bool* retain_full_file) -> int;

auto metadata_hash_mix(uint64_t value) -> uint64_t {
    value ^= value >> 33U;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33U;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33U;
    return value;
}

auto metadata_hash_path(const char* path, size_t len, bool follow_final_symlink, bool require_directory, FSType fs_type, uint64_t dev_id)
    -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= static_cast<unsigned char>(path[i]);
        hash *= 1099511628211ULL;
    }
    hash ^= follow_final_symlink ? 0x9e3779b97f4a7c15ULL : 0x517cc1b727220a95ULL;
    hash = metadata_hash_mix(hash ^ (require_directory ? 0x94d049bb133111ebULL : 0x2545f4914f6cdd1dULL));
    hash = metadata_hash_mix(hash ^ (static_cast<uint64_t>(fs_type) << 56U) ^ dev_id);
    return hash == 0 ? 1 : hash;
}

auto metadata_invalidation_hash_path(const char* path, size_t len) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= static_cast<unsigned char>(path[i]);
        hash *= 1099511628211ULL;
    }
    return hash == 0 ? 1 : hash;
}

auto advisory_hash_path(const char* path) -> uint64_t {
    if (path == nullptr) {
        return 0;
    }
    uint64_t hash = 1469598103934665603ULL;
    for (const char* p = path; *p != '\0'; ++p) {
        hash ^= static_cast<unsigned char>(*p);
        hash *= 1099511628211ULL;
    }
    return hash == 0 ? 1 : hash;
}

auto advisory_user_copy(uint64_t user_addr, void* kernel_buf, size_t size, bool to_user) -> int {
    if (size == 0) {
        return 0;
    }
    if (user_addr == 0 || kernel_buf == nullptr) {
        return -EFAULT;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return -EFAULT;
    }

    auto* kernel_bytes = static_cast<uint8_t*>(kernel_buf);
    for (size_t i = 0; i < size; ++i) {
        uint64_t const PHYS = ker::mod::mm::virt::translate(task->pagemap, user_addr + i);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            return -EFAULT;
        }
        auto* user_byte = reinterpret_cast<uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
        if (to_user) {
            *user_byte = kernel_bytes[i];
        } else {
            kernel_bytes[i] = *user_byte;
        }
    }
    return 0;
}

auto advisory_copy_from_user(uint64_t user_addr, VfsFlockAbi& lock) -> int {
    return advisory_user_copy(user_addr, &lock, sizeof(lock), false);
}

auto advisory_copy_to_user(uint64_t user_addr, const VfsFlockAbi& lock) -> int {
    auto copy = lock;
    return advisory_user_copy(user_addr, &copy, sizeof(copy), true);
}

auto advisory_build_key(File* file, AdvisoryFileKey& key, Stat* stat_out = nullptr, bool allow_backend_stat = true) -> int {
    if (file == nullptr) {
        return -EBADF;
    }

    key = {};
    key.fs_type = file->fs_type;
    key.anonymous_file = file;
    if (file->vfs_path != nullptr) {
        size_t const PATH_LEN = std::strlen(file->vfs_path);
        if (PATH_LEN >= key.path.size()) {
            return -ENAMETOOLONG;
        }
        std::memcpy(key.path.data(), file->vfs_path, PATH_LEN + 1);
        key.path_hash = advisory_hash_path(key.path.data());
        key.has_path = true;
    }

    if (!allow_backend_stat) {
        if (stat_out != nullptr) {
            *stat_out = {};
        }
        return 0;
    }

    Stat st{};
    int const STAT_RET = vfs_fstat_file(file, &st);
    if (STAT_RET == 0) {
        if (stat_out != nullptr) {
            *stat_out = st;
        }
        if (file->fs_type != FSType::REMOTE && st.st_ino != 0) {
            key.dev = st.st_dev;
            key.ino = st.st_ino;
            key.has_inode = true;
        }
    } else if (stat_out != nullptr) {
        return STAT_RET;
    }
    return 0;
}

auto advisory_keys_equal(const AdvisoryFileKey& left, const AdvisoryFileKey& right) -> bool {
    if (left.has_inode && right.has_inode) {
        return left.dev == right.dev && left.ino == right.ino;
    }
    if (left.has_path && right.has_path) {
        return left.fs_type == right.fs_type && left.path_hash == right.path_hash && std::strcmp(left.path.data(), right.path.data()) == 0;
    }
    return left.anonymous_file != nullptr && left.anonymous_file == right.anonymous_file;
}

auto advisory_same_owner(const AdvisoryLock& lock, AdvisoryOwnerKind owner_kind, uint64_t owner_pid, const File* owner_file) -> bool {
    if (lock.owner_kind != owner_kind) {
        return false;
    }
    if (owner_kind == AdvisoryOwnerKind::PROCESS) {
        return lock.owner_pid == owner_pid;
    }
    return lock.owner_file == owner_file;
}

auto advisory_ranges_overlap(uint64_t left_start, uint64_t left_end, uint64_t right_start, uint64_t right_end) -> bool {
    return left_start < right_end && right_start < left_end;
}

auto advisory_ranges_touch_or_overlap(uint64_t left_start, uint64_t left_end, uint64_t right_start, uint64_t right_end) -> bool {
    return left_start <= right_end && right_start <= left_end;
}

auto advisory_locks_conflict(const AdvisoryLock& held, const AdvisoryLock& requested) -> bool {
    if (held.family != requested.family) {
        return false;
    }
    if (!advisory_keys_equal(held.key, requested.key)) {
        return false;
    }
    if (advisory_same_owner(held, requested.owner_kind, requested.owner_pid, requested.owner_file)) {
        return false;
    }
    if (!advisory_ranges_overlap(held.start, held.end, requested.start, requested.end)) {
        return false;
    }
    return held.type == AdvisoryLockType::WRITE || requested.type == AdvisoryLockType::WRITE;
}

void advisory_remove_lock_at(size_t index) {
    auto it = g_advisory_locks.begin();
    for (size_t i = 0; i < index; ++i) {
        ++it;
    }
    g_advisory_locks.erase(it);
}

void advisory_unlock_owned_range_locked(const AdvisoryFileKey& key, AdvisoryLockFamily family, AdvisoryOwnerKind owner_kind,
                                        uint64_t owner_pid, const File* owner_file, uint64_t start, uint64_t end) {
    for (size_t i = 0; i < g_advisory_locks.size();) {
        AdvisoryLock& lock = g_advisory_locks.at(i);
        if (lock.family != family || !advisory_keys_equal(lock.key, key) || !advisory_same_owner(lock, owner_kind, owner_pid, owner_file) ||
            !advisory_ranges_overlap(lock.start, lock.end, start, end)) {
            ++i;
            continue;
        }

        if (start <= lock.start && end >= lock.end) {
            advisory_remove_lock_at(i);
            continue;
        }
        if (start <= lock.start) {
            lock.start = end;
            ++i;
            continue;
        }
        if (end >= lock.end) {
            lock.end = start;
            ++i;
            continue;
        }

        AdvisoryLock right = lock;
        right.start = end;
        lock.end = start;
        g_advisory_locks.push_back(right);
        ++i;
    }
}

void advisory_insert_owned_lock_locked(AdvisoryLock lock) {
    advisory_unlock_owned_range_locked(lock.key, lock.family, lock.owner_kind, lock.owner_pid, lock.owner_file, lock.start, lock.end);

    for (size_t i = 0; i < g_advisory_locks.size();) {
        AdvisoryLock& existing = g_advisory_locks.at(i);
        if (existing.family != lock.family || !advisory_keys_equal(existing.key, lock.key) ||
            !advisory_same_owner(existing, lock.owner_kind, lock.owner_pid, lock.owner_file) || existing.type != lock.type ||
            !advisory_ranges_touch_or_overlap(existing.start, existing.end, lock.start, lock.end)) {
            ++i;
            continue;
        }

        lock.start = std::min(lock.start, existing.start);
        lock.end = std::max(lock.end, existing.end);
        advisory_remove_lock_at(i);
    }

    g_advisory_locks.push_back(lock);
}

auto advisory_find_conflict_locked(const AdvisoryLock& requested, AdvisoryLock* conflict_out = nullptr) -> bool {
    for (const auto& held : g_advisory_locks) {
        if (!advisory_locks_conflict(held, requested)) {
            continue;
        }
        if (conflict_out != nullptr) {
            *conflict_out = held;
        }
        return true;
    }
    return false;
}

auto advisory_process_has_locks_locked(uint64_t pid) -> bool {
    for (const auto& lock : g_advisory_locks) {
        if (lock.owner_kind == AdvisoryOwnerKind::PROCESS && lock.owner_pid == pid) {
            return true;
        }
    }
    return false;
}

auto advisory_process_has_inode_locks_locked(uint64_t pid) -> bool {
    for (const auto& lock : g_advisory_locks) {
        if (lock.owner_kind == AdvisoryOwnerKind::PROCESS && lock.owner_pid == pid && lock.key.has_inode) {
            return true;
        }
    }
    return false;
}

void advisory_release_process_locks_by_key_locked(uint64_t pid, const AdvisoryFileKey& key) {
    for (size_t i = 0; i < g_advisory_locks.size();) {
        const AdvisoryLock& lock = g_advisory_locks.at(i);
        if (lock.owner_kind == AdvisoryOwnerKind::PROCESS && lock.owner_pid == pid && advisory_keys_equal(lock.key, key)) {
            advisory_remove_lock_at(i);
            continue;
        }
        ++i;
    }
}

auto advisory_to_lock_type(int16_t flock_type, AdvisoryLockType& out) -> int {
    switch (flock_type) {
        case F_RDLCK_TYPE:
            out = AdvisoryLockType::READ;
            return 0;
        case F_WRLCK_TYPE:
            out = AdvisoryLockType::WRITE;
            return 0;
        default:
            return -EINVAL;
    }
}

auto advisory_range_from_flock(const VfsFlockAbi& flock, File* file, const Stat& stat, uint64_t& start, uint64_t& end) -> int {
    constexpr auto ADVISORY_RANGE_EOF_WIDE = static_cast<__int128>(ADVISORY_RANGE_EOF);
    __int128 base = 0;
    switch (flock.l_whence) {
        case SEEK_SET_VALUE:
            base = 0;
            break;
        case SEEK_CUR_VALUE:
            base = file != nullptr ? file->pos : 0;
            break;
        case SEEK_END_VALUE:
            base = stat.st_size;
            break;
        default:
            return -EINVAL;
    }

    __int128 range_start = base + static_cast<__int128>(flock.l_start);
    __int128 range_end = 0;
    if (flock.l_len == 0) {
        range_end = ADVISORY_RANGE_EOF_WIDE;
    } else if (flock.l_len > 0) {
        range_end = range_start + static_cast<__int128>(flock.l_len);
    } else {
        range_end = range_start;
        range_start += static_cast<__int128>(flock.l_len);
    }

    if (range_start < 0 || range_end < 0 || range_end < range_start || range_start > ADVISORY_RANGE_EOF_WIDE) {
        return -EINVAL;
    }
    start = static_cast<uint64_t>(range_start);
    if (range_end > ADVISORY_RANGE_EOF_WIDE) {
        end = ADVISORY_RANGE_EOF;
    } else {
        end = static_cast<uint64_t>(range_end);
    }
    return start < end ? 0 : -EINVAL;
}

auto advisory_set_lock(File* file, uint64_t owner_pid, AdvisoryOwnerKind owner_kind, AdvisoryLockFamily family, const VfsFlockAbi& flock,
                       bool wait) -> int {
    AdvisoryLockType lock_type{};
    if (flock.l_type != F_UNLCK_TYPE) {
        if (int const TYPE_RET = advisory_to_lock_type(flock.l_type, lock_type); TYPE_RET < 0) {
            return TYPE_RET;
        }
    }

    Stat stat{};
    bool const NEEDS_STAT_FOR_RANGE = flock.l_whence == SEEK_END_VALUE;
    bool const ALLOW_BACKEND_STAT = file->fs_type != FSType::REMOTE || NEEDS_STAT_FOR_RANGE;
    AdvisoryFileKey key{};
    if (int const KEY_RET = advisory_build_key(file, key, NEEDS_STAT_FOR_RANGE ? &stat : nullptr, ALLOW_BACKEND_STAT); KEY_RET < 0) {
        return KEY_RET;
    }

    uint64_t start = 0;
    uint64_t end = ADVISORY_RANGE_EOF;
    if (int const RANGE_RET = advisory_range_from_flock(flock, file, stat, start, end); RANGE_RET < 0) {
        return RANGE_RET;
    }

    const File* owner_file = owner_kind == AdvisoryOwnerKind::OPEN_FILE ? file : nullptr;
    if (flock.l_type == F_UNLCK_TYPE) {
        g_advisory_lock_mutex.lock();
        advisory_unlock_owned_range_locked(key, family, owner_kind, owner_pid, owner_file, start, end);
        g_advisory_lock_mutex.unlock();
        return 0;
    }

    AdvisoryLock requested{};
    requested.key = key;
    requested.family = family;
    requested.owner_kind = owner_kind;
    requested.owner_pid = owner_pid;
    requested.owner_file = owner_file;
    requested.type = lock_type;
    requested.start = start;
    requested.end = end;

    for (;;) {
        g_advisory_lock_mutex.lock();
        bool const HAS_CONFLICT = advisory_find_conflict_locked(requested);
        if (!HAS_CONFLICT) {
            advisory_insert_owned_lock_locked(requested);
            g_advisory_lock_mutex.unlock();
            return 0;
        }
        g_advisory_lock_mutex.unlock();

        if (!wait) {
            return -EAGAIN;
        }

        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr && task->has_interrupting_signal_pending()) {
            return -EINTR;
        }
        ker::mod::sched::kern_yield();
    }
}

auto advisory_get_lock(File* file, uint64_t owner_pid, AdvisoryOwnerKind owner_kind, AdvisoryLockFamily family, VfsFlockAbi& flock) -> int {
    AdvisoryLockType lock_type{};
    if (flock.l_type == F_UNLCK_TYPE) {
        flock.l_pid = 0;
        return 0;
    }
    if (int const TYPE_RET = advisory_to_lock_type(flock.l_type, lock_type); TYPE_RET < 0) {
        return TYPE_RET;
    }

    Stat stat{};
    bool const NEEDS_STAT_FOR_RANGE = flock.l_whence == SEEK_END_VALUE;
    bool const ALLOW_BACKEND_STAT = file->fs_type != FSType::REMOTE || NEEDS_STAT_FOR_RANGE;
    AdvisoryFileKey key{};
    if (int const KEY_RET = advisory_build_key(file, key, NEEDS_STAT_FOR_RANGE ? &stat : nullptr, ALLOW_BACKEND_STAT); KEY_RET < 0) {
        return KEY_RET;
    }

    uint64_t start = 0;
    uint64_t end = ADVISORY_RANGE_EOF;
    if (int const RANGE_RET = advisory_range_from_flock(flock, file, stat, start, end); RANGE_RET < 0) {
        return RANGE_RET;
    }

    AdvisoryLock requested{};
    requested.key = key;
    requested.family = family;
    requested.owner_kind = owner_kind;
    requested.owner_pid = owner_pid;
    requested.owner_file = owner_kind == AdvisoryOwnerKind::OPEN_FILE ? file : nullptr;
    requested.type = lock_type;
    requested.start = start;
    requested.end = end;

    AdvisoryLock conflict{};
    g_advisory_lock_mutex.lock();
    bool const HAS_CONFLICT = advisory_find_conflict_locked(requested, &conflict);
    g_advisory_lock_mutex.unlock();

    if (!HAS_CONFLICT) {
        flock.l_type = F_UNLCK_TYPE;
        flock.l_pid = 0;
        return 0;
    }

    flock.l_type = conflict.type == AdvisoryLockType::READ ? F_RDLCK_TYPE : F_WRLCK_TYPE;
    flock.l_whence = SEEK_SET_VALUE;
    flock.l_start = static_cast<off_t>(std::min<uint64_t>(conflict.start, static_cast<uint64_t>(INT64_MAX)));
    flock.l_len = conflict.end == ADVISORY_RANGE_EOF ? 0 : static_cast<off_t>(conflict.end - conflict.start);
    flock.l_pid = conflict.owner_kind == AdvisoryOwnerKind::PROCESS ? static_cast<int32_t>(conflict.owner_pid) : -1;
    return 0;
}

auto advisory_flock(File* file, int options) -> int {
    if ((options & ~(LOCK_SH_VALUE | LOCK_EX_VALUE | LOCK_NB_VALUE | LOCK_UN_VALUE)) != 0) {
        return -EINVAL;
    }
    int const MODE_COUNT =
        ((options & LOCK_SH_VALUE) != 0 ? 1 : 0) + ((options & LOCK_EX_VALUE) != 0 ? 1 : 0) + ((options & LOCK_UN_VALUE) != 0 ? 1 : 0);
    if (MODE_COUNT != 1) {
        return -EINVAL;
    }

    VfsFlockAbi flock{};
    if ((options & LOCK_UN_VALUE) != 0) {
        flock.l_type = F_UNLCK_TYPE;
    } else if ((options & LOCK_EX_VALUE) != 0) {
        flock.l_type = F_WRLCK_TYPE;
    } else {
        flock.l_type = F_RDLCK_TYPE;
    }
    flock.l_whence = SEEK_SET_VALUE;
    flock.l_start = 0;
    flock.l_len = 0;

    bool const WAIT = (options & LOCK_NB_VALUE) == 0;
    return advisory_set_lock(file, 0, AdvisoryOwnerKind::OPEN_FILE, AdvisoryLockFamily::FLOCK, flock, WAIT);
}

void advisory_release_file_owner_locks(const File* file) {
    if (file == nullptr) {
        return;
    }
    g_advisory_lock_mutex.lock();
    for (size_t i = 0; i < g_advisory_locks.size();) {
        const AdvisoryLock& lock = g_advisory_locks.at(i);
        if (lock.owner_kind == AdvisoryOwnerKind::OPEN_FILE && lock.owner_file == file) {
            advisory_remove_lock_at(i);
            continue;
        }
        ++i;
    }
    g_advisory_lock_mutex.unlock();
}

void advisory_release_process_locks_for_file(uint64_t pid, File* file) {
    if (file == nullptr) {
        return;
    }

    g_advisory_lock_mutex.lock();
    bool const HAS_PROCESS_LOCKS = advisory_process_has_locks_locked(pid);
    g_advisory_lock_mutex.unlock();
    if (!HAS_PROCESS_LOCKS) {
        return;
    }

    AdvisoryFileKey cheap_key{};
    if (advisory_build_key(file, cheap_key, nullptr, false) < 0) {
        return;
    }

    g_advisory_lock_mutex.lock();
    advisory_release_process_locks_by_key_locked(pid, cheap_key);
    bool const NEEDS_INODE_RELEASE = file->fs_type != FSType::REMOTE && advisory_process_has_inode_locks_locked(pid);
    g_advisory_lock_mutex.unlock();

    if (!NEEDS_INODE_RELEASE) {
        return;
    }

    AdvisoryFileKey inode_key{};
    if (advisory_build_key(file, inode_key) < 0) {
        return;
    }

    g_advisory_lock_mutex.lock();
    advisory_release_process_locks_by_key_locked(pid, inode_key);
    g_advisory_lock_mutex.unlock();
}

auto metadata_normalized_path_len(const char* path) -> size_t {
    if (path == nullptr || path[0] != '/') {
        return 0;
    }
    size_t len = std::strlen(path);
    while (len > 1 && path[len - 1] == '/') {
        --len;
    }
    return len;
}

auto metadata_parent_path_len(const char* path, size_t path_len) -> size_t {
    if (path == nullptr || path_len == 0 || path[0] != '/') {
        return 0;
    }
    if (path_len <= 1) {
        return 1;
    }

    size_t slash = path_len - 1;
    while (slash > 0 && path[slash] != '/') {
        --slash;
    }
    return slash == 0 ? 1 : slash;
}

void metadata_invalidation_clear_locked() {
    for (auto& set : g_metadata_subtree_invalidations) {
        set.overflow_generation = 0;
        for (auto& entry : set.ways) {
            entry.valid = false;
        }
    }
    for (auto& set : g_metadata_exact_invalidations) {
        set.overflow_generation = 0;
        for (auto& entry : set.ways) {
            entry.valid = false;
        }
    }
}

void metadata_cache_bump_generation_locked() {
    g_metadata_cache_generation.fetch_add(1, std::memory_order_acq_rel);
    g_vfs_metadata_generation_resets.fetch_add(1, std::memory_order_relaxed);
    metadata_invalidation_clear_locked();
}

void metadata_invalidation_store_locked(std::array<MetadataInvalidationSet, METADATA_INVALIDATION_SET_COUNT>& table, uint64_t path_hash,
                                        uint64_t generation) {
    auto& set = table.at(path_hash & (METADATA_INVALIDATION_SET_COUNT - 1));
    MetadataInvalidationEntry* free_entry = nullptr;
    for (auto& entry : set.ways) {
        if (entry.valid && entry.path_hash == path_hash) {
            entry.generation = generation;
            return;
        }
        if (!entry.valid && free_entry == nullptr) {
            free_entry = &entry;
        }
    }
    if (free_entry == nullptr) {
        set.overflow_generation = std::max(set.overflow_generation, generation);
        return;
    }

    free_entry->path_hash = path_hash;
    free_entry->generation = generation;
    free_entry->valid = true;
}

auto metadata_invalidation_generation_for_hash_locked(const std::array<MetadataInvalidationSet, METADATA_INVALIDATION_SET_COUNT>& table,
                                                      uint64_t path_hash) -> uint64_t {
    auto const& set = table.at(path_hash & (METADATA_INVALIDATION_SET_COUNT - 1));
    uint64_t generation = set.overflow_generation;
    for (auto const& entry : set.ways) {
        if (entry.valid && entry.path_hash == path_hash) {
            generation = std::max(generation, entry.generation);
        }
    }
    return generation;
}

void metadata_cache_note_one_path_changed_locked(const char* path) {
    size_t const PATH_LEN = metadata_normalized_path_len(path);
    if (PATH_LEN == 0) {
        return;
    }
    g_vfs_metadata_path_invalidations.fetch_add(1, std::memory_order_relaxed);
    if (PATH_LEN == 1) {
        metadata_cache_bump_generation_locked();
        return;
    }

    uint64_t const PATH_GENERATION = g_metadata_invalidation_generation.fetch_add(1, std::memory_order_acq_rel) + 1;
    uint64_t const PATH_HASH = metadata_invalidation_hash_path(path, PATH_LEN);
    metadata_invalidation_store_locked(g_metadata_subtree_invalidations, PATH_HASH, PATH_GENERATION);

    size_t const PARENT_LEN = metadata_parent_path_len(path, PATH_LEN);
    if (PARENT_LEN == 0) {
        return;
    }
    uint64_t const PARENT_GENERATION = g_metadata_invalidation_generation.fetch_add(1, std::memory_order_acq_rel) + 1;
    uint64_t const PARENT_HASH = metadata_invalidation_hash_path(path, PARENT_LEN);
    metadata_invalidation_store_locked(g_metadata_exact_invalidations, PARENT_HASH, PARENT_GENERATION);
}

void metadata_cache_note_path_changed(const char* old_path, const char* new_path) {
    if (old_path == nullptr && new_path == nullptr) {
        return;
    }

    g_metadata_cache_epoch.fetch_add(1, std::memory_order_acq_rel);
    g_metadata_invalidation_lock.lock();
    metadata_cache_note_one_path_changed_locked(old_path);
    if (new_path != nullptr && (old_path == nullptr || std::strcmp(old_path, new_path) != 0)) {
        metadata_cache_note_one_path_changed_locked(new_path);
    }
    g_metadata_invalidation_lock.unlock();
}

auto metadata_path_invalidated_since(const char* path, size_t path_len, uint64_t seen_generation) -> bool {
    if (path == nullptr || path_len == 0 || path[0] != '/') {
        return true;
    }
    if (seen_generation == g_metadata_invalidation_generation.load(std::memory_order_acquire)) {
        return false;
    }

    bool invalidated = false;
    g_metadata_invalidation_lock.lock();

    uint64_t const EXACT_HASH = metadata_invalidation_hash_path(path, path_len);
    invalidated = metadata_invalidation_generation_for_hash_locked(g_metadata_exact_invalidations, EXACT_HASH) > seen_generation;

    size_t prefix_len = 1;
    while (!invalidated && prefix_len < path_len) {
        ++prefix_len;
        while (prefix_len < path_len && path[prefix_len] != '/') {
            ++prefix_len;
        }
        uint64_t const PREFIX_HASH = metadata_invalidation_hash_path(path, prefix_len);
        invalidated = metadata_invalidation_generation_for_hash_locked(g_metadata_subtree_invalidations, PREFIX_HASH) > seen_generation;
    }

    g_metadata_invalidation_lock.unlock();
    return invalidated;
}

auto metadata_cacheable_fs(FSType fs_type) -> bool {
    return fs_type == FSType::TMPFS || fs_type == FSType::FAT32 || fs_type == FSType::XFS;
}

void metadata_cache_note_observation_store() { g_metadata_observation_epoch.fetch_add(1, std::memory_order_acq_rel); }

auto metadata_cache_note_exact_path_changed(const char* path) -> bool {
    size_t const PATH_LEN = metadata_normalized_path_len(path);
    if (PATH_LEN == 0) {
        return false;
    }

    g_metadata_cache_epoch.fetch_add(1, std::memory_order_acq_rel);
    uint64_t const PATH_GENERATION = g_metadata_invalidation_generation.fetch_add(1, std::memory_order_acq_rel) + 1;
    uint64_t const PATH_HASH = metadata_invalidation_hash_path(path, PATH_LEN);

    g_metadata_invalidation_lock.lock();
    metadata_invalidation_store_locked(g_metadata_exact_invalidations, PATH_HASH, PATH_GENERATION);
    g_metadata_invalidation_lock.unlock();
    return true;
}

auto metadata_cache_has_path_variant(const char* path, size_t path_len, FSType fs_type, uint64_t dev_id, bool follow_final_symlink,
                                     bool require_directory) -> bool {
    uint64_t const EPOCH = g_metadata_cache_generation.load(std::memory_order_acquire);
    uint64_t const HASH = metadata_hash_path(path, path_len, follow_final_symlink, require_directory, fs_type, dev_id);
    auto& set = g_metadata_cache[HASH & (METADATA_CACHE_SET_COUNT - 1)];

    ker::mod::sys::MutexGuard guard(set.lock);
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.epoch != EPOCH || entry.hash != HASH || entry.path_len != path_len || entry.fs_type != fs_type ||
            entry.dev_id != dev_id || entry.follow_final_symlink != follow_final_symlink || entry.require_directory != require_directory) {
            continue;
        }
        if (std::memcmp(entry.path.data(), path, path_len + 1) != 0) {
            continue;
        }
        if (metadata_path_invalidated_since(entry.path.data(), entry.path_len, entry.invalidation_generation)) {
            entry.valid = false;
            continue;
        }
        return true;
    }
    return false;
}

auto metadata_cache_has_file_data_observation(File* file) -> bool {
    if (file == nullptr || file->vfs_path == nullptr) {
        return true;
    }

    size_t const PATH_LEN = metadata_normalized_path_len(file->vfs_path);
    if (PATH_LEN == 0 || PATH_LEN >= MAX_PATH_LEN) {
        return true;
    }

    MountRef mount_ref = find_mount_point(file->vfs_path);
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr || !metadata_cacheable_fs(mount->fs_type)) {
        return true;
    }

    return metadata_cache_has_path_variant(file->vfs_path, PATH_LEN, mount->fs_type, mount->dev_id, true, false) ||
           metadata_cache_has_path_variant(file->vfs_path, PATH_LEN, mount->fs_type, mount->dev_id, false, false);
}

void metadata_cache_note_file_data_changed(File* file) {
    if (file == nullptr || file->vfs_path == nullptr || !metadata_cacheable_fs(file->fs_type)) {
        return;
    }

    uint64_t const OBSERVED_EPOCH = g_metadata_observation_epoch.load(std::memory_order_acquire);
    if (file->metadata_data_invalidation_observation_epoch == OBSERVED_EPOCH) {
        return;
    }
    if (!metadata_cache_has_file_data_observation(file)) {
        file->metadata_data_invalidation_observation_epoch = OBSERVED_EPOCH;
        return;
    }

    if (metadata_cache_note_exact_path_changed(file->vfs_path)) {
        file->metadata_data_invalidation_observation_epoch = OBSERVED_EPOCH;
    }
}

void metadata_cache_mark_file_data_observed(File* file) {
    if (file == nullptr) {
        return;
    }
    file->metadata_data_invalidation_observation_epoch = g_metadata_observation_epoch.load(std::memory_order_acquire);
}

void metadata_cache_note_path_data_changed(const char* path, FSType fs_type) {
    if (path == nullptr || (!metadata_cacheable_fs(fs_type) && fs_type != FSType::DEVFS)) {
        return;
    }

    static_cast<void>(metadata_cache_note_exact_path_changed(path));
}

auto metadata_cacheable_result(int result) -> bool { return result == 0 || result == -ENOENT; }

auto symlink_cacheable_fs(FSType fs_type) -> bool { return fs_type == FSType::TMPFS || fs_type == FSType::FAT32 || fs_type == FSType::XFS; }

auto symlink_cacheable_result(ssize_t result) -> bool { return result >= 0 || result == -EINVAL || result == -ENOSYS || result == -ENOENT; }

auto file_stat_snapshot_anonymous_cacheable(const File* file) -> bool {
    if (file == nullptr || file->vfs_path != nullptr || (file->open_flags & ker::vfs::O_NO_CACHE) != 0) {
        return false;
    }
    return file->fs_type == FSType::SOCKET || file->fs_type == FSType::DEVFS ||
           (file->fs_type == FSType::TMPFS && file->fops != nullptr && file->fops != ker::vfs::tmpfs::get_tmpfs_fops());
}

auto file_stat_snapshot_path_cacheable_fs(FSType fs_type) -> bool {
    return metadata_cacheable_fs(fs_type) || fs_type == FSType::DEVFS || fs_type == FSType::REMOTE;
}

auto file_stat_snapshot_cacheable(const File* file) -> bool {
    if (file == nullptr || (file->open_flags & ker::vfs::O_NO_CACHE) != 0) {
        return false;
    }
    if (file->vfs_path == nullptr) {
        return file_stat_snapshot_anonymous_cacheable(file);
    }
    return file_stat_snapshot_path_cacheable_fs(file->fs_type);
}

void file_stat_snapshot_record_uncacheable_miss(const File* file, const Stat* statbuf) {
    g_vfs_fstat_snapshot_miss_uncacheable.fetch_add(1, std::memory_order_relaxed);
    if (file == nullptr || statbuf == nullptr) {
        g_vfs_fstat_snapshot_miss_bad_args.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if ((file->open_flags & ker::vfs::O_NO_CACHE) != 0) {
        g_vfs_fstat_snapshot_miss_no_cache.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (file->vfs_path == nullptr) {
        g_vfs_fstat_snapshot_miss_pathless.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    g_vfs_fstat_snapshot_miss_fs.fetch_add(1, std::memory_order_relaxed);
}

auto file_stat_snapshot_prefetchable(const File* file) -> bool {
    if (!file_stat_snapshot_cacheable(file)) {
        return false;
    }
    if ((file->open_flags & (ker::vfs::O_TRUNC | ker::vfs::O_CREAT)) != 0) {
        return false;
    }
    if ((file->open_flags & 3) != 0) {
        return false;
    }
    return true;
}

auto file_stat_snapshot_mode_cacheable(mode_t mode) -> bool {
    mode_t const TYPE = mode & static_cast<mode_t>(S_IFMT);
    return TYPE == static_cast<mode_t>(S_IFREG) || TYPE == static_cast<mode_t>(S_IFDIR);
}

auto file_stat_snapshot_devfs_mode_cacheable(mode_t mode) -> bool {
    mode_t const TYPE = mode & static_cast<mode_t>(S_IFMT);
    return TYPE == static_cast<mode_t>(S_IFCHR) || TYPE == static_cast<mode_t>(S_IFBLK) || TYPE == static_cast<mode_t>(S_IFDIR) ||
           TYPE == static_cast<mode_t>(S_IFLNK);
}

auto file_stat_snapshot_result_cacheable(const File* file, mode_t mode) -> bool {
    if (file_stat_snapshot_mode_cacheable(mode)) {
        return true;
    }
    if (file != nullptr && file->fs_type == FSType::DEVFS) {
        return file_stat_snapshot_devfs_mode_cacheable(mode);
    }
    if (!file_stat_snapshot_anonymous_cacheable(file)) {
        return false;
    }
    mode_t const TYPE = mode & static_cast<mode_t>(S_IFMT);
    return TYPE == static_cast<mode_t>(S_IFIFO) || TYPE == static_cast<mode_t>(S_IFSOCK);
}

struct MetadataSnapshotStamp {
    uint64_t cache_generation = 0;
    uint64_t invalidation_generation = 0;
};

auto metadata_snapshot_stamp() -> MetadataSnapshotStamp {
    return MetadataSnapshotStamp{
        .cache_generation = g_metadata_cache_generation.load(std::memory_order_acquire),
        .invalidation_generation = g_metadata_invalidation_generation.load(std::memory_order_acquire),
    };
}

void file_stat_snapshot_invalidate(File* file) {
    if (file != nullptr) {
        file->stat_cache_valid = false;
    }
}

void file_stat_snapshot_store(File* file, const Stat& statbuf, MetadataSnapshotStamp stamp) {
    if (!file_stat_snapshot_cacheable(file) || !file_stat_snapshot_result_cacheable(file, statbuf.st_mode)) {
        file_stat_snapshot_invalidate(file);
        return;
    }
    if (file->vfs_path == nullptr) {
        file->stat_cache = statbuf;
        file->stat_cache_generation = 0;
        file->stat_cache_invalidation_generation = 0;
        file->stat_cache_path_len = 0;
        file->stat_cache_valid = true;
        g_vfs_fstat_snapshot_stores.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    size_t const PATH_LEN = metadata_normalized_path_len(file->vfs_path);
    if (PATH_LEN == 0) {
        file_stat_snapshot_invalidate(file);
        return;
    }
    file->stat_cache = statbuf;
    file->stat_cache_generation = stamp.cache_generation;
    file->stat_cache_invalidation_generation = stamp.invalidation_generation;
    file->stat_cache_path_len = PATH_LEN;
    file->stat_cache_valid = true;
    metadata_cache_note_observation_store();
    g_vfs_fstat_snapshot_stores.fetch_add(1, std::memory_order_relaxed);
}

auto file_stat_snapshot_current(File* file) -> bool {
    if (!file_stat_snapshot_cacheable(file) || !file->stat_cache_valid) {
        return false;
    }
    if (cache_notify_file_dirty_impl(file)) {
        return false;
    }
    if (file->vfs_path == nullptr) {
        return true;
    }
    return file->stat_cache_generation == g_metadata_cache_generation.load(std::memory_order_acquire) && file->stat_cache_path_len != 0 &&
           !metadata_path_invalidated_since(file->vfs_path, file->stat_cache_path_len, file->stat_cache_invalidation_generation);
}

auto file_stat_snapshot_promote_prefilled_path(File* file) -> bool {
    if (file == nullptr || file->vfs_path == nullptr || !file_stat_snapshot_prefetchable(file) || !file->stat_cache_valid ||
        file->stat_cache_path_len != 0) {
        return false;
    }

    size_t const PATH_LEN = metadata_normalized_path_len(file->vfs_path);
    if (PATH_LEN == 0) {
        file_stat_snapshot_invalidate(file);
        return false;
    }
    if (file->stat_cache_generation != g_metadata_cache_generation.load(std::memory_order_acquire) ||
        metadata_path_invalidated_since(file->vfs_path, PATH_LEN, file->stat_cache_invalidation_generation)) {
        file_stat_snapshot_invalidate(file);
        return false;
    }
    if (!file_stat_snapshot_result_cacheable(file, file->stat_cache.st_mode)) {
        file_stat_snapshot_invalidate(file);
        return false;
    }

    file->stat_cache_path_len = PATH_LEN;
    metadata_cache_note_observation_store();
    g_vfs_fstat_snapshot_stores.fetch_add(1, std::memory_order_relaxed);
    return true;
}

auto file_stat_snapshot_lookup(File* file, Stat* statbuf) -> bool {
    if (file == nullptr || statbuf == nullptr || !file_stat_snapshot_cacheable(file)) {
        file_stat_snapshot_record_uncacheable_miss(file, statbuf);
        g_vfs_fstat_snapshot_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (cache_notify_file_dirty_impl(file)) {
        file_stat_snapshot_invalidate(file);
        g_vfs_fstat_snapshot_miss_invalidated.fetch_add(1, std::memory_order_relaxed);
        g_vfs_fstat_snapshot_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (!file->stat_cache_valid) {
        g_vfs_fstat_snapshot_miss_empty.fetch_add(1, std::memory_order_relaxed);
        g_vfs_fstat_snapshot_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (file->vfs_path == nullptr) {
        *statbuf = file->stat_cache;
        g_vfs_fstat_snapshot_hits.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    if (file->stat_cache_generation != g_metadata_cache_generation.load(std::memory_order_acquire)) {
        g_vfs_fstat_snapshot_miss_generation.fetch_add(1, std::memory_order_relaxed);
        g_vfs_fstat_snapshot_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (file->stat_cache_path_len == 0 ||
        metadata_path_invalidated_since(file->vfs_path, file->stat_cache_path_len, file->stat_cache_invalidation_generation)) {
        g_vfs_fstat_snapshot_miss_invalidated.fetch_add(1, std::memory_order_relaxed);
        g_vfs_fstat_snapshot_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    *statbuf = file->stat_cache;
    g_vfs_fstat_snapshot_hits.fetch_add(1, std::memory_order_relaxed);
    return true;
}

auto file_stat_snapshot_refresh_from_backend(File* file, Stat* statbuf) -> int {
    if (file == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    MetadataSnapshotStamp const STAMP = metadata_snapshot_stamp();
    int const RESULT = vfs_stream_cache_get_file_stat(file, statbuf);
    if (RESULT == 0) {
        file_stat_snapshot_store(file, *statbuf, STAMP);
    }
    return RESULT;
}

void file_stat_snapshot_refresh(File* file) {
    if (!file_stat_snapshot_prefetchable(file)) {
        return;
    }
    if (file_stat_snapshot_current(file)) {
        return;
    }
    if (file_stat_snapshot_promote_prefilled_path(file)) {
        return;
    }
    Stat statbuf{};
    static_cast<void>(file_stat_snapshot_refresh_from_backend(file, &statbuf));
}

auto metadata_cache_lookup(const char* path, FSType fs_type, uint64_t dev_id, bool follow_final_symlink, bool require_directory,
                           Stat* statbuf, bool record_miss = true) -> int {
    if (path == nullptr || statbuf == nullptr || !metadata_cacheable_fs(fs_type)) {
        return -EAGAIN;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN == 0 || PATH_LEN >= MAX_PATH_LEN) {
        return -EAGAIN;
    }

    uint64_t const EPOCH = g_metadata_cache_generation.load(std::memory_order_acquire);
    uint64_t const HASH = metadata_hash_path(path, PATH_LEN, follow_final_symlink, require_directory, fs_type, dev_id);
    auto& set = g_metadata_cache[HASH & (METADATA_CACHE_SET_COUNT - 1)];

    bool saw_valid = false;
    bool saw_stale_generation = false;
    bool saw_invalidated = false;
    bool cache_hit = false;
    int cache_result = -EAGAIN;
    {
        ker::mod::sys::MutexGuard guard(set.lock);
        for (auto& entry : set.ways) {
            if (!entry.valid) {
                continue;
            }
            saw_valid = true;
            if (entry.hash != HASH || entry.path_len != PATH_LEN || entry.fs_type != fs_type || entry.dev_id != dev_id ||
                entry.follow_final_symlink != follow_final_symlink || entry.require_directory != require_directory) {
                continue;
            }
            if (std::memcmp(entry.path.data(), path, PATH_LEN + 1) != 0) {
                continue;
            }
            if (entry.epoch != EPOCH) {
                saw_stale_generation = true;
                continue;
            }
            if (metadata_path_invalidated_since(entry.path.data(), entry.path_len, entry.invalidation_generation)) {
                entry.valid = false;
                saw_invalidated = true;
                continue;
            }

            entry.last_used = ++set.clock;
            cache_result = entry.result;
            if (cache_result == 0) {
                *statbuf = entry.stat;
            }
            cache_hit = true;
            break;
        }
    }
    if (cache_hit) {
        g_vfs_metadata_hits.fetch_add(1, std::memory_order_relaxed);
        return cache_result;
    }
    if (record_miss) {
        g_vfs_metadata_misses.fetch_add(1, std::memory_order_relaxed);
        if (saw_invalidated) {
            g_vfs_metadata_miss_invalidated.fetch_add(1, std::memory_order_relaxed);
        } else if (saw_stale_generation) {
            g_vfs_metadata_miss_stale_generation.fetch_add(1, std::memory_order_relaxed);
        } else if (!saw_valid) {
            g_vfs_metadata_miss_empty.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_vfs_metadata_miss_conflict.fetch_add(1, std::memory_order_relaxed);
        }
    }
    return -EAGAIN;
}

void metadata_cache_store(const char* path, FSType fs_type, uint64_t dev_id, bool follow_final_symlink, bool require_directory, int result,
                          const Stat* statbuf, MetadataSnapshotStamp stamp) {
    if (path == nullptr || !metadata_cacheable_fs(fs_type) || !metadata_cacheable_result(result)) {
        return;
    }
    if (result == 0 && statbuf == nullptr) {
        return;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN == 0 || PATH_LEN >= MAX_PATH_LEN) {
        return;
    }

    uint64_t const EPOCH = g_metadata_cache_generation.load(std::memory_order_acquire);
    if (stamp.cache_generation != EPOCH || metadata_path_invalidated_since(path, PATH_LEN, stamp.invalidation_generation)) {
        return;
    }

    uint64_t const HASH = metadata_hash_path(path, PATH_LEN, follow_final_symlink, require_directory, fs_type, dev_id);
    auto& set = g_metadata_cache[HASH & (METADATA_CACHE_SET_COUNT - 1)];

    {
        ker::mod::sys::MutexGuard guard(set.lock);
        uint64_t const USE_STAMP = ++set.clock;
        MetadataCacheEntry* victim = &set.ways.front();
        for (auto& entry : set.ways) {
            if (entry.valid && entry.epoch == EPOCH && entry.hash == HASH && entry.path_len == PATH_LEN && entry.fs_type == fs_type &&
                entry.dev_id == dev_id && entry.follow_final_symlink == follow_final_symlink &&
                entry.require_directory == require_directory && std::memcmp(entry.path.data(), path, PATH_LEN + 1) == 0) {
                victim = &entry;
                break;
            }
            if (!entry.valid || entry.epoch != EPOCH) {
                victim = &entry;
                break;
            }
            if (entry.last_used < victim->last_used) {
                victim = &entry;
            }
        }

        std::memcpy(victim->path.data(), path, PATH_LEN + 1);
        victim->stat = (result == 0) ? *statbuf : Stat{};
        victim->hash = HASH;
        victim->epoch = EPOCH;
        victim->last_used = USE_STAMP;
        victim->dev_id = dev_id;
        victim->invalidation_generation = stamp.invalidation_generation;
        victim->path_len = PATH_LEN;
        victim->result = result;
        victim->fs_type = fs_type;
        victim->follow_final_symlink = follow_final_symlink;
        victim->require_directory = require_directory;
        victim->valid = true;
    }
    metadata_cache_note_observation_store();
    g_vfs_metadata_stores.fetch_add(1, std::memory_order_relaxed);
}

auto metadata_cache_proves_final_not_symlink(const char* path, FSType fs_type, uint64_t dev_id) -> bool {
    Stat cached{};
    int const CACHED_RESULT = metadata_cache_lookup(path, fs_type, dev_id, false, false, &cached, false);
    if (CACHED_RESULT == -ENOENT) {
        return true;
    }
    if (CACHED_RESULT != 0) {
        return false;
    }
    return (cached.st_mode & static_cast<mode_t>(S_IFMT)) != static_cast<mode_t>(S_IFLNK);
}

auto symlink_hash_path(const char* path, size_t len, FSType fs_type, uint64_t dev_id) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= static_cast<unsigned char>(path[i]);
        hash *= 1099511628211ULL;
    }
    return metadata_hash_mix(hash ^ (static_cast<uint64_t>(fs_type) << 56U) ^ dev_id);
}

auto symlink_cache_lookup(const char* path, FSType fs_type, uint64_t dev_id, char* buf, size_t bufsize, ssize_t* out_result) -> bool {
    if (path == nullptr || buf == nullptr || bufsize == 0 || out_result == nullptr || !symlink_cacheable_fs(fs_type)) {
        return false;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN == 0 || PATH_LEN >= MAX_PATH_LEN) {
        return false;
    }

    uint64_t const EPOCH = g_metadata_cache_generation.load(std::memory_order_acquire);
    uint64_t const HASH = symlink_hash_path(path, PATH_LEN, fs_type, dev_id);
    auto& set = g_symlink_cache[HASH & (SYMLINK_CACHE_SET_COUNT - 1)];

    bool cache_hit = false;
    ssize_t cache_result = 0;
    {
        ker::mod::sys::MutexGuard guard(set.lock);
        for (auto& entry : set.ways) {
            if (!entry.valid || entry.epoch != EPOCH || entry.hash != HASH || entry.path_len != PATH_LEN || entry.fs_type != fs_type ||
                entry.dev_id != dev_id) {
                continue;
            }
            if (std::memcmp(entry.path.data(), path, PATH_LEN + 1) != 0) {
                continue;
            }
            if (metadata_path_invalidated_since(entry.path.data(), entry.path_len, entry.invalidation_generation)) {
                entry.valid = false;
                continue;
            }

            entry.last_used = ++set.clock;
            cache_result = entry.result;
            if (cache_result > 0) {
                size_t const TO_COPY = std::min<size_t>(entry.target_len, bufsize);
                std::memcpy(buf, entry.target.data(), TO_COPY);
                cache_result = static_cast<ssize_t>(TO_COPY);
            }
            cache_hit = true;
            break;
        }
    }
    if (cache_hit) {
        *out_result = cache_result;
        g_vfs_symlink_hits.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    g_vfs_symlink_misses.fetch_add(1, std::memory_order_relaxed);
    return false;
}

void symlink_cache_store(const char* path, FSType fs_type, uint64_t dev_id, ssize_t result, const char* target) {
    if (path == nullptr || !symlink_cacheable_fs(fs_type) || !symlink_cacheable_result(result)) {
        return;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN == 0 || PATH_LEN >= MAX_PATH_LEN) {
        return;
    }

    size_t target_len = 0;
    if (result > 0) {
        if (target == nullptr) {
            return;
        }
        target_len = static_cast<size_t>(result);
        if (target_len >= MAX_PATH_LEN) {
            return;
        }
    }

    uint64_t const EPOCH = g_metadata_cache_generation.load(std::memory_order_acquire);
    uint64_t const INVALIDATION_GENERATION = g_metadata_invalidation_generation.load(std::memory_order_acquire);
    uint64_t const HASH = symlink_hash_path(path, PATH_LEN, fs_type, dev_id);
    auto& set = g_symlink_cache[HASH & (SYMLINK_CACHE_SET_COUNT - 1)];

    {
        ker::mod::sys::MutexGuard guard(set.lock);
        uint64_t const USE_STAMP = ++set.clock;
        SymlinkCacheEntry* victim = &set.ways.front();
        for (auto& entry : set.ways) {
            if (entry.valid && entry.epoch == EPOCH && entry.hash == HASH && entry.path_len == PATH_LEN && entry.fs_type == fs_type &&
                entry.dev_id == dev_id && std::memcmp(entry.path.data(), path, PATH_LEN + 1) == 0) {
                victim = &entry;
                break;
            }
            if (!entry.valid || entry.epoch != EPOCH) {
                victim = &entry;
                break;
            }
            if (entry.last_used < victim->last_used) {
                victim = &entry;
            }
        }

        std::memcpy(victim->path.data(), path, PATH_LEN + 1);
        if (target_len > 0) {
            std::memcpy(victim->target.data(), target, target_len);
            victim->target[target_len] = '\0';
        }
        victim->hash = HASH;
        victim->epoch = EPOCH;
        victim->last_used = USE_STAMP;
        victim->dev_id = dev_id;
        victim->invalidation_generation = INVALIDATION_GENERATION;
        victim->path_len = PATH_LEN;
        victim->target_len = target_len;
        victim->result = result;
        victim->fs_type = fs_type;
        victim->valid = true;
    }
    g_vfs_symlink_stores.fetch_add(1, std::memory_order_relaxed);
}

void vfs_file_clear_path(File* file) {
    if (file == nullptr) {
        return;
    }
    if (file->vfs_path_heap_allocated) {
        delete[] file->vfs_path;
    }
    file->vfs_path = nullptr;
    file->vfs_path_heap_allocated = false;
    file->vfs_path_inline.at(0) = '\0';
}

auto vfs_file_set_path(File* file, const char* path) -> bool {
    if (file == nullptr) {
        return false;
    }
    vfs_file_clear_path(file);
    if (path == nullptr) {
        return true;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN + 1 <= file->vfs_path_inline.size()) {
        std::memcpy(file->vfs_path_inline.data(), path, PATH_LEN + 1);
        file->vfs_path = file->vfs_path_inline.data();
        return true;
    }

    auto* path_copy = new char[PATH_LEN + 1];
    if (path_copy == nullptr) {
        return false;
    }
    std::memcpy(path_copy, path, PATH_LEN + 1);
    file->vfs_path = path_copy;
    file->vfs_path_heap_allocated = true;
    return true;
}

auto vfs_destroy_file(File* f) -> int {
    if (f == nullptr) {
        return 0;
    }

    int close_result = 0;
    stream_detach_file(f);
    cache_notify_detach_file(f);
    advisory_release_file_owner_locks(f);
    if ((f->fops != nullptr) && (f->fops->vfs_close != nullptr)) {
        close_result = f->fops->vfs_close(f);
    }
    vfs_file_clear_path(f);
    f->private_data = nullptr;
    delete f;
    return close_result;
}

auto vfs_install_open_file(ker::mod::sched::task::Task* task, File* file) -> int {
    int const FD = vfs_alloc_fd(task, file);
    if (FD < 0) {
        vfs_put_file(file);
        return FD;
    }
    return FD;
}

#ifdef WOS_SELFTEST
std::atomic<int> g_vfs_selftest_close_count{0};
std::atomic<bool> g_vfs_selftest_force_dup2_insert_failure{false};

auto vfs_selftest_close(File*) -> int {
    g_vfs_selftest_close_count.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

FileOperations g_vfs_selftest_fops = {
    .vfs_open = nullptr,
    .vfs_close = vfs_selftest_close,
    .vfs_read = nullptr,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};
#endif

struct VfsDup2ReplaceResult {
    bool inserted = false;
    File* existing = nullptr;
};

auto vfs_replace_fd_for_dup2_locked(ker::mod::sched::task::Task* task, int newfd, File* file, bool cloexec = false)
    -> VfsDup2ReplaceResult {
    auto* existing = reinterpret_cast<File*>(task->fd_table.lookup(static_cast<uint64_t>(newfd)));
#ifdef WOS_SELFTEST
    if (g_vfs_selftest_force_dup2_insert_failure.load(std::memory_order_relaxed)) {
        return VfsDup2ReplaceResult{.inserted = false, .existing = existing};
    }
#endif
    bool const INSERTED = task->fd_table.insert(static_cast<uint64_t>(newfd), file);
    if (INSERTED) {
        if (cloexec) {
            task->set_fd_cloexec(static_cast<unsigned>(newfd));
        } else {
            task->clear_fd_cloexec(static_cast<unsigned>(newfd));
        }
    }
    return VfsDup2ReplaceResult{.inserted = INSERTED, .existing = existing};
}

auto apply_open_truncation(File* f, int flags) -> int {
    if ((flags & ker::vfs::O_TRUNC) == 0 || f == nullptr || f->is_directory) {
        return 0;
    }
    if (f->fops == nullptr || f->fops->vfs_truncate == nullptr) {
        return 0;
    }

    int const RET = f->fops->vfs_truncate(f, 0);
    if (RET == 0) {
        cache_notify_file_data_changed_impl(f);
    }
    return RET;
}

auto open_create_should_invalidate_metadata(const File* file, int flags) -> bool {
    if ((flags & ker::vfs::O_CREAT) == 0) {
        return false;
    }
    if (file == nullptr || !file->open_create_result_known) {
        return true;
    }
    return file->created_by_open;
}

auto vfs_take_fd_locked(ker::mod::sched::task::Task* task, int fd) -> File* {
    if (task == nullptr || fd < 0) {
        return nullptr;
    }

    auto* file = reinterpret_cast<File*>(task->fd_table.lookup(static_cast<uint64_t>(fd)));
    if (file == nullptr) {
        return nullptr;
    }

    task->fd_table.remove(static_cast<uint64_t>(fd));
    task->clear_fd_cloexec(static_cast<unsigned>(fd));
    return file;
}

auto vfs_find_free_fd_below_limit_locked(ker::mod::sched::task::Task* task, uint64_t start) -> uint64_t {
    if (task == nullptr || start >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return UINT64_MAX;
    }

    return task->fd_table.find_first_unset_below(start, ker::mod::sched::task::Task::FD_TABLE_SIZE);
}

auto clamp_io_count(ssize_t result, size_t requested) -> ssize_t {
    if (result <= 0 || std::cmp_less_equal(result, requested)) {
        return result;
    }
    return static_cast<ssize_t>(requested);
}

struct StreamFreshnessStamp {
    off_t size = 0;
    int64_t mtime_sec = 0;
    int64_t mtime_nsec = 0;
    int64_t ctime_sec = 0;
    int64_t ctime_nsec = 0;
    bool valid = false;
    bool size_only = false;
};

struct StreamCacheIdentity {
    const void* scope_key = nullptr;
    FSType fs_type = FSType::TMPFS;
    ino_t ino = 0;
    uint64_t remote_path_hash = 0;
    uint16_t remote_owner_node = 0;
    uint32_t remote_resource_id = 0;
};

struct StreamChunk {
    uint64_t offset = 0;
    uint32_t size = 0;
    std::array<uint8_t, STREAM_CHUNK_SIZE> data = {};
};

struct StreamCacheEntry;
struct StreamIsland;

struct StreamReaderAttachment {
    static constexpr uint64_t MAGIC = 0x53545245414d5241ULL;  // "STREAMRA"
    uint64_t magic = MAGIC;
    StreamCacheEntry* entry = nullptr;
    StreamIsland* island = nullptr;
    uint64_t desired_offset = 0;
    StreamReaderAttachment* prev = nullptr;
    StreamReaderAttachment* next = nullptr;
};

struct StreamIsland {
    bool retired = false;
    bool eof = false;
    bool producer_active = false;
    int error = 0;
    uint64_t next_fetch_offset = 0;
    uint64_t last_access_us = 0;
    StreamReaderAttachment* readers = nullptr;
    std::deque<std::unique_ptr<StreamChunk>> chunks;
};

struct StreamCacheEntry {
    StreamCacheIdentity identity = {};
    StreamFreshnessStamp freshness = {};
    bool can_reuse_detached = false;
    bool retain_full_file = false;
    bool pinned_detached = false;
    uint64_t last_used_us = 0;
    size_t cached_bytes = 0;
    std::deque<std::unique_ptr<StreamIsland>> islands;
};

std::deque<std::unique_ptr<StreamCacheEntry>> g_stream_cache;
ker::mod::sys::Mutex g_stream_cache_lock;

struct CacheNotifyEntry {
    uint64_t path_hash = 0;
    StreamCacheIdentity identity = {};
    StreamFreshnessStamp freshness = {};
    bool has_snapshot = false;
    bool can_reuse_detached = false;
    bool retain_full_file = false;
    uint64_t generation = 1;
    uint64_t validated_generation = 0;
    uint64_t last_used_us = 0;
    size_t watcher_count = 0;
};

struct CacheNotifyWatcher {
    static constexpr uint64_t MAGIC = 0x434143484e4f5446ULL;  // "CACHNOTF"
    uint64_t magic = MAGIC;
    File* file = nullptr;
    CacheNotifyEntry* entry = nullptr;
    uint64_t seen_generation = 0;
};

std::deque<std::unique_ptr<CacheNotifyEntry>> g_cache_notify_entries;
std::deque<CacheNotifyWatcher*> g_cache_notify_watchers;
ker::mod::sys::Mutex g_cache_notify_lock;
constexpr uint64_t CACHE_NOTIFY_ENTRY_TTL_US = 60000000;

auto stream_attachment_pointer_looks_valid(const void* ptr) -> bool {
    auto const ADDR = reinterpret_cast<uintptr_t>(ptr);
    constexpr uintptr_t HHDM_START = 0xffff800000000000ULL;
    constexpr uintptr_t HHDM_END = 0xffff900000000000ULL;
    constexpr uintptr_t KERNEL_STATIC_START = 0xffffffff80000000ULL;
    constexpr uintptr_t KERNEL_STATIC_END = 0xffffffffc0000000ULL;
    bool const IN_HHDM = ADDR >= HHDM_START && ADDR < HHDM_END;
    bool const IN_KERNEL_STATIC = ADDR >= KERNEL_STATIC_START && ADDR < KERNEL_STATIC_END;
    return (IN_HHDM || IN_KERNEL_STATIC) && ((ADDR & (alignof(StreamReaderAttachment) - 1)) == 0);
}

auto stream_now_us() -> uint64_t { return ker::mod::time::get_us(); }

auto stream_identity_equals(const StreamCacheIdentity& lhs, const StreamCacheIdentity& rhs) -> bool {
    if (lhs.fs_type != rhs.fs_type) {
        return false;
    }

    if (lhs.fs_type == FSType::REMOTE) {
        if (lhs.remote_path_hash == 0 || lhs.remote_path_hash != rhs.remote_path_hash) {
            return false;
        }

        bool const HAS_STABLE_REMOTE_SCOPE =
            lhs.remote_owner_node != 0 && rhs.remote_owner_node != 0 && lhs.remote_resource_id != 0 && rhs.remote_resource_id != 0;
        if (HAS_STABLE_REMOTE_SCOPE) {
            return lhs.remote_owner_node == rhs.remote_owner_node && lhs.remote_resource_id == rhs.remote_resource_id;
        }

        return lhs.scope_key != nullptr && lhs.scope_key == rhs.scope_key;
    }

    if (lhs.scope_key != rhs.scope_key) {
        return false;
    }
    return lhs.ino == rhs.ino;
}

auto stream_hash_path(const char* path) -> uint64_t {
    if (path == nullptr || path[0] == '\0') {
        return 0;
    }

    uint64_t hash = 1469598103934665603ULL;
    for (auto const* p = reinterpret_cast<const uint8_t*>(path); *p != 0; ++p) {
        hash ^= *p;
        hash *= 1099511628211ULL;
    }
    return hash;
}

auto cache_notify_entry_is_clean(const CacheNotifyEntry* entry) -> bool {
    return entry != nullptr && entry->has_snapshot && entry->validated_generation == entry->generation;
}

auto cache_notify_entry_is_dirty(const CacheNotifyEntry* entry) -> bool { return entry != nullptr && !cache_notify_entry_is_clean(entry); }

auto cache_notify_find_entry_locked(uint64_t path_hash) -> CacheNotifyEntry* {
    for (auto& entry : g_cache_notify_entries) {
        if (entry != nullptr && entry->path_hash == path_hash) {
            return entry.get();
        }
    }
    return nullptr;
}

auto cache_notify_attach_pointer_looks_valid(const void* ptr) -> bool {
    auto const ADDR = reinterpret_cast<uintptr_t>(ptr);
    constexpr uintptr_t HHDM_START = 0xffff800000000000ULL;
    constexpr uintptr_t HHDM_END = 0xffff900000000000ULL;
    constexpr uintptr_t KERNEL_STATIC_START = 0xffffffff80000000ULL;
    constexpr uintptr_t KERNEL_STATIC_END = 0xffffffffc0000000ULL;
    bool const IN_HHDM = ADDR >= HHDM_START && ADDR < HHDM_END;
    bool const IN_KERNEL_STATIC = ADDR >= KERNEL_STATIC_START && ADDR < KERNEL_STATIC_END;
    return (IN_HHDM || IN_KERNEL_STATIC) && ((ADDR & (alignof(CacheNotifyWatcher) - 1)) == 0);
}

void cache_notify_gc_locked(uint64_t now_us) {
    for (auto it = g_cache_notify_entries.begin(); it != g_cache_notify_entries.end();) {
        auto* entry = it->get();
        if (entry != nullptr && entry->watcher_count == 0 && (now_us - entry->last_used_us) > CACHE_NOTIFY_ENTRY_TTL_US) {
            it = g_cache_notify_entries.erase(it);
        } else {
            ++it;
        }
    }
}

auto cache_notify_ensure_entry_locked(uint64_t path_hash, uint64_t now_us) -> CacheNotifyEntry* {
    CacheNotifyEntry* entry = cache_notify_find_entry_locked(path_hash);
    if (entry == nullptr) {
        g_cache_notify_entries.push_back(std::make_unique<CacheNotifyEntry>());
        entry = g_cache_notify_entries.back().get();
        entry->path_hash = path_hash;
    }
    entry->last_used_us = now_us;
    return entry;
}

void cache_notify_drop_snapshot_locked(CacheNotifyEntry* entry) {
    if (entry == nullptr) {
        return;
    }
    entry->validated_generation = 0;
    entry->has_snapshot = false;
    entry->can_reuse_detached = false;
    entry->retain_full_file = false;
}

void cache_notify_attach_file(File* file) {
    if (file == nullptr || file->vfs_path == nullptr || (file->open_flags & ker::vfs::O_NOTIFY_CACHE_CHANGE) == 0 ||
        file->cache_notify_attachment != nullptr) {
        return;
    }

    uint64_t const PATH_HASH = stream_hash_path(file->vfs_path);
    if (PATH_HASH == 0) {
        return;
    }

    auto* watcher = new (std::nothrow) CacheNotifyWatcher;
    if (watcher == nullptr) {
        return;
    }

    watcher->file = file;

    uint64_t const NOW_US = stream_now_us();
    g_cache_notify_lock.lock();
    cache_notify_gc_locked(NOW_US);
    CacheNotifyEntry* entry = cache_notify_ensure_entry_locked(PATH_HASH, NOW_US);
    entry->watcher_count++;
    watcher->entry = entry;
    watcher->seen_generation = entry->generation;
    g_cache_notify_watchers.push_back(watcher);
    file->cache_notify_attachment = watcher;
    g_cache_notify_lock.unlock();
}

void cache_notify_detach_file(File* file) {
    if (file == nullptr || file->cache_notify_attachment == nullptr) {
        return;
    }

    auto* watcher = static_cast<CacheNotifyWatcher*>(file->cache_notify_attachment);
    file->cache_notify_attachment = nullptr;
    if (!cache_notify_attach_pointer_looks_valid(watcher) || watcher->magic != CacheNotifyWatcher::MAGIC) {
        return;
    }

    g_cache_notify_lock.lock();
    if (watcher->entry != nullptr && watcher->entry->watcher_count > 0) {
        watcher->entry->watcher_count--;
        watcher->entry->last_used_us = stream_now_us();
    }
    std::erase(g_cache_notify_watchers, watcher);
    cache_notify_gc_locked(stream_now_us());
    g_cache_notify_lock.unlock();

    watcher->magic = 0;
    delete watcher;
}

auto cache_notify_try_copy_snapshot(File* file, StreamCacheIdentity* identity, StreamFreshnessStamp* freshness, bool* can_reuse_detached,
                                    bool* retain_full_file) -> bool {
    if (file == nullptr || file->cache_notify_attachment == nullptr || identity == nullptr || freshness == nullptr ||
        can_reuse_detached == nullptr || retain_full_file == nullptr) {
        return false;
    }

    auto* watcher = static_cast<CacheNotifyWatcher*>(file->cache_notify_attachment);
    if (!cache_notify_attach_pointer_looks_valid(watcher) || watcher->magic != CacheNotifyWatcher::MAGIC) {
        return false;
    }

    bool found = false;
    g_cache_notify_lock.lock();
    CacheNotifyEntry* entry = watcher->entry;
    if (entry != nullptr && cache_notify_entry_is_clean(entry)) {
        *identity = entry->identity;
        *freshness = entry->freshness;
        *can_reuse_detached = entry->can_reuse_detached;
        *retain_full_file = entry->retain_full_file;
        watcher->seen_generation = entry->generation;
        entry->last_used_us = stream_now_us();
        found = true;
    }
    g_cache_notify_lock.unlock();
    return found;
}

void cache_notify_store_snapshot(File* file, const StreamCacheIdentity& identity, const StreamFreshnessStamp& freshness,
                                 bool can_reuse_detached, bool retain_full_file) {
    if (file == nullptr || file->cache_notify_attachment == nullptr) {
        return;
    }

    auto* watcher = static_cast<CacheNotifyWatcher*>(file->cache_notify_attachment);
    if (!cache_notify_attach_pointer_looks_valid(watcher) || watcher->magic != CacheNotifyWatcher::MAGIC) {
        return;
    }

    g_cache_notify_lock.lock();
    if (watcher->entry != nullptr) {
        watcher->entry->identity = identity;
        watcher->entry->freshness = freshness;
        watcher->entry->has_snapshot = true;
        watcher->entry->can_reuse_detached = can_reuse_detached;
        watcher->entry->retain_full_file = retain_full_file;
        watcher->entry->validated_generation = watcher->entry->generation;
        watcher->entry->last_used_us = stream_now_us();
        watcher->seen_generation = watcher->entry->generation;
    }
    g_cache_notify_lock.unlock();
}

auto cache_notify_invalidate_path_local(const char* vfs_path) -> bool {
    if (vfs_path == nullptr || vfs_path[0] == '\0') {
        return false;
    }

    g_cache_notify_lock.lock();
    bool const NO_NOTIFY_ENTRIES = g_cache_notify_entries.empty();
    g_cache_notify_lock.unlock();
    if (NO_NOTIFY_ENTRIES) {
        return false;
    }

    uint64_t const PATH_HASH = stream_hash_path(vfs_path);
    if (PATH_HASH == 0) {
        return false;
    }

    StreamCacheIdentity cached_identity = {};
    bool have_cached_identity = false;
    ker::util::SmallVec<File*, 8> remote_files;
    bool transitioned_to_dirty = false;

    g_cache_notify_lock.lock();
    if (CacheNotifyEntry* entry = cache_notify_find_entry_locked(PATH_HASH); entry != nullptr) {
        bool const WAS_DIRTY = cache_notify_entry_is_dirty(entry);
        if (!WAS_DIRTY) {
            entry->generation++;
            transitioned_to_dirty = true;
        }
        entry->last_used_us = stream_now_us();
        if (transitioned_to_dirty && entry->has_snapshot) {
            cached_identity = entry->identity;
            have_cached_identity = true;
        }
        cache_notify_drop_snapshot_locked(entry);

        if (transitioned_to_dirty) {
            for (auto* watcher : g_cache_notify_watchers) {
                if (watcher == nullptr || watcher->entry != entry || watcher->file == nullptr) {
                    continue;
                }
                if (watcher->file->fs_type == FSType::REMOTE) {
                    (void)remote_files.push_back(watcher->file);
                }
            }
        }
    }
    cache_notify_gc_locked(stream_now_us());
    g_cache_notify_lock.unlock();

    if (have_cached_identity) {
        g_stream_cache_lock.lock();
        stream_invalidate_identity_locked(cached_identity);
        stream_gc_locked(stream_now_us());
        g_stream_cache_lock.unlock();
    }

    for (auto* file : remote_files) {
        ker::net::wki::wki_remote_vfs_invalidate_open_file_caches(file);
    }

    return transitioned_to_dirty;
}

auto stream_stat_has_freshness(const Stat& st) -> bool {
    return st.st_mtim.tv_sec != 0 || st.st_mtim.tv_nsec != 0 || st.st_ctim.tv_sec != 0 || st.st_ctim.tv_nsec != 0;
}

auto stream_capture_freshness(const Stat& st, FSType fs_type) -> StreamFreshnessStamp {
    StreamFreshnessStamp stamp = {};
    stamp.size = st.st_size;
    stamp.mtime_sec = st.st_mtim.tv_sec;
    stamp.mtime_nsec = st.st_mtim.tv_nsec;
    stamp.ctime_sec = st.st_ctim.tv_sec;
    stamp.ctime_nsec = st.st_ctim.tv_nsec;
    if (fs_type == FSType::REMOTE && st.st_size > 0) {
        stamp.valid = true;
        stamp.size_only = true;
    } else {
        stamp.valid = stream_stat_has_freshness(st);
    }
    return stamp;
}

auto stream_freshness_matches(const StreamFreshnessStamp& stamp, const Stat& st) -> bool {
    if (!stamp.valid) {
        return false;
    }

    if (stamp.size_only) {
        return stamp.size > 0 && stamp.size == st.st_size;
    }

    return stamp.size == st.st_size && stamp.mtime_sec == st.st_mtim.tv_sec && stamp.mtime_nsec == st.st_mtim.tv_nsec &&
           stamp.ctime_sec == st.st_ctim.tv_sec && stamp.ctime_nsec == st.st_ctim.tv_nsec;
}

auto stream_island_start(const StreamIsland* island) -> uint64_t {
    if (island == nullptr || island->chunks.empty()) {
        return (island != nullptr) ? island->next_fetch_offset : 0;
    }
    return island->chunks.front()->offset;
}

auto stream_island_end(const StreamIsland* island) -> uint64_t {
    if (island == nullptr || island->chunks.empty()) {
        return (island != nullptr) ? island->next_fetch_offset : 0;
    }
    const auto* back = island->chunks.back().get();
    return back->offset + back->size;
}

auto stream_island_span(const StreamIsland* island) -> uint64_t {
    uint64_t const START = stream_island_start(island);
    uint64_t const END = stream_island_end(island);
    return (END > START) ? (END - START) : 0;
}

auto stream_island_reader_count(const StreamIsland* island) -> size_t {
    size_t count = 0;
    for (auto* reader = (island != nullptr) ? island->readers : nullptr; reader != nullptr; reader = reader->next) {
        count++;
    }
    return count;
}

auto stream_island_slowest_offset(const StreamIsland* island) -> uint64_t {
    uint64_t slowest = stream_island_end(island);
    bool seen = false;
    for (auto* reader = (island != nullptr) ? island->readers : nullptr; reader != nullptr; reader = reader->next) {
        if (!seen || reader->desired_offset < slowest) {
            slowest = reader->desired_offset;
            seen = true;
        }
    }

    if (!seen) {
        return stream_island_end(island);
    }
    return slowest;
}

void stream_unlink_attachment(StreamReaderAttachment* attachment) {
    if (attachment == nullptr || attachment->island == nullptr) {
        return;
    }

    auto* island = attachment->island;
    if (attachment->prev != nullptr) {
        attachment->prev->next = attachment->next;
    } else {
        island->readers = attachment->next;
    }
    if (attachment->next != nullptr) {
        attachment->next->prev = attachment->prev;
    }

    attachment->prev = nullptr;
    attachment->next = nullptr;
    attachment->island = nullptr;
}

void stream_link_attachment(StreamReaderAttachment* attachment, StreamIsland* island, uint64_t offset) {
    if (attachment == nullptr) {
        return;
    }

    if (attachment->island == island) {
        attachment->desired_offset = offset;
        return;
    }

    stream_unlink_attachment(attachment);
    attachment->desired_offset = offset;
    attachment->island = island;
    if (island == nullptr) {
        return;
    }

    attachment->next = island->readers;
    if (island->readers != nullptr) {
        island->readers->prev = attachment;
    }
    island->readers = attachment;
}

auto stream_entry_has_readers(const StreamCacheEntry* entry) -> bool {
    if (entry == nullptr) {
        return false;
    }

    for (const auto& island : entry->islands) {
        if (island != nullptr && island->readers != nullptr) {
            return true;
        }
    }
    return false;
}

auto stream_entry_is_fully_cached(const StreamCacheEntry* entry) -> bool {
    if (entry == nullptr || !entry->retain_full_file || entry->freshness.size <= 0) {
        return false;
    }

    for (const auto& island : entry->islands) {
        if (island == nullptr || island->retired || !island->eof) {
            continue;
        }
        if (stream_island_start(island.get()) == 0 && stream_island_end(island.get()) >= static_cast<uint64_t>(entry->freshness.size)) {
            return true;
        }
    }
    return false;
}

auto stream_entry_cached_prefix_bytes(const StreamCacheEntry* entry) -> uint64_t {
    if (entry == nullptr) {
        return 0;
    }

    uint64_t best_prefix = 0;
    for (const auto& island : entry->islands) {
        if (island == nullptr || island->retired) {
            continue;
        }

        if (stream_island_start(island.get()) != 0) {
            continue;
        }

        best_prefix = std::max(best_prefix, stream_island_end(island.get()));
    }

    return best_prefix;
}

auto stream_detached_ttl_us(const StreamCacheEntry* entry) -> uint64_t {
    if (entry != nullptr && entry->identity.fs_type != FSType::REMOTE && entry->retain_full_file && entry->freshness.size > 0 &&
        std::cmp_less_equal(entry->freshness.size, STREAM_LOCAL_SMALL_FILE_RETAIN_MAX)) {
        return STREAM_LOCAL_SMALL_FILE_DETACHED_TTL_US;
    }
    return STREAM_DETACHED_TTL_US;
}

auto stream_entry_should_keep_detached(const StreamCacheEntry* entry, uint64_t now_us) -> bool {
    if (entry == nullptr || stream_entry_has_readers(entry)) {
        return false;
    }
    if (entry->pinned_detached && entry->retain_full_file && stream_entry_is_fully_cached(entry)) {
        return true;
    }
    if (!entry->can_reuse_detached || (now_us - entry->last_used_us) > stream_detached_ttl_us(entry)) {
        return false;
    }

    if (entry->retain_full_file && stream_entry_is_fully_cached(entry)) {
        return true;
    }

    if (entry->identity.fs_type != FSType::REMOTE || !entry->retain_full_file) {
        return false;
    }

    uint64_t const PREFIX_BYTES = stream_entry_cached_prefix_bytes(entry);
    return PREFIX_BYTES > 0 && std::cmp_less_equal(PREFIX_BYTES, STREAM_DETACHED_PARTIAL_REUSE_MAX);
}

auto stream_total_cached_bytes_locked() -> size_t {
    size_t total = 0;
    for (const auto& entry : g_stream_cache) {
        if (entry == nullptr) {
            continue;
        }
        if (entry->cached_bytes > SIZE_MAX - total) {
            return SIZE_MAX;
        }
        total += entry->cached_bytes;
    }
    return total;
}

void stream_enforce_detached_global_cap_locked() {
    size_t total = stream_total_cached_bytes_locked();
    while (total > STREAM_DETACHED_GLOBAL_BYTE_CAP) {
        auto candidate = g_stream_cache.end();
        uint64_t oldest_used_us = UINT64_MAX;
        for (auto it = g_stream_cache.begin(); it != g_stream_cache.end(); ++it) {
            StreamCacheEntry* entry = it->get();
            if (entry == nullptr || entry->cached_bytes == 0 || entry->pinned_detached || stream_entry_has_readers(entry)) {
                continue;
            }
            if (candidate == g_stream_cache.end() || entry->last_used_us < oldest_used_us) {
                candidate = it;
                oldest_used_us = entry->last_used_us;
            }
        }
        if (candidate == g_stream_cache.end()) {
            break;
        }
        size_t const CACHED_BYTES = (*candidate)->cached_bytes;
        total = CACHED_BYTES <= total ? total - CACHED_BYTES : 0;
        g_stream_cache.erase(candidate);
    }
}

void stream_gc_locked(uint64_t now_us) {
    for (auto entry_it = g_stream_cache.begin(); entry_it != g_stream_cache.end();) {
        auto* entry = entry_it->get();
        for (auto island_it = entry->islands.begin(); island_it != entry->islands.end();) {
            auto* island = island_it->get();
            if (island->retired && island->readers == nullptr && !island->producer_active) {
                island_it = entry->islands.erase(island_it);
            } else {
                ++island_it;
            }
        }

        if (!stream_entry_should_keep_detached(entry, now_us)) {
            if (!stream_entry_has_readers(entry)) {
                entry_it = g_stream_cache.erase(entry_it);
                continue;
            }
        }
        ++entry_it;
    }
    stream_enforce_detached_global_cap_locked();
}

auto stream_find_entry_locked(const StreamCacheIdentity& identity) -> StreamCacheEntry* {
    for (auto& entry : g_stream_cache) {
        if (entry != nullptr && stream_identity_equals(entry->identity, identity)) {
            return entry.get();
        }
    }
    return nullptr;
}

auto stream_find_island_locked(StreamCacheEntry* entry, uint64_t offset) -> StreamIsland* {
    StreamIsland* best = nullptr;
    uint64_t best_gap = UINT64_MAX;

    for (auto& island : entry->islands) {
        if (island == nullptr || island->retired) {
            continue;
        }

        uint64_t const START = stream_island_start(island.get());
        uint64_t const END = stream_island_end(island.get());
        if (offset >= START && offset <= END) {
            return island.get();
        }

        if (!island->eof && offset > END) {
            uint64_t const GAP = offset - END;
            if (GAP <= STREAM_CHUNK_SIZE && GAP < best_gap) {
                best = island.get();
                best_gap = GAP;
            }
        }
    }

    return best;
}

void stream_retire_island_locked(StreamCacheEntry* entry, StreamIsland* island) {
    if (entry == nullptr || island == nullptr || island->retired) {
        return;
    }

    island->retired = true;
    island->eof = false;
    island->error = 0;
    if (!island->producer_active) {
        for (const auto& chunk : island->chunks) {
            if (chunk != nullptr) {
                entry->cached_bytes -= chunk->size;
            }
        }
        island->chunks.clear();
    }
}

void stream_trim_island_front_locked(StreamCacheEntry* entry, StreamIsland* island) {
    if (entry == nullptr || island == nullptr || entry->retain_full_file || island->chunks.empty()) {
        return;
    }

    uint64_t const SLOWEST = stream_island_slowest_offset(island);
    while (!island->chunks.empty()) {
        auto* chunk = island->chunks.front().get();
        uint64_t const CHUNK_END = chunk->offset + chunk->size;
        if (CHUNK_END > SLOWEST) {
            break;
        }

        entry->cached_bytes -= chunk->size;
        island->chunks.pop_front();
    }
}

void stream_enforce_entry_cap_locked(StreamCacheEntry* entry, uint64_t preferred_offset) {
    if (entry == nullptr) {
        return;
    }

    for (auto& island : entry->islands) {
        if (island != nullptr) {
            stream_trim_island_front_locked(entry, island.get());
        }
    }

    while (entry->cached_bytes > STREAM_ENTRY_BYTE_CAP) {
        StreamIsland* candidate = nullptr;
        uint64_t candidate_distance = 0;
        bool prefer_unread = false;

        for (auto& island : entry->islands) {
            if (island == nullptr || island->retired) {
                continue;
            }

            uint64_t const CENTER = stream_island_start(island.get()) + (stream_island_span(island.get()) / 2);
            uint64_t const DISTANCE = (preferred_offset > CENTER) ? (preferred_offset - CENTER) : (CENTER - preferred_offset);
            bool const UNREAD = stream_island_reader_count(island.get()) == 0;

            if (candidate == nullptr || (UNREAD && !prefer_unread) || (UNREAD == prefer_unread && DISTANCE > candidate_distance)) {
                candidate = island.get();
                candidate_distance = DISTANCE;
                prefer_unread = UNREAD;
            }
        }

        if (candidate == nullptr) {
            break;
        }

        stream_retire_island_locked(entry, candidate);

        bool removed_any = false;
        for (auto island_it = entry->islands.begin(); island_it != entry->islands.end();) {
            auto* island = island_it->get();
            if (island->retired && island->readers == nullptr && !island->producer_active) {
                island_it = entry->islands.erase(island_it);
                removed_any = true;
            } else {
                ++island_it;
            }
        }

        if (!removed_any) {
            break;
        }
    }
}

void stream_ensure_island_budget_locked(StreamCacheEntry* entry, uint64_t preferred_offset) {
    size_t active_islands = 0;
    for (const auto& island : entry->islands) {
        if (island != nullptr && !island->retired) {
            active_islands++;
        }
    }

    while (active_islands >= STREAM_MAX_ACTIVE_ISLANDS) {
        StreamIsland* candidate = nullptr;
        uint64_t candidate_distance = 0;
        bool prefer_unread = false;

        for (auto& island : entry->islands) {
            if (island == nullptr || island->retired) {
                continue;
            }

            uint64_t const CENTER = stream_island_start(island.get()) + (stream_island_span(island.get()) / 2);
            uint64_t const DISTANCE = (preferred_offset > CENTER) ? (preferred_offset - CENTER) : (CENTER - preferred_offset);
            bool const UNREAD = stream_island_reader_count(island.get()) == 0;
            if (candidate == nullptr || (UNREAD && !prefer_unread) || (UNREAD == prefer_unread && DISTANCE > candidate_distance)) {
                candidate = island.get();
                candidate_distance = DISTANCE;
                prefer_unread = UNREAD;
            }
        }

        if (candidate == nullptr) {
            break;
        }

        stream_retire_island_locked(entry, candidate);
        active_islands--;
    }
}

auto stream_create_island_locked(StreamCacheEntry* entry, uint64_t offset) -> StreamIsland* {
    if (entry == nullptr) {
        return nullptr;
    }

    stream_ensure_island_budget_locked(entry, offset);
    entry->islands.push_back(std::make_unique<StreamIsland>());
    auto* island = entry->islands.back().get();
    island->next_fetch_offset = offset;
    island->last_access_us = stream_now_us();
    return island;
}

void stream_reset_entry_locked(StreamCacheEntry* entry) {
    if (entry == nullptr) {
        return;
    }

    entry->pinned_detached = false;
    entry->cached_bytes = 0;
    for (auto& island : entry->islands) {
        if (island == nullptr) {
            continue;
        }
        island->retired = true;
        island->eof = false;
        island->error = 0;
        island->chunks.clear();
        if (island->readers == nullptr && !island->producer_active) {
            island.reset();
        }
    }
    std::erase_if(entry->islands, [](const std::unique_ptr<StreamIsland>& island) { return island == nullptr; });
}

auto stream_scope_key_for_mount(const MountPoint* mount) -> const void* {
    if (mount == nullptr) {
        return nullptr;
    }
    if (mount->fs_type == FSType::REMOTE || mount->fs_type == FSType::XFS) {
        return mount->private_data;
    }
    return mount;
}

auto stream_build_xfs_open_identity(File* file, StreamCacheIdentity* identity) -> bool {
    if (file == nullptr || identity == nullptr || file->fs_type != FSType::XFS) {
        return false;
    }

    ker::vfs::xfs::XfsMountContext* mount = nullptr;
    uint64_t ino = 0;
    if (!ker::vfs::xfs::xfs_file_regular_identity(file, &mount, &ino) || mount == nullptr || ino == 0) {
        return false;
    }

    *identity = {};
    identity->scope_key = mount;
    identity->fs_type = FSType::XFS;
    identity->ino = static_cast<ino_t>(ino);
    return true;
}

void fill_tmpfs_stat_timestamps(const ker::vfs::tmpfs::TmpNode* node, Stat* statbuf) {
    if (node == nullptr || statbuf == nullptr) {
        return;
    }
    statbuf->st_atim = node->atime;
    statbuf->st_mtim = node->mtime;
    statbuf->st_ctim = node->ctime;
}

void fill_tmpfs_node_stat(uint32_t dev_id, const ker::vfs::tmpfs::TmpNode* node, Stat* statbuf) {
    if (node == nullptr || statbuf == nullptr) {
        return;
    }

    auto const* stat_node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
    if (stat_node == nullptr) {
        return;
    }

    statbuf->st_dev = dev_id;
    statbuf->st_ino = reinterpret_cast<ino_t>(stat_node);
    statbuf->st_nlink = ker::vfs::tmpfs::tmpfs_link_count(stat_node);
    statbuf->st_uid = stat_node->uid;
    statbuf->st_gid = stat_node->gid;
    statbuf->st_rdev = 0;
    statbuf->st_size = static_cast<off_t>(stat_node->size);
    statbuf->st_blksize = 4096;
    statbuf->st_blocks = static_cast<blkcnt_t>((stat_node->size + 511) / 512);
    switch (stat_node->type) {
        case ker::vfs::tmpfs::TmpNodeType::FILE:
            statbuf->st_mode = S_IFREG | stat_node->mode;
            break;
        case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
            statbuf->st_mode = S_IFDIR | stat_node->mode;
            break;
        case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
            statbuf->st_mode = S_IFLNK | stat_node->mode;
            break;
    }
    fill_tmpfs_stat_timestamps(stat_node, statbuf);
}

void fill_devfs_stat_timestamps(const ker::vfs::devfs::DevFSNode* node, Stat* statbuf) {
    if (node == nullptr || statbuf == nullptr) {
        return;
    }
    statbuf->st_atim = node->atime;
    statbuf->st_mtim = node->mtime;
    statbuf->st_ctim = node->ctime;
}

auto vfs_stream_cache_get_file_stat(File* file, Stat* statbuf) -> int {
    if (file == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    std::memset(statbuf, 0, sizeof(Stat));

    auto stream_stat_dev_id = [&]() -> uint32_t {
        MountRef sc_mount_ref = (file->vfs_path != nullptr) ? find_mount_point(file->vfs_path) : MountRef{};
        MountPoint const* sc_mount = sc_mount_ref.get();
        return sc_mount != nullptr ? sc_mount->dev_id : 0;
    };

    switch (file->fs_type) {
        case FSType::TMPFS: {
            // Pipes and epoll reuse FSType::TMPFS but private_data is not a TmpNode
            if (file->fops != ker::vfs::tmpfs::get_tmpfs_fops()) {
                return -ENOSYS;
            }
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(file->private_data);
            if (node == nullptr) {
                return -EBADF;
            }
            fill_tmpfs_node_stat(stream_stat_dev_id(), node, statbuf);
            return 0;
        }
        case FSType::FAT32: {
            int const R = ker::vfs::fat32::fat32_fstat(file, statbuf);
            if (R == 0) {
                statbuf->st_dev = stream_stat_dev_id();
            }
            return R;
        }
        case FSType::DEVFS:
        case FSType::SOCKET:
        case FSType::PROCFS:
            return -ENOSYS;
        case FSType::REMOTE: {
            int const R = ker::net::wki::wki_remote_vfs_fstat(file, statbuf);
            if (R == 0 && statbuf->st_dev == 0) {
                statbuf->st_dev = stream_stat_dev_id();
            }
            return R;
        }
        case FSType::XFS: {
            return ker::vfs::xfs::xfs_fstat(file, statbuf);
        }
        default:
            return -ENOSYS;
    }
}

auto stream_build_identity(File* file, const Stat& statbuf, StreamCacheIdentity* identity, StreamFreshnessStamp* stamp,
                           bool* can_reuse_detached, bool* retain_full_file) -> int {
    if (file == nullptr || file->vfs_path == nullptr || identity == nullptr) {
        return -EINVAL;
    }

    const auto MODE = static_cast<mode_t>(statbuf.st_mode & S_IFMT);
    if (MODE != S_IFREG) {
        return -ENOTSUP;
    }
    if ((file->open_flags & 3) != 0) {
        return -EACCES;
    }

    FSType identity_fs_type = file->fs_type;
    const void* scope_key = nullptr;
    identity->remote_path_hash = 0;
    identity->remote_owner_node = 0;
    identity->remote_resource_id = 0;

    if (file->fs_type == FSType::XFS) {
        scope_key = ker::vfs::xfs::xfs_file_mount_context(file);
        if (scope_key == nullptr) {
            return -ENOSYS;
        }
    } else {
        auto mount_ref = find_mount_point(file->vfs_path);
        MountPoint const* mount = mount_ref.get();
        if (mount == nullptr) {
            return -ENOENT;
        }
        identity_fs_type = mount->fs_type;
        scope_key = stream_scope_key_for_mount(mount);

        if (mount->fs_type == FSType::REMOTE) {
            const char* fs_path = strip_mount_prefix(mount, file->vfs_path);
            identity->remote_path_hash = stream_hash_path(fs_path);
            auto const* state = static_cast<const ker::net::wki::ProxyVfsState*>(mount->private_data);
            if (state != nullptr) {
                identity->remote_owner_node = state->owner_node;
                identity->remote_resource_id = state->resource_id;
            }
        }
    }

    identity->scope_key = scope_key;
    identity->fs_type = identity_fs_type;
    identity->ino = statbuf.st_ino;

    bool const REMOTE_IDENTITY_VALID =
        identity->remote_path_hash != 0 &&
        ((identity->remote_owner_node != 0 && identity->remote_resource_id != 0) || identity->scope_key != nullptr);
    bool const LOCAL_IDENTITY_VALID = identity->scope_key != nullptr && identity->ino != 0;
    bool const IDENTITY_VALID = (identity_fs_type == FSType::REMOTE) ? REMOTE_IDENTITY_VALID : LOCAL_IDENTITY_VALID;
    if (!IDENTITY_VALID) {
        return -ENOSYS;
    }

    bool const HAS_REUSABLE_FRESHNESS = (identity_fs_type == FSType::REMOTE) ? statbuf.st_size > 0 : stream_stat_has_freshness(statbuf);

    if (stamp != nullptr) {
        *stamp = stream_capture_freshness(statbuf, identity_fs_type);
    }
    if (can_reuse_detached != nullptr) {
        *can_reuse_detached = HAS_REUSABLE_FRESHNESS;
    }
    if (retain_full_file != nullptr) {
        *retain_full_file =
            HAS_REUSABLE_FRESHNESS && statbuf.st_size > 0 && std::cmp_less_equal(statbuf.st_size, STREAM_DETACHED_REUSE_MAX);
    }

    return 0;
}

auto stream_copy_available_locked(StreamReaderAttachment* attachment, uint64_t offset, uint8_t* dst, size_t len) -> size_t {
    if (attachment == nullptr || attachment->island == nullptr || attachment->island->retired) {
        return 0;
    }

    size_t total = 0;
    uint64_t cursor = offset;
    for (const auto& chunk : attachment->island->chunks) {
        if (chunk == nullptr) {
            continue;
        }
        uint64_t const CHUNK_END = chunk->offset + chunk->size;
        if (cursor >= CHUNK_END) {
            continue;
        }
        if (cursor < chunk->offset) {
            break;
        }

        auto const IN_CHUNK = static_cast<size_t>(cursor - chunk->offset);
        size_t const AVAILABLE = chunk->size - IN_CHUNK;
        size_t const TO_COPY = std::min(len - total, AVAILABLE);
        std::memcpy(dst + total, chunk->data.data() + IN_CHUNK, TO_COPY);
        total += TO_COPY;
        cursor += TO_COPY;
        if (total == len) {
            break;
        }
    }
    return total;
}

auto stream_select_island_locked(StreamReaderAttachment* attachment, uint64_t offset) -> StreamIsland* {
    if (attachment == nullptr || attachment->entry == nullptr) {
        return nullptr;
    }

    auto* entry = attachment->entry;
    auto* current = attachment->island;
    if (current != nullptr && !current->retired) {
        uint64_t const START = stream_island_start(current);
        uint64_t const END = stream_island_end(current);
        if ((offset >= START && offset <= END) || (!current->eof && offset >= END && (offset - END) <= STREAM_CHUNK_SIZE)) {
            stream_link_attachment(attachment, current, offset);
            return current;
        }

        if (!entry->retain_full_file && stream_island_reader_count(current) > 1) {
            uint64_t const SLOWEST = stream_island_slowest_offset(current);
            if (offset > SLOWEST && (offset - SLOWEST) > STREAM_SPLIT_DISTANCE_BYTES) {
                auto* new_island = stream_create_island_locked(entry, offset);
                stream_link_attachment(attachment, new_island, offset);
                return new_island;
            }
        }
    }

    if (auto* existing = stream_find_island_locked(entry, offset); existing != nullptr) {
        stream_link_attachment(attachment, existing, offset);
        return existing;
    }

    auto* island = stream_create_island_locked(entry, offset);
    stream_link_attachment(attachment, island, offset);
    return island;
}

auto stream_attach_file(File* file) -> StreamReaderAttachment* {
    if (file == nullptr) {
        return nullptr;
    }

    auto* existing = static_cast<StreamReaderAttachment*>(file->stream_cache_attachment);
    if (existing != nullptr) {
        uint64_t const NOW_US = stream_now_us();
        g_stream_cache_lock.lock();

        bool entry_alive = false;
        for (auto& entry : g_stream_cache) {
            if (entry.get() == existing->entry) {
                entry_alive = true;
                break;
            }
        }

        if (entry_alive) {
            existing->desired_offset = static_cast<uint64_t>(file->pos);
            if (existing->island == nullptr || existing->island->retired) {
                if (stream_select_island_locked(existing, existing->desired_offset) == nullptr) {
                    file->stream_cache_attachment = nullptr;
                    g_stream_cache_lock.unlock();
                    delete existing;
                    return nullptr;
                }
            }
            existing->entry->last_used_us = NOW_US;
            g_stream_cache_lock.unlock();
            return existing;
        }

        file->stream_cache_attachment = nullptr;
        g_stream_cache_lock.unlock();
        delete existing;
    }

    StreamCacheIdentity identity = {};
    StreamFreshnessStamp freshness = {};
    bool can_reuse_detached = false;
    bool retain_full_file = false;
    Stat st = {};
    bool have_notify_snapshot = cache_notify_try_copy_snapshot(file, &identity, &freshness, &can_reuse_detached, &retain_full_file);

    if (!have_notify_snapshot) {
        if (!file_stat_snapshot_lookup(file, &st)) {
            if (file_stat_snapshot_refresh_from_backend(file, &st) != 0) {
                return nullptr;
            }
        }

        if (stream_build_identity(file, st, &identity, &freshness, &can_reuse_detached, &retain_full_file) != 0) {
            return nullptr;
        }

        if ((file->open_flags & ker::vfs::O_NOTIFY_CACHE_CHANGE) != 0) {
            cache_notify_store_snapshot(file, identity, freshness, can_reuse_detached, retain_full_file);
        }
    }
    bool const ALWAYS_CACHE = (file->open_flags & ker::vfs::O_ALWAYS_CACHE) != 0;
    bool const PIN_DETACHED = ALWAYS_CACHE && retain_full_file;

    auto* attachment = new StreamReaderAttachment;
    attachment->desired_offset = static_cast<uint64_t>(file->pos);

    uint64_t const NOW_US = stream_now_us();
    g_stream_cache_lock.lock();
    stream_gc_locked(NOW_US);

    StreamCacheEntry* entry = stream_find_entry_locked(identity);
    if (entry == nullptr) {
        g_stream_cache.push_back(std::make_unique<StreamCacheEntry>());
        entry = g_stream_cache.back().get();
        entry->identity = identity;
        entry->freshness = freshness;
        entry->can_reuse_detached = can_reuse_detached;
        entry->retain_full_file = retain_full_file;
        entry->pinned_detached = PIN_DETACHED;
        entry->last_used_us = NOW_US;
    } else if (!stream_entry_has_readers(entry)) {
        if (!ALWAYS_CACHE && (!stream_entry_should_keep_detached(entry, NOW_US) ||
                              (!have_notify_snapshot && !stream_freshness_matches(entry->freshness, st)))) {
            stream_reset_entry_locked(entry);
            entry->freshness = freshness;
            entry->can_reuse_detached = can_reuse_detached;
            entry->retain_full_file = retain_full_file;
            entry->pinned_detached = PIN_DETACHED;
        }
        entry->pinned_detached = entry->pinned_detached || PIN_DETACHED;
        entry->last_used_us = NOW_US;
    } else if (PIN_DETACHED) {
        entry->pinned_detached = true;
    }

    attachment->entry = entry;
    if (stream_select_island_locked(attachment, attachment->desired_offset) == nullptr) {
        g_stream_cache_lock.unlock();
        delete attachment;
        return nullptr;
    }
    file->stream_cache_attachment = attachment;
    g_stream_cache_lock.unlock();
    return attachment;
}

void stream_detach_file(File* file) {
    if (file == nullptr || file->stream_cache_attachment == nullptr) {
        return;
    }

    auto* attachment = static_cast<StreamReaderAttachment*>(file->stream_cache_attachment);
    file->stream_cache_attachment = nullptr;
    if (!stream_attachment_pointer_looks_valid(attachment) || attachment->magic != StreamReaderAttachment::MAGIC) {
        return;
    }

    g_stream_cache_lock.lock();
    stream_unlink_attachment(attachment);
    stream_gc_locked(stream_now_us());
    g_stream_cache_lock.unlock();

    attachment->magic = 0;
    delete attachment;
}

void stream_invalidate_identity_locked(const StreamCacheIdentity& identity) {
    auto* entry = stream_find_entry_locked(identity);
    if (entry == nullptr) {
        return;
    }

    stream_reset_entry_locked(entry);
    entry->last_used_us = stream_now_us();
}

void stream_invalidate_file(File* file) {
    if (file == nullptr || file->vfs_path == nullptr) {
        return;
    }

    g_stream_cache_lock.lock();
    bool const STREAM_CACHE_EMPTY = g_stream_cache.empty();
    g_stream_cache_lock.unlock();
    if (STREAM_CACHE_EMPTY) {
        g_vfs_stream_invalidate_empty_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    StreamCacheIdentity identity = {};
    if (!stream_build_xfs_open_identity(file, &identity)) {
        Stat st = {};
        if (vfs_stream_cache_get_file_stat(file, &st) != 0) {
            return;
        }

        if (stream_build_identity(file, st, &identity, nullptr, nullptr, nullptr) != 0) {
            return;
        }
    }

    g_stream_cache_lock.lock();
    stream_invalidate_identity_locked(identity);
    stream_gc_locked(stream_now_us());
    g_stream_cache_lock.unlock();
}

auto stream_cache_read_eligible(const File* file) -> bool {
    if (file == nullptr || file->vfs_path == nullptr || (file->open_flags & ker::vfs::O_NO_CACHE) != 0) {
        return false;
    }
    if ((file->open_flags & 3) != 0) {
        return false;
    }

    // Keep local XFS out of the stream cache. Build tools frequently read
    // files while another process is still generating them; a fresh stat size
    // can then race ahead of XFS-readable data and make a valid short read look
    // like a cache-level premature EOF.
    return file->fs_type == FSType::TMPFS || file->fs_type == FSType::FAT32 || file->fs_type == FSType::REMOTE;
}

void stream_invalidate_scope_locked(FSType fs_type, const void* scope_key) {
    if (scope_key == nullptr) {
        return;
    }

    for (auto& entry : g_stream_cache) {
        if (entry != nullptr && entry->identity.fs_type == fs_type && entry->identity.scope_key == scope_key) {
            stream_reset_entry_locked(entry.get());
        }
    }
}

void stream_invalidate_mount_scope(FSType fs_type, const void* scope_key) {
    g_stream_cache_lock.lock();
    stream_invalidate_scope_locked(fs_type, scope_key);
    stream_gc_locked(stream_now_us());
    g_stream_cache_lock.unlock();
}

void cache_notify_register_open_file_impl(File* file) {
    if (file == nullptr || file->vfs_path == nullptr || (file->open_flags & ker::vfs::O_NOTIFY_CACHE_CHANGE) == 0) {
        return;
    }

    cache_notify_attach_file(file);

    if ((file->open_flags & ker::vfs::O_NO_CACHE) != 0) {
        return;
    }

    StreamCacheIdentity identity = {};
    StreamFreshnessStamp freshness = {};
    bool can_reuse_detached = false;
    bool retain_full_file = false;
    if (cache_notify_try_copy_snapshot(file, &identity, &freshness, &can_reuse_detached, &retain_full_file)) {
        return;
    }

    Stat st = {};
    if (!file_stat_snapshot_lookup(file, &st)) {
        if (file_stat_snapshot_refresh_from_backend(file, &st) != 0) {
            return;
        }
    }

    if (stream_build_identity(file, st, &identity, &freshness, &can_reuse_detached, &retain_full_file) != 0) {
        return;
    }

    cache_notify_store_snapshot(file, identity, freshness, can_reuse_detached, retain_full_file);
}

void cache_notify_invalidate_path_impl(const char* vfs_path) {
    if (vfs_path != nullptr) {
        metadata_cache_note_path_changed(vfs_path, nullptr);
    }
    cache_notify_invalidate_path_local(vfs_path);
}

void cache_notify_path_changed_impl(const char* old_vfs_path, const char* new_vfs_path) {
    if (old_vfs_path != nullptr || new_vfs_path != nullptr) {
        metadata_cache_note_path_changed(old_vfs_path, new_vfs_path);
    }
    bool old_became_dirty = false;
    bool new_became_dirty = false;
    if (old_vfs_path != nullptr) {
        old_became_dirty = cache_notify_invalidate_path_local(old_vfs_path);
    }
    if (new_vfs_path != nullptr && (old_vfs_path == nullptr || std::strcmp(old_vfs_path, new_vfs_path) != 0)) {
        new_became_dirty = cache_notify_invalidate_path_local(new_vfs_path);
    }
    // Invalidation notifications are edge-triggered: once a path is dirty,
    // further writes do not need another network broadcast until some reader
    // revalidates and stores a fresh snapshot.
    if (old_became_dirty || new_became_dirty) {
        ker::net::wki::wki_remote_vfs_notify_path_changed(old_vfs_path, new_vfs_path);
    }
}

void cache_notify_path_data_changed_impl(const char* vfs_path, FSType fs_type) {
    metadata_cache_note_path_data_changed(vfs_path, fs_type);
    if (cache_notify_invalidate_path_local(vfs_path)) {
        ker::net::wki::wki_remote_vfs_notify_path_changed(vfs_path, nullptr);
    }
}

void cache_notify_file_changed_impl(File* file) {
    if (file == nullptr) {
        return;
    }
    stream_invalidate_file(file);
    // devfs-backed character devices (for example PTYs) can carry stream I/O
    // without any underlying pathname/content mutation. Treating each write as
    // a path change floods remote VFS peers with bogus invalidates such as
    // /dev/ptmx and /dev/pts/N.
    if (file->fs_type == FSType::DEVFS) {
        return;
    }
    cache_notify_path_changed_impl(file->vfs_path, nullptr);
}

void cache_notify_file_data_changed_impl(File* file) {
    if (file == nullptr) {
        return;
    }

    stream_invalidate_file(file);
    file_stat_snapshot_invalidate(file);
    if (file->fs_type == FSType::DEVFS) {
        return;
    }

    metadata_cache_note_file_data_changed(file);

    if (cache_notify_invalidate_path_local(file->vfs_path)) {
        ker::net::wki::wki_remote_vfs_notify_path_changed(file->vfs_path, nullptr);
    }
}

void cache_notify_file_metadata_changed_impl(File* file) {
    if (file == nullptr) {
        return;
    }

    file_stat_snapshot_invalidate(file);
    cache_notify_path_data_changed_impl(file->vfs_path, file->fs_type);
}

auto cache_notify_file_dirty_impl(File* file) -> bool {
    if (file == nullptr || file->cache_notify_attachment == nullptr) {
        return false;
    }

    auto* watcher = static_cast<CacheNotifyWatcher*>(file->cache_notify_attachment);
    if (!cache_notify_attach_pointer_looks_valid(watcher) || watcher->magic != CacheNotifyWatcher::MAGIC) {
        return false;
    }

    bool dirty = false;
    g_cache_notify_lock.lock();
    if (watcher->entry != nullptr) {
        dirty = watcher->seen_generation != watcher->entry->generation;
    }
    g_cache_notify_lock.unlock();
    return dirty;
}

void cache_notify_acknowledge_file_impl(File* file) {
    if (file == nullptr || file->cache_notify_attachment == nullptr) {
        return;
    }

    auto* watcher = static_cast<CacheNotifyWatcher*>(file->cache_notify_attachment);
    if (!cache_notify_attach_pointer_looks_valid(watcher) || watcher->magic != CacheNotifyWatcher::MAGIC) {
        return;
    }

    g_cache_notify_lock.lock();
    if (watcher->entry != nullptr) {
        watcher->seen_generation = watcher->entry->generation;
        watcher->entry->last_used_us = stream_now_us();
    }
    g_cache_notify_lock.unlock();
}

auto vfs_stream_cache_try_read(File* file, void* buf, size_t count, uint64_t start_offset, size_t* actual_size, ssize_t* result) -> bool {
    if (result == nullptr || file == nullptr || buf == nullptr || count == 0 || file->fops == nullptr || file->fops->vfs_read == nullptr) {
        return false;
    }

    auto* attachment = stream_attach_file(file);
    if (attachment == nullptr || attachment->entry == nullptr) {
        g_vfs_stream_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    auto* dst = static_cast<uint8_t*>(buf);
    size_t total = 0;
    int premature_eof_retries = 0;

    while (total < count) {
        uint64_t const OFFSET = start_offset + total;

        g_stream_cache_lock.lock();
        auto* island = stream_select_island_locked(attachment, OFFSET);
        if (island == nullptr) {
            g_stream_cache_lock.unlock();
            g_vfs_stream_misses.fetch_add(1, std::memory_order_relaxed);
            break;
        }

        attachment->entry->last_used_us = stream_now_us();
        island->last_access_us = attachment->entry->last_used_us;

        size_t const COPIED = stream_copy_available_locked(attachment, OFFSET, dst + total, count - total);
        if (COPIED > 0) {
            attachment->desired_offset = OFFSET + COPIED;
            stream_trim_island_front_locked(attachment->entry, island);
            stream_enforce_entry_cap_locked(attachment->entry, attachment->desired_offset);
            g_stream_cache_lock.unlock();
            g_vfs_stream_hits.fetch_add(1, std::memory_order_relaxed);
            g_vfs_stream_copied_bytes.fetch_add(COPIED, std::memory_order_relaxed);
            total += COPIED;
            continue;
        }

        if (island->error != 0) {
            int const ERR = island->error;
            g_stream_cache_lock.unlock();
            if (total == 0) {
                *result = ERR;
                return true;
            }
            break;
        }

        uint64_t const END = stream_island_end(island);
        if (island->eof && OFFSET >= END) {
            g_stream_cache_lock.unlock();
            break;
        }

        if (island->producer_active) {
            g_stream_cache_lock.unlock();
            ker::mod::sched::kern_yield();
            continue;
        }

        uint64_t const FETCH_OFFSET = (OFFSET > island->next_fetch_offset) ? OFFSET : island->next_fetch_offset;
        island->producer_active = true;
        g_stream_cache_lock.unlock();

        auto chunk = std::make_unique<StreamChunk>();
        chunk->offset = FETCH_OFFSET;
        g_vfs_stream_backend_reads.fetch_add(1, std::memory_order_relaxed);
        ssize_t const READ_RET = clamp_io_count(
            file->fops->vfs_read(file, chunk->data.data(), STREAM_CHUNK_SIZE, static_cast<size_t>(FETCH_OFFSET)), STREAM_CHUNK_SIZE);
        if (READ_RET > 0) {
            g_vfs_stream_backend_bytes.fetch_add(static_cast<uint64_t>(READ_RET), std::memory_order_relaxed);
        }

        g_stream_cache_lock.lock();
        island->producer_active = false;
        bool retry_premature_eof = false;
        if (READ_RET > 0) {
            chunk->size = static_cast<uint32_t>(READ_RET);
            island->next_fetch_offset = FETCH_OFFSET + chunk->size;
            attachment->entry->cached_bytes += chunk->size;
            island->chunks.push_back(std::move(chunk));
            if (attachment->entry->freshness.size > 0 &&
                std::cmp_greater_equal(island->next_fetch_offset, attachment->entry->freshness.size)) {
                island->eof = true;
            }
            if (!attachment->entry->retain_full_file) {
                stream_trim_island_front_locked(attachment->entry, island);
            }
            stream_enforce_entry_cap_locked(attachment->entry, OFFSET);
            premature_eof_retries = 0;
        } else if (READ_RET == 0) {
            bool const PREMATURE_EOF =
                attachment->entry->freshness.size > 0 && std::cmp_less(FETCH_OFFSET, attachment->entry->freshness.size);
            if (PREMATURE_EOF && premature_eof_retries < STREAM_PREMATURE_EOF_RETRIES) {
                premature_eof_retries++;
                retry_premature_eof = true;
            } else if (PREMATURE_EOF) {
                island->error = -EIO;
                log::warn("stream cache: premature EOF for %s at offset=%llu size=%llu after %d retries", file->vfs_path,
                          static_cast<unsigned long long>(FETCH_OFFSET), static_cast<unsigned long long>(attachment->entry->freshness.size),
                          STREAM_PREMATURE_EOF_RETRIES);
            } else {
                island->eof = true;
            }
        } else {
            island->error = static_cast<int>(READ_RET);
        }
        g_stream_cache_lock.unlock();

        if (retry_premature_eof) {
            log::warn("stream cache: retrying premature EOF for %s at offset=%llu size=%llu attempt=%d/%d", file->vfs_path,
                      static_cast<unsigned long long>(FETCH_OFFSET), static_cast<unsigned long long>(attachment->entry->freshness.size),
                      premature_eof_retries, STREAM_PREMATURE_EOF_RETRIES);
            ker::mod::sched::kern_yield();
            continue;
        }

        if (READ_RET < 0) {
            if (total == 0) {
                *result = READ_RET;
                return true;
            }
            break;
        }
        if (READ_RET == 0) {
            break;
        }
    }

    if (actual_size != nullptr) {
        *actual_size = total;
    }
    *result = static_cast<ssize_t>(total);
    return true;
}

auto copy_path_string(const char* src, char* dst, size_t dst_size) -> int {
    if (src == nullptr || dst == nullptr || dst_size == 0) {
        return -EINVAL;
    }

    size_t const LEN = std::strlen(src);
    if (LEN + 1 > dst_size) {
        return -ENAMETOOLONG;
    }

    std::memcpy(dst, src, LEN + 1);
    return 0;
}

auto path_requires_directory(const char* path) -> bool {
    if (path == nullptr || path[0] == '\0') {
        return false;
    }

    size_t end = std::strlen(path);
    while (end > 0 && path[end - 1] == '/') {
        end--;
    }

    return end > 0 && path[end] == '/';
}

auto tmpfs_root_for_mount(const MountPoint* mount) -> ker::vfs::tmpfs::TmpNode* {
    if (mount != nullptr && mount->fs_type == FSType::TMPFS && mount->private_data != nullptr) {
        return ker::vfs::tmpfs::mount_root(static_cast<ker::vfs::tmpfs::TmpfsMount*>(mount->private_data));
    }
    return ker::vfs::tmpfs::get_root_node();
}

auto tmpfs_resolve_parent_directory_and_name(ker::vfs::tmpfs::TmpNode* root, const char* fs_path, ker::vfs::tmpfs::TmpNode** parent_out,
                                             const char** name_out) -> int {
    if (root == nullptr || fs_path == nullptr || parent_out == nullptr || name_out == nullptr) {
        return -EINVAL;
    }

    *parent_out = nullptr;
    *name_out = nullptr;

    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    *name_out = (last_slash == nullptr) ? fs_path : last_slash + 1;
    if (**name_out == '\0') {
        return -ENOENT;
    }

    const char* const PARENT_END = (last_slash == nullptr) ? fs_path : last_slash;
    ker::vfs::tmpfs::tmpfs_lock_tree();
    auto* current = root;
    int result = 0;
    const char* cursor = fs_path;

    while (result == 0 && cursor < PARENT_END) {
        while (cursor < PARENT_END && *cursor == '/') {
            ++cursor;
        }
        if (cursor >= PARENT_END) {
            break;
        }

        std::array<char, ker::vfs::tmpfs::TMPFS_NAME_MAX> component{};
        size_t component_len = 0;
        while (cursor < PARENT_END && *cursor != '/') {
            if (component_len >= ker::vfs::tmpfs::TMPFS_NAME_MAX - 1) {
                result = -ENAMETOOLONG;
                break;
            }
            component.at(component_len) = *cursor;
            ++component_len;
            ++cursor;
        }
        if (result < 0) {
            break;
        }
        component.at(component_len) = '\0';

        if (std::strcmp(component.data(), ".") == 0) {
            continue;
        }
        if (std::strcmp(component.data(), "..") == 0) {
            if (current->parent != nullptr) {
                current = current->parent;
            }
            continue;
        }
        if (current->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
            result = -ENOTDIR;
            break;
        }

        current = ker::vfs::tmpfs::tmpfs_lookup(current, component.data());
        if (current == nullptr) {
            result = -ENOENT;
            break;
        }
    }

    if (result == 0 && current->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        result = -ENOTDIR;
    }
    if (result == 0) {
        *parent_out = current;
    }
    ker::vfs::tmpfs::tmpfs_unlock_tree();
    return result;
}

auto task_submitter_hostname(const ker::mod::sched::task::Task* task) -> const char* {
    if (task != nullptr && task->wki_submitter_hostname[0] != '\0') {
        return task->wki_submitter_hostname.data();
    }
    return ker::net::wki::g_wki.local_hostname.data();
}

auto path_prefix_matches(const char* path, const char* prefix, size_t prefix_len) -> bool {
    if (path == nullptr || prefix == nullptr || prefix_len == 0) {
        return false;
    }

    if (prefix_len == 1 && prefix[0] == '/') {
        return path[0] == '/';
    }

    if (std::strncmp(path, prefix, prefix_len) != 0) {
        return false;
    }

    return path[prefix_len] == '\0' || path[prefix_len] == '/';
}

auto strip_mount_prefix(const MountPoint* mount, const char* path) -> const char* {
    if (mount == nullptr || path == nullptr) {
        return path;
    }

    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    if (mount_len == 1 && mount->path[0] == '/') {
        return path + 1;
    }
    if (path[mount_len] == '/') {
        return path + mount_len + 1;
    }
    if (path[mount_len] == '\0') {
        return "";
    }
    return path + mount_len;
}

auto find_first_mount_child(const char* path) -> MountRef {
    if (path == nullptr) {
        return MountRef{};
    }

    size_t const PATH_LEN = std::strlen(path);
    for (size_t mi = 0; mi < get_mount_count(); ++mi) {
        auto mount_ref = get_mount_at(mi);
        MountPoint* mp = mount_ref.get();
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }

        size_t const MP_LEN = std::strlen(mp->path);
        if (MP_LEN > PATH_LEN && std::strncmp(mp->path, path, PATH_LEN) == 0 && mp->path[PATH_LEN] == '/') {
            return mount_ref;
        }
    }

    return MountRef{};
}

bool is_logical_wki_root_dir(const char* path) {
    if (path == nullptr) {
        return false;
    }

    if (std::strcmp(path, "/wki") == 0) {
        return true;
    }

    if (!ker::mod::sched::can_query_current_task()) {
        return false;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }

    size_t const ROOT_LEN = std::strlen(task->root.data());
    if (ROOT_LEN <= 1) {
        return false;
    }

    return std::strncmp(path, task->root.data(), ROOT_LEN) == 0 && std::strcmp(path + ROOT_LEN, "/wki") == 0;
}

bool logical_wki_root_has_mount_child() {
    std::array<char, MAX_PATH_LEN> resolved{"/wki"};

    if (ker::mod::sched::can_query_current_task()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t const ROOT_LEN = std::strlen(task->root.data());
            if (ROOT_LEN > 1) {
                if (ROOT_LEN + 4 >= resolved.size()) {
                    return false;
                }
                std::memcpy(resolved.data(), task->root.data(), ROOT_LEN);
                std::memcpy(resolved.data() + ROOT_LEN, "/wki", 5);
            }
        }
    }

    return static_cast<bool>(find_first_mount_child(resolved.data()));
}

auto fill_synthetic_mount_dir_stat(const char* path, Stat* statbuf) -> int {
    if (statbuf == nullptr) {
        return -EINVAL;
    }

    auto child_mount_ref = find_first_mount_child(path);
    MountPoint const* child_mount = child_mount_ref.get();
    if (child_mount == nullptr && !(ker::net::wki::g_wki.initialized && is_logical_wki_root_dir(path))) {
        return -ENOENT;
    }

    std::memset(statbuf, 0, sizeof(Stat));
    statbuf->st_dev = 0;
    const void* synthetic_anchor = child_mount != nullptr ? static_cast<const void*>(child_mount) : static_cast<const void*>(path);
    statbuf->st_ino = reinterpret_cast<ino_t>(synthetic_anchor);
    statbuf->st_nlink = 2;
    statbuf->st_uid = 0;
    statbuf->st_gid = 0;
    statbuf->st_rdev = 0;
    statbuf->st_size = 0;
    statbuf->st_blksize = 4096;
    statbuf->st_blocks = 0;
    statbuf->st_mode = S_IFDIR | 0755;
    return 0;
}

auto create_synthetic_mount_dir_file(const char* path, FSType fs_type) -> File* {
    if (!find_first_mount_child(path) && !(ker::net::wki::g_wki.initialized && is_logical_wki_root_dir(path))) {
        return nullptr;
    }

    auto* file = new File;
    file->fd = -1;
    file->private_data = nullptr;
    file->fops = nullptr;
    file->pos = 0;
    file->is_directory = true;
    file->fs_type = fs_type;
    file->refcount = 1;
    file->open_flags = 0;
    file->fd_flags = 0;
    file->vfs_path = nullptr;
    file->dir_fs_count = static_cast<size_t>(-1);
    file->stream_cache_attachment = nullptr;
    return file;
}

auto strip_task_root_prefix(const ker::mod::sched::task::Task* task, const char* path, char* out, size_t out_size, bool* stripped) -> int {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return -EINVAL;
    }

    if (stripped != nullptr) {
        *stripped = false;
    }

    if (task == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    size_t const ROOT_LEN = std::strlen(task->root.data());
    if (ROOT_LEN <= 1) {
        return copy_path_string(path, out, out_size);
    }

    if (std::strncmp(path, task->root.data(), ROOT_LEN) != 0 || (path[ROOT_LEN] != '\0' && path[ROOT_LEN] != '/')) {
        return copy_path_string(path, out, out_size);
    }

    if (stripped != nullptr) {
        *stripped = true;
    }

    const char* logical_path = path + ROOT_LEN;
    if (*logical_path == '\0') {
        return copy_path_string("/", out, out_size);
    }

    return copy_path_string(logical_path, out, out_size);
}

auto strip_current_task_root_prefix(const char* path, char* out, size_t out_size) -> int {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return -EINVAL;
    }

    if (!ker::mod::sched::can_query_current_task()) {
        return copy_path_string(path, out, out_size);
    }

    auto* task = ker::mod::sched::get_current_task();
    return strip_task_root_prefix(task, path, out, out_size, nullptr);
}

auto build_wki_host_path(const char* hostname, const char* suffix, char* out, size_t out_size) -> int {
    if (hostname == nullptr || hostname[0] == '\0') {
        return -ENOENT;
    }

    const char* trimmed_suffix = suffix;
    while (trimmed_suffix != nullptr && *trimmed_suffix == '/') {
        trimmed_suffix++;
    }

    size_t const HOST_LEN = std::strlen(hostname);
    size_t const SUFFIX_LEN = (trimmed_suffix != nullptr) ? std::strlen(trimmed_suffix) : 0;
    size_t const TOTAL = WKI_PATH_PREFIX_LEN + HOST_LEN + (SUFFIX_LEN > 0 ? 1 + SUFFIX_LEN : 0) + 1;
    if (TOTAL > out_size) {
        return -ENAMETOOLONG;
    }

    size_t pos = 0;
    std::memcpy(out + pos, "/wki/", WKI_PATH_PREFIX_LEN);
    pos += WKI_PATH_PREFIX_LEN;
    std::memcpy(out + pos, hostname, HOST_LEN);
    pos += HOST_LEN;
    if (SUFFIX_LEN > 0) {
        out[pos++] = '/';
        std::memcpy(out + pos, trimmed_suffix, SUFFIX_LEN);
        pos += SUFFIX_LEN;
    }
    out[pos] = '\0';
    return 0;
}

inline constexpr bool ENABLE_LOADER_PATH_TRACE = false;

auto is_loader_debug_path(const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }
    return std::strcmp(path, "libc.so") == 0 || std::strcmp(path, "libc++abi.so") == 0 || std::strcmp(path, "debugserver") == 0;
}

void log_loader_path_event(const char* stage, const char* raw_path, const char* resolved_path, const MountPoint* mount, int rc) {
    if constexpr (!ENABLE_LOADER_PATH_TRACE) {
        return;
    }
    if (!is_loader_debug_path(raw_path) && !is_loader_debug_path(resolved_path)) {
        return;
    }

    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    char const* task_name = (task != nullptr) ? task->name : "?";
    uint64_t const PID = (task != nullptr) ? task->pid : 0;
    char const* submitter = (task != nullptr && task->wki_submitter_hostname[0] != '\0') ? task->wki_submitter_hostname.data() : "-";
    char const* mount_path = (mount != nullptr && mount->path != nullptr) ? mount->path : "-";
    int const MOUNT_TYPE = (mount != nullptr) ? static_cast<int>(mount->fs_type) : -1;

    log::info("loader-path: stage=%s pid=%llx task=%s submitter=%s raw=%s resolved=%s mount=%s fs=%d rc=%d", stage,
              static_cast<unsigned long long>(PID), task_name, submitter, raw_path != nullptr ? raw_path : "-",
              resolved_path != nullptr ? resolved_path : "-", mount_path, MOUNT_TYPE, rc);
}

auto ensure_wki_host_root_mount(const char* path) -> int {
    if (path == nullptr || !ker::net::wki::g_wki.initialized) {
        return 0;
    }

    std::array<char, MAX_PATH_LEN> logical{};
    if (strip_current_task_root_prefix(path, logical.data(), logical.size()) < 0) {
        return 0;
    }

    constexpr std::string_view WKI_PREFIX{"/wki/"};
    if (std::strncmp(logical.data(), WKI_PREFIX.data(), WKI_PREFIX.size()) != 0) {
        return 0;
    }

    const char* host_part = logical.data() + WKI_PREFIX.size();
    const char* host_end = host_part;
    while (*host_end != '\0' && *host_end != '/') {
        host_end++;
    }
    if (host_end == host_part) {
        return 0;
    }

    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> hostname{};
    auto host_len = static_cast<size_t>(host_end - host_part);
    if (host_len >= hostname.size()) {
        return 0;
    }
    std::memcpy(hostname.data(), host_part, host_len);
    hostname[host_len] = '\0';

    if (std::strcmp(hostname.data(), ker::net::wki::g_wki.local_hostname.data()) == 0) {
        return 0;
    }

    std::array<char, MAX_PATH_LEN> mount_root{};
    int const MOUNT_PATH_RET = std::snprintf(mount_root.data(), mount_root.size(), "/wki/%s", hostname.data());
    if (MOUNT_PATH_RET <= 0 || static_cast<size_t>(MOUNT_PATH_RET) >= mount_root.size()) {
        return 0;
    }

    std::array<char, MAX_PATH_LEN> resolved_mount_root{};
    if (resolve_mount_path(mount_root.data(), resolved_mount_root.data(), resolved_mount_root.size()) == 0) {
        auto existing_ref = find_mount_point(resolved_mount_root.data());
        MountPoint const* existing = existing_ref.get();
        if (existing != nullptr && existing->fs_type == FSType::REMOTE && std::strcmp(existing->path, resolved_mount_root.data()) == 0) {
            return 0;
        }
    }

    uint16_t const NODE_ID = ker::net::wki::wki_peer_find_by_hostname(hostname.data());
    if (NODE_ID == ker::net::wki::WKI_NODE_INVALID || NODE_ID == ker::net::wki::g_wki.my_node_id) {
        return 0;
    }
    auto* peer = ker::net::wki::wki_peer_find(NODE_ID);
    if (peer == nullptr || peer->state != ker::net::wki::PeerState::CONNECTED) {
        return 0;
    }

    struct RootExportFindCtx {
        uint16_t node_id;
        ker::net::wki::DiscoveredResource* result;
    };
    RootExportFindCtx find_ctx = {.node_id = NODE_ID, .result = nullptr};
    ker::net::wki::wki_resource_foreach(
        [](const ker::net::wki::DiscoveredResource& res, void* ctx_ptr) {
            auto* ctx = static_cast<RootExportFindCtx*>(ctx_ptr);
            if (ctx == nullptr || ctx->result != nullptr) {
                return;
            }
            bool const IS_ROOT_EXPORT =
                std::strncmp(static_cast<const char*>(res.name), "/", ker::net::wki::DISCOVERED_RESOURCE_NAME_LEN) == 0;
            if (res.node_id == ctx->node_id && res.resource_type == ker::net::wki::ResourceType::VFS && IS_ROOT_EXPORT) {
                ctx->result = const_cast<ker::net::wki::DiscoveredResource*>(&res);
            }
        },
        &find_ctx);

    if (find_ctx.result == nullptr) {
        return 0;
    }

    vfs_mkdir("/wki", 0755);
    vfs_mkdir(mount_root.data(), 0755);

    int const RET = ker::net::wki::wki_remote_vfs_mount(NODE_ID, find_ctx.result->resource_id, mount_root.data());
    if (RET == 0) {
        log::info("auto-mounted WKI host root %s for path %s", mount_root.data(), logical.data());
    }
    return RET;
}

auto rewrite_wki_host_alias(const ker::mod::sched::task::Task* task, const char* path, char* out, size_t out_size) -> int {
    constexpr std::string_view HOST_PREFIX{"/wki/host"};
    constexpr size_t HOST_PREFIX_LEN = HOST_PREFIX.size();

    if (path == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    if (task == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    std::array<char, MAX_PATH_LEN> current{};
    int copy_result = copy_path_string(path, current.data(), current.size());
    if (copy_result < 0) {
        return copy_result;
    }

    std::array<char, MAX_PATH_LEN> self_prefix{};
    size_t self_prefix_len = 0;
    if (ker::net::wki::g_wki.local_hostname[0] != '\0') {
        copy_result = build_wki_host_path(ker::net::wki::g_wki.local_hostname.data(), "", self_prefix.data(), self_prefix.size());
        if (copy_result < 0) {
            return copy_result;
        }
        self_prefix_len = std::strlen(self_prefix.data());
    }

    for (int depth = 0; depth < MAX_SYMLINK_DEPTH; ++depth) {
        bool const REWRITE_HOST_ALIAS = std::strncmp(current.data(), HOST_PREFIX.data(), HOST_PREFIX.size()) == 0 &&
                                        (current[HOST_PREFIX.size()] == '\0' || current[HOST_PREFIX.size()] == '/');
        bool const REWRITE_SELF_ALIAS = self_prefix_len > WKI_PATH_PREFIX_LEN &&
                                        std::strncmp(current.data(), self_prefix.data(), self_prefix_len) == 0 &&
                                        (current[self_prefix_len] == '\0' || current[self_prefix_len] == '/');
        if (!REWRITE_HOST_ALIAS && !REWRITE_SELF_ALIAS) {
            break;
        }

        const char* suffix = nullptr;
        if (REWRITE_HOST_ALIAS) {
            const char* submitter = task_submitter_hostname(task);
            if (submitter != nullptr && submitter[0] != '\0' && std::strcmp(submitter, ker::net::wki::g_wki.local_hostname.data()) != 0) {
                copy_result = build_wki_host_path(submitter, current.data() + HOST_PREFIX_LEN, current.data(), current.size());
                if (copy_result < 0) {
                    return copy_result;
                }
                continue;
            }

            suffix = current.data() + HOST_PREFIX_LEN;
        } else {
            suffix = current.data() + self_prefix_len;
        }

        while (*suffix == '/') {
            suffix++;
        }

        if (*suffix == '\0') {
            copy_result = copy_path_string("/", current.data(), current.size());
        } else {
            size_t const SUFFIX_LEN = std::strlen(suffix);
            if (SUFFIX_LEN + 2 > current.size()) {
                return -ENAMETOOLONG;
            }

            current[0] = '/';
            std::memcpy(current.data() + 1, suffix, SUFFIX_LEN + 1);
            copy_result = 0;
        }

        if (copy_result < 0) {
            return copy_result;
        }
    }

    return copy_path_string(current.data(), out, out_size);
}

auto dir_contains_name(ker::vfs::File* file, bool has_fs_readdir, size_t fs_count, const char* name) -> bool {
    if (!has_fs_readdir || file == nullptr || file->fops == nullptr || file->fops->vfs_readdir == nullptr || name == nullptr) {
        return false;
    }

    DirEntry probe = {};
    size_t const NAME_LEN = std::strlen(name);
    for (size_t index = 0; index < fs_count;) {
        if (file->fops->vfs_readdir(file, &probe, index) != 0) {
            break;
        }

        size_t const PROBE_LEN = std::strlen(probe.d_name.data());
        if (PROBE_LEN == NAME_LEN && std::memcmp(probe.d_name.data(), name, NAME_LEN) == 0) {
            return true;
        }
        size_t next_index = index + 1;
        if (probe.d_off > static_cast<uint64_t>(index) && probe.d_off <= static_cast<uint64_t>(~size_t{0})) {
            next_index = static_cast<size_t>(probe.d_off);
        }
        index = next_index;
    }

    return false;
}

// Re-apply the calling task's root prefix after following an absolute symlink.
// Without this, absolute symlink targets (e.g. /usr/sbin) escape the pivoted
// root and resolve against the global root instead of the task's root.
auto reapply_root_prefix(char* path, size_t bufsize) -> int {
    if (!ker::mod::sched::can_query_current_task()) {
        return 0;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return 0;
    }
    size_t const ROOT_LEN = std::strlen(task->root.data());
    if (ROOT_LEN <= 1) {
        return 0;  // root == "/"
    }
    size_t const PATH_LEN = std::strlen(path);
    if (ROOT_LEN + PATH_LEN + 1 > bufsize) {
        return -ENAMETOOLONG;
    }
    std::memmove(path + ROOT_LEN, path, PATH_LEN + 1);
    std::memcpy(path, task->root.data(), ROOT_LEN);
    return 0;
}

auto splice_symlink_target(const char* original_path, size_t prefix_len, const char* target, char* out, size_t out_size) -> int {
    if (original_path == nullptr || target == nullptr || out == nullptr || out_size == 0) {
        return -EINVAL;
    }

    const char* remainder = original_path + prefix_len;
    while (*remainder == '/') {
        remainder++;
    }

    size_t const REMAINDER_LEN = std::strlen(remainder);
    size_t const TARGET_LEN = std::strlen(target);
    size_t pos = 0;

    if (target[0] == '/') {
        if (TARGET_LEN + 1 > out_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out, target, TARGET_LEN);
        pos = TARGET_LEN;
    } else {
        size_t parent_len = 1;
        for (size_t i = 1; i < prefix_len; ++i) {
            if (original_path[i] == '/') {
                parent_len = i;
            }
        }

        if (parent_len + 1 > out_size) {
            return -ENAMETOOLONG;
        }

        std::memcpy(out, original_path, parent_len);
        pos = parent_len;
        if (pos == 0 || out[pos - 1] != '/') {
            out[pos++] = '/';
        }

        if (pos + TARGET_LEN + 1 > out_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out + pos, target, TARGET_LEN);
        pos += TARGET_LEN;
    }

    if (REMAINDER_LEN > 0) {
        if (pos == 0 || out[pos - 1] != '/') {
            if (pos + 1 >= out_size) {
                return -ENAMETOOLONG;
            }
            out[pos++] = '/';
        }
        if (pos + REMAINDER_LEN + 1 > out_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out + pos, remainder, REMAINDER_LEN + 1);
    } else {
        if (pos >= out_size) {
            return -ENAMETOOLONG;
        }
        out[pos] = '\0';
    }

    return canonicalize_path(out, out_size);
}

auto resolve_prefix_symlink_once(char* path, size_t bufsize, bool apply_task_policy, bool follow_final_symlink) -> int {
    if (path == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    for (size_t end = 1;; ++end) {
        char const CH = path[end];
        if (CH != '/' && CH != '\0') {
            continue;
        }
        if (end == 1) {
            if (CH == '\0') {
                break;
            }
            continue;
        }
        if (!follow_final_symlink && CH == '\0') {
            break;
        }

        std::array<char, MAX_PATH_LEN> prefix{};
        if (end + 1 > prefix.size()) {
            return -ENAMETOOLONG;
        }
        std::memcpy(prefix.data(), path, end);
        prefix[end] = '\0';

        std::array<char, MAX_PATH_LEN> linkbuf{};
        ssize_t const LINK_LEN = readlink_resolved(prefix.data(), linkbuf.data(), linkbuf.size() - 1);
        if (LINK_LEN > 0) {
            linkbuf[LINK_LEN] = '\0';

            std::array<char, MAX_PATH_LEN> substituted{};
            int const SPLICE_RESULT = splice_symlink_target(path, end, linkbuf.data(), substituted.data(), substituted.size());
            if (SPLICE_RESULT < 0) {
                return SPLICE_RESULT;
            }

            int const COPY_RESULT = copy_path_string(substituted.data(), path, bufsize);
            if (COPY_RESULT < 0) {
                return COPY_RESULT;
            }

            // Absolute symlink targets must stay within the task's root.
            if (linkbuf[0] == '/') {
                int const RR = reapply_root_prefix(path, bufsize);
                if (RR < 0) {
                    return RR;
                }
            }

            if (apply_task_policy) {
                int const NORMALIZE = normalize_task_path_inplace(path, bufsize);
                if (NORMALIZE < 0) {
                    return NORMALIZE;
                }
            }

            return 1;
        }

        if (CH == '\0') {
            break;
        }
    }

    return 0;
}

auto choose_task_route(const ker::mod::sched::task::Task* task, const char* path) -> VfsRouteDecision {
    VfsRouteDecision best = {};

    if (task != nullptr) {
        for (const auto& rule : task->wki_vfs_rules) {
            if (rule.prefix_len == 0 || !path_prefix_matches(path, rule.prefix.data(), rule.prefix_len)) {
                continue;
            }
            if (rule.prefix_len > best.prefix_len) {
                best.route = rule.route;
                best.prefix_len = rule.prefix_len;
            }
        }
    }

    for (const auto& rule : g_default_vfs_rules) {
        if (rule.prefix_len == 0 || !path_prefix_matches(path, rule.prefix.data(), rule.prefix_len)) {
            continue;
        }
        if (rule.prefix_len > best.prefix_len) {
            best.route = rule.route;
            best.prefix_len = rule.prefix_len;
        }
    }

    return best;
}

auto apply_task_vfs_route(const ker::mod::sched::task::Task* task, const char* path, char* out, size_t out_size) -> int {
    if (path == nullptr || out == nullptr) {
        return -EINVAL;
    }

    if (task == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    std::array<char, MAX_PATH_LEN> logical_path{};
    bool had_root_prefix = false;
    int const LOGICAL_RESULT = strip_task_root_prefix(task, path, logical_path.data(), logical_path.size(), &had_root_prefix);
    if (LOGICAL_RESULT < 0) {
        return LOGICAL_RESULT;
    }

    std::array<char, MAX_PATH_LEN> aliased{};
    int alias_result = rewrite_wki_host_alias(task, logical_path.data(), aliased.data(), aliased.size());
    if (alias_result < 0) {
        return alias_result;
    }

    VfsRouteDecision const DECISION = choose_task_route(task, aliased.data());
    std::array<char, MAX_PATH_LEN> routed{};
    if (DECISION.route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST)) {
        alias_result = copy_path_string(aliased.data(), routed.data(), routed.size());
    } else {
        const char* submitter = task_submitter_hostname(task);
        if (submitter == nullptr || submitter[0] == '\0' || std::strcmp(submitter, ker::net::wki::g_wki.local_hostname.data()) == 0) {
            alias_result = copy_path_string(aliased.data(), routed.data(), routed.size());
        } else {
            alias_result = build_wki_host_path(submitter, aliased.data(), routed.data(), routed.size());
        }
    }

    if (alias_result < 0) {
        return alias_result;
    }

    if (!had_root_prefix) {
        return copy_path_string(routed.data(), out, out_size);
    }

    size_t const ROOT_LEN = std::strlen(task->root.data());
    if (ROOT_LEN <= 1) {
        return copy_path_string(routed.data(), out, out_size);
    }

    size_t const ROUTED_LEN = std::strlen(routed.data());
    if (ROOT_LEN + ROUTED_LEN + 1 > out_size) {
        return -ENAMETOOLONG;
    }

    std::memmove(out + ROOT_LEN, routed.data(), ROUTED_LEN + 1);
    std::memcpy(out, task->root.data(), ROOT_LEN);
    return 0;
}

auto normalize_task_path_inplace(char* path, size_t bufsize) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    int const CANONICAL = canonicalize_path(path, bufsize);
    if (CANONICAL < 0) {
        return CANONICAL;
    }

    return normalize_task_path_inplace_with_route(path, bufsize, true);
}

auto normalize_task_path_inplace_with_route(char* path, size_t bufsize, bool apply_task_route) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    if (!apply_task_route) {
        return 0;
    }

    std::array<char, MAX_PATH_LEN> routed{};
    ker::mod::sched::task::Task const* current_task =
        ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    int const ROUTE_RESULT = apply_task_vfs_route(current_task, path, routed.data(), routed.size());
    if (ROUTE_RESULT < 0) {
        return ROUTE_RESULT;
    }

    return copy_path_string(routed.data(), path, bufsize);
}

auto resolve_task_path_raw(const char* path, char* out, size_t outsize) -> int {
    return resolve_task_path_raw_impl(path, out, outsize, true);
}

auto resolve_task_path_raw_impl(const char* path, char* out, size_t outsize, bool apply_task_route) -> int {
    int const ABSOLUTE = make_absolute(path, out, outsize);
    if (ABSOLUTE < 0) {
        return ABSOLUTE;
    }

    // Canonicalize before applying the per-task root prefix. If we prepend
    // first, paths like "/.." become "/rootfs/.." and collapse to "/",
    // escaping the pivot_root namespace.
    int const CANONICAL = canonicalize_path(out, outsize);
    if (CANONICAL < 0) {
        return CANONICAL;
    }

    // Prepend per-process root prefix when it differs from "/".
    // This makes pivot_root transparent: after pivot_root("/rootfs", ...),
    // task->root becomes "/rootfs" and all absolute paths get prefixed.
    if (ker::mod::sched::can_query_current_task()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t const ROOT_LEN = std::strlen(task->root.data());
            if (ROOT_LEN > 1) {  // root != "/"
                size_t const PATH_LEN = std::strlen(out);
                if (ROOT_LEN + PATH_LEN + 1 > outsize) {
                    return -ENAMETOOLONG;
                }
                // Shift existing path right to make room for root prefix
                std::memmove(out + ROOT_LEN, out, PATH_LEN + 1);
                std::memcpy(out, task->root.data(), ROOT_LEN);
            }
        }
    }

    return normalize_task_path_inplace_with_route(out, outsize, apply_task_route);
}

auto add_default_vfs_rule(const char* prefix, uint8_t route) -> int {
    if (prefix == nullptr || prefix[0] != '/') {
        return -EINVAL;
    }
    if (route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL) &&
        route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST)) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> canonical{};
    int const COPY_RESULT = copy_path_string(prefix, canonical.data(), canonical.size());
    if (COPY_RESULT < 0) {
        return COPY_RESULT;
    }

    int const CANONICAL_RESULT = canonicalize_path(canonical.data(), canonical.size());
    if (CANONICAL_RESULT < 0) {
        return CANONICAL_RESULT;
    }

    size_t const PREFIX_LEN = std::strlen(canonical.data());
    if (PREFIX_LEN == 0 || PREFIX_LEN >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX) {
        return -ENAMETOOLONG;
    }

    for (auto& rule : g_default_vfs_rules) {
        if (rule.prefix_len == PREFIX_LEN && std::strncmp(rule.prefix.data(), canonical.data(), PREFIX_LEN) == 0) {
            std::memcpy(rule.prefix.data(), canonical.data(), PREFIX_LEN + 1);
            rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
            rule.route = route;
            rule.reserved = 0;
            return 0;
        }
    }

    ker::mod::sched::task::WkiVfsRule new_rule{};
    std::memcpy(new_rule.prefix.data(), canonical.data(), PREFIX_LEN + 1);
    new_rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
    new_rule.route = route;
    new_rule.reserved = 0;
    if (!g_default_vfs_rules.push_back(new_rule)) {
        return -ENOMEM;
    }
    return 0;
}

void install_builtin_vfs_rules() {
    g_default_vfs_rules.clear();
    add_default_vfs_rule("/wki", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/proc", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/dev", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/tmp", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/run", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST));
}

auto parse_vfs_route(const char* route_text, uint8_t* route_out) -> bool {
    if (route_text == nullptr || route_out == nullptr) {
        return false;
    }

    if (std::strcmp(route_text, "local") == 0) {
        *route_out = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
        return true;
    }
    if (std::strcmp(route_text, "host") == 0) {
        *route_out = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST);
        return true;
    }
    return false;
}

void release_open_file(File* file) {
    if (file == nullptr) {
        return;
    }

    if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
        file->fops->vfs_close(file);
    }

    vfs_file_clear_path(file);
    delete file;
}

void load_vfs_rules_from_buffer(char* buffer) {
    if (buffer == nullptr) {
        return;
    }

    char* line = buffer;
    while (*line != '\0') {
        char* line_end = line;
        while (*line_end != '\0' && *line_end != '\n' && *line_end != '\r') {
            line_end++;
        }

        char* next_line = line_end;
        while (*next_line == '\n' || *next_line == '\r') {
            *next_line = '\0';
            next_line++;
        }
        if (*line_end != '\0') {
            *line_end = '\0';
        }

        while (*line == ' ' || *line == '\t') {
            line++;
        }

        if (*line != '\0' && *line != '#') {
            char const* prefix = line;
            while (*line != '\0' && *line != ' ' && *line != '\t') {
                line++;
            }

            if (*line != '\0') {
                *line++ = '\0';
                while (*line == ' ' || *line == '\t') {
                    line++;
                }

                char const* route_text = line;
                while (*line != '\0' && *line != ' ' && *line != '\t') {
                    line++;
                }
                *line = '\0';

                uint8_t route = 0;
                if (parse_vfs_route(route_text, &route)) {
                    add_default_vfs_rule(prefix, route);
                }
            }
        }

        line = next_line;
    }
}

// Convert a possibly-relative path to an absolute path by prepending CWD.
// If the path is already absolute, copies it as-is.
// Returns 0 on success, negative on error.
auto make_absolute(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }
    size_t const PLEN = std::strlen(path);
    if (PLEN == 0) {
        return -EINVAL;
    }

    if (path[0] == '/') {
        if (PLEN + 1 > outsize) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out, path, PLEN + 1);
        return 0;
    }

    // Relative path - prepend task CWD
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    size_t const CWDLEN = std::strlen(task->cwd.data());
    // Need: cwd + "/" + path + '\0'
    bool const NEED_SEP = (CWDLEN > 1);  // Root "/" doesn't need extra /
    size_t const TOTAL = CWDLEN + (NEED_SEP ? 1 : 0) + PLEN + 1;
    if (TOTAL > outsize) {
        return -ENAMETOOLONG;
    }

    std::memcpy(out, task->cwd.data(), CWDLEN);
    if (NEED_SEP) {
        out[CWDLEN] = '/';
        std::memcpy(out + CWDLEN + 1, path, PLEN + 1);
    } else {
        std::memcpy(out + CWDLEN, path, PLEN + 1);
    }
    return 0;
}

// Canonicalize a path in place: resolve ".", "..", and collapse "//".
// The path must be absolute (start with "/").
// Returns 0 on success, -ENAMETOOLONG if the path is too long.
auto canonicalize_path(char* path, size_t bufsize) -> int {
    if (path == nullptr || bufsize == 0 || path[0] != '/') {
        return -EINVAL;
    }

    // Split into components, resolving . and ..
    const char* components[MAX_COMPONENTS];  // NOLINT
    size_t num_components = 0;

    char* p = path + 1;  // skip leading /
    while (*p != '\0') {
        // Skip slashes
        while (*p == '/') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        // Find end of component
        char const* comp_start = p;
        while (*p != '\0' && *p != '/') {
            p++;
        }

        // Null-terminate this component in the buffer
        char const SAVED = *p;
        *p = '\0';

        if (comp_start[0] == '.' && comp_start[1] == '\0') {
            // "." - skip
        } else if (comp_start[0] == '.' && comp_start[1] == '.' && comp_start[2] == '\0') {
            // ".." - pop last component
            if (num_components > 0) {
                num_components--;
            }
        } else {
            if (num_components >= MAX_COMPONENTS) {
                return -ENAMETOOLONG;
            }
            components[num_components++] = comp_start;
        }

        // Keep the null terminator in place - the component pointers
        // stored in components[] depend on it for correct strlen/memcpy
        // during reconstruction.  Parsing still works because we advance
        // p past the '\0' below.
        if (SAVED == '/') {
            p++;
        }
    }

    // Reconstruct canonical path
    char result[MAX_PATH_LEN];  // NOLINT
    size_t pos = 0;
    result[pos++] = '/';

    for (size_t i = 0; i < num_components; ++i) {
        if (i > 0) {
            if (pos >= MAX_PATH_LEN - 1) {
                return -ENAMETOOLONG;
            }
            result[pos++] = '/';
        }
        size_t const COMP_LEN = std::strlen(components[i]);
        if (pos + COMP_LEN >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(result) + pos, components[i], COMP_LEN);
        pos += COMP_LEN;
    }
    result[pos] = '\0';

    if (pos >= bufsize) {
        return -ENAMETOOLONG;
    }
    std::memcpy(path, static_cast<const char*>(result), pos + 1);
    return 0;
}

// Resolve symlinks in a path. The resolved path is written to resolved_buf.
// Returns 0 on success, -ELOOP on too many symlinks, or another negative errno.
auto resolve_symlinks(const char* path, char* resolved_buf, size_t bufsize, bool apply_task_policy = false,
                      bool follow_final_symlink = true) -> int {
    if (path == nullptr || resolved_buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    // Copy initial path to working buffer
    size_t path_len = 0;
    while (path[path_len] != '\0' && path_len < bufsize - 1) {
        resolved_buf[path_len] = path[path_len];
        path_len++;
    }
    resolved_buf[path_len] = '\0';

    if (apply_task_policy) {
        int const NORMALIZE = normalize_task_path_inplace(resolved_buf, bufsize);
        if (NORMALIZE < 0) {
            return NORMALIZE;
        }
    }

    for (int depth = 0; depth < MAX_SYMLINK_DEPTH; ++depth) {
        int const PREFIX_RESULT = resolve_prefix_symlink_once(resolved_buf, bufsize, apply_task_policy, follow_final_symlink);
        if (PREFIX_RESULT < 0) {
            return PREFIX_RESULT;
        }
        if (PREFIX_RESULT > 0) {
            continue;
        }
        if (!follow_final_symlink) {
            return 0;
        }

        auto mount_ref = find_mount_point(resolved_buf);
        MountPoint const* mount = mount_ref.get();
        if (mount == nullptr) {
            return 0;
        }

        if (mount->fs_type == FSType::PROCFS) {
            // Handle procfs symlinks (e.g., /proc/self -> /proc/<pid>)
            size_t mount_len = 0;
            while (mount->path[mount_len] != '\0') {
                mount_len++;
            }
            const char* fs_path = resolved_buf;
            if (mount_len == 1 && mount->path[0] == '/') {
                fs_path = resolved_buf + 1;
            } else if (resolved_buf[mount_len] == '/') {
                fs_path = resolved_buf + mount_len + 1;
            } else if (resolved_buf[mount_len] == '\0') {
                fs_path = "";
            } else {
                fs_path = resolved_buf + mount_len;
            }

            auto* f = ker::vfs::procfs::procfs_open_path(fs_path, 0, 0);
            if (f == nullptr) {
                return 0;
            }
            auto* pfd = static_cast<ker::vfs::procfs::ProcFileData*>(f->private_data);
            bool const IS_SYMLINK = (pfd != nullptr && (pfd->node.type == ker::vfs::procfs::ProcNodeType::SELF_LINK ||
                                                        pfd->node.type == ker::vfs::procfs::ProcNodeType::EXE_LINK ||
                                                        pfd->node.type == ker::vfs::procfs::ProcNodeType::CWD_LINK ||
                                                        pfd->node.type == ker::vfs::procfs::ProcNodeType::ROOT_LINK ||
                                                        pfd->node.type == ker::vfs::procfs::ProcNodeType::FD_LINK));
            if (!IS_SYMLINK) {
                ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
                delete f;
                return 0;
            }
            std::array<char, MAX_PATH_LEN> linkbuf{};
            ssize_t const LINK_LEN = ker::vfs::procfs::get_procfs_fops()->vfs_readlink(f, linkbuf.data(), linkbuf.size());
            ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
            delete f;
            if (LINK_LEN <= 0) {
                return 0;
            }
            linkbuf[static_cast<size_t>(LINK_LEN)] = '\0';
            if (linkbuf[0] == '/') {
                if (std::cmp_greater_equal(LINK_LEN, bufsize)) {
                    return -ENAMETOOLONG;
                }
                std::memcpy(resolved_buf, linkbuf.data(), static_cast<size_t>(LINK_LEN) + 1);
                int const RR = reapply_root_prefix(resolved_buf, bufsize);
                if (RR < 0) {
                    return RR;
                }
            } else {
                size_t last_slash = 0;
                bool found_slash = false;
                for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                    if (resolved_buf[i] == '/') {
                        last_slash = i;
                        found_slash = true;
                    }
                }
                size_t const PREFIX_LEN = found_slash ? last_slash + 1 : 0;
                if (PREFIX_LEN + static_cast<size_t>(LINK_LEN) >= bufsize) {
                    return -ENAMETOOLONG;
                }
                std::array<char, MAX_PATH_LEN> new_path{};
                std::memcpy(new_path.data(), resolved_buf, PREFIX_LEN);
                std::memcpy(new_path.data() + PREFIX_LEN, linkbuf.data(), static_cast<size_t>(LINK_LEN));
                new_path[PREFIX_LEN + static_cast<size_t>(LINK_LEN)] = '\0';
                std::memcpy(resolved_buf, new_path.data(), PREFIX_LEN + static_cast<size_t>(LINK_LEN) + 1);
            }
            if (apply_task_policy) {
                int const NORMALIZE = normalize_task_path_inplace(resolved_buf, bufsize);
                if (NORMALIZE < 0) {
                    return NORMALIZE;
                }
            }
            continue;  // re-resolve after substitution
        }

        // Remote mounts: ask the server to resolve symlinks
        if (mount->fs_type == FSType::REMOTE) {
            size_t mount_len = 0;
            while (mount->path[mount_len] != '\0') {
                mount_len++;
            }
            const char* fs_path = resolved_buf;
            if (mount_len == 1 && mount->path[0] == '/') {
                fs_path = resolved_buf + 1;
            } else if (resolved_buf[mount_len] == '/') {
                fs_path = resolved_buf + mount_len + 1;
            } else if (resolved_buf[mount_len] == '\0') {
                fs_path = "";
            } else {
                fs_path = resolved_buf + mount_len;
            }

            if (fs_path[0] == '\0') {
                return 0;
            }

            std::array<char, MAX_PATH_LEN> linkbuf{};
            ssize_t const LINK_LEN =
                ker::net::wki::wki_remote_vfs_readlink_path(mount->private_data, fs_path, linkbuf.data(), linkbuf.size() - 1);
            if (LINK_LEN <= 0) {
                return 0;  // Not a symlink or readlink failed - resolution complete
            }
            linkbuf[static_cast<size_t>(LINK_LEN)] = '\0';

            if (linkbuf[0] == '/') {
                // Absolute symlink target - replace entire path
                if (std::cmp_greater_equal(LINK_LEN, bufsize)) {
                    return -ENAMETOOLONG;
                }
                std::memcpy(resolved_buf, linkbuf.data(), static_cast<size_t>(LINK_LEN) + 1);
                int const RR = reapply_root_prefix(resolved_buf, bufsize);
                if (RR < 0) {
                    return RR;
                }
            } else {
                // Relative symlink target - replace last component
                size_t last_slash = 0;
                bool found_slash = false;
                for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                    if (resolved_buf[i] == '/') {
                        last_slash = i;
                        found_slash = true;
                    }
                }
                size_t const PREFIX_LEN = found_slash ? last_slash + 1 : 0;
                if (PREFIX_LEN + static_cast<size_t>(LINK_LEN) >= bufsize) {
                    return -ENAMETOOLONG;
                }
                std::array<char, MAX_PATH_LEN> new_path{};
                std::memcpy(new_path.data(), resolved_buf, PREFIX_LEN);
                std::memcpy(new_path.data() + PREFIX_LEN, linkbuf.data(), static_cast<size_t>(LINK_LEN));
                new_path[PREFIX_LEN + static_cast<size_t>(LINK_LEN)] = '\0';
                std::memcpy(resolved_buf, new_path.data(), PREFIX_LEN + static_cast<size_t>(LINK_LEN) + 1);
            }
            if (apply_task_policy) {
                int const NORMALIZE = normalize_task_path_inplace(resolved_buf, bufsize);
                if (NORMALIZE < 0) {
                    return NORMALIZE;
                }
            }
            continue;  // Re-resolve after substitution
        }

        // The prefix pass above already checked the final XFS component via
        // readlink_resolved(). Avoid immediately reopening the same path for
        // another negative readlink probe on common non-symlink files.
        if (mount->fs_type == FSType::XFS) {
            return 0;
        }

        // The remaining legacy final-node walk is only for tmpfs. Other local
        // final symlinks are resolved by the prefix pass above.
        if (mount->fs_type != FSType::TMPFS) {
            return 0;
        }

        // Strip mount prefix to get fs-relative path
        size_t mount_len = 0;
        while (mount->path[mount_len] != '\0') {
            mount_len++;
        }

        const char* fs_path = resolved_buf;
        if (mount_len == 1 && mount->path[0] == '/') {
            fs_path = resolved_buf + 1;
        } else if (resolved_buf[mount_len] == '/') {
            fs_path = resolved_buf + mount_len + 1;
        } else if (resolved_buf[mount_len] == '\0') {
            fs_path = "";
        } else {
            fs_path = resolved_buf + mount_len;
        }

        // Walk the tmpfs path to find the node
        auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
        if (node == nullptr) {
            return 0;  // Path doesn't exist yet (might be created with O_CREAT)
        }
        node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
        if (node == nullptr) {
            return 0;
        }

        if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK) {
            return 0;  // Not a symlink, resolution complete
        }

        if (node->symlink_target == nullptr) {
            return -EIO;
        }

        // Build the new path
        std::array<char, MAX_PATH_LEN> new_path{};
        size_t target_len = 0;
        while (node->symlink_target[target_len] != '\0') {
            target_len++;
        }

        if (node->symlink_target[0] == '/') {
            // Absolute symlink target - replace entire path
            if (target_len >= bufsize) {
                return -ENAMETOOLONG;
            }
            std::memcpy(resolved_buf, node->symlink_target, target_len + 1);
            int const RR = reapply_root_prefix(resolved_buf, bufsize);
            if (RR < 0) {
                return RR;
            }
        } else {
            // Relative symlink target - replace last component
            size_t last_slash = 0;
            bool found_slash = false;
            for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                if (resolved_buf[i] == '/') {
                    last_slash = i;
                    found_slash = true;
                }
            }

            size_t const PREFIX_LEN = found_slash ? last_slash + 1 : 0;
            if (PREFIX_LEN + target_len >= bufsize) {
                return -ENAMETOOLONG;
            }
            std::memcpy(new_path.data(), resolved_buf, PREFIX_LEN);
            std::memcpy(new_path.data() + PREFIX_LEN, node->symlink_target, target_len);
            new_path[PREFIX_LEN + target_len] = '\0';
            std::memcpy(resolved_buf, new_path.data(), PREFIX_LEN + target_len + 1);
        }

        if (apply_task_policy) {
            int const NORMALIZE = normalize_task_path_inplace(resolved_buf, bufsize);
            if (NORMALIZE < 0) {
                return NORMALIZE;
            }
        }
    }

    return -ELOOP;
}

auto vfs_try_open_procfs_fd_link(const char* path, bool apply_task_policy) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    std::array<char, MAX_PATH_LEN> prefix_resolved{};
    int const RESOLVE_RET = resolve_symlinks(path, prefix_resolved.data(), prefix_resolved.size(), apply_task_policy, false);
    if (RESOLVE_RET != 0) {
        return nullptr;
    }

    auto mount_ref = find_mount_point(prefix_resolved.data());
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr || mount->fs_type != FSType::PROCFS) {
        return nullptr;
    }

    const char* fs_relative_path = strip_mount_prefix(mount, prefix_resolved.data());
    return ker::vfs::procfs::procfs_open_fd_link_path(fs_relative_path);
}
}  // namespace

auto vfs_open(std::string_view path, int flags, int mode) -> int {
    vfs_debug_log("vfs_open: opening file\n");
    bool const OPEN_LOCAL = (flags & ker::vfs::O_LOCAL) != 0;

    // Apply umask on creation
    if ((flags & ker::vfs::O_CREAT) != 0) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            mode = mode & ~static_cast<int>(task->umask);
        }
    }

    // Convert string_view to null-terminated string
    std::array<char, MAX_PATH_LEN> raw_path{};
    if (path.size() >= MAX_PATH_LEN) {
        return -ENAMETOOLONG;
    }
    std::memcpy(raw_path.data(), path.data(), path.size());
    raw_path[path.size()] = '\0';

    bool const PATH_REQUIRES_DIRECTORY = path_requires_directory(raw_path.data());
    bool const FLAGS_REQUIRE_DIRECTORY = (flags & ker::vfs::O_DIRECTORY) != 0;
    if (FLAGS_REQUIRE_DIRECTORY && (flags & ker::vfs::O_CREAT) != 0) {
        return -EINVAL;
    }
    int backend_flags = flags;
    if (PATH_REQUIRES_DIRECTORY) {
        backend_flags &= ~ker::vfs::O_CREAT;
    }

    std::array<char, MAX_PATH_LEN> path_buffer{};
    // NOLINTNEXTLINE(readability-suspicious-call-argument)
    if (resolve_task_path_raw_impl(raw_path.data(), path_buffer.data(), MAX_PATH_LEN, !OPEN_LOCAL) < 0) {
        log_loader_path_event("resolve-failed", raw_path.data(), nullptr, nullptr, -ENOENT);
        return -ENOENT;
    }

    ensure_wki_host_root_mount(path_buffer.data());
    log_loader_path_event("resolved", raw_path.data(), path_buffer.data(), nullptr, 0);

    // Remote mounts resolve symlinks on the server side during the actual open.
    // Avoid probing each path component with client-side READLINK RPCs here:
    // they are redundant and can fail independently of the real open.
    auto mount_ref = find_mount_point(path_buffer.data());
    MountPoint const* mount = mount_ref.get();
    bool const REMOTE_MOUNT = (mount != nullptr && mount->fs_type == FSType::REMOTE);

    bool const SKIP_FINAL_SYMLINK_PROBE = mount != nullptr && !REMOTE_MOUNT && !PATH_REQUIRES_DIRECTORY && !FLAGS_REQUIRE_DIRECTORY &&
                                          metadata_cache_proves_final_not_symlink(path_buffer.data(), mount->fs_type, mount->dev_id);

    auto* current = ker::mod::sched::get_current_task();
    if (current == nullptr) {
        vfs_debug_log("vfs_open: no current task\n");
        return -ESRCH;
    }

    if (!REMOTE_MOUNT) {
        auto* fd_link_file = vfs_try_open_procfs_fd_link(path_buffer.data(), !OPEN_LOCAL);
        if (fd_link_file != nullptr) {
            if (PATH_REQUIRES_DIRECTORY || FLAGS_REQUIRE_DIRECTORY) {
                vfs_put_file(fd_link_file);
                return -ENOTDIR;
            }
            int const FD = vfs_install_open_file(current, fd_link_file);
            if (FD < 0) {
                return FD;
            }
            if ((flags & ker::vfs::O_CLOEXEC) != 0) {
                current->set_fd_cloexec(static_cast<unsigned>(FD));
            }
            return FD;
        }
    }

    if (!REMOTE_MOUNT) {
        std::array<char, MAX_PATH_LEN> resolved{};
        int const RESOLVE_RET =
            resolve_symlinks(path_buffer.data(), resolved.data(), resolved.size(), !OPEN_LOCAL, !SKIP_FINAL_SYMLINK_PROBE);
        if (RESOLVE_RET == -ELOOP) {
            log::warn("vfs_open: too many symlink levels");
            return -ELOOP;
        }
        if (RESOLVE_RET == 0) {
            // Use the resolved path
            std::memcpy(path_buffer.data(), resolved.data(), resolved.size());
        }
        log_loader_path_event("symlink-resolved", raw_path.data(), path_buffer.data(), nullptr, RESOLVE_RET);
    } else {
        log_loader_path_event("symlink-deferred-remote", raw_path.data(), path_buffer.data(), mount, 0);
    }

    int const ACCMODE = flags & 3;

    // Find the mount point for this path
    mount_ref = find_mount_point(path_buffer.data());
    mount = mount_ref.get();
    if (mount == nullptr) {
        vfs_debug_log("vfs_open: no mount point found for path\n");
        log::warn("vfs_open: no mount point found for path: %s", path_buffer.data());
        log_loader_path_event("mount-miss", raw_path.data(), path_buffer.data(), nullptr, -ENOENT);
        return -ENOENT;
    }
    log_loader_path_event("mount-found", raw_path.data(), path_buffer.data(), mount, 0);

    const char* fs_relative_path = strip_mount_prefix(mount, path_buffer.data());
    if (mount->read_only && open_flags_require_fs_write(backend_flags)) {
        log_loader_path_event("open-readonly", raw_path.data(), path_buffer.data(), mount, -EROFS);
        return -EROFS;
    }

    ker::vfs::File* f = nullptr;

    // Route to the appropriate filesystem driver based on mount point
    switch (mount->fs_type) {
        case FSType::DEVFS:
            f = ker::vfs::devfs::devfs_open_path(fs_relative_path, backend_flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::devfs::get_devfs_fops();
                f->fs_type = FSType::DEVFS;
            }
            break;
        case FSType::FAT32:
            f = ker::vfs::fat32::fat32_open_path(fs_relative_path, backend_flags, mode,
                                                 static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::fat32::get_fat32_fops();
                f->fs_type = FSType::FAT32;
            } else {
                log::warn("vfs_open: fat32_open_path failed for '%s' (mount='%s', original path='%s')", fs_relative_path, mount->path,
                          path_buffer.data());
            }
            break;
        case FSType::TMPFS:
            f = ker::vfs::tmpfs::tmpfs_open_path(tmpfs_root_for_mount(mount), fs_relative_path, backend_flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
                f->fs_type = FSType::TMPFS;
            }
            break;
        case FSType::REMOTE:
            f = ker::net::wki::wki_remote_vfs_open_path(fs_relative_path, backend_flags, mode, mount->private_data);
            break;
        case FSType::PROCFS:
            f = ker::vfs::procfs::procfs_open_path(fs_relative_path, backend_flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::procfs::get_procfs_fops();
                f->fs_type = FSType::PROCFS;
            }
            break;
        case FSType::XFS:
            f = ker::vfs::xfs::xfs_open_path(fs_relative_path, backend_flags, mode,
                                             static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::xfs::get_xfs_fops();
                f->fs_type = FSType::XFS;
            }
            break;
        default:
            vfs_debug_log("vfs_open: unknown filesystem type\n");
            return -ENOSYS;
    }

    if (f == nullptr && ACCMODE == 0 && (backend_flags & ker::vfs::O_CREAT) == 0) {
        f = create_synthetic_mount_dir_file(path_buffer.data(), mount->fs_type);
    }

    if (f == nullptr) {
        vfs_debug_log("vfs_open: failed to open file\n");
        log_loader_path_event("open-failed", raw_path.data(), path_buffer.data(), mount, -ENOENT);
        return -ENOENT;
    }
    if ((PATH_REQUIRES_DIRECTORY || FLAGS_REQUIRE_DIRECTORY) && !f->is_directory) {
        vfs_destroy_file(f);
        return -ENOTDIR;
    }
    log_loader_path_event("open-ok", raw_path.data(), path_buffer.data(), mount, 0);

    // Store the absolute VFS path for mount-overlay directory listing.
    static_cast<void>(vfs_file_set_path(f, path_buffer.data()));
    f->dir_fs_count = static_cast<size_t>(-1);
    f->open_flags = flags;
    f->fd_flags = 0;  // fd_flags on File is legacy; CLOEXEC is per-fd in task bitmap

    // Permission check: verify R/W access based on open flags
    // Build required access bits from open flags
    int required_access = 0;
    if (ACCMODE == 0 || ACCMODE == 2) {
        required_access |= 4;  // R_OK
    }
    if (ACCMODE == 1 || ACCMODE == 2) {
        required_access |= 2;  // W_OK
    }

    // Get the file's mode/uid/gid for permission check
    if (required_access != 0 && f->fs_type == FSType::TMPFS) {
        auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
        if (node != nullptr) {
            int const PERM_RET = vfs_check_permission(node->mode, node->uid, node->gid, required_access);
            if (PERM_RET < 0) {
                vfs_destroy_file(f);
                return PERM_RET;
            }
        }
    }

    int const TRUNCATE_RET = apply_open_truncation(f, backend_flags);
    if (TRUNCATE_RET < 0) {
        vfs_destroy_file(f);
        return TRUNCATE_RET;
    }
    if (open_create_should_invalidate_metadata(f, backend_flags)) {
        vfs_cache_notify_path_changed(path_buffer.data(), nullptr);
        metadata_cache_mark_file_data_observed(f);
    }
    if ((flags & ker::vfs::O_NO_CACHE) != 0) {
        vfs_cache_notify_path_changed(f->vfs_path, nullptr);
    }
    vfs_cache_notify_register_open_file(f);
    file_stat_snapshot_refresh(f);

    int const FD = vfs_install_open_file(current, f);
    if (FD < 0) {
        return FD;
    }
    if ((flags & ker::vfs::O_CLOEXEC) != 0) {
        current->set_fd_cloexec(static_cast<unsigned>(FD));
    }
    return FD;
}

auto vfs_close_file(File* file) -> int { return vfs_destroy_file(file); }

auto vfs_close(int fd) -> int {
    // Release FD from current task
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    uint64_t const IRQF = t->fd_table_lock.lock_irqsave();
    ker::vfs::File* f = vfs_take_fd_locked(t, fd);
    size_t const FD_COUNT = t->fd_table.size();
    t->fd_table_lock.unlock_irqrestore(IRQF);
    if (f == nullptr) {
        return -EBADF;
    }

    advisory_release_process_locks_for_file(ker::mod::sched::task::process_pid(*t), f);

    ker::mod::perf::record_container_stat(0, t->pid, ker::mod::perf::PerfSubsystem::FD_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(FD_COUNT), 0, reinterpret_cast<uint64_t>(__builtin_return_address(0)));

    // Atomically decrement; only the CPU that drives refcount to 0 does teardown.
    if (f->refcount.fetch_sub(1, std::memory_order_acq_rel) > 1) {
        return 0;
    }

    return vfs_destroy_file(f);
}

namespace {
auto vfs_file_can_read(const File* file) -> bool {
    if (file == nullptr) {
        return false;
    }
    int const ACCMODE = file->open_flags & O_ACCMODE_MASK;
    return ACCMODE == O_RDONLY_MODE || ACCMODE == O_RDWR_MODE;
}

auto vfs_file_can_write(const File* file) -> bool {
    if (file == nullptr) {
        return false;
    }
    int const ACCMODE = file->open_flags & O_ACCMODE_MASK;
    return ACCMODE == O_WRONLY_MODE || ACCMODE == O_RDWR_MODE;
}
}  // namespace

auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if (!vfs_file_can_read(f)) {
        vfs_put_file(f);
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_read == nullptr)) {
        vfs_put_file(f);
        return -EINVAL;
    }

    bool const USE_STREAM_CACHE = stream_cache_read_eligible(f);
    ssize_t cached_result = 0;
    if (USE_STREAM_CACHE && vfs_stream_cache_try_read(f, buf, count, static_cast<uint64_t>(f->pos), actual_size, &cached_result)) {
        cached_result = clamp_io_count(cached_result, count);
        if (cached_result >= 0) {
            f->pos += cached_result;
            if (actual_size != nullptr) {
                *actual_size = static_cast<size_t>(cached_result);
            }
        }
        vfs_put_file(f);
        return cached_result;
    }
    if (USE_STREAM_CACHE) {
        g_vfs_stream_misses.fetch_add(1, std::memory_order_relaxed);
    }

    ssize_t const R = clamp_io_count(f->fops->vfs_read(f, buf, count, static_cast<size_t>(f->pos)), count);
    if (R >= 0) {
        f->pos += R;
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(R);
        }
        vfs_put_file(f);
        return R;
    }
    vfs_put_file(f);
    return R;
}

auto vfs_write_file(File* f, const void* buf, size_t count, size_t* actual_size) -> ssize_t {
    if (f == nullptr) {
        return -EBADF;
    }
    if (!vfs_file_can_write(f)) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_write == nullptr)) {
        return -EINVAL;
    }
    ssize_t result = 0;
    size_t append_offset = 0;
    bool const TMPFS_APPEND =
        ((f->open_flags & ker::vfs::O_APPEND) != 0) && f->fs_type == FSType::TMPFS && f->fops == ker::vfs::tmpfs::get_tmpfs_fops();
    bool const XFS_APPEND =
        ((f->open_flags & ker::vfs::O_APPEND) != 0) && f->fs_type == FSType::XFS && f->fops == ker::vfs::xfs::get_xfs_fops();
    if (TMPFS_APPEND) {
        result = ker::vfs::tmpfs::tmpfs_write_append(f, buf, count, &append_offset);
    } else if (XFS_APPEND) {
        result = ker::vfs::xfs::xfs_write_append(f, buf, count, &append_offset);
    } else {
        if (((f->open_flags & ker::vfs::O_APPEND) != 0) && (f->fops->vfs_lseek != nullptr)) {
            f->fops->vfs_lseek(f, 0, 2);  // SEEK_END
        }
        result = f->fops->vfs_write(f, buf, count, static_cast<size_t>(f->pos));
    }
    result = clamp_io_count(result, count);
    if (result >= 0) {
        if (result > 0) {
            cache_notify_file_data_changed_impl(f);
        }
        if (TMPFS_APPEND || XFS_APPEND) {
            f->pos = static_cast<off_t>(append_offset + static_cast<size_t>(result));
        } else {
            f->pos += result;
        }
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(result);
        }
        return result;
    }
    return result;
}

auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    ssize_t const RESULT = vfs_write_file(f, buf, count, actual_size);
    vfs_put_file(f);
    return RESULT;
}

auto vfs_lseek(int fd, off_t offset, int whence) -> off_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_lseek == nullptr)) {
        vfs_put_file(f);
        return -ESPIPE;
    }
    off_t const RC = f->fops->vfs_lseek(f, offset, whence);
    vfs_put_file(f);
    return RC;
}

auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int {
    if ((task == nullptr) || (file == nullptr)) {
        return -EINVAL;
    }
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    uint64_t const SLOT = vfs_find_free_fd_below_limit_locked(task, 0);
    bool const INSERTED = SLOT != UINT64_MAX && task->fd_table.insert(SLOT, file);
    if (INSERTED) {
        task->clear_fd_cloexec(static_cast<unsigned>(SLOT));
    }
    size_t const FD_COUNT = task->fd_table.size();
    task->fd_table_lock.unlock_irqrestore(IRQF);

    if (!INSERTED) {
        return -EMFILE;  // fd_table cannot currently distinguish OOM from exhaustion
    }
    file->fd = static_cast<int>(SLOT);
    ker::mod::perf::record_container_stat(0, task->pid, ker::mod::perf::PerfSubsystem::FD_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(FD_COUNT), 0, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    return static_cast<int>(SLOT);
}

auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File* {
    if (task == nullptr || fd < 0) {
        return nullptr;
    }
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    auto* file = reinterpret_cast<struct File*>(task->fd_table.lookup(static_cast<uint64_t>(fd)));
    task->fd_table_lock.unlock_irqrestore(IRQF);
    return file;
}

auto vfs_get_file_retain(ker::mod::sched::task::Task* task, int fd) -> File* {
    if (task == nullptr || fd < 0) {
        return nullptr;
    }
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    auto* file = reinterpret_cast<File*>(task->fd_table.lookup(static_cast<uint64_t>(fd)));
    if (file == nullptr) {
        task->fd_table_lock.unlock_irqrestore(IRQF);
        return nullptr;
    }
    file->refcount.fetch_add(1, std::memory_order_acq_rel);
    task->fd_table_lock.unlock_irqrestore(IRQF);
    return file;
}

void vfs_put_file(File* f) {
    if (f == nullptr) {
        return;
    }
    if (f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        vfs_destroy_file(f);
    }
}

void vfs_retain_file(File* f) {
    if (f == nullptr) {
        return;
    }
    f->refcount.fetch_add(1, std::memory_order_acq_rel);
}

auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int {
    if (task == nullptr || fd < 0) {
        return -EINVAL;
    }
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    task->fd_table.remove(static_cast<uint64_t>(fd));
    task->clear_fd_cloexec(static_cast<unsigned>(fd));
    size_t const FD_COUNT = task->fd_table.size();
    task->fd_table_lock.unlock_irqrestore(IRQF);
    ker::mod::perf::record_container_stat(0, task->pid, ker::mod::perf::PerfSubsystem::FD_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(FD_COUNT), 0, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    return 0;
}

auto vfs_resolve_dirfd(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, char* resolved, size_t resolved_size) -> int {
    if (task == nullptr || pathname == nullptr || resolved == nullptr || resolved_size == 0) {
        return -EINVAL;
    }

    // Absolute pathnames ignore dirfd entirely
    if (pathname[0] == '/') {
        size_t const LEN = std::strlen(pathname);
        if (LEN >= resolved_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(resolved, pathname, LEN + 1);
        return 0;
    }

    // Determine the base directory path
    const char* base = nullptr;
    if (dirfd == AT_FDCWD) {
        base = task->cwd.data();
    } else {
        auto* file = vfs_get_file_retain(task, dirfd);
        if (file == nullptr) {
            return -EBADF;
        }
        if (!file->is_directory) {
            vfs_put_file(file);
            return -ENOTDIR;
        }
        if (file->vfs_path == nullptr) {
            vfs_put_file(file);
            return -EBADF;
        }
        int const STRIP_RET = strip_task_root_prefix(task, file->vfs_path, resolved, resolved_size, nullptr);
        if (STRIP_RET < 0) {
            vfs_put_file(file);
            return STRIP_RET;
        }
        vfs_put_file(file);
        base = resolved;
    }

    // Concatenate base + "/" + pathname
    size_t base_len = std::strlen(base);
    size_t const PATH_LEN = std::strlen(pathname);

    // Strip trailing slash from base
    while (base_len > 1 && base[base_len - 1] == '/') {
        base_len--;
    }

    // Need: base + "/" + pathname + '\0'
    if (base_len + 1 + PATH_LEN + 1 > resolved_size) {
        return -ENAMETOOLONG;
    }

    for (size_t i = 0; i < base_len; ++i) {
        resolved[i] = base[i];
    }
    resolved[base_len] = '/';
    std::memcpy(resolved + base_len + 1, pathname, PATH_LEN + 1);
    return 0;
}

auto vfs_isatty(int fd) -> bool {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return false;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return false;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_isatty == nullptr)) {
        vfs_put_file(f);
        return false;
    }
    bool const RESULT = f->fops->vfs_isatty(f);
    vfs_put_file(f);
    return RESULT;
}

auto vfs_read_dir_entries(int fd, void* buffer, size_t max_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    // Check if this is a directory
    if (!f->is_directory) {
        vfs_put_file(f);
        return -ENOTDIR;
    }

    // Buffer must be large enough for at least one DirEntry
    if (buffer == nullptr || max_size < sizeof(DirEntry)) {
        vfs_put_file(f);
        return -EINVAL;
    }

    // We allow vfs_readdir to be null - the directory may contain only mount children
    bool const HAS_FS_READDIR = (f->fops != nullptr) && (f->fops->vfs_readdir != nullptr);

    auto* entries = static_cast<DirEntry*>(buffer);
    size_t const MAX_ENTRIES = max_size / sizeof(DirEntry);
    size_t entries_read = 0;

    // Read directory entries using the current position as an opaque-ish index.
    // Filesystems that need sparse/stable cookies can return d_off greater
    // than the requested index; older backends that leave d_off at the current
    // index still advance by one.
    auto next_index = static_cast<size_t>(f->pos);
    auto advance_after_entry = [&](size_t actual_index) {
        uint64_t const ENTRY_OFF = entries[entries_read].d_off;
        next_index = actual_index + 1;
        if (ENTRY_OFF > static_cast<uint64_t>(actual_index) && ENTRY_OFF <= static_cast<uint64_t>(~size_t{0})) {
            next_index = static_cast<size_t>(ENTRY_OFF);
        }
        entries_read++;
    };

    for (size_t i = 0; i < MAX_ENTRIES; ++i) {
        size_t const ACTUAL_INDEX = next_index;

        // Phase 1: try filesystem readdir
        if (HAS_FS_READDIR && (std::cmp_equal(f->dir_fs_count, -1) || ACTUAL_INDEX < f->dir_fs_count)) {
            int const RET = f->fops->vfs_readdir(f, &entries[entries_read], ACTUAL_INDEX);
            if (RET == 0) {
                advance_after_entry(ACTUAL_INDEX);
                continue;
            }
            // FS entries exhausted at this index
            f->dir_fs_count = ACTUAL_INDEX;
        }

        // Phase 2: inject synthetic task-aware aliases and mount-point children.
        // For each mount whose path starts with vfs_path, extract the first
        // path component after vfs_path as a child directory name.
        // Deduplicate against FS entries and against earlier mounts that
        // yield the same child component.
        bool found_mount_child = false;
        if (f->vfs_path != nullptr) {
            size_t const FS_COUNT = HAS_FS_READDIR ? f->dir_fs_count : 0;
            size_t synthetic_index = ACTUAL_INDEX - FS_COUNT;

            char visible_dir_path[MAX_PATH_LEN] = {};
            if (strip_current_task_root_prefix(f->vfs_path, visible_dir_path, sizeof(visible_dir_path)) < 0) {
                break;
            }

            const char* local_hostname = ker::net::wki::g_wki.local_hostname.data();
            bool const INJECT_WKI_ROOT = std::strcmp(visible_dir_path, "/") == 0 && ker::net::wki::g_wki.initialized &&
                                         !logical_wki_root_has_mount_child() && !dir_contains_name(f, HAS_FS_READDIR, FS_COUNT, "wki");
            bool const INJECT_HOST_ALIAS =
                std::strcmp(visible_dir_path, "/wki") == 0 && !dir_contains_name(f, HAS_FS_READDIR, FS_COUNT, "host");
            bool const INJECT_LOCAL_ALIAS = std::strcmp(visible_dir_path, "/wki") == 0 && local_hostname[0] != '\0' &&
                                            !dir_contains_name(f, HAS_FS_READDIR, FS_COUNT, local_hostname);

            if (INJECT_WKI_ROOT) {
                if (synthetic_index == 0) {
                    entries[entries_read].d_ino = 0x574b49524f4f54ULL;
                    entries[entries_read].d_off = ACTUAL_INDEX + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);
                    entries[entries_read].d_type = DT_DIR | DT_WOSLINK;
                    std::memcpy(entries[entries_read].d_name.data(), "wki", 4);
                    entries[entries_read].d_name[3] = '\0';
                    advance_after_entry(ACTUAL_INDEX);
                    continue;
                }
                synthetic_index--;
            }

            if (INJECT_HOST_ALIAS) {
                if (synthetic_index == 0) {
                    entries[entries_read].d_ino = 0x574b49486f7374ULL;
                    entries[entries_read].d_off = ACTUAL_INDEX + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);
                    entries[entries_read].d_type = DT_DIR | DT_WOSLINK;
                    std::memcpy(entries[entries_read].d_name.data(), "host", 5);
                    advance_after_entry(ACTUAL_INDEX);
                    continue;
                }
                synthetic_index--;
            }

            if (INJECT_LOCAL_ALIAS) {
                if (synthetic_index == 0) {
                    entries[entries_read].d_ino = 0x574b494c6f6361ULL;
                    entries[entries_read].d_off = ACTUAL_INDEX + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);
                    entries[entries_read].d_type = DT_DIR | DT_WOSLINK;
                    size_t copy_len = std::strlen(local_hostname);
                    if (copy_len >= DIRENT_NAME_MAX) {
                        copy_len = DIRENT_NAME_MAX - 1;
                    }
                    std::memcpy(entries[entries_read].d_name.data(), local_hostname, copy_len);
                    entries[entries_read].d_name[copy_len] = '\0';
                    advance_after_entry(ACTUAL_INDEX);
                    continue;
                }
                synthetic_index--;
            }

            size_t const MOUNT_IDX = synthetic_index;

            size_t const DIR_LEN = std::strlen(visible_dir_path);
            size_t child_count = 0;

            for (size_t mi = 0; mi < get_mount_count(); ++mi) {
                MountSnapshot mount_snapshot = {};
                if (!get_mount_snapshot_at(mi, &mount_snapshot)) {
                    continue;
                }

                char visible_mount_path[MAX_PATH_LEN] = {};
                if (strip_current_task_root_prefix(mount_snapshot.path, visible_mount_path, sizeof(visible_mount_path)) < 0) {
                    continue;
                }

                size_t const MP_LEN = std::strlen(visible_mount_path);
                const char* child_start = nullptr;
                size_t child_len = 0;

                if (DIR_LEN == 1 && visible_dir_path[0] == '/') {
                    // Root directory: child is first component of "/xxx[/...]"
                    if (MP_LEN > 1 && visible_mount_path[0] == '/') {
                        child_start = visible_mount_path + 1;
                    }
                } else {
                    // Non-root: mount must start with dir_path + "/"
                    if (MP_LEN > DIR_LEN && std::memcmp(visible_mount_path, visible_dir_path, DIR_LEN) == 0 &&
                        visible_mount_path[DIR_LEN] == '/') {
                        child_start = visible_mount_path + DIR_LEN + 1;
                    }
                }

                if (child_start == nullptr || *child_start == '\0') {
                    continue;
                }

                // Extract only the first path component
                const char* p = child_start;
                while (*p != '\0' && *p != '/') {
                    p++;
                }
                child_len = static_cast<size_t>(p - child_start);
                if (child_len == 0) {
                    continue;
                }

                // Dedup against earlier mounts that yield the same child name
                bool dup_mount = false;
                for (size_t mj = 0; mj < mi; ++mj) {
                    MountSnapshot mount_snapshot2 = {};
                    if (!get_mount_snapshot_at(mj, &mount_snapshot2)) {
                        continue;
                    }

                    char visible_mount_path2[MAX_PATH_LEN] = {};
                    if (strip_current_task_root_prefix(mount_snapshot2.path, visible_mount_path2, sizeof(visible_mount_path2)) < 0) {
                        continue;
                    }

                    size_t const MP2_LEN = std::strlen(visible_mount_path2);
                    const char* c2 = nullptr;

                    if (DIR_LEN == 1 && visible_dir_path[0] == '/') {
                        if (MP2_LEN > 1 && visible_mount_path2[0] == '/') {
                            c2 = visible_mount_path2 + 1;
                        }
                    } else {
                        if (MP2_LEN > DIR_LEN && std::memcmp(visible_mount_path2, visible_dir_path, DIR_LEN) == 0 &&
                            visible_mount_path2[DIR_LEN] == '/') {
                            c2 = visible_mount_path2 + DIR_LEN + 1;
                        }
                    }
                    if (c2 == nullptr || *c2 == '\0') {
                        continue;
                    }

                    const char* p2 = c2;
                    while (*p2 != '\0' && *p2 != '/') {
                        p2++;
                    }
                    auto const C2_LEN = static_cast<size_t>(p2 - c2);

                    if (C2_LEN == child_len && std::memcmp(child_start, c2, child_len) == 0) {
                        dup_mount = true;
                        break;
                    }
                }
                if (dup_mount) {
                    continue;
                }

                // Dedup against FS readdir entries
                if (HAS_FS_READDIR && FS_COUNT > 0) {
                    bool already_in_fs = false;
                    DirEntry probe = {};
                    for (size_t pi = 0; pi < FS_COUNT;) {
                        int const PRET = f->fops->vfs_readdir(f, &probe, pi);
                        if (PRET != 0) {
                            break;
                        }
                        size_t const DN_LEN = std::strlen(probe.d_name.data());
                        if (DN_LEN == child_len && std::memcmp(probe.d_name.data(), child_start, child_len) == 0) {
                            already_in_fs = true;
                            break;
                        }
                        size_t next_pi = pi + 1;
                        if (probe.d_off > static_cast<uint64_t>(pi) && probe.d_off <= static_cast<uint64_t>(~size_t{0})) {
                            next_pi = static_cast<size_t>(probe.d_off);
                        }
                        pi = next_pi;
                    }
                    if (already_in_fs) {
                        continue;
                    }
                }

                if (child_count == MOUNT_IDX) {
                    // Fill the synthetic DirEntry
                    entries[entries_read].d_ino = (static_cast<uint64_t>(mount_snapshot.dev_id) << 32) | 0x4d4e5455ULL;
                    entries[entries_read].d_off = ACTUAL_INDEX + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);

                    // Mark WKI entries with WOSLINK flag for recursion prevention:
                    // - listing /wki: all mount children (wos-0, wos-1, ...) are WOSLINK
                    // - listing /: the "wki" mount child is WOSLINK
                    if (std::strcmp(visible_dir_path, "/wki") == 0 ||
                        (DIR_LEN == 1 && visible_dir_path[0] == '/' && child_len == 3 && std::memcmp(child_start, "wki", 3) == 0)) {
                        entries[entries_read].d_type = DT_DIR | DT_WOSLINK;
                    } else {
                        entries[entries_read].d_type = DT_DIR;
                    }

                    size_t const COPY_LEN = child_len < DIRENT_NAME_MAX - 1 ? child_len : DIRENT_NAME_MAX - 1;
                    std::memcpy(entries[entries_read].d_name.data(), child_start, COPY_LEN);
                    entries[entries_read].d_name[COPY_LEN] = '\0';

                    advance_after_entry(ACTUAL_INDEX);
                    found_mount_child = true;
                    break;
                }
                child_count++;
            }
        }

        if (found_mount_child) {
            continue;
        }

        // No more entries from either FS or mount children
        break;
    }

    // Update file position
    f->pos = static_cast<off_t>(next_index);

    auto result = static_cast<ssize_t>(entries_read * sizeof(DirEntry));
    vfs_put_file(f);
    return result;
}

// --- Symlink / mkdir / mount operations ---

auto vfs_symlink(const char* target, const char* linkpath) -> int {
    if (target == nullptr || linkpath == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> abs_linkpath{};
    if (resolve_task_path_raw(linkpath, abs_linkpath.data(), abs_linkpath.size()) < 0) {
        return -ENOENT;
    }

    // Find mount point for the linkpath
    auto mount_ref = find_mount_point(abs_linkpath.data());
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, abs_linkpath.data());
        int const RET = ker::net::wki::wki_remote_vfs_symlink(mount->private_data, target, fs_path);
        if (RET == 0) {
            vfs_cache_notify_path_changed(abs_linkpath.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, abs_linkpath.data());
        int const RET = ker::vfs::xfs::xfs_symlink_path(target, fs_path, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
        if (RET == 0) {
            vfs_cache_notify_path_changed(abs_linkpath.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, abs_linkpath.data());

    // Split into parent path and link name
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* link_name = nullptr;

    if (last_slash == nullptr) {
        parent = tmpfs_root_for_mount(mount);
        link_name = fs_path;
    } else {
        std::array<char, MAX_PATH_LEN> parent_path{};
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        if (parent_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(parent_path.data(), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), parent_path.data(), true);
        link_name = last_slash + 1;
    }

    if (parent == nullptr || link_name == nullptr || *link_name == '\0') {
        return -ENOENT;
    }

    auto* node = ker::vfs::tmpfs::tmpfs_create_symlink(parent, link_name, target);
    if (node == nullptr) {
        return -1;
    }
    vfs_cache_notify_path_changed(abs_linkpath.data(), nullptr);
    return 0;
}

// Internal readlink operating on an already-resolved absolute path (no root
// prefix applied).  Used by resolve_prefix_symlink_once which works on paths
// that already include the task root.
namespace {

auto readlink_resolved(const char* abs_path, char* buf, size_t bufsize) -> ssize_t {
    if (abs_path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    auto mount_ref = find_mount_point(abs_path);
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }
    uint64_t const DEV_ID = mount->dev_id;
    ssize_t cached_result = 0;
    if (symlink_cache_lookup(abs_path, mount->fs_type, DEV_ID, buf, bufsize, &cached_result)) {
        return cached_result;
    }

    if (mount->fs_type == FSType::PROCFS) {
        const char* fsp = strip_mount_prefix(mount, abs_path);

        auto* f = ker::vfs::procfs::procfs_open_path(fsp, 0, 0);
        if (f == nullptr) {
            return -ENOENT;
        }
        ssize_t const RET = ker::vfs::procfs::get_procfs_fops()->vfs_readlink(f, buf, bufsize);
        ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
        delete f;
        return RET;
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, abs_path);

        // The root of a mounted remote export is the mount point itself, not
        // a symlink target inside that export. Avoid a pointless remote
        // READLINK round-trip for exact mount-root probes.
        if (fs_path[0] == '\0') {
            return -EINVAL;
        }

        return ker::net::wki::wki_remote_vfs_readlink_path(mount->private_data, fs_path, buf, bufsize);
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, abs_path);
        auto* xctx = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
        auto* f = ker::vfs::xfs::xfs_open_path(fs_path, 0, 0, xctx);
        if (f == nullptr) {
            symlink_cache_store(abs_path, mount->fs_type, DEV_ID, -ENOENT, nullptr);
            return -ENOENT;
        }
        ssize_t const RET = ker::vfs::xfs::get_xfs_fops()->vfs_readlink(f, buf, bufsize);
        ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
        delete f;
        if (RET <= 0 || std::cmp_less(RET, bufsize)) {
            symlink_cache_store(abs_path, mount->fs_type, DEV_ID, RET, RET > 0 ? buf : nullptr);
        }
        return RET;
    }

    if (mount->fs_type == FSType::DEVFS) {
        const char* fs_path = strip_mount_prefix(mount, abs_path);

        auto* f = ker::vfs::devfs::devfs_open_path(fs_path, 0, 0);
        if (f == nullptr) {
            return -ENOENT;
        }

        if (f->fops == nullptr || f->fops->vfs_readlink == nullptr) {
            if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                f->fops->vfs_close(f);
            }
            delete f;
            return -ENOSYS;
        }

        ssize_t const RET = f->fops->vfs_readlink(f, buf, bufsize);
        if (f->fops->vfs_close != nullptr) {
            f->fops->vfs_close(f);
        }
        delete f;
        return RET;
    }

    if (mount->fs_type != FSType::TMPFS) {
        symlink_cache_store(abs_path, mount->fs_type, DEV_ID, -ENOSYS, nullptr);
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, abs_path);

    auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
    if (node == nullptr) {
        symlink_cache_store(abs_path, mount->fs_type, DEV_ID, -ENOENT, nullptr);
        return -ENOENT;
    }
    node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
    if (node == nullptr) {
        symlink_cache_store(abs_path, mount->fs_type, DEV_ID, -ENOENT, nullptr);
        return -ENOENT;
    }

    if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK || node->symlink_target == nullptr) {
        symlink_cache_store(abs_path, mount->fs_type, DEV_ID, -EINVAL, nullptr);
        return -EINVAL;
    }

    size_t len = 0;
    while (node->symlink_target[len] != '\0') {
        len++;
    }
    size_t const TO_COPY = (len < bufsize) ? len : bufsize;
    std::memcpy(buf, node->symlink_target, TO_COPY);
    if (TO_COPY == len) {
        symlink_cache_store(abs_path, mount->fs_type, DEV_ID, static_cast<ssize_t>(TO_COPY), buf);
    }
    return static_cast<ssize_t>(TO_COPY);
}

auto realpath_reset_to_task_root(char* resolved, size_t resolved_size, size_t* min_len) -> int {
    if (resolved == nullptr || min_len == nullptr || resolved_size == 0) {
        return -EINVAL;
    }

    const char* root = "/";
    if (ker::mod::sched::can_query_current_task()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr && task->root[0] != '\0') {
            root = task->root.data();
        }
    }

    int const COPY_RESULT = copy_path_string(root, resolved, resolved_size);
    if (COPY_RESULT < 0) {
        return COPY_RESULT;
    }

    *min_len = std::strlen(resolved);
    if (*min_len == 0) {
        return copy_path_string("/", resolved, resolved_size);
    }
    if (*min_len == 1) {
        *min_len = 1;
    }
    return 0;
}

auto realpath_append_component(char* resolved, size_t resolved_size, const char* component, size_t component_len) -> int {
    if (resolved == nullptr || component == nullptr) {
        return -EINVAL;
    }
    if (component_len == 0) {
        return 0;
    }

    size_t const BASE_LEN = std::strlen(resolved);
    bool const NEED_SLASH = BASE_LEN != 1 || resolved[0] != '/';
    size_t const TOTAL = BASE_LEN + (NEED_SLASH ? 1 : 0) + component_len + 1;
    if (TOTAL > resolved_size) {
        return -ENAMETOOLONG;
    }

    size_t pos = BASE_LEN;
    if (NEED_SLASH) {
        resolved[pos++] = '/';
    }
    std::memcpy(resolved + pos, component, component_len);
    resolved[pos + component_len] = '\0';
    return 0;
}

void realpath_pop_component(char* resolved, size_t min_len) {
    if (resolved == nullptr) {
        return;
    }

    size_t len = std::strlen(resolved);
    if (len <= min_len) {
        return;
    }

    char* slash = resolved + len;
    while (slash > resolved && *slash != '/') {
        --slash;
    }

    if (slash <= resolved + min_len) {
        resolved[min_len] = '\0';
        return;
    }
    if (slash == resolved) {
        resolved[1] = '\0';
        return;
    }
    *slash = '\0';
}

auto realpath_set_pending(const char* first, size_t first_len, const char* rest, char* pending, size_t pending_size) -> int {
    if (pending == nullptr) {
        return -EINVAL;
    }

    while (first != nullptr && first_len > 0 && *first == '/') {
        ++first;
        --first_len;
    }
    while (rest != nullptr && *rest == '/') {
        ++rest;
    }

    size_t const REST_LEN = rest != nullptr ? std::strlen(rest) : 0;
    size_t const SEP_LEN = (first_len > 0 && REST_LEN > 0) ? 1 : 0;
    if (first_len + SEP_LEN + REST_LEN + 1 > pending_size) {
        return -ENAMETOOLONG;
    }

    std::array<char, MAX_PATH_LEN> rest_copy{};
    if (REST_LEN > 0) {
        if (REST_LEN >= rest_copy.size()) {
            return -ENAMETOOLONG;
        }
        std::memcpy(rest_copy.data(), rest, REST_LEN);
        rest_copy[REST_LEN] = '\0';
    }

    size_t pos = 0;
    if (first_len > 0) {
        std::memcpy(pending + pos, first, first_len);
        pos += first_len;
    }
    if (SEP_LEN != 0) {
        pending[pos++] = '/';
    }
    if (REST_LEN > 0) {
        std::memcpy(pending + pos, rest_copy.data(), REST_LEN);
        pos += REST_LEN;
    }
    pending[pos] = '\0';
    return 0;
}

auto realpath_resolve_visible_path(const char* path, char* resolved, size_t resolved_size) -> int {
    if (path == nullptr || resolved == nullptr || resolved_size == 0) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> visible_abs{};
    int ret = make_absolute(path, visible_abs.data(), visible_abs.size());
    if (ret < 0) {
        return ret;
    }

    std::array<char, MAX_PATH_LEN> logical_abs{};
    ret = strip_current_task_root_prefix(visible_abs.data(), logical_abs.data(), logical_abs.size());
    if (ret < 0) {
        return ret;
    }

    size_t min_len = 1;
    ret = realpath_reset_to_task_root(resolved, resolved_size, &min_len);
    if (ret < 0) {
        return ret;
    }

    std::array<char, MAX_PATH_LEN> pending{};
    const char* initial = logical_abs.data();
    while (*initial == '/') {
        ++initial;
    }
    ret = copy_path_string(initial, pending.data(), pending.size());
    if (ret < 0) {
        return ret;
    }

    size_t pos = 0;
    int symlink_depth = 0;
    while (true) {
        while (pending[pos] == '/') {
            ++pos;
        }
        if (pending[pos] == '\0') {
            break;
        }

        const char* component = pending.data() + pos;
        size_t component_len = 0;
        while (component[component_len] != '\0' && component[component_len] != '/') {
            ++component_len;
        }
        pos += component_len;
        const char* rest = pending.data() + pos;
        while (*rest == '/') {
            ++rest;
        }

        if (component_len == 1 && component[0] == '.') {
            continue;
        }
        if (component_len == 2 && component[0] == '.' && component[1] == '.') {
            realpath_pop_component(resolved, min_len);
            continue;
        }

        ret = realpath_append_component(resolved, resolved_size, component, component_len);
        if (ret < 0) {
            return ret;
        }

        std::array<char, MAX_PATH_LEN> linkbuf{};
        ssize_t const LINK_LEN = readlink_resolved(resolved, linkbuf.data(), linkbuf.size() - 1);
        if (LINK_LEN == -EINVAL || LINK_LEN == -ENOSYS) {
            if (*rest != '\0') {
                Stat st{};
                ret = vfs_stat_resolved(resolved, &st);
                if (ret < 0) {
                    return ret;
                }
                if ((st.st_mode & S_IFMT) != S_IFDIR) {
                    return -ENOTDIR;
                }
            }
            continue;
        }
        if (LINK_LEN < 0) {
            return static_cast<int>(LINK_LEN);
        }
        if (std::cmp_greater_equal(LINK_LEN, linkbuf.size())) {
            return -ENAMETOOLONG;
        }
        linkbuf[static_cast<size_t>(LINK_LEN)] = '\0';
        if (++symlink_depth > MAX_SYMLINK_DEPTH) {
            return -ELOOP;
        }

        if (linkbuf[0] == '/') {
            ret = realpath_reset_to_task_root(resolved, resolved_size, &min_len);
            if (ret < 0) {
                return ret;
            }
        } else {
            realpath_pop_component(resolved, min_len);
        }

        ret = realpath_set_pending(linkbuf.data(), static_cast<size_t>(LINK_LEN), rest, pending.data(), pending.size());
        if (ret < 0) {
            return ret;
        }
        pos = 0;
    }

    return 0;
}

}  // namespace

void vfs_cache_notify_register_open_file(File* file) { cache_notify_register_open_file_impl(file); }

void vfs_cache_notify_invalidate_path(const char* vfs_path) { cache_notify_invalidate_path_impl(vfs_path); }

void vfs_cache_notify_path_changed(const char* old_vfs_path, const char* new_vfs_path) {
    cache_notify_path_changed_impl(old_vfs_path, new_vfs_path);
}

void vfs_cache_notify_file_changed(File* file) { cache_notify_file_changed_impl(file); }

void vfs_cache_notify_file_data_changed(File* file) { cache_notify_file_data_changed_impl(file); }

auto vfs_cache_notify_file_dirty(File* file) -> bool { return cache_notify_file_dirty_impl(file); }

void vfs_cache_notify_acknowledge_file(File* file) { cache_notify_acknowledge_file_impl(file); }

auto vfs_cache_epoch_snapshot() -> uint64_t { return g_metadata_cache_epoch.load(std::memory_order_acquire); }

void vfs_get_cache_perf_snapshot(VfsCachePerfSnapshot& out) {
    out.metadata_hits = g_vfs_metadata_hits.load(std::memory_order_relaxed);
    out.metadata_misses = g_vfs_metadata_misses.load(std::memory_order_relaxed);
    out.metadata_stores = g_vfs_metadata_stores.load(std::memory_order_relaxed);
    out.metadata_miss_empty = g_vfs_metadata_miss_empty.load(std::memory_order_relaxed);
    out.metadata_miss_invalidated = g_vfs_metadata_miss_invalidated.load(std::memory_order_relaxed);
    out.metadata_miss_stale_generation = g_vfs_metadata_miss_stale_generation.load(std::memory_order_relaxed);
    out.metadata_miss_conflict = g_vfs_metadata_miss_conflict.load(std::memory_order_relaxed);
    out.metadata_path_invalidations = g_vfs_metadata_path_invalidations.load(std::memory_order_relaxed);
    out.metadata_generation_resets = g_vfs_metadata_generation_resets.load(std::memory_order_relaxed);
    out.symlink_hits = g_vfs_symlink_hits.load(std::memory_order_relaxed);
    out.symlink_misses = g_vfs_symlink_misses.load(std::memory_order_relaxed);
    out.symlink_stores = g_vfs_symlink_stores.load(std::memory_order_relaxed);
    out.stream_hits = g_vfs_stream_hits.load(std::memory_order_relaxed);
    out.stream_misses = g_vfs_stream_misses.load(std::memory_order_relaxed);
    out.stream_backend_reads = g_vfs_stream_backend_reads.load(std::memory_order_relaxed);
    out.stream_backend_bytes = g_vfs_stream_backend_bytes.load(std::memory_order_relaxed);
    out.stream_copied_bytes = g_vfs_stream_copied_bytes.load(std::memory_order_relaxed);
    out.stream_invalidate_empty_skips = g_vfs_stream_invalidate_empty_skips.load(std::memory_order_relaxed);
    out.fstat_snapshot_hits = g_vfs_fstat_snapshot_hits.load(std::memory_order_relaxed);
    out.fstat_snapshot_misses = g_vfs_fstat_snapshot_misses.load(std::memory_order_relaxed);
    out.fstat_snapshot_stores = g_vfs_fstat_snapshot_stores.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_uncacheable = g_vfs_fstat_snapshot_miss_uncacheable.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_bad_args = g_vfs_fstat_snapshot_miss_bad_args.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_no_cache = g_vfs_fstat_snapshot_miss_no_cache.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_pathless = g_vfs_fstat_snapshot_miss_pathless.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_fs = g_vfs_fstat_snapshot_miss_fs.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_empty = g_vfs_fstat_snapshot_miss_empty.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_generation = g_vfs_fstat_snapshot_miss_generation.load(std::memory_order_relaxed);
    out.fstat_snapshot_miss_invalidated = g_vfs_fstat_snapshot_miss_invalidated.load(std::memory_order_relaxed);
}

void vfs_prefill_file_stat_snapshot(File* file, const Stat& statbuf) {
    if (file == nullptr || !file_stat_snapshot_path_cacheable_fs(file->fs_type) ||
        !file_stat_snapshot_result_cacheable(file, statbuf.st_mode) ||
        (file->open_flags & (ker::vfs::O_NO_CACHE | ker::vfs::O_TRUNC | ker::vfs::O_CREAT)) != 0 || (file->open_flags & 3) != 0) {
        file_stat_snapshot_invalidate(file);
        return;
    }

    MetadataSnapshotStamp const STAMP = metadata_snapshot_stamp();
    file->stat_cache = statbuf;
    file->stat_cache_generation = STAMP.cache_generation;
    file->stat_cache_invalidation_generation = STAMP.invalidation_generation;
    file->stat_cache_path_len = 0;
    file->stat_cache_valid = true;
}

auto vfs_readlink_resolved(const char* path, char* buf, size_t bufsize) -> ssize_t {
    if (path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    return readlink_resolved(path, buf, bufsize);
}

auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t {
    if (path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> abs_path{};
    if (resolve_task_path_raw(path, abs_path.data(), abs_path.size()) < 0) {
        return -ENOENT;
    }

    if (path_requires_directory(path)) {
        std::array<char, MAX_PATH_LEN> resolved{};
        int const RESOLVE_RET = resolve_symlinks(abs_path.data(), resolved.data(), resolved.size(), true, true);
        if (RESOLVE_RET < 0) {
            return RESOLVE_RET;
        }

        Stat st{};
        int const STAT_RET = vfs_stat_resolved(resolved.data(), &st);
        if (STAT_RET < 0) {
            return STAT_RET;
        }
        if ((st.st_mode & S_IFMT) != S_IFDIR) {
            return -ENOTDIR;
        }

        return readlink_resolved(resolved.data(), buf, bufsize);
    }

    return readlink_resolved(abs_path.data(), buf, bufsize);
}

auto vfs_realpath(const char* path, char* buf, size_t bufsize) -> int {
    if (path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> local_backing_path{};
    int ret = resolve_task_path_raw_impl(path, local_backing_path.data(), local_backing_path.size(), false);
    if (ret < 0) {
        return ret;
    }

    std::array<char, MAX_PATH_LEN> backing_path{};
    ret = resolve_task_path_raw(path, backing_path.data(), backing_path.size());
    if (ret < 0) {
        return ret;
    }

    // If WKI routing rewrites the path to a backing namespace, keep userspace on
    // the generic resolver so realpath() returns process-visible paths.
    if (std::strcmp(local_backing_path.data(), backing_path.data()) != 0) {
        return -ENOSYS;
    }

    ensure_wki_host_root_mount(backing_path.data());

    std::array<char, MAX_PATH_LEN> resolved{};
    ret = realpath_resolve_visible_path(path, resolved.data(), resolved.size());
    if (ret < 0) {
        return ret;
    }

    Stat st{};
    ret = vfs_stat_resolved(resolved.data(), &st);
    if (ret < 0) {
        return ret;
    }

    std::array<char, MAX_PATH_LEN> visible_path{};
    ret = strip_current_task_root_prefix(resolved.data(), visible_path.data(), visible_path.size());
    if (ret < 0) {
        return ret;
    }

    ret = canonicalize_path(visible_path.data(), visible_path.size());
    if (ret < 0) {
        return ret;
    }

    size_t const LEN = std::strlen(visible_path.data());
    if (LEN + 1 > bufsize) {
        return -ERANGE;
    }
    std::memcpy(buf, visible_path.data(), LEN + 1);
    return 0;
}

auto vfs_mkdir(const char* path, int mode) -> int {
    (void)mode;
    if (path == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> abs_path{};
    if (resolve_task_path_raw(path, abs_path.data(), abs_path.size()) < 0) {
        return -ENOENT;
    }

    auto mount_ref = find_mount_point(abs_path.data());
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    const char* fs_path = strip_mount_prefix(mount, abs_path.data());

    if (fs_path[0] == '\0') {
        return 0;
    }

    if (mount->fs_type == FSType::TMPFS) {
        auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, true);
        if (node == nullptr) {
            return -1;
        }
        vfs_cache_notify_path_changed(abs_path.data(), nullptr);
        return 0;
    }

    if (mount->fs_type == FSType::XFS) {
        auto* xctx = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
        int const R = ker::vfs::xfs::xfs_mkdir_path(fs_path, mode, xctx);
        // mkdir -p calls mkdir on existing dirs; treat EEXIST as success
        int const RESULT = (R == -EEXIST) ? 0 : R;
        if (R == 0) {
            vfs_cache_notify_path_changed(abs_path.data(), nullptr);
        }
        return RESULT;
    }

    if (mount->fs_type == FSType::REMOTE) {
        int const R = ker::net::wki::wki_remote_vfs_mkdir(mount->private_data, fs_path, mode);
        int const RESULT = (R == -EEXIST) ? 0 : R;
        if (R == 0) {
            vfs_cache_notify_path_changed(abs_path.data(), nullptr);
        }
        return RESULT;
    }

    // For other mounts (devfs, procfs, etc.) return 0 if the directory exists
    ker::vfs::Stat st{};
    if (vfs_stat(abs_path.data(), &st) == 0) {
        return 0;
    }
    return -ENOSYS;
}

static auto vfs_stat_impl(const char* path, ker::vfs::Stat* statbuf, bool resolve_task_path, bool apply_task_policy,
                          bool follow_final_symlink) -> int {
    if (path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    bool const REQUIRE_DIRECTORY = resolve_task_path && path_requires_directory(path);
    bool const EFFECTIVE_FOLLOW_FINAL_SYMLINK = follow_final_symlink || REQUIRE_DIRECTORY;
    bool is_wki_entry = false;
    if (resolve_task_path) {
        // WOSLINK detection: compute canonical pre-rewrite path to detect /wki
        // entries before resolve_task_path_raw rewrites them (e.g., /wki/host -> /).
        std::array<char, MAX_PATH_LEN> pre_rewrite{};
        if (make_absolute(path, pre_rewrite.data(), pre_rewrite.size()) == 0 &&
            canonicalize_path(pre_rewrite.data(), pre_rewrite.size()) == 0) {
            if (std::strcmp(pre_rewrite.data(), "/wki") == 0) {
                is_wki_entry = true;
            } else if (std::strncmp(pre_rewrite.data(), "/wki/", 5) == 0) {
                // Direct child of /wki (one component, no further slashes)
                const char* child = pre_rewrite.data() + 5;
                bool has_slash = false;
                for (const char* p = child; *p != '\0'; ++p) {
                    if (*p == '/') {
                        has_slash = true;
                        break;
                    }
                }
                if (*child != '\0' && !has_slash) {
                    is_wki_entry = true;
                }
            }
        }
    }

    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    if (resolve_task_path) {
        if (resolve_task_path_raw(path, pathBuffer, MAX_PATH_LEN) < 0) {
            log_loader_path_event("stat-resolve-failed", path, nullptr, nullptr, -ENOENT);
            return -ENOENT;
        }
        // Stat probes for exact /wki/<host> entries are common during shell
        // completion. Do not turn those metadata checks into synchronous remote
        // attaches; deeper paths still lazy-mount below when they are actually
        // accessed.
        if (!is_wki_entry) {
            ensure_wki_host_root_mount(pathBuffer);
        }
    } else if (copy_path_string(path, pathBuffer, sizeof(pathBuffer)) < 0) {
        return -ENOENT;
    }
    log_loader_path_event("stat-resolved", path, pathBuffer, nullptr, 0);

    // Remote mounts resolve symlinks on the server side during the actual stat.
    // Avoid a redundant client-side READLINK walk here; it pessimizes metadata
    // traffic and can fail independently of the real remote stat operation.
    auto mount_ref = find_mount_point(pathBuffer);
    MountPoint const* mount = mount_ref.get();
    bool const REMOTE_MOUNT = (mount != nullptr && mount->fs_type == FSType::REMOTE);

    if (mount != nullptr && !REMOTE_MOUNT && !is_wki_entry) {
        int const CACHED_RESULT = metadata_cache_lookup(pathBuffer, mount->fs_type, mount->dev_id, EFFECTIVE_FOLLOW_FINAL_SYMLINK,
                                                        REQUIRE_DIRECTORY, statbuf, false);
        if (CACHED_RESULT != -EAGAIN) {
            log_loader_path_event(CACHED_RESULT == 0 ? "stat-cache-hit-pre-symlink" : "stat-cache-negative-hit-pre-symlink", path,
                                  pathBuffer, mount, CACHED_RESULT);
            return CACHED_RESULT;
        }
    }

    if (!REMOTE_MOUNT) {
        char resolved[MAX_PATH_LEN];  // NOLINT
        int const RESOLVE_RET = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN, apply_task_policy, EFFECTIVE_FOLLOW_FINAL_SYMLINK);
        if (RESOLVE_RET == -ELOOP) {
            return -ELOOP;
        }
        if (RESOLVE_RET < 0) {
            log_loader_path_event("stat-symlink-failed", path, pathBuffer, nullptr, RESOLVE_RET);
            return RESOLVE_RET;
        }
        std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
        log_loader_path_event("stat-symlink-resolved", path, pathBuffer, nullptr, RESOLVE_RET);
    } else {
        log_loader_path_event("stat-symlink-deferred-remote", path, pathBuffer, mount, 0);
    }

    // Post-rewrite WOSLINK check: after host alias rewriting, deeper paths
    // like /wki/host/wki resolve to /wki, and /wki/host/wki/wos-1 resolves
    // to /wki/wos-1.  Catch these by examining the resolved pathBuffer.
    if (!is_wki_entry) {
        if (std::strcmp(pathBuffer, "/wki") == 0) {
            is_wki_entry = true;
        } else if (std::strncmp(pathBuffer, "/wki/", 5) == 0) {
            const char* child = pathBuffer + 5;
            bool has_slash = false;
            for (const char* p = child; *p != '\0'; ++p) {
                if (*p == '/') {
                    has_slash = true;
                    break;
                }
            }
            if (*child != '\0' && !has_slash) {
                is_wki_entry = true;
            }
        }
    }

    // Find mount point
    mount_ref = find_mount_point(pathBuffer);
    mount = mount_ref.get();
    if (mount == nullptr) {
        log_loader_path_event("stat-mount-miss", path, pathBuffer, nullptr, -ENOENT);
        return -ENOENT;
    }
    log_loader_path_event("stat-mount-found", path, pathBuffer, mount, 0);

    const char* fs_path = strip_mount_prefix(mount, pathBuffer);
    if (!is_wki_entry) {
        int const CACHED_RESULT =
            metadata_cache_lookup(pathBuffer, mount->fs_type, mount->dev_id, EFFECTIVE_FOLLOW_FINAL_SYMLINK, REQUIRE_DIRECTORY, statbuf);
        if (CACHED_RESULT != -EAGAIN) {
            log_loader_path_event(CACHED_RESULT == 0 ? "stat-cache-hit" : "stat-cache-negative-hit", path, pathBuffer, mount,
                                  CACHED_RESULT);
            return CACHED_RESULT;
        }
    }

    // Initialize stat buffer
    std::memset(statbuf, 0, sizeof(ker::vfs::Stat));

    int result = -ENOSYS;
    bool synthetic_stat = false;
    MetadataSnapshotStamp const STAT_STAMP = metadata_snapshot_stamp();

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
            if (node == nullptr) {
                result = -ENOENT;
                break;
            }
            fill_tmpfs_node_stat(mount->dev_id, node, statbuf);
            result = 0;
            break;
        }
        case FSType::FAT32: {
            result = ker::vfs::fat32::fat32_stat(fs_path, statbuf, static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            break;
        }
        case FSType::DEVFS: {
            // Walk devfs tree to determine if directory or device
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) {
                return -ENOENT;
            }
            statbuf->st_dev = mount->dev_id;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            // Set mode based on node type
            if (node->type == ker::vfs::devfs::DevFSNodeType::DIRECTORY) {
                statbuf->st_mode = S_IFDIR | node->mode;
            } else if (node->type == ker::vfs::devfs::DevFSNodeType::SYMLINK) {
                statbuf->st_mode = S_IFLNK | 0777;
            } else if (node->device != nullptr && node->device->type == ker::dev::DeviceType::BLOCK) {
                statbuf->st_mode = S_IFBLK | node->mode;
            } else {
                statbuf->st_mode = S_IFCHR | node->mode;
            }
            fill_devfs_stat_timestamps(node, statbuf);
            result = 0;
            break;
        }
        case FSType::REMOTE: {
            result = ker::net::wki::wki_remote_vfs_stat(mount->private_data, fs_path, statbuf);
            break;
        }
        case FSType::PROCFS: {
            // procfs: open the path, check if it exists
            auto* f = ker::vfs::procfs::procfs_open_path(fs_path, 0, 0);
            if (f == nullptr) {
                return -ENOENT;
            }
            result = ker::vfs::procfs::procfs_fill_stat(f, statbuf, mount->dev_id);
            // Clean up temporary file (allocated with new in procfs_open_path)
            ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
            delete f;
            break;
        }
        case FSType::XFS: {
            result = ker::vfs::xfs::xfs_stat(fs_path, statbuf, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            break;
        }
        default:
            return -ENOSYS;
    }

    // Synthetic mount-intermediate directory: if the filesystem does not know
    // about this path but it is a strict prefix of an existing mount, the
    // path acts as a virtual directory parent of that mount.  Return a
    // synthetic directory stat so that `ls /` can stat `/wki` even when the
    // backing XFS filesystem has no such directory entry.
    if (result != 0) {
        result = fill_synthetic_mount_dir_stat(pathBuffer, statbuf);
        synthetic_stat = result == 0;
    }

    if (result == 0 && REQUIRE_DIRECTORY && (statbuf->st_mode & S_IFMT) != S_IFDIR) {
        result = -ENOTDIR;
    }

    if (result == 0 && mount != nullptr) {
        statbuf->st_dev = mount->dev_id;
    }

    // WOSLINK post-processing: mark WKI entry directories with S_WOSLINK
    // so userspace tools (e.g., ls -R) can avoid infinite recursion through
    // /wki/host/wki/host/... or /wki/node-1/wki/node-0/wki/node-1/...
    if (result == 0 && is_wki_entry && (statbuf->st_mode & S_IFMT) == S_IFDIR) {
        statbuf->st_mode |= S_WOSLINK;
    }

    if (!is_wki_entry && !synthetic_stat) {
        metadata_cache_store(pathBuffer, mount->fs_type, mount->dev_id, EFFECTIVE_FOLLOW_FINAL_SYMLINK, REQUIRE_DIRECTORY, result, statbuf,
                             STAT_STAMP);
    }

    log_loader_path_event(result == 0 ? "stat-ok" : "stat-failed", path, pathBuffer, mount, result);

    return result;
}

auto vfs_stat(const char* path, Stat* statbuf) -> int { return vfs_stat_impl(path, statbuf, true, true, true); }

auto vfs_lstat(const char* path, Stat* statbuf) -> int { return vfs_stat_impl(path, statbuf, true, true, false); }

auto vfs_stat_resolved(const char* path, Stat* statbuf) -> int { return vfs_stat_impl(path, statbuf, false, false, true); }

auto vfs_fstat_file(File* file, Stat* statbuf) -> int {
    if (file == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    if (file_stat_snapshot_lookup(file, statbuf)) {
        return 0;
    }

    MetadataSnapshotStamp const STAT_STAMP = metadata_snapshot_stamp();
    auto finish_stat = [&](int result) -> int {
        if (result == 0) {
            file_stat_snapshot_store(file, *statbuf, STAT_STAMP);
        }
        return result;
    };

    std::memset(statbuf, 0, sizeof(Stat));

    if (file->fops == nullptr && file->private_data == nullptr && file->is_directory && file->vfs_path != nullptr) {
        return finish_stat(fill_synthetic_mount_dir_stat(file->vfs_path, statbuf));
    }

    auto fstat_dev_id = [&]() -> uint32_t {
        MountRef fstat_mount_ref = (file->vfs_path != nullptr) ? find_mount_point(file->vfs_path) : MountRef{};
        MountPoint const* fstat_mount = fstat_mount_ref.get();
        return fstat_mount != nullptr ? fstat_mount->dev_id : 0;
    };

    switch (file->fs_type) {
        case FSType::TMPFS: {
            uint32_t const FSTAT_DEV_ID = fstat_dev_id();
            // Pipes and epoll reuse FSType::TMPFS but private_data is not a TmpNode
            if (file->fops != ker::vfs::tmpfs::get_tmpfs_fops()) {
                // Return minimal stat for pseudo-TMPFS (pipes, epoll)
                statbuf->st_mode = S_IFIFO;
                statbuf->st_blksize = 4096;
                return finish_stat(0);
            }
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(file->private_data);
            if (node == nullptr) {
                return -EBADF;
            }
            fill_tmpfs_node_stat(FSTAT_DEV_ID, node, statbuf);
            return finish_stat(0);
        }
        case FSType::FAT32: {
            int const R = ker::vfs::fat32::fat32_fstat(file, statbuf);
            if (R == 0) {
                statbuf->st_dev = fstat_dev_id();
            }
            return finish_stat(R);
        }
        case FSType::DEVFS: {
            uint32_t const FSTAT_DEV_ID = fstat_dev_id();
            auto* node = ker::vfs::devfs::devfs_file_node(file);
            statbuf->st_dev = FSTAT_DEV_ID;
            statbuf->st_ino = (node != nullptr) ? reinterpret_cast<ino_t>(node) : 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = (node != nullptr) ? node->uid : 0;
            statbuf->st_gid = (node != nullptr) ? node->gid : 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            if (node != nullptr && node->type == ker::vfs::devfs::DevFSNodeType::DIRECTORY) {
                statbuf->st_mode = S_IFDIR | node->mode;
            } else if (node != nullptr && node->type == ker::vfs::devfs::DevFSNodeType::SYMLINK) {
                statbuf->st_mode = S_IFLNK | node->mode;
            } else if (node != nullptr && node->device != nullptr && node->device->type == ker::dev::DeviceType::BLOCK) {
                statbuf->st_mode = S_IFBLK | node->mode;
            } else {
                statbuf->st_mode = S_IFCHR | ((node != nullptr) ? node->mode : 0666);
            }
            fill_devfs_stat_timestamps(node, statbuf);
            return finish_stat(0);
        }
        case FSType::SOCKET: {
            statbuf->st_dev = fstat_dev_id();
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFSOCK | 0666;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            return finish_stat(0);
        }
        case FSType::REMOTE: {
            int const RET = ker::net::wki::wki_remote_vfs_fstat(file, statbuf);
            if (RET == 0) {
                return finish_stat(0);
            }

            // Fall back to a synthetic stat if path-based remote metadata lookup fails.
            uint32_t const FSTAT_DEV_ID = fstat_dev_id();
            statbuf->st_dev = FSTAT_DEV_ID;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            if (file->is_directory) {
                statbuf->st_mode = S_IFDIR | S_WOSLINK | 0755;
                statbuf->st_size = 0;
            } else {
                statbuf->st_mode = S_IFREG | 0644;
                statbuf->st_size = 0;
            }
            return finish_stat(0);
        }
        case FSType::PROCFS: {
            int const R = ker::vfs::procfs::procfs_fill_stat(file, statbuf, fstat_dev_id());
            return finish_stat(R);
        }
        case FSType::XFS: {
            int const R = ker::vfs::xfs::xfs_fstat(file, statbuf);
            return finish_stat(R);
        }
        default:
            return -ENOSYS;
    }
}

auto vfs_fstat(int fd, Stat* statbuf) -> int {
    if (statbuf == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* file = vfs_get_file_retain(task, fd);
    if (file == nullptr) {
        return -EBADF;
    }

    int const RESULT = vfs_fstat_file(file, statbuf);
    vfs_put_file(file);
    return RESULT;
}

// --- statvfs / fstatvfs ---

static void fill_synthetic_statvfs(Statvfs* buf) {
    std::memset(buf, 0, sizeof(Statvfs));
    buf->f_bsize = 4096;
    buf->f_frsize = 4096;
    buf->f_namemax = 255;
}

auto vfs_statvfs(const char* path, Statvfs* buf) -> int {
    if (path == nullptr || buf == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> path_buffer{};
    if (resolve_task_path_raw(path, path_buffer.data(), path_buffer.size()) < 0) {
        return -ENOENT;
    }

    std::array<char, MAX_PATH_LEN> resolved{};
    if (resolve_symlinks(path_buffer.data(), resolved.data(), resolved.size(), true) < 0) {
        return -ENOENT;
    }

    auto mount_ref = find_mount_point(resolved.data());
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    std::memset(buf, 0, sizeof(Statvfs));

    switch (mount->fs_type) {
        case FSType::XFS:
            return ker::vfs::xfs::xfs_statvfs(static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data), buf);
        case FSType::FAT32:
            return ker::vfs::fat32::fat32_statvfs(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), buf);
        case FSType::TMPFS: {
            return ker::vfs::tmpfs::tmpfs_statvfs(static_cast<ker::vfs::tmpfs::TmpfsMount*>(mount->private_data), buf);
        }
        case FSType::DEVFS:
        case FSType::PROCFS:
        case FSType::SOCKET:
            fill_synthetic_statvfs(buf);
            return 0;
        case FSType::REMOTE:
            fill_synthetic_statvfs(buf);
            buf->f_flag = ST_RDONLY;
            return 0;
        default:
            return -ENOSYS;
    }
}

auto vfs_fstatvfs(int fd, Statvfs* buf) -> int {
    if (buf == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* file = vfs_get_file_retain(task, fd);
    if (file == nullptr) {
        return -EBADF;
    }

    std::memset(buf, 0, sizeof(Statvfs));

    // For any fs type that has a path, delegate to vfs_statvfs so mount
    // context lookup is centralised.
    if (file->vfs_path != nullptr) {
        auto mount_ref = find_mount_point(file->vfs_path);
        MountPoint const* mount = mount_ref.get();
        if (mount != nullptr) {
            switch (mount->fs_type) {
                case FSType::XFS: {
                    int const RESULT = ker::vfs::xfs::xfs_statvfs(static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data), buf);
                    vfs_put_file(file);
                    return RESULT;
                }
                case FSType::FAT32: {
                    int const RESULT =
                        ker::vfs::fat32::fat32_statvfs(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), buf);
                    vfs_put_file(file);
                    return RESULT;
                }
                case FSType::TMPFS: {
                    int const RESULT = ker::vfs::tmpfs::tmpfs_statvfs(static_cast<ker::vfs::tmpfs::TmpfsMount*>(mount->private_data), buf);
                    vfs_put_file(file);
                    return RESULT;
                }
                default:
                    break;
            }
        }
    }

    // Fallback: synthesise by fs_type on the file itself
    switch (file->fs_type) {
        case FSType::TMPFS: {
            ker::vfs::tmpfs::tmpfs_statvfs(nullptr, buf);
            vfs_put_file(file);
            return 0;
        }
        case FSType::DEVFS:
        case FSType::PROCFS:
        case FSType::SOCKET:
            fill_synthetic_statvfs(buf);
            vfs_put_file(file);
            return 0;
        case FSType::REMOTE:
            fill_synthetic_statvfs(buf);
            buf->f_flag = ST_RDONLY;
            vfs_put_file(file);
            return 0;
        default:
            vfs_put_file(file);
            return -ENOSYS;
    }
}

// --- umount ---
auto vfs_umount(const char* target) -> int {
    // Resolve once for mount-scope invalidation lookup.
    // unmount_filesystem() performs its own task-root-aware resolution,
    // so passing an already-resolved path would double-prefix after pivot_root.
    std::array<char, MAX_PATH_LEN> resolved{};
    if (resolve_task_path_raw(target, resolved.data(), resolved.size()) < 0) {
        return -ENAMETOOLONG;
    }

    {
        auto mount_ref = find_mount_point(resolved.data());
        MountPoint const* mount = mount_ref.get();
        if (mount != nullptr && mount->path != nullptr && std::strcmp(mount->path, resolved.data()) == 0) {
            stream_invalidate_mount_scope(mount->fs_type, stream_scope_key_for_mount(mount));
        }
    }
    return unmount_filesystem(target);
}

// --- pivot_root ---
auto vfs_pivot_root(const char* new_root, const char* put_old) -> int {
    if (new_root == nullptr || put_old == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Rewrite mount paths under the mount-table lock so concurrent WKI
    // auto-mounts cannot race a path free/update in find_mount_point().
    size_t const NEW_ROOT_LEN = std::strlen(new_root);
    int const REMAP_RET = remap_mounts_for_pivot(new_root, put_old);
    if (REMAP_RET != 0) {
        if (REMAP_RET == -EINVAL) {
            log::warn("pivot_root: new_root '%s' is not an exact mount point", new_root);
        }
        return REMAP_RET;
    }

    // Set root to new_root for ALL active tasks (not just the caller).
    // Kernel threads (TCP timer, WKI timer, netpoll workers, backlog handlers)
    // must see the same root so that VFS paths like /wki/... resolve through
    // the correct mount hierarchy after the root has moved.
    if (NEW_ROOT_LEN >= ker::mod::sched::task::Task::CWD_MAX) {
        return -ENAMETOOLONG;
    }
    {
        uint32_t const COUNT = ker::mod::sched::get_active_task_count();
        for (uint32_t i = 0; i < COUNT; ++i) {
            auto* t = ker::mod::sched::get_active_task_at_safe(i);
            if (t == nullptr) {
                continue;
            }
            // Only update tasks that still have the old root "/"
            if (t->root[0] == '/' && t->root[1] == '\0') {
                std::memcpy(t->root.data(), new_root, NEW_ROOT_LEN + 1);
            }
            t->release();
        }
    }

    // WKI auto-mounts are driven by deferred work and can land after the
    // initial mount snapshot above, while this pivot is still in progress.
    // Rebase any stale /wki mounts once task roots now point at new_root.
    rebase_wki_mounts_for_new_root(new_root);

    log::info("pivot_root: task '%s' (pid %x) root set to '%s'", task->name, task->pid, new_root);

    if (ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_remote_vfs_rebuild_exports();
    }

    return 0;
}

// --- dup / dup2 ---
auto vfs_dup(int oldfd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, oldfd);
    if (f == nullptr) {
        return -EBADF;
    }
    int const NEWFD = vfs_alloc_fd(task, f);
    if (NEWFD < 0) {
        vfs_put_file(f);
        return -EMFILE;
    }
    return NEWFD;
}

auto vfs_dup2(int oldfd, int newfd, int flags) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    if ((flags & ~ker::vfs::O_CLOEXEC) != 0) {
        return -EINVAL;
    }
    if (newfd < 0 || std::cmp_greater_equal(newfd, ker::mod::sched::task::Task::FD_TABLE_SIZE)) {
        return -EBADF;
    }
    auto* f = vfs_get_file_retain(task, oldfd);
    if (f == nullptr) {
        return -EBADF;
    }
    if (oldfd == newfd) {
        vfs_put_file(f);
        if (flags != 0) {
            return -EINVAL;
        }
        return newfd;
    }

    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    VfsDup2ReplaceResult const REPLACE = vfs_replace_fd_for_dup2_locked(task, newfd, f, (flags & ker::vfs::O_CLOEXEC) != 0);
    task->fd_table_lock.unlock_irqrestore(IRQF);

    if (!REPLACE.inserted) {
        vfs_put_file(f);
        return -EMFILE;
    }

    if (REPLACE.existing != nullptr) {
        advisory_release_process_locks_for_file(ker::mod::sched::task::process_pid(*task), REPLACE.existing);
        vfs_put_file(REPLACE.existing);
    }
    return newfd;
}

// --- getcwd / chdir ---
auto vfs_getcwd(char* buf, size_t size) -> int {
    if (buf == nullptr || size == 0) {
        return -EINVAL;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    size_t const LEN = std::strlen(task->cwd.data());
    if (LEN + 1 > size) {
        return -ERANGE;
    }
    std::memcpy(buf, task->cwd.data(), LEN + 1);
    return 0;
}

auto vfs_chdir(const char* path) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    std::array<char, MAX_PATH_LEN> logical{};
    int const ABSOLUTE = make_absolute(path, logical.data(), logical.size());
    if (ABSOLUTE < 0) {
        return ABSOLUTE;
    }

    int const CANONICAL = canonicalize_path(logical.data(), logical.size());
    if (CANONICAL < 0) {
        return CANONICAL;
    }

    // Verify the path is a directory.  vfs_stat handles root-prefix
    // resolution internally, so pass the logical (user-visible) path.
    ker::vfs::Stat st{};
    int const RET = vfs_stat(logical.data(), &st);
    if (RET < 0) {
        return RET;
    }
    if ((st.st_mode & S_IFDIR) == 0) {
        return -ENOTDIR;
    }

    // Copy to task cwd
    size_t const RLEN = std::strlen(logical.data());
    if (RLEN + 1 > ker::mod::sched::task::Task::CWD_MAX) {
        return -ENAMETOOLONG;
    }
    std::memcpy(task->cwd.data(), logical.data(), RLEN + 1);
    return 0;
}

// --- access ---
// R_OK=4, W_OK=2, X_OK=1, F_OK=0
auto vfs_check_permission(uint32_t file_mode, uint32_t file_uid, uint32_t file_gid, int access_bits) -> int {
    if (access_bits == 0) {
        return 0;  // F_OK - existence only
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Root can do anything (except execute if no execute bit set anywhere)
    if (task->euid == 0) {
        if (((access_bits & 1) != 0) && ((file_mode & 0111) == 0U)) {
            return -EACCES;  // No execute bit set at all
        }
        return 0;
    }

    uint32_t perm_bits{};
    if (task->euid == file_uid) {
        perm_bits = (file_mode >> 6) & 7;  // Owner bits
    } else if (task->has_group(file_gid)) {
        perm_bits = (file_mode >> 3) & 7;  // Group bits
    } else {
        perm_bits = file_mode & 7;  // Other bits
    }

    if (((access_bits & 4) != 0) && ((perm_bits & 4) == 0U)) {
        return -EACCES;  // R_OK
    }
    if (((access_bits & 2) != 0) && ((perm_bits & 2) == 0U)) {
        return -EACCES;  // W_OK
    }
    if (((access_bits & 1) != 0) && ((perm_bits & 1) == 0U)) {
        return -EACCES;  // X_OK
    }
    return 0;
}

auto vfs_access(const char* path, int mode) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    // Check existence first
    ker::vfs::Stat st{};
    int const RET = vfs_stat(path, &st);
    if (RET < 0) {
        return RET;
    }

    if (mode == 0) {
        return 0;  // F_OK - just existence check
    }

    // st_mode already has the full mode bits from stat
    return vfs_check_permission(st.st_mode & 07777, st.st_uid, st.st_gid, mode);
}

namespace {
auto validate_positional_offset(off_t offset, size_t* out) -> int {
    if (offset < 0 || out == nullptr) {
        return -EINVAL;
    }
    *out = static_cast<size_t>(offset);
    return 0;
}
}  // namespace

// --- pread / pwrite ---
auto vfs_pread(int fd, void* buf, size_t count, off_t offset) -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if (!vfs_file_can_read(f)) {
        vfs_put_file(f);
        return -EBADF;
    }
    if (f->fops == nullptr || f->fops->vfs_read == nullptr) {
        vfs_put_file(f);
        return -ENOSYS;
    }
    size_t positional_offset = 0;
    int const OFFSET_RET = validate_positional_offset(offset, &positional_offset);
    if (OFFSET_RET < 0) {
        vfs_put_file(f);
        return OFFSET_RET;
    }
    // Positional reads are often random-access probes over files that were
    // just written (for example Git pack verification). Keep them out of the
    // stream cache so cached EOF/island state cannot affect pread semantics.
    f->positional_read_depth.fetch_add(1, std::memory_order_acq_rel);
    auto result = clamp_io_count(f->fops->vfs_read(f, buf, count, positional_offset), count);
    f->positional_read_depth.fetch_sub(1, std::memory_order_acq_rel);
    vfs_put_file(f);
    return result;
}

auto vfs_pread_file(File* f, void* buf, size_t count, off_t offset) -> ssize_t {
    if (f == nullptr) {
        return -EBADF;
    }
    if (!vfs_file_can_read(f)) {
        return -EBADF;
    }
    if (f->fops == nullptr || f->fops->vfs_read == nullptr) {
        return -ENOSYS;
    }
    size_t positional_offset = 0;
    int const OFFSET_RET = validate_positional_offset(offset, &positional_offset);
    if (OFFSET_RET < 0) {
        return OFFSET_RET;
    }

    f->positional_read_depth.fetch_add(1, std::memory_order_acq_rel);
    auto result = clamp_io_count(f->fops->vfs_read(f, buf, count, positional_offset), count);
    f->positional_read_depth.fetch_sub(1, std::memory_order_acq_rel);
    return result;
}

auto vfs_pread_file_direct(File* f, void* buf, size_t count, off_t offset) -> ssize_t {
    if (f == nullptr) {
        return -EBADF;
    }
    if (!vfs_file_can_read(f)) {
        return -EBADF;
    }
    if (f->fops == nullptr || f->fops->vfs_read == nullptr) {
        return -ENOSYS;
    }
    size_t positional_offset = 0;
    int const OFFSET_RET = validate_positional_offset(offset, &positional_offset);
    if (OFFSET_RET < 0) {
        return OFFSET_RET;
    }

    f->positional_read_depth.fetch_add(1, std::memory_order_acq_rel);
    auto result = clamp_io_count(f->fops->vfs_read(f, buf, count, positional_offset), count);
    f->positional_read_depth.fetch_sub(1, std::memory_order_acq_rel);
    return result;
}

auto vfs_pwrite(int fd, const void* buf, size_t count, off_t offset) -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if (!vfs_file_can_write(f)) {
        vfs_put_file(f);
        return -EBADF;
    }
    if (f->fops == nullptr || f->fops->vfs_write == nullptr) {
        vfs_put_file(f);
        return -ENOSYS;
    }
    size_t positional_offset = 0;
    int const OFFSET_RET = validate_positional_offset(offset, &positional_offset);
    if (OFFSET_RET < 0) {
        vfs_put_file(f);
        return OFFSET_RET;
    }
    auto result = clamp_io_count(f->fops->vfs_write(f, buf, count, positional_offset), count);
    if (result > 0) {
        cache_notify_file_data_changed_impl(f);
    }
    vfs_put_file(f);
    return result;
}

// --- unlink ---
auto vfs_unlink(const char* path) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> path_buf{};
    if (resolve_task_path_raw(path, path_buf.data(), MAX_PATH_LEN) < 0) {
        return -ENAMETOOLONG;
    }

    auto mount_ref = find_mount_point(path_buf.data());
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        int const RET = ker::vfs::xfs::xfs_unlink_path(fs_path, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
        if (RET == 0) {
            vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type == FSType::FAT32) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        int const RET = ker::vfs::fat32::fat32_unlink_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
        if (RET == 0) {
            vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        int const RET = ker::net::wki::wki_remote_vfs_unlink(mount->private_data, fs_path);
        if (RET == 0) {
            vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, path_buf.data());

    // Walk to parent, then find child
    const char* last_slash = nullptr;
    for (const char* p = fs_path; (*p) != 0; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr) {
        parent = tmpfs_root_for_mount(mount);
        name = fs_path;
    } else {
        std::array<char, MAX_PATH_LEN> parent_path{};
        auto paren_len = static_cast<size_t>(last_slash - fs_path);
        if (paren_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(parent_path.data(), fs_path, paren_len);
        parent_path[paren_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), parent_path.data(), false);
        name = last_slash + 1;
    }

    if (parent == nullptr || name == nullptr || *name == '\0') {
        return -ENOENT;
    }

    // Hold tmpfs tree lock to serialize with open_count increment in tmpfs_open_path
    ker::vfs::tmpfs::tmpfs_lock_tree();
    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -ENOENT;
    }
    if (child->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -EISDIR;
    }
    bool const HARDLINK_COUNT_CHANGE = ker::vfs::tmpfs::tmpfs_link_count(child) > 1;
    if (ker::vfs::tmpfs::tmpfs_detach_child(parent, child)) {
        ker::vfs::tmpfs::tmpfs_drop_detached_node(child);
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        if (HARDLINK_COUNT_CHANGE) {
            metadata_cache_note_path_changed("/", nullptr);
        }
        vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        return 0;
    }
    ker::vfs::tmpfs::tmpfs_unlock_tree();
    return -ENOENT;
}

// --- rmdir ---
auto vfs_rmdir(const char* path) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> path_buf{};
    if (resolve_task_path_raw(path, path_buf.data(), path_buf.size()) < 0) {
        return -ENAMETOOLONG;
    }

    auto mount_ref = find_mount_point(path_buf.data());
    MountPoint const* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type == FSType::FAT32) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        int const RET = ker::vfs::fat32::fat32_rmdir_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
        if (RET == 0) {
            vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        int const RET = ker::net::wki::wki_remote_vfs_rmdir(mount->private_data, fs_path);
        if (RET == 0) {
            vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        int const RET = ker::vfs::xfs::xfs_rmdir_path(fs_path, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
        if (RET == 0) {
            vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        }
        return RET;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, path_buf.data());

    const char* last_slash = nullptr;
    for (const char* p = fs_path; (*p) != 0; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr) {
        parent = tmpfs_root_for_mount(mount);
        name = fs_path;
    } else {
        std::array<char, MAX_PATH_LEN> parent_path{};
        auto paren_len = static_cast<size_t>(last_slash - fs_path);
        if (paren_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(parent_path.data(), fs_path, paren_len);
        parent_path[paren_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), parent_path.data(), false);
        name = last_slash + 1;
    }

    if (parent == nullptr || name == nullptr || *name == '\0') {
        return -ENOENT;
    }

    ker::vfs::tmpfs::tmpfs_lock_tree();
    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -ENOENT;
    }
    if (child->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -ENOTDIR;
    }
    if (!ker::vfs::tmpfs::tmpfs_directory_is_empty(child)) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -ENOTEMPTY;
    }

    if (ker::vfs::tmpfs::tmpfs_detach_child(parent, child)) {
        if (child->open_count.load(std::memory_order_acquire) > 0) {
            child->unlinked = true;
        } else {
            ker::vfs::tmpfs::tmpfs_free_node(child);
        }
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        vfs_cache_notify_path_changed(path_buf.data(), nullptr);
        return 0;
    }
    ker::vfs::tmpfs::tmpfs_unlock_tree();
    return -ENOENT;
}

// --- rename ---
auto vfs_rename(const char* oldpath, const char* newpath) -> int {
    if (oldpath == nullptr || newpath == nullptr) {
        return -EINVAL;
    }

    bool const OLD_PATH_REQUIRES_DIRECTORY = path_requires_directory(oldpath);
    bool const NEW_PATH_REQUIRES_DIRECTORY = path_requires_directory(newpath);
    std::array<char, MAX_PATH_LEN> old_buf{};
    std::array<char, MAX_PATH_LEN> new_buf{};
    if (resolve_task_path_raw(oldpath, old_buf.data(), old_buf.size()) < 0) {
        return -ENAMETOOLONG;
    }
    if (resolve_task_path_raw(newpath, new_buf.data(), new_buf.size()) < 0) {
        return -ENAMETOOLONG;
    }

    auto old_mount_ref = find_mount_point(old_buf.data());
    auto new_mount_ref = find_mount_point(new_buf.data());
    MountPoint* old_mount = old_mount_ref.get();
    MountPoint* new_mount = new_mount_ref.get();
    if ((old_mount == nullptr) || (new_mount == nullptr)) {
        return -ENOENT;
    }

    if (OLD_PATH_REQUIRES_DIRECTORY || NEW_PATH_REQUIRES_DIRECTORY) {
        Stat old_stat{};
        int stat_result = vfs_stat_resolved(old_buf.data(), &old_stat);
        if (stat_result < 0) {
            return stat_result;
        }
        if (OLD_PATH_REQUIRES_DIRECTORY && (old_stat.st_mode & S_IFMT) != S_IFDIR) {
            return -ENOTDIR;
        }
        if (NEW_PATH_REQUIRES_DIRECTORY) {
            Stat new_stat{};
            stat_result = vfs_stat_resolved(new_buf.data(), &new_stat);
            if (stat_result < 0) {
                return stat_result;
            }
            if ((new_stat.st_mode & S_IFMT) != S_IFDIR) {
                return -ENOTDIR;
            }
            if ((old_stat.st_mode & S_IFMT) != S_IFDIR) {
                return -EISDIR;
            }
        }
    }

    if (old_mount != new_mount) {
        return -EXDEV;
    }

    if (old_mount->fs_type == FSType::FAT32 && new_mount->fs_type == FSType::FAT32 && old_mount == new_mount) {
        int const RET = ker::vfs::fat32::fat32_rename_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(old_mount->private_data),
                                                           strip_mount_prefix(old_mount, old_buf.data()),
                                                           strip_mount_prefix(new_mount, new_buf.data()));
        if (RET == 0) {
            vfs_cache_notify_path_changed(old_buf.data(), new_buf.data());
        }
        return RET;
    }

    if (old_mount->fs_type == FSType::REMOTE && new_mount->fs_type == FSType::REMOTE && old_mount == new_mount) {
        int const RET = ker::net::wki::wki_remote_vfs_rename(old_mount->private_data, strip_mount_prefix(old_mount, old_buf.data()),
                                                             strip_mount_prefix(new_mount, new_buf.data()));
        if (RET == 0) {
            vfs_cache_notify_path_changed(old_buf.data(), new_buf.data());
        }
        return RET;
    }

    if (old_mount->fs_type == FSType::XFS && new_mount->fs_type == FSType::XFS && old_mount == new_mount) {
        int const RET =
            ker::vfs::xfs::xfs_rename_path(strip_mount_prefix(old_mount, old_buf.data()), strip_mount_prefix(new_mount, new_buf.data()),
                                           static_cast<ker::vfs::xfs::XfsMountContext*>(old_mount->private_data));
        if (RET == 0) {
            vfs_cache_notify_path_changed(old_buf.data(), new_buf.data());
        }
        return RET;
    }

    if (old_mount->fs_type != FSType::TMPFS || new_mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    // Helper lambda to strip mount prefix
    auto strip_mount = [](const char* buf, MountPoint* m) -> const char* {
        size_t const ML = std::strlen(m->path);
        if (ML == 1 && m->path[0] == '/') {
            return buf + 1;
        }
        if (buf[ML] == '/') {
            return buf + ML + 1;
        }
        return buf + ML;
    };

    const char* old_fs = strip_mount(old_buf.data(), old_mount);
    const char* new_fs = strip_mount(new_buf.data(), new_mount);

    if (*old_fs == '\0') {
        return -EINVAL;  // Can't rename root
    }

    ker::vfs::tmpfs::TmpNode* old_parent = nullptr;
    const char* old_name = nullptr;
    int parent_result = tmpfs_resolve_parent_directory_and_name(tmpfs_root_for_mount(old_mount), old_fs, &old_parent, &old_name);
    if (parent_result < 0) {
        return parent_result;
    }

    ker::vfs::tmpfs::tmpfs_lock_tree();
    auto* old_node = ker::vfs::tmpfs::tmpfs_lookup(old_parent, old_name);
    ker::vfs::tmpfs::tmpfs_unlock_tree();
    if (old_node == nullptr) {
        return -ENOENT;
    }
    if (OLD_PATH_REQUIRES_DIRECTORY && old_node->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }

    ker::vfs::tmpfs::TmpNode* new_parent = nullptr;
    const char* new_name = nullptr;
    parent_result = tmpfs_resolve_parent_directory_and_name(tmpfs_root_for_mount(new_mount), new_fs, &new_parent, &new_name);
    if (parent_result < 0) {
        return parent_result;
    }

    if (new_parent == nullptr || new_name == nullptr || *new_name == '\0') {
        return -ENOENT;
    }
    if (new_parent->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }

    // If destination exists, remove it
    ker::vfs::tmpfs::tmpfs_lock_tree();
    auto* existing = ker::vfs::tmpfs::tmpfs_lookup(new_parent, new_name);
    if (NEW_PATH_REQUIRES_DIRECTORY) {
        if (existing == nullptr) {
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return -ENOENT;
        }
        if (existing->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return -ENOTDIR;
        }
        if (old_node->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return -EISDIR;
        }
    }
    if (existing == old_node) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return 0;
    }
    bool existing_hardlink_count_changed = false;
    if (existing != nullptr) {
        if (existing->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY && !ker::vfs::tmpfs::tmpfs_directory_is_empty(existing)) {
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return -ENOTEMPTY;
        }
        existing_hardlink_count_changed =
            existing->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY && ker::vfs::tmpfs::tmpfs_link_count(existing) > 1;
        if (!ker::vfs::tmpfs::tmpfs_detach_child(new_parent, existing)) {
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return -ENOENT;
        }
        if (existing->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY && existing->open_count.load(std::memory_order_acquire) > 0) {
            existing->unlinked = true;
        } else if (existing->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
            ker::vfs::tmpfs::tmpfs_free_node(existing);
        } else {
            ker::vfs::tmpfs::tmpfs_drop_detached_node(existing);
        }
    }

    if (!ker::vfs::tmpfs::tmpfs_detach_child(old_parent, old_node)) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -ENOENT;
    }

    // Rename and reparent
    size_t const NN_LEN = std::strlen(new_name);
    size_t const COPY_LEN = NN_LEN < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1 ? NN_LEN : ker::vfs::tmpfs::TMPFS_NAME_MAX - 1;
    std::memcpy(old_node->name.data(), new_name, COPY_LEN);
    old_node->name[COPY_LEN] = '\0';

    if (!ker::vfs::tmpfs::tmpfs_attach_child(new_parent, old_node)) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -EIO;
    }

    ker::vfs::tmpfs::tmpfs_unlock_tree();
    if (existing_hardlink_count_changed) {
        metadata_cache_note_path_changed("/", nullptr);
    }
    vfs_cache_notify_path_changed(old_buf.data(), new_buf.data());
    return 0;
}

// --- chmod (stub) ---
auto vfs_chmod(const char* path, int mode) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> path_buffer{};
    if (resolve_task_path_raw(path, path_buffer.data(), path_buffer.size()) < 0) {
        return -ENAMETOOLONG;
    }

    auto mount_ref = find_mount_point(path_buffer.data());
    auto* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    const char* fs_path = strip_mount_prefix(mount, path_buffer.data());

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
            if (node == nullptr) {
                return -ENOENT;
            }
            node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
            if (node == nullptr) {
                return -ENOENT;
            }
            node->mode = static_cast<uint32_t>(mode) & 07777;
            cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type);
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) {
                return -ENOENT;
            }
            node->mode = static_cast<uint32_t>(mode) & 07777;
            cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type);
            return 0;
        }
        case FSType::FAT32:
            return 0;  // FAT32 has no permission model; silently accept
        case FSType::XFS: {
            int const RET = ker::vfs::xfs::xfs_chmod_path(fs_path, mode, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            if (RET == 0) {
                cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type);
            }
            return RET;
        }
        default:
            return -ENOSYS;
    }
}

auto vfs_fchmod(int fd, int mode) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    switch (f->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
            if (node == nullptr) {
                vfs_put_file(f);
                return -EBADF;
            }
            node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
            if (node == nullptr) {
                vfs_put_file(f);
                return -EBADF;
            }
            node->mode = static_cast<uint32_t>(mode) & 07777;
            cache_notify_file_metadata_changed_impl(f);
            vfs_put_file(f);
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
            vfs_put_file(f);
            return 0;  // No permission model; silently accept
        case FSType::XFS: {
            int const RESULT = ker::vfs::xfs::xfs_fchmod(f, mode);
            if (RESULT == 0) {
                cache_notify_file_metadata_changed_impl(f);
            }
            vfs_put_file(f);
            return RESULT;
        }
        default:
            vfs_put_file(f);
            return -ENOSYS;
    }
}

auto vfs_chown(const char* path, uint32_t owner, uint32_t group) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> path_buffer{};
    if (resolve_task_path_raw(path, path_buffer.data(), path_buffer.size()) < 0) {
        return -ENAMETOOLONG;
    }

    auto mount_ref = find_mount_point(path_buffer.data());
    auto* mount = mount_ref.get();
    if (mount == nullptr) {
        return -ENOENT;
    }

    const char* fs_path = strip_mount_prefix(mount, path_buffer.data());

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
            if (node == nullptr) {
                return -ENOENT;
            }
            node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
            if (node == nullptr) {
                return -ENOENT;
            }
            if (std::cmp_not_equal(owner, -1)) {
                node->uid = owner;
            }
            if (std::cmp_not_equal(group, -1)) {
                node->gid = group;
            }
            cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type);
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) {
                return -ENOENT;
            }
            if (std::cmp_not_equal(owner, -1)) {
                node->uid = owner;
            }
            if (std::cmp_not_equal(group, -1)) {
                node->gid = group;
            }
            cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type);
            return 0;
        }
        case FSType::FAT32:
        case FSType::XFS:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

auto vfs_fchown(int fd, uint32_t owner, uint32_t group) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    switch (f->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
            if (node == nullptr) {
                vfs_put_file(f);
                return -EBADF;
            }
            node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
            if (node == nullptr) {
                vfs_put_file(f);
                return -EBADF;
            }
            if (std::cmp_not_equal(owner, -1)) {
                node->uid = owner;
            }
            if (std::cmp_not_equal(group, -1)) {
                node->gid = group;
            }
            cache_notify_file_data_changed_impl(f);
            vfs_put_file(f);
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
        case FSType::XFS:
            vfs_put_file(f);
            return 0;  // Accept silently
        default:
            vfs_put_file(f);
            return -ENOSYS;
    }
}

namespace {

auto current_vfs_timespec() -> Timespec {
    uint64_t const NOW_NS = ker::mod::time::get_epoch_ns();
    return Timespec{
        .tv_sec = static_cast<int64_t>(NOW_NS / static_cast<uint64_t>(VFS_NSEC_PER_SEC)),
        .tv_nsec = static_cast<int64_t>(NOW_NS % static_cast<uint64_t>(VFS_NSEC_PER_SEC)),
    };
}

auto resolve_one_utimens_time(const Timespec& requested, const Timespec& now, Timespec* out, bool* should_set) -> int {
    if (out == nullptr || should_set == nullptr) {
        return -EINVAL;
    }

    if (requested.tv_nsec == VFS_UTIME_NOW) {
        *out = now;
        *should_set = true;
        return 0;
    }
    if (requested.tv_nsec == VFS_UTIME_OMIT) {
        *should_set = false;
        return 0;
    }
    if (requested.tv_nsec < 0 || requested.tv_nsec >= VFS_NSEC_PER_SEC) {
        return -EINVAL;
    }

    *out = requested;
    *should_set = true;
    return 0;
}

auto resolve_utimens_times(const Timespec* times, VfsResolvedTimes* resolved) -> int {
    if (resolved == nullptr) {
        return -EINVAL;
    }

    Timespec const NOW = current_vfs_timespec();
    resolved->ctime = NOW;
    if (times == nullptr) {
        resolved->atime = NOW;
        resolved->mtime = NOW;
        resolved->set_atime = true;
        resolved->set_mtime = true;
        return 0;
    }

    if (int const RET = resolve_one_utimens_time(times[0], NOW, &resolved->atime, &resolved->set_atime); RET < 0) {
        return RET;
    }
    return resolve_one_utimens_time(times[1], NOW, &resolved->mtime, &resolved->set_mtime);
}

auto apply_tmpfs_utimens(ker::vfs::tmpfs::TmpNode* node, const VfsResolvedTimes& times) -> int {
    node = ker::vfs::tmpfs::tmpfs_canonical_node(node);
    if (node == nullptr) {
        return -ENOENT;
    }
    if (times.set_atime) {
        node->atime = times.atime;
    }
    if (times.set_mtime) {
        node->mtime = times.mtime;
    }
    if (times.set_atime || times.set_mtime) {
        node->ctime = times.ctime;
    }
    return 0;
}

auto apply_devfs_utimens(ker::vfs::devfs::DevFSNode* node, const VfsResolvedTimes& times) -> int {
    if (node == nullptr) {
        return -ENOENT;
    }
    if (times.set_atime) {
        node->atime = times.atime;
    }
    if (times.set_mtime) {
        node->mtime = times.mtime;
    }
    if (times.set_atime || times.set_mtime) {
        node->ctime = times.ctime;
    }
    return 0;
}

auto vfs_apply_utimens_to_path(const char* path, const Timespec* times, bool follow_final_symlink) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    VfsResolvedTimes resolved_times{};
    if (int const RET = resolve_utimens_times(times, &resolved_times); RET < 0) {
        return RET;
    }

    std::array<char, MAX_PATH_LEN> path_buffer{};
    if (resolve_task_path_raw(path, path_buffer.data(), path_buffer.size()) < 0) {
        return -ENAMETOOLONG;
    }

    auto mount_ref = find_mount_point(path_buffer.data());
    auto* mount = mount_ref.get();
    bool const REMOTE_MOUNT = mount != nullptr && mount->fs_type == FSType::REMOTE;
    if (follow_final_symlink && !REMOTE_MOUNT) {
        std::array<char, MAX_PATH_LEN> resolved_path{};
        int const RESOLVE_RET = resolve_symlinks(path_buffer.data(), resolved_path.data(), resolved_path.size(), true, true);
        if (RESOLVE_RET < 0) {
            return RESOLVE_RET;
        }
        std::memcpy(path_buffer.data(), resolved_path.data(), path_buffer.size());
        mount_ref = find_mount_point(path_buffer.data());
        mount = mount_ref.get();
    }

    if (mount == nullptr) {
        return -ENOENT;
    }
    const char* fs_path = strip_mount_prefix(mount, path_buffer.data());

    int ret = -ENOSYS;
    bool changed = resolved_times.set_atime || resolved_times.set_mtime;
    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
            ret = apply_tmpfs_utimens(node, resolved_times);
            break;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            ret = apply_devfs_utimens(node, resolved_times);
            break;
        }
        case FSType::XFS:
            ret = ker::vfs::xfs::xfs_set_times_path(fs_path, resolved_times.atime, resolved_times.mtime, resolved_times.set_atime,
                                                    resolved_times.set_mtime,
                                                    static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            break;
        case FSType::FAT32:
            changed = false;
            ret = 0;
            break;
        case FSType::REMOTE:
        case FSType::PROCFS:
        case FSType::SOCKET:
        default:
            ret = -ENOSYS;
            break;
    }

    if (ret == 0 && changed) {
        cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type);
    }
    return ret;
}

}  // namespace

auto vfs_utimensat(int dirfd, const char* pathname, const Timespec* times, int flags) -> int {
    constexpr int ALLOWED_FLAGS = AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH;
    if (pathname == nullptr || (flags & ~ALLOWED_FLAGS) != 0) {
        return -EINVAL;
    }

    if ((flags & AT_EMPTY_PATH) != 0 && pathname[0] == '\0') {
        return vfs_futimens(dirfd, times);
    }

    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return vfs_apply_utimens_to_path(pathname, times, (flags & AT_SYMLINK_NOFOLLOW) == 0);
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    std::array<char, MAX_PATH_LEN> resolved{};
    int const RES = vfs_resolve_dirfd(task, dirfd, pathname, resolved.data(), resolved.size());
    if (RES < 0) {
        return RES;
    }
    return vfs_apply_utimens_to_path(resolved.data(), times, (flags & AT_SYMLINK_NOFOLLOW) == 0);
}

auto vfs_futimens(int fd, const Timespec* times) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    VfsResolvedTimes resolved_times{};
    int ret = resolve_utimens_times(times, &resolved_times);
    bool changed = resolved_times.set_atime || resolved_times.set_mtime;
    if (ret == 0) {
        switch (f->fs_type) {
            case FSType::TMPFS:
                if (f->fops != ker::vfs::tmpfs::get_tmpfs_fops()) {
                    ret = -ENOSYS;
                    break;
                }
                ret = apply_tmpfs_utimens(static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data), resolved_times);
                break;
            case FSType::DEVFS:
                ret = apply_devfs_utimens(ker::vfs::devfs::devfs_file_node(f), resolved_times);
                break;
            case FSType::XFS:
                ret = ker::vfs::xfs::xfs_set_times_file(f, resolved_times.atime, resolved_times.mtime, resolved_times.set_atime,
                                                        resolved_times.set_mtime);
                break;
            case FSType::FAT32:
                changed = false;
                ret = 0;
                break;
            case FSType::REMOTE:
            case FSType::PROCFS:
            case FSType::SOCKET:
            default:
                ret = -ENOSYS;
                break;
        }
    }

    if (ret == 0 && changed) {
        cache_notify_file_metadata_changed_impl(f);
    }
    vfs_put_file(f);
    return ret;
}

// --- ftruncate ---
auto vfs_ftruncate(int fd, off_t length) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if (f->fops == nullptr || f->fops->vfs_truncate == nullptr) {
        vfs_put_file(f);
        return -ENOSYS;
    }
    int const RET = f->fops->vfs_truncate(f, length);
    if (RET == 0) {
        cache_notify_file_data_changed_impl(f);
    }
    vfs_put_file(f);
    return RET;
}

// --- fcntl ---
auto vfs_fcntl(int fd, int cmd, uint64_t arg) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* f = vfs_get_file_retain(task, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    // F_DUPFD=0, F_GETFD=1, F_SETFD=2, F_GETFL=3, F_SETFL=4 (Linux values)
    switch (cmd) {
        case 0: {  // F_DUPFD - dup to fd >= arg
            if (arg >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
                vfs_put_file(f);
                return -EINVAL;
            }
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            uint64_t const SLOT = vfs_find_free_fd_below_limit_locked(task, arg);
            bool const INSERTED = SLOT != UINT64_MAX && task->fd_table.insert(SLOT, f);
            if (INSERTED) {
                task->clear_fd_cloexec(static_cast<unsigned>(SLOT));
            }
            task->fd_table_lock.unlock_irqrestore(IRQF);
            if (!INSERTED) {
                vfs_put_file(f);
                return -EMFILE;
            }
            return static_cast<int>(SLOT);
        }
        case 1:  // F_GETFD
        {
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            int const RESULT = task->get_fd_cloexec(static_cast<unsigned>(fd)) ? 1 : 0;
            task->fd_table_lock.unlock_irqrestore(IRQF);
            vfs_put_file(f);
            return RESULT;
        }
        case 2:  // F_SETFD
        {
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            if ((arg & 1) != 0U) {
                task->set_fd_cloexec(static_cast<unsigned>(fd));
            } else {
                task->clear_fd_cloexec(static_cast<unsigned>(fd));
            }
            task->fd_table_lock.unlock_irqrestore(IRQF);
            vfs_put_file(f);
            return 0;
        }
        case 3:  // F_GETFL
        {
            int const RESULT = f->open_flags;
            vfs_put_file(f);
            return RESULT;
        }
        case 4:  // F_SETFL
            f->open_flags = fcntl_setfl_flags(f->open_flags, arg);
            vfs_put_file(f);
            return 0;
        case F_GETLK_CMD:
        case F_OFD_GETLK_CMD: {
            VfsFlockAbi flock{};
            int ret = advisory_copy_from_user(arg, flock);
            if (ret == 0) {
                AdvisoryOwnerKind const OWNER_KIND = cmd == F_OFD_GETLK_CMD ? AdvisoryOwnerKind::OPEN_FILE : AdvisoryOwnerKind::PROCESS;
                ret = advisory_get_lock(f, ker::mod::sched::task::process_pid(*task), OWNER_KIND, AdvisoryLockFamily::RECORD, flock);
            }
            if (ret == 0) {
                ret = advisory_copy_to_user(arg, flock);
            }
            vfs_put_file(f);
            return ret;
        }
        case F_SETLK_CMD:
        case F_SETLKW_CMD:
        case F_OFD_SETLK_CMD:
        case F_OFD_SETLKW_CMD: {
            VfsFlockAbi flock{};
            int ret = advisory_copy_from_user(arg, flock);
            if (ret == 0) {
                AdvisoryOwnerKind const OWNER_KIND =
                    (cmd == F_OFD_SETLK_CMD || cmd == F_OFD_SETLKW_CMD) ? AdvisoryOwnerKind::OPEN_FILE : AdvisoryOwnerKind::PROCESS;
                bool const WAIT = cmd == F_SETLKW_CMD || cmd == F_OFD_SETLKW_CMD;
                ret = advisory_set_lock(f, ker::mod::sched::task::process_pid(*task), OWNER_KIND, AdvisoryLockFamily::RECORD, flock, WAIT);
            }
            vfs_put_file(f);
            return ret;
        }
        case WOS_FLOCK_CMD: {
            int const RET = advisory_flock(f, static_cast<int>(arg));
            vfs_put_file(f);
            return RET;
        }
        case 1030: {  // F_DUPFD_CLOEXEC - dup to fd >= arg, set close-on-exec
            if (arg >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
                vfs_put_file(f);
                return -EINVAL;
            }
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            uint64_t const SLOT = vfs_find_free_fd_below_limit_locked(task, arg);
            bool const INSERTED = SLOT != UINT64_MAX && task->fd_table.insert(SLOT, f);
            if (INSERTED) {
                task->set_fd_cloexec(static_cast<unsigned>(SLOT));
            }
            task->fd_table_lock.unlock_irqrestore(IRQF);
            if (!INSERTED) {
                vfs_put_file(f);
                return -EMFILE;
            }
            return static_cast<int>(SLOT);
        }
        default:
            vfs_put_file(f);
            return -EINVAL;
    }
}

// --- pipe ---

// File-scope pointers to the pipe fops (set once during first vfs_pipe() call).
// Used by vfs_is_pipe_file() to identify pipe file descriptors.
namespace {
FileOperations* g_pipe_read_fops_ptr = nullptr;
FileOperations* g_pipe_write_fops_ptr = nullptr;
using PipeWakeList = std::array<uint64_t, PIPE_WAKE_BATCH>;
using PipeWaiterList = ker::util::SmallVec<uint64_t, PIPE_WAITER_INLINE_CAPACITY>;
struct PipeState {
    char* buf;
    size_t capacity;
    size_t head;   // write position
    size_t tail;   // read position
    size_t count;  // bytes in buffer
    bool write_closed;
    bool read_closed;
    bool direct_write_active;
    // Counts open file-ends (read + write). Initialized to 2; the closer that
    // drives it to 0 is responsible for freeing buf and this struct.
    std::atomic<int> open_ends{2};
    ker::mod::sys::Spinlock lock;

    // Wait queues for blocking pipe I/O. Keep a 32-job make jobserver's pipe
    // waiters inline so registration does not allocate under the pipe lock.
    PipeWaiterList readers_waiting;
    PipeWaiterList writers_waiting;

    PipeWaiterList read_poll_waiting;
    PipeWaiterList write_poll_waiting;
};

std::deque<PipeState*> g_pipe_states;
ker::mod::sys::Spinlock g_pipe_states_lock;
std::atomic<uint64_t> g_pipe_created_since_reset{0};
std::atomic<uint64_t> g_pipe_active_count{0};
std::atomic<uint64_t> g_pipe_active_capacity_bytes{0};
std::atomic<uint64_t> g_pipe_peak_count{0};
std::atomic<uint64_t> g_pipe_peak_capacity_bytes{0};

struct PipeDirectWriteWindow {
    size_t offset;
    size_t capacity;
};

void pipe_diag_append(char* buf, size_t bufsz, size_t& len, bool& truncated, const char* fmt, ...) __attribute__((format(printf, 5, 6)));

void pipe_diag_append(char* buf, size_t bufsz, size_t& len, bool& truncated, const char* fmt, ...) {
    if (len >= bufsz) {
        truncated = true;
        return;
    }

    va_list args;
    va_start(args, fmt);
    int const WRITTEN = std::vsnprintf(buf + len, bufsz - len, fmt, args);
    va_end(args);
    if (WRITTEN < 0) {
        return;
    }

    auto const USED = static_cast<size_t>(WRITTEN);
    if (USED >= bufsz - len) {
        len = bufsz - 1;
        truncated = true;
        return;
    }
    len += USED;
}

#ifdef WOS_SELFTEST
enum class PipeSelftestFailStage : int {
    NONE = 0,
    BUFFER = 1,
    STATE = 2,
    READ_FILE = 3,
    WRITE_FILE = 4,
    READ_FD = 5,
    WRITE_FD = 6,
};

std::atomic<int> g_vfs_selftest_pipe_fail_stage{static_cast<int>(PipeSelftestFailStage::NONE)};

auto pipe_selftest_should_fail(PipeSelftestFailStage stage) -> bool {
    return g_vfs_selftest_pipe_fail_stage.load(std::memory_order_relaxed) == static_cast<int>(stage);
}
#endif

void pipe_update_peak(std::atomic<uint64_t>& peak, uint64_t candidate) {
    uint64_t observed = peak.load(std::memory_order_relaxed);
    while (candidate > observed && !peak.compare_exchange_weak(observed, candidate, std::memory_order_release, std::memory_order_relaxed)) {
    }
}

void pipe_note_capacity_delta(size_t added, size_t removed = 0) {
    uint64_t active_capacity = g_pipe_active_capacity_bytes.load(std::memory_order_relaxed);
    if (added > 0) {
        active_capacity = g_pipe_active_capacity_bytes.fetch_add(added, std::memory_order_acq_rel) + added;
    }
    if (removed > 0) {
        active_capacity = g_pipe_active_capacity_bytes.fetch_sub(removed, std::memory_order_acq_rel) - removed;
    }
    pipe_update_peak(g_pipe_peak_capacity_bytes, active_capacity);
}

void pipe_register_state(PipeState* st) {
    if (st == nullptr) {
        return;
    }

    uint64_t const IRQF = g_pipe_states_lock.lock_irqsave();
    g_pipe_states.push_back(st);
    g_pipe_states_lock.unlock_irqrestore(IRQF);

    g_pipe_created_since_reset.fetch_add(1, std::memory_order_acq_rel);
    uint64_t const ACTIVE = g_pipe_active_count.fetch_add(1, std::memory_order_acq_rel) + 1;
    pipe_update_peak(g_pipe_peak_count, ACTIVE);
    pipe_note_capacity_delta(st->capacity);
}

void pipe_unregister_state(PipeState* st) {
    if (st == nullptr) {
        return;
    }

    uint64_t const IRQF = g_pipe_states_lock.lock_irqsave();
    auto it = std::ranges::find(g_pipe_states, st);
    if (it != g_pipe_states.end()) {
        g_pipe_states.erase(it);
        g_pipe_active_count.fetch_sub(1, std::memory_order_acq_rel);
        pipe_note_capacity_delta(0, st->capacity);
    }
    g_pipe_states_lock.unlock_irqrestore(IRQF);
}

void pipe_destroy_state(PipeState* st) {
    if (st == nullptr) {
        return;
    }

    pipe_unregister_state(st);
    delete[] st->buf;
    delete st;
}

void pipe_destroy_unregistered_state(PipeState* st) {
    if (st == nullptr) {
        return;
    }

    delete[] st->buf;
    delete st;
}

void pipe_init_file(File* file, PipeState* state, FileOperations* fops, int open_flags) {
    file->private_data = state;
    file->fops = fops;
    file->pos = 0;
    file->is_directory = false;
    file->fs_type = FSType::TMPFS;
    file->refcount.store(1, std::memory_order_relaxed);
    file->open_flags = open_flags;
    file->fd_flags = 0;
    file->vfs_path = nullptr;
    file->dir_fs_count = 0;
}

auto pipe_register_waiter(PipeWaiterList& waiters, uint64_t pid) -> bool {
    for (unsigned long waiter : waiters) {
        if (waiter == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

auto pipe_register_poll_waiter(PipeWaiterList& waiters, uint64_t pid) -> bool { return pipe_register_waiter(waiters, pid); }

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
}

auto current_task_has_deliverable_signal() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }
    return task->has_interrupting_signal_pending();
}

void signal_current_sigpipe() {
    auto* task = ker::mod::sched::get_current_task();
    if (task != nullptr) {
        task->signal_add_pending_mask(1ULL << (13 - 1));
    }
}

void perf_record_local_pipe_event(uint8_t op, ker::mod::perf::WkiPerfPhase phase, uint32_t correlation, int32_t status, uint32_t aux,
                                  uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::LOCAL_PIPE, op, phase, 0, 0,
                                     correlation, status, aux, callsite);
}

void perf_record_local_pipe_summary(uint8_t op, int32_t status, uint32_t latency_us, uint64_t bytes = 0) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_PIPE, op, 0, 0, status, latency_us, true, 0, bytes);
}

auto perf_elapsed_since_us(uint64_t started_us) -> uint32_t {
    uint64_t const NOW_US = ker::mod::time::get_us();
    uint64_t const ELAPSED_US = NOW_US >= started_us ? NOW_US - started_us : 0;
    return ELAPSED_US > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(ELAPSED_US);
}

auto perf_local_pipe_stage_started_us() -> uint64_t { return ker::mod::perf::is_wki_recording_enabled() ? ker::mod::time::get_us() : 0; }

void perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp op, uint32_t correlation, int32_t status, uint64_t started_us,
                                  uint64_t bytes, uint64_t callsite) {
    if (started_us == 0 || !ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    uint32_t const ELAPSED_US = perf_elapsed_since_us(started_us);
    auto const OP = static_cast<uint8_t>(op);
    perf_record_local_pipe_event(OP, ker::mod::perf::WkiPerfPhase::END, correlation, status, ELAPSED_US, callsite);
    perf_record_local_pipe_summary(OP, status, ELAPSED_US, bytes);
}

void pipe_collect_waiters_locked(PipeWaiterList& waiters, PipeWakeList& pending, size_t* pending_count) {
    size_t copied = 0;
    while (copied < pending.size() && !waiters.empty()) {
        pending[copied++] = waiters.at(0);
        static_cast<void>(waiters.remove_at(0));
    }
    *pending_count = copied;
}

void pipe_reschedule_waiters(const PipeWakeList& waiters, size_t waiter_count, bool sigpipe = false) {
    for (size_t i = 0; i < waiter_count; i++) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(waiters[i]);
        if (waiter == nullptr) {
            continue;
        }

        if (sigpipe) {
            waiter->signal_add_pending_mask(1ULL << (13 - 1));
        }

        ker::mod::sched::wake_task_from_event(waiter);
        waiter->release();
    }
}

auto pipe_contiguous_write_space_locked(const PipeState* st) -> size_t {
    if (st == nullptr || st->count >= st->capacity) {
        return 0;
    }
    if (st->head < st->tail) {
        return st->tail - st->head;
    }
    return st->capacity - st->head;
}

auto pipe_direct_capacity_target(File* infile, off_t source_offset, size_t count) -> size_t {
    if (infile == nullptr || source_offset < 0 || count == 0) {
        return 0;
    }

    Stat statbuf{};
    if (vfs_stream_cache_get_file_stat(infile, &statbuf) != 0 || statbuf.st_size <= source_offset) {
        return 0;
    }

    auto const REMAINING64 = static_cast<uint64_t>(statbuf.st_size - source_offset);
    size_t target = count > PIPE_DIRECT_MAX_CAPACITY ? PIPE_DIRECT_MAX_CAPACITY : count;
    if (REMAINING64 < static_cast<uint64_t>(target)) {
        target = static_cast<size_t>(REMAINING64);
    }
    return target;
}

auto pipe_install_buffer_locked(PipeState* st, char* new_buf, size_t new_capacity) -> char* {
    if (st == nullptr || new_buf == nullptr || new_capacity <= st->capacity || new_capacity < st->count || st->direct_write_active) {
        return nullptr;
    }

    char* old_buf = st->buf;
    if (st->count > 0) {
        size_t const FIRST = std::min(st->count, st->capacity - st->tail);
        std::memcpy(new_buf, old_buf + st->tail, FIRST);
        if (FIRST < st->count) {
            std::memcpy(new_buf + FIRST, old_buf, st->count - FIRST);
        }
    }

    st->buf = new_buf;
    st->capacity = new_capacity;
    st->tail = 0;
    st->head = st->count % st->capacity;
    return old_buf;
}

void pipe_reserve_direct_capacity(PipeState* st, File* infile, off_t source_offset, size_t count) {
    size_t const TARGET = pipe_direct_capacity_target(infile, source_offset, count);
    if (st == nullptr || TARGET <= PIPE_DEFAULT_CAPACITY) {
        return;
    }

    uint64_t irqf = st->lock.lock_irqsave();
    bool const ALREADY_LARGE_ENOUGH = TARGET <= st->capacity;
    bool const RESIZE_BLOCKED = st->direct_write_active;
    st->lock.unlock_irqrestore(irqf);
    if (ALREADY_LARGE_ENOUGH || RESIZE_BLOCKED) {
        return;
    }

    auto* new_buf = new char[TARGET];
    if (new_buf == nullptr) {
        return;
    }

    char* old_buf = nullptr;
    size_t old_capacity = 0;
    irqf = st->lock.lock_irqsave();
    old_capacity = st->capacity;
    old_buf = pipe_install_buffer_locked(st, new_buf, TARGET);
    st->lock.unlock_irqrestore(irqf);

    if (old_buf == nullptr) {
        delete[] new_buf;
        return;
    }

    pipe_note_capacity_delta(TARGET, old_capacity);
    delete[] old_buf;
}

auto pipe_begin_direct_write(File* pipe_file, PipeState* st, PipeDirectWriteWindow* window, uint32_t correlation, uint64_t callsite)
    -> int {
    if (pipe_file == nullptr || st == nullptr || window == nullptr) {
        return -EINVAL;
    }

    for (;;) {
        uint64_t const IRQF = st->lock.lock_irqsave();
        if (st->read_closed) {
            st->lock.unlock_irqrestore(IRQF);
            signal_current_sigpipe();
            return -EPIPE;
        }

        if (!st->direct_write_active) {
            size_t const CONTIGUOUS = pipe_contiguous_write_space_locked(st);
            if (CONTIGUOUS > 0) {
                st->direct_write_active = true;
                window->offset = st->head;
                window->capacity = CONTIGUOUS;
                st->lock.unlock_irqrestore(IRQF);
                return 0;
            }
        }

        if (pipe_file->open_flags & O_NONBLOCK) {
            st->lock.unlock_irqrestore(IRQF);
            return -EAGAIN;
        }

        auto* current_task = ker::mod::sched::get_current_task();
        if (current_task == nullptr) {
            st->lock.unlock_irqrestore(IRQF);
            return -ESRCH;
        }

        if (current_task_has_deliverable_signal()) {
            st->lock.unlock_irqrestore(IRQF);
            return -EINTR;
        }

        bool const REGISTERED = pipe_register_waiter(st->writers_waiting, current_task->pid);
        auto const PIPE_SPACE = static_cast<uint32_t>(st->capacity - st->count);
        st->lock.unlock_irqrestore(IRQF);
        perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::BLOCK_WRITE),
                                     ker::mod::perf::WkiPerfPhase::POINT, correlation, 0, PIPE_SPACE, callsite);
        if (REGISTERED) {
            ker::mod::sched::preemptible_syscall_park("pipe_write", ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE);
        } else {
            ker::mod::sched::kern_yield();
        }
    }
}

auto pipe_finish_direct_write(PipeState* st, size_t bytes_read) -> bool {
    if (st == nullptr) {
        return false;
    }

    PipeWakeList pending_readers{};
    size_t pending_readers_count = 0;
    PipeWakeList pending_read_pollers{};
    size_t pending_read_pollers_count = 0;
    PipeWakeList pending_writers{};
    size_t pending_writers_count = 0;
    PipeWakeList pending_write_pollers{};
    size_t pending_write_pollers_count = 0;
    bool committed = false;

    uint64_t const IRQF = st->lock.lock_irqsave();
    st->direct_write_active = false;
    if (bytes_read > 0 && !st->read_closed) {
        st->head = (st->head + bytes_read) % st->capacity;
        st->count += bytes_read;
        committed = true;

        if (!st->readers_waiting.empty()) {
            pipe_collect_waiters_locked(st->readers_waiting, pending_readers, &pending_readers_count);
        }
        if (!st->read_poll_waiting.empty()) {
            pipe_collect_waiters_locked(st->read_poll_waiting, pending_read_pollers, &pending_read_pollers_count);
        }
    }

    if (st->capacity > st->count) {
        if (!st->writers_waiting.empty()) {
            pipe_collect_waiters_locked(st->writers_waiting, pending_writers, &pending_writers_count);
        }
        if (!st->write_poll_waiting.empty()) {
            pipe_collect_waiters_locked(st->write_poll_waiting, pending_write_pollers, &pending_write_pollers_count);
        }
    }
    bool const READ_CLOSED = st->read_closed;
    st->lock.unlock_irqrestore(IRQF);

    pipe_reschedule_waiters(pending_readers, pending_readers_count);
    pipe_reschedule_waiters(pending_read_pollers, pending_read_pollers_count);
    pipe_reschedule_waiters(pending_writers, pending_writers_count, READ_CLOSED);
    pipe_reschedule_waiters(pending_write_pollers, pending_write_pollers_count);
    return committed;
}

auto file_pread_direct(File* file, void* buf, size_t count, off_t offset) -> ssize_t {
    return vfs_pread_file_direct(file, buf, count, offset);
}

auto vfs_sendfile_to_pipe(File* outfile, File* infile, off_t* source_offset, size_t count) -> ssize_t {
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = ker::mod::time::get_us();
    auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
        auto const ELAPSED_US = static_cast<uint32_t>(ker::mod::time::get_us() - STARTED_US);
        int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
        perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::WRITE), ker::mod::perf::WkiPerfPhase::END,
                                     CORRELATION, STATUS, ELAPSED_US, CALLSITE);
        perf_record_local_pipe_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::WRITE), STATUS, ELAPSED_US, bytes);
        return rc;
    };
    perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::WRITE), ker::mod::perf::WkiPerfPhase::BEGIN,
                                 CORRELATION, 0, 0, CALLSITE);

    auto* st = static_cast<PipeState*>(outfile->private_data);
    if (st == nullptr) {
        return finish(-EBADF);
    }

    {
        uint64_t const RESERVE_CALLSITE = WOS_PERF_CALLSITE();
        uint64_t const RESERVE_STARTED_US = perf_local_pipe_stage_started_us();
        pipe_reserve_direct_capacity(st, infile, *source_offset, count);
        perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp::DIRECT_RESERVE, CORRELATION, 0, RESERVE_STARTED_US, 0,
                                     RESERVE_CALLSITE);
    }

    ssize_t total_sent = 0;
    size_t remaining = count;
    while (remaining > 0) {
        PipeDirectWriteWindow window{};
        uint64_t const BEGIN_CALLSITE = WOS_PERF_CALLSITE();
        uint64_t const BEGIN_STARTED_US = perf_local_pipe_stage_started_us();
        int const BEGIN_RET = pipe_begin_direct_write(outfile, st, &window, CORRELATION, CALLSITE);
        perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp::DIRECT_BEGIN, CORRELATION, BEGIN_RET < 0 ? BEGIN_RET : 0,
                                     BEGIN_STARTED_US, BEGIN_RET < 0 ? 0 : window.capacity, BEGIN_CALLSITE);
        if (BEGIN_RET < 0) {
            return finish(total_sent == 0 ? static_cast<ssize_t>(BEGIN_RET) : total_sent, static_cast<uint64_t>(total_sent));
        }

        size_t const TO_READ = std::min(remaining, window.capacity);
        uint64_t const READ_CALLSITE = WOS_PERF_CALLSITE();
        uint64_t const READ_STARTED_US = perf_local_pipe_stage_started_us();
        ssize_t const READ_RET = file_pread_direct(infile, st->buf + window.offset, TO_READ, *source_offset);
        uint64_t const READ_BYTES = READ_RET > 0 ? static_cast<uint64_t>(std::min(static_cast<size_t>(READ_RET), TO_READ)) : 0;
        perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp::DIRECT_READ, CORRELATION,
                                     READ_RET < 0 ? static_cast<int32_t>(READ_RET) : 0, READ_STARTED_US, READ_BYTES, READ_CALLSITE);
        if (READ_RET <= 0) {
            uint64_t const COMMIT_CALLSITE = WOS_PERF_CALLSITE();
            uint64_t const COMMIT_STARTED_US = perf_local_pipe_stage_started_us();
            static_cast<void>(pipe_finish_direct_write(st, 0));
            perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp::DIRECT_COMMIT, CORRELATION, 0, COMMIT_STARTED_US, 0,
                                         COMMIT_CALLSITE);
            if (READ_RET < 0 && total_sent == 0) {
                return finish(READ_RET);
            }
            break;
        }

        size_t const BYTES_READ = static_cast<size_t>(READ_BYTES);
        uint64_t const COMMIT_CALLSITE = WOS_PERF_CALLSITE();
        uint64_t const COMMIT_STARTED_US = perf_local_pipe_stage_started_us();
        if (!pipe_finish_direct_write(st, BYTES_READ)) {
            perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp::DIRECT_COMMIT, CORRELATION, -EPIPE, COMMIT_STARTED_US,
                                         BYTES_READ, COMMIT_CALLSITE);
            signal_current_sigpipe();
            return finish(total_sent == 0 ? static_cast<ssize_t>(-EPIPE) : total_sent, static_cast<uint64_t>(total_sent));
        }
        perf_record_local_pipe_stage(ker::mod::perf::WkiPerfLocalPipeOp::DIRECT_COMMIT, CORRELATION, 0, COMMIT_STARTED_US, BYTES_READ,
                                     COMMIT_CALLSITE);

        total_sent += static_cast<ssize_t>(BYTES_READ);
        *source_offset += static_cast<off_t>(BYTES_READ);
        remaining -= BYTES_READ;
    }

    return finish(total_sent, static_cast<uint64_t>(total_sent));
}
}  // namespace

namespace {
auto vfs_pipe_for_task(ker::mod::sched::task::Task* task, int pipefd[2],
                       int flags = 0) -> int {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    if (pipefd == nullptr) {
        return -EINVAL;
    }
    if (task == nullptr) {
        return -ESRCH;
    }
    constexpr int PIPE_SUPPORTED_FLAGS = ker::vfs::O_CLOEXEC | O_NONBLOCK;
    if ((flags & ~PIPE_SUPPORTED_FLAGS) != 0) {
        return -EINVAL;
    }

    // Keep a moderate default capacity so simple producer/consumer pipelines do
    // not bounce through the scheduler every 4 KiB.
#ifdef WOS_SELFTEST
    if (pipe_selftest_should_fail(PipeSelftestFailStage::BUFFER)) {
        return -ENOMEM;
    }
#endif
    auto* pipe_buf = new char[PIPE_DEFAULT_CAPACITY];
    if (pipe_buf == nullptr) {
        return -ENOMEM;
    }

#ifdef WOS_SELFTEST
    if (pipe_selftest_should_fail(PipeSelftestFailStage::STATE)) {
        delete[] pipe_buf;
        return -ENOMEM;
    }
#endif
    auto* ps = new PipeState{};
    if (ps == nullptr) {
        delete[] pipe_buf;
        return -ENOMEM;
    }
    ps->buf = pipe_buf;
    ps->capacity = PIPE_DEFAULT_CAPACITY;
    ps->head = 0;
    ps->tail = 0;
    ps->count = 0;
    ps->write_closed = false;
    ps->read_closed = false;
    ps->direct_write_active = false;

    // Pipe fops - static lambdas converted to function pointers
    static auto pipe_read = [](File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
        uint64_t const STARTED_US = ker::mod::time::get_us();
        auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
            uint32_t const ELAPSED_US = static_cast<uint32_t>(ker::mod::time::get_us() - STARTED_US);
            int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
            perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::READ), ker::mod::perf::WkiPerfPhase::END,
                                         CORRELATION, STATUS, ELAPSED_US, CALLSITE);
            perf_record_local_pipe_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::READ), STATUS, ELAPSED_US, bytes);
            return rc;
        };
        perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::READ), ker::mod::perf::WkiPerfPhase::BEGIN,
                                     CORRELATION, 0, 0, CALLSITE);
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) {
            return finish(-EBADF);
        }
        if (count == 0) {
            return finish(0);
        }

        for (;;) {
            PipeWakeList pending_writers{};
            size_t pending_writers_count = 0;
            PipeWakeList pending_write_pollers{};
            size_t pending_write_pollers_count = 0;
            uint64_t const IRQF = st->lock.lock_irqsave();

            if (st->count > 0) {
                size_t const TO_READ = count < st->count ? count : st->count;
                auto* dst = static_cast<char*>(buf);
                size_t const FIRST = st->capacity - st->tail;
                if (FIRST >= TO_READ) {
                    std::memcpy(dst, st->buf + st->tail, TO_READ);
                } else {
                    std::memcpy(dst, st->buf + st->tail, FIRST);
                    std::memcpy(dst + FIRST, st->buf, TO_READ - FIRST);
                }
                st->tail = (st->tail + TO_READ) % st->capacity;
                st->count -= TO_READ;

                if (!st->writers_waiting.empty()) {
                    pipe_collect_waiters_locked(st->writers_waiting, pending_writers, &pending_writers_count);
                }
                if (!st->write_poll_waiting.empty()) {
                    pipe_collect_waiters_locked(st->write_poll_waiting, pending_write_pollers, &pending_write_pollers_count);
                }

                st->lock.unlock_irqrestore(IRQF);
                pipe_reschedule_waiters(pending_writers, pending_writers_count);
                pipe_reschedule_waiters(pending_write_pollers, pending_write_pollers_count);
                return finish(static_cast<ssize_t>(TO_READ), static_cast<uint64_t>(TO_READ));
            }

            if (st->write_closed) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(0);
            }

            if (f->open_flags & O_NONBLOCK) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(-EAGAIN);
            }

            auto* current_task = ker::mod::sched::get_current_task();
            if (current_task == nullptr) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(-ESRCH);
            }

            if (current_task_has_deliverable_signal()) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(-EINTR);
            }

            bool const REGISTERED = pipe_register_waiter(st->readers_waiting, current_task->pid);
            uint32_t const PIPE_COUNT = static_cast<uint32_t>(st->count);
            st->lock.unlock_irqrestore(IRQF);
            perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::BLOCK_READ),
                                         ker::mod::perf::WkiPerfPhase::POINT, CORRELATION, 0, PIPE_COUNT, CALLSITE);
            if (REGISTERED) {
                ker::mod::sched::preemptible_syscall_park("pipe_read", ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE);
            } else {
                ker::mod::sched::kern_yield();
            }
        }
    };

    static auto pipe_write = [](File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
        uint64_t const STARTED_US = ker::mod::time::get_us();
        auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
            uint32_t const ELAPSED_US = static_cast<uint32_t>(ker::mod::time::get_us() - STARTED_US);
            int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
            perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::WRITE), ker::mod::perf::WkiPerfPhase::END,
                                         CORRELATION, STATUS, ELAPSED_US, CALLSITE);
            perf_record_local_pipe_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::WRITE), STATUS, ELAPSED_US, bytes);
            return rc;
        };
        perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::WRITE), ker::mod::perf::WkiPerfPhase::BEGIN,
                                     CORRELATION, 0, 0, CALLSITE);
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) {
            return finish(-EBADF);
        }
        if (count == 0) {
            return finish(0);
        }

        for (;;) {
            PipeWakeList pending_readers{};
            size_t pending_readers_count = 0;
            PipeWakeList pending_read_pollers{};
            size_t pending_read_pollers_count = 0;
            uint64_t const IRQF = st->lock.lock_irqsave();
            if (st->read_closed) {
                st->lock.unlock_irqrestore(IRQF);
                // Send SIGPIPE to the writing process (signal 13)
                auto* task = ker::mod::sched::get_current_task();
                if (task) {
                    task->signal_add_pending_mask(1ULL << (13 - 1));
                }
                return finish(-EPIPE);
            }

            if (st->direct_write_active) {
                if (f->open_flags & O_NONBLOCK) {
                    st->lock.unlock_irqrestore(IRQF);
                    return finish(-EAGAIN);
                }

                auto* current_task = ker::mod::sched::get_current_task();
                if (current_task == nullptr) {
                    st->lock.unlock_irqrestore(IRQF);
                    return finish(-ESRCH);
                }

                if (current_task_has_deliverable_signal()) {
                    st->lock.unlock_irqrestore(IRQF);
                    return finish(-EINTR);
                }

                bool const REGISTERED = pipe_register_waiter(st->writers_waiting, current_task->pid);
                auto const PIPE_SPACE = static_cast<uint32_t>(st->capacity - st->count);
                st->lock.unlock_irqrestore(IRQF);
                perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::BLOCK_WRITE),
                                             ker::mod::perf::WkiPerfPhase::POINT, CORRELATION, 0, PIPE_SPACE, CALLSITE);
                if (REGISTERED) {
                    ker::mod::sched::preemptible_syscall_park("pipe_write", ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE);
                } else {
                    ker::mod::sched::kern_yield();
                }
                continue;
            }

            size_t const AVAIL = st->capacity - st->count;
            if (AVAIL > 0) {
                size_t const TO_WRITE = count < AVAIL ? count : AVAIL;
                const auto* src = static_cast<const char*>(buf);
                size_t const FIRST = st->capacity - st->head;
                if (FIRST >= TO_WRITE) {
                    std::memcpy(st->buf + st->head, src, TO_WRITE);
                } else {
                    std::memcpy(st->buf + st->head, src, FIRST);
                    std::memcpy(st->buf, src + FIRST, TO_WRITE - FIRST);
                }
                st->head = (st->head + TO_WRITE) % st->capacity;
                st->count += TO_WRITE;

                if (!st->readers_waiting.empty()) {
                    pipe_collect_waiters_locked(st->readers_waiting, pending_readers, &pending_readers_count);
                }
                if (!st->read_poll_waiting.empty()) {
                    pipe_collect_waiters_locked(st->read_poll_waiting, pending_read_pollers, &pending_read_pollers_count);
                }

                st->lock.unlock_irqrestore(IRQF);
                pipe_reschedule_waiters(pending_readers, pending_readers_count);
                pipe_reschedule_waiters(pending_read_pollers, pending_read_pollers_count);
                return finish(static_cast<ssize_t>(TO_WRITE), static_cast<uint64_t>(TO_WRITE));
            }

            if (f->open_flags & O_NONBLOCK) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(-EAGAIN);
            }

            auto* current_task = ker::mod::sched::get_current_task();
            if (current_task == nullptr) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(-ESRCH);
            }

            if (current_task_has_deliverable_signal()) {
                st->lock.unlock_irqrestore(IRQF);
                return finish(-EINTR);
            }

            bool const REGISTERED = pipe_register_waiter(st->writers_waiting, current_task->pid);
            uint32_t const PIPE_SPACE = static_cast<uint32_t>(st->capacity - st->count);
            st->lock.unlock_irqrestore(IRQF);
            perf_record_local_pipe_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalPipeOp::BLOCK_WRITE),
                                         ker::mod::perf::WkiPerfPhase::POINT, CORRELATION, 0, PIPE_SPACE, CALLSITE);
            if (REGISTERED) {
                ker::mod::sched::preemptible_syscall_park("pipe_write", ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE);
            } else {
                ker::mod::sched::kern_yield();
            }
        }
    };

    static auto pipe_close_read = [](File* f) -> int {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) {
            return 0;
        }
        PipeWakeList pending_writers{};
        size_t pending_writers_count = 0;
        PipeWakeList pending_write_pollers{};
        size_t pending_write_pollers_count = 0;
        {
            uint64_t const IRQF = st->lock.lock_irqsave();
            st->read_closed = true;
            if (!st->writers_waiting.empty()) {
                pipe_collect_waiters_locked(st->writers_waiting, pending_writers, &pending_writers_count);
            }
            if (!st->write_poll_waiting.empty()) {
                pipe_collect_waiters_locked(st->write_poll_waiting, pending_write_pollers, &pending_write_pollers_count);
            }
            st->lock.unlock_irqrestore(IRQF);
        }
        pipe_reschedule_waiters(pending_writers, pending_writers_count, true);
        pipe_reschedule_waiters(pending_write_pollers, pending_write_pollers_count);
        if (st->open_ends.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            pipe_destroy_state(st);
        }
        return 0;
    };

    static auto pipe_close_write = [](File* f) -> int {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) {
            return 0;
        }
        PipeWakeList pending_readers{};
        size_t pending_readers_count = 0;
        PipeWakeList pending_read_pollers{};
        size_t pending_read_pollers_count = 0;
        {
            uint64_t const IRQF = st->lock.lock_irqsave();
            st->write_closed = true;
            if (!st->readers_waiting.empty()) {
                pipe_collect_waiters_locked(st->readers_waiting, pending_readers, &pending_readers_count);
            }
            if (!st->read_poll_waiting.empty()) {
                pipe_collect_waiters_locked(st->read_poll_waiting, pending_read_pollers, &pending_read_pollers_count);
            }
            st->lock.unlock_irqrestore(IRQF);
        }
        pipe_reschedule_waiters(pending_readers, pending_readers_count);
        pipe_reschedule_waiters(pending_read_pollers, pending_read_pollers_count);
        if (st->open_ends.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            pipe_destroy_state(st);
        }
        return 0;
    };

    static FileOperations pipe_read_fops = {
        .vfs_open = nullptr,
        .vfs_close = pipe_close_read,
        .vfs_read = pipe_read,
        .vfs_write = nullptr,
        .vfs_lseek = nullptr,
        .vfs_isatty = nullptr,
        .vfs_readdir = nullptr,
        .vfs_readlink = nullptr,
        .vfs_truncate = nullptr,
        .vfs_poll_check = [](File* f, int events) -> int {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) {
                return 0;
            }
            int ready = 0;
            uint64_t const IRQF = st->lock.lock_irqsave();
            if ((events & 0x0001) && (st->count > 0 || st->write_closed)) {  // POLLIN
                ready |= 0x0001;
            }
            if (st->write_closed && st->count == 0) {  // POLLHUP
                ready |= 0x0010;
            }
            st->lock.unlock_irqrestore(IRQF);
            return ready;
        },
        .vfs_poll_register_waiter = [](File* f, uint64_t pid) -> bool {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) {
                return false;
            }
            uint64_t const IRQF = st->lock.lock_irqsave();
            bool const OK = pipe_register_poll_waiter(st->read_poll_waiting, pid);
            st->lock.unlock_irqrestore(IRQF);
            return OK;
        },
        .vfs_poll_wait_kind = [](File*) -> ker::mod::sched::task::WaitChannelKind {
            return ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE;
        },
        .vfs_ioctl = nullptr,
    };

    static FileOperations pipe_write_fops = {
        .vfs_open = nullptr,
        .vfs_close = pipe_close_write,
        .vfs_read = nullptr,
        .vfs_write = pipe_write,
        .vfs_lseek = nullptr,
        .vfs_isatty = nullptr,
        .vfs_readdir = nullptr,
        .vfs_readlink = nullptr,
        .vfs_truncate = nullptr,
        .vfs_poll_check = [](File* f, int events) -> int {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) {
                return 0;
            }
            int ready = 0;
            uint64_t const IRQF = st->lock.lock_irqsave();
            if ((events & 0x0004) && (st->count < st->capacity || st->read_closed)) {  // POLLOUT
                ready |= 0x0004;
            }
            if (st->read_closed) {  // POLLERR (broken pipe)
                ready |= 0x0008;
            }
            st->lock.unlock_irqrestore(IRQF);
            return ready;
        },
        .vfs_poll_register_waiter = [](File* f, uint64_t pid) -> bool {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) {
                return false;
            }
            uint64_t const IRQF = st->lock.lock_irqsave();
            bool const OK = pipe_register_poll_waiter(st->write_poll_waiting, pid);
            st->lock.unlock_irqrestore(IRQF);
            return OK;
        },
        .vfs_poll_wait_kind = [](File*) -> ker::mod::sched::task::WaitChannelKind {
            return ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE;
        },
        .vfs_ioctl = nullptr,
    };

    // Expose fops pointers for vfs_is_pipe_file() identity check
    g_pipe_read_fops_ptr = &pipe_read_fops;
    g_pipe_write_fops_ptr = &pipe_write_fops;

    // Create read-end File
#ifdef WOS_SELFTEST
    if (pipe_selftest_should_fail(PipeSelftestFailStage::READ_FILE)) {
        pipe_destroy_unregistered_state(ps);
        return -ENOMEM;
    }
#endif
    auto* rf = new File{};
    if (rf == nullptr) {
        pipe_destroy_unregistered_state(ps);
        return -ENOMEM;
    }
    int const READ_OPEN_FLAGS = (flags & O_NONBLOCK) != 0 ? O_NONBLOCK : 0;
    int const WRITE_OPEN_FLAGS = 1 | ((flags & O_NONBLOCK) != 0 ? O_NONBLOCK : 0);
    pipe_init_file(rf, ps, &pipe_read_fops, READ_OPEN_FLAGS);

    // Create write-end File
#ifdef WOS_SELFTEST
    if (pipe_selftest_should_fail(PipeSelftestFailStage::WRITE_FILE)) {
        delete rf;
        pipe_destroy_unregistered_state(ps);
        return -ENOMEM;
    }
#endif
    auto* wf = new File{};
    if (wf == nullptr) {
        delete rf;
        pipe_destroy_unregistered_state(ps);
        return -ENOMEM;
    }
    pipe_init_file(wf, ps, &pipe_write_fops, WRITE_OPEN_FLAGS);

#ifdef WOS_SELFTEST
    if (pipe_selftest_should_fail(PipeSelftestFailStage::READ_FD)) {
        delete rf;
        delete wf;
        pipe_destroy_unregistered_state(ps);
        return -EMFILE;
    }
#endif
    int const RFD = vfs_alloc_fd(task, rf);
    if (RFD < 0) {
        delete rf;
        delete wf;
        pipe_destroy_unregistered_state(ps);
        return RFD;
    }

#ifdef WOS_SELFTEST
    if (pipe_selftest_should_fail(PipeSelftestFailStage::WRITE_FD)) {
        vfs_release_fd(task, RFD);
        delete rf;
        delete wf;
        pipe_destroy_unregistered_state(ps);
        return -EMFILE;
    }
#endif
    int const WFD = vfs_alloc_fd(task, wf);
    if (WFD < 0) {
        vfs_release_fd(task, RFD);
        delete rf;
        delete wf;
        pipe_destroy_unregistered_state(ps);
        return WFD;
    }

    if ((flags & ker::vfs::O_CLOEXEC) != 0) {
        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        task->set_fd_cloexec(static_cast<unsigned>(RFD));
        task->set_fd_cloexec(static_cast<unsigned>(WFD));
        task->fd_table_lock.unlock_irqrestore(IRQF);
    }

    pipefd[0] = RFD;
    pipefd[1] = WFD;
    pipe_register_state(ps);
    return 0;
}
}  // namespace

auto vfs_pipe(int pipefd[2], int flags) -> int {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    return vfs_pipe_for_task(ker::mod::sched::get_current_task(), pipefd, flags);
}

auto vfs_pipe_reserve_capacity(File* file, size_t capacity) -> bool {
    if (!vfs_is_pipe_file(file) || file->private_data == nullptr || capacity <= PIPE_DEFAULT_CAPACITY) {
        return false;
    }

    auto* st = static_cast<PipeState*>(file->private_data);
    uint64_t irqf = st->lock.lock_irqsave();
    bool const ALREADY_LARGE_ENOUGH = capacity <= st->capacity;
    bool const RESIZE_BLOCKED = st->direct_write_active;
    st->lock.unlock_irqrestore(irqf);
    if (ALREADY_LARGE_ENOUGH) {
        return true;
    }
    if (RESIZE_BLOCKED) {
        return false;
    }

    auto* new_buf = new char[capacity];
    if (new_buf == nullptr) {
        return false;
    }

    char* old_buf = nullptr;
    size_t old_capacity = 0;
    irqf = st->lock.lock_irqsave();
    old_capacity = st->capacity;
    old_buf = pipe_install_buffer_locked(st, new_buf, capacity);
    st->lock.unlock_irqrestore(irqf);

    if (old_buf == nullptr) {
        delete[] new_buf;
        return false;
    }

    pipe_note_capacity_delta(capacity, old_capacity);
    delete[] old_buf;
    return true;
}

void vfs_get_local_pipe_perf_snapshot(LocalPipePerfSnapshot& out) {
    LocalPipePerfSnapshot snapshot{
        .created_since_reset = g_pipe_created_since_reset.load(std::memory_order_acquire),
        .peak_pipes = g_pipe_peak_count.load(std::memory_order_acquire),
        .peak_capacity_bytes = g_pipe_peak_capacity_bytes.load(std::memory_order_acquire),
    };

    uint64_t const STATES_IRQF = g_pipe_states_lock.lock_irqsave();
    for (auto* st : g_pipe_states) {
        if (st == nullptr) {
            continue;
        }

        uint64_t const PIPE_IRQF = st->lock.lock_irqsave();
        snapshot.active_pipes++;
        snapshot.capacity_bytes += st->capacity;
        snapshot.buffered_bytes += st->count;
        snapshot.reader_waiters += st->readers_waiting.size();
        snapshot.writer_waiters += st->writers_waiting.size();
        snapshot.poll_waiters += st->read_poll_waiting.size() + st->write_poll_waiting.size();
        if (st->direct_write_active) {
            snapshot.direct_writes++;
        }
        if (st->read_closed) {
            snapshot.read_closed++;
        }
        if (st->write_closed) {
            snapshot.write_closed++;
        }
        st->lock.unlock_irqrestore(PIPE_IRQF);
    }
    g_pipe_states_lock.unlock_irqrestore(STATES_IRQF);

    snapshot.approx_alloc_bytes = (snapshot.active_pipes * sizeof(PipeState)) + snapshot.capacity_bytes;
    out = snapshot;
}

auto vfs_generate_local_pipe_diag(char* buf, size_t bufsz) -> size_t {
    if (buf == nullptr || bufsz == 0) {
        return 0;
    }

    size_t len = 0;
    bool output_truncated = false;

    struct PipeOwnerRecord {
        PipeState* state{};
        uint64_t pid{};
        uint64_t fd{};
        uint64_t file_refs{};
        int flags{};
        bool cloexec{};
        bool write_end{};
    };

    constexpr size_t MAX_OWNER_RECORDS = 2048;
    auto* owners = new PipeOwnerRecord[MAX_OWNER_RECORDS];
    if (owners == nullptr) {
        pipe_diag_append(buf, bufsz, len, output_truncated, "%s", "error=ENOMEM\n");
        return len;
    }

    size_t owner_count = 0;
    bool owner_truncated = false;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }

        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        task->fd_table.for_each([&](uint64_t fd, void* value) -> void {
            if (value == nullptr) {
                return;
            }
            auto* file = static_cast<File*>(value);
            bool const IS_READ_END = g_pipe_read_fops_ptr != nullptr && file->fops == g_pipe_read_fops_ptr;
            bool const IS_WRITE_END = g_pipe_write_fops_ptr != nullptr && file->fops == g_pipe_write_fops_ptr;
            if (!IS_READ_END && !IS_WRITE_END) {
                return;
            }
            if (owner_count >= MAX_OWNER_RECORDS) {
                owner_truncated = true;
                return;
            }
            owners[owner_count++] = PipeOwnerRecord{
                .state = static_cast<PipeState*>(file->private_data),
                .pid = task->pid,
                .fd = fd,
                .file_refs = static_cast<uint64_t>(file->refcount.load(std::memory_order_relaxed)),
                .flags = file->open_flags,
                .cloexec = task->get_fd_cloexec(static_cast<unsigned>(fd)),
                .write_end = IS_WRITE_END,
            };
        });
        task->fd_table_lock.unlock_irqrestore(IRQF);
        task->release();
    }

    pipe_diag_append(buf, bufsz, len, output_truncated, "owner_records=%llu owner_truncated=%u\n",
                     static_cast<unsigned long long>(owner_count), owner_truncated ? 1U : 0U);

    uint64_t const STATES_IRQF = g_pipe_states_lock.lock_irqsave();
    for (auto* st : g_pipe_states) {
        if (st == nullptr) {
            continue;
        }

        constexpr size_t MAX_DIAG_WAITERS = 16;
        size_t read_fds = 0;
        size_t write_fds = 0;
        for (size_t i = 0; i < owner_count; ++i) {
            if (owners[i].state != st) {
                continue;
            }
            if (owners[i].write_end) {
                write_fds++;
            } else {
                read_fds++;
            }
        }

        size_t capacity = 0;
        size_t buffered = 0;
        size_t reader_waiters = 0;
        size_t writer_waiters = 0;
        size_t poll_waiters = 0;
        size_t read_poll_waiters = 0;
        size_t write_poll_waiters = 0;
        bool read_closed = false;
        bool write_closed = false;
        bool direct_write = false;
        int open_ends = 0;
        std::array<uint64_t, MAX_DIAG_WAITERS> reader_waiter_pids{};
        std::array<uint64_t, MAX_DIAG_WAITERS> writer_waiter_pids{};
        std::array<uint64_t, MAX_DIAG_WAITERS> read_poll_waiter_pids{};
        std::array<uint64_t, MAX_DIAG_WAITERS> write_poll_waiter_pids{};
        size_t reader_waiter_diag_count = 0;
        size_t writer_waiter_diag_count = 0;
        size_t read_poll_waiter_diag_count = 0;
        size_t write_poll_waiter_diag_count = 0;
        uint64_t const PIPE_IRQF = st->lock.lock_irqsave();
        capacity = st->capacity;
        buffered = st->count;
        reader_waiters = st->readers_waiting.size();
        writer_waiters = st->writers_waiting.size();
        read_poll_waiters = st->read_poll_waiting.size();
        write_poll_waiters = st->write_poll_waiting.size();
        poll_waiters = read_poll_waiters + write_poll_waiters;
        read_closed = st->read_closed;
        write_closed = st->write_closed;
        direct_write = st->direct_write_active;
        open_ends = st->open_ends.load(std::memory_order_relaxed);
        reader_waiter_diag_count = std::min(reader_waiters, MAX_DIAG_WAITERS);
        writer_waiter_diag_count = std::min(writer_waiters, MAX_DIAG_WAITERS);
        read_poll_waiter_diag_count = std::min(read_poll_waiters, MAX_DIAG_WAITERS);
        write_poll_waiter_diag_count = std::min(write_poll_waiters, MAX_DIAG_WAITERS);
        for (size_t i = 0; i < reader_waiter_diag_count; ++i) {
            reader_waiter_pids[i] = st->readers_waiting.at(i);
        }
        for (size_t i = 0; i < writer_waiter_diag_count; ++i) {
            writer_waiter_pids[i] = st->writers_waiting.at(i);
        }
        for (size_t i = 0; i < read_poll_waiter_diag_count; ++i) {
            read_poll_waiter_pids[i] = st->read_poll_waiting.at(i);
        }
        for (size_t i = 0; i < write_poll_waiter_diag_count; ++i) {
            write_poll_waiter_pids[i] = st->write_poll_waiting.at(i);
        }
        st->lock.unlock_irqrestore(PIPE_IRQF);

        pipe_diag_append(buf, bufsz, len, output_truncated,
                         "pipe=%p open_ends=%d capacity=%llu buffered=%llu read_closed=%u write_closed=%u direct=%u reader_waiters=%llu "
                         "writer_waiters=%llu poll_waiters=%llu read_fds=%llu write_fds=%llu\n",
                         static_cast<void*>(st), open_ends, static_cast<unsigned long long>(capacity),
                         static_cast<unsigned long long>(buffered), read_closed ? 1U : 0U, write_closed ? 1U : 0U, direct_write ? 1U : 0U,
                         static_cast<unsigned long long>(reader_waiters), static_cast<unsigned long long>(writer_waiters),
                         static_cast<unsigned long long>(poll_waiters), static_cast<unsigned long long>(read_fds),
                         static_cast<unsigned long long>(write_fds));

        auto append_waiters = [&](const char* label, const std::array<uint64_t, MAX_DIAG_WAITERS>& pids, size_t shown, size_t total) {
            if (total == 0) {
                return;
            }
            pipe_diag_append(buf, bufsz, len, output_truncated, " waiters %s", label);
            for (size_t i = 0; i < shown; ++i) {
                pipe_diag_append(buf, bufsz, len, output_truncated, " %llu", static_cast<unsigned long long>(pids[i]));
            }
            if (shown < total) {
                pipe_diag_append(buf, bufsz, len, output_truncated, " ...(+%llu)", static_cast<unsigned long long>(total - shown));
            }
            pipe_diag_append(buf, bufsz, len, output_truncated, "%s", "\n");
        };

        append_waiters("read", reader_waiter_pids, reader_waiter_diag_count, reader_waiters);
        append_waiters("write", writer_waiter_pids, writer_waiter_diag_count, writer_waiters);
        append_waiters("read_poll", read_poll_waiter_pids, read_poll_waiter_diag_count, read_poll_waiters);
        append_waiters("write_poll", write_poll_waiter_pids, write_poll_waiter_diag_count, write_poll_waiters);

        for (size_t i = 0; i < owner_count; ++i) {
            if (owners[i].state != st) {
                continue;
            }
            pipe_diag_append(buf, bufsz, len, output_truncated, " owner pid=%llu fd=%llu kind=%s cloexec=%u flags=0x%x file_refs=%llu\n",
                             static_cast<unsigned long long>(owners[i].pid), static_cast<unsigned long long>(owners[i].fd),
                             owners[i].write_end ? "write" : "read", owners[i].cloexec ? 1U : 0U, owners[i].flags,
                             static_cast<unsigned long long>(owners[i].file_refs));
        }
    }
    g_pipe_states_lock.unlock_irqrestore(STATES_IRQF);

    delete[] owners;

    if (output_truncated && len < bufsz - 1) {
        pipe_diag_append(buf, bufsz, len, output_truncated, "%s", "output_truncated=1\n");
    }
    return len;
}

void vfs_reset_local_pipe_perf_counters() {
    uint64_t active_pipes = 0;
    uint64_t active_capacity = 0;

    uint64_t const STATES_IRQF = g_pipe_states_lock.lock_irqsave();
    for (auto* st : g_pipe_states) {
        if (st == nullptr) {
            continue;
        }
        uint64_t const PIPE_IRQF = st->lock.lock_irqsave();
        active_pipes++;
        active_capacity += st->capacity;
        st->lock.unlock_irqrestore(PIPE_IRQF);
    }
    g_pipe_states_lock.unlock_irqrestore(STATES_IRQF);

    g_pipe_created_since_reset.store(0, std::memory_order_release);
    g_pipe_peak_count.store(active_pipes, std::memory_order_release);
    g_pipe_peak_capacity_bytes.store(active_capacity, std::memory_order_release);
}

#ifdef WOS_SELFTEST
auto pipe_snapshot_unchanged(const LocalPipePerfSnapshot& before, const LocalPipePerfSnapshot& after) -> bool {
    return before.active_pipes == after.active_pipes && before.created_since_reset == after.created_since_reset &&
           before.capacity_bytes == after.capacity_bytes && before.buffered_bytes == after.buffered_bytes &&
           before.approx_alloc_bytes == after.approx_alloc_bytes;
}

auto pipe_close_fake_task_fd(ker::mod::sched::task::Task& task, int fd) -> bool {
    auto* file = static_cast<File*>(task.fd_table.lookup(static_cast<uint64_t>(fd)));
    if (file == nullptr) {
        return false;
    }
    if (vfs_release_fd(&task, fd) < 0) {
        return false;
    }
    vfs_put_file(file);
    return true;
}

auto vfs_selftest_pipe_failure_unwinds() -> bool {
    constexpr std::array<PipeSelftestFailStage, 6> FAIL_STAGES{
        PipeSelftestFailStage::BUFFER,     PipeSelftestFailStage::STATE,   PipeSelftestFailStage::READ_FILE,
        PipeSelftestFailStage::WRITE_FILE, PipeSelftestFailStage::READ_FD, PipeSelftestFailStage::WRITE_FD,
    };

    bool ok = true;
    for (PipeSelftestFailStage stage : FAIL_STAGES) {
        ker::mod::sched::task::Task task{};
        std::array<int, 2> pipefd{-1, -1};
        LocalPipePerfSnapshot before{};
        LocalPipePerfSnapshot after{};
        vfs_get_local_pipe_perf_snapshot(before);

        g_vfs_selftest_pipe_fail_stage.store(static_cast<int>(stage), std::memory_order_relaxed);
        int const RET = vfs_pipe_for_task(&task, pipefd.data());
        g_vfs_selftest_pipe_fail_stage.store(static_cast<int>(PipeSelftestFailStage::NONE), std::memory_order_relaxed);

        vfs_get_local_pipe_perf_snapshot(after);
        bool const EXPECTED_ERRNO =
            (stage == PipeSelftestFailStage::READ_FD || stage == PipeSelftestFailStage::WRITE_FD) ? (RET == -EMFILE) : (RET == -ENOMEM);
        ok = ok && EXPECTED_ERRNO && pipefd[0] == -1 && pipefd[1] == -1 && task.fd_table.empty() && pipe_snapshot_unchanged(before, after);
    }

    ker::mod::sched::task::Task task{};
    std::array<int, 2> pipefd{-1, -1};
    LocalPipePerfSnapshot before_success{};
    LocalPipePerfSnapshot active_success{};
    LocalPipePerfSnapshot after_success_cleanup{};
    vfs_get_local_pipe_perf_snapshot(before_success);
    int const RET = vfs_pipe_for_task(&task, pipefd.data());
    vfs_get_local_pipe_perf_snapshot(active_success);
    ok = ok && RET == 0 && pipefd[0] >= 0 && pipefd[1] >= 0 && task.fd_table.size() == 2 &&
         active_success.active_pipes == before_success.active_pipes + 1 &&
         active_success.capacity_bytes == before_success.capacity_bytes + PIPE_DEFAULT_CAPACITY;

    bool const CLOSED_READ = pipefd[0] >= 0 && pipe_close_fake_task_fd(task, pipefd[0]);
    bool const CLOSED_WRITE = pipefd[1] >= 0 && pipe_close_fake_task_fd(task, pipefd[1]);
    vfs_get_local_pipe_perf_snapshot(after_success_cleanup);
    ok = ok && CLOSED_READ && CLOSED_WRITE && task.fd_table.empty() && after_success_cleanup.active_pipes == before_success.active_pipes &&
         after_success_cleanup.capacity_bytes == before_success.capacity_bytes;

    return ok;
}

auto vfs_selftest_pipe_flags() -> bool {
    ker::mod::sched::task::Task task{};
    std::array<int, 2> pipefd{-1, -1};

    bool ok = vfs_pipe_for_task(&task, pipefd.data(), 2) == -EINVAL && pipefd[0] == -1 && pipefd[1] == -1 && task.fd_table.empty();

    LocalPipePerfSnapshot before{};
    LocalPipePerfSnapshot after{};
    vfs_get_local_pipe_perf_snapshot(before);
    int const RET = vfs_pipe_for_task(&task, pipefd.data(), ker::vfs::O_CLOEXEC | O_NONBLOCK);
    vfs_get_local_pipe_perf_snapshot(after);

    auto* read_file = static_cast<File*>(task.fd_table.lookup(static_cast<uint64_t>(pipefd[0])));
    auto* write_file = static_cast<File*>(task.fd_table.lookup(static_cast<uint64_t>(pipefd[1])));
    ok = ok && RET == 0 && pipefd[0] >= 0 && pipefd[1] >= 0 && read_file != nullptr && write_file != nullptr &&
         task.get_fd_cloexec(static_cast<unsigned>(pipefd[0])) && task.get_fd_cloexec(static_cast<unsigned>(pipefd[1])) &&
         (read_file->open_flags & O_NONBLOCK) != 0 && (write_file->open_flags & O_NONBLOCK) != 0 &&
         (read_file->open_flags & ker::vfs::O_CLOEXEC) == 0 && (write_file->open_flags & ker::vfs::O_CLOEXEC) == 0 &&
         after.active_pipes == before.active_pipes + 1 && after.capacity_bytes == before.capacity_bytes + PIPE_DEFAULT_CAPACITY;
    char dummy = 0;
    ok = ok && read_file->fops->vfs_read(read_file, &dummy, 0, 0) == 0;
    ok = ok && write_file->fops->vfs_write(write_file, &dummy, 0, 0) == 0;

    bool const CLOSED_READ = pipefd[0] >= 0 && pipe_close_fake_task_fd(task, pipefd[0]);
    bool const CLOSED_WRITE = pipefd[1] >= 0 && pipe_close_fake_task_fd(task, pipefd[1]);
    LocalPipePerfSnapshot cleanup{};
    vfs_get_local_pipe_perf_snapshot(cleanup);
    return ok && CLOSED_READ && CLOSED_WRITE && task.fd_table.empty() && cleanup.active_pipes == before.active_pipes &&
           cleanup.capacity_bytes == before.capacity_bytes;
}

auto vfs_selftest_anonymous_fstat_snapshot_hits() -> bool {
    ker::mod::sched::task::Task task{};
    std::array<int, 2> pipefd{-1, -1};
    if (vfs_pipe_for_task(&task, pipefd.data()) != 0) {
        return false;
    }

    auto* read_file = static_cast<File*>(task.fd_table.lookup(static_cast<uint64_t>(pipefd[0])));
    bool ok = read_file != nullptr && read_file->vfs_path == nullptr;

    VfsCachePerfSnapshot before{};
    VfsCachePerfSnapshot after_first{};
    VfsCachePerfSnapshot after_second{};
    Stat st{};
    vfs_get_cache_perf_snapshot(before);

    ok = ok && vfs_fstat_file(read_file, &st) == 0 && (st.st_mode & static_cast<mode_t>(S_IFMT)) == static_cast<mode_t>(S_IFIFO);
    vfs_get_cache_perf_snapshot(after_first);

    ok = ok && vfs_fstat_file(read_file, &st) == 0;
    vfs_get_cache_perf_snapshot(after_second);

    ok = ok && after_first.fstat_snapshot_stores > before.fstat_snapshot_stores &&
         after_second.fstat_snapshot_hits > after_first.fstat_snapshot_hits;

    auto* devfs_file = ker::vfs::devfs::devfs_open_path("/dev", 0, 0);
    ok = ok && devfs_file != nullptr && devfs_file->vfs_path == nullptr;
    if (devfs_file != nullptr) {
        VfsCachePerfSnapshot before_devfs{};
        VfsCachePerfSnapshot after_devfs_first{};
        VfsCachePerfSnapshot after_devfs_second{};
        vfs_get_cache_perf_snapshot(before_devfs);

        ok = ok && vfs_fstat_file(devfs_file, &st) == 0 && (st.st_mode & static_cast<mode_t>(S_IFMT)) == static_cast<mode_t>(S_IFDIR);
        vfs_get_cache_perf_snapshot(after_devfs_first);

        ok = ok && vfs_fstat_file(devfs_file, &st) == 0;
        vfs_get_cache_perf_snapshot(after_devfs_second);

        ok = ok && after_devfs_first.fstat_snapshot_stores > before_devfs.fstat_snapshot_stores &&
             after_devfs_second.fstat_snapshot_hits > after_devfs_first.fstat_snapshot_hits;
        vfs_put_file(devfs_file);
    }

    bool const CLOSED_READ = pipefd[0] >= 0 && pipe_close_fake_task_fd(task, pipefd[0]);
    bool const CLOSED_WRITE = pipefd[1] >= 0 && pipe_close_fake_task_fd(task, pipefd[1]);
    return ok && CLOSED_READ && CLOSED_WRITE && task.fd_table.empty();
}

auto vfs_selftest_remote_fstat_snapshot_cacheable() -> bool {
    constexpr const char PATH[] = "/remote/ktest-fstat-snapshot";

    File file{};
    file.fs_type = FSType::REMOTE;
    file.vfs_path = PATH;

    Stat st{};
    st.st_mode = S_IFREG | 0644;
    st.st_size = 12345;
    st.st_dev = 7;

    VfsCachePerfSnapshot before{};
    VfsCachePerfSnapshot after_store{};
    VfsCachePerfSnapshot after_lookup{};
    vfs_get_cache_perf_snapshot(before);

    file_stat_snapshot_store(&file, st, metadata_snapshot_stamp());
    vfs_get_cache_perf_snapshot(after_store);

    Stat out{};
    bool const HIT = file_stat_snapshot_lookup(&file, &out);
    vfs_get_cache_perf_snapshot(after_lookup);

    return HIT && out.st_size == st.st_size && out.st_dev == st.st_dev && (out.st_mode & S_IFMT) == S_IFREG &&
           after_store.fstat_snapshot_stores > before.fstat_snapshot_stores &&
           after_lookup.fstat_snapshot_hits > after_store.fstat_snapshot_hits &&
           after_lookup.fstat_snapshot_miss_uncacheable == before.fstat_snapshot_miss_uncacheable;
}

auto vfs_selftest_file_path_storage() -> bool {
    File file{};
    constexpr const char SHORT_PATH[] = "/tmp/short-vfs-path";

    bool ok = vfs_file_set_path(&file, SHORT_PATH);
    ok = ok && file.vfs_path == file.vfs_path_inline.data() && !file.vfs_path_heap_allocated && std::strcmp(file.vfs_path, SHORT_PATH) == 0;

    vfs_file_clear_path(&file);
    ok = ok && file.vfs_path == nullptr && !file.vfs_path_heap_allocated && file.vfs_path_inline.at(0) == '\0';

    std::array<char, File::INLINE_VFS_PATH_CAPACITY + 8> long_path{};
    long_path.at(0) = '/';
    for (size_t i = 1; i + 1 < long_path.size(); ++i) {
        long_path.at(i) = 'a';
    }
    long_path.back() = '\0';

    ok = ok && vfs_file_set_path(&file, long_path.data());
    ok = ok && file.vfs_path != nullptr && file.vfs_path != file.vfs_path_inline.data() && file.vfs_path_heap_allocated &&
         std::strcmp(file.vfs_path, long_path.data()) == 0;

    vfs_file_clear_path(&file);

    constexpr const char LITERAL_PATH[] = "/tmp/non-owned-literal";
    file.vfs_path = LITERAL_PATH;
    file.vfs_path_heap_allocated = false;
    vfs_file_clear_path(&file);

    return ok && file.vfs_path == nullptr && !file.vfs_path_heap_allocated;
}

auto vfs_selftest_file_data_write_invalidates_path_stat() -> bool {
    vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_file_data_stat_invalidation";
    constexpr char FIRST[] = "abc";
    constexpr char SECOND[] = "defgh";
    vfs_unlink(PATH);

    auto* file = vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_write == nullptr) {
        return false;
    }

    bool ok = file->fops->vfs_write(file, FIRST, sizeof(FIRST) - 1, 0) == static_cast<ssize_t>(sizeof(FIRST) - 1);
    cache_notify_file_data_changed_impl(file);

    Stat st{};
    ok = ok && vfs_stat(PATH, &st) == 0 && st.st_size == static_cast<off_t>(sizeof(FIRST) - 1);
    ok = ok && vfs_stat(PATH, &st) == 0 && st.st_size == static_cast<off_t>(sizeof(FIRST) - 1);

    constexpr size_t SECOND_OFFSET = sizeof(FIRST) - 1;
    ok = ok && file->fops->vfs_write(file, SECOND, sizeof(SECOND) - 1, SECOND_OFFSET) == static_cast<ssize_t>(sizeof(SECOND) - 1);
    cache_notify_file_data_changed_impl(file);

    ok = ok && vfs_stat(PATH, &st) == 0 && st.st_size == static_cast<off_t>((sizeof(FIRST) - 1) + (sizeof(SECOND) - 1));

    vfs_put_file(file);
    ok = ok && vfs_unlink(PATH) == 0;
    return ok;
}

auto vfs_selftest_file_data_write_skips_uncached_path_invalidation() -> bool {
    vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_file_data_uncached_path_invalidation";
    constexpr const char* UNRELATED = "/tmp/ktest_file_data_unrelated_missing_observation";
    vfs_unlink(PATH);

    auto* file = vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    if (file == nullptr) {
        return false;
    }

    VfsCachePerfSnapshot before_unrelated{};
    VfsCachePerfSnapshot after_unrelated{};
    VfsCachePerfSnapshot after_uncached_write{};
    Stat st{};
    vfs_get_cache_perf_snapshot(before_unrelated);
    metadata_cache_store(UNRELATED, FSType::TMPFS, 0, true, false, -ENOENT, nullptr, metadata_snapshot_stamp());
    vfs_get_cache_perf_snapshot(after_unrelated);
    bool const STORED_UNRELATED = after_unrelated.metadata_stores > before_unrelated.metadata_stores;
    bool const HAD_PATH_OBSERVATION_BEFORE_STAT = metadata_cache_has_file_data_observation(file);
    bool ok = STORED_UNRELATED && !HAD_PATH_OBSERVATION_BEFORE_STAT;

    metadata_cache_note_file_data_changed(file);
    vfs_get_cache_perf_snapshot(after_uncached_write);
    bool const SKIPPED_UNOBSERVED_INVALIDATION =
        after_uncached_write.metadata_path_invalidations == after_unrelated.metadata_path_invalidations;
    ok = ok && SKIPPED_UNOBSERVED_INVALIDATION;

    VfsCachePerfSnapshot after_first_stat{};
    VfsCachePerfSnapshot after_second_stat{};
    VfsCachePerfSnapshot after_observed_write{};
    VfsCachePerfSnapshot after_refill_stat{};
    int const STAT_RET = vfs_stat(PATH, &st);
    ok = ok && STAT_RET == 0;
    vfs_get_cache_perf_snapshot(after_first_stat);

    bool const HAD_PATH_OBSERVATION_AFTER_STAT = metadata_cache_has_file_data_observation(file);
    ok = ok && HAD_PATH_OBSERVATION_AFTER_STAT;
    ok = ok && vfs_stat(PATH, &st) == 0;
    vfs_get_cache_perf_snapshot(after_second_stat);
    bool const CACHED_OBSERVED_PATH = after_second_stat.metadata_hits > after_first_stat.metadata_hits;
    ok = ok && CACHED_OBSERVED_PATH;

    metadata_cache_note_file_data_changed(file);
    vfs_get_cache_perf_snapshot(after_observed_write);
    ok = ok && vfs_stat(PATH, &st) == 0;
    vfs_get_cache_perf_snapshot(after_refill_stat);
    bool const REFILLED_OBSERVED_PATH = after_refill_stat.metadata_misses > after_observed_write.metadata_misses &&
                                        after_refill_stat.metadata_stores > after_observed_write.metadata_stores;
    ok = ok && REFILLED_OBSERVED_PATH;

    vfs_put_file(file);
    ok = ok && vfs_unlink(PATH) == 0;
    return ok;
}

auto vfs_selftest_file_metadata_change_invalidates_path_stat() -> bool {
    vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_file_metadata_stat_invalidation";
    vfs_unlink(PATH);

    auto* file = vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    if (file == nullptr || file->fs_type != FSType::TMPFS || file->private_data == nullptr) {
        return false;
    }

    Stat st{};
    bool ok = vfs_stat(PATH, &st) == 0 && (st.st_mode & 07777) == 0644;
    ok = ok && vfs_stat(PATH, &st) == 0 && (st.st_mode & 07777) == 0644;

    auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(file->private_data);
    node->mode = 0755;

    VfsCachePerfSnapshot before{};
    VfsCachePerfSnapshot after{};
    vfs_get_cache_perf_snapshot(before);
    cache_notify_file_metadata_changed_impl(file);
    vfs_get_cache_perf_snapshot(after);

    ok = ok && after.stream_invalidate_empty_skips == before.stream_invalidate_empty_skips;
    ok = ok && vfs_stat(PATH, &st) == 0 && (st.st_mode & 07777) == 0755;

    vfs_put_file(file);
    ok = ok && vfs_unlink(PATH) == 0;
    return ok;
}

auto vfs_selftest_open_create_metadata_hint() -> bool {
    File unknown_backend{};
    File existing_file{};
    existing_file.open_create_result_known = true;

    File created_file{};
    created_file.open_create_result_known = true;
    created_file.created_by_open = true;

    return !open_create_should_invalidate_metadata(&created_file, 0) &&
           open_create_should_invalidate_metadata(nullptr, ker::vfs::O_CREAT) &&
           open_create_should_invalidate_metadata(&unknown_backend, ker::vfs::O_CREAT) &&
           !open_create_should_invalidate_metadata(&existing_file, ker::vfs::O_CREAT) &&
           open_create_should_invalidate_metadata(&created_file, ker::vfs::O_CREAT);
}

auto vfs_selftest_metadata_cache_rejects_stale_negative_store() -> bool {
    constexpr const char* PATH = "/tmp/ktest_metadata_stale_negative_store";
    Stat st{};

    MetadataSnapshotStamp const STALE_STAMP = metadata_snapshot_stamp();
    metadata_cache_note_path_changed(PATH, nullptr);
    metadata_cache_store(PATH, FSType::TMPFS, 0, true, false, -ENOENT, nullptr, STALE_STAMP);

    return metadata_cache_lookup(PATH, FSType::TMPFS, 0, true, false, &st) == -EAGAIN;
}

auto vfs_selftest_fcntl_setfl_preserves_open_policy_flags() -> bool {
    constexpr int IMMUTABLE_FLAGS = 2 | ker::vfs::O_CREAT | ker::vfs::O_TRUNC | ker::vfs::O_DIRECTORY | ker::vfs::O_CLOEXEC |
                                    ker::vfs::O_NOTIFY_CACHE_CHANGE | ker::vfs::O_NO_CACHE | ker::vfs::O_LOCAL | ker::vfs::O_ALWAYS_CACHE;
    int const NONBLOCK_ONLY = fcntl_setfl_flags(IMMUTABLE_FLAGS | ker::vfs::O_APPEND, O_NONBLOCK);
    bool ok = (NONBLOCK_ONLY & IMMUTABLE_FLAGS) == IMMUTABLE_FLAGS && (NONBLOCK_ONLY & O_NONBLOCK) != 0 &&
              (NONBLOCK_ONLY & ker::vfs::O_APPEND) == 0;

    int const APPEND_ONLY = fcntl_setfl_flags(NONBLOCK_ONLY, ker::vfs::O_APPEND);
    ok = ok && (APPEND_ONLY & IMMUTABLE_FLAGS) == IMMUTABLE_FLAGS && (APPEND_ONLY & ker::vfs::O_APPEND) != 0 &&
         (APPEND_ONLY & O_NONBLOCK) == 0;

    int const STATUS_CLEARED = fcntl_setfl_flags(APPEND_ONLY, 0);
    return ok && (STATUS_CLEARED & IMMUTABLE_FLAGS) == IMMUTABLE_FLAGS && (STATUS_CLEARED & ker::vfs::O_APPEND) == 0 &&
           (STATUS_CLEARED & O_NONBLOCK) == 0;
}

auto vfs_selftest_stream_cache_read_eligibility() -> bool {
    char path[] = "/tmp/ktest_stream_cache_read_eligibility";

    File xfs_read{};
    xfs_read.fs_type = FSType::XFS;
    xfs_read.vfs_path = path;

    File xfs_write{};
    xfs_write.fs_type = FSType::XFS;
    xfs_write.vfs_path = path;
    xfs_write.open_flags = 1;

    File xfs_no_cache{};
    xfs_no_cache.fs_type = FSType::XFS;
    xfs_no_cache.vfs_path = path;
    xfs_no_cache.open_flags = ker::vfs::O_NO_CACHE;

    File anonymous_pipe{};
    anonymous_pipe.fs_type = FSType::TMPFS;

    File devfs_read{};
    devfs_read.fs_type = FSType::DEVFS;
    devfs_read.vfs_path = path;

    File remote_read{};
    remote_read.fs_type = FSType::REMOTE;
    remote_read.vfs_path = path;

    bool const LOCAL_REGULAR_REJECTED = !stream_cache_read_eligible(&xfs_read);
    bool const WRITABLE_REJECTED = !stream_cache_read_eligible(&xfs_write);
    bool const NO_CACHE_REJECTED = !stream_cache_read_eligible(&xfs_no_cache);
    bool const ANONYMOUS_REJECTED = !stream_cache_read_eligible(&anonymous_pipe);
    bool const DEVFS_REJECTED = !stream_cache_read_eligible(&devfs_read);
    bool const REMOTE_ALLOWED = stream_cache_read_eligible(&remote_read);
    return LOCAL_REGULAR_REJECTED && WRITABLE_REJECTED && NO_CACHE_REJECTED && ANONYMOUS_REJECTED && DEVFS_REJECTED && REMOTE_ALLOWED;
}

auto vfs_selftest_stream_cache_local_detached_ttl() -> bool {
    StreamCacheEntry local{};
    local.identity.fs_type = FSType::XFS;
    local.retain_full_file = true;
    local.freshness.size = static_cast<off_t>(STREAM_LOCAL_SMALL_FILE_RETAIN_MAX);

    StreamCacheEntry local_too_large{};
    local_too_large.identity.fs_type = FSType::XFS;
    local_too_large.retain_full_file = true;
    local_too_large.freshness.size = static_cast<off_t>(STREAM_LOCAL_SMALL_FILE_RETAIN_MAX + 1);

    StreamCacheEntry remote{};
    remote.identity.fs_type = FSType::REMOTE;
    remote.retain_full_file = true;
    remote.freshness.size = static_cast<off_t>(STREAM_LOCAL_SMALL_FILE_RETAIN_MAX);

    return stream_detached_ttl_us(&local) == STREAM_LOCAL_SMALL_FILE_DETACHED_TTL_US &&
           stream_detached_ttl_us(&local_too_large) == STREAM_DETACHED_TTL_US && stream_detached_ttl_us(&remote) == STREAM_DETACHED_TTL_US;
}
#endif

namespace {

auto resolve_mount_source_path(const char* source, char* out, size_t outsize) -> int {
    if (source == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> abs_source{};
    int const RAW_RET = resolve_task_path_raw(source, abs_source.data(), abs_source.size());
    if (RAW_RET < 0) {
        return RAW_RET;
    }

    std::array<char, MAX_PATH_LEN> resolved_source{};
    int const SYMLINK_RET = resolve_symlinks(abs_source.data(), resolved_source.data(), resolved_source.size(), true);
    if (SYMLINK_RET < 0) {
        return SYMLINK_RET;
    }

    return strip_current_task_root_prefix(resolved_source.data(), out, outsize);
}

enum class BlockFsProbe : uint8_t {
    UNKNOWN,
    FAT32,
    XFS,
};

auto probe_block_sector_fstype(ker::dev::BlockDevice* bdev, uint64_t lba) -> BlockFsProbe {
    if (bdev == nullptr || bdev->block_size < 512) {
        return BlockFsProbe::UNKNOWN;
    }

    auto* sector = new uint8_t[bdev->block_size];
    if (sector == nullptr) {
        return BlockFsProbe::UNKNOWN;
    }

    int const READ_RC = ker::dev::block_read(bdev, lba, 1, sector);
    if (READ_RC != 0) {
        delete[] sector;
        return BlockFsProbe::UNKNOWN;
    }

    BlockFsProbe result = BlockFsProbe::UNKNOWN;
    if (ker::dev::gpt::sector_looks_like_xfs_superblock(sector, bdev->block_size)) {
        result = BlockFsProbe::XFS;
    } else if (ker::dev::gpt::sector_looks_like_fat32_boot(sector, bdev->block_size)) {
        result = BlockFsProbe::FAT32;
    }

    delete[] sector;
    return result;
}

auto block_fs_probe_name(BlockFsProbe probe) -> const char* {
    switch (probe) {
        case BlockFsProbe::FAT32:
            return "fat32";
        case BlockFsProbe::XFS:
            return "xfs";
        case BlockFsProbe::UNKNOWN:
            break;
    }
    return nullptr;
}

auto probe_block_device_fstype(ker::dev::BlockDevice* bdev) -> const char* {
    BlockFsProbe const DIRECT_PROBE = probe_block_sector_fstype(bdev, 0);
    if (DIRECT_PROBE != BlockFsProbe::UNKNOWN) {
        return block_fs_probe_name(DIRECT_PROBE);
    }

    if (bdev == nullptr || bdev->is_partition) {
        return nullptr;
    }

    if (ker::dev::gpt::gpt_find_fat32_partition(bdev) != 0) {
        return "fat32";
    }

    return nullptr;
}

}  // namespace

auto vfs_mount(const char* source, const char* target, const char* fstype, unsigned long flags, const char* data) -> int {
    if (ker::mod::power::shutdown_in_progress()) {
        return -ESHUTDOWN;
    }

    if (target == nullptr) {
        return -EINVAL;
    }

    // Default fstype to "fat32" when not specified (auto-detect for block devices)
    const char* effective_fstype = fstype;
    if (effective_fstype == nullptr || effective_fstype[0] == '\0') {
        effective_fstype = "fat32";
    }

    ker::dev::BlockDevice* bdev = nullptr;

    if (source != nullptr) {
        // V2: Handle wki://<hostname>/<export> URIs
        if (source[0] == 'w' && source[1] == 'k' && source[2] == 'i' && source[3] == ':' && source[4] == '/' && source[5] == '/') {
            const char* host_start = source + 6;
            const char* slash = host_start;
            while (*slash != '\0' && *slash != '/') {
                slash++;
            }
            auto const HOST_LEN = static_cast<size_t>(slash - host_start);
            if (HOST_LEN == 0 || HOST_LEN >= 64) {
                return -EINVAL;
            }

            char hostname[64] = {};  // NOLINT(modernize-avoid-c-arrays)
            std::memcpy(hostname, host_start, HOST_LEN);
            hostname[HOST_LEN] = '\0';

            const char* export_name = (*slash == '/') ? slash + 1 : "";
            if (export_name[0] == '\0') {
                return -EINVAL;
            }

            // Resolve hostname to node_id
            uint16_t const NODE_ID = ker::net::wki::wki_peer_find_by_hostname(hostname);
            if (NODE_ID == 0) {
                return -ENODEV;
            }
            auto* peer = ker::net::wki::wki_peer_find(NODE_ID);
            if (peer == nullptr || peer->state != ker::net::wki::PeerState::CONNECTED) {
                return -EHOSTUNREACH;
            }

            // Find matching VFS resource from discovered table
            struct VfsFindCtx {
                uint16_t node_id;
                const char* export_name;
                ker::net::wki::DiscoveredResource* result;
            };
            VfsFindCtx find_ctx = {.node_id = NODE_ID, .export_name = export_name, .result = nullptr};
            ker::net::wki::wki_resource_foreach(
                [](const ker::net::wki::DiscoveredResource& r, void* ctx_ptr) {
                    auto* fc = static_cast<VfsFindCtx*>(ctx_ptr);
                    if (fc->result != nullptr) {
                        return;
                    }
                    bool const NAME_MATCH =
                        std::strncmp(static_cast<const char*>(r.name), fc->export_name, ker::net::wki::DISCOVERED_RESOURCE_NAME_LEN) == 0;
                    if (r.node_id == fc->node_id && r.resource_type == ker::net::wki::ResourceType::VFS && NAME_MATCH) {
                        fc->result = const_cast<ker::net::wki::DiscoveredResource*>(&r);
                    }
                },
                &find_ctx);

            if (find_ctx.result == nullptr) {
                return -ENXIO;
            }

            // Create mount target directory
            vfs_mkdir(target, 0755);

            return ker::net::wki::wki_remote_vfs_mount(NODE_ID, find_ctx.result->resource_id, target);
        }

        // Check for PARTUUID= prefix
        constexpr size_t PARTUUID_PREFIX_LEN = 9;  // "PARTUUID="
        bool const IS_PARTUUID = (source[0] == 'P' && source[1] == 'A' && source[2] == 'R' && source[3] == 'T' && source[4] == 'U' &&
                                  source[5] == 'U' && source[6] == 'I' && source[7] == 'D' && source[8] == '=');

        if (IS_PARTUUID) {
            bdev = ker::dev::block_device_find_by_partuuid(source + PARTUUID_PREFIX_LEN);
            if (bdev == nullptr) {
                log::warn("vfs_mount: PARTUUID not found: %s", source + PARTUUID_PREFIX_LEN);
                return -ENOENT;
            }
        } else if (source[0] == '/' && source[1] == 'd' && source[2] == 'e' && source[3] == 'v' && source[4] == '/') {
            // /dev/XXX - lookup by device name
            std::array<char, MAX_PATH_LEN> resolved_source{};
            int const SOURCE_RET = resolve_mount_source_path(source, resolved_source.data(), resolved_source.size());
            if (SOURCE_RET < 0) {
                return SOURCE_RET;
            }

            const char* block_source = resolved_source.data();
            if (block_source[0] != '/' || block_source[1] != 'd' || block_source[2] != 'e' || block_source[3] != 'v' ||
                block_source[4] != '/') {
                log::warn("vfs_mount: device symlink did not resolve under /dev: %s -> %s", source, block_source);
                return -ENOENT;
            }

            bdev = ker::dev::block_device_find_by_name(block_source + 5);
            if (bdev == nullptr) {
                // Walk devfs tree - handles subdirectory paths like wki/block/<name>
                // and triggers WKI proxy attach for remote block devices
                bdev = ker::vfs::devfs::devfs_resolve_block_device(block_source + 5);
            }
            if (bdev == nullptr) {
                log::warn("vfs_mount: device not found: %s", block_source);
                return -ENOENT;
            }
        }
    }

    // Auto-detect filesystem type when a block device is present and the
    // caller did NOT supply an explicit fstype. Probe the selected device
    // directly, then inspect GPT partition contents for whole-disk mounts.
    if (bdev != nullptr && (fstype == nullptr || fstype[0] == '\0')) {
        const char* detected_fstype = probe_block_device_fstype(bdev);
        if (detected_fstype != nullptr) {
            effective_fstype = detected_fstype;
            log::info("vfs_mount: auto-detected %s filesystem", detected_fstype);
        }
    }

    // Create mount point directory in tmpfs if needed
    vfs_mkdir(target, 0755);

    return mount_filesystem(target, effective_fstype, bdev, flags, data);
}

void init() {
    vfs_debug_log("vfs: init\n");
    // Register tmpfs as a minimal root filesystem
    ker::vfs::tmpfs::register_tmpfs();
    // Mount tmpfs at root
    mount_filesystem("/", "tmpfs", nullptr);

    // Register FAT32 driver (will be mounted when a disk is available)
    ker::vfs::fat32::register_fat32();

    // Register XFS driver
    ker::vfs::xfs::register_xfs();

    // Register and mount devfs for device files
    ker::vfs::devfs::devfs_init();
    mount_filesystem("/dev", "devfs", nullptr);

    // Register and mount procfs for process information
    ker::vfs::procfs::procfs_init();

    install_builtin_vfs_rules();
}

void vfs_stream_cache_invalidate_remote_scope(const void* remote_scope) { stream_invalidate_mount_scope(FSType::REMOTE, remote_scope); }

void vfs_wki_load_default_rules() {
    install_builtin_vfs_rules();

    ker::vfs::Stat st{};
    if (vfs_stat("/etc/vfstab", &st) < 0 || st.st_size <= 0) {
        return;
    }

    size_t const BYTES_TO_READ = std::min<size_t>(static_cast<size_t>(st.st_size), MAX_VFSTAB_BYTES);
    auto* file = vfs_open_file("/etc/vfstab", 0, 0);
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_read == nullptr) {
        release_open_file(file);
        return;
    }

    auto* buffer = new char[BYTES_TO_READ + 1];
    if (buffer == nullptr) {
        release_open_file(file);
        return;
    }

    ssize_t const BYTES_READ = clamp_io_count(file->fops->vfs_read(file, buffer, BYTES_TO_READ, 0), BYTES_TO_READ);
    release_open_file(file);
    if (BYTES_READ <= 0) {
        delete[] buffer;
        return;
    }

    buffer[static_cast<size_t>(BYTES_READ)] = '\0';
    load_vfs_rules_from_buffer(buffer);
    delete[] buffer;
}

auto vfs_wki_rule_add(const char* prefix, uint32_t route) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || prefix == nullptr) {
        return -EINVAL;
    }
    if (route != static_cast<uint32_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL) &&
        route != static_cast<uint32_t>(ker::mod::sched::task::WkiVfsRoute::HOST)) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> canonical{};
    int const ABSOLUTE = make_absolute(prefix, canonical.data(), canonical.size());
    if (ABSOLUTE < 0) {
        return ABSOLUTE;
    }

    int const CANONICAL_RESULT = canonicalize_path(canonical.data(), canonical.size());
    if (CANONICAL_RESULT < 0) {
        return CANONICAL_RESULT;
    }

    size_t const PREFIX_LEN = std::strlen(canonical.data());
    if (PREFIX_LEN == 0 || PREFIX_LEN >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX) {
        return -ENAMETOOLONG;
    }

    for (auto& rule : task->wki_vfs_rules) {
        if (rule.prefix_len == PREFIX_LEN && std::strncmp(rule.prefix.data(), canonical.data(), PREFIX_LEN) == 0) {
            std::memcpy(rule.prefix.data(), canonical.data(), PREFIX_LEN + 1);
            rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
            rule.route = static_cast<uint8_t>(route);
            rule.reserved = 0;
            return 0;
        }
    }

    mod::sched::task::WkiVfsRule new_rule{};
    std::memcpy(new_rule.prefix.data(), canonical.data(), PREFIX_LEN + 1);
    new_rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
    new_rule.route = static_cast<uint8_t>(route);
    new_rule.reserved = 0;
    if (!task->wki_vfs_rules.push_back(new_rule)) {
        return -ENOMEM;
    }
    return 0;
}

auto vfs_wki_rule_get(uint32_t index, char* prefix_buf, size_t prefix_buf_size, uint32_t* route_out) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -EINVAL;
    }

    if (index >= task->wki_vfs_rules.size()) {
        return -ENOENT;
    }

    const auto& rule = task->wki_vfs_rules[index];
    if (prefix_buf != nullptr) {
        if (std::cmp_greater(rule.prefix_len + 1, prefix_buf_size)) {
            return -ERANGE;
        }
        std::memcpy(prefix_buf, rule.prefix.data(), rule.prefix_len + 1);
    }
    if (route_out != nullptr) {
        *route_out = rule.route;
    }
    return static_cast<int>(rule.prefix_len);
}

auto vfs_wki_default_rule_get(uint32_t index, char* prefix_buf, size_t prefix_buf_size, uint32_t* route_out) -> int {
    if (index >= g_default_vfs_rules.size()) {
        return -ENOENT;
    }

    const auto& rule = g_default_vfs_rules[index];
    if (prefix_buf != nullptr) {
        if (std::cmp_greater(rule.prefix_len + 1, prefix_buf_size)) {
            return -ERANGE;
        }
        std::memcpy(prefix_buf, rule.prefix.data(), rule.prefix_len + 1);
    }
    if (route_out != nullptr) {
        *route_out = rule.route;
    }
    return static_cast<int>(rule.prefix_len);
}

auto vfs_wki_effective_route_for_path(const ker::mod::sched::task::Task* task, const char* path, uint32_t* route_out) -> int {
    if (path == nullptr || route_out == nullptr) {
        return -EINVAL;
    }

    VfsRouteDecision const DECISION = choose_task_route(task, path);
    *route_out = DECISION.route;
    return static_cast<int>(DECISION.prefix_len);
}

auto vfs_wki_rule_clear() -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -EINVAL;
    }

    task->wki_vfs_rules.clear();
    return 0;
}

static auto vfs_open_file_impl(const char* path, int flags, int mode, bool resolve_task_path, bool apply_task_policy) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    bool const OPEN_LOCAL = (flags & ker::vfs::O_LOCAL) != 0;
    int const ACCMODE = flags & 3;
    bool const PATH_REQUIRES_DIRECTORY = resolve_task_path && path_requires_directory(path);
    bool const FLAGS_REQUIRE_DIRECTORY = (flags & ker::vfs::O_DIRECTORY) != 0;
    if (FLAGS_REQUIRE_DIRECTORY && (flags & ker::vfs::O_CREAT) != 0) {
        return nullptr;
    }
    int backend_flags = flags;
    if (PATH_REQUIRES_DIRECTORY) {
        backend_flags &= ~ker::vfs::O_CREAT;
    }

    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    if (resolve_task_path) {
        if (resolve_task_path_raw_impl(path, pathBuffer, MAX_PATH_LEN, !OPEN_LOCAL) < 0) {
            return nullptr;
        }

        // attach before the backend open can find a mount point.
        ensure_wki_host_root_mount(pathBuffer);
    } else if (copy_path_string(path, pathBuffer, sizeof(pathBuffer)) < 0) {
        return nullptr;
    }

    auto mount_ref = find_mount_point(pathBuffer);
    MountPoint const* mount = mount_ref.get();
    bool const REMOTE_MOUNT = mount != nullptr && mount->fs_type == FSType::REMOTE;
    bool const SKIP_FINAL_SYMLINK_PROBE = mount != nullptr && !REMOTE_MOUNT && !PATH_REQUIRES_DIRECTORY && !FLAGS_REQUIRE_DIRECTORY &&
                                          metadata_cache_proves_final_not_symlink(pathBuffer, mount->fs_type, mount->dev_id);
    if (!REMOTE_MOUNT) {
        auto* fd_link_file = vfs_try_open_procfs_fd_link(pathBuffer, apply_task_policy && !OPEN_LOCAL);
        if (fd_link_file != nullptr) {
            if (PATH_REQUIRES_DIRECTORY || FLAGS_REQUIRE_DIRECTORY) {
                vfs_put_file(fd_link_file);
                return nullptr;
            }
            return fd_link_file;
        }
    }
    if (!REMOTE_MOUNT) {
        char resolved[MAX_PATH_LEN];  // NOLINT
        int const RESOLVE_RET =
            resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN, apply_task_policy && !OPEN_LOCAL, !SKIP_FINAL_SYMLINK_PROBE);
        if (RESOLVE_RET == 0) {
            std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
        }

        mount_ref = find_mount_point(pathBuffer);
        mount = mount_ref.get();
    }

    if (mount == nullptr) {
        return nullptr;
    }

    // Strip mount prefix
    const char* fs_relative_path = pathBuffer;
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    if (mount_len > 0 && pathBuffer[mount_len - 1] == '/' && mount_len == 1) {
        fs_relative_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        fs_relative_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        fs_relative_path = "";
    } else {
        fs_relative_path = pathBuffer + mount_len;
    }

    if (mount->read_only && open_flags_require_fs_write(backend_flags)) {
        return nullptr;
    }

    File* f = nullptr;

    switch (mount->fs_type) {
        case FSType::DEVFS:
            f = ker::vfs::devfs::devfs_open_path(fs_relative_path, backend_flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::devfs::get_devfs_fops();
                f->fs_type = FSType::DEVFS;
            }
            break;
        case FSType::FAT32:
            f = ker::vfs::fat32::fat32_open_path(fs_relative_path, backend_flags, mode,
                                                 static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::fat32::get_fat32_fops();
                f->fs_type = FSType::FAT32;
            }
            break;
        case FSType::TMPFS:
            f = ker::vfs::tmpfs::tmpfs_open_path(tmpfs_root_for_mount(mount), fs_relative_path, backend_flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
                f->fs_type = FSType::TMPFS;
            }
            break;
        case FSType::REMOTE:
            f = ker::net::wki::wki_remote_vfs_open_path(fs_relative_path, backend_flags, mode, mount->private_data);
            break;
        case FSType::XFS:
            f = ker::vfs::xfs::xfs_open_path(fs_relative_path, backend_flags, mode,
                                             static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::xfs::get_xfs_fops();
                f->fs_type = FSType::XFS;
            }
            break;
        case FSType::PROCFS:
            f = ker::vfs::procfs::procfs_open_path(fs_relative_path, backend_flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::procfs::get_procfs_fops();
                f->fs_type = FSType::PROCFS;
            }
            break;
        default:
            return nullptr;
    }

    if (f == nullptr && ACCMODE == 0 && (backend_flags & ker::vfs::O_CREAT) == 0) {
        f = create_synthetic_mount_dir_file(pathBuffer, mount->fs_type);
    }

    if (f != nullptr && (PATH_REQUIRES_DIRECTORY || FLAGS_REQUIRE_DIRECTORY) && !f->is_directory) {
        vfs_destroy_file(f);
        return nullptr;
    }

    if (f != nullptr) {
        int const TRUNCATE_RET = apply_open_truncation(f, backend_flags);
        if (TRUNCATE_RET < 0) {
            vfs_destroy_file(f);
            return nullptr;
        }
    }

    // Store the absolute VFS path for mount-overlay directory listing
    if (f != nullptr) {
        static_cast<void>(vfs_file_set_path(f, pathBuffer));
        f->dir_fs_count = static_cast<size_t>(-1);
        f->open_flags = flags;
        f->fd_flags = 0;
        if (open_create_should_invalidate_metadata(f, backend_flags)) {
            vfs_cache_notify_path_changed(pathBuffer, nullptr);
            metadata_cache_mark_file_data_observed(f);
        }
        if ((flags & ker::vfs::O_NO_CACHE) != 0) {
            vfs_cache_notify_path_changed(f->vfs_path, nullptr);
        }
        vfs_cache_notify_register_open_file(f);
        file_stat_snapshot_refresh(f);
    }

    return f;
}

auto vfs_open_file(const char* path, int flags, int mode) -> File* { return vfs_open_file_impl(path, flags, mode, true, true); }

auto vfs_open_file_resolved(const char* path, int flags, int mode) -> File* { return vfs_open_file_impl(path, flags, mode, false, false); }

#ifdef WOS_SELFTEST
auto vfs_selftest_make_file() -> File* {
    auto* file = new File();
    file->refcount.store(1, std::memory_order_relaxed);
    file->fops = &g_vfs_selftest_fops;
    return file;
}

auto vfs_selftest_fd_install_failure_closes_file() -> bool {
    g_vfs_selftest_close_count.store(0, std::memory_order_relaxed);

    auto* file = vfs_selftest_make_file();

    int const RET = vfs_install_open_file(nullptr, file);
    return RET == -EINVAL && g_vfs_selftest_close_count.load(std::memory_order_relaxed) == 1;
}

auto vfs_selftest_dup2_replace_preserves_newfd_on_failure() -> bool {
    constexpr int FD = 7;

    g_vfs_selftest_close_count.store(0, std::memory_order_relaxed);
    g_vfs_selftest_force_dup2_insert_failure.store(false, std::memory_order_relaxed);

    ker::mod::sched::task::Task task{};
    auto* existing = vfs_selftest_make_file();
    auto* replacement = vfs_selftest_make_file();
    if (!task.fd_table.insert(FD, existing)) {
        vfs_put_file(existing);
        vfs_put_file(replacement);
        return false;
    }
    task.set_fd_cloexec(FD);

    VfsDup2ReplaceResult const SUCCESS = vfs_replace_fd_for_dup2_locked(&task, FD, replacement);
    bool ok = SUCCESS.inserted && SUCCESS.existing == existing && task.fd_table.lookup(FD) == static_cast<void*>(replacement) &&
              !task.get_fd_cloexec(FD) && g_vfs_selftest_close_count.load(std::memory_order_relaxed) == 0;
    vfs_put_file(SUCCESS.existing);

    auto* failed_replacement = vfs_selftest_make_file();
    task.set_fd_cloexec(FD);
    g_vfs_selftest_force_dup2_insert_failure.store(true, std::memory_order_relaxed);
    VfsDup2ReplaceResult const FAILURE = vfs_replace_fd_for_dup2_locked(&task, FD, failed_replacement);
    g_vfs_selftest_force_dup2_insert_failure.store(false, std::memory_order_relaxed);
    ok = ok && !FAILURE.inserted && FAILURE.existing == replacement && task.fd_table.lookup(FD) == static_cast<void*>(replacement) &&
         task.get_fd_cloexec(FD) && g_vfs_selftest_close_count.load(std::memory_order_relaxed) == 1;
    vfs_put_file(failed_replacement);

    auto* cloexec_replacement = vfs_selftest_make_file();
    VfsDup2ReplaceResult const CLOEXEC = vfs_replace_fd_for_dup2_locked(&task, FD, cloexec_replacement, true);
    ok = ok && CLOEXEC.inserted && CLOEXEC.existing == replacement && task.fd_table.lookup(FD) == static_cast<void*>(cloexec_replacement) &&
         task.get_fd_cloexec(FD);
    vfs_put_file(CLOEXEC.existing);

    auto* still_open = reinterpret_cast<File*>(task.fd_table.remove(FD));
    task.clear_fd_cloexec(FD);
    ok = ok && still_open == cloexec_replacement;
    vfs_put_file(still_open);

    return ok && g_vfs_selftest_close_count.load(std::memory_order_relaxed) == 4;
}

auto vfs_selftest_fd_allocation_caps_cloexec_range() -> bool {
    using ker::mod::sched::task::Task;

    ker::mod::sched::task::Task task{};
    bool ok = true;

    for (unsigned fd = 0; fd < Task::FD_TABLE_SIZE; ++fd) {
        auto* file = vfs_selftest_make_file();
        if (file == nullptr) {
            ok = false;
            break;
        }

        int const ALLOCATED = vfs_alloc_fd(&task, file);
        if (ALLOCATED != static_cast<int>(fd)) {
            if (ALLOCATED < 0) {
                vfs_put_file(file);
            }
            ok = false;
            break;
        }

        task.set_fd_cloexec(fd);
        ok = ok && task.get_fd_cloexec(fd);
    }

    auto* overflow = vfs_selftest_make_file();
    if (overflow == nullptr) {
        ok = false;
    } else {
        int const RET = vfs_alloc_fd(&task, overflow);
        ok = ok && RET == -EMFILE && task.fd_table.lookup(Task::FD_TABLE_SIZE) == nullptr &&
             vfs_find_free_fd_below_limit_locked(&task, 0) == UINT64_MAX &&
             vfs_find_free_fd_below_limit_locked(&task, Task::FD_TABLE_SIZE) == UINT64_MAX;
        vfs_put_file(overflow);
    }

    constexpr unsigned HOLE_FD = 17;
    auto* removed = static_cast<File*>(task.fd_table.lookup(HOLE_FD));
    int const RELEASE_RET = vfs_release_fd(&task, HOLE_FD);
    ok = ok && RELEASE_RET == 0 && !task.get_fd_cloexec(HOLE_FD);
    vfs_put_file(removed);

    ok = ok && vfs_find_free_fd_below_limit_locked(&task, HOLE_FD) == HOLE_FD;
    auto* replacement = vfs_selftest_make_file();
    if (replacement == nullptr) {
        ok = false;
    } else {
        int const RET = vfs_alloc_fd(&task, replacement);
        ok = ok && RET == static_cast<int>(HOLE_FD) && !task.get_fd_cloexec(HOLE_FD);
        if (RET < 0) {
            vfs_put_file(replacement);
        }
    }

    task.fd_table.for_each([](uint64_t /*fd*/, void* val) {
        if (val != nullptr) {
            vfs_put_file(static_cast<File*>(val));
        }
    });

    return ok;
}
#endif

auto vfs_sendfile(int outfd, int infd, off_t* offset, size_t count) -> ssize_t {
    // Get the current task
    auto* task = mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Get input file
    auto* infile = vfs_get_file_retain(task, infd);
    if (infile == nullptr) {
        return -EBADF;
    }

    // Get output file
    auto* outfile = vfs_get_file_retain(task, outfd);
    if (outfile == nullptr) {
        vfs_put_file(infile);
        return -EBADF;
    }

    off_t source_offset = offset != nullptr ? *offset : infile->pos;
    if (g_pipe_write_fops_ptr != nullptr && outfile->fops == g_pipe_write_fops_ptr) {
        ssize_t const RET = vfs_sendfile_to_pipe(outfile, infile, &source_offset, count);
        if (offset != nullptr) {
            *offset = source_offset;
        } else {
            infile->pos = source_offset;
        }
        vfs_put_file(outfile);
        vfs_put_file(infile);
        return RET;
    }

    // Keep the staging buffer modest so blocking outputs do not cause sendfile()
    // to pre-read large chunks that cannot be written in the same call.
    constexpr size_t BUF_SIZE = 64UL * 1024UL;
    auto* buffer = new char[BUF_SIZE];
    if (buffer == nullptr) {
        vfs_put_file(outfile);
        vfs_put_file(infile);
        return -ENOMEM;
    }

    ssize_t total_sent = 0;
    size_t remaining = count;

    while (remaining > 0) {
        size_t const TO_READ = remaining > BUF_SIZE ? BUF_SIZE : remaining;
        ssize_t const READ_RESULT = vfs_pread_file(infile, buffer, TO_READ, source_offset);
        if (READ_RESULT < 0) {
            if (total_sent == 0) {
                delete[] buffer;
                vfs_put_file(outfile);
                vfs_put_file(infile);
                return READ_RESULT;
            }
            break;
        }

        if (READ_RESULT == 0) {
            break;
        }

        auto const CHUNK_SIZE = static_cast<size_t>(READ_RESULT);
        size_t chunk_offset = 0;
        while (chunk_offset < CHUNK_SIZE) {
            size_t bytes_written = 0;
            ssize_t const WRITE_RESULT = vfs_write_file(outfile, buffer + chunk_offset, CHUNK_SIZE - chunk_offset, &bytes_written);
            if (WRITE_RESULT < 0) {
                if (total_sent == 0) {
                    delete[] buffer;
                    if (offset != nullptr) {
                        *offset = source_offset;
                    } else {
                        infile->pos = source_offset;
                    }
                    vfs_put_file(outfile);
                    vfs_put_file(infile);
                    return WRITE_RESULT;
                }
                remaining = 0;
                break;
            }

            if (bytes_written == 0) {
                remaining = 0;
                break;
            }

            chunk_offset += bytes_written;
            total_sent += static_cast<ssize_t>(bytes_written);
            source_offset += static_cast<off_t>(bytes_written);
            remaining -= bytes_written;
        }
    }

    if (offset != nullptr) {
        *offset = source_offset;
    } else {
        infile->pos = source_offset;
    }

    delete[] buffer;
    vfs_put_file(outfile);
    vfs_put_file(infile);
    return total_sent;
}

auto vfs_fsync(int fd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    auto* file = vfs_get_file_retain(task, fd);
    if (file == nullptr) {
        return -EBADF;
    }

    int const RESULT = vfs_fsync_file(file);
    vfs_put_file(file);
    return RESULT;
}

auto vfs_fsync_file(File* file) -> int {
    if (file == nullptr) {
        return -EBADF;
    }

    switch (file->fs_type) {
        case FSType::FAT32: {
            return ker::vfs::fat32::fat32_fsync(file);
        }
        case FSType::XFS: {
            return ker::vfs::xfs::xfs_fsync(file);
        }
        case FSType::REMOTE: {
            return ker::net::wki::wki_remote_vfs_fsync(file);
        }
        case FSType::TMPFS:
        case FSType::DEVFS:
        case FSType::PROCFS:
        default:
            return 0;  // No-op for in-memory or read-only filesystems
    }
}

auto vfs_sync() -> int {
    int result = 0;

    for (size_t i = 0;; ++i) {
        auto mount_ref = get_mount_at(i);
        if (!mount_ref) {
            break;
        }

        auto* mount = mount_ref.get();
        int sync_result = 0;
        switch (mount->fs_type) {
            case FSType::FAT32:
                sync_result = ker::vfs::fat32::fat32_sync_mount(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
                break;
            case FSType::XFS:
                sync_result = ker::vfs::xfs::xfs_sync_mount(static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
                break;
            case FSType::TMPFS:
            case FSType::DEVFS:
            case FSType::PROCFS:
            case FSType::REMOTE:
            default:
                sync_result = 0;
                break;
        }

        if (sync_result != 0 && result == 0) {
            result = sync_result;
        }
    }

    return result;
}

auto vfs_shutdown_sync() -> int { return vfs_sync(); }

auto vfs_shutdown_unmount_all(const char* root_path) -> int { return shutdown_unmount_all_exact(root_path); }

auto vfs_link(const char* oldpath, const char* newpath) -> int {
    if (oldpath == nullptr || newpath == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_PATH_LEN> old_buf{};
    std::array<char, MAX_PATH_LEN> new_buf{};
    if (resolve_task_path_raw(oldpath, old_buf.data(), old_buf.size()) < 0) {
        return -ENAMETOOLONG;
    }
    if (resolve_task_path_raw(newpath, new_buf.data(), new_buf.size()) < 0) {
        return -ENAMETOOLONG;
    }

    auto old_mount_ref = find_mount_point(old_buf.data());
    auto new_mount_ref = find_mount_point(new_buf.data());
    MountPoint const* old_mount = old_mount_ref.get();
    MountPoint const* new_mount = new_mount_ref.get();
    if ((old_mount == nullptr) || (new_mount == nullptr)) {
        return -ENOENT;
    }

    // Cross-filesystem link is not allowed
    if (old_mount != new_mount) {
        return -EXDEV;
    }

    // FAT32 does not support hard links
    if (old_mount->fs_type == FSType::FAT32) {
        return -EPERM;
    }

    if (old_mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    // --- tmpfs hard link ---
    const char* old_fs = strip_mount_prefix(old_mount, old_buf.data());
    const char* new_fs = strip_mount_prefix(new_mount, new_buf.data());

    // Look up the source node
    auto* src_node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(old_mount), old_fs, false);
    if (src_node == nullptr) {
        return -ENOENT;
    }
    src_node = ker::vfs::tmpfs::tmpfs_canonical_node(src_node);
    if (src_node == nullptr) {
        return -ENOENT;
    }

    // Cannot hard link directories
    if (src_node->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        return -EPERM;
    }

    // Walk to new parent, extract new name
    const char* new_last_slash = nullptr;
    for (const char* p = new_fs; (*p) != 0; ++p) {
        if (*p == '/') {
            new_last_slash = p;
        }
    }

    ker::vfs::tmpfs::TmpNode* new_parent = nullptr;
    const char* new_name = nullptr;

    if (new_last_slash == nullptr) {
        new_parent = tmpfs_root_for_mount(new_mount);
        new_name = new_fs;
    } else {
        std::array<char, MAX_PATH_LEN> parent_path{};
        auto plen = static_cast<size_t>(new_last_slash - new_fs);
        if (plen >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(parent_path.data(), new_fs, plen);
        parent_path[plen] = '\0';
        new_parent = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(new_mount), parent_path.data(), false);
        new_name = new_last_slash + 1;
    }

    if (new_parent == nullptr || new_name == nullptr || *new_name == '\0') {
        return -ENOENT;
    }
    if (new_parent->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }

    ker::vfs::tmpfs::tmpfs_lock_tree();

    if (ker::vfs::tmpfs::tmpfs_lookup(new_parent, new_name) != nullptr) {
        ker::vfs::tmpfs::tmpfs_unlock_tree();
        return -EEXIST;
    }

    auto* link_node = ker::vfs::tmpfs::tmpfs_create_hardlink(new_parent, new_name, src_node);
    ker::vfs::tmpfs::tmpfs_unlock_tree();
    if (link_node == nullptr) {
        return -ENOMEM;
    }

    metadata_cache_note_path_changed("/", nullptr);
    vfs_cache_notify_path_changed(old_buf.data(), new_buf.data());
    return 0;
}

auto vfs_is_pipe_file(const File* f) -> bool {
    return f != nullptr && (f->fops == g_pipe_read_fops_ptr || f->fops == g_pipe_write_fops_ptr) && g_pipe_read_fops_ptr != nullptr;
}

auto vfs_is_socket_file(const File* f) -> bool { return f != nullptr && f->fs_type == FSType::SOCKET; }

}  // namespace ker::vfs
