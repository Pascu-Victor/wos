#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <memory>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wki.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
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

namespace {
constexpr size_t MAX_PATH_LEN = 512;
constexpr int MAX_SYMLINK_DEPTH = 8;
constexpr size_t MAX_COMPONENTS = 64;
constexpr size_t MAX_VFSTAB_BYTES = 4096;
constexpr size_t WKI_PATH_PREFIX_LEN = 5;

auto make_absolute(const char* path, char* out, size_t outsize) -> int;
auto canonicalize_path(char* path, size_t bufsize) -> int;
auto normalize_task_path_inplace(char* path, size_t bufsize) -> int;
auto readlink_resolved(const char* abs_path, char* buf, size_t bufsize) -> ssize_t;
auto strip_mount_prefix(const MountPoint* mount, const char* path) -> const char*;
auto tmpfs_root_for_mount(const MountPoint* mount) -> ker::vfs::tmpfs::TmpNode*;

ker::util::SmallVec<ker::mod::sched::task::WkiVfsRule, 8> g_default_vfs_rules;

struct VfsRouteDecision {
    uint8_t route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
    size_t prefix_len = 0;
};

constexpr size_t STREAM_CHUNK_SIZE = 65536;
constexpr size_t STREAM_ENTRY_BYTE_CAP = size_t{8} * 1024 * 1024;
constexpr size_t STREAM_DETACHED_REUSE_MAX = size_t{8} * 1024 * 1024;
constexpr size_t STREAM_MAX_ACTIVE_ISLANDS = 4;
constexpr uint64_t STREAM_DETACHED_TTL_US = 5000000;
constexpr uint64_t STREAM_SPLIT_DISTANCE_BYTES = uint64_t{2} * 1024 * 1024;
constexpr int STREAM_PREMATURE_EOF_RETRIES = 3;
constexpr size_t PIPE_WAKE_BATCH = 32;
constexpr size_t PIPE_DEFAULT_CAPACITY = 256UL * 1024UL;
constexpr size_t PIPE_DIRECT_MAX_CAPACITY = 256UL * 1024UL;

void stream_detach_file(File* file);
void stream_invalidate_file(File* file);
auto vfs_stream_cache_try_read(File* file, void* buf, size_t count, size_t* actual_size, ssize_t* result) -> bool;

auto vfs_destroy_file(File* f) -> int {
    if (f == nullptr) {
        return 0;
    }

    int close_result = 0;
    stream_detach_file(f);
    if ((f->fops != nullptr) && (f->fops->vfs_close != nullptr)) {
        close_result = f->fops->vfs_close(f);
    }
    delete[] f->vfs_path;
    f->private_data = nullptr;
    delete f;
    return close_result;
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

struct StreamFreshnessStamp {
    off_t size = 0;
    int64_t mtime_sec = 0;
    int64_t mtime_nsec = 0;
    int64_t ctime_sec = 0;
    int64_t ctime_nsec = 0;
    bool valid = false;
};

struct StreamCacheIdentity {
    const void* scope_key = nullptr;
    FSType fs_type = FSType::TMPFS;
    ino_t ino = 0;
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
    uint64_t last_used_us = 0;
    size_t cached_bytes = 0;
    std::deque<std::unique_ptr<StreamIsland>> islands;
};

std::deque<std::unique_ptr<StreamCacheEntry>> g_stream_cache;
ker::mod::sys::Mutex g_stream_cache_lock;

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
    return lhs.scope_key == rhs.scope_key && lhs.fs_type == rhs.fs_type && lhs.ino == rhs.ino;
}

auto stream_stat_has_freshness(const Stat& st) -> bool {
    return st.st_mtim.tv_sec != 0 || st.st_mtim.tv_nsec != 0 || st.st_ctim.tv_sec != 0 || st.st_ctim.tv_nsec != 0;
}

auto stream_capture_freshness(const Stat& st) -> StreamFreshnessStamp {
    StreamFreshnessStamp stamp = {};
    stamp.size = st.st_size;
    stamp.mtime_sec = st.st_mtim.tv_sec;
    stamp.mtime_nsec = st.st_mtim.tv_nsec;
    stamp.ctime_sec = st.st_ctim.tv_sec;
    stamp.ctime_nsec = st.st_ctim.tv_nsec;
    stamp.valid = stream_stat_has_freshness(st);
    return stamp;
}

auto stream_freshness_matches(const StreamFreshnessStamp& stamp, const Stat& st) -> bool {
    if (!stamp.valid) {
        return false;
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

auto stream_entry_should_keep_detached(const StreamCacheEntry* entry, uint64_t now_us) -> bool {
    if (entry == nullptr || stream_entry_has_readers(entry)) {
        return false;
    }
    if (!entry->can_reuse_detached || !entry->retain_full_file || !stream_entry_is_fully_cached(entry)) {
        return false;
    }
    return (now_us - entry->last_used_us) <= STREAM_DETACHED_TTL_US;
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
    if (mount->fs_type == FSType::REMOTE) {
        return mount->private_data;
    }
    return mount;
}

auto vfs_stream_cache_get_file_stat(File* file, Stat* statbuf) -> int {
    if (file == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    std::memset(statbuf, 0, sizeof(Stat));

    MountPoint const* sc_mount = (file->vfs_path != nullptr) ? find_mount_point(file->vfs_path) : nullptr;
    uint32_t const SC_DEV_ID = (sc_mount != nullptr) ? sc_mount->dev_id : 0;

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
            statbuf->st_dev = SC_DEV_ID;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = static_cast<blkcnt_t>((node->size + 511) / 512);
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
            return 0;
        }
        case FSType::FAT32: {
            int const R = ker::vfs::fat32::fat32_fstat(file, statbuf);
            if (R == 0) {
                statbuf->st_dev = SC_DEV_ID;
            }
            return R;
        }
        case FSType::DEVFS:
        case FSType::SOCKET:
        case FSType::PROCFS:
        case FSType::REMOTE:  // REMOTE has its own caching (ReadAheadCache/WriteBehindBuffer/RDMA); not in kernel stream cache
            return -ENOSYS;
        case FSType::XFS: {
            int const R = ker::vfs::xfs::xfs_fstat(file, statbuf);
            if (R == 0) {
                statbuf->st_dev = SC_DEV_ID;
            }
            return R;
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

    MountPoint const* mount = find_mount_point(file->vfs_path);
    if (mount == nullptr) {
        return -ENOENT;
    }

    const auto MODE = static_cast<mode_t>(statbuf.st_mode & S_IFMT);
    if (MODE != S_IFREG) {
        return -ENOTSUP;
    }
    if ((file->open_flags & 3) != 0) {
        return -EACCES;
    }

    identity->scope_key = stream_scope_key_for_mount(mount);
    identity->fs_type = mount->fs_type;
    identity->ino = statbuf.st_ino;

    if (identity->scope_key == nullptr || identity->ino == 0) {
        return -ENOSYS;
    }

    if (stamp != nullptr) {
        *stamp = stream_capture_freshness(statbuf);
    }
    if (can_reuse_detached != nullptr) {
        *can_reuse_detached = stream_stat_has_freshness(statbuf);
    }
    if (retain_full_file != nullptr) {
        *retain_full_file =
            stream_stat_has_freshness(statbuf) && statbuf.st_size > 0 && std::cmp_less_equal(statbuf.st_size, STREAM_DETACHED_REUSE_MAX);
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

    Stat st = {};
    if (vfs_stream_cache_get_file_stat(file, &st) != 0) {
        return nullptr;
    }

    StreamCacheIdentity identity = {};
    StreamFreshnessStamp freshness = {};
    bool can_reuse_detached = false;
    bool retain_full_file = false;
    if (stream_build_identity(file, st, &identity, &freshness, &can_reuse_detached, &retain_full_file) != 0) {
        return nullptr;
    }

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
        entry->last_used_us = NOW_US;
    } else if (!stream_entry_has_readers(entry)) {
        if (!stream_entry_should_keep_detached(entry, NOW_US) || !stream_freshness_matches(entry->freshness, st)) {
            stream_reset_entry_locked(entry);
            entry->freshness = freshness;
            entry->can_reuse_detached = can_reuse_detached;
            entry->retain_full_file = retain_full_file;
        }
        entry->last_used_us = NOW_US;
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

    Stat st = {};
    if (vfs_stream_cache_get_file_stat(file, &st) != 0) {
        return;
    }

    StreamCacheIdentity identity = {};
    if (stream_build_identity(file, st, &identity, nullptr, nullptr, nullptr) != 0) {
        return;
    }

    g_stream_cache_lock.lock();
    stream_invalidate_identity_locked(identity);
    stream_gc_locked(stream_now_us());
    g_stream_cache_lock.unlock();
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

auto vfs_stream_cache_try_read(File* file, void* buf, size_t count, size_t* actual_size, ssize_t* result) -> bool {
    if (result == nullptr || file == nullptr || buf == nullptr || count == 0 || file->fops == nullptr || file->fops->vfs_read == nullptr) {
        return false;
    }

    auto* attachment = stream_attach_file(file);
    if (attachment == nullptr || attachment->entry == nullptr) {
        return false;
    }

    auto* dst = static_cast<uint8_t*>(buf);
    size_t total = 0;
    int premature_eof_retries = 0;

    while (total < count) {
        uint64_t const OFFSET = static_cast<uint64_t>(file->pos) + total;

        g_stream_cache_lock.lock();
        auto* island = stream_select_island_locked(attachment, OFFSET);
        if (island == nullptr) {
            g_stream_cache_lock.unlock();
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
        ssize_t const READ_RET = file->fops->vfs_read(file, chunk->data.data(), STREAM_CHUNK_SIZE, static_cast<size_t>(FETCH_OFFSET));

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
        return static_cast<ker::vfs::tmpfs::TmpNode*>(mount->private_data);
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

auto find_first_mount_child(const char* path) -> MountPoint* {
    if (path == nullptr) {
        return nullptr;
    }

    size_t const PATH_LEN = std::strlen(path);
    for (size_t mi = 0; mi < get_mount_count(); ++mi) {
        MountPoint* mp = get_mount_at(mi);
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }

        size_t const MP_LEN = std::strlen(mp->path);
        if (MP_LEN > PATH_LEN && std::strncmp(mp->path, path, PATH_LEN) == 0 && mp->path[PATH_LEN] == '/') {
            return mp;
        }
    }

    return nullptr;
}

bool is_logical_wki_root_dir(const char* path) {
    if (path == nullptr) {
        return false;
    }

    if (std::strcmp(path, "/wki") == 0) {
        return true;
    }

    if (!ker::mod::sched::has_run_queues()) {
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

    if (ker::mod::sched::has_run_queues()) {
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

    return find_first_mount_child(resolved.data()) != nullptr;
}

auto fill_synthetic_mount_dir_stat(const char* path, Stat* statbuf) -> int {
    if (statbuf == nullptr) {
        return -EINVAL;
    }

    MountPoint const* child_mount = find_first_mount_child(path);
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
    if (find_first_mount_child(path) == nullptr && !(ker::net::wki::g_wki.initialized && is_logical_wki_root_dir(path))) {
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

    if (!ker::mod::sched::has_run_queues()) {
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

auto is_loader_debug_path(const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }
    return std::strcmp(path, "libc.so") == 0 || std::strcmp(path, "libc++abi.so") == 0 || std::strcmp(path, "debugserver") == 0;
}

void log_loader_path_event(const char* stage, const char* raw_path, const char* resolved_path, const MountPoint* mount, int rc) {
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
        if (MountPoint const* existing = find_mount_point(resolved_mount_root.data());
            existing != nullptr && existing->fs_type == FSType::REMOTE && std::strcmp(existing->path, resolved_mount_root.data()) == 0) {
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
    for (size_t index = 0; index < fs_count; ++index) {
        if (file->fops->vfs_readdir(file, &probe, index) != 0) {
            break;
        }

        size_t const PROBE_LEN = std::strlen(probe.d_name.data());
        if (PROBE_LEN == NAME_LEN && std::memcmp(probe.d_name.data(), name, NAME_LEN) == 0) {
            return true;
        }
    }

    return false;
}

// Re-apply the calling task's root prefix after following an absolute symlink.
// Without this, absolute symlink targets (e.g. /usr/sbin) escape the pivoted
// root and resolve against the global root instead of the task's root.
auto reapply_root_prefix(char* path, size_t bufsize) -> int {
    if (!ker::mod::sched::has_run_queues()) {
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

auto resolve_prefix_symlink_once(char* path, size_t bufsize, bool apply_task_policy) -> int {
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

    std::array<char, MAX_PATH_LEN> routed{};
    ker::mod::sched::task::Task const* current_task = nullptr;
    if (ker::mod::sched::can_query_current_task()) {
        current_task = ker::mod::sched::get_current_task();
    }

    int const ROUTE_RESULT = apply_task_vfs_route(current_task, path, routed.data(), routed.size());
    if (ROUTE_RESULT < 0) {
        return ROUTE_RESULT;
    }

    return copy_path_string(routed.data(), path, bufsize);
}

auto resolve_task_path_raw(const char* path, char* out, size_t outsize) -> int {
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

    return normalize_task_path_inplace(out, outsize);
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

    delete[] file->vfs_path;
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
auto resolve_symlinks(const char* path, char* resolved_buf, size_t bufsize, bool apply_task_policy = false) -> int {
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
        int const PREFIX_RESULT = resolve_prefix_symlink_once(resolved_buf, bufsize, apply_task_policy);
        if (PREFIX_RESULT < 0) {
            return PREFIX_RESULT;
        }
        if (PREFIX_RESULT > 0) {
            continue;
        }

        MountPoint const* mount = find_mount_point(resolved_buf);
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
                                                        pfd->node.type == ker::vfs::procfs::ProcNodeType::EXE_LINK));
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

        // Only tmpfs and XFS support symlinks currently
        if (mount->fs_type != FSType::TMPFS && mount->fs_type != FSType::XFS) {
            return 0;
        }

        // XFS: resolve symlinks via xfs_readlink
        if (mount->fs_type == FSType::XFS) {
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

            // Open the path to check if it's a symlink
            auto* xctx = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
            auto* f = ker::vfs::xfs::xfs_open_path(fs_path, 0, 0, xctx);
            if (f == nullptr) {
                return 0;
            }
            if (f->fops == nullptr || f->fops->vfs_readlink == nullptr) {
                ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
                delete f;
                return 0;
            }
            std::array<char, MAX_PATH_LEN> linkbuf{};
            ssize_t const LINK_LEN = f->fops->vfs_readlink(f, linkbuf.data(), linkbuf.size() - 1);
            ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
            delete f;
            if (LINK_LEN <= 0) {
                return 0;  // Not a symlink or error
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
            continue;
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
}  // namespace

auto vfs_open(std::string_view path, int flags, int mode) -> int {
    vfs_debug_log("vfs_open: opening file\n");

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
    if (resolve_task_path_raw(raw_path.data(), path_buffer.data(), MAX_PATH_LEN) < 0) {
        log_loader_path_event("resolve-failed", raw_path.data(), nullptr, nullptr, -ENOENT);
        return -ENOENT;
    }

    ensure_wki_host_root_mount(path_buffer.data());
    log_loader_path_event("resolved", raw_path.data(), path_buffer.data(), nullptr, 0);

    // Remote mounts resolve symlinks on the server side during the actual open.
    // Avoid probing each path component with client-side READLINK RPCs here:
    // they are redundant and can fail independently of the real open.
    MountPoint const* mount = find_mount_point(path_buffer.data());
    bool const REMOTE_MOUNT = (mount != nullptr && mount->fs_type == FSType::REMOTE);

    if (!REMOTE_MOUNT) {
        char resolved[MAX_PATH_LEN];  // NOLINT
        int const RESOLVE_RET = resolve_symlinks(path_buffer.data(), resolved, MAX_PATH_LEN, true);
        if (RESOLVE_RET == -ELOOP) {
            log::warn("vfs_open: too many symlink levels");
            return -ELOOP;
        }
        if (RESOLVE_RET == 0) {
            // Use the resolved path
            std::memcpy(path_buffer.data(), resolved, MAX_PATH_LEN);
        }
        log_loader_path_event("symlink-resolved", raw_path.data(), path_buffer.data(), nullptr, RESOLVE_RET);
    } else {
        log_loader_path_event("symlink-deferred-remote", raw_path.data(), path_buffer.data(), mount, 0);
    }

    auto* current = ker::mod::sched::get_current_task();
    if (current == nullptr) {
        vfs_debug_log("vfs_open: no current task\n");
        return -ESRCH;
    }

    int const ACCMODE = flags & 3;

    // Find the mount point for this path
    mount = find_mount_point(path_buffer.data());
    if (mount == nullptr) {
        vfs_debug_log("vfs_open: no mount point found for path\n");
        log::warn("vfs_open: no mount point found for path: %s", path_buffer.data());
        log_loader_path_event("mount-miss", raw_path.data(), path_buffer.data(), nullptr, -ENOENT);
        return -ENOENT;
    }
    log_loader_path_event("mount-found", raw_path.data(), path_buffer.data(), mount, 0);

    const char* fs_relative_path = strip_mount_prefix(mount, path_buffer.data());

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

    // Store the absolute VFS path for mount-overlay directory listing
    size_t const PATH_LEN = std::strlen(path_buffer.data());
    auto* path_copy = new char[PATH_LEN + 1];
    if (path_copy != nullptr) {
        std::memcpy(path_copy, path_buffer.data(), PATH_LEN + 1);
        f->vfs_path = path_copy;
    } else {
        f->vfs_path = nullptr;
    }
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
                // Permission denied - clean up and return
                delete[] f->vfs_path;
                delete f;
                return PERM_RET;
            }
        }
    }

    int const FD = vfs_alloc_fd(current, f);
    if (FD < 0) {
        return FD;
    }
    if ((flags & ker::vfs::O_CLOEXEC) != 0) {
        current->set_fd_cloexec(static_cast<unsigned>(FD));
    }
    return FD;
}

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

    ker::mod::perf::record_container_stat(0, t->pid, ker::mod::perf::PerfSubsystem::FD_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(FD_COUNT), 0, reinterpret_cast<uint64_t>(__builtin_return_address(0)));

    // Atomically decrement; only the CPU that drives refcount to 0 does teardown.
    if (f->refcount.fetch_sub(1, std::memory_order_acq_rel) > 1) {
        return 0;
    }

    return vfs_destroy_file(f);
}

auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_read == nullptr)) {
        vfs_put_file(f);
        return -EINVAL;
    }

    ssize_t cached_result = 0;
    if (vfs_stream_cache_try_read(f, buf, count, actual_size, &cached_result)) {
        if (cached_result >= 0) {
            f->pos += cached_result;
        }
        vfs_put_file(f);
        return cached_result;
    }

    ssize_t const R = f->fops->vfs_read(f, buf, count, static_cast<size_t>(f->pos));
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

auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file_retain(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_write == nullptr)) {
        vfs_put_file(f);
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
    if (result >= 0) {
        stream_invalidate_file(f);
        if (TMPFS_APPEND || XFS_APPEND) {
            f->pos = static_cast<off_t>(append_offset + static_cast<size_t>(result));
        } else {
            f->pos += result;
        }
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(result);
        }
        vfs_put_file(f);
        return result;
    }
    vfs_put_file(f);
    return result;
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
    uint64_t const SLOT = task->fd_table.find_first_unset(0);
    bool const INSERTED = SLOT != UINT64_MAX && task->fd_table.insert(SLOT, file);
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

auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int {
    if (task == nullptr || fd < 0) {
        return -EINVAL;
    }
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    task->fd_table.remove(static_cast<uint64_t>(fd));
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
        size_t const BASE_LEN = std::strlen(file->vfs_path);
        if (BASE_LEN >= resolved_size) {
            vfs_put_file(file);
            return -ENAMETOOLONG;
        }
        std::memcpy(resolved, file->vfs_path, BASE_LEN + 1);
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

    std::memcpy(resolved, base, base_len);
    resolved[base_len] = '\0';
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

    // Read directory entries using the current position as index
    auto const START_INDEX = static_cast<size_t>(f->pos);

    for (size_t i = 0; i < MAX_ENTRIES; ++i) {
        size_t const ACTUAL_INDEX = START_INDEX + i;

        // Phase 1: try filesystem readdir
        if (HAS_FS_READDIR && (std::cmp_equal(f->dir_fs_count, -1) || ACTUAL_INDEX < f->dir_fs_count)) {
            int const RET = f->fops->vfs_readdir(f, &entries[entries_read], ACTUAL_INDEX);
            if (RET == 0) {
                entries_read++;
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
                    entries_read++;
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
                    entries_read++;
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
                    entries_read++;
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
                    for (size_t pi = 0; pi < FS_COUNT; ++pi) {
                        int const PRET = f->fops->vfs_readdir(f, &probe, pi);
                        if (PRET != 0) {
                            break;
                        }
                        size_t const DN_LEN = std::strlen(probe.d_name.data());
                        if (DN_LEN == child_len && std::memcmp(probe.d_name.data(), child_start, child_len) == 0) {
                            already_in_fs = true;
                            break;
                        }
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

                    entries_read++;
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
    f->pos += static_cast<off_t>(entries_read);

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
    MountPoint const* mount = find_mount_point(abs_linkpath.data());
    if (mount == nullptr) {
        return -ENOENT;
    }

    // Only tmpfs supports symlinks
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
    return (node != nullptr) ? 0 : -1;
}

// Internal readlink operating on an already-resolved absolute path (no root
// prefix applied).  Used by resolve_prefix_symlink_once which works on paths
// that already include the task root.
namespace {

auto readlink_resolved(const char* abs_path, char* buf, size_t bufsize) -> ssize_t {
    if (abs_path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    MountPoint const* mount = find_mount_point(abs_path);
    if (mount == nullptr) {
        return -ENOENT;
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
            return -ENOENT;
        }
        ssize_t const RET = ker::vfs::xfs::get_xfs_fops()->vfs_readlink(f, buf, bufsize);
        ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
        delete f;
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
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, abs_path);

    auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
    if (node == nullptr) {
        return -ENOENT;
    }

    if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK || node->symlink_target == nullptr) {
        return -EINVAL;
    }

    size_t len = 0;
    while (node->symlink_target[len] != '\0') {
        len++;
    }
    size_t const TO_COPY = (len < bufsize) ? len : bufsize;
    std::memcpy(buf, node->symlink_target, TO_COPY);
    return static_cast<ssize_t>(TO_COPY);
}

}  // namespace

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

    return readlink_resolved(abs_path.data(), buf, bufsize);
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

    MountPoint const* mount = find_mount_point(abs_path.data());
    if (mount == nullptr) {
        return -ENOENT;
    }

    const char* fs_path = strip_mount_prefix(mount, abs_path.data());

    if (fs_path[0] == '\0') {
        return 0;
    }

    if (mount->fs_type == FSType::TMPFS) {
        auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, true);
        return (node != nullptr) ? 0 : -1;
    }

    if (mount->fs_type == FSType::XFS) {
        auto* xctx = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
        int const R = ker::vfs::xfs::xfs_mkdir_path(fs_path, mode, xctx);
        // mkdir -p calls mkdir on existing dirs; treat EEXIST as success
        return (R == -EEXIST) ? 0 : R;
    }

    if (mount->fs_type == FSType::REMOTE) {
        int const R = ker::net::wki::wki_remote_vfs_mkdir(mount->private_data, fs_path, mode);
        return (R == -EEXIST) ? 0 : R;
    }

    // For other mounts (devfs, procfs, etc.) return 0 if the directory exists
    ker::vfs::Stat st{};
    if (vfs_stat(abs_path.data(), &st) == 0) {
        return 0;
    }
    return -ENOSYS;
}

static auto vfs_stat_impl(const char* path, ker::vfs::Stat* statbuf, bool resolve_task_path, bool apply_task_policy) -> int {
    if (path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    bool const REQUIRE_DIRECTORY = resolve_task_path && path_requires_directory(path);
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
    MountPoint const* mount = find_mount_point(pathBuffer);
    bool const REMOTE_MOUNT = (mount != nullptr && mount->fs_type == FSType::REMOTE);

    if (!REMOTE_MOUNT) {
        char resolved[MAX_PATH_LEN];  // NOLINT
        int const RESOLVE_RET = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN, apply_task_policy);
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
    mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        log_loader_path_event("stat-mount-miss", path, pathBuffer, nullptr, -ENOENT);
        return -ENOENT;
    }
    log_loader_path_event("stat-mount-found", path, pathBuffer, mount, 0);

    const char* fs_path = strip_mount_prefix(mount, pathBuffer);

    // Initialize stat buffer
    std::memset(statbuf, 0, sizeof(ker::vfs::Stat));

    int result = -ENOSYS;

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(mount), fs_path, false);
            if (node == nullptr) {
                return -ENOENT;
            }
            // Access node fields immediately while potentially still holding any implicit lock
            statbuf->st_dev = mount->dev_id;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = static_cast<blkcnt_t>((node->size + 511) / 512);
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
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

    log_loader_path_event(result == 0 ? "stat-ok" : "stat-failed", path, pathBuffer, mount, result);

    return result;
}

auto vfs_stat(const char* path, Stat* statbuf) -> int { return vfs_stat_impl(path, statbuf, true, true); }

auto vfs_stat_resolved(const char* path, Stat* statbuf) -> int { return vfs_stat_impl(path, statbuf, false, false); }

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

    // Initialize stat buffer
    std::memset(statbuf, 0, sizeof(Stat));

    if (file->fops == nullptr && file->private_data == nullptr && file->is_directory && file->vfs_path != nullptr) {
        int const RESULT = fill_synthetic_mount_dir_stat(file->vfs_path, statbuf);
        vfs_put_file(file);
        return RESULT;
    }

    MountPoint const* fstat_mount = (file->vfs_path != nullptr) ? find_mount_point(file->vfs_path) : nullptr;
    uint32_t const FSTAT_DEV_ID = (fstat_mount != nullptr) ? fstat_mount->dev_id : 0;

    switch (file->fs_type) {
        case FSType::TMPFS: {
            // Pipes and epoll reuse FSType::TMPFS but private_data is not a TmpNode
            if (file->fops != ker::vfs::tmpfs::get_tmpfs_fops()) {
                // Return minimal stat for pseudo-TMPFS (pipes, epoll)
                statbuf->st_mode = S_IFIFO;
                statbuf->st_blksize = 4096;
                vfs_put_file(file);
                return 0;
            }
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(file->private_data);
            if (node == nullptr) {
                vfs_put_file(file);
                return -EBADF;
            }
            statbuf->st_dev = FSTAT_DEV_ID;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = static_cast<blkcnt_t>((node->size + 511) / 512);
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
            vfs_put_file(file);
            return 0;
        }
        case FSType::FAT32: {
            int const R = ker::vfs::fat32::fat32_fstat(file, statbuf);
            if (R == 0) {
                statbuf->st_dev = FSTAT_DEV_ID;
            }
            vfs_put_file(file);
            return R;
        }
        case FSType::DEVFS: {
            auto* node = static_cast<ker::vfs::devfs::DevFSNode*>(file->private_data);
            statbuf->st_dev = FSTAT_DEV_ID;
            statbuf->st_ino = (node != nullptr) ? reinterpret_cast<ino_t>(node) : 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = (node != nullptr) ? node->uid : 0;
            statbuf->st_gid = (node != nullptr) ? node->gid : 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            // Set mode based on whether this is a directory or device
            if (file->is_directory) {
                statbuf->st_mode = S_IFDIR | ((node != nullptr) ? node->mode : 0755);
            } else {
                statbuf->st_mode = S_IFCHR | ((node != nullptr) ? node->mode : 0666);
            }
            vfs_put_file(file);
            return 0;
        }
        case FSType::SOCKET: {
            statbuf->st_dev = FSTAT_DEV_ID;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFSOCK | 0666;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            vfs_put_file(file);
            return 0;
        }
        case FSType::REMOTE: {
            if (file->vfs_path != nullptr) {
                MountPoint const* mount = find_mount_point(file->vfs_path);
                if (mount != nullptr && mount->fs_type == FSType::REMOTE) {
                    const char* fs_path = strip_mount_prefix(mount, file->vfs_path);
                    int const RET = ker::net::wki::wki_remote_vfs_stat(mount->private_data, fs_path, statbuf);
                    if (RET == 0) {
                        statbuf->st_dev = mount->dev_id;
                        vfs_put_file(file);
                        return 0;
                    }
                }
            }

            // Fall back to a synthetic stat if path-based remote metadata lookup fails.
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
            vfs_put_file(file);
            return 0;
        }
        case FSType::PROCFS: {
            int const R = ker::vfs::procfs::procfs_fill_stat(file, statbuf, FSTAT_DEV_ID);
            vfs_put_file(file);
            return R;
        }
        case FSType::XFS: {
            int const R = ker::vfs::xfs::xfs_fstat(file, statbuf);
            if (R == 0) {
                statbuf->st_dev = FSTAT_DEV_ID;
            }
            vfs_put_file(file);
            return R;
        }
        default:
            vfs_put_file(file);
            return -ENOSYS;
    }
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

    MountPoint const* mount = find_mount_point(resolved.data());
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
            uint64_t const TOTAL_BLOCKS = ker::mod::mm::phys::get_total_mem_bytes() / 4096;
            uint64_t const FREE_BLOCKS = ker::mod::mm::phys::get_free_mem_bytes() / 4096;
            buf->f_bsize = 4096;
            buf->f_frsize = 4096;
            buf->f_blocks = TOTAL_BLOCKS;
            buf->f_bfree = FREE_BLOCKS;
            buf->f_bavail = FREE_BLOCKS;
            buf->f_files = TOTAL_BLOCKS;
            buf->f_ffree = FREE_BLOCKS;
            buf->f_favail = FREE_BLOCKS;
            buf->f_namemax = 255;
            return 0;
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
        MountPoint const* mount = find_mount_point(file->vfs_path);
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
                default:
                    break;
            }
        }
    }

    // Fallback: synthesise by fs_type on the file itself
    switch (file->fs_type) {
        case FSType::TMPFS: {
            uint64_t const TOTAL_BLOCKS = ker::mod::mm::phys::get_total_mem_bytes() / 4096;
            uint64_t const FREE_BLOCKS = ker::mod::mm::phys::get_free_mem_bytes() / 4096;
            buf->f_bsize = 4096;
            buf->f_frsize = 4096;
            buf->f_blocks = TOTAL_BLOCKS;
            buf->f_bfree = FREE_BLOCKS;
            buf->f_bavail = FREE_BLOCKS;
            buf->f_files = TOTAL_BLOCKS;
            buf->f_ffree = FREE_BLOCKS;
            buf->f_favail = FREE_BLOCKS;
            buf->f_namemax = 255;
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

    if (MountPoint const* mount = find_mount_point(resolved.data()); mount != nullptr) {
        stream_invalidate_mount_scope(mount->fs_type, stream_scope_key_for_mount(mount));
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

auto vfs_dup2(int oldfd, int newfd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
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
        return newfd;
    }

    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    auto* existing = vfs_take_fd_locked(task, newfd);
    bool const INSERTED = task->fd_table.insert(static_cast<uint64_t>(newfd), f);
    task->clear_fd_cloexec(static_cast<unsigned>(newfd));
    task->fd_table_lock.unlock_irqrestore(IRQF);

    if (!INSERTED) {
        if (existing != nullptr) {
            vfs_put_file(existing);
        }
        vfs_put_file(f);
        return -EMFILE;
    }

    if (existing != nullptr) {
        vfs_put_file(existing);
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
    if (f->fops == nullptr || f->fops->vfs_read == nullptr) {
        vfs_put_file(f);
        return -ENOSYS;
    }
    // Read at given offset without modifying file position
    f->positional_read_depth.fetch_add(1, std::memory_order_acq_rel);
    auto result = f->fops->vfs_read(f, buf, count, static_cast<size_t>(offset));
    f->positional_read_depth.fetch_sub(1, std::memory_order_acq_rel);
    vfs_put_file(f);
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
    if (f->fops == nullptr || f->fops->vfs_write == nullptr) {
        vfs_put_file(f);
        return -ENOSYS;
    }
    auto result = f->fops->vfs_write(f, buf, count, static_cast<size_t>(offset));
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

    MountPoint const* mount = find_mount_point(path_buf.data());
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        return ker::vfs::xfs::xfs_unlink_path(fs_path, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
    }

    if (mount->fs_type == FSType::FAT32) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        return ker::vfs::fat32::fat32_unlink_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        return ker::net::wki::wki_remote_vfs_unlink(mount->private_data, fs_path);
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

    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) {
        return -ENOENT;
    }
    if (child->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        return -EISDIR;
    }

    // Hold tmpfs tree lock to serialize with open_count increment in tmpfs_open_path
    ker::vfs::tmpfs::tmpfs_lock_tree();
    // Remove child from parent's children array
    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] == child) {
            // Shift remaining children down
            for (size_t j = i; j + 1 < parent->children_count; ++j) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            parent->children[parent->children_count] = nullptr;
            child->parent = nullptr;
            // POSIX: defer freeing if file handles are still open
            if (child->open_count.load(std::memory_order_acquire) > 0) {
                child->unlinked = true;
            } else {
                ker::vfs::tmpfs::tmpfs_free_node(child);
            }
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return 0;
        }
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

    MountPoint const* mount = find_mount_point(path_buf.data());
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type == FSType::FAT32) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        return ker::vfs::fat32::fat32_rmdir_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        return ker::net::wki::wki_remote_vfs_rmdir(mount->private_data, fs_path);
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, path_buf.data());
        return ker::vfs::xfs::xfs_rmdir_path(fs_path, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
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

    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) {
        return -ENOENT;
    }
    if (child->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }
    if (child->children_count > 0) {
        return -ENOTEMPTY;
    }

    ker::vfs::tmpfs::tmpfs_lock_tree();
    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] == child) {
            for (size_t j = i; j + 1 < parent->children_count; ++j) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            parent->children[parent->children_count] = nullptr;
            child->parent = nullptr;
            if (child->open_count.load(std::memory_order_acquire) > 0) {
                child->unlinked = true;
            } else {
                ker::vfs::tmpfs::tmpfs_free_node(child);
            }
            ker::vfs::tmpfs::tmpfs_unlock_tree();
            return 0;
        }
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

    MountPoint* old_mount = find_mount_point(old_buf.data());
    MountPoint* new_mount = find_mount_point(new_buf.data());
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

    if (old_mount->fs_type == FSType::FAT32 && new_mount->fs_type == FSType::FAT32 && old_mount == new_mount) {
        return ker::vfs::fat32::fat32_rename_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(old_mount->private_data),
                                                  strip_mount_prefix(old_mount, old_buf.data()),
                                                  strip_mount_prefix(new_mount, new_buf.data()));
    }

    if (old_mount->fs_type == FSType::REMOTE && new_mount->fs_type == FSType::REMOTE && old_mount == new_mount) {
        return ker::net::wki::wki_remote_vfs_rename(old_mount->private_data, strip_mount_prefix(old_mount, old_buf.data()),
                                                    strip_mount_prefix(new_mount, new_buf.data()));
    }

    if (old_mount->fs_type == FSType::XFS && new_mount->fs_type == FSType::XFS && old_mount == new_mount) {
        return ker::vfs::xfs::xfs_rename_path(strip_mount_prefix(old_mount, old_buf.data()), strip_mount_prefix(new_mount, new_buf.data()),
                                              static_cast<ker::vfs::xfs::XfsMountContext*>(old_mount->private_data));
    }

    if (old_mount->fs_type != FSType::TMPFS || new_mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }
    if (old_mount != new_mount) {
        return -EXDEV;
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
    if (existing != nullptr) {
        // Remove existing from parent
        for (size_t i = 0; i < new_parent->children_count; ++i) {
            if (new_parent->children[i] == existing) {
                for (size_t j = i; j + 1 < new_parent->children_count; ++j) {
                    new_parent->children[j] = new_parent->children[j + 1];
                }
                new_parent->children_count--;
                new_parent->children[new_parent->children_count] = nullptr;
                existing->parent = nullptr;
                if (existing->open_count.load(std::memory_order_acquire) > 0) {
                    existing->unlinked = true;
                } else {
                    ker::vfs::tmpfs::tmpfs_free_node(existing);
                }
                break;
            }
        }
    }

    // Remove old node from old parent
    for (size_t i = 0; i < old_parent->children_count; ++i) {
        if (old_parent->children[i] == old_node) {
            for (size_t j = i; j + 1 < old_parent->children_count; ++j) {
                old_parent->children[j] = old_parent->children[j + 1];
            }
            old_parent->children_count--;
            old_parent->children[old_parent->children_count] = nullptr;
            break;
        }
    }

    // Rename and reparent
    size_t const NN_LEN = std::strlen(new_name);
    size_t const COPY_LEN = NN_LEN < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1 ? NN_LEN : ker::vfs::tmpfs::TMPFS_NAME_MAX - 1;
    std::memcpy(old_node->name.data(), new_name, COPY_LEN);
    old_node->name[COPY_LEN] = '\0';

    // Add to new parent (inline - avoid circular include of tmpfs internal helpers)
    // Grow children array if needed
    if (new_parent->children_count >= new_parent->children_capacity) {
        size_t const NEW_CAP = (new_parent->children_capacity == 0) ? 8 : new_parent->children_capacity * 2;
        auto** new_arr = new ker::vfs::tmpfs::TmpNode*[NEW_CAP];
        for (size_t i = 0; i < new_parent->children_count; ++i) {
            new_arr[i] = new_parent->children[i];
        }
        for (size_t i = new_parent->children_count; i < NEW_CAP; ++i) {
            new_arr[i] = nullptr;
        }
        delete[] new_parent->children;
        new_parent->children = new_arr;
        new_parent->children_capacity = NEW_CAP;
    }
    new_parent->children[new_parent->children_count++] = old_node;
    old_node->parent = new_parent;

    ker::vfs::tmpfs::tmpfs_unlock_tree();
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

    auto* mount = find_mount_point(path_buffer.data());
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
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) {
                return -ENOENT;
            }
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::FAT32:
            return 0;  // FAT32 has no permission model; silently accept
        case FSType::XFS:
            return ker::vfs::xfs::xfs_chmod_path(fs_path, mode, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
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
            node->mode = static_cast<uint32_t>(mode) & 07777;
            vfs_put_file(f);
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
            vfs_put_file(f);
            return 0;  // No permission model; silently accept
        case FSType::XFS: {
            int const RESULT = ker::vfs::xfs::xfs_fchmod(f, mode);
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

    auto* mount = find_mount_point(path_buffer.data());
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
            if (std::cmp_not_equal(owner, -1)) {
                node->uid = owner;
            }
            if (std::cmp_not_equal(group, -1)) {
                node->gid = group;
            }
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
            if (std::cmp_not_equal(owner, -1)) {
                node->uid = owner;
            }
            if (std::cmp_not_equal(group, -1)) {
                node->gid = group;
            }
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
        stream_invalidate_file(f);
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
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            uint64_t const SLOT = task->fd_table.find_first_unset(arg);
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
            f->open_flags = static_cast<int>(arg);
            vfs_put_file(f);
            return 0;
        case 1030: {  // F_DUPFD_CLOEXEC - dup to fd >= arg, set close-on-exec
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            uint64_t const SLOT = task->fd_table.find_first_unset(arg);
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

    // Wait queues for blocking pipe I/O
    ker::util::SmallVec<uint64_t, 2> readers_waiting;
    ker::util::SmallVec<uint64_t, 2> writers_waiting;

    ker::util::SmallVec<uint64_t, 2> read_poll_waiting;
    ker::util::SmallVec<uint64_t, 2> write_poll_waiting;
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

auto pipe_register_waiter(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    for (unsigned long waiter : waiters) {
        if (waiter == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

auto pipe_register_poll_waiter(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    return pipe_register_waiter(waiters, pid);
}

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
    return (task->sig_pending & ~task->sig_mask) != 0;
}

void signal_current_sigpipe() {
    auto* task = ker::mod::sched::get_current_task();
    if (task != nullptr) {
        task->sig_pending |= (1ULL << (13 - 1));
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

void pipe_collect_waiters_locked(ker::util::SmallVec<uint64_t, 2>& waiters, PipeWakeList& pending, size_t* pending_count) {
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
            waiter->sig_pending |= (1ULL << (13 - 1));
        }

        ker::mod::sched::wake_task_from_event(waiter, ker::mod::sched::EventWakeDeferredSwitch::CANCEL);
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
            ker::mod::sched::preemptible_syscall_park("pipe_write");
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
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_read == nullptr) {
        return -ENOSYS;
    }
    file->positional_read_depth.fetch_add(1, std::memory_order_acq_rel);
    ssize_t const RESULT = file->fops->vfs_read(file, buf, count, static_cast<size_t>(offset));
    file->positional_read_depth.fetch_sub(1, std::memory_order_acq_rel);
    return RESULT;
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

auto vfs_pipe(int pipefd[2]) -> int {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    if (pipefd == nullptr) {
        return -EINVAL;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Keep a moderate default capacity so simple producer/consumer pipelines do
    // not bounce through the scheduler every 4 KiB.
    auto* pipe_buf = new char[PIPE_DEFAULT_CAPACITY];

    auto* ps = new PipeState{};
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
                ker::mod::sched::preemptible_syscall_park("pipe_read");
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
                    task->sig_pending |= (1ULL << (13 - 1));
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
                    ker::mod::sched::preemptible_syscall_park("pipe_write");
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
                ker::mod::sched::preemptible_syscall_park("pipe_write");
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
        .vfs_ioctl = nullptr,
    };

    // Expose fops pointers for vfs_is_pipe_file() identity check
    g_pipe_read_fops_ptr = &pipe_read_fops;
    g_pipe_write_fops_ptr = &pipe_write_fops;

    // Create read-end File
    auto* rf = new File;
    rf->private_data = ps;
    rf->fops = &pipe_read_fops;
    rf->pos = 0;
    rf->is_directory = false;
    rf->fs_type = FSType::TMPFS;  // pseudo-type
    rf->refcount = 1;
    rf->open_flags = 0;  // O_RDONLY
    rf->fd_flags = 0;
    rf->vfs_path = nullptr;
    rf->dir_fs_count = 0;

    // Create write-end File
    auto* wf = new File;
    wf->private_data = ps;
    wf->fops = &pipe_write_fops;
    wf->pos = 0;
    wf->is_directory = false;
    wf->fs_type = FSType::TMPFS;
    wf->refcount = 1;
    wf->open_flags = 1;  // O_WRONLY
    wf->fd_flags = 0;
    wf->vfs_path = nullptr;
    wf->dir_fs_count = 0;

    int const RFD = vfs_alloc_fd(task, rf);
    if (RFD < 0) {
        delete rf;
        delete wf;
        delete[] pipe_buf;
        delete ps;
        return -EMFILE;
    }
    int const WFD = vfs_alloc_fd(task, wf);
    if (WFD < 0) {
        vfs_release_fd(task, RFD);
        delete rf;
        delete wf;
        delete[] pipe_buf;
        delete ps;
        return -EMFILE;
    }

    pipefd[0] = RFD;
    pipefd[1] = WFD;
    pipe_register_state(ps);
    return 0;
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

auto vfs_mount(const char* source, const char* target, const char* fstype) -> int {
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
            bdev = ker::dev::block_device_find_by_name(source + 5);
            if (bdev == nullptr) {
                // Walk devfs tree - handles subdirectory paths like wki/block/<name>
                // and triggers WKI proxy attach for remote block devices
                bdev = ker::vfs::devfs::devfs_resolve_block_device(source + 5);
            }
            if (bdev == nullptr) {
                log::warn("vfs_mount: device not found: %s", source);
                return -ENOENT;
            }
        }
    }

    // Auto-detect filesystem type when a block device is present and the
    // caller did NOT supply an explicit fstype (i.e. fstype was NULL or
    // empty, which we defaulted to "fat32" above).  Probe the first sector
    // for known superblock magic so the correct driver is selected.
    if (bdev != nullptr && (fstype == nullptr || fstype[0] == '\0')) {
        ker::vfs::buffer_cache_init();
        auto* probe_buf = ker::vfs::bread(bdev, 0);
        if (probe_buf != nullptr) {
            // XFS superblock magic at offset 0: 0x58465342 ('XFSB') big-endian
            if (probe_buf->size >= 4) {
                uint32_t const MAGIC = (static_cast<uint32_t>(probe_buf->data[0]) << 24) |
                                       (static_cast<uint32_t>(probe_buf->data[1]) << 16) |
                                       (static_cast<uint32_t>(probe_buf->data[2]) << 8) | (static_cast<uint32_t>(probe_buf->data[3]));
                if (MAGIC == 0x58465342) {  // XFS_SB_MAGIC
                    effective_fstype = "xfs";
                    log::info("vfs_mount: auto-detected XFS filesystem");
                }
            }
            ker::vfs::brelse(probe_buf);
        }
    }

    // Create mount point directory in tmpfs if needed
    vfs_mkdir(target, 0755);

    return mount_filesystem(target, effective_fstype, bdev);
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

    ssize_t const BYTES_READ = file->fops->vfs_read(file, buffer, BYTES_TO_READ, 0);
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
        if (resolve_task_path_raw(path, pathBuffer, MAX_PATH_LEN) < 0) {
            return nullptr;
        }
    } else if (copy_path_string(path, pathBuffer, sizeof(pathBuffer)) < 0) {
        return nullptr;
    }

    char resolved[MAX_PATH_LEN];  // NOLINT
    int const RESOLVE_RET = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN, apply_task_policy);
    if (RESOLVE_RET == 0) {
        std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
    }

    // Find mount point
    MountPoint const* mount = find_mount_point(pathBuffer);
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

    // Store the absolute VFS path for mount-overlay directory listing
    if (f != nullptr) {
        size_t const PL = std::strlen(pathBuffer);
        auto* pc = new char[PL + 1];
        if (pc != nullptr) {
            std::memcpy(pc, pathBuffer, PL + 1);
            f->vfs_path = pc;
        } else {
            f->vfs_path = nullptr;
        }
        f->dir_fs_count = static_cast<size_t>(-1);
    }

    return f;
}

auto vfs_open_file(const char* path, int flags, int mode) -> File* { return vfs_open_file_impl(path, flags, mode, true, true); }

auto vfs_open_file_resolved(const char* path, int flags, int mode) -> File* { return vfs_open_file_impl(path, flags, mode, false, false); }

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
        ssize_t const READ_RESULT = vfs_pread(infd, buffer, TO_READ, source_offset);
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
            ssize_t const WRITE_RESULT = vfs_write(outfd, buffer + chunk_offset, CHUNK_SIZE - chunk_offset, &bytes_written);
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
                chunk_offset = CHUNK_SIZE;
                remaining = 0;
                break;
            }

            if (bytes_written == 0) {
                chunk_offset = CHUNK_SIZE;
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

    switch (file->fs_type) {
        case FSType::FAT32: {
            int const RESULT = ker::vfs::fat32::fat32_fsync(file);
            vfs_put_file(file);
            return RESULT;
        }
        case FSType::XFS: {
            int const RESULT = ker::vfs::xfs::xfs_fsync(file);
            vfs_put_file(file);
            return RESULT;
        }
        case FSType::TMPFS:
        case FSType::DEVFS:
        case FSType::PROCFS:
            vfs_put_file(file);
            return 0;  // No-op for in-memory or read-only filesystems
        default:
            vfs_put_file(file);
            return 0;
    }
}

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

    MountPoint const* old_mount = find_mount_point(old_buf.data());
    MountPoint const* new_mount = find_mount_point(new_buf.data());
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

    // --- tmpfs hard link (data-copy) ---
    const char* old_fs = strip_mount_prefix(old_mount, old_buf.data());
    const char* new_fs = strip_mount_prefix(new_mount, new_buf.data());

    // Look up the source node
    auto* src_node = ker::vfs::tmpfs::tmpfs_walk_path(tmpfs_root_for_mount(old_mount), old_fs, false);
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

    // Destination must not already exist
    if (ker::vfs::tmpfs::tmpfs_lookup(new_parent, new_name) != nullptr) {
        return -EEXIST;
    }

    // Create the new node as a copy of the source
    if (src_node->type == ker::vfs::tmpfs::TmpNodeType::SYMLINK) {
        // Copy symlink
        ker::vfs::tmpfs::tmpfs_create_symlink(new_parent, new_name, src_node->symlink_target);
    } else {
        // Regular file - copy data
        auto* dst = ker::vfs::tmpfs::tmpfs_create_file(new_parent, new_name, src_node->mode);
        if (dst == nullptr) {
            return -ENOMEM;
        }
        if (src_node->data != nullptr && src_node->size > 0) {
            dst->data = new char[src_node->size];
            std::memcpy(dst->data, src_node->data, src_node->size);
            dst->size = src_node->size;
            dst->capacity = src_node->size;
        }
        dst->uid = src_node->uid;
        dst->gid = src_node->gid;
    }

    return 0;
}

auto vfs_is_pipe_file(const File* f) -> bool {
    return f != nullptr && (f->fops == g_pipe_read_fops_ptr || f->fops == g_pipe_write_fops_ptr) && g_pipe_read_fops_ptr != nullptr;
}

auto vfs_is_socket_file(const File* f) -> bool { return f != nullptr && f->fs_type == FSType::SOCKET; }

}  // namespace ker::vfs
