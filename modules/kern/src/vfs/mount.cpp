#include "mount.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/gpt.hpp>
#include <memory>
#include <net/wki/event.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>

#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/fs/devfs.hpp"
#include "vfs/fs/procfs.hpp"
#include "vfs/fs/tmpfs.hpp"
#include "vfs/vfs.hpp"

namespace ker::vfs {

// Mount point registry
namespace {
ker::util::SmallVec<MountPoint*, 8> mounts;
struct MountInitialization {
    mount_ns::NamespaceRef namespace_owner{};
    std::array<char, MOUNT_PATH_MAX> path{};
    bool active = false;
};
constexpr size_t MAX_MOUNT_INITIALIZATIONS = 64;
std::array<MountInitialization, MAX_MOUNT_INITIALIZATIONS> mount_initializations{};
mod::sys::Spinlock mount_lock;  // Protects mounts and mount_count
std::atomic<uint64_t> mount_generation{1};
// Even values admit mount-table publication. remap_mounts_for_pivot() makes
// this odd until task roots are updated and rebase_wki_mounts_for_new_root()
// completes the namespace transition.
std::atomic<uint64_t> mount_pivot_epoch{2};
// Protected by mount_lock; only the exact pivot owner may reopen the epoch.
uint64_t pivot_namespace_generation = 0;
// Zero is exhausted. IDs also bind exact unmount/export retirement requests,
// so no published identity may be reused after the last uint32_t value.
uint32_t next_dev_id = 1;
using log = ker::mod::dbg::logger<"vfs_mount">;

constexpr size_t MAX_MOUNT_PATH = 512;
constexpr size_t MAX_MOUNT_COMPONENTS = 64;
constexpr uint32_t SHUTDOWN_UNMOUNT_DRAIN_YIELDS = 5000;
constexpr size_t MOUNT_LOOKUP_CACHE_SET_COUNT = 128;
constexpr size_t MOUNT_LOOKUP_CACHE_WAYS = 2;
constexpr size_t MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT = 64;
constexpr size_t MOUNT_ROOT_FALLBACK_CACHE_WAYS = 2;
constexpr size_t MOUNT_ROOT_COMPONENT_CACHE_MAX = 64;
static_assert((MOUNT_LOOKUP_CACHE_SET_COUNT & (MOUNT_LOOKUP_CACHE_SET_COUNT - 1)) == 0);
static_assert((MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT & (MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT - 1)) == 0);

struct PivotMountReplacement {
    MountPoint* mount = nullptr;
    char* path = nullptr;
    size_t path_len = 0;
};

// This owner is destroyed only after namespace publication is unlocked. The
// native handles keep failed lookup results alive until that same point.
struct PivotCoverEntry {
    File* candidate = nullptr;
    File* retired = nullptr;
    MountRef parent_hold;
    tmpfs::TmpfsLookupHandle tmp;
    xfs::XfsLookupHandle xfs;
    ~PivotCoverEntry() {
        if (candidate != nullptr) vfs_put_file(candidate);
        if (retired != nullptr) vfs_put_file(retired);
    }
};
struct PivotCoverState {
    std::unique_ptr<PivotCoverEntry[]> entries;
    size_t count = 0;
};

auto pin_pivot_cover(const ker::mod::security::Snapshot& credentials, MountPoint* parent, const char* relative, uint64_t generation,
                     PivotCoverEntry& output) -> int {
    if (parent == nullptr || relative == nullptr || output.candidate != nullptr) return -EINVAL;
    output.parent_hold = retain_mount_point(parent);
    if (!output.parent_hold) return -EAGAIN;
    int status = -ENOTSUP;
    // The caller owns the odd mount epoch and namespace publication. Native
    // backend lookup deliberately avoids VFS routing and even-epoch waits.
    if (parent->fs_type == FSType::TMPFS) {
        auto* root = parent->private_data != nullptr ? tmpfs::mount_root(static_cast<tmpfs::TmpfsMount*>(parent->private_data))
                                                     : tmpfs::get_root_node();
        status = tmpfs::tmpfs_acquire_lookup(credentials, root, relative, &output.tmp);
        if (status == 0) output.candidate = tmpfs::tmpfs_pin_directory_lookup(credentials, output.tmp, &status, false);
    } else if (parent->fs_type == FSType::XFS) {
        status = xfs::xfs_acquire_lookup(credentials, relative, static_cast<xfs::XfsMountContext*>(parent->private_data), &output.xfs);
        if (status == 0) output.candidate = xfs::xfs_pin_directory_lookup(credentials, output.xfs, &status, false);
    }
    if (status != 0) return status;
    if (output.candidate == nullptr) return -ENOMEM;
    if (!retain_mount_for_open_file(parent)) return -EAGAIN;
    auto& file = *output.candidate;
    file.mount_owner = parent;
    file.mount_dev_id = parent->dev_id;
    file.mount_generation = generation;
    file.dir_fs_count = static_cast<size_t>(-1);
    return 0;
}

auto wait_for_stable_mount_pivot_epoch() -> uint64_t {
    while (true) {
        uint64_t const EPOCH = mount_pivot_epoch.load(std::memory_order_acquire);
        if ((EPOCH & 1U) == 0) {
            return EPOCH;
        }
        if (ker::mod::sched::can_query_current_task() && ker::mod::sched::preemptible() && ker::mod::sched::interrupts_enabled()) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }
}

void release_pivot_replacements(ker::util::SmallVec<PivotMountReplacement, 8>& replacements) {
    for (auto& replacement : replacements) {
        delete[] replacement.path;
        replacement.path = nullptr;
    }
    replacements.clear();
}

auto abort_mount_pivot(uint64_t odd_epoch, int error) -> int {
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) == odd_epoch) {
        mount_pivot_epoch.store(odd_epoch + 1, std::memory_order_release);
    }
    mount_lock.unlock();
    return error;
}

struct MountLookupCacheEntry {
    std::array<char, MOUNT_PATH_MAX> path{};
    uint64_t hash = 0;
    uint64_t generation = 0;
    uint64_t namespace_generation = 0;
    uint64_t last_used = 0;
    MountPoint* mount = nullptr;
    size_t path_len = 0;
    bool valid = false;
};

struct MountLookupCacheSet {
    mod::sys::Spinlock lock;
    std::array<MountLookupCacheEntry, MOUNT_LOOKUP_CACHE_WAYS> ways{};
    uint64_t clock = 0;
};

struct MountRootFallbackCacheEntry {
    std::array<char, MOUNT_ROOT_COMPONENT_CACHE_MAX> component{};
    uint64_t hash = 0;
    uint64_t generation = 0;
    uint64_t namespace_generation = 0;
    uint64_t last_used = 0;
    MountPoint* root_mount = nullptr;
    size_t component_len = 0;
    bool valid = false;
};

struct MountRootFallbackCacheSet {
    mod::sys::Spinlock lock;
    std::array<MountRootFallbackCacheEntry, MOUNT_ROOT_FALLBACK_CACHE_WAYS> ways{};
    uint64_t clock = 0;
};

std::array<MountLookupCacheSet, MOUNT_LOOKUP_CACHE_SET_COUNT> mount_lookup_cache{};
std::array<MountRootFallbackCacheSet, MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT> mount_root_fallback_cache{};

#ifdef WOS_SELFTEST
std::atomic<uint64_t> mount_lookup_cache_hits{0};
std::atomic<uint64_t> mount_lookup_calls{0};
#endif

auto path_is_under_root(const char* path, const char* root, size_t root_len) -> bool {
    return std::strncmp(path, root, root_len) == 0 && (path[root_len] == '/' || path[root_len] == '\0');
}

auto copy_mount_path_string(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN + 1 > outsize) {
        return -ENAMETOOLONG;
    }

    std::memcpy(out, path, PATH_LEN + 1);
    return 0;
}

auto make_mount_path_absolute(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }

    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN == 0) {
        return -EINVAL;
    }

    if (path[0] == '/') {
        return copy_mount_path_string(path, out, outsize);
    }

    if (!ker::mod::sched::can_query_current_task()) {
        return -ESRCH;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    size_t const CWD_LEN = std::strlen(task->cwd.data());
    if (CWD_LEN == 0) {
        return -EINVAL;
    }

    bool const NEED_SEP = CWD_LEN > 1;
    size_t const TOTAL = CWD_LEN + (NEED_SEP ? 1 : 0) + PATH_LEN + 1;
    if (TOTAL > outsize) {
        return -ENAMETOOLONG;
    }

    std::memcpy(out, task->cwd.data(), CWD_LEN);
    if (NEED_SEP) {
        out[CWD_LEN] = '/';
        std::memcpy(out + CWD_LEN + 1, path, PATH_LEN + 1);
    } else {
        std::memcpy(out + CWD_LEN, path, PATH_LEN + 1);
    }
    return 0;
}

auto canonicalize_mount_path(char* path, size_t bufsize) -> int {
    if (path == nullptr || bufsize == 0 || path[0] != '/') {
        return -EINVAL;
    }

    std::array<const char*, MAX_MOUNT_COMPONENTS> components __attribute__((uninitialized));
    size_t num_components = 0;

    char* p = path + 1;
    while (*p != '\0') {
        while (*p == '/') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        char const* comp_start = p;
        while (*p != '\0' && *p != '/') {
            p++;
        }

        char const SAVED = *p;
        *p = '\0';

        if (comp_start[0] == '.' && comp_start[1] == '\0') {
            // Skip ".".
        } else if (comp_start[0] == '.' && comp_start[1] == '.' && comp_start[2] == '\0') {
            if (num_components > 0) {
                num_components--;
            }
        } else {
            if (num_components >= components.size()) {
                return -ENAMETOOLONG;
            }
            components[num_components++] = comp_start;
        }

        if (SAVED == '/') {
            p++;
        }
    }

    std::array<char, MAX_MOUNT_PATH> result __attribute__((uninitialized));
    size_t pos = 0;
    result[pos++] = '/';

    for (size_t i = 0; i < num_components; ++i) {
        if (i > 0) {
            if (pos >= result.size() - 1) {
                return -ENAMETOOLONG;
            }
            result[pos++] = '/';
        }

        size_t const COMP_LEN = std::strlen(components[i]);
        if (pos + COMP_LEN >= result.size()) {
            return -ENAMETOOLONG;
        }

        std::memcpy(result.data() + pos, components[i], COMP_LEN);
        pos += COMP_LEN;
    }
    result[pos] = '\0';

    if (pos >= bufsize) {
        return -ENAMETOOLONG;
    }

    std::memcpy(path, result.data(), pos + 1);
    return 0;
}

auto apply_current_task_root_prefix(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }

    if (ker::mod::sched::can_query_current_task()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t const ROOT_LEN = std::strlen(task->root.data());
            if (ROOT_LEN > 1) {
                size_t const PATH_LEN = std::strlen(path);
                if (ROOT_LEN + PATH_LEN + 1 > outsize) {
                    return -ENAMETOOLONG;
                }

                if (out == path) {
                    std::memmove(out + ROOT_LEN, out, PATH_LEN + 1);
                } else {
                    std::memcpy(out + ROOT_LEN, path, PATH_LEN + 1);
                }
                std::memcpy(out, task->root.data(), ROOT_LEN);
                return 0;
            }
        }
    }

    return copy_mount_path_string(path, out, outsize);
}

auto mount_has_active_refs_locked(const MountPoint* mount, const PivotCoverState* prepared = nullptr) -> bool {
    if (mount == nullptr) return false;
    uint32_t internal_pins = 0;
    uint32_t prepared_refs = 0;
    for (auto* child : mounts) {
        auto* pin = child != nullptr ? child->covered_directory : nullptr;
        // Each publication gets an independent File. An external reference
        // makes this an active user again, preserving pivot's busy contract.
        if (pin != nullptr && pin->mount_owner == mount && pin->refcount.load(std::memory_order_acquire) == 1) ++internal_pins;
    }
    if (prepared != nullptr) {
        for (size_t index = 0; index < prepared->count; ++index) {
            if (prepared->entries[index].parent_hold.get() == mount) ++prepared_refs;
            auto* pin = prepared->entries[index].candidate;
            if (pin != nullptr && pin->mount_owner == mount && pin->refcount.load(std::memory_order_acquire) == 1) ++internal_pins;
        }
    }
    return mount->refs.load(std::memory_order_acquire) != prepared_refs ||
           mount->open_files.load(std::memory_order_acquire) != internal_pins || mount->retiring.load(std::memory_order_acquire);
}

auto retain_mount_locked(MountPoint* mount) -> bool {
    if (mount == nullptr || mount->path == nullptr || mount->retiring.load(std::memory_order_acquire)) {
        return false;
    }
    mount->refs.fetch_add(1, std::memory_order_acq_rel);
    return true;
}

void bump_mount_generation_locked() { mount_generation.fetch_add(1, std::memory_order_acq_rel); }

constexpr size_t MOUNT_INDEX_NONE = static_cast<size_t>(-1);

auto mount_index_locked(const MountPoint* mount) -> size_t {
    for (size_t i = 0; i < mounts.size(); ++i) {
        if (mounts.at(i) == mount) {
            return i;
        }
    }
    return MOUNT_INDEX_NONE;
}

auto mount_at_visible_index_locked(const mount_ns::NamespaceRef& scope, size_t index) -> MountPoint* {
    for (auto* mount : mounts) {
        if (mount == nullptr || mount->namespace_owner != scope) continue;
        if (index == 0) return mount;
        --index;
    }
    return nullptr;
}

auto mount_path_occupied_locked(const char* path, const mount_ns::NamespaceRef& scope) -> bool {
    if (path == nullptr) {
        return false;
    }
    for (auto* mount : mounts) {
        if (mount != nullptr && mount->namespace_owner == scope && mount->path != nullptr && std::strcmp(mount->path, path) == 0) {
            return true;
        }
    }
    return false;
}

auto mount_path_initializing_locked(const char* path, const mount_ns::NamespaceRef& scope) -> bool {
    if (path == nullptr) {
        return false;
    }
    for (const auto& initialization : mount_initializations) {
        if (initialization.active && initialization.namespace_owner == scope && std::strcmp(initialization.path.data(), path) == 0) {
            return true;
        }
    }
    return false;
}

// Mount-table topology only, not proof of native directory membership. Paths
// are canonical table/reservation keys, and every caller holds mount_lock.
auto mount_path_is_descendant(const char* child, const char* parent) -> bool {
    if (child == nullptr || parent == nullptr || parent[0] == '\0') return false;
    if (parent[0] == '/' && parent[1] == '\0') return child[0] == '/' && child[1] != '\0';
    auto const LENGTH = std::strlen(parent);
    return std::strncmp(child, parent, LENGTH) == 0 && child[LENGTH] == '/';
}

auto mount_path_has_descendants_locked(const char* path, const mount_ns::NamespaceRef& scope) -> bool {
    for (auto* mount : mounts)
        if (mount != nullptr && mount->namespace_owner == scope && mount_path_is_descendant(mount->path, path)) return true;
    for (const auto& initialization : mount_initializations)
        if (initialization.active && initialization.namespace_owner == scope && mount_path_is_descendant(initialization.path.data(), path))
            return true;
    return false;
}

auto mount_path_has_retiring_ancestor_locked(const char* path, const mount_ns::NamespaceRef& scope) -> bool {
    for (auto* mount : mounts)
        if (mount != nullptr && mount->namespace_owner == scope && mount->retiring.load(std::memory_order_acquire) &&
            mount_path_is_descendant(path, mount->path))
            return true;
    return false;
}

class MountInitializationReservation {
   public:
    MountInitializationReservation() = default;
    MountInitializationReservation(const MountInitializationReservation&) = delete;
    auto operator=(const MountInitializationReservation&) -> MountInitializationReservation& = delete;
    ~MountInitializationReservation() { reset(); }

    auto acquire(const char* path, uint64_t pivot_epoch, const mount_ns::NamespaceRef& scope = {}) -> int {
        if (path == nullptr || index_ != MOUNT_INDEX_NONE) {
            return -EINVAL;
        }
        size_t const PATH_LEN = std::strlen(path);
        if (PATH_LEN >= MOUNT_PATH_MAX) {
            return -ENAMETOOLONG;
        }

        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != pivot_epoch || scope.closed() || mount_path_occupied_locked(path, scope) ||
            mount_path_initializing_locked(path, scope) || mount_path_has_retiring_ancestor_locked(path, scope)) {
            mount_lock.unlock();
            return -EBUSY;
        }
        for (size_t i = 0; i < mount_initializations.size(); ++i) {
            auto& initialization = mount_initializations.at(i);
            if (initialization.active) {
                continue;
            }
            int const RETAIN = scope.retain(initialization.namespace_owner);
            if (RETAIN != 0) {
                mount_lock.unlock();
                return RETAIN;
            }
            std::memcpy(initialization.path.data(), path, PATH_LEN + 1);
            initialization.active = true;
            index_ = i;
            mount_lock.unlock();
            return 0;
        }
        mount_lock.unlock();
        return -EAGAIN;
    }

    auto active_locked() const -> bool { return index_ < mount_initializations.size() && mount_initializations.at(index_).active; }

    void commit_locked() {
        if (!active_locked()) {
            return;
        }
        auto& initialization = mount_initializations.at(index_);
        initialization.active = false;
        initialization.namespace_owner.reset();
        initialization.path.at(0) = '\0';
        index_ = MOUNT_INDEX_NONE;
    }

    void reset() {
        if (index_ == MOUNT_INDEX_NONE) {
            return;
        }
        mount_lock.lock();
        commit_locked();
        mount_lock.unlock();
    }

   private:
    size_t index_ = MOUNT_INDEX_NONE;
};

auto mount_lookup_known_path_len(const char* path, size_t known_path_len) -> size_t {
    if (known_path_len != UNKNOWN_MOUNT_PATH_LEN) {
        return known_path_len;
    }
    if (path == nullptr) {
        return UNKNOWN_MOUNT_PATH_LEN;
    }

    size_t path_len = 0;
    while (path_len < MOUNT_PATH_MAX && path[path_len] != '\0') {
        ++path_len;
    }
    return path_len < MOUNT_PATH_MAX ? path_len : UNKNOWN_MOUNT_PATH_LEN;
}

auto mount_lookup_hash(const char* path, size_t path_len) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < path_len; ++i) {
        hash ^= static_cast<unsigned char>(path[i]);
        hash *= 1099511628211ULL;
    }
    hash ^= path_len;
    hash *= 1099511628211ULL;
    return hash;
}

auto mount_is_root(const MountPoint* mount) -> bool {
    return mount != nullptr && mount->path != nullptr && mount->path_len == 1 && mount->path[0] == '/';
}

auto mount_first_component_len(const char* path, size_t path_len) -> size_t {
    if (path == nullptr || path_len < 2 || path[0] != '/') {
        return 0;
    }

    size_t end = 1;
    while (end < path_len && path[end] != '/') {
        ++end;
    }
    return end - 1;
}

auto mount_component_hash(const char* component, size_t component_len) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < component_len; ++i) {
        hash ^= static_cast<unsigned char>(component[i]);
        hash *= 1099511628211ULL;
    }
    hash ^= component_len;
    hash *= 1099511628211ULL;
    return hash == 0 ? 1 : hash;
}

auto mount_first_component_matches(const MountPoint* mount, const char* path, size_t component_len) -> bool {
    if (mount == nullptr || mount->path == nullptr || path == nullptr || component_len == 0 || mount_is_root(mount) ||
        mount->path_len < 2 || mount->path[0] != '/') {
        return false;
    }

    size_t mount_component_len = 0;
    for (size_t pos = 1; pos < mount->path_len && mount->path[pos] != '/'; ++pos) {
        ++mount_component_len;
    }
    return mount_component_len == component_len && std::memcmp(mount->path + 1, path + 1, component_len) == 0;
}

auto mount_path_matches(const MountPoint* mount, const char* path, size_t path_len, bool path_len_known) -> bool {
    if (mount == nullptr || mount->path == nullptr || path == nullptr) {
        return false;
    }

    size_t const MOUNT_LEN = mount->path_len;
    if (MOUNT_LEN == 0) {
        return false;
    }
    if (path_len_known && MOUNT_LEN > path_len) {
        return false;
    }

    if (MOUNT_LEN == 1 && mount->path[0] == '/') {
        return path[0] == '/';
    }

    if (path_len_known) {
        if (std::memcmp(path, mount->path, MOUNT_LEN) != 0) {
            return false;
        }
        return MOUNT_LEN == path_len || path[MOUNT_LEN] == '/';
    }

    return std::strncmp(path, mount->path, MOUNT_LEN) == 0 && (path[MOUNT_LEN] == '\0' || path[MOUNT_LEN] == '/');
}

auto mount_lookup_cache_get_retained(const char* path, size_t path_len, uint64_t pivot_epoch, const mount_ns::NamespaceRef& scope)
    -> MountPoint* {
    if (path == nullptr || path_len == 0 || path_len >= MOUNT_PATH_MAX) {
        return nullptr;
    }

    uint64_t const GENERATION = mount_generation.load(std::memory_order_acquire);
    uint64_t const HASH = mount_lookup_hash(path, path_len);
    auto& set = mount_lookup_cache.at(HASH & (MOUNT_LOOKUP_CACHE_SET_COUNT - 1));

    MountPoint* candidate = nullptr;
    set.lock.lock();
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.generation != GENERATION || entry.namespace_generation != scope.generation() || entry.hash != HASH ||
            entry.path_len != path_len) {
            continue;
        }
        if (std::memcmp(entry.path.data(), path, path_len) != 0) {
            continue;
        }
        entry.last_used = ++set.clock;
        candidate = entry.mount;
        break;
    }
    set.lock.unlock();

    if (candidate == nullptr) {
        return nullptr;
    }

    MountPoint* retained = nullptr;
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) == pivot_epoch &&
        mount_generation.load(std::memory_order_acquire) == GENERATION && !scope.closed() && candidate->namespace_owner == scope &&
        mount_path_matches(candidate, path, path_len, true) && retain_mount_locked(candidate)) {
        retained = candidate;
    }
    mount_lock.unlock();

#ifdef WOS_SELFTEST
    if (retained != nullptr) {
        mount_lookup_cache_hits.fetch_add(1, std::memory_order_relaxed);
    }
#endif

    return retained;
}

auto mount_root_fallback_cache_get_retained(const char* path, size_t component_len, uint64_t pivot_epoch,
                                            const mount_ns::NamespaceRef& scope) -> MountPoint* {
    if (path == nullptr || component_len == 0 || component_len >= MOUNT_ROOT_COMPONENT_CACHE_MAX || path[0] != '/') {
        return nullptr;
    }

    uint64_t const GENERATION = mount_generation.load(std::memory_order_acquire);
    uint64_t const HASH = mount_component_hash(path + 1, component_len);
    auto& set = mount_root_fallback_cache.at(HASH & (MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT - 1));

    MountPoint* candidate = nullptr;
    set.lock.lock();
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.generation != GENERATION || entry.namespace_generation != scope.generation() || entry.hash != HASH ||
            entry.component_len != component_len) {
            continue;
        }
        if (std::memcmp(entry.component.data(), path + 1, component_len) != 0) {
            continue;
        }
        entry.last_used = ++set.clock;
        candidate = entry.root_mount;
        break;
    }
    set.lock.unlock();

    if (candidate == nullptr) {
        return nullptr;
    }

    MountPoint* retained = nullptr;
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) == pivot_epoch &&
        mount_generation.load(std::memory_order_acquire) == GENERATION && !scope.closed() && candidate->namespace_owner == scope &&
        mount_is_root(candidate) && retain_mount_locked(candidate)) {
        retained = candidate;
    }
    mount_lock.unlock();
    return retained;
}

void mount_lookup_cache_store(const char* path, size_t path_len, MountPoint* mount, uint64_t generation) {
    if (path == nullptr || mount == nullptr || path_len == 0 || path_len >= MOUNT_PATH_MAX) {
        return;
    }

    uint64_t const HASH = mount_lookup_hash(path, path_len);
    auto& set = mount_lookup_cache.at(HASH & (MOUNT_LOOKUP_CACHE_SET_COUNT - 1));

    set.lock.lock();
    MountLookupCacheEntry* victim = nullptr;
    for (auto& entry : set.ways) {
        if (entry.valid && entry.generation == generation && entry.namespace_generation == mount->namespace_owner.generation() &&
            entry.hash == HASH && entry.path_len == path_len && std::memcmp(entry.path.data(), path, path_len) == 0) {
            entry.mount = mount;
            entry.last_used = ++set.clock;
            set.lock.unlock();
            return;
        }
        if (!entry.valid && victim == nullptr) {
            victim = &entry;
        }
    }

    if (victim == nullptr) {
        victim = &set.ways.at(0);
        for (auto& entry : set.ways) {
            if (entry.last_used < victim->last_used) {
                victim = &entry;
            }
        }
    }

    victim->valid = false;
    std::memcpy(victim->path.data(), path, path_len);
    victim->path.at(path_len) = '\0';
    victim->hash = HASH;
    victim->generation = generation;
    victim->last_used = ++set.clock;
    victim->mount = mount;
    victim->namespace_generation = mount->namespace_owner.generation();
    victim->path_len = path_len;
    victim->valid = true;
    set.lock.unlock();
}

void mount_root_fallback_cache_store(const char* path, size_t component_len, MountPoint* root_mount, uint64_t generation) {
    if (path == nullptr || path[0] != '/' || component_len == 0 || component_len >= MOUNT_ROOT_COMPONENT_CACHE_MAX ||
        !mount_is_root(root_mount)) {
        return;
    }

    uint64_t const HASH = mount_component_hash(path + 1, component_len);
    auto& set = mount_root_fallback_cache.at(HASH & (MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT - 1));

    set.lock.lock();
    MountRootFallbackCacheEntry* victim = nullptr;
    for (auto& entry : set.ways) {
        if (entry.valid && entry.generation == generation && entry.namespace_generation == root_mount->namespace_owner.generation() &&
            entry.hash == HASH && entry.component_len == component_len &&
            std::memcmp(entry.component.data(), path + 1, component_len) == 0) {
            entry.root_mount = root_mount;
            entry.last_used = ++set.clock;
            set.lock.unlock();
            return;
        }
        if (!entry.valid && victim == nullptr) {
            victim = &entry;
        }
    }

    if (victim == nullptr) {
        victim = &set.ways.at(0);
        for (auto& entry : set.ways) {
            if (entry.last_used < victim->last_used) {
                victim = &entry;
            }
        }
    }

    victim->valid = false;
    std::memcpy(victim->component.data(), path + 1, component_len);
    victim->component.at(component_len) = '\0';
    victim->hash = HASH;
    victim->generation = generation;
    victim->last_used = ++set.clock;
    victim->root_mount = root_mount;
    victim->namespace_generation = root_mount->namespace_owner.generation();
    victim->component_len = component_len;
    victim->valid = true;
    set.lock.unlock();
}

auto destroy_mount_private_data(MountPoint* mount, bool force) -> int {
    if (mount == nullptr || mount->private_data == nullptr) {
        return 0;
    }

    switch (mount->fs_type) {
        case FSType::FAT32: {
            int const RET = ker::vfs::fat32::fat32_unmount(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            if (RET != 0) return RET;
            break;
        }
        case FSType::XFS: {
            auto* context = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
            if (force) {
                ker::vfs::xfs::xfs_unmount_force(context);
            } else {
                int const RET = ker::vfs::xfs::xfs_unmount(context);
                if (RET != 0) {
                    return RET;
                }
            }
            break;
        }
        case FSType::TMPFS:
            ker::vfs::tmpfs::destroy_mount_context(static_cast<ker::vfs::tmpfs::TmpfsMount*>(mount->private_data));
            break;
        default:
            break;
    }
    mount->private_data = nullptr;
    return 0;
}

void destroy_mount_storage(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    // The row is already unpublished (or was never published). Its native
    // backend has closed; drain the parent pin with all registry locks free.
    if (mount->covered_directory != nullptr) vfs_put_file(mount->covered_directory);
    delete[] mount->path;
    delete[] mount->fstype;
    delete mount;
}

auto destroy_mount(MountPoint* mount, bool force = false) -> int {
    if (mount == nullptr) {
        return 0;
    }
    int const RET = destroy_mount_private_data(mount, force);
    if (RET != 0) {
        return RET;
    }
    destroy_mount_storage(mount);
    return 0;
}

struct PreparedMountState {
    MountPoint* mount{};
    MountInitializationReservation reservation{};
    uint64_t pivot_epoch{};
    size_t mount_count_after_insert{};
    bool published{};
};

void wait_for_mount_refs_to_drain(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    while (mount->refs.load(std::memory_order_acquire) != 0) {
        if (ker::mod::sched::can_query_current_task()) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }
}

auto wait_for_mount_refs_to_drain_bounded(MountPoint* mount) -> bool {
    if (mount == nullptr) {
        return true;
    }
    for (uint32_t attempt = 0; attempt < SHUTDOWN_UNMOUNT_DRAIN_YIELDS; ++attempt) {
        if (mount->refs.load(std::memory_order_acquire) == 0) {
            return true;
        }
        if (ker::mod::sched::can_query_current_task()) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }
    return mount->refs.load(std::memory_order_acquire) == 0;
}

void rollback_mount_retirement(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    mount_lock.lock();
    if (mount_index_locked(mount) != MOUNT_INDEX_NONE && mount->retiring.load(std::memory_order_acquire)) {
        mount->retiring.store(false, std::memory_order_release);
        bump_mount_generation_locked();
    }
    mount_lock.unlock();
}

auto complete_retired_mount_teardown(MountPoint* mount, bool bounded_ref_wait, size_t* mount_count_after_remove, bool wait_for_refs = true)
    -> int {
    if (mount == nullptr) {
        return -EINVAL;
    }

    bool refs_drained = true;
    if (!wait_for_refs) {
        refs_drained = mount->refs.load(std::memory_order_acquire) == 0;
    } else if (bounded_ref_wait) {
        refs_drained = wait_for_mount_refs_to_drain_bounded(mount);
    } else {
        wait_for_mount_refs_to_drain(mount);
    }
    if (!refs_drained) {
        log::warn("unmount busy: path=%s refs=%u", mount->path != nullptr ? mount->path : "?", mount->refs.load(std::memory_order_acquire));
        rollback_mount_retirement(mount);
        return -EBUSY;
    }

    mount_lock.lock();
    size_t const INDEX = mount_index_locked(mount);
    if (INDEX == MOUNT_INDEX_NONE || !mount->retiring.load(std::memory_order_acquire)) {
        mount_lock.unlock();
        return -ENOENT;
    }
    // Shutdown marks a whole set before leaf-first teardown. A child that
    // remained busy (or a still-initializing child) must keep this backend live.
    if (mount_path_has_descendants_locked(mount->path, mount->namespace_owner)) {
        mount->retiring.store(false, std::memory_order_release);
        bump_mount_generation_locked();
        mount_lock.unlock();
        return -EBUSY;
    }
    uint32_t const OPEN_FILES = mount->open_files.load(std::memory_order_acquire);
    if (OPEN_FILES != 0) {
        mount->retiring.store(false, std::memory_order_release);
        bump_mount_generation_locked();
        mount_lock.unlock();
        log::warn("unmount busy: path=%s open_files=%u", mount->path != nullptr ? mount->path : "?", OPEN_FILES);
        return -EBUSY;
    }
    mount_lock.unlock();

    // The mount stays published as an unretainable tombstone while backend
    // closeout performs blocking I/O. A failed XFS teardown leaves its
    // context live, so reopening publication makes the failure retryable.
    // Pin before clearing private_data: a concurrent WKI unmount must still
    // see this retiring row and return EBUSY, rather than free its owner on
    // ENOENT. Busy File/ref checks above leave the group entirely unchanged.
    auto* remote_owner = mount->fs_type == FSType::REMOTE ? ker::net::wki::wki_remote_vfs_pin_mount_teardown(mount->private_data) : nullptr;
    int const DESTROY_RET = destroy_mount_private_data(mount, false);
    if (DESTROY_RET != 0) {
        ker::net::wki::wki_remote_vfs_finish_mount_teardown(remote_owner, false);
        rollback_mount_retirement(mount);
        return DESTROY_RET;
    }

    mount_lock.lock();
    size_t const REMOVE_INDEX = mount_index_locked(mount);
    if (REMOVE_INDEX == MOUNT_INDEX_NONE) {
        mount_lock.unlock();
        ker::net::wki::wki_remote_vfs_finish_mount_teardown(remote_owner, false);
        log::warn("unmount lost retiring mount after backend teardown");
        return -EIO;
    }
    mounts.remove_at(REMOVE_INDEX);
    bump_mount_generation_locked();
    if (mount_count_after_remove != nullptr) {
        *mount_count_after_remove = mounts.size();
    }
    mount_lock.unlock();

    destroy_mount_storage(mount);
    ker::net::wki::wki_remote_vfs_finish_mount_teardown(remote_owner, true);
    return 0;
}

auto sync_mount_for_shutdown(MountPoint* mount) -> int {
    if (mount == nullptr) {
        return -EINVAL;
    }
    switch (mount->fs_type) {
        case FSType::FAT32:
            return ker::vfs::fat32::fat32_sync_mount(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
        case FSType::XFS:
            return ker::vfs::xfs::xfs_sync_mount(static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
        case FSType::TMPFS:
        case FSType::DEVFS:
        case FSType::PROCFS:
        case FSType::REMOTE:
        default:
            return 0;
    }
}

auto is_shutdown_root_mount(const MountPoint* mount, const char* root_path) -> bool {
    return mount != nullptr && mount->path != nullptr && root_path != nullptr && std::strcmp(mount->path, root_path) == 0;
}

auto mount_path_depth(const MountPoint* mount) -> size_t {
    if (mount == nullptr || mount->path == nullptr) {
        return 0;
    }
    size_t depth = 0;
    for (const char* p = mount->path; *p != '\0'; ++p) {
        if (*p == '/') {
            ++depth;
        }
    }
    return depth;
}

auto mount_should_come_after(const MountPoint* lhs, const MountPoint* rhs, const char* root_path) -> bool {
    bool const LHS_ROOT = is_shutdown_root_mount(lhs, root_path);
    bool const RHS_ROOT = is_shutdown_root_mount(rhs, root_path);
    if (LHS_ROOT != RHS_ROOT) {
        return LHS_ROOT;
    }

    size_t const LHS_DEPTH = mount_path_depth(lhs);
    size_t const RHS_DEPTH = mount_path_depth(rhs);
    if (LHS_DEPTH != RHS_DEPTH) {
        return LHS_DEPTH < RHS_DEPTH;
    }

    size_t const LHS_LEN = lhs != nullptr && lhs->path != nullptr ? lhs->path_len : 0;
    size_t const RHS_LEN = rhs != nullptr && rhs->path != nullptr ? rhs->path_len : 0;
    return LHS_LEN < RHS_LEN;
}

void sort_shutdown_mounts(MountPoint** mounts_to_unmount, size_t count, const char* root_path) {
    if (mounts_to_unmount == nullptr) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = i + 1; j < count; ++j) {
            if (mount_should_come_after(mounts_to_unmount[i], mounts_to_unmount[j], root_path)) {
                auto* tmp = mounts_to_unmount[i];
                mounts_to_unmount[i] = mounts_to_unmount[j];
                mounts_to_unmount[j] = tmp;
            }
        }
    }
}

}  // namespace

void put_mount_point(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    uint32_t const OLD_REFS = mount->refs.fetch_sub(1, std::memory_order_acq_rel);
    if (OLD_REFS == 0) {
        mount->refs.store(0, std::memory_order_release);
    }
}

auto retain_mount_point(MountPoint* mount) -> MountRef {
    mount_lock.lock();
    MountPoint* const RETAINED = mount_index_locked(mount) != MOUNT_INDEX_NONE && retain_mount_locked(mount) ? mount : nullptr;
    mount_lock.unlock();
    return MountRef{RETAINED};
}

auto retain_mount_by_private_data(const void* private_data) -> MountRef {
    if (private_data == nullptr) return {};
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        MountPoint* retained = nullptr;
        for (auto* mount : mounts) {
            if (mount != nullptr && !mount->retiring.load(std::memory_order_acquire) && mount->private_data == private_data &&
                retain_mount_locked(mount)) {
                retained = mount;
                break;
            }
        }
        mount_lock.unlock();
        return MountRef{retained};
    }
}

auto retain_remote_mount_descendant_at(const void* parent_private_data, size_t index, size_t* path_length) -> MountRef {
    if (parent_private_data == nullptr || path_length == nullptr) return {};
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        MountPoint* parent = nullptr;
        for (auto* mount : mounts) {
            if (mount != nullptr && mount->fs_type == FSType::REMOTE && !mount->retiring.load(std::memory_order_acquire) &&
                mount->private_data == parent_private_data) {
                parent = mount;
                break;
            }
        }
        MountPoint* child = parent != nullptr ? mount_at_visible_index_locked(parent->namespace_owner, index) : nullptr;
        MountPoint* retained = nullptr;
        if (parent != nullptr && child != nullptr && parent->path != nullptr && child->path != nullptr &&
            child->namespace_owner == parent->namespace_owner && child->fs_type == FSType::REMOTE && child->path_len > parent->path_len &&
            std::memcmp(child->path, parent->path, parent->path_len) == 0 &&
            (parent->path_len == 1 || child->path[parent->path_len] == '/') && retain_mount_locked(child)) {
            retained = child;
            *path_length = child->path_len;
        }
        mount_lock.unlock();
        return MountRef{retained};
    }
}

auto retain_mount_for_open_file(MountPoint* mount) -> bool {
    if (mount == nullptr) {
        return false;
    }

    mount_lock.lock();
    bool const RETAINED =
        mount_index_locked(mount) != MOUNT_INDEX_NONE && mount->path != nullptr && !mount->retiring.load(std::memory_order_acquire);
    if (RETAINED) {
        mount->open_files.fetch_add(1, std::memory_order_acq_rel);
    }
    mount_lock.unlock();
    return RETAINED;
}

void release_mount_from_open_file(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    mount_lock.lock();
    uint32_t const OPEN_FILES = mount->open_files.load(std::memory_order_acquire);
    if (OPEN_FILES != 0) {
        mount->open_files.store(OPEN_FILES - 1, std::memory_order_release);
    } else {
        log::warn("open-file mount pin released twice: path=%s", mount->path != nullptr ? mount->path : "?");
    }
    mount_lock.unlock();
}

auto MountRef::operator=(MountRef&& other) noexcept -> MountRef& {
    if (this != &other) {
        put_mount_point(mount_);
        mount_ = other.mount_;
        other.mount_ = nullptr;
    }
    return *this;
}

MountRef::~MountRef() { reset(); }

void MountRef::reset(MountPoint* mount) {
    if (mount_ == mount) {
        return;
    }
    put_mount_point(mount_);
    mount_ = mount;
}

#ifdef WOS_SELFTEST
auto mount_point_ref_count_for_test(const MountPoint* mount) -> uint32_t {
    if (mount == nullptr) {
        return 0;
    }
    return mount->refs.load(std::memory_order_acquire);
}

auto mount_point_open_file_count_for_test(const MountPoint* mount) -> uint32_t {
    if (mount == nullptr) {
        return 0;
    }
    return mount->open_files.load(std::memory_order_acquire);
}

void mount_lookup_cache_reset_for_test() {
    for (auto& set : mount_lookup_cache) {
        set.lock.lock();
        set.clock = 0;
        for (auto& entry : set.ways) {
            entry.valid = false;
            entry.mount = nullptr;
            entry.path_len = 0;
            entry.hash = 0;
            entry.generation = 0;
            entry.last_used = 0;
        }
        set.lock.unlock();
    }
    for (auto& set : mount_root_fallback_cache) {
        set.lock.lock();
        set.clock = 0;
        for (auto& entry : set.ways) {
            entry.valid = false;
            entry.root_mount = nullptr;
            entry.component_len = 0;
            entry.hash = 0;
            entry.generation = 0;
            entry.last_used = 0;
        }
        set.lock.unlock();
    }
    mount_lookup_cache_hits.store(0, std::memory_order_relaxed);
}

auto mount_lookup_cache_hits_for_test() -> uint64_t { return mount_lookup_cache_hits.load(std::memory_order_relaxed); }

auto mount_lookup_calls_for_test() -> uint64_t { return mount_lookup_calls.load(std::memory_order_relaxed); }
#endif

// Resolve path like resolve_task_path_raw does for mount-table keys:
// first make relative targets absolute against cwd, then canonicalize, then
// apply the current task's root prefix. After pivot_root("/rootfs", ...),
// "mnt" from cwd "/root" becomes "/rootfs/root/mnt".
auto resolve_mount_path(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }

    std::array<char, MAX_MOUNT_PATH> logical __attribute__((uninitialized));
    int result = make_mount_path_absolute(path, logical.data(), logical.size());
    if (result < 0) {
        return result;
    }

    result = canonicalize_mount_path(logical.data(), logical.size());
    if (result < 0) {
        return result;
    }

    return apply_current_task_root_prefix(logical.data(), out, outsize);
}

auto fstype_to_enum(const char* fstype) -> FSType {
    if (fstype == nullptr) {
        return FSType::TMPFS;
    }
    if (std::strcmp(fstype, "fat32") == 0 || std::strcmp(fstype, "vfat") == 0) {
        return FSType::FAT32;
    }
    if (std::strcmp(fstype, "devfs") == 0) {
        return FSType::DEVFS;
    }
    if (std::strcmp(fstype, "remote") == 0) {
        return FSType::REMOTE;
    }
    if (std::strcmp(fstype, "procfs") == 0) {
        return FSType::PROCFS;
    }
    if (std::strcmp(fstype, "xfs") == 0) {
        return FSType::XFS;
    }
    return FSType::TMPFS;
}

auto mounted_block_device_overlaps(const ker::dev::BlockDevice* device) -> bool {
    if (device == nullptr) {
        return false;
    }

    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        for (auto* mount : mounts) {
            if (mount != nullptr && mount->device != nullptr && ker::dev::block_devices_overlap(mount->device, device)) {
                mount_lock.unlock();
                return true;
            }
        }
        mount_lock.unlock();
        return false;
    }
}

auto prepare_mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device, unsigned long flags, const char* data,
                              void* initial_private_data, FileOperations* initial_fops, PreparedMount* out, MountPathKind path_kind,
                              const mount_ns::NamespaceRef& scope) -> int {
    if (path == nullptr || fstype == nullptr || out == nullptr || out->state != nullptr) {
        vfs_debug_log("mount_filesystem: invalid arguments\n");
        return -EINVAL;
    }
    if (ker::mod::power::shutdown_in_progress()) {
        vfs_debug_log("mount_filesystem: shutdown in progress\n");
        return -ESHUTDOWN;
    }

    if (scope.closed()) return -EIDRM;
    if (!scope.is_host()) {
        // Device-backed/shared backends need separate admission. Native procfs
        // captures the requester's PID view in each File and owns no device.
        bool const NATIVE = std::strcmp(fstype, "tmpfs") == 0 || std::strcmp(fstype, "procfs") == 0;
        if (path_kind != MountPathKind::RETAINED_PHYSICAL || !NATIVE) return -ENOTSUP;
        if (device != nullptr) return -EINVAL;
    }

    uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();

    // Public mount already admitted a physical path. Never reinterpret it
    // through a later ambient root/cwd observation during preparation.
    std::array<char, MAX_MOUNT_PATH> resolved __attribute__((uninitialized));
    if (path_kind == MountPathKind::RETAINED_PHYSICAL && path[0] != '/') {
        return -EINVAL;
    }
    int const PATH_RET = path_kind == MountPathKind::RETAINED_PHYSICAL ? copy_mount_path_string(path, resolved.data(), resolved.size())
                                                                       : resolve_mount_path(path, resolved.data(), resolved.size());
    if (PATH_RET < 0) {
        vfs_debug_log("mount_filesystem: path resolution failed\n");
        return PATH_RET;
    }

    // Reject an existing or retiring exact-path mount before filesystem
    // initialization. Retiring mounts remain in the table specifically so a
    // replacement cannot run recovery against their still-owned storage.
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
        mount_lock.unlock();
        return -EBUSY;
    }
    if (scope.closed() || mount_path_occupied_locked(resolved.data(), scope) || mount_path_initializing_locked(resolved.data(), scope) ||
        mount_path_has_retiring_ancestor_locked(resolved.data(), scope)) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: mount path already occupied\n");
        return -EBUSY;
    }
    mount_lock.unlock();

    auto* state = new (std::nothrow) PreparedMountState;
    if (state == nullptr) {
        return -ENOMEM;
    }
    auto* mount = new (std::nothrow) MountPoint;
    if (mount == nullptr) {
        delete state;
        return -ENOMEM;
    }
    state->mount = mount;
    state->pivot_epoch = PIVOT_EPOCH;
    int const RETAIN_SCOPE = scope.retain(mount->namespace_owner);
    if (RETAIN_SCOPE != 0) {
        delete mount;
        delete state;
        return RETAIN_SCOPE;
    }

    // Copy resolved path and fstype into kernel heap.
    size_t const PATH_LEN = std::strlen(resolved.data());
    auto* path_copy = new char[PATH_LEN + 1];
    if (path_copy == nullptr) {
        static_cast<void>(destroy_mount(mount, true));
        state->mount = nullptr;
        delete state;
        return -ENOMEM;
    }
    std::memcpy(path_copy, resolved.data(), PATH_LEN + 1);
    mount->path = path_copy;
    mount->path_len = PATH_LEN;

    int const RESERVATION_RET = state->reservation.acquire(mount->path, PIVOT_EPOCH, scope);
    if (RESERVATION_RET != 0) {
        static_cast<void>(destroy_mount(mount, true));
        state->mount = nullptr;
        delete state;
        return RESERVATION_RET;
    }

    size_t const FSTYPE_LEN = std::strlen(fstype);
    auto* fstype_copy = new char[FSTYPE_LEN + 1];
    if (fstype_copy == nullptr) {
        static_cast<void>(destroy_mount(mount, true));
        state->mount = nullptr;
        delete state;
        return -ENOMEM;
    }
    std::memcpy(fstype_copy, fstype, FSTYPE_LEN + 1);
    mount->fstype = fstype_copy;

    mount->fs_type = fstype_to_enum(fstype);
    mount->device = device;
    mount->private_data = nullptr;
    mount->fops = nullptr;
    mount->read_only = (flags & MOUNT_FLAG_RDONLY) != 0 || (device != nullptr && ker::dev::block_device_is_read_only(device));
    if (!scope.is_host() && mount->fs_type == FSType::PROCFS) mount->read_only = true;
    mount->nosuid = (flags & MOUNT_FLAG_NOSUID) != 0;

    bool const BLOCK_RW_FS = mount->fs_type == FSType::FAT32 || mount->fs_type == FSType::XFS;
    if (BLOCK_RW_FS && device != nullptr && !mount->read_only &&
        !mount->block_writer_lease.try_acquire(device, ker::dev::BlockWriterLeaseOwner::LOCAL_MOUNT)) {
        vfs_debug_log("mount_filesystem: block device has a remote writer lease\n");
        static_cast<void>(destroy_mount(mount, true));
        state->mount = nullptr;
        delete state;
        return -EBUSY;
    }

    // Initialize the appropriate filesystem
    if (std::strcmp(fstype, "fat32") == 0 || std::strcmp(fstype, "vfat") == 0) {
        // FAT32 filesystem
        if (device == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 requires a block device\n");
            static_cast<void>(destroy_mount(mount, true));
            state->mount = nullptr;
            delete state;
            return -EINVAL;
        }

        // Determine partition start LBA.
        // If the device is already a partition, the offset is baked into its read/write ops.
        // If it's a whole disk, scan the GPT for a FAT32 partition.
        uint64_t partition_start_lba = 0;
        if (device->is_partition) {
            // Partition device: I/O already offset, FAT32 starts at LBA 0 relative to partition
            partition_start_lba = 0;
        } else {
            partition_start_lba = ker::dev::gpt::gpt_find_fat32_partition(device);
            if (partition_start_lba == 0) {
                vfs_debug_log("mount_filesystem: No FAT32 partition found (assuming raw FAT32 at LBA 0)\n");
            }
        }

        // Initialize FAT32 with the device and partition offset
        auto* context = ker::vfs::fat32::fat32_init_device(device, partition_start_lba, mount->read_only);
        if (context == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 initialization failed\n");
            static_cast<void>(destroy_mount(mount, true));
            state->mount = nullptr;
            delete state;
            return -EIO;
        }

        // Store mount context for per-mount use
        mount->private_data = context;

        mount->fops = ker::vfs::fat32::get_fat32_fops();
    } else if (std::strcmp(fstype, "tmpfs") == 0) {
        // tmpfs filesystem
        bool const ROOT_COMPAT = scope.is_host() && std::strcmp(resolved.data(), "/") == 0;
        auto* root = ROOT_COMPAT ? ker::vfs::tmpfs::get_root_node() : ker::vfs::tmpfs::create_root_node();
        int tmpfs_error = 0;
        mount->private_data = ker::vfs::tmpfs::create_mount_context(root, data, ROOT_COMPAT, &tmpfs_error);
        if (mount->private_data == nullptr) {
            vfs_debug_log("mount_filesystem: tmpfs root allocation failed\n");
            if (!ROOT_COMPAT) {
                ker::vfs::tmpfs::tmpfs_free_node(root);
            }
            static_cast<void>(destroy_mount(mount, true));
            state->mount = nullptr;
            delete state;
            return tmpfs_error != 0 ? tmpfs_error : -ENOMEM;
        }
        mount->fops = ker::vfs::tmpfs::get_tmpfs_fops();
    } else if (std::strcmp(fstype, "devfs") == 0) {
        // devfs filesystem
        mount->fops = ker::vfs::devfs::get_devfs_fops();
    } else if (std::strcmp(fstype, "remote") == 0) {
        // Publish remote ownership together with the mount-table row. A
        // deferred withdrawal can otherwise configure or retire a different
        // same-path mount in the gap after this function returns.
        mount->private_data = initial_private_data;
        mount->fops = initial_fops;
    } else if (std::strcmp(fstype, "procfs") == 0) {
        mount->fops = ker::vfs::procfs::get_procfs_fops();
    } else if (std::strcmp(fstype, "xfs") == 0) {
        // XFS filesystem
        if (device == nullptr) {
            vfs_debug_log("mount_filesystem: XFS requires a block device\n");
            static_cast<void>(destroy_mount(mount, true));
            state->mount = nullptr;
            delete state;
            return -EINVAL;
        }
        auto* xfs_ctx = ker::vfs::xfs::xfs_vfs_init_device(device, mount->read_only);
        if (xfs_ctx == nullptr) {
            vfs_debug_log("mount_filesystem: XFS initialization failed\n");
            static_cast<void>(destroy_mount(mount, true));
            state->mount = nullptr;
            delete state;
            return -EIO;
        }
        mount->private_data = xfs_ctx;
        mount->fops = ker::vfs::xfs::get_xfs_fops();
    } else {
        vfs_debug_log("mount_filesystem: unknown filesystem type\n");
        static_cast<void>(destroy_mount(mount, true));
        state->mount = nullptr;
        delete state;
        return -ENODEV;
    }

    out->state = state;
    return 0;
}

namespace {
auto publish_prepared_mount_with_identity(PreparedMount* prepared, const char* expected_physical_path, File* covered_directory,
                                          uint32_t& next_identity) -> int {
    if (prepared == nullptr || prepared->state == nullptr) {
        return -EINVAL;
    }
    auto* state = static_cast<PreparedMountState*>(prepared->state);
    MountPoint* const mount = state->mount;
    if (mount == nullptr || state->published) {
        return -EINVAL;
    }
    if (expected_physical_path != nullptr && std::strcmp(mount->path, expected_physical_path) != 0) {
        return -EAGAIN;
    }

    mount_lock.lock();
    if (covered_directory != nullptr) {
        auto* parent = covered_directory->mount_owner;
        // The producer owns a fresh detached native pin from its admitted
        // target lookup. Refuse sharing a File between mount relationships.
        if (parent == nullptr || parent->namespace_owner != mount->namespace_owner || mount_index_locked(parent) == MOUNT_INDEX_NONE ||
            parent->retiring.load(std::memory_order_acquire) || covered_directory->refcount.load(std::memory_order_acquire) != 1 ||
            !covered_directory->is_directory || covered_directory->private_data == nullptr || covered_directory->directory_view ||
            covered_directory->vfs_path != nullptr || (covered_directory->open_flags & 3) != FILE_ACCESS_SEARCH_ONLY ||
            (covered_directory->open_flags & O_DIRECTORY) == 0 || covered_directory->fs_type != parent->fs_type ||
            (parent->fs_type != FSType::TMPFS && parent->fs_type != FSType::XFS) || !mount_path_is_descendant(mount->path, parent->path)) {
            mount_lock.unlock();
            return -EINVAL;
        }
    }
    if (mount_pivot_epoch.load(std::memory_order_acquire) != state->pivot_epoch) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: pivot namespace changed before publication\n");
        return -EBUSY;
    }
    if (mount->namespace_owner.closed()) {
        mount_lock.unlock();
        return -EIDRM;
    }
    if (!state->reservation.active_locked() || mount_path_occupied_locked(mount->path, mount->namespace_owner) ||
        mount_path_has_retiring_ancestor_locked(mount->path, mount->namespace_owner)) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: mount path already occupied before publication\n");
        return -EBUSY;
    }
    if (!mount->namespace_owner.is_host()) {
        size_t count = 0;
        for (auto* existing : mounts)
            if (existing != nullptr && existing->namespace_owner == mount->namespace_owner) ++count;
        if (count >= mount_ns::MAX_PRIVATE_MOUNTS) {
            mount_lock.unlock();
            return -ENOSPC;
        }
    }
    if (next_identity == 0) {
        mount_lock.unlock();
        return -EOVERFLOW;
    }
    if (!mounts.push_back(mount)) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: mount table full (OOM)\n");
        return -ENOMEM;
    }
    if (covered_directory != nullptr) {
        vfs_retain_file(covered_directory);
        mount->covered_directory = covered_directory;
    }
    mount->dev_id = next_identity;
    next_identity = next_identity == UINT32_MAX ? 0 : next_identity + 1;
    if (mount->fs_type == FSType::XFS && mount->private_data != nullptr) {
        static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data)->dev_id = mount->dev_id;
    }
    state->reservation.commit_locked();
    bump_mount_generation_locked();
    state->mount_count_after_insert = mounts.size();
    state->published = true;
    mount_lock.unlock();

    return 0;
}
}  // namespace

auto publish_prepared_mount(PreparedMount* prepared, const char* expected_physical_path, File* covered_directory) -> int {
    return publish_prepared_mount_with_identity(prepared, expected_physical_path, covered_directory, next_dev_id);
}

void finish_prepared_mount(PreparedMount* prepared) {
    if (prepared == nullptr || prepared->state == nullptr) {
        return;
    }
    auto* state = static_cast<PreparedMountState*>(prepared->state);
    prepared->state = nullptr;
    MountPoint* const mount = state->mount;
    if (!state->published) {
        static_cast<void>(destroy_mount(mount, true));
        state->mount = nullptr;
        delete state;
        return;
    }

    vfs_debug_log("mount_filesystem: mounted ");
    vfs_debug_log(mount->fstype);
    vfs_debug_log(" at ");
    vfs_debug_log(mount->path);
    vfs_debug_log("\n");

    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(state->mount_count_after_insert), 0, 0);

    // Emit WKI storage mount event (if WKI is active)
    if (mount->namespace_owner.is_host() && ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_MOUNT, mount->path,
                                         static_cast<uint16_t>(mount->path_len + 1));
        ker::net::wki::wki_resource_advertise_all();
    }

    state->mount = nullptr;
    delete state;
}

auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device, unsigned long flags, const char* data,
                      void* initial_private_data, FileOperations* initial_fops, const mount_ns::NamespaceRef& scope) -> int {
    PreparedMount prepared{};
    int const PREPARE = prepare_mount_filesystem(path, fstype, device, flags, data, initial_private_data, initial_fops, &prepared,
                                                 scope.is_host() ? MountPathKind::TASK_VISIBLE : MountPathKind::RETAINED_PHYSICAL, scope);
    if (PREPARE != 0) {
        return PREPARE;
    }
    int const PUBLISH = publish_prepared_mount(&prepared);
    finish_prepared_mount(&prepared);
    return PUBLISH;
}

namespace {
auto unmount_filesystem_impl(const char* path, const void* expected_private_data, bool require_private_data_match,
                             uint32_t expected_dev_id = 0, bool require_dev_id_match = false, const mount_ns::NamespaceRef& scope = {},
                             MountPathKind path_kind = MountPathKind::TASK_VISIBLE) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    MountPoint* retiring_mount = nullptr;
    size_t mount_count_after_remove = 0;
    while (retiring_mount == nullptr) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();

        // Resolve again after every pivot retry so the task-root prefix and
        // stored mount namespace stay in the same publication epoch.
        std::array<char, MAX_MOUNT_PATH> resolved __attribute__((uninitialized));
        int const PATH_RET = scope.is_host() && path_kind == MountPathKind::TASK_VISIBLE
                                 ? resolve_mount_path(path, resolved.data(), resolved.size())
                                 : (path[0] == '/' ? copy_mount_path_string(path, resolved.data(), resolved.size()) : -EINVAL);
        if (PATH_RET < 0) {
            return PATH_RET;
        }

        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        if (scope.closed()) {
            mount_lock.unlock();
            return -EIDRM;
        }
        for (size_t i = 0; i < mounts.size(); ++i) {
            MountPoint* mp = mounts.at(i);
            bool const PATH_MATCHES =
                mp != nullptr && mp->namespace_owner == scope && mp->path != nullptr && std::strcmp(resolved.data(), mp->path) == 0;
            bool const OWNER_MATCHES = !require_private_data_match || (mp != nullptr && mp->private_data == expected_private_data);
            bool const DEV_ID_MATCHES = !require_dev_id_match || (mp != nullptr && mp->dev_id == expected_dev_id);
            if (!PATH_MATCHES || !OWNER_MATCHES || !DEV_ID_MATCHES) {
                continue;
            }
            if (mp->retiring.load(std::memory_order_acquire) || mount_path_has_descendants_locked(mp->path, mp->namespace_owner)) {
                mount_lock.unlock();
                return -EBUSY;
            }
            mp->retiring.store(true, std::memory_order_release);
            bump_mount_generation_locked();
            retiring_mount = mp;
            break;
        }
        mount_lock.unlock();
        if (retiring_mount == nullptr) {
            return -ENOENT;
        }
    }

    int const UNMOUNT_RET = complete_retired_mount_teardown(retiring_mount, false, &mount_count_after_remove);
    if (UNMOUNT_RET != 0) {
        return UNMOUNT_RET;
    }

    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(mount_count_after_remove), 0, 0);

    vfs_debug_log("unmount_filesystem: unmounted ");
    vfs_debug_log(path);
    vfs_debug_log("\n");

    // Emit WKI storage unmount event (if WKI is active)
    if (scope.is_host() && ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_UNMOUNT, path,
                                         static_cast<uint16_t>(std::strlen(path) + 1));
        ker::net::wki::wki_resource_advertise_all();
    }

    return 0;
}
}  // namespace

auto unmount_filesystem(const char* path, const mount_ns::NamespaceRef& scope) -> int {
    return unmount_filesystem_impl(path, nullptr, false, 0, false, scope);
}

namespace {
auto retire_mount_namespace_impl(const mount_ns::NamespaceRef& scope, bool single_step) -> int {
    if (scope.is_host()) return -EINVAL;
    // Keep the exact owner pinned across unlocked backend teardown. The caller
    // also owns scope; this API never revokes explicitly retained Files.
    mount_ns::NamespaceRef owner;
    int const RETAIN = scope.retain(owner);
    if (RETAIN != 0) return RETAIN;
    while (true) {
        auto const EPOCH = single_step ? mount_pivot_epoch.load(std::memory_order_acquire) : wait_for_stable_mount_pivot_epoch();
        if (single_step && (EPOCH & 1U) != 0) return -EBUSY;
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != EPOCH) {
            mount_lock.unlock();
            if (single_step) return -EBUSY;
            continue;
        }
        owner.close();
        MountPoint* leaf = nullptr;
        for (const auto& initialization : mount_initializations) {
            if (initialization.active && initialization.namespace_owner == owner) {
                mount_lock.unlock();
                return -EBUSY;
            }
        }
        for (auto* mount : mounts) {
            if (mount == nullptr || mount->namespace_owner != owner) continue;
            if (mount->retiring.load(std::memory_order_acquire)) {
                mount_lock.unlock();
                return -EBUSY;
            }
            // '/' and '/child' both contain one slash. Reuse shutdown's
            // root-last, depth/length ordering rather than counting slashes
            // alone, which would try to retire the root before its child.
            if (leaf == nullptr || mount_should_come_after(leaf, mount, "/")) leaf = mount;
        }
        if (leaf == nullptr) {
            mount_lock.unlock();
            return 0;
        }
        if (single_step && (leaf->refs.load(std::memory_order_acquire) != 0 || leaf->open_files.load(std::memory_order_acquire) != 0)) {
            mount_lock.unlock();
            return -EBUSY;
        }
        leaf->retiring.store(true, std::memory_order_release);
        bump_mount_generation_locked();
        mount_lock.unlock();
        int const DRAIN = complete_retired_mount_teardown(leaf, true, nullptr, !single_step);
        if (DRAIN != 0) return DRAIN;
        if (single_step) return get_mount_count(owner) == 0 ? 0 : -EAGAIN;
    }
}
}  // namespace

auto retire_mount_namespace(const mount_ns::NamespaceRef& scope) -> int { return retire_mount_namespace_impl(scope, false); }

auto retire_mount_namespace_step(const mount_ns::NamespaceRef& scope) -> int { return retire_mount_namespace_impl(scope, true); }

auto unmount_filesystem_if_private_data(const char* path, const void* expected_private_data) -> int {
    if (expected_private_data == nullptr) {
        return -EINVAL;
    }
    return unmount_filesystem_impl(path, expected_private_data, true);
}

auto unmount_filesystem_if_dev_id(const char* path, uint32_t expected_dev_id, const mount_ns::NamespaceRef& scope, MountPathKind path_kind)
    -> int {
    if (expected_dev_id == 0) {
        return -EINVAL;
    }
    return unmount_filesystem_impl(path, nullptr, false, expected_dev_id, true, scope, path_kind);
}

auto unmount_filesystem_by_private_data(const void* expected_private_data) -> int {
    if (expected_private_data == nullptr) {
        return -EINVAL;
    }

    std::array<char, MAX_MOUNT_PATH> mounted_path{};
    MountPoint* owned_mount = nullptr;
    size_t mount_count_after_remove = 0;
    while (owned_mount == nullptr) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        for (size_t i = 0; i < mounts.size(); ++i) {
            MountPoint* const MP = mounts.at(i);
            if (MP == nullptr || MP->private_data != expected_private_data) {
                continue;
            }
            if (MP->retiring.load(std::memory_order_acquire) || mount_path_has_descendants_locked(MP->path, MP->namespace_owner)) {
                mount_lock.unlock();
                return -EBUSY;
            }
            if (MP->path != nullptr) {
                std::snprintf(mounted_path.data(), mounted_path.size(), "%s", MP->path);
            }
            MP->retiring.store(true, std::memory_order_release);
            bump_mount_generation_locked();
            owned_mount = MP;
            break;
        }
        mount_lock.unlock();
        if (owned_mount == nullptr) {
            return -ENOENT;
        }
    }

    bool const HOST_MOUNT = owned_mount->namespace_owner.is_host();
    int const UNMOUNT_RET = complete_retired_mount_teardown(owned_mount, false, &mount_count_after_remove);
    if (UNMOUNT_RET != 0) {
        return UNMOUNT_RET;
    }
    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(mount_count_after_remove), 0, 0);

    vfs_debug_log("unmount_filesystem: unmounted owned mount\n");
    if (HOST_MOUNT && ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_UNMOUNT, mounted_path.data(),
                                         static_cast<uint16_t>(std::strlen(mounted_path.data()) + 1));
        ker::net::wki::wki_resource_advertise_all();
    }
    return 0;
}

auto shutdown_unmount_all_exact(const char* root_path) -> int {
    if (root_path == nullptr || root_path[0] != '/') {
        return -EINVAL;
    }

    uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
        mount_lock.unlock();
        return -EBUSY;
    }
    size_t const COUNT = mounts.size();
    mount_lock.unlock();
    if (COUNT == 0) {
        return 0;
    }

    auto** pending = new MountPoint*[COUNT];
    if (pending == nullptr) {
        return -ENOMEM;
    }

    int result = 0;
    size_t pending_count = 0;
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
        mount_lock.unlock();
        delete[] pending;
        return -EBUSY;
    }
    for (size_t i = 0; i < mounts.size() && pending_count < COUNT; ++i) {
        auto* mp = mounts.at(i);
        if (mp == nullptr) {
            continue;
        }
        if (mp->retiring.load(std::memory_order_acquire)) {
            if (result == 0) {
                result = -EBUSY;
            }
            continue;
        }
        mp->retiring.store(true, std::memory_order_release);
        pending[pending_count++] = mp;
    }
    if (pending_count != 0) {
        bump_mount_generation_locked();
    }
    if (mounts.size() > COUNT) {
        log::warn("shutdown mount table changed while snapshotting; %lu mounts now published", static_cast<unsigned long>(mounts.size()));
        if (result == 0) {
            result = -EAGAIN;
        }
    }
    mount_lock.unlock();

    sort_shutdown_mounts(pending, pending_count, root_path);

    for (size_t i = 0; i < pending_count; ++i) {
        auto* mp = pending[i];
        if (mp == nullptr) {
            continue;
        }

        int const SYNC_RET = sync_mount_for_shutdown(mp);
        if (SYNC_RET != 0) {
            if (result == 0) {
                result = SYNC_RET;
            }
            rollback_mount_retirement(mp);
            continue;
        }

        std::array<char, MAX_MOUNT_PATH> mounted_path{};
        if (mp->path != nullptr) {
            std::snprintf(mounted_path.data(), mounted_path.size(), "%s", mp->path);
        }
        int const UNMOUNT_RET = complete_retired_mount_teardown(mp, true, nullptr);
        if (UNMOUNT_RET != 0) {
            if (result == 0) {
                result = UNMOUNT_RET;
            }
            continue;
        }
        log::info("shutdown unmounted %s", mounted_path.data());
    }

    delete[] pending;
    return result;
}

auto find_mount_point(const char* path, size_t known_path_len, const mount_ns::NamespaceRef& scope) -> MountRef {
    if (path == nullptr || scope.closed()) {
        return MountRef{};
    }

#ifdef WOS_SELFTEST
    mount_lookup_calls.fetch_add(1, std::memory_order_relaxed);
#endif

    size_t const PATH_LEN = mount_lookup_known_path_len(path, known_path_len);
    bool const PATH_LEN_KNOWN = PATH_LEN != UNKNOWN_MOUNT_PATH_LEN;
    size_t const FIRST_COMPONENT_LEN = PATH_LEN_KNOWN ? mount_first_component_len(path, PATH_LEN) : 0;
    bool const ROOT_FALLBACK_CACHEABLE = FIRST_COMPONENT_LEN > 0 && FIRST_COMPONENT_LEN < MOUNT_ROOT_COMPONENT_CACHE_MAX && path[0] == '/';
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        if (PATH_LEN_KNOWN) {
            if (ROOT_FALLBACK_CACHEABLE) {
                if (auto* cached_root = mount_root_fallback_cache_get_retained(path, FIRST_COMPONENT_LEN, PIVOT_EPOCH, scope);
                    cached_root != nullptr) {
                    return MountRef{cached_root};
                }
                if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
                    continue;
                }
            }
            if (auto* cached = mount_lookup_cache_get_retained(path, PATH_LEN, PIVOT_EPOCH, scope); cached != nullptr) {
                return MountRef{cached};
            }
            if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
                continue;
            }
        }

        // Find the longest matching mount point.
        MountPoint* best_match = nullptr;
        size_t best_length = 0;
        uint64_t generation = 0;
        bool first_component_has_nonroot_mount = false;

        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        if (scope.closed()) {
            mount_lock.unlock();
            return {};
        }
        for (auto* mount : mounts) {
            if (mount == nullptr || mount->namespace_owner != scope || mount->path == nullptr) {
                continue;
            }

            if (ROOT_FALLBACK_CACHEABLE && mount_first_component_matches(mount, path, FIRST_COMPONENT_LEN)) {
                first_component_has_nonroot_mount = true;
            }

            size_t const MOUNT_LEN = mount->path_len;
            if (mount_path_matches(mount, path, PATH_LEN, PATH_LEN_KNOWN) && MOUNT_LEN > best_length) {
                best_match = mount;
                best_length = MOUNT_LEN;
            }
        }
        if (best_match != nullptr && !retain_mount_locked(best_match)) {
            best_match = nullptr;
        }
        generation = mount_generation.load(std::memory_order_acquire);
        mount_lock.unlock();

        if (best_match != nullptr && PATH_LEN_KNOWN) {
            mount_lookup_cache_store(path, PATH_LEN, best_match, generation);
            if (ROOT_FALLBACK_CACHEABLE && mount_is_root(best_match) && !first_component_has_nonroot_mount) {
                mount_root_fallback_cache_store(path, FIRST_COMPONENT_LEN, best_match, generation);
            }
        }

        return MountRef{best_match};
    }
}

auto mount_table_generation_snapshot() -> uint64_t {
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        uint64_t const GENERATION = mount_generation.load(std::memory_order_acquire);
        if (mount_pivot_epoch.load(std::memory_order_acquire) == PIVOT_EPOCH) {
            return GENERATION;
        }
    }
}

auto configure_mount_point_exact(const char* path, FSType expected_type, void* private_data, FileOperations* fops,
                                 const mount_ns::NamespaceRef& scope) -> bool {
    if (path == nullptr || scope.closed()) {
        return false;
    }

    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        for (auto* mp : mounts) {
            if (mp == nullptr || mp->namespace_owner != scope || scope.closed() || mp->path == nullptr) {
                continue;
            }
            if (mp->retiring.load(std::memory_order_acquire)) {
                continue;
            }
            if (mp->fs_type != expected_type) {
                continue;
            }
            if (std::strcmp(mp->path, path) != 0) {
                continue;
            }

            mp->private_data = private_data;
            mp->fops = fops;
            mount_lock.unlock();
            return true;
        }
        mount_lock.unlock();
        return false;
    }
}

PivotMountReferences::~PivotMountReferences() { delete static_cast<PivotCoverState*>(state_); }

auto remap_mounts_for_pivot(const char* new_root, const char* put_old, const ker::mod::security::Snapshot* credentials,
                            PivotMountReferences* references, const mount_ns::NamespaceRef& scope, const char* current_root) -> int {
    if ((credentials == nullptr) != (references == nullptr) || (references != nullptr && references->state_ != nullptr)) return -EINVAL;
    PivotMountReferences local_references;
    auto& reference_owner = references != nullptr ? *references : local_references;
    if (new_root == nullptr || put_old == nullptr) {
        return -EINVAL;
    }

    size_t const NEW_ROOT_LEN = std::strlen(new_root);
    size_t const PUT_OLD_LEN = std::strlen(put_old);
    if (NEW_ROOT_LEN == 0 || PUT_OLD_LEN == 0) {
        return -EINVAL;
    }
    if (NEW_ROOT_LEN >= MAX_MOUNT_PATH || PUT_OLD_LEN >= MAX_MOUNT_PATH) {
        return -ENAMETOOLONG;
    }
    if (current_root != nullptr && scope.is_host()) return -ENOTSUP;
    const char* const OLD_ROOT = current_root != nullptr ? current_root : "/";
    size_t const OLD_ROOT_LEN = std::strlen(OLD_ROOT);
    if (OLD_ROOT_LEN == 0 || OLD_ROOT_LEN >= MAX_MOUNT_PATH || OLD_ROOT[0] != '/') return -EINVAL;
    auto under_old_root = [&](const char* path) {
        return OLD_ROOT_LEN == 1 ? path[0] == '/' : path_is_under_root(path, OLD_ROOT, OLD_ROOT_LEN);
    };
    if (current_root != nullptr &&
        (!under_old_root(new_root) || !path_is_under_root(put_old, new_root, NEW_ROOT_LEN) || PUT_OLD_LEN <= NEW_ROOT_LEN))
        return -EINVAL;
    auto old_root_suffix = [&](const MountPoint* mount) {
        return mount->path + (current_root != nullptr && OLD_ROOT_LEN != 1 ? OLD_ROOT_LEN : 0);
    };

    mount_lock.lock();
    if (scope.closed()) {
        mount_lock.unlock();
        return -EIDRM;
    }
    // A foreign teardown also mutates the shared row storage while its heavy
    // close runs unlocked. Keep the old global publication barrier: no row
    // may already be retiring when this transaction owns the odd epoch.
    for (auto* mount : mounts) {
        if (mount != nullptr && mount->retiring.load(std::memory_order_acquire)) {
            mount_lock.unlock();
            return -EBUSY;
        }
    }
    for (const auto& initialization : mount_initializations) {
        if (initialization.active && initialization.namespace_owner == scope) {
            mount_lock.unlock();
            return -EBUSY;
        }
    }

    uint64_t const STABLE_EPOCH = mount_pivot_epoch.load(std::memory_order_acquire);
    if ((STABLE_EPOCH & 1U) != 0 || STABLE_EPOCH > UINT64_MAX - 2) {
        mount_lock.unlock();
        return -EBUSY;
    }
    uint64_t const ODD_EPOCH = STABLE_EPOCH + 1;
    pivot_namespace_generation = scope.generation();
    mount_pivot_epoch.store(ODD_EPOCH, std::memory_order_release);

    MountPoint const* new_mount = nullptr;
    MountPoint* old_root_mount = nullptr;
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->namespace_owner != scope || mp->path == nullptr) {
            continue;
        }
        if (mp->retiring.load(std::memory_order_acquire)) {
            mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
            mount_lock.unlock();
            return -EBUSY;
        }
        if (std::strcmp(mp->path, new_root) == 0) {
            new_mount = mp;
        }
        if (current_root != nullptr && !under_old_root(mp->path)) {
            mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
            mount_lock.unlock();
            return -EXDEV;
        }
        if (std::strcmp(mp->path, OLD_ROOT) == 0) {
            old_root_mount = mp;
        }
    }

    if (new_mount == nullptr) {
        mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
        mount_lock.unlock();
        return -EINVAL;
    }
    if (new_mount == old_root_mount) {
        mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
        mount_lock.unlock();
        return -EINVAL;
    }
    if (current_root != nullptr && old_root_mount == nullptr) {
        mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
        mount_lock.unlock();
        return -EXDEV;
    }

    if (old_root_mount != nullptr && mount_has_active_refs_locked(old_root_mount)) {
        mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
        mount_lock.unlock();
        return -EBUSY;
    }

    for (auto* mp : mounts) {
        if (mp == nullptr || mp->namespace_owner != scope || mp->path == nullptr || mp == new_mount || mp == old_root_mount) {
            continue;
        }
        if (path_is_under_root(mp->path, new_root, NEW_ROOT_LEN)) {
            continue;
        }
        if (mount_has_active_refs_locked(mp)) {
            mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
            mount_lock.unlock();
            return -EBUSY;
        }
        if (std::strlen(old_root_suffix(mp)) >= MAX_MOUNT_PATH - NEW_ROOT_LEN) {
            mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
            mount_lock.unlock();
            return -ENAMETOOLONG;
        }
    }

    uint64_t const TABLE_GENERATION = mount_generation.load(std::memory_order_acquire);
    size_t const MOUNT_COUNT = mounts.size();
    mount_lock.unlock();

    // Snapshot one pointer at a time under mount_lock, but grow the temporary
    // vectors only after unlocking. The odd epoch keeps table publishers and
    // removers out while all potentially blocking allocations take place.
    ker::util::SmallVec<MountPoint*, 8> replacement_mounts;
    for (size_t i = 0; i < MOUNT_COUNT; ++i) {
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != ODD_EPOCH ||
            mount_generation.load(std::memory_order_acquire) != TABLE_GENERATION || i >= mounts.size()) {
            mount_lock.unlock();
            return abort_mount_pivot(ODD_EPOCH, -EBUSY);
        }
        MountPoint* const MP = mounts.at(i);
        mount_lock.unlock();

        if (MP == nullptr || MP->namespace_owner != scope || MP->path == nullptr || MP == new_mount ||
            (MP != old_root_mount && path_is_under_root(MP->path, new_root, NEW_ROOT_LEN))) {
            continue;
        }
        if (!replacement_mounts.push_back(MP)) {
            return abort_mount_pivot(ODD_EPOCH, -ENOMEM);
        }
    }

    ker::util::SmallVec<PivotMountReplacement, 8> replacements;
    for (auto* mp : replacement_mounts) {
        size_t const TARGET_LEN = mp == old_root_mount ? PUT_OLD_LEN : NEW_ROOT_LEN + std::strlen(old_root_suffix(mp));
        auto* target = new char[TARGET_LEN + 1];
        if (target == nullptr) {
            release_pivot_replacements(replacements);
            return abort_mount_pivot(ODD_EPOCH, -ENOMEM);
        }
        if (mp == old_root_mount) {
            std::memcpy(target, put_old, PUT_OLD_LEN + 1);
        } else {
            std::snprintf(target, TARGET_LEN + 1, "%s%s", new_root, old_root_suffix(mp));
        }
        PivotMountReplacement const REPLACEMENT = {.mount = mp, .path = target, .path_len = TARGET_LEN};
        if (!replacements.push_back(REPLACEMENT)) {
            delete[] target;
            release_pivot_replacements(replacements);
            return abort_mount_pivot(ODD_EPOCH, -ENOMEM);
        }
    }

    // Reader/ref acquisition and every mount-table writer validate the even
    // pivot epoch under mount_lock. While this transition owns the odd epoch,
    // the table and all path strings are therefore stable without holding the
    // spinlock through the potentially quadratic collision analysis.
    if (mount_pivot_epoch.load(std::memory_order_acquire) != ODD_EPOCH ||
        mount_generation.load(std::memory_order_acquire) != TABLE_GENERATION) {
        release_pivot_replacements(replacements);
        return abort_mount_pivot(ODD_EPOCH, -EBUSY);
    }
    bool target_collision = false;
    for (size_t i = 0; i < replacements.size() && !target_collision; ++i) {
        for (size_t j = i + 1; j < replacements.size(); ++j) {
            if (std::strcmp(replacements.at(i).path, replacements.at(j).path) == 0) {
                target_collision = true;
                break;
            }
        }
        if (target_collision) {
            break;
        }
        for (auto* existing : mounts) {
            bool existing_is_replaced = false;
            for (const auto& replacement : replacements) {
                if (replacement.mount == existing) {
                    existing_is_replaced = true;
                    break;
                }
            }
            if (!existing_is_replaced && existing != nullptr && existing->namespace_owner == scope && existing->path != nullptr &&
                std::strcmp(replacements.at(i).path, existing->path) == 0) {
                target_collision = true;
                break;
            }
        }
    }
    if (target_collision) {
        release_pivot_replacements(replacements);
        return abort_mount_pivot(ODD_EPOCH, -EBUSY);
    }

    auto* cover_state = new (std::nothrow) PivotCoverState{};
    if (cover_state != nullptr) {
        reference_owner.state_ = cover_state;
        cover_state->entries.reset(new (std::nothrow) PivotCoverEntry[MOUNT_COUNT]);
        cover_state->count = MOUNT_COUNT;
    }
    if (cover_state == nullptr || !cover_state->entries) {
        release_pivot_replacements(replacements);
        return abort_mount_pivot(ODD_EPOCH, -ENOMEM);
    }
    auto destination = [&](const MountPoint* mount) -> const char* {
        for (const auto& replacement : replacements)
            if (replacement.mount == mount) return replacement.path;
        return mount->path;
    };
    if (credentials != nullptr) {
        auto const REQUEST_CREDENTIALS = *credentials;
        for (size_t index = 0; index < MOUNT_COUNT; ++index) {
            auto* child = mounts.at(index);
            if (child == nullptr || child->namespace_owner != scope) continue;
            const char* const CHILD_PATH = destination(child);
            MountPoint* parent = nullptr;
            size_t parent_length = 0;
            for (auto* possible : mounts) {
                if (possible == nullptr || possible->namespace_owner != scope || possible == child) continue;
                const char* const PARENT_PATH = destination(possible);
                size_t const LENGTH = std::strlen(PARENT_PATH);
                if (LENGTH > parent_length && std::strlen(CHILD_PATH) > LENGTH && path_is_under_root(CHILD_PATH, PARENT_PATH, LENGTH)) {
                    parent = possible;
                    parent_length = LENGTH;
                }
            }
            if (parent == nullptr || (parent->fs_type != FSType::TMPFS && parent->fs_type != FSType::XFS)) continue;
            auto* relative = CHILD_PATH + (parent_length == 1 ? 0 : parent_length);
            int const PIN = pin_pivot_cover(REQUEST_CREDENTIALS, parent, relative, TABLE_GENERATION + 1, cover_state->entries[index]);
            // Synthetic mount paths can lack a native directory. They remain
            // explicitly unproven; scoped membership must keep refusing them.
            if (PIN == -ENOENT || PIN == -ENOTDIR || PIN == -ENOTSUP) continue;
            if (PIN != 0) {
                release_pivot_replacements(replacements);
                return abort_mount_pivot(ODD_EPOCH, PIN);
            }
        }
    }

    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) != ODD_EPOCH ||
        mount_generation.load(std::memory_order_acquire) != TABLE_GENERATION) {
        mount_lock.unlock();
        release_pivot_replacements(replacements);
        return abort_mount_pivot(ODD_EPOCH, -EBUSY);
    }
    if (scope.closed()) {
        mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
        mount_lock.unlock();
        release_pivot_replacements(replacements);
        return -EIDRM;
    }
    for (const auto& replacement : replacements) {
        if (mount_has_active_refs_locked(replacement.mount, cover_state)) {
            mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
            mount_lock.unlock();
            release_pivot_replacements(replacements);
            return -EBUSY;
        }
    }

    // Publish independently pinned destination covers with the remapped paths.
    // Failure above leaves every old edge intact; all retired references stay
    // in the caller's owner until namespace and task-root publication finish.
    for (size_t i = 0; i < mounts.size(); ++i) {
        auto* mount = mounts.at(i);
        if (mount == nullptr || mount->namespace_owner != scope) continue;
        auto& entry = cover_state->entries[i];
        entry.retired = mount->covered_directory;
        mount->covered_directory = std::exchange(entry.candidate, nullptr);
    }
    bump_mount_generation_locked();
    for (auto& replacement : replacements) {
        auto* mp = replacement.mount;
        log::info("pivot_root remapped mount '%s' -> '%s'", mp->path, replacement.path);
        delete[] mp->path;
        mp->path = replacement.path;
        mp->path_len = replacement.path_len;
        replacement.path = nullptr;
    }
    mount_lock.unlock();

    release_pivot_replacements(replacements);
    return 0;
}

void rebase_wki_mounts_for_new_root(const char* new_root, const mount_ns::NamespaceRef& scope) {
    if (new_root == nullptr) {
        return;
    }

    mount_lock.lock();
    uint64_t const ODD_EPOCH = mount_pivot_epoch.load(std::memory_order_acquire);
    if ((ODD_EPOCH & 1U) == 0 || pivot_namespace_generation != scope.generation()) {
        mount_lock.unlock();
        return;
    }

    size_t const NEW_ROOT_LEN = std::strlen(new_root);
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->namespace_owner != scope || mp->path == nullptr) {
            continue;
        }
        if (std::strcmp(mp->path, "/wki") != 0 && std::strncmp(mp->path, "/wki/", 5) != 0) {
            continue;
        }
        if (path_is_under_root(mp->path, new_root, NEW_ROOT_LEN)) {
            continue;
        }
        // Mount publication is closed for the entire odd epoch, and the
        // transactional remap included every pre-existing /wki mount. This is
        // therefore an invariant diagnostic rather than a best-effort repair.
        log::warn("pivot_root found an unremapped WKI mount '%s'", mp->path);
    }
    mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
    mount_lock.unlock();
}

auto get_mount_count(const mount_ns::NamespaceRef& scope) -> size_t {
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        size_t count = 0;
        for (auto* mount : mounts)
            if (mount != nullptr && mount->namespace_owner == scope) ++count;
        mount_lock.unlock();
        return count;
    }
}

auto get_mount_at(size_t index, const mount_ns::NamespaceRef& scope) -> MountRef {
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        MountPoint* mp = mount_at_visible_index_locked(scope, index);
        if (scope.closed()) mp = nullptr;
        if (!retain_mount_locked(mp)) {
            mp = nullptr;
        }
        mount_lock.unlock();
        return MountRef{mp};
    }
}

auto copy_mount_path_exact(const MountPoint* mount, char* out, size_t capacity) -> int {
    if (mount == nullptr || out == nullptr || capacity == 0) return -EINVAL;
    mount_lock.lock();
    int result = -ENOENT;
    if ((mount_pivot_epoch.load(std::memory_order_acquire) & 1U) != 0) {
        mount_lock.unlock();
        return -EAGAIN;
    }
    for (auto* candidate : mounts) {
        if (candidate != mount || candidate->retiring.load(std::memory_order_acquire)) continue;
        if (candidate->path == nullptr) break;
        if (candidate->path_len >= capacity) {
            result = -ENAMETOOLONG;
        } else {
            std::memcpy(out, candidate->path, candidate->path_len + 1);
            result = 0;
        }
        break;
    }
    mount_lock.unlock();
    return result;
}

auto get_mount_snapshot_at(size_t index, MountSnapshot* out, const mount_ns::NamespaceRef& scope) -> bool {
    if (out == nullptr) {
        return false;
    }

    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        MountPoint const* mp = mount_at_visible_index_locked(scope, index);
        if (mp == nullptr || mp->path == nullptr) {
            mount_lock.unlock();
            return false;
        }

        size_t const PATH_LEN = mp->path_len;
        if (PATH_LEN >= MOUNT_PATH_MAX) {
            mount_lock.unlock();
            return false;
        }

        std::memcpy(static_cast<void*>(out->path), mp->path, PATH_LEN + 1);
        out->fs_type = mp->fs_type;
        out->dev_id = mp->dev_id;
        out->read_only = mp->read_only;
        out->nosuid = mp->nosuid;
        if (mp->fstype != nullptr) {
            size_t const FSTYPE_LEN = std::strlen(mp->fstype);
            size_t const COPY_LEN = (FSTYPE_LEN < MOUNT_FSTYPE_MAX) ? FSTYPE_LEN : MOUNT_FSTYPE_MAX - 1;
            std::memcpy(static_cast<void*>(out->fstype), mp->fstype, COPY_LEN);
            out->fstype[COPY_LEN] = '\0';
        }

        mount_lock.unlock();
        return true;
    }
}

auto capture_mount_covered_directory(MountPoint* mount, File** output, uint64_t* generation) -> int {
    if (mount == nullptr || output == nullptr || *output != nullptr || generation == nullptr) return -EINVAL;
    mount_lock.lock();
    int result = 0;
    if (mount_index_locked(mount) == MOUNT_INDEX_NONE)
        result = -ENOENT;
    else if ((mount_pivot_epoch.load(std::memory_order_acquire) & 1U) != 0 || mount->retiring.load(std::memory_order_acquire))
        result = -EAGAIN;
    else if (mount->covered_directory == nullptr)
        result = -ENOTSUP;
    if (result == 0) {
        vfs_retain_file(mount->covered_directory);
        *output = mount->covered_directory;
        *generation = mount_generation.load(std::memory_order_acquire);
    }
    mount_lock.unlock();
    return result;
}

#ifdef WOS_SELFTEST
auto mount_point_pivot_busy_for_test(MountPoint* mount) -> bool {
    mount_lock.lock();
    bool const BUSY = mount_has_active_refs_locked(mount);
    mount_lock.unlock();
    return BUSY;
}
auto mount_lock_available_for_test() -> bool {
    bool const AVAILABLE = mount_lock.try_lock();
    if (AVAILABLE) mount_lock.unlock();
    return AVAILABLE;
}
auto mount_pivot_pending_for_test() -> bool { return (mount_pivot_epoch.load(std::memory_order_acquire) & 1U) != 0; }
auto mount_selftest_identity_exhaustion() -> bool {
    constexpr const char* LAST_PATH = "/tmp/ktest_mount_last_identity";
    constexpr const char* NEXT_PATH = "/tmp/ktest_mount_exhausted_identity";
    auto check = [](bool okay, int line) {
        if (!okay) log::warn("mount identity exhaustion check failed at %d", line);
        return okay;
    };
    // The fixture uses a private cursor and never rewinds the production one.
    // Before exhaustion, the last ID cannot have been issued by production.
    mount_lock.lock();
    bool const LAST_AVAILABLE = next_dev_id != 0 && next_dev_id != UINT32_MAX;
    mount_lock.unlock();
    if (!check(LAST_AVAILABLE && !ker::net::wki::g_wki.initialized, __LINE__)) return false;
    struct Cleanup {
        PreparedMount last{}, next{};
        uint32_t last_id = 0;
        uint32_t next_id = 0;
        ~Cleanup() {
            finish_prepared_mount(&last);
            finish_prepared_mount(&next);
            if (next_id != 0) static_cast<void>(unmount_filesystem_if_dev_id(NEXT_PATH, next_id));
            if (last_id != 0) static_cast<void>(unmount_filesystem_if_dev_id(LAST_PATH, last_id));
        }
    } cleanup;
    if (!check(prepare_mount_filesystem(LAST_PATH, "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &cleanup.last) == 0, __LINE__))
        return false;
    uint32_t cursor = UINT32_MAX;
    if (!check(publish_prepared_mount_with_identity(&cleanup.last, nullptr, nullptr, cursor) == 0, __LINE__)) return false;
    auto* last = static_cast<PreparedMountState*>(cleanup.last.state);
    cleanup.last_id = last->mount->dev_id;
    if (!check(cleanup.last_id == UINT32_MAX && cursor == 0, __LINE__)) return false;
    finish_prepared_mount(&cleanup.last);
    if (!check(prepare_mount_filesystem(NEXT_PATH, "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &cleanup.next) == 0, __LINE__))
        return false;
    auto* candidate = static_cast<PreparedMountState*>(cleanup.next.state);
    auto* storage = candidate->mount->private_data;
    auto const COUNT = get_mount_count();
    auto const GENERATION = mount_table_generation_snapshot();
    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        if (!check(publish_prepared_mount_with_identity(&cleanup.next, nullptr, nullptr, cursor) == -EOVERFLOW && cursor == 0 &&
                       get_mount_count() == COUNT && mount_table_generation_snapshot() == GENERATION && !candidate->published &&
                       candidate->mount->dev_id == 0 && candidate->mount->private_data == storage &&
                       candidate->mount->covered_directory == nullptr,
                   __LINE__))
            return false;
    }
    // Failed publication kept its reservation and backend. The ordinary cursor
    // can publish that same candidate, without reinitializing or borrowing ID 0.
    if (!check(publish_prepared_mount(&cleanup.next) == 0, __LINE__)) return false;
    cleanup.next_id = candidate->mount->dev_id;
    if (!check(cleanup.next_id != 0 && cleanup.next_id != cleanup.last_id, __LINE__)) return false;
    finish_prepared_mount(&cleanup.next);
    if (!check(unmount_filesystem_if_dev_id(NEXT_PATH, cleanup.last_id) == -ENOENT &&
                   unmount_filesystem_if_dev_id(NEXT_PATH, cleanup.next_id) == 0,
               __LINE__))
        return false;
    cleanup.next_id = 0;
    if (!check(unmount_filesystem_if_dev_id(LAST_PATH, cleanup.last_id) == 0, __LINE__)) return false;
    cleanup.last_id = 0;
    return true;
}

auto mount_selftest_pivot_covers(bool disk) -> bool {
    constexpr const char* BASE = "/tmp/ktest_pivot_cover";
    constexpr const char* CHILD = "/tmp/ktest_pivot_cover/covered";
    auto check = [](bool okay, int line) {
        if (!okay) log::warn("pivot native cover check failed at %d", line);
        return okay;
    };
    struct Cleanup {
        bool mounted = false;
        bool child = false;
        ~Cleanup() {
            if (child) static_cast<void>(unmount_filesystem(CHILD));
            if (mounted) {
                static_cast<void>(vfs_rmdir(CHILD));
                static_cast<void>(unmount_filesystem(BASE));
            }
            static_cast<void>(vfs_rmdir(BASE));
        }
    } cleanup;
    if (!check(vfs_mkdir(BASE, 0755) == 0, __LINE__)) return false;
    auto* device = disk ? ker::dev::block_device_find_by_name("sdb2") : nullptr;
    if (!check((!disk || device != nullptr) && mount_filesystem(BASE, disk ? "xfs" : "tmpfs", device) == 0, __LINE__)) return false;
    cleanup.mounted = true;
    if (!check(vfs_mkdir(CHILD, 0755) == 0, __LINE__)) return false;
    Stat original{};
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    if (!check(vfs_stat(CHILD, &original) == 0, __LINE__)) return false;
    {
        ker::mod::sched::task::Task task{};
        File* pin = nullptr;
        if (!check(vfs_pin_directory_resolved_beneath_as(task, ROOT, BASE, CHILD, &pin) == 0, __LINE__)) return false;
        auto release = [](File* file) { vfs_put_file(file); };
        std::unique_ptr<File, decltype(release)> covered(pin);
        PreparedMount prepared{};
        int status =
            prepare_mount_filesystem(CHILD, "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &prepared, MountPathKind::RETAINED_PHYSICAL);
        if (status == 0) status = publish_prepared_mount(&prepared, CHILD, covered.get());
        finish_prepared_mount(&prepared);
        if (!check(status == 0, __LINE__)) return false;
        cleanup.child = true;
    }
    auto parent = find_mount_point(BASE);
    auto* const PARENT = parent.get();
    if (!check(PARENT != nullptr, __LINE__)) return false;
    auto const GENERATION = mount_table_generation_snapshot();
    {
        PivotCoverState prepared{.entries = std::unique_ptr<PivotCoverEntry[]>(new (std::nothrow) PivotCoverEntry[2]), .count = 2};
        if (!check(static_cast<bool>(prepared.entries), __LINE__)) return false;
        // A normal VFS lookup selects the overlaid child. Pivot must pin the
        // exact native covered inode in its independently selected parent.
        if (!check(pin_pivot_cover(ROOT, PARENT, "/covered", GENERATION, prepared.entries[0]) == 0 &&
                       pin_pivot_cover(ROOT, PARENT, "/missing", GENERATION, prepared.entries[1]) == -ENOENT,
                   __LINE__))
            return false;
        auto* file = prepared.entries[0].candidate;
        Stat actual{};
        if (!check(file && file->mount_owner == PARENT && file->vfs_path == nullptr && !file->directory_view &&
                       (file->open_flags & 3) == FILE_ACCESS_SEARCH_ONLY && vfs_fstat_file(file, &actual) == 0 &&
                       actual.st_ino == original.st_ino && actual.st_dev == original.st_dev && !prepared.entries[1].candidate,
                   __LINE__))
            return false;
        parent.reset();
        auto busy = [&]() {
            mount_lock.lock();
            bool const RESULT = mount_has_active_refs_locked(PARENT, &prepared);
            mount_lock.unlock();
            return RESULT;
        };
        if (!check(!busy() && mount_point_pivot_busy_for_test(PARENT), __LINE__)) return false;
        vfs_retain_file(file);
        bool const EXTERNAL_FILE_BUSY = busy();
        vfs_put_file(file);
        if (!check(EXTERNAL_FILE_BUSY && !busy(), __LINE__)) return false;
        {
            auto external = retain_mount_point(PARENT);
            if (!check(external && busy(), __LINE__)) return false;
        }
        if (!check(!busy(), __LINE__)) return false;
        auto old_root = find_mount_point("/");
        auto child = find_mount_point(CHILD);
        File* old_cover = nullptr;
        uint64_t captured_generation = 0;
        if (!check(old_root && child && capture_mount_covered_directory(child.get(), &old_cover, &captured_generation) == 0, __LINE__))
            return false;
        auto release_file = [](File* value) { vfs_put_file(value); };
        std::unique_ptr<File, decltype(release_file)> retained(old_cover);
        PivotMountReferences references;
        if (!check(remap_mounts_for_pivot(BASE, "/tmp/ktest_pivot_cover/oldroot", &ROOT, &references) == -EBUSY &&
                       mount_table_generation_snapshot() == GENERATION && child->covered_directory == retained.get() &&
                       std::strcmp(child->path, CHILD) == 0,
                   __LINE__))
            return false;
    }
    if (!check(!mount_point_pivot_busy_for_test(PARENT), __LINE__)) return false;
    return check(mount_lock_available_for_test(), __LINE__);
}

auto mount_selftest_descendant_lifetime(bool disk) -> bool {
    constexpr const char* BASE = "/tmp/ktest_mount_tree";
    constexpr const char* CHILD = "/tmp/ktest_mount_tree/child";
    constexpr const char* DEEP = "/tmp/ktest_mount_tree/child/deep";
    constexpr const char* SIBLING = "/tmp/ktest_mount_tree2";
    constexpr const char* SEED = "/tmp/ktest_mount_tree/child/seed";
    auto check = [](bool okay, int line) {
        if (!okay) log::warn("mount descendant lifetime check failed at %d", line);
        return okay;
    };
    PreparedMount child{};
    struct Cleanup {
        PreparedMount& child;
        MountPoint* parent = nullptr;
        MountPoint* child_mount = nullptr;
        File* file = nullptr;
        ~Cleanup() {
            if (parent != nullptr) rollback_mount_retirement(parent);
            if (child_mount != nullptr) rollback_mount_retirement(child_mount);
            if (file != nullptr) vfs_put_file(file);
            finish_prepared_mount(&child);
            static_cast<void>(unmount_filesystem(DEEP));
            static_cast<void>(vfs_unlink(SEED));
            static_cast<void>(unmount_filesystem(CHILD));
            static_cast<void>(unmount_filesystem(BASE));
            static_cast<void>(unmount_filesystem(SIBLING));
            static_cast<void>(vfs_rmdir(BASE));
            static_cast<void>(vfs_rmdir(SIBLING));
        }
    } cleanup{child};
    if (!check(mount_path_is_descendant("/child", "/") && !mount_path_is_descendant("/", "/") && mount_path_is_descendant(DEEP, BASE) &&
                   !mount_path_is_descendant(SIBLING, BASE) && !mount_path_is_descendant(BASE, BASE) &&
                   !mount_path_is_descendant(BASE, CHILD),
               __LINE__))
        return false;
    if (!check(vfs_mkdir(BASE, 0755) == 0 && vfs_mkdir(SIBLING, 0755) == 0, __LINE__)) return false;
    auto* device = disk ? ker::dev::block_device_find_by_name("sdb2") : nullptr;
    if (!check((!disk || device != nullptr) && mount_filesystem(BASE, disk ? "xfs" : "tmpfs", device) == 0 &&
                   mount_filesystem(SIBLING, "tmpfs", nullptr) == 0,
               __LINE__))
        return false;
    auto prepare_child = [&] {
        return prepare_mount_filesystem(CHILD, "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &child, MountPathKind::RETAINED_PHYSICAL);
    };
    auto select_parent = [&] {
        auto parent = find_mount_point(BASE);
        cleanup.parent = parent.get();
        return parent && std::strcmp(parent->path, BASE) == 0;
    };
    if (!check(select_parent(), __LINE__)) return false;
    // These denials must precede retiring/generation changes, even with no
    // caller-held MountRef or File keeping the parent busy independently.
    auto deny_parent = [&] {
        auto* parent = cleanup.parent;
        auto* const DATA = parent->private_data;
        auto const DEV = parent->dev_id;
        auto const GENERATION = mount_table_generation_snapshot();
        auto const COUNT = get_mount_count();
        for (unsigned selector = 0; selector < 4; ++selector) {
            int result = selector == 0   ? unmount_filesystem(BASE)
                         : selector == 1 ? unmount_filesystem_if_private_data(BASE, DATA)
                         : selector == 2 ? unmount_filesystem_by_private_data(DATA)
                                         : unmount_filesystem_if_dev_id(BASE, DEV);
            if (result != -EBUSY) {
                cleanup.parent = nullptr;
                return false;
            }
            auto retained = find_mount_point(BASE);
            if (!retained || retained.get() != parent || retained->private_data != DATA || retained->dev_id != DEV ||
                retained->retiring.load() || get_mount_count() != COUNT || mount_table_generation_snapshot() != GENERATION)
                return false;
        }
        return unmount_filesystem_if_dev_id(BASE, 0) == -EINVAL && unmount_filesystem_if_dev_id(BASE, DEV + 1) == -ENOENT &&
               mount_table_generation_snapshot() == GENERATION;
    };
    if (!check(prepare_child() == 0 && deny_parent(), __LINE__)) return false;
    // Cancellation removes the initialization dependency. A similar-prefix
    // sibling is not a descendant and must not keep this parent busy.
    finish_prepared_mount(&child);
    cleanup.parent = nullptr;
    if (!check(unmount_filesystem(BASE) == 0 && mount_filesystem(BASE, disk ? "xfs" : "tmpfs", device) == 0 && select_parent() &&
                   prepare_child() == 0,
               __LINE__))
        return false;
    auto mark_retiring = [](MountPoint* mount) {
        mount_lock.lock();
        mount->retiring.store(true, std::memory_order_release);
        bump_mount_generation_locked();
        mount_lock.unlock();
    };
    // Deterministically exercise shutdown's bulk-marking interleaving with a
    // previously reserved child. Publication preserves the prepared backend.
    mark_retiring(cleanup.parent);
    auto const RETIRING_GENERATION = mount_table_generation_snapshot();
    auto* const PREPARED = static_cast<PreparedMountState*>(child.state);
    auto* const CHILD_DATA = PREPARED->mount->private_data;
    if (!check(publish_prepared_mount(&child, CHILD) == -EBUSY && !PREPARED->published && PREPARED->mount->private_data == CHILD_DATA &&
                   mount_table_generation_snapshot() == RETIRING_GENERATION,
               __LINE__))
        return false;
    PreparedMount rejected{};
    int const PREPARE =
        prepare_mount_filesystem(DEEP, "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &rejected, MountPathKind::RETAINED_PHYSICAL);
    bool const REJECTED = PREPARE == -EBUSY && rejected.state == nullptr;
    finish_prepared_mount(&rejected);
    MountInitializationReservation raced;
    if (!check(REJECTED && raced.acquire(DEEP, mount_pivot_epoch.load()) == -EBUSY &&
                   mount_table_generation_snapshot() == RETIRING_GENERATION &&
                   complete_retired_mount_teardown(cleanup.parent, true, nullptr) == -EBUSY && !cleanup.parent->retiring.load(),
               __LINE__))
        return false;
    // Once the parent returns to service the same prepared child can publish.
    if (!check(publish_prepared_mount(&child, CHILD) == 0, __LINE__)) return false;
    finish_prepared_mount(&child);
    {
        auto retained = find_mount_point(CHILD);
        if (!check(retained && std::strcmp(retained->path, CHILD) == 0 && retained->private_data == CHILD_DATA, __LINE__)) return false;
        cleanup.child_mount = retained.get();
    }
    if (!check(deny_parent() && mount_filesystem(DEEP, "tmpfs", nullptr) == 0 && deny_parent(), __LINE__)) return false;
    auto const DEEP_GENERATION = mount_table_generation_snapshot();
    if (!check(unmount_filesystem(CHILD) == -EBUSY && mount_table_generation_snapshot() == DEEP_GENERATION && unmount_filesystem(DEEP) == 0,
               __LINE__))
        return false;
    cleanup.file = vfs_open_file(SEED, O_CREAT | O_EXCL | 2, 0600);
    if (!check(cleanup.file != nullptr && vfs_pwrite_file(cleanup.file, "seed", 4, 0) == 4, __LINE__)) return false;
    mark_retiring(cleanup.child_mount);
    mark_retiring(cleanup.parent);
    // A busy child's rollback during shutdown cannot permit parent destruction.
    if (!check(complete_retired_mount_teardown(cleanup.child_mount, true, nullptr) == -EBUSY &&
                   complete_retired_mount_teardown(cleanup.parent, true, nullptr) == -EBUSY && deny_parent(),
               __LINE__))
        return false;
    std::array<char, 4> read{};
    if (!check(vfs_pread_file(cleanup.file, read.data(), read.size(), 0) == 4 && std::memcmp(read.data(), "seed", 4) == 0, __LINE__))
        return false;
    vfs_put_file(cleanup.file);
    cleanup.file = nullptr;
    if (!check(vfs_unlink(SEED) == 0, __LINE__)) return false;
    cleanup.child_mount = nullptr;
    if (!check(unmount_filesystem(CHILD) == 0, __LINE__)) return false;
    cleanup.parent = nullptr;
    if (!check(unmount_filesystem(BASE) == 0, __LINE__)) return false;
    auto sibling = find_mount_point(SIBLING);
    return check(sibling && std::strcmp(sibling->path, SIBLING) == 0, __LINE__);
}
#endif

}  // namespace ker::vfs
