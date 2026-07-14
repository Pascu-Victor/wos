#include "mount.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/gpt.hpp>
#include <net/wki/event.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
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
mod::sys::Spinlock mount_lock;  // Protects mounts and mount_count
std::atomic<uint64_t> mount_generation{1};
// Even values admit mount-table publication. remap_mounts_for_pivot() makes
// this odd until task roots are updated and rebase_wki_mounts_for_new_root()
// completes the namespace transition.
std::atomic<uint64_t> mount_pivot_epoch{2};
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

    std::array<const char*, MAX_MOUNT_COMPONENTS> components{};
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

    std::array<char, MAX_MOUNT_PATH> result{};
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

auto mount_has_active_refs_locked(const MountPoint* mount) -> bool {
    return mount != nullptr && mount->refs.load(std::memory_order_acquire) != 0;
}

auto retain_mount_locked(MountPoint* mount) -> bool {
    if (mount == nullptr || mount->path == nullptr || mount->retiring.load(std::memory_order_acquire)) {
        return false;
    }
    mount->refs.fetch_add(1, std::memory_order_acq_rel);
    return true;
}

void bump_mount_generation_locked() { mount_generation.fetch_add(1, std::memory_order_acq_rel); }

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

auto mount_lookup_cache_get_retained(const char* path, size_t path_len, uint64_t pivot_epoch) -> MountPoint* {
    if (path == nullptr || path_len == 0 || path_len >= MOUNT_PATH_MAX) {
        return nullptr;
    }

    uint64_t const GENERATION = mount_generation.load(std::memory_order_acquire);
    uint64_t const HASH = mount_lookup_hash(path, path_len);
    auto& set = mount_lookup_cache.at(HASH & (MOUNT_LOOKUP_CACHE_SET_COUNT - 1));

    MountPoint* candidate = nullptr;
    set.lock.lock();
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.generation != GENERATION || entry.hash != HASH || entry.path_len != path_len) {
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
        mount_generation.load(std::memory_order_acquire) == GENERATION && mount_path_matches(candidate, path, path_len, true) &&
        retain_mount_locked(candidate)) {
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

auto mount_root_fallback_cache_get_retained(const char* path, size_t component_len, uint64_t pivot_epoch) -> MountPoint* {
    if (path == nullptr || component_len == 0 || component_len >= MOUNT_ROOT_COMPONENT_CACHE_MAX || path[0] != '/') {
        return nullptr;
    }

    uint64_t const GENERATION = mount_generation.load(std::memory_order_acquire);
    uint64_t const HASH = mount_component_hash(path + 1, component_len);
    auto& set = mount_root_fallback_cache.at(HASH & (MOUNT_ROOT_FALLBACK_CACHE_SET_COUNT - 1));

    MountPoint* candidate = nullptr;
    set.lock.lock();
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.generation != GENERATION || entry.hash != HASH || entry.component_len != component_len) {
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
        mount_generation.load(std::memory_order_acquire) == GENERATION && mount_is_root(candidate) && retain_mount_locked(candidate)) {
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
        if (entry.valid && entry.generation == generation && entry.hash == HASH && entry.path_len == path_len &&
            std::memcmp(entry.path.data(), path, path_len) == 0) {
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
        if (entry.valid && entry.generation == generation && entry.hash == HASH && entry.component_len == component_len &&
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
    victim->component_len = component_len;
    victim->valid = true;
    set.lock.unlock();
}

void destroy_mount_private_data(MountPoint* mount) {
    if (mount == nullptr || mount->private_data == nullptr) {
        return;
    }

    switch (mount->fs_type) {
        case FSType::FAT32:
            ker::vfs::fat32::fat32_unmount(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            break;
        case FSType::XFS:
            ker::vfs::xfs::xfs_unmount(static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            break;
        case FSType::TMPFS:
            ker::vfs::tmpfs::destroy_mount_context(static_cast<ker::vfs::tmpfs::TmpfsMount*>(mount->private_data));
            break;
        default:
            break;
    }
    mount->private_data = nullptr;
}

void destroy_mount(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    destroy_mount_private_data(mount);
    delete[] mount->path;
    delete[] mount->fstype;
    delete mount;
}

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
#endif

// Resolve path like resolve_task_path_raw does for mount-table keys:
// first make relative targets absolute against cwd, then canonicalize, then
// apply the current task's root prefix. After pivot_root("/rootfs", ...),
// "mnt" from cwd "/root" becomes "/rootfs/root/mnt".
auto resolve_mount_path(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) {
        return -EINVAL;
    }

    std::array<char, MAX_MOUNT_PATH> logical{};
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

auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device, unsigned long flags, const char* data,
                      void* initial_private_data, FileOperations* initial_fops) -> int {
    (void)flags;
    if (path == nullptr || fstype == nullptr) {
        vfs_debug_log("mount_filesystem: invalid arguments\n");
        return -EINVAL;
    }
    if (ker::mod::power::shutdown_in_progress()) {
        vfs_debug_log("mount_filesystem: shutdown in progress\n");
        return -ESHUTDOWN;
    }

    uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();

    // Resolve through task root prefix so the stored path matches what
    // find_mount_point receives after resolve_task_path_raw.
    std::array<char, MAX_MOUNT_PATH> resolved{};
    int const PATH_RET = resolve_mount_path(path, resolved.data(), resolved.size());
    if (PATH_RET < 0) {
        vfs_debug_log("mount_filesystem: path resolution failed\n");
        return PATH_RET;
    }

    auto* mount = new MountPoint;

    // Copy resolved path and fstype into kernel heap.
    size_t const PATH_LEN = std::strlen(resolved.data());
    auto* path_copy = new char[PATH_LEN + 1];
    if (path_copy == nullptr) {
        destroy_mount(mount);
        return -ENOMEM;
    }
    std::memcpy(path_copy, resolved.data(), PATH_LEN + 1);
    mount->path = path_copy;
    mount->path_len = PATH_LEN;

    size_t const FSTYPE_LEN = std::strlen(fstype);
    auto* fstype_copy = new char[FSTYPE_LEN + 1];
    if (fstype_copy == nullptr) {
        destroy_mount(mount);
        return -ENOMEM;
    }
    std::memcpy(fstype_copy, fstype, FSTYPE_LEN + 1);
    mount->fstype = fstype_copy;

    mount->fs_type = fstype_to_enum(fstype);
    mount->device = device;
    mount->private_data = nullptr;
    mount->fops = nullptr;
    mount->read_only = device != nullptr && ker::dev::block_device_is_read_only(device);

    bool const BLOCK_RW_FS = mount->fs_type == FSType::FAT32 || mount->fs_type == FSType::XFS;
    if (BLOCK_RW_FS && device != nullptr && !ker::dev::block_device_is_read_only(device) &&
        !mount->block_writer_lease.try_acquire(device, ker::dev::BlockWriterLeaseOwner::LOCAL_MOUNT)) {
        vfs_debug_log("mount_filesystem: block device has a remote writer lease\n");
        destroy_mount(mount);
        return -EBUSY;
    }

    // Initialize the appropriate filesystem
    if (std::strcmp(fstype, "fat32") == 0 || std::strcmp(fstype, "vfat") == 0) {
        // FAT32 filesystem
        if (device == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 requires a block device\n");
            destroy_mount(mount);
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
        auto* context = ker::vfs::fat32::fat32_init_device(device, partition_start_lba);
        if (context == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 initialization failed\n");
            destroy_mount(mount);
            return -EIO;
        }

        // Store mount context for per-mount use
        mount->private_data = context;

        mount->fops = ker::vfs::fat32::get_fat32_fops();
    } else if (std::strcmp(fstype, "tmpfs") == 0) {
        // tmpfs filesystem
        bool const ROOT_COMPAT = std::strcmp(resolved.data(), "/") == 0;
        auto* root = ROOT_COMPAT ? ker::vfs::tmpfs::get_root_node() : ker::vfs::tmpfs::create_root_node();
        int tmpfs_error = 0;
        mount->private_data = ker::vfs::tmpfs::create_mount_context(root, data, ROOT_COMPAT, &tmpfs_error);
        if (mount->private_data == nullptr) {
            vfs_debug_log("mount_filesystem: tmpfs root allocation failed\n");
            if (!ROOT_COMPAT) {
                ker::vfs::tmpfs::tmpfs_free_node(root);
            }
            destroy_mount(mount);
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
            destroy_mount(mount);
            return -EINVAL;
        }
        auto* xfs_ctx = ker::vfs::xfs::xfs_vfs_init_device(device);
        if (xfs_ctx == nullptr) {
            vfs_debug_log("mount_filesystem: XFS initialization failed\n");
            destroy_mount(mount);
            return -EIO;
        }
        mount->private_data = xfs_ctx;
        mount->fops = ker::vfs::xfs::get_xfs_fops();
    } else {
        vfs_debug_log("mount_filesystem: unknown filesystem type\n");
        destroy_mount(mount);
        return -ENODEV;
    }

    size_t mount_count_after_insert = 0;
    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: pivot namespace changed before publication\n");
        destroy_mount(mount);
        return -EBUSY;
    }
    if (mount->fs_type == FSType::REMOTE) {
        for (auto* existing : mounts) {
            if (existing != nullptr && existing->path != nullptr && std::strcmp(existing->path, mount->path) == 0) {
                mount_lock.unlock();
                vfs_debug_log("mount_filesystem: remote mount path already occupied\n");
                destroy_mount(mount);
                return -EBUSY;
            }
        }
    }
    mount->dev_id = next_dev_id++;
    if (mount->fs_type == FSType::XFS && mount->private_data != nullptr) {
        static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data)->dev_id = mount->dev_id;
    }
    bump_mount_generation_locked();
    if (!mounts.push_back(mount)) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: mount table full (OOM)\n");
        destroy_mount(mount);
        return -ENOMEM;
    }
    mount_count_after_insert = mounts.size();
    mount_lock.unlock();

    vfs_debug_log("mount_filesystem: mounted ");
    vfs_debug_log(fstype);
    vfs_debug_log(" at ");
    vfs_debug_log(path);
    vfs_debug_log("\n");

    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(mount_count_after_insert), 0, 0);

    // Emit WKI storage mount event (if WKI is active)
    if (ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_MOUNT, path,
                                         static_cast<uint16_t>(std::strlen(path) + 1));
        ker::net::wki::wki_resource_advertise_all();
    }

    return 0;
}

namespace {
auto unmount_filesystem_impl(const char* path, const void* expected_private_data, bool require_private_data_match) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    MountPoint* removed_mount = nullptr;
    size_t mount_count_after_remove = 0;
    while (removed_mount == nullptr) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();

        // Resolve again after every pivot retry so the task-root prefix and
        // stored mount namespace stay in the same publication epoch.
        std::array<char, MAX_MOUNT_PATH> resolved{};
        int const PATH_RET = resolve_mount_path(path, resolved.data(), resolved.size());
        if (PATH_RET < 0) {
            return PATH_RET;
        }

        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        for (size_t i = 0; i < mounts.size(); ++i) {
            MountPoint* mp = mounts.at(i);
            bool const PATH_MATCHES = mp != nullptr && mp->path != nullptr && std::strcmp(resolved.data(), mp->path) == 0;
            bool const OWNER_MATCHES = !require_private_data_match || (mp != nullptr && mp->private_data == expected_private_data);
            if (!PATH_MATCHES || !OWNER_MATCHES) {
                continue;
            }
            mp->retiring.store(true, std::memory_order_release);
            bump_mount_generation_locked();
            mounts.remove_at(i);
            mount_count_after_remove = mounts.size();
            removed_mount = mp;
            break;
        }
        mount_lock.unlock();
        if (removed_mount == nullptr) {
            return -ENOENT;
        }
    }

    wait_for_mount_refs_to_drain(removed_mount);
    destroy_mount(removed_mount);

    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(mount_count_after_remove), 0, 0);

    vfs_debug_log("unmount_filesystem: unmounted ");
    vfs_debug_log(path);
    vfs_debug_log("\n");

    // Emit WKI storage unmount event (if WKI is active)
    if (ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_UNMOUNT, path,
                                         static_cast<uint16_t>(std::strlen(path) + 1));
        ker::net::wki::wki_resource_advertise_all();
    }

    return 0;
}
}  // namespace

auto unmount_filesystem(const char* path) -> int { return unmount_filesystem_impl(path, nullptr, false); }

auto unmount_filesystem_if_private_data(const char* path, const void* expected_private_data) -> int {
    if (expected_private_data == nullptr) {
        return -EINVAL;
    }
    return unmount_filesystem_impl(path, expected_private_data, true);
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
            if (MP->path != nullptr) {
                std::snprintf(mounted_path.data(), mounted_path.size(), "%s", MP->path);
            }
            MP->retiring.store(true, std::memory_order_release);
            bump_mount_generation_locked();
            mounts.remove_at(i);
            mount_count_after_remove = mounts.size();
            owned_mount = MP;
            break;
        }
        mount_lock.unlock();
        if (owned_mount == nullptr) {
            return -ENOENT;
        }
    }

    wait_for_mount_refs_to_drain(owned_mount);
    destroy_mount(owned_mount);
    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(mount_count_after_remove), 0, 0);

    vfs_debug_log("unmount_filesystem: unmounted owned mount\n");
    if (ker::net::wki::g_wki.initialized) {
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
    while (!mounts.empty() && pending_count < COUNT) {
        auto* mp = mounts.at(0);
        if (mp != nullptr) {
            mp->retiring.store(true, std::memory_order_release);
            pending[pending_count++] = mp;
        }
        bump_mount_generation_locked();
        mounts.remove_at(0);
    }
    if (!mounts.empty()) {
        log::warn("shutdown unmount table changed while snapshotting; %lu mounts remain", static_cast<unsigned long>(mounts.size()));
        result = -EAGAIN;
    }
    mount_lock.unlock();

    sort_shutdown_mounts(pending, pending_count, root_path);

    for (size_t i = 0; i < pending_count; ++i) {
        auto* mp = pending[i];
        if (mp == nullptr) {
            continue;
        }

        int const SYNC_RET = sync_mount_for_shutdown(mp);
        if (SYNC_RET != 0 && result == 0) {
            result = SYNC_RET;
        }

        if (!wait_for_mount_refs_to_drain_bounded(mp)) {
            log::warn("shutdown unmount busy: path=%s refs=%u", mp->path != nullptr ? mp->path : "?",
                      mp->refs.load(std::memory_order_acquire));
            if (result == 0) {
                result = -EBUSY;
            }
            continue;
        }

        log::info("shutdown unmounted %s", mp->path != nullptr ? mp->path : "?");
        destroy_mount(mp);
    }

    delete[] pending;
    return result;
}

auto find_mount_point(const char* path, size_t known_path_len) -> MountRef {
    if (path == nullptr) {
        return MountRef{};
    }

    size_t const PATH_LEN = mount_lookup_known_path_len(path, known_path_len);
    bool const PATH_LEN_KNOWN = PATH_LEN != UNKNOWN_MOUNT_PATH_LEN;
    size_t const FIRST_COMPONENT_LEN = PATH_LEN_KNOWN ? mount_first_component_len(path, PATH_LEN) : 0;
    bool const ROOT_FALLBACK_CACHEABLE = FIRST_COMPONENT_LEN > 0 && FIRST_COMPONENT_LEN < MOUNT_ROOT_COMPONENT_CACHE_MAX && path[0] == '/';
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        if (PATH_LEN_KNOWN) {
            if (ROOT_FALLBACK_CACHEABLE) {
                if (auto* cached_root = mount_root_fallback_cache_get_retained(path, FIRST_COMPONENT_LEN, PIVOT_EPOCH);
                    cached_root != nullptr) {
                    return MountRef{cached_root};
                }
                if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
                    continue;
                }
            }
            if (auto* cached = mount_lookup_cache_get_retained(path, PATH_LEN, PIVOT_EPOCH); cached != nullptr) {
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
        for (auto* mount : mounts) {
            if (mount == nullptr || mount->path == nullptr) {
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

auto configure_mount_point_exact(const char* path, FSType expected_type, void* private_data, FileOperations* fops) -> bool {
    if (path == nullptr) {
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
            if (mp == nullptr || mp->path == nullptr) {
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

auto remap_mounts_for_pivot(const char* new_root, const char* put_old) -> int {
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

    mount_lock.lock();

    uint64_t const STABLE_EPOCH = mount_pivot_epoch.load(std::memory_order_acquire);
    if ((STABLE_EPOCH & 1U) != 0 || STABLE_EPOCH > UINT64_MAX - 2) {
        mount_lock.unlock();
        return -EBUSY;
    }
    uint64_t const ODD_EPOCH = STABLE_EPOCH + 1;
    mount_pivot_epoch.store(ODD_EPOCH, std::memory_order_release);

    MountPoint const* new_mount = nullptr;
    MountPoint* old_root_mount = nullptr;
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }
        if (std::strcmp(mp->path, new_root) == 0) {
            new_mount = mp;
        }
        if (std::strcmp(mp->path, "/") == 0) {
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

    if (old_root_mount != nullptr && mount_has_active_refs_locked(old_root_mount)) {
        mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
        mount_lock.unlock();
        return -EBUSY;
    }

    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr || mp == new_mount || mp == old_root_mount) {
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
        if (mp->path_len >= MAX_MOUNT_PATH - NEW_ROOT_LEN) {
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

        if (MP == nullptr || MP->path == nullptr || MP == new_mount ||
            (MP != old_root_mount && path_is_under_root(MP->path, new_root, NEW_ROOT_LEN))) {
            continue;
        }
        if (!replacement_mounts.push_back(MP)) {
            return abort_mount_pivot(ODD_EPOCH, -ENOMEM);
        }
    }

    ker::util::SmallVec<PivotMountReplacement, 8> replacements;
    for (auto* mp : replacement_mounts) {
        size_t const TARGET_LEN = mp == old_root_mount ? PUT_OLD_LEN : NEW_ROOT_LEN + mp->path_len;
        auto* target = new char[TARGET_LEN + 1];
        if (target == nullptr) {
            release_pivot_replacements(replacements);
            return abort_mount_pivot(ODD_EPOCH, -ENOMEM);
        }
        if (mp == old_root_mount) {
            std::memcpy(target, put_old, PUT_OLD_LEN + 1);
        } else {
            std::snprintf(target, TARGET_LEN + 1, "%s%s", new_root, mp->path);
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
            if (!existing_is_replaced && existing != nullptr && existing->path != nullptr &&
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

    mount_lock.lock();
    if (mount_pivot_epoch.load(std::memory_order_acquire) != ODD_EPOCH ||
        mount_generation.load(std::memory_order_acquire) != TABLE_GENERATION) {
        mount_lock.unlock();
        release_pivot_replacements(replacements);
        return abort_mount_pivot(ODD_EPOCH, -EBUSY);
    }
    for (const auto& replacement : replacements) {
        if (mount_has_active_refs_locked(replacement.mount)) {
            mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release);
            mount_lock.unlock();
            release_pivot_replacements(replacements);
            return -EBUSY;
        }
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

void rebase_wki_mounts_for_new_root(const char* new_root) {
    if (new_root == nullptr) {
        return;
    }

    mount_lock.lock();
    uint64_t const ODD_EPOCH = mount_pivot_epoch.load(std::memory_order_acquire);
    if ((ODD_EPOCH & 1U) == 0) {
        mount_lock.unlock();
        return;
    }

    size_t const NEW_ROOT_LEN = std::strlen(new_root);
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr) {
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

auto get_mount_count() -> size_t {
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        size_t const COUNT = mounts.size();
        mount_lock.unlock();
        return COUNT;
    }
}

auto get_mount_at(size_t index) -> MountRef {
    while (true) {
        uint64_t const PIVOT_EPOCH = wait_for_stable_mount_pivot_epoch();
        mount_lock.lock();
        if (mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH) {
            mount_lock.unlock();
            continue;
        }
        if (index >= mounts.size()) {
            mount_lock.unlock();
            return MountRef{};
        }
        MountPoint* mp = mounts.at(index);
        if (!retain_mount_locked(mp)) {
            mp = nullptr;
        }
        mount_lock.unlock();
        return MountRef{mp};
    }
}

auto get_mount_snapshot_at(size_t index, MountSnapshot* out) -> bool {
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
        if (index >= mounts.size()) {
            mount_lock.unlock();
            return false;
        }

        MountPoint const* mp = mounts.at(index);
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

}  // namespace ker::vfs
