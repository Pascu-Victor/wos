#include "task.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <iterator>
#include <memory>
#include <net/wki/remote_compute.hpp>
#include <new>
#ifdef WOS_SELFTEST
#include <platform/acpi/numa_boot.hpp>
#endif
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/loader/gdb_interface.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/user_layout.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/lazy_vmem.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/security/compartment_wki.hpp>
#include <platform/security/file_policy.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/mutex.hpp>
#include <syscalls_impl/process/exec.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>
#include <vfs/content_match.hpp>
#include <vfs/file.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/sched/threading.hpp"
#include "util/hcf.hpp"

extern "C" void wos_kernel_thread_trampoline();  // NOLINT(readability-identifier-naming)

namespace ker::mod::sched::task {
namespace {

struct BootFileCacheEntry {
    std::array<char, 512> path = {};
    ker::vfs::Stat freshness = {};
    uint8_t* buffer = nullptr;
    size_t size = 0;
    uint64_t last_used = 0;
};

constexpr size_t BOOT_FILE_CACHE_MAX_ENTRIES = 4;

std::deque<BootFileCacheEntry> g_boot_file_cache;
ker::mod::sys::Mutex g_boot_file_cache_lock;
std::atomic<uint64_t> g_boot_file_cache_clock{0};
#ifdef WOS_SELFTEST
std::atomic<bool> g_task_selftest_force_fd_clone_insert_failure{false};
#endif

auto boot_file_stat_has_freshness(const ker::vfs::Stat& st) -> bool {
    // Size selects a candidate only. The admitted File must validate every
    // candidate byte and exact EOF before the interpreter can reuse it.
    return st.st_size > 0;
}

auto boot_file_freshness_matches(const ker::vfs::Stat& lhs, const ker::vfs::Stat& rhs) -> bool {
    return lhs.st_size > 0 && lhs.st_size == rhs.st_size;
}

auto boot_file_path_matches(const BootFileCacheEntry& entry, const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }

    size_t const ENTRY_LEN = std::strlen(entry.path.data());
    size_t const PATH_LEN = std::strlen(path);
    return ENTRY_LEN == PATH_LEN && std::strncmp(entry.path.data(), path, PATH_LEN) == 0;
}

auto infer_user_stack_range(Task* task, uint64_t user_sp, uint64_t& stack_base, uint64_t& stack_size) -> bool {
    if (task == nullptr || user_sp == 0) {
        return false;
    }

    uint64_t const STACK_PAGE = page_align_down(user_sp);
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (STACK_PAGE >= range.start && STACK_PAGE < range.end) {
            stack_base = range.start;
            stack_size = range.end - range.start;
            task->lazy_vmem_lock.unlock_irqrestore(IRQF);
            return true;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return false;
}

void build_boot_file_cache_key(const char* submitter, const char* path, char* out, size_t out_size) {
    if (out == nullptr || out_size == 0) {
        return;
    }

    out[0] = '\0';
    char const* const CACHE_SUBMITTER = (submitter != nullptr && submitter[0] != '\0') ? submitter : "-";
    char const* const CACHE_PATH = (path != nullptr) ? path : "";
    std::snprintf(out, out_size, "%s:%s", CACHE_SUBMITTER, CACHE_PATH);
}

auto boot_file_is_dynamic_loader(const char* path) -> bool {
    if (path == nullptr || path[0] == '\0') {
        return false;
    }

    char const* name = path;
    for (char const* cursor = path; *cursor != '\0'; ++cursor) {
        if (*cursor == '/') {
            name = cursor + 1;
        }
    }
    return std::strcmp(name, "ld.so") == 0;
}

auto boot_file_open_flags(const char* path, bool has_remote_submitter_identity) -> int {
    int flags = ker::vfs::O_NOTIFY_CACHE_CHANGE;
    if (!has_remote_submitter_identity && boot_file_is_dynamic_loader(path)) {
        flags |= ker::vfs::O_LOCAL;
    }
    return flags;
}

auto boot_file_cache_lookup_copy(const char* path, const ker::vfs::Stat& freshness, uint8_t** out_buf, size_t* out_size) -> bool {
    if (path == nullptr || out_buf == nullptr || out_size == nullptr) {
        return false;
    }

    g_boot_file_cache_lock.lock();
#ifdef TASK_LDSO_CACHE_DEBUG
    dbg::log("boot_file_cache: probe key='%s' entries=%zu req_size=%ld", path, g_boot_file_cache.size(),
             static_cast<long>(freshness.st_size));
    size_t entry_index = 0;
#endif
    for (auto& entry : g_boot_file_cache) {
        bool const PATH_MATCH = boot_file_path_matches(entry, path);
        bool const FRESHNESS_MATCH = boot_file_freshness_matches(entry.freshness, freshness);
#ifdef TASK_LDSO_CACHE_DEBUG
        dbg::log("boot_file_cache: entry[%zu] key='%s' buf=%p size=%zu st_size=%ld path_match=%d freshness_match=%d", entry_index,
                 entry.path.data(), static_cast<void*>(entry.buffer), entry.size, static_cast<long>(entry.freshness.st_size),
                 PATH_MATCH ? 1 : 0, FRESHNESS_MATCH ? 1 : 0);
        entry_index++;
#endif
        if (entry.buffer == nullptr || entry.size == 0 || !PATH_MATCH || !FRESHNESS_MATCH) {
            continue;
        }

        auto* clone = new uint8_t[entry.size];
        std::memcpy(clone, entry.buffer, entry.size);
        entry.last_used = g_boot_file_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
        *out_buf = clone;
        *out_size = entry.size;
        g_boot_file_cache_lock.unlock();
        return true;
    }
    g_boot_file_cache_lock.unlock();
    return false;
}

void boot_file_cache_store_copy(const char* path, const ker::vfs::Stat& freshness, const uint8_t* buf, size_t size) {
    if (path == nullptr || buf == nullptr || size == 0 || !boot_file_stat_has_freshness(freshness)) {
        return;
    }

    auto* cached = new uint8_t[size];
    std::memcpy(cached, buf, size);

    g_boot_file_cache_lock.lock();
    for (auto& entry : g_boot_file_cache) {
        if (boot_file_path_matches(entry, path) && boot_file_freshness_matches(entry.freshness, freshness)) {
            delete[] entry.buffer;
            entry.buffer = cached;
            entry.size = size;
            entry.freshness = freshness;
            entry.last_used = g_boot_file_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
            g_boot_file_cache_lock.unlock();
            return;
        }
    }

    if (g_boot_file_cache.size() >= BOOT_FILE_CACHE_MAX_ENTRIES) {
        auto victim = g_boot_file_cache.begin();
        for (auto it = g_boot_file_cache.begin(); it != g_boot_file_cache.end(); ++it) {
            if (it->last_used < victim->last_used) {
                victim = it;
            }
        }
        delete[] victim->buffer;
        g_boot_file_cache.erase(victim);
    }

    BootFileCacheEntry entry = {};
    std::strncpy(entry.path.data(), path, entry.path.size() - 1);
    entry.freshness = freshness;
    entry.buffer = cached;
    entry.size = size;
    entry.last_used = g_boot_file_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    g_boot_file_cache.push_back(entry);
#ifdef TASK_LDSO_CACHE_DEBUG
    dbg::log("boot_file_cache: stored key='%s' size=%zu entries=%zu", path, size, g_boot_file_cache.size());
#endif
    g_boot_file_cache_lock.unlock();
}

void release_boot_file(ker::vfs::File* file) {
    if (file == nullptr) {
        return;
    }
    (void)ker::vfs::vfs_close_file(file);
}

void release_cloned_fd_table_refs(Task* task) {
    if (task == nullptr) {
        return;
    }
    task->fd_table.for_each([](uint64_t /*key*/, void* val) {
        if (val != nullptr) {
            ker::vfs::vfs_put_file(static_cast<ker::vfs::File*>(val));
        }
    });
}

#ifdef WOS_SELFTEST
auto clone_user_thread_fds_checked(Task* parent, Task* child) -> bool {
    if (parent == nullptr || child == nullptr) {
        return false;
    }

    bool ok = true;
    uint64_t const IRQF = parent->fd_table_lock.lock_irqsave();
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (!ok) {
            return;
        }

        if (val != nullptr) {
            static_cast<ker::vfs::File*>(val)->refcount.fetch_add(1, std::memory_order_relaxed);
        }
#ifdef WOS_SELFTEST
        if (g_task_selftest_force_fd_clone_insert_failure.load(std::memory_order_relaxed)) {
            if (val != nullptr) {
                static_cast<ker::vfs::File*>(val)->refcount.fetch_sub(1, std::memory_order_relaxed);
            }
            ok = false;
            return;
        }
#endif
        if (!child->fd_table.insert(key, val)) {
            if (val != nullptr) {
                static_cast<ker::vfs::File*>(val)->refcount.fetch_sub(1, std::memory_order_relaxed);
            }
            ok = false;
        }
    });
    parent->fd_table_lock.unlock_irqrestore(IRQF);
    return ok;
}
#endif

auto read_boot_file_fully(Task& image_task, const char* path, uint8_t** out_buf, size_t* out_size, int* interpreter_error,
                          Task* interpreter_executor, bool observe_current_executor) -> bool {
    if (path == nullptr || out_buf == nullptr || out_size == nullptr) {
        return false;
    }

    *out_buf = nullptr;
    *out_size = 0;

    int const SCOPE = security::compartment::remote_namespace_admission(&image_task);
    if (SCOPE != 0 && SCOPE != -ENOTSUP) {
        if (interpreter_error != nullptr) *interpreter_error = SCOPE;
        return false;
    }
    auto* current_task = SCOPE == -ENOTSUP ? nullptr
                         : observe_current_executor
                             ? (ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr)
                             : interpreter_executor;
    [[maybe_unused]] auto const* task_name = (current_task != nullptr && current_task->name != nullptr) ? current_task->name : "?";
    [[maybe_unused]] auto const* task_root = (current_task != nullptr) ? current_task->root.data() : "-";
    [[maybe_unused]] auto const* task_cwd = (current_task != nullptr) ? current_task->cwd.data() : "-";
    bool const HAS_REMOTE_SUBMITTER_IDENTITY = current_task != nullptr && current_task->wki_submitter_hostname.front() != '\0';
    auto const* submitter = HAS_REMOTE_SUBMITTER_IDENTITY ? current_task->wki_submitter_hostname.data() : "-";
    bool constexpr TRACE_INTERP = false;
    bool constexpr ALLOW_BOOT_FILE_CACHE = true;
    std::array<char, 512> cache_key = {};
    build_boot_file_cache_key(submitter, path, cache_key.data(), cache_key.size());

    // A cache hit supplies bytes, not authority. Open the interpreter first
    // so traversal and read admission apply even when another caller warmed
    // this path. Probe the retained object's freshness below, after admission.
    // Keep the kernel-side loader open local for ordinary exec paths, but
    // still honor the submitter's temporary VFS identity during remote task
    // construction so cross-node launches continue to resolve against the
    // submitter when required.
    int const OPEN_FLAGS = boot_file_open_flags(path, HAS_REMOTE_SUBMITTER_IDENTITY);
    auto const CREDENTIALS = image_task.credentials();
    auto& path_context = current_task != nullptr ? *current_task : image_task;
    ker::vfs::File* file = nullptr;
    int const OPEN_RESULT = ker::vfs::vfs_open_read_file_as(path_context, CREDENTIALS, path, (OPEN_FLAGS & ker::vfs::O_LOCAL) == 0, &file);
    if (OPEN_RESULT != 0 || file == nullptr || file->fops == nullptr || file->fops->vfs_read == nullptr ||
        file->fops->vfs_lseek == nullptr) {
        if constexpr (TRACE_INTERP) {
            ker::vfs::Stat statbuf = {};
            int const STAT_RET = ker::vfs::vfs_stat(path, &statbuf);
            dbg::log(
                "read_boot_file_fully: open failed path='%s' task=%s pid=0x%lx root='%s' cwd='%s' submitter='%s' file=%p fops=%p "
                "read=%p lseek=%p fs=%d stat_ret=%d mode=0x%x size=%ld",
                path, task_name, static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), task_root, task_cwd,
                submitter, static_cast<void*>(file), (file != nullptr) ? static_cast<void*>(file->fops) : nullptr,
                (file != nullptr && file->fops != nullptr) ? reinterpret_cast<void*>(file->fops->vfs_read) : nullptr,
                (file != nullptr && file->fops != nullptr) ? reinterpret_cast<void*>(file->fops->vfs_lseek) : nullptr,
                (file != nullptr) ? static_cast<int>(file->fs_type) : -1, STAT_RET, static_cast<unsigned>(statbuf.st_mode),
                static_cast<long>(statbuf.st_size));
        }
        release_boot_file(file);
        return false;
    }

    if constexpr (TRACE_INTERP) {
        auto const PID = static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0);
        auto const* vfs_path = file->vfs_path != nullptr ? file->vfs_path : "-";
        dbg::log("read_boot_file_fully: opened path='%s' task=%s pid=0x%lx root='%s' cwd='%s' submitter='%s' vfs_path='%s' fs=%d", path,
                 task_name, PID, task_root, task_cwd, submitter, vfs_path, static_cast<int>(file->fs_type));
    }

    if (ker::vfs::vfs_stabilize_exec_file(&file) != 0) {
        release_boot_file(file);
        return false;
    }

    ker::vfs::Stat freshness = {};
    if (ker::vfs::vfs_fstat_file(file, &freshness) != 0) {
        release_boot_file(file);
        return false;
    }
    // PT_INTERP supplies executable code. A cached byte copy grants neither
    // read nor execute authority; both use the original admitted identity.
    int const ACCESS_RESULT = security::authorize_file_access(
        CREDENTIALS, {freshness.st_uid, freshness.st_gid, static_cast<uint32_t>(freshness.st_mode & 07777)}, 1);
    if (ACCESS_RESULT != 0) {
        if (interpreter_error != nullptr) *interpreter_error = ACCESS_RESULT;
        release_boot_file(file);
        return false;
    }
    bool const HAVE_FRESHNESS = ALLOW_BOOT_FILE_CACHE && boot_file_stat_has_freshness(freshness);
    if (HAVE_FRESHNESS) {
        size_t cached_size = 0;
        uint8_t* cached = nullptr;
        if (boot_file_cache_lookup_copy(cache_key.data(), freshness, &cached, &cached_size)) {
            constexpr size_t SCRATCH_SIZE = 64 * 1024;
            std::unique_ptr<uint8_t[]> scratch(new (std::nothrow) uint8_t[SCRATCH_SIZE]);
            bool const MATCH =
                scratch && ker::vfs::content_matches(
                               std::span<const uint8_t>(cached, cached_size), std::span<uint8_t>(scratch.get(), SCRATCH_SIZE),
                               [file](uint8_t* bytes, size_t count, size_t offset) {
                                   return ker::vfs::vfs_pread_file(file, bytes, count, static_cast<off_t>(offset));
                               },
                               [] { return true; });
            if (MATCH) {
                *out_buf = cached;
                *out_size = cached_size;
                release_boot_file(file);
                return true;
            }
            delete[] cached;
        }
        if constexpr (TRACE_INTERP) {
            dbg::log("read_boot_file_fully: cache miss key='%s' task=%s pid=0x%lx stat_size=%ld", cache_key.data(), task_name,
                     static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), static_cast<long>(freshness.st_size));
        }
    } else if constexpr (TRACE_INTERP) {
        dbg::log("read_boot_file_fully: cache unavailable key='%s' task=%s pid=0x%lx stat_size=%ld", cache_key.data(), task_name,
                 static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), static_cast<long>(freshness.st_size));
    }

    off_t const FILE_SIZE = file->fops->vfs_lseek(file, 0, 2);
    if (FILE_SIZE <= 0) {
        if constexpr (TRACE_INTERP) {
            dbg::log("read_boot_file_fully: seek-end failed path='%s' task=%s pid=0x%lx size=%ld", path, task_name,
                     static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), static_cast<long>(FILE_SIZE));
        }
        release_boot_file(file);
        return false;
    }

    if constexpr (TRACE_INTERP) {
        dbg::log("read_boot_file_fully: seek-end ok path='%s' task=%s pid=0x%lx size=%ld", path, task_name,
                 static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), static_cast<long>(FILE_SIZE));
    }

    auto const BOOT_FILE_SIZE = static_cast<size_t>(FILE_SIZE);
    auto* buf = new uint8_t[BOOT_FILE_SIZE];
    size_t total_read = 0;
    while (total_read < BOOT_FILE_SIZE) {
        ssize_t const READ_SIZE = file->fops->vfs_read(file, buf + total_read, BOOT_FILE_SIZE - total_read, total_read);
        if (READ_SIZE <= 0) {
            if constexpr (TRACE_INTERP) {
                dbg::log("read_boot_file_fully: read failed path='%s' task=%s pid=0x%lx read=%ld total=%lu size=%lu", path, task_name,
                         static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), static_cast<long>(READ_SIZE),
                         static_cast<unsigned long>(total_read), static_cast<unsigned long>(FILE_SIZE));
            }
            release_boot_file(file);
            delete[] buf;
            return false;
        }
        total_read += static_cast<size_t>(READ_SIZE);
    }
    release_boot_file(file);

    if (HAVE_FRESHNESS && freshness.st_size == FILE_SIZE) {
        boot_file_cache_store_copy(cache_key.data(), freshness, buf, BOOT_FILE_SIZE);
        if constexpr (TRACE_INTERP) {
            dbg::log("read_boot_file_fully: cache store key='%s' task=%s pid=0x%lx size=%zu", cache_key.data(), task_name,
                     static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), BOOT_FILE_SIZE);
        }
    } else if constexpr (TRACE_INTERP) {
        dbg::log("read_boot_file_fully: cache store skipped key='%s' task=%s pid=0x%lx have_freshness=%d stat_size=%ld file_size=%ld",
                 cache_key.data(), task_name, static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0),
                 HAVE_FRESHNESS ? 1 : 0, static_cast<long>(freshness.st_size), static_cast<long>(FILE_SIZE));
    }

    *out_buf = buf;
    *out_size = BOOT_FILE_SIZE;
    return true;
}

}  // namespace

void destroy_unpublished_user_thread(Task* task) {
    if (task == nullptr) {
        return;
    }

    release_lazy_vmem_ranges(*task);
    release_cloned_fd_table_refs(task);
    delete task->thread;
    delete reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    delete[] task->name;
    if (task->context.syscall_kernel_stack >= ker::mod::mm::KERNEL_STACK_SIZE) {
        mm::phys::page_free(reinterpret_cast<void*>(task->context.syscall_kernel_stack - ker::mod::mm::KERNEL_STACK_SIZE));
    }
    delete task;
}

auto claim_unpublished_process(Task* owner, Task* child) -> bool {
    if (owner == nullptr || child == nullptr || owner == child ||
        child->native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr) {
        return false;
    }
    if (!child->try_acquire_lifetime_ref()) {
        return false;
    }
    Task* expected = nullptr;
    if (owner->owned_unpublished_process.compare_exchange_strong(expected, child, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
    }
    child->release();
    return false;
}

auto release_unpublished_process(Task* owner, Task* child) -> bool {
    if (owner == nullptr || child == nullptr || child->native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr) {
        return false;
    }
    Task* expected = child;
    if (!owner->owned_unpublished_process.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel,
                                                                  std::memory_order_acquire)) {
        return false;
    }
    child->release();
    return true;
}

auto take_unpublished_process(Task* owner) -> Task* {
    if (owner == nullptr) {
        return nullptr;
    }
    auto* child = owner->owned_unpublished_process.load(std::memory_order_acquire);
    auto* record = child != nullptr ? child->native_file_mapping_retirement.load(std::memory_order_acquire) : nullptr;
    if (record != nullptr) {
        if (record->controller != owner) dbg::panic_handler("native unpublished slot changed controller without retirement");
        // A begun retirement owns this slot until COMPLETE. Fatal handoffs
        // already defer on the owner's teardown flag; do not detach it again.
        if (record->state != mm::file_mapping::UnpublishedRetirementState::PREPARED) return nullptr;
    }
    // Transfer, rather than drop, the slot's lifetime reference. Exit cleanup
    // must keep the child alive from the atomic detach through the final
    // published-state/refcount validation and teardown.
    return owner->owned_unpublished_process.exchange(nullptr, std::memory_order_acq_rel);
}

namespace {
auto unpublished_process_is_exclusively_owned(Task* task) -> bool {
    // Scheduler publication transfers the creator's initial reference. Never
    // turn this recovery helper into an alternate teardown path for a task
    // that another CPU can already find or run.
    if (task == nullptr || task->scheduler_published.load(std::memory_order_acquire) ||
        task->state.load(std::memory_order_acquire) != TaskState::ACTIVE || task->gc_queued.load(std::memory_order_acquire)) {
        if (task != nullptr) {
            ker::mod::dbg::log("Refusing to destroy published/shared task PID %x as an unpublished process", task->pid);
        }
        return false;
    }
    if (task->native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr ||
        task->ref_count.load(std::memory_order_acquire) != 2) {
        uint32_t self_refs = 0;
        if (task->type != TaskType::PROCESS || task->is_thread ||
            ker::vfs::vfs_count_exclusive_unpublished_self_refs(*task, self_refs) != 0 ||
            task->ref_count.load(std::memory_order_acquire) != 2 + self_refs || task->scheduler_published.load(std::memory_order_acquire) ||
            task->gc_queued.load(std::memory_order_acquire) || task->state.load(std::memory_order_acquire) != TaskState::ACTIVE) {
            ker::mod::dbg::log("Refusing to destroy shared/self-referenced unpublished task PID %x", task->pid);
            return false;
        }
    }
    return true;
}

void teardown_unpublished_process_resources(Task* task, mm::file_mapping::UnpublishedMappingRetirement* native = nullptr) {
    // A private creator may have attached loader scratch before the first read.
    // The child is exclusively unpublished; it cannot be another live executor.
    ker::syscall::process::cleanup_exec_preparation(*task);
    task->root_anchor = {};
    task->cwd_anchor = {};
    loader::debug::unregister_process(task->pid);
    loader::debug::remove_gdb_debug_info(task->pid);
    release_lazy_vmem_ranges(*task);
    for (unsigned fd = 0; fd < Task::FD_TABLE_SIZE; ++fd) {
        auto* file = static_cast<ker::vfs::File*>(task->fd_table.lookup(fd));
        if (file == nullptr) {
            continue;
        }
        task->fd_table.remove(fd);
        task->clear_fd_cloexec(fd);
        ker::vfs::vfs_put_file(file);
    }

    if (task->is_elf_buffer_shared) {
        static_cast<void>(ker::net::wki::wki_remote_compute_release_elf_buffer(task->elf_buffer));
    } else {
        delete[] task->elf_buffer;
    }
    task->elf_buffer = nullptr;
    task->elf_buffer_size = 0;
    task->is_elf_buffer_shared = false;
    if (task->exec_image_file != nullptr) {
        ker::vfs::vfs_put_file(task->exec_image_file);
        task->exec_image_file = nullptr;
        task->exec_image_size = 0;
    }

    if (task->thread != nullptr) {
        // User pages belong to the pagemap destroyed below. The Thread row
        // still has to be retired without independently freeing those pages.
        task->thread->tls_phys_ptr = 0;
        task->thread->stack_phys_ptr = 0;
        threading::destroy_thread(task->thread);
        task->thread = nullptr;
    }

    delete reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    task->context.syscall_scratch_area = 0;

    if (task->pagemap != nullptr && task->pagemap != mm::virt::get_kernel_pagemap()) {
        auto* pagemap = task->detach_pagemap_after_usercopy_quiescence();
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(pagemap);
        if (native != nullptr) {
            native->retired_root = pagemap;
        } else {
            mm::virt::destroy_user_space(pagemap, task->pid, task->name, "unpublished-process");
            mm::virt::release_pagemap(pagemap);
        }
    }

    if (native != nullptr) return;  // Root provenance/name survive resumable walking.
    delete[] task->name;
    task->name = nullptr;
    if (task->context.syscall_kernel_stack >= ker::mod::mm::KERNEL_STACK_SIZE) {
        mm::phys::page_free(reinterpret_cast<void*>(task->context.syscall_kernel_stack - ker::mod::mm::KERNEL_STACK_SIZE));
        task->context.syscall_kernel_stack = 0;
    }
}
}  // namespace

namespace {
sys::Spinlock native_retirement_queue_lock;
mm::file_mapping::UnpublishedMappingRetirement* native_retirement_queue_head{};
mm::file_mapping::UnpublishedMappingRetirement* native_retirement_queue_tail{};
size_t native_retirement_queue_outstanding{};  // Includes the single GC-claimed prefix.
sys::Spinlock native_mapping_admission_lock;
mm::file_mapping::UnpublishedMappingRetirement* native_mapping_active_records{};
size_t native_mapping_constructions{};
bool native_mapping_construction_closed{};

void unregister_native_file_mapping_record(Task& child, mm::file_mapping::UnpublishedMappingRetirement& record) {
    uint64_t const FLAGS = native_mapping_admission_lock.lock_irqsave();
    auto** link = &native_mapping_active_records;
    while (*link != nullptr && *link != &record) link = &(*link)->active_next;
    if (*link != &record || record.active_subject != &child) __builtin_trap();
    *link = record.active_next;
    record.active_next = nullptr;
    record.active_subject = nullptr;
    native_mapping_admission_lock.unlock_irqrestore(FLAGS);
}

void append_native_retirement_locked(mm::file_mapping::UnpublishedMappingRetirement& record) {
    record.deferred_next = nullptr;
    if (native_retirement_queue_tail != nullptr)
        native_retirement_queue_tail->deferred_next = &record;
    else
        native_retirement_queue_head = &record;
    native_retirement_queue_tail = &record;
}

auto advance_native_unpublished_process(Task* owner, Task* child, mm::file_mapping::UnpublishedRetirementMode mode, int* close_result,
                                        bool detached) -> mm::file_mapping::UnpublishedRetirementResult {
    namespace fm = mm::file_mapping;
    using Result = fm::UnpublishedRetirementResult;
    if (owner == nullptr || child == nullptr || owner == child || !sched::can_query_current_task() || sched::get_current_task() != owner ||
        !sched::interrupts_enabled() || sched::preempt_count() != 0 || fm::LifetimeRetirementExecutor::is_current_worker())
        return Result::REFUSED;
    auto* record = child->native_file_mapping_retirement.load(std::memory_order_acquire);
    if (record == nullptr || (detached ? record->controller != nullptr || record->deferred_subject != child || !record->deferred_claimed ||
                                             mode != fm::UnpublishedRetirementMode::DEFERRED_TRANSFERRED
                                       : record->controller != owner || mode == fm::UnpublishedRetirementMode::DEFERRED_TRANSFERRED))
        return Result::REFUSED;
    auto const HOLD = sched::hold_kernel_shutdown();
    if (record->state == fm::UnpublishedRetirementState::PREPARED) {
        bool expected = false;
        if (!detached && !owner->unpublished_teardown_in_progress.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
            return Result::REFUSED;
        bool const SLOT = owner->owned_unpublished_process.load(std::memory_order_acquire) == child;
        if ((!detached && (mode == fm::UnpublishedRetirementMode::OWNED_SLOT ? !SLOT : SLOT)) ||
            !unpublished_process_is_exclusively_owned(child)) {
            if (!detached) owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
            return Result::REFUSED;
        }
        record->mode = mode;
        record->state = fm::UnpublishedRetirementState::RETIRING;
        teardown_unpublished_process_resources(child, record);
    } else if (record->mode != mode || (!detached && !owner->unpublished_teardown_in_progress.load(std::memory_order_acquire))) {
        return Result::REFUSED;
    }
    if (record->state == fm::UnpublishedRetirementState::RETIRING) {
        // A RAW fork may wait IRQ-off on a colliding root stripe. Keep this
        // gate's owner running until release; never park it behind that waiter.
        // The admitted <=49 tables/512 leaves consume <=1217 walker steps.
        // Empty-first-root file-only construction has no anonymous swap rows.
        sched::PreemptGuard const ROOT_RETIREMENT;
        if (record->retired_root != nullptr) {
            if (!record->root_walk_started) {
                if (!mm::virt::begin_prepared_destroy_user_space(*record->root_walk, record->retired_root, child->pid, child->name,
                                                                 "native-unpublished-process", false))
                    return Result::PENDING;
                record->root_walk_started = true;
            }
            if (!mm::virt::destroy_user_space_budgeted(record->root_walk, 2048))
                dbg::panic_handler("native root exceeded bounded nonpreemptible retirement profile");
            if (!mm::virt::finish_destroy_user_space_and_release_pagemap(record->root_walk))
                dbg::panic_handler("native completed root continuation lost final owner");
        } else {
            mm::virt::destroy_user_space_budget_state_destroy(record->root_walk);
        }
        record->root_walk = nullptr;
        record->retired_root = nullptr;
        record->root_walk_started = false;
        record->state = fm::UnpublishedRetirementState::WAITING_RETURN;
    }
    for (size_t i = 0; i < record->count; ++i) {
        auto const& completion = record->entries.at(i).completion;
        if (completion.empty()) dbg::panic_handler("native counted File retirement observer missing");
        auto const RETURN = completion.query();
        if (RETURN.state == fm::RetirementReturnState::INVALID || RETURN.state == fm::RetirementReturnState::EMPTY)
            dbg::panic_handler("native counted File retirement observer invalid");
        if (RETURN.state != fm::RetirementReturnState::RETURNED) return Result::PENDING;
        if (record->first_close_result == 0 && RETURN.callback_result != 0) record->first_close_result = RETURN.callback_result;
    }
    if (child->ref_count.load(std::memory_order_acquire) != 2) return Result::PENDING;
    if (mode == fm::UnpublishedRetirementMode::OWNED_SLOT) {
        Task* expected = child;
        if (!owner->owned_unpublished_process.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel))
            dbg::panic_handler("native unpublished process lost exact owner slot");
    }
    if (close_result != nullptr) *close_result = record->first_close_result;
    child->native_file_mapping_retirement.store(nullptr, std::memory_order_release);
    unregister_native_file_mapping_record(*child, *record);
    delete[] child->name;
    child->name = nullptr;
    if (child->context.syscall_kernel_stack >= mm::KERNEL_STACK_SIZE)
        mm::phys::page_free(reinterpret_cast<void*>(child->context.syscall_kernel_stack - mm::KERNEL_STACK_SIZE));
    child->context.syscall_kernel_stack = 0;
    delete record;
    child->release();  // Consume the exact slot/transferred owner, then creator.
    delete child;
    if (!detached) owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
    return Result::COMPLETE;
}
}  // namespace

auto retire_native_unpublished_process(Task* owner, Task* child, mm::file_mapping::UnpublishedRetirementMode mode, int* close_result)
    -> mm::file_mapping::UnpublishedRetirementResult {
    return advance_native_unpublished_process(owner, child, mode, close_result, false);
}

auto begin_native_file_mapping_construction() -> bool {
    uint64_t const FLAGS = native_mapping_admission_lock.lock_irqsave();
    bool const ADMITTED = !native_mapping_construction_closed && native_mapping_constructions != SIZE_MAX;
    if (ADMITTED) ++native_mapping_constructions;
    native_mapping_admission_lock.unlock_irqrestore(FLAGS);
    return ADMITTED;
}

void end_native_file_mapping_construction() {
    uint64_t const FLAGS = native_mapping_admission_lock.lock_irqsave();
    if (native_mapping_constructions == 0) __builtin_trap();
    --native_mapping_constructions;
    native_mapping_admission_lock.unlock_irqrestore(FLAGS);
}

void register_native_file_mapping_record(Task& child, mm::file_mapping::UnpublishedMappingRetirement& record) {
    uint64_t const FLAGS = native_mapping_admission_lock.lock_irqsave();
    if (native_mapping_constructions == 0 || record.active_subject != nullptr) __builtin_trap();
    record.active_subject = &child;
    record.active_next = native_mapping_active_records;
    native_mapping_active_records = &record;
    native_mapping_admission_lock.unlock_irqrestore(FLAGS);
}

auto close_native_file_mapping_admission() -> bool {
    uint64_t const FLAGS = native_mapping_admission_lock.lock_irqsave();
    native_mapping_construction_closed = true;
    bool const DRAINED = native_mapping_constructions == 0 && native_mapping_active_records == nullptr;
    native_mapping_admission_lock.unlock_irqrestore(FLAGS);
    return DRAINED;
}

auto native_unpublished_retirements_pending() -> bool {
    uint64_t const FLAGS = native_retirement_queue_lock.lock_irqsave();
    bool const PENDING = native_retirement_queue_outstanding != 0;
    native_retirement_queue_lock.unlock_irqrestore(FLAGS);
    return PENDING;
}

auto process_native_unpublished_retirements_slice(bool& root_active) -> bool {
    root_active = false;
    if (!sched::is_current_gc_worker() || !sched::interrupts_enabled() || sched::preempt_count() != 0)
        return native_unpublished_retirements_pending();
    uint64_t flags = native_retirement_queue_lock.lock_irqsave();
    auto* record = native_retirement_queue_head;
    if (record != nullptr) {
        native_retirement_queue_head = record->deferred_next;
        if (native_retirement_queue_head == nullptr) native_retirement_queue_tail = nullptr;
        record->deferred_next = nullptr;
        record->deferred_claimed = true;
    }
    native_retirement_queue_lock.unlock_irqrestore(flags);
    if (record == nullptr) return native_unpublished_retirements_pending();
    auto const RESULT =
        advance_native_unpublished_process(sched::get_current_task(), record->deferred_subject,
                                           mm::file_mapping::UnpublishedRetirementMode::DEFERRED_TRANSFERRED, nullptr, true);
    flags = native_retirement_queue_lock.lock_irqsave();
    if (RESULT == mm::file_mapping::UnpublishedRetirementResult::COMPLETE) {
        // COMPLETE deleted the record; only permanent queue accounting remains.
        if (native_retirement_queue_outstanding == 0) __builtin_trap();
        --native_retirement_queue_outstanding;
    } else {
        root_active = record->state == mm::file_mapping::UnpublishedRetirementState::RETIRING && record->root_walk_started;
        record->deferred_claimed = false;
        if (root_active) {
            // Hashed root gates remain exclusive across slices. Finish this
            // exact walk before another native or ordinary GC root can wait
            // for a gate whose only progressing owner is this same worker.
            record->deferred_next = native_retirement_queue_head;
            native_retirement_queue_head = record;
            if (native_retirement_queue_tail == nullptr) native_retirement_queue_tail = record;
        } else {
            append_native_retirement_locked(*record);
        }
    }
    bool const PENDING = native_retirement_queue_outstanding != 0;
    native_retirement_queue_lock.unlock_irqrestore(flags);
    return PENDING;
}

namespace {
void defer_transferred_native_unpublished_process(Task& owner, Task& child) {
    auto* record = child.native_file_mapping_retirement.load(std::memory_order_acquire);
    if (record == nullptr || record->controller != &owner || record->state != mm::file_mapping::UnpublishedRetirementState::PREPARED ||
        owner.owned_unpublished_process.load(std::memory_order_acquire) == &child || record->deferred_subject != nullptr)
        dbg::panic_handler("native transfer refusal lost exact recoverable owner");
    record->controller = nullptr;
    record->deferred_subject = &child;  // Transfer the caller's existing ref; never drop it.
    record->mode = mm::file_mapping::UnpublishedRetirementMode::DEFERRED_TRANSFERRED;
    uint64_t const FLAGS = native_retirement_queue_lock.lock_irqsave();
    if (native_retirement_queue_outstanding == SIZE_MAX) __builtin_trap();
    ++native_retirement_queue_outstanding;
    append_native_retirement_locked(*record);
    native_retirement_queue_lock.unlock_irqrestore(FLAGS);
    sched::request_gc();  // Ordinary ACTIVE caller, outside all queue/MM guards.
}

auto drain_native_unpublished_process(Task* owner, Task* child, mm::file_mapping::UnpublishedRetirementMode mode) -> bool {
    for (;;) {
        auto const RESULT = retire_native_unpublished_process(owner, child, mode);
        if (RESULT == mm::file_mapping::UnpublishedRetirementResult::COMPLETE) return true;
        if (RESULT == mm::file_mapping::UnpublishedRetirementResult::REFUSED) return false;
        sched::kern_yield();
    }
}
}  // namespace

auto destroy_unpublished_process(Task* task) -> bool {
    if (task == nullptr) {
        return true;
    }
    if (task->native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr) {
        auto* owner = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
        bool const COMPLETE = drain_native_unpublished_process(owner, task, mm::file_mapping::UnpublishedRetirementMode::TRANSFERRED_SLOT);
        if (!COMPLETE) {
            if (owner == nullptr) dbg::panic_handler("native transferred cleanup outside its actual controller");
            defer_transferred_native_unpublished_process(*owner, *task);
        }
        return COMPLETE;
    }
    if (!unpublished_process_is_exclusively_owned(task)) {
        // take_unpublished_process() transferred one reference to this caller.
        // A refusal must still return that reference to the published owner.
        task->release();
        return false;
    }

    teardown_unpublished_process_resources(task);
    task->release();  // Consume the transferred owner-slot reference.
    delete task;
    return true;
}

auto destroy_owned_unpublished_process(Task* owner, Task* child) -> bool {
    if (owner == nullptr || child == nullptr || owner == child) {
        return false;
    }
    if (child->native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr)
        return drain_native_unpublished_process(owner, child, mm::file_mapping::UnpublishedRetirementMode::OWNED_SLOT);

    bool expected_teardown = false;
    if (!owner->unpublished_teardown_in_progress.compare_exchange_strong(expected_teardown, true, std::memory_order_acq_rel,
                                                                         std::memory_order_acquire)) {
        return false;
    }

    // Keep the slot and its reference published throughout every operation
    // that may enter a voluntary VFS wait. Scheduler/group-exit handoffs see
    // the flag above and leave fatal exit pending until this section finishes.
    bool const OWNED = owner->owned_unpublished_process.load(std::memory_order_acquire) == child;
    if (!OWNED || !unpublished_process_is_exclusively_owned(child)) {
        owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
        return false;
    }

    teardown_unpublished_process_resources(child);

    Task* const TAKEN = owner->owned_unpublished_process.exchange(nullptr, std::memory_order_acq_rel);
    if (TAKEN != child) [[unlikely]] {
        dbg::panic_handler("unpublished process owner changed during deferred-exit teardown");
        hcf();
    }
    child->release();  // Consume the owner-slot reference.
    delete child;
    owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
    return true;
}

auto complete_unpublished_process_construction(Task* task, const mm::numa::AllocationContext& allocation_context, int* interpreter_error,
                                               Task* interpreter_executor, bool observe_current_executor) -> bool {
    if (interpreter_error != nullptr) *interpreter_error = 0;
    if (task != nullptr && task->native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr) return false;
    if (task == nullptr || task->type != TaskType::PROCESS || task->thread == nullptr || task->pagemap == nullptr) {
        return false;
    }
    if (task->pending_interp_path.front() == '\0') {
        return true;
    }

    uint8_t* interp_buf = nullptr;
    size_t interp_size = 0;
    if (!read_boot_file_fully(*task, task->pending_interp_path.data(), &interp_buf, &interp_size, interpreter_error, interpreter_executor,
                              observe_current_executor)) {
        dbg::log("Failed to open interpreter '%s' for task %s", task->pending_interp_path.data(), task->name);
        return false;
    }

    loader::elf::ElfLoadOptions const INTERP_OPTIONS{
        .register_special_symbols = false,
        .base_address = 0,
        .lazy_file_ranges = nullptr,
        .debug_registry_pid = 0,
        .image_role = mm::user_layout::ImageRole::INTERPRETER,
        .policy_owner = task,
        .allocation_context = allocation_context,
        .allocation_origin = loader::elf::ElfAllocationOrigin::EXECUTOR_CAPTURE,
    };
    loader::elf::ElfLoadResult const INTERP_RESULT =
        loader::elf::load_elf(interp_buf, interp_size, task->pagemap, task->pid, "ld.so", INTERP_OPTIONS);
    delete[] interp_buf;
    if (INTERP_RESULT.entry_point == 0) {
        dbg::log("Failed to load interpreter ELF '%s'", task->pending_interp_path.data());
        if (interpreter_error != nullptr && INTERP_RESULT.error != 0) *interpreter_error = INTERP_RESULT.error;
        return false;
    }

    task->context.frame.rip = INTERP_RESULT.entry_point;
    task->interp_base = INTERP_RESULT.load_base;
    task->interp_vaddr_start = INTERP_RESULT.image_start;
    task->interp_vaddr_end = INTERP_RESULT.image_end;
    task->pending_interp_path.fill('\0');
    (void)resource::observe_address_space(*task);
    return true;
}

auto clone_lazy_vmem_ranges(Task& dst, Task& src) -> bool {
    if (!dst.lazy_vmem_ranges.empty()) return false;
    size_t retained = 0;
    uint64_t const IRQF = src.lazy_vmem_lock.lock_irqsave();
    if (src.native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr) {
        src.lazy_vmem_lock.unlock_irqrestore(IRQF);
        return false;
    }
    bool ok = true;
    for (auto const& range : src.lazy_vmem_ranges) ok &= !lazy_vmem_has_lifetime(range);
    if (ok) ok = dst.lazy_vmem_ranges.clone_from(src.lazy_vmem_ranges);
    if (ok) {
        for (const auto& range : dst.lazy_vmem_ranges) {
            if (!try_retain_lazy_vmem_range(range)) {
                ok = false;
                break;
            }
            ++retained;
        }
    }
    src.lazy_vmem_lock.unlock_irqrestore(IRQF);
    if (!ok) {
        for (size_t i = 0; i < retained; ++i) release_lazy_vmem_range(dst.lazy_vmem_ranges.at(i));
        dst.lazy_vmem_ranges.clear();
    }
    return ok;
}

void release_lazy_vmem_ranges(Task& task) {
    LazyVmemRangeVec ranges;
    uint64_t const IRQF = task.lazy_vmem_lock.lock_irqsave();
    ranges = std::move(task.lazy_vmem_ranges);
    task.lazy_vmem_ranges = LazyVmemRangeVec{};
    task.lazy_vmem_lock.unlock_irqrestore(IRQF);

    release_lazy_vmem_range_refs(ranges);
}

#ifdef WOS_SELFTEST
auto task_selftest_fd_clone_failure_releases_refs() -> bool {
    Task parent{};
    Task child{};
    ker::vfs::File file{};
    file.refcount.store(1, std::memory_order_relaxed);

    constexpr uint64_t FD = 19;
    if (!parent.fd_table.insert(FD, &file)) {
        return false;
    }

    g_task_selftest_force_fd_clone_insert_failure.store(true, std::memory_order_relaxed);
    bool const CLONED = clone_user_thread_fds_checked(&parent, &child);
    g_task_selftest_force_fd_clone_insert_failure.store(false, std::memory_order_relaxed);

    bool const OK = !CLONED && file.refcount.load(std::memory_order_relaxed) == 1 && child.fd_table.lookup(FD) == nullptr;
    parent.fd_table.remove(FD);
    return OK;
}

auto task_selftest_destroy_unpublished_user_thread_releases_refs() -> bool {
    auto* task = new Task{};
    if (task == nullptr) {
        return false;
    }

    ker::vfs::File file{};
    file.refcount.store(2, std::memory_order_relaxed);

    constexpr uint64_t FD = 23;
    if (!task->fd_table.insert(FD, &file)) {
        delete task;
        return false;
    }

    destroy_unpublished_user_thread(task);
    return file.refcount.load(std::memory_order_relaxed) == 1;
}

auto task_selftest_unpublished_process_owner_releases_resources() -> bool {
    Task owner{};
    auto* child = new Task{};
    if (child == nullptr) {
        return false;
    }

    ker::vfs::File file{};
    file.refcount.store(2, std::memory_order_relaxed);
    ker::vfs::File exec_file{};
    exec_file.refcount.store(2, std::memory_order_relaxed);
    constexpr uint64_t FD = 29;
    if (!child->fd_table.insert(FD, &file)) {
        delete child;
        return false;
    }
    child->elf_buffer = new uint8_t[4];
    child->elf_buffer_size = 4;
    child->exec_image_file = &exec_file;
    child->exec_image_size = 4096;
    if (child->elf_buffer == nullptr) {
        child->fd_table.remove(FD);
        delete child;
        return false;
    }

    bool const CLAIMED = claim_unpublished_process(&owner, child);
    bool const OWNER_REF_HELD = child->ref_count.load(std::memory_order_acquire) == 2;
    bool const DUPLICATE_REJECTED = !claim_unpublished_process(&owner, child);
    bool const WRONG_RELEASE_REJECTED = !release_unpublished_process(&owner, &owner);
    Task* const TAKEN = take_unpublished_process(&owner);
    bool const OWNER_CLEARED = owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr;
    bool const TRANSFERRED_REF_HELD = TAKEN == child && child->ref_count.load(std::memory_order_acquire) == 2;
    bool const DESTROYED = destroy_unpublished_process(TAKEN);
    return CLAIMED && OWNER_REF_HELD && DUPLICATE_REJECTED && WRONG_RELEASE_REJECTED && OWNER_CLEARED && TRANSFERRED_REF_HELD &&
           DESTROYED && file.refcount.load(std::memory_order_relaxed) == 1 && exec_file.refcount.load(std::memory_order_relaxed) == 1;
}

auto task_selftest_owned_unpublished_process_teardown_releases_resources() -> bool {
    Task owner{};
    auto* child = new Task{};
    if (child == nullptr) {
        return false;
    }

    ker::vfs::File file{};
    file.refcount.store(2, std::memory_order_relaxed);
    constexpr uint64_t FD = 31;
    if (!child->fd_table.insert(FD, &file)) {
        delete child;
        return false;
    }
    if (!claim_unpublished_process(&owner, child)) {
        child->fd_table.remove(FD);
        delete child;
        return false;
    }

    bool const DESTROYED = destroy_owned_unpublished_process(&owner, child);
    return DESTROYED && owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr &&
           !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire) && file.refcount.load(std::memory_order_relaxed) == 1;
}

auto task_selftest_published_process_refuses_unpublished_teardown() -> bool {
    Task owner{};
    auto* child = new Task{};
    if (child == nullptr || !claim_unpublished_process(&owner, child)) {
        delete child;
        return false;
    }

    child->scheduler_published.store(true, std::memory_order_release);
    Task* const TAKEN = take_unpublished_process(&owner);
    bool const REFUSED = !destroy_unpublished_process(TAKEN);
    bool const INITIAL_REF_REMAINS = child->ref_count.load(std::memory_order_acquire) == 1;
    bool const OWNER_CLEARED = owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr;
    delete child;
    return REFUSED && INITIAL_REF_REMAINS && OWNER_CLEARED;
}

#endif

auto Task::wki_execution_identity() const -> WkiExecutionIdentity {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    WkiExecutionIdentity identity{.submitter_hostname = wki_submitter_hostname,
                                  .remote_pid = wki_remote_pid,
                                  .proxy_task_id = wki_proxy_task_id,
                                  .signal_task_id = m_accepted_signal_task_id};
    path_metadata_lock.unlock_irqrestore(FLAGS);
    identity.submitter_hostname.back() = '\0';
    return identity;
}

void Task::set_wki_remote_pid(uint64_t remote_pid) {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    wki_remote_pid = remote_pid;
    path_metadata_lock.unlock_irqrestore(FLAGS);
}

void Task::set_wki_proxy_task_id(uint32_t proxy_task_id) {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    wki_proxy_task_id = proxy_task_id;
    path_metadata_lock.unlock_irqrestore(FLAGS);
}

auto Task::capture_filesystem_context(FilesystemContext& output) const -> bool {
    auto const* owner = filesystem_owner();
    if (owner == nullptr) return false;
    FilesystemContext candidate;
    uint64_t const FLAGS = owner->path_metadata_lock.lock_irqsave();
    candidate.root_anchor = owner->root_anchor;
    candidate.root = owner->root;
    candidate.root_len = owner->root_len;
    candidate.cwd = owner->cwd;
    candidate.cwd_len = owner->cwd_len;
    candidate.cwd_anchor = owner->cwd_anchor;
    owner->path_metadata_lock.unlock_irqrestore(FLAGS);
    output = std::move(candidate);
    return true;
}

void Task::publish_accepted_signal_route(uint32_t task_id, uint64_t remote_pid) {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    if (task_id != 0 && (m_accepted_signal_task_id == 0 || m_accepted_signal_task_id == task_id)) {
        m_accepted_signal_task_id = task_id;
        wki_remote_pid = remote_pid;
    }
    path_metadata_lock.unlock_irqrestore(FLAGS);
}

namespace {
auto allocate_signal_home_serial(std::atomic<uint64_t>& counter) -> uint64_t {
    uint64_t next = counter.load(std::memory_order_relaxed);
    while (next != UINT64_MAX) {
        if (counter.compare_exchange_weak(next, next + 1, std::memory_order_relaxed)) return next;
    }
    return 0;
}
}  // namespace

auto Task::signal_home_identity(bool initialize) const -> SignalHomeIdentity {
    auto* owner = credential_owner();
    if (owner == nullptr) return {};
    if (owner != this) return owner->signal_home_identity(initialize);
    static std::atomic<uint64_t> next_serial{1};
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    if (initialize && m_signal_home_serial == 0 && m_signal_home_anchor) {
        m_signal_home_serial = allocate_signal_home_serial(next_serial);
    }
    SignalHomeIdentity const RESULT{.origin = m_signal_home_origin, .serial = m_signal_home_serial};
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto Task::is_signal_home_anchor() const -> bool {
    if (is_thread) return false;
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    bool const RESULT = m_signal_home_anchor;
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto Task::signal_receiver_serial() const -> uint64_t {
    auto* owner = credential_owner();
    if (owner == nullptr) return 0;
    if (owner != this) return owner->signal_receiver_serial();
    static std::atomic<uint64_t> next_serial{1};
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    if (m_signal_receiver_serial == 0) m_signal_receiver_serial = allocate_signal_home_serial(next_serial);
    uint64_t const SERIAL = m_signal_receiver_serial;
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return SERIAL;
}

auto Task::adopt_signal_home(SignalHomeIdentity identity) -> bool {
    if (is_thread || scheduler_published.load(std::memory_order_acquire) || identity.serial == 0 || identity.origin.node == 0 ||
        identity.origin.boot == 0 || identity.origin.node == UINT16_MAX)
        return false;
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    bool const ALLOWED = m_signal_home_anchor && m_signal_home_serial == 0;
    if (ALLOWED) {
        m_signal_home_serial = identity.serial;
        m_signal_home_origin = identity.origin;
        m_signal_home_anchor = false;
    }
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return ALLOWED;
}

void Task::hide_group_signals_until_publication() {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    m_group_signal_visible = false;
    path_metadata_lock.unlock_irqrestore(FLAGS);
}
void Task::publish_group_signal_visibility() {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    m_group_signal_visible = true;
    path_metadata_lock.unlock_irqrestore(FLAGS);
}
auto Task::group_signal_visible() const -> bool {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    bool const VISIBLE = m_group_signal_visible;
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return VISIBLE;
}
auto Task::generate_group_signal(uint64_t mask) -> bool {
    signal_generate_pending_mask(mask);
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    bool const VISIBLE = m_group_signal_visible;
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return VISIBLE;
}

#ifdef WOS_SELFTEST
auto task_selftest_signal_home_and_visibility() -> bool {
    std::atomic<uint64_t> counter{UINT64_MAX - 1};
    bool ok =
        allocate_signal_home_serial(counter) == UINT64_MAX - 1 && allocate_signal_home_serial(counter) == 0 && counter.load() == UINT64_MAX;
    Task first{}, reused{}, receiver{}, local_child{};
    first.pid = reused.pid = 77;
    ok &= first.signal_home_identity(false).serial == 0;
    auto const FIRST = first.signal_home_identity();
    auto const REUSED = reused.signal_home_identity();
    ok &= FIRST.serial != 0 && REUSED.serial != 0 && FIRST.serial != REUSED.serial && first.signal_home_identity().serial == FIRST.serial;
    ok &= first.is_signal_home_anchor() && !first.adopt_signal_home({.origin = {.node = 7, .boot = 8}, .serial = 44});
    ok &= !receiver.adopt_signal_home({}) && receiver.adopt_signal_home({.origin = {.node = 7, .boot = 8}, .serial = FIRST.serial});
    ok &= !receiver.is_signal_home_anchor() && receiver.signal_home_identity().serial == FIRST.serial &&
          !receiver.adopt_signal_home({.origin = {.node = 9, .boot = 10}, .serial = REUSED.serial});
    ok &= local_child.is_signal_home_anchor() && local_child.signal_home_identity().serial != FIRST.serial;
    auto const FIRST_RECEIVER = first.signal_receiver_serial();
    auto const IMPORTED_RECEIVER = receiver.signal_receiver_serial();
    ok &= FIRST_RECEIVER != 0 && IMPORTED_RECEIVER != 0 && FIRST_RECEIVER != IMPORTED_RECEIVER &&
          first.signal_receiver_serial() == FIRST_RECEIVER && reused.signal_receiver_serial() != FIRST_RECEIVER;
    {
        Task thread{};
        thread.is_thread = true;
        ok &= thread.signal_home_identity().serial == 0;
        ok &= thread.resource_owner.attach(&receiver);
        ok &= thread.signal_home_identity().serial == FIRST.serial && !thread.is_signal_home_anchor();
        ok &= thread.signal_receiver_serial() == IMPORTED_RECEIVER;
    }
    constexpr uint64_t SIGNAL = uint64_t{1} << 11;
    receiver.hide_group_signals_until_publication();
    ok &= !receiver.generate_group_signal(SIGNAL) && receiver.signal_pending_bits() == SIGNAL;
    receiver.publish_group_signal_visibility();
    ok &= receiver.group_signal_visible() && receiver.generate_group_signal(SIGNAL) && receiver.signal_pending_bits() == SIGNAL;
    return ok && first.ref_count.load() == 1 && receiver.ref_count.load() == 1;
}
#endif

auto Task::signal_admission_identity() const -> SignalAdmissionIdentity {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    SignalAdmissionIdentity identity{
        .credentials = credentials(), .job = job_identity(), .remote = m_accepted_signal_task_id != 0 || wki_proxy_task_id != 0};
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return identity;
}

auto Task::begin_local_signal_admission(session::Names* visible_job) -> SignalAdmissionIdentity {
    uint64_t const FLAGS = path_metadata_lock.lock_irqsave();
    auto const JOB = job_identity_snapshot();
    SignalAdmissionIdentity identity{
        .credentials = credentials(), .job = JOB.internal, .remote = m_accepted_signal_task_id != 0 || wki_proxy_task_id != 0};
    if (visible_job != nullptr) *visible_job = JOB.visible;
    if (!identity.remote && m_local_signal_admissions.load(std::memory_order_relaxed) != UINT32_MAX) {
        m_local_signal_admissions.fetch_add(1, std::memory_order_relaxed);
        identity.local_lease = true;
    }
    path_metadata_lock.unlock_irqrestore(FLAGS);
    return identity;
}

void Task::end_local_signal_admission() { m_local_signal_admissions.fetch_sub(1, std::memory_order_release); }

auto Task::local_signal_admissions_drained() const -> bool { return m_local_signal_admissions.load(std::memory_order_acquire) == 0; }

auto Task::try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) -> bool {
    out = nullptr;
    if (state.load(std::memory_order_acquire) != TaskState::ACTIVE || usercopy_pagemap_closing.load(std::memory_order_acquire)) {
        return false;
    }

    uint32_t accesses = usercopy_pagemap_accesses.load(std::memory_order_acquire);
    while (accesses != UINT32_MAX) {
        if (usercopy_pagemap_accesses.compare_exchange_weak(accesses, accesses + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }
    if (accesses == UINT32_MAX) {
        return false;
    }

    if (state.load(std::memory_order_acquire) != TaskState::ACTIVE || usercopy_pagemap_closing.load(std::memory_order_acquire) ||
        pagemap == nullptr) {
        release_usercopy_pagemap();
        return false;
    }

    out = pagemap;
    return true;
}

void Task::release_usercopy_pagemap() { usercopy_pagemap_accesses.fetch_sub(1, std::memory_order_acq_rel); }

void Task::quiesce_usercopy_pagemap() {
    usercopy_pagemap_closing.store(true, std::memory_order_release);
    while (usercopy_pagemap_accesses.load(std::memory_order_acquire) != 0) {
        asm volatile("pause" ::: "memory");
    }
}

auto Task::detach_pagemap_after_usercopy_quiescence() -> mm::paging::PageTable* {
    quiesce_usercopy_pagemap();
    auto* detached = pagemap;
    pagemap = nullptr;
    return detached;
}

auto Task::replace_pagemap_after_usercopy_quiescence(mm::paging::PageTable* replacement) -> mm::paging::PageTable* {
    quiesce_usercopy_pagemap();
    auto* replaced = pagemap;
    pagemap = replacement;
    usercopy_pagemap_closing.store(false, std::memory_order_release);
    return replaced;
}

void Task::signal_generate_pending_mask(uint64_t mask) {
    if (job_control::generate(*this, mask)) return;
    signal_generate_pending_mask_local(mask);
}

void Task::signal_generate_pending_mask_local(uint64_t mask, bool enqueue) {
    constexpr uint64_t CONT_MASK = uint64_t{1} << (18 - 1);
    constexpr uint64_t STOP_MASK =
        (uint64_t{1} << (19 - 1)) | (uint64_t{1} << (20 - 1)) | (uint64_t{1} << (21 - 1)) | (uint64_t{1} << (22 - 1));
    uint64_t const DISCARD = ((mask & CONT_MASK) != 0 ? STOP_MASK : 0) | ((mask & STOP_MASK) != 0 ? CONT_MASK : 0);
    if (DISCARD == 0) {
        if (enqueue) signal_add_pending_mask(mask);
        return;
    }
    uint64_t pending = sig_pending.load(std::memory_order_relaxed);
    while (!sig_pending.compare_exchange_weak(pending, (pending & ~DISCARD) | (enqueue ? mask : 0), std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
    }
}

void Task::signal_restore_job_control_pending(uint64_t bits) {
    constexpr uint64_t JOB_MASK = ((uint64_t{1} << 22) - 1) & ~((uint64_t{1} << 17) - 1);
    uint64_t pending = sig_pending.load(std::memory_order_relaxed);
    while (!sig_pending.compare_exchange_weak(pending, (pending & ~JOB_MASK) | (bits & JOB_MASK), std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
    }
}

Task::Task(const char* name, uint64_t elf_start, size_t elf_size, uint64_t kernel_rsp, TaskType type) {
    // CRITICAL: Copy the name string to kernel heap memory!
    // The passed 'name' might point to Limine boot memory or user memory
    // which won't be mapped when we switch pagemaps.
    if (name != nullptr) {
        size_t const NAME_LEN = std::strlen(name);
        char* name_copy = new char[NAME_LEN + 1];
        std::memcpy(name_copy, name, NAME_LEN + 1);
        this->name = name_copy;
    } else {
        this->name = nullptr;
    }
    this->parent_pid = 0;        // Initialize to 0 (no parent by default, will be set by exec or fork)
    this->elf_buffer = nullptr;  // No ELF buffer by default
    this->elf_buffer_size = 0;
    this->exec_image_file = nullptr;
    this->exec_image_size = 0;
    this->has_run = false;  // Task hasn't run yet, context.frame contains initial setup
    this->exit_status = 0;  // Initialize exit status
    this->exit_in_progress = false;
    this->has_exited = false;  // Task hasn't exited yet
    this->jobctl_stopped.store(false, std::memory_order_relaxed);
    this->jobctl_stop_pending.store(false, std::memory_order_relaxed);
    this->jobctl_stop_publish_deferred.store(false, std::memory_order_relaxed);
    this->jobctl_stop_signal = 0;
    this->exit_notify_ready.store(false, std::memory_order_relaxed);
    this->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    this->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    this->deferred_task_switch = false;  // No deferred switch by default
    this->yield_switch = false;
    this->kthread_entry = nullptr;
    this->preempt_disable_depth = 0;
    this->preempt_pending = false;
    this->preempt_disable_start_us = 0;
    this->preempt_disable_max_us = 0;
    this->preempt_disable_owner = 0;

    // Process time accounting
    this->start_time_us = 0;  // Will be set when task is first scheduled
    this->user_time_us = 0;
    this->system_time_us = 0;
    this->child_user_time_us = 0;
    this->child_system_time_us = 0;
    this->syscall_account_start_us = 0;
    this->precharged_syscall_time_us = 0;

    // EEVDF scheduling fields
    this->vruntime = 0;
    this->vdeadline = 0;
    this->sched_weight = 1024;  // nice-0 baseline
    this->sched_nice = 0;
    this->slice_ns = 10'000'000;  // 10ms
    this->slice_used_ns = 0;
    this->heap_index = -1;
    this->sched_queue = sched_queue::NONE;
    this->sched_next = nullptr;
    this->wake_at_us = 0;
    this->poll_wait_deadline_us = 0;
    this->wants_block = false;

    // Signal infrastructure
    this->signal_pending_store(0, std::memory_order_relaxed);
    this->signal_mask_store(0, std::memory_order_relaxed);
    this->sigsuspend_saved_mask = 0;
    this->sigsuspend_active = false;
    this->in_signal_handler = false;
    this->do_sigreturn = false;

    // fd_table is initialized by RadixTree default constructor (empty)

    if (type == TaskType::IDLE) {
        // Idle tasks use the kernel pagemap
        this->pagemap = mm::virt::get_kernel_pagemap();
        this->type = type;
        this->store_cpu(cpu::current_cpu());
        this->context.syscall_kernel_stack = kernel_rsp;

        // Initialize syscall scratch area even for idle tasks
        // This is needed because switchTo() sets GS_BASE from this field
        this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(new cpu::PerCpu());
        auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(this->context.syscall_scratch_area);
        scratch_area->syscall_stack = kernel_rsp;
        scratch_area->cpu_id = cpu::current_cpu();

        // Idle tasks get PID 0 (kernel/swapper convention) - they don't consume real PIDs
        // This ensures the first user process (init) always gets PID 1 regardless of core count
        this->pid = 0;
        this->entry = 0;
        this->kthread_entry = nullptr;
        this->thread = nullptr;
        return;
    }

    if (type == TaskType::DAEMON) {
        // Kernel thread: ring 0, kernel pagemap, no user thread/TLS, no ELF
        this->pagemap = mm::virt::get_kernel_pagemap();
        this->type = type;
        this->store_cpu(cpu::current_cpu());
        // Daemons are mostly background service work. Give them a much smaller
        // fair-share budget than user compute threads so periodic wakeups do not
        // dominate a CPU and stretch long-running PROCESS benchmarks by 2-4x.
        this->sched_weight = 128;
        this->sched_nice = 10;
        this->slice_ns = 2'000'000;
        this->context.syscall_kernel_stack = kernel_rsp;
        this->thread = nullptr;

        auto* per_cpu = new cpu::PerCpu();
        per_cpu->syscall_stack = kernel_rsp;
        per_cpu->cpu_id = cpu::current_cpu();
        this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

        this->pid = sched::task::get_next_pid();
        this->entry = 0;

        // Ring 0 interrupt frame for kernel-mode execution
        this->context.frame.rip = 0;        // Set by create_kernel_thread
        this->context.frame.cs = 0x08;      // GDT_KERN_CS
        this->context.frame.ss = 0x10;      // GDT_KERN_DS
        this->context.frame.flags = 0x202;  // IF=1, reserved bit 1
        this->context.frame.rsp = kernel_rsp;
        this->context.frame.int_num = 0;
        this->context.frame.err_code = 0;
        this->context.regs = cpu::GPRegs();
        return;
    }

    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::create_pagemap();
    if (this->pagemap == nullptr) {
        dbg::log("Failed to create pagemap for task %s", name);
        hcf();
    }
    this->context.frame.rsp = 0;
    this->context.regs = cpu::GPRegs();
    this->type = type;
    this->store_cpu(cpu::current_cpu());
    this->context.syscall_kernel_stack = kernel_rsp;

    this->pid = sched::task::get_next_pid();
    // Uninitialized job identity resolves SID/PGID to this new PID.

    // CRITICAL: Copy kernel mappings FIRST so we can access kernel memory (like elfBuffer)
    // The elfStart pointer points to kernel heap memory allocated by the parent process
    mm::virt::copy_kernel_mappings(this);

    // A null source constructs a recoverable process skeleton. Exec's
    // file-backed path claims unpublished ownership before it performs any
    // potentially blocking VFS reads in initialize_process_image().
    if (elf_start == 0) {
        return;
    }

    // Compatibility raw constructor is explicit trusted LOCAL. Production boot
    // uses a skeleton and passes one saved context through main + interpreter.
    mm::numa::AllocationContext allocation_context{};
    if (mm::numa::capture_allocation_context({}, allocation_context) != mm::numa::AllocationError::NONE) return;
    loader::elf::ElfLoadOptions const OPTIONS{.policy_owner = this,
                                              .allocation_context = allocation_context,
                                              .allocation_origin = loader::elf::ElfAllocationOrigin::TRUSTED_LOCAL};
    static_cast<void>(initialize_process_image(reinterpret_cast<const uint8_t*>(elf_start), elf_size, OPTIONS));
}

auto Task::initialize_unpublished_process_skeleton(Task& owner, const char* process_name
#ifdef WOS_SELFTEST
                                                   ,
                                                   [[maybe_unused]] ker::syscall::process::NativeConstructionToken fault_token
#endif
                                                   ) -> int {
    if (owner.owned_unpublished_process.load(std::memory_order_acquire) != this || ref_count.load(std::memory_order_acquire) != 2 ||
        scheduler_published.load(std::memory_order_acquire) || state.load(std::memory_order_acquire) != TaskState::ACTIVE || is_thread ||
        has_run || type != TaskType::DAEMON || name != nullptr || pagemap != nullptr || thread != nullptr || entry != 0 || pid != 0 ||
        context.syscall_kernel_stack < mm::KERNEL_STACK_SIZE || process_name == nullptr)
        return -EINVAL;
    // Establish teardown identity before allocations/reclaim can reschedule.
    type = TaskType::PROCESS;
    pid = get_next_pid();
    store_cpu(cpu::current_cpu());
    // Match ordinary process construction before any reclaiming allocation.
    // A default Task's zero index does not mean detached from a RunHeap.
    heap_index = -1;
    sched_weight = 1024;
    slice_ns = 10'000'000;
    size_t length = 0;
    while (length < EXE_PATH_MAX && process_name[length] != '\0') ++length;
    if (length == EXE_PATH_MAX) return -ENAMETOOLONG;
#ifdef WOS_SELFTEST
    if (ker::syscall::process::exec_selftest_take_native_construction_fault(ker::syscall::process::NativeConstructionFault::NAME,
                                                                            fault_token))
        return -ENOMEM;
#endif
    auto* copy = new (std::nothrow) char[length + 1];
    if (copy == nullptr) return -ENOMEM;
    name = copy;
    std::memcpy(copy, process_name, length + 1);
#ifdef WOS_SELFTEST
    if (ker::syscall::process::exec_selftest_take_native_construction_fault(ker::syscall::process::NativeConstructionFault::PAGEMAP,
                                                                            fault_token))
        return -ENOMEM;
#endif
    pagemap = mm::virt::create_pagemap_may_fail();
    if (pagemap == nullptr) return -ENOMEM;
    mm::virt::copy_kernel_mappings(this);
    return 0;
}

auto Task::initialize_process_image(const uint8_t* elf_data, size_t elf_size, const loader::elf::ElfLoadOptions& options,
                                    int* construction_error) -> bool {
    if (construction_error != nullptr) *construction_error = 0;
    if (type != TaskType::PROCESS || pagemap == nullptr || thread != nullptr || entry != 0 || elf_data == nullptr) {
        return false;
    }
    auto fail_process_construction = [&]() {
        // Leave every acquired resource attached to the now fully constructed
        // Task. The creator's unpublished-owner path can then tear it down
        // without guessing which constructor stage succeeded.
        this->entry = 0;
        this->context.frame.rip = 0;
    };

    // Add compiler memory barrier to ensure elfStart is fully visible
    __asm__ volatile("mfence" ::: "memory");

    ker::loader::elf::TlsModule actual_tls_info{};
    if (!loader::elf::inspect_tls(elf_data, elf_size, actual_tls_info)) {
        dbg::log("ERROR: Invalid bounded ELF source for task %s (size=%zu)", name, elf_size);
        fail_process_construction();
        return false;
    }

    if (int const ERROR = loader::elf::image_allocation_context_error(options); ERROR != 0) {
        if (construction_error != nullptr) *construction_error = ERROR;
        return false;
    }
    this->thread =
        threading::create_thread(ker::mod::mm::USER_STACK_SIZE, actual_tls_info.tls_size, this->pagemap, namespace_process_pid(*this),
                                 actual_tls_info, this, options.allocation_context, construction_error);
    if (this->thread == nullptr) {
        dbg::log("Failed to create thread for task %s - OOM", name);
        // Preserve the pagemap for unpublished-process cleanup.
        this->entry = 0;
        return false;
    }
    threading::record_stack_usage(*this);
    uint64_t mmap_cursor = 0;
    if (!mm::user_layout::choose_mmap_cursor(mmap_cursor)) {
        dbg::log("Failed to seed mmap placement for task %s", name);
        fail_process_construction();
        return false;
    }
    this->mmap_next.store(mmap_cursor, std::memory_order_relaxed);

    // Allocate a KERNEL-space PerCpu structure for syscall scratch area
    // This must be in kernel memory, not user memory! The user's gsbase/TLS is separate.
    // After swapgs in syscall entry: GS_BASE will point to this kernel scratch area.
    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = context.syscall_kernel_stack;
    per_cpu->cpu_id = cpu::current_cpu();
    this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

    this->context.frame.rsp = this->thread->stack;

    auto LOAD_OPTIONS = options;
    LOAD_OPTIONS.policy_owner = this;
    loader::elf::ElfLoadResult elf_result = loader::elf::load_elf(elf_data, elf_size, this->pagemap, this->pid, this->name, LOAD_OPTIONS);
    if (elf_result.entry_point == 0) {
        dbg::log("Failed to load ELF for task %s", name);
        if (construction_error != nullptr && elf_result.error != 0) *construction_error = elf_result.error;
        fail_process_construction();
        return false;
    }
    this->entry = elf_result.entry_point;
    this->context.frame.rip = elf_result.entry_point;
    this->program_header_addr = elf_result.program_header_addr;
    this->elf_header_addr = elf_result.elf_header_addr;
    this->image_load_base = elf_result.load_base;
    this->image_vaddr_start = elf_result.image_start;
    this->image_vaddr_end = elf_result.image_end;
    this->program_header_count = elf_result.program_header_count;
    this->program_header_ent_size = elf_result.program_header_ent_size;

    // Record PT_INTERP without entering the VFS from a constructor. Runtime
    // process creators complete this stage only after a persistent owner slot
    // can recover the fully constructed Task across a fatal signal handoff.
    if (elf_result.has_interp) {
        const char* const INTERP_PATH = std::begin(elf_result.interp_path);
        size_t const INTERP_LEN = std::strlen(INTERP_PATH);
        if (INTERP_LEN >= this->pending_interp_path.size()) {
            dbg::log("Interpreter path is too long for task %s", name);
            fail_process_construction();
            return false;
        }
        std::memcpy(this->pending_interp_path.data(), INTERP_PATH, INTERP_LEN + 1);
    }

    // Initialize interrupt frame fields for usermode
    this->context.frame.int_num = 0;    // Not from a real interrupt
    this->context.frame.err_code = 0;   // No error code
    this->context.frame.ss = 0x1b;      // User stack segment (GDT entry 3, RPL=3) NOLINT
    this->context.frame.cs = 0x23;      // User code segment (GDT entry 4, RPL=3) NOLINT
    this->context.frame.flags = 0x202;  // IF (interrupts enabled) + reserved bit 1 NOLINT
    sys::context_switch::record_saved_frame_class(this, this->context.frame, SavedFrameOrigin::SYNTHETIC_USER_RETURN);

    // Initialize important TLS symbols (e.g. SafeStack pointer) now that ELF is loaded and relocations processed
    ker::loader::debug::DebugSymbol const* ssym = ker::loader::debug::get_process_symbol(this->pid, "__safestack_unsafe_stack_ptr");
    if ((ssym != nullptr) && ssym->is_tls_offset) {
        uint64_t const DEST_VADDR = this->thread->tls_base_virt + ssym->raw_value;  // rawValue stores st_value (TLS offset)
        uint64_t const DEST_PADDR = mm::virt::translate(this->pagemap, DEST_VADDR);
        if (DEST_PADDR != mm::virt::PADDR_INVALID) {
            auto* dest_ptr = static_cast<uint64_t*>(mm::addr::get_virt_pointer(DEST_PADDR));
            *dest_ptr = this->thread->safestack_ptr_value;
            dbg::log("Wrote SafeStack ptr for PID %x at vaddr=%x (phys=%x) value=%x", this->pid, DEST_VADDR, DEST_PADDR,
                     this->thread->safestack_ptr_value);
        } else {
            dbg::log("Failed to translate SafeStack TLS vaddr %x for PID %x", DEST_VADDR, this->pid);
        }
    }
    (void)resource::observe_address_space(*this);
    return true;
}

auto Task::initialize_process_image(const ker::loader::elf::ElfFileView& elf, const ker::loader::elf::ElfLoadOptions& options,
                                    int* construction_error) -> bool {
    if (construction_error != nullptr) *construction_error = 0;
    if (type != TaskType::PROCESS || pagemap == nullptr || thread != nullptr || entry != 0) {
        return false;
    }

    ker::loader::elf::TlsModule TLS_INFO{};
    if (!loader::elf::inspect_tls(elf, TLS_INFO)) {
        dbg::log("Invalid file-backed ELF for task %s", name);
        return false;
    }
    if (int const ERROR = loader::elf::image_allocation_context_error(options); ERROR != 0) {
        if (construction_error != nullptr) *construction_error = ERROR;
        return false;
    }
    thread = threading::create_thread(ker::mod::mm::USER_STACK_SIZE, TLS_INFO.tls_size, pagemap, namespace_process_pid(*this), TLS_INFO,
                                      this, options.allocation_context, construction_error);
    if (thread == nullptr) {
        dbg::log("Failed to create thread for task %s - OOM", name);
        return false;
    }
    threading::record_stack_usage(*this);
    uint64_t mmap_cursor = 0;
    if (!mm::user_layout::choose_mmap_cursor(mmap_cursor)) {
        dbg::log("Failed to seed mmap placement for task %s", name);
        return false;
    }
    mmap_next.store(mmap_cursor, std::memory_order_relaxed);

    auto* per_cpu = new cpu::PerCpu();
    if (per_cpu == nullptr) {
        return false;
    }
    per_cpu->syscall_stack = context.syscall_kernel_stack;
    per_cpu->cpu_id = cpu::current_cpu();
    context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);
    context.frame.rsp = thread->stack;

    auto load_options = options;
    load_options.policy_owner = this;
    loader::elf::ElfLoadResult const ELF_RESULT = loader::elf::load_elf(elf, pagemap, pid, name, load_options);
    if (ELF_RESULT.entry_point == 0) {
        dbg::log("Failed to load file-backed ELF for task %s", name);
        if (construction_error != nullptr && ELF_RESULT.error != 0) *construction_error = ELF_RESULT.error;
        return false;
    }
    entry = ELF_RESULT.entry_point;
    context.frame.rip = ELF_RESULT.entry_point;
    program_header_addr = ELF_RESULT.program_header_addr;
    elf_header_addr = ELF_RESULT.elf_header_addr;
    image_load_base = ELF_RESULT.load_base;
    image_vaddr_start = ELF_RESULT.image_start;
    image_vaddr_end = ELF_RESULT.image_end;
    program_header_count = ELF_RESULT.program_header_count;
    program_header_ent_size = ELF_RESULT.program_header_ent_size;

    if (ELF_RESULT.has_interp) {
        const char* const INTERP_PATH = std::begin(ELF_RESULT.interp_path);
        size_t const INTERP_LEN = std::strlen(INTERP_PATH);
        if (INTERP_LEN >= pending_interp_path.size()) {
            dbg::log("Interpreter path is too long for task %s", name);
            entry = 0;
            context.frame.rip = 0;
            return false;
        }
        std::memcpy(pending_interp_path.data(), INTERP_PATH, INTERP_LEN + 1);
    }

    context.frame.int_num = 0;
    context.frame.err_code = 0;
    context.frame.ss = 0x1b;
    context.frame.cs = 0x23;
    context.frame.flags = 0x202;
    sys::context_switch::record_saved_frame_class(this, context.frame, SavedFrameOrigin::SYNTHETIC_USER_RETURN);

    ker::loader::debug::DebugSymbol const* ssym = ker::loader::debug::get_process_symbol(pid, "__safestack_unsafe_stack_ptr");
    if ((ssym != nullptr) && ssym->is_tls_offset) {
        uint64_t const DEST_VADDR = thread->tls_base_virt + ssym->raw_value;
        uint64_t const DEST_PADDR = mm::virt::translate(pagemap, DEST_VADDR);
        if (DEST_PADDR != mm::virt::PADDR_INVALID) {
            auto* dest_ptr = static_cast<uint64_t*>(mm::addr::get_virt_pointer(DEST_PADDR));
            *dest_ptr = thread->safestack_ptr_value;
        }
    }
    (void)resource::observe_address_space(*this);
    return true;
}

auto Task::memory_policy_snapshot() const noexcept -> mm::numa::Policy {
    uint64_t const FLAGS = m_memory_policy_lock.lock_irqsave();
    auto const VALUE = m_memory_policy;
    m_memory_policy_lock.unlock_irqrestore(FLAGS);
    return VALUE;
}

void Task::publish_memory_policy_snapshot(mm::numa::Policy value) noexcept {
    uint64_t const FLAGS = m_memory_policy_lock.lock_irqsave();
    m_memory_policy = value;
    m_memory_policy_lock.unlock_irqrestore(FLAGS);
}

#ifdef WOS_SELFTEST
auto Task::selftest_set_memory_policy(mm::numa::Policy candidate) noexcept -> bool {
    auto const* topology = acpi::numa::boot_topology();
    if (!mm::numa::valid_policy(candidate, topology != nullptr ? topology->node_count : 1) ||
        (topology == nullptr && candidate.mode != mm::numa::Mode::LOCAL))
        return false;
    publish_memory_policy_snapshot(candidate);
    return true;
}
#endif

Task* Task::create_user_thread(Task* parent, uint64_t tcb_vaddr, uint64_t user_sp, uint64_t enter_thread_va, uint64_t entry_va,
                               uint64_t user_arg_va, mm::numa::Policy memory_policy, bool* stack_exhausted, LazyVmemClone* prepared_lazy) {
    if (stack_exhausted != nullptr) {
        *stack_exhausted = false;
    }
    auto const KSTACK_BASE = reinterpret_cast<uint64_t>(mm::phys::kernel_stack_alloc("user_thread_kstack"));
    if (KSTACK_BASE == 0) {
        if (stack_exhausted != nullptr) {
            *stack_exhausted = true;
        }
        dbg::log("createUserThread: OOM allocating kernel stack");
        return nullptr;
    }
    uint64_t const K_RSP = KSTACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* t = new Task{};  // default-constructed; all fields set explicitly below
    t->publish_memory_policy_snapshot(memory_policy);

    // Copy name from parent
    if (parent->name != nullptr) {
        size_t const NAME_LEN = std::strlen(parent->name);
        char* name_copy = new char[NAME_LEN + 1];
        std::memcpy(name_copy, parent->name, NAME_LEN + 1);
        t->name = name_copy;
    } else {
        t->name = nullptr;
    }

    // Identity
    uint64_t const OWNER_PID = sched::task::process_pid(*parent);
    t->pid = sched::task::get_next_pid();
    t->parent_pid = (parent->parent_pid != 0U) ? parent->parent_pid : OWNER_PID;
    t->type = TaskType::PROCESS;
    t->is_thread = true;
    t->owner_pid = OWNER_PID;
    t->store_cpu(cpu::current_cpu());

    // Share the parent's pagemap - do NOT create a new one
    t->pagemap = parent->pagemap;
    t->entry = parent->entry;
    t->program_header_addr = parent->program_header_addr;
    t->elf_header_addr = parent->elf_header_addr;
    t->image_load_base = parent->image_load_base;
    t->image_vaddr_start = parent->image_vaddr_start;
    t->image_vaddr_end = parent->image_vaddr_end;
    t->program_header_count = parent->program_header_count;
    t->program_header_ent_size = parent->program_header_ent_size;
    t->interp_base = parent->interp_base;
    t->interp_vaddr_start = parent->interp_vaddr_start;
    t->interp_vaddr_end = parent->interp_vaddr_end;
    t->at_random_addr = parent->at_random_addr;
    t->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
    if (prepared_lazy != nullptr ? !prepared_lazy->matches(*parent) : !clone_lazy_vmem_ranges(*t, *parent)) {
        delete[] t->name;
        mm::phys::page_free(reinterpret_cast<void*>(KSTACK_BASE));
        delete t;
        return nullptr;
    }
    parent->shares_user_pagemap.store(true, std::memory_order_release);
    t->shares_user_pagemap.store(true, std::memory_order_release);

    // Thread struct: only fsbase and stack are meaningful; mlibc manages the TLS allocation
    auto* thr = new threading::Thread{};
    uint64_t stack_base = 0;
    uint64_t stack_size = 0;
    (void)infer_user_stack_range(prepared_lazy != nullptr ? parent : t, user_sp, stack_base, stack_size);
    thr->magic = 0xDEADBEEF;
    thr->fsbase = tcb_vaddr;
    thr->gsbase = 0;
    thr->stack = user_sp;
    thr->stack_size = stack_size;
    thr->stack_base_virt = stack_base;
    // The kernel-managed initial stack/TLS/SafeStack reservation belongs to
    // the shared address space, not only to its initial Thread object. Keep
    // that immutable protection metadata visible when this sibling issues VM
    // syscalls so it cannot replace a guard with MAP_FIXED.
    if (parent->thread != nullptr) {
        thr->layout_base_virt = parent->thread->layout_base_virt;
        thr->layout_size = parent->thread->layout_size;
    }
    thr->tls_size = 0;
    thr->tls_base_virt = 0;
    thr->tls_phys_ptr = 0;
    thr->stack_phys_ptr = 0;
    t->thread = thr;

    // Kernel-space PerCpu scratch area for syscall entry.
    // gsbase must also point here: after swapgs on syscall entry, GS_BASE becomes
    // gsbase, so it must be the PerCpu scratch area - not 0.
    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = K_RSP;
    per_cpu->cpu_id = cpu::current_cpu();
    t->context.syscall_kernel_stack = K_RSP;
    t->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);
    thr->gsbase = reinterpret_cast<uint64_t>(per_cpu);
    auto cleanup_constructed_thread_task = [&]() { destroy_unpublished_user_thread(t); };

    // User-mode interrupt frame: jump straight into __mlibc_enter_thread. The
    // syscall layer already snapshotted the prepared [entry, user_arg] words
    // before taking the shared-VM publication mutex.
    t->context.frame.rip = enter_thread_va;  // __mlibc_enter_thread
    t->context.frame.rsp = user_sp + 16;     // skip the two pushed words
    t->context.frame.cs = 0x23;              // user code segment
    t->context.frame.ss = 0x1b;              // user stack segment
    t->context.frame.flags = 0x202;          // IF=1, reserved=1
    sys::context_switch::record_saved_frame_class(t, t->context.frame, SavedFrameOrigin::SYNTHETIC_USER_RETURN);
    t->context.regs = cpu::GPRegs{};
    t->context.regs.rdi = entry_va;     // arg1: entry function
    t->context.regs.rsi = user_arg_va;  // arg2: user_arg

    // POSIX threads share their process descriptor table. Keep this per-thread
    // table empty; VFS fd helpers resolve user threads through owner_pid.
    // Copy fixed per-process storage inherited by userspace threads.
    t->fd_cloexec = parent->fd_cloexec;
    {
        auto* filesystem_parent = parent->filesystem_owner();
        uint64_t const PATH_IRQF = filesystem_parent->path_metadata_lock.lock_irqsave();
        t->cwd = filesystem_parent->cwd;
        t->cwd_len = filesystem_parent->cwd_len;
        t->root = filesystem_parent->root;
        t->root_len = filesystem_parent->root_len;
        filesystem_parent->path_metadata_lock.unlock_irqrestore(PATH_IRQF);
    }
    t->exe_path = parent->exe_path;
    t->personality = parent->personality;
    auto const PARENT_JOB = parent->job_identity_snapshot();
    t->initialize_job_identity(PARENT_JOB.internal, PARENT_JOB.visible);
    t->wki_target_hostname = parent->wki_target_hostname;
    t->wki_target_flags = parent->wki_target_flags;
    auto const PARENT_WKI = parent->wki_execution_identity();
    t->wki_submitter_hostname = PARENT_WKI.submitter_hostname;
    t->wki_remote_pid = PARENT_WKI.remote_pid;
    if (!t->wki_vfs_rules.clone_from(parent->wki_vfs_rules)) {
        cleanup_constructed_thread_task();
        return nullptr;
    }
    t->wki_skip_legacy_placement = false;

    // ELF buffer: threads have no separate ELF
    t->elf_buffer = nullptr;
    t->elf_buffer_size = 0;
    t->exec_image_file = nullptr;
    t->exec_image_size = 0;

    // Scheduling defaults
    t->has_run = false;
    t->exit_in_progress = false;
    t->has_exited = false;
    t->exit_notify_ready.store(false, std::memory_order_relaxed);
    t->exit_status = 0;
    t->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    t->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    t->deferred_task_switch = false;
    t->yield_switch = false;
    t->set_voluntary_blocked(false);
    t->jobctl_stopped.store(false, std::memory_order_relaxed);
    t->jobctl_stop_pending.store(false, std::memory_order_relaxed);
    t->jobctl_stop_publish_deferred.store(false, std::memory_order_relaxed);
    t->jobctl_stop_signal = 0;
    t->vruntime = 0;
    t->vdeadline = 0;
    t->sched_weight = parent->sched_weight;
    t->sched_nice = parent->sched_nice;
    t->slice_ns = 10'000'000;
    t->slice_used_ns = 0;
    t->heap_index = -1;
    t->sched_queue = sched_queue::NONE;
    t->sched_next = nullptr;

    // Signals: inherit the private mask; dispositions resolve the retained process owner.
    t->signal_pending_store(0, std::memory_order_relaxed);
    t->signal_mask_store(parent->signal_mask_bits(), std::memory_order_relaxed);
    t->sigsuspend_saved_mask = 0;
    t->sigsuspend_active = false;
    t->in_signal_handler = false;
    t->do_sigreturn = false;

    // Time accounting
    t->start_time_us = 0;
    t->user_time_us = 0;
    t->system_time_us = 0;
    t->child_user_time_us = 0;
    t->child_system_time_us = 0;
    t->syscall_account_start_us = 0;
    t->precharged_syscall_time_us = 0;

    if (!resource::bind_thread_owner(*t, *parent)) {
        destroy_unpublished_user_thread(t);
        return nullptr;
    }
    if (prepared_lazy != nullptr && !prepared_lazy->consume(*t)) {
        destroy_unpublished_user_thread(t);
        return nullptr;
    }
    (void)resource::observe_address_space(*t);
    return t;
}

Task* Task::create_kernel_thread(const char* name, void (*entry_func)()) {
    if (entry_func == nullptr) {
        dbg::log("create_kernel_thread: null entry for '%s'", name != nullptr ? name : "?");
        return nullptr;
    }

    auto stack_base = reinterpret_cast<uint64_t>(mm::phys::kernel_stack_alloc("kernel_thread_kstack"));
    if (stack_base == 0) {
        dbg::log("create_kernel_thread: OOM allocating kernel stack for '%s'", name);
        return nullptr;
    }
    uint64_t const KERNEL_RSP = stack_base + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* task = new (std::nothrow) Task(name, 0, 0, KERNEL_RSP, TaskType::DAEMON);
    if (task == nullptr) {
        mm::phys::page_free(reinterpret_cast<void*>(stack_base));
        return nullptr;
    }
    task->kthread_entry = entry_func;
    task->context.frame.rip = reinterpret_cast<uint64_t>(wos_kernel_thread_trampoline);
    task->context.regs.rdi = reinterpret_cast<uint64_t>(entry_func);
    sys::context_switch::record_saved_frame_class(task, task->context.frame, SavedFrameOrigin::DAEMON_START);
    return task;
}

void Task::load_context(cpu::GPRegs* gpr) { this->context.regs = *gpr; }

void Task::save_context(cpu::GPRegs* gpr) const {
    cpu_set_msr(IA32_KERNEL_GS_BASE, this->context.syscall_scratch_area);
    *gpr = context.regs;
}

auto get_next_pid() -> uint64_t {
    static std::atomic<uint64_t> next_pid{1};  // Start from 1 to avoid confusion with kernel tasks
    uint64_t pid = 0;
    while (pid == 0) {
        pid = next_pid.fetch_add(1, std::memory_order_relaxed);
    }
    return pid;
}

}  // namespace ker::mod::sched::task
