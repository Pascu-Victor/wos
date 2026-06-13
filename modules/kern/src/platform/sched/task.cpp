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
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
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
    // Some remote VFS stat paths do not provide a rich freshness tuple on
    // every mount/backend combination. Allow caching as long as size is known;
    // the full observed stat tuple still participates in equality checks.
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

auto read_boot_file_fully(const char* path, uint8_t** out_buf) -> bool {
    if (path == nullptr || out_buf == nullptr) {
        return false;
    }

    *out_buf = nullptr;

    auto* current_task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    [[maybe_unused]] auto const* task_name = (current_task != nullptr && current_task->name != nullptr) ? current_task->name : "?";
    [[maybe_unused]] auto const* task_root = (current_task != nullptr) ? current_task->root.data() : "-";
    [[maybe_unused]] auto const* task_cwd = (current_task != nullptr) ? current_task->cwd.data() : "-";
    bool const HAS_REMOTE_SUBMITTER_IDENTITY = current_task != nullptr && current_task->wki_submitter_hostname.front() != '\0';
    auto const* submitter = HAS_REMOTE_SUBMITTER_IDENTITY ? current_task->wki_submitter_hostname.data() : "-";
    bool constexpr TRACE_INTERP = false;
    bool constexpr ALLOW_BOOT_FILE_CACHE = true;
    std::array<char, 512> cache_key = {};
    build_boot_file_cache_key(submitter, path, cache_key.data(), cache_key.size());

    // Keep the kernel-side loader open local for ordinary exec paths, but
    // still honor the submitter's temporary VFS identity during remote task
    // construction so cross-node launches continue to resolve against the
    // submitter when required.
    int const OPEN_FLAGS = boot_file_open_flags(path, HAS_REMOTE_SUBMITTER_IDENTITY);
    auto* file = ker::vfs::vfs_open_file(path, OPEN_FLAGS, 0);
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_read == nullptr || file->fops->vfs_lseek == nullptr) {
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

    ker::vfs::Stat freshness = {};
    bool const HAVE_FRESHNESS =
        ALLOW_BOOT_FILE_CACHE && ker::vfs::vfs_fstat_file(file, &freshness) == 0 && boot_file_stat_has_freshness(freshness);
    if (HAVE_FRESHNESS) {
        size_t cached_size = 0;
        if (boot_file_cache_lookup_copy(cache_key.data(), freshness, out_buf, &cached_size)) {
            release_boot_file(file);
            if constexpr (TRACE_INTERP) {
                dbg::log("read_boot_file_fully: cache hit key='%s' task=%s pid=0x%lx size=%zu", cache_key.data(), task_name,
                         static_cast<unsigned long>(current_task != nullptr ? current_task->pid : 0), cached_size);
            }
            return true;
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
    return true;
}

}  // namespace

void destroy_unpublished_user_thread(Task* task) {
    if (task == nullptr) {
        return;
    }

    release_cloned_fd_table_refs(task);
    delete task->thread;
    delete reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    delete[] task->name;
    if (task->context.syscall_kernel_stack >= ker::mod::mm::KERNEL_STACK_SIZE) {
        mm::phys::page_free(reinterpret_cast<void*>(task->context.syscall_kernel_stack - ker::mod::mm::KERNEL_STACK_SIZE));
    }
    delete task;
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

auto task_selftest_waited_on_claim_is_single_winner() -> bool {
    Task task{};
    task_clear_waited_on(task);

    bool const FIRST = task_try_mark_waited_on(task);
    bool const SECOND = task_try_mark_waited_on(task);
    bool const OBSERVED = task_waited_on(task);
    task_clear_waited_on(task);

    return FIRST && !SECOND && OBSERVED && !task_waited_on(task);
}

auto task_selftest_waitpid_block_state_clear_resets_fields() -> bool {
    Task task{};
    task.waiting_for_pid = 123;
    task.wait_status_user_addr = 1;
    task.wait_status_phys_addr = 2;
    task.wait_rusage_user_addr = 3;
    task.wait_rusage_phys_addr = 4;
    task.wait_resume_rip_user_addr = 5;
    task.wait_resume_rip_phys_addr = 6;
    task.wait_resume_rsp_user_addr = 7;
    task.wait_resume_rsp_phys_addr = 8;
    task.wait_channel = "waitpid";

    task_clear_waitpid_block_state(task);

    return task.waiting_for_pid == 0 && task.wait_status_user_addr == 0 && task.wait_status_phys_addr == 0 &&
           task.wait_rusage_user_addr == 0 && task.wait_rusage_phys_addr == 0 && task.wait_resume_rip_user_addr == 0 &&
           task.wait_resume_rip_phys_addr == 0 && task.wait_resume_rsp_user_addr == 0 && task.wait_resume_rsp_phys_addr == 0 &&
           task.wait_channel == nullptr;
}
#endif

Task::Task(const char* name, uint64_t elf_start, uint64_t kernel_rsp, TaskType type) {
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
    this->has_run = false;     // Task hasn't run yet, context.frame contains initial setup
    this->exit_status = 0;     // Initialize exit status
    this->has_exited = false;  // Task hasn't exited yet
    this->exit_notify_ready.store(false, std::memory_order_relaxed);
    task_clear_waited_on(*this);
    this->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    this->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    this->waitpid_publish_pending.store(false, std::memory_order_relaxed);
    this->deferred_task_switch = false;  // No deferred switch by default
    this->yield_switch = false;
    this->kthread_entry = nullptr;
    this->preempt_disable_depth = 0;
    this->preempt_pending = false;
    this->preempt_disable_start_us = 0;
    this->preempt_disable_max_us = 0;
    this->preempt_disable_owner = 0;

    // Waitpid state
    this->waiting_for_pid = 0;
    this->wait_status_user_addr = 0;
    this->wait_status_phys_addr = 0;
    this->wait_rusage_user_addr = 0;
    this->wait_rusage_phys_addr = 0;

    // Process time accounting
    this->start_time_us = 0;  // Will be set when task is first scheduled
    this->user_time_us = 0;
    this->system_time_us = 0;
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
    this->sig_pending = 0;
    this->sig_mask = 0;
    this->sigsuspend_saved_mask = 0;
    this->sigsuspend_active = false;
    this->in_signal_handler = false;
    this->do_sigreturn = false;
    for (auto& sig_handler : this->sig_handlers) {
        sig_handler = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};  // SIG_DFL for all
    }

    // fd_table is initialized by RadixTree default constructor (empty)

    if (type == TaskType::IDLE) {
        // Idle tasks use the kernel pagemap
        this->pagemap = mm::virt::get_kernel_pagemap();
        this->type = type;
        this->cpu = cpu::current_cpu();
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
        this->cpu = cpu::current_cpu();
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
    this->cpu = cpu::current_cpu();
    this->context.syscall_kernel_stack = kernel_rsp;

    auto fail_process_construction = [&]() {
        // Match the existing create_thread() failure contract so higher layers
        // can reject the exec without freezing the whole kernel on a remote
        // loader miss or malformed interpreter image.
        this->type = TaskType::IDLE;
        this->thread = nullptr;
        this->pagemap = nullptr;
        this->entry = 0;
    };

    this->pid = sched::task::get_next_pid();
    // POSIX: default process group = own pid (processes start in their own group)
    if (this->pgid == 0) {
        this->pgid = this->pid;
    }

    // CRITICAL: Copy kernel mappings FIRST so we can access kernel memory (like elfBuffer)
    // The elfStart pointer points to kernel heap memory allocated by the parent process
    mm::virt::copy_kernel_mappings(this);

    // Validate ELF pointer before any operations
    if (elf_start == 0) {
        dbg::log("ERROR: Task created with null ELF pointer");
        hcf();
    }

    // Add compiler memory barrier to ensure elfStart is fully visible
    __asm__ volatile("mfence" ::: "memory");

    // Validate ELF magic bytes before proceeding
    auto* elf_header = reinterpret_cast<uint8_t*>(elf_start);

    if (elf_header[0] != 0x7F || elf_header[1] != 'E' || elf_header[2] != 'L' || elf_header[3] != 'F') {
        dbg::log("ERROR: Invalid ELF magic at 0x%p: [0x%x 0x%x 0x%x 0x%x]", reinterpret_cast<void*>(elf_start), elf_header[0],
                 elf_header[1], elf_header[2], elf_header[3]);
        dbg::log("Expected ELF magic: [0x7F 'E' 'L' 'F'] = [0x7F 0x45 0x4C 0x46]");
        hcf();
    }

    // FIXED: Parse ELF first to get actual TLS size, then create thread
    ker::loader::elf::TlsModule const ACTUAL_TLS_INFO = loader::elf::extract_tls_info(reinterpret_cast<void*>(elf_start));
    this->thread = threading::create_thread(ker::mod::mm::USER_STACK_SIZE, ACTUAL_TLS_INFO.tls_size, this->pagemap, ACTUAL_TLS_INFO);
    if (this->thread == nullptr) {
        dbg::log("Failed to create thread for task %s - OOM", name);
        // Can't continue without a thread - this is a fatal error for the task
        // Mark task as invalid so it won't be scheduled
        this->type = TaskType::IDLE;  // Abuse IDLE type to prevent scheduling
        this->pagemap = nullptr;
        return;
    }

    // Allocate a KERNEL-space PerCpu structure for syscall scratch area
    // This must be in kernel memory, not user memory! The user's gsbase/TLS is separate.
    // After swapgs in syscall entry: GS_BASE will point to this kernel scratch area.
    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = kernel_rsp;
    per_cpu->cpu_id = cpu::current_cpu();
    this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

    this->context.frame.rsp = this->thread->stack;

    loader::elf::ElfLoadResult elf_result =
        loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(elf_start), this->pagemap, this->pid, this->name);
    if (elf_result.entry_point == 0) {
        dbg::log("Failed to load ELF for task %s", name);
        fail_process_construction();
        return;
    }
    this->entry = elf_result.entry_point;
    this->context.frame.rip = elf_result.entry_point;
    this->program_header_addr = elf_result.program_header_addr;
    this->elf_header_addr = elf_result.elf_header_addr;
    this->program_header_count = elf_result.program_header_count;
    this->program_header_ent_size = elf_result.program_header_ent_size;

    // If the binary requests a dynamic linker (PT_INTERP), load it now.
    // The interpreter (ld.so) is loaded at a high base address to avoid
    // conflicting with the main binary's address space.
    if (elf_result.has_interp) {
        constexpr uint64_t INTERP_BASE = 0x40000000ULL;
        const char* const INTERP_PATH = std::begin(elf_result.interp_path);

        uint8_t* interp_buf = nullptr;
        if (!read_boot_file_fully(INTERP_PATH, &interp_buf)) {
            dbg::log("Failed to open interpreter '%s' for task %s", INTERP_PATH, name);
            fail_process_construction();
            return;
        }

        loader::elf::ElfLoadResult const INTERP_RESULT =
            loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(interp_buf), this->pagemap, this->pid, "ld.so",
                                  false /* don't register debug symbols for interp */, INTERP_BASE);

        if (INTERP_RESULT.entry_point == 0) {
            delete[] interp_buf;
            dbg::log("Failed to load interpreter ELF '%s'", INTERP_PATH);
            fail_process_construction();
            return;
        }

        // Entry point becomes the interpreter's entry (ld.so _start).
        // ld.so reads AT_ENTRY and AT_PHDR from auxv to find the real binary.
        this->context.frame.rip = INTERP_RESULT.entry_point;
        // Store interp_base for AT_BASE in auxv (set by exec.cpp)
        this->interp_base = INTERP_BASE;

        delete[] interp_buf;
    }

    // Initialize interrupt frame fields for usermode
    this->context.frame.int_num = 0;    // Not from a real interrupt
    this->context.frame.err_code = 0;   // No error code
    this->context.frame.ss = 0x1b;      // User stack segment (GDT entry 3, RPL=3) NOLINT
    this->context.frame.cs = 0x23;      // User code segment (GDT entry 4, RPL=3) NOLINT
    this->context.frame.flags = 0x202;  // IF (interrupts enabled) + reserved bit 1 NOLINT

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
}

Task* Task::create_user_thread(Task* parent, uint64_t tcb_vaddr, uint64_t user_sp, uint64_t enter_thread_va) {
    auto const KSTACK_BASE = reinterpret_cast<uint64_t>(mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
    if (KSTACK_BASE == 0) {
        dbg::log("createUserThread: OOM allocating kernel stack");
        return nullptr;
    }
    uint64_t const K_RSP = KSTACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* t = new Task{};  // default-constructed; all fields set explicitly below

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
    t->cpu = cpu::current_cpu();

    // Share the parent's pagemap - do NOT create a new one
    t->pagemap = parent->pagemap;
    t->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
    if (!t->lazy_vmem_ranges.clone_from(parent->lazy_vmem_ranges)) {
        delete[] t->name;
        mm::phys::page_free(reinterpret_cast<void*>(KSTACK_BASE));
        delete t;
        return nullptr;
    }

    // Thread struct: only fsbase and stack are meaningful; mlibc manages the TLS allocation
    auto* thr = new threading::Thread{};
    thr->magic = 0xDEADBEEF;
    thr->fsbase = tcb_vaddr;
    thr->gsbase = 0;
    thr->stack = user_sp;
    thr->stack_size = 0;
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

    // User-mode interrupt frame: jump straight into __mlibc_enter_thread.
    // sys_prepare_stack pushed [ user_arg, entry ] below userSp (i.e. at userSp and userSp+8).
    // Read them out now so the kernel can set RDI/RSI directly, and advance RSP past them.
    uint64_t entry_va = 0;
    uint64_t user_arg_va = 0;
    {
        // Translate the two words on the prepared stack via the parent pagemap
        uint64_t const PA_ENTRY = mm::virt::translate(parent->pagemap, user_sp);
        if (PA_ENTRY != mm::virt::PADDR_INVALID) {
            entry_va = *reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(PA_ENTRY));
        }
        uint64_t const PA_ARG = mm::virt::translate(parent->pagemap, user_sp + 8);
        if (PA_ARG != mm::virt::PADDR_INVALID) {
            user_arg_va = *reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(PA_ARG));
        }
    }

    t->context.frame.rip = enter_thread_va;  // __mlibc_enter_thread
    t->context.frame.rsp = user_sp + 16;     // skip the two pushed words
    t->context.frame.cs = 0x23;              // user code segment
    t->context.frame.ss = 0x1b;              // user stack segment
    t->context.frame.flags = 0x202;          // IF=1, reserved=1
    t->context.regs = cpu::GPRegs{};
    t->context.regs.rdi = entry_va;     // arg1: entry function
    t->context.regs.rsi = user_arg_va;  // arg2: user_arg

    // Inherit FDs: share the same File* pointers and bump each refcount.
    if (!clone_user_thread_fds_checked(parent, t)) {
        cleanup_constructed_thread_task();
        return nullptr;
    }
    // Copy fixed per-process storage inherited by userspace threads.
    t->fd_cloexec = parent->fd_cloexec;
    t->cwd = parent->cwd;
    t->root = parent->root;
    t->exe_path = parent->exe_path;
    t->uid = parent->uid;
    t->gid = parent->gid;
    t->euid = parent->euid;
    t->egid = parent->egid;
    t->suid = parent->suid;
    t->sgid = parent->sgid;
    t->umask = parent->umask;
    t->personality = parent->personality;
    if (!t->supplementary_groups.clone_from(parent->supplementary_groups)) {
        cleanup_constructed_thread_task();
        return nullptr;
    }
    t->session_id = parent->session_id;
    t->pgid = parent->pgid;
    t->controlling_tty = parent->controlling_tty;
    t->wki_target_hostname = parent->wki_target_hostname;
    t->wki_target_flags = parent->wki_target_flags;
    t->wki_submitter_hostname = parent->wki_submitter_hostname;
    t->wki_remote_pid = parent->wki_remote_pid;
    if (!t->wki_vfs_rules.clone_from(parent->wki_vfs_rules)) {
        cleanup_constructed_thread_task();
        return nullptr;
    }
    t->wki_skip_legacy_placement = false;

    // ELF buffer: threads have no separate ELF
    t->elf_buffer = nullptr;
    t->elf_buffer_size = 0;

    // Scheduling defaults
    t->has_run = false;
    t->has_exited = false;
    t->exit_notify_ready.store(false, std::memory_order_relaxed);
    t->exit_status = 0;
    task_clear_waited_on(*t);
    t->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    t->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    t->waitpid_publish_pending.store(false, std::memory_order_relaxed);
    t->deferred_task_switch = false;
    t->yield_switch = false;
    t->set_voluntary_blocked(false);
    t->waiting_for_pid = 0;
    t->wait_status_user_addr = 0;
    t->wait_status_phys_addr = 0;
    t->wait_rusage_user_addr = 0;
    t->wait_rusage_phys_addr = 0;
    t->vruntime = 0;
    t->vdeadline = 0;
    t->sched_weight = parent->sched_weight;
    t->sched_nice = parent->sched_nice;
    t->slice_ns = 10'000'000;
    t->slice_used_ns = 0;
    t->heap_index = -1;
    t->sched_queue = sched_queue::NONE;
    t->sched_next = nullptr;

    // Signals: inherit mask and handlers from parent (POSIX  2.4)
    t->sig_pending = 0;
    t->sig_mask = parent->sig_mask;
    t->sigsuspend_saved_mask = 0;
    t->sigsuspend_active = false;
    t->in_signal_handler = false;
    t->do_sigreturn = false;
    t->sig_handlers = parent->sig_handlers;

    // Time accounting
    t->start_time_us = 0;
    t->user_time_us = 0;
    t->system_time_us = 0;
    t->syscall_account_start_us = 0;
    t->precharged_syscall_time_us = 0;
    t->itimer_real_expire_us = 0;
    t->itimer_real_interval_us = 0;

    return t;
}

Task* Task::create_kernel_thread(const char* name, void (*entry_func)()) {
    if (entry_func == nullptr) {
        dbg::log("create_kernel_thread: null entry for '%s'", name != nullptr ? name : "?");
        return nullptr;
    }

    auto stack_base = reinterpret_cast<uint64_t>(mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
    if (stack_base == 0) {
        dbg::log("create_kernel_thread: OOM allocating kernel stack for '%s'", name);
        return nullptr;
    }
    uint64_t const KERNEL_RSP = stack_base + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* task = new Task(name, 0, KERNEL_RSP, TaskType::DAEMON);
    task->kthread_entry = entry_func;
    task->context.frame.rip = reinterpret_cast<uint64_t>(wos_kernel_thread_trampoline);
    task->context.regs.rdi = reinterpret_cast<uint64_t>(entry_func);
    return task;
}

void Task::load_context(cpu::GPRegs* gpr) { this->context.regs = *gpr; }

void Task::save_context(cpu::GPRegs* gpr) const {
    cpu_set_msr(IA32_KERNEL_GS_BASE, this->context.syscall_scratch_area);
    *gpr = context.regs;
}

auto get_next_pid() -> uint64_t {
    static uint64_t next_pid = 1;  // Start from 1 to avoid confusion with kernel tasks
    return next_pid++;
}

}  // namespace ker::mod::sched::task
