#include "procfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/virtio/virtio_net.hpp>
#include <minimalist_malloc/mini_malloc.hpp>
#include <net/backlog.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/tcp.hpp>
#include <new>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/memacc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "net/wki/remote_compute.hpp"
#include "net/wki/remote_ipc.hpp"
#include "net/wki/remote_vfs.hpp"
#include "net/wki/wire.hpp"
#include "net/wki/wki.hpp"
#include "platform/mm/paging.hpp"
#include "platform/sched/scheduler.hpp"
#include "release.hpp"
#include "vfs/file_operations.hpp"

namespace ker::vfs::procfs {

namespace {

constexpr uint64_t NS_PER_SEC = 1000000000ULL;
constexpr uint64_t NS_PER_US = 1000ULL;
constexpr uint64_t PROC_CLK_TCK = 100ULL;
constexpr uint64_t US_PER_PROC_TICK = 1000000ULL / PROC_CLK_TCK;
constexpr uint64_t KIB = 1024ULL;

uint64_t g_procfs_creation_epoch_ns = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Helper: int to decimal string
void int_to_str(uint64_t val, char* buf, size_t bufsz) {
    if (bufsz == 0) {
        return;
    }
    std::array<char, 24> tmp{};
    int pos = 0;
    if (val == 0) {
        tmp[pos++] = '0';
    } else {
        while (val > 0 && pos < 22) {
            tmp[pos++] = static_cast<char>('0' + static_cast<unsigned char>(val % 10));
            val /= 10;
        }
    }
    // reverse
    int const LEN = pos;
    for (int i = 0; i < LEN && static_cast<size_t>(i) < bufsz - 1; ++i) {
        buf[i] = tmp[LEN - 1 - i];
    }
    buf[std::cmp_less(LEN, bufsz) ? LEN : static_cast<int>(bufsz - 1)] = '\0';
}

using ProcTask = ker::mod::sched::task::Task;

auto proc_task_state_char(const ProcTask& task) -> char {
    auto const TASK_STATE = task.state.load(std::memory_order_acquire);
    if (TASK_STATE == ker::mod::sched::task::TaskState::DEAD || TASK_STATE == ker::mod::sched::task::TaskState::EXITING ||
        task.has_exited) {
        return 'Z';
    }
    if (task.sched_queue == ProcTask::sched_queue::RUNNABLE) {
        return 'R';
    }
    if (task.sched_queue == ProcTask::sched_queue::WAITING) {
        return task.wait_channel != nullptr ? 'D' : 'S';
    }
    return 'S';
}

auto proc_task_state_label(char state) -> const char* {
    switch (state) {
        case 'Z':
            return "Z (zombie)";
        case 'R':
            return "R (running)";
        case 'D':
            return "D (blocked)";
        default:
            return "S (sleeping)";
    }
}

struct ProcTaskStats {
    uint64_t user_time_us{};
    uint64_t system_time_us{};
    uint64_t thread_count{1};
    char state{'S'};
};

auto collect_proc_task_stats(const ProcTask& task, bool thread_view) -> ProcTaskStats {
    ProcTaskStats stats{
        .user_time_us = task.user_time_us,
        .system_time_us = task.system_time_us,
        .thread_count = 1,
        .state = proc_task_state_char(task),
    };
    if (thread_view || task.is_thread || task.type != ker::mod::sched::task::TaskType::PROCESS) {
        return stats;
    }

    uint64_t const GROUP_PID = ker::mod::sched::task::process_pid(task);
    uint64_t thread_count = 0;
    uint64_t user_time_us = 0;
    uint64_t system_time_us = 0;
    bool any_running = false;
    bool any_blocked = false;
    bool any_sleeping = false;
    bool any_zombie = false;

    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::same_thread_group(*candidate, GROUP_PID)) {
            candidate->release();
            continue;
        }

        thread_count++;
        user_time_us += candidate->user_time_us;
        system_time_us += candidate->system_time_us;
        char const STATE = proc_task_state_char(*candidate);
        any_running = any_running || STATE == 'R';
        any_blocked = any_blocked || STATE == 'D';
        any_sleeping = any_sleeping || STATE == 'S';
        any_zombie = any_zombie || STATE == 'Z';
        candidate->release();
    }

    if (thread_count == 0) {
        return stats;
    }

    stats.user_time_us = user_time_us;
    stats.system_time_us = system_time_us;
    stats.thread_count = thread_count;
    if (any_running) {
        stats.state = 'R';
    } else if (any_blocked) {
        stats.state = 'D';
    } else if (any_sleeping) {
        stats.state = 'S';
    } else if (any_zombie) {
        stats.state = 'Z';
    }
    return stats;
}

auto process_visible_task_at(size_t index) -> ProcTask* {
    size_t seen = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::process_visible(*task)) {
            task->release();
            continue;
        }
        if (seen == index) {
            return task;
        }
        seen++;
        task->release();
    }
    return nullptr;
}

auto thread_group_task_at(uint64_t group_pid, size_t index) -> ProcTask* {
    size_t seen = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::same_thread_group(*task, group_pid)) {
            task->release();
            continue;
        }
        if (seen == index) {
            return task;
        }
        seen++;
        task->release();
    }
    return nullptr;
}

auto task_group_pid_for(uint64_t pid, uint64_t& group_pid) -> bool {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return false;
    }
    group_pid = ker::mod::sched::task::process_pid(*task);
    task->release();
    return true;
}

auto find_task_in_group_safe(uint64_t group_pid, uint64_t tid) -> ProcTask* {
    auto* task = ker::mod::sched::find_task_by_pid_safe(tid);
    if (task == nullptr) {
        return nullptr;
    }
    if (!ker::mod::sched::task::same_thread_group(*task, group_pid)) {
        task->release();
        return nullptr;
    }
    return task;
}

void fill_numeric_dirent(DirEntry* buf, size_t count, uint64_t pid) {
    buf->d_ino = pid + 100;
    buf->d_off = count + 1;
    buf->d_reclen = sizeof(DirEntry);
    buf->d_type = DT_DIR;
    int_to_str(pid, buf->d_name.data(), buf->d_name.size());
}

auto procfs_readdir(File* f, DirEntry* buf, size_t count) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);

    // Synthetic . and ..
    if (count == 0) {
        buf->d_ino = 1;
        buf->d_off = 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        buf->d_name[0] = '.';
        buf->d_name[1] = '\0';
        return 0;
    }
    if (count == 1) {
        buf->d_ino = 1;
        buf->d_off = 2;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        buf->d_name[0] = '.';
        buf->d_name[1] = '.';
        buf->d_name[2] = '\0';
        return 0;
    }

    if (pfd->node.type == ProcNodeType::ROOT_DIR) {
        // /proc root: fixed entries then PID directories
        // index 2 = "self", 3 = "mounts", 4 = "uptime", 5 = "version",
        // 6 = "stat", 7 = "loadavg", 8 = "meminfo", 9 = "wki", 10 = "memacc", 11+ = active task PIDs
        if (count == 2) {
            buf->d_ino = 2;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "self", 5);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 3;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "mounts", 7);
            return 0;
        }
        if (count == 4) {
            buf->d_ino = 4;
            buf->d_off = 5;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "uptime", 7);
            return 0;
        }
        if (count == 5) {
            buf->d_ino = 5;
            buf->d_off = 6;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "version", 8);
            return 0;
        }
        if (count == 6) {
            buf->d_ino = 6;
            buf->d_off = 7;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "stat", 5);
            return 0;
        }
        if (count == 7) {
            buf->d_ino = 7;
            buf->d_off = 8;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "loadavg", 8);
            return 0;
        }
        if (count == 8) {
            buf->d_ino = 8;
            buf->d_off = 9;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "meminfo", 8);
            return 0;
        }
        if (count == 9) {
            buf->d_ino = 9;
            buf->d_off = 10;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "wki", 4);
            return 0;
        }
        if (count == 10) {
            buf->d_ino = 22;
            buf->d_off = 11;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "memacc", 7);
            return 0;
        }
        // PID directories from active task list. Threads and WKI proxy tasks
        // stay hidden here; real threads are exposed under /proc/<pid>/task/<tid>.
        size_t const PID_INDEX = count - 11;
        auto* task = process_visible_task_at(PID_INDEX);
        if (task == nullptr) {
            return -ENOENT;
        }
        uint64_t const PID = task->pid;
        task->release();
        fill_numeric_dirent(buf, count, PID);
        return 0;
    }

    if (pfd->node.type == ProcNodeType::PID_DIR || pfd->node.type == ProcNodeType::TASK_TID_DIR) {
        // /proc/<pid> and /proc/<pid>/task/<tid>: index 2 = "stat", 3 = "status", 4 = "statm",
        // 5 = "cmdline", 6 = "exe", 7 = "wki_launcher", 8 = "wki_runner", 9 = "wki_remote_pid".
        // Top-level process dirs additionally expose "task" at index 10.
        if (count == 2) {
            buf->d_ino = 10;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "stat", 5);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 11;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "status", 7);
            return 0;
        }
        if (count == 4) {
            buf->d_ino = 12;
            buf->d_off = 5;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "statm", 6);
            return 0;
        }
        if (count == 5) {
            buf->d_ino = 13;
            buf->d_off = 6;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "cmdline", 8);
            return 0;
        }
        if (count == 6) {
            buf->d_ino = 14;
            buf->d_off = 7;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "exe", 4);
            return 0;
        }
        if (count == 7) {
            buf->d_ino = 15;
            buf->d_off = 8;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_launcher", 13);
            return 0;
        }
        if (count == 8) {
            buf->d_ino = 16;
            buf->d_off = 9;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_runner", 11);
            return 0;
        }
        if (count == 9) {
            buf->d_ino = 17;
            buf->d_off = 10;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_remote_pid", 15);
            return 0;
        }
        if (pfd->node.type == ProcNodeType::PID_DIR && count == 10) {
            buf->d_ino = 18;
            buf->d_off = 11;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "task", 5);
            return 0;
        }
        return -ENOENT;  // No more entries
    }

    if (pfd->node.type == ProcNodeType::TASK_DIR) {
        auto* task = thread_group_task_at(pfd->node.pid, count - 2);
        if (task == nullptr) {
            return -ENOENT;
        }
        uint64_t const TID = task->pid;
        task->release();
        fill_numeric_dirent(buf, count, TID);
        return 0;
    }

    if (pfd->node.type == ProcNodeType::WKI_DIR) {
        if (count == 2) {
            buf->d_ino = 20;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "peers", 6);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 21;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "netdiag", 8);
            return 0;
        }
        return -ENOENT;
    }

    if (pfd->node.type == ProcNodeType::MEMACC_DIR) {
        struct Entry {
            const char* name;
            uint8_t type;
        };
        constexpr std::array<Entry, 12> ENTRIES{{
            {.name = "summary", .type = DT_REG},
            {.name = "zones", .type = DT_REG},
            {.name = "procs", .type = DT_REG},
            {.name = "dead", .type = DT_REG},
            {.name = "alloc_totals", .type = DT_REG},
            {.name = "slabs", .type = DT_REG},
            {.name = "kmalloc_live", .type = DT_REG},
            {.name = "kmalloc_callers", .type = DT_REG},
            {.name = "page_callers", .type = DT_REG},
            {.name = "features", .type = DT_REG},
            {.name = "track", .type = DT_DIR},
            {.name = "reclaim", .type = DT_DIR},
        }};
        size_t const INDEX = count - 2;
        if (INDEX >= ENTRIES.size()) {
            return -ENOENT;
        }
        buf->d_ino = 30 + INDEX;
        buf->d_off = count + 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = ENTRIES.at(INDEX).type;
        std::strncpy(buf->d_name.data(), ENTRIES.at(INDEX).name, buf->d_name.size() - 1);
        buf->d_name[buf->d_name.size() - 1] = '\0';
        return 0;
    }

    if (pfd->node.type == ProcNodeType::MEMACC_TRACK_DIR) {
        if (count == 2) {
            buf->d_ino = 40;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "page_callers", 13);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 41;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "kmalloc_debug", 14);
            return 0;
        }
        return -ENOENT;
    }

    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_DIR) {
        if (count == 2) {
            buf->d_ino = 42;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "buffer_cache", 13);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 43;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "packet_pool", 12);
            return 0;
        }
        return -ENOENT;
    }

    return -ENOENT;
}

// Helper to parse a PID from a path component
auto parse_pid(const char* s) -> int64_t {
    if (s == nullptr || *s == '\0') {
        return -EINVAL;
    }
    int64_t val = 0;
    for (const char* p = s; *p != 0; ++p) {
        if (*p < '0' || *p > '9') {
            return -EINVAL;
        }
        val = (val * 10) + (*p - '0');
    }
    return val;
}

// Generate content for /proc/<pid>/status
auto generate_status(uint64_t pid, char* buf, size_t bufsz, bool thread_view) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return 0;
    }
    auto const MEM = ker::mod::mm::virt::collect_user_memory_stats(task->pagemap);
    auto const STATS = collect_proc_task_stats(*task, thread_view);
    uint64_t const PROCESS_PID = ker::mod::sched::task::process_pid(*task);

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    append("Name:\t");
    append((task->exe_path[0] != 0) ? task->exe_path.data() : "(unknown)");
    append("\nTgid:\t");
    append_int(PROCESS_PID);
    append("\nPid:\t");
    append_int(task->pid);
    append("\nPPid:\t");
    append_int(task->parent_pid);
    append("\nUid:\t");
    append_int(task->uid);
    append("\t");
    append_int(task->euid);
    append("\t");
    append_int(task->suid);
    append("\t");
    append_int(task->uid);
    append("\nGid:\t");
    append_int(task->gid);
    append("\t");
    append_int(task->egid);
    append("\t");
    append_int(task->sgid);
    append("\t");
    append_int(task->gid);
    append("\nThreads:\t");
    append_int(STATS.thread_count);
    append("\nVmSize:\t");
    append_int(MEM.virtual_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nVmRSS:\t");
    append_int(MEM.resident_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nRssShmem:\t");
    append_int(MEM.shared_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nVmPTE:\t");
    append_int(MEM.page_table_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");

    // State
    append("\nState:\t");
    append(proc_task_state_label(STATS.state));

    // Scheduling info
    append("\nCpu:\t");
    append_int(task->cpu);
    append("\nSchedQueue:\t");
    switch (task->sched_queue) {
        case ker::mod::sched::task::Task::sched_queue::NONE:
            append("NONE");
            break;
        case ker::mod::sched::task::Task::sched_queue::RUNNABLE:
            append("RUNNABLE");
            break;
        case ker::mod::sched::task::Task::sched_queue::WAITING:
            append("WAITING");
            break;
        case ker::mod::sched::task::Task::sched_queue::DEAD_GC:
            append("DEAD_GC");
            break;
    }

    // Wait channel
    append("\nWchan:\t");
    if (task->wait_channel != nullptr) {
        append(task->wait_channel);
    } else {
        append("-");
    }

    // Blocking state flags
    append("\nDeferredSwitch:\t");
    append(task->deferred_task_switch ? "1" : "0");
    append("\nVoluntaryBlock:\t");
    append(task->is_voluntary_blocked() ? "1" : "0");
    append("\nWaitingForPid:\t");
    append_int(task->waiting_for_pid);

    // Signals
    append("\nSigPnd:\t");
    append_int(task->sig_pending);
    append("\nSigBlk:\t");
    append_int(task->sig_mask);
    append("\nInSigHandler:\t");
    append(task->in_signal_handler ? "1" : "0");

    // Terminal
    append("\nTTY:\t");
    if (task->controlling_tty >= 0) {
        append_int(static_cast<uint64_t>(task->controlling_tty));
    } else {
        append("-1");
    }
    append("\nPGID:\t");
    append_int(task->pgid != 0 ? task->pgid : task->pid);
    append("\nSID:\t");
    append_int(task->session_id != 0 ? task->session_id : task->pid);

    // Time accounting
    append("\nUserTime:\t");
    append_int(STATS.user_time_us);
    append(" us");
    append("\nSysTime:\t");
    append_int(STATS.system_time_us);
    append(" us");
    append("\nStartTime:\t");
    append_int(task->start_time_us);
    append(" us");

    append("\n");

    buf[off] = '\0';
    task->release();
    return off;
}

// Generate content for /proc/<pid>/stat (Linux-compatible format)
// Format: pid (comm) state ppid pgid sid tty tpgid flags minflt cminflt majflt cmajflt
//         utime stime cutime cstime priority nice nthreads itrealvalue starttime vsize rss ...
auto generate_stat(uint64_t pid, char* buf, size_t bufsz, bool thread_view) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return 0;
    }
    auto const MEM = ker::mod::mm::virt::collect_user_memory_stats(task->pagemap);
    auto const STATS = collect_proc_task_stats(*task, thread_view);

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    // Extract comm (basename of exe_path)
    const auto* comm = task->exe_path.data();
    if (comm[0] != '\0') {
        const char* p = comm;
        while (*p != 0) {
            if (*p == '/') {
                comm = p + 1;
            }
            p++;
        }
    }
    if (comm[0] == '\0') {
        comm = (task->name != nullptr) ? task->name : "unknown";
    }

    // pid (comm) state ppid pgid sid tty_nr tpgid flags
    append_int(task->pid);
    append(" (");
    append(comm);
    append(") ");
    buf[off++] = STATS.state;
    append(" ");
    append_int(task->parent_pid);  // ppid
    append(" ");
    append_int(task->pgid != 0 ? task->pgid : task->pid);  // pgrp
    append(" ");
    append_int(task->session_id != 0 ? task->session_id : task->pid);  // session
    append(" ");
    append_int(task->controlling_tty >= 0 ? static_cast<uint64_t>(task->controlling_tty) : 0);  // tty_nr
    append(" ");
    append_int(0);  // tpgid
    append(" ");
    append_int(0);  // flags
    append(" ");

    // minflt cminflt majflt cmajflt
    append("0 ");  // minflt
    append("0 ");  // cminflt
    append("0 ");  // majflt
    append("0 ");  // cmajflt

    // utime stime cutime cstime (in clock ticks, CLK_TCK=100, so 1 tick=10000us)
    append_int(STATS.user_time_us / 10000);  // utime
    append(" ");
    append_int(STATS.system_time_us / 10000);  // stime
    append(" ");
    append("0 ");  // cutime (children)
    append("0 ");  // cstime (children)

    // priority nice num_threads itrealvalue starttime
    append_int(static_cast<uint64_t>(20 + task->sched_nice));  // priority
    append(" ");
    if (task->sched_nice < 0) {
        append("-");
        append_int(static_cast<uint64_t>(-task->sched_nice));
    } else {
        append_int(static_cast<uint64_t>(task->sched_nice));
    }
    append(" ");
    append_int(STATS.thread_count);  // num_threads
    append(" ");
    append("0 ");                             // itrealvalue
    append_int(task->start_time_us / 10000);  // starttime (in ticks since boot)
    append(" ");

    // vsize rss
    append_int(MEM.virtual_pages * ker::mod::mm::paging::PAGE_SIZE);  // vsize
    append(" ");
    append_int(MEM.resident_pages);  // rss
    append(" ");

    // rlim signal blocked sigignore sigcatch wchan nswap cnswap exit_signal processor
    append("0 ");                   // rlim
    append_int(task->sig_pending);  // signal (pending)
    append(" ");
    append_int(task->sig_mask);  // blocked
    append(" ");
    append("0 ");  // sigignore
    append("0 ");  // sigcatch
    // wchan: kernel wait channel name (0 if not blocked)
    if (task->wait_channel != nullptr) {
        append(task->wait_channel);
    } else {
        append("0");
    }
    append(" ");
    append("0 ");           // nswap
    append("0 ");           // cnswap
    append("0 ");           // exit_signal
    append_int(task->cpu);  // processor

    append("\n");
    buf[off] = '\0';
    task->release();
    return off;
}

// Generate content for /proc/<pid>/statm.
// Format: size resident shared text lib data dt (all in pages).
auto generate_statm(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return 0;
    }
    auto const MEM = ker::mod::mm::virt::collect_user_memory_stats(task->pagemap);
    task->release();

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    append_int(MEM.virtual_pages);
    append(" ");
    append_int(MEM.resident_pages);
    append(" ");
    append_int(MEM.shared_pages);
    append(" 0 0 0 0\n");
    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/cmdline (NUL-separated argv)
auto generate_cmdline(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return 0;
    }

    // Use exe_path as fallback (most processes won't have a saved argv)
    const auto* path = task->exe_path.data();
    if (path[0] == '\0') {
        path = (task->name != nullptr) ? task->name : "";
    }
    size_t len = strlen(path);
    if (len >= bufsz) {
        len = bufsz - 1;
    }
    memcpy(buf, path, len);
    buf[len] = '\0';  // NUL terminator (cmdline ends with NUL)
    task->release();
    return len + 1;  // Include the trailing NUL as part of content length
}

// Generate content for /proc/mounts
auto generate_mounts(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };

    // Strip the task root prefix so mount points appear task-root-relative,
    // matching what POSIX processes expect (e.g. "/dev" not "/rootfs/dev").
    const char* task_root = "/";
    size_t root_len = 1;
    if (ker::mod::sched::can_query_current_task()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr && task->root[0] == '/' && task->root[1] != '\0') {
            task_root = task->root.data();
            root_len = std::strlen(task_root);
        }
    }

    // Iterate mount table
    for (size_t i = 0; i < ker::vfs::get_mount_count(); ++i) {
        auto* m = ker::vfs::get_mount_at(i);
        if (m == nullptr) {
            continue;
        }
        const char* mount_path = (m->path != nullptr) ? m->path : "/";

        // Compute the task-relative display path by stripping the task root prefix.
        const char* display_path = mount_path;
        size_t const MOUNT_PATH_LEN = std::strlen(mount_path);
        if (root_len > 1 && MOUNT_PATH_LEN >= root_len && std::strncmp(mount_path, task_root, root_len) == 0 &&
            (mount_path[root_len] == '/' || mount_path[root_len] == '\0')) {
            display_path = mount_path + root_len;
            if (display_path[0] == '\0') {
                display_path = "/";
            }
        }

        append((m->fstype != nullptr) ? m->fstype : "none");
        append(" ");
        append(display_path);
        append(" ");
        append((m->fstype != nullptr) ? m->fstype : "none");
        append(" rw 0 0\n");
    }

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/uptime
// Format: "<uptime_seconds> <idle_seconds>\n" with two decimal places
auto generate_uptime(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    uint64_t const UPTIME_US = ker::mod::time::get_us();
    uint64_t const UPTIME_SEC = UPTIME_US / 1000000ULL;
    uint64_t const UPTIME_FRAC = (UPTIME_US % 1000000ULL) / 10000ULL;  // two decimal places

    append_int(UPTIME_SEC);
    append(".");
    // Zero-pad the fractional part to two digits
    if (UPTIME_FRAC < 10) {
        append("0");
    }
    append_int(UPTIME_FRAC);

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }
    uint64_t idle_us = 0;
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        idle_us += ker::mod::sched::get_cpu_accounting_snapshot(cpu).idle_us;
    }
    uint64_t const IDLE_AVG_US = idle_us / cpu_count;
    uint64_t const IDLE_SEC = IDLE_AVG_US / 1000000ULL;
    uint64_t const IDLE_FRAC = (IDLE_AVG_US % 1000000ULL) / 10000ULL;

    append(" ");
    append_int(IDLE_SEC);
    append(".");
    if (IDLE_FRAC < 10) {
        append("0");
    }
    append_int(IDLE_FRAC);
    append("\n");

    buf[off] = '\0';
    return off;
}

auto generate_cpu_stat(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };
    auto append_ticks = [&](uint64_t us) { append_int(us / US_PER_PROC_TICK); };

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }

    ker::mod::sched::CpuAccountingSnapshot total{};
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        auto const SNAP = ker::mod::sched::get_cpu_accounting_snapshot(cpu);
        total.user_us += SNAP.user_us;
        total.nice_us += SNAP.nice_us;
        total.system_us += SNAP.system_us;
        total.idle_us += SNAP.idle_us;
        total.iowait_us += SNAP.iowait_us;
        total.irq_us += SNAP.irq_us;
        total.softirq_us += SNAP.softirq_us;
        total.steal_us += SNAP.steal_us;
    }

    auto append_cpu_row = [&](const char* prefix, uint64_t cpu_no, const ker::mod::sched::CpuAccountingSnapshot& snap, bool numbered) {
        append(prefix);
        if (numbered) {
            append_int(cpu_no);
        }
        append("  ");
        append_ticks(snap.user_us);
        append(" ");
        append_ticks(snap.nice_us);
        append(" ");
        append_ticks(snap.system_us);
        append(" ");
        append_ticks(snap.idle_us);
        append(" ");
        append_ticks(snap.iowait_us);
        append(" ");
        append_ticks(snap.irq_us);
        append(" ");
        append_ticks(snap.softirq_us);
        append(" ");
        append_ticks(snap.steal_us);
        append(" 0 0\n");
    };

    append_cpu_row("cpu", 0, total, false);
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        append_cpu_row("cpu", cpu, ker::mod::sched::get_cpu_accounting_snapshot(cpu), true);
    }

    auto const LOAD = ker::mod::sched::get_load_average_snapshot();
    append("procs_running ");
    append_int(LOAD.runnable_tasks);
    append("\nprocs_blocked ");
    append_int(LOAD.uninterruptible_tasks);
    append("\n");

    buf[off] = '\0';
    return off;
}

auto generate_loadavg(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };
    auto append_load = [&](uint64_t milli) {
        append_int(milli / 1000ULL);
        append(".");
        uint64_t const FRAC = (milli % 1000ULL) / 10ULL;
        if (FRAC < 10) {
            append("0");
        }
        append_int(FRAC);
    };

    auto const LOAD = ker::mod::sched::get_load_average_snapshot();
    append_load(LOAD.load1_milli);
    append(" ");
    append_load(LOAD.load5_milli);
    append(" ");
    append_load(LOAD.load15_milli);
    append(" ");
    append_int(LOAD.runnable_tasks);
    append("/");
    append_int(LOAD.total_tasks);
    append(" ");
    append_int(LOAD.last_pid);
    append("\n");

    buf[off] = '\0';
    return off;
}

auto generate_meminfo(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };
    auto append_kb_line = [&](const char* name, uint64_t bytes) {
        append(name);
        append(":\t");
        append_int(bytes / KIB);
        append(" kB\n");
    };

    uint64_t const TOTAL = ker::mod::mm::phys::get_total_mem_bytes();
    uint64_t const FREE = ker::mod::mm::phys::get_free_mem_bytes();
    auto const BCACHE = ker::vfs::buffer_cache_stats();
    auto const FILE_CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    auto const ELF_CACHE = ker::net::wki::wki_shared_elf_cache_stats();
    uint64_t const BUFFERS = BCACHE.total_bytes;
    uint64_t const CACHED = FILE_CACHE.bytes + ELF_CACHE.bytes;
    uint64_t const AVAILABLE = std::min<uint64_t>(TOTAL, FREE + BCACHE.clean_bytes + CACHED);

    uint64_t page_table_bytes = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!task->is_thread) {
            auto const MEM = ker::mod::mm::virt::collect_user_memory_stats(task->pagemap);
            page_table_bytes += MEM.page_table_pages * ker::mod::mm::paging::PAGE_SIZE;
        }
        task->release();
    }

    append_kb_line("MemTotal", TOTAL);
    append_kb_line("MemFree", FREE);
    append_kb_line("MemAvailable", AVAILABLE);
    append_kb_line("Buffers", BUFFERS);
    append_kb_line("Cached", CACHED);
    append_kb_line("SwapCached", 0);
    append_kb_line("Dirty", BCACHE.dirty_bytes);
    append_kb_line("Writeback", 0);
    append_kb_line("PageTables", page_table_bytes);
    append_kb_line("SwapTotal", 0);
    append_kb_line("SwapFree", 0);

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/wki_launcher
// Returns the hostname of the node that submitted/launched this process,
// or the local hostname if the process was launched locally.
auto generate_wki_launcher(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    const char* hostname = nullptr;
    if (task != nullptr && task->wki_submitter_hostname[0] != '\0') {
        hostname = task->wki_submitter_hostname.data();
    } else {
        hostname = ker::net::wki::g_wki.local_hostname.data();
    }
    if (hostname == nullptr || hostname[0] == '\0') {
        hostname = "local";
    }
    size_t len = strlen(hostname);
    if (len + 2 > bufsz) {
        len = bufsz - 2;
    }
    memcpy(buf, hostname, len);
    buf[len] = '\n';
    buf[len + 1] = '\0';
    return len + 1;
}

// Generate content for /proc/<pid>/wki_runner
// Returns the hostname of the node currently executing this process.
auto generate_wki_runner(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    const char* hostname = ker::net::wki::g_wki.local_hostname.data();
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> proxy_hostname{};
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task != nullptr && ker::net::wki::wki_proxy_task_remote_info(task, nullptr, proxy_hostname.data(), proxy_hostname.size()) &&
        proxy_hostname.front() != '\0') {
        hostname = proxy_hostname.data();
    }
    if (task != nullptr) {
        task->release();
    }
    if (hostname == nullptr || hostname[0] == '\0') {
        hostname = "local";
    }
    size_t len = strlen(hostname);
    if (len + 2 > bufsz) {
        len = bufsz - 2;
    }
    memcpy(buf, hostname, len);
    buf[len] = '\n';
    buf[len + 1] = '\0';
    return len + 1;
}

auto task_wki_remote_pid(const ker::mod::sched::task::Task* task) -> uint64_t {
    if (task == nullptr) {
        return 0;
    }
    if (task->wki_remote_pid != 0) {
        return task->wki_remote_pid;
    }
    const char* local_hostname = ker::net::wki::g_wki.local_hostname.data();
    if (task->wki_submitter_hostname[0] != '\0' && local_hostname != nullptr && local_hostname[0] != '\0' &&
        std::strcmp(task->wki_submitter_hostname.data(), local_hostname) != 0) {
        return task->pid;
    }
    return 0;
}

auto generate_wki_remote_pid(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    int const LEN = std::snprintf(buf, bufsz, "%llu\n", static_cast<unsigned long long>(task_wki_remote_pid(task)));
    if (LEN < 0) {
        if (bufsz != 0) {
            buf[0] = '\0';
        }
        return 0;
    }
    if (std::cmp_greater_equal(LEN, bufsz)) {
        return bufsz != 0 ? bufsz - 1 : 0;
    }
    return static_cast<size_t>(LEN);
}

auto generate_wki_peers(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s != '\0' && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_row = [&](const std::array<char, net::wki::WKI_HOSTNAME_MAX>& hostname, uint64_t node_id, bool connected, uint64_t cpus,
                          uint64_t load_pct, uint64_t last_update_us, bool local) {
        if (off >= bufsz - 1) {
            return;
        }
        int const LEN =
            std::snprintf(buf + off, bufsz - off, "%s %llu %u %llu %llu %llu %u\n", hostname[0] != '\0' ? hostname.data() : "unknown",
                          static_cast<unsigned long long>(node_id), connected ? 1U : 0U, static_cast<unsigned long long>(cpus),
                          static_cast<unsigned long long>(load_pct), static_cast<unsigned long long>(last_update_us), local ? 1U : 0U);
        if (LEN <= 0) {
            return;
        }
        off += std::min(static_cast<size_t>(LEN), bufsz - off - 1);
    };

    append("hostname node_id connected cpus load_pct last_update_us local\n");

    uint64_t const CPU_COUNT = std::max<uint64_t>(ker::mod::smt::get_core_count(), 1);
    uint64_t local_runnable = 0;
    for (uint64_t cpu = 0; cpu < CPU_COUNT; ++cpu) {
        auto stats = ker::mod::sched::get_run_queue_stats(cpu);
        local_runnable += stats.active_task_count;
    }
    uint64_t const LOCAL_LOAD = std::min<uint64_t>((local_runnable * 1000ULL) / CPU_COUNT, 1000ULL);
    uint16_t const LOCAL_NODE = ker::net::wki::g_wki.my_node_id != ker::net::wki::WKI_NODE_INVALID ? ker::net::wki::g_wki.my_node_id : 0;
    append_row(ker::net::wki::g_wki.local_hostname, LOCAL_NODE, true, CPU_COUNT, LOCAL_LOAD, ker::net::wki::wki_now_us(), true);

    uint64_t const FLAGS = ker::net::wki::g_wki.peer_lock.lock_irqsave();
    for (const auto& peer : ker::net::wki::g_wki.peers) {
        if (peer.node_id == ker::net::wki::WKI_NODE_INVALID) {
            continue;
        }

        auto const* load = ker::net::wki::wki_remote_node_load(peer.node_id);
        uint64_t const CPUS = load != nullptr && load->valid && load->num_cpus != 0 ? load->num_cpus : 0;
        uint64_t const LOAD_PCT = load != nullptr && load->valid ? load->avg_load_pct : 0;
        uint64_t const LAST_UPDATE =
            load != nullptr && load->valid ? load->last_update_us : std::max(peer.last_heartbeat, peer.last_rx_activity);
        append_row(peer.hostname, peer.node_id, peer.state == ker::net::wki::PeerState::CONNECTED, CPUS, LOAD_PCT, LAST_UPDATE, false);
    }
    ker::net::wki::g_wki.peer_lock.unlock_irqrestore(FLAGS);

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/version
auto generate_version(char* buf, size_t bufsz) -> size_t;

// Generate a one-line status string for /proc/kperfctl
auto generate_kperfctl(char* buf, size_t bufsz) -> size_t {
    const char* state = ker::mod::perf::is_enabled() ? "enabled\n" : "disabled\n";
    size_t len = strlen(state);
    if (len >= bufsz) {
        len = bufsz - 1;
    }
    memcpy(buf, state, len);
    buf[len] = '\0';
    return len;
}

// Forward declarations for helpers defined below
static void append_dec64(char*& p, const char* end, uint64_t v);
static void append_sconst(char*& p, const char* end, const char* s);
static void append_char(char*& p, const char* end, char c);

// Generate content for /proc/kcontstat
// Shows per-subsystem container aggregate statistics (non-destructive).
auto generate_kcontstat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    for (size_t i = 1; i < ker::mod::perf::PERF_SUBSYSTEM_COUNT; ++i) {
        auto s = ker::mod::perf::get_subsystem_stats(static_cast<ker::mod::perf::PerfSubsystem>(i));
        uint64_t const INS = s.inserts;
        uint64_t const REM = s.removes;
        uint64_t const RES = s.resizes;
        uint64_t const OOM = s.oom_failures;
        uint64_t const PEAK = s.peak_count;
        uint64_t const CUR = s.current_count;

        // Skip subsystems with no activity
        if (INS == 0 && REM == 0 && RES == 0 && OOM == 0) {
            continue;
        }

        append_sconst(p, end, "subsys=");
        append_sconst(p, end, ker::mod::perf::subsystem_name(static_cast<ker::mod::perf::PerfSubsystem>(i)));
        append_sconst(p, end, " inserts=");
        append_dec64(p, end, INS);
        append_sconst(p, end, " removes=");
        append_dec64(p, end, REM);
        append_sconst(p, end, " resizes=");
        append_dec64(p, end, RES);
        append_sconst(p, end, " oom=");
        append_dec64(p, end, OOM);
        append_sconst(p, end, " peak=");
        append_dec64(p, end, PEAK);
        append_sconst(p, end, " current=");
        append_dec64(p, end, CUR);
        if (p + 1 < end) {
            *p++ = '\n';
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_kwkistat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    std::array<ker::mod::perf::WkiPerfSummarySnapshot, ker::mod::perf::WKI_PERF_SUMMARY_BUCKETS> rows{};
    size_t const ROW_COUNT = ker::mod::perf::get_wki_summary_snapshots(rows.data(), rows.size());

    for (size_t i = 0; i < ROW_COUNT; ++i) {
        const auto& row = rows[i];
        if (row.calls == 0) {
            continue;
        }

        uint64_t const AVG = row.calls != 0 ? (row.total_latency_us / row.calls) : 0;
        append_sconst(p, end, "scope=");
        append_sconst(p, end, ker::mod::perf::wki_scope_name(static_cast<ker::mod::perf::WkiPerfScope>(row.scope)));
        append_sconst(p, end, " op=");
        append_sconst(p, end, ker::mod::perf::wki_op_name(static_cast<ker::mod::perf::WkiPerfScope>(row.scope), row.op));
        append_sconst(p, end, " peer=");
        append_dec64(p, end, row.peer);
        append_sconst(p, end, " channel=");
        append_dec64(p, end, row.channel);
        append_sconst(p, end, " calls=");
        append_dec64(p, end, row.calls);
        append_sconst(p, end, " errors=");
        append_dec64(p, end, row.errors);
        append_sconst(p, end, " retries=");
        append_dec64(p, end, row.retries);
        append_sconst(p, end, " bytes=");
        append_dec64(p, end, row.bytes);
        append_sconst(p, end, " samples=");
        append_dec64(p, end, row.latency_samples);
        append_sconst(p, end, " total_us=");
        append_dec64(p, end, row.total_latency_us);
        append_sconst(p, end, " avg_us=");
        append_dec64(p, end, AVG);
        append_sconst(p, end, " max_us=");
        append_dec64(p, end, row.max_latency_us);
        append_sconst(p, end, " p50_us=");
        append_dec64(p, end, row.p50_us);
        append_sconst(p, end, " p95_us=");
        append_dec64(p, end, row.p95_us);
        append_sconst(p, end, " p99_us=");
        append_dec64(p, end, row.p99_us);
        append_sconst(p, end, " p999_us=");
        append_dec64(p, end, row.p999_us);
        append_sconst(p, end, " p9999_us=");
        append_dec64(p, end, row.p9999_us);
        append_sconst(p, end, " p99999_us=");
        append_dec64(p, end, row.p99999_us);
        if (p + 1 < end) {
            *p++ = '\n';
        }
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_kipcstat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    ker::net::wki::WkiIpcPerfSnapshot snapshot{};
    ker::net::wki::wki_ipc_get_perf_snapshot(snapshot);
    ker::vfs::LocalPipePerfSnapshot local_pipe{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(local_pipe);

    append_sconst(p, end, "exports=");
    append_dec64(p, end, snapshot.exports);
    append_sconst(p, end, " proxies=");
    append_dec64(p, end, snapshot.proxies);
    append_sconst(p, end, " pump_tasks=");
    append_dec64(p, end, snapshot.pump_tasks);
    append_sconst(p, end, " ring_bytes=");
    append_dec64(p, end, snapshot.proxy_ring_bytes);
    append_sconst(p, end, " ring_used=");
    append_dec64(p, end, snapshot.proxy_ring_used_bytes);
    append_sconst(p, end, " blocked_readers=");
    append_dec64(p, end, snapshot.blocked_readers);
    append_sconst(p, end, " poll_waiters=");
    append_dec64(p, end, snapshot.poll_waiters);
    append_sconst(p, end, " pending_deliveries=");
    append_dec64(p, end, snapshot.pending_deliveries);
    append_sconst(p, end, " pending_chunks=");
    append_dec64(p, end, snapshot.pending_chunks);
    append_sconst(p, end, " pending_bytes=");
    append_dec64(p, end, snapshot.pending_bytes);
    append_sconst(p, end, " export_backlogs=");
    append_dec64(p, end, snapshot.export_backlogs);
    append_sconst(p, end, " export_backlog_chunks=");
    append_dec64(p, end, snapshot.export_backlog_chunks);
    append_sconst(p, end, " export_backlog_bytes=");
    append_dec64(p, end, snapshot.export_backlog_bytes);
    append_sconst(p, end, " export_flush_queue=");
    append_dec64(p, end, snapshot.export_flush_queue);
    append_sconst(p, end, " dev_op_queue=");
    append_dec64(p, end, snapshot.dev_op_queue);
    append_sconst(p, end, " dev_op_payload_bytes=");
    append_dec64(p, end, snapshot.dev_op_payload_bytes);
    append_sconst(p, end, " approx_alloc_bytes=");
    append_dec64(p, end, snapshot.approx_alloc_bytes);
    append_sconst(p, end, " local_pipe_active=");
    append_dec64(p, end, local_pipe.active_pipes);
    append_sconst(p, end, " local_pipe_created=");
    append_dec64(p, end, local_pipe.created_since_reset);
    append_sconst(p, end, " local_pipe_peak=");
    append_dec64(p, end, local_pipe.peak_pipes);
    append_sconst(p, end, " local_pipe_capacity=");
    append_dec64(p, end, local_pipe.capacity_bytes);
    append_sconst(p, end, " local_pipe_peak_capacity=");
    append_dec64(p, end, local_pipe.peak_capacity_bytes);
    append_sconst(p, end, " local_pipe_buffered=");
    append_dec64(p, end, local_pipe.buffered_bytes);
    append_sconst(p, end, " local_pipe_reader_waiters=");
    append_dec64(p, end, local_pipe.reader_waiters);
    append_sconst(p, end, " local_pipe_writer_waiters=");
    append_dec64(p, end, local_pipe.writer_waiters);
    append_sconst(p, end, " local_pipe_poll_waiters=");
    append_dec64(p, end, local_pipe.poll_waiters);
    append_sconst(p, end, " local_pipe_direct_writes=");
    append_dec64(p, end, local_pipe.direct_writes);
    append_sconst(p, end, " local_pipe_read_closed=");
    append_dec64(p, end, local_pipe.read_closed);
    append_sconst(p, end, " local_pipe_write_closed=");
    append_dec64(p, end, local_pipe.write_closed);
    append_sconst(p, end, " local_pipe_approx_alloc_bytes=");
    append_dec64(p, end, local_pipe.approx_alloc_bytes);
    if (p + 1 < end) {
        *p++ = '\n';
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Hex helper
void append_hex64(char*& p, const char* end, uint64_t v) {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    if (p + 18 >= end) {
        return;
    }
    *p++ = '0';
    *p++ = 'x';
    for (int i = 60; i >= 0; i -= 4) {
        *p++ = HEX_DIGITS[static_cast<size_t>((v >> i) & 0xf)];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
}

void append_dec64(char*& p, const char* end, uint64_t v) {
    std::array<char, 22> tmp{};
    int n = 0;
    if (v == 0) {
        tmp[static_cast<size_t>(n++)] = '0';  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    } else {
        while (v > 0 && static_cast<size_t>(n) < tmp.size() - 1) {
            tmp[static_cast<size_t>(n++)] =
                static_cast<char>('0' + (v % 10));  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            v /= 10;
        }
    }
    // reverse into output
    if (p + n >= end) {
        return;
    }
    for (int i = n - 1; i >= 0; --i) {
        *p++ = tmp[static_cast<size_t>(i)];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
}

void append_sdec64(char*& p, const char* end, int64_t v) {
    if (v < 0) {
        append_char(p, end, '-');
        append_dec64(p, end, static_cast<uint64_t>(-v));
        return;
    }
    append_dec64(p, end, static_cast<uint64_t>(v));
}

void append_sconst(char*& p, const char* end, const char* s) {
    while (((*s) != 0) && p + 1 < end) {
        *p++ = *s++;
    }
}

void append_char(char*& p, const char* end, char c) {
    if (p + 1 < end) {
        *p++ = c;
    }
}

void append_bool01(char*& p, const char* end, bool v) { append_dec64(p, end, v ? 1U : 0U); }

void append_hex16(char*& p, const char* end, uint16_t v) {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    if (p + 6 >= end) {
        return;
    }
    *p++ = '0';
    *p++ = 'x';
    for (int i = 12; i >= 0; i -= 4) {
        *p++ = HEX_DIGITS[static_cast<size_t>((v >> i) & 0xf)];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
}

auto remote_compute_diag_kind_name(ker::net::wki::WkiRemoteComputeDiagKind kind) -> const char* {
    switch (kind) {
        case ker::net::wki::WkiRemoteComputeDiagKind::UNKNOWN:
            return "unknown";
        case ker::net::wki::WkiRemoteComputeDiagKind::SUBMITTED:
            return "submitted";
        case ker::net::wki::WkiRemoteComputeDiagKind::RUNNING:
            return "running";
        case ker::net::wki::WkiRemoteComputeDiagKind::PENDING_COMPLETE:
            return "pending_complete";
    }
    return "unknown";
}

void append_ipv4(char*& p, const char* end, uint32_t ip) {
    append_dec64(p, end, (ip >> 24U) & 0xffU);
    append_char(p, end, '.');
    append_dec64(p, end, (ip >> 16U) & 0xffU);
    append_char(p, end, '.');
    append_dec64(p, end, (ip >> 8U) & 0xffU);
    append_char(p, end, '.');
    append_dec64(p, end, ip & 0xffU);
}

auto peer_state_name(ker::net::wki::PeerState state) -> const char* {
    switch (state) {
        case ker::net::wki::PeerState::UNKNOWN:
            return "UNKNOWN";
        case ker::net::wki::PeerState::HELLO_SENT:
            return "HELLO_SENT";
        case ker::net::wki::PeerState::CONNECTED:
            return "CONNECTED";
        case ker::net::wki::PeerState::FENCED:
            return "FENCED";
        case ker::net::wki::PeerState::RECONNECTING:
            return "RECONNECTING";
    }
    return "?";
}

auto tcp_state_name(uint8_t state) -> const char* {
    switch (static_cast<ker::net::proto::TcpState>(state)) {
        case ker::net::proto::TcpState::CLOSED:
            return "CLOSED";
        case ker::net::proto::TcpState::LISTEN:
            return "LISTEN";
        case ker::net::proto::TcpState::SYN_SENT:
            return "SYN_SENT";
        case ker::net::proto::TcpState::SYN_RECEIVED:
            return "SYN_RECEIVED";
        case ker::net::proto::TcpState::ESTABLISHED:
            return "ESTABLISHED";
        case ker::net::proto::TcpState::FIN_WAIT_1:
            return "FIN_WAIT_1";
        case ker::net::proto::TcpState::FIN_WAIT_2:
            return "FIN_WAIT_2";
        case ker::net::proto::TcpState::CLOSE_WAIT:
            return "CLOSE_WAIT";
        case ker::net::proto::TcpState::CLOSING:
            return "CLOSING";
        case ker::net::proto::TcpState::LAST_ACK:
            return "LAST_ACK";
        case ker::net::proto::TcpState::TIME_WAIT:
            return "TIME_WAIT";
    }
    return "?";
}

auto priority_name(uint8_t priority) -> const char* {
    return priority == static_cast<uint8_t>(ker::net::wki::PriorityClass::THROUGHPUT) ? "THROUGHPUT" : "LATENCY";
}

auto napi_state_name(uint8_t state) -> const char* {
    switch (static_cast<ker::net::NapiState>(state)) {
        case ker::net::NapiState::IDLE:
            return "IDLE";
        case ker::net::NapiState::SCHEDULED:
            return "SCHEDULED";
        case ker::net::NapiState::POLLING:
            return "POLLING";
        case ker::net::NapiState::DISABLED:
            return "DISABLED";
    }
    return "?";
}

void append_netdev_name(char*& p, const char* end, const std::array<char, ker::net::NETDEV_NAME_LEN>& name) {
    for (char c : name) {
        if (c == '\0') {
            return;
        }
        append_char(p, end, c);
    }
}

void append_memacc_key(char*& p, const char* end, const char* key) {
    append_char(p, end, ' ');
    append_sconst(p, end, key);
    append_char(p, end, '=');
}

void append_memacc_dec(char*& p, const char* end, const char* key, uint64_t value) {
    append_memacc_key(p, end, key);
    append_dec64(p, end, value);
}

void append_memacc_hex(char*& p, const char* end, const char* key, uint64_t value) {
    append_memacc_key(p, end, key);
    append_hex64(p, end, value);
}

void append_memacc_bool(char*& p, const char* end, const char* key, bool value) { append_memacc_dec(p, end, key, value ? 1U : 0U); }

void append_percent_byte(char*& p, const char* end, uint8_t value) {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    if (p + 3 >= end) {
        return;
    }
    *p++ = '%';
    *p++ = HEX_DIGITS.at((value >> 4U) & 0xfU);
    *p++ = HEX_DIGITS.at(value & 0xfU);
}

void append_memacc_str(char*& p, const char* end, const char* key, const char* value) {
    append_memacc_key(p, end, key);
    if (value == nullptr || value[0] == '\0') {
        append_sconst(p, end, "-");
        return;
    }
    while (*value != '\0' && p + 1 < end) {
        auto const CH = static_cast<uint8_t>(*value);
        if (CH <= static_cast<uint8_t>(' ') || CH == static_cast<uint8_t>('%') || CH == static_cast<uint8_t>('=')) {
            append_percent_byte(p, end, CH);
        } else {
            append_char(p, end, *value);
        }
        value++;
    }
}

void append_memacc_feature_row(char*& p, const char* end, const char* name, bool available, bool enabled, bool default_enabled,
                               uint64_t generation) {
    append_sconst(p, end, "feature");
    append_memacc_str(p, end, "name", name);
    append_memacc_bool(p, end, "available", available);
    append_memacc_bool(p, end, "default", default_enabled);
    append_memacc_bool(p, end, "enabled", enabled);
    append_memacc_dec(p, end, "generation", generation);
    append_char(p, end, '\n');
}

auto task_type_name(ker::mod::sched::task::TaskType type) -> const char* {
    switch (type) {
        case ker::mod::sched::task::TaskType::DAEMON:
            return "daemon";
        case ker::mod::sched::task::TaskType::PROCESS:
            return "process";
        case ker::mod::sched::task::TaskType::IDLE:
            return "idle";
    }
    return "unknown";
}

auto task_queue_name(enum ker::mod::sched::task::Task::sched_queue queue) -> const char* {
    switch (queue) {
        case ker::mod::sched::task::Task::sched_queue::NONE:
            return "none";
        case ker::mod::sched::task::Task::sched_queue::RUNNABLE:
            return "runnable";
        case ker::mod::sched::task::Task::sched_queue::WAITING:
            return "waiting";
        case ker::mod::sched::task::Task::sched_queue::DEAD_GC:
            return "dead_gc";
    }
    return "unknown";
}

auto task_state_name(const ker::mod::sched::task::Task* task) -> const char* {
    if (task == nullptr) {
        return "unknown";
    }
    auto const STATE = task->state.load(std::memory_order_acquire);
    if (STATE == ker::mod::sched::task::TaskState::DEAD || task->has_exited) {
        return "dead";
    }
    if (STATE == ker::mod::sched::task::TaskState::EXITING) {
        return "exiting";
    }
    if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::RUNNABLE) {
        return "running";
    }
    if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING) {
        return task->wait_channel != nullptr ? "blocked" : "sleeping";
    }
    return "sleeping";
}

auto task_short_name(const ker::mod::sched::task::Task* task) -> const char* {
    if (task == nullptr) {
        return "unknown";
    }
    if (task->name != nullptr && task->name[0] != '\0') {
        return task->name;
    }
    const char* comm = task->exe_path.data();
    if (comm[0] != '\0') {
        for (const char* p = comm; *p != '\0'; ++p) {
            if (*p == '/') {
                comm = p + 1;
            }
        }
        if (comm[0] != '\0') {
            return comm;
        }
    }
    return "unknown";
}

auto task_command_name(const ker::mod::sched::task::Task* task) -> const char* {
    if (task == nullptr) {
        return "unknown";
    }
    if (task->exe_path[0] != '\0') {
        return task->exe_path.data();
    }
    return task_short_name(task);
}

constexpr uint64_t MEMACC_PAGE_BYTES = ker::mod::mm::paging::PAGE_SIZE;

auto pages_to_bytes(uint64_t pages) -> uint64_t { return pages * MEMACC_PAGE_BYTES; }

struct MemaccProcessTotals {
    uint64_t task_count;
    uint64_t process_count;
    uint64_t kernel_task_count;
    uint64_t virtual_bytes;
    uint64_t resident_bytes;
    uint64_t shared_bytes;
    uint64_t pte_bytes;
    uint64_t code_bytes;
    uint64_t heap_bytes;
    uint64_t mmap_bytes;
    uint64_t stack_bytes;
    uint64_t other_bytes;
    uint64_t rw_bytes;
    uint64_t rx_bytes;
    uint64_t ro_bytes;
};

void add_process_totals(MemaccProcessTotals& totals, const ker::mod::mm::memacc::UserMemoryBreakdown& mem) {
    totals.virtual_bytes += pages_to_bytes(mem.virtual_pages);
    totals.resident_bytes += pages_to_bytes(mem.resident_pages);
    totals.shared_bytes += pages_to_bytes(mem.shared_pages);
    totals.pte_bytes += pages_to_bytes(mem.page_table_pages);
    totals.code_bytes += pages_to_bytes(mem.code_pages);
    totals.heap_bytes += pages_to_bytes(mem.heap_pages);
    totals.mmap_bytes += pages_to_bytes(mem.mmap_pages);
    totals.stack_bytes += pages_to_bytes(mem.stack_pages);
    totals.other_bytes += pages_to_bytes(mem.other_pages);
    totals.rw_bytes += pages_to_bytes(mem.rw_pages);
    totals.rx_bytes += pages_to_bytes(mem.rx_pages);
    totals.ro_bytes += pages_to_bytes(mem.ro_pages);
}

auto collect_memacc_process_totals() -> MemaccProcessTotals {
    MemaccProcessTotals totals{};
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        totals.task_count++;
        if (task->type != ker::mod::sched::task::TaskType::PROCESS) {
            totals.kernel_task_count++;
        }
        if (ker::mod::sched::task::process_visible(*task)) {
            totals.process_count++;
            add_process_totals(totals, ker::mod::mm::memacc::collect_user_memory_breakdown(task->pagemap));
        }
        task->release();
    }
    return totals;
}

auto generate_memacc_summary(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    ker::mod::mm::phys::AllocStatsSnapshot phys{};
    ker::mod::mm::phys::get_alloc_stats_snapshot(phys);
    auto const PROC = collect_memacc_process_totals();
    auto const BCACHE = ker::vfs::buffer_cache_stats();
    auto const FILE_CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    auto const ELF_CACHE = ker::net::wki::wki_shared_elf_cache_stats();
    ker::net::wki::WkiIpcPerfSnapshot ipc{};
    ker::net::wki::wki_ipc_get_perf_snapshot(ipc);
    ker::vfs::LocalPipePerfSnapshot local_pipe{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(local_pipe);
    ker::mod::mm::dyn::kmalloc::KmallocTrackedTotals kmalloc{};
    ker::mod::mm::dyn::kmalloc::get_tracked_alloc_breakdown(kmalloc);
    auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();

    uint64_t const USED_BYTES = phys.total_mem_bytes >= phys.free_mem_bytes ? phys.total_mem_bytes - phys.free_mem_bytes : 0;
    uint64_t const KMALLOC_BYTES = kmalloc.medium_bytes + kmalloc.large_bytes;
    uint64_t const KMALLOC_DEBUG_BYTES = KMALLOC_DEBUG.block_bytes;
    uint64_t const MINI_BYTES = ker::mod::mm::mini_malloc::mini_get_total_slab_bytes();
    uint64_t const ALLOCATOR_BYTES = KMALLOC_BYTES + MINI_BYTES + KMALLOC_DEBUG_BYTES;
    uint64_t const CACHE_BYTES = static_cast<uint64_t>(BCACHE.total_bytes) + FILE_CACHE.bytes + ELF_CACHE.bytes + ipc.approx_alloc_bytes +
                                 local_pipe.approx_alloc_bytes;
    uint64_t const USER_UNIQUE_ESTIMATE =
        PROC.resident_bytes >= PROC.shared_bytes ? PROC.resident_bytes - PROC.shared_bytes : PROC.resident_bytes;
    uint64_t accounted = USER_UNIQUE_ESTIMATE + PROC.pte_bytes + ALLOCATOR_BYTES + CACHE_BYTES;
    uint64_t const UNACCOUNTED = USED_BYTES > accounted ? USED_BYTES - accounted : 0;

    append_sconst(p, end, "summary");
    append_memacc_dec(p, end, "total_bytes", phys.total_mem_bytes);
    append_memacc_dec(p, end, "free_bytes", phys.free_mem_bytes);
    append_memacc_dec(p, end, "used_bytes", USED_BYTES);
    append_memacc_dec(p, end, "processes", PROC.process_count);
    append_memacc_dec(p, end, "tasks", PROC.task_count);
    append_memacc_dec(p, end, "kernel_tasks", PROC.kernel_task_count);
    append_memacc_dec(p, end, "process_virtual_bytes", PROC.virtual_bytes);
    append_memacc_dec(p, end, "process_rss_bytes", PROC.resident_bytes);
    append_memacc_dec(p, end, "process_shared_bytes", PROC.shared_bytes);
    append_memacc_dec(p, end, "process_pte_bytes", PROC.pte_bytes);
    append_memacc_dec(p, end, "process_code_bytes", PROC.code_bytes);
    append_memacc_dec(p, end, "process_heap_bytes", PROC.heap_bytes);
    append_memacc_dec(p, end, "process_mmap_bytes", PROC.mmap_bytes);
    append_memacc_dec(p, end, "process_stack_bytes", PROC.stack_bytes);
    append_memacc_dec(p, end, "process_other_bytes", PROC.other_bytes);
    append_memacc_dec(p, end, "process_rw_bytes", PROC.rw_bytes);
    append_memacc_dec(p, end, "process_rx_bytes", PROC.rx_bytes);
    append_memacc_dec(p, end, "process_ro_bytes", PROC.ro_bytes);
    append_memacc_dec(p, end, "allocator_bytes", ALLOCATOR_BYTES);
    append_memacc_dec(p, end, "kmalloc_bytes", KMALLOC_BYTES);
    append_memacc_dec(p, end, "kmalloc_debug_pool_bytes", KMALLOC_DEBUG_BYTES);
    append_memacc_dec(p, end, "mini_slab_bytes", MINI_BYTES);
    append_memacc_dec(p, end, "cache_bytes", CACHE_BYTES);
    append_memacc_dec(p, end, "unaccounted_estimate_bytes", UNACCOUNTED);
    append_char(p, end, '\n');

    append_memacc_feature_row(p, end, "page_callers", ker::mod::mm::phys::page_caller_stats_available(),
                              ker::mod::mm::phys::page_caller_stats_enabled(), ker::mod::mm::phys::page_caller_stats_default_enabled(),
                              ker::mod::mm::phys::page_caller_stats_generation());
    append_memacc_feature_row(p, end, "kmalloc_debug", ker::mod::mm::dyn::kmalloc::debug_info_available(),
                              ker::mod::mm::dyn::kmalloc::debug_info_enabled(), ker::mod::mm::dyn::kmalloc::debug_info_default_enabled(),
                              ker::mod::mm::dyn::kmalloc::debug_info_generation());

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_zones(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_ZONES = 32;
    std::array<ker::mod::mm::phys::ZoneSnapshot, MAX_ZONES> zones{};
    size_t const ROWS = ker::mod::mm::phys::snapshot_zones(zones.data(), zones.size());
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& zone = zones.at(i);
        append_sconst(p, end, "zone");
        append_memacc_dec(p, end, "id", zone.zone_num);
        append_memacc_str(p, end, "name", zone.name);
        append_memacc_hex(p, end, "start", zone.start);
        append_memacc_dec(p, end, "len_bytes", zone.len);
        append_memacc_dec(p, end, "page_count", zone.page_count);
        append_memacc_dec(p, end, "total_pages", zone.total_pages);
        append_memacc_dec(p, end, "usable_pages", zone.usable_pages);
        append_memacc_dec(p, end, "free_pages", zone.free_pages);
        append_memacc_dec(p, end, "metadata_pages", zone.metadata_pages);
        append_memacc_dec(p, end, "scanned_free_pages", zone.scanned_free_pages);
        append_memacc_bool(p, end, "has_allocator", zone.has_allocator);
        append_memacc_bool(p, end, "invalid_allocator", zone.invalid_allocator);
        append_memacc_bool(p, end, "bad_order", zone.bad_order);
        append_memacc_bool(p, end, "free_count_mismatch", zone.free_count_mismatch);
        append_char(p, end, '\n');

        for (size_t order = 0; order < zone.buddy_order_counts.size(); ++order) {
            uint64_t const BLOCKS = zone.buddy_order_counts.at(order);
            if (BLOCKS == 0) {
                continue;
            }
            uint64_t const PAGES = BLOCKS * (uint64_t{1} << order);
            append_sconst(p, end, "buddy");
            append_memacc_dec(p, end, "zone", zone.zone_num);
            append_memacc_dec(p, end, "order", order);
            append_memacc_dec(p, end, "free_blocks", BLOCKS);
            append_memacc_dec(p, end, "free_pages", PAGES);
            append_memacc_dec(p, end, "free_bytes", pages_to_bytes(PAGES));
            append_char(p, end, '\n');
        }
    }
    if (ROWS == MAX_ZONES) {
        append_sconst(p, end, "truncated file=zones");
        append_memacc_dec(p, end, "max_rows", MAX_ZONES);
        append_char(p, end, '\n');
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_procs(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::process_visible(*task)) {
            task->release();
            continue;
        }

        auto const MEM = ker::mod::mm::memacc::collect_user_memory_breakdown(task->pagemap);
        append_sconst(p, end, "proc");
        append_memacc_dec(p, end, "pid", task->pid);
        append_memacc_dec(p, end, "ppid", task->parent_pid);
        append_memacc_dec(p, end, "uid", task->uid);
        append_memacc_dec(p, end, "gid", task->gid);
        append_memacc_str(p, end, "state", task_state_name(task));
        append_memacc_str(p, end, "queue", task_queue_name(task->sched_queue));
        append_memacc_dec(p, end, "cpu", task->cpu);
        append_memacc_str(p, end, "type", task_type_name(task->type));
        append_memacc_str(p, end, "name", task_short_name(task));
        append_memacc_str(p, end, "cmd", task_command_name(task));
        append_memacc_dec(p, end, "virt_bytes", pages_to_bytes(MEM.virtual_pages));
        append_memacc_dec(p, end, "rss_bytes", pages_to_bytes(MEM.resident_pages));
        append_memacc_dec(p, end, "shr_bytes", pages_to_bytes(MEM.shared_pages));
        append_memacc_dec(p, end, "pte_bytes", pages_to_bytes(MEM.page_table_pages));
        append_memacc_dec(p, end, "code_bytes", pages_to_bytes(MEM.code_pages));
        append_memacc_dec(p, end, "heap_bytes", pages_to_bytes(MEM.heap_pages));
        append_memacc_dec(p, end, "mmap_bytes", pages_to_bytes(MEM.mmap_pages));
        append_memacc_dec(p, end, "stack_bytes", pages_to_bytes(MEM.stack_pages));
        append_memacc_dec(p, end, "other_bytes", pages_to_bytes(MEM.other_pages));
        append_memacc_dec(p, end, "rw_bytes", pages_to_bytes(MEM.rw_pages));
        append_memacc_dec(p, end, "rx_bytes", pages_to_bytes(MEM.rx_pages));
        append_memacc_dec(p, end, "ro_bytes", pages_to_bytes(MEM.ro_pages));
        append_memacc_hex(p, end, "pagemap", reinterpret_cast<uint64_t>(task->pagemap));
        append_char(p, end, '\n');
        task->release();
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_dead(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }

    constexpr size_t BATCH = 64;
    std::array<uint64_t, BATCH> pids{};
    std::array<uint32_t, BATCH> refcounts{};
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        auto const STATS = ker::mod::sched::get_run_queue_stats(cpu);
        append_sconst(p, end, "queue");
        append_memacc_dec(p, end, "cpu", cpu);
        append_memacc_dec(p, end, "run", STATS.active_task_count);
        append_memacc_dec(p, end, "wait", STATS.wait_queue_count);
        append_memacc_dec(p, end, "dead", STATS.expired_task_count);
        append_char(p, end, '\n');

        size_t const DEAD_COUNT = ker::mod::sched::get_dead_task_count(cpu);
        for (size_t start = 0; start < DEAD_COUNT; start += BATCH) {
            size_t const ROWS = ker::mod::sched::get_expired_task_refcounts(cpu, pids.data(), refcounts.data(), pids.size(), start);
            for (size_t i = 0; i < ROWS; ++i) {
                append_sconst(p, end, "dead_task");
                append_memacc_dec(p, end, "cpu", cpu);
                append_memacc_dec(p, end, "pid", pids.at(i));
                append_memacc_dec(p, end, "refcount", refcounts.at(i));
                append_char(p, end, '\n');
            }
            if (ROWS == 0) {
                break;
            }
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_alloc_totals(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    ker::mod::mm::phys::AllocStatsSnapshot phys{};
    ker::mod::mm::phys::get_alloc_stats_snapshot(phys);
    append_sconst(p, end, "phys");
    append_memacc_dec(p, end, "total_allocated_bytes", phys.total_allocated_bytes);
    append_memacc_dec(p, end, "total_freed_bytes", phys.total_freed_bytes);
    append_memacc_dec(p, end, "live_allocated_bytes", phys.live_allocated_bytes);
    append_memacc_dec(p, end, "alloc_count", phys.alloc_count);
    append_memacc_dec(p, end, "free_count", phys.free_count);
    append_memacc_dec(p, end, "total_mem_bytes", phys.total_mem_bytes);
    append_memacc_dec(p, end, "free_mem_bytes", phys.free_mem_bytes);
    append_char(p, end, '\n');

    ker::mod::mm::dyn::kmalloc::KmallocTrackedTotals kmalloc{};
    ker::mod::mm::dyn::kmalloc::get_tracked_alloc_breakdown(kmalloc);
    auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
    append_sconst(p, end, "kmalloc");
    append_memacc_dec(p, end, "medium_count", kmalloc.medium_count);
    append_memacc_dec(p, end, "medium_bytes", kmalloc.medium_bytes);
    append_memacc_dec(p, end, "large_count", kmalloc.large_count);
    append_memacc_dec(p, end, "large_bytes", kmalloc.large_bytes);
    append_memacc_dec(p, end, "debug_pool_blocks", KMALLOC_DEBUG.block_count);
    append_memacc_dec(p, end, "debug_pool_bytes", KMALLOC_DEBUG.block_bytes);
    append_memacc_dec(p, end, "debug_capacity", KMALLOC_DEBUG.capacity);
    append_memacc_dec(p, end, "debug_active", KMALLOC_DEBUG.active);
    append_memacc_dec(p, end, "debug_high_water", KMALLOC_DEBUG.high_water);
    append_memacc_dec(p, end, "debug_dropped", KMALLOC_DEBUG.dropped);
    append_memacc_dec(p, end, "total_count", kmalloc.medium_count + kmalloc.large_count);
    append_memacc_dec(p, end, "total_bytes", kmalloc.medium_bytes + kmalloc.large_bytes);
    append_memacc_dec(p, end, "accounted_bytes", kmalloc.medium_bytes + kmalloc.large_bytes + KMALLOC_DEBUG.block_bytes);
    append_char(p, end, '\n');

    append_sconst(p, end, "mini_slab");
    append_memacc_dec(p, end, "total_bytes", ker::mod::mm::mini_malloc::mini_get_total_slab_bytes());
    append_char(p, end, '\n');

    auto const BCACHE = ker::vfs::buffer_cache_stats();
    append_sconst(p, end, "buffer_cache");
    append_memacc_dec(p, end, "buffers", BCACHE.total_buffers);
    append_memacc_dec(p, end, "dirty_buffers", BCACHE.dirty_buffers);
    append_memacc_dec(p, end, "total_bytes", BCACHE.total_bytes);
    append_memacc_dec(p, end, "clean_bytes", BCACHE.clean_bytes);
    append_memacc_dec(p, end, "dirty_bytes", BCACHE.dirty_bytes);
    append_memacc_dec(p, end, "max_bytes", BCACHE.max_bytes);
    append_memacc_dec(p, end, "hits", BCACHE.hits);
    append_memacc_dec(p, end, "misses", BCACHE.misses);
    append_char(p, end, '\n');

    auto const FILE_CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    append_sconst(p, end, "file_cache");
    append_memacc_dec(p, end, "pages", FILE_CACHE.pages);
    append_memacc_dec(p, end, "bytes", FILE_CACHE.bytes);
    append_memacc_dec(p, end, "capacity_pages", FILE_CACHE.capacity_pages);
    append_char(p, end, '\n');

    auto const ELF_CACHE = ker::net::wki::wki_shared_elf_cache_stats();
    append_sconst(p, end, "wki_elf_cache");
    append_memacc_dec(p, end, "entries", ELF_CACHE.entries);
    append_memacc_dec(p, end, "bytes", ELF_CACHE.bytes);
    append_memacc_dec(p, end, "max_entries", ELF_CACHE.max_entries);
    append_memacc_dec(p, end, "max_bytes", ELF_CACHE.max_bytes);
    append_char(p, end, '\n');

    ker::net::wki::WkiIpcPerfSnapshot ipc{};
    ker::net::wki::wki_ipc_get_perf_snapshot(ipc);
    append_sconst(p, end, "wki_ipc");
    append_memacc_dec(p, end, "exports", ipc.exports);
    append_memacc_dec(p, end, "proxies", ipc.proxies);
    append_memacc_dec(p, end, "pump_tasks", ipc.pump_tasks);
    append_memacc_dec(p, end, "ring_bytes", ipc.proxy_ring_bytes);
    append_memacc_dec(p, end, "ring_used_bytes", ipc.proxy_ring_used_bytes);
    append_memacc_dec(p, end, "pending_chunks", ipc.pending_chunks);
    append_memacc_dec(p, end, "pending_bytes", ipc.pending_bytes);
    append_memacc_dec(p, end, "export_backlog_bytes", ipc.export_backlog_bytes);
    append_memacc_dec(p, end, "dev_op_payload_bytes", ipc.dev_op_payload_bytes);
    append_memacc_dec(p, end, "approx_alloc_bytes", ipc.approx_alloc_bytes);
    append_char(p, end, '\n');

    ker::vfs::LocalPipePerfSnapshot local_pipe{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(local_pipe);
    append_sconst(p, end, "local_pipe");
    append_memacc_dec(p, end, "active_pipes", local_pipe.active_pipes);
    append_memacc_dec(p, end, "created", local_pipe.created_since_reset);
    append_memacc_dec(p, end, "peak_pipes", local_pipe.peak_pipes);
    append_memacc_dec(p, end, "capacity_bytes", local_pipe.capacity_bytes);
    append_memacc_dec(p, end, "buffered_bytes", local_pipe.buffered_bytes);
    append_memacc_dec(p, end, "reader_waiters", local_pipe.reader_waiters);
    append_memacc_dec(p, end, "writer_waiters", local_pipe.writer_waiters);
    append_memacc_dec(p, end, "poll_waiters", local_pipe.poll_waiters);
    append_memacc_dec(p, end, "approx_alloc_bytes", local_pipe.approx_alloc_bytes);
    append_char(p, end, '\n');

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_slabs(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_SLABS = 16;
    std::array<ker::mod::mm::mini_malloc::MiniSlabStats, MAX_SLABS> slabs{};
    size_t const ROWS = ker::mod::mm::mini_malloc::mini_collect_slab_stats(slabs.data(), slabs.size());
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& slab = slabs.at(i);
        append_sconst(p, end, "slab");
        append_memacc_str(p, end, "class", slab.name);
        append_memacc_dec(p, end, "object_size", slab.object_size);
        append_memacc_dec(p, end, "slabs", slab.slab_count);
        append_memacc_dec(p, end, "total_blocks", slab.total_blocks);
        append_memacc_dec(p, end, "free_blocks", slab.free_blocks);
        append_memacc_dec(p, end, "used_blocks", slab.total_blocks >= slab.free_blocks ? slab.total_blocks - slab.free_blocks : 0);
        append_memacc_dec(p, end, "page_bytes", slab.page_bytes);
        append_memacc_dec(p, end, "used_object_bytes",
                          (slab.total_blocks >= slab.free_blocks ? slab.total_blocks - slab.free_blocks : 0) * slab.object_size);
        append_char(p, end, '\n');
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_kmalloc_live(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_ROWS = 2048;
    auto* rows = new (std::nothrow) ker::mod::mm::dyn::kmalloc::KmallocLiveAlloc[MAX_ROWS];
    if (rows == nullptr) {
        append_sconst(p, end, "error file=kmalloc_live reason=oom\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }
    size_t total_rows = 0;
    size_t const ROWS = ker::mod::mm::dyn::kmalloc::snapshot_live_allocs(rows, MAX_ROWS, total_rows);
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& row = rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "kmalloc_live");
        append_memacc_str(p, end, "tier", row.tier);
        append_memacc_hex(p, end, "addr", row.addr);
        append_memacc_dec(p, end, "size", row.size);
        append_memacc_bool(p, end, "has_debug", row.has_debug);
        if (row.has_debug) {
            append_memacc_hex(p, end, "caller", row.caller);
            append_memacc_str(p, end, "tag", row.tag);
        }
        append_char(p, end, '\n');
    }
    if (ROWS < total_rows) {
        append_sconst(p, end, "truncated file=kmalloc_live");
        append_memacc_dec(p, end, "rows", ROWS);
        append_memacc_dec(p, end, "total_rows", total_rows);
        append_char(p, end, '\n');
    }
    delete[] rows;
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_kmalloc_callers(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_ROWS = 512;
    auto* rows = new (std::nothrow) ker::mod::mm::dyn::kmalloc::KmallocCallerStat[MAX_ROWS];
    if (rows == nullptr) {
        append_sconst(p, end, "error file=kmalloc_callers reason=oom\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }

    size_t total_rows = 0;
    size_t const ROWS = ker::mod::mm::dyn::kmalloc::snapshot_caller_stats(rows, MAX_ROWS, total_rows);
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& row = rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "kmalloc_caller");
        append_memacc_str(p, end, "tier", row.tier);
        append_memacc_dec(p, end, "count", row.count);
        append_memacc_dec(p, end, "bytes", row.bytes);
        append_memacc_bool(p, end, "has_debug", row.has_debug);
        if (row.has_debug) {
            append_memacc_hex(p, end, "caller", row.caller);
            append_memacc_str(p, end, "tag", row.tag);
        }
        append_char(p, end, '\n');
    }
    if (ROWS < total_rows) {
        append_sconst(p, end, "truncated file=kmalloc_callers");
        append_memacc_dec(p, end, "rows", ROWS);
        append_memacc_dec(p, end, "total_rows", total_rows);
        append_char(p, end, '\n');
    }

    delete[] rows;
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_page_callers(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    append_sconst(p, end, "page_callers_status");
    append_memacc_bool(p, end, "available", ker::mod::mm::phys::page_caller_stats_available());
    append_memacc_bool(p, end, "enabled", ker::mod::mm::phys::page_caller_stats_enabled());
    append_memacc_dec(p, end, "generation", ker::mod::mm::phys::page_caller_stats_generation());
    append_memacc_str(p, end, "mode", "live");
    append_char(p, end, '\n');

    constexpr size_t MAX_ROWS = 64;
    std::array<ker::mod::mm::phys::CallerPageStat, MAX_ROWS> rows{};
    size_t total_rows = 0;
    if (ker::mod::mm::phys::snapshot_page_caller_stats(rows.data(), rows.size(), total_rows)) {
        size_t const ROWS = std::min(total_rows, rows.size());
        for (size_t i = 0; i < ROWS; ++i) {
            const auto& row = rows.at(i);
            append_sconst(p, end, "page_caller");
            append_memacc_hex(p, end, "caller", row.caller);
            append_memacc_dec(p, end, "pages", row.pages);
            append_memacc_dec(p, end, "bytes", pages_to_bytes(row.pages));
            append_char(p, end, '\n');
        }
        if (ROWS < total_rows) {
            append_sconst(p, end, "truncated file=page_callers");
            append_memacc_dec(p, end, "rows", ROWS);
            append_memacc_dec(p, end, "total_rows", total_rows);
            append_char(p, end, '\n');
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_features(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    append_memacc_feature_row(p, end, "page_callers", ker::mod::mm::phys::page_caller_stats_available(),
                              ker::mod::mm::phys::page_caller_stats_enabled(), ker::mod::mm::phys::page_caller_stats_default_enabled(),
                              ker::mod::mm::phys::page_caller_stats_generation());
    append_memacc_feature_row(p, end, "kmalloc_debug", ker::mod::mm::dyn::kmalloc::debug_info_available(),
                              ker::mod::mm::dyn::kmalloc::debug_info_enabled(), ker::mod::mm::dyn::kmalloc::debug_info_default_enabled(),
                              ker::mod::mm::dyn::kmalloc::debug_info_generation());
    auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
    append_sconst(p, end, "kmalloc_debug_pool");
    append_memacc_dec(p, end, "blocks", KMALLOC_DEBUG.block_count);
    append_memacc_dec(p, end, "bytes", KMALLOC_DEBUG.block_bytes);
    append_memacc_dec(p, end, "capacity", KMALLOC_DEBUG.capacity);
    append_memacc_dec(p, end, "active", KMALLOC_DEBUG.active);
    append_memacc_dec(p, end, "high_water", KMALLOC_DEBUG.high_water);
    append_memacc_dec(p, end, "dropped", KMALLOC_DEBUG.dropped);
    append_char(p, end, '\n');

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_track_state(ProcNodeType type, char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    if (type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE) {
        append_memacc_feature_row(p, end, "page_callers", ker::mod::mm::phys::page_caller_stats_available(),
                                  ker::mod::mm::phys::page_caller_stats_enabled(), ker::mod::mm::phys::page_caller_stats_default_enabled(),
                                  ker::mod::mm::phys::page_caller_stats_generation());
    } else {
        append_memacc_feature_row(
            p, end, "kmalloc_debug", ker::mod::mm::dyn::kmalloc::debug_info_available(), ker::mod::mm::dyn::kmalloc::debug_info_enabled(),
            ker::mod::mm::dyn::kmalloc::debug_info_default_enabled(), ker::mod::mm::dyn::kmalloc::debug_info_generation());
        auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
        append_sconst(p, end, "kmalloc_debug_pool");
        append_memacc_dec(p, end, "blocks", KMALLOC_DEBUG.block_count);
        append_memacc_dec(p, end, "bytes", KMALLOC_DEBUG.block_bytes);
        append_memacc_dec(p, end, "capacity", KMALLOC_DEBUG.capacity);
        append_memacc_dec(p, end, "active", KMALLOC_DEBUG.active);
        append_memacc_dec(p, end, "high_water", KMALLOC_DEBUG.high_water);
        append_memacc_dec(p, end, "dropped", KMALLOC_DEBUG.dropped);
        append_char(p, end, '\n');
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_buffer_cache(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    auto const BCACHE = ker::vfs::buffer_cache_stats();
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "buffer_cache");
    append_memacc_dec(p, end, "total_bytes", BCACHE.total_bytes);
    append_memacc_dec(p, end, "clean_bytes", BCACHE.clean_bytes);
    append_memacc_dec(p, end, "dirty_bytes", BCACHE.dirty_bytes);
    append_memacc_dec(p, end, "buffers", BCACHE.total_buffers);
    append_memacc_dec(p, end, "dirty_buffers", BCACHE.dirty_buffers);
    append_memacc_dec(p, end, "default_target_bytes", 0);
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_packet_pool(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    auto const POOL = ker::net::pkt_pool_snapshot();
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "packet_pool");
    append_memacc_dec(p, end, "capacity", POOL.capacity);
    append_memacc_dec(p, end, "baseline_capacity", POOL.baseline_capacity);
    append_memacc_dec(p, end, "active_capacity", POOL.active_capacity);
    append_memacc_dec(p, end, "free", POOL.free);
    append_memacc_dec(p, end, "used", POOL.used);
    append_memacc_dec(p, end, "draining_buffers", POOL.draining_buffers);
    append_memacc_dec(p, end, "draining_free", POOL.draining_free);
    append_memacc_dec(p, end, "buffer_size", POOL.buffer_size);
    append_memacc_dec(p, end, "object_size", POOL.object_size);
    append_memacc_dec(p, end, "total_bytes", POOL.capacity * POOL.object_size);
    append_memacc_dec(p, end, "active_bytes", POOL.active_capacity * POOL.object_size);
    append_memacc_dec(p, end, "free_bytes", POOL.free * POOL.object_size);
    append_memacc_dec(p, end, "used_bytes", POOL.used * POOL.object_size);
    append_memacc_dec(p, end, "draining_bytes", POOL.draining_buffers * POOL.object_size);
    append_memacc_dec(p, end, "draining_free_bytes", POOL.draining_free * POOL.object_size);
    append_memacc_dec(p, end, "rx_reserve", POOL.rx_reserve);
    append_memacc_dec(p, end, "grow_chunk", POOL.grow_chunk);
    append_memacc_dec(p, end, "default_target_capacity", std::max(POOL.baseline_capacity, POOL.used + POOL.rx_reserve + POOL.grow_chunk));
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

void append_virtqueue_diag(char*& p, const char* end, const char* prefix, const ker::dev::virtio::VirtqueueDiagSnapshot& q) {
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_size=");
    append_dec64(p, end, q.size);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_free=");
    append_dec64(p, end, q.num_free);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_inflight=");
    append_dec64(p, end, q.size >= q.num_free ? q.size - q.num_free : 0);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_pending=");
    append_dec64(p, end, q.pending);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_mapped=");
    append_dec64(p, end, q.mapped);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_avail_idx=");
    append_dec64(p, end, q.avail_idx);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_used_idx=");
    append_dec64(p, end, q.used_idx);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_last_used=");
    append_dec64(p, end, q.last_used_idx);
}

auto generate_wki_netdiag(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }

    char* p = buf;
    char const* end = buf + bufsz - 1;

    auto pool = ker::net::pkt_pool_snapshot();
    append_sconst(p, end, "packet_pool capacity=");
    append_dec64(p, end, pool.capacity);
    append_sconst(p, end, " baseline=");
    append_dec64(p, end, pool.baseline_capacity);
    append_sconst(p, end, " active_capacity=");
    append_dec64(p, end, pool.active_capacity);
    append_sconst(p, end, " free=");
    append_dec64(p, end, pool.free);
    append_sconst(p, end, " used=");
    append_dec64(p, end, pool.used);
    append_sconst(p, end, " draining=");
    append_dec64(p, end, pool.draining_buffers);
    append_sconst(p, end, " draining_free=");
    append_dec64(p, end, pool.draining_free);
    append_sconst(p, end, " rx_reserve=");
    append_dec64(p, end, pool.rx_reserve);
    append_sconst(p, end, " grow_chunk=");
    append_dec64(p, end, pool.grow_chunk);
    append_sconst(p, end, " buf_size=");
    append_dec64(p, end, pool.buffer_size);
    append_sconst(p, end, " headroom=");
    append_dec64(p, end, pool.headroom);
    append_sconst(p, end, " tx_refused=");
    append_dec64(p, end, pool.tx_refused);
    append_sconst(p, end, " expanding=");
    append_bool01(p, end, pool.expand_in_progress);
    append_char(p, end, '\n');

    ker::net::BacklogSnapshot backlog{};
    ker::net::backlog_get_snapshot(backlog);
    append_sconst(p, end, "backlog ready=");
    append_bool01(p, end, backlog.ready);
    append_sconst(p, end, " cpus=");
    append_dec64(p, end, backlog.num_cpus);
    append_sconst(p, end, " queues=");
    append_dec64(p, end, backlog.queue_count);
    append_sconst(p, end, " queued=");
    append_dec64(p, end, backlog.total_queued);
    append_char(p, end, '\n');
    for (size_t i = 0; i < backlog.queue_count; ++i) {
        const auto& row = backlog.queues.at(i);
        append_sconst(p, end, "backlog_cpu cpu=");
        append_dec64(p, end, row.cpu);
        append_sconst(p, end, " queued=");
        append_dec64(p, end, row.queued);
        append_sconst(p, end, " handler_pid=");
        append_dec64(p, end, row.handler_pid);
        append_sconst(p, end, " handler_cpu=");
        append_dec64(p, end, row.handler_cpu);
        append_sconst(p, end, " active=");
        append_bool01(p, end, row.handler_active);
        append_char(p, end, '\n');
    }

    std::array<ker::net::NetDeviceSnapshot, ker::net::MAX_NET_DEVICES> devs{};
    size_t const DEV_COUNT = ker::net::netdev_snapshot(devs.data(), devs.size());
    for (size_t i = 0; i < DEV_COUNT; ++i) {
        const auto& dev = devs.at(i);
        append_sconst(p, end, "netdev name=");
        append_netdev_name(p, end, dev.name);
        append_sconst(p, end, " ifindex=");
        append_dec64(p, end, dev.ifindex);
        append_sconst(p, end, " state=");
        append_sconst(p, end, dev.state != 0 ? "up" : "down");
        append_sconst(p, end, " mtu=");
        append_dec64(p, end, dev.mtu);
        append_sconst(p, end, " txqlen=");
        append_dec64(p, end, dev.tx_queue_len);
        append_sconst(p, end, " wki_transport=");
        append_bool01(p, end, dev.wki_transport);
        append_sconst(p, end, " rx_forward=");
        append_bool01(p, end, dev.wki_rx_forward);
        append_sconst(p, end, " remotable=");
        append_bool01(p, end, dev.remotable);
        append_sconst(p, end, " rx_packets=");
        append_dec64(p, end, dev.rx_packets);
        append_sconst(p, end, " rx_bytes=");
        append_dec64(p, end, dev.rx_bytes);
        append_sconst(p, end, " rx_dropped=");
        append_dec64(p, end, dev.rx_dropped);
        append_sconst(p, end, " tx_packets=");
        append_dec64(p, end, dev.tx_packets);
        append_sconst(p, end, " tx_bytes=");
        append_dec64(p, end, dev.tx_bytes);
        append_sconst(p, end, " tx_dropped=");
        append_dec64(p, end, dev.tx_dropped);
        append_char(p, end, '\n');
    }

    std::array<ker::dev::virtio::VirtIONetDiagSnapshot, ker::dev::virtio::VIRTIO_NET_DIAG_MAX_ROWS> virtio_rows{};
    size_t const VIRTIO_COUNT = ker::dev::virtio::virtio_net_diag_snapshot(virtio_rows.data(), virtio_rows.size());
    for (size_t i = 0; i < VIRTIO_COUNT; ++i) {
        const auto& row = virtio_rows.at(i);
        append_sconst(p, end, "virtio_net name=");
        append_netdev_name(p, end, row.name);
        append_sconst(p, end, " ifindex=");
        append_dec64(p, end, row.ifindex);
        append_sconst(p, end, " pair=");
        append_dec64(p, end, row.pair);
        append_sconst(p, end, " active=");
        append_bool01(p, end, row.active);
        append_sconst(p, end, " pairs=");
        append_dec64(p, end, row.num_queue_pairs);
        append_sconst(p, end, " configured=");
        append_dec64(p, end, row.configured_queue_pairs);
        append_sconst(p, end, " msix=");
        append_bool01(p, end, row.msix_enabled);
        append_sconst(p, end, " vector=");
        append_dec64(p, end, row.irq_vector);
        append_sconst(p, end, " hdr_size=");
        append_dec64(p, end, row.hdr_size);
        append_sconst(p, end, " napi_state=");
        append_sconst(p, end, napi_state_name(row.napi_state));
        append_sconst(p, end, " napi_work=");
        append_bool01(p, end, row.napi_has_work);
        append_sconst(p, end, " worker_pid=");
        append_dec64(p, end, row.napi_worker_pid);
        append_sconst(p, end, " worker_cpu=");
        append_dec64(p, end, row.napi_worker_cpu);
        append_sconst(p, end, " polls=");
        append_dec64(p, end, row.napi_polls);
        append_sconst(p, end, " completes=");
        append_dec64(p, end, row.napi_completes);
        append_virtqueue_diag(p, end, "rx", row.rx);
        append_virtqueue_diag(p, end, "tx", row.tx);
        append_char(p, end, '\n');
    }
    if (VIRTIO_COUNT == virtio_rows.size()) {
        append_sconst(p, end, "virtio_net_truncated max=");
        append_dec64(p, end, virtio_rows.size());
        append_char(p, end, '\n');
    }

    std::array<ker::net::proto::TcpListenerSnapshot, ker::net::proto::TCP_LISTENER_SNAPSHOT_MAX> listeners{};
    size_t const LISTENER_COUNT = ker::net::proto::tcp_listener_snapshot(listeners.data(), listeners.size());
    for (size_t i = 0; i < LISTENER_COUNT; ++i) {
        const auto& listener = listeners.at(i);
        append_sconst(p, end, "tcp_listener local=");
        append_ipv4(p, end, listener.local_ip);
        append_char(p, end, ':');
        append_dec64(p, end, listener.local_port);
        append_sconst(p, end, " state=");
        append_sconst(p, end, tcp_state_name(listener.state));
        append_sconst(p, end, " owner_pid=");
        append_dec64(p, end, listener.owner_pid);
        append_sconst(p, end, " accept_queue=");
        append_dec64(p, end, listener.accept_queue);
        append_sconst(p, end, " backlog=");
        append_dec64(p, end, static_cast<uint64_t>(listener.backlog));
        append_sconst(p, end, " rcvbuf_used=");
        append_dec64(p, end, listener.rcvbuf_used);
        append_sconst(p, end, " rcvbuf_capacity=");
        append_dec64(p, end, listener.rcvbuf_capacity);
        append_sconst(p, end, " rcv_wnd=");
        append_dec64(p, end, listener.rcv_wnd);
        append_sconst(p, end, " refcnt=");
        append_dec64(p, end, listener.refcount);
        append_char(p, end, '\n');
    }

    ker::net::wki::WkiIpcPerfSnapshot ipc{};
    ker::net::wki::wki_ipc_get_perf_snapshot(ipc);
    append_sconst(p, end, "wki_ipc exports=");
    append_dec64(p, end, ipc.exports);
    append_sconst(p, end, " proxies=");
    append_dec64(p, end, ipc.proxies);
    append_sconst(p, end, " active_pumps=");
    append_dec64(p, end, ipc.pump_tasks);
    append_sconst(p, end, " ring_used=");
    append_dec64(p, end, ipc.proxy_ring_used_bytes);
    append_sconst(p, end, " pending_deliveries=");
    append_dec64(p, end, ipc.pending_deliveries);
    append_sconst(p, end, " pending_bytes=");
    append_dec64(p, end, ipc.pending_bytes);
    append_sconst(p, end, " export_flush_queue=");
    append_dec64(p, end, ipc.export_flush_queue);
    append_sconst(p, end, " dev_op_queue=");
    append_dec64(p, end, ipc.dev_op_queue);
    append_char(p, end, '\n');

    ker::net::wki::WkiRemoteComputeDiagCounts compute_counts{};
    std::array<ker::net::wki::WkiRemoteComputeDiagRow, ker::net::wki::WKI_REMOTE_COMPUTE_DIAG_MAX> compute_rows{};
    size_t const COMPUTE_ROW_COUNT =
        ker::net::wki::wki_remote_compute_diag_snapshot(compute_rows.data(), compute_rows.size(), &compute_counts);
    append_sconst(p, end, "wki_compute submitted=");
    append_dec64(p, end, compute_counts.submitted_total);
    append_sconst(p, end, " submitted_active=");
    append_dec64(p, end, compute_counts.submitted_active);
    append_sconst(p, end, " running=");
    append_dec64(p, end, compute_counts.running_total);
    append_sconst(p, end, " running_active=");
    append_dec64(p, end, compute_counts.running_active);
    append_sconst(p, end, " pending_complete=");
    append_dec64(p, end, compute_counts.pending_completions);
    append_sconst(p, end, " truncated=");
    append_dec64(p, end, compute_counts.truncated);
    append_char(p, end, '\n');
    for (size_t i = 0; i < COMPUTE_ROW_COUNT; ++i) {
        const auto& row = compute_rows.at(i);
        append_sconst(p, end, "wki_compute_task kind=");
        append_sconst(p, end, remote_compute_diag_kind_name(row.kind));
        append_sconst(p, end, " task=");
        append_dec64(p, end, row.task_id);
        append_sconst(p, end, " peer=");
        append_hex16(p, end, row.peer_node);
        append_sconst(p, end, " local_pid=");
        append_dec64(p, end, row.local_pid);
        append_sconst(p, end, " local_task=");
        append_hex64(p, end, row.local_task_ptr);
        append_sconst(p, end, " active=");
        append_bool01(p, end, row.active);
        append_sconst(p, end, " response_pending=");
        append_bool01(p, end, row.response_pending);
        append_sconst(p, end, " complete_pending=");
        append_bool01(p, end, row.complete_pending);
        append_sconst(p, end, " proxy_ready=");
        append_bool01(p, end, row.proxy_ready);
        append_sconst(p, end, " has_local_task=");
        append_bool01(p, end, row.has_local_task);
        append_sconst(p, end, " exit=");
        append_sdec64(p, end, row.exit_status);
        append_sconst(p, end, " accepted_age_us=");
        append_dec64(p, end, row.accepted_age_us);
        append_sconst(p, end, " complete_age_us=");
        append_dec64(p, end, row.complete_age_us);
        append_sconst(p, end, " ipc_fds=");
        append_dec64(p, end, row.ipc_fd_count);
        append_sconst(p, end, " output_len=");
        append_dec64(p, end, row.output_len);
        append_char(p, end, '\n');
    }

    std::array<ker::net::wki::WkiRemoteVfsProxyDiag, ker::net::wki::WKI_REMOTE_VFS_PROXY_DIAG_MAX> vfs_proxies{};
    size_t const VFS_PROXY_COUNT = ker::net::wki::wki_remote_vfs_proxy_diag_snapshot(vfs_proxies.data(), vfs_proxies.size());
    for (size_t i = 0; i < VFS_PROXY_COUNT; ++i) {
        const auto& proxy = vfs_proxies.at(i);
        append_sconst(p, end, "wki_vfs_proxy owner=");
        append_hex16(p, end, proxy.owner_node);
        append_sconst(p, end, " res_id=");
        append_dec64(p, end, proxy.resource_id);
        append_sconst(p, end, " ch=");
        append_dec64(p, end, proxy.assigned_channel);
        append_sconst(p, end, " active=");
        append_bool01(p, end, proxy.active);
        append_sconst(p, end, " op_pending=");
        append_bool01(p, end, proxy.op_pending);
        append_sconst(p, end, " op_id=");
        append_dec64(p, end, proxy.op_expected_id);
        append_sconst(p, end, " op_seq=");
        append_dec64(p, end, proxy.op_expected_seq);
        append_sconst(p, end, " attach_pending=");
        append_bool01(p, end, proxy.attach_pending);
        append_sconst(p, end, " mount=");
        append_sconst(p, end, proxy.local_mount_path.data());
        append_char(p, end, '\n');
    }
    if (VFS_PROXY_COUNT == vfs_proxies.size()) {
        append_sconst(p, end, "wki_vfs_proxy_truncated max=");
        append_dec64(p, end, vfs_proxies.size());
        append_char(p, end, '\n');
    }

    std::array<ker::net::wki::WkiChannelDiag, ker::net::wki::WKI_CHANNEL_DIAG_MAX> channels{};
    size_t const CHANNEL_COUNT = ker::net::wki::wki_channel_diag_snapshot(channels.data(), channels.size());
    for (size_t i = 0; i < CHANNEL_COUNT; ++i) {
        const auto& ch = channels.at(i);
        append_sconst(p, end, "wki_channel peer=");
        append_hex16(p, end, ch.peer_node);
        append_sconst(p, end, " state=");
        append_sconst(p, end, peer_state_name(ch.peer_state));
        append_sconst(p, end, " direct=");
        append_bool01(p, end, ch.peer_direct);
        append_sconst(p, end, " next_hop=");
        append_hex16(p, end, ch.next_hop);
        append_sconst(p, end, " hops=");
        append_dec64(p, end, ch.hop_count);
        append_sconst(p, end, " ch=");
        append_dec64(p, end, ch.channel_id);
        append_sconst(p, end, " priority=");
        append_sconst(p, end, priority_name(ch.priority));
        append_sconst(p, end, " active=");
        append_bool01(p, end, ch.active);
        append_sconst(p, end, " tx_seq=");
        append_dec64(p, end, ch.tx_seq);
        append_sconst(p, end, " tx_ack=");
        append_dec64(p, end, ch.tx_ack);
        append_sconst(p, end, " rx_seq=");
        append_dec64(p, end, ch.rx_seq);
        append_sconst(p, end, " rx_ack=");
        append_dec64(p, end, ch.rx_ack_pending);
        append_sconst(p, end, " ack_pending=");
        append_bool01(p, end, ch.ack_pending);
        append_sconst(p, end, " tx_credits=");
        append_dec64(p, end, ch.tx_credits);
        append_sconst(p, end, " rx_credits=");
        append_dec64(p, end, ch.rx_credits);
        append_sconst(p, end, " retransmit_count=");
        append_dec64(p, end, ch.retransmit_count);
        append_sconst(p, end, " reorder_count=");
        append_dec64(p, end, ch.reorder_count);
        append_sconst(p, end, " retransmits=");
        append_dec64(p, end, ch.retransmits);
        append_sconst(p, end, " bytes_tx=");
        append_dec64(p, end, ch.bytes_sent);
        append_sconst(p, end, " bytes_rx=");
        append_dec64(p, end, ch.bytes_received);
        append_char(p, end, '\n');
    }
    if (CHANNEL_COUNT == channels.size()) {
        append_sconst(p, end, "wki_channel_truncated max=");
        append_dec64(p, end, channels.size());
        append_char(p, end, '\n');
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

void append_perf_callsite(char*& p, char* end, uint64_t callsite) {
    if (callsite == 0) {
        append_sconst(p, end, "?");
        return;
    }

    if (callsite >= 0xffff800000000000ULL && (callsite & (alignof(ker::mod::perf::PerfCallsiteInfo) - 1)) == 0) {
        const auto* info = reinterpret_cast<const ker::mod::perf::PerfCallsiteInfo*>(callsite);
        if (info->magic == ker::mod::perf::PERF_CALLSITE_MAGIC && info->file != nullptr && info->line != 0) {
            const char* base = info->file;
            for (const char* cur = info->file; *cur != '\0'; ++cur) {
                if (*cur == '/') {
                    base = cur + 1;
                }
            }
            append_sconst(p, end, base);
            if (p + 1 < end) {
                *p++ = ':';
            }
            append_dec64(p, end, info->line);
            return;
        }
    }

    append_hex64(p, end, callsite);
}

// Generate content for /proc/kperf
// Drains the kernel perf ring buffer and formats each event as one text line.
// Format:
//   S <ts_ns> <cpu> <pid> <rip_hex>   <lag_v>  <flags>    SAMPLE
//   X <ts_ns> <cpu> <prev_pid> <next_pid> <lag_v> <flags> <run_us> <callsite>   SWITCH
//   W <ts_ns> <cpu> <pid> <wake_at_us> <sleep_us> <flags> <callsite> <wait_channel> WAKE
//   B <ts_ns> <cpu> <pid> <wake_at_us> <run_us>   <flags> <callsite> <wait_channel> SLEEP
//   K <ts_ns> <cpu> <pid> <scope> <op> <phase> <peer> <channel> <corr> <status> <aux> <callsite>
auto generate_kperf(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char* end = buf + bufsz - 1;

    std::array<ker::mod::perf::PerfEvent, 64> batch{};
    size_t n = 0;
    while ((n = ker::mod::perf::drain_events(batch.data(), 64, UINT32_MAX)) > 0) {
        for (size_t i = 0; i < n; ++i) {
            const auto& ev = batch[i];
            // Determine event letter
            char letter = '?';
            switch (static_cast<ker::mod::perf::PerfEventType>(ev.type)) {
                case ker::mod::perf::PerfEventType::SAMPLE:
                    letter = 'S';
                    break;
                case ker::mod::perf::PerfEventType::SWITCH:
                    letter = 'X';
                    break;
                case ker::mod::perf::PerfEventType::WAKE:
                    letter = 'W';
                    break;
                case ker::mod::perf::PerfEventType::SLEEP:
                    letter = 'B';
                    break;
                case ker::mod::perf::PerfEventType::CONTAINER_STAT:
                    letter = 'C';
                    break;
                case ker::mod::perf::PerfEventType::WKI:
                    letter = 'K';
                    break;
            }
            if (p + 2 >= end) {
                break;
            }
            *p++ = letter;
            *p++ = ' ';
            append_dec64(p, end, ev.ts_ns);
            *p++ = ' ';
            append_dec64(p, end, ev.cpu);
            *p++ = ' ';
            // Type-specific fields
            if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::SAMPLE) {
                append_dec64(p, end, ev.pid);
                *p++ = ' ';
                append_hex64(p, end, ev.data);  // RIP
                *p++ = ' ';
                if (ev.lag_v < 0 && p + 1 < end) {
                    *p++ = '-';
                }
                append_dec64(p, end, static_cast<uint64_t>(ev.lag_v >= 0 ? ev.lag_v : -ev.lag_v));
                *p++ = ' ';
                append_dec64(p, end, ev.flags);
            } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::SWITCH) {
                append_dec64(p, end, ev.pid);  // prev
                *p++ = ' ';
                append_dec64(p, end, ev.data);  // next
                *p++ = ' ';
                append_dec64(p, end, static_cast<uint64_t>(ev.lag_v >= 0 ? ev.lag_v : 0U));
                *p++ = ' ';
                append_dec64(p, end, ev.flags);
                *p++ = ' ';
                append_dec64(p, end, ev.aux);
                *p++ = ' ';
                append_perf_callsite(p, end, ev.callsite);
            } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::CONTAINER_STAT) {
                // CONTAINER_STAT: C <ts> <cpu> <pid> <subsys_name> <flags> <count> <capacity> <callsite>
                auto const SUBSYS_ID = static_cast<uint8_t>(ev.data >> 32);
                append_dec64(p, end, ev.pid);
                *p++ = ' ';
                append_sconst(p, end, ker::mod::perf::subsystem_name(static_cast<ker::mod::perf::PerfSubsystem>(SUBSYS_ID)));
                *p++ = ' ';
                append_dec64(p, end, ev.flags);
                *p++ = ' ';
                if (ev.lag_v < 0 && p + 1 < end) {
                    *p++ = '-';
                }
                append_dec64(p, end, static_cast<uint64_t>(ev.lag_v >= 0 ? ev.lag_v : -ev.lag_v));
                *p++ = ' ';
                append_dec64(p, end, ev.aux);
                *p++ = ' ';
                append_perf_callsite(p, end, ev.callsite);
            } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::WKI) {
                ker::mod::perf::WkiPerfScope scope = ker::mod::perf::WkiPerfScope::NONE;
                ker::mod::perf::WkiPerfPhase phase = ker::mod::perf::WkiPerfPhase::POINT;
                uint8_t op = 0;
                uint16_t peer = 0;
                uint16_t channel = 0;
                ker::mod::perf::wki_unpack_event_data(ev.data, scope, op, phase, peer, channel);

                append_dec64(p, end, ev.pid);
                *p++ = ' ';
                append_sconst(p, end, ker::mod::perf::wki_scope_name(scope));
                *p++ = ' ';
                append_sconst(p, end, ker::mod::perf::wki_op_name(scope, op));
                *p++ = ' ';
                append_sconst(p, end, ker::mod::perf::wki_phase_name(phase));
                *p++ = ' ';
                append_dec64(p, end, peer);
                *p++ = ' ';
                append_dec64(p, end, channel);
                *p++ = ' ';
                append_dec64(p, end, ker::mod::perf::wki_unpack_trace_correlation(ev.lag_v));
                *p++ = ' ';
                if (ker::mod::perf::wki_unpack_trace_status(ev.lag_v) < 0 && p + 1 < end) {
                    *p++ = '-';
                }
                append_dec64(p, end,
                             static_cast<uint64_t>(ker::mod::perf::wki_unpack_trace_status(ev.lag_v) < 0
                                                       ? -ker::mod::perf::wki_unpack_trace_status(ev.lag_v)
                                                       : ker::mod::perf::wki_unpack_trace_status(ev.lag_v)));
                *p++ = ' ';
                append_dec64(p, end, ev.aux);
                *p++ = ' ';
                if (scope == ker::mod::perf::WkiPerfScope::LOCAL_VMEM ||
                    (scope == ker::mod::perf::WkiPerfScope::LOCAL_LOADER &&
                     (op == static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalLoaderOp::PT_LOAD_MAIN) ||
                      op == static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalLoaderOp::PT_LOAD_INTERP)))) {
                    append_hex64(p, end, ev.callsite);
                } else {
                    append_perf_callsite(p, end, ev.callsite);
                }
            } else {
                // WAKE or SLEEP
                append_dec64(p, end, ev.pid);
                *p++ = ' ';
                append_dec64(p, end, ev.data);  // wakeAtUs
                *p++ = ' ';
                append_dec64(p, end, ev.aux);
                *p++ = ' ';
                append_dec64(p, end, ev.flags);
                *p++ = ' ';
                append_perf_callsite(p, end, ev.callsite);
                *p++ = ' ';
                auto const* wait_channel = reinterpret_cast<const char*>(static_cast<uint64_t>(ev.lag_v));
                append_sconst(p, end, wait_channel != nullptr ? wait_channel : "-");
            }
            if (p + 1 < end) {
                *p++ = '\n';
            }
        }
        if (n < 64) {
            break;  // ring drained
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Generate content for /proc/kcpustat
// Shows per-CPU aggregate scheduler counters (non-destructive).
auto generate_kcpustat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }
    cpu_count = std::min(cpu_count, ker::mod::perf::get_num_perf_cpus());

    for (uint64_t c = 0; c < cpu_count; ++c) {
        auto s = ker::mod::perf::get_cpu_stats(static_cast<uint32_t>(c));
        auto sched = ker::mod::sched::get_scheduler_trace_stats(c);
        auto mm_destroy = ker::mod::mm::virt::get_destroy_user_space_stats(c);
        append_sconst(p, end, "cpu=");
        append_dec64(p, end, c);
        append_sconst(p, end, " ctx=");
        append_dec64(p, end, s.ctx_switches);
        append_sconst(p, end, " preempt=");
        append_dec64(p, end, s.preemptions);
        append_sconst(p, end, " yield=");
        append_dec64(p, end, s.yields);
        append_sconst(p, end, " sleep=");
        append_dec64(p, end, s.sleeps);
        append_sconst(p, end, " wake=");
        append_dec64(p, end, s.wakes);
        append_sconst(p, end, " sample=");
        append_dec64(p, end, s.samples);
        append_sconst(p, end, " fast_skip=");
        append_dec64(p, end, s.fastpath_skips);
        append_sconst(p, end, " ring_write=");
        append_dec64(p, end, s.ring_writes);
        append_sconst(p, end, " timer_irq=");
        append_dec64(p, end, sched.scheduler_timer_interrupts);
        append_sconst(p, end, " sched_arm=");
        append_dec64(p, end, sched.scheduler_timer_arms);
        append_sconst(p, end, " sched_disarm=");
        append_dec64(p, end, sched.scheduler_timer_disarms);
        append_sconst(p, end, " arm_wait=");
        append_dec64(p, end, sched.scheduler_timer_arm_wait_deadline);
        append_sconst(p, end, " arm_itimer=");
        append_dec64(p, end, sched.scheduler_timer_arm_itimer);
        append_sconst(p, end, " arm_vol=");
        append_dec64(p, end, sched.scheduler_timer_arm_voluntary);
        append_sconst(p, end, " arm_idlework=");
        append_dec64(p, end, sched.scheduler_timer_arm_idle_work);
        append_sconst(p, end, " arm_runq=");
        append_dec64(p, end, sched.scheduler_timer_arm_runqueue);
        append_sconst(p, end, " arm_comp=");
        append_dec64(p, end, sched.scheduler_timer_arm_competitor);
        append_sconst(p, end, " idle_arm=");
        append_dec64(p, end, sched.idle_timer_arms);
        append_sconst(p, end, " idle_disarm=");
        append_dec64(p, end, sched.idle_timer_disarms);
        append_sconst(p, end, " idle_wake=");
        append_dec64(p, end, sched.idle_timer_wakeups);
        append_sconst(p, end, " wake_ipi=");
        append_dec64(p, end, sched.wake_ipis_sent);
        append_sconst(p, end, " wake_coal=");
        append_dec64(p, end, sched.wake_ipis_coalesced);
        append_sconst(p, end, " local_resched=");
        append_dec64(p, end, sched.local_reschedule_requests);
        append_sconst(p, end, " local_poke=");
        append_dec64(p, end, sched.local_reschedule_timer_pokes);
        append_sconst(p, end, " slow_scan=");
        append_dec64(p, end, sched.slow_reschedule_scans);
        append_sconst(p, end, " wait_scan=");
        append_dec64(p, end, sched.wait_list_scan_iterations);
        append_sconst(p, end, " wait_pass=");
        append_dec64(p, end, sched.wait_list_scan_passes);
        append_sconst(p, end, " wait_max=");
        append_dec64(p, end, sched.wait_list_scan_max);
        append_sconst(p, end, " timer_wake=");
        append_dec64(p, end, sched.timer_expired_wakeups);
        append_sconst(p, end, " gc_pass=");
        append_dec64(p, end, sched.gc_passes_triggered);
        append_sconst(p, end, " gc_reclaim=");
        append_dec64(p, end, sched.gc_tasks_reclaimed);
        append_sconst(p, end, " gc_us=");
        append_dec64(p, end, sched.gc_work_us_total);
        append_sconst(p, end, " gc_max_us=");
        append_dec64(p, end, sched.gc_work_us_max);
        append_sconst(p, end, " gc_task_us=");
        append_dec64(p, end, sched.gc_task_us_total);
        append_sconst(p, end, " gc_task_max=");
        append_dec64(p, end, sched.gc_task_us_max);
        append_sconst(p, end, " gc_detach_us=");
        append_dec64(p, end, sched.gc_detach_us_total);
        append_sconst(p, end, " gc_detach_max=");
        append_dec64(p, end, sched.gc_detach_us_max);
        append_sconst(p, end, " gc_pm_us=");
        append_dec64(p, end, sched.gc_pagemap_us_total);
        append_sconst(p, end, " gc_pm_max=");
        append_dec64(p, end, sched.gc_pagemap_us_max);
        append_sconst(p, end, " gc_thr_us=");
        append_dec64(p, end, sched.gc_thread_us_total);
        append_sconst(p, end, " gc_thr_max=");
        append_dec64(p, end, sched.gc_thread_us_max);
        append_sconst(p, end, " gc_misc_us=");
        append_dec64(p, end, sched.gc_misc_us_total);
        append_sconst(p, end, " gc_misc_max=");
        append_dec64(p, end, sched.gc_misc_us_max);
        append_sconst(p, end, " gc_dbg_us=");
        append_dec64(p, end, sched.gc_debug_us_total);
        append_sconst(p, end, " gc_dbg_max=");
        append_dec64(p, end, sched.gc_debug_us_max);
        append_sconst(p, end, " dus_calls=");
        append_dec64(p, end, mm_destroy.calls);
        append_sconst(p, end, " dus_collect_us=");
        append_dec64(p, end, mm_destroy.collect_frames_us_total);
        append_sconst(p, end, " dus_collect_max=");
        append_dec64(p, end, mm_destroy.collect_frames_us_max);
        append_sconst(p, end, " dus_data_us=");
        append_dec64(p, end, mm_destroy.free_data_us_total);
        append_sconst(p, end, " dus_data_max=");
        append_dec64(p, end, mm_destroy.free_data_us_max);
        append_sconst(p, end, " dus_pt_us=");
        append_dec64(p, end, mm_destroy.free_pt_us_total);
        append_sconst(p, end, " dus_pt_max=");
        append_dec64(p, end, mm_destroy.free_pt_us_max);
        append_sconst(p, end, " dus_tlb_us=");
        append_dec64(p, end, mm_destroy.tlb_flush_us_total);
        append_sconst(p, end, " dus_tlb_max=");
        append_dec64(p, end, mm_destroy.tlb_flush_us_max);
        append_sconst(p, end, " dus_leaf=");
        append_dec64(p, end, mm_destroy.data_leaf_entries_visited);
        append_sconst(p, end, " dus_refdec=");
        append_dec64(p, end, mm_destroy.data_pages_ref_decremented);
        append_sconst(p, end, " dus_freed=");
        append_dec64(p, end, mm_destroy.data_pages_freed);
        append_sconst(p, end, " dus_ptfree=");
        append_dec64(p, end, mm_destroy.page_table_pages_freed);
        append_sconst(p, end, " dus_huge_skip=");
        append_dec64(p, end, mm_destroy.skipped_huge_pages);
        append_sconst(p, end, " dus_unknown_skip=");
        append_dec64(p, end, mm_destroy.skipped_unknown_frames);
        append_sconst(p, end, " dus_slab_skip=");
        append_dec64(p, end, mm_destroy.skipped_slab_alloc_frames);
        append_sconst(p, end, " dus_medium_skip=");
        append_dec64(p, end, mm_destroy.skipped_medium_alloc_frames);
        append_sconst(p, end, " dus_kmalloc_large_skip=");
        append_dec64(p, end, mm_destroy.skipped_kmalloc_large_alloc_frames);
        append_sconst(p, end, " dus_alias_skip=");
        append_dec64(p, end, mm_destroy.skipped_page_table_aliases);
        append_sconst(p, end, " dus_corrupt=");
        append_dec64(p, end, mm_destroy.skipped_corrupt_entries);
        append_sconst(p, end, " dus_corrupt_skip=");
        append_dec64(p, end, mm_destroy.skipped_corrupt_entries);
        append_sconst(p, end, " lb_push=");
        append_dec64(p, end, sched.load_balance_pushes);
        if (p + 1 < end) {
            *p++ = '\n';
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Generate content for /proc/version
auto generate_version(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };

    append(ker::release::NAME);
    append(" version ");
    append(ker::release::VERSION);
    append(" (");
    append(ker::release::BUILDER);
    append(") (");
    append(ker::release::COMPILER);
    append(") #1 ");
    append(ker::release::SMP);
    append("\n");

    buf[off] = '\0';
    return off;
}

// Procfs stores generated content in the File's private_data as ProcFileData.

// --- FileOperations ---

auto procfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);

    // Lazily generate content
    if (pfd->content == nullptr) {
        constexpr size_t MAX_PROCFS_BUF = 4096;
        constexpr size_t MAX_KPERF_BUF = 65536;  // 64 KiB for event streams
        constexpr size_t MAX_MEMACC_BUF = 262144;
        bool const IS_KPERF = (pfd->node.type == ProcNodeType::KPERF_FILE || pfd->node.type == ProcNodeType::KWKISTAT_FILE ||
                               pfd->node.type == ProcNodeType::KCPUSTAT_FILE || pfd->node.type == ProcNodeType::KCONTSTAT_FILE ||
                               pfd->node.type == ProcNodeType::KIPCSTAT_FILE || pfd->node.type == ProcNodeType::WKI_NETDIAG_FILE ||
                               pfd->node.type == ProcNodeType::CPU_STAT_FILE);
        bool const IS_MEMACC =
            (pfd->node.type == ProcNodeType::MEMACC_SUMMARY_FILE || pfd->node.type == ProcNodeType::MEMACC_ZONES_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_PROCS_FILE || pfd->node.type == ProcNodeType::MEMACC_DEAD_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_ALLOC_TOTALS_FILE || pfd->node.type == ProcNodeType::MEMACC_SLABS_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_KMALLOC_LIVE_FILE || pfd->node.type == ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_PAGE_CALLERS_FILE || pfd->node.type == ProcNodeType::MEMACC_FEATURES_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE);
        size_t alloc_sz = MAX_PROCFS_BUF;
        if (IS_MEMACC) {
            alloc_sz = MAX_MEMACC_BUF;
        } else if (IS_KPERF) {
            alloc_sz = MAX_KPERF_BUF;
        }
        pfd->content = new (std::nothrow) char[alloc_sz];
        if (pfd->content == nullptr) {
            return -ENOMEM;
        }

        switch (pfd->node.type) {
            case ProcNodeType::STATUS_FILE:
                pfd->content_len = generate_status(pfd->node.pid, pfd->content, MAX_PROCFS_BUF, pfd->node.thread_view);
                break;
            case ProcNodeType::STAT_FILE:
                pfd->content_len = generate_stat(pfd->node.pid, pfd->content, MAX_PROCFS_BUF, pfd->node.thread_view);
                break;
            case ProcNodeType::STATM_FILE:
                pfd->content_len = generate_statm(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::CMDLINE_FILE:
                pfd->content_len = generate_cmdline(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MOUNTS_FILE:
                pfd->content_len = generate_mounts(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::UPTIME_FILE:
                pfd->content_len = generate_uptime(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::CPU_STAT_FILE:
                pfd->content_len = generate_cpu_stat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::LOADAVG_FILE:
                pfd->content_len = generate_loadavg(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMINFO_FILE:
                pfd->content_len = generate_meminfo(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::VERSION_FILE:
                pfd->content_len = generate_version(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::KPERF_FILE:
                pfd->content_len = generate_kperf(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KWKISTAT_FILE:
                pfd->content_len = generate_kwkistat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KCPUSTAT_FILE:
                pfd->content_len = generate_kcpustat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KPERFCTL_FILE:
                pfd->content_len = generate_kperfctl(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::KCONTSTAT_FILE:
                pfd->content_len = generate_kcontstat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KIPCSTAT_FILE:
                pfd->content_len = generate_kipcstat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::WKI_LAUNCHER_FILE:
                pfd->content_len = generate_wki_launcher(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::WKI_RUNNER_FILE:
                pfd->content_len = generate_wki_runner(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::WKI_REMOTE_PID_FILE:
                pfd->content_len = generate_wki_remote_pid(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::WKI_PEERS_FILE:
                pfd->content_len = generate_wki_peers(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::WKI_NETDIAG_FILE:
                pfd->content_len = generate_wki_netdiag(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::MEMACC_SUMMARY_FILE:
                pfd->content_len = generate_memacc_summary(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_ZONES_FILE:
                pfd->content_len = generate_memacc_zones(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_PROCS_FILE:
                pfd->content_len = generate_memacc_procs(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_DEAD_FILE:
                pfd->content_len = generate_memacc_dead(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_ALLOC_TOTALS_FILE:
                pfd->content_len = generate_memacc_alloc_totals(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_SLABS_FILE:
                pfd->content_len = generate_memacc_slabs(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_KMALLOC_LIVE_FILE:
                pfd->content_len = generate_memacc_kmalloc_live(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE:
                pfd->content_len = generate_memacc_kmalloc_callers(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_PAGE_CALLERS_FILE:
                pfd->content_len = generate_memacc_page_callers(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_FEATURES_FILE:
                pfd->content_len = generate_memacc_features(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE:
            case ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE:
                pfd->content_len = generate_memacc_track_state(pfd->node.type, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE:
                pfd->content_len = generate_memacc_reclaim_buffer_cache(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE:
                pfd->content_len = generate_memacc_reclaim_packet_pool(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::EXE_LINK: {
                auto* task = ker::mod::sched::find_task_by_pid_safe(pfd->node.pid);
                if (task != nullptr && task->exe_path[0] != '\0') {
                    size_t const LEN = strlen(task->exe_path.data());
                    memcpy(pfd->content, task->exe_path.data(), LEN);
                    pfd->content[LEN] = '\0';
                    pfd->content_len = LEN;
                } else {
                    pfd->content[0] = '\0';
                    pfd->content_len = 0;
                }
                if (task != nullptr) {
                    task->release();
                }
                break;
            }
            default:
                pfd->content[0] = '\0';
                pfd->content_len = 0;
                break;
        }
    }

    auto requires_live_task_content = [](ProcNodeType type) -> bool {
        switch (type) {
            case ProcNodeType::STATUS_FILE:
            case ProcNodeType::STAT_FILE:
            case ProcNodeType::STATM_FILE:
                return true;
            default:
                return false;
        }
    };
    if (pfd->content_len == 0 && requires_live_task_content(pfd->node.type)) {
        delete[] pfd->content;
        pfd->content = nullptr;
        return -ESRCH;
    }

    if (offset >= pfd->content_len) {
        return 0;
    }
    size_t const AVAIL = pfd->content_len - offset;
    count = std::min(count, AVAIL);
    memcpy(buf, pfd->content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_ascii_space(char c) -> bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; }

auto procfs_command_equals(const char* s, size_t count, const char* expected) -> bool {
    while (count > 0 && procfs_ascii_space(*s)) {
        s++;
        count--;
    }
    while (count > 0 && procfs_ascii_space(s[count - 1])) {
        count--;
    }

    size_t expected_len = 0;
    while (expected[expected_len] != '\0') {
        expected_len++;
    }
    if (count != expected_len) {
        return false;
    }
    for (size_t i = 0; i < count; ++i) {
        char lhs = s[i];
        char rhs = expected[i];
        if (lhs >= 'A' && lhs <= 'Z') {
            lhs = static_cast<char>(lhs - 'A' + 'a');
        }
        if (rhs >= 'A' && rhs <= 'Z') {
            rhs = static_cast<char>(rhs - 'A' + 'a');
        }
        if (lhs != rhs) {
            return false;
        }
    }
    return true;
}

auto procfs_parse_u64_trimmed(const char* s, size_t count, uint64_t& out) -> bool {
    while (count > 0 && procfs_ascii_space(*s)) {
        s++;
        count--;
    }
    while (count > 0 && procfs_ascii_space(s[count - 1])) {
        count--;
    }
    if (count == 0) {
        return false;
    }

    uint64_t value = 0;
    for (size_t i = 0; i < count; ++i) {
        char const CH = s[i];
        if (CH < '0' || CH > '9') {
            return false;
        }
        auto const DIGIT = static_cast<uint64_t>(CH - '0');
        if (value > (UINT64_MAX - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
    }
    out = value;
    return true;
}

auto procfs_write_memacc_track(ProcNodeType type, const char* s, size_t count) -> ssize_t {
    bool const PAGE_CALLERS = type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE;
    bool const AVAILABLE =
        PAGE_CALLERS ? ker::mod::mm::phys::page_caller_stats_available() : ker::mod::mm::dyn::kmalloc::debug_info_available();
    if (!AVAILABLE) {
        return -ENOTSUP;
    }

    if (procfs_command_equals(s, count, "on") || procfs_command_equals(s, count, "1")) {
        if (PAGE_CALLERS) {
            ker::mod::mm::phys::page_caller_stats_reset();
            ker::mod::mm::phys::page_caller_stats_set_enabled(true);
        } else {
            ker::mod::mm::dyn::kmalloc::debug_info_reset();
            ker::mod::mm::dyn::kmalloc::debug_info_set_enabled(true);
        }
    } else if (procfs_command_equals(s, count, "off") || procfs_command_equals(s, count, "0")) {
        if (PAGE_CALLERS) {
            ker::mod::mm::phys::page_caller_stats_set_enabled(false);
        } else {
            ker::mod::mm::dyn::kmalloc::debug_info_set_enabled(false);
        }
    } else if (procfs_command_equals(s, count, "reset")) {
        if (PAGE_CALLERS) {
            ker::mod::mm::phys::page_caller_stats_reset();
        } else {
            ker::mod::mm::dyn::kmalloc::debug_info_reset();
        }
    } else if (procfs_command_equals(s, count, "status")) {
        // No-op convenience for scripts that use one path for reads and writes.
    } else {
        return -EINVAL;
    }

    return static_cast<ssize_t>(count);
}

auto procfs_write_memacc_reclaim_buffer_cache(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    uint64_t target_bytes = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all") || procfs_command_equals(s, count, "reset")) {
        target_bytes = 0;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, target_bytes)) {
        return -EINVAL;
    }

    if (target_bytes > static_cast<uint64_t>(SIZE_MAX)) {
        return -EOVERFLOW;
    }
    ker::vfs::reclaim_clean_buffer_cache(static_cast<size_t>(target_bytes));
    return static_cast<ssize_t>(count);
}

auto procfs_write_memacc_reclaim_packet_pool(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    uint64_t target_capacity = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all") || procfs_command_equals(s, count, "reset")) {
        target_capacity = 0;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, target_capacity)) {
        return -EINVAL;
    }

    if (target_capacity > static_cast<uint64_t>(SIZE_MAX)) {
        return -EOVERFLOW;
    }
    ker::net::pkt_pool_reclaim_free(static_cast<size_t>(target_capacity));
    return static_cast<ssize_t>(count);
}

auto procfs_write(File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    if (pfd->node.type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE || pfd->node.type == ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE) {
        return procfs_write_memacc_track(pfd->node.type, static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE) {
        return procfs_write_memacc_reclaim_buffer_cache(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE) {
        return procfs_write_memacc_reclaim_packet_pool(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type != ProcNodeType::KPERFCTL_FILE) {
        return -EPERM;
    }
    if (count == 0) {
        return 0;
    }

    // Accept "enable", "enable <filter>", "mask <filter>", or "disable"
    const char* s = static_cast<const char*>(buf);
    if (count >= 6 && memcmp(s, "enable", 6) == 0) {
        // Check for optional filter: "enable switch,wake,container"
        if (count > 7 && s[6] == ' ') {
            uint16_t const MASK = ker::mod::perf::parse_event_mask(s + 7, count - 7);
            if (MASK == 0) {
                return -EINVAL;
            }
            ker::mod::perf::set_event_mask(MASK);
        } else {
            ker::mod::perf::set_event_mask(ker::mod::perf::PERF_MASK_ALL);
        }
        // Reset ring buffers on fresh enable so report only sees the new session
        ker::mod::perf::reset_rings();
        ker::vfs::vfs_reset_local_pipe_perf_counters();
        ker::mod::perf::enable();
    } else if (count >= 4 && memcmp(s, "mask", 4) == 0) {
        // Change mask without resetting rings or toggling enable: "mask switch,container"
        if (count > 5 && s[4] == ' ') {
            uint16_t const MASK = ker::mod::perf::parse_event_mask(s + 5, count - 5);
            if (MASK == 0) {
                return -EINVAL;
            }
            ker::mod::perf::set_event_mask(MASK);
        } else {
            return -EINVAL;
        }
    } else if (count >= 7 && memcmp(s, "disable", 7) == 0) {
        ker::mod::perf::disable();
    } else {
        return -EINVAL;
    }
    return static_cast<ssize_t>(count);
}

auto procfs_close(File* f) -> int {
    if (f == nullptr) {
        return -EINVAL;
    }
    if (f->private_data != nullptr) {
        auto* pfd = static_cast<ProcFileData*>(f->private_data);
        if (pfd->content != nullptr) {
            delete[] pfd->content;
            pfd->content = nullptr;
        }
        delete pfd;
        f->private_data = nullptr;
    }
    return 0;
}

auto procfs_lseek(File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    off_t new_pos = 0;
    switch (whence) {
        case 0:
            new_pos = offset;
            break;  // SEEK_SET
        case 1:
            new_pos = f->pos + offset;
            break;  // SEEK_CUR
        case 2:
            new_pos = static_cast<off_t>(pfd->content_len) + offset;
            break;  // SEEK_END
        default:
            return -EINVAL;
    }
    if (new_pos < 0) {
        return -EINVAL;
    }
    f->pos = new_pos;
    return new_pos;
}

auto procfs_readlink(File* f, char* buf, size_t bufsz) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    if (pfd->node.type != ProcNodeType::EXE_LINK && pfd->node.type != ProcNodeType::SELF_LINK) {
        return -EINVAL;
    }

    if (pfd->node.type == ProcNodeType::SELF_LINK) {
        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return -ESRCH;
        }
        std::array<char, 24> tmp{};
        int_to_str(ker::mod::sched::task::process_pid(*task), tmp.data(), tmp.size());
        std::array<char, 64> link{};
        memcpy(link.data(), "/proc/", 6);
        size_t off = 6;
        for (const char* p = tmp.data(); (*p != 0) && off < link.size() - 1; ++p, ++off) {
            link[off] = *p;
        }
        link[off] = '\0';
        size_t len = off;
        len = std::min(len, bufsz);
        memcpy(buf, link.data(), len);
        return static_cast<ssize_t>(len);
    }

    // EXE_LINK
    auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
    if (task == nullptr) {
        return -ESRCH;
    }
    size_t len = strlen(task->exe_path.data());
    len = std::min(len, bufsz);
    memcpy(buf, task->exe_path.data(), len);
    return static_cast<ssize_t>(len);
}

FileOperations procfs_fops_instance = {
    .vfs_open = nullptr,
    .vfs_close = procfs_close,
    .vfs_read = procfs_read,
    .vfs_write = procfs_write,
    .vfs_lseek = procfs_lseek,
    .vfs_isatty = nullptr,
    .vfs_readdir = procfs_readdir,
    .vfs_readlink = procfs_readlink,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

}  // namespace

auto get_procfs_fops() -> FileOperations* { return &procfs_fops_instance; }

namespace {

auto task_start_epoch_ns(const ker::mod::sched::task::Task* task) -> uint64_t {
    if (task == nullptr || task->start_time_us == 0) {
        return 0;
    }

    uint64_t const NOW_US = ker::mod::time::get_us();
    uint64_t const NOW_EPOCH_NS = ker::mod::time::get_epoch_ns();
    if (task->start_time_us >= NOW_US) {
        return NOW_EPOCH_NS;
    }

    uint64_t const AGE_NS = (NOW_US - task->start_time_us) * NS_PER_US;
    return (NOW_EPOCH_NS > AGE_NS) ? (NOW_EPOCH_NS - AGE_NS) : NOW_EPOCH_NS;
}

auto procfs_node_creation_epoch_ns(const ker::mod::sched::task::Task* owner_task) -> uint64_t {
    uint64_t const TASK_EPOCH_NS = task_start_epoch_ns(owner_task);
    if (TASK_EPOCH_NS != 0) {
        return TASK_EPOCH_NS;
    }

    if (g_procfs_creation_epoch_ns != 0) {
        return g_procfs_creation_epoch_ns;
    }

    return ker::mod::time::get_epoch_ns();
}

void set_stat_timestamps(Stat* statbuf, uint64_t epoch_ns) {
    if (epoch_ns == 0) {
        epoch_ns = ker::mod::time::get_epoch_ns();
    }

    Timespec const TS{
        .tv_sec = static_cast<int64_t>(epoch_ns / NS_PER_SEC),
        .tv_nsec = static_cast<int64_t>(epoch_ns % NS_PER_SEC),
    };
    statbuf->st_atim = TS;
    statbuf->st_mtim = TS;
    statbuf->st_ctim = TS;
}

}  // namespace

auto procfs_fill_stat(File* f, Stat* statbuf, dev_t dev_id) -> int {
    if (f == nullptr || statbuf == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }

    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    auto* owner_task = static_cast<ker::mod::sched::task::Task*>(nullptr);
    if (pfd->node.pid != 0) {
        owner_task = ker::mod::sched::find_task_by_pid_safe(pfd->node.pid);
    }

    std::memset(statbuf, 0, sizeof(Stat));
    statbuf->st_dev = dev_id;
    statbuf->st_ino = 1;
    statbuf->st_nlink = 1;
    statbuf->st_uid = (owner_task != nullptr) ? owner_task->uid : 0;
    statbuf->st_gid = (owner_task != nullptr) ? owner_task->gid : 0;
    statbuf->st_rdev = 0;
    statbuf->st_size = 0;
    statbuf->st_blksize = 4096;
    statbuf->st_blocks = 0;

    if (f->is_directory) {
        statbuf->st_mode = S_IFDIR | 0555;
    } else if (pfd->node.type == ProcNodeType::EXE_LINK || pfd->node.type == ProcNodeType::SELF_LINK) {
        statbuf->st_mode = S_IFLNK | 0777;
    } else if (pfd->node.type == ProcNodeType::KPERFCTL_FILE || pfd->node.type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE) {
        statbuf->st_mode = S_IFREG | 0644;
    } else {
        statbuf->st_mode = S_IFREG | 0444;
    }

    set_stat_timestamps(statbuf, procfs_node_creation_epoch_ns(owner_task));
    if (owner_task != nullptr) {
        owner_task->release();
    }
    return 0;
}

// --- Open a procfs path ---

auto procfs_open_path(const char* path, int flags, int mode) -> File* {
    (void)flags;
    (void)mode;

    if (path == nullptr) {
        return nullptr;
    }

    // Skip leading /
    while (*path == '/') {
        path++;
    }

    auto* task = ker::mod::sched::get_current_task();
    uint64_t const SELF_PID = (task != nullptr) ? ker::mod::sched::task::process_pid(*task) : 0;

    auto make_file = [](ProcNodeType type, uint64_t pid, bool is_dir, bool thread_view = false) -> File* {
        auto* pfd = new ProcFileData;
        pfd->node.type = type;
        pfd->node.pid = pid;
        pfd->node.thread_view = thread_view;
        pfd->content = nullptr;
        pfd->content_len = 0;

        auto* f = new File;
        f->private_data = pfd;
        f->fd = -1;
        f->pos = 0;
        f->is_directory = is_dir;
        f->fs_type = FSType::PROCFS;
        f->refcount = 1;
        f->fops = nullptr;  // set by caller
        return f;
    };
    auto pid_exists = [](uint64_t pid) -> bool {
        auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
        if (task == nullptr) {
            return false;
        }
        task->release();
        return true;
    };
    auto open_task_subpath = [&](uint64_t group_pid, const char* subpath) -> File* {
        if (subpath == nullptr || *subpath == '\0') {
            return make_file(ProcNodeType::TASK_DIR, group_pid, true);
        }

        const char* slash = nullptr;
        for (const char* p = subpath; *p != 0; ++p) {
            if (*p == '/') {
                slash = p;
                break;
            }
        }

        std::array<char, 24> tid_str{};
        size_t const TID_LEN = (slash == nullptr) ? std::strlen(subpath) : static_cast<size_t>(slash - subpath);
        if (TID_LEN == 0 || TID_LEN >= tid_str.size()) {
            return nullptr;
        }
        memcpy(tid_str.data(), subpath, TID_LEN);
        tid_str[TID_LEN] = '\0';
        int64_t const TID = parse_pid(tid_str.data());
        if (TID < 0) {
            return nullptr;
        }

        auto const TID_VALUE = static_cast<uint64_t>(TID);
        auto* target = find_task_in_group_safe(group_pid, TID_VALUE);
        if (target == nullptr) {
            return nullptr;
        }
        target->release();

        if (slash == nullptr) {
            return make_file(ProcNodeType::TASK_TID_DIR, TID_VALUE, true);
        }

        const char* task_sub = slash + 1;
        if (strcmp(task_sub, "exe") == 0) {
            return make_file(ProcNodeType::EXE_LINK, TID_VALUE, false);
        }
        if (strcmp(task_sub, "status") == 0) {
            return make_file(ProcNodeType::STATUS_FILE, TID_VALUE, false, true);
        }
        if (strcmp(task_sub, "stat") == 0) {
            return make_file(ProcNodeType::STAT_FILE, TID_VALUE, false, true);
        }
        if (strcmp(task_sub, "statm") == 0) {
            return make_file(ProcNodeType::STATM_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "cmdline") == 0) {
            return make_file(ProcNodeType::CMDLINE_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "wki_launcher") == 0) {
            return make_file(ProcNodeType::WKI_LAUNCHER_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "wki_runner") == 0) {
            return make_file(ProcNodeType::WKI_RUNNER_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "wki_remote_pid") == 0) {
            return make_file(ProcNodeType::WKI_REMOTE_PID_FILE, TID_VALUE, false);
        }
        return nullptr;
    };

    // /proc (root)
    if (*path == '\0') {
        return make_file(ProcNodeType::ROOT_DIR, 0, true);
    }

    // /proc/mounts
    if (strcmp(path, "mounts") == 0) {
        return make_file(ProcNodeType::MOUNTS_FILE, 0, false);
    }

    // /proc/uptime
    if (strcmp(path, "uptime") == 0) {
        return make_file(ProcNodeType::UPTIME_FILE, 0, false);
    }

    // /proc/stat
    if (strcmp(path, "stat") == 0) {
        return make_file(ProcNodeType::CPU_STAT_FILE, 0, false);
    }

    // /proc/loadavg
    if (strcmp(path, "loadavg") == 0) {
        return make_file(ProcNodeType::LOADAVG_FILE, 0, false);
    }

    // /proc/meminfo
    if (strcmp(path, "meminfo") == 0) {
        return make_file(ProcNodeType::MEMINFO_FILE, 0, false);
    }

    // /proc/version
    if (strcmp(path, "version") == 0) {
        return make_file(ProcNodeType::VERSION_FILE, 0, false);
    }

    // /proc/kperf - drain kernel perf ring buffer as text events
    if (strcmp(path, "kperf") == 0) {
        return make_file(ProcNodeType::KPERF_FILE, 0, false);
    }

    // /proc/kwkistat - recording-scoped WKI summary statistics
    if (strcmp(path, "kwkistat") == 0) {
        return make_file(ProcNodeType::KWKISTAT_FILE, 0, false);
    }

    // /proc/kcpustat - per-CPU aggregate scheduler statistics
    if (strcmp(path, "kcpustat") == 0) {
        return make_file(ProcNodeType::KCPUSTAT_FILE, 0, false);
    }

    // /proc/kperfctl - write "enable"/"disable" to start/stop perf recording
    if (strcmp(path, "kperfctl") == 0) {
        return make_file(ProcNodeType::KPERFCTL_FILE, 0, false);
    }

    // /proc/kcontstat - per-subsystem container statistics
    if (strcmp(path, "kcontstat") == 0) {
        return make_file(ProcNodeType::KCONTSTAT_FILE, 0, false);
    }

    // /proc/kipcstat - WKI IPC memory/queue snapshot
    if (strcmp(path, "kipcstat") == 0) {
        return make_file(ProcNodeType::KIPCSTAT_FILE, 0, false);
    }

    // /proc/wki
    if (strcmp(path, "wki") == 0) {
        return make_file(ProcNodeType::WKI_DIR, 0, true);
    }

    // /proc/wki/peers
    if (strcmp(path, "wki/peers") == 0) {
        return make_file(ProcNodeType::WKI_PEERS_FILE, 0, false);
    }

    // /proc/wki/netdiag
    if (strcmp(path, "wki/netdiag") == 0) {
        return make_file(ProcNodeType::WKI_NETDIAG_FILE, 0, false);
    }

    // /proc/memacc structured memory accounting diagnostics
    if (strcmp(path, "memacc") == 0) {
        return make_file(ProcNodeType::MEMACC_DIR, 0, true);
    }
    if (strcmp(path, "memacc/summary") == 0) {
        return make_file(ProcNodeType::MEMACC_SUMMARY_FILE, 0, false);
    }
    if (strcmp(path, "memacc/zones") == 0) {
        return make_file(ProcNodeType::MEMACC_ZONES_FILE, 0, false);
    }
    if (strcmp(path, "memacc/procs") == 0) {
        return make_file(ProcNodeType::MEMACC_PROCS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/dead") == 0) {
        return make_file(ProcNodeType::MEMACC_DEAD_FILE, 0, false);
    }
    if (strcmp(path, "memacc/alloc_totals") == 0) {
        return make_file(ProcNodeType::MEMACC_ALLOC_TOTALS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/slabs") == 0) {
        return make_file(ProcNodeType::MEMACC_SLABS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/kmalloc_live") == 0) {
        return make_file(ProcNodeType::MEMACC_KMALLOC_LIVE_FILE, 0, false);
    }
    if (strcmp(path, "memacc/kmalloc_callers") == 0) {
        return make_file(ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/page_callers") == 0) {
        return make_file(ProcNodeType::MEMACC_PAGE_CALLERS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/features") == 0) {
        return make_file(ProcNodeType::MEMACC_FEATURES_FILE, 0, false);
    }
    if (strcmp(path, "memacc/track") == 0) {
        return make_file(ProcNodeType::MEMACC_TRACK_DIR, 0, true);
    }
    if (strcmp(path, "memacc/reclaim") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_DIR, 0, true);
    }
    if (strcmp(path, "memacc/track/page_callers") == 0) {
        return make_file(ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/track/kmalloc_debug") == 0) {
        return make_file(ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/buffer_cache") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/packet_pool") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE, 0, false);
    }

    // /proc/self -> symlink to /proc/<pid>
    if (strcmp(path, "self") == 0) {
        return make_file(ProcNodeType::SELF_LINK, SELF_PID, false);
    }
    // /proc/self/exe
    if (strcmp(path, "self/exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, SELF_PID, false);
    }
    // /proc/self/status
    if (strcmp(path, "self/status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, SELF_PID, false);
    }
    // /proc/self/stat
    if (strcmp(path, "self/stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, SELF_PID, false);
    }
    // /proc/self/statm
    if (strcmp(path, "self/statm") == 0) {
        return make_file(ProcNodeType::STATM_FILE, SELF_PID, false);
    }
    // /proc/self/cmdline
    if (strcmp(path, "self/cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, SELF_PID, false);
    }
    // /proc/self/wki_launcher
    if (strcmp(path, "self/wki_launcher") == 0) {
        return make_file(ProcNodeType::WKI_LAUNCHER_FILE, SELF_PID, false);
    }
    // /proc/self/wki_runner
    if (strcmp(path, "self/wki_runner") == 0) {
        return make_file(ProcNodeType::WKI_RUNNER_FILE, SELF_PID, false);
    }
    // /proc/self/wki_remote_pid
    if (strcmp(path, "self/wki_remote_pid") == 0) {
        return make_file(ProcNodeType::WKI_REMOTE_PID_FILE, SELF_PID, false);
    }
    // /proc/self/task[/<tid>/...]
    if (strcmp(path, "self/task") == 0) {
        return open_task_subpath(SELF_PID, "");
    }
    if (strncmp(path, "self/task/", 10) == 0) {
        return open_task_subpath(SELF_PID, path + 10);
    }

    // /proc/<pid>
    // Find first / in path
    const char* slash = nullptr;
    for (const char* p = path; *p != 0; ++p) {
        if (*p == '/') {
            slash = p;
            break;
        }
    }

    if (slash == nullptr) {
        // Just /proc/<pid>
        int64_t const PID = parse_pid(path);
        if (PID < 0) {
            return nullptr;
        }
        // Check if task exists
        if (!pid_exists(static_cast<uint64_t>(PID))) {
            return nullptr;
        }
        return make_file(ProcNodeType::PID_DIR, static_cast<uint64_t>(PID), true);
    }

    // /proc/<pid>/<subpath>
    std::array<char, 24> pid_str{};
    auto pid_len = static_cast<size_t>(slash - path);
    if (pid_len >= pid_str.size()) {
        return nullptr;
    }
    memcpy(pid_str.data(), path, pid_len);
    pid_str[pid_len] = '\0';
    int64_t const PID = parse_pid(pid_str.data());
    if (PID < 0) {
        return nullptr;
    }
    if (!pid_exists(static_cast<uint64_t>(PID))) {
        return nullptr;
    }

    const char* sub = slash + 1;
    uint64_t group_pid = 0;
    if (!task_group_pid_for(static_cast<uint64_t>(PID), group_pid)) {
        return nullptr;
    }
    if (strcmp(sub, "task") == 0) {
        return open_task_subpath(group_pid, "");
    }
    if (strncmp(sub, "task/", 5) == 0) {
        return open_task_subpath(group_pid, sub + 5);
    }
    if (strcmp(sub, "exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "statm") == 0) {
        return make_file(ProcNodeType::STATM_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "wki_launcher") == 0) {
        return make_file(ProcNodeType::WKI_LAUNCHER_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "wki_runner") == 0) {
        return make_file(ProcNodeType::WKI_RUNNER_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "wki_remote_pid") == 0) {
        return make_file(ProcNodeType::WKI_REMOTE_PID_FILE, static_cast<uint64_t>(PID), false);
    }

    return nullptr;  // Unknown procfs path
}

void procfs_init() {
    if (g_procfs_creation_epoch_ns == 0) {
        g_procfs_creation_epoch_ns = ker::mod::time::get_epoch_ns();
    }

    // Mount procfs at /proc
    ker::vfs::mount_filesystem("/proc", "procfs", nullptr);
}

}  // namespace ker::vfs::procfs
