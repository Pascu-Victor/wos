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
#include <net/backlog.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/tcp.hpp>
#include <new>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "net/wki/remote_compute.hpp"
#include "net/wki/remote_ipc.hpp"
#include "net/wki/remote_vfs.hpp"
#include "net/wki/wire.hpp"
#include "net/wki/wki.hpp"
#include "platform/sched/scheduler.hpp"
#include "release.hpp"
#include "vfs/file_operations.hpp"

namespace ker::vfs::procfs {

namespace {

constexpr uint64_t NS_PER_SEC = 1000000000ULL;
constexpr uint64_t NS_PER_US = 1000ULL;

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
        // index 2 = "self", 3 = "mounts", 4+ = active task PIDs
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
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "wki", 4);
            return 0;
        }
        // PID directories from active task list
        size_t const PID_INDEX = count - 7;
        uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
        if (PID_INDEX >= TASK_COUNT) {
            return -ENOENT;  // No more entries
        }
        auto* task = ker::mod::sched::get_active_task_at(static_cast<uint32_t>(PID_INDEX));
        if (task == nullptr) {
            return -ENOENT;
        }
        buf->d_ino = task->pid + 100;
        buf->d_off = count + 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        int_to_str(task->pid, buf->d_name.data(), buf->d_name.size());
        return 0;
    }

    if (pfd->node.type == ProcNodeType::PID_DIR) {
        // /proc/<pid>: index 2 = "stat", 3 = "status", 4 = "cmdline",
        // 5 = "exe", 6 = "wki_launcher", 7 = "wki_runner", 8 = "wki_remote_pid"
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
            std::memcpy(buf->d_name.data(), "cmdline", 8);
            return 0;
        }
        if (count == 5) {
            buf->d_ino = 13;
            buf->d_off = 6;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "exe", 4);
            return 0;
        }
        if (count == 6) {
            buf->d_ino = 14;
            buf->d_off = 7;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_launcher", 13);
            return 0;
        }
        if (count == 7) {
            buf->d_ino = 15;
            buf->d_off = 8;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_runner", 11);
            return 0;
        }
        if (count == 8) {
            buf->d_ino = 16;
            buf->d_off = 9;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_remote_pid", 15);
            return 0;
        }
        return -ENOENT;  // No more entries
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
auto generate_status(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    if (task == nullptr) {
        return 0;
    }

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

    // State
    append("\nState:\t");
    auto ts_st = task->state.load(std::memory_order_acquire);
    if (ts_st == ker::mod::sched::task::TaskState::DEAD || ts_st == ker::mod::sched::task::TaskState::EXITING || task->has_exited) {
        append("Z (zombie)");
    } else if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::RUNNABLE) {
        append("R (running)");
    } else if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING) {
        append(task->wait_channel != nullptr ? "D (blocked)" : "S (sleeping)");
    } else {
        append("S (sleeping)");
    }

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
    append(task->voluntary_block ? "1" : "0");
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
    append_int(task->user_time_us);
    append(" us");
    append("\nSysTime:\t");
    append_int(task->system_time_us);
    append(" us");
    append("\nStartTime:\t");
    append_int(task->start_time_us);
    append(" us");

    append("\n");

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/stat (Linux-compatible format)
// Format: pid (comm) state ppid pgid sid tty tpgid flags minflt cminflt majflt cmajflt
//         utime stime cutime cstime priority nice nthreads itrealvalue starttime vsize rss ...
auto generate_stat(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    if (task == nullptr) {
        return 0;
    }

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

    // Determine state character
    char state = 'S';  // Default sleeping
    auto ts = task->state.load(std::memory_order_acquire);
    if (ts == ker::mod::sched::task::TaskState::DEAD || ts == ker::mod::sched::task::TaskState::EXITING || task->has_exited) {
        state = 'Z';
    } else if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::RUNNABLE) {
        state = 'R';
    } else if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING) {
        state = task->wait_channel != nullptr ? 'D' : 'S';
    }

    // pid (comm) state ppid pgid sid tty_nr tpgid flags
    append_int(task->pid);
    append(" (");
    append(comm);
    append(") ");
    buf[off++] = state;
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
    append_int(task->user_time_us / 10000);  // utime
    append(" ");
    append_int(task->system_time_us / 10000);  // stime
    append(" ");
    append("0 ");  // cutime (children)
    append("0 ");  // cstime (children)

    // priority nice num_threads itrealvalue starttime
    append("20 ");                            // priority
    append("0 ");                             // nice
    append("1 ");                             // num_threads
    append("0 ");                             // itrealvalue
    append_int(task->start_time_us / 10000);  // starttime (in ticks since boot)
    append(" ");

    // vsize rss
    append("0 ");  // vsize
    append("0 ");  // rss

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
    return off;
}

// Generate content for /proc/<pid>/cmdline (NUL-separated argv)
auto generate_cmdline(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
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
    return len + 1;   // Include the trailing NUL as part of content length
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

    // Idle time: sum idle across all CPUs, divide by CPU count for average
    // For now, report 0.00 as idle tracking is not yet implemented
    append(" 0.00\n");

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
auto generate_wki_runner(char* buf, size_t bufsz) -> size_t {
    const char* hostname = ker::net::wki::g_wki.local_hostname.data();
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
    append_sconst(p, end, " free=");
    append_dec64(p, end, pool.free);
    append_sconst(p, end, " used=");
    append_dec64(p, end, pool.used);
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

    ker::mod::perf::PerfEvent batch[64];
    size_t n = 0;
    while ((n = ker::mod::perf::drain_events(batch, 64, UINT32_MAX)) > 0) {
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
        bool const IS_KPERF = (pfd->node.type == ProcNodeType::KPERF_FILE || pfd->node.type == ProcNodeType::KWKISTAT_FILE ||
                               pfd->node.type == ProcNodeType::KCPUSTAT_FILE || pfd->node.type == ProcNodeType::KCONTSTAT_FILE ||
                               pfd->node.type == ProcNodeType::KIPCSTAT_FILE || pfd->node.type == ProcNodeType::WKI_NETDIAG_FILE);
        size_t const ALLOC_SZ = IS_KPERF ? MAX_KPERF_BUF : MAX_PROCFS_BUF;
        pfd->content = new (std::nothrow) char[ALLOC_SZ];
        if (pfd->content == nullptr) {
            return -ENOMEM;
        }

        switch (pfd->node.type) {
            case ProcNodeType::STATUS_FILE:
                pfd->content_len = generate_status(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::STAT_FILE:
                pfd->content_len = generate_stat(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
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
                pfd->content_len = generate_wki_runner(pfd->content, MAX_PROCFS_BUF);
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
            case ProcNodeType::EXE_LINK: {
                auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
                if (task != nullptr && task->exe_path[0] != '\0') {
                    size_t const LEN = strlen(task->exe_path.data());
                    memcpy(pfd->content, task->exe_path.data(), LEN);
                    pfd->content[LEN] = '\0';
                    pfd->content_len = LEN;
                } else {
                    pfd->content[0] = '\0';
                    pfd->content_len = 0;
                }
                break;
            }
            default:
                pfd->content[0] = '\0';
                pfd->content_len = 0;
                break;
        }
    }

    if (offset >= pfd->content_len) {
        return 0;
    }
    size_t const AVAIL = pfd->content_len - offset;
    count = std::min(count, AVAIL);
    memcpy(buf, pfd->content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_write(File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
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
        int_to_str(task->pid, tmp.data(), tmp.size());
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
    const ker::mod::sched::task::Task* owner_task = nullptr;
    if (pfd->node.pid != 0) {
        owner_task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
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
    } else {
        statbuf->st_mode = S_IFREG | 0444;
    }

    set_stat_timestamps(statbuf, procfs_node_creation_epoch_ns(owner_task));
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
    uint64_t const SELF_PID = (task != nullptr) ? task->pid : 0;

    auto make_file = [](ProcNodeType type, uint64_t pid, bool is_dir) -> File* {
        auto* pfd = new ProcFileData;
        pfd->node.type = type;
        pfd->node.pid = pid;
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
        if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(PID)) == nullptr) {
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
    if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(PID)) == nullptr) {
        return nullptr;
    }

    const char* sub = slash + 1;
    if (strcmp(sub, "exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, static_cast<uint64_t>(PID), false);
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
