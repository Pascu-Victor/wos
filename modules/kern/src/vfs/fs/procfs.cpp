#include "procfs.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "platform/sched/scheduler.hpp"
#include "release.hpp"
#include "vfs/file_operations.hpp"

namespace ker::vfs::procfs {

namespace {

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
    int len = pos;
    for (int i = 0; i < len && static_cast<size_t>(i) < bufsz - 1; ++i) {
        buf[i] = tmp[len - 1 - i];
    }
    buf[len < static_cast<int>(bufsz) ? len : static_cast<int>(bufsz - 1)] = '\0';
}

auto procfs_readdir(File* f, DirEntry* buf, size_t count) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
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
        // PID directories from active task list
        size_t pid_index = count - 6;
        uint32_t task_count = ker::mod::sched::get_active_task_count();
        if (pid_index >= task_count) {
            return -1;  // No more entries
        }
        auto* task = ker::mod::sched::get_active_task_at(static_cast<uint32_t>(pid_index));
        if (task == nullptr) {
            return -1;
        }
        buf->d_ino = task->pid + 100;
        buf->d_off = count + 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        int_to_str(task->pid, buf->d_name.data(), buf->d_name.size());
        return 0;
    }

    if (pfd->node.type == ProcNodeType::PID_DIR) {
        // /proc/<pid>: index 2 = "stat", 3 = "status", 4 = "cmdline", 5 = "exe"
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
        return -1;  // No more entries
    }

    return -1;
}

// Helper to parse a PID from a path component
auto parse_pid(const char* s) -> int64_t {
    if (s == nullptr || *s == '\0') {
        return -1;
    }
    int64_t val = 0;
    for (const char* p = s; *p != 0; ++p) {
        if (*p < '0' || *p > '9') {
            return -1;
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
    append((task->exe_path[0] != 0) ? static_cast<const char*>(task->exe_path) : "(unknown)");
    append("\nPid:\t");
    append_int(task->pid);
    append("\nPPid:\t");
    append_int(task->parentPid);
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
    if (ts_st == ker::mod::sched::task::TaskState::DEAD || ts_st == ker::mod::sched::task::TaskState::EXITING || task->hasExited) {
        append("Z (zombie)");
    } else if (task->schedQueue == ker::mod::sched::task::Task::SchedQueue::RUNNABLE) {
        append("R (running)");
    } else if (task->schedQueue == ker::mod::sched::task::Task::SchedQueue::WAITING) {
        append(task->wait_channel != nullptr ? "D (blocked)" : "S (sleeping)");
    } else {
        append("S (sleeping)");
    }

    // Scheduling info
    append("\nCpu:\t");
    append_int(task->cpu);
    append("\nSchedQueue:\t");
    switch (task->schedQueue) {
        case ker::mod::sched::task::Task::SchedQueue::NONE: append("NONE"); break;
        case ker::mod::sched::task::Task::SchedQueue::RUNNABLE: append("RUNNABLE"); break;
        case ker::mod::sched::task::Task::SchedQueue::WAITING: append("WAITING"); break;
        case ker::mod::sched::task::Task::SchedQueue::DEAD_GC: append("DEAD_GC"); break;
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
    append(task->deferredTaskSwitch ? "1" : "0");
    append("\nVoluntaryBlock:\t");
    append(task->voluntaryBlock ? "1" : "0");
    append("\nWaitingForPid:\t");
    append_int(task->waitingForPid);

    // Signals
    append("\nSigPnd:\t");
    append_int(task->sigPending);
    append("\nSigBlk:\t");
    append_int(task->sigMask);
    append("\nInSigHandler:\t");
    append(task->inSignalHandler ? "1" : "0");

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
    const auto* comm = static_cast<const char*>(task->exe_path);
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
    if (ts == ker::mod::sched::task::TaskState::DEAD || ts == ker::mod::sched::task::TaskState::EXITING || task->hasExited) {
        state = 'Z';
    } else if (task->schedQueue == ker::mod::sched::task::Task::SchedQueue::RUNNABLE) {
        state = 'R';
    } else if (task->schedQueue == ker::mod::sched::task::Task::SchedQueue::WAITING) {
        state = task->wait_channel != nullptr ? 'D' : 'S';
    }

    // pid (comm) state ppid pgid sid tty_nr tpgid flags
    append_int(task->pid);
    append(" (");
    append(comm);
    append(") ");
    buf[off++] = state;
    append(" ");
    append_int(task->parentPid);  // ppid
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
    append("0 ");  // rlim
    append_int(task->sigPending);  // signal (pending)
    append(" ");
    append_int(task->sigMask);  // blocked
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
    append("0 ");  // nswap
    append("0 ");  // cnswap
    append("0 ");  // exit_signal
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
    const auto* path = static_cast<const char*>(task->exe_path);
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

    // Iterate mount table
    for (size_t i = 0; i < ker::vfs::get_mount_count(); ++i) {
        auto* m = ker::vfs::get_mount_at(i);
        if (m == nullptr) {
            continue;
        }
        append((m->fstype != nullptr) ? m->fstype : "none");
        append(" ");
        append((m->path != nullptr) ? m->path : "/");
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

    uint64_t uptime_us = ker::mod::time::getUs();
    uint64_t uptime_sec = uptime_us / 1000000ULL;
    uint64_t uptime_frac = (uptime_us % 1000000ULL) / 10000ULL;  // two decimal places

    append_int(uptime_sec);
    append(".");
    // Zero-pad the fractional part to two digits
    if (uptime_frac < 10) {
        append("0");
    }
    append_int(uptime_frac);

    // Idle time: sum idle across all CPUs, divide by CPU count for average
    // For now, report 0.00 as idle tracking is not yet implemented
    append(" 0.00\n");

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/version
auto generate_version(char* buf, size_t bufsz) -> size_t;

// Generate a one-line status string for /proc/kperfctl
auto generate_kperfctl(char* buf, size_t bufsz) -> size_t {
    const char* state = ker::mod::perf::is_enabled() ? "enabled\n" : "disabled\n";
    size_t len = strlen(state);
    if (len >= bufsz) len = bufsz - 1;
    memcpy(buf, state, len);
    buf[len] = '\0';
    return len;
}

// Forward declarations for helpers defined below
static void append_dec64(char*& p, char* end, uint64_t v);
static void append_sconst(char*& p, char* end, const char* s);

// Generate content for /proc/kcontstat
// Shows per-subsystem container aggregate statistics (non-destructive).
auto generate_kcontstat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char* end = buf + bufsz - 1;

    for (size_t i = 1; i < ker::mod::perf::PERF_SUBSYSTEM_COUNT; ++i) {
        auto s = ker::mod::perf::get_subsystem_stats(static_cast<ker::mod::perf::PerfSubsystem>(i));
        uint64_t ins = s.inserts;
        uint64_t rem = s.removes;
        uint64_t res = s.resizes;
        uint64_t oom = s.oom_failures;
        uint64_t peak = s.peak_count;
        uint64_t cur = s.current_count;

        // Skip subsystems with no activity
        if (ins == 0 && rem == 0 && res == 0 && oom == 0) continue;

        append_sconst(p, end, "subsys=");
        append_sconst(p, end, ker::mod::perf::subsystem_name(static_cast<ker::mod::perf::PerfSubsystem>(i)));
        append_sconst(p, end, " inserts=");
        append_dec64(p, end, ins);
        append_sconst(p, end, " removes=");
        append_dec64(p, end, rem);
        append_sconst(p, end, " resizes=");
        append_dec64(p, end, res);
        append_sconst(p, end, " oom=");
        append_dec64(p, end, oom);
        append_sconst(p, end, " peak=");
        append_dec64(p, end, peak);
        append_sconst(p, end, " current=");
        append_dec64(p, end, cur);
        if (p + 1 < end) *p++ = '\n';
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Hex helper
static void append_hex64(char*& p, char* end, uint64_t v) {
    static const char hx[] = "0123456789abcdef";
    if (p + 18 >= end) return;
    *p++ = '0';
    *p++ = 'x';
    for (int i = 60; i >= 0; i -= 4) *p++ = hx[(v >> i) & 0xf];
}

static void append_dec64(char*& p, char* end, uint64_t v) {
    char tmp[22];
    int n = 0;
    if (v == 0) {
        tmp[n++] = '0';
    } else {
        while (v > 0 && n < 21) {
            tmp[n++] = static_cast<char>('0' + v % 10);
            v /= 10;
        }
    }
    // reverse into output
    if (p + n >= end) return;
    for (int i = n - 1; i >= 0; --i) *p++ = tmp[i];
}

static void append_sconst(char*& p, char* end, const char* s) {
    while (*s && p + 1 < end) *p++ = *s++;
}

static void append_perf_callsite(char*& p, char* end, uint64_t callsite) {
    if (callsite == 0) {
        append_sconst(p, end, "?");
        return;
    }

    if (callsite >= 0xffff800000000000ULL) {
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
//   W <ts_ns> <cpu> <pid> <wake_at_us> <sleep_us> <flags> <callsite>            WAKE
//   B <ts_ns> <cpu> <pid> <wake_at_us> <run_us>   <flags> <callsite>            SLEEP
auto generate_kperf(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char* end = buf + bufsz - 1;

    ker::mod::perf::PerfEvent batch[64];
    size_t n;
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
            }
            if (p + 2 >= end) break;
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
                append_dec64(p, end, static_cast<uint64_t>(ev.lag_v >= 0 ? ev.lag_v : 0u));
                *p++ = ' ';
                append_dec64(p, end, ev.flags);
                *p++ = ' ';
                append_dec64(p, end, ev.aux);
                *p++ = ' ';
                append_perf_callsite(p, end, ev.callsite);
            } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::CONTAINER_STAT) {
                // CONTAINER_STAT: C <ts> <cpu> <pid> <subsys_name> <flags> <count> <capacity> <callsite>
                uint8_t subsys_id = static_cast<uint8_t>(ev.data >> 32);
                append_dec64(p, end, ev.pid);
                *p++ = ' ';
                append_sconst(p, end, ker::mod::perf::subsystem_name(static_cast<ker::mod::perf::PerfSubsystem>(subsys_id)));
                *p++ = ' ';
                append_dec64(p, end, ev.flags);
                *p++ = ' ';
                if (ev.lag_v < 0 && p + 1 < end) *p++ = '-';
                append_dec64(p, end, static_cast<uint64_t>(ev.lag_v >= 0 ? ev.lag_v : -ev.lag_v));
                *p++ = ' ';
                append_dec64(p, end, ev.aux);
                *p++ = ' ';
                append_perf_callsite(p, end, ev.callsite);
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
            }
            if (p + 1 < end) *p++ = '\n';
        }
        if (n < 64) break;  // ring drained
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Generate content for /proc/kcpustat
// Shows per-CPU aggregate scheduler counters (non-destructive).
auto generate_kcpustat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char* end = buf + bufsz - 1;

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) cpu_count = 8;
    if (cpu_count > ker::mod::perf::get_num_perf_cpus()) cpu_count = ker::mod::perf::get_num_perf_cpus();

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
        if (p + 1 < end) *p++ = '\n';
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
        bool is_kperf = (pfd->node.type == ProcNodeType::KPERF_FILE || pfd->node.type == ProcNodeType::KCPUSTAT_FILE ||
                         pfd->node.type == ProcNodeType::KCONTSTAT_FILE);
        size_t alloc_sz = is_kperf ? MAX_KPERF_BUF : MAX_PROCFS_BUF;
        pfd->content = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(alloc_sz));
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
            case ProcNodeType::KCPUSTAT_FILE:
                pfd->content_len = generate_kcpustat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KPERFCTL_FILE:
                pfd->content_len = generate_kperfctl(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::KCONTSTAT_FILE:
                pfd->content_len = generate_kcontstat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::EXE_LINK: {
                auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
                if (task != nullptr && task->exe_path[0] != '\0') {
                    size_t len = strlen(static_cast<const char*>(task->exe_path));
                    memcpy(pfd->content, static_cast<const char*>(task->exe_path), len);
                    pfd->content[len] = '\0';
                    pfd->content_len = len;
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
    size_t avail = pfd->content_len - offset;
    count = std::min(count, avail);
    memcpy(buf, pfd->content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_write(File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) return -EINVAL;
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    if (pfd->node.type != ProcNodeType::KPERFCTL_FILE) return -EPERM;
    if (count == 0) return 0;

    // Accept "enable", "enable <filter>", "mask <filter>", or "disable"
    const char* s = static_cast<const char*>(buf);
    if (count >= 6 && memcmp(s, "enable", 6) == 0) {
        // Check for optional filter: "enable switch,wake,container"
        if (count > 7 && s[6] == ' ') {
            uint8_t mask = ker::mod::perf::parse_event_mask(s + 7, count - 7);
            if (mask == 0) return -EINVAL;
            ker::mod::perf::set_event_mask(mask);
        } else {
            ker::mod::perf::set_event_mask(ker::mod::perf::PERF_MASK_ALL);
        }
        // Reset ring buffers on fresh enable so report only sees the new session
        ker::mod::perf::reset_rings();
        ker::mod::perf::enable();
    } else if (count >= 4 && memcmp(s, "mask", 4) == 0) {
        // Change mask without resetting rings or toggling enable: "mask switch,container"
        if (count > 5 && s[4] == ' ') {
            uint8_t mask = ker::mod::perf::parse_event_mask(s + 5, count - 5);
            if (mask == 0) return -EINVAL;
            ker::mod::perf::set_event_mask(mask);
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
            ker::mod::mm::dyn::kmalloc::free(pfd->content);
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
    off_t new_pos = f->pos;
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
    size_t len = strlen(static_cast<const char*>(task->exe_path));
    len = std::min(len, bufsz);
    memcpy(buf, static_cast<const char*>(task->exe_path), len);
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
};

}  // namespace

auto get_procfs_fops() -> FileOperations* { return &procfs_fops_instance; }

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
    uint64_t self_pid = (task != nullptr) ? task->pid : 0;

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

    // /proc/self -> symlink to /proc/<pid>
    if (strcmp(path, "self") == 0) {
        return make_file(ProcNodeType::SELF_LINK, self_pid, false);
    }
    // /proc/self/exe
    if (strcmp(path, "self/exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, self_pid, false);
    }
    // /proc/self/status
    if (strcmp(path, "self/status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, self_pid, false);
    }
    // /proc/self/stat
    if (strcmp(path, "self/stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, self_pid, false);
    }
    // /proc/self/cmdline
    if (strcmp(path, "self/cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, self_pid, false);
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
        int64_t pid = parse_pid(path);
        if (pid < 0) {
            return nullptr;
        }
        // Check if task exists
        if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(pid)) == nullptr) {
            return nullptr;
        }
        return make_file(ProcNodeType::PID_DIR, static_cast<uint64_t>(pid), true);
    }

    // /proc/<pid>/<subpath>
    std::array<char, 24> pid_str{};
    auto pid_len = static_cast<size_t>(slash - path);
    if (pid_len >= pid_str.size()) {
        return nullptr;
    }
    memcpy(pid_str.data(), path, pid_len);
    pid_str[pid_len] = '\0';
    int64_t pid = parse_pid(pid_str.data());
    if (pid < 0) {
        return nullptr;
    }
    if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(pid)) == nullptr) {
        return nullptr;
    }

    const char* sub = slash + 1;
    if (strcmp(sub, "exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, static_cast<uint64_t>(pid), false);
    }

    return nullptr;  // Unknown procfs path
}

void procfs_init() {
    // Mount procfs at /proc
    ker::vfs::mount_filesystem("/proc", "procfs", nullptr);
}

}  // namespace ker::vfs::procfs
