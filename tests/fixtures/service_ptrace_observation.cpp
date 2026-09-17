// Functional register/control access, followed by irreversible authority loss.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/process.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <new>

#include "../../modules/kern/src/abi/ptrace.hpp"

namespace {
namespace trace = ker::abi::ptrace;
int64_t call(trace::request op, pid_t target, uint64_t data = 0) {
    return ker::process::ptrace(static_cast<uint64_t>(op), target, 0, data);
}
uint64_t now();
bool wait_stop(pid_t target) {
    int status = 0;
    uint64_t const deadline = now() + 5000000000;
    while (now() < deadline) {
        pid_t const waited = waitpid(target, &status, WUNTRACED | WNOHANG);
        if (waited > 0) return waited == target && WIFSTOPPED(status);
        if (waited < 0 && errno != EINTR) return false;
        usleep(1000);
    }
    return false;
}
uint64_t now() {
    timespec time{};
    if (clock_gettime(CLOCK_MONOTONIC, &time)) return 0;
    return static_cast<uint64_t>(time.tv_sec) * 1000000000 + time.tv_nsec;
}
bool progress(std::atomic<uint64_t>& ticks, uint64_t previous) {
    uint64_t const deadline = now() + 2000000000;
    while (ticks.load(std::memory_order_relaxed) == previous && now() < deadline) sched_yield();
    return ticks.load(std::memory_order_relaxed) != previous;
}
int64_t registers(pid_t target, bool write, trace::regset kind, void* buffer, size_t& size) {
    trace::RegsetIo io{kind, buffer, size};
    int64_t result = -EBUSY;
    // EBUSY/EIO precede mutation while a requested stop leaves its CPU.
    for (unsigned i = 0; i < 1000; ++i) {
        result = call(write ? trace::request::SETREGSET : trace::request::GETREGSET, target, reinterpret_cast<uint64_t>(&io));
        if (result != -EBUSY && result != -EIO) break;
        sched_yield();
    }
    size = io.size;
    return result;
}
bool rich_exec_exit(bool killed = false) {
    pid_t child = fork();
    if (child < 0) return false;
    if (child == 0) {
        if (call(trace::request::TRACEME, 0) != 0 || raise(SIGSTOP)) _exit(10);
        execl("/usr/bin/true", "true", nullptr);
        _exit(11);
    }
    bool ok = wait_stop(child);
    if (ok && killed) {
        int status = 0;
        bool const sent = call(trace::request::KILL, child) == 0;
        if (!sent) {
            (void)call(trace::request::DETACH, child);
            (void)kill(child, SIGKILL);
        }
        return waitpid(child, &status, 0) == child && sent && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    }
    trace::Event before{}, after{};
    ok = ok && call(trace::request::GETEVENTMSG, child, reinterpret_cast<uint64_t>(&before)) == 0 &&
         call(trace::request::SYSCALL_WAIT, child, 1) == -EFAULT &&
         call(trace::request::GETEVENTMSG, child, reinterpret_cast<uint64_t>(&after)) == 0 && !memcmp(&before, &after, sizeof(before));
    bool exited = false;
    unsigned syscall_stops = 0;
    for (unsigned i = 0; ok && i < 2048; ++i) {
        trace::StopInfo stop{};
        if (call(trace::request::SYSCALL_WAIT, child, reinterpret_cast<uint64_t>(&stop)) != 0) {
            ok = false;
            break;
        }
        if (stop.flags & trace::STOP_INFO_EXITED) {
            exited = true;
            ok = WIFEXITED(stop.wait_status) && WEXITSTATUS(stop.wait_status) == 0 && stop.event.reason == trace::stop_reason::EXIT;
            break;
        }
        if (stop.event.reason == trace::stop_reason::SYSCALL_ENTER || stop.event.reason == trace::stop_reason::SYSCALL_EXIT) {
            ++syscall_stops;
            ok = (stop.flags & trace::STOP_INFO_REGS_VALID) != 0;
        }
    }
    int status = 0;
    if (ok && exited && syscall_stops) {
        errno = 0;
        return waitpid(child, &status, WNOHANG) == -1 && errno == ECHILD;
    }
    (void)call(trace::request::DETACH, child);
    (void)kill(child, SIGKILL);
    uint64_t const deadline = now() + 2000000000;
    while (waitpid(child, &status, WNOHANG) == 0 && now() < deadline) usleep(1000);
    return false;
}
bool proxy_observation(const char* node, int report) {
    const char* peer = strcmp(node, "wos-0") == 0 ? "wos-3" : "wos-0";
    pid_t proxy = fork();
    if (proxy < 0) return false;
    if (proxy == 0) {
        // The report belongs to this local harness, not the remote payload.
        close(report);
        // This payload has no I/O. Do not re-export the harness's forwarded
        // stdio when this check itself is running on the WKI receiver.
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        execl("/usr/bin/on", "on", peer, "/usr/bin/sleep", "20", nullptr);
        _exit(12);
    }
    trace::RemoteInfo remote{};
    bool found = false;
    uint64_t const deadline = now() + 5000000000;
    while (now() < deadline) {
        if (call(trace::request::GET_REMOTE_INFO, proxy, reinterpret_cast<uint64_t>(&remote)) == 0 && remote.is_proxy && remote.task_id &&
            remote.remote_pid && !strcmp(remote.target_hostname.data(), peer)) {
            found = remote.proxy_pid == static_cast<uint64_t>(proxy) && remote.target_node != UINT64_MAX;
            break;
        }
        usleep(1000);
    }
    pid_t observer = found ? fork() : -1;
    if (observer == 0) {
        memset(&remote, 0xa5, sizeof(remote));
        auto const before = remote;
        bool const ok = ker::process::prctl(0x574f5302, 0, 0, 0, 0) == 0 &&
                        call(trace::request::GET_REMOTE_INFO, proxy, reinterpret_cast<uint64_t>(&remote)) == -EPERM &&
                        !memcmp(&before, &remote, sizeof(remote));
        _exit(ok ? 0 : 13);
    }
    int status = 0;
    bool ok = observer > 0 && waitpid(observer, &status, 0) == observer && status == 0;
    int const observer_status = status;
    int const killed = kill(proxy, SIGKILL);
    ok = killed == 0 && ok;
    uint64_t const cleanup_deadline = now() + 5000000000;
    pid_t reaped = 0;
    while (now() < cleanup_deadline && (reaped = waitpid(proxy, &status, WNOHANG)) == 0) usleep(1000);
    dprintf(report, "proxy found=%d id=%llu remote=%llu runner=%s observer=%ld observer_status=%d kill=%d reaped=%ld status=%d\n", found,
            (unsigned long long)remote.task_id, (unsigned long long)remote.remote_pid, remote.target_hostname.data(), (long)observer,
            observer_status, killed, (long)reaped, status);
    return found && ok && reaped == proxy && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
}
}  // namespace

int main(int argc, char** argv) {
    char node[64]{};
    if (argc != 4 || geteuid() != 0 || gethostname(node, sizeof(node)) || strcmp(node, argv[2])) return 2;
    int report = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (report < 0) return 3;
    static_assert(std::atomic<uint64_t>::is_always_lock_free);
    int const shmid = shmget(IPC_PRIVATE, 4096, 0600);
    if (shmid < 0) return 4;
    void* shared = shmat(shmid, nullptr, 0);
    if (shared == reinterpret_cast<void*>(-1)) return 4;
    auto* ticks = new (shared) std::atomic<uint64_t>(0);
    auto* use_syscalls = new (static_cast<unsigned char*>(shared) + 64) std::atomic<bool>(strcmp(argv[3], "syscall") == 0);
    int ready[2];
    if (pipe(ready)) return 5;
    alarm(45);
    pid_t target = fork();
    if (target < 0) return 6;
    if (target == 0) {
        close(ready[0]);
        close(report);
        if (shmdt(shared)) _exit(10);
        shared = shmat(shmid, nullptr, 0);
        if (shared == reinterpret_cast<void*>(-1)) _exit(10);
        ticks = static_cast<std::atomic<uint64_t>*>(shared);
        use_syscalls = reinterpret_cast<std::atomic<bool>*>(static_cast<unsigned char*>(shared) + 64);
        if (setgroups(0, nullptr) || setgid(2000) || setuid(1000) || ker::process::prctl(4, 1, 0, 0, 0)) _exit(10);
        if (write(ready[1], "R", 1) != 1) _exit(11);
        close(ready[1]);
        for (;;) {
            ticks->fetch_add(1, std::memory_order_relaxed);
            if (use_syscalls->load(std::memory_order_relaxed)) sched_yield();
        }
    }
    close(ready[1]);
    unsigned checks = 0, failures = 0;
    auto check = [&](const char* name, bool ok) {
        ++checks;
        if (!ok) ++failures;
        dprintf(report, "node=%s op=%s preserved=%d\n", node, name, ok);
        if (!ok) {
            (void)call(trace::request::DETACH, target);
            (void)kill(target, SIGKILL);
            uint64_t const deadline = now() + 2000000000;
            int status;
            while (waitpid(target, &status, WNOHANG) == 0 && now() < deadline) usleep(1000);
            (void)shmctl(shmid, IPC_RMID, nullptr);
            (void)shmdt(shared);
            dprintf(report, "FAIL: ptrace-observation checks=%u failures=%u\n", checks, failures);
            (void)fsync(report);
            close(report);
            _exit(1);
        }
    };
    char value = 0;
    check("target-ready", read(ready[0], &value, 1) == 1 && value == 'R' && progress(*ticks, 0) && shmctl(shmid, IPC_RMID, nullptr) == 0);
    close(ready[0]);
    check("attach", call(trace::request::ATTACH, target) == 0);
    check("initial-stop", wait_stop(target));
    check("rich-exec-exit-reaped", rich_exec_exit());
    check("ptrace-kill-reaped", rich_exec_exit(true));
    check("proxy-info-authority-and-cleanup", proxy_observation(node, report));
    uint64_t tids[16]{};
    trace::ThreadList threads{tids, 16, 99};
    bool contains = false;
    bool thread_ok = call(trace::request::LIST_THREADS, target, reinterpret_cast<uint64_t>(&threads)) == 0 && threads.count <= 16;
    for (size_t i = 0; i < threads.count && i < 16; ++i) contains = contains || tids[i] == static_cast<uint64_t>(target);
    check("thread-list", thread_ok && contains);
    trace::ThreadList short_threads{nullptr, 0, 99};
    check("thread-count",
          call(trace::request::LIST_THREADS, target, reinterpret_cast<uint64_t>(&short_threads)) == -ENOSPC && short_threads.count >= 1);
    trace::Event event{};
    check("event", call(trace::request::GETEVENTMSG, target, reinterpret_cast<uint64_t>(&event)) == 0 &&
                       event.tid == static_cast<uint64_t>(target) && event.reason == trace::stop_reason::INTERRUPT);
    trace::RemoteInfo remote{};
    check("remote-info", call(trace::request::GET_REMOTE_INFO, target, reinterpret_cast<uint64_t>(&remote)) == 0 &&
                             remote.proxy_pid == static_cast<uint64_t>(target) && remote.is_proxy == 0);
    check("self-info", call(trace::request::GET_REMOTE_INFO, 0, reinterpret_cast<uint64_t>(&remote)) == 0 &&
                           remote.proxy_pid == static_cast<uint64_t>(getpid()));
    trace::X86_64GprState original{};
    size_t size = sizeof(original);
    bool const have_gprs = registers(target, false, trace::regset::X86_64_GPR, &original, size) == 0 && size == sizeof(original);
    check("read-gprs", have_gprs);
    dprintf(report, "initial rip=%llx rsp=%llx rax=%llx rdi=%llx\n", (unsigned long long)original.rip, (unsigned long long)original.rsp,
            (unsigned long long)original.rax, (unsigned long long)original.rdi);
    unsigned char fpu[8192]{};
    size_t fpu_size = sizeof(fpu);
    bool const have_fpu = registers(target, false, trace::regset::X86_64_XSAVE, fpu, fpu_size) == 0 && fpu_size <= sizeof(fpu);
    check("read-fpu", have_fpu);
    auto modified = original;
    modified.rax ^= 0x1234;
    trace::X86_64GprState observed{};
    size = sizeof(original);
    bool ok = have_gprs && registers(target, true, trace::regset::X86_64_GPR, &modified, size) == 0 &&
              registers(target, false, trace::regset::X86_64_GPR, &observed, size) == 0 && observed.rax == modified.rax &&
              registers(target, true, trace::regset::X86_64_GPR, &original, size) == 0;
    check("update-restore-gprs", ok);
    unsigned char after_fpu[8192]{};
    size = fpu_size;
    ok = have_fpu && registers(target, true, trace::regset::X86_64_XSAVE, fpu, size) == 0 &&
         registers(target, false, trace::regset::X86_64_XSAVE, after_fpu, size) == 0 && size == fpu_size &&
         !memcmp(fpu, after_fpu, fpu_size);
    check("restore-fpu", ok);
    trace::HwBreak point{.address = original.rip, .length = 1, .type = trace::hw_break_type::EXECUTE, .slot = 0};
    check("install-breakpoint", have_gprs && call(trace::request::SET_HW_BREAK, target, reinterpret_cast<uint64_t>(&point)) == 0);
    check("clear-breakpoint", call(trace::request::DEL_HW_BREAK, target, reinterpret_cast<uint64_t>(&point)) == 0);
    check("set-options", call(trace::request::SETOPTIONS, target, 1) == 0);
    check("single-step", call(trace::request::SINGLESTEP, target) == 0 && wait_stop(target));
    size = sizeof(observed);
    auto const step_result = registers(target, false, trace::regset::X86_64_GPR, &observed, size);
    dprintf(report, "stepped result=%lld rip=%llx rsp=%llx rax=%llx rdi=%llx\n", (long long)step_result, (unsigned long long)observed.rip,
            (unsigned long long)observed.rsp, (unsigned long long)observed.rax, (unsigned long long)observed.rdi);
    uint64_t previous = ticks->load(std::memory_order_relaxed);
    check("continue", call(trace::request::CONT, target) == 0 && progress(*ticks, previous));
    check("interrupt", call(trace::request::INTERRUPT, target) == 0 && wait_stop(target));
    use_syscalls->store(true, std::memory_order_relaxed);
    trace::StopInfo stop{};
    check("rich-syscall-stop",
          call(trace::request::SYSCALL_WAIT, target, reinterpret_cast<uint64_t>(&stop)) == 0 &&
              (stop.event.reason == trace::stop_reason::SYSCALL_ENTER || stop.event.reason == trace::stop_reason::SYSCALL_EXIT) &&
              (stop.flags & trace::STOP_INFO_REGS_VALID) && stop.event.tid == static_cast<uint64_t>(target));
    check("matching-user", !setgroups(0, nullptr) && !setgid(2000) && !setuid(1000));
    check("drop-ceiling", ker::process::prctl(0x574f5302, 0, 0, 0, 0) == 0);
    previous = ticks->load(std::memory_order_relaxed);
    for (bool write : {false, true}) {
        for (bool xstate : {false, true}) {
            unsigned char buffer[8192];
            memset(buffer, 0xa5, sizeof(buffer));
            if (write) {
                if (xstate)
                    memcpy(buffer, fpu, sizeof(fpu));
                else
                    memcpy(buffer, &original, sizeof(original));
            }
            unsigned char before[8192];
            memcpy(before, buffer, sizeof(buffer));
            trace::RegsetIo io{xstate ? trace::regset::X86_64_XSAVE : trace::regset::X86_64_GPR, buffer, sizeof(buffer)};
            auto const original_io = io;
            ok = call(write ? trace::request::SETREGSET : trace::request::GETREGSET, target, reinterpret_cast<uint64_t>(&io)) == -EPERM &&
                 !memcmp(&io, &original_io, sizeof(io)) && !memcmp(buffer, before, sizeof(buffer));
            check(write ? (xstate ? "denied-set-fpu" : "denied-set-gprs") : (xstate ? "denied-get-fpu" : "denied-get-gprs"), ok);
        }
    }
    check("denied-install-breakpoint", call(trace::request::SET_HW_BREAK, target, reinterpret_cast<uint64_t>(&point)) == -EPERM);
    check("denied-clear-breakpoint", call(trace::request::DEL_HW_BREAK, target, reinterpret_cast<uint64_t>(&point)) == -EPERM);
    for (auto op : {trace::request::KILL, trace::request::CONT, trace::request::SINGLESTEP, trace::request::SYSCALL,
                    trace::request::INTERRUPT, trace::request::SETOPTIONS}) {
        char label[40];
        snprintf(label, sizeof(label), "denied-control-%llu", static_cast<unsigned long long>(op));
        check(label, call(op, target) == -EPERM);
    }
    usleep(20000);
    memset(tids, 0xa5, sizeof(tids));
    uint64_t const before_tid = tids[0];
    threads = {tids, 16, 99};
    auto const before_threads = threads;
    check("denied-threads", call(trace::request::LIST_THREADS, target, reinterpret_cast<uint64_t>(&threads)) == -EPERM &&
                                !memcmp(&threads, &before_threads, sizeof(threads)) && tids[0] == before_tid);
    memset(&event, 0xa5, sizeof(event));
    auto const before_event = event;
    check("denied-event", call(trace::request::GETEVENTMSG, target, reinterpret_cast<uint64_t>(&event)) == -EPERM &&
                              !memcmp(&event, &before_event, sizeof(event)));
    memset(&remote, 0xa5, sizeof(remote));
    auto const before_remote = remote;
    check("denied-remote-info", call(trace::request::GET_REMOTE_INFO, target, reinterpret_cast<uint64_t>(&remote)) == -EPERM &&
                                    !memcmp(&remote, &before_remote, sizeof(remote)));
    memset(&stop, 0xa5, sizeof(stop));
    auto const before_stop = stop;
    check("denied-rich-wait", call(trace::request::SYSCALL_WAIT, target, reinterpret_cast<uint64_t>(&stop)) == -EPERM &&
                                  !memcmp(&stop, &before_stop, sizeof(stop)));
    check("self-info-after-drop", call(trace::request::GET_REMOTE_INFO, 0, reinterpret_cast<uint64_t>(&remote)) == 0 &&
                                      remote.proxy_pid == static_cast<uint64_t>(getpid()));
    check("denials-keep-stopped", ticks->load(std::memory_order_relaxed) == previous);
    check("detach-after-drop", call(trace::request::DETACH, target) == 0);
    check("detach-resumes", progress(*ticks, previous));
    int status = 0;
    ok = kill(target, SIGKILL) == 0;
    pid_t waited;
    do {
        waited = waitpid(target, &status, 0);
    } while (waited < 0 && errno == EINTR);
    check("target-reaped", ok && waited == target && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
    int const unmapped = shmdt(shared);
    dprintf(report, "%s: ptrace-observation checks=%u failures=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    int const synced = fsync(report);
    close(report);
    return failures || synced || unmapped || checks != 45 ? 1 : 0;
}
