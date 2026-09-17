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
            dprintf(report, "FAIL: ptrace-registers checks=%u failures=%u\n", checks, failures);
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
    check("syscall-stop", call(trace::request::SYSCALL, target) == 0 && wait_stop(target));
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
    dprintf(report, "%s: ptrace-registers checks=%u failures=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    int const synced = fsync(report);
    close(report);
    return failures || synced || unmapped || checks != 32 ? 1 : 0;
}
