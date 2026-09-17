// Ordinary signal/ptrace interaction checks on owned children in a private VM.
#include <errno.h>
#include <grp.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
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
struct State {
    std::atomic<unsigned long> progress{0};
    std::atomic<unsigned> handled{0};
    std::atomic<unsigned> command{0};
};
State* shared;
void notice(int) { shared->handled.fetch_add(1, std::memory_order_relaxed); }
auto now() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(20);
    return static_cast<uint64_t>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
}
void delay(unsigned ms) {
    auto const end = now() + ms;
    while (now() < end) usleep(1000);
}
auto control(trace::request request, pid_t target) -> int64_t { return ker::process::ptrace(static_cast<uint64_t>(request), target, 0, 0); }
auto wait_bounded(pid_t child, int& status, int flags, unsigned ms) -> pid_t {
    auto const end = now() + ms;
    do {
        pid_t result = waitpid(child, &status, flags | WNOHANG);
        if (result != 0 && (result > 0 || errno != EINTR)) return result;
        usleep(1000);
    } while (now() < end);
    return 0;
}
auto run_case(unsigned uid, bool syscalls, const char* mode) -> int {
    alarm(20);
    bool const denied = !strcmp(mode, "denied");
    if (!denied && uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) return 2;
    int const id = shmget(IPC_PRIVATE, 4096, 0600);
    if (id < 0) return 3;
    void* memory = shmat(id, nullptr, 0);
    if (memory == reinterpret_cast<void*>(-1)) {
        shmctl(id, IPC_RMID, nullptr);
        return 4;
    }
    shared = new (memory) State;
    int ready[2];
    if (pipe(ready)) {
        shmctl(id, IPC_RMID, nullptr);
        shmdt(memory);
        return 5;
    }
    pid_t const child = fork();
    if (child == 0) {
        close(ready[0]);
        if (shmdt(memory)) _exit(6);
        memory = shmat(id, nullptr, 0);
        if (memory == reinterpret_cast<void*>(-1)) _exit(7);
        shared = static_cast<State*>(memory);
        if (denied && uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) _exit(11);
        struct sigaction action{};
        action.sa_handler = notice;
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGUSR1, &action, nullptr) || ker::process::prctl(4, 1, 0, 0, 0)) _exit(8);
        if (write(ready[1], "R", 1) != 1) _exit(9);
        close(ready[1]);
        for (;;) {
            shared->progress.fetch_add(1, std::memory_order_relaxed);
            unsigned const command = shared->command.exchange(0);
            if (command == 1) static_cast<void>(syscall(0x7fffffffL));
            if (command == 2) asm volatile("int3");
            if (syscalls) sched_yield();
            asm volatile("pause");
        }
    }
    close(ready[1]);
    if (child < 0) {
        close(ready[0]);
        shmctl(id, IPC_RMID, nullptr);
        shmdt(memory);
        return 10;
    }
    pollfd event{.fd = ready[0], .events = POLLIN};
    char value = 0;
    bool const ready_ok = poll(&event, 1, 2000) > 0 && read(ready[0], &value, 1) == 1 && value == 'R';
    close(ready[0]);
    bool const marked = shmctl(id, IPC_RMID, nullptr) == 0;
    delay(20);
    bool const running = shared->progress.load() != 0;
    bool const attached = ready_ok && running && control(trace::request::ATTACH, child) == 0;
    int status = 0;
    bool stopped = attached;
    auto reason = trace::stop_reason::INTERRUPT;
    auto get_event = [&]() {
        trace::Event event{};
        bool const ok =
            ker::process::ptrace(static_cast<uint64_t>(trace::request::GETEVENTMSG), child, 0, reinterpret_cast<uint64_t>(&event)) == 0;
        reason = event.reason;
        return ok;
    };
    if (strcmp(mode, "queued")) {
        stopped = stopped && wait_bounded(child, status, WUNTRACED, 2000) == child && WIFSTOPPED(status);
    } else {
        stopped = stopped && get_event() && reason == trace::stop_reason::INTERRUPT;
    }
    if (stopped && (!strcmp(mode, "entry") || !strcmp(mode, "exit"))) {
        stopped = control(trace::request::SYSCALL, child) == 0 && wait_bounded(child, status, WUNTRACED, 2000) == child &&
                  WIFSTOPPED(status) && get_event() && reason == trace::stop_reason::SYSCALL_ENTER;
        if (stopped && !strcmp(mode, "exit")) {
            stopped = control(trace::request::SYSCALL, child) == 0 && wait_bounded(child, status, WUNTRACED, 2000) == child &&
                      WIFSTOPPED(status) && get_event() && reason == trace::stop_reason::SYSCALL_EXIT;
        }
    }
    if (stopped && (!strcmp(mode, "fatal") || !strcmp(mode, "breakpoint"))) {
        bool const fatal = !strcmp(mode, "fatal");
        shared->command.store(fatal ? 1 : 2);
        stopped = control(trace::request::CONT, child) == 0 && wait_bounded(child, status, WUNTRACED, 2000) == child &&
                  WIFSTOPPED(status) && get_event() && reason == (fatal ? trace::stop_reason::EXCEPTION : trace::stop_reason::BREAKPOINT);
    }
    delay(20);
    auto const before = shared->progress.load();
    delay(50);
    bool const quiet = stopped && shared->progress.load() == before;
    bool accepted = false, effect = false, recovered = false;
    pid_t reaped = 0;
    if (quiet && !strcmp(mode, "continue")) {
        accepted = kill(child, SIGUSR1) == 0 && kill(child, SIGCONT) == 0;
        delay(100);
        bool const preserved = shared->progress.load() == before && shared->handled.load() == 0;
        bool const detached = control(trace::request::DETACH, child) == 0;
        auto const deadline = now() + 2000;
        while (shared->handled.load() == 0 && now() < deadline) usleep(1000);
        effect = accepted && preserved && detached && shared->handled.load() == 1 && shared->progress.load() != before;
        accepted = accepted && kill(child, SIGKILL) == 0;
        reaped = wait_bounded(child, status, 0, 2000);
    } else if (quiet) {
        bool denial_ok = true;
        if (denied) {
            pid_t const sender = fork();
            if (sender == 0) {
                if (setgroups(0, nullptr) || setgid(200) || setuid(200)) _exit(12);
                errno = 0;
                int const result = kill(child, SIGKILL);
                _exit(result == -1 && errno == EPERM ? 0 : 13);
            }
            int sender_status = -1;
            bool const sender_reaped = sender > 0 && wait_bounded(sender, sender_status, 0, 2000) == sender;
            delay(100);
            denial_ok = sender_reaped && sender_status == 0 && shared->progress.load() == before && shared->handled.load() == 0;
            printf("denial reaped=%d status=%d preserved=%d\n", sender_reaped, sender_status, denial_ok);
        }
        accepted = !strcmp(mode, "ptrace-kill") ? control(trace::request::KILL, child) == 0 : kill(child, SIGKILL) == 0;
        reaped = wait_bounded(child, status, 0, 2000);
        if (!strcmp(mode, "queued")) {
            bool const consumed = reaped == child && WIFSTOPPED(status);
            printf("queued-stop consumed=%d\n", consumed);
            denial_ok = denial_ok && consumed;
            if (consumed) reaped = wait_bounded(child, status, 0, 2000);
        }
        effect = denial_ok && accepted && reaped == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    }
    if (reaped != child) {
        // Keep primary failure separate from recovery; never count this detach
        // as successful ordinary kill delivery while the trace stop was owned.
        if (attached) static_cast<void>(control(trace::request::DETACH, child));
        static_cast<void>(kill(child, SIGKILL));
        reaped = wait_bounded(child, status, 0, 2000);
        recovered = reaped == child;
    }
    bool const cleaned = reaped == child && shmdt(memory) == 0;
    bool const pass =
        ready_ok && marked && running && quiet && accepted && effect && cleaned && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    printf(
        "%s uid=%u loop=%s mode=%s stop_kind=%lu target=%ld ready=%d running=%d stopped=%d quiet=%d accepted=%d effect=%d "
        "recovered=%d status=%d reaped=%d cleaned=%d\n",
        pass ? "PASS" : "FAIL", uid, syscalls ? "syscall" : "user", mode, static_cast<unsigned long>(reason), static_cast<long>(child),
        ready_ok, running, stopped, quiet, accepted, effect, recovered, status, reaped == child, cleaned);
    fflush(stdout);
    return pass ? 0 : 1;
}
}  // namespace

int main(int argc, char** argv) {
    static_assert(std::atomic<unsigned long>::is_always_lock_free && std::atomic<unsigned>::is_always_lock_free);
    if (geteuid() != 0) return 2;
    unsigned failures = 0;
    bool const extended = argc == 2 && !strcmp(argv[1], "--extended");
    const char* const baseline_modes[] = {"kill", "ptrace-kill", "continue"};
    const char* const extended_modes[] = {"queued", "entry", "exit", "fatal", "breakpoint", "denied"};
    for (unsigned uid : {0U, 104U}) {
        for (bool syscalls : {false, true}) {
            if (extended && !syscalls) continue;
            for (unsigned i = 0; i < (extended ? 6U : 3U); ++i) {
                const char* mode = extended ? extended_modes[i] : baseline_modes[i];
                pid_t child = fork();
                if (child == 0) _exit(run_case(uid, syscalls, mode));
                if (child < 0) return 3;
                int status = 0;
                pid_t const waited = wait_bounded(child, status, 0, 25000);
                printf("controller reaped=%d status=%d\n", waited == child, status);
                fflush(stdout);
                if (waited != child) return 4;
                if (!WIFEXITED(status) || WEXITSTATUS(status)) ++failures;
            }
        }
    }
    return failures ? 1 : 0;
}
