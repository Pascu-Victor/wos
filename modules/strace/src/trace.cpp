#include "trace.hpp"

#include <abi-bits/pid_t.h>
#include <abi-bits/signal.h>
#include <abi-bits/wait.h>
#include <abi/callnums/process.h>
#include <sys/callnums.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <format>
#include <print>
#include <unordered_map>
#include <vector>

#include "decode.hpp"
#include "output.hpp"
#include "ptrace_client.hpp"
#include "remote.hpp"
#include "time_format.hpp"

namespace wos::strace {
namespace {

constexpr uint64_t TRACE_SYSGOOD_OPTION = 0x00000001ULL;
constexpr uint32_t STRACE_STARTUP_WAIT_RETRIES = 500;
constexpr useconds_t STRACE_STARTUP_WAIT_POLL_US = 10 * 1000;

auto wait_for_trace_startup_stop(pid_t pid, int& status) -> bool {
    for (uint32_t attempt = 0; attempt < STRACE_STARTUP_WAIT_RETRIES; attempt++) {
        status = 0;
        pid_t const WAITED = waitpid(pid, &status, WUNTRACED | WNOHANG);
        if (WAITED == pid) {
            if (WIFSTOPPED(status)) {
                return true;
            }
            if (WIFEXITED(status)) {
                std::println(stderr, "strace: pid {} exited with {} before tracing", static_cast<int>(pid), WEXITSTATUS(status));
            } else if (WIFSIGNALED(status)) {
                std::println(stderr, "strace: pid {} was killed by signal {} before tracing", static_cast<int>(pid), WTERMSIG(status));
            } else {
                std::println(stderr, "strace: pid {} reported unexpected startup wait status {:#x}", static_cast<int>(pid), status);
            }
            return false;
        }
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("strace: waitpid");
            return false;
        }
        usleep(STRACE_STARTUP_WAIT_POLL_US);
    }

    std::println(stderr, "strace: timed out waiting for pid {} to stop before tracing", static_cast<int>(pid));
    return false;
}

void reap_tracee_after_startup_failure(pid_t pid) {
    (void)kill(pid, SIGKILL);
    for (uint32_t attempt = 0; attempt < STRACE_STARTUP_WAIT_RETRIES; attempt++) {
        int status = 0;
        pid_t const WAITED = waitpid(pid, &status, WNOHANG);
        if (WAITED == pid || (WAITED < 0 && errno != EINTR)) {
            return;
        }
        usleep(STRACE_STARTUP_WAIT_POLL_US);
    }
}

void detach_tracee_after_startup_failure(uint64_t pid) { (void)ptrace_call(ker::abi::ptrace::request::DETACH, pid, 0, 0); }

struct TraceState {
    std::vector<pid_t> helper_pids;
    int helper_status = 0;
};

auto helper_exit_code(int status) -> int {
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return 128 + WTERMSIG(status);
    }
    return 1;
}

void reap_follow_helpers(TraceState& state, bool block) {
    for (size_t idx = 0; idx < state.helper_pids.size();) {
        int status = 0;
        pid_t const WAITED = waitpid(state.helper_pids.at(idx), &status, block ? 0 : WNOHANG);
        if (WAITED == state.helper_pids.at(idx)) {
            int const RC = helper_exit_code(status);
            if (RC != 0 && state.helper_status == 0) {
                state.helper_status = RC;
            }
            state.helper_pids.erase(state.helper_pids.begin() + static_cast<ptrdiff_t>(idx));
            continue;
        }
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (state.helper_status == 0) {
                state.helper_status = 1;
            }
            state.helper_pids.erase(state.helper_pids.begin() + static_cast<ptrdiff_t>(idx));
            continue;
        }
        ++idx;
    }
}

auto fork_child_from_syscall_result(const PendingSyscall& pending, int64_t result, uint64_t& child_pid) -> bool {
    if (result <= 0 || static_cast<ker::abi::callnums>(pending.callnum) != ker::abi::callnums::process) {
        return false;
    }
    if (static_cast<ker::abi::process::procmgmt_ops>(pending.a1) != ker::abi::process::procmgmt_ops::FORK) {
        return false;
    }
    child_pid = static_cast<uint64_t>(result);
    return true;
}

void spawn_follow_helper(TraceState& state, TraceOutput& output, uint64_t child_pid, const TraceOptions& options) {
    if (child_pid == 0) {
        return;
    }
    if (output.stream != nullptr) {
        std::fflush(output.stream);
    }

    pid_t const HELPER = fork();
    if (HELPER < 0) {
        std::perror("strace: fork follow helper");
        if (state.helper_status == 0) {
            state.helper_status = 1;
        }
        return;
    }
    if (HELPER == 0) {
        TraceOptions child_options = options;
        child_options.append_output = !child_options.output_path.empty() && !child_options.output_separately;
        int const RC = attach_and_trace(child_pid, false, child_options);
        _exit(RC == 0 ? 0 : 1);
    }
    state.helper_pids.push_back(HELPER);
}

auto trace_loop(uint64_t pid, const TraceOptions& options, bool kill_on_setup_failure) -> int {
    TraceOutput output = open_trace_output(pid, options);
    if (!output.valid) {
        if (kill_on_setup_failure) {
            reap_tracee_after_startup_failure(static_cast<pid_t>(pid));
        } else {
            detach_tracee_after_startup_failure(pid);
        }
        return 1;
    }

    TraceState state{};
    std::unordered_map<uint64_t, PendingSyscall> pending;
    pending.reserve(8);

    (void)ptrace_call(ker::abi::ptrace::request::SETOPTIONS, pid, 0, TRACE_SYSGOOD_OPTION);

    for (;;) {
        ker::abi::ptrace::StopInfo stop{};
        if (!syscall_wait(pid, stop)) {
            std::println(stderr, "strace: PTRACE_SYSCALL_WAIT failed");
            reap_follow_helpers(state, true);
            close_trace_output(output);
            return 1;
        }
        reap_follow_helpers(state, false);

        if ((stop.flags & ker::abi::ptrace::STOP_INFO_EXITED) != 0) {
            if (WIFEXITED(stop.wait_status)) {
                emit_trace_line(options, output, pid, current_realtime(options),
                                std::format("+++ exited with {} +++", WEXITSTATUS(stop.wait_status)));
            } else if (WIFSIGNALED(stop.wait_status)) {
                emit_trace_line(options, output, pid, current_realtime(options),
                                std::format("+++ killed by signal {} +++", WTERMSIG(stop.wait_status)));
            } else {
                emit_trace_line(options, output, pid, current_realtime(options),
                                std::format("+++ exited with status {:#x} +++", stop.wait_status));
            }
            reap_follow_helpers(state, true);
            close_trace_output(output);
            return state.helper_status;
        }

        auto const& event = stop.event;
        if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_ENTER) {
            auto it = pending.try_emplace(event.tid).first;
            if (!it->second.valid && (stop.flags & ker::abi::ptrace::STOP_INFO_REGS_VALID) != 0) {
                auto const& regs = stop.regs;
                it->second = PendingSyscall{
                    .valid = true,
                    .callnum = regs.rax,
                    .a1 = regs.rdi,
                    .a2 = regs.rsi,
                    .a3 = regs.rdx,
                    .a4 = regs.r8,
                    .a5 = regs.r9,
                    .a6 = regs.r10,
                    .entered_at = current_realtime(options),
                    .duration_started_at = current_monotonic(),
                };
            }
        } else if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_EXIT) {
            if ((stop.flags & ker::abi::ptrace::STOP_INFO_REGS_VALID) != 0) {
                auto const RESULT = static_cast<int64_t>(stop.regs.rax);
                auto it = pending.find(event.tid);
                if (it != pending.end() && it->second.valid) {
                    timespec const EXITED_AT = current_monotonic();
                    emit_trace_line(options, output, pid, it->second.entered_at,
                                    std::format("{} = {}{}", format_entry(pid, it->second), format_result(RESULT),
                                                format_duration_suffix(it->second.duration_started_at, EXITED_AT)));
                    if (options.follow_forks) {
                        uint64_t child_pid = 0;
                        if (fork_child_from_syscall_result(it->second, RESULT, child_pid)) {
                            spawn_follow_helper(state, output, child_pid, options);
                        }
                    }
                    it->second.valid = false;
                } else {
                    emit_trace_line(options, output, pid, current_realtime(options),
                                    std::format("{} = {}", callnum_name(event.message), format_result(RESULT)));
                }
            }
        } else {
            emit_trace_line(options, output, pid, current_realtime(options), std::format("--- stopped by signal {} ---", event.signal));
        }
    }
}

auto launch_tracee(char** argv) -> int {
    if (ptrace_call(ker::abi::ptrace::request::TRACEME, 0, 0, 0) < 0) {
        std::perror("strace: PTRACE_TRACEME");
        return 1;
    }
    int64_t const TARGET_RC =
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to pin tracee locally: {}", format_result(TARGET_RC));
        return 1;
    }
    if (kill(getpid(), SIGSTOP) != 0) {
        std::perror("strace: SIGSTOP");
        return 1;
    }
    execvp(argv[0], argv);
    std::perror("strace: execvp");
    return 127;
}

}  // namespace

auto attach_and_trace(uint64_t pid, bool route_proxy, const TraceOptions& options) -> int {
    if (route_proxy) {
        ker::abi::ptrace::RemoteInfo info{};
        if (read_remote_info(pid, info) && is_proxy_info(info)) {
            return exec_strace_remote_attach(info, options);
        }
    }

    if (ptrace_call(ker::abi::ptrace::request::ATTACH, pid, 0, 0) < 0) {
        std::println(stderr, "strace: attach to {} failed", pid);
        return 1;
    }
    int status = 0;
    if (!wait_for_trace_startup_stop(static_cast<pid_t>(pid), status)) {
        detach_tracee_after_startup_failure(pid);
        return 1;
    }
    return trace_loop(pid, options, false);
}

auto trace_command(char** argv, const TraceOptions& options) -> int {
    pid_t const CHILD = fork();
    if (CHILD < 0) {
        std::perror("strace: fork");
        return 1;
    }
    if (CHILD == 0) {
        return launch_tracee(argv);
    }
    int status = 0;
    if (!wait_for_trace_startup_stop(CHILD, status)) {
        reap_tracee_after_startup_failure(CHILD);
        return 1;
    }
    if (target_is_proxy(static_cast<uint64_t>(CHILD))) {
        std::println(stderr, "strace: tracee became a WKI proxy before syscall tracing could start");
        reap_tracee_after_startup_failure(CHILD);
        return 1;
    }
    return trace_loop(static_cast<uint64_t>(CHILD), options, true);
}

}  // namespace wos::strace
