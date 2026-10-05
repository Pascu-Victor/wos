#!/usr/bin/env python3
"""Compare actual ptrace and scheduler yield classifiers, including proxy waits."""

from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source, signature):
    start = source.index(signature)
    end = source.index("{", start) + 1
    depth = 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


PRELUDE = r'''
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cstdio>
namespace ker::mod::sched::task {
enum class WaitChannelKind { NONE, GENERIC, WAITPID, PTRACE, FUTEX, WKI_EXECVE_PROXY };
struct Task {
    bool deferred_task_switch = false, yield_switch = false;
    uint64_t wki_proxy_task_id = 0;
    const char *wait_channel = nullptr;
    WaitChannelKind wait_channel_kind = WaitChannelKind::NONE;
    bool wait_channel_is(WaitChannelKind kind) const { return wait_channel_kind == kind; }
};
}
namespace task = ker::mod::sched::task;
using Task = task::Task;
'''

CASES = r'''
int main() {
    const char *names[] = {nullptr, "sched_yield", "waitpid", "ptrace", "futex_wait", "wki_execve_proxy"};
    unsigned cases = 0;
    for (bool yield : {false, true}) for (bool deferred : {false, true})
    for (uint64_t proxy : {uint64_t{0}, uint64_t{42}})
    for (auto kind : {task::WaitChannelKind::NONE, task::WaitChannelKind::GENERIC, task::WaitChannelKind::WAITPID,
                      task::WaitChannelKind::PTRACE, task::WaitChannelKind::FUTEX, task::WaitChannelKind::WKI_EXECVE_PROXY})
    for (const char *name : names) {
        Task t{deferred, yield, proxy, name, kind};
        bool schedulerYield = scheduler_classifier::deferred_switch_is_sched_yield(&t);
        assert(deferred_switch_is_sched_yield(t) == schedulerYield);
        bool handoff = deferred && proxy && kind == task::WaitChannelKind::WKI_EXECVE_PROXY;
        assert(should_suppress_deferred_syscall_exit_stop(t) == (handoff || (deferred && !schedulerYield)));
        ++cases;
    }
    Task internal{true, true};
    assert(!should_suppress_deferred_syscall_exit_stop(internal));
    Task staleYield{true, true, 0, "futex_wait", task::WaitChannelKind::FUTEX};
    assert(should_suppress_deferred_syscall_exit_stop(staleYield));
    Task proxy{true, true, 42, "wki_execve_proxy", task::WaitChannelKind::WKI_EXECVE_PROXY};
    assert(should_suppress_deferred_syscall_exit_stop(proxy));
    std::printf("%u actual ptrace/scheduler classification combinations passed\n", cases);
}
'''


def main():
    ptrace = (ROOT / "modules/kern/src/platform/debug/ptrace.cpp").read_text()
    scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
    actual = "\n".join(function(ptrace, "auto " + name + "(") for name in (
        "is_wki_execve_proxy_wait", "is_wki_execve_proxy_handoff", "deferred_switch_is_sched_yield",
        "should_suppress_deferred_syscall_exit_stop"))
    oracle = function(scheduler, "inline auto deferred_switch_is_sched_yield(")
    with tempfile.TemporaryDirectory(prefix="wos-ptrace-yield-") as temp:
        cpp, executable = Path(temp) / "test.cpp", Path(temp) / "test"
        cpp.write_text(PRELUDE + "#include <initializer_list>\n" + actual
                       + "\nnamespace scheduler_classifier {\n" + oracle + "\n}\n" + CASES)
        subprocess.run(["/usr/bin/c++", "-std=c++20", "-Wall", "-Wextra", "-Werror",
                        str(cpp), "-o", str(executable)], check=True)
        subprocess.run([str(executable)], check=True)


if __name__ == "__main__":
    main()
