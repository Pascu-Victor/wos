#!/usr/bin/env python3
"""Exercise real preemption guards with and without expensive clock sampling."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source: str, name: str) -> str:
    match = re.search(rf"^(?:auto|void) {name}\([^)]*\)(?: -> [^{{\n]+)? \{{", source, re.M)
    assert match, f"missing {name}"
    end, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


PRELUDE = r'''
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <platform/sched/preemption_policy.hpp>
using namespace ker::mod::sched;
namespace ker::mod::sched::task { struct Task {
    uint32_t preempt_disable_depth = 0;
    uint64_t preempt_disable_start_us = 0, preempt_disable_max_us = 0;
    uint64_t preempt_disable_owner = 0;
    std::atomic<bool> scheduler_transition_active{false};
    bool deferred_task_switch = false, preempt_pending = false;
}; }
task::Task* current = nullptr;
unsigned reads = 0, reschedules = 0;
uint64_t now_us = 100;
namespace clock_stub { uint64_t get_us() { ++reads; return now_us; } }
// Avoid colliding with hosted ::time while retaining the actual function bodies.
#define time clock_stub
task::Task* current_task_for_preempt() { return current; }
struct Trap {};
[[noreturn]] void hcf() { throw Trap{}; }
void* run_queues = reinterpret_cast<void*>(1);
void request_local_reschedule() { ++reschedules; }
#ifdef WOS_PREEMPT_GUARD_TIMING
constexpr bool timing = true;
#else
constexpr bool timing = false;
#endif
'''

CASES = r'''
int main() {
    assert(preempt_disable_token_at(7) == nullptr);
    preempt_enable_token_at(nullptr, 7);
    assert(reads == 0 && reschedules == 0);

    task::Task a, b;
    current = &a;
    auto* owner = preempt_disable_token_at(7);
    assert(owner == &a && a.preempt_disable_depth == 1);
    assert(a.preempt_disable_owner == 7);
    assert(reads == unsigned(timing));
    assert(a.preempt_disable_start_us == (timing ? 100U : 0U));
    now_us = 125;
    assert(preempt_disable_token_at(9) == &a);
    assert(a.preempt_disable_depth == 2 && a.preempt_disable_owner == 7);
    assert(reads == unsigned(timing));
    a.preempt_pending = true;
    preempt_enable_token_at(owner, 9);
    assert(a.preempt_disable_depth == 1 && a.preempt_pending && reschedules == 0);

    // The saved token must release its owner even after current task changes.
    current = &b;
    now_us = 150;
    preempt_enable_token_at(owner, 7);
    assert(a.preempt_disable_depth == 0 && a.preempt_disable_owner == 0);
    assert(a.preempt_disable_start_us == 0 && a.preempt_disable_max_us == (timing ? 50U : 0U));
    assert(b.preempt_disable_depth == 0 && !a.preempt_pending && reschedules == 1);
    assert(reads == 2U * unsigned(timing));

    // Both return-transition forms preserve pending work until service is safe.
    current = &a;
    for (unsigned transition = 0; transition < 2; ++transition) {
        a.scheduler_transition_active.store(transition == 0);
        a.deferred_task_switch = transition == 1;
        owner = preempt_disable_token_at(11);
        a.preempt_pending = true;
        preempt_enable_token_at(owner, 11);
        assert(a.preempt_pending && reschedules == 1);
    }
    a.scheduler_transition_active.store(false);
    a.deferred_task_switch = false;
    owner = preempt_disable_token_at(12);
    preempt_enable_token_at(owner, 12);
    assert(!a.preempt_pending && reschedules == 2);

    // A pending guard does not try to reschedule before runqueues exist.
    run_queues = nullptr;
    owner = preempt_disable_token_at(13);
    a.preempt_pending = true;
    preempt_enable_token_at(owner, 13);
    assert(!a.preempt_pending && reschedules == 2);

    // Timing must retain the prior maximum and reject a backwards sample.
    now_us = 300;
    owner = preempt_disable_token_at(14);
    now_us = 299;
    preempt_enable_token_at(owner, 14);
    assert(a.preempt_disable_start_us == 0 && a.preempt_disable_max_us == (timing ? 50U : 0U));
    unsigned const before_error = reads;
    try { preempt_enable_token_at(&a, 15); assert(false); } catch (Trap const&) {}
    assert(a.preempt_disable_depth == 0 && reads == before_error);
    a.preempt_disable_depth = UINT32_MAX;
    try { preempt_disable_token_at(15); assert(false); } catch (Trap const&) {}
    assert(a.preempt_disable_depth == UINT32_MAX && reads == before_error);
    std::puts(timing ? "PASS sampled guard durations and unchanged safety transitions"
                     : "PASS zero clock reads and unchanged guard safety transitions");
}
'''


def main() -> None:
    source = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
    source = re.sub(r"\[\[clang::no_sanitize\([^\n]*?\)\]\]\s*", "", source)
    compiler = os.environ.get("HOST_CXX") or shutil.which("c++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    code = PRELUDE + "\n".join(function(source, name) for name in
                              ("preempt_disable_token_at", "preempt_enable_token_at")) + CASES
    with tempfile.TemporaryDirectory(prefix="wos-preempt-timing-") as directory:
        harness = Path(directory) / "guard.cpp"
        harness.write_text(code)
        for timing in (False, True):
            binary = Path(directory) / f"guard-{int(timing)}"
            flags = ["-DWOS_PREEMPT_GUARD_TIMING=1"] if timing else []
            subprocess.run([compiler, "-std=c++23", "-Wall", "-Wextra", "-Werror", *flags,
                            "-I", str(ROOT / "modules/kern/src"), str(harness), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True, timeout=10)


if __name__ == "__main__":
    main()
