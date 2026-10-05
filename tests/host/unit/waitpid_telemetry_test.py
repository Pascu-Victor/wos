#!/usr/bin/env python3
"""Run the actual waitpid continuation against controlled event/park hooks."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source: str) -> str:
    match = re.search(r"^auto wos_proc_waitpid\([^)]*\) -> uint64_t \{", source, re.M)
    assert match, "missing waitpid continuation"
    end, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


PRELUDE = r'''
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <vector>
#define WOS_PERF_CALLSITE() uint64_t{123}
namespace ker::mod::cpu { struct GPRegs {}; unsigned current_cpu(); }
namespace ker::mod::sched::task {
enum class WaitChannelKind { WAITPID };
struct Task {
    uint64_t pid = 11;
    unsigned releases = 0;
    bool interrupt = false;
    void release() { ++releases; }
    bool has_interrupting_signal_pending() { return interrupt; }
};
}
namespace sched_task = ker::mod::sched::task;
sched_task::Task waiter, owner;
bool have_waiter, have_owner, writable, valid_selector, copied, committed;
bool recording, scope_enabled, registration, observed_nowait, drop_during_park;
unsigned reads, correlations, summaries, cancels, claim_releases, commits, probes, parks, cpu;
uint64_t now_us, park_advance, returned_pid;
std::optional<uint64_t> remote;
enum class Mode { CLAIM, NONE, BLOCK } mode;
namespace ker::mod::time { uint64_t get_us() { ++reads; return now_us; } }
namespace ker::mod::cpu { unsigned current_cpu() { return ::cpu; } }
namespace ker::mod::perf {
enum class WkiPerfScope { LOCAL_PROC };
enum class WkiPerfLocalProcOp : uint8_t { WAITPID = 14 };
enum class WkiPerfPhase { BEGIN, POINT, END };
struct Event { uint64_t pid; WkiPerfPhase phase; uint32_t corr; int32_t status; uint32_t aux, cpu; };
std::vector<Event> emitted;
bool is_wki_scope_recording_enabled(WkiPerfScope, uint8_t op) {
    assert(op == 14); return recording && scope_enabled;
}
uint32_t next_wki_trace_correlation() { return ++correlations; }
void record_wki_event(uint32_t c, uint64_t pid, WkiPerfScope, uint8_t op, WkiPerfPhase phase,
                      uint16_t peer, uint16_t channel, uint32_t corr, int32_t status,
                      uint32_t aux, uint64_t callsite) {
    assert(op == 14 && peer == 0 && channel == 0 && callsite == 123);
    if (recording && scope_enabled) emitted.push_back({pid, phase, corr, status, aux, c});
}
void record_wki_summary(WkiPerfScope, uint8_t op, uint16_t peer, uint16_t channel,
                        int32_t status, uint32_t duration, bool latency, uint32_t retries, uint64_t bytes) {
    assert(op == 14 && peer == 0 && channel == 0 && latency && retries == 0 && bytes == 0);
    if (recording && scope_enabled) {
        assert(emitted.back().status == status && emitted.back().aux == duration);
        ++summaries;
    }
}
}
namespace perf = ker::mod::perf;
namespace ker::mod::sched {
sched_task::Task* get_current_task() { return have_waiter ? &waiter : nullptr; }
void preemptible_syscall_park(const char*, task::WaitChannelKind) {
    assert(registration && owner.releases == 0 && cancels == 0 && parks == 0);
    ++parks; now_us += park_advance; ::cpu = 7;
    if (drop_during_park) recording = false;
}
}
namespace events {
constexpr int32_t OPTION_NOHANG = 1, OPTION_NOWAIT = 0x1000000;
struct Selector {};
struct ClaimedEvent { uint64_t subject_pid; };
enum class ProbeResult { CLAIMED, WOULD_BLOCK, NO_CHILD };
sched_task::Task* acquire_process_owner(sched_task::Task&) { return have_owner ? &owner : nullptr; }
ProbeResult claim_or_register(sched_task::Task&, sched_task::Task&, Selector, int32_t,
                              bool block, ClaimedEvent& claimed) {
    ++probes; registration = block;
    if (mode == Mode::NONE) return ProbeResult::NO_CHILD;
    if (mode == Mode::BLOCK && parks == 0) return ProbeResult::WOULD_BLOCK;
    claimed.subject_pid = returned_pid; return ProbeResult::CLAIMED;
}
void release_claim(sched_task::Task&, ClaimedEvent&) { ++claim_releases; }
void cancel_wait(sched_task::Task&) { ++cancels; registration = false; }
bool commit_claim(sched_task::Task&, sched_task::Task&, ClaimedEvent&, bool nowait) {
    ++commits; observed_nowait = nowait; return committed;
}
}
bool outputs_writable(sched_task::Task&, int32_t*, uint64_t) { return writable; }
std::optional<uint64_t> wait_remote(int64_t, sched_task::Task&, sched_task::Task&, int32_t*, int32_t, uint64_t) {
    return remote;
}
bool wait_selector(int64_t, sched_task::Task&, events::Selector&) { return valid_selector; }
bool copy_outputs(sched_task::Task&, int32_t*, uint64_t, events::ClaimedEvent&) { return copied; }
'''

CASES = r'''
void reset() {
    waiter = {}; owner = {}; owner.pid = 99;
    have_waiter = have_owner = writable = valid_selector = copied = committed = true;
    recording = scope_enabled = true;
    registration = observed_nowait = drop_during_park = false;
    reads = correlations = summaries = cancels = claim_releases = commits = probes = parks = 0;
    cpu = 2; now_us = 100; park_advance = 250; returned_pid = 42;
    remote.reset(); mode = Mode::CLAIM; perf::emitted.clear();
}
void check(int64_t expected, int32_t options = 0, bool span = true, uint32_t duration = 0) {
    ker::mod::cpu::GPRegs regs;
    assert(wos_proc_waitpid(-1, nullptr, options, 0, regs) == static_cast<uint64_t>(expected));
    if (!span) {
        assert(reads == 0 && correlations == 0 && summaries == 0 && perf::emitted.empty());
        return;
    }
    assert(reads == 2 && correlations == 1 && summaries == 1);
    assert(perf::emitted.size() == (expected > 0 ? 3U : 2U));
    const auto& begin = perf::emitted.front(); const auto& end = perf::emitted.back();
    assert(begin.phase == perf::WkiPerfPhase::BEGIN && end.phase == perf::WkiPerfPhase::END);
    assert(begin.pid == waiter.pid && end.pid == waiter.pid && begin.corr == end.corr);
    assert(begin.status == 0 && begin.aux == uint32_t(options) && begin.cpu == 2);
    assert(end.status == (expected < 0 ? expected : 0) && end.aux == duration && end.cpu == cpu);
    if (expected > 0) {
        const auto& point = perf::emitted.at(1);
        assert(point.phase == perf::WkiPerfPhase::POINT && point.pid == uint64_t(expected));
        assert(point.corr == end.corr && point.status == 0 && point.aux == 0 && point.cpu == cpu);
    }
}
int main() {
    reset(); have_waiter = false; check(-ESRCH, 0, false); assert(owner.releases == 0);
    reset(); writable = false; check(-EFAULT); assert(owner.releases == 0 && probes == 0);
    reset(); have_owner = false; check(-ECHILD); assert(owner.releases == 0);
    reset(); valid_selector = false; check(-EINVAL); assert(owner.releases == 1 && probes == 0);
    for (int64_t value : {int64_t(42), int64_t(0), int64_t(-EINTR), int64_t(-ETIMEDOUT)}) {
        reset(); remote = static_cast<uint64_t>(value); check(value);
        assert(owner.releases == 1 && probes == 0 && cancels == 0);
    }
    reset(); copied = false; check(-EFAULT);
    assert(claim_releases == 1 && commits == 0 && cancels == 1 && owner.releases == 1);
    reset(); committed = false; check(-EINTR);
    assert(commits == 1 && cancels == 1 && owner.releases == 1);
    reset(); check(42, events::OPTION_NOWAIT);
    assert(observed_nowait && commits == 1 && cancels == 1 && owner.releases == 1);
    reset(); returned_pid = uint64_t{1} << 40; check(int64_t(returned_pid));
    reset(); mode = Mode::NONE; check(-ECHILD); assert(cancels == 1 && owner.releases == 1);
    reset(); mode = Mode::BLOCK; check(0, events::OPTION_NOHANG);
    assert(parks == 0 && commits == 0 && cancels == 1 && owner.releases == 1);
    reset(); mode = Mode::BLOCK; waiter.interrupt = true; check(-EINTR);
    assert(parks == 0 && cancels == 1 && owner.releases == 1);
    reset(); mode = Mode::BLOCK; check(42, 0, true, 250);
    assert(parks == 1 && probes == 2 && !registration && cancels == 1 && owner.releases == 1);
    reset(); mode = Mode::BLOCK; park_advance = uint64_t(UINT32_MAX) + 7; check(42, 0, true, UINT32_MAX);
    reset(); mode = Mode::BLOCK; park_advance = uint64_t(-1); check(42, 0, true, 0);
    for (bool filter_excluded : {false, true}) {
        reset(); recording = filter_excluded; scope_enabled = !filter_excluded;
        mode = Mode::BLOCK; check(42, 0, false);
        assert(parks == 1 && commits == 1 && cancels == 1 && owner.releases == 1);
    }
    // Recording may end while parked: the producer drops the end, preserving
    // execution. A later capture receipt must report the incomplete span.
    reset(); mode = Mode::BLOCK; drop_during_park = true;
    ker::mod::cpu::GPRegs regs;
    assert(wos_proc_waitpid(-1, nullptr, 0, 0, regs) == 42);
    assert(reads == 2 && perf::emitted.size() == 1 && summaries == 0 && owner.releases == 1);
    puts("waitpid telemetry: native return/claim/park/migration/filter checks pass");
}
'''


def main() -> None:
    compiler = shutil.which("c++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    source = (ROOT / "modules/kern/src/syscalls_impl/process/waitpid.cpp").read_text()
    with tempfile.TemporaryDirectory(prefix="wos-waitpid-telemetry-") as directory:
        cpp, binary = Path(directory) / "test.cpp", Path(directory) / "test"
        cpp.write_text(PRELUDE + function(source) + CASES)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror", str(cpp), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True)


if __name__ == "__main__":
    main()
