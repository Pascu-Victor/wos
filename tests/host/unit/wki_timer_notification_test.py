#!/usr/bin/env python3
"""Compile the real WKI timer loop, notification and wait with boundary hooks.

Instrumented atomics inject a producer at sleep publication/check boundaries.
Scheduler stubs retain wake-before-sleep, matching kern_sleep_us's pending wake
contract; this checks timer ordering, not the scheduler implementation itself.
"""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source: str, name: str) -> str:
    match = re.search(rf"^(?:\[\[noreturn\]\] )?void {name}\([^)]*\) \{{", source, re.M)
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
struct Finish {};
enum class Boundary { None, Tick, Fast, Event, Periodic, Arm, WaitTime, WaitCheck, Park };
Boundary boundary = Boundary::None;
bool injected = false, connected = true, pending_wake = false;
unsigned ticks = 0, time_reads = 0, armed_reads = 0, wakes = 0, attempts = 0, parks = 0;
uint64_t now = 1000, slept = 0, fast = UINT64_MAX, event = UINT64_MAX, periodic = UINT64_MAX;
constexpr uint64_t WKI_TIMER_CONNECTED_MAINTENANCE_US = 500000;
constexpr uint64_t WKI_TIMER_IDLE_SLEEP_US = 1000000, WKI_TIMER_OVERDUE_BACKOFF_US = 1000;
void wki_timer_notify();
void inject(Boundary here) {
    if (boundary == here && !injected) { injected = true; wki_timer_notify(); }
}
struct Armed {
    std::atomic<bool> value{false};
    void store(bool next, std::memory_order order) {
        value.store(next, order);
        if (next) { armed_reads = 0; inject(Boundary::Arm); }
    }
    bool exchange(bool next, std::memory_order order) { return value.exchange(next, order); }
} s_wki_timer_sleep_armed;
struct Sequence {
    std::atomic<uint32_t> value{0};
    uint32_t load(std::memory_order order) {
        auto result = value.load(order);
        if (s_wki_timer_sleep_armed.value.load() && ++armed_reads == 2) {
            // Producer runs after wait_until's final sequence read, before sleep.
            inject(Boundary::WaitCheck);
        }
        return result;
    }
    uint32_t fetch_add(uint32_t amount, std::memory_order order) { return value.fetch_add(amount, order); }
} s_wki_timer_notify_seq;
namespace mod::time {
uint64_t get_us() { if (++time_reads == 3) inject(Boundary::WaitTime); return now; }
}
namespace mod::sched {
namespace task { struct Task {}; }
void kern_wake(task::Task*) { ++wakes; pending_wake = true; }
void kern_sleep_us(uint64_t delay) {
    ++attempts;
    inject(Boundary::Park);
    if (pending_wake) { pending_wake = false; return; }
    ++parks; slept += delay; now += delay;
}
}
mod::sched::task::Task task;
mod::sched::task::Task* s_wki_timer_task = &task;
void wki_peer_timer_tick(uint64_t) {
    if (++ticks == 2) throw Finish{};
    inject(Boundary::Tick);
}
uint64_t wki_next_fast_timer_deadline_us(uint64_t) { inject(Boundary::Fast); return fast; }
uint64_t wki_event_next_timer_deadline_us(uint64_t) { inject(Boundary::Event); return event; }
uint64_t wki_next_periodic_deadline_us(uint64_t) { inject(Boundary::Periodic); return periodic; }
bool wki_has_connected_peers() { return connected; }
uint64_t wki_future_deadline_us(uint64_t time, uint64_t delay) { return time + delay; }
'''

CASES = r'''
void reset(Boundary at = Boundary::None) {
    boundary = at; injected = pending_wake = false; connected = true;
    ticks = time_reads = armed_reads = wakes = attempts = parks = 0;
    now = 1000; slept = 0; fast = event = periodic = UINT64_MAX;
    s_wki_timer_sleep_armed.value.store(false); s_wki_timer_notify_seq.value.store(0);
    s_wki_timer_task = &task;
}
void run() { try { wki_timer_thread(); } catch (Finish&) {} assert(ticks == 2); }
int main() {
    for (auto at : {Boundary::Fast, Boundary::Event, Boundary::Periodic, Boundary::Arm,
                    Boundary::WaitTime, Boundary::WaitCheck, Boundary::Park}) {
        reset(at); run();
        assert(injected && parks == 0 && slept == 0);
        assert(!s_wki_timer_sleep_armed.value.load());
        if (at == Boundary::WaitCheck || at == Boundary::Park) assert(attempts == 1 && wakes == 1);
    }
    // Earlier tick notifications are coalesced, so the serviced iteration sleeps.
    reset(Boundary::Tick); run(); assert(injected && parks == 1 && slept == 500000 && wakes == 0);
    reset(); run(); assert(parks == 1 && slept == 500000);
    reset(); connected = false; run(); assert(parks == 1 && slept == 1000000);
    for (unsigned which = 0; which != 3; ++which) {
        reset(); (which == 0 ? fast : which == 1 ? event : periodic) = now + 17;
        run(); assert(parks == 1 && slept == 17);
    }
    reset(); fast = now; run(); assert(parks == 1 && slept == 1000);
    reset(); event = now - 1; run(); assert(parks == 1 && slept == 1000);
    reset(); fast = now + 20; event = now + 30; periodic = now + 10;
    run(); assert(slept == 10);
    reset(Boundary::Periodic); s_wki_timer_notify_seq.value.store(UINT32_MAX);
    run(); assert(injected && parks == 0 && s_wki_timer_notify_seq.value.load() == 0);
    // Coalescing still advances the sequence for every producer; only one wake.
    reset(); s_wki_timer_sleep_armed.value.store(true);
    wki_timer_notify(); wki_timer_notify(); wki_timer_notify();
    assert(wakes == 1 && s_wki_timer_notify_seq.value.load() == 3);
    reset(); s_wki_timer_task = nullptr; wki_timer_notify();
    assert(wakes == 0 && s_wki_timer_notify_seq.value.load() == 1);
    std::puts("PASS WKI timer scan/arm/wait/park notifications, coalescing, deadlines and sequence wrap");
}
'''


def main() -> None:
    source = (ROOT / "modules/kern/src/net/wki/peer.cpp").read_text()
    compiler = os.environ.get("HOST_CXX") or shutil.which("c++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    code = PRELUDE + "\n".join(function(source, name) for name in
                              ("wki_timer_wait_until", "wki_timer_notify", "wki_timer_thread")) + CASES
    with tempfile.TemporaryDirectory(prefix="wos-wki-timer-") as directory:
        harness, binary = Path(directory) / "timer.cpp", Path(directory) / "timer"
        harness.write_text(code)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror",
                        str(harness), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=10)


if __name__ == "__main__":
    main()
