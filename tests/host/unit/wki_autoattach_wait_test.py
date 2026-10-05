#!/usr/bin/env python3
"""Exercise real WKI wait/guard/slot code with controlled completion boundaries.

Scheduler hooks model the existing pending-wake contract using the actual
scheduler cancellation predicate. This tests wait-role selection and ownership,
not host emulation of the kernel scheduler or network stack.
"""

import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def block(source: str, pattern: str) -> str:
    match = re.search(pattern, source, re.M)
    assert match, f"missing {pattern}"
    end, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


def function(source: str, name: str) -> str:
    return block(source, rf"^(?:\[\[nodiscard\]\] |inline )?(?:auto|void) {name}\([^{{;]*\{{")


PRELUDE = r'''
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
namespace mod::sched::task {
enum class TaskType { DAEMON, PROCESS, IDLE };
enum class WaitChannelKind { GENERIC, WKI_EXECVE_PROXY };
struct Task {
    TaskType type = TaskType::DAEMON;
    uint64_t syscall_account_start_us = 0, wki_proxy_task_id = 0, wake_at_us = 0;
    bool deferred_task_switch = false, wants_block = false, voluntary = false, signal = false;
    unsigned refs = 0;
    WaitChannelKind channel = WaitChannelKind::GENERIC;
    std::atomic<bool> wakeup_pending{false};
    bool is_voluntary_blocked() const { return voluntary; }
    bool has_interrupting_signal_pending() const { return signal; }
    bool wait_channel_is(WaitChannelKind kind) const { return channel == kind; }
    void set_wait_channel(const char*) { channel = WaitChannelKind::GENERIC; }
    bool try_acquire() { ++refs; return true; }
    void release() { assert(refs); --refs; }
};
}
using Task = mod::sched::task::Task;
Task client, service, ordinary;
Task* current = &client;
unsigned depth = 0;
namespace mod::sched {
Task* get_current_task() { return current; }
unsigned preempt_count() { return depth; }
enum class EventWakeDeferredSwitch { PRESERVE };
enum class SchedulerHaltWaitKind : uint8_t { YIELD, BLOCK, PROCESS_PARK };
void kern_block();
void kern_yield();
void kern_sleep_us(uint64_t);
void preemptible_syscall_park(const char*, uint64_t);
void wake_task_from_event(Task*, EventWakeDeferredSwitch);
}
namespace mod::dbg { template<class... Args> void log(const char*, Args...) { assert(false); } }
namespace ker { namespace mod = ::mod; }
std::atomic<bool> s_timer_deferred_running{false}, s_auto_attach_deferred_running{false};
std::atomic<Task*> s_timer_deferred_task{nullptr}, s_auto_attach_deferred_task{nullptr};
struct Lock {
    bool held = false;
    void lock() { assert(!held); held = true; }
    void unlock() { assert(held); held = false; }
} s_wait_lock;
constexpr int WKI_ERR_TIMEOUT = -5, WKI_ERR_PEER_FENCED = -3;
constexpr uint64_t WKI_WAIT_DIAG_FIRST_US = 500000, WKI_WAIT_DIAG_INTERVAL_US = 1000000;
constexpr uint64_t VFS_PROXY_CONTENTION_SLEEP_US = 1000;
uint64_t now_us = 100;
bool wait_diag_enabled() { return false; }
uint64_t wki_now_us() { return now_us; }
uint64_t wki_future_deadline_us(uint64_t now, uint64_t delay) { return now + delay; }
void wki_timer_notify();
void wki_spin_yield();
void wki_wait_timeout_scan(uint64_t);
void wki_finish_claimed_op(struct WkiWaitEntry*, int);
bool wki_claim_op(struct WkiWaitEntry*);
void wki_wake_op(struct WkiWaitEntry*, int);
void log_wait_diag(const char*, const struct WkiWaitEntry*, uint64_t, uint64_t, uint64_t) {}
struct ProxySlotCaller { uint64_t pid = 1; mod::sched::task::TaskType type; };
'''

HOOKS = r'''
WkiWaitEntry* s_wait_head = nullptr;
WkiWaitEntry* active = nullptr;
enum class Mode { ACK, BEFORE_BLOCK, SPURIOUS, TIMER_TIMEOUT, LOCAL_TIMEOUT, CLAIMED, SERVICE };
Mode mode = Mode::ACK;
unsigned blocks, parks, yields, spins, timer_notifies, wakes, process_parks, sleeps;
uint64_t slept;
void wki_timer_notify() { ++timer_notifies; }
void wki_spin_yield() { ++spins; }
namespace mod::sched {
void wake_task_from_event(Task* target, EventWakeDeferredSwitch policy) {
    assert(policy == EventWakeDeferredSwitch::PRESERVE && target && target->refs);
    ++wakes; target->wakeup_pending.store(true);
}
void kern_block() {
    assert(!s_wait_lock.held && current->type == task::TaskType::DAEMON && depth == 0);
    assert(active && active->task.load() == current);
    ++blocks;
    current->wants_block = true;
    if (mode == Mode::BEFORE_BLOCK) wki_wake_op(active, 42);
    if (mode == Mode::SPURIOUS && blocks == 1) current->wakeup_pending.store(true);
    if (scheduler_wait_should_cancel(current, SchedulerHaltWaitKind::BLOCK)) {
        current->wants_block = false;
        return;
    }
    ++parks;
    if (mode == Mode::TIMER_TIMEOUT) {
        now_us = active->deadline_us;
        wki_wait_timeout_scan(now_us);
        assert(active->next == nullptr && active->prev == nullptr && s_wait_head == nullptr);
    } else if (mode == Mode::LOCAL_TIMEOUT) {
        now_us = active->deadline_us;
        current->wakeup_pending.store(true);
    } else if (mode == Mode::CLAIMED) {
        assert(active->state.load() == WkiWaitEntry::CLAIMED);
        now_us += 50000;
        wki_finish_claimed_op(active, 77);
    } else {
        now_us += 50000;
        wki_wake_op(active, 42);
    }
    assert(scheduler_wait_should_cancel(current, SchedulerHaltWaitKind::BLOCK));
    current->wants_block = false;
}
void kern_yield() {
    assert(!s_wait_lock.held);
    ++yields;
    if (active) {
        now_us += 1;
        if (active->state.load() == WkiWaitEntry::CLAIMED) wki_finish_claimed_op(active, 77);
        else wki_wake_op(active, 42);
    }
}
void preemptible_syscall_park(const char*, uint64_t deadline) {
    assert(!s_wait_lock.held && current->type == task::TaskType::PROCESS);
    ++process_parks;
    if (active) { assert(deadline == active->deadline_us); now_us += 50000; wki_wake_op(active, 42); }
    else slept = deadline - now_us;
}
void kern_sleep_us(uint64_t duration) { assert(!s_wait_lock.held); ++sleeps; slept = duration; }
}
'''

CASES = r'''
void reset(Task* task = &client, Mode selected = Mode::ACK) {
    assert(!s_wait_lock.held && s_wait_head == nullptr);
    current = task; depth = 0; now_us = 100; active = nullptr; mode = selected;
    blocks = parks = yields = spins = timer_notifies = wakes = process_parks = sleeps = 0;
    slept = 0;
    for (auto* t : {&client, &service, &ordinary}) {
        assert(t->refs == 0);
        t->type = mod::sched::task::TaskType::DAEMON;
        t->syscall_account_start_us = t->wki_proxy_task_id = t->wake_at_us = 0;
        t->deferred_task_switch = t->wants_block = t->voluntary = t->signal = false;
        t->channel = mod::sched::task::WaitChannelKind::GENERIC;
        t->wakeup_pending.store(false);
    }
    s_timer_deferred_running.store(false); s_auto_attach_deferred_running.store(false);
    s_timer_deferred_task.store(nullptr); s_auto_attach_deferred_task.store(nullptr);
}
void auto_wait(Mode selected, int expected, unsigned expected_blocks, unsigned expected_parks) {
    reset(&client, selected);
    AutoAttachDeferredGuard guard; assert(guard.try_enter());
    WkiWaitEntry entry{}; active = &entry;
    if (selected == Mode::CLAIMED) { assert(wki_claim_op(&entry)); wki_wake_op(&entry, -99); }
    assert(wki_wait_for_op(&entry, 60000000) == expected);
    assert(blocks == expected_blocks && parks == expected_parks && yields == 0 && spins == 0);
    assert(entry.state.load() == WkiWaitEntry::DONE && s_wait_head == nullptr);
    assert(entry.next == nullptr && entry.prev == nullptr && client.refs == 0);
    assert(timer_notifies == 1);
}
int main() {
    auto_wait(Mode::ACK, 42, 1, 1);
    auto_wait(Mode::BEFORE_BLOCK, 42, 1, 0);
    auto_wait(Mode::SPURIOUS, 42, 2, 1);
    auto_wait(Mode::TIMER_TIMEOUT, WKI_ERR_TIMEOUT, 1, 1);
    auto_wait(Mode::LOCAL_TIMEOUT, WKI_ERR_TIMEOUT, 1, 1);
    auto_wait(Mode::CLAIMED, 77, 1, 1);
    reset();
    { AutoAttachDeferredGuard guard, nested; assert(guard.try_enter()); assert(!nested.try_enter());
      assert(s_auto_attach_deferred_task.load() == &client); assert(!wki_current_wait_must_drive_progress()); }
    assert(!s_auto_attach_deferred_running.load() && s_auto_attach_deferred_task.load() == nullptr);
    reset(&service);
    { DeferredGuard guard, nested; assert(guard.try_enter()); assert(!nested.try_enter());
      assert(wki_current_wait_must_drive_progress());
      WkiWaitEntry entry{}; active = &entry; assert(wki_wait_for_op(&entry, 60000000) == 42);
      assert(blocks == 0 && parks == 0 && spins == 1 && yields == 1); }
    assert(!s_timer_deferred_running.load() && s_timer_deferred_task.load() == nullptr);
    reset(&ordinary);
    { WkiWaitEntry entry{}; active = &entry; assert(wki_wait_for_op(&entry, 100000) == 42); assert(parks == 1); }
    reset();
    { AutoAttachDeferredGuard guard; assert(guard.try_enter()); WkiWaitEntry entry{}; active = &entry;
      wki_wake_op(&entry, 91); assert(wki_wait_for_op(&entry, 60000000) == 91);
      assert(blocks == 0 && timer_notifies == 0 && wakes == 0 && s_wait_head == nullptr);
      wki_wake_op(&entry, -99); assert(entry.result == 91); }
    reset();
    { AutoAttachDeferredGuard guard; assert(guard.try_enter()); WkiWaitEntry entry{}; active = &entry;
      wki_wake_op(&entry, WKI_ERR_PEER_FENCED); assert(wki_wait_for_op(&entry, 60000000) == WKI_ERR_PEER_FENCED);
      assert(blocks == 0); }
    // Keep all existing PROCESS syscall eligibility exclusions.
    reset(); client.type = mod::sched::task::TaskType::PROCESS; client.syscall_account_start_us = 1;
    assert(wki_current_process_wait_can_park());
    { WkiWaitEntry entry{}; active = &entry; assert(wki_wait_for_op(&entry, 100000) == 42);
      assert(process_parks == 1 && blocks == 0 && yields == 0); }
    for (unsigned which = 0; which != 8; ++which) {
        reset(); client.type = mod::sched::task::TaskType::PROCESS; client.syscall_account_start_us = 1;
        if (which == 0) client.syscall_account_start_us = 0;
        if (which == 1) client.deferred_task_switch = true;
        if (which == 2) client.wants_block = true;
        if (which == 3) client.voluntary = true;
        if (which == 4) client.wki_proxy_task_id = 99;
        if (which == 5) client.channel = mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY;
        if (which == 6) depth = 1;
        if (which == 7) client.signal = true;
        assert(!wki_current_process_wait_can_park());
    }
    reset(); current = nullptr; assert(!wki_current_process_wait_can_park());
    assert(!wki_current_wait_must_drive_progress());
    reset(&service); service.type = mod::sched::task::TaskType::PROCESS; service.syscall_account_start_us = 1;
    { DeferredGuard guard; assert(guard.try_enter()); assert(!wki_current_process_wait_can_park()); }
    // The exported role must also permit the independent autoattach slot waiter to sleep.
    for (bool registered : {false, true}) {
        reset(); AutoAttachDeferredGuard guard; assert(guard.try_enter());
        park_proxy_slot_caller({1, client.type}, now_us + 5000, registered);
        assert(sleeps == 1 && slept == (registered ? 5000 : 1000) && spins == 0 && yields == 0);
    }
    reset(&service);
    { DeferredGuard guard; assert(guard.try_enter()); park_proxy_slot_caller({1, service.type}, now_us + 5000, true);
      assert(spins == 1 && yields == 1 && sleeps == 0); }
    reset(); park_proxy_slot_caller({1, client.type}, now_us, true); assert(sleeps == 0);
    std::puts("PASS actual WKI autoattach ACK/block races, timeout, claim/fence, guards, process exclusions and VFS slot roles");
}
'''


def main() -> None:
    source = Path(sys.argv[1]).read_text() if len(sys.argv) > 1 else (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    header = (ROOT / "modules/kern/src/net/wki/wki.hpp").read_text()
    scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.hpp").read_text()
    vfs = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    wait_entry = block(header, r"^struct WkiWaitEntry \{") + ";\n"
    # Diagnostic fields use only a node constant from wire.hpp.
    code = PRELUDE + "constexpr uint16_t WKI_NODE_INVALID = 0;\n" + wait_entry
    code += "namespace mod::sched {\n" + function(scheduler, "scheduler_wait_should_cancel") + "\n}\n"
    code += HOOKS
    code += block(source, r"^struct DeferredGuard \{") + ";\n"
    code += block(source, r"^struct AutoAttachDeferredGuard \{") + ";\n"
    for name in ("is_timer_deferred_waiter", "wki_current_wait_must_drive_progress",
                 "wait_list_link", "wait_list_unlink", "wait_state", "wait_done",
                 "claim_wait_entry", "publish_claimed_wait_entry", "process_wait_can_park",
                 "wki_current_process_wait_can_park", "wki_wait_for_op", "wki_wake_op",
                 "wki_claim_op", "wki_finish_claimed_op", "wki_wait_timeout_scan"):
        code += function(source, name) + "\n"
    code += function(vfs, "park_proxy_slot_caller") + CASES
    compiler = os.environ.get("HOST_CXX") or shutil.which("c++", path=os.defpath)
    assert compiler, "host C++ compiler required"
    with tempfile.TemporaryDirectory(prefix="wos-autoattach-wait-") as directory:
        harness, binary = Path(directory) / "wait.cpp", Path(directory) / "wait"
        harness.write_text(code)
        subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror",
                        "-fsanitize=address,undefined", "-fno-omit-frame-pointer",
                        str(harness), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=10,
                       env={**os.environ, "ASAN_OPTIONS": "detect_leaks=0"})


if __name__ == "__main__":
    main()
