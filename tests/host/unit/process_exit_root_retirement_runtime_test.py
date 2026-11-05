#!/usr/bin/env python3
"""Compile actual exit/Task/GC/epoch bodies against controlled Host endpoints.

The reader is a real Host shared_mutex, not the kernel's striped MM lock. CR3,
IRQ state, registry/list ownership and final Task/stack cleanup are fixtures.
Actual GC selection/publisher/epoch and Task detach/quiesce bodies are exercised;
this is not GC queue/failure-fallback, arbitrary Task lifetime or VM proof.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import time

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
sys.path.insert(0, str(ROOT / 'tests/host/unit'))
from gc_epoch_progress_test import bounded_command, output_guard
from exec_trace_ownership_test import block, function

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <thread>
#include <vector>
thread_local bool inside_exit_helper = false, irq_enabled = false;
std::atomic<unsigned> helper_allocations{0};
__attribute__((noinline)) void* operator new(std::size_t size) {
    if (inside_exit_helper) ++helper_allocations;
    if (void* value = std::malloc(size ? size : 1)) return value;
    throw std::bad_alloc();
}
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }
namespace ker::mod::cpu { uint64_t current_cpu() { return 0; } }
namespace ker::mod::smt {
uint64_t get_core_count() { return 2; }
uint64_t get_cpu_index_from_apic_id(uint64_t id) { return id; }
}
namespace ker::mod::apic { uint64_t get_apic_id() { return 0; } }
namespace ker::mod::dbg { template<class... Args> void log(const char*, Args...) { std::abort(); } }
// Only private visibility is exposed for deterministic epoch initialization.
EPOCH_HEADER
EPOCH_SOURCE
namespace ker::mod::mm::paging { struct PageTable {}; }
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, EXITING, DEAD };
struct Task {
    TaskType type = TaskType::PROCESS;
    bool is_thread = false, has_exited = true, waitable = true;
    std::atomic<TaskState> state{TaskState::EXITING};
    std::atomic<uint64_t> death_epoch{40};
    std::atomic<uint32_t> ref_count{2}, usercopy_pagemap_accesses{0};
    std::atomic<bool> shares_user_pagemap{false}, usercopy_pagemap_closing{false};
    std::atomic<bool> zombie_resources_reclaiming{false}, zombie_resources_reclaimed{false};
    mm::paging::PageTable* pagemap{};
    Task* sched_next{};
    void* thread{};
    uint64_t pid = 91;
    const char* name = "exit-fixture";
    void quiesce_usercopy_pagemap();
    auto detach_pagemap_after_usercopy_quiescence() -> mm::paging::PageTable*;
};
QUIESCE
DETACH
}
using Task = ker::mod::sched::task::Task;
using Root = ker::mod::mm::paging::PageTable;
Root* const root = reinterpret_cast<Root*>(0xffff800000012000ULL);
std::shared_mutex root_reader_gate;
std::mutex event_mutex;
std::condition_variable event_cv;
bool helper_entered = false, helper_returned = false, destroy_entered = false;
bool observed_late_drain = false;
std::atomic<unsigned> switches{0}, destroys{0}, releases{0}, file_releases{0};
unsigned final_task_cleanups = 0, zombie_resource_cleanups = 0, checks = 0, failures = 0;
void check(bool value, const char* name) {
    ++checks;
    if (!value) { ++failures; std::fprintf(stderr, "EXIT_RETIREMENT_FAILURE %s\n", name); }
}
namespace ker::mod::mm::virt {
void switch_to_kernel_pagemap() { ++switches; }
void destroy_user_space(Root* value, uint64_t, const char*, const char*) {
    assert(value == root);
    { std::lock_guard lock(event_mutex); destroy_entered = true; }
    event_cv.notify_all();
    std::unique_lock lock(root_reader_gate);
    ++destroys;
}
void release_pagemap(Root* value, uint64_t) { assert(value == root); ++releases; }
}
namespace ker::syscall::vmem {
void release_file_mmap_ranges_for_pagemap(Root* value) { assert(value == root); ++file_releases; }
}
namespace ker::syscall::process::child_events {
bool is_waitable_zombie(Task& task) { return task.waitable; }
}
namespace ker::mod::time { uint64_t get_us() { static uint64_t now = 100; return ++now; } }
namespace ker::mod::sched {
struct Lock {
    uint64_t lock_irqsave() { return irq_enabled; }
    void unlock_irqrestore(uint64_t flags) { irq_enabled = flags; }
} global_task_registry_lock;
std::vector<Task*> registry;
uint32_t active_task_count = 0;
Task* active_task_slot(uint32_t index) { return registry[index]; }
void active_list_remove(Task* task) {
    registry.erase(std::remove(registry.begin(), registry.end(), task), registry.end());
    active_task_count = registry.size();
}
struct List { Task* head{}; uint32_t count{}; };
struct RunQueue { List dead_list; Task* gc_reclaim_scan_cursor{}; Task* current_task{}; };
struct Queues {
    std::array<RunQueue, 2> queues;
    RunQueue* that_cpu(uint64_t id) { return &queues[id]; }
    template<class F> auto with_lock(uint64_t id, F fn) { return fn(that_cpu(id)); }
} storage;
Queues* run_queues = &storage;
struct GcPressureDiagnostics {
    unsigned not_dead_skips{}, epoch_skips{}, epoch_still_young{}, current_skips{};
    unsigned zombie_busy_skips{}, retained_zombie_skips{}, refcount_skips{}, invalid_tasks{};
};
GC_DETACHED
GC_TIMING
// These are controlled registry/list/lifetime endpoints, not extracted proof of
// the immediate-parent-reap race or scheduler's complete lifetime protocol.
bool try_withdraw_gc_task_visibility(Task& task) {
    if (task.ref_count.load() != 1) return false;
    active_list_remove(&task); return true;
}
void transition_detach_dead_locked(RunQueue* rq, Task* task, bool = false) {
    assert(rq->dead_list.head == task && rq->dead_list.count == 1);
    rq->dead_list = {}; rq->gc_reclaim_scan_cursor = nullptr; task->sched_next = nullptr;
}
REGISTERED_PUBLISHER
LIVE_PUBLISHER
GC_PUBLISHER
GC_SELECTION
inline uint64_t elapsed_us_since(uint64_t start, uint64_t end) { return end >= start ? end - start : 0; }
void finish_waitable_zombie_resource_cleanup(Task* task, GcTaskTiming&, uint64_t) {
    assert(task && task->pagemap == nullptr);
    task->zombie_resources_reclaimed.store(true); task->zombie_resources_reclaiming.store(false);
    ++zombie_resource_cleanups;
}
void cleanup_task_after_pagemap(Task* task, GcTaskTiming&, uint64_t) {
    assert(task && task->pagemap == nullptr); ++final_task_cleanups;
}
GC_ZOMBIE_CLEANUP
GC_FINAL_CLEANUP
}
namespace ker::syscall::process {
EXIT_HELPER
}
'''

CASES = r'''
using namespace ker::mod;
using namespace sched;
std::array<CpuEpoch, 2> epoch_states;
void reset() {
    assert(!inside_exit_helper);
    switches = destroys = releases = file_releases = helper_allocations = 0;
    final_task_cleanups = zombie_resource_cleanups = 0;
    helper_entered = helper_returned = destroy_entered = observed_late_drain = false;
    irq_enabled = false;
    registry.clear(); active_task_count = 0; storage.queues = {};
    EpochManager::cpu_epochs = epoch_states.data(); EpochManager::global_epoch.store(40);
    for (auto& state : epoch_states) { state.local_epoch.store(0); state.in_critical_section.store(false); }
}
void publish(Task& task) {
    registry.push_back(&task); active_task_count = registry.size();
    storage.queues[0].dead_list = {&task, 1}; storage.queues[0].gc_reclaim_scan_cursor = nullptr;
}
auto select(GcPressureDiagnostics& diagnostics) {
    uint64_t scanned = 0;
    return detach_next_reclaimable_task_locked(&storage.queues[0], 0, 1, &scanned, &diagnostics);
}
void call_exit(Task* task) { ker::syscall::process::SELECTED_EXIT_HELPER(task); }
int main() {
    reset(); Task subject; subject.pagemap = root;
    // Hold a real Host reader while the actual exit helper runs at EXITING/IFoff.
    // The old helper enters real exclusive mutex waiting in the MM endpoint.
    std::shared_lock reader(root_reader_gate);
    bool worker_irq = true;
    std::thread worker([&] {
        irq_enabled = false;
        { std::lock_guard lock(event_mutex); helper_entered = true; } event_cv.notify_all();
        inside_exit_helper = true; call_exit(&subject); inside_exit_helper = false;
        worker_irq = irq_enabled;
        { std::lock_guard lock(event_mutex); helper_returned = true; } event_cv.notify_all();
    });
    {
        std::unique_lock lock(event_mutex);
        bool signalled = event_cv.wait_for(lock, std::chrono::seconds(3), [] { return helper_entered && (helper_returned || destroy_entered); });
        check(signalled, "EXIT_HELPER_REACHED_FINITE_BOUNDARY");
        observed_late_drain = destroy_entered && !helper_returned;
        check(!observed_late_drain, "EXIT_TAIL_READER_DRAIN");
    }
    // Release only after observing whether the helper returned or tried drain.
    reader.unlock(); worker.join();
    check(subject.pagemap == root, "EXIT_ROOT_OWNER_RETAINED");
    check(switches == 1, "EXIT_CR3_SWITCH");
    check(!worker_irq, "EXIT_IRQ_OFF_PRESERVED");
    check(helper_allocations == 0, "EXIT_NO_ALLOCATION");
    check(destroys == 0 && releases == 0 && file_releases == 0, "EXIT_NO_INLINE_TEARDOWN");
    if (failures) {
        std::printf("EXIT_RETIREMENT_RESULT checks=%u failures=%u late_drain=%u\n", checks, failures, observed_late_drain);
        return 3;
    }
    // Actual epoch predicate AND actual GC selector refuse EXITING, young DEAD,
    // an old epoch reader and a CPU still publishing the dead Task.
    publish(subject); GcPressureDiagnostics d{};
    auto picked = select(d); check(!picked.task && d.not_dead_skips == 1, "GC_REFUSES_EXITING");
    subject.state.store(task::TaskState::DEAD);
    picked = select(d); check(!picked.task && d.epoch_still_young == 1, "GC_REFUSES_YOUNG_DEATH");
    EpochManager::enter_critical_for_cpu(1); EpochManager::advance_epoch();
    picked = select(d); check(!picked.task && d.epoch_skips == 2, "GC_REFUSES_OLD_EPOCH_READER");
    EpochManager::exit_critical_for_cpu(1); storage.queues[1].current_task = &subject;
    picked = select(d); check(!picked.task && d.current_skips == 1, "GC_REFUSES_CURRENT_CPU");
    storage.queues[1].current_task = nullptr;
    check(destroys == 0 && subject.pagemap == root, "NO_FREE_BEFORE_GC_HANDOFF");
    picked = select(d); check(picked.task == &subject && picked.should_free_pagemap && picked.zombie_resources_only, "UNREAPED_ZOMBIE_RESOURCE_HANDOFF");
    GcTaskTiming timing{}; cleanup_waitable_zombie_resources(picked, timing);
    check(subject.pagemap == nullptr && subject.usercopy_pagemap_closing.load() && subject.zombie_resources_reclaimed.load(), "ACTUAL_TASK_DETACH_AND_ZOMBIE_FINISH");
    check(destroys == 1 && releases == 1 && file_releases == 1 && !final_task_cleanups && subject.waitable && subject.ref_count.load() == 2, "UNREAPED_STATUS_RETAINED_ROOT_FREED_ONCE");

    // A registered EXITING publisher and then a DEAD dead-list-only publisher
    // both retain a shared root. Actual nonlast detach must leave the peer root.
    reset(); Task first, last; first.pagemap = last.pagemap = root;
    first.shares_user_pagemap.store(true); last.shares_user_pagemap.store(true);
    first.state.store(task::TaskState::DEAD); last.state.store(task::TaskState::EXITING);
    publish(first); registry.push_back(&last); active_task_count = registry.size(); EpochManager::advance_epoch();
    check(gc_task_has_pagemap_sibling_locked(&first), "REGISTERED_EXITING_PUBLISHER_VISIBLE");
    last.state.store(task::TaskState::DEAD); registry.pop_back(); active_task_count = registry.size(); storage.queues[1].dead_list = {&last, 1};
    check(gc_task_has_pagemap_sibling_locked(&first), "DEAD_LIST_PUBLISHER_VISIBLE");
    picked = select(d); check(picked.task == &first && !picked.should_free_pagemap, "NONLAST_OWNER_HANDOFF");
    cleanup_waitable_zombie_resources(picked, timing);
    check(!first.pagemap && last.pagemap == root && !destroys && !releases, "NONLAST_DETACH_NO_FREE");
    // Last publisher is a user thread: final teardown is independent of leader.
    storage.queues[1].dead_list = {}; publish(last); last.waitable = false; last.is_thread = true; last.ref_count.store(2);
    picked = select(d); check(!picked.task && d.refcount_skips > 0, "CONTROLLED_REAP_LIFETIME_PIN_RETAINS_TASK");
    last.ref_count.store(1); picked = select(d);
    check(picked.task == &last && picked.should_free_pagemap && !picked.zombie_resources_only, "LAST_THREAD_OWNER_HANDOFF");
    cleanup_detached_gc_task(picked, timing);
    check(!last.pagemap && destroys == 1 && releases == 1 && file_releases == 1 && final_task_cleanups == 1, "LAST_PUBLISHER_ROOT_FREE_ONCE");

    // A still-active registered sibling owns the shared root too. Exit must
    // switch this CPU away without detaching either Task's publication.
    reset(); Task exiting, active_sibling;
    exiting.pagemap = active_sibling.pagemap = root;
    exiting.shares_user_pagemap.store(true); active_sibling.shares_user_pagemap.store(true);
    active_sibling.state.store(task::TaskState::ACTIVE); active_sibling.has_exited = false;
    registry = {&exiting, &active_sibling}; active_task_count = registry.size();
    check(task_has_live_pagemap_sibling(&exiting), "ACTIVE_SHARED_PUBLISHER_VISIBLE_TO_EXIT");
    inside_exit_helper = true; call_exit(&exiting); inside_exit_helper = false;
    check(switches == 1 && exiting.pagemap == root && active_sibling.pagemap == root && !destroys && !releases && !file_releases,
          "SHARED_PROCESS_SWITCHES_OWN_CPU_RETAINS_BOTH_ROOTS");
    check(!helper_allocations && !irq_enabled, "SHARED_PROCESS_EXIT_NO_ALLOCATION_IRQ_CHANGE");

    // Actual usercopy quiescence does not detach until its live reader releases.
    reset(); Task copying; copying.pagemap = root; copying.usercopy_pagemap_accesses.store(1);
    std::atomic<bool> detached{false}; Root* detached_root = nullptr;
    std::thread copier([&] { detached_root = copying.detach_pagemap_after_usercopy_quiescence(); detached.store(true); });
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!copying.usercopy_pagemap_closing.load() && std::chrono::steady_clock::now() < until) std::this_thread::yield();
    check(copying.usercopy_pagemap_closing.load() && !detached.load(), "ACTUAL_USERCOPY_QUIESCENCE_WAITS");
    copying.usercopy_pagemap_accesses.fetch_sub(1); copier.join();
    check(detached_root == root && !copying.pagemap, "ACTUAL_USERCOPY_DETACH_AFTER_RELEASE");

    for (unsigned mode = 0; mode != 4; ++mode) {
        reset(); Task skipped; skipped.pagemap = root;
        if (mode == 0) skipped.type = task::TaskType::DAEMON;
        if (mode == 1) skipped.is_thread = true;
        if (mode == 2) skipped.pagemap = nullptr;
        Root* before = skipped.pagemap;
        call_exit(mode == 3 ? nullptr : &skipped);
        check(skipped.pagemap == before && !switches && !destroys && !releases && !helper_allocations && !irq_enabled, "INELIGIBLE_EXIT_NOOP");
    }
    reset(); Task daemon; daemon.type = task::TaskType::DAEMON; daemon.state.store(task::TaskState::DEAD); daemon.pagemap = root;
    daemon.waitable = false; daemon.ref_count.store(1);
    publish(daemon); EpochManager::advance_epoch(); picked = select(d);
    check(picked.task == &daemon && !picked.should_free_pagemap && !picked.zombie_resources_only, "GC_DAEMON_NOT_USER_ROOT_OWNER");
    cleanup_detached_gc_task(picked, timing);
    check(!daemon.pagemap && !destroys && !releases && final_task_cleanups == 1 && !zombie_resource_cleanups,
          "GC_DAEMON_DETACH_WITHOUT_ROOT_FREE");
    std::printf("EXIT_RETIREMENT_RESULT checks=%u failures=%u late_drain=%u\n", checks, failures, observed_late_drain);
    return failures ? 3 : 0;
}
'''


def generated(exit_path):
    paths = [exit_path, ROOT / 'modules/kern/src/platform/sched/scheduler.cpp',
             ROOT / 'modules/kern/src/platform/sched/task.cpp', ROOT / 'modules/kern/src/platform/sched/epoch.hpp',
             ROOT / 'modules/kern/src/platform/sched/epoch.cpp']
    contents = {p: p.read_text() for p in paths}
    exit_text, scheduler, task, header, epoch = (contents[p] for p in paths)
    name = 'leave_exiting_user_address_space' if re.search(r'^void leave_exiting_user_address_space\(', exit_text, re.M) else 'release_exiting_user_address_space'
    pieces = {'EXIT_HELPER': function(exit_text, name), 'QUIESCE': function(task, 'Task::quiesce_usercopy_pagemap'),
              'DETACH': function(task, 'Task::detach_pagemap_after_usercopy_quiescence'),
              'REGISTERED_PUBLISHER': function(scheduler, 'task_has_registered_pagemap_sibling'),
              'LIVE_PUBLISHER': function(scheduler, 'task_has_live_pagemap_sibling'),
              'GC_PUBLISHER': function(scheduler, 'gc_task_has_pagemap_sibling_locked'),
              'GC_SELECTION': function(scheduler, 'detach_next_reclaimable_task_locked'),
              'GC_ZOMBIE_CLEANUP': function(scheduler, 'cleanup_waitable_zombie_resources'),
              'GC_FINAL_CLEANUP': function(scheduler, 'cleanup_detached_gc_task'),
              'GC_DETACHED': block(scheduler, r'^struct GcDetachedTask \{') + ';',
              'GC_TIMING': block(scheduler, r'^struct GcTaskTiming \{') + ';',
              'EPOCH_HEADER': header.replace('#pragma once', '').replace('   private:', '   public:'),
              'EPOCH_SOURCE': re.sub(r'^#include[^\n]*\n', '', epoch, flags=re.M)}
    source = PRELUDE
    for key, value in pieces.items():
        marker = '\n' + key + '\n'
        assert source.count(marker) == 1, key
        source = source.replace(marker, '\n' + value + '\n')
    source += CASES.replace('SELECTED_EXIT_HELPER', name)
    return source, pieces, {str(p): hashlib.sha256(text.encode()).hexdigest() for p, text in contents.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default=os.environ.get('CXX', 'c++'))
    parser.add_argument('--exit-source', type=Path)
    parser.add_argument('--expect-exit-tail-failure', action='store_true')
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    assert __debug__ and (not args.expect_exit_tail_failure or args.exit_source is not None)
    compiler = shutil.which(args.cxx, path=os.defpath) if not Path(args.cxx).is_absolute() else args.cxx
    assert compiler and Path(compiler).is_file(), 'actual selected host compiler'
    exit_path = (args.exit_source or ROOT / 'modules/kern/src/syscalls_impl/process/exit.cpp').resolve()
    source, pieces, inputs = generated(exit_path)
    switch = '    ker::mod::mm::virt::switch_to_kernel_pagemap();'
    assert pieces['EXIT_HELPER'].count(switch) == 1, 'unique actual CR3 boundary'
    variants = [('actual', source, None)]
    if not args.expect_exit_tail_failure:
        changes = [
            ('pointer-drop', switch + '\n    task->pagemap = nullptr;', 'EXIT_ROOT_OWNER_RETAINED'),
            ('skip-cr3-switch', '    (void)task;', 'EXIT_CR3_SWITCH'),
            ('inline-teardown', switch + '\n    ker::mod::mm::virt::destroy_user_space(task->pagemap, task->pid, task->name, "bad-exit");', 'EXIT_TAIL_READER_DRAIN'),
        ]
        for name, replacement, expected in changes:
            changed = pieces['EXIT_HELPER'].replace(switch, replacement)
            assert changed != pieces['EXIT_HELPER'] and source.count(pieces['EXIT_HELPER']) == 1
            variants.append((name, source.replace(pieces['EXIT_HELPER'], changed), expected))
    inputs[str(Path(__file__).resolve())] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    for name in ('gc_epoch_progress_test.py', 'exec_trace_ownership_test.py'):
        path = ROOT / 'tests/host/unit' / name; inputs[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    deadline = time.monotonic() + 120
    scratch = Path(tempfile.mkdtemp(prefix='wos-process-exit-retirement-'))
    ownership = {'safe_to_remove': True}
    result = {'source_inputs': inputs, 'fragments': {k: hashlib.sha256(v.encode()).hexdigest() for k, v in pieces.items()}, 'cases': [],
              'scope': __doc__, 'kernel_or_VM_qualification': False}
    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    def interrupted(number, _frame):
        raise KeyboardInterrupt(str(number))
    for sig in handlers: signal.signal(sig, interrupted)
    try:
        for name, text, expected in variants:
            cpp, binary = scratch / (name + '.cpp'), scratch / name
            cpp.write_text(text); output_guard(scratch)
            command = [str(compiler), '-std=c++23', '-O1', '-g', '-pthread', '-Wall', '-Wextra', '-Werror', str(cpp), '-o', str(binary)]
            code, out, err = bounded_command(command, scratch, name + '-compile', 30, deadline, ownership)
            assert code == 0, 'compiler failure never qualifies a negative: ' + out + err
            code, out, err = bounded_command([str(binary)], scratch, name + '-run', 10, deadline, ownership)
            if expected is not None:
                assert code == 3 and 'EXIT_RETIREMENT_FAILURE ' + expected in err, 'compiled negative must hit its exact runtime oracle: ' + out + err
            elif args.expect_exit_tail_failure:
                assert code == 3 and 'EXIT_RETIREMENT_FAILURE EXIT_TAIL_READER_DRAIN' in err and 'late_drain=1' in out, 'actual predecessor must reach the late reader-drain oracle: ' + out + err
            else:
                assert code == 0 and 'failures=0 late_drain=0' in out, out + err
            result['cases'].append(dict(name=name, native_exit=code, expected_runtime_failure=expected, stdout=out, stderr=err,
                                        generated_source_sha256=hashlib.sha256(text.encode()).hexdigest()))
            print(name + ': ' + out.strip())
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in inputs.items()), 'all extracted source/helper bytes unchanged'
        result.update(result='PASS_HOST_EXIT_RETIREMENT_ONLY', all_source_bookends_equal=True, predecessor_expected_failure=args.expect_exit_tail_failure)
        if args.evidence_dir:
            target = args.evidence_dir.resolve(); assert not target.exists() and target.parent.is_dir()
            target.mkdir(mode=0o700)
            (target / 'actual-source.cpp').write_text(source)
            (target / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result, sort_keys=True))
    finally:
        for sig, handler in handlers.items(): signal.signal(sig, handler)
        if ownership['safe_to_remove']: shutil.rmtree(scratch)
        else: print('Preserving uncertain owned scratch: ' + str(scratch), file=sys.stderr)


if __name__ == '__main__':
    main()
