#!/usr/bin/env python3
"""Actual GC epoch/exclusive-pass code with real cooperative-shutdown ownership.

Compiled Host caller/ownership proof only; no kernel teardown, concurrency or
VM qualification. Default uses current repository sources. Optional explicit
--scheduler-source/--expect-cooperative-exit-failure selects a true archived
predecessor and requires actual gate-plus-cleanup abandonment observations.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = next(p for p in Path(__file__).resolve().parents
            if (p / 'modules/kern/src').is_dir() and (p / 'tests/host').is_dir())
sys.path.insert(0, str(ROOT / 'tests/host/unit'))
from exec_trace_ownership_test import block, function

TOTAL_SECONDS = 120
COMPILER_SECONDS = 30
CASE_SECONDS = 10
LOG_CAP = 1 << 20
SCRATCH_CAP = 64 << 20

PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>
namespace ker::mod::cpu {
thread_local uint64_t on_cpu = 5;
uint64_t current_cpu() { return on_cpu; }
}
namespace ker::mod::smt {
uint64_t get_core_count() { return 8; }
uint64_t get_cpu_index_from_apic_id(uint64_t id) { return id; }
}
namespace ker::mod::apic { uint64_t get_apic_id() { return cpu::current_cpu(); } }
namespace ker::mod::dbg { void log(const char*) { assert(false); } }
HEADER
EPOCH_IMPLEMENTATION
TASK_SUPPORT
namespace ker::mod::time {
uint64_t now = 100;
uint64_t get_us() { sched::controlled_clock_boundary(); return now++; }
}
namespace ker::mod::sched {
DIAGNOSTICS
RESULT
std::atomic<bool> scheduler_gc_reclaim_active{false};
uint32_t gc_expired_tasks_budgeted_impl(uint32_t, uint64_t, uint32_t, uint64_t,
                                      uint64_t*, bool*, bool*, GcPressureDiagnostics*);
unsigned recorded = 0, scanned_calls = 0;
void note_gc_pass_result(uint32_t, uint64_t) {
    assert(scheduler_gc_reclaim_active.load()); controlled_boundary(4); ++recorded;
}
OWNED_PASS
// The cleanup seam uses the actual epoch predicate. It does not model Task,
// current-task/ref/zombie checks or pagemap destruction; those remain unchanged.
std::vector<uint64_t> deaths;
unsigned pool_free = 0;
bool check_nested = false, exhausted = false, pending = false;
uint32_t gc_expired_tasks_budgeted_impl(uint32_t budget, uint64_t us, uint32_t pages,
                                      uint64_t scan_budget, uint64_t* scanned,
                                      bool* timed_out, bool* has_pending,
                                      GcPressureDiagnostics* diagnostics) {
    assert(scheduler_gc_reclaim_active.load());
    assert(us == 50000 && pages == 65536);
    ++scanned_calls;
    ++outstanding_cleanup; controlled_boundary(2);
    if (observe_cleanup_hold) shutdown_check(daemon.kernel_shutdown_ownership.held(), "actual hold covers cleanup");
    if (check_nested) {
        auto before = EpochManager::current_epoch();
        auto nested = run_gc_reclaim_pass_exclusive(512, 50000, 65536, UINT64_MAX, nullptr);
        assert(!nested.ran && !nested.reclaimed && !nested.scanned);
        assert(EpochManager::current_epoch() == before);
        if (observe_nested_hold) shutdown_check(daemon.kernel_shutdown_ownership.held(), "nested guard released without dropping outer hold");
    }
    uint32_t reclaimed = 0;
    *scanned = 0;
    for (auto it = deaths.begin(); it != deaths.end() && reclaimed < budget && *scanned < scan_budget;) {
        ++*scanned;
        if (EpochManager::is_safe_to_reclaim(*it)) {
            it = deaths.erase(it); ++reclaimed; ++pool_free;
        } else {
            if (diagnostics) ++diagnostics->epoch_skips;
            ++it;
        }
    }
    *timed_out = exhausted; *has_pending = pending;
    --outstanding_cleanup; ++retired_cleanup;
    return reclaimed;
}
std::array<CpuEpoch, 8> states;
void reset() {
    assert(!scheduler_gc_reclaim_active.load());
    EpochManager::cpu_epochs = states.data();
    EpochManager::global_epoch.store(40);
    for (auto& state : states) {
        state.local_epoch.store(0); state.in_critical_section.store(false);
    }
    deaths.clear(); pool_free = recorded = scanned_calls = 0;
    check_nested = exhausted = pending = false;
    cpu::on_cpu = 5;
    reset_shutdown_fixture();
}
GcPassResult pass(uint32_t budget = 512, uint64_t scan_budget = UINT64_MAX,
                  GcPressureDiagnostics* diagnostics = nullptr) {
    return run_gc_reclaim_pass_exclusive(budget, 50000, 65536, scan_budget, diagnostics);
}
}
SHUTDOWN_CASES
CASES
'''

CASES = r'''
int main() {
    using namespace ker::mod;
    using namespace sched;
    // Reproduce the observed same-epoch cohort without any CPU0 IRQ.
    reset(); deaths.assign(130, EpochManager::current_epoch());
    check_nested = true;
    auto result = pass();
    assert(pool_free == 130 && result.reclaimed == 130 && result.scanned == 130);
    assert(result.ran && recorded == 1 && scanned_calls == 1);
    assert(EpochManager::current_epoch() == 41 && !scheduler_gc_reclaim_active.load());

    // A concurrent owner must not manufacture additional progress or scan.
    reset(); scheduler_gc_reclaim_active.store(true);
    auto before = EpochManager::current_epoch(); result = pass();
    assert(!result.ran && !result.reclaimed && !result.scanned);
    assert(EpochManager::current_epoch() == before && !recorded && !scanned_calls);
    scheduler_gc_reclaim_active.store(false);

    // An old reader on every CPU, including the caller CPU, still blocks.
    for (uint64_t reader = 0; reader < 8; ++reader) {
        reset(); deaths.assign(130, EpochManager::current_epoch());
        EpochManager::enter_critical_for_cpu(reader);
        GcPressureDiagnostics diagnostics{};
        result = pass(512, UINT64_MAX, &diagnostics);
        assert(result.reclaimed == 0 && pool_free == 0 && diagnostics.epoch_skips == 130);
        assert(states[reader].in_critical_section.load() && states[reader].local_epoch.load() == 40);
        assert(!EpochManager::is_safe_to_reclaim(40));
        EpochManager::exit_critical_for_cpu(reader);
        result = pass(); assert(result.reclaimed == 130 && pool_free == 130);
    }

    // A reader entering after the age advance cannot hold the old cohort;
    // it still protects tasks retired in its own epoch through later passes.
    reset(); EpochManager::advance_epoch();
    EpochManager::enter_critical_for_cpu(3);
    assert(EpochManager::is_safe_to_reclaim(40));
    assert(!EpochManager::is_safe_to_reclaim(41));
    deaths = {40, 41}; result = pass();
    assert(result.reclaimed == 1 && deaths.size() == 1 && deaths[0] == 41);
    EpochManager::exit_critical_for_cpu(3); result = pass(); assert(result.reclaimed == 1);

    // Preserve both task and scan bounds and the original pending/time flags.
    reset(); deaths.assign(130, 40); pending = exhausted = true;
    result = pass(7, 3);
    assert(result.reclaimed == 3 && result.scanned == 3 && deaths.size() == 127);
    assert(result.time_budget_exhausted && result.has_pending_work);
    result = pass(7, 100); assert(result.reclaimed == 7 && result.scanned == 7);

    // Existing timer writers and the GC owner can increment concurrently.
    // This exercises the actual atomic increment; no read-side state is forced.
    reset(); std::vector<std::thread> writers;
    for (unsigned n = 0; n < 8; ++n) writers.emplace_back([] {
        for (unsigned i = 0; i < 1000; ++i) EpochManager::advance_epoch();
    });
    for (unsigned i = 0; i < 1000; ++i) { result = pass(); assert(result.ran); }
    for (auto& writer : writers) writer.join();
    assert(EpochManager::current_epoch() == 9040 && recorded == 1000);
    for (const auto& state : states) assert(!state.in_critical_section.load() && !state.local_epoch.load());
    shutdown_cases();
    puts("PASS actual GC epoch progress without BSP ticks; all old reader guards, busy ownership, bounds and concurrent atomic writers preserved");
    return shutdown_failures ? 3 : 0;
}
'''

TASK_SUPPORT = r'''
// Real header, hold factory, requested getter and cooperative hook are used.
// Only query/Task endpoint primitives below are controlled Host fixtures.
SHUTDOWN_HEADER
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
struct Task {
    TaskType type = TaskType::DAEMON;
    bool exit_in_progress = false, preempt_pending = false;
    uint32_t preempt_disable_depth = 0;
    std::atomic<bool> kernel_shutdown_requested{false}, wakeup_pending{true};
    std::atomic<void*> owned_unpublished_process{nullptr};
    ShutdownOwnership kernel_shutdown_ownership;
    bool deferred_task_switch = true, yield_switch = true, wants_block = true;
    uint64_t wake_at_us = 77;
    bool voluntary_blocked = true, wait_channel = true;
    void set_voluntary_blocked(bool value) { voluntary_blocked = value; }
    void clear_wait_channel() { wait_channel = false; }
};
}
struct SimulatedCooperativeExit {};
unsigned simulated_exit_calls = 0;
namespace ker::syscall::process {
[[noreturn]] void wos_proc_exit(int status) {
    assert(status == 0); ++simulated_exit_calls;
    // This controlled Host exception is not kernel noreturn unwinding proof.
    throw SimulatedCooperativeExit{};
}
}
namespace ker::mod::sched {
task::Task daemon;
task::Task* fixture_current = &daemon;
bool fixture_can_query = true, fixture_interrupts = true;
std::atomic<bool> kernel_thread_shutdown_requested{false};
bool can_query_current_task() { return fixture_can_query; }
auto get_current_task() -> task::Task* { return fixture_current; }
bool interrupts_enabled() { return fixture_interrupts; }
REQUESTED_GETTER
HOLD_FACTORY
COOPERATIVE_HOOK
unsigned shutdown_boundary = 0, boundary_clock_reads = 0;
unsigned shutdown_vectors = 0, shutdown_failures = 0;
unsigned outstanding_cleanup = 0, retired_cleanup = 0;
unsigned abandoned_gate_observations = 0, unretired_cleanup_observations = 0;
bool observe_cleanup_hold = false, observe_nested_hold = false;
void shutdown_check(bool okay, const char* predicate) {
    if (!okay) { ++shutdown_failures; std::fprintf(stderr, "GC_SHUTDOWN_FAILURE %s\n", predicate); }
}
void controlled_boundary(unsigned point) {
    if (shutdown_boundary == point) maybe_exit_current_kernel_thread_for_shutdown();
}
void controlled_clock_boundary() {
    ++boundary_clock_reads;
    controlled_boundary(boundary_clock_reads == 1 ? 1 : 3);
}
void reset_shutdown_fixture() {
    assert(!daemon.kernel_shutdown_ownership.held());
    fixture_current = &daemon; fixture_can_query = fixture_interrupts = true;
    kernel_thread_shutdown_requested.store(false);
    daemon.type = task::TaskType::DAEMON; daemon.exit_in_progress = daemon.preempt_pending = false;
    daemon.preempt_disable_depth = 0; daemon.kernel_shutdown_requested.store(false);
    daemon.owned_unpublished_process.store(nullptr); daemon.wakeup_pending.store(true);
    daemon.deferred_task_switch = daemon.yield_switch = daemon.wants_block = true;
    daemon.wake_at_us = 77; daemon.voluntary_blocked = daemon.wait_channel = true;
    shutdown_boundary = boundary_clock_reads = 0;
    outstanding_cleanup = retired_cleanup = simulated_exit_calls = 0;
    observe_cleanup_hold = observe_nested_hold = false;
}
}
'''

SHUTDOWN_CASES = r'''
namespace ker::mod::sched {
void request_fixture_shutdown() {
    kernel_thread_shutdown_requested.store(true);
    daemon.kernel_shutdown_requested.store(true);
}
bool call_exit_hook() {
    try { maybe_exit_current_kernel_thread_for_shutdown(); }
    catch (SimulatedCooperativeExit const&) { return true; }
    return false;
}
void shutdown_cases() {
    // Four distinct actual-pass boundaries. No blanket completion/drain claim.
    for (unsigned boundary : {1U, 2U, 3U, 4U}) {
        reset(); ++shutdown_vectors; deaths.assign(1, EpochManager::current_epoch());
        request_fixture_shutdown(); shutdown_boundary = boundary; observe_cleanup_hold = true;
        bool interrupted = false;
        try {
            auto result = pass();
            shutdown_check(result.ran && result.reclaimed == 1 && result.scanned == 1, "actual owned pass result");
        } catch (SimulatedCooperativeExit const&) { interrupted = true; }
        if (interrupted && scheduler_gc_reclaim_active.load()) ++abandoned_gate_observations;
        if (interrupted && outstanding_cleanup != 0 && retired_cleanup == 0) ++unretired_cleanup_observations;
        shutdown_check(!interrupted && simulated_exit_calls == 0, "cooperative exit deferred inside owned pass");
        shutdown_check(!scheduler_gc_reclaim_active.load(), "owned gate released before return");
        shutdown_check(outstanding_cleanup == 0 && retired_cleanup == 1 && recorded == 1, "owned cleanup retired and accounted exactly once");
        shutdown_check(!daemon.kernel_shutdown_ownership.held(), "lexical hold released after pass");
        if (!interrupted) {
            shutdown_check(daemon.preempt_pending && daemon.wakeup_pending.load() && daemon.wait_channel,
                           "cooperative request and wait state retained while held");
            shutdown_check(call_exit_hook() && simulated_exit_calls == 1, "same actual hook resumes exit after return");
            shutdown_check(!daemon.wakeup_pending.load() && !daemon.deferred_task_switch && !daemon.yield_switch &&
                           !daemon.wants_block && daemon.wake_at_us == 0 && !daemon.voluntary_blocked && !daemon.wait_channel,
                           "actual hook clears terminal state only at accepted exit");
        }
        // Test-only recovery permits subsequent vectors after observing the
        // exact predecessor abandonment. It is never kernel cleanup evidence.
        scheduler_gc_reclaim_active.store(false); outstanding_cleanup = 0;
    }

    reset(); ++shutdown_vectors; request_fixture_shutdown(); scheduler_gc_reclaim_active.store(true);
    auto before = EpochManager::current_epoch(); auto result = pass();
    shutdown_check(!result.ran && !result.reclaimed && !result.scanned && !recorded && !scanned_calls &&
                   EpochManager::current_epoch() == before && scheduler_gc_reclaim_active.load() &&
                   !daemon.kernel_shutdown_ownership.held(), "busy peer keeps gate and no temporary ownership leaks");
    scheduler_gc_reclaim_active.store(false);

    reset(); ++shutdown_vectors; deaths.assign(1, 40); request_fixture_shutdown();
    fixture_current = nullptr; fixture_can_query = false; shutdown_boundary = 2;
    result = pass();
    shutdown_check(result.ran && result.reclaimed == 1 && retired_cleanup == 1 && simulated_exit_calls == 0 &&
                   !scheduler_gc_reclaim_active.load() && !daemon.kernel_shutdown_ownership.held(), "pre-task null owner is established no-op");

    reset(); ++shutdown_vectors; deaths.assign(1, 40); request_fixture_shutdown();
    check_nested = observe_nested_hold = observe_cleanup_hold = true; shutdown_boundary = 2;
    try { result = pass(); }
    catch (SimulatedCooperativeExit const&) { shutdown_check(false, "nested outer pass must defer exit"); scheduler_gc_reclaim_active.store(false); }
    shutdown_check(!daemon.kernel_shutdown_ownership.held() && !scheduler_gc_reclaim_active.load() && recorded == 1,
                   "nested busy attempt balances inner guard and retains outer owner through cleanup");
    outstanding_cleanup = 0;

    reset(); ++shutdown_vectors; deaths.assign(1, 40); request_fixture_shutdown(); shutdown_boundary = 2;
    {
        auto outer = hold_kernel_shutdown();
        result = pass();
        shutdown_check(daemon.kernel_shutdown_ownership.held() && !scheduler_gc_reclaim_active.load() &&
                       !call_exit_hook(), "pass return retains earlier external ownership");
    }
    shutdown_check(!daemon.kernel_shutdown_ownership.held() && call_exit_hook(), "last earlier scope enables actual exit hook");

    reset(); ++shutdown_vectors; deaths.assign(1, 40); pending = exhausted = true;
    result = pass(7, 3);
    shutdown_check(result.ran && result.reclaimed == 1 && result.scanned == 1 && result.has_pending_work &&
                   result.time_budget_exhausted && retired_cleanup == 1 && !daemon.kernel_shutdown_ownership.held(),
                   "normal no-shutdown flags and finite budgets preserved without pending-queue drain claim");

    // Existing eligibility gates still refuse voluntary shutdown independently.
    for (unsigned restriction = 0; restriction != 6; ++restriction) {
        reset(); ++shutdown_vectors; request_fixture_shutdown();
        if (restriction == 0) daemon.preempt_disable_depth = 1;
        if (restriction == 1) fixture_interrupts = false;
        if (restriction == 2) daemon.owned_unpublished_process.store(&daemon);
        if (restriction == 3) daemon.type = task::TaskType::PROCESS;
        if (restriction == 4) daemon.exit_in_progress = true;
        if (restriction == 5) daemon.kernel_shutdown_requested.store(false);
        shutdown_check(!call_exit_hook() && simulated_exit_calls == 0 && daemon.wakeup_pending.load(),
                       "real hook keeps existing preempt/IF/unpublished/type/exit/request restrictions");
    }
    reset(); ++shutdown_vectors; daemon.kernel_shutdown_requested.store(true);
    shutdown_check(!call_exit_hook(), "global shutdown request remains necessary");
    std::printf("GC_SHUTDOWN_RESULT vectors=%u failures=%u abandoned_gate=%u unretired_cleanup=%u\n",
                shutdown_vectors, shutdown_failures, abandoned_gate_observations, unretired_cleanup_observations);
}
}
'''

def output_guard(root):
    logical = allocated = 0
    for path in root.rglob('*'):
        assert not path.is_symlink(), 'owned scratch has no symlinks'
        info = path.stat()
        assert info.st_uid == os.getuid(), 'owned scratch UID'
        if path.is_file():
            assert info.st_nlink == 1, 'owned scratch has no external hardlink'
            logical += info.st_size
            allocated += info.st_blocks * 512
    assert max(logical, allocated) <= SCRATCH_CAP, 'bounded owned scratch'


def child_limits():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (16 << 20, 16 << 20))


def bounded_command(command, root, name, seconds, deadline, ownership):
    assert time.monotonic() < deadline, 'whole-test absolute deadline'
    out_path, err_path = root / (name + '.stdout'), root / (name + '.stderr')
    process = None
    reaped = False
    end = min(time.monotonic() + seconds, deadline - 3)
    assert end > time.monotonic(), 'reserve exact-owned kill/reap time'
    env = dict(os.environ)
    env.update(ASAN_OPTIONS='detect_leaks=0', TMPDIR=str(root))
    try:
        with out_path.open('xb') as out, err_path.open('xb') as err:
            process = subprocess.Popen(command, stdout=out, stderr=err, env=env,
                                       start_new_session=True, preexec_fn=child_limits)
            ownership['safe_to_remove'] = False
            while True:
                code = process.poll()
                if code is not None:
                    reaped = True
                    break
                assert time.monotonic() < end, 'bounded owned command deadline'
                assert out_path.stat().st_size <= LOG_CAP and err_path.stat().st_size <= LOG_CAP, 'bounded command logs'
                output_guard(root)
                time.sleep(0.02)
    finally:
        if process is not None:
            if not reaped:
                try: os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError: pass
                try: process.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    try: os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    process.wait(timeout=2)
            try: os.killpg(process.pid, 0)
            except ProcessLookupError: ownership['safe_to_remove'] = True
            else: raise AssertionError('unproven session disappearance; preserve owned scratch')
    assert out_path.stat().st_size <= LOG_CAP and err_path.stat().st_size <= LOG_CAP, 'bounded complete command logs'
    output_guard(root)
    return code, out_path.read_text(errors='replace'), err_path.read_text(errors='replace')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scheduler-source', type=Path)
    parser.add_argument('--expect-cooperative-exit-failure', action='store_true')
    args = parser.parse_args()
    assert not args.expect_cooperative_exit_failure or args.scheduler_source is not None, 'negative control must select explicit archived source'
    scheduler_path = args.scheduler_source or ROOT / 'modules/kern/src/platform/sched/scheduler.cpp'
    scheduler = scheduler_path.read_text()
    header = (ROOT / 'modules/kern/src/platform/sched/epoch.hpp').read_text()
    # Same original exposure, used only for deterministic Host epoch inspection.
    header = header.replace('#pragma once', '').replace('   private:', '   public:')
    epoch = (ROOT / 'modules/kern/src/platform/sched/epoch.cpp').read_text()
    epoch = re.sub(r'^#include[^\n]*\n', '', epoch, flags=re.M)
    gc = function(scheduler, 'run_gc_reclaim_pass_exclusive')
    hold = function(scheduler, 'hold_kernel_shutdown')
    requested = function(scheduler, 'kernel_threads_shutdown_requested')
    hook = function(scheduler, 'maybe_exit_current_kernel_thread_for_shutdown')
    ownership_header = (ROOT / 'modules/kern/src/platform/sched/shutdown_ownership.hpp').read_text()
    task_support = TASK_SUPPORT.replace('SHUTDOWN_HEADER', ownership_header.replace('#pragma once', ''))
    task_support = task_support.replace('REQUESTED_GETTER', requested).replace('HOLD_FACTORY', hold).replace('COOPERATIVE_HOOK', hook)
    advance = '    EpochManager::advance_epoch();\n'
    assert gc.count(advance) == 1
    source = PRELUDE.replace('HEADER', header).replace('EPOCH_IMPLEMENTATION', epoch).replace('TASK_SUPPORT', task_support)
    sched_header = (ROOT / 'modules/kern/src/platform/sched/scheduler.hpp').read_text()
    source = source.replace('DIAGNOSTICS', block(sched_header, r'^struct GcPressureDiagnostics \{') + ';')
    source = source.replace('RESULT', block(scheduler, r'^struct GcPassResult \{') + ';')
    source = source.replace('SHUTDOWN_CASES', SHUTDOWN_CASES).replace('CASES', CASES)
    early = gc.replace(advance, '')
    brace = early.index('{')
    early = early[:brace + 1] + '\n' + advance + early[brace + 1:]
    unsafe = source.replace('if (CPU_EPOCH <= death_epoch)', 'if (false && CPU_EPOCH <= death_epoch)')
    cases = [
        ('actual', source.replace('OWNED_PASS', gc), None),
        ('missing-advance', source.replace('OWNED_PASS', gc.replace(advance, '')), 'pool_free == 130'),
        ('advance-before-ownership', source.replace('OWNED_PASS', early), 'current_epoch() == before'),
        ('bypass-old-reader', unsafe.replace('OWNED_PASS', gc), 'result.reclaimed == 0'),
    ]
    if args.expect_cooperative_exit_failure:
        cases = cases[:1]
    compiler_name = os.environ.get('CXX', 'c++')
    compiler = shutil.which(compiler_name, path=os.defpath)
    assert compiler and Path(compiler).is_file(), 'single actual compiler executable path'
    deadline = time.monotonic() + TOTAL_SECONDS
    root = Path(tempfile.mkdtemp(prefix='wos-gc-epoch-shutdown-'))
    ownership = {'safe_to_remove': True}
    try:
        for name, text, failure in cases:
            cpp, binary = root / (name + '.cpp'), root / name
            cpp.write_text(text)
            output_guard(root)
            command = [compiler, '-std=c++23', '-O1', '-g', '-pthread', '-Wall', '-Wextra', '-Werror',
                       '-fsanitize=address,undefined', '-fno-sanitize-recover=all', '-fno-omit-frame-pointer', str(cpp), '-o', str(binary)]
            code, out, err = bounded_command(command, root, name + '-compile', COMPILER_SECONDS, deadline, ownership)
            assert code == 0, 'compiler failure never qualifies negative control: ' + out + err
            code, out, err = bounded_command([str(binary)], root, name + '-run', CASE_SECONDS, deadline, ownership)
            if failure is not None:
                assert code != 0 and failure in err, 'original epoch behavior mutation must reach its intended predicate: ' + err
                print(name + ': rejected by identical original epoch behavior assertions')
            elif args.expect_cooperative_exit_failure:
                match = re.search(r'GC_SHUTDOWN_RESULT vectors=(\d+) failures=(\d+) abandoned_gate=(\d+) unretired_cleanup=(\d+)', out)
                assert code == 3 and match and int(match[2]) > 0 and int(match[3]) > 0 and int(match[4]) > 0, \
                    'predecessor must actually abandon gate and owned cleanup at the controlled exit boundary: ' + out + err
                print(out, end='')
                print('EXPECTED_HOST_COOPERATIVE_ABANDONMENT; no kernel runtime claim')
            else:
                assert code == 0, out + err
                print(out, end='')
        assert time.monotonic() <= deadline, 'whole finite test deadline'
        hashes = {name: hashlib.sha256(value.encode()).hexdigest() for name, value in
                  [('scheduler_file', scheduler), ('exclusive_pass', gc), ('hold_factory', hold),
                   ('shutdown_getter', requested), ('cooperative_hook', hook), ('ownership_header', ownership_header)]}
        completion = {'source_path': str(scheduler_path), 'extracted_actual_hashes': hashes,
                      'compiled_vectors': len(cases), 'kernel_runtime_qualified': False}
    finally:
        if ownership['safe_to_remove']:
            shutil.rmtree(root)
            assert not root.exists(), 'owned scratch removed before completion'
        else:
            print('UNPROVEN_CHILD_SESSION_PRESERVED_SCRATCH ' + str(root), file=sys.stderr)
            raise AssertionError('unproven child session prevents cleanup and completion')
    assert time.monotonic() <= deadline, 'whole finite test deadline includes owned scratch cleanup'
    completion['owned_scratch_removed'] = True
    completion['cleanup_completed_before_deadline'] = True
    print(json.dumps(completion, sort_keys=True))


if __name__ == '__main__':
    main()
