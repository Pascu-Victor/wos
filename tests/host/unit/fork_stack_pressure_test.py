#!/usr/bin/env python3
"""Execute actual fork stack recovery and GC wrapper; hardware/allocator are seams."""
import os
import hashlib
import json
import sys
from pathlib import Path
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import block, function

ROOT = Path(__file__).resolve().parents[3]
PRELUDE = r'''
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
unsigned allocations=0, passes=0, logs=0, snapshots=0, gc_requests=0, yields=0, clocks=0;
unsigned retirement_yields=0; uint64_t elapsed_us=0, yield_us=1000;
bool irq=false, entry_irq=false, pool_held=false, publication_held=false;
bool queues=true, query=true, panic=false, foreign=false;
unsigned preempt=0, failures=0;
bool recover=true, busy=false, concurrent_release=false, gc_changes_if=false;
bool time_limit=false, deferred=false, max_observations=false;
uint64_t pass_scanned=0;
std::string message;
void enable_interrupts() { assert(!pool_held && !publication_held); irq=true; }
void disable_interrupts() { assert(!pool_held && !publication_held); irq=false; }
namespace ker::mod::io::serial { bool is_panic_mode() { return panic; } }
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
struct Task {
 uint64_t pid=0x200000007ULL, syscall_account_start_us=1;
 TaskType type=TaskType::PROCESS;
 std::atomic<bool> scheduler_transition_active{false};
 bool deferred_task_switch=false, wants_block=false, voluntary=false;
 bool is_voluntary_blocked() const { return voluntary; }
};
Task parent, other;
}
namespace ker::mod::time { uint64_t get_us() { ++clocks; return 100+elapsed_us; } }
namespace ker::mod::mm::reclaim { constexpr uint32_t DIRECT_RECLAIM_MAX_PASSES=8; }
namespace ker::mod::sched {
bool has_run_queues() { return queues; }
bool can_query_current_task() { return query; }
unsigned preempt_count() { return preempt; }
bool interrupts_enabled() { return irq; }
task::Task* get_current_task() { return foreign ? &task::other : &task::parent; }
REPORT
struct GcPassResult {
 uint32_t reclaimed=0; uint64_t scanned=0;
 bool time_budget_exhausted=false, has_pending_work=false, ran=false;
};
constexpr uint32_t SCHED_GC_PRESSURE_RECLAIM_BUDGET=512;
constexpr uint64_t SCHED_GC_PRESSURE_WORK_BUDGET_US=50000;
constexpr uint32_t SCHED_GC_PRESSURE_PAGEMAP_STEP_BUDGET=65536;
std::atomic<bool> scheduler_gc_memory_pressure_requested{false};
void request_gc_memory_pressure() { ++gc_requests; }
void kern_yield_impl(uint64_t) { assert(irq && !pool_held && !publication_held && preempt==0); ++yields; elapsed_us+=yield_us; }
GcPassResult run_gc_reclaim_pass_exclusive(uint32_t tasks, uint64_t time,
 uint32_t pages, uint64_t scan, GcPressureDiagnostics* diagnostics) {
 assert(irq && !pool_held && !publication_held && preempt==0);
 assert(tasks==512 && time==50000 && pages==65536 && scan==UINT64_MAX);
 ++passes;
 if(concurrent_release) failures=0;
 if(busy) return {};
 if(recover && yields>=retirement_yields) failures=0;
 if(gc_changes_if) irq=!entry_irq;
 diagnostics->epoch_skips=max_observations ? UINT64_MAX : 3;
 diagnostics->epoch_still_young=max_observations ? UINT64_MAX : 2;
 diagnostics->current_skips=max_observations ? UINT64_MAX : 1;
 diagnostics->refcount_skips=max_observations ? UINT64_MAX : 4;
 diagnostics->zombie_busy_skips=diagnostics->retained_zombie_skips=diagnostics->invalid_tasks=
  max_observations ? UINT64_MAX : 0;
 return {.reclaimed=recover ? 1U : 0U, .scanned=pass_scanned,
  .time_budget_exhausted=time_limit,.has_pending_work=deferred,.ran=true};
}
FOLLOWUP
WRAPPER
}
namespace ker::mod::mm::phys {
void* kernel_stack_alloc(const char* name) {
 assert(!pool_held && !publication_held && irq==entry_irq);
 assert(std::strcmp(name,"fork_kstack")==0); ++allocations;
 if(failures) { --failures; return nullptr; }
 return reinterpret_cast<void*>(0x100000);
}
struct KernelStackPoolStatsSnapshot { uint64_t capacity, free_slots; };
KernelStackPoolStatsSnapshot get_kernel_stack_pool_stats() {
 assert(!pool_held && !publication_held && irq==entry_irq); ++snapshots;
 return {256, failures ? 0ULL : 1ULL};
}
GENERIC_PREDICATE
}
namespace ker::syscall::process {
struct process_log {
 static void info(const char* format, ...) {
  assert(!pool_held && !publication_held && irq==entry_irq); ++logs;
  char data[512]; va_list args; va_start(args,format);
  int length=vsnprintf(data,sizeof(data),format,args); va_end(args);
  assert(length>=0 && static_cast<size_t>(length)<sizeof(data)); message=data;
 }
};
HELPER
SHARED_ALLOCATOR
ALLOCATOR
}
void reset() {
 assert(!pool_held && !publication_held);
 allocations=passes=logs=snapshots=gc_requests=preempt=failures=yields=clocks=retirement_yields=0;
 elapsed_us=0; yield_us=1000;
 irq=entry_irq=panic=foreign=busy=concurrent_release=gc_changes_if=time_limit=deferred=max_observations=false;
 queues=query=recover=true; pass_scanned=7; message.clear();
 ker::mod::sched::scheduler_gc_memory_pressure_requested=false;
 auto& parent=ker::mod::sched::task::parent; parent.pid=0x200000007ULL;
 parent.type=ker::mod::sched::task::TaskType::PROCESS; parent.syscall_account_start_us=1;
 parent.scheduler_transition_active=false; parent.deferred_task_switch=parent.wants_block=parent.voluntary=false;
}
uint64_t run() {
 return ker::syscall::process::alloc_fork_kernel_stack_from_syscall(&ker::mod::sched::task::parent);
}
int main() {
 using namespace ker::mod;
 // No transient-shortage observer, GC pass, snapshot or journal work on the fast path.
 reset(); assert(run()==0x100000 && allocations==1 && !passes && !logs && !snapshots && !yields && !clocks);
 for(bool original_if : {false,true}) {
  reset(); irq=entry_irq=original_if; failures=10;
  assert(run()==0x100000 && allocations==2 && passes==1 && logs==1 && snapshots==1 && irq==original_if);
  assert(message.find("reclaimed=1 ran=1 scanned=7 epoch=3 young=2 current=1 ref=4")!=std::string::npos);
  assert(message.ends_with("stack_free=1 stack_cap=256"));
  // A GC callback changing IF still cannot change the caller's restored interrupt state.
  reset(); irq=entry_irq=original_if; failures=10; gc_changes_if=true;
  assert(run()==0x100000 && irq==original_if && allocations==2 && passes==1);
 }
 // Exact old-source regression: parent reaps while executor still retires; GC has no candidate until voluntary wait.
 for(unsigned retire_after : {1U,3U,7U}) {
  for(bool original_if : {false,true}) {
   reset(); irq=entry_irq=original_if; failures=20; retirement_yields=retire_after;
   assert(run()==0x100000 && yields==retire_after && passes==retire_after+1 && allocations==passes+1);
   assert(logs==passes && snapshots==passes && irq==original_if);
  }
 }
 // Persistent, reader-pinned or busy shortage terminates at eight passes/seven waits, preserving NULL and IF.
 for(bool pass_busy : {false,true}) {
  reset(); failures=20; recover=false; busy=pass_busy;
  assert(run()==0 && allocations==9 && passes==8 && logs==8 && yields==7 && !irq);
  assert(gc_requests==(pass_busy ? 15U : 7U));
 }
 // The elapsed budget stops starting a further iteration; a voluntary scheduler wait may itself overrun.
 for(uint64_t delay : {10000ULL,50000ULL,100000ULL}) {
  reset(); failures=20; recover=false; yield_us=delay;
  assert(run()==0 && passes==(delay==10000 ? 6U : 2U) && yields==passes-1 && allocations==passes+1);
 }
 // A concurrent free may satisfy the retry even if the exclusive pressure pass was busy.
 reset(); failures=10; recover=false; busy=concurrent_release=true;
 assert(run()==0x100000 && allocations==2 && passes==1 && logs==1 && gc_requests==1);
 // Every unsafe-context gate prevents IF enabling, reclamation, observer and retry.
 for(unsigned field=0;field<10;++field) {
  reset(); failures=10; auto& parent=sched::task::parent;
  switch(field) {
   case 0: queues=false; break; case 1: query=false; break; case 2: foreign=true; break;
   case 3: parent.type=sched::task::TaskType::DAEMON; break;
   case 4: parent.syscall_account_start_us=0; break; case 5: preempt=1; break;
   case 6: panic=true; break; case 7: parent.scheduler_transition_active=true; break;
   case 8: parent.deferred_task_switch=true; break; case 9: parent.wants_block=true; break;
  }
  assert(run()==0 && allocations==1 && !passes && !logs && !snapshots && !irq);
 }
 reset(); failures=10; sched::task::parent.voluntary=true;
 assert(run()==0 && allocations==1 && !passes && !logs && !irq);
 assert(!ker::syscall::process::reclaim_fork_stack_from_syscall(nullptr));
 // The unchanged generic allocator predicate continues to reject an IRQ-off context.
 reset(); assert(!mm::phys::can_wait_for_reclaim_impl()); irq=true;
 assert(mm::phys::can_wait_for_reclaim_impl()); preempt=1;
 assert(!mm::phys::can_wait_for_reclaim_impl());
 // Existing pressure wrapper still requests background followup for pending/productive timeout.
 for(unsigned scenario=0;scenario<3;++scenario) {
  reset(); failures=10; deferred=scenario==0; time_limit=scenario==1;
  assert(run()==0x100000 && gc_requests==(scenario<2 ? 1U : 0U) && !yields);
 }
 reset(); failures=10; pass_scanned=UINT64_MAX; max_observations=true;
 sched::task::parent.pid=UINT64_MAX; assert(run()==0x100000 && message.size()<512);
 puts("PASS actual fork guarded-stack recovery: delayed executor retirement, eight-pass/seven-wait and time bounds, fast silence, busy/concurrent/persistent shortage, context gates, IF restore, unchanged generic predicate and bounded journal");
}
'''


def main():
    process = (ROOT / 'modules/kern/src/syscalls_impl/process/process.cpp').read_text()
    helper = function(process, 'reclaim_fork_stack_from_syscall')
    helper = helper.replace('WOS_PERF_CALLSITE()', '0')
    sti, cli = 'asm volatile("sti" ::: "memory");', 'asm volatile("cli" ::: "memory");'
    assert helper.count(sti) == 2 and helper.count(cli) == 1
    helper = helper.replace(sti, 'enable_interrupts();').replace(cli, 'disable_interrupts();')
    fork = function(process, 'wos_proc_fork')
    assert fork.index('GPRegs const FORK_ENTRY_REGS = gpr') < fork.index('alloc_fork_kernel_stack_from_syscall(parent)') < fork.index('new (std::nothrow) sched::task::Task{}')
    scheduler = (ROOT / 'modules/kern/src/platform/sched/scheduler.cpp').read_text()
    header = (ROOT / 'modules/kern/src/platform/sched/scheduler.hpp').read_text()
    source = PRELUDE.replace('REPORT', block(header, r'^struct GcPressureDiagnostics \{') + ';')
    source = source.replace('FOLLOWUP', function(scheduler, 'gc_pass_needs_followup'))
    source = source.replace('WRAPPER', function(scheduler, 'reclaim_memory_pressure'))
    source = source.replace('HELPER', helper)
    source = source.replace('SHARED_ALLOCATOR', function(process, 'alloc_fork_kernel_stack_with_reclaim'))
    source = source.replace('ALLOCATOR', function(process, 'alloc_fork_kernel_stack_from_syscall'))
    phys = (ROOT / 'modules/kern/src/platform/mm/phys.opt.cpp').read_text()
    source = source.replace('GENERIC_PREDICATE', function(phys, 'can_wait_for_reclaim_impl'))
    mutants = {
        'no_retirement_wait': ('sched::kern_yield_impl(0);', '(void)0;'),
        'ignore_pass_limit': ('attempt < ker::mod::mm::reclaim::DIRECT_RECLAIM_MAX_PASSES', 'attempt < UINT32_MAX'),
        'ignore_elapsed_limit': ('NOW_US - START_US >= RETIREMENT_BUDGET_US', 'NOW_US - START_US >= RETIREMENT_BUDGET_US + 10000000'),
        'allow_daemon': ('parent->type != sched::task::TaskType::PROCESS', 'false'),
        'leave_if_enabled': ('disable_interrupts();', 'enable_interrupts();'),
        'fast_path_clock': ('if (stack != 0) {\n        return stack;', 'if (stack != 0) {\n        (void)ker::mod::time::get_us();\n        return stack;'),
    }
    artifacts = Path(sys.argv[1]).resolve() if len(sys.argv) == 2 else None
    if artifacts is not None:
        artifacts.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='wos-fork-stack-') as directory:
        temp = artifacts if artifacts is not None else Path(directory)
        compiler = shutil.which('c++', path=os.defpath)
        flags = [compiler, '-std=c++23', '-O1', '-g', '-Wall', '-Wextra', '-Werror', '-fsanitize=address,undefined']
        bindings = {}
        commands = []
        for label, mutation in [('actual', None), *mutants.items()]:
            body = source
            if mutation is not None:
                old, new = mutation
                assert body.count(old) == 1, label
                body = body.replace(old, new)
            code = temp / (label + '.cpp')
            binary = temp / label
            code.write_text(body)
            command = [*flags, str(code), '-o', str(binary)]
            subprocess.run(command, check=True)
            result = subprocess.run([str(binary)], capture_output=True, text=True)
            assert result.returncode == 0 if mutation is None else result.returncode != 0, (label, result.stdout, result.stderr)
            if mutation is not None:
                assert 'Assertion' in result.stderr, (label, result.stderr)
            bindings[label] = {'source_sha256': hashlib.sha256(body.encode()).hexdigest(),
                               'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                               'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}
            commands.append(command)
            print('ACTUAL_PASS' if mutation is None else 'MUTANT_REJECTED', label, flush=True)
        if artifacts is not None:
            paths = ['modules/kern/src/syscalls_impl/process/process.cpp', 'modules/kern/src/platform/sched/scheduler.cpp',
                     'modules/kern/src/platform/sched/scheduler.hpp', 'modules/kern/src/platform/mm/phys.opt.cpp',
                     'modules/kern/src/platform/mm/reclaim.hpp', 'tests/host/unit/fork_stack_pressure_test.py']
            proof = {'status': 'ACTUAL_DELAYED_RETIREMENT_BOUNDS_CONTEXT_IF_FAST_PATH_AND_SIX_MUTANTS_PASS',
                     'source_bindings': {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths},
                     'commands': commands, 'results': bindings, 'rejected_mutants': list(mutants),
                     'scope': 'Actual fork helper and GC wrapper extracted; hardware/allocator/executor retirement are deterministic seams. ASan/UBSan. Live recovery and ordinary workload remain separately required.'}
            (artifacts / 'proof.json').write_text(json.dumps(proof, indent=2) + '\n')


if __name__ == '__main__':
    main()
