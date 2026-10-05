"""Actual wake-poke/local-request bodies; hardware/preemption are controlled seams."""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

root = Path(__file__).resolve().parents[3]
parser = argparse.ArgumentParser()
parser.add_argument('--expect-race', action='store_true')
parser.add_argument('--source', type=Path)
parser.add_argument('--output', type=Path)
args = parser.parse_args()
temporary = tempfile.TemporaryDirectory(prefix="wos-poke-migration-")
args.output = args.output or Path(os.environ.get("WOS_POKE_MIGRATION_ARTIFACT_DIR", temporary.name))
source = (args.source or root/'modules/kern/src/platform/sched/scheduler.cpp').read_text()
def function(name):
    match = re.search(rf'^(?:inline )?(?:auto|void) {name}\([^;{{]*\)[^;{{]*\{{', source, re.M)
    assert match, name
    pos = source.index('{', match.start())+1
    depth = 1
    while depth:
        depth += (source[pos]=='{')-(source[pos]=='}')
        pos += 1
    return source[match.start():pos]
code = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <platform/sched/migration_guard.hpp>
#include <platform/sched/event_wake_diagnostics.hpp>
namespace ker::mod::sched {
uint64_t live_cpu=0, timer_cpu=UINT64_MAX, ipi_cpu=UINT64_MAX;
unsigned migrations=0, prevented=0, preemptions=0, guards=0, clock_reads=0;
bool route_inject=false, rq_inject=false, hardware_ok=true, have_task=true;
MigrationGuardState migration_state{};
namespace task {
enum class TaskType { PROCESS,DAEMON,IDLE };
struct Task { TaskType type=TaskType::DAEMON; bool wants_block=false, blocked=false;
 uint32_t local_wake_batch_depth=0;bool local_wake_batch_pending=false;
 bool is_voluntary_blocked()const{return blocked;} } caller;
}
void attempt_migration(){
 ++preemptions;
 if(migration_guard_disabled(migration_state)){++prevented;return;}
 live_cpu=1-live_cpu;++migrations;
}
namespace cpu {uint64_t current_cpu(){uint64_t const old=live_cpu;
 if(route_inject){route_inject=false;attempt_migration();}return old;}}
namespace time {uint64_t get_us(){++clock_reads;return 1000;}}
bool interrupts_enabled(){return true;}
task::Task* current_task_for_preempt(){return have_task?&task::caller:nullptr;}
// CPU/task capture is the existing guard's atomic scalar publication seam.
// Use the real production transition/owner rules. This does not model assembly
// return tails, hardware interrupts, or the RunHeap implementation.
auto migration_disable_token_at(uint64_t)->task::Task* {
 if(!have_task)return nullptr;
 auto transition=migration_disable_transition(migration_state,live_cpu);
 assert(transition.error==MigrationGuardTransitionError::NONE);
 migration_state=transition.state;++guards;return &task::caller;
}
void migration_enable_token_at(task::Task* t,uint64_t){
 if(!t)return;
 assert(t==&task::caller);
 auto transition=migration_enable_transition(migration_state,live_cpu);
 assert(transition.error==MigrationGuardTransitionError::NONE);
 migration_state=transition.state;
}
struct RunQueue {
 task::Task* current_task=&task::caller;
 std::atomic<bool> is_idle{false},resched_timer_pending{false};
 std::atomic<uint64_t> local_reschedule_requests{0};
};
struct Queues {std::array<RunQueue,2> rows{};
 RunQueue* this_cpu(){auto* r=&rows.at(live_cpu);
 if(rq_inject){rq_inject=false;attempt_migration();}return r;}
} storage;
Queues* run_queues=&storage;
namespace sys::context_switch {bool request_reschedule(){timer_cpu=live_cpu;return hardware_ok;}}
enum class WakeCpuMode {COALESCE,FORCE};
void wake_cpu(uint64_t target,WakeCpuMode){if(target!=live_cpu)ipi_cpu=target;}
void request_local_reschedule();
// Trace publication is qualified separately; this seam preserves migration interleavings.
void record_scheduler_timer_trace(SchedulerTimerTraceKind,uint64_t,uint32_t){}
'''
# The constructor, destructor and release are the actual production bodies.
for signature in ('MigrationGuard::MigrationGuard()', 'MigrationGuard::~MigrationGuard()',
                  'void MigrationGuard::release()'):
    match = re.search(r'^'+re.escape(signature)+r'[^;{]*\{', source, re.M)
    assert match, signature
    start = source.index('{', match.start())+1
    pos, depth = start, 1
    while depth:
        depth += (source[pos]=='{')-(source[pos]=='}')
        pos += 1
    code += source[match.start():pos]+'\n'
for name in ('active_process_needs_periodic_scheduler_tick', 'local_reschedule_requires_fresh_timer',
             'request_local_reschedule', 'event_wake_clock', 'batch_local_event_wake', 'event_wake_poke'):
    code += function(name)+'\n'
code += r'''
unsigned cases=0,failures=0;
void reset(){
 live_cpu=0;timer_cpu=ipi_cpu=UINT64_MAX;migrations=prevented=preemptions=guards=clock_reads=0;
 route_inject=rq_inject=false;hardware_ok=have_task=true;migration_state={};task::caller={};
 for(auto& r:storage.rows){r.current_task=&task::caller;r.is_idle=false;
 r.resched_timer_pending=false;r.local_reschedule_requests=0;}
}
void check(bool success,const char* label){
 ++cases;if(!success){++failures;std::printf("RACE %s current=%lu timer=%lu ipi=%lu migrations=%u\n",
 label,live_cpu,timer_cpu,ipi_cpu,migrations);}
 assert(migration_state.depth==0&&migration_state.owner_cpu==MIGRATION_CPU_INVALID);
}
void tests(){
 for(bool enabled:{false,true})for(auto mode:{WakeCpuMode::COALESCE,WakeCpuMode::FORCE})
 for(uint64_t target:{uint64_t{0},uint64_t{1}}){
  reset();route_inject=true;EventWakeDiagnostics d;
  event_wake_poke(target,mode,enabled?&d:nullptr);
  check(target==0?timer_cpu==0:ipi_cpu==1,"local/remote selection migration");
  assert(preemptions==1); // MigrationGuard must allow preemption attempts.
  if(enabled)assert(d.pokes==1);else assert(clock_reads==0);
 }
 reset();rq_inject=true;request_local_reschedule();
 check(timer_cpu==0&&storage.rows[0].resched_timer_pending&& !storage.rows[1].resched_timer_pending,
       "runqueue-to-timer migration");assert(preemptions==1);
 reset();storage.rows[0].resched_timer_pending=true;request_local_reschedule();
 check(timer_cpu==UINT64_MAX,"coalesced daemon request");
 reset();storage.rows[0].resched_timer_pending=true;task::caller.blocked=true;
 request_local_reschedule();check(timer_cpu==0,"halted fresh timer");
 reset();hardware_ok=false;request_local_reschedule();
 check(!storage.rows[0].resched_timer_pending,"failed hardware pending clear");
 reset();have_task=false;request_local_reschedule();check(timer_cpu==0,"no task early context");
 reset();storage.rows[0].current_task=nullptr;request_local_reschedule();
 check(timer_cpu==0,"no current task fresh timer");
 reset();storage.rows[0].is_idle=true;request_local_reschedule();check(timer_cpu==0,"idle fresh timer");
 reset();task::caller.type=task::TaskType::PROCESS;storage.rows[0].resched_timer_pending=true;
 request_local_reschedule();check(timer_cpu==0,"active process fresh timer");
}
}
int main(){ker::mod::sched::tests();std::printf("cases=%u failures=%u\n",
 ker::mod::sched::cases,ker::mod::sched::failures);return ker::mod::sched::failures?2:0;}
'''
args.output.mkdir(parents=True,exist_ok=True)
cpp=args.output/'poke_migration.cpp';cpp.write_text(code)
compiler=os.environ.get('WOS_HOST_CXX') or shutil.which('clang++-23') or '/usr/bin/clang++'
binary=args.output/'poke_migration'
subprocess.run([compiler,'-std=c++23','-O1','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer',
               '-fno-sanitize-recover=all','-I',str(root/'modules/kern/src'),str(cpp),'-o',str(binary)],check=True)
r=subprocess.run([str(binary)],capture_output=True,text=True)
(args.output/'result.txt').write_text(r.stdout+r.stderr)
print(r.stdout+r.stderr,end='')
if args.expect_race:
    assert r.returncode==2 and 'cases=16 failures=9' in r.stdout
else:
    assert r.returncode==0,r.returncode

if not args.expect_race:
    for name, function_name, expected in [('local_guard', 'request_local_reschedule', 1),
                                          ('selection_guard', 'event_wake_poke', 8)]:
        body = function(function_name)
        assert body.count('MigrationGuard const CPU_GUARD;') == 1
        mutant = code.replace(body, body.replace('MigrationGuard const CPU_GUARD;', ''), 1)
        mutant_cpp, mutant_bin = args.output/f'{name}.cpp', args.output/name
        mutant_cpp.write_text(mutant)
        subprocess.run([compiler,'-std=c++23','-O1','-g','-fsanitize=address,undefined',
                        '-fno-sanitize-recover=all','-I',str(root/'modules/kern/src'),
                        str(mutant_cpp),'-o',str(mutant_bin)],check=True)
        rejected = subprocess.run([str(mutant_bin)],capture_output=True,text=True)
        (args.output/f'{name}-result.txt').write_text(rejected.stdout+rejected.stderr)
        assert rejected.returncode==2 and f'cases=16 failures={expected}' in rejected.stdout
        print(f'Compiled semantic mutant {name} rejected with {expected} reproduced races')
