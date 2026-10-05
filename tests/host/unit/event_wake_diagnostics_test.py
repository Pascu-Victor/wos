"""Execute actual wake telemetry and leader drain with controlled runqueue/clock seams.

The runqueue callback is a seam; this checks elapsed attribution and unchanged
wake/coalescing behavior, not the heap implementation or hardware IRQ latency.
"""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
source = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()


def function(name):
    match = re.search(rf"^(?:inline )?(?:auto|void) {name}\([^{{]+\{{", source, re.M)
    assert match, name
    pos, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[pos] == "{") - (source[pos] == "}")
        pos += 1
    return source[match.start():pos]


code = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <utility>
#include <platform/sched/event_wake_diagnostics.hpp>
namespace ker::mod::sched {
namespace time { uint64_t now=1000; unsigned reads=0; uint64_t get_us(){++reads;return now;} }
namespace cpu { uint64_t current=0; uint64_t current_cpu(){return current;} }
namespace smt { uint64_t get_core_count(){return 4;} }
// This adapter only checks CPU ownership at the poke boundary. The separate
// scheduler_poke_migration_test executes real migration transitions and guards.
struct MigrationGuard {
 uint64_t owner=cpu::current;
 ~MigrationGuard(){assert(cpu::current==owner);}
};
namespace task {
enum class WaitChannelKind { WKI_EXECVE_PROXY };
struct Task {
 uint64_t cpu=2,pid=19; uint8_t type=0; bool cpu_pinned=true, deferred_task_switch=true,proxy=false;
 uint32_t local_wake_batch_depth=0;bool local_wake_batch_pending=false,wants_block=false;
 bool is_voluntary_blocked()const{return false;}
 std::atomic<bool> wakeup_pending{false},reschedule_pending{false},reschedule_in_progress{false};
 std::atomic<uint64_t> reschedule_requested_cpu{0};
 bool wait_channel_is(WaitChannelKind)const{return proxy;}
}; }
struct RunQueue { std::atomic<uint64_t> cpu_irq_us{5},scheduler_timer_interrupts{10}; SchedulerTimerTrace timer_trace{}; };
struct Queues {
 std::array<RunQueue,4> rows{};bool locked=false;unsigned calls=0;
 RunQueue* that_cpu(uint64_t c){return &rows.at(c);}
 template<class Func>void with_lock_void(uint64_t c,Func&& f){
  assert(!locked);time::now+=11;locked=true;++calls;std::forward<Func>(f)(&rows.at(c));
  locked=false;time::now+=17;
 }
} storage;
Queues* run_queues=&storage;
bool scheduler_timer_trace_enabled(){return false;}
task::Task waker;task::Task* current_task_for_preempt(){return &waker;}
bool interrupts_enabled(){return true;}
void record_scheduler_timer_trace(SchedulerTimerTraceKind,uint64_t,uint32_t){}
enum class EventWakeDeferredSwitch { PRESERVE,CANCEL };
enum class WakeCpuMode { COALESCE,FORCE };
struct MigrationGuardPlacement {bool disabled=false;uint64_t owner_cpu=0;int reason=0;};
MigrationGuardPlacement guarded;
unsigned local_pokes=0,remote_pokes=0,rejections=0,passes=0,requested_passes=1;
bool migrate=false;
bool event_wake_should_cancel_deferred_switch(EventWakeDeferredSwitch d,bool proxy){
 return d==EventWakeDeferredSwitch::CANCEL&&!proxy;
}
MigrationGuardPlacement migration_guard_placement(task::Task*,uint64_t){return guarded;}
void record_migration_rejection(task::Task*,uint64_t,int){++rejections;}
uint64_t scheduler_single_process_cpu_target(task::Task*,uint64_t){return UINT64_MAX;}
uint64_t scheduler_process_owner_cpu_if_migration_disabled(task::Task*,uint64_t){return UINT64_MAX;}
uint64_t get_least_loaded_cpu(){return 0;}
uint64_t cross_cpu_migration_target_or_source(task::Task*,uint64_t,uint64_t target,uint64_t){return target;}
uint64_t event_wake_target_cpu(task::Task* t,uint64_t){time::now+=7;return t->cpu;}
void request_local_reschedule(){assert(!storage.locked);++local_pokes;time::now+=19;}
void wake_cpu(uint64_t,WakeCpuMode){assert(!storage.locked);++remote_pokes;time::now+=19;}
void reschedule_task_for_cpu(uint64_t,task::Task*,EventWakeDiagnostics*);
'''
for name in ("event_wake_clock", "batch_local_event_wake", "event_wake_poke", "queue_reschedule_request",
             "consume_reschedule_request", "retain_or_reacquire_reschedule_leader"):
    code += function(name) + "\n"
code += "template <typename Func>\n" + function("with_event_wake_runqueue_lock") + "\n"
code += r'''
void reschedule_task_for_cpu_once(uint64_t c,task::Task* t,EventWakeDiagnostics* d){
 assert(t->wakeup_pending.load());++passes;
 with_event_wake_runqueue_lock(c,d,[&](RunQueue* rq){
  assert(storage.locked);time::now+=13;rq->cpu_irq_us.fetch_add(3);
  if(passes<requested_passes){assert(!queue_reschedule_request(t,c));}
 });
 event_wake_poke(c,WakeCpuMode::COALESCE,d);
 if(migrate){cpu::current=1;storage.rows[0].scheduler_timer_interrupts.fetch_add(1);}
}
'''
for name in ("reschedule_task_for_cpu", "wake_task_from_event_on_cpu", "wake_task_from_event"):
    code += function(name) + "\n"
code += r'''
unsigned cases=0;
void reset(){
 time::now=1000;time::reads=0;cpu::current=0;run_queues=&storage;
 assert(!storage.locked);storage.calls=0;guarded={};requested_passes=1;migrate=false;
 local_pokes=remote_pokes=rejections=passes=0;
 for(auto& rq:storage.rows){rq.cpu_irq_us.store(5);rq.scheduler_timer_interrupts.store(10);}
}
void tests(){
 for(bool enabled:{false,true})for(uint64_t target:{uint64_t{0},uint64_t{2}})
 for(unsigned n:{1U,2U,127U})for(bool proxy:{false,true}){
  reset();requested_passes=n;task::Task t;t.cpu=target;t.proxy=proxy;EventWakeDiagnostics d;
  wake_task_from_event(&t,EventWakeDeferredSwitch::CANCEL,enabled?&d:nullptr);
  assert(t.wakeup_pending.load()&&t.deferred_task_switch==proxy);
  assert(!t.reschedule_pending.load()&&!t.reschedule_in_progress.load());
  assert(passes==n&&storage.calls==n&&local_pokes+remote_pokes==n);
  assert(local_pokes==(target==0?n:0)&&remote_pokes==(target==2?n:0));
  assert(time::now==1007+60*n);
  if(enabled){
   assert(d.recorded&&d.leader&&d.counters_valid&&d.started_us==1000);
   assert(d.total_us==7+60*n&&d.target_us==7&&d.prepare_us==0&&d.admission_us==0);
   assert(d.reschedule_us==60*n&&d.rq_wait_us==11*n&&d.rq_body_us==13*n);
   assert(d.rq_return_us==17*n&&d.poke_us==19*n&&d.locks==n&&d.passes==n&&d.pokes==n);
   assert(d.cpu_start==0&&d.cpu_end==0&&d.target_cpu==target);
   assert(d.irq_start_us==5&&d.irq_end_us==(target==0?5+3*n:5));
   assert(d.timer_start==10&&d.timer_end==10&&time::reads>0);
  }else{assert(time::reads==0&&!d.recorded&&d.total_us==0);}
  ++cases;
 }
 reset();task::Task loser;loser.reschedule_in_progress.store(true);EventWakeDiagnostics d;
 wake_task_from_event(&loser,EventWakeDeferredSwitch::PRESERVE,&d);
 assert(loser.wakeup_pending.load()&&loser.reschedule_pending.load()&&loser.reschedule_in_progress.load());
 assert(!d.leader&&d.passes==0&&d.locks==0&&d.total_us==7&&passes==0);
 uint64_t c=0;assert(consume_reschedule_request(&loser,c)&&c==2);
 assert(!retain_or_reacquire_reschedule_leader(&loser)&&!loser.reschedule_in_progress.load());++cases;
 reset();d.total_us=99;wake_task_from_event(nullptr,EventWakeDeferredSwitch::PRESERVE,&d);
 assert(!d.recorded&&d.total_us==0&&time::reads==0);++cases;
 reset();run_queues=nullptr;task::Task early;
 wake_task_from_event(&early,EventWakeDeferredSwitch::PRESERVE,&d);
 assert(d.recorded&&!d.counters_valid&&d.total_us==7&&d.passes==0&&early.wakeup_pending.load());++cases;
 reset();guarded={true,UINT64_MAX,3};task::Task rejected;
 wake_task_from_event(&rejected,EventWakeDeferredSwitch::PRESERVE,&d);
 assert(rejected.wakeup_pending.load()&&passes==0&&rejections==1&&d.total_us==7);++cases;
 reset();migrate=true;task::Task moved;moved.cpu=0;
 wake_task_from_event(&moved,EventWakeDeferredSwitch::PRESERVE,&d);
 assert(d.cpu_start==0&&d.cpu_end==1&&d.timer_start==10&&d.timer_end==11);
 assert(d.irq_start_us==5&&d.irq_end_us==8&&d.total_us==67);++cases;
 reset();EventWakeDiagnostics lock;
 with_event_wake_runqueue_lock(2,&lock,[](RunQueue*){return;});
 assert(lock.rq_wait_us==11&&lock.rq_body_us==0&&lock.rq_return_us==17&&lock.locks==1);++cases;
}
}
int main(){ker::mod::sched::tests();std::printf("%u actual wake diagnostic/leader cases PASS\n",ker::mod::sched::cases);}
'''
compiler = os.environ.get("WOS_HOST_CXX") or shutil.which("clang++-23") or "/usr/bin/clang++"
with tempfile.TemporaryDirectory(prefix="wos-wake-diag-") as temporary:
    out = Path(os.environ.get("WOS_WAKE_DIAG_ARTIFACT_DIR", temporary))
    out.mkdir(parents=True, exist_ok=True)
    mutants = [
        ("irq_restore_time", "diagnostics->rq_return_us += RETURNED - finished;",
         "diagnostics->rq_return_us += finished - acquired;"),
        ("off_clock", "return diagnostics != nullptr ? time::get_us() : 0;",
         "return time::get_us();"),
        ("stranded_request", "keep_leadership = retain_or_reacquire_reschedule_leader(task);",
         "keep_leadership = false;"),
    ]
    for name, old, new in [("actual", None, None), *mutants]:
        candidate = code
        if old is not None:
            assert candidate.count(old) == 1, name
            candidate = candidate.replace(old, new)
        cpp, exe = out / f"{name}.cpp", out / name
        cpp.write_text(candidate)
        built = subprocess.run([compiler, "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                                "-fno-sanitize-recover=all", "-I", str(ROOT / "modules/kern/src"),
                                str(cpp), "-o", str(exe)], capture_output=True, text=True)
        (out / f"{name}-build.log").write_text(built.stdout + built.stderr)
        assert built.returncode == 0, built.stdout + built.stderr
        run = subprocess.run([str(exe)], capture_output=True, text=True, timeout=20)
        (out / f"{name}-run.log").write_text(run.stdout + run.stderr)
        if old is None:
            assert run.returncode == 0, run.stdout + run.stderr
            print(run.stdout, end="")
        else:
            assert run.returncode != 0, f"surviving mutant {name}"
            print(f"Compiled semantic mutant {name} rejected")
