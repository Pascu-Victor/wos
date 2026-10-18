#!/usr/bin/env python3
"""Execute WOS wait/accounting bodies and real RunHeap with controlled IRQ time.

Only hardware clock, CLI/HLT, CPU migration and resource notification endpoints
are adapted, plus completed-wait membership admission for already-runnable
subjects. That endpoint asserts real heap membership and cannot repair a waiter;
the native scheduler tests exercise that separate transition. Actual arithmetic,
wait wrappers, bookkeeping and heap methods are compiled under ASan/UBSan.
--source-root tests saved pre-patch scheduler sources.
The added continuation cases use actual picker/cancel bodies and real RunHeap;
job-control/non-invalid publication endpoints are explicit finite refusals, not
models of APIC/assembly timing or new scheduling policy.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def block(source, pattern):
    matches = list(re.finditer(pattern, source, re.M))
    assert len(matches) == 1, (pattern, len(matches))
    start = matches[0].start()
    left = source.index('{', start)
    depth = 0
    for end in range(left, len(source)):
        depth += (source[end] == '{') - (source[end] == '}')
        if depth == 0:
            return source[start:end + 1]
    raise AssertionError(pattern)


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <utility>
namespace ker::mod::time {uint64_t now=100;uint64_t get_us(){return now;}}
namespace ker::mod::dbg {
template<auto> struct logger;
// The logger below is an endpoint only: every valid heap path must stay silent.
struct Silent {template<class... T>static void error(const char*,T...){assert(false);}};
void panic_handler(const char*){assert(false);}
}
namespace ker::mod::mm::virt {
int fixture_kernel_root;
void* get_kernel_pagemap(){return &fixture_kernel_root;}
}
namespace ker::mod::sched {
bool irq_enabled=true,query_ready=true;
unsigned guard_depth=0;
namespace task {
enum class TaskType {PROCESS,DAEMON,IDLE};
enum class WaitChannelKind {GENERIC,LOCAL_PIPE};
enum class TaskState {ACTIVE,DEAD,EXITING};
struct Task {
 TaskType type=TaskType::PROCESS;int sched_nice=0;HEAP_INDEX_DECLARATION
 uint64_t pid=1;void* thread=nullptr;void* pagemap=nullptr;
 struct {uint64_t syscall_kernel_stack=1,syscall_scratch_area=2;} context;
 std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<bool> gc_queued{false};
 bool has_exited=false,jobctl_stopped=false,interrupting_signal=false;
 bool has_interrupting_signal_pending() const{return interrupting_signal;}
 uint32_t sched_weight=1024,slice_ns=10000000,slice_used_ns=0;
 int64_t vruntime=0,vdeadline=10000000;
 bool blocked=false,wants_block=false;
 uint64_t wake_at_us=0,perf_wait_callsite=0;
 uint64_t syscall_account_start_us=0,precharged_syscall_time_us=0;
 uint64_t user_time_us=0,system_time_us=0;
 std::atomic<bool> cpu_accounting_finalized{false},wakeup_pending{false},scheduler_transition_active{false};
 struct {uint64_t cpu_time_us=0,cpu_user_time_us=0,cpu_system_time_us=0;} resources;
 bool is_voluntary_blocked() const{return blocked;}
 void set_voluntary_blocked(bool b){blocked=b;}
 void set_wait_channel(const char*,WaitChannelKind=WaitChannelKind::GENERIC){}
 void clear_wait_channel(){}
};
}
}
#include <platform/sched/run_heap.hpp>
'''
FACES = r'''
namespace ker::mod::sched {
namespace time=ker::mod::time;
struct RunQueue {
 RunHeap runnable_heap;
 task::Task* current_task=nullptr;
 uint64_t last_tick_us=100;
 int64_t total_weight=0,total_weighted_vruntime=0,min_vruntime=0;
 std::atomic<uint64_t> irq_uncharged_us{0},cpu_idle_us{0},cpu_system_us{0},cpu_user_us{0},cpu_nice_us{0};
};
enum class KernelWaitCompletionResult {UNCHANGED,REQUEUED,STALE_INDEX,HEAP_FULL};
KernelWaitCompletionResult complete_current_kernel_wait_locked(RunQueue* rq,task::Task* subject,uint64_t){
 assert(rq && rq->current_task==subject && rq->runnable_heap.contains(subject));
 return KernelWaitCompletionResult::UNCHANGED;
}
bool publish_runnable_task_locked(RunQueue*,task::Task*,const char*){assert(false);return false;}
void hcf(){assert(false);}
struct Queues {
 std::array<RunQueue,2> rows;unsigned cpu=0,locks=0;bool locked=false;
 template<class F>auto this_cpu_locked(F f){
  assert(!irq_enabled || guard_depth);assert(!locked);
  bool old=irq_enabled;irq_enabled=false;locked=true;++locks;
  auto value=f(&rows[cpu]);locked=false;irq_enabled=old;return value;
 }
 template<class F>void this_cpu_locked_void(F f){
  assert(!irq_enabled || guard_depth);assert(!locked);
  bool old=irq_enabled;irq_enabled=false;locked=true;++locks;
  f(&rows[cpu]);locked=false;irq_enabled=old;
 }
};
Queues* run_queues=nullptr;task::Task* current=nullptr;
bool can_query_current_task(){return query_ready && current && run_queues;}
task::Task* get_current_task(){return current;}
unsigned preempt_count(){return guard_depth;}
struct PreemptGuard {PreemptGuard(){++guard_depth;}~PreemptGuard(){--guard_depth;}};
namespace resource {
void charge_cpu(task::Task* p,uint64_t elapsed,uint64_t user=0){
 if(!p || p->type!=task::TaskType::PROCESS || !elapsed || p->cpu_accounting_finalized.load())return;
 assert(user<=elapsed);
 p->resources.cpu_time_us+=elapsed;p->resources.cpu_user_time_us+=user;
 p->resources.cpu_system_time_us+=elapsed-user;
}
}
void maybe_exit_current_kernel_thread_for_shutdown(){}
void note_preempt_disabled_block(const char*,uint64_t){}
bool interrupts_enabled(){return irq_enabled;}
void fixture_cli(){irq_enabled=false;}
void restore_interrupts_after_scheduler_wait(bool was_enabled){if(was_enabled)irq_enabled=true;}
void restore_process_syscall_park_interrupts(bool was_enabled,task::Task* p){
 if(!was_enabled && p && p->type==task::TaskType::PROCESS)irq_enabled=true;
}
constexpr uint64_t saturating_deadline_us(uint64_t a,uint64_t b){return b>UINT64_MAX-a?UINT64_MAX:a+b;}
enum class SchedulerHaltWaitKind {YIELD,BLOCK,PROCESS_PARK};
bool scheduler_wait_halt_once(task::Task*,SchedulerHaltWaitKind,bool);
// These independent job-control/corruption transitions are outside this finite
// ready-pair profile. Unexpected entry fails; no selector outcome is repaired.
bool park_job_control_stopped_task_locked(RunQueue* rq,task::Task* p){
 assert(rq && p && !p->jobctl_stopped);return false;
}
bool remove_from_heap_by_scan_locked(RunQueue*,task::Task*){assert(false);return false;}
void transition_mark_detached_locked(RunQueue*,task::Task*){assert(false);}
'''
HARDWARE = r'''
unsigned timer_mode=0,halt_calls=0;
bool cancel_wait=false,migrate_wait=false,halt_done=false;
void timer_tick(RunQueue* rq){
 auto* current_task=current;bool const IN_KERNEL_MODE=true;
'''
HALT = r'''
}
bool scheduler_wait_halt_once(task::Task* p,SchedulerHaltWaitKind,bool was_enabled){
 assert(!run_queues->locked);assert(p==current && p->blocked);
 if(cancel_wait || halt_done)return false;
 halt_done=true;++halt_calls;
 auto const begin=time::now;
 auto* rq=&run_queues->rows[run_queues->cpu];
 if(timer_mode){time::now=begin+(timer_mode==1?190:200);timer_tick(rq);}
 if(migrate_wait){
  if(!timer_mode){time::now=begin+190;timer_tick(rq);}
  assert(rq->runnable_heap.remove(p));rq->total_weight=0;rq->total_weighted_vruntime=0;
  rq->current_task=nullptr;run_queues->cpu=1;
  auto& incoming=run_queues->rows[1];incoming.current_task=p;
  incoming.last_tick_us=time::now;incoming.total_weight=p->sched_weight;
  incoming.min_vruntime=p->vruntime;assert(incoming.runnable_heap.insert(p));
 }
 time::now=begin+200;
 p->wants_block=false;p->wake_at_us=0;
 irq_enabled=was_enabled;
 return true;
}
}
using namespace ker::mod::sched;
'''
CASES = r'''
unsigned checks=0;
void check(bool b){++checks;assert(b);}
void initialise(Queues& q,task::Task& p,uint32_t weight){
 current=&p;run_queues=&q;query_ready=true;guard_depth=0;time::now=100;
 p.heap_index=-1;p.sched_weight=weight;q.rows[0].current_task=&p;q.rows[0].total_weight=weight;
 check(q.rows[0].runnable_heap.insert(&p));
}
void wait(unsigned kind){
 halt_done=false;
 switch(kind){
 case 0:kern_yield_impl(1);break;
 case 1:kern_block_impl(1);break;
 case 2:kern_sleep_us_impl(200,1);break;
 case 3:preemptible_syscall_park_impl("test",task::WaitChannelKind::LOCAL_PIPE,time::now+200,1);break;
 }
}
void pipeline(unsigned kind,uint32_t weight,unsigned tick,bool enabled,bool cancel,bool manual,bool migration,unsigned count){
 Queues q;task::Task p;initialise(q,p,weight);
 bool const process=kind==0 || kind==3;p.type=process?task::TaskType::PROCESS:task::TaskType::DAEMON;
 irq_enabled=enabled;timer_mode=tick;cancel_wait=cancel;migrate_wait=migration;halt_calls=0;
 time::now=300;begin_syscall_accounting();time::now=310;
 bool paused=false;
 if(manual){paused=pause_syscall_accounting();time::now+=20;}
 for(unsigned i=0;i<count;++i){
  if(i){time::now+=10;}
  wait(kind);
  check(!p.blocked && !p.wants_block && !p.wake_at_us);
  check(irq_enabled==(enabled || (kind==3 && process)));
  if(migration)migrate_wait=false;
 }
 if(paused)resume_syscall_accounting();
 time::now+=10;finish_syscall_accounting();
 time::now+=80;begin_syscall_accounting();time::now+=10;
 auto& last=q.rows[q.cpu];
 if(process)finalize_task_cpu_locked(&last,&p,time::now);
 else account_kernel_wait_boundary_locked(&last,&p,time::now,true);
 auto const extra=(manual?20:0)+10*(count-1);
 if(p.user_time_us!=(process?280:0) || p.system_time_us!=(process?30+extra:310+extra))
  std::fprintf(stderr,"pipeline kind=%u user=%llu system=%llu expected_user=%u expected_system=%u\n",kind,
   (unsigned long long)p.user_time_us,(unsigned long long)p.system_time_us,process?280:0,process?30+extra:310+extra);
 check(p.user_time_us==(process?280:0));
 check(p.system_time_us==(process?30+extra:310+extra));
 check(q.rows[0].cpu_idle_us.load()+q.rows[1].cpu_idle_us.load()==(cancel?0:200*count));
 if(process){
  check(p.resources.cpu_time_us==310+extra);
  check(p.resources.cpu_user_time_us==p.user_time_us && p.resources.cpu_system_time_us==p.system_time_us);
  check(p.cpu_accounting_finalized.load());
 }
 check(p.precharged_syscall_time_us==0);
 check(halt_calls==(cancel?0:count));
 check(last.runnable_heap.contains(&p));
 check(last.runnable_heap.remove(&p));
}
'''
NEW_CASES = r'''
void boundaries_and_guards(){
 // Actual existing and new production selftests, including the real heap.
 check(scheduler_selftest_final_cpu_slice_freezes_accounting());
 check(scheduler_selftest_resource_cpu_split_matches_task());
 check(scheduler_selftest_wait_boundaries_preserve_cpu_and_fairness());
 check(scheduler_selftest_blocked_runtime_excludes_eevdf_and_irq());
 for(unsigned state=0;state<3;++state){
  Queues q;task::Task p;initialise(q,p,1024);irq_enabled=false;
  time::now=300;begin_syscall_accounting();time::now=310;
  if(state==0)guard_depth=1;
  if(state==1)p.scheduler_transition_active=true;
  if(state==2)query_ready=false;
  auto const token=begin_kernel_wait_accounting(&p);
  check(!token.accounted_boundary && p.blocked && q.locks==0);
  finish_kernel_wait_accounting(&p,token);check(!p.blocked && q.locks==0);
  check(p.syscall_account_start_us==(state==2?300:310));
  guard_depth=0;query_ready=true;
 }
 {
  Queues q;task::Task p,foreign;initialise(q,p,1024);irq_enabled=false;
  p.syscall_account_start_us=200;time::now=310;
  auto token=begin_kernel_wait_accounting(&foreign);finish_kernel_wait_accounting(&foreign,token);
  check(!token.accounted_boundary && !token.paused_syscall && p.syscall_account_start_us==200 && q.locks==0);
 }
 {
  Queues q;task::Task p;initialise(q,p,1024);irq_enabled=false;p.blocked=true;
  time::now=310;auto outer=begin_kernel_wait_accounting(&p);
  time::now=400;auto inner=begin_kernel_wait_accounting(&p);
  time::now=500;finish_kernel_wait_accounting(&p,inner);check(p.blocked);
  time::now=510;finish_kernel_wait_accounting(&p,outer);check(p.blocked);
  check(p.user_time_us==0 && p.system_time_us==0 && p.vruntime==0 && q.rows[0].cpu_idle_us==410);
 }
 {
  Queues q;task::Task p;initialise(q,p,1024);irq_enabled=false;
  p.cpu_accounting_finalized=true;p.user_time_us=7;p.system_time_us=11;time::now=310;
  auto token=begin_kernel_wait_accounting(&p);time::now=510;finish_kernel_wait_accounting(&p,token);
  check(p.user_time_us==7 && p.system_time_us==11 && !p.syscall_account_start_us && p.resources.cpu_time_us==0);
 }
 for(uint32_t weight:{335U,1024U,3121U}){
  Queues q;task::Task p,peer;initialise(q,p,weight);auto& rq=q.rows[0];
  peer.heap_index=-1;peer.sched_weight=1024;peer.vruntime=-100;peer.vdeadline=1;
  check(rq.runnable_heap.insert(&peer));rq.total_weight+=peer.sched_weight;
  rq.total_weighted_vruntime=-100*1024;
  p.slice_ns=100000;p.slice_used_ns=99999;p.syscall_account_start_us=300;
  rq.irq_uncharged_us=20;
  account_kernel_wait_boundary_locked(&rq,&p,310,true);
  auto const running=p.vruntime;
  check(running==vruntime_delta_from_runtime_ns(190000,weight));
  check(p.user_time_us==180 && p.system_time_us==10 && p.slice_used_ns==0);
  check(p.vdeadline==vdeadline_from_slice(running,p.slice_ns,weight));
  check(rq.runnable_heap.peek_min()==&peer);
  check(rq.total_weighted_vruntime==(p.vruntime-rq.min_vruntime)*weight+(peer.vruntime-rq.min_vruntime)*1024);
  account_kernel_wait_boundary_locked(&rq,&p,510,false);
  check(p.vruntime==running && rq.cpu_idle_us==200 && rq.irq_uncharged_us==0);
  auto const mark=rq.last_tick_us;account_kernel_wait_boundary_locked(&rq,&p,510,true);
  account_kernel_wait_boundary_locked(&rq,&peer,600,true);check(rq.last_tick_us==mark);
  check(rq.runnable_heap.remove(&p));
  account_kernel_wait_boundary_locked(&rq,&p,520,true);
  check(p.vruntime==running && p.heap_index==-1);
 }
 {
  Queues q;task::Task p;initialise(q,p,1024);auto& rq=q.rows[0];
  irq_enabled=false;p.syscall_account_start_us=150;
  check(charge_syscall_time_until(&p,170,false));time::now=190;
  auto token=begin_kernel_wait_accounting(&p);
  check(p.user_time_us==50 && p.system_time_us==40 && p.precharged_syscall_time_us==0);
  time::now=390;finish_kernel_wait_accounting(&p,token);
  check(p.user_time_us==50 && p.system_time_us==40 && rq.cpu_idle_us==200);
 }
 check(!begin_kernel_wait_accounting(nullptr).accounted_boundary);
 finish_kernel_wait_accounting(nullptr,{});
}
'''



WOKEN_WAIT_CASES = r'''
void woken_wait_continuation(){
 Queues q;task::Task worker,parent;initialise(q,worker,1024);auto& rq=q.rows[0];
 worker.type=task::TaskType::DAEMON;worker.pid=8;
 worker.pagemap=ker::mod::mm::virt::get_kernel_pagemap();
 worker.vdeadline=1;rq.runnable_heap.update(&worker);
 parent.pid=108;parent.thread=&parent;parent.pagemap=&parent;parent.heap_index=-1;
 parent.vruntime=0;parent.vdeadline=2;
 check(rq.runnable_heap.insert(&parent));rq.total_weight+=parent.sched_weight;
 irq_enabled=false;
 auto const accounting=begin_kernel_wait_accounting(&worker);
 check(!accounting.was_blocked && worker.blocked && !worker.wants_block);
 worker.wakeup_pending.store(true,std::memory_order_release);
 // Model repeated timer selection BEFORE the resumed owner gets an instruction.
 // The picker, real heap and token are production; hardware delivery is not.
 for(unsigned early_tick=0;early_tick<64;++early_tick){
  auto* selected=q.this_cpu_locked([&](RunQueue* locked){
   return pick_best_eligible_for_switch_locked(locked,compute_avg_vruntime(locked),&worker);
  });
  check(selected==&worker);
  check(worker.wakeup_pending.load(std::memory_order_acquire));
  check(worker.blocked && rq.runnable_heap.contains(&worker) && rq.runnable_heap.contains(&parent));
 }
 // Selection does not consume the event: actual owner cancellation does.
 check(scheduler_wait_should_cancel(&worker,SchedulerHaltWaitKind::BLOCK));
 check(!worker.wakeup_pending.load(std::memory_order_acquire));
 finish_kernel_wait_accounting(&worker,accounting);
 check(!worker.blocked && !worker.wants_block && rq.runnable_heap.contains(&worker));
 // Preserve the old alternate rule for a genuinely unwoken voluntary owner.
 worker.blocked=true;worker.wakeup_pending.store(false,std::memory_order_release);
 auto* unwoken=q.this_cpu_locked([&](RunQueue* locked){
  return pick_best_eligible_for_switch_locked(locked,compute_avg_vruntime(locked),&worker);
 });
 check(unwoken==&parent && !worker.wakeup_pending.load(std::memory_order_acquire));
 // Pending does not boost priority: a naturally earlier eligible peer wins.
 worker.wakeup_pending.store(true,std::memory_order_release);
 parent.vdeadline=0;rq.runnable_heap.update(&parent);
 auto* earlier=q.this_cpu_locked([&](RunQueue* locked){
  return pick_best_eligible_for_switch_locked(locked,compute_avg_vruntime(locked),&worker);
 });
 check(earlier==&parent && worker.wakeup_pending.load(std::memory_order_acquire));
 // Return the natural EEVDF win to worker; do not manufacture a new quantum.
 parent.vdeadline=2;rq.runnable_heap.update(&parent);
 auto* ready=q.this_cpu_locked([&](RunQueue* locked){
  return pick_best_eligible_for_switch_locked(locked,compute_avg_vruntime(locked),&worker);
 });
 check(ready==&worker && worker.wakeup_pending.load(std::memory_order_acquire));
 check(scheduler_wait_should_cancel(&worker,SchedulerHaltWaitKind::BLOCK));
 worker.blocked=false;
 check(rq.runnable_heap.remove(&parent));check(rq.runnable_heap.remove(&worker));
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-root', type=Path, default=ROOT)
    parser.add_argument('--keep', type=Path)
    args = parser.parse_args()
    source = (args.source_root / 'modules/kern/src/platform/sched/scheduler.cpp').read_text()
    header = (args.source_root / 'modules/kern/src/platform/sched/scheduler.hpp').read_text()
    new = 'auto begin_kernel_wait_accounting(' in source
    heap = (ROOT / 'modules/kern/src/platform/sched/run_heap.cpp').read_text()
    heap = heap[heap.index('namespace ker::mod::sched {'):heap.index('// IntrusiveTaskList -')]
    heap = heap.replace('using log = ker::mod::dbg::logger<"runheap">;', 'using log = ker::mod::dbg::Silent;') + '\n}\n'
    names = [
        ('inline auto', 'runtime_delta_ns_from_us'), ('inline auto', 'vruntime_delta_from_runtime_ns'),
        ('inline auto', 'vdeadline_from_slice'), ('inline auto', 'saturating_i64_mul'),
        ('inline void', 'add_weighted_vruntime_delta'), ('inline auto', 'accumulate_slice_used_ns'),
        ('inline int64_t', 'compute_avg_vruntime'), ('auto', 'consume_precharged_syscall_time'),
        ('auto', 'consume_irq_time_for_runtime'), ('void', 'account_cpu_runtime_delta'),
        ('auto', 'charge_syscall_time_until'), ('void', 'account_task_runtime_delta'),
        ('auto', 'account_runtime_slice')]
    if new:
        names.append(('void', 'account_kernel_wait_boundary_locked'))
    names += [('void', 'finalize_task_cpu_locked'), ('void', 'begin_syscall_accounting'),
              ('void', 'resume_syscall_accounting'), ('auto', 'pause_syscall_accounting'),
              ('void', 'finish_syscall_accounting')]
    if new:
        names += [('auto', 'begin_kernel_wait_accounting'), ('void', 'finish_kernel_wait_accounting')]
    names += [('auto', 'scheduler_selftest_final_cpu_slice_freezes_accounting'),
              ('auto', 'scheduler_selftest_resource_cpu_split_matches_task')]
    if new:
        names += [('auto', 'scheduler_selftest_wait_boundaries_preserve_cpu_and_fairness'),
                  ('auto', 'scheduler_selftest_blocked_runtime_excludes_eevdf_and_irq')]
    if new:
        # Actual picker and eligibility bodies; no replacement selector is defined.
        names += [('inline auto', 'has_kernel_scheduler_context'), ('inline auto', 'is_publishable_runnable_task'),
                  ('inline auto', 'run_heap_entry'), ('inline auto', 'pick_best_eligible_excluding_locked'),
                  ('inline auto', 'pick_best_eligible_for_switch_locked')]
    bodies = []
    for prefix, name in names:
        # Reject forward declarations; only current definitions are executable evidence.
        bodies.append(block(source, '^' + prefix + ' ' + name + r'\([^;{}]*?\)\s*(?:->[^;{]+)?\{'))
    if new:
        bodies.append(block(header, r'^inline auto scheduler_wait_should_cancel\([^;{}]*?\)\s*->[^;{]+\{'))
    token = block(header, r'^struct KernelWaitAccounting ') + ';\n' if new else ''
    start = source.index('        // Compute time delta since last tick\n')
    end = source.index('            // kern_yield() sleepers', start)
    timer_prefix = source[start:end] + '\n }\n'
    start = source.index('        uint64_t const DELTA_NS = runtime_delta_ns_from_us(chargeable_us);', end)
    timer_math = source[start:source.index('        // Perf: periodic CPU sample', start)]
    start = source.index('        // Update vruntime if task is in the heap\n', start)
    timer_math += source[start:source.index('        // User frames retain', start)]
    wrappers = []
    for name in ('kern_yield_impl', 'kern_block_impl', 'kern_sleep_us_impl', 'preemptible_syscall_park_impl'):
        pattern = '^inline void ' + name + r'\([^;{]+?\)\s*\{'
        if name == 'preemptible_syscall_park_impl':
            pattern = r'^inline void preemptible_syscall_park_impl\(const char\* wait_channel, task::WaitChannelKind wait_kind, uint64_t deadline_us,[^;{]+?\)\s*\{'
        body = block(header, pattern)
        # Only privileged hardware CLI is replaced. Preserve the exact wrapper
        # order and its original interrupt-state save/restore implementation calls.
        body = body.replace('asm volatile("cli" ::: "memory");', 'fixture_cli();')
        assert 'asm volatile' not in body
        wrappers.append(body)
    task_header = (ROOT / 'modules/kern/src/platform/sched/task.hpp').read_text()
    heap_index = re.findall(r'^\s*(int32_t heap_index[^;]*;)', task_header, re.M)
    assert len(heap_index) == 1, heap_index
    # Default construction must match the kernel; each published fixture task
    # establishes detached heap membership explicitly, as the scheduler does.
    prelude = PRELUDE.replace('HEAP_INDEX_DECLARATION', heap_index[0])
    code = prelude + heap + FACES + token + '\n'.join(bodies) + '\n'.join(wrappers)
    code += HARDWARE + timer_prefix + timer_math + HALT + CASES
    # Saved sources have no boundary helper; the daemon branch in pipeline
    # needs a faithful old endpoint solely to compile. The first process case
    # executes the actual old wait wrapper and fails before that branch.
    if not new:
        code = code.replace('else account_kernel_wait_boundary_locked(&last,&p,time::now,true);',
                            'else account_runtime_slice(&last,&p,time::now,time::now-last.last_tick_us,true,true);')
    else:
        code += NEW_CASES
    if new:
        code += WOKEN_WAIT_CASES
    code += r'''
int main(){
 unsigned pipelines=0;
 for(unsigned kind=0;kind<4;++kind)for(uint32_t weight:{335U,1024U,3121U})
 for(unsigned tick=0;tick<3;++tick)for(bool enabled:{false,true})
 for(bool cancel:{false,true})for(bool manual:{false,true})
 for(bool migration:{false,true})for(unsigned count:{1U,2U}){
  // The controlled migration endpoint models one transfer per pipeline.
  pipeline(kind,weight,tick,enabled,cancel,manual,migration,count);++pipelines;
 }
''' + ('boundaries_and_guards();\n' if new else '') + ('woken_wait_continuation();\n' if new else '') + r'''
 std::printf("actual wait/accounting/RunHeap: %u pipelines, %u assertions PASS\n",pipelines,checks);
}
'''
    def run(folder):
        folder.mkdir(parents=True, exist_ok=True)
        cpp = folder / 'check.cpp'
        cpp.write_text(code)
        subprocess.run(['/usr/bin/clang++', '-std=c++23', '-O1', '-g', '-Wall', '-Wextra',
                        '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                        '-I' + str(ROOT / 'modules/kern/src'), str(cpp), '-o', str(folder / 'check')], check=True)
        subprocess.run([str(folder / 'check')], check=True, env={
            **os.environ, 'ASAN_OPTIONS': 'detect_leaks=0:halt_on_error=1',
            'UBSAN_OPTIONS': 'halt_on_error=1'})
    if args.keep:
        run(args.keep)
    else:
        with tempfile.TemporaryDirectory(prefix='wos-kernel-wait-') as td:
            run(Path(td))


if __name__ == '__main__':
    main()
