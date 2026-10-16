#!/usr/bin/env python3
"""Execute THREAD_CREATE pressure handling with actual factory error classification.

GC and the remaining task construction are host seams; this checks bounded retries, ownership
lock ordering and the real IRQ-off syscall context, not live epoch reclamation or the
128-thread runtime gate. Hardware IF instructions, preparation/lazy-clone context and
journal output are host seams. The actual Policy header supplies the captured value;
the retry oracle changes the parent policy during GC and requires the original value.
"""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = ROOT
PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <platform/mm/numa_policy.hpp>
int publication_depth=0, pool_depth=0, calls=0, reclaims=0, attached=0, posted=0, destroyed=0, exits=0, pending_checks=0;
int failures=0, reservation_error=0, pressure_logs=0, gc_requests=0, policy_snapshots=0;
using Policy = ker::mod::mm::numa::Policy;
using PolicyMode = ker::mod::mm::numa::Mode;
namespace ker::mod::sched { PLACEMENT_TYPE }
using Placement = ker::mod::sched::TaskPlacementSnapshot;
Placement expected_placement{3,8,17,true,true};
int placement_snapshots=0, placement_initialized=0, placement_capture_error=0, placement_init_error=0;
int gc_context_after_capture=-1;
bool lose_topology_after_capture=false, gc_changes_placement=false;
void apply_late_gc_context();
Policy expected_policy{uint64_t{1} << 63, PolicyMode::PREFERRED};
bool gc_changes_policy=false;
bool pass_ran=true, pass_time=false, pass_pending=false;
bool irq=false, entry_irq=false, queues=true, query=true, panic=false, wrong_current=false;
void enable_interrupts() { assert(publication_depth==0 && pool_depth==0); irq=true; }
void disable_interrupts() { assert(publication_depth==0 && pool_depth==0); irq=false; }
namespace ker::mod::io::serial { bool is_panic_mode() { return panic; } }
bool concurrently_freed=false, gc_changes_if=false;
bool safe=true, stack_pressure=true, recover=true, publish_ok=true, post_ok=true, copy_ok=true;
uint64_t cpus=8, published_tid=0;
namespace ker::mod::mm::phys {
void* kernel_stack_alloc(const char*) {
 assert(publication_depth==1);
 if (failures && stack_pressure) { --failures; return nullptr; }
 return reinterpret_cast<void*>(4096);
}
}
namespace ker::mod::dbg {
void log(const char*) {}
struct FixedString { template<size_t N> consteval FixedString(const char (&)[N]) {} };
template<FixedString Tag> struct logger {
 template<class... Args> static void info(const char*,Args...) {
  assert(publication_depth==0 && pool_depth==0 && !irq); ++pressure_logs;
 }
};
}
namespace ker::mod::sched::task {
struct User { uint32_t real=0; }; struct Cred { User user() const { return {}; } };
enum class TaskType { PROCESS, DAEMON };
class LazyVmemClone;
struct Task {
 TaskType type=TaskType::PROCESS;
 uint64_t syscall_account_start_us=1;
 std::atomic<bool> scheduler_transition_active{false};
 bool deferred_task_switch=false, wants_block=false, voluntary=false;
 bool is_voluntary_blocked() const { return voluntary; }
 uint64_t pid=0x1234567887654321ULL;
 Cred credentials() const { return {}; }
 Policy policy{};
 Placement placement{};
 Policy memory_policy_snapshot() const {
  assert(publication_depth==0 && pool_depth==0); ++policy_snapshots; return policy;
 }
 uint64_t namespace_tid() const { return pid; }
 bool try_acquire_lifetime_ref() { assert(publication_depth==1); return true; }
 void release() { assert(publication_depth==1); }
 static Task* create_user_thread(Task*,uint64_t tcb,uint64_t sp,uint64_t enter,uint64_t entry,uint64_t arg,
                                 Policy memory_policy,bool* stack_exhausted,LazyVmemClone* prepared_lazy) {
  assert(!irq && prepared_lazy && policy_snapshots==1 && memory_policy==expected_policy);
  assert(publication_depth==1 && pool_depth==0 && tcb==256 && sp==512 && enter==768 && entry==1024 && arg==1280);
  ++calls;
  apply_late_gc_context();
  PROLOGUE
  if (failures && !stack_pressure) { --failures; return nullptr; }
  if(lose_topology_after_capture) cpus=0;
  static Task child; child.placement={}; return &child;
 }
};
class LazyVmemClone {
 public:
 int prepare(Task&) { assert(publication_depth==0 && pool_depth==0 && irq); return 0; }
 bool matches(Task&) const { assert(publication_depth==1 && !irq); return true; }
};
Task parent, other;
void destroy_unpublished_user_thread(Task* t) { assert(publication_depth==0 && pool_depth==0 && irq && t); ++destroyed; published_tid=0; }
}
namespace ker::mod::sched {
auto get_current_task() -> task::Task* { return wrong_current ? &task::other : &task::parent; }
int capture_task_placement_snapshot(task::Task* subject, TaskPlacementSnapshot& output) {
 assert(subject==&task::parent && publication_depth==0 && pool_depth==0 && !calls && !placement_snapshots);
 ++placement_snapshots;
 if(placement_capture_error) return placement_capture_error;
 if(cpus==0) return -ENODEV;
 if(cpus>64) return -ERANGE;
 output=subject->placement; return 0;
}
int initialize_inherited_task_placement(task::Task* child,const TaskPlacementSnapshot& copied) {
 assert(publication_depth==1 && placement_snapshots==1 && copied==expected_placement);
 ++placement_initialized;
 if(placement_init_error) return placement_init_error;
 child->placement=copied; return 0;
}
bool has_run_queues() { return queues; }
bool can_query_current_task() { return query; }
uint32_t preempt_count() { return safe ? 0 : 1; }
bool interrupts_enabled() { return irq; }
REPORT
struct GcPassResult {
 uint32_t reclaimed=0;uint64_t scanned=0;bool time_budget_exhausted=false,has_pending_work=false,ran=false;
};
constexpr uint32_t SCHED_GC_PRESSURE_RECLAIM_BUDGET=512;
constexpr uint64_t SCHED_GC_PRESSURE_WORK_BUDGET_US=50000;
constexpr uint32_t SCHED_GC_PRESSURE_PAGEMAP_STEP_BUDGET=65536;
std::atomic<bool> scheduler_gc_memory_pressure_requested{false};
void request_gc_memory_pressure() { ++gc_requests; }
GcPassResult run_gc_reclaim_pass_exclusive(uint32_t tasks,uint64_t time,uint32_t pages,uint64_t scan,GcPressureDiagnostics* d) {
 assert(publication_depth==0 && pool_depth==0 && safe && irq);
 assert(tasks==512 && time==50000 && pages==65536 && scan==UINT64_MAX);++reclaims;
 if(!pass_ran) return {};
 if(recover || concurrently_freed) failures=0;
 if(gc_changes_if) irq=!entry_irq;
 if(gc_changes_policy) task::parent.policy={3,PolicyMode::BIND};
 if(gc_changes_placement) task::parent.placement={1,3,203,false,false};
 if(d) { d->epoch_skips=3;d->current_skips=2;d->refcount_skips=1; }
 return {.reclaimed=recover ? 1U : 0U,.scanned=7,.time_budget_exhausted=pass_time,.has_pending_work=pass_pending,.ran=true};
}
FOLLOWUP
uint32_t reclaim_memory_pressure(GcPressureDiagnostics* diagnostics=nullptr);
WRAPPER
bool post_task_round_robin_inherited(task::Task* child,const TaskPlacementSnapshot& copied,uint64_t) {
 assert(publication_depth==1 && placement_snapshots==1 && placement_initialized==1);
 assert(copied==expected_placement && child->placement==copied);
 assert(copied.owner_cpu<cpus); ++posted; return post_ok;
}
namespace resource {
struct ProcessReservation {
 ProcessReservation(task::Task*,uint32_t) {}
 int result() const { return reservation_error; }
 bool attach(task::Task&) { assert(publication_depth==1); ++attached; return true; }
};
}
}
namespace ker::mod::smt { uint64_t get_core_count() { return cpus; } }
// Explicit host context seams; the THREAD_CREATE body and pressure helper below
// remain production extractions. Model the IF scopes used by ordinary staging.
namespace ker::mod::sys {
class SyscallPreparationScope {
 bool original_;
 public:
 explicit SyscallPreparationScope(sched::task::Task&) : original_(irq) {
  assert(publication_depth==0 && pool_depth==0); irq=true;
 }
 ~SyscallPreparationScope() { assert(publication_depth==0 && pool_depth==0); irq=original_; }
 bool ready() const { return true; }
 class LegacyInterval {
  bool original_;
  public:
  explicit LegacyInterval(SyscallPreparationScope&) : original_(irq) {
   assert(publication_depth==0 && pool_depth==0); irq=false;
  }
  ~LegacyInterval() { assert(publication_depth==0 && pool_depth==0); irq=original_; }
 };
};
}
namespace ker::mod::sched::threading {
bool publish_tid_to_tcb(task::Task&,uint64_t,uint64_t pid,bool touch) {
 assert(publication_depth==1 && !touch); if(pid && !publish_ok) return false; published_tid=pid; return true;
}
}
namespace ker::mod::sys::usercopy {
bool range_valid(uint64_t,size_t) { return copy_ok; }
bool ensure_writable(ker::mod::sched::task::Task&,uint64_t,size_t) { return copy_ok; }
bool copy_from_task(ker::mod::sched::task::Task&,uint64_t,void* p,size_t n) { assert(n==16); auto* w=static_cast<uint64_t*>(p); w[0]=1024; w[1]=1280; return copy_ok; }
}
namespace ker::mod::sys::signal {
constexpr size_t WOS_TCB_SIGNAL_CACHE_BYTES=64;
void sync_task_signal_mask_cache_mapped(ker::mod::sched::task::Task*) { assert(publication_depth==1); }
}
namespace ker::syscall::vmem {
struct SharedVmemPublicationGuard {
 SharedVmemPublicationGuard() { assert(publication_depth++==0 && pool_depth==0); }
 ~SharedVmemPublicationGuard() { assert(publication_depth--==1); }
};
}
namespace ker::syscall::process {
void exit_current_if_process_exit_requested() { assert(publication_depth==0); ++exits; }
int inherit_pending_process_exit_request(ker::mod::sched::task::Task*,ker::mod::sched::task::Task*) { assert(publication_depth==1); ++pending_checks; return 0; }
}
namespace ker::syscall::multiproc {
HELPER
constexpr uint64_t MLIBC_TCB_TID_OFFSET=0x18;
std::atomic<uint64_t> next_thread_cpu{0};
bool publish_thread_tid_to_tcb(ker::mod::sched::task::Task*,uint64_t,uint64_t pid) {
 assert(publication_depth==1); if(pid && !publish_ok) return false; published_tid=pid; return true;
}
auto execute() -> uint64_t {
 uint64_t arg1=256,arg2=512,arg3=768;
BODY
}
}
// Finite endpoint controls change context only AFTER successful entry capture.
// They preserve old GC refusal boundaries; they do not model production hotplug.
void apply_late_gc_context() {
 assert(placement_snapshots==1);
 int const field=gc_context_after_capture; auto& p=ker::mod::sched::task::parent;
 switch(field) {
   case 0:queues=false;break;case 1:query=false;break;case 2:panic=true;break;
   case 3:p.type=ker::mod::sched::task::TaskType::DAEMON;break;
   case 4:p.syscall_account_start_us=0;break;case 5:p.scheduler_transition_active=true;break;
   case 6:p.deferred_task_switch=true;break;case 7:p.wants_block=true;break;case 8:p.voluntary=true;break;
 }
}
void reset() {
 assert(publication_depth==0 && pool_depth==0);
 calls=reclaims=attached=posted=destroyed=exits=pending_checks=failures=reservation_error=pressure_logs=gc_requests=policy_snapshots=0;
 gc_changes_policy=false;
 placement_snapshots=placement_initialized=placement_capture_error=placement_init_error=0;
 gc_context_after_capture=-1;lose_topology_after_capture=gc_changes_placement=false;
 pass_ran=true;pass_time=pass_pending=false;ker::mod::sched::scheduler_gc_memory_pressure_requested=false;
 safe=stack_pressure=recover=publish_ok=post_ok=copy_ok=true;concurrently_freed=gc_changes_if=false;cpus=8;published_tid=0;
 irq=entry_irq=false;queues=query=true;panic=wrong_current=false;
 auto& p=ker::mod::sched::task::parent;p.type=ker::mod::sched::task::TaskType::PROCESS;p.policy=expected_policy;p.placement=expected_placement;
 p.syscall_account_start_us=1;p.scheduler_transition_active=false;p.deferred_task_switch=p.wants_block=p.voluntary=false;
}
auto run() -> int64_t { return static_cast<int64_t>(ker::syscall::multiproc::execute()); }
int main() {
 reset(); assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid); assert(calls==1 && !reclaims && attached==1 && posted==1 && exits==2 && pending_checks==1);
 reset(); failures=1; assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid); assert(calls==2 && reclaims==1 && attached==1 && posted==1 && pending_checks==1);
 reset(); failures=9; recover=false; assert(run()==-ENOMEM); assert(calls==2 && reclaims==1 && !attached && !posted && !published_tid);
 reset(); failures=1; safe=false; assert(run()==-ENOMEM); assert(calls==1 && !reclaims && !attached);
 reset(); failures=1; stack_pressure=false; assert(run()==-ENOMEM); assert(calls==1 && !reclaims && !attached);
 reset(); publish_ok=false; assert(run()==-EFAULT); assert(calls==1 && !reclaims && destroyed==1 && !attached && !posted && !published_tid);
 reset(); post_ok=false; assert(run()==-ENOMEM); assert(calls==1 && !reclaims && destroyed==1 && attached==1 && posted==1 && !published_tid);
 reset(); lose_topology_after_capture=true; assert(run()==-ENOMEM); assert(calls==1 && !reclaims && destroyed==1 && !attached && !posted);
 reset(); copy_ok=false; assert(run()==-EFAULT); assert(!calls && !reclaims && !attached);
 reset(); reservation_error=-EAGAIN; assert(run()==-EAGAIN); assert(!calls && !reclaims && !attached);
 reset(); failures=1;recover=false;concurrently_freed=true;assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid);assert(calls==2 && reclaims==1 && attached==1 && posted==1);
 assert(pressure_logs==1 && !irq);
 reset(); irq=entry_irq=true;failures=1;assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid);assert(calls==2 && reclaims==1 && pressure_logs==1 && irq);
 for(bool original_if : {false,true}) {
  reset();irq=entry_irq=original_if;gc_changes_if=true;failures=1;
  assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid && irq==original_if && reclaims==1);
 }
 for(int field=0;field<9;++field) {
  reset();failures=1;gc_context_after_capture=field;
  assert(run()==-ENOMEM && calls==1 && !reclaims && !pressure_logs && !irq);
 }
 reset();assert(!ker::syscall::multiproc::reclaim_thread_stack_from_syscall(nullptr));
 wrong_current=true;assert(!ker::syscall::multiproc::reclaim_thread_stack_from_syscall(&ker::mod::sched::task::parent));
 assert(!reclaims && !pressure_logs && !irq);
 for(int scenario=0;scenario<4;++scenario) {
  reset();irq=entry_irq=true;
  pass_ran=scenario!=0;pass_time=scenario==2;pass_pending=scenario==3;
  ker::mod::sched::GcPressureDiagnostics d{};d.scanned=d.epoch_skips=d.epoch_still_young=99;d.ran=true;
  auto n=ker::mod::sched::reclaim_memory_pressure(&d);
  assert(d.ran==pass_ran && n==d.reclaimed && d.epoch_still_young==0);
  if(!pass_ran) assert(n==0 && d.scanned==0 && d.epoch_skips==0 && gc_requests==1);
  else assert(n==1 && d.scanned==7 && d.epoch_skips==3 && d.current_skips==2 && d.refcount_skips==1);
  assert(d.time_budget_exhausted==pass_time && d.deferred_pending==pass_pending);
  assert(gc_requests==(scenario==1 ? 0 : 1));
 }
 // Actual THREAD_CREATE must snapshot before staging/PUB, once across GC retry.
 // Check all modes and high-bit masks using the actual production value type.
 for(Policy policy : {Policy{},Policy{uint64_t{1} << 63,PolicyMode::PREFERRED},
                      Policy{3,PolicyMode::BIND},Policy{5,PolicyMode::INTERLEAVE}}) {
  expected_policy=policy; reset(); failures=1;gc_changes_policy=true;
  assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid);
  assert(calls==2 && reclaims==1 && policy_snapshots==1);
  assert(ker::mod::sched::task::parent.policy==Policy(3,PolicyMode::BIND));
 }
 reset();cpus=0;assert(run()==-ENODEV && !calls && !attached && placement_snapshots==1);
 reset();cpus=65;assert(run()==-ERANGE && !calls && !attached);
 reset();placement_capture_error=-EBUSY;assert(run()==-EBUSY && !calls && !attached);
 reset();placement_init_error=-EINVAL;assert(run()==-EINVAL && calls==1 && destroyed==1 && !attached && !published_tid);
 for(Placement placement : {Placement{3,8,17,true,true},Placement{1,10,73,false,true},Placement{63,UINT64_MAX,91,true,false}}) {
  expected_placement=placement;reset();if(placement.owner_cpu==63)cpus=64;
  failures=1;gc_changes_placement=true;
  assert(static_cast<uint64_t>(run())==ker::mod::sched::task::parent.pid);
  assert(calls==2 && reclaims==1 && placement_snapshots==1 && placement_initialized==1);
  assert(ker::mod::sched::task::parent.placement==Placement(1,3,203,false,false));
 }
 puts("PASS actual THREAD_CREATE IRQ-off/IRQ-on pressure recovery, unsafe contexts, publication and captured-policy retry checks");
}
'''


def source():
    def read(relative):
        path = SOURCE_ROOT / relative
        return (path if path.is_file() else ROOT / relative).read_text()
    text = read('modules/kern/src/syscalls_impl/multiproc/threadControl.cpp')
    start = text.index('case abi::multiproc::threadControlOps::THREAD_CREATE: {')
    begin = text.index('{', start) + 1
    end, depth = begin, 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    body = text[begin:end - 1]
    h = text.index('auto reclaim_thread_stack_from_syscall(')
    begin_h = text.index('{', h)
    end_h, depth_h = begin_h + 1, 1
    while depth_h:
        depth_h += (text[end_h] == '{') - (text[end_h] == '}')
        end_h += 1
    helper = text[h:end_h]
    sti, cli = 'asm volatile("sti" ::: "memory");', 'asm volatile("cli" ::: "memory");'
    assert helper.count(sti) == 2 and helper.count(cli) == 1
    helper = helper.replace(sti, 'enable_interrupts();').replace(cli, 'disable_interrupts();')
    factory = read('modules/kern/src/platform/sched/task.cpp')
    a = factory.index('{', factory.index('Task* Task::create_user_thread(')) + 1
    b = factory.index('    uint64_t const K_RSP', a)
    header = read('modules/kern/src/platform/sched/scheduler.hpp')
    aa = header.index('struct GcPressureDiagnostics {')
    bb, depth = header.index('{', aa) + 1, 1
    while depth:
        depth += (header[bb] == '{') - (header[bb] == '}')
        bb += 1
    bb += 1
    scheduler = read('modules/kern/src/platform/sched/scheduler.cpp')
    def function(name):
        aa = scheduler.index('auto ' + name + '(')
        brace = scheduler.index('{', aa)
        bb, depth = brace + 1, 1
        while depth:
            depth += (scheduler[bb] == '{') - (scheduler[bb] == '}')
            bb += 1
        return scheduler[aa:bb]
    pa = header.index('struct TaskPlacementSnapshot {')
    pb = header.index('\n};', pa) + 3
    return (PRELUDE.replace('PLACEMENT_TYPE', header[pa:pb]).replace('PROLOGUE', factory[a:b]).replace('BODY', body).replace('HELPER', helper)
            .replace('REPORT', header[aa:bb]).replace('WRAPPER', function('reclaim_memory_pressure'))
            .replace('FOLLOWUP', function('gc_pass_needs_followup')))


def main():
    compiler = os.environ.get('HOST_CXX') or shutil.which('g++', path=os.defpath) or shutil.which('clang++', path=os.defpath)
    assert compiler, 'host compiler missing'
    actual = source()
    mutations = {
        'retry_policy_resnapshot': actual.replace('prepared_stack_words.at(1), MEMORY_POLICY,', 'prepared_stack_words.at(1), (static_cast<void>(MEMORY_POLICY), parent->memory_policy_snapshot()),'),
        'retry_placement_resnapshot': actual.replace('if (int const ERROR = mod::sched::initialize_inherited_task_placement(t, inherited_placement);', 'if (int const ERROR = (static_cast<void>(mod::sched::capture_task_placement_snapshot(parent, inherited_placement)), mod::sched::initialize_inherited_task_placement(t, inherited_placement));'),
        'lost_placement_value': actual.replace('post_task_round_robin_inherited(t, inherited_placement, PLACEMENT_SEED)', 'post_task_round_robin_inherited(t, mod::sched::TaskPlacementSnapshot{}, PLACEMENT_SEED)'),
        'retry_bound': actual.replace('attempt != 0 ||', 'false ||'),
        'failure_reason_gate': actual.replace('!stack_exhausted ||', 'stack_exhausted ||'),
        'unsafe_context': actual.replace('mod::sched::preempt_count() != 0 || ', ''),
        'irq_off_rejected': actual.replace('mod::sched::preempt_count() != 0 || ', 'mod::sched::preempt_count() != 0 || !mod::sched::interrupts_enabled() || '),
        'irq_not_enabled': actual.replace('    enable_interrupts();', '    /* missing sti */'),
        'irq_not_restored': actual.replace('        disable_interrupts();', '        /* missing cli */'),
        'reclaim_under_publication': actual.replace('auto* t = mod::sched::task::Task::create_user_thread', '(void)mod::sched::reclaim_memory_pressure();\n                    auto* t = mod::sched::task::Task::create_user_thread'),
    }
    with tempfile.TemporaryDirectory(prefix='wos-thread-stack-pressure-') as tmp:
        d = Path(tmp)
        for name, content in [('actual', actual), *mutations.items()]:
            assert name == 'actual' or content != actual, name
            cpp, binary = d / (name + '.cpp'), d / name
            cpp.write_text(content)
            subprocess.run([compiler, '-std=c++23', '-O1', '-g', '-Wall', '-Wextra', '-Werror', '-fno-pie', '-no-pie', '-fsanitize=address,undefined', '-I', str(ROOT / 'modules/kern/src'), str(cpp), '-o', str(binary)], check=True)
            r = subprocess.run([str(binary)], capture_output=True, text=True)
            if name == 'actual':
                assert r.returncode == 0 and not r.stderr, r
                print(r.stdout, end='')
            else:
                assert r.returncode == -signal.SIGABRT and 'Assertion' in r.stderr, (name, r)
                print('PASS semantic mutant rejected: ' + name)


if __name__ == '__main__':
    main()
