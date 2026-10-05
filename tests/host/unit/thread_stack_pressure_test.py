#!/usr/bin/env python3
"""Execute THREAD_CREATE pressure handling with actual factory error classification.

GC and the remaining task construction are host seams; this checks bounded retries, ownership
lock ordering and the real IRQ-off syscall context, not live epoch reclamation or the
128-thread runtime gate. Hardware IF instructions and journal output are host seams.
"""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
int publication_depth=0, pool_depth=0, calls=0, reclaims=0, attached=0, posted=0, destroyed=0, exits=0, pending_checks=0;
int failures=0, reservation_error=0, pressure_logs=0, gc_requests=0;
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
  assert(publication_depth==0 && pool_depth==0 && irq==entry_irq); ++pressure_logs;
 }
};
}
namespace ker::mod::sched::task {
struct User { uint32_t real=0; }; struct Cred { User user() const { return {}; } };
enum class TaskType { PROCESS, DAEMON };
struct Task {
 TaskType type=TaskType::PROCESS;
 uint64_t syscall_account_start_us=1;
 std::atomic<bool> scheduler_transition_active{false};
 bool deferred_task_switch=false, wants_block=false, voluntary=false;
 bool is_voluntary_blocked() const { return voluntary; }
 uint64_t pid=0x1234567887654321ULL;
 Cred credentials() const { return {}; }
 static Task* create_user_thread(Task*,uint64_t tcb,uint64_t sp,uint64_t enter,uint64_t entry,uint64_t arg,bool* stack_exhausted) {
  assert(irq==entry_irq);
  assert(publication_depth==1 && pool_depth==0 && tcb==256 && sp==512 && enter==768 && entry==1024 && arg==1280);
  ++calls;
  PROLOGUE
  if (failures && !stack_pressure) { --failures; return nullptr; }
  static Task child; return &child;
 }
};
Task parent, other;
void destroy_unpublished_user_thread(Task* t) { assert(publication_depth==1 && t); ++destroyed; published_tid=0; }
}
namespace ker::mod::sched {
auto get_current_task() -> task::Task* { return wrong_current ? &task::other : &task::parent; }
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
 if(d) { d->epoch_skips=3;d->current_skips=2;d->refcount_skips=1; }
 return {.reclaimed=recover ? 1U : 0U,.scanned=7,.time_budget_exhausted=pass_time,.has_pending_work=pass_pending,.ran=true};
}
FOLLOWUP
uint32_t reclaim_memory_pressure(GcPressureDiagnostics* diagnostics=nullptr);
WRAPPER
bool post_task_for_cpu(uint64_t cpu,task::Task*) { assert(publication_depth==1 && cpu<cpus); ++posted; return post_ok; }
namespace resource {
struct ProcessReservation {
 ProcessReservation(task::Task*,uint32_t) {}
 int result() const { return reservation_error; }
 bool attach(task::Task&) { assert(publication_depth==1); ++attached; return true; }
};
}
}
namespace ker::mod::smt { uint64_t get_core_count() { return cpus; } }
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
void reset() {
 assert(publication_depth==0 && pool_depth==0);
 calls=reclaims=attached=posted=destroyed=exits=pending_checks=failures=reservation_error=pressure_logs=gc_requests=0;
 pass_ran=true;pass_time=pass_pending=false;ker::mod::sched::scheduler_gc_memory_pressure_requested=false;
 safe=stack_pressure=recover=publish_ok=post_ok=copy_ok=true;concurrently_freed=gc_changes_if=false;cpus=8;published_tid=0;
 irq=entry_irq=false;queues=query=true;panic=wrong_current=false;
 auto& p=ker::mod::sched::task::parent;p.type=ker::mod::sched::task::TaskType::PROCESS;
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
 reset(); cpus=0; assert(run()==-ENOMEM); assert(calls==1 && !reclaims && destroyed==1 && !attached && !posted);
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
  reset();failures=1;auto& p=ker::mod::sched::task::parent;
  switch(field) {
   case 0:queues=false;break;case 1:query=false;break;case 2:panic=true;break;
   case 3:p.type=ker::mod::sched::task::TaskType::DAEMON;break;
   case 4:p.syscall_account_start_us=0;break;case 5:p.scheduler_transition_active=true;break;
   case 6:p.deferred_task_switch=true;break;case 7:p.wants_block=true;break;case 8:p.voluntary=true;break;
  }
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
 puts("PASS actual THREAD_CREATE IRQ-off/IRQ-on pressure recovery, unsafe contexts and publication/ownership checks");
}
'''


def source():
    text = (ROOT / 'modules/kern/src/syscalls_impl/multiproc/threadControl.cpp').read_text()
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
    factory = (ROOT / 'modules/kern/src/platform/sched/task.cpp').read_text()
    a = factory.index('{', factory.index('Task* Task::create_user_thread(')) + 1
    b = factory.index('    uint64_t const K_RSP', a)
    header = (ROOT / 'modules/kern/src/platform/sched/scheduler.hpp').read_text()
    aa = header.index('struct GcPressureDiagnostics {')
    bb, depth = header.index('{', aa) + 1, 1
    while depth:
        depth += (header[bb] == '{') - (header[bb] == '}')
        bb += 1
    bb += 1
    scheduler = (ROOT / 'modules/kern/src/platform/sched/scheduler.cpp').read_text()
    def function(name):
        aa = scheduler.index('auto ' + name + '(')
        brace = scheduler.index('{', aa)
        bb, depth = brace + 1, 1
        while depth:
            depth += (scheduler[bb] == '{') - (scheduler[bb] == '}')
            bb += 1
        return scheduler[aa:bb]
    return (PRELUDE.replace('PROLOGUE', factory[a:b]).replace('BODY', body).replace('HELPER', helper)
            .replace('REPORT', header[aa:bb]).replace('WRAPPER', function('reclaim_memory_pressure'))
            .replace('FOLLOWUP', function('gc_pass_needs_followup')))


def main():
    compiler = os.environ.get('HOST_CXX') or shutil.which('g++', path=os.defpath) or shutil.which('clang++', path=os.defpath)
    assert compiler, 'host compiler missing'
    actual = source()
    mutations = {
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
            subprocess.run([compiler, '-std=c++23', '-O1', '-g', '-Wall', '-Wextra', '-Werror', '-fno-pie', '-no-pie', '-fsanitize=address,undefined', str(cpp), '-o', str(binary)], check=True)
            r = subprocess.run([str(binary)], capture_output=True, text=True)
            if name == 'actual':
                assert r.returncode == 0 and not r.stderr, r
                print(r.stdout, end='')
            else:
                assert r.returncode == -signal.SIGABRT and 'Assertion' in r.stderr, (name, r)
                print('PASS semantic mutant rejected: ' + name)


if __name__ == '__main__':
    main()
