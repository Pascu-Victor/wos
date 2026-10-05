#!/usr/bin/env python3
"""Exercise actual final-return and exec/fatal gates with controlled backends."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]

PRELUDE = r'''
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
bool locked=false, fpu_authorized=false, valid_frame=true, return_policy=true;
bool killed=false, reenter=false, job_resumed=true;
unsigned services=0, commits=0, fpu_entries=0, signal_exits=0, reads=0, drains=0;
namespace ker::mod::mm { constexpr uint64_t KERNEL_STACK_SIZE=65536; }
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON, IDLE };
enum class TaskState { ACTIVE, DEAD };
struct Task {
 TaskType type=TaskType::PROCESS;
 std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<bool> exit_in_progress{false}, unpublished_teardown_in_progress{false}, exec_preparation_teardown_in_progress{false};
 std::atomic<bool> gc_queued{false};std::atomic<void*> pending_exec_handoff{nullptr};
 bool has_exited=false,in_signal_handler=false;
 uint32_t preempt_disable_depth=0;
 struct { uint64_t syscall_kernel_stack=0;int saved_frame_class=0;int frame=0; } context;
 std::atomic<uint64_t> preemption_diagnostic{0};
 uint64_t signals=0;
 uint64_t signal_deliverable_bits(){return signals;}
 struct Handler { uint64_t handler=0; };Handler signal_handler_snapshot(unsigned){return {};}
 bool consume_signal_for_delivery(uint64_t mask){signals&=~mask;return true;}
};
struct Policy { bool deliver_signals; };
Policy saved_frame_restore_policy(int){return {return_policy};}
}
using Task=ker::mod::sched::task::Task;
Task* active=nullptr;
struct RunQueue { Task* current_task=nullptr;Task* handoff_task=nullptr; } rq;
struct Queues {
 RunQueue* this_cpu(){return &rq;}
 template<class F> void this_cpu_locked_void(F f){assert(!locked);locked=true;f(&rq);locked=false;}
} queues;
namespace ker::mod::cpu { unsigned current_cpu(){return 0;} }
namespace ker::mod::time { uint64_t get_us(){++reads;return 123;} }
namespace ker::mod::sys::context_switch {
void validate_handoff_stack_ownership(Task*,Task*,const char*){assert(!locked);}
bool saved_frame_class_is_valid(Task*,int){return valid_frame;}
struct UserReturnFpuAuthorization {
 explicit UserReturnFpuAuthorization(Task&){assert(!fpu_authorized);fpu_authorized=true;++fpu_entries;}
 ~UserReturnFpuAuthorization(){fpu_authorized=false;}
};
}
namespace ker::mod::sched {
Queues* run_queues=&queues;
Task* get_current_task(){return active;}
bool handoff_commit_allowed(unsigned depth,bool change){return !change||!depth;}
uint16_t scheduler_diagnostic_cpu(unsigned c){return c;}
enum class KernelPreemptionBlockReason { PREEMPT_DISABLED };
struct Diagnostic { int frame_class;KernelPreemptionBlockReason reason;uint16_t source_cpu,target_cpu; };
uint64_t encode_preemption_diagnostic(Diagnostic){return 1;}
[[noreturn]] void hcf(){assert(false);__builtin_unreachable();}
Task* debug_slot=nullptr;Task*& debug_task_slot(unsigned){return debug_slot;}
Task* transition_commit_handoff_locked(RunQueue* q,Task* incoming){assert(locked);++commits;auto* old=q->current_task;q->current_task=incoming;q->handoff_task=nullptr;active=incoming;return old;}
void requeue_woken_outgoing_task_locked(RunQueue*,Task*,uint64_t){assert(locked);}
bool is_dead_gc_candidate_task(Task*){return false;}
void insert_into_dead_list(Task*){assert(!locked);}
namespace job_control {void acknowledge_stopped(Task&){assert(!locked);}void drain(){assert(!locked);++drains;}}
namespace resource {void wait_for_exec_quiescence(Task&){assert(!locked && fpu_authorized);}}
void commit_handoff_task_at_return_boundary();
}
namespace ker::syscall::process {
void exit_current_if_process_exit_requested(){assert(!locked && fpu_authorized);}
void wos_proc_exit_signal(int){++signal_exits;}
void service_pending_exec_handoff(){
 assert(!locked && fpu_authorized);++services;
 // Model existing teardown guard protecting a reentrant final-return entry.
 if(reenter){active->exec_preparation_teardown_in_progress=true;ker::mod::sched::commit_handoff_task_at_return_boundary();active->exec_preparation_teardown_in_progress=false;}
 active->pending_exec_handoff=nullptr;
}
}
namespace ker::mod::sys::signal {
using namespace ker::mod;
constexpr uint64_t WOS_SIG_DFL=0;
bool resume_job_control_for_pending_signal(Task*){return job_resumed;}
bool handle_pending_sigkill_at_user_return(Task*){return killed;}
bool default_signal_is_ignored(int){return false;}
bool is_job_control_stop_signal(int){return false;}
void exit_current_on_cpu_limit_user_handoff(){assert(!locked);}
'''

CASES = r'''
void reset(Task& t){
 active=&t;rq.current_task=&t;rq.handoff_task=nullptr;ker::mod::sched::run_queues=&queues;
 locked=false;fpu_authorized=false;valid_frame=true;return_policy=true;killed=false;reenter=false;job_resumed=true;
 services=commits=fpu_entries=signal_exits=reads=drains=0;
 t.type=ker::mod::sched::task::TaskType::PROCESS;t.state=ker::mod::sched::task::TaskState::ACTIVE;
 t.exit_in_progress=false;t.unpublished_teardown_in_progress=false;t.exec_preparation_teardown_in_progress=false;
 t.has_exited=t.in_signal_handler=false;t.pending_exec_handoff=reinterpret_cast<void*>(1);t.signals=0;t.preempt_disable_depth=0;
 uint64_t stack=0;asm volatile("mov %%rsp,%0":"=r"(stack));t.context.syscall_kernel_stack=stack+8192;
}
int main(){
 using ker::mod::sched::commit_handoff_task_at_return_boundary;
 Task owner,incoming;
 reset(owner);commit_handoff_task_at_return_boundary();
 assert(services==1 && commits==0 && fpu_entries==1 && !fpu_authorized && !locked);
 // A handler cannot indefinitely hide a replacement request.
 reset(owner);owner.in_signal_handler=true;commit_handoff_task_at_return_boundary();assert(services==1);
 // Ordinary same-task returns do not enter the blocking helper.
 reset(owner);owner.pending_exec_handoff=nullptr;commit_handoff_task_at_return_boundary();assert(!services && !fpu_entries);
 reset(owner);active=nullptr;commit_handoff_task_at_return_boundary();assert(!services);
 reset(owner);ker::mod::sched::run_queues=nullptr;commit_handoff_task_at_return_boundary();assert(!services && !reads);
 // Actual helper gates reject unsafe or suspended contexts before authorization/service.
 for(unsigned scenario=0;scenario<10;++scenario){
  reset(owner);
  switch(scenario){
   case 0:owner.type=ker::mod::sched::task::TaskType::DAEMON;break;
   case 1:owner.type=ker::mod::sched::task::TaskType::IDLE;break;
   case 2:owner.exit_in_progress=true;break;
   case 3:owner.has_exited=true;break;
   case 4:owner.unpublished_teardown_in_progress=true;break;
   case 5:owner.exec_preparation_teardown_in_progress=true;break;
   case 6:owner.state=ker::mod::sched::task::TaskState::DEAD;break;
   case 7:owner.context.syscall_kernel_stack=0;break;
   case 8:return_policy=false;break;
   case 9:valid_frame=false;break;
  }
  commit_handoff_task_at_return_boundary();assert(!services && !fpu_entries && owner.pending_exec_handoff.load());
 }
 reset(owner);killed=true;commit_handoff_task_at_return_boundary();assert(!services && fpu_entries==1);
 reset(owner);job_resumed=false;commit_handoff_task_at_return_boundary();assert(!services && fpu_entries==1);
 reset(owner);reenter=true;commit_handoff_task_at_return_boundary();assert(services==1 && !owner.pending_exec_handoff.load());
 // Preserve actual incoming transition and existing guarded completion.
 reset(incoming);reset(owner);rq.handoff_task=&incoming;commit_handoff_task_at_return_boundary();
 assert(commits==1 && active==&incoming && services==1 && !locked && drains==1);
 // A saved user frame cannot admit work while another task owns the live stack.
 reset(owner);owner.context.syscall_kernel_stack=0;rq.handoff_task=&incoming;incoming.context.syscall_kernel_stack=0;
 commit_handoff_task_at_return_boundary();assert(commits==1 && !services);
 std::puts("PASS actual same-task/incoming exec return and stack/frame/fatal gates");
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--old-scheduler', type=Path)
    args = parser.parse_args()
    scheduler = (args.old_scheduler or ROOT / 'modules/kern/src/platform/sched/scheduler.cpp').read_text()
    signal = (ROOT / 'modules/kern/src/platform/sys/signal.cpp').read_text()
    code = PRELUDE + function(signal, 'task_owns_current_kernel_stack') + function(signal, 'exit_current_on_pending_fatal_default_signal')
    code += '\n}\nnamespace ker::mod::sched {\n' + function(scheduler, 'commit_handoff_task_at_return_boundary') + '\n}\n' + CASES
    with tempfile.TemporaryDirectory(prefix='wos-exec-owner-return-') as tmp:
        source, binary = Path(tmp) / 'return.cpp', Path(tmp) / 'return'
        source.write_text(code)
        compiler = shutil.which('c++', path=os.defpath)
        assert compiler
        subprocess.run([compiler, '-std=c++23', '-Wall', '-Wextra', '-Werror', '-O1', '-g',
                        '-fsanitize=address,undefined', '-fno-omit-frame-pointer', str(source), '-o', str(binary)], check=True, timeout=60)
        subprocess.run([str(binary)], check=True, timeout=15, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0'})


if __name__ == '__main__':
    main()
