#!/usr/bin/env python3
"""Exercise actual exec cooperation/IF scope with controlled hardware endpoints.

This host test checks eligibility, liveness and restoration, not scheduling or
kernel interrupt delivery. Only the privileged IRQ instructions are replaced.
"""
import os
from pathlib import Path
import subprocess
import tempfile

from exec_trace_ownership_test import block

ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / "modules/kern/src"
source = Path(os.environ.get("WOS_EXEC_LAYOUT_TEST_SOURCE", KERN / "syscalls_impl/process/exec.cpp")).read_text()
header = (KERN / "platform/loader/elf_loader.hpp").read_text()
scope_header = (KERN / "platform/sys/syscall_preparation.hpp").read_text()
context = block(source, r"^class ExecLayoutContext \{") + ";"
token = block(header, r"^class ElfLayoutCooperation \{") + ";"
scope = block(scope_header, r"^class SyscallPreparationScope \{") + ";"


def host_irq(text):
    text = text.replace('asm volatile("pushfq; pop %0; cli" : "=r"(flags)::"memory");',
                        'flags = host::save_disable();')
    text = text.replace('asm volatile("sti" ::: "memory");', 'host::enable();')
    text = text.replace('asm volatile("cli" ::: "memory");', 'host::disable();')
    assert "asm volatile" not in text, "new privileged endpoint needs an explicit host adapter"
    return text


code = r'''
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <sys/resource.h>
#include <type_traits>
#include <vector>
namespace host {
bool irq=false; unsigned enables=0,yields=0,queries=0; unsigned reject_query=0;
std::function<void()> on_enable,on_yield,irq_observer;
auto save_disable()->uint64_t {auto f=irq?512ULL:0ULL;irq=false;return f;}
void enable(){irq=true;++enables;if(irq_observer)irq_observer();if(on_enable)on_enable();}
void disable(){irq=false;}
}
namespace ker::syscall::process{class ExecRequestArguments{};class PreparedNativeExecImage{};}
namespace ker::mod::cpu{constexpr uint64_t GATE_IF_MASK=512;}
namespace ker::mod::mm::virt{
struct PageTable{};PageTable kernel;std::function<void()>on_kernel_query;
auto get_kernel_pagemap()->PageTable*{if(on_kernel_query)on_kernel_query();return &kernel;}
}
namespace ker::mod::sched::task{
enum class TaskType{PROCESS,DAEMON};enum class TaskState{ACTIVE,EXITING};
struct Task{
 TaskType type=TaskType::PROCESS;std::atomic<TaskState>state{TaskState::ACTIVE};
 uint64_t syscall_account_start_us=50;bool deferred_task_switch=false,wants_block=false,has_exited=false;
 std::atomic<bool>cpu_accounting_finalized{false},scheduler_transition_active{false},exit_in_progress{false},
 process_exit_requested{false},exec_preparation_teardown_in_progress{false},usercopy_pagemap_closing{false};
 std::atomic<ker::syscall::process::ExecRequestArguments*>owned_exec_arguments{nullptr};
 std::atomic<ker::syscall::process::PreparedNativeExecImage*>owned_exec_preparation{nullptr};
 ker::mod::mm::virt::PageTable*pagemap=nullptr;
};
}
using Task=ker::mod::sched::task::Task;
Task*current=nullptr,*returning=nullptr;unsigned depth=0;bool queryable=true;
namespace ker::mod::sched{
auto can_query_current_task()->bool{++host::queries;return queryable&&host::queries!=host::reject_query;}
auto get_current_task()->Task*{return current;}
auto get_return_task()->Task*{assert(!host::irq);return returning;}
auto preempt_count()->unsigned{return depth;}
auto interrupts_enabled()->bool{return host::irq;}
void kern_yield(){assert(host::irq&&!depth&&current->exec_preparation_teardown_in_progress);++host::yields;
 // Actual scheduler accounting may renew the timestamp on resumption.
 current->syscall_account_start_us+=100;if(host::on_yield)host::on_yield();
}
}
namespace ker::loader::elf{TOKEN}
namespace ker::mod::sys{SCOPE}
namespace ker::syscall::process{CONTEXT}
using namespace ker::syscall::process;
using Root=ker::mod::mm::virt::PageTable;
static_assert(!std::is_copy_constructible_v<ker::loader::elf::ElfLayoutCooperation>);
static_assert(!std::is_move_constructible_v<ker::loader::elf::ElfLayoutCooperation>);
int main(){
 rlimit no_core{0,0};assert(setrlimit(RLIMIT_CORE,&no_core)==0);
 Root old_root,new_root,other_root;Task actor,other;ExecRequestArguments request,other_request;
 auto reset=[&]{
  actor.type=ker::mod::sched::task::TaskType::PROCESS;actor.state=ker::mod::sched::task::TaskState::ACTIVE;
  actor.syscall_account_start_us=50;actor.cpu_accounting_finalized=false;actor.scheduler_transition_active=false;
  actor.deferred_task_switch=actor.wants_block=actor.has_exited=false;actor.exit_in_progress=false;
  actor.process_exit_requested=false;actor.exec_preparation_teardown_in_progress=false;actor.usercopy_pagemap_closing=false;
  actor.owned_exec_arguments=&request;actor.owned_exec_preparation=nullptr;actor.pagemap=&old_root;
  current=returning=&actor;depth=0;queryable=true;host::irq=false;host::enables=host::yields=host::queries=host::reject_query=0;
  host::on_enable={};host::on_yield={};
  ker::mod::mm::virt::on_kernel_query={};
  host::irq_observer=[&]{assert(actor.exec_preparation_teardown_in_progress);};
 };
 {
  reset();ExecLayoutContext context(actor,&request,&new_root);
  assert(context.cooperation()->try_cooperate(&new_root));assert(!host::irq&&host::yields==1&&!actor.exec_preparation_teardown_in_progress);
  assert(actor.syscall_account_start_us==150); // Exact timestamp identity would reject this.
  assert(context.cooperation()->try_cooperate(&new_root));assert(!host::irq&&host::yields==2&&!actor.exec_preparation_teardown_in_progress);
  assert(!context.cooperation()->try_cooperate(&new_root));assert(!host::irq&&host::yields==2&&!actor.exec_preparation_teardown_in_progress);
 }
 // Independent context/lifetime mutations must refuse BEFORE Scope can STI,
 // and must be detected again after an otherwise successful ordinary yield.
 std::vector<std::function<void()>> invalid={
  [&]{queryable=false;},[&]{current=&other;},[&]{returning=&other;},
  [&]{actor.type=ker::mod::sched::task::TaskType::DAEMON;},
  [&]{actor.state=ker::mod::sched::task::TaskState::EXITING;},
  [&]{actor.syscall_account_start_us=0;},[&]{actor.cpu_accounting_finalized=true;},
  [&]{depth=1;},[&]{actor.scheduler_transition_active=true;},[&]{actor.deferred_task_switch=true;},
  [&]{actor.wants_block=true;},[&]{actor.has_exited=true;},[&]{actor.exit_in_progress=true;},
  [&]{actor.process_exit_requested=true;},[&]{actor.usercopy_pagemap_closing=true;},
  [&]{actor.owned_exec_arguments=&other_request;},[&]{actor.owned_exec_arguments=nullptr;},
  [&]{actor.owned_exec_preparation=reinterpret_cast<PreparedNativeExecImage*>(1);},
  [&]{actor.pagemap=&other_root;},[&]{actor.pagemap=nullptr;},
 };
 for(auto&mutate:invalid){
  reset();ExecLayoutContext before(actor,&request,&new_root);mutate();
  assert(!before.cooperation()->try_cooperate(&new_root));assert(!host::irq&&!host::yields&&!host::enables&&!actor.exec_preparation_teardown_in_progress);
  reset();ExecLayoutContext after(actor,&request,&new_root);host::on_yield=mutate;
  assert(!after.cooperation()->try_cooperate(&new_root));assert(!host::irq&&host::yields==1&&!actor.exec_preparation_teardown_in_progress);
 }
 for(auto*root:{static_cast<Root*>(nullptr),&old_root,&ker::mod::mm::virt::kernel,&other_root}){
  reset();ExecLayoutContext mismatch(actor,&request,&new_root);
  assert(!mismatch.cooperation()->try_cooperate(root));assert(!host::irq&&!host::enables&&!host::yields);
 }
 for(auto*root:{static_cast<Root*>(nullptr),&old_root,&ker::mod::mm::virt::kernel}){
  reset();ExecLayoutContext invalid_root(actor,&request,root);
  assert(!invalid_root.cooperation()->try_cooperate(root));assert(!host::irq&&!host::enables&&!host::yields);
 }
 reset();ExecLayoutContext no_request(actor,nullptr,&new_root);
 assert(!no_request.cooperation()->try_cooperate(&new_root));assert(!host::irq&&!host::enables&&!host::yields);
 reset();ExecLayoutContext outer_deferral(actor,&request,&new_root);actor.exec_preparation_teardown_in_progress=true;
 assert(!outer_deferral.cooperation()->try_cooperate(&new_root));assert(!host::irq&&!host::enables&&!host::yields&&actor.exec_preparation_teardown_in_progress);
 reset();ExecLayoutContext raced_deferral(actor,&request,&new_root);
 ker::mod::mm::virt::on_kernel_query=[&]{actor.exec_preparation_teardown_in_progress=true;};
 assert(!raced_deferral.cooperation()->try_cooperate(&new_root));assert(!host::irq&&!host::enables&&!host::yields&&actor.exec_preparation_teardown_in_progress);
 reset();ExecLayoutContext wrong_if(actor,&request,&new_root);host::irq=true;
 assert(!wrong_if.cooperation()->try_cooperate(&new_root));assert(host::irq&&!host::enables&&!host::yields&&!actor.exec_preparation_teardown_in_progress);
 // Scope rejects a changed context, and liveness is rechecked after its STI.
 reset();ExecLayoutContext rejected_scope(actor,&request,&new_root);host::reject_query=2;
 assert(!rejected_scope.cooperation()->try_cooperate(&new_root));assert(!host::irq&&!host::enables&&!host::yields&&!actor.exec_preparation_teardown_in_progress);
 reset();ExecLayoutContext changed_on_enable(actor,&request,&new_root);
 host::on_enable=[&]{actor.process_exit_requested=true;};
 assert(!changed_on_enable.cooperation()->try_cooperate(&new_root));assert(!host::irq&&host::enables>0&&!host::yields&&!actor.exec_preparation_teardown_in_progress);
 std::puts("Actual exec context/IF scope: identity and liveness before/after yield, accounting renewal, private root, shared hard budget and IF restoration PASS");
}
'''.replace("TOKEN", token).replace("SCOPE", host_irq(scope)).replace("CONTEXT", host_irq(context))

with tempfile.TemporaryDirectory(prefix="wos-exec-layout-") as directory:
    source_path = Path(directory) / "test.cpp"
    binary_path = Path(directory) / "test"
    source_path.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                    str(source_path), "-o", str(binary_path)], check=True)
    environment = os.environ.copy()
    environment["ASAN_OPTIONS"] = "detect_leaks=0"
    subprocess.run([str(binary_path)], env=environment, check=True)
