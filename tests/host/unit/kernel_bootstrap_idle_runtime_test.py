#!/usr/bin/env python3
"""Connect actual scheduler startup/idle handoff to actual strict frame restore.

Whole actual startup, idle entry, publication and restore functions execute.
The exact complete process_tasks idle branch executes inside a finite wrapper.
IRQ instructions, runqueue locking/eligibility/stealing, platform timer/GS/CR3,
address bounds and assembly handoff are finite endpoints. This does not qualify
hardware IRQ delivery, SMP scheduling/stealing, ASAN stack entry or reset cause.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import tempfile
import time
from file_mmap_registry_runtime_test import command, pin, subreaper, capture, dependency_pins

ROOT = next(p for p in Path(__file__).resolve().parents
            if (p / 'modules/kern/src').is_dir() and (p / 'tests/host').is_dir())
KERN = ROOT / 'modules/kern/src'
SOURCE = KERN / 'platform/sched/scheduler.cpp'
CONTEXT = KERN / 'platform/sys/context_switch.cpp'
ASM = KERN / 'platform/sys/context_switch.asm'
HEADERS = [KERN / 'platform/sched/frame_class.hpp', KERN / 'platform/sched/preemption_policy.hpp']

# Complete retained predecessor, with no decision or instruction removed.
ORIGINAL_START = r'''void start_scheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::current_cpu());

    auto* rq = run_queues->this_cpu();
    mm::virt::note_tlb_shootdown_cpu_online();
    sys::context_switch::initialize_clockevent_for_this_cpu();

    // Wait for a real (non-idle) task in the heap.
    // CRITICAL: Use lock for the initial check to prevent double-scheduling.
    // A lockless peek creates a race window where rescheduleTaskForCpu can
    // move the task to another CPU between the peek and the currentTask assignment.
    task::Task* first_task = run_queues->this_cpu_locked([](RunQueue* rq) -> task::Task* {
        if (rq->runnable_heap.size == 0) {
            return nullptr;
        }
        int64_t const AVG = compute_avg_vruntime(rq);
        auto* t = pick_best_eligible_for_switch_locked(rq, AVG, nullptr);
        if (t != nullptr && t->type != task::TaskType::IDLE && (t->type == task::TaskType::DAEMON || t->thread != nullptr)) {
            t->store_cpu(cpu::current_cpu());
            transition_publish_current_locked(rq, t);
        }
        return t;
    });

    if (first_task == nullptr || first_task->type == task::TaskType::IDLE) {
        // Set idle task as current while waiting
        publish_current_task_boot_exception(rq, rq->idle_task);
        scheduler_task_context_ready_mask.fetch_or(1ULL << cpu::current_cpu(), std::memory_order_acq_rel);

        for (;;) {
            rq->is_idle.store(true, std::memory_order_release);
            arm_idle_timer_for_this_cpu();
            asm volatile("sti");
            asm volatile("hlt");
            asm volatile("cli");
            rq->is_idle.store(false, std::memory_order_release);

            // Check under lock if we have a non-idle task
            first_task = run_queues->this_cpu_locked([](RunQueue* rq) -> task::Task* {
                if (rq->runnable_heap.size == 0) {
                    return nullptr;
                }

                // Scan heap for a non-idle, ready task
                int64_t const AVG = compute_avg_vruntime(rq);
                task::Task* candidate = pick_best_eligible_for_switch_locked(rq, AVG, nullptr);
                if (candidate == nullptr) {
                    return nullptr;
                }
                if (candidate->type == task::TaskType::IDLE) {
                    return nullptr;
                }
                if (candidate->type == task::TaskType::PROCESS && candidate->thread == nullptr) {
                    return nullptr;
                }

                candidate->store_cpu(cpu::current_cpu());
                transition_publish_current_locked(rq, candidate);
                return candidate;
            });

            if (first_task != nullptr) {
                dbg::log("CPU %d: Found task PID %x, starting", cpu::current_cpu(), first_task->pid);
                break;
            }
        }
    } else {
        scheduler_task_context_ready_mask.fetch_or(1ULL << cpu::current_cpu(), std::memory_order_acq_rel);
    }

    bool const ALREADY_RAN = first_task->has_run;
    prepare_first_run_daemon(first_task, "start-first");
    record_local_proc_first_run(first_task, WOS_PERF_CALLSITE());
    first_task->has_run = true;
    first_task->store_cpu(cpu::current_cpu());
    publish_current_task_boot_exception(rq, first_task);

    // Set up GS/FS MSRs for the first task
    uint64_t const REAL_CPU_ID = cpu::current_cpu();
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(first_task->context.syscall_scratch_area);
    scratch_area->cpu_id = REAL_CPU_ID;
    if (first_task->thread != nullptr) {
#ifdef SCHED_DEBUG
        dbg::log("Setting MSRs: fsbase=0x%x, gsbase=0x%x, scratchArea=0x%x", first_task->thread->fsbase, first_task->thread->gsbase,
                 first_task->context.syscall_scratch_area);
#endif
        cpu_set_msr(IA32_GS_BASE, first_task->context.syscall_scratch_area);
        cpu_set_msr(IA32_KERNEL_GS_BASE, first_task->thread->gsbase);
        cpu_set_msr(IA32_FS_BASE, first_task->thread->fsbase);
    } else {
        // DAEMON or thread-less task: use scratch area for both GS bases
        cpu_set_msr(IA32_GS_BASE, first_task->context.syscall_scratch_area);
        cpu_set_msr(IA32_KERNEL_GS_BASE, first_task->context.syscall_scratch_area);
    }

    desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(first_task->context.syscall_kernel_stack), REAL_CPU_ID);
    mm::virt::switch_pagemap(first_task);

    // Update debug task pointer
    debug_task_slot(cpu::current_cpu()) = first_task;

    // Initialize HPET timestamp for first tick
    rq->last_tick_us = time::get_us();

    // Keep interrupts masked until the final handoff has installed the target
    // task stack. Otherwise an APIC timer can save `first_task` while we are
    // still executing start_scheduler() on the CPU bootstrap stack.
    asm volatile("cli" ::: "memory");

    // Start the scheduler timer. The eventual enter path restores IF from the
    // target frame or enables it after switching to the kernel-thread stack.
    sys::context_switch::start_sched_timer();

    // Process FPU/SIMD state is restored by the final userspace-entry path.
    // Keep it saved while scheduler startup is still running on kernel stacks.
    sys::context_switch::restore_debug_registers_for_task(first_task);

    if (ALREADY_RAN) {
// Task was already running on another CPU and was migrated here.
// Resume from its saved context instead of restarting at the entry point.
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Resuming PID %x from saved context (rip=0x%x)", cpu::current_cpu(), first_task->pid,
                 first_task->context.frame.rip);
#endif
        validate_kernel_resume_target(first_task, "start-resume");
        wos_deferred_task_switch_return(&first_task->context.regs, &first_task->context.frame);
        __builtin_unreachable();
    }

    // Brand-new DAEMON kernel thread: there is no interrupted kernel frame to
    // resume yet, so install the initial stack directly and enter the thread
    // trampoline. Preempted DAEMONs still use the generic resume path above.
    if (first_task->type == task::TaskType::DAEMON) {
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Starting kernel thread PID %x '%s' (rip=0x%x)", cpu::current_cpu(), first_task->pid,
                 first_task->name ? first_task->name : "?", first_task->context.frame.rip);
#endif
        validate_kernel_resume_target(first_task, "start-daemon");
        if (first_task->kthread_entry == nullptr) {
            resume_log::error("kernel thread without entry: pid=%lu name=%s", first_task->pid,
                              first_task->name != nullptr ? first_task->name : "?");
            hcf();
        }
        wos_start_kernel_thread(first_task->context.frame.rsp, first_task->kthread_entry);
        __builtin_unreachable();
    }

    // Brand new user task - write TLS self-pointer and enter at the prepared initial RIP.
    if (first_task->thread != nullptr) {
        *(reinterpret_cast<uint64_t*>(first_task->thread->fsbase)) = first_task->thread->fsbase;
    }

    for (;;) {
        wos_asm_enter_usermode(first_task->context.frame.rip, first_task->context.frame.rsp);
    }
}'''
ORIGINAL_START_SHA256 = 'c282332e9ec7ec1019a8adb62b2afc14fd805c4b8e2eaff9c5bc7d80aff8e8bd'

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/preemption_policy.hpp>
namespace fixture {
unsigned cases{},assertions{},failures{},lock_depth{},arm_calls{},steal_calls{},pick_calls{},switch_calls{},commits{},first_runs{};
bool irq{},on_idle_stack{},kernel_root{},kernel_gs{},clock_ready{},switch_ok{true};
uint64_t live_rsp{},tss_rsp{},fsbase{},gsbase{},kernel_gsbase{};
constexpr uint64_t IDLE_TOP=0x140000,REAL_TOP=0x150000,BOOT_RSP=0x160000;
struct Halt{};
enum class End {IDLE,USER,DAEMON,RESUME};
struct Stopped {End kind;};
void check(bool value,const char*why){++assertions;if(!value&&failures++<24)std::fprintf(stderr,"FAIL %s\n",why);}
void cli(){irq=false;}
void sti(){check(lock_depth==0,"enable IRQ only outside runqueue lock");irq=true;}
uint64_t save_irq(){uint64_t f=irq?0x200:0;irq=false;return f;}
void hlt();
void timer(uint64_t rip);
}
[[noreturn]] void hcf(){throw fixture::Halt{};}
template<size_t N>struct Literal{char bytes[N];constexpr Literal(char const(&v)[N]){std::copy_n(v,N,bytes);}};
namespace ker::mod::dbg {
template<Literal>struct logger {
 template<class...T>static void error(char const*,T... ){}
 template<class...T>static void warn(char const*,T... ){}
};
template<class...T>void log(char const*,T... ){}
}
// Finite text-address symbols, never executed. They give the actual RIP
// predicates genuine ordered linker symbols without any privileged Host code.
asm(".pushsection .data\n.global __kernel_text_start\n__kernel_text_start:\n.space 16\n"
    ".global fixture_daemon_entry\nfixture_daemon_entry:\n.space 8\n"
    ".global wos_kernel_thread_trampoline\nwos_kernel_thread_trampoline:\n.space 8\n"
    ".global wos_kernel_idle_loop\nwos_kernel_idle_loop:\n.space 8\n"
    ".global wos_kernel_idle_loop_end\nwos_kernel_idle_loop_end:\n.space 16\n"
    ".global __kernel_text_end\n__kernel_text_end:\n.popsection\n");
extern "C" {void fixture_daemon_entry();void wos_kernel_thread_trampoline();void wos_kernel_idle_loop();
void wos_kernel_idle_loop_end();extern char __kernel_text_start[],__kernel_text_end[];}
namespace ker::mod {
namespace gates {
constexpr uint64_t IRQ0=32;
struct InterruptFrame{uint64_t int_num{},err_code{},rip{},cs{},flags{},rsp{},ss{};};
}
namespace desc::gdt {
constexpr uint64_t GDT_KERN_CS=8,GDT_KERN_DS=16,GDT_USER_CS=0x23,GDT_USER_DS=0x1b;
void set_rsp0(uint64_t* rsp,uint64_t){fixture::tss_rsp=reinterpret_cast<uint64_t>(rsp);}
}
namespace cpu {
struct GPRegs{uint64_t rdi{};};struct PerCpu{uint64_t cpu_id{};};
constexpr uint64_t GATE_IF_MASK=0x200;
uint64_t current_cpu(){return 0;}uint64_t get_current_cpu_id_safe(){return 0;}
void wrgsbase(uint64_t){fixture::kernel_gs=true;}
}
namespace mm {
constexpr uint64_t KERNEL_STACK_SIZE=4096;
bool is_kernel_stack_address(uint64_t v){return v>=0x100000&&v<=0x180000;}
namespace virt {
int root;
void* get_kernel_pagemap(){return &root;}
void note_tlb_shootdown_cpu_online(){}
void switch_to_kernel_pagemap(){fixture::check(!fixture::irq&&fixture::lock_depth==0,"kernel root before idle stack with IRQ masked");fixture::kernel_root=true;}
template<class T>void switch_pagemap(T*){fixture::kernel_root=false;}
}
}
namespace time {uint64_t get_us(){return 1000;}}
namespace apic {uint32_t get_apic_id(){return 0;}}
namespace smt {
cpu::PerCpu kernel_per_cpu;
uint64_t get_cpu_index_from_apic_id(uint32_t){return 0;}
cpu::PerCpu const* get_kernel_per_cpu(uint64_t){return &kernel_per_cpu;}
}
namespace sched::task {
enum class TaskType{PROCESS,DAEMON,IDLE};enum class TaskState{ACTIVE,DEAD};
enum class WaitChannelKind{PTRACE};
struct Thread{uint64_t fsbase{},gsbase{};};
struct Context{uint64_t syscall_kernel_stack{},syscall_scratch_area{};cpu::GPRegs regs{};
 gates::InterruptFrame frame{};SavedFrameClass saved_frame_class{SavedFrameClass::INVALID};};
struct Task {
 enum class Queue{NONE,WAITING};
 TaskType type{TaskType::IDLE};uint64_t pid{},cpu_no{};char const*name{"finite"};Thread*thread{};void*pagemap{};
 Context context{};std::atomic<TaskState>state{TaskState::ACTIVE};Queue sched_queue{Queue::NONE};
 bool has_run{},wants_block{},blocked{},ptrace{};void(*kthread_entry)(){};
 void store_cpu(uint64_t v){cpu_no=v;}uint64_t load_cpu()const{return cpu_no;}
 bool is_voluntary_blocked()const{return blocked;}bool ptrace_stop_blocks_execution()const{return ptrace;}
 void set_wait_channel(char const*,WaitChannelKind){}
};
}
namespace sched {
using resume_log=dbg::logger<"resume">;
struct Heap{unsigned size{};task::Task*queued{};bool contains(task::Task*t)const{return size&&queued==t;}};
struct RunQueue {
 task::Task*current_task{},*handoff_task{},*idle_task{};Heap runnable_heap{};
 std::atomic<bool>is_idle{false};std::atomic<uint32_t>cached_current_load_default{0},cached_current_load_process{0};
 int64_t total_weight{},min_vruntime{},total_weighted_vruntime{};uint64_t last_tick_us{},current_task_start_us{};
};
struct Queues {
 RunQueue rq;
 RunQueue*this_cpu(){return &rq;}
 struct Guard{bool prior;Guard():prior(fixture::irq){fixture::irq=false;++fixture::lock_depth;}
  ~Guard(){--fixture::lock_depth;fixture::irq=prior;}};
 template<class F>auto this_cpu_locked(F f){Guard g;return f(&rq);}
 template<class F>void this_cpu_locked_void(F f){Guard g;f(&rq);}
} queues;
Queues*run_queues=&queues;
std::atomic<uint64_t>scheduler_task_context_ready_mask{0};
enum class TransitionPhase{STABLE,HANDOFF_RESERVED};enum class SchedulerTimerTraceKind{HANDOFF};
void assert_task_migration_owner_is_current_cpu(task::Task const*t){if(t)fixture::check(t->load_cpu()==0,"finite same CPU ownership");}
void panic_if_task_reserved_on_other_cpu(task::Task*,char const*){}
void validate_transition_boundary_locked(RunQueue*,TransitionPhase,task::Task*){fixture::check(fixture::lock_depth==1,"transition under RQ lock");}
void record_scheduler_timer_trace(SchedulerTimerTraceKind,uint64_t){}
void record_local_proc_first_run(task::Task*,uint64_t){++fixture::first_runs;}
task::Task*&debug_task_slot(uint64_t){static task::Task*p{};return p;}
task::Task*pick_best_eligible_for_switch_locked(RunQueue*r,int64_t,task::Task*){
 // Finite eligibility endpoint, not an EEVDF/affinity implementation.
 ++fixture::pick_calls;
 if(!r->runnable_heap.size)return nullptr;
 auto*t=r->runnable_heap.queued;
 if(t)r->runnable_heap={}; // A nonempty but unselectable queue stays nonempty.
 return t;
}
bool is_valid_kernel_stack(uint64_t v){return mm::is_kernel_stack_address(v);}
task::Task*debug_find_task_by_kernel_stack(uint64_t){return nullptr;}
bool account_runtime_slice(RunQueue*,task::Task*,uint64_t,uint64_t,bool,bool){return true;}
void try_steal_from_peers(uint64_t,RunQueue*){
 // Explicit finite empty-queue/no-successor endpoint; peer lock/steal policy
 // is not modeled. The actual idle branch still decides whether to return.
 ++fixture::steal_calls;
}
bool transition_detach_runnable_locked(RunQueue*r,task::Task*){r->runnable_heap={};return true;}
void transition_make_waiting_locked(RunQueue*,task::Task*){}
void clear_handoff_task(task::Task*){queues.rq.handoff_task=nullptr;}
void arm_idle_timer_for_this_cpu();
void validate_kernel_resume_target(task::Task*,char const*);
[[noreturn]]void enter_idle_loop(RunQueue*);
[[noreturn]]void wos_enterIdleStack(uint64_t);
[[noreturn]]void wos_start_kernel_thread(uint64_t,void(*)());
[[noreturn]]void wos_deferred_task_switch_return(cpu::GPRegs*,gates::InterruptFrame*);
[[noreturn]]void wos_asm_enter_usermode(uint64_t,uint64_t);
}
namespace sys::context_switch {
constexpr bool K_ENABLE_SCHED_VALIDATE_CONTEXT=false;
constexpr uint64_t USER_ADDR_LIMIT=0x800000000000ULL,USER_RFLAGS_FIXED_ONE=2,USER_RFLAGS_REQUIRED_MASK=0x202;
void validate_handoff_stack_ownership(sched::task::Task const*,sched::task::Task const*,char const*);
auto classify_saved_frame(sched::task::Task const*,gates::InterruptFrame const&,sched::task::SavedFrameOrigin,bool)->sched::task::SavedFrameClass;
void record_saved_frame_class(sched::task::Task*,gates::InterruptFrame const&,sched::task::SavedFrameOrigin);
void validate_classified_frame_for_restore(sched::task::Task*,gates::InterruptFrame const&,sched::task::SavedFrameClass,char const*);
void validate_user_frame(gates::InterruptFrame const&,sched::task::Task*,char const*){
 // User PTE/lazy backing admission is deliberately outside these kernel-idle vectors.
 hcf();
}
void initialize_clockevent_for_this_cpu(){fixture::clock_ready=true;}
void start_sched_timer(){fixture::check(!fixture::irq,"initial real-task timer keeps IRQ masked");}
void restore_debug_registers_for_task(sched::task::Task*){}
bool switch_to(cpu::GPRegs&,gates::InterruptFrame&f,sched::task::Task*t){
 ++fixture::switch_calls;if(!fixture::switch_ok)return false;f=t->context.frame;return true;
}
}
}
constexpr uint64_t IA32_GS_BASE=1,IA32_KERNEL_GS_BASE=2,IA32_FS_BASE=3;
void cpu_set_msr(uint64_t which,uint64_t value){
 if(which==IA32_GS_BASE)fixture::gsbase=value;
 else if(which==IA32_KERNEL_GS_BASE)fixture::kernel_gsbase=value;
 else fixture::fsbase=value;
}
#define WOS_PERF_CALLSITE() uint64_t{1}
// ACTUAL_CONTEXT
// ACTUAL_SCHEDULER
'''

ENDPOINTS = r'''
namespace ker::mod::sched {
void validate_kernel_resume_target(task::Task*t,char const*path){
 sys::context_switch::validate_classified_frame_for_restore(t,t->context.frame,t->context.saved_frame_class,path);
}
void arm_idle_timer_for_this_cpu(){
 ++fixture::arm_calls;
 fixture::check(fixture::clock_ready&&fixture::lock_depth==0,"idle timer after initialized clock and unlocked RQ");
 if(!fixture::on_idle_stack){
  fixture::check(!fixture::irq,"bootstrap idle timer armed with IRQ masked");
  // A ready context may still have no current owner until actual stack commit.
  fixture::check(can_query_current_task(),"ready mask precedes allocated idle handoff");
 }
}
[[noreturn]]void wos_enterIdleStack(uint64_t top){
 fixture::check(!fixture::irq&&fixture::lock_depth==0,"allocated idle stack entered with IRQ masked and no RQ lock");
 fixture::check(fixture::kernel_root&&fixture::kernel_gs,"kernel root and GS established before idle handoff");
 auto*rq=run_queues->this_cpu();
 fixture::check(rq->current_task==nullptr&&rq->handoff_task==rq->idle_task,"bootstrap stack has no fabricated idle current owner");
 fixture::live_rsp=top;fixture::on_idle_stack=true; // Finite actual ASM MOV RSP endpoint.
 run_queues->this_cpu_locked_void([&](RunQueue*r){(void)transition_commit_handoff_locked(r,r->handoff_task);});
 ++fixture::commits;
 fixture::check(rq->current_task==rq->idle_task&&rq->handoff_task==nullptr,"idle current committed after allocated stack install");
 fixture::sti();
 fixture::timer(reinterpret_cast<uint64_t>(wos_kernel_idle_loop)+2);
 throw fixture::Stopped{fixture::End::IDLE};
}
[[noreturn]]void wos_start_kernel_thread(uint64_t stack,void(*entry)()){
 fixture::check(!fixture::irq&&stack==fixture::REAL_TOP&&entry==fixture_daemon_entry,"unchanged fresh daemon entry operands");
 throw fixture::Stopped{fixture::End::DAEMON};
}
[[noreturn]]void wos_deferred_task_switch_return(cpu::GPRegs*,gates::InterruptFrame*){
 fixture::check(!fixture::irq,"unchanged migrated task entry keeps IRQ masked");throw fixture::Stopped{fixture::End::RESUME};
}
[[noreturn]]void wos_asm_enter_usermode(uint64_t rip,uint64_t rsp){
 fixture::check(!fixture::irq&&rip==0x400000&&rsp==0x700000,"unchanged first user entry operands");
 auto*t=run_queues->this_cpu()->current_task;
 fixture::check(t->thread&&*reinterpret_cast<uint64_t*>(t->thread->fsbase)==t->thread->fsbase,"unchanged first user TLS self pointer");
 throw fixture::Stopped{fixture::End::USER};
}
}
namespace fixture {
void timer(uint64_t rip){
 namespace s=ker::mod::sched;namespace c=ker::mod::sys::context_switch;using namespace ker::mod;
 bool previous=irq;irq=false;
 gates::InterruptFrame frame{gates::IRQ0,0,rip,desc::gdt::GDT_KERN_CS,0x202,live_rsp,desc::gdt::GDT_KERN_DS};
 cpu::GPRegs gpr{};auto*live=s::get_current_task();
 auto interrupted=c::classify_saved_frame(live,frame,s::task::SavedFrameOrigin::INTERRUPT,false);
 s::process_idle_timer(gpr,frame);
 auto*return_task=s::get_return_task();
 auto durable=return_task?return_task->context.saved_frame_class:s::task::SavedFrameClass::INVALID;
 auto selected=s::task::select_timer_return_frame_class(return_task&&return_task==live,interrupted,durable);
 c::validate_classified_frame_for_restore(return_task,frame,selected,"finite-timer-return");
 irq=previous;
}
void hlt(){
 check(irq,"bootstrap HLT endpoint received enabled IRQ");
 timer(reinterpret_cast<uint64_t>(__kernel_text_start)+1);
 // If strict restore wrongly accepted this C++ RIP, the very same positive
 // return oracle would succeed. Do not manufacture a refusal after validation.
 throw Stopped{End::IDLE};
}
}
'''

CASES = r'''
namespace fixture {
using namespace ker::mod;namespace s=ker::mod::sched;namespace c=ker::mod::sys::context_switch;
s::task::Task idle,real,outgoing;s::task::Thread user_thread;cpu::PerCpu idle_scratch,real_scratch;uint64_t tcb;
void reset(){
 ++cases;std::destroy_at(&s::queues.rq);std::construct_at(&s::queues.rq);
 for(auto*t:{&idle,&real,&outgoing}){std::destroy_at(t);std::construct_at(t);}
 irq=false;on_idle_stack=kernel_root=kernel_gs=clock_ready=false;switch_ok=true;
 arm_calls=steal_calls=pick_calls=switch_calls=commits=first_runs=lock_depth=0;
 live_rsp=BOOT_RSP;tss_rsp=fsbase=gsbase=kernel_gsbase=0;s::scheduler_task_context_ready_mask=0;
 idle.context.syscall_kernel_stack=IDLE_TOP;idle.context.syscall_scratch_area=reinterpret_cast<uint64_t>(&idle_scratch);
 idle.pagemap=mm::virt::get_kernel_pagemap();s::queues.rq.idle_task=&idle;
 real.type=s::task::TaskType::DAEMON;real.pid=1;real.kthread_entry=fixture_daemon_entry;real.pagemap=mm::virt::get_kernel_pagemap();
 real.context.syscall_kernel_stack=REAL_TOP;real.context.syscall_scratch_area=reinterpret_cast<uint64_t>(&real_scratch);
 real.context.frame.rsp=REAL_TOP;tcb=0;user_thread={reinterpret_cast<uint64_t>(&tcb),0x600000};
}
bool boot(End expected){try{s::start_scheduler();}catch(Stopped const&e){return e.kind==expected;}catch(Halt const&){return false;}return false;}
bool rejected(gates::InterruptFrame f,s::task::Task*t=&idle){
 try{auto k=c::classify_saved_frame(t,f,s::task::SavedFrameOrigin::INTERRUPT,false);
  c::validate_classified_frame_for_restore(t,f,k,"negative-frame");return false;}catch(Halt const&){return true;}
}
gates::InterruptFrame idle_frame(){return {gates::IRQ0,0,reinterpret_cast<uint64_t>(wos_kernel_idle_loop)+2,8,0x202,IDLE_TOP,16};}
void run(){
 //1–3: empty/null first pick; an actual queued IDLE first pick; and a nonempty
 // queue with no eligible pick. The last case takes the actual pick-null path
 // without entering the empty-queue steal port on the subsequent timer.
 for(unsigned mode=0;mode<3;++mode){reset();
  if(mode==1)s::queues.rq.runnable_heap={1,&idle};
  if(mode==2)s::queues.rq.runnable_heap={1,nullptr};
  check(boot(End::IDLE),"bootstrap no-successor timer returns through actual idle frame");
  check(commits==1&&on_idle_stack&&s::queues.rq.current_task==&idle,"startup owns allocated idle stack before enabled interrupt");
  check(steal_calls==(mode==2?0U:1U)&&pick_calls==mode&&switch_calls==0&&s::queues.rq.cached_current_load_default==0,
        "distinct actual null-IDLE-unselectable paths preserve zero-load idle owner");
  check(s::queues.rq.runnable_heap.size==(mode==2?1U:0U),"unselectable nonempty queue is preserved through no-successor return");
 }
 //4: all byte positions in the finite linked idle range use the actual predicate.
 reset();auto begin=reinterpret_cast<uint64_t>(wos_kernel_idle_loop),end=reinterpret_cast<uint64_t>(wos_kernel_idle_loop_end);
 for(uint64_t ip=begin;ip<end;++ip){auto f=idle_frame();f.rip=ip;check(!rejected(f),"all assembly idle continuation RIPs stay accepted");}
 check(idle.context.saved_frame_class==s::task::SavedFrameClass::INVALID,"idle has no durable continuation class");
 //5: unchanged strict validator refuses bootstrap and range-end continuation.
 reset();auto f=idle_frame();f.rip=reinterpret_cast<uint64_t>(__kernel_text_start)+1;
 check(rejected(f),"strict restore rejects bootstrap C++ idle continuation");f.rip=end;
 check(rejected(f),"strict restore rejects half-open idle range end");
 //6: malformed idle frames and non-idle owner never borrow the idle exception.
 reset();f=idle_frame();f.rsp=0;check(rejected(f),"invalid idle frame stack refused");
 f=idle_frame();f.ss=0;check(rejected(f),"invalid idle SS refused");
 f=idle_frame();f.flags=0;check(rejected(f),"invalid idle flags refused");
 f=idle_frame();f.cs=3;check(rejected(f),"invalid idle CS refused");
 real.type=s::task::TaskType::PROCESS;check(rejected(idle_frame(),&real),"nonidle task does not borrow idle restore exception");
 //7–12: real idle-resource predicate refuses each missing/wrong resource.
 for(unsigned axis=0;axis<6;++axis){reset();
  if(axis==0)s::queues.rq.idle_task=nullptr;
  if(axis==1)idle.pid=2;if(axis==2)idle.thread=&user_thread;
  if(axis==3)idle.pagemap=nullptr;if(axis==4)idle.context.syscall_kernel_stack=0;
  if(axis==5)idle.context.syscall_scratch_area=0;
  check(!boot(End::IDLE),"malformed published idle resources refused");
  check(commits==0,"malformed idle never reaches stack commitment");
 }
 //13: IRQon callers are masked before readiness and idle handoff.
 reset();irq=true;check(boot(End::IDLE),"startup masks incoming enabled IF before idle handoff");
 //14: fresh initial daemon path remains the actual unchanged complete startup suffix.
 reset();s::queues.rq.runnable_heap={1,&real};
 check(boot(End::DAEMON),"initial fresh daemon path unchanged");
 check(real.has_run&&s::queues.rq.current_task==&real&&real.context.saved_frame_class==s::task::SavedFrameClass::DAEMON_KERNEL,
       "fresh daemon context prepared and published");
 check(tss_rsp==REAL_TOP&&commits==0&&arm_calls==0,"initial daemon preserves TSS path without idle handoff");
 //15: already-run daemon resumes instead of restarting.
 reset();s::prepare_first_run_daemon(&real,"finite-resume-setup");real.has_run=true;s::queues.rq.runnable_heap={1,&real};
 check(boot(End::RESUME),"initial migrated daemon resumes saved context");
 //16: initial user path retains TLS write and final operands.
 reset();real.type=s::task::TaskType::PROCESS;real.thread=&user_thread;real.context.frame.rip=0x400000;real.context.frame.rsp=0x700000;
 s::queues.rq.runnable_heap={1,&real};check(boot(End::USER),"initial user task path unchanged");
 check(tcb==user_thread.fsbase&&fsbase==user_thread.fsbase&&tss_rsp==REAL_TOP,"initial user TLS MSR and TSS preserved");
 //17: actual later idle timer selection prepares a new daemon and reserves it
 // without replacing the interrupted idle stack's owner before return boundary.
 reset();on_idle_stack=true;live_rsp=IDLE_TOP;clock_ready=true;s::queues.rq.current_task=&idle;s::queues.rq.runnable_heap={1,&real};
 timer(reinterpret_cast<uint64_t>(wos_kernel_idle_loop)+2);
 check(s::queues.rq.current_task==&idle&&s::queues.rq.handoff_task==&real&&real.has_run&&switch_calls==1,
       "later actual idle selection reserves prepared daemon without premature current publication");
 //18: failed finite architecture switch uses the existing idle reservation recovery.
 reset();on_idle_stack=true;live_rsp=IDLE_TOP;clock_ready=true;switch_ok=false;s::queues.rq.current_task=&idle;s::queues.rq.runnable_heap={1,&real};
 timer(reinterpret_cast<uint64_t>(wos_kernel_idle_loop)+2);
 check(s::queues.rq.current_task==&idle&&s::queues.rq.handoff_task==&idle&&!real.has_run&&arm_calls==1,
       "failed switch retains idle owner and reserves existing recovery");
 //19: existing exact stack-collision policy still refuses a distinct owner.
 reset();clock_ready=true;outgoing.type=s::task::TaskType::DAEMON;outgoing.pid=2;
 outgoing.context.syscall_kernel_stack=IDLE_TOP;s::queues.rq.current_task=&outgoing;
 bool collision_refused=false;try{s::enter_idle_loop(&s::queues.rq);}catch(Halt const&){collision_refused=true;}
 check(collision_refused&&commits==0,"distinct outgoing owner cannot share incoming idle stack");
 std::printf("%s bootstrap_idle cases=%u assertions=%u failures=%u\n",failures?"FAIL":"PASS",cases,assertions,failures);
}
}
int main(){try{fixture::run();}catch(...){std::fprintf(stderr,"FAIL unexpected finite boundary escape\n");return 3;}return fixture::failures?1:0;}
'''


def generate(scheduler: str, context: str, assembly: str, control: str) -> tuple[str, dict]:
    selected = {}

    def body(text: str, anchor: str) -> str:
        value, row = capture(text, anchor)
        selected[anchor] = row
        return value

    idle_asm = assembly[assembly.index('wos_enterIdleStack:\n'):assembly.index('; Exec owner entry', assembly.index('wos_enterIdleStack:\n'))]
    instructions = [line.split(';', 1)[0].strip() for line in idle_asm.splitlines()]
    instructions = [line for line in instructions if line and not line.endswith(':')]
    if instructions != ['mov rsp, rdi', 'call wos_commit_handoff_task', 'sti', 'jmp wos_kernel_idle_loop']:
        raise ValueError('finite idle handoff endpoint no longer matches complete actual ASM boundary')
    selected['complete-actual-idle-entry-asm'] = dict(bytes=len(idle_asm.encode()), sha256=hashlib.sha256(idle_asm.encode()).hexdigest())
    if hashlib.sha256(ORIGINAL_START.encode()).hexdigest() != ORIGINAL_START_SHA256:
        raise ValueError('complete retained original startup changed')
    start = body(scheduler, 'void start_scheduler() {')
    if control == 'original-bootstrap':
        start = ORIGINAL_START
    elif control != 'current':
        raise ValueError('unknown complete-body control')
    replacements = {'asm volatile("cli" ::: "memory");': 'fixture::cli();',
                    'asm volatile("cli");': 'fixture::cli();',
                    'asm volatile("sti");': 'fixture::sti();',
                    'asm volatile("hlt");': 'fixture::hlt();'}
    expected = {'current': [2, 0, 0, 0], 'original-bootstrap': [1, 1, 1, 1]}[control]
    for (old, new), count in zip(replacements.items(), expected):
        if start.count(old) != count:
            raise ValueError('exact startup IRQ/HLT endpoint count changed: ' + old)
        start = start.replace(old, new)
    scheduler_anchors = [
        'inline auto task_type_for_log(', 'inline auto task_state_for_log(', 'inline auto task_queue_for_log(',
        'inline auto task_name_for_log(', 'inline auto has_kernel_scheduler_context(', 'inline auto is_publishable_idle_task(',
        'inline int64_t compute_avg_vruntime(', 'inline uint32_t task_load_for_incoming(',
        'inline uint32_t current_task_load_for_incoming(', 'inline void update_current_load_cache(',
        'inline void transition_publish_current_locked(', 'inline void publish_current_task_boot_exception(',
        'inline void transition_reserve_handoff_locked(', 'inline void reserve_handoff_task_locked(',
        'inline auto transition_commit_handoff_locked(', 'inline void restore_kernel_gs_for_idle(',
        'inline void prepare_first_run_daemon(', 'auto get_return_task(', 'auto can_query_current_task()']
    actual_scheduler = [body(scheduler, anchor) for anchor in scheduler_anchors]
    getter = body(scheduler, 'auto get_current_task()')
    save = 'asm volatile("pushfq; pop %0; cli" : "=r"(flags)::"memory");'
    restore = 'asm volatile("sti" ::: "memory");'
    if getter.count(save) != 1 or getter.count(restore) != 1:
        raise ValueError('exact current-task IRQ endpoint changed')
    getter = getter.replace(save, 'flags = fixture::save_irq();').replace(restore, 'fixture::sti();')
    actual_scheduler.append(getter)
    actual_scheduler.append(body(scheduler, '[[noreturn]] void enter_idle_loop('))
    process = body(scheduler, 'void process_tasks(')
    idle_branch = body(process, 'if (current_task == nullptr || current_task->type == task::TaskType::IDLE) {')
    actual_scheduler.append('void process_idle_timer(cpu::GPRegs& gpr, gates::InterruptFrame& frame) {\n'
                            'auto* rq=run_queues->this_cpu();auto* current_task=rq->current_task;\n' + idle_branch + '\nhcf();\n}')
    actual_scheduler.append(start)
    context_anchors = ['inline auto valid_kernel_stack(', 'inline auto is_kernel_text_pointer(',
                       'inline auto stack_belongs_to_task(', 'inline auto valid_user_resume_scalar(',
                       'inline auto valid_user_rflags(', 'inline void validate_kernel_frame(',
                       'inline auto is_idle_return_frame(', 'void validate_handoff_stack_ownership(',
                       'auto classify_saved_frame(', 'void record_saved_frame_class(',
                       'auto classified_frame_is_valid(', 'void validate_classified_frame_for_restore(']
    actual_context = [body(context, anchor) for anchor in context_anchors]
    text = PRELUDE.replace('// ACTUAL_CONTEXT', '\nnamespace ker::mod::sys::context_switch {\n' + '\n'.join(actual_context) + '\n}')
    text = text.replace('// ACTUAL_SCHEDULER', '\nnamespace ker::mod::sched {\n' + '\n'.join(actual_scheduler) + '\n}')
    selected['ported-startup'] = dict(bytes=len(start.encode()), sha256=hashlib.sha256(start.encode()).hexdigest())
    return text + ENDPOINTS + CASES, selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scheduler-source', type=Path, default=SOURCE)
    parser.add_argument('--cxx', default=os.environ.get('CXX', os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++')))
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    scratch = None
    scratch_identity = None
    groups, watched, before, staged, staged_before = [], [], [], [], []
    result = dict(pass_=False, commands=[], scratch_removed=False, native_IRQ_SMP_qualified=False,
                  whole_budget_seconds=90, hardware_reset_cause_qualified=False)
    try:
        source = args.scheduler_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name one executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--cxx must name one executable compiler ELF')
        subreaper()
        watched = [source, CONTEXT, ASM, *HEADERS, Path(__file__).resolve(),
                   ROOT / 'tests/host/unit/file_mmap_registry_runtime_test.py', compiler]
        before = [pin(path) for path in watched]
        result['sources_before'] = before
        scratch = Path(tempfile.mkdtemp(prefix='wos-bootstrap-idle-', dir='/tmp'))
        info = scratch.stat()
        scratch_identity = (info.st_dev, info.st_ino, info.st_uid)
        if scratch.is_symlink() or info.st_uid != os.getuid():
            raise ValueError('fresh scratch ownership is not exact')
        for header in HEADERS:
            target = scratch / 'include/platform/sched' / header.name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(header.read_bytes())
        for control in ('current', 'original-bootstrap'):
            text, actual = generate(scheduler=source.read_text(), context=CONTEXT.read_text(), assembly=ASM.read_text(), control=control)
            result.setdefault('actual_definition_hashes', {})[control] = actual
            translation, binary, depfile = (scratch / (control + suffix) for suffix in ('.cpp', '', '.d'))
            translation.write_text(text)
            if staged_before and [pin(path) for path in staged] != staged_before:
                raise ValueError('earlier staged sources changed before next control')
            staged = sorted(path for path in scratch.rglob('*') if path.is_file() and path.suffix in {'.cpp', '.hpp'})
            staged_before = [pin(path) for path in staged]
            result.setdefault('staged_sources_before', {})[control] = staged_before
            row = command([str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-Wall', '-Wextra', '-Werror',
                           '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-MD', '-MF', str(depfile),
                           '-I', str(scratch / 'include'), str(translation), '-o', str(binary)],
                          scratch / (control + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if depfile.is_file():
                result.setdefault('emitted_dependencies_after_only', {})[control] = dependency_pins(depfile)
            if row['exit'] != 0:
                print((scratch / (control + '-compile.log')).read_text(errors='replace')[-32768:])
                raise ValueError(control + ': compile/link failure is not an accepted negative control')
            log_path = scratch / (control + '-run.log')
            row = command([str(binary)], log_path, scratch, deadline, groups)
            result['commands'].append(row)
            log = log_path.read_text(errors='replace')
            print(log[-32768:], end='')
            positive = row['exit'] == 0 and re.search(r'^PASS bootstrap_idle cases=19 assertions=\d+ failures=0$', log, re.M)
            if (control == 'current') != bool(positive):
                raise ValueError(control + ': complete actual-body oracle did not distinguish expected behavior')
            if control != 'current':
                marker = 'FAIL bootstrap no-successor timer returns through actual idle frame'
                if row['exit'] != 1 or not re.search(r'^FAIL bootstrap_idle cases=19 assertions=\d+ failures=[1-9]\d*$', log, re.M) or marker not in log.splitlines():
                    raise ValueError('strict-compiled complete original must finish all19cases and fail the same positive restore oracle')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if staged_before:
            try:
                result['staged_sources_after'] = [pin(path) for path in staged]
                result['staged_sources_held'] = result['staged_sources_after'] == staged_before
            except OSError:
                result['staged_sources_held'] = False
            if not result['staged_sources_held']:
                result['pass_'] = False
        if before:
            try:
                result['sources_after'] = [pin(path) for path in watched]
                result['sources_held'] = result['sources_after'] == before
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']:
                result['pass_'] = False
        if scratch is not None and not groups:
            info = scratch.stat()
            if scratch.is_symlink() or (info.st_dev, info.st_ino, info.st_uid) != scratch_identity:
                result['pass_'] = False
                result['cleanup_error'] = 'scratch exact ownership changed; preserve it'
            else:
                shutil.rmtree(scratch)
                result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups {groups}; scratch preserved'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole90-second invocation including cleanup exceeded'
        print(json.dumps(result), flush=True)
    return 0 if result['pass_'] and result['scratch_removed'] and time.monotonic() < deadline else 1


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
