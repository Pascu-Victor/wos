#!/usr/bin/env python3
"""Exercise actual native image arm/return code and perf session admission."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import block, function
from perf_capture_kernel_test import PRELUDE, PERF

ROOT = Path(__file__).resolve().parents[3]

FACES = r'''
#undef WOS_PERF_CALLSITE
#define WOS_PERF_CALLSITE() 123ULL
namespace ker::mod::cpu { unsigned current_cpu() { return 0; } }
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON, IDLE };
TRACE_TYPE
struct Task { uint64_t pid=0x300000042ULL; TaskType type=TaskType::PROCESS;
 bool has_run=true; ExecImageTrace exec_image_trace{}; };
}
using Task=ker::mod::sched::task::Task;
Task* current=nullptr; Task* returning=nullptr;
namespace ker::mod::sched {
Task* get_current_task() { return current; }
Task* get_return_task() { return returning; }
}
namespace ker::syscall::process {
STAGE_TYPE
ARM
}
namespace ker::mod::sys::context_switch {
CONSUMER
}
'''

CASES = r'''
namespace p=ker::mod::perf;
using namespace ker::syscall::process;
using ker::mod::sys::context_switch::wos_record_exec_user_return;
constexpr uint64_t RIP=0x200040ULL, SUBJECT=0x100000009ULL;
LocalProcStage attempt() {
 return {.correlation=42,.subject_pid=SUBJECT,
  .capture_session=p::current_wki_capture_session(p::WkiPerfScope::LOCAL_PROC,2),
  .op=p::WkiPerfLocalProcOp::EXECVE,.active=true};
}
void prepare(Task& t) {
 p::start_recording(p::PERF_MASK_LOCAL_PROC);current=returning=&t;
 arm_native_exec_user_return(t,attempt(),RIP);
}
int main() {
 p::init();Task t,other;
 static_assert(sizeof(p::PerfEvent)==48);
 // Recording session zero is valid before the first capture reset.
 p::enable();current=returning=&t;assert(attempt().capture_session==0);
 arm_native_exec_user_return(t,attempt(),RIP);wos_record_exec_user_return(RIP,1);
 assert(p::get_capture_status().written==2);p::disable();
 // All final paths use the same full-width subject/owner binding and consume once.
 for(unsigned kind=1;kind<=4;++kind) {
  prepare(t); auto reads=clocks.load();
  wos_record_exec_user_return(RIP+1,kind);assert(t.exec_image_trace.pending && clocks==reads);
  wos_record_exec_user_return(0xffff800000010000ULL,kind);assert(t.exec_image_trace.pending && clocks==reads);
  returning=nullptr;wos_record_exec_user_return(RIP,kind);assert(t.exec_image_trace.pending);
  returning=&other;wos_record_exec_user_return(RIP,kind);assert(t.exec_image_trace.pending);
  current=nullptr;returning=&t;wos_record_exec_user_return(RIP,kind);assert(t.exec_image_trace.pending);
  current=&t;t.type=ker::mod::sched::task::TaskType::DAEMON;wos_record_exec_user_return(RIP,kind);assert(t.exec_image_trace.pending);
  t.type=ker::mod::sched::task::TaskType::PROCESS;
  wos_record_exec_user_return(RIP,kind);assert(!t.exec_image_trace.pending && t.has_run);
  assert(p::get_capture_status().written==2);
  wos_record_exec_user_return(RIP,kind);assert(p::get_capture_status().written==2);
  p::disable();std::array<p::PerfEvent,2> events{};assert(p::drain_events(events.data(),2,UINT32_MAX)==2);
  assert(events[0].pid==t.pid && events[1].pid==SUBJECT);
  assert(p::wki_unpack_trace_correlation(events[0].lag_v)==42 && p::wki_unpack_trace_correlation(events[1].lag_v)==42);
  assert(events[0].aux==kind && events[1].aux==kind);
  assert(p::wki_op_name(p::WkiPerfScope::LOCAL_PROC,16)==std::string_view("exec_image_owner"));
  assert(p::wki_op_name(p::WkiPerfScope::LOCAL_PROC,17)==std::string_view("exec_user_return"));
 }
 // A redirected signal return cannot consume entry; later sigreturn can.
 prepare(t);wos_record_exec_user_return(RIP+16,2);assert(t.exec_image_trace.pending);
 wos_record_exec_user_return(RIP,2);assert(!t.exec_image_trace.pending && p::get_capture_status().written==2);
 // Arming copies values, never retains a preparation/worker pointer.
 prepare(t);auto a=attempt();arm_native_exec_user_return(t,a,RIP);a={};
 wos_record_exec_user_return(RIP,4);assert(p::get_capture_status().written==2);
 // Replacement supersedes old entry. Default/fork construction starts cold.
 prepare(t);a=attempt();a.correlation=77;a.subject_pid=UINT64_MAX;
 arm_native_exec_user_return(t,a,RIP+32);wos_record_exec_user_return(RIP,1);assert(t.exec_image_trace.pending);
 wos_record_exec_user_return(RIP+32,1);assert(p::get_capture_status().written==2);
 assert(p::g_rings[0].events[1].pid==UINT64_MAX);
 Task child;assert(!child.exec_image_trace.pending);
 // Disabled/excluded/old-session entry consumes state but adds no clock/event.
 for(unsigned action=0;action<3;++action) {
  prepare(t);auto reads=clocks.load();
  if(action==0)p::disable();
  if(action==1)p::set_event_mask(p::PERF_MASK_SWITCH);
  if(action==2)p::start_recording(p::PERF_MASK_LOCAL_PROC);
  wos_record_exec_user_return(RIP,1);
  assert(!t.exec_image_trace.pending && p::get_capture_status().written==0 && clocks==reads);
 }
 prepare(t);a=attempt();a.active=false;arm_native_exec_user_return(t,a,RIP);
 auto reads=clocks.load();wos_record_exec_user_return(RIP,1);assert(p::get_capture_status().written==0 && clocks==reads);
 // Reset while admitted to the owner POINT cannot leak either point into new capture.
 prepare(t);reads=clocks.load();before_lock=[] {p::start_recording(p::PERF_MASK_LOCAL_PROC);};
 wos_record_exec_user_return(RIP,3);assert(!before_lock && p::get_capture_status().written==0 && clocks==reads);
 puts("PASS actual native image arm/consume: ownership, full64 identity, return paths, redirects, once, replacement, filtering and session races");
}
'''


def integration(exec_source, assembly, syscall):
    finish = function(exec_source, 'finish_native_exec_image')
    # Select the definition rather than the forward declaration.
    assert finish.index('ENTRY_ATTEMPT = PREPARED->trace.attempts.front()') < finish.index('cleanup_exec_preparation_with_status(*task, 0)')
    assert finish.index('cleanup_exec_preparation_with_status(*task, 0)') < finish.index('arm_native_exec_user_return')
    assert finish.index('arm_native_exec_user_return') < finish.index('// === CRITICAL SECTION:')
    assert 'ENTRY_ATTEMPT' not in finish[finish.index('// === CRITICAL SECTION:'):]
    assert assembly.count('call wos_record_exec_user_return') == 3
    for target in ('[r14 + GPREGS_SIZE]', '[r14]', 'r12'):
        assert re.search(r'mov rdi, ' + re.escape(target) + r'[^\n]*\n\s+mov esi, [34]\n\s+call wos_record_exec_user_return', assembly)
    for segment in assembly.split('call wos_record_exec_user_return')[:-1]:
        assert segment.rfind('call wos_restore_return_task_fpu') > segment.rfind('call wos_commit_handoff_task')
    assert syscall.count('call wos_record_exec_user_return') == 2
    normal = syscall[syscall.index('jne .sysret_rcx_corrupt_prepop'):syscall.index('.sysret_rcx_corrupt:')]
    assert normal.index('call wos_record_exec_user_return') < normal.index('popq') < normal.index('swapgs')
    signal = syscall[syscall.index('.signal_iret_return:'):]
    assert signal.index('call wos_record_exec_user_return') < signal.index('sub rsp, 40')


def main():
    exec_source = (ROOT/'modules/kern/src/syscalls_impl/process/exec.cpp').read_text()
    context = (ROOT/'modules/kern/src/platform/sys/context_switch.cpp').read_text()
    task = (ROOT/'modules/kern/src/platform/sched/task.hpp').read_text()
    assembly = (ROOT/'modules/kern/src/platform/sys/context_switch.asm').read_text()
    syscall = (ROOT/'modules/kern/src/platform/sys/syscall.asm').read_text()
    integration(exec_source, assembly, syscall)
    header = re.sub(r'^#(?:include|pragma).*$', '', (PERF/'perf_events.hpp').read_text(), flags=re.M)
    source = re.sub(r'^#include.*$', '', (PERF/'perf_events.cpp').read_text(), flags=re.M)
    faces = FACES.replace('TRACE_TYPE', block(task, r'^struct ExecImageTrace \{')+';')
    faces = faces.replace('STAGE_TYPE', block(exec_source, r'^struct LocalProcStage \{')+';')
    faces = faces.replace('ARM', function(exec_source, 'arm_native_exec_user_return'))
    consumer = block(context, r'^extern "C" void wos_record_exec_user_return\(')
    faces = faces.replace('CONSUMER', consumer)
    compiler = shutil.which('c++', path=os.defpath)
    with tempfile.TemporaryDirectory(prefix='wos-image-entry-') as temp:
        path=Path(temp);src=path/'test.cpp';binary=path/'test'
        src.write_text(PRELUDE+header+source+faces+CASES)
        subprocess.run([compiler,'-std=c++23','-O1','-g','-pthread','-fsanitize=address,undefined',str(src),'-o',str(binary)],check=True)
        subprocess.run([str(binary)],check=True,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'})


if __name__ == '__main__':
    main()
