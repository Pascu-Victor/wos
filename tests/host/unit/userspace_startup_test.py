#!/usr/bin/env python3
"""Execute actual numeric consumer/perf, libc token/tag/main, and early assembly."""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from exec_trace_ownership_test import block, function
from perf_capture_kernel_test import PERF, PRELUDE

ROOT = Path(__file__).resolve().parents[3]
WOS = ROOT/'toolchain/src/mlibc/sysdeps/wos'
ABI = ROOT/'modules/kern/src/abi/callnums/perf_trace.h'

KERNEL_FACES = r'''
#include <cerrno>
#undef WOS_PERF_CALLSITE
#define WOS_PERF_CALLSITE() 123ULL
namespace ker::mod::cpu {unsigned current_cpu() {return 0;}}
namespace ker::mod::sched::task {
enum class TaskType {PROCESS,DAEMON};
TRACE_TYPE
struct Task {uint64_t pid=0x300000007ULL;bool is_thread=false;
 TaskType type=TaskType::PROCESS;ExecImageTrace exec_image_trace{};};
}
using Task=ker::mod::sched::task::Task;
Task* current=nullptr;
namespace ker::mod::sched {Task* get_current_task() {return current;}}
namespace ker::abi::sys_log {enum class sys_log_ops:uint64_t {LOG,LOG_LINE,LOG_EX,LOG_BLOCK_BEGIN,LOG_BLOCK_END,PERF_TRACE};}
namespace ker::syscall::log {
CONSUMER
DISPATCH
}
'''
KERNEL_CASES = r'''
namespace p=ker::mod::perf;
using ker::syscall::log::sys_log;
using Op=ker::abi::sys_log::sys_log_ops;
uint64_t marker(uint64_t phase,uint64_t session,uint64_t corr=42,uint64_t version=1,uint64_t reserved=0) {
 return sys_log(Op::PERF_TRACE,phase,session,corr,version,reserved);
}
void prepare(Task& task) {
 p::start_recording(p::PERF_MASK_LOCAL_PROC);current=&task;
 task.exec_image_trace={.subject_pid=0x100000007ULL,.initial_rip=0x100040ULL,
  .capture_session=p::current_wki_capture_session(p::WkiPerfScope::LOCAL_PROC,19),.correlation=42,.pending=false};
}
int main() {
 p::init();Task task;current=&task;
 // Session zero before the first reset is valid and not a disabled sentinel.
 p::enable();task.exec_image_trace.capture_session=0;task.exec_image_trace.correlation=42;
 assert(marker(1,0)==0 && p::get_capture_status().written==1);p::disable();
 for(bool dynamic:{false,true}) {
  prepare(task);auto session=task.exec_image_trace.capture_session;
  for(unsigned phase: dynamic?std::array<unsigned,5>{1,2,3,4,5}:std::array<unsigned,5>{1,3,2,4,5}) {
   auto count=p::get_capture_status().written;
   assert(marker(phase,session)==0 && p::get_capture_status().written==count+1);
   assert(marker(phase,session)==0 && p::get_capture_status().written==count+1);
  }
  assert(p::get_capture_status().written==5);p::disable();std::array<p::PerfEvent,5> events{};
  assert(p::drain_events(events.data(),5,UINT32_MAX)==5);
  for(auto& e:events)assert(e.pid==task.pid && p::wki_unpack_trace_correlation(e.lag_v)==42 && p::wki_unpack_trace_status(e.lag_v)==0);
  assert(p::wki_op_name(p::WkiPerfScope::LOCAL_PROC,19)==std::string_view("userspace_stage"));
 }
 // Additive RTLD milestones preserve both the dynamic and static CRT order.
 for(bool dynamic:{false,true}) {
  task={};prepare(task);auto session=task.exec_image_trace.capture_session;
  auto phases=dynamic?std::array<unsigned,8>{1,6,7,8,2,3,4,5}:std::array<unsigned,8>{1,3,6,7,8,2,4,5};
  for(unsigned phase:phases) {
   auto count=p::get_capture_status().written;
   assert(marker(phase,session)==0 && p::get_capture_status().written==count+1);
   assert(marker(phase,session)==0 && p::get_capture_status().written==count+1);
  }
  assert(task.exec_image_trace.emitted_user_stages==UINT8_MAX);
  p::disable();std::array<p::PerfEvent,8> events{};
  assert(p::drain_events(events.data(),8,UINT32_MAX)==8);
  for(unsigned i=0;i<events.size();++i) {
   assert(events[i].aux==phases[i] && events[i].pid==task.pid);
   assert(p::wki_unpack_trace_correlation(events[i].lag_v)==42);
  }
 }
 // Optional stages cannot precede image entry, skip predecessors or follow ready.
 for(unsigned early:{6,7,8}) {
  task={};prepare(task);auto session=task.exec_image_trace.capture_session;
  marker(early,session);assert(p::get_capture_status().written==0);
 }
 task={};prepare(task);auto optional_session=task.exec_image_trace.capture_session;
 marker(1,optional_session);marker(7,optional_session);marker(8,optional_session);
 assert(p::get_capture_status().written==1);
 marker(6,optional_session);marker(8,optional_session);assert(p::get_capture_status().written==2);
 marker(7,optional_session);marker(8,optional_session);assert(p::get_capture_status().written==4);
 task={};prepare(task);optional_session=task.exec_image_trace.capture_session;
 marker(1,optional_session);marker(2,optional_session);
 for(unsigned late:{6,7,8})marker(late,optional_session);
 assert(p::get_capture_status().written==2 && task.exec_image_trace.emitted_user_stages==3);
 // Ordered prerequisites, immutable owner/worker identity, default/fork state.
 prepare(task);auto session=task.exec_image_trace.capture_session;
 assert(marker(2,session)==0 && marker(4,session)==0 && marker(5,session)==0 && p::get_capture_status().written==0);
 marker(1,session);marker(4,session);assert(p::get_capture_status().written==1);
 marker(2,session);marker(3,session);marker(5,session);assert(p::get_capture_status().written==3);
 marker(4,session);marker(5,session);assert(p::get_capture_status().written==5);
 Task child;current=&child;assert(marker(1,session)==0 && p::get_capture_status().written==5);
 // Every invalid/stale context leaves the ring and image stage mask untouched.
 for(unsigned action=0;action<9;++action) {
  task={};prepare(task);session=task.exec_image_trace.capture_session;
  if(action==0)current=nullptr;
  if(action==1)task.type=ker::mod::sched::task::TaskType::DAEMON;
  if(action==2)task.is_thread=true;
  if(action==3)task.exec_image_trace.pending=true;
  if(action==4)task.exec_image_trace.capture_session=p::PERF_ANY_SESSION;
  if(action==5)++session;
  if(action==6)p::disable();
  if(action==7)p::set_event_mask(p::PERF_MASK_SWITCH);
  if(action==8)p::start_recording(p::PERF_MASK_LOCAL_PROC);
  auto clocks_before=clocks.load();assert(marker(1,session)==0);
  for(unsigned phase:{6,7,8})assert(marker(phase,session)==0);
  assert(p::get_capture_status().written==0 && task.exec_image_trace.emitted_user_stages==0 && clocks==clocks_before);
 }
 task={};prepare(task);session=task.exec_image_trace.capture_session;
 auto bad=static_cast<uint64_t>(-EINVAL);
 assert(marker(0,session)==bad && marker(9,session)==bad && marker(1,session,UINT64_MAX)==bad);
 assert(marker(1,session,42,2)==bad && marker(1,session,42,1,1)==bad && p::get_capture_status().written==0);
 // Whole64 capture identity survives high-bit sessions; reset under ringlock rejects old marker.
 p::g_capture_session.store(0x100000009ULL);task.exec_image_trace.capture_session=0x100000009ULL;
 marker(1,0x100000009ULL);assert(p::get_capture_status().written==1);
 task={};prepare(task);session=task.exec_image_trace.capture_session;
 before_lock=[] {p::start_recording(p::PERF_MASK_LOCAL_PROC);};marker(1,session);
 assert(!before_lock && p::get_capture_status().written==0);
 static_assert(sizeof(p::PerfEvent)==48);
 puts("PASS actual numeric syscall prefix/marker and actual perf: session0/full64, image ownership, prerequisites, duplicate/range, pending/default/fork/filter/reset races");
}
'''

LIBC_FACES = r'''
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <vector>
#include <cerrno>
namespace ker::abi {
enum class callnums:uint64_t {sys_log=0};
namespace sys_log {enum class sys_log_ops:uint64_t {PERF_TRACE=5};}
}
struct Call {uint64_t op,phase,session,corr,version,reserved;};
std::vector<Call> calls;int raw_error=0;
uint64_t syscall(ker::abi::callnums n,uint64_t op,uint64_t phase,uint64_t session,uint64_t corr,uint64_t version,uint64_t reserved) {
 assert(n==ker::abi::callnums::sys_log);calls.push_back({op,phase,session,corr,version,reserved});return static_cast<uint64_t>(-raw_error);
}
namespace mlibc {
struct PerfTrace {};
template<typename T>struct SysdepImpl;
template<>struct SysdepImpl<PerfTrace> {static int operator()(uint64_t,uint64_t,uint64_t,uint64_t);};
template<typename T>using Sysdeps=SysdepImpl<T>;
template<typename T>int sysdep(uint64_t a,uint64_t b,uint64_t c,uint64_t d) {return Sysdeps<T>{}(a,b,c,d);}
TAG
}
TOKEN
namespace mlibc {struct EntryStack {int argc=2;char** argv=nullptr;};EntryStack entry_stack;}
char** environ=nullptr;size_t __hwcap=17;bool loader_called=false;bool main_observed=false;bool replace_image=false;
struct ExitResult {int value;};struct ExecReplaced {};
extern "C" void __dlapi_enter(uintptr_t* stack) {loader_called=true;mlibc::perf_startup_point(mlibc::perf_startup_token(stack),2);}
void mocked_exit(int result) {throw ExitResult{result};}
#define exit mocked_exit
ENTRY
#undef exit
int app_main(int argc,char** argv,char** env) {
 assert(loader_called && __hwcap==0 && argc==2 && argv==nullptr && env==nullptr);main_observed=true;
 if(replace_image)throw ExecReplaced{};
 return 27;
}
'''
LIBC_CASES = r'''
int main() {
 std::array<uintptr_t,24> stack{}; // argc2,argvs,NULL,env,NULL,unknownaux,tracepairs,NULL
 stack[0]=2;stack[1]=11;stack[2]=12;stack[4]=13;stack[6]=7;stack[7]=0x1000;
 stack[8]=WOS_AT_PERF_TRACE_VERSION;stack[9]=1;
 stack[10]=WOS_AT_PERF_TRACE_SESSION;stack[11]=0x100000009ULL;
 stack[12]=WOS_AT_PERF_TRACE_CORRELATION;stack[13]=UINT32_MAX;
 auto token=mlibc::perf_startup_token(stack.data());assert(token.version==1 && token.session==0x100000009ULL && token.correlation==UINT32_MAX);
 stack[11]=0;token=mlibc::perf_startup_token(stack.data());assert(token.version==1 && token.session==0);
 mlibc::perf_startup_point(token,4);assert(calls.size()==1 && calls.back().op==5 && calls.back().session==0 && calls.back().corr==UINT32_MAX && calls.back().version==1 && calls.back().reserved==0);
 calls.clear();assert(mlibc::perf_startup_token(nullptr).version==0);
 for(unsigned mode=0;mode<6;++mode) {
  auto bad=stack;
  if(mode==0)bad[8]=0; // old kernel, no capability
  if(mode==1)bad[9]=2;
  if(mode==2)bad[10]=0; // incomplete advertisement
  if(mode==3)bad[11]=UINT64_MAX;
  if(mode==4)bad[13]=uint64_t(UINT32_MAX)+1;
  if(mode==5){bad[14]=WOS_AT_PERF_TRACE_VERSION;bad[15]=1;}
  auto t=mlibc::perf_startup_token(bad.data());assert(t.version==0);mlibc::perf_startup_point(t,4);assert(calls.empty());
 }
 raw_error=EINVAL;assert(mlibc::Sysdeps<mlibc::PerfTrace>{}(4,0,42,1)==EINVAL);raw_error=0;calls.clear();
 // An older version-1 kernel can reject the additive points without stopping
 // the loader or preventing the original ready marker from being attempted.
 raw_error=EINVAL;
 for(unsigned phase:{6,7,8})mlibc::perf_startup_point(token,phase);
 raw_error=0;mlibc::perf_startup_point(token,2);
 assert(calls.size()==4 && calls[0].phase==6 && calls[1].phase==7 && calls[2].phase==8 && calls[3].phase==2);
 calls.clear();
 for(unsigned phase:{6,7,8,2})mlibc::perf_startup_point({},phase);
 assert(calls.empty());
 try {__mlibc_entry(stack.data(),app_main);assert(false);}catch(ExitResult e){assert(e.value==27);}
 assert(main_observed && calls.size()==3 && calls[0].phase==2 && calls[1].phase==4 && calls[2].phase==5);
 calls.clear();replace_image=true;loader_called=main_observed=false;
 try {__mlibc_entry(stack.data(),app_main);assert(false);}catch(ExecReplaced){}
 assert(main_observed && calls.size()==2 && calls[0].phase==2 && calls[1].phase==4); // never invent MAIN_END
 auto old=stack;old[8]=0;calls.clear();replace_image=false;
 try {__mlibc_entry(old.data(),app_main);assert(false);}catch(ExitResult e){assert(e.value==27);}
 assert(calls.empty()); // actual new entry/tag stay silent on old kernel
 puts("PASS actual WOS token/tag/main entry: full64 and zero session, old/unknown/partial/duplicate capability, errno and return/nonreturn boundaries");
}
'''


def compile_run(path, name, code):
    src=path/(name+'.cpp');out=path/name;src.write_text(code)
    subprocess.run([shutil.which('c++',path=os.defpath),'-std=c++23','-O1','-g','-pthread','-fsanitize=address,undefined',str(src),'-o',str(out)],check=True)
    subprocess.run([str(out)],check=True,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'})


def assembly_test(path):
    # Run the production macro, replacing only its privilege boundary. The mock
    # preserves the documented syscall inputs and destroys RAX/RCX/R11, so the
    # actual save/restore and static two-marker path must survive that boundary.
    header=(WOS/'include/mlibc/perf-startup-asm.h').read_text().replace('#pragma once','')
    header=header.replace('#include "../callnums/perf_trace.h"', ABI.read_text().replace('#pragma once',''))
    registers=['rax','rbx','rcx','rdx','rsi','rdi','rbp','r8','r9','r10','r11','r12','r13','r14','r15']
    wrappers=[]
    for name,stage in [('invoke_rtld',1),('invoke_crt',3)]:
        wrappers.append(f'.global {name}\n{name}:\n\tpushfq\n'+
            ''.join(f'\tpush %{r}\n' for r in ['rbx','rbp','r12','r13','r14','r15'])+
            '\tmov %rsp, host_rsp(%rip)\n\tmov %rdi, %rsp\n'+
            ''.join(f'\tmov ${0x100+i}, %{r}\n' for i,r in enumerate(registers))+
            '\tstd\n\tstc\n\tpushfq\n\tpopq expected_flags(%rip)\n'+
            f'\tWOS_PERF_STARTUP {stage}\n'+
            ''.join(f'\tmov %{r}, observed+{8*i}(%rip)\n' for i,r in enumerate(registers))+
            '\tmov %rsp, observed+120(%rip)\n\tpushfq\n\tpopq observed+128(%rip)\n'+
            '\tmov host_rsp(%rip), %rsp\n'+
            ''.join(f'\tpop %{r}\n' for r in ['r15','r14','r13','r12','rbp','rbx'])+
            '\tpopfq\n\tret\n')
    mock=r'''
.global mock_syscall
mock_syscall:
    push %rax
    mov mock_count(%rip), %rax
    cmp $4, %rax
    jae .Loverflow
    imul $56, %rax, %rax
    lea mock_calls(%rip), %rcx
    add %rax, %rcx
    pop %rax
    mov %rax, 0(%rcx)
    mov %rdi, 8(%rcx)
    mov %rsi, 16(%rcx)
    mov %rdx, 24(%rcx)
    mov %r8, 32(%rcx)
    mov %r9, 40(%rcx)
    mov %r10, 48(%rcx)
    incq mock_count(%rip)
    mov $-22, %rax
    mov $0xBAD, %rcx
    mov $0xBAD, %r11
    ret
.Loverflow:
    ud2
.bss
.global observed, expected_flags, mock_count, mock_calls
.comm observed,136,8
.comm expected_flags,8,8
.comm mock_count,8,8
.comm mock_calls,224,8
.comm host_rsp,8,8
.section .note.GNU-stack,"",@progbits
'''
    asm=path/'startup.S'
    asm.write_text(header.replace('\tsyscall','\tcall mock_syscall')+'\n.text\n'+''.join(wrappers)+mock)
    driver=path/'assembly.cpp'
    driver.write_text(ABI.read_text().replace('#pragma once','')+r'''
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
extern "C" {
void invoke_rtld(uintptr_t*);void invoke_crt(uintptr_t*);
extern uint64_t observed[17],expected_flags,mock_count,mock_calls[4][7];
}
int main() {
 alignas(16) std::array<uintptr_t,512> storage{};auto* stack=storage.data()+128;
 stack[0]=2;stack[1]=11;stack[2]=12;stack[4]=13;
 stack[6]=7;stack[7]=0x1000;stack[8]=WOS_AT_PERF_TRACE_VERSION;stack[9]=1;
 stack[10]=WOS_AT_PERF_TRACE_SESSION;stack[11]=0x100000009ULL;
 stack[12]=WOS_AT_PERF_TRACE_CORRELATION;stack[13]=UINT32_MAX;
 auto run=[&](void (*invoke)(uintptr_t*),std::initializer_list<unsigned> phases) {
  mock_count=0;invoke(stack);
  for(unsigned i=0;i<15;++i)assert(observed[i]==0x100+i);
  assert(observed[15]==reinterpret_cast<uintptr_t>(stack) && observed[16]==expected_flags);
  assert(mock_count==phases.size());unsigned i=0;
  for(auto phase:phases) {
   auto* call=mock_calls[i++];assert(call[0]==0 && call[1]==5 && call[2]==phase);
   assert(call[3]==stack[11] && call[4]==stack[13] && call[5]==1 && call[6]==0);
  }
 };
 run(invoke_rtld,{1});run(invoke_crt,{3});stack[7]=0;run(invoke_crt,{1,3});
 stack[11]=0;stack[13]=0;run(invoke_crt,{1,3});run(invoke_rtld,{1});
 for(unsigned mode=0;mode<6;++mode) {
  std::array<uintptr_t,20> saved{};for(unsigned i=0;i<20;++i)saved[i]=stack[i];
  if(mode==0)stack[8]=0;
  if(mode==1)stack[9]=2;
  if(mode==2)stack[10]=0;
  if(mode==3)stack[11]=UINT64_MAX;
  if(mode==4)stack[13]=uint64_t(UINT32_MAX)+1;
  if(mode==5){stack[14]=WOS_AT_PERF_TRACE_VERSION;stack[15]=1;}
  run(invoke_crt,{});run(invoke_rtld,{});
  for(unsigned i=0;i<20;++i)stack[i]=saved[i];
 }
 puts("PASS actual early assembly: all15 GPs/RFLAGS/RSP, raw numeric ABI, dynamic/static/error return, zero/full64 identity and unsupported capability silence");
}
''')
    out=path/'assembly'
    subprocess.run([shutil.which('c++',path=os.defpath),'-std=c++23','-O1','-g',str(driver),str(asm),'-o',str(out)],check=True)
    subprocess.run([str(out)],check=True)
    # Audit the unmodified real-syscall macro, not the mock wrapper's legitimate
    # references to its fixture globals. It must need no loader relocations.
    audit=path/'entry-audit.S';obj=path/'entry-audit.o'
    audit.write_text(header+'\n.text\n.global probe\nprobe:\n WOS_PERF_STARTUP 1\n WOS_PERF_STARTUP 3\n ret\n.section .note.GNU-stack,"",@progbits\n')
    subprocess.run([shutil.which('cc',path=os.defpath),'-c',str(audit),'-o',str(obj)],check=True)
    relocations=subprocess.check_output(['readelf','-r',str(obj)],text=True)
    assert 'There are no relocations in this file.' in relocations,relocations
    print('PASS actual early assembly relocation audit: no GOT/PLT/TLS or other relocations')


def integration(rtld_source=None):
    source=(ROOT/'modules/kern/src/syscalls_impl/process/exec.cpp').read_text()
    # Only native captured root stack construction advertises. Preserve spawn.
    native=function(source,'wos_proc_execve_impl')
    assert 'STARTUP_ATTEMPT = request->trace.attempts.front()' in native
    assert 'AUXV_BASE_QWORDS + (new_interp_base != 0 ? 2 : 0) + (STARTUP_TRACE ? 6 : 0)' in native
    assert 'SmallVec<uint64_t, 36> auxv' in native
    assert native.index('auxv.push_back(WOS_AT_PERF_TRACE_VERSION)') < native.index('auxv.push_back(AT_NULL)')
    assert 'STARTUP_TRACE' not in source[:source.index('auto wos_proc_execve(uint64_t')]
    # Maximum existing dynamic vector28words + diagnostic6 <= inline36; six
    # extra words preserve the existing structured-stack parity/alignment.
    policy=(ROOT/'modules/kern/src/platform/security/exec_policy.hpp').read_text()
    assert 'IDENTITY_AUXV_WORDS = 10' in policy
    for interp in [0,2]:
        for trace in [0,6]:
            assert 16+10+interp+trace<=36
            assert (16+10+interp+trace)%2==(16+10+interp)%2
    assembly=(ROOT/'toolchain/src/mlibc/options/rtld/x86_64/entry.S').read_text()
    assert assembly.index('WOS_PERF_STARTUP WOS_PERF_STAGE_IMAGE_ENTRY') < assembly.index('call relocateSelf')
    for name in ['crt1.S','Scrt1.S']:
        s=(WOS/'crt-x86_64'/name).read_text();assert s.index('WOS_PERF_STARTUP WOS_PERF_STAGE_CRT_ENTRY') < s.index('mov %rsp, %rdi')
    rtld=(rtld_source or ROOT/'toolchain/src/mlibc/options/rtld/generic/main.cpp').read_text()
    assert rtld.index('linker.initObjects(initialRepository.get())') < rtld.index('WOS_PERF_STAGE_RTLD_READY') < rtld.index('return executableSO->entry')
    # Boundaries retain all work, including interpreter RELRO. TCB_READY does
    # not promise initialized TLS images; initObjects still performs that work.
    points=['WOS_PERF_STAGE_RTLD_DEPENDENCIES_READY','WOS_PERF_STAGE_RTLD_LINK_READY','WOS_PERF_STAGE_RTLD_TCB_READY','WOS_PERF_STAGE_RTLD_READY']
    assert all(rtld.count(point)==1 for point in points)
    assert rtld.index('discoverDependenciesFromLoadedObject(current)') < rtld.index(points[0]) < rtld.index('linker.linkObjects(executableSO)')
    assert rtld.index('linker.protectRelro(ldso)') < rtld.index(points[1]) < rtld.index('auto tcb = allocateTcb()')
    assert rtld.index('mlibc::sysdep<TcbSet>(tcb)') < rtld.index(points[2]) < rtld.index('linker.initObjects(initialRepository.get())')
    assert rtld.count('mlibc::perf_startup_token(entry_stack)')==1
    assert all('mlibc::perf_startup_point(startup_token, '+point+')' in rtld for point in points)
    assert ABI.read_text()==(WOS/'include/callnums/perf_trace.h').read_text()
    assert 'PerfTrace,' in (WOS/'include/mlibc/sysdeps.hpp').read_text()
    assert 'struct PerfTrace {};' in (ROOT/'toolchain/src/mlibc/options/internal/include/mlibc/sysdep-tags.hpp').read_text()
    assert 'SYSDEP_FUNC(PerfTrace,' in (ROOT/'toolchain/src/mlibc/options/internal/include/mlibc/sysdep-signatures.hpp').read_text()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--consumer-source',type=Path,help='Saved or mutated sys_log.cpp for actual-body negative checks')
    parser.add_argument('--rtld-source',type=Path,help='Saved or mutated main.cpp for boundary negative checks')
    args=parser.parse_args()
    integration(args.rtld_source)
    header=re.sub(r'^#(?:include|pragma).*$', '', (PERF/'perf_events.hpp').read_text(),flags=re.M)
    perf=re.sub(r'^#include.*$', '', (PERF/'perf_events.cpp').read_text(),flags=re.M)
    task=(ROOT/'modules/kern/src/platform/sched/task.hpp').read_text()
    log=(args.consumer_source or ROOT/'modules/kern/src/syscalls_impl/log/sys_log.cpp').read_text()
    dispatcher=function(log,'sys_log');end=dispatcher.index('// A byte-counted log')
    # Retain the actual numeric dispatch prefix; the unrelated logging branch is
    # outside this fixture, rather than a parallel model of the operation.
    dispatcher=dispatcher[:end]+'return 999;\n}'
    faces=KERNEL_FACES.replace('TRACE_TYPE',block(task,r'^struct ExecImageTrace \{')+';').replace('CONSUMER',function(log,'record_userspace_startup_marker')).replace('DISPATCH',dispatcher)
    abi=ABI.read_text().replace('#pragma once','')
    token=re.sub(r'^#(?:include|pragma).*$', '', (WOS/'include/mlibc/perf-startup.hpp').read_text(),flags=re.M)
    sysdeps=(WOS/'generic/sysdeps.cpp').read_text();tag=block(sysdeps,r'^int Sysdeps<PerfTrace>::operator\(\)')
    entry=block((WOS/'generic/entry.cpp').read_text(),r'^extern "C" void\n__mlibc_entry\(')
    libc=LIBC_FACES.replace('TAG',tag).replace('TOKEN',token).replace('ENTRY',entry)
    with tempfile.TemporaryDirectory(prefix='wos-startup-') as temp:
        p=Path(temp)
        compile_run(p,'kernel',PRELUDE+abi+header+perf+faces+KERNEL_CASES)
        compile_run(p,'libc',abi+libc+LIBC_CASES)
        assembly_test(p)


if __name__=='__main__':
    main()
