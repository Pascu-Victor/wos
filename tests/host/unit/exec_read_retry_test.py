#!/usr/bin/env python3
"""Compile the real pipe read, Task interruption classifiers and READ dispatch.

Scheduling, exec publication and File/usercopy lifetimes use controlled host
endpoints. This forces LOCAL fallback before native preparation; it does not
emulate the full kernel. --old-dispatch accepts the saved syscall.cpp.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def block(source, pattern):
    match = re.search(pattern, source, re.M)
    assert match, pattern
    begin = source.index('{', match.start())
    end, depth = begin + 1, 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[match.start():end]


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/types.h>
#define WOS_PERF_CALLSITE() 1ULL
struct Replaced {};
struct Exited {};
unsigned pins=0,read_calls=0,pauses=0,resumes=0,services=0,waits=0,parks=0;
bool accounting=true,copy_ok=true;
std::function<void()> service_hook,wait_hook,park_hook;
namespace ker::mod::sched::task {
struct Task {
 std::atomic<bool> process_exit_requested{false};
 std::atomic<void*> pending_exec_handoff{nullptr};
 bool epoch=false,exec_read_interrupted=false,sigsuspend_active=false;
 uint64_t pid=0x100000001ULL,bits=0,mask=0;
 std::array<uint64_t,64> handlers{};
 bool exec_quiescence_requested() const { return epoch; }
 uint64_t signal_deliverable_bits() { return bits & ~mask; }
 struct Handler { uint64_t handler; };
 Handler signal_handler_snapshot(unsigned n) { return {handlers[n]}; }
 bool consume_signal_for_delivery(uint64_t bit) { bool existed=(bits&bit)!=0;bits&=~bit;return existed; }
 void signal_mask_store(uint64_t value) { mask=value; }
 uint64_t signal_frame_saved_mask() { sigsuspend_active=false;return 0; }
'''
BACKENDS = r'''
};
}
using Task=ker::mod::sched::task::Task;
Task subject;
Task* current=&subject;
void unlocked() { assert(pins==0); }
namespace ker::mod::sched {
Task* get_current_task() { return current; }
bool pause_syscall_accounting() { unlocked();++pauses;bool old=accounting;accounting=false;return old; }
void resume_syscall_accounting() { unlocked();assert(!accounting);++resumes;accounting=true; }
namespace resource {
void wait_for_exec_quiescence(Task&) { unlocked();assert(!accounting);++waits;if(wait_hook)wait_hook(); }
}
namespace task { enum class WaitChannelKind { LOCAL_PIPE }; }
void preemptible_syscall_park(const char*,task::WaitChannelKind) { ++parks;if(park_hook)park_hook();else assert(false); }
void kern_yield() { assert(false); }
}
namespace ker::syscall::process {
void exit_current_if_process_exit_requested() { unlocked();assert(!accounting);if(current->process_exit_requested.load())throw Exited{}; }
void service_pending_exec_handoff() { unlocked();assert(!accounting);++services;if(service_hook)service_hook(); }
}
namespace ker::mod::time { uint64_t get_us() { static uint64_t t=0;return ++t; } }
namespace ker::mod::perf { enum class WkiPerfLocalPipeOp { READ,BLOCK_READ };enum class WkiPerfPhase { BEGIN,END,POINT }; }
namespace ker::vfs {
struct Lock { bool held=false;uint64_t lock_irqsave(){assert(!held);held=true;return 0;}
 void unlock_irqrestore(uint64_t){assert(held);held=false;} };
struct PipeState { Lock lock;size_t count=0,capacity=64;bool read_closed=false,write_closed=false;
 std::atomic<int> open_ends{2};std::vector<uint64_t> readers_waiting,writers_waiting,write_poll_waiting; } pipe;
struct File { void* private_data=&pipe;int open_flags=0; } file;
constexpr size_t PIPE_COPY_CHUNK=16;
using PipeWakeList=std::vector<uint64_t>;
struct Logger { template<class...T>static void warn(const char*,T...){assert(false);} };
using log=Logger;
bool pipe_diag_enabled(){return false;}
uint64_t perf_current_pid(){return current?current->pid:0;}
const char* current_task_name(){return "owned";}
template<class...T>void perf_record_local_pipe_event(T...){}
template<class...T>void perf_record_local_pipe_summary(T...){}
void pipe_collect_waiters_locked(std::vector<uint64_t>&,PipeWakeList&,size_t*){}
void pipe_reschedule_waiters(PipeWakeList&,size_t){}
void pipe_copy_from_ring_locked(PipeState* s,char* buf,size_t n){assert(s->lock.held);std::memset(buf,'R',n);s->count-=n;}
bool pipe_copy_to_caller(void* out,char* data,size_t n){assert(!pipe.lock.held);if(!copy_ok)return false;std::memcpy(out,data,n);return true;}
bool pipe_register_waiter(std::vector<uint64_t>& waiters,uint64_t pid){waiters.push_back(pid);return true;}
'''
DISPATCH_FACES = r'''
}
namespace ker::mod::perf { uint32_t next_wki_trace_correlation(){static uint32_t c=0;return ++c;} }
namespace ker::syscall::vfs {
int64_t sys_vfs(uint64_t op,uint64_t fd,uint64_t out,uint64_t count,uint64_t actual,uint64_t extra) {
 assert(op==7 && fd==12 && extra==0);++read_calls;
 struct FilePin { FilePin(){++pins;}~FilePin(){--pins;} } pin;
 auto rc=ker::vfs::pipe_read(&ker::vfs::file,reinterpret_cast<void*>(out),count,0);
 if(rc>=0 && actual)*reinterpret_cast<size_t*>(actual)=static_cast<size_t>(rc);
 return rc;
}
}
namespace ker::mod::sys {
namespace sched=ker::mod::sched;
'''
CASES = r'''
}
void reset() {
 assert(!ker::vfs::pipe.lock.held && pins==0);
 subject.process_exit_requested=false;subject.pending_exec_handoff=nullptr;
 subject.epoch=subject.exec_read_interrupted=subject.sigsuspend_active=false;
 subject.bits=subject.mask=0;subject.handlers.fill(0);current=&subject;
 read_calls=pauses=resumes=services=waits=parks=0;accounting=copy_ok=true;
 service_hook=wait_hook=park_hook={};ker::vfs::pipe.count=0;
 ker::vfs::pipe.read_closed=ker::vfs::pipe.write_closed=false;ker::vfs::file.open_flags=0;
}
void pending(){subject.pending_exec_handoff=reinterpret_cast<void*>(1);}
int64_t run(size_t len=1) {
 char out[16]{};size_t actual=999;
 auto result=static_cast<int64_t>(ker::mod::sys::read_vfs_with_exec_retry(7,12,reinterpret_cast<uint64_t>(out),len,reinterpret_cast<uint64_t>(&actual),0));
 if(result>0)assert(out[0]=='R' && actual==static_cast<size_t>(result));
 else if(result<0)assert(actual==999);
 assert(accounting && pins==0 && !ker::vfs::pipe.lock.held);return result;
}
int main() {
 reset();ker::vfs::pipe.count=1;assert(run()==1 && read_calls==1 && pauses==0);
 // The original dispatcher leaks EINTR here; real new READ dispatch retries
 // after canonical service chooses LOCAL, with no old VFS owner on the stack.
 reset();pending();service_hook=[] {subject.pending_exec_handoff=nullptr;ker::vfs::pipe.count=1;};
 assert(run()==1 && read_calls==2 && services==1 && pauses==1 && resumes==1);
 // Retry must also survive a second native handoff published after LOCAL.
 reset();pending();service_hook=[] {if(services==1)pending();else {subject.pending_exec_handoff=nullptr;ker::vfs::pipe.count=1;}};
 assert(run()==1 && read_calls==3 && services==2 && pauses==2 && resumes==2);
 reset();pending();service_hook=[] {assert(!subject.exec_read_interrupted);throw Replaced{};};
 try {run();assert(false);}catch(const Replaced&){assert(read_calls==1 && pauses==1 && resumes==0 && pins==0 && !accounting);}
 // Worker epoch rollback reaches the existing safe wait outside File pins.
 reset();subject.epoch=true;wait_hook=[] {assert(subject.epoch);subject.epoch=false;ker::vfs::pipe.count=1;};
 assert(run()==1 && waits==1 && read_calls==2);
 reset();pending();service_hook=[] {if(services==3){subject.pending_exec_handoff=nullptr;ker::vfs::pipe.count=1;}};
 assert(run()==1 && read_calls==4 && pauses==3 && resumes==3);
 for(unsigned signo:{9U,10U,12U}) {
  reset();subject.bits=uint64_t{1}<<(signo-1);subject.handlers[signo-1]=signo==9?0:123;
  assert(run()==-EINTR && read_calls==1 && services==0 && pauses==0);
  reset();pending();subject.bits=uint64_t{1}<<(signo-1);subject.handlers[signo-1]=signo==9?0:123;
  assert(run()==-EINTR && read_calls==1 && services==0 && pauses==1 && resumes==1);
 }
 reset();pending();service_hook=[] {subject.pending_exec_handoff=nullptr;subject.bits=1ULL<<9;subject.handlers[9]=123;};
 assert(run()==-EINTR && read_calls==1 && services==1);
 // Ignored signals remain consumed, and masked signals remain pending.
 for(unsigned signo:{17U,23U,28U,18U,12U}) {
  reset();pending();subject.bits=uint64_t{1}<<(signo-1);subject.handlers[signo-1]=signo==12?1:0;
  service_hook=[] {subject.pending_exec_handoff=nullptr;ker::vfs::pipe.count=1;};
  assert(run()==1 && subject.bits==0 && read_calls==2);
 }
 reset();pending();subject.mask=subject.bits=1ULL<<11;service_hook=[] {subject.pending_exec_handoff=nullptr;ker::vfs::pipe.count=1;};
 assert(run()==1 && subject.bits==(1ULL<<11));
 reset();subject.process_exit_requested=true;pending();assert(run()==-EINTR && pauses==0 && services==0);
 reset();subject.epoch=true;wait_hook=[] {subject.process_exit_requested=true;};
 try {run();assert(false);}catch(const Exited&){assert(pins==0 && !accounting && services==0 && resumes==0);}
 reset();subject.exec_read_interrupted=true;ker::vfs::pipe.write_closed=true;assert(run()==0 && pauses==0 && !subject.exec_read_interrupted);
 reset();pending();ker::vfs::pipe.count=2;assert(run()==1 && ker::vfs::pipe.count==1 && services==0);
 reset();pending();ker::vfs::file.open_flags=O_NONBLOCK;assert(run()==-EAGAIN && pauses==0);
 reset();pending();ker::vfs::pipe.count=1;copy_ok=false;assert(run()==-EFAULT && pauses==0);
 reset();assert(run(0)==0 && pauses==0);
 reset();current=nullptr;assert(run()==-ESRCH && pauses==0);
 reset();subject.exec_read_interrupted=true;subject.bits=1ULL<<11;subject.handlers[11]=123;
 assert(run()==-EINTR && pauses==0 && !subject.exec_read_interrupted);
 // Classifier cause capture shares the exact pending/epoch observation.
 reset();bool cause=true;assert(!subject.has_interrupting_signal_pending(&cause) && !cause);
 pending();assert(subject.has_interrupting_signal_pending(&cause) && cause);
 subject.process_exit_requested=true;assert(subject.has_interrupting_signal_pending(&cause) && !cause);
 std::puts("actual READ/pipe/classifier internal exec interruption boundaries passed");
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--old-dispatch', type=Path)
    args = parser.parse_args()
    task = (ROOT / 'modules/kern/src/platform/sched/task.hpp').read_text()
    syscall = (ROOT / 'modules/kern/src/platform/sys/syscall.cpp').read_text()
    core = (ROOT / 'modules/kern/src/vfs/core.cpp').read_text()
    classifiers = '\n'.join(block(task, rf'    auto {name}\([^{{;]*\{{') for name in (
        'has_interrupting_signal_pending', 'has_interrupting_user_signal_pending'))
    pipe = block(core, r'    static auto pipe_read = \[\][^{]*\{').replace('    static auto', 'auto', 1)+';\n'
    helper = block(syscall, r'^auto read_vfs_with_exec_retry\([^{{;]*\{')
    assert 'A1 == static_cast<uint64_t>(abi::vfs::ops::READ) ? read_vfs_with_exec_retry' in syscall
    if args.old_dispatch:
        old = args.old_dispatch.read_text()
        assert 'case abi::callnums::VFS:\n            result = ker::syscall::vfs::sys_vfs(A1, A2, A3, A4, A5, A6);' in old
        helper = '''auto read_vfs_with_exec_retry(uint64_t op,uint64_t a1,uint64_t a2,uint64_t a3,uint64_t a4,uint64_t a5)->uint64_t {
return ker::syscall::vfs::sys_vfs(op,a1,a2,a3,a4,a5);
}'''
    # Declare correlation before the real lambda references it.
    prelude = PRELUDE.replace('#define WOS_PERF_CALLSITE()', 'namespace ker::mod::perf { uint32_t next_wki_trace_correlation(); }\n#define WOS_PERF_CALLSITE()')
    with tempfile.TemporaryDirectory(prefix='wos-exec-read-retry-') as temp:
        cpp, exe = Path(temp)/'test.cpp', Path(temp)/'test'
        cpp.write_text(prelude+classifiers+BACKENDS+pipe+DISPATCH_FACES+helper+CASES)
        subprocess.run(['/usr/bin/c++', '-std=c++23', '-Wall', '-Wextra', '-Werror', '-Wno-attributes',
                        '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie',
                        str(cpp), '-o', str(exe)], check=True)
        subprocess.run([str(exe)], check=True, timeout=15, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0'})


if __name__ == '__main__':
    main()
