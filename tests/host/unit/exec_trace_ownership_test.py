#!/usr/bin/env python3
"""Run actual exec trace, wrapper, cleanup, claim and service boundary code.

Loader, publication, scheduling and RPC endpoints are controlled host stubs.
This validates telemetry ownership around those boundaries, not kernel emulation.
--old-helper / --old-wrapper accept a saved exec.cpp for negative evidence.
"""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def block(source, pattern):
    match = re.search(pattern, source, re.M)
    assert match, pattern
    end, depth = source.index('{', match.start()) + 1, 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[match.start():end]


def function(source, name):
    return block(source, rf'^(?:\[\[nodiscard\]\] )?(?:auto|void) {name}\([^{{;]*\{{')


PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#define WOS_PERF_CALLSITE() 123ULL
namespace ker::syscall::process { class ExecRequestArguments; class PreparedNativeExecImage; struct WorkerExecHandoff; }
struct Lock {
 bool held=false;
 uint64_t lock_irqsave() { assert(!held); held=true; return 0; }
 void unlock_irqrestore(uint64_t) { assert(held); held=false; }
};
namespace ker::mod::cpu { struct GPRegs {}; unsigned current_cpu() { return 2; } }
namespace ker::mod::security { struct Snapshot {}; }
namespace ker::mod::sched::task {
enum class TaskState { ACTIVE, EXITING };
struct Task {
 uint64_t pid=0x100000007ULL;
 bool is_thread=false,has_exited=false;
 std::atomic<bool> exit_in_progress{false},exec_preparation_teardown_in_progress{false};
 std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<ker::syscall::process::PreparedNativeExecImage*> owned_exec_preparation{nullptr};
 std::atomic<ker::syscall::process::ExecRequestArguments*> owned_exec_arguments{nullptr};
 std::atomic<ker::syscall::process::WorkerExecHandoff*> pending_exec_handoff{nullptr},active_exec_handoff{nullptr};
 Lock resource_lock;
 ker::mod::security::Snapshot credentials() { return {}; }
};
}
using Task=ker::mod::sched::task::Task;
Task* current=nullptr;
namespace ker::mod::sched { Task* get_current_task() { return current; } }
namespace ker::mod::power { bool shutdown=false; bool shutdown_in_progress() { return shutdown; } }
namespace ker::mod::dbg { void panic_handler(const char*) { std::abort(); } }
[[noreturn]] void hcf() { std::abort(); }
namespace ker::mod::time {
uint64_t now=1000; unsigned reads=0;
uint64_t get_us() { ++reads; return now++; }
}
namespace ker::mod::perf {
enum class WkiPerfScope:uint8_t { LOCAL_PROC=8 };
enum class WkiPerfPhase:uint8_t { BEGIN=1,END=2,POINT=3 };
enum class WkiPerfLocalProcOp:uint8_t {
 EXECVE=2,ELF_READ=3,ARG_COPY=4,OPEN_ACCESS=5,NEW_IMAGE=6,LOAD_ELF=7,LOAD_INTERP=8,STACK_SETUP=9,COMMIT=10,DESTROY_OLD=11,REMOTE_SPAWN=12
};
struct Event { uint64_t pid; uint8_t op; WkiPerfPhase phase; uint32_t corr; int32_t status; uint32_t aux; };
std::vector<Event> events;
bool enabled=true; uint8_t excluded=255; unsigned correlations=0,summaries=0;
constexpr uint64_t PERF_ANY_SESSION=UINT64_MAX;
uint64_t session=0;
bool is_wki_scope_recording_enabled(WkiPerfScope,uint8_t op=0) { return enabled && op!=excluded; }
uint64_t current_wki_capture_session(WkiPerfScope scope,uint8_t op) {
 return is_wki_scope_recording_enabled(scope,op)?session:PERF_ANY_SESSION;
}
uint32_t next_wki_trace_correlation() { return ++correlations; }
void record_wki_event(uint32_t,uint64_t pid,WkiPerfScope,uint8_t op,WkiPerfPhase phase,uint16_t,uint16_t,
 uint32_t corr,int32_t status,uint32_t aux,uint64_t,uint64_t expected=PERF_ANY_SESSION) {
 assert(current==nullptr || !current->resource_lock.held);
 if (!is_wki_scope_recording_enabled(WkiPerfScope::LOCAL_PROC,op) || (expected!=PERF_ANY_SESSION && expected!=session)) return;
 if (op==static_cast<uint8_t>(WkiPerfLocalProcOp::EXECVE) && phase==WkiPerfPhase::END) {
  assert(current && current->owned_exec_arguments.load()==nullptr && current->owned_exec_preparation.load()==nullptr);
  assert(current->exec_preparation_teardown_in_progress.load());
 }
 events.push_back({pid,op,phase,corr,status,aux});
}
void record_wki_summary(WkiPerfScope scope,uint8_t op,uint16_t,uint16_t,int32_t,uint64_t,bool,uint32_t,uint64_t,uint64_t expected=PERF_ANY_SESSION) {
 if (is_wki_scope_recording_enabled(scope,op) && (expected==PERF_ANY_SESSION || expected==session)) ++summaries;
}
}
namespace ker::net::wki { enum class WkiRemoteSpawnResult:uint8_t { LOCAL,REMOTE,FAILED }; }
namespace ker::syscall::multiproc { unsigned exits=0; void wos_thread_exit_current() { ++exits; } }
namespace ker::syscall::process {
constexpr int MAX_SHEBANG_DEPTH=4;
'''

FACES = r'''
struct ExecArgumentsSnapshot { std::array<char,8> path{}; const char** argv{}; const char** envp{}; };
class ExecRequestArguments;
struct WorkerExecHandoff {
 ~WorkerExecHandoff();
 bool remote{}; ExecRequestArguments* remote_request{}; Task* worker{}; NativeExecCompletion completion{};
 std::atomic<int> result{-EINPROGRESS};
};
std::function<void()> release_hook;
unsigned discards=0,argument_destroys=0,image_destroys=0;
class PreparedNativeExecImage {
 public: ExecTraceState trace;
 void discard() { ++discards; if (release_hook) release_hook(); }
 ~PreparedNativeExecImage() { ++image_destroys; }
};
class ExecRequestArguments {
 public:
 ExecArgumentsSnapshot snapshot; ker::mod::security::Snapshot admitted_credentials; NativeExecCompletion completion;
 WorkerExecHandoff handoff; ExecTraceState trace; uint32_t remote_proxy_id{}; bool remote_worker_committed{};
 ~ExecRequestArguments() { ++argument_destroys; }
};
WorkerExecHandoff::~WorkerExecHandoff(){delete remote_request;}
void cleanup_exec_preparation(Task&);
struct RemoteExecOutcome {
 ker::net::wki::WkiRemoteSpawnResult result=ker::net::wki::WkiRemoteSpawnResult::FAILED;
 int error=-EAGAIN; WorkerExecHandoff* pending{};
};
int snapshot_error=0,loader_error=0,commit_error=0,handoff_error=0;
unsigned recursion=0; bool remote_case=false,release_worker_early=false; int remote_result=0;
Task* canonical=nullptr;
auto snapshot_exec_arguments(Task&,uint64_t,uint64_t,uint64_t,ExecArgumentsSnapshot&) -> int { return snapshot_error; }
void publish_remote_exec_proxy(Task&,uint32_t);
void wos_asm_park_exec_owner(uint64_t);
uint64_t wos_proc_execve_impl(const char*,const char* const*,const char* const*,ker::mod::cpu::GPRegs&,int);
uint64_t finish_native_exec_image(Task*,PreparedNativeExecImage*,NativeExecCompletion,WorkerExecHandoff* = nullptr);
uint64_t handoff_worker_exec(Task&,PreparedNativeExecImage&,ExecRequestArguments&);
RemoteExecOutcome run_remote_exec_owner(Task&,ExecRequestArguments&,WorkerExecHandoff*);
'''

ENDPOINTS = r'''
uint64_t finish_native_exec_image(Task* task,PreparedNativeExecImage* p,NativeExecCompletion c,WorkerExecHandoff* h) {
 current=task;
 auto& stage=p->trace.pending.at(c.trace_depth);
 end_local_proc_stage(task,ker::mod::perf::WkiPerfLocalProcOp::COMMIT,stage,commit_error,0,123);
 if (commit_error) { cleanup_exec_preparation_with_status(*task,commit_error); if(h)acknowledge_worker_exec(*task,*h,commit_error); return static_cast<uint64_t>(commit_error); }
 if(h)acknowledge_worker_exec(*task,*h,0);
 p->trace.pending.at(c.trace_depth)=begin_local_proc_stage_for_subject(c.subject_pid,ker::mod::perf::WkiPerfLocalProcOp::DESTROY_OLD,0,123);
 cleanup_exec_preparation_with_status(*task,0); return 0;
}
uint64_t handoff_worker_exec(Task& worker,PreparedNativeExecImage&,ExecRequestArguments& r) {
 if (handoff_error) return static_cast<uint64_t>(handoff_error);
 auto& h=r.handoff;h.worker=&worker;h.completion=r.completion;
 canonical->pending_exec_handoff.store(&h);
 current=canonical;
 assert(claim_pending_exec_handoff(*canonical)==&h);
 auto completion=h.completion;
 // Simulate the actual acceptance ACK boundary before immediate worker request destruction.
 acknowledge_worker_exec(*canonical,h,0);
 current=&worker;cleanup_exec_preparation(worker);
 current=canonical;
 auto ret=finish_native_exec_image(canonical,canonical->owned_exec_preparation.load(),completion);
 current=&worker;return ret;
}
RemoteExecOutcome run_remote_exec_owner(Task& owner,ExecRequestArguments& r,WorkerExecHandoff* h) {
 if(remote_result==1) {
  acknowledge_worker_exec(owner,*h,1);
  auto* worker=h->worker;
  if(release_worker_early){current=worker;cleanup_exec_preparation(*current);} // frees h before owner endpoint use
  current=&owner;
  r.remote_proxy_id=77;
  for(auto& slot:r.trace.pending)if(slot.active && slot.op==ker::mod::perf::WkiPerfLocalProcOp::REMOTE_SPAWN)
   end_local_proc_stage(&owner,slot.op,slot,1,slot.bytes,123);
  return {ker::net::wki::WkiRemoteSpawnResult::REMOTE,0,nullptr};
 }
 return {remote_result<0?ker::net::wki::WkiRemoteSpawnResult::FAILED:ker::net::wki::WkiRemoteSpawnResult::LOCAL,remote_result,h};
}
void publish_remote_exec_proxy(Task&,uint32_t id) { assert(id==77); }
void wos_asm_park_exec_owner(uint64_t) {}
uint64_t wos_proc_execve_impl(const char*,const char* const*,const char* const*,ker::mod::cpu::GPRegs&,int) {
 auto* r=current->owned_exec_arguments.load();
 for(unsigned d=0;d<=recursion;++d)r->trace.begin(current,d,123);
 // The omitted loader is a controlled producer, not a model of VFS/MM.
 auto op=remote_case?ker::mod::perf::WkiPerfLocalProcOp::REMOTE_SPAWN:ker::mod::perf::WkiPerfLocalProcOp::STACK_SETUP;
 auto& pending=r->trace.stage(current,recursion,op,0,123);pending.bytes=99;
 if(loader_error)return static_cast<uint64_t>(loader_error);
 if(remote_case) {
  auto& h=r->handoff;h.remote=true;h.worker=current;h.remote_request=new ExecRequestArguments;
  canonical->pending_exec_handoff.store(&h);
  auto* worker=current;current=canonical;service_pending_exec_handoff();current=worker;
  if(remote_result==1){r->remote_worker_committed=true;return 0;}
  assert(r->handoff.result.load()==remote_result);
  r->handoff.remote=false;r->handoff.result.store(-EINPROGRESS);
  end_local_proc_stage(worker,op,pending,remote_result,99,123);
  if(remote_result<0)return static_cast<uint64_t>(remote_result);
 }
 else end_local_proc_stage(current,op,pending,0,99,123);
 r->trace.stage(current,recursion,ker::mod::perf::WkiPerfLocalProcOp::COMMIT,0,123);
 r->completion={.trace_depth=recursion,.subject_pid=current->pid};
 current->owned_exec_preparation.store(new PreparedNativeExecImage);
 return 0;
}
} // process
using namespace ker::syscall::process;
using namespace ker::mod::perf;
void reset(Task& task) {
 current=&task;events.clear();correlations=summaries=0;ker::mod::time::reads=0;ker::mod::time::now=1000;
 enabled=true;excluded=255;session=0;snapshot_error=loader_error=commit_error=handoff_error=0;recursion=0;remote_case=release_worker_early=false;remote_result=0;
 release_hook={};discards=argument_destroys=image_destroys=0;ker::syscall::multiproc::exits=0;
}
void paired(unsigned attempts,int status,uint64_t subject) {
 std::map<std::tuple<uint64_t,uint8_t,uint32_t>,std::pair<unsigned,unsigned>> counts;
 unsigned begins=0,ends=0;std::map<uint32_t,uint32_t> depths;std::vector<uint32_t> closed_depths;
 for(auto& e:events) {
  auto& c=counts[{e.pid,e.op,e.corr}];
  if(e.phase==WkiPerfPhase::BEGIN)++c.first;else ++c.second;
  if(e.op==static_cast<uint8_t>(WkiPerfLocalProcOp::EXECVE)) {
   assert(e.pid==subject);
   if(e.phase==WkiPerfPhase::BEGIN){++begins;depths[e.corr]=e.aux;}
   else {++ends;assert(e.status==status);closed_depths.push_back(depths.at(e.corr));}
  }
 }
 assert(begins==attempts && ends==attempts);
 for(unsigned d=0;d<attempts;++d)assert(closed_depths.at(d)==attempts-d-1);
 for(auto& [k,c]:counts)assert(c.first==1 && c.second==1);
}
int main() {
 Task owner,worker;owner.pid=0x200000007ULL;worker.is_thread=true;canonical=&owner;ker::mod::cpu::GPRegs regs;
 // All exact errno results, including snapshot failure before the loader runs.
 for(int error:{-EFAULT,-E2BIG,-ENOENT,-ENOEXEC,-EACCES,-ENOMEM,-ELOOP,-EOVERFLOW,-EIO,-EAGAIN}) {
  Task t;reset(t);snapshot_error=error;
  assert(static_cast<int64_t>(wos_proc_execve(1,2,3,regs))==error);paired(1,error,t.pid);assert(argument_destroys==1);
  for(unsigned d=0;d<=4;++d) {
   reset(t);recursion=d;loader_error=error;
   assert(static_cast<int64_t>(wos_proc_execve(1,2,3,regs))==error);paired(d+1,error,t.pid);
   assert(!t.owned_exec_arguments.load() && !t.owned_exec_preparation.load());
  }
 }
 for(unsigned d=0;d<=4;++d) {
  Task t;reset(t);recursion=d;
  release_hook=[&] { assert(t.exec_preparation_teardown_in_progress && t.owned_exec_preparation.load());auto count=events.size();cleanup_exec_preparation(t);assert(events.size()==count); };
  assert(wos_proc_execve(1,2,3,regs)==0);paired(d+1,0,t.pid);assert(discards==1 && argument_destroys==1 && image_destroys==1);
 }
 // Deferred completion carries the original full64-bit worker identity beyond ACK/destruction.
 for(unsigned d=0;d<=4;++d) {
  reset(worker);recursion=d;assert(wos_proc_execve(1,2,3,regs)==0);paired(d+1,0,worker.pid);
  assert(!owner.active_exec_handoff.load() && !worker.owned_exec_arguments.load());
 }
 for(int error:{-EAGAIN,-ESRCH,-ECANCELED,-EIO}) {
  Task t;reset(t);commit_error=error;assert(static_cast<int64_t>(wos_proc_execve(1,2,3,regs))==error);paired(1,error,t.pid);
  reset(worker);handoff_error=error;assert(static_cast<int64_t>(wos_proc_execve(1,2,3,regs))==error);paired(1,error,worker.pid);
 }
 for(int result:{0,-EACCES,-ECANCELED,1}) {
  reset(worker);remote_case=true;remote_result=result;recursion=3;
  auto ret=wos_proc_execve(1,2,3,regs);assert(static_cast<int64_t>(ret)==(result<0?result:0));paired(4,result<0?result:0,worker.pid);
  assert(!owner.active_exec_handoff.load() && !owner.owned_exec_arguments.load());
 }
 // Canonical remote service must also survive immediate acknowledged worker request destruction.
 reset(worker);remote_result=1;release_worker_early=true;
 auto* accepted=new ExecRequestArguments;worker.owned_exec_arguments.store(accepted);
 for(unsigned d=0;d<=4;++d){accepted->trace.begin(&worker,d,123);accepted->trace.stage(&worker,d,WkiPerfLocalProcOp::REMOTE_SPAWN,0,123);}
 accepted->handoff.remote=true;accepted->handoff.worker=&worker;accepted->handoff.remote_request=new ExecRequestArguments;
 owner.pending_exec_handoff.store(&accepted->handoff);current=&owner;service_pending_exec_handoff();paired(5,0,worker.pid);
 // A holder-only remote claim has no worker; preserve its independently owned trace.
 reset(owner);WorkerExecHandoff holder_only;holder_only.remote=true;
 holder_only.remote_request=new ExecRequestArguments;
 holder_only.remote_request->trace.begin(&owner,0,123);
 owner.pending_exec_handoff.store(&holder_only);
 assert(claim_pending_exec_handoff(owner)==&holder_only && !holder_only.remote_request);
 cleanup_exec_preparation(owner);acknowledge_worker_exec(owner,holder_only,-ECANCELED);
 paired(1,-ECANCELED,owner.pid);
 // Default fatal cleanup closes every attempt and pending stage once; reentry is inert.
 reset(worker);auto* r=new ExecRequestArguments;worker.owned_exec_arguments.store(r);
 for(unsigned d=0;d<=4;++d){r->trace.begin(&worker,d,123);r->trace.stage(&worker,d,WkiPerfLocalProcOp::STACK_SETUP,0,123);}
 cleanup_exec_preparation(worker);cleanup_exec_preparation(worker);paired(5,-ECANCELED,worker.pid);
 // Cancel queued and claimed native/remote requests using the actual result-publication function.
 for(bool remote:{false,true})for(bool claimed:{false,true}) {
  reset(worker);auto* request=new ExecRequestArguments;worker.owned_exec_arguments.store(request);
  for(unsigned d=0;d<=4;++d){request->trace.begin(&worker,d,123);request->trace.stage(&worker,d,WkiPerfLocalProcOp::COMMIT,0,123);}
  auto& h=request->handoff;h.worker=&worker;h.remote=remote;
  if(remote)h.remote_request=new ExecRequestArguments;
  else {auto* p=new PreparedNativeExecImage;p->trace=std::exchange(request->trace,{});worker.owned_exec_preparation.store(p);}
  owner.pending_exec_handoff.store(&h);current=&owner;
  if(claimed)assert(claim_pending_exec_handoff(owner)==&h);
  cancel_pending_exec_handoff(owner);assert(h.result.load()==-ECANCELED);
  assert(!owner.pending_exec_handoff.load() && !owner.active_exec_handoff.load());
  current=&worker;cleanup_exec_preparation(worker);current=&owner;cleanup_exec_preparation(owner);
  paired(5,-ECANCELED,worker.pid);
 }
 // Disabled/filter paths add no clocks/correlation/summary/events; END after a late enable stays inert.
 for(bool off:{false,true}) {
  Task t;reset(t);enabled=!off;if(!off)excluded=static_cast<uint8_t>(WkiPerfLocalProcOp::EXECVE);
  auto span=begin_local_proc_stage(&t,WkiPerfLocalProcOp::EXECVE,0,123);
  enabled=true;excluded=255;end_local_proc_stage(&t,WkiPerfLocalProcOp::EXECVE,span,0,0,123);
  assert(events.empty() && correlations==0 && summaries==0 && ker::mod::time::reads==0);
 }
 for(int mode:{0,1}) {
  Task t;reset(t);if(mode==0)enabled=false;else excluded=static_cast<uint8_t>(WkiPerfLocalProcOp::ARG_COPY);
  if(mode==0){loader_error=-ENOENT;wos_proc_execve(1,2,3,regs);assert(events.empty() && correlations==0 && summaries==0 && ker::mod::time::reads==0);}
  else {wos_proc_execve(1,2,3,regs);for(auto&e:events)assert(e.op!=excluded);paired(1,0,t.pid);}
 }
 // Backward clocks clamp0; huge wall durations saturate instead of wrapping.
 {
  Task t;reset(t);auto span=begin_local_proc_stage(&t,WkiPerfLocalProcOp::ELF_READ,0,123);
  ++session;events.clear();correlations=summaries=0;auto reads=ker::mod::time::reads;
  assert(end_local_proc_stage(&t,WkiPerfLocalProcOp::ELF_READ,span,0,99,123)==0);
  assert(events.empty() && summaries==0 && ker::mod::time::reads==reads);
 }
 for(bool backward:{true,false}) {
  Task t;reset(t);auto span=begin_local_proc_stage(&t,WkiPerfLocalProcOp::ELF_READ,0,123);
  ker::mod::time::now=backward?1:static_cast<uint64_t>(UINT32_MAX)+2000;
  auto elapsed=end_local_proc_stage(&t,WkiPerfLocalProcOp::ELF_READ,span,-EIO,99,123);
  assert(elapsed==(backward?0:UINT32_MAX));auto n=events.size();end_local_proc_stage(&t,WkiPerfLocalProcOp::ELF_READ,span,0,0,123);assert(events.size()==n);
 }
 std::puts("PASS actual exec trace/wrapper/cleanup/claim/service ownership boundaries");
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--old-helper', type=Path)
    parser.add_argument('--old-wrapper', type=Path)
    parser.add_argument('--old-stages', type=Path)
    args = parser.parse_args()
    source = (ROOT / 'modules/kern/src/syscalls_impl/process/exec.cpp').read_text()
    if args.old_stages:
        saved = args.old_stages.read_text()
        for name in ('begin_local_proc_stage_for_subject', 'end_local_proc_stage'):
            source = source.replace(function(source, name), function(saved, name))
    prelude = PRELUDE.replace(' Lock resource_lock;', ' struct { uint64_t syscall_kernel_stack=0; } context; Lock resource_lock;')
    helpers = '\n'.join([function(source, 'clamp_perf_aux'), block(source, r'^struct LocalProcStage \{')+';',
                          block(source, r'^struct NativeExecCompletion \{')+';', function(source, 'begin_local_proc_stage_for_subject'),
                          function(source, 'begin_local_proc_stage'), function(source, 'end_local_proc_stage'),
                          block(source, r'^struct ExecTraceState \{')+';'])
    if args.old_helper:
        old = args.old_helper.read_text()
        # Same admitted-stage API, actual saved helper bodies, minimal host hooks.
        code = prelude + '\n'.join([function(old,'record_local_proc_event'),function(old,'clamp_perf_aux'),
                  block(old,r'^struct LocalProcStage \{')+';',function(old,'begin_local_proc_stage'),function(old,'end_local_proc_stage')])
        code += r'''
} int main(){ Task t;current=&t;ker::mod::perf::enabled=false;
 auto stage=ker::syscall::process::begin_local_proc_stage(&t,ker::mod::perf::WkiPerfLocalProcOp::EXECVE,0,123);
 ker::syscall::process::end_local_proc_stage(&t,ker::mod::perf::WkiPerfLocalProcOp::EXECVE,stage,0,0,123);
 assert(ker::mod::time::reads==0 && ker::mod::perf::correlations==0);
}'''
    else:
        wrapper = function(args.old_wrapper.read_text() if args.old_wrapper else source,'wos_proc_execve')
        code = prelude + helpers + FACES + '\n'.join(function(source,name) for name in [
            'cleanup_exec_preparation_with_status','cleanup_exec_preparation','adopt_exec_arguments',
            'acknowledge_worker_exec','claim_pending_exec_handoff','cancel_pending_exec_handoff','service_pending_exec_handoff']) + wrapper + ENDPOINTS
    with tempfile.TemporaryDirectory(prefix='wos-exec-trace-') as tmp:
        src,binary=Path(tmp)/'trace.cpp',Path(tmp)/'trace'
        src.write_text(code)
        compiler=shutil.which('c++',path=os.defpath)
        assert compiler
        subprocess.run([compiler,'-std=c++23','-Wall','-Wextra','-Werror','-O1','-g',
                        '-fsanitize=address,undefined','-fno-omit-frame-pointer',str(src),'-o',str(binary)],check=True,timeout=60)
        subprocess.run([str(binary)],check=True,timeout=15,env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'})


if __name__ == '__main__':
    main()
