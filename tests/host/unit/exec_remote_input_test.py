#!/usr/bin/env python3
"""Execute actual exec plan, owner submit and preparation with modeled endpoints."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tests/host/unit'))
from exec_trace_ownership_test import function

source = Path(os.environ.get('WOS_REMOTE_INPUT_TEST_SOURCE', ROOT / 'modules/kern/src/syscalls_impl/process/exec.cpp')).read_text()
start = source.index('struct RemoteExecPlan {')
plan = source[start:source.index('// The waiting worker retains', start)]
owner = function(source, 'run_remote_exec_owner')
prepare = function(source, 'try_process_remote_exec')
code = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>
namespace alloc {
bool tracking=false;size_t attempts=0,live=0,fail_at=0;
std::array<void*,32> pointers{};
void* get(size_t n){if(tracking&&++attempts==fail_at)return nullptr;auto*p=std::malloc(n?n:1);assert(p);if(tracking){auto i=std::find(pointers.begin(),pointers.end(),nullptr);assert(i!=pointers.end());*i=p;++live;}return p;}
void put(void*p){if(!p)return;auto i=std::find(pointers.begin(),pointers.end(),p);if(i!=pointers.end()){*i=nullptr;assert(live);--live;}std::free(p);}
}
void* operator new(size_t n){return alloc::get(n);}
void* operator new[](size_t n){return alloc::get(n);}
void* operator new(size_t n,const std::nothrow_t&)noexcept{return alloc::get(n);}
void* operator new[](size_t n,const std::nothrow_t&)noexcept{return alloc::get(n);}
void operator delete(void*p)noexcept{alloc::put(p);}
void operator delete[](void*p)noexcept{alloc::put(p);}
void operator delete(void*p,size_t)noexcept{alloc::put(p);}
void operator delete[](void*p,size_t)noexcept{alloc::put(p);}
void operator delete(void*p,const std::nothrow_t&)noexcept{alloc::put(p);}
void operator delete[](void*p,const std::nothrow_t&)noexcept{alloc::put(p);}
namespace ker::mod::security {struct Snapshot{int identity=7;};struct ExecIdentity{uint32_t uid{},gid{};bool set_uid{},set_gid{},nosuid{};uint32_t mode{};};}
// Value-only production interfaces; no allocator or loader implementation is modeled.
namespace ker::mod::mm::numa {
enum class Mode:uint8_t{LOCAL,PREFERRED,BIND,INTERLEAVE};
struct Policy{uint64_t nodes{};Mode mode{Mode::LOCAL};auto operator==(const Policy&)const->bool=default;};
struct AllocationContext{Policy policy{};uint16_t local_node{UINT16_MAX};uint64_t virtual_page_index{};};
}
namespace ker::loader::elf{enum class ElfAllocationOrigin:uint8_t{MISSING=0,EXECUTOR_CAPTURE=1,TRUSTED_LOCAL=2};}
namespace ker::mod::perf {enum class WkiPerfLocalProcOp{REMOTE_SPAWN};}
struct Lock{bool held=false;auto lock_irqsave()->uint64_t{assert(!held);held=true;return 0;}void unlock_irqrestore(uint64_t){assert(held);held=false;}};
struct ExecRequestArguments;struct WorkerExecHandoff;
namespace ker::mod::sched::task {
enum class TaskState{ACTIVE,EXITING};
struct Task{
 static constexpr size_t EXE_PATH_MAX=256,WKI_TARGET_HOSTNAME_MAX=64;
 static constexpr uint32_t WKI_TARGET_FLAG_LOCAL=1;
 struct FilesystemContext{int root_anchor{},cwd_anchor{};std::array<char,32>cwd{'/','\0'};};
 Task* canonical=this;bool acquire_ok=true,filesystem_ok=true,resource_cpu_admission_closed=false;
 size_t refs=1;uint32_t wki_target_flags{};std::array<char,64>wki_target_hostname{};std::array<char,256>exe_path{};
 bool wki_skip_legacy_placement=false;
 Lock resource_lock,path_metadata_lock;
 std::atomic<bool> exit_in_progress{false};std::atomic<TaskState>state{TaskState::ACTIVE};
 std::atomic<ExecRequestArguments*>owned_exec_arguments{nullptr};
 std::atomic<WorkerExecHandoff*>pending_exec_handoff{nullptr},active_exec_handoff{nullptr};
 auto credential_owner()->Task*{return canonical;}
 auto try_acquire()->bool{if(!acquire_ok)return false;++refs;return true;}
 void release(){assert(refs>1);--refs;}
 auto capture_filesystem_context(FilesystemContext&f)->bool{if(!filesystem_ok)return false;f.root_anchor=11;f.cwd_anchor=12;return true;}
};
}
using Task=ker::mod::sched::task::Task;
namespace ker::mod::sched::resource {
struct ExecAdmission{Task* owner{};auto begin(Task&t)->bool{if(owner)return false;owner=t.credential_owner();return owner!=nullptr;}void finish(){owner=nullptr;}~ExecAdmission(){finish();}};
struct ExecSafeWait{explicit ExecSafeWait(Task&){}void wait_for_release(){}};
}
namespace ker::syscall::vmem{struct SharedVmemPublicationGuard{};}
namespace ker::net::wki {
enum class WkiRemoteSpawnResult{LOCAL,FAILED,REMOTE};
struct TaskExecCredentialTransition{};struct TaskExecSignals{};
struct WkiRemoteExecCandidate{const char*path{};std::span<const uint8_t>bytes{};bool complete{};const char*target{};uint32_t target_flags{};};
struct WkiRemoteSpawnSpec{
 const char*const*argv{};const char*const*envp{};const char*cwd{};int*error_out{};
 int(*prepare)(Task&,void*){};void(*accepted)(Task&,void*){};
 int(*cutover)(Task&,uint32_t,uint64_t,TaskExecSignals&,void*){};void*context{};
 const TaskExecSignals*exec_signals{};uint32_t*deferred_proxy_id{};
 const Task::FilesystemContext*filesystem{};const TaskExecCredentialTransition*exec_credentials{};
 const WkiRemoteExecCandidate*exec_candidate{};
 std::optional<ker::mod::mm::numa::Policy>memory_policy{};
};
auto wki_try_remote_spawn(Task*,const WkiRemoteSpawnSpec&)->WkiRemoteSpawnResult;
}
using Result=ker::net::wki::WkiRemoteSpawnResult;
struct Stage{bool active{};ker::mod::perf::WkiPerfLocalProcOp op{};uint64_t bytes{};};
struct Trace{std::array<Stage,2>pending{};};
struct WorkerExecHandoff{bool remote{};ExecRequestArguments*remote_request{};Task*worker{};std::atomic<int>result{-EINPROGRESS};~WorkerExecHandoff();};
constexpr size_t EXEC_ARG_BYTES_MAX=2*1024*1024;
void consume_successful_one_shot_wki_target(Task*,Result){}
#define WOS_PERF_CALLSITE() 0
PLAN
struct ExecRequestArguments{std::unique_ptr<RemoteExecPlan>remote;ker::mod::security::Snapshot admitted_credentials;WorkerExecHandoff handoff;bool remote_worker_committed{};Trace trace;uint32_t remote_proxy_id{};ker::mod::mm::numa::Policy memory_policy{};ker::mod::mm::numa::AllocationContext allocation_context{};ker::loader::elf::ElfAllocationOrigin allocation_origin{ker::loader::elf::ElfAllocationOrigin::MISSING};};
WorkerExecHandoff::~WorkerExecHandoff(){delete remote_request;}
struct RemoteExecOutcome{Result result=Result::FAILED;int error{};WorkerExecHandoff*pending{};};
int prepare_remote_exec_owner(Task&,void*){return 0;}
void accept_remote_exec_owner(Task&,void*){}
int cutover_remote_exec_owner(Task&,uint32_t,uint64_t,ker::net::wki::TaskExecSignals&,void*){return 0;}
void end_local_proc_stage(Task*,ker::mod::perf::WkiPerfLocalProcOp,Stage&,int32_t,uint64_t,int){}
OWNER
struct Stat{uint32_t st_uid=1000,st_gid=2000,st_mode=0755;};
namespace ker::vfs{using Stat=::Stat;}
namespace test {
Result outcome=Result::LOCAL;size_t submits=0;bool worker_mode=false;
const char*const*original_args{};const char*const*original_env{};const uint8_t*original_elf{};size_t original_size{};
char* mutable_arg{};char*mutable_env{};uint8_t*mutable_elf{};
bool new_version=false;
constexpr ker::mod::mm::numa::Policy captured_policy{4,ker::mod::mm::numa::Mode::BIND};
}
auto ker::net::wki::wki_try_remote_spawn(Task*t,const WkiRemoteSpawnSpec&s)->Result{
 ++test::submits;assert(t&&s.exec_candidate&&s.argv&&s.envp&&s.context&&s.prepare&&s.accepted&&s.cutover);
 auto&plan=*static_cast<RemoteExecPlan*>(s.context);auto&c=*s.exec_candidate;
 if(EXPECT_REDUCED)assert(s.memory_policy && *s.memory_policy==test::captured_policy);
 assert(c.bytes.size()==test::original_size&&c.complete&&std::strcmp(c.path,"/bin/true")==0);
 assert(std::strcmp(t->exe_path.data(),"/old")==0&&!t->wki_skip_legacy_placement);
 assert(plan.actor->refs==2&&s.filesystem->root_anchor==11&&s.filesystem->cwd_anchor==12);
 if(test::worker_mode){
  assert(c.bytes.data()!=test::original_elf&&s.argv!=test::original_args&&s.envp!=test::original_env);
  // Worker input can be retired after acceptance; the owner's copies persist.
  test::mutable_arg[0]='X';test::mutable_env[0]='Y';test::mutable_elf[0]=0xff;
 }else if(test::new_version){assert(c.bytes.data()==test::original_elf&&s.argv==test::original_args&&s.envp==test::original_env);}
 assert(std::strcmp(s.argv[0],"true")==0&&s.argv[1]==nullptr&&std::strcmp(s.envp[0],"K=value")==0&&s.envp[1]==nullptr);
 assert(c.bytes[0]==0x7f);
 *s.error_out=test::outcome==Result::FAILED?-EACCES:0;
 if(plan.handoff){auto*h=std::exchange(plan.handoff,nullptr);h->result.store(test::outcome==Result::REMOTE?1:test::outcome==Result::LOCAL?0:-EACCES);}
 return test::outcome;
}
namespace ker::mod::sched {
void wake_task_for_signal(Task*){}
void wake_task_from_event(Task*owner){
 auto*h=owner->pending_exec_handoff.exchange(nullptr);assert(h&&h->remote_request);
 auto*r=std::exchange(h->remote_request,nullptr);owner->active_exec_handoff.store(h);owner->owned_exec_arguments.store(r);
 if(EXPECT_REDUCED){
  assert(r->memory_policy==test::captured_policy && r->allocation_context.policy==test::captured_policy);
  assert(r->allocation_context.local_node==2 && r->allocation_context.virtual_page_index==123);
  assert(r->allocation_origin==ker::loader::elf::ElfAllocationOrigin::EXECUTOR_CAPTURE);
 }
 auto result=run_remote_exec_owner(*owner,*r,h);assert(result.pending==nullptr);
 owner->active_exec_handoff.store(nullptr);owner->owned_exec_arguments.store(nullptr);delete r;
}
void kern_yield(){assert(false);}
}
PREPARE
template<class P>auto bind_sync(P&p,const uint8_t*elf,size_t size,const char*const*argv,const char*const*envp)->bool{
 if constexpr(requires{p.borrow_current_input(elf,size,argv,envp);})return p.borrow_current_input(elf,size,argv,envp);
 else{if(!p.copy_arguments(argv,envp))return false;p.bytes=new(std::nothrow)uint8_t[size];if(!p.bytes)return false;std::memcpy(p.bytes,elf,size);return true;}
}
template<class P>auto argument_view(const P&p)->const char*const*{if constexpr(requires{p.arguments();})return p.arguments();else return p.argv;}
template<class P>auto environment_view(const P&p)->const char*const*{if constexpr(requires{p.environment();})return p.environment();else return p.envp;}
int main(){
 test::new_version=NEW_VERSION;
 for(bool worker:{false,true})for(Result outcome:{Result::LOCAL,Result::FAILED,Result::REMOTE}){
  size_t const allocations=worker?6:test::new_version?1:5;
  for(size_t fail=0;fail<=allocations;++fail){
   Task owner,actor;auto&subject=worker?actor:owner;subject.canonical=&owner;std::memcpy(owner.exe_path.data(),"/old",5);
   ExecRequestArguments request;subject.owned_exec_arguments.store(&request);
   request.memory_policy=test::captured_policy;request.allocation_context={test::captured_policy,2,123};
   request.allocation_origin=ker::loader::elf::ElfAllocationOrigin::EXECUTOR_CAPTURE;
   char arg[]="true",env[]="K=value";const char*args[]={arg,nullptr};const char*envs[]={env,nullptr};std::array<uint8_t,64>elf{};elf[0]=0x7f;
   test::outcome=outcome;test::worker_mode=worker;test::submits=0;test::original_args=args;test::original_env=envs;test::original_elf=elf.data();test::original_size=elf.size();test::mutable_arg=arg;test::mutable_env=env;test::mutable_elf=elf.data();
   alloc::attempts=alloc::live=0;alloc::fail_at=fail;alloc::tracking=true;int error=99;
   auto result=try_process_remote_exec(subject,elf.data(),elf.size(),true,"/bin/true",args,envs,{},false,error);
   alloc::tracking=false;
   assert(alloc::live==0&&subject.refs==1&&owner.refs==1&&!request.remote&&!owner.active_exec_handoff.load()&&!owner.pending_exec_handoff.load());
   assert(std::strcmp(owner.exe_path.data(),"/old")==0&&!owner.wki_skip_legacy_placement);
   if(fail){assert(result==Result::FAILED&&error==-ENOMEM&&test::submits==0);}
   else{assert(result==outcome&&error==(outcome==Result::FAILED?-EACCES:0)&&test::submits==1);if(!worker){assert(arg[0]=='t'&&env[0]=='K'&&elf[0]==0x7f);if(EXPECT_REDUCED)assert(alloc::attempts==1);}}
  }
 }
 // Same combined argument budget, including forwarded/null vectors. Borrowing
 // cannot enlarge the old limit or change nonnull empty submit classification.
 std::array<uint8_t,8>empty_elf{};
 {RemoteExecPlan p;assert(bind_sync(p,empty_elf.data(),empty_elf.size(),nullptr,nullptr));assert(argument_view(p)&&environment_view(p)&&!argument_view(p)[0]&&!environment_view(p)[0]);}
 auto text=std::make_unique<char[]>(EXEC_ARG_BYTES_MAX+1);std::memset(text.get(),'a',EXEC_ARG_BYTES_MAX);text[EXEC_ARG_BYTES_MAX]='\0';
 const char*large_args[]={text.get(),nullptr};
 for(bool over:{false,true}){size_t length=EXEC_ARG_BYTES_MAX-3*sizeof(char*)-1+(over?1:0);text[length]='\0';{RemoteExecPlan p;assert(bind_sync(p,empty_elf.data(),empty_elf.size(),large_args,nullptr)==!over);}text[length]='a';}
 {RemoteExecPlan p;assert(!bind_sync(p,empty_elf.data(),empty_elf.size(),large_args,nullptr));}
 for(int problem:{0,1,2,3,4}){
  Task owner,actor;auto&subject=problem==4?actor:owner;subject.canonical=problem==0?nullptr:&owner;ExecRequestArguments request;subject.owned_exec_arguments.store(problem==1?nullptr:&request);
  if(problem==2)subject.acquire_ok=false;if(problem==3)subject.filesystem_ok=false;if(problem==4)owner.resource_cpu_admission_closed=true;
  char arg[]="true",env[]="K=value";const char*args[]={arg,nullptr};const char*envs[]={env,nullptr};int error=99;test::submits=0;alloc::attempts=alloc::live=alloc::fail_at=0;alloc::tracking=true;
  auto result=try_process_remote_exec(subject,empty_elf.data(),empty_elf.size(),true,"/bin/true",args,envs,{},false,error);alloc::tracking=false;
  assert(result==Result::FAILED&&error==(problem==2?-ENOMEM:problem==4?-EAGAIN:-ESRCH)&&!test::submits&&!alloc::live&&subject.refs==1);
 }
 {Task t;t.canonical=nullptr;t.wki_target_flags=Task::WKI_TARGET_FLAG_LOCAL;int error=99;alloc::attempts=0;alloc::tracking=true;assert(try_process_remote_exec(t,nullptr,0,false,nullptr,nullptr,nullptr,{},false,error)==Result::LOCAL&&error==0);alloc::tracking=false;assert(!alloc::attempts);}
 std::puts(EXPECT_REDUCED ? "Actual synchronous-owner/worker preparation and submit: LOCAL/FAILED/REMOTE, independent worker copies through input retirement, original image unchanged, every allocation failure/rollback/refcount PASS; owner exactly one allocation" : "Actual before owner/worker functional oracle PASS; no reduced-allocation assertion enabled");
}
'''.replace('PLAN',plan).replace('OWNER',owner).replace('PREPARE',prepare).replace('NEW_VERSION','true' if 'borrow_current_input' in plan else 'false').replace('EXPECT_REDUCED','false' if os.environ.get('WOS_REMOTE_INPUT_ALLOW_OLD') else 'true')
with tempfile.TemporaryDirectory(prefix='wos-remote-input-') as directory:
    cpp=Path(directory)/'test.cpp';binary=Path(directory)/'test';cpp.write_text(code)
    subprocess.run(['/usr/bin/clang++','-std=c++23','-O1','-g','-fsanitize=address,undefined',str(cpp),'-o',str(binary)],check=True)
    environment=os.environ.copy();environment['ASAN_OPTIONS']='detect_leaks=0'
    subprocess.run([str(binary)],env=environment,check=True)
