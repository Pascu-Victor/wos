#!/usr/bin/env python3
"""Execute actual fork recorders and attempt-entry reset with finite Host ports.

Copy predicate capture is exercised by the managed/fork fixtures. This fixture
checks copied scalar fidelity and deferred emission, not native MM ownership.
"""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import block, function

ROOT = Path(os.environ.get("WOS_FORK_DIAGNOSTICS_ROOT", Path(__file__).resolve().parents[3]))
PROCESS_SOURCE = Path(os.environ.get("WOS_FORK_DIAGNOSTICS_SOURCE", ROOT / "modules/kern/src/syscalls_impl/process/process.cpp"))
MANAGED_HUGE_SOURCE = PROCESS_SOURCE.parents[2] / "platform/mm/managed_huge.hpp"
VIRT_SOURCE = PROCESS_SOURCE.parents[2] / "platform/mm/virt.hpp"
STAGES = (
    'pressure-admission', 'kernel-stack', 'task', 'name', 'vfs-rules', 'credentials',
    'pagemap', 'vm-snapshot', 'thread', 'per-cpu', 'fd-table', 'child-publication',
    'scheduler-publication',
)
PRELUDE = r'''
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>
std::vector<std::string> calls;
bool held=false;
unsigned lock_count=0, logs=0, ends=0;
uint64_t recorded_bytes=0;
int32_t recorded_status=0;
std::string message;
std::vector<std::string> messages;
unsigned diagnostic_owners=0;
bool allow_context=true, diagnostic_irq=true, discard_refusal_log=false;
uint32_t diagnostic_preempt=0;
struct Lock {
 uint64_t lock_irqsave() { assert(!held); held=true; ++lock_count;
  calls.emplace_back("pool-lock"); return 0x202; }
 void unlock_irqrestore(uint64_t flags) { assert(held && flags==0x202);
  held=false; calls.emplace_back("pool-unlock"); }
};
namespace ker::mod::mm::phys {
SNAPSHOT
struct Pool { Lock lock; size_t capacity=0, free_count=0, high_watermark=0; } kernel_stack_pool;
GETTER
uint64_t free_pages=123456;
uint64_t get_free_mem_pages() { assert(!held); calls.emplace_back("physical"); return free_pages; }
}
namespace ker::mod::cpu { uint32_t id=7; uint32_t current_cpu() { return id; } }
namespace ker::mod::smt { uint64_t cores=4;
 uint64_t get_core_count() { assert(!held); calls.emplace_back("cores"); return cores; } }
namespace ker::mod::sched::task {
enum class TaskType:uint32_t { PROCESS=1, DAEMON=2 };
enum class TaskState:uint32_t { ACTIVE=1, WAITING=2 };
struct Task { uint64_t pid=0x200000007ULL;
 TaskType type{TaskType::PROCESS}; std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<bool> scheduler_transition_active{false}; uint64_t syscall_account_start_us=5;
}; }
namespace ker::mod::sched {
task::Task* diagnostic_current=nullptr;
bool can_query_current_task(){assert(allow_context);return true;}
task::Task* get_current_task(){assert(allow_context);return diagnostic_current;}
bool interrupts_enabled(){assert(allow_context);return diagnostic_irq;}
uint32_t preempt_count(){assert(allow_context);return diagnostic_preempt;}
struct RunQueueStats { uint64_t expired_task_count, active_task_count, wait_queue_count; };
RunQueueStats queues[8]{};
RunQueueStats get_run_queue_stats(uint64_t cpu) { assert(!held && cpu<ker::mod::smt::cores);
 calls.emplace_back("rq"+std::to_string(cpu)); return queues[cpu]; }
struct SchedulerTraceStats { uint64_t gc_deferred_depth; };
uint64_t deferred=9;
SchedulerTraceStats get_scheduler_trace_stats(uint64_t cpu) {
 assert(!held && cpu==0 && ker::mod::smt::cores>0); calls.emplace_back("deferred"); return {deferred}; }
}
namespace ker::mod::mm::huge {
GEOMETRY_TYPES
}
namespace ker::mod::mm::virt {
COPY_TYPES
}
namespace ker::mod::sys {
struct SyscallPreparationScope { bool prepared; bool ready() const { return prepared; } };
}
namespace ker::syscall::vmem {
struct LegacyFileForkPreparation { enum class MatchConflict { None, RegistryBusy }; };
}
namespace ker::mod::perf { enum class WkiPerfLocalProcOp { FORK }; }
#define WOS_PERF_CALLSITE() 123ULL
namespace ker::syscall::process {
struct process_log {
 static void warn(const char* format, ...) {
  assert(!held);
  bool const refusal=std::strstr(format,"fork_refusal_v1")==format;
  bool const geometry=std::strstr(format,"fork_geometry_refusal_v1")==format;
  bool const copy=std::strstr(format,"fork_copy_refusal_v1")==format;
  if(refusal || geometry || copy){assert(diagnostic_owners==0);if(discard_refusal_log)return;}
  ++logs; calls.emplace_back("journal");
  char data[512]; va_list args; va_start(args,format);
  int length=vsnprintf(data,sizeof(data),format,args); va_end(args);
  assert(length>=0 && static_cast<size_t>(length)<sizeof(data)); message=data;
  messages.emplace_back(data);
 }
};
namespace detail {
TRANSLATOR
int actual_attempt_entry(ker::mod::sched::task::Task& actor, bool ready,
 ForkVmFailurePoint* failure_point, ker::mod::mm::huge::RangePrepareWitness* geometry_failure,
 ker::mod::mm::virt::ForkCopyRefusalWitness* copy_failure,
 ker::mod::mm::virt::ForkSourceAdmission* source_admission=nullptr) {
 ker::mod::sys::SyscallPreparationScope preparation{ready};
 ker::syscall::vmem::LegacyFileForkPreparation::MatchConflict* conflict=nullptr;
ACTUAL_ATTEMPT_ENTRY_PREFIX
 return 0;
}
}
DIAGNOSTIC
REFUSAL_DIAGNOSTIC
void end_local_proc_stage(ker::mod::sched::task::Task*, ker::mod::perf::WkiPerfLocalProcOp,
 int&, int32_t status, uint64_t bytes, uint64_t callsite) {
 assert(!held && callsite==123); ++ends; calls.emplace_back("end");
 recorded_bytes=bytes; recorded_status=status;
}
uint64_t finish(ker::mod::sched::task::Task* parent, int64_t result, uint64_t bytes, const char* stage) {
 using namespace ker::mod; int FORK_STAGE=0;
LAMBDA;
 return finish_fork(result,bytes,stage);
}
}
'''
CASES = r'''
// Finite owner/IF endpoints wrap the actual recorder and actual finish lambda.
// They are not a replacement process/VM ownership algorithm.
struct DiagnosticOwner {
 DiagnosticOwner(){++diagnostic_owners;}
 ~DiagnosticOwner(){assert(diagnostic_owners>0);--diagnostic_owners;diagnostic_irq=false;}
};
uint64_t refusal_path(ker::mod::sched::task::Task* parent,int raw,int mapped,uint32_t point,bool reservation=false) {
 using ker::syscall::process::ForkRefusalDiagnostic;
 ACTUAL_REFUSAL_DECLARATION
 {
  DiagnosticOwner owner;
  if(reservation)refusal.capture(*parent,"process-reservation",raw,mapped);
  else refusal.capture(*parent,"vm-snapshot",raw,mapped,point,1,"coordinator-return");
  // The emission must use the captured values only after this endpoint retires.
  parent->pid=0;parent->type=ker::mod::sched::task::TaskType::DAEMON;
  parent->state.store(ker::mod::sched::task::TaskState::WAITING);
  parent->scheduler_transition_active.store(true);parent->syscall_account_start_us=0;
  allow_context=false;
  return ker::syscall::process::finish(parent,mapped,42,nullptr);
 }
}
void refusal_cases() {
 using namespace ker::mod; using ker::syscall::process::detail::public_fork_vm_error;
 using Point=ker::syscall::process::detail::ForkVmFailurePoint;
 for(bool reservation:{false,true}) {
  sched::task::Task parent; sched::diagnostic_current=&parent;
  calls.clear();logs=0;allow_context=true;diagnostic_irq=true;diagnostic_preempt=0;
  assert(refusal_path(&parent,-EAGAIN,-EAGAIN,static_cast<uint32_t>(Point::LegacyMatch),reservation)==static_cast<uint64_t>(-EAGAIN));
  assert(logs==1 && diagnostic_owners==0 && !diagnostic_irq);
  assert((calls==std::vector<std::string>{"end","journal"}));
  assert(message.find("raw=-11 public=-11 parent=8589934599")!=std::string::npos);
  assert(message.find("if=1 preempt=0 current=1 type=1 state=1 transition=0 accounted=1")!=std::string::npos);
  assert(message.find(reservation?"stage=process-reservation":"stage=vm-snapshot")!=std::string::npos);
  assert(message.find(reservation?"point=4294967295 prep=2":"point=8 prep=1")!=std::string::npos);
  assert(message.ends_with(reservation?"context=reservation-return":"context=coordinator-return"));
 }
 // Raw/private status and the existing real translator's public result are distinct.
 {
  sched::task::Task parent; sched::diagnostic_current=&parent;
  allow_context=true;diagnostic_irq=false;diagnostic_preempt=7;logs=0;calls.clear();
  {
   ker::syscall::process::ForkRefusalDiagnostic diagnostic{parent.pid};
   DiagnosticOwner owner;
   auto mapped=public_fork_vm_error(Point::Copy,-EAGAIN);
   assert(mapped==-ENOMEM);
   diagnostic.capture(parent,"vm-snapshot",-EAGAIN,mapped,static_cast<uint32_t>(Point::Copy),0,"coordinator-return");
   assert(logs==0);allow_context=false;
  }
  assert(logs==1 && message.find("raw=-11 public=-12")!=std::string::npos && message.find("prep=0 if=0 preempt=7")!=std::string::npos);
 }
 // Successful/unarmed paths do not log or read context. Dropped journal work
 // cannot change the actual error return: logging is not an acceptance gate.
 {
  logs=0;allow_context=false;ker::syscall::process::ForkRefusalDiagnostic unarmed{1};
 }
 assert(logs==0);
 {
  sched::task::Task parent;sched::diagnostic_current=&parent;
  logs=0;allow_context=true;diagnostic_irq=true;diagnostic_preempt=0;
  {ker::syscall::process::ForkRefusalDiagnostic quiet{1};quiet.capture(parent,"success",0,0);}
  assert(logs==0);
  discard_refusal_log=true;
  assert(refusal_path(&parent,-EAGAIN,-EAGAIN,static_cast<uint32_t>(Point::Context))==static_cast<uint64_t>(-EAGAIN));
  discard_refusal_log=false;assert(logs==0 && diagnostic_owners==0);
 }
 // Maximum-width fields fit one existing journal message; no lookup at emit.
 {
  sched::task::Task parent;parent.pid=UINT64_MAX;sched::diagnostic_current=&parent;
  allow_context=true;diagnostic_preempt=UINT32_MAX;logs=0;
  {ker::syscall::process::ForkRefusalDiagnostic diagnostic{parent.pid};
   diagnostic.capture(parent,"process-reservation",INT32_MIN,INT32_MIN,UINT32_MAX,UINT32_MAX,"coordinator-return");
   allow_context=false;
  }
  assert(logs==1 && message.size()<512 && message.find("parent=18446744073709551615")!=std::string::npos);
 }
 allow_context=true;diagnostic_preempt=0;
}
void geometry_refusal_cases() {
 using namespace ker::mod; using namespace mm::huge;
 using Recorder=ker::syscall::process::ForkRefusalDiagnostic;
 using Point=ker::syscall::process::detail::ForkVmFailurePoint;
 // Actual recorder copies source-owned scalars before their lifetime ends.
 {
  sched::task::Task parent;sched::diagnostic_current=&parent;
  logs=0;messages.clear();calls.clear();allow_context=true;diagnostic_irq=true;diagnostic_preempt=0;
  {
   Recorder diagnostic{parent.pid};
   DiagnosticOwner owner;
   {
    RangePrepareWitness witness{};
    witness.next=0x4000;witness.count=2;witness.expected_count=3;
    witness.error=Error::Again;witness.site=RangePrepareSite::PublicationBusy;
    witness.pass=RangePreparePass::Count;witness.attempt=3;
    witness.valid=true;witness.epoch_changed=true;witness.exhausted=true;
    int const mapped=ker::syscall::process::detail::public_fork_vm_error(Point::GeometryPrepare,-EAGAIN);
    assert(mapped==-EAGAIN);
    diagnostic.capture(parent,"vm-snapshot",-EAGAIN,mapped,static_cast<uint32_t>(Point::GeometryPrepare),1,"coordinator-return",&witness);
    assert(logs==0);
    witness={}; // Neither later overwrite nor destruction can alter the copy.
   }
   parent.pid=0;parent.type=sched::task::TaskType::DAEMON;
   parent.state.store(sched::task::TaskState::WAITING);
   allow_context=false;
  }
  assert(logs==2 && diagnostic_owners==0 && !diagnostic_irq && messages.size()==2);
  assert((calls==std::vector<std::string>{"journal","journal"}));
  assert(messages[0]=="fork_refusal_v1 stage=vm-snapshot raw=-11 public=-11 parent=8589934599 point=6 prep=1 if=1 preempt=0 current=1 type=1 state=1 transition=0 accounted=1 context=coordinator-return");
  assert(messages[1]=="fork_geometry_refusal_v1 parent=8589934599 raw=-11 public=-11 point=6 error="+
   std::to_string(static_cast<unsigned>(Error::Again))+" site="+
   std::to_string(static_cast<unsigned>(RangePrepareSite::PublicationBusy))+" pass="+
   std::to_string(static_cast<unsigned>(RangePreparePass::Count))+
   " attempt=3 next=16384 count=2 expected=3 epoch_changed=1 exhausted=1");
 }
 // Null, invalid and non-geometry captures cannot emit a companion. A later
 // actual negative capture also clears an earlier copied geometry witness.
 for(unsigned mode:{0U,1U,2U,3U}) {
  sched::task::Task parent;sched::diagnostic_current=&parent;
  logs=0;messages.clear();allow_context=true;diagnostic_irq=true;diagnostic_preempt=0;
  {
   Recorder diagnostic{parent.pid};DiagnosticOwner owner;
   RangePrepareWitness witness{};witness.valid=true;witness.error=Error::Again;
   witness.site=RangePrepareSite::StripedReader;
   if(mode==3)diagnostic.capture(parent,"vm-snapshot",-EAGAIN,-EAGAIN,6,1,"coordinator-return",&witness);
   if(mode==1 || mode==3)witness.valid=false;
   auto const point=mode==2?Point::Copy:Point::GeometryPrepare;
   diagnostic.capture(parent,"vm-snapshot",-EAGAIN,-EAGAIN,static_cast<uint32_t>(point),1,"coordinator-return",mode==0?nullptr:&witness);
   assert(logs==0);allow_context=false;
  }
  assert(logs==1 && messages.size()==1 && messages[0].starts_with("fork_refusal_v1 ") && diagnostic_owners==0);
 }
 // Successful capture is unarmed even if a valid source witness was supplied.
 {
  sched::task::Task parent;logs=0;messages.clear();allow_context=false;
  Recorder diagnostic{parent.pid};RangePrepareWitness witness{};witness.valid=true;
  diagnostic.capture(parent,"success",0,0,6,1,"coordinator-return",&witness);
 }
 assert(logs==0 && messages.empty());
 // Maximum representable scalar fields fit separate existing512B records.
 // Geometry's point is6 by contract; the old case still covers UINT32_MAX point.
 {
  sched::task::Task parent;parent.pid=UINT64_MAX;sched::diagnostic_current=&parent;
  logs=0;messages.clear();allow_context=true;diagnostic_irq=true;diagnostic_preempt=UINT32_MAX;
  {
   Recorder diagnostic{parent.pid};DiagnosticOwner owner;
   RangePrepareWitness witness{};witness.next=UINT64_MAX;
   witness.count=UINT16_MAX;witness.expected_count=UINT16_MAX;
   witness.error=static_cast<Error>(UINT8_MAX);witness.site=static_cast<RangePrepareSite>(UINT8_MAX);
   witness.pass=static_cast<RangePreparePass>(UINT8_MAX);witness.attempt=UINT8_MAX;
   witness.valid=true;witness.epoch_changed=true;witness.exhausted=true;
   diagnostic.capture(parent,"vm-snapshot",INT32_MIN,INT32_MIN,6,UINT32_MAX,"coordinator-return",&witness);
   allow_context=false;
  }
  assert(logs==2 && messages.size()==2 && messages[0].size()<512 && messages[1].size()<512);
  assert(messages[0].starts_with("fork_refusal_v1 stage=vm-snapshot raw=-2147483648 public=-2147483648 parent=18446744073709551615"));
  assert(messages[1]=="fork_geometry_refusal_v1 parent=18446744073709551615 raw=-2147483648 public=-2147483648 point=6 error=255 site=255 pass=255 attempt=255 next=18446744073709551615 count=65535 expected=65535 epoch_changed=1 exhausted=1");
 }
 allow_context=true;diagnostic_preempt=0;
}
void copy_refusal_cases() {
 using namespace ker::mod;
 using Recorder=ker::syscall::process::ForkRefusalDiagnostic;
 using Point=ker::syscall::process::detail::ForkVmFailurePoint;
 using Site=mm::virt::ForkCopyRefusalSite;
 using ChildSite=mm::virt::UnpublishedRootRefusalSite;
 using Witness=mm::virt::ForkCopyRefusalWitness;
 // Copy actual scalar output while its caller-owned lifetime is still present.
 // Later overwrite/context loss must not change the deferred, owner-free record.
 for(bool child:{false,true}) {
  sched::task::Task parent;sched::diagnostic_current=&parent;
  logs=0;messages.clear();calls.clear();allow_context=true;diagnostic_irq=true;diagnostic_preempt=0;
  {
   Recorder diagnostic{parent.pid};DiagnosticOwner owner;
   {
    Witness witness{};witness.valid=true;witness.raw_status=-EINVAL;
    witness.site=child?Site::ChildRoot:Site::SplitCommit;
    witness.child.valid=child;witness.child.site=child?ChildSite::ReaderAdmissionRefused:ChildSite::None;
    witness.detail=child?0:static_cast<uint32_t>(mm::huge::Error::Invalid);
    diagnostic.capture(parent,"vm-snapshot",-EINVAL,-ENOMEM,static_cast<uint32_t>(Point::Copy),1,"coordinator-return",nullptr,&witness);
    witness={};assert(logs==0);
   }
   parent.pid=0;parent.type=sched::task::TaskType::DAEMON;allow_context=false;
  }
  assert(logs==2 && messages.size()==2 && diagnostic_owners==0 && !diagnostic_irq);
  assert((calls==std::vector<std::string>{"journal","journal"}));
  assert(messages[0]=="fork_refusal_v1 stage=vm-snapshot raw=-22 public=-12 parent=8589934599 point=11 prep=1 if=1 preempt=0 current=1 type=1 state=1 transition=0 accounted=1 context=coordinator-return");
  assert(messages[1]=="fork_copy_refusal_v1 parent=8589934599 raw=-22 public=-12 point=11 site="+
   std::to_string(static_cast<unsigned>(child?Site::ChildRoot:Site::SplitCommit))+" child_valid="+(child?"1":"0")+
   " child_site="+std::to_string(static_cast<unsigned>(child?ChildSite::ReaderAdmissionRefused:ChildSite::None))+
   " user_slot=65535 detail="+std::to_string(child?0:static_cast<uint32_t>(mm::huge::Error::Invalid)));
 }
 // Null, invalid, wrong-phase and mismatched-raw witnesses cannot emit. Every
 // later negative capture clears an earlier Copy witness before deciding anew.
 for(unsigned mode=0;mode!=6;++mode) {
  sched::task::Task parent;sched::diagnostic_current=&parent;
  logs=0;messages.clear();allow_context=true;diagnostic_irq=true;diagnostic_preempt=0;
  {
   Recorder diagnostic{parent.pid};DiagnosticOwner owner;
   Witness witness{};witness.valid=true;witness.raw_status=-EINVAL;witness.site=Site::ChildRoot;
   witness.child.valid=true;witness.child.site=ChildSite::UserEntryNonzero;witness.child.user_slot=17;
   if(mode>=4)diagnostic.capture(parent,"vm-snapshot",-EINVAL,-ENOMEM,11,1,"coordinator-return",nullptr,&witness);
   if(mode==1 || mode==4)witness.valid=false;
   if(mode==3)witness.raw_status=-EAGAIN;
   auto const point=mode==2 || mode==5?Point::LegacyMatch:Point::Copy;
   diagnostic.capture(parent,"vm-snapshot",-EINVAL,-ENOMEM,static_cast<uint32_t>(point),1,"coordinator-return",nullptr,mode==0?nullptr:&witness);
   assert(logs==0);allow_context=false;
  }
  assert(logs==1 && messages.size()==1 && messages[0].starts_with("fork_refusal_v1 "));
 }
 // No context query or log on an unarmed success, even with a valid witness.
 {
  sched::task::Task parent;logs=0;messages.clear();allow_context=false;
  Recorder diagnostic{parent.pid};Witness witness{};witness.valid=true;witness.raw_status=0;
  diagnostic.capture(parent,"success",0,0,11,1,"coordinator-return",nullptr,&witness);
 }
 assert(logs==0 && messages.empty());
 // Execute only the actual attempt-entry prefix through its earliest context
 // refusal. Controlled preparation/context ports are not the full coordinator.
 {
  sched::task::Task parent;Witness witness{};witness.valid=true;witness.site=Site::CowWalk;
  witness.raw_status=-EINVAL;witness.child={true,ChildSite::UserEntryNonzero,17};witness.detail=123;
  mm::huge::RangePrepareWitness geometry{};geometry.valid=true;Point point=Point::Copy;
  allow_context=false;
  auto source_admission=mm::virt::ForkSourceAdmission::Busy;
  assert(ker::syscall::process::detail::actual_attempt_entry(parent,false,&point,&geometry,&witness,&source_admission)==-EWOULDBLOCK);
  assert(source_admission==mm::virt::ForkSourceAdmission::NotAttempted);
  assert(!witness.valid && witness.site==Site::None && witness.raw_status==0 && !witness.child.valid &&
   witness.child.site==ChildSite::None && witness.child.user_slot==UINT16_MAX && witness.detail==0);
  assert(!geometry.valid && point==Point::Context);
 }
 // Every representable field fits its own existing512B journal message.
 {
  sched::task::Task parent;parent.pid=UINT64_MAX;sched::diagnostic_current=&parent;
  logs=0;messages.clear();allow_context=true;diagnostic_irq=true;diagnostic_preempt=UINT32_MAX;
  {
   Recorder diagnostic{parent.pid};DiagnosticOwner owner;Witness witness{};
   witness.valid=true;witness.raw_status=INT32_MIN;witness.site=static_cast<Site>(UINT8_MAX);
   witness.child={true,static_cast<ChildSite>(UINT8_MAX),UINT16_MAX};witness.detail=UINT32_MAX;
   diagnostic.capture(parent,"vm-snapshot",INT32_MIN,INT32_MIN,11,UINT32_MAX,"coordinator-return",nullptr,&witness);
   allow_context=false;
  }
  assert(logs==2 && messages.size()==2 && messages[0].size()<512 && messages[1].size()<512);
  assert(messages[1]=="fork_copy_refusal_v1 parent=18446744073709551615 raw=-2147483648 public=-2147483648 point=11 site=255 child_valid=1 child_site=255 user_slot=65535 detail=4294967295");
 }
 allow_context=true;diagnostic_preempt=0;
}
int main() {
 using namespace ker::mod; using ker::syscall::process::finish;
 auto& pool=mm::phys::kernel_stack_pool; sched::task::Task parent;
 // Getter copies one coherent metadata tuple without allocation or logger work.
 for (uint64_t free : {0ULL, 1ULL, 256ULL}) {
  pool.capacity=256; pool.free_count=free; pool.high_watermark=256-free;
  calls.clear(); auto s=mm::phys::get_kernel_stack_pool_stats();
  assert(s.capacity==256 && s.free_slots==free && s.high_watermark==256-free);
  assert((calls==std::vector<std::string>{"pool-lock","pool-unlock"}) && !held && logs==0);
 }
 // Successful and unlabelled/other failures preserve errno/bytes and do no observation.
 for (int64_t result : {0LL, 0x200000007LL, -ENOMEM*1LL, -EAGAIN*1LL}) {
  calls.clear(); auto before=lock_count;
  const char* label=result==-ENOMEM ? nullptr : "kernel-stack";
  assert(finish(&parent,result,998877,label)==static_cast<uint64_t>(result));
  assert(recorded_bytes==998877 && recorded_status==(result<0?result:0));
  assert((calls==std::vector<std::string>{"end"}) && lock_count==before && logs==0);
 }
 for (uint64_t cpu=0;cpu<4;++cpu) sched::queues[cpu]={cpu+1,2*cpu+1,3*cpu+1};
 pool.capacity=256; pool.free_count=0; pool.high_watermark=256;
 const char* stages[]={STAGE_STRINGS};
 for (const char* stage : stages) {
  calls.clear(); auto before=logs;
  assert(finish(&parent,-ENOMEM,42,stage)==static_cast<uint64_t>(-ENOMEM));
  assert(recorded_bytes==42 && recorded_status==-ENOMEM && logs==before+1);
  assert((calls==std::vector<std::string>{"end","pool-lock","pool-unlock","physical","cores",
   "rq0","rq1","rq2","rq3","deferred","journal"}));
  assert(message.find(std::string("stage=")+stage+" status=-12 parent=8589934599")!=std::string::npos);
  assert(message.find("stack_cap=256 stack_free=0 stack_high=256")!=std::string::npos);
  assert(message.find("phys_free_pages=123456 dead=10 runnable=16 waiting=22 gc_deferred=9")!=std::string::npos);
 }
 // The actual scalar translator feeds the actual dynamic VM diagnostic route.
 using Point=ker::syscall::process::detail::ForkVmFailurePoint;
 struct DynamicCase {Point point;int status;int expected;};
 DynamicCase const dynamic[]={
  {Point::Copy,-EAGAIN,-ENOMEM},{Point::LazyPrepare,-EBUSY,-EBUSY},
  {Point::GeometryPrepare,-EOPNOTSUPP,-EOPNOTSUPP},{Point::Complete,0,0},
  {Point::LegacyMatch,-EFAULT,-EINVAL},{static_cast<Point>(255),-ENOMEM,-EINVAL}};
 for(auto const& item:dynamic){
  calls.clear();auto before=logs;
  auto mapped=ker::syscall::process::detail::public_fork_vm_error(item.point,item.status);
  assert(mapped==item.expected);
  assert(finish(&parent,mapped,42,"vm-snapshot")==static_cast<uint64_t>(item.expected));
  assert(recorded_bytes==42 && recorded_status==item.expected);
  assert(logs==before+(mapped==-ENOMEM?1U:0U));
  if(mapped==-ENOMEM)assert(message.find("stage=vm-snapshot status=-12")!=std::string::npos);
  else assert((calls==std::vector<std::string>{"end"}));
 }
 // A zero-core snapshot skips queue access; no GC request/reclaim is a dependency.
 calls.clear(); smt::cores=0; finish(&parent,-ENOMEM,0,"task");
 assert((calls==std::vector<std::string>{"end","pool-lock","pool-unlock","physical","cores","journal"}));
 assert(message.find("dead=0 runnable=0 waiting=0 gc_deferred=0")!=std::string::npos);
 // Maximum-width values and the longest stage fit the actual journal message bound.
 smt::cores=1; auto max=std::numeric_limits<uint64_t>::max(); parent.pid=max;
 cpu::id=std::numeric_limits<uint32_t>::max(); pool.capacity=max; pool.free_count=max;
 pool.high_watermark=max; mm::phys::free_pages=max; sched::queues[0]={max,max,max}; sched::deferred=max;
 finish(&parent,-ENOMEM,0,"scheduler-publication");
 assert(message.size()<512 && message.ends_with("gc_deferred=18446744073709551615"));
 refusal_cases();
 geometry_refusal_cases();
 copy_refusal_cases();
 puts("PASS actual fork failure getter/helper/finish/Copy recorder and attempt-entry reset: 13 stages + 6 dynamic VM cases, errno/bytes, deferred scalar fidelity, success silence, lock ordering, CPU bounds, 512-byte journal bound");
}
'''


def validate_dynamic_vm_route(fork):
    """Keep the one explicit dynamic status alongside the exact literal labels."""
    exact = re.sub(r"\s+", "", fork)
    call = "detail::prepare_fork_vm_snapshot(*parent,*child,preparation,EAGER_COPY?&fork_placement:nullptr,EAGER_COPY?&fork_page:nullptr,&point,&geometry_failure,&copy_failure,&private_child_authority)"
    result = 'returnfinish_fork(detail::public_fork_vm_error(point,FORK_VM_ERROR),0,"vm-snapshot");'
    assert exact.count("detail::ForkVmFailurePointpoint{};") == 1
    assert exact.count("mm::huge::RangePrepareWitnessgeometry_failure{};") == 1
    assert exact.count("mm::virt::ForkCopyRefusalWitnesscopy_failure{};") == 1
    assert exact.count('refusal.capture(*parent,"vm-snapshot",FORK_VM_ERROR,detail::public_fork_vm_error(point,FORK_VM_ERROR),static_cast<uint32_t>(point),preparation.ready()?1U:0U,"coordinator-return",&geometry_failure,&copy_failure);') == 1
    assert exact.count(call) == 1
    assert exact.count(result) == 1
    cursor = 0
    for token in ("sys::SyscallPreparationScopepreparation(*parent);", "detail::ForkVmFailurePointpoint{};",
                  "mm::huge::RangePrepareWitnessgeometry_failure{};",
                  "mm::virt::ForkCopyRefusalWitnesscopy_failure{};",
                  "intconstFORK_VM_ERROR=" + call, "private_child_authority.invalidate();", "if(FORK_VM_ERROR!=0)",
                  "shm_cleanup_for_task(child)", "release_file_mmap_ranges_for_pagemap(child->pagemap)",
                  "release_lazy_vmem_ranges(*child)", "destroy_user_space(child->pagemap", "deletechild;", result):
        position = exact.find(token, cursor)
        assert position >= 0, token
        cursor = position + len(token)


def main():
    process = PROCESS_SOURCE.read_text()
    fork = function(process, 'wos_proc_fork')
    labels = re.findall(r'finish_fork\(-ENOMEM, 0, "([^"]+)"\)', fork)
    literal_stages = tuple(stage for stage in STAGES if stage != 'vm-snapshot')
    assert tuple(labels) == literal_stages
    assert fork.count('finish_fork(-ENOMEM') == len(literal_stages)
    validate_dynamic_vm_route(fork)
    snapshot = block((ROOT / 'modules/kern/src/platform/mm/phys.hpp').read_text(),
                     r'^struct KernelStackPoolStatsSnapshot \{') + ';'
    getter = function((ROOT / 'modules/kern/src/platform/mm/phys.opt.cpp').read_text(),
                      'get_kernel_stack_pool_stats')
    source = PRELUDE.replace('SNAPSHOT', snapshot).replace('GETTER', getter)
    managed = MANAGED_HUGE_SOURCE.read_text()
    core = (ROOT / 'modules/kern/src/platform/mm/managed_huge_core.hpp').read_text()
    geometry_types = block(core, r'^enum class Error :') + ';\n'
    for name in ('RangePrepareSite', 'RangePreparePass'):
        geometry_types += block(managed, r'^enum class ' + name + ' :') + ';\n'
    geometry_types += block(managed, r'^struct RangePrepareWitness \{') + ';\n'
    bound = re.search(r'^static_assert\(sizeof\(RangePrepareWitness\).*;$', managed, re.M)
    assert bound is not None
    geometry_types += bound.group(0)
    source = source.replace('GEOMETRY_TYPES', geometry_types)
    virt = VIRT_SOURCE.read_text()
    copy_types = ''
    for name in ('UnpublishedRootRefusalSite', 'ForkCopyRefusalSite', 'ForkSourceAdmission'):
        copy_types += block(virt, r'^enum class ' + name + ' :') + ';\n'
    for name in ('UnpublishedRootRefusal', 'ForkCopyRefusalWitness'):
        copy_types += block(virt, r'^struct ' + name + r' \{') + ';\n'
    # Keep the actual public POD bounds in the compiled extraction.
    copy_bounds = [line for line in virt.splitlines() if line.startswith('static_assert(') and
                   any(name in line for name in ('UnpublishedRootRefusal', 'ForkCopyRefusalWitness'))]
    assert len(copy_bounds) == 3
    source = source.replace('COPY_TYPES', copy_types + '\n'.join(copy_bounds))
    translator = block(process, r'^enum class ForkVmFailurePoint :') + ';\n' + function(process, 'public_fork_vm_error')
    point_bound = 'static_assert(static_cast<uint32_t>(ForkVmFailurePoint::Copy) == 11);'
    assert process.count(point_bound) == 1
    translator += '\n' + point_bound
    source = source.replace('TRANSLATOR', translator)
    once = function(process, 'prepare_fork_vm_snapshot_once')
    attempt_prefix = once[once.index('{') + 1:once.index('    // Every fallible owner')]
    reset_copy = 'if (copy_failure != nullptr) *copy_failure = {};'
    assert attempt_prefix.count(reset_copy) == 1
    once_exact = re.sub(r'\s+', '', once)
    assert once_exact.count('MatchConflict*conflict=nullptr,ker::mod::mm::virt::ForkCopyRefusalWitness*copy_failure=nullptr,ker::mod::mm::virt::ForkPrivateChildRoot*private_child_authority=nullptr,ker::mod::mm::virt::ForkSourceAdmission*source_admission=nullptr)') == 1
    assert once_exact.count('eager_context,empty_result,swap,copy_failure);') == 2
    assert 'if(private_child_authority!=nullptr)' in once_exact
    assert 'actor,child,*private_child_authority,source_owner,preparation' in once_exact
    wrapper = re.sub(r'\s+', '', function(process, 'prepare_fork_vm_snapshot'))
    assert wrapper.count('prepare_fork_vm_snapshot_once(actor,child,preparation,eager_context,empty_result,&point,geometry_failure,&conflict,copy_failure,private_child_authority,&source_admission)') == 1
    source = source.replace('ACTUAL_ATTEMPT_ENTRY_PREFIX', attempt_prefix)
    recorder = block(process, r'^struct ForkRefusalDiagnostic \{') + ';'
    source = source.replace('REFUSAL_DIAGNOSTIC', recorder)
    source = source.replace('DIAGNOSTIC', function(process, 'note_fork_failure'))
    source = source.replace('LAMBDA', block(fork, r'^    auto finish_fork ='))
    declaration = re.search(r'^    ForkRefusalDiagnostic refusal\{parent->pid\};$', fork, re.M)
    assert declaration is not None
    source += CASES.replace('STAGE_STRINGS', ','.join('"' + s + '"' for s in STAGES)).replace(
        'ACTUAL_REFUSAL_DECLARATION', declaration.group(0).strip())
    with tempfile.TemporaryDirectory(prefix='wos-fork-failure-') as directory:
        temp = Path(directory)
        capture_body = block(recorder, r'^    void capture\(')
        destructor = block(recorder, r'^    ~ForkRefusalDiagnostic\(')
        early = capture_body[:-1] + destructor[destructor.index('{') + 1:-1] + '}'
        geometry_emit = block(destructor, r'^        if \(geometry.valid\)')
        early_geometry = capture_body[:-1] + geometry_emit + '}'
        copy_emit = block(destructor, r'^        if \(copy.valid\)')
        early_copy = capture_body[:-1] + copy_emit + '}'
        copy_site = 'static_cast<unsigned>(copy.site)'
        copy_phase = 'vm_point == 11'
        copy_status = ' && copy_failure->raw_status == raw'
        site = 'static_cast<unsigned>(geometry.site)'
        assert source.count(site) == 1 and source.count(geometry_emit) == 1
        assert source.count(copy_emit) == source.count(copy_site) == source.count(copy_phase) == source.count(copy_status) == source.count(reset_copy) == 1
        variants = {'actual': source,
                    'suppress-refusal': source.replace(destructor, destructor[:destructor.index('{')] + '{}', 1),
                    'early-refusal': source.replace(capture_body, early, 1),
                    'suppress-geometry': source.replace(geometry_emit, '', 1),
                    'mislabel-geometry': source.replace(site, 'static_cast<unsigned>(ker::mod::mm::huge::RangePrepareSite::Unreported)', 1),
                    'early-geometry': source.replace(capture_body, early_geometry, 1),
                    'suppress-copy': source.replace(copy_emit, '', 1),
                    'mislabel-copy': source.replace(copy_site, 'static_cast<unsigned>(ker::mod::mm::virt::ForkCopyRefusalSite::None)', 1),
                    'early-copy': source.replace(capture_body, early_copy, 1),
                    'capture-noncopy': source.replace(copy_phase, 'true', 1),
                    'accept-stale-copy-status': source.replace(copy_status, '', 1),
                    'retain-prior-attempt-copy': source.replace(reset_copy, '(void)copy_failure;', 1)}
        for label, content in variants.items():
            if label != 'actual': assert content != source
            cpp, binary = temp / (label + '.cpp'), temp / label
            cpp.write_text(content)
            subprocess.run([shutil.which('c++', path=os.defpath), '-std=c++23', '-O1', '-g',
                            '-Wall', '-Wextra', '-Werror', '-fsanitize=address,undefined',
                            str(cpp), '-o', str(binary)], check=True)
            result = subprocess.run([str(binary)], capture_output=True, text=True)
            if label == 'actual':
                assert result.returncode == 0, result.stdout + result.stderr
            else:
                assert result.returncode != 0 and 'Assertion' in result.stderr, label + ': genuine behavior control'


if __name__ == '__main__':
    main()
