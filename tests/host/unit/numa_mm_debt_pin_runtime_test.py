#!/usr/bin/env python3
"""Execute live MM access/pin/Task/StableUserPage bodies with owned Host endpoints.

Physical refs, PTE snapshots, index binding, Task state and IRQ lock endpoints
are controlled. Counter engine, acquire/release/rollback and usercopy owner
bodies are extracted unchanged. No fake current Task/GS or kernel runtime claim.
"""
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
VIRT=ROOT/'modules/kern/src/platform/mm/virt.opt.cpp'
TASK=ROOT/'modules/kern/src/platform/sched/task.cpp'
COPY=ROOT/'modules/kern/src/platform/sys/usercopy.cpp'
VH=ROOT/'modules/kern/src/platform/mm/virt.hpp'
CH=ROOT/'modules/kern/src/platform/sys/usercopy.hpp'
def extract(text,signature,declaration=False):
 start=text.index(signature);brace=text.index('{',start);depth=1;i=brace+1
 while depth:
  if text[i]=='{':depth+=1
  if text[i]=='}':depth-=1
  i+=1
 return text[start:i]+(';' if declaration else '')
def program(v,t,c,vh,ch):
 core=v[v.index('#ifdef WOS_SELFTEST\nnamespace ker::test::numa_lifecycle::mm_debt {'):]
 setup=r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <utility>
#include <platform/mm/lifecycle_census.hpp>
#include <test/numa_lifecycle_mm_debt.hpp>
'''+core+r'''
namespace nl=ker::test::numa_lifecycle;
namespace debt=nl::mm_debt;
using DebtField=nl::CounterField;
static size_t checks=0;
#define CHECK(x) do { ++checks; if(!(x)){std::fprintf(stderr,"line %d: %s\n",__LINE__,#x);std::abort();} } while(false)
nl::MmDebtSnapshot snap(){nl::MmDebtSnapshot x{};debt::snapshot(x);return x;}
struct HookCount {
 std::atomic<uint32_t> value{0}; std::atomic<bool>* close_after_cas{}; bool require_stable_return{};
 auto load(std::memory_order o)const{return value.load(o);}
 void store(uint32_t x,std::memory_order o){value.store(x,o);}
 auto fetch_sub(uint32_t x,std::memory_order o){if(require_stable_return){auto debt=snap();CHECK(debt.stable_handles>0&&debt.pin_returns>0);}return value.fetch_sub(x,o);}
 bool compare_exchange_weak(uint32_t& expected,uint32_t next,std::memory_order a,std::memory_order b){bool ok=value.compare_exchange_weak(expected,next,a,b);if(ok&&close_after_cas){close_after_cas->store(true);close_after_cas=nullptr;}return ok;}
};
namespace ker::mod::mm {
namespace phys {
struct PageLookupHint{uint64_t value{};};
struct alignas(4096) Frame{std::array<unsigned char,4096> bytes{};unsigned refs{1};bool refuse{};unsigned returns{};};
Frame frame;
bool check_return_tail{};
auto page_ref_try_inc(void* p,PageLookupHint*)->bool{CHECK(p==frame.bytes.data());CHECK(snap().inflight>0);if(frame.refuse)return false;++frame.refs;return true;}
void page_ref_dec(void* p,PageLookupHint*){CHECK(p==frame.bytes.data()&&frame.refs>0);auto x=snap();CHECK(x.inflight>0||x.pin_returns>0);if(check_return_tail)CHECK(x.user_page_pins>0&&x.pin_returns>0);--frame.refs;++frame.returns;}
}
namespace file_mapping {
enum class BindingOwner{PIN};enum class FileMappingPinResult{ACQUIRED,REFUSE};
struct FileMappingBinding{unsigned refs{};bool expect_full_tail{};void release(BindingOwner){auto x=snap();CHECK(refs>0);if(expect_full_tail)CHECK(x.user_page_pins>0&&x.pin_returns>0&&x.access_readers==0);--refs;}};
}
namespace addr{auto get_virt_pointer(uint64_t p){return p;}}
namespace virt {
using vaddr_t=uint64_t;using paddr_t=uint64_t;constexpr uint64_t PADDR_INVALID=UINT64_MAX;
struct alignas(4096) PageTable{std::array<unsigned char,4096> bytes{};bool present{true},writable{true};};
constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT=4,USER_PAGEMAP_ACCESS_GATE_NONE=static_cast<size_t>(-1);
struct alignas(64) UserPagemapAccessGate{HookCount readers;std::atomic<bool> teardown{false};};
std::array<UserPagemapAccessGate,USER_PAGEMAP_ACCESS_GATE_COUNT> user_pagemap_access_gates{};
struct UserRootControl{uint64_t serial{37};};UserRootControl control;
ker::mod::sys::Spinlock user_mapping_pin_lock;
file_mapping::FileMappingBinding binding;
bool refuse_binding{},change_second_snapshot{},second_snapshot{};unsigned snapshots{};
struct Index{auto retain_for_pin_locked(PageTable*,uint64_t,uint64_t,file_mapping::FileMappingBinding*&out){CHECK(snap().inflight>0);if(refuse_binding){out=nullptr;return file_mapping::FileMappingPinResult::REFUSE;}++binding.refs;out=&binding;return file_mapping::FileMappingPinResult::ACQUIRED;}} file_mapping_index;
auto find_user_root_control_locked(PageTable*){return &control;}
auto page_align_down(uint64_t p){return p&~uint64_t{4095};}
bool snapshot_user_page_mapping_locked(PageTable* r,uint64_t,bool w,uint64_t& pa,bool* dirty=nullptr){++snapshots;pa=reinterpret_cast<uint64_t>(phys::frame.bytes.data());if(dirty)*dirty=true;if(change_second_snapshot&&snapshots%2==0)return false;return r&&r->present&&(!w||r->writable);}
void service_user_pagemap_gate_wait(uint64_t& n){++n;std::this_thread::yield();}
'''+extract(vh,'struct UserPagePin {',True)+extract(vh,'struct UserFutexIdentity {',True)+r'''
}
}
namespace ker::mod::mm::paging {using PageTable=virt::PageTable;}
namespace ker::mod::sched::task {
enum class TaskState{ACTIVE,EXITING,DEAD};
struct Task {
 std::atomic<TaskState> state{TaskState::ACTIVE};std::atomic<bool> usercopy_pagemap_closing{false};HookCount usercopy_pagemap_accesses;
 mm::virt::PageTable* pagemap{};
 auto try_acquire_usercopy_pagemap(mm::virt::PageTable*&) ->bool;
 void release_usercopy_pagemap();
};
'''+extract(t,'auto Task::try_acquire_usercopy_pagemap')+'\n'+extract(t,'void Task::release_usercopy_pagemap')+r'''
}
namespace ker::mod::mm::virt {
'''
 access=[extract(v,x) for x in ['[[nodiscard]] auto user_pagemap_access_gate_index','[[nodiscard]] auto acquire_user_pagemap_access','void release_user_pagemap_access','auto census_try_exclusive_flag','[[nodiscard]] auto begin_user_pagemap_exclusive','[[nodiscard]] auto try_begin_user_pagemap_exclusive','void end_user_pagemap_exclusive','auto try_pin_current_user_page_mapping','void unpin_user_page']]
 setup+='\n'.join(access)+r'''
enum class PublicReadPrepareResult:uint8_t{NOT_TRACKED,READY,REFUSED};
bool refuse_prepare{},refuse_pin{};
auto prepare_tmpfs_read_shared_page(sched::task::Task&,uint64_t,bool){return refuse_prepare?PublicReadPrepareResult::REFUSED:PublicReadPrepareResult::NOT_TRACKED;}
auto pin_user_page_for_task(sched::task::Task*,PageTable*root,uint64_t va,bool writable,bool,UserPagePin&out)->bool{if(refuse_pin)return false;size_t gate=USER_PAGEMAP_ACCESS_GATE_NONE;if(!acquire_user_pagemap_access(root,false,gate))return false;if(try_pin_current_user_page_mapping(root,va,writable,gate,out))return true;release_user_pagemap_access(gate);return false;}
bool user_page_pin_still_mapped(const UserPagePin&p){return p.hhdm_page!=nullptr;}
bool user_page_pin_commit_write(const UserPagePin&p){return p.hhdm_page!=nullptr;}
bool capture_user_futex_identity(const UserPagePin&,uint64_t,UserFutexIdentity&){return false;}
bool user_futex_identity_still_mapped(const UserPagePin&,const UserFutexIdentity&){return false;}
}
namespace ker::mod::sys::usercopy {
constexpr uint64_t USER_ADDR_LIMIT=0x0000800000000000ULL;
'''+extract(ch,'class UserPageFrameRef {',True)+extract(ch,'class StableUserPage {',True)+'\n'
 owners=['UserPageFrameRef::~UserPageFrameRef()','UserPageFrameRef::UserPageFrameRef(UserPageFrameRef&&','auto UserPageFrameRef::operator=','auto UserPageFrameRef::valid()','void UserPageFrameRef::reset()','StableUserPage::~StableUserPage()','auto StableUserPage::valid()','auto StableUserPage::retain_frame','void StableUserPage::reset()','auto range_valid(','auto pin_task_user_page(']
 # Constructor's first braces are a member initializer, so preserve the exact complete known definition.
 for a in owners:
  if a=='UserPageFrameRef::UserPageFrameRef(UserPageFrameRef&&':
   x=c[c.index(a):c.index('\nauto UserPageFrameRef::operator=',c.index(a))]
  else:x=extract(c,a)
  if a=='auto pin_task_user_page(':setup+='bool range_valid(uint64_t,size_t);\n'
  setup+=x+'\n'
 setup+=r'''
}
namespace fixture {
namespace mm=ker::mod::mm;namespace virt=mm::virt;namespace phys=mm::phys;namespace task=ker::mod::sched::task;namespace uc=ker::mod::sys::usercopy;
void clean(){auto x=snap();CHECK(x.valid&&x.inflight==0&&x.access_readers==0&&x.access_exclusives==0&&x.user_page_pins==0&&x.stable_handles==0&&x.retained_frame_refs==0&&x.task_usercopy_accesses==0&&x.pin_returns==0);CHECK(phys::frame.refs==1&&virt::binding.refs==0);}
void tests(){
 virt::PageTable root;auto index=virt::user_pagemap_access_gate_index(&root);auto& gate=virt::user_pagemap_access_gates.at(index);size_t out=0;
 CHECK(!virt::acquire_user_pagemap_access(nullptr,false,out)&&out==virt::USER_PAGEMAP_ACCESS_GATE_NONE);clean();
 CHECK(virt::acquire_user_pagemap_access(&root,false,out));CHECK(out==index&&snap().access_readers==1);CHECK(virt::try_begin_user_pagemap_exclusive(&root)==virt::USER_PAGEMAP_ACCESS_GATE_NONE);CHECK(!gate.teardown.load()&&snap().access_exclusives==0&&snap().access_readers==1);virt::release_user_pagemap_access(out);virt::release_user_pagemap_access(virt::USER_PAGEMAP_ACCESS_GATE_NONE);clean();
 gate.readers.store(UINT32_MAX,std::memory_order_release);CHECK(!virt::acquire_user_pagemap_access(&root,false,out));CHECK(snap().valid&&snap().inflight==0&&snap().access_readers==0);gate.readers.store(0,std::memory_order_release);clean();
 gate.readers.close_after_cas=&gate.teardown;CHECK(!virt::acquire_user_pagemap_access(&root,false,out));CHECK(gate.readers.load(std::memory_order_acquire)==0&&snap().inflight==0&&snap().access_readers==0);gate.teardown.store(false);clean();
 auto exclusive=virt::begin_user_pagemap_exclusive(&root);CHECK(exclusive==index&&snap().access_exclusives==1);CHECK(!virt::acquire_user_pagemap_access(&root,false,out));CHECK(virt::try_begin_user_pagemap_exclusive(&root)==virt::USER_PAGEMAP_ACCESS_GATE_NONE);CHECK(snap().access_exclusives==1);virt::end_user_pagemap_exclusive(exclusive);clean();
 exclusive=virt::try_begin_user_pagemap_exclusive(&root);CHECK(exclusive==index&&snap().access_exclusives==1);virt::end_user_pagemap_exclusive(exclusive);clean();
 task::Task actor;actor.pagemap=&root;mm::virt::PageTable* observed=nullptr;CHECK(actor.try_acquire_usercopy_pagemap(observed)&&observed==&root);CHECK(snap().task_usercopy_accesses==1);actor.release_usercopy_pagemap();clean();
 actor.state.store(task::TaskState::EXITING);CHECK(!actor.try_acquire_usercopy_pagemap(observed)&&observed==nullptr);actor.state.store(task::TaskState::ACTIVE);actor.usercopy_pagemap_accesses.store(UINT32_MAX,std::memory_order_release);CHECK(!actor.try_acquire_usercopy_pagemap(observed));CHECK(snap().inflight==0&&snap().task_usercopy_accesses==0);actor.usercopy_pagemap_accesses.store(0,std::memory_order_release);clean();
 actor.usercopy_pagemap_accesses.close_after_cas=&actor.usercopy_pagemap_closing;CHECK(!actor.try_acquire_usercopy_pagemap(observed));CHECK(actor.usercopy_pagemap_accesses.load(std::memory_order_acquire)==0&&observed==nullptr);actor.usercopy_pagemap_closing.store(false);clean();
 for(unsigned branch=0;branch<4;++branch){virt::UserPagePin pin{};CHECK(virt::acquire_user_pagemap_access(&root,false,out));virt::snapshots=0;phys::frame.refuse=branch==0;virt::change_second_snapshot=branch==1;virt::refuse_binding=branch==2;bool got=virt::try_pin_current_user_page_mapping(&root,0x4013,true,out,pin);CHECK(got==(branch==3));if(got){CHECK(pin.hhdm_page==phys::frame.bytes.data()&&pin.root_incarnation==37&&snap().user_page_pins==1&&virt::binding.refs==1);phys::check_return_tail=true;virt::binding.expect_full_tail=true;virt::unpin_user_page(pin);phys::check_return_tail=false;virt::binding.expect_full_tail=false;CHECK(pin.hhdm_page==nullptr);}else{CHECK(phys::frame.refs==1&&snap().inflight==0&&snap().user_page_pins==0);virt::release_user_pagemap_access(out);}virt::unpin_user_page(pin);phys::frame.refuse=false;virt::change_second_snapshot=false;virt::refuse_binding=false;clean();}
 actor.usercopy_pagemap_accesses.require_stable_return=true;
 {uc::StableUserPage pin;virt::refuse_prepare=true;CHECK(!uc::pin_task_user_page(actor,0x4013,true,true,pin));virt::refuse_prepare=false;clean();virt::refuse_pin=true;CHECK(!uc::pin_task_user_page(actor,0x4013,true,true,pin));virt::refuse_pin=false;CHECK(!pin.valid());clean();CHECK(!uc::pin_task_user_page(actor,uc::USER_ADDR_LIMIT,true,false,pin));clean();}
 {uc::UserPageFrameRef retained;{uc::StableUserPage pin;CHECK(uc::pin_task_user_page(actor,0x4013,true,false,pin));CHECK(snap().stable_handles==1&&snap().user_page_pins==1&&snap().task_usercopy_accesses==1);phys::frame.refuse=true;CHECK(!pin.retain_frame(retained));phys::frame.refuse=false;CHECK(snap().retained_frame_refs==0&&snap().inflight==0);CHECK(pin.retain_frame(retained));CHECK(!pin.retain_frame(retained));CHECK(snap().retained_frame_refs==1&&phys::frame.refs==3);uc::UserPageFrameRef moved(std::move(retained));CHECK(!retained.valid()&&moved.valid()&&snap().retained_frame_refs==1);retained=std::move(moved);CHECK(retained.valid()&&!moved.valid()&&snap().retained_frame_refs==1);phys::check_return_tail=true;virt::binding.expect_full_tail=true;}phys::check_return_tail=false;virt::binding.expect_full_tail=false;CHECK(snap().retained_frame_refs==1&&snap().stable_handles==0&&snap().user_page_pins==0&&snap().task_usercopy_accesses==0&&phys::frame.refs==2);retained.reset();retained.reset();clean();}
 {uc::StableUserPage pin;uc::UserPageFrameRef a,b;CHECK(uc::pin_task_user_page(actor,0x4013,true,false,pin));CHECK(pin.retain_frame(a)&&pin.retain_frame(b));CHECK(snap().retained_frame_refs==2);a=std::move(b);CHECK(a.valid()&&!b.valid()&&snap().retained_frame_refs==1);a=std::move(a);CHECK(a.valid()&&snap().retained_frame_refs==1);}
 clean();
}
}
int main(){fixture::tests();std::printf("PASS actual MM access/pin/Task/usercopy owner bodies; checks=%zu; no kernel/IRQ/SMP credit\n",checks);}
'''
 return setup
def main():
 compiler=os.environ.get('HOST_CXX') or shutil.which('g++',path=os.defpath) or shutil.which('clang++',path=os.defpath)
 if compiler is None:raise SystemExit('no Host C++ compiler')
 paths=[VIRT,TASK,COPY,VH,CH];before=[p.read_bytes() for p in paths]
 code=program(*(b.decode() for b in before))
 environment={**os.environ,'ASAN_OPTIONS':'detect_leaks=0:halt_on_error=1','UBSAN_OPTIONS':'halt_on_error=1:print_stacktrace=1'}
 with tempfile.TemporaryDirectory(prefix='wos-numa-mm-debt-pin-') as scratch:
  scratch=Path(scratch);cpp=scratch/'actual.cpp';binary=scratch/'actual';cpp.write_text(code)
  commands=[([compiler,'-std=c++23','-DWOS_SELFTEST=1','-DWOS_HOST_TEST=1','-I'+str(ROOT/'tests/host/kern_shim'),'-I'+str(ROOT/'modules/kern/src'),'-I'+str(ROOT/'modules'),'-O1','-g','-fno-pie','-no-pie','-fsanitize=address,undefined','-fno-omit-frame-pointer','-pthread',str(cpp),'-o',str(binary)],60),([str(binary)],20)]
  for argv,timeout in commands:
   p=subprocess.run(argv,capture_output=True,text=True,timeout=timeout,env=environment)
   if p.stdout:print(p.stdout,end='')
   if p.stderr:print(p.stderr,end='')
   if p.returncode:raise SystemExit(p.returncode)
 assert before==[p.read_bytes() for p in paths], 'live source changed'
if __name__=='__main__':main()
