#!/usr/bin/env python3
"""Execute live private MM-debt core and swap ownership bodies on supplied-owned Host seams.

This tests scalar arithmetic, descriptor/list/queue/waiter ownership and return
ordering. IRQ masking, Task release, physical IO and scheduling are controlled
endpoints; this is not kernel/SMP/real swap qualification.
"""
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'modules/kern/src/platform/mm/virt.opt.cpp'
def extract(text,signature):
 start=text.index(signature);brace=text.index('{',start);depth=1;i=brace+1
 while depth:
  if text[i]=='{':depth+=1
  if text[i]=='}':depth-=1
  i+=1
 return text[start:i]
def program():
 source=SOURCE.read_text()
 core=source[source.index('#ifdef WOS_SELFTEST\nnamespace ker::test::numa_lifecycle::mm_debt {'):]
 sigs=['template <typename T>\nauto allocate_swap_descriptor','template <typename T>\nvoid delete_swap_descriptor','void insert_anonymous_swap_mapping_locked','void remove_anonymous_swap_mapping_locked','auto anonymous_pagein_enqueue','auto anonymous_pagein_dequeue','void wake_anonymous_swap_waiters']
 actual=[extract(source,x) for x in sigs]
 probe=extract(source,'auto probe_anonymous_swap_fault_locked')
 waiter_fragment=probe[probe.index('#ifdef WOS_SELFTEST\n    debt::begin_change();'):].removesuffix('}')
 actual.append('enum class AnonymousSwapFaultDisposition {FAILED,PARK};\nauto register_waiter(AnonymousSwapMapping* mapping,sched::task::Task* task,uint64_t& wait_generation) -> AnonymousSwapFaultDisposition {\n'+waiter_fragment+'}\n')
 fields=['access_readers','access_exclusives','user_page_pins','stable_handles','retained_frame_refs','task_usercopy_accesses','pin_returns']+[f'lazy_rows[{i}]' for i in range(5)]+['lazy_returns','swap_objects','swap_mappings','swap_queued','swap_pageins','swap_pageouts','swap_waiters','swap_returns','legacy_file_registry_rows','native_file_registry_rows','registry_returns']
 cpp=r'''
 #include <array>
 #include <atomic>
 #include <cassert>
 #include <cstddef>
 #include <cstdint>
 #include <cstdio>
 #include <new>
 #include <thread>
 #include <vector>
 #include <platform/mm/lifecycle_census.hpp>
 #include <test/numa_lifecycle_mm_debt.hpp>
 '''+core+r'''
 namespace nl=ker::test::numa_lifecycle;
 namespace debt=nl::mm_debt;
 using DebtField=nl::CounterField;
 static size_t checks=0;
 #define CHECK(x) do { ++checks; if(!(x)){std::fprintf(stderr,"line %d: %s\n",__LINE__,#x); std::abort();} } while(false)
 std::array<uint64_t,23> fields(const nl::MmDebtSnapshot& x){return {'''+','.join('x.'+x for x in fields)+r'''};}
 nl::MmDebtSnapshot snap(){nl::MmDebtSnapshot x{};debt::snapshot(x);return x;}
 void clean(){auto x=snap();CHECK(x.valid&&x.inflight==0);for(auto n:fields(x))CHECK(n==0);}
 namespace sched::task { struct Task; }
 struct AnonymousSwapMapping {
  AnonymousSwapMapping* next{}; uint64_t generation{12}; sched::task::Task* waiters{};
  ~AnonymousSwapMapping(){auto x=snap();CHECK(x.swap_mappings>0&&x.swap_returns>0&&x.inflight>0);}
 };
 AnonymousSwapMapping* anonymous_swap_mappings{};
 ker::mod::sys::Spinlock anonymous_pagein_queue_lock;
 std::array<AnonymousSwapMapping*,2> anonymous_pagein_queue{};
 size_t anonymous_pagein_queue_head{},anonymous_pagein_queue_tail{},anonymous_pagein_queue_count{};
 namespace sched {
 namespace task { struct Task {Task* anonymous_swap_wait_next{}; bool released{}; AnonymousSwapMapping* anonymous_swap_wait_mapping{}; uint64_t anonymous_swap_wait_generation{}; std::atomic<uint64_t> anonymous_swap_completion_generation{99}; bool allow_acquire{true}; bool try_acquire(){CHECK(snap().inflight>0);return allow_acquire;} void release(){auto x=snap();CHECK(x.swap_waiters>0);released=true;}}; }
 void wake_task_from_event(task::Task* t){CHECK(t&&!t->released);auto x=snap();CHECK(x.swap_waiters>0);}
 }
 '''+ '\n'.join(actual)+r'''
 struct FailsAllocation {static void* operator new(size_t) noexcept{return nullptr;} };
 int main(){
  {debt::CounterState state{};nl::MmDebtSnapshot x{};debt::collect(state,x);CHECK(x.valid&&x.inflight==0);CHECK(x.version_begin==1&&x.version_end==1);
   for(size_t i=0;i<23;++i){debt::adjust(state,static_cast<DebtField>(i),true);debt::collect(state,x);auto row=fields(x);CHECK(x.valid&&x.inflight==0);for(size_t j=0;j<23;++j)CHECK(row[j]==(i==j));debt::adjust(state,static_cast<DebtField>(i),false);}
   debt::collect(state,x);CHECK(x.valid&&x.inflight==0);for(auto n:fields(x))CHECK(n==0);
   state.epoch.begin();state.epoch.begin();debt::collect(state,x);CHECK(x.valid&&x.inflight==2);state.epoch.end();state.epoch.end();debt::collect(state,x);CHECK(x.valid&&x.inflight==0);
  }
  {debt::CounterState state{};nl::MmDebtSnapshot x{};debt::adjust(state,DebtField::SWAP_PAGEINS,false);debt::collect(state,x);CHECK(!x.valid&&x.swap_pageins==0);debt::adjust(state,DebtField::SWAP_PAGEINS,true);debt::collect(state,x);CHECK(!x.valid&&x.swap_pageins==1);}
  {debt::CounterState state{};nl::MmDebtSnapshot x{};state.values[16].store(UINT64_MAX);debt::adjust(state,DebtField::SWAP_PAGEINS,true);debt::collect(state,x);CHECK(!x.valid&&x.swap_pageins==UINT64_MAX);}
  {debt::CounterState state{};nl::MmDebtSnapshot x{};debt::adjust(state,static_cast<DebtField>(999),true);debt::collect(state,x);CHECK(!x.valid);for(auto n:fields(x))CHECK(n==0);}
  {debt::CounterState state{};std::vector<std::thread> ts;for(int i=0;i<4;++i)ts.emplace_back([&]{for(int j=0;j<1000;++j){debt::adjust(state,DebtField::SWAP_PAGEINS,true);debt::adjust(state,DebtField::SWAP_PAGEINS,false);}});for(auto& t:ts)t.join();nl::MmDebtSnapshot x{};debt::collect(state,x);CHECK(x.valid&&x.inflight==0&&x.swap_pageins==0);CHECK(x.version_begin==16001);}
  clean();
  {auto* p=allocate_swap_descriptor<FailsAllocation>(DebtField::SWAP_OBJECTS);CHECK(p==nullptr);clean();delete_swap_descriptor(p,DebtField::SWAP_OBJECTS);clean();}
  {auto* a=allocate_swap_descriptor<AnonymousSwapMapping>(DebtField::SWAP_MAPPINGS);auto* b=allocate_swap_descriptor<AnonymousSwapMapping>(DebtField::SWAP_MAPPINGS);CHECK(a&&b);CHECK(snap().swap_mappings==2);insert_anonymous_swap_mapping_locked(a);insert_anonymous_swap_mapping_locked(b);CHECK(anonymous_swap_mappings==b&&b->next==a);remove_anonymous_swap_mapping_locked(a);CHECK(snap().swap_returns==1&&snap().swap_mappings==2);CHECK(b->next==nullptr);remove_anonymous_swap_mapping_locked(a);CHECK(snap().swap_returns==1);delete_swap_descriptor(a,DebtField::SWAP_MAPPINGS,true);CHECK(snap().swap_returns==0&&snap().swap_mappings==1);remove_anonymous_swap_mapping_locked(b);delete_swap_descriptor(b,DebtField::SWAP_MAPPINGS,true);CHECK(anonymous_swap_mappings==nullptr);clean();}
  {auto* a=allocate_swap_descriptor<AnonymousSwapMapping>(DebtField::SWAP_MAPPINGS);auto* b=allocate_swap_descriptor<AnonymousSwapMapping>(DebtField::SWAP_MAPPINGS);debt::add(DebtField::SWAP_PAGEINS);debt::add(DebtField::SWAP_PAGEINS);CHECK(anonymous_pagein_enqueue(a));CHECK(anonymous_pagein_enqueue(b));CHECK(!anonymous_pagein_enqueue(a));CHECK(snap().swap_queued==2&&snap().swap_pageins==2);CHECK(anonymous_pagein_dequeue()==a);CHECK(snap().swap_queued==1&&snap().swap_pageins==2);CHECK(anonymous_pagein_dequeue()==b);CHECK(anonymous_pagein_dequeue()==nullptr);CHECK(snap().swap_queued==0&&snap().swap_pageins==2);debt::add(DebtField::SWAP_RETURNS);debt::drop(DebtField::SWAP_PAGEINS);CHECK(snap().swap_pageins==1&&snap().swap_returns==1);debt::drop(DebtField::SWAP_RETURNS);debt::drop(DebtField::SWAP_PAGEINS);delete_swap_descriptor(a,DebtField::SWAP_MAPPINGS);delete_swap_descriptor(b,DebtField::SWAP_MAPPINGS);clean();}
  {sched::task::Task b{},a{&b};debt::add(DebtField::SWAP_WAITERS);debt::add(DebtField::SWAP_WAITERS);wake_anonymous_swap_waiters(&a);CHECK(a.released&&b.released&&a.anonymous_swap_wait_next==nullptr&&b.anonymous_swap_wait_next==nullptr);clean();}
  {AnonymousSwapMapping* mapping=allocate_swap_descriptor<AnonymousSwapMapping>(DebtField::SWAP_MAPPINGS);sched::task::Task t{};uint64_t generation=0;t.allow_acquire=false;CHECK(register_waiter(mapping,&t,generation)==AnonymousSwapFaultDisposition::FAILED);CHECK(snap().inflight==0&&snap().swap_waiters==0&&mapping->waiters==nullptr);t.allow_acquire=true;CHECK(register_waiter(mapping,&t,generation)==AnonymousSwapFaultDisposition::PARK);CHECK(generation==12&&t.anonymous_swap_wait_mapping==mapping&&t.anonymous_swap_wait_generation==12&&t.anonymous_swap_completion_generation.load()==0&&mapping->waiters==&t);CHECK(snap().swap_waiters==1&&snap().inflight==0);CHECK(register_waiter(mapping,&t,generation)==AnonymousSwapFaultDisposition::FAILED);CHECK(snap().swap_waiters==1&&snap().inflight==0);wake_anonymous_swap_waiters(mapping->waiters);mapping->waiters=nullptr;delete_swap_descriptor(mapping,DebtField::SWAP_MAPPINGS);clean();}
  std::printf("PASS actual counter/descriptor/list/queue/waiter bodies; checks=%zu; scalar fields=23; concurrent pairs=4000\n",checks);
 }
 '''
 return cpp

def main():
 compiler=os.environ.get('HOST_CXX') or shutil.which('g++',path=os.defpath) or shutil.which('clang++',path=os.defpath)
 if compiler is None: raise SystemExit('no Host C++ compiler')
 environment={**os.environ,'ASAN_OPTIONS':'detect_leaks=0:halt_on_error=1','UBSAN_OPTIONS':'halt_on_error=1:print_stacktrace=1'}
 paths=[SOURCE,ROOT/'modules/kern/src/test/numa_lifecycle_mm_debt.hpp',ROOT/'modules/kern/src/platform/mm/lifecycle_census.hpp'];before=[p.read_bytes() for p in paths]
 with tempfile.TemporaryDirectory(prefix='wos-numa-mm-debt-core-') as scratch:
  scratch=Path(scratch);source=scratch/'actual.cpp';binary=scratch/'actual';source.write_text(program())
  commands=[([compiler,'-std=c++23','-DWOS_SELFTEST=1','-DWOS_HOST_TEST=1','-I'+str(ROOT/'tests/host/kern_shim'),'-I'+str(ROOT/'modules/kern/src'),'-I'+str(ROOT/'modules'),'-O1','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer','-pthread',str(source),'-o',str(binary)],60),([str(binary)],20)]
  for argv,timeout in commands:
   result=subprocess.run(argv,capture_output=True,text=True,timeout=timeout,env=environment)
   if result.stdout:print(result.stdout,end='')
   if result.stderr:print(result.stderr,end='')
   if result.returncode:raise SystemExit(result.returncode)
 assert before==[p.read_bytes() for p in paths], 'live source changed'
if __name__=='__main__':main()
