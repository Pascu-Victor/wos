#!/usr/bin/env python3
"""Compile actual accounting code against an independent logical-occupancy model."""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
BASE = Path(os.environ.get('WOS_VAS_REUSE_BASE', ROOT))
source = (BASE / 'modules/kern/src/platform/sched/resource_as.cpp').read_text()
header = (BASE / 'modules/kern/src/platform/sched/resource_as.hpp').read_text()
declaration = header[header.index('class AddressSpaceObservation'):header.index('\n};', header.index('class AddressSpaceObservation')) + 3]
source = source[source.index('namespace ker::mod::sched::resource {'):]
fast = 'auto admit_mapping(' in declaration
mm_source = (BASE / 'modules/kern/src/platform/mm/virt.opt.cpp').read_text()
if 'auto validate_user_pagemap_access(' in mm_source:
    begin = mm_source.index('auto validate_user_pagemap_access(')
    gate_check = mm_source[begin:mm_source.index('\n}', begin) + 2]
else:
    gate_check = 'bool validate_user_pagemap_access(PageTable *root) { return root && root->gate; }'
vmem = (BASE / 'modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp').read_text()

def branch(start):
    begin = vmem.index(start, vmem.index('auto anon_allocate('))
    cursor = vmem.index('{', begin)
    depth = 1
    while depth:
        cursor += 1
        depth += (vmem[cursor] == '{') - (vmem[cursor] == '}')
    return vmem[begin:cursor + 1]

branches = [branch('if (is_prot_none(prot) ||'), branch('if (lazy_anon_mmap_enabled() &&'),
            branch('if ((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG && lazy_file_mmap_enabled())')]
mapping_code = r'''
namespace ker::vfs { constexpr unsigned S_IFMT=0170000, S_IFREG=0100000; }
bool fail_metadata=false, fail_register=false;
bool is_prot_none(uint64_t){return true;}
bool lazy_anon_mmap_enabled(){return true;}
bool lazy_file_mmap_enabled(){return true;}
bool add_shared_vmem_range_locked(task::Task *t,uint64_t va,uint64_t size,uint64_t,uint64_t){
 t->lazy_vmem_ranges.push_back({va,va+size});return !fail_metadata;
}
void release_mmap_reservation_locked(task::Task *t,uint64_t va,uint64_t,bool){
 std::erase_if(t->lazy_vmem_ranges,[va](auto r){return r.start==va;});
}
bool remove_shared_vmem_range_locked(task::Task *t,uint64_t va,uint64_t size){
 release_mmap_reservation_locked(t,va,size,true);return true;
}
template<class... T> bool add_shared_lazy_file_vmem_range_locked(task::Task *t,uint64_t va,uint64_t size,T...){
 return add_shared_vmem_range_locked(t,va,size,0,0);
}
template<class... T> int register_file_mmap_range(T...){return fail_register?-EIO:0;}
void advance_shared_mmap_cursor_locked(task::Task *,uint64_t,uint64_t){}
template<class... T> void record_local_vmem_event(T...){}
uint32_t vmem_latency_since(uint64_t){return 0;}
'''
for index, body in enumerate(branches):
    mapping_code += f'uint64_t actual_mapping_{index}(task::Task *task) {{' + r'''
 sched::resource::AddressSpaceObservation observation(task);
 uint64_t size=2*P,vaddr=40*P,prot=0,flags=0;
 bool HAS_ADDRESS_RESERVATION=true,TRACK_FILE_MAPPING=true,WRITEBACK=true;
 uint64_t REQUESTED_SIZE=size-1,offset=0,NUM_PAGES=2,PERF_STARTED_US=0;
 int file=3;struct Stat{unsigned st_mode=ker::vfs::S_IFREG;} st;
 if(!admit(observation,*task,size))return static_cast<uint64_t>(-ENOMEM);
 ''' + body + '\n assert(false);return 0;\n}\n'
code = r'''
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <vector>
#include <platform/sched/resource_model.hpp>
#include <abi/callnums/vmem.h>
namespace ker::mod::mm::paging {
constexpr uint64_t PAGE_SIZE = 4096;
struct PageTable { std::vector<uint64_t> pages; bool gate = true; };
}
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
struct Range { uint64_t start, end; };
struct Lock { bool held = false; uint64_t lock_irqsave() { assert(!held); held=true; return 0; }
 void unlock_irqrestore(uint64_t) { assert(held); held=false; } };
struct Task {
 uint64_t pid=73; mm::paging::PageTable *pagemap=nullptr;
 TaskType type=TaskType::PROCESS; bool is_thread=false, active=true, closing=false;
 std::atomic<bool> shares_user_pagemap{false};
 std::vector<Range> lazy_vmem_ranges;
 resource::State resources; Lock resource_lock; unsigned pins=0, acquisitions=0, releases=0;
 bool try_acquire_usercopy_pagemap(mm::paging::PageTable *&out) {
  ++acquisitions; out=nullptr;
  if (!active || closing || !pagemap) return false;
  out=pagemap; ++pins; return true;
 }
 void release_usercopy_pagemap() { assert(pins==1); --pins; ++releases; }
};
}
namespace ker::mod::sched {
task::Task *current=nullptr;
task::Task *get_current_task() { return current; }
unsigned guards=0, records=0;
struct PreemptGuard { PreemptGuard(){++guards;} ~PreemptGuard(){--guards;} };
void record_address_space_usage(task::Task *subject, mm::paging::PageTable *root, uint64_t bytes) {
 assert(subject->pins==1 && guards && subject->pagemap==root); ++records;
 subject->resources.commit_extent(static_cast<unsigned>(abi::resource::Id::AS),bytes);
}
}
namespace ker::mod::mm::virt {
unsigned walks=0, checks=0;
using paging::PageTable;
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE=static_cast<size_t>(-1);
unsigned gate_readers=0;
bool acquire_user_pagemap_access(PageTable *root,bool wait,size_t &gate) {
 assert(!wait && sched::guards && sched::current->pins==1);++checks;
 gate=USER_PAGEMAP_ACCESS_GATE_NONE;
 if(!root || !root->gate) return false;
 gate=3; ++gate_readers; return true;
}
void release_user_pagemap_access(size_t gate) { assert(gate==3 && gate_readers==1);--gate_readers; }
bool count_user_virtual_bytes(paging::PageTable *root,uint64_t start,uint64_t end,uint64_t &bytes) {
 bytes=0;
 if(start==end) return true;
 ++walks; if (!root->gate) return false;
 for (auto va:root->pages) if(va>=start && va<end) bytes+=paging::PAGE_SIZE;
 return true;
}
GATE_CHECK
}
namespace ker::mod::time { uint64_t get_us(){assert(false); return 0;} }
namespace ker::mod::cpu { unsigned current_cpu(){assert(false); return 0;} }
namespace ker::mod::perf {
enum class WkiPerfScope { LOCAL_VMEM }; enum class WkiPerfLocalVmemOp { ANON_MMAP=1,FILE_MMAP=3,VAS_OBSERVE=11 };
enum class WkiPerfPhase { END }; constexpr uint64_t PERF_ANY_SESSION=UINT64_MAX;
uint64_t current_wki_capture_session(WkiPerfScope,uint8_t){return PERF_ANY_SESSION;}
uint32_t next_wki_trace_correlation(){assert(false);return 0;}
template<class... T> void record_wki_event(T...){assert(false);}
template<class... T> void record_wki_summary(T...){assert(false);}
}
namespace ker::mod::sched::resource {
class Owner { task::Task *p; public: explicit Owner(task::Task *t):p(t){} task::Task *get(){return p;} };
''' + declaration + '\n}\n' + source + r'''
using namespace ker::mod;
constexpr uint64_t P=mm::paging::PAGE_SIZE, END=0x0000800000000000ULL;
constexpr unsigned AS=static_cast<unsigned>(abi::resource::Id::AS);
uint64_t reference(task::Task &t) {
 std::vector<uint64_t> pages=t.pagemap->pages;
 for(auto r:t.lazy_vmem_ranges) for(auto va=r.start;va<r.end;va+=P) pages.push_back(va);
 std::sort(pages.begin(),pages.end());
 pages.erase(std::unique(pages.begin(),pages.end()),pages.end()); return pages.size()*P;
}
bool admit(sched::resource::AddressSpaceObservation &o,task::Task &t,uint64_t added,uint64_t start=0,uint64_t removed=0) {
 ADMIT_CALL
}
void commit(sched::resource::AddressSpaceObservation &o) { COMMIT_CALL }
void reset(task::Task &t,mm::paging::PageTable &root) {
 t.pagemap=&root; root.pages={P,3*P,8*P}; root.gate=true;
 t.lazy_vmem_ranges={{12*P,15*P},{5*P,7*P}};
 t.resources=sched::resource::State{};
 t.resources.entries[AS].current=999*P; t.resources.entries[AS].peak=999*P;
 t.active=true;t.closing=false;t.type=task::TaskType::PROCESS;t.is_thread=false;
 t.shares_user_pagemap.store(false);sched::current=&t;sched::records=0;mm::virt::walks=mm::virt::checks=0;
}
MAPPING_CODE
int main() {
 task::Task t; mm::paging::PageTable root;
 unsigned cases=0;
 for(bool fixed:{false,true}) for(bool fail:{false,true}) for(bool shared:{false,true}) {
  reset(t,root);t.shares_user_pagemap.store(shared);
  unsigned after_admit=0;
  {
   sched::resource::AddressSpaceObservation o(&t);
   assert(admit(o,t,4*P,fixed?4*P:0,fixed?4*P:0));
   after_admit=mm::virt::walks;
   if(fixed){t.lazy_vmem_ranges.erase(t.lazy_vmem_ranges.begin()+1);}
   if(!fail){t.lazy_vmem_ranges.push_back({fixed?4*P:20*P,fixed?8*P:24*P});commit(o);}
  }
  assert(t.resources.entries[AS].current==reference(t));
  assert(t.resources.entries[AS].peak==999*P && t.pins==0);
  assert(sched::records==1 && sched::guards==0);
  if(!shared && !fail && REQUIRE_FAST) assert(mm::virt::walks==after_admit && mm::virt::checks==1);
  if(shared || fail) assert(mm::virt::walks>after_admit);
  ++cases;
 }
 // Materialized lazy pages contribute once; shrinking above a lowered limit remains legal.
 reset(t,root);root.pages.push_back(5*P);root.pages.push_back(13*P);
 t.resources.entries[AS].limit.soft=0;
 {sched::resource::AddressSpaceObservation o(&t);
  assert(admit(o,t,P,12*P,3*P));t.lazy_vmem_ranges[0]={12*P,13*P};
  root.pages.erase(std::remove(root.pages.begin(),root.pages.end(),13*P),root.pages.end());commit(o);}
 assert(t.resources.entries[AS].current==reference(t));++cases;
 // A fresh larger mapping increases peak using the exact page-set union,
 // independent of deliberately stale resource.current.
 reset(t,root);t.resources.entries[AS].peak=0;
 {sched::resource::AddressSpaceObservation o(&t);assert(admit(o,t,20*P));
  t.lazy_vmem_ranges.push_back({20*P,40*P});commit(o);}
 assert(t.resources.entries[AS].current==reference(t) && t.resources.entries[AS].peak==reference(t));++cases;
 for(bool overflow:{false,true}) {
  reset(t,root);t.resources.entries[AS].limit.soft=0;
  {sched::resource::AddressSpaceObservation o(&t);assert(!admit(o,t,overflow?UINT64_MAX:P));commit(o);}
  assert(t.resources.entries[AS].current==reference(t));assert(t.resources.entries[AS].denied==1);
  assert(t.resources.entries[AS].reason==(overflow?abi::resource::Denial::OVERFLOW:abi::resource::Denial::SOFT_LIMIT));++cases;
 }
 for(int mode=0;mode<6;++mode) {
  reset(t,root);mm::paging::PageTable other;other.pages={77*P};
  {sched::resource::AddressSpaceObservation o(&t);assert(admit(o,t,P));t.lazy_vmem_ranges.push_back({20*P,21*P});commit(o);
   if(mode==0)t.closing=true;if(mode==1)t.active=false;if(mode==2)root.gate=false;
   if(mode==3)t.pagemap=&other;if(mode==4)t.shares_user_pagemap.store(true);if(mode==5)t.is_thread=true;
  }
  assert(t.pins==0 && sched::guards==0);
  if(mode<3) assert(sched::records==0 && t.resources.entries[AS].current==999*P);
 else assert(sched::records==1 && t.resources.entries[AS].current==reference(t));++cases;
 }
 // Uncommitted/duplicate and ineligible receipts must recount changed state.
 for(int mode=0;mode<8;++mode) {
  reset(t,root);task::Task stranger;
  unsigned after_admit=0;
  {
   sched::resource::AddressSpaceObservation o(&t);
   if(mode==3)t.type=task::TaskType::DAEMON;
   if(mode==4)t.is_thread=true;
   if(mode==5)t.shares_user_pagemap.store(true);
   if(mode==6)sched::current=&stranger;
   if(mode==7)t.lazy_vmem_ranges.push_back({3*P,4*P});
   assert(admit(o,t,2*P));after_admit=mm::virt::walks;
   t.lazy_vmem_ranges.push_back({20*P,22*P});
   if(mode==1){commit(o);commit(o);}
   else if(mode==2){assert(admit(o,t,P));commit(o);t.lazy_vmem_ranges.push_back({24*P,25*P});}
   else if(mode!=0)commit(o);
  }
  assert(t.resources.entries[AS].current==reference(t));assert(t.pins==0 && !mm::virt::gate_readers);
  if(mode!=7) assert(mm::virt::walks>after_admit);++cases;
 }
 reset(t,root);t.lazy_vmem_ranges.push_back({END,END+P});
 {sched::resource::AddressSpaceObservation o(&t);assert(!admit(o,t,P));commit(o);}
 assert(sched::records==0 && !t.pins);++cases;
 reset(t,root);mm::paging::PageTable staged;staged.pages={70*P,90*P};
 {sched::resource::AddressSpaceObservation o(&t);
  STAGED_ADMIT
  commit(o);t.lazy_vmem_ranges.push_back({20*P,23*P});}
 assert(t.resources.entries[AS].current==reference(t));++cases;
 reset(t,root);
 {sched::resource::AddressSpaceObservation o(nullptr);assert(sched::resource::admit_address_space(nullptr,&root,P,0,0));commit(o);}
 assert(!sched::records && !mm::virt::gate_readers);++cases;
 reset(t,root);
 {sched::resource::AddressSpaceObservation o(&t);assert(!admit(o,t,P,END,P));commit(o);}
 assert(t.resources.entries[AS].current==reference(t));++cases;
 using Mapping=uint64_t(*)(task::Task*);
 for(Mapping mapping:{actual_mapping_0,actual_mapping_1,actual_mapping_2})
 for(bool metadata:{false,true}) for(bool registration:{false,true}) {
  reset(t,root);fail_metadata=metadata;fail_register=registration;
  uint64_t result=mapping(&t);
  bool failed=metadata || (mapping==actual_mapping_2 && registration);
  assert(failed ? static_cast<int64_t>(result)<0 : result==40*P);
  assert(t.resources.entries[AS].current==reference(t) && t.resources.entries[AS].peak==999*P);
  assert(!t.pins && !mm::virt::gate_readers && sched::records==1);
  if(REQUIRE_FAST) assert(mm::virt::checks==unsigned(!failed));++cases;
 }
 printf("actual-source admission/observation/mapping %u semantic cases PASS; require-fast=%d\n",cases,REQUIRE_FAST);
}
'''
# Alias only for test code outside the kernel namespace.
code = code.replace('using namespace ker::mod;', 'using namespace ker::mod; namespace task = ker::mod::sched::task; namespace abi = ker::abi;')
code = code.replace('ADMIT_CALL', 'return o.admit_mapping(t.pagemap,added,start,removed);' if fast else 'return sched::resource::admit_address_space(&t,t.pagemap,added,start,removed);')
code = code.replace('COMMIT_CALL', 'o.commit_mapping();' if fast else '(void)o;')
code = code.replace('GATE_CHECK', gate_check)
code = code.replace('MAPPING_CODE', mapping_code)
code = code.replace('STAGED_ADMIT', 'assert(o.admit_mapping(&staged,P));' if fast else 'assert(sched::resource::admit_address_space(&t,&staged,P,0,0));')
code = code.replace('REQUIRE_FAST', 'true' if os.environ.get('WOS_VAS_REUSE_REQUIRE_FAST', '1') == '1' else 'false')
with tempfile.TemporaryDirectory(prefix='wos-vas-admission-') as temp:
    p = Path(temp)
    (p / 'check.cpp').write_text(code)
    subprocess.run(['/usr/bin/clang++', '-std=c++23', '-O2', '-g', '-fsanitize=address,undefined',
                    '-fno-omit-frame-pointer', '-I', str(ROOT / 'modules/kern/src'), str(p / 'check.cpp'), '-o', str(p / 'check')], check=True)
    subprocess.run([str(p / 'check')], check=True)
