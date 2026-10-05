#!/usr/bin/env python3
"""Run actual initial-thread construction and lazy growth against dirty pages.

Physical allocation, page tables, entropy and locks are host endpoints. This
checks startup residency, full reserved capacity, guards, resource limits and
failure cleanup; it cannot establish boot behavior or a launch-time gain.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from exec_trace_ownership_test import block

ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(os.environ.get("WOS_THREAD_INITIAL_TEST_SOURCE", ROOT / "modules/kern/src/platform/sched/threading.cpp"))
source = SOURCE.read_text()
header = (ROOT / "modules/kern/src/platform/sched/threading.hpp").read_text()
thread = header[header.index("struct Thread {"):header.index("void init_threading();")]
constants = "\n".join(re.findall(r"constexpr uint64_t STACK_\w+ = [^;]+;", source))
names = ["min_u64", "max_u64", "checked_add", "checked_page_align_up", "stack_top",
         "free_mapped_user_range", "map_zeroed_user_range", "write_mapped_user_bytes",
         "stack_budget", "deny_stack", "back_stack_pages", "write_mapped_user_value",
         "has_automatic_stack", "record_stack_usage", "ensure_stack_backing",
         "handle_lazy_stack_fault", "create_thread", "range_overlaps_initial_layout", "destroy_thread"]
bodies = []
for name in names:
    body = block(source, rf"^(?:auto|void|bool|Thread\*) {name}\([^{{;]*\{{")
    if name == "write_mapped_user_value":
        body = "template <typename T>\n" + body
    bodies.append(body)

code = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <vector>
#include <platform/sched/resource_model.hpp>
namespace recon {
constexpr uint64_t PAGE = 4096, CAPACITY = 64ULL*1024*1024;
int guard_depth=0, attempts=0, fail_at=-1, checks=0;
struct Lock {int depth=0; void lock(){assert(!depth);++depth;} void unlock(){assert(depth==1);--depth;}
 uint64_t lock_irqsave(){lock();return 0x200;} void unlock_irqrestore(uint64_t f){assert(f==0x200);unlock();}};
struct PreemptGuard {PreemptGuard(){++guard_depth;}~PreemptGuard(){--guard_depth;}};
struct log {template<class...T>static void error(const char*,T...) {}};
namespace cpu {struct PerCpu {std::array<std::byte,128> bytes;};}
namespace mm {
enum class PhysicalPageOwner {USER_THREAD_TLS,USER_THREAD_STACK};
namespace paging {constexpr uint64_t PAGE_SIZE=PAGE,PAGE_NX=1ULL<<63;
 namespace page_types {constexpr uint64_t USER=7;}
 struct PageTable {std::set<uint64_t> reserved;std::map<uint64_t,uint64_t> mapped;};}
std::map<void*,PhysicalPageOwner> live;
namespace addr {auto get_phys_pointer(uint64_t p)->void*{return reinterpret_cast<void*>(p);}
 auto get_virt_pointer(uint64_t p)->void*{return reinterpret_cast<void*>(p);}}
namespace phys {
auto allocate(PhysicalPageOwner owner)->void* {
 assert((owner==PhysicalPageOwner::USER_THREAD_STACK)==(guard_depth>0));
 if(attempts++==fail_at)return nullptr;
 auto*p=std::aligned_alloc(PAGE,PAGE);assert(p);std::memset(p,0xa5,PAGE);assert(live.emplace(p,owner).second);return p;}
auto page_alloc_full_overwrite_may_fail(PhysicalPageOwner o,uint64_t size,const char*)->void*{assert(size==PAGE);return allocate(o);}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner o,const char*)->void*{return allocate(o);}
void page_free(void*p){assert(live.erase(p)==1);std::free(p);}
}
namespace virt {
constexpr uint64_t PADDR_INVALID=UINT64_MAX;
auto translate(paging::PageTable*t,uint64_t v)->uint64_t {auto i=t->mapped.find(v&~(PAGE-1));return i==t->mapped.end()?PADDR_INVALID:i->second+(v&(PAGE-1));}
auto is_page_mapped_or_reserved(paging::PageTable*t,uint64_t v)->bool{return t->reserved.contains(v)||t->mapped.contains(v);}
void reserve_page_range(paging::PageTable*t,uint64_t v,uint64_t n){for(uint64_t i=0;i<n;++i)assert(t->reserved.insert(v+i*PAGE).second);}
void map_page(paging::PageTable*t,uint64_t v,uint64_t p,uint64_t flags){
 assert(t->reserved.contains(v)&&!t->mapped.contains(v));assert(flags==(paging::page_types::USER|paging::PAGE_NX));
 auto*b=reinterpret_cast<unsigned char*>(p);for(size_t i=0;i<PAGE;++i)assert(b[i]==0);t->mapped[v]=p;}
void unmap_page(paging::PageTable*t,uint64_t v){auto i=t->mapped.find(v);if(i!=t->mapped.end()){phys::page_free(reinterpret_cast<void*>(i->second));t->mapped.erase(i);}t->reserved.erase(v);}
}
namespace user_layout {bool choose_thread_region(paging::PageTable*,uint64_t,uint64_t&out){out=0x100000;return true;}}
}
uint64_t page_align_down(uint64_t v){return v&~(PAGE-1);}
uint64_t page_align_up(uint64_t v){return (v+PAGE-1)&~(PAGE-1);}
THREAD_STRUCT
namespace task {struct Task {Thread*thread=nullptr;mm::paging::PageTable*pagemap=nullptr;Lock resource_lock;ker::mod::sched::resource::State resources;};}
namespace resource {
using ker::mod::sched::resource::Id;using ker::mod::sched::resource::Denial;
using ker::mod::sched::resource::stack_backing_budget;
struct Owner {task::Task*p;explicit Owner(task::Task*t):p(t){}task::Task*get(){return p;}};
bool admit_address_space(task::Task*t,mm::paging::PageTable*,uint64_t n){return t&&t->resources.admit_address_space(0,0,n);}
}
namespace random::entropy {bool get_bytes(void*p,size_t n){std::memset(p,0x53,n);return true;}}
namespace ker::loader::elf {struct TlsModule {uint64_t tls_size=176;};}
template<class T>struct List {std::vector<T> v;void push_back(T t){v.push_back(t);}void remove(T t){auto i=std::find(v.begin(),v.end(),t);assert(i!=v.end());v.erase(i);}size_t size(){return v.size();}};
List<Thread*> active_threads;Lock active_threads_lock;
STACK_CONSTANTS
ACTUAL_FUNCTIONS
constexpr unsigned STACK = static_cast<unsigned>(resource::Id::STACK);
auto count(mm::PhysicalPageOwner o)->size_t {return std::count_if(mm::live.begin(),mm::live.end(),[o](auto&p){return p.second==o;});}
void clear(mm::paging::PageTable&root,task::Task&t,Thread*th){
 if(th)destroy_thread(th);t.thread=nullptr;
 while(!root.reserved.empty())mm::virt::unmap_page(&root,*root.reserved.begin());
 assert(root.mapped.empty()&&mm::live.empty()&&!active_threads.size()&&!active_threads_lock.depth&&!t.resource_lock.depth&&!guard_depth);
 attempts=0;fail_at=-1;++checks;}
Thread*make(mm::paging::PageTable&r,task::Task&t,uint64_t soft=UINT64_MAX,uint64_t capacity=CAPACITY){
 t.pagemap=&r;t.resources.entries[STACK].limit.soft=soft;
 auto*th=create_thread(capacity,176,&r,42,ker::loader::elf::TlsModule{},&t);t.thread=th;return th;}
uint64_t peek(mm::paging::PageTable&r,uint64_t a){auto p=mm::virt::translate(&r,a);assert(p!=mm::virt::PADDR_INVALID);uint64_t v;std::memcpy(&v,reinterpret_cast<void*>(p),sizeof(v));return v;}
void run(){
 {mm::paging::PageTable r;task::Task t;auto*th=make(r,t);assert(th);
  // Eight upper pages plus the existing bottom compatibility page. The old
  // eager window violates this ceiling with65 normal-stack allocations.
  assert(count(mm::PhysicalPageOwner::USER_THREAD_STACK)==9);
  assert(count(mm::PhysicalPageOwner::USER_THREAD_TLS)==17);
  auto top=stack_top(th);assert(th->stack_size==CAPACITY&&th->stack_lowest_backed==top-8*PAGE);
  assert(r.reserved.size()==th->layout_size/PAGE);assert(th->stack==top-sizeof(cpu::PerCpu));
  assert(peek(r,th->fsbase)==th->fsbase&&peek(r,th->fsbase+0x18)==42);
  assert(peek(r,th->tls_base_virt)==th->safestack_ptr_value);
  assert(range_overlaps_initial_layout(th,th->layout_base_virt,th->layout_size));
  for(auto guard:{th->layout_base_virt,top,th->tls_base_virt+PAGE,th->layout_base_virt+th->layout_size-PAGE}){
   auto before=attempts;assert(mm::virt::translate(&r,guard)==mm::virt::PADDR_INVALID);
   assert(!handle_lazy_stack_fault(th,&r,guard,top-PAGE,&t));assert(attempts==before);}
  auto old=mm::virt::translate(&r,top-PAGE);auto before=attempts;
  fail_at=attempts+2;assert(!ensure_stack_backing(th,&r,top-24*PAGE,top,&t));
  assert(th->stack_lowest_backed==top-8*PAGE&&count(mm::PhysicalPageOwner::USER_THREAD_STACK)==9);
  assert(mm::virt::translate(&r,top-PAGE)==old);assert(attempts==before+3);fail_at=-1;
  // Explicit argument materialization larger than the startup window.
  assert(ensure_stack_backing(th,&r,top-24*PAGE,top,&t));assert(count(mm::PhysicalPageOwner::USER_THREAD_STACK)==25);
  assert(th->stack_lowest_backed==top-24*PAGE&&t.resources.entries[STACK].current==24*PAGE);
  auto child=*th;assert(child.stack_lowest_backed==th->stack_lowest_backed);
  assert(handle_lazy_stack_fault(th,&r,top-25*PAGE,top-25*PAGE,&t));
  assert(th->stack_lowest_backed<top-25*PAGE&&count(mm::PhysicalPageOwner::USER_THREAD_STACK)>25);
  assert(mm::virt::translate(&r,th->stack_lowest_backed)!=mm::virt::PADDR_INVALID);
  clear(r,t,th);}
 for(auto soft:{uint64_t(0),PAGE-1,PAGE,2*PAGE,8*PAGE,64*PAGE,UINT64_MAX}){
  mm::paging::PageTable r;task::Task t;auto*th=make(r,t,soft);
  if(soft<PAGE){assert(!th&&attempts==0&&t.resources.entries[STACK].denied==1);}else{
   assert(th);auto initial=std::min(soft-soft%PAGE,8*PAGE);assert(th->stack_lowest_backed==stack_top(th)-initial);
   assert(count(mm::PhysicalPageOwner::USER_THREAD_STACK)==initial/PAGE+1);
   t.resources.entries[STACK].limit.soft=0;
   assert(ensure_stack_backing(th,&r,stack_top(th)-PAGE,stack_top(th),&t));
   assert(!handle_lazy_stack_fault(th,&r,th->stack_lowest_backed-PAGE,th->stack_lowest_backed-PAGE,&t));}
  clear(r,t,th);}
 {mm::paging::PageTable r;task::Task t;auto*th=make(r,t,UINT64_MAX,4*PAGE);assert(th);
  assert(count(mm::PhysicalPageOwner::USER_THREAD_STACK)==4);clear(r,t,th);}
 // Every TLS/SafeStack, top-window and compatibility-page allocation failure
 // must leave no mapped or reserved page and no published Thread.
 for(int fail=0;fail<26;++fail){mm::paging::PageTable r;task::Task t;fail_at=fail;auto*th=make(r,t);
  assert(!th&&r.mapped.empty()&&r.reserved.empty()&&mm::live.empty()&&!active_threads.size());clear(r,t,th);}
 {mm::paging::PageTable r;task::Task t;auto*th=make(r,t,12*PAGE);assert(th);
  auto top=stack_top(th);assert(handle_lazy_stack_fault(th,&r,top-9*PAGE,top-9*PAGE,&t));
  assert(th->stack_lowest_backed==top-12*PAGE&&t.resources.entries[STACK].current==12*PAGE);
  assert(mm::virt::translate(&r,top-13*PAGE)==mm::virt::PADDR_INVALID);clear(r,t,th);}
 std::printf("ACTUAL_INITIAL_STACK_PASS cases=%d startup_stack_pages=9 tls_safestack_pages=17 capacity=67108864\n",checks);
}
}
int main(){recon::run();}
'''.replace("THREAD_STRUCT", thread).replace("STACK_CONSTANTS", constants).replace("ACTUAL_FUNCTIONS", "\n".join(bodies))


def execute(directory):
    directory = Path(directory)
    native_source = directory / "test.cpp"
    binary = directory / "test"
    native_source.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                    "-I", str(ROOT / "modules/kern/src"), str(native_source), "-o", str(binary)], check=True)
    subprocess.run([str(binary)], check=True)
    (directory / "proof.json").write_text(json.dumps({
        "status": "ACTUAL_INITIAL_CREATION_AND_LAZY_GROWTH_HOST_PASS",
        "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        "thread_header_sha256": hashlib.sha256(header.encode()).hexdigest(),
        "test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "resource_model_sha256": hashlib.sha256((ROOT / "modules/kern/src/platform/sched/resource_model.hpp").read_bytes()).hexdigest(),
        "scope": "Host endpoint model; boot, real faults, COW, interrupts and elapsed performance require fresh runtime evidence."
    }, indent=2) + "\n")


if output := os.environ.get("WOS_THREAD_INITIAL_TEST_OUTPUT"):
    directory = Path(output)
    directory.mkdir(exist_ok=False, parents=True)
    execute(directory)
else:
    with tempfile.TemporaryDirectory(prefix="wos-thread-initial-") as directory:
        execute(directory)
