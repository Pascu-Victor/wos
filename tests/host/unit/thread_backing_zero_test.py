#!/usr/bin/env python3
"""Exercise actual stack/TLS backing with dirty allocator pages and failures.

Host endpoints replace physical allocation/PTE publication. This checks zeroing,
page ownership, staged rollback and preemption context, not hardware CR3 cost.
"""
import os
from pathlib import Path
import subprocess
import tempfile

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]
source = Path(os.environ.get("WOS_THREAD_BACKING_TEST_SOURCE", ROOT / "modules/kern/src/platform/sched/threading.cpp")).read_text()
a = function(source, "map_zeroed_user_range")
b = function(source, "back_stack_pages")
code=r'''
#define WOS_SELFTEST 1
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <limits>
namespace recon {
int guard_depth=0,attempts=0,freed=0,fail_at=-1,allocator_zeroings=0;
std::array<bool,8> live{};std::array<uint64_t,8> mappings{};
alignas(4096) std::array<std::array<std::byte,4096>,8> pages{};
struct PreemptGuard{PreemptGuard(){++guard_depth;}~PreemptGuard(){--guard_depth;}};
namespace mm {
enum class PhysicalPageOwner {USER_THREAD_TLS,USER_THREAD_STACK};
namespace paging{constexpr uint64_t PAGE_SIZE=4096,PAGE_NX=1ULL<<63;namespace page_types {constexpr uint64_t USER=7;}}
namespace addr{auto get_phys_pointer(uint64_t p)->void*{return reinterpret_cast<void*>(p);}}
namespace phys{
auto allocate(PhysicalPageOwner owner)->void* {assert((guard_depth>0)==(owner==PhysicalPageOwner::USER_THREAD_STACK));auto n=attempts++;if(n==fail_at)return nullptr;for(size_t i=0;i<live.size();++i)if(!live[i]){live[i]=true;pages[i].fill(std::byte{0xa5});return pages[i].data();}assert(false);return nullptr;}
auto page_alloc_full_overwrite_may_fail(PhysicalPageOwner o,uint64_t sz,const char*)->void*{assert(sz==4096);return allocate(o);}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner o,const char*)->void*{return allocate(o);}
auto page_alloc_may_fail(PhysicalPageOwner o,uint64_t sz,const char*)->void*{assert(sz==4096);auto*p=allocate(o);if(p){std::memset(p,0,4096);++allocator_zeroings;}return p;}
auto page_alloc_with_reclaim_may_fail(PhysicalPageOwner o,uint64_t sz,const char*n)->void*{return page_alloc_may_fail(o,sz,n);}
void page_free(void*p){for(size_t i=0;i<live.size();++i)if(pages[i].data()==p){assert(live[i]);live[i]=false;++freed;return;}assert(false);}
}
namespace virt{
constexpr uint64_t PADDR_INVALID=UINT64_MAX;
auto translate(void*,uint64_t v)->uint64_t{assert(v>=0x10000&&v<0x18000);return mappings[(v-0x10000)/4096]?mappings[(v-0x10000)/4096]:PADDR_INVALID;}
void map_page(void*,uint64_t v,uint64_t p,uint64_t flags){assert(flags==(paging::page_types::USER|paging::PAGE_NX));auto*b=reinterpret_cast<std::byte*>(p);for(size_t i=0;i<4096;++i)assert(b[i]==std::byte{0});auto idx=(v-0x10000)/4096;assert(!mappings[idx]);mappings[idx]=p;}
}
}
TLS_FUNCTION
STACK_FUNCTION
void reset(){assert(!guard_depth);live.fill(false);mappings.fill(0);attempts=freed=0;fail_at=-1;}
size_t mapped(){return std::count_if(mappings.begin(),mappings.end(),[](auto p){return p!=0;});}
int run(){void*const root=reinterpret_cast<void*>(1);
 for(int fail=0;fail<4;++fail){reset();fail_at=fail;assert(!back_stack_pages(root,0x10000,0x14000));assert(mapped()==0&&freed==fail&&!guard_depth);for(auto l:live)assert(!l);}
 reset();assert(back_stack_pages(root,0x10000,0x14000));assert(mapped()==4&&!guard_depth);assert(attempts==4);assert(back_stack_pages(root,0x10000,0x14000));assert(attempts==4&&mapped()==4);
 auto old=mappings[0];assert(!back_stack_pages(root,0x14000,0x18000,1));assert(mapped()==4&&mappings[0]==old&&freed==1);
 reset();assert(back_stack_pages(root,0x10000,0x10000));assert(attempts==0);assert(!back_stack_pages(root,0x10000,0x12000,1));assert(mapped()==0&&freed==1);
 reset();assert(map_zeroed_user_range(root,0x10000,3*4096,"tls"));assert(mapped()==3&&attempts==3&&!guard_depth);
 reset();fail_at=2;assert(!map_zeroed_user_range(root,0x10000,3*4096,"safestack"));assert(mapped()==2&&attempts==3);for(auto p:mappings)if(p)mm::phys::page_free(reinterpret_cast<void*>(p));for(auto l:live)assert(!l);
 assert(allocator_zeroings==0); // Caller clears every successful page before publication.
 std::puts("Actual stack/TLS full-overwrite dirty pages: zero-before-publication, every staged allocation failure, injected rollback, existing mapping reuse, partial TLS failure cleanup PASS");return 0;}
}
int main(){return recon::run();}
'''.replace('TLS_FUNCTION',a).replace('STACK_FUNCTION',b).replace('mm::paging::PageTable*','void*')
with tempfile.TemporaryDirectory(prefix="wos-thread-backing-") as directory:
    source_path = Path(directory) / "test.cpp"
    binary_path = Path(directory) / "test"
    source_path.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", str(source_path), "-o", str(binary_path)], check=True)
    environment = os.environ.copy()
    environment["ASAN_OPTIONS"] = "detect_leaks=0"
    subprocess.run([str(binary_path)], env=environment, check=True)
