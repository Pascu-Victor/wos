#!/usr/bin/env python3
"""Exercise actual stack/TLS backing with dirty allocator pages and failures.

Host endpoints replace physical allocation/PTE publication. This checks zeroing,
page ownership, staged rollback and preemption context, not hardware CR3 cost.
"""
import os
from pathlib import Path
import subprocess
import tempfile

from exec_trace_ownership_test import function, block

ROOT = Path(os.environ.get("WOS_THREAD_BACKING_TEST_ROOT", Path(__file__).resolve().parents[3]))
source = Path(os.environ.get("WOS_THREAD_BACKING_TEST_SOURCE", ROOT / "modules/kern/src/platform/sched/threading.cpp")).read_text()

allocation_header = (ROOT / "modules/kern/src/platform/mm/numa_alloc.hpp").read_text()
allocation_source = (ROOT / "modules/kern/src/platform/mm/numa_alloc.cpp").read_text()
allocation_types = allocation_header[allocation_header.index("struct AllocationContext {"):allocation_header.index("// Immutable managed-affinity lookup")]
owned_methods = allocation_source[allocation_source.index("OwnedSpan::~OwnedSpan()"):allocation_source.index("auto capture_allocation_context(")]
capture_body = block(allocation_source, r"^auto capture_allocation_context\([^{;]*\{")
into_body = block(allocation_source, r"^\[\[gnu::noinline\]\] void try_allocate_pages_into\([^{;]*\{")

a = function(source, "map_zeroed_user_range").replace("map_zeroed_user_range(", "actual_map_zeroed_user_range(", 1)
b = function(source, "back_stack_pages").replace("back_stack_pages(", "actual_back_stack_pages(", 1)
errors = "\n".join(function(source, name) for name in ("backing_allocation_error", "set_backing_error"))
code=r'''
#define WOS_SELFTEST 1
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <limits>
#include <map>
#include <utility>
#include <cerrno>
#include <platform/mm/numa_policy.hpp>
namespace recon {
int guard_depth=0,attempts=0,freed=0,fail_at=-1,allocator_zeroings=0;
std::array<bool,8> live{};std::array<uint64_t,8> mappings{};
alignas(4096) std::array<std::array<std::byte,4096>,8> pages{};
struct PreemptGuard{PreemptGuard(){++guard_depth;}~PreemptGuard(){--guard_depth;}};
namespace cpu {uint64_t current=0;auto current_cpu()->uint64_t{assert(guard_depth>0);return current;}};

namespace sched {using PreemptGuard=::recon::PreemptGuard;}
namespace acpi::numa {
using Topology=::ker::mod::acpi::numa::Topology;
Topology topology;
bool topology_present=true;
auto boot_topology()->const Topology* {return topology_present?&topology:nullptr;}
}
void init_topology(){acpi::numa::topology.node_count=2;acpi::numa::topology.cpu_count=2;
 acpi::numa::topology.cpu_nodes[0]=0;acpi::numa::topology.cpu_nodes[1]=1;}

namespace mm {
enum class PhysicalPageOwner {USER_THREAD_TLS,USER_THREAD_STACK};
namespace paging{constexpr uint64_t PAGE_SIZE=4096,PAGE_NX=1ULL<<63;namespace page_types {constexpr uint64_t USER=7;}}
namespace addr{auto get_phys_pointer(uint64_t p)->void*{return reinterpret_cast<void*>(p);}}
namespace phys{
enum class PageInitialization:uint8_t{ZERO,FULL_OVERWRITE};
auto allocate(PhysicalPageOwner owner)->void* {assert((guard_depth>0)==(owner==PhysicalPageOwner::USER_THREAD_STACK));auto n=attempts++;if(n==fail_at)return nullptr;for(size_t i=0;i<live.size();++i)if(!live[i]){live[i]=true;pages[i].fill(std::byte{0xa5});return pages[i].data();}assert(false);return nullptr;}
auto page_alloc_full_overwrite_may_fail(PhysicalPageOwner o,uint64_t sz,const char*)->void*{assert(sz==4096);return allocate(o);}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner o,const char*)->void*{return allocate(o);}
auto page_alloc_may_fail(PhysicalPageOwner o,uint64_t sz,const char*)->void*{assert(sz==4096);auto*p=allocate(o);if(p){std::memset(p,0,4096);++allocator_zeroings;}return p;}
auto page_alloc_with_reclaim_may_fail(PhysicalPageOwner o,uint64_t sz,const char*n)->void*{return page_alloc_may_fail(o,sz,n);}
void page_free(void*p){for(size_t i=0;i<live.size();++i)if(pages[i].data()==p){assert(live[i]);live[i]=false;++freed;return;}assert(false);}
}
namespace phys {void page_ref_dec(void*p){page_free(p);}}

namespace numa {
using Policy=::ker::mod::mm::numa::Policy;
using Mode=::ker::mod::mm::numa::Mode;
using NodeId=::ker::mod::mm::numa::NodeId;
using ::ker::mod::mm::numa::valid_policy;
constexpr NodeId INVALID_NODE=::ker::mod::mm::numa::INVALID_NODE;
constexpr auto MAX_NODES=::ker::mod::mm::numa::MAX_NODES;
ACTUAL_ALLOCATION_TYPES
ACTUAL_OWNED_METHODS
ACTUAL_CAPTURE_BODY
Policy expected_policy{};
NodeId expected_local{};
unsigned factory_calls{};
void (*after_allocate)()=nullptr;
std::map<void*,uint64_t> destinations;
auto try_allocate_pages(const AllocationContext& c,uint32_t count,uint64_t alignment,PhysicalPageOwner owner,
                        phys::PageInitialization initialization,ReclaimMode reclaim,const char*)->AllocationResult {
 assert(c.policy==expected_policy&&c.local_node==expected_local);
 assert(count==1&&alignment==4096&&initialization==phys::PageInitialization::FULL_OVERWRITE);
 assert(reclaim==(owner==PhysicalPageOwner::USER_THREAD_STACK?ReclaimMode::NEVER:ReclaimMode::LEGACY_UMA));
 ++factory_calls;AllocationResult result;
 void* page=phys::allocate(owner);
 if(!page){result.error=AllocationError::EXHAUSTED;return result;}
 destinations[page]=c.virtual_page_index*4096;
 result.span=OwnedSpan{page,1};result.requested=result.actual=c.local_node;result.error=AllocationError::NONE;
 if(after_allocate)after_allocate();return result;
}
ACTUAL_INTO_BODY
}

namespace virt{
constexpr uint64_t PADDR_INVALID=UINT64_MAX;
auto translate(void*,uint64_t v)->uint64_t{assert(v>=0x10000&&v<0x18000);return mappings[(v-0x10000)/4096]?mappings[(v-0x10000)/4096]:PADDR_INVALID;}
void map_page(void*,uint64_t v,uint64_t p,uint64_t flags){assert(flags==(paging::page_types::USER|paging::PAGE_NX));auto*b=reinterpret_cast<std::byte*>(p);for(size_t i=0;i<4096;++i)assert(b[i]==std::byte{0});auto idx=(v-0x10000)/4096;assert(!mappings[idx]);assert(numa::destinations.at(reinterpret_cast<void*>(p))==v);mappings[idx]=p;}
}
}
ERROR_FUNCTIONS
TLS_FUNCTION
STACK_FUNCTION
mm::numa::AllocationContext saved_context{};
auto back_stack_pages(void*root,uint64_t lo,uint64_t hi,int failure=-1)->bool {
 int error=0;auto ok=actual_back_stack_pages(root,lo,hi,saved_context,&error,failure);
 assert(ok?error==0:error==-ENOMEM);return ok;
}
auto map_zeroed_user_range(void*root,uint64_t start,uint64_t size,const char*name)->bool {
 int error=0;auto ok=actual_map_zeroed_user_range(root,start,size,name,saved_context,&error);
 assert(ok?error==0:error==-ENOMEM);return ok;
}
void reset(){assert(!guard_depth);live.fill(false);mappings.fill(0);attempts=freed=0;fail_at=-1;}
size_t mapped(){return std::count_if(mappings.begin(),mappings.end(),[](auto p){return p!=0;});}
int run(){init_topology();saved_context.policy=mm::numa::expected_policy;saved_context.local_node=mm::numa::expected_local;void*const root=reinterpret_cast<void*>(1);
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
'''.replace('ERROR_FUNCTIONS', errors).replace('TLS_FUNCTION',a).replace('STACK_FUNCTION',b).replace('mm::paging::PageTable*','void*').replace("ACTUAL_ALLOCATION_TYPES", allocation_types).replace("ACTUAL_OWNED_METHODS", owned_methods).replace("ACTUAL_CAPTURE_BODY", capture_body).replace("ACTUAL_INTO_BODY", into_body)
with tempfile.TemporaryDirectory(prefix="wos-thread-backing-") as directory:
    source_path = Path(directory) / "test.cpp"
    binary_path = Path(directory) / "test"
    source_path.write_text(code)
    subprocess.run(["/usr/bin/clang++", "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-I", str(ROOT / "modules/kern/src"), str(source_path), "-o", str(binary_path)], check=True, timeout=60)
    environment = os.environ.copy()
    environment["ASAN_OPTIONS"] = "detect_leaks=0"
    subprocess.run([str(binary_path)], env=environment, check=True, timeout=20)
