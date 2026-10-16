#!/usr/bin/env python3
"""Execute actual lazy anonymous/file backing and allocator preparation bodies.

Dirty host pages, IO, PTEs and lock endpoints are controlled models. They verify
zero-before-IO/publication, ownership and failure/revalidation order; they do
not emulate hardware fault scheduling, reclaim or measure guest CR3 latency.
"""
import os
from pathlib import Path
import subprocess
import tempfile
from raw_typed_file_seams import extend_raw_fixture
from exec_trace_ownership_test import block, function

ROOT = Path(__file__).resolve().parents[3]
# A detached test can override ROOT without relocating imported extraction code.
ROOT = Path(os.environ.get("WOS_LAZY_BACKING_ROOT", ROOT))
virt = Path(os.environ.get("WOS_LAZY_BACKING_VIRT_SOURCE", ROOT / "modules/kern/src/platform/mm/virt.opt.cpp")).read_text()
vmem = Path(os.environ.get("WOS_LAZY_BACKING_VMEM_SOURCE", ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp")).read_text()
phys = (ROOT / "modules/kern/src/platform/mm/phys.opt.cpp").read_text()
allocator = (ROOT / "modules/kern/src/platform/mm/numa_alloc.cpp").read_text()
allocator_header = (ROOT / "modules/kern/src/platform/mm/numa_alloc.hpp").read_text()
demand_virt = (ROOT / "modules/kern/src/platform/mm/virt.opt.cpp").read_text()
virt_header = (ROOT / "modules/kern/src/platform/mm/virt.hpp").read_text()
code = r'''
#define WOS_KASAN 1
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <memory>
#include <functional>
#include <limits>
#include <new>
#include <string_view>
#include <utility>
#include <vector>
using vaddr_t=uint64_t;using paddr_t=uint64_t;
namespace model {
bool expected_stack_size(size_t);
std::vector<int> locks;
alignas(4096) std::array<uint8_t,4096> bytes;
bool eager=false,cacheable=true,lazy_enabled=false,fail_capture=false,fail_admit=false;int pub=0,reservations=0,released_reservations=0,rolled_back=0,registered=0,register_error=0,capture_calls=0;
bool live=false,unpoisoned=false,fail_alloc=false,fail_read=false,share=true,fail_cache=false;
uint64_t observed_nodes=0,observed_index=0;unsigned observed_mode=0;uint16_t observed_local=0;
int attempts=0,reclaim_calls=0,zero_calls=0,cr3_writes=0,unpoison_calls=0,drops=0,reads=0,caches=0,installs=0,tlbs=0;
uint64_t mapping=0,current_cr3=0x111,expected_flags=0,read_offset=0;
size_t read_size=0;
std::function<void()> allocation_hook,read_hook;
void expect_zero(){assert(live&&unpoisoned);for(auto b:bytes)assert(b==0);}
void reset(){assert(locks.empty()&&pub==0);eager=lazy_enabled=fail_capture=fail_admit=false;cacheable=true;reservations=released_reservations=rolled_back=registered=register_error=capture_calls=0;live=unpoisoned=fail_alloc=fail_read=fail_cache=false;share=true;
 attempts=reclaim_calls=zero_calls=cr3_writes=unpoison_calls=drops=reads=caches=installs=tlbs=0;
 mapping=expected_flags=read_offset=read_size=0;current_cr3=0x111;allocation_hook=read_hook={};bytes.fill(0xa5);}
uint8_t data(uint64_t offset){return static_cast<uint8_t>((offset*37+19)%251+1);}
void check_data(uint64_t offset,size_t size){assert(live&&unpoisoned);for(size_t i=0;i<4096;++i)assert(bytes[i]==(i<size?data(offset+i):0));}
struct Lock{int id;uint64_t lock_irqsave(){assert(std::find(locks.begin(),locks.end(),id)==locks.end());
 if(id==1)assert(locks.empty());else assert(locks.empty()||locks.back()==1);locks.push_back(id);return id;}
 void unlock_irqrestore(uint64_t f){assert(f==static_cast<unsigned>(id)&&!locks.empty()&&locks.back()==id);locks.pop_back();}};
}
namespace ker {
namespace abi::vmem{constexpr uint64_t PROT_WRITE=2,MAP_FIXED=16;constexpr int VMEM_EFAULT=EFAULT,VMEM_EINVAL=EINVAL,VMEM_EOVERFLOW=EOVERFLOW,VMEM_ENOMEM=ENOMEM;}
namespace vfs {
enum class FSType{SOCKET,TMPFS};struct File{int refs=1;int open_flags=0;FSType fs_type=FSType::SOCKET;void*fops=nullptr;void*mount_owner=nullptr;};struct Timespec{int64_t tv_sec=0,tv_nsec=0;};
struct Stat{uint64_t st_dev=0,st_ino=0;off_t st_size=0;Timespec st_mtim,st_ctim;uint64_t st_mode=0;};constexpr uint64_t S_IFREG=0100000,S_IFMT=0170000;
namespace tmpfs{void*get_tmpfs_fops(){return nullptr;}}
bool file_mount_read_only(const File&){return false;}
void vfs_retain_file(File*f){assert(!model::locks.empty()&&model::locks.back()==2&&f->refs>0);++f->refs;}
void vfs_put_file(File*f){assert(model::locks.empty()&&f->refs>1);--f->refs;}
}
namespace mod {
namespace time{uint64_t get_us(){return 17;}}
namespace perf{enum class WkiPerfLocalVmemOp{FILE_MMAP};enum class WkiPerfPhase{END};}

namespace sched{struct PreemptGuard{PreemptGuard(){assert(model::locks.empty());}~PreemptGuard(){assert(model::locks.empty());}};}
namespace sched::task {
enum class LazyVmemKind{ANONYMOUS,FILE_BACKED};
struct LazyVmemRange{uint64_t start=0x10000,end=0x12000,prot=3,flags=0x20;LazyVmemKind kind=LazyVmemKind::ANONYMOUS;
 ker::vfs::File*file=nullptr;uint64_t file_offset=0,file_dev=3,file_ino=7,file_size=0;
 int64_t file_mtime_sec=13,file_mtime_nsec=17,file_ctime_sec=19,file_ctime_nsec=23;};
enum class TaskType{PROCESS};
struct Task{TaskType type=TaskType::PROCESS;const char*name="fixture";uint64_t policy_nodes=0;unsigned policy_mode=0;
 ker::mod::mm::numa::Policy memory_policy_snapshot(){assert(model::pub==0&&model::locks.empty());return {policy_nodes,static_cast<ker::mod::mm::numa::Mode>(policy_mode)};}
 struct Retirement{void*load(std::memory_order)const{return nullptr;}}native_file_mapping_retirement;
 void*pagemap=reinterpret_cast<void*>(1);uint64_t pid=42;model::Lock lazy_vmem_lock{2};std::vector<LazyVmemRange>lazy_vmem_ranges;};
}
namespace sched::resource{struct AddressSpaceObservation{template<class T>explicit AddressSpaceObservation(T*){}
 bool admit_mapping(void*,uint64_t,uint64_t=0,uint64_t=0){return !model::fail_admit;}void commit_mapping(){}};}
namespace security{int authorize_file_mapping(unsigned,bool,bool,bool){return 0;}}
namespace mm {
enum class PhysicalPageOwner{USER_PRIVATE_MAPPING,USER_FILE_MAPPING};
namespace paging{constexpr uint64_t PAGE_SIZE=4096;struct PageFault{unsigned writable=0;};}
namespace addr{auto get_phys_pointer(uint64_t p)->void*{return reinterpret_cast<void*>(p);}}
namespace kasan{
bool is_enabled(){return true;}bool in_shadow_fault(){return false;}
void unpoison_range(void*p,uint64_t s){if(p==model::bytes.data()){assert(s==4096&&model::live);model::unpoisoned=true;++model::unpoison_calls;}
 else assert(s==sizeof(sched::task::LazyVmemRange)||model::expected_stack_size(s));}
}
namespace phys {
enum class PageInitialization:uint8_t{ZERO,FULL_OVERWRITE};
constexpr uint32_t PAGE_ALLOC_RECLAIM_RETRY_DEFAULT=8;
uint64_t kernel_cr3=0x222;
uint64_t rdcr3(){return model::current_cr3;}void wrcr3(uint64_t c){model::current_cr3=c;++model::cr3_writes;}
enum class ReturnedPageZeroing:uint8_t{ZERO,FULL_OVERWRITE};
PREPARE
void* page_alloc_impl(PhysicalPageOwner o,uint64_t s,std::string_view n,ReturnedPageZeroing z,void*,bool fatal){
 assert(model::locks.empty()&&s==4096&&!fatal);
 assert((o==PhysicalPageOwner::USER_PRIVATE_MAPPING&&n=="lazy-vmem")||(o==PhysicalPageOwner::USER_FILE_MAPPING&&(n=="vmem-file-lazy"||n=="vmem-file")));
 ++model::attempts;if(model::allocation_hook)model::allocation_hook();if(model::fail_alloc)return nullptr;
 assert(!model::live);model::live=true;model::bytes.fill(0xa5);prepare_allocated_block(model::bytes.data(),s,z);
 if(z==ReturnedPageZeroing::ZERO){++model::zero_calls;model::expect_zero();}else for(auto b:model::bytes)assert(b==0xa5);
 assert(model::current_cr3==0x111&&model::unpoisoned);return model::bytes.data();}
void* page_alloc_with_reclaim_impl(PhysicalPageOwner o,uint64_t s,std::string_view n,ReturnedPageZeroing z,void*caller,uint32_t retries,bool fatal){
 assert(o==PhysicalPageOwner::USER_FILE_MAPPING&&retries==8);++model::reclaim_calls;return page_alloc_impl(o,s,n,z,caller,fatal);}
WRAPPERS
void page_ref_dec(void*p){assert(p==model::bytes.data()&&model::live&&model::locks.empty());model::live=false;++model::drops;}
}
namespace numa {
NUMA_DECLARATIONS
NUMA_METHODS
// Fixed RAW/default endpoint only. Actual policy selection/reclaim dispatch is
// independently compiled in numa_alloc_test; no second placement model here.
auto try_allocate_pages(const AllocationContext& context,uint32_t pages,uint64_t alignment,PhysicalPageOwner owner,
                        phys::PageInitialization initialization,ReclaimMode reclaim,const char*name)->AllocationResult{
 assert(model::locks.empty()&&pages==1&&alignment==4096);
 model::observed_nodes=context.policy.nodes;model::observed_mode=static_cast<unsigned>(context.policy.mode);
 model::observed_local=context.local_node;model::observed_index=context.virtual_page_index;
 assert(initialization==phys::PageInitialization::FULL_OVERWRITE||(model::eager&&initialization==phys::PageInitialization::ZERO));
 assert((owner==PhysicalPageOwner::USER_PRIVATE_MAPPING&&reclaim==ReclaimMode::NEVER)||
        (owner==PhysicalPageOwner::USER_FILE_MAPPING&&reclaim==ReclaimMode::LEGACY_UMA));
 AllocationResult result;void*page=owner==PhysicalPageOwner::USER_FILE_MAPPING?
 (initialization==phys::PageInitialization::ZERO?phys::page_alloc_with_reclaim_may_fail(owner,4096,name):
 phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(owner,name)):phys::page_alloc_full_overwrite_page_may_fail(owner,name);
 if(!page){result.error=AllocationError::EXHAUSTED;return result;}
 result.span=OwnedSpan{page,1};result.requested=result.actual=0;result.error=AllocationError::NONE;return result;
}
auto capture_allocation_context(Policy policy,AllocationContext&out) noexcept->AllocationError{
 assert(model::locks.empty()&&model::pub==0);++model::capture_calls;if(model::fail_capture)return AllocationError::INVALID_POLICY;
 out={policy,3,0};return AllocationError::NONE;
}
NUMA_ADAPTER
}
namespace virt {
using namespace ker::mod;
DEMAND_TYPES
DEMAND_STORAGE
void record_demand_candidate(const DemandContext&,bool){assert(model::locks.empty());}
bool demand_is_observed(const DemandContext&){assert(model::locks.empty());return false;}
void record_demand_result(const DemandContext&,DemandBacking,numa::NodeId,numa::NodeId,bool,bool,bool){assert(model::locks.empty());}
constexpr uint64_t PADDR_INVALID=UINT64_MAX;
struct PageMapBatch{};void init_page_map_batch(PageMapBatch*,void*,uint64_t){}void flush_page_map_batch(PageMapBatch*){}
enum class LazyFilePageInstallResult{MAPPED,ALREADY_MAPPED,STALE_RANGE};
model::Lock cow_pte_lock{1};
// This harness has no swap records; genuine saved-byte preservation is native.
struct AnonymousSwapMapping;
auto find_anonymous_swap_mapping_locked(void*,uint64_t)->AnonymousSwapMapping*{
 assert(!model::locks.empty()&&model::locks.front()==1);return nullptr;
}
uint64_t translate(void*,uint64_t){assert(!model::locks.empty()&&model::locks.front()==1);return model::mapping?model::mapping:PADDR_INVALID;}
void map_page(void*,uint64_t,uint64_t p,uint64_t flags){assert((model::eager?(model::pub==1&&model::locks.empty()):(!model::locks.empty()&&model::locks.front()==1))&&!model::mapping&&model::live&&model::unpoisoned);
 assert(p==reinterpret_cast<uint64_t>(model::bytes.data())&&flags==model::expected_flags);
 model::mapping=p;++model::installs;}
void invalidate_local_tlb_if_current(void*,uint64_t,bool all){assert(model::locks.empty()&&!all);++model::tlbs;}
uint64_t lazy_user_vmem_flags(uint64_t p){model::expect_zero();return p;}
void log_lazy_vmem_fault_state(sched::task::Task*,uint64_t,const paging::PageFault&,const char*,uint64_t,uint64_t){assert(model::locks.empty());}
namespace log{template<class...T>void error(const char*,T...){assert(model::locks.empty());}}
ANON_REVALIDATE
FILE_MATCH
INSTALL
}
}
}
namespace syscall::vmem {
namespace numa=ker::mod::mm::numa;
namespace log{template<class...T>void warn(const char*,T...){} }
using LazyVmemRange=ker::mod::sched::task::LazyVmemRange;using LazyVmemKind=ker::mod::sched::task::LazyVmemKind;
bool file_mmap_can_share(const ker::vfs::Stat&st,uint64_t){assert(st.st_dev==3&&st.st_ino==7&&st.st_mtim.tv_sec==13&&st.st_mtim.tv_nsec==17&&st.st_ctim.tv_sec==19&&st.st_ctim.tv_nsec==23&&st.st_mode==ker::vfs::S_IFREG);return model::share;}
bool read_file_mapping_exact(ker::vfs::File*f,void*p,size_t n,uint64_t off){assert(f&&f->refs>=1&&model::locks.empty()&&p==model::bytes.data());
 model::expect_zero();++model::reads;assert(n==model::read_size&&off==model::read_offset);
 if(model::read_hook)model::read_hook();for(size_t i=0;i<(model::fail_read?n/2:n);++i)model::bytes[i]=model::data(off+i);return !model::fail_read;}
bool file_mmap_cached_page_for_file(ker::vfs::File*f,const ker::vfs::Stat&st,uint64_t off,void**out){
 assert(model::locks.empty()&&f&&off==model::read_offset);++model::caches;if(model::fail_cache)return false;
 assert(!model::live);model::live=model::unpoisoned=true;model::bytes.fill(0);
 size_t n=off<static_cast<uint64_t>(st.st_size)?std::min<uint64_t>(4096,static_cast<uint64_t>(st.st_size)-off):0;
 for(size_t i=0;i<n;++i)model::bytes[i]=model::data(off+i);*out=model::bytes.data();return true;}
uint64_t file_mapping_page_flags(uint64_t p,uint64_t f){if(!model::eager)model::check_data(model::read_offset,model::read_size);return p|(f<<8);}
// Eager entrypoint shell: real admission/IO/owner/publication body below;
// these scalar endpoints model one fixed admitted descriptor and one page.
constexpr uint64_t USER_SPACE_START=0x10000,USER_SPACE_END=0x80000000;
uint64_t page_align_up(uint64_t n){return (n+4095)&~4095ULL;}
ker::mod::sched::task::Task*get_current_task(){return nullptr;}
bool file_mapping_offset_range_valid(uint64_t o,uint64_t n){return o<=UINT64_MAX-n;}
bool lazy_file_mmap_enabled(){return model::lazy_enabled;}
struct SharedVmemPublicationGuard{SharedVmemPublicationGuard(){assert(model::pub++==0);}~SharedVmemPublicationGuard(){assert(--model::pub==0);}};
struct RetainedEndpoint{ker::vfs::File* file{};ker::vfs::Stat stat;};RetainedEndpoint endpoint;
uint64_t allocate_public_shared_tmpfs(ker::mod::sched::task::Task&,ker::vfs::File&,const ker::vfs::Stat&,uint64_t,uint64_t,uint64_t,uint64_t,uint64_t){std::abort();}
bool overlaps_kernel_managed_user_layout_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t){return false;}
bool public_shared_file_overlap(ker::mod::sched::task::Task&,uint64_t,uint64_t){return false;}
uint64_t reserve_free_mmap_range_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t,uint64_t&va){assert(model::pub==1);++model::reservations;va=0x10000;return 0;}
void release_fixed_mmap_range_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t){}
void release_mmap_reservation_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t,bool owned){assert(model::pub==1);if(owned)++model::released_reservations;}
bool add_shared_lazy_file_vmem_range_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t,uint64_t,uint64_t,ker::vfs::File*,uint64_t,const ker::vfs::Stat&){return true;}
bool remove_shared_vmem_range_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t){return true;}
int register_file_mmap_range(void*,uint64_t,uint64_t,uint64_t,ker::vfs::File*,bool){++model::registered;return model::register_error;}
void advance_shared_mmap_cursor_locked(ker::mod::sched::task::Task*,uint64_t,uint64_t){}
void rollback_mapped_pages(ker::mod::sched::task::Task*,uint64_t,uint64_t n){assert(model::pub==1);model::rolled_back+=n;if(n){assert(n==1&&model::mapping);model::mapping=0;ker::mod::mm::phys::page_ref_dec(model::bytes.data());}}
bool map_file_mmap_cached_page(ker::mod::mm::virt::PageMapBatch*,ker::vfs::File*f,const ker::vfs::Stat&st,uint64_t va,uint64_t off,uint64_t flags,uint64_t&pa){
 void*p=nullptr;if(!file_mmap_cached_page_for_file(f,st,off,&p))return false;pa=reinterpret_cast<uint64_t>(p);ker::mod::mm::virt::map_page(nullptr,va,pa,flags);return true;}
bool is_watched_mmap_vaddr(uint64_t){return false;}
template<class...T>void record_local_vmem_event(T...){}
uint64_t vmem_latency_since(uint64_t){return 0;}
}
namespace vfs {
int vfs_get_file_retain_checked(ker::mod::sched::task::Task*,int fd,File*&out){assert(fd==5&&ker::syscall::vmem::endpoint.file);out=ker::syscall::vmem::endpoint.file;++out->refs;return 0;}
int vfs_fstat_file(File*,Stat*out){*out=ker::syscall::vmem::endpoint.stat;return 0;}
}
namespace syscall::vmem {
namespace numa=ker::mod::mm::numa;
FILE_ALLOCATE
FILE_MATERIALIZE
// Explicit default context for direct legacy RAW vectors only.
bool materialize_lazy_file_page_impl(ker::mod::sched::task::Task*t,const LazyVmemRange&r,uint64_t p,const ker::mod::mm::paging::PageFault&f){
 ker::mod::mm::numa::AllocationContext context;context.local_node=0;return materialize_lazy_file_page_impl(t,r,p,f,context);}
bool materialize_lazy_file_page(ker::mod::sched::task::Task*t,const LazyVmemRange&r,uint64_t p,const ker::mod::mm::paging::PageFault&f,const ker::mod::mm::numa::AllocationContext&c){return materialize_lazy_file_page_impl(t,r,p,f,c);}
}
namespace mod::mm::virt {
FAULT
bool exercise_lazy_fault(sched::task::Task*task,uint64_t address,const paging::PageFault&fault){
 DemandContextStorage storage;auto&demand=storage.get();demand.allocation.local_node=0;
 return handle_lazy_vmem_fault(task,address,fault DEMAND_ARGUMENT);
}
}
}
namespace model {
using namespace ker::mod;using namespace ker::mod::sched::task;
bool expected_stack_size(size_t size){return size==sizeof(mm::virt::DemandContext)||size==sizeof(mm::numa::AllocationResult);}
void final_alloc_check(){assert(locks.empty()&&current_cr3==0x111);
#ifdef FUNCTIONAL_ONLY
 assert(cr3_writes==2*zero_calls);
#else
 assert(zero_calls==0&&cr3_writes==0);
#endif
}
Task make_task(const LazyVmemRange&r){Task t;t.lazy_vmem_ranges.push_back(r);return t;}
int run(){
 using mm::paging::PageFault;using mm::virt::exercise_lazy_fault;
 LazyVmemRange anon;ker::vfs::File file;LazyVmemRange fr=anon;fr.kind=LazyVmemKind::FILE_BACKED;fr.file=&file;fr.flags=2;
 // Null/outside/protection failures must not allocate or retain a file.
 for(bool file_kind:{false,true})for(unsigned w:{0U,1U})for(uint64_t prot:{0ULL,1ULL,3ULL}){
  reset();auto r=file_kind?fr:anon;r.prot=prot;auto t=make_task(r);PageFault f{w};expected_flags=prot|(file_kind?r.flags<<8:0);
  assert(!exercise_lazy_fault(nullptr,0x10000,f));t.pagemap=nullptr;assert(!exercise_lazy_fault(&t,0x10000,f));t.pagemap=reinterpret_cast<void*>(1);
  assert(!exercise_lazy_fault(&t,0xfff,f)&&!exercise_lazy_fault(&t,r.end,f));
  if(prot==0||(prot==1&&w)){assert(!exercise_lazy_fault(&t,0x10001,f)&&attempts==0&&caches==0);}
  else{assert(exercise_lazy_fault(&t,0x10001,f));expect_zero();assert(installs==1&&file.refs==1);assert(file_kind?(prot==1?caches==1&&reclaim_calls==0:reclaim_calls==1):reclaim_calls==0);}
  final_alloc_check();
 }
 for(int mode=0;mode<6;++mode){reset();auto t=make_task(anon);expected_flags=3;PageFault f{1};
  if(mode==1)fail_alloc=true;
  if(mode>=2)allocation_hook=[&]{if(mode==2)t.lazy_vmem_ranges.clear();if(mode==3)t.lazy_vmem_ranges[0].prot=1;if(mode==4)mapping=0xdead;if(mode==5){t.lazy_vmem_ranges[0].prot=1;expected_flags=1;}};
  if(mode==5)f.writable=0;
  bool ok=exercise_lazy_fault(&t,0x1000f,f);
  assert(ok==(mode==0||mode==4||mode==5));assert(attempts==1&&reclaim_calls==0);
  assert(drops==((mode>=2&&mode<=4)?1:0));assert(installs==((mode==0||mode==5)?1:0));assert(tlbs==(mode==4?1:0));
  if(installs)expect_zero();final_alloc_check();
 }
 // Full pages, every EOF tail length and pages beyond EOF. IO sees zeroed bytes.
 for(size_t n=0;n<=4096;++n){reset();auto r=fr;r.file_offset=83;r.file_size=83+n;read_offset=83;read_size=n;expected_flags=r.prot|(r.flags<<8);auto t=make_task(r);
  assert(exercise_lazy_fault(&t,0x10003,PageFault{1}));check_data(83,n);assert(installs==1&&attempts==1&&reclaim_calls==1&&reads==(n?1:0)&&file.refs==1);final_alloc_check();}
 for(int mode=0;mode<8;++mode){reset();auto r=fr;r.file_size=1000;read_size=1000;expected_flags=r.prot|(r.flags<<8);auto t=make_task(r);
  if(mode==0)fail_alloc=true;if(mode==1)fail_read=true;
  if(mode==2)read_hook=[&]{t.lazy_vmem_ranges.clear();};
  if(mode==3)read_hook=[&]{t.lazy_vmem_ranges[0].prot=1;};
  if(mode==4)read_hook=[&]{t.lazy_vmem_ranges[0].file_ino=8;};
  if(mode==5)read_hook=[&]{mapping=0xbeef;};
  if(mode==6){r.file_offset=static_cast<uint64_t>(std::numeric_limits<off_t>::max())+1;r.file_size=r.file_offset+1000;read_offset=r.file_offset;t.lazy_vmem_ranges[0]=r;}
  if(mode==7){r.file_offset=UINT64_MAX-5;t.lazy_vmem_ranges[0]=r;assert(!ker::syscall::vmem::materialize_lazy_file_page_impl(&t,r,0x11000,PageFault{}));assert(attempts==0);final_alloc_check();continue;}
  bool ok=exercise_lazy_fault(&t,0x10000,PageFault{1});assert(ok==(mode==5));
  assert(installs==0&&drops==(mode==0?0:1)&&attempts==1&&file.refs==1&&tlbs==(mode==5?1:0));final_alloc_check();
 }
 // Unchanged readonly cache ownership and install/discard branches.
 for(int mode=0;mode<5;++mode){reset();auto r=fr;r.prot=1;r.file_size=123;read_size=123;expected_flags=r.prot|(r.flags<<8);auto t=make_task(r);
  if(mode==1)fail_cache=true;if(mode==2)mapping=0xcafe;if(mode==3){share=false;fail_read=true;}if(mode==4)share=false;
  bool ok=exercise_lazy_fault(&t,0x10000,PageFault{});assert(ok==(mode==0||mode==2||mode==4));assert(file.refs==1);
  assert(caches==(mode<3?1:0)&&attempts==(mode>=3?1:0)&&reclaim_calls==(mode>=3?1:0));
  assert(drops==((mode==2||mode==3)?1:0)&&tlbs==(mode==2?1:0));if(installs)check_data(0,123);final_alloc_check();
 }
 reset();auto t=make_task(fr);assert(!ker::syscall::vmem::materialize_lazy_file_page_impl(&t,fr,0x10001,PageFault{}));assert(attempts==0&&caches==0);
 // The extracted materializer forwards captured placement and destination VA,
 // even for legacy shared-copy and readonly noncacheable data. This endpoint
 // observes factory arguments; real policy selection has its own factory test.
 for(auto mode:{mm::numa::Mode::BIND,mm::numa::Mode::INTERLEAVE})for(uint64_t flags:{1ULL,2ULL}){
  reset();auto r=fr;r.start=0x10000;r.end=0x14000;r.flags=flags;r.file_offset=83;r.file_size=83+4096;
  auto t=make_task(r);read_offset=83+4096;read_size=0;expected_flags=r.prot|(r.flags<<8);
  mm::numa::AllocationContext context{{10,mode},3,999};
  assert(ker::syscall::vmem::materialize_lazy_file_page_impl(&t,r,0x11000,PageFault{},context));
  assert(observed_nodes==10&&observed_mode==static_cast<unsigned>(mode)&&observed_local==3&&observed_index==17);
  assert(installs==1&&attempts==1&&reclaim_calls==1&&drops==0&&context.virtual_page_index==999);final_alloc_check();
 }
 // Real eager body: pre-PUB actor capture, canonical cache unchanged,
 // direct and cache-fallback copies, legacy shared registration/rollback,
 // failure before publication, exact byte/VA arguments and candidate ref.
 for(int mode=0;mode<9;++mode){
  reset();eager=true;share=mode==2||mode==3;fail_cache=mode==3;fail_capture=mode==4;
  fail_alloc=mode==5;fail_read=mode==6;fail_admit=mode==7;register_error=mode==8?-EIO:0;
  read_offset=83;read_size=1000;expected_flags=3|(1<<8);auto t=make_task(fr);t.policy_nodes=10;t.policy_mode=2;
  ker::syscall::vmem::endpoint.file=&file;auto&st=ker::syscall::vmem::endpoint.stat;st={};
  st.st_dev=3;st.st_ino=7;st.st_mode=ker::vfs::S_IFREG;st.st_size=1083;
  st.st_mtim={13,17};st.st_ctim={19,23};
  if(mode==1)lazy_enabled=true;
  uint64_t result=ker::syscall::vmem::file_allocate(&t,0,4096,3,1,5,83);
  bool success=mode<4;assert(result==(success?0x10000ULL:static_cast<uint64_t>(mode==4?-EINVAL:mode==5||mode==7?-ENOMEM:mode==6?-EFAULT:-EIO)));
  assert(file.refs==1&&pub==0&&capture_calls==(mode==1?0:1));
  if(mode==1)assert(attempts==0&&mapping==0&&reservations==1);
  if(mode==0||mode==3){assert(observed_nodes==10&&observed_mode==2&&observed_local==3&&observed_index==16);check_data(83,1000);assert(installs==1&&drops==0);}
  if(mode==2)assert(caches==1&&attempts==0&&mapping);
  if(mode>=4)assert(!mapping&&!live&&drops==(mode==6||mode==8?1:0));
  if(mode==4)assert(reservations==0);if(mode==8)assert(rolled_back==1&&registered==1);
 }
 std::puts("Actual lazy-page backing PASS: dirty pages, every EOF tail, zero-before-IO/publication, failures and partial reads, protection/metadata races, competing mappings, refs/lock order, actual allocator preparation/wrappers/KASan/default reclaim");return 0;
}
}
int main(){return model::run();}
'''
# Compile actual typed carriers/owners/storage, with scalar demand counters and
# the fixed default allocation endpoint disclosed above. Old --virt source
# overrides keep their real function body/signature through the explicit wrapper.
namespace_start = allocator_header.index('namespace ker::mod::mm::numa {') + len('namespace ker::mod::mm::numa {')
numa_declarations = allocator_header[namespace_start:allocator_header.rindex('}  // namespace ker::mod::mm::numa')]
# Only the real owner methods belong to this backing fixture. CPU/context
# capture is exercised by the actual factory target, not this fixed endpoint.
numa_methods = allocator[allocator.index('OwnedSpan::~OwnedSpan()'):allocator.index('auto capture_allocation_context(')]
demand_storage = demand_virt[demand_virt.index('struct DemandContext {'):demand_virt.index('struct DemandCounters {')]
demand_types = '\n'.join(block(virt_header, rf'^enum class {name}[^;]*\{{') + ';'
                         for name in ['DemandOrigin', 'DemandBacking'])
actual_fault = function(virt, 'handle_lazy_vmem_fault')
demand_argument = ', demand' if 'DemandContext' in actual_fault[:actual_fault.index('{')] else ''
code = '#include "' + str(ROOT / 'modules/kern/src/platform/mm/numa_policy.hpp') + '"\n' + code
code = extend_raw_fixture(code, 'model::Lock user_mapping_pin_lock{3};')
replacements = {
    'PREPARE': function(phys, 'prepare_allocated_block'),
    'WRAPPERS': '\n'.join(function(phys, n) for n in ['page_alloc_may_fail', 'page_alloc_full_overwrite_may_fail', 'page_alloc_full_overwrite_page_may_fail', 'page_alloc_with_reclaim_may_fail', 'page_alloc_full_overwrite_page_with_reclaim_may_fail']),
    'ANON_REVALIDATE': function(virt, 'anonymous_lazy_range_allows_fault'),
    'FILE_MATCH': function(virt, 'lazy_file_range_matches'),
    'INSTALL': function(virt, 'install_lazy_file_page_if_current'),
    'FILE_ALLOCATE': function(vmem, 'file_allocate'),
    'FILE_MATERIALIZE': function(vmem, 'materialize_lazy_file_page_impl'),
    'FAULT': actual_fault,
    'NUMA_DECLARATIONS': numa_declarations,
    'NUMA_METHODS': numa_methods,
    'NUMA_ADAPTER': block(allocator, r'^\[\[gnu::noinline\]\] void try_allocate_pages_into\([^;{]*\{'),
    'DEMAND_TYPES': demand_types,
    'DEMAND_STORAGE': demand_storage,
}
# C++ default arguments are in phys.hpp, outside the extracted definitions.
replacements['WRAPPERS'] = replacements['WRAPPERS'].replace('uint32_t retry_count)', 'uint32_t retry_count = PAGE_ALLOC_RECLAIM_RETRY_DEFAULT)')
code = code.replace('DEMAND_ARGUMENT', demand_argument)
for name, text in replacements.items():
    code = code.replace('\n' + name + '\n', '\n' + text + '\n')
with tempfile.TemporaryDirectory(prefix='wos-lazy-page-backing-') as directory:
    source = Path(directory) / 'test.cpp'
    binary = Path(directory) / 'test'
    source.write_text(code)
    flags = ['-DFUNCTIONAL_ONLY'] if os.environ.get('WOS_LAZY_BACKING_FUNCTIONAL_ONLY') == '1' else []
    subprocess.run([os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++'), '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined', *flags,
                    '-I', str(ROOT / 'modules/kern/src'), '-I', str(ROOT / 'modules'), str(source), '-o', str(binary)], check=True)
    environment = os.environ.copy()
    environment['ASAN_OPTIONS'] = 'detect_leaks=0'
    subprocess.run([str(binary)], env=environment, check=True)
