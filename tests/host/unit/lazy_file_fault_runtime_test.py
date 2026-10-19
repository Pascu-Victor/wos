#!/usr/bin/env python3
"""Execute production file-fault admission, worker and final materialization.

The shared striped-reader atomic body, file probe and handler branch are real.
Scheduler/physical/File lifetime boundaries are controlled. This verifies
ordering and ownership, not real IRET/timer progress or guest latency.
WOS_LAZY_FAULT_PROBE_CONTROL=predecessor with
WOS_LAZY_FAULT_PREDECESSOR_SOURCE selects the complete actual old probe/handler
against the SAME new oracle; it must compile then fail. retry-all-failures is a
separate deliberately indiscriminate mutation that must fail negative cases.
"""
import os
import resource
from pathlib import Path
import subprocess
import tempfile
from raw_typed_file_seams import extend_raw_fixture

from exec_trace_ownership_test import block, function

ROOT = Path(os.environ.get("WOS_LAZY_BACKING_ROOT", Path(__file__).resolve().parents[3]))
virt = Path(os.environ.get("WOS_LAZY_BACKING_VIRT_SOURCE", ROOT / "modules/kern/src/platform/mm/virt.opt.cpp")).read_text()
vmem = Path(os.environ.get("WOS_LAZY_BACKING_VMEM_SOURCE", ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp")).read_text()
scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
allocator = (ROOT / "modules/kern/src/platform/mm/numa_alloc.cpp").read_text()
allocator_header = (ROOT / "modules/kern/src/platform/mm/numa_alloc.hpp").read_text()
virt_header = (ROOT / "modules/kern/src/platform/mm/virt.hpp").read_text()
start = virt.index("enum class LazyFileFaultDisposition")
end = virt.index("\n#endif\n", virt.index("void start_lazy_file_fault_worker()", start))
region = virt[start:end]
branch_start = virt.index("        auto* const FILE_FAULT_ROOT =", end)
branch_end = virt.index("\n#endif", branch_start)
branch = virt[branch_start:branch_end]
control = os.environ.get("WOS_LAZY_FAULT_PROBE_CONTROL", "current")
if control == "predecessor":
    predecessor = Path(os.environ["WOS_LAZY_FAULT_PREDECESSOR_SOURCE"]).read_text()
    old_probe = function(predecessor, "probe_lazy_file_fault")
    current_probe = function(virt, "probe_lazy_file_fault")
    assert region.count(current_probe) == 1
    region = region.replace(current_probe, old_probe)
    old_start = predecessor.index("        auto const FILE_FAULT = probe_lazy_file_fault")
    branch = predecessor[old_start:predecessor.index("\n#endif", old_start)]
elif control == "retry-all-failures":
    assert "return LazyFileFaultDisposition::FAILED;" in region
    region = region.replace("return LazyFileFaultDisposition::FAILED;", "return LazyFileFaultDisposition::RETRY;")
else:
    assert control == "current", "unknown actual-probe control"
fallback_start = virt.index("        constexpr bool ALLOW_FILE_IO = false;", branch_end)
fallback_end = virt.index("        if (current_task != nullptr && current_task->pagemap != nullptr && current_task->thread != nullptr", fallback_start)
fallback = virt[fallback_start:fallback_end]
fallback = fallback[:fallback.index("#else")] + fallback[fallback.index("#endif") + len("#endif"):]
code = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <new>
#include <utility>
#include <vector>
#include <cerrno>
using vaddr_t=uint64_t; using paddr_t=uint64_t;
constexpr paddr_t PADDR_INVALID=UINT64_MAX;
constexpr uint64_t page_align_down(uint64_t p){return p&~uint64_t{4095};}
uint64_t capture_nodes=0,observed_nodes=0,observed_index=0;unsigned capture_mode=0,observed_mode=0;uint16_t capture_local=0,observed_local=0;bool fail_capture=false;
int guards=0, created=0, destroyed=0, reads=0, allocated=0, dropped=0, puts=0, parks=0, wakes=0;
bool fail_create=false, fail_post=false, shutdown_requested=false, fail_alloc=false, fail_read=false, software_prefault=false;
std::function<void()> read_hook, pin_hook, park_hook, put_hook;
std::function<void()> clean_exit_hook, task_release_hook, atomic_gate_hook;
struct CleanKernelExit {};
struct Lock;std::function<void(Lock*)>unlock_hook;
struct Lock{bool held=false;uint64_t lock_irqsave(){assert(!held);held=true;++guards;return 0;}
 void unlock_irqrestore(uint64_t){assert(held);held=false;--guards;if(unlock_hook)unlock_hook(this);}};
struct alignas(4096) PageTable{uint64_t address=0;void*page=nullptr;bool closing=false;};
// An atomic transition endpoint injects an exclusive flag immediately after the
// actual successful reader CAS, before its actual post-CAS recheck/rollback.
struct HookCount{std::atomic<uint32_t>value{0};std::atomic<bool>*close_after_cas=nullptr;
 uint32_t load(std::memory_order o=std::memory_order_seq_cst)const{return value.load(o);}
 void store(uint32_t v,std::memory_order o=std::memory_order_seq_cst){value.store(v,o);}
 uint32_t fetch_sub(uint32_t v,std::memory_order o){return value.fetch_sub(v,o);}
 bool compare_exchange_weak(uint32_t&v,uint32_t n,std::memory_order a,std::memory_order b){
  bool acquired=value.compare_exchange_weak(v,n,a,b);if(acquired&&close_after_cas){auto*flag=std::exchange(close_after_cas,nullptr);
   flag->store(true,std::memory_order_release);if(atomic_gate_hook){auto hook=std::move(atomic_gate_hook);atomic_gate_hook={};hook();}}
  return acquired;}};
namespace ker{
namespace syscall::process{[[noreturn]] void wos_proc_exit(int);}
namespace abi::vmem{constexpr uint64_t PROT_WRITE=2;}
namespace vfs{
struct File{int refs=1;unsigned id=37;};struct Timespec{int64_t tv_sec=0,tv_nsec=0;};
struct Stat{uint64_t st_dev=0,st_ino=0;int64_t st_size=0;Timespec st_mtim,st_ctim;unsigned st_mode=0;};
constexpr unsigned S_IFREG=0100000;
void vfs_retain_file(File*f){assert(guards>0&&f&&f->refs>0);++f->refs;}
void vfs_put_file(File*f){assert(guards==0&&f&&f->refs>0);if(put_hook)put_hook();--f->refs;++puts;}
}
namespace mod{
namespace cpu{inline constexpr uint64_t GATE_IF_MASK=1ULL<<9;}
namespace dbg{[[noreturn]] void panic_handler(const char*){std::abort();}}
namespace sys{using Spinlock=::Lock;}
namespace sched::task{
enum class TaskType{PROCESS,DAEMON};enum class TaskState{ACTIVE,EXITING};
enum class LazyVmemKind{ANONYMOUS,FILE_BACKED};
struct LazyVmemRange{uint64_t start=0x10000,end=0x11000,prot=3,flags=0;
 LazyVmemKind kind=LazyVmemKind::FILE_BACKED;ker::vfs::File*file=nullptr;
 uint64_t file_offset=0,file_dev=1,file_ino=2,file_size=4096;
 int64_t file_mtime_sec=0,file_mtime_nsec=0,file_ctime_sec=0,file_ctime_nsec=0;};
struct Task{TaskType type=TaskType::PROCESS;std::atomic<TaskState>state{TaskState::ACTIVE};
 uint32_t preempt_disable_depth=0,reclaim_no_io_depth=0;int refs=1,pins=0,holds=0,clean_exit_calls=0;uint64_t pid=23;bool posted=false;
 std::atomic<bool>usercopy_pagemap_closing{false},scheduler_transition_active{false},exit_in_progress{false},process_exit_requested{false};
 bool has_exited=false,deferred_task_switch=false,wants_block=false,refuse_pin=false;
 PageTable*pagemap=nullptr;Lock lazy_vmem_lock;std::vector<LazyVmemRange>lazy_vmem_ranges;
 uint64_t lazy_file_fault_generation=0,lazy_file_fault_vaddr=0;int lazy_file_fault_status=0;
 std::atomic<uint64_t>lazy_file_fault_completion_generation{0};void(*entry)()=nullptr;
 bool try_acquire(){if(state.load()!=TaskState::ACTIVE)return false;assert(refs>0);++refs;return true;}
 bool try_acquire_lifetime_ref(){assert(refs>0);++refs;return true;}
 void release(){assert(refs>0);--refs;if(task_release_hook){auto hook=std::move(task_release_hook);task_release_hook={};hook();}}
 bool try_acquire_usercopy_pagemap(PageTable*&p){if(refuse_pin||!pagemap||pagemap->closing||usercopy_pagemap_closing.load())return false;++pins;p=pagemap;
  if(pin_hook){auto f=std::move(pin_hook);pin_hook={};f();}return true;}
 void release_usercopy_pagemap(){assert(pins>0);--pins;}
 static Task*create_kernel_thread(const char*,void(*f)()){assert(guards==0);if(fail_create)return nullptr;
  auto*t=new Task;t->type=TaskType::DAEMON;t->entry=f;++created;return t;}
};
void destroy_unpublished_user_thread(Task*t){assert(guards==0&&t->refs==1&&!t->posted);delete t;++destroyed;}
}
namespace sched{
struct PreemptGuard{PreemptGuard(){assert(guards==0);}~PreemptGuard(){assert(guards==0);}};
task::Task*current=nullptr;task::Task*posted_worker=nullptr;task::Task*return_override=nullptr;bool queryable=true;
task::Task*get_current_task(){return current;}
task::Task*get_return_task(){return return_override?return_override:current;}
bool can_query_current_task(){return queryable&&current!=nullptr;}
bool kernel_threads_shutdown_requested(){return shutdown_requested;}
struct Hold{task::Task*owner;explicit Hold(task::Task*t):owner(t){if(owner)++owner->holds;}
 ~Hold(){if(owner){assert(owner->holds>0);--owner->holds;}}};
Hold hold_kernel_shutdown(){return Hold(current);}
@CLEAN_EXIT@
bool post_task_balanced(task::Task*t){assert(guards==0);if(fail_post)return false;t->posted=true;posted_worker=t;return true;}
void wake_task_from_event(task::Task*t){assert(guards==0&&t&&t->refs>0);++wakes;}
void kern_block(){assert(guards==0&&current&&current->holds>0);shutdown_requested=true;}
enum class ExceptionWaitResult{PARKED,COMPLETION_WON};
struct GPRegs{std::array<uint64_t,15>values{};bool operator==(const GPRegs&)const=default;};
struct Frame{uint64_t cs=3,rip=0,rsp=0,flags=0x202,int_num=14,err_code=4,ss=3;bool operator==(const Frame&)const=default;};
ExceptionWaitResult place_task_in_wait_queue_if_pending(GPRegs&,Frame&frame,const std::atomic<uint64_t>&token,uint64_t g){
 assert(guards==0&&frame.cs==3);++parks;if(park_hook)park_hook();
 return token.load()==g?ExceptionWaitResult::COMPLETION_WON:ExceptionWaitResult::PARKED;}
}
namespace mm{
namespace paging{constexpr uint64_t PAGE_SIZE=4096;
// Keep the original two aggregate operands writable/fetch; append fault facts.
struct PageFault{unsigned writable=0,fetch=0,present=0,user=1,reserved=0,protection_key=0,shadow_stack=0;};}
enum class PhysicalPageOwner{USER_FILE_MAPPING,USER_PRIVATE_MAPPING};
namespace phys{
enum class PageInitialization:uint8_t{ZERO,FULL_OVERWRITE};
void*page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner,const char*){assert(guards==0);
 if(fail_alloc)return nullptr;++allocated;auto*p=new uint8_t[4096];std::memset(p,0xa5,4096);return p;}
void*page_alloc_full_overwrite_may_fail(PhysicalPageOwner owner,size_t bytes,const char*name){assert(bytes==4096);
 return page_alloc_full_overwrite_page_with_reclaim_may_fail(owner,name);}
void page_ref_dec(void*p){assert(guards==0);delete[]static_cast<uint8_t*>(p);++dropped;}
}
namespace addr{void*get_phys_pointer(uint64_t p){return reinterpret_cast<void*>(p);}}
namespace numa{
@NUMA_DECLARATIONS@
@NUMA_METHODS@
// RAW file endpoint observes the actual forwarded context/owner, not a
// duplicate policy allocator. Actual topology/buddy dispatch is separate.
auto try_allocate_pages(const AllocationContext&context,uint32_t pages,uint64_t align,PhysicalPageOwner owner,
 phys::PageInitialization init,ReclaimMode reclaim,const char*name)->AllocationResult{
 assert(guards==0&&pages==1&&align==4096&&owner==PhysicalPageOwner::USER_FILE_MAPPING);
 assert(init==phys::PageInitialization::FULL_OVERWRITE&&reclaim==ReclaimMode::LEGACY_UMA);
 observed_nodes=context.policy.nodes;observed_mode=static_cast<unsigned>(context.policy.mode);
 observed_local=context.local_node;observed_index=context.virtual_page_index;
 AllocationResult result;auto*p=phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(owner,name);
 if(!p){result.error=AllocationError::EXHAUSTED;return result;}
 result.span=OwnedSpan{p,1};result.requested=result.actual=context.local_node;result.error=AllocationError::NONE;return result;
}
@NUMA_ADAPTER@
}
namespace virt{
namespace sched=ker::mod::sched;namespace sys=ker::mod::sys;
@DEMAND_TYPES@
@DEMAND_STORAGE@
bool capture_demand_context(sched::task::Task*t,uint64_t,DemandOrigin origin,bool user,DemandContext&output){
 assert(guards==0);if(fail_capture)return false;output.origin=origin;output.hardware_user=user;
 output.allocation={{capture_nodes,static_cast<numa::Mode>(capture_mode)},capture_local,0};return t&&t->pagemap;}
void record_demand_candidate(const DemandContext&,bool){assert(guards==0);}
bool demand_is_observed(const DemandContext&){assert(guards==0);return false;}
void record_demand_result(const DemandContext&,DemandBacking,numa::NodeId,numa::NodeId,bool,bool,bool){assert(guards==0);}
using PageTable=::PageTable;PageTable*kernel_pagemap=nullptr;
auto get_kernel_pagemap(){return kernel_pagemap;}
Lock cow_pte_lock;
// RAW file-only fixture: anonymous swap lookup is deliberately unreachable.
struct AnonymousSwapMapping;
auto find_anonymous_swap_mapping_locked(PageTable*,uint64_t)->AnonymousSwapMapping*{std::abort();}
namespace log{template<class...Args>void error(const char*,Args...){}}
void log_lazy_vmem_fault_state(sched::task::Task*,uint64_t,const paging::PageFault&,const char*,uint64_t,uint64_t){}
uint64_t lazy_user_vmem_flags(uint64_t prot){return prot;}
enum class LazyFilePageInstallResult{STALE_RANGE,ALREADY_MAPPED,MAPPED};
@GATE_DECLARATIONS@
// Raw true/spin is covered by the actual MM-debt driver. This file-only driver
// must never service/wait in the hardware-fault path.
void service_user_pagemap_gate_wait(uint64_t&){std::abort();}
@GATE_FUNCTIONS@
uint32_t reader_count(PageTable*root){return user_pagemap_access_gates.at(user_pagemap_access_gate_index(root)).readers.load();}
uint64_t translate(PageTable*p,uint64_t va){return p&&p->page&&p->address==va?reinterpret_cast<uint64_t>(p->page):PADDR_INVALID;}
void map_page(PageTable*p,uint64_t va,uint64_t pa,uint64_t){assert(guards==2&&p&&!p->page);p->address=va;p->page=reinterpret_cast<void*>(pa);}
void invalidate_local_tlb_if_current(PageTable*,uint64_t,bool){assert(guards==0);}
bool lazy_file_range_matches(const sched::task::LazyVmemRange&,const sched::task::LazyVmemRange&,uint64_t);
LazyFilePageInstallResult install_lazy_file_page_if_current(sched::task::Task*,const sched::task::LazyVmemRange&,uint64_t,uint64_t,uint64_t,
                                                         file_mapping::FileMappingBinding* = nullptr);
}}
}
namespace syscall::vmem{
namespace numa=ker::mod::mm::numa;
using LazyVmemRange=ker::mod::sched::task::LazyVmemRange;using LazyVmemKind=ker::mod::sched::task::LazyVmemKind;
bool file_mmap_can_share(const ker::vfs::Stat&,uint64_t){return false;}
bool file_mmap_cached_page_for_file(ker::vfs::File*,const ker::vfs::Stat&,uint64_t,void**){assert(false);return false;}
bool read_file_mapping_exact(ker::vfs::File*f,void*p,size_t n,uint64_t){assert(guards==0&&f->refs>0);++reads;
 if(read_hook)read_hook();if(fail_read)return false;std::memset(p,f->id,n);return true;}
uint64_t file_mapping_page_flags(uint64_t prot,uint64_t flags){return prot|flags;}
bool materialize_lazy_file_page_impl(ker::mod::sched::task::Task*,const LazyVmemRange&,uint64_t,const ker::mod::mm::paging::PageFault&,const ker::mod::mm::numa::AllocationContext&,const ker::mod::mm::virt::BorrowedFileMappingRootAccess*);
bool materialize_lazy_file_page(ker::mod::sched::task::Task*t,const LazyVmemRange&r,uint64_t a,const ker::mod::mm::paging::PageFault&f,const ker::mod::mm::numa::AllocationContext&context){
 assert(guards==0&&((software_prefault&&ker::mod::sched::current==t)||
 (ker::mod::sched::current!=t&&t->pins==1&&ker::mod::mm::virt::reader_count(t->pagemap)==1)));
 return materialize_lazy_file_page_impl(t,r,a,f,context,nullptr);}
}}
namespace ker::mod::mm::virt{
@MATCH@
@REGION@
// Explicit RAW-only boundary: the actual NATIVE route is tested separately.
bool materialize_issued_native_file_fault(const LazyFileFaultWork&,size_t){std::abort();}
@INSTALL@
@ANONYMOUS_RANGE@
@LAZY_HELPER@
bool exercise_lazy_fault(sched::task::Task*t,uint64_t address,const paging::PageFault&fault,uint64_t rip=0,uint64_t rsp=0,bool file_io=true){
 DemandContextStorage storage;auto&demand=storage.get();demand.allocation.local_node=0;
 return handle_lazy_vmem_fault(t,address,fault,demand,rip,rsp,file_io);
}
bool handle_file_fault_frame(sched::task::Task*current_task,uint64_t control_register,paging::PageFault PAGEFAULT,
                             sched::Frame&frame,sched::GPRegs&gpr){
 uint64_t wait_generation=0;
@BRANCH@
@FALLBACK@
 return false;
}
bool handle_file_fault(sched::task::Task*task,uint64_t address,paging::PageFault fault,sched::Frame frame={}){
 sched::GPRegs gpr;return handle_file_fault_frame(task,address,fault,frame,gpr);
}
}
namespace ker::syscall::vmem{@MATERIALIZE@}
using namespace ker::mod::mm::virt;
namespace sched=ker::mod::sched;
using Task=sched::task::Task;using Range=sched::task::LazyVmemRange;using Fault=ker::mod::mm::paging::PageFault;
namespace ker::syscall::process{
[[noreturn]] void wos_proc_exit(int status){auto*current=sched::current;
 assert(status==0&&guards==0&&current&&current->type==sched::task::TaskType::DAEMON&&current->holds==0&&current->pins==0);
 assert(current->reclaim_no_io_depth==0&&current->refs==1&&current->clean_exit_calls++==0);
 assert(clean_exit_hook);clean_exit_hook();throw CleanKernelExit{};}}
struct Fixture{PageTable map;ker::vfs::File file;Task task;bool owns_file_ref=true;
 Fixture(){task.pagemap=&map;Range range;range.file=&file;task.lazy_vmem_ranges.push_back(range);}
 ~Fixture(){assert(task.refs==1&&task.pins==0&&reader_count(&map)==0&&file.refs==(owns_file_ref?1:0));if(map.page){delete[]static_cast<uint8_t*>(map.page);++dropped;}}
};
void reset(){assert(guards==0&&lazy_file_fault_worker==nullptr);for(auto const&w:lazy_file_fault_work)assert(!w.task);
 for(auto const&gate:user_pagemap_access_gates)assert(gate.readers.load()==0&&!gate.teardown.load()&&!gate.readers.close_after_cas);
 sched::return_override=nullptr;sched::queryable=true;kernel_pagemap=nullptr;task_release_hook=atomic_gate_hook={};
 lazy_file_fault_work={};lazy_file_fault_admission=false;lazy_file_fault_worker_starting=false;lazy_file_fault_next_sequence=1;
 capture_nodes=observed_nodes=observed_index=0;capture_mode=observed_mode=0;capture_local=observed_local=0;fail_capture=false;
 fail_create=fail_post=shutdown_requested=fail_alloc=fail_read=software_prefault=false;reads=parks=wakes=puts=0;read_hook=pin_hook=park_hook=put_hook=clean_exit_hook={};unlock_hook={};}
void start(){sched::current=nullptr;start_lazy_file_fault_worker();assert(lazy_file_fault_worker&&lazy_file_fault_admission);}
void run_worker(Task*t){clean_exit_hook=[](){assert(!lazy_file_fault_worker&&!lazy_file_fault_admission);
 for(auto const&work:lazy_file_fault_work)assert(!work.task);};bool exited=false;
 try{t->entry();}catch(CleanKernelExit&){exited=true;}assert(exited&&t->clean_exit_calls==1);clean_exit_hook={};}
void drain(){auto*t=sched::posted_worker;assert(t&&t==lazy_file_fault_worker);sched::current=t;shutdown_requested=true;run_worker(t);
 assert(t->refs==1&&t->holds==0&&!lazy_file_fault_worker&&!lazy_file_fault_admission);t->release();assert(t->refs==0);delete t;++destroyed;
 sched::posted_worker=nullptr;sched::current=nullptr;}
void one(){LazyFileFaultWork work;size_t index;assert(take_lazy_file_fault(work,index));auto*save=sched::current;
 sched::current=sched::posted_worker;sched::Hold hold(sched::current);
 bool const ok=ker::syscall::vmem::materialize_lazy_file_page(work.task,work.range,work.vaddr,work.fault,work.placement);
 finish_lazy_file_fault(work,index,ok?0:-EFAULT);sched::current=save;}

// Reserve a bounded Host arena, then construct a genuinely distinct root at
// the exact index of the target under the production4096-stripe index function.
struct ForeignRoot{
 void*arena=nullptr;PageTable*root=nullptr;
 explicit ForeignRoot(PageTable*target){
  constexpr size_t bytes=(USER_PAGEMAP_ACCESS_GATE_COUNT+1)*4096;
  arena=std::aligned_alloc(4096,bytes);assert(arena);
  size_t first=user_pagemap_access_gate_index(static_cast<PageTable*>(arena));
  size_t wanted=user_pagemap_access_gate_index(target);
  size_t pages=(wanted+USER_PAGEMAP_ACCESS_GATE_COUNT-first)&(USER_PAGEMAP_ACCESS_GATE_COUNT-1);
  root=new(static_cast<unsigned char*>(arena)+pages*4096)PageTable{};
  assert(root!=target&&user_pagemap_access_gate_index(root)==wanted);
 }
 ~ForeignRoot(){assert(reader_count(root)==0);root->~PageTable();std::free(arena);}
};
void empty_work(){for(auto const&work:lazy_file_fault_work)assert(work.task==nullptr);}
void retry_cases(){
 // Both the pre-CAS refusal and actual post-CAS rollback must return through an
 // unchanged IF1 user RX fault frame, without work/backing ownership or I/O.
 for(bool native:{false,true})for(bool race:{false,true}){
  reset();start();ker::mod::mm::file_mapping::FileMappingLifetime family;
  Fixture f;ForeignRoot foreign(&f.map);sched::current=&f.task;
  auto&range=f.task.lazy_vmem_ranges[0];range.prot=5;
  if(native){range.kind=sched::task::LazyVmemKind::NATIVE_FILE_BACKED;range.file=nullptr;range.lifetime=&family;}
  // The native object is an already-live finite lifetime boundary. Every owner
  // method still aborts: this oracle must finish before any native acquisition.
  size_t index=user_pagemap_access_gate_index(foreign.root);auto&gate=user_pagemap_access_gates.at(index);
  if(race)gate.readers.close_after_cas=&gate.teardown;
  else assert(try_begin_user_pagemap_exclusive(foreign.root)==index);
  Fault fetch{};fetch.fetch=1;sched::Frame frame{};frame.rip=0x10000;frame.rsp=0x80000;frame.err_code=20;
  sched::GPRegs regs{};regs.values[2]=0xa55a;auto before_frame=frame;auto before_regs=regs;int before_wakes=wakes;
  assert(handle_file_fault_frame(&f.task,0x10000,fetch,frame,regs));
  assert(frame==before_frame&&regs==before_regs&&reader_count(&f.map)==0&&f.task.refs==1&&f.task.pins==0);
  assert(f.file.refs==1&&reads==0&&parks==0&&puts==0&&wakes==before_wakes&&!f.map.page);
  assert(f.task.lazy_file_fault_generation==0&&f.task.lazy_file_fault_completion_generation.load()==0&&lazy_file_fault_next_sequence==1);
  empty_work();assert(gate.teardown.load());end_user_pagemap_exclusive(index);
  if(!native){
   assert(handle_file_fault_frame(&f.task,0x10000,fetch,frame,regs));
   assert(parks==1&&f.file.refs==2&&f.task.pins==1&&reader_count(&f.map)==1);
   one();assert(f.map.page&&f.task.lazy_file_fault_status==0&&f.task.lazy_file_fault_completion_generation.load()==1);
   assert(handle_file_fault_frame(&f.task,0x10000,fetch,frame,regs));
   assert(frame==before_frame&&regs==before_regs&&reader_count(&f.map)==0&&f.task.pins==0);
  }
  drain();
 }
 // All failure axes use the same actual probe and handler, even while a
 // different real root holds this stripe. None may acquire backing/work.
 for(unsigned axis=0;axis<31;++axis){
  reset();start();Fixture f;ForeignRoot foreign(&f.map);Task other;sched::current=&f.task;
  auto*worker=lazy_file_fault_worker;auto index=try_begin_user_pagemap_exclusive(foreign.root);assert(index!=SIZE_MAX);
  Fault fault{};sched::Frame frame{};sched::GPRegs regs{};
  switch(axis){
   case 0:frame.flags&=~uint64_t{0x200};break;
   case 1:frame.cs=0;break;
   case 2:f.task.preempt_disable_depth=1;break;
   case 3:sched::current=&other;break;
   case 4:sched::return_override=&other;break;
   case 5:sched::queryable=false;break;
   case 6:f.task.state.store(sched::task::TaskState::EXITING);break;
   case 7:f.task.usercopy_pagemap_closing.store(true);break;
   case 8:f.task.scheduler_transition_active.store(true);break;
   case 9:f.task.exit_in_progress.store(true);break;
   case 10:f.task.process_exit_requested.store(true);break;
   case 11:f.task.has_exited=true;break;
   case 12:f.task.deferred_task_switch=true;break;
   case 13:f.task.wants_block=true;break;
   case 14:pin_hook=[&]{f.task.pagemap=foreign.root;};break;
   case 15:pin_hook=[&]{f.task.lazy_vmem_ranges[0].file_offset+=4096;};break;
   case 16:pin_hook=[&]{f.task.lazy_vmem_ranges.clear();};break;
   case 17:f.task.lazy_vmem_ranges[0].prot=1;fault.fetch=1;break;
   case 18:f.task.lazy_vmem_ranges[0].prot=1;fault.writable=1;break;
   case 19:f.task.lazy_vmem_ranges[0].prot=0;break;
   case 20:f.task.lazy_vmem_ranges[0].file=nullptr;break;
   case 21:fault.present=1;break;
   case 22:fault.user=0;break;
   case 23:fault.reserved=1;break;
   case 24:fault.protection_key=1;break;
   case 25:fault.shadow_stack=1;break;
   case 26:f.task.refuse_pin=true;break;
   case 27:pin_hook=[&]{f.task.usercopy_pagemap_closing.store(true);};break;
   case 28:task_release_hook=[&]{f.task.pagemap=foreign.root;};break;
   case 29:task_release_hook=[&]{f.task.process_exit_requested.store(true);};break;
   case 30:kernel_pagemap=&f.map;break;
  }
  auto before_frame=frame;auto before_regs=regs;
  assert(!handle_file_fault_frame(&f.task,0x10000,fault,frame,regs));
  assert(frame==before_frame&&regs==before_regs&&f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0);
  assert(f.file.refs==1&&reads==0&&puts==0&&parks==0&&!f.map.page&&f.task.lazy_file_fault_generation==0);
  empty_work();end_user_pagemap_exclusive(index);
  f.task.pagemap=&f.map;kernel_pagemap=nullptr;sched::return_override=nullptr;sched::queryable=true;task_release_hook={};
  assert(lazy_file_fault_worker==worker);drain();
 }
 // The actual post-CAS rollback must not rescue context or range changes
 // injected at that atomic transition before the callback can classify it.
 for(unsigned axis=0;axis<4;++axis){
  reset();start();Fixture f;sched::current=&f.task;ForeignRoot foreign(&f.map);
  auto index=user_pagemap_access_gate_index(&f.map);auto&gate=user_pagemap_access_gates.at(index);
  gate.readers.close_after_cas=&gate.teardown;
  atomic_gate_hook=[&]{
   if(axis==0)f.task.usercopy_pagemap_closing.store(true);
   if(axis==1)f.task.pagemap=foreign.root;
   if(axis==2)f.task.lazy_vmem_ranges[0].file_offset+=4096;
   if(axis==3)f.task.process_exit_requested.store(true);
  };
  assert(!handle_file_fault(&f.task,0x10000,Fault{}));
  assert(!atomic_gate_hook&&reader_count(&f.map)==0&&f.task.refs==1&&f.task.pins==0&&f.file.refs==1&&reads==0&&parks==0);
  assert(f.task.lazy_file_fault_generation==0);empty_work();end_user_pagemap_exclusive(index);f.task.pagemap=&f.map;drain();
 }
 // Actual saturation never means transient teardown, including MAX+teardown.
 for(bool teardown:{false,true}){
  reset();start();Fixture f;sched::current=&f.task;auto&gate=user_pagemap_access_gates.at(user_pagemap_access_gate_index(&f.map));
  gate.readers.store(UINT32_MAX);gate.teardown.store(teardown);
  assert(!handle_file_fault(&f.task,0x10000,Fault{}));
  assert(f.task.refs==1&&f.task.pins==0&&f.file.refs==1&&reads==0&&parks==0&&f.task.lazy_file_fault_generation==0);
  empty_work();gate.readers.store(0);gate.teardown.store(false);drain();
 }
 // New RETRY eligibility must pass the ORIGINAL pool predicates first.
 for(unsigned axis=0;axis<6;++axis){
  reset();start();Fixture f;ForeignRoot foreign(&f.map);sched::current=&f.task;auto*worker=lazy_file_fault_worker;
  auto index=try_begin_user_pagemap_exclusive(foreign.root);assert(index!=SIZE_MAX);
  switch(axis){
   case 0:lazy_file_fault_admission=false;break;
   case 1:lazy_file_fault_worker=nullptr;break;
   case 2:lazy_file_fault_worker_starting=true;break;
   case 3:f.task.lazy_file_fault_generation=1;f.task.lazy_file_fault_completion_generation.store(1);
          f.task.lazy_file_fault_vaddr=0x10000;f.task.lazy_file_fault_status=-EIO;break;
   case 4:f.task.lazy_file_fault_generation=UINT64_MAX;f.task.lazy_file_fault_completion_generation.store(UINT64_MAX);break;
   case 5:lazy_file_fault_next_sequence=UINT64_MAX;break;
  }
  auto previous=f.task.lazy_file_fault_generation;auto completed=f.task.lazy_file_fault_completion_generation.load();
  auto sequence=lazy_file_fault_next_sequence;
  assert(!handle_file_fault(&f.task,0x10000,Fault{}));
  assert(f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0&&f.file.refs==1&&reads==0&&parks==0);
  assert(f.task.lazy_file_fault_generation==previous&&f.task.lazy_file_fault_completion_generation.load()==completed&&
         lazy_file_fault_next_sequence==sequence);empty_work();end_user_pagemap_exclusive(index);
  lazy_file_fault_worker=worker;lazy_file_fault_worker_starting=false;drain();
 }
 // A duplicate can park only on a genuinely published same-page generation.
 // The pending exclusive flag models an owner waiting for that work's reader;
 // the second probe must drop only its own local leases before using the token.
 for(bool same_page:{false,true}){
  reset();start();Fixture f;sched::current=&f.task;f.task.lazy_vmem_ranges[0].end=0x12000;
  uint64_t generation=0;assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,generation)==LazyFileFaultDisposition::PARK);
  auto index=user_pagemap_access_gate_index(&f.map);auto&gate=user_pagemap_access_gates.at(index);gate.teardown.store(true);
  assert(handle_file_fault(&f.task,same_page?0x10000:0x11000,Fault{})==same_page);
  assert(f.task.lazy_file_fault_generation==generation&&f.task.refs==2&&f.task.pins==1&&reader_count(&f.map)==1&&f.file.refs==2);
  end_user_pagemap_exclusive(index);one();drain();assert(f.map.page&&f.task.lazy_file_fault_completion_generation.load()==generation);
 }
}
int main(){
 retry_cases();
 reset();{Fixture f;sched::current=&f.task;f.task.lazy_vmem_ranges.clear();uint64_t g=0;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::NOT_TRACKED);
  Range inserted;inserted.file=&f.file;f.task.lazy_vmem_ranges.push_back(inserted);
  assert(!exercise_lazy_fault(&f.task,0x10000,Fault{},0,0,false));
  assert(reads==0&&puts==0&&f.file.refs==1&&f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0&&!f.map.page);
  software_prefault=true;assert(exercise_lazy_fault(&f.task,0x10000,Fault{}));software_prefault=false;
  assert(reads==1&&puts==1&&f.file.refs==1&&f.map.page);}
 reset();{Fixture f;sched::current=&f.task;f.task.lazy_vmem_ranges.clear();
  unlock_hook=[&](Lock*lock){if(lock==&f.task.lazy_vmem_lock){unlock_hook={};Range inserted;inserted.file=&f.file;
   f.task.lazy_vmem_ranges.push_back(inserted);}};
  assert(!handle_file_fault(&f.task,0x10000,Fault{}));
  assert(reads==0&&puts==0&&f.file.refs==1&&!f.map.page&&f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0);}
 reset();{Fixture f;uint64_t g=0;assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  assert(f.file.refs==1&&f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0);}
 reset();start();{Fixture f;sched::current=&f.task;uint64_t g=0;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},false,g)==LazyFileFaultDisposition::FAILED);
  f.task.preempt_disable_depth=1;assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  f.task.preempt_disable_depth=0;f.task.lazy_vmem_ranges[0].prot=1;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{1,0},true,g)==LazyFileFaultDisposition::FAILED);
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{0,1},true,g)==LazyFileFaultDisposition::FAILED);
  assert(f.file.refs==1&&f.task.refs==1);}
 drain();
 reset();start();{Fixture f;sched::current=&f.task;
  assert(handle_file_fault(&f.task,0x10000,Fault{}));assert(parks==1&&f.file.refs==2&&f.task.refs==2);
  uint64_t g=0;assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
  assert(g==1&&f.file.refs==2&&f.task.refs==2);
  put_hook=[&]{assert(f.task.lazy_file_fault_completion_generation.load()==0&&reader_count(&f.map)==0&&f.task.pins==0);};
  one();put_hook={};assert(f.map.page&&f.task.lazy_file_fault_status==0&&f.task.lazy_file_fault_completion_generation.load()==1);
  assert(reads==1&&static_cast<uint8_t*>(f.map.page)[0]==f.file.id);
  assert(handle_file_fault(&f.task,0x10000,Fault{}));assert(parks==1);}
 drain();
 reset();start();{Fixture f;sched::current=&f.task;park_hook=one;
  assert(handle_file_fault(&f.task,0x10000,Fault{}));assert(parks==1&&reads==1&&f.map.page);park_hook={};}
 drain();
 for(int mode=0;mode<4;++mode){reset();start();{Fixture f;sched::current=&f.task;assert(handle_file_fault(&f.task,0x10000,Fault{}));
  if(mode==0)fail_alloc=true;if(mode==1)fail_read=true;
  if(mode==2)read_hook=[&]{f.task.lazy_vmem_ranges[0].file_offset+=4096;};
  if(mode==3)f.task.state.store(sched::task::TaskState::EXITING);
  drain();assert(!f.map.page&&f.task.lazy_file_fault_status==-EFAULT&&f.task.lazy_file_fault_completion_generation.load()==1);
  if(mode!=3){sched::current=&f.task;assert(!handle_file_fault(&f.task,0x10000,Fault{}));assert(f.task.lazy_file_fault_generation==1);}}}
 reset();start();{std::array<Fixture,65> fixtures;uint64_t g=0;
  for(size_t i=0;i<64;++i){sched::current=&fixtures[i].task;
   assert(probe_lazy_file_fault(&fixtures[i].task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);assert(g==1);}
  auto&extra=fixtures[64];sched::current=&extra.task;
  assert(probe_lazy_file_fault(&extra.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  assert(extra.file.refs==1&&extra.task.refs==1&&extra.task.pins==0&&reader_count(&extra.map)==0);
  {ForeignRoot foreign(&extra.map);auto gate=try_begin_user_pagemap_exclusive(foreign.root);assert(gate!=SIZE_MAX);
   assert(!handle_file_fault(&extra.task,0x10000,Fault{}));
   assert(extra.file.refs==1&&extra.task.refs==1&&extra.task.pins==0&&extra.task.lazy_file_fault_generation==0);
   end_user_pagemap_exclusive(gate);}
  LazyFileFaultWork w;size_t index;assert(take_lazy_file_fault(w,index));assert(w.task==&fixtures[0].task);
  finish_lazy_file_fault(w,index,-EFAULT);drain();assert(reads==63);}
 reset();start();{Fixture f;uint64_t g=0;f.task.lazy_file_fault_generation=UINT64_MAX;
  f.task.lazy_file_fault_completion_generation.store(UINT64_MAX);sched::current=&f.task;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  f.task.lazy_file_fault_generation=0;lazy_file_fault_next_sequence=UINT64_MAX;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);}
 drain();
 reset();start();{Fixture f;uint64_t g=0;sched::current=&f.task;pin_hook=[&]{f.task.lazy_vmem_ranges[0].file_offset+=4096;};
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  assert(f.file.refs==1&&puts==0&&f.task.refs==1&&reader_count(&f.map)==0&&f.task.pins==0);}
 drain();
 reset();start();{Fixture f;uint64_t g=0;sched::current=&f.task;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
  f.task.lazy_vmem_ranges.clear();ker::vfs::vfs_put_file(&f.file);f.owns_file_ref=false;assert(f.file.refs==1);
  put_hook=[&]{assert(sched::current->type==sched::task::TaskType::DAEMON&&sched::current->holds>0);
   assert(f.task.lazy_file_fault_completion_generation.load()==0&&f.task.pins==0&&reader_count(&f.map)==0);};
  drain();put_hook={};assert(f.file.refs==0&&!f.map.page&&f.task.lazy_file_fault_status==-EFAULT);}
 reset();start();{Fixture f;int unlocks=0;auto*worker=sched::posted_worker;
  unlock_hook=[&](Lock*lock){if(lock==&lazy_file_fault_lock&&++unlocks==2){unlock_hook={};
   auto*save=sched::current;sched::current=&f.task;uint64_t g=0;
   assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
   sched::current=save;shutdown_requested=true;}};
  sched::current=worker;run_worker(worker);assert(worker->refs==1&&worker->holds==0&&!lazy_file_fault_worker);
  worker->release();delete worker;++destroyed;sched::current=nullptr;sched::posted_worker=nullptr;
  assert(reads==1&&f.map.page&&f.task.lazy_file_fault_completion_generation.load()==1&&!lazy_file_fault_admission);}
 // Queue capture belongs to this generation, not later worker CPU/policy
 // or a duplicate fault. The observer only records actual factory arguments.
 reset();start();{Fixture f;sched::current=&f.task;capture_nodes=10;capture_mode=2;capture_local=3;uint64_t g=0;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
  capture_nodes=4;capture_mode=3;capture_local=7;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
  drain();assert(observed_nodes==10&&observed_mode==2&&observed_local==3&&observed_index==16);
  assert(f.map.page&&f.task.lazy_file_fault_completion_generation.load()==1&&f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0);}
 reset();start();{Fixture f;sched::current=&f.task;fail_capture=true;uint64_t g=0;
  int before_puts=puts;assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  assert(f.task.refs==1&&f.task.pins==0&&reader_count(&f.map)==0&&f.file.refs==1&&puts==before_puts&&g==0&&!f.map.page);}
 drain();
 reset();fail_create=true;start_lazy_file_fault_worker();assert(!lazy_file_fault_worker&&!lazy_file_fault_worker_starting);
 fail_create=false;fail_post=true;int before=destroyed;start_lazy_file_fault_worker();
 assert(!lazy_file_fault_worker&&!lazy_file_fault_worker_starting&&destroyed==before+1);
 fail_post=false;start();drain();assert(created==destroyed&&allocated==dropped&&guards==0);
 return 0;
}
'''
namespace_start = allocator_header.index('namespace ker::mod::mm::numa {') + len('namespace ker::mod::mm::numa {')
numa_declarations = allocator_header[namespace_start:allocator_header.rindex('}  // namespace ker::mod::mm::numa')]
# Only the real owner methods belong to this file fixture. CPU/context capture
# is exercised by the actual factory target, not this unreachable endpoint.
numa_methods = allocator[allocator.index('OwnedSpan::~OwnedSpan()'):allocator.index('auto capture_allocation_context(')]
demand_storage = virt[virt.index('struct DemandContext {'):virt.index('class AnonymousShutdownHoldStorage {')]
demand_types = '\n'.join(block(virt_header, rf'^enum class {name}[^;]*\{{') + ';'
                         for name in ['DemandOrigin', 'DemandBacking'])
code = '#include "' + str(ROOT / 'modules/kern/src/platform/mm/numa_policy.hpp') + '"\n' + code
code = extend_raw_fixture(code, 'Lock user_mapping_pin_lock;')
gate_begin = virt.index("constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT =")
gate_end = virt.index("[[nodiscard]] auto user_pagemap_access_gate_index", gate_begin)
gate_declarations = virt[gate_begin:gate_end]
assert gate_declarations.count("std::atomic<uint32_t> readers{0};") == 1
# Atomic storage is the explicit finite transition primitive. The production
# constants/index/CAS/rollback/callback/release and probe/handler bodies stay whole.
gate_declarations = gate_declarations.replace("std::atomic<uint32_t> readers{0};", "HookCount readers;")
gate_functions = [function(virt, "user_pagemap_access_gate_index"),
                  block(virt, r'^template <typename WaitForTeardown>\n\[\[nodiscard\]\] auto admit_user_pagemap_reader[^;{]*\{')]
gate_functions += [function(virt, name) for name in
                   ("acquire_user_pagemap_access", "release_user_pagemap_access", "try_begin_user_pagemap_exclusive",
                    "end_user_pagemap_exclusive", "try_acquire_lazy_file_fault_reader")]
for key, value in {
    "GATE_DECLARATIONS": gate_declarations,
    "GATE_FUNCTIONS": "\n".join(gate_functions),
    "CLEAN_EXIT": block(scheduler, r'^\[\[noreturn\]\] void exit_current_kernel_thread\([^;{]*\{'),
    "MATCH": function(virt, "lazy_file_range_matches"),
    "REGION": region,
    "INSTALL": function(virt, "install_lazy_file_page_if_current"),
    "ANONYMOUS_RANGE": function(virt, "anonymous_lazy_range_allows_fault"),
    "LAZY_HELPER": function(virt, "handle_lazy_vmem_fault"),
    "BRANCH": branch,
    "FALLBACK": fallback,
    "MATERIALIZE": function(vmem, "materialize_lazy_file_page_impl"),
    "NUMA_DECLARATIONS": numa_declarations,
    "NUMA_METHODS": numa_methods,
    "NUMA_ADAPTER": block(allocator, r'^\[\[gnu::noinline\]\] void try_allocate_pages_into\([^;{]*\{'),
    "DEMAND_TYPES": demand_types,
    "DEMAND_STORAGE": demand_storage,
}.items():
    code = code.replace(f"@{key}@", value)

# Expected negative controls abort their bounded Host child; retain no core image.
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
with tempfile.TemporaryDirectory(prefix="wos-file-fault-") as temporary:
    source = Path(temporary) / "trace.cpp"
    binary = Path(temporary) / "trace"
    source.write_text(code)
    subprocess.run(
        [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
         "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(ROOT / "modules/kern/src"),
         "-I", str(ROOT / "modules"), str(source), "-o", str(binary)],
        check=True,
    )
    subprocess.run([str(binary)], check=True, timeout=30)
print("actual deferred file-fault ownership and materialization traces pass")
