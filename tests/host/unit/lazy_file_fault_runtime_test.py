#!/usr/bin/env python3
"""Execute production file-fault admission, worker and final materialization.

Only scheduler/physical/File lifetime boundaries are controlled. This verifies
ordering and ownership, not real exception stack switching or guest latency.
"""
import os
from pathlib import Path
import subprocess
import tempfile

from exec_trace_ownership_test import block, function

ROOT = Path(__file__).resolve().parents[3]
virt = (ROOT / "modules/kern/src/platform/mm/virt.opt.cpp").read_text()
vmem = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
start = virt.index("enum class LazyFileFaultDisposition")
end = virt.index("\n#endif\n", virt.index("void start_lazy_file_fault_worker()", start))
region = virt[start:end]
branch_start = virt.index("        auto const FILE_FAULT = probe_lazy_file_fault", end)
branch_end = virt.index("\n#endif", branch_start)
branch = virt[branch_start:branch_end]
fallback_start = virt.index("        constexpr bool ALLOW_FILE_IO = false;", branch_end)
fallback_end = virt.index("        if (current_task != nullptr", fallback_start)
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
#include <vector>
#include <cerrno>
using vaddr_t=uint64_t; using paddr_t=uint64_t;
constexpr paddr_t PADDR_INVALID=UINT64_MAX;
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE=SIZE_MAX;
constexpr uint64_t page_align_down(uint64_t p){return p&~uint64_t{4095};}
int guards=0, created=0, destroyed=0, reads=0, allocated=0, dropped=0, puts=0, parks=0, wakes=0;
bool fail_create=false, fail_post=false, shutdown_requested=false, fail_alloc=false, fail_read=false, software_prefault=false;
std::function<void()> read_hook, pin_hook, park_hook, put_hook;
std::function<void()> clean_exit_hook;
struct CleanKernelExit {};
struct Lock;std::function<void(Lock*)>unlock_hook;
struct Lock{bool held=false;uint64_t lock_irqsave(){assert(!held);held=true;++guards;return 0;}
 void unlock_irqrestore(uint64_t){assert(held);held=false;--guards;if(unlock_hook)unlock_hook(this);}};
struct PageTable{unsigned gates=0;uint64_t address=0;void*page=nullptr;bool closing=false;};
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
 PageTable*pagemap=nullptr;Lock lazy_vmem_lock;std::vector<LazyVmemRange>lazy_vmem_ranges;
 uint64_t lazy_file_fault_generation=0,lazy_file_fault_vaddr=0;int lazy_file_fault_status=0;
 std::atomic<uint64_t>lazy_file_fault_completion_generation{0};void(*entry)()=nullptr;
 bool try_acquire(){if(state.load()!=TaskState::ACTIVE)return false;assert(refs>0);++refs;return true;}
 bool try_acquire_lifetime_ref(){assert(refs>0);++refs;return true;}
 void release(){assert(refs>0);--refs;}
 bool try_acquire_usercopy_pagemap(PageTable*&p){if(!pagemap||pagemap->closing)return false;++pins;p=pagemap;
  if(pin_hook){auto f=std::move(pin_hook);pin_hook={};f();}return true;}
 void release_usercopy_pagemap(){assert(pins>0);--pins;}
 static Task*create_kernel_thread(const char*,void(*f)()){assert(guards==0);if(fail_create)return nullptr;
  auto*t=new Task;t->type=TaskType::DAEMON;t->entry=f;++created;return t;}
};
void destroy_unpublished_user_thread(Task*t){assert(guards==0&&t->refs==1&&!t->posted);delete t;++destroyed;}
}
namespace sched{
task::Task*current=nullptr;task::Task*posted_worker=nullptr;
task::Task*get_current_task(){return current;}
bool kernel_threads_shutdown_requested(){return shutdown_requested;}
struct Hold{task::Task*owner;explicit Hold(task::Task*t):owner(t){if(owner)++owner->holds;}
 ~Hold(){if(owner){assert(owner->holds>0);--owner->holds;}}};
Hold hold_kernel_shutdown(){return Hold(current);}
@CLEAN_EXIT@
bool post_task_balanced(task::Task*t){assert(guards==0);if(fail_post)return false;t->posted=true;posted_worker=t;return true;}
void wake_task_from_event(task::Task*t){assert(guards==0&&t&&t->refs>0);++wakes;}
void kern_block(){assert(guards==0&&current&&current->holds>0);shutdown_requested=true;}
enum class ExceptionWaitResult{PARKED,COMPLETION_WON};
struct GPRegs{};struct Frame{uint64_t cs=3,rip=0,rsp=0;};
ExceptionWaitResult place_task_in_wait_queue_if_pending(GPRegs&,Frame&frame,const std::atomic<uint64_t>&token,uint64_t g){
 assert(guards==0&&frame.cs==3);++parks;if(park_hook)park_hook();
 return token.load()==g?ExceptionWaitResult::COMPLETION_WON:ExceptionWaitResult::PARKED;}
}
namespace mm{
namespace paging{constexpr uint64_t PAGE_SIZE=4096;struct PageFault{unsigned writable=0,fetch=0;};}
enum class PhysicalPageOwner{USER_FILE_MAPPING,USER_PRIVATE_MAPPING};
namespace phys{
void*page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner,const char*){assert(guards==0);
 if(fail_alloc)return nullptr;++allocated;auto*p=new uint8_t[4096];std::memset(p,0xa5,4096);return p;}
void*page_alloc_full_overwrite_may_fail(PhysicalPageOwner owner,size_t bytes,const char*name){assert(bytes==4096);
 return page_alloc_full_overwrite_page_with_reclaim_may_fail(owner,name);}
void page_ref_dec(void*p){assert(guards==0);delete[]static_cast<uint8_t*>(p);++dropped;}
}
namespace addr{void*get_phys_pointer(uint64_t p){return reinterpret_cast<void*>(p);}}
namespace virt{
namespace sched=ker::mod::sched;namespace sys=ker::mod::sys;
using PageTable=::PageTable;Lock cow_pte_lock;
namespace log{template<class...Args>void error(const char*,Args...){}}
void log_lazy_vmem_fault_state(sched::task::Task*,uint64_t,const paging::PageFault&,const char*,uint64_t,uint64_t){}
uint64_t lazy_user_vmem_flags(uint64_t prot){return prot;}
enum class LazyFilePageInstallResult{STALE_RANGE,ALREADY_MAPPED,MAPPED};
bool acquire_user_pagemap_access(PageTable*p,bool,size_t&g){if(p->closing)return false;++p->gates;g=reinterpret_cast<size_t>(p);return true;}
void release_user_pagemap_access(size_t g){if(g!=SIZE_MAX){auto*p=reinterpret_cast<PageTable*>(g);assert(p->gates>0);--p->gates;}}
uint64_t translate(PageTable*p,uint64_t va){return p&&p->page&&p->address==va?reinterpret_cast<uint64_t>(p->page):PADDR_INVALID;}
void map_page(PageTable*p,uint64_t va,uint64_t pa,uint64_t){assert(guards==2&&p&&!p->page);p->address=va;p->page=reinterpret_cast<void*>(pa);}
void invalidate_local_tlb_if_current(PageTable*,uint64_t,bool){assert(guards==0);}
bool lazy_file_range_matches(const sched::task::LazyVmemRange&,const sched::task::LazyVmemRange&,uint64_t);
LazyFilePageInstallResult install_lazy_file_page_if_current(sched::task::Task*,const sched::task::LazyVmemRange&,uint64_t,uint64_t,uint64_t);
}}
}
namespace syscall::vmem{
using LazyVmemRange=ker::mod::sched::task::LazyVmemRange;using LazyVmemKind=ker::mod::sched::task::LazyVmemKind;
bool file_mmap_can_share(const ker::vfs::Stat&,uint64_t){return false;}
bool file_mmap_cached_page_for_file(ker::vfs::File*,const ker::vfs::Stat&,uint64_t,void**){assert(false);return false;}
bool read_file_mapping_exact(ker::vfs::File*f,void*p,size_t n,uint64_t){assert(guards==0&&f->refs>0);++reads;
 if(read_hook)read_hook();if(fail_read)return false;std::memset(p,f->id,n);return true;}
uint64_t file_mapping_page_flags(uint64_t prot,uint64_t flags){return prot|flags;}
bool materialize_lazy_file_page_impl(ker::mod::sched::task::Task*,const LazyVmemRange&,uint64_t,const ker::mod::mm::paging::PageFault&);
bool materialize_lazy_file_page(ker::mod::sched::task::Task*t,const LazyVmemRange&r,uint64_t a,const ker::mod::mm::paging::PageFault&f){
 assert(guards==0&&((software_prefault&&ker::mod::sched::current==t)||
 (ker::mod::sched::current!=t&&t->pins==1&&t->pagemap->gates==1)));
 return materialize_lazy_file_page_impl(t,r,a,f);}
}}
namespace ker::mod::mm::virt{
@MATCH@
@REGION@
@INSTALL@
@ANONYMOUS_RANGE@
@LAZY_HELPER@
bool handle_file_fault(sched::task::Task*current_task,uint64_t control_register,paging::PageFault PAGEFAULT,sched::Frame frame={}){
 sched::GPRegs gpr;uint64_t wait_generation=0;
@BRANCH@
@FALLBACK@
 return false;
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
 ~Fixture(){assert(task.refs==1&&task.pins==0&&map.gates==0&&file.refs==(owns_file_ref?1:0));if(map.page){delete[]static_cast<uint8_t*>(map.page);++dropped;}}
};
void reset(){assert(guards==0&&lazy_file_fault_worker==nullptr);for(auto const&w:lazy_file_fault_work)assert(!w.task);
 lazy_file_fault_work={};lazy_file_fault_admission=false;lazy_file_fault_worker_starting=false;lazy_file_fault_next_sequence=1;
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
 bool const ok=ker::syscall::vmem::materialize_lazy_file_page(work.task,work.range,work.vaddr,work.fault);
 finish_lazy_file_fault(work,index,ok?0:-EFAULT);sched::current=save;}
int main(){
 reset();{Fixture f;sched::current=&f.task;f.task.lazy_vmem_ranges.clear();uint64_t g=0;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::NOT_TRACKED);
  Range inserted;inserted.file=&f.file;f.task.lazy_vmem_ranges.push_back(inserted);
  assert(!handle_lazy_vmem_fault(&f.task,0x10000,Fault{},0,0,false));
  assert(reads==0&&puts==0&&f.file.refs==1&&f.task.refs==1&&f.task.pins==0&&f.map.gates==0&&!f.map.page);
  software_prefault=true;assert(handle_lazy_vmem_fault(&f.task,0x10000,Fault{}));software_prefault=false;
  assert(reads==1&&puts==1&&f.file.refs==1&&f.map.page);}
 reset();{Fixture f;sched::current=&f.task;f.task.lazy_vmem_ranges.clear();
  unlock_hook=[&](Lock*lock){if(lock==&f.task.lazy_vmem_lock){unlock_hook={};Range inserted;inserted.file=&f.file;
   f.task.lazy_vmem_ranges.push_back(inserted);}};
  assert(!handle_file_fault(&f.task,0x10000,Fault{}));
  assert(reads==0&&puts==0&&f.file.refs==1&&!f.map.page&&f.task.refs==1&&f.task.pins==0&&f.map.gates==0);}
 reset();{Fixture f;uint64_t g=0;assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::FAILED);
  assert(f.file.refs==1&&f.task.refs==1&&f.task.pins==0&&f.map.gates==0);}
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
  put_hook=[&]{assert(f.task.lazy_file_fault_completion_generation.load()==0&&f.map.gates==1&&f.task.pins==1);};
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
  assert(extra.file.refs==1&&extra.task.refs==1&&extra.task.pins==0&&extra.map.gates==0);
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
  assert(f.file.refs==1&&puts==0&&f.task.refs==1&&f.map.gates==0&&f.task.pins==0);}
 drain();
 reset();start();{Fixture f;uint64_t g=0;sched::current=&f.task;
  assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
  f.task.lazy_vmem_ranges.clear();ker::vfs::vfs_put_file(&f.file);f.owns_file_ref=false;assert(f.file.refs==1);
  put_hook=[&]{assert(sched::current->type==sched::task::TaskType::DAEMON&&sched::current->holds>0);
   assert(f.task.lazy_file_fault_completion_generation.load()==0&&f.task.pins==1&&f.map.gates==1);};
  drain();put_hook={};assert(f.file.refs==0&&!f.map.page&&f.task.lazy_file_fault_status==-EFAULT);}
 reset();start();{Fixture f;int unlocks=0;auto*worker=sched::posted_worker;
  unlock_hook=[&](Lock*lock){if(lock==&lazy_file_fault_lock&&++unlocks==2){unlock_hook={};
   auto*save=sched::current;sched::current=&f.task;uint64_t g=0;
   assert(probe_lazy_file_fault(&f.task,0x10000,Fault{},true,g)==LazyFileFaultDisposition::PARK);
   sched::current=save;shutdown_requested=true;}};
  sched::current=worker;run_worker(worker);assert(worker->refs==1&&worker->holds==0&&!lazy_file_fault_worker);
  worker->release();delete worker;++destroyed;sched::current=nullptr;sched::posted_worker=nullptr;
  assert(reads==1&&f.map.page&&f.task.lazy_file_fault_completion_generation.load()==1&&!lazy_file_fault_admission);}
 reset();fail_create=true;start_lazy_file_fault_worker();assert(!lazy_file_fault_worker&&!lazy_file_fault_worker_starting);
 fail_create=false;fail_post=true;int before=destroyed;start_lazy_file_fault_worker();
 assert(!lazy_file_fault_worker&&!lazy_file_fault_worker_starting&&destroyed==before+1);
 fail_post=false;start();drain();assert(created==destroyed&&allocated==dropped&&guards==0);
 return 0;
}
'''
for key, value in {
    "CLEAN_EXIT": block(scheduler, r'^\[\[noreturn\]\] void exit_current_kernel_thread\([^;{]*\{'),
    "MATCH": function(virt, "lazy_file_range_matches"),
    "REGION": region,
    "INSTALL": function(virt, "install_lazy_file_page_if_current"),
    "ANONYMOUS_RANGE": function(virt, "anonymous_lazy_range_allows_fault"),
    "LAZY_HELPER": function(virt, "handle_lazy_vmem_fault"),
    "BRANCH": branch,
    "FALLBACK": fallback,
    "MATERIALIZE": function(vmem, "materialize_lazy_file_page_impl"),
}.items():
    code = code.replace(f"@{key}@", value)

with tempfile.TemporaryDirectory(prefix="wos-file-fault-") as temporary:
    source = Path(temporary) / "trace.cpp"
    binary = Path(temporary) / "trace"
    source.write_text(code)
    subprocess.run(
        [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
         "-fsanitize=address,undefined", "-fno-omit-frame-pointer", str(source), "-o", str(binary)],
        check=True,
    )
    subprocess.run([str(binary)], check=True, timeout=30)
print("actual deferred file-fault ownership and materialization traces pass")
