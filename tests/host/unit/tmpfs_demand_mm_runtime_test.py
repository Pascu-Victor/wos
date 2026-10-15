#!/usr/bin/env python3
"""Compile complete demand MM/worker/usercopy bodies against controlled primitives.

The real demand capture, lease classes, three-spare adoption, install, current
binding mutation, async probe/finish/worker and software pre-pin bodies execute
unchanged. Scheduler/IRQ/Task/backend/allocator/index are explicit Host seams.
Independent PTE flags/bytes, root generations, per-frame owners and ACK-before-
drop oracles test the connected MM consumer, not native I/O/TLB/executor timing.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import sys
import tempfile
import time

from lazy_file_fault_close_order_runtime_test import allocated, command, pin, subreaper

ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / 'modules/kern/src'


def definition(source: str, anchor: str, declaration: bool = False) -> tuple[str, dict]:
    start = source.index(anchor)
    opening = source.index('{', start)
    # Ignore braces in comments and literals while retaining original offsets.
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda m: ''.join('\n' if c == '\n' else ' ' for c in m[0]), source)
    depth = 0
    for end in range(opening, len(clean)):
        if clean[end] == '{': depth += 1
        elif clean[end] == '}':
            depth -= 1
            if depth == 0:
                end += 1
                if declaration:
                    assert source[end] == ';'
                    end += 1
                body = source[start:end]
                return body, dict(anchor=anchor, line=source.count('\n', 0, start) + 1,
                                  bytes=len(body.encode()), sha256=hashlib.sha256(body.encode()).hexdigest())
    raise ValueError('unterminated complete actual definition: ' + anchor)


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <platform/mm/paging.hpp>
namespace fixture {
inline unsigned assertions{},cases{},irq{},preempt{},task_leases{},root_leases{},io_calls{},metadata_calls{},table_allocs{},table_frees{},acks{},yields{};
inline unsigned fail_table_at{},refuse_readers{};
inline bool publication{},stop{};
inline bool interrupts=true;inline unsigned enabled_interrupts{},restored_interrupts{};
inline int io_error{};
inline std::function<void()> io_hook;
inline void check(bool value,const char* why){++assertions;if(!value){std::fprintf(stderr,"FAIL %s\n",why);std::abort();}}
struct Lock {bool held{};uint64_t lock_irqsave(){check(!held,"nonrecursive IRQ lock");held=true;++irq;return 0;}
 void unlock_irqrestore(uint64_t){check(held&&irq,"IRQ balanced");held=false;--irq;}};
struct Frame {alignas(4096)std::array<unsigned char,4096>bytes{};unsigned refs{1},withdraw_ack{};};
inline std::map<void*,Frame*> frames;
inline std::map<void*,bool> tables;
}
namespace ker::abi::vmem {constexpr uint64_t PROT_READ=1,PROT_WRITE=2,MAP_SHARED=1;}
namespace ker::vfs {struct File;}
namespace ker::mod::mm {using vaddr_t=uint64_t;using paddr_t=uint64_t;enum class PageKind{PAGE_TABLE,NORMAL};}
namespace ker::mod::mm::addr {
inline uint64_t get_phys_pointer(uint64_t p){return p;}
inline uint64_t get_virt_pointer(uint64_t p){return p;}
}
namespace ker::mod::mm::phys {
inline auto page_kind_get(void*p)->PageKind{return fixture::tables.contains(p)?PageKind::PAGE_TABLE:PageKind::NORMAL;}
inline unsigned page_ref_get(void*p){if(fixture::tables.contains(p))return 1;return fixture::frames.at(p)->refs;}
inline void page_free(void*p){fixture::check(!fixture::irq&&!fixture::root_leases&&!fixture::task_leases,"table free after leases");
 fixture::check(fixture::tables.erase(p)==1,"free exactly owned table");++fixture::table_frees;std::free(p);}
inline void page_ref_dec(void*p){auto*f=fixture::frames.at(p);fixture::check(!fixture::irq&&!fixture::root_leases&&!fixture::task_leases,"data drop outside root/IRQ leases");
 fixture::check(f->refs>1,"inode keeps canonical owner");if(f->withdraw_ack)fixture::check(fixture::acks>=f->withdraw_ack,"ACK before retired PTE drop");--f->refs;}
}
namespace ker::vfs::tmpfs {
class TmpfsPageLoan { public: fixture::Frame*frame_{};~TmpfsPageLoan(){if(frame_)ker::mod::mm::phys::page_ref_dec(frame_->bytes.data());}
 void* frame()const{return frame_?frame_->bytes.data():nullptr;}void* release_to_pte(){auto*p=frame();frame_=nullptr;return p;} };
class TmpfsDemandSharedBacking { public:
 std::map<uint64_t,std::unique_ptr<fixture::Frame>>pages;uint64_t limit=1ULL<<30;
 bool write_capable{}; bool writable()const{return write_capable;}
 int sync_metadata()const{++fixture::metadata_calls;fixture::check(fixture::interrupts&&!fixture::irq&&!fixture::preempt&&!fixture::publication&&!fixture::root_leases&&!fixture::task_leases,"metadata IO outside all leases");return 0;}
 uint64_t eof()const{return limit;}
 int prepare_page(uint64_t offset,TmpfsPageLoan&out)const{
  fixture::check(fixture::interrupts&&!fixture::irq&&!fixture::preempt&&!fixture::publication&&!fixture::task_leases&&!fixture::root_leases,"IO before ALL teardown-blocking leases");
  ++fixture::io_calls;if(fixture::io_hook)fixture::io_hook();if(fixture::io_error)return fixture::io_error;
  auto&self=*const_cast<TmpfsDemandSharedBacking*>(this);auto&f=self.pages[offset];if(!f){f=std::make_unique<fixture::Frame>();
   for(size_t i=0;i<4096;++i)f->bytes[i]=static_cast<unsigned char>((offset/4096+i*13)%251);fixture::frames[f->bytes.data()]=f.get();}
  ++f->refs;out.frame_=f.get();return 0;
 }
};
}
namespace ker::mod::mm::file_mapping {
enum class FamilyOwner{SNAPSHOT};enum class BindingOwner{PREPARED,INDEX,PIN,INSTALLER,SNAPSHOT,TLB};
enum class FileMappingBackingMode{PRIVATE_COPY,TMPFS_READ_SHARED,TMPFS_DEMAND_READ_SHARED,TMPFS_DEMAND_WRITE_SHARED};
struct FileMappingLifetime{ker::vfs::tmpfs::TmpfsDemandSharedBacking backend;unsigned refs=1;
 bool try_acquire(FamilyOwner){++refs;return true;}void release(FamilyOwner){fixture::check(!fixture::irq&&!fixture::root_leases&&!fixture::task_leases,"family release outside leases");fixture::check(refs>1,"family snapshot once");--refs;}
 bool is_demand_shared()const{return true;}bool shared_writable()const{return backend.writable();}
 FileMappingBackingMode backing_mode()const{return shared_writable()?FileMappingBackingMode::TMPFS_DEMAND_WRITE_SHARED:FileMappingBackingMode::TMPFS_DEMAND_READ_SHARED;}
 ker::vfs::File*file()const{std::abort();}
 const auto*demand_read_shared_backing()const{return &backend;}};
struct FileMappingBinding{FileMappingLifetime*family{};paging::PageTable*root_{};uint64_t serial{},begin{},last{},offset{};
 std::array<unsigned,6>counts{};bool indexed=true;
 auto lifetime()const{return family;}auto root()const{return root_;}auto root_incarnation()const{return serial;}
 auto start()const{return begin;}auto end()const{return last;}auto file_offset()const{return offset;}
 bool try_acquire(BindingOwner r){++counts.at(static_cast<size_t>(r));return true;}
 bool reclassify(BindingOwner a,BindingOwner b){auto&i=counts.at(static_cast<size_t>(a));if(!i)return false;--i;++counts.at(static_cast<size_t>(b));return true;}
 void release(BindingOwner r){fixture::check(!fixture::irq&&!fixture::root_leases&&!fixture::task_leases,"binding release outside leases");auto&i=counts.at(static_cast<size_t>(r));fixture::check(i>0,"binding owner exactly once");--i;}
 bool configure_unpublished(paging::PageTable*r,uint64_t s,uint64_t a,uint64_t b,uint64_t o){root_=r;serial=s;begin=a;last=b;offset=o;return true;}
};
enum class FileMappingPinResult{NOT_FILE_MAPPING,ACQUIRED,REFUSE};
struct FileMappingTlbRetirement{FileMappingBinding*pending{};bool empty()const{return pending==nullptr;}
 ~FileMappingTlbRetirement(){fixture::check(empty(),"TLB batch never abandoned");}
 void release_after_completed_tlb(){fixture::check(!fixture::publication,"TLB release outside publication");if(auto*p=std::exchange(pending,nullptr))p->release(BindingOwner::TLB);}};
struct FileMappingIndex{std::vector<FileMappingBinding*>rows;
 auto retain_for_pin_locked(paging::PageTable*r,uint64_t s,uint64_t a,FileMappingBinding*&out)->FileMappingPinResult{
  for(auto*b:rows)if(b->indexed&&b->root()==r&&b->serial==s&&a>=b->begin&&a<b->last){b->try_acquire(BindingOwner::PIN);out=b;return FileMappingPinResult::ACQUIRED;}return FileMappingPinResult::NOT_FILE_MAPPING;}
 bool matches_pin_locked(paging::PageTable*r,uint64_t s,uint64_t a,const FileMappingBinding*expected)const{
  for(auto*b:rows)if(b->indexed&&b->root()==r&&b->serial==s&&a>=b->begin&&a<b->last)return b==expected;return expected==nullptr;}
 bool attach_locked(FileMappingBinding*&b,uint64_t){rows.push_back(b);b->indexed=true;b->counts[1]=1;b=nullptr;return true;}
 bool overlap_locked(paging::PageTable*,uint64_t,uint64_t,uint64_t,FileMappingBinding*&,bool&){std::abort();}
 bool detach_locked(FileMappingBinding&b,FileMappingTlbRetirement&out){fixture::check(fixture::irq&&b.indexed,"detach real indexed binding");b.indexed=false;fixture::check(b.reclassify(BindingOwner::INDEX,BindingOwner::TLB),"INDEX transferred not duplicated");out.pending=&b;return true;}
};
}
namespace ker::mod::sched::task {
enum class TaskType{PROCESS,DAEMON};enum class TaskState{ACTIVE,EXITING};
// ACTUAL_RANGE_TYPES
struct Task{TaskType type=TaskType::PROCESS;std::atomic<TaskState>state{TaskState::ACTIVE};mm::paging::PageTable*pagemap{};
 std::atomic<bool>scheduler_transition_active{};uint64_t syscall_account_start_us{};
 fixture::Lock lazy_vmem_lock;std::vector<LazyVmemRange>lazy_vmem_ranges;unsigned refs=1,preempt_disable_depth{},local{};
 uint64_t lazy_file_fault_generation{},lazy_file_fault_vaddr{};int lazy_file_fault_status{};std::atomic<uint64_t>lazy_file_fault_completion_generation{};
 bool try_acquire(){if(state!=TaskState::ACTIVE)return false;++refs;return true;}void release(){fixture::check(refs>1,"retained Task released once");--refs;}
 bool try_acquire_usercopy_pagemap(mm::paging::PageTable*&out){if(state!=TaskState::ACTIVE||!pagemap)return false;++local;++fixture::task_leases;out=pagemap;return true;}
 void release_usercopy_pagemap(){fixture::check(local&&fixture::task_leases,"local root lease balanced");--local;--fixture::task_leases;}
};
}
namespace ker::mod::sched {
inline task::Task*current{};inline unsigned holds{},wakes{};
inline bool can_query_current_task(){return current!=nullptr;}inline auto get_current_task(){return current;}
inline void preempt_disable(){++fixture::preempt;}inline void preempt_enable(){fixture::check(fixture::preempt>0,"preempt balanced");--fixture::preempt;}
inline unsigned preempt_count(){return fixture::preempt;}inline bool interrupts_enabled(){return fixture::interrupts&&fixture::irq==0;}
struct PreemptGuard{PreemptGuard(){preempt_disable();}~PreemptGuard(){preempt_enable();}};
struct Hold{Hold(){++holds;}~Hold(){--holds;}};inline Hold hold_kernel_shutdown(){return {};}
inline bool kernel_threads_shutdown_requested(){return fixture::stop;}inline void kern_block(){fixture::stop=true;}
inline void kern_yield(){fixture::check(!fixture::preempt&&!fixture::root_leases&&!fixture::task_leases&&!fixture::irq,"yield outside all root/IRQ leases");++fixture::yields;}
inline void wake_task_from_event(task::Task*){fixture::check(!fixture::irq,"wake outside IRQ locks");++wakes;}
}
namespace ker::mod::sys{using Spinlock=fixture::Lock;}
namespace fixture {inline void interrupt_instruction(const char*instruction){check(!irq&&!root_leases&&!task_leases,"interrupt change outside root/IRQ leases");
 if(std::strstr(instruction,"sti")){interrupts=true;++enabled_interrupts;}else if(std::strstr(instruction,"cli")){interrupts=false;++restored_interrupts;}else std::abort();}}
// Compile the complete actual scope; only privileged CPU instructions are Host
// primitives. No source body or admission expression is rewritten.
#define asm
#define volatile(...) fixture::interrupt_instruction(#__VA_ARGS__)
#include <platform/sys/syscall_preparation.hpp>
#undef volatile
#undef asm
namespace ker::mod::dbg {[[noreturn]]inline void panic_handler(const char*s){std::fprintf(stderr,"PANIC %s\n",s);std::abort();}}
namespace ker::vfs {struct File{};inline void vfs_retain_file(File*){std::abort();}inline void vfs_put_file(File*){std::abort();}}
namespace ker::syscall::vmem {inline bool materialize_lazy_file_page(ker::mod::sched::task::Task*,const ker::mod::sched::task::LazyVmemRange&,uint64_t,const ker::mod::mm::paging::PageFault&){std::abort();}}
namespace ker::mod::mm::virt {
using namespace paging;namespace sched=ker::mod::sched;namespace sys=ker::mod::sys;namespace dbg=ker::mod::dbg;
constexpr uint64_t PADDR_INVALID=UINT64_MAX;constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE=SIZE_MAX;
enum class PublicReadPrepareResult:uint8_t{NOT_TRACKED,READY,REFUSED};
inline sys::Spinlock cow_pte_lock,user_mapping_pin_lock;inline file_mapping::FileMappingIndex file_mapping_index;
inline PageTable*kernel_pagemap{};
struct Control{uint64_t serial{};};inline std::map<PageTable*,Control>controls;
inline auto find_user_root_control_locked(PageTable*r)->Control*{auto i=controls.find(r);return i==controls.end()?nullptr:&i->second;}
inline bool acquire_user_pagemap_access(PageTable*,bool,size_t&g){if(fixture::refuse_readers){--fixture::refuse_readers;return false;}++fixture::root_leases;g=0;return true;}
inline void release_user_pagemap_access(size_t g){if(g!=SIZE_MAX){fixture::check(fixture::root_leases>0,"reader balanced");--fixture::root_leases;}}
struct PageTableReleaseOrigin{void*caller;uint64_t pid;};
inline bool try_release_page_table_to_pool(PageTable*,PageTableReleaseOrigin){return false;}
inline auto alloc_zeroed_page_table(bool)->PageTable*{fixture::check(!fixture::irq&&!fixture::preempt&&!fixture::root_leases&&!fixture::task_leases&&!fixture::publication,"table staging outside leases");
 ++fixture::table_allocs;if(fixture::fail_table_at==fixture::table_allocs)return nullptr;auto*p=static_cast<PageTable*>(std::aligned_alloc(4096,4096));std::memset(p,0,4096);fixture::tables[p]=true;return p;}
inline uint64_t index_of(uint64_t a,int l){return (a>>(12+9*(l-1)))&511;}inline auto entry_at(PageTable*p,size_t i)->PageTableEntry&{return p->entries.at(i);}
inline uint64_t pte_raw(const PageTableEntry&e){uint64_t r;std::memcpy(&r,&e,8);return r;}
inline void owned_frame_untrack_leaf(PageTable*,uint64_t,const PageTableEntry&e){auto*p=reinterpret_cast<void*>(e.frame<<12);
 if(auto i=fixture::frames.find(p);i!=fixture::frames.end())i->second->withdraw_ack=fixture::acks+1;}
inline void*find_anonymous_swap_mapping_locked(PageTable*,uint64_t){return nullptr;}
inline auto fixture_leaf(PageTable*p,uint64_t a)->PageTableEntry*{for(int l=4;l>1;--l){auto&e=entry_at(p,index_of(a,l));if(!e.present)return nullptr;p=reinterpret_cast<PageTable*>(e.frame<<12);}return &entry_at(p,index_of(a,1));}
inline uint64_t translate(PageTable*p,uint64_t a){auto*e=fixture_leaf(p,a);return e&&e->present?(e->frame<<12):PADDR_INVALID;}
inline bool snapshot_user_page_mapping_locked(PageTable*p,uint64_t a,bool write,paddr_t&out){auto*e=fixture_leaf(p,a);if(!e||!e->present||!e->user||(write&&!e->writable))return false;out=e->frame<<12;return true;}
inline void flush_pagemap_after_update(PageTable*,uint64_t,bool){fixture::check(!fixture::irq,"ACK outside IRQ locks");++fixture::acks;}
class FileMappingInstallerLease{public:~FileMappingInstallerLease();bool acquire(sched::task::Task&,const sched::task::LazyVmemRange&,uint64_t);
 auto binding(){return binding_;}private:file_mapping::FileMappingBinding*binding_{};size_t access_gate_{SIZE_MAX};};
bool capture_user_root_incarnation(PageTable*,uint64_t&)noexcept;
bool prepare_captured_tmpfs_read_shared_page(sched::task::Task&,const sched::task::LazyVmemRange&,uint64_t,uint64_t,file_mapping::FileMappingBinding&);
// ACTUAL_MM_BODIES
// ACTUAL_ASYNC_BODIES
}
namespace ker::mod::mm::virt {
struct HostUserPin{void*hhdm_page{};};
inline bool pin_user_page_for_task(sched::task::Task*t,PageTable*r,uint64_t a,bool write,bool,HostUserPin&out){
 auto*leaf=fixture_leaf(r,a);if(!leaf||!leaf->present||!leaf->user||(write&&!leaf->writable))return false;
 fixture::check(t->local>0,"output Task lease precedes nonblocking pin");++fixture::root_leases;
 out.hhdm_page=reinterpret_cast<void*>(leaf->frame<<12);++fixture::frames.at(out.hhdm_page)->refs;return true;}
}
namespace ker::mod::sys::usercopy {
inline bool range_valid(uint64_t a,size_t n){return a<0x800000000000ULL&&n<=0x800000000000ULL-a;}
struct StableUserPage{sched::task::Task*m_task{};mm::virt::HostUserPin m_pin{};uint64_t m_user_address{};
 ~StableUserPage(){reset();}void reset(){
  if(m_pin.hhdm_page){auto*p=std::exchange(m_pin.hhdm_page,nullptr);mm::virt::release_user_pagemap_access(0);
   // Real unpin drops frame before reader; this primitive owns no backend call.
   --fixture::frames.at(p)->refs;}
  if(auto*t=std::exchange(m_task,nullptr))t->release_usercopy_pagemap();m_user_address=0;}};
// ACTUAL_USERCOPY_PIN
}
'''

CASES = r'''
namespace fixture {
namespace mm=ker::mod::mm;namespace v=mm::virt;namespace fm=mm::file_mapping;namespace s=ker::mod::sched;
struct Fixture{
 mm::paging::PageTable*root{};fm::FileMappingLifetime family;fm::FileMappingBinding binding;s::task::Task task;
 explicit Fixture(uint64_t pages=130){root=static_cast<mm::paging::PageTable*>(std::aligned_alloc(4096,4096));std::memset(root,0,4096);tables[root]=true;
  task.pagemap=root;s::current=&task;v::controls[root]={17};binding.family=&family;binding.root_=root;binding.serial=17;binding.begin=0x200000;binding.last=binding.begin+pages*4096;binding.counts[1]=1;
  v::file_mapping_index.rows.push_back(&binding);s::task::LazyVmemRange r{};r.start=binding.begin;r.end=binding.last;r.prot=1;r.flags=1;
  r.kind=s::task::LazyVmemKind::TMPFS_READ_SHARED;r.lifetime=&family;r.file_size=family.backend.eof();task.lazy_vmem_ranges.push_back(r);++cases;}
 ~Fixture(){check(!irq&&!preempt&&!task_leases&&!root_leases,"all leases retired");check(task.refs==1&&family.refs==1,"Task/family snapshots balanced");
  check(binding.counts[2]==0&&binding.counts[3]==0&&binding.counts[4]==0,"binding transients balanced");
  io_hook={};io_error=0;fail_table_at=0;refuse_readers=0;publication=false;stop=false;interrupts=true;s::current=nullptr;
  v::file_mapping_index.rows.clear();v::controls.clear();
  // Explicit Host root teardown boundary after all actual tested leases end.
  for(auto&[p,owned]:tables){(void)owned;auto*t=static_cast<mm::paging::PageTable*>(p);for(auto&e:t->entries)if(e.present){auto i=frames.find(reinterpret_cast<void*>(e.frame<<12));if(i!=frames.end()){check(i->second->refs>1,"fixture owns remaining PTE");--i->second->refs;}}}
  for(auto&[off,f]:family.backend.pages){(void)off;check(f->refs==1,"exact final inode-only owners");frames.erase(f->bytes.data());}
  for(auto&[p,owned]:tables){(void)owned;std::free(p);}tables.clear();}
 uint64_t address(unsigned page=0)const{return binding.begin+page*4096;}
 bool prepare(unsigned page=0){return v::prepare_tmpfs_read_shared_page(task,address(page),false)==v::PublicReadPrepareResult::READY;}
 mm::paging::PageTableEntry*leaf(unsigned page=0){return v::fixture_leaf(root,address(page));}
};
void run(){
 {Fixture f;check(f.prepare(100),"large public range cold page100");check(io_calls>0,"real demand branch issued IO primitive");auto*e=f.leaf(100);
  check(e&&e->present&&e->user&&!e->writable&&e->no_execute,"actual readonly NX PTE");check((v::pte_raw(*e)&(mm::paging::PAGE_SHARED|mm::paging::PAGE_WRITE_RESTRICTED))==(mm::paging::PAGE_SHARED|mm::paging::PAGE_WRITE_RESTRICTED),"canonical shared provenance");
  auto*data=reinterpret_cast<unsigned char*>(e->frame<<12);for(unsigned i=0;i<4096;++i)check(data[i]==static_cast<unsigned char>((100+i*13)%251),"independent backend bytes");
  unsigned before=io_calls,alloc=table_allocs;check(f.prepare(100)&&io_calls==before&&table_allocs==alloc,"mapped probe has no IO/allocation");
  check(v::prepare_tmpfs_read_shared_page(f.task,f.address(),true)==v::PublicReadPrepareResult::REFUSED&&io_calls==before,"WRITE denied before IO");
  f.task.lazy_vmem_ranges[0].prot=0;check(!f.prepare()&&io_calls==before,"NONE denied before IO");}
 for(unsigned mode=0;mode<7;++mode){Fixture f;fm::FileMappingBinding replacement;unsigned before=io_calls;
  if(mode==0)io_error=-EIO;if(mode==1)fail_table_at=table_allocs+2;
  if(mode>=2)io_hook=[&]{check(!task_leases&&!root_leases,"unlocked IO race hook");if(mode==2)f.task.lazy_vmem_ranges.clear();if(mode==3)f.task.lazy_vmem_ranges[0].prot=0;if(mode==4)v::controls[f.root].serial=18;
   if(mode==5){auto*r=static_cast<mm::paging::PageTable*>(std::aligned_alloc(4096,4096));std::memset(r,0,4096);tables[r]=true;v::controls[r]={19};f.task.pagemap=r;}
   if(mode==6){replacement=f.binding;replacement.counts={};replacement.counts[1]=1;f.binding.indexed=false;f.binding.counts[1]=0;v::file_mapping_index.rows={&replacement};}};
  check(!f.prepare(),"stale/error demand preparation refused");auto*e=f.leaf();check(!e||!e->present,"refusal publishes no PTE");
  if(mode==1)check(io_calls==before,"PT allocation refusal precedes backend IO");for(auto&[off,p]:f.family.backend.pages){(void)off;check(p->refs==1,"failed loan released once");}}
 {Fixture f;ker::mod::sys::usercopy::StableUserPage page;unsigned before=io_calls;
  check(ker::mod::sys::usercopy::pin_task_user_page(f.task,f.address(),false,true,page),"actual software pin prepares demand before output leases");
  check(io_calls==before+1&&task_leases==1&&root_leases==1,"software output pin established only after IO");page.reset();
  check(!ker::mod::sys::usercopy::pin_task_user_page(f.task,f.address(1),false,false,page)&&io_calls==before+1,"mapped-only cold probe never IO");}
 {Fixture f;unsigned before=yields;refuse_readers=2;check(f.prepare()&&yields==before+2,"software capture retries busy hashed gate outside leases");}
 {Fixture f;f.task.syscall_account_start_us=1;interrupts=false;unsigned enabled=enabled_interrupts,restored=restored_interrupts;
  check(f.prepare(),"real syscall preparation scope admits IF-off ordinary demand");check(!interrupts&&enabled_interrupts==enabled+1&&restored_interrupts==restored+1,"actual scope restores original IF after demand owners cleanup");}
 for(unsigned mode=0;mode<2;++mode){Fixture f;interrupts=false;if(mode==1){f.task.syscall_account_start_us=1;f.task.scheduler_transition_active=true;}unsigned before=io_calls;
  check(!f.prepare()&&!interrupts&&io_calls==before,"unaccounted/transition IF-off context refuses before backend IO");}
 {Fixture f;unsigned before=yields,reads=io_calls;io_hook=[] {refuse_readers=2;};check(f.prepare(),"postIO reader retry succeeds");
  check(yields==before+2&&io_calls==reads+1,"owned loan/spares survive contention without repeated IO");}
 {Fixture f;s::task::Task worker;worker.type=s::task::TaskType::DAEMON;worker.refs=2;
  v::lazy_file_fault_worker=&worker;v::lazy_file_fault_admission=true;uint64_t generation=0;mm::paging::PageFault fault{};
  unsigned before=yields;refuse_readers=2;
  check(v::probe_lazy_file_fault(&f.task,f.address(),fault,true,generation)==v::LazyFileFaultDisposition::PARK,"actual hardware demand work admitted");
  check(!task_leases&&!root_leases&&f.task.refs==2&&f.family.refs==2,"queued public work owns snapshots only");
  check(v::lazy_file_fault_work[0].pagemap==nullptr&&v::lazy_file_fault_work[0].access_gate==SIZE_MAX,"old root address/gates absent from runnable public work");
  io_hook=[&]{check(s::holds==1&&f.binding.counts[4]==1,"actual worker holds independent binding and shutdown ownership through IO");};
  s::current=&worker;v::lazy_file_fault_worker_loop();s::current=&f.task;io_hook={};
  check(f.task.lazy_file_fault_completion_generation==generation&&f.task.lazy_file_fault_status==0,"actual generation completes after install/cleanup");
  check(yields==before+1,"actual queued worker retries remaining busy reader after lease-free probe");
  check(f.leaf()&&f.leaf()->present&&worker.refs==1&&s::holds==0,"real worker body drains work before exit");}
 {Fixture f;s::task::Task worker;worker.type=s::task::TaskType::DAEMON;worker.refs=2;
  v::lazy_file_fault_worker=&worker;v::lazy_file_fault_admission=true;uint64_t generation=0;mm::paging::PageFault fault{};
  check(v::probe_lazy_file_fault(&f.task,f.address(),fault,true,generation)==v::LazyFileFaultDisposition::PARK,"second actual public fault admitted");
  io_hook=[&]{f.task.lazy_vmem_ranges.clear();};s::current=&worker;v::lazy_file_fault_worker_loop();s::current=&f.task;io_hook={};
  check(f.task.lazy_file_fault_completion_generation==generation&&f.task.lazy_file_fault_status==-EFAULT,"stale actual worker completes failure after owners drain");
  auto*leaf=f.leaf();check(!leaf||!leaf->present,"stale hardware work cannot resurrect withdrawn mapping");}
 {Fixture f;check(f.prepare(0)&&f.prepare(64)&&f.prepare(129),"three nonadjacent demand leaves");
  publication=true;check(v::preflight_current_file_binding(f.task,f.binding),"actual legal whole binding preflight");
  f.task.lazy_vmem_ranges[0].prot=0;check(v::protect_current_file_binding(f.task,f.binding,0),"whole large NONE");
  for(auto page:{0U,64U,129U})check(f.leaf(page)->present&&!f.leaf(page)->user,"NONE retains present frame owner");
  check(v::preflight_current_file_binding(f.task,f.binding),"NONE preflight");f.task.lazy_vmem_ranges[0].prot=1;
  check(v::protect_current_file_binding(f.task,f.binding,1),"whole READ restored");for(auto page:{0U,64U,129U})check(f.leaf(page)->user&&!f.leaf(page)->writable,"READ restored without WRITE");
  check(v::preflight_current_file_binding(f.task,f.binding),"unmap preflight before metadata");f.task.lazy_vmem_ranges.clear();
  auto*held=f.family.backend.pages.at(0).get();++held->refs;f.binding.try_acquire(fm::BindingOwner::PIN);unsigned before=yields;refuse_readers=2;
  fm::FileMappingTlbRetirement after;check(v::retire_current_file_binding(f.task,f.binding,after),"large whole retirement retries first contention");
  check(yields==before+2&&!f.binding.indexed&&!after.empty(),"INDEX transferred only after all slices");
  for(auto page:{0U,64U,129U})check(!f.leaf(page)->present,"all actual PTEs purged");check(held->refs==2,"retained PIN survives PTE retirement");
  publication=false;after.release_after_completed_tlb();f.binding.release(fm::BindingOwner::PIN);mm::phys::page_ref_dec(held->bytes.data());
  for(auto&[off,p]:f.family.backend.pages){(void)off;check(p->refs==1,"inode-only final canonical owners");}}

 {Fixture f;f.family.backend.write_capable=true;auto&r=f.task.lazy_vmem_ranges[0];r.kind=s::task::LazyVmemKind::TMPFS_WRITE_SHARED;r.prot=3;
  check(v::prepare_tmpfs_read_shared_page(f.task,f.address(),true)==v::PublicReadPrepareResult::READY,"cold software writable preparation");
  auto*e=f.leaf();check(e&&e->present&&e->user&&e->writable&&e->no_execute,"actual writable shared NX leaf");
  check((v::pte_raw(*e)&(mm::paging::PAGE_SHARED|mm::paging::PAGE_WRITE_RESTRICTED|mm::paging::PAGE_COW))==mm::paging::PAGE_SHARED,"writable shared never COW/restricted");
  auto*data=reinterpret_cast<unsigned char*>(e->frame<<12);data[81]=0x37;check(f.family.backend.pages.at(0)->bytes[81]==0x37,"CPU store changes canonical backend bytes");
  e->accessed=1;e->dirty=1;auto*frame=f.family.backend.pages.at(0).get();unsigned refs=frame->refs;
  publication=true;r.prot=1;check(v::preflight_current_file_binding(f.task,f.binding),"writable whole preflight");
  check(v::protect_current_file_binding(f.task,f.binding,1),"revoke write from write-capable binding");
  check(e->user&&!e->writable&&e->accessed&&e->dirty&&frame->refs==refs,"READ downgrade preserves actual AD/frame owners");
  publication=false;check(v::prepare_tmpfs_read_shared_page(f.task,f.address(),true)==v::PublicReadPrepareResult::REFUSED,"software WRITE obeys downgraded row");
  publication=true;r.prot=0;check(v::protect_current_file_binding(f.task,f.binding,0),"write-capable NONE");check(!e->user&&!e->writable&&e->accessed&&e->dirty,"NONE revokes both user/write atomically");
  r.prot=3;check(v::protect_current_file_binding(f.task,f.binding,3),"restore granted WRITE without backend IO under root guards");
  check(e->user&&e->writable&&e->dirty&&frame->refs==refs,"WRITE restore keeps canonical owner and AD");publication=false;
  ker::mod::sys::usercopy::StableUserPage pin;check(ker::mod::sys::usercopy::pin_task_user_page(f.task,f.address(),true,true,pin),"actual writable software output pin");
  static_cast<unsigned char*>(pin.m_pin.hhdm_page)[82]=0x42;check(frame->bytes[82]==0x42,"software output shares canonical frame");pin.reset();}
 {Fixture f;f.task.lazy_vmem_ranges[0].kind=s::task::LazyVmemKind::TMPFS_WRITE_SHARED;unsigned before=io_calls;
  check(!f.prepare()&&io_calls==before,"write-kind cannot upgrade immutable readonly family");}
 {Fixture f;f.family.backend.write_capable=true;auto&r=f.task.lazy_vmem_ranges[0];r.kind=s::task::LazyVmemKind::TMPFS_WRITE_SHARED;r.prot=3;
  s::task::Task worker;worker.type=s::task::TaskType::DAEMON;worker.refs=2;v::lazy_file_fault_worker=&worker;v::lazy_file_fault_admission=true;
  uint64_t generation=0;mm::paging::PageFault fault{};fault.writable=1;
  check(v::probe_lazy_file_fault(&f.task,f.address(),fault,true,generation)==v::LazyFileFaultDisposition::PARK,"hardware WRITE cold work captured");
  check(!task_leases&&!root_leases,"writable work runnable without root leases");s::current=&worker;v::lazy_file_fault_worker_loop();s::current=&f.task;
  check(f.task.lazy_file_fault_status==0&&f.task.lazy_file_fault_completion_generation==generation&&f.leaf()->writable,"hardware worker installs canonical writable leaf");}
 {Fixture f;check(f.prepare(),"canonical duplicate validation setup");auto snapshot=f.task.lazy_vmem_ranges[0];auto*e=f.leaf();uint64_t original=e->frame;
  // Same legal policy but wrong physical word must never pass the duplicate install path.
  e->frame=original+1;check(!v::prepare_captured_tmpfs_read_shared_page(f.task,snapshot,f.address(),17,f.binding),"wrong-frame duplicate refused despite identical flags");e->frame=original;}

 {Fixture f;f.family.backend.write_capable=true;auto&r=f.task.lazy_vmem_ranges[0];r.kind=s::task::LazyVmemKind::TMPFS_WRITE_SHARED;r.prot=3;
  unsigned before=metadata_calls;io_hook=[&]{f.task.lazy_vmem_ranges.clear();};
  check(!f.prepare()&&metadata_calls==before,"withdrawn writable installer performs no metadata publication");
  auto*leaf=f.leaf();check(!leaf||!leaf->present,"withdrawn writable preparation installs no leaf");}
 std::printf("PASS demand_mm cases=%u assertions=%u IO=%u ACK=%u yields=%u failures=0\n",cases,assertions,io_calls,acks,yields);
}
}
int main(){fixture::run();}
'''


def generate(virt_source: Path) -> tuple[str, list[dict]]:
    virt = virt_source.read_text()
    task = (KERN / 'platform/sched/task.hpp').read_text()
    start, end = task.index('enum class LazyVmemKind'), task.index('using LazyVmemRangeVec')
    types = task[start:end]
    # The real helpers mention File::file() only as an unexecuted RAW boundary;
    # its typed family declaration is supplied before the range declarations.
    actual = [dict(anchor='actual-LazyVmemKind-Range-and-helpers', bytes=len(types.encode()), sha256=hashlib.sha256(types.encode()).hexdigest())]
    bodies = []
    for anchor in ['auto lazy_file_range_matches(', 'auto file_binding_matches_range(', 'auto demand_range_policy_valid(',
                   'FileMappingInstallerLease::~FileMappingInstallerLease()', 'auto FileMappingInstallerLease::acquire(',
                   'class DemandRootAccess {', 'struct DemandPageSnapshot {', 'auto capture_demand_page(',
                   'struct DemandTableSpares {', 'auto demand_leaf_locked(', 'auto prepare_captured_tmpfs_read_shared_page(',
                   'auto current_process_root(', 'auto prepare_tmpfs_read_shared_page(', 'auto capture_user_root_incarnation(',
                   'auto demand_binding_matches_locked(', 'auto demand_leaf_valid_locked(', 'auto preflight_current_demand_binding(',
                   'void drop_present_leaf_ref(', 'auto mutate_current_demand_binding(', 'auto preflight_current_file_binding(',
                   'auto retire_current_file_binding(', 'auto protect_current_file_binding(']:
        # Forward declaration of the captured prepare function precedes its body.
        source = virt
        if anchor == 'auto prepare_captured_tmpfs_read_shared_page(':
            source = virt[virt.index('class DemandRootAccess {'):]
        body, row = definition(source, anchor, anchor.startswith(('class ', 'struct ')))
        if source is not virt:
            row['line'] += virt[:virt.index('class DemandRootAccess {')].count('\n')
        bodies.append(body)
        actual.append(row)
    # All fixture fields are explicit seams; actual production definitions above
    # are inserted whole, without replacing any body fragment.
    code = PRELUDE.replace('// ACTUAL_RANGE_TYPES', types)
    code = code.replace('// ACTUAL_MM_BODIES', '\n\n'.join(bodies))
    begin = virt.index('enum class LazyFileFaultDisposition')
    end = virt.index('void wake_lazy_file_fault_worker()', begin)
    async_bodies = [virt[begin:end]]
    actual.append(dict(anchor='actual-private-async-work-pool', bytes=len(async_bodies[0].encode()),
                       sha256=hashlib.sha256(async_bodies[0].encode()).hexdigest()))
    for anchor in ['void wake_lazy_file_fault_worker()', 'auto probe_lazy_file_fault(', 'auto take_lazy_file_fault(',
                   'void finish_lazy_file_fault(', 'void lazy_file_fault_worker_loop()']:
        body, row = definition(virt, anchor)
        async_bodies.append(body)
        actual.append(row)
    code = code.replace('// ACTUAL_ASYNC_BODIES', '\n\n'.join(async_bodies))
    user = (KERN / 'platform/sys/usercopy.cpp').read_text()
    body, row = definition(user, 'auto pin_task_user_page(')
    actual.append(row)
    code = code.replace('// ACTUAL_USERCOPY_PIN', body)
    paging = (KERN / 'platform/mm/paging.opt.cpp').read_text()
    body, row = definition(paging, 'auto create_page_table_entry(')
    actual.append(row)
    code = code.replace('namespace fixture {', 'namespace ker::mod::mm::paging {\n' + body + '\n}\nnamespace fixture {', 1)
    return code + CASES, actual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default=os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++'))
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--virt-source', type=Path, default=KERN / 'platform/mm/virt.opt.cpp')
    args = parser.parse_args()
    start = time.monotonic()
    scratch = None
    groups = []
    result = dict(pass_=False, commands=[], native_runtime_qualified=False, hardware_TLB_qualified=False,
                  backend_and_index_are_explicit_Host_primitives=True)
    watched = [Path(__file__).resolve(), Path(__file__).with_name('lazy_file_fault_close_order_runtime_test.py'),
               args.virt_source.resolve(strict=True), KERN / 'platform/mm/virt.hpp', KERN / 'platform/sched/task.hpp',
               KERN / 'platform/sys/usercopy.cpp',
               KERN / 'platform/sys/syscall_preparation.hpp',
               KERN / 'platform/mm/paging.hpp', KERN / 'platform/mm/paging.opt.cpp']
    before = []
    try:
        compiler = Path(args.cxx).resolve(strict=True)
        watched.append(compiler)
        before = [pin(path) for path in watched]
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        subreaper()
        code, actual = generate(args.virt_source)
        result['actual_definitions'] = actual
        scratch = Path(tempfile.mkdtemp(prefix='wos-demand-mm-', dir='/tmp'))
        source, binary = scratch / 'demand.cpp', scratch / 'demand-test'
        include = scratch / 'include'
        for original, target in [(KERN / 'platform/mm/paging.hpp', include / 'platform/mm/paging.hpp')]:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(original.read_bytes())
        definitions = include / 'defines/defines.hpp'
        definitions.parent.mkdir(parents=True, exist_ok=True)
        definitions.write_text('#pragma once\n// Explicit unused kernel definitions Host boundary.\n')
        for name in ['platform/sched/scheduler.hpp','platform/sched/task.hpp']:
            path=include/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('#pragma once\n// Explicit Host Task/scheduler primitives precede this include.\n')
        scope=include/'platform/sys/syscall_preparation.hpp';scope.parent.mkdir(parents=True,exist_ok=True)
        scope.write_bytes((KERN/'platform/sys/syscall_preparation.hpp').read_bytes())
        source.write_text(code)
        commands = [[str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-fsanitize=address,undefined',
                     '-fno-omit-frame-pointer', '-I', str(include), '-MMD', '-MF', str(scratch / 'demand.d'), str(source), '-o', str(binary)],
                    [str(binary)]]
        for i, argv in enumerate(commands):
            log = scratch / f'command-{i}.log'
            row = command(argv, log, scratch, start + 60, groups)
            result['commands'].append(row)
            output = log.read_text(errors='replace')
            print(output[-32768:], end='')
            if row['exit'] != 0: raise ValueError('actual demand MM compile/runtime failed')
            if i == 1 and not re.search(r'^PASS demand_mm cases=\d+ assertions=\d+ IO=\d+ ACK=\d+ yields=\d+ failures=0$', output, re.M):
                raise ValueError('complete demand MM oracle result missing')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = repr(error)
    finally:
        result['source_before'] = before
        result['source_after'] = [pin(path) for path in watched] if before else []
        result['sources_held'] = result['source_before'] == result['source_after']
        if not result['sources_held']: result['pass_'] = False
        if scratch is not None and not groups:
            if args.evidence_dir is not None:
                args.evidence_dir.mkdir(parents=True, exist_ok=False)
                for path in scratch.rglob('*'):
                    if path.is_file() and path != scratch / 'demand-test':
                        target = args.evidence_dir / path.relative_to(scratch)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(path, target)
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        else:
            result['scratch_removed'] = scratch is None
            if groups: result['pass_'] = False
        result['seconds'] = time.monotonic() - start
        result['pass'] = result.pop('pass_')
        print(json.dumps(result, sort_keys=True))
    return 0 if result['pass'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
