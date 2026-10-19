#!/usr/bin/env python3
"""Exercise actual issued NATIVE work, root gate, installer and materializer.

The complete family/binding/index implementation and genuine declarations join
actual probe/take/worker/finish, shared gate and borrowed installer/materializer.
Scheduler, root registry, physical/PTE, File IO and queued executor dispatch are
finite Host boundaries. This does not qualify native scheduling, TLBs, backend
coherence or the permanent executor. Both counterexamples use the same oracle
and require successful compilation before an expected runtime failure.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import time

from file_mmap_registry_runtime_test import command, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
FAMILY_COMMON = r'''#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// Finite Host primitives only. Family/Binding/Index use their genuine complete
// implementation and headers; this component defines no Task or mm::virt API.
namespace fixture {
void check(bool, const char*);
inline thread_local unsigned lock_depth{};
inline unsigned puts{}, callbacks{};
inline int fail_reserve_after{-1};
struct Lock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
    constexpr Lock() noexcept = default;
    Lock(const Lock&) = delete;
    auto operator=(const Lock&) -> Lock& = delete;
    auto lock_irqsave() noexcept -> uint64_t {
        while (flag.test_and_set(std::memory_order_acquire)) {}
        ++lock_depth;
        return 0;
    }
    void unlock_irqrestore(uint64_t) noexcept {
        check(lock_depth != 0, "finite IRQ lock depth balanced");
        --lock_depth;
        flag.clear(std::memory_order_release);
    }
};
void drain_retirements();
}
namespace ker::mod::sys { using Spinlock = fixture::Lock; }
namespace ker::mod::mm::paging {
inline constexpr uint64_t PAGE_SIZE = 4096, PAGE_SHIFT = 12;
struct PageTable {
    uint64_t address{};
    void* page{};
    uint64_t map_flags{};
    uint64_t serial{17};
};
}
namespace ker::vfs {
struct File { std::atomic<int> refcount{1}; unsigned id{37}; };
struct Timespec { int64_t tv_sec{}, tv_nsec{}; };
struct Stat {
    uint64_t st_dev{}, st_ino{};
    int64_t st_size{};
    Timespec st_mtim{}, st_ctim{};
    unsigned st_mode{};
};
inline constexpr unsigned S_IFREG = 0100000;
// Explicit finite File ref endpoints, not execution of real VFS close/retain.
inline auto vfs_try_retain_file(File* file) noexcept -> bool {
    if (file == nullptr) return false;
    int count = file->refcount.load(std::memory_order_relaxed);
    while (count > 0 && count < INT_MAX) {
        if (file->refcount.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) return true;
    }
    return false;
}
inline void vfs_retain_file(File* file) {
    fixture::check(vfs_try_retain_file(file), "finite File retain must keep a live nonsaturated owner");
}
inline void vfs_put_file(File* file) {
    fixture::check(fixture::lock_depth == 0, "finite File close outside all guards");
    if (file == nullptr) {
        fixture::check(false, "finite File put has an owned argument");
        return;
    }
    int count = file->refcount.load(std::memory_order_relaxed);
    while (count > 0) {
        if (file->refcount.compare_exchange_weak(count, count - 1, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
            ++fixture::puts;
            return;
        }
    }
    fixture::check(false, "finite File put cannot underflow");
}
inline auto vfs_put_file_result(File* file) -> int { vfs_put_file(file); return 0; }
}
'''
FAMILY_SEAMS = r'''#include <fixture_common.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>

namespace fm = ker::mod::mm::file_mapping;
namespace fixture {
namespace {
// Genuine tickets remain opaque. A separate finite ledger supplies the queued
// executor boundary without reading private ticket/executor/completion fields.
enum class State { FREE, RESERVED, READY, RUNNING, DONE };
struct Slot {
    fm::LifetimeRetirementTicket ticket{};
    fm::LifetimeRetirementRun run{};
    void* argument{};
    State state{State::FREE};
};
std::array<Slot, 4096> slots{};
size_t used{};
Lock queue_lock{};
auto lookup(fm::LifetimeRetirementTicket& ticket) noexcept -> Slot* {
    for (size_t i = 0; i < used; ++i)
        if (&slots[i].ticket == &ticket) return &slots[i];
    return nullptr;
}
auto reserve(void*, fm::LifetimeRetirementRun run, void* argument) noexcept -> fm::LifetimeRetirementTicket* {
    check(lock_depth == 0, "finite reservation in ordinary allocation context");
    if (run == nullptr || argument == nullptr) return nullptr;
    auto flags = queue_lock.lock_irqsave();
    if (fail_reserve_after == 0 || used == slots.size()) {
        queue_lock.unlock_irqrestore(flags);
        return nullptr;
    }
    if (fail_reserve_after > 0) --fail_reserve_after;
    auto& slot = slots[used++];
    slot.run = run;
    slot.argument = argument;
    slot.state = State::RESERVED;
    auto* result = &slot.ticket;
    queue_lock.unlock_irqrestore(flags);
    return result;
}
void submit(void*, fm::LifetimeRetirementTicket& ticket) noexcept {
    auto flags = queue_lock.lock_irqsave();
    auto* slot = lookup(ticket);
    check(slot != nullptr && slot->state == State::RESERVED, "submit exact reserved finite ticket once");
    if (slot != nullptr && slot->state == State::RESERVED) slot->state = State::READY;
    queue_lock.unlock_irqrestore(flags);
    // Never execute callbacks under the real owner-count guard or submit call.
}
auto observe(void*, fm::LifetimeRetirementTicket&, fm::LifetimeRetirementRun, void*, fm::RetirementCompletion&) noexcept -> bool {
    return false;  // No observer/return-generation qualification in this fixture.
}
auto cancel(void*, fm::LifetimeRetirementTicket&, fm::LifetimeRetirementRun, void*) noexcept -> bool {
    return false;  // Transactional observer construction is outside PRIVATE_COPY.
}
const fm::LifetimeRetirementPort port{nullptr, reserve, submit, observe, cancel};
}
void drain_retirements() {
    check(lock_depth == 0, "finite retirement drain outside all guards");
    for (size_t turn = 0; turn <= slots.size(); ++turn) {
        bool progressed = false;
        for (size_t i = 0; i < slots.size(); ++i) {
            auto flags = queue_lock.lock_irqsave();
            auto& slot = slots[i];
            auto run = slot.state == State::READY ? slot.run : nullptr;
            auto* argument = slot.argument;
            if (run != nullptr) slot.state = State::RUNNING;
            queue_lock.unlock_irqrestore(flags);
            if (run == nullptr) continue;
            check(lock_depth == 0, "actual Family/Binding callback outside queue/owner guards");
            static_cast<void>(run(argument));
            ++callbacks;
            flags = queue_lock.lock_irqsave();
            slot.state = State::DONE;
            queue_lock.unlock_irqrestore(flags);
            progressed = true;
        }
        if (!progressed) return;
    }
    check(false, "bounded finite retirement dependency drain");
}
}
namespace ker::mod::mm::file_mapping {
auto LifetimeRetirementExecutor::port() noexcept -> const LifetimeRetirementPort* { return &fixture::port; }
RetirementCompletion::RetirementCompletion(RetirementCompletion&&) noexcept = default;
auto RetirementCompletion::operator=(RetirementCompletion&&) noexcept -> RetirementCompletion& = default;
RetirementCompletion::~RetirementCompletion() = default;
void RetirementCompletion::reset() noexcept {}
auto RetirementCompletion::query() const noexcept -> RetirementReturn { return {}; }
}
'''
FAMILY_HEADERS = {
    'platform/mm/paging.hpp': r'''#pragma once
#include <fixture_common.hpp>
''',
    'platform/sys/spinlock.hpp': r'''#pragma once
#include <fixture_common.hpp>
''',
    'vfs/file.hpp': r'''#pragma once
#include <fixture_common.hpp>
''',
    'vfs/vfs.hpp': r'''#pragma once
#include <fixture_common.hpp>
''',
    'platform/sys/usercopy.hpp': r'''#pragma once
#include <cstdint>
namespace ker::mod::sys::usercopy { inline constexpr uint64_t USER_ADDR_LIMIT = 0x800000000000ULL; }
''',
    'vfs/fs/tmpfs.hpp': r'''#pragma once
#include <fixture_common.hpp>
namespace ker::vfs::tmpfs {
// Compile/link boundaries only. The connected case is genuine PRIVATE_COPY.
class TmpfsPageLoan {
 public:
    auto frame() const -> void* { std::abort(); }
    auto release_to_pte() -> void* { std::abort(); }
};
class TmpfsReadSharedBacking {
 public:
    static auto prepare(File&, uint64_t, uint64_t, std::unique_ptr<TmpfsReadSharedBacking>&) -> int { return -EOPNOTSUPP; }
    auto eof() const -> uint64_t { std::abort(); }
    auto loan_page(uint64_t, TmpfsPageLoan&) const -> bool { std::abort(); }
};
class TmpfsDemandSharedBacking {
 public:
    static auto prepare(File&, uint64_t, uint64_t, bool, std::unique_ptr<TmpfsDemandSharedBacking>&) -> int { return -EOPNOTSUPP; }
};
}
''',
}

PRELUDE = r'''
#include "fixture_common.hpp"
#include <platform/mm/file_mapping_lifetime.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <array>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>
#include <sys/types.h>
using vaddr_t=uint64_t;using paddr_t=uint64_t;
constexpr paddr_t PADDR_INVALID=UINT64_MAX;
uint64_t page_align_down(uint64_t p){return p&~uint64_t{4095};}
namespace fixture {
unsigned assertions{},failures{},cases{},allocations{},frees{},reads{},maps{},wakes{},adapter_cases{},acks{},captures{};
uint32_t capture_entry_accesses{};
bool stop{},fail_allocation{},fail_io{},queryable{true};
std::function<void()> io_hook;
void check(bool v,const char* why){++assertions;if(!v&&failures++<24)std::fprintf(stderr,"FAIL %s\n",why);}
void* allocate(){check(lock_depth==0,"allocation outside IRQ locks");if(fail_allocation)return nullptr;
 ++allocations;auto*p=std::aligned_alloc(4096,4096);check(p,"finite frame allocated");return p;}
void drop(void*p){check(lock_depth==0&&p,"final frame outside IRQ locks");++frees;std::free(p);}
}
namespace ker::abi::vmem {constexpr uint64_t PROT_READ=1,PROT_WRITE=2,MAP_SHARED=1;}
namespace ker::mod::mm {
namespace paging {
// ACTUAL_PAGING_FLAGS
struct PageFault{unsigned writable{},fetch{},present{},user{1},reserved{},protection_key{},shadow_stack{};};
}
enum class PhysicalPageOwner{USER_FILE_MAPPING};
namespace phys {enum class PageInitialization:uint8_t{ZERO,FULL_OVERWRITE};void page_ref_dec(void*p){fixture::drop(p);}}
namespace addr {void*get_phys_pointer(uint64_t p){return reinterpret_cast<void*>(p);}}
namespace numa {
using NodeId=uint16_t;using NodeMask=uint64_t;constexpr NodeId INVALID_NODE=UINT16_MAX;
// ACTUAL_POLICY_TYPES
// ACTUAL_ALLOC_TYPES
// ACTUAL_SPAN_METHODS
auto try_allocate_pages(const AllocationContext&context,uint32_t pages,uint64_t alignment,PhysicalPageOwner owner,
 phys::PageInitialization init,ReclaimMode reclaim,const char*)->AllocationResult{
 fixture::check(pages==1&&alignment==4096&&owner==PhysicalPageOwner::USER_FILE_MAPPING&&
 init==phys::PageInitialization::FULL_OVERWRITE&&reclaim==ReclaimMode::LEGACY_UMA,"actual materializer allocation inputs");
 fixture::check(context.policy.nodes==6&&context.policy.mode==Mode::INTERLEAVE&&context.local_node==1&&
 context.virtual_page_index==0x10000/4096,"actual slot placement reaches allocator");
 AllocationResult r;auto*p=fixture::allocate();if(!p){r.error=AllocationError::EXHAUSTED;return r;}
 r.span=OwnedSpan{p,1};r.actual=r.requested=context.local_node;r.error=AllocationError::NONE;return r;
}
// ACTUAL_ALLOC_ADAPTER
}
}
namespace ker::mod::sched::task {
enum class TaskType{PROCESS,DAEMON};enum class TaskState{ACTIVE,EXITING,DEAD};
// ACTUAL_RANGE_TYPES
struct Task {
 TaskType type{TaskType::PROCESS};uint64_t pid{23};std::atomic<TaskState>state{TaskState::ACTIVE};
 std::atomic<uint32_t>ref_count{1},usercopy_pagemap_accesses{},holds{};
 std::atomic<bool>usercopy_pagemap_closing{},scheduler_transition_active{},exit_in_progress{},process_exit_requested{};
 uint32_t preempt_disable_depth{};bool has_exited{},deferred_task_switch{},wants_block{};
 mm::paging::PageTable*pagemap{};fixture::Lock lazy_vmem_lock;std::vector<LazyVmemRange>lazy_vmem_ranges;
 uint64_t lazy_file_fault_generation{},lazy_file_fault_vaddr{};int lazy_file_fault_status{};
 std::atomic<uint64_t>lazy_file_fault_completion_generation{};
 bool try_acquire(){if(state.load()!=TaskState::ACTIVE)return false;auto r=ref_count.load();
  while(r&&r!=UINT32_MAX)if(ref_count.compare_exchange_weak(r,r+1))return true;return false;}
 void release(){fixture::check(ref_count.fetch_sub(1)>0,"Task reference balanced");}
 bool try_acquire_usercopy_pagemap(mm::paging::PageTable*&);void release_usercopy_pagemap();
};
// ACTUAL_TASK_LEASES
}
namespace ker::mod::sched {
thread_local task::Task*current{};
auto get_current_task(){return current;}auto get_return_task(){return current;}
bool can_query_current_task(){return fixture::queryable;}
bool kernel_threads_shutdown_requested(){return fixture::stop;}
struct Hold{task::Task*owner;explicit Hold(task::Task*t):owner(t){if(t)++t->holds;}
 ~Hold(){if(owner)fixture::check(owner->holds.fetch_sub(1)>0,"shutdown hold balanced");}};
auto hold_kernel_shutdown(){return Hold(current);}
void wake_task_from_event(task::Task*){fixture::check(fixture::lock_depth==0,"wake outside locks");++fixture::wakes;}
void kern_block(){fixture::check(fixture::lock_depth==0,"worker block outside locks");fixture::stop=true;}
}
namespace ker::mod::mm::virt {
namespace sched=ker::mod::sched;namespace sys=ker::mod::sys;using PageTable=paging::PageTable;
namespace log{template<class...A>void error(const char*,A...){}}
PageTable*kernel_pagemap{};auto get_kernel_pagemap(){return kernel_pagemap;}
// ACTUAL_CAP_CLASSES
// ACTUAL_DEMAND_TYPES
bool capture_demand_context(sched::task::Task*t,uint64_t address,DemandOrigin origin,bool user,DemandContext&out){
 ++fixture::captures;
 fixture::check(fixture::lock_depth==0,"capture outside IRQ locks");
 fixture::check(t->usercopy_pagemap_accesses.load()==fixture::capture_entry_accesses,
                "capture precedes operation lease admission");
 out.allocation={{6,numa::Mode::INTERLEAVE},1,address/4096};out.origin=origin;out.hardware_user=user;return true;
}
sys::Spinlock cow_pte_lock,user_mapping_pin_lock;file_mapping::FileMappingIndex file_mapping_index;
struct Control{uint64_t serial{};};Control control{17};PageTable*registered_root{};
auto find_user_root_control_locked(PageTable*r)->Control*{return r==registered_root?&control:nullptr;}
bool prepare_captured_tmpfs_read_shared_page(sched::task::Task&,const sched::task::LazyVmemRange&,uint64_t,uint64_t,
 file_mapping::FileMappingBinding&){std::abort();}
uint64_t translate(PageTable*r,uint64_t a){return r&&r->page&&r->address==a?reinterpret_cast<uint64_t>(r->page):PADDR_INVALID;}
void map_page(PageTable*r,uint64_t a,uint64_t p,uint64_t flags){fixture::check(fixture::lock_depth==2&&r&&!r->page,"PTE publish under COW/range");
 ++fixture::maps;r->address=a;r->page=reinterpret_cast<void*>(p);r->map_flags=flags;}
void invalidate_local_tlb_if_current(PageTable*,uint64_t,bool){fixture::check(fixture::lock_depth==0,"TLB outside IRQ locks");++fixture::acks;}
// ACTUAL_GATE_DECLS
void service_user_pagemap_gate_wait(uint64_t&spins){++spins;std::this_thread::yield();}
// ACTUAL_GATE_BODIES
// ACTUAL_MATCHES
// ACTUAL_ROOT_CURRENT
// ACTUAL_CAPTURE_SERIAL
enum class LazyFilePageInstallResult{STALE_RANGE,ALREADY_MAPPED,MAPPED};
// ACTUAL_INSTALL
// ACTUAL_CAP_AND_INSTALLER
}
namespace ker::syscall::vmem {
namespace numa=ker::mod::mm::numa;using LazyVmemRange=ker::mod::sched::task::LazyVmemRange;
using LazyVmemKind=ker::mod::sched::task::LazyVmemKind;
bool file_mmap_can_share(const ker::vfs::Stat&,uint64_t){return false;}
bool file_mmap_cached_page_for_file(ker::vfs::File*,const ker::vfs::Stat&,uint64_t,void**){std::abort();}
bool read_file_mapping_exact(ker::vfs::File*f,void*p,size_t n,uint64_t off){
 fixture::check(fixture::lock_depth==0&&f&&f->refcount.load()>1&&off==4096,"owned backend offset outside locks");
 ++fixture::reads;if(fixture::io_hook)fixture::io_hook();if(fixture::fail_io)return false;std::memset(p,f->id,n);return true;
}
uint64_t file_mapping_page_flags(uint64_t prot,uint64_t flags){
 fixture::check(prot==5&&flags==2,"RX private flags reach finite PTE conversion");
 return ker::mod::mm::paging::PAGE_PRESENT|ker::mod::mm::paging::PAGE_USER;
}
// ACTUAL_MATERIALIZER
// The actual named wrapper executes unchanged behind the finite typed-entry
// adapter. The adapter never constructs, copies or mutates a capability.
// ACTUAL_NAMED_WRAPPER
bool materialize_lazy_file_page(ker::mod::sched::task::Task*t,const LazyVmemRange&r,uint64_t a,
 const ker::mod::mm::paging::PageFault&f,const numa::AllocationContext&p){return materialize_lazy_file_page_impl(t,r,a,f,p);}
}
namespace fixture {bool borrowed_entry(ker::mod::sched::task::Task*,const ker::mod::sched::task::LazyVmemRange&,uint64_t,
 const ker::mod::mm::paging::PageFault&,const ker::mod::mm::numa::AllocationContext&,const ker::mod::mm::virt::BorrowedFileMappingRootAccess&);}
namespace ker::mod::mm::virt {
// ACTUAL_ASYNC_DECLS
// ACTUAL_ASYNC_BODIES
namespace detail {
// ACTUAL_ISSUER
}
// ACTUAL_DISPATCHER
}
'''

CASES = r'''
namespace fixture {
namespace mm=ker::mod::mm;namespace v=mm::virt;namespace st=ker::mod::sched::task;
namespace fm=mm::file_mapping;using Root=mm::paging::PageTable;using Row=st::LazyVmemRange;
unsigned adapter_mode{};st::Task*other_task{};
bool borrowed_entry(st::Task*t,const Row&r,uint64_t a,const mm::paging::PageFault&f,
 const mm::numa::AllocationContext&p,const v::BorrowedFileMappingRootAccess&cap){
 check(f.fetch==1&&f.writable==0,"actual indexed slot fault reaches typed materializer");
 if(adapter_mode){
  ++adapter_cases;Row copy=r;v::FileMappingInstallerLease rejected;
  if(adapter_mode==1)check(!rejected.acquire_borrowed(*other_task,r,a,cap),"cap refuses equal-root wrong Task");
  if(adapter_mode==2)check(!rejected.acquire_borrowed(*t,copy,a,cap),"cap refuses equal-valued other range object");
  if(adapter_mode==3)check(!rejected.acquire_borrowed(*t,r,a+4096,cap),"cap refuses other page in same interval");
  if(adapter_mode==4){auto serial=v::control.serial;++v::control.serial;
   check(!rejected.acquire_borrowed(*t,r,a,cap),"cap refuses changed registered serial");v::control.serial=serial;}
  if(adapter_mode==5){t->usercopy_pagemap_closing=true;
   check(!rejected.acquire_borrowed(*t,r,a,cap),"cap refuses closing subject after mint");t->usercopy_pagemap_closing=false;}
  if(adapter_mode==6){check(rejected.acquire_borrowed(*t,r,a,cap),"one genuine installer acquires");
   check(!rejected.acquire_borrowed(*t,r,a,cap),"nonempty installer cannot reacquire");}
  if(adapter_mode==7){auto*root=t->pagemap;t->pagemap=nullptr;
   check(!rejected.acquire_borrowed(*t,r,a,cap),"cap refuses lost current root after mint");t->pagemap=root;}
  if(adapter_mode==8){t->state=st::TaskState::EXITING;
   check(!rejected.acquire_borrowed(*t,r,a,cap),"cap refuses non-ACTIVE subject after mint");t->state=st::TaskState::ACTIVE;}
 }
 return ker::syscall::vmem::materialize_native_file_page_borrowing_root(t,r,a,f,p,cap);
}
struct World {
 void*storage{};Root*root{};Root*foreign{};st::Task subject,worker,other;ker::vfs::File file;
 fm::FileMappingLifetime*family{};fm::FileMappingBinding*binding{};uint64_t generation{};
 World(){
  check(lock_depth==0,"setup outside locks");storage=std::aligned_alloc(4096,(1U<<24)+4096);check(storage,"two roots allocation");
  root=::new(storage)Root{};foreign=::new(static_cast<char*>(storage)+(1U<<24))Root{};
  check(v::user_pagemap_access_gate_index(root)==v::user_pagemap_access_gate_index(foreign),"actual4096stripe collision");
  subject.pagemap=other.pagemap=root;worker.type=st::TaskType::DAEMON;worker.ref_count=2;other_task=&other;
  v::registered_root=root;v::control.serial=17;v::kernel_pagemap=nullptr;
  family=fm::FileMappingLifetime::prepare(file);check(family,"genuine family prepared");
  check(family&&family->reclassify(fm::FamilyOwner::PREPARED,fm::FamilyOwner::LAZY),"actual LAZY owner");
  Row r{};r.start=0x10000;r.end=0x12000;r.prot=5;r.flags=2;r.kind=st::LazyVmemKind::NATIVE_FILE_BACKED;
  r.lifetime=family;r.file_offset=4096;r.file_dev=1;r.file_ino=2;r.file_size=8192;
  subject.lazy_vmem_ranges.push_back(r);
  check(family->try_acquire(fm::FamilyOwner::LAZY),"second real Task range owns LAZY family reference");
  other.lazy_vmem_ranges.push_back(r);
  auto*prepared=fm::FileMappingBinding::prepare(*family,root,17,r.start,r.end,r.file_offset);binding=prepared;
  check(prepared,"actual binding prepared");auto flags=v::user_mapping_pin_lock.lock_irqsave();
  check(v::file_mapping_index.attach_locked(prepared,17)&&!prepared,"actual INDEX owner publication");
  v::user_mapping_pin_lock.unlock_irqrestore(flags);
  v::lazy_file_fault_worker=&worker;v::lazy_file_fault_admission=true;v::lazy_file_fault_worker_starting=false;
  v::lazy_file_fault_next_sequence=1;stop=fail_allocation=fail_io=false;queryable=true;io_hook={};adapter_mode=0;
  ker::mod::sched::current=&subject;
 }
 void publish(){mm::paging::PageFault f{};f.fetch=1;
  capture_entry_accesses=subject.usercopy_pagemap_accesses.load();unsigned const before_capture=captures;
  check(capture_entry_accesses==0,"ordinary probe starts with zero preexisting local leases");
  check(v::probe_lazy_file_fault(&subject,0x10000,f,true,generation)==v::LazyFileFaultDisposition::PARK,"actual NATIVE work admitted");
  check(captures==before_capture+1,"ordinary probe captures exactly once before admission");
  check(generation==1&&subject.ref_count==2&&subject.usercopy_pagemap_accesses==1,"issued work Task/local owners");
  check(v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(root)).readers==1,"exact outer stripe reader");
  check(family->owner_counts().count(fm::FamilyOwner::SNAPSHOT)==1,"actual work SNAPSHOT owner");
 }
 void completed(bool ok){
  check(subject.lazy_file_fault_completion_generation==generation&&subject.lazy_file_fault_status==(ok?0:-EFAULT),"actual finish publishes exact status/generation");
  check(subject.ref_count==1&&subject.usercopy_pagemap_accesses==0,"Task/local owners returned");
  check(v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(root)).readers==0,"outer stripe returned exactly once");
  check(family->owner_counts().count(fm::FamilyOwner::SNAPSHOT)==0,"work SNAPSHOT returned");
  auto owners=binding->owner_counts();check(owners.total==1&&owners.count(fm::BindingOwner::INDEX)==1,"PIN and INSTALLER return to INDEX baseline");
  for(auto const&slot:v::lazy_file_fault_work)check(!slot.task,"pool is empty after finish");
 }
 ~World(){
  // All continuations/threads are synchronous and joined before this boundary.
  subject.usercopy_pagemap_closing=false;subject.state=st::TaskState::ACTIVE;subject.pagemap=root;
  subject.lazy_vmem_ranges.clear();other.lazy_vmem_ranges.clear();
  family->release(fm::FamilyOwner::LAZY);family->release(fm::FamilyOwner::LAZY);
  fm::FileMappingTlbRetirement retirement;auto flags=v::user_mapping_pin_lock.lock_irqsave();
  check(v::file_mapping_index.detach_locked(*binding,retirement),"actual indexed binding detach");
  v::user_mapping_pin_lock.unlock_irqrestore(flags);retirement.release_after_completed_tlb();drain_retirements();
  check(file.refcount==1,"genuine family releases exact File baseline");
  if(root->page){drop(root->page);root->page=nullptr;}
  check(subject.ref_count==1&&subject.usercopy_pagemap_accesses==0,"scope owns no Task/root debt");
  v::lazy_file_fault_worker=nullptr;v::registered_root=nullptr;ker::mod::sched::current=nullptr;other_task=nullptr;
  root->~Root();foreign->~Root();std::free(storage);
 }
};
void worker_case(bool collision,unsigned mode,bool alloc_failure=false,bool io_failure=false,bool stale=false,bool stale_binding=false){
 ++cases;World w;w.publish();adapter_mode=mode;fail_allocation=alloc_failure;fail_io=io_failure;
 unsigned before_alloc=allocations,before_free=frees,before_read=reads,before_maps=maps;
 if(stale)io_hook=[&]{w.subject.lazy_vmem_ranges.clear();};
 if(stale_binding)io_hook=[&]{++v::control.serial;};
 std::atomic<bool>exclusive_entered{};std::thread waiter;
 auto&gate=v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(w.root));
 if(collision){waiter=std::thread([&]{auto token=v::begin_user_pagemap_exclusive(w.foreign);exclusive_entered=true;v::end_user_pagemap_exclusive(token);});
  auto until=std::chrono::steady_clock::now()+std::chrono::seconds(2);
  while(!gate.teardown.load()&&std::chrono::steady_clock::now()<until)std::this_thread::yield();
  check(gate.teardown.load()&&!exclusive_entered.load()&&gate.readers==1,"real exclusive waiting behind issued reader");
  v::FileMappingInstallerLease ordinary;check(!ordinary.acquire(w.subject,w.subject.lazy_vmem_ranges[0],0x10000),"ordinary acquire still refuses pending teardown");
 }
 ker::mod::sched::current=&w.worker;v::lazy_file_fault_worker_loop();
 if(waiter.joinable())waiter.join();
 bool const ok=!alloc_failure&&!io_failure&&!stale&&!stale_binding;w.completed(ok);
 v::control.serial=17;
 check(w.worker.ref_count==1&&w.worker.holds==0&&!v::lazy_file_fault_worker,"actual worker natural stop releases publication");
 check(!collision||exclusive_entered.load(),"exclusive entered only after actual finish released outer reader");
 check(!gate.teardown.load(),"exclusive returns exact gate bit");
 check((maps-before_maps)==(ok?1U:0U),"independent PTE publication count");
 if(ok){check(w.root->page&&w.root->address==0x10000,"RX page present at exact address");
  check(w.root->map_flags==5&&(w.root->map_flags&mm::paging::PAGE_WRITE)==0&&
        (w.root->map_flags&mm::paging::PAGE_NX)==0,"present user readonly executable PTE flags");
  if(w.root->page){auto*bytes=static_cast<unsigned char*>(w.root->page);
   for(size_t i=0;i<4096;++i)check(bytes[i]==37,"exact backend bytes in published page");}}
 check(allocations-before_alloc==(alloc_failure?0U:1U),"one candidate or allocation refusal");
 check(frees-before_free==((!ok&&!alloc_failure)?1U:0U),"failed candidate freed after installer return");
 check(reads-before_read==(alloc_failure?0U:1U),"IO never before admitted installer/allocation");io_hook={};
}
void refusal_case(unsigned axis){
 ++cases;World w;w.publish();v::LazyFileFaultWork taken;size_t index{};
 check(v::take_lazy_file_fault(taken,index),"actual take supplies issued work");auto&slot=v::lazy_file_fault_work.at(index);
 ker::mod::sched::current=&w.worker;unsigned before_alloc=allocations,before_read=reads;
 switch(axis){case 0:slot.issued=false;break;case 1:++slot.generation;break;case 2:++slot.sequence;break;
 case 3:slot.access_gate=v::USER_PAGEMAP_ACCESS_GATE_NONE;break;case 4:++slot.vaddr;break;
 case 5:slot.range.kind=st::LazyVmemKind::FILE_BACKED;break;case 6:v::lazy_file_fault_worker=nullptr;ker::mod::sched::current=nullptr;break;
 case 7:ker::mod::sched::current=&w.subject;break;case 8:queryable=false;break;
 case 9:w.subject.usercopy_pagemap_closing=true;break;case 10:w.subject.state=st::TaskState::EXITING;break;
 case 11:w.subject.pagemap=w.foreign;break;case 12:v::control.serial=0;break;case 13:v::registered_root=nullptr;break;
 case 14:v::kernel_pagemap=w.root;break;case 15:w.subject.type=st::TaskType::DAEMON;break;
 case 16:++slot.range.file_offset;break;case 17:taken.issued=false;break;
 case 18:slot.range.lifetime=nullptr;break;case 19:slot.task=nullptr;break;
 case 20:w.subject.pagemap=nullptr;break;default:std::abort();}
 check(!v::materialize_issued_native_file_fault(taken,index),"invalid actual issued context refuses capability/materialization");
 check(allocations==before_alloc&&reads==before_read&&!w.root->page,"refusal performs no allocation/backend/PTE");
 // Restore only finite public publication state; no private owner field changes.
 w.subject.usercopy_pagemap_closing=false;w.subject.state=st::TaskState::ACTIVE;w.subject.pagemap=w.root;w.subject.type=st::TaskType::PROCESS;
 v::control.serial=17;v::registered_root=w.root;v::kernel_pagemap=nullptr;queryable=true;
 ker::mod::sched::current=&w.worker;v::finish_lazy_file_fault(taken,index,-EFAULT);w.completed(false);
}
void already_mapped_race(){
 ++cases;World w;w.publish();unsigned before_alloc=allocations,before_free=frees,before_maps=maps,before_acks=acks;
 io_hook=[&]{auto*p=allocate();std::memset(p,0x7b,4096);w.root->address=0x10000;w.root->page=p;w.root->map_flags=5;};
 ker::mod::sched::current=&w.worker;v::lazy_file_fault_worker_loop();io_hook={};w.completed(true);
 check(allocations-before_alloc==2&&frees-before_free==1,"ALREADY_MAPPED returns only losing private candidate");
 check(maps==before_maps&&acks==before_acks+1,"actual ALREADY_MAPPED path invalidates instead of republishing");
 check(w.root->page&&static_cast<unsigned char*>(w.root->page)[0]==0x7b,"concurrent winning PTE bytes remain exact");
}
void actual_slot_payload(){
 ++cases;World w;w.publish();v::LazyFileFaultWork taken;size_t index{};
 check(v::take_lazy_file_fault(taken,index),"actual take before local payload divergence");
 // Local authority fields still correlate, but backend inputs are deliberately
 // different. Only the actual immutable indexed slot may feed the backend.
 taken.fault.writable=1;taken.fault.fetch=0;taken.placement.policy.nodes=1;
 taken.placement.local_node=99;taken.range.file_size=0;
 ker::mod::sched::current=&w.worker;
 bool const ok=v::materialize_issued_native_file_fault(taken,index);
 v::finish_lazy_file_fault(taken,index,ok?0:-EFAULT);w.completed(true);
 check(ok&&w.root->page&&static_cast<unsigned char*>(w.root->page)[0]==37,
       "actual slot metadata, fault and placement override no backend input from local copy");
}
void raw_saturation(){
 ++cases;World w;auto&gate=v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(w.root));
 gate.readers=UINT32_MAX;size_t index=0;check(!v::acquire_user_pagemap_access(w.root,false,index)&&index==v::USER_PAGEMAP_ACCESS_GATE_NONE,"ordinary reader MAX refuses immediately");
 capture_entry_accesses=w.subject.usercopy_pagemap_accesses.load();unsigned before_capture=captures;
 check(capture_entry_accesses==0,"stripe MAX probe starts with zero preexisting local leases");
 uint64_t g{};check(v::probe_lazy_file_fault(&w.subject,0x10000,{},true,g)==v::LazyFileFaultDisposition::FAILED,"MAX publishes no work/cap");
 check(captures==before_capture+1,"stripe MAX probe still captures before failed admission");
 check(w.subject.ref_count==1&&w.subject.usercopy_pagemap_accesses==0&&g==0,"MAX unwinds all prefix owners");gate.readers=0;
 w.subject.usercopy_pagemap_accesses=UINT32_MAX;
 capture_entry_accesses=w.subject.usercopy_pagemap_accesses.load();before_capture=captures;
 check(capture_entry_accesses==UINT32_MAX,"Task local MAX is genuinely preexisting at operation entry");
 check(v::probe_lazy_file_fault(&w.subject,0x10000,{},true,g)==v::LazyFileFaultDisposition::FAILED,"Task local MAX has no cap");
 check(captures==before_capture+1,"Task local MAX probe still captures before failed admission");
 check(w.subject.usercopy_pagemap_accesses==UINT32_MAX&&w.subject.ref_count==1&&g==0,
       "Task local MAX admission changes no preexisting lease or prefix owner");
 w.subject.usercopy_pagemap_accesses=0;
}
void run(){
 static_assert(!std::is_default_constructible_v<v::BorrowedFileMappingRootAccess>);
 static_assert(!std::is_copy_constructible_v<v::BorrowedFileMappingRootAccess>);
 static_assert(!std::is_move_constructible_v<v::BorrowedFileMappingRootAccess>);
 worker_case(false,0);worker_case(true,0);
 for(unsigned mode=1;mode<=8;++mode)worker_case(false,mode);
 worker_case(true,0,true);worker_case(true,0,false,true);worker_case(true,0,false,false,true);
 worker_case(true,0,false,false,false,true);already_mapped_race();
 for(unsigned axis=0;axis<=20;++axis)refusal_case(axis);actual_slot_payload();raw_saturation();
 check(allocations==frees,"all private frame candidates/PTEs return");
 std::printf("%s native_lazy cases=%u assertions=%u adapter_cases=%u failures=%u\n",failures?"FAIL":"PASS",cases,assertions,adapter_cases,failures);
}
}
int main(){fixture::run();return fixture::failures?1:0;}
'''


def definition(source: str, anchor: str, declaration: bool = False) -> tuple[str, dict]:
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda m: ''.join('\n' if c == '\n' else ' ' for c in m[0]), source)
    begin = -1
    while True:
        begin = source.index(anchor, begin + 1)
        opening = clean.index('{', begin)
        if ';' not in clean[begin:opening]:
            break
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
                body = source[begin:end]
                return body, dict(anchor=anchor, bytes=len(body.encode()), sha256=hashlib.sha256(body.encode()).hexdigest(),
                                  line=source.count('\n', 0, begin) + 1)
    raise ValueError('unterminated complete source definition: ' + anchor)


def generate(virt: str, vmem: str, header: str, control: str) -> tuple[str, list[dict]]:
    actual = []
    def take(source: str, anchor: str, declaration: bool = False) -> str:
        body, row = definition(source, anchor, declaration)
        actual.append(row)
        return body
    task_header = (KERN / 'platform/sched/task.hpp').read_text()
    task_source = (KERN / 'platform/sched/task.cpp').read_text()
    alloc_header = (KERN / 'platform/mm/numa_alloc.hpp').read_text()
    alloc_source = (KERN / 'platform/mm/numa_alloc.cpp').read_text()
    policy = (KERN / 'platform/mm/numa_policy.hpp').read_text()
    paging = (KERN / 'platform/mm/paging.hpp').read_text()
    gate_start = virt.index('constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT =')
    gate_end = virt.index('[[nodiscard]] auto user_pagemap_access_gate_index', gate_start)
    async_start = virt.index('enum class LazyFileFaultDisposition')
    async_end = virt.index('void wake_lazy_file_fault_worker()', async_start)
    classes = 'namespace detail {struct IssuedNativeFileFaultAccess;}\n' + take(header, 'class BorrowedFileMappingRootAccess {', True)
    classes += '\n' + take(header, 'class FileMappingInstallerLease {', True)
    cap_matches = take(virt, 'auto BorrowedFileMappingRootAccess::matches(')
    borrowed = take(virt, 'auto FileMappingInstallerLease::acquire_borrowed(')
    if control == 'ordinary-reader':
        borrowed = '''auto FileMappingInstallerLease::acquire_borrowed(sched::task::Task& task,
 const sched::task::LazyVmemRange& range,uint64_t address,const BorrowedFileMappingRootAccess&) -> bool {
 return acquire(task,range,address);
}'''
    elif control == 'accept-all-caps':
        cap_matches = '''auto BorrowedFileMappingRootAccess::matches(const sched::task::Task&,
 const sched::task::LazyVmemRange&,uint64_t,uint64_t)const noexcept -> bool{
 (void)task_;(void)root_;(void)range_;(void)address_;(void)serial_;(void)gate_;return true;
}'''
    elif control != 'current':
        raise ValueError('unknown control')
    installer = '\n'.join([cap_matches, take(virt, 'FileMappingInstallerLease::~FileMappingInstallerLease()'),
                            take(virt, 'auto FileMappingInstallerLease::acquire('), borrowed,
                            take(virt, 'auto FileMappingInstallerLease::acquire_binding(')])
    # The finite typed-call endpoint receives only a genuine minted cap and
    # delegates to the complete actual named materializer. Its invalid-cap
    # probes are separate Host oracles, not a production hook or cap constructor.
    issuer = take(virt, 'struct IssuedNativeFileFaultAccess {', True)
    named_call = 'ker::syscall::vmem::materialize_native_file_page_borrowing_root('
    if issuer.count(named_call) != 1:
        raise ValueError('expected one exact named issuer call')
    issuer = issuer.replace(named_call, 'fixture::borrowed_entry(')
    values = {
        'ACTUAL_PAGING_FLAGS': '\n'.join(re.findall(r'^(?:const static|inline constexpr) uint64_t PAGE_(?:PRESENT|WRITE|USER|NX|SHARED|WRITE_RESTRICTED) =[^;]+;', paging, re.M)),
        'ACTUAL_POLICY_TYPES': policy[policy.index('enum class Mode :'):policy.index('[[nodiscard]] constexpr auto valid_policy')],
        'ACTUAL_ALLOC_TYPES': alloc_header[alloc_header.index('struct AllocationContext {'):alloc_header.index('// NEVER and selected-node attempts')],
        'ACTUAL_SPAN_METHODS': alloc_source[alloc_source.index('OwnedSpan::~OwnedSpan()'):alloc_source.index('auto capture_allocation_context(')],
        'ACTUAL_ALLOC_ADAPTER': take(alloc_source, '[[gnu::noinline]] void try_allocate_pages_into('),
        'ACTUAL_RANGE_TYPES': task_header[task_header.index('enum class LazyVmemKind'):task_header.index('using LazyVmemRangeVec')],
        'ACTUAL_TASK_LEASES': '\n'.join(take(task_source, x) for x in ('auto Task::try_acquire_usercopy_pagemap(', 'void Task::release_usercopy_pagemap()')),
        'ACTUAL_CAP_CLASSES': classes,
        'ACTUAL_DEMAND_TYPES': take(header, 'enum class DemandOrigin', True) + '\n' + take(virt, 'struct DemandContext {', True) + '\n' + take(virt, 'class DemandContextStorage {', True),
        'ACTUAL_GATE_DECLS': virt[gate_start:gate_end],
        'ACTUAL_GATE_BODIES': '\n'.join(take(virt, x) for x in ('[[nodiscard]] auto user_pagemap_access_gate_index(', 'template <typename WaitForTeardown>',
            '[[nodiscard]] auto acquire_user_pagemap_access(', 'void release_user_pagemap_access(', '[[nodiscard]] auto begin_user_pagemap_exclusive(',
            'void end_user_pagemap_exclusive(', 'auto try_acquire_lazy_file_fault_reader(')),
        'ACTUAL_MATCHES': take(virt, 'auto lazy_file_range_matches(') + '\n' + take(virt, 'auto file_binding_matches_range('),
        'ACTUAL_ROOT_CURRENT': take(virt, 'auto borrowed_native_file_root_current('),
        'ACTUAL_CAPTURE_SERIAL': take(virt, 'auto capture_user_root_incarnation('),
        'ACTUAL_INSTALL': take(virt, 'auto install_lazy_file_page_if_current('),
        'ACTUAL_CAP_AND_INSTALLER': installer,
        'ACTUAL_MATERIALIZER': take(vmem, 'auto materialize_lazy_file_page_impl('),
        'ACTUAL_NAMED_WRAPPER': take(vmem, 'auto materialize_native_file_page_borrowing_root('),
        'ACTUAL_ASYNC_DECLS': virt[async_start:async_end],
        'ACTUAL_ASYNC_BODIES': '\n'.join(take(virt, x) for x in ('void wake_lazy_file_fault_worker()', 'auto probe_lazy_file_fault(',
            'auto take_lazy_file_fault(', 'void finish_lazy_file_fault(', 'void lazy_file_fault_worker_loop()')),
        'ACTUAL_ISSUER': issuer,
        'ACTUAL_DISPATCHER': take(virt, 'auto materialize_issued_native_file_fault('),
    }
    text = PRELUDE
    for key, value in values.items():
        marker = '// ' + key
        if text.count(marker) != 1:
            raise ValueError('source seam missing/duplicated: ' + marker)
        text = text.replace(marker, value)
        actual.append(dict(anchor=key, bytes=len(value.encode()), sha256=hashlib.sha256(value.encode()).hexdigest()))
    return text + CASES, actual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--virt-source', type=Path, default=KERN / 'platform/mm/virt.opt.cpp')
    parser.add_argument('--virt-header', type=Path, default=KERN / 'platform/mm/virt.hpp')
    parser.add_argument('--vmem-source', type=Path, default=KERN / 'syscalls_impl/vmem/sys_vmem.cpp')
    parser.add_argument('--cxx', default=os.environ.get('CXX', os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++')))
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    scratch = None
    groups = []
    watched, before = [], []
    result = dict(pass_=False, commands=[], scratch_removed=False, native_executor_runtime_qualified=False)
    try:
        sources = [args.virt_source.resolve(strict=True), args.virt_header.resolve(strict=True), args.vmem_source.resolve(strict=True)]
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name one executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--cxx must name one executable compiler ELF')
        subreaper()
        watched = sources + [KERN / p for p in ('platform/sched/task.hpp', 'platform/sched/task.cpp',
            'platform/mm/numa_alloc.hpp', 'platform/mm/numa_alloc.cpp', 'platform/mm/numa_policy.hpp', 'platform/mm/paging.hpp',
            'platform/mm/file_mapping_lifetime.cpp', 'platform/mm/file_mapping_lifetime.hpp',
            'platform/mm/lifetime_retirement_executor.hpp')] + [Path(__file__).resolve(),
            ROOT / 'tests/host/unit/file_mmap_registry_runtime_test.py', compiler]
        before = [pin(path) for path in watched]
        scratch = Path(tempfile.mkdtemp(prefix='wos-native-lazy-file-', dir='/tmp'))
        include = scratch / 'include'
        include.mkdir()
        (include / 'fixture_common.hpp').write_text(FAMILY_COMMON)
        (scratch / 'family-seams.cpp').write_text(FAMILY_SEAMS)
        for name, text in FAMILY_HEADERS.items():
            path = include / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        for name in ('file_mapping_lifetime.hpp', 'lifetime_retirement_executor.hpp'):
            target = include / 'platform/mm' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((KERN / 'platform/mm' / name).read_bytes())
        for control in ('current', 'ordinary-reader', 'accept-all-caps'):
            source, actual = generate(virt=sources[0].read_text(), vmem=sources[2].read_text(),
                                      header=sources[1].read_text(), control=control)
            result.setdefault('actual_definitions', {})[control] = actual
            translation, binary = scratch / (control + '.cpp'), scratch / control
            translation.write_text(source)
            argv = [str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror',
                    '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-I', str(include),
                    str(translation), str(KERN / 'platform/mm/file_mapping_lifetime.cpp'),
                    str(scratch / 'family-seams.cpp'), '-o', str(binary)]
            compile_row = command(argv, scratch / (control + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(compile_row)
            if compile_row['exit'] != 0:
                print((scratch / (control + '-compile.log')).read_text(errors='replace')[-32768:])
                raise ValueError(control + ': compile/link failed; not a negative-control success')
            run_log = scratch / (control + '-run.log')
            row = command([str(binary)], run_log, scratch, deadline, groups)
            result['commands'].append(row)
            log = run_log.read_text(errors='replace')
            print(log[-32768:], end='')
            passed = row['exit'] == 0 and re.search(r'^PASS native_lazy cases=38 assertions=\d+ adapter_cases=8 failures=0$', log, re.M)
            if (control == 'current') != bool(passed):
                raise ValueError(control + ': same connected oracle did not distinguish expected behavior')
            if control != 'current' and (row['exit'] == 0 or not re.search(
                    r'^FAIL native_lazy cases=38 assertions=\d+ adapter_cases=8 failures=[1-9]\d*$', log, re.M)):
                raise ValueError(control + ': compiled negative must finish and fail the same complete runtime oracle')
            if control != 'current':
                expected_marker = {
                    'ordinary-reader': 'FAIL actual finish publishes exact status/generation',
                    'accept-all-caps': 'FAIL cap refuses equal-root wrong Task',
                }[control]
                if expected_marker not in log.splitlines():
                    raise ValueError(control + ': intended connected oracle failure marker is missing')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups {groups}; scratch preserved'
        if before:
            try:
                result['sources_held'] = [pin(path) for path in watched] == before
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']:
                result['pass_'] = False
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole90-second invocation including cleanup exceeded'
        print(json.dumps({k: v for k, v in result.items() if k != 'actual_definitions'}))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
