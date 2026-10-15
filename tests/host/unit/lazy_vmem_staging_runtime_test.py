#!/usr/bin/env python3
"""Compile complete lazy-row staging/core and actual headers with fault oracles.

Task lifetime, MM root-incarnation lookup, publication/IRQ locks, TMPFS refusal
and a queued callback port are explicit Host primitives. Real SmallVec, complete
lazy_vmem.cpp, complete FileMappingLifetime core, actual row declarations and
checked File CAS are compiled. This is metadata/owner testing, not native MM,
backend coherence, executor scheduling or public syscall qualification.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "modules/kern/src").is_dir())
KERN = ROOT / "modules/kern/src"
SCRATCH_BUDGET = 64 << 20
LOG_BUDGET = 1 << 20
FILE_BUDGET = 8 << 20

COMMON = r'''
#pragma once
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
#include <type_traits>
#include <utility>
namespace fixture {
inline unsigned failures{}, assertions{}, cases{}, irq{}, publication{}, allocations{}, frees{}, puts{}, callbacks{}, shared_cases{}, fork_cases{}, scalar_allocations{};
inline int fail_array_after{-1};
inline int fail_scalar_after{-1}, fail_reserve_after{-1}, fail_attach_after{-1};
inline bool demand_constructor_allowed{};
inline bool irq_enabled=true;
inline unsigned preempt{};
inline void (*publication_hook)(){};
inline void check(bool x,const char* why) { ++assertions; if(!x && failures++<24) std::fprintf(stderr,"FAIL %s\n",why); }
struct Lock {
    bool held{};
    uint64_t lock_irqsave() { check(!held,"recursive IRQ lock"); held=true; ++irq; return 0; }
    void unlock_irqrestore(uint64_t) { check(held && irq,"IRQ lock balanced"); held=false; --irq; }
};
}
namespace ker::mod::sys { using Spinlock=fixture::Lock; }
namespace ker::mod::mm::paging { constexpr uint64_t PAGE_SIZE=4096, PAGE_SHIFT=12; struct PageTable { uint64_t serial{1}; }; }
namespace ker::vfs {
struct File { std::atomic<int> refcount{1}; };
bool vfs_try_retain_file(File*) noexcept;
inline void vfs_put_file(File* f) {
    fixture::check(!fixture::irq&&!fixture::publication,"File put outside guards");
    fixture::check(f && f->refcount.fetch_sub(1)>0,"owned File release"); ++fixture::puts;
}
inline int vfs_put_file_result(File* f) { vfs_put_file(f); return 0; }
}
namespace ker::mod::sched {
inline bool interrupts_enabled(){return fixture::irq_enabled;}
inline unsigned preempt_count(){return fixture::preempt;}
}
namespace ker::syscall::vmem {
struct SharedVmemPublicationGuard {
    SharedVmemPublicationGuard(){
        if(fixture::publication_hook) fixture::publication_hook();
        fixture::check(!fixture::publication,"publication nonrecursive"); ++fixture::publication;
    }
    ~SharedVmemPublicationGuard(){fixture::check(fixture::publication==1,"publication balanced");--fixture::publication;}
};
}
namespace ker::mod::mm::virt {
inline bool capture_user_root_incarnation(paging::PageTable* root,uint64_t& empty) noexcept {
    fixture::check(fixture::publication,"identity captured under publication; MM pin lock is primitive-local");
    if(empty || !root || !root->serial)return false;empty=root->serial;return true;
}
}
'''

SHARED_SEAMS = r'''
namespace ker::mod::sched {
using ActiveTaskPredicate=bool(*)(task::Task*,void*);
inline std::array<task::Task*,16> active_registry{};
inline uint32_t active_task_count{};
inline int reported_count{-1};
inline fixture::Lock global_task_registry_lock;
inline task::Task* current_task{};
inline auto active_task_slot(uint32_t i)->task::Task* {return active_registry.at(i);}
inline auto get_active_task_count()->uint64_t {return reported_count<0?active_task_count:static_cast<uint64_t>(reported_count);}
inline auto get_current_task()->task::Task* {return current_task;}
inline auto can_query_current_task()->bool {return current_task!=nullptr;}
}
namespace ker::mod::dbg {
[[noreturn]] inline void panic_handler(const char* why){std::fprintf(stderr,"production panic: %s\n",why);std::abort();}
}
'''

SEAMS = r'''
namespace fm=ker::mod::mm::file_mapping;
namespace fixture {
std::array<fm::LifetimeRetirementTicket,4096> tickets{};
size_t used{};
fm::LifetimeRetirementTicket* reserve(void*,fm::LifetimeRetirementRun run,void* argument) noexcept {
    if(fail_reserve_after==0)return nullptr;if(fail_reserve_after>0)--fail_reserve_after;
    if(used==tickets.size())return nullptr;auto* t=&tickets[used++];*t={run,argument,false};return t;
}
void submit(void*,fm::LifetimeRetirementTicket& ticket) noexcept {
    check(!ticket.ready,"submit once");ticket.ready=true;
}
bool observe(void*,fm::LifetimeRetirementTicket&,fm::LifetimeRetirementRun,void*,fm::RetirementCompletion&) noexcept {return false;}
bool cancel(void*,fm::LifetimeRetirementTicket&,fm::LifetimeRetirementRun,void*) noexcept {return false;}
const fm::LifetimeRetirementPort port{nullptr,reserve,submit,observe,cancel};
void drain(){
    check(!irq&&!publication,"callback drain outside guards");
    for(size_t turn=0;turn<=used;++turn){bool progressed=false;
        for(size_t i=0;i<used;++i)if(tickets[i].ready){auto run=tickets[i].run;auto arg=tickets[i].argument;tickets[i].ready=false;run(arg);++callbacks;progressed=true;}
        if(!progressed)return;
    }check(false,"bounded queued dependency drain");
}
}
namespace ker::mod::mm::file_mapping {
auto LifetimeRetirementExecutor::port() noexcept -> const LifetimeRetirementPort* {return &fixture::port;}
RetirementCompletion::RetirementCompletion(RetirementCompletion&&) noexcept=default;
auto RetirementCompletion::operator=(RetirementCompletion&&) noexcept -> RetirementCompletion& = default;
RetirementCompletion::~RetirementCompletion()=default;
void RetirementCompletion::reset() noexcept {}
auto RetirementCompletion::query() const noexcept -> RetirementReturn {return {};}
}
void* operator new[](std::size_t n){if(auto* p=std::malloc(n?n:1))return p;throw std::bad_alloc();}
void* operator new[](std::size_t n,const std::nothrow_t&) noexcept {
    fixture::check(!fixture::irq&&!fixture::publication,"array allocation outside all guards");++fixture::allocations;
    if(fixture::fail_array_after==0)return nullptr;
    if(fixture::fail_array_after>0)--fixture::fail_array_after;
    return std::malloc(n?n:1);
}
void operator delete[](void* p) noexcept {
    if(p){fixture::check(!fixture::irq&&!fixture::publication,"array destruction outside all guards");++fixture::frees;}
    std::free(p);
}
void operator delete[](void* p,std::size_t) noexcept {operator delete[](p);}
void* operator new(std::size_t n){if(auto* p=std::malloc(n?n:1))return p;throw std::bad_alloc();}
void* operator new(std::size_t n,const std::nothrow_t&) noexcept {
    fixture::check(!fixture::irq&&!fixture::publication,"scalar owner allocation outside guards");++fixture::scalar_allocations;
    if(fixture::fail_scalar_after==0)return nullptr;if(fixture::fail_scalar_after>0)--fixture::fail_scalar_after;
    return std::malloc(n?n:1);
}
void operator delete(void* p) noexcept {if(p)fixture::check(!fixture::irq&&!fixture::publication,"scalar owner delete outside guards");std::free(p);}
void operator delete(void* p,std::size_t) noexcept {operator delete(p);}
'''

CASES = r'''
namespace fixture {
namespace st=ker::mod::sched::task;
using Root=ker::mod::mm::paging::PageTable;
using Kind=st::LazyVmemKind;
using Row=st::LazyVmemRange;
using Clone=st::LazyVmemClone;
void test(bool x,const char* name){++cases;check(x,name);}
Row row(unsigned i,ker::vfs::File* file=nullptr){Row r{};r.start=0x10000+i*4096;r.end=r.start+4096;r.prot=1;r.flags=2;r.file_offset=i*4096;r.file=file;r.kind=file?Kind::FILE_BACKED:Kind::ANONYMOUS;return r;}
struct World {
    Root root{41}, other{42}; st::Task source{}, destination{};ker::vfs::File raw{}, backing{};fm::FileMappingLifetime* family{};
    World(){source.pagemap=&root;destination.pagemap=&other;}
    void seed(unsigned count,bool typed=false){
        if(typed){family=fm::FileMappingLifetime::prepare(backing);check(family,"actual family prepared");}
        check(source.lazy_vmem_ranges.reserve(count),"seed reserve");
        for(unsigned i=0;i<count;++i){auto r=row(i,&raw);if(typed && i%3==1){r.kind=Kind::TMPFS_READ_SHARED;r.lifetime=family;}
            check(st::try_retain_lazy_vmem_range(r),"seed owns exact row");check(source.lazy_vmem_ranges.push_back_no_grow(r),"seed append");}
    }
    ~World(){st::release_lazy_vmem_ranges(destination);st::release_lazy_vmem_ranges(source);
        if(family)family->release(fm::FamilyOwner::PREPARED);drain();
        check(raw.refcount==1 && backing.refcount==1,"File owners return to baseline");
        check(source.ref_count==1 && destination.ref_count==1,"Task owners return to baseline");}
};
World* growing_world{};
unsigned hook_calls{};
void grow_on_capture(){
    if(++hook_calls%2==0){auto r=row(growing_world->source.lazy_vmem_ranges.size(),&growing_world->raw);
        check(st::try_retain_lazy_vmem_range(r),"concurrent row owner");check(growing_world->source.lazy_vmem_ranges.push_back_no_grow(r),"concurrent no-grow row");}
}
void grow_once(){grow_on_capture();if(hook_calls==2)publication_hook=nullptr;}
void close_on_capture(){if(++hook_calls==2)growing_world->source.usercopy_pagemap_closing=true;}
void vectors(){
    {ker::util::SmallVec<uint64_t,8> v;unsigned a=allocations;check(v.reserve(8),"inline reserve");for(unsigned i=0;i<8;++i)check(v.push_back_no_grow(i),"inline append");test(allocations==a,"inline reserve allocates nothing");
     check(v.reserve(33),"spilled reserve");a=allocations;for(unsigned i=8;i<33;++i)check(v.push_back(i),"reserved normal append");test(allocations==a&&v.capacity()==33,"all reserved appends avoid allocation");test(!v.push_back_no_grow(99)&&v.size()==33,"no-grow refusal leaves vector");
     auto cap=v.capacity();fail_array_after=0;test(!v.reserve(65)&&v.capacity()==cap&&v.size()==33,"reserve OOM preserves storage/count");for(unsigned i=0;i<33;++i)check(v.at(i)==i,"reserve OOM preserves independent values");fail_array_after=-1;
     a=allocations;test(!v.reserve(SIZE_MAX)&&allocations==a,"reserve overflow refuses before allocator");test(v.push_back(33)&&allocations==a+1,"first unreserved append grows once");}
    {ker::util::SmallVec<int,0> v;test(v.push_back(17)&&v.at(0)==17,"zero-inline first append");}
    {struct Layout {std::array<uint64_t,8> a;uint64_t* heap;size_t size,capacity;};
     test(sizeof(ker::util::SmallVec<uint64_t,8>)==sizeof(Layout),"SmallVec fields/layout unchanged");}
    {World w;w.seed(25,true);int raw=w.raw.refcount;auto owners=w.family->owner_counts();Clone c;
     test(c.prepare(w.source)==0,"mixed RAW/typed capture");test(w.raw.refcount>raw&&w.family->owner_counts().total>owners.total,"captured rows own true counts");
     {ker::syscall::vmem::SharedVmemPublicationGuard p;test(c.matches(w.source),"fresh capture matches");test(c.consume(w.destination),"consume counted rows");}
     test(w.destination.lazy_vmem_ranges.size()==25&&c.ranges().empty(),"complete consume moves all rows");c.reset();test(w.source.ref_count==1,"capture Task released after transfer");}
    {World w;w.seed(17,true);Clone c;int raw=w.raw.refcount;auto total=w.family->owner_counts().total;fail_array_after=0;
     test(c.prepare(w.source)==-ENOMEM,"capture reserve OOM");fail_array_after=-1;test(w.raw.refcount==raw&&w.family->owner_counts().total==total&&w.source.ref_count==1,"OOM no owner/source mutation");}
    {World w;w.seed(8);check(w.source.lazy_vmem_ranges.reserve(64),"concurrent row capacity prepared");Clone c;
     growing_world=&w;hook_calls=0;publication_hook=grow_once;
     test(c.prepare(w.source)==0&&c.ranges().size()==9,"capacity race retries with real grown cohort");publication_hook=nullptr;growing_world=nullptr;}
    {World w;w.seed(8);check(w.source.lazy_vmem_ranges.reserve(64),"churn capacity prepared");Clone c;
     growing_world=&w;hook_calls=0;publication_hook=grow_on_capture;
     test(c.prepare(w.source)==-EAGAIN,"bounded continuous cohort growth refuses precommit");publication_hook=nullptr;growing_world=nullptr;
     test(c.ranges().empty()&&w.source.ref_count==1&&w.raw.refcount==13,"churn leaves only actual source owners");}
    {World w;w.seed(8);Clone c;growing_world=&w;hook_calls=0;publication_hook=close_on_capture;
     test(c.prepare(w.source)==-EAGAIN,"source exit between sizing and capture refuses");publication_hook=nullptr;growing_world=nullptr;
     test(c.ranges().empty()&&w.source.ref_count==1&&w.raw.refcount==9,"exit race preserves owner baseline");w.source.usercopy_pagemap_closing=false;}
    {World w;w.seed(8,true);ker::vfs::File saturated{};saturated.refcount=INT_MAX;w.source.lazy_vmem_ranges.at(6).file=&saturated;
     // Replace exactly the old RAW row owner in the independent fixture.
     ker::vfs::vfs_put_file(&w.raw);int raw=w.raw.refcount;auto total=w.family->owner_counts().total;Clone c;
     test(c.prepare(w.source)==-EOVERFLOW,"checked RAW saturation refuses");test(w.raw.refcount==raw&&w.family->owner_counts().total==total&&w.source.ref_count==1,"retained typed/RAW prefix unwound");
     w.source.lazy_vmem_ranges.at(6).file=&w.raw;check(ker::vfs::vfs_try_retain_file(&w.raw),"restore fixture owner");}
    {World w;w.seed(2,true);Clone c;check(c.prepare(w.source)==0,"mutation snapshot");
     for(unsigned field=0;field<13;++field){auto old=w.source.lazy_vmem_ranges.at(0);auto& r=w.source.lazy_vmem_ranges.at(0);
       switch(field){case 0:++r.start;break;case 1:++r.end;break;case 2:++r.prot;break;case 3:++r.flags;break;case 4:++r.file_offset;break;case 5:++r.file_dev;break;case 6:++r.file_ino;break;case 7:++r.file_size;break;case 8:++r.file_mtime_sec;break;case 9:++r.file_mtime_nsec;break;case 10:++r.file_ctime_sec;break;case 11:++r.file_ctime_nsec;break;case 12:r.file=&w.backing;break;}
       {ker::syscall::vmem::SharedVmemPublicationGuard p;test(!c.matches(w.source)&&!c.consume(w.destination),"exact changed row refuses transfer");}r=old;}
     ++w.root.serial;{ker::syscall::vmem::SharedVmemPublicationGuard p;test(!c.matches(w.source),"same pointer new incarnation refuses");}--w.root.serial;
     w.source.pagemap=&w.other;{ker::syscall::vmem::SharedVmemPublicationGuard p;test(!c.matches(w.source),"different root refuses");}w.source.pagemap=&w.root;
     w.source.usercopy_pagemap_closing=true;{ker::syscall::vmem::SharedVmemPublicationGuard p;test(!c.matches(w.source),"closing source refuses");}w.source.usercopy_pagemap_closing=false;
     check(w.destination.lazy_vmem_ranges.push_back(row(99)),"nonempty destination");{ker::syscall::vmem::SharedVmemPublicationGuard p;test(!c.consume(w.destination)&&c.ranges().size()==2,"nonempty destination unchanged");}w.destination.lazy_vmem_ranges.clear();}
    {World w;w.seed(9);check(w.destination.lazy_vmem_ranges.reserve(32),"empty heap destination reserved");Clone c;check(c.prepare(w.source)==0,"empty heap prepare");unsigned f=frees;
     {ker::syscall::vmem::SharedVmemPublicationGuard p;test(c.consume(w.destination),"empty retained heap transfer");}test(frees==f,"no old destination heap freed during commit");}
    {World w;w.seed(1);w.source.native_file_mapping_retirement=reinterpret_cast<void*>(1);Clone c;test(c.prepare(w.source)<0,"unpublished record refuses");w.source.native_file_mapping_retirement=nullptr;
     auto old=w.source.lazy_vmem_ranges.at(0);w.source.lazy_vmem_ranges.at(0).kind=Kind::NATIVE_FILE_BACKED;test(c.prepare(w.source)==-EBUSY,"native row refuses without record");w.source.lazy_vmem_ranges.at(0)=old;}
    {World w;w.seed(1);Clone c;w.source.ref_count=UINT32_MAX;test(c.prepare(w.source)==-ESRCH,"Task ref saturation refuses");w.source.ref_count=1;
     w.root.serial=0;test(c.prepare(w.source)<0&&w.source.ref_count==1,"unknown root releases Task");w.root.serial=41;
     irq_enabled=false;test(c.prepare(w.source)==-EWOULDBLOCK,"unsafe IRQ context refuses");irq_enabled=true;preempt=1;test(c.prepare(w.source)==-EWOULDBLOCK,"preempt-disabled context refuses");preempt=0;}
    {World w;w.seed(3,true);test(!st::clone_lazy_vmem_ranges(w.destination,w.source)&&w.destination.lazy_vmem_ranges.empty(),"legacy clone refuses public typed pointer");}
    {World w;w.seed(3);int raw=w.raw.refcount;test(st::clone_lazy_vmem_ranges(w.destination,w.source),"legacy inline RAW clone stays supported");test(w.raw.refcount==raw+3,"legacy clone owns all checked RAW rows");}
    {World w;w.seed(3,true);auto expected=w.family->owner_counts().total;Row native{};native.kind=Kind::NATIVE_FILE_BACKED;native.lifetime=w.family;
     check(st::try_retain_lazy_vmem_range(native),"native typed extra owner");check(w.destination.lazy_vmem_ranges.push_back(native),"native typed cleanup row");actual_exec::release_lazy_file_refs(w.destination.lazy_vmem_ranges);
     test(w.family->owner_counts().total==expected,"actual exec release drops native typed owner");
     Row shared{};shared.kind=Kind::TMPFS_READ_SHARED;shared.lifetime=w.family;check(st::try_retain_lazy_vmem_range(shared),"public typed extra owner");check(w.destination.lazy_vmem_ranges.push_back(shared),"public typed cleanup row");actual_exec::release_lazy_file_refs(w.destination.lazy_vmem_ranges);
     test(w.family->owner_counts().total==expected,"actual exec release drops public typed owner");}
    drain();check(!irq&&!publication,"all guards released");
}
}
int main(){fixture::vectors();std::printf("%s cases=%u assertions=%u failures=%u allocations=%u callbacks=%u real_smallvec=1 real_staging=1 real_family_core=1\n",fixture::failures?"FAIL":"PASS",fixture::cases,fixture::assertions,fixture::failures,fixture::allocations,fixture::callbacks);return fixture::failures?3:0;}
'''

SHARED_CASES = r'''
namespace fixture {
using Update=ker::syscall::vmem::SharedLazyVmemUpdate;
namespace sched=ker::mod::sched;
void extra(bool x,const char* why){++shared_cases;test(x,why);}
struct SharedWorld {
    World w;st::Task sibling{},foreign{},daemon{};
    SharedWorld(){
        w.seed(10,true);sibling.pagemap=&w.root;foreign.pagemap=&w.other;daemon.pagemap=&w.root;daemon.type=st::TaskType::DAEMON;
        Clone c;check(c.prepare(w.source)==0,"shared seed clone");
        {ker::syscall::vmem::SharedVmemPublicationGuard p;check(c.consume(sibling),"shared seed transfer");}c.reset();
        sched::active_registry={&w.source,&sibling,&foreign,&daemon};sched::active_task_count=4;sched::current_task=&w.source;
    }
    bool unchanged() const {
        if(w.source.lazy_vmem_ranges.size()!=10||sibling.lazy_vmem_ranges.size()!=10)return false;
        for(unsigned i=0;i<10;++i){auto expected=row(i,const_cast<ker::vfs::File*>(&w.raw));
            if(i%3==1){expected.kind=Kind::TMPFS_READ_SHARED;expected.lifetime=w.family;}
            if(!st::lazy_vmem_ranges_equal(expected,w.source.lazy_vmem_ranges.at(i))||
               !st::lazy_vmem_ranges_equal(expected,sibling.lazy_vmem_ranges.at(i)))return false;}
        return foreign.lazy_vmem_ranges.empty()&&daemon.lazy_vmem_ranges.empty();
    }
    ~SharedWorld(){
        sched::active_task_count=0;sched::active_registry={};sched::current_task=nullptr;sched::reported_count=-1;
        st::release_lazy_vmem_ranges(sibling);
        check(sibling.ref_count==1&&foreign.ref_count==1&&daemon.ref_count==1,"shared temporary Task owners returned");
    }
};
void projection(SharedWorld& s,bool remove,uint64_t start,uint64_t end,uint64_t prot){
    for(auto* task:{&s.w.source,&s.sibling}){
        for(uint64_t address=0x10000;address<0x1a000;address+=64){
            unsigned count=0;Row actual{};
            for(auto const& r:task->lazy_vmem_ranges)if(address>=r.start&&address<r.end){actual=r;++count;}
            bool inside=address>=start&&address<end;
            if(remove&&inside){check(count==0,"removed byte has no row");continue;}
            unsigned page=static_cast<unsigned>((address-0x10000)/4096);
            check(count==1,"independent projection has exactly one row");
            check(actual.prot==(inside?prot:1),"independent byte permission");
            check(actual.file_offset+address-actual.start==address-0x10000,"independent file-byte projection");
            check(actual.kind==(page%3==1?Kind::TMPFS_READ_SHARED:Kind::FILE_BACKED),"independent exact owner kind");
            check(page%3==1?actual.lifetime==s.w.family:actual.file==&s.w.raw,"independent exact owner identity");
        }
    }
}
void shared_vectors(){
    unsigned baseline_allocations=0;
    {SharedWorld s;unsigned a=allocations;{
        Update u;check(u.prepare(s.w.source)==0,"complete cohort preparation");
        Row added=row(200,&s.w.raw);check(u.stage(Update::Change::ADD,added.start,added.end,&added)==0,"actual ADD staging");
        baseline_allocations=allocations-a;
        unsigned f=frees,p=puts,al=allocations;{
            ker::syscall::vmem::SharedVmemPublicationGuard publication;check(u.matches(s.w.source),"complete final cohort match");u.commit();}
        extra(allocations==al&&frees==f&&puts==p,"actual matches+commit allocate/free/put nothing");
        extra(s.w.source.lazy_vmem_ranges.size()==11&&s.sibling.lazy_vmem_ranges.size()==11&&
              st::lazy_vmem_ranges_equal(s.w.source.lazy_vmem_ranges.at(10),added)&&
              st::lazy_vmem_ranges_equal(s.sibling.lazy_vmem_ranges.at(10),added),"ADD commits every PROCESS sibling");
        extra(s.foreign.lazy_vmem_ranges.empty()&&s.daemon.lazy_vmem_ranges.empty(),"foreign root and DAEMON excluded");
    }extra(s.w.source.ref_count==1&&s.sibling.ref_count==1,"all cohort/snapshot/validation Task refs retired");}
    extra(baseline_allocations==7,"actual expected seven staging array allocations");
    for(unsigned fail=0;fail<baseline_allocations;++fail){SharedWorld s;int raw=s.w.raw.refcount;auto family=s.w.family->owner_counts().total;{
        Update u;fail_array_after=static_cast<int>(fail);int r=u.prepare(s.w.source);
        if(r==0){Row added=row(200,&s.w.raw);r=u.stage(Update::Change::ADD,added.start,added.end,&added);}fail_array_after=-1;
        extra(r==-ENOMEM,"every actual allocation prefix refuses OOM");
    }extra(s.unchanged()&&s.w.raw.refcount==raw&&s.w.family->owner_counts().total==family&&
            s.w.source.ref_count==1&&s.sibling.ref_count==1,"OOM prefix preserves rows and all owners");}
    for(bool remove:{false,true}){SharedWorld s;uint64_t start=0x11040,end=0x13080;{
        Update u;check(u.prepare(s.w.source)==0,"projection preparation");
        check(u.stage(remove?Update::Change::REMOVE:Update::Change::PROTECT,start,end,nullptr,0)==0,"projection staging");
        {ker::syscall::vmem::SharedVmemPublicationGuard p;check(u.matches(s.w.source),"projection match");u.commit();}
        projection(s,remove,start,end,0);
        extra(!failures,"actual REMOVE/NONE projection matches anchored bytes");}}
    {SharedWorld s;Update u;s.sibling.ref_count=UINT32_MAX;
        extra(u.prepare(s.w.source)==-EAGAIN,"initial census refuses silently unretained sibling");s.sibling.ref_count=1;
        extra(s.unchanged(),"saturated initial cohort has no row effects");}
    {SharedWorld s;Update u;check(u.prepare(s.w.source)==0,"saturated final preparation");
        check(u.stage(Update::Change::REMOVE,0x10000,0x11000)==0,"saturated final staging");s.foreign.pagemap=&s.w.root;s.foreign.ref_count=UINT32_MAX;
        {ker::syscall::vmem::SharedVmemPublicationGuard p;extra(!u.matches(s.w.source),"final count-only census rejects unretained newcomer");}
        s.foreign.ref_count=1;s.foreign.pagemap=&s.w.other;extra(s.unchanged(),"saturated validation has no row effects");}
    for(unsigned change=0;change<7;++change){SharedWorld s;Update u;check(u.prepare(s.w.source)==0,"changed cohort preparation");
        check(u.stage(Update::Change::REMOVE,0x10000,0x11000)==0,"changed cohort staging");
        auto oldrow=s.sibling.lazy_vmem_ranges.at(0);
        if(change==0)s.foreign.pagemap=&s.w.root;
        if(change==1)s.sibling.pagemap=&s.w.other;
        if(change==2){s.sibling.pagemap=&s.w.other;s.foreign.pagemap=&s.w.root;}
        if(change==3)++s.sibling.lazy_vmem_ranges.at(0).file_offset;
        if(change==4)++s.w.root.serial;
        if(change==5)s.sibling.usercopy_pagemap_closing=true;
        if(change==6)sched::active_registry.at(0)=&s.foreign;
        {ker::syscall::vmem::SharedVmemPublicationGuard p;extra(!u.matches(s.w.source),"exact changed cohort/row/root refuses commit");}
        s.foreign.pagemap=&s.w.other;s.sibling.pagemap=&s.w.root;s.sibling.lazy_vmem_ranges.at(0)=oldrow;
        if(change==4)--s.w.root.serial;s.sibling.usercopy_pagemap_closing=false;sched::active_registry.at(0)=&s.w.source;
        extra(s.unchanged(),"failed final validation leaves original rows");}
    {SharedWorld s;Update u;s.sibling.usercopy_pagemap_closing=true;
        extra(u.prepare(s.w.source)!=0,"closing existing sibling cannot disappear from source capture");s.sibling.usercopy_pagemap_closing=false;}
    {SharedWorld s;Update u;check(u.prepare(s.w.source)==0,"closing-after-match preparation");
        check(u.stage(Update::Change::REMOVE,0x10000,0x11000)==0,"closing-after-match staging");
        {ker::syscall::vmem::SharedVmemPublicationGuard p;check(u.matches(s.w.source),"closing race final match");
            s.sibling.usercopy_pagemap_closing=true;u.commit();}
        extra(s.sibling.lazy_vmem_ranges.size()==10&&s.w.source.lazy_vmem_ranges.size()==9,"closing after check is skipped without repopulation");
        s.sibling.usercopy_pagemap_closing=false;}
    {SharedWorld s;int raw=s.w.raw.refcount;auto total=s.w.family->owner_counts().total;{
        Update u;check(u.prepare(s.w.source)==0,"typed failure prefix prepare");Row bad=row(200);bad.kind=Kind::TMPFS_READ_SHARED;bad.lifetime=nullptr;
        extra(u.stage(Update::Change::ADD,bad.start,bad.end,&bad)==-EOVERFLOW,"invalid typed owner refuses after real retained prefix");
    }extra(s.unchanged()&&s.w.raw.refcount==raw&&s.w.family->owner_counts().total==total,"typed owner failure unwinds every real prefix");}
    {SharedWorld s;ker::vfs::File saturated;saturated.refcount=INT_MAX;int raw=s.w.raw.refcount;auto total=s.w.family->owner_counts().total;{
        Update u;check(u.prepare(s.w.source)==0,"RAW failure prefix prepare");Row bad=row(200,&saturated);
        extra(u.stage(Update::Change::ADD,bad.start,bad.end,&bad)==-EOVERFLOW,"actual RAW checked retain saturation refuses");
    }extra(s.unchanged()&&s.w.raw.refcount==raw&&s.w.family->owner_counts().total==total&&saturated.refcount==INT_MAX,"RAW saturation does not release unowned output");}
    {SharedWorld s;Update u;sched::reported_count=0;
        extra(u.prepare(s.w.source)==-EAGAIN&&s.unchanged(),"insufficient cohort census capacity has no owner/row effects");sched::reported_count=-1;}
    check(shared_cases>=40,"complete shared transaction vector floor");
}
}
'''


FORK_SEAMS = r'''
namespace fixture {
inline fm::FileMappingIndex actual_index;
inline Lock index_pin_lock;
inline unsigned simulated_tlb_acks{}, enabled_interrupts{}, restored_interrupts{};
inline void interrupt_instruction(const char* instruction){
    check(!irq&&!publication,"CPU interrupt change outside publication/IRQ");
    if(std::strstr(instruction,"sti")){irq_enabled=true;++enabled_interrupts;}
    else if(std::strstr(instruction,"cli")){irq_enabled=false;++restored_interrupts;}
    else std::abort();
}
void retire_index(ker::mod::mm::paging::PageTable* root){
    fm::FileMappingTlbRetirement batch;
    auto f=index_pin_lock.lock_irqsave();
    bool ok=actual_index.detach_root_locked(root,root->serial,batch);
    index_pin_lock.unlock_irqrestore(f);
    check(ok,"actual root index detach");++simulated_tlb_acks;
    batch.release_after_completed_tlb();
}
}
// Whole actual scope/header. Only its privileged CPU instructions are explicit
// Host primitives; admission, nested interval and destruction ordering are real.
#define asm
#define volatile(...) fixture::interrupt_instruction(#__VA_ARGS__)
#include <platform/sys/syscall_preparation.hpp>
#undef volatile
#undef asm
namespace ker::mod::mm::virt {
inline bool query_current_file_binding(sched::task::Task& task,uint64_t start,uint64_t end,
                                      fm::FileMappingBinding*& owned,bool& exact){
    fixture::check(fixture::publication&&!fixture::irq,"query primitive enters under publication");
    if(sched::get_current_task()!=&task||!task.pagemap||!task.pagemap->serial)return false;
    auto f=fixture::index_pin_lock.lock_irqsave();
    bool ok=fixture::actual_index.overlap_locked(task.pagemap,task.pagemap->serial,start,end,owned,exact);
    fixture::index_pin_lock.unlock_irqrestore(f);return ok;
}
inline bool attach_unpublished_file_binding(fm::FileMappingBinding*& prepared,paging::PageTable* root,
                                            uint64_t start,uint64_t end,uint64_t offset){
    fixture::check(fixture::publication&&!fixture::irq,"attach primitive enters under publication");
    if(fixture::fail_attach_after==0)return false;if(fixture::fail_attach_after>0)--fixture::fail_attach_after;
    auto f=fixture::index_pin_lock.lock_irqsave();
    bool ok=root&&root->serial&&prepared&&prepared->configure_unpublished(root,root->serial,start,end,offset)&&
            fixture::actual_index.attach_locked(prepared,root->serial);
    fixture::index_pin_lock.unlock_irqrestore(f);return ok;
}
}
'''

FORK_CASES = r'''
namespace fixture {
using Fork=ker::syscall::vmem::PublicFileForkPreparation;
void fork_test(bool value,const char* why){++fork_cases;test(value,why);}
struct ForkWorld {
    Root root{501},child_root{9001};st::Task parent{},child{};ker::vfs::File file{};
    fm::FileMappingLifetime* family{};
    std::array<fm::FileMappingBinding*,8> bindings{};size_t count{};
    explicit ForkWorld(size_t n=3):count(n){
        parent.pagemap=&root;child.pagemap=&child_root;ker::mod::sched::current_task=&parent;
        if(!n)return;
        demand_constructor_allowed=true;int error=-1;
        family=fm::FileMappingLifetime::prepare_demand_read_shared_tmpfs(file,7*4096,64*4096,error);
        demand_constructor_allowed=false;check(family&&error==0,"real demand-mode family via declared constructor primitive");
        check(parent.lazy_vmem_ranges.reserve(n),"parent public rows reserve");
        for(size_t i=0;i<n;++i){Row r{};r.start=0x10000+i*0x4000;r.end=r.start+0x2000;r.prot=i%2?0:1;r.flags=1;
            r.kind=Kind::TMPFS_READ_SHARED;r.lifetime=family;r.file_offset=(7+i*3)*4096;
            check(st::try_retain_lazy_vmem_range(r),"parent owns public LAZY");check(parent.lazy_vmem_ranges.push_back_no_grow(r),"parent public append");
            auto* prepared=fm::FileMappingBinding::prepare(*family,&root,root.serial,r.start,r.end,r.file_offset);bindings.at(i)=prepared;
            check(prepared,"real parent binding reservation");
            {ker::syscall::vmem::SharedVmemPublicationGuard p;auto f=index_pin_lock.lock_irqsave();
             check(actual_index.attach_locked(prepared,root.serial),"real parent index attach");index_pin_lock.unlock_irqrestore(f);}
            check(!prepared,"actual parent INDEX consumed prepared owner");
        }
    }
    size_t child_bindings(){size_t n=0;auto f=index_pin_lock.lock_irqsave();check(actual_index.count_root_locked(&child_root,child_root.serial,n),"child count exact serial");index_pin_lock.unlock_irqrestore(f);return n;}
    void clear_child(){retire_index(&child_root);st::release_lazy_vmem_ranges(child);drain();}
    ~ForkWorld(){clear_child();retire_index(&root);st::release_lazy_vmem_ranges(parent);
        if(family)family->release(fm::FamilyOwner::PREPARED);drain();
        check(file.refcount==1&&parent.ref_count==1&&child.ref_count==1,"fork all File/Task owners returned");
        check(actual_index.root_empty_locked(&root)&&actual_index.root_empty_locked(&child_root),"fork roots empty after ACK primitive");
        ker::mod::sched::current_task=nullptr;
    }
};
// A bounded setup seam only: first-member pointer interconversion on the actual
// standard-layout class. No layout/body rewrite; restore saved counters before
// any owner release. The real core/index checked retain executes at UINT64_MAX.
struct SaturateBinding {
    fm::BindingOwnerCounts* counts;fm::BindingOwnerCounts saved;
    explicit SaturateBinding(fm::FileMappingBinding* b){
        static_assert(std::is_standard_layout_v<fm::FileMappingBinding>);
        counts=reinterpret_cast<fm::BindingOwnerCounts*>(b);saved=*counts;
        counts->by_role[static_cast<size_t>(fm::BindingOwner::PIN)]+=UINT64_MAX-counts->total;
        counts->total=UINT64_MAX;
    }
    ~SaturateBinding(){*counts=saved;}
};
void fork_vectors(){
    {ForkWorld w;{
        Clone source;Fork f;check(source.prepare(w.parent)==0,"public clone captures READ/NONE cold rows");
        fork_test(f.prepare(w.parent,source)==0,"actual fork prepares all cold public intervals");
        unsigned al=allocations,sc=scalar_allocations;
        {ker::syscall::vmem::SharedVmemPublicationGuard p;check(source.matches(w.parent)&&f.matches(w.parent),"public parent exact index revalidation");
            check(source.consume(w.child),"public child counted row transfer");check(f.attach(w.child),"public child actual index attach");}
        fork_test(allocations==al&&scalar_allocations==sc,"binding match/attach use no allocator");
        fork_test(w.child_bindings()==3&&w.child.lazy_vmem_ranges.size()==3,"fresh child has complete own index and lazy set");
        for(size_t i=0;i<3;++i){auto const& r=w.child.lazy_vmem_ranges.at(i);fm::FileMappingBinding* b=nullptr;
            auto flags=index_pin_lock.lock_irqsave();check(actual_index.retain_for_pin_locked(&w.child_root,w.child_root.serial,r.start,b)==fm::FileMappingPinResult::ACQUIRED,"child actual binding PIN");index_pin_lock.unlock_irqrestore(flags);
            fork_test(b&&b!=w.bindings.at(i)&&b->root()==&w.child_root&&b->root_incarnation()==9001&&b->lifetime()==w.family&&
                      b->start()==r.start&&b->end()==r.end&&b->file_offset()==(7+i*3)*4096&&r.prot==(i%2?0:1),"independent child identity/NONE/offset projection");
            if(b)b->release(fm::BindingOwner::PIN);
        }
    }w.clear_child();fork_test(w.child_bindings()==0,"actual child index retires after explicit ACK primitive");}
    {ForkWorld w(0);Clone c;Fork f;check(c.prepare(w.parent)==0,"empty clone");unsigned al=allocations,sc=scalar_allocations;
        fork_test(f.prepare(w.parent,c)==0,"zero-public-row preparation");
        {ker::syscall::vmem::SharedVmemPublicationGuard p;check(f.matches(w.parent)&&f.attach(w.child),"zero binding commit");}
        fork_test(allocations==al&&scalar_allocations==sc&&w.child_bindings()==0,"zero-public path allocates no binding storage");}
    {ForkWorld w;Clone c;check(c.prepare(w.parent)==0,"array failure source");auto count=w.family->owner_counts().total;
        {Fork f;fail_array_after=0;fork_test(f.prepare(w.parent,c)==-ENOMEM,"fork entry array OOM before source ownership");fail_array_after=-1;}
        fork_test(w.family->owner_counts().total==count&&w.child_bindings()==0,"array OOM restores family/index baseline");}
    for(int fail=0;fail<3;++fail){ForkWorld w;Clone c;check(c.prepare(w.parent)==0,"child scalar OOM source");auto count=w.family->owner_counts().total;
        {Fork f;fail_scalar_after=fail;fork_test(f.prepare(w.parent,c)==-ENOMEM,"every child binding allocation prefix OOM");fail_scalar_after=-1;}
        drain();fork_test(w.family->owner_counts().total==count&&w.child_bindings()==0,"scalar OOM queued prefix releases exactly");}
    for(int fail=0;fail<3;++fail){ForkWorld w;Clone c;check(c.prepare(w.parent)==0,"ticket OOM source");auto count=w.family->owner_counts().total;
        {Fork f;fail_reserve_after=fail;fork_test(f.prepare(w.parent,c)==-ENOMEM,"every reserved ticket prefix failure");fail_reserve_after=-1;}
        drain();fork_test(w.family->owner_counts().total==count,"ticket failure child/family owner rollback");}
    {ForkWorld w;Clone c;check(c.prepare(w.parent)==0,"missing source");retire_index(&w.root);Fork f;
        fork_test(f.prepare(w.parent,c)==-EAGAIN&&w.child_bindings()==0,"missing parent index refuses cold typed rows");}
    {ForkWorld w;Clone c;check(c.prepare(w.parent)==0,"saturated source");Fork f;
        {SaturateBinding saturated(w.bindings.at(1));fork_test(f.prepare(w.parent,c)==-EAGAIN,"actual SNAPSHOT count saturation refuses after retained prefix");}
        fork_test(w.child_bindings()==0,"saturation has no child index effect");}
    {ForkWorld w;Clone c;Fork f;check(c.prepare(w.parent)==0&&f.prepare(w.parent,c)==0,"saturated final index preparation");
        {SaturateBinding saturated(w.bindings.at(1));ker::syscall::vmem::SharedVmemPublicationGuard p;
            fork_test(!f.matches(w.parent),"actual final query saturation refuses retained validation prefix");}}
    {ForkWorld w;Clone c;Fork f;check(c.prepare(w.parent)==0&&f.prepare(w.parent,c)==0,"index ABA preparation");
        retire_index(&w.root);
        for(size_t i=0;i<3;++i){auto const&r=w.parent.lazy_vmem_ranges.at(i);auto*b=fm::FileMappingBinding::prepare(*w.family,&w.root,w.root.serial,r.start,r.end,r.file_offset);
            {ker::syscall::vmem::SharedVmemPublicationGuard p;auto flags=index_pin_lock.lock_irqsave();check(actual_index.attach_locked(b,w.root.serial),"replacement index attach");index_pin_lock.unlock_irqrestore(flags);}}
        {ker::syscall::vmem::SharedVmemPublicationGuard p;fork_test(!f.matches(w.parent),"same root/VA/family replacement exact binding ABA refused");}}
    for(int prefix=0;prefix<3;++prefix){ForkWorld w;Clone c;Fork f;check(c.prepare(w.parent)==0&&f.prepare(w.parent,c)==0,"attach prefix preparation");
        {ker::syscall::vmem::SharedVmemPublicationGuard p;check(c.matches(w.parent)&&f.matches(w.parent),"prefix exact parent validation");
            check(c.consume(w.child),"prefix child rows");fail_attach_after=prefix;fork_test(!f.attach(w.child),"exact attach prefix refuses");fail_attach_after=-1;}
        fork_test(w.child_bindings()==static_cast<size_t>(prefix),"only genuinely attached prefix belongs to child index");
        w.clear_child();fork_test(w.child_bindings()==0,"normal index/ACK retirement handles failed attachment prefix");}
    // Whole actual syscall scope, explicit CPU instructions only.
    {World w;ker::mod::sched::current_task=&w.source;irq_enabled=false;unsigned enabled=enabled_interrupts,restored=restored_interrupts;bool cleanup=false;
        {ker::mod::sys::SyscallPreparationScope p(w.source);check(p.ready()&&irq_enabled,"actual syscall scope enables IF");
            struct Deferred{bool&done;~Deferred(){check(irq_enabled&&!publication,"deferred owner cleanup sees IF-on after publication");done=true;}}owner{cleanup};
            ker::mod::sys::SyscallPreparationScope::LegacyInterval legacy(p);check(!irq_enabled,"legacy interval restores CLI");
            {ker::syscall::vmem::SharedVmemPublicationGuard guard;check(!irq_enabled,"legacy publication retains original IF-off");}
        }
        fork_test(cleanup&&!irq_enabled&&enabled_interrupts==enabled+2&&restored_interrupts==restored+2,"scope/pub/legacy/owner unwind order restores incoming IF");
        irq_enabled=true;ker::mod::sched::current_task=nullptr;}
    for(unsigned bad=0;bad<6;++bad){World w;ker::mod::sched::current_task=&w.source;irq_enabled=false;
        if(bad==0)w.source.syscall_account_start_us=0;if(bad==1)w.source.scheduler_transition_active=true;
        if(bad==2)ker::mod::sched::current_task=&w.destination;if(bad==3)preempt=1;
        if(bad==4)w.source.type=st::TaskType::DAEMON;if(bad==5)w.source.state=st::TaskState::DEAD;
        {ker::mod::sys::SyscallPreparationScope p(w.source);fork_test(!p.ready()&&!irq_enabled,"invalid ordinary scope denies without changing IF");}
        irq_enabled=true;preempt=0;ker::mod::sched::current_task=nullptr;}
    {World w;ker::mod::sched::current_task=&w.source;unsigned enabled=enabled_interrupts,restored=restored_interrupts;
        {ker::mod::sys::SyscallPreparationScope p(w.source);check(p.ready()&&irq_enabled,"already enabled preparation accepted");}
        fork_test(irq_enabled&&enabled==enabled_interrupts&&restored==restored_interrupts,"already IF-on scope preserves caller state");ker::mod::sched::current_task=nullptr;}
    check(fork_cases==45,"complete public preparation and IF scope vector census");
}
}
'''

def pin(path):
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def capture(source, anchor, declaration=False):
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', lambda m: " " * len(m[0]), source)
    positions = [m.start() for m in re.finditer(re.escape(anchor), clean)]
    if len(positions) != 1:
        raise ValueError(f"expected complete unique actual definition: {anchor}")
    start = clean.rfind("\n", 0, positions[0]) + 1
    opening = clean.index("{", positions[0])
    depth = 1
    end = opening + 1
    while depth:
        if clean[end] == "{": depth += 1
        elif clean[end] == "}": depth -= 1
        end += 1
    if declaration:
        if clean[end] != ";": raise ValueError("complete declaration terminator")
        end += 1
    return source[start:end]


def generate(scratch):
    include = scratch / "include"
    def write(name, data):
        path = include / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data)
    write("fixture_common.hpp", COMMON)
    for name in ["util/smallvec.hpp", "platform/sched/lazy_vmem.hpp", "platform/mm/file_mapping_lifetime.hpp", "platform/sys/syscall_preparation.hpp"]:
        write(name, (KERN / name).read_text())
    task = (KERN / "platform/sched/task.hpp").read_text()
    begin = task.index("enum class LazyVmemKind")
    end = task.index("using LazyVmemRangeVec", begin)
    end = task.index(";", end) + 1
    task_types = task[begin:end]
    write("platform/sched/task.hpp", '#pragma once\n#include <fixture_common.hpp>\n#include <util/smallvec.hpp>\n#include <platform/mm/file_mapping_lifetime.hpp>\nnamespace ker::mod::sched::task {\n' + task_types + r'''
enum class TaskState { ACTIVE, DEAD };
enum class TaskType { PROCESS, DAEMON };
struct Task {
    std::atomic<uint32_t> ref_count{1}; std::atomic<TaskState> state{TaskState::ACTIVE};
    TaskType type{TaskType::PROCESS};
    uint64_t syscall_account_start_us{1};std::atomic<bool> scheduler_transition_active{};
    std::atomic<bool> usercopy_pagemap_closing{};std::atomic<void*> native_file_mapping_retirement{};
    mm::paging::PageTable* pagemap{};fixture::Lock lazy_vmem_lock;LazyVmemRangeVec lazy_vmem_ranges;
    bool try_acquire_lifetime_ref(){auto r=ref_count.load();while(r&&r<UINT32_MAX){if(ref_count.compare_exchange_weak(r,r+1))return true;}return false;}
    void release(){fixture::check(!fixture::irq&&!fixture::publication,"Task release outside guards");fixture::check(ref_count.fetch_sub(1)>1,"fixture Task retained owner");}
};
bool clone_lazy_vmem_ranges(Task&,Task&);
void release_lazy_vmem_ranges(Task&);
}
''')
    for name in ["platform/mm/virt.hpp", "platform/sched/scheduler.hpp", "syscalls_impl/vmem/sys_vmem.hpp", "platform/sys/spinlock.hpp", "platform/mm/paging.hpp", "vfs/file.hpp", "vfs/vfs.hpp"]:
        write(name, "#pragma once\n#include <fixture_common.hpp>\n")
    write("platform/mm/dyn/kmalloc.hpp", "#pragma once\n")
    write("util/hcf.hpp", "#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf(){std::abort();}\n")
    write("platform/sys/usercopy.hpp", "#pragma once\n#include <cstdint>\nnamespace ker::mod::sys::usercopy {constexpr uint64_t USER_ADDR_LIMIT=0x800000000000ULL;}\n")
    write("platform/mm/lifetime_retirement_executor.hpp", "#pragma once\n#include <platform/mm/file_mapping_lifetime.hpp>\nnamespace ker::mod::mm::file_mapping { struct LifetimeRetirementTicket { LifetimeRetirementRun run{}; void* argument{}; bool ready{}; }; class LifetimeRetirementExecutor { public: static auto port() noexcept -> const LifetimeRetirementPort*; }; }\n")
    write("vfs/fs/tmpfs.hpp", "#pragma once\n#include <fixture_common.hpp>\nnamespace ker::vfs::tmpfs {class TmpfsReadSharedBacking { public: static int prepare(File&,uint64_t,uint64_t,std::unique_ptr<TmpfsReadSharedBacking>&){return -EOPNOTSUPP;} }; class TmpfsDemandReadSharedBacking { public: static int prepare(File&,uint64_t,uint64_t,std::unique_ptr<TmpfsDemandReadSharedBacking>&out){if(!fixture::demand_constructor_allowed)return -EOPNOTSUPP;out.reset(new(std::nothrow) TmpfsDemandReadSharedBacking);return out?0:-ENOMEM;} }; }\n")
    # Whole current production translation units, never rewritten staging logic.
    (scratch / "lazy_vmem.cpp").write_bytes((KERN / "platform/sched/lazy_vmem.cpp").read_bytes())
    (scratch / "family.cpp").write_bytes((KERN / "platform/mm/file_mapping_lifetime.cpp").read_bytes())
    task_source = (KERN / "platform/sched/task.cpp").read_text()
    exec_source = (KERN / "syscalls_impl/process/exec.cpp").read_text()
    checked_retain = capture((KERN / "vfs/core.cpp").read_text(), "auto vfs_try_retain_file(")
    bodies = "\n".join(capture(task_source, anchor) for anchor in ["auto clone_lazy_vmem_ranges(", "void release_lazy_vmem_ranges("])
    vmem_source = (KERN / "syscalls_impl/vmem/sys_vmem.cpp").read_text()
    vmem_header = (KERN / "syscalls_impl/vmem/sys_vmem.hpp").read_text()
    write("syscalls_impl/vmem/sys_vmem.hpp", "#pragma once\n#include <platform/sched/lazy_vmem.hpp>\nnamespace ker::syscall::vmem {\n" +
          capture(vmem_header, "class PublicFileForkPreparation ", declaration=True) + "\n}\n")
    scheduler_source = (KERN / "platform/sched/scheduler.cpp").read_text()
    actual_snapshot = capture(scheduler_source, "auto snapshot_active_task_lifetime_refs_if(")
    actual_update = capture(vmem_source, "class SharedLazyVmemUpdate ", declaration=True)
    helpers = "\n".join(capture(vmem_source, anchor) for anchor in [
        "auto ranges_overlap(", "auto slice_lazy_vmem_range(", "void release_lazy_vmem_range_file("])
    harness = '#include <fixture_common.hpp>\n#include <platform/sched/lazy_vmem.hpp>\n#include <platform/mm/lifetime_retirement_executor.hpp>\n#include <syscalls_impl/vmem/sys_vmem.hpp>\n' + SEAMS + SHARED_SEAMS + FORK_SEAMS
    harness += "\nnamespace ker::vfs {\n" + checked_retain + "\n}\nnamespace ker::mod::sched::task {\n" + bodies + "\n}\nnamespace actual_exec {using ker::mod::sched::task::LazyVmemRangeVec;\n" + capture(exec_source, "void release_lazy_file_refs(") + "\n}\n"
    harness += "\nnamespace ker::mod::sched {\n" + actual_snapshot + "\n}\n"
    harness += r'''
namespace ker::syscall::vmem {
using ker::mod::sched::get_current_task;
using ker::mod::sched::task::LazyVmemRange;
using ker::mod::sched::task::LazyVmemRangeVec;
using ker::mod::sched::task::LazyVmemKind;
void advance_mmap_cursor(ker::mod::sched::task::Task*,uint64_t,uint64_t) {
    fixture::check(fixture::publication&&!fixture::irq,"cursor primitive under publication outside IRQ");
}
''' + helpers + "\n" + actual_update + "\n" + capture(vmem_source, "struct PublicFileForkPreparation::Entry ", declaration=True) + "\n"
    harness += "\n".join(capture(vmem_source, name) for name in [
        "PublicFileForkPreparation::~PublicFileForkPreparation(", "auto PublicFileForkPreparation::prepare(",
        "auto PublicFileForkPreparation::matches(", "auto PublicFileForkPreparation::attach("])
    harness += "\n}\nnamespace fixture {void shared_vectors();void fork_vectors();}\n"
    harness += CASES.replace("fixture::vectors();", 'fixture::vectors();fixture::check(fixture::cases==53,"all original53 vectors executed");fixture::shared_vectors();fixture::fork_vectors();').replace(
        "real_family_core=1\\n", "real_family_core=1 real_shared_transaction=1 shared_cases=%u real_public_fork_preparation=1 fork_cases=%u\\n").replace(
        "fixture::allocations,fixture::callbacks);", "fixture::allocations,fixture::callbacks,fixture::shared_cases,fixture::fork_cases);") + SHARED_CASES + FORK_CASES
    (scratch / "harness.cpp").write_text(harness)


def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())


def subreaper() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("bounded owned-session runner requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")


def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])


def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Sequential driver children only; the subreaper adopts escaped owned
        # compiler descendants. An original unreaped leader reserves the SID.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result


def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)


def reap_group(process: subprocess.Popen, deadline: float) -> None:
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            if owned_session(process.pid):
                raise ValueError("owned session/adopted descendants remain after leader reap")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session not proven absent; preserve scratch")
        time.sleep(0.01)


def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("60-second whole budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("child log/private scratch exceeded bounded budget")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("60-second whole budget exhausted")
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {sig: signal.signal(sig, signal.SIG_IGN)
                            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 6),
            "leader_reaped": True, "owned_session_absent": True, "log": pin(log)}


def dependency_pins(depfile: Path) -> list[dict]:
    if depfile.stat().st_size > LOG_BUDGET:
        raise ValueError("compiler dependency file exceeded 1MiB")
    body = depfile.read_text().replace("\\\n", " ")
    if ": " not in body:
        raise ValueError("actual compiler depfile has no target separator")
    words = re.findall(r"(?:\\.|[^\s])+", body.split(": ", 1)[1])
    paths = {Path(re.sub(r"\\(.)", r"\1", word)).resolve(strict=True) for word in words}
    if len(paths) > 2048:
        raise ValueError("actual compiler dependency count exceeded 2048")
    return [pin(p) for p in sorted(paths) if p.is_file()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cxx", default=os.environ.get("CXX", "/usr/bin/clang++"))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 60
    scratch = None
    identity = None
    groups = []
    watched = []
    before = []
    staged = []
    staged_before = []
    evidence = None
    result = {"pass": False, "errors": [], "commands": [], "scratch_removed": False,
              "whole_budget_seconds": 60, "scratch_budget_bytes": SCRATCH_BUDGET,
              "core_dump_limit_bytes": 0, "native_runtime_qualified": False,
              "shared_coherence_qualified": False, "full_goal_complete": False,
              "compiler_external_dependencies_before": False,
              "compiler_external_dependencies_after_only": True}
    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    def interrupted(number, _frame):
        raise KeyboardInterrupt(f"signal {number}")
    try:
        for s in handlers: signal.signal(s, interrupted)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        compiler = Path(args.cxx).resolve(strict=True)
        with compiler.open("rb") as f:
            if f.read(4) != b"\x7fELF" or not os.access(compiler, os.X_OK):
                raise ValueError("--cxx requires one real executable Host ELF compiler")
        if args.evidence_dir:
            evidence = args.evidence_dir.absolute()
            if evidence.parent.resolve(strict=True) != evidence.parent or os.path.lexists(evidence):
                raise ValueError("fresh exclusive canonical evidence directory required")
            evidence.mkdir(mode=0o700)
        watched = [Path(__file__).resolve(), compiler, *(KERN / name for name in [
            "util/smallvec.hpp", "platform/sched/lazy_vmem.hpp", "platform/sched/lazy_vmem.cpp",
            "platform/sched/task.hpp", "platform/sched/task.cpp", "syscalls_impl/process/exec.cpp",
            "platform/mm/file_mapping_lifetime.hpp", "platform/mm/file_mapping_lifetime.cpp", "vfs/core.cpp",
            "syscalls_impl/vmem/sys_vmem.cpp", "syscalls_impl/vmem/sys_vmem.hpp", "platform/sched/scheduler.cpp",
            "platform/sys/syscall_preparation.hpp"])]
        before = [pin(p) for p in watched]
        result["source_before"] = before
        result["primitive_seams"] = ["Task lifetime/state", "MM scalar root identity", "publication and IRQ locks",
                                     "TMPFS anchored preparation refuses; demand constructor allowed only new owner vectors",
                                     "fixed queued callback port dependency drain", "nothrow array/scalar/ticket failure",
                                     "active registry/current Task", "mmap cursor primitive (not qualified)",
                                     "root-query/attach pin lock with real FileMappingIndex", "explicit TLB ACK marker (not hardware)",
                                     "bounded restored actual BindingOwnerCounts saturation setup", "privileged CPU instructions only"]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-lazy-staging-", dir="/tmp"))
        info = scratch.stat()
        identity = (info.st_dev, info.st_ino, info.st_uid)
        if info.st_uid != os.getuid() or scratch.is_symlink(): raise ValueError("private owned scratch")
        generate(scratch)
        staged = sorted(p for p in scratch.rglob("*") if p.is_file())
        staged_before = [pin(p) for p in staged]
        result["staged_sources_before"] = staged_before
        depfile = scratch / "dependencies.d"
        binary = scratch / "staging-test"
        argv = [str(compiler), "-std=c++23", "-O1", "-g0", "-fsanitize=address,undefined",
                "-fno-omit-frame-pointer", "-MD", "-MF", str(depfile), "-I", str(scratch / "include"),
                str(scratch / "harness.cpp"), str(scratch / "lazy_vmem.cpp"), str(scratch / "family.cpp"),
                "-o", str(binary)]
        for index, command_argv in enumerate([argv, [str(binary)]]):
            log = scratch / f"command-{index}.log"
            row = command(command_argv, log, scratch, deadline, groups)
            result["commands"].append(row)
            text = log.read_text(errors="replace")
            if index == 0 and depfile.exists(): result["emitted_dependency_pins_after"] = dependency_pins(depfile)
            if row["exit"] != 0:
                print(text[-32768:], end="", flush=True)
                raise ValueError(f"actual {'compile/link' if index == 0 else 'runtime'} exit {row['exit']}")
            if index == 1:
                match = re.fullmatch(r"PASS cases=(\d+) assertions=(\d+) failures=0 allocations=(\d+) callbacks=(\d+) real_smallvec=1 real_staging=1 real_family_core=1 real_shared_transaction=1 shared_cases=(\d+) real_public_fork_preparation=1 fork_cases=(\d+)\n", text)
                if match is None or int(match[1]) != 144 or int(match[5]) != 46 or int(match[6]) != 45 or int(match[2]) < 1000 or int(match[4]) < 4:
                    raise ValueError("complete actual positive vector census missing")
                result["runtime_census"] = dict(zip(["cases", "assertions", "allocations", "callbacks", "shared_cases", "fork_cases"], map(int, match.groups())))
                print(text, end="", flush=True)
        result["pass"] = True
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for s in handlers: signal.signal(s, signal.SIG_IGN)
        try:
            if before:
                result["source_after"] = [pin(p) for p in watched]
                result["source_held"] = result["source_after"] == before
                if not result["source_held"]: result["errors"].append("actual source/compiler changed")
            if staged_before:
                result["staged_sources_after"] = [pin(p) for p in staged]
                result["staged_held"] = result["staged_sources_after"] == staged_before
                if not result["staged_held"]: result["errors"].append("compiled staged bytes changed")
            if scratch is not None:
                result["owned_sessions_pending"] = list(groups)
                result["scratch_allocated_bytes_before_cleanup"] = allocated(scratch)
                if evidence:
                    result["saved_evidence"] = []
                    for p in [*staged, *scratch.glob("command-*.log"), scratch / "dependencies.d"]:
                        if not p.is_file(): continue
                        if p.stat().st_size > LOG_BUDGET: raise ValueError("bounded saved source/log")
                        dest = evidence / p.relative_to(scratch)
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        with dest.open("xb") as f: f.write(p.read_bytes())
                        result["saved_evidence"].append(pin(dest))
                info = scratch.stat()
                if groups or scratch.is_symlink() or (info.st_dev, info.st_ino, info.st_uid) != identity:
                    result["errors"].append("owned cleanup unproven; preserve scratch")
                else:
                    shutil.rmtree(scratch)
                    result["scratch_removed"] = not os.path.lexists(scratch)
            result["elapsed_seconds"] = time.monotonic() - started
            if result["elapsed_seconds"] >= 60: result["errors"].append("whole60s cleanup bound expired")
        except (OSError, ValueError, TimeoutError) as error:
            result["errors"].append(f"cleanup: {type(error).__name__}: {error}")
        finally:
            for s, handler in handlers.items(): signal.signal(s, handler)
    result["pass"] = result["pass"] and not result["errors"] and result["scratch_removed"]
    result["qualification"] = "PASS_ACTUAL_HOST_LAZY_ROW_STAGING_AND_RESERVE_ONLY" if result["pass"] else "FAIL"
    if evidence:
        with (evidence / "result.json").open("x") as f: json.dump(result, f, indent=2); f.write("\n")
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["pass"] and time.monotonic() < deadline else 1


if __name__ == "__main__":
    raise SystemExit(main())
