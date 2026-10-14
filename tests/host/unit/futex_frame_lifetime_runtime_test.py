#!/usr/bin/env python3
"""Compile real queued-futex/frame-owner bodies against explicit Host seams.

The selected complete futex.cpp and actual IntrHashTable/header bytes are staged
unchanged. Current complete frame-owner/stable-pin methods and checked physical
ref-inc body are extracted without body substitutions. Task/PTE/root gates,
physical lookup/release and IRQ/access operations are controlled Host seams.
This qualifies reference transfer and allocator-reuse exclusion in those seams,
not native allocator/MM/scheduler/IRQ behavior. A complete --futex-source
predecessor must genuinely compile then fail the same independent reuse oracle.
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

ROOT = next(path for path in Path(__file__).resolve().parents
            if (path / "modules/kern/src").is_dir() and (path / ".github/instructions").is_dir())
KERN = ROOT / "modules/kern/src"
SOURCE = KERN / "syscalls_impl/futex/futex.cpp"
VECTORS = ["pending", "table_first", "slot_first", "predicate", "mapping_before",
           "mapping_after", "saturation", "waiter_oom", "table_oom", "invalid_table",
           "missing_task", "stale_limit", "replacement", "mismatch_keeps_old",
           "wake_virtual", "races", "moves", "timeout_order", "zero_timeout",
           "timeout_deadline", "shared_frame", "post_publish_wake"]
SCRATCH_BUDGET = 64 * 1024 * 1024
LOG_BUDGET = 1024 * 1024
FILE_BUDGET = 8 * 1024 * 1024

COMMON = r'''
#pragma once
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <latch>
#include <mutex>
#include <new>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
namespace fixture {
inline std::atomic<unsigned> assertions{}, failures{}, last_drops{}, irq_last_drops{};
inline thread_local unsigned irq_depth{}, access_depth{};
inline std::atomic<bool> fail_object{}, fail_array{};
inline void (*allocation_hook)(){};
inline void (*mapping_hook)(){};
inline std::atomic<uint64_t> now_us{1000000};
inline void check(bool value, const char* message) {
    ++assertions;
    if (!value && failures.fetch_add(1) < 16) std::fprintf(stderr, "FAIL: %s\n", message);
}
struct IrqLock {
    std::mutex mutex;
    uint64_t lock_irqsave() { mutex.lock(); uint64_t old=irq_depth; ++irq_depth; return old; }
    void unlock_irqrestore(uint64_t old) { check(irq_depth==old+1, "balanced IRQ seam"); irq_depth=old; mutex.unlock(); }
};
struct Frame {
    alignas(4096) std::array<unsigned char,4096> bytes{};
    std::atomic<bool> available{true};
    std::atomic<uint64_t> generation{};
};
inline std::array<Frame,8> frames;
inline size_t index_of(const void* page) {
    for(size_t i=0;i<frames.size();++i) if(page==frames[i].bytes.data()) return i;
    return frames.size();
}
inline uint64_t physical(size_t index) { return 0x100000ULL+index*4096; }
struct PageTable {
    std::atomic<Frame*> mapping{};
    std::atomic<unsigned> readers{}, checks{};
    std::atomic<unsigned> reject_check{};
    std::atomic<bool> pin_allowed{true};
};
}
namespace ker::mod::sys { using Spinlock=fixture::IrqLock; }
namespace ker::mod::mm {
using vaddr_t=uint64_t; using paddr_t=uint64_t;
namespace paging { constexpr uint64_t PAGE_SIZE=4096; using PageTable=fixture::PageTable; }
struct PageAllocator { std::array<std::atomic<uint32_t>,8> page_refcounts{}; };
}
'''

PHYS_TAIL = r'''
namespace fixture {
inline ker::mod::mm::PageAllocator allocator;
struct RefStats { std::atomic<unsigned> inc_ops{},inc_cas_retries{}; };
inline RefStats ref_stats;
inline Frame* allocate_exact(size_t i) {
    bool expected=true;
    if(!frames.at(i).available.compare_exchange_strong(expected,false)) return nullptr;
    check(allocator.page_refcounts.at(i).load()==0, "free frame has no outstanding references");
    allocator.page_refcounts.at(i).store(1);
    ++frames.at(i).generation;
    frames.at(i).bytes.fill(0);
    return &frames.at(i);
}
inline Frame* allocate() { for(size_t i=0;i<frames.size();++i) if(auto* p=allocate_exact(i)) return p; return nullptr; }
inline size_t frame_index(Frame* f) { return index_of(f->bytes.data()); }
inline uint32_t refs(Frame* f) { return allocator.page_refcounts.at(frame_index(f)).load(); }
inline bool unavailable(Frame* f) { return !f->available.load(); }
}
namespace ker::mod::mm::phys {
inline auto page_ref_stats_for_current_cpu() -> fixture::RefStats& { return fixture::ref_stats; }
inline auto find_allocator_for_page_cached(void* page, PageLookupHint* hint,
                                           PageAllocator*& out, uint32_t& index) -> bool {
    size_t i=fixture::index_of(page);
    if(i>=fixture::frames.size()) return false;
    // The Host arena represents only REGULAR_ZONE. A hint is a lookup cache,
    // not permission to access another allocator/domain.
    if(hint!=nullptr && (hint->allocator!=&fixture::allocator || hint->owner!=PageLookupOwner::REGULAR_ZONE)) {
        hint->allocator=&fixture::allocator; hint->owner=PageLookupOwner::REGULAR_ZONE;
    }
    out=&fixture::allocator; index=static_cast<uint32_t>(i); return true;
}
inline auto page_ref_dec(void* page, PageLookupHint* hint=nullptr) -> uint32_t {
    PageAllocator* a=nullptr; uint32_t i=0;
    if(!find_allocator_for_page_cached(page,hint,a,i)) { fixture::check(false,"release owned allocator frame"); return 0; }
    auto& count=a->page_refcounts[i]; uint32_t old=count.load();
    for(;;) {
        if(old==0) { fixture::check(false,"no duplicate frame drop"); return 0; }
        if(count.compare_exchange_weak(old,old-1)) break;
    }
    if(old==1) {
        ++fixture::last_drops;
        if(fixture::irq_depth!=0) ++fixture::irq_last_drops;
        // Availability/generation are the independent allocator-reuse oracle;
        // a physical-key value alone cannot keep a frame unavailable.
        fixture::frames.at(i).available.store(true);
    }
    return old-1;
}
auto page_ref_try_inc(void*,PageLookupHint*) -> bool;
inline auto page_ref_try_inc(void* p) -> bool { return page_ref_try_inc(p,nullptr); }
}
'''

TASK = r'''
#pragma once
#include "fixture-common.hpp"
#include <platform/mm/virt.hpp>
namespace ker::mod::sched::task {
enum class WaitChannelKind { FUTEX };
struct Task {
    uint64_t pid{},cpu{};
    std::atomic<void*> futex_waiter{};
    std::atomic<unsigned> task_readers{}, refs{1}, wakes{};
    mm::paging::PageTable* pagemap{};
    std::atomic<uint64_t> wake_at_us{};
    std::atomic<bool> deferred_task_switch{}, channel{}, root_allowed{true};
    uint64_t timeout_address=0x9000; timespec timeout{}; bool timeout_fault{};
    uint64_t load_cpu() const { return cpu; }
    void set_wait_channel(const char*,WaitChannelKind) { channel=true; }
    void clear_wait_channel() { channel=false; }
    bool try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) {
        if(!root_allowed || pagemap==nullptr) return false;
        ++task_readers; out=pagemap; return true;
    }
    void release_usercopy_pagemap() { fixture::check(task_readers.fetch_sub(1)>0,"Task root lease released once"); }
    void release() { fixture::check(refs.fetch_sub(1)>1,"temporary lookup Task reference released once"); }
};
}
namespace fixture {
inline thread_local ker::mod::sched::task::Task* current{};
inline std::array<std::atomic<ker::mod::sched::task::Task*>,32> tasks{};
}
'''

SCHEDULER = r'''
#pragma once
#include <platform/sched/task.hpp>
namespace ker::mod::sched {
enum class EventWakeDeferredSwitch { CANCEL };
inline auto get_current_task() -> task::Task* { return fixture::current; }
inline auto find_task_by_pid_safe(uint64_t pid) -> task::Task* {
    for(auto& slot:fixture::tasks) if(auto* t=slot.load();t!=nullptr && t->pid==pid) { ++t->refs; return t; }
    return nullptr;
}
inline void wake_task_from_event_on_cpu(task::Task* t,uint64_t,EventWakeDeferredSwitch) {
    ++t->wakes; t->deferred_task_switch=false; t->wake_at_us=0; t->clear_wait_channel();
}
}
'''

PIN_SEAMS = r'''
namespace ker::mod::mm::virt {
inline auto pin_user_page_for_task(sched::task::Task*,PageTable* root,vaddr_t address,
                                  bool writable,bool,UserPagePin& out) -> bool {
    auto* frame=root->mapping.load();
    if(!root->pin_allowed || frame==nullptr || address<0x4000 || address>=0x5000) return false;
    phys::PageLookupHint hint{};
    if(!phys::page_ref_try_inc(frame->bytes.data(),&hint)) return false;
    ++root->readers;
    out={.pagemap=root,.user_page=0x4000,.physical_page=fixture::physical(fixture::frame_index(frame)),
         .hhdm_page=frame->bytes.data(),.lookup_hint=hint,.access_gate_index=1,
         .require_writable=writable,.dirty=false};
    return true;
}
inline auto user_page_pin_still_mapped(const UserPagePin& pin) -> bool {
    unsigned check=++pin.pagemap->checks;
    if(fixture::mapping_hook!=nullptr && check==2) { auto hook=fixture::mapping_hook;fixture::mapping_hook=nullptr;hook(); }
    return (pin.pagemap->reject_check==0 || pin.pagemap->reject_check!=check) &&
           pin.pagemap->mapping.load()!=nullptr && pin.pagemap->mapping.load()->bytes.data()==pin.hhdm_page;
}
inline auto user_page_pin_commit_write(const UserPagePin& pin) -> bool { return user_page_pin_still_mapped(pin); }
inline void unpin_user_page(UserPagePin& pin) {
    auto old=pin;pin={};
    if(old.hhdm_page!=nullptr) phys::page_ref_dec(old.hhdm_page,&old.lookup_hint);
    if(old.pagemap!=nullptr) fixture::check(old.pagemap->readers.fetch_sub(1)>0,"pagemap pin lease released once");
}
}
namespace ker::mod::sys::usercopy {
UserAccessGuard::UserAccessGuard() { m_flags=fixture::access_depth; ++fixture::access_depth; }
UserAccessGuard::~UserAccessGuard() { fixture::check(fixture::access_depth==m_flags+1,"access window balanced");fixture::access_depth=static_cast<unsigned>(m_flags); }
auto access_window_active() -> bool { return fixture::access_depth!=0; }
auto copy_from_task(sched::task::Task& task,uint64_t address,void* dst,size_t bytes) -> bool {
    if(task.timeout_fault || address!=task.timeout_address || bytes!=sizeof(timespec)) return false;
    std::memcpy(dst,&task.timeout,bytes);return true;
}
}
'''

CASES = r'''
namespace fixture {
using Task=ker::mod::sched::task::Task;
using Stable=ker::mod::sys::usercopy::StableUserPage;
using Owner=ker::mod::sys::usercopy::UserPageFrameRef;
namespace futex=ker::syscall::futex;
namespace phys=ker::mod::mm::phys;
constexpr uint64_t WORD=0x4004;
static_assert(!std::is_copy_constructible_v<Owner> && !std::is_copy_assignable_v<Owner>);
static_assert(std::is_nothrow_move_constructible_v<Owner> && std::is_nothrow_move_assignable_v<Owner>);
struct Subject {
    PageTable root;Task task;Frame* frame{};uint64_t generation{};
    explicit Subject(unsigned slot=0,Frame* shared=nullptr) {
        frame=shared!=nullptr?shared:allocate();
        check(frame!=nullptr,"bounded frame allocated");
        if(shared!=nullptr) check(phys::page_ref_try_inc(frame->bytes.data()),"second mapping ref");
        generation=frame->generation;
        root.mapping=frame;task.pid=100+slot;task.pagemap=&root;task.cpu=slot;
        tasks.at(slot)=&task;current=&task;
        new(frame->bytes.data()+4) int(7);
    }
    void unmap() {
        auto* p=root.mapping.exchange(nullptr);
        if(p!=nullptr) phys::page_ref_dec(p->bytes.data());
    }
    uint64_t key() const { return physical(frame_index(frame))+4; }
    void leases_gone() {
        check(task.task_readers==0 && root.readers==0,"queued owner keeps no Task/root lease");
        check(access_depth==0 && irq_depth==0,"syscall restores Host access/IRQ state");
    }
    ~Subject() {
        futex::futex_wait_cleanup_for_task(&task);unmap();
        for(auto& slot:tasks) if(slot.load()==&task) slot=nullptr;
        if(current==&task) current=nullptr;
        check(task.refs==1 && task.task_readers==0 && root.readers==0,"subject temporary refs/gates drained");
    }
};
void must_survive(Subject& s,const char* label) {
    check(unavailable(s.frame),label);
    check(s.frame->generation==s.generation,"queued physical frame generation unchanged");
    auto* reused=allocate_exact(frame_index(s.frame));
    check(reused==nullptr,"independent allocator cannot reuse queued physical key");
    if(reused!=nullptr) phys::page_ref_dec(reused->bytes.data());
}
void must_reuse(Subject& s) {
    check(refs(s.frame)==0 && !unavailable(s.frame),"final waiter drop frees unmapped frame");
    auto* next=allocate_exact(frame_index(s.frame));
    check(next==s.frame && next->generation>s.generation,"exact old physical key reusable only after final deletion");
    if(next!=nullptr) phys::page_ref_dec(next->bytes.data());
}
void admitted(Subject& s) {
    check(futex::futex_wait(WORD,7,0)==0,"real wait admission succeeds");
    check(s.task.futex_waiter!=nullptr && futex::futex_table.size()==1,"actual table and Task slot published");
    s.leases_gone();
}
void pending() {
    Subject s;admitted(s);s.unmap();must_survive(s,"pending waiter keeps unmapped allocator frame");
    futex::futex_wait_cleanup_for_task(&s.task);must_reuse(s);
    check(futex::futex_wake_by_phys(s.key(),1)==0,"reused key has no stale waiter");
}
void table_first() {
    Subject s;admitted(s);s.unmap();auto* w=static_cast<futex::FutexWaiter*>(s.task.futex_waiter.load());
    check(futex::futex_table.remove(w),"actual table owner unlinked first");
    check(!futex::release_waiter_ref(w),"table drop alone does not delete waiter");
    must_survive(s,"Task-slot owner retains frame after table retirement");
    futex::futex_wait_cleanup_for_task(&s.task);must_reuse(s);
}
void slot_first() {
    Subject s;admitted(s);s.unmap();auto* w=static_cast<futex::FutexWaiter*>(s.task.futex_waiter.load());
    check(futex::claim_task_waiter(&s.task,w),"actual slot owner elected first");
    check(!futex::release_waiter_ref(w),"slot drop alone does not delete waiter");
    must_survive(s,"table owner retains frame after slot retirement");
    check(futex::futex_wake_by_phys(s.key(),1)==0,"stale slot removal is not counted as wake");
    must_reuse(s);check(irq_last_drops==1,"actual wake callback final drop occurs under IRQ bucket seam");
}
void predicate() {
    Subject s;check(futex::futex_wait(WORD,8,0)==-EAGAIN,"actual insertion predicate mismatch");
    check(s.task.futex_waiter==nullptr && futex::futex_table.size()==0,"mismatch publishes no waiter");
    check(refs(s.frame)==1,"failed insertion releases frame prefix");s.leases_gone();s.unmap();must_reuse(s);
}
void rejected_mapping(unsigned n) {
    Subject s;s.root.reject_check=n;
    check(futex::futex_wait(WORD,7,0)==-EFAULT,"original mapping rejection errno");
    check(s.task.futex_waiter==nullptr && futex::futex_table.size()==0,"mapping failure retires all published prefixes");
    check(!s.task.deferred_task_switch && s.task.wake_at_us==0 && !s.task.channel,"mapping rejection clears wait publication state");
    check(refs(s.frame)==1,"mapping rejection balances frame/pin refs");s.leases_gone();s.unmap();must_reuse(s);
}
void saturation() {
    Subject s;auto& count=allocator.page_refcounts.at(frame_index(s.frame));
    // Controlled Host metadata seam only. Restore genuine baseline before any
    // final teardown; this does not claim a million actual owners were created.
    count=UINT32_MAX-1;
    check(futex::futex_wait(WORD,7,0)==-ENOMEM,"extra checked retain refuses saturation before publication");
    check(s.task.futex_waiter==nullptr && futex::futex_table.size()==0,"saturation remains unpublished");
    check(count==UINT32_MAX-1,"stable pin release restores saturation baseline");
    futex::futex_wait_cleanup_for_task(&s.task);count=1;s.leases_gone();s.unmap();must_reuse(s);
}
void waiter_oom() {
    Subject s;fail_object=true;
    check(futex::futex_wait(WORD,7,0)==-ENOMEM,"waiter allocation failure keeps original errno");
    check(s.task.futex_waiter==nullptr && refs(s.frame)==1,"waiter OOM leaves no extra frame ref");
    fail_object=false;s.leases_gone();s.unmap();must_reuse(s);
}
void table_oom() {
    Subject s;fail_array=true;
    check(futex::futex_wait(WORD,7,0)==-ENOMEM,"real table initialization allocation refusal");
    check(s.task.futex_waiter==nullptr && refs(s.frame)==1,"table OOM precedes root/frame admission");
    fail_array=false;s.leases_gone();s.unmap();must_reuse(s);
}
void invalidate_table() {
    using Table=std::remove_reference_t<decltype(futex::futex_table)>;
    futex::futex_table.~Table();new(&futex::futex_table) Table{};
}
void invalid_table() {
    Subject s;check(futex::ensure_futex_table(),"prepare valid actual table");allocation_hook=invalidate_table;
    check(futex::futex_wait(WORD,7,0)==-ENOMEM,"post-allocation invalid table rejects unpublished node");
    check(s.task.futex_waiter==nullptr && refs(s.frame)==1,"invalid table direct delete balances prefixes");
    s.leases_gone();s.unmap();must_reuse(s);
}
void missing_task() {
    Subject s;admitted(s);s.unmap();tasks.at(0)=nullptr;
    check(futex::futex_wake_by_phys(s.key(),1)==0,"missing Task table retirement is not a wake");
    must_survive(s,"missing Task leaves slot frame owner intact");
    futex::futex_wait_cleanup_for_task(&s.task);must_reuse(s);
}
void stale_limit() {
    Subject live(0);admitted(live);Subject stale(1,live.frame);
    check(futex::futex_wait(WORD,7,0)==0,"second same-key waiter admitted");
    live.unmap();stale.unmap();tasks.at(1)=nullptr;
    check(futex::futex_wake_by_phys(live.key(),1)==1,"stale removal does not consume real wake limit");
    check(live.task.wakes==1 && live.task.futex_waiter==nullptr,"live waiter claimed once");
    must_survive(stale,"missing Task slot retains final shared frame owner");
    futex::futex_wait_cleanup_for_task(&stale.task);must_reuse(stale);
}
void replacement(bool mismatch) {
    Subject s;admitted(s);void* original=s.task.futex_waiter;
    check(futex::futex_wait(WORD,mismatch?8:7,0)==(mismatch?-EAGAIN:0),"repeat wait result preserves predicate");
    check((s.task.futex_waiter==original)==mismatch,"replacement publishes only accepted waiter");
    check(futex::futex_table.size()==1 && refs(s.frame)==2,"replacement retires exactly old owners");
    s.leases_gone();s.unmap();must_survive(s,"remaining repeated waiter retains frame");
    futex::futex_wait_cleanup_for_task(&s.task);must_reuse(s);
}
void wake_virtual() {
    Subject s;admitted(s);
    check(futex::futex_wake(WORD,1)==1 && s.task.wakes==1,"actual virtual wake claims once");
    check(s.task.futex_waiter==nullptr && futex::futex_table.size()==0 && refs(s.frame)==1,"virtual wake releases both owners plus pin");
    s.leases_gone();s.unmap();must_reuse(s);
}
void races() {
    for(unsigned i=0;i<128;++i) {
        Subject s;admitted(s);s.unmap();must_survive(s,"race starts with retained unmapped frame");
        std::latch start(1);std::atomic<int64_t> woke{-1};
        std::thread wake([&]{start.wait();woke=futex::futex_wake_by_phys(s.key(),1);});
        std::thread cancel([&]{start.wait();futex::futex_wait_cleanup_for_task(&s.task);});
        start.count_down();wake.join();cancel.join();
        check(woke==0 || woke==1,"wake/cancel election returns exact bounded count");
        check(s.task.futex_waiter==nullptr && futex::futex_table.size()==0,"racing owners fully retired");
        check(s.task.wakes<=1,"one published waiter wakes at most once");must_reuse(s);
    }
}
void moves() {
    Subject a(0),b(1);Owner first,second;
    {
        Stable pa,pb;
        check(ker::mod::sys::usercopy::pin_task_user_page(a.task,WORD,false,true,pa),"actual stable pin A");
        check(ker::mod::sys::usercopy::pin_task_user_page(b.task,WORD,false,true,pb),"actual stable pin B");
        check(pa.retain_frame(first) && pb.retain_frame(second),"actual frame-only references acquired");
        check(!pa.retain_frame(second) && refs(a.frame)==3 && refs(b.frame)==3,"occupied output unchanged on failed retention");
        Stable invalid;check(!invalid.retain_frame(first),"invalid pin cannot alter owned output");
    }
    a.leases_gone();b.leases_gone();a.unmap();b.unmap();
    Owner third(std::move(first));check(!first.valid() && third.valid(),"move construction transfers one owner");
    third=std::move(third);check(third.valid() && refs(a.frame)==1,"self move preserves owner");
    third=std::move(second);check(!second.valid() && third.valid(),"move assignment transfers replacement owner");
    must_reuse(a);check(refs(b.frame)==1,"assignment retains replacement frame");
    third.reset();third.reset();must_reuse(b);
}
void timeout_order() {
    Subject s;s.task.timeout.tv_nsec=1000000000;s.root.pin_allowed=false;
    check(futex::futex_wait(WORD,7,s.task.timeout_address)==-EFAULT,"mapping error precedes staged timeout error");
    s.root.pin_allowed=true;
    check(futex::futex_wait(WORD,7,s.task.timeout_address)==-EINVAL,"timeout error returned after valid mapping");
    check(refs(s.frame)==1 && s.task.futex_waiter==nullptr,"timeout failures acquire no queued owner");
    check(futex::futex_wait(WORD+1,7,0)==-EINVAL,"unaligned word priority unchanged");
    check(futex::futex_wake(WORD,-1)==-EINVAL && futex::futex_wake(WORD+1,0)==0,"wake count priority unchanged");
    check(futex::sys_futex(999,WORD,7,0)==static_cast<uint64_t>(-ENOSYS),"unsupported dispatcher operation unchanged");
    s.leases_gone();s.unmap();must_reuse(s);
}
void zero_timeout() {
    Subject s;fail_object=true;
    check(futex::futex_wait(WORD,7,s.task.timeout_address)==-ETIMEDOUT,"zero timeout bypasses waiter allocation");
    check(futex::futex_wait(WORD,8,s.task.timeout_address)==-EAGAIN,"zero timeout mismatch keeps original priority");
    fail_object=false;check(s.task.futex_waiter==nullptr && refs(s.frame)==1,"zero timeout acquires no queued owner");
    s.leases_gone();s.unmap();must_reuse(s);
}
void timeout_deadline() {
    Subject s;s.task.timeout.tv_nsec=1;
    check(futex::futex_wait(WORD,7,s.task.timeout_address)==0,"one-nanosecond timeout rounds up");
    check(s.task.wake_at_us==1000001,"deadline calculated at actual insertion");
    futex::futex_wait_cleanup_for_task(&s.task);s.task.timeout.tv_nsec=20000;now_us=UINT64_MAX-10;
    check(futex::futex_wait(WORD,7,s.task.timeout_address)==0,"overflow deadline admission unchanged");
    check(s.task.wake_at_us==UINT64_MAX,"deadline saturation unchanged");
    s.unmap();futex::futex_wait_cleanup_for_task(&s.task);must_reuse(s);s.leases_gone();
}
void shared_frame() {
    Subject waiter(0);admitted(waiter);Subject peer(1,waiter.frame);waiter.unmap();
    check(futex::futex_wake(WORD,1)==1,"shared physical peer preserves original wake key");
    check(waiter.task.wakes==1 && refs(peer.frame)==1,"shared-peer wake releases only waiter owner and wake pin");
    peer.unmap();must_reuse(peer);waiter.leases_gone();peer.leases_gone();
}
inline Subject* hook_subject{};
void wake_after_publication() {
    check(futex::futex_wake_by_phys(hook_subject->key(),1)==1,"concurrent post-publication wake wins slot");
}
void post_publish_wake() {
    Subject s;s.root.reject_check=2;hook_subject=&s;mapping_hook=wake_after_publication;
    check(futex::futex_wait(WORD,7,0)==-EFAULT,"mapping failure remains EFAULT after concurrent wake");
    check(s.task.wakes==1 && s.task.futex_waiter==nullptr && futex::futex_table.size()==0,"failed claim does not drop already retired owners");
    check(refs(s.frame)==1,"post-publication race balances queued and stable references");
    s.leases_gone();s.unmap();must_reuse(s);hook_subject=nullptr;
}
void all_drained() {
    check(futex::futex_table.size()==0,"actual table contains no retained nodes");
    for(size_t i=0;i<frames.size();++i) check(allocator.page_refcounts[i]==0 && frames[i].available,"bounded arena has no leaked owner");
    for(auto& slot:tasks) check(slot==nullptr,"all Host subjects detached");
    check(irq_depth==0 && access_depth==0,"terminal IRQ/access seams restored");
}
}
// Controlled nothrow allocation refusal; real source new/delete sites are not
// replaced. Usual allocation/deallocation use matching malloc/free Host seams.
void* operator new(std::size_t n) { if(auto* p=std::malloc(n==0?1:n)) return p;throw std::bad_alloc(); }
void* operator new[](std::size_t n) { return ::operator new(n); }
void* operator new(std::size_t n,const std::nothrow_t&) noexcept {
    if(fixture::fail_object.exchange(false)) return nullptr;
    auto* p=std::malloc(n==0?1:n);
    if(fixture::allocation_hook!=nullptr) { auto hook=fixture::allocation_hook;fixture::allocation_hook=nullptr;hook(); }
    return p;
}
void* operator new[](std::size_t n,const std::nothrow_t&) noexcept {
    if(fixture::fail_array.exchange(false)) return nullptr;
    return std::malloc(n==0?1:n);
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p,std::size_t) noexcept { std::free(p); }
void operator delete[](void* p,std::size_t) noexcept { std::free(p); }
void operator delete(void* p,const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p,const std::nothrow_t&) noexcept { std::free(p); }
int main(int argc,char** argv) {
    if(argc!=2) return 3;
    std::string_view v=argv[1];
    if(v=="pending")fixture::pending();
    else if(v=="table_first")fixture::table_first();
    else if(v=="slot_first")fixture::slot_first();
    else if(v=="predicate")fixture::predicate();
    else if(v=="mapping_before")fixture::rejected_mapping(1);
    else if(v=="mapping_after")fixture::rejected_mapping(2);
    else if(v=="saturation")fixture::saturation();
    else if(v=="waiter_oom")fixture::waiter_oom();
    else if(v=="table_oom")fixture::table_oom();
    else if(v=="invalid_table")fixture::invalid_table();
    else if(v=="missing_task")fixture::missing_task();
    else if(v=="stale_limit")fixture::stale_limit();
    else if(v=="replacement")fixture::replacement(false);
    else if(v=="mismatch_keeps_old")fixture::replacement(true);
    else if(v=="wake_virtual")fixture::wake_virtual();
    else if(v=="races")fixture::races();
    else if(v=="moves")fixture::moves();
    else if(v=="timeout_order")fixture::timeout_order();
    else if(v=="zero_timeout")fixture::zero_timeout();
    else if(v=="timeout_deadline")fixture::timeout_deadline();
    else if(v=="shared_frame")fixture::shared_frame();
    else if(v=="post_publish_wake")fixture::post_publish_wake();
    else return 3;
    fixture::all_drained();
    std::printf("%s vector=%s assertions=%u failures=%u last_drops=%u irq_last_drops=%u real_futex=1 real_hash=1 real_frame_owner=1\n",
        fixture::failures==0?"PASS":"FAIL",argv[1],fixture::assertions.load(),fixture::failures.load(),
        fixture::last_drops.load(),fixture::irq_last_drops.load());
    return fixture::failures==0?0:3;
}
'''


def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def capture(source: str, anchor: str, *, declaration: bool = False) -> tuple[str, dict]:
    """Capture one complete actual definition, retaining every original byte."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda match: " " * len(match[0]), source)
    starts = [m.start() for m in re.finditer(re.escape(anchor), clean)]
    if len(starts) != 1:
        raise ValueError(f"expected one actual definition for {anchor}, found {len(starts)}")
    start = clean.rfind("\n", 0, starts[0]) + 1
    parentheses = 0
    opening = None
    for index in range(starts[0], len(clean)):
        char = clean[index]
        if char == "(": parentheses += 1
        elif char == ")": parentheses -= 1
        elif char == "{" and parentheses == 0:
            opening = index
            break
        elif char == ";" and parentheses == 0:
            raise ValueError(f"declaration without a body: {anchor}")
    if opening is None:
        raise ValueError(f"no actual body: {anchor}")
    depth = 0
    for index in range(opening, len(clean)):
        if clean[index] == "{": depth += 1
        elif clean[index] == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                if declaration:
                    if clean[end:end + 1] != ";":
                        raise ValueError(f"missing complete declaration semicolon: {anchor}")
                    end += 1
                body = source[start:end]
                return body, {"name": anchor, "line": source.count("\n", 0, start) + 1,
                              "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()}
    raise ValueError(f"unterminated actual definition: {anchor}")


def generate(scratch: Path, selected: Path) -> list[dict]:
    include = scratch / "include"
    actual = []

    def write(name: str, text: str) -> None:
        path = include / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    phys_h = (KERN / "platform/mm/phys.hpp").read_text()
    owner, row = capture(phys_h, "enum class PageLookupOwner", declaration=True); actual.append(row)
    hint, row = capture(phys_h, "struct PageLookupHint", declaration=True); actual.append(row)
    common = COMMON + "\nnamespace ker::mod::mm::phys {\n" + owner + "\n" + hint + "\n}\n" + PHYS_TAIL
    write("fixture-common.hpp", common)
    virt = (KERN / "platform/mm/virt.hpp").read_text()
    user_pin, row = capture(virt, "struct UserPagePin {", declaration=True); actual.append(row)
    write("platform/mm/virt.hpp", '#pragma once\n#include "fixture-common.hpp"\n'
          'namespace ker::mod::sched::task { struct Task; }\n'
          'namespace ker::mod::mm::virt { using PageTable=paging::PageTable; inline constexpr uint64_t PADDR_INVALID=UINT64_MAX;\n'
          + user_pin + '\n'
          'auto pin_user_page_for_task(sched::task::Task*,PageTable*,vaddr_t,bool,bool,UserPagePin&) -> bool;\n'
          'auto user_page_pin_still_mapped(const UserPagePin&) -> bool;\n'
          'auto user_page_pin_commit_write(const UserPagePin&) -> bool;\n'
          'void unpin_user_page(UserPagePin&); }\n')
    write("platform/sched/task.hpp", TASK)
    write("platform/sched/scheduler.hpp", SCHEDULER)
    write("platform/sys/spinlock.hpp", '#pragma once\n#include "fixture-common.hpp"\n')
    write("platform/mm/paging.hpp", '#pragma once\n#include "fixture-common.hpp"\n')
    write("platform/mm/phys.hpp", '#pragma once\n#include "fixture-common.hpp"\n')
    write("platform/asm/cpu.hpp", '#pragma once\n')
    write("platform/mm/dyn/kmalloc.hpp", '#pragma once\n')
    write("platform/ktime/ktime.hpp", '#pragma once\n#include "fixture-common.hpp"\n'
          'namespace ker::mod::time { inline uint64_t get_us(){return fixture::now_us;} }\n')
    for name in ["platform/sys/usercopy.hpp", "util/hashtable.hpp", "abi/callnums/futex.h", "syscalls_impl/futex/futex.hpp"]:
        data = (KERN / name).read_bytes()
        destination = include / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        actual.append({"name": "whole-" + name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    futex_bytes = selected.read_bytes()
    (scratch / "actual-futex.cpp").write_bytes(futex_bytes)
    # Keep the selected TU's original local #include "futex.hpp" unchanged.
    (scratch / "futex.hpp").write_bytes((KERN / "syscalls_impl/futex/futex.hpp").read_bytes())
    actual.append({"name": "whole-selected-futex.cpp", "path": str(selected), "bytes": len(futex_bytes),
                   "sha256": hashlib.sha256(futex_bytes).hexdigest()})
    cpp = (KERN / "platform/sys/usercopy.cpp").read_text()
    user_h = (KERN / "platform/sys/usercopy.hpp").read_text()
    for anchor in ["class UserPageFrameRef {", "class StableUserPage {"]:
        _, row = capture(user_h, anchor, declaration=True)
        actual.append(row)
    anchors = ["UserPageFrameRef::~UserPageFrameRef()", "UserPageFrameRef::UserPageFrameRef(UserPageFrameRef&&",
               "UserPageFrameRef::operator=(UserPageFrameRef&&", "UserPageFrameRef::valid()", "UserPageFrameRef::reset()",
               "StableUserPage::~StableUserPage()", "StableUserPage::valid()", "StableUserPage::kernel_address()",
               "StableUserPage::physical_address()", "StableUserPage::was_dirty()", "StableUserPage::still_mapped()",
               "StableUserPage::commit_write()", "StableUserPage::retain_frame(UserPageFrameRef&", "StableUserPage::reset()",
               "auto pin_task_user_page(", "auto range_valid("]
    bodies = []
    for anchor in anchors:
        body, row = capture(cpp, anchor); actual.append(row)
        bodies.append(f'#line {row["line"]} {json.dumps(str(KERN / "platform/sys/usercopy.cpp"))}\n{body}')
    phys_cpp = (KERN / "platform/mm/phys.opt.cpp").read_text()
    increment, row = capture(phys_cpp, "auto page_ref_try_inc(void* page, PageLookupHint* hint)"); actual.append(row)
    text = '#include "fixture-common.hpp"\n#include <platform/sys/usercopy.hpp>\n#include <platform/sched/scheduler.hpp>\n'
    text += 'namespace ker::mod::mm::phys {\n' + increment + '\n}\n' + PIN_SEAMS
    text += '\nnamespace ker::mod::sys::usercopy {\n' + '\n\n'.join(bodies) + '\n}\n'
    text += '#include "actual-futex.cpp"\n' + CASES
    (scratch / "harness.cpp").write_text(text)
    return actual


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--futex-source", type=Path, default=SOURCE)
    parser.add_argument("--only", choices=["all", *VECTORS], default="all")
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 60
    scratch = None
    groups = []
    watched, before = [], []
    staged, staged_before = [], []
    result = {"pass": False, "commands": [], "scratch_removed": False,
              "native_runtime_qualified": False, "compiler_external_dependencies_after_only": True}
    try:
        selected = args.futex_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError("--cxx must resolve to one executable Host ELF compiler")
        with compiler.open("rb") as executable:
            if executable.read(4) != b"\x7fELF":
                raise ValueError("--cxx must resolve to one executable Host ELF compiler")
        watched = [selected, Path(__file__).resolve(), compiler, *[KERN / name for name in
                   ["platform/sys/usercopy.hpp", "platform/sys/usercopy.cpp", "platform/mm/phys.hpp",
                    "platform/mm/phys.opt.cpp", "platform/mm/virt.hpp", "util/hashtable.hpp",
                    "abi/callnums/futex.h", "syscalls_impl/futex/futex.hpp"]]]
        before = [pin(p) for p in watched]
        result["input_pins"] = before
        result["futex_source"] = before[0]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-futex-frame-", dir="/tmp"))
        result["scratch"] = str(scratch)
        result["actual_definitions"] = generate(scratch, selected)
        staged = sorted(path for path in scratch.rglob("*") if path.is_file())
        staged_before = [pin(path) for path in staged]
        result["actual_staged_sources_before"] = staged_before
        source, binary, depfile = scratch / "harness.cpp", scratch / "futex-frame-test", scratch / "dependencies.d"
        argv = [str(compiler), "-std=c++23", "-O1", "-g0", "-pipe", "-pthread", "-fsanitize=address,undefined",
                "-fno-omit-frame-pointer", "-MD", "-MF", str(depfile), "-I", str(scratch / "include"),
                str(source), "-o", str(binary)]
        vectors = VECTORS if args.only == "all" else [args.only]
        commands = [argv, *[[str(binary), vector] for vector in vectors]]
        for index, argv in enumerate(commands):
            log_path = scratch / f"command-{index}.log"
            row = command(argv, log_path, scratch, deadline, groups)
            result["commands"].append(row)
            log = log_path.read_text(errors="replace")
            if index > 0 or row["exit"] != 0:
                print(log[-32768:], end="", flush=True)
            if index == 0 and depfile.exists():
                result["actual_dependencies_after"] = dependency_pins(depfile)
                if row["exit"] == 0:
                    result["actual_compiled_binary"] = pin(binary)
            if row["exit"] != 0:
                raise ValueError(f"actual-source {'compile/link' if index == 0 else 'runtime oracle'} failed with exit {row['exit']}")
            if index > 0:
                expected = re.escape(vectors[index - 1])
                if not re.search(rf"^PASS vector={expected} assertions=\d+ failures=0 last_drops=\d+ irq_last_drops=\d+ real_futex=1 real_hash=1 real_frame_owner=1$", log, re.M):
                    raise ValueError("actual compiled case lacks exact terminal PASS row")
        result["pass"] = True
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if staged_before:
            try:
                result["actual_staged_sources_after"] = [pin(path) for path in staged]
                result["staged_sources_held"] = result["actual_staged_sources_after"] == staged_before
            except OSError:
                result["staged_sources_held"] = False
            if not result["staged_sources_held"]:
                result["pass"] = False
                result["staged_source_error"] = "actual staged source bytes changed during compile/runtime"
        if scratch is not None and not groups:
            try:
                shutil.rmtree(scratch)
                result["scratch_removed"] = not scratch.exists()
            except OSError as error:
                result["pass"] = False
                result["cleanup_error"] = f"scratch removal: {error}"
        if groups:
            result["pass"] = False
            result["cleanup_error"] = f"owned sessions {groups} not proven gone; scratch preserved"
        if before:
            try:
                result["source_after"] = [pin(p) for p in watched]
                result["sources_held"] = result["source_after"] == before
            except OSError:
                result["sources_held"] = False
            if not result["sources_held"]:
                result["pass"] = False
                result["source_error"] = "selected source/driver/compiler changed during actual execution"
        if time.monotonic() > deadline:
            result["pass"] = False
            result["deadline_error"] = "60-second whole budget exceeded including owned cleanup"
        result["seconds_before_publication"] = round(time.monotonic() - started, 6)
        print(json.dumps(result), flush=True)
    if time.monotonic() > deadline:
        return 1
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
