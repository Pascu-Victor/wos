#!/usr/bin/env python3
"""Compile actual root controls, PTE/pin key capture and whole futex against Host primitives.

The current root-control types/arrays, complete create/retire/pool/pin/key methods,
actual frame-owner/stable methods, selected whole futex TU, and actual hash/paging
headers are compiled unchanged. Raw physical allocator, CPU/IRQ/Task/scheduler,
absent topology/emergency-reserve compatibility, PTE fixture publication and page-fault admission are explicit controlled seams.
Independent roots/control allocations/frame counts and exact wake outcomes are
oracles. This is Host regression, not native MM, physical allocator or Goal39
lifecycle/coherence qualification. Full archived old futex is a genuine selectable
negative control; it must compile before isolation/lost-wake failure.
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
            if (path / "modules/kern/src").is_dir() and (path / "tests/host").is_dir())
KERN = ROOT / "modules/kern/src"
SOURCE = KERN / "syscalls_impl/futex/futex.cpp"
VECTORS = ['roots_4097', 'roots_1000', 'pool_reuse', 'metadata_oom', 'root_oom', 'serial_exhaustion', 'concurrent_growth', 'capture_bounds', 'capture_busy', 'capture_unknown', 'capture_revalidate', 'private_roots', 'private_aliases', 'private_cow', 'shared_roots', 'shared_offsets', 'phys_private_refusal', 'root_reuse_isolation', 'same_root_tasks', 'wake_cleanup_races', 'kernel_release', 'duplicate_admission', 'table_first', 'slot_first', 'final_ref_guard', 'final_kind_guard', 'key_collisions', 'resolver_unknown_ps', 'current_leaf_write_commit']
SCRATCH_BUDGET = 64 * 1024 * 1024
LOG_BUDGET = 1024 * 1024
FILE_BUDGET = 8 * 1024 * 1024
COMMON = '#pragma once\n#include <platform/mm/paging.hpp>\n#include <algorithm>\n#include <array>\n#include <atomic>\n#include <cassert>\n#include <cerrno>\n#include <cstdint>\n#include <cstdio>\n#include <cstdlib>\n#include <cstring>\n#include <ctime>\n#include <latch>\n#include <map>\n#include <mutex>\n#include <new>\n#include <set>\n#include <string_view>\n#include <thread>\n#include <utility>\n#include <vector>\nnamespace fixture {\ninline std::atomic<unsigned> assertions{},failures{},last_drops{},irq_last_drops{};\ninline thread_local unsigned irq_depth{},access_depth{};\ninline void check(bool value,const char* message){++assertions;if(!value&&failures.fetch_add(1)<16)std::fprintf(stderr,"FAIL: %s\\n",message);}\nstruct IrqLock {std::mutex lock;uint64_t lock_irqsave(){lock.lock();auto old=irq_depth;++irq_depth;return old;}void unlock_irqrestore(uint64_t old){check(irq_depth==old+1,"balanced IRQ lock");irq_depth=old;lock.unlock();}};\nstruct Frame{alignas(4096)std::array<unsigned char,4096> bytes{};std::atomic<bool>available{true};std::atomic<uint64_t>generation{};};\ninline std::array<Frame,8>frames;\ninline size_t index_of(const void* p){for(size_t i=0;i<frames.size();++i)if(p==frames[i].bytes.data())return i;return frames.size();}\ninline uint64_t physical(size_t i){return 0x100000ULL+i*4096;}\ninline std::atomic<uint64_t>now_us{1000000};\nstruct InvariantStop{};\ninline std::atomic<bool>fail_root{},fail_metadata{};\ninline std::atomic<unsigned>metadata_allocations{},metadata_frees{};\ninline std::latch* metadata_barrier{};\n}\nnamespace ker::mod::sys{using Spinlock=fixture::IrqLock;}\nnamespace ker::mod::mm{using vaddr_t=uint64_t;using paddr_t=uint64_t;struct PageAllocator{std::array<std::atomic<uint32_t>,8>page_refcounts{};};}\nnamespace ker::mod::mm::addr {\ninline uint64_t get_phys_pointer(uint64_t pointer){return pointer;}\ninline uint64_t get_virt_pointer(uint64_t physical){if(physical>=0x100000&&physical<0x108000)return reinterpret_cast<uint64_t>(fixture::frames[(physical-0x100000)/4096].bytes.data())+(physical&4095);return physical;}\n}\n[[noreturn]]inline void hcf(){throw fixture::InvariantStop{};}\nstruct HostLog{template<class...T>static void critical(const char*,T...){}};\n'

PHYS_TAIL = '\nnamespace fixture {\ninline ker::mod::mm::PageAllocator allocator;\nstruct RefStats { std::atomic<unsigned> inc_ops{},inc_cas_retries{}; };\ninline RefStats ref_stats;\ninline Frame* allocate_exact(size_t i) {\n    bool expected=true;\n    if(!frames.at(i).available.compare_exchange_strong(expected,false)) return nullptr;\n    check(allocator.page_refcounts.at(i).load()==0, "free frame has no outstanding references");\n    allocator.page_refcounts.at(i).store(1);\n    ++frames.at(i).generation;\n    frames.at(i).bytes.fill(0);\n    return &frames.at(i);\n}\ninline Frame* allocate() { for(size_t i=0;i<frames.size();++i) if(auto* p=allocate_exact(i)) return p; return nullptr; }\ninline size_t frame_index(Frame* f) { return index_of(f->bytes.data()); }\ninline uint32_t refs(Frame* f) { return allocator.page_refcounts.at(frame_index(f)).load(); }\ninline bool unavailable(Frame* f) { return !f->available.load(); }\n}\nnamespace ker::mod::mm::phys {\ninline auto page_ref_stats_for_current_cpu() -> fixture::RefStats& { return fixture::ref_stats; }\ninline auto find_allocator_for_page_cached(void* page, PageLookupHint* hint,\n                                           PageAllocator*& out, uint32_t& index) -> bool {\n    size_t i=fixture::index_of(page);\n    if(i>=fixture::frames.size()) return false;\n    // The Host arena represents only REGULAR_ZONE. A hint is a lookup cache,\n    // not permission to access another allocator/domain.\n    if(hint!=nullptr && (hint->allocator!=&fixture::allocator || hint->owner!=PageLookupOwner::REGULAR_ZONE)) {\n        hint->allocator=&fixture::allocator; hint->owner=PageLookupOwner::REGULAR_ZONE;\n    }\n    out=&fixture::allocator; index=static_cast<uint32_t>(i); return true;\n}\ninline auto page_ref_dec(void* page, PageLookupHint* hint=nullptr) -> uint32_t {\n    PageAllocator* a=nullptr; uint32_t i=0;\n    if(!find_allocator_for_page_cached(page,hint,a,i)) { fixture::check(false,"release owned allocator frame"); return 0; }\n    auto& count=a->page_refcounts[i]; uint32_t old=count.load();\n    for(;;) {\n        if(old==0) { fixture::check(false,"no duplicate frame drop"); return 0; }\n        if(count.compare_exchange_weak(old,old-1)) break;\n    }\n    if(old==1) {\n        ++fixture::last_drops;\n        if(fixture::irq_depth!=0) ++fixture::irq_last_drops;\n        // Availability/generation are the independent allocator-reuse oracle;\n        // a physical-key value alone cannot keep a frame unavailable.\n        fixture::frames.at(i).available.store(true);\n    }\n    return old-1;\n}\nauto page_ref_try_inc(void*,PageLookupHint*) -> bool;\ninline auto page_ref_try_inc(void* p) -> bool { return page_ref_try_inc(p,nullptr); }\n}\n'

RAW_PHYS = 'namespace fixture {\nstruct RawPage{ker::mod::mm::PhysicalPageOwner owner;ker::mod::mm::PageKind kind{ker::mod::mm::PageKind::NORMAL};uint32_t refs{1};};\ninline std::mutex raw_lock;inline std::map<void*,RawPage>raw_pages;inline std::vector<ker::mod::mm::paging::PageTable*>root_pool;\ninline void* raw_allocate(ker::mod::mm::PhysicalPageOwner owner){\n check(irq_depth==0,"raw allocator only outside mapping/bucket IRQ lock");\n if(owner==ker::mod::mm::PhysicalPageOwner::USER_ROOT_CONTROL&&fail_metadata.load())return nullptr;\n if(owner==ker::mod::mm::PhysicalPageOwner::PAGE_TABLE&&fail_root.load())return nullptr;\n auto*p=std::aligned_alloc(4096,4096);if(!p)return nullptr;std::memset(p,0,4096);\n {std::lock_guard l(raw_lock);check(!raw_pages.contains(p),"raw allocation not reused while owned");raw_pages.emplace(p,RawPage{owner});}\n if(owner==ker::mod::mm::PhysicalPageOwner::USER_ROOT_CONTROL){++metadata_allocations;if(auto*g=metadata_barrier){g->count_down();g->wait();}}\n return p;\n}\ninline void raw_free(void*p){if(!p)return;check(irq_depth==0,"raw free outside metadata/bucket lock");RawPage metadata{};{std::lock_guard l(raw_lock);auto i=raw_pages.find(p);check(i!=raw_pages.end(),"free exact owned raw allocation");if(i==raw_pages.end())return;metadata=i->second;check(metadata.refs==1,"raw physical page has sole final owner");raw_pages.erase(i);}if(metadata.owner==ker::mod::mm::PhysicalPageOwner::USER_ROOT_CONTROL)++metadata_frees;std::free(p);}\ninline void set_root_metadata(void*p,ker::mod::mm::PageKind kind,uint32_t refs){std::lock_guard l(raw_lock);raw_pages.at(p).kind=kind;raw_pages.at(p).refs=refs;}\nvoid free_pool();\n\n}\nnamespace ker::mod::mm::phys {\ninline auto page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner owner,std::string_view) -> void*{return fixture::raw_allocate(owner);}\ninline void page_free(void*p){fixture::raw_free(p);}\ninline auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner o,std::string_view)->void*{return fixture::raw_allocate(o);}\ninline auto page_alloc_full_overwrite_page_with_reclaim(PhysicalPageOwner o,std::string_view)->void*{return fixture::raw_allocate(o);}\ninline auto page_mark_kind(void*p,PageKind k)->bool{std::lock_guard l(fixture::raw_lock);auto i=fixture::raw_pages.find(p);if(i==fixture::raw_pages.end())return false;i->second.kind=k;return true;}\ninline auto page_reassign_owner(void*p,PhysicalPageOwner o)->bool{std::lock_guard l(fixture::raw_lock);auto i=fixture::raw_pages.find(p);if(i==fixture::raw_pages.end())return false;i->second.owner=o;return true;}\ninline auto page_kind_get(void*p)->PageKind{std::lock_guard l(fixture::raw_lock);auto i=fixture::raw_pages.find(p);return i==fixture::raw_pages.end()?PageKind::NORMAL:i->second.kind;}\ninline auto page_ref_get(void*p)->uint32_t{auto i=fixture::index_of(p);if(i<fixture::frames.size())return fixture::allocator.page_refcounts[i];std::lock_guard l(fixture::raw_lock);return fixture::raw_pages.at(p).refs;}\n}'

TASK = '\n#pragma once\n#include "fixture-common.hpp"\n#include <platform/mm/virt.hpp>\nnamespace ker::mod::sched::task {\nenum class WaitChannelKind { FUTEX };\nstruct Task {\n    uint64_t pid{},cpu{};\n    std::atomic<void*> futex_waiter{};\n    std::atomic<unsigned> task_readers{}, refs{1}, wakes{};\n    mm::paging::PageTable* pagemap{};\n    std::atomic<uint64_t> wake_at_us{};\n    std::atomic<bool> deferred_task_switch{}, channel{}, root_allowed{true};\n    uint64_t timeout_address=0x9000; timespec timeout{}; bool timeout_fault{};\n    uint64_t load_cpu() const { return cpu; }\n    void set_wait_channel(const char*,WaitChannelKind) { channel=true; }\n    void clear_wait_channel() { channel=false; }\n    bool try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) {\n        if(!root_allowed || pagemap==nullptr) return false;\n        ++task_readers; out=pagemap; return true;\n    }\n    void release_usercopy_pagemap() { fixture::check(task_readers.fetch_sub(1)>0,"Task root lease released once"); }\n    void release() { fixture::check(refs.fetch_sub(1)>1,"temporary lookup Task reference released once"); }\n};\n}\nnamespace fixture {\ninline thread_local ker::mod::sched::task::Task* current{};\ninline std::array<std::atomic<ker::mod::sched::task::Task*>,32> tasks{};\n}\n'

SCHEDULER = '\n#pragma once\n#include <platform/sched/task.hpp>\nnamespace ker::mod::sched {\nenum class EventWakeDeferredSwitch { CANCEL };\ninline bool interrupts_enabled(){return fixture::irq_depth==0;}\ninline auto get_current_task() -> task::Task* { return fixture::current; }\ninline auto find_task_by_pid_safe(uint64_t pid) -> task::Task* {\n    for(auto& slot:fixture::tasks) if(auto* t=slot.load();t!=nullptr && t->pid==pid) { ++t->refs; return t; }\n    return nullptr;\n}\ninline void wake_task_from_event_on_cpu(task::Task* t,uint64_t,EventWakeDeferredSwitch) {\n    ++t->wakes; t->deferred_task_switch=false; t->wake_at_us=0; t->clear_wait_channel();\n}\n}\n'

MAP_SEAMS = 'namespace fixture {\nnamespace paging=ker::mod::mm::paging;\ninline paging::PageTableEntry& leaf(paging::PageTable*root,uint64_t word,bool create){auto*t=root;for(int level=4;level>1;--level){auto&e=t->entries[(word>>(12+9*(level-1)))&511];if(!e.present&&create){auto*child=static_cast<paging::PageTable*>(raw_allocate(ker::mod::mm::PhysicalPageOwner::PAGE_TABLE));check(child!=nullptr,"Host child PTE allocation");if(!child)throw InvariantStop{};set_root_metadata(child,ker::mod::mm::PageKind::PAGE_TABLE,1);e.present=e.user=e.writable=1;e.frame=reinterpret_cast<uint64_t>(child)>>12;}if(!e.present)throw InvariantStop{};t=reinterpret_cast<paging::PageTable*>(ker::mod::mm::addr::get_virt_pointer(e.frame<<12));}return t->entries[(word>>12)&511];}\ninline void map_leaf(paging::PageTable*root,uint64_t word,Frame*f,bool shared){auto&e=leaf(root,word,true);auto flags=ker::mod::mm::virt::user_mapping_pin_lock.lock_irqsave();uint64_t old=e.present?(e.frame<<12):UINT64_MAX;auto pa=physical(frame_index(f));if(old!=pa){check(ker::mod::mm::phys::page_ref_try_inc(f->bytes.data()),"mapping frame owner acquired");if(old!=UINT64_MAX)ker::mod::mm::phys::page_ref_dec(reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(old)));}uint64_t raw=pa|paging::PAGE_PRESENT|paging::PAGE_USER|paging::PAGE_WRITE|(shared?paging::PAGE_SHARED:0);std::memcpy(&e,&raw,8);ker::mod::mm::virt::user_mapping_pin_lock.unlock_irqrestore(flags);}\ninline void set_leaf_flags(paging::PageTable*root,uint64_t word,uint64_t policy){auto flags=ker::mod::mm::virt::user_mapping_pin_lock.lock_irqsave();auto&e=leaf(root,word,false);uint64_t raw=(e.frame<<12)|policy;std::memcpy(&e,&raw,8);ker::mod::mm::virt::user_mapping_pin_lock.unlock_irqrestore(flags);}\ninline void remove_leaf(paging::PageTable*root,uint64_t word){auto flags=ker::mod::mm::virt::user_mapping_pin_lock.lock_irqsave();auto&e=leaf(root,word,false);if(e.present){auto pa=e.frame<<12;e={};ker::mod::mm::phys::page_ref_dec(reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(pa)));}ker::mod::mm::virt::user_mapping_pin_lock.unlock_irqrestore(flags);}\ninline void clear_level(paging::PageTable*root,int level){for(auto&e:root->entries){if(!e.present)continue;if(level==1){ker::mod::mm::phys::page_ref_dec(reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(e.frame<<12)));}else{auto*child=reinterpret_cast<paging::PageTable*>(ker::mod::mm::addr::get_virt_pointer(e.frame<<12));clear_level(child,level-1);raw_free(child);}e={};}}\ninline void clear_tree(paging::PageTable*root){if(root)clear_level(root,4);}\ninline void init_kernel_root(){auto*p=static_cast<paging::PageTable*>(raw_allocate(ker::mod::mm::PhysicalPageOwner::PAGE_TABLE));set_root_metadata(p,ker::mod::mm::PageKind::PAGE_TABLE,1);ker::mod::mm::virt::kernel_pagemap=p;}\ninline void free_pool(){auto& pool=ker::mod::mm::virt::page_table_pool;while(pool.count){auto*p=pool.pages[--pool.count];pool.pages[pool.count]=nullptr;raw_free(p);}if(auto*p=ker::mod::mm::virt::kernel_pagemap){ker::mod::mm::virt::kernel_pagemap=nullptr;raw_free(p);}}\n}\nnamespace ker::mod::mm::virt {\ninline bool ensure_user_page_mapped(sched::task::Task*,vaddr_t){return true;}\ninline bool ensure_user_page_writable(sched::task::Task*,vaddr_t){return true;}\n}\nnamespace ker::mod::sys::usercopy {\nUserAccessGuard::UserAccessGuard(){m_flags=fixture::access_depth;++fixture::access_depth;}\nUserAccessGuard::~UserAccessGuard(){fixture::check(fixture::access_depth==m_flags+1,"balanced access window");fixture::access_depth=static_cast<unsigned>(m_flags);}\nauto access_window_active()->bool{return fixture::access_depth!=0;}\nauto copy_from_task(sched::task::Task&task,uint64_t address,void*dst,size_t size)->bool{if(task.timeout_fault||address!=task.timeout_address||size!=sizeof(timespec))return false;std::memcpy(dst,&task.timeout,size);return true;}\n}\n'

CASES = 'namespace fixture {\nnamespace mm=ker::mod::mm;namespace virt=mm::virt;namespace phys=mm::phys;namespace paging=mm::paging;namespace futex=ker::syscall::futex;\nconstexpr uint64_t WORD=0x4040,OTHER=0x5040;\nstruct OwnedRoot{\n paging::PageTable* root{};bool registered{true};\n OwnedRoot(){root=virt::create_pagemap_may_fail();check(root!=nullptr,"actual root admission");}\n OwnedRoot(const OwnedRoot&)=delete;\n ~OwnedRoot(){reset();}\n void reset(){if(!root)return;clear_tree(root);if(registered)virt::release_pagemap(root,0);else if(!virt::try_release_page_table_to_pool(root,{}))phys::page_free(root);root=nullptr;}\n};\nstruct FrameOwner{Frame* frame{allocate()};FrameOwner(){check(frame!=nullptr,"independent allocator frame");}~FrameOwner(){if(frame)phys::page_ref_dec(frame->bytes.data());}};\ninline uint64_t serial(paging::PageTable* r){return virt::selftest_user_root_incarnation(r);}\ninline auto stats(){virt::UserRootControlStats s{};virt::get_user_root_control_stats_snapshot(s);return s;}\nstruct Actor {\n ker::mod::sched::task::Task task;OwnedRoot owned;FrameOwner frame;bool owns_mapping{true};\n Actor(uint64_t pid=1,uint64_t word=WORD,bool shared=false){task.pid=pid;task.pagemap=owned.root;tasks[pid].store(&task);map_leaf(owned.root,word,frame.frame,shared);*reinterpret_cast<int*>(frame.frame->bytes.data()+(word&4095))=0;}\n ~Actor(){futex::futex_wait_cleanup_for_task(&task);tasks[task.pid].store(nullptr);owned.reset();}\n void select(){current=&task;}\n void alias(uint64_t word,Frame* f,bool shared){map_leaf(owned.root,word,f,shared);}\n int64_t wait(uint64_t word=WORD){select();return futex::futex_wait(word,0,0);}\n int64_t wake(uint64_t word=WORD){select();return futex::futex_wake(word,1);}\n};\nvoid roots_4097(){auto before=stats();std::vector<paging::PageTable*>r;for(size_t i=0;i<4097;++i){auto*p=virt::create_pagemap_may_fail();check(p!=nullptr,"more than4096 actual-body roots admitted");if(!p)break;r.push_back(p);}std::set<uint64_t>ids;std::set<size_t>buckets;for(auto*p:r){ids.insert(serial(p));buckets.insert(virt::user_root_control_bucket(p));}check(r.size()==4097&&ids.size()==4097&&!ids.contains(0),"actual complete unique nonzero root identities");check(buckets.size()<r.size(),"actual hash collisions exercised");check(stats().active_roots==before.active_roots+r.size(),"all roots counted");for(auto*p:r)virt::release_pagemap(p,0);check(stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"complete4097-root metadata baseline restored");}\nvoid roots_1000(){auto before=stats();uint64_t last=before.last_serial;for(unsigned i=0;i<1000;++i){auto*r=virt::create_pagemap_may_fail();check(r!=nullptr,"actual repeated root create");if(!r)break;auto id=serial(r);check(id>last,"actual repeated root incarnation monotonic");last=id;virt::release_pagemap(r,0);check(stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"each actual retire restores control baseline");}}\nvoid pool_reuse(){auto*r=virt::create_pagemap_may_fail();auto id=serial(r);virt::release_pagemap(r,0);auto*q=virt::create_pagemap_may_fail();check(q==r&&serial(q)>id,"actual factory assigns new ID to same pooled root");virt::release_pagemap(q,0);}\nvoid metadata_oom(){auto before=stats();fail_metadata=true;check(virt::create_pagemap_may_fail()==nullptr,"actual metadata OOM refuses root");fail_metadata=false;check(stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages&&stats().last_serial==before.last_serial,"metadata OOM has no published prefix");OwnedRoot recovery;}\nvoid root_oom(){auto before=stats();fail_root=true;check(virt::create_pagemap_may_fail()==nullptr,"actual root allocation OOM");fail_root=false;check(stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"root OOM has no control prefix");}\nvoid serial_exhaustion(){auto before=stats();virt::user_root_control_last_serial=UINT64_MAX-1;auto*r=virt::create_pagemap_may_fail();check(r&&serial(r)==UINT64_MAX,"actual final nonzero serial issued");check(virt::create_pagemap_may_fail()==nullptr&&virt::create_pagemap_may_fail()==nullptr,"actual global serial saturation permanent refusal");if(r)virt::release_pagemap(r,0);check(stats().last_serial==UINT64_MAX&&stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"exhaustion no wrap or owner leak");}\nvoid concurrent_growth(){auto before=stats();unsigned a=metadata_allocations,f=metadata_frees;std::latch gate(2);metadata_barrier=&gate;paging::PageTable*x{},*y{};std::thread t([&]{x=virt::create_pagemap_may_fail();});std::thread u([&]{y=virt::create_pagemap_may_fail();});t.join();u.join();metadata_barrier=nullptr;check(x&&y&&x!=y&&serial(x)!=serial(y),"concurrent actual roots unique");check(metadata_allocations==a+2&&metadata_frees==f+1,"actual raced spare chunk disposed");if(x)virt::release_pagemap(x,0);if(y)virt::release_pagemap(y,0);check(stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"concurrent-growth complete baseline");}\nvoid capture_bounds(){Actor a;virt::UserPagePin p{};check(virt::pin_user_page(a.owned.root,WORD,false,p,false),"actual pin");for(uint64_t w:std::array<uint64_t,5>{WORD+1,WORD+2,0x4FFEULL,0x5000ULL,0x3FFCULL}){virt::UserFutexIdentity id{.virtual_word=55,.physical_word=66,.shared=true};check(!virt::capture_user_futex_identity(p,w,id)&&id.root_incarnation==0&&id.virtual_word==55&&id.physical_word==66&&id.shared,"actual invalid word preserves sentinel output");}for(uint64_t w:std::array<uint64_t,3>{0x4000ULL,0x4004ULL,0x4FFCull}){virt::UserFutexIdentity id{};check(virt::capture_user_futex_identity(p,w,id)&&id.virtual_word==w&&id.physical_word==physical(frame_index(a.frame.frame))+(w&4095),"actual aligned same-page word offsets");}virt::unpin_user_page(p);}\nvoid capture_busy(){Actor a;virt::UserPagePin p{};check(virt::pin_user_page(a.owned.root,WORD,false,p,false),"actual pin");virt::UserFutexIdentity id{};check(virt::capture_user_futex_identity(p,WORD,id),"actual identity");auto original=id;check(!virt::capture_user_futex_identity(p,WORD,id)&&id.root_incarnation==original.root_incarnation&&id.virtual_word==original.virtual_word&&id.physical_word==original.physical_word&&id.shared==original.shared,"busy output untouched");id.root_incarnation++;check(!virt::user_futex_identity_still_mapped(p,id),"wrong nonzero incarnation rejects");virt::unpin_user_page(p);}\nvoid capture_unknown(){OwnedRoot r;r.reset();r.registered=false;r.root=virt::alloc_zeroed_page_table(true);FrameOwner f;map_leaf(r.root,WORD,f.frame,false);virt::UserPagePin p{};check(virt::pin_user_page(r.root,WORD,false,p,false),"ordinary pin of privately owned unknown root");virt::UserFutexIdentity id{};check(!virt::capture_user_futex_identity(p,WORD,id)&&id.root_incarnation==0,"actual unknown root refuses no fallback");virt::unpin_user_page(p);}\nvoid capture_revalidate(){Actor a;virt::UserPagePin p{};check(virt::pin_user_page(a.owned.root,WORD,false,p,false),"actual pin");virt::UserFutexIdentity id{};check(virt::capture_user_futex_identity(p,WORD,id),"actual identity");set_leaf_flags(a.owned.root,WORD,paging::PAGE_PRESENT|paging::PAGE_USER|paging::PAGE_SHARED);check(virt::user_page_pin_still_mapped(p)&&!virt::user_futex_identity_still_mapped(p,id),"sharing-bit change detected beyond frame");set_leaf_flags(a.owned.root,WORD,paging::PAGE_PRESENT|paging::PAGE_USER);check(virt::user_futex_identity_still_mapped(p,id),"same complete original identity restored");remove_leaf(a.owned.root,WORD);check(!virt::user_futex_identity_still_mapped(p,id),"actual unmap invalidates identity");virt::unpin_user_page(p);}\nvoid private_roots(){Actor a(1),b(2);b.alias(WORD,a.frame.frame,false);check(a.wait()==0,"private waiter admitted");check(b.wake()==0&&a.task.futex_waiter!=nullptr&&a.task.wakes==0,"different private root same frame cannot wake");check(a.wake()==1,"same root private wake");}\nvoid private_aliases(){Actor a;a.alias(OTHER,a.frame.frame,false);check(a.wait()==0,"private waiter admitted");check(a.wake(OTHER)==0&&a.task.futex_waiter!=nullptr,"same root different VA on zero frame cannot wake");check(a.wake()==1,"same private virtual word wakes");}\nvoid private_cow(){Actor a;check(a.wait()==0,"private waiter on original frame");FrameOwner replacement;map_leaf(a.owned.root,WORD,replacement.frame,false);check(a.wake()==1&&a.task.futex_waiter==nullptr,"private same-root replacement frame wake finds queued old pin");}\nvoid shared_roots(){Actor a(1,WORD,true),b(2,OTHER,true);b.alias(OTHER,a.frame.frame,true);check(a.wait()==0,"explicit shared waiter");check(b.wake(OTHER)==1&&a.task.wakes==1,"shared same-frame different roots/VAs communicate");}\nvoid shared_offsets(){Actor a(1,WORD,true),b(2,OTHER,true);b.alias(OTHER,a.frame.frame,true);check(a.wait()==0,"shared waiter");check(b.wake(OTHER+4)==0&&a.task.futex_waiter!=nullptr,"shared full byte offset separates words");check(b.wake(OTHER)==1,"exact shared physical word wakes");}\nvoid phys_private_refusal(){Actor a;check(a.wait()==0,"private waiter");check(futex::futex_wake_by_phys(physical(frame_index(a.frame.frame))+(WORD&4095),1)==0&&a.task.futex_waiter!=nullptr,"physical helper cannot recover private key");check(a.wake()==1,"private virtual helper finds actual key");}\nvoid same_root_tasks(){Actor a;ker::mod::sched::task::Task peer{};peer.pid=2;peer.pagemap=a.owned.root;tasks[2].store(&peer);check(a.wait()==0,"private thread waiter");current=&peer;check(futex::futex_wake(WORD,1)==1&&a.task.wakes==1,"distinct Task/process metadata same root communicates");tasks[2].store(nullptr);}\nvoid root_reuse_isolation(){Actor a;check(a.wait()==0,"private old-root waiter");auto old=a.owned.root;auto oldid=serial(old);a.owned.reset();a.task.pagemap=nullptr;OwnedRoot replacement;check(replacement.root==old&&serial(replacement.root)>oldid,"actual root address reused");FrameOwner fresh;map_leaf(replacement.root,WORD,fresh.frame,false);ker::mod::sched::task::Task peer{};peer.pid=2;peer.pagemap=replacement.root;tasks[2].store(&peer);current=&peer;check(futex::futex_wake(WORD,1)==0&&a.task.futex_waiter!=nullptr,"new root cannot wake retired image");futex::futex_wait_cleanup_for_task(&a.task);tasks[2].store(nullptr);}\nvoid waiter_order(bool slot_first){Actor a;check(a.wait()==0,"actual waiter");auto*w=static_cast<futex::FutexWaiter*>(a.task.futex_waiter.load());if(slot_first){check(futex::claim_task_waiter(&a.task,w),"slot elected");check(!futex::release_waiter_ref(w),"slot drop retains table owner");check(futex::futex_table.remove(w),"table owner removed");check(futex::release_waiter_ref(w),"table final owner releases frame");}else{check(futex::futex_table.remove(w),"table removed first");check(!futex::release_waiter_ref(w),"table drop retains slot");futex::futex_wait_cleanup_for_task(&a.task);}check(a.task.futex_waiter==nullptr&&futex::futex_table.size()==0,"both owners fully retired");}\nvoid wake_cleanup_races(){for(unsigned i=0;i<128;++i){Actor a;check(a.wait()==0,"raced actual private waiter");std::latch start(2);int64_t woke=-1;std::thread wake([&]{current=&a.task;start.count_down();start.wait();woke=futex::futex_wake(WORD,1);});std::thread cancel([&]{start.count_down();start.wait();futex::futex_wait_cleanup_for_task(&a.task);});wake.join();cancel.join();check(woke==0||woke==1,"race election at most once");check(a.task.futex_waiter==nullptr&&futex::futex_table.size()==0&&a.task.wakes==static_cast<unsigned>(woke),"actual wake/cleanup ownership election");}}\nvoid final_owner(bool bad_kind){OwnedRoot r;auto before=stats();auto id=serial(r.root);set_root_metadata(r.root,bad_kind?mm::PageKind::NORMAL:mm::PageKind::PAGE_TABLE,bad_kind?1:2);bool stopped=false;try{virt::release_pagemap(r.root,0);}catch(const InvariantStop&){stopped=true;}check(stopped&&serial(r.root)==id&&stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"actual invalid final-owner guard leaves association intact");set_root_metadata(r.root,mm::PageKind::PAGE_TABLE,1);}\nvoid kernel_release(){auto before=stats();virt::release_pagemap(virt::kernel_pagemap,0);check(stats().active_roots==before.active_roots&&stats().control_pages==before.control_pages,"kernel root never has user identity or retirement");}\nvoid duplicate_admission(){OwnedRoot r;auto before=stats();virt::UserRootControlChunk*spare=nullptr;auto flags=virt::user_mapping_pin_lock.lock_irqsave();auto result=virt::attach_user_root_control_locked(r.root,spare);virt::user_mapping_pin_lock.unlock_irqrestore(flags);check(result==virt::UserRootAttachResult::DUPLICATE&&spare==nullptr&&stats().active_roots==before.active_roots,"actual duplicate association refuses without capacity/serial consumption");}\nvoid key_collisions(){\n#ifdef FIXTURE_TYPED_FUTEX_KEYS\n struct Node{futex::FutexKey key{};Node*hash_next{};};\n struct Extract{auto operator()(const Node&n)const{return n.key;}};\n ker::util::IntrHashTable<Node,Extract,futex::FutexKeyHash,futex::FutexKeyEqual>table;\n check(table.init(1),"actual collision table admission");\n std::array<Node,32>nodes{};futex::FutexKeyHash hash;futex::FutexKeyEqual equal;\n for(size_t i=0;i<nodes.size();++i){nodes[i].key={futex::FutexKeyKind::PRIVATE,i+1,i+1};check(hash(nodes[i].key)==hash(nodes[0].key),"distinct actual full keys have identical hash");check(table.insert(&nodes[i]),"actual same-hash chain admission");}\n for(size_t i=0;i<nodes.size();++i){size_t seen=0;auto count=table.remove_by_key_limit(nodes[i].key,1,[&](Node*n){check(n==&nodes[i],"hash collision does not select different owner/word");++seen;return true;});check(count==1&&seen==1&&table.size()==nodes.size()-i-1,"actual exact-key removal leaves collision neighbors");}\n check(!equal({futex::FutexKeyKind::PRIVATE,0,0},{futex::FutexKeyKind::SHARED_FRAME,0,0}),"full key tag distinguishes otherwise identical words");\n check(!equal({futex::FutexKeyKind::PRIVATE,1,2},{futex::FutexKeyKind::PRIVATE,2,2}),"full key owner identity compared");\n check(!equal({futex::FutexKeyKind::PRIVATE,1,2},{futex::FutexKeyKind::PRIVATE,1,3}),"full key word identity compared");\n#else\n check(false,"archived whole futex has no typed full-key collision policy");\n#endif\n}\nvoid all_drained(){check(futex::futex_table.size()==0,"actual waiter table drained");check(stats().active_roots==0&&stats().control_pages==0,"actual root control metadata fully retired");check(metadata_allocations==metadata_frees,"all grown/discarded actual metadata chunks freed");for(size_t i=0;i<frames.size();++i)check(allocator.page_refcounts[i]==0&&frames[i].available,"all data-frame owners balanced");for(auto&s:tasks)check(s==nullptr,"all Host subjects detached");check(irq_depth==0&&access_depth==0,"all IRQ/access scopes restored");free_pool();check(raw_pages.empty(),"all declared physical allocator seam allocations freed");}\n}\nint main(int argc,char**argv){if(argc!=2)return 3;fixture::init_kernel_root();std::string_view v=argv[1];\n#define RUN(name) if(v==#name)fixture::name();else\nRUN(roots_4097)RUN(roots_1000)RUN(pool_reuse)RUN(metadata_oom)RUN(root_oom)RUN(serial_exhaustion)RUN(concurrent_growth)RUN(capture_bounds)RUN(capture_busy)RUN(capture_unknown)RUN(capture_revalidate)RUN(private_roots)RUN(private_aliases)RUN(private_cow)RUN(shared_roots)RUN(shared_offsets)RUN(phys_private_refusal)RUN(root_reuse_isolation)RUN(same_root_tasks)RUN(wake_cleanup_races)RUN(kernel_release)RUN(duplicate_admission)RUN(key_collisions)\nif(v=="table_first")fixture::waiter_order(false);else if(v=="slot_first")fixture::waiter_order(true);else if(v=="final_ref_guard")fixture::final_owner(false);else if(v=="final_kind_guard")fixture::final_owner(true);else return 3;\n#undef RUN\nfixture::all_drained();std::printf("%s vector=%s assertions=%u failures=%u real_root_control=1 real_key_capture=1 real_futex=1 real_hash=1 real_frame_owner=1\\n",fixture::failures==0?"PASS":"FAIL",argv[1],fixture::assertions.load(),fixture::failures.load());return fixture::failures==0?0:3;}\n'

# These are actual-helper endpoint/negative cases. All original CASES bytes and
# 27 vector bodies remain unchanged; the generator adds only these functions
# and two dispatch entries. Positive managed mapping is covered separately by
# the registered managed_huge_runtime fixture, not modeled here.
MANAGED_CASES = r"""
void resolver_unknown_ps() {
    OwnedRoot root;
    FrameOwner frame;
    map_leaf(root.root, WORD, frame.frame, false);
    auto* table = root.root;
    for (int level = 4; level >= 2; --level) {
        auto& entry = table->entries[(WORD >> (12 + 9 * (level - 1))) & 511];
        if (level == 3 || level == 2) {
            uint64_t before = virt::pte_raw(entry);
            uint32_t const refs_before = refs(frame.frame);
            auto const flags = virt::user_mapping_pin_lock.lock_irqsave();
            // A plausible PS word is still unauthorised: this exact registry
            // is genuinely unready/empty, never replaced by a success seam.
            entry = virt::pte_from_raw(0x200000ULL | paging::PAGE_PRESENT |
                                      paging::PAGE_USER | paging::PAGE_WRITE | 0x80);
            virt::user_mapping_pin_lock.unlock_irqrestore(flags);
            virt::UserLeafResolution resolved{};
            auto const inspect = virt::user_mapping_pin_lock.lock_irqsave();
            bool const resolved_ps = virt::resolve_user_leaf_locked(root.root, WORD, false, resolved);
            virt::user_mapping_pin_lock.unlock_irqrestore(inspect);
            check(!resolved_ps && resolved.kind == virt::UserLeafKind::Unsupported,
                  "actual scalar resolver refuses PS before allocator pin can mask a provenance defect");
            virt::UserPagePin pin{};
            bool const accepted = virt::pin_user_page(root.root, WORD, false, pin, false);
            check(!accepted && pin.hhdm_page == nullptr && refs(frame.frame) == refs_before,
                  level == 3 ? "actual resolver rejects 1GiB before frame retain"
                             : "actual resolver rejects PS without registry provenance");
            auto const restore = virt::user_mapping_pin_lock.lock_irqsave();
            entry = virt::pte_from_raw(before);
            virt::user_mapping_pin_lock.unlock_irqrestore(restore);
            if (accepted) virt::unpin_user_page(pin);
        }
        if (level > 2) table = reinterpret_cast<paging::PageTable*>(
            mm::addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << 12));
    }
    virt::UserPagePin ordinary{};
    check(virt::pin_user_page(root.root, WORD, true, ordinary, false),
          "ordinary 4KiB translation remains pinnable after PS refusals");
    virt::unpin_user_page(ordinary);
}
void current_leaf_write_commit() {
    OwnedRoot root;
    FrameOwner original, replacement;
    map_leaf(root.root, WORD, original.frame, false);
    virt::UserPagePin pin{};
    check(virt::pin_user_page(root.root, WORD, true, pin, false), "actual writable pin admitted");
    check(virt::user_page_pin_commit_write(pin) && leaf(root.root, WORD, false).dirty,
          "actual current leaf atomic dirty commit");
    map_leaf(root.root, WORD, replacement.frame, false);
    check(!virt::user_page_pin_commit_write(pin) && !leaf(root.root, WORD, false).dirty,
          "stale frame cannot dirty a replacement slot");
    map_leaf(root.root, WORD, original.frame, false);
    set_leaf_flags(root.root, WORD, paging::PAGE_PRESENT | paging::PAGE_USER |
                   paging::PAGE_WRITE | paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED);
    check(!virt::user_page_pin_commit_write(pin) && !leaf(root.root, WORD, false).dirty,
          "actual final shared leaf maximum write ceiling precedes dirty commit");
    set_leaf_flags(root.root, WORD, paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE);
    check(virt::user_page_pin_commit_write(pin) && leaf(root.root, WORD, false).dirty,
          "restored same mapping commits through current resolver");
    virt::unpin_user_page(pin);
}
"""

def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

ROOT_RESERVE_SEAMS = '\n// Explicit compatibility seam: these existing futex vectors have no published\n// topology/emergency reserve. They test the ordinary factory/control path.\nnamespace fixture { inline thread_local unsigned locality_capture_depth{}; }\nnamespace ker::mod::mm::numa {\nusing NodeId=uint16_t;\ninline constexpr NodeId INVALID_NODE=UINT16_MAX;\nnamespace root_reserve {\nenum class Error:uint8_t {NONE,NOT_READY,NO_TOPOLOGY,ALREADY_INITIALIZED,INVALID_NODE,EMPTY,CORRUPT};\nstruct TakeResult {paging::PageTable* table{};Error error{Error::NOT_READY};};\nenum class ReturnResult:uint8_t {NOT_MEMBER,RETURNED,CORRUPT};\ninline auto try_take(NodeId)->TakeResult {\n fixture::check(false,"no published reserve in futex compatibility fixture");return {};\n}\ninline auto try_return(paging::PageTable*,void(*)(paging::PageTable*))->ReturnResult {\n fixture::check(fixture::locality_capture_depth==0,"return outside locality capture");return ReturnResult::NOT_MEMBER;\n}\n}\n}\nnamespace ker::mod::acpi::numa {\nstruct Topology {uint16_t cpu_count{},node_count{};std::array<uint16_t,256>cpu_nodes{};};\ninline auto boot_topology()->const Topology* {return nullptr;}\n}\nnamespace ker::mod::cpu {\ninline auto get_current_cpu_id_safe()->uint64_t {\n fixture::check(fixture::locality_capture_depth==1,"CPU capture under short preemption guard");return 0;\n}\n}\nnamespace ker::mod::sched {\nstruct PreemptGuard {\n PreemptGuard(){fixture::check(fixture::irq_depth==0,"locality capture outside MM IRQ locks");++fixture::locality_capture_depth;}\n ~PreemptGuard(){fixture::check(fixture::locality_capture_depth==1,"balanced locality capture");--fixture::locality_capture_depth;}\n};\n}\nnamespace ker::mod::dbg {[[noreturn]]inline void panic_handler(const char*){throw fixture::InvariantStop{};}}\n'


def capture(source: str, anchor: str, *, declaration: bool = False) -> tuple[str, dict]:
    """Capture one complete actual definition, retaining every original byte."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda match: " " * len(match[0]), source)
    definitions = []
    for match in re.finditer(re.escape(anchor), clean):
        parentheses = 0
        for index in range(match.start(), len(clean)):
            char = clean[index]
            if char == "(": parentheses += 1
            elif char == ")": parentheses -= 1
            elif char == ";" and parentheses == 0:
                break  # A prototype cannot borrow a later function's body.
            elif char == "{" and parentheses == 0:
                definitions.append((match.start(), index))
                break
    if len(definitions) != 1:
        raise ValueError(f"expected one actual definition for {anchor}, found {len(definitions)}")
    definition, opening = definitions[0]
    start = clean.rfind("\n", 0, definition) + 1
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

def generate(scratch: Path, selected: Path) -> list[dict]:
    include=scratch/"include"; actual=[]
    def write(name,text):
        destination=include/name;destination.parent.mkdir(parents=True,exist_ok=True);destination.write_text(text)
    def definition(source,anchor,declaration=False):
        body,row=capture(source,anchor,declaration=declaration);actual.append(row)
        return f'#line {row["line"]} "actual-extracted-source"\n'+body
    def span(source,start,end,label):
        begin=source.index(start);finish=source.index(end,begin);body=source[begin:finish]
        actual.append({"name":label,"line":source.count("\n",0,begin)+1,"bytes":len(body.encode()),"sha256":hashlib.sha256(body.encode()).hexdigest()})
        return body
    paging=(KERN/"platform/mm/paging.hpp").read_bytes();write("platform/mm/paging.hpp",paging.decode());write("defines/defines.hpp","#pragma once\n")
    actual.append({"name":"whole-platform/mm/paging.hpp","bytes":len(paging),"sha256":hashlib.sha256(paging).hexdigest()})
    phys_h=(KERN/"platform/mm/phys.hpp").read_text();owner_h=(KERN/"platform/mm/page_alloc.hpp").read_text()
    common=COMMON+ROOT_RESERVE_SEAMS+'\nnamespace ker::mod::mm {\n'+definition(owner_h,"enum class PhysicalPageOwner",True)+'\n'+definition(owner_h,"enum class PageKind",True)+'\n}\n'
    common+='namespace ker::mod::mm::phys {\n'+definition(phys_h,"enum class PageLookupOwner",True)+'\n'+definition(phys_h,"struct PageLookupHint",True)+'\n}\n'+PHYS_TAIL+RAW_PHYS
    core_header=(KERN/"platform/mm/managed_huge_core.hpp").read_bytes()
    write("platform/mm/managed_huge_core.hpp",core_header.decode())
    actual.append({"name":"whole-platform/mm/managed_huge_core.hpp","bytes":len(core_header),
                   "sha256":hashlib.sha256(core_header).hexdigest()})
    common+='\n#include <platform/mm/managed_huge_core.hpp>\n'
    write("fixture-common.hpp",common)
    vh=(KERN/"platform/mm/virt.hpp").read_text()
    declarations='\n'.join(definition(vh,a,True) for a in ["struct UserPagePin {","struct UserFutexIdentity {","struct UserRootControlStats {"])
    write("platform/mm/virt.hpp",'#pragma once\n#include "fixture-common.hpp"\n#include <platform/mm/file_mapping_lifetime.hpp>\nnamespace ker::mod::sched::task {struct Task;}\nnamespace ker::mod::mm::virt { using PageTable=paging::PageTable; using PageTableEntry=paging::PageTableEntry; inline constexpr paddr_t PADDR_INVALID=UINT64_MAX;\n'+declarations+'''
    auto pin_user_page(PageTable*,vaddr_t,bool,UserPagePin&,bool) -> bool;
    auto pin_user_page_for_task(sched::task::Task*,PageTable*,vaddr_t,bool,bool,UserPagePin&) -> bool;
    auto user_page_pin_still_mapped(const UserPagePin&) -> bool;
    auto user_page_pin_commit_write(const UserPagePin&) -> bool;
    auto capture_user_futex_identity(const UserPagePin&,vaddr_t,UserFutexIdentity&) -> bool;
    auto user_futex_identity_still_mapped(const UserPagePin&,const UserFutexIdentity&) -> bool;
    void unpin_user_page(UserPagePin&);
    // These vectors have no typed public lazy rows; demand preparation is an
    // explicit untracked boundary, not a modeled backend or native claim.
    enum class PublicReadPrepareResult { NOT_TRACKED, READY, REFUSED };
    inline auto prepare_tmpfs_read_shared_page(sched::task::Task&,uint64_t,bool) -> PublicReadPrepareResult {
        return PublicReadPrepareResult::NOT_TRACKED;
    }
    bool ensure_user_page_mapped(sched::task::Task*,vaddr_t);
    bool ensure_user_page_writable(sched::task::Task*,vaddr_t);
    }\n''')
    write("platform/sched/task.hpp",TASK);write("platform/sched/scheduler.hpp",SCHEDULER)
    for name in ["platform/sys/spinlock.hpp","platform/mm/phys.hpp"]:write(name,'#pragma once\n#include "fixture-common.hpp"\n')
    write("platform/asm/cpu.hpp","#pragma once\n");write("platform/mm/dyn/kmalloc.hpp","#pragma once\n");write("platform/ktime/ktime.hpp",'#pragma once\n#include "fixture-common.hpp"\nnamespace ker::mod::time{inline uint64_t get_us(){return fixture::now_us;}}\n')
    for name in ["platform/sys/usercopy.hpp","platform/mm/file_mapping_lifetime.hpp","util/hashtable.hpp","abi/callnums/futex.h","syscalls_impl/futex/futex.hpp"]:
        data=(KERN/name).read_bytes();write(name,data.decode());actual.append({"name":"whole-"+name,"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()})
    (scratch/"futex.hpp").write_bytes((KERN/"syscalls_impl/futex/futex.hpp").read_bytes());(scratch/"actual-futex.cpp").write_bytes(selected.read_bytes())
    actual.append({"name":"whole-selected-futex.cpp",**pin(selected)})
    source=(KERN/"platform/mm/virt.opt.cpp").read_text()
    root_globals=span(source,'constexpr size_t USER_ROOT_CONTROL_BUCKET_COUNT','enum class AnonymousSwapRuntimeState','whole-root-control-declarations-and-globals')
    gate_globals=span(source,'constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT','[[nodiscard]] auto user_pagemap_access_gate_index','whole-root-reader-gate-declarations-and-globals')
    pool_globals=span(source,'constexpr size_t PAGE_TABLE_POOL_CAPACITY','constexpr size_t OWNED_FRAME_TABLE_CAPACITY','whole-page-table-pool-declarations-and-globals')
    private_anchors=['[[nodiscard]] auto user_pagemap_access_gate_index','void service_user_pagemap_gate_wait','[[nodiscard]] auto acquire_user_pagemap_access','void release_user_pagemap_access','[[nodiscard]] auto begin_user_pagemap_exclusive','void end_user_pagemap_exclusive','class UserPagemapExclusiveScope {','void zero_page_table_for_pool','auto try_alloc_page_table_from_pool','auto try_release_page_table_to_pool','auto alloc_zeroed_page_table','auto user_root_control_bucket','auto find_user_root_control_locked','auto next_user_root_serial','enum class UserRootAttachResult','auto attach_user_root_control_locked','auto allocate_user_root_control_chunk','struct RootReserveLocality {','auto capture_root_reserve_locality','auto return_root_to_emergency_reserve','void release_unregistered_user_root','auto create_user_root_common','auto retire_user_root_control_locked','auto pte_raw(const paging::PageTableEntry& e)','auto pte_from_raw(uint64_t raw)','auto index_of(','auto entry_at(PageTable*','auto entry_at(const PageTable*','class MutableUserEntrySlot {','enum class UserLeafKind','struct UserLeafResolution {','auto resolve_user_leaf_locked(','auto snapshot_user_page_mapping_locked','auto try_pin_current_user_page_mapping','auto pin_user_page_common']
    private='\n'.join(definition(source,a,a.startswith(('class ','enum ','struct '))) for a in private_anchors)
    private+='\n'+definition(source,'void release_pagemap_exclusive_owned(')
    # No native file bindings are installed by these root/futex vectors. Compile
    # the actual empty-index paths and their complete checked binding-owner
    # dependencies; never replace the production index with permissive stubs.
    core=(KERN/"platform/mm/file_mapping_lifetime.cpp").read_text()
    owner_helpers=span(core,'constinit sys::Spinlock owner_count_lock{};',
                       'template <typename Counts, typename Role>\nauto reclassify_owner',
                       'actual-permanent-owner-guard-and-checked-acquire-release-templates')
    core_anchors=['auto FileMappingBinding::try_acquire(', 'void FileMappingBinding::release(',
                  'auto FileMappingIndex::bucket(', 'auto FileMappingIndex::root_matches_locked(',
                  'auto FileMappingIndex::find_locked(', 'auto FileMappingIndex::root_empty_locked(',
                  'auto FileMappingIndex::retain_for_pin_locked(', 'auto FileMappingIndex::matches_pin_locked(']
    core_bodies='\n'.join(definition(core,a) for a in core_anchors)
    core_unit='#include <platform/mm/file_mapping_lifetime.hpp>\nnamespace ker::mod::mm::file_mapping { namespace {\n'+owner_helpers+'\n}\n'+core_bodies+'\n}\n'
    public_anchors=['auto create_pagemap()','auto create_pagemap_may_fail()','void release_pagemap(','void get_user_root_control_stats_snapshot','auto selftest_user_root_incarnation(','auto pin_user_page(','auto pin_user_page_for_task(','auto user_page_pin_still_mapped(','auto user_page_pin_commit_write(','auto capture_user_futex_identity(','auto user_futex_identity_still_mapped(','void unpin_user_page(']
    public='\n'.join(definition(source,a) for a in public_anchors)
    mm=('#include "fixture-common.hpp"\n#include <platform/mm/virt.hpp>\n#include <platform/sched/scheduler.hpp>\n'+core_unit+'\nnamespace ker::mod::mm::virt { namespace {\nusing log=HostLog; using namespace paging; PageTable* kernel_pagemap{}; sys::Spinlock user_mapping_pin_lock; file_mapping::FileMappingIndex file_mapping_index;\ninline uint64_t fake_cr3{}; inline uint64_t rdcr3(){return fake_cr3;} inline void wrcr3(uint64_t x){fake_cr3=x;} inline bool is_current_pagemap(PageTable*){return false;} inline void service_pending_tlb_shootdowns(){} inline void owned_frame_purge_pagemap(PageTable*){}\n'+root_globals+gate_globals+pool_globals+
        span(source,'constexpr uint64_t PTE_FRAME_MASK =','\n','actual-physical-entry-mask')+'\n'+
        span(source,'constinit huge::RegistryCore managed_huge_registry{};','\n','actual-empty-managed-huge-registry')+'\n'+private+'\n}\n'+public+'\n}\n')
    (scratch/"actual-mm.cpp").write_text(mm)
    user=(KERN/"platform/sys/usercopy.cpp").read_text()
    user_anchors=["UserPageFrameRef::~UserPageFrameRef()","UserPageFrameRef::UserPageFrameRef(UserPageFrameRef&&","UserPageFrameRef::operator=(UserPageFrameRef&&","UserPageFrameRef::valid()","UserPageFrameRef::reset()","StableUserPage::~StableUserPage()","StableUserPage::valid()","StableUserPage::kernel_address()","StableUserPage::physical_address()","StableUserPage::was_dirty()","StableUserPage::still_mapped()","StableUserPage::commit_write()","StableUserPage::retain_frame(UserPageFrameRef&","StableUserPage::capture_futex_identity(","StableUserPage::futex_identity_still_mapped(","StableUserPage::reset()","auto pin_task_user_page(","auto range_valid("]
    user_bodies='\n'.join(definition(user,a) for a in user_anchors)
    phys_source=(KERN/"platform/mm/phys.opt.cpp").read_text();increment=definition(phys_source,"auto page_ref_try_inc(void* page, PageLookupHint* hint)")
    cases=CASES.replace('void all_drained(){',MANAGED_CASES+'\nvoid all_drained(){')
    cases=cases.replace('RUN(roots_4097)','RUN(resolver_unknown_ps)RUN(current_leaf_write_commit)RUN(roots_4097)',1)
    text=('#define FIXTURE_TYPED_FUTEX_KEYS 1\n' if 'enum class FutexKeyKind' in selected.read_text() else '')+'#include "fixture-common.hpp"\n#include <platform/sys/usercopy.hpp>\n#include <platform/sched/scheduler.hpp>\nnamespace ker::mod::mm::phys{\n'+increment+'\n}\n#include "actual-mm.cpp"\n'+MAP_SEAMS+'\nnamespace ker::mod::sys::usercopy{\n'+user_bodies+'\n}\n#include "actual-futex.cpp"\n'+cases
    (scratch/"harness.cpp").write_text(text)
    return actual

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
                    "platform/mm/phys.opt.cpp", "platform/mm/page_alloc.hpp", "platform/mm/paging.hpp",
                    "platform/mm/virt.hpp", "platform/mm/virt.opt.cpp", "platform/mm/managed_huge_core.hpp", "platform/mm/file_mapping_lifetime.hpp",
                    "platform/mm/file_mapping_lifetime.cpp", "util/hashtable.hpp",
                    "abi/callnums/futex.h", "syscalls_impl/futex/futex.hpp"]]]
        before = [pin(p) for p in watched]
        result["input_pins"] = before
        result["futex_source"] = before[0]
        result["root_transactions_not_full_goal_lifecycles"] = True
        result["primitive_seams"] = ["raw physical allocator", "CPU/IRQ", "Task/scheduler", "fixture PTE publication", "page-fault admission", "no published topology/emergency reserve; ordinary factory compatibility only", "actual default-unready managed RegistryCore: no managed PS admission; unknown2MiB/1GiB refusal only"]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-futex-root-", dir="/tmp"))
        result["scratch"] = str(scratch)
        result["actual_definitions"] = generate(scratch, selected)
        staged = sorted(path for path in scratch.rglob("*") if path.is_file())
        staged_before = [pin(path) for path in staged]
        result["actual_staged_sources_before"] = staged_before
        source, binary, depfile = scratch / "harness.cpp", scratch / "futex-root-test", scratch / "dependencies.d"
        argv = [str(compiler), "-std=c++23", "-O1", "-g0", "-pipe", "-pthread", "-fsanitize=address,undefined",
                "-fno-omit-frame-pointer", "-fno-strict-aliasing", "-MD", "-MF", str(depfile), "-I", str(scratch / "include"),
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
                if not re.search(rf"^PASS vector={expected} assertions=\d+ failures=0 real_root_control=1 real_key_capture=1 real_futex=1 real_hash=1 real_frame_owner=1$", log, re.M):
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
    raise SystemExit(main())
