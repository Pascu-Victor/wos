#!/usr/bin/env python3
"""Compile complete TMPFS page ownership bodies against bounded Host primitives.

Actual node/page/mount declarations, SmallVec, content/coherency declarations,
allocation/materialization, accounting, registry, release, eviction, migration,
bounded/public scan and final node-free bodies are compiled without rewriting
their bytes. Physical frame allocation/refcounts, swap and mutex operations are
named Host seams with independent byte/reference/quota/lifetime oracles. This
does not qualify real MM aliases, devices, concurrency or native coherence.
Actual write/append/copy EOF guards and complete NodeRef acquisition/cleanup
bodies are also compiled. Owned Host inode EOF/counter-limit inputs exercise
those guards. Demand backing/loan bodies are compiled against an explicit Host
File and checked mount-pin primitive; native mount graphs/MM remain separate.

The bounded process/scratch runner is shared with the actual file-registry
regression. --tmpfs-source accepts a complete archived predecessor; --only
selects one genuine compiled vector for an independent negative control.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
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
SOURCE = KERN / "vfs/fs/tmpfs.cpp"
HEADER = KERN / "vfs/fs/tmpfs.hpp"
RUNNER = Path(__file__).with_name("file_mmap_registry_runtime_test.py")
RUNNER_BYTES = RUNNER.read_bytes()
SPEC = importlib.util.spec_from_file_location("wos_tmpfs_bounded_host_runner", RUNNER)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("bounded actual file-registry Host runner is unavailable")
runner = importlib.util.module_from_spec(SPEC)
# Execute the exact captured complete runner bytes, avoiding a separate loader
# read that could diverge from the evidence of the reused implementation.
exec(compile(RUNNER_BYTES, str(RUNNER), "exec"), runner.__dict__)

VECTORS = (
    "materialize_owner", "retained_release", "hole_quota", "uncharged_release",
    "descriptor_relocation", "exclusive_eviction", "borrowed_eviction",
    "allocate_slot_failure", "write_slot_failure", "rehydration",
    "read_slot_failure", "free_slot_failure", "physical_allocation_failure",
    "node_scan_budget", "global_scan_budget", "lookup_deferred_free",
    "guard_before_owner_destruction", "aliases_bounded_skip_unread_target",
    "aliases_public_skip_unread_target", "aliases_migration_skip_unread_target",
    "resize_array_oom", "resize_port_resident_oom", "resize_port_swapped_oom",
    "resize_port_hole_oom", "resize_port_grow_oom", "resize_swap_read_failure",
    "resize_swap_success", "resize_nonallocating", "resize_admission",
    "eof_write_guard", "eof_resize_guard", "eof_copy_guard",
    "checked_node_pins", "checked_alias_pins", "node_pin_real_deferred_free",
    "checked_lookup_transactions",
    "descriptor_growth_oom", "descriptor_growth_overflow",
    "demand_cold_extent", "demand_resident_loans", "demand_sparse_high_offset",
    "demand_swapped_page", "demand_page_failures", "demand_admission_failures",
    "demand_fixed_eof_guards", "demand_unlinked_cleanup",
    "shared_ordinary_admission", "shared_independent_writes", "shared_tail_nontail",
    "shared_multiple_tail", "shared_tail_swapped", "shared_tail_failures",
    "shared_counter_limits", "shared_access_refusals", "shared_sync_metadata",
    "shared_reverse_exec", "shared_file_cache_authority", "shared_lookup_cache_authority",
    "shared_unlinked_tail_cleanup",
    "shared_staging_metadata_rollback",
)

LOCKS = r'''
#pragma once
#include <cstdint>
#include <map>
#include <memory>
namespace fixture {
inline std::map<uintptr_t, bool> locks;
inline uint64_t destroy_held{}, invalid_unlock{}, lock_errors{}, try_attempts{};
}
namespace ker::mod::sys {
class Spinlock {
public:
    Spinlock() { if (!fixture::locks.emplace(reinterpret_cast<uintptr_t>(this), false).second) ++fixture::lock_errors; }
    ~Spinlock() {
        auto it = fixture::locks.find(reinterpret_cast<uintptr_t>(this));
        if (it == fixture::locks.end()) { ++fixture::lock_errors; return; }
        if (it->second) ++fixture::destroy_held;
        fixture::locks.erase(it);
    }
    Spinlock(const Spinlock&) = delete;
    auto operator=(const Spinlock&) -> Spinlock& = delete;
    void lock() {
        auto it = fixture::locks.find(reinterpret_cast<uintptr_t>(this));
        if (it == fixture::locks.end() || it->second) { ++fixture::lock_errors; return; }
        it->second = true;
    }
    auto try_lock() -> bool {
        ++fixture::try_attempts;
        auto it = fixture::locks.find(reinterpret_cast<uintptr_t>(this));
        if (it == fixture::locks.end()) { ++fixture::lock_errors; return false; }
        if (it->second) return false;
        it->second = true;
        return true;
    }
    void unlock() {
        // External lifetime authority detects an unlock after owner deletion
        // without dereferencing the dead mutex's storage in this Host seam.
        auto it = fixture::locks.find(reinterpret_cast<uintptr_t>(this));
        if (it == fixture::locks.end()) { ++fixture::invalid_unlock; return; }
        if (!it->second) { ++fixture::lock_errors; return; }
        it->second = false;
    }
};
class Mutex : public Spinlock {};
class MutexGuard {
    Mutex& mutex_;
public:
    explicit MutexGuard(Mutex& mutex) : mutex_(mutex) { mutex_.lock(); }
    ~MutexGuard() { mutex_.unlock(); }
    MutexGuard(const MutexGuard&) = delete;
    auto operator=(const MutexGuard&) -> MutexGuard& = delete;
};
}
'''

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <new>
#include <string_view>
#include <sys/mman.h>
#include <utility>
#include <unistd.h>
#include <util/smallvec.hpp>
#include <platform/sched/file_growth.hpp>
#include <vfs/exec_content.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/native_coherency.hpp>
#include "actual-types.hpp"

namespace fixture {
constexpr size_t BS = 4096;
const char* current_case = "initialization";
uint64_t assertions{}, failures{}, allocations{}, frees{}, increments{}, decrements{}, direct_free_refused{};
uint64_t slot_allocations{}, slot_frees{}, slot_writes{}, slot_reads{};
bool allocation_failure{}, available = true;
bool ref_failure{}, backing_alloc_failure{}, host_interrupts_enabled = true;
uint64_t backing_allocation_faults{};
uint32_t host_preempt_count{};
uint64_t mount_pins{}, mount_acquires{}, mount_releases{};
int mount_acquire_error{};
int slot_allocate_error{}, slot_write_error{}, slot_read_error{}, slot_free_error{};
struct Frame { uint32_t refs{1}; };
std::map<void*, Frame> frames;
struct Slot { ker::mod::mm::swap::SwapSlot id{}; std::array<unsigned char, BS> bytes{}; };
std::map<uint64_t, Slot> slots;
uint64_t next_slot{};
std::map<void*, size_t> arrays;
uint64_t array_allocations{}, array_frees{}, array_faults{}, predecessor_throws{};
size_t fail_array_bytes{};
bool fail_array_armed{};
uint64_t epoch_ns = 1000000000ULL, fsize_limit = UINT64_MAX;
uint64_t growth_commits{}, growth_denials{}, last_committed_size{};
uintptr_t growth_inode_lock{};
void check(bool value, const char* expression, int line) {
    ++assertions;
    if (!value) {
        if (failures < 64) std::fprintf(stderr, "ASSERT case=%s line=%d expression=%s\n", current_case, line, expression);
        ++failures;
    }
}
#define CHECK(expression) fixture::check(bool(expression), #expression, __LINE__)
void verify_bytes(const void* page, unsigned char seed) {
    CHECK(frames.contains(const_cast<void*>(page)));
    if (!frames.contains(const_cast<void*>(page))) return;
    auto* data = static_cast<const unsigned char*>(page);
    for (size_t i = 0; i < BS; ++i) CHECK(data[i] == static_cast<unsigned char>(seed + i * 17U));
}
void fill_bytes(void* page, unsigned char seed) {
    auto* data = static_cast<unsigned char*>(page);
    for (size_t i = 0; i < BS; ++i) data[i] = static_cast<unsigned char>(seed + i * 17U);
}
auto refs(void* page) -> uint32_t { auto it = frames.find(page); return it == frames.end() ? 0 : it->second.refs; }
}

// Complete actual descriptor allocation sites use these explicit Host array
// primitives. Throwing old new[] fails with defined hosted C++ semantics;
// nothrow new[] returns null. Ledger changes are independent of TMPFS code.
auto operator new[](size_t bytes) -> void* {
    // Refuse malformed predecessor extents within this fixture's 64MiB budget
    // before hosted malloc/ASan can terminate without the cleanup oracles.
    if(bytes>64U*1024U*1024U)throw std::bad_alloc();
    if (fixture::fail_array_armed && fixture::fail_array_bytes == bytes) {
        fixture::fail_array_armed = false;
        ++fixture::array_faults;
        throw std::bad_alloc();
    }
    void* memory = std::malloc(bytes == 0 ? 1 : bytes);
    if (memory == nullptr) throw std::bad_alloc();
    CHECK(fixture::arrays.emplace(memory, bytes).second);
    ++fixture::array_allocations;
    return memory;
}
void operator delete[](void* memory) noexcept {
    if (memory == nullptr) return;
    CHECK(fixture::arrays.erase(memory) == 1);
    ++fixture::array_frees;
    std::free(memory);
}
void operator delete[](void* memory, size_t) noexcept { ::operator delete[](memory); }
auto operator new[](size_t bytes, const std::nothrow_t&) noexcept -> void* {
    try { return ::operator new[](bytes); } catch (const std::bad_alloc&) { return nullptr; }
}
void operator delete[](void* memory, const std::nothrow_t&) noexcept { ::operator delete[](memory); }

// Only the actual backing's nothrow scalar allocation has a scoped Host fault.
auto operator new(size_t bytes, const std::nothrow_t&) noexcept -> void* {
    if(fixture::backing_alloc_failure && bytes==sizeof(ker::vfs::tmpfs::TmpfsDemandSharedBacking)) {
        fixture::backing_alloc_failure=false;++fixture::backing_allocation_faults;return nullptr;
    }
    try { return ::operator new(bytes); } catch(const std::bad_alloc&) { return nullptr; }
}
void operator delete(void* memory,const std::nothrow_t&) noexcept { ::operator delete(memory); }

// Host File identity/table and FileGrowth ownership are primitive boundaries.
// Real File layout, Task credentials/resource ownership and native close are
// not qualified by this facade. Native tests use actual retained Files.
namespace ker::vfs {
struct MountPoint { const void* private_data{}; uint32_t pins{}; bool require_frames_empty{}; };
struct HostFileCount {
    int value{1};
    auto load(std::memory_order = std::memory_order_seq_cst) const -> int {return value;}
    void store(int next,std::memory_order = std::memory_order_seq_cst){value=next;}
    auto operator=(int next)->HostFileCount&{value=next;return *this;}
};
struct File {
    void* private_data{}; FileOperations* fops{}; int open_flags{2}; FSType fs_type{FSType::TMPFS};
    HostFileCount refcount{}; bool is_directory{}; MountPoint* mount_owner{};
    std::unique_ptr<ExecReadView> exec_read_view{}; void* stream_cache_attachment{}; void* cache_notify_attachment{};
    MountPolicy mount_policy{};uint32_t mount_dev_id{};int fd{-1};
};
namespace tmpfs { auto get_tmpfs_fops() -> FileOperations*; }
// Named checked mount-pin primitive, not the actual mount table/graph validator.
NativeFileMappingMountPin::~NativeFileMappingMountPin() { reset(); }
NativeFileMappingMountPin::NativeFileMappingMountPin(NativeFileMappingMountPin&& other) noexcept
    : mount_(std::exchange(other.mount_,nullptr)) {}
auto NativeFileMappingMountPin::operator=(NativeFileMappingMountPin&& other) noexcept -> NativeFileMappingMountPin& {
    if(this!=&other){reset();mount_=std::exchange(other.mount_,nullptr);}return *this;
}
auto NativeFileMappingMountPin::acquire_tmpfs(File& file,const void* context,NativeFileMappingMountPin& out)->int {
    for(auto [_,held]:fixture::locks)CHECK(!held);
    ++fixture::mount_acquires;
    if(fixture::mount_acquire_error)return fixture::mount_acquire_error;
    if(!out.empty() || file.mount_owner==nullptr || file.mount_owner->private_data!=context)return -EOPNOTSUPP;
    if(file.mount_owner->pins==UINT32_MAX)return -EOVERFLOW;
    ++file.mount_owner->pins;++fixture::mount_pins;out.mount_=file.mount_owner;return 0;
}
auto NativeFileMappingMountPin::acquire_tmpfs_demand(File& file,const void* context,NativeFileMappingMountPin& out)->int {
    return acquire_tmpfs(file,context,out);
}
void NativeFileMappingMountPin::reset() noexcept {
    if(mount_==nullptr)return;
    for(auto [_,held]:fixture::locks)CHECK(!held);
    if(mount_->require_frames_empty)CHECK(fixture::frames.empty());
    CHECK(mount_->pins>0 && fixture::mount_pins>0);
    --mount_->pins;--fixture::mount_pins;++fixture::mount_releases;mount_=nullptr;
}
}
namespace ker::mod::sched {
auto interrupts_enabled()->bool{return fixture::host_interrupts_enabled;}
auto preempt_count()->uint32_t{return fixture::host_preempt_count;}
}
// This test isolates pin transactions after search authorization. Credential
// policy is an explicit Host primitive; production FileIdentity is imported.
namespace ker::mod::security {
class Snapshot {};
auto authorize_file_access(const Snapshot&,const FileIdentity& object,int access) -> int {
    CHECK((object.mode & 0170000U)==0040000U && access==1);
    return 0;
}
}
namespace ker::mod::time { auto get_epoch_ns() -> uint64_t { return ++fixture::epoch_ns; } }
namespace ker::mod::sched::resource {
FileGrowth::FileGrowth() : m_caller(nullptr), m_limit(fixture::fsize_limit) {}
FileGrowth::~FileGrowth() {
    if (fixture::growth_inode_lock != 0) {
        auto it = fixture::locks.find(fixture::growth_inode_lock);
        CHECK(it != fixture::locks.end() && !it->second);
    }
    if (m_committed) { ++fixture::growth_commits; fixture::last_committed_size = m_size; }
    if (m_denied) ++fixture::growth_denials;
}
}

// Named primitive boundaries only. The complete TMPFS functions follow below.
namespace ker::mod::mm::phys {
auto get_total_mem_bytes() -> uint64_t { return 0; }
auto page_alloc_can_satisfy(uint64_t, uint64_t) -> bool { return true; }
auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t size, std::string_view) -> void* {
    CHECK(owner == PhysicalPageOwner::TMPFS_DATA && size == fixture::BS);
    if (fixture::allocation_failure) return nullptr;
    void* p = std::aligned_alloc(fixture::BS, fixture::BS);
    CHECK(p != nullptr);
    if (p == nullptr) return nullptr;
    CHECK(fixture::frames.emplace(p, fixture::Frame{}).second);
    ++fixture::allocations;
    std::memset(p, 0xa5, fixture::BS);
    return p;
}
auto page_ref_get(void* p) -> uint32_t { return fixture::refs(p); }
auto page_ref_try_inc(void* p) -> bool {
    auto it=fixture::frames.find(p);
    if(fixture::ref_failure || it==fixture::frames.end() || it->second.refs==0 || it->second.refs==UINT32_MAX)return false;
    ++it->second.refs;++fixture::increments;return true;
}
void page_ref_inc(void* p) {
    auto it = fixture::frames.find(p);
    CHECK(it != fixture::frames.end() && it->second.refs != 0);
    if (it != fixture::frames.end()) { ++it->second.refs; ++fixture::increments; }
}
auto page_ref_dec(void* p) -> uint32_t {
    auto it = fixture::frames.find(p);
    CHECK(it != fixture::frames.end() && it->second.refs != 0);
    if (it == fixture::frames.end() || it->second.refs == 0) return 0;
    ++fixture::decrements;
    uint32_t const remaining = --it->second.refs;
    if (remaining == 0) { fixture::frames.erase(it); ++fixture::frees; std::free(p); }
    return remaining;
}
void page_free(void* p) {
    auto it = fixture::frames.find(p);
    CHECK(it != fixture::frames.end() && it->second.refs != 0);
    if (it == fixture::frames.end()) return;
    // Match the physical allocator's direct-free refusal while borrowed.
    if (it->second.refs > 1) { ++fixture::direct_free_refused; return; }
    fixture::frames.erase(it); ++fixture::frees; std::free(p);
}
}
namespace ker::mod::mm::swap {
auto swap_available() -> bool { return fixture::available; }
auto allocate_slot(SwapSlot* out) -> int {
    ++fixture::slot_allocations;
    CHECK(out != nullptr);
    if (fixture::slot_allocate_error != 0) return fixture::slot_allocate_error;
    uint64_t const index = ++fixture::next_slot;
    *out = {.area = 7, .index = index, .generation = index + 100};
    CHECK(fixture::slots.emplace(index, fixture::Slot{.id = *out}).second);
    return 0;
}
auto free_slot(SwapSlot slot) -> int {
    ++fixture::slot_frees;
    auto it = fixture::slots.find(slot.index);
    CHECK(it != fixture::slots.end() && it->second.id.area == slot.area && it->second.id.generation == slot.generation);
    if (fixture::slot_free_error != 0) return fixture::slot_free_error;
    if (it == fixture::slots.end()) return -EINVAL;
    fixture::slots.erase(it);
    return 0;
}
auto write_slot(SwapSlot slot, const void* page) -> int {
    ++fixture::slot_writes;
    auto it = fixture::slots.find(slot.index);
    CHECK(it != fixture::slots.end() && it->second.id.generation == slot.generation && fixture::frames.contains(const_cast<void*>(page)));
    if (fixture::slot_write_error != 0) return fixture::slot_write_error;
    if (it == fixture::slots.end()) return -EINVAL;
    std::memcpy(it->second.bytes.data(), page, fixture::BS);
    return 0;
}
auto read_slot(SwapSlot slot, void* page) -> int {
    ++fixture::slot_reads;
    auto it = fixture::slots.find(slot.index);
    CHECK(it != fixture::slots.end() && it->second.id.generation == slot.generation && fixture::frames.contains(page));
    if (fixture::slot_read_error != 0) return fixture::slot_read_error;
    if (it == fixture::slots.end()) return -EINVAL;
    std::memcpy(page, it->second.bytes.data(), fixture::BS);
    return 0;
}
}
'''

CASES = r'''
namespace fixture {
using namespace ker::vfs::tmpfs;
namespace phys = ker::mod::mm::phys;
namespace swap = ker::mod::mm::swap;
struct Node {
    TmpfsMount mount{};
    TmpNode* node{new TmpNode};
    Node() {
        mount.max_bytes = 32 * BS;
        node->mount = &mount;
        register_tmp_node(node);
    }
    ~Node() { dispose(); CHECK(mount.used_bytes == 0); }
    void dispose() { if (node != nullptr) { tmpfs_free_node(node); node = nullptr; } }
    auto page(size_t index, unsigned char seed = 0x3b) -> void* {
        ker::mod::sys::MutexGuard guard(node->io_lock);
        TmpPage* out = nullptr;
        CHECK(ensure_page_resident_locked(node, index, &out) == 0 && out != nullptr);
        if (out == nullptr) return nullptr;
        CHECK(out == &node->pages[index] && out->state == TmpPageState::RESIDENT && refs(out->data) == 1);
        fill_bytes(out->data, seed);
        return out->data;
    }
};
void empty_baseline() {
    CHECK(tmpfs_nodes.size() == 0 && tmpfs_resident_pages.load() == 0);
    CHECK(frames.empty() && slots.empty() && allocations == frees);
    CHECK(destroy_held == 0 && invalid_unlock == 0 && lock_errors == 0);
    CHECK(locks.size() == 2); // Complete actual global tree and registry mutexes.
    CHECK(arrays.empty() && array_allocations == array_frees);
    CHECK(mount_pins==0 && mount_acquires>=mount_releases);
    for (auto [_, held] : locks) CHECK(!held);
}
void materialize_owner() {
    Node f;
    ker::mod::sys::MutexGuard guard(f.node->io_lock);
    TmpPage* p = nullptr;
    CHECK(ensure_page_resident_locked(f.node, 3, &p) == 0 && p != nullptr);
    CHECK(f.node->page_count == 4 && f.node->charged_pages == 1 && f.node->resident_pages == 1 && f.mount.used_bytes == BS);
    CHECK(tmpfs_resident_pages.load() == 1 && allocations == 1 && refs(p->data) == 1);
    auto* bytes = static_cast<unsigned char*>(p->data);
    for (size_t i = 0; i < BS; ++i) CHECK(bytes[i] == 0);
    TmpPage* again = nullptr;
    CHECK(ensure_page_resident_locked(f.node, 3, &again) == 0 && again == p && allocations == 1 && refs(p->data) == 1);
}
void retained_release() {
    Node f;
    void* data = f.page(0, 0x67);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); phys::page_ref_inc(data); release_page_locked(f.node, f.node->pages[0], true); }
    CHECK(refs(data) == 1 && decrements == 1 && direct_free_refused == 0);
    CHECK(f.node->pages[0].state == TmpPageState::HOLE && f.node->pages[0].data == nullptr && !swap::slot_valid(f.node->pages[0].swap_slot));
    CHECK(f.node->resident_pages == 0 && f.node->charged_pages == 0 && f.mount.used_bytes == 0 && tmpfs_resident_pages.load() == 0);
    verify_bytes(data, 0x67);
    CHECK(phys::page_ref_dec(data) == 0);
}
void hole_quota() {
    Node f;
    void* data = f.page(5);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); for (size_t i = 0; i < 5; ++i) release_page_locked(f.node, f.node->pages[i], true); }
    CHECK(f.node->charged_pages == 1 && f.mount.used_bytes == BS && f.node->resident_pages == 1 && refs(data) == 1);
    verify_bytes(data, 0x3b);
}
void uncharged_release() {
    Node f;
    f.page(0);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); release_page_locked(f.node, f.node->pages[0], false); }
    CHECK(f.node->charged_pages == 1 && f.mount.used_bytes == BS && f.node->resident_pages == 0 && frames.empty());
    // Explicit caller-owned charge transfer, not another descriptor retirement.
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); --f.node->charged_pages; mount_uncharge(f.node); }
}
void descriptor_relocation() {
    Node f;
    void* data = f.page(0, 0x41);
    phys::page_ref_inc(data);
    TmpPage* old = f.node->pages;
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(ensure_page_descriptors(f.node, 17)); }
    CHECK(f.node->pages != old && f.node->pages[0].data == data && refs(data) == 2 && f.node->charged_pages == 1);
    f.dispose();
    CHECK(refs(data) == 1);
    verify_bytes(data, 0x41);
    CHECK(phys::page_ref_dec(data) == 0);
}
void exclusive_eviction() {
    Node f;
    void* data = f.page(0, 0x29);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(evict_page_locked(f.node, 0) == 0); }
    CHECK(refs(data) == 0 && frames.empty() && slot_allocations == 1 && slot_writes == 1 && slots.size() == 1);
    CHECK(f.node->pages[0].state == TmpPageState::SWAPPED && f.node->resident_pages == 0 && f.node->charged_pages == 1 && f.mount.used_bytes == BS);
    auto& bytes = slots.begin()->second.bytes;
    for (size_t i = 0; i < BS; ++i) CHECK(bytes[i] == static_cast<unsigned char>(0x29 + i * 17U));
    f.dispose();
    CHECK(slot_frees == 1 && slots.empty());
}
void borrowed_eviction() {
    Node f;
    void* data = f.page(0, 0x99);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); phys::page_ref_inc(data); phys::page_ref_inc(data); }
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(evict_page_locked(f.node, 0) == -EAGAIN); }
    CHECK(refs(data) == 3 && f.node->pages[0].data == data && f.node->pages[0].state == TmpPageState::RESIDENT);
    CHECK(slot_allocations == 0 && slot_writes == 0 && slot_frees == 0 && slots.empty());
    CHECK(f.node->resident_pages == 1 && f.node->charged_pages == 1 && f.mount.used_bytes == BS);
    verify_bytes(data, 0x99);
    CHECK(phys::page_ref_dec(data) == 2);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(evict_page_locked(f.node, 0) == -EAGAIN); }
    CHECK(slot_allocations == 0 && slot_writes == 0);
    CHECK(phys::page_ref_dec(data) == 1);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(evict_page_locked(f.node, 0) == 0); }
    CHECK(refs(data) == 0 && slot_allocations == 1 && slot_writes == 1);
}
void eviction_failure(bool allocate) {
    Node f;
    void* data = f.page(0, 0x57);
    slot_allocate_error = allocate ? -ENOSPC : 0;
    slot_write_error = allocate ? 0 : -EIO;
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(evict_page_locked(f.node, 0) == (allocate ? -ENOSPC : -EIO)); }
    CHECK(refs(data) == 1 && frames.size() == 1 && slots.empty() && f.node->pages[0].data == data);
    CHECK(f.node->pages[0].state == TmpPageState::RESIDENT && f.node->resident_pages == 1 && tmpfs_resident_pages.load() == 1);
    CHECK(f.node->charged_pages == 1 && f.mount.used_bytes == BS && slot_allocations == 1 && slot_writes == (allocate ? 0 : 1));
    CHECK(slot_frees == (allocate ? 0 : 1));
    verify_bytes(data, 0x57);
    slot_allocate_error = slot_write_error = 0;
}
void rehydrate(int fault) {
    Node f;
    f.page(0, 0x6a);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(evict_page_locked(f.node, 0) == 0); }
    auto const id = f.node->pages[0].swap_slot;
    slot_read_error = fault == 1 ? -EIO : 0;
    slot_free_error = fault == 2 ? -EIO : 0;
    TmpPage* out = nullptr;
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(ensure_page_resident_locked(f.node, 0, &out) == (fault ? -EIO : 0)); }
    if (fault) {
        CHECK(out == nullptr && frames.empty() && slots.size() == 1 && f.node->pages[0].state == TmpPageState::SWAPPED);
        CHECK(f.node->pages[0].swap_slot.index == id.index && f.node->pages[0].swap_slot.generation == id.generation);
        CHECK(f.node->resident_pages == 0 && tmpfs_resident_pages.load() == 0 && f.node->charged_pages == 1 && f.mount.used_bytes == BS);
        slot_read_error = slot_free_error = 0;
        { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(ensure_page_resident_locked(f.node, 0, &out) == 0 && out != nullptr); }
    }
    CHECK(out != nullptr && out->state == TmpPageState::RESIDENT && refs(out->data) == 1 && slots.empty());
    CHECK(f.node->charged_pages == 1 && f.mount.used_bytes == BS && f.node->resident_pages == 1 && tmpfs_resident_pages.load() == 1);
    verify_bytes(out->data, 0x6a);
}
void physical_allocation_failure() {
    Node f;
    allocation_failure = true;
    TmpPage* out = nullptr;
    { ker::mod::sys::MutexGuard guard(f.node->io_lock); CHECK(ensure_page_resident_locked(f.node, 4, &out) == -ENOSPC); }
    CHECK(out == nullptr && f.node->page_count == 5 && f.node->charged_pages == 0 && f.node->resident_pages == 0 && f.mount.used_bytes == 0);
    CHECK(frames.empty() && allocations == 0 && slots.empty() && slot_allocations == 0);
    allocation_failure = false;
}
void node_scan_budget() {
    Node f;
    void* borrowed = f.page(2, 0x7c);
    f.page(6);
    phys::page_ref_inc(borrowed);
    { ker::mod::sys::MutexGuard guard(f.node->io_lock);
      auto a = reclaim_from_node_locked_bounded(f.node, 8, 3);
      CHECK(a.scanned == 3 && a.reclaimed == 0 && f.node->reclaim_cursor == 3 && slot_allocations == 0);
      auto b = reclaim_from_node_locked_bounded(f.node, 8, 3);
      CHECK(b.scanned == 3 && b.reclaimed == 0 && f.node->reclaim_cursor == 6);
      auto c = reclaim_from_node_locked_bounded(f.node, 1, 99);
      CHECK(c.scanned == 1 && c.reclaimed == 1 && f.node->reclaim_cursor == 0);
      auto d = reclaim_from_node_locked_bounded(f.node, 2, 7, 2);
      CHECK(d.scanned == 7 && d.reclaimed == 0 && f.node->reclaim_cursor == 0);
      auto e = reclaim_from_node_locked_bounded(f.node, 0, 7);
      CHECK(e.scanned == 0 && e.reclaimed == 0);
    }
    verify_bytes(borrowed, 0x7c);
    CHECK(phys::page_ref_dec(borrowed) == 1);
}
void global_scan_budget() {
    Node busy, ready;
    busy.page(0); ready.page(3);
    busy.node->io_lock.lock();
    auto const before = try_attempts;
    auto a = tmpfs_reclaim_bounded(2, 2);
    CHECK(a.scanned == 2 && a.reclaimed == 0 && try_attempts - before <= 2 && slot_allocations == 0);
    busy.node->io_lock.unlock();
    auto b = tmpfs_reclaim_bounded(1, 8);
    CHECK(b.scanned <= 8 && b.reclaimed == 1);
    CHECK(tmpfs_reclaim_pages(8) == 1 && tmpfs_resident_pages.load() == 0 && frames.empty());
    CHECK(tmpfs_reclaim_pages(0) == 0);
    available = false;
    auto c = tmpfs_reclaim_bounded(8, 8);
    CHECK(c.scanned == 0 && c.reclaimed == 0 && tmpfs_reclaim_pages(8) == 0);
    available = true;
}
void lookup_deferred_free() {
    Node f;
    void* data = f.page(0);
    f.node->lookup_ref_count.store(1);
    auto const generation = tmpfs_node_registry_generation;
    tmpfs_free_node(f.node);
    CHECK(f.node->unlinked && f.node->reclaim_registered && refs(data) == 1);
    CHECK(tmpfs_node_registry_generation == generation && f.node->charged_pages == 1 && f.mount.used_bytes == BS);
    f.node->lookup_ref_count.store(0);
    f.dispose();
    CHECK(tmpfs_node_registry_generation == generation + 1 && frames.empty());
}
void guard_before_owner_destruction() {
    Node f;
    void* data = f.page(1, 0x83);
    phys::page_ref_inc(data);
    f.node->symlink_target = new char[9];
    f.node->children = new TmpNode*[3]{};
    f.dispose();
    CHECK(destroy_held == 0 && invalid_unlock == 0 && lock_errors == 0 && refs(data) == 1);
    verify_bytes(data, 0x83);
    CHECK(phys::page_ref_dec(data) == 0);
}
void aliases_skip_unread_target(unsigned kind) {
    // A still-owned real Host TmpNode has temporarily unreadable bytes. This
    // tests absence of target access; it creates no dangling kernel object.
    CHECK(sizeof(TmpNode) <= BS);
    void* memory = ::mmap(nullptr, BS, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK(memory != MAP_FAILED);
    if (memory == MAP_FAILED) return;
    auto* target = new (memory) TmpNode;
    Node alias;
    alias.node->hardlink_target = target;
    CHECK(::mprotect(memory, BS, PROT_NONE) == 0);
    if (kind == 0) {
        auto a = tmpfs_reclaim_bounded(8, 8);
        CHECK(a.scanned == 0 && a.reclaimed == 0);
    } else if (kind == 1) {
        CHECK(tmpfs_reclaim_pages(8) == 0);
    } else {
        ker::mod::sys::MutexGuard guard(alias.node->io_lock);
        CHECK(migrate_node_swap_area_locked(alias.node, 7) == 0);
    }
    CHECK(slot_allocations == 0 && slot_writes == 0 && slot_reads == 0);
    CHECK(::mprotect(memory, BS, PROT_READ | PROT_WRITE) == 0);
    alias.node->hardlink_target = nullptr;
    target->~TmpNode();
    CHECK(::munmap(memory, BS) == 0);
}

struct ResizeSnapshot {
    size_t size{}, count{}, charged{}, resident{}, used{}, live_arrays{};
    uintptr_t identity{};
    std::array<TmpPage, 3> pages{};
    std::array<std::array<unsigned char, BS>, 3> bytes{};
    ker::vfs::Timespec mtime{}, ctime{};
    ker::vfs::coherency::Stamp stamp{};
};
auto resize_snapshot(Node& f) -> ResizeSnapshot {
    ker::mod::sys::MutexGuard guard(f.node->io_lock);
    ResizeSnapshot s{};
    s.size=f.node->size; s.count=f.node->page_count; s.charged=f.node->charged_pages;
    s.resident=f.node->resident_pages; s.used=f.mount.used_bytes; s.live_arrays=arrays.size();
    s.identity=reinterpret_cast<uintptr_t>(f.node->pages); s.mtime=f.node->mtime; s.ctime=f.node->ctime;
    CHECK(f.node->coherency.snapshot(s.stamp));
    for(size_t i=0;i<std::min(s.count,s.pages.size());++i) {
        s.pages[i]=f.node->pages[i];
        if(s.pages[i].state==TmpPageState::RESIDENT) {
            CHECK(refs(s.pages[i].data)>0);
            std::memcpy(s.bytes[i].data(),s.pages[i].data,BS);
        }
    }
    return s;
}
void unchanged_resize(Node& f,const ResizeSnapshot& before) {
    auto after=resize_snapshot(f);
    CHECK(after.size==before.size && after.count==before.count && after.identity==before.identity);
    CHECK(after.charged==before.charged && after.resident==before.resident && after.used==before.used);
    CHECK(after.live_arrays==before.live_arrays);
    CHECK(after.mtime.tv_sec==before.mtime.tv_sec && after.mtime.tv_nsec==before.mtime.tv_nsec);
    CHECK(after.ctime.tv_sec==before.ctime.tv_sec && after.ctime.tv_nsec==before.ctime.tv_nsec);
    CHECK(after.stamp.object==before.stamp.object && after.stamp.directory==before.stamp.directory);
    CHECK(after.stamp.content>=before.stamp.content && after.stamp.metadata>=before.stamp.metadata);
    for(size_t i=0;i<std::min(before.count,before.pages.size());++i) {
        auto const&a=after.pages[i];auto const&b=before.pages[i];
        CHECK(a.state==b.state && a.data==b.data && a.swap_slot.area==b.swap_slot.area &&
              a.swap_slot.index==b.swap_slot.index && a.swap_slot.generation==b.swap_slot.generation);
        if(b.state==TmpPageState::RESIDENT) CHECK(after.bytes[i]==before.bytes[i]);
    }
}
auto resize_file(Node& f) -> ker::vfs::File {
    ker::vfs::File file{};
    file.private_data=f.node; file.fops=get_tmpfs_fops();
    return file;
}
void resize_setup(Node& f,unsigned kind) {
    if(kind!=0) { f.page(0,0x51); f.page(2,0x73); }
    { ker::mod::sys::MutexGuard guard(f.node->io_lock);
      CHECK(ensure_page_descriptors(f.node,3));f.node->size=3*BS;
      f.node->mtime={12,34};f.node->ctime={56,78};
      if(kind==2) CHECK(evict_page_locked(f.node,0)==0);
    }
}
auto truncate(ker::vfs::File& file,off_t length,bool inject=false,TmpfsResizeDescriptorObservation* output=nullptr) -> int {
    auto* node=static_cast<TmpNode*>(file.private_data);
    growth_inode_lock=node!=nullptr?reinterpret_cast<uintptr_t>(&node->io_lock):0;
    int result{};
    try {
        if(inject) {
#if HAS_NATIVE_RESIZE_PORT
            CHECK(output!=nullptr);
            result=selftest_tmpfs_truncate_descriptor_oom(file,length,*output);
#else
            // No invented predecessor implementation of the new native port.
            result=-4243;
#endif
        } else result=file.fops->vfs_truncate(&file,length);
    } catch(const std::bad_alloc&) {
        ++predecessor_throws;
        // An explicit Host exception observation, never an old native errno.
        std::fprintf(stderr,"NOTE actual_predecessor_array_throw=1 native_errno_unqualified=1\n");
        result=-4242;
    }
    growth_inode_lock=0;
    return result;
}
void resize_array_oom() {
    Node f;resize_setup(f,1);auto file=resize_file(f);auto before=resize_snapshot(f);
    fail_array_bytes=sizeof(TmpPage);fail_array_armed=true;
    int ret=truncate(file,127);fail_array_armed=false;
    CHECK(ret==-ENOMEM && array_faults==1 && predecessor_throws==0);
    unchanged_resize(f,before);
    CHECK(growth_commits==0 && growth_denials==0 && slot_reads==0 && slot_writes==0);
    verify_bytes(f.node->pages[0].data,0x51);
}
void resize_port_oom(unsigned kind,bool grow) {
    Node f;resize_setup(f,kind);auto file=resize_file(f);auto before=resize_snapshot(f);
    auto reads=slot_reads,writes=slot_writes,allocated=slot_allocations,freed=slot_frees;
    TmpfsResizeDescriptorObservation out{};
    CHECK(truncate(file,grow?7*BS:127,true,&out)==-ENOMEM);
    CHECK(out.admitted && out.allocation_attempts==1 && out.injected_failures==1);
    unchanged_resize(f,before);
    CHECK(slot_reads==reads && slot_writes==writes && slot_allocations==allocated && slot_frees==freed);
    CHECK(growth_commits==0 && growth_denials==0);
}
void resize_swap_read_failure() {
    Node f;resize_setup(f,2);auto file=resize_file(f);auto before=resize_snapshot(f);
    slot_read_error=-EIO;
    CHECK(truncate(file,127)==-EIO);
    unchanged_resize(f,before);
    CHECK(slot_reads==1 && slots.size()==1 && growth_commits==0);
    slot_read_error=0;
}
void resize_swap_success() {
    Node f;resize_setup(f,2);auto file=resize_file(f);
    CHECK(truncate(file,127)==0);
    CHECK(f.node->size==127 && f.node->page_count==1 && f.node->charged_pages==1 && f.node->resident_pages==1);
    CHECK(f.mount.used_bytes==BS && slots.empty() && slot_reads==1 && slot_frees==1);
    auto const&p=f.node->pages[0];
    CHECK(p.state==TmpPageState::RESIDENT && refs(p.data)==1 && !swap::slot_valid(p.swap_slot));
    auto* data=static_cast<unsigned char*>(p.data);
    for(size_t i=0;i<BS;++i) CHECK(data[i]==(i<127?static_cast<unsigned char>(0x51+i*17U):0));
    CHECK(growth_commits==1 && last_committed_size==127);
}
void resize_nonallocating() {
    Node f;resize_setup(f,1);auto file=resize_file(f);TmpfsResizeDescriptorObservation out{};
    auto live=arrays.size(),allocated=array_allocations;
    CHECK(truncate(file,3*BS-1,true,&out)==0);
    CHECK(out.admitted && out.allocation_attempts==0 && out.injected_failures==0);
    CHECK(arrays.size()==live && array_allocations==allocated && f.node->size==3*BS-1);
    auto* tail=static_cast<const unsigned char*>(f.node->pages[2].data);
    for(size_t i=0;i<BS;++i) CHECK(tail[i]==(i<BS-1?static_cast<unsigned char>(0x73+i*17U):0));
    CHECK(truncate(file,3*BS,true,&out)==0 && out.allocation_attempts==0);
    CHECK(truncate(file,0,true,&out)==0 && out.allocation_attempts==0);
    CHECK(f.node->page_count==0 && f.node->charged_pages==0 && f.mount.used_bytes==0 && frames.empty());
    CHECK(growth_commits==3 && last_committed_size==0);
}
void resize_admission() {
    Node f;resize_setup(f,1);auto file=resize_file(f);auto before=resize_snapshot(f);
    TmpfsResizeDescriptorObservation out{};
    CHECK(truncate(file,-1,true,&out)==-EINVAL && out.allocation_attempts==0);
    unchanged_resize(f,before);
    file.open_flags=3;
    CHECK(truncate(file,127,true,&out)==-EBADF && out.allocation_attempts==0);
    file.open_flags=2;f.node->type=TmpNodeType::DIRECTORY;
    CHECK(truncate(file,127,true,&out)==-EISDIR && out.allocation_attempts==0);
    f.node->type=TmpNodeType::FILE;fsize_limit=16;
    CHECK(truncate(file,127,true,&out)==-EFBIG && out.allocation_attempts==0);
    fsize_limit=UINT64_MAX;
    CHECK(growth_denials==1 && growth_commits==0);
    ker::vfs::ExecReadView view{};
    {ker::mod::sys::MutexGuard guard(f.node->io_lock);CHECK(f.node->exec_content.acquire(view)==0);}
    CHECK(truncate(file,127,true,&out)==-ETXTBSY && out.allocation_attempts==0);
    {ker::mod::sys::MutexGuard guard(f.node->io_lock);f.node->exec_content.release();
      CHECK(tmpfs_resize_locked(f.node,SIZE_MAX)==-EFBIG);}
    unchanged_resize(f,before);
    file.fs_type=ker::vfs::FSType::FAT32;
    CHECK(truncate(file,127,true,&out)==-EINVAL && !out.admitted && out.allocation_attempts==0);
    file.fs_type=ker::vfs::FSType::TMPFS;
    CHECK(growth_commits==0 && array_faults==0);
}
void unchanged_eof(Node& f,const ResizeSnapshot& before) {
    unchanged_resize(f,before);
    auto after=resize_snapshot(f);
    CHECK(after.stamp.content==before.stamp.content && after.stamp.metadata==before.stamp.metadata);
}
void eof_write_guard() {
    Node f;resize_setup(f,1);auto file=resize_file(f);
    auto* frame=f.node->pages[0].data;phys::page_ref_inc(frame);
    f.node->read_shared_eof_leases=1;
    std::array<unsigned char,8> bytes{0x31,0x72,0x19,0x93,0x42,0x51,0x87,0x22};
    growth_inode_lock=reinterpret_cast<uintptr_t>(&f.node->io_lock);
    CHECK(tmpfs_write(&file,bytes.data(),bytes.size(),17)==static_cast<ssize_t>(bytes.size()));
    growth_inode_lock=0;
    CHECK(f.node->pages[0].data==frame && refs(frame)==2 && f.node->size==3*BS);
    CHECK(std::memcmp(static_cast<unsigned char*>(frame)+17,bytes.data(),bytes.size())==0);
    auto before=resize_snapshot(f);auto alloc=allocations,slots_before=slot_allocations;
    auto reads=slot_reads,writes=slot_writes,arrays_before=array_allocations,commits=growth_commits;
    growth_inode_lock=reinterpret_cast<uintptr_t>(&f.node->io_lock);
    CHECK(tmpfs_write(&file,bytes.data(),bytes.size(),3*BS-4)==-EBUSY);
    size_t append_offset=999;
    CHECK(tmpfs_write_append(&file,bytes.data(),bytes.size(),&append_offset)==-EBUSY && append_offset==999);
    growth_inode_lock=0;
    unchanged_eof(f,before);
    CHECK(allocations==alloc && slot_allocations==slots_before && slot_reads==reads && slot_writes==writes);
    CHECK(array_allocations==arrays_before && growth_commits==commits && refs(frame)==2);
    CHECK(tmpfs_write(&file,bytes.data(),0,3*BS+100)==0);
    f.node->read_shared_eof_leases=0;
    CHECK(tmpfs_write(&file,bytes.data(),bytes.size(),3*BS-4)==static_cast<ssize_t>(bytes.size()));
    CHECK(f.node->size==3*BS+4);
    CHECK(phys::page_ref_dec(frame)==1);
}
void eof_resize_guard() {
    for(unsigned kind=0;kind<3;++kind) {
        Node f;resize_setup(f,kind);auto file=resize_file(f);f.node->read_shared_eof_leases=1;
        auto before=resize_snapshot(f);auto alloc=allocations,reads=slot_reads,writes=slot_writes;
        auto arrays_before=array_allocations,commits=growth_commits;
        CHECK(truncate(file,127)==-EBUSY);
        unchanged_eof(f,before);
        CHECK(truncate(file,4*BS)==-EBUSY);
        unchanged_eof(f,before);
        CHECK(allocations==alloc && array_allocations==arrays_before && slot_reads==reads && slot_writes==writes);
        CHECK(growth_commits==commits);
        CHECK(truncate(file,3*BS)==0 && f.node->size==3*BS);
        f.node->read_shared_eof_leases=0;
        CHECK(truncate(file,127)==0 && f.node->size==127);
    }
}
void eof_copy_guard() {
    Node dst,src;resize_setup(dst,1);resize_setup(src,1);
    fill_bytes(src.node->pages[0].data,0x21);
    fill_bytes(src.node->pages[2].data,0x45);
    dst.node->read_shared_eof_leases=1;
    auto before=resize_snapshot(dst),source=resize_snapshot(src);
    auto alloc=allocations,decs=decrements,reads=slot_reads,writes=slot_writes;
    auto arrays_before=array_allocations;
    CHECK(tmpfs_copy_file_contents(dst.node,src.node)==-EBUSY);
    unchanged_eof(dst,before);unchanged_eof(src,source);
    CHECK(allocations==alloc && decrements==decs && slot_reads==reads && slot_writes==writes);
    CHECK(array_allocations==arrays_before && tmpfs_copy_file_contents(dst.node,dst.node)==0);
    unchanged_eof(dst,before);
    dst.node->read_shared_eof_leases=0;
    CHECK(tmpfs_copy_file_contents(dst.node,src.node)==0);
    CHECK(dst.node->size==src.node->size && dst.node->charged_pages==2);
    CHECK(std::memcmp(dst.node->pages[0].data,src.node->pages[0].data,BS)==0);
    CHECK(std::memcmp(dst.node->pages[2].data,src.node->pages[2].data,BS)==0);
}
void checked_node_pins() {
    Node f;
    f.node->lookup_ref_count.store(UINT32_MAX);
    auto denied=tmpfs_acquire_node(f.node);
    CHECK(!denied && f.node->lookup_ref_count.load()==UINT32_MAX);
    // A genuine old unchecked body may wrap. Normalize only this owned Host
    // fault-input counter before releasing any unexpectedly acquired owner.
    if(denied)f.node->lookup_ref_count.store(1);
    denied.reset();f.node->lookup_ref_count.store(0);
    f.node->lookup_ref_count.store(UINT32_MAX-1);
    auto pin=tmpfs_acquire_node(f.node);
    CHECK(pin && pin.get()==f.node && f.node->lookup_ref_count.load()==UINT32_MAX);
    pin.reset();CHECK(f.node->lookup_ref_count.load()==UINT32_MAX-1);
    f.node->lookup_ref_count.store(0);
}
void checked_alias_pins() {
    Node alias,canonical;alias.node->hardlink_target=canonical.node;
    for(unsigned which=0;which<2;++which) {
        alias.node->lookup_ref_count.store(which==0?UINT32_MAX:0);
        canonical.node->lookup_ref_count.store(which==1?UINT32_MAX:0);
        auto denied=tmpfs_acquire_node(alias.node);
        CHECK(!denied && alias.node->lookup_ref_count.load()==(which==0?UINT32_MAX:0));
        CHECK(canonical.node->lookup_ref_count.load()==(which==1?UINT32_MAX:0));
        if(denied) {alias.node->lookup_ref_count.store(1);canonical.node->lookup_ref_count.store(1);}
        denied.reset();alias.node->lookup_ref_count.store(0);canonical.node->lookup_ref_count.store(0);
    }
    alias.node->lookup_ref_count.store(UINT32_MAX-1);canonical.node->lookup_ref_count.store(UINT32_MAX-1);
    auto pin=tmpfs_acquire_node(alias.node);
    CHECK(pin && pin.get()==alias.node && alias.node->lookup_ref_count.load()==UINT32_MAX);
    CHECK(canonical.node->lookup_ref_count.load()==UINT32_MAX);
    TmpfsNodeRef moved(std::move(pin));CHECK(!pin && moved);
    TmpfsNodeRef assigned;assigned=std::move(moved);CHECK(!moved && assigned);
    assigned.reset();
    CHECK(alias.node->lookup_ref_count.load()==UINT32_MAX-1 && canonical.node->lookup_ref_count.load()==UINT32_MAX-1);
    alias.node->lookup_ref_count.store(0);canonical.node->lookup_ref_count.store(0);
    alias.node->hardlink_target=nullptr;
}
void node_pin_real_deferred_free() {
    Node f;f.page(0,0x29);
    auto pin=tmpfs_acquire_node(f.node);
    CHECK(pin && f.node->lookup_ref_count.load()==1);
    f.node->unlinked=true;f.node->link_count.store(0);
    f.dispose();CHECK(frames.size()==1 && f.mount.used_bytes==BS);
    pin.reset();CHECK(frames.empty() && f.mount.used_bytes==0 && tmpfs_nodes.size()==0);
}
void checked_lookup_transactions() {
    Node root,child;root.node->type=TmpNodeType::DIRECTORY;root.node->mode=0755;
    std::memcpy(child.node->name.data(),"leaf",5);
    root.node->children=new TmpNode*[1]{child.node};root.node->children_count=1;
    ker::mod::security::Snapshot credentials{};
    for(const char* path:{"/","."}) {
        root.node->lookup_ref_count.store(UINT32_MAX-1);
        TmpfsLookupHandle out{};
        CHECK(tmpfs_acquire_lookup(credentials,root.node,path,&out)==-EOVERFLOW);
        CHECK(out.parent()==nullptr && out.target()==nullptr && root.node->lookup_ref_count.load()==UINT32_MAX-1);
        if(out.parent()!=nullptr || out.target()!=nullptr)root.node->lookup_ref_count.store(2);
        out=TmpfsLookupHandle{};root.node->lookup_ref_count.store(0);
        CHECK(lock_errors==0 && invalid_unlock==0);
    }
    child.node->lookup_ref_count.store(UINT32_MAX);
    TmpfsLookupHandle denied{};
    CHECK(tmpfs_acquire_lookup(credentials,root.node,"leaf",&denied)==-EOVERFLOW);
    CHECK(denied.parent()==nullptr && denied.target()==nullptr && root.node->lookup_ref_count.load()==0);
    CHECK(child.node->lookup_ref_count.load()==UINT32_MAX);
    if(denied.parent()!=nullptr)root.node->lookup_ref_count.store(1);
    if(denied.target()!=nullptr)child.node->lookup_ref_count.store(1);
    denied=TmpfsLookupHandle{};root.node->lookup_ref_count.store(0);child.node->lookup_ref_count.store(0);
    TmpfsLookupHandle missing{};
    CHECK(tmpfs_acquire_lookup(credentials,root.node,"missing",&missing)==0);
    CHECK(missing.parent()==root.node && missing.target()==nullptr && !missing.target_existed());
    missing=TmpfsLookupHandle{};CHECK(root.node->lookup_ref_count.load()==0);
    root.node->lookup_ref_count.store(UINT32_MAX);
    CHECK(tmpfs_acquire_lookup(credentials,root.node,"missing",&missing)==-EOVERFLOW);
    CHECK(missing.parent()==nullptr && root.node->lookup_ref_count.load()==UINT32_MAX);
    if(missing.parent()!=nullptr)root.node->lookup_ref_count.store(1);
    missing=TmpfsLookupHandle{};root.node->lookup_ref_count.store(0);
    root.node->children[0]=nullptr;
    CHECK(lock_errors==0 && invalid_unlock==0);
}
void descriptor_growth_failure(bool overflow) {
    Node f;void* frame=f.page(0,0x52);
    auto* descriptors=f.node->pages;auto allocated=array_allocations;
    size_t const required=overflow?SIZE_MAX/sizeof(TmpPage)+1:19;
    if(!overflow){fail_array_bytes=required*sizeof(TmpPage);fail_array_armed=true;}
    bool result=true,threw=false;
    try {ker::mod::sys::MutexGuard guard(f.node->io_lock);result=ensure_page_descriptors(f.node,required);}
    catch(const std::bad_alloc&){threw=true;++predecessor_throws;}
    fail_array_armed=false;
    CHECK(!result && !threw);
    CHECK(f.node->pages==descriptors && f.node->page_count==1 && f.node->pages[0].data==frame);
    CHECK(array_allocations==allocated && refs(frame)==1 && f.node->charged_pages==1 && f.mount.used_bytes==BS);
    verify_bytes(frame,0x52);
}
#if HAS_DEMAND_BACKING
struct Demand {
    Node inode;
    ker::vfs::MountPoint mounted{.private_data=&inode.mount};
    ker::vfs::File file;
    std::unique_ptr<TmpfsDemandSharedBacking> backing;
    Demand() {
        file.private_data=inode.node;file.fops=get_tmpfs_fops();
        file.open_flags=ker::vfs::O_NO_CACHE;file.mount_owner=&mounted;
    }
    void prepare(uint64_t offset,uint64_t length) {
        CHECK(TmpfsDemandSharedBacking::prepare(file,offset,length,backing)==0 && backing);
        CHECK(inode.node->lookup_ref_count.load()==1 && inode.node->read_shared_eof_leases==1);
        CHECK(mounted.pins==1 && mount_pins==1);
    }
};
void demand_cold_extent() {
    Demand f;f.inode.node->size=10000*BS+3;
    f.prepare(4096*BS,5000*BS);
    CHECK(f.backing->file_offset()==4096*BS && f.backing->length()==5000*BS);
    CHECK(f.backing->eof()==10000*BS+3 && f.backing->object_identity()==f.inode.node->coherency.identity());
    CHECK(f.inode.node->pages==nullptr && f.inode.node->page_count==0 && allocations==0 && arrays.empty());
    CHECK(f.inode.mount.used_bytes==0 && f.inode.node->charged_pages==0 && slot_reads==0 && slot_writes==0);
    f.backing.reset();CHECK(f.inode.node->lookup_ref_count.load()==0 && f.inode.node->read_shared_eof_leases==0);
    CHECK(mount_pins==0 && mount_acquires==mount_releases);
}
void demand_resident_loans() {
    Demand f;f.inode.node->size=BS;void* frame=f.inode.page(0,0x64);f.prepare(0,BS);
    TmpfsPageLoan first,second;
    CHECK(f.backing->prepare_page(0,first)==0 && first.frame()==frame && refs(frame)==2);
    CHECK(first.object_identity()==f.backing->object_identity() && first.file_offset()==0 && first.eof()==BS);
    CHECK(f.backing->prepare_page(0,second)==0 && second.frame()==frame && refs(frame)==3);
    TmpfsPageLoan moved(std::move(first));CHECK(first.frame()==nullptr && first.eof()==0 && moved.frame()==frame);
    second=std::move(moved);CHECK(moved.frame()==nullptr && second.frame()==frame && refs(frame)==2);
    {ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);CHECK(evict_page_locked(f.inode.node,0)==-EAGAIN);}
    CHECK(slot_allocations==0 && slot_writes==0);verify_bytes(second.frame(),0x64);
    void* pte_owner=second.release_to_pte();CHECK(second.frame()==nullptr && refs(pte_owner)==2);
    f.backing.reset();CHECK(refs(pte_owner)==2 && mount_pins==0);
    CHECK(phys::page_ref_dec(pte_owner)==1);
}
void demand_sparse_high_offset() {
    Demand f;f.inode.node->size=5001*BS;f.inode.mount.max_bytes=BS;f.prepare(4999*BS,2*BS);
    TmpfsPageLoan loan;CHECK(f.backing->prepare_page(5000*BS,loan)==0 && loan.frame()!=nullptr);
    CHECK(f.inode.node->page_count==5001 && f.inode.node->charged_pages==1 && f.inode.mount.used_bytes==BS);
    CHECK(f.inode.node->pages[4999].state==TmpPageState::HOLE && refs(loan.frame())==2);
    for(size_t i=0;i<BS;++i)CHECK(static_cast<unsigned char*>(loan.frame())[i]==0);
    TmpfsPageLoan denied;CHECK(f.backing->prepare_page(4999*BS,denied)==-ENOSPC && denied.frame()==nullptr);
    CHECK(f.inode.node->charged_pages==1 && f.inode.mount.used_bytes==BS && refs(loan.frame())==2);
}
void demand_swapped_page() {
    Demand f;f.inode.node->size=BS;f.inode.page(0,0x75);
    {ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);CHECK(evict_page_locked(f.inode.node,0)==0);}
    f.prepare(0,BS);TmpfsPageLoan loan;auto slot=f.inode.node->pages[0].swap_slot;
    slot_read_error=-EIO;CHECK(f.backing->prepare_page(0,loan)==-EIO && loan.frame()==nullptr);
    CHECK(f.inode.node->pages[0].state==TmpPageState::SWAPPED && slots.contains(slot.index) && frames.empty());
    slot_read_error=0;slot_free_error=-EIO;
    CHECK(f.backing->prepare_page(0,loan)==-EIO && loan.frame()==nullptr);
    CHECK(f.inode.node->pages[0].state==TmpPageState::SWAPPED && slots.contains(slot.index) && frames.empty());
    slot_free_error=0;CHECK(f.backing->prepare_page(0,loan)==0 && loan.frame()!=nullptr);
    CHECK(slots.empty() && refs(loan.frame())==2 && f.inode.node->charged_pages==1);
    verify_bytes(loan.frame(),0x75);
}
void demand_page_failures() {
    Demand f;f.inode.node->size=4*BS;f.prepare(0,4*BS);TmpfsPageLoan loan;
    fail_array_bytes=4*sizeof(TmpPage);fail_array_armed=true;
    CHECK(f.backing->prepare_page(3*BS,loan)==-ENOMEM && loan.frame()==nullptr);
    CHECK(!fail_array_armed && f.inode.node->page_count==0 && arrays.empty() && allocations==0);
    allocation_failure=true;CHECK(f.backing->prepare_page(3*BS,loan)==-ENOSPC && loan.frame()==nullptr);
    allocation_failure=false;CHECK(f.inode.node->page_count==4 && f.inode.node->charged_pages==0 && frames.empty());
    ref_failure=true;CHECK(f.backing->prepare_page(3*BS,loan)==-EOVERFLOW && loan.frame()==nullptr);
    ref_failure=false;CHECK(f.inode.node->charged_pages==1 && refs(f.inode.node->pages[3].data)==1);
    CHECK(f.backing->prepare_page(3*BS,loan)==0 && refs(loan.frame())==2);
    auto alloc=allocations,reads=slot_reads,arr=array_allocations;void* frame=loan.frame();
    CHECK(f.backing->prepare_page(3*BS,loan)==-EINVAL && loan.frame()==frame && refs(frame)==2);
    loan.reset();CHECK(refs(frame)==1);
    CHECK(f.backing->prepare_page(1,loan)==-EINVAL && f.backing->prepare_page(4*BS,loan)==-EINVAL);
    host_interrupts_enabled=false;CHECK(f.backing->prepare_page(3*BS,loan)==-EWOULDBLOCK);host_interrupts_enabled=true;
    host_preempt_count=1;CHECK(f.backing->prepare_page(3*BS,loan)==-EWOULDBLOCK);host_preempt_count=0;
    CHECK(loan.frame()==nullptr && allocations==alloc && slot_reads==reads && array_allocations==arr);
}
void demand_admission_failures() {
    Demand f;f.inode.node->size=BS;
    auto denied=[&](int expected){
        CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,2*BS,f.backing)==expected && !f.backing);
        CHECK(f.inode.node->lookup_ref_count.load()==0 && f.inode.node->read_shared_eof_leases==0 && mount_pins==0);
    };
    f.file.open_flags=ker::vfs::O_ALWAYS_CACHE;denied(-EOPNOTSUPP);f.file.open_flags=ker::vfs::O_NO_CACHE|1;denied(-EBADF);
    f.file.open_flags=ker::vfs::O_NO_CACHE;f.file.refcount.store(0);denied(-EBADF);f.file.refcount.store(1);
    f.file.cache_notify_attachment=&f;denied(-EOPNOTSUPP);f.file.cache_notify_attachment=nullptr;
    f.file.mount_owner=nullptr;denied(-EOPNOTSUPP);f.file.mount_owner=&f.mounted;
    f.file.exec_read_view.reset(new ker::vfs::ExecReadView{});denied(-ETXTBSY);f.file.exec_read_view.reset();
    f.inode.node->exec_content.readers=1;denied(-ETXTBSY);f.inode.node->exec_content.readers=0;
    f.inode.node->lookup_ref_count.store(UINT32_MAX);
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,BS,f.backing)==-EOVERFLOW && !f.backing);
    CHECK(f.inode.node->lookup_ref_count.load()==UINT32_MAX && mount_pins==0);f.inode.node->lookup_ref_count.store(0);
    f.inode.node->read_shared_eof_leases=UINT64_MAX;
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,BS,f.backing)==-EOVERFLOW && !f.backing);
    CHECK(f.inode.node->read_shared_eof_leases==UINT64_MAX && f.inode.node->lookup_ref_count.load()==0 && mount_pins==0);
    f.inode.node->read_shared_eof_leases=0;mount_acquire_error=-EAGAIN;denied(-EAGAIN);mount_acquire_error=0;
    host_interrupts_enabled=false;denied(-EWOULDBLOCK);host_interrupts_enabled=true;
    host_preempt_count=1;denied(-EWOULDBLOCK);host_preempt_count=0;
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,1,BS,f.backing)==-EINVAL);
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,0,f.backing)==-EINVAL);
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,INT64_MAX-4095,2*BS,f.backing)==-EINVAL);
    f.prepare(0,2*BS);TmpfsPageLoan loan;CHECK(f.backing->prepare_page(BS,loan)==-EFAULT && !loan.frame());
    auto* old=f.backing.get();CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,BS,f.backing)==-EINVAL && f.backing.get()==old);
    CHECK(allocations==0 && arrays.empty() && slot_reads==0 && slot_writes==0);
}
void demand_fixed_eof_guards() {
    Demand f;f.inode.node->size=BS;void* frame=f.inode.page(0,0x35);f.prepare(0,2*BS);
    TmpfsPageLoan loan;CHECK(f.backing->prepare_page(0,loan)==0);
    std::array<unsigned char,3> bytes{0x22,0x56,0x76};
    CHECK(tmpfs_write(&f.file,bytes.data(),bytes.size(),17)==3);
    CHECK(std::memcmp(static_cast<unsigned char*>(loan.frame())+17,bytes.data(),bytes.size())==0);
    Node source;source.node->size=BS;source.page(0,0x38);
    auto before=resize_snapshot(f.inode);
    CHECK(tmpfs_write(&f.file,bytes.data(),bytes.size(),BS)==-EBUSY);
    CHECK(truncate(f.file,BS+1)==-EBUSY && truncate(f.file,BS-1)==-EBUSY);
    CHECK(tmpfs_copy_file_contents(f.inode.node,source.node)==-EBUSY);unchanged_eof(f.inode,before);
    f.backing.reset();CHECK(f.inode.node->read_shared_eof_leases==0 && refs(frame)==2);
    CHECK(truncate(f.file,BS-1)==0 && loan.eof()==BS && loan.frame()==frame);
}
void demand_unlinked_cleanup() {
    Demand f;f.inode.node->size=BS;f.inode.page(0,0x46);f.prepare(0,BS);
    f.inode.node->unlinked=true;f.inode.node->link_count.store(0);f.mounted.require_frames_empty=true;
    f.inode.dispose();CHECK(frames.size()==1 && f.inode.mount.used_bytes==BS && mount_pins==1);
    f.backing.reset();CHECK(frames.empty() && f.inode.mount.used_bytes==0 && tmpfs_nodes.size()==0 && mount_pins==0);
    CHECK(mount_acquires==mount_releases && invalid_unlock==0 && destroy_held==0);
}
auto shared_prepare(Demand& f,uint64_t offset,uint64_t length)->std::unique_ptr<TmpfsDemandSharedBacking> {
    std::unique_ptr<TmpfsDemandSharedBacking> out;
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,offset,length,true,out)==0 && out);
    CHECK(out && out->writable() && out->eof()==f.inode.node->size);
    return out;
}
void shared_ordinary_admission() {
    Demand f;f.inode.node->size=2*BS;f.file.open_flags=0;f.file.stream_cache_attachment=&f;
    auto before=tmpfs_file_cache_authority(f.file);
    CHECK(!before.bypass && before.inode==reinterpret_cast<uintptr_t>(f.inode.node));
    f.prepare(0,2*BS);CHECK(!f.backing->writable() && f.file.open_flags==0 && f.file.stream_cache_attachment==&f);
    CHECK(f.inode.node->mapped_writer_families==0 && f.inode.node->tail_writer_families==0);
    f.backing.reset();CHECK(!tmpfs_file_cache_authority(f.file).bypass);
    f.file.open_flags=2;f.backing=shared_prepare(f,0,2*BS);
    CHECK(f.file.open_flags==2 && f.file.stream_cache_attachment==&f && f.inode.node->exec_content.direct_writers==0);
    CHECK(f.inode.node->mapped_writer_families==1 && f.inode.node->tail_writer_families==0);
    auto marked=tmpfs_file_cache_authority(f.file);
    CHECK(marked.bypass && marked.inode==before.inode && marked.object_identity==before.object_identity);
    f.backing.reset();CHECK(f.inode.node->mapped_writer_families==0 && f.inode.node->read_shared_eof_leases==0);
    CHECK(tmpfs_file_cache_authority(f.file).bypass);
}
void shared_independent_writes() {
    Demand f;f.inode.node->size=2*BS+17;f.file.open_flags=2;
    f.backing=shared_prepare(f,0,BS);auto peer=shared_prepare(f,0,BS);
    TmpfsPageLoan first,second;CHECK(f.backing->prepare_page(0,first)==0 && peer->prepare_page(0,second)==0);
    CHECK(first.frame()==second.frame() && refs(first.frame())==3 && first.object_identity()==second.object_identity());
    static_cast<unsigned char*>(first.frame())[27]=0x73;
    CHECK(static_cast<unsigned char*>(second.frame())[27]==0x73);
    std::array<unsigned char,3> bytes{0x91,0x57,0x33};
    CHECK(tmpfs_write(&f.file,bytes.data(),bytes.size(),26)==3);
    CHECK(std::memcmp(static_cast<unsigned char*>(second.frame())+26,bytes.data(),bytes.size())==0);
    CHECK(tmpfs_write(&f.file,bytes.data(),bytes.size(),f.inode.node->size)==-EBUSY);
    CHECK(f.inode.node->mapped_writer_families==2 && f.inode.node->tail_writer_families==0);
    CHECK(f.inode.node->exec_content.direct_writers==0);
    second.reset();first.reset();peer.reset();f.backing.reset();
    CHECK(f.inode.node->mapped_writer_families==0 && f.inode.node->read_shared_eof_leases==0);
}
void shared_tail_nontail() {
    Demand f;f.inode.node->size=BS+13;f.file.open_flags=2;
    f.backing=shared_prepare(f,BS,BS);auto nontail=shared_prepare(f,0,BS);
    void* tail=f.inode.node->pages[1].data;CHECK(refs(tail)==2 && f.inode.node->tail_writer_families==1);
    TmpfsPageLoan writer;CHECK(f.backing->prepare_page(BS,writer)==0 && writer.frame()==tail);
    auto* bytes=static_cast<unsigned char*>(writer.frame());std::memset(bytes,0x62,BS);writer.reset();
    auto slots_before=slot_allocations;{ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);CHECK(evict_page_locked(f.inode.node,1)==-EAGAIN);}
    CHECK(slot_allocations==slots_before);f.backing.reset();
    CHECK(f.inode.node->mapped_writer_families==1 && f.inode.node->tail_writer_families==0 && refs(tail)==1);
    for(size_t i=0;i<BS;++i)CHECK(bytes[i]==(i<13?0x62:0));
    {ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);CHECK(evict_page_locked(f.inode.node,1)==0);}
    CHECK(f.inode.node->pages[1].state==TmpPageState::SWAPPED);
    nontail.reset();CHECK(truncate(f.file,BS+100)==0);
    {ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);TmpPage* page=nullptr;CHECK(ensure_page_resident_locked(f.inode.node,1,&page)==0);
     for(size_t i=13;i<BS;++i)CHECK(static_cast<unsigned char*>(page->data)[i]==0);}
}
void shared_multiple_tail() {
    Demand f;f.inode.node->size=19;f.file.open_flags=2;
    f.backing=shared_prepare(f,0,BS);auto peer=shared_prepare(f,0,BS);void* tail=f.inode.node->pages[0].data;
    CHECK(refs(tail)==3 && f.inode.node->tail_writer_families==2);std::memset(tail,0x68,BS);
    f.backing.reset();CHECK(refs(tail)==2 && f.inode.node->tail_writer_families==1);
    CHECK(static_cast<unsigned char*>(tail)[20]==0x68);
    auto a=allocations,arr=array_allocations,reads=slot_reads,writes=slot_writes,slots_before=slot_allocations;
    allocation_failure=true;ref_failure=true;slot_read_error=-EIO;slot_write_error=-EIO;
    peer.reset();allocation_failure=false;ref_failure=false;slot_read_error=slot_write_error=0;
    CHECK(refs(tail)==1 && f.inode.node->tail_writer_families==0 && f.inode.node->mapped_writer_families==0);
    CHECK(allocations==a && array_allocations==arr && slot_reads==reads && slot_writes==writes && slot_allocations==slots_before);
    for(size_t i=0;i<BS;++i)CHECK(static_cast<unsigned char*>(tail)[i]==(i<19?0x68:0));
}
void shared_tail_swapped() {
    Demand f;f.inode.node->size=BS+7;f.file.open_flags=2;f.inode.page(1,0x5a);
    {ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);CHECK(evict_page_locked(f.inode.node,1)==0);}
    CHECK(frames.empty() && slots.size()==1);f.backing=shared_prepare(f,BS,BS);
    void* frame=f.inode.node->pages[1].data;CHECK(slots.empty() && refs(frame)==2 && f.inode.node->tail_writer_families==1);
    verify_bytes(frame,0x5a);f.backing.reset();
    for(size_t i=7;i<BS;++i)CHECK(static_cast<unsigned char*>(frame)[i]==0);
}
void shared_tail_failures() {
    for(unsigned kind=0;kind<7;++kind) {
        Demand f;f.inode.node->size=BS+7;f.file.open_flags=2;
        if(kind>=5){f.inode.page(1,0x48);ker::mod::sys::MutexGuard guard(f.inode.node->io_lock);CHECK(evict_page_locked(f.inode.node,1)==0);}
        if(kind==0)backing_alloc_failure=true;
        if(kind==1){fail_array_bytes=2*sizeof(TmpPage);fail_array_armed=true;}
        if(kind==2)allocation_failure=true;
        if(kind==3)f.inode.mount.max_bytes=BS-1;
        if(kind==4)ref_failure=true;
        if(kind==5)slot_read_error=-EIO;
        if(kind==6)slot_free_error=-EIO;
        int expected=kind<=1?-ENOMEM:kind<=3?-ENOSPC:kind==4?-EOVERFLOW:-EIO;
        CHECK(TmpfsDemandSharedBacking::prepare(f.file,BS,BS,true,f.backing)==expected && !f.backing);
        backing_alloc_failure=fail_array_armed=allocation_failure=ref_failure=false;slot_read_error=slot_free_error=0;
        CHECK(f.inode.node->lookup_ref_count.load()==0 && f.inode.node->read_shared_eof_leases==0);
        CHECK(f.inode.node->mapped_writer_families==0 && f.inode.node->tail_writer_families==0 && mount_pins==0);
        CHECK(!tmpfs_file_cache_authority(f.file).bypass && f.inode.node->size==BS+7);
        if(kind==4)CHECK(f.inode.node->charged_pages==1 && refs(f.inode.node->pages[1].data)==1);
        if(kind>=5)CHECK(f.inode.node->pages[1].state==TmpPageState::SWAPPED && slots.size()==1 && frames.empty());
    }
    CHECK(backing_allocation_faults==1);
}
void shared_counter_limits() {
    Demand f;f.inode.node->size=7;f.file.open_flags=2;
    for(unsigned which=0;which<4;++which) {
        f.inode.node->read_shared_eof_leases=which==0?UINT64_MAX:0;
        f.inode.node->mapped_writer_families=which==1||which==2?UINT64_MAX:0;
        f.inode.node->tail_writer_families=which==2?UINT64_MAX:which==3?1:0;
        CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,BS,true,f.backing)==(which==3?-EIO:-EOVERFLOW) && !f.backing);
        CHECK(allocations==0 && arrays.empty() && mount_pins==0 && !tmpfs_file_cache_authority(f.file).bypass);
    }
    f.inode.node->tail_writer_families=0;f.inode.node->size=BS;
    f.inode.node->read_shared_eof_leases=UINT64_MAX-1;f.inode.node->mapped_writer_families=UINT64_MAX-1;
    f.backing=shared_prepare(f,0,BS);
    CHECK(f.inode.node->read_shared_eof_leases==UINT64_MAX && f.inode.node->mapped_writer_families==UINT64_MAX);
    f.backing.reset();CHECK(f.inode.node->read_shared_eof_leases==UINT64_MAX-1 && f.inode.node->mapped_writer_families==UINT64_MAX-1);
    // Restore only this owned Host counter-limit input, after the real owner drained.
    f.inode.node->read_shared_eof_leases=f.inode.node->mapped_writer_families=0;
}
void shared_access_refusals() {
    Demand f;f.inode.node->size=BS;
    auto denied=[&](int expected){CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,BS,true,f.backing)==expected && !f.backing);
        CHECK(f.inode.node->lookup_ref_count.load()==0 && f.inode.node->read_shared_eof_leases==0 && mount_pins==0);};
    f.file.open_flags=0;denied(-EACCES);f.file.open_flags=1;denied(-EBADF);
    f.file.open_flags=2;f.file.mount_policy.read_only=true;denied(-EROFS);f.file.mount_policy.read_only=false;
    f.inode.node->exec_content.readers=1;denied(-ETXTBSY);f.inode.node->exec_content.readers=0;
    f.file.open_flags=2|ker::vfs::O_ALWAYS_CACHE;denied(-EOPNOTSUPP);
    f.file.open_flags=2|ker::vfs::O_NOTIFY_CACHE_CHANGE;denied(-EOPNOTSUPP);
    f.file.open_flags=0;f.file.mount_policy.read_only=true;f.prepare(0,BS);CHECK(!f.backing->writable());
    CHECK(f.backing->sync_metadata()==0 && !tmpfs_file_cache_authority(f.file).bypass);
}
void shared_sync_metadata() {
    Demand f;f.inode.node->size=BS;f.file.open_flags=2;f.backing=shared_prepare(f,0,BS);
    auto epoch=epoch_ns;auto a=allocations,arr=array_allocations,reads=slot_reads;
    ker::vfs::coherency::Stamp before{},after{};CHECK(f.inode.node->coherency.snapshot(before));
    CHECK(f.backing->sync_metadata()==0 && epoch_ns>epoch && f.inode.node->mtime.tv_nsec==epoch_ns%1000000000ULL);
    CHECK(f.inode.node->coherency.snapshot(after) && after.content>before.content && after.metadata>before.metadata);
    epoch=epoch_ns;host_interrupts_enabled=false;CHECK(f.backing->sync_metadata()==-EWOULDBLOCK);host_interrupts_enabled=true;
    host_preempt_count=1;CHECK(f.backing->sync_metadata()==-EWOULDBLOCK);host_preempt_count=0;
    CHECK(epoch_ns==epoch && allocations==a && array_allocations==arr && slot_reads==reads);
}
void shared_reverse_exec() {
    Demand f;f.inode.node->size=BS;f.file.open_flags=2;f.backing=shared_prepare(f,0,BS);
    ker::vfs::File* output=&f.file;CHECK(tmpfs_clone_exec_file(&f.file,&output)==-ETXTBSY && output==&f.file);
    f.backing.reset();f.prepare(0,BS);CHECK(tmpfs_clone_exec_file(&f.file,&output)==-ETXTBSY && output==&f.file);
    f.backing.reset();output=nullptr;CHECK(tmpfs_clone_exec_file(&f.file,&output)==0 && output!=nullptr);
    CHECK(f.inode.node->exec_content.readers==1 && f.inode.node->open_count.load()==1);
    CHECK(TmpfsDemandSharedBacking::prepare(f.file,0,BS,true,f.backing)==-ETXTBSY && !f.backing);
    CHECK(tmpfs_fops_close(output)==0);delete output;
    CHECK(f.inode.node->exec_content.readers==0 && f.inode.node->open_count.load()==0);
}
void shared_file_cache_authority() {
    Demand f;f.inode.node->size=BS;auto expected=tmpfs_file_cache_authority(f.file);
    CHECK(!expected.bypass && expected.object_identity==f.inode.node->coherency.identity());
    {ker::mod::sys::MutexGuard tree(tmpfs_lock);ker::mod::sys::MutexGuard io(f.inode.node->io_lock);
     auto value=tmpfs_file_cache_authority(f.file);CHECK(value.inode==expected.inode && value.object_identity==expected.object_identity);}
    f.file.private_data=nullptr;CHECK(tmpfs_file_cache_authority(f.file).bypass);f.file.private_data=f.inode.node;
    f.file.fs_type=ker::vfs::FSType::FAT32;CHECK(!tmpfs_file_cache_authority(f.file).bypass);f.file.fs_type=ker::vfs::FSType::TMPFS;
    ker::vfs::FileOperations unrelated{};f.file.fops=&unrelated;CHECK(tmpfs_file_cache_authority(f.file).object_identity==0);f.file.fops=get_tmpfs_fops();
    f.file.open_flags=2;f.backing=shared_prepare(f,0,BS);CHECK(tmpfs_file_cache_authority(f.file).bypass);
    f.backing.reset();CHECK(tmpfs_file_cache_authority(f.file).bypass && f.inode.node->mapped_writer_families==0);
    // Final public-ref release transfers exclusive File ownership to close;
    // zero public refs do not invalidate the still-owned backend association.
    // The Host File facade supplies this transition, then actual TMPFS close
    // consumes its real backend open owner. No native VFS close claim is made.
    Demand closing;closing.inode.node->open_count.store(1);
    struct HostExclusiveCloseOwner {
        ker::vfs::File& file;
        explicit HostExclusiveCloseOwner(ker::vfs::File& owned):file(owned) {
            CHECK(file.refcount.load()==1);file.refcount.store(0);
        }
        ~HostExclusiveCloseOwner(){CHECK(tmpfs_fops_close(&file)==0);}
    };
    {
        HostExclusiveCloseOwner owner(closing.file);
        auto authority=tmpfs_file_cache_authority(closing.file);
        CHECK(!authority.bypass && authority.inode==reinterpret_cast<uintptr_t>(closing.inode.node));
        CHECK(authority.object_identity==closing.inode.node->coherency.identity());
        CHECK(closing.inode.node->open_count.load()==1 && closing.file.private_data==closing.inode.node);
    }
    CHECK(closing.file.private_data==nullptr && closing.inode.node->open_count.load()==0);
}
void shared_lookup_cache_authority() {
    Node root,alias,canonical;root.node->type=TmpNodeType::DIRECTORY;root.node->mode=0755;
    alias.node->hardlink_target=canonical.node;std::memcpy(alias.node->name.data(),"alias",6);
    root.node->children=new TmpNode*[1]{alias.node};root.node->children_count=1;
    ker::mod::security::Snapshot credentials{};TmpfsLookupHandle handle;
    CHECK(tmpfs_acquire_lookup(credentials,root.node,"alias",&handle)==0);
    TmpfsCacheAuthority value{};CHECK(tmpfs_lookup_cache_authority(handle,value)==0);
    CHECK(!value.bypass && value.inode==reinterpret_cast<uintptr_t>(canonical.node) && value.object_identity==canonical.node->coherency.identity());
    canonical.node->mutable_mapping_cache_bypass.store(true);CHECK(tmpfs_lookup_cache_authority(handle,value)==0 && value.bypass);
    ++root.node->directory_generation;TmpfsCacheAuthority sentinel{true,11,12};value=sentinel;
    CHECK(tmpfs_lookup_cache_authority(handle,value)==-EAGAIN && value.bypass && value.inode==11 && value.object_identity==12);
    handle=TmpfsLookupHandle{};TmpfsLookupHandle missing;
    CHECK(tmpfs_acquire_lookup(credentials,root.node,"missing",&missing)==0);
    CHECK(tmpfs_lookup_cache_authority(missing,value)==-ENOENT && value.inode==11 && value.object_identity==12);
    missing=TmpfsLookupHandle{};root.node->children[0]=nullptr;alias.node->hardlink_target=nullptr;
    CHECK(alias.node->lookup_ref_count.load()==0 && canonical.node->lookup_ref_count.load()==0);
}
void shared_unlinked_tail_cleanup() {
    Demand f;f.inode.node->size=7;f.file.open_flags=2;f.backing=shared_prepare(f,0,BS);
    f.inode.node->unlinked=true;f.inode.node->link_count.store(0);f.mounted.require_frames_empty=true;
    f.inode.dispose();CHECK(frames.size()==1 && mount_pins==1);
    f.backing.reset();CHECK(frames.empty() && tmpfs_nodes.size()==0 && f.inode.mount.used_bytes==0 && mount_pins==0);
}
void shared_staging_metadata_rollback() {
    for(bool resident:{false,true}) {
        Demand f;f.file.open_flags=2;CHECK(truncate(f.file,BS+73)==0);
        std::array<unsigned char,73> prefix{};
        if(resident){prefix.fill(0x46);CHECK(tmpfs_write(&f.file,prefix.data(),prefix.size(),BS)==73);}
        auto mtime=f.inode.node->mtime,ctime=f.inode.node->ctime;auto epoch=epoch_ns;
        auto same_times=[&](){return f.inode.node->mtime.tv_sec==mtime.tv_sec && f.inode.node->mtime.tv_nsec==mtime.tv_nsec &&
            f.inode.node->ctime.tv_sec==ctime.tv_sec && f.inode.node->ctime.tv_nsec==ctime.tv_nsec;};
        // Real backend staging succeeds, but no VM publication/sync occurs.
        // Caller abandonment must keep ordinary visible metadata unchanged.
        f.backing=shared_prepare(f,BS,BS);TmpfsPageLoan staged;
        CHECK(f.backing->prepare_page(BS,staged)==0 && same_times() && epoch_ns==epoch);
        void* frame=staged.frame();staged.reset();f.backing.reset();
        CHECK(same_times() && epoch_ns==epoch && f.inode.node->size==BS+73);
        CHECK(f.inode.node->read_shared_eof_leases==0 && f.inode.node->mapped_writer_families==0 && f.inode.node->tail_writer_families==0);
        CHECK(f.inode.node->lookup_ref_count.load()==0 && mount_pins==0 && refs(frame)==1);
        CHECK(tmpfs_file_cache_authority(f.file).bypass && std::memcmp(frame,prefix.data(),prefix.size())==0);
        for(size_t i=73;i<BS;++i)CHECK(static_cast<unsigned char*>(frame)[i]==0);
        // Explicit post-publication metadata sync is the positive contrast;
        // this Host call models only the caller boundary, never a VM commit.
        f.backing=shared_prepare(f,BS,BS);CHECK(same_times() && epoch_ns==epoch);
        CHECK(f.backing->sync_metadata()==0 && epoch_ns>epoch && !same_times());
        auto synced=epoch_ns;f.backing.reset();CHECK(epoch_ns>synced);
    }
}
#endif
}
int main(int argc, char** argv) {
    using namespace fixture;
    if (argc != 2) return 2;
    current_case = argv[1];
    std::string_view name(argv[1]);
    if (name == "materialize_owner") materialize_owner();
    else if (name == "retained_release") retained_release();
    else if (name == "hole_quota") hole_quota();
    else if (name == "uncharged_release") uncharged_release();
    else if (name == "descriptor_relocation") descriptor_relocation();
    else if (name == "exclusive_eviction") exclusive_eviction();
    else if (name == "borrowed_eviction") borrowed_eviction();
    else if (name == "allocate_slot_failure") eviction_failure(true);
    else if (name == "write_slot_failure") eviction_failure(false);
    else if (name == "rehydration") rehydrate(0);
    else if (name == "read_slot_failure") rehydrate(1);
    else if (name == "free_slot_failure") rehydrate(2);
    else if (name == "physical_allocation_failure") physical_allocation_failure();
    else if (name == "node_scan_budget") node_scan_budget();
    else if (name == "global_scan_budget") global_scan_budget();
    else if (name == "lookup_deferred_free") lookup_deferred_free();
    else if (name == "guard_before_owner_destruction") guard_before_owner_destruction();
    else if (name == "aliases_bounded_skip_unread_target") aliases_skip_unread_target(0);
    else if (name == "aliases_public_skip_unread_target") aliases_skip_unread_target(1);
    else if (name == "aliases_migration_skip_unread_target") aliases_skip_unread_target(2);
    else if (name == "resize_array_oom") resize_array_oom();
    else if (name == "resize_port_resident_oom") resize_port_oom(1,false);
    else if (name == "resize_port_swapped_oom") resize_port_oom(2,false);
    else if (name == "resize_port_hole_oom") resize_port_oom(0,false);
    else if (name == "resize_port_grow_oom") { for(unsigned kind=0;kind<3;++kind) resize_port_oom(kind,true); }
    else if (name == "resize_swap_read_failure") resize_swap_read_failure();
    else if (name == "resize_swap_success") resize_swap_success();
    else if (name == "resize_nonallocating") resize_nonallocating();
    else if (name == "resize_admission") resize_admission();
    else if (name == "eof_write_guard") eof_write_guard();
    else if (name == "eof_resize_guard") eof_resize_guard();
    else if (name == "eof_copy_guard") eof_copy_guard();
    else if (name == "checked_node_pins") checked_node_pins();
    else if (name == "checked_alias_pins") checked_alias_pins();
    else if (name == "node_pin_real_deferred_free") node_pin_real_deferred_free();
    else if (name == "checked_lookup_transactions") checked_lookup_transactions();
    else if (name == "descriptor_growth_oom") descriptor_growth_failure(false);
    else if (name == "descriptor_growth_overflow") descriptor_growth_failure(true);
#if HAS_DEMAND_BACKING
    else if (name == "demand_cold_extent") demand_cold_extent();
    else if (name == "demand_resident_loans") demand_resident_loans();
    else if (name == "demand_sparse_high_offset") demand_sparse_high_offset();
    else if (name == "demand_swapped_page") demand_swapped_page();
    else if (name == "demand_page_failures") demand_page_failures();
    else if (name == "demand_admission_failures") demand_admission_failures();
    else if (name == "demand_fixed_eof_guards") demand_fixed_eof_guards();
    else if (name == "demand_unlinked_cleanup") demand_unlinked_cleanup();
    else if (name == "shared_ordinary_admission") shared_ordinary_admission();
    else if (name == "shared_independent_writes") shared_independent_writes();
    else if (name == "shared_tail_nontail") shared_tail_nontail();
    else if (name == "shared_multiple_tail") shared_multiple_tail();
    else if (name == "shared_tail_swapped") shared_tail_swapped();
    else if (name == "shared_tail_failures") shared_tail_failures();
    else if (name == "shared_counter_limits") shared_counter_limits();
    else if (name == "shared_access_refusals") shared_access_refusals();
    else if (name == "shared_sync_metadata") shared_sync_metadata();
    else if (name == "shared_reverse_exec") shared_reverse_exec();
    else if (name == "shared_file_cache_authority") shared_file_cache_authority();
    else if (name == "shared_lookup_cache_authority") shared_lookup_cache_authority();
    else if (name == "shared_unlinked_tail_cleanup") shared_unlinked_tail_cleanup();
    else if (name == "shared_staging_metadata_rollback") shared_staging_metadata_rollback();
#endif
    else return 2;
    empty_baseline();
    std::printf("%s case=%s assertions=%llu failures=%llu frames=%zu slots=%zu resident=%llu registry=%zu destroy_held=%llu invalid_unlock=%llu real_tmpfs=1 real_smallvec=1\n",
                failures == 0 ? "PASS" : "FAIL", current_case, (unsigned long long)assertions, (unsigned long long)failures,
                frames.size(), slots.size(), (unsigned long long)ker::vfs::tmpfs::tmpfs_resident_pages.load(),
                ker::vfs::tmpfs::tmpfs_nodes.size(), (unsigned long long)destroy_held, (unsigned long long)invalid_unlock);
    return failures == 0 ? 0 : 3;
}
'''

REAL_HEADERS = (
    "util/smallvec.hpp", "vfs/exec_content.hpp", "vfs/native_coherency.hpp",
    "vfs/coherency.hpp", "vfs/stat.hpp", "vfs/file_operations.hpp", "platform/sched/file_growth.hpp",
)


def generate(scratch: Path, selected: Path) -> list[dict]:
    include = scratch / "include"
    actual = []
    for name in REAL_HEADERS:
        path = include / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((KERN / name).read_bytes())
        actual.append({"name": "whole-real-" + name, **runner.pin(KERN / name)})
    seams = {
        "platform/sys/mutex.hpp": LOCKS,
        "platform/sys/spinlock.hpp": '#pragma once\n#include "mutex.hpp"\n',
        "platform/mm/dyn/kmalloc.hpp": "#pragma once\n",
        "util/hcf.hpp": "#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf(){std::abort();}\n",
        "bits/off_t.h": "#pragma once\n#include <sys/types.h>\n",
    }
    for name, text in seams.items():
        path = include / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    types = ["#pragma once\n#include <cstdint>\n#include <cstddef>\nnamespace ker::mod::security { class Snapshot; }\n"
             "namespace ker::vfs { struct File; struct MountPoint; }\n"]
    paging = KERN / "platform/mm/paging.hpp"
    lines = [line for line in paging.read_text().splitlines() if line.startswith("constexpr uint64_t PAGE_SIZE =")]
    if len(lines) != 1:
        raise ValueError("unique real MM PAGE_SIZE declaration unavailable")
    types.append("namespace ker::mod::mm::paging {\n" + lines[0] + "\n}\n")
    actual.append({"source": str(paging), "name": "PAGE_SIZE", "bytes": len(lines[0].encode()),
                   "sha256": hashlib.sha256(lines[0].encode()).hexdigest()})
    for path, namespace, anchors in (
        (KERN / "platform/mm/page_alloc.hpp", "ker::mod::mm", ["enum class PhysicalPageOwner : uint8_t {"]),
        (KERN / "platform/mm/swap.hpp", "ker::mod::mm::swap", ["struct SwapSlot {", "constexpr auto invalid_slot()", "constexpr auto slot_valid("]),
        (KERN / "vfs/file.hpp", "ker::vfs", ["enum class FSType : uint8_t {", "struct MountPolicy {"]),
        (KERN / "vfs/mount.hpp", "ker::vfs", ["class NativeFileMappingMountPin {"]),
        (KERN / "platform/security/file_policy.hpp", "ker::mod::security", ["struct FileIdentity {"]),
        (HEADER, "ker::vfs::tmpfs", ["enum class TmpNodeType : uint8_t {", "enum class TmpPageState : uint8_t {", "struct TmpPage {", "struct TmpNode {", "struct TmpfsMount {", "class TmpfsNodeRef {", "class TmpfsPageLoan {", "class TmpfsDemandSharedBacking {", "class TmpfsLookupHandle {", "struct TmpfsCacheAuthority {", "struct TmpfsObjectMetadata {", "struct TmpfsResizeDescriptorObservation {"]),
    ):
        text = path.read_text()
        types.append(f"namespace {namespace} {{\n")
        if path == KERN / "vfs/file.hpp":
            for prefix in ("constexpr unsigned FILE_ACCESS_SEARCH_ONLY =", "constexpr int O_NO_CACHE =",
                           "constexpr int O_ALWAYS_CACHE =", "constexpr int O_NOTIFY_CACHE_CHANGE ="):
                values = [line for line in text.splitlines() if line.startswith(prefix)]
                if len(values) != 1:
                    raise ValueError(f"unique actual {prefix} declaration unavailable")
                types.append(values[0] + "\n")
                actual.append({"source": str(path), "name": prefix, "bytes": len(values[0].encode()),
                               "sha256": hashlib.sha256(values[0].encode()).hexdigest()})
        if namespace == "ker::vfs::tmpfs":
            names = [line for line in text.splitlines() if line.startswith("constexpr size_t TMPFS_NAME_MAX =")]
            if len(names) != 1:
                raise ValueError("unique actual TMPFS_NAME_MAX declaration unavailable")
            types.append(names[0] + "\nstruct TmpfsMount;\nclass TmpfsLookupHandle;\n")
            actual.append({"source": str(path), "name": "TMPFS_NAME_MAX", "bytes": len(names[0].encode()),
                           "sha256": hashlib.sha256(names[0].encode()).hexdigest()})
        for anchor in anchors:
            body, row = runner.capture(text, anchor, declaration=anchor.startswith(("struct ", "enum ", "class ")))
            actual.append({"source": str(path), **row})
            types.append(f'#line {row["line"]} "{path}"\n' + body + "\n")
        types.append("}\n")
    (include / "actual-types.hpp").write_text("\n".join(types))
    source = selected.read_text()
    chunks = [PRELUDE, "namespace ker::mod::sched::resource {\n"]
    growth_source = KERN / "platform/sched/file_growth.cpp"
    budget_source = KERN / "platform/sched/resource_model.hpp"
    body, row = runner.capture(budget_source.read_text(), "constexpr auto file_write_budget(")
    actual.append({"source": str(budget_source), **row})
    chunks.append(f'#line {row["line"]} "actual-file-write-budget.hpp"\n' + body + "\n")
    for anchor in ("auto FileGrowth::bound_write(", "auto FileGrowth::allow_resize(", "void FileGrowth::committed("):
        body, row = runner.capture(growth_source.read_text(), anchor)
        actual.append({"source": str(growth_source), **row})
        chunks.append(f'#line {row["line"]} "actual-file-growth.cpp"\n' + body + "\n")
    chunks.extend(["}\n", "namespace ker::vfs::tmpfs {\n"])
    constants = ("constexpr size_t DEFAULT_TMPFS_BLOCK_SIZE", "constexpr uint64_t TMPFS_MIN_FREE_RESERVE_BYTES",
                 "constexpr uint64_t TMPFS_MAX_FREE_RESERVE_BYTES", "constexpr uint64_t TMPFS_FREE_RESERVE_DIVISOR", "constexpr uint64_t NS_PER_SEC")
    globals_ = ("ker::mod::sys::Mutex tmpfs_lock;", "ker::mod::sys::Mutex tmpfs_node_registry_lock;", "ker::util::SmallVec<TmpNode*, 64> tmpfs_nodes;",
                "std::atomic<uint64_t> tmpfs_resident_pages", "std::atomic<uint64_t> tmpfs_reclaim_node_cursor",
                "uint64_t tmpfs_node_registry_generation", "std::atomic<uint64_t> tmpfs_namespace_generation_value")
    for anchor in (*constants, *globals_):
        lines = [line for line in source.splitlines() if line.startswith(anchor)]
        if len(lines) != 1:
            raise ValueError(f"missing unique actual declaration: {anchor}")
        body = lines[0]
        chunks.append(body + "\n")
        actual.append({"name": anchor, "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()})
    anchors = (
        "auto tmpfs_canonical_node(TmpNode* node)", "auto current_timespec()", "void touch_modified(", "auto page_count_for_size(",
        "auto tmpfs_free_reserve_bytes()", "auto tmpfs_can_allocate_page()",
        "void register_tmp_node(", "void unregister_tmp_node(", "auto mount_try_charge(", "void mount_uncharge(",
        "auto ensure_page_descriptors(", "void release_page_locked(", "auto evict_page_locked(",
        "struct TmpfsReclaimResult {", "auto reclaim_from_node_locked_bounded(", "auto reclaim_from_node_locked(",
        "auto tmpfs_reclaim_bounded(", "auto allocate_resident_page(", "auto ensure_page_resident_locked(",
        "auto migrate_node_swap_area_locked(", "void tmpfs_free_node(", "auto tmpfs_reclaim_pages(",
    )
    for anchor in anchors:
        body, row = runner.capture(source, anchor, declaration=anchor.startswith("struct "))
        actual.append({"source": str(selected), **row})
        chunks.append(f'#line {row["line"]} "actual-selected-tmpfs.cpp"\n' + body + "\n")
    current_port = "struct ResizeDescriptorAllocation {" in source
    resize_anchors = (["struct ResizeDescriptorAllocation {"] if current_port else []) + ["auto tmpfs_resize_locked("]
    if current_port:
        resize_anchors.append("auto tmpfs_truncate_with_allocation(")
    resize_anchors.append("auto tmpfs_fops_truncate(")
    if current_port:
        resize_anchors.append("auto selftest_tmpfs_truncate_descriptor_oom(")
    for anchor in resize_anchors:
        body, row = runner.capture(source, anchor, declaration=anchor.startswith("struct "))
        actual.append({"source": str(selected), **row})
        chunks.append(f'#line {row["line"]} "actual-selected-tmpfs.cpp"\n' + body + "\n")
    node_retain = "auto tmpfs_retain_node_locked(" if "auto tmpfs_retain_node_locked(" in source else "void tmpfs_retain_node_locked("
    for anchor in ("auto tmpfs_write_locked(", "auto tmpfs_write(", "auto tmpfs_write_append(", "auto tmpfs_copy_file_contents(",
                   node_retain, "auto tmpfs_node_can_be_freed_locked(", "void tmpfs_release_node_ref_locked(",
                   "auto TmpfsNodeRef::acquire_locked(", "TmpfsNodeRef::TmpfsNodeRef(", "auto TmpfsNodeRef::operator=(",
                   "TmpfsNodeRef::~TmpfsNodeRef(", "void TmpfsNodeRef::reset(", "auto tmpfs_acquire_node(",
                   "auto kstrcmp(", "auto tmpfs_lookup(", "auto tmpfs_namespace_generation_locked(", "auto tmpfs_acquire_lookup("):
        body, row = runner.capture(source, anchor)
        actual.append({"source": str(selected), **row})
        chunks.append(f'#line {row["line"]} "actual-selected-tmpfs.cpp"\n' + body + "\n")
    chunks.append("auto get_tmpfs_fops() -> FileOperations* { static FileOperations table{}; table.vfs_truncate=tmpfs_fops_truncate; return &table; }\n")
    current_demand = "auto TmpfsDemandSharedBacking::prepare(" in source
    if current_demand:
        for anchor in ("TmpfsPageLoan::TmpfsPageLoan(", "auto TmpfsPageLoan::operator=(", "TmpfsPageLoan::~TmpfsPageLoan(",
                       "auto TmpfsPageLoan::release_to_pte(", "void TmpfsPageLoan::reset(",
                       "TmpfsDemandSharedBacking::~TmpfsDemandSharedBacking(",
                       "auto TmpfsDemandSharedBacking::prepare(", "auto TmpfsDemandSharedBacking::prepare_page(",
                       "auto TmpfsDemandSharedBacking::sync_metadata(", "auto tmpfs_file_cache_authority(",
                       "auto tmpfs_validate_lookup_locked(", "auto tmpfs_lookup_target_locked(", "auto tmpfs_lookup_cache_authority(",
                       "auto tmpfs_canonical_node(const TmpNode* node)", "auto tmpfs_link_count(",
                       "void tmpfs_fill_metadata_locked(", "void tmpfs_metadata_to_stat_impl(", "void tmpfs_fill_stat_locked(",
                       "auto tmpfs_clone_exec_file(", "auto tmpfs_fops_close("):
            body, row = runner.capture(source, anchor)
            actual.append({"source": str(selected), **row})
            chunks.append(f'#line {row["line"]} "actual-selected-tmpfs.cpp"\n' + body + "\n")
    chunks.append(f"#define HAS_DEMAND_BACKING {int(current_demand)}\n")
    chunks.append(f"#define HAS_NATIVE_RESIZE_PORT {int(current_port)}\n")
    chunks.extend(["}\n", '#line 1 "independent-tmpfs-fixture-oracles"\n', CASES])
    (scratch / "harness.cpp").write_text("\n".join(chunks))
    return actual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tmpfs-source", type=Path, default=SOURCE)
    parser.add_argument("--cxx", default=os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"))
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--only", choices=VECTORS)
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 60
    scratch = None
    scratch_identity = None
    evidence = None
    groups = []
    watched = []
    before = []
    staged = []
    staged_before = []
    vectors = (args.only,) if args.only else VECTORS
    result = {"pass": False, "errors": [], "commands": [], "runtime_census": [], "scratch_removed": False,
              "whole_budget_seconds": 60, "scratch_budget_bytes": runner.SCRATCH_BUDGET,
              "command_log_budget_bytes": runner.LOG_BUDGET, "core_dump_limit_bytes": 0,
              "compiler_external_dependencies_before": False, "compiler_external_dependencies_after_only": True,
              "native_runtime_qualified": False, "shared_coherence_qualified": False, "full_goal_complete": False}
    result["imported_runner_source_snapshot"] = {
        "path": str(RUNNER.resolve()), "bytes": len(RUNNER_BYTES),
        "sha256": hashlib.sha256(RUNNER_BYTES).hexdigest(),
        "execution": "compile exact captured bytes then exec in module namespace",
        "module_name": runner.__name__, "module_file": runner.__file__,
        "source_loader_or_pycache_executed": False,
    }
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    try:
        for sig in handlers:
            signal.signal(sig, interrupted)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        selected = args.tmpfs_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError("--cxx must be a single executable compiler path")
        with compiler.open("rb") as stream:
            if stream.read(4) != b"\x7fELF":
                raise ValueError("--cxx must resolve to a real Host ELF compiler")
        if args.evidence_dir is not None:
            evidence = args.evidence_dir.absolute()
            if evidence.parent.resolve(strict=True) != evidence.parent or os.path.lexists(evidence):
                raise ValueError("--evidence-dir needs an existing canonical parent and a fresh exclusive directory")
            evidence.mkdir(mode=0o700)
        watched = [selected, HEADER, KERN / "platform/mm/page_alloc.hpp", KERN / "platform/mm/swap.hpp", KERN / "platform/mm/paging.hpp",
                   KERN / "vfs/file.hpp", KERN / "vfs/mount.hpp", KERN / "platform/security/file_policy.hpp", KERN / "platform/sched/file_growth.cpp", KERN / "platform/sched/resource_model.hpp",
                   *(KERN / name for name in REAL_HEADERS), RUNNER, Path(__file__).resolve(), compiler]
        before = [runner.pin(path) for path in watched]
        runner_pin = runner.pin(RUNNER)
        if runner_pin["bytes"] != len(RUNNER_BYTES) or runner_pin["sha256"] != hashlib.sha256(RUNNER_BYTES).hexdigest():
            raise ValueError("reused bounded runner changed after exact import capture")
        result["imported_runner_live_source_held_before_commands"] = True
        result["source_before"] = before
        result["selected_tmpfs_source"] = before[0]
        result["primitive_seams"] = ["Host physical frame allocation/reference count/direct-free refusal",
                                     "Host swap slot ownership and injected I/O failures", "Host mutex lifetime/try-lock registry",
                                     "Host protected still-owned target memory", "Host memory policy reports total bytes zero"]
        result["primitive_seams"].extend(["Host File facade/operations table; not real native File layout or lifetime",
                                          "Host FileGrowth owner capture/destruction; actual allow_resize/committed bodies",
                                          "Host throwing/nothrow array allocation ledger, 64MiB per-array ceiling and scoped failure", "Host epoch clock"])
        result["primitive_seams"].append("Owned Host inode EOF interval and lookup counter exhaustion inputs")
        result["primitive_seams"].append("Host checked mount-pin and IF/preempt primitives; actual demand preparation/loan bodies, no native mount graph or MM qualification")
        result["primitive_seams"].append("Always-admitted Host search credentials; actual FileIdentity/lookup transaction, not security policy qualification")
        runner.subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-tmpfs-ownership-", dir="/tmp"))
        info = scratch.stat()
        scratch_identity = (info.st_dev, info.st_ino, info.st_uid)
        if info.st_uid != os.getuid() or scratch.is_symlink():
            raise ValueError("fresh scratch ownership changed")
        result["scratch"] = str(scratch)
        result["actual_definitions"] = generate(scratch, selected)
        staged = sorted(path for path in scratch.rglob("*") if path.is_file())
        staged_before = [runner.pin(path) for path in staged]
        result["staged_sources_before"] = staged_before
        binary = scratch / "tmpfs-ownership-test"
        depfile = scratch / "dependencies.d"
        argv = [str(compiler), "-std=c++23", "-O1", "-g0", "-pipe", "-DWOS_SELFTEST=1", "-fsanitize=address,undefined",
                "-fno-omit-frame-pointer", "-MD", "-MF", str(depfile), "-I", str(scratch / "include"),
                str(scratch / "harness.cpp"), "-o", str(binary)]
        compile_log = scratch / "command-0.log"
        row = runner.command(argv, compile_log, scratch, deadline, groups)
        result["commands"].append(row)
        if depfile.exists():
            result["emitted_dependency_pins_after"] = runner.dependency_pins(depfile)
            result["dependency_file"] = runner.pin(depfile)
        if row["exit"] != 0:
            print(compile_log.read_text(errors="replace")[-32768:], end="", flush=True)
            raise ValueError(f"actual compile/link failed with exit {row['exit']}")
        result["compiled_binary"] = runner.pin(binary)
        for index, vector in enumerate(vectors, 1):
            log = scratch / f"command-{index}.log"
            row = runner.command([str(binary), vector], log, scratch, deadline, groups)
            result["commands"].append(row)
            text = log.read_text(errors="replace")
            print(text[-32768:], end="", flush=True)
            pattern = (rf"^PASS case={re.escape(vector)} assertions=(\d+) failures=0 frames=0 slots=0 resident=0 "
                       r"registry=0 destroy_held=0 invalid_unlock=0 real_tmpfs=1 real_smallvec=1\n$")
            match = re.fullmatch(pattern, text)
            if row["exit"] != 0 or match is None or int(match[1]) == 0:
                result["errors"].append(f"actual compiled vector {vector} failed with exit {row['exit']}")
            else:
                result["runtime_census"].append({"case": vector, "assertions": int(match[1]), "exit": row["exit"]})
        result["pass"] = not result["errors"] and len(result["runtime_census"]) == len(vectors)
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            if staged_before:
                result["staged_sources_after"] = [runner.pin(path) for path in staged]
                result["staged_sources_held"] = result["staged_sources_after"] == staged_before
                if not result["staged_sources_held"]:
                    result["errors"].append("compiled staged source bytes changed")
            if before:
                result["source_after"] = [runner.pin(path) for path in watched]
                result["source_held"] = result["source_after"] == before
                if not result["source_held"]:
                    result["errors"].append("selected actual sources/compiler/shared runner changed")
            if scratch is not None:
                result["scratch_allocated_bytes_before_cleanup"] = runner.allocated(scratch)
                result["owned_sessions_pending"] = list(groups)
                if evidence is not None:
                    saved = []
                    for path in [*staged, *sorted(scratch.glob("command-*.log")), scratch / "dependencies.d"]:
                        if not path.is_file():
                            continue
                        if path.stat().st_size > runner.LOG_BUDGET:
                            raise ValueError("saved small evidence file exceeds 1MiB")
                        destination = evidence / path.relative_to(scratch)
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        with destination.open("xb") as stream:
                            stream.write(path.read_bytes())
                        saved.append(runner.pin(destination))
                    result["retained_small_evidence"] = saved
                info = scratch.stat()
                exact_owned = not scratch.is_symlink() and (info.st_dev, info.st_ino, info.st_uid) == scratch_identity
                if groups or not exact_owned:
                    result["errors"].append("owned session/scratch identity unproven; preserve small scratch")
                else:
                    shutil.rmtree(scratch)
                    result["scratch_removed"] = not os.path.lexists(scratch)
                    if not result["scratch_removed"]:
                        result["errors"].append("owned scratch remains after cleanup")
            result["elapsed_seconds"] = time.monotonic() - started
            if result["elapsed_seconds"] >= 60:
                result["errors"].append("whole 60s actual cleanup/publication budget expired")
        except (OSError, ValueError, TimeoutError) as error:
            result["errors"].append(f"cleanup: {type(error).__name__}: {error}")
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
    result["pass"] = result["pass"] and not result["errors"] and result["scratch_removed"]
    result["qualification"] = "PASS_ACTUAL_HOST_TMPFS_PAGE_OWNERSHIP_ONLY" if result["pass"] else "FAIL"
    if evidence is not None:
        result["evidence_directory"] = str(evidence)
        with (evidence / "result.json").open("x") as stream:
            json.dump(result, stream, sort_keys=True, indent=2)
            stream.write("\n")
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["pass"] and time.monotonic() < deadline else 1


if __name__ == "__main__":
    raise SystemExit(main())
