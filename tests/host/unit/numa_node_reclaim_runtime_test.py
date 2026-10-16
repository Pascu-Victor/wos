#!/usr/bin/env python3
"""Execute actual node-reclaim/cache/allocator bodies with explicit Host seams."""

import argparse, hashlib, json, os, resource, shlex, shutil, signal, subprocess, tempfile, time
from pathlib import Path

FIXTURE = r"""// Host context/address/lock seams; actual allocator, cache victim and coordinator
// bodies are inserted by the driver. No scheduled IRQ/SMP/reclaim proof.
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <vfs/stat.hpp>
#include <platform/mm/numa_policy.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
namespace mm = ker::mod::mm;
namespace phys = mm::phys;
namespace topo = ker::mod::acpi::numa;
namespace reclaim = mm::reclaim;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
static topo::Topology host_topology{};
static bool topology_published = true;
static mm::PageAllocator* host_allocator{};
static uint64_t physical_base{};
static unsigned physical_samples{};
namespace ker::mod::acpi::numa {
auto boot_topology() noexcept -> const Topology* { return topology_published ? &host_topology : nullptr; }
}  // namespace ker::mod::acpi::numa
namespace ker::mod::mm::addr {
static uint64_t host_hhdm{};
auto get_hhdm_offset() -> uint64_t { return host_hhdm; }
auto get_phys_pointer(uint64_t value) -> void* { return reinterpret_cast<void*>(value - host_hhdm); }
}  // namespace ker::mod::mm::addr
namespace ker::mod::mm::phys {
using NodeCounterSnapshot = node_counters::Snapshot;
using NodeCounterWorkspace = node_counters::Workspace;
// CACHE_TYPES
static PerCpuPageCache host_caches[2];
static PerCpuPageCache* per_cpu_caches = host_caches;
static std::array<PageCacheStatsCounters, topo::MAX_CPUS> page_cache_stats_by_cpu{};
static std::atomic<bool> per_cpu_ready{true};
static size_t num_cpus = 2;
static paging::PageZone host_zone{};
static paging::PageZone* zones = &host_zone;
static paging::PageZone* huge_page_zone{};
auto per_cpu_page_cache_enabled() -> bool { return true; }
auto node_counters_ready() noexcept -> bool { return topology_published && host_allocator != nullptr; }
auto snapshot_node_counters(NodeCounterSnapshot& out, NodeCounterWorkspace& scratch) noexcept -> NodeCounterError {
    if (sys::block_test_spinlock_depth != 0 || sys::host_mutex_depth != 0) std::abort();
    ++physical_samples;
    if (host_allocator == nullptr) return NodeCounterError::NOT_READY;
    scratch.views[0] = {host_allocator, physical_base};
    return node_counters::snapshot(scratch.views, 1, host_topology.memory.data(), host_topology.memory_range_count,
                                   host_topology.node_count, out, scratch);
}
auto get_free_mem_pages() -> uint64_t { return host_allocator ? host_allocator->free_count : 1024 * 1024; }
auto can_wait_for_reclaim() -> bool {
    auto* current = ker::mod::sched::get_current_task();
    return current != nullptr && current->preempt_disable_depth == 0;
}
auto snapshot_reclaim_zones(ReclaimZoneSnapshot* out, size_t capacity) -> size_t {
    if (!out || !capacity) return 0;
    out[0] = {0, 1024 * 1024, 0, 18};
    return 1;
}
auto page_ref_get(void* page) -> uint32_t {
    auto idx = (reinterpret_cast<uint64_t>(page) - host_allocator->base) / PAGE;
    return host_allocator->page_refcounts[idx].load();
}
void page_ref_inc(void* page) {
    auto idx = (reinterpret_cast<uint64_t>(page) - host_allocator->base) / PAGE;
    host_allocator->page_refcounts[idx].fetch_add(1);
}
auto page_reassign_owner(void* page, PhysicalPageOwner owner) -> bool {
    auto flags = host_allocator->lock_irq();
    auto ok = host_allocator->reassign_allocated_block_owner(page, owner);
    host_allocator->unlock_irq(flags);
    return ok;
}
void page_ref_dec(void* page) {
    auto idx = static_cast<uint32_t>((reinterpret_cast<uint64_t>(page) - host_allocator->base) / PAGE);
    if (host_allocator->page_refcounts[idx].fetch_sub(1) == 1) {
        auto flags = host_allocator->lock_irq();
        auto bytes = host_allocator->free_order0_at(idx);
        host_allocator->unlock_irq(flags);
        if (bytes != PAGE) std::abort();
    }
}
// ACTUAL_DRAIN
}  // namespace ker::mod::mm::phys
namespace ker::mod::mm::numa {
// ACTUAL_PHYSICAL_NODE
}
namespace ker::syscall::vmem {
namespace numa = ker::mod::mm::numa;
// ACTUAL_FILE_CACHE
}  // namespace ker::syscall::vmem
// ACTUAL_RECLAIM
namespace cache = ker::syscall::vmem;
namespace {
struct Arena {
    void* raw{};
    mm::PageAllocator* a{};
    std::vector<void*> retained;
    explicit Arena(uint16_t node_count = 3) {
        raw = std::aligned_alloc(PAGE, 4096 * PAGE);
        if (!raw) std::abort();
        std::memset(raw, 0, 4096 * PAGE);
        a = ::new (raw) mm::PageAllocator{};
        a->init(reinterpret_cast<uint64_t>(raw), 4096 * PAGE);
        host_allocator = a;
        physical_base = 0x40000000;
        mm::addr::host_hhdm = a->base - physical_base;
        host_topology = {};
        host_topology.node_count = node_count;  // Remaining nodes are genuinely memoryless.
        host_topology.cpu_count = 2;
        host_topology.cpu_nodes[0] = 0;
        host_topology.cpu_nodes[1] = 1;
        host_topology.memory_range_count = 2;
        host_topology.memory[0] = {physical_base, 2048 * PAGE, 0, 0};
        host_topology.memory[1] = {physical_base + 2048 * PAGE, 2048 * PAGE, 1, 0};
        mm::node_counters::BootstrapWorkspace boot{};
        mm::node_counters::AllocatorView view{a, physical_base};
        if (mm::node_counters::initialize(&view, 1, host_topology.memory.data(), 2, node_count, boot) != mm::NodeCounterError::NONE)
            std::abort();
        phys::host_zone = {nullptr, a, a->base, 4096 * PAGE, 4096, 0, "host"};
        topology_published = true;
    }
    ~Arena() {
        for (void* p : retained)
            if (phys::page_ref_get(p) != 0) phys::page_ref_dec(p);
        host_allocator = nullptr;
        a->~PageAllocator();
        std::free(raw);
    }
    auto alloc(uint16_t node) -> void* {
        auto range = host_topology.memory[node];
        auto flags = a->lock_irq();
        void* p = a->alloc_order0_span_in_range(physical_base, range.base, range.base + range.length, 1, PAGE,
                                                mm::PhysicalPageOwner::SELFTEST, 0);
        a->unlock_irq(flags);
        if (!p) return nullptr;
        retained.push_back(p);
        return p;
    }
    auto sample() -> mm::node_counters::Snapshot {
        mm::node_counters::Snapshot out{};
        mm::node_counters::Workspace work{};
        if (phys::snapshot_node_counters(out, work) != mm::NodeCounterError::NONE) std::abort();
        return out;
    }
    void fill(uint16_t node, uint64_t keep) {
        while (sample().nodes[node].buddy_free > keep)
            if (!alloc(node)) std::abort();
    }
    void cached(void* p, size_t cpu) {
        uint32_t idx;
        auto flags = a->lock_irq();
        bool ok = a->cache_allocated_order0(p, idx);
        a->unlock_irq(flags);
        if (!ok) std::abort();
        auto& c = phys::host_caches[cpu];
        c.pages[c.count++] = {a, idx};
    }
};
class NodeReclaim : public testing::Test {
   protected:
    std::unique_ptr<Arena> arena;
    ker::mod::sched::task::Task actor;
    void SetUp() override {
        for (auto& c : phys::host_caches) {
            c.count = 0;
            for (auto& p : c.pages) p = {};
        }
        for (auto& set : cache::g_file_mmap_cache)
            for (auto& entry : set.ways) entry = {};
        cache::g_file_mmap_cache_pages.store(0);
        cache::g_file_mmap_cache_scan_cursor.store(0);
        for (size_t i = 0; i < 65; ++i) {
            cache::g_file_mmap_cache_node_pages[i].store(0);
            cache::g_file_mmap_cache_node_cursor[i].store(0);
        }
        cache::g_file_mmap_cache_shrinker_registered.store(false);
        reclaim::node_pressure_ready.store(false);
        reclaim::pressure_node_count = 0;
        reclaim::node_pending_mask = 0;
        reclaim::next_node = 0;
        reclaim::coordinator.entry_count = 0;
        reclaim::coordinator.scan_owner.store(0);
        for (size_t i = 0; i < 64; ++i) {
            reclaim::node_jobs[i] = {};
            reclaim::node_config[i] = {};
            reclaim::node_cache_cursors[i] = {};
            reclaim::node_rounds[i] = 0;
        }
        for (auto& rows : reclaim::node_cooldowns)
            for (auto& row : rows) row = {};
        reclaim::worker_started.store(false);
        reclaim::worker_task.store(nullptr);
        reclaim::background_pending.store(false);
        reclaim::background_order.store(0);
        reclaim::background_target_pages.store(0);
        reclaim::explicit_job_slot.pending = false;
        ker::mod::sched::block_test_shutdown.store(false);
        ker::mod::sched::block_test_post_hook = {};
        ker::mod::sched::block_test_wake_hook = {};
        ker::mod::sched::block_test_block_hook = {};
        ker::mod::sched::task::block_test_create_worker = false;
        ker::mod::sched::task::block_test_created_worker = nullptr;
        ker::mod::sched::task::block_test_destroyed_workers = 0;
        physical_samples = 0;
        arena = std::make_unique<Arena>();
        ker::mod::sched::block_test_current_task = &actor;
        ASSERT_TRUE(reclaim::initialize_node_pressure());
    }
    void TearDown() override {
        ker::mod::sched::block_test_current_task = &actor;
        ker::mod::sched::block_test_post_hook = {};
        ker::mod::sched::block_test_wake_hook = {};
        ker::mod::sched::block_test_block_hook = {};
        // Host fixture halt only: production intentionally retains its module
        // reference until machine halt. No kernel teardown is modeled here.
        if (auto* worker = reclaim::worker_task.exchange(nullptr)) {
            EXPECT_EQ(worker->ref_count.load(), 2U);
            worker->release();
            delete worker;
            ker::mod::sched::task::block_test_created_worker = nullptr;
        }
        (void)cache::file_mmap_cache_reclaim(SIZE_MAX);
        for (uint64_t mask : {1ULL, 2ULL}) {
            phys::NodeCacheDrainCursor c{};
            (void)phys::drain_page_caches_for_nodes(mask, 128, 4096, c);
        }
        ker::mod::sched::block_test_current_task = nullptr;
        arena.reset();
    }
    auto req(uint64_t mask = 1) -> reclaim::ReclaimRequest {
        return {.context = reclaim::ReclaimContext::DIRECT,
                .priority = reclaim::ReclaimPriority::LOW,
                .target_pages = 1,
                .may_block = true,
                .may_io = true,
                .may_allocate = true,
                .node_mask = mask};
    }
    void insert(uint64_t id, void* p) {
        cache::FileMmapPageKey key{};
        key.exec_content_id = id;
        void* mapping = nullptr;
        cache::file_mmap_cache_insert_or_discard(key, p, &mapping);
        ASSERT_EQ(mapping, p);
        phys::page_ref_dec(mapping);  // Leave the cache's one retained reference.
    }
};
// The real start/notifier/worker loop below runs against finite Host create,
// post, wake-token and park seams. These exercise ordering and lifetime checks,
// not actual runqueue migration/IRQ/SMP semantics.
TEST_F(NodeReclaim, StartupCreateFailureLeavesUnreadyAndRetryable) {
    reclaim::start_worker();
    EXPECT_FALSE(reclaim::worker_started.load());
    EXPECT_EQ(reclaim::worker_task.load(), nullptr);
    EXPECT_EQ(ker::mod::sched::task::block_test_destroyed_workers, 0U);
    EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NOT_READY);
    EXPECT_EQ(reclaim::node_pending_mask, 0U);
}
TEST_F(NodeReclaim, FalsePostReleasesPrivateHoldAndCallsFullUnpublishedCleanup) {
    ker::mod::sched::task::block_test_create_worker = true;
    unsigned posts = 0;
    ker::mod::sched::block_test_post_hook = [&](auto* subject) {
        ++posts;
        EXPECT_EQ(subject->ref_count.load(), 2U);
        EXPECT_EQ(reclaim::worker_task.load(), nullptr);
        EXPECT_EQ(subject->kthread_entry, reclaim::reclaim_worker_main);
        return false;
    };
    reclaim::start_worker();
    EXPECT_EQ(posts, 1U);
    EXPECT_EQ(ker::mod::sched::task::block_test_destroyed_workers, 1U);
    EXPECT_EQ(ker::mod::sched::task::block_test_created_worker, nullptr);
    EXPECT_FALSE(reclaim::worker_started.load());
    EXPECT_EQ(reclaim::worker_task.load(), nullptr);
    EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NOT_READY);
}
TEST_F(NodeReclaim, SuccessfulPostPrecedesReadinessAndRetainsExactBootReference) {
    ker::mod::sched::task::block_test_create_worker = true;
    unsigned posts = 0, wakes = 0;
    ker::mod::sched::block_test_post_hook = [&](auto* subject) {
        ++posts;
        EXPECT_EQ(reclaim::worker_task.load(), nullptr);
        EXPECT_EQ(subject->ref_count.load(), 2U);
        EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NOT_READY);
        subject->scheduler_published.store(true);
        subject->sched_queue = decltype(subject->sched_queue)::RUNNABLE;
        return true;
    };
    ker::mod::sched::block_test_wake_hook = [&](auto*) { ++wakes; };
    reclaim::start_worker();
    auto* subject = reclaim::worker_task.load();
    ASSERT_NE(subject, nullptr);
    EXPECT_EQ(posts, 1U);
    EXPECT_EQ(subject->ref_count.load(), 2U);
    EXPECT_EQ(wakes, 0U);
    reclaim::start_worker();
    EXPECT_EQ(posts, 1U);
    EXPECT_EQ(ker::mod::sched::task::block_test_destroyed_workers, 0U);
}
TEST_F(NodeReclaim, PendingDuringPostIsRecheckedAfterPublicationWithoutLostWake) {
    ker::mod::sched::task::block_test_create_worker = true;
    unsigned wakes = 0;
    ker::mod::sched::block_test_post_hook = [&](auto* subject) {
        // An ordinary scheduled request may enqueue before worker readiness.
        EXPECT_EQ(reclaim::request_background_nodes(1, 7), reclaim::NodeReclaimStatus::NONE);
        EXPECT_EQ(reclaim::worker_task.load(), nullptr);
        subject->scheduler_published.store(true);
        return true;
    };
    ker::mod::sched::block_test_wake_hook = [&](auto* subject) {
        ++wakes;
        EXPECT_EQ(subject, reclaim::worker_task.load());
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
    };
    reclaim::start_worker();
    EXPECT_EQ(wakes, 1U);
    EXPECT_EQ(reclaim::node_pending_mask, 1U);
    EXPECT_EQ(reclaim::node_jobs[0].target, 7U);
}
TEST_F(NodeReclaim, EachLegacyStartupSourceIsRecheckedAfterSuccessfulPost) {
    ker::mod::sched::task::block_test_create_worker = true;
    unsigned posts = 0, wakes = 0;
    ker::mod::sched::block_test_post_hook = [&](auto* subject) {
        ++posts;
        EXPECT_EQ(reclaim::worker_task.load(), nullptr);
        if (posts == 1)
            reclaim::request_background(0, 1);
        else
            reclaim::explicit_job_slot.pending = true;
        subject->scheduler_published.store(true);
        return true;
    };
    ker::mod::sched::block_test_wake_hook = [&](auto*) {
        ++wakes;
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
    };
    reclaim::start_worker();
    EXPECT_EQ(wakes, 1U);
    // Fresh boot simulation between independent startup-source cases.
    auto* first = reclaim::worker_task.exchange(nullptr);
    ASSERT_NE(first, nullptr);
    first->release();
    delete first;
    reclaim::worker_started.store(false);
    reclaim::background_pending.store(false);
    reclaim::start_worker();
    EXPECT_EQ(wakes, 2U);
    EXPECT_EQ(posts, 2U);
}
TEST_F(NodeReclaim, InconsistentFalsePostOwnershipPanicsBeforeAnyCleanup) {
    ker::mod::sched::task::block_test_create_worker = true;
    for (unsigned kind = 0; kind < 3; ++kind) {
        EXPECT_DEATH(
            {
                ker::mod::sched::block_test_post_hook = [&](auto* subject) {
                    if (kind == 0)
                        subject->scheduler_published.store(true);
                    else if (kind == 1)
                        subject->ref_count.fetch_add(1);
                    else
                        subject->sched_list_owner.store(subject);
                    return false;
                };
                reclaim::start_worker();
            },
            "failed post retained unexpected worker ownership");
    }
    EXPECT_EQ(ker::mod::sched::task::block_test_destroyed_workers, 0U);
}
TEST_F(NodeReclaim, NotifierCoalescesOnlyScalarsAndWakesAfterQueueUnlock) {
    ker::mod::sched::task::block_test_create_worker = true;
    ker::mod::sched::block_test_post_hook = [](auto* subject) {
        subject->scheduler_published.store(true);
        return true;
    };
    reclaim::start_worker();
    auto* subject = reclaim::worker_task.load();
    ASSERT_NE(subject, nullptr);
    unsigned wakes = 0;
    auto samples = physical_samples;
    ker::mod::sched::block_test_wake_hook = [&](auto* target) {
        ++wakes;
        EXPECT_EQ(target, subject);
        EXPECT_EQ(target->ref_count.load(), 2U);
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
        EXPECT_EQ(physical_samples, samples);
    };
    actor.preempt_disable_depth = 1;  // Allowed entry; callbacks/reclaim remain deferred.
    EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NONE);
    EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NONE);
    actor.preempt_disable_depth = 0;
    EXPECT_EQ(wakes, 2U);
    EXPECT_EQ(physical_samples, samples);
    EXPECT_EQ(reclaim::node_pending_mask, 1U);
    EXPECT_EQ(reclaim::node_jobs[0].target, 1U);
    EXPECT_EQ(reclaim::node_jobs[0].stats.queued, 2U);
    EXPECT_EQ(reclaim::node_jobs[0].stats.callbacks, 0U);
    ker::mod::sched::block_test_current_task = nullptr;
    EXPECT_EQ(reclaim::notify_order0_node_failure(1), reclaim::NodeReclaimStatus::NOT_READY);
    ker::mod::sched::block_test_current_task = &actor;
    EXPECT_EQ(reclaim::notify_order0_node_failure(63), reclaim::NodeReclaimStatus::INVALID_MASK);
    EXPECT_EQ(wakes, 2U);
}
TEST_F(NodeReclaim, ShutdownRejectsNewAdmissionAndRetainedAddressSurvivesAdmittedRace) {
    ker::mod::sched::task::block_test_create_worker = true;
    ker::mod::sched::block_test_post_hook = [](auto* subject) {
        subject->scheduler_published.store(true);
        return true;
    };
    reclaim::start_worker();
    auto* subject = reclaim::worker_task.load();
    ASSERT_NE(subject, nullptr);
    unsigned wakes = 0;
    ker::mod::sched::block_test_wake_hook = [&](auto* target) {
        ++wakes;
        ker::mod::sched::block_test_shutdown.store(true);
        target->state.store(ker::mod::sched::task::TaskState::DEAD);
        EXPECT_EQ(target->ref_count.load(), 2U);
    };
    EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NONE);
    EXPECT_EQ(reclaim::worker_task.load(), subject);
    EXPECT_EQ(subject->ref_count.load(), 2U);
    EXPECT_EQ(reclaim::notify_order0_node_failure(1), reclaim::NodeReclaimStatus::NOT_READY);
    EXPECT_EQ(reclaim::request_background_nodes(2, 1), reclaim::NodeReclaimStatus::NOT_READY);
    EXPECT_EQ(reclaim::node_pending_mask, 1U);
    EXPECT_EQ(wakes, 1U);
}
struct StopHostWorker {};
TEST_F(NodeReclaim, NotificationImmediatelyBeforeParkSurvivesAsActualWorkerLoopToken) {
    ker::mod::sched::task::block_test_create_worker = true;
    ker::mod::sched::block_test_post_hook = [](auto* subject) {
        subject->scheduler_published.store(true);
        return true;
    };
    reclaim::start_worker();
    auto* subject = reclaim::worker_task.load();
    ASSERT_NE(subject, nullptr);
    ker::mod::sched::block_test_current_task = subject;
    unsigned parks = 0;
    ker::mod::sched::block_test_block_hook = [&] {
        if (++parks == 1) {
            EXPECT_EQ(reclaim::node_pending_mask, 0U);
            EXPECT_EQ(reclaim::notify_order0_node_failure(0), reclaim::NodeReclaimStatus::NONE);
            EXPECT_TRUE(subject->wakeup_pending.load());
        } else
            throw StopHostWorker{};
    };
    EXPECT_THROW(reclaim::reclaim_worker_main(), StopHostWorker);
    EXPECT_EQ(parks, 2U);
    EXPECT_EQ(reclaim::node_pending_mask, 0U);
    EXPECT_EQ(reclaim::node_jobs[0].stats.completed, 1U);
    EXPECT_FALSE(reclaim::node_jobs[0].stats.inflight);
}
TEST_F(NodeReclaim, NotificationAfterHostWaitingTransitionResumesActualWorkerLoop) {
    ker::mod::sched::task::block_test_create_worker = true;
    ker::mod::sched::block_test_post_hook = [](auto* subject) {
        subject->scheduler_published.store(true);
        return true;
    };
    reclaim::start_worker();
    auto* subject = reclaim::worker_task.load();
    ASSERT_NE(subject, nullptr);
    ker::mod::sched::block_test_current_task = subject;
    unsigned parks = 0;
    ker::mod::sched::block_test_block_hook = [&] {
        if (++parks == 1) {
            subject->host_waiting.store(true, std::memory_order_release);
            EXPECT_EQ(reclaim::notify_order0_node_failure(1), reclaim::NodeReclaimStatus::NONE);
            EXPECT_FALSE(subject->host_waiting.load(std::memory_order_acquire));
            EXPECT_TRUE(subject->wakeup_pending.load());
        } else
            throw StopHostWorker{};
    };
    EXPECT_THROW(reclaim::reclaim_worker_main(), StopHostWorker);
    EXPECT_EQ(parks, 2U);
    EXPECT_EQ(reclaim::node_jobs[1].stats.completed, 1U);
}
TEST_F(NodeReclaim, PreexpiredOuterDeadlineKeepsContinuationBeforeAnySample) {
    auto request = req(3);
    request.deadline_us = ker::mod::time::get_us();
    auto before = arena->sample();
    auto result = reclaim::run(request);
    EXPECT_TRUE(result.has_more);
    EXPECT_EQ(result.node_status, reclaim::NodeReclaimStatus::NONE);
    EXPECT_EQ(result.callbacks, 0U);
    EXPECT_EQ(result.sampled_buddy_pages, 0U);
    EXPECT_FALSE(result.made_progress);
    EXPECT_FALSE(result.scan_advanced);
    EXPECT_EQ(reclaim::node_rounds[0], 0U);
    EXPECT_EQ(reclaim::node_rounds[1], 0U);
    EXPECT_EQ(arena->sample().global.buddy_free, before.global.buddy_free);
    // The actual worker retries a has_more slice. Removing the deadline must
    // allow the same ordinary source to run rather than fabricate progress.
    auto* page = arena->alloc(0);
    ASSERT_NE(page, nullptr);
    insert(77, page);
    cache::file_mmap_cache_register_shrinker();
    request = req(1);
    uint64_t callbacks = 0;
    uint64_t progress = 0;
    for (unsigned turn = 0; turn < 32 && cache::file_mmap_cache_node_count(1) != 0; ++turn) {
        result = reclaim::run(request);
        callbacks += result.callbacks;
        progress += result.sampled_buddy_pages;
    }
    EXPECT_GE(callbacks, 1U);
    EXPECT_GE(progress, 1U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 0U);
}
TEST_F(NodeReclaim, InvalidScopeAndSnapshotPreserveOutputAndHaveNoEffects) {
    reclaim::NodeReclaimStatsSnapshot out{};
    out.queued = 123;
    EXPECT_FALSE(reclaim::snapshot_node_stats(63, out));
    EXPECT_EQ(out.queued, 123U);
    auto r = req(1ULL << 63);
    EXPECT_EQ(reclaim::run(r).node_status, reclaim::NodeReclaimStatus::INVALID_MASK);
    EXPECT_EQ(reclaim::request_background_nodes(1ULL << 63, 1), reclaim::NodeReclaimStatus::INVALID_MASK);
    EXPECT_EQ(reclaim::node_pending_mask, 0U);
    r = req();
    r.requested_order = 1;
    EXPECT_EQ(reclaim::run(r).node_status, reclaim::NodeReclaimStatus::UNSUPPORTED_ORDER);
    EXPECT_EQ(reclaim::run(req(4)).node_status, reclaim::NodeReclaimStatus::UNSATISFIABLE);
    reclaim::node_pressure_ready.store(false);
    EXPECT_FALSE(reclaim::snapshot_node_stats(0, out));
    EXPECT_EQ(out.queued, 123U);
    EXPECT_EQ(reclaim::run(req()).node_status, reclaim::NodeReclaimStatus::NOT_READY);
}
TEST_F(NodeReclaim, BusyAndUnsafeOutcomesRemainObservableWithoutPretendingToSamplePressure) {
    reclaim::NodeReclaimStatsSnapshot before{}, after{};
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, before));
    actor.preempt_disable_depth = 1;
    auto r = reclaim::run(req());
    actor.preempt_disable_depth = 0;
    EXPECT_TRUE(r.unsafe_context);
    EXPECT_EQ(r.node_status, reclaim::NodeReclaimStatus::UNSAFE_CONTEXT);
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, after));
    EXPECT_EQ(after.pressure, before.pressure);
    EXPECT_EQ(after.failures, 1U);
    reclaim::coordinator.scan_owner.store(0x777);
    r = reclaim::run(req());
    EXPECT_TRUE(r.lease_contended);
    EXPECT_EQ(r.node_status, reclaim::NodeReclaimStatus::BUSY);
    reclaim::coordinator.scan_owner.store(reclaim::owner_token());
    r = reclaim::run(req());
    EXPECT_TRUE(r.recursion_avoided);
    EXPECT_EQ(r.node_status, reclaim::NodeReclaimStatus::BUSY);
    reclaim::coordinator.scan_owner.store(0);
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, after));
    EXPECT_EQ(after.pressure, before.pressure);
    EXPECT_EQ(after.no_progress, 3U);
    EXPECT_EQ(after.callbacks, 0U);
}
TEST_F(NodeReclaim, ActualForeignCpuCacheFilteringTransfersOnlySelectedCachedFramesToBuddy) {
    void* local = arena->alloc(0);
    void* remote = arena->alloc(1);
    ASSERT_NE(local, nullptr);
    ASSERT_NE(remote, nullptr);
    arena->cached(local, 1);
    arena->cached(remote, 0);  // Frame affinity opposes CPU affinity.
    auto before = arena->sample();
    phys::NodeCacheDrainCursor cursor{};
    auto r = phys::drain_page_caches_for_nodes(1, 128, 4096, cursor);
    ASSERT_FALSE(r.failed);
    EXPECT_EQ(r.released, 1U);
    auto after = arena->sample();
    EXPECT_EQ(after.global.free_inclusive, before.global.free_inclusive);
    EXPECT_EQ(after.nodes[0].buddy_free, before.nodes[0].buddy_free + 1);
    EXPECT_EQ(after.nodes[0].cached, 0U);
    EXPECT_EQ(after.nodes[1].cached, 1U);
    EXPECT_EQ(phys::host_caches[0].count, 1U);
}
TEST_F(NodeReclaim, BudgetOneCursorReachesForeignCpuWithoutDroppingUnselectedEntries) {
    void* local = arena->alloc(0);
    void* remote = arena->alloc(1);
    arena->cached(remote, 0);
    arena->cached(local, 1);
    phys::NodeCacheDrainCursor cursor{};
    uint64_t released = 0;
    for (size_t i = 0; i < 34 && released == 0; ++i) {
        auto r = phys::drain_page_caches_for_nodes(1, 1, 1, cursor);
        EXPECT_LE(r.scanned, 1U);
        released += r.released;
    }
    EXPECT_EQ(released, 1U);
    EXPECT_EQ(phys::host_caches[0].count, 1U);
}
TEST_F(NodeReclaim, MalformedCacheMembershipRefusesWithoutDroppingEntry) {
    phys::host_caches[0].pages[0] = {reinterpret_cast<mm::PageAllocator*>(0x1234), 0};
    phys::host_caches[0].count = 1;
    phys::NodeCacheDrainCursor c{};
    auto r = phys::drain_page_caches_for_nodes(1, 4, 4, c);
    EXPECT_TRUE(r.failed);
    EXPECT_EQ(r.released, 0U);
    EXPECT_EQ(phys::host_caches[0].count, 1U);
    phys::host_caches[0].count = 0;
}
TEST_F(NodeReclaim, FailedCachedReferencePreflightPreservesMembershipAndReportsSourceFailure) {
    auto p = arena->alloc(0);
    arena->cached(p, 0);
    auto idx = static_cast<uint32_t>((reinterpret_cast<uint64_t>(p) - arena->a->base) / PAGE);
    arena->a->page_refcounts[idx].store(1);  // Deliberate malformed cached metadata.
    auto before = arena->sample();
    auto result = reclaim::run(req());
    EXPECT_EQ(result.node_status, reclaim::NodeReclaimStatus::SOURCE_FAILED);
    EXPECT_FALSE(result.made_progress);
    EXPECT_EQ(phys::host_caches[0].count, 1U);
    EXPECT_EQ(phys::host_caches[0].pages[0].page_idx, idx);
    auto after = arena->sample();
    EXPECT_EQ(before.nodes[0].cached, after.nodes[0].cached);
    EXPECT_EQ(before.global.free_inclusive, after.global.free_inclusive);
    reclaim::NodeReclaimStatsSnapshot stats{};
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, stats));
    EXPECT_EQ(stats.failures, 1U);
    arena->a->page_refcounts[idx].store(0);  // Restore fixture so real drain closes it.
}
TEST_F(NodeReclaim, SixtyFourNodeHighBitIsValidAndMemorylessWithoutShiftOverflow) {
    arena.reset();
    arena = std::make_unique<Arena>(64);
    reclaim::node_pressure_ready.store(false);
    ASSERT_TRUE(reclaim::initialize_node_pressure());
    EXPECT_EQ(reclaim::request_background_nodes(1ULL << 63, 1), reclaim::NodeReclaimStatus::NONE);
    EXPECT_TRUE(reclaim::service_one_node_job());
    reclaim::NodeReclaimStatsSnapshot row{};
    ASSERT_TRUE(reclaim::snapshot_node_stats(63, row));
    EXPECT_EQ(row.node, 63U);
    EXPECT_EQ(row.completed, 1U);
    EXPECT_EQ(row.status, reclaim::NodeReclaimStatus::UNSATISFIABLE);
    EXPECT_EQ(reclaim::node_pending_mask, 0U);
}
TEST_F(NodeReclaim, StableUnattributedInsertionNeverBecomesValidNodeDebtAfterPublication) {
    auto p = arena->alloc(0);
    topology_published = false;
    insert(1, p);
    topology_published = true;
    EXPECT_EQ(cache::g_file_mmap_cache_node_pages[64].load(), 1U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 0U);
    auto r = cache::file_mmap_cache_reclaim_bounded(4, cache::FILE_MMAP_CACHE_PAGES, 1);
    EXPECT_EQ(r.reclaimed, 0U);
    EXPECT_EQ(cache::file_mmap_cache_reclaim(1), 1U);
    EXPECT_EQ(cache::g_file_mmap_cache_node_pages[64].load(), 0U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 0U);
}
TEST_F(NodeReclaim, FilteredActualCacheBodySkipsForeignVictimAndCountsOnlyTargetNode) {
    insert(17, arena->alloc(1));
    insert(19, arena->alloc(0));
    auto r = req();
    EXPECT_EQ(cache::file_mmap_cache_reclaim_count(nullptr, r).reclaimable, 1U);
    auto result = cache::file_mmap_cache_reclaim_bounded(128, cache::FILE_MMAP_CACHE_PAGES, 1);
    EXPECT_EQ(result.reclaimed, 1U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 0U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(2), 1U);
    EXPECT_EQ(cache::g_file_mmap_cache_pages.load(), 1U);
}
TEST_F(NodeReclaim, ReplacementAcrossNodesAndDuplicateDiscardMaintainExactOccupancy) {
    std::vector<uint64_t> ids;
    cache::FileMmapPageKey probe{};
    auto const bucket = cache::file_mmap_key_hash(probe) & (cache::FILE_MMAP_CACHE_SET_COUNT - 1);
    for (uint64_t id = 0; ids.size() < 6 && id < 1000000; ++id) {
        probe.exec_content_id = id;
        if ((cache::file_mmap_key_hash(probe) & (cache::FILE_MMAP_CACHE_SET_COUNT - 1)) == bucket) ids.push_back(id);
    }
    ASSERT_EQ(ids.size(), 6U);
    for (size_t i = 0; i < 4; ++i) insert(ids[i], arena->alloc(i % 2));
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 2U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(2), 2U);
    insert(ids[4], arena->alloc(1));  // Oldest node0 -> node1, global count unchanged.
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 1U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(2), 3U);
    insert(ids[5], arena->alloc(0));  // Next oldest node1 -> node0.
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 2U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(2), 2U);
    auto fresh = arena->alloc(0);
    cache::FileMmapPageKey key{};
    key.exec_content_id = ids[3];
    void* mapping = nullptr;
    EXPECT_EQ(cache::file_mmap_cache_insert_or_discard(key, fresh, &mapping), cache::FileMmapCacheInsertResult::DUPLICATE);
    EXPECT_NE(mapping, fresh);
    EXPECT_EQ(phys::page_ref_get(fresh), 0U);
    phys::page_ref_dec(mapping);
    EXPECT_EQ(cache::g_file_mmap_cache_pages.load(), 4U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 2U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(2), 2U);
}
TEST_F(NodeReclaim, SharedMappingRefPreventsFalsePhysicalProgressAfterActualCacheEviction) {
    auto p = arena->alloc(0);
    insert(21, p);
    phys::page_ref_inc(p);
    auto before = arena->sample();
    cache::file_mmap_cache_register_shrinker();
    auto r = req();
    r.scan_budget_units = cache::FILE_MMAP_CACHE_PAGES;
    reclaim::ReclaimRunResult result{};
    uint64_t evicted = 0;
    // The real descriptor caps each scan at4096 slots even if the request asks
    // for more. Advance the genuine finite sweep before asserting eviction.
    for (size_t turn = 0; turn < 32 && cache::file_mmap_cache_node_count(1) != 0; ++turn) {
        result = reclaim::run(r);
        evicted += result.reclaimed_units;
        EXPECT_EQ(result.reclaimed_pages, 0U);
        EXPECT_EQ(result.sampled_buddy_pages, 0U);
        EXPECT_FALSE(result.made_progress);
    }
    EXPECT_EQ(evicted, 1U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 0U);
    auto after = arena->sample();
    EXPECT_EQ(after.nodes[0].buddy_free, before.nodes[0].buddy_free);
    EXPECT_EQ(phys::page_ref_get(p), 1U);
    phys::page_ref_dec(p);
    EXPECT_EQ(arena->sample().nodes[0].buddy_free, before.nodes[0].buddy_free + 1);
}
TEST_F(NodeReclaim, PerNodeQueueRoundRobinAndCompletionKeepOtherNodePending) {
    EXPECT_EQ(reclaim::request_background_nodes(3, 1), reclaim::NodeReclaimStatus::NONE);
    EXPECT_TRUE(reclaim::service_one_node_job());
    reclaim::NodeReclaimStatsSnapshot a{}, b{};
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, a));
    ASSERT_TRUE(reclaim::snapshot_node_stats(1, b));
    EXPECT_EQ(a.completed, 1U);
    EXPECT_EQ(b.completed, 0U);
    EXPECT_TRUE(b.pending);
    EXPECT_EQ(reclaim::request_background_nodes(1, 8), reclaim::NodeReclaimStatus::NONE);
    EXPECT_TRUE(reclaim::service_one_node_job());
    ASSERT_TRUE(reclaim::snapshot_node_stats(1, b));
    EXPECT_EQ(b.completed, 1U);
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, a));
    EXPECT_TRUE(a.pending);
    EXPECT_EQ(a.queued, 2U);
}
static unsigned unsupported_count{}, unsupported_scan{}, supported_scan{};
auto forbidden_count(void*, const reclaim::ReclaimRequest&) -> reclaim::ReclaimCount {
    ++unsupported_count;
    return {.reclaimable = 1};
}
auto forbidden_scan(void*, const reclaim::ReclaimRequest&) -> reclaim::ReclaimScanResult {
    ++unsupported_scan;
    return {.scanned = 1, .reclaimed = 1};
}
auto units_scan(void*, const reclaim::ReclaimRequest&) -> reclaim::ReclaimScanResult {
    ++supported_scan;
    return {.scanned = 1, .reclaimed = 1};
}
TEST_F(NodeReclaim, UnsupportedSkippedBeforeCountNativeUnitsDoNotBecomeBuddyAndGlobalRemainsLegacy) {
    unsupported_count = unsupported_scan = supported_scan = 0;
    reclaim::Shrinker unconverted{.name = "unconverted",
                                  .unit = reclaim::ReclaimUnit::PAGES,
                                  .scan_unit = reclaim::ReclaimUnit::PAGES,
                                  .min_priority = reclaim::ReclaimPriority::LOW,
                                  .max_batch_units = 1,
                                  .max_scan_units = 1,
                                  .count = forbidden_count,
                                  .scan = forbidden_scan};
    ASSERT_TRUE(reclaim::register_shrinker(unconverted));
    auto converted = unconverted;
    converted.name = "converted";
    converted.scan = units_scan;
    converted.supports_node_filter = true;
    ASSERT_TRUE(reclaim::register_shrinker(converted));
    reclaim::GlobalStatsSnapshot global_before{}, global_after{};
    reclaim::get_global_stats(global_before);
    auto r = req();
    auto result = reclaim::run(r);
    reclaim::get_global_stats(global_after);
    EXPECT_EQ(global_before.callbacks, global_after.callbacks);
    EXPECT_EQ(global_before.reclaimed_pages, global_after.reclaimed_pages);
    EXPECT_EQ(global_before.reported_pages, global_after.reported_pages);
    EXPECT_EQ(global_before.no_progress, global_after.no_progress);
    EXPECT_EQ(unsupported_scan, 0U);
    EXPECT_EQ(unsupported_count, 1U);
    EXPECT_EQ(supported_scan, 1U);
    EXPECT_FALSE(result.made_progress);
    EXPECT_EQ(result.sampled_buddy_pages, 0U);
    EXPECT_EQ(result.reclaimed_units, 1U);
    r.node_mask = 0;
    result = reclaim::run(r);
    EXPECT_EQ(unsupported_scan, 1U);
    EXPECT_TRUE(result.made_progress);
    EXPECT_EQ(result.callbacks, 2U);
}
static bool enqueue_on_scan{};
auto notify_scan(void*, const reclaim::ReclaimRequest&) -> reclaim::ReclaimScanResult {
    if (enqueue_on_scan) {
        enqueue_on_scan = false;
        EXPECT_EQ(reclaim::request_background_nodes(1, 7), reclaim::NodeReclaimStatus::NONE);
    }
    return {.scanned = 1};
}
TEST_F(NodeReclaim, InflightNotificationSurvivesCompletionAndDescriptorSupportIsPartOfIdentity) {
    reclaim::Shrinker d{.name = "notify",
                        .unit = reclaim::ReclaimUnit::PAGES,
                        .scan_unit = reclaim::ReclaimUnit::PAGES,
                        .min_priority = reclaim::ReclaimPriority::LOW,
                        .max_batch_units = 1,
                        .max_scan_units = 1,
                        .count = forbidden_count,
                        .scan = notify_scan,
                        .supports_node_filter = true};
    ASSERT_TRUE(reclaim::register_shrinker(d));
    auto different = d;
    different.supports_node_filter = false;
    EXPECT_FALSE(reclaim::register_shrinker(different));
    arena->fill(0, 1);
    enqueue_on_scan = true;
    EXPECT_EQ(reclaim::request_background_nodes(1, 1), reclaim::NodeReclaimStatus::NONE);
    EXPECT_TRUE(reclaim::service_one_node_job());
    reclaim::NodeReclaimStatsSnapshot row{};
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, row));
    EXPECT_EQ(row.completed, 1U);
    EXPECT_EQ(row.queued, 2U);
    EXPECT_TRUE(row.pending);
    EXPECT_FALSE(row.inflight);
    EXPECT_TRUE(reclaim::service_one_node_job());
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, row));
    EXPECT_EQ(row.completed, 2U);
    EXPECT_FALSE(row.pending);
}
TEST_F(NodeReclaim, CooldownForOneNodeDoesNotSuppressAnotherOrGlobalScope) {
    supported_scan = 0;
    unsupported_count = 0;
    reclaim::Shrinker d{.name = "nodes",
                        .unit = reclaim::ReclaimUnit::PAGES,
                        .scan_unit = reclaim::ReclaimUnit::PAGES,
                        .min_priority = reclaim::ReclaimPriority::LOW,
                        .max_batch_units = 1,
                        .max_scan_units = 1,
                        .count = forbidden_count,
                        .scan = units_scan,
                        .supports_node_filter = true};
    ASSERT_TRUE(reclaim::register_shrinker(d));
    (void)reclaim::run(req(1));
    EXPECT_EQ(supported_scan, 1U);
    (void)reclaim::run(req(1));
    EXPECT_EQ(supported_scan, 1U);
    (void)reclaim::run(req(2));
    EXPECT_EQ(supported_scan, 2U);
    (void)reclaim::run(req(0));
    EXPECT_EQ(supported_scan, 3U);
}
TEST_F(NodeReclaim, SparseLastSlotWorkerSearchSurvivesZeroPhysicalProgressTurns) {
    auto p = arena->alloc(0);
    auto& entry = cache::g_file_mmap_cache.back().ways.back();
    entry.page = p;
    entry.physical_node = 0;
    cache::file_mmap_cache_node_add(0);
    cache::g_file_mmap_cache_pages.fetch_add(1);
    arena->fill(0, 1);
    cache::file_mmap_cache_register_shrinker();
    EXPECT_EQ(reclaim::request_background_nodes(1, 1), reclaim::NodeReclaimStatus::NONE);
    unsigned turns = 0;
    while (reclaim::node_job_pending() && turns < 65) {
        ASSERT_TRUE(reclaim::service_one_node_job());
        ++turns;
    }
    EXPECT_LE(turns, 64U);
    EXPECT_EQ(cache::file_mmap_cache_node_count(1), 0U);
    EXPECT_EQ(phys::page_ref_get(p), 0U);
    reclaim::NodeReclaimStatsSnapshot out{};
    ASSERT_TRUE(reclaim::snapshot_node_stats(0, out));
    EXPECT_EQ(out.completed, 1U);
    EXPECT_GE(out.callbacks, 32U);
    EXPECT_EQ(out.failures, 0U);
    EXPECT_GE(out.sampled_buddy_pages, 1U);
}
TEST_F(NodeReclaim, EntryPrivateStorageGrowthIsEightBytesAndOneMibTotal) {
    struct OldEntry {
        cache::FileMmapPageKey key{};
        void* page = nullptr;
        uint64_t last_used = 0;
    };
    EXPECT_EQ(sizeof(cache::FileMmapPageCacheEntry) - sizeof(OldEntry), 8U);
    EXPECT_EQ((sizeof(cache::FileMmapPageCacheEntry) - sizeof(OldEntry)) * cache::FILE_MMAP_CACHE_PAGES, 1024 * 1024U);
    std::printf("ENTRY_LAYOUT old=%zu new=%zu entries=%zu delta=%zu\n", sizeof(OldEntry), sizeof(cache::FileMmapPageCacheEntry),
                cache::FILE_MMAP_CACHE_PAGES, (sizeof(cache::FileMmapPageCacheEntry) - sizeof(OldEntry)) * cache::FILE_MMAP_CACHE_PAGES);
}
}  // namespace
"""


def body(text, marker, ending="}"):
    start = text.index(marker)
    first = text.index("{", start)
    depth = 0
    quote = None
    line = False
    block = False
    i = first
    while i < len(text):
        c = text[i]
        pair = text[i : i + 2]
        if line:
            if c == "\n":
                line = False
        elif block:
            if pair == "*/":
                block = False
                i += 1
        elif quote:
            if c == "\\":
                i += 1
            elif c == quote:
                quote = None
        elif pair == "//":
            line = True
            i += 1
        elif pair == "/*":
            block = True
            i += 1
        elif c in ['"', "'"]:
            quote = c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + len(ending)
                assert text[i:end] == ending
                return text[start:end]
        i += 1
    raise ValueError("unclosed " + marker)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host-build", type=Path, required=True)
    ap.add_argument("--gtest-main", type=Path, required=True)
    ap.add_argument("--gtest-lib", type=Path, required=True)
    ap.add_argument(
        "--source-root", type=Path, default=Path(__file__).resolve().parents[3]
    )
    ap.add_argument("--host-shim", type=Path)
    ap.add_argument("--overlay", type=Path)
    ap.add_argument("--result-dir", type=Path)
    ap.add_argument("--old-start-source", type=Path)
    ap.add_argument("--old-file-source", type=Path)
    ap.add_argument("--old-reclaim-source", type=Path)
    ap.add_argument("--negative-controls", action="store_true")
    a = ap.parse_args()
    R = a.source_root.resolve()
    replacements = json.loads(a.overlay.read_text()) if a.overlay else {}
    allowed = {
        "modules/kern/src/" + p
        for p in [
            "platform/mm/reclaim.cpp",
            "platform/mm/reclaim_callbacks.cpp",
            "platform/mm/reclaim.hpp",
            "platform/mm/reclaim_policy.hpp",
            "platform/mm/phys.hpp",
            "platform/mm/phys.opt.cpp",
            "platform/mm/numa_alloc.cpp",
            "syscalls_impl/vmem/sys_vmem.cpp",
        ]
    }
    if not isinstance(replacements, dict) or not set(replacements) <= allowed:
        ap.error("invalid overlay")

    def source(rel):
        return Path(replacements.get(rel, str(R / rel))).resolve()

    def pin(p):
        p = Path(p).resolve()
        s = p.stat()
        b = p.read_bytes()
        t = p.stat()
        assert (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns) == (
            t.st_dev,
            t.st_ino,
            t.st_size,
            t.st_mtime_ns,
            t.st_ctime_ns,
        )
        return dict(path=str(p), bytes=len(b), sha256=hashlib.sha256(b).hexdigest())

    if a.result_dir:
        a.result_dir.mkdir(exist_ok=False)
    outer = None
    if not a.result_dir:
        outer = tempfile.TemporaryDirectory(prefix="node-reclaim-result-")
        OUT = Path(outer.name)
    else:
        OUT = a.result_dir.resolve()
    T = Path(tempfile.mkdtemp(prefix="scratch-", dir=OUT))
    env = dict(
        os.environ,
        TMPDIR=str(T),
        TMP=str(T),
        TEMP=str(T),
        ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
        UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1",
    )
    started = time.monotonic()
    commands = []
    errors = []
    owned = []
    deps = set()
    selected = []
    inputs = []
    after = []
    reports = []

    def limits():
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (16 * 1024**2, 16 * 1024**2))

    def absent(pid):
        try:
            os.killpg(pid, 0)
            return False
        except ProcessLookupError:
            return True

    def run(label, argv, expected=0):
        remaining = 120 - (time.monotonic() - started)
        used = sum(p.stat().st_blocks * 512 for p in T.rglob("*") if p.is_file())
        if (
            remaining <= 0
            or used > 64 * 1024**2
            or shutil.disk_usage(R).free < 40 * 1024**3
        ):
            raise RuntimeError("time/storage budget")
        log = OUT / (label + ".log")
        begin = time.monotonic()
        with log.open("wb") as f:
            p = subprocess.Popen(
                [str(x) for x in argv],
                cwd=R,
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                preexec_fn=limits,
            )
            owned.append(p.pid)
            try:
                rc = p.wait(timeout=min(30, remaining))
            except BaseException:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
                raise
        if not absent(p.pid):
            os.killpg(p.pid, signal.SIGKILL)
        commands.append(
            dict(
                label=label,
                argv=[str(x) for x in argv],
                exit=rc,
                seconds=time.monotonic() - begin,
                log=pin(log),
                leader_reaped=True,
                owned_group_absent=absent(p.pid),
                expected_exit=expected,
            )
        )
        if rc != expected:
            raise RuntimeError(
                label
                + " unexpected exit "
                + str(rc)
                + " "
                + log.read_text(errors="replace")[-8000:]
            )

    def depfiles(p):
        return [
            Path(x).resolve()
            for x in shlex.split(p.read_text().replace("\\\n", " ").split(":", 1)[1])
        ]

    try:
        K = R / "modules/kern/src"
        P = source("modules/kern/src/platform/mm/phys.opt.cpp").read_text()
        V = source("modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
        N = source("modules/kern/src/platform/mm/numa_alloc.cpp").read_text()
        H = source("modules/kern/src/platform/mm/phys.hpp").read_text()
        shim = T / "shim"
        (shim / "platform/mm").mkdir(parents=True)
        (shim / "platform/sys").mkdir(parents=True)
        # Only actual physical context/address/reference and host mutex boundaries are seams.
        (shim / "platform/mm/phys.hpp").write_text(
            """#pragma once
#include <cstddef>
#include <cstdint>
#include <platform/mm/page_alloc.hpp>
namespace ker::mod::mm::phys {
using NodeCounterSnapshot=node_counters::Snapshot;using NodeCounterWorkspace=node_counters::Workspace;
struct ReclaimZoneSnapshot {uint64_t zone,total_pages,free_pages;int largest_free_order;};
auto get_free_mem_pages()->uint64_t;auto can_wait_for_reclaim()->bool;
auto snapshot_reclaim_zones(ReclaimZoneSnapshot*,size_t)->size_t;
auto snapshot_node_counters(NodeCounterSnapshot&,NodeCounterWorkspace&) noexcept->NodeCounterError;
"""
            + H[
                H.index("struct NodeCacheDrainCursor") : H.index(
                    "auto can_wait_for_reclaim()"
                )
            ]
            + """\nauto page_ref_get(void*)->uint32_t;void page_ref_inc(void*);void page_ref_dec(void*);auto page_reassign_owner(void*,PhysicalPageOwner)->bool;
}\n"""
        )
        (shim / "platform/sys/mutex.hpp").write_text("""#pragma once
#include <mutex>
namespace ker::mod::sys {inline thread_local unsigned host_mutex_depth{};class Mutex {std::mutex m;public:void lock(){m.lock();++host_mutex_depth;}void unlock(){--host_mutex_depth;m.unlock();}};}
""")
        fixture = FIXTURE
        types = "\n".join(
            body(P, x, "};")
            for x in [
                "struct CachedOrder0Page",
                "struct PerCpuPageCache",
                "struct alignas(64) PageCacheStatsCounters",
            ]
        )
        fixture = fixture.replace("// CACHE_TYPES", types)
        fixture = fixture.replace(
            "// ACTUAL_DRAIN", body(P, "auto drain_page_caches_for_nodes(")
        )
        fixture = fixture.replace(
            "// ACTUAL_PHYSICAL_NODE",
            body(N, "auto node_for_address(") + "\n" + body(N, "auto physical_node("),
        )
        cachetypes = "\n".join(
            body(V, x, "};")
            for x in [
                "enum class FileMmapCacheInsertResult",
                "struct FileMmapPageKey",
                "struct FileMmapPageCacheEntry",
                "struct FileMmapPageCacheSet",
            ]
        )
        lo = V.index("constexpr size_t FILE_MMAP_CACHE_SET_COUNT")
        hi = V.index("std::atomic<bool> g_file_mmap_cache_shrinker_registered")
        hi = V.index(";", hi) + 1
        functions = [
            "auto file_mmap_key_equal(",
            "auto file_mmap_key_hash(",
            "auto file_mmap_cache_lookup(",
            "void release_file_mmap_cache_page(",
            "auto file_mmap_cache_insert_or_discard(",
            "auto file_mmap_cache_reclaim(size_t",
            "auto file_mmap_cache_reclaim_bounded(",
            "auto file_mmap_cache_reclaim_count(",
            "auto file_mmap_cache_reclaim_scan(",
            "void file_mmap_cache_register_shrinker(",
        ]
        cachebody = (
            cachetypes
            + "\n"
            + V[lo:hi]
            + "\n"
            + body(V, "struct FileMmapBoundedReclaimResult", "};")
            + "\n"
            + "\n".join(body(V, x) for x in functions)
        )
        fixture = fixture.replace("// ACTUAL_FILE_CACHE", cachebody)
        fixture = fixture.replace(
            "// ACTUAL_RECLAIM",
            '#include "'
            + str(source("modules/kern/src/platform/mm/reclaim.cpp"))
            + '"',
        )
        test = T / "actual.cpp"
        test.write_text(fixture)
        DB = a.host_build.resolve() / "compile_commands.json"
        rows = json.loads(DB.read_text())
        row = next(
            r
            for r in rows
            if r["file"].endswith("/tests/host/unit/page_allocator_interval_test.cpp")
            and "page_allocator_interval_test.dir" in r["command"]
        )
        args = shlex.split(row["command"])
        base = [
            args[0],
            "-I" + str(shim),
            "-I"
            + str(
                a.host_shim.resolve()
                if a.host_shim
                else R / "tests/host/unit/block_request_runtime_shim"
            ),
            "-I" + str(R / "tests/host/unit/block_request_runtime_shim"),
            "-I" + str(R / "tests/host/kern_shim"),
            "-I"
            + str(
                source("modules/kern/src/platform/mm/reclaim.hpp").parent.parent.parent
            ),
            "-I" + str(source("modules/kern/src/platform/mm/reclaim.hpp").parent),
        ]
        i = 1
        while i < len(args):
            if args[i] in ["-o", "-MF", "-MT", "-MQ"]:
                i += 2
                continue
            if args[i] in ["-c", "-MD", "-MMD", "-MP"] or args[i] == row["file"]:
                i += 1
                continue
            base.append(args[i])
            i += 1
        assert "-fsanitize=address,undefined" in base and "-DWOS_HOST_TEST=1" in base
        libs = [a.gtest_main.resolve(), a.gtest_lib.resolve()]
        files = [
            source("modules/kern/src/platform/mm/reclaim.cpp"),
            source("modules/kern/src/platform/mm/reclaim.hpp"),
            source("modules/kern/src/platform/mm/phys.opt.cpp"),
            source("modules/kern/src/platform/mm/phys.hpp"),
            source("modules/kern/src/platform/mm/numa_alloc.cpp"),
            source("modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp"),
            R / "modules/kern/src/platform/mm/page_alloc.cpp",
            R / "modules/kern/src/platform/mm/page_alloc.hpp",
            source("modules/kern/src/platform/mm/reclaim_callbacks.cpp"),
            Path(__file__).resolve(),
            DB,
        ] + libs
        if a.old_start_source:
            files.append(a.old_start_source.resolve())
        if a.old_file_source:
            files.append(a.old_file_source.resolve())
        if a.old_reclaim_source:
            files.append(a.old_reclaim_source.resolve())
        selected = [pin(p) for p in files]
        for name, src in [
            ("test", test),
            ("allocator", R / "modules/kern/src/platform/mm/page_alloc.cpp"),
            (
                "callback-adapter",
                source("modules/kern/src/platform/mm/reclaim_callbacks.cpp"),
            ),
        ]:
            dep = T / (name + ".d")
            run("deps-" + name, base + ["-M", "-MF", str(dep), "-MT", name, str(src)])
            deps.update(p for p in depfiles(dep) if not p.is_relative_to(T))
        inputs = [pin(p) for p in sorted(deps)]
        allocator = T / "allocator.o"
        run(
            "compile-allocator",
            base
            + [
                "-c",
                str(R / "modules/kern/src/platform/mm/page_alloc.cpp"),
                "-o",
                str(allocator),
            ],
        )
        adapter_object = T / "callback-adapter.o"
        run(
            "compile-callback-adapter",
            base
            + [
                "-c",
                str(source("modules/kern/src/platform/mm/reclaim_callbacks.cpp")),
                "-o",
                str(adapter_object),
            ],
        )

        def execute(kind, text, expected=0, pattern=None, provenance=False):
            src = T / (kind + ".cpp")
            src.write_text(text)
            obj = T / (kind + ".o")
            binary = T / kind
            extra = ["-DWOS_PHYS_ALLOC_CALLER_STATS=1"] if provenance else []
            # Rebuild the real allocator for the provenance metadata profile too.
            alloc = allocator
            if provenance:
                alloc = T / "allocator-provenance.o"
                run(
                    "compile-allocator-provenance",
                    base
                    + extra
                    + [
                        "-c",
                        str(R / "modules/kern/src/platform/mm/page_alloc.cpp"),
                        "-o",
                        str(alloc),
                    ],
                )
            run("compile-" + kind, base + extra + ["-c", str(src), "-o", str(obj)])
            run(
                "link-" + kind,
                [
                    base[0],
                    "-fsanitize=address,undefined",
                    str(obj),
                    str(alloc),
                    str(adapter_object),
                ]
                + [str(p) for p in libs]
                + ["-pthread", "-o", str(binary)],
            )
            report = OUT / (kind + ".json")
            cmd = [str(binary), "--gtest_output=json:" + str(report)]
            if pattern:
                cmd += ["--gtest_filter=" + pattern]
            run("run-" + kind, cmd, expected)
            j = json.loads(report.read_text())
            reports.append(
                dict(
                    kind=kind,
                    report=pin(report),
                    tests=j["tests"],
                    failures=j["failures"],
                    expected_runtime_failure=expected != 0,
                )
            )
            if expected == 0 and j["failures"] != 0:
                raise RuntimeError("gtest failures")
            if expected != 0 and int(j["failures"]) < 1:
                raise RuntimeError("missing negative failure")

        execute("candidate", fixture)
        execute("candidate-provenance", fixture, provenance=True)
        if a.old_start_source:
            production = source("modules/kern/src/platform/mm/reclaim.cpp").read_text()
            old = a.old_start_source.read_text()
            previous = T / "reclaim-old-start.cpp"
            previous.write_text(
                production.replace(
                    body(production, "void start_worker()"),
                    body(old, "void start_worker()"),
                    1,
                )
            )
            execute(
                "genuine-old-start",
                fixture.replace(
                    str(source("modules/kern/src/platform/mm/reclaim.cpp")),
                    str(previous),
                ),
                1,
                "NodeReclaim.SuccessfulPostPrecedesReadinessAndRetainsExactBootReference",
            )
        if a.negative_controls:
            if a.old_reclaim_source:
                production = source(
                    "modules/kern/src/platform/mm/reclaim.cpp"
                ).read_text()
                original = a.old_reclaim_source.read_text()
                old_function = body(original, "auto run_nodes(")
                old_reclaim = T / "reclaim-old-outer.cpp"
                old_reclaim.write_text(
                    production.replace(
                        body(production, "auto run_nodes("), old_function, 1
                    )
                )
                execute(
                    "genuine-old-outer-deadline",
                    fixture.replace(
                        str(source("modules/kern/src/platform/mm/reclaim.cpp")),
                        str(old_reclaim),
                    ),
                    1,
                    "NodeReclaim.PreexpiredOuterDeadlineKeepsContinuationBeforeAnySample",
                )
            if a.old_file_source:
                old = a.old_file_source.read_text()
                oldbody = fixture
                for marker in [
                    "auto file_mmap_cache_reclaim_bounded(",
                    "auto file_mmap_cache_reclaim_count(",
                    "auto file_mmap_cache_reclaim_scan(",
                ]:
                    oldbody = oldbody.replace(body(V, marker), body(old, marker), 1)
                adapter = """\nauto file_mmap_cache_reclaim_bounded(size_t pages,size_t scans,uint64_t) -> FileMmapBoundedReclaimResult {return file_mmap_cache_reclaim_bounded(pages,scans);}\n"""
                at = oldbody.index("auto file_mmap_cache_reclaim_count(")
                oldbody = oldbody[:at] + adapter + oldbody[at:]
                execute(
                    "genuine-old-file-bodies",
                    oldbody,
                    1,
                    "NodeReclaim.FilteredActualCacheBodySkipsForeignVictimAndCountsOnlyTargetNode",
                )
            mutant = fixture.replace(
                "} else if ((node_mask & (uint64_t{1} << node)) != 0) {",
                "} else if (true) {",
                1,
            )
            assert mutant != fixture
            execute(
                "mutant-unfiltered-pcpu",
                mutant,
                1,
                "NodeReclaim.ActualForeignCpuCacheFilteringTransfersOnlySelectedCachedFramesToBuddy",
            )
            old = "!shrinker.supports_node_filter ||"
            assert old in source("modules/kern/src/platform/mm/reclaim.cpp").read_text()
            prod = source("modules/kern/src/platform/mm/reclaim.cpp").read_text()
            mut = T / "reclaim-mutant.cpp"
            mut.write_text(prod.replace(old, "false ||", 1))
            execute(
                "mutant-unsupported-callback",
                fixture.replace(
                    str(source("modules/kern/src/platform/mm/reclaim.cpp")), str(mut)
                ),
                1,
                "NodeReclaim.UnsupportedSkippedBeforeCountNativeUnitsDoNotBecomeBuddyAndGlobalRemainsLegacy",
            )
            old = "result.made_progress = result.sampled_buddy_pages != 0 &&"
            assert old in prod
            mut2 = T / "reclaim-progress-mutant.cpp"
            mut2.write_text(
                prod.replace(
                    old, "result.made_progress = result.reclaimed_units != 0 &&", 1
                )
            )
            execute(
                "mutant-native-units-progress",
                fixture.replace(
                    str(source("modules/kern/src/platform/mm/reclaim.cpp")), str(mut2)
                ),
                1,
                "NodeReclaim.SharedMappingRefPreventsFalsePhysicalProgressAfterActualCacheEviction",
            )
        after = [pin(p["path"]) for p in selected]
        emitted_after = [pin(p["path"]) for p in inputs]
        if after != selected or emitted_after != inputs:
            raise RuntimeError("input drift")
    except BaseException as e:
        errors.append(type(e).__name__ + ": " + str(e))
        emitted_after = []
    finally:
        after = [pin(p["path"]) for p in selected]
        emitted_after = [pin(p["path"]) for p in inputs]
        if after != selected or emitted_after != inputs:
            errors.append("source/emitted input drift")
        for pid in owned:
            if not absent(pid):
                os.killpg(pid, signal.SIGKILL)
        used = sum(p.stat().st_blocks * 512 for p in T.rglob("*") if p.is_file())
        shutil.rmtree(T)
        result = dict(
            qualification=(
                "PASS_ACTUAL_BODY_NODE_RECLAIM_HOST_ONLY"
                if not errors
                else "FAIL_ACTUAL_BODY_NODE_RECLAIM_HOST_ONLY"
            ),
            errors=errors,
            commands=commands,
            reports=reports,
            source_before=selected,
            source_after=after,
            emitted_before=inputs,
            emitted_after=emitted_after,
            source_held=bool(selected)
            and selected == after
            and inputs == emitted_after,
            scratch_removed=not T.exists(),
            scratch_allocated_bytes_removed=used,
            owned_groups_pending=[p for p in owned if not absent(p)],
            seconds=time.monotonic() - started,
            scope="Real coordinator/full real PageAllocator + extracted whole cache bodies/physical_node; Host context/clock/address/reference and lock seams. Finite Host create/post/park/wake-token seams exercise actual startup/notifier/loop ordering; no real RQ/IRQ/SMP, kernel build or scheduled pressure credit.",
        )
        (OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(
            json.dumps(
                dict(
                    qualification=result["qualification"],
                    errors=errors,
                    commands=len(commands),
                    reports=reports,
                    source_held=result["source_held"],
                    scratch_removed=result["scratch_removed"],
                    seconds=result["seconds"],
                )
            )
        )
        if outer:
            outer.cleanup()
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
