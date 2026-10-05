#!/usr/bin/env python3
"""Execute current NAPI, virtio rearm and backlog handoffs at race boundaries.

The scheduler event publisher, wait cancellation and block metadata are real
source bodies. Privileged halt/runqueue operations and device/packet callbacks
are host seams. This does not qualify hardware IRQ delivery or DMA ordering.
"""

import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def function(source: str, name: str) -> str:
    match = re.search(
        rf"^(?:(?:inline|constexpr|\[\[noreturn\]\])\s+)?(?:auto|void|bool|int)\s+{name}\([^)]*\)"
        rf"\s*(?:->\s*[^{{]+)?\s*\{{", source, re.M
    )
    assert match, f"missing function {name}"
    end, depth = source.index("{", match.start()) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


def structure(source: str, name: str) -> str:
    start = source.index(f"struct {name} {{")
    end, depth = source.index("{", start) + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    assert source[end] == ";"
    return source[start:end + 1]


PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <algorithm>
#include <string_view>
#include <string>
#include <cstring>
#include <thread>
#include <vector>
#include <net/netpoll.hpp>
#include <net/backlog.hpp>
#include <platform/security/authorization.hpp>
#include <platform/sched/event_wake_diagnostics.hpp>
struct Finish {};
enum class Boundary { None, BeforeArm, AfterArm, AfterPark, IdleEvent, RearmRx, RearmTx, RearmIrq, NewOwner };
Boundary boundary = Boundary::None;
bool injected = false, is_backlog = false, device_pending_rx = false, device_pending_tx = false;
bool diagnostics_enabled = false;
unsigned blocks = 0, parks = 0, yields = 0, wakes = 0, pokes = 0, poll_calls = 0;
unsigned completed_rx = 0, completed_tx = 0, backlog_packets = 0, owner_changes = 0;
int rx_queued = 0, tx_queued = 0;
unsigned lock_depth = 0;
namespace ker::init { const char* cmdline = nullptr; const char* get_kernel_cmdline() { return cmdline; } }
void inject(Boundary);
struct Lock {
    uint64_t lock_irqsave() { ++lock_depth; return 0; }
    void unlock_irqrestore(uint64_t) { assert(lock_depth); --lock_depth; }
};
namespace ker::mod::sched::task {
enum class WaitChannelKind { WKI_EXECVE_PROXY };
struct Task {
    uint64_t cpu = 0, pid = 19, perf_wait_callsite = 0, wake_at_us = 0;
    uint8_t type = 0; bool cpu_pinned = true;
    bool deferred_task_switch = true, wants_block = false, voluntary = false, channel = false;
    std::atomic<bool> wakeup_pending{false};
    bool wait_channel_is(WaitChannelKind) const { return false; }
    bool has_interrupting_signal_pending() const { return false; }
    void set_wait_channel(const char*) { channel = true; }
    void clear_wait_channel() { channel = false; }
    void set_voluntary_blocked(bool value) { voluntary = value; }
};
}
ker::mod::sched::task::Task worker;
namespace ker::mod::cpu { uint64_t current_cpu() { return 0; } }
namespace ker::mod::sys::context_switch { void request_reschedule() { ++pokes; } }
namespace ker::mod::sched {
// Batch ownership/flush and its integration with the actual worker body are
// exercised by local_wake_batch_test; retain this IRQ/park handoff seam.
struct LocalWakeBatch {~LocalWakeBatch(){}};
namespace time { uint64_t get_us() { assert(false); return 0; } }
struct RunQueue { std::atomic<uint64_t> cpu_irq_us{0}, scheduler_timer_interrupts{0}; SchedulerTimerTrace timer_trace{}; };
struct RunQueues { RunQueue row; RunQueue* that_cpu(uint64_t) { return &row; } };
RunQueues* run_queues = nullptr;
// The timer trace is separately qualified; this NAPI harness exercises OFF paths.
bool scheduler_timer_trace_enabled() { return false; }
task::Task* current_task_for_preempt() { return &::worker; }
enum class WakeCpuMode { FORCE, COALESCE };
enum class EventWakeDeferredSwitch { PRESERVE };
enum class SchedulerHaltWaitKind { YIELD, BLOCK, PROCESS_PARK };
struct MigrationGuardPlacement { bool disabled = false; uint64_t owner_cpu = 0; int reason = 0; };
namespace cpu = ker::mod::cpu;
namespace smt { uint64_t get_core_count() { return 8; } }
bool event_wake_should_cancel_deferred_switch(EventWakeDeferredSwitch, bool) { return false; }
MigrationGuardPlacement migration_guard_placement(task::Task*, uint64_t) { return {}; }
void record_migration_rejection(task::Task*, uint64_t, int) { assert(false); }
uint64_t scheduler_single_process_cpu_target(task::Task*, uint64_t) { return UINT64_MAX; }
uint64_t scheduler_process_owner_cpu_if_migration_disabled(task::Task*, uint64_t) { return UINT64_MAX; }
uint64_t get_least_loaded_cpu() { return 0; }
uint64_t cross_cpu_migration_target_or_source(task::Task*, uint64_t, uint64_t target, uint64_t) { return target; }
uint64_t event_wake_target_cpu(task::Task* task, uint64_t) { return task->cpu; }
void reschedule_task_for_cpu(uint64_t, task::Task* task, EventWakeDiagnostics* diagnostics = nullptr) {
    assert(diagnostics == nullptr);
    ++wakes;
    if (parks) task->wants_block = false;
}
void wake_cpu(uint64_t, WakeCpuMode) { ++pokes; }
unsigned preempt_count() { return 0; }
void note_preempt_disabled_block(const char*, uint64_t) { assert(false); }
bool irq_enabled = true;
bool interrupts_enabled() { return irq_enabled; }
void fixture_cli() { irq_enabled = false; }
bool pause_syscall_accounting() { return false; }
void resume_syscall_accounting() { assert(false); }
void restore_interrupts_after_scheduler_wait(bool was_enabled) { if (was_enabled) irq_enabled = true; }
// Accounting arithmetic is covered by kernel_wait_accounting_runtime_test;
// this handoff seam retains the real wrapper's IRQ/blocked-state contract.
struct KernelWaitAccounting { bool was_blocked; };
KernelWaitAccounting begin_kernel_wait_accounting(task::Task* task) {
    assert(!irq_enabled);
    bool const was_blocked = task && task->voluntary;
    if (task) task->set_voluntary_blocked(true);
    return {was_blocked};
}
void finish_kernel_wait_accounting(task::Task* task, KernelWaitAccounting state) {
    assert(!irq_enabled);
    if (task) task->set_voluntary_blocked(state.was_blocked);
}
void maybe_exit_current_kernel_thread_for_shutdown() {
    // Stop a serviced loop at its next empty wait, without changing its body.
    if (poll_calls && rx_queued == 0 && tx_queued == 0) throw Finish{};
    if (backlog_packets) throw Finish{};
}
task::Task* get_current_task() {
    if (boundary == Boundary::IdleEvent && blocks == 1) throw Finish{};
    ++blocks; inject(Boundary::BeforeArm); return &worker;
}
bool scheduler_wait_should_cancel(task::Task*, SchedulerHaltWaitKind);
bool scheduler_wait_halt_once(task::Task* task, SchedulerHaltWaitKind kind, bool) {
    assert(lock_depth == 0);
    inject(Boundary::AfterArm);
    if (scheduler_wait_should_cancel(task, kind)) return false;
    ++parks;
    assert(parks <= 2); // A lost-token mutant must fail, never hang the suite.
    inject(Boundary::AfterPark);
    if (!injected) throw Finish{};
    return true;
}
void kern_block_impl(uint64_t);
void kern_block() { kern_block_impl(123); }
void kern_yield() { assert(lock_depth == 0); ++yields; }
void kern_sleep_us(uint64_t delay) { assert(lock_depth == 0 && delay == 50); throw Finish{}; }
}
namespace ker::net {
struct NetDevice { std::array<char,16> name{}; };
constexpr size_t NETDEV_NAME_LEN = 16;
struct PacketBuffer { PacketBuffer* next = nullptr; unsigned consumed = 0; };
struct log { template<class... T> static void critical(const char*, T...) { assert(false); } };
constexpr int NAPI_WORKER_MAX_FULL_BUDGET_POLLS = 8;
constexpr uint64_t NAPI_WORKER_CONTENTION_SLEEP_US = 50;
constexpr int NAPI_RESCUE_MAX_PASSES = 4;
constexpr uint64_t BACKLOG_RESCUE_MIN_QUEUE_DEPTH = 64;
constexpr uint64_t BACKLOG_CONSUMER_CONTENTION_SLEEP_US = 50;
bool poll_fn_is_valid(NapiPollFn poll) { return poll != nullptr; }
bool napi_worker_has_work(NapiStruct*);
void wake_napi_worker(NapiStruct*);
int napi_poll_struct_inline_budget(NapiStruct*, int, NapiPollSource = NapiPollSource::INLINE);
NapiStruct napi{};
NetDevice netdev{};
PacketBuffer packet{};
std::array<BacklogQueue,1> queues{};
uint64_t num_cpus = 1;
enum class BacklogConsumerStage { EMPTY, RELEASE, NORMAL };
void mark_consumer_acquired(BacklogQueue&, uintptr_t) {}
void mark_consumer_stage(BacklogQueue&, BacklogConsumerStage) {}
void clear_consumer_owner(BacklogQueue&) {}
void mark_consumer_batch(BacklogQueue&, uint64_t, PacketBuffer*, PacketBuffer*) {}
void split_backlog_batch(PacketBuffer* batch, PacketBuffer*& normal, PacketBuffer*& wki) { normal = batch; wki = nullptr; }
PacketBuffer* reverse_packet_list(PacketBuffer* batch) {
    PacketBuffer* reversed = nullptr;
    while (batch) { auto* next = batch->next; batch->next = reversed; reversed = batch; batch = next; }
    return reversed;
}
void process_packet_list(PacketBuffer* batch, bool, bool = false) {
    while (batch) { assert(++batch->consumed == 1); ++backlog_packets; batch = batch->next; }
}
void pkt_free(PacketBuffer* packet) { assert(++packet->consumed == 1); }
int napi_poll_all_pending(NapiPollSource source) { return napi_poll_struct_inline_budget(&napi, NAPI_DEFAULT_WEIGHT, source); }
bool backlog_rescue_needed(uint64_t minimum) { assert(minimum == 64); return false; }
int backlog_drain_all_pending_inline() { assert(false); return 0; }
}
namespace driver {
struct Virtqueue {
    uint16_t flags = 0; void* avail = &flags; std::atomic<bool> failed{false}; Lock lock; bool tx = false;
    std::array<ker::net::PacketBuffer*,4> pkt_map{};
    std::array<uint16_t,6> completed{}; size_t cursor = 0;
};
struct VirtIONetDevice;
struct VirtIONetQueuePair {
    Virtqueue* rxq = nullptr; Virtqueue* txq = nullptr;
    unsigned index = 0; uint8_t rx_empty_pending_rearms = 0;
    ker::net::NapiStruct& napi = ker::net::napi;
    Lock notification_lock;
};
struct VirtIONetDevice { unsigned configured_queue_pairs = 1, num_queue_pairs = 1; std::array<VirtIONetQueuePair,2> queue_pairs{}; };
struct VirtIONetPending { bool rx{}, tx{}; };
constexpr uint16_t VIRTQ_AVAIL_F_NO_INTERRUPT = 1;
constexpr uint8_t RX_EMPTY_PENDING_REARM_LIMIT = 1;
constexpr uint16_t VIRTQ_NO_BUF = 0xFFFF, VIRTQ_BAD_BUF = 0xFFFE;
Virtqueue rx{}, tx{};
VirtIONetDevice device{};
bool virtq_has_pending(Virtqueue* queue) { return queue->tx ? device_pending_tx : device_pending_rx; }
uint16_t virtq_get_buf(Virtqueue* queue, uint32_t*) {
    assert(lock_depth == 1 && queue->cursor < queue->completed.size());
    return queue->completed.at(queue->cursor++);
}
}
'''

CASES = r'''
int fake_poll(ker::net::NapiStruct* napi, int budget, ker::net::NapiPollSource source) {
    ++poll_calls;
    auto& pair = driver::device.queue_pairs[0];
    driver::virtio_net_begin_poll(&pair);
    int count = std::min(budget, rx_queued); rx_queued -= count; completed_rx += count;
    assert(tx_queued <= 4);
    std::array<ker::net::PacketBuffer,4> tx_packets{};
    driver::tx.completed.fill(driver::VIRTQ_NO_BUF); driver::tx.pkt_map.fill(nullptr); driver::tx.cursor = 0;
    for (int i = 0; i < tx_queued; ++i) {
        driver::tx.pkt_map.at(i) = &tx_packets.at(i); driver::tx.completed.at(i) = i;
    }
    uint64_t reclaimed_count = 0;
    auto flags = driver::tx.lock.lock_irqsave();
    auto* reclaimed = driver::collect_tx_for(&driver::tx, napi->diagnostics_enabled ? &reclaimed_count : nullptr);
    driver::tx.lock.unlock_irqrestore(flags);
    driver::free_pkt_list(reclaimed);
    ker::net::napi_diag_tx_reclaimed(napi, source, reclaimed_count);
    completed_tx += tx_queued; tx_queued = 0;
    device_pending_rx = rx_queued != 0; device_pending_tx = false;
    if (count < budget) driver::virtio_net_complete_poll(&driver::device, &pair, napi, count);
    return count;
}
void inject(Boundary at) {
    if ((boundary != at && !(boundary == Boundary::IdleEvent && at == Boundary::BeforeArm)) || injected) return;
    injected = true;
    if (boundary == Boundary::IdleEvent) { ker::net::wake_napi_worker(&ker::net::napi); }
    else if (at == Boundary::RearmRx) { device_pending_rx = true; rx_queued = 1; }
    else if (at == Boundary::RearmTx) { device_pending_tx = true; tx_queued = 1; }
    else if (at == Boundary::RearmIrq) { ker::net::napi_schedule(&ker::net::napi); }
    else if (at == Boundary::NewOwner) {
        assert(ker::net::napi.state.load() == ker::net::NapiState::IDLE);
        ker::net::napi.state.store(ker::net::NapiState::POLLING); ++owner_changes;
    } else if (is_backlog) {
        auto& q = ker::net::queues[0];
        bool empty = ker::net::push_backlog_packet(q, &ker::net::packet);
        ker::net::wake_backlog_handler(q, ker::net::backlog_wake_mode_for_enqueue(empty));
    } else {
        rx_queued = 1; device_pending_rx = true;
        ker::net::napi_schedule(&ker::net::napi);
    }
}
void fake_fence(int order) {
    assert(order == __ATOMIC_SEQ_CST && lock_depth == 1);
    assert(driver::rx.flags == 0 && driver::tx.flags == 0);
    inject(Boundary::RearmRx); inject(Boundary::RearmTx); inject(Boundary::RearmIrq);
    inject(Boundary::NewOwner);
    std::atomic_thread_fence(std::memory_order_seq_cst);
}
void reset(Boundary at = Boundary::None) {
    ker::mod::sched::irq_enabled = true;
    if (!ker::net::napi.diagnostics_enabled) {
        // Check raw optional counters, not merely the disabled snapshot output.
        ker::net::napi.diagnostics_enabled = true;
        auto previous = ker::net::napi_diag_snapshot(&ker::net::napi);
        assert((previous.schedules == std::array<uint64_t,4>{}));
        assert((previous.accepted == std::array<uint64_t,4>{}));
        assert((previous.polls == std::array<uint64_t,3>{}));
        assert((previous.rx_work == std::array<uint64_t,3>{}));
        assert((previous.rx_zero == std::array<uint64_t,3>{}));
        assert((previous.tx_reclaimed == std::array<uint64_t,3>{}));
        assert(previous.coalesced == 0 && previous.polling_rejects == 0 && previous.disabled_rejects == 0);
        assert(previous.park_calls == 0 && previous.park_empty_returns == 0 && previous.contention_sleeps == 0);
        assert(previous.inline_refused == 0 && previous.budget_handoffs == 0);
        ker::net::napi.diagnostics_enabled = false;
    }
    boundary = at; injected = is_backlog = device_pending_rx = device_pending_tx = false;
    blocks = parks = yields = wakes = pokes = poll_calls = completed_rx = completed_tx = backlog_packets = owner_changes = 0;
    rx_queued = tx_queued = 0; assert(lock_depth == 0);
    worker.cpu = 0; worker.wants_block = worker.voluntary = worker.channel = false;
    worker.deferred_task_switch = true; worker.wakeup_pending.store(false);
    ker::net::napi.dev = &ker::net::netdev; ker::net::napi.poll = fake_poll;
    ker::net::napi.weight = ker::net::NAPI_DEFAULT_WEIGHT; ker::net::napi.worker = &worker;
    ker::net::napi.state.store(ker::net::NapiState::IDLE); ker::net::napi.has_work.store(false);
    ker::net::napi.poll_count.store(0); ker::net::napi.complete_count.store(0);
    ker::net::napi.diagnostics_enabled = diagnostics_enabled;
    ker::net::napi_diag_reset(&ker::net::napi);
    driver::rx.flags = driver::tx.flags = 0; driver::tx.tx = true;
    driver::rx.failed.store(false); driver::tx.failed.store(false);
    auto& pair = driver::device.queue_pairs[0]; pair.rxq = &driver::rx; pair.txq = &driver::tx;
    pair.rx_empty_pending_rearms = 0;
    auto& q = ker::net::queues[0]; q.head.store(nullptr); q.depth.store(0);
    q.consumer_active.store(false); q.handler_active.store(false); q.handler = &worker;
    ker::net::packet.next = nullptr; ker::net::packet.consumed = 0;
}
void run_worker() { try { ker::net::napi_worker_loop(&ker::net::napi); } catch (Finish const&) {} }
void run_backlog() { try { ker::net::backlog_handler_loop(0); } catch (Finish const&) {} }
int main(int argc, char** argv) {
    using ker::net::NapiState;
    auto& napi = ker::net::napi;
    auto& pair = driver::device.queue_pairs[0];
    if (argc == 2 && std::string_view(argv[1]) == "diagnostics") {
        diagnostics_enabled = true;
    } else if (argc == 2) {
        assert(std::string_view(argv[1]) == "stale-owner");
        reset(Boundary::NewOwner); rx_queued = 1;
        assert(ker::net::napi_poll_struct_inline_budget(&napi, 64) == 1);
        assert(owner_changes == 1 && napi.state.load() == NapiState::POLLING && napi.complete_count == 1);
        return 0;
    }
    assert(argc == 1 || diagnostics_enabled);
    for (auto at : {Boundary::BeforeArm, Boundary::AfterArm, Boundary::AfterPark}) {
        reset(at); run_worker();
        assert(injected && completed_rx == 1 && poll_calls == 1);
        assert(parks == (at == Boundary::AfterPark ? 1U : 0U));
        assert(!worker.wakeup_pending.load() && worker.deferred_task_switch);
        assert(!worker.wants_block && !worker.voluntary && !worker.channel);
        assert(napi.state.load() == NapiState::IDLE && !napi.has_work.load());
        if (diagnostics_enabled) {
            auto diag = ker::net::napi_diag_snapshot(&napi);
            // The host stops at the next attempted park after servicing work.
            assert(diag.park_calls == 2 && diag.park_empty_returns == 0 && diag.polls[0] == 1 && diag.rx_work[0] == 1);
        }
    }
    reset(); run_worker(); assert(parks == 1 && poll_calls == 0); // Idle really parks.
    reset(Boundary::IdleEvent); run_worker();
    assert(injected && parks == 0 && poll_calls == 0 && !worker.wakeup_pending.load());
    if (diagnostics_enabled) {
        auto diag = ker::net::napi_diag_snapshot(&napi);
        assert(diag.park_calls == 2 && diag.park_empty_returns == 1 && diag.polls[0] == 0);
    }
    reset(); napi.state.store(NapiState::POLLING); napi.has_work.store(true); run_worker();
    if (diagnostics_enabled) assert(ker::net::napi_diag_snapshot(&napi).contention_sleeps == 1);
    reset(); assert(ker::net::napi_schedule(&napi));
    worker.wakeup_pending.store(false); assert(!ker::net::napi_schedule(&napi));
    assert(worker.wakeup_pending.load() && wakes == 2); // Duplicate schedule still helps.
    reset(); worker.cpu = 3; ker::net::napi_schedule(&napi); assert(pokes == 1);
    reset(); napi.worker = nullptr; ker::net::napi_schedule(&napi); assert(wakes == 0);
    reset(); napi.state.store(NapiState::DISABLED);
    assert(!ker::net::napi_schedule(&napi));
    assert(ker::net::napi_poll_struct_inline_budget(&napi, 64) == 0 && poll_calls == 0);
    if (diagnostics_enabled) assert(ker::net::napi_diag_snapshot(&napi).inline_refused == 1);
    // Suppress the scheduling callback: real inline rescue can claim an IDLE queue.
    reset(); rx_queued = 1; tx_queued = 2; device_pending_rx = device_pending_tx = true;
    assert(napi.state.load() == NapiState::IDLE && wakes == 0);
    assert(ker::net::napi_rescue_poll_once() == 1);
    assert(completed_rx == 1 && completed_tx == 2 && napi.state.load() == NapiState::IDLE);
    reset(); tx_queued = 3; device_pending_tx = true;
    assert(ker::net::napi_rescue_poll_once() == 0);
    assert(completed_tx == 3 && poll_calls == 1 && napi.state.load() == NapiState::IDLE);
    // A zero return counts RX only: it cannot establish an empty/spurious poll.
    reset(); rx_queued = 5 * 64 + 1;
    assert(ker::net::napi_rescue_poll_once() == 4 * 64);
    assert(rx_queued == 65 && yields == 4 && napi.state.load() == NapiState::SCHEDULED);
    assert(napi.has_work.load() && worker.wakeup_pending.load());
    if (diagnostics_enabled) {
        auto diag = ker::net::napi_diag_snapshot(&napi);
        assert(diag.enabled && diag.polls[2] == 4 && diag.rx_work[2] == 256);
        assert(diag.budget_handoffs == 4 && diag.polls[0] == 0 && diag.polls[1] == 0);
    }
    reset(); rx_queued = 9 * 64 + 1; ker::net::napi_schedule(&napi); run_worker();
    assert(completed_rx == 577 && yields == 1 && napi.complete_count == 1);
    // Device work posted at notification re-enable must be scheduled without IRQ.
    for (auto at : {Boundary::RearmRx, Boundary::RearmTx}) {
        reset(at); napi.state.store(NapiState::POLLING);
        driver::virtio_net_complete_poll(&driver::device, &pair, &napi, 0);
        assert(injected && napi.state.load() == NapiState::SCHEDULED && napi.has_work.load());
        assert(driver::rx.flags == 1 && driver::tx.flags == 1);
    }
    // Duplicate IRQ/new poll owner survives the former inline caller's epilogue.
    reset(Boundary::RearmIrq); rx_queued = 1;
    assert(ker::net::napi_poll_struct_inline_budget(&napi, 64) == 1);
    assert(napi.state.load() == NapiState::SCHEDULED && napi.has_work.load() && napi.complete_count == 1);
    reset(Boundary::NewOwner); rx_queued = 1;
    assert(ker::net::napi_poll_struct_inline_budget(&napi, 64) == 1);
    assert(owner_changes == 1 && napi.state.load() == NapiState::POLLING && napi.complete_count == 1);
    assert(driver::rx.flags == 1 && driver::tx.flags == 1);
    reset(); device_pending_rx = true;
    assert(driver::should_reschedule_after_notification_rearm(&driver::device, &pair, 0));
    assert(!driver::should_reschedule_after_notification_rearm(&driver::device, &pair, 0));
    device_pending_tx = true;
    for (unsigned i = 0; i < 8; ++i)
        assert(driver::should_reschedule_after_notification_rearm(&driver::device, &pair, 0));
    device_pending_tx = false;
    assert(driver::should_reschedule_after_notification_rearm(&driver::device, &pair, 1));
    reset(); driver::rx.failed.store(true); driver::virtio_net_set_queue_notifications(&driver::rx, true);
    assert(driver::rx.flags == 1); // Failed device never resumes notifications.
    for (auto at : {Boundary::BeforeArm, Boundary::AfterArm, Boundary::AfterPark}) {
        reset(at); is_backlog = true; run_backlog();
        assert(injected && backlog_packets == 1 && ker::net::packet.consumed == 1);
        assert(ker::net::queues[0].head.load() == nullptr && ker::net::queues[0].depth.load() == 0);
        assert(!ker::net::queues[0].consumer_active.load());
        assert(parks == (at == Boundary::AfterPark ? 1U : 0U));
    }
    reset(); is_backlog = true; auto& q = ker::net::queues[0];
    q.consumer_active.store(true); ker::net::push_backlog_packet(q, &ker::net::packet);
    run_backlog(); assert(q.head.load() == &ker::net::packet && backlog_packets == 0);
    ker::net::release_backlog_consumer(q);
    assert(!q.consumer_active.load() && worker.wakeup_pending.load());
    reset(); q.handler = nullptr;
    ker::net::wake_backlog_handler(q, ker::mod::sched::WakeCpuMode::FORCE); assert(wakes == 0);
    // Explicit source/outcome counters, TX-only progress and off-by-default behavior.
    reset(); ker::net::napi_schedule(&napi, ker::net::NapiScheduleSource::IRQ);
    ker::net::napi_schedule(&napi, ker::net::NapiScheduleSource::REARM);
    napi.state.store(NapiState::POLLING);
    ker::net::napi_schedule(&napi, ker::net::NapiScheduleSource::STARTUP);
    napi.state.store(NapiState::DISABLED); ker::net::napi_schedule(&napi);
    napi.state.store(NapiState::IDLE); tx_queued = 3; device_pending_tx = true;
    assert(ker::net::napi_rescue_poll_once() == 0);
    auto diag = ker::net::napi_diag_snapshot(&napi);
    assert(diag.enabled == diagnostics_enabled);
    if (diagnostics_enabled) {
        assert(diag.schedules[0] == 1 && diag.schedules[1] == 1 && diag.schedules[2] == 1 && diag.schedules[3] == 1);
        assert(diag.accepted[1] == 1 && diag.coalesced == 1 && diag.polling_rejects == 1 && diag.disabled_rejects == 1);
        assert(diag.polls[2] == 1 && diag.rx_zero[2] == 1 && diag.tx_reclaimed[2] == 3 && diag.rx_work[2] == 0);
    } else {
        for (auto const& value : napi.diagnostics.schedules) assert(value.load() == 0);
        for (auto const& value : napi.diagnostics.polls) assert(value.load() == 0);
        for (auto const& value : napi.diagnostics.tx_reclaimed) assert(value.load() == 0);
        assert(napi.diagnostics.coalesced.load() == 0 && diag.polls[2] == 0 && diag.tx_reclaimed[2] == 0);
    }
    for (auto token : {static_cast<const char*>(nullptr), "net.napi_diag_extra", "xnet.napi_diag", "net.napi_diag=1",
                       "\t net.napi_diag\n"}) {
        ker::init::cmdline = token;
        ker::net::NapiStruct probe{};
        ker::net::napi_init(&probe, &ker::net::netdev, fake_poll, 0);
        assert(probe.diagnostics_enabled == (token && std::string_view(token) == "\t net.napi_diag\n"));
        assert(probe.state.load() == NapiState::DISABLED && probe.worker == nullptr && probe.weight == 64);
    }
    // A corrupt completion and an absent owner never count as reclaimed buffers.
    reset(); std::array<ker::net::PacketBuffer,2> owners{};
    driver::tx.cursor = 0; driver::tx.pkt_map = {&owners[0], nullptr, &owners[1], nullptr};
    driver::tx.completed = {driver::VIRTQ_BAD_BUF, 0, 1, 2, driver::VIRTQ_NO_BUF, driver::VIRTQ_NO_BUF};
    uint64_t count = 0; auto flags = driver::tx.lock.lock_irqsave();
    auto* list = driver::collect_tx_for(&driver::tx, &count);
    driver::tx.lock.unlock_irqrestore(flags);
    assert(count == 2 && list == &owners[1] && list->next == &owners[0]);
    assert(driver::tx.pkt_map[0] == nullptr && driver::tx.pkt_map[2] == nullptr);
    driver::free_pkt_list(list); assert(owners[0].consumed == 1 && owners[1].consumed == 1);
    // Execute the actual procfs row loop, including legacy queue/worker keys.
    driver::VirtIONetDiagSnapshot row{};
    row.ifindex = UINT32_MAX;
    row.pair = row.num_queue_pairs = row.configured_queue_pairs = row.hdr_size = row.irq_vector = UINT8_MAX;
    row.active = row.msix_enabled = row.napi_has_work = true;
    row.napi_state = static_cast<uint8_t>(NapiState::DISABLED);
    row.name.fill('a'); row.napi_worker_pid = row.napi_worker_cpu = row.napi_polls = row.napi_completes = UINT64_MAX;
    row.rx.size = row.rx.num_free = row.rx.pending = row.rx.avail_idx = row.rx.used_idx = row.rx.last_used_idx = row.rx.mapped = UINT16_MAX;
    row.tx = row.rx;
    row.napi_diag.enabled = true;
    auto& values = row.napi_diag;
    values.schedules.fill(UINT64_MAX); values.accepted.fill(UINT64_MAX);
    values.polls.fill(UINT64_MAX); values.rx_work.fill(UINT64_MAX);
    values.rx_zero.fill(UINT64_MAX); values.tx_reclaimed.fill(UINT64_MAX);
    for (auto* value : {&values.coalesced, &values.polling_rejects, &values.disabled_rejects, &values.park_calls,
                        &values.park_empty_returns, &values.contention_sleeps, &values.inline_refused, &values.budget_handoffs})
        *value = UINT64_MAX;
    auto line = formatter::render(row);
    size_t const worstcase_bytes = line.size();
    assert(line.size() < 4096 && line.ends_with("\n"));
    assert(line.find(" worker_pid=18446744073709551615 worker_cpu=18446744073709551615") != std::string::npos);
    assert(line.find(" txreclaimed_rescue=18446744073709551615") != std::string::npos);
    assert(line.find(" budget_handoffs=18446744073709551615") != std::string::npos);
    // Snapshot acquisition is a bounded host seam; execute the actual dedicated
    // generator and shared formatter with all maximum-valued 32 rows.
    driver::diagnostic_row = row; driver::snapshot_rows = driver::VIRTIO_NET_DIAG_MAX_ROWS;
    std::string output(formatter::MAX_WKI_NAPI_BUF + 2, '!');
    auto length = formatter::generate_wki_napi(output.data() + 1, formatter::MAX_WKI_NAPI_BUF);
    assert(output.front() == '!' && output.back() == '!' && length < formatter::MAX_WKI_NAPI_BUF);
    std::string text(output.data() + 1, length);
    std::string const header = "wki_napi version=1 scope=virtio_admitted\n";
    assert(text.starts_with(header) && text.ends_with("wki_napi_end complete=1 rows=32 snapshot_rows=32\n"));
    assert(text == header + [&] { std::string rows; for (size_t i = 0; i < 32; ++i) rows += line; return rows; }() +
                   "wki_napi_end complete=1 rows=32 snapshot_rows=32\n");
    size_t const view_worstcase_bytes = length;
    assert(formatter::generate_wki_napi(nullptr, 0) == 0);
    std::vector<size_t> capacities;
    for (size_t capacity = 0; capacity <= header.size() + worstcase_bytes * 2 + 100; ++capacity) capacities.push_back(capacity);
    for (size_t count = 0; count <= 32; ++count) {
        size_t const boundary = header.size() + worstcase_bytes * count + 1 + 96;
        for (auto capacity : {boundary - 1, boundary, boundary + 1}) capacities.push_back(capacity);
    }
    capacities.push_back(formatter::MAX_WKI_NAPI_BUF);
    for (size_t capacity : capacities) {
        std::string bounded(capacity + 2, '!');
        auto used = formatter::generate_wki_napi(bounded.data() + 1, capacity);
        assert(bounded.front() == '!' && bounded.back() == '!');
        if (capacity == 0) { assert(used == 0 && bounded[1] == '!'); continue; }
        assert(used < capacity && bounded[used + 1] == '\0');
        std::string actual(bounded.data() + 1, used);
        if (capacity <= 96) {
            assert(actual.find("complete=1") == std::string::npos);
            continue;
        }
        size_t const body_capacity = capacity - 1 - 96;
        size_t const fits = body_capacity >= header.size() ? std::min<size_t>(32, (body_capacity - header.size()) / worstcase_bytes) : 0;
        std::string expected = body_capacity >= header.size() ? header : "";
        for (size_t i = 0; i < fits; ++i) expected += line;
        expected += "wki_napi_end complete=" + std::string(fits == 32 ? "1" : "0") + " rows=" + std::to_string(fits) + " snapshot_rows=32\n";
        assert(actual == expected); // Whole rows only; footer survives every body boundary.
    }
    driver::snapshot_rows = 0;
    length = formatter::generate_wki_napi(output.data() + 1, formatter::MAX_WKI_NAPI_BUF);
    text.assign(output.data() + 1, length);
    assert(text == header + "wki_napi_end complete=1 rows=0 snapshot_rows=0\n");
    driver::snapshot_rows = 32; driver::diagnostic_row.napi_diag.enabled = false;
    length = formatter::generate_wki_napi(output.data() + 1, formatter::MAX_WKI_NAPI_BUF);
    text.assign(output.data() + 1, length);
    assert(text.ends_with("wki_napi_end complete=1 rows=32 snapshot_rows=32\n"));
    assert(text.find(" sched_irq=") == std::string::npos && text.find(" napi_diag=0\n") != std::string::npos);
    using namespace ker::mod::security;
    auto const root = Snapshot::bootstrap_root(); Snapshot user, restricted_root;
    assert(root.prepare({.change=Change::SET_UID, .real=1000}, user) == 0);
    assert(root.prepare({.change=Change::DROP_AUTHORITY, .authority_ceiling=0}, restricted_root) == 0);
    auto const access = formatter::diagnostic_access(formatter::ProcNodeType::WKI_NAPI_FILE);
    assert(access == DiagnosticAccess::KERNEL_READ);
    assert(authorize_diagnostic(root, access, false) == 0);
    assert(authorize_diagnostic(user, access, false) == -EACCES && authorize_diagnostic(restricted_root, access, false) == -EACCES);
    for (auto const& caller : {root, user, restricted_root}) assert(authorize_diagnostic(caller, access, true) == -EPERM);
    std::printf("PASS complete bounded NAPI view32 worstcase=%zu bytes, short-buffer and every row boundary, authority policy\n", view_worstcase_bytes);
    row.napi_diag.enabled = false; line = formatter::render(row);
    assert(line.ends_with(" napi_diag=0\n") && line.find(" sched_irq=") == std::string::npos);
    values.enabled = true;
    std::array<char,10> tiny{}; tiny.back() = '!'; char* cursor = tiny.data();
    formatter::append_napi_diag(cursor, tiny.data() + 8, values);
    assert(cursor <= tiny.data() + 8 && tiny.back() == '!');
    static_assert(sizeof(driver::VirtIONetDiagSnapshot) * 32 < 16 * 1024);
    std::printf("PASS diagnostic row worstcase=%zu bytes, snapshot32=%zu bytes\n", worstcase_bytes, sizeof(row) * 32);
    if (diagnostics_enabled) {
        reset(); std::array<std::thread,3> writers;
        for (size_t i = 0; i < writers.size(); ++i) writers[i] = std::thread([&, i] {
            for (unsigned n = 0; n < 2000; ++n) {
                ker::net::napi_diag_poll(&napi, static_cast<ker::net::NapiPollSource>(i), 2);
                ker::net::napi_diag_tx_reclaimed(&napi, static_cast<ker::net::NapiPollSource>(i), 3);
            }
        });
        for (auto& writer : writers) writer.join();
        auto totals = ker::net::napi_diag_snapshot(&napi);
        for (size_t i = 0; i < 3; ++i) assert(totals.polls[i] == 2000 && totals.rx_work[i] == 4000 && totals.tx_reclaimed[i] == 6000);
        napi.diagnostics.schedules[1].store(UINT64_MAX);
        assert(ker::net::napi_schedule(&napi, ker::net::NapiScheduleSource::IRQ));
        assert(ker::net::napi_diag_snapshot(&napi).schedules[1] == 0); // Defined unsigned wrap.
    }
    std::puts("PASS actual NAPI/scheduler/block/backlog boundaries, virtio RX/TX rearm, inline rescue and budgets");
}
'''


def harness() -> str:
    netpoll = (ROOT / "modules/kern/src/net/netpoll.cpp").read_text()
    scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
    sched_header = (ROOT / "modules/kern/src/platform/sched/scheduler.hpp").read_text()
    driver = (ROOT / "modules/kern/src/dev/virtio/virtio_net.cpp").read_text()
    backlog = (ROOT / "modules/kern/src/net/backlog.cpp").read_text()
    procfs = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    driver_header = (ROOT / "modules/kern/src/dev/virtio/virtio_net.hpp").read_text()
    proc_header = (ROOT / "modules/kern/src/vfs/fs/procfs.hpp").read_text()
    assert "static_assert(MAX_VIRTIO_NET_DEVICES * VIRTIO_NET_MAX_QUEUE_PAIRS <= VIRTIO_NET_DIAG_MAX_ROWS)" in driver
    assert 'return make_file(ProcNodeType::WKI_NAPI_FILE, 0, false);' in procfs
    assert 'alloc_sz = MAX_WKI_NAPI_BUF;' in function(procfs, "procfs_read_with_credentials")
    assert 'generate_wki_napi(pfd->content, MAX_WKI_NAPI_BUF)' in function(procfs, "procfs_read_with_credentials")
    assert '"wki/napi"' in function(procfs, "selftest_global_authorization")
    assert 'std::memcpy(buf->d_name.data(), "napi", 5)' in procfs
    code = PRELUDE
    # Keep budgets/backoffs and the RX no-progress bound tied to local source.
    for source, names in ((netpoll, ("NAPI_WORKER_MAX_FULL_BUDGET_POLLS", "NAPI_WORKER_CONTENTION_SLEEP_US",
                                     "NAPI_RESCUE_MAX_PASSES", "BACKLOG_RESCUE_MIN_QUEUE_DEPTH")),
                          (backlog, ("BACKLOG_CONSUMER_CONTENTION_SLEEP_US",)),
                          (driver, ("RX_EMPTY_PENDING_REARM_LIMIT",))):
        for name in names:
            declaration = re.search(rf"^constexpr \w+ {name} = [^;]+;", source, re.M)
            assert declaration, f"missing constant {name}"
            code, count = re.subn(rf"^constexpr \w+ {name} = [^;]+;", declaration[0], code, flags=re.M)
            assert count == 1, name
    code += "\nnamespace ker::mod::sched {\n"
    code += function(scheduler, "event_wake_clock") + "\n"
    code += "\n".join(function(scheduler, name) for name in
                      ("wake_task_from_event_on_cpu", "wake_task_from_event")).replace(
                          "EventWakeDiagnostics* diagnostics)", "EventWakeDiagnostics* diagnostics = nullptr)")
    code += "\n".join(function(sched_header, name) for name in
                      ("scheduler_wait_should_cancel", "kern_block_impl")).replace(
                          'asm volatile("cli" ::: "memory");', 'fixture_cli();')
    code += "\n}\nnamespace ker::net {\n"
    code += "\n".join(function(netpoll, name) for name in
                      ("napi_diag_add", "napi_diag_poll", "napi_diag_reset", "cmdline_has_token", "napi_init",
                       "napi_diag_snapshot", "napi_diag_tx_reclaimed",
                       "poke_napi_worker_cpu", "wake_napi_worker", "napi_worker_has_work",
                       "napi_schedule", "napi_complete", "napi_poll_struct_inline_budget",
                       "napi_rescue_poll_once", "napi_worker_loop"))
    code += "\n".join(function(backlog, name) for name in
                      ("prepare_backlog_batch", "try_acquire_backlog_consumer", "push_backlog_packet",
                       "backlog_wake_mode_for_enqueue", "wake_backlog_handler", "release_backlog_consumer",
                       "backlog_handler_loop"))
    code += "\n}\nvoid fake_fence(int);\n#define __atomic_thread_fence(order) fake_fence(order)\nnamespace driver {\n"
    code += "\n".join(function(driver, name) for name in
                      ("collect_tx_for", "free_pkt_list", "virtio_net_set_queue_notifications", "virtio_net_set_pair_notifications",
                       "virtio_net_pair_pending", "should_reschedule_after_notification_rearm",
                       "virtio_net_rearm_pair_notifications_locked", "virtio_net_begin_poll",
                       "virtio_net_complete_poll"))
    code += "\n" + structure(driver_header, "VirtqueueDiagSnapshot")
    code += "\n" + structure(driver_header, "VirtIONetDiagSnapshot")
    code += "\n" + re.search(r"^constexpr size_t VIRTIO_NET_DIAG_MAX_ROWS = [^;]+;", driver_header, re.M)[0]
    code += r'''
size_t snapshot_rows = 0;
VirtIONetDiagSnapshot diagnostic_row{};
size_t virtio_net_diag_snapshot(VirtIONetDiagSnapshot* out, size_t max) {
    assert(max == VIRTIO_NET_DIAG_MAX_ROWS && snapshot_rows <= max);
    std::fill_n(out, snapshot_rows, diagnostic_row);
    return snapshot_rows;
}
'''

    code += "\n}\n#undef __atomic_thread_fence\nnamespace ker::dev { namespace virtio = ::driver; }\nnamespace formatter {\n"
    code += "\n" + re.search(r"enum class ProcNodeType[^}]+};", proc_header)[0]
    code += "\nusing ker::mod::security::DiagnosticAccess;\n" + function(procfs, "diagnostic_access")
    code += "\n" + re.search(r"^constexpr size_t MAX_WKI_NAPI_BUF = [^;]+;", procfs, re.M)[0]
    code += "\n".join(function(procfs, name) for name in
                      ("append_dec64", "append_char", "append_sconst", "append_bool01", "napi_state_name",
                       "append_netdev_name", "append_memacc_key", "append_memacc_dec", "append_memacc_bool",
                       "append_napi_diag", "append_virtqueue_diag", "append_virtio_net_diag", "generate_wki_napi"))
    aggregate = function(procfs, "generate_wki_netdiag")
    start = aggregate.index("for (size_t i = 0; i < VIRTIO_COUNT; ++i)")
    end = aggregate.index("if (VIRTIO_COUNT == virtio_rows.size())", start)
    code += "\nstd::string render(driver::VirtIONetDiagSnapshot row) {\n"
    code += "std::array<char,4096> buf{}; char* p = buf.data(); const char* end = buf.data() + buf.size() - 1;\n"
    code += "std::array<driver::VirtIONetDiagSnapshot,1> virtio_rows{row}; constexpr size_t VIRTIO_COUNT = 1;\n"
    code += aggregate[start:end] + "\nreturn {buf.data(), static_cast<size_t>(p - buf.data())};\n}\n}\n"
    return code + CASES


def main() -> None:
    compiler = os.environ.get("HOST_CXX") or shutil.which("clang++", path=os.defpath)
    assert compiler, "host clang++ with ASan/UBSan required"
    code = harness()
    variants = [("current", code, None)]
    for label, original, replacement, expected in (
        ("napi_event_token", "ker::mod::sched::wake_task_from_event(worker, ker::mod::sched::EventWakeDeferredSwitch::PRESERVE);",
         "(void)worker;", "parks <= 2"),
        ("backlog_event_token", "ker::mod::sched::wake_task_from_event(q.handler, ker::mod::sched::EventWakeDeferredSwitch::PRESERVE);",
         "(void)q.handler;", "parks <= 2"),
        ("tx_rearm", "if (PENDING.tx) {\n        return true;", "if (PENDING.tx) {\n        return false;",
         "injected && napi.state.load() == NapiState::SCHEDULED"),
    ):
        assert code.count(original) == 1, label
        variants.append((label, code.replace(original, replacement), expected))
    view = function(code, "generate_wki_napi")
    original = "append_bool01(p, end, complete);"
    assert view.count(original) == 1
    variants.append(("view_false_complete", code.replace(view, view.replace(original, "append_bool01(p, end, true);")),
                     "actual == expected"))
    inline = function(code, "napi_poll_struct_inline_budget")
    increment = "napi->poll_count.fetch_add(1, std::memory_order_relaxed);"
    assert inline.count(increment) == 1
    stale = inline.replace(increment, increment + "\n    if (PROCESSED < EFFECTIVE_BUDGET) napi_complete(napi);")
    variants.append(("stale_completion", code.replace(inline, stale), "owner_changes == 1 && napi.state.load() == NapiState::POLLING"))
    with tempfile.TemporaryDirectory(prefix="wos-napi-handoff-") as directory:
        source, binary = Path(directory) / "handoff.cpp", Path(directory) / "handoff"
        for label, candidate, expected in variants:
            source.write_text(candidate)
            subprocess.run([compiler, "-std=c++20", "-Wall", "-Wextra", "-Werror", "-g", "-pthread",
                            "-fsanitize=address,undefined", "-fno-omit-frame-pointer",
                            "-I", str(ROOT / "modules/kern/src"), str(source), "-o", str(binary)], check=True)
            if expected is None:
                subprocess.run([str(binary)], check=True, timeout=10)
                subprocess.run([str(binary), "diagnostics"], check=True, timeout=10)
            else:
                arguments = [str(binary)] + (["stale-owner"] if label == "stale_completion" else [])
                result = subprocess.run(arguments, capture_output=True, text=True, timeout=10)
                assert result.returncode == -signal.SIGABRT and expected in result.stderr, (label, result)
                print(f"PASS rejected {label} mutant at expected assertion")


if __name__ == "__main__":
    main()
