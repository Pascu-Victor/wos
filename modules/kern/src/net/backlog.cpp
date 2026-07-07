#include "backlog.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <net/wki/wire.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/context_switch.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"backlog">;

namespace {
BacklogQueue* queues = nullptr;
std::atomic<bool> ready{false};
uint64_t num_cpus = 0;

constexpr uint32_t NET_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int NET_LATENCY_DAEMON_NICE = -5;
constexpr uint64_t BACKLOG_CONSUMER_CONTENTION_SLEEP_US = 50;
constexpr uint32_t BACKLOG_HANDLER_COOPERATIVE_BATCH = 128;
constexpr uint64_t BACKLOG_HANDLER_COOPERATIVE_SLEEP_US = 50;
constexpr uint64_t BACKLOG_HANDLER_IDLE_SLEEP_US = 1000;

enum class BacklogConsumerStage : uint8_t {
    IDLE = 0,
    ACQUIRED = 1,
    EMPTY = 2,
    NORMAL = 3,
    WKI = 4,
    RELEASE = 5,
};

auto is_loopback_dev(const NetDevice* dev) -> bool { return dev != nullptr && std::strncmp(dev->name.data(), "lo", dev->name.size()) == 0; }

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = NET_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, NET_LATENCY_DAEMON_NICE);
}

auto packet_is_wki_ethernet_frame(const PacketBuffer* pkt) -> bool {
    if (pkt == nullptr || pkt->len < sizeof(proto::EthernetHeader)) {
        return false;
    }

    const auto* eth = reinterpret_cast<const proto::EthernetHeader*>(pkt->data);
    uint16_t const ETHERTYPE = ntohs(eth->ethertype);
    return ETHERTYPE == proto::ETH_TYPE_WKI || ETHERTYPE == proto::ETH_TYPE_WKI_ROCE;
}

void append_packet(PacketBuffer*& head, PacketBuffer*& tail, PacketBuffer* pkt) {
    pkt->next = nullptr;
    if (tail != nullptr) {
        tail->next = pkt;
    } else {
        head = pkt;
    }
    tail = pkt;
}

auto reverse_packet_list(PacketBuffer* batch) -> PacketBuffer* {
    PacketBuffer* reversed = nullptr;
    while (batch != nullptr) {
        PacketBuffer* next = batch->next;
        batch->next = reversed;
        reversed = batch;
        batch = next;
    }
    return reversed;
}

void split_backlog_batch(PacketBuffer* batch, PacketBuffer*& normal_head, PacketBuffer*& wki_head) {
    PacketBuffer* normal_tail = nullptr;
    PacketBuffer* wki_tail = nullptr;

    while (batch != nullptr) {
        PacketBuffer* next = batch->next;
        if (packet_is_wki_ethernet_frame(batch)) {
            append_packet(wki_head, wki_tail, batch);
        } else {
            append_packet(normal_head, normal_tail, batch);
        }
        batch = next;
    }
}

auto count_packet_list(PacketBuffer* batch) -> uint64_t {
    uint64_t count = 0;
    while (batch != nullptr) {
        ++count;
        batch = batch->next;
    }
    return count;
}

void mark_consumer_stage(BacklogQueue& q, BacklogConsumerStage stage) {
    q.consumer_stage.store(static_cast<uint8_t>(stage), std::memory_order_release);
}

void mark_consumer_acquired(BacklogQueue& q, uintptr_t owner_site) {
    auto* current = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    q.consumer_owner_pid.store(current != nullptr ? current->pid : 0, std::memory_order_relaxed);
    q.consumer_owner_cpu.store(ker::mod::cpu::current_cpu(), std::memory_order_relaxed);
    q.consumer_owner_site.store(owner_site, std::memory_order_relaxed);
    q.consumer_start_us.store(ker::mod::time::get_us(), std::memory_order_release);
    q.consumer_batch.store(0, std::memory_order_relaxed);
    q.consumer_normal.store(0, std::memory_order_relaxed);
    q.consumer_wki.store(0, std::memory_order_relaxed);
    mark_consumer_stage(q, BacklogConsumerStage::ACQUIRED);
}

void mark_consumer_batch(BacklogQueue& q, uint64_t batch_count, PacketBuffer* normal_head, PacketBuffer* wki_head) {
    q.consumer_batch.store(batch_count, std::memory_order_relaxed);
    q.consumer_normal.store(count_packet_list(normal_head), std::memory_order_relaxed);
    q.consumer_wki.store(count_packet_list(wki_head), std::memory_order_relaxed);
}

void clear_consumer_owner(BacklogQueue& q) {
    q.consumer_stage.store(static_cast<uint8_t>(BacklogConsumerStage::IDLE), std::memory_order_release);
    q.consumer_batch.store(0, std::memory_order_relaxed);
    q.consumer_normal.store(0, std::memory_order_relaxed);
    q.consumer_wki.store(0, std::memory_order_relaxed);
    q.consumer_start_us.store(0, std::memory_order_relaxed);
    q.consumer_owner_site.store(0, std::memory_order_relaxed);
    q.consumer_owner_cpu.store(0, std::memory_order_relaxed);
    q.consumer_owner_pid.store(0, std::memory_order_relaxed);
}

void process_backlog_packet(PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }

    NET_TRACE_TICK();
    if (is_loopback_dev(pkt->dev)) {
        if (pkt->len > 0) {
            uint8_t const VERSION = (pkt->data[0] >> 4) & 0xF;
            if (VERSION == 4) {
                proto::ipv4_rx(pkt->dev, pkt);
                return;
            }
            if (VERSION == 6) {
                proto::ipv6_rx(pkt->dev, pkt);
                return;
            }
        }
        pkt_free(pkt);
        return;
    }
    if (pkt->dev != nullptr && pkt->dev->wki_rx_forward != nullptr) {
        pkt->dev->wki_rx_forward(pkt->dev, pkt);
    }
    proto::eth_rx(pkt->dev, pkt);
}

void process_packet_list(PacketBuffer* batch, bool cooperative) {
    uint32_t processed_since_sleep = 0;
    while (batch != nullptr) {
        PacketBuffer* next = batch->next;
        batch->next = nullptr;
        process_backlog_packet(batch);
        batch = next;
        if (cooperative && batch != nullptr && ++processed_since_sleep >= BACKLOG_HANDLER_COOPERATIVE_BATCH) {
            processed_since_sleep = 0;
            ker::mod::sched::kern_sleep_us(BACKLOG_HANDLER_COOPERATIVE_SLEEP_US);
        }
    }
}

auto prepare_backlog_batch(PacketBuffer* batch, BacklogQueue& q, PacketBuffer*& normal_head, PacketBuffer*& wki_head) -> int {
    int queue_drained = 0;
    PacketBuffer* cursor = batch;
    while (cursor != nullptr) {
        ++queue_drained;
        cursor = cursor->next;
    }

    q.depth.fetch_sub(static_cast<uint64_t>(queue_drained), std::memory_order_relaxed);
    split_backlog_batch(reverse_packet_list(batch), normal_head, wki_head);
    return queue_drained;
}

auto try_acquire_backlog_consumer(BacklogQueue& q) -> bool {
    bool expected = false;
    bool const ACQUIRED = q.consumer_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
    if (ACQUIRED) {
        mark_consumer_acquired(q, reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
    }
    return ACQUIRED;
}

void wake_backlog_handler(BacklogQueue& q) {
    if (q.handler == nullptr) {
        return;
    }

    ker::mod::sched::kern_wake(q.handler);
    if (q.handler->cpu == ker::mod::cpu::current_cpu()) {
        ker::mod::sys::context_switch::request_reschedule();
    } else {
        ker::mod::sched::wake_cpu(q.handler->cpu, ker::mod::sched::WakeCpuMode::FORCE);
    }
}

void release_backlog_consumer(BacklogQueue& q) {
    mark_consumer_stage(q, BacklogConsumerStage::RELEASE);
    q.consumer_active.store(false, std::memory_order_release);
    clear_consumer_owner(q);
    if (q.head.load(std::memory_order_acquire) != nullptr) {
        wake_backlog_handler(q);
    }
}

auto drain_backlog_queue_inline(BacklogQueue& q, bool cooperative) -> int {
    if (!try_acquire_backlog_consumer(q)) {
        return 0;
    }

    PacketBuffer* batch = q.head.exchange(nullptr, std::memory_order_acquire);
    if (batch == nullptr) {
        mark_consumer_stage(q, BacklogConsumerStage::EMPTY);
        release_backlog_consumer(q);
        return 0;
    }

    PacketBuffer* normal_head = nullptr;
    PacketBuffer* wki_head = nullptr;
    int const QUEUE_DRAINED = prepare_backlog_batch(batch, q, normal_head, wki_head);
    mark_consumer_batch(q, static_cast<uint64_t>(QUEUE_DRAINED), normal_head, wki_head);

    mark_consumer_stage(q, BacklogConsumerStage::NORMAL);
    process_packet_list(normal_head, cooperative);
    release_backlog_consumer(q);
    process_packet_list(wki_head, cooperative);
    return QUEUE_DRAINED;
}

// Handler thread entry point (one per CPU). DAEMON type, pinned.
void backlog_handler_loop(uint64_t cpu_idx) {
    for (;;) {
        if (cpu_idx >= num_cpus) {
            cpu_idx = 0;
        }
        auto& q = queues[cpu_idx];
        q.handler_active.store(true, std::memory_order_release);

        if (!try_acquire_backlog_consumer(q)) {
            ker::mod::sched::kern_sleep_us(BACKLOG_CONSUMER_CONTENTION_SLEEP_US);
            continue;
        }

        // Atomic drain: grab entire list in one shot (MPSC consumer).
        PacketBuffer* batch = q.head.exchange(nullptr, std::memory_order_acquire);

        if (batch == nullptr) {
            mark_consumer_stage(q, BacklogConsumerStage::EMPTY);
            release_backlog_consumer(q);

            // Store false and re-check for race between exchange and store.
            q.handler_active.store(false, std::memory_order_seq_cst);

            if (q.head.load(std::memory_order_acquire) != nullptr) {
                q.handler_active.store(true, std::memory_order_relaxed);
                continue;
            }

            ker::mod::sched::kern_sleep_us(BACKLOG_HANDLER_IDLE_SLEEP_US);
            q.handler_active.store(true, std::memory_order_relaxed);
            continue;
        }

        PacketBuffer* normal_head = nullptr;
        PacketBuffer* wki_head = nullptr;
        int const QUEUE_DRAINED = prepare_backlog_batch(batch, q, normal_head, wki_head);
        mark_consumer_batch(q, static_cast<uint64_t>(QUEUE_DRAINED), normal_head, wki_head);

        mark_consumer_stage(q, BacklogConsumerStage::NORMAL);
        process_packet_list(normal_head, true);
        release_backlog_consumer(q);
        process_packet_list(wki_head, true);
    }
}

[[noreturn]] void backlog_handler_entry() {
    uint64_t const MY_CPU = ker::mod::cpu::current_cpu();
    backlog_handler_loop(MY_CPU);
    __builtin_unreachable();
}

auto mix_hash(uint32_t h, uint32_t value) -> uint32_t {
    h ^= value + 0x9e3779b9U + (h << 6U) + (h >> 2U);
    return h;
}

}  // namespace

void backlog_init() {
    num_cpus = ker::mod::smt::get_core_count();
    if (num_cpus <= 1) {
        log::info("single CPU, steering disabled");
        return;
    }

    queues = new BacklogQueue[num_cpus]{};
    if (queues == nullptr) {
        log::warn("failed to allocate queues");
        return;
    }

    for (uint64_t i = 0; i < num_cpus; i++) {
        auto* task = ker::mod::sched::task::Task::create_kernel_thread("net_backlog", backlog_handler_entry);
        if (task == nullptr) {
            log::warn("failed to create handler for CPU %u", static_cast<unsigned>(i));
            continue;
        }
        queues[i].handler = task;
        queues[i].handler_active.store(false, std::memory_order_relaxed);
        promote_latency_sensitive_daemon(task);
        ker::mod::sched::post_task_pinned_cpu(i, task);
    }

    ready.store(true, std::memory_order_release);
    log::info("%u handler threads started", static_cast<unsigned>(num_cpus));
}

void backlog_enqueue(uint64_t target_cpu, PacketBuffer* pkt) {
    if (target_cpu >= num_cpus) {
        target_cpu = 0;
    }
    auto& q = queues[target_cpu];

    // Lock-free MPSC push: CAS on head.
    q.depth.fetch_add(1, std::memory_order_relaxed);
    PacketBuffer* old_head = q.head.load(std::memory_order_relaxed);
    do {  // NOLINT
        pkt->next = old_head;
    } while (!q.head.compare_exchange_weak(old_head, pkt, std::memory_order_release, std::memory_order_relaxed));

    wake_backlog_handler(q);
}

auto backlog_flow_hash(PacketBuffer* pkt, uint64_t num_cpus) -> uint64_t {
    if (num_cpus <= 1) {
        return 0;
    }

    if (pkt->len < sizeof(proto::EthernetHeader)) {
        return 0;
    }
    const auto* eth = reinterpret_cast<const proto::EthernetHeader*>(pkt->data);
    uint16_t const ETHERTYPE = ntohs(eth->ethertype);

    // IPv4: hash 5-tuple
    if (ETHERTYPE == 0x0800) {
        constexpr size_t ETH_HDR_SIZE = sizeof(proto::EthernetHeader);
        if (pkt->len < ETH_HDR_SIZE + sizeof(proto::IPv4Header)) {
            return 0;
        }
        const auto* ip = reinterpret_cast<const proto::IPv4Header*>(pkt->data + ETH_HDR_SIZE);
        uint32_t h = ip->src_addr ^ ip->dst_addr ^ static_cast<uint32_t>(ip->protocol);

        // For TCP/UDP, include port numbers
        uint8_t const IHL = (ip->ihl_version & 0x0F) * 4;
        if ((ip->protocol == 6 || ip->protocol == 17) && pkt->len >= ETH_HDR_SIZE + IHL + 4) {
            const auto* ports = reinterpret_cast<const uint16_t*>(pkt->data + ETH_HDR_SIZE + IHL);
            h ^= static_cast<uint32_t>(ports[0]) << 16 | static_cast<uint32_t>(ports[1]);
        }

        h *= 0x9e3779b9U;
        h ^= h >> 16;
        return h % num_cpus;
    }

    auto h = static_cast<uint32_t>(ETHERTYPE);
    for (size_t i = 0; i < proto::MacAddress::SIZE_BYTES; ++i) {
        h = mix_hash(h, static_cast<uint32_t>(eth->src.at(i)));
        h = mix_hash(h, static_cast<uint32_t>(eth->dst.at(i)));
    }

    if ((ETHERTYPE == proto::ETH_TYPE_WKI || ETHERTYPE == proto::ETH_TYPE_WKI_ROCE) &&
        pkt->len >= sizeof(proto::EthernetHeader) + sizeof(wki::WkiHeader)) {
        const auto* hdr = reinterpret_cast<const wki::WkiHeader*>(pkt->data + sizeof(proto::EthernetHeader));
        h = mix_hash(h, static_cast<uint32_t>(hdr->src_node));
        h = mix_hash(h, static_cast<uint32_t>(hdr->dst_node));
        h = mix_hash(h, static_cast<uint32_t>(hdr->channel_id));
    }

    h *= 0x9e3779b9U;
    h ^= h >> 16U;
    return h % num_cpus;
}

auto backlog_drain_all_pending_inline() -> int {
    if (!ready.load(std::memory_order_acquire) || queues == nullptr) {
        return 0;
    }

    int drained = 0;
    for (uint64_t cpu_idx = 0; cpu_idx < num_cpus; ++cpu_idx) {
        drained += drain_backlog_queue_inline(queues[cpu_idx], false);
    }

    return drained;
}

auto backlog_ready() -> bool { return ready.load(std::memory_order_acquire); }

auto backlog_rescue_needed(uint64_t min_queue_depth) -> bool {
    if (!ready.load(std::memory_order_acquire) || queues == nullptr) {
        return false;
    }

    for (uint64_t cpu_idx = 0; cpu_idx < num_cpus; ++cpu_idx) {
        auto& q = queues[cpu_idx];
        uint64_t const QUEUED = q.depth.load(std::memory_order_acquire);
        if (QUEUED >= min_queue_depth && !q.consumer_active.load(std::memory_order_acquire)) {
            return true;
        }
    }

    return false;
}

void backlog_get_snapshot(BacklogSnapshot& out) {
    out = {};
    out.ready = ready.load(std::memory_order_acquire);
    out.num_cpus = num_cpus;
    if (!out.ready || queues == nullptr) {
        return;
    }

    out.queue_count = std::min<size_t>(num_cpus, BACKLOG_SNAPSHOT_MAX_CPUS);
    for (size_t i = 0; i < out.queue_count; ++i) {
        auto& row = out.queues.at(i);
        auto& q = queues[i];
        row.cpu = i;
        row.queued = q.depth.load(std::memory_order_relaxed);
        row.handler_active = q.handler_active.load(std::memory_order_acquire);
        row.consumer_active = q.consumer_active.load(std::memory_order_acquire);
        row.consumer_owner_pid = q.consumer_owner_pid.load(std::memory_order_acquire);
        row.consumer_owner_cpu = q.consumer_owner_cpu.load(std::memory_order_acquire);
        row.consumer_owner_site = q.consumer_owner_site.load(std::memory_order_acquire);
        row.consumer_start_us = q.consumer_start_us.load(std::memory_order_acquire);
        row.consumer_batch = q.consumer_batch.load(std::memory_order_acquire);
        row.consumer_normal = q.consumer_normal.load(std::memory_order_acquire);
        row.consumer_wki = q.consumer_wki.load(std::memory_order_acquire);
        row.consumer_stage = q.consumer_stage.load(std::memory_order_acquire);
        if (row.consumer_active && row.consumer_start_us != 0) {
            uint64_t const NOW_US = ker::mod::time::get_us();
            row.consumer_hold_us = NOW_US >= row.consumer_start_us ? NOW_US - row.consumer_start_us : 0;
        }
        if (q.handler != nullptr) {
            row.handler_pid = q.handler->pid;
            row.handler_cpu = q.handler->cpu;
        }
        out.total_queued += row.queued;
    }
}

}  // namespace ker::net
