#include "backlog.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
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

auto is_loopback_dev(const NetDevice* dev) -> bool { return dev != nullptr && std::strncmp(dev->name.data(), "lo", dev->name.size()) == 0; }

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = NET_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, NET_LATENCY_DAEMON_NICE);
}

auto packet_list_count(const PacketBuffer* batch) -> uint64_t {
    uint64_t count = 0;
    while (batch != nullptr) {
        ++count;
        batch = batch->next;
    }
    return count;
}

void process_backlog_batch(PacketBuffer* batch) {
    // Reverse LIFO->FIFO to preserve packet ordering.
    PacketBuffer* reversed = nullptr;
    while (batch != nullptr) {
        PacketBuffer* next = batch->next;
        batch->next = reversed;
        reversed = batch;
        batch = next;
    }

    // Process each packet through the protocol stack.
    while (reversed != nullptr) {
        PacketBuffer* next = reversed->next;
        reversed->next = nullptr;
        NET_TRACE_TICK();
        if (is_loopback_dev(reversed->dev)) {
            if (reversed->len > 0) {
                uint8_t const VERSION = (reversed->data[0] >> 4) & 0xF;
                if (VERSION == 4) {
                    proto::ipv4_rx(reversed->dev, reversed);
                    reversed = next;
                    continue;
                }
                if (VERSION == 6) {
                    proto::ipv6_rx(reversed->dev, reversed);
                    reversed = next;
                    continue;
                }
            }
            pkt_free(reversed);
            reversed = next;
            continue;
        }
        if (reversed->dev != nullptr && reversed->dev->wki_rx_forward != nullptr) {
            reversed->dev->wki_rx_forward(reversed->dev, reversed);
        }
        proto::eth_rx(reversed->dev, reversed);
        reversed = next;
    }
}

auto try_acquire_backlog_consumer(BacklogQueue& q) -> bool {
    bool expected = false;
    return q.consumer_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
}

void release_backlog_consumer(BacklogQueue& q) { q.consumer_active.store(false, std::memory_order_release); }

// Handler thread entry point (one per CPU). DAEMON type, pinned.
void backlog_handler_loop(uint64_t cpu_idx) {
    for (;;) {
        // Re-read current CPU each iteration: the scheduler may migrate this
        // handler thread, so we must always drain the queue for whichever CPU
        // we're actually running on right now.
        cpu_idx = ker::mod::cpu::current_cpu();
        if (cpu_idx >= num_cpus) {
            cpu_idx = 0;
        }
        auto& q = queues[cpu_idx];

        if (!try_acquire_backlog_consumer(q)) {
            ker::mod::sched::kern_yield();
            continue;
        }

        // Atomic drain: grab entire list in one shot (MPSC consumer).
        PacketBuffer* batch = q.head.exchange(nullptr, std::memory_order_acquire);

        if (batch == nullptr) {
            release_backlog_consumer(q);

            // Store false and re-check for race between exchange and store.
            q.handler_active.store(false, std::memory_order_seq_cst);

            if (q.head.load(std::memory_order_acquire) != nullptr) {
                q.handler_active.store(true, std::memory_order_relaxed);
                continue;
            }

            ker::mod::sched::kern_block();
            q.handler_active.store(true, std::memory_order_relaxed);
            continue;
        }

        q.depth.fetch_sub(packet_list_count(batch), std::memory_order_relaxed);
        process_backlog_batch(batch);
        release_backlog_consumer(q);
    }
}

[[noreturn]] void backlog_handler_entry() {
    // Starting CPU - passed as initial hint but re-read each iteration in the loop.
    uint64_t const MY_CPU = ker::mod::cpu::current_cpu();
    backlog_handler_loop(MY_CPU);
    __builtin_unreachable();
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

    if (q.handler != nullptr) {
        ker::mod::sched::kern_wake(q.handler);
        if (q.handler->cpu == ker::mod::cpu::current_cpu()) {
            // Same-CPU wake_cpu() is a no-op; force a local reschedule so the
            // backlog worker does not wait for the next timer quantum.
            ker::mod::sys::context_switch::request_reschedule();
        } else {
            ker::mod::sched::wake_cpu(q.handler->cpu);
        }
    }
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

    // Non-IPv4: hash source MAC
    const auto& src = eth->src;
    uint32_t mac_hash = static_cast<uint32_t>(src.at(0)) | (static_cast<uint32_t>(src.at(1)) << 8) |
                        (static_cast<uint32_t>(src.at(2)) << 16) | (static_cast<uint32_t>(src.at(3)) << 24);
    mac_hash ^= static_cast<uint32_t>(src.at(4)) | (static_cast<uint32_t>(src.at(5)) << 8);
    mac_hash *= 0x9e3779b9U;
    return mac_hash % num_cpus;
}

auto backlog_drain_all_pending_inline() -> int {
    if (!ready.load(std::memory_order_acquire) || queues == nullptr) {
        return 0;
    }

    int drained = 0;
    for (uint64_t cpu_idx = 0; cpu_idx < num_cpus; ++cpu_idx) {
        auto& q = queues[cpu_idx];
        if (!try_acquire_backlog_consumer(q)) {
            continue;
        }

        PacketBuffer* batch = q.head.exchange(nullptr, std::memory_order_acquire);
        if (batch == nullptr) {
            release_backlog_consumer(q);
            continue;
        }

        int queue_drained = 0;
        PacketBuffer* cursor = batch;
        while (cursor != nullptr) {
            ++queue_drained;
            cursor = cursor->next;
        }
        drained += queue_drained;

        q.depth.fetch_sub(static_cast<uint64_t>(queue_drained), std::memory_order_relaxed);
        process_backlog_batch(batch);
        release_backlog_consumer(q);
    }

    return drained;
}

auto backlog_ready() -> bool { return ready.load(std::memory_order_acquire); }

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
        if (q.handler != nullptr) {
            row.handler_pid = q.handler->pid;
            row.handler_cpu = q.handler->cpu;
        }
        out.total_queued += row.queued;
    }
}

}  // namespace ker::net
