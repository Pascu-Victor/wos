#include "backlog.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

namespace ker::net {

namespace {

constexpr size_t MAX_CPUS = 64;

BacklogQueue queues[MAX_CPUS];
std::atomic<bool> ready{false};
uint64_t num_cpus = 0;

// Handler thread entry point (one per CPU). DAEMON type, pinned.
void backlog_handler_loop(uint64_t cpu_idx) {
    auto& q = queues[cpu_idx];

    for (;;) {
        // Atomic drain: grab entire list in one shot (MPSC consumer).
        PacketBuffer* batch = q.head.exchange(nullptr, std::memory_order_acquire);

        if (batch == nullptr) {
            // Store false and re-check for race between exchange and store.
            q.handler_active.store(false, std::memory_order_seq_cst);

            if (q.head.load(std::memory_order_acquire) != nullptr) {
                q.handler_active.store(true, std::memory_order_relaxed);
                continue;
            }

            ker::mod::sched::kern_yield();
            q.handler_active.store(true, std::memory_order_relaxed);
            continue;
        }

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
            proto::eth_rx(reversed->dev, reversed);
            reversed = next;
        }
    }
}

[[noreturn]] void backlog_handler_entry() {
    // Handler task pinned via post_task_for_cpu, currentCpu() is the queue index.
    uint64_t my_cpu = ker::mod::cpu::currentCpu();
    backlog_handler_loop(my_cpu);
    __builtin_unreachable();
}

}  // namespace

void backlog_init() {
    num_cpus = ker::mod::smt::getCoreCount();
    num_cpus = std::min(num_cpus, MAX_CPUS);
    if (num_cpus <= 1) {
        ker::mod::dbg::log("backlog: single CPU, steering disabled");
        return;
    }

    for (uint64_t i = 0; i < num_cpus; i++) {
        auto* task = ker::mod::sched::task::Task::createKernelThread("net_backlog", backlog_handler_entry);
        if (task == nullptr) {
            ker::mod::dbg::log("backlog: failed to create handler for CPU %u", static_cast<unsigned>(i));
            continue;
        }
        queues[i].handler = task;
        queues[i].handler_active.store(false, std::memory_order_relaxed);
        ker::mod::sched::post_task_for_cpu(i, task);
    }

    ready.store(true, std::memory_order_release);
    ker::mod::dbg::log("backlog: %u handler threads started", static_cast<unsigned>(num_cpus));
}

void backlog_enqueue(uint64_t target_cpu, PacketBuffer* pkt) {
    if (target_cpu >= num_cpus) {
        target_cpu = 0;
    }
    auto& q = queues[target_cpu];

    // Lock-free MPSC push: CAS on head.
    PacketBuffer* old_head = q.head.load(std::memory_order_relaxed);
    do {  // NOLINT
        pkt->next = old_head;
    } while (!q.head.compare_exchange_weak(old_head, pkt, std::memory_order_release, std::memory_order_relaxed));

    ker::mod::sched::wake_cpu(target_cpu);
}

auto backlog_flow_hash(PacketBuffer* pkt, uint64_t num_cpus) -> uint64_t {
    if (num_cpus <= 1) {
        return 0;
    }

    if (pkt->len < sizeof(proto::EthernetHeader)) {
        return 0;
    }
    const auto* eth = reinterpret_cast<const proto::EthernetHeader*>(pkt->data);
    uint16_t ethertype = ntohs(eth->ethertype);

    // IPv4: hash 5-tuple
    if (ethertype == 0x0800) {
        constexpr size_t ETH_HDR_SIZE = sizeof(proto::EthernetHeader);
        if (pkt->len < ETH_HDR_SIZE + sizeof(proto::IPv4Header)) {
            return 0;
        }
        const auto* ip = reinterpret_cast<const proto::IPv4Header*>(pkt->data + ETH_HDR_SIZE);
        uint32_t h = ip->src_addr ^ ip->dst_addr ^ static_cast<uint32_t>(ip->protocol);

        // For TCP/UDP, include port numbers
        uint8_t ihl = (ip->ihl_version & 0x0F) * 4;
        if ((ip->protocol == 6 || ip->protocol == 17) && pkt->len >= ETH_HDR_SIZE + ihl + 4) {
            const auto* ports = reinterpret_cast<const uint16_t*>(pkt->data + ETH_HDR_SIZE + ihl);
            h ^= static_cast<uint32_t>(ports[0]) << 16 | static_cast<uint32_t>(ports[1]);
        }

        h *= 0x9e3779b9U;
        h ^= h >> 16;
        return h % num_cpus;
    }

    // Non-IPv4: hash source MAC
    uint32_t mac_hash = static_cast<uint32_t>(eth->src[0]) | (static_cast<uint32_t>(eth->src[1]) << 8) |
                        (static_cast<uint32_t>(eth->src[2]) << 16) | (static_cast<uint32_t>(eth->src[3]) << 24);
    mac_hash ^= static_cast<uint32_t>(eth->src[4]) | (static_cast<uint32_t>(eth->src[5]) << 8);
    mac_hash *= 0x9e3779b9U;
    return mac_hash % num_cpus;
}

auto backlog_ready() -> bool { return ready.load(std::memory_order_acquire); }

}  // namespace ker::net
