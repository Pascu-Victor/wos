#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::net {

struct PacketBuffer;

constexpr size_t BACKLOG_SNAPSHOT_MAX_CPUS = 64;

struct BacklogQueueSnapshot {
    uint64_t cpu = 0;
    uint64_t queued = 0;
    uint64_t handler_pid = 0;
    uint64_t handler_cpu = 0;
    bool handler_active = false;
};

struct BacklogSnapshot {
    bool ready = false;
    uint64_t num_cpus = 0;
    uint64_t total_queued = 0;
    size_t queue_count = 0;
    std::array<BacklogQueueSnapshot, BACKLOG_SNAPSHOT_MAX_CPUS> queues{};
};

// Lock-free MPSC backlog queue for per-CPU packet distribution
struct BacklogQueue {
    std::atomic<PacketBuffer*> head{nullptr};
    std::atomic<uint64_t> depth{0};
    std::atomic<bool> consumer_active{false};
    std::array<std::byte, 48> pad{};  // avoid false sharing
    ker::mod::sched::task::Task* handler = nullptr;
    std::atomic<bool> handler_active{false};
};

static_assert(offsetof(BacklogQueue, handler) >= 64, "BacklogQueue handler state must stay off the head cache line");

void backlog_init();
void backlog_enqueue(uint64_t target_cpu, PacketBuffer* pkt);
auto backlog_flow_hash(PacketBuffer* pkt, uint64_t num_cpus) -> uint64_t;
auto backlog_ready() -> bool;
void backlog_get_snapshot(BacklogSnapshot& out);
auto backlog_drain_all_pending_inline() -> int;

}  // namespace ker::net
