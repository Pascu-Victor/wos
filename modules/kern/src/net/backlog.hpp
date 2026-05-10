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

// Lock-free MPSC backlog queue for per-CPU packet distribution
struct BacklogQueue {
    std::atomic<PacketBuffer*> head{nullptr};
    std::array<std::byte, 56> pad{};  // avoid false sharing
    ker::mod::sched::task::Task* handler = nullptr;
    std::atomic<bool> handler_active{false};
};

static_assert(offsetof(BacklogQueue, handler) >= 64, "BacklogQueue handler state must stay off the head cache line");

void backlog_init();
void backlog_enqueue(uint64_t target_cpu, PacketBuffer* pkt);
auto backlog_flow_hash(PacketBuffer* pkt, uint64_t num_cpus) -> uint64_t;
auto backlog_ready() -> bool;

}  // namespace ker::net
