#include "packet.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <net/netdevice.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

namespace {

// ---------------------------------------------------------------------------
// Global pool (fallback)
// ---------------------------------------------------------------------------
PacketBuffer* pool = nullptr;
size_t pool_capacity = 0;
PacketBuffer* free_list = nullptr;
ker::mod::sys::Spinlock pool_lock;
bool initialized = false;

// Approximate count of free buffers (global pool + per-CPU caches).
// Decremented on alloc, incremented on free.  Used by pkt_alloc_tx()
// to cheaply check whether we should reserve buffers for RX.
std::atomic<size_t> free_count{0};

// ---------------------------------------------------------------------------
// Per-CPU packet cache — avoids global pool_lock contention on the hot path.
// The NAPI worker and the spin-waiting caller are typically on the same CPU,
// so recently-freed RX buffers are cache-hot when reused for the next TX.
// ---------------------------------------------------------------------------
constexpr size_t PKT_PERCPU_CACHE_SIZE = 8;  // entries per CPU
constexpr size_t PKT_PERCPU_MAX_CPUS = 256;

struct PktPerCpuCache {
    PacketBuffer* head = nullptr;
    uint32_t count = 0;
};

PktPerCpuCache s_percpu_cache[PKT_PERCPU_MAX_CPUS];
std::atomic<bool> s_percpu_ready{false};

void add_buffers_to_pool(size_t count) {
    // Allocate new buffers
    auto* new_buffers = static_cast<PacketBuffer*>(ker::mod::mm::dyn::kmalloc::calloc(count, sizeof(PacketBuffer)));
    if (new_buffers == nullptr) {
        ker::mod::dbg::log("net: Failed to allocate %zu packet buffers", count);
        return;
    }

    pool_lock.lock();
    // Link new buffers into free list
    for (size_t i = 0; i < count; i++) {
        new_buffers[i].next = free_list;
        free_list = &new_buffers[i];
    }
    pool_capacity += count;
    pool_lock.unlock();

    free_count.fetch_add(count, std::memory_order_relaxed);

    ker::mod::dbg::log("net: Added %zu packet buffers (total: %zu)", count, pool_capacity);
}

// Allocate from global pool (locked)
auto pkt_global_alloc() -> PacketBuffer* {
    pool_lock.lock();
    if (free_list == nullptr) {
        pool_lock.unlock();
        return nullptr;
    }
    auto* pkt = free_list;
    free_list = pkt->next;
    pool_lock.unlock();
    return pkt;
}

// Return to global pool (locked)
void pkt_global_free(PacketBuffer* pkt) {
    pool_lock.lock();
    pkt->next = free_list;
    free_list = pkt;
    pool_lock.unlock();
}

}  // namespace

void pkt_pool_init() {
    if (initialized) {
        return;
    }

    // Start with minimum pool size
    add_buffers_to_pool(PKT_POOL_MIN_SIZE);
    initialized = true;
}

void pkt_pool_expand_for_nics() {
    // Calculate required size: 1024 buffers per NIC, minimum 1024 total
    size_t nic_count = netdev_count();
    size_t required = nic_count * PKT_POOL_PER_NIC;
    required = std::max(required, PKT_POOL_MIN_SIZE);

    // Add more buffers if needed
    if (required > pool_capacity) {
        size_t to_add = required - pool_capacity;
        add_buffers_to_pool(to_add);
    }

    // Per-CPU cache is safe once SMP + NAPI workers are up
    s_percpu_ready.store(true, std::memory_order_release);
}

auto pkt_pool_size() -> size_t { return pool_capacity; }

auto pkt_pool_free_count() -> size_t { return free_count.load(std::memory_order_relaxed); }

auto pkt_alloc() -> PacketBuffer* {
    PacketBuffer* pkt = nullptr;

    // Fast path: try per-CPU cache (no lock, no cache-line bounce)
    if (s_percpu_ready.load(std::memory_order_acquire)) {
        auto& cache = s_percpu_cache[ker::mod::cpu::currentCpu()];
        if (cache.head != nullptr) {
            pkt = cache.head;
            cache.head = pkt->next;
            cache.count--;
        }
    }

    // Slow path: global pool
    if (pkt == nullptr) {
        pkt = pkt_global_alloc();
        if (pkt == nullptr) {
            return nullptr;
        }
    }

    // Initialize the packet buffer
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    pkt->len = 0;
    pkt->next = nullptr;
    pkt->dev = nullptr;
    pkt->protocol = 0;

    free_count.fetch_sub(1, std::memory_order_relaxed);

    return pkt;
}

auto pkt_alloc_tx() -> PacketBuffer* {
    // Reserve at least 256 buffers for RX so the NIC can always receive.
    // Without this, TX/retransmit queues can exhaust the pool, preventing
    // ACKs from arriving, which creates an unbreakable deadlock.
    constexpr size_t RX_RESERVE = 256;

    size_t avail = free_count.load(std::memory_order_relaxed);
    if (avail <= RX_RESERVE) {
        ker::mod::dbg::log("pkt_alloc_tx: REFUSED (free=%zu reserve=%zu)", avail, RX_RESERVE);
        return nullptr;  // Pool too low, reserve for RX
    }

    return pkt_alloc();
}

void pkt_free(PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }

    free_count.fetch_add(1, std::memory_order_relaxed);

    // Fast path: return to per-CPU cache if not full
    if (s_percpu_ready.load(std::memory_order_acquire)) {
        auto& cache = s_percpu_cache[ker::mod::cpu::currentCpu()];
        if (cache.count < PKT_PERCPU_CACHE_SIZE) {
            pkt->next = cache.head;
            cache.head = pkt;
            cache.count++;
            return;
        }
    }

    // Slow path: return to global pool
    pkt_global_free(pkt);
}

}  // namespace ker::net
