#include "packet.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/netdevice.hpp>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"net">;

namespace {

constexpr size_t RX_RESERVE = 256;
constexpr size_t DEBUG_TOP_SITES = 8;
constexpr uint32_t REFUSE_DUMP_STRIDE = 64;

struct PacketChunk {
    PacketBuffer* base = nullptr;
    size_t count = 0;
    PacketChunk* next = nullptr;
};

struct SiteSummary {
    uintptr_t site = 0;
    size_t count = 0;
    uint32_t oldest_seq = UINT32_MAX;
    const PacketBuffer* oldest_pkt = nullptr;
};

constexpr size_t DEBUG_SITE_TRACK_SLOTS = 32;

// ---------------------------------------------------------------------------
// Global pool (fallback)
// ---------------------------------------------------------------------------
size_t pool_capacity = 0;
PacketBuffer* free_list = nullptr;
PacketChunk* chunk_list = nullptr;
ker::mod::sys::Spinlock pool_lock;
bool initialized = false;

// Approximate count of free buffers (global pool + per-CPU caches).
// Decremented on alloc, incremented on free.  Used by pkt_alloc_tx()
// to cheaply check whether we should reserve buffers for RX.
std::atomic<size_t> free_count{0};
std::atomic<uint32_t> alloc_seq{0};
std::atomic<uint32_t> refuse_count{0};
std::atomic<bool> expand_in_progress{false};

auto round_up_growth(size_t count) -> size_t {
    if (count == 0) {
        return 0;
    }
    size_t rem = count % PKT_POOL_GROW_CHUNK;
    if (rem == 0) {
        return count;
    }
    return count + (PKT_POOL_GROW_CHUNK - rem);
}

void pkt_debug_dump_in_use(size_t avail) {
    std::array<SiteSummary, DEBUG_SITE_TRACK_SLOTS> site_counts{};
    size_t outstanding = 0;

    for (PacketChunk* chunk = chunk_list; chunk != nullptr; chunk = chunk->next) {
        for (size_t i = 0; i < chunk->count; i++) {
            PacketBuffer* pkt = &chunk->base[i];
            if (!pkt->debug_in_use) {
                continue;
            }

            outstanding++;

            size_t slot = DEBUG_SITE_TRACK_SLOTS;
            for (size_t j = 0; j < DEBUG_SITE_TRACK_SLOTS; j++) {
                if (site_counts[j].site == pkt->debug_alloc_site) {
                    slot = j;
                    break;
                }
            }

            if (slot == DEBUG_SITE_TRACK_SLOTS) {
                for (size_t j = 0; j < DEBUG_SITE_TRACK_SLOTS; j++) {
                    if (site_counts[j].count == 0) {
                        slot = j;
                        site_counts[j].site = pkt->debug_alloc_site;
                        site_counts[j].oldest_seq = pkt->debug_alloc_seq;
                        site_counts[j].oldest_pkt = pkt;
                        break;
                    }
                }
            }

            if (slot != DEBUG_SITE_TRACK_SLOTS && site_counts[slot].site == pkt->debug_alloc_site) {
                site_counts[slot].count++;
                if (pkt->debug_alloc_seq < site_counts[slot].oldest_seq) {
                    site_counts[slot].oldest_seq = pkt->debug_alloc_seq;
                    site_counts[slot].oldest_pkt = pkt;
                }
            }
        }
    }

    std::ranges::sort(site_counts, [](const SiteSummary& a, const SiteSummary& b) -> bool { return a.count > b.count; });

    log::debug("pkt pool snapshot: free=%zu reserve=%zu outstanding=%zu capacity=%zu", avail, RX_RESERVE, outstanding, pool_capacity);
    for (size_t i = 0; i < DEBUG_TOP_SITES && i < site_counts.size(); i++) {
        const auto& entry = site_counts[i];
        if (entry.count == 0 || entry.oldest_pkt == nullptr) {
            continue;
        }
        log::debug("pkt holder: site=%p count=%zu oldest_seq=%u pkt=%p len=%zu dev=%p proto=0x%04x alloc_cpu=%u free_site=%p",
                   reinterpret_cast<void*>(entry.site), entry.count, entry.oldest_seq, static_cast<const void*>(entry.oldest_pkt),
                   entry.oldest_pkt->len, static_cast<void*>(entry.oldest_pkt->dev), entry.oldest_pkt->protocol,
                   entry.oldest_pkt->debug_alloc_cpu, reinterpret_cast<void*>(entry.oldest_pkt->debug_free_site));
    }
}

void add_buffers_to_pool(size_t count) {
    // Allocate new buffers
    auto* new_buffers = new (std::nothrow) PacketBuffer[count]{};
    if (new_buffers == nullptr) {
        log::error("Failed to allocate %zu packet buffers", count);
        return;
    }

    auto* chunk = new (std::nothrow) PacketChunk{};
    if (chunk == nullptr) {
        delete[] new_buffers;
        log::error("Failed to allocate packet chunk metadata for %zu buffers", count);
        return;
    }
    chunk->base = new_buffers;
    chunk->count = count;

    pool_lock.lock();
    chunk->next = chunk_list;
    chunk_list = chunk;
    // Link new buffers into free list
    for (size_t i = 0; i < count; i++) {
        new_buffers[i].next = free_list;
        free_list = &new_buffers[i];
    }
    pool_capacity += count;
    pool_lock.unlock();

    free_count.fetch_add(count, std::memory_order_relaxed);

    log::debug("Added %zu packet buffers (total: %zu)", count, pool_capacity);
}

auto pkt_pool_try_grow(size_t min_free, const char* reason) -> bool {
    size_t free_now = free_count.load(std::memory_order_relaxed);
    if (free_now >= min_free) {
        return true;
    }

    bool expected = false;
    if (!expand_in_progress.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return free_count.load(std::memory_order_relaxed) >= min_free;
    }

    free_now = free_count.load(std::memory_order_relaxed);
    if (free_now < min_free) {
        size_t deficit = min_free - free_now;
        size_t grow_by = round_up_growth(std::max(deficit, PKT_POOL_GROW_CHUNK));
        log::debug("Growing packet pool by %zu buffers for %s (free=%zu target=%zu capacity=%zu)", grow_by, reason, free_now, min_free,
                   pool_capacity);
        add_buffers_to_pool(grow_by);
    }

    expand_in_progress.store(false, std::memory_order_release);
    return free_count.load(std::memory_order_relaxed) >= min_free;
}

// Allocate from global pool
auto pkt_global_alloc() -> PacketBuffer* {
    uint64_t flags = pool_lock.lock_irqsave();
    if (free_list == nullptr) {
        pool_lock.unlock_irqrestore(flags);
        return nullptr;
    }
    auto* pkt = free_list;
    free_list = pkt->next;
    pool_lock.unlock_irqrestore(flags);
    return pkt;
}

// Return to global pool (IRQ-safe - see pkt_global_alloc comment).
void pkt_global_free(PacketBuffer* pkt) {
    uint64_t flags = pool_lock.lock_irqsave();
    pkt->next = free_list;
    free_list = pkt;
    pool_lock.unlock_irqrestore(flags);
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
}

auto pkt_pool_size() -> size_t { return pool_capacity; }

auto pkt_pool_free_count() -> size_t { return free_count.load(std::memory_order_relaxed); }

void pkt_pool_ensure_free(size_t min_free) { static_cast<void>(pkt_pool_try_grow(min_free, "runtime")); }

auto pkt_alloc() -> PacketBuffer* {
    PacketBuffer* pkt = pkt_global_alloc();
    if (pkt == nullptr) {
        return nullptr;
    }

    // Initialize the packet buffer
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    pkt->len = 0;
    pkt->next = nullptr;
    pkt->dev = nullptr;
    pkt->protocol = 0;
    pkt->debug_in_use = true;
    pkt->debug_alloc_cpu = static_cast<uint16_t>(ker::mod::cpu::current_cpu());
    pkt->debug_alloc_seq = alloc_seq.fetch_add(1, std::memory_order_relaxed) + 1;
    pkt->debug_alloc_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    pkt->debug_free_cpu = 0;
    pkt->debug_free_site = 0;

    free_count.fetch_sub(1, std::memory_order_relaxed);

    return pkt;
}

auto pkt_alloc_tx() -> PacketBuffer* {
    size_t avail = free_count.load(std::memory_order_relaxed);
    if (avail <= RX_RESERVE) {
        static_cast<void>(pkt_pool_try_grow(RX_RESERVE + PKT_POOL_GROW_CHUNK, "tx reserve"));
        avail = free_count.load(std::memory_order_relaxed);
    }
    if (avail <= RX_RESERVE) {
        uint32_t count = refuse_count.fetch_add(1, std::memory_order_relaxed) + 1;
        log::error("pkt_alloc_tx: REFUSED (free=%zu reserve=%zu refused=%u)", avail, RX_RESERVE, count);
        if ((count % REFUSE_DUMP_STRIDE) == 1) {
            pkt_debug_dump_in_use(avail);
        }
        return nullptr;  // Pool too low, reserve for RX
    }

    PacketBuffer* pkt = pkt_alloc();
    if (pkt != nullptr) {
        pkt->debug_alloc_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    }
    return pkt;
}

void pkt_free(PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }

    pkt->debug_in_use = false;
    pkt->debug_free_cpu = static_cast<uint16_t>(ker::mod::cpu::current_cpu());
    pkt->debug_free_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    pkt_global_free(pkt);
    free_count.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace ker::net
