#include "packet.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/netdevice.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>

#ifdef WOS_NET_PACKET_DEBUG
#include <platform/asm/cpu.hpp>
#endif

namespace ker::net {

using log = ker::mod::dbg::logger<"net">;

namespace {

#ifdef WOS_NET_PACKET_DEBUG
constexpr size_t DEBUG_TOP_SITES = 8;
constexpr uint32_t REFUSE_DUMP_STRIDE = 64;
#endif

struct PacketChunk {
    PacketBuffer** buffers = nullptr;
    size_t count = 0;
    size_t free = 0;
    bool reclaimable = false;
    bool draining = false;
    PacketChunk* next = nullptr;
};

#ifdef WOS_NET_PACKET_DEBUG
struct SiteSummary {
    uintptr_t site = 0;
    size_t count = 0;
    uint32_t oldest_seq = UINT32_MAX;
    const PacketBuffer* oldest_pkt = nullptr;
};

constexpr size_t DEBUG_SITE_TRACK_SLOTS = 32;

auto packet_debug_cpu() -> uint16_t { return static_cast<uint16_t>(ker::mod::cpu::get_current_cpu_id_safe()); }
#endif

// ---------------------------------------------------------------------------
// Global pool (fallback)
// ---------------------------------------------------------------------------
size_t pool_capacity = 0;
size_t pool_reserve_capacity = 0;
PacketBuffer* reserve_free_list = nullptr;
PacketBuffer* reclaimable_free_list = nullptr;
PacketChunk* chunk_list = nullptr;
ker::mod::sys::Spinlock pool_lock;
bool initialized = false;

// Approximate count of free buffers (global pool + per-CPU caches).
// Decremented on alloc, incremented on free.  Used by pkt_alloc_tx()
// to cheaply check whether we should reserve buffers for RX.
std::atomic<size_t> free_count{0};
#ifdef WOS_NET_PACKET_DEBUG
std::atomic<uint32_t> alloc_seq{0};
#endif
std::atomic<uint32_t> refuse_count{0};
std::atomic<bool> expand_in_progress{false};

auto round_up_growth(size_t count) -> size_t {
    if (count == 0) {
        return 0;
    }
    size_t const REM = count % PKT_POOL_GROW_CHUNK;
    if (REM == 0) {
        return count;
    }
    return count + (PKT_POOL_GROW_CHUNK - REM);
}

auto baseline_pool_capacity() -> size_t {
    size_t const NIC_COUNT = netdev_count();
    return std::max(NIC_COUNT * PKT_POOL_PER_NIC, PKT_POOL_MIN_SIZE);
}

constexpr size_t align_up(size_t val, size_t align) { return (val + align - 1U) & ~(align - 1U); }

constexpr size_t PACKET_BUFFER_ALLOC_BYTES = align_up(sizeof(PacketBuffer), ker::mod::mm::paging::PAGE_SIZE);
static_assert(offsetof(PacketBuffer, storage) == 0, "virtio RX DMA assumes packet storage starts at allocation base");
static_assert(PACKET_BUFFER_ALLOC_BYTES >= PKT_BUF_SIZE, "packet DMA allocation must cover advertised RX buffer length");

auto alloc_dma_packet_buffer(ker::mod::mm::PhysicalPageOwner owner) -> PacketBuffer* {
    void* mem = ker::mod::mm::phys::page_alloc_may_fail(owner, PACKET_BUFFER_ALLOC_BYTES, "net_packet_buffer");
    if (mem == nullptr) {
        return nullptr;
    }
    return new (mem) PacketBuffer{};
}

void free_dma_packet_buffer(PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }
    pkt->~PacketBuffer();
    ker::mod::mm::phys::page_free(pkt);
}

void free_packet_buffer_array(PacketBuffer** buffers, size_t count) {
    if (buffers == nullptr) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        free_dma_packet_buffer(buffers[i]);
    }
    delete[] buffers;
}

#ifdef WOS_NET_PACKET_DEBUG
void pkt_debug_dump_in_use(size_t avail) {
    std::array<SiteSummary, DEBUG_SITE_TRACK_SLOTS> site_counts{};
    size_t outstanding = 0;

    for (PacketChunk const* chunk = chunk_list; chunk != nullptr; chunk = chunk->next) {
        for (size_t i = 0; i < chunk->count; i++) {
            PacketBuffer const* pkt = chunk->buffers[i];
            if (pkt == nullptr || !pkt->debug_in_use) {
                continue;
            }

            outstanding++;

            size_t slot = DEBUG_SITE_TRACK_SLOTS;
            for (size_t j = 0; j < DEBUG_SITE_TRACK_SLOTS; j++) {
                if (site_counts.at(j).site == pkt->debug_alloc_site) {
                    slot = j;
                    break;
                }
            }

            if (slot == DEBUG_SITE_TRACK_SLOTS) {
                for (size_t j = 0; j < DEBUG_SITE_TRACK_SLOTS; j++) {
                    if (site_counts.at(j).count == 0) {
                        slot = j;
                        auto& summary = site_counts.at(j);
                        summary.site = pkt->debug_alloc_site;
                        summary.oldest_seq = pkt->debug_alloc_seq;
                        summary.oldest_pkt = pkt;
                        break;
                    }
                }
            }

            if (slot != DEBUG_SITE_TRACK_SLOTS && site_counts.at(slot).site == pkt->debug_alloc_site) {
                auto& summary = site_counts.at(slot);
                summary.count++;
                if (pkt->debug_alloc_seq < summary.oldest_seq) {
                    summary.oldest_seq = pkt->debug_alloc_seq;
                    summary.oldest_pkt = pkt;
                }
            }
        }
    }

    std::ranges::sort(site_counts, [](const SiteSummary& a, const SiteSummary& b) -> bool { return a.count > b.count; });

    log::debug("pkt pool snapshot: free=%zu reserve=%zu outstanding=%zu capacity=%zu", avail, PKT_POOL_TX_RESERVE, outstanding,
               pool_capacity);
    for (size_t i = 0; i < DEBUG_TOP_SITES; i++) {
        const auto& entry = site_counts.at(i);
        if (entry.count == 0 || entry.oldest_pkt == nullptr) {
            continue;
        }
        log::debug("pkt holder: site=%p count=%zu oldest_seq=%u pkt=%p len=%zu dev=%p proto=0x%04x alloc_cpu=%u free_site=%p",
                   reinterpret_cast<void*>(entry.site), entry.count, entry.oldest_seq, static_cast<const void*>(entry.oldest_pkt),
                   entry.oldest_pkt->len, static_cast<void*>(entry.oldest_pkt->dev), entry.oldest_pkt->protocol,
                   entry.oldest_pkt->debug_alloc_cpu, reinterpret_cast<void*>(entry.oldest_pkt->debug_free_site));
    }
}
#endif

auto add_buffers_to_pool(size_t count, bool reclaimable) -> bool {
    auto* new_buffers = new (std::nothrow) PacketBuffer*[count]{};
    if (new_buffers == nullptr) {
        log::error("Failed to allocate packet pointer table for %zu buffers", count);
        return false;
    }

    auto* chunk = new (std::nothrow) PacketChunk{};
    if (chunk == nullptr) {
        delete[] new_buffers;
        log::error("Failed to allocate packet chunk metadata for %zu buffers", count);
        return false;
    }
    size_t allocated = 0;
    ker::mod::mm::PhysicalPageOwner const OWNER =
        reclaimable ? ker::mod::mm::PhysicalPageOwner::NETWORK_PACKET : ker::mod::mm::PhysicalPageOwner::NETWORK_PACKET_RESERVE;
    for (; allocated < count; ++allocated) {
        new_buffers[allocated] = alloc_dma_packet_buffer(OWNER);
        if (new_buffers[allocated] == nullptr) {
            break;
        }
    }
    if (allocated != count) {
        free_packet_buffer_array(new_buffers, allocated);
        delete chunk;
        log::error("Failed to allocate physically contiguous packet buffers (%zu/%zu)", allocated, count);
        return false;
    }
    chunk->buffers = new_buffers;
    chunk->count = count;
    chunk->free = count;
    chunk->reclaimable = reclaimable;

    pool_lock.lock();
    chunk->next = chunk_list;
    chunk_list = chunk;
    // Keep permanent and reclaimable buffers on separate O(1) free lists.
    // Persistent RX descriptors prefer the permanent list so runtime growth
    // chunks remain transient and can be released without device traffic.
    PacketBuffer*& target_free_list = reclaimable ? reclaimable_free_list : reserve_free_list;
    for (size_t i = 0; i < count; i++) {
        new_buffers[i]->pool_chunk = chunk;
        new_buffers[i]->next = target_free_list;
        target_free_list = new_buffers[i];
    }
    pool_capacity += count;
    if (!reclaimable) {
        pool_reserve_capacity += count;
    }
    free_count.fetch_add(count, std::memory_order_relaxed);
    size_t const TOTAL = pool_capacity;
    pool_lock.unlock();

    log::debug("Added %zu packet buffers (total: %zu)", count, TOTAL);
    return true;
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
        size_t const DEFICIT = min_free - free_now;
        size_t const GROW_BY = round_up_growth(std::max(DEFICIT, PKT_POOL_GROW_CHUNK));
        log::debug("Growing packet pool by %zu buffers for %s (free=%zu target=%zu capacity=%zu)", GROW_BY, reason, free_now, min_free,
                   pool_capacity);
        static_cast<void>(add_buffers_to_pool(GROW_BY, true));
    }

    expand_in_progress.store(false, std::memory_order_release);
    return free_count.load(std::memory_order_relaxed) >= min_free;
}

// Allocate from the global pool. Ordinary/transient users preserve the
// dynamic-first behavior; RX descriptors prefer the permanent reserve.
auto pkt_global_alloc(bool prefer_reserve) -> PacketBuffer* {
    uint64_t const FLAGS = pool_lock.lock_irqsave();
    bool const USE_RESERVE = (prefer_reserve && reserve_free_list != nullptr) || reclaimable_free_list == nullptr;
    PacketBuffer** selected_list = USE_RESERVE ? &reserve_free_list : &reclaimable_free_list;
    if (*selected_list == nullptr) {
        pool_lock.unlock_irqrestore(FLAGS);
        return nullptr;
    }
    auto* pkt = *selected_list;
    *selected_list = pkt->next;
    auto* chunk = static_cast<PacketChunk*>(pkt->pool_chunk);
    if (chunk == nullptr || chunk->free == 0) {
        ker::mod::dbg::panic_handler("packet pool allocation accounting corrupt");
        __builtin_unreachable();
    }
    chunk->free--;
    free_count.fetch_sub(1, std::memory_order_relaxed);
    pool_lock.unlock_irqrestore(FLAGS);
    return pkt;
}

// Return to global pool (IRQ-safe - see pkt_global_alloc comment).
void pkt_global_free(PacketBuffer* pkt) {
    uint64_t const FLAGS = pool_lock.lock_irqsave();
    auto* chunk = static_cast<PacketChunk*>(pkt->pool_chunk);
    if (chunk == nullptr || chunk->free >= chunk->count) {
        ker::mod::dbg::panic_handler("packet pool free accounting corrupt");
        __builtin_unreachable();
    }
    chunk->free++;
    if (!chunk->draining) {
        PacketBuffer*& target_free_list = chunk->reclaimable ? reclaimable_free_list : reserve_free_list;
        pkt->next = target_free_list;
        target_free_list = pkt;
        free_count.fetch_add(1, std::memory_order_relaxed);
    }
    pool_lock.unlock_irqrestore(FLAGS);
}

}  // namespace

void pkt_pool_init() {
    if (initialized) {
        return;
    }

    // Start with minimum pool size
    static_cast<void>(add_buffers_to_pool(PKT_POOL_MIN_SIZE, false));
    initialized = true;
}

void pkt_pool_expand_for_nics() {
    // Calculate required size: 1024 buffers per NIC, minimum 1024 total
    size_t const REQUIRED = baseline_pool_capacity();

    // Runtime growth never substitutes for the configured permanent reserve.
    // A fully free growth chunk must remain independently reclaimable.
    uint64_t const FLAGS = pool_lock.lock_irqsave();
    size_t const RESERVE_CAPACITY = pool_reserve_capacity;
    pool_lock.unlock_irqrestore(FLAGS);
    if (REQUIRED > RESERVE_CAPACITY) {
        size_t const TO_ADD = REQUIRED - RESERVE_CAPACITY;
        static_cast<void>(add_buffers_to_pool(TO_ADD, false));
    }
}

void pkt_pool_reserve_for_rx_descriptors(size_t descriptor_count) {
    if (descriptor_count > SIZE_MAX - PKT_POOL_RX_REFILL_RESERVE) {
        log::error("RX descriptor packet reserve size overflow (%zu)", descriptor_count);
        return;
    }
    size_t const REQUIRED_FREE = descriptor_count + PKT_POOL_RX_REFILL_RESERVE;
    size_t reserve_free = 0;
    uint64_t const FLAGS = pool_lock.lock_irqsave();
    for (PacketChunk const* chunk = chunk_list; chunk != nullptr; chunk = chunk->next) {
        if (!chunk->reclaimable) {
            reserve_free += chunk->free;
        }
    }
    pool_lock.unlock_irqrestore(FLAGS);
    if (reserve_free < REQUIRED_FREE) {
        static_cast<void>(add_buffers_to_pool(REQUIRED_FREE - reserve_free, false));
    }
}

auto pkt_pool_size() -> size_t { return pool_capacity; }

auto pkt_pool_free_count() -> size_t { return free_count.load(std::memory_order_relaxed); }

namespace {

void fill_packet_pool_snapshot_common_locked(PacketPoolSnapshot& snapshot) {
    snapshot.capacity = pool_capacity;
    snapshot.baseline_capacity = pool_reserve_capacity;
    snapshot.free = free_count.load(std::memory_order_relaxed);
    snapshot.active_capacity = snapshot.capacity;
    for (PacketChunk const* chunk = chunk_list; chunk != nullptr; chunk = chunk->next) {
        snapshot.used += chunk->count - chunk->free;
        if (chunk->draining) {
            snapshot.active_capacity -= chunk->count;
            snapshot.draining_buffers += chunk->count;
            snapshot.draining_free += chunk->free;
        }
    }
    snapshot.rx_reserve = PKT_POOL_RX_REFILL_RESERVE;
    snapshot.grow_chunk = PKT_POOL_GROW_CHUNK;
    snapshot.buffer_size = PKT_BUF_SIZE;
    snapshot.object_size = sizeof(PacketBuffer);
    snapshot.headroom = PKT_HEADROOM;
    snapshot.tx_refused = refuse_count.load(std::memory_order_relaxed);
    snapshot.expand_in_progress = expand_in_progress.load(std::memory_order_acquire);
}

}  // namespace

auto pkt_pool_snapshot() -> PacketPoolSnapshot {
    PacketPoolSnapshot snapshot{};
    uint64_t const FLAGS = pool_lock.lock_irqsave();
    fill_packet_pool_snapshot_common_locked(snapshot);
    pool_lock.unlock_irqrestore(FLAGS);
    return snapshot;
}

auto pkt_pool_try_snapshot(PacketPoolSnapshot& snapshot) -> bool {
    snapshot = {};

    uint64_t flags = 0;
    asm volatile("pushfq; popq %0" : "=r"(flags));
    asm volatile("cli" ::: "memory");
    bool const LOCKED = pool_lock.try_lock();
    if (LOCKED) {
        fill_packet_pool_snapshot_common_locked(snapshot);
        pool_lock.unlock();
    }
    if ((flags & 0x200) != 0) {
        asm volatile("sti" ::: "memory");
    }
    return LOCKED;
}

auto pkt_pool_reclaim_free(size_t target_capacity) -> PacketPoolReclaimStats {
    PacketPoolReclaimStats stats{};
    PacketChunk* retired = nullptr;
    uint64_t const FLAGS = pool_lock.lock_irqsave();
    target_capacity = std::max(target_capacity, pool_reserve_capacity);
    PacketPoolSnapshot before{};
    fill_packet_pool_snapshot_common_locked(before);
    stats.before_capacity = pool_capacity;
    stats.before_free = before.free;
    stats.before_draining_buffers = before.draining_buffers;
    stats.before_draining_free = before.draining_free;

    PacketChunk** chunk_link = &chunk_list;
    while (*chunk_link != nullptr) {
        PacketChunk* chunk = *chunk_link;
        if (!chunk->reclaimable || chunk->free != chunk->count || pool_capacity < chunk->count ||
            pool_capacity - chunk->count < target_capacity) {
            chunk_link = &chunk->next;
            continue;
        }

        size_t removed = 0;
        PacketBuffer** free_link = chunk->reclaimable ? &reclaimable_free_list : &reserve_free_list;
        while (*free_link != nullptr) {
            if ((*free_link)->pool_chunk == chunk) {
                *free_link = (*free_link)->next;
                removed++;
            } else {
                free_link = &(*free_link)->next;
            }
        }
        size_t const EXPECTED_FREE_LIST_BUFFERS = chunk->draining ? 0 : chunk->count;
        if (removed != EXPECTED_FREE_LIST_BUFFERS) {
            ker::mod::dbg::panic_handler("packet pool reclaim free-list mismatch");
        }

        *chunk_link = chunk->next;
        pool_capacity -= chunk->count;
        if (!chunk->draining) {
            free_count.fetch_sub(chunk->count, std::memory_order_relaxed);
        }
        stats.freed_chunks++;
        stats.freed_buffers += chunk->count;
        chunk->next = retired;
        retired = chunk;
    }

    PacketPoolSnapshot after_retire{};
    fill_packet_pool_snapshot_common_locked(after_retire);
    size_t projected_capacity = pool_capacity - after_retire.draining_buffers;
    size_t active_capacity = after_retire.active_capacity;
    for (PacketChunk* chunk = chunk_list; chunk != nullptr && projected_capacity > target_capacity; chunk = chunk->next) {
        if (!chunk->reclaimable || chunk->draining || active_capacity < chunk->count ||
            active_capacity - chunk->count < pool_reserve_capacity) {
            continue;
        }

        size_t removed = 0;
        PacketBuffer** free_link = &reclaimable_free_list;
        while (*free_link != nullptr) {
            if ((*free_link)->pool_chunk == chunk) {
                *free_link = (*free_link)->next;
                removed++;
            } else {
                free_link = &(*free_link)->next;
            }
        }
        if (removed != chunk->free) {
            ker::mod::dbg::panic_handler("packet pool drain free-list mismatch");
        }

        chunk->draining = true;
        free_count.fetch_sub(removed, std::memory_order_relaxed);
        active_capacity -= chunk->count;
        projected_capacity -= chunk->count;
        stats.marked_draining_chunks++;
        stats.marked_draining_buffers += chunk->count;
        stats.deactivated_free_buffers += removed;
    }

    PacketPoolSnapshot after{};
    fill_packet_pool_snapshot_common_locked(after);
    stats.after_capacity = pool_capacity;
    stats.after_free = after.free;
    stats.after_draining_buffers = after.draining_buffers;
    stats.after_draining_free = after.draining_free;
    pool_lock.unlock_irqrestore(FLAGS);

    while (retired != nullptr) {
        PacketChunk* chunk = retired;
        retired = retired->next;
        free_packet_buffer_array(chunk->buffers, chunk->count);
        delete chunk;
    }
    return stats;
}

auto pkt_pool_reclaim_for_pressure() -> size_t { return pkt_pool_reclaim_free(0).freed_buffers; }

auto pkt_pool_populate_reclaimable(size_t count) -> bool {
    if (count == 0 || count > PKT_POOL_DIAGNOSTIC_GROW_MAX) {
        return false;
    }
    return add_buffers_to_pool(round_up_growth(count), true);
}

void pkt_pool_ensure_free(size_t min_free) { static_cast<void>(pkt_pool_try_grow(min_free, "runtime")); }

auto pkt_alloc() -> PacketBuffer* {
    PacketBuffer* pkt = pkt_global_alloc(false);
    if (pkt == nullptr) {
        return nullptr;
    }

    // Initialize the packet buffer
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    pkt->len = 0;
    pkt->next = nullptr;
    pkt->dev = nullptr;
    pkt->lifetime_ctx = nullptr;
    pkt->lifetime_release = nullptr;
    pkt->protocol = 0;
#ifdef WOS_NET_PACKET_DEBUG
    pkt->debug_in_use = true;
    pkt->debug_alloc_cpu = packet_debug_cpu();
    pkt->debug_alloc_seq = alloc_seq.fetch_add(1, std::memory_order_relaxed) + 1;
    pkt->debug_alloc_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    pkt->debug_free_cpu = 0;
    pkt->debug_free_site = 0;
#endif

    return pkt;
}

auto pkt_alloc_rx() -> PacketBuffer* {
    PacketBuffer* pkt = pkt_global_alloc(true);
    if (pkt == nullptr) {
        return nullptr;
    }

    // Initialize the packet buffer
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    pkt->len = 0;
    pkt->next = nullptr;
    pkt->dev = nullptr;
    pkt->lifetime_ctx = nullptr;
    pkt->lifetime_release = nullptr;
    pkt->protocol = 0;
#ifdef WOS_NET_PACKET_DEBUG
    pkt->debug_in_use = true;
    pkt->debug_alloc_cpu = packet_debug_cpu();
    pkt->debug_alloc_seq = alloc_seq.fetch_add(1, std::memory_order_relaxed) + 1;
    pkt->debug_alloc_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    pkt->debug_free_cpu = 0;
    pkt->debug_free_site = 0;
#endif

    return pkt;
}

auto pkt_alloc_tx() -> PacketBuffer* {
    size_t avail = free_count.load(std::memory_order_relaxed);
    if (avail <= PKT_POOL_TX_RESERVE) {
        static_cast<void>(pkt_pool_try_grow(PKT_POOL_RX_REFILL_RESERVE, "tx reserve"));
        avail = free_count.load(std::memory_order_relaxed);
    }
    if (avail <= PKT_POOL_TX_RESERVE) {
        uint32_t const COUNT = refuse_count.fetch_add(1, std::memory_order_relaxed) + 1;
        log::error("pkt_alloc_tx: REFUSED (free=%zu reserve=%zu refused=%u)", avail, PKT_POOL_TX_RESERVE, COUNT);
#ifdef WOS_NET_PACKET_DEBUG
        if ((COUNT % REFUSE_DUMP_STRIDE) == 1) {
            pkt_debug_dump_in_use(avail);
        }
#endif
        return nullptr;  // Pool too low, reserve for RX
    }

    PacketBuffer* pkt = pkt_alloc();
#ifdef WOS_NET_PACKET_DEBUG
    if (pkt != nullptr) {
        pkt->debug_alloc_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    }
#endif
    return pkt;
}

void pkt_free(PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }

    auto* release = pkt->lifetime_release;
    void* release_ctx = pkt->lifetime_ctx;
    pkt->lifetime_release = nullptr;
    pkt->lifetime_ctx = nullptr;
    if (release != nullptr) {
        release(release_ctx);
    }

#ifdef WOS_NET_PACKET_DEBUG
    pkt->debug_in_use = false;
    pkt->debug_free_cpu = packet_debug_cpu();
    pkt->debug_free_site = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
#endif
    pkt_global_free(pkt);
}

}  // namespace ker::net
