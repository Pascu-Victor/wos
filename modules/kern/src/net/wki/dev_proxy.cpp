#include "dev_proxy.hpp"

#include <array>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <memory>
#include <net/netpoll.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>

#define DEBUG_WKI_TRANSPORT

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {
std::deque<std::unique_ptr<ProxyBlockState>> g_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_dev_proxy_initialized = false;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_proxy_lock;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Caller must hold s_proxy_lock.
auto find_proxy_by_bdev(ker::dev::BlockDevice* bdev) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if (p->active && &p->bdev == bdev) {
            return p.get();
        }
    }
    return nullptr;
}

// Caller must hold s_proxy_lock.
auto find_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if ((p->active || p->op_pending) && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

// Caller must hold s_proxy_lock.
auto find_proxy_by_attach(uint16_t owner_node) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if (p->attach_pending && p->owner_node == owner_node) {
            return p.get();
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// RDMA ring helpers
// -----------------------------------------------------------------------------

// Allocate a data slot from the bitmap. Returns slot index or -1 if none free.
auto rdma_alloc_slot(ProxyBlockState* state) -> int {
    uint32_t max_slots = BLK_RING_DEFAULT_DATA_SLOTS < 64 ? BLK_RING_DEFAULT_DATA_SLOTS : 64;
    for (uint32_t i = 0; i < max_slots; i++) {
        if ((state->data_slot_bitmap & (1ULL << i)) == 0) {
            state->data_slot_bitmap |= (1ULL << i);
            return static_cast<int>(i);
        }
    }
    return -1;
}

void rdma_free_slot(ProxyBlockState* state, uint32_t slot) { state->data_slot_bitmap &= ~(1ULL << slot); }

// Allocate a tag from the bitmap pool. Returns tag index (0..63) or -1 if none free.
auto rdma_alloc_tag(ProxyBlockState* state) -> int {
    for (uint32_t i = 0; i < ProxyBlockState::TAG_POOL_SIZE; i++) {
        if ((state->tag_bitmap & (1ULL << i)) == 0) {
            state->tag_bitmap |= (1ULL << i);
            state->tag_completions[i].pending = true;
            state->tag_completions[i].completed = false;
            return static_cast<int>(i);
        }
    }
    return -1;
}

void rdma_free_tag(ProxyBlockState* state, uint32_t tag) {
    if (tag < ProxyBlockState::TAG_POOL_SIZE) {
        state->tag_bitmap &= ~(1ULL << tag);
        state->tag_completions[tag].pending = false;
        state->tag_completions[tag].completed = false;
    }
}

// RoCE helper: push SQ region (header + SQ entries) to server so it can see new SQEs.
void roce_push_sq(ProxyBlockState* state) {
    if (!state->rdma_roce || state->rdma_transport == nullptr) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    // Push header (contains sq_head) + entire SQ ring
    uint32_t sq_region_size = BLK_RING_HEADER_SIZE + blk_ring_sq_size(hdr->sq_depth);
    state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0, state->rdma_zone_ptr,
                                      sq_region_size);
}

// RoCE helper: push a data slot to the server (for WRITE ops — data must be visible before SQE).
void roce_push_data_slot(ProxyBlockState* state, uint32_t slot, uint32_t bytes) {
    if (!state->rdma_roce || state->rdma_transport == nullptr || bytes == 0) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    uint32_t slot_offset = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (slot * hdr->data_slot_size);
    state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, slot_offset,
                                      blk_data_slot(state->rdma_zone_ptr, hdr, slot), bytes);
}

// RoCE helper: pull CQ region + header from server to see completions.
// Saves and restores consumer-owned header fields (sq_head, cq_tail) to prevent
// the full header read from clobbering them with stale server-side copies.
void roce_pull_cq(ProxyBlockState* state) {
    if (!state->rdma_roce || state->rdma_transport == nullptr) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);

    // Save consumer-owned fields before the pull overwrites them
    uint32_t saved_sq_head = hdr->sq_head;
    uint32_t saved_cq_tail = hdr->cq_tail;

    // Pull CQ entries
    uint32_t cq_off = blk_ring_cq_offset(hdr->sq_depth);
    uint32_t cq_total = blk_ring_cq_size(hdr->cq_depth);
    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, cq_off,
                                     static_cast<uint8_t*>(state->rdma_zone_ptr) + cq_off, cq_total);
    // Pull updated header (cq_head changed by server)
    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0, state->rdma_zone_ptr,
                                     BLK_RING_HEADER_SIZE);

    // Restore consumer-owned fields — the server's copy of sq_head/cq_tail may
    // be stale (lagging behind the consumer's latest values)
    hdr->sq_head = saved_sq_head;
    hdr->cq_tail = saved_cq_tail;
}

// RoCE helper: pull a data slot from server (for READ ops — data filled by server).
void roce_pull_data_slot(ProxyBlockState* state, uint32_t slot, uint32_t bytes) {
    if (!state->rdma_roce || state->rdma_transport == nullptr || bytes == 0) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    uint32_t slot_offset = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (slot * hdr->data_slot_size);
    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, slot_offset,
                                     blk_data_slot(state->rdma_zone_ptr, hdr, slot), bytes);
}

// -----------------------------------------------------------------------------
// Read-ahead cache helpers
// -----------------------------------------------------------------------------

void ra_invalidate(ProxyBlockState* state) {
    state->ra_valid = false;
    state->ra_block_count = 0;
}

auto ra_cache_hit(ProxyBlockState* state, uint64_t lba, uint32_t count) -> bool {
    if (!state->ra_valid || state->ra_buffer == nullptr) {
        return false;
    }
    return lba >= state->ra_base_lba && (lba + count) <= (state->ra_base_lba + state->ra_block_count);
}

// Server-push CQ drain: reads all available CQEs into per-tag completion array.
// In multi-outstanding mode, multiple CQEs may arrive for different tags.
auto rdma_wait_cqe_push(ProxyBlockState* state, uint32_t tag, BlkCqEntry* out_cqe) -> bool {
    // Check if already completed in a previous drain
    if (tag < ProxyBlockState::TAG_POOL_SIZE && state->tag_completions[tag].completed) {
        *out_cqe = state->tag_completions[tag].cqe;
        state->tag_completions[tag].completed = false;
        return true;
    }

    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);

    asm volatile("" ::: "memory");  // read barrier

    // Drain all available CQEs into per-tag tracking (multi-outstanding mode)
    while (!blk_cq_empty(hdr)) {
        uint32_t idx = hdr->cq_tail % hdr->cq_depth;
        uint32_t ctag = cq[idx].tag;
        if (ctag < ProxyBlockState::TAG_POOL_SIZE && (state->tag_bitmap & (1ULL << ctag)) != 0) {
            state->tag_completions[ctag].cqe = cq[idx];
            state->tag_completions[ctag].completed = true;
            state->tag_completions[ctag].pending = false;
        }
        asm volatile("" ::: "memory");
        hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
    }

    // Check if our target tag completed
    if (tag < ProxyBlockState::TAG_POOL_SIZE && state->tag_completions[tag].completed) {
        *out_cqe = state->tag_completions[tag].cqe;
        state->tag_completions[tag].completed = false;
        return true;
    }

    return false;
}

// Drain all available CQ entries into the per-tag completion tracking array.
void rdma_drain_cq(ProxyBlockState* state) {
    // For RoCE zones: pull CQ + header from server before checking for completions
    roce_pull_cq(state);

    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);

    while (!blk_cq_empty(hdr)) {
        asm volatile("" ::: "memory");  // read barrier
        uint32_t idx = hdr->cq_tail % hdr->cq_depth;
        const auto& cqe = cq[idx];

        // Store completion in per-tag tracking array (O(1) lookup by tag)
        uint32_t tag = cqe.tag;
        if (tag < ProxyBlockState::TAG_POOL_SIZE && (state->tag_bitmap & (1ULL << tag)) != 0) {
            state->tag_completions[tag].cqe = cqe;
            state->tag_completions[tag].completed = true;
            state->tag_completions[tag].pending = false;
        }
        // Tags not in the bitmap are stale/orphaned — safe to drop

        asm volatile("" ::: "memory");  // write barrier before advancing tail
        hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
    }
}

// Drain CQ and look for a specific tag. Returns true if found and fills out_cqe.
auto rdma_drain_cq_for_tag(ProxyBlockState* state, uint32_t tag, BlkCqEntry* out_cqe) -> bool {
    // Check per-tag completion array (O(1) lookup)
    if (tag < ProxyBlockState::TAG_POOL_SIZE && state->tag_completions[tag].completed) {
        *out_cqe = state->tag_completions[tag].cqe;
        state->tag_completions[tag].completed = false;
        return true;
    }

    // Drain fresh CQ entries
    rdma_drain_cq(state);

    // Check again
    if (tag < ProxyBlockState::TAG_POOL_SIZE && state->tag_completions[tag].completed) {
        *out_cqe = state->tag_completions[tag].cqe;
        state->tag_completions[tag].completed = false;
        return true;
    }

    return false;
}

// Signal server that new SQ entries are available (tiered signaling).
void rdma_signal_server(ProxyBlockState* state) {
    auto* peer = wki_peer_find(state->owner_node);
    if (peer == nullptr) {
        return;
    }

    // Tier 1: ivshmem doorbell (near-zero latency)
    if (peer->transport != nullptr && peer->transport->rdma_capable) {
        // ivshmem transport has native doorbell — zone post handler will fire
        ZoneNotifyPayload notify = {};
        notify.zone_id = state->rdma_zone_id;
        notify.op_type = 1;  // WRITE (new SQ entries available)
        wki_send(state->owner_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &notify, sizeof(notify));
        return;
    }

    // Tier 2: RoCE doorbell (raw Ethernet frame)
    if (peer->rdma_transport != nullptr && peer->rdma_transport->doorbell != nullptr) {
        peer->rdma_transport->doorbell(peer->rdma_transport, state->owner_node, state->rdma_zone_id);
        return;
    }

    // Tier 3: WKI reliable message fallback
    ZoneNotifyPayload notify = {};
    notify.zone_id = state->rdma_zone_id;
    notify.op_type = 1;  // WRITE (new SQ entries available)
    wki_send(state->owner_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &notify, sizeof(notify));
}

// -----------------------------------------------------------------------------
// Remote block device function pointers
// -----------------------------------------------------------------------------

// Block until the proxy is no longer fenced, or until the fence timeout expires.
// Returns true if the proxy came back, false if the fence timed out (proxy torn down).
auto wait_for_fence_lift(ProxyBlockState* state) -> bool {
    while (state->fenced) {
        // Check if the fence timeout has expired (hard teardown will clear active)
        if (!state->active) {
            return false;
        }
        asm volatile("pause" ::: "memory");
        // Yield the current task so other kernel work (including the WKI timer
        // thread that processes reconnection) can make progress.
        ker::mod::sched::kern_yield();
    }
    return state->active;
}

// Message-based block read (fallback when RDMA not available)
auto remote_block_read_msg(ProxyBlockState* state, ker::dev::BlockDevice* dev, uint64_t block, uint32_t count, void* buffer) -> int {
    auto* dest = static_cast<uint8_t*>(buffer);
    uint64_t lba = block;
    uint32_t remaining = count;

    // Calculate max blocks per chunk based on response payload capacity
    auto max_resp_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
    uint32_t blocks_per_chunk = max_resp_data / static_cast<uint32_t>(dev->block_size);
    if (blocks_per_chunk == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > blocks_per_chunk) ? blocks_per_chunk : remaining;
        auto chunk_bytes = static_cast<uint16_t>(chunk * static_cast<uint32_t>(dev->block_size));

        // Build request: DevOpReqPayload + {lba:u64, count:u32}
        constexpr uint16_t REQ_DATA_LEN = 12;
        std::array<uint8_t, sizeof(DevOpReqPayload) + REQ_DATA_LEN> req_buf = {};

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
        req->op_id = OP_BLOCK_READ;
        req->data_len = REQ_DATA_LEN;

        auto* req_data = req_buf.data() + sizeof(DevOpReqPayload);
        memcpy(req_data, &lba, sizeof(uint64_t));
        memcpy(req_data + sizeof(uint64_t), &chunk, sizeof(uint32_t));

        // Set up blocking state
        WkiWaitEntry wait = {};
        state->lock.lock();
        state->op_wait_entry = &wait;
        state->op_pending.store(true, std::memory_order_release);
        state->op_status = 0;
        state->op_resp_buf = dest;
        state->op_resp_max = chunk_bytes;
        state->op_resp_len = 0;
        state->lock.unlock();

        // Send request
        int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf.data(),
                                static_cast<uint16_t>(sizeof(DevOpReqPayload) + REQ_DATA_LEN));
        if (send_ret != WKI_OK) {
            state->op_wait_entry = nullptr;
            state->op_pending.store(false, std::memory_order_relaxed);
            return -1;
        }

        // Async wait for response
        int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        state->op_wait_entry = nullptr;
        if (wait_rc == WKI_ERR_TIMEOUT) {
            state->op_pending.store(false, std::memory_order_relaxed);
            return -1;
        }

        if (state->op_status != 0) {
            return static_cast<int>(state->op_status);
        }

        dest += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

// RDMA ring-based block read — with read-ahead cache and server-push optimisation.
//
// Prefetches a full data-slot worth of blocks (typically 64 KB) on each RDMA
// round-trip and caches the excess for subsequent reads.  For RoCE zones the
// server already pushes data + CQ + header via rdma_write, so the consumer
// avoids all rdma_read round-trips (no roce_pull_cq / roce_pull_data_slot).
auto remote_block_read_rdma(ProxyBlockState* state, uint64_t block, uint32_t count, void* buffer) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr) {
        return -1;
    }
    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    uint32_t blk_sz = ring_hdr->block_size;
    uint32_t blocks_per_slot = ring_hdr->data_slot_size / blk_sz;
    if (blocks_per_slot == 0) {
        return -1;
    }

    // -- 1. Serve from read-ahead cache if the request is fully covered ------
    if (ra_cache_hit(state, block, count)) {
        uint64_t off = (block - state->ra_base_lba) * blk_sz;
        memcpy(buffer, state->ra_buffer + off, static_cast<size_t>(count) * blk_sz);
        return 0;
    }

    // -- 2. Cache miss — fetch a full slot starting at the requested LBA -----
    //    Cap at device boundary.
    uint32_t fetch_count = blocks_per_slot;
    if (block + fetch_count > ring_hdr->total_blocks) {
        fetch_count = static_cast<uint32_t>(ring_hdr->total_blocks - block);
    }
    if (fetch_count == 0) {
        return -1;
    }
    auto fetch_bytes = fetch_count * blk_sz;

    // Allocate a data slot
    int slot = rdma_alloc_slot(state);
    if (slot < 0) {
        rdma_drain_cq(state);
        slot = rdma_alloc_slot(state);
        if (slot < 0) {
            return -1;
        }
    }

    // Wait for SQ space
    while (blk_sq_full(ring_hdr)) {
        asm volatile("pause" ::: "memory");
        rdma_drain_cq(state);
    }

    // Post SQE — request the full prefetch range
    int tag_id = rdma_alloc_tag(state);
    if (tag_id < 0) {
        rdma_free_slot(state, static_cast<uint32_t>(slot));
        return -1;
    }
    uint32_t tag = static_cast<uint32_t>(tag_id);
    uint32_t sq_idx = ring_hdr->sq_head % ring_hdr->sq_depth;
    auto& sqe = sq[sq_idx];
    sqe.tag = tag;
    sqe.opcode = static_cast<uint8_t>(BlkOpcode::READ);
    sqe.lba = block;
    sqe.block_count = fetch_count;
    sqe.data_slot = static_cast<uint32_t>(slot);

    asm volatile("" ::: "memory");  // write barrier before advancing head
    ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

    // Push SQ to server (RoCE)
    roce_push_sq(state);

    // Signal server
    rdma_signal_server(state);

    // -- 3. Wait for completion ----------------------------------------------
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    BlkCqEntry cqe = {};
    bool got_cqe = false;

    if (state->rdma_roce) {
        // Server-push mode: the server pushes data slot + CQ + header into our
        // local zone via rdma_write.  wki_spin_yield_channel drives NIC RX so
        // those frames land in local memory.  No rdma_read needed.
        while (!got_cqe) {
            got_cqe = rdma_wait_cqe_push(state, tag, &cqe);
            if (got_cqe) {
                break;
            }
            if (wki_now_us() >= deadline) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
                rdma_free_tag(state, tag);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel(ch);
        }
    } else {
        // ivshmem path: shared memory is coherent, use existing drain logic
        while (!got_cqe) {
            got_cqe = rdma_drain_cq_for_tag(state, tag, &cqe);
            if (got_cqe) {
                break;
            }
            if (wki_now_us() >= deadline) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
                rdma_free_tag(state, tag);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel(ch);
        }
    }

    if (cqe.status != 0) {
        rdma_free_slot(state, static_cast<uint32_t>(slot));
        rdma_free_tag(state, tag);
        ra_invalidate(state);
        return cqe.status;
    }

    // -- 4. Populate read-ahead cache from the data slot ---------------------
    // For RoCE the server already pushed data into our local zone — no pull needed.
    if (!state->rdma_roce) {
        roce_pull_data_slot(state, static_cast<uint32_t>(slot), fetch_bytes);
    }

    auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, static_cast<uint32_t>(slot));

    if (state->ra_buffer != nullptr) {
        memcpy(state->ra_buffer, slot_data, fetch_bytes);
        state->ra_base_lba = block;
        state->ra_block_count = fetch_count;
        state->ra_valid = true;
    }

    rdma_free_slot(state, static_cast<uint32_t>(slot));
    rdma_free_tag(state, tag);

    // -- 5. Copy the originally-requested data to the caller's buffer --------
    memcpy(buffer, state->ra_buffer != nullptr ? state->ra_buffer : slot_data, static_cast<size_t>(count) * blk_sz);

    return 0;
}

// Dispatcher: uses RDMA ring if available, falls back to message-based
auto remote_block_read(ker::dev::BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }

    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }

    // Re-check active after potentially blocking in fence wait
    if (!state->active) {
        return -1;
    }

    auto cnt = static_cast<uint32_t>(count);
    if (state->rdma_attached) {
        return remote_block_read_rdma(state, block, cnt, buffer);
    }
    return remote_block_read_msg(state, dev, block, cnt, buffer);
}

// Message-based block write (fallback when RDMA not available)
auto remote_block_write_msg(ProxyBlockState* state, ker::dev::BlockDevice* dev, uint64_t block, uint32_t count, const void* buffer) -> int {
    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t lba = block;
    uint32_t remaining = count;

    // Calculate max blocks per chunk based on request payload capacity
    // Request: DevOpReqPayload(4) + lba(8) + count(4) + data
    constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
    auto max_req_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);
    uint32_t blocks_per_chunk = max_req_data / static_cast<uint32_t>(dev->block_size);
    if (blocks_per_chunk == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > blocks_per_chunk) ? blocks_per_chunk : remaining;
        auto chunk_bytes = (chunk * static_cast<uint32_t>(dev->block_size));

        // Build request: DevOpReqPayload + {lba:u64, count:u32} + data
        auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + 12 + chunk_bytes);
        auto* req_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_total));
        if (req_buf == nullptr) {
            return -1;
        }

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
        req->op_id = OP_BLOCK_WRITE;
        req->data_len = static_cast<uint16_t>(12 + chunk_bytes);

        auto* req_data = req_buf + sizeof(DevOpReqPayload);
        memcpy(req_data, &lba, sizeof(uint64_t));
        memcpy(req_data + sizeof(uint64_t), &chunk, sizeof(uint32_t));
        memcpy(req_data + 12, src, chunk_bytes);

        // Set up blocking state
        WkiWaitEntry wait = {};
        state->lock.lock();
        state->op_wait_entry = &wait;
        state->op_pending.store(true, std::memory_order_release);
        state->op_status = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->lock.unlock();

        // Send request
        int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
        ker::mod::mm::dyn::kmalloc::free(req_buf);

        if (send_ret != WKI_OK) {
            state->op_wait_entry = nullptr;
            state->op_pending.store(false, std::memory_order_relaxed);
            return -1;
        }

        // Async wait for response
        int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        state->op_wait_entry = nullptr;
        if (wait_rc == WKI_ERR_TIMEOUT) {
            state->op_pending.store(false, std::memory_order_relaxed);
            return -1;
        }

        if (state->op_status != 0) {
            return static_cast<int>(state->op_status);
        }

        src += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

// RDMA ring-based block write — consumer copies data into slot, server reads from it
auto remote_block_write_rdma(ProxyBlockState* state, uint64_t block, uint32_t count, const void* buffer) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr) {
        return -1;
    }
    // Invalidate read-ahead cache — written data may overlap cached range
    ra_invalidate(state);

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t lba = block;
    uint32_t remaining = count;

    uint32_t blocks_per_slot = ring_hdr->data_slot_size / ring_hdr->block_size;
    if (blocks_per_slot == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > blocks_per_slot) ? blocks_per_slot : remaining;
        auto chunk_bytes = chunk * ring_hdr->block_size;

        // Allocate a data slot
        int slot = rdma_alloc_slot(state);
        if (slot < 0) {
            rdma_drain_cq(state);
            slot = rdma_alloc_slot(state);
            if (slot < 0) {
                return -1;
            }
        }

        // Copy data INTO the RDMA zone data slot before posting SQE
        auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, static_cast<uint32_t>(slot));
        memcpy(slot_data, src, chunk_bytes);

        // For RoCE: push data slot to server before posting SQE (server needs data visible)
        roce_push_data_slot(state, static_cast<uint32_t>(slot), chunk_bytes);

        // Wait for SQ space
        while (blk_sq_full(ring_hdr)) {
            asm volatile("pause" ::: "memory");
            rdma_drain_cq(state);
        }

        // Post SQE
        int tag_id = rdma_alloc_tag(state);
        if (tag_id < 0) {
            rdma_free_slot(state, static_cast<uint32_t>(slot));
            return -1;
        }
        uint32_t tag = static_cast<uint32_t>(tag_id);
        uint32_t sq_idx = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[sq_idx];
        sqe.tag = tag;
        sqe.opcode = static_cast<uint8_t>(BlkOpcode::WRITE);
        sqe.lba = lba;
        sqe.block_count = chunk;
        sqe.data_slot = static_cast<uint32_t>(slot);

        asm volatile("" ::: "memory");
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        // For RoCE: push SQ to server so it can see the new entry
        roce_push_sq(state);

        // Signal server
        rdma_signal_server(state);

        // Spin-wait for CQE with matching tag
        WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        BlkCqEntry cqe = {};
        bool got_cqe = false;

        while (!got_cqe) {
            got_cqe = rdma_drain_cq_for_tag(state, tag, &cqe);
            if (got_cqe) {
                break;
            }
            if (wki_now_us() >= deadline) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
                rdma_free_tag(state, tag);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel(ch);
        }

        rdma_free_slot(state, static_cast<uint32_t>(slot));
        rdma_free_tag(state, tag);

        if (cqe.status != 0) {
            return cqe.status;
        }

        src += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

// Dispatcher: uses RDMA ring if available, falls back to message-based
auto remote_block_write(ker::dev::BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }

    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }

    // Re-check active after potentially blocking in fence wait
    if (!state->active) {
        return -1;
    }

    auto cnt = static_cast<uint32_t>(count);
    if (state->rdma_attached) {
        return remote_block_write_rdma(state, block, cnt, buffer);
    }
    return remote_block_write_msg(state, dev, block, cnt, buffer);
}

// Message-based block flush (fallback when RDMA not available)
auto remote_block_flush_msg(ProxyBlockState* state) -> int {
    DevOpReqPayload req = {};
    req.op_id = OP_BLOCK_FLUSH;
    req.data_len = 0;

    WkiWaitEntry wait = {};
    state->lock.lock();
    state->op_wait_entry = &wait;
    state->op_pending.store(true, std::memory_order_release);
    state->op_status = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
    if (send_ret != WKI_OK) {
        state->op_wait_entry = nullptr;
        state->op_pending.store(false, std::memory_order_relaxed);
        return -1;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
    state->op_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        state->op_pending.store(false, std::memory_order_relaxed);
        return -1;
    }

    return static_cast<int>(state->op_status);
}

// RDMA ring-based flush
auto remote_block_flush_rdma(ProxyBlockState* state) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr) {
        return -1;
    }
    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    // Wait for SQ space
    while (blk_sq_full(ring_hdr)) {
        asm volatile("pause" ::: "memory");
        rdma_drain_cq(state);
    }

    // Post SQE — flush uses no data slot
    int tag_id = rdma_alloc_tag(state);
    if (tag_id < 0) {
        return -1;
    }
    uint32_t tag = static_cast<uint32_t>(tag_id);
    uint32_t sq_idx = ring_hdr->sq_head % ring_hdr->sq_depth;
    auto& sqe = sq[sq_idx];
    sqe.tag = tag;
    sqe.opcode = static_cast<uint8_t>(BlkOpcode::FLUSH);
    sqe.lba = 0;
    sqe.block_count = 0;
    sqe.data_slot = 0;

    asm volatile("" ::: "memory");
    ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

    // For RoCE: push SQ to server so it can see the flush entry
    roce_push_sq(state);

    rdma_signal_server(state);

    // Spin-wait for CQE
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    BlkCqEntry cqe = {};
    bool got_cqe = false;

    while (!got_cqe) {
        got_cqe = rdma_drain_cq_for_tag(state, tag, &cqe);
        if (got_cqe) {
            break;
        }
        if (wki_now_us() >= deadline) {
            rdma_free_tag(state, tag);
            return -1;
        }
        asm volatile("pause" ::: "memory");
        wki_spin_yield_channel(ch);
    }

    rdma_free_tag(state, tag);
    return cqe.status;
}

// Dispatcher
auto remote_block_flush(ker::dev::BlockDevice* dev) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }

    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }

    // Re-check active after potentially blocking in fence wait
    if (!state->active) {
        return -1;
    }

    if (state->rdma_attached) {
        return remote_block_flush_rdma(state);
    }
    return remote_block_flush_msg(state);
}

// -----------------------------------------------------------------------------
// Batch I/O — async SQ pipeline (submits multiple SQEs, single doorbell)
// -----------------------------------------------------------------------------

// Per-batch tracking entry (tag + data slot allocated for one SQE)
struct BatchEntry {
    uint32_t tag;
    uint32_t slot;
};

// Post up to `count` SQEs into the SQ ring without signaling the server.
// Allocates tags and data slots for each entry.  Returns number actually posted.
auto rdma_batch_submit(ProxyBlockState* state, BlkOpcode opcode, const BlockRange* ranges, uint32_t count, BatchEntry* entries_out) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr || count == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    uint32_t posted = 0;
    for (uint32_t i = 0; i < count; i++) {
        // Allocate tag
        int tag = rdma_alloc_tag(state);
        if (tag < 0) {
            rdma_drain_cq(state);
            tag = rdma_alloc_tag(state);
            if (tag < 0) {
                break;
            }
        }

        // Allocate data slot (not needed for FLUSH)
        int slot = -1;
        if (opcode != BlkOpcode::FLUSH) {
            slot = rdma_alloc_slot(state);
            if (slot < 0) {
                rdma_drain_cq(state);
                slot = rdma_alloc_slot(state);
                if (slot < 0) {
                    rdma_free_tag(state, static_cast<uint32_t>(tag));
                    break;
                }
            }
        }

        // For writes: copy data to slot before posting
        if (opcode == BlkOpcode::WRITE && slot >= 0 && ranges[i].buffer != nullptr) {
            uint32_t bytes = ranges[i].block_count * ring_hdr->block_size;
            auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, static_cast<uint32_t>(slot));
            memcpy(slot_data, ranges[i].buffer, bytes);
            roce_push_data_slot(state, static_cast<uint32_t>(slot), bytes);
        }

        // Wait for SQ space
        while (blk_sq_full(ring_hdr)) {
            asm volatile("pause" ::: "memory");
            rdma_drain_cq(state);
        }

        // Post SQE
        uint32_t sq_idx = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[sq_idx];
        sqe.tag = static_cast<uint32_t>(tag);
        sqe.opcode = static_cast<uint8_t>(opcode);
        sqe.lba = ranges[i].lba;
        sqe.block_count = ranges[i].block_count;
        sqe.data_slot = (slot >= 0) ? static_cast<uint32_t>(slot) : 0;

        asm volatile("" ::: "memory");  // write barrier before advancing head
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        entries_out[posted].tag = static_cast<uint32_t>(tag);
        entries_out[posted].slot = (slot >= 0) ? static_cast<uint32_t>(slot) : 0;
        posted++;
    }

    return static_cast<int>(posted);
}

// Send a single doorbell/notification after all SQEs are posted.
void rdma_batch_signal(ProxyBlockState* state) {
    roce_push_sq(state);
    rdma_signal_server(state);
}

// Poll CQ for completions matching the given batch entries.
// In blocking mode, spins until all `count` entries complete or timeout.
// Returns the number of completed entries.
auto rdma_batch_collect(ProxyBlockState* state, const BatchEntry* entries, uint32_t count, bool blocking) -> int {
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;

    for (;;) {
        // Count completed entries
        uint32_t completed = 0;
        for (uint32_t i = 0; i < count; i++) {
            uint32_t tag = entries[i].tag;
            if (tag < ProxyBlockState::TAG_POOL_SIZE && state->tag_completions[tag].completed) {
                completed++;
            }
        }
        if (completed == count) {
            return static_cast<int>(completed);
        }

        // Drain CQ entries into per-tag array
        if (state->rdma_roce) {
            // Server-push mode: CQEs arrive via RDMA writes into local zone memory;
            // drive NIC RX to receive those frames, then scan CQ ring locally.
            auto* hdr = blk_ring_header(state->rdma_zone_ptr);
            auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);
            asm volatile("" ::: "memory");
            while (!blk_cq_empty(hdr)) {
                uint32_t idx = hdr->cq_tail % hdr->cq_depth;
                uint32_t ctag = cq[idx].tag;
                if (ctag < ProxyBlockState::TAG_POOL_SIZE && (state->tag_bitmap & (1ULL << ctag)) != 0) {
                    state->tag_completions[ctag].cqe = cq[idx];
                    state->tag_completions[ctag].completed = true;
                    state->tag_completions[ctag].pending = false;
                }
                asm volatile("" ::: "memory");
                hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
            }
        } else {
            rdma_drain_cq(state);
        }

        if (!blocking) {
            // Non-blocking: count what we got and return
            uint32_t got = 0;
            for (uint32_t i = 0; i < count; i++) {
                uint32_t tag = entries[i].tag;
                if (tag < ProxyBlockState::TAG_POOL_SIZE && state->tag_completions[tag].completed) {
                    got++;
                }
            }
            return static_cast<int>(got);
        }

        if (wki_now_us() >= deadline) {
            return -1;  // timeout
        }

        asm volatile("pause" ::: "memory");
        wki_spin_yield_channel(ch);
    }
}

// Batch RDMA read: submits multiple read SQEs, single doorbell, collects all CQEs.
auto remote_block_read_batch_rdma(ProxyBlockState* state, const BlockRange* ranges, uint32_t count) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr || count == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);

    // Invalidate RA cache — batch reads bypass the single-block read-ahead
    ra_invalidate(state);

    // Clamp batch to max depth
    uint32_t batch_count = (count > WKI_DEV_PROXY_MAX_BATCH) ? WKI_DEV_PROXY_MAX_BATCH : count;

    std::array<BatchEntry, WKI_DEV_PROXY_MAX_BATCH> entries = {};

    int posted = rdma_batch_submit(state, BlkOpcode::READ, ranges, batch_count, entries.data());
    if (posted <= 0) {
        return -1;
    }

    rdma_batch_signal(state);

    int collected = rdma_batch_collect(state, entries.data(), static_cast<uint32_t>(posted), true);
    if (collected != posted) {
        // Timeout — free resources for all posted entries
        for (int i = 0; i < posted; i++) {
            rdma_free_slot(state, entries[i].slot);
            rdma_free_tag(state, entries[i].tag);
        }
        return -1;
    }

    // Collect results: copy data from slots to caller buffers
    int result = 0;
    for (int i = 0; i < posted; i++) {
        uint32_t tag = entries[i].tag;
        auto& tc = state->tag_completions[tag];

        if (tc.cqe.status != 0) {
            result = tc.cqe.status;
        } else {
            // For non-RoCE ivshmem: data is already in local shared memory.
            // roce_pull_data_slot is a no-op for both paths (server-push for
            // RoCE, coherent shared mem for ivshmem).
            auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, entries[i].slot);
            uint32_t copy_bytes = ranges[i].block_count * ring_hdr->block_size;
            memcpy(ranges[i].buffer, slot_data, copy_bytes);
        }

        tc.completed = false;
        rdma_free_slot(state, entries[i].slot);
        rdma_free_tag(state, entries[i].tag);
    }

    // Handle remaining ranges (if count > MAX_BATCH) via recursive call
    if (count > batch_count && result == 0) {
        return remote_block_read_batch_rdma(state, ranges + batch_count, count - batch_count);
    }

    return result;
}

// Batch RDMA write: submits multiple write SQEs, single doorbell, collects all CQEs.
auto remote_block_write_batch_rdma(ProxyBlockState* state, const BlockRange* ranges, uint32_t count) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr || count == 0) {
        return -1;
    }

    // Invalidate read-ahead cache — written data may overlap cached range
    ra_invalidate(state);

    // Clamp batch to max depth
    uint32_t batch_count = (count > WKI_DEV_PROXY_MAX_BATCH) ? WKI_DEV_PROXY_MAX_BATCH : count;

    std::array<BatchEntry, WKI_DEV_PROXY_MAX_BATCH> entries = {};

    int posted = rdma_batch_submit(state, BlkOpcode::WRITE, ranges, batch_count, entries.data());
    if (posted <= 0) {
        return -1;
    }

    rdma_batch_signal(state);

    int collected = rdma_batch_collect(state, entries.data(), static_cast<uint32_t>(posted), true);
    if (collected != posted) {
        // Timeout — free resources
        for (int i = 0; i < posted; i++) {
            rdma_free_slot(state, entries[i].slot);
            rdma_free_tag(state, entries[i].tag);
        }
        return -1;
    }

    // Collect results
    int result = 0;
    for (int i = 0; i < posted; i++) {
        uint32_t tag = entries[i].tag;
        auto& tc = state->tag_completions[tag];

        if (tc.cqe.status != 0) {
            result = tc.cqe.status;
        }

        tc.completed = false;
        rdma_free_slot(state, entries[i].slot);
        rdma_free_tag(state, entries[i].tag);
    }

    // Handle remaining ranges (if count > MAX_BATCH)
    if (count > batch_count && result == 0) {
        return remote_block_write_batch_rdma(state, ranges + batch_count, count - batch_count);
    }

    return result;
}

// Dispatcher for batch read: RDMA batch if available, fallback to sequential single reads
auto remote_block_read_batch(ker::dev::BlockDevice* dev, const BlockRange* ranges, uint32_t count) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }

    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (!state->active) {
        return -1;
    }

    if (state->rdma_attached) {
        return remote_block_read_batch_rdma(state, ranges, count);
    }

    // Fallback: sequential single reads via message path
    for (uint32_t i = 0; i < count; i++) {
        int ret = remote_block_read_msg(state, dev, ranges[i].lba, ranges[i].block_count, ranges[i].buffer);
        if (ret != 0) {
            return ret;
        }
    }
    return 0;
}

// Dispatcher for batch write: RDMA batch if available, fallback to sequential single writes
auto remote_block_write_batch(ker::dev::BlockDevice* dev, const BlockRange* ranges, uint32_t count) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }

    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (!state->active) {
        return -1;
    }

    if (state->rdma_attached) {
        return remote_block_write_batch_rdma(state, ranges, count);
    }

    // Fallback: sequential single writes via message path
    for (uint32_t i = 0; i < count; i++) {
        int ret = remote_block_write_msg(state, dev, ranges[i].lba, ranges[i].block_count, ranges[i].buffer);
        if (ret != 0) {
            return ret;
        }
    }
    return 0;
}

}  // namespace

// -----------------------------------------------------------------------------
// Streaming Bulk Transfer — large contiguous I/O via direct RDMA to/from
// a pre-registered consumer staging buffer.  Bypasses the normal per-slot
// SQ/CQ pipeline for transfers exceeding BLK_RING_BULK_THRESHOLD bytes.
// -----------------------------------------------------------------------------

namespace {

// Bulk RDMA read: post a BULK_READ SQE, server RDMA-writes entire range into
// consumer's staging buffer, single CQE covers the whole transfer.
auto remote_block_bulk_read_rdma(ProxyBlockState* state, uint64_t lba, uint32_t block_count, void* buffer) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr || !state->bulk_capable) {
        return -1;
    }
    if (state->bulk_staging_buf == nullptr || state->bulk_staging_rkey == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);
    uint32_t blk_sz = ring_hdr->block_size;

    // Clamp to staging buffer size; if larger, split into chunks
    auto* dest = static_cast<uint8_t*>(buffer);
    uint64_t remaining_lba = lba;
    uint32_t remaining_blocks = block_count;

    while (remaining_blocks > 0) {
        uint32_t chunk_blocks = remaining_blocks;
        uint64_t chunk_bytes = static_cast<uint64_t>(chunk_blocks) * blk_sz;
        if (chunk_bytes > state->bulk_staging_size) {
            chunk_blocks = state->bulk_staging_size / blk_sz;
            chunk_bytes = static_cast<uint64_t>(chunk_blocks) * blk_sz;
        }
        if (chunk_blocks == 0) {
            return -1;
        }

        // Invalidate RA cache (bulk bypasses it)
        ra_invalidate(state);

        // Wait for SQ space
        while (blk_sq_full(ring_hdr)) {
            asm volatile("pause" ::: "memory");
            rdma_drain_cq(state);
        }

        // Allocate tag
        int tag_id = rdma_alloc_tag(state);
        if (tag_id < 0) {
            rdma_drain_cq(state);
            tag_id = rdma_alloc_tag(state);
            if (tag_id < 0) {
                return -1;
            }
        }
        auto tag = static_cast<uint32_t>(tag_id);

        // Post Bulk SQE (same binary layout as BlkSqEntry, data_slot = roce_rkey)
        uint32_t sq_idx = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[sq_idx];
        sqe.tag = tag;
        sqe.opcode = static_cast<uint8_t>(BlkOpcode::BULK_READ);
        sqe.lba = remaining_lba;
        sqe.block_count = chunk_blocks;
        sqe.data_slot = state->bulk_staging_rkey;  // consumer's rkey for staging buffer

        asm volatile("" ::: "memory");
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        // Push SQ to server and signal
        roce_push_sq(state);
        rdma_signal_server(state);

        // Wait for completion — server RDMA-writes data into our staging buffer
        WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
        uint64_t deadline = wki_now_us() + (WKI_DEV_PROXY_TIMEOUT_US * 4);  // larger timeout for bulk
        BlkCqEntry cqe = {};
        bool got_cqe = false;

        if (state->rdma_roce) {
            while (!got_cqe) {
                got_cqe = rdma_wait_cqe_push(state, tag, &cqe);
                if (got_cqe) {
                    break;
                }
                if (wki_now_us() >= deadline) {
                    rdma_free_tag(state, tag);
                    return -1;
                }
                asm volatile("pause" ::: "memory");
                wki_spin_yield_channel(ch);
            }
        } else {
            while (!got_cqe) {
                got_cqe = rdma_drain_cq_for_tag(state, tag, &cqe);
                if (got_cqe) {
                    break;
                }
                if (wki_now_us() >= deadline) {
                    rdma_free_tag(state, tag);
                    return -1;
                }
                asm volatile("pause" ::: "memory");
                wki_spin_yield_channel(ch);
            }
        }

        rdma_free_tag(state, tag);

        if (cqe.status != 0) {
            return cqe.status;
        }

        // Copy from staging buffer to caller's buffer
        memcpy(dest, state->bulk_staging_buf, static_cast<size_t>(chunk_bytes));

        dest += chunk_bytes;
        remaining_lba += chunk_blocks;
        remaining_blocks -= chunk_blocks;
    }

    return 0;
}

// Bulk RDMA write: copy data into staging buffer, post a BULK_WRITE SQE,
// server RDMA-reads from staging buffer and writes to disk.
auto remote_block_bulk_write_rdma(ProxyBlockState* state, uint64_t lba, uint32_t block_count, const void* buffer) -> int {
    if (!state->active || state->rdma_zone_ptr == nullptr || !state->bulk_capable) {
        return -1;
    }
    if (state->bulk_staging_buf == nullptr || state->bulk_staging_rkey == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);
    uint32_t blk_sz = ring_hdr->block_size;

    // Invalidate RA cache — written data may overlap cached range
    ra_invalidate(state);

    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t remaining_lba = lba;
    uint32_t remaining_blocks = block_count;

    while (remaining_blocks > 0) {
        uint32_t chunk_blocks = remaining_blocks;
        uint64_t chunk_bytes = static_cast<uint64_t>(chunk_blocks) * blk_sz;
        if (chunk_bytes > state->bulk_staging_size) {
            chunk_blocks = state->bulk_staging_size / blk_sz;
            chunk_bytes = static_cast<uint64_t>(chunk_blocks) * blk_sz;
        }
        if (chunk_blocks == 0) {
            return -1;
        }

        // Copy caller data into staging buffer
        memcpy(state->bulk_staging_buf, src, static_cast<size_t>(chunk_bytes));

        // For RoCE: push staging buffer to server so it can read the data
        if (state->rdma_roce && state->rdma_transport != nullptr) {
            state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0, state->bulk_staging_buf,
                                              static_cast<uint32_t>(chunk_bytes));
        }

        // Wait for SQ space
        while (blk_sq_full(ring_hdr)) {
            asm volatile("pause" ::: "memory");
            rdma_drain_cq(state);
        }

        // Allocate tag
        int tag_id = rdma_alloc_tag(state);
        if (tag_id < 0) {
            rdma_drain_cq(state);
            tag_id = rdma_alloc_tag(state);
            if (tag_id < 0) {
                return -1;
            }
        }
        uint32_t tag = static_cast<uint32_t>(tag_id);

        // Post Bulk SQE
        uint32_t sq_idx = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[sq_idx];
        sqe.tag = tag;
        sqe.opcode = static_cast<uint8_t>(BlkOpcode::BULK_WRITE);
        sqe.lba = remaining_lba;
        sqe.block_count = chunk_blocks;
        sqe.data_slot = state->bulk_staging_rkey;  // consumer's rkey for staging buffer

        asm volatile("" ::: "memory");
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        roce_push_sq(state);
        rdma_signal_server(state);

        // Wait for completion
        WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
        uint64_t deadline = wki_now_us() + (WKI_DEV_PROXY_TIMEOUT_US * 4);
        BlkCqEntry cqe = {};
        bool got_cqe = false;

        while (!got_cqe) {
            got_cqe = rdma_drain_cq_for_tag(state, tag, &cqe);
            if (got_cqe) {
                break;
            }
            if (wki_now_us() >= deadline) {
                rdma_free_tag(state, tag);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel(ch);
        }

        rdma_free_tag(state, tag);

        if (cqe.status != 0) {
            return cqe.status;
        }

        src += chunk_bytes;
        remaining_lba += chunk_blocks;
        remaining_blocks -= chunk_blocks;
    }

    return 0;
}

// Dispatcher: bulk read (called from public API)
auto remote_block_bulk_read(ker::dev::BlockDevice* dev, uint64_t lba, uint32_t block_count, void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }
    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (!state->active || !state->rdma_attached || !state->bulk_capable) {
        return -1;
    }
    return remote_block_bulk_read_rdma(state, lba, block_count, buffer);
}

// Dispatcher: bulk write (called from public API)
auto remote_block_bulk_write(ker::dev::BlockDevice* dev, uint64_t lba, uint32_t block_count, const void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (state == nullptr || !state->active) {
        return -1;
    }
    if (state->fenced) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (!state->active || !state->rdma_attached || !state->bulk_capable) {
        return -1;
    }
    return remote_block_bulk_write_rdma(state, lba, block_count, buffer);
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_dev_proxy_init() {
    if (g_dev_proxy_initialized) {
        return;
    }
    g_dev_proxy_initialized = true;
    ker::mod::dbg::log("[WKI] Dev proxy subsystem initialized");
}

// -----------------------------------------------------------------------------
// Attach / Detach
// -----------------------------------------------------------------------------

auto wki_dev_proxy_attach_block(uint16_t owner_node, uint32_t resource_id, const char* local_name) -> ker::dev::BlockDevice* {
    // Allocate a new proxy state via unique_ptr (deque stores pointers, not objects)
    s_proxy_lock.lock();
    g_proxies.push_back(std::make_unique<ProxyBlockState>());
    auto* state = g_proxies.back().get();
    s_proxy_lock.unlock();
    // Proxy pointer is stable (std::deque + unique_ptr); safe to use after unlock.

    state->owner_node = owner_node;
    state->resource_id = resource_id;

    // Send DEV_ATTACH_REQ with retry logic
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.requested_channel = 0;  // auto-assign

    state->attach_pending.store(true, std::memory_order_release);
    state->attach_status = 0;
    state->attach_channel = 0;

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] attach_block: starting attach to node=0x%04x res_id=%u cpu=%u proxies=%u", owner_node, resource_id,
                       static_cast<unsigned>(ker::mod::cpu::currentCpu()), static_cast<unsigned>(g_proxies.size()));
#endif

    constexpr int MAX_ATTACH_RETRIES = 3;
    for (int retry = 0; retry < MAX_ATTACH_RETRIES; retry++) {
        WkiWaitEntry wait = {};
        state->attach_wait_entry = &wait;
        if (retry > 0) {
            ker::mod::dbg::log("[WKI] Dev proxy attach retry %d: node=0x%04x res_id=%u pending=%d", retry, owner_node, resource_id,
                               state->attach_pending.load(std::memory_order_relaxed) ? 1 : 0);
            state->attach_pending.store(true, std::memory_order_release);
        }

        int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
        if (send_ret != WKI_OK) {
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] attach_block: send failed err=%d retry=%d", send_ret, retry);
#endif
            state->attach_wait_entry = nullptr;
            continue;  // Retry on send failure
        }

        // Async wait for attach ACK
        int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        state->attach_wait_entry = nullptr;
        if (wait_rc == WKI_ERR_TIMEOUT) {
            continue;  // Timeout, try again
        }

        if (!state->attach_pending) {
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] attach_block: ACK received on retry=%d", retry);
#endif
            break;  // Got ACK, exit retry loop
        }
    }

    if (state->attach_pending) {
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] attach_block: TIMEOUT - setting attach_pending=false, popping proxy");
#endif
        state->attach_pending.store(false, std::memory_order_relaxed);
        s_proxy_lock.lock();
        g_proxies.pop_back();
        s_proxy_lock.unlock();
        ker::mod::dbg::log("[WKI] Dev proxy attach timeout after %d retries: node=0x%04x res_id=%u proxies_left=%u", MAX_ATTACH_RETRIES,
                           owner_node, resource_id, static_cast<unsigned>(g_proxies.size()));
        return nullptr;
    }

    // Check attach result
    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Dev proxy attach rejected: node=0x%04x res_id=%u status=%u", owner_node, resource_id,
                           state->attach_status);
        s_proxy_lock.lock();
        g_proxies.pop_back();
        s_proxy_lock.unlock();
        return nullptr;
    }

    state->assigned_channel = state->attach_channel;
    state->max_op_size = state->attach_max_op_size;

    uint64_t block_size = 0;
    uint64_t total_blocks = 0;

    // Try RDMA ring path: wait for zone to become active and read device info from ring header
    if (state->rdma_zone_id != 0) {
        mod::dbg::log("[WKI] Dev proxy attach ACK received, waiting for RDMA zone: node=0x%04x res_id=%u zone_id=0x%08x", owner_node,
                      resource_id, state->rdma_zone_id);
        uint64_t zone_deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        WkiZone* zone = nullptr;

        // Wait for the RDMA zone to appear (server creates it, zone negotiation completes)
        while (wki_now_us() < zone_deadline) {
            zone = wki_zone_find(state->rdma_zone_id);
            if (zone != nullptr && zone->state == ZoneState::ACTIVE) {
                break;
            }
            zone = nullptr;
            asm volatile("pause" ::: "memory");
            WkiChannel* res_ch = wki_channel_get(owner_node, WKI_CHAN_RESOURCE);
            wki_spin_yield_channel(res_ch);
        }

        if (zone != nullptr) {
            state->rdma_zone_ptr = zone->local_vaddr;

            // Populate RoCE state from zone (must be done before server_ready check
            // so that roce_pull_cq etc. work correctly once we start using the ring)
            state->rdma_roce = zone->is_roce;
            state->rdma_transport = zone->rdma_transport;

            // For RoCE zones: the initiator (server) sends its rkey via a
            // ZONE_NOTIFY_POST (op_type=0xFE) after confirming the zone.  We must
            // wait for zone->remote_rkey to become non-zero before we can do any
            // RDMA operations targeting the server's zone memory.
            if (state->rdma_roce) {
                uint64_t rkey_deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
                while (zone->remote_rkey == 0 && wki_now_us() < rkey_deadline) {
                    asm volatile("pause" ::: "memory");
                    // Drive NIC + WKI RX so the rkey-exchange notification can be processed
                    net::NetDevice* net_dev = wki_eth_get_netdev();
                    if (net_dev != nullptr) {
                        net::napi_poll_inline(net_dev);
                    }
                }
            }
            state->rdma_remote_rkey = zone->remote_rkey;

            // For RoCE zones: pull the ring header from server to get server_ready.
            // Even if rdma_remote_rkey is still 0 (rkey-exchange notification was
            // late), the server may have pushed the header via rdma_write to our
            // local zone memory.  We must poll the NIC so those RDMA_WRITE frames
            // are received and copied into our zone backing.
            if (state->rdma_roce && state->rdma_transport != nullptr && state->rdma_remote_rkey != 0) {
                state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0, state->rdma_zone_ptr,
                                                 BLK_RING_HEADER_SIZE);
            }

            // Wait for server_ready flag in ring header
            auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
            uint64_t ready_deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
            while (ring_hdr->server_ready == 0 && wki_now_us() < ready_deadline) {
                asm volatile("pause" ::: "memory");

                // Always poll NIC: the server pushes the ring header via
                // RDMA_WRITE (EtherType 0x88B8) to our local zone memory.
                // Without polling, those frames stay in the RX queue and
                // ring_hdr->server_ready is never updated locally.
                net::NetDevice* net_dev = wki_eth_get_netdev();
                if (net_dev != nullptr) {
                    net::napi_poll_inline(net_dev);
                }

                // Also re-check rkey — the 0xFE notification may arrive
                // during the NIC poll above.
                if (state->rdma_remote_rkey == 0 && zone->remote_rkey != 0) {
                    state->rdma_remote_rkey = zone->remote_rkey;
                }

                // For RoCE with valid rkey: also actively pull the header
                if (state->rdma_roce && state->rdma_transport != nullptr && state->rdma_remote_rkey != 0) {
                    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0,
                                                     state->rdma_zone_ptr, BLK_RING_HEADER_SIZE);
                }
            }

            if (ring_hdr->server_ready != 0) {
                // Read device info directly from the ring header — no OP_BLOCK_INFO needed
                block_size = ring_hdr->block_size;
                total_blocks = ring_hdr->total_blocks;
                state->rdma_attached = true;
                state->data_slot_bitmap = 0;
                state->tag_bitmap = 0;
                for (auto& tc : state->tag_completions) {
                    tc = {};
                }

                // Allocate read-ahead cache buffer (one data-slot's worth)
                state->ra_buffer = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(ring_hdr->data_slot_size));
                if (state->ra_buffer != nullptr) {
                    state->ra_capacity = ring_hdr->data_slot_size / ring_hdr->block_size;
                } else {
                    state->ra_capacity = 0;
                }
                state->ra_valid = false;
                state->ra_block_count = 0;

                ker::mod::dbg::log("[WKI] Dev proxy RDMA ring attached: zone=0x%08x bs=%u tb=%llu slots=%u roce=%d ra=%s",
                                   state->rdma_zone_id, ring_hdr->block_size, ring_hdr->total_blocks, ring_hdr->data_slot_count,
                                   state->rdma_roce ? 1 : 0, state->ra_buffer != nullptr ? "yes" : "no");
            } else {
                ker::mod::dbg::log("[WKI] Dev proxy RDMA ring server_ready timeout — falling back to msg path");
                state->rdma_zone_id = 0;
                state->rdma_zone_ptr = nullptr;
                state->rdma_roce = false;
                state->rdma_transport = nullptr;
                state->rdma_remote_rkey = 0;
            }
        } else {
            ker::mod::dbg::log("[WKI] Dev proxy RDMA zone not found (0x%08x) — falling back to msg path", state->rdma_zone_id);
            state->rdma_zone_id = 0;
            state->rdma_roce = false;
            state->rdma_transport = nullptr;
            state->rdma_remote_rkey = 0;
        }
    }

    // Fallback: query block device info via OP_BLOCK_INFO message if no RDMA
    if (!state->rdma_attached) {
        DevOpReqPayload info_req = {};
        info_req.op_id = OP_BLOCK_INFO;
        info_req.data_len = 0;

        std::array<uint8_t, 16> info_buf = {};
        WkiWaitEntry wait = {};
        state->lock.lock();
        state->op_wait_entry = &wait;
        state->op_pending.store(true, std::memory_order_release);
        state->op_status = 0;
        state->op_resp_buf = static_cast<void*>(info_buf.data());
        state->op_resp_max = info_buf.size();
        state->op_resp_len = 0;
        state->lock.unlock();

        int send_ret = wki_send(owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &info_req, sizeof(info_req));
        if (send_ret != WKI_OK) {
            state->op_wait_entry = nullptr;
            state->op_pending.store(false, std::memory_order_relaxed);
            mod::dbg::log("[WKI] Dev proxy block info request send failed: node=0x%04x res_id=%u err=%d", owner_node, resource_id,
                          send_ret);
            DevDetachPayload det = {};
            det.target_node = owner_node;
            det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
            det.resource_id = resource_id;
            wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
            s_proxy_lock.lock();
            g_proxies.pop_back();
            s_proxy_lock.unlock();
            return nullptr;
        }

        int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        state->op_wait_entry = nullptr;
        if (wait_rc == WKI_ERR_TIMEOUT) {
            state->op_pending.store(false, std::memory_order_relaxed);
            mod::dbg::log("[WKI] Dev proxy block info request timeout: node=0x%04x res_id=%u", owner_node, resource_id);
            s_proxy_lock.lock();
            g_proxies.pop_back();
            s_proxy_lock.unlock();
            return nullptr;
        }

        if (state->op_status != 0 || state->op_resp_len < 16) {
            s_proxy_lock.lock();
            g_proxies.pop_back();
            s_proxy_lock.unlock();
            return nullptr;
        }

        memcpy(&block_size, static_cast<const void*>(info_buf.data()), sizeof(uint64_t));
        memcpy(&total_blocks, static_cast<const void*>(info_buf.data() + sizeof(uint64_t)), sizeof(uint64_t));
    }

    // Populate the proxy BlockDevice
    state->bdev.major = 0;
    state->bdev.minor = 0;
    if (local_name != nullptr) {
        size_t name_len = strlen(local_name);
        if (name_len >= ker::dev::BLOCK_NAME_SIZE) {
            name_len = ker::dev::BLOCK_NAME_SIZE - 1;
        }
        memcpy(state->bdev.name.data(), local_name, name_len);
        state->bdev.name[name_len] = '\0';
    }
    state->bdev.block_size = static_cast<size_t>(block_size);
    state->bdev.total_blocks = total_blocks;
    state->bdev.read_blocks = remote_block_read;
    state->bdev.write_blocks = remote_block_write;
    state->bdev.flush = remote_block_flush;
    state->bdev.private_data = state;

    // Set up streaming bulk transfer if server advertised support
    if (state->rdma_attached && state->bulk_capable && state->rdma_transport != nullptr &&
        state->rdma_transport->rdma_register_region != nullptr) {
        // Default bulk staging buffer: 2 MB (clamped to advertised max)
        constexpr uint32_t DEFAULT_BULK_STAGING = 2 * 1024 * 1024;
        uint32_t staging_sz = (state->bulk_max_transfer > 0 && state->bulk_max_transfer < DEFAULT_BULK_STAGING) ? state->bulk_max_transfer
                                                                                                                : DEFAULT_BULK_STAGING;
        state->bulk_staging_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(staging_sz));
        if (state->bulk_staging_buf != nullptr) {
            uint32_t rkey = 0;
            int reg_ret = state->rdma_transport->rdma_register_region(
                state->rdma_transport, reinterpret_cast<uint64_t>(state->bulk_staging_buf), staging_sz, &rkey);
            if (reg_ret == 0) {
                state->bulk_staging_size = staging_sz;
                state->bulk_staging_rkey = rkey;
                state->bdev.capabilities |= ker::dev::BDEV_CAP_BULK_RDMA;
                ker::mod::dbg::log("[WKI] Dev proxy bulk staging registered: size=%uKB rkey=0x%08x", staging_sz / 1024, rkey);
            } else {
                ker::mod::mm::dyn::kmalloc::free(state->bulk_staging_buf);
                state->bulk_staging_buf = nullptr;
                state->bulk_capable = false;
                ker::mod::dbg::log("[WKI] Dev proxy bulk staging register failed: err=%d", reg_ret);
            }
        } else {
            state->bulk_capable = false;
        }
    }

    state->active = true;

    // Check for naming collision before registering
    if (ker::dev::block_device_find_by_name(state->bdev.name.data()) != nullptr) {
        ker::mod::dbg::log("[WKI] Dev proxy name collision: %s already registered", state->bdev.name.data());
        // Send detach and clean up
        DevDetachPayload det = {};
        det.target_node = owner_node;
        det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
        det.resource_id = resource_id;
        wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
        s_proxy_lock.lock();
        g_proxies.pop_back();
        s_proxy_lock.unlock();
        return nullptr;
    }

    // Register in the block device subsystem
    ker::dev::block_device_register(&state->bdev);

    ker::mod::dbg::log("[WKI] Dev proxy attached: %s node=0x%04x res_id=%u ch=%u bs=%u tb=%llu", state->bdev.name.data(), owner_node,
                       resource_id, state->assigned_channel, static_cast<unsigned>(block_size), total_blocks);

    return &state->bdev;
}

void wki_dev_proxy_detach_block(ker::dev::BlockDevice* proxy_bdev) {
    // Lock to find proxy and copy info needed for cleanup outside the lock.
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(proxy_bdev);
    if (state == nullptr) {
        s_proxy_lock.unlock();
        return;
    }

    // Copy info for cleanup outside the lock
    uint16_t owner_node = state->owner_node;
    uint32_t resource_id = state->resource_id;
    uint16_t assigned_channel = state->assigned_channel;
    bool had_rdma = state->rdma_attached;
    uint32_t rdma_zone_id = state->rdma_zone_id;
    uint8_t* ra_buf = state->ra_buffer;
    uint8_t* bulk_buf = state->bulk_staging_buf;

    // Neutralise proxy under lock so no other thread picks it up
    state->ra_buffer = nullptr;
    state->bulk_staging_buf = nullptr;
    state->bulk_capable = false;
    state->bulk_staging_rkey = 0;
    state->bulk_staging_size = 0;
    state->bulk_max_transfer = 0;
    ra_invalidate(state);
    state->active = false;
    if (had_rdma) {
        state->rdma_attached = false;
        state->rdma_zone_ptr = nullptr;
        state->rdma_zone_id = 0;
        state->rdma_roce = false;
        state->rdma_transport = nullptr;
        state->rdma_remote_rkey = 0;
    }
    s_proxy_lock.unlock();

    // --- Cleanup outside lock (no blocking operations while holding spinlock) ---

    // Free read-ahead cache
    if (ra_buf != nullptr) {
        ker::mod::mm::dyn::kmalloc::free(ra_buf);
    }

    // Free bulk staging buffer
    if (bulk_buf != nullptr) {
        ker::mod::mm::dyn::kmalloc::free(bulk_buf);
    }

    // Destroy RDMA zone before sending detach
    if (had_rdma && rdma_zone_id != 0) {
        wki_zone_destroy(rdma_zone_id);
    }

    // Unregister from block device subsystem
    ker::dev::block_device_unregister(proxy_bdev);

    // Send DEV_DETACH to owner
    DevDetachPayload det = {};
    det.target_node = owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    det.resource_id = resource_id;
    wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(owner_node, assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    ker::mod::dbg::log("[WKI] Dev proxy detached: node=0x%04x res=%u ch=%u", owner_node, resource_id, assigned_channel);

    // Remove inactive entries under lock
    s_proxy_lock.lock();
    for (auto it = g_proxies.begin(); it != g_proxies.end();) {
        if (!(*it)->active) {
            it = g_proxies.erase(it);
        } else {
            ++it;
        }
    }
    s_proxy_lock.unlock();
}

// -----------------------------------------------------------------------------
// Fencing — suspend / resume / hard teardown
// -----------------------------------------------------------------------------

void wki_dev_proxy_suspend_for_peer(uint16_t node_id) {
    uint64_t now = wki_now_us();

    // Info collected under lock for cleanup outside
    struct PendingCleanup {
        uint16_t owner_node;
        uint16_t channel;
        uint8_t* ra_buf;
    };
    PendingCleanup cleanup[64];  // NOLINT
    int cleanup_count = 0;

    s_proxy_lock.lock();
    for (auto& p : g_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        // Fail any in-flight operation so the waiter unblocks,
        // but keep the proxy registered — callers will see fenced==true
        // on retry and block in wait_for_fence_lift().
        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending.store(false, std::memory_order_release);
            if (p->op_wait_entry != nullptr) {
                wki_wake_op(p->op_wait_entry, -1);
            }
            if (p->attach_wait_entry != nullptr) {
                wki_wake_op(p->attach_wait_entry, -1);
            }
        }

        // Save RA buffer + channel info for cleanup outside lock
        if (cleanup_count < 64) {
            cleanup[cleanup_count].owner_node = p->owner_node;
            cleanup[cleanup_count].channel = p->assigned_channel;
            cleanup[cleanup_count].ra_buf = p->ra_buffer;
            cleanup_count++;
        }

        p->ra_buffer = nullptr;
        ra_invalidate(p.get());

        // Clear RDMA state — zone will be destroyed by wki_zones_destroy_for_peer()
        if (p->rdma_attached) {
            p->rdma_attached = false;
            p->rdma_zone_ptr = nullptr;
            p->rdma_zone_id = 0;
            p->data_slot_bitmap = 0;
            p->rdma_roce = false;
            p->rdma_transport = nullptr;
            p->rdma_remote_rkey = 0;
        }

        p->fenced = true;
        p->fence_time_us = now;

        ker::mod::dbg::log("[WKI] Dev proxy suspended (fenced): %s node=0x%04x — I/O will block until reconnect or %llu s timeout",
                           p->bdev.name.data(), node_id, WKI_DEV_PROXY_FENCE_WAIT_US / 1000000ULL);
    }
    s_proxy_lock.unlock();

    // Free RA buffers and close channels outside lock
    for (int i = 0; i < cleanup_count; i++) {
        if (cleanup[i].ra_buf != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(cleanup[i].ra_buf);
        }
        WkiChannel* ch = wki_channel_get(cleanup[i].owner_node, cleanup[i].channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }
    }
}

void wki_dev_proxy_resume_for_peer(uint16_t node_id) {
    // Collect matching proxies under lock (deque pointer stability guarantees
    // the raw pointers remain valid after unlock as long as no erase occurs;
    // erases only happen in detach/cleanup paths which mark active=false first).
    ProxyBlockState* to_resume[64];  // NOLINT
    size_t resume_count = 0;

    s_proxy_lock.lock();
    for (auto& p : g_proxies) {
        if (p->active && p->fenced && p->owner_node == node_id && resume_count < 64) {
            to_resume[resume_count++] = p.get();
        }
    }
    s_proxy_lock.unlock();

    for (size_t ri = 0; ri < resume_count; ri++) {
        auto* p = to_resume[ri];

        // Re-check validity (another thread may have detached this proxy)
        if (!p->active || !p->fenced) {
            continue;
        }

        // Re-attach: send DEV_ATTACH_REQ to get a fresh dynamic channel
        DevAttachReqPayload attach_req = {};
        attach_req.target_node = node_id;
        attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
        attach_req.resource_id = p->resource_id;
        attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
        attach_req.requested_channel = 0;  // auto-assign

        p->attach_pending.store(true, std::memory_order_release);
        p->attach_status = 0;
        p->attach_channel = 0;

        constexpr int MAX_RESUME_RETRIES = 5;
        bool attached = false;
        for (int retry = 0; retry < MAX_RESUME_RETRIES; retry++) {
            WkiWaitEntry wait = {};
            p->attach_wait_entry = &wait;
            if (retry > 0) {
                p->attach_pending.store(true, std::memory_order_release);
            }

            int send_ret = wki_send(node_id, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
            if (send_ret != WKI_OK) {
                p->attach_wait_entry = nullptr;
                continue;
            }

            int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
            p->attach_wait_entry = nullptr;
            if (wait_rc == WKI_ERR_TIMEOUT) {
                continue;
            }

            if (!p->attach_pending && p->attach_status == static_cast<uint8_t>(DevAttachStatus::OK)) {
                attached = true;
                break;
            }
        }

        if (!attached) {
            ker::mod::dbg::log("[WKI] Dev proxy resume FAILED (re-attach): %s node=0x%04x — will hard-detach", p->bdev.name.data(),
                               node_id);
            p->attach_pending.store(false, std::memory_order_relaxed);
            // Leave fenced=true; the fence_timeout_tick will clean it up
            continue;
        }

        // Lock briefly to set channel / fenced fields
        s_proxy_lock.lock();
        p->assigned_channel = p->attach_channel;
        p->max_op_size = p->attach_max_op_size;
        s_proxy_lock.unlock();

        // Re-attach RDMA zone if the new attach ACK provided one (RDMA ops outside lock)
        if (p->rdma_zone_id != 0 && !p->rdma_attached) {
            WkiZone* zone = wki_zone_find(p->rdma_zone_id);
            if (zone != nullptr && zone->state == ZoneState::ACTIVE) {
                p->rdma_zone_ptr = zone->local_vaddr;
                p->rdma_roce = zone->is_roce;
                p->rdma_transport = zone->rdma_transport;
                p->rdma_remote_rkey = zone->remote_rkey;

                // For RoCE: pull ring header from server to check server_ready
                if (p->rdma_roce && p->rdma_transport != nullptr) {
                    p->rdma_transport->rdma_read(p->rdma_transport, p->owner_node, p->rdma_remote_rkey, 0, p->rdma_zone_ptr,
                                                 BLK_RING_HEADER_SIZE);
                }

                auto* ring_hdr = blk_ring_header(p->rdma_zone_ptr);
                if (ring_hdr->server_ready != 0) {
                    p->rdma_attached = true;
                    p->data_slot_bitmap = 0;
                    p->tag_bitmap = 0;
                    for (auto& tc : p->tag_completions) {
                        tc = {};
                    }

                    // Re-allocate read-ahead cache if needed
                    if (p->ra_buffer == nullptr) {
                        p->ra_buffer = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(ring_hdr->data_slot_size));
                        if (p->ra_buffer != nullptr) {
                            p->ra_capacity = ring_hdr->data_slot_size / ring_hdr->block_size;
                        }
                    }
                    ra_invalidate(p);

                    ker::mod::dbg::log("[WKI] Dev proxy RDMA ring re-attached on resume: zone=0x%08x roce=%d", p->rdma_zone_id,
                                       p->rdma_roce ? 1 : 0);
                }
            }
        }

        s_proxy_lock.lock();
        p->fenced = false;
        p->fence_time_us = 0;
        s_proxy_lock.unlock();

        ker::mod::dbg::log("[WKI] Dev proxy resumed: %s node=0x%04x ch=%u — blocked I/O will now proceed", p->bdev.name.data(), node_id,
                           p->assigned_channel);
    }
}

void wki_dev_proxy_detach_all_for_peer(uint16_t node_id) {
    // Info collected under lock for cleanup outside
    struct DetachInfo {
        uint8_t* ra_buf;
        uint32_t rdma_zone_id;
        uint16_t owner_node;
        uint16_t channel;
        ker::dev::BlockDevice* bdev;
    };
    DetachInfo info[64];  // NOLINT
    int info_count = 0;

    // Lock to iterate, collect info, and mark inactive
    s_proxy_lock.lock();
    for (auto& p : g_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        // Fail any pending operation
        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending.store(false, std::memory_order_release);
            if (p->op_wait_entry != nullptr) {
                wki_wake_op(p->op_wait_entry, -1);
            }
            if (p->attach_wait_entry != nullptr) {
                wki_wake_op(p->attach_wait_entry, -1);
            }
        }

        // Collect info for cleanup outside lock
        if (info_count < 64) {
            info[info_count].ra_buf = p->ra_buffer;
            info[info_count].rdma_zone_id = p->rdma_zone_id;
            info[info_count].owner_node = p->owner_node;
            info[info_count].channel = p->assigned_channel;
            info[info_count].bdev = &p->bdev;
            info_count++;
        }

        // Neutralise fields under lock
        p->ra_buffer = nullptr;
        ra_invalidate(p.get());
        if (p->rdma_zone_id != 0) {
            p->rdma_attached = false;
            p->rdma_zone_ptr = nullptr;
            p->rdma_zone_id = 0;
            p->rdma_roce = false;
            p->rdma_transport = nullptr;
            p->rdma_remote_rkey = 0;
        }

        p->fenced = false;
        p->active = false;
    }
    s_proxy_lock.unlock();

    // Cleanup outside lock: unregister + close channels + free RA + destroy zones
    for (int i = 0; i < info_count; i++) {
        if (info[i].ra_buf != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(info[i].ra_buf);
        }
        if (info[i].rdma_zone_id != 0) {
            wki_zone_destroy(info[i].rdma_zone_id);
        }
        WkiChannel* ch = wki_channel_get(info[i].owner_node, info[i].channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }
        ker::dev::block_device_unregister(info[i].bdev);
        ker::mod::dbg::log("[WKI] Dev proxy hard-detached: node=0x%04x ch=%u", info[i].owner_node, info[i].channel);
    }

    // Remove inactive entries under lock
    s_proxy_lock.lock();
    for (auto it = g_proxies.begin(); it != g_proxies.end();) {
        if (!(*it)->active) {
            it = g_proxies.erase(it);
        } else {
            ++it;
        }
    }
    s_proxy_lock.unlock();
}

void wki_dev_proxy_fence_timeout_tick(uint64_t now_us) {
    // Collect timed-out proxies under lock, mark inactive, then unregister outside.
    struct TimedOutInfo {
        ProxyBlockState* state;
        uint64_t elapsed;
    };
    TimedOutInfo timed_out[64];  // NOLINT
    int to_count = 0;

    s_proxy_lock.lock();
    for (auto& p : g_proxies) {
        if (!p->active || !p->fenced) {
            continue;
        }

        // Guard against unsigned underflow from TSC skew between CPUs —
        // fence_time_us may have been stamped on a different core whose
        // TSC is slightly ahead of ours.
        if (p->fence_time_us == 0 || now_us < p->fence_time_us) {
            continue;
        }

        uint64_t elapsed = now_us - p->fence_time_us;
        if (elapsed >= WKI_DEV_PROXY_FENCE_WAIT_US) {
            // Mark inactive FIRST so that wait_for_fence_lift() returns
            // false.  Leave fenced=true so spinning I/O threads stay in
            // the wait loop until they check active and bail out.
            // Memory barrier ensures the active=false store is visible to
            // other CPUs before we continue.
            p->active = false;
            asm volatile("mfence" ::: "memory");

            if (to_count < 64) {
                timed_out[to_count].state = p.get();
                timed_out[to_count].elapsed = elapsed;
                to_count++;
            }
        }
    }
    s_proxy_lock.unlock();

    // Unregister block devices outside lock (pointer still valid — no erase).
    // Do NOT erase the proxy from g_proxies here — I/O threads on
    // other CPUs may still hold a raw ProxyBlockState* pointer from
    // find_proxy_by_bdev().  Freeing the memory would cause a
    // use-after-free.  The dead entry is harmless: find_proxy_by_bdev
    // checks active, and all other g_proxies iterators skip inactive
    // entries.
    for (int i = 0; i < to_count; i++) {
        auto* st = timed_out[i].state;
        ker::mod::dbg::log("[WKI] Dev proxy fence timeout (%llu s): %s node=0x%04x — tearing down", timed_out[i].elapsed / 1000000ULL,
                           st->bdev.name.data(), st->owner_node);
        ker::dev::block_device_unregister(&st->bdev);
    }
}

// -----------------------------------------------------------------------------
// Batch I/O — public API
// -----------------------------------------------------------------------------

auto wki_dev_proxy_batch_read(ker::dev::BlockDevice* bdev, const BlockRange* ranges, uint32_t count) -> int {
    if (bdev == nullptr || ranges == nullptr || count == 0) {
        return -1;
    }
    return remote_block_read_batch(bdev, ranges, count);
}

auto wki_dev_proxy_batch_write(ker::dev::BlockDevice* bdev, const BlockRange* ranges, uint32_t count) -> int {
    if (bdev == nullptr || ranges == nullptr || count == 0) {
        return -1;
    }
    return remote_block_write_batch(bdev, ranges, count);
}

// -----------------------------------------------------------------------------
// Streaming Bulk Transfer — public API
// -----------------------------------------------------------------------------

auto wki_dev_proxy_bulk_read(ker::dev::BlockDevice* bdev, uint64_t lba, uint32_t block_count, void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr || block_count == 0) {
        return -1;
    }
    return remote_block_bulk_read(bdev, lba, block_count, buffer);
}

auto wki_dev_proxy_bulk_write(ker::dev::BlockDevice* bdev, uint64_t lba, uint32_t block_count, const void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr || block_count == 0) {
        return -1;
    }
    return remote_block_bulk_write(bdev, lba, block_count, buffer);
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: src=0x%04x ch=%u payload_len=%u", hdr->src_node, hdr->channel_id, payload_len);
#endif

    if (payload_len < sizeof(DevAttachAckPayload)) {
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: payload too small (%u < %u)", payload_len,
                           static_cast<unsigned>(sizeof(DevAttachAckPayload)));
#endif
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: status=%u assigned_ch=%u max_op=%u", ack->status, ack->assigned_channel,
                       ack->max_op_size);

    // Find the pending attach for this owner node
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: searching %u proxies for node 0x%04x (cpu=%u)",
                       static_cast<unsigned>(g_proxies.size()), hdr->src_node, static_cast<unsigned>(ker::mod::cpu::currentCpu()));
    for (size_t i = 0; i < g_proxies.size(); i++) {
        auto& p = g_proxies[i];
        ker::mod::dbg::log("[WKI-DBG]   proxy[%u]: owner=0x%04x attach_pending=%d active=%d", static_cast<unsigned>(i), p->owner_node,
                           p->attach_pending ? 1 : 0, p->active ? 1 : 0);
    }
#endif
    ProxyBlockState* state = find_proxy_by_attach(hdr->src_node);
    if (state == nullptr) {
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: NO proxy found for node 0x%04x", hdr->src_node);
#endif
        return;
    }

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: proxy found, clearing attach_pending");
#endif

    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    // Check for RDMA block ring zone in the extended payload
    if (payload_len >= sizeof(DevAttachAckPayload) && (ack->rdma_flags & DEV_ATTACH_RDMA_BLK_RING) != 0 && ack->blk_zone_id != 0) {
        state->rdma_zone_id = ack->blk_zone_id;
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: RDMA blk ring zone_id=0x%08x", ack->blk_zone_id);
#endif
    }

    // Check for bulk RDMA transfer support
    if (payload_len >= sizeof(DevAttachAckPayload) && (ack->rdma_flags & DEV_ATTACH_RDMA_BULK) != 0) {
        state->bulk_capable = true;
        // Default max transfer size if not explicitly negotiated (2 MB)
        state->bulk_max_transfer = 2 * 1024 * 1024;
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: bulk transfer supported, max=%u", state->bulk_max_transfer);
#endif
    }

    // Release the waiter
    state->attach_pending.store(false, std::memory_order_release);
    if (state->attach_wait_entry != nullptr) {
        wki_wake_op(state->attach_wait_entry, 0);
    }
}

void handle_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t resp_data_len = resp->data_len;

    // Verify data fits
    if (sizeof(DevOpRespPayload) + resp_data_len > payload_len) {
        return;
    }

    // Find proxy by (src_node, channel_id)
    ProxyBlockState* state = find_proxy_by_channel(hdr->src_node, hdr->channel_id);
    if (state == nullptr || !state->op_pending) {
        return;
    }

    state->lock.lock();
    state->op_status = resp->status;

    // Copy response data if there's a destination buffer
    if (resp_data_len > 0 && state->op_resp_buf != nullptr) {
        uint16_t copy_len = (resp_data_len > state->op_resp_max) ? state->op_resp_max : resp_data_len;
        memcpy(state->op_resp_buf, resp_data, copy_len);
        state->op_resp_len = copy_len;
    } else {
        state->op_resp_len = 0;
    }

    state->lock.unlock();

    // Release the waiter
    state->op_pending.store(false, std::memory_order_release);
    if (state->op_wait_entry != nullptr) {
        wki_wake_op(state->op_wait_entry, 0);
    }
}

}  // namespace detail

}  // namespace ker::net::wki
