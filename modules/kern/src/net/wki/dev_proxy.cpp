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

auto find_proxy_by_bdev(ker::dev::BlockDevice* bdev) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if (p->active && &p->bdev == bdev) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyBlockState* {
    for (auto& p : g_proxies) {
        if ((p->active || p->op_pending) && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

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

// Spin-wait for a CQE by checking local zone memory only (server-push mode).
// The server has already pushed data + CQ + header via rdma_write; the consumer's
// napi_poll_inline (called by wki_spin_yield_channel) delivers those frames into
// the local zone.  No rdma_read round-trips needed.
auto rdma_wait_cqe_push(ProxyBlockState* state, uint32_t tag, BlkCqEntry* out_cqe) -> bool {
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);

    asm volatile("" ::: "memory");  // read barrier

    if (blk_cq_empty(hdr)) {
        return false;
    }

    // In single-outstanding mode the next CQE is always at cq_tail
    uint32_t idx = hdr->cq_tail % hdr->cq_depth;
    if (cq[idx].tag == tag) {
        *out_cqe = cq[idx];
        asm volatile("" ::: "memory");
        hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
        return true;
    }

    return false;
}

// Drain all available CQ entries into the pending completion cache.
void rdma_drain_cq(ProxyBlockState* state) {
    // For RoCE zones: pull CQ + header from server before checking for completions
    roce_pull_cq(state);

    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);

    while (!blk_cq_empty(hdr)) {
        asm volatile("" ::: "memory");  // read barrier
        uint32_t idx = hdr->cq_tail % hdr->cq_depth;
        const auto& cqe = cq[idx];

        // Store in pending cache (find first free slot)
        bool cached = false;
        for (auto& pc : state->pending_cq) {
            if (!pc.valid) {
                pc.cqe = cqe;
                pc.valid = true;
                cached = true;
                break;
            }
        }

        if (!cached) {
            // Pending cache full — stop draining to avoid losing this CQE.
            // The caller will retry after consuming entries from the cache.
            break;
        }

        asm volatile("" ::: "memory");  // write barrier before advancing tail
        hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
    }
}

// Drain CQ and look for a specific tag. Returns true if found and fills out_cqe.
auto rdma_drain_cq_for_tag(ProxyBlockState* state, uint32_t tag, BlkCqEntry* out_cqe) -> bool {
    // First check pending cache
    for (auto& pc : state->pending_cq) {
        if (pc.valid && pc.cqe.tag == tag) {
            *out_cqe = pc.cqe;
            pc.valid = false;
            return true;
        }
    }

    // Drain fresh CQ entries
    rdma_drain_cq(state);

    // Check again
    for (auto& pc : state->pending_cq) {
        if (pc.valid && pc.cqe.tag == tag) {
            *out_cqe = pc.cqe;
            pc.valid = false;
            return true;
        }
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
        state->lock.lock();
        state->op_pending = true;
        state->op_status = 0;
        state->op_resp_buf = dest;
        state->op_resp_max = chunk_bytes;
        state->op_resp_len = 0;
        state->lock.unlock();

        // Send request
        int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf.data(),
                                static_cast<uint16_t>(sizeof(DevOpReqPayload) + REQ_DATA_LEN));
        if (send_ret != WKI_OK) {
            state->op_pending = false;
            return -1;
        }

        // Spin-wait for response — targeted single-channel tick
        WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        while (state->op_pending) {
            asm volatile("pause" ::: "memory");
            if (!state->op_pending) {
                break;
            }
            if (wki_now_us() >= deadline) {
                state->op_pending = false;
                return -1;
            }
            wki_spin_yield_channel(ch);
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

    // ── 1. Serve from read-ahead cache if the request is fully covered ──────
    if (ra_cache_hit(state, block, count)) {
        uint64_t off = (block - state->ra_base_lba) * blk_sz;
        memcpy(buffer, state->ra_buffer + off, static_cast<size_t>(count) * blk_sz);
        return 0;
    }

    // ── 2. Cache miss — fetch a full slot starting at the requested LBA ─────
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
    uint32_t tag = state->next_tag++;
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

    // ── 3. Wait for completion ──────────────────────────────────────────────
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
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel(ch);
        }
    }

    if (cqe.status != 0) {
        rdma_free_slot(state, static_cast<uint32_t>(slot));
        ra_invalidate(state);
        return cqe.status;
    }

    // ── 4. Populate read-ahead cache from the data slot ─────────────────────
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

    // ── 5. Copy the originally-requested data to the caller's buffer ────────
    memcpy(buffer, state->ra_buffer != nullptr ? state->ra_buffer : slot_data, static_cast<size_t>(count) * blk_sz);

    return 0;
}

// Dispatcher: uses RDMA ring if available, falls back to message-based
auto remote_block_read(ker::dev::BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    auto* state = find_proxy_by_bdev(dev);
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
        state->lock.lock();
        state->op_pending = true;
        state->op_status = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->lock.unlock();

        // Send request
        int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
        ker::mod::mm::dyn::kmalloc::free(req_buf);

        if (send_ret != WKI_OK) {
            state->op_pending = false;
            return -1;
        }

        // Spin-wait for response — targeted single-channel tick
        WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        while (state->op_pending) {
            asm volatile("pause" ::: "memory");
            if (!state->op_pending) {
                break;
            }
            if (wki_now_us() >= deadline) {
                state->op_pending = false;
                return -1;
            }
            wki_spin_yield_channel(ch);
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
        uint32_t tag = state->next_tag++;
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
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel(ch);
        }

        rdma_free_slot(state, static_cast<uint32_t>(slot));

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
    auto* state = find_proxy_by_bdev(dev);
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

    state->lock.lock();
    state->op_pending = true;
    state->op_status = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
    if (send_ret != WKI_OK) {
        state->op_pending = false;
        return -1;
    }

    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->op_pending) {
        asm volatile("pause" ::: "memory");
        if (!state->op_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            state->op_pending = false;
            return -1;
        }
        wki_spin_yield_channel(ch);
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
    uint32_t tag = state->next_tag++;
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
            return -1;
        }
        asm volatile("pause" ::: "memory");
        wki_spin_yield_channel(ch);
    }

    return cqe.status;
}

// Dispatcher
auto remote_block_flush(ker::dev::BlockDevice* dev) -> int {
    auto* state = find_proxy_by_bdev(dev);
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
    g_proxies.push_back(std::make_unique<ProxyBlockState>());
    auto* state = g_proxies.back().get();

    state->owner_node = owner_node;
    state->resource_id = resource_id;

    // Send DEV_ATTACH_REQ with retry logic
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.requested_channel = 0;  // auto-assign

    state->attach_pending = true;
    state->attach_status = 0;
    state->attach_channel = 0;

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] attach_block: starting attach to node=0x%04x res_id=%u cpu=%u proxies=%u", owner_node, resource_id,
                       static_cast<unsigned>(ker::mod::cpu::currentCpu()), static_cast<unsigned>(g_proxies.size()));
#endif

    constexpr int MAX_ATTACH_RETRIES = 3;
    for (int retry = 0; retry < MAX_ATTACH_RETRIES; retry++) {
        if (retry > 0) {
            ker::mod::dbg::log("[WKI] Dev proxy attach retry %d: node=0x%04x res_id=%u pending=%d", retry, owner_node, resource_id,
                               state->attach_pending ? 1 : 0);
            state->attach_pending = true;
        }

        int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
        if (send_ret != WKI_OK) {
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] attach_block: send failed err=%d retry=%d", send_ret, retry);
#endif
            continue;  // Retry on send failure
        }

        // Spin-wait for attach ACK — targeted single-channel tick
        WkiChannel* res_ch = wki_channel_get(owner_node, WKI_CHAN_RESOURCE);
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        while (state->attach_pending) {
            asm volatile("pause" ::: "memory");
            if (!state->attach_pending) {
                break;
            }
            if (wki_now_us() >= deadline) {
                break;  // Timeout, try again
            }
            wki_spin_yield_channel(res_ch);
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
        state->attach_pending = false;
        g_proxies.pop_back();
        ker::mod::dbg::log("[WKI] Dev proxy attach timeout after %d retries: node=0x%04x res_id=%u proxies_left=%u", MAX_ATTACH_RETRIES,
                           owner_node, resource_id, static_cast<unsigned>(g_proxies.size()));
        return nullptr;
    }

    // Check attach result
    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Dev proxy attach rejected: node=0x%04x res_id=%u status=%u", owner_node, resource_id,
                           state->attach_status);
        g_proxies.pop_back();
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
                state->next_tag = 1;
                for (auto& pc : state->pending_cq) {
                    pc.valid = false;
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
        state->lock.lock();
        state->op_pending = true;
        state->op_status = 0;
        state->op_resp_buf = static_cast<void*>(info_buf.data());
        state->op_resp_max = info_buf.size();
        state->op_resp_len = 0;
        state->lock.unlock();

        int send_ret = wki_send(owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &info_req, sizeof(info_req));
        if (send_ret != WKI_OK) {
            mod::dbg::log("[WKI] Dev proxy block info request send failed: node=0x%04x res_id=%u err=%d", owner_node, resource_id,
                          send_ret);
            DevDetachPayload det = {};
            det.target_node = owner_node;
            det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
            det.resource_id = resource_id;
            wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
            g_proxies.pop_back();
            return nullptr;
        }

        WkiChannel* info_ch = wki_channel_get(owner_node, state->assigned_channel);
        uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
        while (state->op_pending) {
            asm volatile("pause" ::: "memory");
            if (!state->op_pending) {
                break;
            }
            if (wki_now_us() >= deadline) {
                mod::dbg::log("[WKI] Dev proxy block info request timeout: node=0x%04x res_id=%u", owner_node, resource_id);
                state->op_pending = false;
                g_proxies.pop_back();
                return nullptr;
            }
            wki_spin_yield_channel(info_ch);
        }

        if (state->op_status != 0 || state->op_resp_len < 16) {
            g_proxies.pop_back();
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
        g_proxies.pop_back();
        return nullptr;
    }

    // Register in the block device subsystem
    ker::dev::block_device_register(&state->bdev);

    ker::mod::dbg::log("[WKI] Dev proxy attached: %s node=0x%04x res_id=%u ch=%u bs=%u tb=%llu", state->bdev.name.data(), owner_node,
                       resource_id, state->assigned_channel, static_cast<unsigned>(block_size), total_blocks);

    return &state->bdev;
}

void wki_dev_proxy_detach_block(ker::dev::BlockDevice* proxy_bdev) {
    auto* state = find_proxy_by_bdev(proxy_bdev);
    if (state == nullptr) {
        return;
    }

    // Free read-ahead cache
    if (state->ra_buffer != nullptr) {
        ker::mod::mm::dyn::kmalloc::free(state->ra_buffer);
        state->ra_buffer = nullptr;
    }
    ra_invalidate(state);

    // Destroy RDMA zone before sending detach
    if (state->rdma_attached && state->rdma_zone_id != 0) {
        wki_zone_destroy(state->rdma_zone_id);
        state->rdma_attached = false;
        state->rdma_zone_ptr = nullptr;
        state->rdma_zone_id = 0;
        state->rdma_roce = false;
        state->rdma_transport = nullptr;
        state->rdma_remote_rkey = 0;
    }

    // Unregister from block device subsystem
    ker::dev::block_device_unregister(&state->bdev);

    // Send DEV_DETACH to owner
    DevDetachPayload det = {};
    det.target_node = state->owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    det.resource_id = state->resource_id;
    wki_send(state->owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    ker::mod::dbg::log("[WKI] Dev proxy detached: %s", state->bdev.name.data());

    state->active = false;

    // Remove inactive entries
    for (auto it = g_proxies.begin(); it != g_proxies.end();) {
        if (!(*it)->active) {
            it = g_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

// -----------------------------------------------------------------------------
// Fencing — suspend / resume / hard teardown
// -----------------------------------------------------------------------------

void wki_dev_proxy_suspend_for_peer(uint16_t node_id) {
    uint64_t now = wki_now_us();
    for (auto& p : g_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        // Fail any in-flight operation so the spin-wait unblocks,
        // but keep the proxy registered — callers will see fenced==true
        // on retry and block in wait_for_fence_lift().
        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending = false;
        }

        // Free read-ahead cache
        if (p->ra_buffer != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(p->ra_buffer);
            p->ra_buffer = nullptr;
        }
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

        // Close the dynamic channel (stale seq/ack state)
        WkiChannel* ch = wki_channel_get(p->owner_node, p->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        p->fenced = true;
        p->fence_time_us = now;

        ker::mod::dbg::log("[WKI] Dev proxy suspended (fenced): %s node=0x%04x — I/O will block until reconnect or %llu s timeout",
                           p->bdev.name.data(), node_id, WKI_DEV_PROXY_FENCE_WAIT_US / 1000000ULL);
    }
}

void wki_dev_proxy_resume_for_peer(uint16_t node_id) {
    for (auto& p : g_proxies) {
        if (!p->active || !p->fenced || p->owner_node != node_id) {
            continue;
        }

        // Re-attach: send DEV_ATTACH_REQ to get a fresh dynamic channel
        DevAttachReqPayload attach_req = {};
        attach_req.target_node = node_id;
        attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
        attach_req.resource_id = p->resource_id;
        attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
        attach_req.requested_channel = 0;  // auto-assign

        p->attach_pending = true;
        p->attach_status = 0;
        p->attach_channel = 0;

        constexpr int MAX_RESUME_RETRIES = 5;
        bool attached = false;
        for (int retry = 0; retry < MAX_RESUME_RETRIES; retry++) {
            if (retry > 0) {
                p->attach_pending = true;
            }

            int send_ret = wki_send(node_id, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
            if (send_ret != WKI_OK) {
                continue;
            }

            WkiChannel* res_ch = wki_channel_get(node_id, WKI_CHAN_RESOURCE);
            uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
            while (p->attach_pending) {
                asm volatile("pause" ::: "memory");
                if (!p->attach_pending) {
                    break;
                }
                if (wki_now_us() >= deadline) {
                    break;
                }
                wki_spin_yield_channel(res_ch);
            }

            if (!p->attach_pending && p->attach_status == static_cast<uint8_t>(DevAttachStatus::OK)) {
                attached = true;
                break;
            }
        }

        if (!attached) {
            ker::mod::dbg::log("[WKI] Dev proxy resume FAILED (re-attach): %s node=0x%04x — will hard-detach", p->bdev.name.data(),
                               node_id);
            p->attach_pending = false;
            // Leave fenced=true; the fence_timeout_tick will clean it up
            continue;
        }

        p->assigned_channel = p->attach_channel;
        p->max_op_size = p->attach_max_op_size;

        // Re-attach RDMA zone if the new attach ACK provided one
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
                    p->next_tag = 1;
                    for (auto& pc : p->pending_cq) {
                        pc.valid = false;
                    }

                    // Re-allocate read-ahead cache if needed
                    if (p->ra_buffer == nullptr) {
                        p->ra_buffer = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(ring_hdr->data_slot_size));
                        if (p->ra_buffer != nullptr) {
                            p->ra_capacity = ring_hdr->data_slot_size / ring_hdr->block_size;
                        }
                    }
                    ra_invalidate(p.get());

                    ker::mod::dbg::log("[WKI] Dev proxy RDMA ring re-attached on resume: zone=0x%08x roce=%d", p->rdma_zone_id,
                                       p->rdma_roce ? 1 : 0);
                }
            }
        }

        p->fenced = false;
        p->fence_time_us = 0;

        ker::mod::dbg::log("[WKI] Dev proxy resumed: %s node=0x%04x ch=%u — blocked I/O will now proceed", p->bdev.name.data(), node_id,
                           p->assigned_channel);
    }
}

void wki_dev_proxy_detach_all_for_peer(uint16_t node_id) {
    for (auto& p : g_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        // Fail any pending operation
        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending = false;
        }

        // Free read-ahead cache
        if (p->ra_buffer != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(p->ra_buffer);
            p->ra_buffer = nullptr;
        }
        ra_invalidate(p.get());

        // Destroy RDMA zone
        if (p->rdma_zone_id != 0) {
            wki_zone_destroy(p->rdma_zone_id);
            p->rdma_attached = false;
            p->rdma_zone_ptr = nullptr;
            p->rdma_zone_id = 0;
            p->rdma_roce = false;
            p->rdma_transport = nullptr;
            p->rdma_remote_rkey = 0;
        }

        // Close the dynamic channel
        WkiChannel* ch = wki_channel_get(p->owner_node, p->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        // Unregister from block device subsystem
        ker::dev::block_device_unregister(&p->bdev);

        ker::mod::dbg::log("[WKI] Dev proxy hard-detached: %s node=0x%04x", p->bdev.name.data(), node_id);

        p->fenced = false;
        p->active = false;
    }

    // Remove inactive entries
    for (auto it = g_proxies.begin(); it != g_proxies.end();) {
        if (!(*it)->active) {
            it = g_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

void wki_dev_proxy_fence_timeout_tick(uint64_t now_us) {
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
            ker::mod::dbg::log("[WKI] Dev proxy fence timeout (%llu s): %s node=0x%04x — tearing down", elapsed / 1000000ULL,
                               p->bdev.name.data(), p->owner_node);

            // 1. Unregister from block device subsystem — prevents new I/O
            ker::dev::block_device_unregister(&p->bdev);

            // 2. Mark inactive FIRST so that wait_for_fence_lift() returns
            //    false.  Leave fenced=true so spinning I/O threads stay in
            //    the wait loop until they check active and bail out.
            //    Memory barrier ensures the active=false store is visible to
            //    other CPUs before we continue.
            p->active = false;
            asm volatile("mfence" ::: "memory");

            // Do NOT erase the proxy from g_proxies here — I/O threads on
            // other CPUs may still hold a raw ProxyBlockState* pointer from
            // find_proxy_by_bdev().  Freeing the memory would cause a
            // use-after-free.  The dead entry is harmless: find_proxy_by_bdev
            // checks active, and all other g_proxies iterators skip inactive
            // entries.
        }
    }
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

    // Release the spin-wait
    asm volatile("" ::: "memory");
    state->attach_pending = false;
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

    // Release the spin-wait
    asm volatile("" ::: "memory");
    state->op_pending = false;
}

}  // namespace detail

}  // namespace ker::net::wki
