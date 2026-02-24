#pragma once

#include <cstdint>

namespace ker::net::wki {

// ─────────────────────────────────────────────────────────────────────────────
// Block RDMA Ring — Shared Memory Layout
//
// This header defines data structures that live in an RDMA zone shared between
// the block device server (owner) and the consumer (proxy). Both sides read and
// write these structures directly — no WKI messages carry block data.
//
// Layout within the RDMA zone:
//   [0..63]                          BlkRingHeader (control, cache-line aligned)
//   [64..64+SQ_SIZE-1]               Submission queue entries (consumer→server)
//   [64+SQ_SIZE..64+SQ_SIZE+CQ_SIZE] Completion queue entries (server→consumer)
//   [DATA_OFFSET..end]               Data slots (block data transfer area)
//
// Ring protocol:
//   - SQ: consumer writes at sq_head, server reads at sq_tail (SPSC)
//   - CQ: server writes at cq_head, consumer reads at cq_tail (SPSC)
//   - Data slots: consumer fills (for writes) or server fills (for reads)
//   - Signaling: doorbell (ivshmem/RoCE) or ZONE_NOTIFY_POST (fallback)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Configuration defaults (can be overridden at zone creation time)
// ─────────────────────────────────────────────────────────────────────────────

constexpr uint32_t BLK_RING_DEFAULT_SQ_DEPTH = 64;
constexpr uint32_t BLK_RING_DEFAULT_CQ_DEPTH = 64;
constexpr uint32_t BLK_RING_DEFAULT_DATA_SLOTS = 64;
constexpr uint32_t BLK_RING_DEFAULT_DATA_SLOT_SIZE = 65536;  // 64KB per slot

// ─────────────────────────────────────────────────────────────────────────────
// Submission Queue Entry — consumer writes, server reads
// ─────────────────────────────────────────────────────────────────────────────

enum class BlkOpcode : uint8_t {
    READ = 0,
    WRITE = 1,
    FLUSH = 2,
};

struct BlkSqEntry {
    uint32_t tag;          // unique request ID (consumer-assigned, echoed in CQE)
    uint8_t opcode;        // BlkOpcode
    uint8_t reserved[3];
    uint64_t lba;          // starting logical block address
    uint32_t block_count;  // number of blocks to read/write
    uint32_t data_slot;    // index into the data region (0..data_slot_count-1)
    // For WRITE: consumer fills data_slot before posting SQE
    // For READ:  server fills data_slot, then posts CQE
    // For FLUSH: data_slot is ignored
} __attribute__((packed));

static_assert(sizeof(BlkSqEntry) == 24, "BlkSqEntry must be 24 bytes");

// ─────────────────────────────────────────────────────────────────────────────
// Completion Queue Entry — server writes, consumer reads
// ─────────────────────────────────────────────────────────────────────────────

struct BlkCqEntry {
    uint32_t tag;              // echoed from SQE
    int32_t status;            // 0 = success, negative = error
    uint32_t data_slot;        // which data slot has the result (for reads)
    uint32_t bytes_transferred;  // actual bytes transferred
} __attribute__((packed));

static_assert(sizeof(BlkCqEntry) == 16, "BlkCqEntry must be 16 bytes");

// ─────────────────────────────────────────────────────────────────────────────
// Ring Header — first 64 bytes of the zone (cache-line aligned)
// ─────────────────────────────────────────────────────────────────────────────

struct BlkRingHeader {
    // SQ pointers: consumer produces at sq_head, server consumes at sq_tail
    volatile uint32_t sq_head;  // next slot for consumer to write
    volatile uint32_t sq_tail;  // next slot for server to read

    // CQ pointers: server produces at cq_head, consumer consumes at cq_tail
    volatile uint32_t cq_head;  // next slot for server to write
    volatile uint32_t cq_tail;  // next slot for consumer to read

    // Negotiated parameters (set by server during zone init)
    uint32_t sq_depth;
    uint32_t cq_depth;
    uint32_t data_slot_count;
    uint32_t data_slot_size;    // bytes per data slot
    uint32_t block_size;        // block device block size (e.g. 512, 4096)
    uint64_t total_blocks;      // block device total blocks

    volatile uint8_t server_ready;  // 1 when server has initialized the ring
    uint8_t reserved[19];           // pad to exactly 64 bytes (cache line)
} __attribute__((packed));

static_assert(sizeof(BlkRingHeader) == 64, "BlkRingHeader must be exactly 64 bytes");

// ─────────────────────────────────────────────────────────────────────────────
// Offset calculations — compute layout within a zone
// ─────────────────────────────────────────────────────────────────────────────

// Header is at offset 0, padded to 64 bytes (cache line)
constexpr uint32_t BLK_RING_HEADER_SIZE = 64;

inline constexpr auto blk_ring_sq_offset() -> uint32_t { return BLK_RING_HEADER_SIZE; }

inline constexpr auto blk_ring_sq_size(uint32_t depth) -> uint32_t { return depth * sizeof(BlkSqEntry); }

inline constexpr auto blk_ring_cq_offset(uint32_t sq_depth) -> uint32_t { return blk_ring_sq_offset() + blk_ring_sq_size(sq_depth); }

inline constexpr auto blk_ring_cq_size(uint32_t depth) -> uint32_t { return depth * sizeof(BlkCqEntry); }

inline constexpr auto blk_ring_data_offset(uint32_t sq_depth, uint32_t cq_depth) -> uint32_t {
    return blk_ring_cq_offset(sq_depth) + blk_ring_cq_size(cq_depth);
}

inline constexpr auto blk_ring_data_size(uint32_t slot_count, uint32_t slot_size) -> uint32_t { return slot_count * slot_size; }

// Total zone size (page-aligned upward, 4KB pages)
inline constexpr auto blk_ring_zone_size(uint32_t sq_depth, uint32_t cq_depth, uint32_t slot_count, uint32_t slot_size) -> uint32_t {
    uint32_t raw = blk_ring_data_offset(sq_depth, cq_depth) + blk_ring_data_size(slot_count, slot_size);
    return (raw + 0xFFFU) & ~0xFFFU;
}

// Default zone size with default parameters
inline constexpr auto blk_ring_default_zone_size() -> uint32_t {
    return blk_ring_zone_size(BLK_RING_DEFAULT_SQ_DEPTH, BLK_RING_DEFAULT_CQ_DEPTH, BLK_RING_DEFAULT_DATA_SLOTS, BLK_RING_DEFAULT_DATA_SLOT_SIZE);
}

// ─────────────────────────────────────────────────────────────────────────────
// Inline accessors — cast into the zone's shared memory
// ─────────────────────────────────────────────────────────────────────────────

inline auto blk_ring_header(void* zone_base) -> BlkRingHeader* { return reinterpret_cast<BlkRingHeader*>(zone_base); }

inline auto blk_ring_header(const void* zone_base) -> const BlkRingHeader* { return reinterpret_cast<const BlkRingHeader*>(zone_base); }

inline auto blk_sq_entries(void* zone_base) -> BlkSqEntry* {
    return reinterpret_cast<BlkSqEntry*>(static_cast<uint8_t*>(zone_base) + blk_ring_sq_offset());
}

inline auto blk_cq_entries(void* zone_base, uint32_t sq_depth) -> BlkCqEntry* {
    return reinterpret_cast<BlkCqEntry*>(static_cast<uint8_t*>(zone_base) + blk_ring_cq_offset(sq_depth));
}

inline auto blk_data_slot(void* zone_base, uint32_t sq_depth, uint32_t cq_depth, uint32_t slot_idx, uint32_t slot_size) -> uint8_t* {
    return static_cast<uint8_t*>(zone_base) + blk_ring_data_offset(sq_depth, cq_depth) + (static_cast<uint64_t>(slot_idx) * slot_size);
}

// Convenience: use header's own depth/size fields
inline auto blk_sq_entries(void* zone_base, const BlkRingHeader*) -> BlkSqEntry* { return blk_sq_entries(zone_base); }

inline auto blk_cq_entries(void* zone_base, const BlkRingHeader* hdr) -> BlkCqEntry* { return blk_cq_entries(zone_base, hdr->sq_depth); }

inline auto blk_data_slot(void* zone_base, const BlkRingHeader* hdr, uint32_t slot_idx) -> uint8_t* {
    return blk_data_slot(zone_base, hdr->sq_depth, hdr->cq_depth, slot_idx, hdr->data_slot_size);
}

// ─────────────────────────────────────────────────────────────────────────────
// Ring state queries — SPSC lock-free
// ─────────────────────────────────────────────────────────────────────────────

// SQ full: consumer cannot post (one slot wasted as sentinel)
inline auto blk_sq_full(const BlkRingHeader* hdr) -> bool { return ((hdr->sq_head + 1) % hdr->sq_depth) == hdr->sq_tail; }

// SQ empty: server has no work
inline auto blk_sq_empty(const BlkRingHeader* hdr) -> bool { return hdr->sq_head == hdr->sq_tail; }

// SQ count: number of entries available for server to consume
inline auto blk_sq_count(const BlkRingHeader* hdr) -> uint32_t { return (hdr->sq_head - hdr->sq_tail + hdr->sq_depth) % hdr->sq_depth; }

// CQ full: server cannot post completions
inline auto blk_cq_full(const BlkRingHeader* hdr) -> bool { return ((hdr->cq_head + 1) % hdr->cq_depth) == hdr->cq_tail; }

// CQ empty: consumer has no completions to read
inline auto blk_cq_empty(const BlkRingHeader* hdr) -> bool { return hdr->cq_head == hdr->cq_tail; }

// CQ count: number of completions available for consumer
inline auto blk_cq_count(const BlkRingHeader* hdr) -> uint32_t { return (hdr->cq_head - hdr->cq_tail + hdr->cq_depth) % hdr->cq_depth; }

}  // namespace ker::net::wki
