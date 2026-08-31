#pragma once

#include <array>
#include <cstdint>
#include <limits>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Block RDMA Ring - Shared Memory Layout
//
// This header defines data structures that live in an RDMA zone shared between
// the block device server (owner) and the consumer (proxy). Both sides read and
// write these structures directly - no WKI messages carry block data.
//
// Layout within the RDMA zone:
//   [0..63]                          BlkRingHeader (control, cache-line aligned)
//   [64..64+SQ_SIZE-1]               Submission queue entries (consumer->server)
//   [64+SQ_SIZE..64+SQ_SIZE+CQ_SIZE] Completion queue entries (server->consumer)
//   [DATA_OFFSET..end]               Data slots (block data transfer area)
//
// Ring protocol:
//   - SQ: consumer writes at sq_head, server reads at sq_tail (SPSC)
//   - CQ: server writes at cq_head, consumer reads at cq_tail (SPSC)
//   - Data slots: consumer fills (for writes) or server fills (for reads)
//   - Signaling: doorbell (ivshmem/RoCE) or ZONE_NOTIFY_POST (fallback)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Configuration defaults (can be overridden at zone creation time)
// -----------------------------------------------------------------------------

constexpr uint32_t BLK_RING_DEFAULT_SQ_DEPTH = 64;
constexpr uint32_t BLK_RING_DEFAULT_CQ_DEPTH = 64;
constexpr uint32_t BLK_RING_DEFAULT_DATA_SLOTS = 64;
constexpr uint32_t BLK_RING_DEFAULT_DATA_SLOT_SIZE = 65536;  // 64KB per slot
constexpr uint32_t BLK_RING_MAX_BULK_TRANSFER = 2 * 1024 * 1024;

// -----------------------------------------------------------------------------
// Submission Queue Entry - consumer writes, server reads
// -----------------------------------------------------------------------------

enum class BlkOpcode : uint8_t {
    READ = 0,
    WRITE = 1,
    FLUSH = 2,
    BULK_READ = 3,   // Streaming bulk: server RDMA-writes entire block range to consumer's registered buffer
    BULK_WRITE = 4,  // Streaming bulk: server RDMA-reads entire block range from consumer's registered buffer
};

// Threshold in bytes above which the bulk transfer path is used instead of the
// async SQ pipeline.  Transfers <= this size use standard per-slot I/O.
constexpr uint32_t BLK_RING_BULK_THRESHOLD = 262144;  // 256 KB

struct BlkSqEntry {
    uint32_t tag;    // unique request ID (consumer-assigned, echoed in CQE)
    uint8_t opcode;  // BlkOpcode
    std::array<uint8_t, 3> reserved;
    uint64_t lba;          // starting logical block address
    uint32_t block_count;  // number of blocks to read/write
    uint32_t data_slot;    // index into the data region (0..data_slot_count-1)
    // For WRITE: consumer fills data_slot before posting SQE
    // For READ:  server fills data_slot, then posts CQE
    // For FLUSH: data_slot is ignored
} __attribute__((packed));

static_assert(sizeof(BlkSqEntry) == 24, "BlkSqEntry must be 24 bytes");

// -----------------------------------------------------------------------------
// Bulk Transfer SQ Entry - same size as BlkSqEntry, union-compatible.
// Used with BULK_READ / BULK_WRITE opcodes.  Instead of a data_slot index the
// consumer passes its RDMA rkey so the server can RDMA-write/read the entire
// block range directly into/from the consumer's registered buffer.
// -----------------------------------------------------------------------------

struct BlkBulkSqEntry {
    uint32_t tag;    // unique request ID (consumer-assigned)
    uint8_t opcode;  // BlkOpcode::BULK_READ or BULK_WRITE
    std::array<uint8_t, 3> reserved;
    uint64_t lba;          // starting logical block address
    uint32_t block_count;  // number of blocks to transfer
    uint32_t roce_rkey;    // consumer's RDMA rkey for the registered staging buffer
} __attribute__((packed));

static_assert(sizeof(BlkBulkSqEntry) == 24, "BlkBulkSqEntry must be 24 bytes (union-compatible with BlkSqEntry)");

// -----------------------------------------------------------------------------
// Completion Queue Entry - server writes, consumer reads
// -----------------------------------------------------------------------------

struct BlkCqEntry {
    uint32_t tag;                // echoed from SQE
    int32_t status;              // 0 = success, negative = error
    uint32_t data_slot;          // which data slot has the result (for reads)
    uint32_t bytes_transferred;  // actual bytes transferred
} __attribute__((packed));

static_assert(sizeof(BlkCqEntry) == 16, "BlkCqEntry must be 16 bytes");

// -----------------------------------------------------------------------------
// Ring Header - first 64 bytes of the zone (cache-line aligned)
// -----------------------------------------------------------------------------

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
    uint32_t data_slot_size;  // bytes per data slot
    uint32_t block_size;      // block device block size (e.g. 512, 4096)
    uint64_t total_blocks;    // block device total blocks

    volatile uint8_t server_ready;     // 1 when server has initialized the ring
    std::array<uint8_t, 19> reserved;  // pad to exactly 64 bytes (cache line)
} __attribute__((packed));

static_assert(sizeof(BlkRingHeader) == 64, "BlkRingHeader must be exactly 64 bytes");

// -----------------------------------------------------------------------------
// Validation helpers
// -----------------------------------------------------------------------------

struct BlkTransferValidation {
    bool valid = false;
    uint32_t bytes = 0;
};

constexpr auto blk_lba_range_valid(uint64_t lba, uint64_t block_count, uint64_t total_blocks) -> bool {
    return lba <= total_blocks && block_count <= total_blocks - lba;
}

// Validate both the logical-block range and the byte multiplication before a
// caller allocates or accesses a transfer buffer.  A zero-block operation is
// valid at any in-range LBA, matching the local block-device API; the shared
// ring validator below rejects zero-length READ/WRITE descriptors explicitly.
constexpr auto blk_validate_transfer(uint64_t lba, uint32_t block_count, uint64_t block_size, uint64_t total_blocks, uint64_t max_bytes)
    -> BlkTransferValidation {
    if (block_size == 0 || !blk_lba_range_valid(lba, block_count, total_blocks)) {
        return {};
    }
    if (block_count != 0 && block_size > std::numeric_limits<uint64_t>::max() / block_count) {
        return {};
    }

    uint64_t const BYTES = static_cast<uint64_t>(block_count) * block_size;
    if (BYTES > max_bytes || BYTES > std::numeric_limits<uint32_t>::max()) {
        return {};
    }
    return {.valid = true, .bytes = static_cast<uint32_t>(BYTES)};
}

struct BlkRingGeometry {
    uint32_t sq_depth = 0;
    uint32_t cq_depth = 0;
    uint32_t data_slot_count = 0;
    uint32_t data_slot_size = 0;
    uint32_t block_size = 0;
    uint64_t total_blocks = 0;
    uint8_t server_ready = 0;
};

struct BlkRingIndices {
    uint32_t sq_head = 0;
    uint32_t sq_tail = 0;
    uint32_t cq_head = 0;
    uint32_t cq_tail = 0;
};

inline auto blk_ring_geometry_snapshot(const BlkRingHeader* hdr) -> BlkRingGeometry {
    if (hdr == nullptr) {
        return {};
    }
    const volatile BlkRingHeader* shared = hdr;
    return {.sq_depth = shared->sq_depth,
            .cq_depth = shared->cq_depth,
            .data_slot_count = shared->data_slot_count,
            .data_slot_size = shared->data_slot_size,
            .block_size = shared->block_size,
            .total_blocks = shared->total_blocks,
            .server_ready = shared->server_ready};
}

inline auto blk_ring_indices_snapshot(const BlkRingHeader* hdr) -> BlkRingIndices {
    if (hdr == nullptr) {
        return {};
    }
    const volatile BlkRingHeader* shared = hdr;
    return {.sq_head = shared->sq_head, .sq_tail = shared->sq_tail, .cq_head = shared->cq_head, .cq_tail = shared->cq_tail};
}

// Block rings currently have one fixed, server-created layout.  Requiring the
// exact negotiated values prevents a corrupt remote-writable header from
// redirecting queue or data-slot accesses outside that allocation.
constexpr auto blk_ring_layout_valid(const BlkRingGeometry& geometry) -> bool {
    return geometry.server_ready == 1 && geometry.sq_depth == BLK_RING_DEFAULT_SQ_DEPTH && geometry.cq_depth == BLK_RING_DEFAULT_CQ_DEPTH &&
           geometry.data_slot_count == BLK_RING_DEFAULT_DATA_SLOTS && geometry.data_slot_size == BLK_RING_DEFAULT_DATA_SLOT_SIZE &&
           geometry.block_size != 0 && geometry.block_size <= geometry.data_slot_size;
}

constexpr auto blk_ring_geometry_valid(const BlkRingGeometry& geometry, uint64_t expected_block_size, uint64_t expected_total_blocks)
    -> bool {
    return blk_ring_layout_valid(geometry) && expected_block_size != 0 && expected_block_size <= std::numeric_limits<uint32_t>::max() &&
           geometry.block_size == static_cast<uint32_t>(expected_block_size) && geometry.total_blocks == expected_total_blocks;
}

constexpr auto blk_ring_indices_valid(const BlkRingIndices& indices, const BlkRingGeometry& geometry) -> bool {
    return geometry.sq_depth != 0 && geometry.cq_depth != 0 && indices.sq_head < geometry.sq_depth && indices.sq_tail < geometry.sq_depth &&
           indices.cq_head < geometry.cq_depth && indices.cq_tail < geometry.cq_depth;
}

constexpr auto blk_ring_next_index(uint32_t index, uint32_t depth) -> uint32_t { return index + 1 == depth ? 0 : index + 1; }

struct BlkSqValidation {
    bool valid = false;
    uint32_t bytes = 0;
};

constexpr auto blk_validate_sq_entry(const BlkSqEntry& entry, const BlkRingGeometry& geometry) -> BlkSqValidation {
    switch (static_cast<BlkOpcode>(entry.opcode)) {
        case BlkOpcode::READ:
        case BlkOpcode::WRITE: {
            if (entry.block_count == 0 || entry.data_slot >= geometry.data_slot_count) {
                return {};
            }
            BlkTransferValidation const TRANSFER =
                blk_validate_transfer(entry.lba, entry.block_count, geometry.block_size, geometry.total_blocks, geometry.data_slot_size);
            return {.valid = TRANSFER.valid, .bytes = TRANSFER.bytes};
        }
        case BlkOpcode::FLUSH:
            return {.valid = true, .bytes = 0};
        case BlkOpcode::BULK_READ:
        case BlkOpcode::BULK_WRITE: {
            if (entry.data_slot == 0 || entry.block_count == 0) {
                return {};
            }
            BlkTransferValidation const TRANSFER =
                blk_validate_transfer(entry.lba, entry.block_count, geometry.block_size, geometry.total_blocks, BLK_RING_MAX_BULK_TRANSFER);
            return {.valid = TRANSFER.valid, .bytes = TRANSFER.bytes};
        }
    }
    return {};
}

// -----------------------------------------------------------------------------
// Offset calculations - compute layout within a zone
// -----------------------------------------------------------------------------

// Header is at offset 0, padded to 64 bytes (cache line)
constexpr uint32_t BLK_RING_HEADER_SIZE = 64;

constexpr auto blk_ring_sq_offset() -> uint32_t { return BLK_RING_HEADER_SIZE; }

constexpr auto blk_ring_sq_size(uint32_t depth) -> uint32_t { return depth * sizeof(BlkSqEntry); }

constexpr auto blk_ring_cq_offset(uint32_t sq_depth) -> uint32_t { return blk_ring_sq_offset() + blk_ring_sq_size(sq_depth); }

constexpr auto blk_ring_cq_size(uint32_t depth) -> uint32_t { return depth * sizeof(BlkCqEntry); }

constexpr auto blk_ring_data_offset(uint32_t sq_depth, uint32_t cq_depth) -> uint32_t {
    return blk_ring_cq_offset(sq_depth) + blk_ring_cq_size(cq_depth);
}

constexpr auto blk_ring_data_size(uint32_t slot_count, uint32_t slot_size) -> uint32_t { return slot_count * slot_size; }

// Total zone size (page-aligned upward, 4KB pages)
constexpr auto blk_ring_zone_size(uint32_t sq_depth, uint32_t cq_depth, uint32_t slot_count, uint32_t slot_size) -> uint32_t {
    uint32_t const RAW = blk_ring_data_offset(sq_depth, cq_depth) + blk_ring_data_size(slot_count, slot_size);
    return (RAW + 0xFFFU) & ~0xFFFU;
}

// Default zone size with default parameters
constexpr auto blk_ring_default_zone_size() -> uint32_t {
    return blk_ring_zone_size(BLK_RING_DEFAULT_SQ_DEPTH, BLK_RING_DEFAULT_CQ_DEPTH, BLK_RING_DEFAULT_DATA_SLOTS,
                              BLK_RING_DEFAULT_DATA_SLOT_SIZE);
}

// -----------------------------------------------------------------------------
// Inline accessors - cast into the zone's shared memory
// -----------------------------------------------------------------------------

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
inline auto blk_sq_entries(void* zone_base, const BlkRingHeader* /*unused*/) -> BlkSqEntry* { return blk_sq_entries(zone_base); }

inline auto blk_cq_entries(void* zone_base, const BlkRingHeader* hdr) -> BlkCqEntry* { return blk_cq_entries(zone_base, hdr->sq_depth); }

inline auto blk_data_slot(void* zone_base, const BlkRingHeader* hdr, uint32_t slot_idx) -> uint8_t* {
    return blk_data_slot(zone_base, hdr->sq_depth, hdr->cq_depth, slot_idx, hdr->data_slot_size);
}

// -----------------------------------------------------------------------------
// Ring state queries - SPSC lock-free
// -----------------------------------------------------------------------------

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
