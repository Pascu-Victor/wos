#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <net/wki/blk_ring.hpp>

namespace {

using namespace ker::net::wki;

constexpr auto valid_geometry() -> BlkRingGeometry {
    return {.sq_depth = BLK_RING_DEFAULT_SQ_DEPTH,
            .cq_depth = BLK_RING_DEFAULT_CQ_DEPTH,
            .data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS,
            .data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE,
            .block_size = 512,
            .total_blocks = 8192,
            .server_ready = 1};
}

TEST(WkiBlockValidation, TransferBoundariesRejectOverflowAndOutOfRangeRequests) {
    struct Vector {
        uint64_t lba;
        uint32_t count;
        uint64_t block_size;
        uint64_t total_blocks;
        uint64_t max_bytes;
        bool valid;
        uint32_t bytes;
    };

    constexpr std::array VECTORS{
        Vector{.lba = 8, .count = 2, .block_size = 512, .total_blocks = 10, .max_bytes = 1024, .valid = true, .bytes = 1024},
        Vector{.lba = 10, .count = 0, .block_size = 512, .total_blocks = 10, .max_bytes = 0, .valid = true, .bytes = 0},
        Vector{.lba = 11, .count = 0, .block_size = 512, .total_blocks = 10, .max_bytes = 0, .valid = false, .bytes = 0},
        Vector{.lba = 9, .count = 2, .block_size = 512, .total_blocks = 10, .max_bytes = 1024, .valid = false, .bytes = 0},
        Vector{.lba = 0, .count = 1, .block_size = 0, .total_blocks = 10, .max_bytes = 512, .valid = false, .bytes = 0},
        Vector{.lba = 0, .count = 2, .block_size = 512, .total_blocks = 10, .max_bytes = 1023, .valid = false, .bytes = 0},
        Vector{.lba = 0,
               .count = 2,
               .block_size = std::numeric_limits<uint64_t>::max(),
               .total_blocks = 10,
               .max_bytes = std::numeric_limits<uint64_t>::max(),
               .valid = false,
               .bytes = 0},
        Vector{.lba = 0,
               .count = 2,
               .block_size = std::numeric_limits<uint32_t>::max(),
               .total_blocks = 10,
               .max_bytes = std::numeric_limits<uint64_t>::max(),
               .valid = false,
               .bytes = 0},
        Vector{.lba = 0,
               .count = 1,
               .block_size = std::numeric_limits<uint32_t>::max(),
               .total_blocks = 1,
               .max_bytes = std::numeric_limits<uint32_t>::max(),
               .valid = true,
               .bytes = std::numeric_limits<uint32_t>::max()},
    };

    for (const auto& vector : VECTORS) {
        BlkTransferValidation const RESULT =
            blk_validate_transfer(vector.lba, vector.count, vector.block_size, vector.total_blocks, vector.max_bytes);
        EXPECT_EQ(RESULT.valid, vector.valid);
        EXPECT_EQ(RESULT.bytes, vector.bytes);
    }
}

TEST(WkiBlockValidation, RingGeometryMustMatchTheFixedServerAllocation) {
    BlkRingGeometry geometry = valid_geometry();
    EXPECT_TRUE(blk_ring_layout_valid(geometry));
    EXPECT_TRUE(blk_ring_geometry_valid(geometry, 512, 8192));

    geometry.server_ready = 0;
    EXPECT_FALSE(blk_ring_layout_valid(geometry));
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    geometry.sq_depth = 0;
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    geometry.sq_depth++;
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    geometry.cq_depth--;
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    geometry.data_slot_count++;
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    geometry.data_slot_size--;
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    geometry.block_size = 4096;
    EXPECT_TRUE(blk_ring_layout_valid(geometry));
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry.block_size = geometry.data_slot_size + 1;
    EXPECT_FALSE(blk_ring_layout_valid(geometry));
    geometry = valid_geometry();
    geometry.total_blocks--;
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry = valid_geometry();
    EXPECT_FALSE(blk_ring_geometry_valid(geometry, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1, 8192));
}

TEST(WkiBlockValidation, FixedDepthIndexWrappingHasNoModuloPrecondition) {
    EXPECT_EQ(blk_ring_next_index(0, BLK_RING_DEFAULT_SQ_DEPTH), 1u);
    EXPECT_EQ(blk_ring_next_index(BLK_RING_DEFAULT_SQ_DEPTH - 1, BLK_RING_DEFAULT_SQ_DEPTH), 0u);
    EXPECT_EQ(blk_ring_next_index(BLK_RING_DEFAULT_CQ_DEPTH - 1, BLK_RING_DEFAULT_CQ_DEPTH), 0u);
}

TEST(WkiBlockValidation, EveryRingIndexMustRemainInsideItsNegotiatedQueue) {
    BlkRingGeometry const GEOMETRY = valid_geometry();
    BlkRingIndices indices = {};
    EXPECT_TRUE(blk_ring_indices_valid(indices, GEOMETRY));

    indices.sq_head = GEOMETRY.sq_depth;
    EXPECT_FALSE(blk_ring_indices_valid(indices, GEOMETRY));
    indices = {};
    indices.sq_tail = GEOMETRY.sq_depth;
    EXPECT_FALSE(blk_ring_indices_valid(indices, GEOMETRY));
    indices = {};
    indices.cq_head = GEOMETRY.cq_depth;
    EXPECT_FALSE(blk_ring_indices_valid(indices, GEOMETRY));
    indices = {};
    indices.cq_tail = GEOMETRY.cq_depth;
    EXPECT_FALSE(blk_ring_indices_valid(indices, GEOMETRY));
}

TEST(WkiBlockValidation, SubmissionValidationBoundsSlotsRangesAndBulkStaging) {
    BlkRingGeometry const GEOMETRY = valid_geometry();
    BlkSqEntry entry = {};
    entry.opcode = static_cast<uint8_t>(BlkOpcode::READ);
    entry.lba = GEOMETRY.total_blocks - 128;
    entry.block_count = 128;
    entry.data_slot = GEOMETRY.data_slot_count - 1;

    BlkSqValidation result = blk_validate_sq_entry(entry, GEOMETRY);
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.bytes, BLK_RING_DEFAULT_DATA_SLOT_SIZE);

    entry.block_count = 0;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.opcode = static_cast<uint8_t>(BlkOpcode::WRITE);
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.opcode = static_cast<uint8_t>(BlkOpcode::READ);
    entry.block_count = 128;
    entry.block_count++;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.block_count = 128;
    entry.data_slot = GEOMETRY.data_slot_count;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.data_slot = 0;
    entry.lba = GEOMETRY.total_blocks - 127;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);

    entry = {};
    entry.opcode = static_cast<uint8_t>(BlkOpcode::FLUSH);
    entry.lba = std::numeric_limits<uint64_t>::max();
    entry.block_count = std::numeric_limits<uint32_t>::max();
    EXPECT_TRUE(blk_validate_sq_entry(entry, GEOMETRY).valid);

    entry = {};
    entry.opcode = static_cast<uint8_t>(BlkOpcode::BULK_WRITE);
    entry.lba = GEOMETRY.total_blocks - (BLK_RING_MAX_BULK_TRANSFER / GEOMETRY.block_size);
    entry.block_count = BLK_RING_MAX_BULK_TRANSFER / GEOMETRY.block_size;
    entry.data_slot = 1;
    result = blk_validate_sq_entry(entry, GEOMETRY);
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.bytes, BLK_RING_MAX_BULK_TRANSFER);

    entry.block_count++;
    entry.lba--;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.block_count = 0;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.block_count = 1;
    entry.data_slot = 0;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.opcode = 0xFF;
    entry.data_slot = 1;
    EXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
}

}  // namespace
