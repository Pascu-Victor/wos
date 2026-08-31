#include <atomic>
#include <cstdint>
#include <net/wki/blk_ring.hpp>
#include <net/wki/dev_server.hpp>
#include <test/ktest.hpp>
#include <type_traits>
#include <utility>

namespace {

constexpr auto valid_block_ring_geometry() -> ker::net::wki::BlkRingGeometry {
    using namespace ker::net::wki;
    return {.sq_depth = BLK_RING_DEFAULT_SQ_DEPTH,
            .cq_depth = BLK_RING_DEFAULT_CQ_DEPTH,
            .data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS,
            .data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE,
            .block_size = 512,
            .total_blocks = 8192,
            .server_ready = 1};
}

}  // namespace

KTEST(WkiDevServerBinding, LifecycleFlagsAreAtomic) {
    using Binding = ker::net::wki::DevServerBinding;

    constexpr bool REFS_ATOMIC = std::is_same_v<decltype(std::declval<Binding&>().refs), std::atomic<uint32_t>>;
    constexpr bool RETIRING_ATOMIC = std::is_same_v<decltype(std::declval<Binding&>().retiring), std::atomic<bool>>;

    KEXPECT_TRUE(REFS_ATOMIC);
    KEXPECT_TRUE(RETIRING_ATOMIC);
}

KTEST(WkiDevServerVfs, DeferredRequestStorageIsFixedBoundedAndRecycled) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_deferred_vfs_storage_is_coallocated());
}

KTEST(WkiDevServerBinding, MovePreservesLifecycleFlags) { KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_binding_lifecycle_flags()); }

KTEST(WkiDevServerBinding, MoveTransfersBlockWriterLeaseExactlyOnce) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_block_writer_lease_transfer());
}

KTEST(WkiDevServerBinding, RetirementOwnershipAndWriterReservationPersistUntilErase) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_retirement_ownership_guards());
}

KTEST(WkiDevServerDetach, AdmissionIsExactIdempotentAndBlocksReplacement) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_detach_admission_lifecycle());
}

KTEST(WkiDevServerAttachAckFailure, DefersExactBlockVfsAndNetCleanupOutsideRx) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_attach_ack_failure_defers_cleanup());
}

KTEST(WkiDevServerBlockRx, UsesBoundedFixedAdmissionBeforeTaskContextIo) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_block_ops_use_fixed_admission());
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_block_attach_honors_disable_rdma());
}

KTEST(WkiDevServerBlockValidation, RejectsOverflowAndOutOfRangeBeforeIo) {
    using namespace ker::net::wki;

    BlkTransferValidation result = blk_validate_transfer(8191, 1, 512, 8192, 512);
    KEXPECT_TRUE(result.valid);
    KEXPECT_EQ(result.bytes, 512U);

    result = blk_validate_transfer(8192, 1, 512, 8192, 512);
    KEXPECT_FALSE(result.valid);
    result = blk_validate_transfer(0, 129, 512, 8192, BLK_RING_DEFAULT_DATA_SLOT_SIZE);
    KEXPECT_FALSE(result.valid);
    result = blk_validate_transfer(0, 2, UINT64_MAX, 8192, UINT64_MAX);
    KEXPECT_FALSE(result.valid);
}

KTEST(WkiDevServerBlockValidation, RejectsCorruptRingGeometryAndIndices) {
    using namespace ker::net::wki;

    BlkRingGeometry geometry = valid_block_ring_geometry();
    KEXPECT_TRUE(blk_ring_geometry_valid(geometry, 512, 8192));
    geometry.sq_depth = 0;
    KEXPECT_FALSE(blk_ring_geometry_valid(geometry, 512, 8192));

    geometry = valid_block_ring_geometry();
    BlkRingIndices indices = {};
    KEXPECT_TRUE(blk_ring_indices_valid(indices, geometry));
    indices.cq_tail = geometry.cq_depth;
    KEXPECT_FALSE(blk_ring_indices_valid(indices, geometry));
}

KTEST(WkiDevServerBlockValidation, BoundsStandardSlotsAndBulkStaging) {
    using namespace ker::net::wki;

    BlkRingGeometry const GEOMETRY = valid_block_ring_geometry();
    BlkSqEntry entry = {};
    entry.opcode = static_cast<uint8_t>(BlkOpcode::READ);
    entry.block_count = BLK_RING_DEFAULT_DATA_SLOT_SIZE / GEOMETRY.block_size;
    entry.data_slot = BLK_RING_DEFAULT_DATA_SLOTS - 1;
    KEXPECT_TRUE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.block_count = 0;
    KEXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.opcode = static_cast<uint8_t>(BlkOpcode::WRITE);
    KEXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.opcode = static_cast<uint8_t>(BlkOpcode::READ);
    entry.block_count = BLK_RING_DEFAULT_DATA_SLOT_SIZE / GEOMETRY.block_size;
    entry.block_count++;
    KEXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);

    entry = {};
    entry.opcode = static_cast<uint8_t>(BlkOpcode::BULK_READ);
    entry.block_count = BLK_RING_MAX_BULK_TRANSFER / GEOMETRY.block_size;
    entry.data_slot = 1;
    KEXPECT_TRUE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.block_count++;
    KEXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
    entry.block_count = 1;
    entry.data_slot = 0;
    KEXPECT_FALSE(blk_validate_sq_entry(entry, GEOMETRY).valid);
}
