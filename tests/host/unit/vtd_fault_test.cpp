#include <gtest/gtest.h>

#include <dev/iommu/fault.hpp>
#include <vector>

namespace vtd = ker::dev::iommu::vtd;
namespace {
struct Hardware {
    std::array<std::array<uint64_t, 2>, 256> records{};
    std::vector<uint32_t> writes;
    uint32_t status = 0;
    uint32_t unstable = UINT32_MAX;
    size_t reads = 0;
    auto read32(uint32_t offset) -> uint32_t {
        EXPECT_EQ(offset, vtd::FSTS);
        bool any = false;
        for (auto const& r : records) any |= (r[1] & vtd::FAULT_VALID) != 0;
        return any ? status | 2U : status & ~2U;
    }
    auto read64(uint32_t offset) -> uint64_t {
        ++reads;
        size_t index = (offset - 0x100) / 16;
        auto& value = records.at(index).at((offset & 8U) != 0);
        if (offset == unstable) value ^= 1;
        return value;
    }
    void write64(uint32_t offset, uint64_t value) {
        EXPECT_EQ(value, vtd::FAULT_VALID);
        EXPECT_EQ(offset & 15U, 8U);
        writes.push_back(offset);
        records.at((offset - 0x100) / 16)[1] &= ~value;
    }
    void write32(uint32_t offset, uint32_t value) {
        EXPECT_EQ(offset, vtd::FSTS);
        EXPECT_EQ(value, 1U);
        for (auto const& r : records) EXPECT_EQ(r[1] & vtd::FAULT_VALID, 0U);
        writes.push_back(offset);
        status &= ~value;
    }
};
}  // namespace

TEST(VtdFault, CapturesFifoRawIdentityAccessAndClearsOnlyValidBits) {
    Hardware h;
    h.status = (2U << 8U) | 2;
    h.records[2] = {0x12345000, vtd::FAULT_VALID | (uint64_t{1} << 62) | (uint64_t{6} << 32) | 0x1234};
    h.records[0] = {0xabcdef000, vtd::FAULT_VALID | (uint64_t{5} << 32) | 0x5678};
    vtd::FaultReport r;
    vtd::capture_faults(h, 0x100, 3, h.status, r);
    ASSERT_EQ(r.count, 2U);
    EXPECT_EQ(r.records[0].index, 2);
    EXPECT_EQ(r.records[0].source(), 0x1234);
    EXPECT_EQ(r.records[0].address(), 0x12345000U);
    EXPECT_EQ(r.records[0].reason(), 6);
    EXPECT_TRUE(r.records[0].read());
    EXPECT_FALSE(r.records[1].read());
    EXPECT_TRUE(r.records[0].stable);
    EXPECT_EQ(r.remaining & 3U, 0U);
    EXPECT_EQ(h.writes, (std::vector<uint32_t>{0x128, 0x108}));
}
TEST(VtdFault, OverflowScansHolesAndAcknowledgesOnlyAfterAllRecordsAreCleared) {
    Hardware h;
    h.status = (250U << 8U) | 3;
    for (size_t i : {size_t{0}, size_t{150}, size_t{255}}) h.records[i] = {i * 4096, vtd::FAULT_VALID | i};
    vtd::FaultReport r;
    vtd::capture_faults(h, 0x100, 256, h.status, r);
    EXPECT_EQ(r.count, 3U);
    EXPECT_EQ(r.records[0].index, 255);
    EXPECT_EQ(r.remaining & 3U, 0U);
    ASSERT_EQ(h.writes.size(), 4U);
    EXPECT_EQ(h.writes.back(), vtd::FSTS);
    EXPECT_LE(h.reads, 3U * 256U);
}
TEST(VtdFault, UnstableRecordRetainsEvidenceAndNeverClearsOverflow) {
    Hardware h;
    h.status = 3;
    h.records[0] = {0x1000, vtd::FAULT_VALID};
    h.unstable = 0x108;
    vtd::FaultReport r;
    vtd::capture_faults(h, 0x100, 1, h.status, r);
    ASSERT_EQ(r.count, 1U);
    EXPECT_FALSE(r.records[0].stable);
    EXPECT_TRUE(r.invalid);
    EXPECT_EQ(r.remaining & 3U, 3U);
    EXPECT_TRUE(h.writes.empty());
}
TEST(VtdFault, InvalidBoundsAndErrorOnlyStatusDoNotInventRecordsOrRecovery) {
    for (uint16_t count : {uint16_t{0}, uint16_t{257}}) {
        Hardware h;
        vtd::FaultReport r;
        vtd::capture_faults(h, 0x100, count, 0x73, r);
        EXPECT_TRUE(r.invalid);
        EXPECT_EQ(h.reads, 0U);
        EXPECT_TRUE(h.writes.empty());
    }
    Hardware h;
    h.status = 0x70;
    vtd::FaultReport r;
    vtd::capture_faults(h, 0x100, 1, h.status, r);
    EXPECT_EQ(r.count, 0U);
    EXPECT_EQ(r.remaining, 0x70U);
    EXPECT_TRUE(h.writes.empty());
    h.status = (3U << 8) | 3;
    h.records[0] = {0x1000, vtd::FAULT_VALID};
    vtd::FaultReport invalid_index;
    vtd::capture_faults(h, 0x100, 1, h.status, invalid_index);
    EXPECT_TRUE(invalid_index.invalid);
    EXPECT_EQ(invalid_index.count, 1U);
    EXPECT_EQ(invalid_index.remaining & 1U, 1U);
}

TEST(VtdFault, RejectsBadRegisterExtentsAndDecodesAllRequestTypesWithoutUsingUndefinedFri) {
    for (uint32_t offset : {0U, 0xe0U, 0x101U, 0xfffffff0U}) {
        Hardware h;
        vtd::FaultReport r;
        vtd::capture_faults(h, offset, 1, 2, r);
        EXPECT_TRUE(r.invalid);
        EXPECT_EQ(h.reads, 0U);
        EXPECT_TRUE(h.writes.empty());
    }
    Hardware h;
    h.status = 0xff70;  // FRI is undefined without PPF; don't diagnose it as corrupt.
    vtd::FaultReport r;
    vtd::capture_faults(h, 0x100, 1, h.status, r);
    EXPECT_FALSE(r.invalid);
    EXPECT_EQ(r.count, 0U);
    vtd::FaultRecord record;
    EXPECT_STREQ(record.type(), "write");
    record.high = uint64_t{1} << 28;
    EXPECT_STREQ(record.type(), "page-request");
    record.high |= uint64_t{1} << 62;
    EXPECT_STREQ(record.type(), "atomic");
    record.high &= ~(uint64_t{1} << 28);
    EXPECT_STREQ(record.type(), "read");
}

TEST(VtdFault, MaximumHardwareRecordCountFitsRetainedStorageAndWrapsExactlyOnce) {
    Hardware h;
    h.status = 0xff03;
    for (size_t i = 0; i < h.records.size(); ++i) h.records[i] = {i * 4096, vtd::FAULT_VALID | i};
    vtd::FaultReport r;
    vtd::capture_faults(h, 0x100, 256, h.status, r);
    ASSERT_EQ(r.count, 256U);
    EXPECT_EQ(r.records.front().index, 255);
    EXPECT_EQ(r.records.back().index, 254);
    EXPECT_EQ(h.reads, 768U);
    EXPECT_EQ(h.writes.size(), 257U);
    EXPECT_EQ(r.remaining & 3U, 0U);
    EXPECT_FALSE(r.invalid);
}
