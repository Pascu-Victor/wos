#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::dev::iommu::vtd {
constexpr uint32_t FSTS = 0x34, FECTL = 0x38, FEDATA = 0x3c, FEADDR = 0x40, FEUADDR = 0x44;
constexpr uint32_t FAULT_STATUS = 0x73;
constexpr uint32_t FAULT_MASK = 1U << 31U;
constexpr uint64_t FAULT_VALID = uint64_t{1} << 63U;
constexpr size_t MAX_FAULT_RECORDS = 256;
struct FaultRecord {
    uint64_t low = 0, high = 0;
    uint64_t domain_generation = 0;
    uint16_t domain = 0;
    uint16_t index = 0;
    bool identified = false;
    bool stable = false;
    auto address() const -> uint64_t { return low & ~uint64_t{4095}; }
    auto source() const -> uint16_t { return static_cast<uint16_t>(high); }
    auto reason() const -> uint8_t { return static_cast<uint8_t>(high >> 32U); }
    auto read() const -> bool { return (high & (uint64_t{1} << 62U)) != 0; }
    auto type() const -> const char* {
        bool const SECOND = (high & (uint64_t{1} << 28U)) != 0;
        return read() ? (SECOND ? "atomic" : "read") : (SECOND ? "page-request" : "write");
    }
};
struct FaultReport {
    uint32_t status = 0, remaining = 0;
    size_t count = 0;
    bool invalid = false;
    std::array<FaultRecord, MAX_FAULT_RECORDS> records{};
};

// Intel VT-d 7.2/11.4.7 primary fault logging. Caller masks delivery, closes
// admission and exclusively owns initially empty retained output. Bounded, allocation-free;
// no waits, logging or recovery. An unstable record is retained, never cleared.
// Overflow requires scanning ALL records before its RW1C acknowledgement.
template <typename Backend>
void capture_faults(Backend& io, uint32_t offset, uint16_t count, uint32_t status, FaultReport& out) {
    out.status = status;
    if (count == 0 || count > MAX_FAULT_RECORDS || offset < 0xf0 || (offset & 15U) != 0 || offset > UINT32_MAX - uint32_t{count} * 16U) {
        out.invalid = true;
        return;
    }
    auto first = static_cast<uint16_t>((status & 2U) != 0 ? (status >> 8U) & 255U : 0);
    if (first >= count) {
        out.invalid = true;
        first = 0;
    }
    for (size_t n = 0; n < count; ++n) {
        auto const INDEX = static_cast<uint16_t>((first + n) % count);
        uint32_t const AT = offset + uint32_t{INDEX} * 16U;
        uint64_t const HIGH = io.read64(AT + 8U);
        if ((HIGH & FAULT_VALID) == 0) continue;
        auto& record = out.records.at(out.count++);
        record.index = INDEX;
        record.high = HIGH;
        record.low = io.read64(AT);
        record.stable = io.read64(AT + 8U) == HIGH;
        if (record.stable)
            io.write64(AT + 8U, FAULT_VALID);
        else
            out.invalid = true;
    }
    out.remaining = io.read32(FSTS);
    if ((status & 1U) != 0 && (out.remaining & 2U) == 0 && !out.invalid) {
        io.write32(FSTS, 1U);
        out.remaining = io.read32(FSTS);
    }
    // IQE/ICE/ITE remain visible on the permanently closed unit. Clearing them
    // would not establish invalidation completion or safe queued-mode recovery.
}
}  // namespace ker::dev::iommu::vtd
