#pragma once

#include <cstdint>
#include <optional>

namespace ker::dev::iommu::vtd {

// Legacy translation formats and register invalidation, Intel VT-d rev. 4.0,
// sections 9.1, 9.3, 9.8 and 11.4.6. No packed compiler bitfields or MMIO here.
constexpr uint64_t PAGE_SIZE = 4096;
constexpr uint64_t BUSY = uint64_t{1} << 63U;
constexpr uint64_t GLOBAL_CONTEXT_COMMAND = BUSY | (uint64_t{1} << 61U);
constexpr uint64_t GLOBAL_IOTLB_COMMAND = BUSY | (uint64_t{1} << 60U) | (uint64_t{3} << 48U);

struct Entry {
    uint64_t low = 0;
    uint64_t high = 0;
};
static_assert(sizeof(Entry) == 16);

[[nodiscard]] constexpr auto page_address_valid(uint64_t address, uint8_t physical_bits) -> bool {
    // Second-stage entries have at most 52 physical address bits. Require a
    // complete page inside the supported width, never truncate high bits.
    return physical_bits >= 12 && physical_bits <= 52 && (address & (PAGE_SIZE - 1)) == 0 &&
           address <= (uint64_t{1} << physical_bits) - PAGE_SIZE;
}

[[nodiscard]] constexpr auto root_entry(uint64_t context_table, uint8_t physical_bits) -> std::optional<Entry> {
    if (!page_address_valid(context_table, physical_bits)) {
        return std::nullopt;
    }
    return Entry{context_table | 1U, 0};
}

[[nodiscard]] constexpr auto context_entry(uint64_t page_table, uint8_t physical_bits, uint16_t domain, uint8_t levels,
                                           uint8_t supported_agaw, uint32_t domain_count) -> std::optional<Entry> {
    if (!page_address_valid(page_table, physical_bits) || levels < 3 || levels > 5 || domain_count == 0 || domain_count > 65536 ||
        domain >= domain_count) {
        return std::nullopt;
    }
    auto const AW = static_cast<uint8_t>(levels - 2U);
    if ((supported_agaw & (1U << AW)) == 0) {
        return std::nullopt;
    }
    // TT=00 translates untranslated requests and rejects ATS. FPD=0 keeps
    // fault reporting enabled. The domain ID occupies context bits 87:72.
    return Entry{page_table | 1U, (uint64_t{domain} << 8U) | AW};
}

enum class Access : uint8_t { read = 1, write = 2, read_write = 3 };

[[nodiscard]] constexpr auto passthrough_entry(uint16_t domain, uint8_t supported_agaw, uint32_t domain_count, bool supported)
    -> std::optional<Entry> {
    if (!supported || domain == 0 || domain_count == 0 || domain_count > 65536 || domain >= domain_count || (supported_agaw & ~14U) != 0)
        return std::nullopt;
    // TT=10 requires the largest supported AGAW, even though SSPTPTR is ignored.
    uint8_t const AW = (supported_agaw & 8U) != 0 ? 3 : (supported_agaw & 4U) != 0 ? 2 : (supported_agaw & 2U) != 0 ? 1 : 0;
    if (AW == 0) return std::nullopt;
    return Entry{9, (uint64_t{domain} << 8U) | AW};
}

[[nodiscard]] constexpr auto page_entry(uint64_t physical, uint8_t physical_bits, Access access) -> std::optional<uint64_t> {
    auto const BITS = static_cast<uint8_t>(access);
    if (!page_address_valid(physical, physical_bits) || BITS == 0 || (BITS & ~3U) != 0) {
        return std::nullopt;
    }
    return physical | BITS;
}

[[nodiscard]] constexpr auto table_entry(uint64_t physical, uint8_t physical_bits) -> std::optional<uint64_t> {
    return page_entry(physical, physical_bits, Access::read_write);
}

[[nodiscard]] constexpr auto page_index(uint64_t address, uint8_t level) -> std::optional<uint16_t> {
    if (level < 1 || level > 5) {
        return std::nullopt;
    }
    return static_cast<uint16_t>((address >> (12U + 9U * (level - 1U))) & 511U);
}

enum class InvalidationPhase : uint8_t { idle, context, need_iotlb, iotlb, complete, quarantined };
enum class Observation : uint8_t { stale, busy, advance, failed };

// One instance per hardware unit, protected by the same lock as its command
// registers. Caller first drains the device/IRQ users and publishes PTE writes
// with the required cache synchronization. This model cannot prove those facts.
// Register invalidation requires supported legacy mode, no outstanding queued
// invalidation, and read/write draining support. Unsupported units must refuse
// this backend. No new command may be issued until its predecessor completes.
class Invalidation {
   public:
    [[nodiscard]] auto phase() const -> InvalidationPhase { return phase_; }

    // Begin before writing GLOBAL_CONTEXT_COMMAND. Zero means no admission.
    [[nodiscard]] auto begin() -> uint64_t {
        if ((phase_ != InvalidationPhase::idle && phase_ != InvalidationPhase::complete) || sequence_ == UINT64_MAX) {
            return 0;
        }
        ++sequence_;
        phase_ = InvalidationPhase::context;
        return sequence_;
    }

    [[nodiscard]] auto observe_context(uint64_t ticket, uint64_t value) -> Observation {
        if (ticket == 0 || ticket != sequence_ || phase_ != InvalidationPhase::context) {
            return Observation::stale;
        }
        if ((value & BUSY) != 0) {
            return Observation::busy;
        }
        // A global request requires global actual granularity (CAIG=01).
        if (((value >> 59U) & 3U) != 1U) {
            phase_ = InvalidationPhase::quarantined;
            return Observation::failed;
        }
        phase_ = InvalidationPhase::need_iotlb;
        return Observation::advance;
    }

    // Call immediately before writing GLOBAL_IOTLB_COMMAND under the unit lock.
    [[nodiscard]] auto issue_iotlb(uint64_t ticket) -> bool {
        if (ticket == 0 || ticket != sequence_ || phase_ != InvalidationPhase::need_iotlb) {
            return false;
        }
        phase_ = InvalidationPhase::iotlb;
        return true;
    }

    [[nodiscard]] auto observe_iotlb(uint64_t ticket, uint64_t value) -> Observation {
        if (ticket == 0 || ticket != sequence_ || phase_ != InvalidationPhase::iotlb) {
            return Observation::stale;
        }
        if ((value & BUSY) != 0) {
            return Observation::busy;
        }
        // IVT clear alone is not success: IAIG=00 reports a rejected command.
        if (((value >> 57U) & 3U) != 1U) {
            phase_ = InvalidationPhase::quarantined;
            return Observation::failed;
        }
        phase_ = InvalidationPhase::complete;
        return Observation::advance;
    }

    [[nodiscard]] auto timeout(uint64_t ticket) -> bool {
        if (ticket == 0 || ticket != sequence_ || phase_ == InvalidationPhase::idle || phase_ == InvalidationPhase::complete ||
            phase_ == InvalidationPhase::quarantined) {
            return false;
        }
        phase_ = InvalidationPhase::quarantined;
        return true;
    }

    // This only establishes invalidation completion; device completion and
    // draining remain independent lifetime conditions in the DMA owner.
    [[nodiscard]] auto completed(uint64_t ticket) const -> bool {
        return ticket != 0 && ticket == sequence_ && phase_ == InvalidationPhase::complete;
    }

   private:
    uint64_t sequence_ = 0;
    InvalidationPhase phase_ = InvalidationPhase::idle;
};

}  // namespace ker::dev::iommu::vtd
