#pragma once

#include <dev/iommu/vtd_model.hpp>

namespace ker::dev::iommu::vtd {
constexpr uint32_t GCMD = 0x18;
constexpr uint32_t GSTS = 0x1c;
constexpr uint32_t RTADDR = 0x20;
constexpr uint32_t CCMD = 0x28;
constexpr uint32_t TE = 1U << 31;
constexpr uint32_t SRTP = 1U << 30;
constexpr uint32_t WBF = 1U << 27;
constexpr uint32_t QIE = 1U << 26;
constexpr uint32_t IRE = 1U << 25;
constexpr uint32_t PERSISTENT_STATUS = 0x96ffffff;

enum class Activation : uint8_t { unprepared, rejected, root_pending, context_pending, iotlb_pending, enable_pending, active };

// Caller serializes the complete sequence with every other operation on this
// unit. Publication precedes this call; failed proof permanently closes the unit.
template <typename Backend>
[[nodiscard]] auto invalidate_live(Backend& io, Invalidation& operation, uint64_t root, uint32_t iotlb) -> bool {
    if (iotlb < 0xf8 || (iotlb & 15U) != 8U || io.read64(RTADDR) != root || (io.read32(GSTS) & (TE | QIE | IRE | WBF)) != TE ||
        (io.read64(CCMD) & BUSY) != 0 || (io.read64(iotlb) & BUSY) != 0)
        return false;
    auto const TICKET = operation.begin();
    if (TICKET == 0) return false;
    io.write64(CCMD, GLOBAL_CONTEXT_COMMAND);
    if (!io.wait64(CCMD, BUSY, 0)) {
        (void)operation.timeout(TICKET);
        return false;
    }
    if (operation.observe_context(TICKET, io.read64(CCMD)) != Observation::advance || !operation.issue_iotlb(TICKET)) return false;
    io.write64(iotlb, GLOBAL_IOTLB_COMMAND);
    if (!io.wait64(iotlb, BUSY, 0)) {
        (void)operation.timeout(TICKET);
        return false;
    }
    return operation.observe_iotlb(TICKET, io.read64(iotlb)) == Observation::advance && operation.completed(TICKET);
}

// Backend waits must have time and iteration bounds. Prepared root/context
// storage is already cache-synchronized, and PCI BME is disabled by the caller.
// Any result after rejected exposes storage: retain it until independently
// proven hardware retirement. No automatic rollback/free on command failure.
template <typename Backend>
[[nodiscard]] auto activate(Backend& io, uint64_t root, uint8_t physical_bits, uint32_t iotlb) -> Activation {
    if (!page_address_valid(root, physical_bits) || root == 0 || iotlb < 0xf8 || (iotlb & 15U) != 8U) return Activation::rejected;
    uint32_t const STATUS = io.read32(GSTS);
    if ((STATUS & (TE | QIE | IRE | WBF)) != 0 || (io.read64(CCMD) & BUSY) != 0 || (io.read64(iotlb) & BUSY) != 0)
        return Activation::rejected;
    io.write64(RTADDR, root);  // Legacy translation-table mode, TTM=00.
    io.write32(GCMD, (STATUS & PERSISTENT_STATUS) | SRTP);
    if (!io.wait32(GSTS, SRTP, SRTP)) return Activation::root_pending;
    Invalidation invalidation;
    auto const TICKET = invalidation.begin();
    io.write64(CCMD, GLOBAL_CONTEXT_COMMAND);
    if (!io.wait64(CCMD, BUSY, 0) || invalidation.observe_context(TICKET, io.read64(CCMD)) != Observation::advance)
        return Activation::context_pending;
    if (!invalidation.issue_iotlb(TICKET)) return Activation::context_pending;
    io.write64(iotlb, GLOBAL_IOTLB_COMMAND);
    if (!io.wait64(iotlb, BUSY, 0) || invalidation.observe_iotlb(TICKET, io.read64(iotlb)) != Observation::advance ||
        !invalidation.completed(TICKET))
        return Activation::iotlb_pending;
    io.write32(GCMD, (io.read32(GSTS) & PERSISTENT_STATUS) | TE);
    if (!io.wait32(GSTS, TE, TE)) return Activation::enable_pending;
    return Activation::active;
}
}  // namespace ker::dev::iommu::vtd
