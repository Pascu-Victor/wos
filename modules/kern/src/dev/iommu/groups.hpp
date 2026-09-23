#pragma once

#include <array>
#include <dev/iommu/scopes.hpp>
#include <dev/pci_ecam.hpp>
#include <span>

namespace ker::dev::iommu::groups {
constexpr size_t MAX_REQUESTERS = scopes::MAX_DEVICES * 2;
constexpr uint16_t NONE = scopes::NONE;
enum class Error : uint8_t { none, incomplete, topology, capability, requester_mode, unit, limit, isolation_boundary, shared_domain };
enum Reason : uint8_t { bridge_alias = 1, acs_missing = 2, multifunction = 4 };
struct Evidence {
    bool readable = false;
    pci::ecam::Capabilities capability{};
};
struct Requester {
    uint16_t sid = 0;
    uint16_t owner = NONE;  // Canonical device index; never a DID.
    uint16_t device = NONE;
    bool alias = false;  // Source may represent another member, even if BDF exists.
};
struct Result {
    Error error = Error::incomplete;
    uint16_t error_device = NONE;
    size_t devices = 0, count = 0, requester_count = 0;
    std::array<uint16_t, scopes::MAX_DEVICES> owners{};
    std::array<uint8_t, scopes::MAX_DEVICES> reasons{};
    std::array<Requester, MAX_REQUESTERS> requesters{};
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};
// Conservative: only explicitly implemented AND enabled controls establish
// this boundary. Never infer isolation from capability presence alone.
inline auto acs_boundary(const pci::ecam::Capabilities& cap) -> bool {
    constexpr uint16_t REQUIRED = 0x1f;  // Source validation, translation block, RR, CR, UF.
    return cap.acs != 0 && (cap.acs_capability & REQUIRED) == REQUIRED && (cap.acs_control & REQUIRED) == REQUIRED &&
           (cap.acs_control & 0x40U) == 0;  // No direct translated peer-to-peer.
}
inline auto source(const scopes::Device& device) -> uint16_t {
    return static_cast<uint16_t>((uint16_t{device.bus} << 8U) | (device.slot << 3U) | device.function);
}

// Scope ownership and configuration evidence must refer to this exact immutable
// registry. PCIe aliases not implemented here are refused, never guessed.
inline auto qualify(std::span<const scopes::Device> devices, std::span<const Evidence> evidence, const scopes::Result& ownership,
                    bool strict = false) -> Result {
    auto fail = [](Error error, uint16_t device = NONE) -> Result { return {.error = error, .error_device = device}; };
    if (devices.size() > scopes::MAX_DEVICES) return fail(Error::limit);
    if (!ownership.valid() || ownership.device_count != devices.size() || evidence.size() != devices.size()) return fail(Error::incomplete);
    if (!scopes::detail::topology(devices)) return fail(Error::topology);
    Result result{.error = Error::none, .devices = devices.size()};
    for (size_t i = 0; i < devices.size(); ++i) result.owners[i] = static_cast<uint16_t>(i);
    auto root = [&](size_t i) {
        while (result.owners[i] != i) i = result.owners[i];
        return static_cast<uint16_t>(i);
    };
    auto join = [&](size_t a, size_t b) {
        auto const A = root(a), B = root(b);
        if (A < B)
            result.owners[B] = A;
        else
            result.owners[A] = B;
    };
    for (size_t i = 0; i < devices.size(); ++i) {
        auto const& node = devices[i];
        auto const& cap = evidence[i].capability;
        if (node.segment != 0 || ownership.devices[i].unit == NONE) return fail(Error::unit);
        if (!evidence[i].readable || !cap.valid()) return fail(Error::capability);
        if ((cap.device_control & (1U << 9U)) != 0 || (cap.device_control2 & (1U << 5U)) != 0 || (cap.ats_control & 0x8000U) != 0 ||
            (cap.pri_control & 1U) != 0 || (cap.pasid_control & 1U) != 0 || (cap.sriov_control & 1U) != 0)
            return fail(Error::requester_mode);
        if (cap.pcie != 0) {
            bool const BRIDGE = cap.pcie_type >= 4 && cap.pcie_type <= 8;
            bool const ENDPOINT = cap.pcie_type == 0 || cap.pcie_type == 1 || cap.pcie_type == 9 || cap.pcie_type == 10;
            if ((node.header == 1 && !BRIDGE) || (node.header == 0 && !ENDPOINT)) return fail(Error::capability);
        }
        if (strict) {
            // VT-d 3.12.4 requires unique requester IDs for root-complex
            // integrated PCI and PCIe functions. External links additionally
            // need real ACS boundaries; neither merging nor aliases qualify.
            bool const ROOT = node.parent == NONE;
            auto const PARENT_TYPE = ROOT ? uint8_t{255} : evidence[node.parent].capability.pcie_type;
            bool isolated = false;
            if (node.header == 1) {
                bool const PLACEMENT = (cap.pcie_type == 4 && ROOT) ||
                                       (cap.pcie_type == 5 && !ROOT && (PARENT_TYPE == 4 || PARENT_TYPE == 6)) ||
                                       (cap.pcie_type == 6 && !ROOT && PARENT_TYPE == 5);
                isolated = cap.pcie != 0 && PLACEMENT && acs_boundary(cap);
            } else if (ROOT) {
                isolated = cap.pcie == 0 || cap.pcie_type == 9 || cap.pcie_type == 10;
            } else {
                isolated = cap.pcie != 0 && (cap.pcie_type == 0 || cap.pcie_type == 1) && (PARENT_TYPE == 4 || PARENT_TYPE == 6);
            }
            if (!isolated) return fail(Error::isolation_boundary, static_cast<uint16_t>(i));
        }
        result.requesters[result.requester_count++] = {
            .sid = source(node), .owner = static_cast<uint16_t>(i), .device = static_cast<uint16_t>(i)};
        if (node.header == 1) {
            bool const ALIAS = cap.pcie == 0 || cap.pcie_type == 7 || cap.pcie_type == 8;
            if (ALIAS || !acs_boundary(cap)) {
                auto const REASON = static_cast<uint8_t>(ALIAS ? bridge_alias : acs_missing);
                for (size_t child = i; child < devices.size(); ++child)
                    if (scopes::detail::under(devices, child, i)) {
                        join(i, child);
                        result.reasons[child] |= REASON;
                    }
            }
            if (ALIAS && node.secondary != 0) {
                // VT-d 3.12.1: PCIe-to-PCI replaces RID with secondary:00.0.
                // 3.12.2: conventional bridges use their own requester ID.
                uint16_t const SID = cap.pcie != 0 && cap.pcie_type == 7 ? static_cast<uint16_t>(node.secondary << 8U) : source(node);
                if (result.requester_count == MAX_REQUESTERS) return fail(Error::limit);
                result.requesters[result.requester_count++] = {.sid = SID, .owner = static_cast<uint16_t>(i), .alias = true};
            }
        }
        for (size_t j = 0; j < i; ++j) {
            auto const& peer = devices[j];
            if (peer.bus != node.bus || peer.slot != node.slot) continue;
            // VT-d 3.12.4 distinguishes root-complex integrated requesters.
            // External multifunction enclosures require an ACS boundary.
            bool const INTEGRATED = node.parent == NONE && peer.parent == NONE && (cap.pcie == 0 || cap.pcie_type == 9) &&
                                    (evidence[j].capability.pcie == 0 || evidence[j].capability.pcie_type == 9);
            if (!INTEGRATED && (!acs_boundary(cap) || !acs_boundary(evidence[j].capability))) {
                join(i, j);
                result.reasons[i] |= multifunction;
                result.reasons[j] |= multifunction;
            }
        }
    }
    // An alias that collides with an enumerated BDF extends the same group.
    for (size_t i = 0; i < result.requester_count; ++i)
        for (size_t j = 0; j < i; ++j)
            if (result.requesters[i].sid == result.requesters[j].sid) join(result.requesters[i].owner, result.requesters[j].owner);
    for (size_t i = 0; i < devices.size(); ++i) {
        result.owners[i] = root(i);
        if (strict && result.owners[i] != i) return fail(Error::shared_domain, static_cast<uint16_t>(i));
        if (ownership.devices[i].unit != ownership.devices[result.owners[i]].unit) return fail(Error::unit);
        if (result.owners[i] == i) ++result.count;
    }
    size_t used = 0;
    for (size_t i = 0; i < result.requester_count; ++i) {
        auto entry = result.requesters[i];
        entry.owner = result.owners[entry.owner];
        size_t j = 0;
        for (; j < used; ++j)
            if (result.requesters[j].sid == entry.sid) break;
        if (j == used)
            result.requesters[used++] = entry;
        else {
            result.requesters[j].alias |= entry.alias;
            if (entry.device != NONE) result.requesters[j].device = entry.device;
        }
    }
    result.requester_count = used;
    return result;
}
}  // namespace ker::dev::iommu::groups
