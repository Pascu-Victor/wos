#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/pci_aperture.hpp>
#include <dev/pci_ecam.hpp>
#include <platform/acpi/bounded_tables.hpp>
#include <span>

namespace ker::dev::pci::aperture {
constexpr size_t PLATFORM_CAPACITY = 1024;
struct Platform {
    std::array<Span, PLATFORM_CAPACITY> spans{};
    size_t count{};
    uint8_t physical_bits{};
    ecam::Table ecam{};
    bool valid{};
    [[nodiscard]] auto add(uint64_t start, uint64_t bytes) -> bool {
        if (count > spans.size()) return false;
        auto const RANGE = rounded(start, bytes);
        if (!RANGE || physical_bits < 32 || physical_bits > 52 || RANGE->end() > (uint64_t{1} << physical_bits)) return false;
        for (size_t i = 0; i < count; ++i)
            if (contains(spans[i], *RANGE)) return true;
        if (count == spans.size()) return false;
        spans[count++] = *RANGE;
        return true;
    }
    [[nodiscard]] auto excludes(Span span) const -> bool {
        if (!valid || count > spans.size() || span.bytes == 0 || span.bytes > UINT64_MAX - span.start) return true;
        for (size_t i = 0; i < count; ++i)
            if (overlaps(spans[i], span)) return true;
        return false;
    }
};

namespace platform_detail {
namespace acpi = ker::mod::acpi::bounded;
[[nodiscard]] inline auto table(std::span<const uint8_t> bytes, const char* signature, size_t minimum, size_t maximum) -> bool {
    return bytes.size() >= minimum && bytes.size() <= maximum && acpi::detail::signature(bytes, signature) &&
           acpi::detail::read(bytes, 4, 4) == bytes.size() && acpi::detail::checksum(bytes);
}
[[nodiscard]] inline auto madt(Platform& output, std::span<const uint8_t> bytes, uint64_t actual_apic) -> bool {
    if (!table(bytes, "APIC", 44, 16384) || !output.add(actual_apic, 4096)) return false;
    uint64_t const BASE = acpi::detail::read(bytes, 36, 4);
    if (BASE == 0 || (BASE & 4095U) != 0 || !output.add(BASE, 4096)) return false;
    size_t offset = 44, count = 0;
    bool override_seen = false;
    while (offset < bytes.size()) {
        if (bytes.size() - offset < 2 || ++count > 512) return false;
        size_t const LENGTH = bytes[offset + 1];
        uint8_t const TYPE = bytes[offset];
        if (LENGTH < 2 || LENGTH > bytes.size() - offset) return false;
        if (TYPE == 1) {
            if (LENGTH != 12 || bytes[offset + 3] != 0) return false;
            uint64_t const ADDRESS = acpi::detail::read(bytes, offset + 4, 4);
            if (ADDRESS == 0 || (ADDRESS & 4095U) != 0 || !output.add(ADDRESS, 4096)) return false;
        } else if (TYPE == 5) {
            if (LENGTH != 12 || override_seen || acpi::detail::read(bytes, offset + 2, 2) != 0) return false;
            uint64_t const ADDRESS = acpi::detail::read(bytes, offset + 4, 8);
            if (ADDRESS == 0 || (ADDRESS & 4095U) != 0 || !output.add(ADDRESS, 4096)) return false;
            override_seen = true;
        } else {
            size_t expected = 0;
            switch (TYPE) {
                case 0:
                    expected = 8;
                    break;
                case 2:
                    expected = 10;
                    break;
                case 3:
                    expected = 8;
                    break;
                case 4:
                    expected = 6;
                    break;
                case 9:
                    expected = 16;
                    break;
                case 10:
                    expected = 12;
                    break;
                default:
                    return false;  // Unknown controller/routing formats.
            }
            if (LENGTH != expected) return false;
        }
        offset += LENGTH;
    }
    return offset == bytes.size();
}

[[nodiscard]] inline auto gas(Platform& output, std::span<const uint8_t> bytes, size_t offset, size_t minimum_bytes = 0) -> bool {
    if (offset > bytes.size() || bytes.size() - offset < 12) return false;
    uint8_t const SPACE = bytes[offset], WIDTH = bytes[offset + 1], BIT_OFFSET = bytes[offset + 2], ACCESS = bytes[offset + 3];
    uint64_t const ADDRESS = acpi::detail::read(bytes, offset + 4, 8);
    if (ADDRESS == 0) return SPACE <= 1 && WIDTH == 0 && BIT_OFFSET == 0 && ACCESS == 0;
    if (SPACE > 1 || WIDTH == 0 || WIDTH > 64 || BIT_OFFSET != 0 || ACCESS > 4) return false;
    uint64_t size = (WIDTH + 7U) / 8U;
    if (ACCESS != 0 && size < (uint64_t{1} << (ACCESS - 1))) size = uint64_t{1} << (ACCESS - 1);
    if (size < minimum_bytes) size = minimum_bytes;
    if (SPACE == 1) return ADDRESS < 65536 && size <= 65536 - ADDRESS;
    return output.add(ADDRESS, size);
}
// A GPE block contains byte-accessed status/enable arrays, not one scalar.
// ACPI6.5 sections4.8.4.1/5.2.9 use the FADT block length for its extent.
[[nodiscard]] inline auto gpe_block(Platform& output, std::span<const uint8_t> bytes, size_t offset, size_t length) -> bool {
    if (offset > bytes.size() || bytes.size() - offset < 12 || length % 2 != 0) return false;
    uint8_t const SPACE = bytes[offset], WIDTH = bytes[offset + 1], BIT_OFFSET = bytes[offset + 2], ACCESS = bytes[offset + 3];
    uint64_t const ADDRESS = acpi::detail::read(bytes, offset + 4, 8);
    if (ADDRESS == 0) return gas(output, bytes, offset);  // Preserve strict absent-GAS validation.
    if (length == 0 || SPACE > 1 || WIDTH != length * 8 || BIT_OFFSET != 0 || ACCESS > 1 || (ADDRESS & 3U) != 0) return false;
    if (SPACE == 1) return ADDRESS < 65536 && length <= 65536 - ADDRESS;
    return output.add(ADDRESS, length);
}

[[nodiscard]] inline auto hpet(Platform& output, std::span<const uint8_t> bytes) -> bool {
    if (!table(bytes, "HPET", 56, 56) || bytes[40] != 0 || (bytes[41] != 0 && bytes[41] != 64) || bytes[42] != 0 || bytes[43] != 0)
        return false;
    uint64_t const BASE = acpi::detail::read(bytes, 44, 8);
    // HPET1.0a section3.2.4 describes a fixed register bank; its fourth GAS
    // byte is reserved. QEMU11.1.3 build_hpet emits width0 for this bank.
    return BASE != 0 && (BASE & 4095U) == 0 && output.add(BASE, 4096);
}
[[nodiscard]] inline auto fadt(Platform& output, std::span<const uint8_t> bytes) -> bool {
    if (!table(bytes, "FACP", 244, 276) || bytes[8] < 3 || bytes[8] > 6) return false;
    // q35-compatible non-reduced ACPI; all eight legacy blocks are I/O ports.
    if ((acpi::detail::read(bytes, 112, 4) & (uint64_t{1} << 20)) != 0) return false;
    for (size_t i = 0; i < 8; ++i) {
        uint64_t const BASE = acpi::detail::read(bytes, 56 + 4 * i, 4);
        size_t const LENGTH_INDEX = i < 2 ? 88 : i < 4 ? 89 : i == 4 ? 90 : i == 5 ? 91 : i == 6 ? 92 : 93;
        uint64_t const LENGTH = bytes[LENGTH_INDEX];
        if (BASE != 0 && (LENGTH == 0 || BASE >= 65536 || LENGTH > 65536 - BASE)) return false;
        if (i >= 6) {
            if (!gpe_block(output, bytes, 148 + 12 * i, LENGTH)) return false;
        } else if (!gas(output, bytes, 148 + 12 * i, BASE == 0 ? 0 : LENGTH)) {
            return false;
        }
    }
    if (!gas(output, bytes, 116)) return false;
    if (bytes.size() > 244 && bytes.size() < 268) return false;
    if (bytes.size() >= 268 && (!gas(output, bytes, 244) || !gas(output, bytes, 256))) return false;
    return true;
}

[[nodiscard]] inline auto register_window(Platform& output, uint64_t raw, uint64_t address_mask, uint64_t size) -> bool {
    if ((raw & ~(address_mask | uint64_t{1})) != 0) return false;
    if ((raw & 1U) == 0) return true;  // Final PCI write fence keeps it disabled.
    uint64_t const BASE = raw & address_mask;
    return BASE != 0 && output.add(BASE, size);
}

// Intel316966-002 D0:F0 register geometry, plus QEMU11.1.3's explicitly
// profiled extended TSEG/SMBASE behavior. No MMIO dereference is performed.
[[nodiscard]] inline auto q35(Platform& output, const Image& host, const Image& lpc) -> bool {
    if (host.read(0, 4) != 0x29c08086U || lpc.read(0, 4) != 0x29188086U) return false;
    if (!output.add(0, 0x100000) || !output.add(0xfec00000, 0x1400000)) return false;
    if (!register_window(output, host.read64(0x40), 0x0000000ffffff000ULL, 4096) ||
        !register_window(output, host.read64(0x48), 0x0000000fffffc000ULL, 16384) ||
        !register_window(output, host.read64(0x68), 0x0000000ffffff000ULL, 4096) ||
        !register_window(output, lpc.read(0xf0, 4), 0xffffc000ULL, 16384))
        return false;
    uint64_t const PCIEXBAR = host.read64(0x60);
    unsigned const LENGTH = static_cast<unsigned>((PCIEXBAR >> 1) & 3U);
    if (LENGTH == 3 || (PCIEXBAR & 1U) == 0) return false;
    uint64_t const BYTES = uint64_t{1} << (28U - LENGTH);
    uint64_t const MASK = 0x0000000ffc000000ULL & ~(BYTES - 1);
    if ((PCIEXBAR & ~(MASK | uint64_t{7})) != 0) return false;
    uint64_t const BASE = PCIEXBAR & MASK;
    if (BASE == 0 || output.ecam.count != 1 || output.ecam.windows[0].segment != 0 || output.ecam.windows[0].start() != BASE ||
        output.ecam.windows[0].end() != BASE + BYTES)
        return false;
    // The initial profile has no integrated-graphics stolen allocation. These
    // registers are not conventional BARs; nonzero GMS/GGMS needs a separate
    // fully observable stolen-memory profile, even when no IGD is enumerated.
    uint32_t const GRAPHICS = host.read(0x52, 2);
    if ((GRAPHICS & ~uint32_t{2}) != 0 || host.read(0xa4, 4) != 0 || host.read(0xa8, 4) != 0) return false;
    if ((host.read(0x9c, 1) & ~uint32_t{3}) != 0) return false;  // QEMU SMBASE/query state.
    for (size_t i = 0; i < 7; ++i)
        if ((host.read(0x90 + i, 1) & ~(i == 0 ? uint32_t{0x30} : uint32_t{0x33})) != 0) return false;
    uint64_t const TOM = host.read(0xa0, 2);
    uint64_t const UPPER = uint64_t{host.read(0xa2, 2)} << 20;
    uint64_t const LOW_RAW = host.read(0xb0, 2);
    if ((TOM & ~uint64_t{0x3ff}) != 0 || (LOW_RAW & 15U) != 0) return false;
    uint64_t const LOW_END = LOW_RAW << 16;
    if (LOW_END != 0 && !output.add(0, LOW_END)) return false;
    if (UPPER > (uint64_t{1} << 32) && !output.add(uint64_t{1} << 32, UPPER - (uint64_t{1} << 32))) return false;
    uint64_t const REMAP_BASE = host.read(0x98, 2), REMAP_LIMIT = host.read(0x9a, 2);
    if ((REMAP_BASE & ~uint64_t{0x3ff}) != 0 || (REMAP_LIMIT & ~uint64_t{0x3ff}) != 0) return false;
    if (REMAP_BASE <= REMAP_LIMIT && !output.add(REMAP_BASE << 26, (REMAP_LIMIT - REMAP_BASE + 1) << 26)) return false;
    uint8_t const SMRAM = static_cast<uint8_t>(host.read(0x9d, 1));
    uint8_t const EXT = static_cast<uint8_t>(host.read(0x9e, 1));
    // QEMU's SMM-absent variant exposes the entire SMRAM pair as zero.
    // This exact observation adds no hidden-range inference; the sealed PCI
    // configuration fence continues to forbid enabling any chipset decoder.
    if (SMRAM == 0 && EXT == 0) return true;
    if ((SMRAM & 7U) != 2 || (SMRAM & 0x80U) != 0) return false;
    if ((SMRAM & 8U) != 0 && (EXT & 1U) != 0) {
        unsigned const CODE = (EXT >> 1) & 3U;
        uint64_t const SIZE = CODE == 3 ? uint64_t{host.read(0x50, 2)} << 20 : (CODE == 0 ? 1ULL : CODE == 1 ? 2ULL : 8ULL) << 20;
        uint64_t const BASE_TSEG = host.read(0xac, 4);
        // QEMU can expose an opaque active TSEG with no TSEGMB readback. Do
        // not infer its location from RAM totals or pretend zero is absence.
        if (SIZE == 0 || SIZE > (uint64_t{4095} << 20) || BASE_TSEG == 0 || (BASE_TSEG & 0xfffffU) != 0 ||
            BASE_TSEG >= (uint64_t{1} << 32) || SIZE > (uint64_t{1} << 32) - BASE_TSEG || !output.add(BASE_TSEG, SIZE))
            return false;
    }
    return true;
}
}  // namespace platform_detail

// Actual native implementation owns the firmware/config source; pure helpers
// above also operate on bounded hostile Host fixtures, never adoption proofs.
void collect_platform(Platform& output, const Image& host, const Image& lpc);
}  // namespace ker::dev::pci::aperture
