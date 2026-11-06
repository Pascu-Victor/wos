#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/pci_resource.hpp>
#include <optional>

namespace ker::dev::pci::aperture {
struct Span {
    uint64_t start{};
    uint64_t bytes{};
    [[nodiscard]] constexpr auto end() const -> uint64_t { return start + bytes; }
};

[[nodiscard]] constexpr auto rounded(uint64_t start, uint64_t bytes) -> std::optional<Span> {
    constexpr uint64_t MASK = 4095;
    if (bytes == 0 || bytes > UINT64_MAX - start || start + bytes > UINT64_MAX - MASK) return std::nullopt;
    uint64_t const BEGIN = start & ~MASK;
    return Span{BEGIN, ((start + bytes + MASK) & ~MASK) - BEGIN};
}
[[nodiscard]] constexpr auto overlaps(Span a, Span b) -> bool {
    return a.bytes != 0 && b.bytes != 0 && a.start < b.end() && b.start < a.end();
}
[[nodiscard]] constexpr auto contains(Span outer, Span inner) -> bool {
    return outer.bytes != 0 && inner.bytes != 0 && outer.start <= inner.start && inner.end() <= outer.end();
}

// Bounds for the explicitly trusted, conforming conventional PCI profile.
// They are not measured sizes or absence observations. A zero32 remains a
// possible 2-GiB resource; zero64 remains the complete physical address space.
[[nodiscard]] constexpr auto possible_bar(resource::Bar bar, uint8_t physical_bits) -> std::optional<Span> {
    if (!bar.memory() || physical_bits < 32 || physical_bits > 52) return std::nullopt;
    uint64_t const LIMIT = uint64_t{1} << physical_bits;
    if (bar.address >= LIMIT) return std::nullopt;
    uint64_t size = bar.kind == resource::BarKind::memory32 ? uint64_t{1} << 31 : LIMIT;
    if (bar.kind == resource::BarKind::memory32 && bar.address >= (uint64_t{1} << 32)) return std::nullopt;
    if (bar.address != 0) {
        uint64_t const ALIGNMENT = bar.address & (~bar.address + 1);
        if (ALIGNMENT < size) size = ALIGNMENT;
    }
    if (size > LIMIT - bar.address) return std::nullopt;
    return rounded(bar.address, size);
}
[[nodiscard]] constexpr auto possible_rom(uint32_t raw) -> std::optional<Span> {
    if ((raw & 0x7feU) != 0) return std::nullopt;
    uint64_t const BASE = raw & ~uint32_t{2047};
    uint64_t size = uint64_t{1} << 24;
    if (size > (uint64_t{1} << 32) - BASE) size = (uint64_t{1} << 32) - BASE;
    return rounded(BASE, size);
}

struct Image {
    std::array<uint32_t, 64> words{};
    [[nodiscard]] constexpr auto read(size_t offset, size_t width) const -> uint32_t {
        if ((width != 1 && width != 2 && width != 4) || offset >= 256 || width > 4 - offset % 4) return UINT32_MAX;
        return (words[offset / 4] >> ((offset % 4) * 8)) & (width == 4 ? UINT32_MAX : (uint32_t{1} << (width * 8)) - 1);
    }
    [[nodiscard]] constexpr auto read64(size_t offset) const -> uint64_t {
        if (offset % 4 != 0 || offset > 248) return UINT64_MAX;
        return uint64_t{words[offset / 4]} | (uint64_t{words[offset / 4 + 1]} << 32);
    }
};

// Cold-only conventional layout validation. Legacy ECAM validity stays
// unchanged. A supported MSI payload must not alias a different capability's
// power/reset/control bytes through overlapping firmware pointers.
[[nodiscard]] inline auto conventional_layout(const Image& image) -> bool {
    if ((image.read(6, 2) & 0x10U) == 0) return true;
    std::array<uint16_t, 48> starts{}, lengths{};
    size_t count = 0;
    uint64_t visited = 0;
    size_t at = image.read(0x34, 1);
    while (at != 0) {
        if (at < 0x40 || at > 0xfc || at % 4 != 0 || count == starts.size() || (visited & (uint64_t{1} << (at / 4))) != 0) return false;
        visited |= uint64_t{1} << (at / 4);
        size_t length = 0;
        switch (image.read(at, 1)) {
            case 1:
            case 3:
                length = 8;
                break;
            case 5: {
                uint32_t const CONTROL = image.read(at + 2, 2);
                if ((CONTROL & ~uint32_t{0x1ff}) != 0) return false;
                length = (CONTROL & 0x80U) != 0 ? 14 : 10;
                if ((CONTROL & 0x100U) != 0) length += 10;
                break;
            }
            case 0x10: {
                uint32_t const FLAGS = image.read(at + 2, 2);
                unsigned const VERSION = FLAGS & 15U, TYPE = (FLAGS >> 4) & 15U;
                if (VERSION == 0 || VERSION > 2 || TYPE == 2 || TYPE == 3 || TYPE > 10 || (image.read(at + 8, 2) & 0x8000U) != 0)
                    return false;
                // RCiEP has no Link/Slot/Root group. Version2 still reserves
                // the hole before DeviceCap/Control/Status2, through0x2c.
                length = TYPE == 9 ? (VERSION == 2 ? 0x2c : 0x0c) : VERSION == 2 ? 0x3c : TYPE == 0 || TYPE == 1 ? 0x14 : 0x24;
                break;
            }
            case 0x11:
                length = 12;
                break;
            case 0x12:
                if (image.read(0, 4) != 0x29228086U) return false;
                length = 8;
                break;
            default:
                return false;
        }
        if (length > 256 - at) return false;
        for (size_t i = 0; i < count; ++i)
            if (at < starts[i] + lengths[i] && starts[i] < at + length) return false;
        starts[count] = static_cast<uint16_t>(at);
        lengths[count++] = static_cast<uint16_t>(length);
        at = image.read(at + 1, 1);
    }
    return true;
}
[[nodiscard]] constexpr auto safe_device_control(uint16_t prior, uint16_t next) -> bool {
    return (next & 0x8000U) == 0 && (next | prior) == prior && ((prior ^ next) & ~uint16_t{0x0810}) == 0;
}
[[nodiscard]] constexpr auto upper_canonical(uint64_t address) -> bool { return address >= 0xffff800000000000ULL; }
[[nodiscard]] constexpr auto hhdm_disjoint(Span physical, uint64_t hhdm, Span arena) -> bool {
    if (physical.bytes == 0 || physical.bytes > UINT64_MAX - physical.start || arena.bytes == 0 || arena.bytes > UINT64_MAX - arena.start ||
        !upper_canonical(arena.start) || !upper_canonical(arena.end() - 1) || physical.start > UINT64_MAX - hhdm ||
        physical.bytes > UINT64_MAX - (physical.start + hhdm))
        return false;
    uint64_t const START = physical.start + hhdm, LAST = START + physical.bytes - 1;
    return upper_canonical(START) && upper_canonical(LAST) && (LAST < arena.start || arena.end() <= START);
}

struct Windows {
    bool valid{};
    Span memory{};
    Span prefetch{};
};
[[nodiscard]] constexpr auto bridge_windows(const Image& image, uint8_t physical_bits) -> Windows {
    if (physical_bits < 32 || physical_bits > 52) return {};
    uint64_t const LIMIT = uint64_t{1} << physical_bits;
    uint32_t const MEMORY = image.read(0x20, 4);
    if ((MEMORY & 0x000f000fU) != 0) return {};
    uint64_t const BASE = uint64_t{MEMORY & 0xfff0U} << 16;
    uint64_t const END = (uint64_t{(MEMORY >> 16) & 0xfff0U} << 16) + 0x100000;
    Windows result{.valid = true};
    if (BASE < END) result.memory = {BASE, END - BASE};
    uint32_t const PREFETCH = image.read(0x24, 4);
    uint32_t const TYPE = PREFETCH & 15U;
    if (TYPE > 1 || ((PREFETCH >> 16) & 15U) != TYPE) return {};
    uint64_t base = uint64_t{PREFETCH & 0xfff0U} << 16;
    uint64_t end = (uint64_t{(PREFETCH >> 16) & 0xfff0U} << 16) | 0xfffff;
    if (TYPE == 1) {
        base |= uint64_t{image.read(0x28, 4)} << 32;
        end |= uint64_t{image.read(0x2c, 4)} << 32;
    } else if (image.read(0x28, 4) != 0 || image.read(0x2c, 4) != 0) {
        return {};
    }
    if (base <= end) {
        if (end == UINT64_MAX || end >= LIMIT) return {};
        result.prefetch = {base, end + 1 - base};
    }
    return result;
}

[[nodiscard]] constexpr auto effective_offset(uint8_t offset, size_t width) -> size_t { return static_cast<size_t>(offset) & ~(width - 1); }
[[nodiscard]] constexpr auto touches(size_t effective, size_t width, size_t field, size_t bytes) -> bool {
    return effective < field + bytes && field < effective + width;
}
[[nodiscard]] constexpr auto project16(uint16_t prior, size_t field, size_t effective, size_t width, uint32_t value) -> uint16_t {
    uint16_t result = prior;
    for (size_t byte = 0; byte < width; ++byte) {
        size_t const AT = effective + byte;
        if (AT < field || AT >= field + 2) continue;
        auto const SHIFT = static_cast<unsigned>((AT - field) * 8);
        result = static_cast<uint16_t>((result & ~(uint16_t{255} << SHIFT)) | (((value >> (byte * 8)) & 255U) << SHIFT));
    }
    return result;
}
[[nodiscard]] constexpr auto geometry_write(uint8_t header, size_t effective, size_t width) -> bool {
    uint8_t const TYPE = header & 0x7fU;
    if ((width != 1 && width != 2 && width != 4) || effective > 256 - width || TYPE > 1) return true;
    if (touches(effective, width, 0x0e, 2)) return true;  // Header/BIST.
    if (TYPE == 0) return touches(effective, width, 0x10, 24) || touches(effective, width, 0x30, 4);
    // Includes buses, I/O/memory/prefetch windows and both upper words.
    return touches(effective, width, 0x10, 36) || touches(effective, width, 0x38, 4) || touches(effective, width, 0x3e, 2);
}
}  // namespace ker::dev::pci::aperture
