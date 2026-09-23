#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace ker::mod::acpi::bounded {

constexpr size_t SDT_HEADER_SIZE = 36;
constexpr size_t MAX_TABLE_BYTES = 1024 * 1024;
constexpr size_t MAX_ROOT_ENTRIES = 256;

enum class Error : uint8_t { none, not_found, inaccessible, signature, length, checksum, root_entries, duplicate };

struct Result {
    std::span<const uint8_t> bytes{};
    uint64_t physical = 0;
    Error error = Error::not_found;
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};

[[nodiscard]] constexpr auto contains(uint64_t base, uint64_t length, uint64_t address, uint64_t bytes) -> bool {
    return length != 0 && length <= UINT64_MAX - base && bytes != 0 && address >= base && address - base < length &&
           bytes <= length - (address - base);
}

namespace detail {
inline auto read(std::span<const uint8_t> bytes, size_t offset, size_t count) -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < count; ++i) {
        value |= uint64_t{bytes[offset + i]} << (8U * i);
    }
    return value;
}

inline auto signature(std::span<const uint8_t> bytes, std::string_view text) -> bool {
    if (bytes.size() < text.size()) {
        return false;
    }
    for (size_t i = 0; i < text.size(); ++i) {
        if (bytes[i] != static_cast<uint8_t>(text[i])) {
            return false;
        }
    }
    return true;
}

inline auto checksum(std::span<const uint8_t> bytes) -> bool {
    uint8_t sum = 0;
    for (auto byte : bytes) {
        sum = static_cast<uint8_t>(sum + byte);
    }
    return sum == 0;
}

inline auto fail(Error error, uint64_t physical) -> Result { return {.physical = physical, .error = error}; }

template <typename Reader>
auto sdt(Reader& reader, uint64_t physical) -> Result {
    auto const HEADER = reader(physical, SDT_HEADER_SIZE);
    if (HEADER.size() != SDT_HEADER_SIZE) {
        return fail(Error::inaccessible, physical);
    }
    size_t const LENGTH = read(HEADER, 4, 4);
    if (LENGTH < SDT_HEADER_SIZE || LENGTH > MAX_TABLE_BYTES || LENGTH > UINT64_MAX - physical) {
        return fail(Error::length, physical);
    }
    auto const BYTES = reader(physical, LENGTH);
    if (BYTES.size() != LENGTH) {
        return fail(Error::inaccessible, physical);
    }
    if (!checksum(BYTES)) {
        return fail(Error::checksum, physical);
    }
    return {.bytes = BYTES, .physical = physical, .error = Error::none};
}
}  // namespace detail

// Reader must return exactly the requested immutable, accessible bytes or an
// empty span. It independently validates physical ownership and CPU mappings;
// firmware lengths never establish accessibility. Reader storage outlives result.
template <typename Reader>
[[nodiscard]] auto find(Reader& reader, uint64_t rsdp_physical, std::string_view wanted) -> Result {
    using detail::fail;
    using detail::read;
    if (wanted.size() != 4) {
        return fail(Error::signature, rsdp_physical);
    }
    auto const LEGACY = reader(rsdp_physical, 20);
    if (LEGACY.size() != 20) {
        return fail(Error::inaccessible, rsdp_physical);
    }
    if (!detail::signature(LEGACY, "RSD PTR ")) {
        return fail(Error::signature, rsdp_physical);
    }
    if (!detail::checksum(LEGACY)) {
        return fail(Error::checksum, rsdp_physical);
    }
    uint64_t root_physical = read(LEGACY, 16, 4);
    size_t stride = 4;
    if (LEGACY[15] >= 2) {
        auto const EXTENDED = reader(rsdp_physical, 36);
        if (EXTENDED.size() != 36) {
            return fail(Error::inaccessible, rsdp_physical);
        }
        size_t const LENGTH = read(EXTENDED, 20, 4);
        if (LENGTH < 36 || LENGTH > 4096 || LENGTH > UINT64_MAX - rsdp_physical) {
            return fail(Error::length, rsdp_physical);
        }
        auto const BYTES = reader(rsdp_physical, LENGTH);
        if (BYTES.size() != LENGTH) {
            return fail(Error::inaccessible, rsdp_physical);
        }
        if (!detail::checksum(BYTES)) {
            return fail(Error::checksum, rsdp_physical);
        }
        uint64_t const XSDT = read(BYTES, 24, 8);
        if (XSDT != 0) {
            root_physical = XSDT;
            stride = 8;
        }
    }
    if (root_physical == 0) {
        return fail(Error::inaccessible, 0);
    }
    auto const ROOT = detail::sdt(reader, root_physical);
    if (!ROOT.valid()) {
        return ROOT;
    }
    if (!detail::signature(ROOT.bytes, stride == 8 ? "XSDT" : "RSDT")) {
        return fail(Error::signature, root_physical);
    }
    size_t const PAYLOAD = ROOT.bytes.size() - SDT_HEADER_SIZE;
    if (PAYLOAD % stride != 0 || PAYLOAD / stride > MAX_ROOT_ENTRIES) {
        return fail(Error::root_entries, root_physical);
    }
    Result found{};
    for (size_t offset = SDT_HEADER_SIZE; offset < ROOT.bytes.size(); offset += stride) {
        uint64_t const PHYSICAL = read(ROOT.bytes, offset, stride);
        auto const HEADER = reader(PHYSICAL, SDT_HEADER_SIZE);
        if (HEADER.size() != SDT_HEADER_SIZE) {
            return fail(Error::inaccessible, PHYSICAL);
        }
        if (!detail::signature(HEADER, wanted)) {
            continue;
        }
        if (found.valid()) {
            return fail(Error::duplicate, PHYSICAL);
        }
        found = detail::sdt(reader, PHYSICAL);
        if (!found.valid()) {
            return found;
        }
    }
    return found;
}

}  // namespace ker::mod::acpi::bounded
