#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace ker::dev::iommu::dmar {

constexpr size_t HEADER_SIZE = 48;
constexpr size_t MAX_TABLE_SIZE = 65536;
constexpr size_t MAX_RECORDS = 256;
constexpr size_t MAX_SCOPES = 1024;

enum class Error : uint8_t {
    none,
    truncated,
    signature,
    length,
    checksum,
    revision,
    address_width,
    reserved,
    record_length,
    record_order,
    limit,
    register_range,
    duplicate_unit,
    scope_length,
    scope_type,
    scope_path,
    reserved_range,
    missing_scope,
    missing_unit,
};

// The backing bytes must remain immutable and live while this view is used.
// A successful parse validates structure, not PCI topology or hardware support.
struct Table {
    std::span<const uint8_t> bytes{};
    uint8_t address_bits = 0;
    uint8_t flags = 0;
    size_t units = 0;
    size_t reserved_regions = 0;
    size_t scopes = 0;
    // Unknown records and namespace scopes must prevent strict admission until
    // their semantics have been implemented; they are never silently ignored.
    bool unsupported = false;
};

struct Result {
    Table table{};
    Error error = Error::none;
    size_t offset = 0;

    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};

namespace detail {

// Callers establish the complete field's bounds before reading it. Byte loads
// avoid alignment, aliasing, and packed-structure lifetime assumptions.
inline auto read(std::span<const uint8_t> bytes, size_t offset, size_t count) -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < count; ++i) {
        value |= uint64_t{bytes[offset + i]} << (i * 8U);
    }
    return value;
}

inline auto zero(std::span<const uint8_t> bytes, size_t begin, size_t end) -> bool {
    for (size_t i = begin; i < end; ++i) {
        if (bytes[i] != 0) {
            return false;
        }
    }
    return true;
}

inline auto failure(Error error, size_t offset) -> Result { return {.error = error, .offset = offset}; }

inline auto address_mask(uint8_t bits) -> uint64_t { return bits == 64 ? UINT64_MAX : (uint64_t{1} << bits) - 1; }

inline auto scopes(std::span<const uint8_t> bytes, size_t base, bool pci_only, bool include_all, Table& table) -> Result {
    for (size_t offset = 0; offset < bytes.size();) {
        if (bytes.size() - offset < 6) {
            return failure(Error::scope_length, base + offset);
        }
        auto const TYPE = bytes[offset];
        size_t const LENGTH = bytes[offset + 1];
        if (LENGTH < 8 || LENGTH > bytes.size() - offset || (LENGTH - 6) % 2 != 0) {
            return failure(Error::scope_length, base + offset);
        }
        if (TYPE == 0 || TYPE > 5 || (pci_only && TYPE > 2) || (include_all && TYPE <= 2)) {
            return failure(Error::scope_type, base + offset);
        }
        if (TYPE >= 4 && LENGTH != 8) {
            return failure(Error::scope_path, base + offset);
        }
        if (!zero(bytes, offset + 2, offset + 4) || (TYPE <= 2 && bytes[offset + 4] != 0)) {
            return failure(Error::reserved, base + offset);
        }
        for (size_t path = offset + 6; path < offset + LENGTH; path += 2) {
            if (bytes[path] > 31 || bytes[path + 1] > 7) {
                return failure(Error::scope_path, base + path);
            }
        }
        if (++table.scopes > MAX_SCOPES) {
            return failure(Error::limit, base + offset);
        }
        table.unsupported |= TYPE == 5;
        offset += LENGTH;
    }
    return {};
}

}  // namespace detail

// No allocation, MMIO, logging, callbacks, or locking. Input length is the
// accessible extent supplied by the caller, never inferred from firmware alone.
[[nodiscard]] inline auto parse(std::span<const uint8_t> input) -> Result {
    using detail::failure;
    using detail::read;
    using detail::zero;
    if (input.size() < HEADER_SIZE) {
        return failure(Error::truncated, input.size());
    }
    if (input[0] != 'D' || input[1] != 'M' || input[2] != 'A' || input[3] != 'R') {
        return failure(Error::signature, 0);
    }
    size_t const LENGTH = read(input, 4, 4);
    if (LENGTH < HEADER_SIZE || LENGTH > input.size() || LENGTH > MAX_TABLE_SIZE) {
        return failure(Error::length, 4);
    }
    auto const BYTES = input.first(LENGTH);
    uint8_t sum = 0;
    for (auto byte : BYTES) {
        sum = static_cast<uint8_t>(sum + byte);
    }
    if (sum != 0) {
        return failure(Error::checksum, 9);
    }
    if (BYTES[8] != 1) {
        return failure(Error::revision, 8);
    }
    if (BYTES[36] < 11 || BYTES[36] > 63) {
        return failure(Error::address_width, 36);
    }
    if ((BYTES[37] & ~7U) != 0 || !zero(BYTES, 38, HEADER_SIZE)) {
        return failure(Error::reserved, 37);
    }
    Table table{.address_bits = static_cast<uint8_t>(BYTES[36] + 1U), .flags = BYTES[37]};
    uint64_t const MASK = detail::address_mask(table.address_bits);
    size_t records = 0;
    uint16_t previous_type = 0;
    for (size_t offset = HEADER_SIZE; offset < LENGTH;) {
        if (++records > MAX_RECORDS) {
            return failure(Error::limit, offset);
        }
        if (LENGTH - offset < 4) {
            return failure(Error::record_length, offset);
        }
        auto const TYPE = static_cast<uint16_t>(read(BYTES, offset, 2));
        size_t const SIZE = read(BYTES, offset + 2, 2);
        if (SIZE < 4 || SIZE > LENGTH - offset) {
            return failure(Error::record_length, offset);
        }
        if (TYPE < previous_type) {
            return failure(Error::record_order, offset);
        }
        previous_type = TYPE;
        auto const RECORD = BYTES.subspan(offset, SIZE);
        size_t scope_start = SIZE;
        if (TYPE == 0) {
            if (SIZE < 16) {
                return failure(Error::record_length, offset);
            }
            if ((RECORD[4] & ~1U) != 0 || (RECORD[5] & ~15U) != 0) {
                return failure(Error::reserved, offset + 4);
            }
            uint64_t const BASE = read(RECORD, 8, 8);
            uint64_t const REGISTER_BYTES = uint64_t{4096} << RECORD[5];
            if (BASE == 0 || (BASE & (REGISTER_BYTES - 1)) != 0 || BASE > MASK || REGISTER_BYTES - 1 > MASK - BASE) {
                return failure(Error::register_range, offset + 8);
            }
            // All preceding DRHDs are already structurally validated. Reject
            // overlapping register banks and ambiguous segment-wide defaults.
            for (size_t prior = HEADER_SIZE; prior < offset; prior += read(BYTES, prior + 2, 2)) {
                uint64_t const PRIOR_BASE = read(BYTES, prior + 8, 8);
                uint64_t const PRIOR_SIZE = uint64_t{4096} << BYTES[prior + 5];
                bool const OVERLAP = BASE <= PRIOR_BASE + PRIOR_SIZE - 1 && PRIOR_BASE <= BASE + REGISTER_BYTES - 1;
                bool const TWO_DEFAULTS = (RECORD[4] & BYTES[prior + 4] & 1U) != 0 && read(RECORD, 6, 2) == read(BYTES, prior + 6, 2);
                if (OVERLAP || TWO_DEFAULTS) {
                    return failure(Error::duplicate_unit, offset);
                }
                if ((BYTES[prior + 4] & 1U) != 0 && read(RECORD, 6, 2) == read(BYTES, prior + 6, 2)) {
                    return failure(Error::record_order, offset);
                }
            }
            if (SIZE == 16 && (RECORD[4] & 1U) == 0) {
                return failure(Error::missing_scope, offset);
            }
            ++table.units;
            scope_start = 16;
        } else if (TYPE == 1) {
            if (SIZE < 24) {
                return failure(Error::record_length, offset);
            }
            if (!zero(RECORD, 4, 6)) {
                return failure(Error::reserved, offset + 4);
            }
            uint64_t const BASE = read(RECORD, 8, 8);
            uint64_t const END = read(RECORD, 16, 8);
            if (BASE > END || END > MASK || (BASE & 4095U) != 0 || (END & 4095U) != 4095U) {
                return failure(Error::reserved_range, offset + 8);
            }
            if (SIZE == 24) {
                return failure(Error::missing_scope, offset);
            }
            ++table.reserved_regions;
            scope_start = 24;
        } else {
            // Preserve forward-compatible framing, but report unsupported
            // semantics. Enforcement must reject this result until resolved.
            table.unsupported = true;
        }
        bool const INCLUDE_ALL = TYPE == 0 && (RECORD[4] & 1U) != 0;
        auto const SCOPES = detail::scopes(RECORD.subspan(scope_start), offset + scope_start, TYPE == 1, INCLUDE_ALL, table);
        if (!SCOPES.valid()) {
            return SCOPES;
        }
        offset += SIZE;
    }
    if (table.units == 0) {
        return failure(Error::missing_unit, HEADER_SIZE);
    }
    table.bytes = BYTES;
    return {.table = table};
}

}  // namespace ker::dev::iommu::dmar
