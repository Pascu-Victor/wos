#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace ker::dev::iommu::ivrs {

constexpr size_t HEADER_SIZE = 48;
constexpr size_t MAX_TABLE_SIZE = 65536;
constexpr size_t MAX_BLOCKS = 256;
constexpr size_t MAX_DEVICE_ENTRIES = 4096;

enum class Error : uint8_t {
    none,
    truncated,
    signature,
    length,
    checksum,
    revision,
    reserved,
    record_length,
    limit,
    register_range,
    capability_offset,
    device_entry_length,
    device_entry_type,
    device_range,
    memory_range,
    missing_hardware,
};

// Borrowed immutable observations, not resolved requester ownership or hardware
// support. IVHD alternatives may describe the SAME IOMMU: hardware_blocks is a
// block count, never a count of unique hardware units. No MMIO is authorized.
struct Table {
    std::span<const uint8_t> bytes{};
    uint32_t ivinfo = 0;
    uint8_t revision = 0;
    size_t hardware_blocks = 0;
    size_t memory_blocks = 0;
    size_t device_entries = 0;
    bool unsupported = false;
};

struct Result {
    Table table{};
    Error error = Error::none;
    size_t offset = 0;
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};

namespace detail {
// Complete bounds are established by the caller. Byte loads avoid packed
// structures, unaligned accesses and object-lifetime/aliasing assumptions.
inline auto read(std::span<const uint8_t> bytes, size_t offset, size_t count) -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < count; ++i) value |= uint64_t{bytes[offset + i]} << (i * 8U);
    return value;
}
inline auto zero(std::span<const uint8_t> bytes, size_t begin, size_t end) -> bool {
    for (size_t i = begin; i < end; ++i)
        if (bytes[i] != 0) return false;
    return true;
}
inline auto failure(Error error, size_t offset) -> Result { return {.error = error, .offset = offset}; }

inline auto entries(std::span<const uint8_t> bytes, size_t base, Table& table) -> Result {
    bool range = false;
    uint16_t start = 0;
    for (size_t offset = 0; offset < bytes.size();) {
        if (++table.device_entries > MAX_DEVICE_ENTRIES) return failure(Error::limit, base + offset);
        uint8_t const TYPE = bytes[offset];
        size_t size = 0;
        if (TYPE < 0x80) {
            size = size_t{4} << (TYPE >> 6U);
        } else if (TYPE == 0xf0) {
            // HID/CID + UID format/length. UID is variable, NOT a 32-byte
            // entry implied by the top two type bits. Do not decode its IDs.
            if (bytes.size() - offset < 22) return failure(Error::device_entry_length, base + offset);
            size = size_t{22} + bytes[offset + 21];
            table.unsupported = true;
        } else {
            // 16/32-byte encodings are reserved. Their future semantics are
            // unknown; guessing their length could misframe later requesters.
            return failure(Error::device_entry_type, base + offset);
        }
        if (size > bytes.size() - offset) return failure(Error::device_entry_length, base + offset);
        auto const ENTRY = bytes.subspan(offset, size);
        auto const ID = static_cast<uint16_t>(read(ENTRY, 1, 2));
        table.unsupported |= (ENTRY[3] & 8U) != 0;
        bool const PAD = TYPE == 0 || TYPE == 0x40;
        if (PAD) {
            // Ignored padding fields are not requester identities. Preserve
            // unfamiliar values as unsupported rather than inventing a
            // universal firmware zero requirement for them.
            table.unsupported |= !zero(ENTRY, 1, size);
        } else if (TYPE == 4) {
            if (!range || ID < start) return failure(Error::device_range, base + offset);
            range = false;
        } else {
            // A range is one start/end pair (padding is harmless). Other
            // entries cannot replace its start or implicitly terminate it.
            if (range) return failure(Error::device_range, base + offset);
            if (TYPE == 3 || TYPE == 0x43 || TYPE == 0x47) {
                range = true;
                start = ID;
            }
            if (TYPE == 0x42 || TYPE == 0x43) {
                // One used_id at +5 applies to the entire alias range. It is
                // not the base of an incrementing destination-ID range.
                if (ENTRY[4] != 0 || ENTRY[7] != 0) return failure(Error::reserved, base + offset);
            } else if (TYPE == 0x46 || TYPE == 0x47) {
                // Preserve all extended data. Applying its ATS policy belongs
                // to requester/backend qualification, which is not implemented.
                table.unsupported = true;
            } else if (TYPE == 0x48) {
                table.unsupported |= ENTRY[7] != 1 && ENTRY[7] != 2;
            } else if (TYPE != 1 && TYPE != 2 && TYPE != 3) {
                // Unknown 4/8-byte entries have safe framing, but must never
                // be silently treated as resolved requester ownership.
                table.unsupported = true;
            }
        }
        offset += size;
    }
    if (range) return failure(Error::device_range, base + bytes.size());
    return {};
}
}  // namespace detail

// Bounded, allocation-free syntax observer; no locks, logging, callbacks or
// hardware accesses. Revisions 1/2 and legacy PCI capability geometry are an
// explicit WOS observational subset, not a promise of complete IVRS semantics.
// See docs/gpu_platform.md for provenance and deliberately unsupported features.
[[nodiscard]] inline auto parse(std::span<const uint8_t> input) -> Result {
    using detail::failure;
    using detail::read;
    if (input.size() < HEADER_SIZE) return failure(Error::truncated, input.size());
    if (input[0] != 'I' || input[1] != 'V' || input[2] != 'R' || input[3] != 'S') return failure(Error::signature, 0);
    size_t const LENGTH = read(input, 4, 4);
    if (LENGTH < HEADER_SIZE || LENGTH > input.size() || LENGTH > MAX_TABLE_SIZE) return failure(Error::length, 4);
    auto const BYTES = input.first(LENGTH);
    uint8_t sum = 0;
    for (auto byte : BYTES) sum = static_cast<uint8_t>(sum + byte);
    if (sum != 0) return failure(Error::checksum, 9);
    if (BYTES[8] != 1 && BYTES[8] != 2) return failure(Error::revision, 8);
    if (!detail::zero(BYTES, 40, HEADER_SIZE)) return failure(Error::reserved, 40);
    Table table{.ivinfo = static_cast<uint32_t>(read(BYTES, 36, 4)), .revision = BYTES[8]};
    size_t blocks = 0;
    for (size_t offset = HEADER_SIZE; offset < LENGTH;) {
        if (++blocks > MAX_BLOCKS) return failure(Error::limit, offset);
        if (LENGTH - offset < 4) return failure(Error::record_length, offset);
        uint8_t const TYPE = BYTES[offset];
        size_t const SIZE = read(BYTES, offset + 2, 2);
        if (SIZE < 4 || SIZE > LENGTH - offset) return failure(Error::record_length, offset);
        auto const BLOCK = BYTES.subspan(offset, SIZE);
        if (TYPE == 0x10 || TYPE == 0x11 || TYPE == 0x40) {
            size_t const FIXED = TYPE == 0x10 ? 24 : 40;
            if (SIZE < FIXED) return failure(Error::record_length, offset);
            uint64_t const BASE = read(BLOCK, 8, 8);
            if (BASE == 0 || (BASE & 4095U) != 0) return failure(Error::register_range, offset + 8);
            uint64_t const CAP = read(BLOCK, 6, 2);
            if (CAP < 0x40 || CAP > 0xfc || (CAP & 3U) != 0) return failure(Error::capability_offset, offset + 6);
            // Flags, feature/attribute words and both EFR images are retained
            // raw. They are not reserved merely because older headers omit
            // their definitions. Type40's relative-name semantics remain open.
            table.unsupported |= TYPE != 0x10;
            ++table.hardware_blocks;
            auto const ENTRIES = detail::entries(BLOCK.subspan(FIXED), offset + FIXED, table);
            if (!ENTRIES.valid()) return ENTRIES;
        } else if (TYPE >= 0x20 && TYPE <= 0x22) {
            if (SIZE != 32) return failure(Error::record_length, offset);
            uint64_t const BASE = read(BLOCK, 16, 8);
            uint64_t const BYTES_COUNT = read(BLOCK, 24, 8);
            // Deliberate WOS subset: whole positive pages, no wrapping extent.
            // The field at +8 can contain a segment, so it is NOT zero-tested.
            if (BYTES_COUNT == 0 || (BASE & 4095U) != 0 || (BYTES_COUNT & 4095U) != 0 || BYTES_COUNT - 1 > UINT64_MAX - BASE)
                return failure(Error::memory_range, offset + 16);
            if (TYPE == 0x22 && read(BLOCK, 6, 2) < read(BLOCK, 4, 2)) return failure(Error::device_range, offset + 4);
            table.unsupported |= (BLOCK[1] & ~15U) != 0;
            table.unsupported |= !detail::zero(BLOCK, 8, 16);
            ++table.memory_blocks;
        } else {
            table.unsupported = true;
        }
        offset += SIZE;
    }
    if (table.hardware_blocks == 0) return failure(Error::missing_hardware, HEADER_SIZE);
    table.bytes = BYTES;
    return {.table = table};
}

}  // namespace ker::dev::iommu::ivrs
