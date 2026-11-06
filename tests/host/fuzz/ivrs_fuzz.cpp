#include <cstddef>
#include <cstdint>
#include <dev/iommu/ivrs.hpp>
#include <span>
#include <vector>

namespace {
namespace ivrs = ker::dev::iommu::ivrs;
using Bytes = std::vector<uint8_t>;

void put(Bytes& bytes, size_t offset, uint64_t value, size_t count) {
    for (size_t i = 0; i < count; ++i) bytes.at(offset + i) = static_cast<uint8_t>(value >> (8U * i));
}

void check(std::span<const uint8_t> bytes) {
    auto const result = ivrs::parse(bytes);
    auto const repeated = ivrs::parse(bytes);
    if (result.error != repeated.error || result.offset != repeated.offset ||
        result.table.device_entries != repeated.table.device_entries || result.table.unsupported != repeated.table.unsupported)
        __builtin_trap();
    if (result.valid()) {
        if (result.table.bytes.data() != bytes.data() || result.table.bytes.size() < 48 || result.table.bytes.size() > bytes.size() ||
            result.table.bytes.size() > ivrs::MAX_TABLE_SIZE || result.table.hardware_blocks == 0 ||
            result.table.hardware_blocks + result.table.memory_blocks > ivrs::MAX_BLOCKS ||
            result.table.device_entries > ivrs::MAX_DEVICE_ENTRIES || (result.table.revision != 1 && result.table.revision != 2))
            __builtin_trap();
    } else if (!result.table.bytes.empty() || result.table.hardware_blocks != 0 || result.table.memory_blocks != 0 ||
               result.table.device_entries != 0 || result.table.ivinfo != 0 || result.table.revision != 0 || result.table.unsupported ||
               result.offset > bytes.size()) {
        __builtin_trap();
    }
}

auto header() -> Bytes {
    Bytes bytes(48);
    bytes[0] = 'I';
    bytes[1] = 'V';
    bytes[2] = 'R';
    bytes[3] = 'S';
    bytes[8] = 1;
    return bytes;
}

void seal_and_check(Bytes& bytes) {
    put(bytes, 4, bytes.size(), 4);
    bytes[9] = 0;
    uint8_t sum = 0;
    for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
    bytes[9] = static_cast<uint8_t>(0U - sum);
    check(bytes);
}

void append_hardware(Bytes& bytes, uint8_t kind) {
    size_t const offset = bytes.size();
    size_t const fixed = kind == 0x10 ? 24 : 40;
    bytes.resize(offset + fixed);
    bytes[offset] = kind;
    put(bytes, offset + 2, fixed, 2);
    put(bytes, offset + 6, 0x40, 2);
    put(bytes, offset + 8, 0xfed90000, 8);
}
}  // namespace

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) -> int {
    std::span<const uint8_t> const input{data, size};
    check(input);
    if (size <= ivrs::MAX_TABLE_SIZE - 48) {
        // Reach record framing without rediscovering ACPI signature/checksum.
        auto bytes = header();
        if (!input.empty()) bytes.insert(bytes.end(), input.begin(), input.end());
        seal_and_check(bytes);
    }
    if (size <= ivrs::MAX_TABLE_SIZE - 88) {
        // Also reach entry parsing behind a valid fixed hardware header. The
        // raw payload still controls entry types, ranges, HID lengths and tails.
        uint8_t const kind = size == 0 || data[0] % 3 == 0 ? 0x10 : data[0] % 3 == 1 ? 0x11 : 0x40;
        auto bytes = header();
        append_hardware(bytes, kind);
        if (!input.empty()) bytes.insert(bytes.end(), input.begin(), input.end());
        put(bytes, 50, bytes.size() - 48, 2);
        seal_and_check(bytes);
    }
    if (size <= 32) {
        // Fixed IVMD framing reaches geometry and requester-range validation;
        // default fields provide a valid positive-page seed for short inputs.
        auto bytes = header();
        append_hardware(bytes, 0x10);
        size_t const offset = bytes.size();
        bytes.resize(offset + 32);
        put(bytes, offset + 24, 4096, 8);
        for (size_t i = 0; i < size; ++i) bytes[offset + i] = input[i];
        bytes[offset] = static_cast<uint8_t>(0x20 + (size == 0 ? 0 : data[0] % 3));
        put(bytes, offset + 2, 32, 2);
        seal_and_check(bytes);
    }
    return 0;
}
