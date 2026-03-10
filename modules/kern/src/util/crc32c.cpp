// CRC32C (Castagnoli) implementation.
// SSE4.2 hardware path using __builtin_ia32_crc32* intrinsics, plus a
// software lookup-table fallback for completeness.
//
// The CRC32C polynomial is 0x1EDC6F41 (Castagnoli), different from the
// "normal" CRC32 polynomial used by Ethernet/zlib (0x04C11DB7).
//
// Reference: Linux lib/crc32.c, arch/x86/crypto/crc32c-intel_glue.c

#include "crc32c.hpp"

#include <cstdint>
#include <cstring>

namespace ker::util {

// ---------------------------------------------------------------------------
// CPUID helper — detect SSE4.2 (bit 20 of ECX from CPUID leaf 1)
// ---------------------------------------------------------------------------

static bool hw_crc32_available = false;
static bool hw_checked = false;

auto crc32c_has_hw() -> bool {
    if (!hw_checked) {
        uint32_t eax = 0;
        uint32_t ebx = 0;
        uint32_t ecx = 0;
        uint32_t edx = 0;
        asm volatile("cpuid" : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "a"(1));
        hw_crc32_available = (ecx & (1u << 20)) != 0;
        hw_checked = true;
    }
    return hw_crc32_available;
}

// ---------------------------------------------------------------------------
// SSE4.2 hardware CRC32C
// ---------------------------------------------------------------------------

__attribute__((target("sse4.2"))) static auto crc32c_hw(uint32_t crc, const uint8_t* data, size_t length) -> uint32_t {
    // Process 8 bytes at a time using the 64-bit CRC32 instruction
    uint64_t crc64 = crc;
    while (length >= 8) {
        uint64_t val;
        __builtin_memcpy(&val, data, 8);
        crc64 = __builtin_ia32_crc32di(crc64, val);
        data += 8;
        length -= 8;
    }
    auto crc32 = static_cast<uint32_t>(crc64);

    // Handle remaining bytes with the 8-bit CRC32 instruction
    while (length > 0) {
        crc32 = __builtin_ia32_crc32qi(crc32, *data);
        data++;
        length--;
    }
    return crc32;
}

// ---------------------------------------------------------------------------
// Software CRC32C lookup table (CRC32C polynomial 0x82F63B78, reflected)
// ---------------------------------------------------------------------------

static uint32_t sw_table[256];  // NOLINT
static bool sw_table_built = false;

static void build_sw_table() {
    constexpr uint32_t POLY = 0x82F63B78u;  // Reflected CRC32C polynomial
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if ((crc & 1u) != 0) {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
        sw_table[i] = crc;
    }
    sw_table_built = true;
}

static auto crc32c_sw(uint32_t crc, const uint8_t* data, size_t length) -> uint32_t {
    if (!sw_table_built) {
        build_sw_table();
    }
    for (size_t i = 0; i < length; i++) {
        crc = sw_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

auto crc32c(uint32_t crc, const void* data, size_t length) -> uint32_t {
    if (data == nullptr || length == 0) {
        return crc;
    }
    const auto* p = static_cast<const uint8_t*>(data);

    if (crc32c_has_hw()) {
        [[likely]] return crc32c_hw(crc, p, length);
    }
    return crc32c_sw(crc, p, length);
}

auto crc32c_block_with_cksum(const void* buffer, size_t length, size_t cksum_offset) -> uint32_t {
    const auto* buf = static_cast<const char*>(buffer);
    uint32_t zero = 0;

    // CRC up to the checksum field
    uint32_t crc = crc32c(CRC32C_SEED, buf, cksum_offset);

    // Skip checksum field, feeding zeros
    crc = crc32c(crc, &zero, sizeof(uint32_t));

    // CRC the remainder after the checksum field
    size_t after_offset = cksum_offset + sizeof(uint32_t);
    crc = crc32c(crc, buf + after_offset, length - after_offset);

    return crc32c_final(crc);
}

}  // namespace ker::util
