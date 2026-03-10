#pragma once

// CRC32C (Castagnoli) — used by XFS v5 for metadata integrity verification.
// Hardware-accelerated via SSE4.2 crc32 instruction on x86_64, with a
// software lookup-table fallback.
//
// Reference: Linux lib/crc32.c, arch/x86/crypto/crc32c-intel_glue.c,
//            xfs/libxfs/xfs_cksum.h

#include <cstddef>
#include <cstdint>

namespace ker::util {

// Compute CRC32C over a buffer.
// `crc` is the initial/seed value (use CRC32C_SEED for a fresh computation,
// or the result of a previous call for incremental hashing).
auto crc32c(uint32_t crc, const void* data, size_t length) -> uint32_t;

// Seed value matching XFS convention (~0U).
constexpr uint32_t CRC32C_SEED = ~static_cast<uint32_t>(0);

// Finalize a CRC32C value (bitwise complement, matching Linux crc32c_le convention).
inline auto crc32c_final(uint32_t crc) -> uint32_t { return ~crc; }

// Convenience: compute CRC32C of a buffer from scratch and finalize.
inline auto crc32c_compute(const void* data, size_t length) -> uint32_t { return crc32c_final(crc32c(CRC32C_SEED, data, length)); }

// XFS-specific helper: compute CRC32C of a metadata block that contains an
// embedded CRC field at `cksum_offset` bytes.  The CRC field is treated as
// zero during computation (matching xfs_start_cksum_safe / xfs_end_cksum).
auto crc32c_block_with_cksum(const void* buffer, size_t length, size_t cksum_offset) -> uint32_t;

// Detect SSE4.2 CRC32C hardware support.
auto crc32c_has_hw() -> bool;

}  // namespace ker::util
