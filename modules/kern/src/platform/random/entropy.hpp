#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::mod::random::entropy {

// Seed the kernel DRBG from bounded RDSEED/RDRAND samples. No deterministic
// fallback is permitted: callers must fail closed when this returns false.
[[nodiscard]] auto initialize() -> bool;

// Readiness is monotonic after a successful initialize().
[[nodiscard]] auto is_ready() -> bool;

// Generate cryptographic bytes without allocation. Output is produced by the
// DRBG and is never raw hardware-random output. The IRQ-disabled critical
// section is capped to a small, fixed amount of work per iteration.
[[nodiscard]] auto get_bytes(void* output, size_t size) -> bool;

// Incorporate caller-supplied data into an already seeded DRBG. Supplied data
// can strengthen the state, but can never transition an unseeded DRBG to ready.
[[nodiscard]] auto mix_bytes(const void* input, size_t size) -> bool;

// Draw uniformly from [0, upper_exclusive) with rejection sampling.
[[nodiscard]] auto uniform_u64(uint64_t upper_exclusive, uint64_t& output) -> bool;

#ifdef WOS_SELFTEST
// Pure deterministic hooks. They never inspect or mutate the live DRBG.
[[nodiscard]] auto selftest_chacha20_known_answer() -> bool;
[[nodiscard]] auto selftest_uniform_from_sample(uint64_t sample, uint64_t upper_exclusive, uint64_t& output) -> bool;
#endif

}  // namespace ker::mod::random::entropy
