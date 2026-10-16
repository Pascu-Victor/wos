#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace numa_bench {
inline constexpr uint64_t PAGE = 4096;
inline constexpr uint32_t STRIDE = 64;
struct Options {
    uint32_t pairs = 10;
    uint64_t bytes = 67108864;
    uint64_t loads = 16777216;
    uint64_t seed = 0x402109;
    uint32_t chase_cpu = 0, local_init_cpu = 0, remote_init_cpu = 4;
    uint32_t stride = STRIDE, timeout_ms = 600000;
};
inline auto number(const char* text, uint64_t& output) -> bool {
    if (text == nullptr || text[0] < '0' || text[0] > '9') return false;
    errno = 0;
    char* end = nullptr;
    auto const value = std::strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0') return false;
    output = value;
    return true;
}
inline auto parse_options(int argc, char** argv, Options& output) -> bool {
    Options candidate;
    uint32_t seen = 0;
    constexpr const char* NAMES[]{"--pairs",          "--bytes",           "--loads",  "--seed",      "--chase-cpu",
                                  "--local-init-cpu", "--remote-init-cpu", "--stride", "--timeout-ms"};
    for (int i = 0; i < argc; i += 2) {
        if (i + 1 >= argc) return false;
        size_t field = 0;
        while (field < 9 && std::strcmp(argv[i], NAMES[field]) != 0) ++field;
        if (field == 9 || (seen & (uint32_t{1} << field)) != 0) return false;
        seen |= uint32_t{1} << field;
        uint64_t value;
        if (!number(argv[i + 1], value)) return false;
        switch (field) {
            case 0:
                if (value < 1 || value > 10) return false;
                candidate.pairs = static_cast<uint32_t>(value);
                break;
            case 1:
                if (value < (1U << 20) || value > (64U << 20) || value % PAGE != 0) return false;
                candidate.bytes = value;
                break;
            case 2:
                if (value < 1 || value > (uint64_t{1} << 28)) return false;
                candidate.loads = value;
                break;
            case 3:
                candidate.seed = value;
                break;
            case 4:
                if (value > 63) return false;
                candidate.chase_cpu = static_cast<uint32_t>(value);
                break;
            case 5:
                if (value > 63) return false;
                candidate.local_init_cpu = static_cast<uint32_t>(value);
                break;
            case 6:
                if (value > 63) return false;
                candidate.remote_init_cpu = static_cast<uint32_t>(value);
                break;
            case 7:
                if (value != STRIDE) return false;
                candidate.stride = static_cast<uint32_t>(value);
                break;
            case 8:
                if (value < 1000 || value > 600000) return false;
                candidate.timeout_ms = static_cast<uint32_t>(value);
                break;
        }
    }
    output = candidate;
    return true;
}
inline auto next_random(uint64_t& state) noexcept -> uint64_t {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}
// One shuffled cycle covers every stride element, including non-power-of-two
// page-multiple lengths. Scratch is checked caller-owned preparation memory.
inline void permutation(uint32_t* indices, uint32_t count, uint64_t seed) noexcept {
    for (uint32_t i = 0; i < count; ++i) indices[i] = i;
    uint64_t state = seed ^ 0x9e3779b97f4a7c15ULL;
    if (state == 0) state = 1;
    for (uint32_t i = count - 1; i != 0; --i) {
        uint32_t const j = static_cast<uint32_t>(next_random(state) % (static_cast<uint64_t>(i) + 1));
        uint32_t const previous = indices[i];
        indices[i] = indices[j];
        indices[j] = previous;
    }
}
inline void initialize_cycle(void* mapping, size_t bytes, const uint32_t* indices, uint32_t count) noexcept {
    auto* payload = static_cast<volatile uint8_t*>(mapping);
    for (size_t i = 0; i < bytes; ++i) payload[i] = 0xa5;
    uintptr_t const base = reinterpret_cast<uintptr_t>(mapping);
    for (uint32_t i = 0; i < count; ++i) {
        auto* slot = reinterpret_cast<volatile uintptr_t*>(base + static_cast<uintptr_t>(indices[i]) * STRIDE);
        *slot = base + static_cast<uintptr_t>(indices[(i + 1) % count]) * STRIDE;
    }
}
// Fixed finite dependent volatile loads only. No clock, query, logging,
// allocation, cancellation or deadline call belongs inside this measured loop.
[[gnu::noinline]] inline auto dependent_loads(uintptr_t cursor, uint64_t loads) noexcept -> uintptr_t {
    for (uint64_t i = 0; i < loads; ++i) cursor = *reinterpret_cast<const volatile uintptr_t*>(cursor);
    return cursor;
}
inline auto consumed_index(uintptr_t base, uint64_t bytes, uintptr_t result, uint64_t& index) noexcept -> bool {
    if (result < base || result - base >= bytes || (result - base) % STRIDE != 0) return false;
    index = (result - base) / STRIDE;
    return true;
}
inline auto raw_error(uint64_t value) noexcept -> bool { return value >= UINT64_MAX - 4094; }
inline auto exact_mask(uint64_t value, uint32_t cpu) noexcept -> bool { return cpu < 64 && value == (uint64_t{1} << cpu); }
}  // namespace numa_bench
