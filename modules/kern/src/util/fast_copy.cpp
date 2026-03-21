#include "fast_copy.hpp"

#include <array>
#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::util {
namespace {

constexpr size_t SIMD_COPY_THRESHOLD = 4096;
constexpr size_t MAX_FAST_COPY_CPUS = 64;

inline void copy_scalar(uint8_t* dst, const uint8_t* src, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        dst[i] = src[i];
    }
}

struct alignas(16) FxState {
    std::array<uint8_t, 512> bytes{};
};

// SIMD save/restore buffers
std::array<FxState, MAX_FAST_COPY_CPUS> g_fx_state{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

inline void copy_sse(uint8_t* dst, const uint8_t* src, size_t len) {
    uint64_t cpu = ker::mod::cpu::currentCpu();
    if (cpu >= MAX_FAST_COPY_CPUS) {
        cpu = 0;
    }
    FxState& state = g_fx_state[cpu];

    uint64_t rflags = 0;
    // Prevent IRQ/preemption interleaving while owning SIMD state.
    asm volatile("pushfq; pop %0; cli" : "=r"(rflags) : : "memory", "cc");
    asm volatile("fxsave64 %0" : "=m"(state) : : "memory");

    size_t chunks = len / 16;
    if (chunks != 0) {
        asm volatile(
            "1:\n\t"
            "movdqu (%[s]), %%xmm0\n\t"
            "movdqu %%xmm0, (%[d])\n\t"
            "add $16, %[s]\n\t"
            "add $16, %[d]\n\t"
            "dec %[c]\n\t"
            "jnz 1b\n\t"
            : [d] "+r"(dst), [s] "+r"(src), [c] "+r"(chunks)
            :
            : "xmm0", "memory", "cc");
    }

    size_t tail = len & 0x0F;
    if (tail != 0) {
        copy_scalar(dst, src, tail);
    }

    asm volatile("fxrstor64 %0" : : "m"(state) : "memory");
    asm volatile("push %0; popfq" : : "r"(rflags) : "memory", "cc");
}

}  // namespace

void copy_fast(void* dst, const void* src, size_t len) {
    auto* d = static_cast<uint8_t*>(dst);
    const auto* s = static_cast<const uint8_t*>(src);

    if (len >= SIMD_COPY_THRESHOLD) {
        copy_sse(d, s, len);
        return;
    }

    copy_scalar(d, s, len);
}

}  // namespace ker::util
