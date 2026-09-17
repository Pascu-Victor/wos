#pragma once

#include <abi/ptrace.hpp>
#include <cstddef>
#include <cstdint>
#include <span>

namespace ker::mod::security {

inline constexpr uint64_t DEBUG_USER_LIMIT = 0x0000'8000'0000'0000ULL;
inline constexpr uint64_t DEBUG_USER_FLAGS = 0x250fd7;
inline constexpr uint64_t DEBUG_REQUIRED_FLAGS = 0x202;

[[nodiscard]] constexpr auto valid_debug_gprs(const abi::ptrace::X86_64GprState& state, uint64_t user_cs, uint64_t user_ss) -> bool {
    return state.rip != 0 && state.rip < DEBUG_USER_LIMIT && state.rsp != 0 && state.rsp < DEBUG_USER_LIMIT &&
           state.fs_base < DEBUG_USER_LIMIT && state.gs_base < DEBUG_USER_LIMIT && state.cs == user_cs && state.ss == user_ss &&
           (state.rflags & ~DEBUG_USER_FLAGS) == 0 && (state.rflags & DEBUG_REQUIRED_FLAGS) == DEBUG_REQUIRED_FLAGS;
}

// The caller supplies the CPU's enabled XCR0 and a trusted FXSAVE MXCSR_MASK,
// never the mask in the proposed image. WOS restores standard x87/SSE/AVX only.
[[nodiscard]] constexpr auto valid_debug_xstate(std::span<const uint8_t> state, uint64_t enabled, uint32_t mxcsr_mask) -> bool {
    if (state.size() < 512 || (enabled != 0 && enabled != 3 && enabled != 7)) {
        return false;
    }
    uint32_t mxcsr = 0;
    for (size_t i = 0; i < sizeof(mxcsr); ++i) {
        mxcsr |= static_cast<uint32_t>(state[24 + i]) << (8 * i);
    }
    if ((mxcsr & ~mxcsr_mask) != 0) {
        return false;
    }
    if (enabled == 0) {
        return true;
    }
    if (state.size() < (enabled == 7 ? 832U : 576U)) {
        return false;
    }
    uint64_t features = 0;
    for (size_t i = 0; i < sizeof(features); ++i) {
        features |= static_cast<uint64_t>(state[512 + i]) << (8 * i);
    }
    if ((features & ~enabled) != 0) {
        return false;
    }
    // XCOMP_BV and every reserved XSAVE header byte must be zero.
    for (size_t i = 520; i < 576; ++i) {
        if (state[i] != 0) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] constexpr auto valid_debug_breakpoint(const abi::ptrace::HwBreak& desc, bool enable) -> bool {
    if (desc.slot >= 4 || desc.reserved != 0) {
        return false;
    }
    if (!enable) {
        return true;
    }
    using Type = abi::ptrace::hw_break_type;
    if (desc.type != Type::EXECUTE && desc.type != Type::WRITE && desc.type != Type::READ_WRITE) {
        return false;
    }
    if (desc.length != 1 && desc.length != 2 && desc.length != 4 && desc.length != 8) {
        return false;
    }
    if ((desc.type == Type::EXECUTE && desc.length != 1) || desc.address == 0 || desc.address >= DEBUG_USER_LIMIT ||
        desc.length > DEBUG_USER_LIMIT - desc.address) {
        return false;
    }
    return desc.address % desc.length == 0;
}

}  // namespace ker::mod::security
