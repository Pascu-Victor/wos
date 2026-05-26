#include <cstdint>
#include <cstddef>
#include <test/ktest.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/sys/signal.hpp"

KTEST(SignalAbi, FrameAddressPreservesRedZone) {
    constexpr uint64_t USER_RSP = 0x7ffefffee548ULL;
    constexpr uint64_t FRAME_ADDR = ker::mod::sys::signal::signal_frame_address(USER_RSP);
    constexpr uint64_t RED_ZONE_LOW = USER_RSP - ker::mod::sys::signal::USER_RED_ZONE_SIZE;
    constexpr uint64_t FRAME_END = FRAME_ADDR + sizeof(ker::mod::sys::signal::SignalFrame);

    KEXPECT_EQ(FRAME_ADDR & 0xFULL, 8ULL);
    KEXPECT_TRUE(FRAME_END <= RED_ZONE_LOW);
}

KTEST(SignalAbi, FrameAddressMatchesSysvCallEntryAlignment) {
    constexpr uint64_t USER_RSP = 0x700000000000ULL;
    constexpr uint64_t FRAME_ADDR = ker::mod::sys::signal::signal_frame_address(USER_RSP);

    KEXPECT_EQ((FRAME_ADDR + 8) & 0xFULL, 0ULL);
}

KTEST(SyscallAbi, PerCpuEntryScratchOffset) {
    KEXPECT_EQ(offsetof(ker::mod::cpu::PerCpu, syscall_entry_tmp), static_cast<size_t>(0x38));
    KEXPECT_EQ(sizeof(ker::mod::cpu::PerCpu), static_cast<size_t>(64));
}
