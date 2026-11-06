#include "cpu_safe_msr.hpp"

#include <cstddef>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/interrupt/idt.hpp>

extern "C" {
auto wos_optional_msr_read(uint32_t index, uint64_t* output) -> bool;
extern const unsigned char wos_optional_msr_fault;
extern const unsigned char wos_optional_msr_recovery;
#ifdef WOS_SELFTEST
[[noreturn]] void wos_selftest_unmatched_msr_fault();
#endif
}

static_assert(sizeof(ker::mod::cpu::GPRegs) == 120);
static_assert(sizeof(ker::mod::gates::InterruptFrame) == 56);
static_assert(offsetof(ker::mod::gates::InterruptFrame, int_num) == 0);
static_assert(offsetof(ker::mod::gates::InterruptFrame, err_code) == 8);
static_assert(offsetof(ker::mod::gates::InterruptFrame, rip) == 16);
static_assert(offsetof(ker::mod::gates::InterruptFrame, cs) == 24);
namespace {
struct SavedInterruptFrame {
    ker::mod::cpu::GPRegs registers;
    ker::mod::gates::InterruptFrame frame;
};
static_assert(offsetof(SavedInterruptFrame, frame) == 120);
}  // namespace

namespace ker::mod::cpu {

auto try_read_msr(uint32_t index, uint64_t& output) -> bool {
    CpuidContext features{};
    features.function = 1;
    cpuid(&features);
    if ((static_cast<uint32_t>(features.edx) & CPUID_FEAT_EDX_MSR) == 0 || !desc::idt::kernel_idt_loaded_on_current_cpu()) return false;
    return wos_optional_msr_read(index, &output);
}

auto try_recover_msr_read(uint64_t vector, uint64_t error, uint64_t cs, uint64_t& rip) -> bool {
    if (vector != 13 || error != 0 || (cs & 3U) != 0 || rip != reinterpret_cast<uint64_t>(&wos_optional_msr_fault)) return false;
    rip = reinterpret_cast<uint64_t>(&wos_optional_msr_recovery);
    return true;
}

#ifdef WOS_SELFTEST
[[noreturn]] void selftest_unmatched_msr_fault() { wos_selftest_unmatched_msr_fault(); }
#endif

}  // namespace ker::mod::cpu

extern "C" auto wos_try_recover_msr_frame(ker::mod::gates::InterruptFrame* frame) -> bool {
    if (frame == nullptr) return false;
    uint64_t rip = frame->rip;
    if (!ker::mod::cpu::try_recover_msr_read(frame->int_num, frame->err_code, frame->cs, rip)) return false;
    frame->rip = rip;
    return true;
}
