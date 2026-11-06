#include <platform/asm/cpu.hpp>
#include <platform/asm/cpu_safe_msr.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/idt.hpp>
#include <platform/mm/cache_policy.hpp>
#include <test/ktest.hpp>
#include <test/safe_msr_ktest.hpp>

namespace {
auto flags() -> uint64_t {
    uint64_t value = 0;
    asm volatile("pushfq; popq %0" : "=r"(value) : : "memory");
    return value;
}
auto has_required_features() -> bool {
    ker::mod::cpu::CpuidContext features{};
    features.function = 1;
    ker::mod::cpu::cpuid(&features);
    return (static_cast<uint32_t>(features.edx) & (CPUID_FEAT_EDX_MSR | CPUID_FEAT_EDX_PAT)) == (CPUID_FEAT_EDX_MSR | CPUID_FEAT_EDX_PAT) &&
           (static_cast<uint32_t>(features.ecx) & (uint32_t{1} << 21)) != 0;
}
}  // namespace

KTEST(SafeMsr, KnownPatUsesInstalledLocalIdt) {
    KREQUIRE_TRUE(has_required_features());
    KREQUIRE_TRUE(ker::mod::desc::idt::kernel_idt_loaded_on_current_cpu());
    ker::mod::mm::cache::CpuPat retained{};
    KREQUIRE_TRUE(ker::mod::mm::cache::cpu_pat(static_cast<size_t>(ker::mod::cpu::get_current_cpu_id_safe()), retained));
    KREQUIRE_TRUE(retained.supported);
    uint64_t value = UINT64_MAX;
    uint64_t const BEFORE = flags();
    bool const RESULT = ker::mod::cpu::try_read_msr(0x277, value);
    uint64_t const AFTER = flags();
    KREQUIRE_TRUE(RESULT);
    KEXPECT_EQ(value, retained.raw);
    KEXPECT_EQ(AFTER & ker::mod::cpu::GATE_IF_MASK, BEFORE & ker::mod::cpu::GATE_IF_MASK);
}

KTEST(SafeMsr, WriteOnlySelfIpiReadPreservesOutputAndIf) {
    KREQUIRE_TRUE(has_required_features());
    KREQUIRE_TRUE(ker::mod::desc::idt::kernel_idt_loaded_on_current_cpu());
    uint64_t apic_base = 0;
    KREQUIRE_TRUE(ker::mod::cpu::try_read_msr(IA32_APIC_BASE, apic_base));
    constexpr uint64_t X2APIC_MODE = (uint64_t{1} << 10) | (uint64_t{1} << 11);
    KREQUIRE_EQ(apic_base & X2APIC_MODE, X2APIC_MODE);
    for (unsigned attempt = 0; attempt < 32; ++attempt) {
        uint64_t value = 0xfedcba9876543210ULL ^ attempt;
        uint64_t const SENTINEL = value;
        uint64_t const INCOMING = flags();
        // Isolated KTEST parks APs before scheduler startup with valid BSP
        // GS/IDT, as in ivshmem_pci_ktest. Exercise both incoming IF values,
        // then restore the actual caller's IF before assertions or logging.
        bool const ENABLED = (attempt & 1U) != 0;
        if (ENABLED)
            asm volatile("sti" ::: "memory");
        else
            asm volatile("cli" ::: "memory");
        uint64_t const BEFORE = flags();
        // Intel SDM Table13-6 and AMD APM16.15 both require #GP(0) for a
        // read of write-only SELF_IPI. Unknown MSRs may be ignored by a VMM.
        bool const RESULT = ker::mod::cpu::try_read_msr(0x83f, value);
        uint64_t const AFTER = flags();
        if ((INCOMING & ker::mod::cpu::GATE_IF_MASK) != 0)
            asm volatile("sti" ::: "memory");
        else
            asm volatile("cli" ::: "memory");
        uint64_t const RESTORED = flags();
        KEXPECT_FALSE(RESULT);
        KEXPECT_EQ(value, SENTINEL);
        KEXPECT_EQ((BEFORE & ker::mod::cpu::GATE_IF_MASK) != 0, ENABLED);
        KEXPECT_EQ(AFTER & ker::mod::cpu::GATE_IF_MASK, BEFORE & ker::mod::cpu::GATE_IF_MASK);
        KEXPECT_EQ(RESTORED & ker::mod::cpu::GATE_IF_MASK, INCOMING & ker::mod::cpu::GATE_IF_MASK);
    }
}

namespace ker::test {
void run_safe_msr_selftest() {
    using log = ker::mod::dbg::logger<"safe_msr">;
    g_pass = 0;
    g_fail = 0;
    auto run = [](const char* name, void (*body)()) {
        int const BEFORE_PASS = g_pass;
        int const BEFORE_FAIL = g_fail;
        log::info("SAFE_MSR_RUNTIME RUN %s", name);
        body();
        if (g_pass == BEFORE_PASS && g_fail == BEFORE_FAIL) ++g_fail;
        if (g_fail == BEFORE_FAIL)
            log::info("SAFE_MSR_RUNTIME CASE_PASS %s", name);
        else
            log::error("SAFE_MSR_RUNTIME CASE_FAIL %s", name);
    };
    run("SafeMsr/KnownPatUsesInstalledLocalIdt", _kt_SafeMsr_KnownPatUsesInstalledLocalIdt);
    run("SafeMsr/WriteOnlySelfIpiReadPreservesOutputAndIf", _kt_SafeMsr_WriteOnlySelfIpiReadPreservesOutputAndIf);
    log::info("SAFE_MSR_RUNTIME %s cases=2 assertions=%u failures=%u", g_fail == 0 ? "PASS" : "FAIL",
              static_cast<unsigned>(g_pass + g_fail), static_cast<unsigned>(g_fail));
    if (g_fail != 0) ker::mod::dbg::panic_handler("safe MSR native selftest failed");
}
}  // namespace ker::test
