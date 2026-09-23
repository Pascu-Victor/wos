#include "cpu.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/acpi/apic/apic.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/smt/smt.hpp>
#include <util/hcf.hpp>

extern "C" uint8_t wos_per_cpu_ready_state = 0;
extern "C" uint8_t wos_smap_active = 0;

namespace ker::mod::cpu {

namespace {
uint64_t kernel_stack_guard{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
alignas(PerCpu) std::byte bootstrap_per_cpu_storage[sizeof(PerCpu)]{};
PerCpu* bootstrap_per_cpu_context{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> supervisor_policy_ready{};
SupervisorProtectionState supervisor_policy{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

__attribute__((no_stack_protector)) auto early_hardware_entropy(uint64_t& value) -> bool {
    constexpr unsigned RETRIES = 64;
    uint32_t max_leaf = 0;
    uint32_t unused_b = 0;
    uint32_t unused_c = 0;
    uint32_t unused_d = 0;
    asm volatile("cpuid" : "=a"(max_leaf), "=b"(unused_b), "=c"(unused_c), "=d"(unused_d) : "a"(0), "c"(0) : "memory");

    bool has_rdseed = false;
    bool has_rdrand = false;
    if (max_leaf >= 1) {
        uint32_t features_c = 0;
        asm volatile("cpuid" : "=a"(unused_b), "=b"(unused_c), "=c"(features_c), "=d"(unused_d) : "a"(1), "c"(0) : "memory");
        has_rdrand = (features_c & (uint32_t{1} << 30U)) != 0U;
    }
    if (max_leaf >= 7) {
        uint32_t features_b = 0;
        asm volatile("cpuid" : "=a"(unused_c), "=b"(features_b), "=c"(unused_d), "=d"(unused_b) : "a"(7), "c"(0) : "memory");
        has_rdseed = (features_b & (uint32_t{1} << 18U)) != 0U;
    }

    for (unsigned attempt = 0; attempt < RETRIES; ++attempt) {
        unsigned char ok = 0;
        if (has_rdseed) {
            asm volatile("rdseed %0; setc %1" : "=&r"(value), "=qm"(ok)::"memory");
        }
        if (ok == 0 && has_rdrand) {
            asm volatile("rdrand %0; setc %1" : "=&r"(value), "=qm"(ok)::"memory");
        }
        if (ok != 0) {
            return true;
        }
        asm volatile("pause" ::: "memory");
    }
    return false;
}
}  // namespace

__attribute__((no_stack_protector)) auto stack_protector_guard() -> uint64_t { return kernel_stack_guard; }

void initialize_stack_guard(PerCpu& per_cpu) { per_cpu.stack_guard = kernel_stack_guard; }

extern "C" __attribute__((no_stack_protector)) auto wos_stack_protector_bootstrap() -> bool {
    bootstrap_per_cpu_context = new (bootstrap_per_cpu_storage) PerCpu{};
    auto const CONTEXT = reinterpret_cast<uint64_t>(bootstrap_per_cpu_context);
    cpu_set_msr(IA32_GS_BASE, CONTEXT);
    cpu_set_msr(IA32_KERNEL_GS_BASE, CONTEXT);

    uint64_t guard = 0;
    if (!early_hardware_entropy(guard)) {
        return false;
    }
    guard &= ~uint64_t{0xff};
    if (guard == 0) {
        return false;
    }
    kernel_stack_guard = guard;
    bootstrap_per_cpu_context->stack_guard = guard;
    return true;
}

extern "C" __attribute__((no_stack_protector)) void wos_stack_protector_install_ap_bootstrap() {
    auto const CONTEXT = reinterpret_cast<uint64_t>(bootstrap_per_cpu_context);
    cpu_set_msr(IA32_GS_BASE, CONTEXT);
    cpu_set_msr(IA32_KERNEL_GS_BASE, CONTEXT);
}

#ifdef WOS_SELFTEST
[[gnu::noinline, clang::no_sanitize("address", "undefined", "coverage")]] void selftest_corrupt_stack_canary() {
    volatile uint8_t protected_buffer[32]{};
    asm volatile("" : : "r"(protected_buffer) : "memory");
    asm volatile("movq $0, -8(%%rbp)" ::: "memory");
    asm volatile("" : : "r"(protected_buffer) : "memory");
}
#endif

[[clang::no_sanitize("coverage")]] auto get_current_cpu_id_safe() -> uint64_t {
    if (per_cpu_ready_flag_acquire()) {
        return current_cpu_fast();
    }
    // Early boot: use APIC ID
    uint32_t const APIC_ID = apic::get_apic_id();
    if (smt::has_cpu_data()) {
        return smt::get_cpu_index_from_apic_id(APIC_ID);
    }
    return 0;  // BSP during very early init
}

[[clang::no_sanitize("coverage")]] auto is_per_cpu_ready() -> bool { return per_cpu_ready_flag_acquire(); }

void notify_per_cpu_ready() { __atomic_store_n(&wos_per_cpu_ready_state, static_cast<uint8_t>(1), __ATOMIC_RELEASE); }
void cpuid(struct CpuidContext* cpuid_context) {
    asm volatile("cpuid"
                 : "=a"(cpuid_context->eax), "=b"(cpuid_context->ebx), "=c"(cpuid_context->ecx), "=d"(cpuid_context->edx)
                 : "a"(cpuid_context->function));
}

[[clang::no_sanitize("coverage")]] uint64_t current_cpu() { return current_cpu_fast(); }

[[clang::no_sanitize("coverage")]] void set_current_cpuid(uint64_t id) {
    // Write cpuId to gs:0x10 (offset of cpuId in PerCpu structure)
    asm volatile("mov %0, %%gs:0x10" ::"r"(id) : "memory");
}

void enable_pae() {
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    cr4 |= 1 << 5;  // PAE
    wrcr4(cr4);
}

void enable_pse() {
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    cr4 |= 1 << 4;  // PSE
    wrcr4(cr4);
}

auto enable_nx() -> bool {
    constexpr uint32_t EXTENDED_LEAF_MAX = 0x80000000U;
    constexpr uint32_t EXTENDED_FEATURES = 0x80000001U;
    constexpr uint32_t CPUID_NX = uint32_t{1} << 20U;
    constexpr uint64_t EFER_NXE = uint64_t{1} << 11U;

    CpuidContext max_leaf{};
    max_leaf.function = static_cast<int>(EXTENDED_LEAF_MAX);
    cpuid(&max_leaf);
    if (static_cast<uint32_t>(max_leaf.eax) < EXTENDED_FEATURES) {
        return false;
    }

    CpuidContext features{};
    features.function = static_cast<int>(EXTENDED_FEATURES);
    cpuid(&features);
    if ((static_cast<uint32_t>(features.edx) & CPUID_NX) == 0U) {
        return false;
    }

    uint64_t efer = 0;
    cpu_get_msr(IA32_EFER, &efer);
    cpu_set_msr(IA32_EFER, efer | EFER_NXE);
    return true;
}

auto nx_enabled() -> bool {
    constexpr uint64_t EFER_NXE = uint64_t{1} << 11U;
    uint64_t efer = 0;
    cpu_get_msr(IA32_EFER, &efer);
    return (efer & EFER_NXE) != 0U;
}

void enable_write_protect() {
    constexpr uint64_t CR0_WP = uint64_t{1} << 16U;
    uint64_t cr0 = 0;
    asm volatile("mov %%cr0, %0" : "=r"(cr0)::"memory");
    cr0 |= CR0_WP;
    asm volatile("mov %0, %%cr0" : : "r"(cr0) : "memory");
}

auto write_protect_enabled() -> bool {
    constexpr uint64_t CR0_WP = uint64_t{1} << 16U;
    uint64_t cr0 = 0;
    asm volatile("mov %%cr0, %0" : "=r"(cr0)::"memory");
    return (cr0 & CR0_WP) != 0U;
}

auto supervisor_protection_state() -> SupervisorProtectionState {
    constexpr uint32_t BASIC_FEATURE_LEAF = 7;
    constexpr uint32_t CPUID_SMEP = uint32_t{1} << 7U;
    constexpr uint32_t CPUID_SMAP = uint32_t{1} << 20U;
    constexpr uint64_t CR4_SMEP = uint64_t{1} << 20U;
    constexpr uint64_t CR4_SMAP = uint64_t{1} << 21U;

    CpuidContext max_leaf{};
    max_leaf.function = 0;
    cpuid(&max_leaf);
    uint32_t features = 0;
    if (static_cast<uint32_t>(max_leaf.eax) >= BASIC_FEATURE_LEAF) {
        uint32_t eax = 0;
        uint32_t ecx = 0;
        uint32_t edx = 0;
        asm volatile("cpuid" : "=a"(eax), "=b"(features), "=c"(ecx), "=d"(edx) : "a"(BASIC_FEATURE_LEAF), "c"(0) : "memory");
    }
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    return {
        .smep_supported = (features & CPUID_SMEP) != 0U,
        .smep_enabled = (cr4 & CR4_SMEP) != 0U,
        .smap_supported = (features & CPUID_SMAP) != 0U,
        .smap_enabled = (cr4 & CR4_SMAP) != 0U,
    };
}

auto enable_supervisor_protections() -> SupervisorProtectionState {
    constexpr uint64_t CR4_SMEP = uint64_t{1} << 20U;
    constexpr uint64_t CR4_SMAP = uint64_t{1} << 21U;
    SupervisorProtectionState const SUPPORTED = supervisor_protection_state();

    // Clear AC before SMAP becomes active. popfq is valid regardless of SMAP
    // support, unlike executing CLAC on a CPU that lacks the instruction.
    uint64_t flags = 0;
    asm volatile("pushfq; pop %0" : "=r"(flags)::"memory");
    flags &= ~(uint64_t{1} << 18U);
    asm volatile("push %0; popfq" : : "r"(flags) : "cc", "memory");

    uint64_t cr4 = 0;
    rdcr4(&cr4);
    if (SUPPORTED.smep_supported) {
        cr4 |= CR4_SMEP;
    }
    if (SUPPORTED.smap_supported) {
        cr4 |= CR4_SMAP;
    }
    wrcr4(cr4);
    return supervisor_protection_state();
}

void publish_supervisor_protection_policy(const SupervisorProtectionState& state) {
    supervisor_policy = state;
    __atomic_store_n(&wos_smap_active, state.smap_enabled ? uint8_t{1} : uint8_t{0}, __ATOMIC_RELEASE);
    supervisor_policy_ready.store(true, std::memory_order_release);
}

auto supervisor_protection_policy_matches(const SupervisorProtectionState& state) -> bool {
    return supervisor_policy_ready.load(std::memory_order_acquire) && state.smep_supported == supervisor_policy.smep_supported &&
           state.smep_enabled == supervisor_policy.smep_enabled && state.smap_supported == supervisor_policy.smap_supported &&
           state.smap_enabled == supervisor_policy.smap_enabled;
}

auto smep_enabled() -> bool { return supervisor_protection_state().smep_enabled; }

auto smap_enabled() -> bool { return supervisor_protection_state().smap_enabled; }

void enable_fsgsbase() {
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    cr4 |= 1 << 16;  // FSGSBASE
    wrcr4(cr4);
}

extern "C" void wos_enable_sse_asm(void);
extern "C" auto wos_enable_xsave_asm(void) -> uint64_t;

void enable_sse() { wos_enable_sse_asm(); }

uint64_t xsave_area_size = 0;
uint64_t xsave_feature_mask = 0;

namespace {

struct CpuidLeaf {
    uint32_t eax = 0;
    uint32_t ebx = 0;
    uint32_t ecx = 0;
    uint32_t edx = 0;
};

auto cpuid_count(uint32_t leaf, uint32_t subleaf) -> CpuidLeaf {
    CpuidLeaf out{};
    asm volatile("cpuid" : "=a"(out.eax), "=b"(out.ebx), "=c"(out.ecx), "=d"(out.edx) : "a"(leaf), "c"(subleaf) : "memory");
    return out;
}

auto read_xcr0() -> uint64_t {
    uint32_t eax = 0;
    uint32_t edx = 0;
    asm volatile("xgetbv" : "=a"(eax), "=d"(edx) : "c"(0) : "memory");
    return static_cast<uint64_t>(eax) | (static_cast<uint64_t>(edx) << 32U);
}

void write_xcr0(uint64_t mask) {
    auto const EAX = static_cast<uint32_t>(mask);
    auto const EDX = static_cast<uint32_t>(mask >> 32U);
    asm volatile("xsetbv" : : "a"(EAX), "d"(EDX), "c"(0) : "memory");
}

auto read_cr4() -> uint64_t {
    uint64_t value = 0;
    asm volatile("mov %%cr4, %0" : "=r"(value)::"memory");
    return value;
}

void write_cr4(uint64_t value) { asm volatile("mov %0, %%cr4" ::"r"(value) : "memory"); }

auto current_xsave_size() -> uint64_t {
    CpuidLeaf const LEAF = cpuid_count(0x0D, 0);
    return LEAF.ebx;
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n') {
            ++cursor;
        }
        if (*cursor == '\0') {
            break;
        }

        const char* start = cursor;
        size_t segment_len = 0;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && *cursor != '\n') {
            ++cursor;
            ++segment_len;
        }
        if (segment_len == TOKEN_LEN && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto xsave_avx_disabled_by_cmdline() -> bool {
    static std::atomic<int> s_disabled{-1};
    int const CACHED = s_disabled.load(std::memory_order_acquire);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    bool const DISABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "cpu.no_avx");
    int expected = -1;
    if (s_disabled.compare_exchange_strong(expected, DISABLED ? 1 : 0, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (DISABLED) {
            dbg::logger<"cpu">::warn("AVX xstate disabled by cmdline; XSAVE remains enabled for x87/SSE");
        }
        return DISABLED;
    }

    return expected != 0;
}

auto xsave_disabled_by_cmdline() -> bool {
    static std::atomic<int> s_disabled{-1};
    int const CACHED = s_disabled.load(std::memory_order_acquire);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    bool const DISABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "cpu.no_xsave");
    int expected = -1;
    if (s_disabled.compare_exchange_strong(expected, DISABLED ? 1 : 0, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (DISABLED) {
            dbg::logger<"cpu">::warn("XSAVE disabled by cmdline; using FXSAVE/FXRSTOR for x87/SSE state");
        }
        return DISABLED;
    }

    return expected != 0;
}

void disable_xsave_hardware_state() {
    constexpr uint64_t CR4_OSXSAVE = 1ULL << 18U;
    uint64_t const CR4 = read_cr4();
    if ((CR4 & CR4_OSXSAVE) == 0) {
        xsave_feature_mask = 0;
        xsave_area_size = 0;
        return;
    }

    uint64_t const XCR0 = read_xcr0();
    uint64_t const LEGACY_XCR0 = XCR0 & XSAVE_LEGACY_MASK;
    if (LEGACY_XCR0 != XCR0) {
        write_xcr0(LEGACY_XCR0 != 0 ? LEGACY_XCR0 : XSAVE_LEGACY_MASK);
    }

    write_cr4(CR4 & ~CR4_OSXSAVE);
    xsave_feature_mask = 0;
    xsave_area_size = 0;
    dbg::logger<"cpu">::warn("OSXSAVE cleared by cmdline; userspace AVX/XSAVE disabled");
}

void publish_xsave_config(uint64_t mask, uint64_t size) {
    if ((mask & XSAVE_LEGACY_MASK) != XSAVE_LEGACY_MASK || size == 0 || size > XSAVE_STATIC_AREA_SIZE) {
        dbg::logger<"cpu">::error("invalid XSAVE config: mask=0x%llx size=%llu max=%llu", static_cast<unsigned long long>(mask),
                                  static_cast<unsigned long long>(size), static_cast<unsigned long long>(XSAVE_STATIC_AREA_SIZE));
        hcf();
    }

    if (xsave_feature_mask == 0) {
        xsave_feature_mask = mask;
        xsave_area_size = size;
        dbg::logger<"cpu">::info("XSAVE enabled: mask=0x%llx size=%llu", static_cast<unsigned long long>(mask),
                                 static_cast<unsigned long long>(size));
        return;
    }

    if (xsave_feature_mask != mask || xsave_area_size != size) {
        dbg::logger<"cpu">::error("inconsistent XSAVE config: prev_mask=0x%llx prev_size=%llu mask=0x%llx size=%llu",
                                  static_cast<unsigned long long>(xsave_feature_mask), static_cast<unsigned long long>(xsave_area_size),
                                  static_cast<unsigned long long>(mask), static_cast<unsigned long long>(size));
        hcf();
    }
}

}  // namespace

void enable_xsave() {
    if (xsave_disabled_by_cmdline()) {
        disable_xsave_hardware_state();
        return;
    }

    if (wos_enable_xsave_asm() == 0) {
        return;
    }

    uint64_t mask = read_xcr0() & XSAVE_SUPPORTED_MASK;
    if (xsave_avx_disabled_by_cmdline()) {
        mask &= ~XSAVE_AVX_MASK;
        mask |= XSAVE_LEGACY_MASK;
    }
    if (mask != read_xcr0()) {
        write_xcr0(mask);
    }

    publish_xsave_config(mask, current_xsave_size());
}

}  // namespace ker::mod::cpu
