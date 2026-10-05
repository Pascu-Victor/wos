#include "kvmclock.hpp"

#include <new>
#include <platform/acpi/hpet/hpet.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/msr.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>

namespace ker::mod::time::kvmclock {
namespace {
constinit Runtime runtime;

struct Cpuid {
    uint32_t eax;
    uint32_t ebx;
    uint32_t ecx;
    uint32_t edx;
};
auto leaf(uint32_t function) -> Cpuid {
    Cpuid result{};
    asm volatile("cpuid" : "=a"(result.eax), "=b"(result.ebx), "=c"(result.ecx), "=d"(result.edx) : "a"(function), "c"(0) : "memory");
    return result;
}
auto available() -> bool {
    if (leaf(0).eax < 1) return false;
    auto const BASIC = leaf(1);
    if ((BASIC.ecx & (1U << 31)) == 0 ||
        (BASIC.edx & (CPUID_FEAT_EDX_TSC | CPUID_FEAT_EDX_MSR)) != (CPUID_FEAT_EDX_TSC | CPUID_FEAT_EDX_MSR))
        return false;
    if (leaf(0x80000000U).eax < 0x80000001U || (leaf(0x80000001U).edx & (1U << 27)) == 0) return false;
    auto const SIGNATURE = leaf(0x40000000);
    // Never issue a feature leaf/MSR for a different hypervisor signature.
    if (!supported(SIGNATURE.eax, SIGNATURE.ebx, SIGNATURE.ecx, SIGNATURE.edx, REQUIRED_FEATURES)) return false;
    return supported(SIGNATURE.eax, SIGNATURE.ebx, SIGNATURE.ecx, SIGNATURE.edx, leaf(0x40000001).eax);
}

struct Hardware {
    auto available() -> bool { return kvmclock::available(); }
    auto allocate() -> TimeInfo* {
        // Hypervisor CPU metadata is retained until shutdown and has no PCI
        // device, DMA registry entry, or IOMMU mapping to retire.
        void* const PAGE = mm::phys::page_alloc_may_fail(mm::PhysicalPageOwner::KVM_CLOCK, mm::paging::PAGE_SIZE, "kvmclock-vcpu");
        return PAGE == nullptr ? nullptr : new (PAGE) TimeInfo{};
    }
    void register_page(TimeInfo& page) {
        auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(&page)));
        std::atomic_thread_fence(std::memory_order_seq_cst);
        cpu_set_msr(SYSTEM_TIME_MSR, PHYSICAL | 1U);
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
    auto counter() -> uint64_t {
        uint32_t lo = 0;
        uint32_t hi = 0;
        asm volatile("rdtscp; lfence" : "=a"(lo), "=d"(hi) : : "ecx", "memory");
        return (static_cast<uint64_t>(hi) << 32) | lo;
    }
    auto reference() -> uint64_t { return hpet::get_ns(); }
};

// Pin the vCPU record and its counter to the same CPU without any scheduler
// dependency; clock reads also occur before task/runqueue initialization.
class IrqGuard {
    uint64_t flags_{};

   public:
    IrqGuard() { asm volatile("pushfq; pop %0; cli" : "=r"(flags_) : : "memory"); }
    ~IrqGuard() { asm volatile("push %0; popfq" : : "r"(flags_) : "memory", "cc"); }
};
}  // namespace

void prepare(uint64_t cpus) { runtime.prepare(available(), cpus); }
void register_current_cpu(uint64_t cpu, bool bootstrap) {
    IrqGuard const GUARD;
    Hardware hardware;
    runtime.register_cpu(cpu, bootstrap, hardware);
}
auto read(uint64_t& result) -> bool {
    IrqGuard const GUARD;
    Hardware hardware;
    return runtime.read(cpu::current_cpu_fast(), false, hardware, result);
}
void watchdog_tick() {
    if (runtime.mode() != Mode::active) return;
    IrqGuard const GUARD;
    Hardware hardware;
    uint64_t ignored = 0;
    (void)runtime.read(cpu::current_cpu_fast(), true, hardware, ignored);
}
auto mode() -> Mode { return runtime.mode(); }
auto registered() -> uint64_t { return runtime.registered(); }
auto expected() -> uint64_t { return runtime.expected(); }
auto deviation() -> uint64_t { return runtime.deviation(); }
auto checks() -> uint64_t { return runtime.checks(); }
auto fallback_hpet_time(uint64_t reference) -> uint64_t { return runtime.fallback(reference); }
}  // namespace ker::mod::time::kvmclock
