#include "cache_policy.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/asm/msr.hpp>

namespace ker::mod::mm::cache {
namespace {
constinit detail::PatReports reports{};

struct CpuIo {
    auto features() -> uint32_t {
        cpu::CpuidContext context{};
        context.function = 1;
        cpu::cpuid(&context);
        return static_cast<uint32_t>(context.edx);
    }
    auto read_pat() -> uint64_t {
        uint64_t raw = 0;
        cpu_get_msr(0x277, &raw);
        return raw;
    }
};
}  // namespace

auto configure_boot_cpu_count(size_t count) -> bool { return reports.configure(count); }
auto capture_boot_cpu(size_t index) -> bool {
    return reports.capture(index, [] {
        CpuIo io{};
        return detail::observe_pat(io);
    });
}
auto qualification() -> PatQualification { return reports.qualification(); }
auto cpu_pat(size_t index, CpuPat& out) -> bool { return reports.cpu_pat(index, out); }
}  // namespace ker::mod::mm::cache
