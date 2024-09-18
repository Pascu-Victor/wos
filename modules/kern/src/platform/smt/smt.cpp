#include "smt.hpp"

__attribute__((used, section(".requests"))) const static volatile limine_smp_request smp_request = {
    .id = LIMINE_SMP_REQUEST,
    .revision = 0,
    .response = nullptr,
    .flags = 1,
};
namespace ker::mod::smt {
extern "C" limine_smp_info _wOS_CPUDATA[SMT_MAX_CPUS];
uint64_t revision;
uint32_t flags;
uint32_t bsp_lapic_id;
uint64_t cpu_count;

uint64_t getCoreCount(void) { return cpu_count; }

const limine_smp_info& getCpu(uint64_t number) { return _wOS_CPUDATA[number]; }

const limine_smp_info& thisCpuInfo() { return _wOS_CPUDATA[cpu::currentCpu()]; }

// Future NUMA support here
static inline uint64_t getCpuNode(uint64_t cpuNo) { return cpuNo; }

// init per cpu data
void init(void) {
    revision = smp_request.revision;
    flags = smp_request.response->flags;
    bsp_lapic_id = smp_request.response->bsp_lapic_id;
    cpu_count = smp_request.response->cpu_count;

    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        _wOS_CPUDATA[i] = *smp_request.response->cpus[i];
    }
}
}  // namespace ker::mod::smt
