#include "smt.hpp"

__attribute__((used, section(".requests")))
static volatile limine_smp_request smp_request = {
    .id = LIMINE_SMP_REQUEST,
    .revision = 0,
    .response = nullptr,
    .flags = 1,
};

namespace ker::mod::smt
{
    static inline bool isInit = false;

    limine_smp_response *smp_info;
    void init(void) {
        if (isInit) {
            return;
        }
        smp_info = smp_request.response;
        isInit = true;
    }

    uint64_t getCoreCount(void) {
        return smp_info->cpu_count;
    }

    limine_smp_info* getCpu(uint64_t number) {
        return smp_info->cpus[number];
    }
    
}