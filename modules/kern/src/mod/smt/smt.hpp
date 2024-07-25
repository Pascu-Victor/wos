#pragma once
#include <limine.h>

namespace ker::mod::smt
{
    void init(void);

    uint64_t getCoreCount(void);

    limine_smp_info* getCpu(uint64_t number);
    
}