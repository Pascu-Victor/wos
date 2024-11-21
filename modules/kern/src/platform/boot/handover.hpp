#pragma once
#include <defines/defines.hpp>

namespace ker::mod::boot {

struct HandoverModule {
    void* entry;
    uint64_t size;
    const char* cmdline;
    const char* name;
};

struct HandoverModules {
    uint64_t count;
    HandoverModule modules[32];
};

}  // namespace ker::mod::boot
