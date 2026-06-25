#pragma once

// Host shim: provides an opaque Task type so WKI headers that reference
// Task* compile on the host.  No real scheduling is needed.

#include <cstdint>

namespace ker::mod::sched::task {

struct Task {
    uint64_t id = 0;
    uint64_t pid = 0;
};

}  // namespace ker::mod::sched::task
