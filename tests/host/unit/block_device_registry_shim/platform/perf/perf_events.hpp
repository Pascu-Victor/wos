#pragma once

#include <cstdint>

namespace ker::mod::perf {
enum class PerfSubsystem { DEVICE_REG, BLOCK_DEV };
constexpr int PERF_FLAG_CT_INSERT = 1;
constexpr int PERF_FLAG_CT_REMOVE = 2;
void registry_accounting_observer(PerfSubsystem subsystem, int operation, int64_t size);
inline void record_container_stat(int, int, PerfSubsystem subsystem, int, int operation, int64_t size, int, int) {
    registry_accounting_observer(subsystem, operation, size);
}
}  // namespace ker::mod::perf
