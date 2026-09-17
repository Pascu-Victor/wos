#pragma once

#include <cstdint>

namespace ker::mod::perf {
enum class PerfSubsystem { DEVICE_REG };
constexpr int PERF_FLAG_CT_INSERT = 1;
constexpr int PERF_FLAG_CT_REMOVE = 2;
void registry_accounting_observer(int operation, int64_t size);
inline void record_container_stat(int, int, PerfSubsystem, int, int operation, int64_t size, int, int) {
    registry_accounting_observer(operation, size);
}
}  // namespace ker::mod::perf
