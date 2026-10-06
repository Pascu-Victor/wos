#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/sched/task.hpp>

namespace ker::mod::sys::usercopy {
inline bool block_test_fail_copy{};
inline auto copy_from_task_mapped(sched::task::Task&, uint64_t address, void* destination, size_t bytes) -> bool {
    if (block_test_fail_copy) return false;
    std::memcpy(destination, reinterpret_cast<const void*>(address), bytes);
    return true;
}
inline auto copy_to_task_mapped(sched::task::Task&, uint64_t address, const void* source, size_t bytes) -> bool {
    if (block_test_fail_copy) return false;
    std::memcpy(reinterpret_cast<void*>(address), source, bytes);
    return true;
}
}  // namespace ker::mod::sys::usercopy
