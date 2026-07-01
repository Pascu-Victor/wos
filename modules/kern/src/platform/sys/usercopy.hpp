#pragma once

#include <cstddef>
#include <cstdint>

#include "platform/sched/task.hpp"

namespace ker::mod::sys::usercopy {

inline constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;

[[nodiscard]] auto range_valid(uint64_t user_addr, size_t size) -> bool;
[[nodiscard]] auto ensure_writable(sched::task::Task& task, uint64_t user_addr, size_t size) -> bool;
[[nodiscard]] auto copy_from_task(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> bool;
[[nodiscard]] auto copy_to_task(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool;
[[nodiscard]] auto copy_to_task_mapped(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool;
[[nodiscard]] auto copy_cstring_from_task(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> bool;

template <typename T>
[[nodiscard]] auto copy_value_from_task(sched::task::Task& task, uint64_t user_addr, T& out) -> bool {
    return copy_from_task(task, user_addr, &out, sizeof(out));
}

template <typename T>
[[nodiscard]] auto copy_value_to_task(sched::task::Task& task, uint64_t user_addr, const T& value) -> bool {
    return copy_to_task(task, user_addr, &value, sizeof(value));
}

template <typename T>
[[nodiscard]] auto copy_value_to_task_mapped(sched::task::Task& task, uint64_t user_addr, const T& value) -> bool {
    return copy_to_task_mapped(task, user_addr, &value, sizeof(value));
}

}  // namespace ker::mod::sys::usercopy
