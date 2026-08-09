#pragma once

#include <cstddef>
#include <cstdint>

#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"

namespace ker::mod::sys::usercopy {

inline constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;

struct CopyResult {
    size_t bytes_copied{};
    bool fault{};

    [[nodiscard]] auto complete(size_t requested) const -> bool { return !fault && bytes_copied == requested; }
};

enum class CStringCopyStatus : uint8_t {
    COMPLETE,
    FAULT,
    TOO_LONG,
};

// Holds both the target Task pagemap-access gate and one allocator frame
// reference. The HHDM address therefore remains safe even if another thread
// removes the mapping; still_mapped() supplies the operation's final
// linearization check before callers commit or report progress.
class StableUserPage {
   public:
    StableUserPage() = default;
    ~StableUserPage();

    StableUserPage(const StableUserPage&) = delete;
    StableUserPage(StableUserPage&&) = delete;
    auto operator=(const StableUserPage&) -> StableUserPage& = delete;
    auto operator=(StableUserPage&&) -> StableUserPage& = delete;

    [[nodiscard]] auto valid() const -> bool;
    [[nodiscard]] auto kernel_address() const -> void*;
    [[nodiscard]] auto physical_address() const -> uint64_t;
    [[nodiscard]] auto was_dirty() const -> bool;
    [[nodiscard]] auto still_mapped() const -> bool;
    [[nodiscard]] auto commit_write() const -> bool;

   private:
    friend auto pin_task_user_page(sched::task::Task&, uint64_t, bool, bool, StableUserPage&) -> bool;
    void reset();

    sched::task::Task* m_task{};
    mm::virt::UserPagePin m_pin{};
    uint64_t m_user_address{};
};

[[nodiscard]] auto range_valid(uint64_t user_addr, size_t size) -> bool;
[[nodiscard]] auto pin_task_user_page(sched::task::Task& task, uint64_t user_addr, bool require_writable, bool fault_in,
                                      StableUserPage& out) -> bool;
// Stable no-fault translation for diagnostics only. The returned physical
// address is a snapshot and must never be dereferenced after this function
// releases its pin.
[[nodiscard]] auto mapped_physical_address(sched::task::Task& task, uint64_t user_addr, bool require_writable = false) -> uint64_t;
[[nodiscard]] auto ensure_writable(sched::task::Task& task, uint64_t user_addr, size_t size) -> bool;
[[nodiscard]] auto copy_from_task_partial(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> CopyResult;
[[nodiscard]] auto copy_to_task_partial(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> CopyResult;
[[nodiscard]] auto copy_to_task_mapped_partial(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> CopyResult;
[[nodiscard]] auto copy_from_task(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> bool;
// No-fault variant for diagnostic/unsafe contexts. Every covered page must
// already be a present user mapping; lazy backing is never installed.
[[nodiscard]] auto copy_from_task_mapped(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> bool;
[[nodiscard]] auto copy_to_task(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool;
[[nodiscard]] auto copy_to_task_mapped(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool;
[[nodiscard]] auto copy_cstring_from_task_status(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size)
    -> CStringCopyStatus;
[[nodiscard]] auto copy_cstring_from_task(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> bool;
[[nodiscard]] auto copy_cstring_from_task_strict(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> bool;

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
