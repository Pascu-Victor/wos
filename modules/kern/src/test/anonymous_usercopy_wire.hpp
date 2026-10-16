#pragma once
#ifdef WOS_SELFTEST
#include <cstddef>
#include <cstdint>

namespace ker::test::anonymous_usercopy {
inline constexpr uint64_t PRIVATE_VMEM_OP = 0x47403402;
inline constexpr uint32_t VERSION = 1;
enum class Case : uint32_t { WRITE_PATTERN, READ_SUBRANGE, WRITE_PREFIX, FUTEX_ZERO_TIMEOUT };
struct Request {
    uint32_t version{}, bytes{}, case_id{}, flags{};
    uint64_t nonce{}, address{}, length{};
    int64_t fd{};
    uint64_t timeout_address{}, output_address{}, output_bytes{}, reserved{};
};
struct Report {
    uint64_t version{}, bytes{}, nonce{}, case_id{}, actor_pid{}, root_serial{}, checks{}, valid{};
    int64_t operation_result{}, expected_result{};
    uint64_t source_ref1{}, resident_ref1{}, selected_pages{}, used_before{}, used_swapped{}, used_after_copy{};
    uint64_t total_before{}, total_active{}, total_after_cleanup{}, free_active{}, used_after_cleanup{};
    uint64_t block_writes{}, block_reads{}, history_overflow{}, pattern_bytes_checked{}, guard_bytes_checked{};
    uint64_t mapping_consumed{}, cleanup_complete{}, task_access_after{}, root_readers_after{}, futex_waiter_after{}, reserved{};
};
static_assert(sizeof(Request) == 80 && offsetof(Request, nonce) == 16 && offsetof(Request, fd) == 40 &&
              offsetof(Request, timeout_address) == 48 && offsetof(Request, output_address) == 56 && offsetof(Request, reserved) == 72);
static_assert(sizeof(Report) == 256 && offsetof(Report, operation_result) == 64 && offsetof(Report, expected_result) == 72 &&
              offsetof(Report, source_ref1) == 80 && offsetof(Report, reserved) == 248);
}  // namespace ker::test::anonymous_usercopy
#endif
