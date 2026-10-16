#pragma once
// Private kernel/fixture protocol. Never installed in the public SDK.
#ifdef WOS_SELFTEST
#include <cstddef>
#include <cstdint>
#include <test/numa_lifecycle_mm_debt.hpp>
namespace ker::mod::sched::task {
struct Task;
}
namespace ker::test::numa_lifecycle {
inline constexpr uint64_t PRIVATE_VMEM_OP = 0x47403401;
inline constexpr uint32_t VERSION = 1;
inline constexpr uint64_t MAX_REPORT_BYTES = 128 * 1024;
inline constexpr uint64_t MAX_FRAMES = 8 * 1024 * 1024;
inline constexpr size_t MAX_NODES = 64;
inline constexpr size_t OWNER_COUNT = 41;  // Actual PhysicalPageOwner::COUNT; assert against source enum.
enum class Phase : uint32_t { PREPARE = 0, BEGIN = 1, END = 2 };
enum class Outcome : uint64_t { WAIT = 0, ACCEPT = 1, REFUSE = 2 };
enum class Reason : uint64_t {
    NONE = 0,
    ADMISSION = 1,
    COOKIE = 2,
    ROOT_CHANGED = 3,
    DEADLINE = 4,
    COVERAGE = 5,
    COUNTER_INVALID = 6,
    OWNER_CHURN = 7,
    MM_CHURN = 8,
    PROVIDER_DEBT = 9,
    BASELINE_DEBT = 10,
    PHYSICAL = 11,
    TRANSPORT = 12
};
enum Coverage : uint64_t {
    TYPED_STORAGE = 1ULL << 0,
    OWNER_EPOCHS = 1ULL << 1,
    CURRENT_FD_AND_POPULATION = 1ULL << 2,
    ROOTS_AND_FILE_BINDINGS = 1ULL << 3,
    PIN_AND_RELEASE_TAILS = 1ULL << 4,
    ANONYMOUS_AND_LAZY_MAP_TAILS = 1ULL << 5,
    SCHEDULER_AND_RETURNED_DRAINS = 1ULL << 6,
    PER_NODE_PHYSICAL_OWNERS = 1ULL << 7,
    // Host/witness evidence only; the kernel endpoint NEVER sets this bit.
    ACTUAL_WORK_AND_CONSUMED_WAIT = 1ULL << 8
};
inline constexpr uint64_t KERNEL_COVERAGE_REQUIRED = 0xff;
inline constexpr uint64_t COMBINED_COVERAGE_REQUIRED = 0x1ff;
struct Request {
    uint32_t version, bytes, phase, flags;
    uint64_t cookie, nonce;
    uint64_t output_address, output_bytes;
    uint64_t reserved[2];
};
static_assert(sizeof(Request) == 64 && alignof(Request) == 8);
static_assert(offsetof(Request, cookie) == 16 && offsetof(Request, output_address) == 32);
static_assert(offsetof(Request, reserved) == 48);

struct OwnerRow {
    uint64_t pages, objects;
};
struct PhysicalRow {
    uint64_t node, managed_pages, metadata_pages, descriptor_pages, free_inclusive, cached_free;
    OwnerRow owners[OWNER_COUNT];
};
static_assert(sizeof(OwnerRow) == 16 && sizeof(PhysicalRow) == 704);
static_assert(offsetof(PhysicalRow, owners) == 48);
struct DebtTotals {
    uint64_t task_blocks, file_blocks, family_blocks, binding_blocks;
    uint64_t active_roots, root_control_pages, indexed_bindings, active_tasks;
    uint64_t scoped_pids, active_threads, pid_namespaces, mount_namespaces;
    uint64_t ipc_namespaces, mount_preparations, service_members, unfinished_service_reports;
    uint64_t population_attached, child_links, tracee_links, child_events;
    uint64_t wait_registrations, event_claims, installed_fds, distinct_files;
    uint64_t reserved_fds, pending_fds, recovery_presence, deferred_gc_depth;
    uint64_t retirement_outstanding, retirement_done_storage, deferred_vmap_pages, reserved;
};
static_assert(sizeof(DebtTotals) == 256);
struct ReportHeader {
    uint64_t version, bytes, phase, outcome;
    uint64_t cookie, nonce, coverage, required_coverage;
    uint64_t capture_us, absolute_deadline_us, calls, waits;
    uint64_t reason, node_count, root_incarnation, reserved;
};
static_assert(sizeof(ReportHeader) == 128);
struct Report {
    ReportHeader header;
    MmDebtSnapshot mm;
    DebtTotals totals;
    PhysicalRow physical[MAX_NODES + 1];  // global first(node=UINT64_MAX), then dense nodes; unused rows0.
};
static_assert(offsetof(Report, mm) == 128 && offsetof(Report, totals) == 344);
static_assert(offsetof(Report, physical) == 600 && sizeof(Report) == 46360);
static_assert(sizeof(Report) <= MAX_REPORT_BYTES);

}  // namespace ker::test::numa_lifecycle
#endif
