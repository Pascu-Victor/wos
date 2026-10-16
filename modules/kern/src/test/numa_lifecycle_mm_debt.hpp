#pragma once
#ifdef WOS_SELFTEST
#include <cstddef>
#include <cstdint>
#include <platform/mm/lifecycle_stamp.hpp>
namespace ker::test::numa_lifecycle {
enum class CounterField : size_t {
    ACCESS_READERS = 0,
    ACCESS_EXCLUSIVES = 1,
    USER_PAGE_PINS = 2,
    STABLE_HANDLES = 3,
    RETAINED_FRAME_REFS = 4,
    TASK_USERCOPY_ACCESSES = 5,
    PIN_RETURNS = 6,
    LAZY_ANONYMOUS = 7,
    LAZY_FILE = 8,
    LAZY_NATIVE_FILE = 9,
    LAZY_TMPFS_READ_SHARED = 10,
    LAZY_TMPFS_WRITE_SHARED = 11,
    LAZY_RETURNS = 12,
    SWAP_OBJECTS = 13,
    SWAP_MAPPINGS = 14,
    SWAP_QUEUED = 15,
    SWAP_PAGEINS = 16,
    SWAP_PAGEOUTS = 17,
    SWAP_WAITERS = 18,
    SWAP_RETURNS = 19,
    LEGACY_FILE_REGISTRY = 20,
    NATIVE_FILE_REGISTRY = 21,
    REGISTRY_RETURNS = 22,
    COUNT = 23
};
struct MmDebtSnapshot {
    uint64_t version_begin, version_end, inflight, valid;                             // 0..24
    uint64_t access_readers, access_exclusives, user_page_pins;                       // 32..48
    uint64_t stable_handles, retained_frame_refs, task_usercopy_accesses;             // 56..72
    uint64_t pin_returns;                                                             // 80
    uint64_t lazy_rows[5];                                                            // 88; actual LazyVmemKind order 0..4
    uint64_t lazy_returns;                                                            // 128
    uint64_t swap_objects, swap_mappings, swap_queued;                                // 136..152
    uint64_t swap_pageins, swap_pageouts, swap_waiters, swap_returns;                 // 160..184
    uint64_t legacy_file_registry_rows, native_file_registry_rows, registry_returns;  // 192..208
};
static_assert(sizeof(MmDebtSnapshot) == 216 && alignof(MmDebtSnapshot) == 8);
static_assert(offsetof(MmDebtSnapshot, lazy_rows) == 88 && offsetof(MmDebtSnapshot, registry_returns) == 208);
// Named caller-owned epoch storage for the coherent physical collector.
// Construct and make both objects addressable before any allocator lock.
struct MmDebtCaptureScratch {
    mod::mm::lifecycle_census::Stamp before, after;
};
static_assert(sizeof(MmDebtCaptureScratch) == 48 && alignof(MmDebtCaptureScratch) == 8);
// Diagnostic only. Existing MM acceptance uses MmDebtSnapshot unchanged.
struct MmActivitySnapshot {
    mod::mm::lifecycle_census::Stamp before, after;
    uint64_t explicit_begins, explicit_ends;
    uint64_t add_attempts[static_cast<size_t>(CounterField::COUNT)];
    uint64_t drop_attempts[static_cast<size_t>(CounterField::COUNT)];
    uint64_t invalid_selectors, valid;
};
struct MmActivityRelation {
    uint64_t reconciled, mm_delta, accounted_delta, begin_delta, end_delta, adjust_calls;
};
static_assert(sizeof(MmActivitySnapshot) == 448 && alignof(MmActivitySnapshot) == 8);
static_assert(sizeof(MmActivityRelation) == 48 && alignof(MmActivityRelation) == 8);
namespace mm_debt {
// Internal scalar hooks. No lock, allocation, callback, backend or usercopy.
// Change brackets are explicit, never stack RAII spanning fatal usercopy/I/O.
void begin_change() noexcept;
void end_change() noexcept;
void add(CounterField field) noexcept;
void drop(CounterField field) noexcept;
// Copied LazyVmemKind value0..4; invalid input poisons diagnostic arithmetic.
void add_lazy_row(uint8_t copied_kind) noexcept;
void drop_lazy_row(uint8_t copied_kind) noexcept;
// Exclusive addressable output, by-ref, no hidden return and no owner refs.
// valid means stable arithmetic/stamp sampling; acceptance ALSO needs inflight==0.
void snapshot(MmDebtSnapshot& output) noexcept;
// Same fixed atomic observation with explicit caller-born epoch scratch. The
// collector preflights this exact provider outside locks, then overwrites all
// fields in its one authoritative capture while all allocator locks are held.
void snapshot_addressable(MmDebtSnapshot& output, MmDebtCaptureScratch& scratch) noexcept;
// Caller-born addressable output; snapshots/counters grant no acceptance.
void activity_snapshot(MmActivitySnapshot& output) noexcept;
// Reconciled only for unchanged, nonsaturated stamps matching both old MM
// samples. UNKNOWN on any mismatch/overflow; this is not producer identity.
void interpret_activity(const MmDebtSnapshot& a, const MmDebtSnapshot& b, const MmActivitySnapshot& activity_a,
                        const MmActivitySnapshot& activity_b, MmActivityRelation& output) noexcept;
// This checkpoint has NO handler or coverage authority. Disconnected fields
// remain required integration work, never zero-valued qualification evidence.
}  // namespace mm_debt
}  // namespace ker::test::numa_lifecycle
#endif
