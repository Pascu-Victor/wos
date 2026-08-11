#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "reclaim_policy.hpp"

namespace ker::mod::mm::reclaim {

inline constexpr size_t MAX_SHRINKERS = 16;
// Match the physical allocator's fixed zone-table bound so policy never drops
// a managed zone merely because firmware supplied a fragmented memory map.
inline constexpr size_t MAX_WATERMARK_ZONES = 128;
inline constexpr uint32_t DIRECT_RECLAIM_MAX_PASSES = 8;

enum class ReclaimContext : uint8_t {
    DIRECT = 0,
    BACKGROUND,
    EXPLICIT,
};

enum ReclaimCapability : uint32_t {
    RECLAIM_NONE = 0,
    RECLAIM_MAY_BLOCK = 1U << 0U,
    RECLAIM_MAY_IO = 1U << 1U,
    RECLAIM_MAY_ALLOCATE = 1U << 2U,
};

struct ReclaimRequest {
    ReclaimContext context{ReclaimContext::DIRECT};
    ReclaimPriority priority{ReclaimPriority::NORMAL};
    uint8_t requested_order{};
    uint64_t target_pages{1};
    uint64_t budget_units{};
    uint64_t scan_budget_units{};
    uint64_t deadline_us{};
    bool may_block{};
    bool may_io{};
    bool may_allocate{};
};

struct ReclaimCount {
    uint64_t reclaimable{};
    uint64_t dirty{};
    uint64_t pinned{};
    uint64_t reserved{};
    uint64_t unreclaimable{};
};

struct ReclaimScanResult {
    uint64_t scanned{};
    uint64_t reclaimed{};
    bool has_more{};
    bool failed{};
};

using CountCallback = auto (*)(void* opaque, const ReclaimRequest& request) -> ReclaimCount;
using ScanCallback = auto (*)(void* opaque, const ReclaimRequest& request) -> ReclaimScanResult;

struct Shrinker {
    const char* name{};
    ReclaimUnit unit{ReclaimUnit::PAGES};
    ReclaimUnit scan_unit{ReclaimUnit::PAGES};
    uint8_t rank{};
    ReclaimPriority min_priority{ReclaimPriority::LOW};
    uint32_t capabilities{RECLAIM_NONE};
    uint64_t max_batch_units{1};
    uint64_t max_scan_units{1};
    CountCallback count{};
    ScanCallback scan{};
    void* opaque{};
};

struct ReclaimRunResult {
    uint64_t callbacks{};
    uint64_t scanned_units{};
    uint64_t reclaimed_units{};
    uint64_t reclaimed_pages{};
    uint64_t elapsed_us{};
    bool has_more{};
    bool made_progress{};
    bool recursion_avoided{};
    bool lease_contended{};
    bool unsafe_context{};
};

struct ZoneWatermarkSnapshot {
    uint64_t zone{};
    uint64_t total_pages{};
    uint64_t free_pages{};
    uint64_t critical_pages{};
    uint64_t low_pages{};
    uint64_t high_pages{};
    int largest_free_order{-1};
    uint8_t requested_order{};
    PressureLevel level{PressureLevel::UNSATISFIABLE};
};

struct GlobalStatsSnapshot {
    uint64_t direct_attempts{};
    uint64_t background_attempts{};
    uint64_t explicit_attempts{};
    uint64_t explicit_requests{};
    uint64_t explicit_completions{};
    uint64_t explicit_rejections{};
    uint64_t callbacks{};
    // Legacy internal totals retained for callers that only need monotonic
    // activity. They mix native shrinker units and must not be exported as a
    // quantity with one implied unit.
    uint64_t scanned_units{};
    uint64_t reclaimed_units{};
    uint64_t scanned_pages{};
    uint64_t scanned_bytes{};
    uint64_t scanned_objects{};
    uint64_t reported_pages{};
    uint64_t reported_bytes{};
    uint64_t reported_objects{};
    // Actual increase in allocator-visible free pages across callbacks. This
    // is the only cross-shrinker reclaimed-page total.
    uint64_t reclaimed_pages{};
    uint64_t no_progress{};
    uint64_t failures{};
    uint64_t recursion_avoided{};
    uint64_t lease_contention{};
    uint64_t unsafe_context_skips{};
    uint64_t capability_skips{};
    uint64_t cooldown_skips{};
    uint64_t latency_us{};
    uint64_t latency_max_us{};
    uint64_t low_transitions{};
    uint64_t critical_transitions{};
    uint64_t recovered_transitions{};
    uint64_t worker_wakeups{};
    uint64_t worker_no_progress{};
    PressureLevel current_pressure{PressureLevel::NONE};
};

struct ShrinkerStatsSnapshot {
    const char* name{};
    ReclaimUnit unit{ReclaimUnit::PAGES};
    ReclaimUnit scan_unit{ReclaimUnit::PAGES};
    uint8_t rank{};
    ReclaimPriority min_priority{ReclaimPriority::LOW};
    uint32_t capabilities{};
    uint64_t count_calls{};
    uint64_t scan_calls{};
    uint64_t scanned_units{};
    uint64_t reclaimed_units{};
    uint64_t reclaimed_pages{};
    uint64_t no_progress{};
    uint64_t failures{};
    uint64_t context_skips{};
    uint64_t cooldown_skips{};
    uint64_t latency_us{};
    uint64_t latency_max_us{};
    ReclaimCount last_count{};
};

void init();
[[nodiscard]] auto register_shrinker(const Shrinker& shrinker) -> bool;
[[nodiscard]] auto run(const ReclaimRequest& request, std::string_view only_name = {}) -> ReclaimRunResult;
// Queue a targeted explicit request for the reclaim worker. This is the safe
// entrypoint for procfs and other callers that can arrive with preemption
// disabled; it never invokes a shrinker in the caller's context.
[[nodiscard]] auto request_explicit(const ReclaimRequest& request, std::string_view only_name) -> bool;
[[nodiscard]] auto reclaim_for_allocation(uint8_t requested_order, uint64_t target_pages) -> ReclaimRunResult;
void request_background(uint8_t requested_order, uint64_t target_pages);
void note_allocation(uint8_t requested_order, uint64_t allocated_pages);
void start_worker();

[[nodiscard]] auto pressure_for_order(uint8_t requested_order) -> PressureLevel;
[[nodiscard]] auto above_high_watermark(uint8_t requested_order) -> bool;
auto snapshot_zone_watermarks(ZoneWatermarkSnapshot* out, size_t capacity, uint8_t requested_order) -> size_t;
void get_global_stats(GlobalStatsSnapshot& out);
auto snapshot_shrinker_stats(ShrinkerStatsSnapshot* out, size_t capacity) -> size_t;
[[nodiscard]] auto unit_name(ReclaimUnit unit) -> const char*;
[[nodiscard]] auto priority_name(ReclaimPriority priority) -> const char*;
[[nodiscard]] auto pressure_name(PressureLevel level) -> const char*;

#ifdef WOS_SELFTEST
[[nodiscard]] auto selftest_priority_and_progress() -> bool;
[[nodiscard]] auto selftest_no_progress_backoff() -> bool;
[[nodiscard]] auto selftest_recursion_guard() -> bool;
[[nodiscard]] auto selftest_context_filtering() -> bool;
[[nodiscard]] auto selftest_budget_bounds() -> bool;
#endif

}  // namespace ker::mod::mm::reclaim
