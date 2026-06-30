#include "reports.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <string_view>

#include "processes.hpp"
#include "procfs_io.hpp"
#include "rows.hpp"

namespace memacc {
namespace {

void print_kib_line(const char* label, uint64_t bytes) {
    std::printf("%-24s %12llu KiB\n", label, static_cast<unsigned long long>(bytes_to_kib(bytes)));
}

template <size_t N>
auto contains_key(const std::array<std::string_view, N>& keys, std::string_view key) -> bool {
    return std::find(keys.begin(), keys.end(), key) != keys.end();
}

template <size_t N>
void print_ordered_row(const Row& row, const std::array<std::string_view, N>& keys) {
    std::printf("%s", row.record.c_str());
    for (std::string_view key : keys) {
        auto it = row.kv.find(std::string(key));
        if (it != row.kv.end()) {
            std::printf(" %.*s=%s", static_cast<int>(key.size()), key.data(), it->second.c_str());
        }
    }
    for (const auto& kv : row.kv) {
        if (!contains_key(keys, kv.first)) {
            std::printf(" %s=%s", kv.first.c_str(), kv.second.c_str());
        }
    }
    std::printf("\n");
}

void print_raw_row(const Row& row) {
    std::printf("%s", row.record.c_str());
    for (const auto& kv : row.kv) {
        std::printf(" %s=%s", kv.first.c_str(), kv.second.c_str());
    }
    std::printf("\n");
}

}  // namespace

void print_summary() {
    auto rows = read_rows("summary");
    const Row* summary = first_record(rows, "summary");
    if (summary == nullptr) {
        std::printf("memacc: /proc/memacc/summary unavailable\n");
        return;
    }

    std::printf("Memory\n");
    print_kib_line("total", get_u64(*summary, "total_bytes"));
    print_kib_line("free", get_u64(*summary, "free_bytes"));
    print_kib_line("used", get_u64(*summary, "used_bytes"));
    print_kib_line("process rss", get_u64(*summary, "process_rss_bytes"));
    print_kib_line("process pte", get_u64(*summary, "process_pte_bytes"));
    print_kib_line("allocator", get_u64(*summary, "allocator_bytes"));
    print_kib_line("caches", get_u64(*summary, "cache_bytes"));
    print_kib_line("unaccounted estimate", get_u64(*summary, "unaccounted_estimate_bytes"));
    std::printf("processes %llu, tasks %llu, kernel tasks %llu\n", static_cast<unsigned long long>(get_u64(*summary, "processes")),
                static_cast<unsigned long long>(get_u64(*summary, "tasks")),
                static_cast<unsigned long long>(get_u64(*summary, "kernel_tasks")));

    auto alloc_rows = read_rows("alloc_totals");
    if (const Row* phys = first_record(alloc_rows, "phys"); phys != nullptr) {
        std::printf("physical allocator pages allocated=%llu freed=%llu live=%llu free=%llu alloc_ops=%llu free_ops=%llu\n",
                    static_cast<unsigned long long>(get_u64(*phys, "total_allocated_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "total_freed_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "live_allocated_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "current_free_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "alloc_count")),
                    static_cast<unsigned long long>(get_u64(*phys, "free_count")));
    }

    for (const auto& row : rows) {
        if (row.record == "feature") {
            std::printf("feature %-14s available=%s enabled=%s generation=%llu\n", get_string(row, "name").c_str(),
                        get_u64(row, "available") != 0 ? "yes" : "no", get_u64(row, "enabled") != 0 ? "yes" : "no",
                        static_cast<unsigned long long>(get_u64(row, "generation")));
        }
    }
}

void print_alloc_rows() {
    constexpr std::array<std::string_view, 13> PHYS_ALLOC_KEYS{
        "total_allocated_bytes", "total_freed_bytes",     "live_allocated_bytes",  "alloc_count",       "free_count",
        "total_mem_bytes",       "free_mem_bytes",        "total_allocated_pages", "total_freed_pages", "live_allocated_pages",
        "current_free_pages",    "alloc_operation_count", "free_operation_count"};

    for (const auto& row : read_rows("alloc_totals")) {
        if (row.record == "phys") {
            print_ordered_row(row, PHYS_ALLOC_KEYS);
        } else {
            print_raw_row(row);
        }
    }
}

void print_kernel() {
    print_alloc_rows();
    for (const auto& row : read_rows("dead")) {
        if (row.record == "queue") {
            std::printf("cpu=%llu run=%llu wait=%llu dead=%llu\n", static_cast<unsigned long long>(get_u64(row, "cpu")),
                        static_cast<unsigned long long>(get_u64(row, "run")), static_cast<unsigned long long>(get_u64(row, "wait")),
                        static_cast<unsigned long long>(get_u64(row, "dead")));
        }
    }
}

void print_dump(const Options& opt) {
    print_summary();
    std::printf("\nTop processes\n");
    Options proc_opt = opt;
    if (proc_opt.full) {
        proc_opt.limit = 0;
    }
    print_proc_table(filtered_procs(proc_opt));

    std::printf("\nAllocator totals\n");
    print_alloc_rows();

    std::printf("\nZones\n");
    for (const auto& row : read_rows("zones")) {
        if (row.record != "zone") {
            continue;
        }
        std::printf("zone=%llu name=%s free=%llu/%llu pages cached0=%llu metadata=%llu mismatch=%llu\n",
                    static_cast<unsigned long long>(get_u64(row, "id")), get_string(row, "name").c_str(),
                    static_cast<unsigned long long>(get_u64(row, "free_pages")),
                    static_cast<unsigned long long>(get_u64(row, "usable_pages")),
                    static_cast<unsigned long long>(get_u64(row, "cached_order0_pages")),
                    static_cast<unsigned long long>(get_u64(row, "metadata_pages")),
                    static_cast<unsigned long long>(get_u64(row, "free_count_mismatch")));
    }

    if (!opt.full) {
        return;
    }

    std::printf("\nDead queues\n");
    auto dead_text = read_file(memacc_path("dead"));
    if (dead_text.has_value()) {
        std::printf("%s", dead_text->c_str());
    }
    std::printf("\nSlabs\n");
    auto slabs_text = read_file(memacc_path("slabs"));
    if (slabs_text.has_value()) {
        std::printf("%s", slabs_text->c_str());
    }
    std::printf("\nkmalloc live\n");
    auto kmalloc_text = read_file(memacc_path("kmalloc_live"));
    if (kmalloc_text.has_value()) {
        std::printf("%s", kmalloc_text->c_str());
    }
    std::printf("\nPage callers\n");
    auto page_text = read_file(memacc_path("page_callers"));
    if (page_text.has_value()) {
        std::printf("%s", page_text->c_str());
    }
}

}  // namespace memacc
