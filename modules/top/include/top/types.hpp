#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace top {

inline constexpr int DEFAULT_INTERVAL_MS = 1000;
inline constexpr uint64_t PAGE_SIZE = 4096;
inline constexpr uint64_t KIB_PER_PAGE = PAGE_SIZE / 1024;
inline constexpr uint64_t PROC_CLK_TCK = 100;
inline constexpr int HEADER_LINES = 7;
inline constexpr int USER_WIDTH = 8;
inline constexpr int HOST_WIDTH = 12;
inline constexpr int COMMAND_WIDTH_FALLBACK = 32;

struct CpuTimes {
    uint64_t user = 0;
    uint64_t nice = 0;
    uint64_t system = 0;
    uint64_t idle = 0;
    uint64_t iowait = 0;
    uint64_t irq = 0;
    uint64_t softirq = 0;
    uint64_t steal = 0;
};

struct CpuPercent {
    double user = 0.0;
    double nice = 0.0;
    double system = 0.0;
    double idle = 0.0;
    double iowait = 0.0;
    double irq = 0.0;
    double softirq = 0.0;
    double steal = 0.0;
};

struct MemInfo {
    uint64_t total_kib = 0;
    uint64_t free_kib = 0;
    uint64_t available_kib = 0;
    uint64_t buffers_kib = 0;
    uint64_t cached_kib = 0;
    uint64_t swap_total_kib = 0;
    uint64_t swap_free_kib = 0;
};

struct LoadAverage {
    double one = 0.0;
    double five = 0.0;
    double fifteen = 0.0;
};

struct ProcTimes {
    uint64_t total_ticks = 0;
};

struct ProcRow {
    uint64_t pid = 0;
    uint32_t uid = 0;
    int64_t priority = 20;
    int64_t nice = 0;
    uint64_t virt_kib = 0;
    uint64_t resident_kib = 0;
    uint64_t shared_kib = 0;
    char state = 'S';
    double cpu_pct = 0.0;
    double mem_pct = 0.0;
    uint64_t total_ticks = 0;
    std::string user;
    std::string host;
    std::string comm;
    std::string command;
};

struct Snapshot {
    CpuTimes cpu{};
    uint64_t cpu_count = 1;
    MemInfo mem{};
    LoadAverage load{};
    double uptime_seconds = 0.0;
    std::vector<ProcRow> rows;
    std::unordered_map<uint64_t, ProcTimes> proc_times;
};

auto cpu_total(const CpuTimes& cpu) -> uint64_t;
auto diff_counter(uint64_t now, uint64_t old) -> uint64_t;
auto diff_cpu(const CpuTimes& now, const CpuTimes& old) -> CpuTimes;
auto cpu_percent_from_delta(const CpuTimes& delta) -> CpuPercent;

}  // namespace top
