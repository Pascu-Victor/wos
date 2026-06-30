#include "processes.hpp"

#include <algorithm>
#include <cstdio>

#include "rows.hpp"

namespace memacc {
namespace {

auto sort_value(const ProcRow& proc, std::string_view key) -> uint64_t {
    if (key == "total") {
        return proc.total_bytes;
    }
    if (key == "rss") {
        return proc.rss_bytes;
    }
    if (key == "pte") {
        return proc.pte_bytes;
    }
    if (key == "mmap") {
        return proc.mmap_bytes;
    }
    if (key == "heap") {
        return proc.heap_bytes;
    }
    if (key == "stack") {
        return proc.stack_bytes;
    }
    if (key == "pid") {
        return proc.pid;
    }
    return proc.rss_bytes;
}

}  // namespace

auto load_procs() -> std::vector<ProcRow> {
    std::vector<ProcRow> out;
    for (const auto& row : read_rows("procs")) {
        if (row.record != "proc") {
            continue;
        }
        ProcRow proc;
        proc.pid = get_u64(row, "pid");
        proc.ppid = get_u64(row, "ppid");
        proc.cpu = get_u64(row, "cpu");
        proc.state = get_string(row, "state");
        proc.name = get_string(row, "name");
        proc.cmd = get_string(row, "cmd");
        proc.virt_bytes = get_u64(row, "virt_bytes");
        proc.rss_bytes = get_u64(row, "rss_bytes");
        proc.shr_bytes = get_u64(row, "shr_bytes");
        proc.pte_bytes = get_u64(row, "pte_bytes");
        proc.heap_bytes = get_u64(row, "heap_bytes");
        proc.mmap_bytes = get_u64(row, "mmap_bytes");
        proc.stack_bytes = get_u64(row, "stack_bytes");
        proc.total_bytes = proc.rss_bytes + proc.pte_bytes;
        out.push_back(std::move(proc));
    }
    return out;
}

auto filtered_procs(const Options& opt) -> std::vector<ProcRow> {
    std::vector<ProcRow> rows = load_procs();
    rows.erase(std::remove_if(rows.begin(), rows.end(),
                              [&opt](const ProcRow& proc) {
                                  if (opt.pid.has_value() && proc.pid != *opt.pid) {
                                      return true;
                                  }
                                  if (!opt.name.empty() && proc.name.find(opt.name) == std::string::npos &&
                                      proc.cmd.find(opt.name) == std::string::npos) {
                                      return true;
                                  }
                                  if (!opt.state.empty() && proc.state != opt.state) {
                                      return true;
                                  }
                                  if (bytes_to_kib(proc.total_bytes) < opt.min_kib) {
                                      return true;
                                  }
                                  return false;
                              }),
               rows.end());
    if (opt.sort == "name") {
        std::sort(rows.begin(), rows.end(), [](const ProcRow& a, const ProcRow& b) { return a.name < b.name; });
    } else {
        std::sort(rows.begin(), rows.end(), [&opt](const ProcRow& a, const ProcRow& b) {
            uint64_t const AV = sort_value(a, opt.sort);
            uint64_t const BV = sort_value(b, opt.sort);
            if (AV == BV) {
                return a.pid < b.pid;
            }
            return AV > BV;
        });
    }
    if (opt.limit > 0 && static_cast<size_t>(opt.limit) < rows.size()) {
        rows.resize(static_cast<size_t>(opt.limit));
    }
    return rows;
}

void print_proc_table(const std::vector<ProcRow>& rows) {
    std::printf("%6s %6s %-9s %3s %10s %10s %10s %10s %10s %10s %10s  %s\n", "PID", "PPID", "STATE", "CPU", "VIRT", "RSS", "SHR", "PTE",
                "HEAP", "MMAP", "STACK", "NAME");
    for (const auto& proc : rows) {
        std::printf(
            "%6llu %6llu %-9s %3llu %10llu %10llu %10llu %10llu %10llu %10llu %10llu  %s\n", static_cast<unsigned long long>(proc.pid),
            static_cast<unsigned long long>(proc.ppid), proc.state.c_str(), static_cast<unsigned long long>(proc.cpu),
            static_cast<unsigned long long>(bytes_to_kib(proc.virt_bytes)), static_cast<unsigned long long>(bytes_to_kib(proc.rss_bytes)),
            static_cast<unsigned long long>(bytes_to_kib(proc.shr_bytes)), static_cast<unsigned long long>(bytes_to_kib(proc.pte_bytes)),
            static_cast<unsigned long long>(bytes_to_kib(proc.heap_bytes)), static_cast<unsigned long long>(bytes_to_kib(proc.mmap_bytes)),
            static_cast<unsigned long long>(bytes_to_kib(proc.stack_bytes)), proc.name.c_str());
    }
}

}  // namespace memacc
