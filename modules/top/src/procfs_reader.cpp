#include "top/procfs_reader.hpp"

#include <dirent.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <string_view>
#include <utility>

#include "top/io.hpp"
#include "top/parse.hpp"

namespace top {
namespace {

auto read_cpu_times(uint64_t& cpu_count) -> CpuTimes {
    cpu_count = 1;
    auto text = read_file("/proc/stat");
    if (!text.has_value()) {
        return {};
    }

    CpuTimes cpu{};
    size_t line_start = 0;
    while (line_start < text->size()) {
        size_t line_end = text->find('\n', line_start);
        if (line_end == std::string::npos) {
            line_end = text->size();
        }
        std::string_view line(text->data() + line_start, line_end - line_start);
        if (line.starts_with("cpu ")) {
            auto tokens = split_ws(line);
            if (tokens.size() >= 9) {
                (void)parse_u64(tokens[1], cpu.user);
                (void)parse_u64(tokens[2], cpu.nice);
                (void)parse_u64(tokens[3], cpu.system);
                (void)parse_u64(tokens[4], cpu.idle);
                (void)parse_u64(tokens[5], cpu.iowait);
                (void)parse_u64(tokens[6], cpu.irq);
                (void)parse_u64(tokens[7], cpu.softirq);
                (void)parse_u64(tokens[8], cpu.steal);
            }
        } else if (line.starts_with("cpu") && line.size() > 3 && line[3] >= '0' && line[3] <= '9') {
            cpu_count++;
        }
        line_start = line_end + 1;
    }
    if (cpu_count > 1) {
        cpu_count--;
    }
    return cpu;
}

auto read_meminfo() -> MemInfo {
    MemInfo mem{};
    auto text = read_file("/proc/meminfo");
    if (!text.has_value()) {
        return mem;
    }

    size_t line_start = 0;
    while (line_start < text->size()) {
        size_t line_end = text->find('\n', line_start);
        if (line_end == std::string::npos) {
            line_end = text->size();
        }
        std::string_view line(text->data() + line_start, line_end - line_start);
        size_t const COLON = line.find(':');
        if (COLON != std::string_view::npos) {
            auto key = line.substr(0, COLON);
            auto values = split_ws(line.substr(COLON + 1));
            uint64_t value = 0;
            if (!values.empty()) {
                (void)parse_u64(values[0], value);
            }
            if (key == "MemTotal") {
                mem.total_kib = value;
            } else if (key == "MemFree") {
                mem.free_kib = value;
            } else if (key == "MemAvailable") {
                mem.available_kib = value;
            } else if (key == "Buffers") {
                mem.buffers_kib = value;
            } else if (key == "Cached") {
                mem.cached_kib = value;
            } else if (key == "SwapTotal") {
                mem.swap_total_kib = value;
            } else if (key == "SwapFree") {
                mem.swap_free_kib = value;
            }
        }
        line_start = line_end + 1;
    }
    return mem;
}

auto read_loadavg() -> LoadAverage {
    LoadAverage load{};
    auto text = read_file("/proc/loadavg", 256);
    if (!text.has_value()) {
        return load;
    }
    auto tokens = split_ws(*text);
    if (tokens.size() >= 3) {
        load.one = parse_double(tokens[0]);
        load.five = parse_double(tokens[1]);
        load.fifteen = parse_double(tokens[2]);
    }
    return load;
}

auto read_uptime() -> double {
    auto text = read_file("/proc/uptime", 128);
    if (!text.has_value()) {
        return 0.0;
    }
    auto tokens = split_ws(*text);
    return tokens.empty() ? 0.0 : parse_double(tokens[0]);
}

auto read_status_uid(uint64_t pid) -> uint32_t {
    std::array<char, 64> path{};
    std::snprintf(path.data(), path.size(), "/proc/%llu/status", static_cast<unsigned long long>(pid));
    auto text = read_file(path.data(), 4096);
    if (!text.has_value()) {
        return 0;
    }
    size_t line_start = 0;
    while (line_start < text->size()) {
        size_t line_end = text->find('\n', line_start);
        if (line_end == std::string::npos) {
            line_end = text->size();
        }
        std::string_view line(text->data() + line_start, line_end - line_start);
        if (line.starts_with("Uid:")) {
            auto tokens = split_ws(line.substr(4));
            uint64_t uid = 0;
            if (!tokens.empty() && parse_u64(tokens[0], uid)) {
                return static_cast<uint32_t>(uid);
            }
        }
        line_start = line_end + 1;
    }
    return 0;
}

auto read_cmdline(uint64_t pid, std::string_view fallback) -> std::string {
    std::array<char, 64> path{};
    std::snprintf(path.data(), path.size(), "/proc/%llu/cmdline", static_cast<unsigned long long>(pid));
    auto text = read_file(path.data(), 4096);
    if (!text.has_value() || text->empty()) {
        return std::string(fallback);
    }
    for (char& ch : *text) {
        if (ch == '\0') {
            ch = ' ';
        }
    }
    while (!text->empty() && text->back() == ' ') {
        text->pop_back();
    }
    return text->empty() ? std::string(fallback) : *text;
}

auto read_trimmed_proc_file(uint64_t pid, std::string_view name, std::string_view fallback) -> std::string {
    std::array<char, 96> path{};
    std::snprintf(path.data(), path.size(), "/proc/%llu/%.*s", static_cast<unsigned long long>(pid), static_cast<int>(name.size()),
                  name.data());
    auto text = read_file(path.data(), 256);
    if (!text.has_value()) {
        return std::string(fallback);
    }
    while (!text->empty() && (text->back() == '\n' || text->back() == '\r' || text->back() == '\0')) {
        text->pop_back();
    }
    return text->empty() ? std::string(fallback) : *text;
}

auto parse_stat(uint64_t pid, ProcRow& row) -> bool {
    std::array<char, 64> path{};
    std::snprintf(path.data(), path.size(), "/proc/%llu/stat", static_cast<unsigned long long>(pid));
    auto text = read_file(path.data(), 4096);
    if (!text.has_value()) {
        return false;
    }

    size_t const LPAREN = text->find('(');
    size_t const RPAREN = text->rfind(')');
    if (LPAREN == std::string::npos || RPAREN == std::string::npos || RPAREN <= LPAREN) {
        return false;
    }

    row.pid = pid;
    row.comm = text->substr(LPAREN + 1, RPAREN - LPAREN - 1);
    auto tokens = split_ws(std::string_view(text->data() + RPAREN + 1, text->size() - RPAREN - 1));
    if (tokens.size() < 22) {
        return false;
    }

    row.state = tokens[0].empty() ? '?' : tokens[0].front();
    uint64_t utime = 0;
    uint64_t stime = 0;
    (void)parse_u64(tokens[11], utime);
    (void)parse_u64(tokens[12], stime);
    (void)parse_i64(tokens[15], row.priority);
    (void)parse_i64(tokens[16], row.nice);
    (void)parse_u64(tokens[20], row.virt_kib);
    row.virt_kib /= 1024;
    uint64_t rss_pages = 0;
    (void)parse_u64(tokens[21], rss_pages);
    row.resident_kib = rss_pages * KIB_PER_PAGE;
    row.total_ticks = utime + stime;
    return true;
}

void read_statm(uint64_t pid, ProcRow& row) {
    std::array<char, 64> path{};
    std::snprintf(path.data(), path.size(), "/proc/%llu/statm", static_cast<unsigned long long>(pid));
    auto text = read_file(path.data(), 512);
    if (!text.has_value()) {
        return;
    }
    auto tokens = split_ws(*text);
    if (tokens.size() < 3) {
        return;
    }
    uint64_t size_pages = 0;
    uint64_t resident_pages = 0;
    uint64_t shared_pages = 0;
    (void)parse_u64(tokens[0], size_pages);
    (void)parse_u64(tokens[1], resident_pages);
    (void)parse_u64(tokens[2], shared_pages);
    row.virt_kib = size_pages * KIB_PER_PAGE;
    row.resident_kib = resident_pages * KIB_PER_PAGE;
    row.shared_kib = shared_pages * KIB_PER_PAGE;
}

auto is_numeric_name(const char* name, uint64_t& pid) -> bool {
    if (name == nullptr || name[0] == '\0') {
        return false;
    }
    return parse_u64(name, pid);
}

auto read_processes(const std::unordered_map<uint32_t, std::string>& users, const Snapshot* previous, const CpuTimes& cpu_delta,
                    uint64_t cpu_count, uint64_t mem_total_kib) -> std::vector<ProcRow> {
    std::vector<ProcRow> rows;
    ScopedDir dir(opendir("/proc"));
    if (dir.get() == nullptr) {
        return rows;
    }

    uint64_t const CPU_DELTA_TOTAL = cpu_total(cpu_delta);
    for (;;) {
        errno = 0;
        dirent* ent = readdir(dir.get());
        if (ent == nullptr) {
            break;
        }
        uint64_t pid = 0;
        if (!is_numeric_name(ent->d_name, pid)) {
            continue;
        }

        ProcRow row{};
        if (!parse_stat(pid, row)) {
            continue;
        }
        read_statm(pid, row);
        row.uid = read_status_uid(pid);
        if (auto it = users.find(row.uid); it != users.end()) {
            row.user = it->second;
        } else {
            row.user = std::to_string(row.uid);
        }
        row.host = read_trimmed_proc_file(pid, "wki_runner", "-");
        row.command = read_cmdline(pid, row.comm);

        if (previous != nullptr) {
            auto it = previous->proc_times.find(pid);
            if (it != previous->proc_times.end() && CPU_DELTA_TOTAL != 0) {
                uint64_t const PROC_DELTA = diff_counter(row.total_ticks, it->second.total_ticks);
                row.cpu_pct =
                    (static_cast<double>(PROC_DELTA) * 100.0 * static_cast<double>(cpu_count)) / static_cast<double>(CPU_DELTA_TOTAL);
            }
        }
        if (mem_total_kib != 0) {
            row.mem_pct = (static_cast<double>(row.resident_kib) * 100.0) / static_cast<double>(mem_total_kib);
        }
        rows.push_back(std::move(row));
    }

    std::sort(rows.begin(), rows.end(), [](const ProcRow& lhs, const ProcRow& rhs) {
        if (lhs.cpu_pct != rhs.cpu_pct) {
            return lhs.cpu_pct > rhs.cpu_pct;
        }
        if (lhs.resident_kib != rhs.resident_kib) {
            return lhs.resident_kib > rhs.resident_kib;
        }
        return lhs.pid < rhs.pid;
    });
    return rows;
}

}  // namespace

auto read_passwd() -> std::unordered_map<uint32_t, std::string> {
    std::unordered_map<uint32_t, std::string> users;
    auto text = read_file("/etc/passwd", 65536);
    if (!text.has_value()) {
        users.emplace(0, "root");
        return users;
    }

    size_t line_start = 0;
    while (line_start < text->size()) {
        size_t line_end = text->find('\n', line_start);
        if (line_end == std::string::npos) {
            line_end = text->size();
        }
        std::string_view line(text->data() + line_start, line_end - line_start);
        size_t const FIRST = line.find(':');
        if (FIRST != std::string_view::npos) {
            size_t const SECOND = line.find(':', FIRST + 1);
            size_t const THIRD = SECOND == std::string_view::npos ? std::string_view::npos : line.find(':', SECOND + 1);
            if (SECOND != std::string_view::npos && THIRD != std::string_view::npos) {
                uint64_t uid = 0;
                std::string_view uid_field(line.data() + SECOND + 1, THIRD - SECOND - 1);
                std::string_view name_field(line.data(), FIRST);
                if (parse_u64(uid_field, uid)) {
                    users[static_cast<uint32_t>(uid)] = std::string(name_field);
                }
            }
        }
        line_start = line_end + 1;
    }
    users.try_emplace(0, "root");
    return users;
}

auto make_snapshot(const Snapshot* previous, const std::unordered_map<uint32_t, std::string>& users) -> Snapshot {
    Snapshot snap{};
    snap.cpu = read_cpu_times(snap.cpu_count);
    snap.mem = read_meminfo();
    snap.load = read_loadavg();
    snap.uptime_seconds = read_uptime();
    CpuTimes const CPU_DELTA = previous != nullptr ? diff_cpu(snap.cpu, previous->cpu) : snap.cpu;
    snap.rows = read_processes(users, previous, CPU_DELTA, snap.cpu_count, snap.mem.total_kib);
    for (const auto& row : snap.rows) {
        snap.proc_times[row.pid] = ProcTimes{.total_ticks = row.total_ticks};
    }
    return snap;
}

}  // namespace top
