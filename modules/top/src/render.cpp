#include "top/render.hpp"

#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX time APIs are exposed here.

#include <algorithm>
#include <array>
#include <cstdio>
#include <string_view>

#include "top/terminal.hpp"

namespace top {
namespace {

auto format_user(std::string_view user) -> std::string {
    if (user.size() <= USER_WIDTH) {
        return std::string(user);
    }
    std::string out(user.substr(0, USER_WIDTH - 1));
    out.push_back('+');
    return out;
}

auto format_host(std::string_view host) -> std::string {
    if (host.size() <= HOST_WIDTH) {
        return std::string(host);
    }
    std::string out(host.substr(0, HOST_WIDTH - 1));
    out.push_back('+');
    return out;
}

auto format_mem(uint64_t kib) -> std::string {
    std::array<char, 32> buf{};
    if (kib >= 1024ULL * 1024ULL) {
        std::snprintf(buf.data(), buf.size(), "%.1fg", static_cast<double>(kib) / (1024.0 * 1024.0));
    } else if (kib >= 1024ULL) {
        std::snprintf(buf.data(), buf.size(), "%.1fm", static_cast<double>(kib) / 1024.0);
    } else {
        std::snprintf(buf.data(), buf.size(), "%llu", static_cast<unsigned long long>(kib));
    }
    return buf.data();
}

auto format_time_ticks(uint64_t ticks) -> std::string {
    uint64_t const HUNDREDTHS = (ticks * 100ULL) / PROC_CLK_TCK;
    uint64_t const MINUTES = HUNDREDTHS / 6000ULL;
    uint64_t const SECONDS = (HUNDREDTHS / 100ULL) % 60ULL;
    uint64_t const FRACTION = HUNDREDTHS % 100ULL;
    std::array<char, 32> buf{};
    std::snprintf(buf.data(), buf.size(), "%llu:%02llu.%02llu", static_cast<unsigned long long>(MINUTES),
                  static_cast<unsigned long long>(SECONDS), static_cast<unsigned long long>(FRACTION));
    return buf.data();
}

auto format_uptime(double uptime_seconds) -> std::string {
    auto total = static_cast<uint64_t>(uptime_seconds);
    uint64_t const DAYS = total / 86400ULL;
    total %= 86400ULL;
    uint64_t const HOURS = total / 3600ULL;
    uint64_t const MINUTES = (total % 3600ULL) / 60ULL;
    std::array<char, 64> buf{};
    if (DAYS != 0) {
        std::snprintf(buf.data(), buf.size(), "%llu day%s, %02llu:%02llu", static_cast<unsigned long long>(DAYS), DAYS == 1 ? "" : "s",
                      static_cast<unsigned long long>(HOURS), static_cast<unsigned long long>(MINUTES));
    } else if (HOURS != 0) {
        std::snprintf(buf.data(), buf.size(), "%02llu:%02llu", static_cast<unsigned long long>(HOURS),
                      static_cast<unsigned long long>(MINUTES));
    } else {
        std::snprintf(buf.data(), buf.size(), "%llu min", static_cast<unsigned long long>(MINUTES));
    }
    return buf.data();
}

auto current_time_string() -> std::string {
    time_t const NOW = time(nullptr);
    tm* local = localtime(&NOW);
    std::array<char, 32> buf{};
    if (local == nullptr) {
        std::snprintf(buf.data(), buf.size(), "00:00:00");
    } else {
        std::snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", local->tm_hour, local->tm_min, local->tm_sec);
    }
    return buf.data();
}

auto append_visible_line(std::string& out, std::string_view line, int col_offset, int cols, bool terminate_line = true) -> void {
    if (col_offset > 0) {
        auto skip = static_cast<size_t>(col_offset);
        if (skip >= line.size()) {
            if (terminate_line) {
                out.append("\r\n");
            }
            return;
        }
        line.remove_prefix(skip);
    }
    if (cols > 0 && line.size() > static_cast<size_t>(cols)) {
        line = line.substr(0, static_cast<size_t>(cols));
    }
    out.append(line.data(), line.size());
    if (terminate_line) {
        out.append("\r\n");
    }
}

}  // namespace

auto render(const Snapshot& snap, const Snapshot* previous, int row_offset, int col_offset, bool interactive) -> std::string {
    TerminalSize const TERM = terminal_size();
    CpuPercent const CPU = cpu_percent_from_delta(previous != nullptr ? diff_cpu(snap.cpu, previous->cpu) : snap.cpu);
    std::string out;
    out.reserve(8192);
    if (interactive) {
        out += "\r\x1b[H\x1b[2J\x1b[H\x1b[?25l";
    }
    int rendered_lines = 0;
    auto append_screen_line = [&](std::string_view text) -> bool {
        if (rendered_lines >= TERM.rows) {
            return false;
        }
        bool const TERMINATE = rendered_lines + 1 < TERM.rows;
        append_visible_line(out, text, col_offset, TERM.cols, TERMINATE);
        rendered_lines++;
        return true;
    };

    uint32_t running = 0;
    uint32_t sleeping = 0;
    uint32_t d_sleep = 0;
    uint32_t stopped = 0;
    uint32_t zombie = 0;
    for (const auto& row : snap.rows) {
        switch (row.state) {
            case 'R':
                running++;
                break;
            case 'D':
                d_sleep++;
                break;
            case 'T':
            case 't':
                stopped++;
                break;
            case 'Z':
                zombie++;
                break;
            default:
                sleeping++;
                break;
        }
    }

    uint64_t const BUFF_CACHE = snap.mem.buffers_kib + snap.mem.cached_kib;
    uint64_t const USED = snap.mem.total_kib > snap.mem.free_kib + BUFF_CACHE ? snap.mem.total_kib - snap.mem.free_kib - BUFF_CACHE : 0;
    uint64_t const SWAP_USED = snap.mem.swap_total_kib > snap.mem.swap_free_kib ? snap.mem.swap_total_kib - snap.mem.swap_free_kib : 0;

    std::array<char, 256> line{};
    std::snprintf(line.data(), line.size(), "top - %s up %s,  1 user,  load average: %.2f, %.2f, %.2f", current_time_string().c_str(),
                  format_uptime(snap.uptime_seconds).c_str(), snap.load.one, snap.load.five, snap.load.fifteen);
    append_screen_line(line.data());
    std::snprintf(line.data(), line.size(), "Tasks: %zu total, %u running, %u sleep, %u d-sleep, %u stopped, %u zombie", snap.rows.size(),
                  running, sleeping, d_sleep, stopped, zombie);
    append_screen_line(line.data());
    std::snprintf(line.data(), line.size(), "%%Cpu(s): %4.1f us, %4.1f sy, %4.1f ni, %4.1f id, %4.1f wa, %4.1f hi, %4.1f si, %4.1f st",
                  CPU.user, CPU.system, CPU.nice, CPU.idle, CPU.iowait, CPU.irq, CPU.softirq, CPU.steal);
    append_screen_line(line.data());
    std::snprintf(line.data(), line.size(), "MiB Mem : %8.1f total, %8.1f free, %8.1f used, %8.1f buff/cache",
                  static_cast<double>(snap.mem.total_kib) / 1024.0, static_cast<double>(snap.mem.free_kib) / 1024.0,
                  static_cast<double>(USED) / 1024.0, static_cast<double>(BUFF_CACHE) / 1024.0);
    append_screen_line(line.data());
    std::snprintf(line.data(), line.size(), "MiB Swap: %8.1f total, %8.1f free, %8.1f used. %8.1f avail Mem",
                  static_cast<double>(snap.mem.swap_total_kib) / 1024.0, static_cast<double>(snap.mem.swap_free_kib) / 1024.0,
                  static_cast<double>(SWAP_USED) / 1024.0, static_cast<double>(snap.mem.available_kib) / 1024.0);
    append_screen_line(line.data());
    append_screen_line("");
    append_screen_line("    PID USER     HOST          PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND");

    int const VISIBLE_ROWS = std::max(0, TERM.rows - rendered_lines);
    int const START = std::max(0, row_offset);
    int const END = std::min<int>(static_cast<int>(snap.rows.size()), START + VISIBLE_ROWS);
    for (int i = START; i < END; ++i) {
        const auto& row = snap.rows[static_cast<size_t>(i)];
        std::string const USER = format_user(row.user);
        std::string const HOST = format_host(row.host);
        std::string const VIRT = format_mem(row.virt_kib);
        std::string const RES = format_mem(row.resident_kib);
        std::string const SHR = format_mem(row.shared_kib);
        std::string const TIME = format_time_ticks(row.total_ticks);
        int const COMMAND_WIDTH = std::max(COMMAND_WIDTH_FALLBACK, TERM.cols - 93);
        std::snprintf(line.data(), line.size(), "%7llu %-8s %-12s %3lld %3lld %7s %6s %6s %c %5.1f %5.1f %9s %-*.*s",
                      static_cast<unsigned long long>(row.pid), USER.c_str(), HOST.c_str(), static_cast<long long>(row.priority),
                      static_cast<long long>(row.nice), VIRT.c_str(), RES.c_str(), SHR.c_str(), row.state, row.cpu_pct, row.mem_pct,
                      TIME.c_str(), COMMAND_WIDTH, COMMAND_WIDTH, row.command.c_str());
        if (!append_screen_line(line.data())) {
            break;
        }
    }

    return out;
}

}  // namespace top
