// perf — kernel performance measurement tool for WOS
//
// Usage:
//   perf stat     [ms=1000]   CPU% per process over given sampling window
//   perf record   [ms=1000]   Enable kernel event recording for N ms, save to perf.data
//   perf report   [n=2000]    Display events from perf.data (or live /proc/kperf)
//   perf sched    [n=2000]    Alias for perf report
//   perf cpustat              Per-CPU aggregate scheduler statistics
//   perf top      [ms=1000]   Continuous stat snapshots (Ctrl-C to stop)
//   perf run      <cmd> [args] Trace cmd+descendants, save to perf.data
//   perf show-map             Show PID->name/cmdline map from perf.data

#include <dirent.h>
#include <fcntl.h>
#include <sys/process.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <optional>
#include <print>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern char** environ;

namespace {
constexpr uint8_t FLAG_USER_MODE = 0x01;
constexpr uint8_t FLAG_PREEMPT = 0x02;
constexpr uint8_t FLAG_YIELD = 0x04;
constexpr uint8_t FLAG_BLOCK = 0x08;
constexpr uint8_t FLAG_TIMED = 0x10;
constexpr uint8_t FLAG_EXPLICIT_WAKE = 0x20;
constexpr uint8_t FLAG_WAKE_CURRENT = 0x40;

constexpr uint32_t SHORT_RUN_THRESHOLD_US = 4000;
constexpr uint32_t SHORT_SLEEP_THRESHOLD_US = 20000;
constexpr int DEFAULT_MAX_EVENTS = 2000;
constexpr int DEFAULT_SAMPLE_MS = 1000;
constexpr double MIN_CPU_PCT = 0.01;

constexpr int64_t MILLISECONDS_PER_SECOND = 1000;
constexpr int64_t MICROSECONDS_PER_MILLISECOND = 1000;
constexpr int64_t NANOSECONDS_PER_MICROSECOND = 1000;
constexpr int64_t NANOSECONDS_PER_MILLISECOND = 1000000;
constexpr int SLEEP_SLICE_MS = 10;
constexpr int PARSE_BASE_DECIMAL = 10;
constexpr int PROC_STAT_SKIP_FIELDS = 8;
constexpr int PROC_WAIT_NOHANG = 1;
constexpr char EXITED_STATE = 'Z';
constexpr int DRAIN_INTERVAL_MS = 250;
constexpr int EVENT_MIN_TOKEN_COUNT = 4;
constexpr int EVENT_EXTENDED_TOKEN_COUNT = 6;
constexpr int HOTSPOT_ROW_LIMIT = 10;
constexpr int EXEC_FAILURE_EXIT_CODE = 127;

constexpr std::size_t INITIAL_FILE_CAPACITY = 131072;
constexpr std::size_t PROC_READ_CAPACITY = 512;
constexpr std::size_t PERF_DRAIN_CAPACITY = 65536;
constexpr std::size_t CPUSTAT_READ_CAPACITY = 4096;
constexpr std::size_t MAX_EVENT_TOKENS = 10;

constexpr std::size_t PID_PREFIX_SIZE = 4;
constexpr std::size_t COMM_PREFIX_SIZE = 6;
constexpr std::size_t CMD_PREFIX_SIZE = 5;

constexpr mode_t PERF_DATA_MODE = 0644;

constexpr std::string_view PERF_DATA_FILE = "perf.data";
constexpr std::string_view PROC_ROOT = "/proc/";
constexpr std::string_view PROC_STAT_SUFFIX = "/stat";
constexpr std::string_view PROC_CMDLINE_SUFFIX = "/cmdline";
constexpr std::string_view KPERF_PATH = "/proc/kperf";
constexpr std::string_view KPERFCTL_PATH = "/proc/kperfctl";
constexpr std::string_view KCPUSTAT_PATH = "/proc/kcpustat";
constexpr std::string_view SECTION_HEADER = "--- SECTION";
constexpr std::string_view SECTION_EVENTS = "--- SECTION EVENTS ---\n";
constexpr std::string_view SECTION_EVENTS_END = "--- END EVENTS ---\n";
constexpr std::string_view SECTION_PROC_MAP = "--- SECTION PROC_MAP ---\n";
constexpr std::string_view SECTION_PROC_MAP_END = "--- END PROC_MAP ---\n";
constexpr std::string_view END_PREFIX = "--- END";
constexpr std::string_view UNKNOWN_CALLSITE = "?";
constexpr std::string_view COMM_FIELD_PREFIX = " comm=";
constexpr std::string_view CMD_FIELD_PREFIX = " cmd=";

class ScopedFd {
   public:
    explicit ScopedFd(int fd = -1) : fd(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    auto operator=(const ScopedFd&) -> ScopedFd& = delete;

    ScopedFd(ScopedFd&& other) noexcept : fd(std::exchange(other.fd, -1)) {}

    auto operator=(ScopedFd&& other) noexcept -> ScopedFd& {
        if (this != &other) {
            reset();
            fd = std::exchange(other.fd, -1);
        }
        return *this;
    }

    ~ScopedFd() { reset(); }

    auto get() const -> int { return fd; }

    auto valid() const -> bool { return fd >= 0; }

    void reset(int new_fd = -1) {
        if (fd >= 0) {
            close(fd);
        }
        fd = new_fd;
    }

   private:
    int fd;
};

class ScopedDir {
   public:
    explicit ScopedDir(DIR* dir = nullptr) : dir(dir) {}
    ScopedDir(const ScopedDir&) = delete;
    auto operator=(const ScopedDir&) -> ScopedDir& = delete;

    ScopedDir(ScopedDir&& other) noexcept : dir(std::exchange(other.dir, nullptr)) {}

    auto operator=(ScopedDir&& other) noexcept -> ScopedDir& {
        if (this != &other) {
            reset();
            dir = std::exchange(other.dir, nullptr);
        }
        return *this;
    }

    ~ScopedDir() { reset(); }

    auto get() const -> DIR* { return dir; }

    auto valid() const -> bool { return dir != nullptr; }

    void reset(DIR* new_dir = nullptr) {
        if (dir != nullptr) {
            closedir(dir);
        }
        dir = new_dir;
    }

   private:
    DIR* dir;
};

struct StatInfo {
    uint64_t pid{};
    uint64_t pgid{};
    std::string comm;
    char state{};
    uint64_t utime{};
    uint64_t stime{};
};

struct ProcMapEntry {
    uint64_t pid{};
    std::string comm;
};

struct EventInfo {
    char type{};
    uint64_t ts_ns{};
    uint32_t cpu{};
    uint64_t pid{};
    uint64_t other_pid{};
    uint64_t data{};
    std::string callsite;
    int64_t lag{};
    uint8_t flags{};
    uint32_t aux{};
};

struct HotspotStats {
    uint64_t pid{};
    std::string callsite;
    std::string comm;
    uint64_t yield_count{};
    uint64_t short_yield_count{};
    uint64_t yield_run_total_us{};
    uint32_t yield_run_max_us{};
    uint64_t sleep_count{};
    uint64_t short_sleep_count{};
    uint64_t sleep_run_total_us{};
    uint64_t wake_count{};
    uint64_t short_wake_count{};
    uint64_t wake_sleep_total_us{};
    uint32_t wake_sleep_max_us{};
    uint64_t explicit_wake_count{};
    uint64_t current_wake_count{};
};

struct TrackedProc {
    uint64_t pid{};
    std::string comm;
    std::string cmdline;
    uint64_t last_utime{};
    uint64_t last_stime{};
};

struct CpuRow {
    uint64_t pid{};
    std::string comm;
    char state{};
    double cpu_pct{};
    bool in_group{};
};

auto now_ms() -> int64_t {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<int64_t>(ts.tv_sec) * MILLISECONDS_PER_SECOND) + (static_cast<int64_t>(ts.tv_nsec) / NANOSECONDS_PER_MILLISECOND);
}

void wall_sleep_ms(int target_ms) {
    int64_t deadline = now_ms() + target_ms;
    while (true) {
        int64_t remaining = deadline - now_ms();
        if (remaining <= 0) {
            break;
        }

        int64_t slice_us = std::min<int64_t>(remaining, SLEEP_SLICE_MS) * MICROSECONDS_PER_MILLISECOND;
        timespec req{
            .tv_sec = static_cast<time_t>(slice_us / (MILLISECONDS_PER_SECOND * MICROSECONDS_PER_MILLISECOND)),
            .tv_nsec =
                static_cast<long>((slice_us % (MILLISECONDS_PER_SECOND * MICROSECONDS_PER_MILLISECOND)) * NANOSECONDS_PER_MICROSECOND),
        };
        nanosleep(&req, nullptr);
    }
}

auto open_readonly(std::string_view path) -> ScopedFd {
    std::string owned_path(path);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    return ScopedFd(open(owned_path.c_str(), O_RDONLY, 0));
}

auto open_writeonly(std::string_view path) -> ScopedFd {
    std::string owned_path(path);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    return ScopedFd(open(owned_path.c_str(), O_WRONLY, 0));
}

auto open_write_trunc(std::string_view path, mode_t mode = PERF_DATA_MODE) -> ScopedFd {
    std::string owned_path(path);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    return ScopedFd(open(owned_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode));
}

void write_all(int fd, std::string_view text) {
    std::size_t written = 0;
    while (written < text.size()) {
        ssize_t rc = write(fd, text.data() + written, text.size() - written);
        if (rc <= 0) {
            break;
        }
        written += static_cast<std::size_t>(rc);
    }
}

auto read_fd(ScopedFd& fd, std::size_t initial_capacity = INITIAL_FILE_CAPACITY) -> std::string {
    std::size_t capacity = std::max<std::size_t>(initial_capacity, 1);
    std::string buffer(capacity, '\0');
    std::size_t total = 0;

    for (;;) {
        if (total == buffer.size()) {
            buffer.resize(buffer.size() * 2);
        }

        ssize_t count = read(fd.get(), buffer.data() + total, buffer.size() - total);
        if (count <= 0) {
            break;
        }
        total += static_cast<std::size_t>(count);
    }

    buffer.resize(total);
    return buffer;
}

auto read_file(std::string_view path, std::size_t initial_capacity = INITIAL_FILE_CAPACITY) -> std::optional<std::string> {
    ScopedFd fd = open_readonly(path);
    if (!fd.valid()) {
        return std::nullopt;
    }
    return read_fd(fd, initial_capacity);
}

auto build_proc_path(std::string_view pid, std::string_view suffix) -> std::string {
    std::string path(PROC_ROOT);
    path += pid;
    path += suffix;
    return path;
}

auto build_proc_path(uint64_t pid, std::string_view suffix) -> std::string { return build_proc_path(std::to_string(pid), suffix); }

auto parse_u64(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint64_t {
    std::string owned(text);
    return static_cast<uint64_t>(strtoull(owned.c_str(), nullptr, base));
}

auto parse_i64(std::string_view text, int base = PARSE_BASE_DECIMAL) -> int64_t {
    std::string owned(text);
    return static_cast<int64_t>(strtoll(owned.c_str(), nullptr, base));
}

auto parse_u32(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint32_t {
    std::string owned(text);
    return static_cast<uint32_t>(strtoul(owned.c_str(), nullptr, base));
}

auto parse_u8(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint8_t {
    std::string owned(text);
    return static_cast<uint8_t>(strtoul(owned.c_str(), nullptr, base));
}

auto trim_left(std::string_view text) -> std::string_view {
    while (!text.empty() && text.front() == ' ') {
        text.remove_prefix(1);
    }
    return text;
}

auto next_token(std::string_view text, std::size_t& pos) -> std::string_view {
    while (pos < text.size() && text[pos] == ' ') {
        ++pos;
    }
    if (pos >= text.size()) {
        return {};
    }

    std::size_t end = pos;
    while (end < text.size() && text[end] != ' ') {
        ++end;
    }

    std::string_view token = text.substr(pos, end - pos);
    pos = end;
    return token;
}

auto next_line(std::string_view text, std::size_t& pos) -> std::string_view {
    if (pos >= text.size()) {
        return {};
    }

    std::size_t end = text.find('\n', pos);
    if (end == std::string_view::npos) {
        std::string_view line = text.substr(pos);
        pos = text.size();
        return line;
    }

    std::string_view line = text.substr(pos, end - pos);
    pos = end + 1;
    return line;
}

auto is_all_digits(std::string_view text) -> bool {
    if (text.empty()) {
        return false;
    }

    return std::ranges::all_of(text, [](char ch) { return ch >= '0' && ch <= '9'; });
}

auto parse_stat(std::string_view buf, StatInfo& out) -> bool {
    std::size_t paren = buf.find('(');
    std::size_t paren_end = buf.rfind(')');
    if (paren == std::string_view::npos || paren_end == std::string_view::npos || paren_end <= paren) {
        return false;
    }

    out.pid = parse_u64(trim_left(buf.substr(0, paren)));
    out.comm = std::string(buf.substr(paren + 1, paren_end - paren - 1));

    std::string_view tail = trim_left(buf.substr(paren_end + 1));
    std::size_t pos = 0;

    std::string_view state_token = next_token(tail, pos);
    if (state_token.empty()) {
        return false;
    }
    out.state = state_token.front();

    if (next_token(tail, pos).empty()) {
        return false;
    }

    std::string_view pgid_token = next_token(tail, pos);
    if (pgid_token.empty()) {
        return false;
    }
    out.pgid = parse_u64(pgid_token);

    for (int index = 0; index < PROC_STAT_SKIP_FIELDS; ++index) {
        if (next_token(tail, pos).empty()) {
            return false;
        }
    }

    std::string_view utime_token = next_token(tail, pos);
    std::string_view stime_token = next_token(tail, pos);
    if (utime_token.empty() || stime_token.empty()) {
        return false;
    }

    out.utime = parse_u64(utime_token);
    out.stime = parse_u64(stime_token);
    return true;
}

template <typename Func>
void for_each_process_stat(Func func) {
    ScopedDir dir(opendir("/proc"));
    if (!dir.valid()) {
        return;
    }

    dirent* entry = nullptr;
    while ((entry = readdir(dir.get())) != nullptr) {
        std::string_view name{&entry->d_name[0]};
        if (!is_all_digits(name)) {
            continue;
        }

        auto stat_text = read_file(build_proc_path(name, PROC_STAT_SUFFIX), PROC_READ_CAPACITY);
        if (!stat_text.has_value()) {
            continue;
        }

        StatInfo info{};
        if (!parse_stat(*stat_text, info)) {
            continue;
        }

        func(info, name);
    }
}

auto collect_stats() -> std::vector<StatInfo> {
    std::vector<StatInfo> stats;
    for_each_process_stat([&](const StatInfo& info, std::string_view) { stats.push_back(info); });
    return stats;
}

auto read_cmdline(uint64_t pid) -> std::string {
    auto raw = read_file(build_proc_path(pid, PROC_CMDLINE_SUFFIX), PROC_READ_CAPACITY);
    if (!raw.has_value() || raw->empty()) {
        return {};
    }

    for (char& ch : *raw) {
        if (ch == '\0') {
            ch = ' ';
        }
    }

    while (!raw->empty() && raw->back() == ' ') {
        raw->pop_back();
    }
    return *raw;
}

auto proc_map_line(uint64_t pid, std::string_view comm, std::string_view cmdline) -> std::string {
    std::string line = "pid=";
    line += std::to_string(pid);
    line += " comm=";
    line += comm;
    line += " cmd=";
    line += cmdline.empty() ? std::string(comm) : std::string(cmdline);
    line += '\n';
    return line;
}

void write_section_proc_map(int fd) {
    write_all(fd, SECTION_PROC_MAP);
    for_each_process_stat(
        [&](const StatInfo& info, std::string_view) { write_all(fd, proc_map_line(info.pid, info.comm, read_cmdline(info.pid))); });
    write_all(fd, SECTION_PROC_MAP_END);
}

auto write_section_events(int fd) -> ssize_t {
    auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
    if (!events.has_value() || events->empty()) {
        return 0;
    }

    write_all(fd, SECTION_EVENTS);
    write_all(fd, *events);
    write_all(fd, SECTION_EVENTS_END);
    return static_cast<ssize_t>(events->size());
}

void save_perf_data() {
    ScopedFd fd = open_write_trunc(PERF_DATA_FILE);
    if (!fd.valid()) {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
        return;
    }

    write_section_proc_map(fd.get());
    ssize_t event_bytes = write_section_events(fd.get());

    if (event_bytes <= 0) {
        std::println("perf: ring buffer empty — PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes)", PERF_DATA_FILE, event_bytes);
    }
}

void set_recording_enabled(bool enabled) {
    ScopedFd fd = open_writeonly(KPERFCTL_PATH);
    if (!fd.valid()) {
        return;
    }
    write_all(fd.get(), enabled ? "enable" : "disable");
}

void cmd_stat(int ms) {
    if (ms < 1) {
        ms = DEFAULT_SAMPLE_MS;
    }

    auto before = collect_stats();
    int64_t start_ms = now_ms();
    wall_sleep_ms(ms);
    int64_t elapsed_ms = now_ms() - start_ms;
    if (elapsed_ms < 1) {
        elapsed_ms = ms;
    }

    auto after = collect_stats();
    std::vector<CpuRow> rows;
    rows.reserve(after.size());

    for (const auto& prior : before) {
        for (const auto& current : after) {
            if (current.pid != prior.pid) {
                continue;
            }

            uint64_t delta_ticks = (current.utime + current.stime) - (prior.utime + prior.stime);
            double cpu = static_cast<double>(delta_ticks) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);

            rows.push_back(CpuRow{
                .pid = current.pid,
                .comm = current.comm,
                .state = current.state,
                .cpu_pct = cpu,
                .in_group = false,
            });
            break;
        }
    }

    std::ranges::sort(rows, [](const CpuRow& lhs, const CpuRow& rhs) { return lhs.cpu_pct > rhs.cpu_pct; });

    std::println("=== perf stat ({}ms, actual {}ms) ====================================", ms, elapsed_ms);
    std::println("{:>6}  {:<20}  {:>5}  {}", "PID", "NAME", "STATE", "CPU%");
    std::println("{:->6}  {:->20}  {:->5}  {:->7}", "", "", "", "");
    for (const auto& row : rows) {
        if (row.cpu_pct < MIN_CPU_PCT) {
            continue;
        }
        std::println("{:>6}  {:<20}  {:>5}  {:>6.1f}%", row.pid, row.comm, row.state, row.cpu_pct);
    }
}

void cmd_record(int ms) {
    if (ms < 1) {
        ms = DEFAULT_SAMPLE_MS;
    }

    ScopedFd control_fd = open_writeonly(KPERFCTL_PATH);
    if (!control_fd.valid()) {
        std::println("perf: cannot open /proc/kperfctl");
        return;
    }

    write_all(control_fd.get(), "enable");
    std::println("perf: recording for {} ms...", ms);
    wall_sleep_ms(ms);
    write_all(control_fd.get(), "disable");
    std::println("perf: recording stopped.");
    save_perf_data();
}

auto parse_proc_map_section(std::string_view buffer) -> std::vector<ProcMapEntry> {
    std::vector<ProcMapEntry> entries;
    std::size_t header_pos = buffer.find(SECTION_PROC_MAP);
    if (header_pos == std::string_view::npos) {
        return entries;
    }

    std::size_t pos = buffer.find('\n', header_pos);
    if (pos == std::string_view::npos) {
        return entries;
    }
    ++pos;

    while (pos < buffer.size()) {
        std::string_view line = next_line(buffer, pos);
        if (line.starts_with(END_PREFIX)) {
            break;
        }
        if (line.empty()) {
            continue;
        }

        std::size_t pid_pos = line.find("pid=");
        std::size_t comm_pos = line.find(" comm=");
        if (pid_pos == std::string_view::npos || comm_pos == std::string_view::npos) {
            continue;
        }

        std::size_t cmd_pos = line.find(CMD_FIELD_PREFIX, comm_pos + 1);
        std::string_view pid_text = line.substr(pid_pos + PID_PREFIX_SIZE, comm_pos - (pid_pos + PID_PREFIX_SIZE));
        std::string_view comm_text = cmd_pos == std::string_view::npos
                                         ? line.substr(comm_pos + COMM_PREFIX_SIZE)
                                         : line.substr(comm_pos + COMM_PREFIX_SIZE, cmd_pos - (comm_pos + COMM_PREFIX_SIZE));

        entries.push_back(ProcMapEntry{.pid = parse_u64(pid_text), .comm = std::string(comm_text)});
    }

    return entries;
}

auto parse_event_line(std::string_view line, EventInfo& out) -> bool {
    constexpr int TOK_TS = 0;
    constexpr int TOK_CPU = 1;
    constexpr int TOK_PID = 2;
    constexpr int TOK_DATA = 3;
    constexpr int TOK_LAG = 4;
    constexpr int TOK_FLAGS = 5;
    constexpr int TOK_AUX = 6;
    constexpr int TOK_CALLSITE = 7;

    out = {};
    if (line.size() < 2 || line[1] != ' ') {
        return false;
    }

    std::array<std::string_view, MAX_EVENT_TOKENS> tokens{};
    int token_count = 0;
    std::size_t pos = 2;
    while (token_count < static_cast<int>(tokens.size())) {
        std::string_view token = next_token(line, pos);
        if (token.empty()) {
            break;
        }
        tokens[static_cast<std::size_t>(token_count)] = token;
        ++token_count;
    }

    if (token_count < EVENT_MIN_TOKEN_COUNT) {
        return false;
    }

    out.type = line.front();
    out.ts_ns = parse_u64(tokens[TOK_TS]);
    out.cpu = parse_u32(tokens[TOK_CPU]);

    switch (out.type) {
        case 'S':
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens[TOK_PID]);
            out.data = parse_u64(tokens[TOK_DATA], 0);
            out.lag = parse_i64(tokens[TOK_LAG]);
            out.flags = parse_u8(tokens[TOK_FLAGS]);
            return true;

        case 'X':
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens[TOK_PID]);
            out.other_pid = parse_u64(tokens[TOK_DATA]);
            out.lag = parse_i64(tokens[TOK_LAG]);
            out.flags = parse_u8(tokens[TOK_FLAGS]);
            if (token_count >= TOK_AUX + 1) {
                out.aux = parse_u32(tokens[TOK_AUX]);
            }
            if (token_count >= TOK_CALLSITE + 1) {
                out.callsite = std::string(tokens[TOK_CALLSITE]);
            }
            return true;

        case 'W':
        case 'B':
            out.pid = parse_u64(tokens[TOK_PID]);
            out.data = parse_u64(tokens[TOK_DATA]);
            if (token_count >= EVENT_EXTENDED_TOKEN_COUNT) {
                out.aux = parse_u32(tokens[TOK_LAG]);
                out.flags = parse_u8(tokens[TOK_FLAGS]);
                if (token_count >= TOK_CALLSITE + 1) {
                    out.callsite = std::string(tokens[TOK_CALLSITE]);
                }
            } else {
                out.flags = static_cast<uint8_t>((out.type == 'B' ? FLAG_BLOCK : 0U) | (out.data != 0 ? FLAG_TIMED : 0U));
            }
            return true;

        default:
            return false;
    }
}

auto next_event(std::string_view text, std::size_t& pos, bool sectioned, EventInfo& out) -> bool {
    while (pos < text.size()) {
        std::size_t line_start = pos;
        std::string_view line = next_line(text, pos);
        if (sectioned && text.substr(line_start).starts_with(END_PREFIX)) {
            return false;
        }
        if (line.empty()) {
            continue;
        }
        if (parse_event_line(line, out)) {
            return true;
        }
    }
    return false;
}

auto comm_of_pid(const std::vector<ProcMapEntry>& proc_map, uint64_t pid) -> std::string_view {
    for (const auto& entry : proc_map) {
        if (entry.pid == pid) {
            return entry.comm;
        }
    }
    return {};
}

auto format_pid_name(uint64_t pid, const std::vector<ProcMapEntry>& proc_map) -> std::string {
    std::string_view comm = comm_of_pid(proc_map, pid);
    if (!comm.empty()) {
        std::string name = std::to_string(pid);
        name += '(';
        name += comm;
        name += ')';
        return name;
    }
    return std::to_string(pid);
}

auto display_callsite(std::string_view callsite) -> std::string_view { return callsite.empty() ? UNKNOWN_CALLSITE : callsite; }

auto hotspot_for(std::vector<HotspotStats>& rows, uint64_t pid, std::string_view callsite, const std::vector<ProcMapEntry>& proc_map)
    -> HotspotStats& {
    std::string_view display_site = display_callsite(callsite);
    for (auto& row : rows) {
        if (row.pid == pid && row.callsite == display_site) {
            return row;
        }
    }

    HotspotStats row{};
    row.pid = pid;
    row.callsite = std::string(display_site);
    row.comm = std::string(comm_of_pid(proc_map, pid));
    rows.push_back(std::move(row));
    return rows.back();
}

auto summarize_hotspots(std::string_view events, bool sectioned, const std::vector<ProcMapEntry>& proc_map) -> std::vector<HotspotStats> {
    std::vector<HotspotStats> rows;
    std::size_t pos = 0;
    EventInfo event{};
    while (next_event(events, pos, sectioned, event)) {
        auto& row = hotspot_for(rows, event.pid, event.callsite, proc_map);
        switch (event.type) {
            case 'X':
                if ((event.flags & FLAG_YIELD) != 0U) {
                    row.yield_count++;
                    row.yield_run_total_us += event.aux;
                    row.yield_run_max_us = std::max(row.yield_run_max_us, event.aux);
                    if (event.aux != 0U && event.aux <= SHORT_RUN_THRESHOLD_US) {
                        row.short_yield_count++;
                    }
                }
                break;

            case 'B':
                row.sleep_count++;
                row.sleep_run_total_us += event.aux;
                if (event.aux != 0U && event.aux <= SHORT_RUN_THRESHOLD_US) {
                    row.short_sleep_count++;
                }
                break;

            case 'W':
                row.wake_count++;
                row.wake_sleep_total_us += event.aux;
                row.wake_sleep_max_us = std::max(row.wake_sleep_max_us, event.aux);
                if (event.aux != 0U && event.aux <= SHORT_SLEEP_THRESHOLD_US) {
                    row.short_wake_count++;
                }
                if ((event.flags & FLAG_EXPLICIT_WAKE) != 0U) {
                    row.explicit_wake_count++;
                }
                if ((event.flags & FLAG_WAKE_CURRENT) != 0U) {
                    row.current_wake_count++;
                }
                break;

            default:
                break;
        }
    }
    return rows;
}

void print_sched_flags(uint8_t flags) {
    bool first = true;
    auto print_flag = [&](std::string_view flag) {
        if (!first) {
            std::print("|");
        }
        std::print("{}", flag);
        first = false;
    };

    if ((flags & FLAG_USER_MODE) != 0U) {
        print_flag("USER");
    }
    if ((flags & FLAG_PREEMPT) != 0U) {
        print_flag("PREEMPT");
    }
    if ((flags & FLAG_YIELD) != 0U) {
        print_flag("YIELD");
    }
    if ((flags & FLAG_BLOCK) != 0U) {
        print_flag("BLOCK");
    }
    if (first) {
        std::print("KERN");
    }
}

void print_wait_flags(uint8_t flags) {
    bool first = true;
    auto print_flag = [&](std::string_view flag) {
        if (!first) {
            std::print("|");
        }
        std::print("{}", flag);
        first = false;
    };

    if ((flags & FLAG_BLOCK) != 0U) {
        print_flag("BLOCK");
    }
    if ((flags & FLAG_TIMED) != 0U) {
        print_flag("TIMED");
    }
    if ((flags & FLAG_EXPLICIT_WAKE) != 0U) {
        print_flag("EXPLICIT");
    }
    if ((flags & FLAG_WAKE_CURRENT) != 0U) {
        print_flag("CURRENT");
    }
    if (first) {
        std::print("NONE");
    }
}

void print_hotspot_tables(const std::vector<HotspotStats>& rows) {
    std::vector<HotspotStats> busy_yield = rows;
    auto removed = std::ranges::remove_if(busy_yield, [](const HotspotStats& row) { return row.yield_count == 0; });
    busy_yield.erase(removed.begin(), removed.end());
    std::ranges::sort(busy_yield, [](const HotspotStats& lhs, const HotspotStats& rhs) {
        if (lhs.short_yield_count != rhs.short_yield_count) {
            return lhs.short_yield_count > rhs.short_yield_count;
        }
        if (lhs.yield_count != rhs.yield_count) {
            return lhs.yield_count > rhs.yield_count;
        }
        return lhs.yield_run_total_us > rhs.yield_run_total_us;
    });

    std::println("=== busy-yield hotspots (short <= {}us) =============================", SHORT_RUN_THRESHOLD_US);
    if (busy_yield.empty()) {
        std::println("none");
    } else {
        std::println("{:>6}  {:<20}  {:<18}  {:>7}  {:>7}  {:>11}  {:>11}", "PID", "NAME", "CALLSITE", "yield", "short", "avg_run(us)",
                     "max_run(us)");
        std::println("{:->6}  {:->20}  {:->18}  {:->7}  {:->7}  {:->11}  {:->11}", "", "", "", "", "", "", "");
        for (std::size_t index = 0; index < busy_yield.size() && index < HOTSPOT_ROW_LIMIT; ++index) {
            const auto& row = busy_yield[index];
            uint64_t avg_run = row.yield_count != 0U ? row.yield_run_total_us / row.yield_count : 0;
            std::println("{:>6}  {:<20}  {:<18}  {:>7}  {:>7}  {:>11}  {:>11}", row.pid, row.comm.empty() ? "?" : row.comm,
                         display_callsite(row.callsite), row.yield_count, row.short_yield_count, avg_run, row.yield_run_max_us);
        }
    }

    std::println("");

    std::vector<HotspotStats> wake_churn = rows;
    auto removed_churn =
        std::ranges::remove_if(wake_churn, [](const HotspotStats& row) { return row.wake_count == 0 && row.sleep_count == 0; });
    wake_churn.erase(removed_churn.begin(), removed_churn.end());
    std::ranges::sort(wake_churn, [](const HotspotStats& lhs, const HotspotStats& rhs) {
        uint64_t lhs_score = (lhs.short_wake_count * 3U) + (lhs.explicit_wake_count * 2U) + lhs.short_sleep_count;
        uint64_t rhs_score = (rhs.short_wake_count * 3U) + (rhs.explicit_wake_count * 2U) + rhs.short_sleep_count;
        if (lhs_score != rhs_score) {
            return lhs_score > rhs_score;
        }
        if (lhs.wake_count != rhs.wake_count) {
            return lhs.wake_count > rhs.wake_count;
        }
        return lhs.sleep_count > rhs.sleep_count;
    });

    std::println("=== wake/sleep churn hotspots (short sleep <= {}us) =================", SHORT_SLEEP_THRESHOLD_US);
    if (wake_churn.empty()) {
        std::println("none");
    } else {
        std::println("{:>6}  {:<20}  {:<18}  {:>5}  {:>5}  {:>8}  {:>7}  {:>13}  {:>11}", "PID", "NAME", "CALLSITE", "wake", "short",
                     "explicit", "current", "avg_sleep(us)", "avg_run(us)");
        std::println("{:->6}  {:->20}  {:->18}  {:->5}  {:->5}  {:->8}  {:->7}  {:->13}  {:->11}", "", "", "", "", "", "", "", "", "");
        for (std::size_t index = 0; index < wake_churn.size() && index < HOTSPOT_ROW_LIMIT; ++index) {
            const auto& row = wake_churn[index];
            uint64_t avg_sleep = row.wake_count != 0U ? row.wake_sleep_total_us / row.wake_count : 0;
            uint64_t avg_run = row.sleep_count != 0U ? row.sleep_run_total_us / row.sleep_count : 0;
            std::println("{:>6}  {:<20}  {:<18}  {:>5}  {:>5}  {:>8}  {:>7}  {:>13}  {:>11}", row.pid, row.comm.empty() ? "?" : row.comm,
                         display_callsite(row.callsite), row.wake_count, row.short_wake_count, row.explicit_wake_count,
                         row.current_wake_count, avg_sleep, avg_run);
        }
    }

    std::println("");
}

void cmd_sched(int max_events) {
    if (max_events < 1) {
        max_events = DEFAULT_MAX_EVENTS;
    }

    std::string_view src = access(PERF_DATA_FILE.begin(), R_OK) == 0 ? PERF_DATA_FILE : KPERF_PATH;
    auto buffer = read_file(src);
    if (!buffer.has_value() || buffer->empty()) {
        std::println("perf: no data (run 'perf record' or 'perf run' first)");
        return;
    }

    std::string_view view(*buffer);
    bool sectioned = view.starts_with(SECTION_HEADER);
    std::vector<ProcMapEntry> proc_map;
    std::size_t section_start = 0;

    if (sectioned) {
        proc_map = parse_proc_map_section(view);
        std::size_t events_hdr = view.find(SECTION_EVENTS);
        if (events_hdr == std::string_view::npos) {
            std::println("perf: perf.data has no EVENTS section");
            return;
        }
        section_start = view.find('\n', events_hdr);
        if (section_start == std::string_view::npos) {
            return;
        }
        ++section_start;
    } else {
        for (const auto& stat : collect_stats()) {
            proc_map.push_back(ProcMapEntry{.pid = stat.pid, .comm = stat.comm});
        }
    }

    std::string_view event_view = view.substr(section_start);
    auto hotspot_rows = summarize_hotspots(event_view, sectioned, proc_map);
    print_hotspot_tables(hotspot_rows);

    std::println("=== perf report [{}] (up to {} events) ============================", src, max_events);
    std::println("{:>3}  {:>18}  {:>3}  {}", "EVT", "TIME(ns)", "CPU", "DETAILS");
    std::println("{:->3}  {:->18}  {:->3}  {:->40}", "", "", "", "");

    int count = 0;
    EventInfo event{};
    std::size_t event_pos = 0;
    while (count < max_events && next_event(event_view, event_pos, sectioned, event)) {
        if (event.type == 'S') {
            std::print("SMP  {:>18}  {:>3}  {:<24} rip={:#016x} lag={:>10}  ", event.ts_ns, event.cpu, format_pid_name(event.pid, proc_map),
                       event.data, event.lag);
            print_sched_flags(event.flags);
            std::println("");
        } else if (event.type == 'X') {
            std::print("CTX  {:>18}  {:>3}  {:<24} -> {:<24} lag={:>10} run_us={:>6} site={}  ", event.ts_ns, event.cpu,
                       format_pid_name(event.pid, proc_map), format_pid_name(event.other_pid, proc_map), event.lag, event.aux,
                       display_callsite(event.callsite));
            print_sched_flags(event.flags);
            std::println("");
        } else if (event.type == 'W') {
            std::print("WKE  {:>18}  {:>3}  {:<24} wake_at_us={} sleep_us={:>6} site={}  ", event.ts_ns, event.cpu,
                       format_pid_name(event.pid, proc_map), event.data, event.aux, display_callsite(event.callsite));
            print_wait_flags(event.flags);
            std::println("");
        } else if (event.type == 'B') {
            std::print("SLP  {:>18}  {:>3}  {:<24} wake_at_us={} run_us={:>6} site={}  ", event.ts_ns, event.cpu,
                       format_pid_name(event.pid, proc_map), event.data, event.aux, display_callsite(event.callsite));
            print_wait_flags(event.flags);
            std::println("");
        }

        ++count;
    }
    std::println("=== {} events ==", count);
}

void cmd_cpustat() {
    auto buffer = read_file(KCPUSTAT_PATH, CPUSTAT_READ_CAPACITY);
    if (!buffer.has_value() || buffer->empty()) {
        std::println("perf: cannot read /proc/kcpustat");
        return;
    }

    std::println("=== perf cpustat ===================================================");
    std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}", "CPU", "ctx", "preempt", "yield", "sleep", "wake", "sample");
    std::println("{:->4}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}", "", "", "", "", "", "", "");

    std::size_t pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t end = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, end == std::string_view::npos ? line.size() - key_pos : end - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}", get_val("cpu="), get_val("ctx="), get_val("preempt="),
                     get_val("yield="), get_val("sleep="), get_val("wake="), get_val("sample="));
    }
}

void cmd_run(int argc, char** argv) {
    if (argc < 1) {
        std::println("perf run: no command specified");
        return;
    }

    auto before = collect_stats();
    int64_t start_ms = now_ms();
    set_recording_enabled(true);

    int64_t child_pid = ker::process::fork();
    if (child_pid == 0) {
        ker::process::setpgid(0, 0);
        const auto* exec_argv = const_cast<const char* const*>(argv);
        const auto* exec_envp = const_cast<const char* const*>(environ);
        ker::process::execve(argv[0], exec_argv, exec_envp);
        exit(EXEC_FAILURE_EXIT_CODE);
    }

    if (child_pid < 0) {
        std::println("perf run: fork failed ({})", child_pid);
        set_recording_enabled(false);
        return;
    }

    int64_t target_pgid = child_pid;
    std::println("perf run: tracing pgid={} cmd={}", target_pgid, argv[0]);

    std::vector<TrackedProc> tracked;
    auto upsert_tracked = [&](const StatInfo& stat) {
        for (auto& proc : tracked) {
            if (proc.pid == stat.pid) {
                proc.last_utime = stat.utime;
                proc.last_stime = stat.stime;
                proc.comm = stat.comm;
                return;
            }
        }

        tracked.push_back(TrackedProc{
            .pid = stat.pid,
            .comm = stat.comm,
            .cmdline = read_cmdline(stat.pid),
            .last_utime = stat.utime,
            .last_stime = stat.stime,
        });
    };

    ScopedFd data_fd = open_write_trunc(PERF_DATA_FILE);
    if (data_fd.valid()) {
        write_all(data_fd.get(), SECTION_EVENTS);
    }

    int32_t status = 0;
    int64_t last_drain_ms = now_ms();
    ssize_t total_event_bytes = 0;

    for (;;) {
        while (ker::process::waitpid(-1, &status, PROC_WAIT_NOHANG, nullptr) > 0) {
        }

        bool any_alive = false;
        for_each_process_stat([&](const StatInfo& stat, std::string_view) {
            if (static_cast<int64_t>(stat.pgid) == target_pgid) {
                any_alive = true;
                upsert_tracked(stat);
            }
        });

        if (!any_alive) {
            break;
        }

        wall_sleep_ms(SLEEP_SLICE_MS);
        if (data_fd.valid() && now_ms() - last_drain_ms >= DRAIN_INTERVAL_MS) {
            auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
            if (events.has_value() && !events->empty()) {
                write_all(data_fd.get(), *events);
                total_event_bytes += static_cast<ssize_t>(events->size());
            }
            last_drain_ms = now_ms();
        }
    }

    while (ker::process::waitpid(-1, &status, 1, nullptr) > 0) {
    }

    int64_t elapsed_ms = now_ms() - start_ms;
    elapsed_ms = std::max<int64_t>(elapsed_ms, 1);

    set_recording_enabled(false);
    auto after = collect_stats();
    std::vector<CpuRow> rows;

    for (const auto& proc : tracked) {
        uint64_t ticks_after = proc.last_utime + proc.last_stime;
        uint64_t ticks_before = 0;
        for (const auto& prior : before) {
            if (prior.pid == proc.pid) {
                ticks_before = prior.utime + prior.stime;
                break;
            }
        }

        uint64_t delta = ticks_after >= ticks_before ? ticks_after - ticks_before : ticks_after;
        double cpu = static_cast<double>(delta) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);
        if (cpu < MIN_CPU_PCT) {
            continue;
        }

        char state = EXITED_STATE;
        for (const auto& current : after) {
            if (current.pid == proc.pid) {
                state = current.state;
                break;
            }
        }

        rows.push_back(CpuRow{
            .pid = proc.pid,
            .comm = proc.comm,
            .state = state,
            .cpu_pct = cpu,
            .in_group = true,
        });
    }

    for (const auto& prior : before) {
        bool was_tracked = false;
        for (const auto& proc : tracked) {
            if (proc.pid == prior.pid) {
                was_tracked = true;
                break;
            }
        }
        if (was_tracked) {
            continue;
        }

        for (const auto& current : after) {
            if (current.pid != prior.pid) {
                continue;
            }

            uint64_t delta = (current.utime + current.stime) - (prior.utime + prior.stime);
            double cpu = static_cast<double>(delta) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);
            if (cpu >= MIN_CPU_PCT) {
                rows.push_back(CpuRow{
                    .pid = current.pid,
                    .comm = current.comm,
                    .state = current.state,
                    .cpu_pct = cpu,
                    .in_group = false,
                });
            }
            break;
        }
    }

    std::ranges::sort(rows, [](const CpuRow& lhs, const CpuRow& rhs) { return lhs.cpu_pct > rhs.cpu_pct; });

    std::println("=== perf run stat (actual {}ms) =====================================", elapsed_ms);
    std::println("{:>6}  {:<20}  {:>5}  {:>7}  {}", "PID", "NAME", "STATE", "CPU%", "GROUP");
    std::println("{:->6}  {:->20}  {:->5}  {:->7}  {:->5}", "", "", "", "", "");
    for (const auto& row : rows) {
        std::println("{:>6}  {:<20}  {:>5}  {:>6.1f}%  {}", row.pid, row.comm, row.state, row.cpu_pct, row.in_group ? "*" : "");
    }
    std::println("");

    if (data_fd.valid()) {
        auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
        if (events.has_value() && !events->empty()) {
            write_all(data_fd.get(), *events);
            total_event_bytes += static_cast<ssize_t>(events->size());
        }

        write_all(data_fd.get(), SECTION_EVENTS_END);
        write_all(data_fd.get(), SECTION_PROC_MAP);

        for_each_process_stat([&](const StatInfo& stat, std::string_view) {
            write_all(data_fd.get(), proc_map_line(stat.pid, stat.comm, read_cmdline(stat.pid)));
        });

        for (const auto& proc : tracked) {
            bool alive = false;
            for (const auto& current : after) {
                if (current.pid == proc.pid) {
                    alive = true;
                    break;
                }
            }
            if (!alive) {
                write_all(data_fd.get(), proc_map_line(proc.pid, proc.comm, proc.cmdline));
            }
        }

        write_all(data_fd.get(), SECTION_PROC_MAP_END);

        if (total_event_bytes <= 0) {
            std::println("perf: ring buffer empty — PROC_MAP saved, no events");
        } else {
            std::println("perf: saved to {} ({} event bytes)", PERF_DATA_FILE, total_event_bytes);
        }
    } else {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
    }

    cmd_sched(DEFAULT_MAX_EVENTS);
}

void cmd_show_map() {
    if (access(PERF_DATA_FILE.begin(), R_OK) != 0) {
        std::println("perf: no perf.data found (run 'perf record' or 'perf run' first)");
        return;
    }

    auto buffer = read_file(PERF_DATA_FILE);
    if (!buffer.has_value() || buffer->empty()) {
        return;
    }

    std::string_view view(*buffer);
    std::size_t header_pos = view.find(SECTION_PROC_MAP);
    if (header_pos == std::string_view::npos) {
        std::println("perf: perf.data has no PROC_MAP section");
        return;
    }

    std::size_t pos = view.find('\n', header_pos);
    if (pos == std::string_view::npos) {
        return;
    }
    ++pos;

    std::println("=== perf show-map [{}] ==============================================", PERF_DATA_FILE);
    std::println("{:>6}  {:<20}  {}", "PID", "COMM", "CMDLINE");
    std::println("{:->6}  {:->20}  {:->40}", "", "", "");

    while (pos < view.size()) {
        std::string_view line = next_line(view, pos);
        if (line.starts_with(END_PREFIX)) {
            break;
        }
        if (line.empty()) {
            continue;
        }

        std::size_t pid_pos = line.find("pid=");
        std::size_t comm_pos = line.find(COMM_FIELD_PREFIX);
        std::size_t cmd_pos = line.find(CMD_FIELD_PREFIX);
        if (pid_pos == std::string_view::npos || comm_pos == std::string_view::npos) {
            continue;
        }

        std::string_view pid_text = line.substr(pid_pos + PID_PREFIX_SIZE, comm_pos - (pid_pos + PID_PREFIX_SIZE));
        std::string_view comm_text = cmd_pos == std::string_view::npos
                                         ? line.substr(comm_pos + COMM_PREFIX_SIZE)
                                         : line.substr(comm_pos + COMM_PREFIX_SIZE, cmd_pos - (comm_pos + COMM_PREFIX_SIZE));
        std::string_view cmd_text = cmd_pos == std::string_view::npos ? std::string_view{} : line.substr(cmd_pos + CMD_PREFIX_SIZE);

        std::println("{:>6}  {:<20}  {}", parse_u64(pid_text), comm_text, cmd_text);
    }
}

void cmd_top(int ms) {
    if (ms < 1) {
        ms = DEFAULT_SAMPLE_MS;
    }

    for (;;) {
        cmd_stat(ms);
        std::println("");
    }
}

void usage() {
    std::println("Usage: perf <command> [args]");
    std::println("Commands:");
    std::println("  stat     [ms=1000]    CPU% per process (sampling window in ms)");
    std::println("  record   [ms=1000]    Record kernel events for N ms, save to perf.data");
    std::println("  report   [n=2000]     Display events from perf.data (or live ring buffer)");
    std::println("  sched    [n=2000]     Alias for report");
    std::println("  cpustat               Per-CPU aggregate scheduler statistics");
    std::println("  top      [ms=1000]    Continuous CPU% snapshots");
    std::println("  run      <cmd> [args] Trace cmd+descendants, save to perf.data");
    std::println("  show-map              Show PID->name/cmdline map from perf.data");
}
}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        usage();
        return 1;
    }

    std::string_view cmd(argv[1]);

    if (cmd == "stat") {
        int ms = argc >= 3 ? static_cast<int>(strtol(argv[2], nullptr, PARSE_BASE_DECIMAL)) : DEFAULT_SAMPLE_MS;
        cmd_stat(ms);
    } else if (cmd == "record") {
        int ms = argc >= 3 ? static_cast<int>(strtol(argv[2], nullptr, PARSE_BASE_DECIMAL)) : DEFAULT_SAMPLE_MS;
        cmd_record(ms);
    } else if (cmd == "report" || cmd == "sched") {
        int max_events = argc >= 3 ? static_cast<int>(strtol(argv[2], nullptr, PARSE_BASE_DECIMAL)) : DEFAULT_MAX_EVENTS;
        cmd_sched(max_events);
    } else if (cmd == "cpustat") {
        cmd_cpustat();
    } else if (cmd == "top") {
        int ms = argc >= 3 ? static_cast<int>(strtol(argv[2], nullptr, PARSE_BASE_DECIMAL)) : DEFAULT_SAMPLE_MS;
        cmd_top(ms);
    } else if (cmd == "run") {
        if (argc < 3) {
            std::println("perf run: usage: perf run <program> [args...]");
            return 1;
        }
        cmd_run(argc - 2, argv + 2);
    } else if (cmd == "show-map") {
        cmd_show_map();
    } else {
        usage();
        return 1;
    }

    return 0;
}
