// perf - kernel performance measurement tool for WOS
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
#include <unordered_map>
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
constexpr int DEFAULT_WKI_TRACE_EVENTS = 200;
constexpr int DEFAULT_WKI_TAIL_ROWS = 15;
constexpr int DEFAULT_WKI_LAUNCH_ROWS = 20;

constexpr std::size_t INITIAL_FILE_CAPACITY = 131072;
constexpr std::size_t PROC_READ_CAPACITY = 512;
constexpr std::size_t PERF_DRAIN_CAPACITY = 65536;
constexpr std::size_t CPUSTAT_READ_CAPACITY = 4096;
constexpr std::size_t MAX_EVENT_TOKENS = 14;

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
constexpr std::string_view KWKISTAT_PATH = "/proc/kwkistat";
constexpr std::string_view KCPUSTAT_PATH = "/proc/kcpustat";
constexpr std::string_view KCONTSTAT_PATH = "/proc/kcontstat";
constexpr std::string_view DEV_NODES_ROOT = "/dev/nodes";
constexpr std::string_view SECTION_HEADER = "--- SECTION";
constexpr std::string_view SECTION_EVENTS = "--- SECTION EVENTS ---\n";
constexpr std::string_view SECTION_EVENTS_END = "--- END EVENTS ---\n";
constexpr std::string_view SECTION_WKI_SUMMARY = "--- SECTION WKI_SUMMARY ---\n";
constexpr std::string_view SECTION_WKI_SUMMARY_END = "--- END WKI_SUMMARY ---\n";
constexpr std::string_view SECTION_PROC_MAP = "--- SECTION PROC_MAP ---\n";
constexpr std::string_view SECTION_PROC_MAP_END = "--- END PROC_MAP ---\n";
constexpr std::string_view SECTION_PEER_MAP = "--- SECTION PEER_MAP ---\n";
constexpr std::string_view SECTION_PEER_MAP_END = "--- END PEER_MAP ---\n";
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
    std::string subsys_name;
    std::string scope_name;
    std::string op_name;
    std::string phase_name;
    int64_t lag{};
    uint8_t flags{};
    uint32_t aux{};
    uint64_t peer{};
    uint64_t channel{};
    uint64_t correlation{};
    int32_t status{};
};

struct WkiSummaryRow {
    std::string scope;
    std::string op;
    uint64_t peer{};
    uint64_t channel{};
    uint64_t calls{};
    uint64_t errors{};
    uint64_t retries{};
    uint64_t bytes{};
    uint64_t avg_us{};
    uint64_t max_us{};
    uint64_t p95_us{};
    uint64_t p99_us{};
    uint64_t p999_us{};
    uint64_t p9999_us{};
    uint64_t p99999_us{};
};

struct WkiGapRow {
    std::string scope;
    std::string op;
    uint64_t peer{};
    uint64_t channel{};
    uint64_t gap_ns{};
    uint64_t prev_ts_ns{};
    uint64_t next_ts_ns{};
};

struct WkiLaunchRow {
    uint64_t peer{};
    uint64_t correlation{};
    std::optional<uint32_t> submit_total_us;
    std::optional<int32_t> submit_status;
    std::optional<uint32_t> handle_submit_us;
    std::optional<int32_t> handle_status;
    std::optional<uint32_t> load_elf_us;
    std::optional<int32_t> load_status;
    std::optional<uint32_t> defer_wait_us;
    std::optional<uint32_t> task_runtime_us;
    std::optional<uint32_t> proxy_ready_wait_us;
    std::optional<uint32_t> complete_hold_us;
    std::optional<uint32_t> complete_wait_us;
    std::optional<int32_t> complete_status;
    bool accepted{};
    bool rejected{};
    bool completed{};
    bool proxy_ready{};
};

struct WkiTraceFilter {
    std::string scope;
    std::string op;
    std::optional<uint64_t> peer;
    std::optional<uint64_t> channel;
    std::optional<uint64_t> pid;
    std::optional<uint64_t> min_us;
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

struct WkiPeerMapEntry {
    uint64_t peer{};
    std::string hostname;
};

struct WkiDisplayOptions {
    bool show_peer_ids{};
};

struct WkiPeerResolver {
    bool show_peer_ids{};
    std::unordered_map<uint64_t, std::string> hostnames;
    std::unordered_map<uint64_t, std::string> display_cache;
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

auto build_dev_nodes_path(std::string_view hostname, std::string_view suffix = {}) -> std::string {
    std::string path(DEV_NODES_ROOT);
    if (!hostname.empty()) {
        path += '/';
        path += hostname;
    }
    path += suffix;
    return path;
}

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

auto is_dot_entry(std::string_view text) -> bool { return text == "." || text == ".."; }

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

auto peer_map_line(uint64_t peer, std::string_view hostname) -> std::string {
    std::string line = "peer=";
    line += std::to_string(peer);
    line += " hostname=";
    line += hostname;
    line += '\n';
    return line;
}

auto collect_live_wki_peer_map() -> std::vector<WkiPeerMapEntry> {
    std::vector<WkiPeerMapEntry> entries;
    ScopedDir dir(opendir(std::string(DEV_NODES_ROOT).c_str()));
    if (!dir.valid()) {
        return entries;
    }

    dirent* entry = nullptr;
    while ((entry = readdir(dir.get())) != nullptr) {
        std::string_view hostname{&entry->d_name[0]};
        if (hostname.empty() || is_dot_entry(hostname)) {
            continue;
        }

        auto id_text = read_file(build_dev_nodes_path(hostname, "/id"), PROC_READ_CAPACITY);
        if (!id_text.has_value() || id_text->empty()) {
            continue;
        }

        entries.push_back(WkiPeerMapEntry{
            .peer = parse_u64(*id_text, 0),
            .hostname = std::string(hostname),
        });
    }

    std::ranges::sort(entries, [](const WkiPeerMapEntry& lhs, const WkiPeerMapEntry& rhs) {
        if (lhs.peer != rhs.peer) {
            return lhs.peer < rhs.peer;
        }
        return lhs.hostname < rhs.hostname;
    });
    return entries;
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

auto write_section_wki_summary(int fd) -> ssize_t {
    auto summary = read_file(KWKISTAT_PATH, PERF_DRAIN_CAPACITY);
    if (!summary.has_value() || summary->empty()) {
        return 0;
    }

    write_all(fd, SECTION_WKI_SUMMARY);
    write_all(fd, *summary);
    write_all(fd, SECTION_WKI_SUMMARY_END);
    return static_cast<ssize_t>(summary->size());
}

void write_section_peer_map(int fd) {
    auto peers = collect_live_wki_peer_map();
    if (peers.empty()) {
        return;
    }

    write_all(fd, SECTION_PEER_MAP);
    for (const auto& peer : peers) {
        write_all(fd, peer_map_line(peer.peer, peer.hostname));
    }
    write_all(fd, SECTION_PEER_MAP_END);
}

void save_perf_data() {
    ScopedFd fd = open_write_trunc(PERF_DATA_FILE);
    if (!fd.valid()) {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
        return;
    }

    write_section_proc_map(fd.get());
    write_section_peer_map(fd.get());
    ssize_t event_bytes = write_section_events(fd.get());
    write_section_wki_summary(fd.get());

    if (event_bytes <= 0) {
        std::println("perf: ring buffer empty - PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes)", PERF_DATA_FILE, event_bytes);
    }
}

void set_recording_enabled(bool enabled, const char* filter = nullptr) {
    ScopedFd fd = open_writeonly(KPERFCTL_PATH);
    if (!fd.valid()) {
        return;
    }
    if (enabled && filter != nullptr) {
        std::string cmd = std::string("enable ") + filter;
        write_all(fd.get(), cmd);
    } else {
        write_all(fd.get(), enabled ? "enable" : "disable");
    }
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

void cmd_record(int ms, const char* filter = nullptr) {
    if (ms < 1) {
        ms = DEFAULT_SAMPLE_MS;
    }

    ScopedFd control_fd = open_writeonly(KPERFCTL_PATH);
    if (!control_fd.valid()) {
        std::println("perf: cannot open /proc/kperfctl");
        return;
    }

    if (filter != nullptr) {
        std::string cmd = std::string("enable ") + filter;
        write_all(control_fd.get(), cmd.c_str());
        std::println("perf: recording with filter '{}' for {} ms...", filter, ms);
    } else {
        write_all(control_fd.get(), "enable");
        std::println("perf: recording for {} ms...", ms);
    }

    ScopedFd data_fd = open_write_trunc(PERF_DATA_FILE);
    if (data_fd.valid()) {
        write_all(data_fd.get(), SECTION_EVENTS);
    }

    // Track all processes seen during recording so short-lived ones
    // that exit before the final proc-map snapshot are still resolved.
    std::vector<TrackedProc> tracked;
    auto upsert_tracked = [&](const StatInfo& stat) {
        for (auto& proc : tracked) {
            if (proc.pid == stat.pid) {
                proc.comm = stat.comm;
                return;
            }
        }
        tracked.push_back(TrackedProc{
            .pid = stat.pid,
            .comm = stat.comm,
            .cmdline = read_cmdline(stat.pid),
        });
    };

    // Initial snapshot of all running processes.
    for_each_process_stat([&](const StatInfo& stat, std::string_view) { upsert_tracked(stat); });

    int64_t deadline_ms = now_ms() + ms;
    int64_t last_drain_ms = now_ms();
    ssize_t total_event_bytes = 0;

    while (true) {
        int64_t remaining_ms = deadline_ms - now_ms();
        if (remaining_ms <= 0) {
            break;
        }

        wall_sleep_ms(static_cast<int>(std::min<int64_t>(remaining_ms, SLEEP_SLICE_MS)));
        if (!data_fd.valid() || (now_ms() - last_drain_ms) < DRAIN_INTERVAL_MS) {
            continue;
        }

        auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
        if (events.has_value() && !events->empty()) {
            write_all(data_fd.get(), *events);
            total_event_bytes += static_cast<ssize_t>(events->size());
        }

        // Pick up any newly-spawned processes.
        for_each_process_stat([&](const StatInfo& stat, std::string_view) { upsert_tracked(stat); });

        last_drain_ms = now_ms();
    }

    write_all(control_fd.get(), "disable");
    std::println("perf: recording stopped.");

    if (!data_fd.valid()) {
        save_perf_data();
        return;
    }

    auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
    if (events.has_value() && !events->empty()) {
        write_all(data_fd.get(), *events);
        total_event_bytes += static_cast<ssize_t>(events->size());
    }

    write_all(data_fd.get(), SECTION_EVENTS_END);
    write_section_wki_summary(data_fd.get());
    write_section_peer_map(data_fd.get());

    // Write proc map: current live processes plus any tracked ones that exited.
    write_all(data_fd.get(), SECTION_PROC_MAP);
    auto after = collect_stats();
    for_each_process_stat([&](const StatInfo& info, std::string_view) {
        write_all(data_fd.get(), proc_map_line(info.pid, info.comm, read_cmdline(info.pid)));
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
        std::println("perf: ring buffer empty - PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes)", PERF_DATA_FILE, total_event_bytes);
    }
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

auto extract_value(std::string_view line, std::string_view key) -> std::string_view;

auto parse_peer_map_line(std::string_view line, WkiPeerMapEntry& out) -> bool {
    if (line.empty() || !line.starts_with("peer=")) {
        return false;
    }

    out = {};
    auto peer = extract_value(line, "peer=");
    auto hostname = extract_value(line, "hostname=");
    if (peer.empty() || hostname.empty()) {
        return false;
    }

    out.peer = parse_u64(peer, 0);
    out.hostname = std::string(hostname);
    return true;
}

auto parse_peer_map_section(std::string_view buffer) -> std::vector<WkiPeerMapEntry> {
    std::vector<WkiPeerMapEntry> entries;
    std::size_t header_pos = buffer.find(SECTION_PEER_MAP);
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

        WkiPeerMapEntry entry{};
        if (parse_peer_map_line(line, entry)) {
            entries.push_back(std::move(entry));
        }
    }

    return entries;
}

auto extract_value(std::string_view line, std::string_view key) -> std::string_view {
    std::size_t key_pos = line.find(key);
    if (key_pos == std::string_view::npos) {
        return {};
    }
    key_pos += key.size();
    std::size_t end = line.find(' ', key_pos);
    return line.substr(key_pos, end == std::string_view::npos ? line.size() - key_pos : end - key_pos);
}

auto parse_wki_summary_line(std::string_view line, WkiSummaryRow& out) -> bool {
    if (line.empty() || !line.starts_with("scope=")) {
        return false;
    }

    out = {};
    auto scope = extract_value(line, "scope=");
    auto op = extract_value(line, "op=");
    auto peer = extract_value(line, "peer=");
    auto channel = extract_value(line, "channel=");
    auto calls = extract_value(line, "calls=");
    if (scope.empty() || op.empty() || calls.empty()) {
        return false;
    }

    out.scope = std::string(scope);
    out.op = std::string(op);
    out.peer = peer.empty() ? 0 : parse_u64(peer, 0);
    out.channel = channel.empty() ? 0 : parse_u64(channel, 0);
    out.calls = parse_u64(calls);
    out.errors = parse_u64(extract_value(line, "errors="));
    out.retries = parse_u64(extract_value(line, "retries="));
    out.bytes = parse_u64(extract_value(line, "bytes="));
    out.avg_us = parse_u64(extract_value(line, "avg_us="));
    out.max_us = parse_u64(extract_value(line, "max_us="));
    out.p95_us = parse_u64(extract_value(line, "p95_us="));
    out.p99_us = parse_u64(extract_value(line, "p99_us="));
    out.p999_us = parse_u64(extract_value(line, "p999_us="));
    out.p9999_us = parse_u64(extract_value(line, "p9999_us="));
    out.p99999_us = parse_u64(extract_value(line, "p99999_us="));
    return true;
}

auto parse_wki_summary_section(std::string_view buffer, bool sectioned) -> std::vector<WkiSummaryRow> {
    std::vector<WkiSummaryRow> rows;
    std::size_t pos = 0;

    if (sectioned) {
        std::size_t header_pos = buffer.find(SECTION_WKI_SUMMARY);
        if (header_pos == std::string_view::npos) {
            return rows;
        }

        pos = buffer.find('\n', header_pos);
        if (pos == std::string_view::npos) {
            return rows;
        }
        ++pos;
    }

    while (pos < buffer.size()) {
        std::string_view line = next_line(buffer, pos);
        if (sectioned && line.starts_with(END_PREFIX)) {
            break;
        }
        WkiSummaryRow row{};
        if (parse_wki_summary_line(line, row)) {
            rows.push_back(std::move(row));
        }
    }

    return rows;
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
    constexpr int TOK_SCOPE = 3;
    constexpr int TOK_OP = 4;
    constexpr int TOK_PHASE = 5;
    constexpr int TOK_PEER = 6;
    constexpr int TOK_CHANNEL = 7;
    constexpr int TOK_CORR = 8;
    constexpr int TOK_STATUS = 9;
    constexpr int TOK_WKI_AUX = 10;
    constexpr int TOK_WKI_CALLSITE = 11;

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

        case 'C':
            // CONTAINER_STAT: C <ts> <cpu> <pid> <subsys_name> <flags> <count> <capacity> <callsite>
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens[TOK_PID]);
            out.subsys_name = std::string(tokens[TOK_DATA]);  // tok 3 = subsys_name
            out.flags = parse_u8(tokens[TOK_LAG]);            // tok 4 = flags
            out.lag = parse_i64(tokens[TOK_FLAGS]);           // tok 5 = element count
            if (token_count >= TOK_AUX + 1) {
                out.aux = parse_u32(tokens[TOK_AUX]);  // tok 6 = capacity
            }
            if (token_count >= TOK_CALLSITE + 1) {
                out.callsite = std::string(tokens[TOK_CALLSITE]);
            }
            return true;

        case 'K':
            if (token_count < TOK_WKI_AUX + 1) {
                return false;
            }
            out.pid = parse_u64(tokens[TOK_PID]);
            out.scope_name = std::string(tokens[TOK_SCOPE]);
            out.op_name = std::string(tokens[TOK_OP]);
            out.phase_name = std::string(tokens[TOK_PHASE]);
            out.peer = parse_u64(tokens[TOK_PEER], 0);
            out.channel = parse_u64(tokens[TOK_CHANNEL], 0);
            out.correlation = parse_u64(tokens[TOK_CORR], 0);
            out.status = static_cast<int32_t>(parse_i64(tokens[TOK_STATUS], 0));
            out.aux = parse_u32(tokens[TOK_WKI_AUX], 0);
            if (token_count >= TOK_WKI_CALLSITE + 1) {
                out.callsite = std::string(tokens[TOK_WKI_CALLSITE]);
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

auto make_wki_peer_resolver(std::string_view buffer, bool sectioned, const WkiDisplayOptions& options) -> WkiPeerResolver {
    WkiPeerResolver resolver{};
    resolver.show_peer_ids = options.show_peer_ids;

    auto insert_entries = [&](const std::vector<WkiPeerMapEntry>& entries) {
        for (const auto& entry : entries) {
            if (entry.hostname.empty()) {
                continue;
            }
            resolver.hostnames.try_emplace(entry.peer, entry.hostname);
        }
    };

    if (sectioned) {
        insert_entries(parse_peer_map_section(buffer));
    }
    insert_entries(collect_live_wki_peer_map());
    resolver.display_cache.reserve(resolver.hostnames.size());
    return resolver;
}

auto wki_peer_label(uint64_t peer, WkiPeerResolver& resolver) -> const std::string& {
    auto cached = resolver.display_cache.find(peer);
    if (cached != resolver.display_cache.end()) {
        return cached->second;
    }

    auto [it, _] = resolver.display_cache.emplace(peer, std::string{});
    auto mapped = resolver.hostnames.find(peer);
    if (!resolver.show_peer_ids && mapped != resolver.hostnames.end() && !mapped->second.empty()) {
        it->second = mapped->second;
    } else {
        it->second = std::to_string(peer);
    }
    return it->second;
}

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

void cmd_sched(int max_events, const WkiDisplayOptions& display_options) {
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
    auto peer_resolver = make_wki_peer_resolver(view, sectioned, display_options);
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
        } else if (event.type == 'C') {
            std::println("CNT  {:>18}  {:>3}  {:<24} subsys={} count={} cap={} flags={} site={}", event.ts_ns, event.cpu,
                         format_pid_name(event.pid, proc_map), event.subsys_name, event.lag, event.aux, static_cast<unsigned>(event.flags),
                         display_callsite(event.callsite));
        } else if (event.type == 'K') {
            std::println("WKI  {:>18}  {:>3}  {:<24} {}:{}:{} peer={} ch={} corr={} status={} aux={} site={}", event.ts_ns, event.cpu,
                         format_pid_name(event.pid, proc_map), event.scope_name, event.op_name, event.phase_name,
                         wki_peer_label(event.peer, peer_resolver), event.channel, event.correlation, event.status, event.aux,
                         display_callsite(event.callsite));
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

void cmd_contstat() {
    auto buffer = read_file(KCONTSTAT_PATH, CPUSTAT_READ_CAPACITY);
    if (!buffer.has_value() || buffer->empty()) {
        std::println("perf: cannot read /proc/kcontstat (no activity yet?)");
        return;
    }

    std::println("=== perf contstat ==================================================");
    std::println("{:<14}  {:>10}  {:>10}  {:>10}  {:>6}  {:>10}  {:>10}", "subsystem", "inserts", "removes", "resizes", "oom", "peak",
                 "current");
    std::println("{:->14}  {:->10}  {:->10}  {:->10}  {:->6}  {:->10}  {:->10}", "", "", "", "", "", "", "");

    std::size_t pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) -> uint64_t {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return 0;
            }
            key_pos += key.size();
            std::size_t end = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, end == std::string_view::npos ? line.size() - key_pos : end - key_pos));
        };

        auto get_str = [&](std::string_view key) -> std::string_view {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return "?";
            }
            key_pos += key.size();
            std::size_t end = line.find(' ', key_pos);
            return line.substr(key_pos, end == std::string_view::npos ? line.size() - key_pos : end - key_pos);
        };

        std::println("{:<14}  {:>10}  {:>10}  {:>10}  {:>6}  {:>10}  {:>10}", get_str("subsys="), get_val("inserts="), get_val("removes="),
                     get_val("resizes="), get_val("oom="), get_val("peak="), get_val("current="));
    }
}

auto current_proc_map() -> std::vector<ProcMapEntry> {
    std::vector<ProcMapEntry> proc_map;
    for (const auto& stat : collect_stats()) {
        proc_map.push_back(ProcMapEntry{.pid = stat.pid, .comm = stat.comm});
    }
    return proc_map;
}

struct WkiLoadedText {
    std::string source;
    std::string buffer;
    bool sectioned{};
};

auto event_section_view(std::string_view view, bool sectioned) -> std::string_view {
    if (!sectioned) {
        return view;
    }

    std::size_t events_hdr = view.find(SECTION_EVENTS);
    if (events_hdr == std::string_view::npos) {
        return {};
    }

    std::size_t section_start = view.find('\n', events_hdr);
    if (section_start == std::string_view::npos) {
        return {};
    }
    return view.substr(section_start + 1);
}

auto collect_wki_events(std::string_view view, bool sectioned) -> std::vector<EventInfo> {
    std::vector<EventInfo> events;
    std::string_view event_view = event_section_view(view, sectioned);
    std::size_t pos = 0;
    EventInfo event{};
    while (next_event(event_view, pos, sectioned, event)) {
        if (event.type == 'K') {
            events.push_back(event);
        }
    }
    std::ranges::sort(events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.ts_ns < rhs.ts_ns; });
    return events;
}

auto load_wki_summary_text() -> std::optional<WkiLoadedText> {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE, PERF_DRAIN_CAPACITY);
        if (saved.has_value() && !saved->empty()) {
            WkiLoadedText loaded{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved)};
            loaded.sectioned = std::string_view(loaded.buffer).starts_with(SECTION_HEADER);
            if (!parse_wki_summary_section(loaded.buffer, loaded.sectioned).empty()) {
                return loaded;
            }
        }
    }

    auto live = read_file(KWKISTAT_PATH, PERF_DRAIN_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }

    return WkiLoadedText{.source = std::string(KWKISTAT_PATH), .buffer = std::move(*live), .sectioned = false};
}

auto load_wki_event_text() -> std::optional<WkiLoadedText> {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE, PERF_DRAIN_CAPACITY);
        if (saved.has_value() && !saved->empty()) {
            WkiLoadedText loaded{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved)};
            loaded.sectioned = std::string_view(loaded.buffer).starts_with(SECTION_HEADER);
            if (!collect_wki_events(loaded.buffer, loaded.sectioned).empty()) {
                return loaded;
            }
        }
    }

    auto live = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }

    return WkiLoadedText{.source = std::string(KPERF_PATH), .buffer = std::move(*live), .sectioned = false};
}

auto wki_trace_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (!filter.scope.empty() && event.scope_name != filter.scope) {
        return false;
    }
    if (!filter.op.empty() && event.op_name != filter.op) {
        return false;
    }
    if (filter.peer.has_value() && event.peer != *filter.peer) {
        return false;
    }
    if (filter.channel.has_value() && event.channel != *filter.channel) {
        return false;
    }
    if (filter.pid.has_value() && event.pid != *filter.pid) {
        return false;
    }
    if (filter.min_us.has_value() && event.aux < *filter.min_us) {
        return false;
    }
    return true;
}

auto is_wki_launch_event_op(std::string_view op) -> bool {
    return op == "submit_inline" || op == "submit_vfs_ref" || op == "complete_wait" || op == "accept" || op == "reject" ||
           op == "complete" || op == "proxy_ready" || op == "defer_wait" || op == "load_elf" || op == "handle_submit" ||
           op == "task_runtime" || op == "proxy_ready_wait" || op == "complete_hold";
}

auto is_wki_launch_summary_op(std::string_view op) -> bool {
    return op == "submit_inline" || op == "submit_vfs_ref" || op == "complete_wait" || op == "defer_wait" || op == "load_elf" ||
           op == "handle_submit" || op == "task_runtime" || op == "proxy_ready_wait" || op == "complete_hold";
}

auto find_wki_launch_row(std::vector<WkiLaunchRow>& rows, uint64_t peer, uint64_t correlation) -> WkiLaunchRow& {
    for (auto& row : rows) {
        if (row.peer == peer && row.correlation == correlation) {
            return row;
        }
    }

    WkiLaunchRow row{};
    row.peer = peer;
    row.correlation = correlation;
    rows.push_back(row);
    return rows.back();
}

auto format_optional_us(const std::optional<uint32_t>& value) -> std::string {
    return value.has_value() ? std::to_string(*value) : std::string("-");
}

auto format_wki_launch_result(const WkiLaunchRow& row) -> std::string {
    if (row.rejected) {
        if (row.submit_status.has_value()) {
            return std::string("reject(") + std::to_string(*row.submit_status) + ')';
        }
        if (row.handle_status.has_value()) {
            return std::string("reject(") + std::to_string(*row.handle_status) + ')';
        }
        return "reject";
    }
    if (row.completed) {
        if (row.complete_status.has_value()) {
            return std::string("complete(") + std::to_string(*row.complete_status) + ')';
        }
        return "complete";
    }
    if (row.accepted) {
        return row.proxy_ready ? "accepted" : "accept";
    }
    if (row.submit_status.has_value() && *row.submit_status < 0) {
        return std::string("submit(") + std::to_string(*row.submit_status) + ')';
    }
    if (row.handle_status.has_value() && *row.handle_status < 0) {
        return std::string("handle(") + std::to_string(*row.handle_status) + ')';
    }
    if (row.load_status.has_value() && *row.load_status < 0) {
        return std::string("load(") + std::to_string(*row.load_status) + ')';
    }
    return "inflight";
}

auto wki_launch_score(const WkiLaunchRow& row) -> uint32_t {
    uint32_t score = 0;
    if (row.submit_total_us.has_value()) {
        score = std::max(score, *row.submit_total_us);
    }
    if (row.handle_submit_us.has_value()) {
        score = std::max(score, *row.handle_submit_us);
    }
    if (row.load_elf_us.has_value()) {
        score = std::max(score, *row.load_elf_us);
    }
    if (row.defer_wait_us.has_value()) {
        score = std::max(score, *row.defer_wait_us);
    }
    if (row.task_runtime_us.has_value()) {
        score = std::max(score, *row.task_runtime_us);
    }
    if (row.proxy_ready_wait_us.has_value()) {
        score = std::max(score, *row.proxy_ready_wait_us);
    }
    if (row.complete_hold_us.has_value()) {
        score = std::max(score, *row.complete_hold_us);
    }
    if (row.complete_wait_us.has_value()) {
        score = std::max(score, *row.complete_wait_us);
    }
    return score;
}

void cmd_wki_launch(int limit, const WkiDisplayOptions& display_options) {
    if (limit < 1) {
        limit = DEFAULT_WKI_LAUNCH_ROWS;
    }

    auto summary_loaded = load_wki_summary_text();
    std::vector<WkiSummaryRow> summary_rows;
    if (summary_loaded.has_value()) {
        auto rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        for (const auto& row : rows) {
            if (row.scope == "remote_compute" && is_wki_launch_summary_op(row.op)) {
                summary_rows.push_back(row);
            }
        }
        std::ranges::sort(summary_rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
            if (lhs.p99_us != rhs.p99_us) {
                return lhs.p99_us > rhs.p99_us;
            }
            if (lhs.max_us != rhs.max_us) {
                return lhs.max_us > rhs.max_us;
            }
            return lhs.calls > rhs.calls;
        });

        if (!summary_rows.empty()) {
            std::println("=== perf wki-launch summary [{}] ====================================", summary_loaded->source);
            std::println("{:<16}  {:>7}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}", "OP", "CALLS", "ERR", "AVG(us)", "P99(us)", "P999(us)",
                         "MAX(us)");
            std::println("{:->16}  {:->7}  {:->6}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "");
            for (const auto& row : summary_rows) {
                std::println("{:<16}  {:>7}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}", row.op, row.calls, row.errors, row.avg_us, row.p99_us,
                             row.p999_us, row.max_us);
            }
        }
    }

    auto event_loaded = load_wki_event_text();
    if (!event_loaded.has_value()) {
        if (!summary_rows.empty()) {
            std::println("");
            std::println("perf: no raw WKI launch events available in perf.data or /proc/kperf");
            return;
        }
        std::println("perf: no WKI launch data");
        return;
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    auto peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    std::vector<WkiLaunchRow> launch_rows;
    for (const auto& event : events) {
        if (event.scope_name != "remote_compute" || !is_wki_launch_event_op(event.op_name)) {
            continue;
        }

        auto& row = find_wki_launch_row(launch_rows, event.peer, event.correlation);
        if (event.phase_name == "end") {
            if (event.op_name == "submit_vfs_ref" || event.op_name == "submit_inline") {
                row.submit_total_us = event.aux;
                row.submit_status = event.status;
            } else if (event.op_name == "handle_submit") {
                row.handle_submit_us = event.aux;
                row.handle_status = event.status;
            } else if (event.op_name == "load_elf") {
                row.load_elf_us = event.aux;
                row.load_status = event.status;
            } else if (event.op_name == "defer_wait") {
                row.defer_wait_us = event.aux;
            } else if (event.op_name == "task_runtime") {
                row.task_runtime_us = event.aux;
                row.completed = true;
                row.complete_status = event.status;
            } else if (event.op_name == "proxy_ready_wait") {
                row.proxy_ready_wait_us = event.aux;
            } else if (event.op_name == "complete_hold") {
                row.complete_hold_us = event.aux;
            } else if (event.op_name == "complete_wait") {
                row.complete_wait_us = event.aux;
            }
        } else if (event.phase_name == "point") {
            if (event.op_name == "accept") {
                row.accepted = true;
            } else if (event.op_name == "reject") {
                row.rejected = true;
                row.submit_status = event.status;
            } else if (event.op_name == "complete") {
                row.completed = true;
                row.complete_status = event.status;
            } else if (event.op_name == "proxy_ready") {
                row.proxy_ready = true;
            }
        }
    }

    if (launch_rows.empty()) {
        if (!summary_rows.empty()) {
            std::println("");
            std::println("perf: no raw WKI launch rows found in {}", event_loaded->source);
            return;
        }
        std::println("perf: no WKI launch rows found in {}", event_loaded->source);
        return;
    }

    std::ranges::sort(launch_rows,
                      [](const WkiLaunchRow& lhs, const WkiLaunchRow& rhs) { return wki_launch_score(lhs) > wki_launch_score(rhs); });

    std::println("");
    std::println("=== slowest WKI launches [{}] ======================================", event_loaded->source);
    std::println("{:<18}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {}", "PEER", "CORR", "TOTAL(us)",
                 "HANDLE(us)", "LOAD(us)", "SETUP(us)", "QUEUE(us)", "RUN(us)", "READY(us)", "HOLD(us)", "WAIT(us)", "RESULT");
    std::println("{:->18}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->12}", "", "", "", "",
                 "", "", "", "", "", "", "", "");
    for (int i = 0; i < static_cast<int>(launch_rows.size()) && i < limit; ++i) {
        const auto& row = launch_rows[static_cast<std::size_t>(i)];
        std::optional<uint32_t> setup_us;
        if (row.handle_submit_us.has_value() && row.load_elf_us.has_value() && *row.handle_submit_us >= *row.load_elf_us) {
            setup_us = *row.handle_submit_us - *row.load_elf_us;
        }
        std::println("{:<18}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
                     wki_peer_label(row.peer, peer_resolver), row.correlation,
                     format_optional_us(row.submit_total_us), format_optional_us(row.handle_submit_us), format_optional_us(row.load_elf_us),
                     format_optional_us(setup_us), format_optional_us(row.defer_wait_us), format_optional_us(row.task_runtime_us),
                     format_optional_us(row.proxy_ready_wait_us), format_optional_us(row.complete_hold_us),
                     format_optional_us(row.complete_wait_us), format_wki_launch_result(row));
    }
}

void cmd_wki_report(const WkiDisplayOptions& display_options) {
    auto loaded = load_wki_summary_text();
    if (!loaded.has_value()) {
        std::println("perf: no WKI summary data");
        return;
    }

    auto rows = parse_wki_summary_section(loaded->buffer, loaded->sectioned);
    if (rows.empty()) {
        std::println("perf: no WKI summary rows found in {}", loaded->source);
        return;
    }

    std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
        if (lhs.p99_us != rhs.p99_us) {
            return lhs.p99_us > rhs.p99_us;
        }
        if (lhs.max_us != rhs.max_us) {
            return lhs.max_us > rhs.max_us;
        }
        return lhs.calls > rhs.calls;
    });

    auto peer_resolver = make_wki_peer_resolver(loaded->buffer, loaded->sectioned, display_options);
    std::println("=== perf wki-report [{}] ============================================", loaded->source);
    std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER", "CH", "CALLS",
                 "ERR", "RETRY", "AVG(us)", "P99(us)", "P999(us)", "MAX(us)");
    std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->7}  {:->6}  {:->7}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "",
                 "", "", "");
    for (const auto& row : rows) {
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                     wki_peer_label(row.peer, peer_resolver), row.channel, row.calls, row.errors, row.retries, row.avg_us, row.p99_us,
                     row.p999_us, row.max_us);
    }
}

void cmd_wki_tail(int limit, const WkiDisplayOptions& display_options) {
    if (limit < 1) {
        limit = DEFAULT_WKI_TAIL_ROWS;
    }

    auto summary_loaded = load_wki_summary_text();
    std::vector<WkiSummaryRow> rows;
    if (summary_loaded.has_value()) {
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
            if (lhs.p99999_us != rhs.p99999_us) {
                return lhs.p99999_us > rhs.p99999_us;
            }
            if (lhs.p9999_us != rhs.p9999_us) {
                return lhs.p9999_us > rhs.p9999_us;
            }
            if (lhs.max_us != rhs.max_us) {
                return lhs.max_us > rhs.max_us;
            }
            return lhs.calls > rhs.calls;
        });

        if (!rows.empty()) {
            auto summary_peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
            std::println("=== perf wki-tail summary [{}] ======================================", summary_loaded->source);
            std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER", "CH", "CALLS",
                         "P999(us)", "P9999(us)", "P99999(us)", "MAX(us)");
            std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->7}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "", "");
            for (int i = 0; i < static_cast<int>(rows.size()) && i < limit; ++i) {
                const auto& row = rows[static_cast<std::size_t>(i)];
                std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                             wki_peer_label(row.peer, summary_peer_resolver), row.channel, row.calls, row.p999_us, row.p9999_us,
                             row.p99999_us, row.max_us);
            }
        }
    }

    auto event_loaded = load_wki_event_text();
    if (!event_loaded.has_value()) {
        if (!rows.empty()) {
            std::println("");
            std::println("perf: no raw WKI events available in perf.data or /proc/kperf");
            return;
        }
        std::println("perf: no WKI summary or raw trace data");
        return;
    }

    if (rows.empty()) {
        std::println("perf: no WKI summary data; showing raw-event tail from {}", event_loaded->source);
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    auto event_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    if (events.empty()) {
        if (!rows.empty()) {
            std::println("");
            std::println("perf: no raw WKI events found in {}", event_loaded->source);
            return;
        }
        std::println("perf: no raw WKI events found in {}", event_loaded->source);
        return;
    }

    std::vector<EventInfo> slow_events;
    for (const auto& event : events) {
        if ((event.phase_name == "end" || event.phase_name == "point") && event.aux > 0) {
            slow_events.push_back(event);
        }
    }
    std::ranges::sort(slow_events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.aux > rhs.aux; });

    std::println("");
    std::println("=== slowest WKI events [{}] ========================================", event_loaded->source);
    std::println("{:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}", "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS",
                 "CORR");
    std::println("{:->14}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->10}", "", "", "", "", "", "", "", "");
    for (int i = 0; i < static_cast<int>(slow_events.size()) && i < limit; ++i) {
        const auto& event = slow_events[static_cast<std::size_t>(i)];
        std::println("{:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}", event.scope_name, event.op_name, event.phase_name,
                     wki_peer_label(event.peer, event_peer_resolver), event.channel, event.aux, event.status, event.correlation);
    }

    struct LastBegin {
        std::string scope;
        std::string op;
        uint64_t peer{};
        uint64_t channel{};
        uint64_t last_ts_ns{};
    };

    std::vector<LastBegin> last_begins;
    std::vector<WkiGapRow> gaps;
    for (const auto& event : events) {
        if (event.phase_name != "begin") {
            continue;
        }

        auto it = std::ranges::find_if(last_begins, [&](const LastBegin& row) {
            return row.scope == event.scope_name && row.op == event.op_name && row.peer == event.peer && row.channel == event.channel;
        });

        if (it != last_begins.end()) {
            gaps.push_back(WkiGapRow{
                .scope = event.scope_name,
                .op = event.op_name,
                .peer = event.peer,
                .channel = event.channel,
                .gap_ns = event.ts_ns - it->last_ts_ns,
                .prev_ts_ns = it->last_ts_ns,
                .next_ts_ns = event.ts_ns,
            });
            it->last_ts_ns = event.ts_ns;
        } else {
            last_begins.push_back(LastBegin{
                .scope = event.scope_name, .op = event.op_name, .peer = event.peer, .channel = event.channel, .last_ts_ns = event.ts_ns});
        }
    }

    std::ranges::sort(gaps, [](const WkiGapRow& lhs, const WkiGapRow& rhs) { return lhs.gap_ns > rhs.gap_ns; });

    std::println("");
    std::println("=== largest WKI inter-call gaps ====================================");
    std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>12}", "SCOPE", "OP", "PEER", "CH", "GAP(us)");
    std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->12}", "", "", "", "", "");
    for (int i = 0; i < static_cast<int>(gaps.size()) && i < limit; ++i) {
        const auto& gap = gaps[static_cast<std::size_t>(i)];
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>12}", gap.scope, gap.op, wki_peer_label(gap.peer, event_peer_resolver),
                     gap.channel, gap.gap_ns / static_cast<uint64_t>(NANOSECONDS_PER_MICROSECOND));
    }
}

void cmd_wki_trace(int max_events, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options) {
    if (max_events < 1) {
        max_events = DEFAULT_WKI_TRACE_EVENTS;
    }

    auto loaded = load_wki_event_text();
    if (!loaded.has_value()) {
        auto summary_loaded = load_wki_summary_text();
        if (summary_loaded.has_value() && !parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned).empty()) {
            std::println("perf: WKI summary data exists in {}, but no raw WKI events were saved", summary_loaded->source);
        } else {
            std::println("perf: no WKI trace data");
        }
        return;
    }

    auto proc_map = loaded->sectioned ? parse_proc_map_section(loaded->buffer) : current_proc_map();
    auto events = collect_wki_events(loaded->buffer, loaded->sectioned);
    auto peer_resolver = make_wki_peer_resolver(loaded->buffer, loaded->sectioned, display_options);
    if (events.empty()) {
        auto summary_loaded = load_wki_summary_text();
        if (summary_loaded.has_value() && !parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned).empty()) {
            std::println("perf: WKI summary data exists in {}, but no raw WKI events were saved", summary_loaded->source);
        } else {
            std::println("perf: no WKI trace data");
        }
        return;
    }

    std::println("=== perf wki-trace [{}] ============================================", loaded->source);
    std::println("{:<18}  {:>3}  {:<24}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>8}  {}", "TIME(ns)", "CPU", "PID", "SCOPE",
                 "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "CALLSITE");
    std::println("{:->18}  {:->3}  {:->24}  {:->14}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->8}  {:->20}", "", "", "", "", "",
                 "", "", "", "", "", "", "");

    int printed = 0;
    for (const auto& event : events) {
        if (!wki_trace_matches(event, filter)) {
            continue;
        }
        std::println("{:<18}  {:>3}  {:<24}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>8}  {}", event.ts_ns, event.cpu,
                     format_pid_name(event.pid, proc_map), event.scope_name, event.op_name, event.phase_name,
                     wki_peer_label(event.peer, peer_resolver), event.channel, event.aux, event.status, event.correlation,
                     display_callsite(event.callsite));
        printed++;
        if (printed >= max_events) {
            break;
        }
    }
}

// Resolve a command name to a full path by searching PATH.
// If cmd already contains '/', returns it as-is (if accessible).
// Returns empty string if not found.
auto resolve_command(const char* cmd) -> std::string {
    if (cmd == nullptr || cmd[0] == '\0') return {};

    // If cmd contains '/', treat as a path (absolute or relative)
    if (std::strchr(cmd, '/') != nullptr) {
        if (access(cmd, X_OK) == 0) {
            return {cmd};
        }
        return {};
    }

    // Search PATH
    const char* path_env = getenv("PATH");
    if (path_env == nullptr) {
        path_env = "/bin";
    }

    std::string_view path_sv(path_env);
    std::size_t start = 0;
    while (start <= path_sv.size()) {
        auto colon = path_sv.find(':', start);
        auto dir = path_sv.substr(start, colon == std::string_view::npos ? std::string_view::npos : colon - start);
        if (dir.empty()) {
            dir = ".";
        }

        std::string candidate;
        candidate.reserve(dir.size() + 1 + std::strlen(cmd));
        candidate.append(dir);
        candidate.push_back('/');
        candidate.append(cmd);

        if (access(candidate.c_str(), X_OK) == 0) {
            return candidate;
        }

        if (colon == std::string_view::npos) break;
        start = colon + 1;
    }
    return {};
}

// Read the shebang line from a script file. Returns the interpreter path,
// or empty string if the file is not a script.
auto read_shebang(const char* path) -> std::string {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return {};
    }
    char hdr[256];
    ssize_t n = read(fd, hdr, sizeof(hdr) - 1);
    close(fd);
    if (n < 3 || hdr[0] != '#' || hdr[1] != '!') {
        return {};
    }
    hdr[n] = '\0';
    // Find end of first line
    char* nl = std::strchr(hdr + 2, '\n');
    if (nl != nullptr) {
        *nl = '\0';
    }
    // Skip whitespace after #!
    const char* p = hdr + 2;
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (*p == '\0') {
        return {};
    }
    // Take only the interpreter path (stop at first space for args)
    const char* end = p;
    while (*end != '\0' && *end != ' ' && *end != '\t') {
        end++;
    }
    return {p, end};
}

void cmd_run(int argc, char** argv) {
    if (argc < 1) {
        std::println("perf run: no command specified");
        return;
    }

    // Parse --filter=... option (strip from argv before exec)
    const char* filter = "switch,wake,sleep";  // default: no container spam
    std::vector<char*> cmd_argv;
    for (int i = 0; i < argc; i++) {
        std::string_view arg(argv[i]);
        if (arg.starts_with("--filter=")) {
            filter = argv[i] + 9;
        } else {
            cmd_argv.push_back(argv[i]);
        }
    }
    argc = static_cast<int>(cmd_argv.size());
    argv = cmd_argv.data();

    if (argc < 1) {
        std::println("perf run: no command specified");
        return;
    }

    // Resolve the command to a full path (PATH search)
    std::string resolved = resolve_command(argv[0]);
    if (resolved.empty()) {
        std::println("perf run: command not found: {}", argv[0]);
        return;
    }

    // Check if this is a script with a shebang - if so, wrap with the interpreter
    std::string interp = read_shebang(resolved.c_str());
    std::vector<char*> new_argv;
    if (!interp.empty()) {
        // Resolve the interpreter too
        std::string interp_resolved = resolve_command(interp.c_str());
        if (interp_resolved.empty()) {
            std::println("perf run: interpreter not found: {} (from shebang in {})", interp, resolved);
            return;
        }
        interp = std::move(interp_resolved);

        // Build new argv: [interpreter, script_path, original_args[1:]]
        new_argv.push_back(interp.data());
        new_argv.push_back(resolved.data());
        for (int i = 1; i < argc; i++) {
            new_argv.push_back(argv[i]);
        }
        new_argv.push_back(nullptr);
    } else {
        // ELF binary - use resolved path as argv[0], keep rest
        new_argv.push_back(resolved.data());
        for (int i = 1; i < argc; i++) {
            new_argv.push_back(argv[i]);
        }
        new_argv.push_back(nullptr);
    }

    char** exec_argv = new_argv.data();
    const char* exec_path = exec_argv[0];

    auto before = collect_stats();
    int64_t start_ms = now_ms();
    set_recording_enabled(true, filter);

    int64_t child_pid = ker::process::fork();
    if (child_pid == 0) {
        ker::process::setpgid(0, 0);
        const auto* ea = const_cast<const char* const*>(exec_argv);
        const auto* exec_envp = const_cast<const char* const*>(environ);
        ker::process::execve(exec_path, ea, exec_envp);
        // If we get here, exec failed
        std::println("perf run: exec failed for '{}'", exec_path);
        _exit(EXEC_FAILURE_EXIT_CODE);
    }

    if (child_pid < 0) {
        std::println("perf run: fork failed ({})", child_pid);
        set_recording_enabled(false);
        return;
    }

    int64_t target_pgid = child_pid;
    std::println("perf run: tracing pgid={} cmd={}", target_pgid, exec_path);

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
        write_section_wki_summary(data_fd.get());
        write_section_peer_map(data_fd.get());
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
            std::println("perf: ring buffer empty - PROC_MAP saved, no events");
        } else {
            std::println("perf: saved to {} ({} event bytes)", PERF_DATA_FILE, total_event_bytes);
        }
    } else {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
    }

    cmd_sched(DEFAULT_MAX_EVENTS, {});
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
    std::println("  record   [ms=1000] [--filter=switch,container,wki,wki_launch]  Record events");
    std::println("  report   [n=2000] [--peer-ids]  Display events from perf.data (or live ring buffer)");
    std::println("  sched    [n=2000] [--peer-ids]  Alias for report");
    std::println("  cpustat               Per-CPU aggregate scheduler statistics");
    std::println("  contstat              Per-subsystem container statistics");
    std::println("  wki-report [--peer-ids]            WKI summary tables from perf.data or /proc/kwkistat");
    std::println("  wki-launch [n=20] [--peer-ids]     Remote launch stage timings from perf.data or /proc/kperf");
    std::println("  wki-tail [n=15] [--peer-ids]       WKI tail summary, slowest events, and inter-call gaps");
    std::println("  wki-trace [n=200] [--scope=..] [--op=..] [--peer=N] [--channel=N] [--pid=N] [--min-us=N] [--peer-ids]");
    std::println("  top      [ms=1000]    Continuous CPU% snapshots");
    std::println("  run      [--filter=switch,wake,sleep,wki,wki_launch] <cmd> [args]  Trace cmd (default: no container)");
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
        int ms = DEFAULT_SAMPLE_MS;
        const char* filter = nullptr;
        for (int i = 2; i < argc; ++i) {
            std::string_view arg(argv[i]);
            if (arg.starts_with("--filter=")) {
                filter = argv[i] + 9;
            } else {
                ms = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
            }
        }
        cmd_record(ms, filter);
    } else if (cmd == "report" || cmd == "sched") {
        int max_events = DEFAULT_MAX_EVENTS;
        WkiDisplayOptions display_options{};
        for (int i = 2; i < argc; ++i) {
            std::string_view arg(argv[i]);
            if (arg == "--peer-ids") {
                display_options.show_peer_ids = true;
            } else {
                max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
            }
        }
        cmd_sched(max_events, display_options);
    } else if (cmd == "cpustat") {
        cmd_cpustat();
    } else if (cmd == "contstat") {
        cmd_contstat();
    } else if (cmd == "wki-report") {
        WkiDisplayOptions display_options{};
        for (int i = 2; i < argc; ++i) {
            if (std::string_view(argv[i]) == "--peer-ids") {
                display_options.show_peer_ids = true;
            }
        }
        cmd_wki_report(display_options);
    } else if (cmd == "wki-launch") {
        int rows = DEFAULT_WKI_LAUNCH_ROWS;
        WkiDisplayOptions display_options{};
        for (int i = 2; i < argc; ++i) {
            std::string_view arg(argv[i]);
            if (arg == "--peer-ids") {
                display_options.show_peer_ids = true;
            } else {
                rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
            }
        }
        cmd_wki_launch(rows, display_options);
    } else if (cmd == "wki-tail") {
        int rows = DEFAULT_WKI_TAIL_ROWS;
        WkiDisplayOptions display_options{};
        for (int i = 2; i < argc; ++i) {
            std::string_view arg(argv[i]);
            if (arg == "--peer-ids") {
                display_options.show_peer_ids = true;
            } else {
                rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
            }
        }
        cmd_wki_tail(rows, display_options);
    } else if (cmd == "wki-trace") {
        int max_events = DEFAULT_WKI_TRACE_EVENTS;
        WkiTraceFilter filter{};
        WkiDisplayOptions display_options{};
        for (int i = 2; i < argc; ++i) {
            std::string_view arg(argv[i]);
            if (arg.starts_with("--scope=")) {
                filter.scope = std::string(arg.substr(8));
            } else if (arg.starts_with("--op=")) {
                filter.op = std::string(arg.substr(5));
            } else if (arg.starts_with("--peer=")) {
                filter.peer = parse_u64(arg.substr(7), 0);
            } else if (arg.starts_with("--channel=")) {
                filter.channel = parse_u64(arg.substr(10), 0);
            } else if (arg.starts_with("--pid=")) {
                filter.pid = parse_u64(arg.substr(6), 0);
            } else if (arg.starts_with("--min-us=")) {
                filter.min_us = parse_u64(arg.substr(9), 0);
            } else if (arg == "--peer-ids") {
                display_options.show_peer_ids = true;
            } else {
                max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
            }
        }
        cmd_wki_trace(max_events, filter, display_options);
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
