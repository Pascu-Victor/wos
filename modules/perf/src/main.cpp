// perf - kernel performance measurement tool for WOS
//
// Usage:
//   perf stat     [ms=1000]   CPU% per process over given sampling window
//   perf record   [ms=1000]   Enable kernel event recording for N ms, save to perf.data
//   perf report   [n=2000] [--time=boot|unix-ns|iso] Display events from perf.data (or live /proc/kperf)
//   perf sched    [n=2000] [--time=boot|unix-ns|iso] Alias for perf report
//   perf cpustat              Per-CPU aggregate scheduler statistics
//   perf ipc-report [n=15] [--time=boot|unix-ns|iso] IPC latency/jitter/throughput/memory report
//   perf wki-report           Remote/WKI summary statistics
//   perf local-report         Local pipe/process summary statistics
//   perf vmem-report          Local mmap/cache/COW summary statistics
//   perf all-report           Combined WKI and local summary statistics
//   perf run      [--time=boot|unix-ns|iso] <cmd> [args] Trace cmd+descendants, save to perf.data
//   perf show-map             Show PID->name/cmdline map from perf.data

#include <abi-bits/access.h>
#include <abi-bits/clockid_t.h>
#include <abi-bits/fcntl.h>
#include <abi-bits/mode_t.h>
#include <bits/ssize_t.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/process.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX clock_gettime/nanosleep are declared here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <print>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

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
constexpr int64_t NANOSECONDS_PER_SECOND = 1000000000;
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
constexpr uint64_t PERF_PAGE_SIZE = 4096;
constexpr int BOOT_TIME_COLUMN_WIDTH = 18;
constexpr int UNIX_TIME_COLUMN_WIDTH = 19;
constexpr int ISO_TIME_COLUMN_WIDTH = 30;

constexpr std::size_t INITIAL_FILE_CAPACITY = 131072;
constexpr std::size_t PROC_READ_CAPACITY = 512;
constexpr std::size_t PERF_DRAIN_CAPACITY = 65536;
constexpr std::size_t CPUSTAT_READ_CAPACITY = 16384;
constexpr std::size_t MAX_EVENT_TOKENS = 14;

constexpr std::size_t PID_PREFIX_SIZE = 4;
constexpr std::size_t COMM_PREFIX_SIZE = 6;
constexpr std::size_t CMD_PREFIX_SIZE = 5;

constexpr mode_t PERF_DATA_MODE = 0644;

constexpr std::string_view PERF_DATA_FILE = "perf.data";
constexpr std::string_view PROC_ROOT = "/proc/";
constexpr std::string_view PROC_STAT_SUFFIX = "/stat";
constexpr std::string_view PROC_CMDLINE_SUFFIX = "/cmdline";
constexpr std::string_view PROC_TASK_SUFFIX = "/task";
constexpr std::string_view KPERF_PATH = "/proc/kperf";
constexpr std::string_view KPERFCTL_PATH = "/proc/kperfctl";
constexpr std::string_view KWKISTAT_PATH = "/proc/kwkistat";
constexpr std::string_view KIPCSTAT_PATH = "/proc/kipcstat";
constexpr std::string_view KCPUSTAT_PATH = "/proc/kcpustat";
constexpr std::string_view KCONTSTAT_PATH = "/proc/kcontstat";
constexpr std::string_view DEV_NODES_ROOT = "/dev/nodes";
constexpr std::string_view SECTION_HEADER = "--- SECTION";
constexpr std::string_view SECTION_TIMEBASE = "--- SECTION TIMEBASE ---\n";
constexpr std::string_view SECTION_TIMEBASE_END = "--- END TIMEBASE ---\n";
constexpr std::string_view SECTION_EVENTS = "--- SECTION EVENTS ---\n";
constexpr std::string_view SECTION_EVENTS_END = "--- END EVENTS ---\n";
constexpr std::string_view SECTION_WKI_SUMMARY = "--- SECTION WKI_SUMMARY ---\n";
constexpr std::string_view SECTION_WKI_SUMMARY_END = "--- END WKI_SUMMARY ---\n";
constexpr std::string_view SECTION_IPC_STATS = "--- SECTION IPC_STATS ---\n";
constexpr std::string_view SECTION_IPC_STATS_END = "--- END IPC_STATS ---\n";
constexpr std::string_view SECTION_PROC_MAP = "--- SECTION PROC_MAP ---\n";
constexpr std::string_view SECTION_PROC_MAP_END = "--- END PROC_MAP ---\n";
constexpr std::string_view SECTION_PEER_MAP = "--- SECTION PEER_MAP ---\n";
constexpr std::string_view SECTION_PEER_MAP_END = "--- END PEER_MAP ---\n";
constexpr std::string_view END_PREFIX = "--- END";
constexpr std::string_view REALTIME_OFFSET_NS_KEY = "realtime_offset_ns=";
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

    [[nodiscard]] auto get() const -> int { return fd; }

    [[nodiscard]] auto valid() const -> bool { return fd >= 0; }

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

    [[nodiscard]] auto get() const -> DIR* { return dir; }

    [[nodiscard]] auto valid() const -> bool { return dir != nullptr; }

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
    std::string wait_channel;
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
    uint64_t samples{};
    uint64_t total_us{};
    uint64_t avg_us{};
    uint64_t max_us{};
    uint64_t p50_us{};
    uint64_t p95_us{};
    uint64_t p99_us{};
    uint64_t p999_us{};
    uint64_t p9999_us{};
    uint64_t p99999_us{};
};

struct IpcStatsSnapshot {
    uint64_t exports{};
    uint64_t proxies{};
    uint64_t pump_tasks{};
    uint64_t ring_bytes{};
    uint64_t ring_used{};
    uint64_t blocked_readers{};
    uint64_t poll_waiters{};
    uint64_t pending_deliveries{};
    uint64_t pending_chunks{};
    uint64_t pending_bytes{};
    uint64_t export_backlogs{};
    uint64_t export_backlog_chunks{};
    uint64_t export_backlog_bytes{};
    uint64_t export_flush_queue{};
    uint64_t dev_op_queue{};
    uint64_t dev_op_payload_bytes{};
    uint64_t approx_alloc_bytes{};
    uint64_t local_pipe_active{};
    uint64_t local_pipe_created{};
    uint64_t local_pipe_peak{};
    uint64_t local_pipe_capacity{};
    uint64_t local_pipe_peak_capacity{};
    uint64_t local_pipe_buffered{};
    uint64_t local_pipe_reader_waiters{};
    uint64_t local_pipe_writer_waiters{};
    uint64_t local_pipe_poll_waiters{};
    uint64_t local_pipe_direct_writes{};
    uint64_t local_pipe_read_closed{};
    uint64_t local_pipe_write_closed{};
    uint64_t local_pipe_approx_alloc_bytes{};
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
    std::optional<uint64_t> first_ts_ns;
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
    std::string phase;
    std::optional<uint64_t> peer;
    std::optional<uint64_t> channel;
    std::optional<uint64_t> correlation;
    std::optional<uint64_t> pid;
    std::optional<uint64_t> min_us;
    std::optional<uint64_t> from_ns;
    std::optional<uint64_t> to_ns;
};

enum class WkiDataView : uint8_t {
    WKI,
    LOCAL,
    VMEM,
    ALL,
};

enum class TimeDisplayFormat : uint8_t {
    BOOT_NS,
    UNIX_NS,
    ISO_REALTIME,
};

struct HotspotStats {
    uint64_t pid{};
    std::string callsite;
    std::string wait_channel;
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
    TimeDisplayFormat time_format{TimeDisplayFormat::BOOT_NS};
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

auto parse_timebase_offset(std::string_view buffer, bool sectioned) -> std::optional<int64_t>;

auto now_ms() -> int64_t {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<int64_t>(ts.tv_sec) * MILLISECONDS_PER_SECOND) + (static_cast<int64_t>(ts.tv_nsec) / NANOSECONDS_PER_MILLISECOND);
}

auto timespec_to_ns(const timespec& ts) -> int64_t {
    return (static_cast<int64_t>(ts.tv_sec) * NANOSECONDS_PER_SECOND) + static_cast<int64_t>(ts.tv_nsec);
}

auto read_clock_ns(clockid_t clock_id) -> std::optional<int64_t> {
    timespec ts{};
    if (clock_gettime(clock_id, &ts) != 0) {
        return std::nullopt;
    }
    return timespec_to_ns(ts);
}

auto realtime_offset_ns() -> int64_t {
    auto const MONOTONIC_NS = read_clock_ns(CLOCK_MONOTONIC);
    auto const REALTIME_NS = read_clock_ns(CLOCK_REALTIME);
    if (!MONOTONIC_NS.has_value() || !REALTIME_NS.has_value()) {
        return 0;
    }
    return *REALTIME_NS - *MONOTONIC_NS;
}

struct TimeDisplay {
    TimeDisplayFormat format{TimeDisplayFormat::BOOT_NS};
    int64_t realtime_offset_ns{};
};

auto make_time_display(const WkiDisplayOptions& options, std::string_view buffer = {}, bool sectioned = false) -> TimeDisplay {
    TimeDisplay display{.format = options.time_format};
    if (display.format != TimeDisplayFormat::BOOT_NS) {
        display.realtime_offset_ns = parse_timebase_offset(buffer, sectioned).value_or(realtime_offset_ns());
    }
    return display;
}

auto add_realtime_offset(uint64_t boot_ns, int64_t offset_ns) -> int64_t {
    if (boot_ns > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::numeric_limits<int64_t>::max();
    }

    auto const BOOT_NS = static_cast<int64_t>(boot_ns);
    if (offset_ns > 0 && BOOT_NS > std::numeric_limits<int64_t>::max() - offset_ns) {
        return std::numeric_limits<int64_t>::max();
    }
    if (offset_ns < 0 && BOOT_NS < std::numeric_limits<int64_t>::min() - offset_ns) {
        return std::numeric_limits<int64_t>::min();
    }
    return BOOT_NS + offset_ns;
}

auto format_nine_digits(uint64_t value) -> std::string {
    std::string out(9, '0');
    for (auto index = out.size(); index > 0; --index) {
        out.at(index - 1) = static_cast<char>('0' + (value % 10U));
        value /= 10U;
    }
    return out;
}

auto format_iso_realtime_ns(int64_t realtime_ns) -> std::string {
    int64_t seconds = realtime_ns / NANOSECONDS_PER_SECOND;
    int64_t nanos = realtime_ns % NANOSECONDS_PER_SECOND;
    if (nanos < 0) {
        nanos += NANOSECONDS_PER_SECOND;
        --seconds;
    }

    auto const UNIX_SECONDS = static_cast<time_t>(seconds);
    tm utc{};
    if (gmtime_r(&UNIX_SECONDS, &utc) == nullptr) {
        return std::to_string(realtime_ns);
    }

    std::array<char, 32> prefix{};
    std::size_t const PREFIX_LEN = strftime(prefix.data(), prefix.size(), "%Y-%m-%dT%H:%M:%S", &utc);
    if (PREFIX_LEN == 0) {
        return std::to_string(realtime_ns);
    }

    std::string out(prefix.data(), PREFIX_LEN);
    out += '.';
    out += format_nine_digits(static_cast<uint64_t>(nanos));
    out += 'Z';
    return out;
}

auto format_event_time(uint64_t boot_ns, const TimeDisplay& display) -> std::string {
    switch (display.format) {
        case TimeDisplayFormat::BOOT_NS:
            return std::to_string(boot_ns);
        case TimeDisplayFormat::UNIX_NS:
            return std::to_string(add_realtime_offset(boot_ns, display.realtime_offset_ns));
        case TimeDisplayFormat::ISO_REALTIME:
            return format_iso_realtime_ns(add_realtime_offset(boot_ns, display.realtime_offset_ns));
        default:
            return std::to_string(boot_ns);
    }
}

auto format_optional_event_time(const std::optional<uint64_t>& boot_ns, const TimeDisplay& display) -> std::string {
    return boot_ns.has_value() ? format_event_time(*boot_ns, display) : std::string("-");
}

auto time_column_header(const TimeDisplay& display) -> std::string_view {
    switch (display.format) {
        case TimeDisplayFormat::BOOT_NS:
            return "TIME(boot-ns)";
        case TimeDisplayFormat::UNIX_NS:
            return "TIME(unix-ns)";
        case TimeDisplayFormat::ISO_REALTIME:
            return "TIME(iso)";
        default:
            return "TIME(boot-ns)";
    }
}

auto time_column_width(const TimeDisplay& display) -> int {
    switch (display.format) {
        case TimeDisplayFormat::BOOT_NS:
            return BOOT_TIME_COLUMN_WIDTH;
        case TimeDisplayFormat::UNIX_NS:
            return UNIX_TIME_COLUMN_WIDTH;
        case TimeDisplayFormat::ISO_REALTIME:
            return ISO_TIME_COLUMN_WIDTH;
        default:
            return BOOT_TIME_COLUMN_WIDTH;
    }
}

auto syscall_result_i64(uint64_t raw) -> int64_t {
    if (raw <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return static_cast<int64_t>(raw);
    }
    uint64_t const MAGNITUDE = (~raw) + 1U;
    if (MAGNITUDE > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::numeric_limits<int64_t>::min();
    }
    return -static_cast<int64_t>(MAGNITUDE);
}

auto waitpid_nohang(int64_t pid, int32_t* status) -> int64_t {
    return syscall_result_i64(ker::process::waitpid(pid, status, PROC_WAIT_NOHANG, nullptr));
}

void wall_sleep_ms(int target_ms) {
    int64_t const DEADLINE = now_ms() + target_ms;
    while (true) {
        int64_t const REMAINING = DEADLINE - now_ms();
        if (REMAINING <= 0) {
            break;
        }

        int64_t const SLICE_US = std::min<int64_t>(REMAINING, SLEEP_SLICE_MS) * MICROSECONDS_PER_MILLISECOND;
        timespec const REQ{
            .tv_sec = static_cast<time_t>(SLICE_US / (MILLISECONDS_PER_SECOND * MICROSECONDS_PER_MILLISECOND)),
            .tv_nsec =
                static_cast<long>((SLICE_US % (MILLISECONDS_PER_SECOND * MICROSECONDS_PER_MILLISECOND)) * NANOSECONDS_PER_MICROSECOND),
        };
        nanosleep(&REQ, nullptr);
    }
}

auto open_readonly(std::string_view path) -> ScopedFd {
    std::string const OWNED_PATH(path);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    return ScopedFd(open(OWNED_PATH.c_str(), O_RDONLY, 0));
}

auto open_writeonly(std::string_view path) -> ScopedFd {
    std::string const OWNED_PATH(path);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    return ScopedFd(open(OWNED_PATH.c_str(), O_WRONLY, 0));
}

auto open_write_trunc(std::string_view path, mode_t mode = PERF_DATA_MODE) -> ScopedFd {
    std::string const OWNED_PATH(path);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    return ScopedFd(open(OWNED_PATH.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode));
}

void write_all(int fd, std::string_view text) {
    std::size_t written = 0;
    while (written < text.size()) {
        ssize_t const RC = write(fd, text.data() + written, text.size() - written);
        if (RC <= 0) {
            break;
        }
        written += static_cast<std::size_t>(RC);
    }
}

auto read_fd(ScopedFd& fd, std::size_t initial_capacity = INITIAL_FILE_CAPACITY) -> std::string {
    std::size_t const CAPACITY = std::max<std::size_t>(initial_capacity, 1);
    std::string buffer(CAPACITY, '\0');
    std::size_t total = 0;

    for (;;) {
        if (total == buffer.size()) {
            buffer.resize(buffer.size() * 2);
        }

        ssize_t const COUNT = read(fd.get(), buffer.data() + total, buffer.size() - total);
        if (COUNT <= 0) {
            break;
        }
        total += static_cast<std::size_t>(COUNT);
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

auto build_proc_task_path(std::string_view pid, std::string_view tid, std::string_view suffix) -> std::string {
    std::string path(PROC_ROOT);
    path += pid;
    path += PROC_TASK_SUFFIX;
    path += '/';
    path += tid;
    path += suffix;
    return path;
}

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
    std::string const OWNED(text);
    return static_cast<uint64_t>(strtoull(OWNED.c_str(), nullptr, base));
}

auto parse_i64(std::string_view text, int base = PARSE_BASE_DECIMAL) -> int64_t {
    std::string const OWNED(text);
    return static_cast<int64_t>(strtoll(OWNED.c_str(), nullptr, base));
}

auto parse_u32(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint32_t {
    std::string const OWNED(text);
    return static_cast<uint32_t>(strtoul(OWNED.c_str(), nullptr, base));
}

auto parse_u8(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint8_t {
    std::string const OWNED(text);
    return static_cast<uint8_t>(strtoul(OWNED.c_str(), nullptr, base));
}

auto trim_left(std::string_view text) -> std::string_view {
    while (!text.empty() && text.front() == ' ') {
        text.remove_prefix(1);
    }
    return text;
}

auto next_token(std::string_view text, std::size_t& pos) -> std::string_view {
    while (pos < text.size() && text.at(pos) == ' ') {
        ++pos;
    }
    if (pos >= text.size()) {
        return {};
    }

    std::size_t end = pos;
    while (end < text.size() && text.at(end) != ' ') {
        ++end;
    }

    std::string_view const TOKEN = text.substr(pos, end - pos);
    pos = end;
    return TOKEN;
}

auto next_line(std::string_view text, std::size_t& pos) -> std::string_view {
    if (pos >= text.size()) {
        return {};
    }

    std::size_t const END = text.find('\n', pos);
    if (END == std::string_view::npos) {
        std::string_view const LINE = text.substr(pos);
        pos = text.size();
        return LINE;
    }

    std::string_view const LINE = text.substr(pos, END - pos);
    pos = END + 1;
    return LINE;
}

auto parse_timebase_offset(std::string_view buffer, bool sectioned) -> std::optional<int64_t> {
    if (!sectioned) {
        return std::nullopt;
    }

    std::size_t const HEADER_POS = buffer.find(SECTION_TIMEBASE);
    if (HEADER_POS == std::string_view::npos) {
        return std::nullopt;
    }

    std::size_t pos = buffer.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    ++pos;

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }

        std::size_t const KEY_POS = LINE.find(REALTIME_OFFSET_NS_KEY);
        if (KEY_POS == std::string_view::npos) {
            continue;
        }

        std::size_t const VALUE_POS = KEY_POS + REALTIME_OFFSET_NS_KEY.size();
        std::size_t const END = LINE.find(' ', VALUE_POS);
        return parse_i64(LINE.substr(VALUE_POS, END == std::string_view::npos ? LINE.size() - VALUE_POS : END - VALUE_POS));
    }

    return std::nullopt;
}

auto is_all_digits(std::string_view text) -> bool {
    if (text.empty()) {
        return false;
    }

    return std::ranges::all_of(text, [](char ch) { return ch >= '0' && ch <= '9'; });
}

auto is_dot_entry(std::string_view text) -> bool { return text == "." || text == ".."; }

auto parse_stat(std::string_view buf, StatInfo& out) -> bool {
    std::size_t const PAREN = buf.find('(');
    std::size_t const PAREN_END = buf.rfind(')');
    if (PAREN == std::string_view::npos || PAREN_END == std::string_view::npos || PAREN_END <= PAREN) {
        return false;
    }

    out.pid = parse_u64(trim_left(buf.substr(0, PAREN)));
    out.comm = std::string(buf.substr(PAREN + 1, PAREN_END - PAREN - 1));

    std::string_view const TAIL = trim_left(buf.substr(PAREN_END + 1));
    std::size_t pos = 0;

    std::string_view const STATE_TOKEN = next_token(TAIL, pos);
    if (STATE_TOKEN.empty()) {
        return false;
    }
    out.state = STATE_TOKEN.front();

    if (next_token(TAIL, pos).empty()) {
        return false;
    }

    std::string_view const PGID_TOKEN = next_token(TAIL, pos);
    if (PGID_TOKEN.empty()) {
        return false;
    }
    out.pgid = parse_u64(PGID_TOKEN);

    for (int index = 0; index < PROC_STAT_SKIP_FIELDS; ++index) {
        if (next_token(TAIL, pos).empty()) {
            return false;
        }
    }

    std::string_view const UTIME_TOKEN = next_token(TAIL, pos);
    std::string_view const STIME_TOKEN = next_token(TAIL, pos);
    if (UTIME_TOKEN.empty() || STIME_TOKEN.empty()) {
        return false;
    }

    out.utime = parse_u64(UTIME_TOKEN);
    out.stime = parse_u64(STIME_TOKEN);
    return true;
}

template <typename Func>
auto emit_proc_stat(Func& func, std::string_view name, std::string_view path) -> bool {
    auto stat_text = read_file(path, PROC_READ_CAPACITY);
    if (!stat_text.has_value()) {
        return false;
    }

    StatInfo info{};
    if (!parse_stat(*stat_text, info)) {
        return false;
    }

    func(info, name);
    return true;
}

template <typename Func>
void for_each_process_stat(Func func) {
    ScopedDir const DIR(opendir("/proc"));
    if (!DIR.valid()) {
        return;
    }

    dirent const* entry = nullptr;
    while ((entry = readdir(DIR.get())) != nullptr) {
        std::string_view const NAME{&entry->d_name[0]};
        if (!is_all_digits(NAME)) {
            continue;
        }

        bool emitted_task = false;
        ScopedDir const TASK_DIR(opendir(build_proc_path(NAME, PROC_TASK_SUFFIX).c_str()));
        if (TASK_DIR.valid()) {
            dirent const* task_entry = nullptr;
            while ((task_entry = readdir(TASK_DIR.get())) != nullptr) {
                std::string_view const TID{&task_entry->d_name[0]};
                if (!is_all_digits(TID)) {
                    continue;
                }
                emitted_task = emit_proc_stat(func, TID, build_proc_task_path(NAME, TID, PROC_STAT_SUFFIX)) || emitted_task;
            }
        }

        if (emitted_task) {
            continue;
        }
        (void)emit_proc_stat(func, NAME, build_proc_path(NAME, PROC_STAT_SUFFIX));
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

void write_section_timebase(int fd) {
    write_all(fd, SECTION_TIMEBASE);
    std::string line(REALTIME_OFFSET_NS_KEY);
    line += std::to_string(realtime_offset_ns());
    line += '\n';
    write_all(fd, line);
    write_all(fd, SECTION_TIMEBASE_END);
}

auto collect_live_wki_peer_map() -> std::vector<WkiPeerMapEntry> {
    std::vector<WkiPeerMapEntry> entries;
    ScopedDir const DIR(opendir(std::string(DEV_NODES_ROOT).c_str()));
    if (!DIR.valid()) {
        return entries;
    }

    dirent const* entry = nullptr;
    while ((entry = readdir(DIR.get())) != nullptr) {
        std::string_view const HOSTNAME{&entry->d_name[0]};
        if (HOSTNAME.empty() || is_dot_entry(HOSTNAME)) {
            continue;
        }

        auto id_text = read_file(build_dev_nodes_path(HOSTNAME, "/id"), PROC_READ_CAPACITY);
        if (!id_text.has_value() || id_text->empty()) {
            continue;
        }

        entries.push_back(WkiPeerMapEntry{
            .peer = parse_u64(*id_text, 0),
            .hostname = std::string(HOSTNAME),
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

auto write_section_ipc_stats(int fd) -> ssize_t {
    auto stats = read_file(KIPCSTAT_PATH, PROC_READ_CAPACITY);
    if (!stats.has_value() || stats->empty()) {
        return 0;
    }

    write_all(fd, SECTION_IPC_STATS);
    write_all(fd, *stats);
    write_all(fd, SECTION_IPC_STATS_END);
    return static_cast<ssize_t>(stats->size());
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
    ScopedFd const FD = open_write_trunc(PERF_DATA_FILE);
    if (!FD.valid()) {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
        return;
    }

    write_section_timebase(FD.get());
    write_section_proc_map(FD.get());
    write_section_peer_map(FD.get());
    ssize_t event_bytes = write_section_events(FD.get());
    ssize_t const SUMMARY_BYTES = write_section_wki_summary(FD.get());
    ssize_t const IPC_BYTES = write_section_ipc_stats(FD.get());

    if (event_bytes <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0) {
        std::println("perf: ring buffer empty - PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes)", PERF_DATA_FILE, event_bytes, SUMMARY_BYTES,
                     IPC_BYTES);
    }
}

void set_recording_enabled(bool enabled, const char* filter = nullptr) {
    ScopedFd const FD = open_writeonly(KPERFCTL_PATH);
    if (!FD.valid()) {
        return;
    }
    if (enabled && filter != nullptr) {
        std::string const CMD = std::string("enable ") + filter;
        write_all(FD.get(), CMD);
    } else {
        write_all(FD.get(), enabled ? "enable" : "disable");
    }
}

void cmd_stat(int ms) {
    if (ms < 1) {
        ms = DEFAULT_SAMPLE_MS;
    }

    auto before = collect_stats();
    int64_t const START_MS = now_ms();
    wall_sleep_ms(ms);
    int64_t elapsed_ms = now_ms() - START_MS;
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

            uint64_t const DELTA_TICKS = (current.utime + current.stime) - (prior.utime + prior.stime);
            double const CPU =
                static_cast<double>(DELTA_TICKS) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);

            rows.push_back(CpuRow{
                .pid = current.pid,
                .comm = current.comm,
                .state = current.state,
                .cpu_pct = CPU,
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

    ScopedFd const CONTROL_FD = open_writeonly(KPERFCTL_PATH);
    if (!CONTROL_FD.valid()) {
        std::println("perf: cannot open /proc/kperfctl");
        return;
    }

    if (filter != nullptr) {
        std::string const CMD = std::string("enable ") + filter;
        write_all(CONTROL_FD.get(), CMD);
        std::println("perf: recording with filter '{}' for {} ms...", filter, ms);
    } else {
        write_all(CONTROL_FD.get(), "enable");
        std::println("perf: recording for {} ms...", ms);
    }

    ScopedFd data_fd = open_write_trunc(PERF_DATA_FILE);
    if (data_fd.valid()) {
        write_section_timebase(data_fd.get());
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

    int64_t const DEADLINE_MS = now_ms() + ms;
    int64_t last_drain_ms = now_ms();
    ssize_t total_event_bytes = 0;

    while (true) {
        int64_t const REMAINING_MS = DEADLINE_MS - now_ms();
        if (REMAINING_MS <= 0) {
            break;
        }

        wall_sleep_ms(static_cast<int>(std::min<int64_t>(REMAINING_MS, SLEEP_SLICE_MS)));
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

    write_all(CONTROL_FD.get(), "disable");
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
    ssize_t const SUMMARY_BYTES = write_section_wki_summary(data_fd.get());
    ssize_t const IPC_BYTES = write_section_ipc_stats(data_fd.get());
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

    if (total_event_bytes <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0) {
        std::println("perf: ring buffer empty - PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes)", PERF_DATA_FILE, total_event_bytes, SUMMARY_BYTES,
                     IPC_BYTES);
    }
}

auto parse_proc_map_section(std::string_view buffer) -> std::vector<ProcMapEntry> {
    std::vector<ProcMapEntry> entries;
    std::size_t const HEADER_POS = buffer.find(SECTION_PROC_MAP);
    if (HEADER_POS == std::string_view::npos) {
        return entries;
    }

    std::size_t pos = buffer.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return entries;
    }
    ++pos;

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }
        if (LINE.empty()) {
            continue;
        }

        std::size_t const PID_POS = LINE.find("pid=");
        std::size_t const COMM_POS = LINE.find(" comm=");
        if (PID_POS == std::string_view::npos || COMM_POS == std::string_view::npos) {
            continue;
        }

        std::size_t const CMD_POS = LINE.find(CMD_FIELD_PREFIX, COMM_POS + 1);
        std::string_view const PID_TEXT = LINE.substr(PID_POS + PID_PREFIX_SIZE, COMM_POS - (PID_POS + PID_PREFIX_SIZE));
        std::string_view const COMM_TEXT = CMD_POS == std::string_view::npos
                                               ? LINE.substr(COMM_POS + COMM_PREFIX_SIZE)
                                               : LINE.substr(COMM_POS + COMM_PREFIX_SIZE, CMD_POS - (COMM_POS + COMM_PREFIX_SIZE));

        entries.push_back(ProcMapEntry{.pid = parse_u64(PID_TEXT), .comm = std::string(COMM_TEXT)});
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
    std::size_t const HEADER_POS = buffer.find(SECTION_PEER_MAP);
    if (HEADER_POS == std::string_view::npos) {
        return entries;
    }

    std::size_t pos = buffer.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return entries;
    }
    ++pos;

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }
        if (LINE.empty()) {
            continue;
        }

        WkiPeerMapEntry entry{};
        if (parse_peer_map_line(LINE, entry)) {
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
    std::size_t const END = line.find(' ', key_pos);
    return line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos);
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
    out.samples = parse_u64(extract_value(line, "samples="));
    out.total_us = parse_u64(extract_value(line, "total_us="));
    out.avg_us = parse_u64(extract_value(line, "avg_us="));
    if (out.total_us == 0 && out.avg_us != 0 && out.calls != 0) {
        out.total_us = out.avg_us * out.calls;
    }
    if (out.samples == 0) {
        out.samples = out.calls;
    }
    out.max_us = parse_u64(extract_value(line, "max_us="));
    out.p50_us = parse_u64(extract_value(line, "p50_us="));
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
        std::size_t const HEADER_POS = buffer.find(SECTION_WKI_SUMMARY);
        if (HEADER_POS == std::string_view::npos) {
            return rows;
        }

        pos = buffer.find('\n', HEADER_POS);
        if (pos == std::string_view::npos) {
            return rows;
        }
        ++pos;
    }

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (sectioned && LINE.starts_with(END_PREFIX)) {
            break;
        }
        WkiSummaryRow row{};
        if (parse_wki_summary_line(LINE, row)) {
            rows.push_back(std::move(row));
        }
    }

    return rows;
}

auto parse_ipc_stats_line(std::string_view line, IpcStatsSnapshot& out) -> bool {
    if (line.empty() || !line.starts_with("exports=")) {
        return false;
    }

    out = {};
    out.exports = parse_u64(extract_value(line, "exports="));
    out.proxies = parse_u64(extract_value(line, "proxies="));
    out.pump_tasks = parse_u64(extract_value(line, "pump_tasks="));
    out.ring_bytes = parse_u64(extract_value(line, "ring_bytes="));
    out.ring_used = parse_u64(extract_value(line, "ring_used="));
    out.blocked_readers = parse_u64(extract_value(line, "blocked_readers="));
    out.poll_waiters = parse_u64(extract_value(line, "poll_waiters="));
    out.pending_deliveries = parse_u64(extract_value(line, "pending_deliveries="));
    out.pending_chunks = parse_u64(extract_value(line, "pending_chunks="));
    out.pending_bytes = parse_u64(extract_value(line, "pending_bytes="));
    out.export_backlogs = parse_u64(extract_value(line, "export_backlogs="));
    out.export_backlog_chunks = parse_u64(extract_value(line, "export_backlog_chunks="));
    out.export_backlog_bytes = parse_u64(extract_value(line, "export_backlog_bytes="));
    out.export_flush_queue = parse_u64(extract_value(line, "export_flush_queue="));
    out.dev_op_queue = parse_u64(extract_value(line, "dev_op_queue="));
    out.dev_op_payload_bytes = parse_u64(extract_value(line, "dev_op_payload_bytes="));
    out.approx_alloc_bytes = parse_u64(extract_value(line, "approx_alloc_bytes="));
    out.local_pipe_active = parse_u64(extract_value(line, "local_pipe_active="));
    out.local_pipe_created = parse_u64(extract_value(line, "local_pipe_created="));
    out.local_pipe_peak = parse_u64(extract_value(line, "local_pipe_peak="));
    out.local_pipe_capacity = parse_u64(extract_value(line, "local_pipe_capacity="));
    out.local_pipe_peak_capacity = parse_u64(extract_value(line, "local_pipe_peak_capacity="));
    out.local_pipe_buffered = parse_u64(extract_value(line, "local_pipe_buffered="));
    out.local_pipe_reader_waiters = parse_u64(extract_value(line, "local_pipe_reader_waiters="));
    out.local_pipe_writer_waiters = parse_u64(extract_value(line, "local_pipe_writer_waiters="));
    out.local_pipe_poll_waiters = parse_u64(extract_value(line, "local_pipe_poll_waiters="));
    out.local_pipe_direct_writes = parse_u64(extract_value(line, "local_pipe_direct_writes="));
    out.local_pipe_read_closed = parse_u64(extract_value(line, "local_pipe_read_closed="));
    out.local_pipe_write_closed = parse_u64(extract_value(line, "local_pipe_write_closed="));
    out.local_pipe_approx_alloc_bytes = parse_u64(extract_value(line, "local_pipe_approx_alloc_bytes="));
    return true;
}

auto parse_ipc_stats_section(std::string_view buffer, bool sectioned) -> std::optional<IpcStatsSnapshot> {
    std::size_t pos = 0;

    if (sectioned) {
        std::size_t const HEADER_POS = buffer.find(SECTION_IPC_STATS);
        if (HEADER_POS == std::string_view::npos) {
            return std::nullopt;
        }

        pos = buffer.find('\n', HEADER_POS);
        if (pos == std::string_view::npos) {
            return std::nullopt;
        }
        ++pos;
    }

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (sectioned && LINE.starts_with(END_PREFIX)) {
            break;
        }

        IpcStatsSnapshot snapshot{};
        if (parse_ipc_stats_line(LINE, snapshot)) {
            return snapshot;
        }
    }

    return std::nullopt;
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
    constexpr int TOK_WAIT = 8;
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
    if (line.size() < 2 || line.at(1) != ' ') {
        return false;
    }

    std::array<std::string_view, MAX_EVENT_TOKENS> tokens{};
    int token_count = 0;
    std::size_t pos = 2;
    while (std::cmp_less(token_count, tokens.size())) {
        std::string_view const TOKEN = next_token(line, pos);
        if (TOKEN.empty()) {
            break;
        }
        tokens.at(static_cast<std::size_t>(token_count)) = TOKEN;
        ++token_count;
    }

    if (token_count < EVENT_MIN_TOKEN_COUNT) {
        return false;
    }

    out.type = line.front();
    out.ts_ns = parse_u64(tokens.at(TOK_TS));
    out.cpu = parse_u32(tokens.at(TOK_CPU));

    switch (out.type) {
        case 'S':
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.data = parse_u64(tokens.at(TOK_DATA), 0);
            out.lag = parse_i64(tokens.at(TOK_LAG));
            out.flags = parse_u8(tokens.at(TOK_FLAGS));
            return true;

        case 'X':
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.other_pid = parse_u64(tokens.at(TOK_DATA));
            out.lag = parse_i64(tokens.at(TOK_LAG));
            out.flags = parse_u8(tokens.at(TOK_FLAGS));
            if (token_count >= TOK_AUX + 1) {
                out.aux = parse_u32(tokens.at(TOK_AUX));
            }
            if (token_count >= TOK_CALLSITE + 1) {
                out.callsite = std::string(tokens.at(TOK_CALLSITE));
            }
            return true;

        case 'W':
        case 'B':
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.data = parse_u64(tokens.at(TOK_DATA));
            if (token_count >= EVENT_EXTENDED_TOKEN_COUNT) {
                out.aux = parse_u32(tokens.at(TOK_LAG));
                out.flags = parse_u8(tokens.at(TOK_FLAGS));
                if (token_count >= TOK_CALLSITE + 1) {
                    out.callsite = std::string(tokens.at(TOK_CALLSITE));
                }
                if (token_count >= TOK_WAIT + 1) {
                    out.wait_channel = std::string(tokens.at(TOK_WAIT));
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
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.subsys_name = std::string(tokens.at(TOK_DATA));  // tok 3 = subsys_name
            out.flags = parse_u8(tokens.at(TOK_LAG));            // tok 4 = flags
            out.lag = parse_i64(tokens.at(TOK_FLAGS));           // tok 5 = element count
            if (token_count >= TOK_AUX + 1) {
                out.aux = parse_u32(tokens.at(TOK_AUX));  // tok 6 = capacity
            }
            if (token_count >= TOK_CALLSITE + 1) {
                out.callsite = std::string(tokens.at(TOK_CALLSITE));
            }
            return true;

        case 'K':
            if (token_count < TOK_WKI_AUX + 1) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.scope_name = std::string(tokens.at(TOK_SCOPE));
            out.op_name = std::string(tokens.at(TOK_OP));
            out.phase_name = std::string(tokens.at(TOK_PHASE));
            out.peer = parse_u64(tokens.at(TOK_PEER), 0);
            out.channel = parse_u64(tokens.at(TOK_CHANNEL), 0);
            out.correlation = parse_u64(tokens.at(TOK_CORR), 0);
            out.status = static_cast<int32_t>(parse_i64(tokens.at(TOK_STATUS), 0));
            out.aux = parse_u32(tokens.at(TOK_WKI_AUX), 0);
            if (token_count >= TOK_WKI_CALLSITE + 1) {
                out.callsite = std::string(tokens.at(TOK_WKI_CALLSITE));
            }
            return true;

        default:
            return false;
    }
}

auto next_event(std::string_view text, std::size_t& pos, bool sectioned, EventInfo& out) -> bool {
    while (pos < text.size()) {
        std::size_t const LINE_START = pos;
        std::string_view const LINE = next_line(text, pos);
        if (sectioned && text.substr(LINE_START).starts_with(END_PREFIX)) {
            return false;
        }
        if (LINE.empty()) {
            continue;
        }
        if (parse_event_line(LINE, out)) {
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
    std::string_view const COMM = comm_of_pid(proc_map, pid);
    if (!COMM.empty()) {
        std::string name = std::to_string(pid);
        name += '(';
        name += COMM;
        name += ')';
        return name;
    }
    return std::to_string(pid);
}

auto display_callsite(std::string_view callsite) -> std::string_view { return callsite.empty() ? UNKNOWN_CALLSITE : callsite; }

constexpr uint32_t WKI_STALL_FLAG_OUTSTANDING = 1U << 0;
constexpr uint32_t WKI_STALL_FLAG_NO_CREDITS = 1U << 1;
constexpr uint32_t WKI_STALL_FLAG_ACK_PENDING = 1U << 2;
constexpr uint32_t WKI_STALL_FLAG_HAS_RETRANSMIT = 1U << 3;
constexpr uint32_t WKI_STALL_FLAG_RETRANSMIT_DUE = 1U << 4;
constexpr uint32_t WKI_STALL_FLAG_FAST_RETRANSMIT = 1U << 5;
constexpr uint32_t WKI_STALL_FLAG_ACK_DELAY_DUE = 1U << 6;

auto is_transport_stall_event(const EventInfo& event) -> bool { return event.scope_name == "transport" && event.op_name == "stall"; }

auto is_local_vmem_event(const EventInfo& event) -> bool { return event.scope_name == "local_vmem"; }

auto is_local_loader_event(const EventInfo& event) -> bool { return event.scope_name == "local_loader"; }

auto is_local_xfs_event(const EventInfo& event) -> bool { return event.scope_name == "local_xfs"; }

auto is_local_irq_event(const EventInfo& event) -> bool { return event.scope_name == "local_irq"; }

auto is_vmem_cow_op(std::string_view op) -> bool { return op == "cow_zero" || op == "cow_copy" || op == "cow_promote"; }

auto is_vmem_file_cache_op(std::string_view op) -> bool { return op.starts_with("file_cache_"); }

auto format_cow_ref_category(uint64_t category) -> std::string_view {
    switch (category) {
        case 1:
            return "single";
        case 2:
            return "shared_le4";
        case 3:
            return "shared_le16";
        case 4:
            return "shared_gt16";
        default:
            return "?";
    }
}

auto append_flag_name(std::string& out, bool& first, uint32_t flags, uint32_t bit, std::string_view name) -> void {
    if ((flags & bit) == 0) {
        return;
    }
    if (!first) {
        out += '|';
    }
    out += name;
    first = false;
}

auto format_transport_stall_flags(uint32_t flags) -> std::string {
    std::string out;
    bool first = true;
    append_flag_name(out, first, flags, WKI_STALL_FLAG_OUTSTANDING, "out");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_NO_CREDITS, "cred0");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_ACK_PENDING, "ack");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_HAS_RETRANSMIT, "rt");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_RETRANSMIT_DUE, "rto");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_FAST_RETRANSMIT, "fastrt");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_ACK_DELAY_DUE, "ackdue");
    if (out.empty()) {
        out = "-";
    }
    return out;
}

auto format_transport_stall_detail(const EventInfo& event) -> std::string {
    auto const STATUS = static_cast<uint32_t>(event.status);
    uint32_t const FLAGS = STATUS & 0xFFU;
    uint32_t const CREDITS = (STATUS >> 8U) & 0xFFU;
    uint32_t const RETRANSMIT_COUNT = (STATUS >> 16U) & 0xFFU;
    uint32_t const INFLIGHT = (STATUS >> 24U) & 0x7FU;

    std::string out;
    out += "inflight=";
    out += std::to_string(INFLIGHT);
    out += " credits=";
    out += std::to_string(CREDITS);
    out += " rtq=";
    out += std::to_string(RETRANSMIT_COUNT);
    out += " flags=";
    out += format_transport_stall_flags(FLAGS);
    return out;
}

auto format_local_vmem_detail(const EventInfo& event) -> std::string {
    std::string out;
    out += is_vmem_file_cache_op(event.op_name) ? "offset=" : "addr=";
    out += std::string(display_callsite(event.callsite));
    out += " pages=";
    out += std::to_string(event.peer);
    if (is_vmem_cow_op(event.op_name)) {
        out += " refcat=";
        out += std::string(format_cow_ref_category(event.channel));
        out += " ref=";
        out += std::to_string(event.status);
    } else {
        out += " detail=";
        out += std::to_string(event.channel);
        out += " status=";
        out += std::to_string(event.status);
    }
    return out;
}

auto format_local_loader_detail(const EventInfo& event) -> std::string {
    std::string out;
    out += "pages=";
    out += std::to_string(event.peer);
    if (event.op_name.starts_with("pt_load_")) {
        auto const PACKED = static_cast<uint32_t>(event.status);
        uint32_t const ALLOCATED = (PACKED >> 16U) & 0xFFFFU;
        uint32_t const ALREADY = PACKED & 0xFFFFU;
        out += " seg=";
        out += std::to_string(event.channel);
        out += " alloc=";
        out += std::to_string(ALLOCATED);
        out += " already=";
        out += std::to_string(ALREADY);
        out += " addr=";
        out += std::string(display_callsite(event.callsite));
    } else {
        out += " site=";
        out += std::string(display_callsite(event.callsite));
    }
    return out;
}

auto format_local_xfs_detail(const EventInfo& event) -> std::string {
    std::string out(display_callsite(event.callsite));
    out += " bytes~";
    out += std::to_string(static_cast<uint64_t>(event.channel) * 1024ULL);
    return out;
}

auto format_local_irq_kind(uint64_t kind) -> std::string_view {
    switch (kind) {
        case 1:
            return "context";
        case 2:
            return "legacy";
        case 3:
            return "unhandled";
        case 4:
            return "timer";
        default:
            return "?";
    }
}

auto format_local_irq_detail(const EventInfo& event) -> std::string {
    std::string out;
    out += "vector=";
    out += std::to_string(event.peer);
    out += " kind=";
    out += std::string(format_local_irq_kind(event.channel));
    out += " site=";
    out += std::string(display_callsite(event.callsite));
    return out;
}

auto format_wki_trace_detail(const EventInfo& event) -> std::string {
    std::string out(display_callsite(event.callsite));
    if (is_local_vmem_event(event)) {
        return format_local_vmem_detail(event);
    }
    if (is_local_loader_event(event)) {
        return format_local_loader_detail(event);
    }
    if (is_local_xfs_event(event)) {
        return format_local_xfs_detail(event);
    }
    if (is_local_irq_event(event)) {
        return format_local_irq_detail(event);
    }
    if (!is_transport_stall_event(event)) {
        return out;
    }

    out += " ";
    out += format_transport_stall_detail(event);
    return out;
}

auto display_wait_channel(std::string_view wait_channel) -> std::string_view {
    return wait_channel.empty() ? UNKNOWN_CALLSITE : wait_channel;
}

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

auto hotspot_for(std::vector<HotspotStats>& rows, uint64_t pid, std::string_view callsite, std::string_view wait_channel,
                 const std::vector<ProcMapEntry>& proc_map) -> HotspotStats& {
    std::string_view const DISPLAY_SITE = display_callsite(callsite);
    std::string_view const DISPLAY_WAIT = display_wait_channel(wait_channel);
    for (auto& row : rows) {
        if (row.pid == pid && row.callsite == DISPLAY_SITE && row.wait_channel == DISPLAY_WAIT) {
            return row;
        }
    }

    HotspotStats row{};
    row.pid = pid;
    row.callsite = std::string(DISPLAY_SITE);
    row.wait_channel = std::string(DISPLAY_WAIT);
    row.comm = std::string(comm_of_pid(proc_map, pid));
    rows.push_back(std::move(row));
    return rows.back();
}

auto summarize_hotspots(std::string_view events, bool sectioned, const std::vector<ProcMapEntry>& proc_map) -> std::vector<HotspotStats> {
    std::vector<HotspotStats> rows;
    std::size_t pos = 0;
    EventInfo event{};
    while (next_event(events, pos, sectioned, event)) {
        auto& row = hotspot_for(rows, event.pid, event.callsite, event.wait_channel, proc_map);
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
        constexpr auto BUSY_HDR_FMT = "{:>6}  {:<20}  {:<18}  {:<18}  {:>7}  {:>7}  {:>11}  {:>11}";
        constexpr auto BUSY_SEP_FMT = "{:->6}  {:->20}  {:->18}  {:->18}  {:->7}  {:->7}  {:->11}  {:->11}";
        std::println(BUSY_HDR_FMT, "PID", "NAME", "WAIT", "CALLSITE", "yield", "short", "avg_run(us)", "max_run(us)");
        std::println(BUSY_SEP_FMT, "", "", "", "", "", "", "", "");
        for (std::size_t index = 0; index < busy_yield.size() && index < HOTSPOT_ROW_LIMIT; ++index) {
            const auto& row = busy_yield.at(index);
            uint64_t avg_run = row.yield_count != 0U ? row.yield_run_total_us / row.yield_count : 0;
            std::println("{:>6}  {:<20}  {:<18}  {:<18}  {:>7}  {:>7}  {:>11}  {:>11}", row.pid, row.comm.empty() ? "?" : row.comm,
                         display_wait_channel(row.wait_channel), display_callsite(row.callsite), row.yield_count, row.short_yield_count,
                         avg_run, row.yield_run_max_us);
        }
    }

    std::println("");

    std::vector<HotspotStats> wake_churn = rows;
    auto removed_churn =
        std::ranges::remove_if(wake_churn, [](const HotspotStats& row) { return row.wake_count == 0 && row.sleep_count == 0; });
    wake_churn.erase(removed_churn.begin(), removed_churn.end());
    std::ranges::sort(wake_churn, [](const HotspotStats& lhs, const HotspotStats& rhs) {
        uint64_t const LHS_SCORE = (lhs.short_wake_count * 3U) + (lhs.explicit_wake_count * 2U) + lhs.short_sleep_count;
        uint64_t const RHS_SCORE = (rhs.short_wake_count * 3U) + (rhs.explicit_wake_count * 2U) + rhs.short_sleep_count;
        if (LHS_SCORE != RHS_SCORE) {
            return LHS_SCORE > RHS_SCORE;
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
        constexpr auto CHURN_HDR_FMT = "{:>6}  {:<20}  {:<18}  {:<18}  {:>5}  {:>5}  {:>8}  {:>7}  {:>13}  {:>11}";
        constexpr auto CHURN_SEP_FMT = "{:->6}  {:->20}  {:->18}  {:->18}  {:->5}  {:->5}  {:->8}  {:->7}  {:->13}  {:->11}";
        std::println(CHURN_HDR_FMT, "PID", "NAME", "WAIT", "CALLSITE", "wake", "short", "explicit", "current", "avg_sleep(us)",
                     "avg_run(us)");
        std::println(CHURN_SEP_FMT, "", "", "", "", "", "", "", "", "", "");
        for (std::size_t index = 0; index < wake_churn.size() && index < HOTSPOT_ROW_LIMIT; ++index) {
            const auto& row = wake_churn.at(index);
            uint64_t avg_sleep = row.wake_count != 0U ? row.wake_sleep_total_us / row.wake_count : 0;
            uint64_t avg_run = row.sleep_count != 0U ? row.sleep_run_total_us / row.sleep_count : 0;
            std::println("{:>6}  {:<20}  {:<18}  {:<18}  {:>5}  {:>5}  {:>8}  {:>7}  {:>13}  {:>11}", row.pid,
                         row.comm.empty() ? "?" : row.comm, display_wait_channel(row.wait_channel), display_callsite(row.callsite),
                         row.wake_count, row.short_wake_count, row.explicit_wake_count, row.current_wake_count, avg_sleep, avg_run);
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

    std::string_view const VIEW(*buffer);
    bool const SECTIONED = VIEW.starts_with(SECTION_HEADER);
    std::vector<ProcMapEntry> proc_map;
    std::size_t section_start = 0;

    if (SECTIONED) {
        proc_map = parse_proc_map_section(VIEW);
        std::size_t const EVENTS_HDR = VIEW.find(SECTION_EVENTS);
        if (EVENTS_HDR == std::string_view::npos) {
            std::println("perf: perf.data has no EVENTS section");
            return;
        }
        section_start = VIEW.find('\n', EVENTS_HDR);
        if (section_start == std::string_view::npos) {
            return;
        }
        ++section_start;
    } else {
        for (const auto& stat : collect_stats()) {
            proc_map.push_back(ProcMapEntry{.pid = stat.pid, .comm = stat.comm});
        }
    }

    std::string_view const EVENT_VIEW = VIEW.substr(section_start);
    auto peer_resolver = make_wki_peer_resolver(VIEW, SECTIONED, display_options);
    auto hotspot_rows = summarize_hotspots(EVENT_VIEW, SECTIONED, proc_map);
    print_hotspot_tables(hotspot_rows);
    auto const TIME_DISPLAY = make_time_display(display_options, VIEW, SECTIONED);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);

    std::println("=== perf report [{}] (up to {} events) ============================", src, max_events);
    std::println("{:>3}  {:>{}}  {:>3}  {}", "EVT", time_column_header(TIME_DISPLAY), TIME_WIDTH, "CPU", "DETAILS");
    std::println("{:->3}  {:->{}}  {:->3}  {:->40}", "", "", TIME_WIDTH, "", "");

    int count = 0;
    EventInfo event{};
    std::size_t event_pos = 0;
    while (count < max_events && next_event(EVENT_VIEW, event_pos, SECTIONED, event)) {
        if (event.type == 'S') {
            std::print("SMP  {:>{}}  {:>3}  {:<24} rip={:#016x} lag={:>10}  ", format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH,
                       event.cpu, format_pid_name(event.pid, proc_map), event.data, event.lag);
            print_sched_flags(event.flags);
            std::println("");
        } else if (event.type == 'X') {
            std::print("CTX  {:>{}}  {:>3}  {:<24} -> {:<24} lag={:>10} run_us={:>6} site={}  ",
                       format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                       format_pid_name(event.other_pid, proc_map), event.lag, event.aux, display_callsite(event.callsite));
            print_sched_flags(event.flags);
            std::println("");
        } else if (event.type == 'W') {
            std::print("WKE  {:>{}}  {:>3}  {:<24} wake_at_us={} sleep_us={:>6} wait={} site={}  ",
                       format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                       event.data, event.aux, display_wait_channel(event.wait_channel), display_callsite(event.callsite));
            print_wait_flags(event.flags);
            std::println("");
        } else if (event.type == 'B') {
            std::print("SLP  {:>{}}  {:>3}  {:<24} wake_at_us={} run_us={:>6} wait={} site={}  ",
                       format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                       event.data, event.aux, display_wait_channel(event.wait_channel), display_callsite(event.callsite));
            print_wait_flags(event.flags);
            std::println("");
        } else if (event.type == 'C') {
            std::println("CNT  {:>{}}  {:>3}  {:<24} subsys={} count={} cap={} flags={} site={}",
                         format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                         event.subsys_name, event.lag, event.aux, static_cast<unsigned>(event.flags), display_callsite(event.callsite));
        } else if (event.type == 'K') {
            std::println("WKI  {:>{}}  {:>3}  {:<24} {}:{}:{} peer={} ch={} corr={} status={} aux={} site={}",
                         format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                         event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, peer_resolver), event.channel,
                         event.correlation, event.status, event.aux, display_callsite(event.callsite));
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
    std::println(
        "{:>4}  {:>10}  {:>8}  {:>10}  {:>10}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  "
        "{:>10}",
        "CPU", "ctx", "preempt", "sleep", "wake", "sample", "fast_skip", "ring_write", "timer_irq", "sched_arm", "sched_disarm",
        "local_poke", "slow_scan", "wait_scan", "wait_pass", "wait_max", "timer_wake");
    std::println(
        "{:->4}  {:->10}  {:->8}  {:->10}  {:->10}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}  {:->10}  "
        "{:->10}  {:->10}  {:->10}  {:->10}",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "");

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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println(
            "{:>4}  {:>10}  {:>8}  {:>10}  {:>10}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  "
            "{:>10}",
            get_val("cpu="), get_val("ctx="), get_val("preempt="), get_val("sleep="), get_val("wake="), get_val("sample="),
            get_val("fast_skip="), get_val("ring_write="), get_val("timer_irq="), get_val("sched_arm="), get_val("sched_disarm="),
            get_val("local_poke="), get_val("slow_scan="), get_val("wait_scan="), get_val("wait_pass="), get_val("wait_max="),
            get_val("timer_wake="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}", "CPU", "idle_arm", "idle_disarm",
                 "idle_wake", "wake_ipi", "wake_coal", "gc_pass", "gc_reclaim", "gc_us", "gc_max_us", "lb_push");
    std::println("{:->4}  {:->10}  {:->12}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->8}", "", "", "", "", "", "",
                 "", "", "", "", "");

    pos = 0;
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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}", get_val("cpu="),
                     get_val("idle_arm="), get_val("idle_disarm="), get_val("idle_wake="), get_val("wake_ipi="), get_val("wake_coal="),
                     get_val("gc_pass="), get_val("gc_reclaim="), get_val("gc_us="), get_val("gc_max_us="), get_val("lb_push="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}", "CPU", "arm_wait", "arm_itimer", "arm_vol", "arm_idlework",
                 "arm_runq", "arm_comp");
    std::println("{:->4}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}  {:->10}", "", "", "", "", "", "", "");

    pos = 0;
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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}", get_val("cpu="), get_val("arm_wait="), get_val("arm_itimer="),
                     get_val("arm_vol="), get_val("arm_idlework="), get_val("arm_runq="), get_val("arm_comp="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>11}  {:>12}  {:>13}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>11}  {:>10}  {:>10}", "CPU", "gc_task",
                 "gc_task_max", "gc_detach", "gc_detach_max", "gc_pm", "gc_pm_max", "gc_thr", "gc_thr_max", "gc_misc", "gc_misc_max",
                 "gc_dbg", "gc_dbg_max");
    std::println("{:->4}  {:->10}  {:->11}  {:->12}  {:->13}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->11}  {:->10}  {:->10}", "",
                 "", "", "", "", "", "", "", "", "", "", "", "");

    pos = 0;
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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>11}  {:>12}  {:>13}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>11}  {:>10}  {:>10}",
                     get_val("cpu="), get_val("gc_task_us="), get_val("gc_task_max="), get_val("gc_detach_us="), get_val("gc_detach_max="),
                     get_val("gc_pm_us="), get_val("gc_pm_max="), get_val("gc_thr_us="), get_val("gc_thr_max="), get_val("gc_misc_us="),
                     get_val("gc_misc_max="), get_val("gc_dbg_us="), get_val("gc_dbg_max="));
    }

    std::println("");
    std::println("{:>4}  {:>9}  {:>11}  {:>12}  {:>11}  {:>12}  {:>9}  {:>10}  {:>10}  {:>11}", "CPU", "dus_calls", "collect_us",
                 "collect_max", "data_us", "data_max", "pt_us", "pt_max", "tlb_us", "tlb_max");
    std::println("{:->4}  {:->9}  {:->11}  {:->12}  {:->11}  {:->12}  {:->9}  {:->10}  {:->10}  {:->11}", "", "", "", "", "", "", "", "",
                 "", "");

    pos = 0;
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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>9}  {:>11}  {:>12}  {:>11}  {:>12}  {:>9}  {:>10}  {:>10}  {:>11}", get_val("cpu="), get_val("dus_calls="),
                     get_val("dus_collect_us="), get_val("dus_collect_max="), get_val("dus_data_us="), get_val("dus_data_max="),
                     get_val("dus_pt_us="), get_val("dus_pt_max="), get_val("dus_tlb_us="), get_val("dus_tlb_max="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>11}  {:>15}  {:>11}", "CPU", "dus_leaf", "dus_refdec", "dus_ptfree", "huge_skip",
                 "medium_skip", "dus_corrupt");
    std::println("{:->4}  {:->10}  {:->10}  {:->10}  {:->11}  {:->15}  {:->11}", "", "", "", "", "", "", "");

    pos = 0;
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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>11}  {:>15}  {:>11}", get_val("cpu="), get_val("dus_leaf="), get_val("dus_refdec="),
                     get_val("dus_ptfree="), get_val("dus_huge_skip="), get_val("dus_medium_skip="), get_val("dus_corrupt="));
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
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        auto get_str = [&](std::string_view key) -> std::string_view {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return "?";
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos);
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

    std::size_t const EVENTS_HDR = view.find(SECTION_EVENTS);
    if (EVENTS_HDR == std::string_view::npos) {
        return {};
    }

    std::size_t const SECTION_START = view.find('\n', EVENTS_HDR);
    if (SECTION_START == std::string_view::npos) {
        return {};
    }
    return view.substr(SECTION_START + 1);
}

auto collect_wki_events(std::string_view view, bool sectioned) -> std::vector<EventInfo> {
    std::vector<EventInfo> events;
    std::string_view const EVENT_VIEW = event_section_view(view, sectioned);
    std::size_t pos = 0;
    EventInfo event{};
    while (next_event(EVENT_VIEW, pos, sectioned, event)) {
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

auto load_ipc_stats_text() -> std::optional<WkiLoadedText> {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE, PERF_DRAIN_CAPACITY);
        if (saved.has_value() && !saved->empty()) {
            WkiLoadedText loaded{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved)};
            loaded.sectioned = std::string_view(loaded.buffer).starts_with(SECTION_HEADER);
            if (parse_ipc_stats_section(loaded.buffer, loaded.sectioned).has_value()) {
                return loaded;
            }
        }
    }

    auto live = read_file(KIPCSTAT_PATH, PROC_READ_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }

    return WkiLoadedText{.source = std::string(KIPCSTAT_PATH), .buffer = std::move(*live), .sectioned = false};
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

struct WkiSummaryKey {
    std::string scope;
    std::string op;
    uint64_t peer{};
    uint64_t channel{};
};

struct WkiSummaryAccum {
    WkiSummaryKey key;
    uint64_t calls{};
    uint64_t errors{};
    uint64_t retries{};
    uint64_t total_us{};
    std::vector<uint32_t> samples;
};

auto wki_trace_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool;

auto wki_view_name(WkiDataView view) -> std::string_view {
    switch (view) {
        case WkiDataView::WKI:
            return "wki";
        case WkiDataView::LOCAL:
            return "local";
        case WkiDataView::VMEM:
            return "vmem";
        case WkiDataView::ALL:
            return "all";
        default:
            return "wki";
    }
}

auto is_local_data_scope(std::string_view scope) -> bool { return scope.starts_with("local_"); }

auto summary_peer_for_event(const EventInfo& event) -> uint64_t {
    if (event.scope_name == "local_vmem" || event.scope_name == "local_loader" || event.scope_name == "local_xfs") {
        return 0;
    }
    return event.peer;
}

auto summary_channel_for_event(const EventInfo& event) -> uint64_t {
    if (event.scope_name == "local_vmem" || event.scope_name == "local_loader" || event.scope_name == "local_xfs") {
        return 0;
    }
    return event.channel;
}

auto scope_matches_view(std::string_view scope, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (!filter.scope.empty()) {
        return true;
    }
    if (view == WkiDataView::ALL) {
        return true;
    }
    if (view == WkiDataView::VMEM) {
        return scope == "local_vmem";
    }
    bool const IS_LOCAL = is_local_data_scope(scope);
    return view == WkiDataView::LOCAL ? IS_LOCAL : !IS_LOCAL;
}

auto wki_summary_matches(const WkiSummaryRow& row, const WkiTraceFilter& filter) -> bool {
    if (!filter.scope.empty() && row.scope != filter.scope) {
        return false;
    }
    if (!filter.op.empty() && row.op != filter.op) {
        return false;
    }
    if (filter.peer.has_value() && row.peer != *filter.peer) {
        return false;
    }
    if (filter.channel.has_value() && row.channel != *filter.channel) {
        return false;
    }
    if (filter.min_us.has_value() && row.max_us < *filter.min_us) {
        return false;
    }
    return true;
}

auto view_summary_matches(const WkiSummaryRow& row, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (!scope_matches_view(row.scope, filter, view)) {
        return false;
    }
    return wki_summary_matches(row, filter);
}

auto is_default_ipc_scope(std::string_view scope) -> bool { return scope == "remote_ipc" || scope == "local_pipe"; }

auto ipc_summary_matches(const WkiSummaryRow& row, const WkiTraceFilter& filter) -> bool {
    if (filter.scope.empty() && !is_default_ipc_scope(row.scope)) {
        return false;
    }
    return wki_summary_matches(row, filter);
}

auto wki_summary_event_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (event.type != 'K') {
        return false;
    }
    if (event.phase_name != "end" && event.phase_name != "point") {
        return false;
    }
    return wki_trace_matches(event, filter);
}

auto view_trace_matches(const EventInfo& event, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (!scope_matches_view(event.scope_name, filter, view)) {
        return false;
    }
    return wki_trace_matches(event, filter);
}

auto view_summary_event_matches(const EventInfo& event, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (event.type != 'K') {
        return false;
    }
    if (event.phase_name != "end" && event.phase_name != "point") {
        return false;
    }
    return view_trace_matches(event, filter, view);
}

auto ipc_summary_event_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (filter.scope.empty() && !is_default_ipc_scope(event.scope_name)) {
        return false;
    }
    return wki_summary_event_matches(event, filter);
}

auto percentile_from_sorted(const std::vector<uint32_t>& sorted, uint64_t numerator, uint64_t denominator) -> uint64_t {
    if (sorted.empty()) {
        return 0;
    }
    if (sorted.size() == 1) {
        return sorted.front();
    }
    uint64_t const N = sorted.size() - 1;
    uint64_t const INDEX = (N * numerator) / denominator;
    return sorted.at(static_cast<std::size_t>(INDEX));
}

auto build_wki_summary_from_events(const std::vector<EventInfo>& events, const WkiTraceFilter& filter, WkiDataView view = WkiDataView::ALL)
    -> std::vector<WkiSummaryRow> {
    std::vector<WkiSummaryAccum> accums;

    auto find_accum = [&](const EventInfo& event) -> WkiSummaryAccum& {
        uint64_t const SUMMARY_PEER = summary_peer_for_event(event);
        uint64_t const SUMMARY_CHANNEL = summary_channel_for_event(event);
        for (auto& accum : accums) {
            if (accum.key.scope == event.scope_name && accum.key.op == event.op_name && accum.key.peer == SUMMARY_PEER &&
                accum.key.channel == SUMMARY_CHANNEL) {
                return accum;
            }
        }

        WkiSummaryAccum accum{};
        accum.key.scope = event.scope_name;
        accum.key.op = event.op_name;
        accum.key.peer = SUMMARY_PEER;
        accum.key.channel = SUMMARY_CHANNEL;
        accums.push_back(std::move(accum));
        return accums.back();
    };

    for (const auto& event : events) {
        if (!view_summary_event_matches(event, filter, view)) {
            continue;
        }
        auto& accum = find_accum(event);
        accum.calls++;
        if (event.status < 0) {
            accum.errors++;
        }
        accum.total_us += event.aux;
        accum.samples.push_back(static_cast<uint32_t>(std::min<uint64_t>(event.aux, std::numeric_limits<uint32_t>::max())));
    }

    std::vector<WkiSummaryRow> rows;
    rows.reserve(accums.size());
    for (auto& accum : accums) {
        if (accum.calls == 0) {
            continue;
        }
        std::ranges::sort(accum.samples);

        WkiSummaryRow row{};
        row.scope = std::move(accum.key.scope);
        row.op = std::move(accum.key.op);
        row.peer = accum.key.peer;
        row.channel = accum.key.channel;
        row.calls = accum.calls;
        row.errors = accum.errors;
        row.retries = accum.retries;
        row.samples = accum.samples.size();
        row.total_us = accum.total_us;
        row.avg_us = accum.calls != 0 ? accum.total_us / accum.calls : 0;
        row.max_us = accum.samples.empty() ? 0 : accum.samples.back();
        row.p50_us = percentile_from_sorted(accum.samples, 50, 100);
        row.p95_us = percentile_from_sorted(accum.samples, 95, 100);
        row.p99_us = percentile_from_sorted(accum.samples, 99, 100);
        row.p999_us = percentile_from_sorted(accum.samples, 999, 1000);
        row.p9999_us = percentile_from_sorted(accum.samples, 9999, 10000);
        row.p99999_us = percentile_from_sorted(accum.samples, 99999, 100000);
        rows.push_back(std::move(row));
    }

    return rows;
}

auto wki_trace_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (!filter.scope.empty() && event.scope_name != filter.scope) {
        return false;
    }
    if (!filter.op.empty() && event.op_name != filter.op) {
        return false;
    }
    if (!filter.phase.empty() && event.phase_name != filter.phase) {
        return false;
    }
    if (filter.peer.has_value() && event.peer != *filter.peer) {
        return false;
    }
    if (filter.channel.has_value() && event.channel != *filter.channel) {
        return false;
    }
    if (filter.correlation.has_value() && event.correlation != *filter.correlation) {
        return false;
    }
    if (filter.pid.has_value() && event.pid != *filter.pid) {
        return false;
    }
    if (filter.min_us.has_value() && event.aux < *filter.min_us) {
        return false;
    }
    if (filter.from_ns.has_value() && event.ts_ns < *filter.from_ns) {
        return false;
    }
    if (filter.to_ns.has_value() && event.ts_ns > *filter.to_ns) {
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

auto format_scaled_bytes(uint64_t bytes) -> std::string {
    if (bytes >= 1024ULL * 1024ULL) {
        constexpr uint64_t MIB = 1024ULL * 1024ULL;
        uint64_t whole = bytes / MIB;
        uint64_t frac = (((bytes % MIB) * 10U) + (MIB / 2U)) / MIB;
        if (frac >= 10U) {
            whole++;
            frac = 0;
        }
        return std::to_string(whole) + "." + std::to_string(frac) + "MiB";
    }
    if (bytes >= 1024ULL) {
        constexpr uint64_t KIB = 1024ULL;
        uint64_t whole = bytes / KIB;
        uint64_t frac = (((bytes % KIB) * 10U) + (KIB / 2U)) / KIB;
        if (frac >= 10U) {
            whole++;
            frac = 0;
        }
        return std::to_string(whole) + "." + std::to_string(frac) + "KiB";
    }
    return std::to_string(bytes) + "B";
}

auto format_hundredths(uint64_t scaled) -> std::string {
    return std::to_string(scaled / 100U) + "." + (scaled % 100U < 10U ? "0" : "") + std::to_string(scaled % 100U);
}

auto format_mib_per_s(uint64_t bytes, uint64_t total_us) -> std::string {
    if (bytes == 0 || total_us == 0) {
        return "0.00";
    }

    constexpr uint64_t BYTES_PER_MIB = 1024ULL * 1024ULL;
    constexpr uint64_t HUNDREDTHS_PER_UNIT = 100ULL;
    constexpr uint64_t MICROSECONDS_PER_SECOND_U64 = 1000000ULL;
    auto numerator = static_cast<unsigned __int128>(bytes) * HUNDREDTHS_PER_UNIT * MICROSECONDS_PER_SECOND_U64;
    auto denominator = static_cast<unsigned __int128>(total_us) * BYTES_PER_MIB;
    auto scaled = static_cast<uint64_t>((numerator + (denominator / 2U)) / denominator);
    return format_hundredths(scaled);
}

auto row_jitter_us(const WkiSummaryRow& row) -> uint64_t {
    if (row.p50_us != 0 && row.p99_us >= row.p50_us) {
        return row.p99_us - row.p50_us;
    }
    if (row.p95_us != 0 && row.p99_us >= row.p95_us) {
        return row.p99_us - row.p95_us;
    }
    return 0;
}

void print_ipc_memory_snapshot(const std::optional<WkiLoadedText>& loaded) {
    if (!loaded.has_value()) {
        std::println("perf: no IPC memory snapshot available from perf.data or {}", KIPCSTAT_PATH);
        return;
    }

    auto snapshot = parse_ipc_stats_section(loaded->buffer, loaded->sectioned);
    if (!snapshot.has_value()) {
        std::println("perf: no IPC memory snapshot found in {}", loaded->source);
        return;
    }

    double ring_pct = 0.0;
    if (snapshot->ring_bytes != 0) {
        ring_pct = (static_cast<double>(snapshot->ring_used) * 100.0) / static_cast<double>(snapshot->ring_bytes);
    }

    uint64_t const TOTAL_APPROX_ALLOC = snapshot->approx_alloc_bytes + snapshot->local_pipe_approx_alloc_bytes;

    std::println("=== perf ipc memory [{}] ============================================", loaded->source);
    std::println("total_approx={}  remote_approx={}  local_pipe_approx={}", format_scaled_bytes(TOTAL_APPROX_ALLOC),
                 format_scaled_bytes(snapshot->approx_alloc_bytes), format_scaled_bytes(snapshot->local_pipe_approx_alloc_bytes));
    std::println("remote: exports={} proxies={} pumps={}  ring={}/{} ({:.1f}%)", snapshot->exports, snapshot->proxies, snapshot->pump_tasks,
                 format_scaled_bytes(snapshot->ring_used), format_scaled_bytes(snapshot->ring_bytes), ring_pct);
    std::println("remote: pending={} chunks={} bytes={}  backlog={} chunks={} bytes={}  devq={} payload={}", snapshot->pending_deliveries,
                 snapshot->pending_chunks, format_scaled_bytes(snapshot->pending_bytes), snapshot->export_backlogs,
                 snapshot->export_backlog_chunks, format_scaled_bytes(snapshot->export_backlog_bytes), snapshot->dev_op_queue,
                 format_scaled_bytes(snapshot->dev_op_payload_bytes));
    std::println("remote: blocked_readers={} poll_waiters={} export_flush_queue={}", snapshot->blocked_readers, snapshot->poll_waiters,
                 snapshot->export_flush_queue);
    std::println("local_pipe: active={} created={} peak={}  capacity={} peak_capacity={} buffered={}", snapshot->local_pipe_active,
                 snapshot->local_pipe_created, snapshot->local_pipe_peak, format_scaled_bytes(snapshot->local_pipe_capacity),
                 format_scaled_bytes(snapshot->local_pipe_peak_capacity), format_scaled_bytes(snapshot->local_pipe_buffered));
    std::println("local_pipe: readers={} writers={} poll_waiters={} direct={} read_closed={} write_closed={}",
                 snapshot->local_pipe_reader_waiters, snapshot->local_pipe_writer_waiters, snapshot->local_pipe_poll_waiters,
                 snapshot->local_pipe_direct_writes, snapshot->local_pipe_read_closed, snapshot->local_pipe_write_closed);
    std::println("");
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
        if (!row.first_ts_ns.has_value() || event.ts_ns < *row.first_ts_ns) {
            row.first_ts_ns = event.ts_ns;
        }
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
    auto const TIME_DISPLAY = make_time_display(display_options, event_loaded->buffer, event_loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    std::println("{:<{}}  {:<18}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
                 time_column_header(TIME_DISPLAY), TIME_WIDTH, "PEER", "CORR", "TOTAL(us)", "HANDLE(us)", "LOAD(us)", "SETUP(us)",
                 "QUEUE(us)", "RUN(us)", "READY(us)", "HOLD(us)", "WAIT(us)", "RESULT");
    std::println("{:->{}}  {:->18}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->12}", "",
                 TIME_WIDTH, "", "", "", "", "", "", "", "", "", "", "", "");
    for (int i = 0; std::cmp_less(i, launch_rows.size()) && i < limit; ++i) {
        const auto& row = launch_rows.at(static_cast<std::size_t>(i));
        std::optional<uint32_t> setup_us;
        if (row.handle_submit_us.has_value() && row.load_elf_us.has_value() && *row.handle_submit_us >= *row.load_elf_us) {
            setup_us = *row.handle_submit_us - *row.load_elf_us;
        }
        std::println("{:<{}}  {:<18}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
                     format_optional_event_time(row.first_ts_ns, TIME_DISPLAY), TIME_WIDTH, wki_peer_label(row.peer, peer_resolver),
                     row.correlation, format_optional_us(row.submit_total_us), format_optional_us(row.handle_submit_us),
                     format_optional_us(row.load_elf_us), format_optional_us(setup_us), format_optional_us(row.defer_wait_us),
                     format_optional_us(row.task_runtime_us), format_optional_us(row.proxy_ready_wait_us),
                     format_optional_us(row.complete_hold_us), format_optional_us(row.complete_wait_us), format_wki_launch_result(row));
    }
}

void cmd_ipc_report(int limit, WkiTraceFilter filter, const WkiDisplayOptions& display_options) {
    if (limit < 1) {
        limit = DEFAULT_WKI_TAIL_ROWS;
    }

    auto ipc_stats_loaded = load_ipc_stats_text();
    print_ipc_memory_snapshot(ipc_stats_loaded);

    std::string summary_source;
    std::vector<WkiSummaryRow> rows;
    auto summary_loaded = load_wki_summary_text();
    if (summary_loaded.has_value()) {
        summary_source = summary_loaded->source;
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !ipc_summary_matches(row, filter); });
    }

    auto event_loaded = load_wki_event_text();
    if (rows.empty() && event_loaded.has_value()) {
        summary_source = event_loaded->source;
        rows = build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !ipc_summary_matches(row, filter); });
    }

    if (rows.empty()) {
        std::println("perf: no IPC summary rows matched the requested filters (default scopes: remote_ipc,local_pipe)");
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

    WkiPeerResolver summary_peer_resolver{};
    if (summary_loaded.has_value()) {
        summary_peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
    } else if (event_loaded.has_value()) {
        summary_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    } else {
        summary_peer_resolver = make_wki_peer_resolver({}, false, display_options);
    }

    std::println("=== perf ipc-report [{}] ============================================", summary_source);
    std::println("{:<12}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER",
                 "CH", "CALLS", "ERR", "BYTES", "AVG(us)", "P50(us)", "P99(us)", "JIT(us)", "MAX(us)", "MiB/s");
    std::println("{:->12}  {:->16}  {:->18}  {:->4}  {:->7}  {:->6}  {:->10}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "",
                 "", "", "", "", "", "", "", "", "", "");
    for (int i = 0; std::cmp_less(i, rows.size()) && i < limit; ++i) {
        const auto& row = rows.at(static_cast<std::size_t>(i));
        std::println("{:<12}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                     wki_peer_label(row.peer, summary_peer_resolver), row.channel, row.calls, row.errors, format_scaled_bytes(row.bytes),
                     row.avg_us, row.p50_us, row.p99_us, row_jitter_us(row), row.max_us, format_mib_per_s(row.bytes, row.total_us));
    }

    if (!event_loaded.has_value()) {
        return;
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    std::vector<EventInfo> slow_events;
    for (const auto& event : events) {
        if (ipc_summary_event_matches(event, filter) && event.aux > 0) {
            slow_events.push_back(event);
        }
    }
    if (slow_events.empty()) {
        return;
    }
    std::ranges::sort(slow_events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.aux > rhs.aux; });

    auto event_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    auto const TIME_DISPLAY = make_time_display(display_options, event_loaded->buffer, event_loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    std::println("");
    std::println("=== slowest IPC events [{}] ========================================", event_loaded->source);
    std::println("{:<{}}  {:<12}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", time_column_header(TIME_DISPLAY), TIME_WIDTH,
                 "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "DETAILS");
    std::println("{:->{}}  {:->12}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->10}  {:->24}", "", TIME_WIDTH, "", "", "", "", "",
                 "", "", "", "");
    for (int i = 0; std::cmp_less(i, slow_events.size()) && i < limit; ++i) {
        const auto& event = slow_events.at(static_cast<std::size_t>(i));
        std::println("{:<{}}  {:<12}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", format_event_time(event.ts_ns, TIME_DISPLAY),
                     TIME_WIDTH, event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, event_peer_resolver),
                     event.channel, event.aux, event.status, event.correlation, display_callsite(event.callsite));
    }
}

void cmd_wki_report(const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view) {
    std::string source;
    std::vector<WkiSummaryRow> rows;
    std::optional<WkiLoadedText> event_loaded;
    std::optional<WkiLoadedText> summary_loaded;
    bool const PREFER_SAVED_SUMMARY =
        view == WkiDataView::VMEM || filter.scope == "local_vmem" || filter.scope == "local_loader" || filter.scope == "local_xfs";

    auto load_summary_rows = [&]() {
        summary_loaded = load_wki_summary_text();
        if (!summary_loaded.has_value()) {
            return;
        }
        source = summary_loaded->source;
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !view_summary_matches(row, filter, view); });
    };

    if (PREFER_SAVED_SUMMARY) {
        load_summary_rows();
    }
    if (rows.empty()) {
        event_loaded = load_wki_event_text();
        if (event_loaded.has_value()) {
            source = event_loaded->source;
            rows = build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter, view);
        }
    }
    if (rows.empty() && !PREFER_SAVED_SUMMARY) {
        load_summary_rows();
        if (!summary_loaded.has_value()) {
            std::println("perf: no {} summary data", wki_view_name(view));
            return;
        }
    }

    if (rows.empty()) {
        std::println("perf: no {} summary rows matched the requested filters", wki_view_name(view));
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

    WkiPeerResolver peer_resolver{};
    if (event_loaded.has_value()) {
        peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    } else if (summary_loaded.has_value()) {
        peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
    } else {
        peer_resolver = make_wki_peer_resolver({}, false, display_options);
    }
    std::println("=== perf {}-report [{}] ============================================", wki_view_name(view), source);
    std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>7}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER", "CH",
                 "CALLS", "ERR", "RETRY", "BYTES", "AVG(us)", "P99(us)", "P999(us)", "MAX(us)");
    std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->7}  {:->6}  {:->7}  {:->10}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "",
                 "", "", "", "", "", "", "");
    for (const auto& row : rows) {
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>7}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                     wki_peer_label(row.peer, peer_resolver), row.channel, row.calls, row.errors, row.retries,
                     format_scaled_bytes(row.bytes), row.avg_us, row.p99_us, row.p999_us, row.max_us);
    }
}

void cmd_vmem_report(WkiTraceFilter filter) {
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }

    std::string source;
    std::vector<WkiSummaryRow> rows;

    auto loaded = load_wki_summary_text();
    if (loaded.has_value()) {
        source = loaded->source;
        rows = parse_wki_summary_section(loaded->buffer, loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !view_summary_matches(row, filter, WkiDataView::VMEM); });
    }

    if (rows.empty()) {
        auto event_loaded = load_wki_event_text();
        if (event_loaded.has_value()) {
            source = event_loaded->source;
            rows =
                build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter, WkiDataView::VMEM);
        }
    }

    if (rows.empty()) {
        std::println("perf: no vmem rows matched the requested filters");
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

    std::println("=== perf vmem-report [{}] ===========================================", source);
    std::println("{:<18}  {:>7}  {:>6}  {:>11}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", "OP", "CALLS", "ERR", "BYTES", "PAGES",
                 "AVG(us)", "P50(us)", "P99(us)", "P999(us)", "MAX(us)");
    std::println("{:->18}  {:->7}  {:->6}  {:->11}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "", "",
                 "");
    for (const auto& row : rows) {
        uint64_t const PAGES = row.bytes / PERF_PAGE_SIZE;
        std::println("{:<18}  {:>7}  {:>6}  {:>11}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", row.op, row.calls, row.errors,
                     format_scaled_bytes(row.bytes), PAGES, row.avg_us, row.p50_us, row.p99_us, row.p999_us, row.max_us);
    }
}

void cmd_wki_tail(int limit, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view) {
    if (limit < 1) {
        limit = DEFAULT_WKI_TAIL_ROWS;
    }

    auto event_loaded = load_wki_event_text();
    std::vector<WkiSummaryRow> rows;
    if (event_loaded.has_value()) {
        rows = build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter, view);
    }

    auto summary_loaded = load_wki_summary_text();
    if (rows.empty() && summary_loaded.has_value()) {
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !view_summary_matches(row, filter, view); });
    }

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
        std::string summary_source = "?";
        WkiPeerResolver summary_peer_resolver = make_wki_peer_resolver({}, false, display_options);
        if (event_loaded.has_value()) {
            summary_source = event_loaded->source;
            summary_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
        } else if (summary_loaded.has_value()) {
            summary_source = summary_loaded->source;
            summary_peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
        }
        std::println("=== perf {}-tail summary [{}] ======================================", wki_view_name(view), summary_source);
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER", "CH", "CALLS", "P999(us)",
                     "P9999(us)", "P99999(us)", "MAX(us)");
        std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->7}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "", "");
        for (int i = 0; std::cmp_less(i, rows.size()) && i < limit; ++i) {
            const auto& row = rows.at(static_cast<std::size_t>(i));
            std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                         wki_peer_label(row.peer, summary_peer_resolver), row.channel, row.calls, row.p999_us, row.p9999_us, row.p99999_us,
                         row.max_us);
        }
    }

    if (!event_loaded.has_value()) {
        if (!rows.empty()) {
            std::println("");
            std::println("perf: no raw {} events available in perf.data or /proc/kperf", wki_view_name(view));
            return;
        }
        std::println("perf: no {} summary or raw trace data", wki_view_name(view));
        return;
    }

    if (rows.empty()) {
        std::println("perf: no {} summary data; showing raw-event tail from {}", wki_view_name(view), event_loaded->source);
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    auto event_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    auto const TIME_DISPLAY = make_time_display(display_options, event_loaded->buffer, event_loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    if (events.empty()) {
        if (!rows.empty()) {
            std::println("");
            std::println("perf: no raw {} events found in {}", wki_view_name(view), event_loaded->source);
            return;
        }
        std::println("perf: no raw {} events found in {}", wki_view_name(view), event_loaded->source);
        return;
    }

    std::vector<EventInfo> slow_events;
    for (const auto& event : events) {
        if (view_summary_event_matches(event, filter, view) && event.aux > 0) {
            slow_events.push_back(event);
        }
    }
    std::ranges::sort(slow_events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.aux > rhs.aux; });

    std::println("");
    std::println("=== slowest {} events [{}] ========================================", wki_view_name(view), event_loaded->source);
    std::println("{:<{}}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", time_column_header(TIME_DISPLAY), TIME_WIDTH,
                 "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "DETAILS");
    std::println("{:->{}}  {:->14}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->10}  {:->24}", "", TIME_WIDTH, "", "", "", "", "",
                 "", "", "", "");
    for (int i = 0; std::cmp_less(i, slow_events.size()) && i < limit; ++i) {
        const auto& event = slow_events.at(static_cast<std::size_t>(i));
        std::println("{:<{}}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", format_event_time(event.ts_ns, TIME_DISPLAY),
                     TIME_WIDTH, event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, event_peer_resolver),
                     event.channel, event.aux, event.status, event.correlation, format_wki_trace_detail(event));
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
        if (!view_trace_matches(event, filter, view)) {
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
    std::println("=== largest {} inter-call gaps ====================================", wki_view_name(view));
    std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>12}  {:<{}}  {:<{}}", "SCOPE", "OP", "PEER", "CH", "GAP(us)",
                 time_column_header(TIME_DISPLAY), TIME_WIDTH, "NEXT", TIME_WIDTH);
    std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->12}  {:->{}}  {:->{}}", "", "", "", "", "", "", TIME_WIDTH, "", TIME_WIDTH);
    for (int i = 0; std::cmp_less(i, gaps.size()) && i < limit; ++i) {
        const auto& gap = gaps.at(static_cast<std::size_t>(i));
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>12}  {:<{}}  {:<{}}", gap.scope, gap.op,
                     wki_peer_label(gap.peer, event_peer_resolver), gap.channel,
                     gap.gap_ns / static_cast<uint64_t>(NANOSECONDS_PER_MICROSECOND), format_event_time(gap.prev_ts_ns, TIME_DISPLAY),
                     TIME_WIDTH, format_event_time(gap.next_ts_ns, TIME_DISPLAY), TIME_WIDTH);
    }
}

void cmd_wki_trace(int max_events, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view) {
    if (max_events < 1) {
        max_events = DEFAULT_WKI_TRACE_EVENTS;
    }

    auto loaded = load_wki_event_text();
    if (!loaded.has_value()) {
        auto summary_loaded = load_wki_summary_text();
        if (summary_loaded.has_value() && !parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned).empty()) {
            std::println("perf: {} summary data exists in {}, but no raw {} events were saved", wki_view_name(view), summary_loaded->source,
                         wki_view_name(view));
        } else {
            std::println("perf: no {} trace data", wki_view_name(view));
        }
        return;
    }

    auto proc_map = loaded->sectioned ? parse_proc_map_section(loaded->buffer) : current_proc_map();
    auto events = collect_wki_events(loaded->buffer, loaded->sectioned);
    auto peer_resolver = make_wki_peer_resolver(loaded->buffer, loaded->sectioned, display_options);
    auto const TIME_DISPLAY = make_time_display(display_options, loaded->buffer, loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    if (events.empty()) {
        auto summary_loaded = load_wki_summary_text();
        if (summary_loaded.has_value() && !parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned).empty()) {
            std::println("perf: {} summary data exists in {}, but no raw {} events were saved", wki_view_name(view), summary_loaded->source,
                         wki_view_name(view));
        } else {
            std::println("perf: no {} trace data", wki_view_name(view));
        }
        return;
    }

    std::println("=== perf {}-trace [{}] ============================================", wki_view_name(view), loaded->source);
    std::println("{:<{}}  {:>3}  {:<24}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>8}  {}", time_column_header(TIME_DISPLAY),
                 TIME_WIDTH, "CPU", "PID", "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "CALLSITE/DETAILS");
    std::println("{:->{}}  {:->3}  {:->24}  {:->14}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->8}  {:->28}", "", TIME_WIDTH, "",
                 "", "", "", "", "", "", "", "", "", "");

    int printed = 0;
    for (const auto& event : events) {
        if (!view_trace_matches(event, filter, view)) {
            continue;
        }
        std::println("{:<{}}  {:>3}  {:<24}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>8}  {}",
                     format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                     event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, peer_resolver), event.channel, event.aux,
                     event.status, event.correlation, format_wki_trace_detail(event));
        printed++;
        if (printed >= max_events) {
            break;
        }
    }
}

auto parse_time_display_format(std::string_view value, TimeDisplayFormat& out) -> bool {
    if (value == "boot" || value == "boot-ns" || value == "since-boot" || value == "monotonic" || value == "ns") {
        out = TimeDisplayFormat::BOOT_NS;
        return true;
    }
    if (value == "unix" || value == "unix-ns" || value == "epoch-ns" || value == "realtime-ns") {
        out = TimeDisplayFormat::UNIX_NS;
        return true;
    }
    if (value == "iso" || value == "iso8601" || value == "realtime-iso") {
        out = TimeDisplayFormat::ISO_REALTIME;
        return true;
    }
    return false;
}

auto parse_display_arg(std::string_view arg, WkiDisplayOptions& display_options) -> bool {
    constexpr std::string_view TIME_PREFIX = "--time=";
    constexpr std::string_view TIMECODE_PREFIX = "--timecode=";

    if (arg == "--peer-ids") {
        display_options.show_peer_ids = true;
        return true;
    }
    if (arg.starts_with(TIME_PREFIX)) {
        return parse_time_display_format(arg.substr(TIME_PREFIX.size()), display_options.time_format);
    }
    if (arg.starts_with(TIMECODE_PREFIX)) {
        return parse_time_display_format(arg.substr(TIMECODE_PREFIX.size()), display_options.time_format);
    }
    if (arg == "--boot-time" || arg == "--time-boot") {
        display_options.time_format = TimeDisplayFormat::BOOT_NS;
        return true;
    }
    if (arg == "--unix-time" || arg == "--time-unix-ns") {
        display_options.time_format = TimeDisplayFormat::UNIX_NS;
        return true;
    }
    if (arg == "--iso-time" || arg == "--time-iso") {
        display_options.time_format = TimeDisplayFormat::ISO_REALTIME;
        return true;
    }
    return false;
}

auto parse_wki_filter_arg(std::string_view arg, WkiTraceFilter& filter, WkiDisplayOptions& display_options) -> bool {
    if (parse_display_arg(arg, display_options)) {
        return true;
    }
    if (arg.starts_with("--scope=")) {
        filter.scope = std::string(arg.substr(8));
        return true;
    }
    if (arg.starts_with("--op=")) {
        filter.op = std::string(arg.substr(5));
        return true;
    }
    if (arg.starts_with("--phase=")) {
        filter.phase = std::string(arg.substr(8));
        return true;
    }
    if (arg.starts_with("--peer=")) {
        filter.peer = parse_u64(arg.substr(7), 0);
        return true;
    }
    if (arg.starts_with("--channel=")) {
        filter.channel = parse_u64(arg.substr(10), 0);
        return true;
    }
    if (arg.starts_with("--corr=")) {
        filter.correlation = parse_u64(arg.substr(7), 0);
        return true;
    }
    if (arg.starts_with("--pid=")) {
        filter.pid = parse_u64(arg.substr(6), 0);
        return true;
    }
    if (arg.starts_with("--min-us=")) {
        filter.min_us = parse_u64(arg.substr(9), 0);
        return true;
    }
    if (arg.starts_with("--from-ns=")) {
        filter.from_ns = parse_u64(arg.substr(10), 0);
        return true;
    }
    if (arg.starts_with("--to-ns=")) {
        filter.to_ns = parse_u64(arg.substr(8), 0);
        return true;
    }
    return false;
}

// Resolve a command name to a full path by searching PATH.
// If cmd already contains '/', returns it as-is (if accessible).
// Returns empty string if not found.
auto resolve_command(const char* cmd) -> std::string {
    if (cmd == nullptr || cmd[0] == '\0') {
        return {};
    }

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

    std::string_view const PATH_SV(path_env);
    std::size_t start = 0;
    while (start <= PATH_SV.size()) {
        auto colon = PATH_SV.find(':', start);
        auto dir = PATH_SV.substr(start, colon == std::string_view::npos ? std::string_view::npos : colon - start);
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

        if (colon == std::string_view::npos) {
            break;
        }
        start = colon + 1;
    }
    return {};
}

// Read the shebang line from a script file. Returns the interpreter path,
// or empty string if the file is not a script.
auto read_shebang(const char* path) -> std::string {
    int const FD = open(path, O_RDONLY);
    if (FD < 0) {
        return {};
    }
    std::array<char, 256> hdr{};
    ssize_t const N = read(FD, hdr.data(), hdr.size() - 1);
    close(FD);
    if (N < 3 || hdr.at(0) != '#' || hdr.at(1) != '!') {
        return {};
    }
    hdr.at(static_cast<size_t>(N)) = '\0';
    // Find end of first line
    char* nl = std::strchr(hdr.data() + 2, '\n');
    if (nl != nullptr) {
        *nl = '\0';
    }
    // Skip whitespace after #!
    const char* p = hdr.data() + 2;
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
    WkiDisplayOptions display_options{};
    std::vector<char*> cmd_argv;
    for (int i = 0; i < argc; i++) {
        std::string_view const ARG(argv[i]);
        if (ARG.starts_with("--filter=")) {
            filter = argv[i] + 9;
        } else if (parse_display_arg(ARG, display_options)) {
            continue;
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

    char* const* exec_argv = new_argv.data();
    const char* exec_path = exec_argv[0];

    auto before = collect_stats();
    int64_t const START_MS = now_ms();
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

    ker::process::setpgid(child_pid, child_pid);
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
        write_section_timebase(data_fd.get());
        write_all(data_fd.get(), SECTION_EVENTS);
    }

    int32_t status = 0;
    int64_t last_drain_ms = now_ms();
    ssize_t total_event_bytes = 0;
    bool command_exited = false;

    for (;;) {
        for (;;) {
            int64_t const REAPED = waitpid_nohang(-1, &status);
            if (REAPED <= 0) {
                break;
            }
            if (REAPED == child_pid) {
                command_exited = true;
            }
        }

        bool any_alive = false;
        for_each_process_stat([&](const StatInfo& stat, std::string_view) {
            if (std::cmp_equal(stat.pgid, target_pgid)) {
                upsert_tracked(stat);
                if (stat.state != EXITED_STATE) {
                    any_alive = true;
                }
            }
        });

        if (command_exited || !any_alive) {
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

    while (waitpid_nohang(-1, &status) > 0) {
    }

    int64_t elapsed_ms = now_ms() - START_MS;
    elapsed_ms = std::max<int64_t>(elapsed_ms, 1);

    set_recording_enabled(false);
    auto after = collect_stats();
    std::vector<CpuRow> rows;

    for (const auto& proc : tracked) {
        uint64_t const TICKS_AFTER = proc.last_utime + proc.last_stime;
        uint64_t ticks_before = 0;
        for (const auto& prior : before) {
            if (prior.pid == proc.pid) {
                ticks_before = prior.utime + prior.stime;
                break;
            }
        }

        uint64_t const DELTA = TICKS_AFTER >= ticks_before ? TICKS_AFTER - ticks_before : TICKS_AFTER;
        double const CPU = static_cast<double>(DELTA) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);
        if (CPU < MIN_CPU_PCT) {
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
            .cpu_pct = CPU,
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

            uint64_t const DELTA = (current.utime + current.stime) - (prior.utime + prior.stime);
            double const CPU = static_cast<double>(DELTA) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);
            if (CPU >= MIN_CPU_PCT) {
                rows.push_back(CpuRow{
                    .pid = current.pid,
                    .comm = current.comm,
                    .state = current.state,
                    .cpu_pct = CPU,
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
        ssize_t const SUMMARY_BYTES = write_section_wki_summary(data_fd.get());
        ssize_t const IPC_BYTES = write_section_ipc_stats(data_fd.get());
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

        if (total_event_bytes <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0) {
            std::println("perf: ring buffer empty - PROC_MAP saved, no events");
        } else {
            std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes)", PERF_DATA_FILE, total_event_bytes,
                         SUMMARY_BYTES, IPC_BYTES);
        }
    } else {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
    }

    cmd_sched(DEFAULT_MAX_EVENTS, display_options);
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

    std::string_view const VIEW(*buffer);
    std::size_t const HEADER_POS = VIEW.find(SECTION_PROC_MAP);
    if (HEADER_POS == std::string_view::npos) {
        std::println("perf: perf.data has no PROC_MAP section");
        return;
    }

    std::size_t pos = VIEW.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return;
    }
    ++pos;

    std::println("=== perf show-map [{}] ==============================================", PERF_DATA_FILE);
    std::println("{:>6}  {:<20}  {}", "PID", "COMM", "CMDLINE");
    std::println("{:->6}  {:->20}  {:->40}", "", "", "");

    while (pos < VIEW.size()) {
        std::string_view const LINE = next_line(VIEW, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }
        if (LINE.empty()) {
            continue;
        }

        std::size_t const PID_POS = LINE.find("pid=");
        std::size_t const COMM_POS = LINE.find(COMM_FIELD_PREFIX);
        std::size_t const CMD_POS = LINE.find(CMD_FIELD_PREFIX);
        if (PID_POS == std::string_view::npos || COMM_POS == std::string_view::npos) {
            continue;
        }

        std::string_view const PID_TEXT = LINE.substr(PID_POS + PID_PREFIX_SIZE, COMM_POS - (PID_POS + PID_PREFIX_SIZE));
        std::string_view comm_text = CMD_POS == std::string_view::npos
                                         ? LINE.substr(COMM_POS + COMM_PREFIX_SIZE)
                                         : LINE.substr(COMM_POS + COMM_PREFIX_SIZE, CMD_POS - (COMM_POS + COMM_PREFIX_SIZE));
        std::string_view cmd_text = CMD_POS == std::string_view::npos ? std::string_view{} : LINE.substr(CMD_POS + CMD_PREFIX_SIZE);

        std::println("{:>6}  {:<20}  {}", parse_u64(PID_TEXT), comm_text, cmd_text);
    }
}

using CommandHandler = int (*)(int argc, char** argv);

struct CommandSpec {
    std::string_view name;
    std::string_view args;
    std::string_view summary;
    CommandHandler handler;
};

int run_stat_command(int argc, char** argv);
int run_record_command(int argc, char** argv);
int run_report_command(int argc, char** argv);
int run_cpustat_command(int argc, char** argv);
int run_contstat_command(int argc, char** argv);
int run_ipc_report_command(int argc, char** argv);
int run_wki_report_command(int argc, char** argv);
int run_local_report_command(int argc, char** argv);
int run_all_report_command(int argc, char** argv);
int run_vmem_report_command(int argc, char** argv);
int run_wki_launch_command(int argc, char** argv);
int run_wki_tail_command(int argc, char** argv);
int run_local_tail_command(int argc, char** argv);
int run_all_tail_command(int argc, char** argv);
int run_vmem_tail_command(int argc, char** argv);
int run_wki_trace_command(int argc, char** argv);
int run_local_trace_command(int argc, char** argv);
int run_all_trace_command(int argc, char** argv);
int run_vmem_trace_command(int argc, char** argv);
int run_run_command(int argc, char** argv);
int run_show_map_command(int argc, char** argv);
int run_help_command(int argc, char** argv);

constexpr std::array<CommandSpec, 23> COMMANDS = {{
    {.name = "stat", .args = "[ms=1000]", .summary = "CPU% per process over a sampling window", .handler = run_stat_command},
    {.name = "record",
     .args = "[ms=1000] [--filter=<filters>]",
     .summary = "Record kernel perf events to perf.data",
     .handler = run_record_command},
    {.name = "report",
     .args = "[n=2000] [--time=FMT] [--peer-ids]",
     .summary = "Display events from perf.data or live /proc/kperf",
     .handler = run_report_command},
    {.name = "sched",
     .args = "[n=2000] [--time=FMT] [--peer-ids]",
     .summary = "Alias for report with scheduler hotspot tables",
     .handler = run_report_command},
    {.name = "cpustat", .args = "", .summary = "Per-CPU aggregate scheduler statistics", .handler = run_cpustat_command},
    {.name = "contstat", .args = "", .summary = "Per-subsystem container statistics", .handler = run_contstat_command},
    {.name = "ipc-report",
     .args = "[n=15] [filters]",
     .summary = "Remote IPC and local pipe latency, jitter, throughput, memory, and queue pressure",
     .handler = run_ipc_report_command},
    {.name = "wki-report", .args = "[filters]", .summary = "Remote/WKI summary statistics", .handler = run_wki_report_command},
    {.name = "local-report", .args = "[filters]", .summary = "Local pipe/process summary statistics", .handler = run_local_report_command},
    {.name = "all-report", .args = "[filters]", .summary = "Combined WKI and local summary statistics", .handler = run_all_report_command},
    {.name = "vmem-report",
     .args = "[filters]",
     .summary = "Local vmem mmap/COW/cache summary statistics",
     .handler = run_vmem_report_command},
    {.name = "wki-launch",
     .args = "[n=20] [--time=FMT] [--peer-ids]",
     .summary = "Remote launch stage timings",
     .handler = run_wki_launch_command},
    {.name = "wki-tail",
     .args = "[n=15] [filters]",
     .summary = "Tail-latency WKI summary and slow remote events",
     .handler = run_wki_tail_command},
    {.name = "local-tail",
     .args = "[n=15] [filters]",
     .summary = "Tail-latency local summary and slow local events",
     .handler = run_local_tail_command},
    {.name = "all-tail",
     .args = "[n=15] [filters]",
     .summary = "Tail-latency combined WKI and local events",
     .handler = run_all_tail_command},
    {.name = "vmem-tail", .args = "[n=15] [filters]", .summary = "Tail-latency local vmem events", .handler = run_vmem_tail_command},
    {.name = "wki-trace", .args = "[n=200] [filters]", .summary = "Raw remote/WKI trace events", .handler = run_wki_trace_command},
    {.name = "local-trace", .args = "[n=200] [filters]", .summary = "Raw local trace events", .handler = run_local_trace_command},
    {.name = "all-trace",
     .args = "[n=200] [filters]",
     .summary = "Raw combined WKI and local trace events",
     .handler = run_all_trace_command},
    {.name = "vmem-trace", .args = "[n=200] [filters]", .summary = "Raw local vmem trace events", .handler = run_vmem_trace_command},
    {.name = "run",
     .args = "[--filter=<filters>] [--time=FMT] <cmd> [args]",
     .summary = "Trace a command and descendants",
     .handler = run_run_command},
    {.name = "show-map", .args = "", .summary = "Show PID to command map from perf.data", .handler = run_show_map_command},
    {.name = "help", .args = "[filters]", .summary = "Show this screen or detailed filter help", .handler = run_help_command},
}};

struct FilterHelp {
    std::string_view name;
    std::string_view summary;
};

constexpr std::array<FilterHelp, 15> RECORD_FILTER_HELP = {{
    {.name = "sample", .summary = "timer CPU samples"},
    {.name = "switch", .summary = "context-switch events"},
    {.name = "wake", .summary = "task wake events"},
    {.name = "sleep", .summary = "task sleep/block events"},
    {.name = "container", .summary = "container and data-structure events"},
    {.name = "wki", .summary = "all WKI transport/RPC/IPC events"},
    {.name = "wki_launch", .summary = "remote-compute launch events only"},
    {.name = "local_pipe", .summary = "local pipe events"},
    {.name = "local_proc", .summary = "local fork/exec/process events"},
    {.name = "vmem", .summary = "local vmem mmap/cache/COW events"},
    {.name = "loader", .summary = "local ELF loader segment/perms events"},
    {.name = "xfs", .summary = "local XFS/buffer-cache/inode events"},
    {.name = "irq", .summary = "slow local interrupt-handler events"},
    {.name = "local", .summary = "all local pipe/process/vmem/loader/XFS/IRQ events"},
    {.name = "all", .summary = "all event classes"},
}};

constexpr std::array<FilterHelp, 12> WKI_FILTER_HELP = {{
    {.name = "--scope=NAME", .summary = "scope name such as remote_ipc, local_pipe, local_vmem, local_xfs, local_irq"},
    {.name = "--op=NAME", .summary = "operation name such as write_io, inode_fetch, buf_disk_write, execve, read"},
    {.name = "--phase=NAME", .summary = "begin, end, or point"},
    {.name = "--peer=N", .summary = "numeric WKI peer id"},
    {.name = "--channel=N", .summary = "WKI channel id"},
    {.name = "--corr=N", .summary = "trace correlation id"},
    {.name = "--pid=N", .summary = "subject process id"},
    {.name = "--min-us=N", .summary = "minimum auxiliary latency in microseconds"},
    {.name = "--from-ns=N", .summary = "minimum event timestamp"},
    {.name = "--to-ns=N", .summary = "maximum event timestamp"},
    {.name = "--time=FMT", .summary = "timestamp display: boot (default), unix-ns, or iso"},
    {.name = "--peer-ids", .summary = "show numeric peer ids instead of hostnames"},
}};

auto joined_record_filter_names() -> std::string {
    std::string out;
    for (const auto& filter : RECORD_FILTER_HELP) {
        if (!out.empty()) {
            out += ',';
        }
        out += filter.name;
    }
    return out;
}

void print_filters() {
    std::println("Event filters for --filter=<list>:");
    for (const auto& filter : RECORD_FILTER_HELP) {
        std::println("  {:<12} {}", filter.name, filter.summary);
    }
    std::println("");
    std::println("Trace filter and display options:");
    for (const auto& filter : WKI_FILTER_HELP) {
        std::println("  {:<14} {}", filter.name, filter.summary);
    }
}

void usage() {
    std::println("Usage: perf <command> [args]");
    std::println("Commands:");
    for (const auto& command : COMMANDS) {
        std::println("  {:<12} {:<34} {}", command.name, command.args, command.summary);
    }
    std::println("");
    std::println("Available --filter names: {}", joined_record_filter_names());
    std::println("Use 'perf help filters' for filter and display details.");
}

int run_stat_command(int argc, char** argv) {
    int const MS = argc >= 3 ? static_cast<int>(strtol(argv[2], nullptr, PARSE_BASE_DECIMAL)) : DEFAULT_SAMPLE_MS;
    cmd_stat(MS);
    return 0;
}

int run_record_command(int argc, char** argv) {
    int ms = DEFAULT_SAMPLE_MS;
    const char* filter = nullptr;
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (ARG.starts_with("--filter=")) {
            filter = argv[i] + 9;
        } else {
            ms = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        }
    }
    cmd_record(ms, filter);
    return 0;
}

int run_report_command(int argc, char** argv) {
    int max_events = DEFAULT_MAX_EVENTS;
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_display_arg(ARG, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_sched(max_events, display_options);
    return 0;
}

int run_cpustat_command(int /*argc*/, char** /*argv*/) {
    cmd_cpustat();
    return 0;
}

int run_contstat_command(int /*argc*/, char** /*argv*/) {
    cmd_contstat();
    return 0;
}

int run_ipc_report_command(int argc, char** argv) {
    int rows = DEFAULT_WKI_TAIL_ROWS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf ipc-report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_ipc_report(rows, filter, display_options);
    return 0;
}

int run_view_report_command(int argc, char** argv, WkiDataView view) {
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        if (!parse_wki_filter_arg(argv[i], filter, display_options)) {
            std::println("perf {}-report: unrecognized argument '{}'", wki_view_name(view), argv[i]);
            return 1;
        }
    }
    cmd_wki_report(filter, display_options, view);
    return 0;
}

int run_wki_report_command(int argc, char** argv) { return run_view_report_command(argc, argv, WkiDataView::WKI); }

int run_local_report_command(int argc, char** argv) { return run_view_report_command(argc, argv, WkiDataView::LOCAL); }

int run_all_report_command(int argc, char** argv) { return run_view_report_command(argc, argv, WkiDataView::ALL); }

int run_vmem_report_command(int argc, char** argv) {
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        if (!parse_wki_filter_arg(argv[i], filter, display_options)) {
            std::println("perf vmem-report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }
    cmd_vmem_report(filter);
    return 0;
}

int run_wki_launch_command(int argc, char** argv) {
    int rows = DEFAULT_WKI_LAUNCH_ROWS;
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_display_arg(ARG, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf wki-launch: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_wki_launch(rows, display_options);
    return 0;
}

int run_view_tail_command(int argc, char** argv, WkiDataView view) {
    int rows = DEFAULT_WKI_TAIL_ROWS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf {}-tail: unrecognized argument '{}'", wki_view_name(view), argv[i]);
            return 1;
        }
    }
    cmd_wki_tail(rows, filter, display_options, view);
    return 0;
}

int run_wki_tail_command(int argc, char** argv) { return run_view_tail_command(argc, argv, WkiDataView::WKI); }

int run_local_tail_command(int argc, char** argv) { return run_view_tail_command(argc, argv, WkiDataView::LOCAL); }

int run_all_tail_command(int argc, char** argv) { return run_view_tail_command(argc, argv, WkiDataView::ALL); }

int run_vmem_tail_command(int argc, char** argv) {
    int rows = DEFAULT_WKI_TAIL_ROWS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf vmem-tail: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }
    cmd_wki_tail(rows, filter, display_options, WkiDataView::VMEM);
    return 0;
}

int run_view_trace_command(int argc, char** argv, WkiDataView view) {
    int max_events = DEFAULT_WKI_TRACE_EVENTS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf {}-trace: unrecognized argument '{}'", wki_view_name(view), argv[i]);
            return 1;
        }
    }
    cmd_wki_trace(max_events, filter, display_options, view);
    return 0;
}

int run_wki_trace_command(int argc, char** argv) { return run_view_trace_command(argc, argv, WkiDataView::WKI); }

int run_local_trace_command(int argc, char** argv) { return run_view_trace_command(argc, argv, WkiDataView::LOCAL); }

int run_all_trace_command(int argc, char** argv) { return run_view_trace_command(argc, argv, WkiDataView::ALL); }

int run_vmem_trace_command(int argc, char** argv) {
    int max_events = DEFAULT_WKI_TRACE_EVENTS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf vmem-trace: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }
    cmd_wki_trace(max_events, filter, display_options, WkiDataView::VMEM);
    return 0;
}

int run_run_command(int argc, char** argv) {
    if (argc < 3) {
        std::println("perf run: usage: perf run <program> [args...]");
        return 1;
    }
    cmd_run(argc - 2, argv + 2);
    return 0;
}

int run_show_map_command(int /*argc*/, char** /*argv*/) {
    cmd_show_map();
    return 0;
}

int run_help_command(int argc, char** argv) {
    if (argc >= 3 && std::string_view(argv[2]) == "filters") {
        print_filters();
    } else {
        usage();
    }
    return 0;
}
}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char* argv[]) {
    if (argc < 2) {
        usage();
        return 1;
    }

    std::string_view const CMD(argv[1]);

    for (const auto& command : COMMANDS) {
        if (CMD == command.name) {
            return command.handler(argc, argv);
        }
    }

    usage();
    return 1;
}
