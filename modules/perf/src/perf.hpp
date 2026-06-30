#pragma once

#include <abi-bits/access.h>
#include <abi-bits/clockid_t.h>
#include <abi-bits/fcntl.h>
#include <abi-bits/mode_t.h>
#include <bits/ssize_t.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers,misc-include-cleaner): WOS signal constants live here.
#include <sys/process.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX clock_gettime/nanosleep are declared here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
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

namespace perf {

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
constexpr int PERF_RUN_PROC_SCAN_INTERVAL_MS = 100;
constexpr int PARSE_BASE_DECIMAL = 10;
constexpr int PROC_STAT_SKIP_FIELDS = 8;
constexpr int PROC_WAIT_NOHANG = 1;
constexpr char EXITED_STATE = 'Z';
constexpr int DRAIN_INTERVAL_MS = 250;
constexpr int PERF_RUN_CANCEL_KILL_AFTER_MS = 2000;
constexpr int EVENT_MIN_TOKEN_COUNT = 4;
constexpr int EVENT_EXTENDED_TOKEN_COUNT = 6;
constexpr int HOTSPOT_ROW_LIMIT = 10;
constexpr int EXEC_FAILURE_EXIT_CODE = 127;
constexpr int DEFAULT_WKI_TRACE_EVENTS = 200;
constexpr int DEFAULT_WKI_TAIL_ROWS = 15;
constexpr int DEFAULT_WKI_LAUNCH_ROWS = 20;
constexpr int DEFAULT_CHECKOUT_ROWS = 12;
constexpr uint64_t PERF_PAGE_SIZE = 4096;
constexpr int BOOT_TIME_COLUMN_WIDTH = 18;
constexpr int UNIX_TIME_COLUMN_WIDTH = 19;
constexpr int ISO_TIME_COLUMN_WIDTH = 30;

constexpr std::size_t INITIAL_FILE_CAPACITY = 131072;
constexpr std::size_t PROC_READ_CAPACITY = 512;
constexpr std::size_t PERF_DRAIN_CAPACITY = 65536;
constexpr std::size_t CPUSTAT_READ_CAPACITY = 16384;
constexpr std::size_t READ_CHUNK_CAPACITY = 4096;
constexpr std::size_t PROCFS_READ_LIMIT = 262144;
constexpr std::size_t PERF_PROC_READ_LIMIT = 65536;
constexpr std::size_t PERF_DATA_READ_LIMIT = std::size_t{8} * 1024 * 1024;
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
constexpr std::string_view MEMACC_ALLOC_TOTALS_PATH = "/proc/memacc/alloc_totals";
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
constexpr std::string_view SECTION_CONTSTAT = "--- SECTION CONTSTAT ---\n";
constexpr std::string_view SECTION_CONTSTAT_END = "--- END CONTSTAT ---\n";
constexpr std::string_view SECTION_MEMACC_ALLOC_TOTALS = "--- SECTION MEMACC_ALLOC_TOTALS ---\n";
constexpr std::string_view SECTION_MEMACC_ALLOC_TOTALS_END = "--- END MEMACC_ALLOC_TOTALS ---\n";
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
    uint64_t proxy_write_payload_bytes{};
    uint64_t proxy_write_no_credit_waits{};
    uint64_t proxy_write_block_us{};
    uint64_t proxy_pipe_rdma_full_waits{};
    uint64_t proxy_ring_full_waits{};
    uint64_t proxy_ring_full_bytes{};
    uint64_t pipe_payload_bytes{};
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

struct KeyValueRow {
    std::string record;
    std::unordered_map<std::string, std::string> kv;
};

struct CheckoutSummaryRows {
    std::string source;
    std::vector<WkiSummaryRow> rows;
};

struct TimeDisplay {
    TimeDisplayFormat format{TimeDisplayFormat::BOOT_NS};
    int64_t realtime_offset_ns{};
};

auto parse_timebase_offset(std::string_view buffer, bool sectioned) -> std::optional<int64_t>;
auto now_ms() -> int64_t;
auto timespec_to_ns(const timespec& ts) -> int64_t;
auto read_clock_ns(clockid_t clock_id) -> std::optional<int64_t>;
auto realtime_offset_ns() -> int64_t;
auto make_time_display(const WkiDisplayOptions& options, std::string_view buffer = {}, bool sectioned = false) -> TimeDisplay;
auto add_realtime_offset(uint64_t boot_ns, int64_t offset_ns) -> int64_t;
auto format_nine_digits(uint64_t value) -> std::string;
auto format_iso_realtime_ns(int64_t realtime_ns) -> std::string;
auto format_event_time(uint64_t boot_ns, const TimeDisplay& display) -> std::string;
auto format_optional_event_time(const std::optional<uint64_t>& boot_ns, const TimeDisplay& display) -> std::string;
auto time_column_header(const TimeDisplay& display) -> std::string_view;
auto time_column_width(const TimeDisplay& display) -> int;
auto syscall_result_i64(uint64_t raw) -> int64_t;
auto waitpid_nohang(int64_t pid, int32_t* status) -> int64_t;
void wall_sleep_ms(int target_ms);

auto open_readonly(std::string_view path) -> ScopedFd;
auto open_writeonly(std::string_view path) -> ScopedFd;
auto open_write_trunc(std::string_view path, mode_t mode = PERF_DATA_MODE) -> ScopedFd;
void write_all(int fd, std::string_view text);
auto read_limit_for_path(std::string_view path) -> std::size_t;
auto read_fd(ScopedFd& fd, std::size_t initial_capacity = INITIAL_FILE_CAPACITY, std::size_t max_bytes = PERF_DATA_READ_LIMIT)
    -> std::optional<std::string>;
auto read_file(std::string_view path, std::size_t initial_capacity = INITIAL_FILE_CAPACITY) -> std::optional<std::string>;
auto build_proc_path(std::string_view pid, std::string_view suffix) -> std::string;
auto build_proc_path(uint64_t pid, std::string_view suffix) -> std::string;
auto build_proc_task_path(std::string_view pid, std::string_view tid, std::string_view suffix) -> std::string;
auto build_dev_nodes_path(std::string_view hostname, std::string_view suffix = {}) -> std::string;
auto parse_u64(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint64_t;
auto parse_i64(std::string_view text, int base = PARSE_BASE_DECIMAL) -> int64_t;
auto parse_u32(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint32_t;
auto parse_u8(std::string_view text, int base = PARSE_BASE_DECIMAL) -> uint8_t;
auto trim_left(std::string_view text) -> std::string_view;
auto next_token(std::string_view text, std::size_t& pos) -> std::string_view;
auto next_line(std::string_view text, std::size_t& pos) -> std::string_view;
auto is_all_digits(std::string_view text) -> bool;
auto is_dot_entry(std::string_view text) -> bool;
auto parse_stat(std::string_view buf, StatInfo& out) -> bool;
auto collect_stats() -> std::vector<StatInfo>;
auto collect_main_stats() -> std::vector<StatInfo>;
auto read_cmdline(uint64_t pid) -> std::string;
auto proc_map_line(uint64_t pid, std::string_view comm, std::string_view cmdline) -> std::string;
auto peer_map_line(uint64_t peer, std::string_view hostname) -> std::string;
void write_section_timebase(int fd);
auto collect_live_wki_peer_map() -> std::vector<WkiPeerMapEntry>;
void write_section_proc_map(int fd);
auto write_section_events(int fd) -> ssize_t;
auto write_section_wki_summary(int fd) -> ssize_t;
auto write_section_ipc_stats(int fd) -> ssize_t;
auto write_section_contstat(int fd) -> ssize_t;
auto write_section_memacc_alloc_totals(int fd) -> ssize_t;
void write_section_peer_map(int fd);
void save_perf_data();
void set_recording_enabled(bool enabled, const char* filter = nullptr);

auto parse_proc_map_section(std::string_view buffer) -> std::vector<ProcMapEntry>;
auto extract_value(std::string_view line, std::string_view key) -> std::string_view;
auto parse_peer_map_line(std::string_view line, WkiPeerMapEntry& out) -> bool;
auto parse_peer_map_section(std::string_view buffer) -> std::vector<WkiPeerMapEntry>;
auto parse_wki_summary_line(std::string_view line, WkiSummaryRow& out) -> bool;
auto parse_wki_summary_section(std::string_view buffer, bool sectioned) -> std::vector<WkiSummaryRow>;
auto hex_value(char ch) -> int;
auto percent_decode(std::string_view value) -> std::string;
auto parse_key_value_rows(std::string_view text) -> std::vector<KeyValueRow>;
auto get_row_string(const KeyValueRow& row, std::string_view key) -> std::string_view;
auto get_row_u64(const KeyValueRow& row, std::string_view key) -> uint64_t;
auto find_row_by_record(const std::vector<KeyValueRow>& rows, std::string_view record) -> const KeyValueRow*;
auto find_row_by_key(const std::vector<KeyValueRow>& rows, std::string_view key, std::string_view value) -> const KeyValueRow*;
auto parse_ipc_stats_line(std::string_view line, IpcStatsSnapshot& out) -> bool;
auto parse_ipc_stats_section(std::string_view buffer, bool sectioned) -> std::optional<IpcStatsSnapshot>;
auto parse_event_line(std::string_view line, EventInfo& out) -> bool;
auto next_event(std::string_view text, std::size_t& pos, bool sectioned, EventInfo& out) -> bool;
auto comm_of_pid(const std::vector<ProcMapEntry>& proc_map, uint64_t pid) -> std::string_view;
auto format_pid_name(uint64_t pid, const std::vector<ProcMapEntry>& proc_map) -> std::string;
auto display_callsite(std::string_view callsite) -> std::string_view;
auto is_transport_stall_event(const EventInfo& event) -> bool;
auto is_local_vmem_event(const EventInfo& event) -> bool;
auto is_local_loader_event(const EventInfo& event) -> bool;
auto is_local_xfs_event(const EventInfo& event) -> bool;
auto is_local_irq_event(const EventInfo& event) -> bool;
auto is_vmem_cow_op(std::string_view op) -> bool;
auto is_vmem_file_cache_op(std::string_view op) -> bool;
auto format_cow_ref_category(uint64_t category) -> std::string_view;
auto append_flag_name(std::string& out, bool& first, uint32_t flags, uint32_t bit, std::string_view name) -> void;
auto format_transport_stall_flags(uint32_t flags) -> std::string;
auto format_transport_stall_detail(const EventInfo& event) -> std::string;
auto format_local_vmem_detail(const EventInfo& event) -> std::string;
auto format_local_loader_detail(const EventInfo& event) -> std::string;
auto format_local_xfs_detail(const EventInfo& event) -> std::string;
auto format_local_irq_kind(uint64_t kind) -> std::string_view;
auto format_local_irq_detail(const EventInfo& event) -> std::string;
auto format_wki_trace_detail(const EventInfo& event) -> std::string;
auto display_wait_channel(std::string_view wait_channel) -> std::string_view;
auto make_wki_peer_resolver(std::string_view buffer, bool sectioned, const WkiDisplayOptions& options) -> WkiPeerResolver;
auto wki_peer_label(uint64_t peer, WkiPeerResolver& resolver) -> const std::string&;
auto hotspot_for(std::vector<HotspotStats>& rows, uint64_t pid, std::string_view callsite, std::string_view wait_channel,
                 const std::vector<ProcMapEntry>& proc_map) -> HotspotStats&;
auto summarize_hotspots(std::string_view events, bool sectioned, const std::vector<ProcMapEntry>& proc_map) -> std::vector<HotspotStats>;

void print_sched_flags(uint8_t flags);
void print_wait_flags(uint8_t flags);
void print_hotspot_tables(const std::vector<HotspotStats>& rows);

auto current_proc_map() -> std::vector<ProcMapEntry>;
auto wki_view_name(WkiDataView view) -> std::string_view;
auto wki_trace_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool;
void cmd_stat(int ms);
void cmd_record(int ms, const char* filter = nullptr);
void cmd_sched(int max_events, const WkiDisplayOptions& display_options);
void cmd_cpustat();
void cmd_contstat();
void cmd_ipc_report(int limit, WkiTraceFilter filter, const WkiDisplayOptions& display_options);
void cmd_wki_report(const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view);
void cmd_vmem_report(WkiTraceFilter filter);
void cmd_checkout_report(int limit);
void cmd_wki_launch(int limit, const WkiDisplayOptions& display_options);
void cmd_wki_tail(int limit, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view);
void cmd_wki_trace(int max_events, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view);
void cmd_run(int argc, char** argv);
void cmd_show_map();

auto parse_time_display_format(std::string_view value, TimeDisplayFormat& out) -> bool;
auto parse_display_arg(std::string_view arg, WkiDisplayOptions& display_options) -> bool;
auto parse_wki_filter_arg(std::string_view arg, WkiTraceFilter& filter, WkiDisplayOptions& display_options) -> bool;

int run_perf(int argc, char** argv);

}  // namespace perf
