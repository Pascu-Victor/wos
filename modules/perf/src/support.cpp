#include "perf.hpp"
namespace perf {

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

auto make_time_display(const WkiDisplayOptions& options, std::string_view buffer, bool sectioned) -> TimeDisplay {
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

auto open_write_trunc(std::string_view path, mode_t mode) -> ScopedFd {
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

auto read_limit_for_path(std::string_view path) -> std::size_t {
    if (path == KPERF_PATH || path == KWKISTAT_PATH || path == KIPCSTAT_PATH || path == KCPUSTAT_PATH || path == KCONTSTAT_PATH) {
        return PERF_PROC_READ_LIMIT;
    }
    if (path.starts_with(PROC_ROOT) || path.starts_with(DEV_NODES_ROOT)) {
        return PROCFS_READ_LIMIT;
    }
    return PERF_DATA_READ_LIMIT;
}

auto read_fd(ScopedFd& fd, std::size_t initial_capacity, std::size_t max_bytes) -> std::optional<std::string> {
    std::string buffer;
    buffer.reserve(std::min(std::max<std::size_t>(initial_capacity, 1), max_bytes));
    std::array<char, READ_CHUNK_CAPACITY> chunk{};
    for (;;) {
        std::size_t const REMAINING = max_bytes - buffer.size();
        if (REMAINING == 0) {
            char extra = '\0';
            ssize_t const COUNT = read(fd.get(), &extra, 1);
            if (COUNT < 0 && errno == EINTR) {
                continue;
            }
            if (COUNT < 0 || COUNT > 0) {
                return std::nullopt;
            }
            return buffer;
        }

        ssize_t const COUNT = read(fd.get(), chunk.data(), std::min(chunk.size(), REMAINING));
        if (COUNT < 0 && errno == EINTR) {
            continue;
        }
        if (COUNT < 0) {
            return std::nullopt;
        }
        if (COUNT == 0) {
            return buffer;
        }
        buffer.append(chunk.data(), static_cast<std::size_t>(COUNT));
    }
}

auto read_file(std::string_view path, std::size_t initial_capacity) -> std::optional<std::string> {
    ScopedFd fd = open_readonly(path);
    if (!fd.valid()) {
        return std::nullopt;
    }
    return read_fd(fd, initial_capacity, read_limit_for_path(path));
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

auto build_dev_nodes_path(std::string_view hostname, std::string_view suffix) -> std::string {
    std::string path(DEV_NODES_ROOT);
    if (!hostname.empty()) {
        path += '/';
        path += hostname;
    }
    path += suffix;
    return path;
}

auto parse_u64(std::string_view text, int base) -> uint64_t {
    std::string const OWNED(text);
    return static_cast<uint64_t>(strtoull(OWNED.c_str(), nullptr, base));
}

auto parse_i64(std::string_view text, int base) -> int64_t {
    std::string const OWNED(text);
    return static_cast<int64_t>(strtoll(OWNED.c_str(), nullptr, base));
}

auto parse_u32(std::string_view text, int base) -> uint32_t {
    std::string const OWNED(text);
    return static_cast<uint32_t>(strtoul(OWNED.c_str(), nullptr, base));
}

auto parse_u8(std::string_view text, int base) -> uint8_t {
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

namespace {

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
void for_each_process_main_stat(Func func) {
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

        (void)emit_proc_stat(func, NAME, build_proc_path(NAME, PROC_STAT_SUFFIX));
    }
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

}  // namespace

auto collect_stats() -> std::vector<StatInfo> {
    std::vector<StatInfo> stats;
    for_each_process_stat([&](const StatInfo& info, std::string_view) { stats.push_back(info); });
    return stats;
}

auto collect_main_stats() -> std::vector<StatInfo> {
    std::vector<StatInfo> stats;
    for_each_process_main_stat([&](const StatInfo& info, std::string_view) { stats.push_back(info); });
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

auto write_section_contstat(int fd) -> ssize_t {
    auto stats = read_file(KCONTSTAT_PATH, CPUSTAT_READ_CAPACITY);
    if (!stats.has_value() || stats->empty()) {
        return 0;
    }

    write_all(fd, SECTION_CONTSTAT);
    write_all(fd, *stats);
    write_all(fd, SECTION_CONTSTAT_END);
    return static_cast<ssize_t>(stats->size());
}

auto write_section_memacc_alloc_totals(int fd) -> ssize_t {
    auto stats = read_file(MEMACC_ALLOC_TOTALS_PATH, CPUSTAT_READ_CAPACITY);
    if (!stats.has_value() || stats->empty()) {
        return 0;
    }

    write_all(fd, SECTION_MEMACC_ALLOC_TOTALS);
    write_all(fd, *stats);
    write_all(fd, SECTION_MEMACC_ALLOC_TOTALS_END);
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
    ssize_t const CONTSTAT_BYTES = write_section_contstat(FD.get());
    ssize_t const MEMACC_BYTES = write_section_memacc_alloc_totals(FD.get());
    ssize_t const DIAG_BYTES = CONTSTAT_BYTES + MEMACC_BYTES;

    if (event_bytes <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0 && DIAG_BYTES <= 0) {
        std::println("perf: ring buffer empty - PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes, {} diag bytes)", PERF_DATA_FILE, event_bytes,
                     SUMMARY_BYTES, IPC_BYTES, DIAG_BYTES);
    }
}

void set_recording_enabled(bool enabled, const char* filter) {
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

void cmd_record(int ms, const char* filter) {
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
    ssize_t const CONTSTAT_BYTES = write_section_contstat(data_fd.get());
    ssize_t const MEMACC_BYTES = write_section_memacc_alloc_totals(data_fd.get());
    ssize_t const DIAG_BYTES = CONTSTAT_BYTES + MEMACC_BYTES;
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

    if (total_event_bytes <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0 && DIAG_BYTES <= 0) {
        std::println("perf: ring buffer empty - PROC_MAP saved, no events");
    } else {
        std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes, {} diag bytes)", PERF_DATA_FILE, total_event_bytes,
                     SUMMARY_BYTES, IPC_BYTES, DIAG_BYTES);
    }
}

}  // namespace perf
