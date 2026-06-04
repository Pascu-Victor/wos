#include <abi-bits/fcntl.h>
#include <abi-bits/ioctls.h>
#include <bits/ssize_t.h>
#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX time APIs are exposed here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

constexpr int DEFAULT_INTERVAL_MS = 1000;
constexpr uint64_t PAGE_SIZE = 4096;
constexpr uint64_t KIB_PER_PAGE = PAGE_SIZE / 1024;
constexpr uint64_t PROC_CLK_TCK = 100;
constexpr int HEADER_LINES = 7;
constexpr int USER_WIDTH = 8;
constexpr int HOST_WIDTH = 12;
constexpr int COMMAND_WIDTH_FALLBACK = 32;

struct TerminalSize {
    int rows = 24;
    int cols = 80;
};

auto terminal_size() -> TerminalSize;

void write_stdout_best_effort(std::string_view text) {
    while (!text.empty()) {
        ssize_t const N = write(STDOUT_FILENO, text.data(), text.size());
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return;
        }
        text.remove_prefix(static_cast<size_t>(N));
    }
}

void soft_clear_visible_screen() {
    TerminalSize const TERM = terminal_size();
    for (int row = 0; row < TERM.rows; ++row) {
        static constexpr std::string_view NEWLINE = "\r\n";
        write_stdout_best_effort(NEWLINE);
    }
}

struct ScopedFd {
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

struct ScopedDir {
    explicit ScopedDir(DIR* dir = nullptr) : dir(dir) {}
    ScopedDir(const ScopedDir&) = delete;
    auto operator=(const ScopedDir&) -> ScopedDir& = delete;
    ~ScopedDir() {
        if (dir != nullptr) {
            closedir(dir);
        }
    }
    [[nodiscard]] auto get() const -> DIR* { return dir; }

   private:
    DIR* dir;
};

class TerminalMode {
   public:
    TerminalMode() {
        if (!isatty(STDIN_FILENO)) {
            return;
        }
        if (tcgetattr(STDIN_FILENO, &old_term) != 0) {
            return;
        }
        termios raw = old_term;
        cfmakeraw(&raw);
        if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) != 0) {
            return;
        }
        old_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
        if (old_flags >= 0) {
            (void)fcntl(STDIN_FILENO, F_SETFL, old_flags | O_NONBLOCK);
        }
        active = true;
        soft_clear_visible_screen();
    }

    TerminalMode(const TerminalMode&) = delete;
    auto operator=(const TerminalMode&) -> TerminalMode& = delete;

    ~TerminalMode() {
        if (!active) {
            return;
        }
        (void)tcsetattr(STDIN_FILENO, TCSANOW, &old_term);
        if (old_flags >= 0) {
            (void)fcntl(STDIN_FILENO, F_SETFL, old_flags);
        }
        static constexpr std::string_view SHOW_CURSOR = "\x1b[?25h\x1b[0m\r\n";
        write_stdout_best_effort(SHOW_CURSOR);
    }

   private:
    termios old_term{};
    int old_flags = -1;
    bool active = false;
};

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

auto write_all(int fd, std::string_view text) -> bool {
    size_t done = 0;
    while (done < text.size()) {
        ssize_t const N = write(fd, text.data() + done, text.size() - done);
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return false;
        }
        done += static_cast<size_t>(N);
    }
    return true;
}

auto read_file(std::string_view path, size_t limit = 65536) -> std::optional<std::string> {
    std::string path_copy(path);
    ScopedFd fd(open(path_copy.c_str(), O_RDONLY));
    if (!fd.valid()) {
        return std::nullopt;
    }

    std::string out;
    out.reserve(std::min<size_t>(limit, 4096));
    std::array<char, 1024> buf{};
    while (out.size() < limit) {
        ssize_t const N = read(fd.get(), buf.data(), std::min(buf.size(), limit - out.size()));
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            break;
        }
        out.append(buf.data(), static_cast<size_t>(N));
    }
    return out;
}

auto parse_u64(std::string_view text, uint64_t& out) -> bool {
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    for (char ch : text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        value = (value * 10ULL) + static_cast<uint64_t>(ch - '0');
    }
    out = value;
    return true;
}

auto parse_i64(std::string_view text, int64_t& out) -> bool {
    if (text.empty()) {
        return false;
    }
    bool neg = false;
    if (text.front() == '-') {
        neg = true;
        text.remove_prefix(1);
    }
    uint64_t value = 0;
    if (!parse_u64(text, value)) {
        return false;
    }
    out = neg ? -static_cast<int64_t>(value) : static_cast<int64_t>(value);
    return true;
}

auto split_ws(std::string_view text) -> std::vector<std::string_view> {
    std::vector<std::string_view> tokens;
    size_t pos = 0;
    while (pos < text.size()) {
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n' || text[pos] == '\r')) {
            pos++;
        }
        size_t const START = pos;
        while (pos < text.size() && text[pos] != ' ' && text[pos] != '\t' && text[pos] != '\n' && text[pos] != '\r') {
            pos++;
        }
        if (pos > START) {
            tokens.emplace_back(text.data() + START, pos - START);
        }
    }
    return tokens;
}

auto parse_double(std::string_view text) -> double {
    std::string copy(text);
    char* end = nullptr;
    double const value = std::strtod(copy.c_str(), &end);
    return (end != nullptr && end != copy.c_str()) ? value : 0.0;
}

auto cpu_total(const CpuTimes& cpu) -> uint64_t {
    return cpu.user + cpu.nice + cpu.system + cpu.idle + cpu.iowait + cpu.irq + cpu.softirq + cpu.steal;
}

auto diff_counter(uint64_t now, uint64_t old) -> uint64_t { return now >= old ? now - old : 0; }

auto diff_cpu(const CpuTimes& now, const CpuTimes& old) -> CpuTimes {
    return CpuTimes{
        .user = diff_counter(now.user, old.user),
        .nice = diff_counter(now.nice, old.nice),
        .system = diff_counter(now.system, old.system),
        .idle = diff_counter(now.idle, old.idle),
        .iowait = diff_counter(now.iowait, old.iowait),
        .irq = diff_counter(now.irq, old.irq),
        .softirq = diff_counter(now.softirq, old.softirq),
        .steal = diff_counter(now.steal, old.steal),
    };
}

auto cpu_percent_from_delta(const CpuTimes& delta) -> CpuPercent {
    uint64_t const TOTAL = cpu_total(delta);
    if (TOTAL == 0) {
        return {};
    }
    auto pct = [TOTAL](uint64_t value) -> double { return (static_cast<double>(value) * 100.0) / static_cast<double>(TOTAL); };
    return CpuPercent{
        .user = pct(delta.user),
        .nice = pct(delta.nice),
        .system = pct(delta.system),
        .idle = pct(delta.idle),
        .iowait = pct(delta.iowait),
        .irq = pct(delta.irq),
        .softirq = pct(delta.softirq),
        .steal = pct(delta.steal),
    };
}

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

auto terminal_size() -> TerminalSize {
    TerminalSize size{};
    winsize ws{};
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0) {
        if (ws.ws_row > 0) {
            size.rows = ws.ws_row;
        }
        if (ws.ws_col > 0) {
            size.cols = ws.ws_col;
        }
    }
    return size;
}

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

auto render(const Snapshot& snap, const Snapshot* previous, int row_offset, int col_offset) -> std::string {
    TerminalSize const TERM = terminal_size();
    CpuPercent const CPU = cpu_percent_from_delta(previous != nullptr ? diff_cpu(snap.cpu, previous->cpu) : snap.cpu);
    std::string out;
    out.reserve(8192);
    out += "\r\x1b[H\x1b[2J\x1b[H\x1b[?25l";
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

enum class Key : uint8_t {
    NONE,
    QUIT,
    UP,
    DOWN,
    LEFT,
    RIGHT,
    PAGE_UP,
    PAGE_DOWN,
    HOME,
    END,
};

auto read_key() -> Key {
    std::array<char, 16> buf{};
    ssize_t const N = read(STDIN_FILENO, buf.data(), buf.size());
    if (N <= 0) {
        return Key::NONE;
    }
    if (buf[0] == 'q' || buf[0] == 'Q') {
        return Key::QUIT;
    }
    if (buf[0] != '\x1b' || N < 3 || buf[1] != '[') {
        return Key::NONE;
    }
    switch (buf[2]) {
        case 'A':
            return Key::UP;
        case 'B':
            return Key::DOWN;
        case 'C':
            return Key::RIGHT;
        case 'D':
            return Key::LEFT;
        case 'H':
            return Key::HOME;
        case 'F':
            return Key::END;
        case '1':
            return (N >= 4 && buf[3] == '~') ? Key::HOME : Key::NONE;
        case '4':
            return (N >= 4 && buf[3] == '~') ? Key::END : Key::NONE;
        case '5':
            return (N >= 4 && buf[3] == '~') ? Key::PAGE_UP : Key::NONE;
        case '6':
            return (N >= 4 && buf[3] == '~') ? Key::PAGE_DOWN : Key::NONE;
        default:
            return Key::NONE;
    }
}

void apply_key(Key key, int& row_offset, int& col_offset, int row_count) {
    TerminalSize const TERM = terminal_size();
    int const PAGE = std::max(1, TERM.rows - HEADER_LINES);
    int const MAX_ROW = std::max(0, row_count - PAGE);
    switch (key) {
        case Key::UP:
            row_offset = std::max(0, row_offset - 1);
            break;
        case Key::DOWN:
            row_offset = std::min(MAX_ROW, row_offset + 1);
            break;
        case Key::LEFT:
            col_offset = std::max(0, col_offset - 4);
            break;
        case Key::RIGHT:
            col_offset += 4;
            break;
        case Key::PAGE_UP:
            row_offset = std::max(0, row_offset - PAGE);
            break;
        case Key::PAGE_DOWN:
            row_offset = std::min(MAX_ROW, row_offset + PAGE);
            break;
        case Key::HOME:
            row_offset = 0;
            col_offset = 0;
            break;
        case Key::END:
            row_offset = MAX_ROW;
            break;
        case Key::NONE:
        case Key::QUIT:
            break;
    }
}

auto parse_interval_ms(int argc, char** argv) -> int {
    if (argc < 2) {
        return DEFAULT_INTERVAL_MS;
    }
    uint64_t parsed = 0;
    if (!parse_u64(argv[1], parsed) || parsed == 0 || parsed > 60000) {
        return DEFAULT_INTERVAL_MS;
    }
    return static_cast<int>(parsed);
}

}  // namespace

int main(int argc, char** argv) {
    int const INTERVAL_MS = parse_interval_ms(argc, argv);
    [[maybe_unused]] TerminalMode const terminal_mode;
    auto users = read_passwd();

    std::optional<Snapshot> previous;
    int row_offset = 0;
    int col_offset = 0;
    bool quit = false;

    while (!quit) {
        Snapshot snap = make_snapshot(previous.has_value() ? &*previous : nullptr, users);
        TerminalSize const TERM = terminal_size();
        int const PAGE = std::max(1, TERM.rows - HEADER_LINES);
        row_offset = std::clamp(row_offset, 0, std::max(0, static_cast<int>(snap.rows.size()) - PAGE));

        std::string screen = render(snap, previous.has_value() ? &*previous : nullptr, row_offset, col_offset);
        if (!write_all(STDOUT_FILENO, screen)) {
            return 1;
        }

        previous = std::move(snap);
        int remaining_ms = INTERVAL_MS;
        while (remaining_ms > 0 && !quit) {
            int const WAIT_MS = std::min(remaining_ms, 100);
            pollfd pfd{
                .fd = STDIN_FILENO,
                .events = POLLIN,
                .revents = 0,
            };
            int const READY = poll(&pfd, 1, WAIT_MS);
            if (READY > 0 && (pfd.revents & POLLIN) != 0) {
                Key const KEY = read_key();
                if (KEY == Key::QUIT) {
                    quit = true;
                    break;
                }
                apply_key(KEY, row_offset, col_offset, static_cast<int>(previous->rows.size()));
                if (KEY != Key::NONE) {
                    break;
                }
            }
            remaining_ms -= WAIT_MS;
        }
    }

    return 0;
}
