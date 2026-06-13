#include <abi-bits/fcntl.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX nanosleep is declared here.
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

constexpr std::string_view MEMACC_ROOT = "/proc/memacc";
constexpr uint64_t KIB = 1024;
constexpr int DEFAULT_LIMIT = 15;
constexpr size_t READ_CHUNK_CAPACITY = 4096;
constexpr size_t MEMACC_READ_LIMIT = 262144;

struct Row {
    std::string record;
    std::unordered_map<std::string, std::string> kv;
};

struct ProcRow {
    uint64_t pid = 0;
    uint64_t ppid = 0;
    uint64_t virt_bytes = 0;
    uint64_t rss_bytes = 0;
    uint64_t shr_bytes = 0;
    uint64_t pte_bytes = 0;
    uint64_t heap_bytes = 0;
    uint64_t mmap_bytes = 0;
    uint64_t stack_bytes = 0;
    uint64_t total_bytes = 0;
    uint64_t cpu = 0;
    std::string state;
    std::string name;
    std::string cmd;
};

struct Options {
    bool full = false;
    std::optional<uint64_t> pid;
    std::string name;
    std::string state;
    std::string sort = "rss";
    uint64_t min_kib = 0;
    int limit = DEFAULT_LIMIT;
    int interval_seconds = 1;
};

struct WatchSnapshot {
    uint64_t free_bytes = 0;
    uint64_t used_bytes = 0;
    uint64_t allocator_bytes = 0;
    std::unordered_map<uint64_t, uint64_t> proc_bytes;
    std::unordered_map<uint64_t, std::string> proc_names;
};

void usage() {
    std::printf(
        "usage: memacc [dump [--full]|summary|procs|proc <pid>|kernel|allocs|raw [file]|watch [-n sec]|track <feature> <action>|reclaim "
        "<buffer_cache|packet_pool> [target]]\n");
}

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

auto read_file(std::string_view path, size_t max_bytes = MEMACC_READ_LIMIT) -> std::optional<std::string> {
    ScopedFd fd(open(std::string(path).c_str(), O_RDONLY));
    if (!fd.valid()) {
        return std::nullopt;
    }

    std::string out;
    out.reserve(std::min(max_bytes, READ_CHUNK_CAPACITY));
    std::array<char, READ_CHUNK_CAPACITY> buf{};
    while (true) {
        size_t const REMAINING = max_bytes - out.size();
        if (REMAINING == 0) {
            char extra = '\0';
            ssize_t const COUNT = read(fd.get(), &extra, 1);
            if (COUNT < 0 && errno == EINTR) {
                continue;
            }
            if (COUNT < 0 || COUNT > 0) {
                return std::nullopt;
            }
            return out;
        }

        ssize_t const COUNT = read(fd.get(), buf.data(), std::min(buf.size(), REMAINING));
        if (COUNT < 0 && errno == EINTR) {
            continue;
        }
        if (COUNT < 0) {
            return std::nullopt;
        }
        if (COUNT == 0) {
            break;
        }
        out.append(buf.data(), static_cast<size_t>(COUNT));
    }
    return out;
}

auto write_file(std::string_view path, std::string_view text) -> bool {
    ScopedFd fd(open(std::string(path).c_str(), O_WRONLY));
    if (!fd.valid()) {
        return false;
    }
    size_t done = 0;
    while (done < text.size()) {
        ssize_t const n = write(fd.get(), text.data() + done, text.size() - done);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return false;
        }
        done += static_cast<size_t>(n);
    }
    return true;
}

auto hex_value(char c) -> int {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

auto percent_decode(std::string_view value) -> std::string {
    std::string out;
    out.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            int const HI = hex_value(value[i + 1]);
            int const LO = hex_value(value[i + 2]);
            if (HI >= 0 && LO >= 0) {
                out.push_back(static_cast<char>((HI << 4) | LO));
                i += 2;
                continue;
            }
        }
        out.push_back(value[i]);
    }
    return out;
}

auto parse_rows(std::string_view text) -> std::vector<Row> {
    std::vector<Row> rows;
    size_t pos = 0;
    while (pos < text.size()) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        std::string_view line = text.substr(pos, end - pos);
        pos = end + 1;
        if (line.empty()) {
            continue;
        }

        Row row;
        size_t token_start = 0;
        size_t token_end = line.find(' ');
        row.record = std::string(line.substr(0, token_end));
        token_start = token_end == std::string_view::npos ? line.size() : token_end + 1;
        while (token_start < line.size()) {
            token_end = line.find(' ', token_start);
            if (token_end == std::string_view::npos) {
                token_end = line.size();
            }
            std::string_view token = line.substr(token_start, token_end - token_start);
            size_t const EQ = token.find('=');
            if (EQ != std::string_view::npos && EQ > 0) {
                row.kv.emplace(std::string(token.substr(0, EQ)), percent_decode(token.substr(EQ + 1)));
            }
            token_start = token_end + 1;
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

auto memacc_path(std::string_view file) -> std::string {
    if (!file.empty() && file.front() == '/') {
        return std::string(file);
    }
    std::string path(MEMACC_ROOT);
    path.push_back('/');
    path.append(file);
    return path;
}

auto read_rows(std::string_view file) -> std::vector<Row> {
    auto text = read_file(memacc_path(file));
    if (!text.has_value()) {
        return {};
    }
    return parse_rows(*text);
}

auto get_string(const Row& row, std::string_view key) -> std::string {
    auto it = row.kv.find(std::string(key));
    return it == row.kv.end() ? std::string{} : it->second;
}

auto get_u64(const Row& row, std::string_view key) -> uint64_t {
    std::string const VALUE = get_string(row, key);
    if (VALUE.empty() || VALUE == "-") {
        return 0;
    }
    int base = 10;
    const char* s = VALUE.c_str();
    if (VALUE.size() > 2 && VALUE[0] == '0' && (VALUE[1] == 'x' || VALUE[1] == 'X')) {
        base = 16;
    }
    return static_cast<uint64_t>(std::strtoull(s, nullptr, base));
}

auto first_record(const std::vector<Row>& rows, std::string_view name) -> const Row* {
    for (const auto& row : rows) {
        if (row.record == name) {
            return &row;
        }
    }
    return nullptr;
}

auto bytes_to_kib(uint64_t bytes) -> uint64_t { return bytes / KIB; }

void print_kib_line(const char* label, uint64_t bytes) {
    std::printf("%-24s %12llu KiB\n", label, static_cast<unsigned long long>(bytes_to_kib(bytes)));
}

template <size_t N>
auto contains_key(const std::array<std::string_view, N>& keys, std::string_view key) -> bool {
    return std::find(keys.begin(), keys.end(), key) != keys.end();
}

template <size_t N>
void print_ordered_row(const Row& row, const std::array<std::string_view, N>& keys) {
    std::printf("%s", row.record.c_str());
    for (std::string_view key : keys) {
        auto it = row.kv.find(std::string(key));
        if (it != row.kv.end()) {
            std::printf(" %.*s=%s", static_cast<int>(key.size()), key.data(), it->second.c_str());
        }
    }
    for (const auto& kv : row.kv) {
        if (!contains_key(keys, kv.first)) {
            std::printf(" %s=%s", kv.first.c_str(), kv.second.c_str());
        }
    }
    std::printf("\n");
}

void print_raw_row(const Row& row) {
    std::printf("%s", row.record.c_str());
    for (const auto& kv : row.kv) {
        std::printf(" %s=%s", kv.first.c_str(), kv.second.c_str());
    }
    std::printf("\n");
}

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

auto parse_u64_arg(std::string_view text) -> uint64_t {
    return static_cast<uint64_t>(std::strtoull(std::string(text).c_str(), nullptr, 10));
}

auto parse_u64_arg_strict(std::string_view text, uint64_t& out) -> bool {
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    for (char ch : text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        auto const DIGIT = static_cast<uint64_t>(ch - '0');
        if (value > (UINT64_MAX - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
    }
    out = value;
    return true;
}

auto parse_options(int argc, char** argv, int start) -> Options {
    Options opt;
    for (int i = start; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--full") {
            opt.full = true;
        } else if (arg == "--pid" && i + 1 < argc) {
            opt.pid = parse_u64_arg(argv[++i]);
        } else if (arg == "--name" && i + 1 < argc) {
            opt.name = argv[++i];
        } else if (arg == "--state" && i + 1 < argc) {
            opt.state = argv[++i];
        } else if (arg == "--min-kib" && i + 1 < argc) {
            opt.min_kib = parse_u64_arg(argv[++i]);
        } else if (arg == "--sort" && i + 1 < argc) {
            opt.sort = argv[++i];
        } else if (arg == "--limit" && i + 1 < argc) {
            opt.limit = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else if (arg == "-n" && i + 1 < argc) {
            opt.interval_seconds = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
            if (opt.interval_seconds <= 0) {
                opt.interval_seconds = 1;
            }
        }
    }
    return opt;
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

void print_summary() {
    auto rows = read_rows("summary");
    const Row* summary = first_record(rows, "summary");
    if (summary == nullptr) {
        std::printf("memacc: /proc/memacc/summary unavailable\n");
        return;
    }

    std::printf("Memory\n");
    print_kib_line("total", get_u64(*summary, "total_bytes"));
    print_kib_line("free", get_u64(*summary, "free_bytes"));
    print_kib_line("used", get_u64(*summary, "used_bytes"));
    print_kib_line("process rss", get_u64(*summary, "process_rss_bytes"));
    print_kib_line("process pte", get_u64(*summary, "process_pte_bytes"));
    print_kib_line("allocator", get_u64(*summary, "allocator_bytes"));
    print_kib_line("caches", get_u64(*summary, "cache_bytes"));
    print_kib_line("unaccounted estimate", get_u64(*summary, "unaccounted_estimate_bytes"));
    std::printf("processes %llu, tasks %llu, kernel tasks %llu\n", static_cast<unsigned long long>(get_u64(*summary, "processes")),
                static_cast<unsigned long long>(get_u64(*summary, "tasks")),
                static_cast<unsigned long long>(get_u64(*summary, "kernel_tasks")));

    auto alloc_rows = read_rows("alloc_totals");
    if (const Row* phys = first_record(alloc_rows, "phys"); phys != nullptr) {
        std::printf("physical allocator pages allocated=%llu freed=%llu live=%llu free=%llu alloc_ops=%llu free_ops=%llu\n",
                    static_cast<unsigned long long>(get_u64(*phys, "total_allocated_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "total_freed_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "live_allocated_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "current_free_pages")),
                    static_cast<unsigned long long>(get_u64(*phys, "alloc_count")),
                    static_cast<unsigned long long>(get_u64(*phys, "free_count")));
    }

    for (const auto& row : rows) {
        if (row.record == "feature") {
            std::printf("feature %-14s available=%s enabled=%s generation=%llu\n", get_string(row, "name").c_str(),
                        get_u64(row, "available") != 0 ? "yes" : "no", get_u64(row, "enabled") != 0 ? "yes" : "no",
                        static_cast<unsigned long long>(get_u64(row, "generation")));
        }
    }
}

void print_alloc_rows() {
    constexpr std::array<std::string_view, 13> PHYS_ALLOC_KEYS{
        "total_allocated_bytes", "total_freed_bytes",     "live_allocated_bytes",  "alloc_count",       "free_count",
        "total_mem_bytes",       "free_mem_bytes",        "total_allocated_pages", "total_freed_pages", "live_allocated_pages",
        "current_free_pages",    "alloc_operation_count", "free_operation_count"};

    for (const auto& row : read_rows("alloc_totals")) {
        if (row.record == "phys") {
            print_ordered_row(row, PHYS_ALLOC_KEYS);
        } else {
            print_raw_row(row);
        }
    }
}

void print_kernel() {
    print_alloc_rows();
    for (const auto& row : read_rows("dead")) {
        if (row.record == "queue") {
            std::printf("cpu=%llu run=%llu wait=%llu dead=%llu\n", static_cast<unsigned long long>(get_u64(row, "cpu")),
                        static_cast<unsigned long long>(get_u64(row, "run")), static_cast<unsigned long long>(get_u64(row, "wait")),
                        static_cast<unsigned long long>(get_u64(row, "dead")));
        }
    }
}

void print_dump(const Options& opt) {
    print_summary();
    std::printf("\nTop processes\n");
    Options proc_opt = opt;
    if (proc_opt.full) {
        proc_opt.limit = 0;
    }
    print_proc_table(filtered_procs(proc_opt));

    std::printf("\nAllocator totals\n");
    print_alloc_rows();

    std::printf("\nZones\n");
    for (const auto& row : read_rows("zones")) {
        if (row.record != "zone") {
            continue;
        }
        std::printf("zone=%llu name=%s free=%llu/%llu pages cached0=%llu metadata=%llu mismatch=%llu\n",
                    static_cast<unsigned long long>(get_u64(row, "id")), get_string(row, "name").c_str(),
                    static_cast<unsigned long long>(get_u64(row, "free_pages")),
                    static_cast<unsigned long long>(get_u64(row, "usable_pages")),
                    static_cast<unsigned long long>(get_u64(row, "cached_order0_pages")),
                    static_cast<unsigned long long>(get_u64(row, "metadata_pages")),
                    static_cast<unsigned long long>(get_u64(row, "free_count_mismatch")));
    }

    if (!opt.full) {
        return;
    }

    std::printf("\nDead queues\n");
    auto dead_text = read_file(memacc_path("dead"));
    if (dead_text.has_value()) {
        std::printf("%s", dead_text->c_str());
    }
    std::printf("\nSlabs\n");
    auto slabs_text = read_file(memacc_path("slabs"));
    if (slabs_text.has_value()) {
        std::printf("%s", slabs_text->c_str());
    }
    std::printf("\nkmalloc live\n");
    auto kmalloc_text = read_file(memacc_path("kmalloc_live"));
    if (kmalloc_text.has_value()) {
        std::printf("%s", kmalloc_text->c_str());
    }
    std::printf("\nPage callers\n");
    auto page_text = read_file(memacc_path("page_callers"));
    if (page_text.has_value()) {
        std::printf("%s", page_text->c_str());
    }
}

auto collect_watch_snapshot() -> WatchSnapshot {
    WatchSnapshot snap;
    auto summary_rows = read_rows("summary");
    if (const Row* summary = first_record(summary_rows, "summary"); summary != nullptr) {
        snap.free_bytes = get_u64(*summary, "free_bytes");
        snap.used_bytes = get_u64(*summary, "used_bytes");
        snap.allocator_bytes = get_u64(*summary, "allocator_bytes");
    }
    for (const auto& proc : load_procs()) {
        snap.proc_bytes[proc.pid] = proc.total_bytes;
        snap.proc_names[proc.pid] = proc.name;
    }
    return snap;
}

void sleep_seconds(int seconds) {
    timespec req{.tv_sec = seconds, .tv_nsec = 0};
    while (nanosleep(&req, &req) != 0 && errno == EINTR) {
    }
}

void print_delta_line(const char* label, uint64_t old_value, uint64_t new_value) {
    int64_t const DELTA = static_cast<int64_t>(new_value) - static_cast<int64_t>(old_value);
    std::printf("%-12s %12llu KiB  delta %+lld KiB\n", label, static_cast<unsigned long long>(bytes_to_kib(new_value)),
                static_cast<long long>(DELTA / static_cast<int64_t>(KIB)));
}

void run_watch(const Options& opt) {
    std::optional<WatchSnapshot> prev;
    while (true) {
        WatchSnapshot cur = collect_watch_snapshot();
        std::printf("\x1b[2J\x1b[H");
        std::printf("memacc watch interval=%d\n\n", opt.interval_seconds);
        if (prev.has_value()) {
            print_delta_line("free", prev->free_bytes, cur.free_bytes);
            print_delta_line("used", prev->used_bytes, cur.used_bytes);
            print_delta_line("allocator", prev->allocator_bytes, cur.allocator_bytes);

            struct Mover {
                uint64_t pid;
                int64_t delta;
                uint64_t now;
                std::string name;
            };
            std::vector<Mover> movers;
            for (const auto& [pid, now] : cur.proc_bytes) {
                uint64_t const OLD = prev->proc_bytes.contains(pid) ? prev->proc_bytes.at(pid) : 0;
                int64_t const DELTA = static_cast<int64_t>(now) - static_cast<int64_t>(OLD);
                if (DELTA != 0) {
                    movers.push_back(Mover{.pid = pid, .delta = DELTA, .now = now, .name = cur.proc_names[pid]});
                }
            }
            std::sort(movers.begin(), movers.end(), [](const Mover& a, const Mover& b) {
                uint64_t const AA = a.delta < 0 ? static_cast<uint64_t>(-a.delta) : static_cast<uint64_t>(a.delta);
                uint64_t const BB = b.delta < 0 ? static_cast<uint64_t>(-b.delta) : static_cast<uint64_t>(b.delta);
                return AA > BB;
            });
            std::printf("\nTop movers\n");
            int shown = 0;
            for (const auto& mover : movers) {
                if (opt.limit > 0 && shown >= opt.limit) {
                    break;
                }
                std::printf("%6llu %+10lld KiB now=%10llu KiB %s\n", static_cast<unsigned long long>(mover.pid),
                            static_cast<long long>(mover.delta / static_cast<int64_t>(KIB)),
                            static_cast<unsigned long long>(bytes_to_kib(mover.now)), mover.name.c_str());
                shown++;
            }
        } else {
            print_delta_line("free", cur.free_bytes, cur.free_bytes);
            print_delta_line("used", cur.used_bytes, cur.used_bytes);
            print_delta_line("allocator", cur.allocator_bytes, cur.allocator_bytes);
        }
        prev = std::move(cur);
        sleep_seconds(opt.interval_seconds);
    }
}

auto normalize_feature(std::string_view feature) -> std::string {
    if (feature == "page_callers" || feature == "pages" || feature == "page") {
        return "page_callers";
    }
    if (feature == "kmalloc_debug" || feature == "kmalloc") {
        return "kmalloc_debug";
    }
    return {};
}

auto run_track(int argc, char** argv) -> int {
    if (argc < 4) {
        usage();
        return 1;
    }
    std::string feature = normalize_feature(argv[2]);
    if (feature.empty()) {
        std::printf("memacc: unknown feature '%s'\n", argv[2]);
        return 1;
    }
    std::string action = argv[3];
    std::string path = memacc_path(std::string("track/") + feature);
    if (action != "status" && !write_file(path, action)) {
        std::printf("memacc: failed to write %s\n", path.c_str());
        return 1;
    }
    auto text = read_file(path);
    if (!text.has_value()) {
        std::printf("memacc: failed to read %s\n", path.c_str());
        return 1;
    }
    std::printf("%s", text->c_str());
    return 0;
}

auto run_reclaim(int argc, char** argv) -> int {
    if (argc < 3) {
        usage();
        return 1;
    }

    std::string_view target_arg(argv[2]);
    std::string target;
    if (target_arg == "buffer_cache" || target_arg == "bcache" || target_arg == "cache") {
        target = "buffer_cache";
    } else if (target_arg == "packet_pool" || target_arg == "packets" || target_arg == "pkt") {
        target = "packet_pool";
    } else {
        std::printf("memacc: unknown reclaim target '%s'\n", argv[2]);
        return 1;
    }

    std::string const PROC_FILE = std::string("reclaim/") + target;
    std::string path = memacc_path(PROC_FILE);
    auto before_rows = read_rows(PROC_FILE);
    const Row* before = first_record(before_rows, "reclaim");

    std::string command = "drop";
    if (argc >= 4) {
        std::string_view arg(argv[3]);
        if (arg == "drop" || arg == "all") {
            command = std::string(arg);
        } else {
            uint64_t target_kib = 0;
            if (!parse_u64_arg_strict(arg, target_kib)) {
                std::printf("memacc: invalid reclaim target '%s'\n", argv[3]);
                return 1;
            }
            uint64_t const TARGET_KIB = target_kib;
            if (TARGET_KIB > UINT64_MAX / KIB) {
                std::printf("memacc: reclaim target is too large\n");
                return 1;
            }
            command = target == "buffer_cache" ? std::to_string(TARGET_KIB * KIB) : std::to_string(TARGET_KIB);
        }
    }

    if (!write_file(path, command)) {
        std::printf("memacc: failed to write %s\n", path.c_str());
        return 1;
    }

    auto after_rows = read_rows(PROC_FILE);
    const Row* after = first_record(after_rows, "reclaim");
    if (after == nullptr) {
        std::printf("memacc: reclaim wrote %s but status is unavailable\n", path.c_str());
        return 1;
    }

    if (target == "buffer_cache") {
        uint64_t const BEFORE_BYTES = before != nullptr ? get_u64(*before, "total_bytes") : 0;
        uint64_t const AFTER_BYTES = get_u64(*after, "total_bytes");
        uint64_t const FREED_BYTES = BEFORE_BYTES >= AFTER_BYTES ? BEFORE_BYTES - AFTER_BYTES : 0;
        std::printf("buffer_cache before=%llu KiB after=%llu KiB freed=%llu KiB clean=%llu KiB dirty=%llu KiB buffers=%llu\n",
                    static_cast<unsigned long long>(bytes_to_kib(BEFORE_BYTES)), static_cast<unsigned long long>(bytes_to_kib(AFTER_BYTES)),
                    static_cast<unsigned long long>(bytes_to_kib(FREED_BYTES)),
                    static_cast<unsigned long long>(bytes_to_kib(get_u64(*after, "clean_bytes"))),
                    static_cast<unsigned long long>(bytes_to_kib(get_u64(*after, "dirty_bytes"))),
                    static_cast<unsigned long long>(get_u64(*after, "buffers")));
    } else {
        uint64_t const BEFORE_BYTES = before != nullptr ? get_u64(*before, "total_bytes") : 0;
        uint64_t const BEFORE_CAPACITY = before != nullptr ? get_u64(*before, "capacity") : 0;
        uint64_t const AFTER_BYTES = get_u64(*after, "total_bytes");
        uint64_t const AFTER_CAPACITY = get_u64(*after, "capacity");
        uint64_t const FREED_BYTES = BEFORE_BYTES >= AFTER_BYTES ? BEFORE_BYTES - AFTER_BYTES : 0;
        uint64_t const FREED_BUFFERS = BEFORE_CAPACITY >= AFTER_CAPACITY ? BEFORE_CAPACITY - AFTER_CAPACITY : 0;
        std::printf(
            "packet_pool before=%llu buffers after=%llu buffers freed=%llu buffers freed_bytes=%llu KiB active=%llu free=%llu used=%llu "
            "baseline=%llu draining=%llu draining_free=%llu\n",
            static_cast<unsigned long long>(BEFORE_CAPACITY), static_cast<unsigned long long>(AFTER_CAPACITY),
            static_cast<unsigned long long>(FREED_BUFFERS), static_cast<unsigned long long>(bytes_to_kib(FREED_BYTES)),
            static_cast<unsigned long long>(get_u64(*after, "active_capacity")), static_cast<unsigned long long>(get_u64(*after, "free")),
            static_cast<unsigned long long>(get_u64(*after, "used")), static_cast<unsigned long long>(get_u64(*after, "baseline_capacity")),
            static_cast<unsigned long long>(get_u64(*after, "draining_buffers")),
            static_cast<unsigned long long>(get_u64(*after, "draining_free")));
    }
    return 0;
}

auto run_raw(int argc, char** argv) -> int {
    std::string file = argc >= 3 ? argv[2] : "summary";
    if (file == "all") {
        constexpr std::array<std::string_view, 12> FILES{
            "summary",         "zones",        "procs",        "dead",     "alloc_totals",         "slabs",
            "kmalloc_callers", "kmalloc_live", "page_callers", "features", "reclaim/buffer_cache", "reclaim/packet_pool"};
        for (auto one : FILES) {
            auto text = read_file(memacc_path(one));
            if (text.has_value()) {
                auto const name = std::string(one);
                std::printf("== %s ==\n%s", name.c_str(), text->c_str());
            }
        }
        return 0;
    }
    auto text = read_file(memacc_path(file));
    if (!text.has_value()) {
        std::printf("memacc: failed to read %s\n", memacc_path(file).c_str());
        return 1;
    }
    std::printf("%s", text->c_str());
    return 0;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    std::string_view cmd = argc >= 2 ? std::string_view(argv[1]) : std::string_view("dump");

    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        usage();
        return 0;
    }
    if (cmd == "dump") {
        print_dump(parse_options(argc, argv, 2));
        return 0;
    }
    if (cmd == "summary") {
        print_summary();
        return 0;
    }
    if (cmd == "procs") {
        print_proc_table(filtered_procs(parse_options(argc, argv, 2)));
        return 0;
    }
    if (cmd == "proc") {
        if (argc < 3) {
            usage();
            return 1;
        }
        Options opt = parse_options(argc, argv, 3);
        opt.pid = parse_u64_arg(argv[2]);
        opt.limit = 0;
        print_proc_table(filtered_procs(opt));
        return 0;
    }
    if (cmd == "kernel") {
        print_kernel();
        return 0;
    }
    if (cmd == "allocs") {
        print_alloc_rows();
        Options opt = parse_options(argc, argv, 2);
        if (opt.full) {
            if (auto text = read_file(memacc_path("slabs")); text.has_value()) {
                std::printf("\n%s", text->c_str());
            }
            if (auto text = read_file(memacc_path("kmalloc_callers")); text.has_value()) {
                std::printf("\n%s", text->c_str());
            }
            if (auto text = read_file(memacc_path("kmalloc_live")); text.has_value()) {
                std::printf("\n%s", text->c_str());
            }
            if (auto text = read_file(memacc_path("page_callers")); text.has_value()) {
                std::printf("\n%s", text->c_str());
            }
        }
        return 0;
    }
    if (cmd == "raw") {
        return run_raw(argc, argv);
    }
    if (cmd == "watch") {
        run_watch(parse_options(argc, argv, 2));
        return 0;
    }
    if (cmd == "track") {
        return run_track(argc, argv);
    }
    if (cmd == "reclaim") {
        return run_reclaim(argc, argv);
    }

    usage();
    return 1;
}
