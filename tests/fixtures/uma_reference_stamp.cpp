// Fixed opt-in suite transport. Case status is decoded shell status, never raw wait.
#include <fcntl.h>
#ifdef WOS_UMA_LINKED_AUDIT
#include <dirent.h>

#include "uma_reference_arm.hpp"
#endif
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include "uma_reference_catalog.hpp"

namespace uma_reference {
constexpr uint64_t SECOND = 1'000'000'000;
constexpr uint64_t LOG_LIMIT = 16 * 1024 * 1024;
constexpr size_t BEGIN_LIMIT = 512, END_LIMIT = 768, PARENT_LIMIT = 256, WAIT_LIMIT = 256;
static_assert(234 * (BEGIN_LIMIT + END_LIMIT + PARENT_LIMIT + WAIT_LIMIT) + 32768 < 512 * 1024);

struct Fd {
    int value{-1};
    explicit Fd(int fd = -1) : value(fd) {}
    Fd(const Fd&) = delete;
    auto operator=(const Fd&) -> Fd& = delete;
    ~Fd() {
        if (value >= 0) (void)::close(value);
    }
    auto finish() -> bool {
        int const fd = value;
        value = -1;
        return fd >= 0 && ::close(fd) == 0;
    }
};
struct Context {
    const char* phase{};
    const char* name{};
    const char* nonce{};
    uint64_t deadline{};
};
struct Begin {
    uint64_t helper{}, child{}, start{}, resolution{}, deadline{};
};
struct End {
    uint64_t helper{}, child{}, start{}, end{}, resolution{}, body{};
};
struct Parent {
    uint64_t helper{}, child{}, suite{};
};

auto owner_uid() -> uid_t {
#ifdef WOS_UMA_TIMING_HOST
    return ::getuid();
#else
    return 0;
#endif
}
auto owner_gid() -> gid_t {
#ifdef WOS_UMA_TIMING_HOST
    return ::getgid();
#else
    return 0;
#endif
}
auto decimal(const char* text, uint64_t& value) -> bool {
    value = 0;
    if (!text || !*text || (*text == '0' && text[1] != '\0')) return false;
    for (; *text; ++text) {
        if (*text < '0' || *text > '9' || value > (UINT64_MAX - static_cast<unsigned>(*text - '0')) / 10) return false;
        value = value * 10 + static_cast<unsigned>(*text - '0');
    }
    return true;
}
auto identity() -> bool {
    uid_t r{}, e{}, s{};
    gid_t gr{}, ge{}, gs{};
    if (::getresuid(&r, &e, &s) != 0 || ::getresgid(&gr, &ge, &gs) != 0 || r != owner_uid() || e != owner_uid() || s != owner_uid() ||
        gr != owner_gid() || ge != owner_gid() || gs != owner_gid())
        return false;
#ifndef WOS_UMA_TIMING_HOST
    return ::getgroups(0, nullptr) == 0;
#else
    return true;  // Only the Host credential seam permits the invoking user's supplementary groups.
#endif
}
auto valid_context(const Context& c) -> bool {
    if (!c.phase || (std::strcmp(c.phase, "warmup") && std::strcmp(c.phase, "measure")) || !c.name || !c.nonce ||
        std::strlen(c.nonce) != 32 || c.deadline == 0 || !identity())
        return false;
    for (size_t i = 0; i < 32; ++i)
        if (!(c.nonce[i] >= '0' && c.nonce[i] <= '9') && !(c.nonce[i] >= 'a' && c.nonce[i] <= 'f')) return false;
    for (auto name : CASES)
        if (name == c.name) return true;
    return false;
}
auto clock_value(clockid_t id, uint64_t& value, bool resolution = false) -> bool {
    timespec data{};
    int const rc = resolution ? ::clock_getres(id, &data) : ::clock_gettime(id, &data);
    if (rc != 0 || data.tv_sec < 0 || data.tv_nsec < 0 || data.tv_nsec >= static_cast<long>(SECOND) ||
        static_cast<uint64_t>(data.tv_sec) > (UINT64_MAX - static_cast<uint64_t>(data.tv_nsec)) / SECOND)
        return false;
    value = static_cast<uint64_t>(data.tv_sec) * SECOND + static_cast<uint64_t>(data.tv_nsec);
    return value != 0;
}
auto regular(int fd, mode_t mode, uint64_t limit) -> bool {
    struct stat s{};
    return fd >= 0 && ::fstat(fd, &s) == 0 && S_ISREG(s.st_mode) && s.st_uid == owner_uid() && s.st_gid == owner_gid() && s.st_nlink == 1 &&
           (s.st_mode & 07777) == mode && s.st_size >= 0 && static_cast<uint64_t>(s.st_size) <= limit;
}
auto directory(int fd) -> bool {
    struct stat s{};
    return fd >= 0 && ::fstat(fd, &s) == 0 && S_ISDIR(s.st_mode) && s.st_uid == owner_uid() && s.st_gid == owner_gid() &&
           (s.st_mode & 07777) == 0700;
}
auto open_phase(const Context& c) -> int {
#ifdef WOS_UMA_TIMING_HOST
    const char* root = std::getenv("WOS_UMA_TEST_ROOT");
#else
    const char* root = "/var/lib/uma-reference";
#endif
    if (!root || root[0] != '/' || std::strlen(root) >= 512) return -1;
    Fd current(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    size_t at = 1;
    while (root[at]) {
        size_t first = at;
        while (root[at] && root[at] != '/') ++at;
        if (first == at || at - first > 255) return -1;
        std::array<char, 256> component{};
        std::memcpy(component.data(), root + first, at - first);
        if (!std::strcmp(component.data(), ".") || !std::strcmp(component.data(), "..")) return -1;
        int const next = ::openat(current.value, component.data(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (next < 0) return -1;
        if (!current.finish()) {
            (void)::close(next);
            return -1;
        }
        current.value = next;
        if (root[at]) ++at;
    }
    if (!directory(current.value)) return -1;
    Fd phase(::openat(current.value, c.phase, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!directory(phase.value) || !current.finish()) return -1;
    int const result = phase.value;
    phase.value = -1;
    return result;
}
auto open_child_directory(int phase, const char* name) -> int {
    Fd fd(::openat(phase, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!directory(fd.value)) return -1;
    int const out = fd.value;
    fd.value = -1;
    return out;
}
auto filename(const Context& c, const char* suffix, std::array<char, 96>& out) -> bool {
    int const n = std::snprintf(out.data(), out.size(), "%s.%s", c.name, suffix);
    return n > 0 && static_cast<size_t>(n) < out.size();
}
auto write_all(int fd, const char* data, size_t size) -> bool {
    while (size) {
        auto n = ::write(fd, data, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0 || static_cast<uint64_t>(n) > size) return false;
        data += n;
        size -= static_cast<size_t>(n);
    }
    return true;
}
auto read_record(int dir, const Context& c, const char* suffix, char* out, size_t capacity) -> bool {
    std::array<char, 96> name{};
    if (!filename(c, suffix, name)) return false;
    Fd fd(::openat(dir, name.data(), O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW));
    if (!regular(fd.value, 0600, capacity - 1)) return false;
    size_t size = 0;
    while (size < capacity) {
        auto n = ::read(fd.value, out + size, capacity - size);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 || static_cast<uint64_t>(n) > capacity - size) return false;
        if (n == 0) break;
        size += static_cast<size_t>(n);
    }
    if (size == 0 || size >= capacity || out[size - 1] != '\n' || std::memchr(out, '\0', size)) return false;
    out[size] = '\0';
    return fd.finish();
}
auto save_record(int dir, const Context& c, const char* suffix, const char* data, size_t size, size_t limit) -> bool {
    std::array<char, 96> name{};
    if (!size || size > limit || !filename(c, suffix, name)) return false;
    Fd fd(::openat(dir, name.data(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (!regular(fd.value, 0600, 0)) return false;
    bool const saved = write_all(fd.value, data, size) && ::fsync(fd.value) == 0;
    bool const closed = fd.finish();
    return saved && closed;
}
auto format_begin(char* out, size_t cap, const Context& c, const Begin& b) -> int {
    return std::snprintf(out, cap, "UMA_BEGIN v=1 nonce=%s phase=%s case=%s helper=%llu child=%llu start=%llu res=%llu deadline=%llu\n",
                         c.nonce, c.phase, c.name, (unsigned long long)b.helper, (unsigned long long)b.child, (unsigned long long)b.start,
                         (unsigned long long)b.resolution, (unsigned long long)b.deadline);
}
auto parse_begin(int dir, const Context& c, Begin& b) -> bool {
    std::array<char, BEGIN_LIMIT + 1> bytes{}, canonical{};
    std::array<char, 33> nonce{};
    std::array<char, 8> phase{};
    std::array<char, 64> name{};
    unsigned long long h{}, p{}, s{}, r{}, d{};
    int used = 0;
    if (!read_record(dir, c, "begin", bytes.data(), bytes.size()) ||
        std::sscanf(
            bytes.data(),
            "UMA_BEGIN v=1 nonce=%32[0-9a-f] phase=%7[a-z] case=%63[a-z0-9_] helper=%llu child=%llu start=%llu res=%llu deadline=%llu\n%n",
            nonce.data(), phase.data(), name.data(), &h, &p, &s, &r, &d, &used) != 8 ||
        used != static_cast<int>(std::strlen(bytes.data())) || std::strcmp(nonce.data(), c.nonce) || std::strcmp(phase.data(), c.phase) ||
        std::strcmp(name.data(), c.name))
        return false;
    b = {h, p, s, r, d};
    int const n = format_begin(canonical.data(), canonical.size(), c, b);
    return n > 0 && static_cast<size_t>(n) < canonical.size() && !std::strcmp(canonical.data(), bytes.data()) && b.helper > 1 &&
           b.child > 1 && b.start > 0 && b.resolution > 0 && b.start < b.deadline && b.deadline == c.deadline;
}
auto format_end(char* out, size_t cap, const Context& c, const End& e) -> int {
    return std::snprintf(
        out, cap,
        "UMA_END v=1 nonce=%s phase=%s case=%s helper=%llu child=%llu start=%llu end=%llu res=%llu body=%llu effective=%llu complete=1\n",
        c.nonce, c.phase, c.name, (unsigned long long)e.helper, (unsigned long long)e.child, (unsigned long long)e.start,
        (unsigned long long)e.end, (unsigned long long)e.resolution, (unsigned long long)e.body, (unsigned long long)e.body);
}
auto parse_end(int dir, const Context& c, End& e) -> bool {
    std::array<char, END_LIMIT + 1> bytes{}, canonical{};
    std::array<char, 33> nonce{};
    std::array<char, 8> phase{};
    std::array<char, 64> name{};
    unsigned long long h{}, p{}, s{}, end{}, r{}, body{}, effective{};
    int used = 0;
    if (!read_record(dir, c, "end", bytes.data(), bytes.size()) ||
        std::sscanf(bytes.data(),
                    "UMA_END v=1 nonce=%32[0-9a-f] phase=%7[a-z] case=%63[a-z0-9_] helper=%llu child=%llu start=%llu end=%llu res=%llu "
                    "body=%llu effective=%llu complete=1\n%n",
                    nonce.data(), phase.data(), name.data(), &h, &p, &s, &end, &r, &body, &effective, &used) != 10 ||
        used != static_cast<int>(std::strlen(bytes.data())) || std::strcmp(nonce.data(), c.nonce) || std::strcmp(phase.data(), c.phase) ||
        std::strcmp(name.data(), c.name) || body > 255 || effective != body)
        return false;
    e = {h, p, s, end, r, body};
    int const n = format_end(canonical.data(), canonical.size(), c, e);
    return n > 0 && static_cast<size_t>(n) < canonical.size() && !std::strcmp(canonical.data(), bytes.data());
}
auto format_parent(char* out, size_t cap, const Context& c, const Parent& p) -> int {
    return std::snprintf(out, cap, "UMA_PARENT v=1 nonce=%s phase=%s case=%s helper=%llu child=%llu suite=%llu\n", c.nonce, c.phase, c.name,
                         (unsigned long long)p.helper, (unsigned long long)p.child, (unsigned long long)p.suite);
}
auto parse_parent(int dir, const Context& c, Parent& p) -> bool {
    std::array<char, PARENT_LIMIT + 1> bytes{}, canonical{};
    std::array<char, 33> nonce{};
    std::array<char, 8> phase{};
    std::array<char, 64> name{};
    unsigned long long h{}, child{}, suite{};
    int used = 0;
    if (!read_record(dir, c, "parent", bytes.data(), bytes.size()) ||
        std::sscanf(bytes.data(), "UMA_PARENT v=1 nonce=%32[0-9a-f] phase=%7[a-z] case=%63[a-z0-9_] helper=%llu child=%llu suite=%llu\n%n",
                    nonce.data(), phase.data(), name.data(), &h, &child, &suite, &used) != 6 ||
        used != static_cast<int>(std::strlen(bytes.data())) || std::strcmp(nonce.data(), c.nonce) || std::strcmp(phase.data(), c.phase) ||
        std::strcmp(name.data(), c.name))
        return false;
    p = {h, child, suite};
    int const n = format_parent(canonical.data(), canonical.size(), c, p);
    return n > 0 && static_cast<size_t>(n) < canonical.size() && !std::strcmp(canonical.data(), bytes.data()) && p.helper > 1 &&
           p.child > 1 && p.suite > 1;
}
auto begin(const Context& c, int dir) -> bool {
    Begin b{static_cast<uint64_t>(::getpid()), static_cast<uint64_t>(::getppid()), 0, 0, c.deadline};
    std::array<char, 96> name{};
    if (!filename(c, "begin", name)) return false;
    Fd output(::openat(dir, name.data(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (!regular(output.value, 0600, 0)) return false;
    std::array<char, BEGIN_LIMIT> bytes{};
    if (b.helper <= 1 || b.child <= 1 || !clock_value(CLOCK_MONOTONIC, b.resolution, true) || !clock_value(CLOCK_MONOTONIC, b.start) ||
        b.start >= c.deadline)
        return false;
    int const n = format_begin(bytes.data(), bytes.size(), c, b);
    bool const written = n > 0 && static_cast<size_t>(n) < bytes.size() && write_all(output.value, bytes.data(), static_cast<size_t>(n));
    bool const closed = output.finish();
    return written && closed;
}
auto end(const Context& c, int phase, int dir, uint64_t body) -> bool {
    // Capture before reading BEGIN, opening outputs, formatting, writing or syncing END.
    End e{static_cast<uint64_t>(::getpid()), static_cast<uint64_t>(::getppid()), 0, 0, 0, body};
    if (!clock_value(CLOCK_MONOTONIC, e.end) || !clock_value(CLOCK_MONOTONIC, e.resolution, true) || body > 255) return false;
    Begin b{};
    if (!parse_begin(dir, c, b) || e.child != b.child || e.helper <= 1 || e.end <= b.start || e.end >= c.deadline ||
        e.resolution != b.resolution)
        return false;
    e.start = b.start;
    Fd logs(open_child_directory(phase, "logs"));
    std::array<char, 96> name{};
    if (!filename(c, "log", name)) return false;
    Fd output(::openat(logs.value, name.data(), O_RDWR | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW));
    if (!regular(output.value, 0644, LOG_LIMIT)) return false;
    std::array<char, 96> bname{};
    if (!filename(c, "begin", bname)) return false;
    Fd start(::openat(dir, bname.data(), O_RDWR | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW));
    if (!regular(start.value, 0600, BEGIN_LIMIT)) return false;
    std::array<char, END_LIMIT> bytes{};
    int const n = format_end(bytes.data(), bytes.size(), c, e);
    bool const written = n > 0 && static_cast<size_t>(n) < bytes.size() && ::fsync(start.value) == 0 && ::fsync(output.value) == 0 &&
                         save_record(dir, c, "end", bytes.data(), static_cast<size_t>(n), END_LIMIT);
    bool const a = start.finish(), bclose = output.finish(), l = logs.finish();
    return written && a && bclose && l;
}
auto parent(const Context& c, int dir, uint64_t child, uint64_t suite) -> bool {
    Parent p{static_cast<uint64_t>(::getpid()), child, suite};
    if (p.helper <= 1 || child <= 1 || child > INT64_MAX || suite <= 1 || suite > INT64_MAX ||
        static_cast<uint64_t>(::getppid()) != suite || child == suite)
        return false;
    std::array<char, PARENT_LIMIT> bytes{};
    int const n = format_parent(bytes.data(), bytes.size(), c, p);
    return n > 0 && static_cast<size_t>(n) < bytes.size() &&
           save_record(dir, c, "parent", bytes.data(), static_cast<size_t>(n), PARENT_LIMIT);
}
auto format_wait(char* out, size_t cap, const Context& c, uint64_t child, uint64_t suite, uint64_t marker, uint64_t decoded,
                 uint64_t timeout, bool joined) -> int {
    return std::snprintf(out, cap,
                         "UMA_WAIT v=1 nonce=%s phase=%s case=%s child=%llu suite=%llu marker=%llu decoded=%llu timeout=%llu joined=%u\n",
                         c.nonce, c.phase, c.name, (unsigned long long)child, (unsigned long long)suite, (unsigned long long)marker,
                         (unsigned long long)decoded, (unsigned long long)timeout, joined ? 1U : 0U);
}
auto waited(const Context& c, int dir, uint64_t child, uint64_t suite, uint64_t marker, uint64_t decoded, uint64_t timeout) -> bool {
    if (child <= 1 || child > INT64_MAX || suite <= 1 || suite > INT64_MAX || marker > 255 || decoded > 255 || timeout > 1 ||
        static_cast<uint64_t>(::getppid()) != suite)
        return false;
    Begin b{};
    End e{};
    Parent p{};
    bool const joined = parse_begin(dir, c, b) && parse_end(dir, c, e) && parse_parent(dir, c, p) && b.child == child && e.child == child &&
                        p.child == child && p.suite == suite && b.start == e.start && b.resolution == e.resolution && e.end > b.start &&
                        e.end < c.deadline && e.body == marker && marker == decoded && timeout == 0;
    std::array<char, WAIT_LIMIT> bytes{};
    int const n = format_wait(bytes.data(), bytes.size(), c, child, suite, marker, decoded, timeout, joined);
    bool const saved =
        n > 0 && static_cast<size_t>(n) < bytes.size() && save_record(dir, c, "wait", bytes.data(), static_cast<size_t>(n), WAIT_LIMIT);
    return saved && joined;
}

#ifdef WOS_UMA_LINKED_AUDIT
namespace arm {
namespace {
auto read_line(int fd, char* out, size_t capacity, size_t& size) -> bool {
    size = 0;
    while (size + 1 < capacity) {
        char c{};
        auto n = ::read(fd, &c, 1);
        if (n < 0 && errno == EINTR) continue;
        if (n != 1 || c == '\0' || c == '\r') return false;
        out[size++] = c;
        if (c == '\n') {
            out[size] = 0;
            return true;
        }
    }
    return false;
}
auto record_count(int dir) -> bool {
    Fd clone(::openat(dir, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (clone.value < 0) return false;
    DIR* stream = ::fdopendir(clone.value);
    if (!stream) return false;
    clone.value = -1;
    unsigned count{};
    bool good = true;
    for (;;) {
        errno = 0;
        auto* ent = ::readdir(stream);
        if (!ent) {
            if (errno) good = false;
            break;
        }
        if (!std::strcmp(ent->d_name, ".") || !std::strcmp(ent->d_name, "..")) continue;
        if (++count > CASES.size() * 4) {
            good = false;
            break;
        }
        bool known = false;
        for (auto name : CASES)
            for (const char* kind : std::array<const char*, 4>{"begin", "end", "parent", "wait"}) {
                std::array<char, 96> expected{};
                int n = std::snprintf(expected.data(), expected.size(), "%.*s.%s", int(name.size()), name.data(), kind);
                if (n > 0 && size_t(n) < expected.size() && !std::strcmp(ent->d_name, expected.data())) known = true;
            }
        if (!known) {
            good = false;
            break;
        }
    }
    return ::closedir(stream) == 0 && good && count == CASES.size() * 4;
}
auto wait_record(int dir, const Context& c, uint64_t child, uint64_t suite, uint64_t& bytes) -> bool {
    std::array<char, WAIT_LIMIT + 1> raw{}, canonical{};
    if (!read_record(dir, c, "wait", raw.data(), raw.size())) return false;
    int n = format_wait(canonical.data(), canonical.size(), c, child, suite, 0, 0, 0, true);
    if (n <= 0 || size_t(n) >= canonical.size() || std::strcmp(raw.data(), canonical.data())) return false;
    bytes += std::strlen(raw.data());
    return true;
}
auto record_bytes(int dir, const Context& c, const char* kind, size_t limit, uint64_t& bytes) -> bool {
    std::array<char, END_LIMIT + 1> data{};
    if (limit > END_LIMIT || !read_record(dir, c, kind, data.data(), limit + 1)) return false;
    bytes += std::strlen(data.data());
    return true;
}
auto zero_work(int logs, const char* phase) -> bool {
    Fd fd(::openat(logs, "make_large_file.log", O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK));
    if (!regular(fd.value, 0644, LOG_LIMIT)) return false;
    std::array<char, 256> expected{}, line{};
    int n = std::snprintf(expected.data(), expected.size(), "created 67108864 bytes at %s/%s/work/large-input.bin using /dev/zero\n", ROOT,
                          phase);
    if (n <= 0 || size_t(n) >= expected.size()) return false;
    size_t at{};
    bool long_line = false;
    unsigned matches{};
    // Streaming, bounded by the already-admitted16MiB file extent. A long line
    // is ignored without allocating a payload-sized audit buffer.
    for (uint64_t total = 0; total <= LOG_LIMIT; ++total) {
        char c{};
        auto got = ::read(fd.value, &c, 1);
        if (got < 0 && errno == EINTR) {
            --total;
            continue;
        }
        if (got < 0) return false;
        if (!got) return !at && !long_line && matches == 1 && fd.finish();
        if (c == '\0' || c == '\r') return false;
        if (!long_line && at + 1 < line.size())
            line[at++] = c;
        else
            long_line = true;
        if (c == '\n') {
            if (!long_line) {
                line[at] = 0;
                if (!std::strcmp(line.data(), expected.data())) ++matches;
                if (std::strstr(line.data(), "falling back") || std::strstr(line.data(), "copies of /srv/bee.txt")) return false;
            }
            at = 0;
            long_line = false;
        }
    }
    return false;
}
}  // namespace

auto audit_phase(const char* phase_name, const char* nonce, uint64_t deadline, pid_t suite, PhaseAudit& output) -> bool {
    Context context{phase_name, CASES[0].data(), nonce, deadline};
    if (!valid_context(context) || suite <= 1) return false;
    Fd phase(open_phase(context)), timing(open_child_directory(phase.value, "timing")), logs(open_child_directory(phase.value, "logs"));
    if (phase.value < 0 || timing.value < 0 || logs.value < 0 || !record_count(timing.value)) return false;
    Fd summary(::openat(phase.value, "summary.tsv", O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK));
    if (!regular(summary.value, 0644, 65536)) return false;
    Fd work(open_child_directory(phase.value, "work"));
    if (work.value < 0) return false;
    PhaseAudit result{};
    uint64_t previous{};
    for (auto name : CASES) {
        context.name = name.data();
        Begin b{};
        End e{};
        Parent p{};
        std::array<char, 96> marker{};
        struct stat marker_stat{};
        if (!filename(context, "timeout", marker)) return false;
        if (::fstatat(work.value, marker.data(), &marker_stat, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT) return false;
        if (!parse_begin(timing.value, context, b) || !parse_end(timing.value, context, e) || !parse_parent(timing.value, context, p) ||
            b.child != e.child || b.child != p.child || b.child == uint64_t(suite) || p.suite != uint64_t(suite) || b.child > INT64_MAX ||
            b.helper > INT64_MAX || e.helper > INT64_MAX || p.helper > INT64_MAX || b.helper == b.child || b.helper == uint64_t(suite) ||
            e.helper <= 1 || e.helper == b.child || e.helper == uint64_t(suite) || p.helper == b.child || p.helper == uint64_t(suite) ||
            b.start != e.start || b.start <= previous || e.end <= b.start || e.end >= deadline || b.resolution != e.resolution ||
            e.body != 0 || (result.resolution && result.resolution != b.resolution) ||
            !wait_record(timing.value, context, b.child, p.suite, result.transport) ||
            !record_bytes(timing.value, context, "begin", BEGIN_LIMIT, result.transport) ||
            !record_bytes(timing.value, context, "end", END_LIMIT, result.transport) ||
            !record_bytes(timing.value, context, "parent", PARENT_LIMIT, result.transport))
            return false;
        std::array<char, 256> line{}, prefix{};
        size_t bytes{};
        int length = std::snprintf(prefix.data(), prefix.size(), "%.*s\tPASS\t", int(name.size()), name.data());
        if (length <= 0 || size_t(length) >= prefix.size() || !read_line(summary.value, line.data(), line.size(), bytes) ||
            std::strncmp(line.data(), prefix.data(), size_t(length)))
            return false;
        auto number = line.data() + length;
        auto end = line.data() + bytes;
        if (number >= end || *number < '0' || *number > '9') return false;
        while (number < end && *number >= '0' && *number <= '9') ++number;
        if (number + 2 != end || number[0] != 's' || number[1] != '\n') return false;
        if (!result.first) result.first = b.start;
        result.last = e.end;
        result.resolution = b.resolution;
        previous = e.end;
        ++result.cases;
    }
    char extra{};
    ssize_t got;
    do {
        got = ::read(summary.value, &extra, 1);
    } while (got < 0 && errno == EINTR);
    if (got != 0 || !zero_work(logs.value, phase_name) || !phase_output_bytes(phase_name, result.output) ||
        result.output > 64 * 1024 * 1024 || result.transport + 32768 > 512 * 1024 || !summary.finish() || !work.finish() ||
        !logs.finish() || !timing.finish() || !phase.finish())
        return false;
    result.metrics = 11;
    output = result;
    return true;
}
}  // namespace arm
#endif  // WOS_UMA_LINKED_AUDIT
}  // namespace uma_reference

#ifndef WOS_UMA_LINKED_AUDIT
auto main(int argc, char** argv) -> int {
    using namespace uma_reference;
    if (argc < 6) return 126;
    Context c{argv[2], argv[3], argv[4], 0};
    if (!decimal(argv[5], c.deadline) || !valid_context(c)) return 126;
    Fd phase(open_phase(c));
    Fd dir(open_child_directory(phase.value, "timing"));
    if (phase.value < 0 || dir.value < 0) return 126;
    bool ok = false;
    if (!std::strcmp(argv[1], "--begin") && argc == 6)
        ok = begin(c, dir.value);
    else if (!std::strcmp(argv[1], "--end") && argc == 7) {
        uint64_t body{};
        ok = decimal(argv[6], body) && end(c, phase.value, dir.value, body);
    } else if (!std::strcmp(argv[1], "--parent") && argc == 8) {
        uint64_t child{}, suite{};
        ok = decimal(argv[6], child) && decimal(argv[7], suite) && parent(c, dir.value, child, suite);
    } else if (!std::strcmp(argv[1], "--wait") && argc == 11) {
        uint64_t child{}, suite{}, marker{}, decoded{}, timeout{};
        ok = decimal(argv[6], child) && decimal(argv[7], suite) && decimal(argv[8], marker) && decimal(argv[9], decoded) &&
             decimal(argv[10], timeout) && waited(c, dir.value, child, suite, marker, decoded, timeout);
    }
    bool const a = dir.finish(), b = phase.finish();
    return ok && a && b ? 0 : 126;
}

#endif  // WOS_UMA_LINKED_AUDIT
