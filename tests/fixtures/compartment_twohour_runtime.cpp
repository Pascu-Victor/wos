// Two-hour public compartment qualification fixture; see the companion protocol and provenance notes.
#include <abi/callnums/time.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/init_control.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <abi/compartment.hpp>
#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
namespace spec = ker::abi::compartment;
constexpr char DIRECTORY[] = "/srv/twohour";
constexpr char EXECUTABLE[] = "/usr/libexec/goal39-twohour-runtime";
constexpr char PRIVATE_EXECUTABLE[] = "/app/probe";
constexpr char CLI[] = "/usr/bin/wos-compartment";
constexpr uint64_t MAGIC = 0x5733393248435031ULL, GUARD = 0xa596c387e1d24b6fULL;
constexpr uint64_t VERSION = 1, INTERVAL_US = 5'000'000, TOLERANCE_US = 2'000'000;
constexpr uint64_t OVERLAP_US = 7'200'000'000, GUEST_US = 7'440'000'000;
constexpr uint64_t WITNESS_US = GUEST_US + 30'000'000;
constexpr uint64_t STATUS_SENTINEL = 0xa5a5a5a5ffffffffULL, CONSUMED_STATUS = 0xa5a5a5a500000000ULL;
constexpr uint64_t ROOT_BYTES = 33'554'432;
constexpr unsigned ROUNDS = 1443, LAST_ROUND = 1442, FINISH_ROUND = 1443;
constexpr size_t CONTROL_BYTES = 786432, HOST_BYTES = 763392, PREFIX_BYTES = 24576;
constexpr size_t ROOT_LEDGER = 24576, WORKER_LEDGER = 393984;
constexpr int REPORT_FD = 9;
constexpr int64_t SKIP = INT64_MIN;
using Record = std::array<uint64_t, 64>;
using Small = std::array<uint64_t, 32>;
using Row = std::array<uint64_t, 512>;
using Token = std::array<uint64_t, 8>;
using Text = std::array<char, 8192>;
constexpr std::array<const char*, 8> RETAINED_NAMES{"control-a.bin", "control-b.bin", "host-ledger.bin", "host-operational",
                                                    "marker-a",      "marker-b",      "cli-stdout.bin",  "cli-stderr.bin"};
constexpr std::array<size_t, 8> RETAINED_BYTES{CONTROL_BYTES, CONTROL_BYTES, HOST_BYTES, 64, 64, 64, 8192, 8192};
constexpr std::array<int, 4> PRIVATE_STAT_FDS{0, 1, 2, REPORT_FD};
static_assert(std::endian::native == std::endian::little);
static_assert(sizeof(Record) == 512 && sizeof(Small) == 256 && sizeof(Row) == 4096 && sizeof(Token) == 64);
static_assert(sizeof(spec::Spec) == 408 && sizeof(struct stat) == 144 && sizeof(ker::abi::ClockSleepRequest) == 32);
static_assert(offsetof(struct stat, st_mode) == 24 && offsetof(struct stat, st_size) == 48 && offsetof(struct stat, __unused) == 120);
static_assert(ROUNDS * sizeof(Small) == 369408 && ROOT_LEDGER + ROUNDS * sizeof(Small) == WORKER_LEDGER);
static_assert(WORKER_LEDGER + ROUNDS * sizeof(Small) == 763392 && CONTROL_BYTES - 763392 == 23040);
static_assert(PREFIX_BYTES + ROUNDS * sizeof(Record) == HOST_BYTES);
static_assert(2 * CONTROL_BYTES + HOST_BYTES + 3 * 64 + 2 * 8192 + 2 * 16384 + 4160 + 2 * 512 == 2390784);

struct Owned {
    int64_t pid = -1;
    bool consumed = false;
};
struct Status {
    uint64_t before{};
    uint64_t value = STATUS_SENTINEL;
    uint64_t after{};
};
struct GuardedStat {
    uint64_t before{};
    struct stat value{};
    uint64_t after{};
};
struct GuardedLimit {
    uint64_t before{};
    spec::Limit value{};
    uint64_t after{};
};
static_assert(offsetof(Status, value) == 8 && sizeof(Status) == 24);
static_assert(offsetof(GuardedStat, after) == 152 && sizeof(GuardedStat) == 160);
static_assert(offsetof(GuardedLimit, after) == 24 && sizeof(GuardedLimit) == 32);
auto bits(int64_t value) -> uint64_t { return std::bit_cast<uint64_t>(value); }
auto signed_value(uint64_t value) -> int64_t { return std::bit_cast<int64_t>(value); }
auto process(uint64_t op, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0) -> int64_t {
    return signed_value(syscall(ker::abi::callnums::process, op, a, b, c));
}
auto vfs(uint64_t op, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0, uint64_t d = 0) -> int64_t {
    return signed_value(syscall(ker::abi::callnums::vfs, op, a, b, c, d));
}
auto fcntl_raw(int fd, uint64_t op, uint64_t value = 0) -> int64_t { return vfs(28, static_cast<uint64_t>(fd), op, value); }
auto stat_raw(int fd, struct stat& value) -> int64_t { return vfs(13, static_cast<uint64_t>(fd), reinterpret_cast<uint64_t>(&value)); }
auto now_us() -> uint64_t {
    timespec t{};
    auto const result = signed_value(syscall(ker::abi::callnums::time, uint64_t{1}, reinterpret_cast<uint64_t>(&t), uint64_t{1}));
    if (result != 0 || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= 1'000'000'000) return UINT64_MAX;
    auto const seconds = static_cast<uint64_t>(t.tv_sec), fraction = static_cast<uint64_t>(t.tv_nsec) / 1000;
    if (seconds > (UINT64_MAX - fraction) / 1'000'000) return UINT64_MAX;
    return seconds * 1'000'000 + fraction;
}
auto timely(uint64_t deadline) -> bool {
    auto const n = now_us();
    return n != UINT64_MAX && n < deadline;
}
auto phase(uint64_t deadline, uint64_t duration = TOLERANCE_US) -> uint64_t {
    auto const n = now_us();
    if (n == UINT64_MAX || n >= deadline || n > UINT64_MAX - duration) return 0;
    return std::min(deadline, n + duration);
}
auto sleep_until(uint64_t target, uint64_t deadline) -> bool {
    if (target >= deadline || target > UINT64_MAX / 1000) return false;
    auto const ns = target * 1000;
    ker::abi::ClockSleepRequest request{1, ker::abi::TIME_SLEEP_ABSTIME, static_cast<int64_t>(ns / 1'000'000'000),
                                        static_cast<int64_t>(ns % 1'000'000'000), 0};
    uint64_t previous{};
    while (timely(deadline)) {
        auto const before = now_us();
        if (before == UINT64_MAX || before < previous) return false;
        if (before >= target) return true;
        auto const result = signed_value(syscall(ker::abi::callnums::time, uint64_t{7}, reinterpret_cast<uint64_t>(&request), uint64_t{1}));
        auto const after = now_us();
        if (after == UINT64_MAX || after < before || after >= deadline) return false;
        previous = after;
        if (result != 0 && result != -EINTR) return false;
        if (result == 0 && after >= target) return true;
    }
    return false;
}
auto pause_until(uint64_t deadline) -> bool {
    auto const n = now_us();
    return n != UINT64_MAX && n <= UINT64_MAX - 20'000 && sleep_until(std::min(n + 20'000, deadline - (deadline != 0)), deadline);
}
auto guard(uint64_t nonce, unsigned tree, unsigned role, uint64_t selector) -> uint64_t {
    return GUARD ^ nonce ^ (static_cast<uint64_t>(tree) << 48) ^ (static_cast<uint64_t>(role) << 32) ^ selector;
}
auto commit(uint64_t nonce, unsigned role, uint64_t round, uint64_t sequence) -> uint64_t {
    return MAGIC ^ nonce ^ (static_cast<uint64_t>(role) << 32) ^ round ^ sequence;
}
auto hash(const void* data, size_t count) -> uint64_t {
    auto const* p = static_cast<const uint8_t*>(data);
    uint64_t result = 1469598103934665603ULL;
    for (size_t i = 0; i < count; ++i) result = (result ^ p[i]) * 1099511628211ULL;
    return result;
}
auto number(const char* text, uint64_t& value) -> bool {
    value = 0;
    if (text == nullptr) return false;
    for (size_t i = 0; i < 21; ++i) {
        char const c = text[i];
        if (c == '\0') return i != 0;
        if (c < '0' || c > '9' || value > (UINT64_MAX - static_cast<uint64_t>(c - '0')) / 10) return false;
        value = value * 10 + static_cast<uint64_t>(c - '0');
    }
    return false;
}
auto filename(char* out, size_t capacity, const char* name) -> bool {
    int const n = std::snprintf(out, capacity, "%s/%s", DIRECTORY, name);
    return n > 0 && static_cast<size_t>(n) < capacity;
}
auto transport(int fd, void* data, size_t bytes, size_t offset, bool writing, uint64_t deadline) -> bool {
    if (offset > static_cast<size_t>(INT64_MAX) || bytes > static_cast<size_t>(INT64_MAX) - offset) return false;
    auto* cursor = static_cast<uint8_t*>(data);
    size_t done{};
    auto previous = now_us();
    while (done < bytes) {
        auto const before = now_us();
        if (before == UINT64_MAX || before < previous || before >= deadline) return false;
        auto const raw =
            vfs(writing ? 27U : 26U, static_cast<uint64_t>(fd), reinterpret_cast<uint64_t>(cursor + done), bytes - done, offset + done);
        auto const after = now_us();
        if (after == UINT64_MAX || after < before || after >= deadline) return false;
        previous = after;
        if (raw == -EINTR) continue;
        if (raw <= 0 || static_cast<uint64_t>(raw) > bytes - done) return false;
        done += static_cast<size_t>(raw);
    }
    return true;
}
auto flush(int fd, uint64_t deadline) -> bool { return timely(deadline) && vfs(39, static_cast<uint64_t>(fd)) == 0 && timely(deadline); }
template <size_t N>
auto publish(int fd, std::array<uint64_t, N>& row, size_t offset, size_t terminal, uint64_t value, uint64_t deadline) -> bool {
    if (terminal >= N || value == 0) return false;
    row[terminal] = 0;
    if (!transport(fd, row.data(), sizeof(row), offset, true, deadline) || !flush(fd, deadline)) return false;
    if (!transport(fd, &value, sizeof(value), offset + terminal * 8, true, deadline) || !flush(fd, deadline)) return false;
    row[terminal] = value;
    return true;
}
template <size_t N>
auto double_read(int fd, std::array<uint64_t, N>& row, size_t offset, uint64_t deadline) -> bool {
    std::array<uint64_t, N> second{};
    return transport(fd, row.data(), sizeof(row), offset, false, deadline) &&
           transport(fd, second.data(), sizeof(second), offset, false, deadline) && row == second;
}
auto save_file(const char* name, const void* data, size_t bytes, uint64_t deadline) -> bool {
    char path[160]{};
    if (!filename(path, sizeof(path), name) || !timely(deadline)) return false;
    int const fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    bool const written = transport(fd, const_cast<void*>(data), bytes, 0, true, deadline);
    bool const flushed = flush(fd, deadline), closed = close(fd) == 0;
    return written && flushed && closed;
}
auto zero_file(const char* name, size_t bytes, uint64_t deadline) -> int {
    char path[160]{};
    if (!filename(path, sizeof(path), name) || !timely(deadline)) return -1;
    int const fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    if (ftruncate(fd, static_cast<off_t>(bytes)) == 0 && flush(fd, deadline)) return fd;
    (void)close(fd);
    return -1;
}
auto regular_stat(const struct stat& s, uint64_t bytes, uint64_t dev, uint64_t ino) -> bool {
    bool good = S_ISREG(s.st_mode) && (s.st_mode & 07777U) == 0600 && s.st_uid == 100 && s.st_gid == 100 && s.st_nlink == 1 &&
                s.st_rdev == 0 && s.st_size >= 0 && static_cast<uint64_t>(s.st_size) == bytes && s.st_dev == dev && s.st_ino == ino &&
                ino != 0 && s.st_blksize == 4096 && s.st_blocks >= 0 && s.st_blocks % 8 == 0;
    for (auto const& t : {s.st_atim, s.st_mtim, s.st_ctim}) good = t.tv_sec >= 0 && t.tv_nsec >= 0 && t.tv_nsec < 1'000'000'000 && good;
    for (auto v : s.__unused) good = v == 0 && good;
    uint32_t padding{};
    std::memcpy(&padding, reinterpret_cast<const uint8_t*>(&s) + 36, 4);
    return good && padding == 0;
}
auto guarded_wait(Owned& child, Status& s, uint64_t sentinel) -> int64_t {
    s = {sentinel, STATUS_SENTINEL, ~sentinel};
    auto const raw =
        child.pid > 0 && !child.consumed ? process(2, static_cast<uint64_t>(child.pid), reinterpret_cast<uint64_t>(&s.value), 1) : SKIP;
    // Consume ownership before any clock or output work; never later signal a reaped PID.
    if (raw == child.pid && child.pid > 0) child.consumed = true;
    return raw;
}
auto untouched(Status const& s, uint64_t sentinel) -> bool {
    return s.before == sentinel && s.after == ~sentinel && s.value == STATUS_SENTINEL;
}
auto consume(Owned& child, Status& s, int64_t& raw, uint64_t sentinel, uint64_t deadline, uint64_t& start, uint64_t& end,
             uint64_t* polls = nullptr) -> bool {
    start = now_us();
    raw = SKIP;
    while (child.pid > 0 && !child.consumed && timely(deadline)) {
        raw = guarded_wait(child, s, sentinel);
        if (raw == 0 && polls != nullptr) ++*polls;
        if (child.consumed) break;
        if ((raw != 0 && raw != -EINTR) || !untouched(s, sentinel) || !pause_until(deadline)) break;
    }
    end = now_us();
    return child.consumed && raw == child.pid && s.before == sentinel && s.after == ~sentinel && s.value == CONSUMED_STATUS &&
           start != UINT64_MAX && end >= start && end < deadline;
}
void cleanup(Owned& child) {
    if (child.pid <= 0 || child.consumed) return;
    (void)process(8, static_cast<uint64_t>(child.pid), SIGKILL);
    Status status{};
    int64_t raw{};
    uint64_t start{}, end{};
    (void)consume(child, status, raw, GUARD, phase(UINT64_MAX, 5'000'000), start, end);
}
[[noreturn]] void exit_child(int code) {
    (void)process(0, static_cast<uint64_t>(code));
    for (;;) {
    }
}
auto marker_token(uint64_t nonce, unsigned tree) -> Token {
    return {MAGIC,
            VERSION,
            nonce,
            tree,
            tree == 1 ? 0x4141414141414141ULL : 0x4242424242424242ULL,
            guard(nonce, tree, 0, 11),
            0,
            commit(nonce, 0, 0, 1)};
}
auto operation_token(uint64_t nonce, unsigned round) -> Token {
    return {MAGIC, VERSION, nonce, 5, round, round + 1U, guard(nonce, 0, 5, 10 + round), commit(nonce, 5, round, round + 1U)};
}
auto cpu_result(uint64_t nonce, unsigned role, unsigned round) -> uint64_t {
    uint64_t result = (nonce ^ (static_cast<uint64_t>(role) << 32) ^ round) | 1U;
    for (uint64_t i = 0; i < 256; ++i) result = (result * 6364136223846793005ULL + 1442695040888963407ULL) ^ i;
    return result;
}
struct Private {
    Record request{};
    uint64_t nonce{}, deadline{}, anchor{};
    unsigned tree{}, role{};
    int64_t fork_result{};
    Owned worker{};
};
auto sample_offset(unsigned role, unsigned round) -> size_t {
    return (role == 1 || role == 3 ? ROOT_LEDGER : WORKER_LEDGER) + static_cast<size_t>(round) * sizeof(Small);
}
auto command_offset(unsigned role) -> size_t { return role == 1 || role == 3 ? 512 : 1024; }
auto progress_offset(unsigned role) -> size_t { return role == 1 || role == 3 ? 1536 : 2048; }
auto attestation_offset(unsigned role, unsigned phase_number) -> size_t {
    return (phase_number == 0 ? 4096U : 12288U) + (role == 1 || role == 3 ? 0U : 4096U);
}
auto request_valid(Private const& p) -> bool {
    auto const& r = p.request;
    bool good = r[0] == MAGIC && r[1] == VERSION && r[2] == p.nonce && r[3] == p.tree && r[4] > 2 && r[5] == p.deadline && r[10] == 1 &&
                r[11] == 0 && r[12] == 256 && r[14] == 64 && r[16] != 0 && r[17] == CONTROL_BYTES && r[18] == 2 && r[19] == p.tree &&
                r[20] == ROUNDS && r[21] == INTERVAL_US && r[22] == OVERLAP_US && r[23] == TOLERANCE_US && r[24] == ROOT_BYTES &&
                r[25] == sizeof(Record) && r[26] != 0 && r[28] == 0 && r[29] == 0 && r[30] == guard(p.nonce, p.tree, 0, 0) &&
                r[31] == commit(p.nonce, 0, 0, 1);
    for (size_t i = 6; i < 10; ++i) good = r[i] == 100 && good;
    for (size_t i = 0; i < 16; ++i) {
        uint64_t const wanted = i == 7 ? 256 : UINT64_MAX;
        good = r[32 + 2 * i] == wanted && r[33 + 2 * i] == wanted && good;
    }
    auto const marker = marker_token(p.nonce, p.tree);
    return good && r[13] == hash(marker.data(), sizeof(marker));
}
auto read_private_marker(Private const& p, int64_t& opened, int64_t& read, int64_t& closed, uint64_t& actual_hash, uint64_t deadline)
    -> bool {
    Token marker{};
    opened = timely(deadline) ? vfs(0, reinterpret_cast<uint64_t>("/same/name"), O_RDONLY | O_CLOEXEC, 0) : SKIP;
    read = opened >= 0 ? vfs(26, static_cast<uint64_t>(opened), reinterpret_cast<uint64_t>(marker.data()), sizeof(marker), 0) : SKIP;
    closed = opened >= 0 ? vfs(3, static_cast<uint64_t>(opened)) : SKIP;
    actual_hash = hash(marker.data(), sizeof(marker));
    return opened >= 0 && read == 64 && closed == 0 && marker == marker_token(p.nonce, p.tree) && actual_hash == p.request[13] &&
           timely(deadline);
}
auto fill_limits(Row& row, Record const& request, uint64_t nonce, unsigned tree, unsigned role) -> bool {
    bool good = true;
    for (size_t i = 0; i < 16; ++i) {
        auto const g = guard(nonce, tree, role, 0x100 + i);
        GuardedLimit value{g, {}, ~g};
        auto const raw = process(48, i, reinterpret_cast<uint64_t>(&value.value), 1);
        size_t const at = 32 + i * 6;
        row[at] = bits(raw);
        row[at + 1] = value.before;
        row[at + 2] = value.value.soft;
        row[at + 3] = value.value.hard;
        row[at + 4] = value.after;
        row[at + 5] = i;
        good = raw == 0 && value.before == g && value.after == ~g && value.value.soft == request[32 + i * 2] &&
               value.value.hard == request[33 + i * 2] && good;
    }
    return good;
}
auto fill_stat(Row& row, size_t slot, int fd, uint64_t nonce, unsigned tree, unsigned role, bool present, unsigned access, uint64_t bytes,
               uint64_t dev, uint64_t ino) -> bool {
    auto const g = guard(nonce, tree, role, 0x200 + static_cast<uint64_t>(fd));
    GuardedStat value{};
    value.before = g;
    value.after = ~g;
    std::memset(&value.value, 0xa5, sizeof(value.value));
    auto const flags = fcntl_raw(fd, 1), mode = fcntl_raw(fd, 3), raw = stat_raw(fd, value.value);
    size_t const at = 384 + slot * 24;
    row[at] = static_cast<uint64_t>(fd);
    row[at + 1] = bits(flags);
    row[at + 2] = bits(mode);
    row[at + 3] = bits(raw);
    row[at + 4] = value.before;
    std::memcpy(row.data() + at + 5, &value.value, sizeof(value.value));
    row[at + 23] = value.after;
    bool good = value.before == g && value.after == ~g;
    if (present)
        return good && flags >= 0 && mode >= 0 && (static_cast<uint64_t>(mode) & 3U) == access && raw == 0 &&
               regular_stat(value.value, bytes, dev, ino);
    std::array<uint8_t, 144> empty{};
    empty.fill(0xa5);
    return good && flags == -EBADF && mode == -EBADF && raw == -EBADF && std::memcmp(&value.value, empty.data(), empty.size()) == 0;
}
auto attest_private(Private const& p, unsigned phase_number, Row& row) -> bool {
    row = {};
    row[0] = MAGIC;
    row[1] = VERSION;
    row[2] = p.nonce;
    row[3] = p.role;
    row[4] = phase_number;
    row[5] = p.tree;
    row[6] = bits(process(3));
    row[7] = bits(process(4));
    for (size_t i = 0; i < 4; ++i) row[8 + i] = bits(process(10 + i));
    row[12] = bits(process(36, 39));
    row[13] = bits(process(36, 0x574f5301));
    row[14] = bits(process(36, 0x574f5304));
    row[15] = now_us();
    row[16] = ROOT_BYTES;
    row[17] = p.request[15];
    row[18] = p.request[16];
    row[19] = CONTROL_BYTES;
    row[20] = 2;
    row[24] = bits(p.fork_result);
    row[25] = row[15];
    row[27] = REPORT_FD;
    bool const root = p.role == 1 || p.role == 3;
    bool good =
        request_valid(p) && (root ? row[6] == 1 && row[7] == 0 && p.fork_result > 1 : row[6] > 1 && row[7] == 1 && p.fork_result == 0);
    for (size_t i = 8; i < 12; ++i) good = row[i] == 100 && good;
    bool const limits = fill_limits(row, p.request, p.nonce, p.tree, p.role);
    good = row[12] == 1 && row[13] == 0 && row[14] == 256 && limits && good;
    int64_t opened{}, read{}, closed{};
    good = read_private_marker(p, opened, read, closed, row[21], phase(p.deadline)) && good;
    row[22] = bits(read);
    auto const forbidden = vfs(0, reinterpret_cast<uint64_t>("/srv/twohour/host-operational"), O_RDONLY | O_CLOEXEC, 0);
    row[23] = bits(forbidden);
    if (forbidden >= 0) (void)vfs(3, static_cast<uint64_t>(forbidden));
    good = forbidden == -ENOENT && good;
    for (int fd = 0; fd < 256; ++fd) {
        auto const value = fcntl_raw(fd, 1);
        row[128 + static_cast<size_t>(fd)] = bits(value);
        good = value == (fd == REPORT_FD ? 0 : -EBADF) && good;
    }
    for (size_t i = 0; i < PRIVATE_STAT_FDS.size(); ++i)
        good =
            fill_stat(row, i, PRIVATE_STAT_FDS[i], p.nonce, p.tree, p.role, i == 3, 2, CONTROL_BYTES, p.request[15], p.request[16]) && good;
    row[26] = now_us();
    good = row[25] != UINT64_MAX && row[26] >= row[25] && timely(p.deadline) && good;
    row[29] = good ? 0 : 1;
    row[30] = guard(p.nonce, p.tree, p.role, 5 + phase_number);
    row[31] = commit(p.nonce, p.role, 0, phase_number + 1U);
    return publish(REPORT_FD, row, attestation_offset(p.role, phase_number), 511, row[31], phase(p.deadline)) && good;
}
auto await_command(Private& p, unsigned round, Record& command) -> bool {
    uint64_t const sequence = round + 1U;
    while (timely(p.deadline)) {
        if (!double_read(REPORT_FD, command, command_offset(p.role), phase(p.deadline))) {
            if (!pause_until(p.deadline)) return false;
            continue;
        }
        if (command[63] == 0 || command[5] < sequence) {
            if (!pause_until(p.deadline)) return false;
            continue;
        }
        bool good = command[0] == MAGIC && command[1] == VERSION && command[2] == p.nonce && command[3] == p.tree && command[4] == p.role &&
                    command[5] == sequence &&
                    command[6] == (round == 0              ? 1U
                                   : round == FINISH_ROUND ? 3U
                                                           : 2U) &&
                    command[7] == round && command[10] == p.deadline && command[11] == p.request[30] &&
                    command[62] == guard(p.nonce, p.tree, p.role, 1 + round) && command[63] == commit(p.nonce, p.role, round, sequence);
        for (size_t i = 12; i < 62; ++i) good = command[i] == 0 && good;
        if (round == 0)
            good = command[9] == 0 && command[8] >= now_us() && good;
        else {
            if (round == 1) p.anchor = command[9];
            good =
                p.anchor != 0 && command[9] == p.anchor && p.anchor <= UINT64_MAX - static_cast<uint64_t>(LAST_ROUND) * INTERVAL_US && good;
            good = command[8] == (round == FINISH_ROUND ? 0 : p.anchor + static_cast<uint64_t>(round) * INTERVAL_US) && good;
        }
        return good;
    }
    return false;
}
auto private_sample(Private& p, unsigned round, uint64_t target) -> bool {
    Small row{};
    bool good = target <= UINT64_MAX - TOLERANCE_US && sleep_until(target, std::min(p.deadline, target + TOLERANCE_US));
    uint64_t const deadline = good ? std::min(p.deadline, target + TOLERANCE_US) : p.deadline;
    row[0] = MAGIC;
    row[1] = VERSION;
    row[2] = p.nonce;
    row[3] = p.role;
    row[4] = round;
    row[5] = round + 1U;
    row[6] = now_us();
    row[8] = bits(process(3));
    row[9] = bits(process(4));
    for (size_t i = 0; i < 4; ++i) row[10 + i] = bits(process(10 + i));
    row[14] = bits(process(36, 39));
    row[15] = bits(process(36, 0x574f5301));
    row[16] = bits(process(36, 0x574f5304));
    int64_t opened{}, read{}, closed{};
    good = read_private_marker(p, opened, read, closed, row[20], deadline) && good;
    row[17] = bits(opened);
    row[18] = bits(read);
    row[19] = bits(closed);
    row[21] = 256;
    row[22] = cpu_result(p.nonce, p.role, round);
    auto const g = guard(p.nonce, p.tree, p.role, 0x300);
    Status status{g, STATUS_SENTINEL, ~g};
    auto const waited = p.role == 1 || p.role == 3 ? guarded_wait(p.worker, status, g) : SKIP;
    row[23] = bits(waited);
    row[24] = status.before;
    row[25] = status.value;
    row[26] = status.after;
    good = waited == (p.role == 1 || p.role == 3 ? 0 : SKIP) && untouched(status, g) && good;
    bool const root = p.role == 1 || p.role == 3;
    good = (root ? row[8] == 1 && row[9] == 0 : row[8] > 1 && row[9] == 1) && row[14] == 1 && row[15] == 0 && row[16] == 256 && good;
    for (size_t i = 10; i < 14; ++i) good = row[i] == 100 && good;
    row[7] = now_us();
    row[28] = target;
    good = row[6] != UINT64_MAX && row[6] >= target && row[7] >= row[6] && row[7] < deadline && good;
    row[27] = good ? 0 : 1;
    row[30] = guard(p.nonce, p.tree, p.role, 3 + round);
    uint64_t const token = commit(p.nonce, p.role, round, round + 1U);
    bool const saved = publish(REPORT_FD, row, sample_offset(p.role, round), 31, token, deadline);
    Record progress{};
    std::copy(row.begin(), row.end(), progress.begin());
    bool const progress_saved = saved && publish(REPORT_FD, progress, progress_offset(p.role), 63, token, deadline);
    return good && saved && progress_saved;
}
auto private_finish(Private& p) -> bool {
    bool good = true;
    Record lifecycle{};
    lifecycle[0] = MAGIC;
    lifecycle[1] = VERSION;
    lifecycle[2] = p.nonce;
    lifecycle[3] = p.role;
    lifecycle[4] = p.tree;
    auto const g = guard(p.nonce, p.tree, p.role, 0x300);
    Status status{g, STATUS_SENTINEL, ~g};
    int64_t raw = SKIP;
    uint64_t start = now_us(), end = start;
    if (p.role == 1 || p.role == 3) good = consume(p.worker, status, raw, g, phase(p.deadline, 10'000'000), start, end);
    lifecycle[5] = p.role == 1 || p.role == 3 ? bits(p.worker.pid) : 0;
    lifecycle[6] = bits(raw);
    lifecycle[7] = status.before;
    lifecycle[8] = status.value;
    lifecycle[9] = status.after;
    lifecycle[10] = 1;
    lifecycle[11] = start;
    lifecycle[12] = end;
    Row final{};
    good = attest_private(p, 1, final) && good;
    auto const flushed = vfs(39, REPORT_FD);
    lifecycle[13] = bits(flushed);
    lifecycle[14] = 0;
    good = flushed == 0 && timely(p.deadline) && good;
    lifecycle[15] = good ? 0 : 1;
    bool const saved =
        publish(REPORT_FD, lifecycle, p.role == 1 || p.role == 3 ? 20480 : 20992, 63, commit(p.nonce, p.role, 0, 2), phase(p.deadline));
    bool const last_flush = flush(REPORT_FD, phase(p.deadline)), closed = vfs(3, REPORT_FD) == 0;
    return good && saved && last_flush && closed;
}
auto private_tree(unsigned tree, uint64_t nonce, uint64_t host_pid, uint64_t deadline) -> int {
    Private p{};
    p.tree = tree;
    p.role = tree == 1 ? 1U : 3U;
    p.nonce = nonce;
    p.deadline = deadline;
    if (!double_read(REPORT_FD, p.request, 0, phase(deadline)) || !request_valid(p) || p.request[4] != host_pid || process(3) != 1 ||
        process(4) != 0)
        return 70;
    p.fork_result = process(5);
    if (p.fork_result > 0) p.worker.pid = p.fork_result;
    if (p.fork_result < 0 || p.fork_result == 1) return 70;
    if (p.fork_result == 0) ++p.role;
    Row initial{};
    bool good = attest_private(p, 0, initial);
    for (unsigned round = 0; good && round < ROUNDS; ++round) {
        Record command{};
        good = await_command(p, round, command) && private_sample(p, round, command[8]);
    }
    Record finish{};
    good = good && await_command(p, FINISH_ROUND, finish);
    if (good) good = private_finish(p);
    if (!good && (p.role == 1 || p.role == 3)) cleanup(p.worker);
    if (!good) (void)vfs(3, REPORT_FD);
    return good ? 0 : 70;
}
struct Host {
    std::array<int, 8> fd{};
    std::array<struct stat, 8> identity{};
    std::array<Record, 2> requests{};
    Record request{}, anchor{};
    Row initial{}, final{}, summary{};
    std::array<Row, 4> private_initial{};
    std::array<uint64_t, 4> physical{}, local{};
    Owned a{}, cli{};
    uint64_t nonce{}, start{}, deadline{}, common{}, end{};
    uint64_t stdout_length{}, stderr_length{}, start_hash{}, end_hash{};
    Host() { fd.fill(-1); }
    ~Host() {
        for (int f : fd)
            if (f >= 0) (void)close(f);
    }
    auto close_handles() -> bool {
        bool good = true;
        for (auto& f : fd)
            if (f >= 0) {
                int const owned = f;
                f = -1;
                good = close(owned) == 0 && good;
            }
        return good;
    }
};
auto read_status(uint64_t pid, std::array<char, 4096>& output, size_t& length, uint64_t deadline) -> bool {
    output = {};
    length = 0;
    char name[80]{};
    int const n = std::snprintf(name, sizeof(name), "/proc/%llu/status", static_cast<unsigned long long>(pid));
    if (n <= 0 || static_cast<size_t>(n) >= sizeof(name) || !timely(deadline)) return false;
    int const fd = open(name, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    bool good = true;
    for (;;) {
        char overflow{};
        void* target = length < 4095 ? static_cast<void*>(output.data() + length) : &overflow;
        size_t const capacity = length < 4095 ? 4095 - length : 1;
        auto const raw = vfs(1, static_cast<uint64_t>(fd), reinterpret_cast<uint64_t>(target), capacity);
        if (!timely(deadline)) {
            good = false;
            break;
        }
        if (raw == -EINTR) continue;
        if (raw == 0) break;
        if (raw < 0 || static_cast<uint64_t>(raw) > capacity || length == 4095) {
            good = false;
            break;
        }
        length += static_cast<size_t>(raw);
    }
    good = close(fd) == 0 && good;
    return good && length > 0 && length <= 4095 && std::memchr(output.data(), '\0', length) == nullptr;
}
auto field(std::array<char, 4096> const& text, const char* key, uint64_t& value) -> bool {
    size_t const prefix = std::strlen(key);
    unsigned matches{};
    const char* found{};
    for (const char* p = text.data(); *p != '\0'; ++p)
        if ((p == text.data() || p[-1] == '\n') && std::strncmp(p, key, prefix) == 0) {
            found = p + prefix;
            ++matches;
        }
    if (matches != 1 || found == nullptr) return false;
    while (*found == ' ' || *found == '\t') ++found;
    char number_text[24]{};
    size_t count{};
    while (found[count] >= '0' && found[count] <= '9' && count < 20) {
        number_text[count] = found[count];
        ++count;
    }
    if (count == 0 || (found[count] != '\n' && found[count] != '\t' && found[count] != ' ')) return false;
    return number(number_text, value);
}
auto uid_status(std::array<char, 4096> const& text) -> bool {
    auto const* p = std::strstr(text.data(), "\nUid:\t");
    if (p == nullptr) return false;
    unsigned a{}, b{}, c{}, d{};
    int used{};
    return std::sscanf(p + 1, "Uid:\t%u\t%u\t%u\t%u%n", &a, &b, &c, &d, &used) == 4 && a == 100 && b == 100 && c == 100 && d == 100 &&
           p[1 + used] == '\n' && std::strstr(p + 1, "\nUid:") == nullptr;
}
auto valid_status(std::array<char, 4096> const& text, uint64_t pid, uint64_t parent) -> bool {
    uint64_t observed{}, tgid{}, ppid{};
    return std::strncmp(text.data(), "Name:\tprobe\n", 12) == 0 && field(text, "Pid:", observed) && field(text, "Tgid:", tgid) &&
           field(text, "PPid:", ppid) && observed == pid && tgid == pid && ppid == parent && uid_status(text);
}
auto discover_workers(Host& h, uint64_t deadline) -> bool {
    while (timely(deadline)) {
        std::array<uint64_t, 2> selected{};
        std::array<unsigned, 2> matches{};
        DIR* directory = opendir("/proc");
        if (directory == nullptr) return false;
        bool bounded = true;
        unsigned entries{};
        int error{};
        for (;;) {
            errno = 0;
            auto* entry = readdir(directory);
            if (entry == nullptr) {
                error = errno;
                break;
            }
            if (++entries > 4096 || !timely(deadline)) {
                bounded = false;
                break;
            }
            uint64_t pid{};
            if (!number(entry->d_name, pid) || pid <= 2) continue;
            std::array<char, 4096> text{};
            size_t length{};
            uint64_t parent{};
            if (!read_status(pid, text, length, deadline) || !field(text, "PPid:", parent)) continue;
            for (size_t i = 0; i < 2; ++i)
                if (parent == h.physical[i * 2] && valid_status(text, pid, parent)) {
                    selected[i] = pid;
                    ++matches[i];
                }
        }
        bounded = closedir(directory) == 0 && error == 0 && bounded;
        if (!bounded || matches[0] > 1 || matches[1] > 1) return false;
        if (matches[0] == 1 && matches[1] == 1) {
            h.physical[1] = selected[0];
            h.physical[3] = selected[1];
            for (size_t i = 0; i < 4; ++i) {
                if (h.physical[i] <= 2) return false;
                for (size_t j = 0; j < i; ++j)
                    if (h.physical[i] == h.physical[j]) return false;
            }
            return true;
        }
        if (!pause_until(deadline)) return false;
    }
    return false;
}
auto capture_proc(Host& h, bool final, uint64_t deadline) -> bool {
    char path[160]{};
    if (!filename(path, sizeof(path), final ? "proc-final.bin" : "proc-initial.bin")) return false;
    int const fd = open(path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) return false;
    bool good = true;
    for (size_t i = 0; i < 4; ++i) {
        std::array<char, 4096> text{};
        size_t length{};
        uint64_t const parent = i == 0 ? h.request[4] : i == 2 ? static_cast<uint64_t>(h.cli.pid) : h.physical[i - 1];
        bool const read = read_status(h.physical[i], text, length, deadline);
        h.summary[(final ? 26 : 22) + i] = length;
        h.summary[(final ? 76 : 72) + i] = hash(text.data(), text.size());
        good = read && valid_status(text, h.physical[i], parent) && good;
        good = transport(fd, text.data(), text.size(), i * text.size(), true, deadline) && good;
    }
    good = flush(fd, deadline) && good;
    return close(fd) == 0 && good;
}
auto manifest(Host& h, Text const& buffer) -> bool {
    auto const* terminator = static_cast<const char*>(std::memchr(buffer.data(), '\0', buffer.size()));
    if (terminator == nullptr || terminator == buffer.data()) return false;
    size_t const bytes = static_cast<size_t>(terminator - buffer.data());
    if (buffer[bytes - 1] != '\n') return false;
    for (size_t i = bytes; i < buffer.size(); ++i)
        if (buffer[i] != '\0') return false;
    std::array<std::array<char, 512>, 23> lines{};
    size_t line{}, column{};
    for (size_t i = 0; i < bytes; ++i) {
        if (buffer[i] == '\n') {
            if (line >= lines.size() || column == 0) return false;
            ++line;
            column = 0;
        } else {
            if (line >= lines.size() || column >= 511) return false;
            lines[line][column++] = buffer[i];
        }
    }
    if (line != lines.size() || column != 0) return false;
    long long pid{};
    unsigned long long ceiling{}, root_bytes{};
    char executable[128]{}, network[32]{};
    int used{};
    if (std::sscanf(lines[0].data(), "compartment spec=1 host-pid=%lld exec=%127s network=%31s ceiling=0x%llx root-bytes=%llu%n", &pid,
                    executable, network, &ceiling, &root_bytes, &used) != 5 ||
        lines[0][static_cast<size_t>(used)] != '\0' || pid <= 2 || std::strcmp(executable, PRIVATE_EXECUTABLE) != 0 ||
        std::strcmp(network, "denied") != 0 || ceiling != 0 || root_bytes != ROOT_BYTES)
        return false;
    std::array<const char*, 4> sources{EXECUTABLE, "/lib/ld.so", "/usr/lib/libc.so", "/srv/twohour/marker-b"};
    std::array<const char*, 4> destinations{PRIVATE_EXECUTABLE, "/lib/ld.so", "/usr/lib/libc.so", "/same/name"};
    std::array<int, 4> descriptors{};
    for (size_t i = 0; i < 4; ++i) {
        char source[128]{}, destination[128]{};
        unsigned mode{};
        used = 0;
        if (std::sscanf(lines[i + 1].data(), "asset fd=%d source=%127s destination=%127s mode=%o copied%n", &descriptors[i], source,
                        destination, &mode, &used) != 4 ||
            lines[i + 1][static_cast<size_t>(used)] != '\0' || descriptors[i] < 0 || descriptors[i] >= 256 || descriptors[i] == 1 ||
            descriptors[i] == 2 || descriptors[i] == 9 || std::strcmp(source, sources[i]) != 0 ||
            std::strcmp(destination, destinations[i]) != 0 || mode != (i < 2 ? 0755U : 0644U))
            return false;
        for (size_t j = 0; j < i; ++j)
            if (descriptors[i] == descriptors[j]) return false;
    }
    if (std::strcmp(lines[5].data(), "grant source-fd=9 destination-fd=9 shared-file rights-and-offset") != 0 ||
        std::strcmp(lines[6].data(), "arguments=6 environment=1 dumpable=0 service-privileges=0x0") != 0)
        return false;
    for (size_t i = 0; i < 16; ++i) {
        size_t resource{};
        unsigned long long soft{}, hard{};
        used = 0;
        if (std::sscanf(lines[7 + i].data(), "limit id=%zu soft=%llu hard=%llu%n", &resource, &soft, &hard, &used) != 3 ||
            lines[7 + i][static_cast<size_t>(used)] != '\0' || resource != i || soft != h.request[32 + 2 * i] ||
            hard != h.request[33 + 2 * i])
            return false;
    }
    if (h.physical[2] != 0 && h.physical[2] != static_cast<uint64_t>(pid)) return false;
    h.physical[2] = static_cast<uint64_t>(pid);
    h.stdout_length = bytes;
    return true;
}
auto await_manifest(Host& h, uint64_t deadline) -> bool {
    while (timely(deadline)) {
        Text text{};
        if (transport(h.fd[6], text.data(), text.size(), 0, false, deadline) && manifest(h, text)) return true;
        Status status{};
        auto const g = guard(h.nonce, 0, 5, 0x301);
        if (guarded_wait(h.cli, status, g) != 0 || !untouched(status, g) || !pause_until(deadline)) return false;
    }
    return false;
}
auto text(const char* value) -> spec::String { return {reinterpret_cast<uint64_t>(value), std::strlen(value)}; }
auto create_a(Host& h) -> bool {
    std::array<const char*, 4> sources{EXECUTABLE, "/lib/ld.so", "/usr/lib/libc.so", "/srv/twohour/marker-a"};
    std::array<const char*, 4> destinations{PRIVATE_EXECUTABLE, "/lib/ld.so", "/usr/lib/libc.so", "/same/name"};
    std::array<spec::Asset, 4> assets{};
    bool good = true;
    for (size_t i = 0; i < 4; ++i) {
        assets[i].source_fd = open(sources[i], O_RDONLY | O_CLOEXEC);
        assets[i].mode = i < 2 ? 0755U : 0644U;
        assets[i].destination = text(destinations[i]);
        struct stat st{};
        good = assets[i].source_fd >= 0 && stat_raw(assets[i].source_fd, st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0 && good;
    }
    char nonce[24]{}, pid[24]{}, deadline[24]{};
    std::snprintf(nonce, sizeof(nonce), "%llu", static_cast<unsigned long long>(h.nonce));
    std::snprintf(pid, sizeof(pid), "%llu", static_cast<unsigned long long>(h.request[4]));
    std::snprintf(deadline, sizeof(deadline), "%llu", static_cast<unsigned long long>(h.deadline));
    std::array args{text(PRIVATE_EXECUTABLE), text("--tree"), text("A"), text(nonce), text(pid), text(deadline)};
    std::array environment{text("LD_LIBRARY_PATH=/usr/lib")};
    spec::Descriptor descriptor{h.fd[0], REPORT_FD, 0};
    spec::Spec request{};
    for (size_t i = 0; i < 16; ++i) request.limits[i] = {h.request[32 + 2 * i], h.request[33 + 2 * i]};
    request.executable = args[0];
    request.assets = reinterpret_cast<uint64_t>(assets.data());
    request.asset_count = assets.size();
    request.descriptors = reinterpret_cast<uint64_t>(&descriptor);
    request.descriptor_count = 1;
    request.argv = reinterpret_cast<uint64_t>(args.data());
    request.argc = args.size();
    request.envp = reinterpret_cast<uint64_t>(environment.data());
    request.envc = environment.size();
    auto const raw = good ? process(54, reinterpret_cast<uint64_t>(&request)) : SKIP;
    if (raw > 0) h.a.pid = raw;
    h.summary[16] = bits(raw);
    h.physical[0] = raw > 0 ? static_cast<uint64_t>(raw) : 0;
    for (auto const& a : assets)
        if (a.source_fd >= 0) good = close(a.source_fd) == 0 && good;
    return raw > 2 && good;
}
auto create_b(Host& h) -> bool {
    char out[160]{}, err[160]{};
    if (!filename(out, sizeof(out), RETAINED_NAMES[6]) || !filename(err, sizeof(err), RETAINED_NAMES[7])) return false;
    int const stdout_writer = open(out, O_WRONLY | O_CLOEXEC), stderr_writer = open(err, O_WRONLY | O_CLOEXEC);
    bool good =
        stdout_writer >= 0 && stderr_writer >= 0 && h.fd[1] != stdout_writer && h.fd[1] != stderr_writer && stdout_writer != stderr_writer;
    for (int scratch : {200, 201, 202})
        good = fcntl_raw(scratch, 1) == -EBADF && h.fd[1] != scratch && stdout_writer != scratch && stderr_writer != scratch && good;
    char nonce[24]{}, pid[24]{}, deadline[24]{};
    std::snprintf(nonce, sizeof(nonce), "%llu", static_cast<unsigned long long>(h.nonce));
    std::snprintf(pid, sizeof(pid), "%llu", static_cast<unsigned long long>(h.request[4]));
    std::snprintf(deadline, sizeof(deadline), "%llu", static_cast<unsigned long long>(h.deadline));
    std::array<const char*, 39> args{};
    std::array<const char*, 38> literals{CLI,
                                         "--asset",
                                         EXECUTABLE,
                                         PRIVATE_EXECUTABLE,
                                         "755",
                                         "--asset",
                                         "/lib/ld.so",
                                         "/lib/ld.so",
                                         "755",
                                         "--asset",
                                         "/usr/lib/libc.so",
                                         "/usr/lib/libc.so",
                                         "644",
                                         "--asset",
                                         "/srv/twohour/marker-b",
                                         "/same/name",
                                         "644",
                                         "--fd",
                                         "9",
                                         "9",
                                         "--env",
                                         "LD_LIBRARY_PATH=/usr/lib",
                                         "--root-bytes",
                                         "33554432",
                                         "--ceiling",
                                         "0",
                                         "--service-privileges",
                                         "0",
                                         "--",
                                         PRIVATE_EXECUTABLE,
                                         "--tree",
                                         "B",
                                         nonce,
                                         pid,
                                         deadline,
                                         nullptr,
                                         nullptr,
                                         nullptr};
    // Copy exact argv until first nullptr, leaving a complete null-terminated bounded vector.
    std::copy(literals.begin(), literals.end(), args.begin());
    std::array<const char*, 2> environment{"LD_LIBRARY_PATH=/usr/lib", nullptr};
    auto const raw = good ? process(5) : SKIP;
    if (raw == 0) {
        auto duplicate = [](int source, int destination) {
            return vfs(16, static_cast<uint64_t>(source), static_cast<uint64_t>(destination), 0) == destination;
        };
        bool copied = duplicate(h.fd[1], 200) && duplicate(stdout_writer, 201) && duplicate(stderr_writer, 202);
        copied = copied && duplicate(200, 9) && duplicate(201, 1) && duplicate(202, 2);
        for (int keep : {1, 2, 9}) copied = fcntl_raw(keep, 2, 0) == 0 && copied;
        for (int f = 0; f < 256; ++f) {
            if (f == 1 || f == 2 || f == 9) continue;
            auto const flags = fcntl_raw(f, 1);
            if (flags != -EBADF) copied = flags >= 0 && vfs(3, static_cast<uint64_t>(f)) == 0 && copied;
        }
        if (copied)
            (void)process(24, reinterpret_cast<uint64_t>(CLI), reinterpret_cast<uint64_t>(args.data()),
                          reinterpret_cast<uint64_t>(environment.data()));
        exit_child(71);
    }
    if (raw > 0) h.cli.pid = raw;
    h.summary[17] = bits(raw);
    if (stdout_writer >= 0) good = close(stdout_writer) == 0 && good;
    if (stderr_writer >= 0) good = close(stderr_writer) == 0 && good;
    return raw > 2 && good;
}
auto prepare_host(Host& h) -> bool {
    h.start = now_us();
    auto const pid = process(3), parent = process(4);
    if (h.start == UINT64_MAX || h.start > UINT64_MAX - GUEST_US || pid <= 2 || parent <= 0) return false;
    h.deadline = h.start + GUEST_US;
    h.nonce = (h.start ^ (static_cast<uint64_t>(pid) << 32) ^ GUARD) | 1U;
    h.request[0] = MAGIC;
    h.request[1] = VERSION;
    h.request[2] = h.nonce;
    h.request[3] = 5;
    h.request[4] = static_cast<uint64_t>(pid);
    h.request[5] = static_cast<uint64_t>(parent);
    h.request[6] = h.deadline;
    h.request[7] = OVERLAP_US;
    h.request[8] = h.start;
    h.request[9] = bits(process(36, 39));
    h.request[10] = bits(process(36, 0x574f5301));
    h.request[11] = bits(process(36, 0x574f5304));
    bool good = h.request[9] == 1 && h.request[10] == 0 && h.request[11] == 256;
    for (size_t i = 0; i < 4; ++i) {
        h.request[12 + i] = bits(process(10 + i));
        good = h.request[12 + i] == 100 && good;
    }
    h.request[16] = spec::VERSION;
    h.request[17] = sizeof(spec::Spec);
    h.request[18] = CONTROL_BYTES;
    h.request[19] = HOST_BYTES;
    h.request[20] = ROUNDS;
    h.request[21] = INTERVAL_US;
    h.request[22] = OVERLAP_US;
    h.request[23] = TOLERANCE_US;
    h.request[24] = 20ULL * 1024 * 1024 * 1024;
    h.request[25] = 40ULL * 1024 * 1024 * 1024;
    h.request[26] = 3 * 1024 * 1024;
    h.request[27] = 3;
    h.request[30] = guard(h.nonce, 0, 5, 0);
    struct stat directory{};
    good = stat(DIRECTORY, &directory) == 0 && S_ISDIR(directory.st_mode) && (directory.st_mode & 07777U) == 0700 &&
           directory.st_uid == 100 && directory.st_gid == 100 && good;
    struct stat console{};
    good = fcntl_raw(2, 1) == 0 && fcntl_raw(2, 3) >= 0 && (fcntl_raw(2, 3) & 3) == 1 && stat_raw(2, console) == 0 &&
           S_ISCHR(console.st_mode) && good;
    for (int f = 0; f < 256; ++f) {
        if (f == 2) continue;
        auto const value = fcntl_raw(f, 1);
        if (value != -EBADF) good = value >= 0 && close(f) == 0 && good;
    }
    for (size_t i = 0; i < 16; ++i) {
        spec::Limit limit{};
        auto const result = process(48, i, reinterpret_cast<uint64_t>(&limit), 1);
        h.request[32 + 2 * i] = limit.soft;
        h.request[33 + 2 * i] = limit.hard;
        uint64_t const wanted = i == 7 ? 256 : UINT64_MAX;
        good = result == 0 && limit.soft == wanted && limit.hard == wanted && good;
    }
    if (!good) return false;
    for (size_t i = 0; i < 8; ++i) {
        h.fd[i] = zero_file(RETAINED_NAMES[i], RETAINED_BYTES[i], phase(h.deadline));
        if (h.fd[i] < 0) return false;
        if (i == 4 || i == 5) {
            auto marker = marker_token(h.nonce, static_cast<unsigned>(i - 3));
            if (!transport(h.fd[i], marker.data(), sizeof(marker), 0, true, phase(h.deadline)) || !flush(h.fd[i], phase(h.deadline)))
                return false;
        }
        if (i >= 4) {
            if (close(h.fd[i]) != 0) {
                h.fd[i] = -1;
                return false;
            }
            h.fd[i] = -1;
            char path[160]{};
            if (!filename(path, sizeof(path), RETAINED_NAMES[i])) return false;
            h.fd[i] = open(path, O_RDONLY | O_CLOEXEC);
            if (h.fd[i] < 0) return false;
        }
        if (stat_raw(h.fd[i], h.identity[i]) != 0 ||
            !regular_stat(h.identity[i], RETAINED_BYTES[i], h.identity[i].st_dev, h.identity[i].st_ino))
            return false;
        for (size_t j = 0; j < i; ++j)
            if (h.fd[i] == h.fd[j] || (h.identity[i].st_dev == h.identity[j].st_dev && h.identity[i].st_ino == h.identity[j].st_ino))
                return false;
    }
    for (const char* name : {"proc-initial.bin", "proc-final.bin"}) {
        int const fd = zero_file(name, 16384, phase(h.deadline));
        if (fd < 0 || close(fd) != 0) return false;
    }
    if (!publish(h.fd[2], h.request, 0, 31, commit(h.nonce, 5, 0, 1), phase(h.deadline))) return false;
    for (size_t i = 0; i < 2; ++i) {
        auto& r = h.requests[i];
        unsigned const tree = static_cast<unsigned>(i + 1);
        r[0] = MAGIC;
        r[1] = VERSION;
        r[2] = h.nonce;
        r[3] = tree;
        r[4] = static_cast<uint64_t>(pid);
        r[5] = h.deadline;
        for (size_t n = 6; n < 10; ++n) r[n] = 100;
        r[10] = 1;
        r[11] = 0;
        r[12] = 256;
        auto const marker = marker_token(h.nonce, tree);
        r[13] = hash(marker.data(), sizeof(marker));
        r[14] = 64;
        r[15] = h.identity[i].st_dev;
        r[16] = h.identity[i].st_ino;
        r[17] = CONTROL_BYTES;
        r[18] = 2;
        r[19] = tree;
        r[20] = ROUNDS;
        r[21] = INTERVAL_US;
        r[22] = OVERLAP_US;
        r[23] = TOLERANCE_US;
        r[24] = ROOT_BYTES;
        r[25] = sizeof(Record);
        r[26] = h.identity[4 + i].st_ino;
        r[27] = h.identity[4 + i].st_dev;
        r[30] = guard(h.nonce, tree, 0, 0);
        for (size_t n = 32; n < 64; ++n) r[n] = h.request[n];
        if (!publish(h.fd[i], r, 0, 31, commit(h.nonce, 0, 0, 1), phase(h.deadline))) return false;
    }
    h.summary[0] = MAGIC;
    h.summary[1] = VERSION;
    h.summary[2] = h.nonce;
    h.summary[3] = 5;
    h.summary[4] = 1;
    h.summary[7] = h.start;
    return true;
}
auto attest_host(Host& h, unsigned phase_number, Row& row) -> bool {
    row = {};
    row[0] = MAGIC;
    row[1] = VERSION;
    row[2] = h.nonce;
    row[3] = 5;
    row[4] = phase_number;
    row[6] = bits(process(3));
    row[7] = bits(process(4));
    for (size_t i = 0; i < 4; ++i) row[8 + i] = bits(process(10 + i));
    row[12] = bits(process(36, 39));
    row[13] = bits(process(36, 0x574f5301));
    row[14] = bits(process(36, 0x574f5304));
    row[15] = now_us();
    row[16] = h.summary[16];
    row[17] = h.summary[17];
    row[18] = h.physical[2];
    row[19] = h.physical[1];
    row[20] = h.physical[3];
    row[25] = row[15];
    row[27] = 8;
    bool const limits = fill_limits(row, h.request, h.nonce, 0, 5);
    bool good = row[6] == h.request[4] && row[7] == h.request[5] && row[12] == 1 && row[13] == 0 && row[14] == 256 && limits;
    for (size_t i = 8; i < 12; ++i) good = row[i] == 100 && good;
    for (size_t i = 0; i < 2; ++i) {
        Token marker{};
        auto const raw = vfs(26, static_cast<uint64_t>(h.fd[4 + i]), reinterpret_cast<uint64_t>(marker.data()), sizeof(marker), 0);
        row[22 + i] = bits(raw);
        good = raw == 64 && marker == marker_token(h.nonce, static_cast<unsigned>(i + 1)) && good;
    }
    for (int f = 0; f < 256; ++f) {
        auto const flags = fcntl_raw(f, 1);
        row[128 + static_cast<size_t>(f)] = bits(flags);
        auto const found = std::find(h.fd.begin(), h.fd.end(), f);
        int64_t const expected = f == 2 ? 0 : found != h.fd.end() ? 1 : -EBADF;
        if (flags >= 0) ++row[21];
        good = flags == expected && good;
        if (phase_number == 1) good = row[128 + static_cast<size_t>(f)] == h.initial[128 + static_cast<size_t>(f)] && good;
    }
    for (size_t i = 0; i < 4; ++i)
        good = fill_stat(row, i, h.fd[i], h.nonce, 0, 5, true, 2, RETAINED_BYTES[i], h.identity[i].st_dev, h.identity[i].st_ino) && good;
    for (size_t i = 0; i < 16; ++i) row[480 + i] = i < 8 ? static_cast<uint64_t>(h.fd[i]) : bits(SKIP);
    row[26] = now_us();
    good = row[21] == 9 && row[26] >= row[25] && timely(h.deadline) && good;
    row[29] = good ? 0 : 1;
    row[30] = guard(h.nonce, 0, 5, 5 + phase_number);
    row[31] = commit(h.nonce, 5, 0, phase_number + 1U);
    return publish(h.fd[2], row, phase_number == 0 ? 4096 : 8192, 511, row[31], phase(h.deadline)) && good;
}
auto validate_private_row(Host const& h, size_t index, unsigned phase_number, Row const& row) -> bool {
    unsigned const role = static_cast<unsigned>(index + 1), tree = static_cast<unsigned>(index / 2 + 1);
    auto const& request = h.requests[index / 2];
    uint64_t const local = index % 2 == 0 ? 1 : h.local[index];
    bool good = row[0] == MAGIC && row[1] == VERSION && row[2] == h.nonce && row[3] == role && row[4] == phase_number && row[5] == tree &&
                row[6] == local && row[7] == (index % 2 == 0 ? 0U : 1U) && row[12] == 1 && row[13] == 0 && row[14] == 256 &&
                row[15] != UINT64_MAX && row[16] == ROOT_BYTES && row[17] == request[15] && row[18] == request[16] &&
                row[19] == CONTROL_BYTES && row[20] == 2 && row[21] == request[13] && row[22] == 64 && signed_value(row[23]) == -ENOENT &&
                row[25] == row[15] && row[26] >= row[25] && row[26] < h.deadline && row[27] == 9 && row[28] == 0 && row[29] == 0 &&
                row[30] == guard(h.nonce, tree, role, 5 + phase_number) && row[31] == commit(h.nonce, role, 0, phase_number + 1U) &&
                row[511] == row[31];
    good = row[24] == (index % 2 == 0 ? h.local[index + 1] : 0) && good;
    for (size_t i = 8; i < 12; ++i) good = row[i] == 100 && good;
    for (size_t i = 0; i < 16; ++i) {
        size_t const at = 32 + 6 * i;
        auto const g = guard(h.nonce, tree, role, 0x100 + i);
        good = signed_value(row[at]) == 0 && row[at + 1] == g && row[at + 2] == request[32 + 2 * i] && row[at + 3] == request[33 + 2 * i] &&
               row[at + 4] == ~g && row[at + 5] == i && good;
    }
    for (int f = 0; f < 256; ++f) good = signed_value(row[128 + static_cast<size_t>(f)]) == (f == REPORT_FD ? 0 : -EBADF) && good;
    for (size_t slot = 0; slot < 4; ++slot) {
        int const fd = PRIVATE_STAT_FDS[slot];
        size_t const at = 384 + slot * 24;
        auto const g = guard(h.nonce, tree, role, 0x200 + static_cast<uint64_t>(fd));
        struct stat value{};
        std::memcpy(&value, row.data() + at + 5, sizeof(value));
        good = row[at] == static_cast<uint64_t>(fd) && row[at + 4] == g && row[at + 23] == ~g && good;
        if (slot == 3)
            good = row[at + 1] == 0 && signed_value(row[at + 2]) >= 0 && (row[at + 2] & 3U) == 2 && row[at + 3] == 0 &&
                   regular_stat(value, CONTROL_BYTES, request[15], request[16]) && good;
        else {
            std::array<uint8_t, 144> untouched_bytes{};
            untouched_bytes.fill(0xa5);
            good = signed_value(row[at + 1]) == -EBADF && signed_value(row[at + 2]) == -EBADF && signed_value(row[at + 3]) == -EBADF &&
                   std::memcmp(&value, untouched_bytes.data(), untouched_bytes.size()) == 0 && good;
        }
    }
    for (size_t i = 480; i < 511; ++i) good = row[i] == 0 && good;
    return good;
}
auto await_initial(Host& h, uint64_t deadline) -> bool {
    while (timely(deadline)) {
        bool ready = true;
        for (size_t i = 0; i < 4; ++i) {
            auto& row = h.private_initial[i];
            bool const read = double_read(h.fd[i / 2], row, attestation_offset(static_cast<unsigned>(i + 1), 0), deadline);
            ready = read && row[511] != 0 && ready;
        }
        if (ready) {
            h.local[0] = h.local[2] = 1;
            h.local[1] = h.private_initial[0][24];
            h.local[3] = h.private_initial[2][24];
            if (h.local[1] <= 1 || h.local[3] <= 1) return false;
            for (size_t i = 0; i < 4; ++i) {
                if (!validate_private_row(h, i, 0, h.private_initial[i])) return false;
                h.summary[64 + i] = hash(h.private_initial[i].data(), sizeof(Row));
            }
            return true;
        }
        if (!pause_until(deadline)) return false;
    }
    return false;
}
auto make_command(Host const& h, unsigned role, unsigned round, uint64_t target) -> Record {
    unsigned const tree = (role + 1) / 2;
    Record row{};
    row[0] = MAGIC;
    row[1] = VERSION;
    row[2] = h.nonce;
    row[3] = tree;
    row[4] = role;
    row[5] = round + 1U;
    row[6] = round == 0 ? 1 : round == FINISH_ROUND ? 3 : 2;
    row[7] = round;
    row[8] = target;
    row[9] = h.common;
    row[10] = h.deadline;
    row[11] = h.requests[tree - 1][30];
    row[62] = guard(h.nonce, tree, role, 1 + round);
    row[63] = commit(h.nonce, role, round, round + 1U);
    return row;
}
auto send_command(Host const& h, unsigned role, unsigned round, uint64_t target, uint64_t deadline) -> bool {
    unsigned const tree = (role + 1) / 2;
    auto row = make_command(h, role, round, target);
    return publish(h.fd[tree - 1], row, command_offset(role), 63, commit(h.nonce, role, round, round + 1U), deadline);
}
auto validate_small(Host const& h, size_t index, unsigned round, uint64_t target, Small const& row) -> bool {
    unsigned const role = static_cast<unsigned>(index + 1), tree = static_cast<unsigned>(index / 2 + 1);
    auto const g = guard(h.nonce, tree, role, 0x300);
    bool good = row[0] == MAGIC && row[1] == VERSION && row[2] == h.nonce && row[3] == role && row[4] == round && row[5] == round + 1U &&
                row[6] >= target && row[6] != UINT64_MAX && row[7] >= row[6] && row[7] < target + TOLERANCE_US &&
                row[8] == h.local[index] && row[9] == (index % 2 == 0 ? 0U : 1U) && row[14] == 1 && row[15] == 0 && row[16] == 256 &&
                signed_value(row[17]) >= 0 && row[17] < 256 && row[17] != REPORT_FD && row[18] == 64 && row[19] == 0 &&
                row[20] == h.requests[index / 2][13] && row[21] == 256 && row[22] == cpu_result(h.nonce, role, round) &&
                signed_value(row[23]) == (index % 2 == 0 ? 0 : SKIP) && row[24] == g && row[25] == STATUS_SENTINEL && row[26] == ~g &&
                row[27] == 0 && row[28] == target && row[29] == 0 && row[30] == guard(h.nonce, tree, role, 3 + round) &&
                row[31] == commit(h.nonce, role, round, round + 1U);
    for (size_t i = 10; i < 14; ++i) good = row[i] == 100 && good;
    return good;
}
auto await_samples(Host const& h, unsigned round, uint64_t target, std::array<Small, 4>& rows, uint64_t deadline) -> bool {
    while (timely(deadline)) {
        bool ready = true;
        for (size_t i = 0; i < 4; ++i) {
            unsigned const role = static_cast<unsigned>(i + 1);
            Record progress{};
            bool const read = double_read(h.fd[i / 2], progress, progress_offset(role), deadline);
            if (!read || progress[5] < round + 1U || progress[63] == 0) {
                ready = false;
                continue;
            }
            if (progress[5] != round + 1U || progress[63] != commit(h.nonce, role, round, round + 1U)) return false;
            for (size_t j = 32; j < 63; ++j)
                if (progress[j] != 0) return false;
            if (!double_read(h.fd[i / 2], rows[i], sample_offset(role, round), deadline) || !validate_small(h, i, round, target, rows[i]) ||
                !std::equal(rows[i].begin(), rows[i].end(), progress.begin()))
                return false;
        }
        if (ready) return true;
        if (!pause_until(deadline)) return false;
    }
    return false;
}
auto host_sample(Host& h, unsigned round, uint64_t target, Record& row, std::array<Small, 4>& private_rows) -> bool {
    row = {};
    if (target > UINT64_MAX - TOLERANCE_US) return false;
    auto const deadline = std::min(h.deadline, target + TOLERANCE_US);
    bool good = sleep_until(target, deadline);
    row[0] = MAGIC;
    row[1] = VERSION;
    row[2] = h.nonce;
    row[3] = 5;
    row[4] = round;
    row[5] = round + 1U;
    row[6] = now_us();
    row[8] = bits(process(3));
    row[9] = bits(process(4));
    for (size_t i = 0; i < 4; ++i) row[10 + i] = bits(process(10 + i));
    row[14] = target;
    auto expected = operation_token(h.nonce, round);
    Token actual{};
    row[15] = bits(vfs(27, static_cast<uint64_t>(h.fd[3]), reinterpret_cast<uint64_t>(expected.data()), sizeof(expected), 0));
    row[16] = bits(vfs(26, static_cast<uint64_t>(h.fd[3]), reinterpret_cast<uint64_t>(actual.data()), sizeof(actual), 0));
    row[17] = bits(vfs(39, static_cast<uint64_t>(h.fd[3])));
    row[18] = hash(actual.data(), sizeof(actual));
    row[19] = hash(expected.data(), sizeof(expected));
    good = row[15] == 64 && row[16] == 64 && row[17] == 0 && actual == expected && row[18] == row[19] && good;
    for (size_t i = 0; i < 4; ++i) {
        row[20 + i] = bits(process(8, h.physical[i], 0));
        row[44 + i] = h.physical[i];
        good = row[20 + i] == 0 && good;
    }
    for (size_t i = 0; i < 2; ++i) {
        auto const g = guard(h.nonce, 0, 5, 0x300 + i);
        Status status{};
        auto const raw = guarded_wait(i == 0 ? h.a : h.cli, status, g);
        size_t const at = i == 0 ? 24 : 30;
        row[at] = static_cast<uint64_t>(i == 0 ? h.a.pid : h.cli.pid);
        row[at + 1] = bits(raw);
        row[at + 2] = status.before;
        row[at + 3] = status.value;
        row[at + 4] = status.after;
        row[at + 5] = 1;
        good = raw == 0 && untouched(status, g) && good;
    }
    auto const opened = vfs(0, reinterpret_cast<uint64_t>("/srv/twohour/host-operational"), O_RDONLY | O_CLOEXEC, 0);
    Token fresh{};
    auto const read =
        opened >= 0 ? vfs(26, static_cast<uint64_t>(opened), reinterpret_cast<uint64_t>(fresh.data()), sizeof(fresh), 0) : SKIP;
    auto const closed = opened >= 0 ? vfs(3, static_cast<uint64_t>(opened)) : SKIP;
    row[48] = bits(opened);
    row[49] = bits(read);
    row[50] = bits(closed);
    row[51] = hash(fresh.data(), sizeof(fresh));
    good = opened >= 0 && read == 64 && closed == 0 && fresh == expected && row[51] == row[19] && good;
    good = await_samples(h, round, target, private_rows, deadline) && good;
    for (size_t i = 0; i < 4; ++i) {
        row[36 + i * 2] = private_rows[i][5];
        row[37 + i * 2] = private_rows[i][7];
    }
    row[7] = now_us();
    good = row[6] != UINT64_MAX && row[6] >= target && row[7] >= row[6] && row[7] < deadline && row[8] == h.request[4] &&
           row[9] == h.request[5] && good;
    for (size_t i = 10; i < 14; ++i) good = row[i] == 100 && good;
    row[61] = guard(h.nonce, 0, 5, 4 + round);
    row[62] = good ? 0 : 1;
    bool const saved = publish(h.fd[2], row, PREFIX_BYTES + static_cast<size_t>(round) * sizeof(Record), 63,
                               commit(h.nonce, 5, round, round + 1U), deadline);
    return good && saved;
}
auto serial_start(Host& h, uint64_t round0_target, Record const& host_row, std::array<Small, 4> const& rows) -> bool {
    auto& anchor = h.anchor;
    anchor[0] = MAGIC;
    anchor[1] = VERSION;
    anchor[2] = h.nonce;
    anchor[3] = 5;
    anchor[4] = 1;
    anchor[5] = h.common;
    anchor[6] = round0_target;
    for (size_t i = 0; i < 4; ++i) {
        anchor[8 + i] = rows[i][7];
        anchor[13 + i] = hash(rows[i].data(), sizeof(Small));
    }
    anchor[12] = host_row[7];
    anchor[17] = hash(host_row.data(), sizeof(Record));
    auto const g = guard(h.nonce, 0, 5, 0x202);
    GuardedStat value{};
    value.before = g;
    value.after = ~g;
    std::memset(&value.value, 0xa5, sizeof(value.value));
    anchor[18] = 2;
    anchor[19] = bits(fcntl_raw(2, 1));
    anchor[20] = bits(fcntl_raw(2, 3));
    anchor[21] = bits(stat_raw(2, value.value));
    anchor[22] = value.before;
    std::memcpy(anchor.data() + 23, &value.value, sizeof(value.value));
    anchor[41] = value.after;
    bool good = anchor[19] == 0 && signed_value(anchor[20]) >= 0 && (anchor[20] & 3U) == 1 && anchor[21] == 0 && value.before == g &&
                value.after == ~g && S_ISCHR(value.value.st_mode) && value.value.st_ino > 0 && value.value.st_nlink == 1 &&
                (value.value.st_mode & 07777U) == 0666 && value.value.st_uid == 0 && value.value.st_gid == 0 && value.value.st_rdev == 0 &&
                value.value.st_size == 0 && value.value.st_blksize == 4096 && value.value.st_blocks == 0;
    uint32_t padding{};
    std::memcpy(&padding, reinterpret_cast<const uint8_t*>(&value.value) + 36, 4);
    good = padding == 0 && good;
    for (auto item : value.value.__unused) good = item == 0 && good;
    for (auto const& t : {value.value.st_atim, value.value.st_mtim, value.value.st_ctim})
        good = t.tv_sec >= 0 && t.tv_nsec >= 0 && t.tv_nsec < 1'000'000'000 && good;
    char line[128]{};
    int const count = std::snprintf(line, sizeof(line), "WOS39_2H_START nonce=%016llx S=%llu rounds=1443 roles=5\n",
                                    static_cast<unsigned long long>(h.nonce), static_cast<unsigned long long>(h.common));
    auto const raw = good && count > 0 && static_cast<size_t>(count) < sizeof(line)
                         ? vfs(2, 2, reinterpret_cast<uint64_t>(line), static_cast<uint64_t>(count), 0)
                         : SKIP;
    anchor[42] = bits(raw);
    anchor[43] = count > 0 ? static_cast<uint64_t>(count) : 0;
    anchor[44] = count > 0 && static_cast<size_t>(count) < sizeof(line) ? hash(line, static_cast<size_t>(count)) : 0;
    h.start_hash = anchor[44];
    anchor[45] = 8;
    anchor[7] = now_us();
    anchor[62] = guard(h.nonce, 0, 5, 9);
    return good && raw == count && count > 0 && publish(h.fd[2], anchor, 512, 63, commit(h.nonce, 5, 0, 1), phase(h.deadline));
}
auto serial_end(Host& h) -> bool {
    char line[128]{};
    int const count = std::snprintf(line, sizeof(line), "WOS39_2H_END nonce=%016llx E=%llu rounds=1443 roles=5\n",
                                    static_cast<unsigned long long>(h.nonce), static_cast<unsigned long long>(h.end));
    auto const raw = count > 0 && static_cast<size_t>(count) < sizeof(line)
                         ? vfs(2, 2, reinterpret_cast<uint64_t>(line), static_cast<uint64_t>(count), 0)
                         : SKIP;
    h.end_hash = count > 0 && static_cast<size_t>(count) < sizeof(line) ? hash(line, static_cast<size_t>(count)) : 0;
    h.summary[52] = 2;
    h.summary[53] = bits(raw);
    h.summary[54] = count > 0 ? static_cast<uint64_t>(count) : 0;
    h.summary[55] = h.start_hash;
    h.summary[56] = h.end_hash;
    return count > 0 && raw == count && timely(h.deadline);
}
auto committed_event(const char* owner, uint64_t nonce, uint64_t sequence, uint64_t deadline) -> bool {
    // Completion events are separate from the two clock-proof markers.
    char line[128]{};
    int const count = std::snprintf(line, sizeof(line), "WOS39_2H_%s_COMMITTED nonce=%016llx sequence=%llu\n", owner,
                                    static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(sequence));
    struct stat console{};
    auto const flags = fcntl_raw(2, 1), mode = fcntl_raw(2, 3);
    bool const capable = flags == 0 && mode >= 0 && (mode & 3) == 1 && stat_raw(2, console) == 0 && S_ISCHR(console.st_mode);
    auto const raw = capable && count > 0 && static_cast<size_t>(count) < sizeof(line) && timely(deadline)
                         ? vfs(2, 2, reinterpret_cast<uint64_t>(line), static_cast<uint64_t>(count), 0)
                         : SKIP;
    return count > 0 && static_cast<size_t>(count) < sizeof(line) && raw == count && timely(deadline);
}
auto validate_host_sample(Host const& h, unsigned round, uint64_t target, Record const& row, std::array<Small, 4> const& private_rows)
    -> bool {
    bool good = row[0] == MAGIC && row[1] == VERSION && row[2] == h.nonce && row[3] == 5 && row[4] == round && row[5] == round + 1U &&
                row[6] >= target && row[6] != UINT64_MAX && row[7] >= row[6] && row[7] < target + TOLERANCE_US && row[8] == h.request[4] &&
                row[9] == h.request[5] && row[14] == target && row[15] == 64 && row[16] == 64 && row[17] == 0 && row[18] == row[19] &&
                signed_value(row[48]) >= 0 && row[48] < 256 && row[49] == 64 && row[50] == 0 && row[51] == row[19] &&
                row[61] == guard(h.nonce, 0, 5, 4 + round) && row[62] == 0 && row[63] == commit(h.nonce, 5, round, round + 1U);
    auto const token = operation_token(h.nonce, round);
    good = row[19] == hash(token.data(), sizeof(token)) && good;
    for (size_t i = 10; i < 14; ++i) good = row[i] == 100 && good;
    for (size_t i = 0; i < 4; ++i)
        good = row[20 + i] == 0 && row[44 + i] == h.physical[i] && row[36 + 2 * i] == private_rows[i][5] &&
               row[37 + 2 * i] == private_rows[i][7] && good;
    for (size_t i = 0; i < 2; ++i) {
        size_t const at = i == 0 ? 24 : 30;
        auto const g = guard(h.nonce, 0, 5, 0x300 + i);
        good = row[at] == static_cast<uint64_t>(i == 0 ? h.a.pid : h.cli.pid) && row[at + 1] == 0 && row[at + 2] == g &&
               row[at + 3] == STATUS_SENTINEL && row[at + 4] == ~g && row[at + 5] == 1 && good;
    }
    for (size_t i = 52; i < 61; ++i) good = row[i] == 0 && good;
    return good;
}
auto validate_lifecycle(Host const& h, size_t index, Record const& row) -> bool {
    unsigned const role = static_cast<unsigned>(index + 1), tree = static_cast<unsigned>(index / 2 + 1);
    auto const g = guard(h.nonce, tree, role, 0x300);
    bool const root = index % 2 == 0;
    bool good = row[0] == MAGIC && row[1] == VERSION && row[2] == h.nonce && row[3] == role && row[4] == tree &&
                row[5] == (root ? h.local[index + 1] : 0) &&
                signed_value(row[6]) == (root ? static_cast<int64_t>(h.local[index + 1]) : SKIP) && row[7] == g &&
                row[8] == (root ? CONSUMED_STATUS : STATUS_SENTINEL) && row[9] == ~g && row[10] == 1 && row[11] != UINT64_MAX &&
                row[12] >= row[11] && row[12] < h.deadline && row[13] == 0 && row[14] == 0 && row[15] == 0 &&
                row[63] == commit(h.nonce, role, 0, 2);
    for (size_t i = 16; i < 63; ++i) good = row[i] == 0 && good;
    return good;
}
auto zero_region(int fd, size_t offset, size_t bytes, uint64_t deadline) -> bool {
    std::array<uint8_t, 512> buffer{};
    if (offset > SIZE_MAX - bytes) return false;
    while (bytes != 0) {
        auto const count = std::min(bytes, buffer.size());
        if (!transport(fd, buffer.data(), count, offset, false, deadline)) return false;
        for (size_t i = 0; i < count; ++i)
            if (buffer[i] != 0) return false;
        offset += count;
        bytes -= count;
    }
    return true;
}
auto whole_campaign(Host& h, uint64_t round0_target) -> bool {
    Record saved{};
    if (!double_read(h.fd[2], saved, 0, phase(h.deadline)) || saved != h.request || !double_read(h.fd[2], saved, 512, phase(h.deadline)) ||
        saved != h.anchor || !zero_region(h.fd[2], 1024, 3072, phase(h.deadline)) || !zero_region(h.fd[2], 16384, 8192, phase(h.deadline)))
        return false;
    Row initial{};
    if (!double_read(h.fd[2], initial, 4096, phase(h.deadline)) || initial != h.initial) return false;
    for (size_t tree = 0; tree < 2; ++tree)
        if (!double_read(h.fd[tree], saved, 0, phase(h.deadline)) || saved != h.requests[tree] ||
            !zero_region(h.fd[tree], 2560, 1536, phase(h.deadline)) || !zero_region(h.fd[tree], 21504, 3072, phase(h.deadline)) ||
            !zero_region(h.fd[tree], 763392, 23040, phase(h.deadline)))
            return false;
    for (unsigned round = 0; round < ROUNDS; ++round) {
        uint64_t const target = round == 0 ? round0_target : h.common + static_cast<uint64_t>(round) * INTERVAL_US;
        std::array<Small, 4> private_rows{};
        for (size_t i = 0; i < 4; ++i) {
            if (!double_read(h.fd[i / 2], private_rows[i], sample_offset(static_cast<unsigned>(i + 1), round), phase(h.deadline)) ||
                !validate_small(h, i, round, target, private_rows[i]))
                return false;
            ++h.summary[80 + i];
        }
        Record host_row{};
        if (!double_read(h.fd[2], host_row, PREFIX_BYTES + static_cast<size_t>(round) * sizeof(Record), phase(h.deadline)) ||
            !validate_host_sample(h, round, target, host_row, private_rows))
            return false;
        ++h.summary[84];
    }
    for (size_t i = 0; i < 4; ++i) {
        unsigned const role = static_cast<unsigned>(i + 1);
        Row final{};
        Record lifecycle{}, progress{};
        Small last{};
        if (!double_read(h.fd[i / 2], initial, attestation_offset(role, 0), phase(h.deadline)) || initial != h.private_initial[i] ||
            !double_read(h.fd[i / 2], saved, command_offset(role), phase(h.deadline)) || saved != make_command(h, role, FINISH_ROUND, 0) ||
            !double_read(h.fd[i / 2], last, sample_offset(role, LAST_ROUND), phase(h.deadline)) ||
            !double_read(h.fd[i / 2], progress, progress_offset(role), phase(h.deadline)) ||
            !std::equal(last.begin(), last.end(), progress.begin()) || progress[63] != commit(h.nonce, role, LAST_ROUND, ROUNDS))
            return false;
        for (size_t j = 32; j < 63; ++j)
            if (progress[j] != 0) return false;
        if (!double_read(h.fd[i / 2], final, attestation_offset(static_cast<unsigned>(i + 1), 1), phase(h.deadline)) ||
            !validate_private_row(h, i, 1, final) || !double_read(h.fd[i / 2], lifecycle, i % 2 == 0 ? 20480 : 20992, phase(h.deadline)) ||
            !validate_lifecycle(h, i, lifecycle))
            return false;
        h.summary[68 + i] = hash(final.data(), sizeof(final));
    }
    Text stdout_text{}, stderr_text{};
    auto const out_raw = vfs(26, static_cast<uint64_t>(h.fd[6]), reinterpret_cast<uint64_t>(stdout_text.data()), stdout_text.size(), 0);
    auto const err_raw = vfs(26, static_cast<uint64_t>(h.fd[7]), reinterpret_cast<uint64_t>(stderr_text.data()), stderr_text.size(), 0);
    h.summary[50] = bits(out_raw);
    h.summary[51] = bits(err_raw);
    if (out_raw != 8192 || err_raw != 8192 || !manifest(h, stdout_text)) return false;
    for (char c : stderr_text)
        if (c != '\0') return false;
    h.summary[18] = h.stdout_length;
    h.summary[19] = 0;
    h.summary[20] = hash(stdout_text.data(), stdout_text.size());
    h.summary[21] = hash(stderr_text.data(), stderr_text.size());
    Token operation{};
    return transport(h.fd[3], operation.data(), sizeof(operation), 0, false, phase(h.deadline)) &&
           operation == operation_token(h.nonce, LAST_ROUND);
}
auto host_finish(Host& h, uint64_t round0_target) -> bool {
    auto const finish_deadline = phase(h.deadline, 10'000'000);
    for (unsigned role : {2U, 4U, 1U, 3U})
        if (!send_command(h, role, FINISH_ROUND, 0, finish_deadline)) return false;
    for (size_t i = 0; i < 2; ++i) {
        auto& child = i == 0 ? h.a : h.cli;
        auto const g = guard(h.nonce, 0, 5, 0x300 + i);
        Status status{};
        int64_t raw{};
        uint64_t start{}, end{};
        size_t const at = i == 0 ? 32 : 40;
        bool const consumed = consume(child, status, raw, g, phase(h.deadline, 10'000'000), start, end, &h.summary[48 + i]);
        h.summary[at] = bits(child.pid);
        h.summary[at + 1] = bits(raw);
        h.summary[at + 2] = status.before;
        h.summary[at + 3] = status.value;
        h.summary[at + 4] = status.after;
        h.summary[at + 5] = 1;
        h.summary[at + 6] = start;
        h.summary[at + 7] = end;
        if (!consumed) return false;
    }
    return whole_campaign(h, round0_target) && attest_host(h, 1, h.final);
}
auto host() -> int {
    Host h{};
    bool good = prepare_host(h);
    uint64_t round0_target{};
    if (good) good = create_a(h) && create_b(h);
    auto const startup_deadline = good ? phase(h.deadline, 30'000'000) : 0;
    if (good)
        good = await_manifest(h, startup_deadline) && await_initial(h, startup_deadline) &&
               discover_workers(h, phase(startup_deadline, 5'000'000)) && capture_proc(h, false, startup_deadline) &&
               attest_host(h, 0, h.initial);
    if (good) {
        auto const n = now_us();
        good = n != UINT64_MAX && n <= UINT64_MAX - INTERVAL_US;
        if (good) round0_target = n + INTERVAL_US;
    }
    Record host_row{};
    std::array<Small, 4> private_rows{};
    for (unsigned round = 0; good && round < ROUNDS; ++round) {
        uint64_t const target = round == 0 ? round0_target : h.common + static_cast<uint64_t>(round) * INTERVAL_US;
        uint64_t const command_deadline = round == 0 ? round0_target : target;
        for (unsigned role = 1; good && role <= 4; ++role) good = send_command(h, role, round, target, command_deadline);
        if (good) good = host_sample(h, round, target, host_row, private_rows);
        if (!good) break;
        if (round == 0) {
            h.common = host_row[7];
            for (auto const& row : private_rows) h.common = std::max(h.common, row[7]);
            good =
                h.common <= UINT64_MAX - static_cast<uint64_t>(LAST_ROUND) * INTERVAL_US && serial_start(h, target, host_row, private_rows);
        }
        if (round == LAST_ROUND) {
            h.end = host_row[6];
            for (auto const& row : private_rows) h.end = std::min(h.end, row[6]);
            good = h.end >= h.common && h.end - h.common >= OVERLAP_US && capture_proc(h, true, phase(h.deadline)) && serial_end(h);
        }
    }
    if (good) good = host_finish(h, round0_target);
    h.summary[5] = h.common;
    h.summary[6] = h.end;
    h.summary[8] = now_us();
    h.summary[9] = bits(h.cli.pid);
    h.summary[10] = h.physical[0];
    h.summary[11] = h.physical[2];
    h.summary[12] = h.physical[1];
    h.summary[13] = h.physical[3];
    h.summary[14] = ROUNDS;
    h.summary[15] = h.end >= h.common ? h.end - h.common : 0;
    h.summary[30] = guard(h.nonce, 0, 5, 8);
    h.summary[31] = commit(h.nonce, 5, 0, 2);
    if (h.fd[2] >= 0)
        good = publish(h.fd[2], h.summary, 12288, 511, h.summary[31], phase(h.deadline)) && good;
    else
        good = false;
    if (!good) {
        cleanup(h.a);
        cleanup(h.cli);
    }
    std::array<char, 512> status{};
    int const length = std::snprintf(
        status.data(), status.size(), "%s twohour-runtime nonce=%llu host=%llu rounds=1443 S=%llu E=%llu overlap-us=%llu failures=%u\n",
        good ? "PASS" : "FAIL", static_cast<unsigned long long>(h.nonce), static_cast<unsigned long long>(h.request[4]),
        static_cast<unsigned long long>(h.common), static_cast<unsigned long long>(h.end), static_cast<unsigned long long>(h.summary[15]),
        good ? 0U : 1U);
    bool const saved = length > 0 && static_cast<size_t>(length) < status.size() &&
                       save_file("campaign-status", status.data(), status.size(), phase(UINT64_MAX));
    bool const signalled = good && saved && committed_event("CONTROLLER", h.nonce, 1, phase(h.deadline));
    bool const closed = h.close_handles();
    return good && saved && signalled && closed ? 0 : 71;
}
auto init_exit_witness() -> int {
    namespace init = ker::abi::init_control;
    constexpr char SERVICE[] = "twohour-controller";
    struct GuardedSnapshot {
        uint64_t before = GUARD;
        init::StatusSnapshot value{};
        uint64_t after = ~GUARD;
    };
    static_assert(sizeof(init::StatusSnapshot) == 4160 && init::ABI_VERSION == 1 && offsetof(GuardedSnapshot, value) == 8 &&
                  offsetof(GuardedSnapshot, after) == 4168);
    auto const started = now_us();
    bool const clock_valid = started != UINT64_MAX && started <= UINT64_MAX - WITNESS_US;
    uint64_t const deadline = clock_valid ? started + WITNESS_US : 0;
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    bool observed = false, history = false;
    uint64_t nonce{};
    int64_t error = clock_valid ? 0 : -EIO;
    for (size_t i = 0; i < 4; ++i)
        if (process(10 + i) != 100) error = -EPERM;
    while (error == 0 && !observed) {
        if (!timely(deadline)) {
            error = -ETIMEDOUT;
            break;
        }
        GuardedSnapshot value{};
        auto const raw = ker::process::init_status_read(&value.value);
        if (!timely(deadline)) {
            error = -ETIMEDOUT;
            break;
        }
        if (raw != 0 && raw != -EAGAIN) {
            error = raw;
            break;
        }
        if (value.before != GUARD || value.after != ~GUARD) {
            error = -EOVERFLOW;
            break;
        }
        if (raw == 0) {
            snapshot = value.value;
            bool valid = snapshot.size == sizeof(snapshot) && snapshot.version == init::ABI_VERSION && snapshot.sequence != 0 &&
                         snapshot.published_mono_ns != 0 &&
                         (snapshot.state == init::SupervisorState::STARTING || snapshot.state == init::SupervisorState::RUNNING) &&
                         snapshot.flags == 0 && snapshot.service_count == 3 && snapshot.reserved0 == 0;
            for (auto item : snapshot.reserved) valid = item == 0 && valid;
            std::array<const char*, 3> names{"journald", SERVICE, "twohour-exit-witness"};
            std::array<unsigned, 3> matches{};
            for (uint32_t i = 0; valid && i < snapshot.service_count; ++i) {
                auto const& service = snapshot.services[i];
                valid = std::memchr(service.service, '\0', sizeof(service.service)) != nullptr && service.reserved0 == 0 &&
                        service.reserved1 == 0 && service.history_count <= init::MAX_TRANSITION_HISTORY &&
                        service.state_since_mono_ns != 0 && service.state_since_mono_ns <= snapshot.published_mono_ns &&
                        static_cast<unsigned>(service.state) >= 1 && static_cast<unsigned>(service.state) <= 8 &&
                        static_cast<unsigned>(service.readiness) >= 1 && static_cast<unsigned>(service.readiness) <= 3 &&
                        (service.flags & ~7U) == 0;
                for (auto item : service.reserved) valid = item == 0 && valid;
                if (!valid) break;
                auto const* nul = static_cast<const char*>(std::memchr(service.service, '\0', sizeof(service.service)));
                for (auto const* p = nul; p < service.service + sizeof(service.service); ++p) valid = *p == '\0' && valid;
                for (size_t name = 0; name < names.size(); ++name)
                    if (std::strcmp(service.service, names[name]) == 0) {
                        ++matches[name];
                        if (name == 1) selected = service;
                    }
                uint64_t previous{};
                for (uint32_t j = 0; j < init::MAX_TRANSITION_HISTORY; ++j) {
                    auto const& transition = service.history[j];
                    if (j < service.history_count) {
                        valid = transition.timestamp_mono_ns != 0 && transition.timestamp_mono_ns >= previous &&
                                transition.timestamp_mono_ns <= snapshot.published_mono_ns && transition.reserved0 == 0 &&
                                static_cast<unsigned>(transition.from_state) <= 8 && static_cast<unsigned>(transition.to_state) <= 8 &&
                                static_cast<unsigned>(transition.reason) >= 1 && static_cast<unsigned>(transition.reason) <= 9 && valid;
                        previous = transition.timestamp_mono_ns;
                    } else {
                        init::Transition empty{};
                        valid = std::memcmp(&transition, &empty, sizeof(empty)) == 0 && valid;
                    }
                }
            }
            for (auto count : matches) valid = count == 1 && valid;
            for (uint32_t i = snapshot.service_count; valid && i < init::MAX_STATUS_SERVICES; ++i) {
                init::ServiceStatus empty{};
                valid = std::memcmp(&snapshot.services[i], &empty, sizeof(empty)) == 0;
            }
            if (!valid) {
                error = -EPROTO;
                break;
            }
            history = false;
            for (uint32_t i = 0; i < selected.history_count; ++i) {
                auto const& transition = selected.history[i];
                if (transition.reason == init::TransitionReason::EXIT && transition.to_state == init::ServiceState::EXITED &&
                    transition.timestamp_mono_ns >= selected.state_since_mono_ns)
                    history = true;
            }
            observed = snapshot.state == init::SupervisorState::RUNNING && selected.generation == 1 && selected.restart_count == 0 &&
                       selected.state == init::ServiceState::EXITED && selected.readiness == init::ReadinessState::NOT_READY &&
                       selected.pid == -1 && selected.pgid == -1 && selected.last_wait_status == 0 && selected.last_error == 0 &&
                       selected.deadline_mono_ns == 0 && selected.flags == 3 && history;
        }
        if (!observed) {
            auto const n = now_us();
            if (n == UINT64_MAX || n >= deadline || n > UINT64_MAX - 1'000'000 ||
                !sleep_until(std::min(n + 1'000'000, deadline - 1), deadline))
                error = -ETIMEDOUT;
        }
    }
    if (observed && error == 0) {
        char path[160]{};
        Record request{};
        bool const named = filename(path, sizeof(path), "host-ledger.bin");
        int const fd = named ? open(path, O_RDONLY | O_CLOEXEC) : -1;
        bool const read = fd >= 0 && double_read(fd, request, 0, deadline);
        bool const closed = fd >= 0 && close(fd) == 0;
        nonce = request[2];
        if (!read || !closed || request[0] != MAGIC || request[1] != VERSION || nonce == 0 || request[3] != 5 || request[4] <= 2 ||
            request[20] != ROUNDS || request[22] != OVERLAP_US || request[30] != guard(nonce, 0, 5, 0) ||
            request[31] != commit(nonce, 5, 0, 1))
            error = -EPROTO;
    }
    auto const receipt_deadline = phase(UINT64_MAX);
    bool const snapshot_saved = save_file("init-exit-snapshot.bin", &snapshot, sizeof(snapshot), receipt_deadline);
    bool const passed = observed && error == 0 && snapshot_saved;
    std::array<char, 512> status{};
    int const count = std::snprintf(
        status.data(), status.size(),
        "%s twohour-init-exit nonce=%llu service=twohour-controller generation=%llu restarts=%llu sequence=%llu state=%u readiness=%u "
        "wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u read-error=%lld failures=%u\n",
        passed ? "PASS" : "FAIL", static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(selected.generation),
        static_cast<unsigned long long>(selected.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned>(selected.state), static_cast<unsigned>(selected.readiness), selected.last_wait_status, selected.last_error,
        history ? 1U : 0U, snapshot.size, snapshot.version, static_cast<long long>(error), passed ? 0U : 1U);
    bool const saved = count > 0 && static_cast<size_t>(count) < status.size() &&
                       save_file("init-exit-status", status.data(), status.size(), receipt_deadline);
    bool const signalled = passed && saved && committed_event("WITNESS", nonce, snapshot.sequence, phase(UINT64_MAX));
    return passed && saved && signalled ? 0 : 72;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 2 && std::strcmp(argv[1], "--host") == 0) exit_child(host());
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) exit_child(init_exit_witness());
    uint64_t nonce{}, host_pid{}, deadline{};
    if (argc == 6 && std::strcmp(argv[1], "--tree") == 0 && (std::strcmp(argv[2], "A") == 0 || std::strcmp(argv[2], "B") == 0) &&
        number(argv[3], nonce) && nonce != 0 && number(argv[4], host_pid) && host_pid > 2 && number(argv[5], deadline) && deadline != 0 &&
        environ != nullptr && environ[0] != nullptr && std::strcmp(environ[0], "LD_LIBRARY_PATH=/usr/lib") == 0 && environ[1] == nullptr)
        exit_child(private_tree(argv[2][0] == 'A' ? 1U : 2U, nonce, host_pid, deadline));
    return 73;
}
