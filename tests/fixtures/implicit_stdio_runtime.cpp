// Finite public raw EXEC/SPAWN/EXECVE descriptor provenance probe. Draft only.
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
namespace wire = ker::abi::compartment;
constexpr char RESULTS[] = "/srv/implicit-stdio-results";
constexpr char FIXTURE[] = "/usr/libexec/implicit-stdio-runtime";
constexpr char PRIVATE_FIXTURE[] = "/app/probe";
constexpr uint64_t MAGIC = 0x39535444494F5052ULL, VERSION = 1;
constexpr uint64_t GUARD = 0xA596C387E1D24B6FULL, ENTRY = 0x45584543454E5452ULL;
constexpr int64_t IO_SKIPPED = INT64_MIN;
constexpr uint64_t RUN_US = 60'000'000, STEP_US = 5'000'000;
constexpr int REPORT_FD = 9;
constexpr size_t LIMIT_BASE = 32, FD_BASE = 128, STAT_BASE = 384, WAIT_BASE = 480;
using Request = std::array<uint64_t, 64>;
using Row = std::array<uint64_t, 512>;
using Summary = std::array<uint64_t, 256>;
using Wait = std::array<uint64_t, 6>;
constexpr std::array<const char*, 3> LEDGERS{"absent-ledger.bin", "granted-ledger.bin", "host-ledger.bin"};
constexpr std::array<const char*, 3> MARKERS{"marker-absent", "marker-granted", "marker-host"};
constexpr std::array<const char*, 3> STDIO_FILES{"stdin-regular", "stdout-regular", "stderr-regular"};
constexpr std::array<size_t, 3> LEDGER_BYTES{20992, 20992, 12800};
constexpr std::array<unsigned, 4> PRIVATE_CASES{1, 2, 3, 4};
constexpr std::array<unsigned, 3> HOST_CASES{1, 3, 4};
constexpr std::array<int, 4> STAT_FDS{0, 1, 2, REPORT_FD};
static_assert(sizeof(Request) == 512 && sizeof(Row) == 4096 && sizeof(Summary) == 2048);
static_assert(LIMIT_BASE + 16 * 6 == FD_BASE && FD_BASE + 256 == STAT_BASE);
static_assert(STAT_BASE + 4 * 24 == WAIT_BASE && WAIT_BASE + 4 * 6 == 504);
static_assert(2 * (512 + 5 * 4096) + (512 + 3 * 4096) + 2048 + 4160 + 2 * 512 + 6 * 64 == 62400);
static_assert(62400 < 65536 && sizeof(wire::Spec) == 408 && sizeof(struct stat) == 144);
static_assert(offsetof(struct stat, st_mode) == 24 && offsetof(struct stat, st_size) == 48 && offsetof(struct stat, __unused) == 120);
static_assert(sizeof(ker::abi::process::SpawnOptions) == 72 && std::endian::native == std::endian::little);

enum Header : size_t {
    H_MAGIC,
    H_VERSION,
    H_KIND,
    H_NONCE,
    H_WAVE,
    H_CASE,
    H_PID,
    H_PPID,
    H_UID,
    H_EUID,
    H_GID,
    H_EGID,
    H_NNP,
    H_CEILING,
    H_PRIVILEGES,
    H_CHECKS,
    H_FAILURES,
    H_FIRST,
    H_START,
    H_END,
    H_DEADLINE,
    H_STDIO_MASK,
    H_PRESENT,
    H_UNEXPECTED,
    H_MARKER_OPEN,
    H_MARKER_READ,
    H_MARKER_CLOSE,
    H_MARKER_BYTES,
    H_MARKER_HASH,
    H_CLOEXEC_ACTION,
    H_ENTRY,
    H_GUARD
};
enum class Failure : uint64_t { CLOCK = 1, IO, REQUEST, IDENTITY, AUTHORITY, LIMIT, MARKER, FD, STAT, CREATE, WAIT, ROW, CLOEXEC };
struct Probe {
    Request request{};
    Row row{};
    uint64_t deadline{};
};
struct Owned {
    int64_t pid = -1;
    bool consumed = false;
};
struct GuardedStat {
    uint64_t before{};
    struct stat value{};
    uint64_t after{};
};
struct GuardedLimit {
    uint64_t before{};
    wire::Limit value{};
    uint64_t after{};
};
static_assert(offsetof(GuardedStat, value) == 8 && offsetof(GuardedStat, after) == 152 && sizeof(GuardedStat) == 160);
static_assert(offsetof(GuardedLimit, value) == 8 && offsetof(GuardedLimit, after) == 24 && sizeof(GuardedLimit) == 32);

auto bits(int64_t value) -> uint64_t { return std::bit_cast<uint64_t>(value); }
auto signed_value(uint64_t value) -> int64_t { return std::bit_cast<int64_t>(value); }
auto proc(uint64_t op, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0, uint64_t d = 0) -> int64_t {
    return signed_value(syscall(ker::abi::callnums::process, op, a, b, c, d));
}
auto fd_call(int fd, uint64_t op, uint64_t argument = 0) -> int64_t {
    return signed_value(syscall(ker::abi::callnums::vfs, uint64_t{28}, static_cast<uint64_t>(fd), op, argument));
}
auto raw_stat(int fd, struct stat& value) -> int64_t {
    return signed_value(syscall(ker::abi::callnums::vfs, uint64_t{13}, static_cast<uint64_t>(fd), reinterpret_cast<uint64_t>(&value)));
}
auto clock_us() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    auto const seconds = static_cast<uint64_t>(value.tv_sec);
    if (seconds > (UINT64_MAX - 999'999) / 1'000'000) return UINT64_MAX;
    return seconds * 1'000'000 + static_cast<uint64_t>(value.tv_nsec) / 1000;
}
auto phase(uint64_t deadline) -> uint64_t {
    auto const time = clock_us();
    if (time == UINT64_MAX || time > UINT64_MAX - STEP_US) return 0;
    auto const next = time + STEP_US;
    return next < deadline ? next : deadline;
}
auto in_time(uint64_t deadline) -> bool {
    auto const time = clock_us();
    return time != UINT64_MAX && time < deadline;
}
void pause() {
    timespec duration{0, 1'000'000};
    (void)nanosleep(&duration, nullptr);
}
void check(Probe& probe, bool good, Failure failure) {
    ++probe.row[H_CHECKS];
    if (good) return;
    ++probe.row[H_FAILURES];
    if (probe.row[H_FIRST] == 0) probe.row[H_FIRST] = static_cast<uint64_t>(failure);
}
auto guard(uint64_t nonce, unsigned wave, unsigned which, uint64_t selector) -> uint64_t {
    return GUARD ^ nonce ^ (static_cast<uint64_t>(wave) << 48) ^ (static_cast<uint64_t>(which) << 32) ^ selector;
}
auto io(int fd, void* data, size_t bytes, off_t offset, bool writing, uint64_t deadline) -> int64_t {
    auto* cursor = static_cast<uint8_t*>(data);
    size_t done{};
    auto previous = clock_us();
    while (done < bytes) {
        auto const before = clock_us();
        if (before == UINT64_MAX || before < previous || before >= deadline) return -ETIMEDOUT;
        auto const count = writing ? pwrite(fd, cursor + done, bytes - done, offset + static_cast<off_t>(done))
                                   : pread(fd, cursor + done, bytes - done, offset + static_cast<off_t>(done));
        auto const after = clock_us();
        if (after == UINT64_MAX || after < before || after >= deadline) return -ETIMEDOUT;
        previous = after;
        if (count < 0) {
            if (errno == EINTR) continue;
            return -errno;
        }
        if (count == 0 || static_cast<size_t>(count) > bytes - done) return -EIO;
        done += static_cast<size_t>(count);
    }
    return static_cast<int64_t>(done);
}
auto flush(int fd, uint64_t deadline) -> bool { return in_time(deadline) && fsync(fd) == 0 && in_time(deadline); }
auto path(char* output, size_t capacity, const char* name) -> bool {
    auto const count = std::snprintf(output, capacity, "%s/%s", RESULTS, name);
    return count > 0 && static_cast<size_t>(count) < capacity;
}
auto save(const char* name, const void* data, size_t bytes, uint64_t deadline) -> bool {
    char filename[160]{};
    if (!path(filename, sizeof(filename), name) || !in_time(deadline)) return false;
    int const fd = open(filename, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    bool const written = io(fd, const_cast<void*>(data), bytes, 0, true, deadline) == static_cast<int64_t>(bytes);
    bool const flushed = flush(fd, deadline), closed = close(fd) == 0;
    return written && flushed && closed;
}
auto number(const char* text, uint64_t& result) -> bool {
    result = 0;
    for (size_t index = 0; index < 21; ++index) {
        char const digit = text[index];
        if (digit == '\0') return index != 0;
        if (digit < '0' || digit > '9' || result > (UINT64_MAX - static_cast<uint64_t>(digit - '0')) / 10) return false;
        result = result * 10 + static_cast<uint64_t>(digit - '0');
    }
    return false;
}
auto hash(const void* data, size_t bytes) -> uint64_t {
    auto const* cursor = static_cast<const uint8_t*>(data);
    uint64_t result = 1469598103934665603ULL;
    for (size_t index = 0; index < bytes; ++index) result = (result ^ cursor[index]) * 1099511628211ULL;
    return result;
}
auto regular_token(std::array<char, 64>& output, uint64_t nonce, unsigned which, int fd, bool original) -> bool {
    int const count =
        original
            ? std::snprintf(output.data(), output.size(), "explicit regular fd=%d nonce=%llu\n", fd, static_cast<unsigned long long>(nonce))
            : std::snprintf(output.data(), output.size(), "stdio n=%llu c=%u fd=%d\n", static_cast<unsigned long long>(nonce), which, fd);
    return count > 0 && static_cast<size_t>(count) < output.size();
}
auto positional(int fd, std::array<char, 64>& data, bool writing, uint64_t deadline, bool& timely) -> int64_t {
    timely = false;
    auto const before = clock_us();
    if (before == UINT64_MAX || before >= deadline) return IO_SKIPPED;
    auto const result = signed_value(syscall(ker::abi::callnums::vfs, writing ? uint64_t{27} : uint64_t{26}, static_cast<uint64_t>(fd),
                                             reinterpret_cast<uint64_t>(data.data()), data.size(), uint64_t{0}));
    auto const after = clock_us();
    // Preserve the actual raw return, including a late or regressed-clock call.
    timely = after != UINT64_MAX && after >= before && after < deadline;
    return result;
}
auto row_offset(unsigned wave, unsigned which) -> off_t {
    unsigned const index = wave != 3 ? which : (which == 1 ? 0U : which == 3 ? 1U : 2U);
    return 512 + static_cast<off_t>(index) * static_cast<off_t>(sizeof(Row));
}
auto publish(Probe& probe) -> bool {
    auto const deadline = phase(probe.deadline);
    auto const offset = row_offset(static_cast<unsigned>(probe.row[H_WAVE]), static_cast<unsigned>(probe.row[H_CASE]));
    probe.row[H_END] = clock_us();
    check(probe, probe.row[H_END] >= probe.row[H_START] && in_time(probe.deadline), Failure::CLOCK);
    probe.row[511] = 0;
    if (io(REPORT_FD, probe.row.data(), sizeof(Row), offset, true, deadline) != static_cast<int64_t>(sizeof(Row)) ||
        !flush(REPORT_FD, deadline))
        return false;
    auto const commit = ~probe.row[H_GUARD];
    if (io(REPORT_FD, const_cast<uint64_t*>(&commit), sizeof(commit), offset + 4088, true, deadline) !=
            static_cast<int64_t>(sizeof(commit)) ||
        !flush(REPORT_FD, deadline))
        return false;
    probe.row[511] = commit;
    return true;
}
auto read_row(int fd, unsigned wave, unsigned which, uint64_t nonce, int64_t pid, Row& row, uint64_t deadline) -> bool {
    Row first{}, second{};
    auto const offset = row_offset(wave, which);
    if (io(fd, first.data(), sizeof(first), offset, false, deadline) != static_cast<int64_t>(sizeof(first)) ||
        io(fd, second.data(), sizeof(second), offset, false, deadline) != static_cast<int64_t>(sizeof(second)))
        return false;
    if (first != second || first[H_MAGIC] != MAGIC || first[H_VERSION] != VERSION || first[H_NONCE] != nonce || first[H_WAVE] != wave ||
        first[H_CASE] != which || first[H_PID] != static_cast<uint64_t>(pid) || first[H_GUARD] != guard(nonce, wave, which, 0) ||
        first[511] != ~first[H_GUARD])
        return false;
    if (first[510] != 0) return false;
    row = first;
    return true;
}
auto request_read(Probe& probe, unsigned wave, unsigned which, uint64_t nonce, uint64_t deadline, uint64_t action, uint64_t entry) -> bool {
    probe.deadline = deadline;
    auto& row = probe.row;
    row[H_MAGIC] = MAGIC;
    row[H_VERSION] = VERSION;
    row[H_KIND] = which == 0 ? 1 : wave == 3 ? 3 : 2;
    row[H_NONCE] = nonce;
    row[H_WAVE] = wave;
    row[H_CASE] = which;
    row[H_START] = clock_us();
    row[H_DEADLINE] = deadline;
    row[H_CLOEXEC_ACTION] = action;
    row[H_ENTRY] = entry;
    row[H_GUARD] = guard(nonce, wave, which, 0);
    Request second{};
    bool const read =
        io(REPORT_FD, probe.request.data(), sizeof(Request), 0, false, phase(deadline)) == static_cast<int64_t>(sizeof(Request)) &&
        io(REPORT_FD, second.data(), sizeof(Request), 0, false, phase(deadline)) == static_cast<int64_t>(sizeof(Request));
    bool const valid = read && second == probe.request && probe.request[0] == MAGIC && probe.request[1] == VERSION &&
                       probe.request[2] == nonce && probe.request[3] == wave && probe.request[5] == deadline &&
                       probe.request[13] == (wave == 3 ? 3U : 5U) && probe.request[14] == (wave == 2 ? 7U : 0U) &&
                       probe.request[15] == 64 && probe.request[19] == LEDGER_BYTES[wave - 1] && probe.request[20] == 2 &&
                       probe.request[30] == guard(nonce, wave, 0, 0x52514553) && probe.request[31] == ~probe.request[30];
    check(probe, valid, Failure::REQUEST);
    return valid;
}
void attest(Probe& probe) {
    auto& row = probe.row;
    unsigned const wave = static_cast<unsigned>(row[H_WAVE]), which = static_cast<unsigned>(row[H_CASE]);
    row[H_PID] = bits(proc(3));
    row[H_PPID] = bits(proc(4));
    for (size_t index = 0; index < 4; ++index) row[H_UID + index] = bits(proc(10 + index));
    row[H_NNP] = bits(proc(36, 39));
    row[H_CEILING] = bits(proc(36, 0x574f5301));
    row[H_PRIVILEGES] = bits(proc(36, 0x574f5304));
    check(probe,
          wave == 3    ? row[H_PID] > 2 && row[H_PPID] == probe.request[4]
          : which == 0 ? row[H_PID] == 1 && row[H_PPID] == 0
                       : row[H_PID] > 1 && row[H_PPID] == 1,
          Failure::IDENTITY);
    for (size_t index = 0; index < 4; ++index)
        check(probe, row[H_UID + index] == probe.request[6 + index] && row[H_UID + index] == 100, Failure::IDENTITY);
    check(probe, row[H_NNP] == probe.request[10] && row[H_CEILING] == probe.request[11] && row[H_PRIVILEGES] == probe.request[12],
          Failure::AUTHORITY);
    check(probe, row[H_ENTRY] == ENTRY, Failure::CREATE);
    check(probe, row[H_CLOEXEC_ACTION] == 0, Failure::CLOEXEC);
    for (size_t resource = 0; resource < 16; ++resource) {
        auto const sentinel = guard(row[H_NONCE], wave, which, 0x100 + resource);
        GuardedLimit value{sentinel, {}, ~sentinel};
        auto const result = proc(48, resource, reinterpret_cast<uint64_t>(&value.value), 1);
        size_t const at = LIMIT_BASE + resource * 6;
        row[at] = bits(result);
        row[at + 1] = value.before;
        row[at + 2] = value.value.soft;
        row[at + 3] = value.value.hard;
        row[at + 4] = value.after;
        row[at + 5] = resource;
        check(probe,
              result == 0 && value.before == sentinel && value.after == ~sentinel && value.value.soft == probe.request[32 + resource * 2] &&
                  value.value.hard == probe.request[33 + resource * 2],
              Failure::LIMIT);
    }
    char marker_path[160]{};
    bool const named = wave == 3 ? path(marker_path, sizeof(marker_path), MARKERS[2])
                                 : std::snprintf(marker_path, sizeof(marker_path), "/same/marker") > 0;
    int const marker_fd = named ? open(marker_path, O_RDONLY | O_CLOEXEC) : -1;
    row[H_MARKER_OPEN] = bits(marker_fd >= 0 ? marker_fd : -errno);
    std::array<uint8_t, 64> marker{};
    auto const read = marker_fd >= 0 ? io(marker_fd, marker.data(), marker.size(), 0, false, phase(probe.deadline)) : -EBADF;
    int const closed = marker_fd >= 0 ? close(marker_fd) : -1;
    row[H_MARKER_READ] = bits(read);
    row[H_MARKER_CLOSE] = bits(closed == 0 ? 0 : -errno);
    row[H_MARKER_BYTES] = read > 0 ? static_cast<uint64_t>(read) : 0;
    row[H_MARKER_HASH] = hash(marker.data(), marker.size());
    check(probe, marker_fd >= 0 && read == 64 && closed == 0 && row[H_MARKER_HASH] == probe.request[16], Failure::MARKER);
    unsigned const mask = wave == 1 || (wave == 3 && which == 3) ? 0U : wave == 2 && which == 4 ? 3U : 7U;
    row[H_STDIO_MASK] = mask;
    for (int fd = 0; fd < 256; ++fd) {
        auto const result = fd_call(fd, 1);
        row[FD_BASE + static_cast<size_t>(fd)] = bits(result);
        bool const expected = fd == REPORT_FD || (fd < 3 && (mask & (1U << static_cast<unsigned>(fd))) != 0);
        if (result >= 0) ++row[H_PRESENT];
        if ((!expected && result != -EBADF) || (expected && result != 0)) ++row[H_UNEXPECTED];
        check(probe, expected ? result == 0 : result == -EBADF, Failure::FD);
    }
    for (size_t slot = 0; slot < STAT_FDS.size(); ++slot) {
        int const fd = STAT_FDS[slot];
        auto const sentinel = guard(row[H_NONCE], wave, which, 0x200 + static_cast<unsigned>(fd));
        GuardedStat value{};
        value.before = sentinel;
        value.after = ~sentinel;
        std::memset(&value.value, 0xA5, sizeof(value.value));
        auto const descriptor = fd_call(fd, 1), access = fd_call(fd, 3), result = raw_stat(fd, value.value);
        size_t const at = STAT_BASE + slot * 24;
        row[at] = static_cast<uint64_t>(fd);
        row[at + 1] = bits(descriptor);
        row[at + 2] = bits(access);
        row[at + 3] = bits(result);
        row[at + 4] = value.before;
        std::memcpy(row.data() + at + 5, &value.value, sizeof(value.value));
        row[at + 23] = value.after;
        bool const expected = fd == REPORT_FD || (mask & (1U << static_cast<unsigned>(fd))) != 0;
        bool good = value.before == sentinel && value.after == ~sentinel;
        if (!expected) {
            std::array<uint8_t, 144> untouched{};
            untouched.fill(0xA5);
            good = good && descriptor == -EBADF && access == -EBADF && result == -EBADF &&
                   std::memcmp(&value.value, untouched.data(), untouched.size()) == 0;
        } else {
            bool const report = fd == REPORT_FD, console = !report && wave == 3;
            unsigned const wanted_access = report ? 2U : fd == 0 ? 0U : 1U;
            good = good && descriptor == 0 && access >= 0 && (static_cast<uint64_t>(access) & 3U) == wanted_access && result == 0 &&
                   (value.value.st_mode & S_IFMT) == (console ? S_IFCHR : S_IFREG);
            if (!console) {
                size_t const base = report ? 17 : 21 + static_cast<size_t>(fd) * 3;
                good = good && value.value.st_dev == probe.request[base] && value.value.st_ino == probe.request[base + 1] &&
                       value.value.st_size >= 0 && static_cast<uint64_t>(value.value.st_size) == probe.request[base + 2];
            }
        }
        check(probe, good, Failure::STAT);
        if (fd < 3) {
            auto& read_raw = row[504 + static_cast<size_t>(fd) * 2];
            auto& write_raw = row[505 + static_cast<size_t>(fd) * 2];
            read_raw = bits(IO_SKIPPED);
            write_raw = bits(IO_SKIPPED);
            bool const console = expected && wave == 3;
            // A pre-fix private leak may be a real console. Only proven regular
            // grants or proven absent descriptors can reach positional IO.
            if (good && !console) {
                std::array<char, 64> actual{}, token{}, original{};
                bool const tokens =
                    regular_token(token, row[H_NONCE], which, fd, false) && regular_token(original, row[H_NONCE], which, fd, true);
                bool read_timely = false, write_timely = false;
                auto const read_result = positional(fd, actual, false, phase(probe.deadline), read_timely);
                auto const write_result = positional(fd, token, true, phase(probe.deadline), write_timely);
                read_raw = bits(read_result);
                write_raw = bits(write_result);
                bool const readable = expected && fd == 0, writable = expected && fd != 0;
                check(probe,
                      tokens && read_timely && write_timely &&
                          (readable ? read_result == 64 && actual == original : read_result == -EBADF) &&
                          (writable ? write_result == 64 : write_result == -EBADF) && in_time(probe.deadline),
                      Failure::IO);
                if (writable) check(probe, flush(fd, phase(probe.deadline)), Failure::IO);
            } else {
                check(probe, good && console && wave == 3, Failure::IO);
            }
        }
    }
    check(probe, row[H_START] != UINT64_MAX && in_time(probe.deadline), Failure::CLOCK);
}
void cleanup(Owned& child) {
    if (child.pid <= 0 || child.consumed) return;
    (void)proc(8, static_cast<uint64_t>(child.pid), SIGKILL);
    auto const deadline = phase(UINT64_MAX);
    while (in_time(deadline)) {
        int status = -1;
        auto const result = proc(2, static_cast<uint64_t>(child.pid), reinterpret_cast<uint64_t>(&status), 1);
        if (result == child.pid || result == -ECHILD) {
            child.consumed = true;
            return;
        }
        if (result != 0 && result != -EINTR) return;
        pause();
    }
}
auto consume(Owned& child, Wait& record, uint64_t deadline) -> bool {
    int status = -1;
    int64_t result = 0;
    while (child.pid > 0 && in_time(deadline)) {
        result = proc(2, static_cast<uint64_t>(child.pid), reinterpret_cast<uint64_t>(&status), 1);
        if (result == child.pid) {
            child.consumed = true;
            break;
        }
        if (result != 0 && result != -EINTR) break;
        pause();
    }
    record[2] = bits(result);
    record[3] = bits(status);
    record[5] = clock_us();
    bool const good = child.consumed && result == child.pid && status == 0 && record[5] >= record[4] && record[5] < deadline;
    if (!child.consumed) cleanup(child);
    return good;
}
[[noreturn]] void exit_child(int status) {
    (void)proc(0, static_cast<uint64_t>(status));
    for (;;) {
    }
}
auto leaf(unsigned wave, unsigned which, uint64_t nonce, uint64_t deadline, uint64_t action, uint64_t entry) -> int {
    Probe probe{};
    if (request_read(probe, wave, which, nonce, deadline, action, entry)) attest(probe);
    bool const saved = publish(probe);
    return saved && probe.row[H_FAILURES] == 0 ? 0 : 70;
}
auto launch(unsigned wave, unsigned which, uint64_t nonce, uint64_t deadline, Owned& child, Wait& record) -> bool {
    char wave_text[24]{}, which_text[24]{}, nonce_text[24]{}, deadline_text[24]{};
    std::snprintf(wave_text, sizeof(wave_text), "%u", wave);
    std::snprintf(which_text, sizeof(which_text), "%u", which);
    std::snprintf(nonce_text, sizeof(nonce_text), "%llu", static_cast<unsigned long long>(nonce));
    std::snprintf(deadline_text, sizeof(deadline_text), "%llu", static_cast<unsigned long long>(deadline));
    char const* executable = wave == 3 ? FIXTURE : PRIVATE_FIXTURE;
    std::array<const char*, 9> args{executable, "--leaf", wave_text, which_text, nonce_text, deadline_text, "0", nullptr, nullptr};
    std::array<const char*, 2> environment{"LD_LIBRARY_PATH=/usr/lib", nullptr};
    ker::abi::process::SpawnOptions options{};
    options.size = sizeof(options);
    options.version = 1;
    record[0] = which;
    record[4] = clock_us();
    int64_t result = -ETIMEDOUT;
    if (in_time(deadline)) {
        if (which == 4) {
            result = proc(5);
            if (result == 0) {
                auto const action = wave == 2 ? fd_call(2, 2, 1) : 0;
                char action_text[24]{};
                std::snprintf(action_text, sizeof(action_text), "%llu", static_cast<unsigned long long>(bits(action)));
                args[6] = action_text;
                auto const returned = proc(24, reinterpret_cast<uint64_t>(executable), reinterpret_cast<uint64_t>(args.data()),
                                           reinterpret_cast<uint64_t>(environment.data()));
                // A returned EXECVE is an actual failure; never count the old image as a replacement entry.
                (void)leaf(wave, which, nonce, deadline, bits(action), bits(returned));
                exit_child(70);
            }
        } else if (which == 1) {
            result = proc(1, reinterpret_cast<uint64_t>(executable), reinterpret_cast<uint64_t>(args.data()),
                          reinterpret_cast<uint64_t>(environment.data()));
        } else {
            result = proc(43, reinterpret_cast<uint64_t>(executable), reinterpret_cast<uint64_t>(args.data()),
                          reinterpret_cast<uint64_t>(environment.data()), which == 3 ? reinterpret_cast<uint64_t>(&options) : 0);
        }
    }
    // Ownership is recorded before another clock observation, report operation or wait.
    if (result > 0) child.pid = result;
    record[1] = bits(result);
    return result > 0;
}
auto private_root(unsigned wave, uint64_t nonce, uint64_t deadline) -> int {
    Probe probe{};
    if (!request_read(probe, wave, 0, nonce, deadline, 0, ENTRY)) return 70;
    attest(probe);
    for (size_t index = 0; index < PRIVATE_CASES.size(); ++index) {
        Owned child{};
        Wait record{};
        unsigned const which = PRIVATE_CASES[index];
        auto const child_deadline = phase(deadline);
        bool const created = launch(wave, which, nonce, deadline, child, record);
        check(probe, created && child.pid > 1, Failure::CREATE);
        bool const reaped = created && consume(child, record, child_deadline);
        check(probe, reaped, Failure::WAIT);
        Row row{};
        bool const read = child.consumed && read_row(REPORT_FD, wave, which, nonce, child.pid, row, phase(deadline));
        check(probe, read && row[H_FAILURES] == 0 && row[H_PPID] == 1, Failure::ROW);
        std::copy(record.begin(), record.end(), probe.row.begin() + static_cast<ptrdiff_t>(WAIT_BASE + index * 6));
        cleanup(child);
    }
    bool const saved = publish(probe);
    return saved && probe.row[H_FAILURES] == 0 ? 0 : 70;
}
auto text(const char* value) -> wire::String { return {reinterpret_cast<uint64_t>(value), std::strlen(value)}; }
auto close_ambient() -> int64_t {
    int64_t closed{};
    for (int fd = 0; fd < 256; ++fd) {
        if (fd == REPORT_FD) continue;
        auto const present = fd_call(fd, 1);
        if (present == -EBADF) continue;
        if (present < 0 || close(fd) != 0) return -EIO;
        ++closed;
    }
    return closed;
}
auto open_ledger(unsigned wave, uint64_t nonce, uint64_t host_pid, uint64_t deadline, Request& request) -> int {
    char filename[160]{};
    if (!path(filename, sizeof(filename), LEDGERS[wave - 1])) return -1;
    int const fd = open(filename, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    bool good = ftruncate(fd, static_cast<off_t>(LEDGER_BYTES[wave - 1])) == 0;
    request[0] = MAGIC;
    request[1] = VERSION;
    request[2] = nonce;
    request[3] = wave;
    request[4] = host_pid;
    request[5] = deadline;
    for (size_t index = 0; index < 4; ++index) request[6 + index] = bits(proc(10 + index));
    request[10] = wave == 3 ? bits(proc(36, 39)) : 1;
    request[11] = wave == 3 ? bits(proc(36, 0x574f5301)) : 0;
    request[12] = wave == 3 ? bits(proc(36, 0x574f5304)) : 256;
    good = request[10] <= 1 && request[11] <= 0xffff && (request[12] & ~uint64_t{259}) == 0 && good;
    request[13] = wave == 3 ? 3 : 5;
    request[14] = wave == 2 ? 7 : 0;
    request[15] = 64;
    std::array<uint8_t, 64> marker{};
    if (!path(filename, sizeof(filename), MARKERS[wave - 1])) good = false;
    int const marker_fd = good ? open(filename, O_RDONLY | O_CLOEXEC) : -1;
    if (marker_fd < 0) good = false;
    if (marker_fd >= 0) {
        good = io(marker_fd, marker.data(), marker.size(), 0, false, phase(deadline)) == 64 && good;
        good = close(marker_fd) == 0 && good;
    }
    request[16] = hash(marker.data(), marker.size());
    struct stat stat_value{};
    good = raw_stat(fd, stat_value) == 0 && S_ISREG(stat_value.st_mode) &&
           stat_value.st_size == static_cast<off_t>(LEDGER_BYTES[wave - 1]) && good;
    request[17] = stat_value.st_dev;
    request[18] = stat_value.st_ino;
    request[19] = static_cast<uint64_t>(stat_value.st_size);
    request[20] = 2;
    for (size_t resource = 0; resource < 16; ++resource) {
        wire::Limit limit{};
        good = proc(48, resource, reinterpret_cast<uint64_t>(&limit), 1) == 0 && good;
        request[32 + resource * 2] = limit.soft;
        request[33 + resource * 2] = limit.hard;
    }
    for (size_t index = 0; wave == 2 && index < 3; ++index) {
        if (!path(filename, sizeof(filename), STDIO_FILES[index])) {
            good = false;
            continue;
        }
        int const source = open(filename, (index == 0 ? O_RDONLY : O_WRONLY) | O_CLOEXEC);
        if (source < 0) {
            good = false;
            continue;
        }
        good = raw_stat(source, stat_value) == 0 && S_ISREG(stat_value.st_mode) && stat_value.st_size == 64 && good;
        request[21 + index * 3] = stat_value.st_dev;
        request[22 + index * 3] = stat_value.st_ino;
        request[23 + index * 3] = static_cast<uint64_t>(stat_value.st_size);
        good = close(source) == 0 && good;
    }
    request[30] = guard(nonce, wave, 0, 0x52514553);
    request[31] = ~request[30];
    good = io(fd, request.data(), sizeof(request), 0, true, phase(deadline)) == static_cast<int64_t>(sizeof(request)) && good;
    good = flush(fd, phase(deadline)) && good;
    if (!good) {
        (void)close(fd);
        return -1;
    }
    return fd;
}
auto create_root(unsigned wave, Request const& request, int ledger, Owned& root, bool& inputs_closed) -> int64_t {
    char marker[160]{};
    if (!path(marker, sizeof(marker), MARKERS[wave - 1])) return -EINVAL;
    std::array<const char*, 4> sources{FIXTURE, "/lib/ld.so", "/usr/lib/libc.so", marker};
    std::array<const char*, 4> destinations{PRIVATE_FIXTURE, "/lib/ld.so", "/usr/lib/libc.so", "/same/marker"};
    std::array<wire::Asset, 4> assets{};
    std::array<int, 3> stdio{};
    stdio.fill(-1);
    bool good = in_time(request[5]);
    for (size_t index = 0; index < assets.size(); ++index) {
        assets[index].source_fd = open(sources[index], O_RDONLY | O_CLOEXEC);
        assets[index].mode = index < 2 ? 0755U : 0644U;
        assets[index].destination = text(destinations[index]);
        good = assets[index].source_fd >= 0 && good;
    }
    std::array<wire::Descriptor, 4> descriptors{};
    descriptors[0] = {ledger, REPORT_FD, 0};
    for (size_t index = 0; wave == 2 && index < stdio.size(); ++index) {
        char filename[160]{};
        if (!path(filename, sizeof(filename), STDIO_FILES[index])) {
            good = false;
            continue;
        }
        stdio[index] = open(filename, (index == 0 ? O_RDONLY : O_WRONLY) | O_CLOEXEC);
        descriptors[index + 1] = {stdio[index], static_cast<uint32_t>(index), 0};
        good = stdio[index] >= 0 && good;
    }
    char wave_text[24]{}, nonce_text[24]{}, deadline_text[24]{};
    std::snprintf(wave_text, sizeof(wave_text), "%u", wave);
    std::snprintf(nonce_text, sizeof(nonce_text), "%llu", static_cast<unsigned long long>(request[2]));
    std::snprintf(deadline_text, sizeof(deadline_text), "%llu", static_cast<unsigned long long>(request[5]));
    std::array args{text(PRIVATE_FIXTURE), text("--root"), text(wave_text), text(nonce_text), text(deadline_text)};
    std::array environment{text("LD_LIBRARY_PATH=/usr/lib")};
    wire::Spec spec{};
    for (size_t resource = 0; resource < 16; ++resource) spec.limits[resource] = {request[32 + resource * 2], request[33 + resource * 2]};
    spec.executable = args[0];
    spec.assets = reinterpret_cast<uint64_t>(assets.data());
    spec.asset_count = assets.size();
    spec.descriptors = reinterpret_cast<uint64_t>(descriptors.data());
    spec.descriptor_count = wave == 2 ? 4 : 1;
    spec.argv = reinterpret_cast<uint64_t>(args.data());
    spec.argc = args.size();
    spec.envp = reinterpret_cast<uint64_t>(environment.data());
    spec.envc = environment.size();
    auto const result = good ? proc(54, reinterpret_cast<uint64_t>(&spec)) : -EIO;
    // Exact positive ownership precedes all asset closes and later observations.
    if (result > 0) root.pid = result;
    for (auto const& asset : assets)
        if (asset.source_fd >= 0) good = close(asset.source_fd) == 0 && good;
    for (int const fd : stdio)
        if (fd >= 0) good = close(fd) == 0 && good;
    inputs_closed = good;
    return result;
}
auto host() -> int {
    Summary summary{};
    auto const start = clock_us();
    if (start == UINT64_MAX || start > UINT64_MAX - RUN_US) return 71;
    auto const deadline = start + RUN_US, pid = bits(proc(3));
    auto const nonce = (start ^ (pid << 32) ^ GUARD) | 1U;
    summary[0] = MAGIC;
    summary[1] = VERSION;
    summary[2] = nonce;
    summary[7] = pid;
    summary[8] = start;
    summary[10] = deadline;
    for (size_t index = 0; index < 4; ++index) summary[3 + index] = bits(proc(10 + index));
    uint64_t failures{};
    auto expect = [&](bool value) {
        ++summary[11];
        if (!value) ++failures;
    };
    expect(summary[3] == 100 && summary[4] == 100 && summary[5] == 100 && summary[6] == 100 && pid > 2);
    // The report descriptor is not ambient either: close it before creating this run's files.
    if (fd_call(REPORT_FD, 1) >= 0) expect(close(REPORT_FD) == 0);
    auto const closed = close_ambient();
    summary[14] = bits(closed);
    expect(closed >= 0);
    expect(mkdir(RESULTS, 0700) == 0 || errno == EEXIST);
    for (unsigned wave = 1; wave <= 3; ++wave) {
        std::array<char, 64> marker{};
        auto const count = std::snprintf(marker.data(), marker.size(), "implicit-stdio nonce=%llu wave=%u\n",
                                         static_cast<unsigned long long>(nonce), wave);
        expect(count > 0 && static_cast<size_t>(count) < marker.size());
        expect(save(MARKERS[wave - 1], marker.data(), marker.size(), phase(deadline)));
    }
    for (size_t index = 0; index < 3; ++index) {
        std::array<char, 64> data{};
        expect(regular_token(data, nonce, 0, static_cast<int>(index), true));
        expect(save(STDIO_FILES[index], data.data(), data.size(), phase(deadline)));
    }
    for (unsigned wave = 1; wave <= 2; ++wave) {
        Request request{};
        auto const wave_deadline = phase(deadline);
        int const ledger = open_ledger(wave, nonce, pid, wave_deadline, request);
        expect(ledger >= 0);
        Owned root{};
        Wait record{54, 0, 0, bits(-1), clock_us(), 0};
        bool inputs_closed = false;
        auto const created = ledger >= 0 ? create_root(wave, request, ledger, root, inputs_closed) : -EIO;
        record[1] = bits(created);
        if (root.pid > 0) ++summary[15];
        expect(created > 2 && root.pid == created && inputs_closed);
        bool const reaped = root.pid > 0 && consume(root, record, wave_deadline);
        if (root.consumed) ++summary[16];
        expect(reaped);
        std::copy(record.begin(), record.end(), summary.begin() + static_cast<ptrdiff_t>(32 + (wave - 1) * 6));
        Row root_row{};
        bool const read = root.consumed && ledger >= 0 && read_row(ledger, wave, 0, nonce, 1, root_row, phase(deadline));
        expect(read && root_row[H_FAILURES] == 0 && root_row[H_PPID] == 0);
        if (read) {
            ++summary[19];
            ++summary[20];
            for (size_t index = 0; index < PRIVATE_CASES.size(); ++index) {
                auto const child_pid = signed_value(root_row[WAIT_BASE + index * 6 + 1]);
                Row child_row{};
                bool const child_read =
                    child_pid > 1 && read_row(ledger, wave, PRIVATE_CASES[index], nonce, child_pid, child_row, phase(deadline));
                expect(child_read && child_row[H_FAILURES] == 0 && child_row[H_PPID] == 1 &&
                       signed_value(root_row[WAIT_BASE + index * 6 + 2]) == child_pid && root_row[WAIT_BASE + index * 6 + 3] == 0);
                if (child_read) ++summary[20];
            }
        }
        if (wave == 2 && root.consumed) {
            for (size_t slot = 0; slot < 2; ++slot) {
                int const stdio_fd = static_cast<int>(slot) + 1;
                unsigned const last_case = stdio_fd == 1 ? 4U : 3U;
                char filename[160]{};
                bool const named = path(filename, sizeof(filename), STDIO_FILES[static_cast<size_t>(stdio_fd)]);
                int const source = named ? open(filename, O_RDONLY | O_CLOEXEC) : -1;
                std::array<char, 64> actual{}, expected{};
                bool const token = regular_token(expected, nonce, last_case, stdio_fd, false);
                auto const read_raw = source >= 0 ? io(source, actual.data(), actual.size(), 0, false, phase(deadline)) : -EBADF;
                struct stat value{};
                bool const identity = source >= 0 && raw_stat(source, value) == 0 && S_ISREG(value.st_mode) && value.st_size == 64 &&
                                      value.st_dev == request[21 + static_cast<size_t>(stdio_fd) * 3] &&
                                      value.st_ino == request[22 + static_cast<size_t>(stdio_fd) * 3];
                bool const closed_source = source >= 0 && close(source) == 0;
                summary[23 + slot] = bits(read_raw);
                summary[25 + slot] = hash(actual.data(), actual.size());
                bool const verified = token && identity && read_raw == 64 && actual == expected && closed_source;
                expect(verified);
                if (verified) ++summary[22];
            }
        }
        cleanup(root);
        if (ledger >= 0) expect(close(ledger) == 0);
    }
    Request request{};
    int const ledger = open_ledger(3, nonce, pid, deadline, request);
    expect(ledger >= 0);
    if (ledger >= 0) {
        if (ledger != REPORT_FD) {
            expect(dup2(ledger, REPORT_FD) == REPORT_FD);
            expect(close(ledger) == 0);
        }
        expect(fd_call(REPORT_FD, 2, 0) == 0);
        auto const final_closed = close_ambient();
        summary[21] = bits(final_closed);
        expect(final_closed >= 0);
        std::array<uint64_t, 2> console_identity{};
        for (size_t index = 0; index < HOST_CASES.size(); ++index) {
            unsigned const which = HOST_CASES[index];
            Owned child{};
            Wait record{};
            bool const created = launch(3, which, nonce, deadline, child, record);
            if (child.pid > 0) ++summary[17];
            expect(created && child.pid > 2);
            bool const reaped = created && consume(child, record, phase(deadline));
            if (child.consumed) ++summary[18];
            expect(reaped);
            std::copy(record.begin(), record.end(), summary.begin() + static_cast<ptrdiff_t>(44 + index * 6));
            Row row{};
            bool const read = child.consumed && read_row(REPORT_FD, 3, which, nonce, child.pid, row, phase(deadline));
            expect(read && row[H_FAILURES] == 0 && row[H_PPID] == pid);
            if (read) {
                ++summary[20];
                if (which != 3) {
                    for (size_t slot = 0; slot < 3; ++slot) {
                        size_t const at = STAT_BASE + slot * 24 + 5;
                        if (index == 0 && slot == 0) console_identity = {row[at], row[at + 1]};
                        expect(row[at] == console_identity[0] && row[at + 1] == console_identity[1]);
                    }
                }
            }
            cleanup(child);
        }
        expect(flush(REPORT_FD, phase(deadline)));
        expect(close(REPORT_FD) == 0);
    }
    expect(summary[15] == 2 && summary[16] == 2 && summary[17] == 3 && summary[18] == 3 && summary[19] == 2 && summary[20] == 13 &&
           summary[22] == 2);
    summary[9] = clock_us();
    expect(summary[9] >= start && summary[9] < deadline);
    summary[12] = failures;
    summary[254] = guard(nonce, 0, 0, 0x484F5354);
    summary[255] = ~summary[254];
    auto const receipt_deadline = phase(UINT64_MAX);
    bool const saved = save("host.bin", summary.data(), sizeof(summary), receipt_deadline);
    std::array<char, 512> status{};
    auto const count = std::snprintf(
        status.data(), status.size(),
        "%s implicit-stdio-runtime nonce=%llu uid=100 private-waves=2 cases=8 host-cases=3 rows=%llu failures=%llu deadline-us=%llu\n",
        failures == 0 && saved ? "PASS" : "FAIL", static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(summary[20]),
        static_cast<unsigned long long>(failures + (saved ? 0U : 1U)), static_cast<unsigned long long>(deadline));
    bool const status_saved =
        count > 0 && static_cast<size_t>(count) < status.size() && save("status", status.data(), status.size(), receipt_deadline);
    return failures == 0 && saved && status_saved ? 0 : 71;
}
auto init_exit_witness() -> int {
    namespace init = ker::abi::init_control;
    constexpr char SERVICE[] = "implicit-stdio-probe";
    struct GuardedSnapshot {
        uint64_t before = GUARD;
        init::StatusSnapshot value{};
        uint64_t after = ~GUARD;
    };
    static_assert(sizeof(init::StatusSnapshot) == 4160 && init::ABI_VERSION == 1 && offsetof(GuardedSnapshot, value) == 8 &&
                  offsetof(GuardedSnapshot, after) == 4168);
    auto const start = clock_us();
    bool const valid_clock = start != UINT64_MAX && start <= UINT64_MAX - 30'000'000;
    uint64_t const deadline = valid_clock ? start + 30'000'000 : 0;
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    bool observed = false, history = false;
    int64_t error = !valid_clock ? -EIO : (proc(10) != 100 || proc(11) != 100 || proc(12) != 100 || proc(13) != 100 ? -EPERM : 0);
    while (error == 0 && !observed) {
        if (!in_time(deadline)) {
            error = -ETIMEDOUT;
            break;
        }
        GuardedSnapshot guarded{};
        auto const result = ker::process::init_status_read(&guarded.value);
        if (!in_time(deadline)) {
            error = -ETIMEDOUT;
            break;
        }
        if (result != 0 && result != -EAGAIN) {
            error = result;
            break;
        }
        if (guarded.before != GUARD || guarded.after != ~GUARD) {
            error = -EOVERFLOW;
            break;
        }
        if (result == 0) {
            snapshot = guarded.value;
            bool valid = snapshot.size == sizeof(snapshot) && snapshot.version == init::ABI_VERSION && snapshot.sequence != 0 &&
                         snapshot.published_mono_ns != 0 && snapshot.service_count != 0 &&
                         snapshot.service_count <= init::MAX_STATUS_SERVICES;
            if (valid)
                for (uint32_t index = 0; index < snapshot.service_count; ++index)
                    valid = valid &&
                            std::memchr(snapshot.services[index].service, '\0', sizeof(snapshot.services[index].service)) != nullptr &&
                            snapshot.services[index].history_count <= init::MAX_TRANSITION_HISTORY;
            if (!valid) {
                error = -EPROTO;
                break;
            }
            unsigned matches{};
            for (uint32_t index = 0; index < snapshot.service_count; ++index)
                if (std::strcmp(snapshot.services[index].service, SERVICE) == 0) {
                    selected = snapshot.services[index];
                    ++matches;
                }
            if (matches > 1) {
                error = -EPROTO;
                break;
            }
            history = false;
            if (matches == 1)
                for (uint32_t index = 0; index < selected.history_count; ++index) {
                    auto const& transition = selected.history[index];
                    if (transition.reason == init::TransitionReason::EXIT && transition.to_state == init::ServiceState::EXITED &&
                        transition.timestamp_mono_ns != 0 && transition.timestamp_mono_ns <= snapshot.published_mono_ns)
                        history = true;
                }
            observed = matches == 1 && selected.generation == 1 && selected.restart_count == 0 &&
                       selected.state == init::ServiceState::EXITED && selected.readiness == init::ReadinessState::READY &&
                       selected.pid == -1 && selected.pgid == -1 && selected.last_wait_status == 0 && selected.last_error == 0 && history;
        }
        if (!observed) pause();
    }
    auto const receipt_deadline = phase(UINT64_MAX);
    bool const snapshot_saved = save("init-exit-snapshot.bin", &snapshot, sizeof(snapshot), receipt_deadline);
    bool const passed = observed && error == 0 && snapshot_saved;
    std::array<char, 512> status{};
    auto const count = std::snprintf(
        status.data(), status.size(),
        "%s implicit-stdio-init-exit service=implicit-stdio-probe generation=%llu restarts=%llu sequence=%llu state=%u readiness=%u "
        "wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u read-error=%lld failures=%u\n",
        passed ? "PASS" : "FAIL", static_cast<unsigned long long>(selected.generation),
        static_cast<unsigned long long>(selected.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned>(selected.state), static_cast<unsigned>(selected.readiness), selected.last_wait_status, selected.last_error,
        history ? 1U : 0U, snapshot.size, snapshot.version, static_cast<long long>(error), passed ? 0U : 1U);
    bool const saved =
        count > 0 && static_cast<size_t>(count) < status.size() && save("init-exit-status", status.data(), status.size(), receipt_deadline);
    return passed && saved ? 0 : 72;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 1) return host();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return init_exit_witness();
    uint64_t wave{}, which{}, nonce{}, deadline{}, action{};
    if (argc == 5 && std::strcmp(argv[1], "--root") == 0 && number(argv[2], wave) && (wave == 1 || wave == 2) && number(argv[3], nonce) &&
        nonce != 0 && number(argv[4], deadline) && deadline != 0)
        return private_root(static_cast<unsigned>(wave), nonce, deadline);
    if (argc == 7 && std::strcmp(argv[1], "--leaf") == 0 && number(argv[2], wave) && wave >= 1 && wave <= 3 && number(argv[3], which) &&
        which >= 1 && which <= 4 && number(argv[4], nonce) && nonce != 0 && number(argv[5], deadline) && deadline != 0 &&
        number(argv[6], action))
        return leaf(static_cast<unsigned>(wave), static_cast<unsigned>(which), nonce, deadline, action, ENTRY);
    return 73;
}
