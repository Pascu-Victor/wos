// Two uninjected public native waves. Kernel/controller/census behavior is unchanged.
#include <abi/callnums/process.h>
#include <abi/callnums/time.h>
#include <abi/callnums/vfs.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <abi/compartment.hpp>
#include <abi/init_control.hpp>
#include <abi/resource.hpp>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "lifecycle_raw_pilot_expectations.hpp"

namespace {
namespace wire = ker::abi::compartment;
namespace proc = ker::abi::process;
namespace init = ker::abi::init_control;
constexpr char RESULTS[] = "/srv/lifecycle-raw-pilot-results/";
constexpr char ASSETS[] = "/usr/libexec/lifecycle-raw-artifacts/";
constexpr uint64_t HOST_MAGIC = 0x413150494c4f5452, INPUT_MAGIC = 0x4131524157494e50;
constexpr uint64_t READY_MAGIC = 0x4131524157525054, HB_MAGIC = 0x41314c4956454842;
constexpr uint64_t TINY_MARKER = 0x54494e593431484f, USER_END = 0x0000800000000000;
constexpr uint64_t RUN_US = 30'000'000, STEP_US = 5'000'000, CHILD_US = 10'000'000;
constexpr uint64_t GUARD = 0xa596c387e1d24b6f;
constexpr int64_t BAD_FD = -9, INTERRUPTED = -4, AGAIN = -11;
template <size_t N>
using Words = std::array<uint64_t, N>;
using HostReport = Words<256>;
using Input = Words<64>;
using Ready = Words<256>;
using Heartbeat = Words<32>;
using Limits = std::array<ker::abi::resource::Limit, 16>;
static_assert(sizeof(HostReport) == 2048 && sizeof(Input) == 512 && sizeof(Ready) == 2048 && sizeof(Heartbeat) == 256);
static_assert(sizeof(wire::Spec) == 408 && sizeof(struct stat) == 144 && std::endian::native == std::endian::little);
static_assert(pilot_artifacts::STATIC_BYTES.size() + pilot_artifacts::TINY_BYTES.size() + pilot_artifacts::INTERP_BYTES.size() == 44584);
static_assert(pilot_artifacts::MAIN_ENTRY == 0x401000 && pilot_artifacts::STATIC_PHNUM == 4 && pilot_artifacts::TINY_PHNUM == 5 &&
              pilot_artifacts::INTERP_ENTRY == 0x1000 && pilot_artifacts::INTERP_PHNUM == 7);

enum class Stage : uint64_t {
    CLOCK = 1,
    IDENTITY,
    BASELINE,
    ASSET,
    SOURCE,
    POLICY,
    INPUT,
    CREATE,
    READY,
    HEARTBEAT,
    GRANT_CLOSE,
    KILL,
    WAIT,
    REOPEN,
    PERSIST,
    FINAL,
    WITNESS
};
struct Timespec {
    int64_t seconds;
    int64_t nanoseconds;
};
static_assert(sizeof(Timespec) == 16);
auto address(const void* p) -> uint64_t { return reinterpret_cast<uint64_t>(p); }
auto call(uint64_t subsystem, uint64_t op, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0, uint64_t a5 = 0) -> int64_t {
    register uint64_t rax __asm__("rax") = subsystem;
    register uint64_t rdi __asm__("rdi") = op;
    register uint64_t rsi __asm__("rsi") = a1;
    register uint64_t rdx __asm__("rdx") = a2;
    register uint64_t r8 __asm__("r8") = a3;
    register uint64_t r9 __asm__("r9") = a4;
    register uint64_t r10 __asm__("r10") = a5;
    __asm__ volatile("syscall" : "+a"(rax) : "D"(rdi), "S"(rsi), "d"(rdx), "r"(r8), "r"(r9), "r"(r10) : "rcx", "r11", "memory");
    return static_cast<int64_t>(rax);
}
auto process(proc::procmgmt_ops op, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0) -> int64_t {
    return call(3, static_cast<uint64_t>(op), a1, a2, a3, a4);
}
auto vfs(ker::abi::vfs::ops op, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0) -> int64_t {
    return call(5, static_cast<uint64_t>(op), a1, a2, a3, a4);
}
auto fcntl_raw(int fd, int command) -> int64_t { return vfs(ker::abi::vfs::ops::FCNTL, static_cast<uint64_t>(fd), command); }
auto pread_raw(int fd, void* p, size_t bytes, uint64_t offset) -> int64_t {
    return vfs(ker::abi::vfs::ops::PREAD, static_cast<uint64_t>(fd), address(p), bytes, offset);
}
auto pwrite_raw(int fd, const void* p, size_t bytes, uint64_t offset) -> int64_t {
    return vfs(ker::abi::vfs::ops::PWRITE, static_cast<uint64_t>(fd), address(p), bytes, offset);
}
auto stat_raw(int fd, struct stat& value) -> int64_t { return vfs(ker::abi::vfs::ops::FSTAT, static_cast<uint64_t>(fd), address(&value)); }
struct FdSnapshot {
    std::array<int64_t, 256> fd{};
    std::array<int64_t, 256> fl{};
    Words<4> present{};
    uint64_t count{};
};
struct Context {
    HostReport report{};
    FdSnapshot baseline{};
    Limits limits{};
    Words<8> identity{};
    uint64_t last_clock{};
    uint64_t deadline{};
    uint64_t checks{};
    uint64_t failures{};
    uint64_t failure_deadline{};
    Stage first{Stage::CLOCK};
    bool clock_failed{};
    auto expect(bool value, Stage stage) -> bool {
        ++checks;
        if (!value) {
            if (failures == 0) {
                first = stage;
                // One aggregate cleanup/receipt allowance; it never extends
                // when a later failure or persistence attempt occurs.
                if (last_clock <= UINT64_MAX - STEP_US) failure_deadline = last_clock + STEP_US;
            }
            ++failures;
        }
        return value;
    }
};
static_assert(sizeof(Context) < 8192);
auto now_us(Context& c) -> uint64_t {
    Timespec t{};
    auto rc = call(4, static_cast<uint64_t>(ker::abi::sys_time_ops::CLOCK_GETTIME), address(&t), 1);
    bool valid = rc == 0 && t.seconds >= 0 && t.nanoseconds >= 0 && t.nanoseconds < 1'000'000'000;
    uint64_t sec = valid ? static_cast<uint64_t>(t.seconds) : UINT64_MAX;
    valid = valid && sec <= (UINT64_MAX - 999999) / 1'000'000;
    uint64_t value = valid ? sec * 1'000'000 + static_cast<uint64_t>(t.nanoseconds) / 1000 : UINT64_MAX;
    valid = valid && value >= c.last_clock;
    if (!c.expect(valid, Stage::CLOCK)) {
        c.clock_failed = true;
        return UINT64_MAX;
    }
    c.last_clock = value;
    return value;
}
auto before(Context& c, uint64_t deadline) -> bool {
    auto time = now_us(c);
    return time != UINT64_MAX && time < deadline;
}
auto pause(Context& c, uint64_t deadline) -> bool {
    if (!before(c, deadline)) return false;
    Timespec const request{0, 1'000'000};
    return c.expect(call(4, static_cast<uint64_t>(ker::abi::sys_time_ops::NANOSLEEP), address(&request)) == 0, Stage::CLOCK) &&
           before(c, deadline);
}
auto close_owned(Context& c, int& fd, Stage stage) -> int64_t {
    if (fd < 0) return 0;
    int const old = fd;
    fd = -1;  // No ambiguous/reused descriptor is retried after a failed close.
    auto rc = vfs(ker::abi::vfs::ops::CLOSE, static_cast<uint64_t>(old));
    c.expect(rc == 0, stage);
    return rc;
}
auto path(char (&out)[192], const char* prefix, const char* name) -> bool {
    size_t used = 0;
    for (auto* s : {prefix, name})
        for (size_t i = 0; s[i] != '\0'; ++i) {
            if (used + 1 >= sizeof(out)) return false;
            out[used++] = s[i];
        }
    out[used] = '\0';
    return true;
}
auto open_raw(Context& c, const char* prefix, const char* name, int flags, unsigned mode, Stage stage) -> int {
    char full[192]{};
    if (!c.expect(path(full, prefix, name), stage)) return -1;
    auto rc = vfs(ker::abi::vfs::ops::OPEN, address(full), static_cast<uint64_t>(flags), mode);
    if (!c.expect(rc >= 0 && rc < 256, stage)) return -1;
    return static_cast<int>(rc);
}
auto scan(Context& c, FdSnapshot& out, Stage stage) -> bool {
    out = {};
    bool valid = true;
    for (int fd = 0; fd < 256; ++fd) {
        auto const flags = fcntl_raw(fd, F_GETFD);
        out.fd[static_cast<size_t>(fd)] = flags;
        if (flags == BAD_FD) {
            out.fl[static_cast<size_t>(fd)] = BAD_FD;
            continue;
        }
        auto const fl = fcntl_raw(fd, F_GETFL);
        out.fl[static_cast<size_t>(fd)] = fl;
        valid = c.expect(flags >= 0 && fl >= 0, stage) && valid;
        if (flags >= 0) {
            out.present[static_cast<unsigned>(fd) / 64] |= uint64_t{1} << (fd % 64);
            ++out.count;
        }
    }
    return valid;
}
auto baseline_equal(Context& c, Stage stage) -> bool {
    FdSnapshot current{};
    return scan(c, current, stage) && c.expect(current.fd == c.baseline.fd && current.fl == c.baseline.fl, stage);
}
auto allowlist(Context& c, const std::array<int, 3>& extra) -> bool {
    FdSnapshot current{};
    if (!scan(c, current, Stage::SOURCE)) return false;
    bool valid = true;
    for (size_t fd = 0; fd < 256; ++fd) {
        bool allowed = false;
        for (int value : extra) allowed = allowed || (value >= 0 && static_cast<size_t>(value) == fd);
        if (allowed)
            valid =
                c.expect(c.baseline.fd[fd] == BAD_FD && current.fd[fd] >= 0 && (current.fd[fd] & FD_CLOEXEC) != 0, Stage::SOURCE) && valid;
        else
            valid = c.expect(current.fd[fd] == c.baseline.fd[fd] && current.fl[fd] == c.baseline.fl[fd], Stage::SOURCE) && valid;
    }
    return valid;
}
auto identity(Context& c, Words<8>& values, uint64_t& target_flags, Limits& limits) -> bool {
    constexpr std::array<proc::procmgmt_ops, 4> ids{proc::procmgmt_ops::GETUID, proc::procmgmt_ops::GETEUID, proc::procmgmt_ops::GETGID,
                                                    proc::procmgmt_ops::GETEGID};
    bool valid = true;
    for (size_t i = 0; i < ids.size(); ++i) {
        auto rc = process(ids[i]);
        values[i] = static_cast<uint64_t>(rc);
        valid = c.expect(rc == 100, Stage::IDENTITY) && valid;
    }
    constexpr std::array<uint64_t, 3> controls{39, 0x574f5301, 0x574f5304};
    constexpr std::array<int64_t, 3> expected{1, 0, 256};
    for (size_t i = 0; i < controls.size(); ++i) {
        auto rc = process(proc::procmgmt_ops::PRCTL, controls[i]);
        values[4 + i] = static_cast<uint64_t>(rc);
        valid = c.expect(rc == expected[i], Stage::IDENTITY) && valid;
    }
    auto groups = process(proc::procmgmt_ops::GETGROUPS);
    values[7] = static_cast<uint64_t>(groups);
    valid = c.expect(groups == 0, Stage::IDENTITY) && valid;
    char hostname[256]{};
    uint32_t flags{};
    auto target = process(proc::procmgmt_ops::GETWKITARGET, address(hostname), sizeof(hostname), address(&flags));
    target_flags = flags;
    valid = c.expect(target == 0 && hostname[0] == '\0' && flags == 2, Stage::IDENTITY) && valid;
    for (size_t i = 0; i < limits.size(); ++i)
        valid = c.expect(process(proc::procmgmt_ops::GETRLIMIT, i, address(&limits[i]), 1) == 0 && limits[i].soft <= limits[i].hard,
                         Stage::POLICY) &&
                valid;
    return valid;
}
auto limits_equal(const Limits& a, const Limits& b) -> bool {
    for (size_t i = 0; i < a.size(); ++i)
        if (a[i].soft != b[i].soft || a[i].hard != b[i].hard) return false;
    return true;
}
auto policy(Context& c, wire::Spec& spec) -> bool {
    for (size_t i = 0; i < c.limits.size(); ++i) spec.limits[i] = {c.limits[i].soft, c.limits[i].hard};
    bool valid = c.expect(c.limits[0].soft >= 10 && c.limits[0].hard >= 10, Stage::POLICY);
    spec.limits[0] = {10, 10};
    for (auto [index, ceiling] : {std::array<uint64_t, 2>{7, 256}, std::array<uint64_t, 2>{1, 1024 * 1024}}) {
        auto& value = spec.limits[index];
        if (value.soft > ceiling) value.soft = ceiling;
        if (value.hard > ceiling) value.hard = ceiling;
    }
    valid = c.expect(spec.limits[7].soft >= 10 && spec.limits[7].hard >= 10 && spec.limits[1].soft >= 8192 && spec.limits[1].hard >= 8192,
                     Stage::POLICY) &&
            valid;
    for (size_t i = 0; i < spec.limits.size(); ++i)
        valid = c.expect(spec.limits[i].soft <= spec.limits[i].hard && spec.limits[i].soft <= c.limits[i].soft &&
                             spec.limits[i].hard <= c.limits[i].hard,
                         Stage::POLICY) &&
                valid;
    return valid;
}
auto same_stat(const struct stat& a, const struct stat& b) -> bool {
    return a.st_dev == b.st_dev && a.st_ino == b.st_ino && a.st_size == b.st_size && a.st_mode == b.st_mode && a.st_uid == b.st_uid &&
           a.st_gid == b.st_gid;
}
template <size_t N>
auto asset_exact(Context& c, int fd, const std::array<unsigned char, N>& expected, struct stat& original, bool first) -> bool {
    struct stat value{};
    if (!c.expect(stat_raw(fd, value) == 0 && value.st_size == static_cast<int64_t>(N) && value.st_mode == (S_IFREG | 0555) &&
                      value.st_uid == 0 && value.st_gid == 0 && (first || same_stat(original, value)),
                  Stage::ASSET))
        return false;
    if (first) original = value;
    std::array<unsigned char, 512> bytes{};
    for (size_t offset = 0; offset < N;) {
        if (!c.expect(before(c, c.deadline), Stage::ASSET)) return false;
        size_t count = N - offset;
        if (count > bytes.size()) count = bytes.size();
        if (!c.expect(pread_raw(fd, bytes.data(), count, offset) == static_cast<int64_t>(count), Stage::ASSET)) return false;
        for (size_t i = 0; i < count; ++i)
            if (!c.expect(bytes[i] == expected[offset + i], Stage::ASSET)) return false;
        offset += count;
    }
    unsigned char eof{};
    return c.expect(pread_raw(fd, &eof, 1, N) == 0, Stage::ASSET);
}
template <size_t N>
auto equal_reads(Context& c, int fd, Words<N>& out, uint64_t offset, Stage stage, bool& same) -> bool {
    Words<N> first{}, second{};
    bool valid = c.expect(pread_raw(fd, first.data(), sizeof(first), offset) == static_cast<int64_t>(sizeof(first)), stage);
    valid = c.expect(pread_raw(fd, second.data(), sizeof(second), offset) == static_cast<int64_t>(sizeof(second)), stage) && valid;
    same = valid && first == second;
    if (same) out = first;
    return valid;
}
auto user_pointer(uint64_t value, uint64_t bytes = 1) -> bool { return value >= 4096 && value < USER_END && bytes <= USER_END - value; }
auto ready_valid(Context& c, const Input& h, const Ready& r, uint64_t observed) -> bool {
    bool const tiny = h[5] == 2;
    bool good = r[0] == READY_MAGIC && r[1] == 1 && r[2] == 2048 && r[3] == 1 && r[4] == h[3] && r[5] == h[4] && r[6] == h[5] &&
                r[7] == h[5] && r[8] == 1 && r[9] == 0;
    for (size_t i = 10; i <= 13; ++i) good = good && r[i] == 100;
    good = good && r[14] == 1 && r[15] == 0 && r[16] == 256 && r[17] == (tiny ? 0xff : 0x7f) && r[18] == 0 && r[19] == 0 && r[20] == 0 &&
           r[21] > 0 && r[22] == h[6] && r[23] == h[7] && h[7] <= r[24] && r[24] <= r[25] && r[25] <= observed && observed < h[8] &&
           r[26] == h[8] && r[27] == 16 && r[28] == 255 && r[29] == h[58] && r[30] == h[56] && r[31] == h[57];
    for (size_t i = 0; i < 16; ++i) good = good && r[32 + i] == 0 && r[64 + 2 * i] == h[16 + 2 * i] && r[65 + 2 * i] == h[17 + 2 * i];
    good = good && r[48] < 256 && r[48] != 9 && static_cast<int64_t>(r[49]) >= 0 && (r[49] & 3) == 2 && r[50] == 0 && r[51] > 0 &&
           r[51] <= INT32_MAX && user_pointer(r[52], 4096) && (r[52] & 4095) == 0 && r[53] == r[51] && r[54] == 0 &&
           static_cast<int64_t>(r[55]) == -1 && static_cast<int64_t>(r[56]) == -1 && r[57] == 16 && r[58] == ~uint64_t{16} && r[61] == 32 &&
           r[62] == (S_IFREG | 0600) && r[63] == 100;
    good = good && user_pointer(r[96], 1024) && (r[96] & 15) == 0 && r[97] == pilot_artifacts::MAIN_ENTRY &&
           r[98] == pilot_artifacts::MAIN_ENTRY && r[105] == 1 && r[106] == 0 && r[107] == 0x1040 && r[108] == 56 &&
           r[109] == (tiny ? pilot_artifacts::TINY_PHNUM : pilot_artifacts::STATIC_PHNUM) && r[110] == 4096 && r[111] == 0;
    // Incoming RDI/RSI/RDX and static R12 are opaque: direct sysret and iret differ.
    if (tiny)
        good = good && r[99] >= 0x30000000 && r[99] < 0x70000000 && (r[99] & 0x1fffff) == 0 &&
               r[100] == r[99] + pilot_artifacts::INTERP_ENTRY && r[101] == TINY_MARKER;
    else
        good = good && r[99] == 0 && r[100] == 0;
    good = good && r[112] == 8192 && r[113] == 0 && r[114] == 512 && r[115] == 100 && r[116] == 0x4141 && r[117] == 4096 && r[118] == 100 &&
           r[119] == 100 && r[120] == 0600 && r[121] == 1 && r[122] == h[3] && r[123] == h[4] && r[124] == 16 && r[125] == ~uint64_t{16} &&
           r[126] == h[3] && r[127] == h[4];
    for (size_t i = 0; i < 16; ++i)
        good = good && r[128 + i] == 32 && r[144 + i] == 32 && r[160 + i] == r[51] && r[176 + i] == 0 && r[192 + i] == 1 && r[208 + i] == 1;
    good = good && r[224] == h[3] && r[225] == 0 && r[226] == 1;
    for (size_t i = 227; i <= 230; ++i) good = good && r[i] == 100;
    good = good && r[231] == 1 && r[232] == 1 && r[233] == (tiny ? 1U : 0U) && user_pointer(r[234], 16) && user_pointer(r[235], 19) &&
           r[236] == (tiny ? 13U : 12U) && r[237] == (tiny ? pilot_artifacts::INTERP_ENTRY : 0) && r[238] == 0 && r[239] == 0 &&
           r[255] == h[3];
    for (size_t i = 240; i < 255; ++i) good = good && r[i] == 0;
    return c.expect(good, Stage::READY);
}
auto heartbeat_valid(Context& c, const Input& h, const Ready& r, const Heartbeat& b, uint64_t minimum, uint64_t observed) -> bool {
    bool good = b[0] == HB_MAGIC && b[1] == 1 && b[2] == h[3] && b[3] == h[4] && b[4] == h[5] && b[5] >= minimum && b[5] >= 17 &&
                b[5] <= 10000 && b[6] >= r[25] && b[6] <= observed && b[6] < h[8] && b[7] == 32 && b[8] == 32 && b[9] == r[51] &&
                b[10] == 0 && b[11] == b[5] && b[12] == ~b[5] && b[13] >= r[21] && b[14] == 0 && b[15] == 0 && b[16] == r[48] &&
                b[17] == r[51] && b[18] == r[52] && b[19] == 0 && b[20] == 1 && b[21] == 1 && b[22] == 100 && b[23] == 1 && b[31] == b[5];
    for (size_t i = 24; i <= 30; ++i) good = good && b[i] == 0;
    return c.expect(good, Stage::HEARTBEAT);
}
auto poll_ready(Context& c, int fd, const Input& h, Ready& out, uint64_t deadline, uint64_t& observation) -> bool {
    while (before(c, deadline)) {
        bool same{};
        Ready value{};
        if (!equal_reads(c, fd, value, 4096, Stage::READY, same)) return false;
        auto time = now_us(c);
        if (time == UINT64_MAX || time >= deadline) break;
        if (same && value[255] != 0) {
            if (!ready_valid(c, h, value, time)) return false;
            out = value;
            observation = time;
            return true;
        }
        if (!pause(c, deadline)) break;
    }
    return c.expect(false, Stage::READY);
}
auto poll_heartbeat(Context& c, int fd, const Input& h, const Ready& r, Heartbeat& out, uint64_t minimum, uint64_t deadline,
                    uint64_t& observation) -> bool {
    while (before(c, deadline)) {
        Heartbeat value{};
        bool same{};
        if (!equal_reads(c, fd, value, 1024, Stage::HEARTBEAT, same)) return false;
        auto time = now_us(c);
        if (time == UINT64_MAX || time >= deadline) break;
        if (same && value[31] != 0 && value[31] == value[5]) {
            if (!heartbeat_valid(c, h, r, value, minimum, time)) return false;
            out = value;
            observation = time;
            return true;
        }
        if (!pause(c, deadline)) break;
    }
    return c.expect(false, Stage::HEARTBEAT);
}
auto persist(Context& c, const char* name, const void* data, size_t bytes, uint64_t deadline) -> bool {
    if (!c.expect(before(c, deadline), Stage::PERSIST)) return false;
    int fd = open_raw(c, RESULTS, name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600, Stage::PERSIST);
    if (fd < 0) return false;
    bool good = c.expect(pwrite_raw(fd, data, bytes, 0) == static_cast<int64_t>(bytes), Stage::PERSIST);
    good = c.expect(vfs(ker::abi::vfs::ops::FSYNC, static_cast<uint64_t>(fd)) == 0, Stage::PERSIST) && good;
    good = close_owned(c, fd, Stage::PERSIST) == 0 && good;
    return c.expect(before(c, deadline), Stage::PERSIST) && good;
}
struct Wave {
    Wave() = default;
    Wave(const Wave&) = delete;
    auto operator=(const Wave&) -> Wave& = delete;
    Wave(Wave&&) = delete;
    auto operator=(Wave&&) -> Wave& = delete;
    int main_fd{-1}, interpreter_fd{-1}, report_fd{-1}, reopen_fd{-1};
    int64_t pid{};
    bool reaped{};
    bool wait_ownership_uncertain{};
    bool input_committed{}, ready_accepted{}, heartbeat_accepted{};
    std::array<bool, 3> receipt_attempted{};
    struct stat main_stat{}, interpreter_stat{}, report_stat{};
    Input input{};
    Ready ready{};
    Heartbeat heartbeat{};
};
static_assert(sizeof(Wave) < 4096);
auto wait_owned(Context& c, Wave& wave, uint64_t* row, uint64_t deadline) -> bool {
    if (!c.expect(wave.pid > 2 && !wave.reaped, Stage::WAIT)) return false;
    row[55] = now_us(c);
    for (unsigned attempts = 0; attempts < 5000 && before(c, deadline); ++attempts) {
        int32_t status{};
        auto rc = process(proc::procmgmt_ops::WAITPID, static_cast<uint64_t>(wave.pid), address(&status), WNOHANG, 0);
        row[13] = static_cast<uint64_t>(rc);
        row[14] = static_cast<uint64_t>(static_cast<int64_t>(status));
        if (rc == wave.pid) {
            wave.reaped = true;
            row[17] = 1;  // Consumed before any following I/O or clock.
            row[56] = now_us(c);
            return c.expect(status == 9 && row[56] != UINT64_MAX && row[56] < deadline, Stage::WAIT);
        }
        if (rc == 0)
            ++row[15];
        else if (rc == INTERRUPTED)
            ++row[16];
        else {
            // Unexpected consuming/error results invalidate further PID-only
            // signal authority. Do not risk a reused PID during failure cleanup.
            wave.wait_ownership_uncertain = true;
            return c.expect(false, Stage::WAIT);
        }
        if (!pause(c, deadline)) break;
    }
    row[56] = now_us(c);
    return c.expect(false, Stage::WAIT);
}
auto execute_wave(Context& c, uint64_t mode, uint64_t cookie) -> bool {
    auto* row = c.report.data() + 128 + (mode - 1) * 64;
    row[0] = mode;
    row[1] = cookie;
    uint64_t const failures_before = c.failures;
    Wave wave{};
    auto execute = [&]() -> bool {
        wave.main_fd = open_raw(c, ASSETS, mode == 1 ? "lifecycle-raw-static" : "lifecycle-raw-tiny", O_RDONLY | O_CLOEXEC | O_NOFOLLOW, 0,
                                Stage::ASSET);
        if (wave.main_fd < 0) return false;
        if (mode == 2) {
            wave.interpreter_fd = open_raw(c, ASSETS, "lifecycle-tiny-ld.so", O_RDONLY | O_CLOEXEC | O_NOFOLLOW, 0, Stage::ASSET);
            if (wave.interpreter_fd < 0) return false;
        }
        row[18] = mode == 2 ? 2 : 1;
        bool exact = mode == 1 ? asset_exact(c, wave.main_fd, pilot_artifacts::STATIC_BYTES, wave.main_stat, true)
                               : asset_exact(c, wave.main_fd, pilot_artifacts::TINY_BYTES, wave.main_stat, true);
        if (mode == 2) exact = asset_exact(c, wave.interpreter_fd, pilot_artifacts::INTERP_BYTES, wave.interpreter_stat, true) && exact;
        if (!exact) return false;
        char const* raw_name = mode == 1 ? "wave-static.raw" : "wave-tiny.raw";
        wave.report_fd = open_raw(c, RESULTS, raw_name, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600, Stage::SOURCE);
        if (wave.report_fd < 0) return false;
        row[20] = static_cast<uint64_t>(wave.report_fd);
        auto truncate = vfs(ker::abi::vfs::ops::TRUNCATE, static_cast<uint64_t>(wave.report_fd), 8192);
        auto flags = fcntl_raw(wave.report_fd, F_GETFL);
        if (!c.expect(truncate == 0 && stat_raw(wave.report_fd, wave.report_stat) == 0 && wave.report_stat.st_size == 8192 &&
                          wave.report_stat.st_mode == (S_IFREG | 0600) && wave.report_stat.st_uid == 100 &&
                          wave.report_stat.st_gid == 100 && flags >= 0 && (flags & O_ACCMODE) == O_RDWR,
                      Stage::SOURCE))
            return false;
        row[28] = wave.report_stat.st_ino;
        row[29] = wave.report_stat.st_dev;
        row[30] = static_cast<uint64_t>(flags);
        row[54] = 8192;
        if (!allowlist(c, {wave.main_fd, wave.interpreter_fd, wave.report_fd})) return false;
        row[25] = 1;
        Words<8> current_identity{};
        Limits current_limits{};
        uint64_t current_flags{};
        if (!identity(c, current_identity, current_flags, current_limits) ||
            !c.expect(current_identity == c.identity && current_flags == c.report[104] && limits_equal(current_limits, c.limits),
                      Stage::POLICY))
            return false;
        wire::Spec spec{};
        spec.root_bytes = 8 * 1024 * 1024;
        if (!policy(c, spec)) return false;
        constexpr char executable[] = "/app/lifecycle-raw", interpreter[] = "/lib/lifecycle-tiny-ld.so";
        wire::String argument{address(executable), sizeof(executable) - 1};
        std::array<wire::Asset, 2> assets{
            {{wave.main_fd, 0755, argument, 0}, {wave.interpreter_fd, 0755, {address(interpreter), sizeof(interpreter) - 1}, 0}}};
        wire::Descriptor grant{wave.report_fd, 9, 0};
        spec.executable = argument;
        spec.assets = address(assets.data());
        spec.asset_count = row[18];
        spec.descriptors = address(&grant);
        spec.descriptor_count = 1;
        spec.argv = address(&argument);
        spec.argc = 1;
        auto* h = wave.input.data();
        row[4] = now_us(c);
        if (!c.expect(row[4] != UINT64_MAX && row[4] <= UINT64_MAX - CHILD_US && row[4] < c.deadline, Stage::INPUT)) return false;
        row[7] = row[4] + CHILD_US;
        h[0] = INPUT_MAGIC;
        h[1] = 1;
        h[2] = 512;
        h[3] = c.report[4];
        h[4] = cookie;
        h[5] = mode;
        h[6] = c.report[5];
        h[7] = row[4];
        h[8] = row[7];
        h[9] = 100;
        h[10] = 1;
        h[12] = 256;
        for (size_t i = 0; i < 16; ++i) {
            h[16 + 2 * i] = spec.limits[i].soft;
            h[17 + 2 * i] = spec.limits[i].hard;
        }
        h[48] = 4096;
        h[49] = 2048;
        h[50] = 8192;
        h[51] = 32;
        h[52] = 0x4141;
        h[53] = 4096;
        h[54] = 1;
        h[56] = row[28];
        h[57] = row[29];
        h[58] = row[30];
        h[59] = 1024;
        h[60] = 256;
        h[61] = 16;
        h[62] = 10000;
        if (!c.expect(pwrite_raw(wave.report_fd, h, sizeof(wave.input), 0) == 512, Stage::INPUT)) return false;
        h[63] = h[3];
        if (!c.expect(pwrite_raw(wave.report_fd, &h[63], 8, 504) == 8 &&
                          vfs(ker::abi::vfs::ops::FSYNC, static_cast<uint64_t>(wave.report_fd)) == 0,
                      Stage::INPUT))
            return false;
        Input readback{};
        if (!c.expect(pread_raw(wave.report_fd, readback.data(), sizeof(readback), 0) == 512 && readback == wave.input, Stage::INPUT))
            return false;
        wave.input_committed = true;
        row[5] = now_us(c);
        if (!c.expect(row[5] != UINT64_MAX && row[5] < c.deadline && row[5] < row[7] - 2'000'000, Stage::CREATE)) return false;
        auto const created = process(proc::procmgmt_ops::COMPARTMENT_CREATE, address(&spec));
        row[2] = static_cast<uint64_t>(created);
        if (created > 2) {
            wave.pid = created;
            row[3] = static_cast<uint64_t>(created);
        }
        // Positive ownership is recorded before any clock, close, read or other I/O.
        row[6] = now_us(c);
        if (!c.expect(created > 2 && row[6] != UINT64_MAX && row[6] < c.deadline && row[6] < row[7] - 2'000'000, Stage::CREATE))
            return false;
        exact = mode == 1 ? asset_exact(c, wave.main_fd, pilot_artifacts::STATIC_BYTES, wave.main_stat, false)
                          : asset_exact(c, wave.main_fd, pilot_artifacts::TINY_BYTES, wave.main_stat, false);
        if (mode == 2) exact = asset_exact(c, wave.interpreter_fd, pilot_artifacts::INTERP_BYTES, wave.interpreter_stat, false) && exact;
        if (close_owned(c, wave.main_fd, Stage::ASSET) == 0)
            ++row[19];
        else
            exact = false;
        if (mode == 2) {
            if (close_owned(c, wave.interpreter_fd, Stage::ASSET) == 0)
                ++row[19];
            else
                exact = false;
        }
        if (!exact) return false;
        row[49] = 1;
        uint64_t poll_deadline = c.deadline - row[6] < STEP_US ? c.deadline : row[6] + STEP_US;
        if (poll_deadline > row[7] - 2'000'000) poll_deadline = row[7] - 2'000'000;
        if (poll_deadline > c.deadline) poll_deadline = c.deadline;
        if (!poll_ready(c, wave.report_fd, wave.input, wave.ready, poll_deadline, row[8])) return false;
        wave.ready_accepted = true;
        row[23] = 1;
        if (!poll_heartbeat(c, wave.report_fd, wave.input, wave.ready, wave.heartbeat, 17, poll_deadline, row[9])) return false;
        wave.heartbeat_accepted = true;
        if (!c.expect(process(proc::procmgmt_ops::KILL, static_cast<uint64_t>(wave.pid), 0) == 0, Stage::HEARTBEAT)) return false;
        row[22] = static_cast<uint64_t>(vfs(ker::abi::vfs::ops::FSYNC, static_cast<uint64_t>(wave.report_fd)));
        Ready stable_ready{};
        bool same{};
        if (!c.expect(row[22] == 0, Stage::HEARTBEAT) || !equal_reads(c, wave.report_fd, stable_ready, 4096, Stage::READY, same) ||
            !c.expect(same && stable_ready == wave.ready, Stage::READY))
            return false;
        uint64_t const minimum = wave.heartbeat[5];
        if (!poll_heartbeat(c, wave.report_fd, wave.input, wave.ready, wave.heartbeat, minimum, poll_deadline, row[9])) return false;
        row[24] = 1;
        row[10] = wave.heartbeat[5];
        auto const& r = wave.ready;
        row[31] = r[4];
        row[32] = r[5];
        row[33] = r[21];
        row[34] = r[24];
        row[35] = r[25];
        row[36] = wave.heartbeat[6];
        row[37] = r[48];
        row[38] = r[51];
        row[39] = r[52];
        row[40] = r[59];
        row[41] = r[60];
        row[42] = r[98];
        row[43] = r[109];
        row[44] = r[237];
        row[45] = r[99];
        row[46] = r[101];
        row[47] = r[105];
        row[48] = r[106];
        row[50] = 1;
        row[51] = 1;
        row[52] = 1;
        row[53] = 1;
        row[21] = static_cast<uint64_t>(close_owned(c, wave.report_fd, Stage::GRANT_CLOSE));
        if (!c.expect(row[21] == 0, Stage::GRANT_CLOSE) || !baseline_equal(c, Stage::GRANT_CLOSE)) return false;
        row[26] = 1;
        row[59] = 1;
        row[11] = now_us(c);
        if (!c.expect(row[11] != UINT64_MAX && row[11] < c.deadline && row[11] < row[7] - 1'000'000 && row[11] >= row[8] &&
                          row[11] - row[8] <= 2'000'000 && row[11] >= row[9] && row[11] - row[9] <= 2'000'000,
                      Stage::KILL))
            return false;
        row[12] = static_cast<uint64_t>(process(proc::procmgmt_ops::KILL, static_cast<uint64_t>(wave.pid), SIGKILL));
        if (!c.expect(row[12] == 0, Stage::KILL)) return false;
        uint64_t wait_deadline = c.deadline - row[11] < STEP_US ? c.deadline : row[11] + STEP_US;
        if (wait_deadline > c.deadline) wait_deadline = c.deadline;
        if (!wait_owned(c, wave, row, wait_deadline)) return false;
        wave.reopen_fd = open_raw(c, RESULTS, raw_name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW, 0, Stage::REOPEN);
        if (wave.reopen_fd < 0) return false;
        struct stat final_stat{};
        Input final_input{};
        Ready final_ready{};
        bool final_good = c.expect(stat_raw(wave.reopen_fd, final_stat) == 0 && same_stat(final_stat, wave.report_stat), Stage::REOPEN);
        final_good =
            c.expect(pread_raw(wave.reopen_fd, final_input.data(), sizeof(final_input), 0) == 512 && final_input == wave.input &&
                         pread_raw(wave.reopen_fd, final_ready.data(), sizeof(final_ready), 4096) == 2048 && final_ready == wave.ready,
                     Stage::REOPEN) &&
            final_good;
        final_good = close_owned(c, wave.reopen_fd, Stage::REOPEN) == 0 && final_good;
        if (!final_good) return false;
        row[60] = 1;
        wave.receipt_attempted[0] = true;
        bool saved = persist(c, mode == 1 ? "static-input.bin" : "tiny-input.bin", wave.input.data(), sizeof(wave.input), c.deadline);
        wave.receipt_attempted[1] = true;
        saved = persist(c, mode == 1 ? "static-ready.bin" : "tiny-ready.bin", wave.ready.data(), sizeof(wave.ready), c.deadline) && saved;
        wave.receipt_attempted[2] = true;
        saved = persist(c, mode == 1 ? "static-heartbeat.bin" : "tiny-heartbeat.bin", wave.heartbeat.data(), sizeof(wave.heartbeat),
                        c.deadline) &&
                saved;
        if (!saved || !baseline_equal(c, Stage::FINAL)) return false;
        Words<8> after{};
        Limits limits{};
        uint64_t after_flags{};
        if (!identity(c, after, after_flags, limits) ||
            !c.expect(after == c.identity && after_flags == c.report[104] && limits_equal(limits, c.limits), Stage::FINAL))
            return false;
        row[27] = 1;
        row[61] = 1;
        return true;
    };
    bool const completed = execute();
    if (wave.main_fd >= 0 && close_owned(c, wave.main_fd, Stage::ASSET) == 0) ++row[19];
    if (wave.interpreter_fd >= 0 && close_owned(c, wave.interpreter_fd, Stage::ASSET) == 0) ++row[19];
    if (wave.report_fd >= 0) row[21] = static_cast<uint64_t>(close_owned(c, wave.report_fd, Stage::GRANT_CLOSE));
    close_owned(c, wave.reopen_fd, Stage::REOPEN);
    if (wave.pid > 2 && !wave.reaped && !wave.wait_ownership_uncertain) {
        row[58] = 1;
        auto rc = process(proc::procmgmt_ops::KILL, static_cast<uint64_t>(wave.pid), SIGKILL);
        c.expect(rc == 0, Stage::KILL);
        auto start = now_us(c);
        if (start != UINT64_MAX && start <= UINT64_MAX - STEP_US) {
            auto deadline = start + STEP_US;
            if (c.failure_deadline < deadline) deadline = c.failure_deadline;
            (void)wait_owned(c, wave, row, deadline);
        }
    }
    if (!completed) {
        // Save only independently accepted buffers, once, after handles and
        // known-child cleanup. Never replace a failed/partial O_EXCL receipt.
        auto start = now_us(c);
        if (start != UINT64_MAX && c.failure_deadline != 0) {
            auto deadline = c.failure_deadline;
            if (wave.input_committed && !wave.receipt_attempted[0]) {
                wave.receipt_attempted[0] = true;
                (void)persist(c, mode == 1 ? "static-input.bin" : "tiny-input.bin", wave.input.data(), sizeof(wave.input), deadline);
            }
            if (wave.ready_accepted && !wave.receipt_attempted[1]) {
                wave.receipt_attempted[1] = true;
                (void)persist(c, mode == 1 ? "static-ready.bin" : "tiny-ready.bin", wave.ready.data(), sizeof(wave.ready), deadline);
            }
            if (wave.heartbeat_accepted && !wave.receipt_attempted[2]) {
                wave.receipt_attempted[2] = true;
                (void)persist(c, mode == 1 ? "static-heartbeat.bin" : "tiny-heartbeat.bin", wave.heartbeat.data(), sizeof(wave.heartbeat),
                              deadline);
            }
        }
    }
    bool const passed = completed && c.failures == failures_before;
    if (!passed) row[57] = static_cast<uint64_t>(c.first);
    return passed;
}
struct Line {
    char data[512]{};
    size_t used{};
    bool valid{true};
    void text(const char* s) {
        for (size_t i = 0; s[i]; ++i) {
            if (used + 1 >= sizeof(data)) {
                valid = false;
                return;
            }
            data[used++] = s[i];
        }
    }
    void number(uint64_t value) {
        char reverse[20]{};
        size_t count = 0;
        do {
            reverse[count++] = static_cast<char>('0' + value % 10);
            value /= 10;
        } while (value);
        while (count) {
            char one[2]{reverse[--count], 0};
            text(one);
        }
    }
    void hex(uint64_t value) {
        constexpr char alphabet[] = "0123456789abcdef";
        for (unsigned i = 0; i < 16; ++i) {
            char one[2]{alphabet[(value >> (60 - i * 4)) & 15], 0};
            text(one);
        }
    }
};
auto parse_hex(const char* value, uint64_t& out) -> bool {
    out = 0;
    size_t digits = 0;
    while (value[digits]) {
        if (digits == 16) return false;
        char ch = value[digits++];
        unsigned digit;
        if (ch >= '0' && ch <= '9')
            digit = static_cast<unsigned>(ch - '0');
        else if (ch >= 'a' && ch <= 'f')
            digit = static_cast<unsigned>(ch - 'a' + 10);
        else if (ch >= 'A' && ch <= 'F')
            digit = static_cast<unsigned>(ch - 'A' + 10);
        else
            return false;
        out = (out << 4) | digit;
    }
    return digits > 0 && out != 0;
}
auto pilot(uint64_t nonce, uint64_t static_cookie, uint64_t tiny_cookie) -> int {
    Context c{};
    auto* g = c.report.data();
    g[0] = HOST_MAGIC;
    g[1] = 1;
    g[2] = sizeof(c.report);
    g[4] = nonce;
    g[5] = static_cast<uint64_t>(process(proc::procmgmt_ops::GETPID));
    g[6] = static_cast<uint64_t>(process(proc::procmgmt_ops::GETPPID));
    g[7] = now_us(c);
    bool passed =
        c.expect(g[5] > 2 && g[5] <= INT64_MAX && g[6] == 1 && g[7] != UINT64_MAX && g[7] <= UINT64_MAX - RUN_US, Stage::IDENTITY);
    c.deadline = passed ? g[7] + RUN_US : 0;
    g[8] = c.deadline;
    uint64_t flags{};
    passed = identity(c, c.identity, flags, c.limits) && passed;
    g[104] = flags;
    for (size_t i = 0; i < 8; ++i) g[16 + i] = c.identity[i];
    for (size_t i = 0; i < 16; ++i) {
        g[40 + 2 * i] = c.limits[i].soft;
        g[41 + 2 * i] = c.limits[i].hard;
    }
    passed = scan(c, c.baseline, Stage::BASELINE) && passed;
    bool baseline = c.baseline.fd[1] >= 0 && c.baseline.fd[2] >= 0;
    for (size_t i = 3; i < 256; ++i) baseline = baseline && c.baseline.fd[i] == BAD_FD;
    passed = c.expect(baseline, Stage::BASELINE) && passed;
    g[14] = c.baseline.count;
    for (size_t i = 0; i < 4; ++i) g[32 + i] = c.baseline.present[i];
    if (passed && execute_wave(c, 1, static_cookie)) {
        ++g[10];
        if (execute_wave(c, 2, tiny_cookie)) ++g[10];
    }
    Words<8> final_identity{};
    Limits final_limits{};
    uint64_t final_flags{};
    bool final = identity(c, final_identity, final_flags, final_limits);
    FdSnapshot final_fds{};
    final = scan(c, final_fds, Stage::FINAL) && final;
    bool exact_flags = final_fds.fd == c.baseline.fd && final_fds.fl == c.baseline.fl;
    final = c.expect(exact_flags && final_identity == c.identity && final_flags == flags && limits_equal(final_limits, c.limits),
                     Stage::FINAL) &&
            final;
    for (size_t i = 0; i < 8; ++i) g[24 + i] = final_identity[i];
    for (size_t i = 0; i < 4; ++i) g[36 + i] = final_fds.present[i];
    for (size_t i = 0; i < 16; ++i) {
        g[72 + 2 * i] = final_limits[i].soft;
        g[73 + 2 * i] = final_limits[i].hard;
    }
    g[15] = final_fds.count;
    g[105] = final_flags;
    g[106] = exact_flags ? 1 : 0;
    g[9] = now_us(c);
    bool const end_valid = c.expect(g[9] != UINT64_MAX && g[9] < c.deadline, Stage::FINAL);
    passed = passed && final && end_valid && g[10] == 2 && c.failures == 0;
    g[3] = passed ? 1 : 2;
    g[11] = c.checks;
    g[12] = c.failures;
    g[13] = c.failures ? static_cast<uint64_t>(c.first) : 0;
    uint64_t receipt_deadline = c.deadline;
    if (!passed) receipt_deadline = c.failure_deadline;
    bool saved = persist(c, "host.bin", c.report.data(), sizeof(c.report), receipt_deadline);
    Line line{};
    line.text(passed && saved ? "PASS" : "FAIL");
    line.text(" lifecycle-raw-pilot nonce=");
    line.hex(nonce);
    line.text(" pid=");
    line.number(g[5]);
    line.text(" waves=");
    line.number(g[10]);
    line.text(" static-pid=");
    line.number(g[131]);
    line.text(" tiny-pid=");
    line.number(g[195]);
    line.text(" checks=");
    line.number(c.checks);
    line.text(" failures=");
    line.number(c.failures + (saved ? 0 : 1));
    line.text("\n");
    bool status_saved = line.valid && persist(c, "status", line.data, line.used, receipt_deadline);
    if (status_saved)
        (void)vfs(ker::abi::vfs::ops::WRITE, 1, address(line.data), line.used);
    else {
        constexpr char failure[] = "FAIL lifecycle-raw-pilot receipt-write failures=1\n";
        (void)vfs(ker::abi::vfs::ops::WRITE, 1, address(failure), sizeof(failure) - 1);
    }
    return passed && saved && status_saved && c.failures == 0 ? 0 : 75;
}
auto snapshot_valid(const init::StatusSnapshot& s) -> bool {
    if (s.size != sizeof(s) || s.version != 1 || s.sequence == 0 || s.published_mono_ns == 0 || s.service_count == 0 ||
        s.service_count > init::MAX_STATUS_SERVICES || s.flags != 0 || s.reserved0 != 0 ||
        (s.state != init::SupervisorState::STARTING && s.state != init::SupervisorState::RUNNING))
        return false;
    for (auto reserved : s.reserved)
        if (reserved != 0) return false;
    for (uint32_t i = 0; i < s.service_count; ++i) {
        auto const& row = s.services[i];
        if (row.service[0] == 0 || std::memchr(row.service, '\0', sizeof(row.service)) == nullptr ||
            row.history_count > init::MAX_TRANSITION_HISTORY || row.reserved0 != 0 || row.reserved1 != 0 || (row.flags & ~7U) != 0 ||
            static_cast<uint32_t>(row.state) > 8 || static_cast<uint32_t>(row.readiness) > 3)
            return false;
        for (auto reserved : row.reserved)
            if (reserved != 0) return false;
        for (uint32_t j = 0; j < row.history_count; ++j) {
            auto const& h = row.history[j];
            if (h.reserved0 != 0 || h.timestamp_mono_ns == 0 || h.timestamp_mono_ns > s.published_mono_ns ||
                static_cast<uint32_t>(h.from_state) > 8 || static_cast<uint32_t>(h.to_state) > 8 || static_cast<uint32_t>(h.reason) > 9)
                return false;
        }
    }
    return true;
}
auto witness() -> int {
    Context c{};
    auto start = now_us(c);
    bool good = c.expect(start != UINT64_MAX && start <= UINT64_MAX - RUN_US, Stage::WITNESS);
    c.deadline = good ? start + RUN_US : 0;
    for (auto op : {proc::procmgmt_ops::GETUID, proc::procmgmt_ops::GETEUID, proc::procmgmt_ops::GETGID, proc::procmgmt_ops::GETEGID})
        good = c.expect(process(op) == 100, Stage::WITNESS) && good;
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    bool observed = false, history = false;
    while (good && !observed && before(c, c.deadline)) {
        struct Guarded {
            uint64_t before{GUARD};
            init::StatusSnapshot value{};
            uint64_t after{GUARD};
        } guarded;
        static_assert(sizeof(init::StatusSnapshot) == 4160 && offsetof(Guarded, value) == 8 && offsetof(Guarded, after) == 4168);
        auto rc = process(proc::procmgmt_ops::INIT_STATUS_READ, address(&guarded.value));
        good = c.expect((rc == 0 || rc == AGAIN) && guarded.before == GUARD && guarded.after == GUARD && before(c, c.deadline),
                        Stage::WITNESS);
        if (!good) break;
        if (rc == 0) {
            if (!c.expect(snapshot_valid(guarded.value), Stage::WITNESS)) {
                good = false;
                break;
            }
            snapshot = guarded.value;
            unsigned matches = 0;
            for (uint32_t i = 0; i < snapshot.service_count; ++i)
                if (std::strcmp(snapshot.services[i].service, "lifecycle-raw-pilot") == 0) {
                    selected = snapshot.services[i];
                    ++matches;
                }
            if (!c.expect(matches <= 1, Stage::WITNESS)) {
                good = false;
                break;
            }
            history = false;
            if (matches == 1)
                for (uint32_t i = 0; i < selected.history_count; ++i)
                    history = history || (selected.history[i].reason == init::TransitionReason::EXIT &&
                                          selected.history[i].to_state == init::ServiceState::EXITED);
            observed = matches == 1 && selected.generation == 1 && selected.restart_count == 0 &&
                       selected.state == init::ServiceState::EXITED && selected.readiness == init::ReadinessState::READY &&
                       selected.pid == -1 && selected.pgid == -1 && selected.last_wait_status == 0 && selected.last_error == 0 && history;
        }
        if (!observed && !pause(c, c.deadline)) break;
    }
    good = c.expect(observed && good && c.failures == 0, Stage::WITNESS);
    auto end = now_us(c);
    uint64_t receipt_deadline =
        good && c.failures == 0 && end != UINT64_MAX && end <= UINT64_MAX - STEP_US ? end + STEP_US : c.failure_deadline;
    bool saved = persist(c, "init-exit-snapshot.bin", &snapshot, sizeof(snapshot), receipt_deadline);
    Line line{};
    line.text(good && saved && c.failures == 0 ? "PASS" : "FAIL");
    line.text(" lifecycle-raw-pilot-init-exit service=lifecycle-raw-pilot generation=");
    line.number(selected.generation);
    line.text(" restarts=");
    line.number(selected.restart_count);
    line.text(" sequence=");
    line.number(snapshot.sequence);
    line.text(" published-ns=");
    line.number(snapshot.published_mono_ns);
    line.text(" state=");
    line.number(static_cast<uint32_t>(selected.state));
    line.text(" readiness=");
    line.number(static_cast<uint32_t>(selected.readiness));
    line.text(" wait=");
    if (selected.last_wait_status < 0) line.text("-");
    line.number(selected.last_wait_status < 0 ? static_cast<uint64_t>(-static_cast<int64_t>(selected.last_wait_status))
                                              : static_cast<uint64_t>(selected.last_wait_status));
    line.text(" error=");
    if (selected.last_error < 0) line.text("-");
    line.number(selected.last_error < 0 ? static_cast<uint64_t>(-static_cast<int64_t>(selected.last_error))
                                        : static_cast<uint64_t>(selected.last_error));
    line.text(" history-exit=");
    line.number(history ? 1 : 0);
    line.text(" snapshot-bytes=");
    line.number(snapshot.size);
    line.text(" abi=");
    line.number(snapshot.version);
    line.text(" uid=100 failures=");
    line.number(c.failures + (saved ? 0 : 1));
    line.text("\n");
    bool status_saved = line.valid && persist(c, "init-exit-status", line.data, line.used, receipt_deadline);
    if (status_saved)
        (void)vfs(ker::abi::vfs::ops::WRITE, 1, address(line.data), line.used);
    else {
        constexpr char failure[] = "FAIL lifecycle-raw-pilot-init-exit receipt-write failures=1\n";
        (void)vfs(ker::abi::vfs::ops::WRITE, 1, address(failure), sizeof(failure) - 1);
    }
    return good && saved && status_saved && c.failures == 0 ? 0 : 76;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return witness();
    uint64_t nonce{}, static_cookie{}, tiny_cookie{};
    if (argc != 5 || std::strcmp(argv[1], "--pilot") != 0 || !parse_hex(argv[2], nonce) || !parse_hex(argv[3], static_cookie) ||
        !parse_hex(argv[4], tiny_cookie) || static_cookie >= (uint64_t{1} << 60) || tiny_cookie >= (uint64_t{1} << 60) ||
        static_cookie == tiny_cookie)
        return 77;
    return pilot(nonce, static_cookie, tiny_cookie);
}
