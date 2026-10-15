// Staged real public CREATE/kill/consuming-WAIT workload. No user fault API.
#include <abi/callnums/process.h>
#include <abi/callnums/time.h>
#include <abi/callnums/vfs.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <abi/compartment.hpp>
#include <abi/resource.hpp>
#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <test/lifecycle_wire60.hpp>

namespace {
namespace cw = criterion9::wire;
namespace wire = ker::abi::compartment;
namespace proc = ker::abi::process;
constexpr uint64_t INPUT_MAGIC = 0x4131524157494e50, READY_MAGIC = 0x4131524157525054;
constexpr uint64_t HB_MAGIC = 0x41314c4956454842, TINY_MARKER = 0x54494e593431484f;
constexpr uint64_t USER_END = 0x0000800000000000, CHILD_US = 60'000'000;
constexpr int64_t BAD_FD = -9;
template <size_t N>
using Words = std::array<uint64_t, N>;
using Input = cw::Input;
using Ready = cw::Ready;
using Heartbeat = cw::Heartbeat;
using Limits = std::array<ker::abi::resource::Limit, 16>;
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
    int64_t seconds, nanoseconds;
};
struct FdSnapshot {
    std::array<int64_t, 256> fd{}, fl{};
    Words<4> present{};
    uint64_t count{};
};
struct Context {
    cw::Manifest manifest{};
    FdSnapshot baseline{};
    Limits limits{};
    Words<8> identity{};
    std::array<int, 9> fd{-1, -1, -1, -1, -1, -1, -1, -1, -1};
    std::array<struct stat, 3> assets{};
    struct stat raw_stat{};
    uint64_t last_clock{}, deadline{}, checks{}, failures{}, failure_deadline{}, baseline_hash{};
    uint64_t pid{}, parent{}, target_flags{}, completed{}, warmups{}, faults{}, successes{}, start{};
    Stage first{Stage::CLOCK};
    bool clock_failed{};
    auto expect(bool good, Stage stage) -> bool {
        if (checks == UINT64_MAX) return false;
        ++checks;
        if (!good) {
            if (failures == 0) first = stage;
            if (failures != UINT64_MAX) ++failures;
        }
        return good;
    }
};
enum Fd : size_t { STATIC, TINY, INTERP, RAW, CONTROL, ATTEMPTS, EVIDENCE, FD_LEDGER, STATUS };
static_assert(sizeof(wire::Spec) == 408 && sizeof(struct stat) == 144 && sizeof(Timespec) == 16);
static_assert(std::endian::native == std::endian::little);
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

auto user_pointer(uint64_t value, uint64_t bytes = 1) -> bool { return value >= 4096 && value < USER_END && bytes <= USER_END - value; }

template <size_t N>
auto equal_reads(Context& c, int fd, Words<N>& out, uint64_t offset, Stage stage, bool& same) -> bool {
    Words<N> first{}, second{};
    bool valid = c.expect(pread_raw(fd, first.data(), sizeof(first), offset) == static_cast<int64_t>(sizeof(first)), stage);
    valid = c.expect(pread_raw(fd, second.data(), sizeof(second), offset) == static_cast<int64_t>(sizeof(second)), stage) && valid;
    same = valid && first == second;
    if (same) out = first;
    return valid;
}

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
    good = good && user_pointer(r[96], 1024) && (r[96] & 15) == 0 && r[97] == c.manifest.entries[h[5]].entry &&
           r[98] == c.manifest.entries[h[5]].entry && r[105] == 1 && r[106] == 0 && r[107] == 0x1040 && r[108] == 56 &&
           r[109] == (tiny ? c.manifest.entries[2].phnum : c.manifest.entries[1].phnum) && r[110] == 4096 && r[111] == 0;
    // Incoming RDI/RSI/RDX and static R12 are opaque: direct sysret and iret differ.
    if (tiny)
        good = good && r[99] >= 0x30000000 && r[99] < 0x70000000 && (r[99] & 0x1fffff) == 0 &&
               r[100] == r[99] + c.manifest.entries[3].entry && r[101] == TINY_MARKER;
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
           r[236] == (tiny ? 13U : 12U) && r[237] == (tiny ? c.manifest.entries[3].entry : 0) && r[238] == 0 && r[239] == 0 &&
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

// Wait for genuine advancing work, rather than rejecting an unchanged complete
// sample that raced the parent's previous read. Wrong committed identity fails.
auto poll_next(Context& c, const Input& input, const Ready& ready, Heartbeat& out, uint64_t minimum, uint64_t deadline, uint64_t& observed)
    -> bool {
    while (before(c, deadline)) {
        Heartbeat value{};
        bool same{};
        if (!equal_reads(c, c.fd[RAW], value, 1024, Stage::HEARTBEAT, same)) return false;
        auto now = now_us(c);
        if (now == UINT64_MAX || now >= deadline) break;
        if (same && value[31] != 0 && value[31] == value[5]) {
            if (!c.expect(value[0] == HB_MAGIC && value[2] == input[3] && value[3] == input[4] && value[4] == input[5], Stage::HEARTBEAT))
                return false;
            if (value[5] >= minimum) {
                if (!heartbeat_valid(c, input, ready, value, minimum, now)) return false;
                out = value;
                observed = now;
                return true;
            }
        }
        if (!pause(c, deadline)) break;
    }
    return c.expect(false, Stage::HEARTBEAT);
}
auto flush(Context& c, int fd) -> bool { return c.expect(vfs(ker::abi::vfs::ops::FSYNC, static_cast<uint64_t>(fd)) == 0, Stage::PERSIST); }
auto exact_write(Context& c, int fd, const void* p, size_t bytes, uint64_t offset) -> bool {
    return c.expect(pwrite_raw(fd, p, bytes, offset) == static_cast<int64_t>(bytes), Stage::PERSIST);
}
auto publish(Context& c, int fd, cw::Record row, uint64_t offset) -> bool {
    cw::seal(row);
    auto const commit = row[cw::COMMIT];
    row[cw::COMMIT] = 0;
    if (!exact_write(c, fd, row.data(), sizeof(row), offset) || !flush(c, fd) ||
        !exact_write(c, fd, &commit, sizeof(commit), offset + 504) || !flush(c, fd))
        return false;
    row[cw::COMMIT] = commit;
    cw::Record actual{};
    return c.expect(pread_raw(fd, actual.data(), sizeof(actual), offset) == 512 && actual == row, Stage::PERSIST);
}
auto zero_file(Context& c, int fd, uint64_t bytes) -> bool {
    if (!c.expect(vfs(ker::abi::vfs::ops::TRUNCATE, static_cast<uint64_t>(fd), bytes) == 0, Stage::SOURCE)) return false;
    Words<512> zero{};
    for (uint64_t offset = 0; offset < bytes;) {
        if (!before(c, c.deadline)) return false;
        auto count = static_cast<size_t>(std::min<uint64_t>(sizeof(zero), bytes - offset));
        if (!exact_write(c, fd, zero.data(), count, offset)) return false;
        offset += count;
    }
    return flush(c, fd);
}
auto baseline_equal(Context& c) -> bool {
    FdSnapshot current{};
    return scan(c, current, Stage::BASELINE) && c.expect(current.fd == c.baseline.fd && current.fl == c.baseline.fl, Stage::BASELINE);
}
auto load_manifest(Context& c) -> bool {
    int fd = static_cast<int>(vfs(ker::abi::vfs::ops::OPEN, address(cw::MANIFEST_PATH), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NO_CACHE, 0));
    if (!c.expect(fd >= 0 && fd < 256, Stage::ASSET)) return false;
    struct stat st{};
    cw::Manifest first{}, second{};
    bool good = stat_raw(fd, st) == 0 && st.st_size == 1024 && st.st_mode == (S_IFREG | 0444) && st.st_uid == 0 && st.st_gid == 0 &&
                st.st_nlink == 1;
    good = pread_raw(fd, &first, sizeof(first), 0) == 1024 && pread_raw(fd, &second, sizeof(second), 0) == 1024 && good;
    auto const* a = reinterpret_cast<const uint8_t*>(&first);
    auto const* b = reinterpret_cast<const uint8_t*>(&second);
    for (size_t i = 0; i < sizeof(first); ++i) good = (a[i] == b[i]) && good;
    // Closing manifest precedes every persistent ledger File and baseline.
    good = vfs(ker::abi::vfs::ops::CLOSE, static_cast<uint64_t>(fd)) == 0 && good;
    good = first.magic == cw::MANIFEST_MAGIC && first.version == 1 && first.bytes == 1024 && first.record_count == 4 && first.nonce != 0 &&
           first.warmups == 8 && first.faults == 24 && first.successes == 1000 && good;
    for (auto value : first.reserved) good = (value == 0) && good;
    for (size_t i = 0; i < first.entries.size(); ++i) {
        auto const& e = first.entries[i];
        bool nonzero = false;
        for (auto value : e.sha256) nonzero |= value != 0;
        good = e.role == i + 1 && e.bytes > 0 && e.bytes <= (i == 0 ? 4U * 1024U * 1024U : 65536U) && e.entry > 0 && e.phnum > 0 &&
               e.phnum <= 64 && nonzero && good;
        good = e.elf_type == (i == 1 || i == 2 ? 2U : 3U) && good;
        for (auto value : e.reserved) good = (value == 0) && good;
    }
    c.manifest = first;
    return c.expect(good, Stage::ASSET);
}
auto initialize(Context& c) -> bool {
    c.start = now_us(c);
    if (!c.expect(c.start != UINT64_MAX && c.start <= UINT64_MAX - cw::CAMPAIGN_US, Stage::CLOCK)) return false;
    c.deadline = c.start + cw::CAMPAIGN_US;
    auto pid = process(proc::procmgmt_ops::GETPID), parent = process(proc::procmgmt_ops::GETPPID);
    if (!c.expect(pid > 2 && parent == 1, Stage::IDENTITY)) return false;
    c.pid = static_cast<uint64_t>(pid);
    c.parent = static_cast<uint64_t>(parent);
    // No Host console File description participates in the private ledger.
    // Do this once before any open, so an EBADF never closes a reused slot.
    for (int fd = 0; fd < 3; ++fd) {
        auto const rc = vfs(ker::abi::vfs::ops::CLOSE, static_cast<uint64_t>(fd));
        if (!c.expect(rc == 0 || rc == BAD_FD, Stage::BASELINE)) return false;
    }
    if (!identity(c, c.identity, c.target_flags, c.limits) || !load_manifest(c)) return false;
    constexpr std::array<const char*, 3> asset_names{"lifecycle-raw-static", "lifecycle-raw-tiny", "lifecycle-tiny-ld.so"};
    for (size_t i = 0; i < 3; ++i) {
        c.fd[i] = open_raw(c, cw::ASSETS, asset_names[i], O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NO_CACHE, 0, Stage::ASSET);
        if (c.fd[i] < 0) return false;
        auto& st = c.assets[i];
        auto const& e = c.manifest.entries[i + 1];
        if (!c.expect(stat_raw(c.fd[i], st) == 0 && st.st_size == static_cast<int64_t>(e.bytes) && st.st_mode == (S_IFREG | 0555) &&
                          st.st_uid == 0 && st.st_gid == 0 && st.st_nlink == 1,
                      Stage::ASSET))
            return false;
    }
    constexpr std::array<const char*, 6> result_names{"child.raw",    "control.bin",   "attempts.bin",
                                                      "evidence.bin", "fd-ledger.bin", "driver-status"};
    constexpr std::array<uint64_t, 6> sizes{cw::CHILD_BYTES,        cw::CONTROL_BYTES, cw::LEDGER_BYTES,
                                            cw::ALL_EVIDENCE_BYTES, cw::FD_BYTES,      cw::STATUS_BYTES};
    for (size_t i = 0; i < 6; ++i) {
        auto slot = i + 3;
        c.fd[slot] =
            open_raw(c, cw::RESULTS, result_names[i], O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NO_CACHE, 0600, Stage::SOURCE);
        if (c.fd[slot] < 0 || !zero_file(c, c.fd[slot], sizes[i])) return false;
        struct stat st{};
        if (!c.expect(stat_raw(c.fd[slot], st) == 0 && st.st_size == static_cast<int64_t>(sizes[i]) && st.st_mode == (S_IFREG | 0600) &&
                          st.st_uid == 100 && st.st_gid == 100 && st.st_nlink == 1,
                      Stage::SOURCE))
            return false;
        if (slot == RAW) c.raw_stat = st;
    }
    if (!scan(c, c.baseline, Stage::BASELINE)) return false;
    if (!c.expect(c.baseline.count == c.fd.size(), Stage::BASELINE)) return false;
    for (size_t fd = 0; fd < c.baseline.fd.size(); ++fd) {
        size_t owners = 0;
        for (auto owned : c.fd)
            if (owned == static_cast<int>(fd)) ++owners;
        bool const installed = c.baseline.fd[fd] >= 0;
        if (!c.expect(owners == (installed ? 1U : 0U), Stage::BASELINE)) return false;
    }
    cw::FdLedger ledger{};
    cw::Record descriptor_header{};
    descriptor_header[0] = cw::FD_MAGIC;
    descriptor_header[1] = 1;
    descriptor_header[2] = cw::FD_BYTES;
    descriptor_header[3] = c.manifest.nonce;
    descriptor_header[4] = c.pid;
    descriptor_header[5] = c.baseline.count;
    descriptor_header[6] = 256;
    descriptor_header[7] = 2;
    cw::seal(descriptor_header);
    std::copy(descriptor_header.begin(), descriptor_header.end(), ledger.begin());
    for (size_t i = 0; i < 256; ++i) {
        ledger[64 + i] = static_cast<uint64_t>(c.baseline.fd[i]);
        ledger[320 + i] = static_cast<uint64_t>(c.baseline.fl[i]);
    }
    c.baseline_hash = cw::hash_words(ledger);
    if (!exact_write(c, c.fd[FD_LEDGER], ledger.data(), sizeof(ledger), 0) || !flush(c, c.fd[FD_LEDGER])) return false;
    cw::Record header{};
    header[cw::H_MAGIC] = cw::HEADER_MAGIC;
    header[cw::H_VERSION] = 1;
    header[cw::H_BYTES] = 512;
    header[cw::H_NONCE] = c.manifest.nonce;
    header[cw::H_DRIVER_PID] = c.pid;
    header[cw::H_PARENT_PID] = c.parent;
    header[cw::H_ATTEMPTS] = 1032;
    header[cw::H_WARMUPS] = 8;
    header[cw::H_FAULTS] = 24;
    header[cw::H_SUCCESSES] = 1000;
    header[cw::H_RECORD_BYTES] = 512;
    header[cw::H_EVIDENCE_BYTES] = 4096;
    header[cw::H_CHILD_BYTES] = 8192;
    header[cw::H_CONTROL_BYTES] = 4096;
    header[cw::H_FD_BYTES] = 8192;
    header[cw::H_STATUS_BYTES] = 512;
    header[cw::H_START_US] = c.start;
    header[cw::H_DEADLINE_US] = c.deadline;
    for (size_t i = 0; i < c.fd.size(); ++i) header[cw::H_STATIC_FD + i] = static_cast<uint64_t>(c.fd[i]);
    return publish(c, c.fd[CONTROL], header, 0) && baseline_equal(c);
}
auto make_input(Context& c, uint64_t index, const wire::Spec& spec, Input& h, cw::Record& row) -> bool {
    auto start = now_us(c);
    if (!c.expect(start != UINT64_MAX && start <= UINT64_MAX - CHILD_US && start + CHILD_US < c.deadline, Stage::INPUT)) return false;
    auto const mode = cw::variant(index);
    h = {};
    h[0] = INPUT_MAGIC;
    h[1] = 1;
    h[2] = 512;
    h[3] = c.manifest.nonce;
    h[4] = index + 1;
    h[5] = mode;
    h[6] = c.pid;
    h[7] = start;
    h[8] = start + CHILD_US;
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
    h[56] = c.raw_stat.st_ino;
    h[57] = c.raw_stat.st_dev;
    auto flags = fcntl_raw(c.fd[RAW], F_GETFL);
    if (!c.expect(flags >= 0 && (flags & O_ACCMODE) == O_RDWR, Stage::INPUT)) return false;
    h[58] = static_cast<uint64_t>(flags);
    h[59] = 1024;
    h[60] = 256;
    h[61] = 16;
    h[62] = 10000;
    if (!zero_file(c, c.fd[RAW], 8192) || !exact_write(c, c.fd[RAW], h.data(), sizeof(h), 0)) return false;
    h[63] = h[3];
    if (!exact_write(c, c.fd[RAW], &h[63], 8, 504) || !flush(c, c.fd[RAW])) return false;
    Input reread{};
    if (!c.expect(pread_raw(c.fd[RAW], reread.data(), 512, 0) == 512 && reread == h, Stage::INPUT)) return false;
    row = {};
    row[cw::MAGIC] = cw::ATTEMPT_MAGIC;
    row[cw::VER] = 1;
    row[cw::BYTES] = 512;
    row[cw::NONCE] = h[3];
    row[cw::INDEX] = index;
    row[cw::ATTEMPT_ID] = index + 1;
    row[cw::KIND] = static_cast<uint64_t>(cw::kind(index));
    row[cw::VARIANT] = mode;
    row[cw::STATE] = static_cast<uint64_t>(cw::State::REQUEST);
    row[cw::START_US] = start;
    row[cw::DEADLINE_US] = h[8];
    row[cw::CREATE_RAW] = static_cast<uint64_t>(cw::UNOBSERVED);
    row[cw::KILL_RAW] = static_cast<uint64_t>(cw::UNOBSERVED);
    row[cw::WAIT_RAW] = static_cast<uint64_t>(cw::UNOBSERVED);
    row[cw::INPUT_HASH] = cw::hash_words(h);
    row[cw::RAW_DEV] = c.raw_stat.st_dev;
    row[cw::RAW_INO] = c.raw_stat.st_ino;
    row[cw::RAW_FD] = static_cast<uint64_t>(c.fd[RAW]);
    row[cw::FD_LEDGER_HASH] = c.baseline_hash;
    row[cw::MAIN_DEV] = c.assets[mode - 1].st_dev;
    row[cw::MAIN_INO] = c.assets[mode - 1].st_ino;
    row[cw::MAIN_BYTES] = static_cast<uint64_t>(c.assets[mode - 1].st_size);
    if (mode == 2) {
        row[cw::INTERP_DEV] = c.assets[2].st_dev;
        row[cw::INTERP_INO] = c.assets[2].st_ino;
        row[cw::INTERP_BYTES] = static_cast<uint64_t>(c.assets[2].st_size);
    }
    row[cw::PARENT_PID] = c.pid;
    return true;
}
auto save_evidence(Context& c, uint64_t index, const Input& input, const Ready& ready, const Heartbeat& a, const Heartbeat& b) -> bool {
    cw::Evidence value{};
    std::copy(input.begin(), input.end(), value.begin());
    std::copy(ready.begin(), ready.end(), value.begin() + cw::READY_OFFSET / 8);
    std::copy(a.begin(), a.end(), value.begin() + cw::HEARTBEAT_A_OFFSET / 8);
    std::copy(b.begin(), b.end(), value.begin() + cw::HEARTBEAT_B_OFFSET / 8);
    return exact_write(c, c.fd[EVIDENCE], value.data(), sizeof(value), index * cw::EVIDENCE_BYTES) && flush(c, c.fd[EVIDENCE]);
}
auto consume(Context& c, int64_t pid, cw::Record& row, bool& reaped, bool& uncertain) -> bool {
    while (before(c, row[cw::DEADLINE_US])) {
        int32_t status{};
        auto result = process(proc::procmgmt_ops::WAITPID, static_cast<uint64_t>(pid), address(&status), WNOHANG, 0);
        if (result == pid) {
            reaped = true;  // First operation after an actual consuming result.
            row[cw::WAIT_RAW] = static_cast<uint64_t>(result);
            row[cw::WAIT_STATUS] = static_cast<uint64_t>(static_cast<int64_t>(status));
            row[cw::WAIT_DONE_US] = now_us(c);
            return c.expect(status == 9 && row[cw::WAIT_DONE_US] != UINT64_MAX && row[cw::WAIT_DONE_US] < row[cw::DEADLINE_US],
                            Stage::WAIT);
        }
        if (result == 0)
            ++row[cw::WAIT_ZERO];
        else if (result == -4)
            ++row[cw::WAIT_EINTR];
        else {
            uncertain = true;
            return c.expect(false, Stage::WAIT);
        }
        if (!pause(c, row[cw::DEADLINE_US])) break;
    }
    return c.expect(false, Stage::WAIT);
}
auto attempt(Context& c, uint64_t index) -> bool {
    wire::Spec spec{};
    spec.root_bytes = 8U * 1024U * 1024U;
    if (!policy(c, spec)) return false;
    auto mode = cw::variant(index);
    wire::String executable{address(cw::EXECUTABLE), sizeof(cw::EXECUTABLE) - 1};
    std::array<wire::Asset, 2> assets{
        {{c.fd[mode - 1], 0755, executable, 0}, {c.fd[INTERP], 0755, {address(cw::INTERPRETER), sizeof(cw::INTERPRETER) - 1}, 0}}};
    wire::Descriptor grant{c.fd[RAW], 9, 0};
    spec.executable = executable;
    spec.assets = address(assets.data());
    spec.asset_count = mode == 2 ? 2 : 1;
    spec.descriptors = address(&grant);
    spec.descriptor_count = 1;
    spec.argv = address(&executable);
    spec.argc = 1;
    Input input{};
    Ready ready{};
    Heartbeat a{}, b{};
    cw::Record row{};
    if (!baseline_equal(c) || !make_input(c, index, spec, input, row) || !save_evidence(c, index, input, ready, a, b) ||
        !publish(c, c.fd[ATTEMPTS], row, index * 512) || !publish(c, c.fd[CONTROL], row, cw::REQUEST_OFFSET))
        return false;
    int64_t pid = 0;
    bool reaped = false, uncertain = false;
    auto execute = [&]() -> bool {
        auto created = process(proc::procmgmt_ops::COMPARTMENT_CREATE, address(&spec));
        if (created > 2) pid = created;  // Before any next IO/clock: exact owned PID.
        row[cw::CREATE_RAW] = static_cast<uint64_t>(created);
        row[cw::PID] = pid > 2 ? static_cast<uint64_t>(pid) : 0;
        row[cw::CREATE_RETURN_US] = now_us(c);
        if (!c.expect(row[cw::CREATE_RETURN_US] != UINT64_MAX && row[cw::CREATE_RETURN_US] < row[cw::DEADLINE_US], Stage::CREATE))
            return false;
        if (cw::kind(index) == cw::Kind::FAULT) return c.expect(created == cw::expected_error(index), Stage::CREATE);
        if (!c.expect(pid > 2, Stage::CREATE)) return false;
        uint64_t poll_deadline = std::min(row[cw::DEADLINE_US] - 1'000'000, now_us(c) + 5'000'000);
        if (!poll_ready(c, c.fd[RAW], input, ready, poll_deadline, row[cw::READY_OBS_US]) ||
            !poll_next(c, input, ready, a, 17, poll_deadline, row[cw::HB_A_US]) ||
            !poll_next(c, input, ready, b, a[5] + 1, poll_deadline, row[cw::HB_B_US]))
            return false;
        row[cw::HB_A_ROUND] = a[5];
        row[cw::HB_B_ROUND] = b[5];
        row[cw::READY_HASH] = cw::hash_words(ready);
        row[cw::HB_A_HASH] = cw::hash_words(a);
        row[cw::HB_B_HASH] = cw::hash_words(b);
        if (!save_evidence(c, index, input, ready, a, b) || !baseline_equal(c)) return false;
        row[cw::KILL_START_US] = now_us(c);
        auto killed = process(proc::procmgmt_ops::KILL, static_cast<uint64_t>(pid), SIGKILL);
        row[cw::KILL_RAW] = static_cast<uint64_t>(killed);
        if (!c.expect(killed == 0 && row[cw::KILL_START_US] < row[cw::DEADLINE_US], Stage::KILL)) return false;
        row[cw::STATE] = static_cast<uint64_t>(cw::State::BEFORE_WAIT);
        row[cw::CHECKS] = c.checks;
        // Actual WAIT hook observes this immutable prefix, then parks before return.
        if (!publish(c, c.fd[ATTEMPTS], row, index * 512) || !publish(c, c.fd[CONTROL], row, cw::REQUEST_OFFSET)) return false;
        return consume(c, pid, row, reaped, uncertain);
    };
    bool good = execute();
    if (!good && pid > 2 && !reaped && !uncertain) {
        // No PID-only signal after an unexpected consuming/error result.
        (void)process(proc::procmgmt_ops::KILL, static_cast<uint64_t>(pid), SIGKILL);
        (void)consume(c, pid, row, reaped, uncertain);
    }
    row[cw::STATE] = static_cast<uint64_t>(cw::State::COMPLETED);
    row[cw::END_US] = now_us(c);
    row[cw::CHECKS] = c.checks;
    row[cw::FAILURE_STAGE] = good ? 0 : static_cast<uint64_t>(c.first);
    good = baseline_equal(c) && good;
    bool saved = publish(c, c.fd[ATTEMPTS], row, index * 512) && publish(c, c.fd[CONTROL], row, cw::REQUEST_OFFSET);
    if (!good || !saved) return false;
    ++c.completed;
    if (cw::kind(index) == cw::Kind::WARMUP)
        ++c.warmups;
    else if (cw::kind(index) == cw::Kind::FAULT)
        ++c.faults;
    else
        ++c.successes;
    return true;
}
auto finish(Context& c, bool good) -> bool {
    if (c.fd[STATUS] >= 0) {
        auto ended = now_us(c);
        good = good && c.failures == 0 && ended != UINT64_MAX && ended < c.deadline;
        cw::Record status{};
        status[0] = cw::STATUS_MAGIC;
        status[1] = 1;
        status[2] = 512;
        status[3] = c.manifest.nonce;
        status[4] = c.completed;
        status[5] = c.warmups;
        status[6] = c.faults;
        status[7] = c.successes;
        status[8] = 1032;
        status[9] = c.start;
        status[10] = c.deadline;
        status[11] = c.pid;
        status[12] = c.parent;
        status[13] = good ? 0 : static_cast<uint64_t>(c.first);
        status[14] = c.failures;
        status[15] = c.checks;
        status[16] = ended;
        status[17] = c.baseline_hash;
        good = publish(c, c.fd[STATUS], status, 0) && good;
    } else
        good = false;
    // All persistent closes occur only after the final WAIT/census returned.
    for (auto& fd : c.fd)
        if (fd >= 0) {
            int old = fd;
            fd = -1;
            good = vfs(ker::abi::vfs::ops::CLOSE, static_cast<uint64_t>(old)) == 0 && good;
        }
    return good;
}
}  // namespace
int main(int argc, char**) {
    Context c{};
    bool good = argc == 1 && initialize(c);
    for (uint64_t index = 0; good && index < cw::ATTEMPTS; ++index) good = attempt(c, index);
    good = c.completed == 1032 && c.warmups == 8 && c.faults == 24 && c.successes == 1000 && c.failures == 0 && good;
    return finish(c, good) ? 0 : 1;
}
