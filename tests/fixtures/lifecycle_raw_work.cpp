// Freestanding WOS workload. Every successful run remains live until its parent kills it.
#include <abi/callnums/net.h>
#include <abi/callnums/process.h>
#include <abi/callnums/shm.h>
#include <abi/callnums/time.h>
#include <abi/callnums/vfs.h>

#include <abi/callnums.hpp>
#include <abi/resource.hpp>
#include <array>
#include <cstddef>
#include <cstdint>

// Compiler-generated aggregate initialization/copies may use these bounded local
// freestanding definitions. They never link a libc or recognize their own loop
// as an implicit library operation (-fno-builtin, volatile byte accesses).
extern "C" auto memset(void* destination, int value, size_t count) -> void* {
    auto* output = static_cast<volatile unsigned char*>(destination);
    for (size_t i = 0; i < count; ++i) output[i] = static_cast<unsigned char>(value);
    return destination;
}
extern "C" auto memcpy(void* destination, const void* source, size_t count) -> void* {
    auto* output = static_cast<volatile unsigned char*>(destination);
    auto const* input = static_cast<const volatile unsigned char*>(source);
    for (size_t i = 0; i < count; ++i) output[i] = input[i];
    return destination;
}

namespace {
constexpr uint64_t INPUT_MAGIC = 0x4131524157494e50;
constexpr uint64_t READY_MAGIC = 0x4131524157525054;
constexpr uint64_t HEARTBEAT_MAGIC = 0x41314c4956454842;
constexpr uint64_t USER_END = 0x0000800000000000;
constexpr uint64_t INITIAL_ROUNDS = 16;
constexpr uint64_t MAX_ROUNDS = 10000;
constexpr uint64_t READY_OFFSET = 4096;
constexpr uint64_t HEARTBEAT_OFFSET = 1024;
constexpr int REPORT_FD = 9;
constexpr int PRIVATE_OPEN_FLAGS = 0100 | 0200 | 2;
constexpr int SHM_CREATE_FLAGS = ker::abi::shm::IPC_CREAT | ker::abi::shm::IPC_EXCL | 0600;
constexpr int SHM_KEY = 0x4141;
constexpr uint64_t SYSCALL_PROCESS = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
constexpr uint64_t SYSCALL_TIME = static_cast<uint64_t>(ker::abi::callnums::TIME);
constexpr uint64_t SYSCALL_VFS = static_cast<uint64_t>(ker::abi::callnums::VFS);
constexpr uint64_t SYSCALL_SHM = static_cast<uint64_t>(ker::abi::callnums::SHM);
constexpr uint64_t SYSCALL_NET = static_cast<uint64_t>(ker::abi::callnums::NET);
static_assert(SYSCALL_PROCESS == 3 && SYSCALL_TIME == 4 && SYSCALL_VFS == 5 && SYSCALL_NET == 6 && SYSCALL_SHM == 10);
static_assert(static_cast<uint64_t>(ker::abi::process::procmgmt_ops::GETRLIMIT) == 48);
static_assert(static_cast<uint64_t>(ker::abi::process::procmgmt_ops::PRCTL) == 36);
static_assert(static_cast<uint64_t>(ker::abi::vfs::ops::PREAD) == 26 && static_cast<uint64_t>(ker::abi::vfs::ops::PWRITE) == 27);
static_assert(static_cast<uint64_t>(ker::abi::vfs::ops::FCNTL) == 28 && static_cast<uint64_t>(ker::abi::vfs::ops::FSYNC) == 39);
static_assert(sizeof(uint64_t) == 8 && sizeof(size_t) == 8 && ker::abi::resource::COUNT == 16);

struct Startup {
    std::array<uint64_t, 26> words;
};
static_assert(sizeof(Startup) == 208 && alignof(Startup) == 8 && offsetof(Startup, words) == 0);
struct Timespec {
    int64_t seconds;
    int64_t nanoseconds;
};
static_assert(sizeof(Timespec) == 16);

// This local ABI mirror avoids including kernel VFS objects/runtime headers.
struct Stat {
    uint64_t device;
    uint64_t inode;
    uint64_t links;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint32_t padding;
    uint64_t rdevice;
    int64_t size;
    int64_t block_size;
    int64_t blocks;
    std::array<Timespec, 3> times;
    std::array<int64_t, 3> unused;
};
static_assert(sizeof(Stat) == 144 && offsetof(Stat, mode) == 24 && offsetof(Stat, size) == 48 && offsetof(Stat, unused) == 120);

template <size_t N>
using Words = std::array<uint64_t, N>;
struct Context {
    Words<64> input{};
    Words<256> ready{};
    uint64_t checks = 0;
    uint64_t last_clock_us = 0;
    int work_fd = -1;
    int shm_id = -1;
    volatile uint64_t* shared = nullptr;
};
static_assert(sizeof(Context) < 4096);

auto raw_call(uint64_t subsystem, uint64_t a1, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0, uint64_t a5 = 0, uint64_t a6 = 0) noexcept
    -> int64_t {
    register uint64_t rax __asm__("rax") = subsystem;
    register uint64_t rdi __asm__("rdi") = a1;
    register uint64_t rsi __asm__("rsi") = a2;
    register uint64_t rdx __asm__("rdx") = a3;
    register uint64_t r8 __asm__("r8") = a4;
    register uint64_t r9 __asm__("r9") = a5;
    register uint64_t r10 __asm__("r10") = a6;
    __asm__ volatile("syscall" : "+a"(rax) : "D"(rdi), "S"(rsi), "d"(rdx), "r"(r8), "r"(r9), "r"(r10) : "rcx", "r11", "memory");
    return static_cast<int64_t>(rax);
}
[[noreturn]] void fail(unsigned code) noexcept {
    (void)raw_call(SYSCALL_PROCESS, 0, code);
    __asm__ volatile("ud2");
    __builtin_unreachable();
}
void require(Context& context, bool predicate, unsigned code) noexcept {
    if (!predicate) fail(code);
    if (context.checks == UINT64_MAX) fail(113);
    ++context.checks;
}
auto address(const void* pointer) noexcept -> uint64_t { return reinterpret_cast<uint64_t>(pointer); }
auto raw_fcntl(int fd, int command) noexcept -> int64_t {
    return raw_call(SYSCALL_VFS, 28, static_cast<uint64_t>(fd), static_cast<uint64_t>(command));
}
auto raw_pread(int fd, void* buffer, size_t count, uint64_t offset) noexcept -> int64_t {
    return raw_call(SYSCALL_VFS, 26, static_cast<uint64_t>(fd), address(buffer), count, offset);
}
auto raw_pwrite(int fd, const void* buffer, size_t count, uint64_t offset) noexcept -> int64_t {
    return raw_call(SYSCALL_VFS, 27, static_cast<uint64_t>(fd), address(buffer), count, offset);
}
auto raw_stat(int fd, Stat& result) noexcept -> int64_t { return raw_call(SYSCALL_VFS, 13, static_cast<uint64_t>(fd), address(&result)); }
auto load_le(const uint8_t* bytes) noexcept -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) value |= static_cast<uint64_t>(bytes[i]) << (8 * i);
    return value;
}
void store_le(uint8_t* bytes, uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) bytes[i] = static_cast<uint8_t>(value >> (8 * i));
}
template <size_t N>
void encode(const Words<N>& words, std::array<uint8_t, N * 8>& bytes) noexcept {
    for (size_t i = 0; i < N; ++i) store_le(bytes.data() + i * 8, words.data()[i]);
}
auto now_us(Context& context) noexcept -> uint64_t {
    Timespec value{};
    int64_t const RESULT = raw_call(SYSCALL_TIME, 1, address(&value), 1);
    require(context, RESULT == 0 && value.seconds >= 0 && value.nanoseconds >= 0 && value.nanoseconds < 1000000000, 110);
    uint64_t const SECONDS = static_cast<uint64_t>(value.seconds);
    require(context, SECONDS <= (UINT64_MAX - 999999) / 1000000, 110);
    uint64_t const TIME = SECONDS * 1000000 + static_cast<uint64_t>(value.nanoseconds) / 1000;
    require(context, TIME >= context.last_clock_us, 110);
    context.last_clock_us = TIME;
    return TIME;
}
void check_deadline(Context& context) noexcept { require(context, now_us(context) < context.input.data()[8], 110); }
auto string_matches(uint64_t pointer, const char* expected, size_t length) noexcept -> bool {
    if (pointer == 0 || pointer >= USER_END || length + 1 > USER_END - pointer) return false;
    auto const* input = reinterpret_cast<const char*>(pointer);
    for (size_t i = 0; i < length; ++i)
        if (input[i] != expected[i]) return false;
    return input[length] == '\0';
}

void verify_main_headers(Context& context, const Startup& startup) noexcept {
    auto const* s = startup.words.data();
    auto const* image = reinterpret_cast<const uint8_t*>(uint64_t{0x400000});
    auto const* copied = reinterpret_cast<const uint8_t*>(uint64_t{0x1000});
    require(context, s[11] == 0x1040 && s[12] == 56 && s[13] >= 1 && s[13] <= 32, 104);
    require(context,
            load_le(image) == load_le(copied) && image[0] == 0x7f && image[1] == 'E' && image[2] == 'L' && image[3] == 'F' &&
                image[4] == 2 && image[5] == 1 && image[6] == 1 && image[16] == 2 && image[17] == 0 && image[18] == 62 && image[19] == 0 &&
                load_le(image + 24) == s[2] && load_le(image + 32) == 64 && image[54] == 56 && image[55] == 0 && image[56] == s[13] &&
                image[57] == 0,
            104);
    for (size_t i = 0; i < 64; ++i) require(context, image[i] == copied[i], 104);
    for (size_t i = 0; i < s[13]; ++i) {
        auto const* original = image + 64 + i * 56;
        auto const* observed = copied + 64 + i * 56;
        bool const IS_PHDR = (load_le(original) & UINT32_MAX) == 6;
        for (size_t word = 0; word < 7; ++word) {
            uint64_t const EXPECTED = IS_PHDR && (word == 2 || word == 3) ? 0x1040 : load_le(original + word * 8);
            require(context, load_le(observed + word * 8) == EXPECTED, 104);
        }
    }
}

void read_input(Context& context, const Startup& startup, uint64_t raw_start) noexcept {
    std::array<uint8_t, 512> first{};
    std::array<uint8_t, 512> second{};
    int64_t const FIRST = raw_pread(REPORT_FD, first.data(), first.size(), 0);
    int64_t const SECOND = raw_pread(REPORT_FD, second.data(), second.size(), 0);
    require(context, FIRST == 512 && SECOND == 512, 106);
    for (size_t i = 0; i < first.size(); ++i) require(context, first.data()[i] == second.data()[i], 106);
    for (size_t i = 0; i < context.input.size(); ++i) context.input.data()[i] = load_le(first.data() + i * 8);
    auto const* h = context.input.data();
    require(context, h[0] == INPUT_MAGIC && h[1] == 1 && h[2] == 512 && h[3] != 0 && h[4] != 0 && h[4] < (uint64_t{1} << 60), 106);
    require(context, (h[5] == 1 || h[5] == 2) && h[5] == startup.words.data()[25] && h[6] > 2, 106);
    require(context, h[7] <= UINT64_MAX - 10000000 && h[8] == h[7] + 10000000 && h[7] <= raw_start && raw_start < h[8], 106);
    require(context, h[9] == 100 && h[10] == 1 && h[11] == 0 && h[12] == 256 && h[13] == 0 && h[14] == 0 && h[15] == 0, 106);
    for (size_t i = 0; i < 16; ++i) require(context, h[16 + i * 2] <= h[17 + i * 2], 106);
    require(context, h[30] >= 10 && h[31] >= 10 && h[18] >= 8192 && h[19] >= 8192, 106);
    require(context, h[48] == READY_OFFSET && h[49] == 2048 && h[50] == 8192 && h[51] == 32 && h[52] == SHM_KEY && h[53] == 4096, 106);
    require(context,
            h[54] == 1 && h[55] == 0 && h[59] == HEARTBEAT_OFFSET && h[60] == 256 && h[61] == INITIAL_ROUNDS && h[62] == MAX_ROUNDS &&
                h[63] == h[3],
            106);
    Stat report_stat{};
    int64_t const STAT_RESULT = raw_stat(REPORT_FD, report_stat);
    int64_t const FLAGS = raw_fcntl(REPORT_FD, 3);
    require(context,
            STAT_RESULT == 0 && (report_stat.mode & 0xf000U) == 0x8000U && report_stat.size == 8192 && report_stat.inode == h[56] &&
                report_stat.device == h[57],
            106);
    require(context, FLAGS >= 0 && static_cast<uint64_t>(FLAGS) == h[58] && (FLAGS & 3) == 2 && raw_fcntl(REPORT_FD, 1) == 0, 106);
    uint64_t absent = 0;
    for (int fd = 0; fd < 256; ++fd) {
        if (fd == REPORT_FD) continue;
        require(context, raw_fcntl(fd, 1) == -9, 106);
        ++absent;
    }
    auto* r = context.ready.data();
    r[28] = absent;
    r[29] = static_cast<uint64_t>(FLAGS);
    r[30] = report_stat.inode;
    r[31] = report_stat.device;
    r[112] = static_cast<uint64_t>(report_stat.size);
    r[113] = static_cast<uint64_t>(STAT_RESULT);
    r[114] = static_cast<uint64_t>(FIRST);
    r[224] = h[63];
    r[226] = 1;
    r[231] = 1;
}

void observe_identity_and_limits(Context& context) noexcept {
    auto* r = context.ready.data();
    constexpr std::array<uint64_t, 6> OPERATIONS{3, 4, 10, 11, 12, 13};
    constexpr std::array<int64_t, 6> EXPECTED{1, 0, 100, 100, 100, 100};
    for (size_t i = 0; i < OPERATIONS.size(); ++i) {
        int64_t const RESULT = raw_call(SYSCALL_PROCESS, OPERATIONS.data()[i]);
        r[8 + i] = static_cast<uint64_t>(RESULT);
        require(context, RESULT == EXPECTED.data()[i], 105);
    }
    constexpr std::array<uint64_t, 3> CONTROLS{39, 0x574f5301, 0x574f5304};
    constexpr std::array<int64_t, 3> CONTROL_RESULTS{1, 0, 256};
    for (size_t i = 0; i < CONTROLS.size(); ++i) {
        int64_t const RESULT = raw_call(SYSCALL_PROCESS, 36, CONTROLS.data()[i]);
        r[14 + i] = static_cast<uint64_t>(RESULT);
        require(context, RESULT == CONTROL_RESULTS.data()[i], 105);
    }
    for (size_t i = 0; i < 16; ++i) {
        ker::abi::resource::Limit actual{};
        int64_t const RESULT = raw_call(SYSCALL_PROCESS, 48, i, address(&actual), 1);
        r[32 + i] = static_cast<uint64_t>(RESULT);
        r[64 + i * 2] = actual.soft;
        r[65 + i * 2] = actual.hard;
        require(context, RESULT == 0 && actual.soft == context.input.data()[16 + i * 2] && actual.hard == context.input.data()[17 + i * 2],
                105);
    }
    int64_t const IPV4 = raw_call(SYSCALL_NET, 0, 2, 1, 0);
    int64_t const IPV6 = raw_call(SYSCALL_NET, 0, 10, 1, 0);
    r[55] = static_cast<uint64_t>(IPV4);
    r[56] = static_cast<uint64_t>(IPV6);
    require(context, IPV4 == -1 && IPV6 == -1, 109);
}

void create_live_objects(Context& context) noexcept {
    constexpr char PATH[] = "/live.bin";
    int64_t const OPEN = raw_call(SYSCALL_VFS, 0, address(PATH), PRIVATE_OPEN_FLAGS, 0600);
    require(context, OPEN >= 0 && OPEN < 256 && OPEN != REPORT_FD, 107);
    context.work_fd = static_cast<int>(OPEN);
    int64_t const FLAGS = raw_fcntl(context.work_fd, 3);
    require(context, FLAGS >= 0 && (FLAGS & 3) == 2, 107);
    int64_t const ID = raw_call(SYSCALL_SHM, 0, SHM_KEY, 4096, SHM_CREATE_FLAGS);
    require(context, ID > 0 && ID <= INT32_MAX, 108);
    context.shm_id = static_cast<int>(ID);
    int64_t const ATTACH = raw_call(SYSCALL_SHM, 1, static_cast<uint64_t>(ID));
    require(context, ATTACH > 0 && static_cast<uint64_t>(ATTACH) < USER_END - 4096 && (ATTACH & 4095) == 0, 108);
    context.shared = reinterpret_cast<volatile uint64_t*>(static_cast<uint64_t>(ATTACH));
    auto* r = context.ready.data();
    r[48] = static_cast<uint64_t>(OPEN);
    r[49] = static_cast<uint64_t>(FLAGS);
    r[51] = static_cast<uint64_t>(ID);
    r[52] = static_cast<uint64_t>(ATTACH);
}

struct Round {
    int64_t written;
    int64_t read;
    int64_t lookup;
    int64_t stat;
};
auto run_round(Context& context, uint64_t round) noexcept -> Round {
    check_deadline(context);
    Words<4> payload{context.input.data()[3], context.input.data()[4], round, ~round};
    std::array<uint8_t, 32> bytes{};
    std::array<uint8_t, 32> received{};
    encode(payload, bytes);
    Round result{};
    result.written = raw_pwrite(context.work_fd, bytes.data(), bytes.size(), 0);
    result.read = raw_pread(context.work_fd, received.data(), received.size(), 0);
    require(context, result.written == 32 && result.read == 32, 107);
    for (size_t i = 0; i < bytes.size(); ++i) require(context, bytes.data()[i] == received.data()[i], 107);
    for (size_t i = 0; i < payload.size(); ++i) context.shared[i] = payload.data()[i];
    __asm__ volatile("" ::: "memory");
    for (size_t i = 0; i < payload.size(); ++i) require(context, context.shared[i] == payload.data()[i], 108);
    result.lookup = raw_call(SYSCALL_SHM, 0, SHM_KEY, 4096, 0600);
    ker::abi::shm::ShmidDs stat{};
    result.stat = raw_call(SYSCALL_SHM, 3, static_cast<uint64_t>(context.shm_id), 2, address(&stat));
    require(context,
            result.lookup == context.shm_id && result.stat == 0 && stat.shm_segsz == 4096 && stat.shm_perm.key == SHM_KEY &&
                stat.shm_perm.uid == 100 && stat.shm_perm.gid == 100 && stat.shm_perm.cuid == 100 && stat.shm_perm.cgid == 100 &&
                stat.shm_perm.mode == 0600 && stat.shm_nattch == 1,
            108);
    auto* r = context.ready.data();
    if (round <= INITIAL_ROUNDS) {
        size_t const INDEX = static_cast<size_t>(round - 1);
        r[128 + INDEX] = static_cast<uint64_t>(result.written);
        r[144 + INDEX] = static_cast<uint64_t>(result.read);
        r[160 + INDEX] = static_cast<uint64_t>(result.lookup);
        r[176 + INDEX] = static_cast<uint64_t>(result.stat);
        r[192 + INDEX] = 1;
        r[208 + INDEX] = 1;
        r[53] = static_cast<uint64_t>(result.lookup);
        r[54] = static_cast<uint64_t>(result.stat);
        r[116] = static_cast<uint64_t>(stat.shm_perm.key);
        r[117] = stat.shm_segsz;
        r[118] = stat.shm_perm.uid;
        r[119] = stat.shm_perm.gid;
        r[120] = stat.shm_perm.mode;
        r[121] = stat.shm_nattch;
    }
    return result;
}

template <size_t N>
void publish(Context& context, Words<N>& words, uint64_t offset, uint64_t commit) noexcept {
    std::array<uint8_t, N * 8> bytes{};
    words.data()[N - 1] = 0;
    encode(words, bytes);
    require(context, raw_pwrite(REPORT_FD, bytes.data(), bytes.size(), offset) == static_cast<int64_t>(bytes.size()), 111);
    std::array<uint8_t, 8> final{};
    store_le(final.data(), commit);
    require(context, raw_pwrite(REPORT_FD, final.data(), final.size(), offset + (N - 1) * 8) == 8, 111);
    words.data()[N - 1] = commit;
}

void publish_ready(Context& context, const Startup& startup, uint64_t raw_start) noexcept {
    Stat stat{};
    int64_t const FSYNC = raw_call(SYSCALL_VFS, 39, static_cast<uint64_t>(context.work_fd));
    int64_t const STAT = raw_stat(context.work_fd, stat);
    require(context, FSYNC == 0 && STAT == 0 && stat.size == 32 && stat.mode == (0x8000U | 0600U) && stat.uid == 100 && stat.gid == 100,
            107);
    uint64_t const READY_TIME = now_us(context);
    require(context, READY_TIME < context.input.data()[8], 110);
    auto* r = context.ready.data();
    auto const* h = context.input.data();
    auto const* s = startup.words.data();
    r[0] = READY_MAGIC;
    r[1] = 1;
    r[2] = 2048;
    r[3] = 1;
    r[4] = h[3];
    r[5] = h[4];
    r[6] = h[5];
    r[7] = s[25];
    r[17] = s[23];
    r[21] = context.checks;
    r[22] = h[6];
    r[23] = h[7];
    r[24] = raw_start;
    r[25] = READY_TIME;
    r[26] = h[8];
    r[27] = INITIAL_ROUNDS;
    r[50] = static_cast<uint64_t>(STAT);
    r[57] = INITIAL_ROUNDS;
    r[58] = ~INITIAL_ROUNDS;
    r[59] = stat.inode;
    r[60] = stat.device;
    r[61] = static_cast<uint64_t>(stat.size);
    r[62] = stat.mode;
    r[63] = stat.uid;
    for (size_t i = 0; i < 16; ++i) r[96 + i] = s[i];
    r[115] = stat.gid;
    r[122] = h[3];
    r[123] = h[4];
    r[124] = INITIAL_ROUNDS;
    r[125] = ~INITIAL_ROUNDS;
    r[126] = h[3];
    r[127] = h[4];
    r[225] = static_cast<uint64_t>(FSYNC);
    for (size_t i = 0; i < 4; ++i) r[227 + i] = s[16 + i];
    r[232] = 1;
    r[233] = s[25] == 2 ? 1 : 0;
    r[234] = s[20];
    r[235] = s[21];
    r[236] = s[22];
    r[237] = s[24];
    r[238] = 0;
    r[239] = 0;
    publish(context, context.ready, READY_OFFSET, h[3]);
}

void publish_heartbeat(Context& context, uint64_t round, const Round& result) noexcept {
    Words<32> h{};
    auto* w = h.data();
    w[0] = HEARTBEAT_MAGIC;
    w[1] = 1;
    w[2] = context.input.data()[3];
    w[3] = context.input.data()[4];
    w[4] = context.input.data()[5];
    w[5] = round;
    w[6] = now_us(context);
    require(context, w[6] < context.input.data()[8], 110);
    w[7] = static_cast<uint64_t>(result.written);
    w[8] = static_cast<uint64_t>(result.read);
    w[9] = static_cast<uint64_t>(result.lookup);
    w[10] = static_cast<uint64_t>(result.stat);
    w[11] = round;
    w[12] = ~round;
    w[13] = context.checks;
    w[16] = static_cast<uint64_t>(context.work_fd);
    w[17] = static_cast<uint64_t>(context.shm_id);
    w[18] = reinterpret_cast<uint64_t>(context.shared);
    w[20] = 1;
    w[21] = 1;
    w[22] = 100;
    w[23] = 1;
    publish(context, h, HEARTBEAT_OFFSET, round);
}
}  // namespace

extern "C" [[noreturn]] void lifecycle_raw_main(const Startup* startup) noexcept {
    Context context{};
    require(context, startup != nullptr, 104);
    auto const* s = startup->words.data();
    require(context, (s[25] == 1 || s[25] == 2) && s[23] == (s[25] == 2 ? 0xff : 0x7f) && s[9] == 1 && s[10] == 0, 104);
    require(context, s[0] != 0 && s[0] < USER_END - 16 && (s[0] & 15) == 0, 104);
    verify_main_headers(context, *startup);
    uint64_t const ARGV0 = *reinterpret_cast<const uint64_t*>(s[0] + 8);
    constexpr char EXECUTABLE[] = "/app/lifecycle-raw";
    require(context, string_matches(ARGV0, EXECUTABLE, sizeof(EXECUTABLE) - 1) && string_matches(s[21], EXECUTABLE, sizeof(EXECUTABLE) - 1),
            104);
    uint64_t const RAW_START = now_us(context);
    read_input(context, *startup, RAW_START);
    observe_identity_and_limits(context);
    create_live_objects(context);
    for (uint64_t round = 1; round <= INITIAL_ROUNDS; ++round) (void)run_round(context, round);
    publish_ready(context, *startup, RAW_START);
    for (uint64_t round = INITIAL_ROUNDS + 1; round <= MAX_ROUNDS; ++round) {
        Round const RESULT = run_round(context, round);
        publish_heartbeat(context, round, RESULT);
        Timespec const REQUEST{0, 1000000};
        require(context, raw_call(SYSCALL_TIME, 2, address(&REQUEST)) == 0, 112);
        check_deadline(context);
    }
    fail(110);
}
