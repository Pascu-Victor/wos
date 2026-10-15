#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// Internal staged test wire, not a syscall ABI or a user fault-control API.
namespace criterion9::wire {
inline constexpr uint64_t VERSION = 1;
inline constexpr uint64_t WARMUPS = 8, FAULTS = 24, SUCCESSES = 1000;
inline constexpr uint64_t ATTEMPTS = WARMUPS + FAULTS + SUCCESSES;
inline constexpr char SERVICE[] = "lifecycle-raw-pilot";
inline constexpr char MANIFEST_PATH[] = "/etc/wos-lifecycle60-manifest.bin";
inline constexpr char RESULTS[] = "/srv/lifecycle60/";
inline constexpr char ASSETS[] = "/usr/libexec/lifecycle60/";
inline constexpr char EXECUTABLE[] = "/app/lifecycle-raw";
inline constexpr char INTERPRETER[] = "/lib/lifecycle-tiny-ld.so";
inline constexpr uint64_t MANIFEST_MAGIC = 0x43394d414e494645ULL;
inline constexpr uint64_t HEADER_MAGIC = 0x4339484541444552ULL;
inline constexpr uint64_t ATTEMPT_MAGIC = 0x4339415454454d50ULL;
inline constexpr uint64_t STATUS_MAGIC = 0x4339535441545553ULL;
inline constexpr uint64_t FD_MAGIC = 0x433946444c454447ULL;
inline constexpr uint64_t SEAL = 0xa69e572d43f18bc5ULL;
inline constexpr uint64_t CONTROL_BYTES = 4096, CHILD_BYTES = 8192;
inline constexpr uint64_t RECORD_BYTES = 512, EVIDENCE_BYTES = 4096;
inline constexpr uint64_t LEDGER_BYTES = ATTEMPTS * RECORD_BYTES;
inline constexpr uint64_t ALL_EVIDENCE_BYTES = ATTEMPTS * EVIDENCE_BYTES;
inline constexpr uint64_t FD_BYTES = 8192, STATUS_BYTES = 512;
inline constexpr uint64_t REQUEST_OFFSET = 512;
inline constexpr uint64_t INPUT_OFFSET = 0, READY_OFFSET = 512;
inline constexpr uint64_t HEARTBEAT_A_OFFSET = 2560, HEARTBEAT_B_OFFSET = 2816;
inline constexpr int64_t UNOBSERVED = INT64_MIN;
inline constexpr uint64_t CAMPAIGN_US = 3'600'000'000ULL;

using Record = std::array<uint64_t, 64>;
using Input = std::array<uint64_t, 64>;
using Ready = std::array<uint64_t, 256>;
using Heartbeat = std::array<uint64_t, 32>;
using Evidence = std::array<uint64_t, EVIDENCE_BYTES / 8>;
using FdLedger = std::array<uint64_t, FD_BYTES / 8>;
enum class Kind : uint64_t { WARMUP = 0, FAULT = 1, SUCCESS = 2 };
enum class State : uint64_t { REQUEST = 1, BEFORE_WAIT = 2, COMPLETED = 3 };
enum Field : size_t {
    MAGIC,
    VER,
    BYTES,
    NONCE,
    INDEX,
    ATTEMPT_ID,
    KIND,
    VARIANT,
    STATE,
    START_US,
    DEADLINE_US,
    CREATE_RAW,
    PID,
    READY_OBS_US,
    HB_A_ROUND,
    HB_B_ROUND,
    HB_A_US,
    HB_B_US,
    KILL_START_US,
    KILL_RAW,
    WAIT_RAW,
    WAIT_STATUS,
    WAIT_DONE_US,
    INPUT_HASH,
    READY_HASH,
    HB_A_HASH,
    HB_B_HASH,
    RAW_DEV,
    RAW_INO,
    RAW_FD,
    CREATE_RETURN_US,
    FAILURE_STAGE,
    FD_LEDGER_HASH,
    MAIN_DEV,
    MAIN_INO,
    MAIN_BYTES,
    INTERP_DEV,
    INTERP_INO,
    INTERP_BYTES,
    PARENT_PID,
    CHECKS,
    END_US,
    WAIT_ZERO,
    WAIT_EINTR,
    CHECKSUM = 62,
    COMMIT = 63
};
enum HeaderField : size_t {
    H_MAGIC,
    H_VERSION,
    H_BYTES,
    H_NONCE,
    H_DRIVER_PID,
    H_PARENT_PID,
    H_ATTEMPTS,
    H_WARMUPS,
    H_FAULTS,
    H_SUCCESSES,
    H_RECORD_BYTES,
    H_EVIDENCE_BYTES,
    H_CHILD_BYTES,
    H_CONTROL_BYTES,
    H_FD_BYTES,
    H_STATUS_BYTES,
    H_START_US,
    H_DEADLINE_US,
    H_STATIC_FD = 24,
    H_TINY_FD,
    H_INTERP_FD,
    H_CHILD_FD,
    H_CONTROL_FD,
    H_ATTEMPTS_FD,
    H_EVIDENCE_FD,
    H_FD_LEDGER_FD,
    H_STATUS_FD
};
enum class AssetRole : uint64_t { DRIVER = 1, STATIC = 2, TINY = 3, INTERPRETER = 4 };
struct ManifestEntry {
    uint64_t role{}, bytes{}, elf_type{}, entry{}, phnum{};
    std::array<uint8_t, 32> sha256{};
    std::array<uint64_t, 7> reserved{};
};
struct Manifest {
    uint64_t magic{}, version{}, bytes{}, record_count{}, nonce{}, warmups{}, faults{}, successes{};
    std::array<ManifestEntry, 4> entries{};
    std::array<uint64_t, 56> reserved{};
};
static_assert(sizeof(ManifestEntry) == 128 && alignof(ManifestEntry) == 8);
static_assert(offsetof(ManifestEntry, sha256) == 40 && offsetof(ManifestEntry, reserved) == 72);
static_assert(sizeof(Manifest) == 1024 && alignof(Manifest) == 8 && offsetof(Manifest, entries) == 64);
static_assert(offsetof(Manifest, reserved) == 576 && sizeof(Record) == 512);
static_assert(LEDGER_BYTES == 528384 && ALL_EVIDENCE_BYTES == 4227072);
static_assert(HEARTBEAT_B_OFFSET + sizeof(Heartbeat) == 3072);

[[nodiscard]] constexpr auto kind(uint64_t index) -> Kind {
    return index < WARMUPS ? Kind::WARMUP : index < WARMUPS + FAULTS ? Kind::FAULT : Kind::SUCCESS;
}
// All fault attempts use a real interpreter-bearing image. Controller chooses
// the fault solely from its fixed kernel plan, never from this value.
[[nodiscard]] constexpr auto variant(uint64_t index) -> uint64_t { return kind(index) == Kind::FAULT ? 2 : 1 + (index & 1U); }
[[nodiscard]] constexpr auto expected_error(uint64_t index) -> int64_t {
    constexpr std::array<int64_t, FAULTS> ERRORS{-12, -12, -12, -12, -12, -125, -12,  -12,  -12, -12, -125, -125,
                                                 -12, -12, -12, -12, -5,  -12,  -125, -125, -11, -11, -12,  -12};
    return kind(index) == Kind::FAULT ? ERRORS[index - WARMUPS] : 0;
}
// Explicit LE64 integrity hash. Not authentication or kernel success authority.
template <size_t N>
[[nodiscard]] constexpr auto hash_words(const std::array<uint64_t, N>& words, size_t count = N) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < count; ++i) {
        for (unsigned byte = 0; byte < 8; ++byte) {
            hash ^= (words[i] >> (byte * 8)) & 0xffU;
            hash *= 1099511628211ULL;
        }
    }
    return hash;
}
constexpr void seal(Record& row) {
    row[CHECKSUM] = hash_words(row, CHECKSUM);
    row[COMMIT] = (row[CHECKSUM] ^ SEAL) | 1U;
}
[[nodiscard]] constexpr auto sealed(const Record& row) -> bool {
    return row[CHECKSUM] == hash_words(row, CHECKSUM) && row[COMMIT] == ((row[CHECKSUM] ^ SEAL) | 1U);
}

// These predicates validate bytes, not provenance, fault consumption, Task
// deletion or census. Caller must supply actual actor/PID/index and compare
// stabilized actual Files outside ACK before acquiring a census permission.
[[nodiscard]] constexpr auto user_pointer(uint64_t value, uint64_t length = 1) -> bool {
    return value >= 4096 && value < 0x800000000000ULL && length <= 0x800000000000ULL - value;
}
[[nodiscard]] constexpr auto record_keys(const Record& row, uint64_t nonce, uint64_t index, State state) -> bool {
    if (index >= ATTEMPTS || row[MAGIC] != ATTEMPT_MAGIC || row[VER] != VERSION || row[BYTES] != RECORD_BYTES || row[NONCE] != nonce ||
        nonce == 0 || row[INDEX] != index || row[ATTEMPT_ID] != index + 1 || row[KIND] != static_cast<uint64_t>(kind(index)) ||
        row[VARIANT] != variant(index) || row[STATE] != static_cast<uint64_t>(state) || !sealed(row))
        return false;
    for (size_t i = 44; i < CHECKSUM; ++i)
        if (row[i] != 0) return false;
    return true;
}
[[nodiscard]] constexpr auto valid_input(const Input& h, const Record& row, uint64_t actual_driver_pid) -> bool {
    if (actual_driver_pid <= 2 || h[0] != 0x4131524157494e50ULL || h[1] != 1 || h[2] != 512 || h[3] == 0 || h[3] != row[NONCE] ||
        h[4] != row[ATTEMPT_ID] || (h[5] != 1 && h[5] != 2) || h[5] != row[VARIANT] || h[6] != actual_driver_pid ||
        row[PARENT_PID] != actual_driver_pid || h[7] == 0 || h[7] != row[START_US] || h[7] > UINT64_MAX - 60'000'000 ||
        h[8] != h[7] + 60'000'000 || h[8] != row[DEADLINE_US] || h[9] != 100 || h[10] != 1 || h[11] != 0 || h[12] != 256 || h[13] != 0 ||
        h[14] != 0 || h[15] != 0)
        return false;
    for (size_t i = 0; i < 16; ++i)
        if (h[16 + 2 * i] > h[17 + 2 * i]) return false;
    if (h[16] != 10 || h[17] != 10 || h[18] < 8192 || h[18] > 1048576 || h[19] < 8192 || h[19] > 1048576 || h[30] < 10 || h[30] > 256 ||
        h[31] < 10 || h[31] > 256 || h[48] != 4096 || h[49] != 2048 || h[50] != 8192 || h[51] != 32 || h[52] != 0x4141 || h[53] != 4096 ||
        h[54] != 1 || h[55] != 0 || h[56] != row[RAW_INO] || h[57] != row[RAW_DEV] || h[58] >= (uint64_t{1} << 63) || (h[58] & 3U) != 2 ||
        h[59] != 1024 || h[60] != 256 || h[61] != 16 || h[62] != 10000 || h[63] != h[3] || hash_words(h) != row[INPUT_HASH])
        return false;
    return true;
}
[[nodiscard]] constexpr auto valid_ready(const Input& h, const Ready& r, const Manifest& manifest, uint64_t observed) -> bool {
    if (h[5] != 1 && h[5] != 2) return false;
    bool const tiny = h[5] == 2;
    auto const& main = manifest.entries[h[5]];
    auto const& interp = manifest.entries[3];
    if (r[0] != 0x4131524157525054ULL || r[1] != 1 || r[2] != 2048 || r[3] != 1 || r[4] != h[3] || r[5] != h[4] || r[6] != h[5] ||
        r[7] != h[5] || r[8] != 1 || r[9] != 0)
        return false;
    for (size_t i = 10; i <= 13; ++i)
        if (r[i] != 100) return false;
    if (r[14] != 1 || r[15] != 0 || r[16] != 256 || r[17] != (tiny ? 255U : 127U) || r[18] != 0 || r[19] != 0 || r[20] != 0 || r[21] == 0 ||
        r[22] != h[6] || r[23] != h[7] || h[7] > r[24] || r[24] > r[25] || r[25] > observed || observed >= h[8] || r[26] != h[8] ||
        r[27] != 16 || r[28] != 255 || r[29] != h[58] || r[30] != h[56] || r[31] != h[57])
        return false;
    for (size_t i = 0; i < 16; ++i)
        if (r[32 + i] != 0 || r[64 + 2 * i] != h[16 + 2 * i] || r[65 + 2 * i] != h[17 + 2 * i]) return false;
    if (r[48] >= 256 || r[48] == 9 || r[49] >= (uint64_t{1} << 63) || (r[49] & 3U) != 2 || r[50] != 0 || r[51] == 0 || r[51] > INT32_MAX ||
        !user_pointer(r[52], 4096) || (r[52] & 4095U) != 0 || r[53] != r[51] || r[54] != 0 || r[55] != UINT64_MAX || r[56] != UINT64_MAX ||
        r[57] != 16 || r[58] != ~uint64_t{16} || r[61] != 32 || r[62] != 0100600 || r[63] != 100 || !user_pointer(r[96], 1024) ||
        (r[96] & 15U) != 0 || r[97] != main.entry || r[98] != main.entry || r[105] != 1 || r[106] != 0 || r[107] != 0x1040 ||
        r[108] != 56 || r[109] != main.phnum || r[110] != 4096 || r[111] != 0)
        return false;
    if (tiny) {
        if (r[99] < 0x30000000 || r[99] >= 0x70000000 || (r[99] & 0x1fffffU) != 0 || interp.entry > UINT64_MAX - r[99] ||
            r[100] != r[99] + interp.entry || r[101] != 0x54494e593431484fULL)
            return false;
    } else if (r[99] != 0 || r[100] != 0)
        return false;
    // Incoming static R12 / RDI / RSI / RDX remain opaque, as in actual startup.
    constexpr std::array<uint64_t, 16> EXPECTED{8192, 0, 512, 100, 0x4141, 4096, 100, 100, 0600, 1, 0, 0, 16, ~uint64_t{16}, 0, 0};
    for (size_t i = 0; i < EXPECTED.size(); ++i) {
        uint64_t value = EXPECTED[i];
        if (i == 10 || i == 14) value = h[3];
        if (i == 11 || i == 15) value = h[4];
        if (r[112 + i] != value) return false;
    }
    for (size_t i = 0; i < 16; ++i)
        if (r[128 + i] != 32 || r[144 + i] != 32 || r[160 + i] != r[51] || r[176 + i] != 0 || r[192 + i] != 1 || r[208 + i] != 1)
            return false;
    if (r[224] != h[3] || r[225] != 0 || r[226] != 1) return false;
    for (size_t i = 227; i <= 230; ++i)
        if (r[i] != 100) return false;
    if (r[231] != 1 || r[232] != 1 || r[233] != (tiny ? 1U : 0U) || !user_pointer(r[234], 16) || !user_pointer(r[235], 19) ||
        r[236] != (tiny ? 13U : 12U) || r[237] != (tiny ? interp.entry : 0) || r[238] != 0 || r[239] != 0 || r[255] != h[3])
        return false;
    for (size_t i = 240; i < 255; ++i)
        if (r[i] != 0) return false;
    return true;
}
[[nodiscard]] constexpr auto valid_heartbeat(const Input& h, const Ready& r, const Heartbeat& b, uint64_t minimum_round, uint64_t observed)
    -> bool {
    if (b[0] != 0x41314c4956454842ULL || b[1] != 1 || b[2] != h[3] || b[3] != h[4] || b[4] != h[5] || b[5] < minimum_round || b[5] < 17 ||
        b[5] > 10000 || b[6] < r[25] || b[6] > observed || observed >= h[8] || b[7] != 32 || b[8] != 32 || b[9] != r[51] || b[10] != 0 ||
        b[11] != b[5] || b[12] != ~b[5] || b[13] < r[21] || b[14] != 0 || b[15] != 0 || b[16] != r[48] || b[17] != r[51] ||
        b[18] != r[52] || b[19] != 0 || b[20] != 1 || b[21] != 1 || b[22] != 100 || b[23] != 1 || b[31] != b[5])
        return false;
    for (size_t i = 24; i <= 30; ++i)
        if (b[i] != 0) return false;
    return true;
}
[[nodiscard]] constexpr auto valid_work_evidence(const Input& h, const Ready& r, const Heartbeat& a, const Heartbeat& b, const Record& row,
                                                 const Manifest& manifest, uint64_t actual_driver_pid) -> bool {
    return manifest.nonce == h[3] && row[PID] > 2 && row[PID] < (uint64_t{1} << 63) && row[PID] != actual_driver_pid &&
           row[CREATE_RAW] == row[PID] && row[KILL_RAW] == 0 && valid_input(h, row, actual_driver_pid) &&
           valid_ready(h, r, manifest, row[READY_OBS_US]) && valid_heartbeat(h, r, a, 17, row[HB_A_US]) && a[5] < 10000 &&
           valid_heartbeat(h, r, b, a[5] + 1, row[HB_B_US]) && b[13] > a[13] && b[6] >= a[6] && row[HB_A_ROUND] == a[5] &&
           row[HB_B_ROUND] == b[5] && row[READY_HASH] == hash_words(r) && row[HB_A_HASH] == hash_words(a) &&
           row[HB_B_HASH] == hash_words(b) && row[CREATE_RETURN_US] <= row[READY_OBS_US] && row[READY_OBS_US] <= row[HB_A_US] &&
           row[HB_A_US] <= row[HB_B_US] && row[HB_B_US] <= row[KILL_START_US] && row[KILL_START_US] < row[DEADLINE_US];
}
}  // namespace criterion9::wire
