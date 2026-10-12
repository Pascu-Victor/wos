// Actual finite Normal-kernel sibling IPC, backing-key futex and init-death test.
#include <abi/callnums/shm.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/futex.h>
#include <sys/init_control.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <abi/compartment.hpp>
#include <array>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
namespace wire = ker::abi::compartment;
constexpr char RESULTS[] = "/srv/compartment-shared-results";
constexpr char FIXTURE[] = "/usr/libexec/compartment-shared-runtime";
constexpr uint64_t MAGIC = 0x3430434F4D505348ULL, VERSION = 1, GUARD = 0xA596C387E1D24B6FULL;
constexpr uint64_t RUN_US = 60'000'000, STEP_US = 5'000'000, RETIRE_US = 10'000'000;
constexpr uint64_t SHM_ADDRESS = 0x0000200100000000ULL;
constexpr int KEY = 0x4039, CONTROL_FD = 9, BACKING_FD = 10;
constexpr unsigned ROUNDS = 16, WAIT_MAX = 32, WAKE_MAX = 128;
constexpr size_t PAGE = 4096, CONTROL_BYTES = 16384, PROC_BYTES = 4096;
constexpr off_t ROOT_COMMAND = 0, CHILD_COMMAND = 512, ROOT_PROGRESS = 1024, CHILD_PROGRESS = 1536;
constexpr off_t ROOT_REPORT = 4096, CHILD_REPORT = 8192;
constexpr off_t METADATA_REPORT = 10240;
constexpr uint64_t METADATA_MAGIC = 0x343253594D455441ULL;
constexpr int HISTORY_KEY = 0x403a;
constexpr size_t METADATA_HEADER = 48, STAT_WORDS = 20;
using Record = std::array<uint64_t, 64>;
using Row = std::array<uint64_t, 256>;
static_assert(sizeof(Row) == 2048 && sizeof(Record) == 512);
static_assert(std::endian::native == std::endian::little);
static_assert(sizeof(ker::abi::shm::ShmidDs) == 120 && sizeof(wire::Spec) == 408);
static_assert(7 * sizeof(Row) + 2 * CONTROL_BYTES + PAGE + 2 * PROC_BYTES + 4160 + 1024 + 64 == 64640);
static_assert(METADATA_REPORT + sizeof(Row) <= CONTROL_BYTES && METADATA_HEADER + 10 * STAT_WORDS == 248);

enum Word : size_t {
    W_MAGIC,
    W_VERSION,
    W_KIND,
    W_NONCE,
    W_ROLE,
    W_PID,
    W_PPID,
    W_UID,
    W_EUID,
    W_GID,
    W_EGID,
    W_STAGE,
    W_CHECKS,
    W_FAILURES,
    W_FIRST,
    W_FAILED_BITS,
    W_WAITS,
    W_WAKES,
    W_DEADLINES,
    W_CLOCKS,
    W_RW_WAITS,
    W_RW_WAKES,
    W_RW_ACKS,
    W_FOREIGN_WAKE_ZERO,
    W_RESCUE_WAKE_ONE,
    W_NEGATIVE_WAIT_ZERO,
    W_FILE_WAKE_ONE,
    W_FILE_WAIT_ZERO,
    W_FILE_ACKS,
    W_FOREIGN_ATTACH_BITS,
    W_FOREIGN_STAT_BITS,
    W_FOREIGN_RMID_BITS,
    W_STAT_GUARD_BITS,
    W_OWN_KEY,
    W_FILE_ZERO,
    W_READONLY_RIGHTS,
    W_MARKER,
    W_HIDDEN_PATH,
    W_SIGNAL_BITS,
    W_SHMID,
    W_SHM_VA,
    W_FILE_VA,
    W_KEY,
    W_FORK,
    W_PHYSICAL_ROOT,
    W_PHYSICAL_CHILD,
    W_HOST_PID,
    W_PROGRESS,
    W_SLEEP_US,
    W_RETURN,
    W_STAT_PRESERVED,
    W_PAYLOAD_LAST,
    W_LOCAL_WAIT,
    W_LOCAL_STATUS,
    W_LOCAL_WAIT_ZERO,
    W_LOCAL_WAIT_EINTR,
    W_REPORT_IO,
    W_START_US,
    W_END_US,
    W_COMMAND_SEQ,
    W_COMMAND_OP
};
constexpr size_t WAIT_BASE = 64, WAKE_BASE = 96, EXTRA = 224, COMMIT = 255;
enum class Failure : uint64_t {
    CLOCK = 1,
    DEADLINE,
    IDENTITY,
    IO,
    CONTROL,
    SHM,
    FOREIGN,
    FORK,
    FUTEX,
    PAYLOAD,
    MAPPING,
    PROC,
    SIGNAL,
    WAIT,
    RETIRE,
    PRESERVATION
};
enum class Stage : uint64_t { START, READY, PREFIX, GRACEFUL, COMPLETE, FAILED };
enum class Op : uint64_t {
    HELLO = 1,
    INFO,
    FOREIGN,
    NEGATIVE_WAIT,
    FOREIGN_WAKE,
    RESCUE,
    RW_WAIT,
    RW_WAKE,
    MAP_BACKING,
    FILE_WAIT,
    FILE_WAKE,
    PREFIX,
    LONG_SLEEP,
    POST_DEATH,
    STOP_CHILD,
    EXIT_ROOT,
    META_ROOT,
    META_CHILD_ATTACH,
    META_CHILD_DETACH,
    HISTORY_CREATE,
    HISTORY_ATTACH
};
enum class State : uint64_t { READY = 1, DONE = 2, FAILED = 3 };

auto raw_shm(uint64_t operation, uint64_t id, uint64_t arg, uint64_t flags) -> int64_t;
// Value-only ownership. Raw process exit does not run this destructor; private_fail explicitly cleans it.
struct MetadataLease {
    int64_t id = -1;
    uint64_t address{};
    bool remove_id{};
    MetadataLease() = default;
    MetadataLease(const MetadataLease&) = delete;
    auto operator=(const MetadataLease&) -> MetadataLease& = delete;
    MetadataLease(MetadataLease&& other) noexcept : id(other.id), address(other.address), remove_id(other.remove_id) {
        other.id = -1;
        other.address = 0;
        other.remove_id = false;
    }
    auto operator=(MetadataLease&& other) noexcept -> MetadataLease& {
        if (this != &other) {
            cleanup();
            id = other.id;
            address = other.address;
            remove_id = other.remove_id;
            other.id = -1;
            other.address = 0;
            other.remove_id = false;
        }
        return *this;
    }
    void cleanup() noexcept {
        if (address != 0) {
            (void)raw_shm(2, address, 0, 0);
            address = 0;
        }
        if (remove_id && id > 0) (void)raw_shm(3, static_cast<uint64_t>(id), 0, 0);
        id = -1;
        remove_id = false;
    }
    ~MetadataLease() { cleanup(); }
};
struct Runtime {
    Row metadata{};
    MetadataLease extra_mapping{}, history{};
    uint64_t metadata_phase{};
    Row row{};
    unsigned role{};
    bool child{};
    int fd = -1;
    uint64_t deadline{};
    uint64_t command_seq{};
    Op command_op{Op::HELLO};
};
struct Shared {
    alignas(8) int word{};
    int padding{};
    uint64_t payload{}, complement{}, acknowledgement{};
};
static_assert(offsetof(Shared, payload) == 8 && offsetof(Shared, acknowledgement) == 24);

auto signed_bits(int64_t value) -> uint64_t { return static_cast<uint64_t>(value); }
void expect(Runtime& runtime, bool condition, Failure failure) {
    ++runtime.row[W_CHECKS];
    if (condition) return;
    ++runtime.row[W_FAILURES];
    auto const code = static_cast<uint64_t>(failure);
    if (runtime.row[W_FIRST] == 0) runtime.row[W_FIRST] = code;
    runtime.row[W_FAILED_BITS] |= uint64_t{1} << (code - 1);
}
auto clock_us() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    auto const seconds = static_cast<uint64_t>(value.tv_sec);
    if (seconds > (UINT64_MAX - 999'999) / 1'000'000) return UINT64_MAX;
    return seconds * 1'000'000 + static_cast<uint64_t>(value.tv_nsec) / 1000;
}
auto now(Runtime& runtime) -> uint64_t {
    auto const value = clock_us();
    if (value == UINT64_MAX || value < runtime.row[W_START_US]) {
        ++runtime.row[W_CLOCKS];
        expect(runtime, false, Failure::CLOCK);
        return UINT64_MAX;
    }
    return value;
}
auto alive(Runtime& runtime, uint64_t deadline) -> bool {
    auto const value = now(runtime);
    if (value == UINT64_MAX) return false;
    if (value >= deadline || value >= runtime.deadline) {
        ++runtime.row[W_DEADLINES];
        expect(runtime, false, Failure::DEADLINE);
        return false;
    }
    return true;
}
auto step_deadline(Runtime& runtime, uint64_t duration = STEP_US) -> uint64_t {
    auto const value = now(runtime);
    if (value == UINT64_MAX || value > UINT64_MAX - duration) return 0;
    auto const requested = value + duration;
    return requested < runtime.deadline ? requested : runtime.deadline;
}
void pause_us(uint64_t duration = 1000) {
    timespec value{static_cast<time_t>(duration / 1'000'000), static_cast<long>((duration % 1'000'000) * 1000)};
    (void)nanosleep(&value, nullptr);
}
auto exact_io(int fd, void* data, size_t bytes, off_t offset, bool writing, uint64_t deadline) -> bool {
    auto* cursor = static_cast<uint8_t*>(data);
    size_t done{};
    while (done < bytes) {
        auto const time = clock_us();
        if (time == UINT64_MAX || time >= deadline) return false;
        ssize_t const count = writing ? pwrite(fd, cursor + done, bytes - done, offset + static_cast<off_t>(done))
                                      : pread(fd, cursor + done, bytes - done, offset + static_cast<off_t>(done));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0 || static_cast<size_t>(count) > bytes - done) return false;
        done += static_cast<size_t>(count);
    }
    return true;
}
auto put_record(int fd, off_t offset, Record record, uint64_t deadline) -> bool {
    uint64_t const sequence = record[4];
    record[63] = 0;
    if (sequence == 0 || !exact_io(fd, record.data(), sizeof(record), offset, true, deadline)) return false;
    auto commit = sequence;
    return exact_io(fd, &commit, sizeof(commit), offset + 504, true, deadline);
}
auto get_record(int fd, off_t offset, uint64_t nonce, unsigned role, Record& output, uint64_t deadline) -> bool {
    Record first{}, second{};
    if (!exact_io(fd, first.data(), sizeof(first), offset, false, deadline) ||
        !exact_io(fd, second.data(), sizeof(second), offset, false, deadline))
        return false;
    if (first != second || first[0] != MAGIC || first[1] != VERSION || first[2] != nonce || first[3] != role || first[4] == 0 ||
        first[63] != first[4])
        return false;
    output = first;
    return true;
}
auto progress(Runtime& runtime, State state, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0) -> bool {
    Record record{};
    record[0] = MAGIC;
    record[1] = VERSION;
    record[2] = runtime.row[W_NONCE];
    record[3] = runtime.role;
    record[4] = runtime.command_seq == 0 ? 1 : runtime.command_seq;
    record[5] = static_cast<uint64_t>(runtime.command_op);
    record[6] = static_cast<uint64_t>(state);
    record[7] = a;
    record[8] = b;
    record[9] = c;
    record[10] = runtime.row[W_FAILURES];
    record[11] = runtime.row[W_PID];
    record[12] = runtime.row[W_PPID];
    record[13] = runtime.row[W_UID];
    bool const saved = put_record(runtime.fd, runtime.child ? CHILD_PROGRESS : ROOT_PROGRESS, record, runtime.deadline);
    if (!saved) {
        ++runtime.row[W_REPORT_IO];
        expect(runtime, false, Failure::IO);
    }
    return saved;
}
auto publish_row(Runtime& runtime) -> bool {
    runtime.row[W_END_US] = now(runtime);
    runtime.row[W_COMMAND_SEQ] = runtime.command_seq;
    runtime.row[W_COMMAND_OP] = static_cast<uint64_t>(runtime.command_op);
    runtime.row[COMMIT] = 0;
    auto const offset = runtime.child ? CHILD_REPORT : ROOT_REPORT;
    bool saved = exact_io(runtime.fd, runtime.row.data(), sizeof(runtime.row), offset, true, runtime.deadline);
    auto commit = runtime.row[W_NONCE];
    saved = saved && exact_io(runtime.fd, &commit, sizeof(commit), offset + static_cast<off_t>(COMMIT * 8), true, runtime.deadline);
    saved = saved && fsync(runtime.fd) == 0;
    runtime.row[COMMIT] = commit;
    if (!saved) {
        ++runtime.row[W_REPORT_IO];
        expect(runtime, false, Failure::IO);
    }
    return saved;
}
[[noreturn]] void private_exit(uint64_t code) {
    ker::process::exit(code);
    __builtin_unreachable();
}
[[noreturn]] void private_fail(Runtime& runtime) {
    runtime.extra_mapping.cleanup();
    runtime.history.cleanup();
    if (runtime.row[W_FAILURES] == 0) expect(runtime, false, Failure::CONTROL);
    runtime.row[W_STAGE] = static_cast<uint64_t>(Stage::FAILED);
    runtime.row[W_RETURN] = 70;
    (void)publish_row(runtime);
    (void)progress(runtime, State::FAILED);
    private_exit(70);
}
auto make_runtime(unsigned role, bool child, uint64_t nonce, uint64_t deadline, uint64_t host_pid) -> Runtime {
    Runtime runtime{};
    runtime.role = role;
    runtime.child = child;
    runtime.fd = CONTROL_FD;
    runtime.deadline = deadline;
    runtime.row[W_MAGIC] = MAGIC;
    runtime.row[W_VERSION] = VERSION;
    runtime.row[W_KIND] = child ? 2 : 1;
    runtime.row[W_NONCE] = nonce;
    runtime.row[W_ROLE] = role;
    runtime.row[W_PID] = ker::process::getpid();
    runtime.row[W_PPID] = ker::process::getppid();
    runtime.row[W_UID] = getuid();
    runtime.row[W_EUID] = geteuid();
    runtime.row[W_GID] = getgid();
    runtime.row[W_EGID] = getegid();
    runtime.row[W_START_US] = clock_us();
    runtime.row[W_RETURN] = UINT64_MAX;
    runtime.row[W_HOST_PID] = host_pid;
    runtime.row[W_KEY] = KEY;
    expect(runtime,
           runtime.row[W_PID] == (child ? 2U : 1U) && runtime.row[W_PPID] == (child ? 1U : 0U) && runtime.row[W_UID] == 100 &&
               runtime.row[W_EUID] == 100 && runtime.row[W_GID] == 100 && runtime.row[W_EGID] == 100,
           Failure::IDENTITY);
    return runtime;
}
auto command(Runtime& runtime, Record& output) -> bool {
    while (alive(runtime, runtime.deadline)) {
        if (get_record(runtime.fd, runtime.child ? CHILD_COMMAND : ROOT_COMMAND, runtime.row[W_NONCE], runtime.role, output,
                       runtime.deadline)) {
            if (output[4] <= runtime.command_seq) {
                pause_us();
                continue;
            }
            if (output[4] != runtime.command_seq + 1) {
                expect(runtime, false, Failure::CONTROL);
                return false;
            }
            runtime.command_seq = output[4];
            runtime.command_op = static_cast<Op>(output[5]);
            return true;
        }
        pause_us();
    }
    return false;
}
auto wait_word(Runtime& runtime, int* word) -> int64_t {
    if (runtime.row[W_WAITS] >= WAIT_MAX || !alive(runtime, runtime.deadline)) {
        expect(runtime, false, Failure::FUTEX);
        return -EOVERFLOW;
    }
    timespec timeout{5, 0};
    auto const result = ker::futex::wait(word, 0, &timeout);
    runtime.row[WAIT_BASE + runtime.row[W_WAITS]++] = signed_bits(result);
    expect(runtime, result == 0, Failure::FUTEX);
    return result;
}
auto wake_word(Runtime& runtime, int* word, bool require_one) -> bool {
    auto const deadline = step_deadline(runtime);
    do {
        if (runtime.row[W_WAKES] >= WAKE_MAX || !alive(runtime, deadline)) {
            expect(runtime, false, Failure::FUTEX);
            return false;
        }
        auto const result = ker::futex::wake(word, 1);
        runtime.row[WAKE_BASE + runtime.row[W_WAKES]++] = signed_bits(result);
        if (!require_one) {
            expect(runtime, result == 0, Failure::FUTEX);
            return result == 0;
        }
        if (result == 1) return true;
        if (result != 0) {
            expect(runtime, false, Failure::FUTEX);
            return false;
        }
        pause_us();
    } while (true);
}
auto raw_shm(uint64_t operation, uint64_t id, uint64_t arg, uint64_t flags = 0) -> int64_t {
    return static_cast<int64_t>(syscall(ker::abi::callnums::shm, operation, id, arg, flags));
}

struct MetadataGuardedStat {
    uint64_t before;
    ker::abi::shm::ShmidDs value;
    uint64_t after;
};
static_assert(sizeof(MetadataGuardedStat) == 136 && offsetof(MetadataGuardedStat, value) == 8 &&
              offsetof(MetadataGuardedStat, after) == 128);
auto metadata_guard(uint64_t nonce, unsigned role, uint64_t event) -> uint64_t {
    return GUARD ^ nonce ^ (static_cast<uint64_t>(role) << 32U) ^ event;
}
auto history_marker(uint64_t nonce) -> uint64_t {
    auto const value = nonce ^ 0x8000000000000000ULL;
    return value == 0 ? 1 : value;
}
auto metadata_address(uint64_t address) -> bool { return address != 0 && address < 0x0000800000000000ULL && address % PAGE == 0; }
void metadata_expect(Runtime& runtime, bool condition) {
    ++runtime.metadata[44];
    if (!condition) ++runtime.metadata[45];
    expect(runtime, condition, Failure::SHM);
}
void metadata_expected(ker::abi::shm::ShmidDs& expected, int id, int key, int64_t creator, int64_t actor, uint64_t attachments) {
    std::memset(&expected, 0, sizeof(expected));
    expected.shm_perm.key = key;
    expected.shm_perm.uid = expected.shm_perm.gid = expected.shm_perm.cuid = expected.shm_perm.cgid = 100;
    expected.shm_perm.mode = 0600;
    expected.shm_perm.seq = id;
    expected.shm_segsz = PAGE;
    expected.shm_cpid = creator;
    expected.shm_lpid = actor;
    expected.shm_nattch = attachments;
}
auto metadata_stat(Runtime& runtime, int id, int key, int64_t creator, int64_t actor, uint64_t attachments, uint64_t event, unsigned role,
                   bool removed = false) -> std::array<uint64_t, STAT_WORDS> {
    MetadataGuardedStat buffer{};
    std::memset(&buffer, 0xA5, sizeof(buffer));
    buffer.before = metadata_guard(runtime.row[W_NONCE], role, event);
    buffer.after = ~buffer.before;
    MetadataGuardedStat original{};
    std::memcpy(&original, &buffer, sizeof(buffer));
    auto const result = raw_shm(3, static_cast<uint64_t>(id), 2, reinterpret_cast<uint64_t>(&buffer.value));
    auto const timestamp = now(runtime);
    ker::abi::shm::ShmidDs expected{};
    metadata_expected(expected, id, key, creator, actor, attachments);
    metadata_expect(runtime, alive(runtime, runtime.deadline) && timestamp != UINT64_MAX && timestamp >= runtime.row[W_START_US] &&
                                 timestamp < runtime.deadline && buffer.before == original.before && buffer.after == original.after &&
                                 (removed ? result == -EINVAL && std::memcmp(&buffer, &original, sizeof(buffer)) == 0
                                          : result == 0 && std::memcmp(&buffer.value, &expected, sizeof(expected)) == 0));
    std::array<uint64_t, STAT_WORDS> slot{};
    slot[0] = event;
    slot[1] = signed_bits(result);
    slot[2] = timestamp;
    slot[3] = buffer.before;
    std::memcpy(slot.data() + 4, &buffer.value, sizeof(buffer.value));
    slot[19] = buffer.after;
    return slot;
}
void metadata_initialize(Runtime& runtime, int own_id) {
    auto& row = runtime.metadata;
    row[0] = METADATA_MAGIC;
    row[1] = 1;
    row[2] = 3;
    row[3] = runtime.row[W_NONCE];
    row[4] = runtime.role;
    row[5] = 1;
    row[6] = 2;
    row[7] = 100;
    row[8] = 100;
    row[9] = runtime.row[W_PHYSICAL_ROOT];
    row[10] = runtime.row[W_PHYSICAL_CHILD];
    row[11] = runtime.row[W_HOST_PID];
    row[12] = static_cast<uint64_t>(own_id);
    row[13] = runtime.row[W_SHM_VA];
}
void metadata_append(Runtime& runtime, int id, int key, int64_t creator, int64_t actor, uint64_t attachments, uint64_t event,
                     bool removed = false) {
    if (event != runtime.metadata[43] + 1 || event == 0 || event > 10) private_fail(runtime);
    auto const slot = metadata_stat(runtime, id, key, creator, actor, attachments, event, runtime.role, removed);
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
    std::memcpy(runtime.metadata.data() + METADATA_HEADER + (event - 1) * STAT_WORDS, slot.data(), sizeof(slot));
    runtime.metadata[42] |= uint64_t{1} << (event - 1);
    runtime.metadata[43] = event;
    if (event == 1) runtime.metadata[46] = slot[2];
    runtime.metadata[47] = slot[2];
}
auto metadata_publish(Runtime& runtime) -> bool {
    runtime.metadata[COMMIT] = 0;
    bool saved = exact_io(runtime.fd, runtime.metadata.data(), sizeof(Row), METADATA_REPORT, true, runtime.deadline);
    auto commit = runtime.row[W_NONCE];
    saved = saved && exact_io(runtime.fd, &commit, sizeof(commit), METADATA_REPORT + COMMIT * 8, true, runtime.deadline);
    saved = saved && fsync(runtime.fd) == 0;
    runtime.metadata[COMMIT] = commit;
    metadata_expect(runtime, saved);
    return saved;
}
auto metadata_child_record(Runtime& runtime, Op operation, uint64_t sequence, Record& record) -> bool {
    bool valid = get_record(runtime.fd, CHILD_PROGRESS, runtime.row[W_NONCE], runtime.role, record, runtime.deadline) &&
                 record[4] == sequence && record[5] == static_cast<uint64_t>(operation) &&
                 record[6] == static_cast<uint64_t>(State::DONE) && record[10] == 0 && record[11] == 2 && record[12] == 1 &&
                 record[13] == 100;
    for (size_t index = 14; index < 63; ++index) valid = valid && record[index] == 0;
    metadata_expect(runtime, valid);
    return valid;
}
void metadata_root_phase(Runtime& runtime, int own_id, uint64_t phase, uint64_t value, uint64_t child_sequence) {
    if (runtime.child || phase > 3 || phase != runtime.metadata_phase || runtime.command_seq != phase + 2) private_fail(runtime);
    if (phase == 0) {
        if (value != 0 || child_sequence != 0) private_fail(runtime);
        metadata_initialize(runtime, own_id);
        metadata_append(runtime, own_id, KEY, 1, 1, 2, 1);
    } else if (phase == 1) {
        Record child{};
        if (child_sequence != 2 || !metadata_child_record(runtime, Op::META_CHILD_ATTACH, child_sequence, child) || child[7] != value ||
            child[8] != static_cast<uint64_t>(own_id) || child[9] != child_sequence || !metadata_address(value) ||
            value == runtime.row[W_SHM_VA])
            private_fail(runtime);
        runtime.metadata[16] = value;
        runtime.metadata[17] = child[7];
        runtime.metadata[18] = child_sequence;
        metadata_append(runtime, own_id, KEY, 1, 2, 3, 2);
    } else if (phase == 2) {
        Record child{};
        if (value != 0 || child_sequence != 3 || !metadata_child_record(runtime, Op::META_CHILD_DETACH, child_sequence, child) ||
            child[7] != 0 || child[8] != runtime.metadata[16] || child[9] != child_sequence)
            private_fail(runtime);
        runtime.metadata[19] = child[7];
        runtime.metadata[20] = child_sequence;
        metadata_append(runtime, own_id, KEY, 1, 2, 2, 3);
    } else {
        if (value != 0 || child_sequence != 0 || runtime.extra_mapping.address != 0) private_fail(runtime);
        auto const attached = raw_shm(1, static_cast<uint64_t>(own_id), 0, 0);
        if (attached > 0) runtime.extra_mapping.address = static_cast<uint64_t>(attached);
        metadata_expect(runtime, attached > 0 && metadata_address(static_cast<uint64_t>(attached)) &&
                                     static_cast<uint64_t>(attached) != runtime.row[W_SHM_VA]);
        if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
        runtime.metadata[21] = static_cast<uint64_t>(attached);
        runtime.metadata[22] = signed_bits(attached);
        metadata_append(runtime, own_id, KEY, 1, 1, 3, 4);
        auto const detached = raw_shm(2, runtime.extra_mapping.address, 0, 0);
        runtime.metadata[23] = signed_bits(detached);
        metadata_expect(runtime, detached == 0);
        if (detached == 0) runtime.extra_mapping.address = 0;
        if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
        metadata_append(runtime, own_id, KEY, 1, 1, 2, 5);
    }
    runtime.metadata[24 + phase] = runtime.command_seq;
    ++runtime.metadata_phase;
    if (!metadata_publish(runtime)) private_fail(runtime);
}
void metadata_check_marker(Runtime& runtime) {
    if (!metadata_address(runtime.history.address)) private_fail(runtime);
    auto const* words = reinterpret_cast<const uint64_t*>(runtime.history.address);
    auto const marker = history_marker(runtime.row[W_NONCE]);
    metadata_expect(runtime, marker != 0 && marker != runtime.row[W_NONCE] && __atomic_load_n(words, __ATOMIC_ACQUIRE) == marker &&
                                 __atomic_load_n(words + 1, __ATOMIC_ACQUIRE) == ~marker);
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
}
void metadata_history_attach(Runtime& runtime, uint64_t id, uint64_t child_va, uint64_t sequence) {
    if (runtime.child || runtime.role != 2 || runtime.command_seq != 60 || runtime.metadata[43] != 5 || sequence != 23 || id == 0 ||
        id > INT32_MAX || !metadata_address(child_va))
        private_fail(runtime);
    Record child{};
    if (!metadata_child_record(runtime, Op::HISTORY_CREATE, sequence, child) || child[7] != id || child[8] != child_va ||
        child[9] != history_marker(runtime.row[W_NONCE]))
        private_fail(runtime);
    auto const found = raw_shm(0, HISTORY_KEY, PAGE, 0600);
    metadata_expect(runtime, found == static_cast<int64_t>(id));
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
    runtime.history.id = found;
    runtime.history.remove_id = true;
    runtime.metadata[14] = id;
    runtime.metadata[15] = HISTORY_KEY;
    runtime.metadata[28] = child[7];
    runtime.metadata[29] = child[8];
    runtime.metadata[30] = child_va;
    runtime.metadata[31] = sequence;
    runtime.metadata[32] = runtime.command_seq;
    metadata_append(runtime, static_cast<int>(id), HISTORY_KEY, 2, 2, 1, 6);
    auto const attached = raw_shm(1, id, 0, 0);
    if (attached > 0) runtime.history.address = static_cast<uint64_t>(attached);
    metadata_expect(runtime, attached > 0 && metadata_address(static_cast<uint64_t>(attached)) &&
                                 static_cast<uint64_t>(attached) != runtime.row[W_SHM_VA]);
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
    runtime.metadata[33] = signed_bits(attached);
    runtime.metadata[34] = static_cast<uint64_t>(attached);
    runtime.metadata[35] = child[9];
    runtime.metadata[36] = ~child[9];
    metadata_check_marker(runtime);
    metadata_append(runtime, static_cast<int>(id), HISTORY_KEY, 2, 1, 2, 7);
    if (!metadata_publish(runtime)) private_fail(runtime);
}
void metadata_history_after_wait(Runtime& runtime, int64_t waited, int status) {
    if (waited != 2 || status != 0 || runtime.row[W_FAILURES] != 0 || runtime.command_seq != 61 || runtime.metadata[43] != 7 ||
        runtime.history.id <= 0 || !runtime.history.remove_id)
        private_fail(runtime);
    metadata_check_marker(runtime);
    auto const id = static_cast<int>(runtime.history.id);
    runtime.metadata[37] = signed_bits(waited);
    runtime.metadata[38] = signed_bits(status);
    metadata_append(runtime, id, HISTORY_KEY, 2, 1, 1, 8);
    auto const detached = raw_shm(2, runtime.history.address, 0, 0);
    runtime.metadata[39] = signed_bits(detached);
    metadata_expect(runtime, detached == 0);
    if (detached == 0) runtime.history.address = 0;
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
    metadata_append(runtime, id, HISTORY_KEY, 2, 1, 0, 9);
    auto const removed = raw_shm(3, static_cast<uint64_t>(id), 0, 0);
    runtime.metadata[40] = signed_bits(removed);
    metadata_expect(runtime, removed == 0);
    if (removed == 0) {
        runtime.history.id = -1;
        runtime.history.remove_id = false;
    }
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
    metadata_append(runtime, id, HISTORY_KEY, 0, 0, 0, 10, true);
    runtime.metadata[41] = runtime.command_seq;
    if (!metadata_publish(runtime)) private_fail(runtime);
}

auto own_stat(Runtime& runtime, int id, bool record_metadata = true) -> bool {
    ker::abi::shm::ShmidDs status{};
    auto const result = raw_shm(3, static_cast<uint64_t>(id), 2, reinterpret_cast<uint64_t>(&status));
    expect(runtime,
           result == 0 && status.shm_perm.key == KEY && status.shm_segsz == PAGE && status.shm_perm.uid == 100 &&
               status.shm_perm.gid == 100 && (status.shm_perm.mode & 0777) == 0600,
           Failure::SHM);
    // Record the caller-visible creator/operator names; physical task identities remain separate.
    if (record_metadata) {
        runtime.row[EXTRA + 17] = signed_bits(status.shm_cpid);
        runtime.row[EXTRA + 18] = signed_bits(status.shm_lpid);
        runtime.row[EXTRA + 22] = signed_bits(result);
    }
    return result == 0 && runtime.row[W_FAILURES] == 0;
}
auto marker_ok(unsigned role) -> bool {
    int const fd = open("/same/name", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    char bytes[16]{};
    ssize_t const count = read(fd, bytes, sizeof(bytes));
    bool const closed = close(fd) == 0;
    char const* expected = role == 1 ? "tree-A\n" : "tree-B\n";
    return closed && count == 7 && std::memcmp(bytes, expected, 7) == 0;
}
void check_foreign(Runtime& runtime, int own_id, int host_id, int sibling_id, uint64_t sibling_pid) {
    runtime.row[EXTRA] = static_cast<uint64_t>(host_id);
    runtime.row[EXTRA + 1] = static_cast<uint64_t>(sibling_id);
    std::array ids{host_id, sibling_id};
    for (unsigned index = 0; index < ids.size(); ++index) {
        uint64_t const bit = uint64_t{1} << index;
        auto const attach = raw_shm(1, static_cast<uint64_t>(ids[index]), 0, 0);
        runtime.row[EXTRA + 2 + index] = signed_bits(attach);
        expect(runtime, attach == -EINVAL, Failure::FOREIGN);
        if (attach == -EINVAL) runtime.row[W_FOREIGN_ATTACH_BITS] |= bit;
        if (attach > 0) (void)shmdt(reinterpret_cast<void*>(static_cast<uint64_t>(attach)));
        struct Guarded {
            uint64_t before;
            ker::abi::shm::ShmidDs value;
            uint64_t after;
        } buffer{}, original{};
        std::memset(&buffer, 0x5A, sizeof(buffer));
        std::memcpy(&original, &buffer, sizeof(buffer));
        auto const stat = raw_shm(3, static_cast<uint64_t>(ids[index]), 2, reinterpret_cast<uint64_t>(&buffer.value));
        runtime.row[EXTRA + 4 + index] = signed_bits(stat);
        bool const untouched = std::memcmp(&buffer, &original, sizeof(buffer)) == 0;
        expect(runtime, stat == -EINVAL && untouched, Failure::FOREIGN);
        if (stat == -EINVAL) runtime.row[W_FOREIGN_STAT_BITS] |= bit;
        if (untouched) runtime.row[W_STAT_GUARD_BITS] |= bit;
        auto const remove = raw_shm(3, static_cast<uint64_t>(ids[index]), 0, 0);
        runtime.row[EXTRA + 6 + index] = signed_bits(remove);
        expect(runtime, remove == -EINVAL, Failure::FOREIGN);
        if (remove == -EINVAL) runtime.row[W_FOREIGN_RMID_BITS] |= bit;
    }
    auto const host_signal = ker::process::kill(static_cast<int64_t>(runtime.row[W_HOST_PID]), 0);
    auto const sibling_signal = ker::process::kill(static_cast<int64_t>(sibling_pid), 0);
    runtime.row[EXTRA + 8] = signed_bits(host_signal);
    runtime.row[EXTRA + 9] = signed_bits(sibling_signal);
    expect(runtime, host_signal == -ESRCH && sibling_signal == -ESRCH, Failure::SIGNAL);
    if (host_signal == -ESRCH) runtime.row[W_SIGNAL_BITS] |= 1;
    if (sibling_signal == -ESRCH) runtime.row[W_SIGNAL_BITS] |= 2;
    int const lookup = shmget(KEY, PAGE, 0600);
    runtime.row[EXTRA + 20] = signed_bits(lookup);
    expect(runtime, lookup == own_id && own_stat(runtime, own_id), Failure::SHM);
    if (lookup == own_id) runtime.row[W_OWN_KEY] = 1;
    void* const extra = shmat(own_id, nullptr, 0);
    runtime.row[EXTRA + 21] = reinterpret_cast<uint64_t>(extra);
    bool const attached = extra != reinterpret_cast<void*>(-1);
    expect(runtime, attached && __atomic_load_n(&static_cast<Shared*>(extra)->word, __ATOMIC_ACQUIRE) == 0, Failure::SHM);
    if (attached) expect(runtime, shmdt(extra) == 0, Failure::SHM);
    runtime.row[W_STAT_PRESERVED] = runtime.row[W_FAILURES] == 0 ? 1 : 0;
}

[[noreturn]] void service_loop(Runtime& runtime, int own_id, Shared* shared, int64_t local_child) {
    void* file_mapping = nullptr;
    runtime.row[W_STAGE] = static_cast<uint64_t>(Stage::READY);
    if (!progress(runtime, State::READY, static_cast<uint64_t>(own_id), reinterpret_cast<uint64_t>(shared))) private_fail(runtime);
    Record request{};
    while (command(runtime, request)) {
        auto const a = request[7], b = request[8], c = request[9];
        switch (runtime.command_op) {
            case Op::INFO:
                runtime.row[W_PHYSICAL_ROOT] = a;
                runtime.row[W_PHYSICAL_CHILD] = b;
                expect(runtime, a > 2 && b > 2 && a != b, Failure::IDENTITY);
                break;
            case Op::META_ROOT:
                metadata_root_phase(runtime, own_id, a, b, c);
                break;
            case Op::META_CHILD_ATTACH: {
                if (!runtime.child || runtime.command_seq != 2 || a != 0 || b != 0 || c != 0 || runtime.extra_mapping.address != 0)
                    private_fail(runtime);
                auto const attached = raw_shm(1, static_cast<uint64_t>(own_id), 0, 0);
                if (attached > 0) runtime.extra_mapping.address = static_cast<uint64_t>(attached);
                metadata_expect(runtime, attached > 0 && metadata_address(static_cast<uint64_t>(attached)) &&
                                             static_cast<uint64_t>(attached) != runtime.row[W_SHM_VA]);
                if (runtime.row[W_FAILURES] != 0 ||
                    !progress(runtime, State::DONE, signed_bits(attached), static_cast<uint64_t>(own_id), runtime.command_seq))
                    private_fail(runtime);
                continue;
            }
            case Op::META_CHILD_DETACH: {
                if (!runtime.child || runtime.command_seq != 3 || a != 0 || b != 0 || c != 0 || runtime.extra_mapping.address == 0)
                    private_fail(runtime);
                auto const address = runtime.extra_mapping.address;
                auto const detached = raw_shm(2, address, 0, 0);
                metadata_expect(runtime, detached == 0);
                if (detached == 0) runtime.extra_mapping.address = 0;
                if (runtime.row[W_FAILURES] != 0 || !progress(runtime, State::DONE, signed_bits(detached), address, runtime.command_seq))
                    private_fail(runtime);
                continue;
            }
            case Op::HISTORY_CREATE: {
                if (!runtime.child || runtime.role != 2 || runtime.command_seq != 23 || a != 0 || b != 0 || c != 0 ||
                    runtime.history.id > 0 || runtime.history.address != 0)
                    private_fail(runtime);
                auto const id = raw_shm(0, HISTORY_KEY, PAGE, IPC_CREAT | IPC_EXCL | 0600);
                if (id > 0) {
                    runtime.history.id = id;
                    runtime.history.remove_id = true;
                }
                metadata_expect(runtime, id > 0 && id <= INT32_MAX && id != own_id);
                if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
                auto const attached = raw_shm(1, static_cast<uint64_t>(id), 0, 0);
                if (attached > 0) runtime.history.address = static_cast<uint64_t>(attached);
                metadata_expect(runtime, attached > 0 && metadata_address(static_cast<uint64_t>(attached)) &&
                                             static_cast<uint64_t>(attached) != runtime.row[W_SHM_VA]);
                if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
                auto* words = reinterpret_cast<uint64_t*>(runtime.history.address);
                auto const marker = history_marker(runtime.row[W_NONCE]);
                __atomic_store_n(words + 1, ~marker, __ATOMIC_RELEASE);
                __atomic_store_n(words, marker, __ATOMIC_RELEASE);
                metadata_check_marker(runtime);
                if (!progress(runtime, State::DONE, signed_bits(id), signed_bits(attached), marker)) private_fail(runtime);
                continue;
            }
            case Op::HISTORY_ATTACH:
                metadata_history_attach(runtime, a, b, c);
                break;
            case Op::FOREIGN:
                if (runtime.child) private_fail(runtime);
                check_foreign(runtime, own_id, static_cast<int>(a), static_cast<int>(b), c);
                break;
            case Op::NEGATIVE_WAIT:
                if (runtime.child || !progress(runtime, State::READY)) private_fail(runtime);
                if (wait_word(runtime, &shared->word) == 0) ++runtime.row[W_NEGATIVE_WAIT_ZERO];
                break;
            case Op::FOREIGN_WAKE:
                if (runtime.child) private_fail(runtime);
                if (wake_word(runtime, &shared->word, false)) ++runtime.row[W_FOREIGN_WAKE_ZERO];
                break;
            case Op::RESCUE:
                if (!runtime.child) private_fail(runtime);
                if (wake_word(runtime, &shared->word, true)) ++runtime.row[W_RESCUE_WAKE_ONE];
                break;
            case Op::RW_WAIT:
                if (!runtime.child || a == 0 || a > ROUNDS || !progress(runtime, State::READY, a)) private_fail(runtime);
                if (wait_word(runtime, &shared->word) == 0) {
                    ++runtime.row[W_RW_WAITS];
                    auto const payload = __atomic_load_n(&shared->payload, __ATOMIC_ACQUIRE);
                    auto const complement = __atomic_load_n(&shared->complement, __ATOMIC_ACQUIRE);
                    expect(runtime, payload == a && complement == ~a, Failure::PAYLOAD);
                    __atomic_store_n(&shared->acknowledgement, a, __ATOMIC_RELEASE);
                    ++runtime.row[W_RW_ACKS];
                    runtime.row[W_PAYLOAD_LAST] = payload;
                }
                break;
            case Op::RW_WAKE: {
                if (runtime.child || a == 0 || a > ROUNDS) private_fail(runtime);
                __atomic_store_n(&shared->complement, ~a, __ATOMIC_RELEASE);
                __atomic_store_n(&shared->payload, a, __ATOMIC_RELEASE);
                if (wake_word(runtime, &shared->word, true)) ++runtime.row[W_RW_WAKES];
                auto const deadline = step_deadline(runtime);
                while (__atomic_load_n(&shared->acknowledgement, __ATOMIC_ACQUIRE) != a && alive(runtime, deadline)) pause_us();
                expect(runtime, __atomic_load_n(&shared->acknowledgement, __ATOMIC_ACQUIRE) == a, Failure::PAYLOAD);
                ++runtime.row[W_RW_ACKS];
                runtime.row[W_PAYLOAD_LAST] = a;
                break;
            }
            case Op::MAP_BACKING: {
                if (runtime.child || file_mapping != nullptr) private_fail(runtime);
                auto const hint = runtime.role == 1 ? 0x0000200200000000ULL : 0x0000200300000000ULL;
                file_mapping = mmap(reinterpret_cast<void*>(hint), PAGE, PROT_READ, MAP_SHARED, BACKING_FD, 0);
                expect(runtime, file_mapping != MAP_FAILED && file_mapping != nullptr, Failure::MAPPING);
                if (file_mapping == MAP_FAILED || file_mapping == nullptr) private_fail(runtime);
                runtime.row[W_FILE_VA] = reinterpret_cast<uint64_t>(file_mapping);
                auto const* word = static_cast<const int*>(file_mapping);
                expect(runtime, __atomic_load_n(word, __ATOMIC_ACQUIRE) == 0, Failure::MAPPING);
                runtime.row[W_FILE_ZERO] = 1;
                break;
            }
            case Op::FILE_WAIT:
                if (runtime.child || file_mapping == nullptr || a == 0 || a > ROUNDS || !progress(runtime, State::READY, a))
                    private_fail(runtime);
                // Address-only cast for the raw ABI. The immutable mapping is never written.
                if (wait_word(runtime, const_cast<int*>(static_cast<const int*>(file_mapping))) == 0) {
                    ++runtime.row[W_FILE_WAIT_ZERO];
                    ++runtime.row[W_FILE_ACKS];
                    expect(runtime, __atomic_load_n(static_cast<const int*>(file_mapping), __ATOMIC_ACQUIRE) == 0, Failure::MAPPING);
                }
                break;
            case Op::FILE_WAKE:
                if (runtime.child || file_mapping == nullptr || a == 0 || a > ROUNDS) private_fail(runtime);
                if (wake_word(runtime, const_cast<int*>(static_cast<const int*>(file_mapping)), true)) ++runtime.row[W_FILE_WAKE_ONE];
                break;
            case Op::PREFIX:
                runtime.row[W_STAGE] = static_cast<uint64_t>(Stage::PREFIX);
                if (!publish_row(runtime)) private_fail(runtime);
                break;
            case Op::LONG_SLEEP: {
                if (!runtime.child || runtime.role != 1) private_fail(runtime);
                runtime.row[W_SLEEP_US] = now(runtime);
                if (!progress(runtime, State::READY, runtime.row[W_SLEEP_US])) private_fail(runtime);
                timespec duration{30, 0};
                int const result = nanosleep(&duration, nullptr);
                // Success requires init retirement to kill us before this call returns.
                runtime.row[EXTRA + 23] = signed_bits(result);
                runtime.row[EXTRA + 24] = static_cast<uint64_t>(errno);
                expect(runtime, false, Failure::RETIRE);
                private_fail(runtime);
            }
            case Op::POST_DEATH:
                expect(runtime,
                       runtime.role == 2 && __atomic_load_n(&shared->word, __ATOMIC_ACQUIRE) == 0 &&
                           __atomic_load_n(&shared->payload, __ATOMIC_ACQUIRE) == ROUNDS &&
                           __atomic_load_n(&shared->complement, __ATOMIC_ACQUIRE) == ~uint64_t{ROUNDS} &&
                           __atomic_load_n(&shared->acknowledgement, __ATOMIC_ACQUIRE) == ROUNDS,
                       Failure::PRESERVATION);
                if (!runtime.child) expect(runtime, marker_ok(runtime.role) && own_stat(runtime, own_id), Failure::PRESERVATION);
                ++runtime.row[W_PROGRESS];
                break;
            case Op::STOP_CHILD:
                if (!runtime.child || runtime.role != 2) private_fail(runtime);
                expect(runtime, shmdt(shared) == 0, Failure::SHM);
                runtime.row[W_STAGE] = static_cast<uint64_t>(Stage::GRACEFUL);
                runtime.row[W_RETURN] = 0;
                if (runtime.row[W_FAILURES] != 0 || !publish_row(runtime) || !progress(runtime, State::DONE)) private_fail(runtime);
                private_exit(0);
            case Op::EXIT_ROOT: {
                if (runtime.child || runtime.role != 2 || local_child != 2) private_fail(runtime);
                auto const deadline = step_deadline(runtime);
                int status = -1;
                int64_t waited = 0;
                do {
                    waited = ker::process::waitpid(local_child, &status, WNOHANG, nullptr);
                    if (waited == 0) ++runtime.row[W_LOCAL_WAIT_ZERO];
                    if (waited == -EINTR) ++runtime.row[W_LOCAL_WAIT_EINTR];
                    if (waited != 0 && waited != -EINTR) break;
                    pause_us();
                } while (alive(runtime, deadline));
                runtime.row[W_LOCAL_WAIT] = signed_bits(waited);
                runtime.row[W_LOCAL_STATUS] = signed_bits(status);
                expect(runtime, waited == 2 && status == 0, Failure::WAIT);
                metadata_history_after_wait(runtime, waited, status);
                if (file_mapping != nullptr) expect(runtime, munmap(file_mapping, PAGE) == 0, Failure::MAPPING);
                bool const detached = shmdt(shared) == 0;
                bool const removed = shmctl(own_id, IPC_RMID, nullptr) == 0;
                expect(runtime, detached && removed, Failure::SHM);
                runtime.row[W_STAGE] = static_cast<uint64_t>(Stage::GRACEFUL);
                runtime.row[W_RETURN] = 0;
                if (runtime.row[W_FAILURES] != 0 || !publish_row(runtime) || !progress(runtime, State::DONE)) private_fail(runtime);
                private_exit(0);
            }
            default:
                expect(runtime, false, Failure::CONTROL);
                private_fail(runtime);
        }
        if (runtime.row[W_FAILURES] != 0 || !progress(runtime, State::DONE, a, b, c)) private_fail(runtime);
    }
    private_fail(runtime);
}

auto private_service(unsigned role, uint64_t nonce, uint64_t host_pid, uint64_t deadline) -> int {
    auto runtime = make_runtime(role, false, nonce, deadline, host_pid);
    struct stat control{}, backing{};
    int const control_flags = fcntl(CONTROL_FD, F_GETFL), backing_flags = fcntl(BACKING_FD, F_GETFL);
    expect(runtime,
           control_flags >= 0 && (control_flags & O_ACCMODE) == O_RDWR && backing_flags >= 0 && (backing_flags & O_ACCMODE) == O_RDONLY &&
               fstat(CONTROL_FD, &control) == 0 && S_ISREG(control.st_mode) && control.st_size == static_cast<off_t>(CONTROL_BYTES) &&
               fstat(BACKING_FD, &backing) == 0 && S_ISREG(backing.st_mode) && backing.st_size == static_cast<off_t>(PAGE) &&
               backing.st_ino != 0,
           Failure::IDENTITY);
    runtime.row[EXTRA + 10] = backing.st_ino;
    runtime.row[EXTRA + 11] = backing.st_size;
    runtime.row[EXTRA + 12] = backing.st_dev;
    runtime.row[EXTRA + 13] = signed_bits(control_flags);
    runtime.row[EXTRA + 14] = signed_bits(backing_flags);
    char byte = 'X';
    errno = 0;
    ssize_t const forbidden = pwrite(BACKING_FD, &byte, 1, 0);
    int const forbidden_error = errno;
    runtime.row[EXTRA + 15] = signed_bits(forbidden);
    runtime.row[EXTRA + 16] = static_cast<uint64_t>(forbidden_error);
    expect(runtime, forbidden == -1 && forbidden_error == EBADF, Failure::IDENTITY);
    runtime.row[W_READONLY_RIGHTS] = forbidden == -1 && forbidden_error == EBADF ? 1 : 0;
    unsigned absent{};
    for (int descriptor = 0; descriptor < 256; ++descriptor)
        if (descriptor != CONTROL_FD && descriptor != BACKING_FD) {
            errno = 0;
            int const result = fcntl(descriptor, F_GETFD);
            bool const missing = result == -1 && errno == EBADF;
            expect(runtime, missing, Failure::IDENTITY);
            if (missing)
                ++absent;
            else if (runtime.row[EXTRA + 26] == 0)
                runtime.row[EXTRA + 26] = static_cast<uint64_t>(descriptor + 1);
        }
    runtime.row[EXTRA + 25] = absent;
    expect(runtime, marker_ok(role), Failure::IDENTITY);
    runtime.row[W_MARKER] = runtime.row[W_FAILURES] == 0 ? 1 : 0;
    errno = 0;
    int const outside = open("/srv/compartment-shared-results/immutable.bin", O_RDONLY);
    expect(runtime, outside == -1 && errno == ENOENT, Failure::IDENTITY);
    if (outside >= 0)
        close(outside);
    else if (errno == ENOENT)
        runtime.row[W_HIDDEN_PATH] = 1;
    int const id = shmget(KEY, PAGE, IPC_CREAT | IPC_EXCL | 0600);
    expect(runtime, id > 0, Failure::SHM);
    runtime.row[W_SHMID] = signed_bits(id);
    if (id <= 0 || runtime.row[W_FAILURES] != 0) private_fail(runtime);
    void* const address = shmat(id, reinterpret_cast<void*>(SHM_ADDRESS), 0);
    expect(runtime, address == reinterpret_cast<void*>(SHM_ADDRESS), Failure::SHM);
    if (address != reinterpret_cast<void*>(SHM_ADDRESS)) private_fail(runtime);
    auto* shared = static_cast<Shared*>(address);
    std::memset(address, 0, PAGE);
    runtime.row[W_SHM_VA] = reinterpret_cast<uint64_t>(address);
    int64_t const child = ker::process::fork();
    if (child == 0) {
        auto child_runtime = make_runtime(role, true, nonce, deadline, host_pid);
        child_runtime.row[W_SHMID] = static_cast<uint64_t>(id);
        child_runtime.row[W_SHM_VA] = reinterpret_cast<uint64_t>(address);
        service_loop(child_runtime, id, shared, 0);
    }
    runtime.row[W_FORK] = signed_bits(child);
    expect(runtime, child == 2, Failure::FORK);
    if (runtime.row[W_FAILURES] != 0) private_fail(runtime);
    service_loop(runtime, id, shared, child);
}

auto path(char* output, size_t capacity, const char* name) -> bool {
    int const count = std::snprintf(output, capacity, "%s/%s", RESULTS, name);
    return count > 0 && static_cast<size_t>(count) < capacity;
}
auto persist(const char* name, const void* data, size_t bytes, uint64_t deadline) -> bool {
    char filename[160]{};
    if (!path(filename, sizeof(filename), name)) return false;
    int const fd = open(filename, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    bool const written = exact_io(fd, const_cast<void*>(data), bytes, 0, true, deadline);
    bool const flushed = fsync(fd) == 0, closed = close(fd) == 0;
    return written && flushed && closed;
}
auto archive_row(Runtime& host, const char* name, const Row& row) -> bool {
    bool const saved = persist(name, row.data(), sizeof(row), host.deadline);
    expect(host, saved, Failure::IO);
    if (saved) ++host.row[W_PROGRESS];
    return saved;
}
auto parse_number(const char* text, uint64_t& output) -> bool {
    uint64_t value{};
    for (size_t index = 0; index < 21; ++index) {
        char const digit = text[index];
        if (digit == '\0') {
            if (index == 0) return false;
            output = value;
            return true;
        }
        if (digit < '0' || digit > '9' || value > (UINT64_MAX - static_cast<uint64_t>(digit - '0')) / 10) return false;
        value = value * 10 + static_cast<uint64_t>(digit - '0');
    }
    return false;
}
auto text(const char* value) -> wire::String { return {reinterpret_cast<uint64_t>(value), std::strlen(value)}; }
struct Tree {
    unsigned role{};
    int fd = -1;
    int64_t root = -1, child = -1;
    int id = -1;
    uint64_t root_seq{}, child_seq{};
    bool reaped{};
    Row root_row{}, child_row{};
};
auto send(Runtime& host, Tree& tree, bool child, Op operation, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0) -> uint64_t {
    auto& sequence = child ? tree.child_seq : tree.root_seq;
    ++sequence;
    Record record{};
    record[0] = MAGIC;
    record[1] = VERSION;
    record[2] = host.row[W_NONCE];
    record[3] = tree.role;
    record[4] = sequence;
    record[5] = static_cast<uint64_t>(operation);
    record[7] = a;
    record[8] = b;
    record[9] = c;
    bool const saved = put_record(tree.fd, child ? CHILD_COMMAND : ROOT_COMMAND, record, host.deadline);
    expect(host, saved, Failure::IO);
    return saved ? sequence : 0;
}
auto await(Runtime& host, Tree& tree, bool child, uint64_t sequence, Op operation, State state, Record* returned = nullptr) -> bool {
    auto const deadline = step_deadline(host);
    while (alive(host, deadline)) {
        Record record{};
        if (get_record(tree.fd, child ? CHILD_PROGRESS : ROOT_PROGRESS, host.row[W_NONCE], tree.role, record, host.deadline) &&
            record[4] == sequence && record[5] == static_cast<uint64_t>(operation)) {
            if (record[6] == static_cast<uint64_t>(State::FAILED) || record[10] != 0) {
                expect(host, false, Failure::CONTROL);
                return false;
            }
            if (record[6] == static_cast<uint64_t>(state)) {
                if (returned != nullptr) *returned = record;
                return true;
            }
        }
        pause_us();
    }
    return false;
}
auto issue(Runtime& host, Tree& tree, bool child, Op operation, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0) -> bool {
    auto const sequence = send(host, tree, child, operation, a, b, c);
    return sequence != 0 && await(host, tree, child, sequence, operation, State::DONE);
}

auto metadata_original_phases(Runtime& host, Tree& tree) -> bool {
    if (!issue(host, tree, false, Op::META_ROOT, 0)) return false;
    auto sequence = send(host, tree, true, Op::META_CHILD_ATTACH);
    Record child{};
    if (sequence != 2 || !await(host, tree, true, sequence, Op::META_CHILD_ATTACH, State::DONE, &child) ||
        !issue(host, tree, false, Op::META_ROOT, 1, child[7], sequence))
        return false;
    sequence = send(host, tree, true, Op::META_CHILD_DETACH);
    if (sequence != 3 || !await(host, tree, true, sequence, Op::META_CHILD_DETACH, State::DONE, &child) ||
        !issue(host, tree, false, Op::META_ROOT, 2, child[7], sequence) || !issue(host, tree, false, Op::META_ROOT, 3))
        return false;
    return true;
}
auto metadata_history_commands(Runtime& host, Tree& tree) -> bool {
    auto const sequence = send(host, tree, true, Op::HISTORY_CREATE);
    Record child{};
    return sequence == 23 && await(host, tree, true, sequence, Op::HISTORY_CREATE, State::DONE, &child) &&
           issue(host, tree, false, Op::HISTORY_ATTACH, child[7], child[8], sequence);
}
auto archive_metadata(Runtime& host, Tree& tree) -> bool {
    Row row{}, second{};
    bool valid = exact_io(tree.fd, row.data(), sizeof(row), METADATA_REPORT, false, host.deadline) &&
                 exact_io(tree.fd, second.data(), sizeof(second), METADATA_REPORT, false, host.deadline) && row == second;
    auto const count = tree.role == 1 ? 5U : 10U;
    valid = valid && row[0] == METADATA_MAGIC && row[1] == 1 && row[2] == 3 && row[3] == host.row[W_NONCE] && row[4] == tree.role &&
            row[5] == 1 && row[6] == 2 && row[7] == 100 && row[8] == 100 && row[9] == static_cast<uint64_t>(tree.root) &&
            row[10] == static_cast<uint64_t>(tree.child) && row[11] == host.row[W_PID] && row[12] == static_cast<uint64_t>(tree.id) &&
            row[13] == SHM_ADDRESS && metadata_address(row[16]) && row[16] != SHM_ADDRESS && row[17] == row[16] && row[18] == 2 &&
            row[19] == 0 && row[20] == 3 && metadata_address(row[21]) && row[21] != SHM_ADDRESS && row[22] == row[21] && row[23] == 0 &&
            row[24] == 2 && row[25] == 3 && row[26] == 4 && row[27] == 5 && row[42] == ((uint64_t{1} << count) - 1) && row[43] == count &&
            row[44] > 0 && row[45] == 0 && row[COMMIT] == host.row[W_NONCE];
    if (tree.role == 1) {
        valid = valid && row[14] == 0 && row[15] == 0;
        for (size_t index = 28; index <= 41; ++index) valid = valid && row[index] == 0;
    } else {
        valid = valid && tree.reaped && row[14] > 0 && row[14] <= INT32_MAX && row[14] != row[12] && row[15] == HISTORY_KEY &&
                row[28] == row[14] && metadata_address(row[30]) && row[29] == row[30] && row[30] != SHM_ADDRESS && row[31] == 23 &&
                row[32] == 60 && metadata_address(row[34]) && row[33] == row[34] && row[34] != SHM_ADDRESS &&
                row[35] == history_marker(host.row[W_NONCE]) && row[36] == ~row[35] && row[37] == 2 && row[38] == 0 && row[39] == 0 &&
                row[40] == 0 && row[41] == 61;
    }
    uint64_t previous{};
    for (uint64_t event = 1; event <= count; ++event) {
        auto const* slot = row.data() + METADATA_HEADER + (event - 1) * STAT_WORDS;
        valid = valid && slot[0] == event && slot[2] >= host.row[W_START_US] && slot[2] >= previous && slot[2] < host.deadline &&
                slot[2] != UINT64_MAX && slot[3] == metadata_guard(host.row[W_NONCE], tree.role, event) && slot[19] == ~slot[3];
        previous = slot[2];
        if (event == 10) {
            valid = valid && slot[1] == signed_bits(-EINVAL);
            std::array<uint8_t, 120> expected{};
            expected.fill(0xA5);
            valid = valid && std::memcmp(slot + 4, expected.data(), expected.size()) == 0;
        } else {
            int const id = static_cast<int>(event <= 5 ? row[12] : row[14]);
            int const key = event <= 5 ? KEY : HISTORY_KEY;
            int64_t const creator = event <= 5 ? 1 : 2;
            int64_t const actor = event == 2 || event == 3 || event == 6 ? 2 : 1;
            uint64_t const attachments = event == 2 || event == 4 ? 3 : event == 6 || event == 8 ? 1 : event == 9 ? 0 : 2;
            ker::abi::shm::ShmidDs expected{};
            metadata_expected(expected, id, key, creator, actor, attachments);
            valid = valid && slot[1] == 0 && std::memcmp(slot + 4, &expected, sizeof(expected)) == 0;
        }
    }
    valid = valid && row[46] == row[METADATA_HEADER + 2] && row[47] == previous;
    for (size_t index = METADATA_HEADER + count * STAT_WORDS; index < COMMIT; ++index) valid = valid && row[index] == 0;
    expect(host, valid, Failure::PRESERVATION);
    if (!valid) return false;
    // Supplemental receipts intentionally do not increment the five-primary-report progress count.
    bool const saved = persist(tree.role == 1 ? "metadata-a.bin" : "metadata-b.bin", row.data(), sizeof(row), host.deadline);
    expect(host, saved, Failure::IO);
    return saved;
}

auto load_row(Runtime& host, Tree& tree, bool child, Row& row, Stage stage) -> bool {
    bool const read = exact_io(tree.fd, row.data(), sizeof(row), child ? CHILD_REPORT : ROOT_REPORT, false, host.deadline);
    bool const valid =
        read && row[W_MAGIC] == MAGIC && row[W_VERSION] == VERSION && row[W_KIND] == (child ? 2U : 1U) &&
        row[W_NONCE] == host.row[W_NONCE] && row[COMMIT] == host.row[W_NONCE] && row[W_ROLE] == tree.role &&
        row[W_PID] == (child ? 2U : 1U) && row[W_PPID] == (child ? 1U : 0U) && row[W_UID] == 100 && row[W_EUID] == 100 &&
        row[W_GID] == 100 && row[W_EGID] == 100 && row[W_STAGE] == static_cast<uint64_t>(stage) && row[W_CHECKS] != 0 &&
        row[W_FAILURES] == 0 && row[W_FIRST] == 0 && row[W_FAILED_BITS] == 0 && row[W_WAITS] <= WAIT_MAX && row[W_WAKES] <= WAKE_MAX &&
        row[W_DEADLINES] == 0 && row[W_CLOCKS] == 0 && row[W_REPORT_IO] == 0 && row[W_PHYSICAL_ROOT] == static_cast<uint64_t>(tree.root) &&
        row[W_PHYSICAL_CHILD] == static_cast<uint64_t>(tree.child) && row[W_START_US] != UINT64_MAX && row[W_END_US] >= row[W_START_US] &&
        row[W_END_US] < host.deadline && (stage != Stage::GRACEFUL || row[W_RETURN] == 0);
    expect(host, valid, Failure::IO);
    return valid;
}
auto read_status(int64_t pid, std::array<char, PROC_BYTES + 1>& output, size_t& bytes, uint64_t deadline) -> bool {
    char filename[96]{};
    int const count = std::snprintf(filename, sizeof(filename), "/proc/%lld/status", static_cast<long long>(pid));
    if (count <= 0 || static_cast<size_t>(count) >= sizeof(filename)) return false;
    int const fd = open(filename, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    size_t done{};
    bool valid = true;
    while (done < output.size()) {
        auto const time = clock_us();
        if (time == UINT64_MAX || time >= deadline) {
            valid = false;
            break;
        }
        ssize_t const got = read(fd, output.data() + done, output.size() - done);
        if (got < 0 && errno == EINTR) continue;
        if (got < 0) {
            valid = false;
            break;
        }
        if (got == 0) break;
        done += static_cast<size_t>(got);
    }
    bool const closed = close(fd) == 0;
    if (!valid || !closed || done == 0 || done > PROC_BYTES) return false;
    output[done] = '\0';
    bytes = done;
    return true;
}
auto field(const char* status, const char* name) -> const char* {
    size_t const length = std::strlen(name);
    for (auto* cursor = status; *cursor != '\0';) {
        if (std::strncmp(cursor, name, length) == 0 && cursor[length] == ':') {
            cursor += length + 1;
            while (*cursor == ' ' || *cursor == '\t') ++cursor;
            return cursor;
        }
        auto const* end = std::strchr(cursor, '\n');
        if (end == nullptr) break;
        cursor = end + 1;
    }
    return nullptr;
}
auto number_field(const char* status, const char* name, uint64_t& value) -> bool {
    auto const* cursor = field(status, name);
    if (cursor == nullptr) return false;
    uint64_t result{};
    unsigned count{};
    while (*cursor >= '0' && *cursor <= '9') {
        auto const digit = static_cast<uint64_t>(*cursor++ - '0');
        if (++count > 20 || result > (UINT64_MAX - digit) / 10) return false;
        result = result * 10 + digit;
    }
    if (count == 0 || (*cursor != '\n' && *cursor != '\0' && *cursor != '\t' && *cursor != ' ')) return false;
    value = result;
    return true;
}
auto text_field(const char* status, const char* name, const char* value) -> bool {
    auto const* found = field(status, name);
    size_t const length = std::strlen(value);
    return found != nullptr && std::strncmp(found, value, length) == 0 && (found[length] == '\n' || found[length] == '\0');
}
auto uid_fields(const char* status) -> bool {
    auto const* cursor = field(status, "Uid");
    if (cursor == nullptr) return false;
    for (unsigned index = 0; index < 4; ++index) {
        while (*cursor == ' ' || *cursor == '\t') ++cursor;
        if (std::strncmp(cursor, "100", 3) != 0 || (cursor[3] != '\t' && cursor[3] != ' ' && cursor[3] != '\n')) return false;
        cursor += 3;
    }
    return *cursor == '\n' || *cursor == '\0';
}
auto child_status(const char* status, int64_t child, int64_t parent) -> bool {
    uint64_t pid{}, tgid{}, ppid{};
    return number_field(status, "Pid", pid) && number_field(status, "Tgid", tgid) && number_field(status, "PPid", ppid) &&
           pid == static_cast<uint64_t>(child) && tgid == pid && ppid == static_cast<uint64_t>(parent) && uid_fields(status) &&
           text_field(status, "Name", "probe");
}
auto capture_child(Runtime& host, Tree& tree) -> bool {
    auto const deadline = step_deadline(host);
    while (alive(host, deadline)) {
        DIR* directory = opendir("/proc");
        if (directory == nullptr) {
            expect(host, false, Failure::PROC);
            return false;
        }
        int64_t selected = -1;
        unsigned matches{}, entries{};
        while (auto* entry = readdir(directory)) {
            if (++entries > 4096 || !alive(host, deadline)) break;
            uint64_t candidate{};
            if (!parse_number(entry->d_name, candidate) || candidate <= 2 || candidate > INT64_MAX ||
                candidate == static_cast<uint64_t>(tree.root))
                continue;
            std::array<char, PROC_BYTES + 1> status{};
            size_t bytes{};
            if (read_status(static_cast<int64_t>(candidate), status, bytes, deadline) &&
                child_status(status.data(), static_cast<int64_t>(candidate), tree.root)) {
                selected = static_cast<int64_t>(candidate);
                ++matches;
            }
        }
        bool const closed = closedir(directory) == 0;
        if (!closed || entries > 4096 || matches > 1) {
            expect(host, false, Failure::PROC);
            return false;
        }
        if (matches == 1 && ker::process::kill(selected, 0) == 0) {
            tree.child = selected;
            host.row[EXTRA + tree.role + 1] = static_cast<uint64_t>(selected);
            return true;
        }
        pause_us();
    }
    return false;
}
auto await_park(Runtime& host, Tree& tree, bool child, const char* channel, bool save = false) -> bool {
    int64_t const pid = child ? tree.child : tree.root;
    auto const deadline = step_deadline(host);
    while (alive(host, deadline)) {
        std::array<char, PROC_BYTES + 1> status{};
        size_t bytes{};
        bool const live_before = ker::process::kill(pid, 0) == 0;
        if (live_before && read_status(pid, status, bytes, deadline) && text_field(status.data(), "SchedQueue", "WAITING") &&
            text_field(status.data(), "Wchan", channel) && ker::process::kill(pid, 0) == 0) {
            uint64_t actual{};
            bool const identity = number_field(status.data(), "Pid", actual) && actual == static_cast<uint64_t>(pid) &&
                                  uid_fields(status.data()) && (!child || child_status(status.data(), tree.child, tree.root));
            if (!identity) {
                expect(host, false, Failure::PROC);
                return false;
            }
            if (save) {
                char name[48]{};
                std::snprintf(name, sizeof(name), "proc-child-%c-before.txt", tree.role == 1 ? 'a' : 'b');
                if (!persist(name, status.data(), bytes, host.deadline)) {
                    expect(host, false, Failure::IO);
                    return false;
                }
            }
            return true;
        }
        pause_us();
    }
    return false;
}
auto create_tree(Runtime& host, Tree& tree, int backing_fd) -> bool {
    char marker[160]{};
    if (!path(marker, sizeof(marker), tree.role == 1 ? "marker-a" : "marker-b")) return false;
    std::array<const char*, 4> sources{FIXTURE, "/lib/ld.so", "/usr/lib/libc.so", marker};
    std::array<const char*, 4> destinations{"/app/probe", "/lib/ld.so", "/usr/lib/libc.so", "/same/name"};
    std::array<wire::Asset, 4> assets{};
    for (auto& asset : assets) asset.source_fd = -1;
    bool opened = true;
    for (size_t index = 0; index < assets.size(); ++index) {
        int const fd = open(sources[index], O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            opened = false;
            break;
        }
        assets[index] = {fd, index < 2 ? 0755U : 0644U, text(destinations[index]), 0};
    }
    wire::Spec specification;
    bool limits = opened;
    for (size_t index = 0; limits && index < specification.limits.size(); ++index) {
        auto const result = static_cast<int64_t>(syscall(ker::abi::callnums::process, uint64_t{48}, index,
                                                         reinterpret_cast<uint64_t>(&specification.limits[index]), uint64_t{1}));
        if (result != 0) limits = false;
    }
    char nonce[24]{}, host_pid[24]{}, deadline[24]{};
    std::snprintf(nonce, sizeof(nonce), "%llu", static_cast<unsigned long long>(host.row[W_NONCE]));
    std::snprintf(host_pid, sizeof(host_pid), "%llu", static_cast<unsigned long long>(host.row[W_PID]));
    std::snprintf(deadline, sizeof(deadline), "%llu", static_cast<unsigned long long>(host.deadline));
    std::array args{text("/app/probe"), text("--service"), text(tree.role == 1 ? "A" : "B"), text(nonce), text(host_pid), text(deadline)};
    std::array environment{text("LD_LIBRARY_PATH=/usr/lib")};
    std::array<wire::Descriptor, 2> descriptors{{{tree.fd, CONTROL_FD, 0}, {backing_fd, BACKING_FD, 0}}};
    specification.executable = args[0];
    specification.assets = reinterpret_cast<uint64_t>(assets.data());
    specification.asset_count = assets.size();
    specification.descriptors = reinterpret_cast<uint64_t>(descriptors.data());
    specification.descriptor_count = descriptors.size();
    specification.argv = reinterpret_cast<uint64_t>(args.data());
    specification.argc = args.size();
    specification.envp = reinterpret_cast<uint64_t>(environment.data());
    specification.envc = environment.size();
    int64_t result = -EIO;
    if (limits && alive(host, host.deadline))
        result =
            static_cast<int64_t>(syscall(ker::abi::callnums::process, wire::CREATE_OPERATION, reinterpret_cast<uint64_t>(&specification)));
    // Record positive ownership before any subsequent IO, close or observation.
    if (result > 0) {
        tree.root = result;
        host.row[EXTRA + tree.role - 1] = static_cast<uint64_t>(result);
    }
    host.row[EXTRA + 4 + tree.role - 1] = signed_bits(result);
    for (auto const& asset : assets)
        if (asset.source_fd >= 0) expect(host, close(asset.source_fd) == 0, Failure::IO);
    expect(host, limits && result > 2, Failure::FORK);
    return limits && result > 2 && host.row[W_FAILURES] == 0;
}
auto reap(Runtime& host, Tree& tree, int expected, size_t result_word, size_t status_word, size_t zero_word) -> bool {
    auto const deadline = step_deadline(host);
    int status = -1;
    int64_t result{};
    do {
        result = ker::process::waitpid(tree.root, &status, WNOHANG, nullptr);
        if (result == 0) ++host.row[EXTRA + zero_word];
        if (result != 0 && result != -EINTR) break;
        pause_us();
    } while (alive(host, deadline));
    host.row[EXTRA + result_word] = signed_bits(result);
    host.row[EXTRA + status_word] = signed_bits(status);
    if (result == tree.root) tree.reaped = true;
    expect(host, result == tree.root && status == expected, Failure::WAIT);
    return result == tree.root && status == expected;
}
auto sharing_rows_ok(Runtime& host, Tree& tree, Stage stage) -> bool {
    if (!load_row(host, tree, false, tree.root_row, stage) || !load_row(host, tree, true, tree.child_row, stage)) return false;
    auto const& root = tree.root_row;
    auto const& child = tree.child_row;
    bool const valid = root[W_SHMID] == static_cast<uint64_t>(tree.id) && child[W_SHMID] == root[W_SHMID] &&
                       root[W_SHM_VA] == SHM_ADDRESS && child[W_SHM_VA] == SHM_ADDRESS && root[W_FORK] == 2 && root[W_OWN_KEY] == 1 &&
                       root[W_FOREIGN_ATTACH_BITS] == 3 && root[W_FOREIGN_STAT_BITS] == 3 && root[W_FOREIGN_RMID_BITS] == 3 &&
                       root[W_STAT_GUARD_BITS] == 3 && root[W_SIGNAL_BITS] == 3 && root[W_MARKER] == 1 && root[W_HIDDEN_PATH] == 1 &&
                       root[W_READONLY_RIGHTS] == 1 && root[W_FILE_ZERO] == 1 && root[W_RW_WAKES] == ROUNDS && root[W_RW_ACKS] == ROUNDS &&
                       child[W_RW_WAITS] == ROUNDS && child[W_RW_ACKS] == ROUNDS && root[W_FOREIGN_WAKE_ZERO] == 1 &&
                       root[W_NEGATIVE_WAIT_ZERO] == 1 && child[W_RESCUE_WAKE_ONE] == 1 && root[W_FILE_WAKE_ONE] == ROUNDS &&
                       root[W_FILE_WAIT_ZERO] == ROUNDS && root[W_FILE_ACKS] == ROUNDS && root[W_WAITS] == ROUNDS + 1 &&
                       child[W_WAITS] == ROUNDS && root[W_PAYLOAD_LAST] == ROUNDS && child[W_PAYLOAD_LAST] == ROUNDS;
    expect(host, valid, Failure::PRESERVATION);
    for (auto const* row : {&root, &child}) {
        for (size_t index = 0; index < (*row)[W_WAITS]; ++index) expect(host, (*row)[WAIT_BASE + index] == 0, Failure::FUTEX);
        unsigned ones{}, zeros{};
        for (size_t index = 0; index < (*row)[W_WAKES]; ++index) {
            auto const result = static_cast<int64_t>((*row)[WAKE_BASE + index]);
            expect(host, result == 0 || result == 1, Failure::FUTEX);
            if (result == 1)
                ++ones;
            else if (result == 0)
                ++zeros;
        }
        expect(host, ones == (row == &root ? ROUNDS * 2 : 1) && (row != &root || zeros >= 1), Failure::FUTEX);
    }
    return valid && host.row[W_FAILURES] == 0;
}
void cleanup_owned(Runtime& host, std::array<Tree, 2>& trees) {
    // Fixed iteration budget still bounds cleanup if the monotonic clock fails.
    for (auto& tree : trees)
        if (tree.root > 2 && !tree.reaped) (void)ker::process::kill(tree.root, SIGKILL);
    for (unsigned attempt = 0; attempt < 500; ++attempt) {
        bool done = true;
        for (auto& tree : trees)
            if (tree.root > 2 && !tree.reaped) {
                int status{};
                auto const result = ker::process::waitpid(tree.root, &status, WNOHANG, nullptr);
                if (result == tree.root)
                    tree.reaped = true;
                else
                    done = false;
            }
        if (done) break;
        pause_us(10'000);
    }
    for (auto& tree : trees) {
        if (tree.root > 2 && !tree.reaped) expect(host, false, Failure::WAIT);
        // Failure-only fallback, never evidence of successful init retirement.
        if (tree.child > 2 && ker::process::kill(tree.child, 0) == 0) (void)ker::process::kill(tree.child, SIGKILL);
    }
}
auto host_work(Runtime& host, std::array<Tree, 2>& trees, int backing, int host_id) -> bool {
    for (auto& tree : trees)
        if (!create_tree(host, tree, backing)) return false;
    for (auto& tree : trees) {
        Record ready{};
        if (!await(host, tree, false, 1, Op::HELLO, State::READY, &ready) || ready[7] == 0 || ready[7] > INT32_MAX ||
            ready[8] != SHM_ADDRESS || !capture_child(host, tree) || !await(host, tree, true, 1, Op::HELLO, State::READY))
            return false;
        tree.id = static_cast<int>(ready[7]);
        if (!issue(host, tree, false, Op::INFO, static_cast<uint64_t>(tree.root), static_cast<uint64_t>(tree.child)) ||
            !issue(host, tree, true, Op::INFO, static_cast<uint64_t>(tree.root), static_cast<uint64_t>(tree.child)))
            return false;
    }
    for (auto& tree : trees)
        if (!metadata_original_phases(host, tree)) return false;
    expect(host,
           trees[0].root != trees[1].root && trees[0].child != trees[1].child && trees[0].id != trees[1].id && trees[0].id != host_id &&
               trees[1].id != host_id,
           Failure::IDENTITY);
    if (host.row[W_FAILURES] != 0) return false;
    host.row[EXTRA] = trees[0].root;
    host.row[EXTRA + 1] = trees[1].root;
    host.row[EXTRA + 2] = trees[0].child;
    host.row[EXTRA + 3] = trees[1].child;
    host.row[EXTRA + 17] = now(host);
    host.row[EXTRA + 25] = static_cast<uint64_t>(host_id);
    host.row[EXTRA + 26] = 1;
    expect(host, ker::process::kill(trees[0].root, 0) == 0 && ker::process::kill(trees[1].root, 0) == 0, Failure::SIGNAL);
    for (unsigned index = 0; index < 2; ++index) {
        auto& tree = trees[index];
        auto& sibling = trees[1 - index];
        if (!issue(host, tree, false, Op::FOREIGN, static_cast<uint64_t>(host_id), static_cast<uint64_t>(sibling.id),
                   static_cast<uint64_t>(sibling.root)))
            return false;
    }
    for (unsigned index = 0; index < 2; ++index) {
        auto& waiter = trees[index];
        auto& foreign = trees[1 - index];
        auto const sequence = send(host, waiter, false, Op::NEGATIVE_WAIT);
        if (sequence == 0 || !await(host, waiter, false, sequence, Op::NEGATIVE_WAIT, State::READY) ||
            !await_park(host, waiter, false, "futex_wait") || !issue(host, foreign, false, Op::FOREIGN_WAKE) ||
            !issue(host, waiter, true, Op::RESCUE) || !await(host, waiter, false, sequence, Op::NEGATIVE_WAIT, State::DONE))
            return false;
        ++host.row[EXTRA + 29];
    }
    for (auto& tree : trees)
        for (unsigned round = 1; round <= ROUNDS; ++round) {
            auto const sequence = send(host, tree, true, Op::RW_WAIT, round);
            if (sequence == 0 || !await(host, tree, true, sequence, Op::RW_WAIT, State::READY) ||
                !await_park(host, tree, true, "futex_wait") || !issue(host, tree, false, Op::RW_WAKE, round) ||
                !await(host, tree, true, sequence, Op::RW_WAIT, State::DONE))
                return false;
            ++host.row[EXTRA + 27];
        }
    for (auto& tree : trees)
        if (!issue(host, tree, false, Op::MAP_BACKING)) return false;
    for (unsigned direction = 0; direction < 2; ++direction)
        for (unsigned round = 1; round <= ROUNDS; ++round) {
            auto& waiter = trees[direction];
            auto& waker = trees[1 - direction];
            auto const sequence = send(host, waiter, false, Op::FILE_WAIT, round);
            if (sequence == 0 || !await(host, waiter, false, sequence, Op::FILE_WAIT, State::READY) ||
                !await_park(host, waiter, false, "futex_wait") || !issue(host, waker, false, Op::FILE_WAKE, round) ||
                !await(host, waiter, false, sequence, Op::FILE_WAIT, State::DONE))
                return false;
            ++host.row[EXTRA + 28];
        }
    for (auto& tree : trees)
        if (!issue(host, tree, false, Op::PREFIX) || !issue(host, tree, true, Op::PREFIX) || !sharing_rows_ok(host, tree, Stage::PREFIX))
            return false;
    expect(host,
           trees[0].root_row[W_FILE_VA] != 0 && trees[1].root_row[W_FILE_VA] != 0 &&
               trees[0].root_row[W_FILE_VA] != trees[1].root_row[W_FILE_VA],
           Failure::MAPPING);
    if (host.row[W_FAILURES] != 0 || !archive_row(host, "root-a.bin", trees[0].root_row) ||
        !archive_row(host, "child-a.bin", trees[0].child_row))
        return false;
    if (!archive_metadata(host, trees[0])) return false;
    auto& victim = trees[0];
    auto& survivor = trees[1];
    auto const sleep_sequence = send(host, victim, true, Op::LONG_SLEEP);
    Record sleep_ready{};
    if (sleep_sequence == 0 || !await(host, victim, true, sleep_sequence, Op::LONG_SLEEP, State::READY, &sleep_ready) ||
        !await_park(host, victim, true, "nanosleep", true))
        return false;
    host.row[EXTRA + 21] = now(host);
    host.row[EXTRA + 30] = sleep_ready[7];
    expect(host, sleep_ready[7] != 0 && sleep_ready[7] != UINT64_MAX && sleep_ready[7] <= host.row[EXTRA + 21], Failure::CLOCK);
    auto const b_root_before = ker::process::kill(survivor.root, 0), b_child_before = ker::process::kill(survivor.child, 0);
    if (b_root_before != 0 || b_child_before != 0 || ker::process::kill(victim.root, 0) != 0 || ker::process::kill(victim.child, 0) != 0) {
        expect(host, false, Failure::PRESERVATION);
        return false;
    }
    host.row[EXTRA + 18] = now(host);
    auto const kill_time = now(host);
    auto const killed = ker::process::kill(victim.root, SIGKILL);
    host.row[EXTRA + 10] = signed_bits(killed);
    host.row[EXTRA + 19] = kill_time;
    expect(host,
           killed == 0 && kill_time != UINT64_MAX && kill_time <= UINT64_MAX - RETIRE_US && kill_time >= host.row[EXTRA + 21] &&
               kill_time - host.row[EXTRA + 21] < 2'000'000,
           Failure::SIGNAL);
    if (host.row[W_FAILURES] != 0 || !reap(host, victim, 9, 6, 7, 22)) return false;
    auto const retire_limit = kill_time + RETIRE_US;
    auto const retire_deadline = retire_limit < host.deadline ? retire_limit : host.deadline;
    int64_t child_probe = 0;
    do {
        child_probe = ker::process::kill(victim.child, 0);
        if (child_probe == -ESRCH) break;
        if (child_probe != 0) {
            expect(host, false, Failure::RETIRE);
            return false;
        }
        host.row[EXTRA + 11] = signed_bits(child_probe);
        pause_us();
    } while (alive(host, retire_deadline));
    host.row[EXTRA + 12] = signed_bits(child_probe);
    host.row[EXTRA + 20] = now(host);
    expect(host, child_probe == -ESRCH && host.row[EXTRA + 20] >= kill_time && host.row[EXTRA + 20] - kill_time < RETIRE_US,
           Failure::RETIRE);
    auto const b_root_after = ker::process::kill(survivor.root, 0), b_child_after = ker::process::kill(survivor.child, 0);
    host.row[EXTRA + 13] = signed_bits(b_root_after);
    host.row[EXTRA + 14] = signed_bits(b_child_after);
    expect(host, b_root_after == 0 && b_child_after == 0, Failure::PRESERVATION);
    if (host.row[W_FAILURES] != 0 || !await_park(host, survivor, true, "nanosleep", true) ||
        !issue(host, survivor, false, Op::POST_DEATH) || !issue(host, survivor, true, Op::POST_DEATH))
        return false;
    host.row[EXTRA + 15] = survivor.root_row[W_PROGRESS];
    if (!metadata_history_commands(host, survivor)) return false;
    if (!issue(host, survivor, true, Op::STOP_CHILD) || !issue(host, survivor, false, Op::EXIT_ROOT) ||
        !reap(host, survivor, 0, 8, 9, 23) || !sharing_rows_ok(host, survivor, Stage::GRACEFUL))
        return false;
    host.row[EXTRA + 16] = survivor.root_row[W_PROGRESS];
    expect(host,
           survivor.root_row[W_PROGRESS] > host.row[EXTRA + 15] && survivor.child_row[W_PROGRESS] > 0 &&
               survivor.root_row[W_LOCAL_WAIT] == 2 && survivor.root_row[W_LOCAL_STATUS] == 0,
           Failure::PRESERVATION);
    if (!archive_metadata(host, survivor)) return false;
    return host.row[W_FAILURES] == 0 && archive_row(host, "root-b.bin", survivor.root_row) &&
           archive_row(host, "child-b.bin", survivor.child_row);
}
auto host_service() -> int {
    auto const start = clock_us();
    Runtime host{};
    host.row[W_MAGIC] = MAGIC;
    host.row[W_VERSION] = VERSION;
    host.row[W_KIND] = 0;
    host.row[W_ROLE] = 0;
    host.row[W_PID] = ker::process::getpid();
    host.row[W_PPID] = ker::process::getppid();
    host.row[W_UID] = getuid();
    host.row[W_EUID] = geteuid();
    host.row[W_GID] = getgid();
    host.row[W_EGID] = getegid();
    host.row[W_START_US] = start;
    bool const valid_clock = start != UINT64_MAX && start <= UINT64_MAX - RUN_US;
    host.deadline = valid_clock ? start + RUN_US : 0;
    uint64_t const nonce = valid_clock ? (start ^ (host.row[W_PID] << 16U) ^ MAGIC) : 0;
    host.row[W_NONCE] = nonce;
    expect(host, valid_clock && nonce != 0, Failure::CLOCK);
    expect(host,
           host.row[W_PID] > 2 && host.row[W_PID] <= INT64_MAX && host.row[W_UID] == 100 && host.row[W_EUID] == 100 &&
               host.row[W_GID] == 100 && host.row[W_EGID] == 100 &&
               ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) == 0,
           Failure::IDENTITY);
    std::array<Tree, 2> trees{};
    trees[0].role = 1;
    trees[1].role = 2;
    int backing = -1, host_id = -1;
    Shared* host_shared = nullptr;
    bool prepared = host.row[W_FAILURES] == 0;
    std::array<uint8_t, PAGE> zero{};
    for (auto& tree : trees)
        if (prepared) {
            char filename[160]{};
            prepared = path(filename, sizeof(filename), tree.role == 1 ? "control-a.bin" : "control-b.bin");
            if (prepared) tree.fd = open(filename, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
            prepared = prepared && tree.fd >= 0 && ftruncate(tree.fd, CONTROL_BYTES) == 0 && fsync(tree.fd) == 0;
        }
    if (prepared)
        prepared = persist("immutable.bin", zero.data(), zero.size(), host.deadline) && persist("marker-a", "tree-A\n", 7, host.deadline) &&
                   persist("marker-b", "tree-B\n", 7, host.deadline);
    if (prepared) {
        char filename[160]{};
        prepared = path(filename, sizeof(filename), "immutable.bin");
        if (prepared) backing = open(filename, O_RDONLY | O_CLOEXEC);
        prepared = backing >= 0;
    }
    if (prepared) {
        host_id = shmget(KEY, PAGE, IPC_CREAT | IPC_EXCL | 0600);
        if (host_id > 0) {
            void* const address = shmat(host_id, nullptr, 0);
            if (address != reinterpret_cast<void*>(-1)) {
                host_shared = static_cast<Shared*>(address);
                __atomic_store_n(&host_shared->payload, nonce, __ATOMIC_RELEASE);
                __atomic_store_n(&host_shared->complement, ~nonce, __ATOMIC_RELEASE);
            }
        }
        prepared = host_id > 0 && host_shared != nullptr;
    }
    expect(host, prepared, Failure::IO);
    bool passed = prepared && host_work(host, trees, backing, host_id);
    if (!passed) {
        expect(host, false, Failure::CONTROL);
        cleanup_owned(host, trees);
    }
    if (host_shared != nullptr) {
        expect(host,
               __atomic_load_n(&host_shared->word, __ATOMIC_ACQUIRE) == 0 &&
                   __atomic_load_n(&host_shared->payload, __ATOMIC_ACQUIRE) == nonce &&
                   __atomic_load_n(&host_shared->complement, __ATOMIC_ACQUIRE) == ~nonce && shmget(KEY, PAGE, 0600) == host_id &&
                   own_stat(host, host_id, false),
               Failure::PRESERVATION);
        auto const host_stat =
            metadata_stat(host, host_id, KEY, static_cast<int64_t>(host.row[W_PID]), static_cast<int64_t>(host.row[W_PID]), 1, 11, 0);
        std::memcpy(host.row.data() + WAIT_BASE, host_stat.data(), sizeof(host_stat));
        expect(host, shmdt(host_shared) == 0, Failure::SHM);
    }
    if (host_id > 0) expect(host, shmctl(host_id, IPC_RMID, nullptr) == 0, Failure::SHM);
    if (backing >= 0) {
        int unchanged = -1;
        expect(host, exact_io(backing, &unchanged, sizeof(unchanged), 0, false, host.deadline) && unchanged == 0, Failure::PRESERVATION);
        expect(host, close(backing) == 0, Failure::IO);
    }
    for (auto& tree : trees)
        if (tree.fd >= 0) {
            bool const flushed = fsync(tree.fd) == 0, closed = close(tree.fd) == 0;
            expect(host, flushed && closed, Failure::IO);
            tree.fd = -1;
        }
    host.row[W_STAGE] = static_cast<uint64_t>(passed && host.row[W_FAILURES] == 0 ? Stage::COMPLETE : Stage::FAILED);
    host.row[W_END_US] = clock_us();
    host.row[W_RETURN] = passed && host.row[W_FAILURES] == 0 ? 0 : 71;
    host.row[COMMIT] = nonce;
    // Cleanup may consume the workload deadline; diagnostic persistence has its own finite allowance.
    auto const after = clock_us();
    uint64_t const receipt_deadline = after != UINT64_MAX && after <= UINT64_MAX - STEP_US ? after + STEP_US : 0;
    bool const saved = persist("host.bin", host.row.data(), sizeof(Row), receipt_deadline);
    passed = passed && host.row[W_FAILURES] == 0 && saved;
    char line[512]{};
    int const count = std::snprintf(
        line, sizeof(line),
        "%s compartment-shared-runtime nonce=%llu host=%llu root-a=%lld root-b=%lld child-a=%lld child-b=%lld mutable=%llu readonly=%llu "
        "negative=%llu wait-a=%lld status-a=%lld wait-b=%lld status-b=%lld retired=%u survivor=%u reports=%llu failures=%llu first=%llu\n",
        passed ? "PASS" : "FAIL", static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(host.row[W_PID]),
        static_cast<long long>(trees[0].root), static_cast<long long>(trees[1].root), static_cast<long long>(trees[0].child),
        static_cast<long long>(trees[1].child), static_cast<unsigned long long>(host.row[EXTRA + 27]),
        static_cast<unsigned long long>(host.row[EXTRA + 28]), static_cast<unsigned long long>(host.row[EXTRA + 29]),
        static_cast<long long>(host.row[EXTRA + 6]), static_cast<long long>(host.row[EXTRA + 7]),
        static_cast<long long>(host.row[EXTRA + 8]), static_cast<long long>(host.row[EXTRA + 9]), passed ? 1U : 0U, passed ? 1U : 0U,
        static_cast<unsigned long long>(host.row[W_PROGRESS] + (saved ? 1 : 0)), static_cast<unsigned long long>(host.row[W_FAILURES]),
        static_cast<unsigned long long>(host.row[W_FIRST]));
    bool const status_saved =
        count > 0 && static_cast<size_t>(count) < sizeof(line) && persist("status", line, static_cast<size_t>(count), receipt_deadline);
    if (status_saved)
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(count));
    else {
        constexpr char failure[] = "FAIL compartment-shared-runtime receipt-write failures=1\n";
        (void)write(STDOUT_FILENO, failure, sizeof(failure) - 1);
    }
    return passed && status_saved ? 0 : 71;
}

auto init_exit_witness() -> int {
    namespace init = ker::abi::init_control;
    constexpr char SERVICE[] = "compartment-shared-probe";
    struct GuardedSnapshot {
        uint64_t before = GUARD;
        init::StatusSnapshot value{};
        uint64_t after = GUARD;
    };
    static_assert(sizeof(init::StatusSnapshot) == 4160 && init::ABI_VERSION == 1 && offsetof(GuardedSnapshot, value) == 8 &&
                  offsetof(GuardedSnapshot, after) == 4168);
    auto const start = clock_us();
    bool const clock_valid = start != UINT64_MAX && start <= UINT64_MAX - 30'000'000;
    uint64_t const deadline = clock_valid ? start + 30'000'000 : 0;
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    bool observed = false, history = false;
    int64_t error = !clock_valid ? -EIO : (getuid() != 100 || geteuid() != 100 || getgid() != 100 || getegid() != 100 ? -EPERM : 0);
    while (error == 0 && !observed) {
        auto const time = clock_us();
        if (time == UINT64_MAX || time < start) {
            error = -EIO;
            break;
        }
        if (time >= deadline) {
            error = -ETIMEDOUT;
            break;
        }
        GuardedSnapshot guarded{};
        auto const result = ker::process::init_status_read(&guarded.value);
        auto const after = clock_us();
        if (after == UINT64_MAX || after < time) {
            error = -EIO;
            break;
        }
        if (after >= deadline) {
            error = -ETIMEDOUT;
            break;
        }
        if (result != 0 && result != -EAGAIN) {
            error = result;
            break;
        }
        if (guarded.before != GUARD || guarded.after != GUARD) {
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
        if (!observed) pause_us();
    }
    auto const end = clock_us();
    auto const receipt_deadline = end != UINT64_MAX && end <= UINT64_MAX - STEP_US ? end + STEP_US : 0;
    bool const snapshot_saved = persist("init-exit-snapshot.bin", &snapshot, sizeof(snapshot), receipt_deadline);
    bool const passed = observed && error == 0 && snapshot_saved;
    char line[512]{};
    int const count = std::snprintf(
        line, sizeof(line),
        "%s compartment-shared-init-exit service=compartment-shared-probe generation=%llu restarts=%llu sequence=%llu published-ns=%llu "
        "state=%u readiness=%u wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u uid=100 read-error=%lld failures=%u\n",
        passed ? "PASS" : "FAIL", static_cast<unsigned long long>(selected.generation),
        static_cast<unsigned long long>(selected.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned long long>(snapshot.published_mono_ns), static_cast<unsigned>(selected.state),
        static_cast<unsigned>(selected.readiness), selected.last_wait_status, selected.last_error, history ? 1U : 0U, snapshot.size,
        snapshot.version, static_cast<long long>(error), passed ? 0U : 1U);
    bool const saved = count > 0 && static_cast<size_t>(count) < sizeof(line) &&
                       persist("init-exit-status", line, static_cast<size_t>(count), receipt_deadline);
    if (saved)
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(count));
    else {
        constexpr char failure[] = "FAIL compartment-shared-init-exit receipt-write failures=1\n";
        (void)write(STDOUT_FILENO, failure, sizeof(failure) - 1);
    }
    return passed && saved ? 0 : 72;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 1) return host_service();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return init_exit_witness();
    uint64_t nonce{}, host{}, deadline{};
    if (argc == 6 && std::strcmp(argv[1], "--service") == 0 && (std::strcmp(argv[2], "A") == 0 || std::strcmp(argv[2], "B") == 0) &&
        parse_number(argv[3], nonce) && nonce != 0 && parse_number(argv[4], host) && host > 2 && host <= INT64_MAX &&
        parse_number(argv[5], deadline) && deadline != 0)
        return private_service(argv[2][0] == 'A' ? 1 : 2, nonce, host, deadline);
    return 73;
}
