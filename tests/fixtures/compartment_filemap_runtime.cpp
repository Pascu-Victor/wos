// Finite File-map characterization; execution completion is separate from semantic acceptance.
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/futex.h>
#include <sys/init_control.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <abi/compartment.hpp>
#include <array>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
namespace wire = ker::abi::compartment;
constexpr char RESULTS[] = "/srv/compartment-filemap-results";
constexpr char BINARY[] = "/usr/libexec/compartment-filemap-runtime";
constexpr char PRIVATE[] = "/app/file-probe";
constexpr uint64_t MAGIC = 0x39464d4150434841ULL, VERSION = 1, GUARD = 0x9d52b691a3e7c845ULL;
constexpr uint64_t RUN_US = 60'000'000, STEP_US = 5'000'000;
constexpr size_t PAGE = 4096, CONTROL_BYTES = 65536, ROOT_WORDS = 4096, SMALL_WORDS = 1024;
using Record = std::array<uint64_t, 64>;
using Request = std::array<uint64_t, 128>;
enum Command : uint64_t {
    HELLO = 1,
    STORE0,
    SYNC0,
    READ0,
    PREAD0,
    PWRITE0,
    REMAP0,
    STORE_PRIVATE,
    READ_PRIVATE,
    FORK,
    CHILD_WAIT,
    WAKE,
    READ_PARENT_PRIVATE,
    EXIT_CHILD,
    WAIT_CHILD,
    PROMOTE,
    BASELINE_PROMO,
    READ_PROMO,
    RIGHTS,
    STOP_ROOT,
    RETAINED
};
enum Event : uint64_t {
    E_MAP = 1,
    E_STORE,
    E_SYNC,
    E_READ,
    E_PREAD,
    E_PWRITE,
    E_UNMAP,
    E_PROTECT,
    E_FORK,
    E_WAIT,
    E_WAKE,
    E_WAITPID,
    E_CLOSE,
    E_PARK,
    E_GUARDS,
    E_OPEN,
    E_FDS
};
enum Phase : uint64_t { INDEPENDENT = 1, WRITEBACK = 2, REMAP = 4, ISOLATION = 8, SHARED_FORK = 16, PROMOTION = 32, PERMISSIONS = 64 };
enum State : uint64_t { COMMAND = 1, READY = 2, DONE = 3, FAIL = 4 };
struct Payload {
    uint64_t marker{}, complement{};
};
struct Guarded {
    uint64_t before{};
    Payload value{};
    uint64_t after{};
};
static_assert(sizeof(Guarded) == 32 && sizeof(Record) == 512 && sizeof(Request) == 1024);
static_assert(sizeof(wire::Spec) == 408 && sizeof(struct stat) == 144 && std::endian::native == std::endian::little);
static_assert(128 + 320 * 12 < ROOT_WORDS && 128 + 64 * 12 < SMALL_WORDS);
struct Runtime {
    uint64_t* row{};
    size_t words{}, capacity{};
    unsigned role{};
    uint64_t nonce{}, deadline{}, sequence{}, operation{}, phase{}, round{};
    int control = 9;
    std::array<uint64_t, 4> maps{};
    Request request{};
    explicit Runtime(unsigned r, uint64_t n, uint64_t d)
        : words(r == 1 || r == 2 ? ROOT_WORDS : SMALL_WORDS), capacity(r == 1 || r == 2 ? 320 : 64), role(r), nonce(n), deadline(d) {
        row = static_cast<uint64_t*>(std::calloc(words, sizeof(uint64_t)));
    }
    ~Runtime() { std::free(row); }
    Runtime(Runtime const&) = delete;
    auto operator=(Runtime const&) -> Runtime& = delete;
};
struct Tree {
    unsigned role{};
    int fd = -1;
    int64_t root = -1, child = -1;
    bool consumed{};
    uint64_t root_seq{}, child_seq{};
};
auto bits(int64_t value) -> uint64_t { return std::bit_cast<uint64_t>(value); }
auto raw(uint64_t subsystem, uint64_t a, uint64_t b = 0, uint64_t c = 0, uint64_t d = 0, uint64_t e = 0, uint64_t f = 0) -> int64_t {
    return std::bit_cast<int64_t>(syscall(subsystem, a, b, c, d, e, f));
}
auto proc(uint64_t op, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0) -> int64_t { return raw(3, op, a, b, c); }
auto now() -> uint64_t {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t) != 0 || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= 1'000'000'000) return UINT64_MAX;
    auto const s = static_cast<uint64_t>(t.tv_sec);
    return s > (UINT64_MAX - 999999) / 1000000 ? UINT64_MAX : s * 1000000 + static_cast<uint64_t>(t.tv_nsec) / 1000;
}
auto within(uint64_t deadline) -> bool {
    auto const t = now();
    return t != UINT64_MAX && t < deadline;
}
auto step(uint64_t deadline) -> uint64_t {
    auto const t = now();
    return t > UINT64_MAX - STEP_US ? 0 : (t + STEP_US < deadline ? t + STEP_US : deadline);
}
void pause() {
    timespec t{0, 1'000'000};
    (void)nanosleep(&t, nullptr);
}
auto guard(uint64_t nonce, unsigned role, uint64_t selector) -> uint64_t { return GUARD ^ nonce ^ (uint64_t{role} << 48) ^ selector; }
auto marker(uint64_t nonce, uint64_t phase, uint64_t round) -> uint64_t {
    return (nonce ^ (phase * 0x9e3779b97f4a7c15ULL) ^ ((round + 1) * 0xd6e8feb86659fd93ULL)) | 1;
}
auto payload(uint64_t value) -> Payload { return {value, value == 0 ? 0 : ~value}; }
auto read_payload(uint64_t address) -> Payload {
    auto* p = reinterpret_cast<Payload*>(address);
    return {__atomic_load_n(&p->marker, __ATOMIC_ACQUIRE), __atomic_load_n(&p->complement, __ATOMIC_RELAXED)};
}
void write_payload(uint64_t address, uint64_t value) {
    auto* p = reinterpret_cast<Payload*>(address);
    __atomic_store_n(&p->complement, ~value, __ATOMIC_RELAXED);
    __atomic_store_n(&p->marker, value, __ATOMIC_RELEASE);
}
auto io(int fd, void* data, size_t bytes, off_t offset, bool writing, uint64_t deadline) -> bool {
    size_t done{};
    auto previous = now();
    unsigned interrupted{};
    while (done < bytes && within(deadline)) {
        auto const before = now();
        if (before < previous) return false;
        auto* p = static_cast<uint8_t*>(data) + done;
        auto const n = writing ? pwrite(fd, p, bytes - done, offset + static_cast<off_t>(done))
                               : pread(fd, p, bytes - done, offset + static_cast<off_t>(done));
        auto const after = now();
        if (after == UINT64_MAX || after < before || after >= deadline) return false;
        previous = after;
        if (n < 0 && errno == EINTR && ++interrupted <= 256) continue;
        if (n <= 0 || static_cast<size_t>(n) > bytes - done) return false;
        done += static_cast<size_t>(n);
    }
    return done == bytes;
}
auto filename(char* output, size_t size, const char* name) -> bool {
    auto const n = std::snprintf(output, size, "%s/%s", RESULTS, name);
    return n > 0 && static_cast<size_t>(n) < size;
}
auto save(const char* name, const void* data, size_t bytes, uint64_t deadline) -> bool {
    char p[160]{};
    if (!filename(p, sizeof(p), name) || !within(deadline)) return false;
    int const fd = open(p, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    bool const written = io(fd, const_cast<void*>(data), bytes, 0, true, deadline);
    bool const synced = fsync(fd) == 0 && within(deadline), closed = close(fd) == 0;
    return written && synced && closed;
}
auto save_committed_row(const char* name, uint64_t* row, size_t words, uint64_t nonce, uint64_t deadline) -> bool {
    char p[160]{};
    if (row == nullptr || words == 0 || words > ROOT_WORDS || !filename(p, sizeof(p), name) || !within(deadline)) return false;
    int const fd = open(p, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    row[words - 1] = 0;
    bool const prepared = io(fd, row, words * 8, 0, true, deadline) && fsync(fd) == 0 && within(deadline);
    row[words - 1] = nonce;
    bool const committed =
        prepared && io(fd, &row[words - 1], 8, static_cast<off_t>((words - 1) * 8), true, deadline) && fsync(fd) == 0 && within(deadline);
    bool const closed = close(fd) == 0;
    return committed && closed;
}
auto publish(Runtime& r, uint64_t failure_bound = 0) -> bool {
    if (r.row == nullptr) return false;
    r.row[18] = now();
    r.row[25] = r.sequence;
    r.row[124] = guard(r.nonce, r.role, 0);
    r.row[125] = ~r.row[124];
    r.row[126] = r.words * 8;
    r.row[127] = r.capacity;
    r.row[r.words - 1] = 0;
    off_t const offset = r.role == 3 ? 40960 : 8192;
    auto const bound = failure_bound == 0 ? step(r.deadline) : failure_bound;
    if (!io(r.control, r.row, r.words * 8, offset, true, bound) || fsync(r.control) != 0 || !within(bound)) return false;
    r.row[r.words - 1] = r.nonce;
    return io(r.control, &r.row[r.words - 1], 8, offset + static_cast<off_t>((r.words - 1) * 8), true, bound) && fsync(r.control) == 0 &&
           within(bound);
}
auto record_write(int fd, off_t offset, Record value, uint64_t deadline) -> bool {
    auto const commit = value[2] ^ value[4];
    value[63] = 0;
    if (!io(fd, value.data(), sizeof(value), offset, true, deadline) || fsync(fd) != 0 || !within(deadline)) return false;
    return io(fd, const_cast<uint64_t*>(&commit), 8, offset + 504, true, deadline) && fsync(fd) == 0 && within(deadline);
}
auto record_read(int fd, off_t offset, Record& value, uint64_t nonce, unsigned role, uint64_t deadline, uint64_t io_bound) -> bool {
    Record other{};
    if (!io(fd, value.data(), sizeof(value), offset, false, io_bound) || !io(fd, other.data(), sizeof(other), offset, false, io_bound))
        return false;
    if (value != other || value[0] != MAGIC || value[1] != VERSION || value[2] != nonce || value[3] != role || value[14] != deadline ||
        value[12] != guard(nonce, role, value[4]) || value[13] != ~value[12] || value[63] != (nonce ^ value[4]))
        return false;
    for (size_t i = 18; i < 63; ++i)
        if (value[i] != 0) return false;
    return value[17] < deadline;
}
auto progress(Runtime& r, State state, int64_t result = 0, Payload p = {}, uint64_t failure_bound = 0) -> bool {
    Record value{};
    value[0] = MAGIC;
    value[1] = VERSION;
    value[2] = r.nonce;
    value[3] = r.role;
    value[4] = r.sequence;
    value[5] = r.operation;
    value[6] = state;
    value[7] = bits(result);
    value[8] = p.marker;
    value[9] = p.complement;
    value[10] = r.phase;
    value[11] = r.round;
    value[12] = guard(r.nonce, r.role, r.sequence);
    value[13] = ~value[12];
    value[14] = r.deadline;
    value[15] = r.row[15];
    value[16] = r.row[16];
    value[17] = now();
    return record_write(r.control, r.role == 3 ? 1536 : 1024, value, failure_bound == 0 ? step(r.deadline) : failure_bound);
}
[[noreturn]] void leave(int code) {
    (void)proc(0, static_cast<uint64_t>(code));
    for (;;) {
    }
}
[[noreturn]] void fail(Runtime& r, uint64_t code) {
    if (r.row != nullptr) {
        ++r.row[21];
        if (r.row[22] == 0) r.row[22] = code;
        auto const bound = step(UINT64_MAX);
        (void)publish(r, bound);
        (void)progress(r, FAIL, -EIO, {}, bound);
    }
    // Raw EXIT does not unwind mappings/Files. The owned Host init-kill/reap
    // path and kernel namespace retirement are the explicit failure backstop.
    leave(80);
}
void demand(Runtime& r, bool good, uint64_t code) {
    if (!good || !within(r.deadline)) fail(r, code);
}
void semantic(Runtime& r, bool good, uint64_t phase) {
    r.row[23] |= phase;
    if (!good) r.row[24] |= phase;
}
auto emit(Runtime& r, Event op, uint64_t a = 0, uint64_t b = 0, int64_t result = 0, Payload actual = {}, uint64_t expected = 0) -> bool {
    auto const t = now();
    uint64_t previous{};
    if (r.row[20] != 0 && r.row[20] <= r.capacity) {
        auto const* last = r.row + 128 + (r.row[20] - 1) * 12;
        previous = last[last[0] == E_FDS ? 3 : 4];
    }
    if (r.row[20] >= r.capacity || t == UINT64_MAX || t < previous || t >= r.deadline) {
        if (r.role != 0) fail(r, 1);
        ++r.row[21];
        if (r.row[22] == 0) r.row[22] = 1;
        return false;  // The Host must retain its exact owned-root cleanup path.
    }
    auto* e = r.row + 128 + r.row[20] * 12;
    e[0] = op;
    e[1] = r.phase;
    e[2] = r.row[20] + 1;
    e[3] = r.round;
    e[4] = t;
    e[5] = a;
    e[6] = b;
    e[7] = bits(result);
    e[8] = actual.marker;
    e[9] = actual.complement;
    e[10] = expected;
    e[11] = r.sequence;
    ++r.row[20];
    return true;
}
void observe(Runtime& r, uint64_t address, uint64_t expected, bool compare = true) {
    auto const actual = read_payload(address);
    emit(r, E_READ, address, 16, 0, actual, expected);
    if (compare && r.phase != 0) {
        auto const p = payload(expected);
        semantic(r, actual.marker == p.marker && actual.complement == p.complement, r.phase);
    }
}
auto positional(Runtime& r, int fd, uint64_t offset, uint64_t expected, bool writing, bool denied = false) -> Payload {
    auto const sentinel = guard(r.nonce, r.role, r.sequence ^ (writing ? 0x57524954 : 0x52454144));
    Guarded data{sentinel, writing ? payload(expected) : Payload{}, ~sentinel};
    auto const before = now();
    auto const result = raw(5, writing ? 27 : 26, static_cast<uint64_t>(fd), reinterpret_cast<uint64_t>(&data.value), 16, offset);
    emit(r, writing ? E_PWRITE : E_PREAD, static_cast<uint64_t>(fd), offset, result, data.value, expected);
    bool const guards = data.before == sentinel && data.after == ~sentinel;
    emit(r, E_GUARDS, data.before, data.after, guards ? 0 : -EOVERFLOW, {}, sentinel);
    demand(r, guards && now() >= before && (denied ? result == -EBADF || result == 16 : result == 16), 3);
    if (denied) {
        auto const original = payload(expected);
        semantic(r, result == -EBADF && data.value.marker == original.marker && data.value.complement == original.complement, PERMISSIONS);
    } else if (!writing && r.phase != 0) {
        auto const p = payload(expected);
        semantic(r, data.value.marker == p.marker && data.value.complement == p.complement, r.phase);
    }
    return data.value;
}
auto map_page(Runtime& r, unsigned page, int fd, int prot, int flags) -> uint64_t {
    auto const result = raw(8, 0, PAGE, static_cast<uint64_t>(prot), static_cast<uint64_t>(flags), static_cast<uint64_t>(fd), page * PAGE);
    emit(r, E_MAP, static_cast<uint64_t>(fd), page * PAGE, result);
    demand(r, result > 0 && (static_cast<uint64_t>(result) & (PAGE - 1)) == 0, 4);
    r.maps[page] = static_cast<uint64_t>(result);
    r.row[101 + page] = r.maps[page];
    return r.maps[page];
}
void unmap(Runtime& r, unsigned page) {
    if (r.maps[page] == 0) return;
    auto const address = r.maps[page];
    auto const result = raw(7, 1, address, PAGE);
    emit(r, E_UNMAP, address, PAGE, result);
    demand(r, result == 0, 5);
    r.maps[page] = 0;
}
void sync(Runtime& r, unsigned page) {
    auto const result = raw(7, 4, r.maps[page], PAGE, 0);
    emit(r, E_SYNC, r.maps[page], PAGE, result);
    demand(r, result == 0, 6);
}
void store(Runtime& r, unsigned page, uint64_t value) {
    write_payload(r.maps[page], value);
    auto const actual = read_payload(r.maps[page]);
    emit(r, E_STORE, r.maps[page], 16, 0, actual, value);
    auto const p = payload(value);
    semantic(r, actual.marker == p.marker && actual.complement == p.complement, r.phase);
}
void attest(Runtime& r) {
    demand(r, r.row != nullptr, 7);
    auto* h = r.row;
    h[0] = MAGIC;
    h[1] = VERSION;
    h[2] = r.role + 1;
    h[3] = r.nonce;
    h[4] = r.role;
    h[5] = bits(proc(3));
    h[6] = bits(proc(4));
    for (size_t i = 0; i < 4; ++i) h[7 + i] = bits(proc(10 + i));
    h[11] = bits(proc(36, 39));
    h[12] = bits(proc(36, 0x574f5301));
    h[13] = bits(proc(36, 0x574f5304));
    h[17] = now();
    h[19] = r.deadline;
    demand(r,
           h[5] == (r.role == 3 ? 2U : 1U) && h[6] == (r.role == 3 ? 1U : 0U) && h[7] == 100 && h[8] == 100 && h[9] == 100 &&
               h[10] == 100 && h[11] == 1 && h[12] == 0 && h[13] == 256,
           8);
    Request other{};
    auto const bound = step(r.deadline);
    demand(r,
           io(9, r.request.data(), sizeof(r.request), 4096, false, bound) && io(9, other.data(), sizeof(other), 4096, false, bound) &&
               r.request == other && r.request[0] == MAGIC && r.request[1] == VERSION && r.request[2] == r.nonce &&
               r.request[3] == (r.role == 3 ? 1U : r.role) && r.request[5] == r.deadline && r.request[8] == PAGE * 4 &&
               r.request[126] == guard(r.nonce, r.role == 3 ? 1 : r.role, 0x52514553) && r.request[127] == ~r.request[126],
           9);
    for (size_t i = 9; i < 126; ++i)
        if (i < 32 || i >= 64) demand(r, r.request[i] == 0, 9);
    demand(r, r.request[4] > 2, 9);
    h[14] = r.request[4];
    for (size_t i = 0; i < 16; ++i) {
        wire::Limit limit{};
        auto const result = proc(48, i, reinterpret_cast<uint64_t>(&limit), 1);
        h[32 + i * 2] = limit.soft;
        h[33 + i * 2] = limit.hard;
        h[85 + i] = bits(result);
        demand(r, result == 0 && limit.soft == r.request[32 + i * 2] && limit.hard == r.request[33 + i * 2] && limit.soft <= limit.hard,
               10);
    }
    h[105] = 3;
    for (unsigned base = 0; base < 256; base += 8) {
        demand(r, h[20] < r.capacity, 1);
        auto* e = h + 128 + h[20] * 12;
        e[0] = E_FDS;
        e[1] = base;
        e[2] = h[20] + 1;
        e[3] = now();
        for (unsigned i = 0; i < 8; ++i) {
            auto const fd = base + i;
            auto const result = raw(5, 28, fd, 1);
            e[4 + i] = bits(result);
            if (result != (fd == 9 || fd == 10 || fd == 11 ? 0 : -EBADF)) ++h[106];
        }
        ++h[20];
    }
    demand(r, h[106] == 0, 11);
    for (unsigned i = 0; i < 3; ++i) {
        int const fd = static_cast<int>(i) + 9;
        struct stat s{};
        auto const getfd = raw(5, 28, fd, 1), getfl = raw(5, 28, fd, 3), result = raw(5, 13, fd, reinterpret_cast<uint64_t>(&s));
        auto* entry = h + 64 + i * 7;
        entry[0] = static_cast<uint64_t>(fd);
        entry[1] = bits(getfd);
        entry[2] = bits(getfl);
        entry[3] = bits(result);
        entry[4] = s.st_dev;
        entry[5] = s.st_ino;
        entry[6] = static_cast<uint64_t>(s.st_size);
        demand(r,
               getfd == 0 && getfl >= 0 && (getfl & 3) == (i == 2 ? 0 : 2) && result == 0 && S_ISREG(s.st_mode) &&
                   (i == 0 ? s.st_size == static_cast<off_t>(CONTROL_BYTES)
                           : s.st_dev == r.request[6] && s.st_ino == r.request[7] && s.st_size == static_cast<off_t>(PAGE * 4)),
               12);
    }
    int const opened = open(RESULTS, O_RDONLY | O_CLOEXEC);
    auto const opened_raw = opened < 0 ? -errno : opened;
    h[107] = bits(opened_raw);
    emit(r, E_OPEN, 0, 0, opened_raw);
    if (opened >= 0) (void)close(opened);
    demand(r, opened_raw == -ENOENT, 13);
}
void cleanup_maps(Runtime& r) {
    for (unsigned i = 0; i < 4; ++i) unmap(r, i);
    for (int fd : std::array{10, 11}) {
        auto const result = raw(5, 3, static_cast<uint64_t>(fd));
        emit(r, E_CLOSE, fd, 0, result);
        r.row[108 + fd - 10] = bits(result);
        demand(r, result == 0, 14);
    }
}
auto next(Runtime& r, Record& request) -> bool {
    auto const bound = step(r.deadline);
    while (within(bound)) {
        if (record_read(r.control, r.role == 3 ? 512 : 0, request, r.nonce, r.role, r.deadline, bound)) {
            if (request[4] <= r.sequence) {
                pause();
                continue;
            }
            demand(r, request[4] == r.sequence + 1 && request[6] == COMMAND && request[5] >= HELLO && request[5] <= RETAINED, 15);
            r.sequence = request[4];
            r.operation = request[5];
            r.phase = request[10];
            r.round = request[11];
            r.row[15] = request[15];
            r.row[16] = request[16];
            return true;
        }
        pause();
    }
    return false;
}
[[noreturn]] void serve(Runtime& r) {
    Record command{};
    while (next(r, command)) {
        uint64_t const expected = command[7];
        Payload answer{};
        int64_t result{};
        switch (r.operation) {
            case HELLO:
                attest(r);
                r.row[15] = command[15];
                r.row[16] = command[16];
                if (r.role != 3) {
                    map_page(r, 0, 10, PROT_READ | PROT_WRITE, MAP_SHARED);
                    observe(r, r.maps[0], 0);
                    map_page(r, 2, 11, PROT_READ | PROT_WRITE, MAP_PRIVATE);
                    observe(r, r.maps[2], 0);
                    if (r.role == 1) {
                        map_page(r, 1, 10, PROT_READ | PROT_WRITE, MAP_SHARED);
                        observe(r, r.maps[1], 0);
                    }
                }
                break;
            case RETAINED:
                demand(r, command[7] == 1, 16);
                semantic(r, true, PERMISSIONS);
                break;
            case STORE0:
                store(r, 0, expected);
                answer = read_payload(r.maps[0]);
                break;
            case SYNC0:
                sync(r, 0);
                break;
            case READ0:
                observe(r, r.maps[0], expected);
                answer = read_payload(r.maps[0]);
                break;
            case PREAD0:
                answer = positional(r, 10, 0, expected, false);
                break;
            case PWRITE0:
                answer = positional(r, 10, 0, expected, true);
                break;
            case REMAP0:
                unmap(r, 0);
                map_page(r, 0, 10, PROT_READ | PROT_WRITE, MAP_SHARED);
                observe(r, r.maps[0], expected);
                answer = read_payload(r.maps[0]);
                break;
            case STORE_PRIVATE:
                store(r, 2, expected);
                if (r.role != 3) sync(r, 2);
                answer = read_payload(r.maps[2]);
                break;
            case READ_PRIVATE:
                observe(r, r.maps[2], 0);
                answer = positional(r, 10, 2 * PAGE, 0, false);
                break;
            case FORK: {
                demand(r, r.role == 1 && r.maps[1] != 0, 17);
                auto const child = proc(5);
                if (child == 0) {
                    // A real copied user tree already contains all materialized pages.
                    // Reset only private evidence; preserve the inherited actual VAs.
                    Runtime child_runtime(3, r.nonce, r.deadline);
                    child_runtime.maps = r.maps;
                    for (unsigned i = 0; i < 4; ++i)
                        if (child_runtime.row) child_runtime.row[101 + i] = r.maps[i];
                    if (child_runtime.row == nullptr) leave(80);
                    serve(child_runtime);
                }
                emit(r, E_FORK, 0, 0, child);
                r.row[26] = bits(child);
                demand(r, child == 2, 18);
                result = child;
                break;
            }
            case CHILD_WAIT: {
                demand(r, r.role == 3 && r.row[30] < 16 && r.maps[1] != 0, 19);
                demand(r, progress(r, READY), 20);
                auto const t = now();
                auto const bound = t > UINT64_MAX - STEP_US ? 0 : std::min(t + STEP_US, r.deadline);
                demand(r, bound > t, 21);
                auto const remaining = bound - t;
                timespec timeout{static_cast<time_t>(remaining / 1000000), static_cast<long>((remaining % 1000000) * 1000)};
                result = ker::futex::wait(reinterpret_cast<int*>(r.maps[1] + 64), 0, &timeout);
                ++r.row[30];
                answer = read_payload(r.maps[1]);
                emit(r, E_WAIT, r.maps[1] + 64, remaining, result, answer, expected);
                demand(r, result == 0, 22);
                auto const p = payload(expected);
                semantic(r, answer.marker == p.marker && answer.complement == p.complement, SHARED_FORK);
                break;
            }
            case WAKE: {
                demand(r, r.role == 1, 23);
                write_payload(r.maps[1], expected);
                auto const bound = step(r.deadline);
                do {
                    demand(r, r.row[29] < 128 && within(bound), 24);
                    result = ker::futex::wake(reinterpret_cast<int*>(r.maps[1] + 64), 1);
                    ++r.row[29];
                    emit(r, E_WAKE, r.maps[1] + 64, 1, result, read_payload(r.maps[1]), expected);
                    demand(r, result == 0 || result == 1, 25);
                    if (result == 1) break;
                    pause();
                } while (true);
                semantic(r, result == 1, SHARED_FORK);
                break;
            }
            case READ_PARENT_PRIVATE:
                demand(r, r.role == 1, 26);
                observe(r, r.maps[2], expected);
                answer = read_payload(r.maps[2]);
                break;
            case EXIT_CHILD:
                demand(r, r.role == 3, 27);
                cleanup_maps(r);
                demand(r, publish(r) && progress(r, DONE), 28);
                demand(r, close(9) == 0, 29);
                leave(0);
            case WAIT_CHILD: {
                demand(r, r.role == 1 && r.row[26] == 2, 30);
                int status = -1;
                auto const bound = step(r.deadline);
                r.row[112] = now();
                do {
                    result = proc(2, 2, reinterpret_cast<uint64_t>(&status), 1);
                    if (result == 0 || result == -EINTR)
                        pause();
                    else
                        break;
                } while (within(bound));
                r.row[113] = now();
                r.row[27] = bits(result);
                r.row[28] = bits(status);
                emit(r, E_WAITPID, 2, bits(status), result);
                demand(r, result == 2 && status == 0 && r.row[113] < bound, 31);
                break;
            }
            case BASELINE_PROMO:
                demand(r, r.role == 2, 32);
                map_page(r, 3, 11, PROT_READ, MAP_SHARED);
                observe(r, r.maps[3], 0);
                break;
            case PROMOTE:
                demand(r, r.role == 1, 33);
                map_page(r, 3, 10, PROT_READ, MAP_PRIVATE);
                observe(r, r.maps[3], 0);
                result = raw(7, 2, r.maps[3], PAGE, PROT_READ | PROT_WRITE);
                emit(r, E_PROTECT, r.maps[3], PROT_READ | PROT_WRITE, result);
                demand(r, result == 0, 34);
                store(r, 3, expected);
                break;
            case READ_PROMO:
                observe(r, r.maps[3], 0);
                answer = positional(r, 10, 3 * PAGE, 0, false);
                break;
            case RIGHTS: {
                demand(r, r.role == 2 && r.maps[3] != 0, 35);
                auto const old = read_payload(r.maps[3]);
                emit(r, E_READ, r.maps[3], 16, 0, old, old.marker);
                (void)positional(r, 11, 3 * PAGE, expected, true, true);
                auto const mapped = raw(8, 0, PAGE, PROT_READ | PROT_WRITE, MAP_SHARED, 11, 3 * PAGE);
                emit(r, E_MAP, 11, 3 * PAGE, mapped);
                semantic(r, mapped == -EACCES, PERMISSIONS);
                demand(r, mapped == -EACCES || mapped > 0, 36);
                if (mapped > 0) {
                    auto const removed = raw(7, 1, static_cast<uint64_t>(mapped), PAGE);
                    emit(r, E_UNMAP, bits(mapped), PAGE, removed);
                    demand(r, removed == 0, 37);
                }
                auto const protected_result = raw(7, 2, r.maps[3], PAGE, PROT_READ | PROT_WRITE);
                emit(r, E_PROTECT, r.maps[3], PROT_READ | PROT_WRITE, protected_result);
                semantic(r, protected_result == -EACCES, PERMISSIONS);
                demand(r, protected_result == 0 || protected_result == -EACCES, 38);
                auto const current = read_payload(r.maps[3]);
                emit(r, E_READ, r.maps[3], 16, 0, current, old.marker);
                semantic(r, current.marker == old.marker && current.complement == old.complement, PERMISSIONS);
                break;
            }
            case STOP_ROOT:
                demand(r, r.role == 1 || r.role == 2, 39);
                cleanup_maps(r);
                demand(r, publish(r) && progress(r, DONE), 40);
                demand(r, close(9) == 0, 41);
                leave(0);
            default:
                fail(r, 42);
        }
        demand(r, progress(r, DONE, result, answer), 43);
    }
    fail(r, 44);
}
auto number(const char* value, uint64_t& result) -> bool {
    result = 0;
    for (unsigned i = 0; i < 21; ++i) {
        char const c = value[i];
        if (c == '\0') return i != 0;
        if (c < '0' || c > '9' || result > (UINT64_MAX - static_cast<uint64_t>(c - '0')) / 10) return false;
        result = result * 10 + static_cast<uint64_t>(c - '0');
    }
    return false;
}
auto text(const char* s) -> wire::String { return {reinterpret_cast<uint64_t>(s), std::strlen(s)}; }
auto issue(Runtime& host, Tree& tree, bool child, Command op, uint64_t value = 0, uint64_t phase = 0, uint64_t round = 0) -> uint64_t {
    auto& sequence = child ? tree.child_seq : tree.root_seq;
    ++sequence;
    Record request{};
    auto const role = child ? 3U : tree.role;
    request[0] = MAGIC;
    request[1] = VERSION;
    request[2] = host.nonce;
    request[3] = role;
    request[4] = sequence;
    request[5] = op;
    request[6] = COMMAND;
    request[7] = value;
    request[10] = phase;
    request[11] = round;
    request[12] = guard(host.nonce, role, sequence);
    request[13] = ~request[12];
    request[14] = host.deadline;
    request[15] = static_cast<uint64_t>(tree.root);
    request[16] = tree.child > 2 ? static_cast<uint64_t>(tree.child) : 0;
    request[17] = now();
    return record_write(tree.fd, child ? 512 : 0, request, step(host.deadline)) ? sequence : 0;
}
auto await(Runtime& host, Tree& tree, bool child, uint64_t seq, Command op, State state, Record* result = nullptr) -> bool {
    auto const bound = step(host.deadline);
    Record value{};
    while (within(bound)) {
        if (record_read(tree.fd, child ? 1536 : 1024, value, host.nonce, child ? 3U : tree.role, host.deadline, bound) && value[4] == seq) {
            if (value[5] != op || value[6] == FAIL) return false;
            if (value[6] == state) {
                if (result) *result = value;
                return true;
            }
        }
        pause();
    }
    return false;
}
auto command(Runtime& host, Tree& tree, bool child, Command op, uint64_t value = 0, uint64_t phase = 0, uint64_t round = 0) -> bool {
    auto const seq = issue(host, tree, child, op, value, phase, round);
    return seq != 0 && await(host, tree, child, seq, op, DONE);
}
auto field(const char* source, const char* name) -> const char* {
    auto const length = std::strlen(name);
    for (auto* p = source; *p;) {
        if (std::strncmp(p, name, length) == 0 && p[length] == ':') {
            p += length + 1;
            while (*p == ' ' || *p == '\t') ++p;
            return p;
        }
        p = std::strchr(p, '\n');
        if (p == nullptr) break;
        ++p;
    }
    return nullptr;
}
auto field_number(const char* source, const char* name, uint64_t wanted) -> bool {
    auto* p = field(source, name);
    if (!p) return false;
    uint64_t value{};
    unsigned digits{};
    while (*p >= '0' && *p <= '9') {
        if (++digits > 20 || value > (UINT64_MAX - static_cast<unsigned>(*p - '0')) / 10) return false;
        value = value * 10 + static_cast<unsigned>(*p++ - '0');
    }
    return digits != 0 && value == wanted && (*p == '\n' || *p == '\t' || *p == ' ' || *p == '\0');
}
auto field_text(const char* source, const char* name, const char* wanted) -> bool {
    auto* p = field(source, name);
    auto const n = std::strlen(wanted);
    return p && std::strncmp(p, wanted, n) == 0 && (p[n] == '\n' || p[n] == '\0');
}
auto uid100(const char* source) -> bool {
    auto* p = field(source, "Uid");
    if (!p) return false;
    for (unsigned i = 0; i < 4; ++i) {
        while (*p == ' ' || *p == '\t') ++p;
        if (std::strncmp(p, "100", 3) != 0 || (p[3] != '\t' && p[3] != ' ' && p[3] != '\n')) return false;
        p += 3;
    }
    return *p == '\n' || *p == '\0';
}
auto status_text(int64_t pid, std::array<char, 4097>& buffer, size_t& count, uint64_t deadline) -> bool {
    char p[96]{};
    std::snprintf(p, sizeof(p), "/proc/%lld/status", static_cast<long long>(pid));
    int const fd = open(p, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    count = 0;
    unsigned interrupted{};
    bool good = true;
    while (count < buffer.size() && within(deadline)) {
        auto const got = read(fd, buffer.data() + count, buffer.size() - count);
        if (got < 0 && errno == EINTR && ++interrupted <= 256) continue;
        if (got < 0) {
            good = false;
            break;
        }
        if (got == 0) break;
        count += static_cast<size_t>(got);
    }
    bool const closed = close(fd) == 0;
    if (!good || !closed || count == 0 || count > 4096 || !within(deadline)) return false;
    buffer[count] = '\0';
    return true;
}
auto child_identity(const char* source, int64_t child, int64_t root) -> bool {
    return field_number(source, "Pid", static_cast<uint64_t>(child)) && field_number(source, "Tgid", static_cast<uint64_t>(child)) &&
           field_number(source, "PPid", static_cast<uint64_t>(root)) && uid100(source) && field_text(source, "Name", "file-probe");
}
auto discover(Runtime& host, Tree& tree) -> bool {
    auto const bound = step(host.deadline);
    while (within(bound)) {
        DIR* directory = opendir("/proc");
        if (!directory) return false;
        unsigned entries{}, matches{};
        int64_t selected = -1;
        std::array<char, 4097> chosen{};
        size_t chosen_bytes{};
        while (auto* entry = readdir(directory)) {
            if (++entries > 4096 || !within(bound)) break;
            uint64_t candidate{};
            if (!number(entry->d_name, candidate) || candidate <= 2 || candidate > INT64_MAX) continue;
            std::array<char, 4097> data{};
            size_t bytes{};
            if (status_text(static_cast<int64_t>(candidate), data, bytes, bound) &&
                child_identity(data.data(), static_cast<int64_t>(candidate), tree.root)) {
                ++matches;
                selected = static_cast<int64_t>(candidate);
                chosen = data;
                chosen_bytes = bytes;
            }
        }
        bool const closed = closedir(directory) == 0;
        if (!closed || entries > 4096 || matches > 1) return false;
        if (matches == 1 && proc(8, static_cast<uint64_t>(selected), 0) == 0) {
            tree.child = selected;
            return save("proc-child-discovered.txt", chosen.data(), chosen_bytes, bound);
        }
        pause();
    }
    return false;
}
auto park(Runtime& host, Tree& tree, unsigned round) -> bool {
    auto const bound = step(host.deadline);
    while (within(bound)) {
        std::array<char, 4097> data{};
        size_t bytes{};
        if (proc(8, static_cast<uint64_t>(tree.child), 0) == 0 && status_text(tree.child, data, bytes, bound) &&
            child_identity(data.data(), tree.child, tree.root) && field_text(data.data(), "SchedQueue", "WAITING") &&
            field_text(data.data(), "Wchan", "futex_wait") && proc(8, static_cast<uint64_t>(tree.child), 0) == 0) {
            host.phase = SHARED_FORK;
            host.round = round;
            host.sequence = tree.child_seq;
            if (!emit(host, E_PARK, static_cast<uint64_t>(tree.child), static_cast<uint64_t>(tree.root), 0)) return false;
            if (round == 0 || round == 15)
                return save(round == 0 ? "proc-child-first-wait.txt" : "proc-child-last-wait.txt", data.data(), bytes, bound);
            return true;
        }
        pause();
    }
    return false;
}
auto create(Runtime& host, Tree& tree, int rw, int ro, struct stat const& backing) -> bool {
    char path[160]{};
    if (!filename(path, sizeof(path), tree.role == 1 ? "control-a.bin" : "control-b.bin")) return false;
    tree.fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (tree.fd < 0 || ftruncate(tree.fd, CONTROL_BYTES) != 0) return false;
    Request request{};
    request[0] = MAGIC;
    request[1] = VERSION;
    request[2] = host.nonce;
    request[3] = tree.role;
    request[4] = host.row[5];
    request[5] = host.deadline;
    request[6] = backing.st_dev;
    request[7] = backing.st_ino;
    request[8] = static_cast<uint64_t>(backing.st_size);
    wire::Spec specification{};
    for (size_t i = 0; i < 16; ++i) {
        wire::Limit limit{};
        auto const result = proc(48, i, reinterpret_cast<uint64_t>(&limit), 1);
        if (result != 0) return false;
        specification.limits[i] = limit;
        request[32 + i * 2] = limit.soft;
        request[33 + i * 2] = limit.hard;
    }
    request[126] = guard(host.nonce, tree.role, 0x52514553);
    request[127] = ~request[126];
    if (!io(tree.fd, request.data(), sizeof(request), 4096, true, step(host.deadline)) || fsync(tree.fd) != 0) return false;
    std::array<const char*, 3> source{BINARY, "/lib/ld.so", "/usr/lib/libc.so"};
    std::array<const char*, 3> destination{PRIVATE, "/lib/ld.so", "/usr/lib/libc.so"};
    std::array<wire::Asset, 3> assets{};
    bool good = true;
    for (size_t i = 0; i < 3; ++i) {
        assets[i] = {open(source[i], O_RDONLY | O_CLOEXEC), i < 2 ? 0755U : 0644U, text(destination[i]), 0};
        good = assets[i].source_fd >= 0 && good;
    }
    std::array<wire::Descriptor, 3> descriptors{{{tree.fd, 9, 0}, {rw, 10, 0}, {ro, 11, 0}}};
    char n[24]{}, d[24]{};
    std::snprintf(n, sizeof(n), "%llu", static_cast<unsigned long long>(host.nonce));
    std::snprintf(d, sizeof(d), "%llu", static_cast<unsigned long long>(host.deadline));
    std::array args{text(PRIVATE), text("--service"), text(tree.role == 1 ? "1" : "2"), text(n), text(d)};
    std::array env{text("LD_LIBRARY_PATH=/usr/lib")};
    specification.executable = args[0];
    specification.assets = reinterpret_cast<uint64_t>(assets.data());
    specification.asset_count = assets.size();
    specification.descriptors = reinterpret_cast<uint64_t>(descriptors.data());
    specification.descriptor_count = descriptors.size();
    specification.argv = reinterpret_cast<uint64_t>(args.data());
    specification.argc = args.size();
    specification.envp = reinterpret_cast<uint64_t>(env.data());
    specification.envc = env.size();
    auto const result = good && within(host.deadline) ? proc(54, reinterpret_cast<uint64_t>(&specification)) : -EIO;
    if (result > 0) tree.root = result;  // Exact ownership precedes every later close/clock.
    for (auto const& asset : assets)
        if (asset.source_fd >= 0) good = close(asset.source_fd) == 0 && good;
    return good && result > 2;
}
auto reap(Runtime& host, Tree& tree, uint64_t deadline, bool clean) -> bool {
    int status = -1;
    int64_t result{};
    auto const start = now();
    while (tree.root > 0 && !tree.consumed && within(deadline)) {
        result = proc(2, static_cast<uint64_t>(tree.root), reinterpret_cast<uint64_t>(&status), 1);
        if (result == tree.root || result == -ECHILD) {
            tree.consumed = true;
            break;
        }
        if (result != 0 && result != -EINTR) break;
        pause();
    }
    if (clean) {
        auto const base = tree.role == 1 ? 114U : 117U;
        host.row[base] = static_cast<uint64_t>(tree.root);
        host.row[base + 1] = bits(result);
        host.row[base + 2] = bits(status);
        auto const timebase = tree.role == 1 ? 120U : 122U;
        host.row[timebase] = start;
        host.row[timebase + 1] = now();
        host.sequence = tree.root_seq;
        host.phase = 0;
        host.round = 0;
        if (!emit(host, E_WAITPID, static_cast<uint64_t>(tree.root), bits(status), result)) return false;
    }
    return tree.consumed && result == tree.root && (clean ? status == 0 : true) && within(deadline);
}
auto archive(Runtime& host, Tree& tree, bool child) -> bool {
    size_t const words = child ? SMALL_WORDS : ROOT_WORDS;
    auto* first = static_cast<uint64_t*>(std::calloc(words, 8));
    auto* second = static_cast<uint64_t*>(std::calloc(words, 8));
    if (!first || !second) {
        std::free(first);
        std::free(second);
        return false;
    }
    off_t const offset = child ? 40960 : 8192;
    auto const bound = step(host.deadline);
    bool const valid =
        io(tree.fd, first, words * 8, offset, false, bound) && io(tree.fd, second, words * 8, offset, false, bound) &&
        std::memcmp(first, second, words * 8) == 0 && first[0] == MAGIC && first[1] == VERSION && first[3] == host.nonce &&
        first[4] == (child ? 3U : tree.role) && first[5] == (child ? 2U : 1U) && first[6] == (child ? 1U : 0U) &&
        first[15] == static_cast<uint64_t>(tree.root) && first[16] == (tree.child > 2 ? static_cast<uint64_t>(tree.child) : 0) &&
        first[2] == (child ? 4U : tree.role + 1) && first[19] == host.deadline && first[20] <= (child ? 64U : 320U) && first[21] == 0 &&
        first[18] >= first[17] && first[18] < host.deadline && first[124] == guard(host.nonce, child ? 3U : tree.role, 0) &&
        first[125] == ~first[124] && first[126] == words * 8 && first[127] == (child ? 64U : 320U) && first[words - 1] == host.nonce;
    if (valid) {
        host.row[23] |= first[23];
        host.row[24] |= first[24];
    }
    bool const saved = valid && save(child ? "child-a.bin" : tree.role == 1 ? "root-a.bin" : "root-b.bin", first, words * 8, bound);
    std::free(first);
    std::free(second);
    return saved;
}
auto host_pread(Runtime& r, int fd, uint64_t offset, uint64_t expected) -> bool {
    auto const sentinel = guard(r.nonce, 0, r.sequence ^ 0x52454144);
    Guarded data{sentinel, {}, ~sentinel};
    auto const before = now();
    auto const result = raw(5, 26, static_cast<uint64_t>(fd), reinterpret_cast<uint64_t>(&data.value), 16, offset);
    bool const guards = data.before == sentinel && data.after == ~sentinel;
    bool const first = emit(r, E_PREAD, static_cast<uint64_t>(fd), offset, result, data.value, expected);
    bool const second = emit(r, E_GUARDS, data.before, data.after, guards ? 0 : -EOVERFLOW, {}, sentinel);
    if (!first || !second || !guards || result != 16 || now() < before || !within(r.deadline)) return false;
    auto const wanted = payload(expected);
    semantic(r, data.value.marker == wanted.marker && data.value.complement == wanted.complement, WRITEBACK);
    return true;
}
auto host() -> int {
    auto const start = now();
    if (start == UINT64_MAX || start > UINT64_MAX - RUN_US || proc(10) != 100 || proc(11) != 100 || proc(12) != 100 || proc(13) != 100)
        return 81;
    auto const pid = bits(proc(3)), nonce = (start ^ (pid << 32) ^ GUARD) | (uint64_t{1} << 63) | 1;
    Runtime r(0, nonce, start + RUN_US);
    if (!r.row) return 81;
    r.row[0] = MAGIC;
    r.row[1] = VERSION;
    r.row[2] = 1;
    r.row[3] = nonce;
    r.row[5] = pid;
    r.row[6] = bits(proc(4));
    r.row[14] = pid;
    r.row[17] = start;
    r.row[19] = r.deadline;
    for (size_t i = 0; i < 4; ++i) r.row[7 + i] = bits(proc(10 + i));
    r.row[11] = bits(proc(36, 39));
    r.row[12] = bits(proc(36, 0x574f5301));
    r.row[13] = bits(proc(36, 0x574f5304));
    if (pid <= 2 || r.row[11] != 1 || r.row[12] != 0 || r.row[13] != 256) return 81;
    for (size_t i = 0; i < 16; ++i) {
        wire::Limit v{};
        auto const ret = proc(48, i, reinterpret_cast<uint64_t>(&v), 1);
        r.row[85 + i] = bits(ret);
        r.row[32 + i * 2] = v.soft;
        r.row[33 + i * 2] = v.hard;
        if (ret != 0 || v.soft > v.hard) return 81;
    }
    for (unsigned fd = 0; fd < 256; ++fd) {
        auto const ret = raw(5, 28, fd, 1);
        if (ret >= 0 && close(static_cast<int>(fd)) != 0) return 81;
        if (ret < 0 && ret != -EBADF) return 81;
    }
    struct stat result_directory{};
    if (stat(RESULTS, &result_directory) != 0 || !S_ISDIR(result_directory.st_mode) || (result_directory.st_mode & 07777U) != 0700U ||
        result_directory.st_uid != 100 || result_directory.st_gid != 100)
        return 81;
    char path[160]{};
    if (!filename(path, sizeof(path), "backing.bin")) return 81;
    int rw = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600), ro = -1;
    std::array<Tree, 2> trees{{{1}, {2}}};
    struct stat backing{}, readonly{};
    bool complete = false;
    do {
        if (rw < 0 || ftruncate(rw, PAGE * 4) != 0) break;
        std::array<uint8_t, PAGE * 4> zero{};
        if (!io(rw, zero.data(), zero.size(), 0, true, step(r.deadline)) || fsync(rw) != 0) break;
        ro = open(path, O_RDONLY | O_CLOEXEC);
        if (ro < 0 || fstat(rw, &backing) != 0 || fstat(ro, &readonly) != 0 || !S_ISREG(backing.st_mode) ||
            backing.st_size != static_cast<off_t>(PAGE * 4) || backing.st_dev != readonly.st_dev || backing.st_ino != readonly.st_ino ||
            readonly.st_size != backing.st_size)
            break;
        r.row[110] = static_cast<uint64_t>(rw);
        r.row[111] = static_cast<uint64_t>(ro);
        if (!create(r, trees[0], rw, ro, backing) || !create(r, trees[1], rw, ro, backing) || !command(r, trees[0], false, HELLO) ||
            !command(r, trees[1], false, HELLO))
            break;
        r.phase = PERMISSIONS;
        auto const closed_rw = raw(5, 3, static_cast<uint64_t>(rw));
        r.row[108] = bits(closed_rw);
        bool const recorded_rw = emit(r, E_CLOSE, static_cast<uint64_t>(rw), 0, closed_rw);
        if (closed_rw == 0) rw = -1;
        auto const closed_ro = raw(5, 3, static_cast<uint64_t>(ro));
        r.row[109] = bits(closed_ro);
        bool const recorded_ro = emit(r, E_CLOSE, static_cast<uint64_t>(ro), 0, closed_ro);
        if (closed_ro == 0) ro = -1;
        if (!recorded_rw || !recorded_ro || closed_rw != 0 || closed_ro != 0 || !command(r, trees[0], false, RETAINED, 1, PERMISSIONS) ||
            !command(r, trees[1], false, RETAINED, 1, PERMISSIONS))
            break;
        bool good = true;
        for (unsigned i = 0; good && i < 16; ++i) {
            auto const m = marker(nonce, INDEPENDENT, i), p = marker(nonce, INDEPENDENT, i + 32);
            if (m == p) {
                good = false;
                break;
            }
            good = command(r, trees[0], false, STORE0, m, INDEPENDENT, i) && command(r, trees[1], false, READ0, m, INDEPENDENT, i) &&
                   command(r, trees[1], false, PREAD0, 0, 0, i) && command(r, trees[0], false, SYNC0, 0, WRITEBACK, i) &&
                   command(r, trees[1], false, READ0, m, INDEPENDENT, i) && command(r, trees[1], false, PREAD0, m, WRITEBACK, i) &&
                   command(r, trees[0], false, PWRITE0, p, WRITEBACK, i) && command(r, trees[0], false, READ0, p, INDEPENDENT, i) &&
                   command(r, trees[1], false, READ0, p, INDEPENDENT, i) && command(r, trees[1], false, PREAD0, p, WRITEBACK, i) &&
                   command(r, trees[1], false, REMAP0, p, REMAP, i);
        }
        for (unsigned i = 0; good && i < 16; ++i)
            good = command(r, trees[0], false, STORE_PRIVATE, marker(nonce, ISOLATION, i), ISOLATION, i) &&
                   command(r, trees[1], false, READ_PRIVATE, 0, ISOLATION, i);
        if (!good || !command(r, trees[0], false, FORK) || !discover(r, trees[0]) || !command(r, trees[0], true, HELLO)) break;
        for (unsigned i = 0; good && i < 16; ++i) {
            auto const m = marker(nonce, SHARED_FORK, i);
            auto const seq = issue(r, trees[0], true, CHILD_WAIT, m, SHARED_FORK, i);
            good = seq != 0 && await(r, trees[0], true, seq, CHILD_WAIT, READY) && park(r, trees[0], i) &&
                   command(r, trees[0], false, WAKE, m, SHARED_FORK, i) && await(r, trees[0], true, seq, CHILD_WAIT, DONE);
        }
        if (!good || !command(r, trees[0], true, STORE_PRIVATE, marker(nonce, ISOLATION, 64), ISOLATION, 64) ||
            !command(r, trees[0], false, READ_PARENT_PRIVATE, marker(nonce, ISOLATION, 15), ISOLATION, 15) ||
            !command(r, trees[0], true, EXIT_CHILD) || !command(r, trees[0], false, WAIT_CHILD) ||
            !command(r, trees[1], false, BASELINE_PROMO, 0, PROMOTION) ||
            !command(r, trees[0], false, PROMOTE, marker(nonce, PROMOTION, 0), PROMOTION) ||
            !command(r, trees[1], false, READ_PROMO, 0, PROMOTION) ||
            !command(r, trees[1], false, RIGHTS, marker(nonce, PERMISSIONS, 0), PERMISSIONS) || !command(r, trees[0], false, STOP_ROOT) ||
            !reap(r, trees[0], step(r.deadline), true) || !archive(r, trees[0], false) || !archive(r, trees[0], true))
            break;
        int const observer = open(path, O_RDONLY | O_CLOEXEC);
        struct stat s{};
        if (observer < 0 || fstat(observer, &s) != 0 || !S_ISREG(s.st_mode) || s.st_dev != backing.st_dev || s.st_ino != backing.st_ino ||
            s.st_size != static_cast<off_t>(PAGE * 4)) {
            if (observer >= 0) (void)close(observer);
            break;
        }
        r.phase = WRITEBACK;
        r.round = 15;
        r.sequence = trees[0].root_seq;
        bool const read_ok = host_pread(r, observer, 0, marker(nonce, INDEPENDENT, 47));
        bool const observer_closed = close(observer) == 0;
        if (!read_ok || !observer_closed || !command(r, trees[1], false, STOP_ROOT) || !reap(r, trees[1], step(r.deadline), true) ||
            !archive(r, trees[1], false))
            break;
        complete = r.row[21] == 0;
    } while (false);
    if (!complete) {
        ++r.row[21];
        if (r.row[22] == 0) r.row[22] = 50;
        auto const t = now();
        auto const cleanup_deadline = t > UINT64_MAX - 10'000'000 ? t : t + 10'000'000;
        for (auto& tree : trees)
            if (tree.root > 0 && !tree.consumed) (void)proc(8, static_cast<uint64_t>(tree.root), SIGKILL);
        for (auto& tree : trees)
            if (tree.root > 0 && !tree.consumed) (void)reap(r, tree, cleanup_deadline, false);
    }
    if (rw >= 0 && close(rw) != 0) complete = false;
    if (ro >= 0 && close(ro) != 0) complete = false;
    for (auto& tree : trees)
        if (tree.fd >= 0 && close(tree.fd) != 0) complete = false;
    r.row[18] = now();
    complete = complete && r.row[18] >= start && r.row[18] < r.deadline && r.row[21] == 0;
    r.row[124] = guard(nonce, 0, 0);
    r.row[125] = ~r.row[124];
    r.row[126] = SMALL_WORDS * 8;
    r.row[127] = 64;
    auto const receipt_deadline = step(UINT64_MAX);
    bool const saved = save_committed_row("host.bin", r.row, SMALL_WORDS, nonce, receipt_deadline);
    std::array<char, 512> status{};
    auto const n =
        std::snprintf(status.data(), status.size(), "%s compartment-filemap nonce=%llu semantic-fail-mask=%llu events=%llu failures=%llu\n",
                      complete && saved ? "CHARACTERIZED_COMPLETE" : "EXECUTION_FAIL", static_cast<unsigned long long>(nonce),
                      static_cast<unsigned long long>(r.row[24]), static_cast<unsigned long long>(r.row[20]),
                      static_cast<unsigned long long>(r.row[21] + (complete && saved ? 0U : 1U)));
    bool const status_saved =
        n > 0 && static_cast<size_t>(n) < status.size() && save("status", status.data(), status.size(), receipt_deadline);
    return complete && saved && status_saved ? 0 : 81;
}
auto witness() -> int {
    namespace init = ker::abi::init_control;
    auto const start = now();
    if (start == UINT64_MAX || start > UINT64_MAX - 30'000'000) return 82;
    auto const deadline = start + 30'000'000;
    init::StatusSnapshot snapshot{};
    init::ServiceStatus target{};
    bool observed{}, history{};
    int64_t error{};
    for (uint64_t op = 10; op <= 13; ++op)
        if (proc(op) != 100) error = -EPERM;
    while (error == 0 && !observed && within(deadline)) {
        struct GuardedSnapshot {
            uint64_t before = GUARD;
            init::StatusSnapshot value{};
            uint64_t after = ~GUARD;
        } value;
        auto const result = ker::process::init_status_read(&value.value);
        if (result != 0 && result != -EAGAIN) {
            error = result;
            break;
        }
        if (value.before != GUARD || value.after != ~GUARD) {
            error = -EOVERFLOW;
            break;
        }
        if (result == 0) {
            snapshot = value.value;
            bool good = snapshot.size == 4160 && snapshot.version == 1 && snapshot.sequence != 0 && snapshot.published_mono_ns != 0 &&
                        snapshot.service_count != 0 && snapshot.service_count <= 16;
            unsigned matches{};
            history = false;
            for (uint32_t i = 0; good && i < snapshot.service_count; ++i) {
                auto const& service = snapshot.services[i];
                good = std::memchr(service.service, '\0', 64) != nullptr && service.history_count <= 4;
                if (good && std::strcmp(service.service, "compartment-filemap-probe") == 0) {
                    target = service;
                    ++matches;
                }
            }
            if (!good || matches > 1) {
                error = -EPROTO;
                break;
            }
            if (matches == 1)
                for (uint32_t i = 0; i < target.history_count; ++i) {
                    auto const& h = target.history[i];
                    if (h.reason == init::TransitionReason::EXIT && h.to_state == init::ServiceState::EXITED && h.timestamp_mono_ns > 0 &&
                        h.timestamp_mono_ns <= snapshot.published_mono_ns)
                        history = true;
                }
            observed = matches == 1 && target.generation == 1 && target.restart_count == 0 && target.state == init::ServiceState::EXITED &&
                       target.readiness == init::ReadinessState::READY && target.pid == -1 && target.pgid == -1 &&
                       target.last_wait_status == 0 && target.last_error == 0 && history;
        }
        if (!observed) pause();
    }
    if (!observed && error == 0) error = -ETIMEDOUT;
    auto const receipt = step(UINT64_MAX);
    bool const saved = save("init-exit-snapshot.bin", &snapshot, sizeof(snapshot), receipt);
    std::array<char, 512> status{};
    bool const success = observed && error == 0 && saved;
    auto const n = std::snprintf(
        status.data(), status.size(),
        "%s compartment-filemap-init-exit service=compartment-filemap-probe generation=%llu restarts=%llu sequence=%llu published-ns=%llu "
        "state=%u readiness=%u wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u read-error=%lld failures=%u\n",
        success ? "PASS" : "FAIL", static_cast<unsigned long long>(target.generation),
        static_cast<unsigned long long>(target.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned long long>(snapshot.published_mono_ns), static_cast<unsigned>(target.state),
        static_cast<unsigned>(target.readiness), target.last_wait_status, target.last_error, history ? 1U : 0U, snapshot.size,
        snapshot.version, static_cast<long long>(error), success ? 0U : 1U);
    bool const durable = n > 0 && static_cast<size_t>(n) < status.size() && save("init-exit-status", status.data(), status.size(), receipt);
    bool const printed = durable && write(STDOUT_FILENO, status.data(), static_cast<size_t>(n)) == n;
    return success && durable && printed ? 0 : 82;
}
}  // namespace
auto main(int argc, char** argv) -> int {
    if (argc == 1) return host();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return witness();
    uint64_t role{}, nonce{}, deadline{};
    if (argc == 5 && std::strcmp(argv[1], "--service") == 0 && number(argv[2], role) && (role == 1 || role == 2) &&
        number(argv[3], nonce) && nonce != 0 && number(argv[4], deadline) && deadline != 0) {
        Runtime runtime(static_cast<unsigned>(role), nonce, deadline);
        if (!runtime.row) return 80;
        serve(runtime);
    }
    return 83;
}
