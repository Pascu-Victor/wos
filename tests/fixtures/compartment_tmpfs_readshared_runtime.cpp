// Public readonly TMPFS mapping regression. Protocol and limits are in the sibling .md.
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/futex.h>
#include <sys/init_control.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <abi/compartment.hpp>
#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
namespace abi = ker::abi::compartment;
constexpr char RESULTS[] = "/srv/tmpfs-readshared-results";
constexpr char BINARY[] = "/usr/libexec/compartment-tmpfs-readshared-runtime";
constexpr char PRIVATE[] = "/app/readshared-probe";
constexpr char BACKING[] = "/backing.bin";
constexpr uint64_t MAGIC = 0x3538524f544d5046ULL, VERSION = 1, GUARD = 0x6d41a8c297e53b0fULL;
constexpr uint64_t PAGE = 4096, LENGTH = 129 * PAGE, RUN_US = 60'000'000, STEP_US = 5'000'000;
constexpr size_t WORDS = 4096, CAPACITY = 256, HEAD = 64, EVENT_WORDS = 12, CONTROL_BYTES = 147456;
constexpr off_t COPY_AT = 4096;
using Record = std::array<uint64_t, 64>;
using Row = std::array<uint64_t, WORDS>;
enum Op : uint64_t {
    GRANT = 1,
    TRUNCATE,
    MAP,
    READ,
    PWRITE,
    COPY,
    PROTECT,
    THREAD_CREATE,
    THREAD_JOIN,
    THREAD_READ,
    THREAD_PROTECT,
    CLOSE,
    DUP,
    FORK,
    READY,
    WAIT,
    WAKE,
    ACK,
    EXEC_INTENT,
    NEW_IMAGE,
    EXIT_INTENT,
    WAITPID,
    UNMAP,
    GROW_RELEASE,
    CREATE,
    REAP,
    PREAD,
    REFUSE,
    COPY_NONE
};
struct Payload {
    uint64_t marker{}, complement{};
};
struct Guarded {
    uint64_t before{};
    Payload payload{};
    uint64_t after{};
};
static_assert(sizeof(Guarded) == 32 && sizeof(Record) == 512 && sizeof(abi::Spec) == 408);
static_assert(sizeof(struct stat) == 144 && std::endian::native == std::endian::little);
static_assert(HEAD + CAPACITY * EVENT_WORDS < WORDS - 1 && 8192 + 4 * WORDS * 8 <= CONTROL_BYTES);
auto bits(int64_t n) -> uint64_t { return std::bit_cast<uint64_t>(n); }
auto raw(uint64_t sub, uint64_t a, uint64_t b = 0, uint64_t c = 0, uint64_t d = 0, uint64_t e = 0, uint64_t f = 0) -> int64_t {
    return std::bit_cast<int64_t>(syscall(sub, a, b, c, d, e, f));
}
auto proc(uint64_t op, uint64_t a = 0, uint64_t b = 0, uint64_t c = 0) -> int64_t { return raw(3, op, a, b, c); }
auto now() -> uint64_t {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t) || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= 1'000'000'000) return UINT64_MAX;
    auto s = static_cast<uint64_t>(t.tv_sec);
    return s > (UINT64_MAX - 999999) / 1000000 ? UINT64_MAX : s * 1000000 + static_cast<uint64_t>(t.tv_nsec) / 1000;
}
auto within(uint64_t deadline) -> bool { return now() < deadline; }
auto bound(uint64_t deadline) -> uint64_t {
    auto t = now();
    return t > UINT64_MAX - STEP_US ? 0 : std::min(deadline, t + STEP_US);
}
void pause() {
    timespec t{0, 1'000'000};
    (void)nanosleep(&t, nullptr);
}
auto marker(uint64_t nonce, uint64_t tag) -> uint64_t { return (nonce ^ (tag * 0xd6e8feb86659fd93ULL)) | 1; }
auto payload(uint64_t value) -> Payload { return {value, value == 0 ? 0 : ~value}; }
auto matches(Payload p, uint64_t value) -> bool {
    auto q = payload(value);
    return p.marker == q.marker && p.complement == q.complement;
}
auto load(uint64_t address) -> Payload {
    auto* p = reinterpret_cast<const Payload*>(address);
    return {__atomic_load_n(&p->marker, __ATOMIC_ACQUIRE), __atomic_load_n(&p->complement, __ATOMIC_RELAXED)};
}
auto io(int fd, void* data, size_t count, off_t offset, bool write, uint64_t deadline) -> bool {
    size_t done{};
    unsigned interrupted{};
    while (done < count && within(deadline)) {
        auto* p = static_cast<unsigned char*>(data) + done;
        auto n = write ? pwrite(fd, p, count - done, offset + static_cast<off_t>(done))
                       : pread(fd, p, count - done, offset + static_cast<off_t>(done));
        if (n < 0 && errno == EINTR && ++interrupted <= 256) continue;
        if (n <= 0 || static_cast<size_t>(n) > count - done || !within(deadline)) return false;
        done += static_cast<size_t>(n);
    }
    return done == count;
}
auto row_offset(unsigned role) -> off_t { return 8192 + static_cast<off_t>(role - 1) * WORDS * 8; }
auto record(int fd, off_t offset, Record r, uint64_t deadline) -> bool {
    auto commit = r[63];
    r[63] = 0;
    return io(fd, r.data(), sizeof(r), offset, true, deadline) && fsync(fd) == 0 &&
           io(fd, &commit, sizeof(commit), offset + 504, true, deadline) && fsync(fd) == 0;
}
auto message(uint64_t nonce, uint64_t deadline, uint64_t sequence, uint64_t state, uint64_t actor, uint64_t value) -> Record {
    Record r{};
    r[0] = MAGIC;
    r[1] = VERSION;
    r[2] = nonce;
    r[3] = deadline;
    r[4] = sequence;
    r[5] = state;
    r[6] = actor;
    r[7] = value;
    r[61] = GUARD ^ nonce ^ sequence;
    r[62] = ~r[61];
    r[63] = nonce;
    return r;
}
auto receive(int fd, off_t offset, uint64_t nonce, uint64_t deadline, uint64_t sequence, uint64_t state, Record& out) -> bool {
    auto end = bound(deadline);
    while (within(end)) {
        Record a{}, b{};
        if (!io(fd, a.data(), sizeof(a), offset, false, end) || !io(fd, b.data(), sizeof(b), offset, false, end)) return false;
        bool reserved = true;
        for (size_t i = 8; i < 61; ++i) reserved &= a[i] == 0;
        if (a == b && a[0] == MAGIC && a[1] == VERSION && a[2] == nonce && a[3] == deadline && a[4] == sequence && a[5] == state &&
            a[61] == (GUARD ^ nonce ^ sequence) && a[62] == ~a[61] && a[63] == nonce && reserved) {
            out = a;
            return true;
        }
        pause();
    }
    return false;
}
struct Runtime {
    Row row{};
    unsigned role;
    uint64_t nonce, deadline;
    int control;
    Runtime(unsigned r, uint64_t n, uint64_t d, int fd = 9) : role(r), nonce(n), deadline(d), control(fd) {
        row[0] = MAGIC;
        row[1] = VERSION;
        row[2] = r;
        row[3] = n;
        row[4] = d;
        row[5] = bits(proc(3));
        row[6] = bits(proc(4));
        row[7] = now();
        for (unsigned i = 0; i < 4; ++i) row[20 + i] = bits(proc(10 + i));
        row[24] = bits(proc(36, 39));
        row[25] = bits(proc(36, 0x574f5301));
        row[26] = bits(proc(36, 0x574f5304));
        row[29] = WORDS * 8;
        row[30] = CAPACITY;
        row[62] = GUARD ^ n ^ r;
        row[63] = ~row[62];
    }
    auto reject(uint64_t code) -> bool {
        ++row[10];
        if (row[11] == 0) row[11] = code;
        return false;
    }
    auto emit(Op op, uint64_t a = 0, uint64_t b = 0, int64_t result = 0, uint64_t x = 0, uint64_t y = 0, uint64_t z = 0, uint64_t w = 0,
              uint64_t v = 0) -> bool {
        auto t = now();
        if (row[9] >= CAPACITY || t >= deadline || (row[9] && t < row[HEAD + (row[9] - 1) * EVENT_WORDS + 2])) return reject(1);
        auto* e = row.data() + HEAD + row[9] * EVENT_WORDS;
        e[0] = op;
        e[1] = ++row[9];
        e[2] = t;
        e[3] = a;
        e[4] = b;
        e[5] = bits(result);
        e[6] = x;
        e[7] = y;
        e[8] = z;
        e[9] = w;
        e[10] = v;
        e[11] = GUARD ^ nonce ^ role ^ e[1];
        return true;
    }
    auto publish() -> bool {
        row[8] = now();
        row[WORDS - 1] = 0;
        auto end = bound(UINT64_MAX);
        if (!io(control, row.data(), sizeof(row), row_offset(role), true, end) || fsync(control)) return false;
        auto commit = nonce;
        return io(control, &commit, 8, row_offset(role) + sizeof(row) - 8, true, end) && fsync(control) == 0;
    }
};
#define NEED(r, expr, code)                                            \
    do {                                                               \
        if (!(expr) || !within((r).deadline)) return (r).reject(code); \
    } while (false)
auto grant(Runtime& r, int fd, int access = -1, bool backing = false) -> bool {
    struct stat s{};
    auto stat_result = raw(5, 13, fd, reinterpret_cast<uint64_t>(&s));
    auto flags = raw(5, 28, fd, 3);
    auto descriptor = raw(5, 28, fd, 1);
    r.emit(GRANT, fd, bits(flags), stat_result, s.st_dev, s.st_ino, static_cast<uint64_t>(s.st_size), s.st_mode, bits(descriptor));
    NEED(r, stat_result == 0 && flags >= 0 && descriptor == 0 && S_ISREG(s.st_mode), 2);
    if (fd == 9) {
        r.row[27] = s.st_dev;
        r.row[28] = s.st_ino;
    }
    if (backing) {
        if (r.row[12] == 0 && r.row[13] == 0) {
            r.row[12] = s.st_dev;
            r.row[13] = s.st_ino;
            r.row[14] = static_cast<uint64_t>(s.st_size);
        }
        NEED(r, s.st_dev == r.row[12] && s.st_ino == r.row[13] && s.st_size == static_cast<off_t>(LENGTH) && (flags & O_NO_CACHE) != 0, 71);
        if (access == 0) r.row[15] = bits(flags);
    } else if (fd != 9)
        NEED(r, s.st_dev == r.row[27] && s.st_ino == r.row[28], 72);
    if (access >= 0) NEED(r, (flags & 3) == access, 73);
    return true;
}
auto observe(Runtime& r, uint64_t address, uint64_t offset, uint64_t expected, uint64_t stage) -> bool {
    auto p = load(address + offset);
    r.emit(READ, address, offset, 0, p.marker, p.complement, expected, stage);
    NEED(r, matches(p, expected), 3);
    return true;
}
auto write_marker(Runtime& r, uint64_t value) -> bool {
    auto p = payload(value);
    auto result = raw(5, 27, 10, reinterpret_cast<uint64_t>(&p), sizeof(p), 0);
    r.emit(PWRITE, 10, 0, result, p.marker, p.complement, value);
    NEED(r, result == 16, 4);
    return true;
}
auto pread_marker(Runtime& r, uint64_t expected) -> bool {
    Guarded p{GUARD ^ r.nonce, {}, ~(GUARD ^ r.nonce)};
    auto result = raw(5, 26, 11, reinterpret_cast<uint64_t>(&p.payload), 16, 0);
    r.emit(PREAD, 11, 0, result, p.payload.marker, p.payload.complement, expected, p.before, p.after);
    NEED(r, result == 16 && matches(p.payload, expected) && p.before == (GUARD ^ r.nonce) && p.after == ~p.before, 74);
    return true;
}
auto mapped(Runtime& r, int fd, int prot, uint64_t id, uint64_t& address, uint64_t hint = 0) -> bool {
    auto result = raw(8, hint, LENGTH, prot, MAP_SHARED, fd, 0);
    r.emit(MAP, LENGTH, prot, result, fd, 0, MAP_SHARED, id, hint);
    NEED(r, result > 0 && (static_cast<uint64_t>(result) & (PAGE - 1)) == 0, 5);
    address = static_cast<uint64_t>(result);
    return true;
}
auto protect(Runtime& r, uint64_t address, int prot) -> bool {
    auto result = raw(7, 2, address, LENGTH, prot);
    r.emit(PROTECT, address, LENGTH, result, prot);
    NEED(r, result == 0, 6);
    return true;
}
auto unmapped(Runtime& r, uint64_t& address) -> bool {
    if (!address) return true;
    auto result = raw(7, 1, address, LENGTH);
    r.emit(UNMAP, address, LENGTH, result);
    NEED(r, result == 0, 7);
    address = 0;
    return true;
}
auto closed(Runtime& r, int fd) -> bool {
    auto result = raw(5, 3, fd);
    r.emit(CLOSE, fd, 0, result);
    NEED(r, result == 0, 8);
    return true;
}
auto truncate_to(Runtime& r, uint64_t length, Op op, int expected) -> bool {
    auto result = ftruncate(10, static_cast<off_t>(length));
    auto actual = result < 0 ? -errno : result;
    struct stat s{};
    auto sr = fstat(10, &s);
    r.emit(op, 10, length, actual, static_cast<uint64_t>(s.st_size), bits(sr));
    NEED(r, actual == expected && sr == 0 && static_cast<uint64_t>(s.st_size) == (expected == 0 ? length : LENGTH), 9);
    return true;
}
auto refused(Runtime& r, uint64_t a) -> bool {
    NEED(r, observe(r, a, 0, marker(r.nonce, 1), 7), 75);
    auto result = raw(7, 2, a, LENGTH, PROT_WRITE);
    r.emit(REFUSE, 1, a, result, LENGTH, PROT_WRITE);
    NEED(r, result == -EACCES, 76);
    result = raw(7, 2, a, LENGTH, PROT_EXEC);
    r.emit(REFUSE, 2, a, result, LENGTH, PROT_EXEC);
    NEED(r, result == -EACCES, 77);
    result = raw(7, 1, a + PAGE, PAGE);
    r.emit(REFUSE, 3, a + PAGE, result, PAGE);
    NEED(r, result == -EOPNOTSUPP, 78);
    result = raw(7, 2, a + PAGE, PAGE, PROT_NONE);
    r.emit(REFUSE, 4, a + PAGE, result, PAGE, PROT_NONE);
    NEED(r, result == -EOPNOTSUPP, 79);
    result = raw(7, 3, a, LENGTH, LENGTH + PAGE, MREMAP_MAYMOVE);
    r.emit(REFUSE, 5, a, result, LENGTH, LENGTH + PAGE, MREMAP_MAYMOVE);
    NEED(r, result == -EOPNOTSUPP && observe(r, a, 0, marker(r.nonce, 1), 8), 80);
    NEED(r, protect(r, a, PROT_NONE), 81);
    result = raw(5, 27, 9, a + 64 * PAGE, 16, COPY_AT + 8);
    Guarded p{};
    NEED(r, io(9, &p, sizeof(p), COPY_AT, false, bound(r.deadline)), 82);
    r.emit(COPY_NONE, a + 64 * PAGE, 16, result, p.payload.marker, p.payload.complement, p.before, p.after);
    NEED(r, result == -EFAULT && p.before == (GUARD ^ r.nonce) && p.after == ~p.before && matches(p.payload, 0), 83);
    NEED(r, protect(r, a, PROT_READ) && observe(r, a, 0, marker(r.nonce, 1), 9), 84);
    return true;
}
struct ThreadJob {
    unsigned ready{}, go{}, done{};
    uint64_t a{}, b{}, deadline{}, expected{};
    int64_t none = -999, read = -999;
    Payload first{}, second{};
};
auto thread_body(void* argument) -> void* {
    auto& j = *static_cast<ThreadJob*>(argument);
    __atomic_store_n(&j.ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&j.go, __ATOMIC_ACQUIRE) && within(j.deadline)) pause();
    if (__atomic_load_n(&j.go, __ATOMIC_ACQUIRE)) {
        j.first = load(j.a);
        j.none = raw(7, 2, j.b, LENGTH, PROT_NONE);
        j.read = raw(7, 2, j.b, LENGTH, PROT_READ);
        if (j.read == 0) j.second = load(j.b);
    }
    __atomic_store_n(&j.done, 1, __ATOMIC_RELEASE);
    return argument;
}
auto wait_flag(unsigned& flag, uint64_t deadline) -> bool {
    while (!__atomic_load_n(&flag, __ATOMIC_ACQUIRE) && within(deadline)) pause();
    return __atomic_load_n(&flag, __ATOMIC_ACQUIRE) != 0;
}
auto reaped(Runtime& r, int64_t& child, uint64_t end) -> bool {
    auto expected = child;
    int status = -1;
    int64_t result = 0;
    while (within(end)) {
        result = proc(2, child, reinterpret_cast<uint64_t>(&status), 1);
        if (result != 0 && result != -EINTR) break;
        pause();
    }
    if (result == expected || result == -ECHILD) child = -1;
    r.emit(WAITPID, expected, bits(status), result);
    NEED(r, result == expected && status == 0, 10);
    return true;
}
auto fork_work(Runtime& r, uint64_t inherited) -> bool {
    r.row[16] = inherited;
    NEED(r, grant(r, 9) && observe(r, inherited, 0, marker(r.nonce, 2), 10) && observe(r, inherited, 127 * PAGE, 0, 11), 11);
    NEED(r, unmapped(r, inherited), 12);
    int ro = open(BACKING, O_RDONLY | O_NO_CACHE);
    NEED(r, ro >= 0 && grant(r, ro, 0, true), 13);
    uint64_t independent = 0;
    NEED(r, mapped(r, ro, PROT_READ, 3, independent, 0x5000000000ULL), 14);
    NEED(r, independent != r.row[16] && observe(r, independent, 0, marker(r.nonce, 2), 12), 15);
    r.row[17] = independent;
    NEED(r, closed(r, ro), 16);
    auto msg = message(r.nonce, r.deadline, 1, 1, r.row[5], independent);
    NEED(r, record(9, 512, msg, bound(r.deadline)), 17);
    r.emit(READY, independent + PAGE + 64, 0, 0);
    auto start = now();
    NEED(r, start <= UINT64_MAX - STEP_US, 18);
    auto end = std::min(r.deadline, start + STEP_US);
    NEED(r, end > start, 18);
    auto remaining = end - start;
    timespec timeout{static_cast<time_t>(remaining / 1000000), static_cast<long>((remaining % 1000000) * 1000)};
    auto result = ker::futex::wait(reinterpret_cast<int*>(independent + PAGE + 64), 0, &timeout);
    r.emit(WAIT, independent + PAGE + 64, remaining, result);
    NEED(r, result == 0, 19);
    NEED(r, observe(r, independent, 0, marker(r.nonce, 3), 13), 20);
    NEED(r, record(9, 1024, message(r.nonce, r.deadline, 2, 2, r.row[5], marker(r.nonce, 3)), bound(r.deadline)), 21);
    r.emit(ACK, independent, 0, 0, marker(r.nonce, 3));
    NEED(r, closed(r, 10) && closed(r, 11), 22);
    r.emit(EXEC_INTENT, r.row[5], 0, 0);
    NEED(r, r.publish(), 23);
    char nonce[24]{}, deadline[24]{};
    std::snprintf(nonce, sizeof(nonce), "%llu", static_cast<unsigned long long>(r.nonce));
    std::snprintf(deadline, sizeof(deadline), "%llu", static_cast<unsigned long long>(r.deadline));
    const char* args[]{PRIVATE, "--after-exec", nonce, deadline, nullptr};
    const char* env[]{"LD_LIBRARY_PATH=/usr/lib", nullptr};
    auto failure = ker::process::execve(PRIVATE, args, env);
    return r.reject(100 + static_cast<uint64_t>(-failure));
}
auto after_exec(Runtime& r) -> bool {
    Record req{};
    NEED(r, receive(9, 0, r.nonce, r.deadline, 0, 1, req), 24);
    r.row[32] = req[7];
    auto a = raw(5, 28, 10, 1), b = raw(5, 28, 11, 1);
    r.emit(NEW_IMAGE, r.row[5], r.row[6], 0, bits(a), bits(b));
    NEED(r, r.row[5] == 3 && r.row[6] == 1 && a == -EBADF && b == -EBADF && grant(r, 9), 25);
    r.emit(EXIT_INTENT, 0, 0, 0);
    return true;
}
auto controller_work(Runtime& r, uint64_t& a, uint64_t& b, int64_t& child, ThreadJob& job, bool& thread_live, pthread_t& thread) -> bool {
    Record req{};
    NEED(r, receive(9, 0, r.nonce, r.deadline, 0, 1, req) && req[7] > 2, 26);
    r.row[32] = req[7];
    NEED(r, r.row[5] == 1 && r.row[6] == 0 && grant(r, 9), 27);
    int rw = open(BACKING, O_CREAT | O_EXCL | O_RDWR | O_NO_CACHE, 0600);
    NEED(r, rw >= 0, 28);
    if (rw != 10) {
        auto result = raw(5, 16, rw, 10);
        r.emit(DUP, rw, 10, result);
        NEED(r, result == 10 && closed(r, rw), 29);
    }
    NEED(r, truncate_to(r, LENGTH, TRUNCATE, 0) && write_marker(r, marker(r.nonce, 1)), 30);
    int ro = open(BACKING, O_RDONLY | O_NO_CACHE);
    NEED(r, ro >= 0, 31);
    if (ro != 11) {
        auto result = raw(5, 16, ro, 11);
        r.emit(DUP, ro, 11, result);
        NEED(r, result == 11 && closed(r, ro), 32);
    }
    NEED(r, grant(r, 10, 2, true) && grant(r, 11, 0, true), 33);
    NEED(r, mapped(r, 11, PROT_READ, 1, a), 34);
    r.row[16] = a;
    pthread_attr_t attributes;
    NEED(r, pthread_attr_init(&attributes) == 0, 35);
    auto attr = pthread_attr_setstacksize(&attributes, 65536);
    job.a = a;
    job.deadline = bound(r.deadline);
    job.expected = marker(r.nonce, 1);
    int created = attr == 0 ? pthread_create(&thread, &attributes, thread_body, &job) : attr;
    pthread_attr_destroy(&attributes);
    if (created == 0) thread_live = true;
    r.emit(THREAD_CREATE, 65536, 0, created);
    NEED(r, created == 0 && wait_flag(job.ready, job.deadline), 36);
    NEED(r, mapped(r, 11, PROT_NONE, 2, b), 37);
    r.row[17] = b;
    NEED(r, protect(r, b, PROT_READ), 38);
    job.b = b;
    __atomic_store_n(&job.go, 1, __ATOMIC_RELEASE);
    NEED(r, wait_flag(job.done, job.deadline), 39);
    void* joined = nullptr;
    auto join = pthread_join(thread, &joined);
    if (join == 0) thread_live = false;
    r.emit(THREAD_JOIN, joined == &job, 0, join);
    NEED(r, join == 0 && joined == &job, 40);
    r.emit(THREAD_READ, a, 1, 0, job.first.marker, job.first.complement, job.expected);
    r.emit(THREAD_PROTECT, b, PROT_NONE, job.none);
    r.emit(THREAD_PROTECT, b, PROT_READ, job.read);
    r.emit(THREAD_READ, b, 2, 0, job.second.marker, job.second.complement, job.expected);
    NEED(r, job.none == 0 && job.read == 0 && matches(job.first, job.expected) && matches(job.second, job.expected), 41);
    NEED(r, refused(r, a), 85);
    NEED(r, truncate_to(r, LENGTH + PAGE, TRUNCATE, -EBUSY), 42);
    // Page65 has never been loaded by userspace: the kernel must fault it for copy_from_task.
    auto copied = raw(5, 27, 9, a + 65 * PAGE, 16, COPY_AT + 8);
    Guarded packet{};
    NEED(r, io(9, &packet, sizeof(packet), COPY_AT, false, bound(r.deadline)), 43);
    r.emit(COPY, a + 65 * PAGE, 16, copied, packet.payload.marker, packet.payload.complement, packet.before, packet.after, 0);
    NEED(r, copied == 16 && packet.before == (GUARD ^ r.nonce) && packet.after == ~packet.before && matches(packet.payload, 0), 44);
    NEED(r,
         observe(r, a, 128 * PAGE, 0, 1) && write_marker(r, marker(r.nonce, 2)) && observe(r, a, 0, marker(r.nonce, 2), 2) &&
             observe(r, b, 0, marker(r.nonce, 2), 3) && pread_marker(r, marker(r.nonce, 2)),
         45);
    NEED(r, closed(r, 11), 46);
    auto reused = raw(5, 16, 9, 11);
    r.emit(DUP, 9, 11, reused);
    NEED(r, reused == 11 && grant(r, 11, 2), 47);
    NEED(r, observe(r, a, 0, marker(r.nonce, 2), 4) && unmapped(r, b), 48);
    child = proc(5);
    if (child == 0) {
        Runtime c(2, r.nonce, r.deadline);
        c.row[32] = r.row[32];
        for (unsigned i = 12; i <= 15; ++i) c.row[i] = r.row[i];
        bool ok = fork_work(c, a);
        if (!ok) c.publish();
        ker::process::exit(ok ? 0 : 80);
        __builtin_unreachable();
    }
    r.emit(FORK, 1, 0, child);
    r.row[18] = bits(child);
    NEED(r, child == 3, 49);
    Record ready{};
    NEED(r, receive(9, 512, r.nonce, r.deadline, 1, 1, ready) && ready[6] == static_cast<uint64_t>(child), 50);
    NEED(r, write_marker(r, marker(r.nonce, 3)), 51);
    auto wake_end = bound(r.deadline);
    int64_t woke = 0;
    for (unsigned attempt = 0; attempt < 128 && within(wake_end); ++attempt) {
        woke = ker::futex::wake(reinterpret_cast<int*>(a + PAGE + 64), 1);
        r.emit(WAKE, a + PAGE + 64, 1, woke, attempt);
        NEED(r, woke == 0 || woke == 1, 52);
        if (woke == 1) break;
        timespec wait{0, 10'000'000};
        (void)nanosleep(&wait, nullptr);
    }
    NEED(r, woke == 1, 53);
    Record ack{};
    NEED(r, receive(9, 1024, r.nonce, r.deadline, 2, 2, ack) && ack[6] == 3 && ack[7] == marker(r.nonce, 3), 54);
    NEED(r, reaped(r, child, bound(r.deadline)), 55);
    child = -1;
    NEED(r, observe(r, a, 0, marker(r.nonce, 3), 5) && truncate_to(r, LENGTH + PAGE, TRUNCATE, -EBUSY), 56);
    child = proc(5);
    if (child == 0) {
        Runtime c(4, r.nonce, r.deadline);
        c.row[16] = a;
        c.row[32] = r.row[32];
        for (unsigned i = 12; i <= 15; ++i) c.row[i] = r.row[i];
        bool ok = grant(c, 9) && observe(c, a, 0, marker(r.nonce, 3), 14) && observe(c, a, 126 * PAGE, 0, 15);
        c.emit(EXIT_INTENT, a, LENGTH, 0);
        ok = c.publish() && ok;
        ker::process::exit(ok ? 0 : 80);
        __builtin_unreachable();
    }
    r.emit(FORK, 2, 0, child);
    r.row[19] = bits(child);
    NEED(r, child == 4 && reaped(r, child, bound(r.deadline)), 57);
    child = -1;
    NEED(r, observe(r, a, 0, marker(r.nonce, 3), 6) && unmapped(r, a), 58);
    auto end = bound(r.deadline);
    bool released = false;
    for (unsigned attempt = 0; attempt < 64 && within(end); ++attempt) {
        auto result = ftruncate(10, LENGTH + PAGE);
        int actual = result < 0 ? -errno : result;
        struct stat s{};
        int sr = fstat(10, &s);
        r.emit(GROW_RELEASE, 10, LENGTH + PAGE, actual, static_cast<uint64_t>(s.st_size), bits(sr));
        NEED(r, sr == 0 && (actual == 0 || actual == -EBUSY) && static_cast<uint64_t>(s.st_size) == (actual == 0 ? LENGTH + PAGE : LENGTH),
             59);
        if (actual == 0) {
            released = true;
            break;
        }
        timespec wait{0, 50'000'000};
        (void)nanosleep(&wait, nullptr);
    }
    NEED(r, released && closed(r, 10) && closed(r, 11), 60);
    r.emit(EXIT_INTENT, 0, 0, 0);
    return true;
}
[[noreturn]] void controller(uint64_t nonce, uint64_t deadline) {
    Runtime r(1, nonce, deadline);
    uint64_t a = 0, b = 0;
    int64_t child = -1;
    ThreadJob job{};
    pthread_t thread{};
    bool thread_live = false;
    bool ok = controller_work(r, a, b, child, job, thread_live, thread);
    if (!ok && child > 0) {
        (void)proc(8, child, SIGKILL);
        (void)reaped(r, child, bound(UINT64_MAX));
    }
    // Never revoke memory while a live thread could still use it. Process EXIT
    // retires every remaining thread/FD/map; namespace init is the child backstop.
    if (!ok && !thread_live) {
        (void)unmapped(r, b);
        (void)unmapped(r, a);
    }
    if (!r.publish()) ok = false;
    ker::process::exit(ok && r.row[10] == 0 ? 0 : 80);
    __builtin_unreachable();
}
auto number(const char* s, uint64_t& out) -> bool {
    if (!s || !*s) return false;
    out = 0;
    for (; *s; ++s) {
        if (*s < '0' || *s > '9' || out > (UINT64_MAX - static_cast<unsigned>(*s - '0')) / 10) return false;
        out = out * 10 + static_cast<unsigned>(*s - '0');
    }
    return true;
}
auto filename(char* out, size_t size, const char* name) -> bool {
    int n = std::snprintf(out, size, "%s/%s", RESULTS, name);
    return n > 0 && static_cast<size_t>(n) < size;
}
auto save(const char* name, const void* data, size_t bytes) -> bool {
    char path[160]{};
    if (!filename(path, sizeof(path), name)) return false;
    int fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    bool ok = io(fd, const_cast<void*>(data), bytes, 0, true, bound(UINT64_MAX)) && fsync(fd) == 0;
    return close(fd) == 0 && ok;
}
auto text(const char* s) -> abi::String { return {reinterpret_cast<uint64_t>(s), std::strlen(s)}; }
auto create(Runtime& r) -> int64_t {
    abi::Spec spec{};
    for (size_t i = 0; i < 16; ++i) {
        abi::Limit l{};
        if (proc(48, i, reinterpret_cast<uint64_t>(&l), 1) != 0) return -EIO;
        spec.limits[i] = l;
    }
    std::array<const char*, 3> sources{BINARY, "/lib/ld.so", "/usr/lib/libc.so"};
    std::array<const char*, 3> targets{PRIVATE, "/lib/ld.so", "/usr/lib/libc.so"};
    std::array<abi::Asset, 3> assets{};
    bool good = true;
    for (size_t i = 0; i < assets.size(); ++i) {
        assets[i] = {open(sources[i], O_RDONLY | O_CLOEXEC), i < 2 ? 0755U : 0644U, text(targets[i]), 0};
        good &= assets[i].source_fd >= 0;
    }
    abi::Descriptor descriptor{r.control, 9, 0};
    char nonce[24]{}, deadline[24]{};
    std::snprintf(nonce, sizeof(nonce), "%llu", static_cast<unsigned long long>(r.nonce));
    std::snprintf(deadline, sizeof(deadline), "%llu", static_cast<unsigned long long>(r.deadline));
    std::array args{text(PRIVATE), text("--controller"), text(nonce), text(deadline)};
    std::array env{text("LD_LIBRARY_PATH=/usr/lib")};
    spec.executable = args[0];
    spec.assets = reinterpret_cast<uint64_t>(assets.data());
    spec.asset_count = assets.size();
    spec.descriptors = reinterpret_cast<uint64_t>(&descriptor);
    spec.descriptor_count = 1;
    spec.argv = reinterpret_cast<uint64_t>(args.data());
    spec.argc = args.size();
    spec.envp = reinterpret_cast<uint64_t>(env.data());
    spec.envc = 1;
    int64_t child = good ? proc(abi::CREATE_OPERATION, reinterpret_cast<uint64_t>(&spec)) : -EIO;
    // Store exact ownership before cleanup/clock/IO, including failed asset closes.
    r.row[18] = bits(child);
    for (auto a : assets)
        if (a.source_fd >= 0 && close(a.source_fd) != 0) good = false;
    r.emit(CREATE, 1, 0, child, good);
    return child;
}
auto host() -> int {
    uint64_t start = now();
    if (start > UINT64_MAX - RUN_US) return 81;
    uint64_t nonce = (start ^ (bits(proc(3)) << 32) ^ GUARD) | (1ULL << 63) | 1;
    Runtime r(0, nonce, start + RUN_US, -1);
    struct stat directory{};
    if (stat(RESULTS, &directory) || !S_ISDIR(directory.st_mode) || (directory.st_mode & 07777) != 0700 || directory.st_uid != 100 ||
        directory.st_gid != 100)
        return 81;
    for (int fd = 0; fd < 256; ++fd) {
        auto result = raw(5, 28, fd, 1);
        if (result >= 0 && close(fd) != 0) return 81;
        if (result < 0 && result != -EBADF) return 81;
    }
    char path[160]{};
    if (!filename(path, sizeof(path), "control.bin")) return 81;
    r.control = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC | O_NO_CACHE, 0600);
    if (r.control < 0) return 81;
    bool ok = ftruncate(r.control, CONTROL_BYTES) == 0;
    Guarded copy{GUARD ^ nonce, {}, ~(GUARD ^ nonce)};
    ok = ok && io(r.control, &copy, sizeof(copy), COPY_AT, true, bound(r.deadline));
    int64_t child = -1;
    bool consumed = false;
    if (ok) {
        child = create(r);
        ok = child > 2 && r.row[HEAD + 6] == 1;
    }
    if (ok) ok = record(r.control, 0, message(nonce, r.deadline, 0, 1, r.row[5], static_cast<uint64_t>(child)), bound(r.deadline));
    int status = -1;
    int64_t result = 0;
    while (ok && within(r.deadline)) {
        result = proc(2, child, reinterpret_cast<uint64_t>(&status), 1);
        if (result == child) {
            consumed = true;
            break;
        }
        if (result == -ECHILD) consumed = true;
        if (result != 0 && result != -EINTR) {
            ok = false;
            break;
        }
        pause();
    }
    r.emit(REAP, child, bits(status), result);
    ok = ok && consumed && status == 0;
    if (!consumed && child > 0) {
        (void)proc(8, child, SIGKILL);
        auto end = bound(UINT64_MAX);
        while (within(end)) {
            result = proc(2, child, reinterpret_cast<uint64_t>(&status), 1);
            if (result == child || result == -ECHILD) {
                consumed = true;
                break;
            }
            pause();
        }
    }
    if (ok) {
        for (unsigned role = 1; role <= 4; ++role) {
            Row a{}, b{};
            char name[32]{};
            std::snprintf(name, sizeof(name), "role-%u.bin", role);
            bool good = io(r.control, a.data(), sizeof(a), row_offset(role), false, bound(r.deadline)) &&
                        io(r.control, b.data(), sizeof(b), row_offset(role), false, bound(r.deadline)) && a == b && a[WORDS - 1] == nonce;
            if (!good || !save(name, a.data(), sizeof(a))) ok = false;
        }
    }
    r.row[8] = now();
    r.row[WORDS - 1] = nonce;
    if (!ok) r.reject(70);
    bool saved = save("host.bin", r.row.data(), sizeof(r.row));
    if (close(r.control) != 0) ok = false;
    std::array<char, 512> status_line{};
    int n = std::snprintf(status_line.data(), status_line.size(), "%s tmpfs-readshared nonce=%llu failures=%llu\n",
                          ok && saved ? "PUBLIC_COMPLETE" : "EXECUTION_FAIL", static_cast<unsigned long long>(nonce),
                          static_cast<unsigned long long>(r.row[10] + (ok && saved ? 0 : 1)));
    return n > 0 && static_cast<size_t>(n) < status_line.size() && save("status", status_line.data(), status_line.size()) && ok && saved
               ? 0
               : 81;
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
                if (good && std::strcmp(service.service, "tmpfs-readshared-probe") == 0) {
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

    bool const saved = save("init-exit-snapshot.bin", &snapshot, sizeof(snapshot));
    std::array<char, 512> status{};
    bool const success = observed && error == 0 && saved;
    auto const n = std::snprintf(
        status.data(), status.size(),
        "%s tmpfs-readshared-init-exit service=tmpfs-readshared-probe generation=%llu restarts=%llu sequence=%llu published-ns=%llu "
        "state=%u readiness=%u wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u read-error=%lld failures=%u\n",
        success ? "PASS" : "FAIL", static_cast<unsigned long long>(target.generation),
        static_cast<unsigned long long>(target.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned long long>(snapshot.published_mono_ns), static_cast<unsigned>(target.state),
        static_cast<unsigned>(target.readiness), target.last_wait_status, target.last_error, history ? 1U : 0U, snapshot.size,
        snapshot.version, static_cast<long long>(error), success ? 0U : 1U);
    bool const durable = n > 0 && static_cast<size_t>(n) < status.size() && save("init-exit-status", status.data(), status.size());
    bool const printed = durable && write(STDOUT_FILENO, status.data(), static_cast<size_t>(n)) == n;
    return success && durable && printed ? 0 : 82;
}
}  // namespace
auto main(int argc, char** argv) -> int {
    if (argc == 1) return host();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return witness();
    uint64_t nonce = 0, deadline = 0;
    if (argc == 4 && number(argv[2], nonce) && nonce && number(argv[3], deadline) && deadline) {
        if (std::strcmp(argv[1], "--controller") == 0) controller(nonce, deadline);
        if (std::strcmp(argv[1], "--after-exec") == 0) {
            Runtime r(3, nonce, deadline);
            bool ok = after_exec(r);
            ok = r.publish() && ok;
            ker::process::exit(ok ? 0 : 80);
            __builtin_unreachable();
        }
    }
    return 83;
}
