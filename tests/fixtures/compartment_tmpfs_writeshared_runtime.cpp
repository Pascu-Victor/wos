// Bounded public ordinary-shm regression; see the sibling protocol document.
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
#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
namespace abi = ker::abi::compartment;
constexpr char RESULTS[] = "/srv/tmpfs-writeshared-results";
constexpr char BINARY[] = "/usr/libexec/compartment-tmpfs-writeshared-runtime";
constexpr char PRIVATE[] = "/app/writeshared-probe";
constexpr char NAME[] = "/writeshared-probe";
constexpr uint64_t MAGIC = 0x35395752544d5046ULL, GUARD = 0x7c5e2198d3a06bf4ULL;
constexpr uint64_t PAGE = 4096, EOF_BYTES = PAGE + 73, LENGTH = 2 * PAGE;
constexpr uint64_t RUN_US = 60'000'000, STEP_US = 5'000'000, CONTROL_BYTES = 32768;
constexpr uint64_t PARENT_VA = 0x4000000000ULL, PARENT_RO = 0x4100000000ULL;
constexpr uint64_t CHILD_VA = 0x5000000000ULL, CHILD_RO = 0x5100000000ULL;
constexpr size_t WORDS = 512, HEAD = 64, CAPACITY = 44, EW = 10, FUTEX_OFFSET = 128;
constexpr off_t SCRATCH = 4096;
using Record = std::array<uint64_t, 64>;
using Row = std::array<uint64_t, WORDS>;
using Bytes = std::array<unsigned char, LENGTH>;
enum Op : uint64_t {
    GRANT = 1,
    SEED,
    PREFILL,
    MAP,
    STORE,
    READ_MAP,
    PWRITE,
    PROTECT,
    COPY_TO,
    COPY_FROM,
    DENY,
    SYNC,
    REUSE,
    FORK,
    READY,
    WAIT,
    WAKE,
    EXEC,
    IMAGE,
    WAITPID,
    UNMAP,
    GROW_BUSY,
    TAIL,
    GROW,
    TAIL_READ,
    CLEANUP,
    CREATE,
    REAP,
    FLAGS
};
struct Payload {
    uint64_t value{}, complement{};
};
struct Scratch {
    uint64_t before{};
    Payload data{};
    uint64_t after{};
};
static_assert(sizeof(Row) == 4096 && sizeof(Record) == 512 && sizeof(Scratch) == 32);
static_assert(sizeof(struct stat) == 144 && sizeof(abi::Spec) == 408 && std::endian::native == std::endian::little);
static_assert(HEAD + CAPACITY * EW < WORDS - 1 && 8192 + 5 * sizeof(Row) <= CONTROL_BYTES);
static_assert(MS_SYNC == 4 && MS_ASYNC == 1 && MS_INVALIDATE == 2);
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
auto payload(uint64_t value) -> Payload { return {value, ~value}; }
auto load(uint64_t address) -> Payload {
    auto* p = reinterpret_cast<const Payload*>(address);
    return {__atomic_load_n(&p->value, __ATOMIC_ACQUIRE), __atomic_load_n(&p->complement, __ATOMIC_RELAXED)};
}
void store(uint64_t address, uint64_t value) {
    auto* p = reinterpret_cast<Payload*>(address);
    __atomic_store_n(&p->complement, ~value, __ATOMIC_RELAXED);
    __atomic_store_n(&p->value, value, __ATOMIC_RELEASE);
}
auto matches(Payload p, uint64_t v) -> bool { return p.value == v && p.complement == ~v; }
auto checksum(const unsigned char* p, size_t size) -> uint64_t {
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < size; ++i) h = (h ^ p[i]) * 1099511628211ULL;
    return h;
}
auto initial(uint64_t nonce) -> Bytes {
    Bytes out{};
    auto p = payload(marker(nonce, 1));
    std::memcpy(out.data(), &p, sizeof(p));
    for (size_t i = 0; i < 73; ++i) out[PAGE + i] = static_cast<unsigned char>((nonce >> ((i % 8) * 8)) ^ (i * 37 + 19));
    return out;
}
auto io(int fd, void* data, size_t size, off_t offset, bool writing, uint64_t deadline) -> bool {
    size_t done{};
    unsigned interrupted{};
    while (done < size && now() < deadline) {
        auto* p = static_cast<unsigned char*>(data) + done;
        auto n = writing ? pwrite(fd, p, size - done, offset + static_cast<off_t>(done))
                         : pread(fd, p, size - done, offset + static_cast<off_t>(done));
        if (n < 0 && errno == EINTR && ++interrupted <= 64) continue;
        if (n <= 0 || static_cast<size_t>(n) > size - done) return false;
        done += static_cast<size_t>(n);
    }
    return done == size && now() < deadline;
}
auto record(int fd, off_t offset, Record r, uint64_t deadline) -> bool {
    auto commit = r[63];
    r[63] = 0;
    return io(fd, r.data(), sizeof(r), offset, true, deadline) && fsync(fd) == 0 && io(fd, &commit, 8, offset + 504, true, deadline) &&
           fsync(fd) == 0;
}
auto message(uint64_t nonce, uint64_t deadline, uint64_t seq, uint64_t state, uint64_t actor, uint64_t value) -> Record {
    Record r{};
    r[0] = MAGIC;
    r[1] = 1;
    r[2] = nonce;
    r[3] = deadline;
    r[4] = seq;
    r[5] = state;
    r[6] = actor;
    r[7] = value;
    r[61] = GUARD ^ nonce ^ seq ^ state;
    r[62] = ~r[61];
    r[63] = nonce;
    return r;
}
auto receive(int fd, off_t offset, uint64_t nonce, uint64_t deadline, uint64_t seq, uint64_t state, Record& out) -> bool {
    auto end = bound(deadline);
    while (now() < end) {
        Record a{}, b{};
        if (!io(fd, a.data(), sizeof(a), offset, false, end) || !io(fd, b.data(), sizeof(b), offset, false, end)) return false;
        bool reserved = true;
        for (size_t i = 8; i < 61; ++i) reserved &= a[i] == 0;
        if (a == b && a[0] == MAGIC && a[1] == 1 && a[2] == nonce && a[3] == deadline && a[4] == seq && a[5] == state &&
            a[61] == (GUARD ^ nonce ^ seq ^ state) && a[62] == ~a[61] && a[63] == nonce && reserved) {
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
        row[1] = 1;
        row[2] = r;
        row[3] = n;
        row[4] = d;
        row[5] = bits(proc(3));
        row[6] = bits(proc(4));
        row[7] = now();
        for (unsigned i = 0; i < 4; ++i) row[25 + i] = bits(proc(10 + i));
        row[32] = LENGTH;
        row[33] = CAPACITY;
        row[34] = sizeof(Row);
        row[35] = MS_SYNC;
        row[36] = MS_ASYNC;
        row[37] = MS_INVALIDATE;
        row[62] = GUARD ^ n ^ r;
        row[63] = ~row[62];
    }
    auto fail(uint64_t stage) -> bool {
        ++row[9];
        if (row[10] == 0) row[10] = stage;
        return false;
    }
    auto event(Op op, uint64_t a = 0, uint64_t b = 0, int64_t result = 0, uint64_t x = 0, uint64_t y = 0, uint64_t z = 0) -> bool {
        auto t = now();
        if (row[11] >= CAPACITY || t >= deadline || (row[11] && t < row[HEAD + (row[11] - 1) * EW + 2])) return fail(1);
        auto* e = row.data() + HEAD + row[11] * EW;
        e[0] = op;
        e[1] = ++row[11];
        e[2] = t;
        e[3] = a;
        e[4] = b;
        e[5] = bits(result);
        e[6] = x;
        e[7] = y;
        e[8] = z;
        e[9] = GUARD ^ nonce ^ role ^ e[1];
        return true;
    }
    auto publish() -> bool {
        row[8] = now();
        row.back() = 0;
        auto end = bound(UINT64_MAX);
        off_t offset = 8192 + static_cast<off_t>(role) * sizeof(Row);
        if (!io(control, row.data(), sizeof(row), offset, true, end) || fsync(control)) return false;
        row.back() = nonce;
        return io(control, &nonce, 8, offset + sizeof(row) - 8, true, end) && fsync(control) == 0;
    }
};
#define NEED(r, expression, stage)                                          \
    do {                                                                    \
        if (!(expression) || now() >= (r).deadline) return (r).fail(stage); \
    } while (false)
auto actual(int result) -> int64_t { return result < 0 ? -errno : result; }
auto normalize(int fd, int target) -> bool {
    if (fd < 0) return false;
    if (fd == target) return true;
    bool ok = dup2(fd, target) == target;
    return close(fd) == 0 && ok;
}
auto grant(Runtime& r, int fd, unsigned access, bool backing) -> bool {
    struct stat st{};
    auto s = fstat(fd, &st);
    auto flags = fcntl(fd, F_GETFL);
    auto descriptor = fcntl(fd, F_GETFD);
    r.event(GRANT, fd, bits(flags), s, st.st_dev, st.st_ino, static_cast<uint64_t>(st.st_size));
    NEED(r,
         s == 0 && flags >= 0 && (static_cast<unsigned>(flags) & 3) == access && descriptor >= 0 && (descriptor & ~FD_CLOEXEC) == 0 &&
             (backing || descriptor == 0) && S_ISREG(st.st_mode),
         2);
    if (backing) {
        NEED(r,
             (flags & O_NO_CACHE) == 0 && (r.row[13] == 0 || (r.row[12] == st.st_dev && r.row[13] == st.st_ino)) &&
                 st.st_size == static_cast<off_t>(EOF_BYTES),
             3);
        r.row[12] = st.st_dev;
        r.row[13] = st.st_ino;
        r.row[14] = st.st_size;
        r.row[30] = st.st_mode;
        r.row[access == 2 ? 15 : 16] = bits(flags);
    } else {
        NEED(r, st.st_size == static_cast<off_t>(CONTROL_BYTES), 4);
        r.row[22] = st.st_dev;
        r.row[23] = st.st_ino;
        r.row[24] = st.st_size;
        r.row[31] = st.st_mode;
    }
    return true;
}
auto map(Runtime& r, int fd, int prot, uint64_t hint, uint64_t& out) -> bool {
    auto result = raw(8, hint, LENGTH, prot, MAP_SHARED | MAP_FIXED, fd, 0);
    r.event(MAP, fd, prot, result, LENGTH, MAP_SHARED | MAP_FIXED, 0);
    NEED(r, result == static_cast<int64_t>(hint), 5);
    out = static_cast<uint64_t>(result);
    return true;
}
auto unmap(Runtime& r, uint64_t& address) -> bool {
    if (address == 0) return true;
    auto result = raw(7, 1, address, LENGTH);
    r.event(UNMAP, address, LENGTH, result);
    if (result == 0) address = 0;
    NEED(r, result == 0, 6);
    return true;
}
auto observe(Runtime& r, uint64_t address, uint64_t tag) -> bool {
    auto p = load(address);
    auto expected = marker(r.nonce, tag);
    r.event(READ_MAP, address, tag, 0, p.value, p.complement, expected);
    NEED(r, matches(p, expected), 7);
    return true;
}
auto mapped_store(Runtime& r, uint64_t address, uint64_t tag) -> bool {
    auto value = marker(r.nonce, tag);
    store(address, value);
    auto p = load(address);
    r.event(STORE, address, tag, 0, p.value, p.complement, value);
    NEED(r, matches(p, value), 8);
    return true;
}
auto reader(Runtime& r, int fd, uint64_t tag) -> bool {
    Bytes got{}, expected = initial(r.nonce);
    auto p = payload(marker(r.nonce, tag));
    std::memcpy(expected.data(), &p, sizeof(p));
    auto seek = lseek(fd, 0, SEEK_SET);
    auto result = seek == 0 ? read(fd, got.data(), EOF_BYTES) : -1;
    r.event(PREFILL, fd, tag, result, checksum(got.data(), EOF_BYTES), checksum(expected.data(), EOF_BYTES), bits(seek));
    NEED(r, seek == 0 && result == static_cast<ssize_t>(EOF_BYTES) && got == expected, 9);
    return true;
}
auto command(Runtime& r, uint64_t seq, uint64_t state, uint64_t value) -> bool {
    return record(r.control, 1024, message(r.nonce, r.deadline, seq, state, r.row[5], value), bound(r.deadline));
}
auto ack(Runtime& r, uint64_t seq, uint64_t state, uint64_t value) -> bool {
    return record(r.control, 1536, message(r.nonce, r.deadline, seq, state, r.row[5], value), bound(r.deadline));
}
auto reap(Runtime& r, int64_t& child) -> bool {
    auto pid = child;
    int status = -1;
    int64_t result{};
    auto end = bound(r.deadline);
    while (now() < end) {
        result = proc(2, pid, reinterpret_cast<uint64_t>(&status), 1);
        if (result != 0 && result != -EINTR) break;
        pause();
    }
    if (result == pid || result == -ECHILD) child = -1;
    r.event(WAITPID, pid, bits(status), result);
    NEED(r, result == pid && status == 0, 10);
    return true;
}
void cancel(int64_t& child) {
    if (child <= 0) return;
    (void)proc(8, child, SIGKILL);
    int status{};
    auto end = bound(UINT64_MAX);
    while (now() < end) {
        auto result = proc(2, child, reinterpret_cast<uint64_t>(&status), 1);
        if (result == child || result == -ECHILD) {
            child = -1;
            return;
        }
        if (result != 0 && result != -EINTR) return;
        pause();
    }
}
auto child_work(Runtime& r, uint64_t inherited, uint64_t inherited_ro, int64_t& descendant) -> bool {
    NEED(r, grant(r, 9, 2, false) && unmap(r, inherited) && unmap(r, inherited_ro), 11);
    for (int fd : {10, 11, 12}) NEED(r, close(fd) == 0, 12);
    NEED(r, normalize(shm_open(NAME, O_RDWR, 0), 13) && grant(r, 13, 2, true), 13);
    uint64_t b{};
    NEED(r, map(r, 13, PROT_READ | PROT_WRITE, CHILD_VA, b), 14);
    r.row[18] = b;
    NEED(r, observe(r, b, 1), 15);
    r.event(READY, b, 0, 0);
    NEED(r, record(9, 512, message(r.nonce, r.deadline, 0, 1, r.row[5], b), bound(r.deadline)), 16);
    Record request{};
    NEED(r, receive(9, 1024, r.nonce, r.deadline, 1, 1, request) && request[7] == marker(r.nonce, 2) && observe(r, b, 2), 17);
    NEED(r, ack(r, 1, 1, marker(r.nonce, 2)), 18);
    NEED(r,
         receive(9, 1024, r.nonce, r.deadline, 2, 2, request) && request[7] == marker(r.nonce, 3) && observe(r, b, 3) &&
             mapped_store(r, b, 4) && ack(r, 2, 2, marker(r.nonce, 4)),
         19);
    NEED(r, receive(9, 1024, r.nonce, r.deadline, 3, 3, request) && request[7] == marker(r.nonce, 5), 20);
    NEED(r, normalize(shm_open(NAME, O_RDONLY, 0), 14) && grant(r, 14, 0, true), 21);
    uint64_t ro{};
    NEED(r, map(r, 14, PROT_READ, CHILD_RO, ro) && observe(r, ro, 5), 22);
    r.row[19] = ro;
    auto end = bound(r.deadline);
    NEED(r, now() < end, 23);
    NEED(r, record(9, 2048, message(r.nonce, r.deadline, 3, 3, r.row[5], ro), end), 24);
    auto start = now();
    NEED(r, start < end, 24);
    timespec timeout{static_cast<time_t>((end - start) / 1000000), static_cast<long>(((end - start) % 1000000) * 1000)};
    auto waited = ker::futex::wait(reinterpret_cast<int*>(ro + FUTEX_OFFSET), 0, &timeout);
    r.event(WAIT, ro + FUTEX_OFFSET, end - start, waited);
    NEED(r, waited == 0 && observe(r, ro, 6) && ack(r, 3, 3, marker(r.nonce, 6)), 25);
    descendant = proc(5);
    r.event(FORK, 1, 0, descendant);
    r.row[20] = bits(descendant);
    if (descendant == 0) {
        Runtime g(4, r.nonce, r.deadline);
        g.row[21] = r.row[21];
        bool ok = grant(g, 9, 2, false) && grant(g, 13, 2, true) && grant(g, 14, 0, true) && observe(g, b, 6) && observe(g, ro, 6);
        if (!g.publish()) ok = false;
        ker::process::exit(ok ? 0 : 80);
        __builtin_unreachable();
    }
    NEED(r, descendant > 0 && reap(r, descendant), 26);
    NEED(r, close(13) == 0 && close(14) == 0, 27);
    r.event(EXEC, r.row[5], b, 0, ro);
    NEED(r, r.publish(), 28);
    char nonce[24]{}, deadline[24]{}, old[24]{};
    std::snprintf(nonce, sizeof(nonce), "%llu", static_cast<unsigned long long>(r.nonce));
    std::snprintf(deadline, sizeof(deadline), "%llu", static_cast<unsigned long long>(r.deadline));
    std::snprintf(old, sizeof(old), "%llu", static_cast<unsigned long long>(b));
    const char* args[]{PRIVATE, "--after-exec", nonce, deadline, old, nullptr};
    const char* env[]{"LD_LIBRARY_PATH=/usr/lib", nullptr};
    auto result = ker::process::execve(PRIVATE, args, env);
    return r.fail(100 + static_cast<uint64_t>(-result));
}
auto after_exec(Runtime& r, uint64_t old) -> bool {
    Record request{};
    NEED(r, receive(9, 0, r.nonce, r.deadline, 0, 1, request) && grant(r, 9, 2, false), 29);
    r.row[21] = request[7];
    for (int fd : {10, 11, 12, 13, 14}) NEED(r, raw(5, 28, fd, F_GETFD) == -EBADF, 30);
    auto copied = raw(5, 27, 9, old, 16, SCRATCH + 8);
    Scratch scratch{};
    NEED(r, io(9, &scratch, sizeof(scratch), SCRATCH, false, bound(r.deadline)), 31);
    r.event(IMAGE, r.row[5], old, copied, scratch.before, scratch.after, scratch.data.value | scratch.data.complement);
    NEED(r,
         copied == -EFAULT && scratch.before == (GUARD ^ r.nonce) && scratch.after == ~scratch.before && scratch.data.value == 0 &&
             scratch.data.complement == 0,
         32);
    return true;
}
auto controller_work(Runtime& r, uint64_t& a, uint64_t& ro, int64_t& child) -> bool {
    Record request{};
    NEED(r, receive(9, 0, r.nonce, r.deadline, 0, 1, request) && grant(r, 9, 2, false), 33);
    r.row[21] = request[7];
    NEED(r, r.row[5] == 1 && r.row[6] == 0, 34);
    for (auto directory : {"/dev", "/dev/shm"}) {
        auto result = mkdir(directory, 0700);
        struct stat st{};
        NEED(r, (result == 0 || errno == EEXIST) && stat(directory, &st) == 0 && S_ISDIR(st.st_mode), 35);
    }
    NEED(r, normalize(shm_open(NAME, O_CREAT | O_EXCL | O_RDWR, 0600), 10), 36);
    auto seed = initial(r.nonce);
    auto truncated = ftruncate(10, EOF_BYTES);
    auto seeded = truncated == 0 ? pwrite(10, seed.data(), EOF_BYTES, 0) : -1;
    r.event(SEED, EOF_BYTES, 0, seeded, checksum(seed.data(), EOF_BYTES), checksum(seed.data() + PAGE, 73), bits(actual(truncated)));
    NEED(r, truncated == 0 && seeded == static_cast<ssize_t>(EOF_BYTES) && grant(r, 10, 2, true), 37);
    NEED(r, normalize(shm_open(NAME, O_RDONLY, 0), 11) && grant(r, 11, 0, true) && reader(r, 11, 1), 38);
    NEED(r, map(r, 10, PROT_READ | PROT_WRITE, PARENT_VA, a), 39);
    r.row[17] = a;
    auto rw_flags = fcntl(10, F_GETFL), ro_flags = fcntl(11, F_GETFL);
    r.event(FLAGS, 10, 11, 0, bits(rw_flags), bits(ro_flags));
    NEED(r, bits(rw_flags) == r.row[15] && bits(ro_flags) == r.row[16], 39);
    NEED(r, normalize(shm_open(NAME, O_RDWR, 0), 12), 40);
    NEED(r, close(10) == 0, 41);
    auto reused = dup2(9, 10);
    struct stat st{};
    auto sr = fstat(10, &st);
    r.event(REUSE, 10, 9, reused, st.st_dev, st.st_ino, bits(sr));
    NEED(r, reused == 10 && sr == 0 && st.st_dev == r.row[22] && st.st_ino == r.row[23], 42);
    child = proc(5);
    r.event(FORK, 0, 0, child);
    r.row[20] = bits(child);
    if (child == 0) {
        Runtime c(2, r.nonce, r.deadline);
        c.row[21] = r.row[21];
        c.row[17] = a;
        int64_t grandchild = -1;
        bool ok = child_work(c, a, ro, grandchild);
        if (!ok) {
            cancel(grandchild);
            (void)c.publish();
        }
        ker::process::exit(ok ? 0 : 80);
        __builtin_unreachable();
    }
    NEED(r,
         child > 0 && receive(9, 512, r.nonce, r.deadline, 0, 1, request) && request[6] == static_cast<uint64_t>(child) &&
             request[7] == CHILD_VA,
         43);
    NEED(r,
         mapped_store(r, a, 2) && command(r, 1, 1, marker(r.nonce, 2)) && receive(9, 1536, r.nonce, r.deadline, 1, 1, request) &&
             request[7] == marker(r.nonce, 2) && reader(r, 11, 2),
         44);
    auto value = payload(marker(r.nonce, 3));
    auto written = pwrite(12, &value, sizeof(value), 0);
    r.event(PWRITE, 12, 0, written, value.value, value.complement, marker(r.nonce, 3));
    NEED(r,
         written == 16 && observe(r, a, 3) && reader(r, 11, 3) && command(r, 2, 2, marker(r.nonce, 3)) &&
             receive(9, 1536, r.nonce, r.deadline, 2, 2, request) && request[7] == marker(r.nonce, 4) && observe(r, a, 4) &&
             reader(r, 11, 4),
         45);
    // Reach the decisive writable/cached-reader oracle before testing the new
    // ordinary readonly-map admission. The genuine old RAW kernel can fail on
    // actual visibility rather than stopping first on its readonly refusal.
    NEED(r, map(r, 11, PROT_READ, PARENT_RO, ro) && observe(r, ro, 4), 45);
    r.row[19] = ro;
    auto protect = [&](int prot) {
        auto result = raw(7, 2, a, LENGTH, prot);
        r.event(PROTECT, a, prot, result, LENGTH);
        return result == 0;
    };
    NEED(r, protect(PROT_READ), 46);
    auto copied = raw(5, 26, 12, a, 16, 0);
    auto unchanged = load(ro);
    r.event(COPY_TO, a, 16, copied, unchanged.value, unchanged.complement, marker(r.nonce, 4));
    NEED(r, copied == -EFAULT && matches(unchanged, marker(r.nonce, 4)) && protect(PROT_NONE), 47);
    copied = raw(5, 27, 9, a, 16, SCRATCH + 8);
    Scratch scratch{};
    NEED(r, io(9, &scratch, sizeof(scratch), SCRATCH, false, bound(r.deadline)), 48);
    r.event(COPY_FROM, a, 16, copied, scratch.before, scratch.after, scratch.data.value | scratch.data.complement);
    NEED(r,
         copied == -EFAULT && scratch.before == (GUARD ^ r.nonce) && scratch.after == ~scratch.before && scratch.data.value == 0 &&
             scratch.data.complement == 0 && protect(PROT_READ | PROT_WRITE) && mapped_store(r, a, 5) && reader(r, 11, 5),
         49);
    for (unsigned op = 1; op <= 4; ++op) {
        int64_t result = op == 1   ? raw(8, 0x4200000000ULL, LENGTH, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, 11, 0)
                         : op == 2 ? raw(7, 2, ro, LENGTH, PROT_READ | PROT_WRITE)
                         : op == 3 ? raw(8, 0x4200000000ULL, LENGTH, PROT_READ | PROT_EXEC, MAP_SHARED | MAP_FIXED, 12, 0)
                                   : raw(7, 2, a, LENGTH, PROT_READ | PROT_EXEC);
        r.event(DENY, op, 0, result);
        if (result > 0 && (op == 1 || op == 3)) (void)raw(7, 1, bits(result), LENGTH);
        NEED(r, result == -EACCES, 50);
    }
    for (int flags : {MS_SYNC, MS_ASYNC, MS_SYNC | MS_INVALIDATE, 8, MS_SYNC | MS_ASYNC}) {
        auto result = actual(msync(reinterpret_cast<void*>(a), LENGTH, flags));
        r.event(SYNC, a, flags, result, LENGTH);
        NEED(r, result == ((flags == 8 || flags == (MS_SYNC | MS_ASYNC)) ? -EINVAL : 0), 51);
    }
    auto busy = actual(ftruncate(12, LENGTH));
    struct stat busy_stat{};
    auto stat_result = fstat(12, &busy_stat);
    r.event(GROW_BUSY, LENGTH, EOF_BYTES, busy, busy_stat.st_size, bits(stat_result));
    NEED(r, busy == -EBUSY && stat_result == 0 && busy_stat.st_size == static_cast<off_t>(EOF_BYTES), 52);
    auto* tail = reinterpret_cast<unsigned char*>(a + EOF_BYTES);
    for (size_t i = 0; i < LENGTH - EOF_BYTES; ++i) __atomic_store_n(tail + i, 0xa5, __ATOMIC_RELAXED);
    r.event(TAIL, a + EOF_BYTES, LENGTH - EOF_BYTES, 0, 0xa5, checksum(tail, LENGTH - EOF_BYTES));
    NEED(r, command(r, 3, 3, marker(r.nonce, 5)) && receive(9, 2048, r.nonce, r.deadline, 3, 3, request) && request[7] == CHILD_RO, 53);
    NEED(r, mapped_store(r, a, 6), 54);
    auto wake_end = bound(r.deadline);
    int64_t woke{};
    unsigned attempts{};
    for (; attempts < 128 && now() < wake_end; ++attempts) {
        woke = ker::futex::wake(reinterpret_cast<int*>(a + FUTEX_OFFSET), 1);
        if (woke == 1 || (woke != 0 && woke != 1)) {
            ++attempts;
            break;
        }
        timespec delay{0, 10'000'000};
        (void)nanosleep(&delay, nullptr);
    }
    r.event(WAKE, a + FUTEX_OFFSET, attempts, woke, CHILD_RO + FUTEX_OFFSET);
    NEED(r, woke == 1 && receive(9, 1536, r.nonce, r.deadline, 3, 3, request) && request[7] == marker(r.nonce, 6) && reap(r, child), 55);
    NEED(r, unmap(r, a) && unmap(r, ro), 56);
    auto grow_end = bound(r.deadline);
    int64_t grown = -EBUSY;
    unsigned retries{};
    struct stat final_stat{};
    for (; retries < 64 && now() < grow_end; ++retries) {
        grown = actual(ftruncate(12, LENGTH));
        if (grown == 0 || grown != -EBUSY) {
            ++retries;
            break;
        }
        timespec delay{0, 50'000'000};
        (void)nanosleep(&delay, nullptr);
    }
    stat_result = fstat(12, &final_stat);
    r.event(GROW, LENGTH, retries, grown, final_stat.st_size, bits(stat_result));
    NEED(r, grown == 0 && stat_result == 0 && final_stat.st_size == static_cast<off_t>(LENGTH), 57);
    Bytes bytes{};
    NEED(r, io(12, bytes.data(), bytes.size(), 0, false, bound(r.deadline)), 58);
    auto expected = initial(r.nonce);
    value = payload(marker(r.nonce, 6));
    std::memcpy(expected.data(), &value, sizeof(value));
    auto zero = std::all_of(bytes.begin() + EOF_BYTES, bytes.end(), [](unsigned char c) { return c == 0; });
    r.event(TAIL_READ, EOF_BYTES, LENGTH - EOF_BYTES, 0, checksum(bytes.data(), bytes.size()),
            checksum(bytes.data() + EOF_BYTES, LENGTH - EOF_BYTES), checksum(bytes.data() + PAGE, 73));
    NEED(r, zero && bytes == expected, 59);
    auto c10 = close(10), c11 = close(11), c12 = close(12), removed = shm_unlink(NAME);
    r.event(CLEANUP, 10, 12, 0, bits(c10), bits(c11), bits(c12) | bits(removed));
    NEED(r, c10 == 0 && c11 == 0 && c12 == 0 && removed == 0, 60);
    return true;
}
[[noreturn]] void controller(uint64_t nonce, uint64_t deadline) {
    Runtime r(1, nonce, deadline);
    uint64_t a{}, ro{};
    int64_t child = -1;
    bool ok = controller_work(r, a, ro, child);
    if (!ok) {
        cancel(child);
        if (a) (void)raw(7, 1, a, LENGTH);
        if (ro) (void)raw(7, 1, ro, LENGTH);
    }
    if (!r.publish()) ok = false;
    ker::process::exit(ok && r.row[9] == 0 ? 0 : 80);
    __builtin_unreachable();
}
auto number(const char* text, uint64_t& out) -> bool {
    if (text == nullptr || *text == 0) return false;
    out = 0;
    for (; *text; ++text) {
        if (*text < '0' || *text > '9' || out > (UINT64_MAX - static_cast<unsigned>(*text - '0')) / 10) return false;
        out = out * 10 + static_cast<unsigned>(*text - '0');
    }
    return true;
}
auto save(const char* name, const void* data, size_t size) -> bool {
    char path[160]{};
    auto n = std::snprintf(path, sizeof(path), "%s/%s", RESULTS, name);
    if (n <= 0 || static_cast<size_t>(n) >= sizeof(path)) return false;
    int fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return false;
    bool ok = io(fd, const_cast<void*>(data), size, 0, true, bound(UINT64_MAX)) && fsync(fd) == 0;
    return close(fd) == 0 && ok;
}
auto text(const char* s) -> abi::String { return {reinterpret_cast<uint64_t>(s), std::strlen(s)}; }
auto create(Runtime& r) -> int64_t {
    abi::Spec spec{};
    for (size_t i = 0; i < 16; ++i) {
        abi::Limit limit{};
        if (proc(48, i, reinterpret_cast<uint64_t>(&limit), 1)) return -EIO;
        spec.limits[i] = limit;
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
    auto child = good ? proc(abi::CREATE_OPERATION, reinterpret_cast<uint64_t>(&spec)) : -EIO;
    r.row[20] = bits(child);  // Record ownership before any fallible cleanup.
    for (auto a : assets)
        if (a.source_fd >= 0 && close(a.source_fd)) good = false;
    r.event(CREATE, 1, 0, child, good);
    return child;
}
auto host() -> int {
    auto start = now();
    if (start > UINT64_MAX - RUN_US) return 81;
    auto nonce = (start ^ (bits(proc(3)) << 32) ^ GUARD) | (1ULL << 63) | 1;
    Runtime r(0, nonce, start + RUN_US, -1);
    struct stat directory{};
    if (stat(RESULTS, &directory) || !S_ISDIR(directory.st_mode) || (directory.st_mode & 07777) != 0700 || directory.st_uid != 100 ||
        directory.st_gid != 100)
        return 81;
    for (int fd = 0; fd < 256; ++fd) {
        auto result = raw(5, 28, fd, F_GETFD);
        if (result >= 0 && close(fd)) return 81;
        if (result < 0 && result != -EBADF) return 81;
    }
    char path[160]{};
    auto n = std::snprintf(path, sizeof(path), "%s/control.bin", RESULTS);
    if (n <= 0 || static_cast<size_t>(n) >= sizeof(path)) return 81;
    r.control = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC | O_NO_CACHE, 0600);
    if (r.control < 0) return 81;
    Scratch scratch{GUARD ^ nonce, {}, ~(GUARD ^ nonce)};
    bool ok = ftruncate(r.control, CONTROL_BYTES) == 0 && io(r.control, &scratch, sizeof(scratch), SCRATCH, true, bound(r.deadline));
    int64_t child = ok ? create(r) : -1;
    bool consumed = false;
    ok = ok && child > 2 && r.row[HEAD + 6] == 1;
    if (ok) ok = record(r.control, 0, message(nonce, r.deadline, 0, 1, r.row[5], bits(child)), bound(r.deadline));
    int status = -1;
    int64_t result{};
    while (ok && now() < r.deadline) {
        result = proc(2, child, reinterpret_cast<uint64_t>(&status), 1);
        if (result == child || result == -ECHILD) {
            consumed = true;
            break;
        }
        if (result != 0 && result != -EINTR) {
            ok = false;
            break;
        }
        pause();
    }
    r.event(REAP, bits(child), bits(status), result);
    ok = ok && consumed && result == child && status == 0;
    if (!consumed) cancel(child);
    // Preserve every committed prefix independently, even when the controller failed.
    for (unsigned role = 1; role <= 4; ++role) {
        Row a{}, b{};
        auto offset = 8192 + static_cast<off_t>(role) * sizeof(Row);
        bool held = io(r.control, a.data(), sizeof(a), offset, false, bound(UINT64_MAX)) &&
                    io(r.control, b.data(), sizeof(b), offset, false, bound(UINT64_MAX)) && a == b && a.back() == nonce;
        char name[32]{};
        std::snprintf(name, sizeof(name), "role-%u.bin", role);
        if (held && !save(name, a.data(), sizeof(a))) ok = false;
        if (!held) ok = false;
    }
    if (!ok) r.fail(70);
    r.row[8] = now();
    r.row.back() = nonce;
    bool saved = save("host.bin", r.row.data(), sizeof(r.row));
    if (close(r.control)) ok = false;
    std::array<char, 512> status_line{};
    n = std::snprintf(status_line.data(), status_line.size(), "%s tmpfs-writeshared nonce=%llu failures=%llu\n",
                      ok && saved ? "PUBLIC_COMPLETE" : "EXECUTION_FAIL", static_cast<unsigned long long>(nonce),
                      static_cast<unsigned long long>(r.row[9] + (ok && saved ? 0 : 1)));
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
                if (good && std::strcmp(service.service, "tmpfs-writeshared-probe") == 0) {
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
        "%s tmpfs-writeshared-init-exit service=tmpfs-writeshared-probe generation=%llu restarts=%llu sequence=%llu published-ns=%llu "
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
    uint64_t nonce{}, deadline{}, old{};
    if ((argc != 4 && argc != 5) || !number(argv[2], nonce) || !number(argv[3], deadline) || !(nonce & (1ULL << 63)) || !(nonce & 1) ||
        deadline <= now() || deadline - now() > RUN_US)
        return 82;
    if (argc == 4 && std::strcmp(argv[1], "--controller") == 0) controller(nonce, deadline);
    if (argc == 5 && std::strcmp(argv[1], "--after-exec") == 0 && number(argv[4], old) && old == CHILD_VA) {
        Runtime r(3, nonce, deadline);
        bool ok = after_exec(r, old);
        if (!r.publish()) ok = false;
        return ok && r.row[9] == 0 ? 0 : 80;
    }
    return 82;
}
