// Actual Normal-kernel SMP registry qualification; no synthetic scheduler Tasks.
#include <fcntl.h>
#include <pthread.h>
#include <sys/init_control.h>
#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
using Op = ker::abi::multiproc::threadControlOps;
constexpr char RESULTS[] = "/srv/cpu-domain-results";
constexpr uint64_t MAGIC = 0x3633444F4D41494EULL;
constexpr uint64_t VERSION = 1;
constexpr uint64_t GUARD = 0xA596C387E1D24B6FULL;
constexpr uint64_t ONLINE = 0xFF;
constexpr unsigned CREATORS = 4, READERS = 2, WORKERS = CREATORS + READERS;
constexpr unsigned WAVES = 16, SLOTS = 64, SWEEPS = 8;
constexpr uint64_t QUERIES = WAVES * SWEEPS * SLOTS;
constexpr uint64_t RUN_US = 60'000'000, REAP_US = 5'000'000;
constexpr int64_t PENDING = INT64_MIN;
constexpr size_t PAGE = 4096;
using Request = std::array<uint8_t, 42>;

struct Query {
    uint64_t mask{};
    std::array<uint32_t, SLOTS> loads{};
};
struct GuardedQuery {
    uint64_t before = GUARD;
    Query output{};
    uint64_t after = GUARD;
};
static_assert(sizeof(Request) == 42 && sizeof(Query) == 264);
static_assert(offsetof(GuardedQuery, output) == 8 && offsetof(GuardedQuery, after) == 272);
static_assert(static_cast<uint64_t>(Op::CREATE_DOMAIN) == 0x106 && static_cast<uint64_t>(Op::QUERY_DOMAIN) == 0x108);
static_assert(std::atomic<int64_t>::is_always_lock_free && std::atomic<unsigned>::is_always_lock_free);

enum Failure : uint64_t {
    CLOCK = 1,
    CPU,
    GUARDS,
    QUERY_RESULT,
    QUERY_MASK,
    QUERY_LOADS,
    INVALID_OUTPUT,
    ABI,
    FRESH,
    STARTUP,
    BARRIER,
    DEADLINE,
    CREATE_RESULT,
    REQUEST_CHANGED,
    CHANGED_MASK,
    ID_MAPPING,
    REPORT_IO,
    REPORT_HEADER,
    REPORT_COUNTS,
    JOIN,
    WAIT,
    HOST_STATE,
    CAPACITY,
    UNIQUE_ID,
    OVERLAP
};
struct Checks {
    uint64_t checks{}, failures{}, first_code{}, first_id{}, first_actual{}, first_expected{};
    void expect(bool condition, Failure code, uint64_t id = 0, uint64_t actual = 0, uint64_t expected = 0) {
        ++checks;
        if (condition) return;
        ++failures;
        if (first_code != 0) return;
        first_code = code;
        first_id = id;
        first_actual = actual;
        first_expected = expected;
    }
};
static_assert(sizeof(Checks) == 48);
struct Outcome {
    uint64_t ticket{}, input_mask{};
    int64_t result = PENDING;
    uint64_t cpu_before{}, cpu_after{}, start_us{}, end_us{};
};
static_assert(sizeof(Outcome) == 56);
struct WorkerReport {
    uint64_t magic = MAGIC, version = VERSION, bytes = sizeof(WorkerReport);
    uint64_t nonce{}, pid{}, tid{}, role{}, index{};  // role0 creator, role1 reader
    Checks checks{};
    uint64_t cpu_seen{}, queries{}, overlap_queries{}, calls{}, completed_waves{};
    std::array<uint64_t, SLOTS> seen_masks{};
    std::array<Outcome, WAVES> outcomes{};
};
struct HostReport {
    uint64_t magic = MAGIC, version = VERSION, bytes = sizeof(HostReport);
    uint64_t nonce{}, pid{}, child{};
    Checks checks{};
    int64_t waited{};
    uint64_t wait_status{}, timed_out{}, workers{}, queries{}, successes{}, refused{};
    uint64_t creator_cpus{}, reader_cpus{}, overlap_queries{}, state_preserved{};
    std::array<int64_t, SLOTS> outcomes{};
    std::array<uint64_t, SLOTS> masks{};
};
static_assert(sizeof(WorkerReport) == 1560 && sizeof(HostReport) == 1208);
static_assert(WORKERS * sizeof(WorkerReport) + sizeof(HostReport) + 1024 <= 64 * 1024);

auto now_us() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    return static_cast<uint64_t>(value.tv_sec) * 1'000'000 + static_cast<uint64_t>(value.tv_nsec) / 1000;
}
void pause_poll() {
    timespec pause{0, 1'000'000};
    (void)nanosleep(&pause, nullptr);
}
auto ptr(const void* value) -> uint64_t { return reinterpret_cast<uint64_t>(value); }
auto raw(Op op, uint64_t a1, uint64_t a2 = 0) -> int64_t {
    return static_cast<int64_t>(syscall(ker::abi::callnums::threading, static_cast<uint64_t>(op), a1, a2));
}
auto mask_for(unsigned ticket) -> uint64_t { return uint64_t{0x80} | ticket; }
auto request_for(unsigned ticket) -> Request {
    Request request{};
    char name[32]{};
    std::snprintf(name, sizeof(name), "cpu36-ticket-%u", ticket);
    std::memcpy(request.data(), name, sizeof(name));
    uint64_t const MASK = mask_for(ticket) | ((ticket & 1U) != 0 ? (uint64_t{1} << 63) : 0);
    std::memcpy(request.data() + 32, &MASK, sizeof(MASK));
    // bytes40/41 remain false: no penalty/hard-placement qualification.
    return request;
}
auto untouched(const Query& query) -> bool {
    std::array<uint8_t, sizeof(Query)> expected{};
    expected.fill(0xA5);
    return std::memcmp(&query, expected.data(), expected.size()) == 0;
}
auto loads_valid(const Query& query) -> bool {
    for (unsigned cpu = 0; cpu < SLOTS; ++cpu)
        if ((query.mask & (uint64_t{1} << cpu)) == 0 && query.loads[cpu] != 0) return false;
    return true;
}
auto query(Checks& checks, uint64_t id, Query& result, bool unpublished_allowed) -> int64_t {
    GuardedQuery buffer{};
    std::memset(&buffer.output, 0xA5, sizeof(buffer.output));
    int64_t const RC = raw(Op::QUERY_DOMAIN, id, ptr(&buffer.output));
    checks.expect(buffer.before == GUARD && buffer.after == GUARD, GUARDS, id);
    if (RC == 0) {
        bool const VALID = id < 2 ? buffer.output.mask == ONLINE : buffer.output.mask >= 0x80 && buffer.output.mask <= 0xBF;
        checks.expect(VALID, QUERY_MASK, id, buffer.output.mask);
        checks.expect(loads_valid(buffer.output), QUERY_LOADS, id);
        result = buffer.output;
    } else {
        checks.expect(RC == -EINVAL && unpublished_allowed && id >= 2, QUERY_RESULT, id, static_cast<uint64_t>(RC));
        checks.expect(untouched(buffer.output), INVALID_OUTPUT, id);
    }
    return RC;
}
void observe_cpu(WorkerReport& report, uint64_t cpu) {
    report.checks.expect(cpu < 8, CPU, report.index, cpu, 8);
    if (cpu < 8) report.cpu_seen |= uint64_t{1} << cpu;
}
auto report_path(char* path, size_t capacity, const char* kind, unsigned index) -> bool {
    int const COUNT = std::snprintf(path, capacity, "%s/%s-%u.bin", RESULTS, kind, index);
    return COUNT >= 0 && static_cast<size_t>(COUNT) < capacity;
}
auto persist(const char* path, const void* data, size_t size) -> bool {
    int const FD = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (FD < 0) return false;
    auto const* bytes = static_cast<const uint8_t*>(data);
    size_t offset = 0;
    while (offset < size) {
        ssize_t const COUNT = write(FD, bytes + offset, size - offset);
        if (COUNT < 0 && errno == EINTR) continue;
        if (COUNT <= 0) break;
        offset += static_cast<size_t>(COUNT);
    }
    bool const FLUSHED = fsync(FD) == 0;
    bool const CLOSED = close(FD) == 0;
    return offset == size && FLUSHED && CLOSED;
}
auto read_report(const char* path, WorkerReport& report) -> bool {
    int const FD = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (FD < 0) return false;
    struct stat metadata{};
    bool const SIZED = fstat(FD, &metadata) == 0 && S_ISREG(metadata.st_mode) && metadata.st_size == static_cast<off_t>(sizeof(report));
    ssize_t count;
    do {
        count = pread(FD, &report, sizeof(report), 0);
    } while (count < 0 && errno == EINTR);
    bool const CLOSED = close(FD) == 0;
    return SIZED && count == static_cast<ssize_t>(sizeof(report)) && CLOSED;
}

struct State;
struct Argument {
    State* state{};
    unsigned index{};
    bool reader{};
};
struct State {
    uint64_t nonce{}, deadline{};
    uint8_t* boundary_request{};
    std::atomic<int> gate{0};  // 0 starting,1 barrier ready,-1 abort
    std::atomic<unsigned> active_creators{0};
    std::atomic<unsigned> report_errors{0};
    std::array<std::atomic<int64_t>, SLOTS> accepted{};
    std::array<WorkerReport, WORKERS> reports{};
    pthread_barrier_t barrier{};
};
void meet(State& state, Checks& checks) {
    int const RC = pthread_barrier_wait(&state.barrier);
    checks.expect(RC == 0 || RC == PTHREAD_BARRIER_SERIAL_THREAD, BARRIER, 0, static_cast<uint64_t>(RC));
}
void create_wave(State& state, WorkerReport& report, unsigned wave) {
    unsigned const TICKET = wave * CREATORS + static_cast<unsigned>(report.index);
    Request const REQUEST = request_for(TICKET);
    Request submitted = REQUEST;
    auto& record = report.outcomes[wave];
    record.ticket = TICKET;
    std::memcpy(&record.input_mask, REQUEST.data() + 32, sizeof(record.input_mask));
    const void* address = submitted.data();
    if (TICKET == 0) address = state.boundary_request;
    record.cpu_before = ker::multiproc::getcurrent_cpu();
    record.start_us = now_us();
    state.active_creators.fetch_add(1, std::memory_order_acq_rel);
    record.result = raw(Op::CREATE_DOMAIN, ptr(address));
    state.active_creators.fetch_sub(1, std::memory_order_acq_rel);
    record.end_us = now_us();
    record.cpu_after = ker::multiproc::getcurrent_cpu();
    ++report.calls;
    observe_cpu(report, record.cpu_before);
    observe_cpu(report, record.cpu_after);
    report.checks.expect(record.start_us != UINT64_MAX && record.end_us != UINT64_MAX && record.end_us >= record.start_us, CLOCK, TICKET);
    report.checks.expect(std::memcmp(address, REQUEST.data(), REQUEST.size()) == 0, REQUEST_CHANGED, TICKET);
    if (TICKET == 0) report.checks.expect(state.boundary_request[-1] == 0x4D, GUARDS, TICKET);
    bool const CREATED = record.result >= 2 && record.result < SLOTS;
    report.checks.expect(CREATED || (wave == WAVES - 1 && record.result == -ENOMEM), CREATE_RESULT, TICKET,
                         static_cast<uint64_t>(record.result));
    state.accepted[TICKET].store(record.result, std::memory_order_release);
}
void read_wave(State& state, WorkerReport& report) {
    for (unsigned sweep = 0; sweep < SWEEPS; ++sweep) {
        for (unsigned id = 0; id < SLOTS; ++id) {
            observe_cpu(report, ker::multiproc::getcurrent_cpu());
            unsigned const BEFORE = state.active_creators.load(std::memory_order_acquire);
            Query output{};
            int64_t const RC = query(report.checks, id, output, true);
            unsigned const AFTER = state.active_creators.load(std::memory_order_acquire);
            ++report.queries;
            if (BEFORE != 0 || AFTER != 0) ++report.overlap_queries;
            if (RC != 0) {
                report.checks.expect(report.seen_masks[id] == 0, CHANGED_MASK, id, static_cast<uint64_t>(RC));
                continue;
            }
            uint64_t const OLD = report.seen_masks[id];
            report.checks.expect(OLD == 0 || OLD == output.mask, CHANGED_MASK, id, output.mask, OLD);
            report.seen_masks[id] = output.mask;
            if (id >= 2 && output.mask >= 0x80 && output.mask <= 0xBF) {
                unsigned const TICKET = static_cast<unsigned>(output.mask & 0x3F);
                int64_t const ACCEPTED = state.accepted[TICKET].load(std::memory_order_acquire);
                report.checks.expect(ACCEPTED == PENDING || ACCEPTED == static_cast<int64_t>(id), ID_MAPPING, id,
                                     static_cast<uint64_t>(ACCEPTED), TICKET);
            }
        }
    }
}
auto worker(void* opaque) -> void* {
    auto const& argument = *static_cast<Argument*>(opaque);
    auto& state = *argument.state;
    auto& report = state.reports[argument.reader ? CREATORS + argument.index : argument.index];
    report.nonce = state.nonce;
    report.pid = ker::process::getpid();
    report.tid = ker::multiproc::currentThreadId();
    report.role = argument.reader ? 1 : 0;
    report.index = argument.index;
    report.checks.expect(report.tid != 0 && report.pid != 0, STARTUP);
    report.checks.expect(ker::process::getuid() == 100 && ker::process::geteuid() == 100 && ker::process::getgid() == 100, HOST_STATE);
    int gate;
    while ((gate = state.gate.load(std::memory_order_acquire)) == 0) {
        if (now_us() >= state.deadline) {
            report.checks.expect(false, DEADLINE);
            // The supervisor will stop the exact child. Do not enter an
            // uninitialized barrier or silently pretend all workers existed.
            state.gate.compare_exchange_strong(gate, -1, std::memory_order_acq_rel);
        }
        ker::multiproc::yield();
    }
    report.checks.expect(gate == 1, STARTUP, report.index, static_cast<uint64_t>(gate), 1);
    if (gate == 1) {
        for (unsigned wave = 0; wave < WAVES; ++wave) {
            meet(state, report.checks);
            report.checks.expect(now_us() < state.deadline, DEADLINE, wave);
            if (argument.reader)
                read_wave(state, report);
            else
                create_wave(state, report, wave);
            meet(state, report.checks);
            ++report.completed_waves;
        }
    }
    char path[128]{};
    bool const SAVED =
        report_path(path, sizeof(path), argument.reader ? "reader" : "creator", argument.index) && persist(path, &report, sizeof(report));
    if (!SAVED) state.report_errors.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
}
auto workload(uint64_t nonce, uint64_t deadline, uint8_t* boundary) -> int {
    State state{};
    state.nonce = nonce;
    state.deadline = deadline;
    state.boundary_request = boundary;
    for (auto& value : state.accepted) value.store(PENDING, std::memory_order_relaxed);
    std::array<Argument, WORKERS> arguments{};
    std::array<pthread_t, WORKERS> threads{};
    Checks checks{};
    pthread_attr_t attributes{};
    bool const ATTR = pthread_attr_init(&attributes) == 0;
    checks.expect(ATTR, STARTUP);
    bool configured = ATTR && pthread_attr_setstacksize(&attributes, 256 * 1024) == 0 && pthread_attr_setguardsize(&attributes, PAGE) == 0;
    checks.expect(configured, STARTUP);
    unsigned created = 0;
    if (configured) {
        for (unsigned i = 0; i < WORKERS; ++i) {
            arguments[i] = {&state, i < CREATORS ? i : i - CREATORS, i >= CREATORS};
            int const RC = pthread_create(&threads[i], &attributes, worker, &arguments[i]);
            checks.expect(RC == 0, STARTUP, i, static_cast<uint64_t>(RC));
            if (RC != 0) break;
            ++created;
        }
    }
    if (ATTR) checks.expect(pthread_attr_destroy(&attributes) == 0, STARTUP);
    bool const BARRIER_READY = created == WORKERS && pthread_barrier_init(&state.barrier, nullptr, WORKERS + 1) == 0;
    checks.expect(BARRIER_READY, STARTUP, 0, created, WORKERS);
    int expected = 0;
    bool const STARTED = BARRIER_READY && state.gate.compare_exchange_strong(expected, 1, std::memory_order_release);
    if (!STARTED) state.gate.store(-1, std::memory_order_release);
    checks.expect(STARTED, STARTUP);
    if (STARTED) {
        for (unsigned wave = 0; wave < WAVES; ++wave) {
            meet(state, checks);
            checks.expect(now_us() < deadline, DEADLINE, wave);
            meet(state, checks);
        }
    }
    for (unsigned i = 0; i < created; ++i) {
        void* value = reinterpret_cast<void*>(1);
        int const RC = pthread_join(threads[i], &value);
        checks.expect(RC == 0, JOIN, i, static_cast<uint64_t>(RC));
        if (RC != 0) {
            // EXIT closes whole-process thread admission, requests every
            // sibling's retirement and waits for their CPU contributions.
            // Do not read an unjoined report or unwind its borrowed State.
            ker::process::exit(50);
            __builtin_unreachable();
        }
        checks.expect(value == nullptr, JOIN, i);
        checks.expect(state.reports[i].checks.failures == 0, REPORT_COUNTS, i, state.reports[i].checks.failures);
    }
    if (BARRIER_READY) checks.expect(pthread_barrier_destroy(&state.barrier) == 0, BARRIER);
    checks.expect(state.report_errors.load(std::memory_order_relaxed) == 0, REPORT_IO);
    return checks.failures == 0 ? 0 : 50;
}

void abi_probes(Checks& checks, uint8_t* page) {
    Request request = request_for(0);
    uint64_t mask = 0;
    std::memcpy(request.data() + 32, &mask, sizeof(mask));
    checks.expect(raw(Op::CREATE_DOMAIN, ptr(request.data())) == -EINVAL, ABI, 1);
    mask = uint64_t{1} << 63;
    std::memcpy(request.data() + 32, &mask, sizeof(mask));
    checks.expect(raw(Op::CREATE_DOMAIN, ptr(request.data())) == -EINVAL, ABI, 2);
    checks.expect(raw(Op::CREATE_DOMAIN, 0) == -EFAULT, ABI, 3);
    std::memcpy(page + PAGE - 41, request.data(), 41);
    checks.expect(raw(Op::CREATE_DOMAIN, ptr(page + PAGE - 41)) == -EFAULT, ABI, 4);
    checks.expect(raw(Op::QUERY_DOMAIN, 0, 0) == -EFAULT, ABI, 5);
    checks.expect(raw(Op::QUERY_DOMAIN, 0, ptr(page + PAGE - 263)) == -EFAULT, ABI, 6);
    for (uint64_t id : {uint64_t{64}, uint64_t{UINT32_MAX}}) {
        GuardedQuery output{};
        std::memset(&output.output, 0xA5, sizeof(output.output));
        checks.expect(raw(Op::QUERY_DOMAIN, id, ptr(&output.output)) == -EINVAL && untouched(output.output) && output.before == GUARD &&
                          output.after == GUARD,
                      ABI, id);
    }
    uint8_t* const EXACT = page + PAGE - sizeof(Query);
    EXACT[-1] = 0x6B;
    checks.expect(raw(Op::QUERY_DOMAIN, 0, ptr(EXACT)) == 0 && EXACT[-1] == 0x6B, ABI, 9);
    Query output{};
    std::memcpy(&output, EXACT, sizeof(output));
    checks.expect(output.mask == ONLINE && loads_valid(output), ABI, 10);
    // Reuse this mapping for one valid42-byte request, with unreadable byte42.
    Request const VALID = request_for(0);
    page[PAGE - VALID.size() - 1] = 0x4D;
    std::memcpy(page + PAGE - VALID.size(), VALID.data(), VALID.size());
}
auto wait_bounded(int64_t child, int& status, uint64_t deadline) -> int64_t {
    for (;;) {
        int64_t const RC = ker::process::waitpid(child, &status, WNOHANG, nullptr);
        if (RC != 0 && RC != -EINTR) return RC;
        if (now_us() >= deadline) return 0;
        pause_poll();
    }
}
void reconcile(HostReport& host, int64_t child) {
    std::array<uint64_t, WORKERS> tids{};
    uint64_t ids = 3;
    host.masks[0] = host.masks[1] = ONLINE;
    for (auto& outcome : host.outcomes) outcome = PENDING;
    for (unsigned slot = 0; slot < WORKERS; ++slot) {
        bool const READER = slot >= CREATORS;
        unsigned const INDEX = READER ? slot - CREATORS : slot;
        char path[128]{};
        WorkerReport report{};
        bool const READ = report_path(path, sizeof(path), READER ? "reader" : "creator", INDEX) && read_report(path, report);
        host.checks.expect(READ, REPORT_IO, slot);
        if (!READ) continue;
        bool const HEADER = report.magic == MAGIC && report.version == VERSION && report.bytes == sizeof(report) &&
                            report.nonce == host.nonce && report.pid == static_cast<uint64_t>(child) && report.tid != 0 &&
                            report.role == (READER ? 1U : 0U) && report.index == INDEX;
        host.checks.expect(HEADER, REPORT_HEADER, slot);
        if (!HEADER) continue;
        ++host.workers;
        for (unsigned earlier = 0; earlier < slot; ++earlier) host.checks.expect(tids[earlier] != report.tid, UNIQUE_ID, slot);
        tids[slot] = report.tid;
        host.checks.expect(report.checks.failures == 0 && report.checks.first_code == 0 && report.completed_waves == WAVES, REPORT_COUNTS,
                           slot, report.checks.first_code);
        if (READER) {
            host.checks.expect(report.queries == QUERIES && report.calls == 0, REPORT_COUNTS, slot, report.queries, QUERIES);
            host.queries += report.queries;
            host.reader_cpus |= report.cpu_seen;
            host.overlap_queries += report.overlap_queries;
            for (unsigned id = 0; id < SLOTS; ++id)
                if (report.seen_masks[id] != 0)
                    host.checks.expect(report.seen_masks[id] == host.masks[id], ID_MAPPING, id, report.seen_masks[id], host.masks[id]);
            continue;
        }
        host.checks.expect(report.calls == WAVES && report.queries == 0, REPORT_COUNTS, slot, report.calls, WAVES);
        host.creator_cpus |= report.cpu_seen;
        for (unsigned wave = 0; wave < WAVES; ++wave) {
            auto const& record = report.outcomes[wave];
            unsigned const TICKET = wave * CREATORS + INDEX;
            Request const REQUEST = request_for(TICKET);
            uint64_t mask{};
            std::memcpy(&mask, REQUEST.data() + 32, sizeof(mask));
            host.checks.expect(record.ticket == TICKET && record.input_mask == mask && record.cpu_before < 8 && record.cpu_after < 8 &&
                                   record.start_us != UINT64_MAX && record.end_us != UINT64_MAX && record.end_us >= record.start_us,
                               REPORT_COUNTS, TICKET);
            host.outcomes[TICKET] = record.result;
            if (record.result >= 2 && record.result < SLOTS) {
                uint64_t const BIT = uint64_t{1} << record.result;
                host.checks.expect((ids & BIT) == 0, UNIQUE_ID, TICKET, static_cast<uint64_t>(record.result));
                ids |= BIT;
                host.masks[static_cast<unsigned>(record.result)] = mask_for(TICKET);
                ++host.successes;
            } else {
                host.checks.expect(wave == WAVES - 1 && record.result == -ENOMEM, CREATE_RESULT, TICKET,
                                   static_cast<uint64_t>(record.result));
                if (record.result == -ENOMEM) ++host.refused;
            }
        }
    }
    host.checks.expect(host.workers == WORKERS && host.queries == READERS * QUERIES, REPORT_COUNTS);
    host.checks.expect(host.successes == 62 && host.refused == 2 && ids == UINT64_MAX, CAPACITY, 0, host.successes, 62);
    host.checks.expect((host.creator_cpus & (host.creator_cpus - 1)) != 0 && host.reader_cpus != 0, CPU, 0, host.creator_cpus);
    host.checks.expect(host.overlap_queries != 0, OVERLAP);
}
auto supervisor() -> int {
    HostReport host{};
    host.pid = ker::process::getpid();
    uint64_t const START = now_us();
    host.nonce = START ^ (host.pid << 32);
    auto& checks = host.checks;
    checks.expect(START != UINT64_MAX && host.nonce != 0, CLOCK);
    uint64_t const UID = ker::process::getuid(), EUID = ker::process::geteuid(), GID = ker::process::getgid();
    checks.expect(UID == 100 && EUID == 100 && GID == 100, HOST_STATE);
    errno = 0;
    int const NICE = getpriority(PRIO_PROCESS, 0);
    checks.expect(errno == 0, HOST_STATE);
    uint64_t const TID = ker::multiproc::currentThreadId();
    int64_t const AFFINITY = ker::multiproc::getThreadAffinityMask(TID);
    checks.expect(AFFINITY == static_cast<int64_t>(ONLINE) && ker::multiproc::nativeThreadCount() == 8, CPU);
    checks.expect(ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) == 0, STARTUP);
    for (unsigned id = 0; id < SLOTS; ++id) {
        Query output{};
        int64_t const RC = query(checks, id, output, true);
        checks.expect(id < 2 ? RC == 0 && output.mask == ONLINE : RC == -EINVAL, FRESH, id, static_cast<uint64_t>(RC));
    }
    auto* page = static_cast<uint8_t*>(mmap(nullptr, PAGE * 2, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    bool const MAPPED = page != MAP_FAILED;
    checks.expect(MAPPED, ABI);
    bool const GUARDED = MAPPED && mprotect(page + PAGE, PAGE, PROT_NONE) == 0;
    checks.expect(GUARDED, ABI);
    if (GUARDED) abi_probes(checks, page);
    // A rejected CREATE must not consume a slot.
    Query absent{};
    checks.expect(query(checks, 2, absent, true) == -EINVAL, FRESH, 2);
    if (checks.failures == 0) {
        int64_t const CHILD = ker::process::fork();
        if (CHILD == 0) {
            int const RESULT = workload(host.nonce, START + RUN_US, page + PAGE - sizeof(Request));
            ker::process::exit(RESULT);
            __builtin_unreachable();
        }
        checks.expect(CHILD > 0, STARTUP, 0, static_cast<uint64_t>(CHILD));
        if (CHILD > 0) {
            host.child = static_cast<uint64_t>(CHILD);
            int status{};
            host.waited = wait_bounded(CHILD, status, START + RUN_US);
            if (host.waited == 0) {
                host.timed_out = 1;
                checks.expect(ker::process::kill(CHILD, 9) == 0, WAIT, host.child);
                uint64_t const NOW = now_us();
                host.waited = NOW == UINT64_MAX ? 0 : wait_bounded(CHILD, status, NOW + REAP_US);
            }
            host.wait_status = static_cast<uint32_t>(status);
            checks.expect(host.waited == CHILD && host.timed_out == 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0, WAIT, host.child,
                          static_cast<uint64_t>(host.waited), host.wait_status);
            if (host.waited == CHILD) {
                reconcile(host, CHILD);
                for (unsigned i = 0; i < 4; ++i) {
                    Request const FULL = request_for(i);
                    checks.expect(raw(Op::CREATE_DOMAIN, ptr(FULL.data())) == -ENOMEM, CAPACITY, i);
                }
                for (unsigned id = 0; id < SLOTS; ++id) {
                    Query output{};
                    int64_t const RC = query(checks, id, output, false);
                    checks.expect(RC == 0 && output.mask == host.masks[id], ID_MAPPING, id, output.mask, host.masks[id]);
                }
                GuardedQuery invalid{};
                std::memset(&invalid.output, 0xA5, sizeof(invalid.output));
                checks.expect(raw(Op::QUERY_DOMAIN, SLOTS, ptr(&invalid.output)) == -EINVAL && untouched(invalid.output) &&
                                  invalid.before == GUARD && invalid.after == GUARD,
                              CAPACITY, SLOTS);
                uint8_t* const EXACT = page + PAGE - sizeof(Query);
                EXACT[-1] = 0x6B;
                checks.expect(raw(Op::QUERY_DOMAIN, 63, ptr(EXACT)) == 0 && EXACT[-1] == 0x6B, ABI, 63);
                Query output{};
                std::memcpy(&output, EXACT, sizeof(output));
                checks.expect(output.mask == host.masks[63] && loads_valid(output), ABI, 63);
            }
        }
    }
    if (MAPPED) checks.expect(munmap(page, PAGE * 2) == 0, ABI);
    errno = 0;
    int const AFTER_NICE = getpriority(PRIO_PROCESS, 0);
    host.state_preserved = ker::process::getpid() == host.pid && ker::process::getuid() == UID && ker::process::geteuid() == EUID &&
                           ker::process::getgid() == GID && AFTER_NICE == NICE && errno == 0 && ker::multiproc::currentThreadId() == TID &&
                           ker::multiproc::getThreadAffinityMask(TID) == AFFINITY;
    checks.expect(host.state_preserved != 0, HOST_STATE);
    checks.expect(now_us() < START + RUN_US + REAP_US, DEADLINE);
    char path[128]{};
    bool const HOST_SAVED = report_path(path, sizeof(path), "host", 0) && persist(path, &host, sizeof(host));
    char line[512]{};
    bool const PASSED = checks.failures == 0 && HOST_SAVED;
    int const COUNT = std::snprintf(
        line, sizeof(line),
        "%s cpu-domain-runtime nonce=%llu child=%llu waited=%lld status=%llu workers=%llu waves=16 attempts=64 successes=%llu "
        "refused=%llu queries=%llu overlap=%llu creator-cpus=0x%llx host-preserved=%llu failures=%llu first=%llu\n",
        PASSED ? "PASS" : "FAIL", static_cast<unsigned long long>(host.nonce), static_cast<unsigned long long>(host.child),
        static_cast<long long>(host.waited), static_cast<unsigned long long>(host.wait_status),
        static_cast<unsigned long long>(host.workers), static_cast<unsigned long long>(host.successes),
        static_cast<unsigned long long>(host.refused), static_cast<unsigned long long>(host.queries),
        static_cast<unsigned long long>(host.overlap_queries), static_cast<unsigned long long>(host.creator_cpus),
        static_cast<unsigned long long>(host.state_preserved), static_cast<unsigned long long>(checks.failures + (HOST_SAVED ? 0 : 1)),
        static_cast<unsigned long long>(checks.first_code));
    bool const LINE_VALID = COUNT > 0 && static_cast<size_t>(COUNT) < sizeof(line);
    std::snprintf(path, sizeof(path), "%s/status", RESULTS);
    bool const STATUS_SAVED = LINE_VALID && persist(path, line, static_cast<size_t>(COUNT));
    if (LINE_VALID && STATUS_SAVED)
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(COUNT));
    else {
        constexpr char FAILED[] = "FAIL cpu-domain-runtime status-write failures=1\n";
        (void)write(STDOUT_FILENO, FAILED, sizeof(FAILED) - 1);
    }
    return PASSED && STATUS_SAVED ? 0 : 51;
}

// A separate service runs this only after init's exit-success dependency is
// satisfied. It observes init's reap; it cannot observe its own future exit.
auto init_exit_witness() -> int {
    namespace init = ker::abi::init_control;
    constexpr char SERVICE[] = "cpu-domain-probe";
    constexpr uint64_t POLL_US = 30'000'000;
    struct GuardedSnapshot {
        uint64_t before = GUARD;
        init::StatusSnapshot output{};
        uint64_t after = GUARD;
    };
    static_assert(sizeof(init::StatusSnapshot) == 4160 && init::ABI_VERSION == 1);
    static_assert(offsetof(GuardedSnapshot, output) == 8 && offsetof(GuardedSnapshot, after) == 4168);
    static_assert(WORKERS * sizeof(WorkerReport) + sizeof(HostReport) + sizeof(init::StatusSnapshot) + 1024 <= 64 * 1024);

    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    uint64_t const UID = ker::process::getuid(), EUID = ker::process::geteuid();
    uint64_t const START = now_us();
    bool const CLOCK_VALID = START != UINT64_MAX && START <= UINT64_MAX - POLL_US;
    bool observed = false;
    bool history_exit = false;
    int64_t read_error = !CLOCK_VALID ? -EIO : (UID == 0 || EUID == 0 ? -EPERM : 0);
    if (read_error == 0) {
        uint64_t const DEADLINE = START + POLL_US;
        for (;;) {
            uint64_t const NOW = now_us();
            if (NOW == UINT64_MAX || NOW < START) {
                read_error = -EIO;
                break;
            }
            if (NOW >= DEADLINE) {
                read_error = -ETIMEDOUT;
                break;
            }
            GuardedSnapshot buffer{};
            int64_t const RC = ker::process::init_status_read(&buffer.output);
            uint64_t const AFTER_READ = now_us();
            if (AFTER_READ == UINT64_MAX || AFTER_READ < NOW) {
                read_error = -EIO;
                break;
            }
            if (AFTER_READ >= DEADLINE) {
                read_error = -ETIMEDOUT;
                break;
            }
            if (RC != 0 && RC != -EAGAIN) {
                read_error = RC;
                break;
            }
            if (buffer.before != GUARD || buffer.after != GUARD) {
                read_error = -EOVERFLOW;
                break;
            }
            if (RC == 0) {
                snapshot = buffer.output;
                bool supported = snapshot.size == sizeof(snapshot) && snapshot.version == init::ABI_VERSION && snapshot.sequence != 0 &&
                                 snapshot.published_mono_ns != 0 && snapshot.service_count != 0 &&
                                 snapshot.service_count <= init::MAX_STATUS_SERVICES;
                if (supported) {
                    for (uint32_t index = 0; index < snapshot.service_count; ++index) {
                        auto const& status = snapshot.services[index];
                        supported = supported && std::memchr(status.service, '\0', sizeof(status.service)) != nullptr &&
                                    status.history_count <= init::MAX_TRANSITION_HISTORY;
                    }
                }
                if (!supported) {
                    read_error = -EPROTO;
                    break;
                }
                unsigned matches = 0;
                for (uint32_t index = 0; index < snapshot.service_count; ++index) {
                    auto const& status = snapshot.services[index];
                    if (std::strcmp(status.service, SERVICE) == 0) {
                        selected = status;
                        ++matches;
                    }
                }
                if (matches > 1) {
                    read_error = -EPROTO;
                    break;
                }
                history_exit = false;
                if (matches == 1) {
                    for (uint32_t index = 0; index < selected.history_count; ++index) {
                        auto const& transition = selected.history[index];
                        if (transition.reason == init::TransitionReason::EXIT && transition.to_state == init::ServiceState::EXITED &&
                            transition.timestamp_mono_ns != 0 && transition.timestamp_mono_ns <= snapshot.published_mono_ns)
                            history_exit = true;
                    }
                }
                observed = matches == 1 && selected.generation == 1 && selected.restart_count == 0 &&
                           selected.state == init::ServiceState::EXITED && selected.readiness == init::ReadinessState::READY &&
                           selected.last_wait_status == 0 && selected.last_error == 0 && history_exit;
                if (observed) break;
            }
            pause_poll();
        }
    }
    char path[128]{};
    std::snprintf(path, sizeof(path), "%s/init-exit-snapshot.bin", RESULTS);
    bool const SNAPSHOT_SAVED = persist(path, &snapshot, sizeof(snapshot));
    bool const PASSED = observed && read_error == 0 && SNAPSHOT_SAVED;
    char line[512]{};
    int const COUNT = std::snprintf(
        line, sizeof(line),
        "%s cpu-domain-init-exit service=cpu-domain-probe generation=%llu restarts=%llu sequence=%llu published-ns=%llu "
        "state=%u readiness=%u wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u uid=%llu read-error=%lld failures=%u\n",
        PASSED ? "PASS" : "FAIL", static_cast<unsigned long long>(selected.generation),
        static_cast<unsigned long long>(selected.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned long long>(snapshot.published_mono_ns), static_cast<unsigned>(selected.state),
        static_cast<unsigned>(selected.readiness), selected.last_wait_status, selected.last_error, history_exit ? 1U : 0U, snapshot.size,
        snapshot.version, static_cast<unsigned long long>(UID), static_cast<long long>(read_error), PASSED ? 0U : 1U);
    bool const LINE_VALID = COUNT > 0 && static_cast<size_t>(COUNT) < sizeof(line);
    std::snprintf(path, sizeof(path), "%s/init-exit-status", RESULTS);
    bool const STATUS_SAVED = LINE_VALID && persist(path, line, static_cast<size_t>(COUNT));
    if (LINE_VALID && STATUS_SAVED)
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(COUNT));
    else {
        constexpr char FAILED[] = "FAIL cpu-domain-init-exit receipt-write failures=1\n";
        (void)write(STDOUT_FILENO, FAILED, sizeof(FAILED) - 1);
    }
    return PASSED && STATUS_SAVED ? 0 : 54;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 1) return supervisor();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return init_exit_witness();
    return 52;
}
