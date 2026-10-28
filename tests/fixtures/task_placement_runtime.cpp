// Actual Normal-kernel public placement and pipe-wake qualification.
#include <fcntl.h>
#include <pthread.h>
#include <sys/init_control.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/resource.h>
#include <sys/stat.h>
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
constexpr char RESULTS[] = "/srv/task-placement-results";
constexpr uint64_t MAGIC = 0x39335441534B504CULL, VERSION = 1;
constexpr uint64_t GUARD = 0xA596C387E1D24B6FULL, ONLINE = 0xFF;
constexpr unsigned WORKERS = 16, SETTERS = 2, CONTROLLERS = 3;
constexpr unsigned CALLS = 1024, EVENTS = 256, HARD_OBSERVATIONS = 64;
constexpr uint64_t RUN_US = 60'000'000, PARK_US = 2'000'000;
constexpr size_t PAGE = 4096;
using Request = std::array<uint8_t, 42>;
struct Query {
    uint64_t mask{};
    std::array<uint32_t, 64> loads{};
};
struct GuardedQuery {
    uint64_t before = GUARD;
    Query output{};
    uint64_t after = GUARD;
};
static_assert(sizeof(Request) == 42 && sizeof(Query) == 264);
static_assert(offsetof(GuardedQuery, output) == 8 && offsetof(GuardedQuery, after) == 272);
static_assert(static_cast<uint64_t>(Op::SET_AFFINITY) == 0x104 && static_cast<uint64_t>(Op::GET_AFFINITY) == 0x105 &&
              static_cast<uint64_t>(Op::CREATE_DOMAIN) == 0x106 && static_cast<uint64_t>(Op::SET_DOMAIN) == 0x107 &&
              static_cast<uint64_t>(Op::QUERY_DOMAIN) == 0x108);
static_assert(std::atomic<uint64_t>::is_always_lock_free && std::atomic<unsigned>::is_always_lock_free);

enum Failure : uint64_t {
    CLOCK = 1,
    DEADLINE,
    IDENTITY,
    STARTUP,
    CPU,
    PIN,
    AFFINITY,
    ABI,
    DOMAIN,
    WAITING,
    HARD_CPU,
    PIPE,
    CONTROL,
    OVERLAP,
    JOIN,
    REPORT_IO
};
struct Checks {
    uint64_t checks{}, failures{}, first_code{}, first_id{}, first_actual{}, first_expected{};
    void expect(bool condition, Failure code, uint64_t id = 0, uint64_t actual = 0, uint64_t expected = 0) {
        ++checks;
        if (condition) return;
        ++failures;
        if (first_code == 0) {
            first_code = code;
            first_id = id;
            first_actual = actual;
            first_expected = expected;
        }
    }
};
struct WorkerReport {
    uint64_t magic = MAGIC, version = VERSION, bytes = sizeof(WorkerReport);
    uint64_t nonce{}, pid{}, tid{}, role{}, index{};
    Checks checks{};
    uint64_t initial_cpu{}, first_cpu{}, pin_mask{}, pin_calls{}, pin_busy{}, cpu_seen{}, hard_observations{}, hard_wrong{};
    uint64_t pipe_reads{}, pipe_acks{}, yields{}, phases{}, local_selected{};
};
struct Outcome {
    uint64_t input{};
    int64_t result{};
};
struct ControlReport {
    uint64_t magic = MAGIC, version = VERSION, bytes = sizeof(ControlReport);
    uint64_t nonce{}, pid{}, tid{}, role{}, index{};  // role1 setter, role2 getter
    Checks checks{};
    uint64_t calls{}, successes{}, busy{}, overlap{}, cpu_seen{}, active_samples{}, completed{}, wake_changes{};
    std::array<Outcome, CALLS> outcomes{};
};
struct HostReport {
    uint64_t magic = MAGIC, version = VERSION, bytes = sizeof(HostReport);
    uint64_t nonce{}, pid{}, tid{};
    Checks checks{};
    uint64_t uid{}, euid{}, gid{}, cpu_count{};
    int64_t local_rc{};
    uint64_t workers{}, controllers{}, setup_calls{}, setup_busy{}, initial_cpu_mask{}, target_tid{}, initial_cpu{}, destination_cpu{},
        domain_id{}, domain_mask{}, subset_mask{};
    uint64_t park_start_us{}, park_probe_us{};
    int64_t park_exclude_rc{}, park_before_get{}, park_after_get{};
    uint64_t hard_calls{}, hard_busy{};
    int64_t hard_rc{}, hard_get{};
    uint64_t full_calls{}, full_busy{};
    int64_t full_rc{}, full_get{};
    uint64_t pipe_roundtrips{}, control_successes{}, control_busy{}, control_overlap{}, wake_changes{}, unique_tids{}, started_us{},
        ended_us{}, closed_fds{};
    std::array<int64_t, 12> selector_results{};
};
static_assert(sizeof(Checks) == 48 && sizeof(WorkerReport) == 216 && sizeof(Outcome) == 16);
static_assert(sizeof(ControlReport) == 16560 && sizeof(HostReport) == 496);
static_assert(WORKERS * sizeof(WorkerReport) + CONTROLLERS * sizeof(ControlReport) + sizeof(HostReport) + 4160 + 1024 < 64 * 1024);

auto now_us() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    return static_cast<uint64_t>(value.tv_sec) * 1'000'000 + static_cast<uint64_t>(value.tv_nsec) / 1000;
}
void pause_us(uint64_t duration) {
    timespec remaining{static_cast<time_t>(duration / 1'000'000), static_cast<long>((duration % 1'000'000) * 1000)};
    while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
    }
}
auto ptr(const void* value) -> uint64_t { return reinterpret_cast<uint64_t>(value); }
auto raw(Op op, uint64_t a1, uint64_t a2 = 0, uint64_t a3 = 0) -> int64_t {
    return static_cast<int64_t>(syscall(ker::abi::callnums::threading, static_cast<uint64_t>(op), a1, a2, a3));
}
auto persist(const char* path, const void* data, size_t size) -> bool;
[[noreturn]] void abort_live(Failure code, uint64_t id = 0, int64_t actual = 0) {
    char line[256]{};
    int const COUNT =
        std::snprintf(line, sizeof(line), "FAIL task-placement-runtime live code=%llu id=%llu actual=%lld\n",
                      static_cast<unsigned long long>(code), static_cast<unsigned long long>(id), static_cast<long long>(actual));
    if (COUNT > 0 && static_cast<size_t>(COUNT) < sizeof(line)) {
        (void)persist("/srv/task-placement-results/failure", line, static_cast<size_t>(COUNT));
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(COUNT));
    }
    // Whole-process EXIT retires siblings. Never unwind their borrowed State.
    ker::process::exit(55);
    __builtin_unreachable();
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
void diagnostic_stage(const char* stage) {
    char path[128]{}, line[256]{};
    int const PATH_SIZE = std::snprintf(path, sizeof(path), "%s/phase-%s", RESULTS, stage);
    int const COUNT = std::snprintf(line, sizeof(line), "task-placement-runtime stage=%s\n", stage);
    if (PATH_SIZE <= 0 || static_cast<size_t>(PATH_SIZE) >= sizeof(path) || COUNT <= 0 || static_cast<size_t>(COUNT) >= sizeof(line) ||
        !persist(path, line, static_cast<size_t>(COUNT)))
        abort_live(REPORT_IO);
}
auto report_path(char* path, size_t capacity, const char* kind, unsigned index) -> bool {
    int const COUNT = std::snprintf(path, capacity, "%s/%s-%u.bin", RESULTS, kind, index);
    return COUNT > 0 && static_cast<size_t>(COUNT) < capacity;
}
struct Published {
    std::atomic<uint64_t> tid{0}, cpu{UINT64_MAX};
    std::atomic<uint64_t> requested_cpu{UINT64_MAX};
    std::atomic<bool> confirmed{false};
};
struct State {
    uint64_t nonce{}, start{}, deadline{}, destination{}, subset{}, target_tid{};
    int data_read{-1}, ack_write{-1};
    std::atomic<unsigned> phase{0}, ready{0}, controller_ready{0}, active_setters{0}, active_getters{0}, event_progress{0};
    std::atomic<bool> stop{false}, setup_done{false}, park_requested{false}, phase1_active{false}, controller_go{false};
    std::atomic<uint64_t> park_start{0}, hard_seen{0};
    std::array<Published, WORKERS> published{};
    std::array<WorkerReport, WORKERS> workers{};
    std::array<ControlReport, CONTROLLERS> controls{};
};
void diagnostic_failure_context(const State& state, Failure code, uint64_t now) {
    uint64_t const TID = ker::multiproc::currentThreadId();
    unsigned const PHASE = state.phase.load(std::memory_order_acquire);
    unsigned const EVENTS_DONE = state.event_progress.load(std::memory_order_acquire);
    uint64_t const HARD_SEEN = state.hard_seen.load(std::memory_order_acquire);
    char line[256]{};
    int const COUNT = std::snprintf(
        line, sizeof(line),
        "FAIL task-placement-runtime context code=%llu tid=%llu phase=%u events=%u hard-seen=%llu start-us=%llu deadline-us=%llu "
        "now-us=%llu\n",
        static_cast<unsigned long long>(code), static_cast<unsigned long long>(TID), PHASE, EVENTS_DONE,
        static_cast<unsigned long long>(HARD_SEEN), static_cast<unsigned long long>(state.start),
        static_cast<unsigned long long>(state.deadline), static_cast<unsigned long long>(now));
    if (COUNT > 0 && static_cast<size_t>(COUNT) < sizeof(line))
        (void)persist("/srv/task-placement-results/failure-context", line, static_cast<size_t>(COUNT));
}
struct Argument {
    State* state{};
    unsigned index{};
};
void alive(const State& state) {
    // CLOCK_GETTIME writable usercopy currently invalidates the shared pagemap.
    // Keep the same absolute deadline while bounding that incidental traffic.
    static thread_local unsigned polls = 0;
    if ((polls++ & 63U) != 0) return;
    uint64_t const NOW = now_us();
    if (NOW == UINT64_MAX || NOW < state.start) {
        diagnostic_failure_context(state, CLOCK, NOW);
        abort_live(CLOCK);
    }
    if (NOW >= state.deadline) {
        diagnostic_failure_context(state, DEADLINE, NOW);
        abort_live(DEADLINE);
    }
}
void observe(Checks& checks, uint64_t& seen, uint64_t cpu, unsigned index) {
    checks.expect(cpu < 8, CPU, index, cpu, 8);
    if (cpu >= 8) abort_live(CPU, index, static_cast<int64_t>(cpu));
    seen |= uint64_t{1} << cpu;
}
auto worker(void* opaque) -> void* {
    auto const& argument = *static_cast<Argument*>(opaque);
    auto& state = *argument.state;
    auto& report = state.workers[argument.index];
    report.nonce = state.nonce;
    report.pid = ker::process::getpid();
    report.tid = ker::multiproc::currentThreadId();
    report.index = argument.index;
    int64_t const LOCAL = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    report.local_selected = LOCAL == 0 ? 1 : 0;
    if (LOCAL != 0 || report.tid == 0) abort_live(IDENTITY, argument.index, LOCAL);
    for (;;) {
        alive(state);
        uint64_t const CPU = ker::multiproc::getcurrent_cpu();
        observe(report.checks, report.cpu_seen, CPU, argument.index);
        ++report.pin_calls;
        int64_t const RC = raw(Op::SET_AFFINITY, report.tid, uint64_t{1} << CPU);
        if (RC == -EBUSY) {
            ++report.pin_busy;
            (void)ker::multiproc::yield();
            continue;
        }
        if (RC != 0) abort_live(PIN, argument.index, RC);
        report.initial_cpu = CPU;
        report.first_cpu = CPU;
        report.pin_mask = uint64_t{1} << CPU;
        if (ker::multiproc::getcurrent_cpu() != CPU || raw(Op::GET_AFFINITY, report.tid) != static_cast<int64_t>(report.pin_mask))
            abort_live(PIN, argument.index);
        break;
    }
    state.published[argument.index].cpu.store(report.initial_cpu, std::memory_order_relaxed);
    state.published[argument.index].tid.store(report.tid, std::memory_order_release);
    state.ready.fetch_add(1, std::memory_order_release);
    // The parent uses real checked cross-CPU setters while these threads yield.
    // A requested destination is confirmed only by executing there ourselves.
    while (!state.setup_done.load(std::memory_order_acquire)) {
        alive(state);
        uint64_t const CPU = ker::multiproc::getcurrent_cpu();
        observe(report.checks, report.cpu_seen, CPU, argument.index);
        uint64_t const REQUESTED = state.published[argument.index].requested_cpu.load(std::memory_order_acquire);
        if (REQUESTED == CPU && !state.published[argument.index].confirmed.load(std::memory_order_relaxed)) {
            ++report.pin_calls;
            int64_t const RC = raw(Op::SET_AFFINITY, report.tid, uint64_t{1} << CPU);
            if (RC == -EBUSY)
                ++report.pin_busy;
            else {
                if (RC != 0 || ker::multiproc::getcurrent_cpu() != CPU ||
                    raw(Op::GET_AFFINITY, report.tid) != static_cast<int64_t>(uint64_t{1} << CPU))
                    abort_live(PIN, argument.index, RC);
                report.initial_cpu = CPU;
                report.pin_mask = uint64_t{1} << CPU;
                state.published[argument.index].cpu.store(CPU, std::memory_order_release);
                state.published[argument.index].confirmed.store(true, std::memory_order_release);
            }
        }
        ++report.yields;
        (void)ker::multiproc::yield();
    }
    bool parked = false;
    for (;;) {
        alive(state);
        unsigned const PHASE = state.phase.load(std::memory_order_acquire);
        report.phases |= uint64_t{1} << PHASE;
        if (state.stop.load(std::memory_order_acquire)) break;
        if (PHASE == 0) {
            if (argument.index == 0 && state.park_requested.load(std::memory_order_acquire) && !parked) {
                state.park_start.store(now_us(), std::memory_order_release);
                parked = true;
                pause_us(PARK_US);
            } else
                pause_us(1000);
            continue;
        }
        if (argument.index == 0 && PHASE == 1) state.phase1_active.store(true, std::memory_order_release);
        uint64_t const CPU = ker::multiproc::getcurrent_cpu();
        observe(report.checks, report.cpu_seen, CPU, argument.index);
        if (argument.index == 0 && PHASE == 2 && report.hard_observations < HARD_OBSERVATIONS) {
            ++report.hard_observations;
            if (CPU != state.destination) {
                ++report.hard_wrong;
                abort_live(HARD_CPU, argument.index, static_cast<int64_t>(CPU));
            }
            state.hard_seen.store(report.hard_observations, std::memory_order_release);
        }
        if (argument.index == 0 && PHASE == 3) {
            uint8_t byte{};
            ssize_t rc;
            do {
                rc = read(state.data_read, &byte, 1);
            } while (rc < 0 && errno == EINTR);
            alive(state);
            if (rc != 1) abort_live(PIPE, report.pipe_reads, rc);
            if (byte == 0 && state.stop.load(std::memory_order_acquire)) break;
            uint8_t const EXPECTED = static_cast<uint8_t>(report.pipe_reads % 254 + 1);
            if (report.pipe_reads >= EVENTS || byte != EXPECTED) abort_live(PIPE, report.pipe_reads, byte);
            ++report.pipe_reads;
            do {
                rc = write(state.ack_write, &byte, 1);
            } while (rc < 0 && errno == EINTR);
            if (rc != 1) abort_live(PIPE, report.pipe_acks, rc);
            ++report.pipe_acks;
        } else {
            ++report.yields;
            (void)ker::multiproc::yield();
        }
    }
    report.checks.expect(report.local_selected == 1 && report.pin_calls == report.pin_busy + 2, PIN, argument.index);
    return nullptr;
}
auto controller(void* opaque) -> void* {
    auto const& argument = *static_cast<Argument*>(opaque);
    auto& state = *argument.state;
    auto& report = state.controls[argument.index];
    report.nonce = state.nonce;
    report.pid = ker::process::getpid();
    report.tid = ker::multiproc::currentThreadId();
    report.role = argument.index < SETTERS ? 1 : 2;
    report.index = argument.index;
    if (report.tid == 0 || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) != 0)
        abort_live(IDENTITY, argument.index);
    state.controller_ready.fetch_add(1, std::memory_order_release);
    while (!state.controller_go.load(std::memory_order_acquire)) {
        alive(state);
        (void)ker::multiproc::yield();
    }
    while (state.event_progress.load(std::memory_order_acquire) == 0) {
        alive(state);
        (void)ker::multiproc::yield();
    }
    for (unsigned index = 0; index < CALLS; ++index) {
        alive(state);
        observe(report.checks, report.cpu_seen, ker::multiproc::getcurrent_cpu(), argument.index);
        auto& outcome = report.outcomes[index];
        unsigned before{}, after{};
        unsigned const BEFORE_PROGRESS = state.event_progress.load(std::memory_order_acquire);
        if (report.role == 1) {
            outcome.input = ((index + argument.index) & 1U) == 0 ? ONLINE : state.subset;
            state.active_setters.fetch_add(1, std::memory_order_acq_rel);
            before = state.active_getters.load(std::memory_order_acquire) + state.active_setters.load(std::memory_order_acquire) - 1;
            outcome.result = raw(Op::SET_AFFINITY, state.target_tid, outcome.input);
            if (index < HARD_OBSERVATIONS) pause_us(1000);
            after = state.active_getters.load(std::memory_order_acquire) + state.active_setters.load(std::memory_order_acquire) - 1;
            state.active_setters.fetch_sub(1, std::memory_order_acq_rel);
            if (outcome.result != 0 && outcome.result != -EBUSY) abort_live(CONTROL, argument.index, outcome.result);
        } else {
            state.active_getters.fetch_add(1, std::memory_order_acq_rel);
            before = state.active_setters.load(std::memory_order_acquire);
            outcome.result = raw(Op::GET_AFFINITY, state.target_tid);
            if (index < HARD_OBSERVATIONS) pause_us(1000);
            after = state.active_setters.load(std::memory_order_acquire);
            state.active_getters.fetch_sub(1, std::memory_order_acq_rel);
            if (outcome.result != static_cast<int64_t>(ONLINE) && outcome.result != static_cast<int64_t>(state.subset) &&
                outcome.result != -EBUSY)
                abort_live(CONTROL, argument.index, outcome.result);
        }
        ++report.calls;
        if (outcome.result == -EBUSY)
            ++report.busy;
        else
            ++report.successes;
        report.active_samples += before + after;
        if (before != 0 || after != 0) ++report.overlap;
        unsigned const AFTER_PROGRESS = state.event_progress.load(std::memory_order_acquire);
        if (BEFORE_PROGRESS != 0 && BEFORE_PROGRESS < EVENTS && AFTER_PROGRESS > BEFORE_PROGRESS && AFTER_PROGRESS <= EVENTS)
            ++report.wake_changes;
        (void)ker::multiproc::yield();
    }
    report.completed = 1;
    report.checks.expect(report.calls == CALLS && report.calls == report.successes + report.busy && report.successes != 0, CONTROL,
                         argument.index, report.successes);
    report.checks.expect(report.overlap != 0, OVERLAP, argument.index);
    report.checks.expect(report.wake_changes != 0, OVERLAP, argument.index);
    return nullptr;
}
void expect_live(Checks& checks, bool condition, Failure code, uint64_t id = 0, int64_t actual = 0, uint64_t expected = 0) {
    checks.expect(condition, code, id, static_cast<uint64_t>(actual), expected);
    if (!condition) abort_live(code, id, actual);
}
auto get_affinity(State& state, uint64_t tid) -> int64_t {
    for (;;) {
        alive(state);
        int64_t const RC = raw(Op::GET_AFFINITY, tid);
        if (RC != -EBUSY) return RC;
        (void)ker::multiproc::yield();
    }
}
void send_byte(State& state, int fd, uint8_t value) {
    for (;;) {
        alive(state);
        ssize_t const RC = write(fd, &value, 1);
        if (RC == 1) return;
        if (RC < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
            pause_us(1000);
            continue;
        }
        abort_live(PIPE, 0, RC);
    }
}
void receive_ack(State& state, int fd, uint8_t expected) {
    for (;;) {
        alive(state);
        uint8_t value{};
        ssize_t const RC = read(fd, &value, 1);
        if (RC == 1) {
            if (value != expected) abort_live(PIPE, 0, value);
            return;
        }
        if (RC < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
            pause_us(1000);
            continue;
        }
        abort_live(PIPE, 0, RC);
    }
}
auto query_domain(Checks& checks, uint64_t id, uint64_t expected) -> int64_t {
    GuardedQuery buffer{};
    int64_t const RC = raw(Op::QUERY_DOMAIN, id, ptr(&buffer.output));
    expect_live(checks, RC == 0 && buffer.before == GUARD && buffer.after == GUARD && buffer.output.mask == expected, ABI, id, RC);
    for (unsigned cpu = 0; cpu < buffer.output.loads.size(); ++cpu)
        expect_live(checks, (expected & (uint64_t{1} << cpu)) != 0 || buffer.output.loads[cpu] == 0, ABI, cpu);
    return RC;
}
auto supervisor() -> int {
    HostReport host{};
    State state{};
    host.started_us = now_us();
    if (host.started_us == UINT64_MAX || host.started_us > UINT64_MAX - RUN_US) abort_live(CLOCK);
    state.start = host.started_us;
    state.deadline = state.start + RUN_US;
    host.pid = ker::process::getpid();
    host.tid = ker::multiproc::currentThreadId();
    host.nonce = (host.started_us << 13) ^ (host.pid << 7) ^ host.tid;
    if (host.nonce == 0) host.nonce = 1;
    state.nonce = host.nonce;
    host.uid = ker::process::getuid();
    host.euid = ker::process::geteuid();
    host.gid = ker::process::getgid();
    host.cpu_count = ker::multiproc::nativeThreadCount();
    host.local_rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    expect_live(host.checks,
                host.uid == 100 && host.euid == 100 && host.gid == 100 && host.cpu_count == 8 && host.local_rc == 0 && host.pid != 0 &&
                    host.tid != 0,
                IDENTITY);
    int data[2]{-1, -1}, ack[2]{-1, -1};
    expect_live(host.checks, pipe(data) == 0 && pipe(ack) == 0, PIPE);
    expect_live(host.checks, fcntl(data[1], F_SETFL, O_NONBLOCK) == 0 && fcntl(ack[0], F_SETFL, O_NONBLOCK) == 0, PIPE);
    state.data_read = data[0];
    state.ack_write = ack[1];
    pthread_attr_t attributes{};
    expect_live(host.checks, pthread_attr_init(&attributes) == 0, STARTUP);
    expect_live(host.checks, pthread_attr_setstacksize(&attributes, 256 * 1024) == 0 && pthread_attr_setguardsize(&attributes, PAGE) == 0,
                STARTUP);
    std::array<Argument, WORKERS> arguments{};
    std::array<pthread_t, WORKERS> threads{};
    for (unsigned index = 0; index < WORKERS; ++index) {
        arguments[index] = {&state, index};
        int const RC = pthread_create(&threads[index], &attributes, worker, &arguments[index]);
        expect_live(host.checks, RC == 0, STARTUP, index, RC);
        ++host.workers;
    }
    while (state.ready.load(std::memory_order_acquire) != WORKERS) {
        alive(state);
        pause_us(1000);
    }
    uint64_t const SETUP_OWNER = state.published[0].cpu.load(std::memory_order_acquire);
    expect_live(host.checks, SETUP_OWNER < 8, CPU);
    for (unsigned index = 0; index < WORKERS; ++index) {
        uint64_t const CPU = (SETUP_OWNER + index) % 8;
        uint64_t const TID = state.published[index].tid.load(std::memory_order_acquire);
        state.published[index].requested_cpu.store(CPU, std::memory_order_release);
        for (;;) {
            alive(state);
            ++host.setup_calls;
            int64_t const RC = raw(Op::SET_AFFINITY, TID, uint64_t{1} << CPU);
            if (RC == -EBUSY) {
                ++host.setup_busy;
                (void)ker::multiproc::yield();
                continue;
            }
            expect_live(host.checks, RC == 0, PIN, index, RC);
            break;
        }
        while (!state.published[index].confirmed.load(std::memory_order_acquire)) {
            alive(state);
            (void)ker::multiproc::yield();
        }
    }
    state.setup_done.store(true, std::memory_order_release);
    std::array<unsigned, 8> cpu_counts{};
    for (unsigned index = 0; index < WORKERS; ++index) {
        uint64_t const TID = state.published[index].tid.load(std::memory_order_acquire);
        uint64_t const CPU = state.published[index].cpu.load(std::memory_order_acquire);
        expect_live(host.checks, TID != 0 && TID != host.tid && CPU < 8, STARTUP, index);
        for (unsigned earlier = 0; earlier < index; ++earlier)
            expect_live(host.checks, TID != state.published[earlier].tid.load(std::memory_order_acquire), STARTUP, index);
        ++host.unique_tids;
        ++cpu_counts[CPU];
        host.initial_cpu_mask |= uint64_t{1} << CPU;
    }
    host.target_tid = state.published[0].tid.load(std::memory_order_acquire);
    host.initial_cpu = state.published[0].cpu.load(std::memory_order_acquire);
    state.target_tid = host.target_tid;
    expect_live(host.checks, host.initial_cpu_mask == ONLINE && cpu_counts[host.initial_cpu] >= 2, CPU, 0,
                static_cast<int64_t>(host.initial_cpu_mask), ONLINE);
    for (unsigned cpu = 0; cpu < 8; ++cpu) expect_live(host.checks, cpu_counts[cpu] == 2, CPU, cpu, cpu_counts[cpu], 2);
    diagnostic_stage("setup");
    host.destination_cpu = (host.initial_cpu + 1) % 8;
    host.domain_mask = uint64_t{1} << host.destination_cpu;
    host.subset_mask = host.domain_mask | (uint64_t{1} << host.initial_cpu);
    state.destination = host.destination_cpu;
    state.subset = host.subset_mask;
    constexpr uint64_t MISSING = UINT64_MAX;
    auto& selectors = host.selector_results;
    selectors[0] = raw(Op::GET_AFFINITY, MISSING);
    selectors[1] = raw(Op::SET_AFFINITY, MISSING, 0);
    selectors[2] = raw(Op::SET_AFFINITY, host.target_tid, 0);
    selectors[3] = raw(Op::SET_AFFINITY, host.target_tid, uint64_t{1} << 63);
    selectors[4] = raw(Op::SET_DOMAIN, MISSING, 63, 1);
    selectors[5] = raw(Op::SET_DOMAIN, MISSING, 0, 1);
    selectors[6] = raw(Op::SET_DOMAIN, MISSING, (uint64_t{1} << 32) | 63, 1);
    selectors[7] = raw(Op::SET_DOMAIN, MISSING, uint64_t{1} << 32, 1);
    selectors[8] = raw(Op::QUERY_DOMAIN, 63, 1);
    Request invalid{};
    selectors[9] = raw(Op::CREATE_DOMAIN, ptr(invalid.data()));
    expect_live(host.checks, invalid == Request{}, ABI, 9);
    selectors[10] = query_domain(host.checks, 0, ONLINE);
    selectors[11] = query_domain(host.checks, 1, ONLINE);
    constexpr std::array<int64_t, 12> EXPECTED{-ESRCH, -ESRCH, -EINVAL, -EINVAL, -EINVAL, -ESRCH, -EINVAL, -ESRCH, -EINVAL, -EINVAL, 0, 0};
    for (unsigned index = 0; index < selectors.size(); ++index)
        expect_live(host.checks, selectors[index] == EXPECTED[index], ABI, index, selectors[index], static_cast<uint64_t>(EXPECTED[index]));
    state.park_requested.store(true, std::memory_order_release);
    while (state.park_start.load(std::memory_order_acquire) == 0) {
        alive(state);
        pause_us(1000);
    }
    host.park_start_us = state.park_start.load(std::memory_order_acquire);
    pause_us(10'000);
    host.park_probe_us = now_us();
    expect_live(host.checks, host.park_probe_us >= host.park_start_us && host.park_probe_us - host.park_start_us < PARK_US / 2, WAITING);
    expect_live(host.checks, host.park_probe_us - host.park_start_us >= 10'000, WAITING);
    host.park_before_get = raw(Op::GET_AFFINITY, host.target_tid);
    host.park_exclude_rc = raw(Op::SET_AFFINITY, host.target_tid, host.domain_mask);
    host.park_after_get = raw(Op::GET_AFFINITY, host.target_tid);
    expect_live(host.checks,
                host.park_exclude_rc == -EBUSY && host.park_before_get == static_cast<int64_t>(uint64_t{1} << host.initial_cpu) &&
                    host.park_after_get == host.park_before_get,
                WAITING, 0, host.park_exclude_rc);
    diagnostic_stage("park");
    Request request{};
    constexpr char NAME[] = "task39-hard-singleton";
    std::memcpy(request.data(), NAME, sizeof(NAME));
    std::memcpy(request.data() + 32, &host.domain_mask, sizeof(host.domain_mask));
    request[41] = 1;
    Request const ORIGINAL = request;
    int64_t const CREATED = raw(Op::CREATE_DOMAIN, ptr(request.data()));
    expect_live(host.checks, CREATED == 2 && request == ORIGINAL, DOMAIN, 0, CREATED, 2);
    host.domain_id = static_cast<uint64_t>(CREATED);
    diagnostic_stage("domain-return");
    (void)query_domain(host.checks, host.domain_id, host.domain_mask);
    diagnostic_stage("query-return");
    state.phase.store(1, std::memory_order_release);
    while (!state.phase1_active.load(std::memory_order_acquire)) {
        alive(state);
        (void)ker::multiproc::yield();
    }
    diagnostic_stage("phase1ack");
    for (;;) {
        alive(state);
        ++host.hard_calls;
        host.hard_rc = raw(Op::SET_DOMAIN, host.target_tid, (uint64_t{1} << 32) | host.domain_id, 1);
        if (host.hard_rc == -EBUSY) {
            ++host.hard_busy;
            (void)ker::multiproc::yield();
            continue;
        }
        expect_live(host.checks, host.hard_rc == 0, DOMAIN, 0, host.hard_rc);
        break;
    }
    host.hard_get = get_affinity(state, host.target_tid);
    expect_live(host.checks, host.hard_get == static_cast<int64_t>(host.domain_mask), DOMAIN, 0, host.hard_get, host.domain_mask);
    diagnostic_stage("hard-admission");
    state.phase.store(2, std::memory_order_release);
    while (state.hard_seen.load(std::memory_order_acquire) < HARD_OBSERVATIONS) {
        alive(state);
        (void)ker::multiproc::yield();
    }
    diagnostic_stage("hard-cpu64");
    for (;;) {
        alive(state);
        ++host.full_calls;
        host.full_rc = raw(Op::SET_AFFINITY, host.target_tid, ONLINE);
        if (host.full_rc == -EBUSY) {
            ++host.full_busy;
            (void)ker::multiproc::yield();
            continue;
        }
        expect_live(host.checks, host.full_rc == 0, AFFINITY, 0, host.full_rc);
        break;
    }
    host.full_get = get_affinity(state, host.target_tid);
    expect_live(host.checks, host.full_get == static_cast<int64_t>(ONLINE), AFFINITY, 0, host.full_get, ONLINE);
    diagnostic_stage("full-affinity");
    state.phase.store(3, std::memory_order_release);
    std::array<Argument, CONTROLLERS> control_arguments{};
    std::array<pthread_t, CONTROLLERS> control_threads{};
    for (unsigned index = 0; index < CONTROLLERS; ++index) {
        control_arguments[index] = {&state, index};
        int const RC = pthread_create(&control_threads[index], &attributes, controller, &control_arguments[index]);
        expect_live(host.checks, RC == 0, STARTUP, WORKERS + index, RC);
        ++host.controllers;
    }
    expect_live(host.checks, pthread_attr_destroy(&attributes) == 0, STARTUP);
    while (state.controller_ready.load(std::memory_order_acquire) != CONTROLLERS) {
        alive(state);
        (void)ker::multiproc::yield();
    }
    diagnostic_stage("controller-ready");
    state.controller_go.store(true, std::memory_order_release);
    for (unsigned index = 0; index < EVENTS; ++index) {
        uint8_t const BYTE = static_cast<uint8_t>(index % 254 + 1);
        send_byte(state, data[1], BYTE);
        receive_ack(state, ack[0], BYTE);
        ++host.pipe_roundtrips;
        state.event_progress.store(index + 1, std::memory_order_release);
        // Leave an empty pipe interval for the target's next blocking read.
        pause_us(1000);
    }
    for (unsigned index = 0; index < CONTROLLERS; ++index) {
        void* result = reinterpret_cast<void*>(1);
        int const RC = pthread_join(control_threads[index], &result);
        expect_live(host.checks, RC == 0 && result == nullptr, JOIN, WORKERS + index, RC);
    }
    diagnostic_stage("pipe-calls-complete");
    state.stop.store(true, std::memory_order_release);
    send_byte(state, data[1], 0);
    for (unsigned index = 0; index < WORKERS; ++index) {
        void* result = reinterpret_cast<void*>(1);
        int const RC = pthread_join(threads[index], &result);
        expect_live(host.checks, RC == 0 && result == nullptr, JOIN, index, RC);
    }
    diagnostic_stage("joins");
    // All report writers have joined; no mutable row is read before this point.
    for (unsigned index = 0; index < WORKERS; ++index) {
        auto const& report = state.workers[index];
        host.checks.expect(report.checks.failures == 0 && report.tid == state.published[index].tid.load(std::memory_order_acquire) &&
                               report.nonce == host.nonce && report.pid == host.pid && report.local_selected == 1,
                           STARTUP, index);
        if (index == 0)
            host.checks.expect(report.hard_observations >= HARD_OBSERVATIONS && report.hard_wrong == 0 && report.pipe_reads == EVENTS &&
                                   report.pipe_acks == EVENTS && (report.phases & 0xF) == 0xF,
                               HARD_CPU, index);
        else
            host.checks.expect(report.yields != 0, CPU, index);
    }
    for (unsigned index = 0; index < CONTROLLERS; ++index) {
        auto const& report = state.controls[index];
        host.control_successes += report.successes;
        host.control_busy += report.busy;
        host.control_overlap += report.overlap;
        host.wake_changes += report.wake_changes;
        host.checks.expect(report.checks.failures == 0 && report.calls == CALLS && report.completed == 1 && report.successes != 0 &&
                               report.overlap != 0 && report.wake_changes != 0 && report.nonce == host.nonce && report.pid == host.pid &&
                               report.tid != host.tid,
                           CONTROL, index);
        for (unsigned worker_index = 0; worker_index < WORKERS; ++worker_index)
            host.checks.expect(report.tid != state.workers[worker_index].tid, STARTUP, index);
        for (unsigned earlier = 0; earlier < index; ++earlier)
            host.checks.expect(report.tid != state.controls[earlier].tid, STARTUP, index);
    }
    for (int const FD : {data[0], data[1], ack[0], ack[1]}) {
        host.checks.expect(close(FD) == 0, PIPE);
        ++host.closed_fds;
    }
    host.ended_us = now_us();
    host.checks.expect(host.ended_us != UINT64_MAX && host.ended_us >= host.started_us && host.ended_us < state.deadline, CLOCK);
    char path[128]{};
    bool saved = true;
    for (unsigned index = 0; index < WORKERS; ++index)
        saved = (report_path(path, sizeof(path), "worker", index) && persist(path, &state.workers[index], sizeof(WorkerReport))) && saved;
    for (unsigned index = 0; index < CONTROLLERS; ++index)
        saved =
            (report_path(path, sizeof(path), "control", index) && persist(path, &state.controls[index], sizeof(ControlReport))) && saved;
    saved = (report_path(path, sizeof(path), "host", 0) && persist(path, &host, sizeof(host))) && saved;
    bool const PASSED = host.checks.failures == 0 && saved;
    char line[512]{};
    int const COUNT = std::snprintf(
        line, sizeof(line),
        "%s task-placement-runtime nonce=%llu pid=%llu tid=%llu workers=16 controllers=3 cpus=0x%llx domain=%llu destination=%llu "
        "hard-calls=%llu hard-busy=%llu hard-observations=%llu roundtrips=%llu calls=3072 successes=%llu busy=%llu overlap=%llu "
        "wake-changes=%llu failures=%llu first=%llu\n",
        PASSED ? "PASS" : "FAIL", static_cast<unsigned long long>(host.nonce), static_cast<unsigned long long>(host.pid),
        static_cast<unsigned long long>(host.target_tid), static_cast<unsigned long long>(host.initial_cpu_mask),
        static_cast<unsigned long long>(host.domain_id), static_cast<unsigned long long>(host.destination_cpu),
        static_cast<unsigned long long>(host.hard_calls), static_cast<unsigned long long>(host.hard_busy),
        static_cast<unsigned long long>(state.workers[0].hard_observations), static_cast<unsigned long long>(host.pipe_roundtrips),
        static_cast<unsigned long long>(host.control_successes), static_cast<unsigned long long>(host.control_busy),
        static_cast<unsigned long long>(host.control_overlap), static_cast<unsigned long long>(host.wake_changes),
        static_cast<unsigned long long>(host.checks.failures + (saved ? 0 : 1)), static_cast<unsigned long long>(host.checks.first_code));
    bool const LINE_VALID = COUNT > 0 && static_cast<size_t>(COUNT) < sizeof(line);
    std::snprintf(path, sizeof(path), "%s/status", RESULTS);
    bool const STATUS_SAVED = LINE_VALID && persist(path, line, static_cast<size_t>(COUNT));
    if (STATUS_SAVED)
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(COUNT));
    else {
        constexpr char FAIL[] = "FAIL task-placement-runtime status-write failures=1\n";
        (void)write(STDOUT_FILENO, FAIL, sizeof(FAIL) - 1);
    }
    return PASSED && STATUS_SAVED ? 0 : 56;
}

// Independent read-only service observes the probe's reap, never its own exit.
auto init_exit_witness() -> int {
    namespace init = ker::abi::init_control;
    constexpr char SERVICE[] = "task-placement-probe";
    constexpr uint64_t POLL_US = 30'000'000;
    struct GuardedSnapshot {
        uint64_t before = GUARD;
        init::StatusSnapshot output{};
        uint64_t after = GUARD;
    };
    static_assert(sizeof(init::StatusSnapshot) == 4160 && init::ABI_VERSION == 1);
    static_assert(offsetof(GuardedSnapshot, output) == 8 && offsetof(GuardedSnapshot, after) == 4168);
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    uint64_t const UID = ker::process::getuid(), EUID = ker::process::geteuid(), GID = ker::process::getgid();
    uint64_t const START = now_us();
    bool const CLOCK_VALID = START != UINT64_MAX && START <= UINT64_MAX - POLL_US;
    bool observed = false, history_exit = false;
    int64_t read_error = !CLOCK_VALID ? -EIO : (UID != 100 || EUID != 100 || GID != 100 ? -EPERM : 0);
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
            uint64_t const AFTER = now_us();
            if (AFTER == UINT64_MAX || AFTER < NOW) {
                read_error = -EIO;
                break;
            }
            if (AFTER >= DEADLINE) {
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
                if (supported)
                    for (uint32_t index = 0; index < snapshot.service_count; ++index) {
                        auto const& status = snapshot.services[index];
                        supported = supported && std::memchr(status.service, '\0', sizeof(status.service)) != nullptr &&
                                    status.history_count <= init::MAX_TRANSITION_HISTORY;
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
                if (matches == 1)
                    for (uint32_t index = 0; index < selected.history_count; ++index) {
                        auto const& transition = selected.history[index];
                        if (transition.reason == init::TransitionReason::EXIT && transition.to_state == init::ServiceState::EXITED &&
                            transition.timestamp_mono_ns != 0 && transition.timestamp_mono_ns <= snapshot.published_mono_ns)
                            history_exit = true;
                    }
                observed = matches == 1 && selected.generation == 1 && selected.restart_count == 0 &&
                           selected.state == init::ServiceState::EXITED && selected.readiness == init::ReadinessState::READY &&
                           selected.pid == -1 && selected.pgid == -1 && selected.last_wait_status == 0 && selected.last_error == 0 &&
                           history_exit;
                if (observed) break;
            }
            pause_us(1000);
        }
    }
    char path[128]{};
    std::snprintf(path, sizeof(path), "%s/init-exit-snapshot.bin", RESULTS);
    bool const SNAPSHOT_SAVED = persist(path, &snapshot, sizeof(snapshot));
    bool const PASSED = observed && read_error == 0 && SNAPSHOT_SAVED;
    char line[512]{};
    int const COUNT = std::snprintf(
        line, sizeof(line),
        "%s task-placement-init-exit service=task-placement-probe generation=%llu restarts=%llu sequence=%llu published-ns=%llu "
        "state=%u readiness=%u wait=%d error=%d history-exit=%u snapshot-bytes=%u abi=%u uid=%llu read-error=%lld failures=%u\n",
        PASSED ? "PASS" : "FAIL", static_cast<unsigned long long>(selected.generation),
        static_cast<unsigned long long>(selected.restart_count), static_cast<unsigned long long>(snapshot.sequence),
        static_cast<unsigned long long>(snapshot.published_mono_ns), static_cast<unsigned>(selected.state),
        static_cast<unsigned>(selected.readiness), selected.last_wait_status, selected.last_error, history_exit ? 1U : 0U, snapshot.size,
        snapshot.version, static_cast<unsigned long long>(UID), static_cast<long long>(read_error), PASSED ? 0U : 1U);
    bool const LINE_VALID = COUNT > 0 && static_cast<size_t>(COUNT) < sizeof(line);
    std::snprintf(path, sizeof(path), "%s/init-exit-status", RESULTS);
    bool const STATUS_SAVED = LINE_VALID && persist(path, line, static_cast<size_t>(COUNT));
    if (STATUS_SAVED)
        (void)write(STDOUT_FILENO, line, static_cast<size_t>(COUNT));
    else {
        constexpr char FAIL[] = "FAIL task-placement-init-exit receipt-write failures=1\n";
        (void)write(STDOUT_FILENO, FAIL, sizeof(FAIL) - 1);
    }
    return PASSED && STATUS_SAVED ? 0 : 57;
}
}  // namespace

namespace weighted_fairness {
// BEGIN WEIGHTED_FAIRNESS_PURE: exercised verbatim by the public Host cases.
constexpr uint64_t TRIALS = 3, TASKS = 4, WARM_US = 1'000'000, MEASURE_US = 5'000'000;
constexpr uint64_t LEAD_US = 50'000, BRACKET_US = 50'000, WHOLE_US = 60'000'000;
constexpr uint64_t MAX_CHUNKS = uint64_t{1} << 24, STEPS = 4096;
struct Window {
    uint64_t warm{}, start{}, end{};
};
struct Endpoint {
    uint64_t before_us{}, cpu_us{}, after_us{};
    int64_t yield_rc{};
};
struct Bounds {
    uint64_t delta{}, low{}, high{};
};
struct PairMath {
    uint64_t weight_a{}, weight_b{}, low_sum{}, delta_sum{}, lower_denominator{}, upper_denominator{};
    uint64_t lower_left{}, lower_right{}, upper_left{}, upper_right{}, passed{};
};
auto add(uint64_t a, uint64_t b, uint64_t& out) -> bool {
    out = 0;
    if (a > UINT64_MAX - b) return false;
    out = a + b;
    return true;
}
auto multiply(uint64_t a, uint64_t b, uint64_t& out) -> bool {
    out = 0;
    if (b != 0 && a > UINT64_MAX / b) return false;
    out = a * b;
    return true;
}
auto clock_us(int64_t seconds, int64_t nanos, bool cpu, uint64_t& out) -> bool {
    out = 0;
    if (seconds < 0 || nanos < 0 || nanos >= 1'000'000'000 || (cpu && nanos % 1000 != 0)) return false;
    uint64_t base = 0;
    return multiply(static_cast<uint64_t>(seconds), 1'000'000, base) && add(base, static_cast<uint64_t>(nanos) / 1000, out);
}
auto make_window(uint64_t now, uint64_t deadline, Window& out) -> bool {
    out = {};
    Window next{};
    uint64_t latest = 0;
    if (!add(now, LEAD_US, next.warm) || !add(next.warm, WARM_US, next.start) || !add(next.start, MEASURE_US, next.end) ||
        !add(next.end, BRACKET_US, latest) || latest >= deadline)
        return false;
    out = next;
    return true;
}
auto runtime_bounds(const Window& window, const Endpoint& start, const Endpoint& end, Bounds& out) -> bool {
    out = {};
    if (window.start < window.warm || window.start - window.warm != WARM_US || window.end < window.start ||
        window.end - window.start != MEASURE_US || start.yield_rc != 0 || end.yield_rc != 0 || start.before_us < window.start ||
        start.after_us < start.before_us || start.after_us - window.start > BRACKET_US || end.before_us < window.end ||
        end.after_us < end.before_us || end.after_us - window.end > BRACKET_US || end.before_us < start.after_us ||
        end.cpu_us < start.cpu_us || start.cpu_us > WHOLE_US || end.cpu_us > WHOLE_US)
        return false;
    uint64_t extra = 0, high = 0;
    uint64_t const DELTA = end.cpu_us - start.cpu_us;
    if (!add(end.after_us - window.end, 2, extra) || !add(DELTA, start.after_us - window.start, high) || !add(high, 2, high)) return false;
    out = {.delta = DELTA, .low = DELTA > extra ? DELTA - extra : 0, .high = high};
    return true;
}
auto judge_pair(uint64_t weight_a, uint64_t weight_b, const Bounds& a, const Bounds& b, PairMath& out) -> bool {
    out = {.weight_a = weight_a, .weight_b = weight_b};
    if (!((weight_a == 1024 && weight_b == 1024) || (weight_a == 1024 && weight_b == 335) || (weight_a == 335 && weight_b == 1024)) ||
        a.delta == 0 || b.delta == 0 || a.low > a.delta || a.delta > a.high || b.low > b.delta || b.delta > b.high ||
        a.high > WHOLE_US + BRACKET_US + 2 || b.high > WHOLE_US + BRACKET_US + 2 || !add(a.low, b.low, out.low_sum) ||
        !add(a.delta, b.delta, out.delta_sum) || out.low_sum < 4'500'000 || out.low_sum > MEASURE_US || out.delta_sum > 5'100'004 ||
        !add(a.low, b.high, out.lower_denominator) || !add(a.high, b.low, out.upper_denominator) || out.lower_denominator == 0 ||
        out.upper_denominator == 0)
        return false;
    uint64_t weights = 0, denominator = 0, center = 0, upper = 0;
    if (!add(weight_a, weight_b, weights) || !multiply(20, weights, denominator) || !multiply(20, weight_a, center) || center < weights ||
        !add(center, weights, upper) || !multiply(a.low, denominator, out.lower_left) ||
        !multiply(out.lower_denominator, center - weights, out.lower_right) || !multiply(a.high, denominator, out.upper_left) ||
        !multiply(out.upper_denominator, upper, out.upper_right))
        return false;
    out.passed = out.lower_left >= out.lower_right && out.upper_left <= out.upper_right ? 1 : 0;
    return out.passed == 1;
}
auto expected_nice(uint64_t trial, unsigned index) -> uint64_t {
    if (trial == 0) return 0;
    return (index % 2) == ((trial == 1 ? 1U : 0U) ^ (index / 2)) ? 5 : 0;
}
// END WEIGHTED_FAIRNESS_PURE

constexpr uint64_t REPORT_MAGIC = 0x3438395746495253ULL, REPORT_VERSION = 1;
struct TaskReport {
    Checks checks{};
    uint64_t nonce{}, pid{}, tid{}, trial{}, index{}, cpu{}, requested_nice{};
    int64_t initial_nice{}, observed_nice{}, final_nice{}, local_rc{}, first_affinity{}, final_affinity{};
    uint64_t pin_calls{}, pin_busy{}, setup_yields{}, initial_cpu{}, warm_enter_us{}, warm_chunks{}, lead_chunks{}, measured_chunks{};
    uint64_t checksum{}, cpu_seen{}, samples{}, same_changes{}, cross_changes{}, first_peer{}, last_peer{}, first_cross{}, last_cross{};
    Endpoint start{}, end{};
    Bounds runtime{};
    uint64_t total_chunks{}, finished_us{};
};
struct PairReport {
    Checks checks{};
    uint64_t trial{}, cpu{}, first_index{}, second_index{};
    PairMath math{};
};
struct TrialReport {
    uint64_t trial{}, warm_start_us{}, measure_start_us{}, measure_end_us{}, joined_us{}, configured{};
    std::array<TaskReport, TASKS> tasks{};
    std::array<PairReport, 2> pairs{};
};
struct FairnessReport {
    Checks checks{};
    uint64_t magic = REPORT_MAGIC, version = REPORT_VERSION, bytes = sizeof(FairnessReport);
    uint64_t nonce{}, pid{}, tid{}, uid{}, euid{}, gid{}, cpu_count{}, started_us{}, deadline_us{}, ended_us{};
    uint64_t initial_mask{}, control_mask{}, control_nice{}, parent_set_calls{}, parent_set_busy{}, completed_trials{}, failures{};
    std::array<TrialReport, TRIALS> trials{};
};
static_assert(sizeof(Endpoint) == 32 && sizeof(Bounds) == 24 && sizeof(PairMath) == 88);
static_assert(sizeof(TaskReport) == 392 && sizeof(PairReport) == 168 && sizeof(TrialReport) == 1952);
static_assert(sizeof(FairnessReport) == 6064 && sizeof(FairnessReport) <= 8192);
struct alignas(64) Slot {
    std::atomic<uint64_t> tid{0}, requested{UINT64_MAX}, progress{0};
    std::atomic<bool> confirmed{false};
};
static_assert(sizeof(Slot) == 64);
struct Run {
    uint64_t nonce{}, pid{}, main_tid{}, started{}, deadline{}, trial{};
    Window window{};
    TrialReport* report{};
    std::array<Slot, TASKS> slots{};
    std::atomic<unsigned> ready{0}, configured{0};
    std::atomic<bool> control_set{false}, control_pinned{false}, go{false};
};
struct WorkArgument {
    Run* run{};
    unsigned index{};
};
auto time_us(clockid_t clock, bool cpu) -> uint64_t {
    timespec value{};
    uint64_t result = 0;
    if (clock_gettime(clock, &value) != 0 || !clock_us(value.tv_sec, value.tv_nsec, cpu, result)) abort_live(CLOCK);
    return result;
}
auto sample(const Run& run) -> uint64_t {
    uint64_t const NOW = time_us(CLOCK_MONOTONIC, false);
    if (NOW < run.started) abort_live(CLOCK);
    if (NOW >= run.deadline) abort_live(DEADLINE);
    return NOW;
}
void count(uint64_t& value, uint64_t amount = 1) {
    if (!add(value, amount, value)) abort_live(CONTROL);
}
void setup_yield(Run& run, TaskReport& report) {
    static_cast<void>(sample(run));
    count(report.setup_yields);
    int64_t const RC = static_cast<int64_t>(ker::multiproc::yield());
    if (RC != 0) abort_live(CONTROL, report.index, RC);
}
auto affinity(Run& run, uint64_t tid) -> int64_t {
    for (;;) {
        static_cast<void>(sample(run));
        int64_t const RC = raw(Op::GET_AFFINITY, tid);
        if (RC != -EBUSY) return RC;
        if (static_cast<int64_t>(ker::multiproc::yield()) != 0) abort_live(CONTROL);
    }
}
void set_affinity(Run& run, uint64_t tid, uint64_t mask, uint64_t& calls, uint64_t& busy) {
    for (;;) {
        static_cast<void>(sample(run));
        count(calls);
        int64_t const RC = raw(Op::SET_AFFINITY, tid, mask);
        if (RC == 0) return;
        if (RC != -EBUSY) abort_live(PIN, tid, RC);
        count(busy);
        if (static_cast<int64_t>(ker::multiproc::yield()) != 0) abort_live(CONTROL);
    }
}
auto own_nice() -> int {
    errno = 0;
    int const NICE = getpriority(PRIO_PROCESS, 0);
    if (errno != 0) abort_live(CONTROL, 0, errno);
    return NICE;
}
// Unmeasured setup state. No field belongs to the durable weighted report ABI.
struct BootstrapState {
    Run timing{};
    uint64_t origin_cpu{}, child_tid{}, child_cpu{}, move_calls{}, move_busy{};
    std::atomic<bool> moved{false};
};

auto pin_controller_current(Run& run, FairnessReport& report) -> uint64_t {
    for (;;) {
        static_cast<void>(sample(run));
        uint64_t const CURRENT = ker::multiproc::getcurrent_cpu();
        expect_live(report.checks, CURRENT < 8, CPU, report.tid, CURRENT);
        count(report.parent_set_calls);
        int64_t const RC = raw(Op::SET_AFFINITY, report.tid, uint64_t{1} << CURRENT);
        if (RC == -EBUSY) {
            count(report.parent_set_busy);
            if (static_cast<int64_t>(ker::multiproc::yield()) != 0) abort_live(CONTROL);
            continue;  // An unrestricted caller may have moved; sample its CPU again.
        }
        expect_live(report.checks,
                    RC == 0 && ker::multiproc::getcurrent_cpu() == CURRENT &&
                        affinity(run, report.tid) == static_cast<int64_t>(uint64_t{1} << CURRENT),
                    PIN, report.tid, RC);
        return CURRENT;
    }
}

auto bootstrap_controller(void* opaque) -> void* {
    auto& state = *static_cast<BootstrapState*>(opaque);
    auto& run = state.timing;
    state.child_tid = ker::multiproc::currentThreadId();
    state.child_cpu = ker::multiproc::getcurrent_cpu();
    if (ker::process::getpid() != run.pid || state.child_tid == 0 || state.child_tid == run.main_tid ||
        state.child_cpu != state.origin_cpu || state.origin_cpu == 0 ||
        affinity(run, state.child_tid) != static_cast<int64_t>(uint64_t{1} << state.origin_cpu) ||
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) != 0 || own_nice() != 0)
        abort_live(STARTUP);
    set_affinity(run, run.main_tid, 1, state.move_calls, state.move_busy);
    if (affinity(run, run.main_tid) != 1 || ker::multiproc::getcurrent_cpu() != state.origin_cpu) abort_live(PIN);
    state.moved.store(true, std::memory_order_release);
    return nullptr;
}

void establish_controller(FairnessReport& report, pthread_attr_t& attributes) {
    BootstrapState state{};
    state.timing.pid = report.pid;
    state.timing.main_tid = report.tid;
    state.timing.started = report.started_us;
    state.timing.deadline = report.deadline_us;
    state.origin_cpu = pin_controller_current(state.timing, report);
    if (state.origin_cpu != 0) {
        pthread_t thread{};
        int const CREATE = pthread_create(&thread, &attributes, bootstrap_controller, &state);
        expect_live(report.checks, CREATE == 0, STARTUP, 0, CREATE);
        while (!state.moved.load(std::memory_order_acquire)) {
            static_cast<void>(sample(state.timing));
            if (static_cast<int64_t>(ker::multiproc::yield()) != 0) abort_live(CONTROL);
        }
        // Joining before the queued cross-move would park the parent on its old CPU.
        expect_live(report.checks, ker::multiproc::getcurrent_cpu() == 0 && affinity(state.timing, report.tid) == 1, PIN);
        void* result = reinterpret_cast<void*>(1);
        int const JOINED = pthread_join(thread, &result);
        expect_live(report.checks, JOINED == 0 && result == nullptr, JOIN, 0, JOINED);
        static_cast<void>(sample(state.timing));
        expect_live(report.checks,
                    state.child_tid != 0 && state.child_tid != report.tid && state.child_cpu == state.origin_cpu &&
                        state.move_calls >= state.move_busy && state.move_calls - state.move_busy == 1,
                    STARTUP);
    }
    expect_live(report.checks, ker::multiproc::getcurrent_cpu() == 0 && affinity(state.timing, report.tid) == 1, PIN);
    report.control_mask = 1;
}

void prepare_trial_control(Run& run, FairnessReport& report) {
    expect_live(report.checks, ker::multiproc::getcurrent_cpu() == 0 && affinity(run, report.tid) == 1, CPU);
    run.control_pinned.store(true, std::memory_order_release);
    run.control_set.store(true, std::memory_order_release);
}

void verify_inherited_contender(Run& run, TaskReport& report) {
    expect_live(report.checks, ker::multiproc::getcurrent_cpu() == 0 && affinity(run, report.tid) == 1, PIN, report.index);
}

void compute(TaskReport& report, uint64_t& checksum) {
    if (report.total_chunks >= MAX_CHUNKS) abort_live(CONTROL, report.index, report.total_chunks);
    ++report.total_chunks;
    for (uint64_t step = 0; step < STEPS; ++step) {
        checksum = (checksum ^ (checksum >> 17)) * 0x9E3779B185EBCA87ULL + 0xD1B54A32D192ED03ULL;
    }
}
void observe_cpu(TaskReport& report) {
    uint64_t const CPU = ker::multiproc::getcurrent_cpu();
    ++report.samples;
    if (CPU != report.cpu || CPU >= 8) abort_live(CPU == UINT64_MAX ? IDENTITY : HARD_CPU, report.index, CPU);
    report.cpu_seen |= uint64_t{1} << CPU;
}
auto endpoint(Run& run, TaskReport& report, uint64_t before) -> Endpoint {
    Endpoint result{.before_us = before};
    result.yield_rc = static_cast<int64_t>(ker::multiproc::yield());
    if (result.yield_rc != 0) abort_live(CONTROL, report.index, result.yield_rc);
    result.cpu_us = time_us(CLOCK_THREAD_CPUTIME_ID, true);
    result.after_us = sample(run);
    observe_cpu(report);
    return result;
}
auto contender(void* opaque) -> void* {
    auto const& argument = *static_cast<WorkArgument*>(opaque);
    auto& run = *argument.run;
    auto& report = run.report->tasks[argument.index];
    auto& slot = run.slots[argument.index];
    report.nonce = run.nonce;
    report.pid = ker::process::getpid();
    report.tid = ker::multiproc::currentThreadId();
    report.trial = run.trial;
    report.index = argument.index;
    report.cpu = 2 + argument.index / 2;
    report.requested_nice = expected_nice(run.trial, argument.index);
    report.local_rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    report.initial_nice = own_nice();
    expect_live(report.checks,
                report.pid == run.pid && report.tid != 0 && report.tid != run.main_tid && report.local_rc == 0 && report.initial_nice == 0,
                IDENTITY, argument.index);
    verify_inherited_contender(run, report);
    for (;;) {
        static_cast<void>(sample(run));
        uint64_t const CURRENT = ker::multiproc::getcurrent_cpu();
        expect_live(report.checks, CURRENT < 8, CPU, argument.index, CURRENT);
        count(report.pin_calls);
        int64_t const RC = raw(Op::SET_AFFINITY, report.tid, uint64_t{1} << CURRENT);
        if (RC == -EBUSY) {
            count(report.pin_busy);
            setup_yield(run, report);
            continue;
        }
        expect_live(report.checks, RC == 0 && ker::multiproc::getcurrent_cpu() == CURRENT, PIN, argument.index, RC);
        report.initial_cpu = CURRENT;
        break;
    }
    slot.tid.store(report.tid, std::memory_order_release);
    run.ready.fetch_add(1, std::memory_order_release);
    while (!slot.confirmed.load(std::memory_order_acquire)) {
        static_cast<void>(sample(run));
        uint64_t const REQUESTED = slot.requested.load(std::memory_order_acquire);
        if (REQUESTED == ker::multiproc::getcurrent_cpu()) {
            set_affinity(run, report.tid, uint64_t{1} << REQUESTED, report.pin_calls, report.pin_busy);
            report.first_affinity = affinity(run, report.tid);
            expect_live(report.checks,
                        ker::multiproc::getcurrent_cpu() == report.cpu && REQUESTED == report.cpu &&
                            report.first_affinity == static_cast<int64_t>(uint64_t{1} << report.cpu),
                        PIN, argument.index);
            int const RC = setpriority(PRIO_PROCESS, 0, static_cast<int>(report.requested_nice));
            expect_live(report.checks, RC == 0, CONTROL, argument.index, errno);
            report.observed_nice = own_nice();
            expect_live(report.checks, report.observed_nice == static_cast<int64_t>(report.requested_nice), CONTROL, argument.index,
                        report.observed_nice, report.requested_nice);
            // Prepare the actual clock/TLS/usercopy destinations before READY.
            static_cast<void>(time_us(CLOCK_THREAD_CPUTIME_ID, true));
            static_cast<void>(sample(run));
            run.configured.fetch_add(1, std::memory_order_release);
            slot.confirmed.store(true, std::memory_order_release);
            break;
        }
        setup_yield(run, report);
    }
    while (!run.go.load(std::memory_order_acquire)) setup_yield(run, report);
    uint64_t checksum = 0x4890000000000001ULL ^ report.index;
    uint64_t now = sample(run);
    while (now < run.window.warm) {
        compute(report, checksum);
        ++report.lead_chunks;
        uint64_t const NEXT = sample(run);
        if (NEXT < now) abort_live(CLOCK);
        now = NEXT;
    }
    report.warm_enter_us = now;
    report.checks.expect(now - run.window.warm <= BRACKET_US, STARTUP, argument.index, now - run.window.warm, BRACKET_US);
    while (now < run.window.start) {
        compute(report, checksum);
        ++report.warm_chunks;
        observe_cpu(report);
        uint64_t const NEXT = sample(run);
        if (NEXT < now) abort_live(CLOCK);
        now = NEXT;
    }
    report.start = endpoint(run, report, now);
    now = report.start.after_us;
    unsigned const PEER = argument.index ^ 1U, CROSS = argument.index ^ 2U;
    report.first_peer = run.slots[PEER].progress.load(std::memory_order_acquire);
    report.last_peer = report.first_peer;
    report.first_cross = run.slots[CROSS].progress.load(std::memory_order_acquire);
    report.last_cross = report.first_cross;
    while (now < run.window.end) {
        compute(report, checksum);
        ++report.measured_chunks;
        slot.progress.store(report.measured_chunks, std::memory_order_release);
        uint64_t const PEER_PROGRESS = run.slots[PEER].progress.load(std::memory_order_acquire);
        uint64_t const CROSS_PROGRESS = run.slots[CROSS].progress.load(std::memory_order_acquire);
        if (PEER_PROGRESS < report.last_peer || CROSS_PROGRESS < report.last_cross) abort_live(OVERLAP, argument.index);
        bool const SAME_CHANGED = PEER_PROGRESS > report.last_peer;
        bool const CROSS_CHANGED = CROSS_PROGRESS > report.last_cross;
        report.last_peer = PEER_PROGRESS;
        report.last_cross = CROSS_PROGRESS;
        observe_cpu(report);
        uint64_t const NEXT = sample(run);
        if (NEXT < now) abort_live(CLOCK);
        if (NEXT < run.window.end) {
            report.same_changes += SAME_CHANGED ? 1 : 0;
            report.cross_changes += CROSS_CHANGED ? 1 : 0;
        }
        now = NEXT;
    }
    report.end = endpoint(run, report, now);
    report.checksum = checksum;
    bool const BOUNDED = runtime_bounds(run.window, report.start, report.end, report.runtime);
    report.checks.expect(BOUNDED, CLOCK, argument.index);
    report.checks.expect(report.measured_chunks != 0 && report.warm_chunks != 0 && report.same_changes >= 2 && report.cross_changes >= 2,
                         OVERLAP, argument.index, report.same_changes, 2);
    report.final_affinity = raw(Op::GET_AFFINITY, report.tid);
    report.final_nice = own_nice();
    report.checks.expect(report.final_affinity == static_cast<int64_t>(uint64_t{1} << report.cpu) &&
                             report.cpu_seen == (uint64_t{1} << report.cpu) &&
                             report.final_nice == static_cast<int64_t>(report.requested_nice),
                         AFFINITY, argument.index);
    report.finished_us = sample(run);
    return nullptr;
}
auto supervisor() -> int {
    FairnessReport report{};
    report.started_us = time_us(CLOCK_MONOTONIC, false);
    if (!add(report.started_us, WHOLE_US, report.deadline_us)) abort_live(CLOCK);
    report.pid = ker::process::getpid();
    report.tid = ker::multiproc::currentThreadId();
    report.nonce = (report.started_us << 13) ^ (report.pid << 7) ^ report.tid;
    if (report.nonce == 0) report.nonce = 1;
    report.uid = ker::process::getuid();
    report.euid = ker::process::geteuid();
    report.gid = ker::process::getgid();
    report.cpu_count = ker::multiproc::nativeThreadCount();
    report.control_nice = static_cast<uint64_t>(own_nice());
    expect_live(report.checks,
                report.pid != 0 && report.tid != 0 && report.uid == 100 && report.euid == 100 && report.gid == 100 &&
                    report.cpu_count == 8 && report.control_nice == 0 &&
                    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) == 0,
                IDENTITY);
    Run initial{};
    initial.started = report.started_us;
    initial.deadline = report.deadline_us;
    report.initial_mask = static_cast<uint64_t>(affinity(initial, report.tid));
    expect_live(report.checks, report.initial_mask == ONLINE, AFFINITY, report.tid, report.initial_mask, ONLINE);
    struct stat directory{};
    expect_live(report.checks,
                lstat(RESULTS, &directory) == 0 && S_ISDIR(directory.st_mode) && (directory.st_mode & 07777) == 0700 &&
                    directory.st_uid == 100 && directory.st_gid == 100,
                REPORT_IO);
    pthread_attr_t attributes{};
    expect_live(report.checks,
                pthread_attr_init(&attributes) == 0 && pthread_attr_setstacksize(&attributes, 256 * 1024) == 0 &&
                    pthread_attr_setguardsize(&attributes, PAGE) == 0,
                STARTUP);
    establish_controller(report, attributes);
    for (unsigned trial = 0; trial < TRIALS; ++trial) {
        auto& row = report.trials[trial];
        row.trial = trial;
        Run run{};
        run.nonce = report.nonce;
        run.pid = report.pid;
        run.main_tid = report.tid;
        run.started = report.started_us;
        run.deadline = report.deadline_us;
        run.trial = trial;
        run.report = &row;
        prepare_trial_control(run, report);
        std::array<WorkArgument, TASKS> arguments{};
        std::array<pthread_t, TASKS> threads{};
        for (unsigned index = 0; index < TASKS; ++index) {
            arguments[index] = {&run, index};
            int const RC = pthread_create(&threads[index], &attributes, contender, &arguments[index]);
            expect_live(report.checks, RC == 0, STARTUP, index, RC);
        }
        while (run.ready.load(std::memory_order_acquire) != TASKS || !run.control_set.load(std::memory_order_acquire)) {
            static_cast<void>(sample(run));
            if (static_cast<int64_t>(ker::multiproc::yield()) != 0) abort_live(CONTROL);
        }
        expect_live(report.checks, ker::multiproc::getcurrent_cpu() == 0 && affinity(run, report.tid) == 1, CPU);
        run.control_pinned.store(true, std::memory_order_release);
        report.control_mask = 1;
        for (unsigned index = 0; index < TASKS; ++index) {
            uint64_t const TID = run.slots[index].tid.load(std::memory_order_acquire);
            uint64_t const CPU = 2 + index / 2;
            run.slots[index].requested.store(CPU, std::memory_order_release);
            set_affinity(run, TID, uint64_t{1} << CPU, report.parent_set_calls, report.parent_set_busy);
            while (!run.slots[index].confirmed.load(std::memory_order_acquire)) {
                static_cast<void>(sample(run));
                if (static_cast<int64_t>(ker::multiproc::yield()) != 0) abort_live(CONTROL);
            }
        }
        expect_live(report.checks, run.configured.load(std::memory_order_acquire) == TASKS, STARTUP);
        row.configured = TASKS;
        expect_live(report.checks, make_window(sample(run), report.deadline_us, run.window), DEADLINE);
        row.warm_start_us = run.window.warm;
        row.measure_start_us = run.window.start;
        row.measure_end_us = run.window.end;
        run.go.store(true, std::memory_order_release);
        for (unsigned index = 0; index < TASKS; ++index) {
            void* result = reinterpret_cast<void*>(1);
            int const RC = pthread_join(threads[index], &result);
            expect_live(report.checks, RC == 0 && result == nullptr, JOIN, index, RC);
            static_cast<void>(sample(run));
        }
        // All mutable worker reports are read only after all four joins.
        row.joined_us = sample(run);
        for (unsigned index = 0; index < TASKS; ++index) {
            auto const& task = row.tasks[index];
            report.checks.expect(task.checks.failures == 0 && task.tid != 0 && task.tid != report.tid && task.pid == report.pid &&
                                     task.trial == trial && task.index == index && task.nonce == report.nonce,
                                 IDENTITY, index);
            for (unsigned previous_trial = 0; previous_trial <= trial; ++previous_trial) {
                unsigned const LIMIT = previous_trial == trial ? index : TASKS;
                for (unsigned previous = 0; previous < LIMIT; ++previous)
                    report.checks.expect(task.tid != report.trials[previous_trial].tasks[previous].tid, IDENTITY, index);
            }
            count(report.failures, task.checks.failures);
        }
        for (unsigned pair = 0; pair < 2; ++pair) {
            auto& selected = row.pairs[pair];
            selected.trial = trial;
            selected.cpu = 2 + pair;
            selected.first_index = pair * 2;
            selected.second_index = pair * 2 + 1;
            auto const& a = row.tasks[selected.first_index];
            auto const& b = row.tasks[selected.second_index];
            uint64_t const WEIGHT_A = a.requested_nice == 0 ? 1024 : 335, WEIGHT_B = b.requested_nice == 0 ? 1024 : 335;
            selected.checks.expect(judge_pair(WEIGHT_A, WEIGHT_B, a.runtime, b.runtime, selected.math), CONTROL, pair);
            count(report.failures, selected.checks.failures);
        }
        ++report.completed_trials;
    }
    expect_live(report.checks, pthread_attr_destroy(&attributes) == 0, STARTUP);
    expect_live(report.checks, own_nice() == 0 && raw(Op::GET_AFFINITY, report.tid) == 1 && ker::multiproc::getcurrent_cpu() == 0, CONTROL);
    report.checks.expect(report.completed_trials == TRIALS, CONTROL);
    report.checks.expect(report.parent_set_calls >= report.parent_set_busy && report.parent_set_calls - report.parent_set_busy == 13,
                         CONTROL);
    count(report.failures, report.checks.failures);
    Run terminal{};
    terminal.started = report.started_us;
    terminal.deadline = report.deadline_us;
    report.ended_us = sample(terminal);
    struct stat current{};
    expect_live(report.checks,
                lstat(RESULTS, &current) == 0 && S_ISDIR(current.st_mode) && current.st_dev == directory.st_dev &&
                    current.st_ino == directory.st_ino && current.st_mode == directory.st_mode && current.st_uid == directory.st_uid &&
                    current.st_gid == directory.st_gid,
                REPORT_IO);
    if (!persist("/srv/task-placement-results/weighted-fairness.bin", &report, sizeof(report))) abort_live(REPORT_IO);
    static_cast<void>(sample(terminal));
    bool const PASSED = report.failures == 0;
    char line[512]{};
    int const COUNT =
        std::snprintf(line, sizeof(line),
                      "%s task-weighted-fairness nonce=%llu pid=%llu tid=%llu trials=3 tasks=12 pairs=6 cpus=2,3 control=0 warm-us=1000000 "
                      "measure-us=5000000 bracket-us=50000 band-points=5 bytes=%llu failures=%llu\n",
                      PASSED ? "PASS" : "FAIL", static_cast<unsigned long long>(report.nonce), static_cast<unsigned long long>(report.pid),
                      static_cast<unsigned long long>(report.tid), static_cast<unsigned long long>(sizeof(report)),
                      static_cast<unsigned long long>(report.failures));
    if (COUNT <= 0 || static_cast<size_t>(COUNT) >= sizeof(line) ||
        !persist("/srv/task-placement-results/weighted-fairness-status", line, static_cast<size_t>(COUNT)))
        abort_live(REPORT_IO);
    static_cast<void>(sample(terminal));
    ssize_t const WRITTEN = write(STDOUT_FILENO, line, static_cast<size_t>(COUNT));
    if (WRITTEN != COUNT) abort_live(REPORT_IO);
    static_cast<void>(sample(terminal));
    return PASSED ? 0 : 56;
}
}  // namespace weighted_fairness

auto main(int argc, char** argv) -> int {
    if (argc == 1) return supervisor();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return init_exit_witness();
    if (argc == 2 && std::strcmp(argv[1], "--weighted-fairness") == 0) return weighted_fairness::supervisor();
    return 58;
}
