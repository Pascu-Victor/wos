// Actual Normal-kernel public placement and pipe-wake qualification.
#include <fcntl.h>
#include <pthread.h>
#include <sys/init_control.h>
#include <sys/multiproc.h>
#include <sys/process.h>
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

auto main(int argc, char** argv) -> int {
    if (argc == 1) return supervisor();
    if (argc == 2 && std::strcmp(argv[1], "--init-exit-witness") == 0) return init_exit_witness();
    return 58;
}
