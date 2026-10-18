// Actual critical guest helpers, explicit finite ports only. The full guest
// entry/setup/final gates run over finite endpoints; actual phase workloads,
// fork/ptrace permissions, Task, PS mapper, AS and physical owner do not run.
#include <abi/callnums/process.h>
#include <abi/callnums/vmem.h>
#include <abi/numa.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>
/* @ACTUAL_CALLNUMS@ */
/* @ACTUAL_PTRACE_TYPES@ */
namespace abi = ker::abi::vmem::numa;
constexpr size_t PAGE = 4096;
constexpr uint64_t NS_PER_MS = 1000000;
/* @ACTUAL_HUGE_TYPES@ */
auto huge_release(HugeState&, HugeMapping&) -> bool;
auto close_fd(int&, OwnResources&) -> bool;
namespace fixture {
struct WaitRow {
    pid_t result;
    int status;
    int error;
};
std::vector<WaitRow> waits;
std::vector<HugeInterval> intervals;
uint64_t ticks{}, clock_step{NS_PER_MS}, transferred{256}, process_error{};
uint64_t vm_result{0x400000}, last_op{}, last_address{}, last_bytes{}, last_flags{};
size_t wait_cursor{};
unsigned checks{}, failures{}, unmaps{};
bool refuse_unmap{};
// Finite resource and environment endpoints for the actual full entry function.
enum class EntryMode { Success, CloseFailure, UnmapRetry, WorkloadFailure, RestoreFailure };
EntryMode entry_mode{EntryMode::Success};
bool entry_complete{}, entry_defaults{}, post_resource_complete{};
uint32_t post_maps{}, post_fds{}, current_policy_mode{};
uint64_t current_policy_nodes{};
unsigned entry_work_calls{}, restore_policy_calls{}, restore_affinity_calls{};
bool refuse_close{};
// Finite already-forked parent context for the actual parent branch below.
bool huge_parent_mode{}, fail_final_ack{}, expire_before_wait{};
unsigned ack_writes{}, parent_closes{}, parent_kills{}, sleeps{}, remaining_writes{};
std::vector<std::string> huge_failure_records;
std::vector<int> wait_flags;
auto close(int fd) -> int {
    if (huge_parent_mode) {
        ++parent_closes;
        if (fd >= 101 && fd <= 104) return 0;
    }
    if (fd != 57 || refuse_close) {
        errno = EIO;
        return -1;
    }
    return 0;
}
auto getpid() -> pid_t { return 77; }
auto currentThreadId() -> uint64_t { return 77; }
auto getThreadAffinityMask(uint64_t) -> int64_t { return 1; }
auto setThreadAffinityMask(uint64_t tid, uint64_t mask) -> int64_t {
    ++restore_affinity_calls;
    return tid == 77 && mask == 1 && entry_mode != EntryMode::RestoreFailure ? 0 : -EIO;
}
auto getrlimit(int resource, rlimit* out) -> int {
    if (resource != RLIMIT_AS) {
        errno = EINVAL;
        return -1;
    }
    *out = {4096, 8192};
    return 0;
}
auto policy_read(QueryBuffers&, abi::SelfPolicy& out) -> int64_t {
    out = {};
    out.mode = current_policy_mode;
    out.nodes = current_policy_nodes;
    return 0;
}
auto policy_set(uint32_t mode, uint64_t nodes) -> int64_t {
    ++restore_policy_calls;
    current_policy_mode = mode;
    current_policy_nodes = nodes;
    return 0;
}
auto policy_matches(QueryBuffers&, uint32_t mode, uint64_t nodes) -> bool {
    return current_policy_mode == mode && current_policy_nodes == nodes;
}
auto policy_topology(QueryBuffers&, Topology& topo, uint32_t domains) -> int64_t {
    topo = {};
    topo.selected_count = 1;
    topo.selected[0] = {0, 0};
    return domains == 1 ? 0 : -EINVAL;
}
auto pin_current_cpu(const Topology&, Selection& selected, uint64_t) -> int64_t {
    selected = {0, 0};
    return 0;
}
auto huge_aliases(HugeState& state) -> bool;
auto huge_mutations(HugeState&) -> bool { return true; }
auto huge_usercopy(HugeState&) -> bool { return true; }
auto huge_bench(HugeState&) -> bool { return false; }  // This entry regression selects correctness only.
auto huge_child_case(HugeState& state, int kind) -> bool {
    if (kind == 3) {
        state.remaining = 0;
        return entry_mode != EntryMode::WorkloadFailure;
    }
    return true;
}
void check(bool condition, const char* name) {
    ++checks;
    if (!condition) {
        ++failures;
        std::printf("FAIL: %s\n", name);
    }
}
void reset() {
    waits.clear();
    intervals.clear();
    ticks = 0;
    clock_step = NS_PER_MS;
    transferred = 256;
    process_error = 0;
    wait_cursor = 0;
    unmaps = 0;
    refuse_unmap = false;
    vm_result = 0x400000;
    entry_mode = EntryMode::Success;
    entry_complete = false;
    entry_defaults = false;
    post_resource_complete = false;
    post_maps = post_fds = 0;
    current_policy_mode = 0;
    current_policy_nodes = 0;
    entry_work_calls = restore_policy_calls = restore_affinity_calls = 0;
    refuse_close = false;
    huge_parent_mode = fail_final_ack = expire_before_wait = false;
    ack_writes = parent_closes = parent_kills = sleeps = remaining_writes = 0;
    huge_failure_records.clear();
    wait_flags.clear();
}
auto clock_gettime(clockid_t, timespec* out) -> int {
    ticks += clock_step;
    out->tv_sec = ticks / 1000000000;
    out->tv_nsec = ticks % 1000000000;
    return 0;
}
auto yield() -> uint64_t {
    ticks += NS_PER_MS;
    return 0;
}
auto munmap(void* address, size_t bytes) -> int {
    if (refuse_unmap) {
        errno = EIO;
        return -1;
    }
    for (auto it = intervals.begin(); it != intervals.end(); ++it)
        if (it->address == reinterpret_cast<uint64_t>(address) && it->bytes == bytes) {
            intervals.erase(it);
            ++unmaps;
            return 0;
        }
    errno = EINVAL;
    return -1;
}
auto huge_aliases(HugeState& state) -> bool {
    ++entry_work_calls;
    entry_complete = state.own.complete;
    entry_defaults = state.options.domains == 1 && state.options.bytes == HUGE_PAYLOAD && state.options.loads == 16777216 &&
                     state.options.timeout_ms == 600000 && state.options.hold_ms == 10000 && state.remaining == 46 && state.own.fds == 0 &&
                     state.own.maps == 0 && state.own.max_fds == 0 && state.own.max_maps == 0 && state.own.created_fds == 0 &&
                     state.own.closed_fds == 0 && state.own.created_maps == 0 && state.own.unmapped == 0;
    int fd = 57;
    ++state.own.fds;
    ++state.own.created_fds;
    refuse_close = entry_mode == EntryMode::CloseFailure;
    bool const CLOSED = ::close_fd(fd, state.own);
    HugeMapping mapping{};
    mapping.address = 0xC00000;
    mapping.bytes = PAGE;
    mapping.owned = true;
    mapping.live[0] = {mapping.address, PAGE};
    ++state.own.maps;
    ++state.own.created_maps;
    intervals = {mapping.live[0]};
    refuse_unmap = entry_mode == EntryMode::UnmapRetry;
    bool const RELEASED = ::huge_release(state, mapping);
    bool retried = true;
    if (!RELEASED) {
        refuse_unmap = false;
        retried = ::huge_release(state, mapping);
    }
    post_resource_complete = state.own.complete;
    post_maps = state.own.maps;
    post_fds = state.own.fds;
    return CLOSED && RELEASED && retried;
}
auto waitpid(pid_t, int* status, int flags) -> pid_t {
    wait_flags.push_back(flags);
    if (wait_cursor >= waits.size()) {
        *status = 0;
        return 0;
    }
    auto row = waits[wait_cursor++];
    *status = row.status;
    errno = row.error;
    return row.result;
}
auto huge_windows(HugeState&, HugePhase, const HugeMapping&, int = -1, int = -1) -> bool { return true; }
auto huge_payload(const HugeMapping&, bool) -> bool { return true; }
auto huge_changed(const HugeMapping&) -> bool { return true; }
auto huge_byte(uint64_t) -> uint8_t { return 0; }
auto clock_nanosleep(clockid_t clock, int flags, const timespec* when, timespec* remaining) -> int {
    ++sleeps;
    if (clock != CLOCK_MONOTONIC || flags != TIMER_ABSTIME || remaining != nullptr) return EINVAL;
    ticks = static_cast<uint64_t>(when->tv_sec) * 1000000000 + when->tv_nsec;
    return 0;
}
auto read(int fd, void* buffer, size_t bytes) -> ssize_t {
    if (!huge_parent_mode || fd != 101 || bytes != sizeof(HugeReady) || ack_writes >= 2) {
        errno = EIO;
        return -1;
    }
    HugeReady message{};
    message.pod = {1,
                   sizeof(HugePod),
                   369,
                   91,
                   91,
                   static_cast<uint64_t>(HugePhase::ExitOwned),
                   ack_writes + 1,
                   0x400000 + static_cast<uint64_t>(ack_writes) * HUGE_BYTES,
                   HUGE_BYTES,
                   1};
    message.pod_address = 0xA00000;
    message.observed = message.pod.range_address;
    message.hold_end = static_cast<uint64_t>(ack_writes + 1) * 10000 * NS_PER_MS;
    std::memcpy(buffer, &message, sizeof(message));
    return sizeof(message);
}
auto write(int fd, const void* buffer, size_t bytes) -> ssize_t {
    if (huge_parent_mode && fd == 104 && bytes == sizeof(uint64_t)) {
        ++remaining_writes;
        return bytes;
    }
    if (!huge_parent_mode || fd != 104 || bytes != 1 || *static_cast<const char*>(buffer) != 'A') {
        errno = EIO;
        return -1;
    }
    ++ack_writes;
    if (fail_final_ack && ack_writes == 2) {
        errno = EPIPE;
        return -1;
    }
    if (expire_before_wait && ack_writes == 2) ticks = 590000 * NS_PER_MS;
    return 1;
}
auto poll(pollfd*, nfds_t, int) -> int {
    errno = ENOSYS;  // These finite cases need no retry; unexpected polling fails.
    return -1;
}
auto kill(pid_t pid, int signal) -> int {
    ++parent_kills;
    if (!huge_parent_mode || pid != 91 || signal != SIGKILL) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}
auto fprintf(FILE*, const char* format, ...) -> int {
    std::array<char, 4096> buffer{};
    va_list arguments;
    va_start(arguments, format);
    int const written = std::vsnprintf(buffer.data(), buffer.size(), format, arguments);
    va_end(arguments);
    check(written > 0 && static_cast<size_t>(written) < buffer.size(), "bounded actual failure record");
    huge_failure_records.emplace_back(buffer.data());
    errno = EOVERFLOW;  // Actual reporter must restore the pre-report errno.
    return written;
}
auto failure_field(size_t record, const char* key) -> long long {
    if (record >= huge_failure_records.size()) return -999999;
    std::string const search = std::string(" ") + key + "=";
    auto const offset = huge_failure_records[record].find(search);
    if (offset == std::string::npos) return -999999;
    return std::strtoll(huge_failure_records[record].c_str() + offset + search.size(), nullptr, 10);
}
template <class Call>
auto process_call(Call, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t data) -> uint64_t {
    auto* io = reinterpret_cast<ker::abi::ptrace::MemIo*>(data);
    io->transferred = transferred;
    return process_error;
}
template <class Call>
auto vmem_call(Call, uint64_t op, uint64_t address, uint64_t bytes, uint64_t, uint64_t flags) -> uint64_t {
    last_op = op;
    last_address = address;
    last_bytes = bytes;
    last_flags = flags;
    return vm_result;
}
}  // namespace fixture
namespace ker::vmem {
/* @ACTUAL_HUGE_WRAPPERS@ */
}
/* @ACTUAL_HUGE_FUNCTIONS@ */
void huge_parent_failure_cases() {
    using fixture::check;
    auto run = [](int mode) {
        fixture::reset();
        fixture::huge_parent_mode = true;
        fixture::fail_final_ack = mode == 1;
        fixture::expire_before_wait = mode == 4;
        HugeState state{};
        state.options.nonce = 369;
        state.remaining = 12;
        state.frontier = 590000 * NS_PER_MS;
        state.tid = 77;
        state.children = 1;
        state.own.fds = 4;
        state.own.created_fds = 4;
        int ready[]{101, 102}, commands[]{103, 104};
        HugeMapping mapping{};
        if (mode == 0) fixture::waits = {{91, 0, 0}};
        if (mode == 1 || mode == 4) fixture::waits = {{91, SIGKILL, 0}};
        if (mode == 2) fixture::waits = {{-1, 0x7fff, ECHILD}, {91, SIGKILL, 0}};
        if (mode == 3) fixture::waits = {{91, 1 << 8, 0}};
        errno = 0;
        bool const result = huge_parent_case(state, 1, 91, true, false, ready, commands, true, mapping);
        check(result == (mode == 0), "actual parent return preserves success and each injected refusal");
        check(state.remaining == 10 && state.children == 0 && state.own.fds == 0 && state.own.maps == 0 && state.own.complete &&
                  fixture::parent_closes == 4 && fixture::remaining_writes == 1 && fixture::ack_writes == 2 && fixture::sleeps == 2,
              "actual two decrements, original four closes, two holds and exact resource settlement");
        check(fixture::wait_cursor == fixture::waits.size(), "all exact injected child waits consumed");
        if (mode == 0) {
            check(fixture::huge_failure_records.empty() && fixture::parent_kills == 0 && fixture::wait_flags == std::vector<int>{WNOHANG},
                  "actual success has no diagnostic or cleanup kill and retains original wait flags");
            return;
        }
        check(fixture::huge_failure_records.size() == 3 && fixture::huge_failure_records[0].find("NUMA_CHILD_HUGE_FAILURE ") == 0 &&
                  fixture::huge_failure_records[1].find("NUMA_CHILD_HUGE_TRANSFER ") == 0 &&
                  fixture::huge_failure_records[2].find("NUMA_CHILD_HUGE_TRANSFER ") == 0,
              "actual failure reporter emits header and both actual transfer snapshots");
        for (const auto& record : fixture::huge_failure_records)
            check(record.find("NUMA_HUGE_") == std::string::npos, "diagnostic is outside strict existing huge grammar");
        check(fixture::failure_field(0, "parent") == 77 && fixture::failure_field(0, "child") == 91 &&
                  fixture::failure_field(0, "kind") == 1 && fixture::failure_field(0, "nonce") == 369 &&
                  fixture::failure_field(0, "frontier") == 590000 * NS_PER_MS && fixture::failure_field(0, "remaining") == 10 &&
                  fixture::failure_field(0, "serve_index") == 1 && fixture::failure_field(0, "accepted_sequence") == 2 &&
                  fixture::failure_field(0, "received_sequence_valid") == 1 && fixture::failure_field(0, "received_sequence") == 2,
              "actual failure carries precise original actor, frontier and second-window identity");
        check(fixture::failure_field(0, "maps") == 0 && fixture::failure_field(0, "fds") == 0 &&
                  fixture::failure_field(0, "children") == 0 && fixture::failure_field(0, "complete") == 1 && errno == 0,
              "actual reporting occurs after settlement and restores errno even when stdio changes it");
        if (mode == 1) {
            check(fixture::failure_field(0, "serve_stage") == static_cast<unsigned>(HugeServeStage::Ack) &&
                      fixture::failure_field(0, "wait_called") == 0 && fixture::failure_field(0, "wait_rc_valid") == 0 &&
                      fixture::failure_field(0, "wait_status_valid") == 0 && fixture::failure_field(0, "precleanup_owned") == 1 &&
                      fixture::failure_field(0, "cleanup_called") == 1 && fixture::failure_field(0, "cleanup_status") == SIGKILL &&
                      fixture::failure_field(2, "io_rc") == -1 && fixture::failure_field(2, "io_errno") == EPIPE &&
                      (fixture::failure_field(2, "valid") & READY_IO_ERRNO) != 0 && fixture::parent_kills == 1 &&
                      fixture::wait_flags == std::vector<int>{0},
                  "actual final ACK failure is distinct from uncalled terminal wait and later SIGKILL cleanup");
        } else if (mode == 2) {
            check(fixture::failure_field(0, "serve_stage") == static_cast<unsigned>(HugeServeStage::Complete) &&
                      fixture::failure_field(0, "wait_called") == 1 && fixture::failure_field(0, "wait_ok") == 0 &&
                      fixture::failure_field(0, "wait_rc_valid") == 1 && fixture::failure_field(0, "wait_rc") == -1 &&
                      fixture::failure_field(0, "wait_errno_valid") == 1 && fixture::failure_field(0, "wait_errno") == ECHILD &&
                      fixture::failure_field(0, "wait_status_valid") == 0 && fixture::failure_field(0, "wait_status") == 0 &&
                      fixture::failure_field(0, "precleanup_owned") == 1 && fixture::failure_field(0, "cleanup_status_valid") == 1 &&
                      fixture::failure_field(0, "cleanup_status") == SIGKILL && fixture::parent_kills == 1 &&
                      fixture::wait_flags == std::vector<int>({WNOHANG, 0}),
                  "actual wait errno/status validity stays separate from exact cleanup consumption");
        } else if (mode == 3) {
            check(fixture::failure_field(0, "wait_status_valid") == 1 && fixture::failure_field(0, "wait_status") == (1 << 8) &&
                      fixture::failure_field(0, "wait_errno_valid") == 0 && fixture::failure_field(0, "precleanup_owned") == 0 &&
                      fixture::failure_field(0, "cleanup_called") == 0 && fixture::failure_field(0, "cleanup_status_valid") == 0 &&
                      fixture::parent_kills == 0 && fixture::wait_flags == std::vector<int>{WNOHANG},
                  "actual nonzero child exit is consumed without a false cleanup kill or errno");
        } else {
            check(fixture::failure_field(0, "wait_called") == 1 && fixture::failure_field(0, "wait_rc_valid") == 0 &&
                      fixture::failure_field(0, "wait_errno_valid") == 0 && fixture::failure_field(0, "wait_status_valid") == 0 &&
                      fixture::failure_field(0, "wait_polls") == 0 && fixture::failure_field(0, "precleanup_owned") == 1 &&
                      fixture::parent_kills == 1 && fixture::wait_flags == std::vector<int>{0},
                  "actual deadline-before-first-wait reports called without inventing a wait result or status");
        }
    };
    for (int mode = 0; mode != 5; ++mode) run(mode);
}
int main() {
    huge_parent_failure_cases();
    using fixture::check;
    fixture::reset();
    HugeOptions parsed{};
    const char* command[]{"--nonce", "369", "--domains", "2", "--phase", "correctness"};
    check(huge_options(6, const_cast<char**>(command), parsed) && parsed.timeout_ms == 600000 && parsed.bytes == HUGE_PAYLOAD,
          "bounded default correctness/500s holds");
    const char* short_clock[]{"--nonce", "369", "--domains", "2", "--phase", "correctness", "--timeout-ms", "120000"};
    HugeOptions short_options{};
    check(!huge_options(8, const_cast<char**>(short_clock), short_options), "refuse too short before allocation");
    const char* duplicate[]{"--nonce", "369", "--domains", "1", "--phase", "bench", "--nonce", "370"};
    HugeOptions duplicate_options{};
    check(!huge_options(8, const_cast<char**>(duplicate), duplicate_options), "duplicate nonce refuses");
    const char* bench[]{"--nonce", "369", "--domains", "1", "--phase", "bench", "--bytes", "67108864", "--loads", "16"};
    HugeOptions bench_options{};
    check(huge_options(10, const_cast<char**>(bench), bench_options) && bench_options.bench && bench_options.bytes == (uint64_t{64} << 20),
          "optional sampled benchmark bound");
    HugeState state{};
    state.options = parsed;
    state.remaining = 50;
    state.frontier = 590000 * NS_PER_MS;
    check(huge_admit(state), "original frontier includes every future hold");
    fixture::ticks = 100000 * NS_PER_MS;
    check(!huge_admit(state), "work cannot consume reserved future holds");
    fixture::reset();
    check(huge_expected(3, 3, 3 * HUGE_BYTES) == 1 && huge_expected(3, 3, 4 * HUGE_BYTES) == 0, "interleave uses absolute2MiB index");
    check(huge_expected(2, 2, 0) == 1 && huge_expected(2, 0, 0) == abi::INVALID_NODE, "strict node/missing mask");
    HugeMapping mapping{};
    mapping.address = 0x400000;
    mapping.bytes = HUGE_PAYLOAD;
    mapping.owned = true;
    mapping.live[0] = {mapping.address, PAGE};
    mapping.live[1] = {mapping.address + 2 * PAGE, mapping.bytes - 2 * PAGE};
    state.own.maps = 1;
    fixture::intervals = {mapping.live[0], mapping.live[1]};
    check(huge_release(state, mapping) && !mapping.owned && state.own.maps == 0 && fixture::intervals.empty() && fixture::unmaps == 2,
          "exact surviving intervals really unmapped");
    mapping.address = 0x800000;
    mapping.bytes = PAGE;
    mapping.owned = true;
    mapping.live[0] = {mapping.address, PAGE};
    state.own.maps = 1;
    state.own.complete = true;
    fixture::intervals = {mapping.live[0]};
    fixture::refuse_unmap = true;
    check(!huge_release(state, mapping) && mapping.owned && mapping.live[0].bytes == PAGE && state.own.maps == 1 && !state.own.complete,
          "failed unmap retains owner/interval");
    fixture::reset();
    bool owned = true;
    HugeWait observation{};
    fixture::waits = {{0, 0x137f, 0}, {91, 0x137f, 0}};
    check(huge_wait(91, 100 * NS_PER_MS, true, 0, owned, observation) && owned && observation.result == 91 && observation.status_valid &&
              observation.polls == 2,
          "zero poll is not a stop/reap; exact stop consumed");
    fixture::reset();
    owned = true;
    observation = {};
    fixture::waits = {{91, 0, 0}};
    check(!huge_wait(91, 100 * NS_PER_MS, true, 0, owned, observation) && !owned && observation.status_valid, "exit0 is not a SIGSTOP");
    fixture::reset();
    owned = true;
    observation = {};
    fixture::waits = {{92, 0x137f, 0}};
    check(!huge_wait(91, 100 * NS_PER_MS, true, 0, owned, observation) && owned && !observation.status_valid,
          "foreign PID cannot publish status or relinquish owner");
    fixture::reset();
    owned = true;
    observation = {};
    fixture::waits = {{91, SIGSEGV, 0}};
    check(huge_wait(91, 100 * NS_PER_MS, false, SIGSEGV, owned, observation) && !owned, "expected permission fault consumes child");
    fixture::reset();
    std::array<uint8_t, 256> buffer{};
    check(huge_ptrace_io(91, 0x5fff80, false, buffer), "actual MemIo full read");
    fixture::transferred = 255;
    check(!huge_ptrace_io(91, 0x5fff80, true, buffer), "zero return with short transferred count refuses");
    fixture::transferred = 256;
    fixture::process_error = static_cast<uint64_t>(-EFAULT);
    check(!huge_ptrace_io(91, 0x5fff80, false, buffer), "ptrace error refuses despite count");
    void* output = reinterpret_cast<void*>(0x369);
    fixture::vm_result = static_cast<uint64_t>(-ENOMEM);
    check(ker::vmem::allocate(&output, HUGE_BYTES, 3, HUGE_FLAGS) == -ENOMEM && output == reinterpret_cast<void*>(0x369),
          "actual raw outparam unchanged on refusal");
    fixture::vm_result = 0x400000;
    check(ker::vmem::allocate(&output, HUGE_BYTES, 3, HUGE_FLAGS) == 0 && output == reinterpret_cast<void*>(0x400000) &&
              fixture::last_bytes == HUGE_BYTES && fixture::last_flags == HUGE_FLAGS,
          "actual allocate ABI transports huge flag/size");
    fixture::vm_result = 0;
    check(ker::vmem::free(output, HUGE_BYTES) == 0 && fixture::last_address == 0x400000 && fixture::last_bytes == HUGE_BYTES,
          "actual free ABI transports owned extent");
    // These execute both complete actual entry functions, not copied setup/final expressions.
    const char* entry_command[]{"--nonce", "369", "--domains", "1", "--phase", "correctness"};
    fixture::reset();
    check(run_numa_huge_predecessor(6, const_cast<char**>(entry_command)) == 1 && fixture::entry_work_calls == 1 &&
              fixture::entry_defaults && !fixture::entry_complete && !fixture::post_resource_complete && fixture::post_maps == 0 &&
              fixture::post_fds == 0 && fixture::restore_policy_calls == 0 && fixture::restore_affinity_calls == 0,
          "genuine old entry wipes complete and refuses restore/final success");
    fixture::reset();
    check(run_numa_huge(6, const_cast<char**>(entry_command)) == 0 && fixture::entry_work_calls == 1 && fixture::entry_defaults &&
              fixture::entry_complete && fixture::post_resource_complete && fixture::post_maps == 0 && fixture::post_fds == 0 &&
              fixture::restore_policy_calls == 1 && fixture::restore_affinity_calls == 1,
          "actual fixed entry restores typed defaults before work and reaches real final gate");
    fixture::reset();
    fixture::entry_mode = fixture::EntryMode::CloseFailure;
    check(run_numa_huge(6, const_cast<char**>(entry_command)) == 1 && fixture::entry_complete && !fixture::post_resource_complete &&
              fixture::post_fds == 1 && fixture::restore_policy_calls == 0 && fixture::restore_affinity_calls == 0,
          "actual close failure remains owned/incomplete and rejects final success");
    fixture::reset();
    fixture::entry_mode = fixture::EntryMode::UnmapRetry;
    check(run_numa_huge(6, const_cast<char**>(entry_command)) == 1 && fixture::entry_complete && !fixture::post_resource_complete &&
              fixture::post_maps == 0 && fixture::post_fds == 0 && fixture::unmaps == 1 && fixture::restore_policy_calls == 0,
          "actual failed unmap stays incomplete after successful owned retirement retry");
    fixture::reset();
    fixture::entry_mode = fixture::EntryMode::WorkloadFailure;
    check(run_numa_huge(6, const_cast<char**>(entry_command)) == 1 && fixture::post_resource_complete && fixture::post_maps == 0 &&
              fixture::post_fds == 0 && fixture::restore_policy_calls == 1 && fixture::restore_affinity_calls == 1,
          "actual final work failure rejects despite clean ledger and successful restore");
    fixture::reset();
    fixture::entry_mode = fixture::EntryMode::RestoreFailure;
    check(run_numa_huge(6, const_cast<char**>(entry_command)) == 1 && fixture::post_resource_complete && fixture::post_maps == 0 &&
              fixture::post_fds == 0 && fixture::restore_policy_calls == 1 && fixture::restore_affinity_calls == 1,
          "actual affinity restore refusal cannot pass the final gate");
    std::printf("huge actual-critical-helper finite cases checks=%u failures=%u\n", fixture::checks, fixture::failures);
    return fixture::failures ? 1 : 0;
}
