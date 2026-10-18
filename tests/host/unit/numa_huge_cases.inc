// Actual critical guest helpers, explicit finite ports only. The full guest
// entry/setup/final gates run over finite endpoints; actual phase workloads,
// fork/ptrace permissions, Task, PS mapper, AS and physical owner do not run.
#include <abi/callnums/process.h>
#include <abi/callnums/vmem.h>
#include <abi/numa.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
auto close(int fd) -> int {
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
auto waitpid(pid_t, int* status, int) -> pid_t {
    if (wait_cursor >= waits.size()) {
        *status = 0;
        return 0;
    }
    auto row = waits[wait_cursor++];
    *status = row.status;
    errno = row.error;
    return row.result;
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
int main() {
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
