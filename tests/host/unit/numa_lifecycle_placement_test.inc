// Actual lifecycle handshake/parent branch on finite syscall endpoints.
// No WOS Task, native migration, concurrent scheduler or real child is modeled.
#include <abi/numa.h>
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>
namespace abi = ker::abi::vmem::numa;
/* @ACTUAL_TYPES@ */
static_assert(sizeof(ChildRecord) == 136 && sizeof(ChildReady) <= 160);
namespace fixture {
unsigned assertions{}, failures{}, cases{};
void check(bool good, const char* reason) {
    ++assertions;
    if (!good && failures++ < 24) std::fprintf(stderr, "FAIL: %s\n", reason);
}
uint64_t now{}, tick{}, tid{}, cpu{}, mask{}, set_count{}, get_count{}, yield_count{};
uint64_t deadline{1000}, deliver_cpu{UINT64_MAX};
int policy_error{}, payload_result{}, payload_calls{}, set_error{}, kill_calls{}, reap_calls{};
bool busy_once{}, always_busy{};
size_t input_at{}, read_max{}, write_max{};
std::vector<unsigned char> input, output;
std::vector<std::string> events;
void reset() {
    ++cases;
    now = 1;
    tick = 1;
    tid = 110;
    cpu = 3;
    mask = 8;
    deadline = 1000;
    set_count = get_count = yield_count = 0;
    deliver_cpu = UINT64_MAX;
    policy_error = payload_result = payload_calls = set_error = kill_calls = reap_calls = 0;
    busy_once = always_busy = false;
    input_at = 0;
    read_max = write_max = 4096;
    input.clear();
    output.clear();
    events.clear();
}
auto clock_gettime(clockid_t which, timespec* out) -> int {
    check(which == CLOCK_MONOTONIC, "original monotonic clock");
    out->tv_sec = static_cast<time_t>(now / 1000000000);
    out->tv_nsec = static_cast<long>(now % 1000000000);
    now += tick;
    return 0;
}
auto getpid() -> pid_t { return 110; }
auto read(int fd, void* destination, size_t bytes) -> ssize_t {
    events.emplace_back("read");
    check(fd == 7, "existing parent read FD");
    if (input_at == input.size()) return 0;
    size_t const count = std::min({bytes, input.size() - input_at, read_max});
    std::memcpy(destination, input.data() + input_at, count);
    input_at += count;
    return static_cast<ssize_t>(count);
}
auto write(int fd, const void* source, size_t bytes) -> ssize_t {
    events.emplace_back("write");
    check(fd == 8, "existing child write FD");
    size_t const count = std::min(bytes, write_max);
    auto* p = static_cast<const unsigned char*>(source);
    output.insert(output.end(), p, p + count);
    return static_cast<ssize_t>(count);
}
auto poll(pollfd*, nfds_t, int milliseconds) -> int {
    now += static_cast<uint64_t>(milliseconds) * 1000000;
    return 0;
}
auto kill(pid_t pid, int signal) -> int {
    check(pid == 110 && signal == SIGKILL, "exact old cleanup kill");
    ++kill_calls;
    return 0;
}
auto waitpid(pid_t pid, int* status, int flags) -> pid_t {
    check(pid == 110 && flags == 0, "exact old consuming cleanup wait");
    ++reap_calls;
    *status = 0;
    return pid;
}
template <class T>
void append(const T& value) {
    auto* p = reinterpret_cast<const unsigned char*>(&value);
    input.insert(input.end(), p, p + sizeof(value));
}
auto topology() -> Topology {
    Topology out{};
    out.summary.node_count = 2;
    out.summary.cpu_count = 8;
    out.nodes[0].managed_bytes = out.nodes[1].managed_bytes = 1 << 20;
    out.nodes[0].cpu_words[0] = 0x0f;
    out.nodes[1].cpu_words[0] = 0xf0;
    out.selected[0] = {0, 0};
    out.selected[1] = {1, 4};
    out.selected_count = 2;
    return out;
}
auto policy() -> abi::SelfPolicy {
    abi::SelfPolicy out{};
    out.version = abi::VERSION;
    out.bytes = sizeof(out);
    out.mode = static_cast<uint32_t>(abi::PolicyMode::LOCAL);
    return out;
}
Selection payload_target{0, 3};
}  // namespace fixture
namespace ker::multiproc {
auto currentThreadId() -> uint64_t {
    fixture::events.emplace_back("tid");
    return fixture::tid;
}
auto getcurrent_cpu() -> uint64_t {
    fixture::events.emplace_back("cpu");
    return fixture::cpu;
}
auto getThreadAffinityMask(uint64_t tid) -> int64_t {
    fixture::events.emplace_back("get");
    ++fixture::get_count;
    fixture::check(tid == 110, "actual child self getter");
    return static_cast<int64_t>(fixture::mask);
}
auto setThreadAffinityMask(uint64_t tid, uint64_t mask) -> int64_t {
    fixture::events.emplace_back("set");
    ++fixture::set_count;
    fixture::check(tid == 110 && mask == 16, "actual parent foreign target setter");
    if (fixture::always_busy || (fixture::busy_once && fixture::set_count == 1)) return -EBUSY;
    if (fixture::set_error) return fixture::set_error;
    fixture::mask = mask;
    fixture::cpu = 4;
    return 0;
}
auto yield() -> uint64_t {
    fixture::events.emplace_back("yield");
    ++fixture::yield_count;
    if (fixture::deliver_cpu != UINT64_MAX) {
        fixture::cpu = fixture::deliver_cpu;
        fixture::mask = uint64_t{1} << fixture::deliver_cpu;
        fixture::deliver_cpu = UINT64_MAX;
        fixture::events.emplace_back("external-set");
    }
    return 0;
}
}  // namespace ker::multiproc
namespace ker::vmem {
auto query_numa(const ::abi::QueryRequest& request) -> int64_t {
    fixture::events.emplace_back("policy");
    fixture::check(
        request.kind == static_cast<uint32_t>(::abi::QueryKind::SELF_POLICY) && request.output_bytes == sizeof(::abi::SelfPolicy),
        "actual bounded policy query");
    if (fixture::policy_error) return fixture::policy_error;
    *reinterpret_cast<::abi::SelfPolicy*>(request.output_address) = fixture::policy();
    return 0;
}
}  // namespace ker::vmem
// Only the unchanged original payload is an endpoint; no copied COW algorithm.
auto child_cycle(uint8_t*, int fd, const Options&, const Topology&, uint32_t, uint64_t) -> int {
    fixture::events.emplace_back("payload");
    ++fixture::payload_calls;
    fixture::check(fd == 8 && fixture::cpu == fixture::payload_target.cpu && fixture::mask == (uint64_t{1} << fixture::payload_target.cpu),
                   "payload requires completed actual placement wait");
    return fixture::payload_result;
}
/* @ACTUAL_HANDSHAKE@ */
auto parent_frontier(const Options& opts, const Topology& topo, Selection parent_selection, Selection child_target, uint32_t cycle,
                     const char*& phase) -> bool {
    int fds[2]{7, -1};
    pid_t pid = 110;
    bool ok = true;
    uint64_t child_deadline = fixture::deadline;
    /* @ACTUAL_PARENT_BRANCH@ */
    return ok;
}
namespace fixture {
auto ready(const Options& opts, Selection target, uint32_t cycle) -> ChildReady {
    ChildReady out{};
    out.magic = CHILD_READY_MAGIC;
    out.version = 1;
    out.bytes = sizeof(out);
    out.seed = opts.seed;
    out.cycle = cycle;
    out.pid = 110;
    out.tid = 110;
    out.inherited_mask = 8;
    out.inherited_cpu = 3;
    out.policy = policy();
    out.target_node = target.node;
    out.target_cpu = target.cpu;
    return out;
}
auto record(const Options& opts, Selection target, uint32_t cycle) -> ChildRecord {
    ChildRecord out{};
    out.magic = RECORD_MAGIC;
    out.version = 1;
    out.bytes = sizeof(out);
    out.seed = opts.seed;
    out.cycle = cycle;
    out.pid = 110;
    out.tid = 110;
    out.cpu_before = out.cpu_after = target.cpu;
    out.policy = policy();
    out.selected_node = target.node;
    out.selected_cpu = target.cpu;
    out.phase = static_cast<uint32_t>(ChildPhase::COMPLETE);
    for (auto& page : out.pages) page = {target.node, static_cast<uint16_t>(abi::PageStatus::RESIDENT), 0};
    return out;
}
void cleanup_refusal() {
    bool owned = true;
    CleanupObservation observation{};
    check(terminate_and_reap(110, owned, &observation) && !owned && kill_calls == 1 && reap_calls == 1 && observation.status_valid &&
              observation.status == 0,
          "new phase refusal reaches unchanged real cleanup helper");
    check(terminate_and_reap(110, owned, &observation) && kill_calls == 1 && reap_calls == 1, "no-owned cleanup remains no-call");
}
void supplied(const Options& opts, Selection target, uint32_t cycle) {
    append(ready(opts, target, cycle));
    append(record(opts, target, cycle));
}
}  // namespace fixture
auto main() -> int {
    using namespace fixture;
    Options opts{};
    Topology topo = topology();
    Selection parent{0, 3}, target{};
    const char* phase = "initial";
    reset();
    check(lifecycle_child_target(topo, parent, 0, target) && target.node == 0 && target.cpu == 3,
          "even exact parent CPU outside selected list");
    reset();
    check(lifecycle_child_target(topo, parent, 1, target) && target.node == 1 && target.cpu == 4, "odd eligible foreign CPU");
    reset();
    {
        auto one = topo;
        one.selected_count = 1;
        check(lifecycle_child_target(one, parent, 1, target) && target.cpu == 3,
              "one eligible preserves parent despite memory-only extra node");
    }
    reset();
    {
        auto bad = topo;
        bad.selected[1] = {0, 1};
        check(!lifecycle_child_target(bad, parent, 1, target), "missing required foreign refuses");
    }
    reset();
    check(!lifecycle_child_target(topo, {0, 64}, 1, target), "invalid parent refuses before shift");
    {
        auto three = topo;
        three.summary.node_count = 3;
        three.summary.cpu_count = 12;
        three.nodes[2].managed_bytes = 1 << 20;
        three.nodes[2].cpu_words[0] = 0xf00;
        check(lifecycle_child_target(three, {2, 8}, 1, target) && target.node == 0 && target.cpu == 0,
              "parent need not belong to either selected node");
    }
    reset();
    {
        auto r = ready(opts, {1, 4}, 1);
        check(valid_child_ready(r, opts, topo, parent, {1, 4}, 1, 110), "complete exact readiness accepted");
        for (unsigned field = 0; field < 13; ++field) {
            auto bad = r;
            switch (field) {
                case 0:
                    bad.magic = 0;
                    break;
                case 1:
                    bad.bytes = 0;
                    break;
                case 2:
                    bad.seed++;
                    break;
                case 3:
                    bad.cycle++;
                    break;
                case 4:
                    bad.pid++;
                    break;
                case 5:
                    bad.tid++;
                    break;
                case 6:
                    bad.inherited_mask = 4;
                    break;
                case 7:
                    bad.inherited_cpu = 2;
                    break;
                case 8:
                    bad.target_cpu = 5;
                    break;
                case 9:
                    bad.target_node = 0;
                    break;
                case 10:
                    bad.reserved = 1;
                    break;
                case 11:
                    bad.error = -EIO;
                    break;
                case 12:
                    bad.version = 2;
                    break;
            }
            check(!valid_child_ready(bad, opts, topo, parent, {1, 4}, 1, 110), "malformed exact readiness refuses");
        }
    }
    reset();
    {
        auto r = ready(opts, {1, 4}, 1);
        r.policy.mode = static_cast<uint32_t>(abi::PolicyMode::BIND);
        check(!valid_child_ready(r, opts, topo, parent, {1, 4}, 1, 110), "inherited policy must be observed LOCAL");
    }
    reset();
    supplied(opts, {0, 3}, 0);
    check(parent_frontier(opts, topo, parent, {0, 3}, 0, phase) && set_count == 0 && get_count == 0,
          "already-target parent has no raced SET or GET");
    reset();
    supplied(opts, {1, 4}, 1);
    busy_once = true;
    check(parent_frontier(opts, topo, parent, {1, 4}, 1, phase) && set_count == 2 && yield_count == 1,
          "remote EBUSY then real setter success");
    reset();
    supplied(opts, {1, 4}, 1);
    always_busy = true;
    deadline = 6;
    check(!parent_frontier(opts, topo, parent, {1, 4}, 1, phase) && set_count > 0, "remote placement bounded by original deadline");
    cleanup_refusal();
    reset();
    supplied(opts, {1, 4}, 1);
    set_error = -EINVAL;
    check(!parent_frontier(opts, topo, parent, {1, 4}, 1, phase) && set_count == 1, "nonEBUSY remote error refuses immediately");
    cleanup_refusal();
    reset();
    {
        auto r = ready(opts, {1, 4}, 1);
        append(r);
        input.resize(sizeof(r) - 1);
        check(!parent_frontier(opts, topo, parent, {1, 4}, 1, phase) && set_count == 0, "partial ready refuses before placement");
        cleanup_refusal();
    }
    reset();
    {
        auto r = ready(opts, {1, 4}, 1);
        r.pid = 111;
        append(r);
        check(!parent_frontier(opts, topo, parent, {1, 4}, 1, phase) && set_count == 0, "wrong child PID refuses before placement");
        cleanup_refusal();
    }
    reset();
    append(ready(opts, {0, 3}, 0));
    append(record(opts, {1, 4}, 0));
    check(!parent_frontier(opts, topo, parent, {0, 3}, 0, phase), "completed payload record must match exact planned target");
    cleanup_refusal();
    reset();
    read_max = 7;
    supplied(opts, {0, 3}, 0);
    check(parent_frontier(opts, topo, parent, {0, 3}, 0, phase) && input_at == input.size(),
          "actual bounded transfer handles chunked complete ready and record");
    reset();
    payload_target = {0, 3};
    check(child_ready_and_cycle(nullptr, 8, opts, topo, parent, 0, deadline) == 0 && payload_calls == 1 && get_count >= 2,
          "child self snapshot then exact target wait then original payload");
    check(std::find(events.begin(), events.end(), "policy") < std::find(events.begin(), events.end(), "write") &&
              std::find(events.begin(), events.end(), "write") < std::find(events.begin(), events.end(), "payload"),
          "inherited policy precedes ready publication and payload");
    reset();
    payload_target = {1, 4};
    deliver_cpu = 4;
    check(child_ready_and_cycle(nullptr, 8, opts, topo, parent, 1, deadline) == 0 && payload_calls == 1 && yield_count > 0,
          "remote child waits for finite external controller effect");
    reset();
    mask = 4;
    check(child_ready_and_cycle(nullptr, 8, opts, topo, parent, 1, deadline) == 66 && payload_calls == 0,
          "wrong inherited mask fails before payload");
    reset();
    policy_error = -EIO;
    check(child_ready_and_cycle(nullptr, 8, opts, topo, parent, 1, deadline) == 66 && payload_calls == 0,
          "policy query failure is sent as refusal");
    reset();
    payload_target = {1, 4};
    deadline = 9;
    check(child_ready_and_cycle(nullptr, 8, opts, topo, parent, 1, deadline) == 67 && payload_calls == 0,
          "unplaced child cannot bypass original deadline");
    std::printf("%s actual lifecycle placement cases=%u assertions=%u failures=%u\n", failures ? "FAIL" : "PASS", cases, assertions,
                failures);
    return failures ? 1 : 0;
}
