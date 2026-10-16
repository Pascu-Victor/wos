// Actual guest bodies on finite transport/Task/placement/memory/child ports.
// No WOS Task, scheduler, hardware fault, physical allocator or kernel quota runs.
#include <abi/callnums/process.h>
#include <abi/numa.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* @ACTUAL_CALLNUMS@ */
#include <abi/resource.hpp>
#include <algorithm>
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
namespace abi = ker::abi::vmem::numa;
constexpr size_t PAGE = 4096;
constexpr uint64_t NS_PER_MS = 1000000;
/* @ACTUAL_TYPES@ */
namespace fixture {
struct Context {
    uint64_t pid{100}, tid{100}, affinity{1};
    uint32_t mode{};
    uint64_t mask{};
    unsigned sets{};
} current;
struct Mapping {
    uint8_t* bytes{};
    size_t size{};
    std::vector<uint16_t> nodes;
    bool resident{};
};
struct Fd {
    bool open{};
    int pipe{-1};
};
struct Pipe {
    std::string bytes;
    size_t cursor{};
};
std::vector<Mapping> mappings;
std::array<Fd, 128> fds{};
std::array<Pipe, 64> pipes{};
uint32_t domains{2}, next_pipe{}, next_fd{3}, child_status{}, child_polls{};
uint64_t ticks{}, next_pid{101}, pending_child{}, next_tid{201};
rlimit limits{RLIM_INFINITY, RLIM_INFINITY};
uint64_t denials{};
std::string output, fault;
PolicyWorker* active_worker{};
bool first_thread_query{}, first_child_query{};
unsigned assertions{}, failures{}, cases{};
void check(bool good, const char* why) {
    ++assertions;
    if (!good) {
        ++failures;
        std::printf("FAIL: %s\n", why);
    }
}
auto logical_bytes() -> uint64_t {
    uint64_t value = 64ULL * 1024 * 1024;
    for (auto const& m : mappings) value += m.size;
    return value;
}
void reset(uint32_t nodes, const char* failure = "") {
    for (auto& m : mappings) std::free(m.bytes);
    mappings.clear();
    fds = {};
    pipes = {};
    current = {};
    domains = nodes;
    next_pipe = 0;
    next_fd = 3;
    ticks = 0;
    next_pid = 101;
    next_tid = 201;
    pending_child = 0;
    child_status = 0;
    child_polls = 0;
    limits = {RLIM_INFINITY, RLIM_INFINITY};
    denials = 0;
    output.clear();
    fault = failure;
    active_worker = nullptr;
    first_thread_query = false;
    first_child_query = false;
}
auto clock_gettime(clockid_t, timespec* ts) -> int {
    ticks += 1000;
    ts->tv_sec = ticks / 1000000000;
    ts->tv_nsec = ticks % 1000000000;
    return 0;
}
auto printf(const char* format, ...) -> int {
    char line[2048];
    va_list args;
    va_start(args, format);
    int count = std::vsnprintf(line, sizeof(line), format, args);
    va_end(args);
    if (count < 0 || static_cast<size_t>(count) >= sizeof(line)) std::abort();
    output.append(line, count);
    return count;
}
auto getpid() -> pid_t { return static_cast<pid_t>(current.pid); }
auto mmap(void*, size_t size, int, int, int, uint64_t) -> void* {
    if (logical_bytes() > limits.rlim_cur || size > limits.rlim_cur - logical_bytes()) {
        ++denials;
        errno = ENOMEM;
        return MAP_FAILED;
    }
    void* address = nullptr;
    if (posix_memalign(&address, PAGE, size) != 0) {
        errno = ENOMEM;
        return MAP_FAILED;
    }
    std::memset(address, 0, size);
    mappings.push_back({static_cast<uint8_t*>(address), size, {}, false});
    return address;
}
auto munmap(void* address, size_t size) -> int {
    if (fault == "unmap") {
        errno = EIO;
        return -1;
    }
    for (auto i = mappings.begin(); i != mappings.end(); ++i)
        if (i->bytes == address && i->size == size) {
            std::free(address);
            mappings.erase(i);
            return 0;
        }
    errno = EINVAL;
    return -1;
}
auto getrlimit(int resource, rlimit* out) -> int {
    check(resource == RLIMIT_AS, "actual AS selector");
    *out = limits;
    return 0;
}
auto setrlimit(int resource, const rlimit* in) -> int {
    check(resource == RLIMIT_AS, "actual AS setter");
    limits = *in;
    return 0;
}
auto resource_call(ker::abi::callnums subsystem, uint64_t op, uint64_t id, uint64_t pointer, uint64_t version) -> uint64_t {
    check(subsystem == ker::abi::callnums::process && op == static_cast<uint64_t>(ker::abi::process::procmgmt_ops::RESOURCE_USAGE) &&
              id == 9 && version == 1,
          "actual resource transport fields");
    auto* out = reinterpret_cast<ker::abi::resource::Usage*>(pointer);
    *out = {};
    out->current = logical_bytes();
    out->peak = out->current;
    out->limit = {limits.rlim_cur, limits.rlim_max};
    out->denied = fault == "no-denial" ? 0 : denials;
    return 0;
}
auto query(const abi::QueryRequest& request) -> int64_t {
    check(request.version == 1 && request.bytes == sizeof(request) && request.flags == 0, "actual query transport");
    void* result = reinterpret_cast<void*>(request.output_address);
    if (request.kind == static_cast<uint32_t>(abi::QueryKind::SUMMARY)) {
        abi::Summary value{};
        value.version = 1;
        value.bytes = sizeof(value);
        value.features = abi::FEATURES_V1;
        value.provenance = domains == 2 ? 2 : 0;
        value.node_count = domains;
        value.cpu_count = domains;
        value.page_bytes = PAGE;
        value.max_page_nodes = 64;
        value.cpu_mask_words = 4;
        std::memcpy(result, &value, sizeof(value));
        return 0;
    }
    if (request.kind == static_cast<uint32_t>(abi::QueryKind::NODE)) {
        abi::Node value{};
        value.version = 1;
        value.bytes = sizeof(value);
        value.node = request.index;
        value.managed_bytes = 4ULL * 1024 * 1024 * 1024;
        value.cpu_words[0] = uint64_t{1} << request.index;
        std::memcpy(result, &value, sizeof(value));
        return 0;
    }
    if (request.kind == static_cast<uint32_t>(abi::QueryKind::SELF_POLICY)) {
        abi::SelfPolicy value{};
        value.version = 1;
        value.bytes = sizeof(value);
        value.mode = current.mode;
        value.nodes = current.mask;
        if ((first_thread_query && fault == "thread-inheritance") || (first_child_query && fault == "child-inheritance"))
            value.mode = (value.mode + 1) % 4;
        first_thread_query = false;
        first_child_query = false;
        std::memcpy(result, &value, sizeof(value));
        return 0;
    }
    auto* rows = static_cast<abi::PageNode*>(result);
    for (uint32_t p = 0; p < request.count; ++p) {
        rows[p] = {abi::INVALID_NODE, static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE), 0};
        uint64_t address = request.address + p * PAGE;
        for (auto& map : mappings)
            if (address >= reinterpret_cast<uint64_t>(map.bytes) && address - reinterpret_cast<uint64_t>(map.bytes) < map.size) {
                // Finite physical endpoint: first payload store becomes resident at
                // the next observation. This is not a hardware fault model/proof.
                if (!map.resident && (map.bytes[0] != 0 || fault == "nonvirgin")) {
                    map.resident = true;
                    map.nodes.resize(map.size / PAGE);
                    for (size_t n = 0; n < map.nodes.size(); ++n) {
                        uint64_t va = reinterpret_cast<uint64_t>(map.bytes) + n * PAGE;
                        uint16_t node = current.mode == 0   ? 0
                                        : current.mode == 3 ? static_cast<uint16_t>((va / PAGE) % domains)
                                                            : static_cast<uint16_t>(current.mask == 2 ? 1 : 0);
                        map.nodes[n] = fault == "placement" && domains == 2 ? 1 - node : node;
                    }
                }
                if (map.resident)
                    rows[p] = {map.nodes[(address - reinterpret_cast<uint64_t>(map.bytes)) / PAGE],
                               static_cast<uint16_t>(abi::PageStatus::RESIDENT), 0};
            }
    }
    return 0;
}
auto set_policy(const abi::PolicyRequest& request) -> int64_t {
    ++current.sets;
    if (fault == "unchanged-actor-policy" && current.tid != 100 && current.sets == 3) return 0;
    current.mode = request.mode;
    current.mask = request.nodes;
    return 0;
}
auto cpu() -> uint64_t { return fault == "cpu" ? 1 : 0; }
auto tid() -> uint64_t { return current.tid; }
auto affinity(uint64_t) -> int64_t { return current.affinity; }
auto set_affinity(uint64_t, uint64_t mask) -> int64_t {
    current.affinity = mask;
    return 0;
}
auto yield() -> uint64_t {
    if (active_worker) active_worker->placed.store(true, std::memory_order_release);
    return 0;
}
auto pipe(int* out) -> int {
    if (next_fd + 2 >= fds.size() || next_pipe >= pipes.size()) std::abort();
    int index = next_pipe++;
    out[0] = next_fd++;
    out[1] = next_fd++;
    fds[out[0]] = {true, index};
    fds[out[1]] = {true, index};
    return 0;
}
auto close(int fd) -> int {
    if (fd < 0 || static_cast<size_t>(fd) >= fds.size() || !fds[fd].open) {
        errno = EBADF;
        return -1;
    }
    fds[fd].open = false;
    return 0;
}
auto fcntl(int fd, int op, int = 0) -> int {
    if (!fds[fd].open) {
        errno = EBADF;
        return -1;
    }
    return op == F_GETFL ? 0 : 0;
}
auto read(int fd, void* data, size_t count) -> ssize_t {
    auto& p = pipes[fds[fd].pipe];
    size_t amount = std::min(count, p.bytes.size() - p.cursor);
    std::memcpy(data, p.bytes.data() + p.cursor, amount);
    p.cursor += amount;
    return amount;
}
auto write(int fd, const void* data, size_t count) -> ssize_t {
    pipes[fds[fd].pipe].bytes.append(static_cast<const char*>(data), count);
    return count;
}
auto poll(pollfd*, nfds_t, int) -> int { return 1; }
auto waitpid(pid_t pid, int* status, int options) -> pid_t {
    if (static_cast<uint64_t>(pid) != pending_child) {
        errno = ECHILD;
        return -1;
    }
    if (options == WNOHANG && child_polls++ == 0) {
        *status = 0x7f7f;
        return 0;
    }  // Actual WNOHANG status is deliberately non-authoritative.
    *status = child_status;
    pending_child = 0;
    return pid;
}
auto kill(pid_t, int) -> int {
    child_status = 9;
    return 0;
}
auto pthread_create(pthread_t*, const pthread_attr_t*, void* (*)(void*), void*) -> int;
auto pthread_join(pthread_t, void**) -> int { return 0; }
auto fork_policy(const PolicyOptions&, const Topology&, Selection, uint32_t, uint64_t, bool, const rlimit&, int, uint64_t) -> pid_t;
[[noreturn]] void exit(int) { std::abort(); }
}  // namespace fixture
namespace ker::vmem {
auto query_numa(const ::abi::QueryRequest& value) -> int64_t { return fixture::query(value); }
auto set_memory_policy(const ::abi::PolicyRequest& value) -> int64_t { return fixture::set_policy(value); }
}  // namespace ker::vmem
namespace ker::multiproc {
auto currentThreadId() -> uint64_t { return fixture::tid(); }
auto getcurrent_cpu() -> uint64_t { return fixture::cpu(); }
auto getThreadAffinityMask(uint64_t id) -> int64_t { return fixture::affinity(id); }
auto setThreadAffinityMask(uint64_t id, uint64_t mask) -> int64_t { return fixture::set_affinity(id, mask); }
auto yield() -> uint64_t { return fixture::yield(); }
}  // namespace ker::multiproc
/* @ACTUAL_HELPERS@ */
/* @ACTUAL_POLICY_FUNCTIONS@ */
namespace fixture {
auto pthread_create(pthread_t* thread, const pthread_attr_t*, void* (*entry)(void*), void* argument) -> int {
    Context saved = current;
    current.tid = next_tid++;
    current.sets = 0;
    *thread = current.tid;
    first_thread_query = true;
    active_worker = static_cast<PolicyWorker*>(argument);
    entry(argument);
    active_worker = nullptr;
    current = saved;
    return 0;
}
auto fork_policy(const PolicyOptions& opts, const Topology& topo, Selection selected, uint32_t mode, uint64_t mask, bool quota,
                 const rlimit& bound, int fd, uint64_t deadline) -> pid_t {
    Context saved = current;
    current.pid = current.tid = next_pid++;
    current.sets = 0;
    pending_child = current.pid;
    first_child_query = true;
    int status = policy_child(opts, topo, selected, mode, mask, quota, bound, fd, deadline);
    child_status = fault == "child-status" ? 256 : status * 256;
    child_polls = 0;
    current = saved;
    return pending_child;
}
auto run(uint32_t nodes, const char* failure = "") -> int {
    reset(nodes, failure);
    char name0[] = "--nonce", value0[] = "19601", name1[] = "--domains", value1[] = "2", name2[] = "--pages", value2[] = "4";
    if (nodes == 1) value1[0] = '1';
    char* argv[] = {name0, value0, name1, value1, name2, value2};
    return run_numa_policy(6, argv);
}
}  // namespace fixture
int main() {
    using namespace fixture;
    ++cases;
    check(run(2) == 0, "healthy NUMA actual guest mode");
    check(mappings.empty(), "all actual NUMA mappings removed");
    check(std::none_of(fds.begin(), fds.end(), [](auto& fd) { return fd.open; }), "all actual NUMA descriptors closed");
    check(current.mode == 0 && current.mask == 0 && limits.rlim_cur == RLIM_INFINITY, "parent policy and limit restored");
    std::string numa = output;
    ++cases;
    check(run(1) == 0, "healthy UMA actual guest mode");
    check(mappings.empty(), "all actual UMA mappings removed");
    std::string uma = output;
    for (auto name : {"placement", "nonvirgin", "thread-inheritance", "child-inheritance", "unmap", "no-denial", "child-status",
                      "unchanged-actor-policy"}) {
        ++cases;
        check(run(2, name) != 0, name);
    }
    PolicyOptions options;
    char a[] = "--nonce", b[] = "1", c[] = "--domains", d[] = "2", e[] = "--nonce", f[] = "2";
    char* duplicate[] = {a, b, c, d, e, f};
    ++cases;
    check(!policy_options(6, duplicate, options), "duplicate bounded option refused");
    reset(2);
    if (failures == 0) {
        std::printf("SAVED_POLICY_BEGIN\n%sSAVED_POLICY_END\nSAVED_UMA_BEGIN\n%sSAVED_UMA_END\n", numa.c_str(), uma.c_str());
    }
    std::printf("POLICY_FIXTURE cases=%u assertions=%u failures=%u\n", cases, assertions, failures);
    return failures ? 1 : 0;
}
