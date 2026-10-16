// Actual guest protocol on finite raw ABI/clock/mapped-byte/accounting/file ports.
// No PMM/buddy/physical capacity, WOS task, native syscall or runtime is modeled.
#include <abi/callnums/process.h>
#include <abi/callnums/vmem.h>
#include <abi/numa.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include <abi/resource.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>
/* @ACTUAL_CALLNUMS@ */
namespace abi = ker::abi::vmem::numa;
constexpr size_t PAGE = 4096;
constexpr uint64_t NS_PER_MS = 1000000;
/* @ACTUAL_PRESSURE_TYPES@ */
namespace fixture {
struct Mapping {
    void* pointer;
    uint64_t bytes;
    uint16_t node;
    bool alive;
};
std::vector<Mapping> mappings;
uint64_t ticks{}, affinity = 3, mode{}, mask{}, peak = 64ULL << 20;
unsigned allocations{}, unmaps{}, failures{}, checks{}, next_fd = 3;
uint32_t summary_cpu_count = 2;
std::array<size_t, 16> cursors{};
std::array<bool, 16> descriptors{};
std::string fault, output;
void check(bool good, const char* why) {
    ++checks;
    if (!good) {
        ++failures;
        std::printf("FAIL: %s\n", why);
    }
}
void reset(const char* issue = "") {
    for (auto& m : mappings) std::free(m.pointer);
    mappings.clear();
    ticks = 0;
    affinity = 3;
    mode = mask = 0;
    peak = 64ULL << 20;
    allocations = unmaps = 0;
    summary_cpu_count = 2;
    next_fd = 3;
    descriptors = {};
    cursors = {};
    fault = issue;
    output.clear();
}
auto bytes_live() -> uint64_t {
    uint64_t out = 0;
    for (auto const& m : mappings)
        if (m.alive) out += m.bytes;
    return out;
}
auto clock_gettime(clockid_t, timespec* ts) -> int {
    ticks += 100000;
    ts->tv_sec = ticks / 1000000000;
    ts->tv_nsec = ticks % 1000000000;
    return 0;
}
auto getpid() -> pid_t { return 101; }
auto currentThreadId() -> uint64_t { return 101; }
auto getcurrent_cpu() -> uint64_t { return 0; }
auto getThreadAffinityMask(uint64_t) -> uint64_t { return affinity; }
auto setThreadAffinityMask(uint64_t, uint64_t value) -> int64_t {
    affinity = value;
    return 0;
}
auto yield() -> uint64_t { return 0; }
auto getrlimit(int, rlimit* out) -> int {
    *out = {RLIM_INFINITY, RLIM_INFINITY};
    return 0;
}
auto open(const char*, int) -> int {
    int fd = next_fd++;
    descriptors[fd] = true;
    return fd;
}
auto close(int fd) -> int {
    if (fd < 0 || !descriptors[fd]) {
        errno = EBADF;
        return -1;
    }
    descriptors[fd] = false;
    return 0;
}
auto read(int fd, void* output, size_t count) -> ssize_t {
    // Only full framing/parsing tests may qualify snapshot grammar. This explicit
    // metadata port supplies finite raw bytes, not kernel physical observations.
    static constexpr char SUMMARY[] = "summary schema=2\n";
    static constexpr char ZONES[] = "zone id=0\n";
    auto* text = (fd % 2) ? SUMMARY : ZONES;
    size_t n = std::strlen(text);
    if (fault == "snapshot-read") {
        errno = EIO;
        return -1;
    }
    size_t at = cursors[fd], got = std::min(count, n - at);
    std::memcpy(output, text + at, got);
    cursors[fd] += got;
    return got;
}
auto write(int, const void* bytes, size_t count) -> ssize_t {
    output.append(static_cast<const char*>(bytes), count);
    return count;
}
auto resource_call(ker::abi::callnums, uint64_t, uint64_t, uint64_t address, uint64_t) -> uint64_t {
    auto* out = reinterpret_cast<ker::abi::resource::Usage*>(address);
    *out = {};
    out->current = (64ULL << 20) + bytes_live();
    if (out->current > peak) peak = out->current;
    out->peak = peak;
    if (fault == "quota" && allocations > 2) out->denied = 1;
    if (fault == "usage" && allocations > 2) ++out->current;
    return 0;
}
template <class... Args>
auto vmem_call(ker::abi::callnums, uint64_t op, Args... words) -> uint64_t {
    std::array<uint64_t, sizeof...(Args)> a{static_cast<uint64_t>(words)...};
    using Op = ker::abi::vmem::ops;
    if (op == static_cast<uint64_t>(Op::SET_MEMORY_POLICY)) {
        auto* request = reinterpret_cast<abi::PolicyRequest*>(a[0]);
        mode = request->mode;
        mask = request->nodes;
        return 0;
    }
    if (op == static_cast<uint64_t>(Op::QUERY_NUMA)) {
        auto const& r = *reinterpret_cast<abi::QueryRequest*>(a[0]);
        auto* dst = reinterpret_cast<void*>(r.output_address);
        if (r.kind == static_cast<uint32_t>(abi::QueryKind::SUMMARY)) {
            auto& s = *static_cast<abi::Summary*>(dst);
            s = {};
            s.version = abi::VERSION;
            s.bytes = sizeof(s);
            s.features = abi::FEATURES_V1;
            s.node_count = 2;
            s.cpu_count = summary_cpu_count;
            s.page_bytes = PAGE;
            s.max_page_nodes = abi::MAX_PAGE_NODES;
            s.cpu_mask_words = 4;
            return 0;
        }
        if (r.kind == static_cast<uint32_t>(abi::QueryKind::NODE)) {
            auto& n = *static_cast<abi::Node*>(dst);
            n = {};
            n.version = abi::VERSION;
            n.bytes = sizeof(n);
            n.node = r.index;
            n.managed_bytes = 4ULL << 30;
            n.cpu_words[0] = 1ULL << r.index;
            return 0;
        }
        if (r.kind == static_cast<uint32_t>(abi::QueryKind::SELF_POLICY)) {
            auto& s = *static_cast<abi::SelfPolicy*>(dst);
            s = {};
            s.version = abi::VERSION;
            s.bytes = sizeof(s);
            s.mode = mode;
            s.nodes = mask;
            return 0;
        }
        if (r.kind == static_cast<uint32_t>(abi::QueryKind::PAGE_NODES)) {
            auto* rows = static_cast<abi::PageNode*>(dst);
            for (unsigned i = 0; i < r.count; ++i) {
                rows[i] = {};
                rows[i].node = abi::INVALID_NODE;
                rows[i].status = static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE);
                auto va = r.address + uint64_t{i} * PAGE;
                for (auto const& m : mappings)
                    if (m.alive && va >= reinterpret_cast<uint64_t>(m.pointer) && va - reinterpret_cast<uint64_t>(m.pointer) < m.bytes) {
                        rows[i].node = m.node;
                        rows[i].status = static_cast<uint16_t>(abi::PageStatus::RESIDENT);
                        break;
                    }
            }
            return 0;
        }
        return uint64_t(-EINVAL);
    }
    if (op == static_cast<uint64_t>(Op::ANON_ALLOCATE)) {
        ++allocations;
        uint64_t size = a[1];
        check(a[2] == PROT_READ && a[3] == (MAP_PRIVATE | MAP_ANONYMOUS), "actual eager raw protection/flags");
        uint64_t live = bytes_live();
        if (fault == "metadata" || (mode == 2 && live + size > (2ULL << 20) + 2 * PAGE)) return uint64_t(-ENOMEM);
        if (fault == "recovery" && mode == 2 && unmaps) return uint64_t(-ENOMEM);
        auto* p = std::aligned_alloc(PAGE, size);
        if (!p) return uint64_t(-ENOMEM);
        std::memset(p, 0, size);
        uint16_t node = mode == 1 ? 0 : 1;
        if (fault == "preferred-selected" && mode == 1) node = 1;
        if (fault == "bind-foreign" && mode == 2) node = 0;
        if (fault == "nonzero") static_cast<unsigned char*>(p)[size - 1] = 1;
        mappings.push_back({p, size, node, true});
        peak = std::max(peak, (uint64_t{64} << 20) + bytes_live());
        if (fault == "deadline") ticks = 300000000001ULL;
        return reinterpret_cast<uint64_t>(p);
    }
    if (op == static_cast<uint64_t>(Op::ANON_FREE)) {
        for (auto& m : mappings)
            if (m.alive && reinterpret_cast<uint64_t>(m.pointer) == a[0] && m.bytes == a[1]) {
                if (fault == "cleanup") return uint64_t(-EIO);
                m.alive = false;
                ++unmaps;
                return 0;
            }
        return uint64_t(-EINVAL);
    }
    return uint64_t(-EINVAL);
}
}  // namespace fixture
namespace ker::vmem {
/* @ACTUAL_RAW_WRAPPERS@ */
}
/* @ACTUAL_PRESSURE_HELPERS@ */
/* @ACTUAL_PRESSURE_FUNCTIONS@ */
int main() {
    const char* bad[]{"preferred-selected", "bind-foreign", "recovery",      "quota",   "usage",
                      "metadata",           "nonzero",      "snapshot-read", "cleanup", "deadline"};
    char nonce[] = "--nonce", value[] = "23401", domains[] = "--domains", two[] = "2";
    char* args[]{nonce, value, domains, two};
    fixture::reset();
    int rc = run_numa_pressure(4, args);
    fixture::check(rc == 0, "actual successful logical flow");
    fixture::check(pressure_state.strict && pressure_state.fallback && pressure_state.payload_clean && pressure_state.recovered,
                   "actual phases");
    fixture::check(fixture::bytes_live() == 0 && fixture::affinity == 3 && fixture::mode == 0, "owned mapping/policy/affinity restoration");
    fixture::check(pressure_state.checkpoints[6].owned_bytes == PAGE && pressure_state.checkpoints[7].owned_bytes == 0,
                   "same-BIND recovery charge and cleanup");
    for (auto issue : bad) {
        fixture::reset(issue);
        rc = run_numa_pressure(4, args);
        fixture::check(rc != 0, "actual negative must refuse");
        if (std::strcmp(issue, "cleanup")) fixture::check(fixture::bytes_live() == 0, "all failure-owned prefix cleanup");
        fixture::check(std::none_of(fixture::descriptors.begin(), fixture::descriptors.end(), [](bool b) { return b; }),
                       "all independent FDs closed");
    }
    for (uint32_t cpus : {0U, 65U, 256U}) {
        fixture::reset();
        fixture::summary_cpu_count = cpus;
        rc = run_numa_pressure(4, args);
        fixture::check(rc != 0 && pressure_state.first_error == -ENOTSUP, "refused SUMMARY cpu count preserves ENOTSUP");
        fixture::check(fixture::allocations == 0 && fixture::next_fd == 3 && fixture::bytes_live() == 0 && fixture::affinity == 3 &&
                           fixture::mode == 0,
                       "refused topology has no acquired pressure resources");
    }
    fixture::reset();
    PressureOptions options;
    fixture::check(!pressure_options(2, args, options), "missing required domains");
    PressureState& s = pressure_state;
    initialize_transport(s);
    s.used = PRESSURE_MAX_ENTRIES;
    s.deadline = UINT64_MAX;
    int64_t raw{}, sentinel{};
    fixture::check(pressure_allocate(s, PAGE, UINT64_MAX, false, raw, sentinel) == -E2BIG && fixture::allocations == 0,
                   "cap refuses before allocation");
    s.deadline = 1;
    fixture::check(!pressure_remaining(s, 1), "deadline refuses before allocation");
    void* output = reinterpret_cast<void*>(uintptr_t{0x101});
    fixture::fault = "metadata";
    fixture::check(ker::vmem::allocate(&output, PAGE, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS) == -ENOMEM &&
                       output == reinterpret_cast<void*>(uintptr_t{0x101}),
                   "actual raw wrapper preserves output sentinel");
    fixture::reset();
    std::printf("pressure actual-body cases checks=%u failures=%u\n", fixture::checks, fixture::failures);
    return fixture::failures ? 1 : 0;
}
