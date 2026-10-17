#include "numa_locality.hpp"

#include <abi/callnums/process.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/numa.h>
#include <sys/resource.h>
#include <sys/resource_policy.h>
#include <sys/vmem.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <type_traits>

namespace {
namespace abi = ker::abi::vmem::numa;
constexpr size_t PAGE = 4096;
constexpr uint64_t NS_PER_MS = 1000000;
constexpr uint64_t RECORD_MAGIC = 0x574f534e554d4131ULL;
constexpr size_t CROSS_START = PAGE - 128;
constexpr size_t CROSS_BYTES = 256;
constexpr size_t CYCLE_BYTES = 3 * PAGE;
constexpr uint64_t DEFAULT_SEED = 0x402102ULL;

struct Options {
    uint32_t pages = 1024;
    uint32_t domains = 0;
    uint32_t cycles = 1000;
    uint32_t timeout_ms = 30000;
    uint32_t child_timeout_ms = 5000;
    uint64_t seed = DEFAULT_SEED;
};
struct Selection {
    uint16_t node;
    uint16_t cpu;
};
struct Topology {
    abi::Summary summary{};
    std::array<abi::Node, 64> nodes{};
    std::array<Selection, 2> selected{};
    uint32_t selected_count = 0;
};
struct QueryBuffers {
    abi::QueryRequest request{};
    std::array<abi::PageNode, abi::MAX_PAGE_NODES> pages{};
};

// These POD transport bytes are separate from measured mappings. Explicit
// volatile initialization prevents their first fault occurring in copyout.
template <class T>
void initialize_transport(T& object) {
    static_assert(std::is_trivially_copyable_v<T>);
    auto* bytes = reinterpret_cast<volatile unsigned char*>(&object);
    for (size_t i = 0; i < sizeof(T); ++i) bytes[i] = 0;
}

enum ClockValid : uint32_t { CLOCK_RESULT = 1, CLOCK_ERRNO = 2, CLOCK_TIMESPEC = 4, CLOCK_NS = 8 };
struct ClockObservation {
    uint32_t valid{};
    int result{}, error{};
    int64_t seconds{}, nanoseconds{};
    uint64_t now{};
};
enum class WaitOutcome : uint32_t { NOT_CALLED, ZERO_EXIT, NONZERO_STATUS, WAIT_ERROR, DEADLINE, CLOCK_ERROR };
struct WaitObservation {
    ClockObservation clock{};
    uint64_t deadline{}, polls{};
    int64_t result{};
    int error{}, status{};
    WaitOutcome outcome{};
    bool result_valid{}, error_valid{}, status_valid{};
};

enum class ReadyStage : uint32_t { NOT_CALLED, TRANSFER, VALIDATION };
enum class ReadyReason : uint32_t {
    NOT_CALLED,
    COMPLETE,
    CLOCK_ERROR,
    DEADLINE,
    COUNT_ERROR,
    EOF_STREAM,
    IO_ERROR,
    POLL_ERROR,
    POLL_REVENTS,
    VALIDATION
};
enum ReadyValid : uint32_t {
    READY_IO_RESULT = 1,
    READY_IO_ERRNO = 2,
    READY_POLL_RESULT = 4,
    READY_POLL_ERRNO = 8,
    READY_POLL_REVENTS = 16,
    READY_FULL_RECORD = 32,
    READY_RECORD_ERROR = 64,
    READY_VALIDATION_REJECT = 128
};
struct ReadyObservation {
    ReadyStage stage{};
    ReadyReason reason{};
    uint32_t valid{};
    uint64_t requested{}, done{}, deadline{};
    ClockObservation clock{};
    int64_t io_result{};
    int io_error{}, poll_result{}, poll_error{}, poll_revents{};
    int64_t ready_error{};
    uint64_t validation_reject{};
};
static_assert(std::is_trivially_copyable_v<ReadyObservation> && sizeof(ReadyObservation) <= 160);

// Failure diagnostics retain the last existing cleanup call and retry counts.
// Status is read only after consuming this exact child; no wait history allocates.
struct CleanupObservation {
    uint64_t waits{}, interrupted_waits{};
    int64_t kill_result{}, wait_result{};
    int kill_error{}, wait_error{}, status{};
    bool kill_result_valid{}, kill_error_valid{}, wait_result_valid{}, wait_error_valid{}, status_valid{};
};

auto now_ns(uint64_t& out, ClockObservation* observation = nullptr) -> bool {
    timespec ts{};
    int const RESULT = clock_gettime(CLOCK_MONOTONIC, &ts);
    if (observation != nullptr) {
        *observation = {.valid = CLOCK_RESULT, .result = RESULT};
        if (RESULT < 0) {
            observation->error = errno;  // Capture before cleanup can replace it.
            observation->valid |= CLOCK_ERRNO;
        } else if (RESULT == 0) {
            observation->seconds = ts.tv_sec;
            observation->nanoseconds = ts.tv_nsec;
            observation->valid |= CLOCK_TIMESPEC;
        }
    }
    if (RESULT != 0 || ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= 1000000000L ||
        static_cast<uint64_t>(ts.tv_sec) > (UINT64_MAX - static_cast<uint64_t>(ts.tv_nsec)) / 1000000000ULL)
        return false;
    out = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    if (observation != nullptr) {
        observation->now = out;
        observation->valid |= CLOCK_NS;
    }
    return true;
}
auto deadline_after(uint32_t ms, uint64_t& out) -> bool {
    uint64_t now;
    if (!now_ns(now) || now > UINT64_MAX - static_cast<uint64_t>(ms) * NS_PER_MS) return false;
    out = now + static_cast<uint64_t>(ms) * NS_PER_MS;
    return true;
}
auto before(uint64_t deadline, ClockObservation* observation = nullptr) -> bool {
    uint64_t now;
    return now_ns(now, observation) && now < deadline;
}
auto raw_error(uint64_t value) -> bool { return value >= UINT64_MAX - 4094; }

auto number(const char* text, uint64_t& value) -> bool {
    if (!text || *text < '0' || *text > '9') return false;
    errno = 0;
    char* end = nullptr;
    auto result = std::strtoull(text, &end, 0);
    if (errno || end == text || *end) return false;
    value = result;
    return true;
}
auto options(int argc, char** argv, bool lifecycle, Options& out) -> bool {
    if (lifecycle) out.timeout_ms = 300000;
    for (int i = 0; i < argc; ++i) {
        if (i + 1 >= argc) return false;
        auto const* name = argv[i++];
        uint64_t value;
        if (!number(argv[i], value)) return false;
        if (std::strcmp(name, "--seed") == 0)
            out.seed = value;
        else if (!lifecycle && std::strcmp(name, "--pages") == 0 && value >= 1 && value <= 16384)
            out.pages = static_cast<uint32_t>(value);
        else if (!lifecycle && std::strcmp(name, "--domains") == 0 && value >= 1 && value <= 2)
            out.domains = static_cast<uint32_t>(value);
        else if (lifecycle && std::strcmp(name, "--cycles") == 0 && value >= 1 && value <= 1000)
            out.cycles = static_cast<uint32_t>(value);
        else if (lifecycle && std::strcmp(name, "--child-timeout-ms") == 0 && value >= 100 && value <= 30000)
            out.child_timeout_ms = static_cast<uint32_t>(value);
        else if (std::strcmp(name, "--timeout-ms") == 0 && value >= 1000 && value <= (lifecycle ? 600000U : 120000U))
            out.timeout_ms = static_cast<uint32_t>(value);
        else
            return false;
    }
    return true;
}

auto query(QueryBuffers& io, abi::QueryKind kind, void* output, uint32_t bytes, uint64_t index = 0, uint64_t address = 0,
           uint32_t count = 0) -> int64_t {
    initialize_transport(io.request);
    io.request.version = abi::VERSION;
    io.request.bytes = sizeof(io.request);
    io.request.kind = static_cast<uint32_t>(kind);
    io.request.index = index;
    io.request.address = address;
    io.request.count = count;
    io.request.output_address = reinterpret_cast<uint64_t>(output);
    io.request.output_bytes = bytes;
    return ker::vmem::query_numa(io.request);
}
auto self_policy(QueryBuffers& io, abi::SelfPolicy& out) -> int64_t {
    initialize_transport(out);
    auto const rc = query(io, abi::QueryKind::SELF_POLICY, &out, sizeof(out));
    if (rc != 0) return rc;
    return out.version == abi::VERSION && out.bytes == sizeof(out) && out.mode == static_cast<uint32_t>(abi::PolicyMode::LOCAL) &&
                   out.nodes == 0 && out.reserved32 == 0 && out.reserved64 == 0
               ? 0
               : -EPROTO;
}
auto local_policy(QueryBuffers& io) -> int64_t {
    abi::PolicyRequest request;
    initialize_transport(request);
    request.version = abi::VERSION;
    request.bytes = sizeof(request);
    request.mode = static_cast<uint32_t>(abi::PolicyMode::LOCAL);
    auto const rc = ker::vmem::set_memory_policy(request);
    if (rc != 0) return rc;
    abi::SelfPolicy actual;
    return self_policy(io, actual);
}
auto page_nodes(QueryBuffers& io, const void* address, uint32_t count) -> int64_t {
    initialize_transport(io.pages);
    for (auto& page : io.pages) {
        page.node = abi::INVALID_NODE;
        page.status = static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE);
    }
    return query(io, abi::QueryKind::PAGE_NODES, io.pages.data(), count * sizeof(abi::PageNode), 0, reinterpret_cast<uint64_t>(address),
                 count);
}
auto source_page_nodes(QueryBuffers& io, const void* mapping, const Topology& topo, Selection parent, uint16_t (&observed)[2]) -> int64_t {
    auto const rc = page_nodes(io, mapping, 2);
    if (rc != 0) return rc;
    for (uint32_t i = 0; i < 2; ++i) {
        auto const& row = io.pages[i];
        if (row.status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) || row.reserved32 || row.node >= topo.summary.node_count ||
            row.node != parent.node)
            return -EPROTO;
    }
    observed[0] = io.pages[0].node;
    observed[1] = io.pages[1].node;
    return 0;
}
auto topology(Topology& out, uint32_t required) -> int64_t {
    QueryBuffers io;
    initialize_transport(io);
    initialize_transport(out.summary);
    auto rc = query(io, abi::QueryKind::SUMMARY, &out.summary, sizeof(out.summary));
    if (rc != 0) return rc;
    auto const& s = out.summary;
    if (s.version != abi::VERSION || s.bytes != sizeof(s) || s.features != abi::FEATURES_V1 || s.node_count == 0 || s.node_count > 64 ||
        s.cpu_count == 0 || s.cpu_count > 256 || s.page_bytes != PAGE || s.max_page_nodes != abi::MAX_PAGE_NODES ||
        s.cpu_mask_words != abi::CPU_MASK_WORDS || s.provenance > static_cast<uint32_t>(abi::Provenance::UMA_REJECTED_FIRMWARE) ||
        s.reserved64[0] || s.reserved64[1] || s.reserved64[2])
        return -EPROTO;
    uint64_t seen[4]{};
    for (uint32_t n = 0; n < s.node_count; ++n) {
        auto& node = out.nodes[n];
        initialize_transport(node);
        node.node = abi::INVALID_NODE;
        rc = query(io, abi::QueryKind::NODE, &node, sizeof(node), n);
        if (rc != 0) return rc;
        if (node.version != abi::VERSION || node.bytes != sizeof(node) || node.node != n || node.reserved16 || node.reserved64 ||
            (node.managed_bytes & (PAGE - 1)))
            return -EPROTO;
        uint32_t cpu = 256;
        for (uint32_t word = 0; word < 4; ++word) {
            if (seen[word] & node.cpu_words[word]) return -EPROTO;
            seen[word] |= node.cpu_words[word];
            for (uint32_t bit = 0; bit < 64; ++bit)
                if (node.cpu_words[word] & (uint64_t{1} << bit)) {
                    uint32_t id = word * 64 + bit;
                    if (id >= s.cpu_count) return -EPROTO;
                    if (id < cpu) cpu = id;
                }
        }
        bool const eligible = node.managed_bytes != 0 && cpu < 64;
        const char* reason = eligible                  ? "eligible"
                             : node.managed_bytes == 0 ? "no_ram"
                             : cpu == 256              ? "memory_only"
                                                       : "cpu_above_affinity64";
        std::printf("NUMA_TOPOLOGY node=%u managed=%llu cpus=%016llx:%016llx:%016llx:%016llx eligible=%u reason=%s\n", n,
                    static_cast<unsigned long long>(node.managed_bytes), static_cast<unsigned long long>(node.cpu_words[0]),
                    static_cast<unsigned long long>(node.cpu_words[1]), static_cast<unsigned long long>(node.cpu_words[2]),
                    static_cast<unsigned long long>(node.cpu_words[3]), static_cast<unsigned>(eligible), reason);
        if (eligible && out.selected_count < 2) out.selected[out.selected_count++] = {static_cast<uint16_t>(n), static_cast<uint16_t>(cpu)};
    }
    for (uint32_t cpu = 0; cpu < s.cpu_count; ++cpu)
        if ((seen[cpu / 64] & (uint64_t{1} << (cpu % 64))) == 0) return -EPROTO;
    if (out.selected_count == 0 || (required && out.selected_count < required)) return -ENOTSUP;
    if (required) out.selected_count = required;
    std::printf("NUMA_PROFILE version=1 provenance=%u nodes=%u cpus=%u workers=%u free_capacity=not_observed policy=LOCAL\n", s.provenance,
                s.node_count, s.cpu_count, out.selected_count);
    return 0;
}
// Cross-CPU HOST_POLICY cannot move its currently reserved subject. Only the
// controller places another cooperatively runnable TID; EBUSY is transient.
auto set_thread_cpu(uint64_t tid, uint16_t cpu, uint64_t deadline) -> int64_t {
    if (tid == 0 || raw_error(tid) || cpu >= 64) return -EINVAL;
    while (before(deadline)) {
        auto const rc = ker::multiproc::setThreadAffinityMask(tid, uint64_t{1} << cpu);
        if (rc != -EBUSY) return rc;
        if (raw_error(ker::multiproc::yield())) return -EIO;
    }
    return -ETIMEDOUT;
}
// Pthreads cannot proceed past placed until this exact getter completes.
// Fork children instead prove their placement in their own record: they can
// legitimately exit after SET commits but before a parent-side GET returns.
auto place_thread(uint64_t tid, uint16_t cpu, uint64_t deadline) -> int64_t {
    if (tid == 0 || raw_error(tid) || cpu >= 64) return -EINVAL;
    uint64_t const mask = uint64_t{1} << cpu;
    while (before(deadline)) {
        auto const rc = set_thread_cpu(tid, cpu, deadline);
        if (rc != 0) return rc;
        auto const actual = static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(tid));
        // Compare bits before interpreting errno so CPU63 remains valid.
        if (actual == mask) return 0;
        if (actual != static_cast<uint64_t>(-EBUSY)) return raw_error(actual) ? static_cast<int64_t>(actual) : -EPROTO;
        if (raw_error(ker::multiproc::yield())) return -EIO;
    }
    return -ETIMEDOUT;
}
auto await_cpu(uint16_t cpu, uint64_t deadline, const std::atomic<bool>* cancel = nullptr, uint64_t pending_online_mask = 0) -> int64_t {
    auto const tid = ker::multiproc::currentThreadId();
    if (tid == 0 || raw_error(tid) || cpu >= 64) return -EINVAL;
    uint64_t const mask = uint64_t{1} << cpu;
    while (before(deadline)) {
        if (cancel != nullptr && cancel->load(std::memory_order_acquire)) return -ECANCELED;
        auto const actual = static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(tid));
        if (actual == mask) {
            auto const current = ker::multiproc::getcurrent_cpu();
            if (current == cpu) return 0;
            if (raw_error(current)) return -EIO;
        } else if (pending_online_mask != 0 && actual == pending_online_mask) {
            // Verified unrestricted ONLINE may look like -1 for >=64 CPUs.
            // It is only pending; exact target mask+CPU above grants success.
        } else if (raw_error(actual) && actual != static_cast<uint64_t>(-EBUSY)) {
            return static_cast<int64_t>(actual);
        }
        if (raw_error(ker::multiproc::yield())) return -EIO;
    }
    return -ETIMEDOUT;
}
auto pin_current_cpu(const Topology& topo, Selection& selected, uint64_t deadline) -> int64_t {
    auto const tid = ker::multiproc::currentThreadId();
    if (tid == 0 || raw_error(tid)) return -EINVAL;
    while (before(deadline)) {
        auto const cpu = ker::multiproc::getcurrent_cpu();
        if (raw_error(cpu)) return -EIO;
        if (cpu >= 64 || cpu >= topo.summary.cpu_count) return -ENOTSUP;
        auto const rc = ker::multiproc::setThreadAffinityMask(tid, uint64_t{1} << cpu);
        if (rc == -EBUSY) {
            if (raw_error(ker::multiproc::yield())) return -EIO;
            continue;  // Recapture actual CPU; never self-request a remote move.
        }
        if (rc != 0) return rc;
        auto const observed = await_cpu(static_cast<uint16_t>(cpu), deadline);
        if (observed != 0) return observed;
        for (uint16_t node = 0; node < topo.summary.node_count; ++node)
            if ((topo.nodes[node].cpu_words[cpu / 64] & (uint64_t{1} << (cpu % 64))) != 0) {
                if (topo.nodes[node].managed_bytes == 0) return -ENOTSUP;
                selected = {node, static_cast<uint16_t>(cpu)};
                return 0;
            }
        return -EPROTO;
    }
    return -ETIMEDOUT;
}
auto valid_selection(const Topology& topo, Selection selected) -> bool {
    return selected.node < topo.summary.node_count && selected.cpu < 64 && selected.cpu < topo.summary.cpu_count &&
           topo.nodes[selected.node].managed_bytes != 0 && (topo.nodes[selected.node].cpu_words[0] & (uint64_t{1} << selected.cpu)) != 0;
}
auto lifecycle_coverage(const Topology& topo, uint32_t completed, uint32_t same_node, uint32_t cross_node, uint32_t cross_node_cow,
                        uint64_t local_pages, uint64_t resident_pages) -> bool {
    return static_cast<uint64_t>(same_node) + cross_node == completed && cross_node_cow <= cross_node &&
           resident_pages == static_cast<uint64_t>(completed) * 2 && resident_pages != 0 && local_pages <= resident_pages &&
           local_pages * 100 >= resident_pages * 95 && (topo.selected_count < 2 || (cross_node != 0 && cross_node_cow != 0)) &&
           (topo.summary.node_count != 1 || cross_node == 0);
}
auto cpu_still(uint16_t cpu) -> bool {
    auto const tid = ker::multiproc::currentThreadId();
    return ker::multiproc::getcurrent_cpu() == cpu &&
           static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(tid)) == (uint64_t{1} << cpu);
}
auto map_private(size_t bytes) -> void* {
    return mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
}

struct FirstWorker {
    const Options* opts = nullptr;
    Selection selection{};
    uint32_t node_count = 0;
    std::atomic<bool>* go = nullptr;
    std::atomic<bool>* cancel = nullptr;
    std::atomic<uint64_t> tid{0};
    std::atomic<bool> started{false};
    std::atomic<bool> placed{false};
    std::atomic<bool> ready{false};
    std::atomic<bool> done{false};
    int64_t error = 0;
    const char* phase = "start";
    uint64_t histogram[64]{};
    uint64_t elapsed_ns = 0;
    uint64_t cpu_before = UINT64_MAX;
    uint64_t cpu_after = UINT64_MAX;
    bool unmapped = true;
};
auto first_body(FirstWorker& w, QueryBuffers& io, void*& mapping, size_t bytes, uint64_t deadline) -> int64_t {
    w.phase = "await_controller_placement";
    while (!w.placed.load(std::memory_order_acquire)) {
        if (w.cancel->load(std::memory_order_acquire)) return -ECANCELED;
        if (!before(deadline)) return -ETIMEDOUT;
        if (raw_error(ker::multiproc::yield())) return -EIO;
    }
    w.phase = "observe_placement";
    auto rc = await_cpu(w.selection.cpu, deadline, w.cancel);
    if (rc != 0) return rc;
    w.phase = "local_policy";
    rc = local_policy(io);
    if (rc != 0) return rc;
    w.phase = "virgin_map";
    mapping = map_private(bytes);
    if (mapping == MAP_FAILED) {
        mapping = nullptr;
        return -errno;
    }
    if ((reinterpret_cast<uint64_t>(mapping) & (PAGE - 1)) != 0) return -EPROTO;
    w.phase = "virgin_query";
    for (uint32_t first = 0; first < w.opts->pages; first += abi::MAX_PAGE_NODES) {
        uint32_t count = w.opts->pages - first;
        if (count > abi::MAX_PAGE_NODES) count = abi::MAX_PAGE_NODES;
        rc = page_nodes(io, static_cast<const uint8_t*>(mapping) + static_cast<size_t>(first) * PAGE, count);
        if (rc != 0) return rc;
        for (uint32_t i = 0; i < count; ++i)
            if (io.pages[i].reserved32 || io.pages[i].node != abi::INVALID_NODE ||
                io.pages[i].status != static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE))
                return -EPROTO;
        if (!before(deadline) || w.cancel->load(std::memory_order_acquire)) return -ETIMEDOUT;
    }
    w.phase = "await_go";
    w.ready.store(true, std::memory_order_release);
    while (!w.go->load(std::memory_order_acquire)) {
        if (!before(deadline) || w.cancel->load(std::memory_order_acquire)) return -ETIMEDOUT;
        (void)ker::multiproc::yield();
    }
    if (w.cancel->load(std::memory_order_acquire)) return -ECANCELED;
    w.cpu_before = ker::multiproc::getcurrent_cpu();
    uint64_t start, end;
    if (w.cpu_before != w.selection.cpu || !now_ns(start)) return -EPROTO;
    w.phase = "virgin_touch";
    auto* payload = static_cast<volatile uint8_t*>(mapping);
    for (uint32_t page = 0; page < w.opts->pages; ++page) {
        payload[static_cast<size_t>(page) * PAGE] = static_cast<uint8_t>(1 + ((w.opts->seed ^ page) & 127));
        if ((page % 64) == 0 && (!before(deadline) || w.cancel->load(std::memory_order_acquire))) return -ETIMEDOUT;
    }
    w.phase = "verify_payload";
    for (uint32_t page = 0; page < w.opts->pages; ++page) {
        size_t base = static_cast<size_t>(page) * PAGE;
        if (payload[base] != static_cast<uint8_t>(1 + ((w.opts->seed ^ page) & 127))) return -EILSEQ;
        for (size_t i = 1; i < PAGE; ++i)
            if (payload[base + i] != 0) return -EILSEQ;
        if ((page % 64) == 0 && (!before(deadline) || w.cancel->load(std::memory_order_acquire))) return -ETIMEDOUT;
    }
    if (!now_ns(end) || end < start) return -EIO;
    w.elapsed_ns = end - start;
    w.cpu_after = ker::multiproc::getcurrent_cpu();
    if (w.cpu_after != w.selection.cpu || !cpu_still(w.selection.cpu)) return -EPROTO;
    w.phase = "resident_query";
    for (uint32_t first = 0; first < w.opts->pages; first += abi::MAX_PAGE_NODES) {
        uint32_t count = w.opts->pages - first;
        if (count > abi::MAX_PAGE_NODES) count = abi::MAX_PAGE_NODES;
        rc = page_nodes(io, static_cast<const uint8_t*>(mapping) + static_cast<size_t>(first) * PAGE, count);
        if (rc != 0) return rc;
        for (uint32_t i = 0; i < count; ++i) {
            auto const& row = io.pages[i];
            if (row.reserved32 || row.status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) || row.node >= w.node_count)
                return -EPROTO;
            ++w.histogram[row.node];
        }
        if (!before(deadline) || w.cancel->load(std::memory_order_acquire)) return -ETIMEDOUT;
    }
    uint64_t total = 0;
    for (uint64_t n : w.histogram) total += n;
    if (total != w.opts->pages || w.histogram[w.selection.node] * 100 < total * 95) return -ERANGE;
    w.phase = "complete";
    return 0;
}
auto first_worker(void* arg) -> void* {
    auto& w = *static_cast<FirstWorker*>(arg);
    QueryBuffers io;
    initialize_transport(io);
    void* mapping = nullptr;
    size_t bytes = static_cast<size_t>(w.opts->pages) * PAGE;
    uint64_t deadline;
    auto const tid = ker::multiproc::currentThreadId();
    w.tid.store(tid, std::memory_order_relaxed);
    w.started.store(true, std::memory_order_release);
    w.error = tid == 0 || raw_error(tid)                     ? -EINVAL
              : deadline_after(w.opts->timeout_ms, deadline) ? first_body(w, io, mapping, bytes, deadline)
                                                             : -EIO;
    if (mapping != nullptr) {
        w.unmapped = munmap(mapping, bytes) == 0;
        if (!w.unmapped) w.error = -errno;
    }
    w.ready.store(true, std::memory_order_release);
    w.done.store(true, std::memory_order_release);
    return nullptr;
}

auto wait_flag(std::atomic<bool>& flag, uint64_t deadline) -> bool {
    while (!flag.load(std::memory_order_acquire)) {
        if (!before(deadline)) return false;
        (void)ker::multiproc::yield();
    }
    return true;
}

auto nonblocking(int fd) -> bool {
    int flags = fcntl(fd, F_GETFL, 0);
    return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}
auto transfer(int fd, void* buffer, size_t bytes, bool writing, uint64_t deadline, ReadyObservation* observation = nullptr) -> bool {
    auto* p = static_cast<uint8_t*>(buffer);
    size_t done = 0;
    if (observation != nullptr) {
        *observation = {};
        observation->stage = ReadyStage::TRANSFER;
        observation->requested = bytes;
        observation->deadline = deadline;
    }
    while (done != bytes) {
        if (!before(deadline, observation != nullptr ? &observation->clock : nullptr)) {
            if (observation != nullptr)
                observation->reason = (observation->clock.valid & CLOCK_NS) != 0 ? ReadyReason::DEADLINE : ReadyReason::CLOCK_ERROR;
            errno = ETIMEDOUT;
            return false;
        }
        ssize_t n = writing ? write(fd, p + done, bytes - done) : read(fd, p + done, bytes - done);
        if (observation != nullptr) {
            observation->io_result = n;
            observation->io_error = n < 0 ? errno : 0;
            observation->valid = (observation->valid & ~READY_IO_ERRNO) | READY_IO_RESULT;
            if (n < 0) observation->valid |= READY_IO_ERRNO;
        }
        if (n > 0) {
            if (static_cast<size_t>(n) > bytes - done) {
                if (observation != nullptr) observation->reason = ReadyReason::COUNT_ERROR;
                errno = EPROTO;
                return false;
            }
            done += static_cast<size_t>(n);
            if (observation != nullptr) observation->done = done;
            continue;
        }
        if (n == 0) {
            if (observation != nullptr) observation->reason = ReadyReason::EOF_STREAM;
            errno = EPIPE;
            return false;
        }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            if (observation != nullptr) observation->reason = ReadyReason::IO_ERROR;
            return false;
        }
        uint64_t now;
        if (!now_ns(now, observation != nullptr ? &observation->clock : nullptr) || now >= deadline) {
            if (observation != nullptr)
                observation->reason = (observation->clock.valid & CLOCK_NS) != 0 ? ReadyReason::DEADLINE : ReadyReason::CLOCK_ERROR;
            errno = ETIMEDOUT;
            return false;
        }
        uint64_t ms = (deadline - now + NS_PER_MS - 1) / NS_PER_MS;
        pollfd poll_fd{fd, static_cast<short>(writing ? POLLOUT : POLLIN), 0};
        int rc = poll(&poll_fd, 1, static_cast<int>(ms > 100 ? 100 : ms));
        if (observation != nullptr) {
            observation->poll_result = rc;
            observation->poll_error = rc < 0 ? errno : 0;
            observation->poll_revents = rc > 0 ? poll_fd.revents : 0;
            observation->valid = (observation->valid & ~(READY_POLL_ERRNO | READY_POLL_REVENTS)) | READY_POLL_RESULT;
            if (rc < 0) observation->valid |= READY_POLL_ERRNO;
            if (rc > 0) observation->valid |= READY_POLL_REVENTS;
        }
        if (rc < 0 && errno != EINTR) {
            if (observation != nullptr) observation->reason = ReadyReason::POLL_ERROR;
            return false;
        }
        // HUP can accompany unread final bytes; retry read before treating EOF.
        if (rc > 0 && (poll_fd.revents & (POLLERR | POLLNVAL))) {
            if (observation != nullptr) observation->reason = ReadyReason::POLL_REVENTS;
            errno = EIO;
            return false;
        }
    }
    if (observation != nullptr) observation->reason = ReadyReason::COMPLETE;
    return true;
}
auto pattern(uint64_t seed, uint32_t cycle, size_t index, bool child) -> uint8_t {
    return static_cast<uint8_t>((seed + static_cast<uint64_t>(cycle) * 131 + index * 17) ^ (child ? 0x5A : 0xA5));
}
auto verify_cycle(const volatile uint8_t* p, uint64_t seed, uint32_t cycle, bool copied_out) -> bool {
    for (size_t i = 0; i < CYCLE_BYTES; ++i) {
        bool child = copied_out && i >= CROSS_START && i < CROSS_START + CROSS_BYTES;
        if (p[i] != pattern(seed, cycle, i, child)) return false;
    }
    return true;
}
struct ChildRecord {
    uint64_t magic;
    uint32_t version;
    uint32_t bytes;
    uint64_t seed;
    uint32_t cycle;
    uint32_t pid;
    uint64_t tid;
    uint64_t cpu_before;
    uint64_t cpu_after;
    uint64_t elapsed_ns;
    int64_t error;
    abi::SelfPolicy policy;
    abi::PageNode pages[2];
    uint16_t selected_node;
    uint16_t selected_cpu;
    uint32_t reserved;
    uint32_t phase;
    uint32_t reserved2;
};
enum class ChildPhase : uint32_t { PIN_CURRENT_CPU = 1, INHERITED_POLICY, ORIGINAL_BYTES, COW, RESIDENT_QUERY, FINAL_CPU, COMPLETE };
static_assert(std::is_trivially_copyable_v<ChildRecord> && sizeof(ChildRecord) == 136);

auto child_cycle(uint8_t* mapping, int fd, const Options& opts, const Topology& topo, uint32_t cycle, uint64_t deadline) -> int {
    ChildRecord record;
    initialize_transport(record);
    QueryBuffers io;
    initialize_transport(io);
    record.magic = RECORD_MAGIC;
    record.version = 1;
    record.bytes = sizeof(record);
    record.seed = opts.seed;
    record.cycle = cycle;
    record.pid = static_cast<uint32_t>(getpid());
    record.tid = ker::multiproc::currentThreadId();
    Selection selected{abi::INVALID_NODE, UINT16_MAX};
    uint64_t start = 0, end = 0;
    record.phase = static_cast<uint32_t>(ChildPhase::PIN_CURRENT_CPU);
    auto rc = pin_current_cpu(topo, selected, deadline);  // Same-owner pin only; never request a remote self move.
    record.selected_cpu = selected.cpu;
    record.selected_node = selected.node;
    record.cpu_before = ker::multiproc::getcurrent_cpu();
    if (rc == 0 && (record.cpu_before != selected.cpu || record.tid != record.pid || raw_error(record.tid) || !now_ns(start))) rc = -EPROTO;
    if (rc == 0) {
        record.phase = static_cast<uint32_t>(ChildPhase::INHERITED_POLICY);
        rc = self_policy(io, record.policy);  // Observe inheritance before payload; child never resets policy.
    }
    if (rc == 0) {
        record.phase = static_cast<uint32_t>(ChildPhase::ORIGINAL_BYTES);
        if (!verify_cycle(mapping, opts.seed, cycle, false)) rc = -EILSEQ;
    }
    if (rc == 0) {
        record.phase = static_cast<uint32_t>(ChildPhase::COW);
        auto* payload = static_cast<volatile uint8_t*>(mapping);
        for (size_t i = CROSS_START; i < CROSS_START + CROSS_BYTES; ++i) payload[i] = pattern(opts.seed, cycle, i, true);
        if (!verify_cycle(payload, opts.seed, cycle, true)) rc = -EILSEQ;
    }
    if (rc == 0) {
        record.phase = static_cast<uint32_t>(ChildPhase::RESIDENT_QUERY);
        rc = page_nodes(io, mapping, 2);
    }
    if (rc == 0)
        for (uint32_t i = 0; i < 2; ++i) {
            record.pages[i] = io.pages[i];
            if (record.pages[i].reserved32 || record.pages[i].status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) ||
                record.pages[i].node >= topo.summary.node_count)
                rc = -EPROTO;
        }
    if (rc == 0) record.phase = static_cast<uint32_t>(ChildPhase::FINAL_CPU);
    record.cpu_after = ker::multiproc::getcurrent_cpu();
    if (rc == 0 && (!cpu_still(selected.cpu) || !now_ns(end) || end < start)) rc = -EPROTO;
    record.elapsed_ns = end >= start ? end - start : 0;
    record.error = rc;
    if (rc == 0) record.phase = static_cast<uint32_t>(ChildPhase::COMPLETE);
    if (!transfer(fd, &record, sizeof(record), true, deadline)) return 60;
    if (rc != 0) return 61;
    // Both source and parent's destination cross the actual4KiB boundary.
    return transfer(fd, mapping + CROSS_START, CROSS_BYTES, true, deadline) ? 0 : 62;
}
// BEGIN lifecycle cooperative placement. The payload body remains separate.
constexpr uint64_t CHILD_READY_MAGIC = 0x574f535245414459ULL;
struct ChildReady {
    uint64_t magic;
    uint32_t version, bytes;
    uint64_t seed;
    uint32_t cycle, pid;
    uint64_t tid, inherited_mask, inherited_cpu;
    abi::SelfPolicy policy;
    uint16_t target_node, target_cpu;
    uint32_t reserved;
    int64_t error;
};
static_assert(std::is_trivially_copyable_v<ChildReady> && sizeof(ChildReady) <= 160);

auto lifecycle_child_target(const Topology& topo, Selection parent, uint32_t cycle, Selection& target) -> bool {
    target = {abi::INVALID_NODE, UINT16_MAX};
    if (topo.selected_count == 0 || topo.selected_count > topo.selected.size() || !valid_selection(topo, parent)) return false;
    if ((cycle & 1U) == 0 || topo.selected_count < 2) {
        target = parent;
        return true;
    }
    for (uint32_t i = 0; i < topo.selected_count; ++i)
        if (topo.selected[i].node != parent.node && valid_selection(topo, topo.selected[i])) {
            target = topo.selected[i];
            return true;
        }
    return false;
}
auto valid_child_ready(const ChildReady& ready, const Options& opts, const Topology& topo, Selection parent, Selection target,
                       uint32_t cycle, pid_t pid) -> bool {
    return pid > 1 && valid_selection(topo, parent) && valid_selection(topo, target) && ready.magic == CHILD_READY_MAGIC &&
           ready.version == 1 && ready.bytes == sizeof(ready) && ready.seed == opts.seed && ready.cycle == cycle &&
           ready.pid == static_cast<uint32_t>(pid) && ready.tid == static_cast<uint64_t>(pid) && ready.tid == ready.pid &&
           !raw_error(ready.tid) && ready.inherited_mask == (uint64_t{1} << parent.cpu) && ready.inherited_cpu == parent.cpu &&
           ready.target_node == target.node && ready.target_cpu == target.cpu && ready.reserved == 0 && ready.error == 0 &&
           ready.policy.version == abi::VERSION && ready.policy.bytes == sizeof(ready.policy) &&
           ready.policy.mode == static_cast<uint32_t>(abi::PolicyMode::LOCAL) && ready.policy.nodes == 0 && ready.policy.reserved32 == 0 &&
           ready.policy.reserved64 == 0;
}
auto child_ready_rejections(const ChildReady& ready, const Options& opts, const Topology& topo, Selection parent, Selection target,
                            uint32_t cycle, pid_t pid) -> uint64_t {
    // Diagnostic only, after complete transfer and an already-failed validator.
    // Guard ranges before any inherited-affinity shift, as the validator does.
    if (pid <= 1) return uint64_t{1} << 0;
    if (!valid_selection(topo, parent)) return uint64_t{1} << 1;
    if (!valid_selection(topo, target)) return uint64_t{1} << 2;
    uint64_t rejected = 0;
    if (ready.magic != CHILD_READY_MAGIC || ready.version != 1 || ready.bytes != sizeof(ChildReady)) rejected |= uint64_t{1} << 3;
    if (ready.seed != opts.seed || ready.cycle != cycle) rejected |= uint64_t{1} << 4;
    uint64_t const EXPECTED_PID = static_cast<uint64_t>(pid);
    if (ready.pid != EXPECTED_PID || ready.tid != EXPECTED_PID || ready.tid != ready.pid || raw_error(ready.tid))
        rejected |= uint64_t{1} << 5;
    uint64_t const INHERITED = uint64_t{1} << parent.cpu;
    if (ready.inherited_mask != INHERITED || ready.inherited_cpu != parent.cpu) rejected |= uint64_t{1} << 6;
    if (ready.target_node != target.node || ready.target_cpu != target.cpu) rejected |= uint64_t{1} << 7;
    if (ready.reserved != 0) rejected |= uint64_t{1} << 8;
    if (ready.error != 0) rejected |= uint64_t{1} << 9;
    if (ready.policy.version != abi::VERSION || ready.policy.bytes != sizeof(abi::SelfPolicy) ||
        ready.policy.mode != static_cast<uint32_t>(abi::PolicyMode::LOCAL) || ready.policy.nodes != 0 || ready.policy.reserved32 != 0 ||
        ready.policy.reserved64 != 0)
        rejected |= uint64_t{1} << 10;
    return rejected;
}

void report_ready_failure(uint32_t cycle, pid_t pid, const ReadyObservation& observation) {
    if (observation.stage == ReadyStage::NOT_CALLED || observation.reason == ReadyReason::COMPLETE) return;
    int const SAVED_ERRNO = errno;
    std::fprintf(
        stderr,
        "NUMA_CHILD_READY_FAILURE version=1 cycle=%u pid=%lld stage=%u reason=%u valid=%u requested=%llu done=%llu deadline=%llu "
        "clock_valid=%u clock_rc=%d clock_errno=%d clock_sec=%lld clock_nsec=%lld clock_ns=%llu "
        "io_rc=%lld io_errno=%d poll_rc=%d poll_errno=%d poll_revents=%d ready_complete=%u ready_error=%lld validation_reject=%llu\n",
        cycle, static_cast<long long>(pid), static_cast<unsigned>(observation.stage), static_cast<unsigned>(observation.reason),
        observation.valid, static_cast<unsigned long long>(observation.requested), static_cast<unsigned long long>(observation.done),
        static_cast<unsigned long long>(observation.deadline), observation.clock.valid, observation.clock.result, observation.clock.error,
        static_cast<long long>(observation.clock.seconds), static_cast<long long>(observation.clock.nanoseconds),
        static_cast<unsigned long long>(observation.clock.now), static_cast<long long>(observation.io_result), observation.io_error,
        observation.poll_result, observation.poll_error, observation.poll_revents,
        static_cast<unsigned>((observation.valid & READY_FULL_RECORD) != 0), static_cast<long long>(observation.ready_error),
        static_cast<unsigned long long>(observation.validation_reject));
    errno = SAVED_ERRNO;
}

auto child_ready_and_cycle(uint8_t* mapping, int fd, const Options& opts, const Topology& topo, Selection parent, uint32_t cycle,
                           uint64_t deadline) -> int {
    ChildReady ready;
    initialize_transport(ready);
    QueryBuffers io;
    initialize_transport(io);
    ready.magic = CHILD_READY_MAGIC;
    ready.version = 1;
    ready.bytes = sizeof(ready);
    ready.seed = opts.seed;
    ready.cycle = cycle;
    ready.pid = static_cast<uint32_t>(getpid());
    ready.tid = ker::multiproc::currentThreadId();
    ready.inherited_mask = ker::multiproc::getThreadAffinityMask(ready.tid);
    ready.inherited_cpu = ker::multiproc::getcurrent_cpu();
    Selection target{};
    int64_t rc = lifecycle_child_target(topo, parent, cycle, target) ? self_policy(io, ready.policy) : -ENOTSUP;
    ready.target_node = target.node;
    ready.target_cpu = target.cpu;
    ready.error = rc;
    if (rc == 0 && !valid_child_ready(ready, opts, topo, parent, target, cycle, static_cast<pid_t>(ready.pid))) ready.error = -EPROTO;
    if (!transfer(fd, &ready, sizeof(ready), true, deadline)) return 65;
    if (ready.error != 0) return 66;
    int64_t const PLACED = await_cpu(target.cpu, deadline);
    if (PLACED != 0) return 67;
    return child_cycle(mapping, fd, opts, topo, cycle, deadline);
}
auto receive_child_ready(int fd, pid_t pid, const Options& opts, const Topology& topo, Selection parent, Selection target, uint32_t cycle,
                         uint64_t deadline, const char*& phase, ReadyObservation* observation = nullptr) -> bool {
    ChildReady ready;
    initialize_transport(ready);
    phase = "child_ready";
    if (!transfer(fd, &ready, sizeof(ready), false, deadline, observation)) return false;
    if (observation != nullptr) {
        observation->valid |= READY_FULL_RECORD | READY_RECORD_ERROR;
        observation->ready_error = ready.error;
    }
    if (!valid_child_ready(ready, opts, topo, parent, target, cycle, pid)) {
        if (observation != nullptr) {
            observation->stage = ReadyStage::VALIDATION;
            observation->reason = ReadyReason::VALIDATION;
            observation->validation_reject = child_ready_rejections(ready, opts, topo, parent, target, cycle, pid);
            observation->valid |= READY_VALIDATION_REJECT;
        }
        return false;
    }
    // An already-target child may exit now: do not invent a raced SET or GET.
    if (ready.inherited_mask == (uint64_t{1} << target.cpu) && ready.inherited_cpu == target.cpu) return true;
    phase = "controller_placement";
    int64_t const PLACE = set_thread_cpu(static_cast<uint64_t>(pid), target.cpu, deadline);
    return PLACE == 0;
}
// END lifecycle cooperative placement.

auto wait_child(pid_t pid, uint64_t deadline, int& status, bool& owned, WaitObservation& observation) -> bool {
    observation.deadline = deadline;
    while (before(deadline, &observation.clock)) {
        pid_t rc = waitpid(pid, &status, WNOHANG);
        int const ERROR = rc < 0 ? errno : 0;
        ++observation.polls;
        observation.result = rc;
        observation.result_valid = true;
        observation.error = ERROR;
        observation.error_valid = rc < 0;
        observation.status_valid = rc == pid;
        observation.status = observation.status_valid ? status : 0;
        if (rc == pid) {
            owned = false;
            bool const SUCCESS = WIFEXITED(status) && WEXITSTATUS(status) == 0;
            observation.outcome = SUCCESS ? WaitOutcome::ZERO_EXIT : WaitOutcome::NONZERO_STATUS;
            return SUCCESS;
        }
        if (rc < 0 && ERROR != EINTR) {
            observation.outcome = WaitOutcome::WAIT_ERROR;
            return false;
        }
        (void)ker::multiproc::yield();
    }
    observation.outcome = (observation.clock.valid & CLOCK_NS) != 0 ? WaitOutcome::DEADLINE : WaitOutcome::CLOCK_ERROR;
    errno = ETIMEDOUT;
    return false;
}
auto terminate_and_reap(pid_t pid, bool& owned, CleanupObservation* observation = nullptr) -> bool {
    if (!owned) return true;
    int const KILL_RESULT = kill(pid, SIGKILL);  // Still consume the exact child even if signaling fails.
    int const KILL_ERROR = KILL_RESULT < 0 ? errno : 0;
    if (observation != nullptr) {
        observation->kill_result = KILL_RESULT;
        observation->kill_result_valid = true;
        observation->kill_error = KILL_ERROR;
        observation->kill_error_valid = KILL_RESULT < 0;
    }
    int status;
    for (;;) {
        pid_t rc = waitpid(pid, &status, 0);
        int const WAIT_ERROR = rc < 0 ? errno : 0;
        if (observation != nullptr) {
            ++observation->waits;
            if (rc < 0 && WAIT_ERROR == EINTR) ++observation->interrupted_waits;
            observation->wait_result = rc;
            observation->wait_result_valid = true;
            observation->wait_error = WAIT_ERROR;
            observation->wait_error_valid = rc < 0;
            observation->status_valid = rc == pid;
            observation->status = observation->status_valid ? status : 0;
        }
        if (rc == pid) {
            owned = false;
            return true;
        }
        if (rc < 0 && WAIT_ERROR == EINTR) continue;
        return false;
    }
}
struct OwnResources {
    uint32_t fds = 0, maps = 0, max_fds = 0, max_maps = 0;
    uint64_t created_fds = 0, closed_fds = 0, created_maps = 0, unmapped = 0;
    bool complete = true;
};
auto close_fd(int& fd, OwnResources& own) -> bool {
    if (fd < 0) return true;
    int saved = fd;
    fd = -1;
    if (close(saved) != 0) {
        own.complete = false;
        return false;
    }
    --own.fds;
    ++own.closed_fds;
    return true;
}
auto confirm_closed(int fd) -> bool {
    if (fd < 0) return true;
    errno = 0;
    return fcntl(fd, F_GETFD, 0) == -1 && errno == EBADF;
}
void report_own(const OwnResources& own, uint32_t cycles, const char* phase) {
    std::printf(
        "NUMA_OWN_PARENT phase=%s cycles=%u fds=%u maps=%u max_fds=%u max_maps=%u created_fds=%llu closed_fds=%llu "
        "created_maps=%llu unmapped=%llu complete=%u source=exact_created_resources global_debt=not_observed\n",
        phase, cycles, own.fds, own.maps, own.max_fds, own.max_maps, static_cast<unsigned long long>(own.created_fds),
        static_cast<unsigned long long>(own.closed_fds), static_cast<unsigned long long>(own.created_maps),
        static_cast<unsigned long long>(own.unmapped), static_cast<unsigned>(own.complete));
}

}  // namespace

auto run_numa_first_touch(int argc, char** argv) -> int {
    Options opts;
    if (!options(argc, argv, false, opts)) {
        std::fprintf(stderr, "numa-first-touch: invalid bounded options\n");
        return 2;
    }
    Topology topo;
    auto rc = topology(topo, opts.domains);
    if (rc != 0) {
        std::fprintf(stderr, "numa-first-touch: topology error=%lld\n", static_cast<long long>(rc));
        return 1;
    }
    std::atomic<bool> go{false}, cancel{false};
    std::array<FirstWorker, 2> workers;
    std::array<pthread_t, 2> threads{};
    uint32_t created = 0;
    bool ok = true;
    uint64_t deadline;
    if (!deadline_after(opts.timeout_ms, deadline)) return 1;
    for (uint32_t i = 0; i < topo.selected_count; ++i) {
        auto& w = workers[i];
        w.opts = &opts;
        w.selection = topo.selected[i];
        w.node_count = topo.summary.node_count;
        w.go = &go;
        w.cancel = &cancel;
        int error = pthread_create(&threads[i], nullptr, first_worker, &w);
        if (error != 0) {
            std::fprintf(stderr, "numa-first-touch: pthread-create index=%u error=%d\n", i, error);
            ok = false;
            break;
        }
        ++created;
    }
    if (created != topo.selected_count) ok = false;
    for (uint32_t i = 0; i < created && ok; ++i) {
        auto& w = workers[i];
        int64_t placement =
            wait_flag(w.started, deadline) ? place_thread(w.tid.load(std::memory_order_acquire), w.selection.cpu, deadline) : -ETIMEDOUT;
        if (placement != 0) {
            std::fprintf(stderr, "NUMA_FIRST_TOUCH_FAIL phase=controller_placement worker=%u tid=%llu cpu=%u error=%lld\n", i,
                         static_cast<unsigned long long>(w.tid.load(std::memory_order_acquire)), w.selection.cpu,
                         static_cast<long long>(placement));
            ok = false;
        } else {
            w.placed.store(true, std::memory_order_release);
        }
    }
    for (uint32_t i = 0; i < created && ok; ++i)
        if (!wait_flag(workers[i].ready, deadline)) ok = false;
    if (!ok) cancel.store(true, std::memory_order_release);
    go.store(true, std::memory_order_release);
    for (uint32_t i = 0; i < created; ++i)
        if (!wait_flag(workers[i].done, deadline)) {
            ok = false;
            cancel.store(true, std::memory_order_release);
        }
    for (uint32_t i = 0; i < created; ++i) {
        if (pthread_join(threads[i], nullptr) != 0) {
            ok = false;
            cancel.store(true, std::memory_order_release);
            // Keep the argument/control lifetime until the worker's final
            // access even on a library join failure. A kernel stall is bounded
            // by the enclosing runtime deadline, never a claimed successful join.
            while (!workers[i].done.load(std::memory_order_acquire)) (void)ker::multiproc::yield();
            continue;
        }
        auto const& w = workers[i];
        if (w.error != 0 || !w.unmapped) ok = false;
        std::printf(
            "NUMA_FIRST_TOUCH worker=%u seed=%llu pages=%u node=%u cpu=%u before=%llu after=%llu local=%llu elapsed_ns=%llu error=%lld "
            "unmapped=%u phase=%s tid=%llu\n",
            i, static_cast<unsigned long long>(opts.seed), opts.pages, w.selection.node, w.selection.cpu,
            static_cast<unsigned long long>(w.cpu_before), static_cast<unsigned long long>(w.cpu_after),
            static_cast<unsigned long long>(w.histogram[w.selection.node]), static_cast<unsigned long long>(w.elapsed_ns),
            static_cast<long long>(w.error), static_cast<unsigned>(w.unmapped), w.phase,
            static_cast<unsigned long long>(w.tid.load(std::memory_order_acquire)));
        for (uint32_t n = 0; n < topo.summary.node_count; ++n)
            std::printf("NUMA_RESIDENT worker=%u node=%u pages=%llu\n", i, n, static_cast<unsigned long long>(w.histogram[n]));
    }
    std::printf(
        "NUMA_FIRST_TOUCH_RESULT pass=%u workers=%u pages_each=%u local_percent_min=95 scope=scheduled_local_virgin_only "
        "persistent_pins=not_tested software_ensure=not_tested pressure=not_observed huge=0\n",
        static_cast<unsigned>(ok), created, opts.pages);
    return ok ? 0 : 1;
}

auto run_numa_lifecycle(int argc, char** argv) -> int {
    Options opts;
    if (!options(argc, argv, true, opts)) {
        std::fprintf(stderr, "numa-lifecycle: invalid bounded options\n");
        return 2;
    }
    Topology topo;
    auto rc = topology(topo, 0);
    if (rc != 0) {
        std::fprintf(stderr, "numa-lifecycle: topology error=%lld\n", static_cast<long long>(rc));
        return 1;
    }
    QueryBuffers io;
    initialize_transport(io);
    uint64_t deadline;
    if (!deadline_after(opts.timeout_ms, deadline)) return 1;
    Selection parent_selection{};
    const char* setup_phase = "local_policy";
    rc = local_policy(io);
    if (rc == 0) {
        setup_phase = "pin_current_cpu";
        rc = pin_current_cpu(topo, parent_selection, deadline);
    }
    if (rc != 0) {
        std::fprintf(stderr, "numa-lifecycle: setup phase=%s error=%lld\n", setup_phase, static_cast<long long>(rc));
        return 1;
    }
    std::printf("NUMA_LIFECYCLE_PLACEMENT parent=%u:%u child=controller_target_cpu method=inherited_ready_cooperative_placement\n",
                parent_selection.node, parent_selection.cpu);
    std::fflush(stdout);
    uint32_t completed = 0;
    uint32_t same_node_cycles = 0, cross_node_cycles = 0, cross_node_cow_cycles = 0;
    uint64_t cow_local_pages = 0, cow_resident_pages = 0;
    OwnResources own;
    report_own(own, 0, "begin");
    for (uint32_t cycle = 0; cycle < opts.cycles; ++cycle) {
        if (!before(deadline)) break;
        uint64_t child_deadline;
        if (!deadline_after(opts.child_timeout_ms, child_deadline)) break;
        if (child_deadline > deadline) child_deadline = deadline;
        void* raw = map_private(CYCLE_BYTES);
        if (raw == MAP_FAILED) {
            std::fprintf(stderr, "numa-lifecycle: mmap cycle=%u errno=%d\n", cycle, errno);
            break;
        }
        ++own.maps;
        ++own.created_maps;
        if (own.maps > own.max_maps) own.max_maps = own.maps;
        auto* mapping = static_cast<uint8_t*>(raw);
        auto* payload = static_cast<volatile uint8_t*>(raw);
        int fds[2]{-1, -1};
        int opened_fds[2]{-1, -1};
        pid_t pid = -1;
        bool owned = false, ok = true;
        const char* phase = "parent_original_bytes";
        int64_t source_query_error = 0;
        uint16_t source_nodes[2]{abi::INVALID_NODE, abi::INVALID_NODE};
        Selection child_target{};
        if (!lifecycle_child_target(topo, parent_selection, cycle, child_target)) {
            phase = "child_target";
            ok = false;
        }
        if ((reinterpret_cast<uint64_t>(raw) & (PAGE - 1)) || !cpu_still(parent_selection.cpu)) ok = false;
        if (ok)
            for (size_t i = 0; i < CYCLE_BYTES; ++i) payload[i] = pattern(opts.seed, cycle, i, false);
        if (ok) {
            phase = "source_node_query";
            source_query_error = source_page_nodes(io, mapping, topo, parent_selection, source_nodes);
            if (source_query_error != 0) ok = false;
        }
        if (ok) {
            phase = "pipe";
            if (pipe(fds) != 0)
                ok = false;
            else {
                opened_fds[0] = fds[0];
                opened_fds[1] = fds[1];
                own.fds += 2;
                own.created_fds += 2;
                if (own.fds > own.max_fds) own.max_fds = own.fds;
                if (!nonblocking(fds[0]) || !nonblocking(fds[1])) ok = false;
            }
        }
        if (ok) {
            phase = "fork";
            pid = fork();
            if (pid == 0) {
                if (close(fds[0]) != 0) _exit(63);
                int child_error = child_ready_and_cycle(mapping, fds[1], opts, topo, parent_selection, cycle, child_deadline);
                if (close(fds[1]) != 0 && child_error == 0) child_error = 64;
                _exit(child_error);
            }
            if (pid < 0)
                ok = false;
            else
                owned = true;
        }
        if (!close_fd(fds[1], own)) ok = false;
        ReadyObservation readiness{};
        if (ok && !receive_child_ready(fds[0], pid, opts, topo, parent_selection, child_target, cycle, child_deadline, phase, &readiness))
            ok = false;
        ChildRecord record;
        initialize_transport(record);
        if (ok) {
            phase = "child_record";
            if (!transfer(fds[0], &record, sizeof(record), false, child_deadline)) ok = false;
        }
        if (ok &&
            (record.magic != RECORD_MAGIC || record.version != 1 || record.bytes != sizeof(record) || record.seed != opts.seed ||
             record.cycle != cycle || record.pid != static_cast<uint32_t>(pid) || record.tid != static_cast<uint64_t>(pid) ||
             raw_error(record.tid) || record.error != 0 || !valid_selection(topo, {record.selected_node, record.selected_cpu}) ||
             record.selected_node != child_target.node || record.selected_cpu != child_target.cpu ||
             record.cpu_before != record.selected_cpu || record.cpu_after != record.selected_cpu || record.reserved || record.reserved2 ||
             record.phase != static_cast<uint32_t>(ChildPhase::COMPLETE) || record.policy.version != abi::VERSION ||
             record.policy.bytes != sizeof(record.policy) || record.policy.mode != static_cast<uint32_t>(abi::PolicyMode::LOCAL) ||
             record.policy.nodes || record.policy.reserved32 || record.policy.reserved64))
            ok = false;
        if (ok)
            for (auto const& page : record.pages)
                if (page.status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) || page.node >= topo.summary.node_count ||
                    page.reserved32)
                    ok = false;
        if (ok) {
            phase = "parent_pre_copyout";
            if (!verify_cycle(payload, opts.seed, cycle, false)) ok = false;
        }
        if (ok) {
            phase = "crosspage_copyout";
            if (!transfer(fds[0], mapping + CROSS_START, CROSS_BYTES, false, child_deadline)) ok = false;
        }
        if (ok) {
            phase = "parent_post_copyout";
            if (!verify_cycle(payload, opts.seed, cycle, true)) ok = false;
        }
        int status = -1;
        WaitObservation wait_observation{};
        if (ok) {
            phase = "wait0";
            if (!wait_child(pid, child_deadline, status, owned, wait_observation)) ok = false;
        }
        bool const WAIT_OWNED = owned;
        bool const READ_CLOSED = close_fd(fds[0], own);
        if (!READ_CLOSED) ok = false;
        bool const CLEANUP_REAP_NEEDED = owned;
        CleanupObservation cleanup_observation{};
        bool const reaped = terminate_and_reap(pid, owned, &cleanup_observation);
        bool const unmapped = munmap(mapping, CYCLE_BYTES) == 0;
        if (unmapped) {
            --own.maps;
            ++own.unmapped;
        } else
            own.complete = false;
        bool const fds_absent = confirm_closed(opened_fds[0]) && confirm_closed(opened_fds[1]);
        bool mapping_absent = false;
        bool const MAPPING_QUERY_VALID = unmapped;
        int64_t const MAPPING_QUERY = unmapped ? page_nodes(io, mapping, 3) : 0;
        if (unmapped && MAPPING_QUERY == 0) {
            mapping_absent = true;
            for (uint32_t i = 0; i < 3; ++i)
                if (io.pages[i].node != abi::INVALID_NODE || io.pages[i].status != static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE) ||
                    io.pages[i].reserved32)
                    mapping_absent = false;
        }
        if (!fds_absent || !mapping_absent) own.complete = false;
        if (!ok || !reaped || owned || !unmapped || !fds_absent || !mapping_absent || own.fds || own.maps || !own.complete) {
            std::fprintf(
                stderr,
                "NUMA_LIFECYCLE_FAIL cycle=%u pid=%d phase=%s source_query_error=%lld child_phase=%u child_error=%lld "
                "wait_status=%d reaped=%u unmapped=%u errno=%d "
                "wait_outcome=%u wait_rc_valid=%u wait_rc=%lld wait_errno_valid=%u wait_errno=%d "
                "wait_status_valid=%u wait_status_observed=%d wait_polls=%llu wait_owned=%u "
                "clock_valid=%u clock_rc=%d clock_errno=%d clock_sec=%lld clock_nsec=%lld clock_ns=%llu wait_deadline=%llu "
                "close_read=%u cleanup_reap_needed=%u cleanup_reap=%u cleanup_owned=%u fd_absent=%u "
                "map_query_valid=%u map_query=%lld mapping_absent=%u own_fds=%u own_maps=%u own_complete=%u "
                "cleanup_kill_rc_valid=%u cleanup_kill_rc=%lld cleanup_kill_errno_valid=%u cleanup_kill_errno=%d "
                "cleanup_wait_calls=%llu cleanup_wait_eintr=%llu cleanup_wait_rc_valid=%u cleanup_wait_rc=%lld "
                "cleanup_wait_errno_valid=%u cleanup_wait_errno=%d cleanup_status_valid=%u cleanup_status=%d child_elapsed_raw_ns=%llu\n",
                cycle, static_cast<int>(pid), phase, static_cast<long long>(source_query_error), record.phase,
                static_cast<long long>(record.error), status, static_cast<unsigned>(reaped && !owned), static_cast<unsigned>(unmapped),
                errno, static_cast<unsigned>(wait_observation.outcome), static_cast<unsigned>(wait_observation.result_valid),
                static_cast<long long>(wait_observation.result), static_cast<unsigned>(wait_observation.error_valid),
                wait_observation.error, static_cast<unsigned>(wait_observation.status_valid), wait_observation.status,
                static_cast<unsigned long long>(wait_observation.polls), static_cast<unsigned>(WAIT_OWNED), wait_observation.clock.valid,
                wait_observation.clock.result, wait_observation.clock.error, static_cast<long long>(wait_observation.clock.seconds),
                static_cast<long long>(wait_observation.clock.nanoseconds), static_cast<unsigned long long>(wait_observation.clock.now),
                static_cast<unsigned long long>(wait_observation.deadline), static_cast<unsigned>(READ_CLOSED),
                static_cast<unsigned>(CLEANUP_REAP_NEEDED), static_cast<unsigned>(reaped), static_cast<unsigned>(owned),
                static_cast<unsigned>(fds_absent), static_cast<unsigned>(MAPPING_QUERY_VALID), static_cast<long long>(MAPPING_QUERY),
                static_cast<unsigned>(mapping_absent), own.fds, own.maps, static_cast<unsigned>(own.complete),
                static_cast<unsigned>(cleanup_observation.kill_result_valid), static_cast<long long>(cleanup_observation.kill_result),
                static_cast<unsigned>(cleanup_observation.kill_error_valid), cleanup_observation.kill_error,
                static_cast<unsigned long long>(cleanup_observation.waits),
                static_cast<unsigned long long>(cleanup_observation.interrupted_waits),
                static_cast<unsigned>(cleanup_observation.wait_result_valid), static_cast<long long>(cleanup_observation.wait_result),
                static_cast<unsigned>(cleanup_observation.wait_error_valid), cleanup_observation.wait_error,
                static_cast<unsigned>(cleanup_observation.status_valid), cleanup_observation.status,
                static_cast<unsigned long long>(record.elapsed_ns));
            report_ready_failure(cycle, pid, readiness);
            break;
        }
        ++completed;
        if (record.selected_node == parent_selection.node) {
            ++same_node_cycles;
        } else {
            ++cross_node_cycles;
            if (record.pages[0].node != source_nodes[0] && record.pages[1].node != source_nodes[1]) ++cross_node_cow_cycles;
        }
        for (auto const& page : record.pages) {
            ++cow_resident_pages;
            if (page.node == record.selected_node) ++cow_local_pages;
        }
        if (completed % 128 == 0 || completed == opts.cycles) report_own(own, completed, "settled");
        std::printf(
            "NUMA_CHILD cycle=%u pid=%u tid=%llu seed=%llu cpu=%llu:%llu target=%u:%u nodes=%u:%u policy=LOCAL elapsed_ns=%llu wait0=1 "
            "unmapped=1 source_nodes=%u:%u\n",
            cycle, record.pid, static_cast<unsigned long long>(record.tid), static_cast<unsigned long long>(record.seed),
            static_cast<unsigned long long>(record.cpu_before), static_cast<unsigned long long>(record.cpu_after), record.selected_node,
            record.selected_cpu, record.pages[0].node, record.pages[1].node, static_cast<unsigned long long>(record.elapsed_ns),
            source_nodes[0], source_nodes[1]);
    }
    report_own(own, completed, "final");
    bool const ok = completed == opts.cycles && own.complete && own.fds == 0 && own.maps == 0 &&
                    lifecycle_coverage(topo, completed, same_node_cycles, cross_node_cycles, cross_node_cow_cycles, cow_local_pages,
                                       cow_resident_pages);
    std::printf(
        "NUMA_LIFECYCLE_RESULT pass=%u completed=%u requested=%u seed=%llu parent_cpu=%u "
        "same_node=%u cross_node=%u cross_node_cow=%u cow_local=%llu cow_resident=%llu cow_local_percent_min=95 "
        "scope=map_fork_cow_crosspage_pipe_wait_unmap_only\n",
        static_cast<unsigned>(ok), completed, opts.cycles, static_cast<unsigned long long>(opts.seed), parent_selection.cpu,
        same_node_cycles, cross_node_cycles, cross_node_cow_cycles, static_cast<unsigned long long>(cow_local_pages),
        static_cast<unsigned long long>(cow_resident_pages));
    return ok ? 0 : 1;
}

namespace {
// Separate healthy public-policy probe. These records are caller-owned; they
// never stand in for kernel census/physical-owner or pressure authority.
constexpr uint32_t POLICY_MAX_PAGES = 256;
struct PolicyOptions {
    uint64_t nonce{};
    uint32_t pages{64}, domains{}, timeout_ms{30000}, child_timeout_ms{5000};
};
struct PolicyReport {
    uint64_t nonce{}, pid{}, tid{}, cpu{}, affinity{}, address{}, nodes_mask{}, limit_soft{}, limit_hard{}, observed_mask{}, changed_mask{};
    int64_t error{}, query_rc{}, change_rc{}, change_query_rc{}, restore_rc{}, map_rc{}, unmap_rc{};
    int map_errno{}, unmap_errno{};
    uint32_t mode{}, actor{}, pages{}, virgin{}, resident{}, stable{}, unmapped{}, local{}, phase{}, observed_mode{}, changed_mode{};
    std::array<uint16_t, POLICY_MAX_PAGES> nodes{};
};
static_assert(std::is_trivially_copyable_v<PolicyReport> && sizeof(PolicyReport) < 1024);
struct PolicyWorker {
    PolicyOptions opts{};
    const Topology* topo{};
    Selection selected{};
    uint32_t mode{};
    uint64_t mask{}, deadline{};
    std::atomic<bool> started{false}, placed{false}, cancel{false}, done{false};
    std::atomic<uint64_t> tid{0};
    PolicyReport report{};
};
struct PolicyQuota {
    uint64_t before{}, denied_before{}, accepted{}, denied_after{}, final{}, inherited_soft{}, inherited_hard{}, final_soft{}, final_hard{};
    int64_t first_rc{}, second_rc{}, restore_rc{};
    int first_errno{}, second_errno{};
    bool child_consumed{}, reservation_removed{};
};

auto policy_options(int argc, char** argv, PolicyOptions& out) -> bool {
    uint32_t seen = 0;
    for (int i = 0; i < argc; ++i) {
        if (i + 1 >= argc) return false;
        const char* key = argv[i++];
        uint64_t value{};
        if (!number(argv[i], value)) return false;
        uint32_t bit = 0;
        if (std::strcmp(key, "--nonce") == 0 && value != 0) {
            bit = 1;
            out.nonce = value;
        } else if (std::strcmp(key, "--pages") == 0 && value >= 1 && value <= POLICY_MAX_PAGES) {
            bit = 2;
            out.pages = value;
        } else if (std::strcmp(key, "--domains") == 0 && value >= 1 && value <= 2) {
            bit = 4;
            out.domains = value;
        } else if (std::strcmp(key, "--timeout-ms") == 0 && value >= 1000 && value <= 120000) {
            bit = 8;
            out.timeout_ms = value;
        } else if (std::strcmp(key, "--child-timeout-ms") == 0 && value >= 100 && value <= 30000) {
            bit = 16;
            out.child_timeout_ms = value;
        } else
            return false;
        if (seen & bit) return false;
        seen |= bit;
    }
    return (seen & 5) == 5 && out.child_timeout_ms <= out.timeout_ms;
}

auto policy_read(QueryBuffers& io, abi::SelfPolicy& out) -> int64_t {
    initialize_transport(out);
    int64_t const RC = query(io, abi::QueryKind::SELF_POLICY, &out, sizeof(out));
    if (RC != 0) return RC;
    return out.version == abi::VERSION && out.bytes == sizeof(out) && out.mode <= 3 && !out.reserved32 && !out.reserved64 ? 0 : -EPROTO;
}
auto policy_set(uint32_t mode, uint64_t mask) -> int64_t {
    abi::PolicyRequest request;
    initialize_transport(request);
    request.version = abi::VERSION;
    request.bytes = sizeof(request);
    request.mode = mode;
    request.nodes = mask;
    return ker::vmem::set_memory_policy(request);
}
auto policy_matches(QueryBuffers& io, uint32_t mode, uint64_t mask) -> bool {
    abi::SelfPolicy out;
    return policy_read(io, out) == 0 && out.mode == mode && out.nodes == mask;
}
auto policy_change(uint32_t mode, uint64_t mask, QueryBuffers& io, PolicyReport& out) -> int64_t {
    out.change_rc = policy_set(mode, mask);
    if (out.change_rc != 0) return out.change_rc;
    abi::SelfPolicy changed;
    initialize_transport(changed);
    out.change_query_rc = policy_read(io, changed);
    out.changed_mode = changed.mode;
    out.changed_mask = changed.nodes;
    if (out.change_query_rc != 0) return out.change_query_rc;
    return changed.mode == mode && changed.nodes == mask ? 0 : -EPROTO;
}
auto policy_topology(QueryBuffers& io, Topology& topo, uint32_t domains) -> int64_t {
    initialize_transport(topo);
    auto rc = query(io, abi::QueryKind::SUMMARY, &topo.summary, sizeof(topo.summary));
    auto const& s = topo.summary;
    if (rc != 0) return rc;
    if (s.version != abi::VERSION || s.bytes != sizeof(s) || s.features != abi::FEATURES_V1 || s.node_count != domains ||
        s.cpu_count == 0 || s.cpu_count > 64 || s.page_bytes != PAGE || s.max_page_nodes != abi::MAX_PAGE_NODES || s.cpu_mask_words != 4 ||
        s.provenance > 3 || s.reserved64[0] || s.reserved64[1] || s.reserved64[2])
        return -ENOTSUP;
    uint64_t seen = 0;
    for (uint32_t n = 0; n < domains; ++n) {
        auto& node = topo.nodes[n];
        rc = query(io, abi::QueryKind::NODE, &node, sizeof(node), n);
        if (rc != 0) return rc;
        uint64_t const mask = node.cpu_words[0];
        if (node.version != abi::VERSION || node.bytes != sizeof(node) || node.node != n || node.reserved16 || node.reserved64 ||
            !node.managed_bytes || node.managed_bytes % PAGE || !mask || node.cpu_words[1] || node.cpu_words[2] || node.cpu_words[3] ||
            (mask & seen) || (s.cpu_count < 64 && mask >> s.cpu_count))
            return -ENOTSUP;
        seen |= mask;
    }
    return seen == (s.cpu_count == 64 ? UINT64_MAX : (uint64_t{1} << s.cpu_count) - 1) ? 0 : -EPROTO;
}
auto policy_expected(uint32_t mode, uint64_t mask, uint64_t address, uint16_t local) -> uint16_t {
    if (mode == 0) return local;
    uint16_t members[2]{};
    uint32_t count = 0;
    for (uint16_t n = 0; n < 2; ++n)
        if (mask & (uint64_t{1} << n)) members[count++] = n;
    if (!count) return abi::INVALID_NODE;
    return mode == 3 ? members[(address / PAGE) % count] : members[0];
}

// Candidate transport exists before mmap, before child publication and before
// any finite AS soft limit. Query transport never aliases the payload.
auto policy_mapping(const PolicyOptions& opts, QueryBuffers& io, Selection selected, uint32_t mode, uint64_t mask, uint32_t actor,
                    uint64_t deadline, PolicyReport& out) -> int64_t {
    initialize_transport(out);
    out.nonce = opts.nonce;
    out.pid = getpid();
    out.tid = ker::multiproc::currentThreadId();
    out.cpu = ker::multiproc::getcurrent_cpu();
    out.affinity = ker::multiproc::getThreadAffinityMask(out.tid);
    out.mode = mode;
    out.nodes_mask = mask;
    out.actor = actor;
    out.pages = opts.pages;
    void* mapping = nullptr;
    int64_t rc = 0;
    out.phase = 1;
    abi::SelfPolicy observed;
    initialize_transport(observed);
    out.query_rc = policy_read(io, observed);
    out.observed_mode = observed.mode;
    out.observed_mask = observed.nodes;
    if (!out.tid || raw_error(out.tid) || out.cpu != selected.cpu || out.affinity != (uint64_t{1} << selected.cpu) || out.query_rc != 0 ||
        observed.mode != mode || observed.nodes != mask)
        rc = -EPROTO;
    if (rc == 0) {
        out.phase = 2;
        mapping = map_private(static_cast<size_t>(opts.pages) * PAGE);
        int const ERROR = mapping == MAP_FAILED ? errno : 0;
        out.map_rc = mapping == MAP_FAILED ? -1 : 0;
        out.map_errno = ERROR;
        if (mapping == MAP_FAILED) {
            mapping = nullptr;
            rc = -ERROR;
        } else
            out.address = reinterpret_cast<uint64_t>(mapping);
    }
    if (rc == 0 && (out.address % PAGE || !before(deadline))) rc = -EPROTO;
    for (uint32_t first = 0; rc == 0 && first < opts.pages; first += abi::MAX_PAGE_NODES) {
        uint32_t const count = opts.pages - first < abi::MAX_PAGE_NODES ? opts.pages - first : abi::MAX_PAGE_NODES;
        out.query_rc = page_nodes(io, static_cast<uint8_t*>(mapping) + static_cast<size_t>(first) * PAGE, count);
        if (out.query_rc != 0) {
            rc = out.query_rc;
            break;
        }
        for (uint32_t i = 0; i < count; ++i) {
            auto const& row = io.pages[i];
            if (row.reserved32 || row.node != abi::INVALID_NODE || row.status != static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE))
                rc = -EPROTO;
            else
                ++out.virgin;
        }
    }
    if (rc == 0) {
        out.phase = 3;
        auto* bytes = static_cast<volatile uint8_t*>(mapping);
        for (uint32_t p = 0; p < opts.pages; ++p) bytes[static_cast<size_t>(p) * PAGE] = static_cast<uint8_t>(1 + ((opts.nonce + p) % 251));
        for (uint32_t p = 0; p < opts.pages; ++p) {
            size_t const start = static_cast<size_t>(p) * PAGE;
            if (bytes[start] != static_cast<uint8_t>(1 + ((opts.nonce + p) % 251))) rc = -EILSEQ;
            for (size_t i = 1; i < PAGE; ++i)
                if (bytes[start + i] != 0) rc = -EILSEQ;
            if (!before(deadline)) {
                rc = -ETIMEDOUT;
                break;
            }
        }
    }
    for (uint32_t first = 0; rc == 0 && first < opts.pages; first += abi::MAX_PAGE_NODES) {
        uint32_t const count = opts.pages - first < abi::MAX_PAGE_NODES ? opts.pages - first : abi::MAX_PAGE_NODES;
        out.query_rc = page_nodes(io, static_cast<uint8_t*>(mapping) + static_cast<size_t>(first) * PAGE, count);
        if (out.query_rc != 0) {
            rc = out.query_rc;
            break;
        }
        for (uint32_t i = 0; i < count; ++i) {
            auto const& row = io.pages[i];
            uint32_t const p = first + i;
            out.nodes[p] = row.node;
            if (row.reserved32 || row.status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) || row.node >= opts.domains)
                rc = -EPROTO;
            else {
                ++out.resident;
                if (row.node == selected.node) ++out.local;
            }
            if (mode != 0 && row.node != policy_expected(mode, mask, out.address + static_cast<uint64_t>(p) * PAGE, selected.node))
                rc = -EPROTO;
        }
    }
    if (rc == 0 && mode == 0 && static_cast<uint64_t>(out.local) * 100 < static_cast<uint64_t>(opts.pages) * 95) rc = -EPROTO;
    if (rc == 0) {
        out.phase = 4;
        rc = policy_change(mode == 0 ? 2 : 0, mode == 0 ? (uint64_t{1} << selected.node) : 0, io, out);
        for (uint32_t first = 0; rc == 0 && first < opts.pages; first += abi::MAX_PAGE_NODES) {
            uint32_t const count = opts.pages - first < abi::MAX_PAGE_NODES ? opts.pages - first : abi::MAX_PAGE_NODES;
            out.query_rc = page_nodes(io, static_cast<uint8_t*>(mapping) + static_cast<size_t>(first) * PAGE, count);
            if (out.query_rc != 0) {
                rc = out.query_rc;
                break;
            }
            for (uint32_t i = 0; i < count; ++i)
                if (io.pages[i].reserved32 || io.pages[i].status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) ||
                    io.pages[i].node != out.nodes[first + i])
                    rc = -EPROTO;
                else
                    ++out.stable;
        }
    }
    out.restore_rc = policy_set(mode, mask);
    if (rc == 0 && (out.restore_rc != 0 || !policy_matches(io, mode, mask) || !cpu_still(selected.cpu))) rc = -EPROTO;
    if (mapping != nullptr) {
        int const RET = munmap(mapping, static_cast<size_t>(opts.pages) * PAGE);
        int const ERROR = RET < 0 ? errno : 0;
        out.unmap_rc = RET;
        out.unmap_errno = ERROR;
        if (RET == 0)
            out.unmapped = 1;
        else if (rc == 0)
            rc = -ERROR;
    }
    if (rc == 0 && !before(deadline)) rc = -ETIMEDOUT;
    out.error = rc;
    if (rc == 0) out.phase = 5;
    return rc;
}

void* policy_worker(void* opaque) {
    auto& worker = *static_cast<PolicyWorker*>(opaque);
    QueryBuffers io;
    initialize_transport(io);
    abi::SelfPolicy inherited;
    initialize_transport(inherited);
    int64_t rc = policy_read(io, inherited);
    uint64_t const TID = ker::multiproc::currentThreadId();
    uint64_t const MASK = ker::multiproc::getThreadAffinityMask(TID);
    if (rc == 0 && (inherited.mode != worker.mode || inherited.nodes != worker.mask || MASK != (uint64_t{1} << worker.selected.cpu)))
        rc = -EPROTO;
    worker.tid.store(TID, std::memory_order_release);
    worker.started.store(true, std::memory_order_release);
    while (!worker.placed.load(std::memory_order_acquire) && rc == 0) {
        if (worker.cancel.load(std::memory_order_acquire) || !before(worker.deadline))
            rc = -ECANCELED;
        else
            (void)ker::multiproc::yield();
    }
    if (rc == 0) rc = policy_mapping(worker.opts, io, worker.selected, worker.mode, worker.mask, 1, worker.deadline, worker.report);
    if (rc == 0)
        rc = policy_change(worker.mode == 0 ? 2 : 0, worker.mode == 0 ? (uint64_t{1} << worker.selected.node) : 0, io, worker.report);
    worker.report.observed_mode = inherited.mode;
    worker.report.observed_mask = inherited.nodes;
    worker.report.error = rc;
    worker.done.store(true, std::memory_order_release);  // Last argument access.
    return nullptr;
}

auto policy_usage(ker::abi::resource::Usage& out) -> int64_t {
    initialize_transport(out);
    return static_cast<int64_t>(
        ::syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::RESOURCE_USAGE),
                  static_cast<uint64_t>(ker::abi::resource::Id::AS), reinterpret_cast<uint64_t>(&out), ker::abi::resource::VERSION));
}

auto policy_child(const PolicyOptions& opts, const Topology& topo, Selection selected, uint32_t mode, uint64_t mask, bool quota_only,
                  const rlimit& expected_limit, int fd, uint64_t deadline) -> int {
    (void)topo;
    PolicyReport report;
    initialize_transport(report);
    QueryBuffers io;
    initialize_transport(io);
    report.nonce = opts.nonce;
    report.pid = getpid();
    report.tid = ker::multiproc::currentThreadId();
    report.mode = mode;
    report.nodes_mask = mask;
    report.actor = quota_only ? 3 : 2;
    abi::SelfPolicy inherited;
    initialize_transport(inherited);
    int64_t rc = policy_read(io, inherited);
    if (rc == 0 && (inherited.mode != mode || inherited.nodes != mask || !cpu_still(selected.cpu))) rc = -EPROTO;
    if (rc == 0 && quota_only) {
        rlimit actual;
        initialize_transport(actual);
        int const RET = getrlimit(RLIMIT_AS, &actual);
        int const ERROR = RET < 0 ? errno : 0;
        report.limit_soft = actual.rlim_cur;
        report.limit_hard = actual.rlim_max;
        if (RET != 0)
            rc = -ERROR;
        else if (actual.rlim_cur != expected_limit.rlim_cur || actual.rlim_max != expected_limit.rlim_max)
            rc = -EPROTO;
    } else if (rc == 0) {
        rc = policy_mapping(opts, io, selected, mode, mask, 2, deadline, report);
        if (rc == 0) rc = policy_change(mode == 0 ? 2 : 0, mode == 0 ? (uint64_t{1} << selected.node) : 0, io, report);
    }
    report.observed_mode = inherited.mode;
    report.observed_mask = inherited.nodes;
    report.error = rc;
    if (report.pid != report.tid) report.error = -EPROTO;
    return transfer(fd, &report, sizeof(report), true, deadline) && report.error == 0 ? 0 : 71;
}

auto policy_pipe(int (&fds)[2], OwnResources& own) -> bool {
    if (pipe(fds) != 0) return false;
    own.fds += 2;
    own.created_fds += 2;
    if (nonblocking(fds[0]) && nonblocking(fds[1])) return true;
    (void)close_fd(fds[0], own);
    (void)close_fd(fds[1], own);
    return false;
}
auto policy_fork(const PolicyOptions& opts, const Topology& topo, Selection selected, uint32_t mode, uint64_t mask, bool quota_only,
                 const rlimit& expected_limit, uint64_t deadline, PolicyReport& report, OwnResources& own, int (&fds)[2]) -> bool {
    initialize_transport(report);
    bool owned = false, ok = before(deadline);
    pid_t pid = -1;
    if (ok) {
        pid = fork();
        if (pid == 0) {
            if (close(fds[0]) != 0) _exit(72);
            int rc = policy_child(opts, topo, selected, mode, mask, quota_only, expected_limit, fds[1], deadline);
            if (close(fds[1]) != 0 && rc == 0) rc = 73;
            _exit(rc);
        }
        if (pid < 0)
            ok = false;
        else
            owned = true;
    }
    if (!close_fd(fds[1], own)) ok = false;
    if (ok && !transfer(fds[0], &report, sizeof(report), false, deadline)) ok = false;
    if (ok && (report.nonce != opts.nonce || report.pid != static_cast<uint64_t>(pid) || report.tid != static_cast<uint64_t>(pid) ||
               report.mode != mode || report.nodes_mask != mask || report.actor != (quota_only ? 3U : 2U) || report.error != 0))
        ok = false;
    int status = -1;
    WaitObservation wait;
    bool const consumed = owned && wait_child(pid, deadline, status, owned, wait);
    if (!close_fd(fds[0], own)) ok = false;
    if (owned) {
        ok = false;
        if (!terminate_and_reap(pid, owned)) own.complete = false;
    }
    std::printf(
        "NUMA_POLICY "
        "{\"type\":\"wait\",\"nonce\":%llu,\"mode\":%u,\"actor\":%u,\"pid\":%lld,\"rc\":%lld,\"rc_valid\":%u,\"status\":%d,\"status_"
        "valid\":%"
        "u,\"errno\":%d,\"errno_valid\":%u,\"consumed\":%u,\"owned\":%u}\n",
        static_cast<unsigned long long>(opts.nonce), mode, quota_only ? 3U : 2U, static_cast<long long>(pid),
        static_cast<long long>(wait.result), static_cast<unsigned>(wait.result_valid), wait.status,
        static_cast<unsigned>(wait.status_valid), wait.error, static_cast<unsigned>(wait.error_valid), static_cast<unsigned>(consumed),
        static_cast<unsigned>(owned));
    return ok && consumed && !owned && own.complete && own.fds == 0 && before(deadline);
}

void policy_print(const PolicyReport& record) {
    std::printf(
        "NUMA_POLICY "
        "{\"type\":\"actor\",\"nonce\":%llu,\"mode\":%u,\"actor\":%u,\"pid\":%llu,\"tid\":%llu,\"cpu\":%llu,\"affinity\":%llu,\"address\":%"
        "llu,\"mask\":%llu,\"observed_mode\":%u,\"observed_mask\":%llu,\"pages\":%u,\"virgin\":%u,\"resident\":%u,\"stable\":%u,"
        "\"unmapped\":%u,\"local\":%u,\"phase\":%u,\"error\":%lld,\"map_rc\":%lld,\"map_errno\":%d,\"query_rc\":%lld,\"change_rc\":%lld,"
        "\"change_query_rc\":%lld,\"changed_mode\":%u,\"changed_mask\":%llu,\"restore_rc\":%lld,\"unmap_rc\":%lld,\"unmap_errno\":%d}\n",
        static_cast<unsigned long long>(record.nonce), record.mode, record.actor, static_cast<unsigned long long>(record.pid),
        static_cast<unsigned long long>(record.tid), static_cast<unsigned long long>(record.cpu),
        static_cast<unsigned long long>(record.affinity), static_cast<unsigned long long>(record.address),
        static_cast<unsigned long long>(record.nodes_mask), record.observed_mode, static_cast<unsigned long long>(record.observed_mask),
        record.pages, record.virgin, record.resident, record.stable, record.unmapped, record.local, record.phase,
        static_cast<long long>(record.error), static_cast<long long>(record.map_rc), record.map_errno,
        static_cast<long long>(record.query_rc), static_cast<long long>(record.change_rc), static_cast<long long>(record.change_query_rc),
        record.changed_mode, static_cast<unsigned long long>(record.changed_mask), static_cast<long long>(record.restore_rc),
        static_cast<long long>(record.unmap_rc), record.unmap_errno);
    for (uint32_t p = 0; p < record.pages && p < POLICY_MAX_PAGES; ++p)
        std::printf("NUMA_POLICY {\"type\":\"page\",\"nonce\":%llu,\"mode\":%u,\"actor\":%u,\"index\":%u,\"va\":%llu,\"node\":%u}\n",
                    static_cast<unsigned long long>(record.nonce), record.mode, record.actor, p,
                    static_cast<unsigned long long>(record.address + static_cast<uint64_t>(p) * PAGE), record.nodes[p]);
}
}  // namespace

auto run_numa_policy(int argc, char** argv) -> int {
    PolicyOptions opts;
    if (!policy_options(argc, argv, opts)) {
        std::fprintf(stderr, "numa-policy: invalid bounded options\n");
        return 2;
    }
    QueryBuffers io;
    initialize_transport(io);
    Topology topo;
    initialize_transport(topo);
    abi::SelfPolicy original;
    initialize_transport(original);
    uint64_t deadline{};
    auto rc = deadline_after(opts.timeout_ms, deadline) ? policy_topology(io, topo, opts.domains) : -EIO;
    if (rc == 0) rc = policy_read(io, original);
    uint64_t const TID = ker::multiproc::currentThreadId();
    uint64_t const ORIGINAL_AFFINITY = ker::multiproc::getThreadAffinityMask(TID);
    Selection selected{};
    if (rc == 0) rc = pin_current_cpu(topo, selected, deadline);
    if (rc != 0) {
        if (TID && !raw_error(TID) && ORIGINAL_AFFINITY) (void)ker::multiproc::setThreadAffinityMask(TID, ORIGINAL_AFFINITY);
        std::fprintf(stderr, "numa-policy: setup error=%lld\n", static_cast<long long>(rc));
        return 1;
    }
    std::printf(
        "NUMA_POLICY "
        "{\"type\":\"begin\",\"version\":1,\"nonce\":%llu,\"domains\":%u,\"pages\":%u,\"pid\":%lld,\"tid\":%llu,\"cpu\":%u,\"local_node\":%"
        "u,"
        "\"cpus\":%u,\"features\":%llu,\"eligible\":\"virgin_private_cpl3_write\",\"excluded\":\"transport_stack_control\",\"scope\":"
        "\"healthy_emulated_placement\"}\n",
        static_cast<unsigned long long>(opts.nonce), opts.domains, opts.pages, static_cast<long long>(getpid()),
        static_cast<unsigned long long>(TID), selected.cpu, selected.node, topo.summary.cpu_count,
        static_cast<unsigned long long>(topo.summary.features));
    for (uint32_t n = 0; n < opts.domains; ++n)
        std::printf("NUMA_POLICY {\"type\":\"node\",\"nonce\":%llu,\"node\":%u,\"managed\":%llu,\"cpu_mask\":%llu}\n",
                    static_cast<unsigned long long>(opts.nonce), n, static_cast<unsigned long long>(topo.nodes[n].managed_bytes),
                    static_cast<unsigned long long>(topo.nodes[n].cpu_words[0]));
    OwnResources own;
    uint32_t completed = 0, joined = 0, children = 0;
    for (uint32_t mode = 0; mode < 4 && rc == 0; ++mode) {
        uint16_t const target = opts.domains == 2 ? 1U - selected.node : selected.node;
        uint64_t const MASK = mode == 0 ? 0 : mode == 3 ? (uint64_t{1} << opts.domains) - 1 : uint64_t{1} << target;
        rc = policy_set(mode, MASK);
        abi::SelfPolicy actual_policy;
        initialize_transport(actual_policy);
        int64_t const QUERY_RC = rc == 0 ? policy_read(io, actual_policy) : rc;
        std::printf(
            "NUMA_POLICY "
            "{\"type\":\"mode\",\"nonce\":%llu,\"mode\":%u,\"mask\":%llu,\"set_rc\":%lld,\"query_rc\":%lld,\"observed_mode\":%u,\"observed_"
            "mask\":%llu}\n",
            static_cast<unsigned long long>(opts.nonce), mode, static_cast<unsigned long long>(MASK), static_cast<long long>(rc),
            static_cast<long long>(QUERY_RC), actual_policy.mode, static_cast<unsigned long long>(actual_policy.nodes));
        if (rc == 0 && (QUERY_RC != 0 || actual_policy.mode != mode || actual_policy.nodes != MASK)) rc = -EPROTO;
        PolicyReport parent;
        initialize_transport(parent);
        if (rc == 0) rc = policy_mapping(opts, io, selected, mode, MASK, 0, deadline, parent);
        policy_print(parent);
        PolicyWorker worker;
        initialize_transport(worker.report);
        worker.opts = opts;
        worker.topo = &topo;
        worker.selected = selected;
        worker.mode = mode;
        worker.mask = MASK;
        worker.deadline = deadline;
        pthread_t thread{};
        bool created = false;
        if (rc == 0) {
            int const ERROR = pthread_create(&thread, nullptr, policy_worker, &worker);
            if (ERROR != 0)
                rc = -ERROR;
            else
                created = true;
        }
        if (created) {
            if (!wait_flag(worker.started, deadline)) rc = -ETIMEDOUT;
            if (rc == 0) rc = place_thread(worker.tid.load(std::memory_order_acquire), selected.cpu, deadline);
            if (rc != 0) worker.cancel.store(true, std::memory_order_release);
            worker.placed.store(true, std::memory_order_release);
            if (!wait_flag(worker.done, deadline)) {
                rc = -ETIMEDOUT;
                worker.cancel.store(true, std::memory_order_release);
            }
            int const ERROR = pthread_join(thread, nullptr);
            if (ERROR == 0)
                ++joined;
            else {
                rc = -ERROR;
                // Keep the shared argument alive through its final access.
                while (!worker.done.load(std::memory_order_acquire)) (void)ker::multiproc::yield();
            }
            if (worker.report.error != 0 && rc == 0) rc = worker.report.error;
            if (!policy_matches(io, mode, MASK)) rc = -EPROTO;
            policy_print(worker.report);
        }
        rlimit unused;
        initialize_transport(unused);
        PolicyReport child;
        initialize_transport(child);
        uint64_t child_deadline{};
        if (rc == 0 && (!deadline_after(opts.child_timeout_ms, child_deadline) || !before(deadline))) rc = -ETIMEDOUT;
        if (child_deadline > deadline) child_deadline = deadline;
        int child_fds[2]{-1, -1};
        if (rc == 0 && !policy_pipe(child_fds, own)) rc = -EIO;
        if (rc == 0) {
            if (!policy_fork(opts, topo, selected, mode, MASK, false, unused, child_deadline, child, own, child_fds))
                rc = -EPROTO;
            else
                ++children;
            policy_print(child);
            if (!policy_matches(io, mode, MASK)) rc = -EPROTO;
        }
        // All actor/report storage above is prepared before lowering AS. Only
        // existing process-limit observations and a single lazy page are used.
        PolicyQuota quota;
        initialize_transport(quota);
        rlimit saved;
        initialize_transport(saved);
        rlimit restored;
        initialize_transport(restored);
        ker::abi::resource::Usage before_usage, after_usage;
        initialize_transport(before_usage);
        initialize_transport(after_usage);
        PolicyReport inherited;
        initialize_transport(inherited);
        int quota_fds[2]{-1, -1};
        if (rc == 0 && !policy_pipe(quota_fds, own)) rc = -EIO;
        bool limited = false;
        void* reservation = nullptr;
        if (rc == 0) {
            int const RET = getrlimit(RLIMIT_AS, &saved);
            int const ERROR = RET < 0 ? errno : 0;
            if (RET != 0)
                rc = -ERROR;
            else
                rc = policy_usage(before_usage);
            quota.before = before_usage.current;
            quota.denied_before = before_usage.denied;
        }
        rlimit finite = saved;
        if (rc == 0) {
            if (before_usage.current > UINT64_MAX - PAGE || before_usage.current + PAGE > saved.rlim_max ||
                before_usage.current + PAGE > saved.rlim_cur)
                rc = -ENOTSUP;
            else {
                finite.rlim_cur = before_usage.current + PAGE;
                int const RET = setrlimit(RLIMIT_AS, &finite);
                int const ERROR = RET < 0 ? errno : 0;
                if (RET != 0)
                    rc = -ERROR;
                else
                    limited = true;
            }
        }
        if (rc == 0) {
            if (!deadline_after(opts.child_timeout_ms, child_deadline)) rc = -EIO;
            if (child_deadline > deadline) child_deadline = deadline;
            if (rc == 0) {
                quota.child_consumed =
                    policy_fork(opts, topo, selected, mode, MASK, true, finite, child_deadline, inherited, own, quota_fds);
                if (!quota.child_consumed)
                    rc = -EPROTO;
                else
                    ++children;
                quota.inherited_soft = inherited.limit_soft;
                quota.inherited_hard = inherited.limit_hard;
            }
        }
        if (rc == 0) {
            reservation = mmap(nullptr, PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            int const ERROR = reservation == MAP_FAILED ? errno : 0;
            quota.first_rc = reservation == MAP_FAILED ? -1 : 0;
            quota.first_errno = ERROR;
            if (reservation == MAP_FAILED) {
                reservation = nullptr;
                rc = -ERROR;
            } else if (policy_usage(after_usage) != 0 || after_usage.current != before_usage.current + PAGE)
                rc = -EPROTO;
            quota.accepted = after_usage.current;
        }
        if (rc == 0) {
            void* excess = mmap(nullptr, PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            int const ERROR = excess == MAP_FAILED ? errno : 0;
            quota.second_rc = excess == MAP_FAILED ? -1 : 0;
            quota.second_errno = ERROR;
            if (excess != MAP_FAILED) {
                if (munmap(excess, PAGE) != 0) own.complete = false;
                rc = -EPROTO;
            } else if (ERROR != ENOMEM || policy_usage(after_usage) != 0 || after_usage.current != quota.accepted ||
                       after_usage.denied <= quota.denied_before)
                rc = -EPROTO;
            quota.denied_after = after_usage.denied;
        }
        if (quota_fds[0] >= 0 && !close_fd(quota_fds[0], own)) rc = -EIO;
        if (quota_fds[1] >= 0 && !close_fd(quota_fds[1], own)) rc = -EIO;
        if (reservation != nullptr) {
            int const RET = munmap(reservation, PAGE);
            int const ERROR = RET < 0 ? errno : 0;
            quota.reservation_removed = RET == 0;
            if (RET != 0 && rc == 0) rc = -ERROR;
        }
        if (limited) {
            int const RET = setrlimit(RLIMIT_AS, &saved);
            int const ERROR = RET < 0 ? errno : 0;
            quota.restore_rc = RET;
            if (RET != 0 && rc == 0) rc = -ERROR;
        }
        if (limited) {
            int const RET = getrlimit(RLIMIT_AS, &restored);
            int const ERROR = RET < 0 ? errno : 0;
            quota.final_soft = restored.rlim_cur;
            quota.final_hard = restored.rlim_max;
            if (RET != 0 && rc == 0)
                rc = -ERROR;
            else if (restored.rlim_cur != saved.rlim_cur || restored.rlim_max != saved.rlim_max)
                rc = -EPROTO;
            if (policy_usage(after_usage) != 0 || after_usage.current != quota.before) rc = -EPROTO;
        }
        quota.final = after_usage.current;
        if (rc == 0 && !before(deadline)) rc = -ETIMEDOUT;
        std::printf(
            "NUMA_POLICY "
            "{\"type\":\"quota\",\"nonce\":%llu,\"mode\":%u,\"before\":%llu,\"accepted\":%llu,\"final\":%llu,\"denied_before\":%llu,"
            "\"denied_after\":%llu,\"first_rc\":%lld,\"first_errno\":%d,\"second_rc\":%lld,\"second_errno\":%d,\"restored\":%lld,"
            "\"removed\":%u,\"child_consumed\":%u,\"inherited_soft\":%llu,\"inherited_hard\":%llu,\"saved_soft\":%llu,\"saved_hard\":%llu,"
            "\"final_soft\":%llu,\"final_hard\":%llu,\"error\":%lld}\n",
            static_cast<unsigned long long>(opts.nonce), mode, static_cast<unsigned long long>(quota.before),
            static_cast<unsigned long long>(quota.accepted), static_cast<unsigned long long>(quota.final),
            static_cast<unsigned long long>(quota.denied_before), static_cast<unsigned long long>(quota.denied_after),
            static_cast<long long>(quota.first_rc), quota.first_errno, static_cast<long long>(quota.second_rc), quota.second_errno,
            static_cast<long long>(quota.restore_rc), static_cast<unsigned>(quota.reservation_removed),
            static_cast<unsigned>(quota.child_consumed), static_cast<unsigned long long>(quota.inherited_soft),
            static_cast<unsigned long long>(quota.inherited_hard), static_cast<unsigned long long>(saved.rlim_cur),
            static_cast<unsigned long long>(saved.rlim_max), static_cast<unsigned long long>(quota.final_soft),
            static_cast<unsigned long long>(quota.final_hard), static_cast<long long>(rc));
        if (rc == 0 && !cpu_still(selected.cpu)) rc = -EPROTO;
        if (rc == 0) ++completed;
    }
    int64_t const POLICY_RESTORE = policy_set(original.mode, original.nodes);
    int64_t const AFFINITY_RESTORE = ker::multiproc::setThreadAffinityMask(TID, ORIGINAL_AFFINITY);
    bool const RESTORED = POLICY_RESTORE == 0 && policy_matches(io, original.mode, original.nodes) && AFFINITY_RESTORE == 0 &&
                          static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(TID)) == ORIGINAL_AFFINITY;
    bool const CLEAN = own.fds == 0 && own.complete && joined == 4 && children == 8;
    bool const PASS = rc == 0 && completed == 4 && CLEAN && RESTORED && before(deadline);
    std::printf(
        "NUMA_POLICY "
        "{\"type\":\"end\",\"nonce\":%llu,\"pass\":%u,\"modes\":%u,\"threads_joined\":%u,\"children_consumed\":%u,\"fds\":%u,\"own_"
        "complete\":%u,\"policy_restore\":%lld,\"affinity_restore\":%lld,\"error\":%lld,\"pressure\":0,\"huge\":0}\n",
        static_cast<unsigned long long>(opts.nonce), static_cast<unsigned>(PASS), completed, joined, children, own.fds,
        static_cast<unsigned>(own.complete), static_cast<long long>(POLICY_RESTORE), static_cast<long long>(AFFINITY_RESTORE),
        static_cast<long long>(rc));
    return PASS ? 0 : 1;
}

namespace {
// Public logical protocol only. Per-domain/all-owner physical closure is a
// separate qualification: empty page tables and ordinary actors remain visible.
constexpr uint64_t PRESSURE_MAX_BYTES = uint64_t{4} << 30;
constexpr uint32_t PRESSURE_MAX_ENTRIES = 4096;
constexpr size_t PRESSURE_SNAPSHOT_BYTES = 262144;
constexpr uint64_t PRESSURE_FNV_OFFSET = 14695981039346656037ULL;
constexpr uint64_t PRESSURE_FNV_PRIME = 1099511628211ULL;
struct PressureOptions {
    uint64_t nonce{};
    uint32_t domains{}, timeout_ms{}, fill_ms{}, probe_ms{};
};
struct PressureEntry {
    uint64_t address{}, bytes{}, digest{};
    int64_t allocate_rc{}, query_rc{}, release_rc{};
    uint32_t resident{}, zero_pages{}, unavailable{};
    uint16_t node{};
    bool owned{}, output_changed{};
};
struct PressureCheckpoint {
    ker::abi::resource::Usage usage{};
    uint64_t cpu{}, affinity{}, policy_mask{}, owned_bytes{}, stamp{};
    int64_t usage_rc{}, policy_rc{}, error{};
    uint32_t policy_mode{};
};
struct PressureSnapshot {
    std::array<char, PRESSURE_SNAPSHOT_BYTES> data{};
    uint64_t bytes{}, digest{}, stamp{};
    int fd{}, error{};
};
struct PressureState {
    PressureOptions options{};
    Topology topology{};
    QueryBuffers query{};
    abi::SelfPolicy original_policy{}, observed_policy{};
    Selection actor{};
    rlimit limits{};
    ker::abi::resource::Usage baseline{};
    std::array<PressureEntry, PRESSURE_MAX_ENTRIES> entries{};
    std::array<PressureCheckpoint, 8> checkpoints{};
    std::array<PressureSnapshot, 4> snapshots{};
    std::array<char, 2048> line{};
    uint64_t tid{}, pid{}, original_affinity{}, deadline{}, fill_deadline{}, work_deadline{}, owned_bytes{}, high_bytes{}, peak{};
    uint64_t attempted{}, released{}, requested_pages{}, released_pages{}, report_bytes{};
    int64_t first_error{}, cleanup_error{}, strict_rc{}, strict_output{}, large_rc{}, large_output{}, preferred_rc{}, recovery_rc{};
    int64_t policy_restore{}, affinity_restore{};
    uint32_t used{}, prefix{}, preferred_index{}, recovery_index{}, target{};
    bool prepared{}, strict{}, fallback{}, prefix_checked{}, payload_clean{}, recovered{}, restored{};
};
static PressureState pressure_state;

uint64_t pressure_hash(const char* bytes, size_t count) {
    uint64_t value = PRESSURE_FNV_OFFSET;
    for (size_t i = 0; i < count; ++i) value = (value ^ static_cast<unsigned char>(bytes[i])) * PRESSURE_FNV_PRIME;
    return value;
}
// A real callee frame warms the later small syscall-helper stack area. Keeping
// this call out of line prevents an optimizer from moving it into the caller's
// already allocated frame above the stack pointer.
[[gnu::noinline]] void pressure_prefault_stack() {
    volatile unsigned char bytes[32768];
    for (size_t i = 0; i < sizeof(bytes); ++i) bytes[i] = 0;
}
auto pressure_options(int argc, char** argv, PressureOptions& out) -> bool {
    out.timeout_ms = 300000;
    out.fill_ms = 180000;
    out.probe_ms = 5000;
    unsigned seen = 0;
    for (int i = 0; i < argc; ++i) {
        if (i + 1 >= argc) return false;
        const char* key = argv[i++];
        uint64_t value{};
        if (!number(argv[i], value)) return false;
        unsigned bit{};
        if (std::strcmp(key, "--nonce") == 0 && value) {
            bit = 1;
            out.nonce = value;
        } else if (std::strcmp(key, "--domains") == 0 && value == 2) {
            bit = 2;
            out.domains = 2;
        } else if (std::strcmp(key, "--timeout-ms") == 0 && value >= 60000 && value <= 300000) {
            bit = 4;
            out.timeout_ms = value;
        } else if (std::strcmp(key, "--fill-ms") == 0 && value >= 1 && value <= 180000) {
            bit = 8;
            out.fill_ms = value;
        } else if (std::strcmp(key, "--probe-ms") == 0 && value >= 1 && value <= 5000) {
            bit = 16;
            out.probe_ms = value;
        } else
            return false;
        if (seen & bit) return false;
        seen |= bit;
    }
    return (seen & 3) == 3;
}
void pressure_error(PressureState& state, int64_t error, bool cleanup = false) {
    if (error == 0) return;
    if (state.first_error == 0) state.first_error = error;
    if (cleanup && state.cleanup_error == 0) state.cleanup_error = error;
}
auto pressure_remaining(PressureState& state, uint64_t bound) -> bool { return before(bound) && before(state.deadline); }
auto pressure_checkpoint(PressureState& state, PressureCheckpoint& out, uint32_t mode, uint64_t mask) -> int64_t {
    initialize_transport(out);
    out.owned_bytes = state.owned_bytes;
    out.cpu = ker::multiproc::getcurrent_cpu();
    out.affinity = ker::multiproc::getThreadAffinityMask(state.tid);
    out.policy_rc = policy_read(state.query, state.observed_policy);
    out.policy_mode = state.observed_policy.mode;
    out.policy_mask = state.observed_policy.nodes;
    out.usage_rc = policy_usage(out.usage);
    if (!now_ns(out.stamp)) return out.error = -EIO;
    auto const& u = out.usage;
    state.peak = state.peak < state.baseline.current + state.high_bytes ? state.baseline.current + state.high_bytes : state.peak;
    bool const valid = out.usage_rc == 0 && out.policy_rc == 0 && u.reserved == 0 && out.cpu == state.actor.cpu &&
                       out.affinity == (uint64_t{1} << state.actor.cpu) && out.policy_mode == mode && out.policy_mask == mask &&
                       u.current == state.baseline.current + state.owned_bytes && u.peak == state.peak &&
                       u.denied == state.baseline.denied && u.reason == state.baseline.reason &&
                       u.limit.soft == state.baseline.limit.soft && u.limit.hard == state.baseline.limit.hard;
    return out.error = valid ? 0 : -EPROTO;
}
auto pressure_verify(PressureState& state, PressureEntry& entry, bool unavailable, bool foreign, uint64_t deadline) -> int64_t {
    uint32_t const pages = entry.bytes / PAGE;
    if (!unavailable) {
        entry.resident = entry.zero_pages = 0;
        entry.digest = PRESSURE_FNV_OFFSET;
    } else
        entry.unavailable = 0;
    for (uint32_t first = 0; first < pages; first += abi::MAX_PAGE_NODES) {
        if (!pressure_remaining(state, deadline)) return -ETIMEDOUT;
        uint32_t const count = pages - first < abi::MAX_PAGE_NODES ? pages - first : abi::MAX_PAGE_NODES;
        entry.query_rc = page_nodes(state.query, reinterpret_cast<void*>(entry.address + uint64_t{first} * PAGE), count);
        if (entry.query_rc != 0) return entry.query_rc;
        for (uint32_t i = 0; i < count; ++i) {
            auto const& row = state.query.pages[i];
            if (row.reserved32 != 0) return -EPROTO;
            if (unavailable) {
                if (row.status != static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE) || row.node != abi::INVALID_NODE) return -EPROTO;
                ++entry.unavailable;
                continue;
            }
            if (row.status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) || row.node >= 2 ||
                (foreign ? row.node == state.target : row.node != state.target))
                return -EPROTO;
            if (entry.resident != 0 && entry.node != row.node) return -EPROTO;
            entry.node = row.node;
            ++entry.resident;
            auto const* bytes = reinterpret_cast<volatile const unsigned char*>(entry.address + uint64_t{first + i} * PAGE);
            for (size_t j = 0; j < PAGE; ++j) {
                unsigned char const value = bytes[j];
                if (value != 0) return -EILSEQ;
                entry.digest = (entry.digest ^ value) * PRESSURE_FNV_PRIME;
            }
            ++entry.zero_pages;
        }
    }
    return pressure_remaining(state, deadline) ? 0 : -ETIMEDOUT;
}
auto pressure_allocate(PressureState& state, uint64_t bytes, uint64_t deadline, bool foreign, int64_t& rc, int64_t& sentinel) -> int64_t {
    rc = INT64_MIN;
    sentinel = -1;  // Explicit uncalled raw-output state on admission refusal.
    if (state.used >= PRESSURE_MAX_ENTRIES || bytes > PRESSURE_MAX_BYTES - state.owned_bytes) return -E2BIG;
    if (!pressure_remaining(state, deadline)) return -ETIMEDOUT;
    void* output = reinterpret_cast<void*>(uintptr_t{0x101});
    ++state.attempted;
    rc = ker::vmem::allocate(&output, bytes, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS);
    sentinel = output == reinterpret_cast<void*>(uintptr_t{0x101});  // Immediately copied raw result/output.
    if (rc != 0) {
        if (!pressure_remaining(state, deadline)) return -ETIMEDOUT;
        return sentinel ? rc : -EPROTO;
    }
    auto& entry = state.entries[state.used++];
    entry.address = reinterpret_cast<uint64_t>(output);
    entry.bytes = bytes;
    entry.allocate_rc = rc;
    entry.release_rc = INT64_MIN;
    entry.output_changed = !sentinel;
    entry.owned = true;  // Own before any query/validation can fail.
    state.owned_bytes += bytes;
    state.requested_pages += bytes / PAGE;
    if (state.owned_bytes > state.high_bytes) state.high_bytes = state.owned_bytes;
    if (sentinel || entry.address == 0 || entry.address % PAGE || entry.address > UINT64_MAX - bytes) return -EPROTO;
    return pressure_verify(state, entry, false, foreign, deadline);
}
void pressure_release(PressureState& state, uint32_t first, uint32_t end) {
    for (uint32_t i = first; i < end; ++i) {
        auto& entry = state.entries[i];
        if (!entry.owned) continue;
        bool const timely = before(state.deadline);
        entry.release_rc = ker::vmem::free(reinterpret_cast<void*>(entry.address), entry.bytes);
        if (entry.release_rc == 0) {
            entry.owned = false;
            state.owned_bytes -= entry.bytes;
            ++state.released;
            state.released_pages += entry.bytes / PAGE;
            pressure_error(state, pressure_verify(state, entry, true, false, state.deadline), true);
        } else
            pressure_error(state, entry.release_rc, true);
        if (!timely || !before(state.deadline)) pressure_error(state, -ETIMEDOUT, true);
    }
}
auto pressure_snapshot(PressureState& state, PressureSnapshot& snapshot) -> bool {
    while (snapshot.bytes < PRESSURE_SNAPSHOT_BYTES) {
        if (!before(state.deadline)) {
            snapshot.error = ETIMEDOUT;
            break;
        }
        ssize_t const got = read(snapshot.fd, snapshot.data.data() + snapshot.bytes, PRESSURE_SNAPSHOT_BYTES - snapshot.bytes);
        int const error = got < 0 ? errno : 0;
        if (got < 0) {
            if (error == EINTR) continue;
            snapshot.error = error;
            break;
        }
        if (got == 0) break;
        snapshot.bytes += static_cast<uint64_t>(got);
    }
    snapshot.digest = pressure_hash(snapshot.data.data(), snapshot.bytes);
    bool const complete = snapshot.error == 0 && snapshot.bytes > 0 && snapshot.bytes < PRESSURE_SNAPSHOT_BYTES &&
                          snapshot.data[snapshot.bytes - 1] == '\n' && now_ns(snapshot.stamp);
    int const closed = close(snapshot.fd);
    int const close_error = closed < 0 ? errno : 0;
    snapshot.fd = -1;
    if (closed != 0) snapshot.error = close_error;
    if (!complete || snapshot.error) pressure_error(state, snapshot.error ? -snapshot.error : -EPROTO);
    return complete && snapshot.error == 0;
}
// Reporting occurs only after payload release and recovery. Fixed formatting and
// raw write loops do not allocate any new measured backing during exhaustion.
auto pressure_write(const char* bytes, size_t count) -> bool {
    while (count) {
        ssize_t const put = write(STDOUT_FILENO, bytes, count);
        if (put < 0 && errno == EINTR) continue;
        if (put <= 0) return false;
        bytes += put;
        count -= static_cast<size_t>(put);
    }
    return true;
}
void pressure_report(PressureState& s) {
    auto emit = [&](int size) {
        if (size < 0 || static_cast<size_t>(size) >= s.line.size() || s.report_bytes + static_cast<uint64_t>(size) > (4U << 20)) {
            pressure_error(s, -E2BIG);
            return;
        }
        s.report_bytes += static_cast<uint64_t>(size);
        if (!pressure_write(s.line.data(), size)) pressure_error(s, -EIO);
    };
    emit(std::snprintf(s.line.data(), s.line.size(),
                       "NUMA_PRESSURE "
                       "{\"type\":\"begin\",\"version\":1,\"nonce\":%llu,\"domains\":2,\"pid\":%llu,\"tid\":%llu,\"timeout_ms\":%u,\"fill_"
                       "ms\":%u,\"probe_ms\":%u,\"cleanup_ms\":45000,\"max_bytes\":4294967296,\"max_entries\":4096}\n",
                       static_cast<unsigned long long>(s.options.nonce), static_cast<unsigned long long>(s.pid),
                       static_cast<unsigned long long>(s.tid), s.options.timeout_ms, s.options.fill_ms, s.options.probe_ms));
    for (unsigned i = 0; i < 2; ++i)
        emit(std::snprintf(s.line.data(), s.line.size(),
                           "NUMA_PRESSURE {\"type\":\"node\",\"nonce\":%llu,\"node\":%u,\"managed\":%llu,\"cpu_mask\":%llu}\n",
                           static_cast<unsigned long long>(s.options.nonce), i,
                           static_cast<unsigned long long>(s.topology.nodes[i].managed_bytes),
                           static_cast<unsigned long long>(s.topology.nodes[i].cpu_words[0])));
    constexpr const char* phases[]{"prepared",       "bind-prefix",     "strict-refusal", "preferred",
                                   "prefix-recheck", "payload-cleanup", "recovery"};
    for (unsigned i = 0; i < 7; ++i) {
        auto const& c = s.checkpoints[i];
        auto const& u = c.usage;
        emit(std::snprintf(s.line.data(), s.line.size(),
                           "NUMA_PRESSURE "
                           "{\"type\":\"%s\",\"nonce\":%llu,\"cpu\":%llu,\"affinity\":%llu,\"mode\":%u,\"mask\":%llu,\"target\":%u,\"owned_"
                           "bytes\":%llu,\"as_current\":%llu,\"as_peak\":%llu,\"as_denied\":%llu,\"as_reason\":%u,\"as_soft\":%llu,\"as_"
                           "hard\":%llu,\"usage_rc\":%lld,\"policy_rc\":%lld,\"error\":%lld,\"stamp\":%llu}\n",
                           phases[i], static_cast<unsigned long long>(s.options.nonce), static_cast<unsigned long long>(c.cpu),
                           static_cast<unsigned long long>(c.affinity), c.policy_mode, static_cast<unsigned long long>(c.policy_mask),
                           s.target, static_cast<unsigned long long>(c.owned_bytes), static_cast<unsigned long long>(u.current),
                           static_cast<unsigned long long>(u.peak), static_cast<unsigned long long>(u.denied),
                           static_cast<unsigned>(u.reason), static_cast<unsigned long long>(u.limit.soft),
                           static_cast<unsigned long long>(u.limit.hard), static_cast<long long>(c.usage_rc),
                           static_cast<long long>(c.policy_rc), static_cast<long long>(c.error), static_cast<unsigned long long>(c.stamp)));
    }
    for (uint32_t i = 0; i < s.used; ++i) {
        auto const& e = s.entries[i];
        emit(std::snprintf(s.line.data(), s.line.size(),
                           "NUMA_PRESSURE "
                           "{\"type\":\"entry\",\"nonce\":%llu,\"index\":%u,\"role\":\"%s\",\"address\":%llu,\"bytes\":%llu,\"node\":%u,"
                           "\"resident\":%u,\"zero_pages\":%u,\"unavailable\":%u,\"digest\":%llu,\"allocate_rc\":%lld,\"release_rc\":%lld,"
                           "\"output_changed\":%u,\"owned\":%u}\n",
                           static_cast<unsigned long long>(s.options.nonce), i,
                           i < s.prefix             ? "bind"
                           : i == s.preferred_index ? "preferred"
                                                    : "recovery",
                           static_cast<unsigned long long>(e.address), static_cast<unsigned long long>(e.bytes), e.node, e.resident,
                           e.zero_pages, e.unavailable, static_cast<unsigned long long>(e.digest), static_cast<long long>(e.allocate_rc),
                           static_cast<long long>(e.release_rc), static_cast<unsigned>(e.output_changed), static_cast<unsigned>(e.owned)));
    }
    bool const pass = s.first_error == 0 && s.prepared && s.strict && s.fallback && s.prefix_checked && s.payload_clean && s.recovered &&
                      s.restored && s.owned_bytes == 0 && before(s.deadline);
    emit(std::snprintf(
        s.line.data(), s.line.size(),
        "NUMA_PRESSURE "
        "{\"type\":\"final\",\"nonce\":%llu,\"pass\":%u,\"first_error\":%lld,\"cleanup_error\":%lld,\"large_rc\":%lld,\"large_sentinel\":%"
        "lld,\"strict_rc\":%lld,\"strict_sentinel\":%lld,\"preferred_rc\":%lld,\"recovery_rc\":%lld,\"attempted\":%llu,\"released\":%llu,"
        "\"outstanding\":%llu,\"high_bytes\":%llu,\"policy_restore\":%lld,\"affinity_restore\":%lld,\"original_mode\":%u,\"original_mask\":"
        "%llu,\"original_affinity\":%llu,\"prefix_entries\":%u,\"physical_closure\":\"OPEN\",\"metadata_cause\":\"UNPROVED\",\"final_as_"
        "current\":%llu,\"final_as_peak\":%llu,\"final_as_denied\":%llu,\"final_usage_rc\":%lld,\"final_checkpoint_error\":%lld}\n",
        static_cast<unsigned long long>(s.options.nonce), static_cast<unsigned>(pass), static_cast<long long>(s.first_error),
        static_cast<long long>(s.cleanup_error), static_cast<long long>(s.large_rc), static_cast<long long>(s.large_output),
        static_cast<long long>(s.strict_rc), static_cast<long long>(s.strict_output), static_cast<long long>(s.preferred_rc),
        static_cast<long long>(s.recovery_rc), static_cast<unsigned long long>(s.attempted), static_cast<unsigned long long>(s.released),
        static_cast<unsigned long long>(s.owned_bytes), static_cast<unsigned long long>(s.high_bytes),
        static_cast<long long>(s.policy_restore), static_cast<long long>(s.affinity_restore), s.original_policy.mode,
        static_cast<unsigned long long>(s.original_policy.nodes), static_cast<unsigned long long>(s.original_affinity), s.prefix,
        static_cast<unsigned long long>(s.checkpoints[7].usage.current), static_cast<unsigned long long>(s.checkpoints[7].usage.peak),
        static_cast<unsigned long long>(s.checkpoints[7].usage.denied), static_cast<long long>(s.checkpoints[7].usage_rc),
        static_cast<long long>(s.checkpoints[7].error)));
    for (unsigned i = 0; i < 4; ++i) {
        auto const& b = s.snapshots[i];
        emit(std::snprintf(s.line.data(), s.line.size(),
                           "NUMA_PRESSURE_RAW version=1 nonce=%llu phase=%s kind=%s bytes=%llu fnv1a64=%llu stamp=%llu capture=%u\n",
                           static_cast<unsigned long long>(s.options.nonce), i < 2 ? "before" : "after", i % 2 ? "zones" : "summary",
                           static_cast<unsigned long long>(b.bytes), static_cast<unsigned long long>(b.digest),
                           static_cast<unsigned long long>(b.stamp), i + 1));
        if (s.report_bytes + b.bytes > (4U << 20))
            pressure_error(s, -E2BIG);
        else {
            s.report_bytes += b.bytes;
            if (!pressure_write(b.data.data(), b.bytes)) pressure_error(s, -EIO);
        }
        emit(std::snprintf(s.line.data(), s.line.size(), "NUMA_PRESSURE_RAW_END nonce=%llu capture=%u\n",
                           static_cast<unsigned long long>(s.options.nonce), i + 1));
    }
}
}  // namespace

auto run_numa_pressure(int argc, char** argv) -> int {
    auto& s = pressure_state;
    initialize_transport(s);  // All fixed buffers and ledger become resident before pressure.
    for (auto& snap : s.snapshots) snap.fd = -1;
    s.preferred_index = s.recovery_index = UINT32_MAX;
    s.large_rc = s.strict_rc = s.preferred_rc = s.recovery_rc = s.policy_restore = s.affinity_restore = INT64_MIN;
    s.large_output = s.strict_output = -1;
    if (!pressure_options(argc, argv, s.options)) return 2;
    pressure_prefault_stack();
    (void)setvbuf(stdout, nullptr, _IONBF, 0);
    s.pid = getpid();
    s.tid = ker::multiproc::currentThreadId();
    if (!s.pid || s.pid != s.tid || raw_error(s.tid) || !deadline_after(s.options.timeout_ms, s.deadline)) return 2;
    s.work_deadline = s.deadline - 45000 * NS_PER_MS;
    uint64_t now{};
    if (!now_ns(now)) return 2;
    s.fill_deadline = now + uint64_t{s.options.fill_ms} * NS_PER_MS;
    if (s.fill_deadline > s.work_deadline) s.fill_deadline = s.work_deadline;
    int64_t rc = policy_topology(s.query, s.topology, 2);
    s.original_affinity = ker::multiproc::getThreadAffinityMask(s.tid);
    bool saved = false;
    uint64_t const online =
        rc == 0 ? (s.topology.summary.cpu_count == 64 ? UINT64_MAX : (uint64_t{1} << s.topology.summary.cpu_count) - 1) : 0;
    if (rc == 0 && (!s.original_affinity || (s.original_affinity & ~online))) rc = -EPROTO;
    if (rc == 0) rc = policy_read(s.query, s.original_policy);
    if (rc == 0) {
        saved = true;
        rc = pin_current_cpu(s.topology, s.actor, s.work_deadline);
    }
    s.target = 1 - s.actor.node;
    if (rc == 0) {
        int const got = getrlimit(RLIMIT_AS, &s.limits);
        rc = got == 0 ? 0 : -errno;
    }
    const char* files[]{"/proc/memacc/summary", "/proc/memacc/zones", "/proc/memacc/summary", "/proc/memacc/zones"};
    for (unsigned i = 0; i < 4 && rc == 0; ++i) {
        s.snapshots[i].fd = open(files[i], O_RDONLY | O_CLOEXEC);
        if (s.snapshots[i].fd < 0) rc = -errno;
    }
    if (rc == 0) rc = policy_set(2, uint64_t{1} << s.target);
    if (rc == 0 && !policy_matches(s.query, 2, uint64_t{1} << s.target)) rc = -EPROTO;
    if (rc == 0 && (!pressure_snapshot(s, s.snapshots[0]) || !pressure_snapshot(s, s.snapshots[1]))) rc = s.first_error;
    if (rc == 0) rc = policy_usage(s.baseline);
    s.peak = s.baseline.peak;
    if (rc == 0 &&
        (s.baseline.reserved || static_cast<uint32_t>(s.baseline.reason) > 3 || s.baseline.denied == UINT64_MAX ||
         s.baseline.limit.soft != s.limits.rlim_cur || s.baseline.limit.hard != s.limits.rlim_max ||
         s.baseline.current > UINT64_MAX - PRESSURE_MAX_BYTES || s.baseline.current + PRESSURE_MAX_BYTES > s.baseline.limit.soft ||
         s.baseline.current + PRESSURE_MAX_BYTES > s.baseline.limit.hard))
        rc = -ENOSPC;
    if (rc == 0) rc = pressure_checkpoint(s, s.checkpoints[0], 2, uint64_t{1} << s.target);
    s.prepared = rc == 0;
    uint64_t chunk = uint64_t{2} << 20;
    int64_t sentinel{};
    while (rc == 0) {
        if (s.used >= PRESSURE_MAX_ENTRIES - 2 || s.owned_bytes > PRESSURE_MAX_BYTES - 8192 - chunk) {
            rc = -E2BIG;
            break;
        }
        int64_t raw{};
        int64_t const attempt = pressure_allocate(s, chunk, s.fill_deadline, false, raw, sentinel);
        if (attempt == -ENOMEM && raw == -ENOMEM && sentinel) {
            if (chunk != PAGE) {
                s.large_rc = raw;
                s.large_output = sentinel;
                chunk = PAGE;
                continue;
            }
            s.strict_rc = raw;
            s.strict_output = sentinel;
            s.strict = true;
            break;
        }
        if (attempt != 0) {
            rc = attempt;
            break;
        }
    }
    s.prefix = s.used;
    if (rc == 0) rc = pressure_checkpoint(s, s.checkpoints[1], 2, uint64_t{1} << s.target);
    if (rc == 0) rc = pressure_checkpoint(s, s.checkpoints[2], 2, uint64_t{1} << s.target);
    if (rc == 0 && (!s.strict || !s.prefix)) rc = -EPROTO;
    uint64_t probe_deadline{};
    if (rc == 0) rc = policy_set(1, uint64_t{1} << s.target);
    if (rc == 0 && !policy_matches(s.query, 1, uint64_t{1} << s.target)) rc = -EPROTO;
    if (rc == 0) {
        s.preferred_index = s.used;
        if (!deadline_after(s.options.probe_ms, probe_deadline))
            rc = -EIO;
        else {
            if (probe_deadline > s.work_deadline) probe_deadline = s.work_deadline;
            rc = pressure_allocate(s, PAGE, probe_deadline, true, s.preferred_rc, sentinel);
            s.fallback = rc == 0;
        }
    }
    if (rc == 0) rc = pressure_checkpoint(s, s.checkpoints[3], 1, uint64_t{1} << s.target);
    for (uint32_t i = 0; i < s.prefix && rc == 0; ++i) rc = pressure_verify(s, s.entries[i], false, false, s.work_deadline);
    if (rc == 0) rc = pressure_checkpoint(s, s.checkpoints[4], 1, uint64_t{1} << s.target);
    s.prefix_checked = rc == 0;
    pressure_error(s, rc);
    pressure_release(s, 0, s.used);
    if (s.prepared && s.first_error == 0) pressure_error(s, pressure_checkpoint(s, s.checkpoints[5], 1, uint64_t{1} << s.target));
    s.payload_clean = s.first_error == 0 && s.owned_bytes == 0;
    if (s.payload_clean) {
        rc = policy_set(2, uint64_t{1} << s.target);
        if (rc == 0 && !policy_matches(s.query, 2, uint64_t{1} << s.target)) rc = -EPROTO;
        s.recovery_index = s.used;
        if (rc == 0 && !deadline_after(s.options.probe_ms, probe_deadline)) rc = -EIO;
        if (probe_deadline > s.work_deadline) probe_deadline = s.work_deadline;
        if (rc == 0) rc = pressure_allocate(s, PAGE, probe_deadline, false, s.recovery_rc, sentinel);
        if (rc == 0) rc = pressure_checkpoint(s, s.checkpoints[6], 2, uint64_t{1} << s.target);
        s.recovered = rc == 0;
        pressure_error(s, rc);
    }
    if (s.recovery_index < s.used) pressure_release(s, s.recovery_index, s.used);
    if (s.recovered && s.first_error == 0) pressure_error(s, pressure_checkpoint(s, s.checkpoints[7], 2, uint64_t{1} << s.target));
    if (saved && s.owned_bytes == 0) {
        s.policy_restore = policy_set(s.original_policy.mode, s.original_policy.nodes);
        s.affinity_restore = ker::multiproc::setThreadAffinityMask(s.tid, s.original_affinity);
        s.restored = s.policy_restore == 0 && policy_matches(s.query, s.original_policy.mode, s.original_policy.nodes) &&
                     s.affinity_restore == 0 && static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(s.tid)) == s.original_affinity;
        if (!s.restored) pressure_error(s, -EPROTO, true);
    }
    if (s.owned_bytes == 0 && s.snapshots[2].fd >= 0) (void)pressure_snapshot(s, s.snapshots[2]);
    if (s.owned_bytes == 0 && s.snapshots[3].fd >= 0) (void)pressure_snapshot(s, s.snapshots[3]);
    for (auto& snap : s.snapshots)
        if (snap.fd >= 0) {
            int const ret = close(snap.fd);
            int const err = ret < 0 ? errno : 0;
            snap.fd = -1;
            if (ret != 0) pressure_error(s, -err, true);
        }
    rlimit final_limits;
    initialize_transport(final_limits);
    int const final_limit_result = getrlimit(RLIMIT_AS, &final_limits);
    int const final_limit_error = final_limit_result < 0 ? errno : 0;
    if (final_limit_result != 0)
        pressure_error(s, final_limit_error ? -final_limit_error : -EPROTO, true);
    else if (final_limits.rlim_cur != s.limits.rlim_cur || final_limits.rlim_max != s.limits.rlim_max)
        pressure_error(s, -EPROTO, true);
    if (!before(s.deadline)) pressure_error(s, -ETIMEDOUT);
    pressure_report(s);
    if (!before(s.deadline)) pressure_error(s, -ETIMEDOUT);
    return s.first_error == 0 ? 0 : 1;
}
