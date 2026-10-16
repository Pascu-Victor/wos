#include "numa_locality.hpp"

#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/numa.h>
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
auto transfer(int fd, void* buffer, size_t bytes, bool writing, uint64_t deadline) -> bool {
    auto* p = static_cast<uint8_t*>(buffer);
    size_t done = 0;
    while (done != bytes) {
        if (!before(deadline)) {
            errno = ETIMEDOUT;
            return false;
        }
        ssize_t n = writing ? write(fd, p + done, bytes - done) : read(fd, p + done, bytes - done);
        if (n > 0) {
            if (static_cast<size_t>(n) > bytes - done) {
                errno = EPROTO;
                return false;
            }
            done += static_cast<size_t>(n);
            continue;
        }
        if (n == 0) {
            errno = EPIPE;
            return false;
        }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return false;
        uint64_t now;
        if (!now_ns(now) || now >= deadline) {
            errno = ETIMEDOUT;
            return false;
        }
        uint64_t ms = (deadline - now + NS_PER_MS - 1) / NS_PER_MS;
        pollfd poll_fd{fd, static_cast<short>(writing ? POLLOUT : POLLIN), 0};
        int rc = poll(&poll_fd, 1, static_cast<int>(ms > 100 ? 100 : ms));
        if (rc < 0 && errno != EINTR) return false;
        // HUP can accompany unread final bytes; retry read before treating EOF.
        if (rc > 0 && (poll_fd.revents & (POLLERR | POLLNVAL))) {
            errno = EIO;
            return false;
        }
    }
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
auto terminate_and_reap(pid_t pid, bool& owned) -> bool {
    if (!owned) return true;
    (void)kill(pid, SIGKILL);  // Still consume the exact child even if signaling fails.
    int status;
    for (;;) {
        pid_t rc = waitpid(pid, &status, 0);
        if (rc == pid) {
            owned = false;
            return true;
        }
        if (rc < 0 && errno == EINTR) continue;
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
    std::printf("NUMA_LIFECYCLE_PLACEMENT parent=%u:%u child=observed_current_cpu method=child_same_owner_pin\n", parent_selection.node,
                parent_selection.cpu);
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
                int child_error = child_cycle(mapping, fds[1], opts, topo, cycle, child_deadline);
                if (close(fds[1]) != 0 && child_error == 0) child_error = 64;
                _exit(child_error);
            }
            if (pid < 0)
                ok = false;
            else
                owned = true;
        }
        if (!close_fd(fds[1], own)) ok = false;
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
        bool const reaped = terminate_and_reap(pid, owned);
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
                "map_query_valid=%u map_query=%lld mapping_absent=%u own_fds=%u own_maps=%u own_complete=%u\n",
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
                static_cast<unsigned>(mapping_absent), own.fds, own.maps, static_cast<unsigned>(own.complete));
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
