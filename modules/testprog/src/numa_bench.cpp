#include "numa_bench.hpp"

#include <pthread.h>
#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/numa.h>
#include <sys/vmem.h>
#include <time.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <new>
#include <type_traits>

#include "numa_bench_core.hpp"

namespace {
namespace abi = ker::abi::vmem::numa;
using numa_bench::Options;
constexpr size_t PAGE = numa_bench::PAGE;
constexpr size_t MAX_ARMS = 20;
struct Topology {
    abi::Summary summary{};
    std::array<abi::Node, 64> nodes{};
    uint16_t chase{}, local{}, remote{};
};
struct Io {
    abi::QueryRequest request{};
    std::array<abi::PageNode, abi::MAX_PAGE_NODES> rows{};
};
template <class T>
void transport(T& object) {
    static_assert(std::is_trivially_copyable_v<T>);
    auto* bytes = reinterpret_cast<volatile uint8_t*>(&object);
    for (size_t i = 0; i < sizeof(T); ++i) bytes[i] = 0;
}
auto now_ns(uint64_t& value) -> bool {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t) != 0 || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= 1000000000L ||
        static_cast<uint64_t>(t.tv_sec) > (UINT64_MAX - static_cast<uint64_t>(t.tv_nsec)) / 1000000000ULL)
        return false;
    value = static_cast<uint64_t>(t.tv_sec) * 1000000000ULL + static_cast<uint64_t>(t.tv_nsec);
    return true;
}
auto before(uint64_t deadline) -> bool {
    uint64_t now;
    return now_ns(now) && now < deadline;
}
auto yield() -> bool { return !numa_bench::raw_error(ker::multiproc::yield()); }
auto resolution(uint64_t& ns) -> bool {
    timespec t{};
    if (clock_getres(CLOCK_MONOTONIC, &t) != 0 || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= 1000000000L ||
        static_cast<uint64_t>(t.tv_sec) > (UINT64_MAX - static_cast<uint64_t>(t.tv_nsec)) / 1000000000ULL)
        return false;
    ns = static_cast<uint64_t>(t.tv_sec) * 1000000000ULL + static_cast<uint64_t>(t.tv_nsec);
    return ns != 0;
}
auto query(Io& io, abi::QueryKind kind, void* output, uint32_t bytes, uint64_t index = 0, uint64_t address = 0, uint32_t count = 0)
    -> int64_t {
    transport(io.request);
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
auto local_policy(Io& io) -> int64_t {
    abi::PolicyRequest request;
    transport(request);
    request.version = abi::VERSION;
    request.bytes = sizeof(request);
    request.mode = static_cast<uint32_t>(abi::PolicyMode::LOCAL);
    auto rc = ker::vmem::set_memory_policy(request);
    if (rc != 0) return rc;
    abi::SelfPolicy policy;
    transport(policy);
    rc = query(io, abi::QueryKind::SELF_POLICY, &policy, sizeof(policy));
    if (rc != 0) return rc;
    return policy.version == abi::VERSION && policy.bytes == sizeof(policy) &&
                   policy.mode == static_cast<uint32_t>(abi::PolicyMode::LOCAL) && policy.nodes == 0 && policy.reserved32 == 0 &&
                   policy.reserved64 == 0
               ? 0
               : -EPROTO;
}
auto cpu_node(const Topology& topo, uint32_t cpu, uint16_t& output) -> bool {
    if (cpu >= 64 || cpu >= topo.summary.cpu_count) return false;
    for (uint16_t node = 0; node < topo.summary.node_count; ++node)
        if ((topo.nodes[node].cpu_words[0] & (uint64_t{1} << cpu)) != 0) {
            if (topo.nodes[node].managed_bytes == 0) return false;
            output = node;
            return true;
        }
    return false;
}
auto topology(const Options& options, Topology& topo, uint64_t deadline) -> int64_t {
    if (!before(deadline)) return -ETIMEDOUT;
    Io io;
    transport(io);
    transport(topo.summary);
    auto rc = query(io, abi::QueryKind::SUMMARY, &topo.summary, sizeof(topo.summary));
    if (rc != 0) return rc;
    auto const& s = topo.summary;
    if (s.version != abi::VERSION || s.bytes != sizeof(s) || s.features != abi::FEATURES_V1 || s.node_count < 2 || s.node_count > 64 ||
        s.cpu_count == 0 || s.cpu_count > 256 || s.page_bytes != PAGE || s.max_page_nodes != abi::MAX_PAGE_NODES ||
        s.cpu_mask_words != abi::CPU_MASK_WORDS || s.provenance > static_cast<uint32_t>(abi::Provenance::UMA_REJECTED_FIRMWARE) ||
        s.reserved64[0] || s.reserved64[1] || s.reserved64[2])
        return -EPROTO;
    uint64_t seen[4]{};
    for (uint16_t n = 0; n < s.node_count; ++n) {
        if (!before(deadline)) return -ETIMEDOUT;
        auto& row = topo.nodes[n];
        transport(row);
        rc = query(io, abi::QueryKind::NODE, &row, sizeof(row), n);
        if (rc != 0) return rc;
        if (row.version != abi::VERSION || row.bytes != sizeof(row) || row.node != n || row.reserved16 || row.reserved64 ||
            row.managed_bytes % PAGE)
            return -EPROTO;
        for (uint32_t word = 0; word < 4; ++word) {
            if (seen[word] & row.cpu_words[word]) return -EPROTO;
            seen[word] |= row.cpu_words[word];
            for (uint32_t bit = 0; bit < 64; ++bit)
                if ((row.cpu_words[word] & (uint64_t{1} << bit)) != 0 && word * 64 + bit >= s.cpu_count) return -EPROTO;
        }
    }
    for (uint32_t cpu = 0; cpu < s.cpu_count; ++cpu)
        if ((seen[cpu / 64] & (uint64_t{1} << (cpu % 64))) == 0) return -EPROTO;
    if (!cpu_node(topo, options.chase_cpu, topo.chase) || !cpu_node(topo, options.local_init_cpu, topo.local) ||
        !cpu_node(topo, options.remote_init_cpu, topo.remote))
        return -ENOTSUP;
    if (!before(deadline)) return -ETIMEDOUT;
    return topo.local == topo.chase && topo.remote != topo.chase ? 0 : -EINVAL;
}
auto place_thread(uint64_t tid, uint32_t cpu, uint64_t deadline) -> int64_t {
    if (tid == 0 || numa_bench::raw_error(tid) || cpu >= 64) return -EINVAL;
    while (before(deadline)) {
        auto const rc = ker::multiproc::setThreadAffinityMask(tid, uint64_t{1} << cpu);
        if (rc == -EBUSY) {
            if (!yield()) return -EIO;
            continue;
        }
        if (rc != 0) return rc;
        auto const observed = static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(tid));
        if (numa_bench::exact_mask(observed, cpu)) return 0;
        if (observed != static_cast<uint64_t>(-EBUSY)) return numa_bench::raw_error(observed) ? static_cast<int64_t>(observed) : -EPROTO;
        if (!yield()) return -EIO;
    }
    return -ETIMEDOUT;
}
auto cpu_observation(uint32_t cpu, uint64_t& observed_cpu, uint64_t& mask) -> bool {
    auto const tid = ker::multiproc::currentThreadId();
    if (tid == 0 || numa_bench::raw_error(tid)) return false;
    observed_cpu = ker::multiproc::getcurrent_cpu();
    mask = static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(tid));
    return observed_cpu == cpu && numa_bench::exact_mask(mask, cpu);
}
auto await_cpu(uint32_t cpu, uint64_t deadline, const std::atomic<bool>& cancel) -> int64_t {
    auto const tid = ker::multiproc::currentThreadId();
    if (tid == 0 || numa_bench::raw_error(tid)) return -EINVAL;
    while (!cancel.load(std::memory_order_acquire) && before(deadline)) {
        auto const mask = static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(tid));
        if (numa_bench::exact_mask(mask, cpu)) {
            auto const actual = ker::multiproc::getcurrent_cpu();
            if (actual == cpu) return 0;
            if (numa_bench::raw_error(actual)) return -EIO;
        } else if (numa_bench::raw_error(mask) && mask != static_cast<uint64_t>(-EBUSY))
            return static_cast<int64_t>(mask);
        if (!yield()) return -EIO;
    }
    return -ETIMEDOUT;
}
struct Arm {
    uint32_t pair{};
    bool remote{};
    void* mapping{};
    uint32_t start_index{}, expected_index{};
    uint64_t virgin_pages{}, warm_checksum{}, checksum{}, elapsed_ns{};
    uint64_t before_histogram[64]{}, after_histogram[64]{};
    uint64_t cpu_before{UINT64_MAX}, cpu_after{UINT64_MAX}, mask_before{}, mask_after{};
    uint64_t init_cpu_before{UINT64_MAX}, init_cpu_after{UINT64_MAX}, init_mask_before{}, init_mask_after{};
    int64_t error{};
    const char* phase = "unstarted";
    bool sampled{}, unmapped{};
};
struct Team;
struct Worker {
    Team* team{};
    uint32_t role{}, cpu{};
    pthread_t thread{};
    bool created{}, joined{};
    pthread_mutex_t mutex{};
    pthread_cond_t condition{};
    bool mutex_initialized{}, condition_initialized{};
    std::atomic<uint64_t> tid{};
    std::atomic<bool> started{}, placed{}, ready{}, done{};
    std::atomic<uint32_t> command{}, completed{};
    Arm* job{};
    std::atomic<int64_t> error{};
};
struct Team {
    const Options& options;
    const Topology& topo;
    uint64_t deadline;
    std::atomic<bool> cancel{};
    Worker workers[3]{};
};
auto active(const Team& team) -> bool { return !team.cancel.load(std::memory_order_acquire) && before(team.deadline); }
auto placement_rows(Worker& worker, Arm& arm, Io& io, bool virgin, uint64_t* histogram) -> int64_t {
    auto& team = *worker.team;
    uint16_t const expected = arm.remote ? team.topo.remote : team.topo.local;
    uint32_t const pages = static_cast<uint32_t>(team.options.bytes / PAGE);
    for (uint32_t first = 0; first < pages; first += abi::MAX_PAGE_NODES) {
        if (!active(team)) return -ETIMEDOUT;
        uint32_t const count = pages - first < abi::MAX_PAGE_NODES ? pages - first : abi::MAX_PAGE_NODES;
        transport(io.rows);
        for (auto& row : io.rows) {
            row.node = abi::INVALID_NODE;
            row.status = static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE);
        }
        auto const rc = query(io, abi::QueryKind::PAGE_NODES, io.rows.data(), count * sizeof(abi::PageNode), 0,
                              reinterpret_cast<uint64_t>(arm.mapping) + static_cast<uint64_t>(first) * PAGE, count);
        if (rc != 0) return rc;
        for (uint32_t i = 0; i < count; ++i) {
            auto const& row = io.rows[i];
            if (row.reserved32 != 0) return -EPROTO;
            if (virgin) {
                if (row.node != abi::INVALID_NODE || row.status != static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE)) return -EPROTO;
                ++arm.virgin_pages;
            } else {
                if (row.status != static_cast<uint16_t>(abi::PageStatus::RESIDENT) || row.node >= team.topo.summary.node_count)
                    return -EPROTO;
                ++histogram[row.node];
                if (row.node != expected) return -ERANGE;
            }
        }
    }
    return 0;
}
auto initialize_arm(Worker& worker, Arm& arm, Io& io) -> int64_t {
    auto& team = *worker.team;
    arm.phase = "initializer_cpu_before";
    if (!cpu_observation(worker.cpu, arm.init_cpu_before, arm.init_mask_before)) return -EPROTO;
    arm.phase = "initializer_policy";
    auto rc = local_policy(io);
    if (rc != 0) return rc;
    arm.phase = "virgin_query";
    rc = placement_rows(worker, arm, io, true, nullptr);
    if (rc != 0) return rc;
    arm.phase = "permutation_scratch";
    uint32_t const elements = static_cast<uint32_t>(team.options.bytes / numa_bench::STRIDE);
    auto* indices = new (std::nothrow) uint32_t[elements];
    if (indices == nullptr) return -ENOMEM;
    numa_bench::permutation(indices, elements, team.options.seed);
    arm.start_index = indices[0];
    arm.expected_index = indices[team.options.loads % elements];
    arm.phase = "full_payload_initialization";
    if (!active(team)) {
        delete[] indices;
        return -ETIMEDOUT;
    }
    numa_bench::initialize_cycle(arm.mapping, static_cast<size_t>(team.options.bytes), indices, elements);
    delete[] indices;
    arm.phase = "initializer_cpu_after";
    if (!cpu_observation(worker.cpu, arm.init_cpu_after, arm.init_mask_after)) return -EPROTO;
    // Verify placement on the writing actor before handing the immutable
    // payload to the chase worker; the chase repeats its full before query.
    uint64_t initialized_histogram[64]{};
    arm.phase = "initializer_placement";
    rc = placement_rows(worker, arm, io, false, initialized_histogram);
    if (rc != 0) return rc;
    return active(team) ? 0 : -ETIMEDOUT;
}
auto measure_arm(Worker& worker, Arm& arm, Io& io) -> int64_t {
    auto& team = *worker.team;
    arm.phase = "before_placement";
    auto rc = placement_rows(worker, arm, io, false, arm.before_histogram);
    if (rc != 0) return rc;
    uintptr_t const base = reinterpret_cast<uintptr_t>(arm.mapping);
    uintptr_t const start = base + static_cast<uintptr_t>(arm.start_index) * numa_bench::STRIDE;
    arm.phase = "equal_full_cycle_warmup";
    auto const warm = numa_bench::dependent_loads(start, team.options.bytes / numa_bench::STRIDE);
    if (!numa_bench::consumed_index(base, team.options.bytes, warm, arm.warm_checksum) || arm.warm_checksum != arm.start_index)
        return -EILSEQ;
    arm.phase = "measured_cpu_before";
    if (!active(team)) return -ETIMEDOUT;
    if (!cpu_observation(worker.cpu, arm.cpu_before, arm.mask_before)) return -EPROTO;
    uint64_t begin, end;
    if (!now_ns(begin)) return -EIO;
    // The only measured workload is this finite volatile dependent chain.
    auto const consumed = numa_bench::dependent_loads(start, team.options.loads);
    if (!now_ns(end) || end <= begin) return -EIO;
    arm.elapsed_ns = end - begin;
    arm.phase = "measured_cpu_after";
    if (!cpu_observation(worker.cpu, arm.cpu_after, arm.mask_after)) return -EPROTO;
    if (!numa_bench::consumed_index(base, team.options.bytes, consumed, arm.checksum) || arm.checksum != arm.expected_index) return -EILSEQ;
    arm.phase = "after_placement";
    rc = placement_rows(worker, arm, io, false, arm.after_histogram);
    if (rc != 0) return rc;
    arm.phase = "complete";
    return active(team) ? 0 : -ETIMEDOUT;
}
auto thread_main(void* arg) -> void* {
    auto& worker = *static_cast<Worker*>(arg);
    auto& team = *worker.team;
    Io io;
    transport(io);
    auto const tid = ker::multiproc::currentThreadId();
    worker.tid.store(tid, std::memory_order_relaxed);
    worker.started.store(true, std::memory_order_release);
    if (tid == 0 || numa_bench::raw_error(tid)) worker.error = -EINVAL;
    while (worker.error == 0 && !worker.placed.load(std::memory_order_acquire)) {
        if (!active(team)) {
            worker.error = -ECANCELED;
            break;
        }
        if (!yield()) {
            worker.error = -EIO;
            break;
        }
    }
    if (worker.error == 0) worker.error = await_cpu(worker.cpu, team.deadline, team.cancel);
    if (worker.error == 0) worker.error = local_policy(io);
    worker.ready.store(true, std::memory_order_release);
    uint32_t previous = 0;
    while (worker.error == 0 && active(team)) {
        int rc = pthread_mutex_lock(&worker.mutex);
        if (rc != 0) {
            worker.error = -rc;
            break;
        }
        while (worker.command.load(std::memory_order_acquire) == previous && active(team)) {
            timespec const limit{static_cast<time_t>(team.deadline / 1000000000ULL), static_cast<long>(team.deadline % 1000000000ULL)};
            rc = pthread_cond_clockwait(&worker.condition, &worker.mutex, CLOCK_MONOTONIC, &limit);
            if (rc != 0) {
                worker.error = -rc;
                break;
            }
        }
        uint32_t const command = worker.command.load(std::memory_order_acquire);
        Arm* const job = worker.job;
        int const unlocked = pthread_mutex_unlock(&worker.mutex);
        if (unlocked != 0 && worker.error == 0) worker.error = -unlocked;
        if (worker.error != 0 || !active(team)) break;
        if (command == previous || job == nullptr) {
            worker.error = -EPROTO;
            break;
        }
        Arm& arm = *job;
        worker.error = worker.role == 0 ? measure_arm(worker, arm, io) : initialize_arm(worker, arm, io);
        arm.error = worker.error.load(std::memory_order_acquire);
        previous = command;
        worker.completed.store(command, std::memory_order_release);
        rc = pthread_mutex_lock(&worker.mutex);
        if (rc != 0) {
            worker.error = -rc;
            break;
        }
        int const signaled = pthread_cond_broadcast(&worker.condition);
        int const released = pthread_mutex_unlock(&worker.mutex);
        if (signaled != 0) worker.error = -signaled;
        if (released != 0) worker.error = -released;
    }
    worker.done.store(true, std::memory_order_release);
    return nullptr;
}
auto await_flag(Team& team, Worker& worker, std::atomic<bool>& flag) -> bool {
    while (!flag.load(std::memory_order_acquire)) {
        if (!active(team) || worker.done.load(std::memory_order_acquire) || !yield()) return false;
    }
    return true;
}
auto dispatch(Team& team, Worker& worker, Arm& arm) -> int64_t {
    if (!active(team) || worker.done.load(std::memory_order_acquire)) return -ETIMEDOUT;
    int rc = pthread_mutex_lock(&worker.mutex);
    if (rc != 0) return -rc;
    uint32_t const command = worker.command.load(std::memory_order_relaxed) + 1;
    worker.job = &arm;
    worker.command.store(command, std::memory_order_release);
    int64_t status = 0;
    rc = pthread_cond_broadcast(&worker.condition);
    if (rc != 0) status = -rc;
    while (status == 0 && worker.completed.load(std::memory_order_acquire) != command) {
        if (!active(team) || worker.done.load(std::memory_order_acquire)) {
            status = -ETIMEDOUT;
            break;
        }
        timespec const limit{static_cast<time_t>(team.deadline / 1000000000ULL), static_cast<long>(team.deadline % 1000000000ULL)};
        rc = pthread_cond_clockwait(&worker.condition, &worker.mutex, CLOCK_MONOTONIC, &limit);
        if (rc != 0) status = -rc;
    }
    rc = pthread_mutex_unlock(&worker.mutex);
    if (rc != 0 && status == 0) status = -rc;
    return status == 0 ? arm.error : status;
}
void histogram(const uint64_t (&values)[64]) {
    std::putchar('[');
    for (size_t i = 0; i < 64; ++i) std::printf("%s%llu", i ? "," : "", static_cast<unsigned long long>(values[i]));
    std::putchar(']');
}
void sample(const Options& o, const Topology& t, const Arm& a, uint64_t res) {
    std::printf(
        "NUMA_CHASE_V1 "
        "{\"version\":1,\"kind\":\"sample\",\"pair\":%u,\"order\":\"%s\",\"condition\":\"%s\",\"seed\":%llu,\"bytes\":%llu,\"stride\":%u,"
        "\"loads\":%llu,\"elapsed_ns\":%llu,\"checksum\":%llu,\"start_index\":%u,\"warm_checksum\":%llu,\"warm_loads\":%llu,\"chase_cpu\":%"
        "u,\"init_cpu\":%u,\"chase_node\":%u,\"expected_node\":%u,\"cpu_before\":%llu,\"cpu_after\":%llu,\"mask_before\":%llu,\"mask_"
        "after\":%llu,\"init_cpu_before\":%llu,\"init_cpu_after\":%llu,\"init_mask_before\":%llu,\"init_mask_after\":%llu,\"node_count\":%"
        "u,\"pages\":%llu,\"virgin_pages\":%llu,\"before_histogram\":",
        a.pair, a.pair % 2 ? "RL" : "LR", a.remote ? "remote" : "local", static_cast<unsigned long long>(o.seed),
        static_cast<unsigned long long>(o.bytes), o.stride, static_cast<unsigned long long>(o.loads),
        static_cast<unsigned long long>(a.elapsed_ns), static_cast<unsigned long long>(a.checksum), a.start_index,
        static_cast<unsigned long long>(a.warm_checksum), static_cast<unsigned long long>(o.bytes / o.stride), o.chase_cpu,
        a.remote ? o.remote_init_cpu : o.local_init_cpu, t.chase, a.remote ? t.remote : t.local,
        static_cast<unsigned long long>(a.cpu_before), static_cast<unsigned long long>(a.cpu_after),
        static_cast<unsigned long long>(a.mask_before), static_cast<unsigned long long>(a.mask_after),
        static_cast<unsigned long long>(a.init_cpu_before), static_cast<unsigned long long>(a.init_cpu_after),
        static_cast<unsigned long long>(a.init_mask_before), static_cast<unsigned long long>(a.init_mask_after), t.summary.node_count,
        static_cast<unsigned long long>(o.bytes / PAGE), static_cast<unsigned long long>(a.virgin_pages));
    histogram(a.before_histogram);
    std::printf(",\"after_histogram\":");
    histogram(a.after_histogram);
    std::printf(",\"clock_resolution_ns\":%llu,\"status\":%lld,\"phase\":\"%s\"}\n", static_cast<unsigned long long>(res),
                static_cast<long long>(a.error), a.phase);
    std::fflush(stdout);
}
}  // namespace

auto run_numa_chase(int argc, char** argv) -> int {
    Options options;
    if (!numa_bench::parse_options(argc, argv, options)) {
        std::fprintf(stderr,
                     "numa-chase: --pairs 1..10 --bytes 1048576..67108864(page multiple) --loads 1..268435456 --seed N --chase-cpu 0..63 "
                     "--local-init-cpu 0..63 --remote-init-cpu 0..63 --stride 64 --timeout-ms 1000..600000; duplicate options refused\n");
        return 2;
    }
    Topology topo;
    uint64_t clock_resolution{}, now{}, deadline{};
    int64_t error = 0;
    const char* phase = "clock";
    if (!now_ns(now) || now > UINT64_MAX - static_cast<uint64_t>(options.timeout_ms) * 1000000) error = -EIO;
    if (error == 0) {
        deadline = now + static_cast<uint64_t>(options.timeout_ms) * 1000000;
        if (!resolution(clock_resolution)) error = -EIO;
    }
    if (error == 0) {
        phase = "topology";
        error = topology(options, topo, deadline);
    }
    if (error != 0) {
        std::printf(
            "NUMA_CHASE_V1 {\"version\":1,\"kind\":\"failure\",\"pair\":0,\"condition\":\"local\",\"status\":%lld,\"phase\":\"%s\"}\n",
            static_cast<long long>(error), phase);
        return 1;
    }
    Team team{options, topo, deadline};
    std::array<Arm, MAX_ARMS> arms{};
    uint32_t created = 0, joined = 0, mapped = 0, unmapped = 0, samples = 0, completed_pairs = 0;
    std::printf(
        "NUMA_CHASE_V1 "
        "{\"version\":1,\"kind\":\"profile\",\"pairs\":%u,\"bytes\":%llu,\"stride\":%u,\"loads\":%llu,\"seed\":%llu,\"timeout_ms\":%u,"
        "\"chase_cpu\":%u,\"local_init_cpu\":%u,\"remote_init_cpu\":%u,\"chase_node\":%u,\"local_node\":%u,\"remote_node\":%u,\"node_"
        "count\":%u,\"cpu_count\":%u,\"provenance\":%u,\"clock_resolution_ns\":%llu}\n",
        options.pairs, static_cast<unsigned long long>(options.bytes), options.stride, static_cast<unsigned long long>(options.loads),
        static_cast<unsigned long long>(options.seed), options.timeout_ms, options.chase_cpu, options.local_init_cpu,
        options.remote_init_cpu, topo.chase, topo.local, topo.remote, topo.summary.node_count, topo.summary.cpu_count,
        topo.summary.provenance, static_cast<unsigned long long>(clock_resolution));
    for (uint32_t i = 0; i < 3 && error == 0; ++i) {
        auto& worker = team.workers[i];
        worker.team = &team;
        worker.role = i;
        worker.cpu = i == 0 ? options.chase_cpu : i == 1 ? options.local_init_cpu : options.remote_init_cpu;
        phase = "control_init";
        int rc = pthread_mutex_init(&worker.mutex, nullptr);
        if (rc != 0) {
            error = -rc;
            break;
        }
        worker.mutex_initialized = true;
        rc = pthread_cond_init(&worker.condition, nullptr);
        if (rc != 0) {
            error = -rc;
            break;
        }
        worker.condition_initialized = true;
    }
    for (auto& worker : team.workers) {
        if (error != 0) break;
        phase = "pthread_create";
        int const rc = pthread_create(&worker.thread, nullptr, thread_main, &worker);
        if (rc != 0) {
            error = -rc;
            break;
        }
        worker.created = true;
        ++created;
    }
    for (auto& worker : team.workers) {
        if (error != 0 || !worker.created) break;
        phase = "await_tid";
        if (!await_flag(team, worker, worker.started)) {
            error = -ETIMEDOUT;
            break;
        }
        phase = "controller_placement";
        error = place_thread(worker.tid.load(std::memory_order_acquire), worker.cpu, deadline);
        if (error != 0) break;
        worker.placed.store(true, std::memory_order_release);
        phase = "worker_ready";
        if (!await_flag(team, worker, worker.ready)) {
            error = -ETIMEDOUT;
            break;
        }
        error = worker.error.load(std::memory_order_acquire);
    }
    for (uint32_t pair = 0; pair < options.pairs && error == 0; ++pair) {
        for (uint32_t index = 0; index < 2 && error == 0; ++index) {
            auto& arm = arms[mapped];
            arm.pair = pair;
            arm.remote = ((pair % 2) == 0) ? index == 1 : index == 0;
            phase = "mmap";
            if (!active(team)) {
                error = -ETIMEDOUT;
                break;
            }
            void* const p = mmap(nullptr, static_cast<size_t>(options.bytes), PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            if (p == MAP_FAILED) {
                error = -errno;
                break;
            }
            arm.mapping = p;
            ++mapped;
            if ((reinterpret_cast<uintptr_t>(p) & (PAGE - 1)) != 0 || reinterpret_cast<uintptr_t>(p) > UINTPTR_MAX - options.bytes) {
                error = -EPROTO;
                break;
            }
            auto* owner = &team.workers[arm.remote ? 2 : 1];
            phase = "initializer";
            error = dispatch(team, *owner, arm);
            if (error == 0) {
                owner = &team.workers[0];
                phase = "chase";
                error = dispatch(team, *owner, arm);
            }
            if (error == 0) {
                arm.sampled = true;
                ++samples;
                sample(options, topo, arm, clock_resolution);
            } else {
                // A controller timeout can race a still-finishing worker. Its
                // mutable Arm fields are inspected only after the real joins.
                if (owner->completed.load(std::memory_order_acquire) == owner->command.load(std::memory_order_acquire)) phase = arm.phase;
            }
        }
        if (error == 0) ++completed_pairs;
    }
    team.cancel.store(true, std::memory_order_release);
    for (auto& worker : team.workers)
        if (worker.created) {
            int rc = pthread_mutex_lock(&worker.mutex);
            if (rc == 0) {
                int const signaled = pthread_cond_broadcast(&worker.condition);
                int const released = pthread_mutex_unlock(&worker.mutex);
                if (signaled != 0)
                    rc = signaled;
                else if (released != 0)
                    rc = released;
            }
            if (rc != 0 && error == 0) {
                error = -rc;
                phase = "cancel_broadcast";
            }
        }
    // No mapping may be returned while any successfully created thread could
    // still access it. An irrecoverable libc/scheduler hang belongs to the
    // external owned-VM deadline; never kill a thread then unmap underneath it.
    for (auto& worker : team.workers)
        if (worker.created) {
            int const rc = pthread_join(worker.thread, nullptr);
            if (rc == 0) {
                worker.joined = true;
                ++joined;
            } else if (error == 0) {
                error = -rc;
                phase = "pthread_join";
            }
        }
    // Completion publication precedes the worker's final broadcast/unlock
    // checks. After real joins, consume any late atomic error before success.
    if (joined == created && error == 0)
        for (auto const& worker : team.workers) {
            auto const status = worker.error.load(std::memory_order_acquire);
            if (worker.created && status != 0) {
                error = status;
                phase = "worker_completion";
                break;
            }
        }
    bool controls_closed = joined == created;
    if (joined == created)
        for (auto& worker : team.workers) {
            if (worker.condition_initialized) {
                int const rc = pthread_cond_destroy(&worker.condition);
                if (rc == 0)
                    worker.condition_initialized = false;
                else {
                    controls_closed = false;
                    if (error == 0) {
                        error = -rc;
                        phase = "condition_destroy";
                    }
                }
            }
            if (worker.mutex_initialized && !worker.condition_initialized) {
                int const rc = pthread_mutex_destroy(&worker.mutex);
                if (rc == 0)
                    worker.mutex_initialized = false;
                else {
                    controls_closed = false;
                    if (error == 0) {
                        error = -rc;
                        phase = "mutex_destroy";
                    }
                }
            }
        }
    if (joined == created && error != 0 && mapped != 0 && !arms[mapped - 1].sampled) {
        auto& failed = arms[mapped - 1];
        if (failed.error == 0) {
            failed.error = error;
            failed.phase = phase;
        }
        sample(options, topo, failed, clock_resolution);
    }
    if (joined == created && controls_closed)
        for (uint32_t i = 0; i < mapped; ++i) {
            auto& arm = arms[i];
            int const rc = munmap(arm.mapping, static_cast<size_t>(options.bytes));
            int const unmap_error = rc == 0 ? 0 : -errno;
            if (rc == 0) {
                arm.unmapped = true;
                ++unmapped;
            } else if (error == 0) {
                error = unmap_error;
                phase = "munmap";
            }
            std::printf(
                "NUMA_CHASE_V1 "
                "{\"version\":1,\"kind\":\"cleanup\",\"pair\":%u,\"condition\":\"%s\",\"unmapped\":%s,\"error\":%d,\"created_threads\":%u,"
                "\"joined_threads\":%u}\n",
                arm.pair, arm.remote ? "remote" : "local", arm.unmapped ? "true" : "false", unmap_error, created, joined);
        }
    if (error == 0 && (!before(deadline) || samples != options.pairs * 2 || mapped != unmapped || joined != 3)) {
        error = -ETIMEDOUT;
        phase = "final_bounds";
    }
    if (error != 0) {
        auto const& arm = arms[mapped == 0 ? 0 : mapped - 1];
        std::printf(
            "NUMA_CHASE_V1 {\"version\":1,\"kind\":\"failure\",\"pair\":%u,\"condition\":\"%s\",\"status\":%lld,\"phase\":\"%s\"}\n",
            arm.pair, arm.remote ? "remote" : "local", static_cast<long long>(error), phase);
    }
    std::printf(
        "NUMA_CHASE_V1 "
        "{\"version\":1,\"kind\":\"end\",\"status\":%lld,\"samples\":%u,\"completed_pairs\":%u,\"created_threads\":%u,\"joined_threads\":%"
        "u,\"mappings_created\":%u,\"mappings_unmapped\":%u,\"peak_mappings\":%u}\n",
        static_cast<long long>(error), samples, completed_pairs, created, joined, mapped, unmapped, mapped);
    bool const output_failed = std::fflush(stdout) != 0 || std::ferror(stdout) != 0;
    if (joined != created || !controls_closed) {
        // Keep Team/Arm storage and all payload mappings alive when libc did
        // not prove thread exit. The owned external VM deadline must terminate
        // this failed fixture; returning would invalidate a live worker's arg.
        for (;;) (void)ker::multiproc::yield();
    }
    return error != 0 || output_failed ? 1 : 0;
}
