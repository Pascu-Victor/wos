#include "perfbench.hpp"

#include <sys/multiproc.h>
#include <time.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <print>
#include <vector>

#include "mandelbench/tinycthread.hpp"

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

static auto now_ns() -> uint64_t {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

static auto rdtsc() -> uint64_t {
    // NOLINTBEGIN(misc-const-correctness)
    uint32_t lo = 0;
    uint32_t hi = 0;
    // NOLINTEND(misc-const-correctness)
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (static_cast<uint64_t>(hi) << 32) | lo;
}

static auto scramble_work(uint64_t value, uint64_t index) -> uint64_t {
    value ^= index + 0x9e3779b97f4a7c15ULL;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 31;
    asm volatile("" : "+r"(value) : : "memory");
    return value;
}

// ---------------------------------------------------------------------------
// Test 1 & 2: Thread spawn latency + CPU placement
// ---------------------------------------------------------------------------

struct SpawnArg {
    uint64_t spawn_ns;  // set by parent just before thrd_create
    uint64_t start_ns;  // set by thread at first instruction
    int cpu_id;
    int id;
};

static auto count_threads_on_cpu(const std::vector<SpawnArg>& args, int cpu_id) -> int {
    int count = 0;
    for (const auto& arg : args) {
        if (arg.cpu_id == cpu_id) {
            ++count;
        }
    }
    return count;
}

static auto spawn_thread(void* param) -> int {
    auto* a = static_cast<SpawnArg*>(param);
    a->start_ns = now_ns();
    a->cpu_id = static_cast<int>(ker::multiproc::getcurrent_cpu());
    return 0;
}

static void test_spawn_and_placement(int n) {
    std::vector<SpawnArg> args(n);
    std::vector<thrd_t> threads(n);

    for (int repeat = 0; repeat < 3; ++repeat) {
        for (int i = 0; i < n; ++i) {
            args[i].id = i;
            args[i].start_ns = 0;
            args[i].cpu_id = -1;
        }

        for (int i = 0; i < n; ++i) {
            args[i].spawn_ns = now_ns();
            thrd_create(&threads[i], spawn_thread, &args[i]);
        }
        for (int i = 0; i < n; ++i) {
            thrd_join(threads[i], nullptr);
        }

        uint64_t sum = 0;
        uint64_t mn = UINT64_MAX;
        uint64_t mx = 0;
        int min_idx = 0;
        int max_idx = 0;
        for (int i = 0; i < n; ++i) {
            uint64_t const LAT = args[i].start_ns - args[i].spawn_ns;
            sum += LAT;
            if (LAT < mn) {
                mn = LAT;
                min_idx = i;
            }
            if (LAT > mx) {
                mx = LAT;
                max_idx = i;
            }
        }
        double mean_ms = static_cast<double>(sum) / n / 1e6;
        double min_ms = static_cast<double>(mn) / 1e6;
        double max_ms = static_cast<double>(mx) / 1e6;

        // CPU placement
        bool seen[256] = {};
        int unique = 0;
        for (int i = 0; i < n; ++i) {
            int const C = args[i].cpu_id;
            if (C >= 0 && C < 256 && !seen[C]) {
                seen[C] = true;
                ++unique;
            }
        }

        std::print(stderr, "[perf] spawn_latency[{}]:  min={:.2f}ms max={:.2f}ms mean={:.2f}ms  cpus:", repeat, min_ms, max_ms, mean_ms);
        for (int i = 0; i < n; ++i) {
            std::print(stderr, " t{}->c{}", i, args[i].cpu_id);
        }
        std::print(stderr, "  [{}/{} unique CPUs", unique, n);
        if (unique < n) {
            std::print(stderr, " *** CO-LOCATION DETECTED ***");
        }
        std::println(stderr, "]");
        std::println(stderr, "[perf]   spawn_outliers[{}]: best=t{} c{} lat={:.2f}ms same_cpu={}  worst=t{} c{} lat={:.2f}ms same_cpu={}",
                     repeat, min_idx, args[min_idx].cpu_id, min_ms, count_threads_on_cpu(args, args[min_idx].cpu_id), max_idx,
                     args[max_idx].cpu_id, max_ms, count_threads_on_cpu(args, args[max_idx].cpu_id));
    }
}

// ---------------------------------------------------------------------------
// Test 3: Context switch cost (mutex ping-pong, deadlock-safe)
//
// Two threads share a single mutex. Thread A locks, increments counter,
// unlocks. Thread B does the same. Each unlock gives the other thread a
// chance to run. The total wall time / (2 * iters) = one lock/unlock round
// trip, which requires a context switch when both threads are competing.
//
// This avoids condvar entirely - no signal can be "lost", no way to deadlock.
// ---------------------------------------------------------------------------

struct PingPongArg {
    mtx_t* mtx;
    volatile int* counter;
    int target;
    int role;  // 0 or 1 - only used for identification
};

static auto pingpong_thread(void* param) -> int {
    auto* a = static_cast<PingPongArg*>(param);
    // Each thread grabs the mutex, bumps the counter, releases.
    // Stop when counter reaches target.
    while (*a->counter < a->target) {
        mtx_lock(a->mtx);
        if (*a->counter < a->target) {
            *a->counter = *a->counter + 1;
        }
        mtx_unlock(a->mtx);
    }
    return 0;
}

static void test_context_switch() {
    constexpr int ITERS = 2000;

    mtx_t mtx;
    volatile int counter = 0;
    mtx_init(&mtx, MTX_PLAIN);

    PingPongArg a0{.mtx = &mtx, .counter = &counter, .target = ITERS * 2, .role = 0};
    PingPongArg a1{.mtx = &mtx, .counter = &counter, .target = ITERS * 2, .role = 1};

    double best_us = 1e18;
    for (int repeat = 0; repeat < 3; ++repeat) {
        counter = 0;
        thrd_t t0 = nullptr;
        thrd_t t1 = nullptr;
        uint64_t const T_START = now_ns();
        thrd_create(&t0, pingpong_thread, &a0);
        thrd_create(&t1, pingpong_thread, &a1);
        thrd_join(t0, nullptr);
        thrd_join(t1, nullptr);
        uint64_t const ELAPSED = now_ns() - T_START;
        // elapsed covers 2*ITERS lock/unlock pairs split across 2 threads
        double const US = static_cast<double>(ELAPSED) / (2.0 * ITERS * 1000.0);
        best_us = std::min(US, best_us);
    }

    mtx_destroy(&mtx);

    std::println(stderr, "[perf] ctx_switch:         best={:.1f}µs/lock-round-trip  ({} total increments)", best_us, ITERS * 2);
}

// ---------------------------------------------------------------------------
// Test 4: Parallel efficiency
// ---------------------------------------------------------------------------

struct ParEffArg {
    int id;
    int threads;
    int iters;
    volatile uint64_t result;
    uint64_t start_ns;
    uint64_t end_ns;
    uint64_t cpu_ns;
    int cpu_id;
    int cpu_end_id;
    uint64_t cpu_mask;
    uint32_t cpu_changes;
    std::atomic<int>* ready_count;
    std::atomic<bool>* go;
};

static auto cpu_bit(int cpu_id) -> uint64_t {
    if (cpu_id < 0 || cpu_id >= 64) {
        return 0;
    }
    return 1ULL << static_cast<unsigned>(cpu_id);
}

static void note_cpu_sample(ParEffArg* arg, int current_cpu) {
    arg->cpu_mask |= cpu_bit(current_cpu);
    if (current_cpu != arg->cpu_end_id) {
        if (arg->cpu_end_id >= 0) {
            ++arg->cpu_changes;
        }
        arg->cpu_end_id = current_cpu;
    }
}

static auto par_eff_thread(void* param) -> int {
    auto* a = static_cast<ParEffArg*>(param);
    a->ready_count->fetch_add(1, std::memory_order_release);
    while (!a->go->load(std::memory_order_acquire)) {
        thrd_yield();
    }
    a->cpu_id = static_cast<int>(ker::multiproc::getcurrent_cpu());
    a->cpu_end_id = a->cpu_id;
    a->cpu_mask = cpu_bit(a->cpu_id);
    a->cpu_changes = 0;
    a->start_ns = now_ns();
    uint64_t const CPU_START = rdtsc();
    uint64_t sum = 0x123456789abcdef0ULL ^ static_cast<uint64_t>(a->id);
    int const CHUNK = a->iters / a->threads;
    int const START = a->id * CHUNK;
    int const END = (a->id == a->threads - 1) ? a->iters : START + CHUNK;
    constexpr int SAMPLE_STRIDE = 1 << 20;
    for (int i = START; i < END; ++i) {
        if (((i - START) % SAMPLE_STRIDE) == 0) {
            note_cpu_sample(a, static_cast<int>(ker::multiproc::getcurrent_cpu()));
        }
        sum = scramble_work(sum, static_cast<uint64_t>(i));
    }
    note_cpu_sample(a, static_cast<int>(ker::multiproc::getcurrent_cpu()));
    a->result = sum;
    a->end_ns = now_ns();
    a->cpu_ns = rdtsc() - CPU_START;
    return 0;
}

static void print_parallel_outliers(int n, const std::vector<ParEffArg>& args) {
    int min_idx = 0;
    int max_idx = 0;
    double min_dur_ms = static_cast<double>(args[0].end_ns - args[0].start_ns) / 1e6;
    double max_dur_ms = min_dur_ms;
    uint64_t earliest_start_ns = args[0].start_ns;
    uint64_t latest_start_ns = args[0].start_ns;
    uint64_t latest_end_ns = args[0].end_ns;

    for (int i = 1; i < n; ++i) {
        double const DUR_MS = static_cast<double>(args[i].end_ns - args[i].start_ns) / 1e6;
        if (DUR_MS < min_dur_ms) {
            min_dur_ms = DUR_MS;
            min_idx = i;
        }
        if (DUR_MS > max_dur_ms) {
            max_dur_ms = DUR_MS;
            max_idx = i;
        }
        earliest_start_ns = std::min(earliest_start_ns, args[i].start_ns);
        latest_start_ns = std::max(latest_start_ns, args[i].start_ns);
        latest_end_ns = std::max(latest_end_ns, args[i].end_ns);
    }

    int best_same_cpu = 0;
    int worst_same_cpu = 0;
    for (int i = 0; i < n; ++i) {
        if (args[i].cpu_id == args[min_idx].cpu_id) {
            ++best_same_cpu;
        }
        if (args[i].cpu_id == args[max_idx].cpu_id) {
            ++worst_same_cpu;
        }
    }

    std::println(stderr, "[perf]   parallel_workers[{}]: best=t{} c{} dur={:.2f}ms same_cpu={}  worst=t{} c{} dur={:.2f}ms same_cpu={}", n,
                 min_idx, args[min_idx].cpu_id, min_dur_ms, best_same_cpu, max_idx, args[max_idx].cpu_id, max_dur_ms, worst_same_cpu);
    std::println(stderr, "[perf]   parallel_span[{}]:    start_skew={:.2f}ms total_span={:.2f}ms best_res={:#x} worst_res={:#x}", n,
                 static_cast<double>(latest_start_ns - earliest_start_ns) / 1e6,
                 static_cast<double>(latest_end_ns - earliest_start_ns) / 1e6, static_cast<unsigned long long>(args[min_idx].result),
                 static_cast<unsigned long long>(args[max_idx].result));
    std::println(
        stderr,
        "[perf]   parallel_migrate[{}]: best=t{} cpus={} first=c{} last=c{} changes={}  worst=t{} cpus={} first=c{} last=c{} changes={}", n,
        min_idx, std::popcount(args[min_idx].cpu_mask), args[min_idx].cpu_id, args[min_idx].cpu_end_id, args[min_idx].cpu_changes, max_idx,
        std::popcount(args[max_idx].cpu_mask), args[max_idx].cpu_id, args[max_idx].cpu_end_id, args[max_idx].cpu_changes);
}

struct PhaseProbeArg {
    int cpu_id;
};

static auto phase_probe_thread(void* param) -> int {
    auto* probe = static_cast<PhaseProbeArg*>(param);
    probe->cpu_id = static_cast<int>(ker::multiproc::getcurrent_cpu());
    return 0;
}

static void align_thread_create_phase(int cpu_count) {
    if (cpu_count <= 1) {
        return;
    }

    PhaseProbeArg probe{.cpu_id = -1};
    thrd_t probe_thread{};
    if (thrd_create(&probe_thread, phase_probe_thread, &probe) != THRD_SUCCESS) {
        return;
    }
    thrd_join(probe_thread, nullptr);

    if (probe.cpu_id < 0) {
        return;
    }

    int const NEXT_CPU = (probe.cpu_id + 1) % cpu_count;
    int const ADVANCE = (cpu_count - NEXT_CPU) % cpu_count;
    for (int i = 0; i < ADVANCE; ++i) {
        PhaseProbeArg dummy{.cpu_id = -1};
        thrd_t dummy_thread{};
        if (thrd_create(&dummy_thread, phase_probe_thread, &dummy) != THRD_SUCCESS) {
            break;
        }
        thrd_join(dummy_thread, nullptr);
    }
}

static auto choose_spread_targets(int cpu_count, int worker_count) -> std::vector<int> {
    std::vector<int> targets;
    targets.reserve(worker_count);

    for (int index = 0; index < worker_count; ++index) {
        int target_cpu = (index * cpu_count) / worker_count;
        if (!targets.empty() && target_cpu <= targets.back()) {
            target_cpu = targets.back() + 1;
        }
        if (target_cpu >= cpu_count) {
            target_cpu = cpu_count - 1;
        }
        targets.push_back(target_cpu);
    }

    return targets;
}

static void test_parallel_efficiency() {
    constexpr int ITERS = 250000000;
    constexpr int REPEATS = 5;
    int const THREAD_COUNTS[] = {1, 2, 4, 8};
    double time_1 = 0;
    double speedups[4] = {};
    int const cpu_count = static_cast<int>(ker::multiproc::nativeThreadCount());

    if (cpu_count <= 0) {
        std::println(stderr, "[perf] parallel_eff:       FAILED (no CPUs reported)");
        return;
    }

    for (int ti = 0; ti < 4; ++ti) {
        int n = THREAD_COUNTS[ti];
        std::vector<ParEffArg> args(n);
        std::vector<thrd_t> threads(n);
        std::vector<std::vector<ParEffArg>> repeat_args;
        std::vector<double> repeat_seconds;

        repeat_args.reserve(REPEATS);
        repeat_seconds.reserve(REPEATS);

        for (int repeat = 0; repeat < REPEATS; ++repeat) {
            std::atomic<int> ready_count{0};
            std::atomic<bool> go{false};
            auto target_cpus = choose_spread_targets(cpu_count, n);
            for (int i = 0; i < n; ++i) {
                args[i] = {.id = i,
                           .threads = n,
                           .iters = ITERS,
                           .result = 0,
                           .start_ns = 0,
                           .end_ns = 0,
                           .cpu_ns = 0,
                           .cpu_id = -1,
                           .cpu_end_id = -1,
                           .cpu_mask = 0,
                           .cpu_changes = 0,
                           .ready_count = &ready_count,
                           .go = &go};
            }
            align_thread_create_phase(cpu_count);
            bool create_failed = false;
            int next_cpu = 0;
            for (int i = 0; i < n; ++i) {
                while (next_cpu != target_cpus[i]) {
                    PhaseProbeArg dummy{.cpu_id = -1};
                    thrd_t dummy_thread{};
                    if (thrd_create(&dummy_thread, phase_probe_thread, &dummy) != THRD_SUCCESS) {
                        create_failed = true;
                        break;
                    }
                    thrd_join(dummy_thread, nullptr);
                    next_cpu = (next_cpu + 1) % cpu_count;
                }
                if (create_failed) {
                    break;
                }
                if (thrd_create(&threads[i], par_eff_thread, &args[i]) != THRD_SUCCESS) {
                    std::println(stderr, "[perf] parallel_eff[{}]: FAILED (thrd_create t{})", n, i);
                    create_failed = true;
                    break;
                }
                next_cpu = (next_cpu + 1) % cpu_count;
            }
            if (create_failed) {
                for (int i = 0; i < n; ++i) {
                    if (threads[i] != nullptr) {
                        thrd_join(threads[i], nullptr);
                    }
                }
                return;
            }
            while (ready_count.load(std::memory_order_acquire) != n) {
                thrd_yield();
            }
            uint64_t const T0 = now_ns();
            go.store(true, std::memory_order_release);
            for (int i = 0; i < n; ++i) {
                thrd_join(threads[i], nullptr);
            }

            double worst_thread_s = 0.0;
            for (int i = 0; i < n; ++i) {
                double const WORKER_S = static_cast<double>(args[i].end_ns - args[i].start_ns) / 1e9;
                worst_thread_s = std::max(worst_thread_s, WORKER_S);
            }

            (void)T0;
            repeat_seconds.push_back(worst_thread_s);
            repeat_args.push_back(args);
        }

        std::vector<int> order(REPEATS);
        for (int i = 0; i < REPEATS; ++i) {
            order[i] = i;
        }
        std::ranges::sort(order, [&repeat_seconds](int lhs, int rhs) { return repeat_seconds[lhs] < repeat_seconds[rhs]; });

        int const MEDIAN_INDEX = order[REPEATS / 2];
        double const MEDIAN_S = repeat_seconds[MEDIAN_INDEX];
        double const MIN_S = repeat_seconds[order.front()];
        double const MAX_S = repeat_seconds[order.back()];

        if (n == 1) {
            time_1 = MEDIAN_S;
            speedups[ti] = 1.0;
            std::println(stderr, "[perf]   parallel_base[1]: median={:.2f}ms min={:.2f}ms max={:.2f}ms", MEDIAN_S * 1e3, MIN_S * 1e3,
                         MAX_S * 1e3);
        } else {
            speedups[ti] = time_1 / MEDIAN_S;
        }

        if (n > 1 && !repeat_args.empty()) {
            print_parallel_outliers(n, repeat_args[MEDIAN_INDEX]);
        }
    }

    std::println(stderr, "[perf] parallel_eff:       t1={:.2f}x t2={:.2f}x t4={:.2f}x t8={:.2f}x  (ideal: 2/4/8x)", speedups[0],
                 speedups[1], speedups[2], speedups[3]);
}

// ---------------------------------------------------------------------------
// Test 5: Memory bandwidth
// ---------------------------------------------------------------------------

static void test_mem_bandwidth() {
    constexpr size_t BUF_SIZE = 64ULL * 1024 * 1024;  // 64 MB
    auto* buf = static_cast<uint64_t*>(malloc(BUF_SIZE));
    if (buf == nullptr) {
        std::println(stderr, "[perf] mem_bandwidth:      FAILED (malloc)");
        return;
    }

    size_t const N = BUF_SIZE / sizeof(uint64_t);

    // Cold write
    uint64_t t0 = now_ns();
    for (size_t i = 0; i < N; ++i) {
        buf[i] = i;
    }
    double cold_write_gbs = static_cast<double>(BUF_SIZE) / static_cast<double>(now_ns() - t0);

    // Warm write
    t0 = now_ns();
    for (size_t i = 0; i < N; ++i) {
        buf[i] = i;
    }
    double warm_write_gbs = static_cast<double>(BUF_SIZE) / static_cast<double>(now_ns() - t0);

    // Warm read
    volatile uint64_t sink = 0;
    t0 = now_ns();
    for (size_t i = 0; i < N; ++i) {
        sink += buf[i];
    }
    double warm_read_gbs = static_cast<double>(BUF_SIZE) / static_cast<double>(now_ns() - t0);
    (void)sink;

    free(buf);

    std::println(stderr, "[perf] mem_bandwidth:      cold_write={:.2f}GB/s  warm_write={:.2f}GB/s  warm_read={:.2f}GB/s", cold_write_gbs,
                 warm_write_gbs, warm_read_gbs);
}

// ---------------------------------------------------------------------------
// Test 6: Timer interrupt overhead (rdtsc loop)
// ---------------------------------------------------------------------------

static void test_timer_overhead() {
    // Calibrate: measure rdtsc ticks per second using CLOCK_MONOTONIC
    uint64_t const CAL_START_NS = now_ns();
    uint64_t const CAL_START_TSC = rdtsc();
    // Spin for ~100ms
    while (now_ns() - CAL_START_NS < 100000000ULL) {
    }
    uint64_t const CAL_END_NS = now_ns();
    uint64_t const CAL_END_TSC = rdtsc();
    double const TSC_HZ = static_cast<double>(CAL_END_TSC - CAL_START_TSC) / (static_cast<double>(CAL_END_NS - CAL_START_NS) / 1e9);

    // Now run a tight rdtsc loop for 1 second and count iterations
    constexpr int LOOP_ITERS = 10000000;
    uint64_t const WALL_START = now_ns();
    uint64_t const TSC_START = rdtsc();
    // Run for ~1 second of wall time
    while (now_ns() - WALL_START < 1000000000ULL) {
        // Tight inner loop
        volatile uint64_t x = 0;
        for (int i = 0; i < LOOP_ITERS; ++i) {
            x = rdtsc();
        }
        (void)x;
    }
    uint64_t const WALL_ELAPSED = now_ns() - WALL_START;
    uint64_t const TSC_ELAPSED = rdtsc() - TSC_START;

    // Expected tsc cycles if we ran continuously
    double const EXPECTED_TSC = TSC_HZ * (static_cast<double>(WALL_ELAPSED) / 1e9);
    // % cycles lost = (1 - actual_tsc / expected_tsc) * 100
    double pct_lost = (1.0 - (static_cast<double>(TSC_ELAPSED) / EXPECTED_TSC)) * 100.0;
    pct_lost = std::max(pct_lost, 0.0);

    std::println(stderr, "[perf] timer_overhead:     {:.1f}% cycles lost to interrupts  (tsc_hz={:.0f}MHz)", pct_lost, TSC_HZ / 1e6);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

auto run_perf([[maybe_unused]] int argc, [[maybe_unused]] char** argv) -> int {
    constexpr int N_THREADS = 8;

    std::println(stderr, "[perf] === WOS perf suite ===");

    std::println(stderr, "[perf] --- spawn latency + CPU placement ({} threads) ---", N_THREADS);
    test_spawn_and_placement(N_THREADS);

    std::println(stderr, "[perf] --- context switch cost ---");
    test_context_switch();

    std::println(stderr, "[perf] --- parallel efficiency ---");
    test_parallel_efficiency();

    std::println(stderr, "[perf] --- memory bandwidth ---");
    test_mem_bandwidth();

    std::println(stderr, "[perf] --- timer interrupt overhead ---");
    test_timer_overhead();

    std::println(stderr, "[perf] === done ===");
    return 0;
}
