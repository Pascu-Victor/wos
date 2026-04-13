#include "mandelbench.hpp"

#include <sched.h>

#include <algorithm>
#include <string>
#if MANDELBENCH_DEBUG
#include <bit>
#endif
#include <cstddef>
#include <format>
#include <print>

#include "config.hpp"
#ifdef WOS
#include <sys/multiproc.h>
#endif
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/time.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <vector>

#include "tinycthread.hpp"
#include "util.hpp"
namespace {
#if MANDELBENCH_DEBUG
auto now_ns() -> uint64_t {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}
#endif

#if MANDELBENCH_DEBUG
auto thread_cpu_ns() -> uint64_t {
    // CLOCK_THREAD_CPUTIME_ID (id 3): kernel-tracked on-CPU time for this thread.
    // user_time_us + system_time_us accumulated by the scheduler - measures actual
    // time the thread ran on a CPU, excluding any time it was preempted.
    timespec ts{};
    clock_gettime((clockid_t)3, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

auto cpu_bit(int cpu_id) -> uint64_t {
    if (cpu_id < 0 || cpu_id >= 64) {
        return 0;
    }
    return 1ULL << static_cast<unsigned>(cpu_id);
}

void note_cpu_sample(struct arg* a, int current_cpu) {
    a->cpu_mask |= cpu_bit(current_cpu);
    if (current_cpu != a->cpu_end_id) {
        if (a->cpu_end_id >= 0) {
            ++a->cpu_changes;
        }
        a->cpu_end_id = current_cpu;
    }
}
#endif

auto generate_image(void* param) -> int {
    auto* a = static_cast<struct arg*>(param);
#if MANDELBENCH_DEBUG
    a->cpu_id = (int)ker::multiproc::getCurrentCpu();
    a->cpu_end_id = a->cpu_id;
    a->cpu_mask = cpu_bit(a->cpu_id);
    a->cpu_changes = 0;
    a->thread_start_ns = now_ns();
    uint64_t cpu_start = thread_cpu_ns();
    a->thread_cpu_ns = 0;
    a->total_iterations = 0;
    a->rows_completed = 0;
    a->preempt_count = 0;
    a->preempt_ns = 0;
    a->max_gap_ns = 0;
#endif

    int row = 0;
    int col = 0;
    int iteration = 0;
    double c_re = 0.0;
    double c_im = 0.0;
    double x = 0.0;
    double y = 0.0;
    double x_new = 0.0;

    unsigned char* image = a->image;
    unsigned char* colormap = a->colormap;
    int width = a->width;
    int height = a->height;
    int max = a->max;

#if MANDELBENCH_DEBUG
    // Gaps between row completions larger than this threshold are counted as
    // preemption events.  2ms is ~4x a typical fast row; tuned so that a NAPI
    // wakeup or timer interrupt (< 50us) doesn't register as a preemption.
    constexpr uint64_t PREEMPT_THRESHOLD_NS = 2000000ULL;  // 2ms
    uint64_t prev_row_end = a->thread_start_ns;
#endif

    for (row = a->id; row < height; row += a->threads) {
#if MANDELBENCH_DEBUG
        uint64_t row_start = now_ns();
        uint64_t gap = row_start - prev_row_end;
        if (gap > PREEMPT_THRESHOLD_NS) {
            a->preempt_count++;
            a->preempt_ns += gap;
            if (gap > a->max_gap_ns) a->max_gap_ns = gap;
        }
        note_cpu_sample(a, (int)ker::multiproc::getCurrentCpu());
        a->rows_completed++;
#endif
        for (col = 0; col < width; col++) {
            c_re = (col - (width / 2.0)) * 4.0 / width;
            c_im = (row - (height / 2.0)) * 4.0 / width;
            x = 0, y = 0;
            iteration = 0;
            while ((x * x) + (y * y) <= 4 && iteration < max) {
                x_new = (x * x) - (y * y) + c_re;
                y = (2 * x * y) + c_im;
                x = x_new;
                iteration++;
            }
#if MANDELBENCH_DEBUG
            a->total_iterations += static_cast<uint64_t>(iteration);
#endif
            iteration = std::min(iteration, max);
            set_pixel(image, width, col, row, &colormap[static_cast<ptrdiff_t>(iteration * 3)]);
        }
#if MANDELBENCH_DEBUG
        prev_row_end = now_ns();
#endif
    }
#if MANDELBENCH_DEBUG
    note_cpu_sample(a, (int)ker::multiproc::getCurrentCpu());
    a->thread_end_ns = prev_row_end;
    a->thread_cpu_ns = thread_cpu_ns() - cpu_start;
#endif
    return 0;
}

auto get_time() -> uint64_t {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000) + (tv.tv_usec / 1000);  // NOLINT
}

constexpr auto DEVICE_NAME = "cpu";

#if MANDELBENCH_DEBUG
auto count_threads_on_cpu(const std::vector<struct arg>& args, int cpu_id) -> int {
    int count = 0;
    for (const auto& thread_arg : args) {
        if (thread_arg.cpu_id == cpu_id) {
            ++count;
        }
    }
    return count;
}
#endif
}  // namespace
auto mandelbench(int width, int height, int max_iteration, int threads, int repeat, unsigned char* image, unsigned char* colormap) -> int {
    std::vector<struct arg> a(threads);
    std::vector<thrd_t> t(threads);
    std::vector<double> times(repeat);
    uint64_t start_time = 0;
    uint64_t end_time = 0;
    int i = 0;
    int r = 0;

    for (r = 0; r < repeat; r++) {
        memset(image, 0, static_cast<long>(width * height) * 4);

        start_time = get_time();
#if MANDELBENCH_DEBUG
        uint64_t spawn_start = now_ns();
#endif
        int created_threads = 0;

        for (i = 0; i < threads; i++) {
            a[i].image = image;
            a[i].colormap = colormap;
            a[i].width = width;
            a[i].height = height;
            a[i].max = max_iteration;
            a[i].id = i;
            a[i].threads = threads;
#if MANDELBENCH_DEBUG
            a[i].thread_start_ns = 0;
            a[i].thread_end_ns = 0;
            a[i].thread_cpu_ns = 0;
            a[i].cpu_mask = 0;
            a[i].total_iterations = 0;
            a[i].rows_completed = 0;
            a[i].cpu_id = -1;
            a[i].cpu_end_id = -1;
            a[i].cpu_changes = 0;
#endif

            int create_result = thrd_create(&t[i], generate_image, &a[i]);
            if (create_result != thrd_success) {
                std::println(stderr, "  error: thrd_create failed for worker {} on repeat {} (rc={})", i, r, create_result);
                break;
            }
            ++created_threads;
        }

#if MANDELBENCH_DEBUG
        uint64_t all_spawned = now_ns();
#endif
        if (created_threads != threads) {
            for (i = 0; i < created_threads; i++) {
                thrd_join(t[i], nullptr);
            }
            return 1;
        }

#if MANDELBENCH_DEBUG
        std::println(stderr, "  [repeat {}/{}] spawned {} workers in {:.2f}ms; computing...", r + 1, repeat, created_threads,
                     (double)(all_spawned - spawn_start) / 1e6);
#endif

        for (i = 0; i < threads; i++) {
            int join_result = thrd_join(t[i], nullptr);
            if (join_result != thrd_success) {
                std::println(stderr, "  error: thrd_join failed for worker {} on repeat {} (rc={})", i, r, join_result);
                return 1;
            }
        }

#if MANDELBENCH_DEBUG
        uint64_t all_joined = now_ns();
#endif
        end_time = get_time();
        times[r] = (double)(end_time - start_time) / 1000.0;

#if MANDELBENCH_DEBUG
        // find earliest/latest thread start and latest thread end
        uint64_t t_start_min = a[0].thread_start_ns;
        uint64_t t_start_max = a[0].thread_start_ns;
        uint64_t t_end_max = a[0].thread_end_ns;
        int best_thread = 0;
        int worst_thread = 0;
        for (i = 1; i < threads; i++) {
            t_start_min = std::min(a[i].thread_start_ns, t_start_min);
            t_start_max = std::max(a[i].thread_start_ns, t_start_max);
            t_end_max = std::max(a[i].thread_end_ns, t_end_max);
        }
        // Per-thread duration (time each thread actually ran)
        double min_thread_dur = 1e18;
        double max_thread_dur = 0;
        double min_cpu_eff = 1e18;
        double max_cpu_eff = 0;
        for (i = 0; i < threads; i++) {
            double dur = (double)(a[i].thread_end_ns - a[i].thread_start_ns) / 1e6;
            if (dur < min_thread_dur) {
                min_thread_dur = dur;
                best_thread = i;
            }
            if (dur > max_thread_dur) {
                max_thread_dur = dur;
                worst_thread = i;
            }
            double eff = (dur > 0.0) ? ((double)a[i].thread_cpu_ns / 1e6) / dur : 0.0;
            min_cpu_eff = std::min(eff, min_cpu_eff);
            max_cpu_eff = std::max(eff, max_cpu_eff);
        }
        // Print per-thread CPU placement for diagnosing co-location
        std::print(stderr, "  cpus:");
        for (i = 0; i < threads; i++) std::print(stderr, " t{}->c{}", i, a[i].cpu_id);
        std::println(stderr, "");
        std::println(stderr,
                     "  breakdown: spawn={:.2f}ms start_skew={:.2f}ms compute={:.2f}ms join={:.2f}ms "
                     "thread_dur=[{:.2f},{:.2f}]ms cpu_rate_mhz=[{:.0f},{:.0f}]",
                     (double)(all_spawned - spawn_start) / 1e6, (double)(t_start_max - t_start_min) / 1e6,
                     (double)(t_end_max - t_start_min) / 1e6, (double)(all_joined - t_end_max) / 1e6, min_thread_dur, max_thread_dur,
                     min_cpu_eff, max_cpu_eff);
        std::println(stderr,
                     "  outliers: best=t{} c{} start={:.2f}ms end={:.2f}ms dur={:.2f}ms same_cpu={}  "
                     "worst=t{} c{} start={:.2f}ms end={:.2f}ms dur={:.2f}ms same_cpu={}",
                     best_thread, a[best_thread].cpu_id, (double)(a[best_thread].thread_start_ns - spawn_start) / 1e6,
                     (double)(a[best_thread].thread_end_ns - spawn_start) / 1e6,
                     (double)(a[best_thread].thread_end_ns - a[best_thread].thread_start_ns) / 1e6,
                     count_threads_on_cpu(a, a[best_thread].cpu_id), worst_thread, a[worst_thread].cpu_id,
                     (double)(a[worst_thread].thread_start_ns - spawn_start) / 1e6,
                     (double)(a[worst_thread].thread_end_ns - spawn_start) / 1e6,
                     (double)(a[worst_thread].thread_end_ns - a[worst_thread].thread_start_ns) / 1e6,
                     count_threads_on_cpu(a, a[worst_thread].cpu_id));
        std::println(stderr, "  migrate:  best=t{} cpus={} first=c{} last=c{} changes={}  worst=t{} cpus={} first=c{} last=c{} changes={}",
                     best_thread, std::popcount(a[best_thread].cpu_mask), a[best_thread].cpu_id, a[best_thread].cpu_end_id,
                     a[best_thread].cpu_changes, worst_thread, std::popcount(a[worst_thread].cpu_mask), a[worst_thread].cpu_id,
                     a[worst_thread].cpu_end_id, a[worst_thread].cpu_changes);
        std::println(stderr, "  work:     best=t{} rows={} iters={}  worst=t{} rows={} iters={}", best_thread,
                     (unsigned long long)a[best_thread].rows_completed, (unsigned long long)a[best_thread].total_iterations, worst_thread,
                     (unsigned long long)a[worst_thread].rows_completed, (unsigned long long)a[worst_thread].total_iterations);
        // Per-thread preemption summary (kernel-measured on-CPU time vs wall time)
        std::print(stderr, "  cpu-eff: ");
        for (int ti = 0; ti < threads; ti++) {
            double wall_ms = (double)(a[ti].thread_end_ns - a[ti].thread_start_ns) / 1e6;
            // thread_cpu_ns delta = on-CPU ns (user+sys time from kernel scheduler)
            double cpu_ms = (double)a[ti].thread_cpu_ns / 1e6;
            double eff = (wall_ms > 0.0) ? (cpu_ms / wall_ms) * 100.0 : 100.0;
            std::print(stderr, " t{}:{:.0f}%({:.0f}/{:.0f}ms)", ti, eff, cpu_ms, wall_ms);
        }
        std::println(stderr, "");
        std::println(
            stderr,
            "  cpu-eff-outliers: best=t{} cpu={:.0f}ms wall={:.0f}ms eff={:.1f}%  "
            "worst=t{} cpu={:.0f}ms wall={:.0f}ms eff={:.1f}%",
            best_thread, (double)a[best_thread].thread_cpu_ns / 1e6,
            (double)(a[best_thread].thread_end_ns - a[best_thread].thread_start_ns) / 1e6,
            ((double)a[best_thread].thread_cpu_ns / (double)(a[best_thread].thread_end_ns - a[best_thread].thread_start_ns)) * 100.0,
            worst_thread, (double)a[worst_thread].thread_cpu_ns / 1e6,
            (double)(a[worst_thread].thread_end_ns - a[worst_thread].thread_start_ns) / 1e6,
            ((double)a[worst_thread].thread_cpu_ns / (double)(a[worst_thread].thread_end_ns - a[worst_thread].thread_start_ns)) * 100.0);
#endif

        std::string path = std::format(IMAGE, DEVICE_NAME, r);
        save_image(path.c_str(), image, width, height);
        progress(DEVICE_NAME, width, height, max_iteration, threads, repeat, r, times[r]);
    }
    report(DEVICE_NAME, width, height, max_iteration, threads, repeat, times);

    return 0;
}
