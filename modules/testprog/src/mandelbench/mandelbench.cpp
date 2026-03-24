#include <sched.h>

#include <algorithm>
#include <bit>
#include <print>

#include "config.hpp"
#define _DEFAULT_SOURCE 1
#include <sys/multiproc.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "tinycthread.hpp"
#include "util.hpp"

static auto now_ns() -> uint64_t {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

static auto thread_cpu_ns() -> uint64_t {
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (static_cast<uint64_t>(hi) << 32) | lo;
}

static auto cpu_bit(int cpu_id) -> uint64_t {
    if (cpu_id < 0 || cpu_id >= 64) {
        return 0;
    }
    return 1ULL << static_cast<unsigned>(cpu_id);
}

static void note_cpu_sample(struct arg* a, int current_cpu) {
    a->cpu_mask |= cpu_bit(current_cpu);
    if (current_cpu != a->cpu_end_id) {
        if (a->cpu_end_id >= 0) {
            ++a->cpu_changes;
        }
        a->cpu_end_id = current_cpu;
    }
}

auto generate_image(void* param) -> int {
    auto* a = static_cast<struct arg*>(param);

    // Read FPU state before touching it
    if (a->id == 0) {
        uint32_t mxcsr_val = 0;
        uint16_t fcw_val = 0;
        asm volatile("stmxcsr %0" : "=m"(mxcsr_val));
        asm volatile("fstcw %0" : "=m"(fcw_val));
        std::println(stderr, "  [t0] MXCSR={:#010x} x87FCW={:#06x}", mxcsr_val, fcw_val);
    }

    a->cpu_id = (int)ker::multiproc::getCurrentCpu();
    a->cpu_end_id = a->cpu_id;
    a->cpu_mask = cpu_bit(a->cpu_id);
    a->cpu_changes = 0;
    a->thread_start_ns = now_ns();
    uint64_t cpu_start = thread_cpu_ns();
    a->thread_cpu_ns = 0;
    a->total_iterations = 0;
    a->rows_completed = 0;

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

    for (row = a->id; row < height; row += a->threads) {
        note_cpu_sample(a, (int)ker::multiproc::getCurrentCpu());
        a->rows_completed++;
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
            a->total_iterations += static_cast<uint64_t>(iteration);
            iteration = std::min(iteration, max);
            set_pixel(image, width, col, row, &colormap[static_cast<ptrdiff_t>(iteration * 3)]);
        }
    }
    note_cpu_sample(a, (int)ker::multiproc::getCurrentCpu());
    a->thread_end_ns = now_ns();
    a->thread_cpu_ns = thread_cpu_ns() - cpu_start;
    return 0;
}

auto get_time() -> uint64_t {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

constexpr auto DEVICE_NAME = "cpu";

static auto count_threads_on_cpu(const std::vector<struct arg>& args, int cpu_id) -> int {
    int count = 0;
    for (const auto& thread_arg : args) {
        if (thread_arg.cpu_id == cpu_id) {
            ++count;
        }
    }
    return count;
}

auto mandelbench(int width, int height, int max_iteration, int threads, int repeat, unsigned char* image, unsigned char* colormap) -> int {
    std::vector<struct arg> a(threads);
    std::vector<thrd_t> t(threads);
    std::vector<double> times(repeat);
    uint64_t start_time = 0;
    uint64_t end_time = 0;
    int i, r;
    char path[255];

    for (r = 0; r < repeat; r++) {
        memset(image, 0, width * height * 4);

        start_time = get_time();
        uint64_t spawn_start = now_ns();
        int created_threads = 0;

        for (i = 0; i < threads; i++) {
            a[i].image = image;
            a[i].colormap = colormap;
            a[i].width = width;
            a[i].height = height;
            a[i].max = max_iteration;
            a[i].id = i;
            a[i].threads = threads;
            a[i].thread_start_ns = 0;
            a[i].thread_end_ns = 0;
            a[i].thread_cpu_ns = 0;
            a[i].cpu_mask = 0;
            a[i].total_iterations = 0;
            a[i].rows_completed = 0;
            a[i].cpu_id = -1;
            a[i].cpu_end_id = -1;
            a[i].cpu_changes = 0;

            int create_result = thrd_create(&t[i], generate_image, &a[i]);
            if (create_result != thrd_success) {
                std::println(stderr, "  error: thrd_create failed for worker {} on repeat {} (rc={})", i, r, create_result);
                break;
            }
            ++created_threads;
        }

        uint64_t all_spawned = now_ns();
        if (created_threads != threads) {
            for (i = 0; i < created_threads; i++) {
                thrd_join(t[i], nullptr);
            }
            return 1;
        }

        std::println(stderr, "  [repeat {}/{}] spawned {} workers in {:.2f}ms; computing...", r + 1, repeat, created_threads,
                     (double)(all_spawned - spawn_start) / 1e6);

        for (i = 0; i < threads; i++) {
            int join_result = thrd_join(t[i], nullptr);
            if (join_result != thrd_success) {
                std::println(stderr, "  error: thrd_join failed for worker {} on repeat {} (rc={})", i, r, join_result);
                return 1;
            }
        }

        uint64_t all_joined = now_ns();
        end_time = get_time();
        times[r] = (double)(end_time - start_time) / 1000.0;

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
        double min_thread_dur = 1e18, max_thread_dur = 0;
        double min_cpu_eff = 1e18, max_cpu_eff = 0;
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
            if (eff < min_cpu_eff) min_cpu_eff = eff;
            if (eff > max_cpu_eff) max_cpu_eff = eff;
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

        sprintf(path, IMAGE, DEVICE_NAME, r);
        save_image(path, image, width, height);
        progress(DEVICE_NAME, width, height, max_iteration, threads, repeat, r, times[r]);
    }
    report(DEVICE_NAME, width, height, max_iteration, threads, repeat, times);

    return 0;
}
