#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX_THREADS 128
#define MONO_THREADS 8
#define MONO_READS 125000
#define STORM_SAMPLES 128
#define LATENCY_SAMPLES 1000

struct worker {
    int mode;
    unsigned samples;
    unsigned errors;
    unsigned regressions;
    unsigned cpu_regressions;
    unsigned early;
    uint64_t thread_progress;
    uint64_t process_progress;
    uint64_t lateness[LATENCY_SAMPLES];
};
static struct worker workers[MAX_THREADS];
static pthread_t threads[MAX_THREADS];
static pthread_mutex_t gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gate_condition = PTHREAD_COND_INITIALIZER;
static int gate_open;
static unsigned ready;
static pthread_cond_t ready_condition = PTHREAD_COND_INITIALIZER;
static uint64_t sorted_lateness[MAX_THREADS * STORM_SAMPLES];

static int clock_ns(clockid_t clock, uint64_t* value) {
    struct timespec time;
    if (clock_gettime(clock, &time)) return -1;
    *value = (uint64_t)time.tv_sec * 1000000000ULL + (uint64_t)time.tv_nsec;
    return 0;
}

static int compare(const void* lhs, const void* rhs) {
    uint64_t a = *(const uint64_t*)lhs;
    uint64_t b = *(const uint64_t*)rhs;
    return (a > b) - (a < b);
}

static void* work(void* argument) {
    struct worker* worker = argument;
    pthread_mutex_lock(&gate_lock);
    ++ready;
    pthread_cond_signal(&ready_condition);
    while (!gate_open) pthread_cond_wait(&gate_condition, &gate_lock);
    pthread_mutex_unlock(&gate_lock);
    uint64_t last = 0, thread_last = 0, process_last = 0;
    uint64_t thread_first = 0, process_first = 0;
    if (worker->mode == 0 && (clock_ns(CLOCK_THREAD_CPUTIME_ID, &thread_first) || clock_ns(CLOCK_PROCESS_CPUTIME_ID, &process_first))) {
        ++worker->errors;
        return NULL;
    }
    thread_last = thread_first;
    process_last = process_first;
    for (unsigned i = 0; i < worker->samples; ++i) {
        uint64_t now;
        if (clock_ns(CLOCK_MONOTONIC, &now)) {
            ++worker->errors;
            break;
        }
        if (now < last) ++worker->regressions;
        last = now;
        if (worker->mode == 0) {
            if ((i % 1024) == 0 || i + 1 == worker->samples) {
                uint64_t thread_now, process_now;
                if (clock_ns(CLOCK_THREAD_CPUTIME_ID, &thread_now) || clock_ns(CLOCK_PROCESS_CPUTIME_ID, &process_now)) {
                    ++worker->errors;
                    break;
                }
                if (thread_now < thread_last || process_now < process_last) ++worker->cpu_regressions;
                thread_last = thread_now;
                process_last = process_now;
            }
            continue;
        }
        uint64_t deadline = now + 1000000ULL;
        struct timespec request = {(time_t)(deadline / 1000000000ULL), (long)(deadline % 1000000000ULL)};
        int error;
        do {
            error = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &request, NULL);
        } while (error == EINTR);
        if (error || clock_ns(CLOCK_MONOTONIC, &now)) {
            ++worker->errors;
            break;
        }
        if (now < deadline) ++worker->early;
        worker->lateness[i] = now > deadline ? now - deadline : 0;
        if (now < last) ++worker->regressions;
        last = now;
    }
    worker->thread_progress = thread_last - thread_first;
    worker->process_progress = process_last - process_first;
    return NULL;
}

static int sample_loop(void) {
    puts("READY");
    fflush(stdout);
    for (unsigned i = 0; i < 256; ++i) {
        int request = getchar();
        if (request == EOF || request == 'Q') return 0;
        if (request != 'S') continue;
        uint64_t value;
        if (clock_ns(CLOCK_MONOTONIC, &value)) return 1;
        printf("%llu\n", (unsigned long long)value);
        fflush(stdout);
    }
    return 1;
}

int main(int argc, char** argv) {
    alarm(180);
    if (argc != 2) return 2;
    if (!strcmp(argv[1], "sample-loop")) return sample_loop();
    unsigned count, samples;
    int mode;
    if (!strcmp(argv[1], "monotonic")) {
        count = MONO_THREADS;
        samples = MONO_READS;
        mode = 0;
    } else if (!strcmp(argv[1], "latency")) {
        count = 1;
        samples = LATENCY_SAMPLES;
        mode = 1;
    } else if (!strcmp(argv[1], "storm")) {
        count = MAX_THREADS;
        samples = STORM_SAMPLES;
        mode = 1;
    } else {
        return 2;
    }
    pthread_attr_t attributes;
    if (pthread_attr_init(&attributes) || pthread_attr_setstacksize(&attributes, 65536)) return 3;
    unsigned created = 0;
    for (; created < count; ++created) {
        workers[created].mode = mode;
        workers[created].samples = samples;
        int error = pthread_create(&threads[created], &attributes, work, &workers[created]);
        if (error) {
            fprintf(stderr, "thread_create index=%u error=%d\n", created, error);
            break;
        }
    }
    pthread_attr_destroy(&attributes);
    pthread_mutex_lock(&gate_lock);
    while (ready != created) pthread_cond_wait(&ready_condition, &gate_lock);
    gate_open = 1;
    pthread_cond_broadcast(&gate_condition);
    pthread_mutex_unlock(&gate_lock);
    unsigned errors = created != count, regressions = 0, cpu_regressions = 0, early = 0;
    uint64_t thread_progress = 0, process_progress = 0;
    for (unsigned i = 0; i < created; ++i) {
        errors += pthread_join(threads[i], NULL) != 0;
        errors += workers[i].errors;
        regressions += workers[i].regressions;
        cpu_regressions += workers[i].cpu_regressions;
        early += workers[i].early;
        thread_progress += workers[i].thread_progress;
        if (workers[i].process_progress > process_progress) process_progress = workers[i].process_progress;
        if (mode) memcpy(sorted_lateness + i * samples, workers[i].lateness, sizeof(uint64_t) * samples);
    }
    uint64_t p99 = 0, maximum = 0;
    if (mode && created) {
        unsigned total = created * samples;
        qsort(sorted_lateness, total, sizeof(uint64_t), compare);
        p99 = sorted_lateness[(99 * total + 99) / 100 - 1];
        maximum = sorted_lateness[total - 1];
    }
    int passed = errors == 0 && regressions == 0 && cpu_regressions == 0 && early == 0;
    if (!mode) passed = passed && thread_progress > 0 && process_progress > 0;
    if (mode) passed = passed && p99 <= (count == 1 ? 5000000ULL : 100000000ULL) && maximum <= (count == 1 ? 50000000ULL : 1000000000ULL);
    printf(
        "{\"mode\":\"%s\",\"threads\":%u,\"samples\":%u,\"errors\":%u,\"regressions\":%u,"
        "\"cpu_regressions\":%u,\"early\":%u,\"p99_ns\":%llu,\"max_ns\":%llu,"
        "\"thread_cpu_progress_ns\":%llu,\"process_cpu_progress_ns\":%llu,\"passed\":%s}\n",
        argv[1], created, created * samples, errors, regressions, cpu_regressions, early, (unsigned long long)p99,
        (unsigned long long)maximum, (unsigned long long)thread_progress, (unsigned long long)process_progress, passed ? "true" : "false");
    return passed ? 0 : 1;
}
