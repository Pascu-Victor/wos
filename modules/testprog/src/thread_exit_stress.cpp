#include "thread_exit_stress.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <print>

namespace {

constexpr int DEFAULT_THREAD_COUNT = 32;
constexpr int MAX_THREAD_COUNT = 64;
constexpr useconds_t CHURN_DURATION_US = 50'000;

pthread_mutex_t churn_mutex{};
pthread_cond_t churn_condition{};
std::atomic<int> ready_workers{0};
std::atomic<bool> start_churn{false};

auto parse_thread_count(int argc, char** argv) -> int {
    if (argc == 0) {
        return DEFAULT_THREAD_COUNT;
    }

    errno = 0;
    char* end = nullptr;
    long const PARSED = std::strtol(argv[0], &end, 10);
    if (end == argv[0] || *end != '\0' || errno != 0 || PARSED <= 0 || PARSED > MAX_THREAD_COUNT) {
        return -1;
    }
    return static_cast<int>(PARSED);
}

void* churn_waiter([[maybe_unused]] void* unused) {
    ready_workers.fetch_add(1, std::memory_order_release);
    for (;;) {
        pthread_mutex_lock(&churn_mutex);
        pthread_cond_wait(&churn_condition, &churn_mutex);
        pthread_mutex_unlock(&churn_mutex);
        sched_yield();
    }
}

void* churn_broadcaster([[maybe_unused]] void* unused) {
    while (!start_churn.load(std::memory_order_acquire)) {
        sched_yield();
    }
    for (;;) {
        pthread_mutex_lock(&churn_mutex);
        pthread_cond_broadcast(&churn_condition);
        pthread_mutex_unlock(&churn_mutex);
        sched_yield();
    }
}

}  // namespace

auto run_thread_exit_stress(int argc, char** argv) -> int {
    int const THREAD_COUNT = parse_thread_count(argc, argv);
    if (THREAD_COUNT < 0) {
        std::println(stderr, "usage: testprog thread-exit-stress [threads:1-{}]", MAX_THREAD_COUNT);
        return 2;
    }

    ready_workers.store(0, std::memory_order_relaxed);
    start_churn.store(false, std::memory_order_relaxed);
    int const MUTEX_RESULT = pthread_mutex_init(&churn_mutex, nullptr);
    int const CONDITION_RESULT = pthread_cond_init(&churn_condition, nullptr);
    if (MUTEX_RESULT != 0 || CONDITION_RESULT != 0) {
        std::println(stderr, "thread-exit-stress: synchronization init failed: mutex={} condition={}", MUTEX_RESULT, CONDITION_RESULT);
        return 3;
    }

    std::array<pthread_t, MAX_THREAD_COUNT> workers{};
    for (int i = 0; i < THREAD_COUNT; ++i) {
        int const RESULT = pthread_create(&workers.at(static_cast<size_t>(i)), nullptr, churn_waiter, nullptr);
        if (RESULT != 0) {
            std::println(stderr, "thread-exit-stress: pthread_create worker {} failed: {}", i, RESULT);
            std::fflush(nullptr);
            _Exit(3);
        }
    }

    pthread_t broadcaster{};
    int const BROADCASTER_RESULT = pthread_create(&broadcaster, nullptr, churn_broadcaster, nullptr);
    if (BROADCASTER_RESULT != 0) {
        std::println(stderr, "thread-exit-stress: pthread_create broadcaster failed: {}", BROADCASTER_RESULT);
        std::fflush(nullptr);
        _Exit(4);
    }

    while (ready_workers.load(std::memory_order_acquire) != THREAD_COUNT) {
        sched_yield();
    }
    start_churn.store(true, std::memory_order_release);
    usleep(CHURN_DURATION_US);

    std::println("thread-exit-stress: exiting with {} futex-churning workers", THREAD_COUNT);
    std::fflush(nullptr);
    _Exit(0);
}
