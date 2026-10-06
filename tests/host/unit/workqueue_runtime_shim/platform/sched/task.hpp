#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <platform/sched/shutdown_ownership.hpp>
#include <thread>
#include <vector>

namespace ker::mod::sched::task {
struct Task {
    void* kernel_thread_context{};
    void (*entry)(){};
    std::atomic<unsigned> references{1};
    std::atomic<bool> finished{};
    std::atomic<unsigned> clean_exit_calls{};
    ShutdownOwnership shutdown;
    std::mutex wake_lock;
    std::condition_variable wake_cv;
    bool wake_pending{};
    std::thread worker;

    static auto create_kernel_thread(const char*, void (*)()) -> Task*;
    auto try_acquire() -> bool {
        unsigned count = references.load(std::memory_order_acquire);
        while (count != 0) {
            if (references.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel)) return true;
        }
        return false;
    }
    auto try_acquire_lifetime_ref() -> bool { return try_acquire(); }
    void release() {
        if (references.fetch_sub(1, std::memory_order_acq_rel) == 0) __builtin_trap();
    }
};

inline std::mutex workqueue_test_registry_lock;
inline std::vector<Task*> workqueue_test_tasks;
inline std::atomic<bool> workqueue_test_fail_create{};
inline std::atomic<unsigned> workqueue_test_failed_cleanup{};
inline auto Task::create_kernel_thread(const char*, void (*function)()) -> Task* {
    if (workqueue_test_fail_create.load(std::memory_order_acquire)) return nullptr;
    auto* result = new Task;
    result->entry = function;
    std::lock_guard guard(workqueue_test_registry_lock);
    workqueue_test_tasks.push_back(result);
    return result;
}
inline void destroy_unpublished_user_thread(Task* task) {
    if (task->references.load(std::memory_order_acquire) != 1 || task->kernel_thread_context != nullptr || task->worker.joinable())
        __builtin_trap();
    std::lock_guard guard(workqueue_test_registry_lock);
    auto const position = std::find(workqueue_test_tasks.begin(), workqueue_test_tasks.end(), task);
    if (position == workqueue_test_tasks.end()) __builtin_trap();
    workqueue_test_tasks.erase(position);
    workqueue_test_failed_cleanup.fetch_add(1, std::memory_order_relaxed);
    delete task;
}
}  // namespace ker::mod::sched::task
