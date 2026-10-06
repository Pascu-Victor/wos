#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/workqueue.hpp>
#include <thread>

using namespace ker::mod::sched;

namespace {
struct Gate {
    std::mutex lock;
    std::condition_variable cv;
    bool entered{};
    bool released{};
    void enter() {
        std::unique_lock guard(lock);
        entered = true;
        cv.notify_all();
        cv.wait(guard, [&] { return released; });
    }
    auto await() -> bool {
        std::unique_lock guard(lock);
        return cv.wait_for(guard, std::chrono::seconds(5), [&] { return entered; });
    }
    void release() {
        std::lock_guard guard(lock);
        released = true;
        cv.notify_all();
    }
};
void block_callback(void* argument) { static_cast<Gate*>(argument)->enter(); }
void increment(void* argument) { static_cast<std::atomic<unsigned>*>(argument)->fetch_add(1, std::memory_order_relaxed); }

class WorkqueueRuntime : public testing::Test {
   public:
    void SetUp() override {
        workqueue_test_shutdown.store(false);
        workqueue_test_abandoned.store(0);
        workqueue_test_held_parks.store(0);
        workqueue_test_yields.store(0);
        task::workqueue_test_fail_create.store(false);
        task::workqueue_test_failed_cleanup.store(0);
        workqueue_test_fail_post.store(false);
    }
    void TearDown() override {
        std::lock_guard guard(task::workqueue_test_registry_lock);
        for (auto* task : task::workqueue_test_tasks) {
            if (task->worker.joinable()) task->worker.join();
            EXPECT_TRUE(task->finished.load(std::memory_order_acquire));
            EXPECT_EQ(task->clean_exit_calls.load(), 1U);
            EXPECT_EQ(task->references.load(), 0U);
            EXPECT_EQ(task->kernel_thread_context, nullptr);
            EXPECT_FALSE(task->shutdown.held());
            delete task;
        }
        task::workqueue_test_tasks.clear();
        EXPECT_EQ(workqueue_test_abandoned.load(), 0U);
    }
};
}  // namespace

TEST_F(WorkqueueRuntime, ConcurrentCreationUsesUniqueTaskContext) {
    constexpr size_t COUNT = 16;
    std::array<Workqueue*, COUNT> queues{};
    std::array<std::thread, COUNT> creators{};
    std::barrier start(COUNT);
    for (size_t i = 0; i < COUNT; ++i) {
        creators[i] = std::thread([&, i] {
            start.arrive_and_wait();
            queues[i] = Workqueue::create("host-concurrent-workq");
        });
    }
    for (auto& creator : creators) creator.join();
    struct Context {
        Workqueue* expected;
        std::atomic<bool> matched{};
    };
    std::array<Context, COUNT> contexts{};
    std::array<WorkItem, COUNT> items{};
    for (size_t i = 0; i < COUNT; ++i) {
        ASSERT_NE(queues[i], nullptr);
        contexts[i].expected = queues[i];
        items[i] = {[](void* argument) {
                        auto& context = *static_cast<Context*>(argument);
                        context.matched.store(get_current_task()->kernel_thread_context == context.expected, std::memory_order_release);
                    },
                    &contexts[i], nullptr};
        EXPECT_TRUE(queues[i]->enqueue(&items[i]));
    }
    for (size_t i = 0; i < COUNT; ++i) {
        EXPECT_TRUE(queues[i]->flush());
        EXPECT_TRUE(contexts[i].matched.load(std::memory_order_acquire));
        EXPECT_TRUE(queues[i]->destroy());
        delete queues[i];
    }
}

TEST_F(WorkqueueRuntime, FailedWorkerPostReleasesUnpublishedResources) {
    workqueue_test_fail_post.store(true);
    EXPECT_EQ(Workqueue::create("host-failed-post"), nullptr);
    EXPECT_EQ(task::workqueue_test_failed_cleanup.load(), 1U);
    {
        std::lock_guard guard(task::workqueue_test_registry_lock);
        EXPECT_TRUE(task::workqueue_test_tasks.empty());
    }
    // A later successful creation still has its unique published context and
    // balanced queue/scheduler references after callback and join.
    workqueue_test_fail_post.store(false);
    std::unique_ptr<Workqueue> queue(Workqueue::create("host-post-retry"));
    ASSERT_NE(queue, nullptr);
    std::atomic<unsigned> callbacks{};
    WorkItem item{increment, &callbacks, nullptr};
    ASSERT_TRUE(queue->enqueue(&item));
    EXPECT_TRUE(queue->flush());
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_TRUE(queue->destroy());
}

TEST_F(WorkqueueRuntime, FlushIncludesCurrentlyRunningCallback) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-flush-active"));
    ASSERT_NE(queue.get(), nullptr);
    Gate gate;
    WorkItem item{block_callback, &gate, nullptr};
    ASSERT_TRUE(queue->enqueue(&item));
    ASSERT_TRUE(gate.await());
    std::atomic<bool> entered{}, joined{};
    std::thread flush([&] {
        entered.store(true, std::memory_order_release);
        EXPECT_TRUE(queue->flush());
        joined.store(true, std::memory_order_release);
    });
    while (!entered.load(std::memory_order_acquire)) std::this_thread::yield();
    auto const DEADLINE = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!joined.load(std::memory_order_acquire) && workqueue_test_yields.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < DEADLINE)
        std::this_thread::yield();
    EXPECT_GT(workqueue_test_yields.load(), 0U);
    EXPECT_FALSE(joined.load(std::memory_order_acquire));
    gate.release();
    flush.join();
    EXPECT_TRUE(joined.load(std::memory_order_acquire));
    EXPECT_TRUE(queue->destroy());
}

TEST_F(WorkqueueRuntime, DestroyClosesAdmissionAndJoinsRunningAndQueuedWork) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-destroy-active"));
    ASSERT_NE(queue.get(), nullptr);
    Gate gate;
    WorkItem first{block_callback, &gate, nullptr};
    std::atomic<unsigned> finished{};
    WorkItem second{increment, &finished, nullptr};
    ASSERT_TRUE(queue->enqueue(&first));
    ASSERT_TRUE(gate.await());
    ASSERT_TRUE(queue->enqueue(&second));
    std::atomic<bool> entered{}, joined{};
    std::thread destroy([&] {
        entered.store(true, std::memory_order_release);
        EXPECT_TRUE(queue->destroy());
        joined.store(true, std::memory_order_release);
    });
    while (!entered.load(std::memory_order_acquire)) std::this_thread::yield();
    EXPECT_FALSE(joined.load(std::memory_order_acquire));
    gate.release();
    destroy.join();
    EXPECT_TRUE(joined.load(std::memory_order_acquire));
    EXPECT_EQ(finished.load(), 1U);
    WorkItem rejected{increment, &finished, reinterpret_cast<WorkItem*>(uintptr_t{1})};
    EXPECT_FALSE(queue->enqueue(&rejected));
    EXPECT_EQ(rejected.next, reinterpret_cast<WorkItem*>(uintptr_t{1}));
    EXPECT_TRUE(queue->flush());
    EXPECT_TRUE(queue->destroy());
}

TEST_F(WorkqueueRuntime, SelfJoinReturnsFalseAndDoesNotAuthorizeFree) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-self-join"));
    ASSERT_NE(queue.get(), nullptr);
    struct Context {
        Workqueue* queue;
        bool flush_result{true};
        bool destroy_result{true};
    } context{queue.get()};
    WorkItem item{[](void* argument) {
                      auto& context = *static_cast<Context*>(argument);
                      context.flush_result = context.queue->flush();
                      context.destroy_result = context.queue->destroy();
                  },
                  &context, nullptr};
    ASSERT_TRUE(queue->enqueue(&item));
    ASSERT_TRUE(queue->flush());
    EXPECT_FALSE(context.flush_result);
    EXPECT_FALSE(context.destroy_result);
    EXPECT_TRUE(queue->destroy());
}

TEST_F(WorkqueueRuntime, CallbackMayReleaseItemAndRequeueIndependentWork) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-requeue"));
    ASSERT_NE(queue.get(), nullptr);
    std::atomic<unsigned> count{};
    WorkItem later{increment, &count, nullptr};
    struct Context {
        Workqueue* queue;
        WorkItem* own;
        WorkItem* later;
    } context{queue.get(), nullptr, &later};
    auto* item = new WorkItem{[](void* argument) {
                                  auto& context = *static_cast<Context*>(argument);
                                  delete context.own;
                                  EXPECT_TRUE(context.queue->enqueue(context.later));
                              },
                              &context, nullptr};
    context.own = item;
    ASSERT_TRUE(queue->enqueue(item));
    EXPECT_TRUE(queue->flush());
    EXPECT_TRUE(queue->destroy());
    EXPECT_EQ(count.load(), 1U);
}

TEST_F(WorkqueueRuntime, ShutdownDrainsAndRetiresBeforeCooperativeExit) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-shutdown"));
    ASSERT_NE(queue.get(), nullptr);
    Gate gate;
    WorkItem first{block_callback, &gate, nullptr};
    std::atomic<unsigned> count{};
    WorkItem second{increment, &count, nullptr};
    ASSERT_TRUE(queue->enqueue(&first));
    ASSERT_TRUE(gate.await());
    ASSERT_TRUE(queue->enqueue(&second));
    workqueue_test_request_shutdown();
    gate.release();
    EXPECT_TRUE(queue->destroy());
    EXPECT_EQ(count.load(), 1U);
    EXPECT_FALSE(queue->enqueue(&second));
}

TEST_F(WorkqueueRuntime, IdleShutdownHoldProtectsFinalQueueRetirement) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-idle-shutdown"));
    ASSERT_NE(queue.get(), nullptr);
    auto const DEADLINE = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (workqueue_test_held_parks.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < DEADLINE) {
        std::this_thread::yield();
    }
    EXPECT_GT(workqueue_test_held_parks.load(), 0U);
    workqueue_test_request_shutdown();
    EXPECT_TRUE(queue->destroy());
}

TEST_F(WorkqueueRuntime, FailedTaskCreationAndInvalidWorkTakeNoOwnership) {
    task::workqueue_test_fail_create.store(true);
    EXPECT_EQ(Workqueue::create("host-failed-create"), nullptr);
    task::workqueue_test_fail_create.store(false);
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-invalid-work"));
    ASSERT_NE(queue.get(), nullptr);
    WorkItem invalid{};
    EXPECT_FALSE(queue->enqueue(nullptr));
    EXPECT_FALSE(queue->enqueue(&invalid));
    EXPECT_TRUE(queue->destroy());
}

TEST_F(WorkqueueRuntime, ConcurrentProducerCloseDrainsExactlyAcceptedItems) {
    auto queue = std::unique_ptr<Workqueue>(Workqueue::create("host-producer-close"));
    ASSERT_NE(queue.get(), nullptr);
    Gate gate;
    WorkItem held{block_callback, &gate, nullptr};
    ASSERT_TRUE(queue->enqueue(&held));
    ASSERT_TRUE(gate.await());
    constexpr size_t PRODUCERS = 8;
    constexpr size_t ITEMS = 64;
    std::array<WorkItem, PRODUCERS * ITEMS> items{};
    std::array<std::thread, PRODUCERS> producers{};
    std::atomic<unsigned> accepted{}, completed{};
    for (auto& item : items) item = {increment, &completed, nullptr};
    std::barrier start(PRODUCERS + 1);
    for (size_t i = 0; i < PRODUCERS; ++i) {
        producers[i] = std::thread([&, i] {
            start.arrive_and_wait();
            for (size_t j = 0; j < ITEMS; ++j) {
                if (queue->enqueue(&items[i * ITEMS + j])) accepted.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    std::thread closer([&] {
        start.arrive_and_wait();
        EXPECT_TRUE(queue->destroy());
    });
    for (auto& producer : producers) producer.join();
    gate.release();
    closer.join();
    EXPECT_EQ(completed.load(), accepted.load());
    EXPECT_FALSE(queue->enqueue(&held));
}
