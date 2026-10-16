#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <functional>
#include <mod/io/serial/serial.hpp>
#include <mutex>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <thread>

using namespace ker::dev;
namespace sched = ker::mod::sched;

namespace ker::dev {
auto block_device_is_read_only(const BlockDevice* device) -> bool { return device != nullptr && device->read_only; }
}  // namespace ker::dev

namespace {
// The private saved-v12 proof compiles with zero added owner references. Shared
// production-source tests always retain the default exact v13 reference count.
#ifndef WOS_TEST_SERVICE_OWNER_REFS
#define WOS_TEST_SERVICE_OWNER_REFS 1
#endif
constexpr unsigned SERVICE_OWNER_REFS = WOS_TEST_SERVICE_OWNER_REFS;
// Permanent queue globals deliberately have no destruction/reset API. A fresh
// child process per case preserves that contract and permits shuffled tests;
// every child joins real worker threads and releases all request owners.
template <class Function>
void isolated(Function function) {
    pid_t const CHILD = fork();
    ASSERT_GE(CHILD, 0);
    if (CHILD == 0) {
        function();
        _exit(testing::Test::HasFailure() ? 1 : 0);
    }
    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(CHILD, &status, 0);
    } while (waited == -1 && errno == EINTR);
    ASSERT_EQ(waited, CHILD);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

template <class Predicate>
auto await(Predicate predicate) -> bool {
    auto const DEADLINE = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate() && std::chrono::steady_clock::now() < DEADLINE) std::this_thread::yield();
    return predicate();
}

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
    auto await_entered() -> bool {
        std::unique_lock guard(lock);
        return cv.wait_for(guard, std::chrono::seconds(5), [&] { return entered; });
    }
    void release() {
        std::lock_guard guard(lock);
        released = true;
        cv.notify_all();
    }
};

struct Provider {
    struct Pending {
        BlockRequest* request{};
        BlockRequestToken token{};
    };
    BlockDevice disk{};
    std::mutex lock;
    std::array<Pending, BLOCK_MODEL_MAX_REQUESTS> pending{};
    std::array<std::byte, 128 * 512> media{};
    std::atomic<bool> auto_complete{};
    bool service_completion{};
    bool allow_boot_scope{};
    std::atomic<unsigned> issued{};
    std::atomic<unsigned> flushes{};
    std::atomic<unsigned> scope_checks{};
    std::atomic<unsigned> invalid_scopes{};
    std::atomic<Gate*> service_gate{};
    unsigned service_calls{};
    std::function<void(unsigned)> service_hook;
    std::atomic<unsigned> poll_calls{};
    bool poll_result{true};
    std::function<void(unsigned)> poll_hook;

    Provider() {
        std::strcpy(disk.name.data(), "host-block-worker");
        disk.block_size = 512;
        disk.total_blocks = 128;
        disk.private_data = this;
    }
    void observe_scope() {
        auto* current = sched::get_current_task();
        scope_checks.fetch_add(1, std::memory_order_relaxed);
        if (current == nullptr && allow_boot_scope) return;
        if (current == nullptr || current->reclaim_no_io_depth == 0 || !current->shutdown.held())
            invalid_scopes.fetch_add(1, std::memory_order_relaxed);
    }
    static auto submit(BlockDevice* disk, BlockRequest& request) -> int {
        auto& provider = *static_cast<Provider*>(disk->private_data);
        provider.observe_scope();
        std::lock_guard guard(provider.lock);
        EXPECT_EQ(provider.pending[request.token.slot].request, nullptr);
        provider.pending[request.token.slot] = {&request, request.token};
        provider.issued.fetch_add(1, std::memory_order_release);
        return 0;
    }
    void retire(BlockRequestToken token, int status, bool from_service = false) {
        size_t bytes = 0;
        {
            std::lock_guard guard(lock);
            auto& transport = pending[token.slot];
            ASSERT_NE(transport.request, nullptr);
            ASSERT_EQ(transport.token, token);
            auto const& request = *transport.request;
            if (status == 0) {
                bytes = request.description.bytes;
                size_t offset = request.description.lba * 512;
                if (request.description.op == BlockRequestOperation::FLUSH) {
                    flushes.fetch_add(1, std::memory_order_relaxed);
                } else {
                    for (size_t i = 0; i < request.segment_count; ++i) {
                        auto const& segment = request.segments[i];
                        if (request.description.op == BlockRequestOperation::WRITE)
                            std::memcpy(media.data() + offset, segment.data, segment.size);
                        else
                            std::memcpy(segment.data, media.data() + offset, segment.size);
                        offset += segment.size;
                    }
                }
            }
            transport = {};  // No transport buffer access after completion.
        }
        EXPECT_TRUE(from_service ? block_request_complete_from_service(&disk, token, status, bytes)
                                 : block_request_complete(&disk, token, status, bytes));
    }
    static void service(BlockDevice* disk) {
        auto& provider = *static_cast<Provider*>(disk->private_data);
        provider.observe_scope();
        std::function<void(unsigned)> hook;
        unsigned calls = 0;
        {
            std::lock_guard guard(provider.lock);
            calls = ++provider.service_calls;
            hook = provider.service_hook;
        }
        if (hook) hook(calls);
        if (provider.issued.load(std::memory_order_acquire) != 0) {
            if (auto* gate = provider.service_gate.exchange(nullptr, std::memory_order_acq_rel); gate != nullptr) gate->enter();
        }
        if (!provider.auto_complete.load(std::memory_order_acquire)) return;
        std::array<BlockRequestToken, BLOCK_MODEL_MAX_REQUESTS> tokens{};
        size_t count = 0;
        {
            std::lock_guard guard(provider.lock);
            for (auto const& pending : provider.pending) {
                if (pending.request != nullptr) tokens[count++] = pending.token;
            }
        }
        for (size_t i = 0; i < count; ++i) provider.retire(tokens[i], 0, provider.service_completion);
    }
    static auto poll_active(BlockDevice* disk) -> bool {
        auto& provider = *static_cast<Provider*>(disk->private_data);
        provider.observe_scope();
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
        auto const calls = provider.poll_calls.fetch_add(1, std::memory_order_relaxed) + 1;
        if (provider.poll_hook) provider.poll_hook(calls);
        return provider.poll_result;
    }
    static void cancel(BlockDevice* disk, BlockRequestToken token, int reason) {
        auto& provider = *static_cast<Provider*>(disk->private_data);
        provider.observe_scope();
        provider.retire(token, reason);
    }
};

struct Scenario {
    sched::task::Task caller;
    Provider provider;
    std::array<BlockRequest, BLOCK_MODEL_MAX_REQUESTS> requests{};
    std::array<std::array<std::byte, 512>, BLOCK_MODEL_MAX_REQUESTS> buffers{};
    std::array<std::atomic<unsigned>, BLOCK_MODEL_MAX_REQUESTS> callback_counts{};
    std::atomic<unsigned> callback_scope_errors{};
    bool configured{};

    Scenario() {
        caller.type = sched::task::TaskType::PROCESS;
        sched::block_worker_current_task = &caller;
        ker::mod::mm::addr::block_test_hhdm_offset.store(0);
        sched::block_worker_exit_argument.store(this);
        sched::block_worker_exit_hook.store([](sched::task::Task*, void* argument) {
            auto& scenario = *static_cast<Scenario*>(argument);
            auto const snapshot = scenario.snapshot();
            EXPECT_FALSE(snapshot.worker_present);
            EXPECT_TRUE(snapshot.caller_driven);
            EXPECT_FALSE(snapshot.async_admission_open);
            EXPECT_EQ(snapshot.queued, 0U);
            EXPECT_EQ(snapshot.issued, 0U);
            EXPECT_EQ(snapshot.completing, 0U);
            std::lock_guard guard(scenario.provider.lock);
            for (auto const& pending : scenario.provider.pending) EXPECT_EQ(pending.request, nullptr);
        });
    }
    ~Scenario() {
        {
            std::lock_guard guard(provider.lock);
            provider.service_hook = {};
            provider.poll_hook = {};
        }
        sched::block_worker_post_hook.store(nullptr);
        sched::block_worker_entry_hook.store(nullptr);
        sched::block_worker_wake_hook.store(nullptr);
        sched::block_worker_park_hook.store(nullptr);
        ker::mod::sys::block_test_service_unlock_hook = {};
        provider.auto_complete.store(true, std::memory_order_release);
        sched::block_worker_request_shutdown();
        join_workers();
        sched::block_worker_exit_hook.store(nullptr);
        sched::block_worker_exit_argument.store(nullptr);
        if (configured) {
            for (auto& request : requests) {
                if (request.queue != nullptr) {
                    EXPECT_EQ(block_request_wait(request), 0);
                    EXPECT_EQ(block_request_release(request), 0);
                }
            }
            EXPECT_EQ(snapshot().owned_requests, 0U);
            EXPECT_EQ(snapshot().owned_bytes, 0U);
        }
        EXPECT_EQ(provider.invalid_scopes.load(), 0U);
        EXPECT_EQ(callback_scope_errors.load(), 0U);
        EXPECT_EQ(caller.reclaim_no_io_depth, 0U);
        EXPECT_EQ(caller.references.load(), 1U);
        EXPECT_FALSE(caller.shutdown.held());
        EXPECT_EQ(sched::block_worker_abandoned.load(), 0U);
        {
            std::lock_guard guard(sched::task::block_worker_registry_lock);
            for (auto* worker : sched::task::block_worker_tasks) {
                EXPECT_TRUE(worker->finished.load(std::memory_order_acquire));
                EXPECT_EQ(worker->clean_exit_calls.load(), 1U);
                EXPECT_EQ(worker->references.load(), 0U);
                EXPECT_EQ(worker->reclaim_no_io_depth, 0U);
                EXPECT_FALSE(worker->shutdown.held());
                delete worker;
            }
            sched::task::block_worker_tasks.clear();
        }
        sched::block_worker_current_task = nullptr;
        sched::block_worker_interrupts_enabled = true;
    }
    auto configure(size_t depth = 8, bool active_poll = false) -> int {
        static const BlockAsyncOps OPS{Provider::submit, Provider::service, Provider::cancel};
        static const BlockAsyncOps POLLED_OPS{Provider::submit, Provider::service, Provider::cancel, Provider::poll_active};
        int const STATUS = block_queue_configure(&provider.disk, {depth, 512, 32, 64, 64 * 512}, active_poll ? &POLLED_OPS : &OPS);
        configured = STATUS == 0;
        return STATUS;
    }
    auto snapshot() -> BlockQueueSnapshot {
        std::array<BlockQueueSnapshot, 64> snapshots{};
        auto const count = block_queue_snapshots(snapshots);
        for (size_t i = 0; i < count; ++i) {
            if (snapshots[i].name == provider.disk.name) return snapshots[i];
        }
        ADD_FAILURE() << "missing permanent worker queue";
        return {};
    }
    static void callback(BlockRequest& request, void* argument) {
        auto& scenario = *static_cast<Scenario*>(argument);
        auto* current = sched::get_current_task();
        if (current == nullptr || current->reclaim_no_io_depth == 0 || !current->shutdown.held())
            scenario.callback_scope_errors.fetch_add(1, std::memory_order_relaxed);
        for (size_t i = 0; i < scenario.requests.size(); ++i) {
            if (&request == &scenario.requests[i]) scenario.callback_counts[i].fetch_add(1, std::memory_order_relaxed);
        }
    }
    auto submit(size_t index, BlockRequestOperation op = BlockRequestOperation::READ) -> int {
        BlockSegment const segment{buffers[index].data(), buffers[index].size()};
        return block_request_submit(&provider.disk, requests[index], op, index, 1, {&segment, 1}, 30'000'000, callback, this);
    }
    auto submit_for_wait(size_t index, BlockRequestOperation op = BlockRequestOperation::READ) -> int {
        BlockSegment const segment{buffers[index].data(), buffers[index].size()};
        return block_request_submit_for_wait(&provider.disk, requests[index], op, index, 1, {&segment, 1});
    }
    void join_workers() {
        std::array<sched::task::Task*, 64> workers{};
        size_t count = 0;
        {
            std::lock_guard guard(sched::task::block_worker_registry_lock);
            for (auto* worker : sched::task::block_worker_tasks) workers[count++] = worker;
        }
        for (size_t i = 0; i < count; ++i) {
            EXPECT_TRUE(await([&] { return workers[i]->finished.load(std::memory_order_acquire); }));
            if (workers[i]->worker.joinable()) workers[i]->worker.join();
        }
    }
};
}  // namespace

namespace {
void expect_if_off_syscall_service_progress(bool return_yield, bool pending_signal = false, bool submit_for_wait = false,
                                            bool active_poll = false) {
    Gate service_owner;
    Scenario scenario;
    ASSERT_EQ(scenario.configure(8, active_poll), 0);
    scenario.caller.syscall_account_start_us = 1;
    scenario.caller.interrupting_signal_pending = pending_signal;
    // Journald also carries an outer NoIo scope. It restricts recursive reclaim,
    // rather than disabling an otherwise safe syscall scheduling boundary.
    scenario.caller.reclaim_no_io_depth = 1;
    scenario.provider.auto_complete.store(true);
    scenario.provider.service_gate.store(&service_owner);
    scenario.provider.media.fill(std::byte{0x3d});
    ASSERT_EQ(submit_for_wait ? scenario.submit_for_wait(0) : scenario.submit(0), 0);
    block_request_start_workers();
    bool const OWNER_ENTERED = service_owner.await_entered();
    EXPECT_TRUE(OWNER_ENTERED);
    if (!OWNER_ENTERED) {
        service_owner.release();
        scenario.caller.reclaim_no_io_depth = 0;
        return;
    }
    EXPECT_EQ(scenario.snapshot().issued, 1U);
    if (return_yield) {
        sched::block_worker_interrupts_enabled = false;
        sched::block_worker_wake_hook.store([](sched::task::Task*, void*) {
            auto* current = sched::get_current_task();
            EXPECT_EQ(current->type, sched::task::TaskType::PROCESS);
            EXPECT_NE(current->syscall_account_start_us, 0U);
            EXPECT_FALSE(sched::interrupts_enabled());
            // Controlled scheduler wake boundary follows the real producer
            // defer_process_reschedule_to_syscall_exit: channel-free return yield.
            current->yield_switch = true;
            current->deferred_task_switch = true;
            current->wait_channel = nullptr;
            current->wait_channel_kind = sched::task::WaitChannelKind::NONE;
        });
        block_queue_wake(&scenario.provider.disk);
        EXPECT_TRUE(scenario.caller.yield_switch);
        EXPECT_TRUE(scenario.caller.deferred_task_switch);
    }
    struct ParkContext {
        Gate* owner;
        bool return_yield;
        bool pending_signal;
        std::atomic<bool> parked{};
    } context{&service_owner, return_yield, pending_signal};
    sched::block_worker_park_argument.store(&context);
    sched::block_worker_park_hook.store([](void* argument) {
        auto& context = *static_cast<ParkContext*>(argument);
        auto* current = sched::get_current_task();
        EXPECT_FALSE(sched::interrupts_enabled());
        EXPECT_EQ(current->reclaim_no_io_depth, 1U);
        EXPECT_TRUE(current->shutdown.held());
        EXPECT_EQ(current->references.load(), 2U);  // Caller plus retained waiter.
        EXPECT_EQ(current->deferred_task_switch, context.return_yield);
        EXPECT_EQ(current->yield_switch, context.return_yield);
        EXPECT_EQ(current->has_interrupting_signal_pending(), context.pending_signal);
        EXPECT_EQ(ker::mod::sys::block_test_service_try_lock_attempts, 1U);
        context.parked.store(true, std::memory_order_release);
        context.owner->release();  // Same-CPU service owner resumes at this safe park.
    });
    int status = -EINPROGRESS;
    bool resumed_with_interrupts = false;
    unsigned const PARKS = sched::block_worker_syscall_parks.load();
    unsigned const YIELDS = sched::block_worker_safe_yields.load();
    sched::block_worker_current_task = nullptr;
    std::thread waiter([&] {
        sched::block_worker_current_task = &scenario.caller;
        sched::block_worker_interrupts_enabled = false;
        ker::mod::sys::block_test_service_try_lock_attempts = 0;
        status = block_request_wait(scenario.requests[0]);
        resumed_with_interrupts = sched::interrupts_enabled();
        sched::block_worker_current_task = nullptr;
    });
    bool const PARKED = await([&] { return context.parked.load(std::memory_order_acquire); });
    // The pre-fix implementation cannot reach the park. Always release the
    // provider on test failure before joining, so no stack SG/owner escapes.
    service_owner.release();
    waiter.join();
    sched::block_worker_current_task = &scenario.caller;
    scenario.caller.reclaim_no_io_depth = 0;
    EXPECT_TRUE(PARKED);
    // Timed PROCESS_PARK restores admitted syscall IF; the one-shot YIELD
    // preserves entry IF. Both must reach the real-thread owner scheduling edge.
    EXPECT_EQ(resumed_with_interrupts, !pending_signal);
    if (pending_signal) {
        EXPECT_GT(sched::block_worker_safe_yields.load(), YIELDS);
        EXPECT_EQ(sched::block_worker_syscall_parks.load(), PARKS);
    } else {
        EXPECT_GT(sched::block_worker_syscall_parks.load(), PARKS);
        EXPECT_EQ(sched::block_worker_safe_yields.load(), YIELDS);
    }
    EXPECT_EQ(status, 0);
    EXPECT_EQ(scenario.caller.deferred_task_switch, return_yield);
    EXPECT_EQ(scenario.caller.yield_switch, return_yield);
    EXPECT_EQ(scenario.callback_counts[0].load(), submit_for_wait ? 0U : 1U);
    EXPECT_EQ(scenario.caller.references.load(), 1U);
    EXPECT_TRUE(
        std::all_of(scenario.buffers[0].begin(), scenario.buffers[0].end(), [](std::byte byte) { return byte == std::byte{0x3d}; }));
    EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
    EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
    EXPECT_EQ(scenario.provider.poll_calls.load(), 0U);  // Contention parks before any optional extra hook.
}
}  // namespace

TEST(BlockWorkerRuntime, IfOffAccountedSyscallParksToResumeServiceMutexOwner) {
    isolated([] { expect_if_off_syscall_service_progress(false); });
}

namespace {
// Pause only the host scheduler seam before worker entry. Production still
// publishes and retains a real worker; caller service owns the queue normally.
struct WorkerEntryHold {
    Gate& gate;
    explicit WorkerEntryHold(Gate& entry) : gate(entry) {
        sched::block_worker_entry_argument.store(&gate);
        sched::block_worker_entry_hook.store([](void* argument) { static_cast<Gate*>(argument)->enter(); });
    }
    ~WorkerEntryHold() { gate.release(); }
};
}  // namespace

TEST(BlockWorkerRuntime, TaskServicePreAndPostClaimsDoNotWakeRetainedWorker) {
    for (bool pre : {false, true}) {
        isolated([&] {
            Gate entry;
            Scenario scenario;
            WorkerEntryHold hold(entry);
            ASSERT_EQ(scenario.configure(), 0);
            block_request_start_workers();
            ASSERT_TRUE(entry.await_entered());
            scenario.provider.service_completion = true;
            scenario.provider.media.fill(std::byte{0x6b});
            ASSERT_EQ(scenario.submit_for_wait(0), 0);
            if (pre) block_queue_service(&scenario.provider.disk);
            unsigned scope_calls = 0;
            scenario.provider.service_hook = [&](unsigned) {
                ++scope_calls;
                EXPECT_EQ(sched::get_current_task(), &scenario.caller);
                EXPECT_EQ(scenario.caller.references.load(), 2U + SERVICE_OWNER_REFS);  // Task, waiter, service owner.
                EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 1U);
                EXPECT_TRUE(scenario.caller.shutdown.held());
            };
            scenario.provider.auto_complete.store(true);
            unsigned const WAKES = sched::block_worker_wakes.load();
            EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
            EXPECT_EQ(scope_calls, pre ? 1U : 2U);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES);
            EXPECT_EQ(scenario.buffers[0][511], std::byte{0x6b});
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            EXPECT_FALSE(scenario.caller.shutdown.held());
            EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
            EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        });
    }
}

TEST(BlockWorkerRuntime, TaskServiceWorkerBatchRetiresCallbacksWithoutSelfWake) {
    isolated([] {
        Gate service;
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.provider.service_completion = true;
        scenario.provider.auto_complete.store(true);
        scenario.provider.service_gate.store(&service);
        for (size_t i = 0; i < 8; ++i) ASSERT_EQ(scenario.submit(i), 0);
        block_request_start_workers();
        bool const ENTERED = service.await_entered();
        EXPECT_TRUE(ENTERED);
        if (!ENTERED) {
            service.release();
            return;
        }
        sched::task::Task* worker = nullptr;
        {
            std::lock_guard guard(sched::task::block_worker_registry_lock);
            worker = sched::task::block_worker_tasks.front();
        }
        EXPECT_EQ(worker->references.load(), 2U + SERVICE_OWNER_REFS);  // Published, scheduler and service owner.
        EXPECT_EQ(worker->reclaim_no_io_depth, 1U);
        EXPECT_TRUE(worker->shutdown.held());
        unsigned const WAKES = sched::block_worker_wakes.load();
        unsigned const SELF_WAKES = sched::block_worker_self_wakes.load();
        service.release();
        ASSERT_TRUE(await([&] {
            return std::all_of(scenario.requests.begin(), scenario.requests.begin() + 8,
                               [](auto const& request) { return request.completed.load(std::memory_order_acquire); });
        }));
        EXPECT_EQ(sched::block_worker_wakes.load(), WAKES);
        EXPECT_EQ(sched::block_worker_self_wakes.load(), SELF_WAKES);
        for (size_t i = 0; i < 8; ++i) {
            EXPECT_EQ(scenario.callback_counts[i].load(), 1U);
            EXPECT_EQ(block_request_wait(scenario.requests[i]), 0);
            EXPECT_EQ(block_request_release(scenario.requests[i]), 0);
        }
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
    });
}

TEST(BlockWorkerRuntime, GenericSameTaskInterruptClaimStillWakesDuringProviderScope) {
    isolated([] {
        Gate entry;
        Scenario scenario;
        WorkerEntryHold hold(entry);
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(entry.await_entered());
        ASSERT_EQ(scenario.submit_for_wait(0), 0);
        scenario.provider.service_hook = [&](unsigned calls) {
            if (calls != 2) return;
            EXPECT_EQ(sched::get_current_task(), &scenario.caller);
            EXPECT_EQ(scenario.caller.references.load(), 1U + SERVICE_OWNER_REFS);
            // The IRQ-safe API must ignore the matching Task/active scope.
            unsigned const WAKES = sched::block_worker_wakes.load();
            scenario.provider.retire(scenario.requests[0].token, 0);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 1);
            EXPECT_FALSE(scenario.requests[0].completed.load());
        };
        block_queue_service(&scenario.provider.disk);
        EXPECT_TRUE(scenario.requests[0].completed.load());
        EXPECT_EQ(scenario.caller.references.load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
    });
}

TEST(BlockWorkerRuntime, ForeignTaskServiceEntryKeepsWakeWhileOwnerIsPreempted) {
    isolated([] {
        Gate entry;
        Gate provider;
        Scenario scenario;
        WorkerEntryHold hold(entry);
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(entry.await_entered());
        ASSERT_EQ(scenario.submit_for_wait(0), 0);
        block_queue_service(&scenario.provider.disk);
        scenario.provider.service_hook = [&](unsigned) { provider.enter(); };
        sched::task::Task service_task;
        std::thread service([&] {
            sched::block_worker_current_task = &service_task;
            block_queue_service(&scenario.provider.disk);
            sched::block_worker_current_task = nullptr;
        });
        bool const ENTERED = provider.await_entered();
        EXPECT_TRUE(ENTERED);
        if (ENTERED) {
            EXPECT_EQ(service_task.references.load(), 1U + SERVICE_OWNER_REFS);
            unsigned const WAKES = sched::block_worker_wakes.load();
            scenario.provider.retire(scenario.requests[0].token, 0, true);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 1);
            EXPECT_FALSE(scenario.requests[0].completed.load());
        }
        provider.release();
        service.join();
        scenario.provider.service_hook = {};
        EXPECT_EQ(service_task.references.load(), 1U);
        EXPECT_EQ(service_task.reclaim_no_io_depth, 0U);
        EXPECT_FALSE(service_task.shutdown.held());
        EXPECT_TRUE(scenario.requests[0].completed.load());
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
    });
}

TEST(BlockWorkerRuntime, TaskEntryOutsideBootOrFailedOwnerScopeKeepsWorkerWake) {
    for (unsigned mode : {0U, 1U, 2U}) {
        isolated([&] {
            Gate entry;
            Scenario scenario;
            WorkerEntryHold hold(entry);
            ASSERT_EQ(scenario.configure(), 0);
            block_request_start_workers();
            ASSERT_TRUE(entry.await_entered());
            ASSERT_EQ(scenario.submit_for_wait(0), 0);
            block_queue_service(&scenario.provider.disk);
            unsigned const WAKES = sched::block_worker_wakes.load();
            if (mode == 0) {
                scenario.provider.retire(scenario.requests[0].token, 0, true);
            } else {
                scenario.provider.service_completion = true;
                scenario.provider.auto_complete.store(true);
                if (mode == 1) {
                    scenario.provider.allow_boot_scope = true;
                    sched::block_worker_current_task = nullptr;
                } else {
                    scenario.caller.reject_acquire.store(true);
                }
                block_queue_service(&scenario.provider.disk);
                sched::block_worker_current_task = &scenario.caller;
                scenario.caller.reject_acquire.store(false);
            }
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 1);
            block_queue_service(&scenario.provider.disk);
            EXPECT_TRUE(scenario.requests[0].completed.load());
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        });
    }
}

TEST(BlockWorkerRuntime, TaskEntryFinalCallbackClaimWakesAndRetiresOnNextGather) {
    isolated([] {
        Gate entry;
        struct Context {
            Scenario* scenario{};
            unsigned callback_count{};
        } context;
        Scenario scenario;
        context.scenario = &scenario;
        WorkerEntryHold hold(entry);
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(entry.await_entered());
        BlockSegment const segment{scenario.buffers[0].data(), 512};
        ASSERT_EQ(block_request_submit(
                      &scenario.provider.disk, scenario.requests[0], BlockRequestOperation::READ, 0, 1, {&segment, 1}, 30'000'000,
                      [](BlockRequest&, void* argument) {
                          auto& context = *static_cast<Context*>(argument);
                          ++context.callback_count;
                          EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
                          EXPECT_EQ(context.scenario->caller.references.load(), 1U + SERVICE_OWNER_REFS);
                          unsigned const WAKES = sched::block_worker_wakes.load();
                          context.scenario->provider.retire(context.scenario->requests[1].token, 0, true);
                          EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 1);
                      },
                      &context),
                  0);
        ASSERT_EQ(scenario.submit_for_wait(1), 0);
        scenario.provider.service_hook = [&](unsigned calls) {
            if (calls == 2) scenario.provider.retire(scenario.requests[0].token, 0, true);
        };
        block_queue_service(&scenario.provider.disk);
        EXPECT_EQ(context.callback_count, 1U);
        EXPECT_TRUE(scenario.requests[0].completed.load());
        EXPECT_FALSE(scenario.requests[1].completed.load());
        EXPECT_EQ(scenario.snapshot().completing, 1U);
        scenario.provider.service_hook = {};
        entry.release();
        ASSERT_TRUE(await([&] { return scenario.requests[1].completed.load(std::memory_order_acquire); }));
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(block_request_release(scenario.requests[1]), 0);
    });
}

TEST(BlockWorkerRuntime, TaskEntryAfterFinalGatherBeforeUnlockKeepsWorkerWake) {
    isolated([] {
        Gate entry;
        Scenario scenario;
        WorkerEntryHold hold(entry);
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(entry.await_entered());
        ASSERT_EQ(scenario.submit_for_wait(0), 0);
        ASSERT_EQ(scenario.submit_for_wait(1), 0);
        scenario.provider.service_hook = [&](unsigned calls) {
            if (calls == 2) scenario.provider.retire(scenario.requests[0].token, 0, true);
        };
        unsigned late_claims = 0;
        ker::mod::sys::block_test_service_unlock_hook = [&] {
            ker::mod::sys::block_test_service_unlock_hook = {};
            ++late_claims;
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            unsigned const WAKES = sched::block_worker_wakes.load();
            scenario.provider.retire(scenario.requests[1].token, 0, true);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 1);
        };
        block_queue_service(&scenario.provider.disk);
        EXPECT_EQ(late_claims, 1U);
        EXPECT_FALSE(scenario.requests[1].completed.load());
        scenario.provider.service_hook = {};
        entry.release();
        ASSERT_TRUE(await([&] { return scenario.requests[1].completed.load(std::memory_order_acquire); }));
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(block_request_release(scenario.requests[1]), 0);
    });
}

TEST(BlockWorkerRuntime, EmptyServiceUnlockSubmissionAndQueuedCancelWakeWorkerForNextPass) {
    isolated([] {
        Gate entry;
        Scenario scenario;
        WorkerEntryHold hold(entry);
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(entry.await_entered());
        unsigned const WAKES = sched::block_worker_wakes.load();
        unsigned injections = 0;
        ker::mod::sys::block_test_service_unlock_hook = [&] {
            ker::mod::sys::block_test_service_unlock_hook = {};
            ++injections;
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            EXPECT_TRUE(scenario.caller.shutdown.held());
            EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
            EXPECT_EQ(scenario.submit(0), 0);
            EXPECT_EQ(block_request_cancel(scenario.requests[0]), 0);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 2);
        };
        block_queue_service(&scenario.provider.disk);
        EXPECT_EQ(injections, 1U);
        EXPECT_FALSE(scenario.requests[0].completed.load());
        EXPECT_EQ(scenario.snapshot().completing, 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), -EBUSY);
        EXPECT_FALSE(scenario.caller.shutdown.held());
        entry.release();
        ASSERT_TRUE(await([&] { return scenario.requests[0].completed.load(std::memory_order_acquire); }));
        EXPECT_EQ(scenario.requests[0].status, -ECANCELED);
        EXPECT_EQ(scenario.requests[0].bytes, 0U);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(scenario.provider.issued.load(), 0U);
        EXPECT_EQ(block_queue_drain(&scenario.provider.disk), 0);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
    });
}

TEST(BlockWorkerRuntime, EmptyServiceUnlockForWaitQueuedCancelRetiresThroughExplicitJoin) {
    isolated([] {
        Gate entry;
        Scenario scenario;
        WorkerEntryHold hold(entry);
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(entry.await_entered());
        unsigned const WAKES = sched::block_worker_wakes.load();
        ker::mod::sys::block_test_service_unlock_hook = [&] {
            ker::mod::sys::block_test_service_unlock_hook = {};
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            EXPECT_EQ(scenario.submit_for_wait(0), 0);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES);
            EXPECT_EQ(block_request_cancel(scenario.requests[0]), 0);
            EXPECT_EQ(sched::block_worker_wakes.load(), WAKES + 1);
        };
        block_queue_service(&scenario.provider.disk);
        EXPECT_FALSE(scenario.requests[0].completed.load());
        EXPECT_EQ(scenario.snapshot().completing, 1U);
        // The real worker is held at entry. Callback-free admission's caller
        // services/joins before releasing any descriptor or SG storage.
        EXPECT_EQ(block_request_wait(scenario.requests[0]), -ECANCELED);
        EXPECT_EQ(scenario.requests[0].bytes, 0U);
        EXPECT_EQ(scenario.callback_counts[0].load(), 0U);
        EXPECT_EQ(scenario.provider.issued.load(), 0U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.caller.references.load(), 1U);
        EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
        EXPECT_FALSE(scenario.caller.shutdown.held());
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
    });
}

TEST(BlockWorkerRuntime, IfOffInternalReturnYieldParksAndPreservesDeferredMarkers) {
    isolated([] { expect_if_off_syscall_service_progress(true); });
}

TEST(BlockWorkerRuntime, StickySignalYieldsUntilServiceMutexOwnerRetiresBuffers) {
    isolated([] { expect_if_off_syscall_service_progress(false, true); });
}

TEST(BlockWorkerRuntime, StickySignalInternalReturnYieldPreservesMarkersAndRetiresBuffers) {
    isolated([] { expect_if_off_syscall_service_progress(true, true); });
}

TEST(BlockWorkerRuntime, ForWaitContentionKeepsIfOffReturnYieldAndStickySignalOwnerProgress) {
    for (bool const RETURN_YIELD : {false, true}) {
        for (bool const SIGNAL : {false, true}) {
            isolated([&] { expect_if_off_syscall_service_progress(RETURN_YIELD, SIGNAL, true); });
        }
    }
}

TEST(BlockWorkerRuntime, ExtraNativePassCompletesWithNoIoAndRefsUnwoundBeforeReturn) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.caller.syscall_account_start_us = 1;
        scenario.caller.deferred_task_switch = scenario.caller.yield_switch = true;
        sched::block_worker_interrupts_enabled = false;
        scenario.provider.service_hook = [&](unsigned calls) {
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 1U);
            EXPECT_TRUE(scenario.caller.shutdown.held());
            EXPECT_EQ(scenario.caller.references.load(), 2U + SERVICE_OWNER_REFS);  // Waiter and per-pass service owner.
            EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
            if (calls == 3) scenario.provider.auto_complete.store(true);
        };
        ASSERT_EQ(scenario.submit(0), 0);
        unsigned const PARKS = sched::block_worker_syscall_parks.load();
        EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.provider.service_calls, 3U);
        EXPECT_EQ(sched::block_worker_syscall_parks.load(), PARKS);
        EXPECT_FALSE(sched::interrupts_enabled());
        EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
        EXPECT_FALSE(scenario.caller.shutdown.held());
        EXPECT_EQ(scenario.caller.references.load(), 1U);
        EXPECT_TRUE(scenario.caller.deferred_task_switch);
        EXPECT_TRUE(scenario.caller.yield_switch);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
    });
}

TEST(BlockWorkerRuntime, UnsafeAndDaemonRequestWaitsAddNoPollingClocksOrPasses) {
    // Each source-compiled request remains pending for one complete mandatory
    // pass, then completes on the next. No capacity-wait shortcut is used.
    for (unsigned context = 0; context < 12; ++context) {
        isolated([context] {
            Scenario scenario;
            ASSERT_EQ(scenario.configure(), 0);
            auto& task = scenario.caller;
            task.syscall_account_start_us = 1;
            switch (context) {
                case 0:
                    task.syscall_account_start_us = 0;
                    break;
                case 1:
                    task.preempt_disable_depth = 1;
                    break;
                case 2:
                    task.scheduler_transition_active.store(true);
                    break;
                case 3:
                    task.deferred_task_switch = true;
                    break;
                case 4:
                    task.deferred_task_switch = task.yield_switch = true;
                    task.wait_channel = "waitpid";
                    task.wait_channel_kind = sched::task::WaitChannelKind::WAITPID;
                    break;
                case 5:
                    task.wants_block = true;
                    break;
                case 6:
                    task.voluntary_block = true;
                    break;
                case 7:
                    task.wki_proxy_task_id = 9;
                    break;
                case 8:
                    task.wait_channel = "proxy";
                    task.wait_channel_kind = sched::task::WaitChannelKind::WKI_EXECVE_PROXY;
                    break;
                case 9:
                    task.type = sched::task::TaskType::DAEMON;
                    break;
                case 10:
                    task.interrupting_signal_pending = true;
                    break;
                case 11:
                    ker::mod::io::serial::block_test_panic_mode.store(true);
                    break;
            }
            scenario.provider.service_hook = [&](unsigned calls) {
                if (calls == 3) scenario.provider.auto_complete.store(true);
            };
            ASSERT_EQ(scenario.submit(0), 0);
            ker::mod::time::block_test_clock_reads = 0;
            EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
            EXPECT_EQ(scenario.provider.service_calls, 3U);
            // Two mandatory service timestamps and terminal latency. No new
            // window/admission time samples on any ineligible path.
            EXPECT_EQ(ker::mod::time::block_test_clock_reads, 3U);
            EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
            EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
            ker::mod::io::serial::block_test_panic_mode.store(false);
            task.type = sched::task::TaskType::PROCESS;
            task.preempt_disable_depth = 0;
            task.scheduler_transition_active.store(false);
            task.deferred_task_switch = task.yield_switch = false;
            task.wants_block = task.voluntary_block = false;
            task.wki_proxy_task_id = 0;
            task.wait_channel = nullptr;
            task.wait_channel_kind = sched::task::WaitChannelKind::NONE;
            task.interrupting_signal_pending = false;
        });
    }
}

TEST(BlockWorkerRuntime, NativeDepthOneKeepsOriginalWaitWithoutPollingClockReads) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(1), 0);
        scenario.caller.syscall_account_start_us = 1;
        scenario.provider.service_hook = [&](unsigned calls) {
            if (calls == 3) scenario.provider.auto_complete.store(true);
        };
        ASSERT_EQ(scenario.submit(0), 0);
        ker::mod::time::block_test_clock_reads = 0;
        EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
        // Original timed PROCESS park adds its deadline timestamp to the two
        // service timestamps and terminal latency; no polling clocks added.
        EXPECT_EQ(ker::mod::time::block_test_clock_reads, 4U);
        EXPECT_EQ(scenario.provider.service_calls, 3U);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
    });
}

TEST(BlockWorkerRuntime, ContextBecomingUnsafeDuringInitialServiceAdmitsNoExtraPassOrClock) {
    for (unsigned context = 0; context < 6; ++context) {
        isolated([context] {
            Scenario scenario;
            ASSERT_EQ(scenario.configure(), 0);
            scenario.caller.syscall_account_start_us = 1;
            scenario.provider.service_hook = [&](unsigned calls) {
                auto& task = scenario.caller;
                if (calls == 2) {
                    switch (context) {
                        case 0:
                            task.syscall_account_start_us = 0;
                            break;
                        case 1:
                            task.preempt_disable_depth = 1;
                            break;
                        case 2:
                            task.scheduler_transition_active.store(true);
                            break;
                        case 3:
                            task.deferred_task_switch = true;
                            break;
                        case 4:
                            task.wki_proxy_task_id = 9;
                            break;
                        case 5:
                            task.wants_block = true;
                            break;
                    }
                }
                if (calls == 3) scenario.provider.auto_complete.store(true);
            };
            ASSERT_EQ(scenario.submit(0), 0);
            ker::mod::time::block_test_clock_reads = 0;
            EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
            // Eligible initial window, two mandatory services, terminal latency.
            // Fresh rejection admits neither an extra pass nor its clock read.
            EXPECT_EQ(ker::mod::time::block_test_clock_reads, 4U);
            EXPECT_EQ(scenario.provider.service_calls, 3U);
            EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
            EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
            scenario.caller.preempt_disable_depth = 0;
            scenario.caller.scheduler_transition_active.store(false);
            scenario.caller.deferred_task_switch = false;
            scenario.caller.wki_proxy_task_id = 0;
            scenario.caller.wants_block = false;
        });
    }
}

TEST(BlockWorkerRuntime, ClosingAndShutdownDuringMandatoryServiceSkipExtrasButRetireOwners) {
    for (auto const boundary : {2U, 4U}) {
        for (bool shutdown : {false, true}) {
            isolated([shutdown, boundary] {
                Scenario scenario;
                ASSERT_EQ(scenario.configure(), 0);
                scenario.caller.syscall_account_start_us = 1;
                scenario.provider.service_hook = [&](unsigned calls) {
                    if (calls == boundary) {
                        if (shutdown)
                            sched::block_worker_shutdown.store(true);
                        else
                            block_queue_close(&scenario.provider.disk);
                    }
                };
                unsigned first_pause_calls = 0;
                struct ParkContext {
                    Scenario* scenario;
                    unsigned boundary;
                } context{&scenario, boundary};
                sched::block_worker_park_argument.store(&context);
                sched::block_worker_park_hook.store([](void* argument) {
                    auto const& context = *static_cast<ParkContext*>(argument);
                    auto& scenario = *context.scenario;
                    unsigned const EXPECTED = context.boundary == 4 && !sched::block_worker_shutdown.load() ? 5 : context.boundary;
                    EXPECT_EQ(scenario.provider.service_calls, EXPECTED);
                    EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
                    EXPECT_TRUE(scenario.caller.shutdown.held());
                    scenario.provider.auto_complete.store(true);
                });
                ASSERT_EQ(scenario.submit(0), 0);
                EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
                first_pause_calls = scenario.provider.service_calls;
                // Shutdown rejects extra polling but leaves queue health open;
                // completion in the following PRE therefore needs no POST.
                EXPECT_EQ(first_pause_calls, shutdown ? boundary + 1 : boundary == 4 ? 7U : 4U);
                EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
                EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
            });
        }
    }
}

TEST(BlockWorkerRuntime, CallerDrivenFinalizerBridgeSkipsExtrasAndUnwindsNoIoScopes) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.caller.syscall_account_start_us = 1;
        sched::task::block_worker_fail_create.store(true);
        block_request_start_workers();
        EXPECT_TRUE(scenario.snapshot().caller_driven);
        sched::block_worker_park_argument.store(&scenario);
        sched::block_worker_park_hook.store([](void* argument) {
            auto& scenario = *static_cast<Scenario*>(argument);
            EXPECT_EQ(scenario.provider.service_calls, 2U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            EXPECT_TRUE(scenario.caller.shutdown.held());
            scenario.provider.auto_complete.store(true);
        });
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::READ, 0, 1, scenario.buffers[0].data()), 0);
        EXPECT_EQ(scenario.provider.service_calls, 4U);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
        EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
        EXPECT_EQ(scenario.caller.references.load(), 1U);
        EXPECT_FALSE(scenario.caller.shutdown.held());
    });
}

TEST(BlockWorkerRuntime, ForWaitBatchDoesNotWakeIdleWorkerBeforeCallerServicesAndJoinsEveryOwner) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.caller.syscall_account_start_us = 1;
        scenario.provider.auto_complete.store(true);
        scenario.provider.media.fill(std::byte{0x7d});
        std::atomic<unsigned> caller_services{};
        std::atomic<unsigned> worker_services{};
        scenario.provider.service_hook = [&](unsigned) {
            if (sched::get_current_task() == &scenario.caller)
                caller_services.fetch_add(1, std::memory_order_relaxed);
            else
                worker_services.fetch_add(1, std::memory_order_relaxed);
        };
        block_request_start_workers();
        auto* worker = sched::task::block_worker_tasks[0];
        ASSERT_TRUE(await([&] {
            std::lock_guard guard(worker->wake_lock);
            return sched::block_worker_idle_waits.load(std::memory_order_acquire) != 0 && !worker->wake_pending;
        }));
        unsigned const WAKES = sched::block_worker_wakes.load();
        unsigned const SELF_WAKES = sched::block_worker_self_wakes.load();
        unsigned const WORKER_ACQUIRES = worker->lifetime_acquires.load();
        unsigned const WORKER_SERVICES = worker_services.load();
        for (size_t i = 0; i < 8; ++i) ASSERT_EQ(scenario.submit_for_wait(i), 0);
        EXPECT_EQ(sched::block_worker_wakes.load(), WAKES);
        EXPECT_EQ(worker->lifetime_acquires.load(), WORKER_ACQUIRES);
        EXPECT_EQ(scenario.snapshot().queued, 8U);
        EXPECT_EQ(scenario.provider.issued.load(), 0U);
        for (size_t i = 0; i < 8; ++i) {
            EXPECT_EQ(scenario.requests[i].callback, nullptr);
            EXPECT_EQ(block_request_wait(scenario.requests[i]), 0);
            EXPECT_EQ(scenario.requests[i].bytes, 512U);
            EXPECT_EQ(scenario.callback_counts[i].load(), 0U);
            EXPECT_TRUE(std::all_of(scenario.buffers[i].begin(), scenario.buffers[i].end(),
                                    [](std::byte byte) { return byte == std::byte{0x7d}; }));
            EXPECT_EQ(block_request_release(scenario.requests[i]), 0);
        }
        EXPECT_EQ(caller_services.load(), 2U);                    // Pre-service, issue batch, post-service.
        EXPECT_EQ(sched::block_worker_wakes.load() - WAKES, 8U);  // All generic completion claim wakes remain.
        EXPECT_EQ(sched::block_worker_self_wakes.load(), SELF_WAKES);
        ASSERT_TRUE(await([&] {
            std::lock_guard guard(worker->wake_lock);
            return sched::block_worker_idle_waits.load(std::memory_order_acquire) != 0 && !worker->wake_pending;
        }));
        // Healthy no-attempt worker passes have one PRE call; each adds one
        // service-owner ref independently of the unchanged eight wake refs.
        EXPECT_EQ(worker->lifetime_acquires.load() - WORKER_ACQUIRES, 8U + SERVICE_OWNER_REFS * (worker_services.load() - WORKER_SERVICES));
        EXPECT_EQ(worker->references.load(), 2U);
        EXPECT_EQ(scenario.caller.references.load(), 1U);
        EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
        EXPECT_FALSE(scenario.caller.shutdown.held());
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
        std::printf("BLOCK_FOR_WAIT_WAKE_COST admitted=8 admission_wakes=0 completion_wakes=8\n");
        std::fflush(stdout);
    });
}

TEST(BlockWorkerRuntime, AcceptedBusyOwnerProgressesThroughTimedWorkerPrePassesWithoutAnotherWake) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(await([] { return sched::block_worker_idle_waits.load(std::memory_order_acquire) != 0; }));
        std::atomic<unsigned> owned_services{};
        {
            std::lock_guard guard(scenario.provider.lock);
            scenario.provider.service_hook = [&](unsigned) {
                // Completion may wake another healthy PRE before the observer
                // resumes; only service of this still-owned request counts.
                if (scenario.provider.issued.load(std::memory_order_acquire) != 0 &&
                    !scenario.requests[0].completed.load(std::memory_order_acquire) &&
                    owned_services.fetch_add(1, std::memory_order_relaxed) + 1 == 3)
                    scenario.provider.auto_complete.store(true, std::memory_order_release);
            };
        }
        unsigned const SLEEPS = sched::block_worker_daemon_sleeps.load();
        ASSERT_EQ(scenario.submit(0), 0);  // Ordinary admission supplies the only initial wake.
        ASSERT_TRUE(await([&] { return scenario.requests[0].completed.load(std::memory_order_acquire); }));
        EXPECT_EQ(owned_services.load(), 3U);  // Accepted POST, then two timed healthy PREs.
        EXPECT_GE(sched::block_worker_daemon_sleeps.load() - SLEEPS, 2U);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(scenario.requests[0].status, 0);
        EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
    });
}

TEST(BlockWorkerRuntime, ActivePollDoesNotReplaceTimedWorkerPrePasses) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(8, true), 0);
        block_request_start_workers();
        ASSERT_TRUE(await([] { return sched::block_worker_idle_waits.load(std::memory_order_acquire) != 0; }));
        std::atomic<unsigned> owned_services{};
        {
            std::lock_guard guard(scenario.provider.lock);
            scenario.provider.service_hook = [&](unsigned) {
                // Keep the exact three owned passes independent of a later
                // healthy PRE caused by the preserved completion wake.
                if (scenario.provider.issued.load(std::memory_order_acquire) != 0 &&
                    !scenario.requests[0].completed.load(std::memory_order_acquire) &&
                    owned_services.fetch_add(1, std::memory_order_relaxed) + 1 == 3)
                    scenario.provider.auto_complete.store(true, std::memory_order_release);
            };
        }
        unsigned const SLEEPS = sched::block_worker_daemon_sleeps.load();
        ASSERT_EQ(scenario.submit(0), 0);
        ASSERT_TRUE(await([&] { return scenario.requests[0].completed.load(std::memory_order_acquire); }));
        EXPECT_EQ(owned_services.load(), 3U);
        EXPECT_GE(sched::block_worker_daemon_sleeps.load() - SLEEPS, 2U);
        EXPECT_EQ(scenario.provider.poll_calls.load(), 0U);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
    });
}

TEST(BlockWorkerRuntime, ActivePollRetainsCallerScopeAndEveryGenericCompletionWake) {
    for (bool shortcut : {false, true}) {
        isolated([shortcut] {
            Gate entry;
            Scenario scenario;
            WorkerEntryHold hold(entry);
            ASSERT_EQ(scenario.configure(8, true), 0);
            block_request_start_workers();
            ASSERT_TRUE(entry.await_entered());
            scenario.caller.syscall_account_start_us = 1;
            sched::block_worker_interrupts_enabled = false;
            scenario.provider.poll_result = shortcut;
            scenario.provider.poll_hook = [&](unsigned) {
                EXPECT_EQ(sched::get_current_task(), &scenario.caller);
                EXPECT_EQ(scenario.caller.references.load(), 2U + SERVICE_OWNER_REFS);
                EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 1U);
                EXPECT_TRUE(scenario.caller.shutdown.held());
                EXPECT_FALSE(scenario.requests[0].completed.load());
                EXPECT_FALSE(scenario.requests[1].completed.load());
            };
            scenario.provider.service_hook = [&](unsigned calls) {
                if (!shortcut && calls == 3) scenario.provider.auto_complete.store(true);
            };
            struct Context {
                Scenario* scenario;
                bool shortcut;
                unsigned parks{};
            } context{&scenario, shortcut};
            sched::block_worker_park_argument.store(&context);
            sched::block_worker_park_hook.store([](void* argument) {
                auto& context = *static_cast<Context*>(argument);
                ++context.parks;
                EXPECT_TRUE(context.shortcut);
                EXPECT_EQ(context.scenario->provider.poll_calls.load(), 32U);
                EXPECT_EQ(context.scenario->provider.service_calls, 2U);
                EXPECT_EQ(context.scenario->caller.references.load(), 2U);
                EXPECT_EQ(context.scenario->caller.reclaim_no_io_depth, 0U);
                context.scenario->provider.auto_complete.store(true);
            });
            ASSERT_EQ(scenario.submit_for_wait(0), 0);
            ASSERT_EQ(scenario.submit_for_wait(1), 0);
            unsigned const WAKES = sched::block_worker_wakes.load();
            EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
            EXPECT_EQ(context.parks, shortcut ? 1U : 0U);
            EXPECT_EQ(scenario.provider.poll_calls.load(), shortcut ? 32U : 1U);
            EXPECT_EQ(scenario.provider.service_calls, 3U);
            EXPECT_EQ(sched::block_worker_wakes.load() - WAKES, 2U);
            EXPECT_TRUE(scenario.requests[1].completed.load());
            EXPECT_EQ(block_request_wait(scenario.requests[1]), 0);
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            EXPECT_FALSE(scenario.caller.shutdown.held());
            for (size_t i = 0; i < 2; ++i) {
                EXPECT_EQ(scenario.callback_counts[i].load(), 0U);
                EXPECT_EQ(block_request_release(scenario.requests[i]), 0);
            }
            EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        });
    }
}

TEST(BlockWorkerRuntime, ActivePollKeepsIneligibleRequestWaitsOnFullService) {
    for (unsigned context = 0; context < 7; ++context) {
        isolated([context] {
            Scenario scenario;
            ASSERT_EQ(scenario.configure(context == 6 ? 1 : 8, true), 0);
            auto& task = scenario.caller;
            task.syscall_account_start_us = 1;
            if (context < 2) task.type = sched::task::TaskType::DAEMON;
            if (context == 2) task.syscall_account_start_us = 0;
            if (context == 3) task.preempt_disable_depth = 1;
            if (context == 4) task.wki_proxy_task_id = 9;
            if (context == 5) task.interrupting_signal_pending = true;
            sched::block_worker_interrupts_enabled = context == 0;
            scenario.provider.service_hook = [&](unsigned calls) {
                if (calls == 3) scenario.provider.auto_complete.store(true);
            };
            ASSERT_EQ(scenario.submit(0), 0);
            EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
            EXPECT_EQ(scenario.provider.service_calls, 3U);
            EXPECT_EQ(scenario.provider.poll_calls.load(), 0U);
            EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
            EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
            task.type = sched::task::TaskType::PROCESS;
            task.preempt_disable_depth = 0;
            task.wki_proxy_task_id = 0;
            task.interrupting_signal_pending = false;
        });
    }
}

TEST(BlockWorkerRuntime, ActivePollContentionRetainsIfOffOwnerProgressAndStickyYield) {
    for (bool signal : {false, true}) {
        isolated([signal] { expect_if_off_syscall_service_progress(true, signal, true, true); });
    }
}

TEST(BlockWorkerRuntime, ActivePollIsExcludedAfterWorkerRetirementAndDuringFinalizerIo) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(8, true), 0);
        scenario.caller.syscall_account_start_us = 1;
        scenario.provider.auto_complete.store(true);
        block_request_start_workers();
        sched::block_worker_request_shutdown();
        scenario.join_workers();
        EXPECT_EQ(scenario.submit_for_wait(0), -ESHUTDOWN);
        for (auto const OP : {BlockRequestOperation::WRITE, BlockRequestOperation::READ, BlockRequestOperation::FLUSH}) {
            bool const FLUSH = OP == BlockRequestOperation::FLUSH;
            EXPECT_EQ(block_request_sync(&scenario.provider.disk, OP, 0, FLUSH ? 0 : 1, FLUSH ? nullptr : scenario.buffers[0].data()), 0);
        }
        EXPECT_EQ(scenario.provider.poll_calls.load(), 0U);
        EXPECT_EQ(scenario.provider.flushes.load(), 1U);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
    });
}

TEST(BlockWorkerRuntime, SynchronousBridgeDefersAdmissionWakeAndPreservesEachCompletionWake) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.caller.syscall_account_start_us = 1;
        scenario.provider.auto_complete.store(true);
        block_request_start_workers();
        auto* worker = sched::task::block_worker_tasks[0];
        scenario.buffers[0].fill(std::byte{0x3c});
        for (auto const OP : {BlockRequestOperation::WRITE, BlockRequestOperation::READ, BlockRequestOperation::FLUSH}) {
            ASSERT_TRUE(await([&] {
                std::lock_guard guard(worker->wake_lock);
                return sched::block_worker_idle_waits.load(std::memory_order_acquire) != 0 && !worker->wake_pending;
            }));
            unsigned const WAKES = sched::block_worker_wakes.load();
            bool const FLUSH = OP == BlockRequestOperation::FLUSH;
            void* buffer = OP == BlockRequestOperation::READ ? scenario.buffers[1].data() : scenario.buffers[0].data();
            EXPECT_EQ(block_request_sync(&scenario.provider.disk, OP, 0, FLUSH ? 0 : 1, FLUSH ? nullptr : buffer), 0);
            EXPECT_EQ(sched::block_worker_wakes.load() - WAKES, 1U);  // Completion claim only.
            EXPECT_EQ(scenario.caller.references.load(), 1U);
            EXPECT_EQ(scenario.caller.reclaim_no_io_depth, 0U);
            EXPECT_FALSE(scenario.caller.shutdown.held());
            EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        }
        EXPECT_EQ(scenario.buffers[0], scenario.buffers[1]);
        EXPECT_EQ(scenario.provider.flushes.load(), 1U);
    });
}

TEST(BlockWorkerRuntime, MeasuresSubmitClaimAndWaiterWakesWithoutChangingTheirSemantics) {
    isolated([] {
        Gate service_owner;
        std::atomic<unsigned> caller_services{};
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.caller.syscall_account_start_us = 1;
        scenario.provider.auto_complete.store(true);
        scenario.provider.service_gate.store(&service_owner);
        scenario.provider.service_hook = [&](unsigned) {
            if (sched::get_current_task() == &scenario.caller) caller_services.fetch_add(1, std::memory_order_relaxed);
        };
        block_request_start_workers();
        EXPECT_TRUE(await([] { return sched::block_worker_held_parks.load() != 0; }));
        unsigned const WAKES = sched::block_worker_wakes.load();
        unsigned const SELF_WAKES = sched::block_worker_self_wakes.load();
        unsigned const CALLER_ACQUIRES = scenario.caller.lifetime_acquires.load();
        ASSERT_EQ(scenario.submit(0), 0);
        bool const ENTERED = service_owner.await_entered();
        EXPECT_TRUE(ENTERED);
        if (!ENTERED) {
            service_owner.release();
            return;
        }
        sched::block_worker_park_argument.store(&service_owner);
        sched::block_worker_park_hook.store([](void* argument) {
            EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
            EXPECT_EQ(sched::get_current_task()->references.load(), 2U);
            static_cast<Gate*>(argument)->release();
        });
        EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
        bool const RETIRED = await([&] {
            return scenario.caller.references.load() == 1 && sched::block_worker_wakes.load() - WAKES >= 3 &&
                   sched::block_worker_self_wakes.load() - SELF_WAKES >= 1;
        });
        EXPECT_TRUE(RETIRED);
        unsigned const TOTAL = sched::block_worker_wakes.load() - WAKES;
        unsigned const SELF = sched::block_worker_self_wakes.load() - SELF_WAKES;
        EXPECT_EQ(TOTAL, 3U);  // Submit -> worker, claim -> self, publication -> waiter.
        EXPECT_EQ(SELF, 1U);
        // The waiter can win an empty PRE after the worker publishes its
        // result. Count that legitimate per-pass owner independently of the
        // unchanged waiter and foreign publication-wake references.
        EXPECT_EQ(scenario.caller.lifetime_acquires.load() - CALLER_ACQUIRES, 2U + SERVICE_OWNER_REFS * caller_services.load());
        // Fork isolation does not propagate child GTest XML properties; retain
        // an explicit flushed measurement line for the host receipt instead.
        std::printf("BLOCK_WORKER_WAKE_COST total=%u self=%u\n", TOTAL, SELF);
        std::fflush(stdout);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
    });
}

TEST(BlockWorkerRuntime, CurrentWaiterThreadRetiresWithoutItsOwnWakeOrExtraTemporaryReference) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        scenario.provider.auto_complete.store(true);
        ASSERT_EQ(scenario.submit(0), 0);
        unsigned const WAKES = sched::block_worker_wakes.load();
        unsigned const SELF_WAKES = sched::block_worker_self_wakes.load();
        unsigned const ACQUIRES = scenario.caller.lifetime_acquires.load();
        int status = -EINPROGRESS;
        sched::block_worker_current_task = nullptr;
        std::thread waiter([&] {
            sched::block_worker_current_task = &scenario.caller;
            sched::block_worker_interrupts_enabled = false;
            status = block_request_wait(scenario.requests[0]);
            sched::block_worker_current_task = nullptr;
        });
        waiter.join();
        sched::block_worker_current_task = &scenario.caller;
        EXPECT_EQ(status, 0);
        EXPECT_EQ(sched::block_worker_wakes.load() - WAKES, 0U);
        EXPECT_EQ(sched::block_worker_self_wakes.load() - SELF_WAKES, 0U);
        EXPECT_EQ(scenario.caller.lifetime_acquires.load() - ACQUIRES, 1U + SERVICE_OWNER_REFS);  // Wait and one service pass.
        EXPECT_EQ(scenario.caller.references.load(), 1U);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
    });
}

TEST(BlockWorkerRuntime, UnsafeWaitContextsDoNotParkOrEnableInterrupts) {
    isolated([] {
        Scenario scenario;
        auto& current = scenario.caller;
        current.syscall_account_start_us = 1;
        current.interrupting_signal_pending = true;
        auto pause_only = [&] {
            unsigned const PARKS = sched::block_worker_syscall_parks.load();
            unsigned const YIELDS = sched::block_worker_safe_yields.load();
            unsigned const SLEEPS = sched::block_worker_daemon_sleeps.load();
            sched::block_worker_interrupts_enabled = false;
            block_queue_wait_for_capacity(nullptr);
            EXPECT_FALSE(sched::interrupts_enabled());
            EXPECT_EQ(sched::block_worker_syscall_parks.load(), PARKS);
            EXPECT_EQ(sched::block_worker_safe_yields.load(), YIELDS);
            EXPECT_EQ(sched::block_worker_daemon_sleeps.load(), SLEEPS);
        };
        current.syscall_account_start_us = 0;
        pause_only();
        current.syscall_account_start_us = 1;
        current.preempt_disable_depth = 1;
        pause_only();
        current.preempt_disable_depth = 0;
        current.scheduler_transition_active.store(true);
        pause_only();
        current.scheduler_transition_active.store(false);
        current.deferred_task_switch = true;
        pause_only();
        current.yield_switch = true;
        current.wait_channel = "waitpid";
        current.wait_channel_kind = sched::task::WaitChannelKind::WAITPID;
        pause_only();
        current.wait_channel = nullptr;  // An inconsistent channel kind is not an internal yield.
        pause_only();
        current.deferred_task_switch = false;
        current.yield_switch = false;
        current.wait_channel_kind = sched::task::WaitChannelKind::NONE;
        current.wants_block = true;
        pause_only();
        current.wants_block = false;
        current.voluntary_block = true;
        pause_only();
        current.voluntary_block = false;
        current.wki_proxy_task_id = 17;
        pause_only();
        current.wki_proxy_task_id = 0;
        current.wait_channel = "wki_execve_proxy";
        current.wait_channel_kind = sched::task::WaitChannelKind::WKI_EXECVE_PROXY;
        pause_only();
        current.wait_channel = nullptr;
        current.wait_channel_kind = sched::task::WaitChannelKind::NONE;
        ker::mod::io::serial::block_test_panic_mode.store(true);
        pause_only();
        ker::mod::io::serial::block_test_panic_mode.store(false);
        sched::block_worker_run_queues.store(false);
        pause_only();
        sched::block_worker_run_queues.store(true);
        sched::block_worker_current_task = nullptr;
        pause_only();
        sched::block_worker_current_task = &current;
        current.type = sched::task::TaskType::DAEMON;
        pause_only();
        current.type = sched::task::TaskType::IDLE;
        pause_only();
        current.type = sched::task::TaskType::PROCESS;
    });
}

TEST(BlockWorkerRuntime, IfEnabledDaemonRetainsItsTimedSleepPath) {
    isolated([] {
        Scenario scenario;
        scenario.caller.type = sched::task::TaskType::DAEMON;
        unsigned const PARKS = sched::block_worker_syscall_parks.load();
        unsigned const SLEEPS = sched::block_worker_daemon_sleeps.load();
        sched::block_worker_interrupts_enabled = true;
        block_queue_wait_for_capacity(nullptr);
        EXPECT_EQ(sched::block_worker_syscall_parks.load(), PARKS);
        EXPECT_EQ(sched::block_worker_daemon_sleeps.load(), SLEEPS + 1);
        EXPECT_TRUE(sched::interrupts_enabled());
        scenario.caller.type = sched::task::TaskType::PROCESS;
    });
}

TEST(BlockWorkerRuntime, FailedPostHidesStartingWorkerAndReleasesUnpublishedReferences) {
    isolated([] {
        Gate post;
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        sched::block_worker_fail_post.store(true);
        sched::block_worker_post_argument.store(&post);
        sched::block_worker_post_hook.store([](void* argument) { static_cast<Gate*>(argument)->enter(); });
        std::thread launch([] { block_request_start_workers(); });
        EXPECT_TRUE(post.await_entered());
        for (size_t i = 0; i < 100; ++i) block_queue_wake(&scenario.provider.disk);
        EXPECT_EQ(sched::block_worker_wakes.load(), 0U);
        {
            std::lock_guard guard(sched::task::block_worker_registry_lock);
            EXPECT_EQ(sched::task::block_worker_tasks.size(), 1U);
            if (!sched::task::block_worker_tasks.empty()) EXPECT_EQ(sched::task::block_worker_tasks[0]->references.load(), 2U);
        }
        post.release();
        launch.join();
        EXPECT_EQ(sched::task::block_worker_failed_cleanup.load(), 1U);
        EXPECT_TRUE(sched::task::block_worker_tasks.empty());
        EXPECT_TRUE(block_queue_configured(&scenario.provider.disk));
        EXPECT_FALSE(block_queue_available(&scenario.provider.disk));
        EXPECT_EQ(scenario.submit(0), -ESHUTDOWN);
        EXPECT_EQ(scenario.requests[0].queue, nullptr);
        scenario.provider.auto_complete.store(true);
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::READ, 0, 1, scenario.buffers[0].data()), 0);
        EXPECT_GT(scenario.provider.scope_checks.load(), 0U);
    });
}

TEST(BlockWorkerRuntime, FailedCreationKeepsOnlyCallerDrivenServiceAndBalancedOwners) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        sched::task::block_worker_fail_create.store(true);
        block_request_start_workers();
        EXPECT_TRUE(sched::task::block_worker_tasks.empty());
        EXPECT_EQ(sched::task::block_worker_failed_cleanup.load(), 0U);
        EXPECT_FALSE(block_queue_available(&scenario.provider.disk));
        EXPECT_EQ(scenario.submit(0), -ESHUTDOWN);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        scenario.provider.auto_complete.store(true);
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::FLUSH, 0, 0, nullptr), 0);
        EXPECT_EQ(scenario.provider.flushes.load(), 1U);
    });
}

TEST(BlockWorkerRuntime, TemporaryWakeReferenceSurvivesWorkerShutdownAndQueueUnpublish) {
    isolated([] {
        Gate wake;
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_TRUE(await([] { return sched::block_worker_held_parks.load() != 0; }));
        auto* worker = sched::task::block_worker_tasks[0];
        sched::block_worker_wake_argument.store(&wake);
        sched::block_worker_wake_hook.store([](sched::task::Task*, void* argument) { static_cast<Gate*>(argument)->enter(); });
        std::thread waker([&] { block_queue_wake(&scenario.provider.disk); });
        EXPECT_TRUE(wake.await_entered());
        sched::block_worker_request_shutdown();
        EXPECT_TRUE(await([&] { return worker->finished.load(std::memory_order_acquire); }));
        EXPECT_EQ(worker->references.load(), 1U);  // Only the blocked wake retains it.
        EXPECT_FALSE(block_queue_available(&scenario.provider.disk));
        wake.release();
        waker.join();
        scenario.join_workers();
        EXPECT_EQ(worker->references.load(), 0U);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
    });
}

TEST(BlockWorkerRuntime, ShutdownDrainsAcceptedTransportAndRetainsFinalizerReadWriteFlush) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        scenario.buffers[0].fill(std::byte{0xa7});
        ASSERT_EQ(scenario.submit(0, BlockRequestOperation::WRITE), 0);
        ASSERT_TRUE(await([&] { return scenario.provider.issued.load() == 1; }));
        auto* worker = sched::task::block_worker_tasks[0];
        sched::block_worker_request_shutdown();
        EXPECT_FALSE(worker->finished.load());
        EXPECT_FALSE(scenario.requests[0].completed.load());
        EXPECT_TRUE(block_queue_available(&scenario.provider.disk));
        EXPECT_EQ(scenario.snapshot().owned_requests, 1U);
        scenario.provider.auto_complete.store(true, std::memory_order_release);
        block_queue_wake(&scenario.provider.disk);
        scenario.join_workers();
        EXPECT_TRUE(scenario.requests[0].completed.load());
        EXPECT_EQ(scenario.requests[0].status, 0);
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        EXPECT_TRUE(block_queue_configured(&scenario.provider.disk));
        EXPECT_FALSE(block_queue_available(&scenario.provider.disk));
        EXPECT_EQ(scenario.submit(1), -ESHUTDOWN);
        EXPECT_EQ(scenario.requests[1].queue, nullptr);
        EXPECT_EQ(scenario.submit_for_wait(1), -ESHUTDOWN);
        EXPECT_EQ(scenario.requests[1].queue, nullptr);
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::READ, 0, 1, scenario.buffers[1].data()), 0);
        EXPECT_EQ(scenario.buffers[1], scenario.buffers[0]);
        scenario.buffers[2].fill(std::byte{0x3d});
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::WRITE, 2, 1, scenario.buffers[2].data()), 0);
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::READ, 2, 1, scenario.buffers[3].data()), 0);
        EXPECT_EQ(scenario.buffers[3], scenario.buffers[2]);
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::FLUSH, 0, 0, nullptr), 0);
        EXPECT_EQ(scenario.provider.flushes.load(), 1U);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
        EXPECT_EQ(scenario.provider.invalid_scopes.load(), 0U);
        EXPECT_EQ(scenario.callback_scope_errors.load(), 0U);
    });
}

TEST(BlockWorkerRuntime, ForWaitAdmissionRacingShutdownJoinsAcceptedOwnersAndRejectsRetiredWorkerAdmission) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_EQ(scenario.submit(0), 0);
        ASSERT_TRUE(await([&] { return scenario.provider.issued.load() == 1; }));
        constexpr size_t RACERS = 16;
        std::array<sched::task::Task, RACERS> callers{};
        std::array<std::thread, RACERS> threads{};
        std::array<int, RACERS> admissions{};
        std::array<int, RACERS> retired{};
        std::barrier start(RACERS + 1);
        for (size_t i = 0; i < RACERS; ++i) {
            callers[i].type = sched::task::TaskType::PROCESS;
            callers[i].syscall_account_start_us = 1;
            threads[i] = std::thread([&, i] {
                sched::block_worker_current_task = &callers[i];
                sched::block_worker_interrupts_enabled = false;
                start.arrive_and_wait();
                admissions[i] = scenario.submit_for_wait(i + 1);
                if (admissions[i] == 0) {
                    // The accepting task joins before returning; the caller
                    // contract never transfers its retirement responsibility.
                    retired[i] = block_request_wait(scenario.requests[i + 1]);
                    EXPECT_EQ(block_request_release(scenario.requests[i + 1]), 0);
                }
                EXPECT_EQ(callers[i].reclaim_no_io_depth, 0U);
                EXPECT_FALSE(callers[i].shutdown.held());
                sched::block_worker_current_task = nullptr;
            });
        }
        start.arrive_and_wait();
        sched::block_worker_request_shutdown();
        scenario.provider.auto_complete.store(true, std::memory_order_release);
        block_queue_wake(&scenario.provider.disk);
        for (auto& thread : threads) thread.join();
        scenario.join_workers();
        EXPECT_EQ(block_request_wait(scenario.requests[0]), 0);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        size_t accepted = 1;
        for (size_t i = 0; i < RACERS; ++i) {
            // completed publishes before an other-task wake drops its temporary
            // reference. Every service executor has now joined, so those refs
            // have also retired before stack Task storage may be destroyed.
            EXPECT_EQ(callers[i].references.load(), 1U);
            if (admissions[i] == 0) {
                ++accepted;
                EXPECT_EQ(retired[i], 0);
                EXPECT_TRUE(scenario.requests[i + 1].completed.load());
                EXPECT_EQ(scenario.requests[i + 1].bytes, 512U);
            } else {
                EXPECT_EQ(admissions[i], -ESHUTDOWN);
            }
            EXPECT_EQ(scenario.requests[i + 1].queue, nullptr);
            EXPECT_EQ(scenario.callback_counts[i + 1].load(), 0U);
        }
        EXPECT_EQ(scenario.provider.issued.load(), accepted);
        EXPECT_EQ(scenario.submit_for_wait(20), -ESHUTDOWN);
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
        EXPECT_FALSE(block_queue_available(&scenario.provider.disk));
        EXPECT_EQ(block_request_sync(&scenario.provider.disk, BlockRequestOperation::FLUSH, 0, 0, nullptr), 0);
    });
}

TEST(BlockWorkerRuntime, ConcurrentAdmissionAndShutdownRetireEveryAcceptedOwnerExactlyOnce) {
    isolated([] {
        Scenario scenario;
        ASSERT_EQ(scenario.configure(), 0);
        block_request_start_workers();
        ASSERT_EQ(scenario.submit(0), 0);
        ASSERT_TRUE(await([&] { return scenario.provider.issued.load() == 1; }));
        constexpr size_t RACERS = 32;
        std::array<std::thread, RACERS> admissions{};
        std::array<int, RACERS> results{};
        std::barrier start(RACERS + 1);
        for (size_t i = 0; i < RACERS; ++i) {
            admissions[i] = std::thread([&, i] {
                start.arrive_and_wait();
                results[i] = scenario.submit(i + 1);
            });
        }
        start.arrive_and_wait();
        sched::block_worker_request_shutdown();
        scenario.provider.auto_complete.store(true, std::memory_order_release);
        block_queue_wake(&scenario.provider.disk);
        for (auto& admission : admissions) admission.join();
        scenario.join_workers();
        ASSERT_TRUE(scenario.requests[0].completed.load());
        EXPECT_EQ(scenario.callback_counts[0].load(), 1U);
        EXPECT_EQ(block_request_release(scenario.requests[0]), 0);
        size_t accepted = 1;
        for (size_t i = 0; i < RACERS; ++i) {
            auto& request = scenario.requests[i + 1];
            if (results[i] == 0) {
                ++accepted;
                EXPECT_TRUE(request.completed.load());
                EXPECT_EQ(request.status, 0);
                EXPECT_EQ(request.bytes, 512U);
                EXPECT_EQ(scenario.callback_counts[i + 1].load(), 1U);
                EXPECT_EQ(block_request_release(request), 0);
            } else {
                EXPECT_EQ(results[i], -ESHUTDOWN);
                EXPECT_EQ(request.queue, nullptr);
                EXPECT_EQ(scenario.callback_counts[i + 1].load(), 0U);
            }
        }
        EXPECT_EQ(scenario.provider.issued.load(), accepted);
        EXPECT_FALSE(block_queue_available(&scenario.provider.disk));
        EXPECT_EQ(scenario.snapshot().owned_requests, 0U);
        EXPECT_EQ(scenario.snapshot().owned_bytes, 0U);
        EXPECT_EQ(scenario.provider.invalid_scopes.load(), 0U);
        EXPECT_EQ(scenario.callback_scope_errors.load(), 0U);
    });
}
