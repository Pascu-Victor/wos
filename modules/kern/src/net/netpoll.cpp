#include "netpoll.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <net/netdevice.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <util/smallvec.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"netpoll">;

extern "C" char __kernel_text_start[];  // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_end[];    // NOLINT(readability-identifier-naming)

namespace {

constexpr uint32_t NET_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int NET_LATENCY_DAEMON_NICE = -5;
constexpr int NAPI_WORKER_MAX_FULL_BUDGET_POLLS = 8;
constexpr size_t NAPI_INLINE_REGISTRY_CAPACITY = 64;
constexpr size_t NAPI_POLL_SNAPSHOT_CAPACITY = 64;

// Registry of all NAPI structures for worker thread lookup
ker::util::SmallVec<NapiStruct*, NAPI_INLINE_REGISTRY_CAPACITY> g_napi_registry;
ker::mod::sys::Spinlock g_registry_lock;

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = NET_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, NET_LATENCY_DAEMON_NICE);
}

auto poll_fn_is_valid(NapiPollFn poll) -> bool {
    auto const ADDR = reinterpret_cast<uintptr_t>(poll);
    auto const TEXT_START = reinterpret_cast<uintptr_t>(__kernel_text_start);
    auto const TEXT_END = reinterpret_cast<uintptr_t>(__kernel_text_end);
    return ADDR >= TEXT_START && ADDR < TEXT_END;
}

// Find NapiStruct by matching worker task pointer
auto find_napi_for_current_task() -> NapiStruct* {
    auto* current = ker::mod::sched::get_current_task();
    for (auto* napi : g_napi_registry) {
        if (napi != nullptr && napi->worker == current) {
            return napi;
        }
    }
    return nullptr;
}

// Register a NapiStruct in the global registry
auto register_napi(NapiStruct* napi) -> bool {
    g_registry_lock.lock();
    bool const INSERTED = g_napi_registry.push_back(napi);
    g_registry_lock.unlock();
    if (!INSERTED) {
        log::critical("failed to register NAPI context for %s", napi->dev != nullptr ? napi->dev->name.data() : "?");
        return false;
    }
    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::NAPI_REG, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(g_napi_registry.size()), 0, 0);
    return true;
}

// Unregister a NapiStruct from the global registry
void unregister_napi(NapiStruct* napi) {
    g_registry_lock.lock();
    for (size_t i = 0; i < g_napi_registry.size(); ++i) {
        if (g_napi_registry.at(i) == napi) {
            g_napi_registry.remove_at(i);
            g_registry_lock.unlock();
            ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::NAPI_REG, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                                  static_cast<int64_t>(g_napi_registry.size()), 0, 0);
            return;
        }
    }
    g_registry_lock.unlock();
}

// Worker thread main loop
[[noreturn]] void napi_worker_loop(NapiStruct* napi) {
    for (;;) {
        // Disable interrupts before checking work flag.
        asm volatile("cli" ::: "memory");

        if (!napi->has_work.load(std::memory_order_acquire)) {
            // No work: block until napi_schedule() explicitly wakes us.
            //
            // The CLI above only protects the work check.  Entering kern_block()
            // with IF clear resumes through the scheduler's sti/hlt/cli path,
            // which is a fragile same-CPL timer-return point for high-rate
            // netpoll workers.  Re-enable first; a racing IRQ wake is preserved
            // by the task's wakeup_pending check inside kern_block().
            asm volatile("sti" ::: "memory");
            ker::mod::sched::kern_block();
            continue;
        }

        // Work available: re-enable interrupts before polling.
        asm volatile("sti" ::: "memory");

        // SCHEDULED -> POLLING
        NapiState expected = NapiState::SCHEDULED;
        if (!napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (expected == NapiState::DISABLED) {
                // Device down: halt.
                for (;;) {
                    asm volatile("hlt");
                }
            }
            continue;
        }

        // Poll while work remains.
        int full_budget_polls = 0;
        for (;;) {
            if (!poll_fn_is_valid(napi->poll)) {
                log::critical("invalid NAPI poll function for %s: poll=0x%lx napi=%p", napi->dev != nullptr ? napi->dev->name.data() : "?",
                              reinterpret_cast<uintptr_t>(napi->poll), napi);
                napi->state.store(NapiState::DISABLED, std::memory_order_release);
                napi->has_work.store(false, std::memory_order_release);
                for (;;) {
                    asm volatile("hlt");
                }
            }
            int const PROCESSED = napi->poll(napi, napi->weight);
            napi->poll_count++;

            if (PROCESSED < napi->weight) {
                // Driver called napi_complete().
                break;
            }

            if (++full_budget_polls >= NAPI_WORKER_MAX_FULL_BUDGET_POLLS) {
                full_budget_polls = 0;
                ker::mod::sched::kern_yield();
            }
        }

        // Clear work flag.
        napi->has_work.store(false, std::memory_order_release);

        // If IRQ arrived during poll, re-arm work flag.
        if (napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED) {
            napi->has_work.store(true, std::memory_order_release);
        }
    }
}

// Entry point for worker threads (finds its own NapiStruct)
[[noreturn]] void napi_worker_entry() {
    // Find our NapiStruct by matching worker task pointer
    NapiStruct* my_napi = find_napi_for_current_task();

    if (my_napi == nullptr) {
        log::critical("worker thread could not find its NapiStruct");
        for (;;) {
            asm volatile("hlt");
        }
    }

    log::debug("worker for %s started", my_napi->dev->name.data());

    // Run the worker loop
    napi_worker_loop(my_napi);
}

}  // namespace

void napi_init(NapiStruct* napi, NetDevice* dev, NapiPollFn poll, int weight) {
    napi->dev = dev;
    napi->poll = poll;
    napi->state.store(NapiState::DISABLED, std::memory_order_release);
    napi->weight = (weight > 0) ? weight : NAPI_DEFAULT_WEIGHT;
    napi->worker = nullptr;
    napi->has_work.store(false, std::memory_order_release);
    napi->poll_count = 0;
    napi->complete_count = 0;
}

void napi_enable(NapiStruct* napi, uint64_t cpu_affinity) {
    // Register in global registry first (so worker can find it)
    if (!register_napi(napi)) {
        return;
    }

    // Build thread name: "netpoll_eth0"
    std::array<char, 32> name{};
    const char* dev_name = napi->dev->name.data();
    std::string_view const DEV_NAME = dev_name;
    std::string_view const PREFIX = "netpoll_";
    size_t name_len = PREFIX.size();
    std::ranges::copy(PREFIX, name.begin());
    size_t const COPY_LEN = std::min(DEV_NAME.size(), name.size() - name_len - 1);
    std::ranges::copy(DEV_NAME.substr(0, COPY_LEN), std::next(name.begin(), static_cast<ptrdiff_t>(name_len)));
    name_len += COPY_LEN;
    name.at(name_len) = '\0';

    // Create dedicated worker thread
    napi->worker = ker::mod::sched::task::Task::create_kernel_thread(name.data(), napi_worker_entry);
    if (napi->worker == nullptr) {
        log::warn("failed to create worker thread for %s", dev_name);
        unregister_napi(napi);
        return;
    }

    promote_latency_sensitive_daemon(napi->worker);

    // Schedule the worker thread on the same CPU as its IRQ vector.  Keeping
    // this pinned preserves NAPI/IRQ locality; otherwise event wakes can drift
    // the worker away from the CPU programmed into MSI-X.
    if (cpu_affinity != UINT64_MAX) {
        ker::mod::sched::post_task_pinned_cpu(cpu_affinity, napi->worker);
    } else {
        ker::mod::sched::post_task_balanced(napi->worker);
    }

    // Transition to IDLE state (ready to receive IRQs)
    napi->state.store(NapiState::IDLE, std::memory_order_release);

    log::debug("enabled for %s (worker PID %lx)", dev_name, napi->worker->pid);
}

auto napi_set_worker_cpu(NapiStruct* napi, uint64_t cpu) -> bool {
    if (napi == nullptr || napi->worker == nullptr) {
        return false;
    }
    auto* worker = napi->worker;
    return ker::mod::sched::pin_task_to_cpu(worker, cpu);
}

void napi_disable(NapiStruct* napi) {
    // Signal worker to stop
    NapiState current = napi->state.load(std::memory_order_acquire);
    while (current != NapiState::DISABLED) {
        if (current == NapiState::POLLING) {
            // Wait for poll to complete
            asm volatile("pause");
            current = napi->state.load(std::memory_order_acquire);
            continue;
        }

        // Try to transition to DISABLED
        if (napi->state.compare_exchange_weak(current, NapiState::DISABLED, std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
        current = napi->state.load(std::memory_order_acquire);
    }

    // Unregister from global registry
    unregister_napi(napi);

    log::debug("disabled for %s", napi->dev->name.data());
}

auto napi_schedule(NapiStruct* napi) -> bool {
    // Try to transition from IDLE to SCHEDULED (atomic, no lock)
    NapiState expected = NapiState::IDLE;
    if (!napi->state.compare_exchange_strong(expected, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire)) {
        // Already scheduled, polling, or disabled - nothing to do
        return false;
    }

    // Set work flag for interface worker thread
    napi->has_work.store(true, std::memory_order_release);

    // Wake the worker thread.
    if (napi->worker != nullptr) {
        ker::mod::sched::kern_wake(napi->worker);
        if (napi->worker->cpu == ker::mod::cpu::current_cpu()) {
            // Same-CPU: wake_cpu() is a no-op, so arm the APIC timer for an
            // immediate scheduling pass instead of waiting up to 1ms for the
            // next quantum tick.
            ker::mod::sys::context_switch::request_reschedule();
        } else {
            ker::mod::sched::wake_cpu(napi->worker->cpu);
        }
    }

    return true;
}

void napi_complete(NapiStruct* napi) {
    napi->complete_count++;

    // Transition from POLLING back to IDLE
    NapiState expected = NapiState::POLLING;
    napi->state.compare_exchange_strong(expected, NapiState::IDLE, std::memory_order_acq_rel, std::memory_order_acquire);

    // Driver should re-enable device interrupts after calling this
}

namespace {

auto napi_poll_struct_inline_budget(NapiStruct* napi, int budget) -> int {
    if (napi == nullptr) {
        return 0;
    }

    // Try to enter POLLING from IDLE or SCHEDULED.
    // If the worker is already POLLING, don't interfere.
    NapiState expected = NapiState::IDLE;
    if (!napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)) {
        expected = NapiState::SCHEDULED;
        if (!napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return 0;  // already polling or disabled
        }
    }

    // Poll once with normal budget.
    // NOTE: The driver's poll function (e.g. virtio_net_poll) calls
    // napi_complete() + re-enables IRQs internally when processed < budget.
    // We must NOT call napi_complete() again in that case, or we corrupt
    // the NAPI state machine - an IRQ arriving between the driver's
    // napi_complete() and ours could schedule the worker (IDLE->SCHEDULED->
    // POLLING), and our stale napi_complete() would yank it back to IDLE,
    // losing packets.
    int const EFFECTIVE_BUDGET = budget > 0 ? budget : napi->weight;
    if (!poll_fn_is_valid(napi->poll)) {
        log::critical("invalid inline NAPI poll function for %s: poll=0x%lx napi=%p", napi->dev != nullptr ? napi->dev->name.data() : "?",
                      reinterpret_cast<uintptr_t>(napi->poll), napi);
        napi->state.store(NapiState::DISABLED, std::memory_order_release);
        napi->has_work.store(false, std::memory_order_release);
        return 0;
    }
    int const PROCESSED = napi->poll(napi, EFFECTIVE_BUDGET);
    napi->poll_count++;

    if (PROCESSED >= EFFECTIVE_BUDGET) {
        // Driver did NOT call napi_complete() (more work pending).
        // Transition back to SCHEDULED so the worker can pick up the
        // remaining work and re-enable IRQs when it's done.
        NapiState exp = NapiState::POLLING;
        napi->state.compare_exchange_strong(exp, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire);
        napi->has_work.store(true, std::memory_order_release);
        if (napi->worker != nullptr) {
            ker::mod::sched::wake_cpu(napi->worker->cpu);
        }
    } else {
        // Driver already called napi_complete() and re-enabled IRQs.
        // Clear work flag so worker doesn't spin unnecessarily.
        napi->has_work.store(false, std::memory_order_release);

        // Re-check for race: if an IRQ arrived during our poll, re-arm
        if (napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED) {
            napi->has_work.store(true, std::memory_order_release);
        }
    }

    return PROCESSED;
}

}  // namespace

auto napi_poll_all_pending() -> int {
    int total = 0;
    // Snapshot count under lock to avoid tearing, then poll without the lock
    // (per-NAPI inline polling is safe to call without the registry lock).
    g_registry_lock.lock();
    size_t count = g_napi_registry.size();
    std::array<NapiStruct*, NAPI_POLL_SNAPSHOT_CAPACITY> snapshot{};
    count = std::min<size_t>(count, snapshot.size());
    for (size_t i = 0; i < count; ++i) {
        snapshot.at(i) = g_napi_registry.at(i);
    }
    g_registry_lock.unlock();

    for (size_t i = 0; i < count; ++i) {
        if (auto* napi = snapshot.at(i); napi != nullptr && napi->dev != nullptr) {
            total += napi_poll_struct_inline_budget(napi, NAPI_DEFAULT_WEIGHT);
        }
    }
    return total;
}

}  // namespace ker::net
