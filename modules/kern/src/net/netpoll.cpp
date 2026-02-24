#include "netpoll.hpp"

#include <cstring>
#include <net/netdevice.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

namespace {

// Registry of all NAPI structures for worker thread lookup
constexpr size_t MAX_NAPI_DEVICES = 16;
NapiStruct* g_napi_registry[MAX_NAPI_DEVICES] = {};
size_t g_napi_count = 0;
ker::mod::sys::Spinlock g_registry_lock;

// Find NapiStruct by matching worker task pointer
auto find_napi_for_current_task() -> NapiStruct* {
    auto* current = ker::mod::sched::get_current_task();
    for (size_t i = 0; i < g_napi_count; ++i) {
        if (g_napi_registry[i] != nullptr && g_napi_registry[i]->worker == current) {
            return g_napi_registry[i];
        }
    }
    return nullptr;
}

// Register a NapiStruct in the global registry
void register_napi(NapiStruct* napi) {
    g_registry_lock.lock();
    if (g_napi_count < MAX_NAPI_DEVICES) {
        g_napi_registry[g_napi_count++] = napi;
    }
    g_registry_lock.unlock();
}

// Unregister a NapiStruct from the global registry
void unregister_napi(NapiStruct* napi) {
    g_registry_lock.lock();
    for (size_t i = 0; i < g_napi_count; ++i) {
        if (g_napi_registry[i] == napi) {
            // Shift remaining entries down
            for (size_t j = i; j < g_napi_count - 1; ++j) {
                g_napi_registry[j] = g_napi_registry[j + 1];
            }
            g_napi_registry[--g_napi_count] = nullptr;
            break;
        }
    }
    g_registry_lock.unlock();
}

// Worker thread main loop
[[noreturn]] void napi_worker_loop(NapiStruct* napi) {
    for (;;) {
        // Disable interrupts BEFORE checking the work flag so that no
        // IRQ can sneak in between the check and the sti;hlt below.
        // The sti;hlt pair is architecturally atomic on x86: if an
        // interrupt is already pending when sti executes, the CPU
        // enters hlt and immediately wakes, so no wakeup is lost.
        asm volatile("cli" ::: "memory");

        if (!napi->has_work.load(std::memory_order_acquire)) {
            // No work - atomically enable interrupts and halt
            ker::mod::sched::kern_yield();
            continue;
        }

        // Work available - re-enable interrupts before polling
        asm volatile("sti" ::: "memory");

        // Transition SCHEDULED -> POLLING
        NapiState expected = NapiState::SCHEDULED;
        if (!napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)) {
            // State changed unexpectedly (disabled?), retry
            if (expected == NapiState::DISABLED) {
                // Device going down, halt
                for (;;) {
                    asm volatile("hlt");
                }
            }
            continue;
        }

        // Inner polling loop - stay here while processing full budget
        for (;;) {
            int processed = napi->poll(napi, napi->weight);
            napi->poll_count++;

            if (processed < napi->weight) {
                // Processed less than budget - driver called napi_complete()
                break;
            }
            // processed >= weight: more work available, continue polling
        }

        // Clear the work flag so we sleep on next iteration
        napi->has_work.store(false, std::memory_order_release);

        // Check for race: if an IRQ arrived between napi_complete() and clearing
        // has_work, state will be SCHEDULED but has_work is now false.
        // Re-set has_work to prevent deadlock.
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
        ker::mod::dbg::log("netpoll: FATAL - worker thread could not find its NapiStruct");
        for (;;) {
            asm volatile("hlt");
        }
    }

    ker::mod::dbg::log("netpoll: worker for %s started", my_napi->dev->name.data());

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

void napi_enable(NapiStruct* napi) {
    // Set direct pointer for lock-free inline poll lookup
    napi->dev->napi = napi;

    // Register in global registry first (so worker can find it)
    register_napi(napi);

    // Build thread name: "netpoll_eth0"
    char name[32] = "netpoll_";
    const char* dev_name = napi->dev->name.data();
    size_t name_len = 8;  // strlen("netpoll_")
    for (size_t i = 0; dev_name[i] != '\0' && name_len < 31; ++i) {
        name[name_len++] = dev_name[i];
    }
    name[name_len] = '\0';

    // Create dedicated worker thread
    napi->worker = ker::mod::sched::task::Task::createKernelThread(name, napi_worker_entry);
    if (napi->worker == nullptr) {
        ker::mod::dbg::log("netpoll: failed to create worker thread for %s", dev_name);
        unregister_napi(napi);
        return;
    }

    // Schedule the worker thread
    ker::mod::sched::post_task_balanced(napi->worker);

    // Transition to IDLE state (ready to receive IRQs)
    napi->state.store(NapiState::IDLE, std::memory_order_release);

    ker::mod::dbg::log("netpoll: enabled for %s (worker PID %lx)", dev_name, napi->worker->pid);
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

    // Clear direct pointer
    napi->dev->napi = nullptr;

    ker::mod::dbg::log("netpoll: disabled for %s", napi->dev->name.data());
}

bool napi_schedule(NapiStruct* napi) {
    // Try to transition from IDLE to SCHEDULED (atomic, no lock)
    NapiState expected = NapiState::IDLE;
    if (!napi->state.compare_exchange_strong(expected, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire)) {
        // Already scheduled, polling, or disabled - nothing to do
        return false;
    }

    // Set work flag for interface worker thread
    napi->has_work.store(true, std::memory_order_release);

    // Wake the worker thread if it is sleeping (via sti;hlt) on another CPU.
    // The NIC IRQ may have been delivered to a different CPU than the one
    // the worker is halted on.  We send a lightweight IPI to break it out
    // of hlt.  Note: rescheduleTaskForCpu does NOT work here because the
    // worker is the currentTask on its CPU (just halted), so the scheduler
    // sees it as "already running" and aborts.
    if (napi->worker != nullptr) {
        ker::mod::sched::wake_cpu(napi->worker->cpu);
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

int napi_poll_inline(NetDevice* dev) {
    if (dev == nullptr) {
        return 0;
    }

    // Lock-free lookup via direct dev->napi pointer (set by napi_enable)
    NapiStruct* napi = dev->napi;
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
    // the NAPI state machine — an IRQ arriving between the driver's
    // napi_complete() and ours could schedule the worker (IDLE→SCHEDULED→
    // POLLING), and our stale napi_complete() would yank it back to IDLE,
    // losing packets.
    int processed = napi->poll(napi, napi->weight);
    napi->poll_count++;

    if (processed >= napi->weight) {
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

    return processed;
}

}  // namespace ker::net
