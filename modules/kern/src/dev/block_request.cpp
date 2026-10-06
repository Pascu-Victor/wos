#include "block_request.hpp"

#include <algorithm>
#include <cstring>
#include <dev/block_device.hpp>
#include <mod/io/serial/serial.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>

namespace ker::dev {
namespace sched = ker::mod::sched;
namespace {
constexpr size_t MAX_QUEUES = 64;
constexpr uint64_t SERVICE_INTERVAL_US = 1000;
constexpr size_t WAIT_POLL_EXTRA_PASSES = 32;
constexpr uint64_t WAIT_POLL_BUDGET_US = 128;
constexpr uint64_t WAIT_RETRY_INTERVAL_US = 16;
ker::mod::sys::Spinlock registry_lock;
std::atomic<bool> workers_started{false};
}  // namespace

// Permanent provider/queue storage: registry lookups never transfer a raw
// provider lifetime to an IRQ or a dynamically freed completion object.
struct BlockRequestQueue {
    BlockDevice* device{};
    BlockQueueLimits limits{};
    BlockAsyncOps ops{};
    bool native{};
    ker::mod::sys::Spinlock lock;
    ker::mod::sys::Mutex service_lock;
    BlockQueueModel model{};
    std::array<BlockRequest*, BLOCK_MODEL_MAX_REQUESTS> owners{};
    std::array<bool, BLOCK_MODEL_MAX_REQUESTS> cancel_notified{};
    sched::task::Task* worker{};  // Owns a lifetime reference while published.
    // Queue guard; present only during a provider service call whose next
    // mandatory gather retires every matching inline completion claim.
    sched::task::Task* provider_completion_owner{};
    bool worker_starting{};
    bool closing{};
    bool caller_driven{};  // Queue guard; public async admission is closed.
    uint64_t flushes{};
    uint64_t latency_us{};
    uint64_t max_latency_us{};
    std::array<uint64_t, 16> latency_histogram{};
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
    BlockWaitDiagnostics wait_diagnostics{};
#endif
};

namespace {
std::array<BlockRequestQueue, MAX_QUEUES> queues;

auto physical_root(const BlockDevice* device) -> const BlockDevice* {
    // Bound corrupt/cyclic partition ancestry before taking the registry guard.
    for (size_t i = 0; device != nullptr && i < MAX_QUEUES; ++i) {
        if (!device->is_partition) return device;
        device = device->parent_disk;
    }
    return nullptr;
}

auto find_queue(const BlockDevice* device) -> BlockRequestQueue* {
    auto const* root = physical_root(device);
    if (root == nullptr) return nullptr;
    uint64_t const FLAGS = registry_lock.lock_irqsave();
    BlockRequestQueue* result = nullptr;
    for (auto& queue : queues) {
        if (queue.device == root) {
            result = &queue;
            break;
        }
    }
    registry_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto unfinished_locked(const BlockRequestQueue& queue) -> size_t {
    size_t count = 0;
    for (auto const& request : queue.model.requests) {
        if (block_model_detail::unfinished(request)) ++count;
    }
    return count;
}

void increment(uint64_t& value, uint64_t amount = 1) { value = amount > UINT64_MAX - value ? UINT64_MAX : value + amount; }

#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
void merge_wait_diagnostics(BlockWaitDiagnostics& target, const BlockWaitDiagnostics& source) {
    constexpr std::array COUNTERS{
        &BlockWaitDiagnostics::bursts,         &BlockWaitDiagnostics::completed,     &BlockWaitDiagnostics::no_context,
        &BlockWaitDiagnostics::mandatory_busy, &BlockWaitDiagnostics::disallowed,    &BlockWaitDiagnostics::clock_regression,
        &BlockWaitDiagnostics::budget,         &BlockWaitDiagnostics::deadline,      &BlockWaitDiagnostics::extra_busy,
        &BlockWaitDiagnostics::count_cap,      &BlockWaitDiagnostics::extra_passes,  &BlockWaitDiagnostics::count_cap_under_budget,
        &BlockWaitDiagnostics::cap_elapsed_us, &BlockWaitDiagnostics::park_attempts,
    };
    for (auto member : COUNTERS) increment(target.*member, source.*member);
    target.cap_elapsed_max_us = std::max(target.cap_elapsed_max_us, source.cap_elapsed_max_us);
}
#endif

auto safe_syscall_task() -> sched::task::Task* {
    if (!sched::has_run_queues() || !sched::can_query_current_task() || ker::mod::io::serial::is_panic_mode()) {
        return nullptr;
    }
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->preempt_disable_depth != 0) return nullptr;
    // Same-CPU syscall wakes request a channel-free yield at syscall return.
    // Preserve that pair; other deferred waits cannot park or poll further.
    bool const RETURN_YIELD = task->deferred_task_switch && task->yield_switch && task->wait_channel == nullptr &&
                              task->wait_channel_kind == sched::task::WaitChannelKind::NONE;
    bool const SAFE_SYSCALL = task->syscall_account_start_us != 0 && !task->scheduler_transition_active.load(std::memory_order_acquire) &&
                              (!task->deferred_task_switch || RETURN_YIELD) && !task->wants_block && !task->is_voluntary_blocked() &&
                              task->wki_proxy_task_id == 0 && !task->wait_channel_is(sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
    return SAFE_SYSCALL ? task : nullptr;
}

void pause_task(uint64_t interval = SERVICE_INTERVAL_US) {
    if (auto* task = safe_syscall_task(); task != nullptr) {
        // Task-only block waits release queue guards before this point.
        // Syscall entry masks IF; the existing safe park handles it. Sticky
        // signals cancel process parks while retirement still owns buffers.
        if (task->has_interrupting_signal_pending()) {
            sched::kern_yield();
        } else {
            sched::preemptible_syscall_park("blockio", sched::saturating_deadline_us(ker::mod::time::get_us(), interval));
        }
        return;
    }
    if (sched::has_run_queues() && sched::can_query_current_task() && !ker::mod::io::serial::is_panic_mode()) {
        auto* task = sched::get_current_task();
        if (task != nullptr && task->preempt_disable_depth == 0 && task->type == sched::task::TaskType::DAEMON &&
            sched::interrupts_enabled()) {
            sched::kern_sleep_us(interval);
            return;
        }
    }
    asm volatile("pause" ::: "memory");
}

// The queue guard also protects the worker's retained reference against exit.
// Acquire a temporary reference before calling into scheduler locking.
void wake_queue(BlockRequestQueue& queue) {
    uint64_t const FLAGS = queue.lock.lock_irqsave();
    auto* worker = queue.worker_starting ? nullptr : queue.worker;
    if (worker != nullptr && !worker->try_acquire()) worker = nullptr;
    queue.lock.unlock_irqrestore(FLAGS);
    if (worker != nullptr) {
        sched::kern_wake(worker);
        worker->release();
    }
}

void finish_completions(BlockRequestQueue& queue) {
    // service_lock serializes callback ownership; IRQ only claims states.
    std::array<BlockRequest*, BLOCK_MODEL_MAX_REQUESTS> completing{};
    size_t count = 0;
    uint64_t flags = queue.lock.lock_irqsave();
    queue.provider_completion_owner = nullptr;
    for (size_t i = 0; i < queue.owners.size(); ++i) {
        auto& state = queue.model.requests[i];
        BlockRequest* request = state.state == BlockRequestState::COMPLETING ? queue.owners[i] : nullptr;
        if (request != nullptr) {
            request->status = state.status;
            request->bytes = state.completed_bytes;
            completing[count++] = request;
        }
    }
    queue.lock.unlock_irqrestore(flags);
    // Gathered owners remain nonterminal until their callback retires. Release
    // requires completed=true, so these pointers cannot be destroyed yet.
    for (size_t i = 0; i < count; ++i) {
        auto* request = completing[i];
        if (request->callback != nullptr) request->callback(*request, request->callback_arg);
        flags = queue.lock.lock_irqsave();
        if (!block_model_finish_completion(queue.model, request->token)) {
            queue.lock.unlock_irqrestore(flags);
            continue;
        }
        uint64_t const NOW = ker::mod::time::get_us();
        uint64_t const LATENCY = NOW >= request->submitted_us ? NOW - request->submitted_us : 0;
        increment(queue.latency_us, LATENCY);
        queue.max_latency_us = std::max(queue.max_latency_us, LATENCY);
        size_t bucket = 0;
        for (uint64_t value = LATENCY; value > 1 && bucket + 1 < queue.latency_histogram.size(); value >>= 1) ++bucket;
        increment(queue.latency_histogram[bucket]);
        auto* waiter = static_cast<sched::task::Task*>(request->waiter);
        if (waiter != nullptr && sched::can_query_current_task() && waiter == sched::get_current_task()) {
            // The current wait retains its own lifetime and observes this
            // publication directly; it needs no scheduler event or extra ref.
            waiter = nullptr;
        } else if (waiter != nullptr && !waiter->try_acquire()) {
            waiter = nullptr;
        }
        // Owner may release and destroy immediately after this publication.
        request->completed.store(true, std::memory_order_release);
        queue.lock.unlock_irqrestore(flags);
        if (waiter != nullptr) {
            sched::kern_wake(waiter);
            waiter->release();
        }
    }
}

auto legacy_issue(BlockRequestQueue& queue, BlockRequest& request) -> int {
    auto* device = queue.device;
    auto const& description = request.description;
    if (description.op == BlockRequestOperation::FLUSH) {
        return device->flush != nullptr ? device->flush(device) : -EOPNOTSUPP;
    }
    // CPU gather/scatter and legacy callbacks are task context only. A single
    // segment avoids staging; multi-segment requests allocate before calling
    // any provider, outside the queue spin guard.
    std::byte* staging = nullptr;
    void* buffer = request.segments[0].data;
    if (request.segment_count != 1) {
        staging = new (std::nothrow) std::byte[description.bytes];
        if (staging == nullptr) return -ENOMEM;
        buffer = staging;
        if (description.op == BlockRequestOperation::WRITE) {
            size_t offset = 0;
            for (size_t i = 0; i < request.segment_count; ++i) {
                std::memcpy(staging + offset, request.segments[i].data, request.segments[i].size);
                offset += request.segments[i].size;
            }
        }
    }
    int result = -ENOSYS;
    if (description.op == BlockRequestOperation::READ && device->read_blocks != nullptr) {
        result = device->read_blocks(device, description.lba, description.count, buffer);
    } else if (description.op == BlockRequestOperation::WRITE && device->write_blocks != nullptr) {
        result = device->write_blocks(device, description.lba, description.count, buffer);
    }
    if (result == 0 && staging != nullptr && description.op == BlockRequestOperation::READ) {
        size_t offset = 0;
        for (size_t i = 0; i < request.segment_count; ++i) {
            std::memcpy(request.segments[i].data, staging + offset, request.segments[i].size);
            offset += request.segments[i].size;
        }
    }
    delete[] staging;
    return result;
}

auto service_queue(BlockRequestQueue& queue, bool extra_poll = false) -> bool {
    auto const SHUTDOWN_HOLD = sched::hold_kernel_shutdown();
    if (!queue.service_lock.try_lock()) return false;
    struct Unlock {
        ker::mod::sys::Mutex& lock;
        ~Unlock() { lock.unlock(); }
    } const UNLOCK{queue.service_lock};
    ker::mod::mm::reclaim::NoIoScope const NO_IO;
    auto* owner = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (owner != nullptr && !owner->try_acquire()) owner = nullptr;
    struct OwnerRef {
        sched::task::Task* task;
        ~OwnerRef() {
            if (task != nullptr) task->release();
        }
    } const OWNER{owner};
    auto const SERVICE_PROVIDER = [&queue, owner](bool poll_active = false) {
        if (queue.ops.service == nullptr) return;
        uint64_t const FLAGS = queue.lock.lock_irqsave();
        queue.provider_completion_owner = owner;
        queue.lock.unlock_irqrestore(FLAGS);
        if (!poll_active || queue.ops.poll_active == nullptr || !queue.ops.poll_active(queue.device)) queue.ops.service(queue.device);
    };

    uint64_t const NOW = ker::mod::time::get_us();
    std::array<BlockRequestToken, BLOCK_MODEL_MAX_REQUESTS> cancel_tokens{};
    std::array<int, BLOCK_MODEL_MAX_REQUESTS> cancel_reasons{};
    size_t cancel_count = 0;
    uint64_t flags = queue.lock.lock_irqsave();
    uint64_t const GENERATION = queue.model.generation;
    bool post_required = !queue.native || !queue.model.admission_open || queue.model.resetting || queue.closing || queue.caller_driven;
    for (size_t i = 0; i < queue.owners.size(); ++i) {
        auto* request = queue.owners[i];
        auto& state = queue.model.requests[i];
        if (request != nullptr && request->deadline_us != 0 && NOW >= request->deadline_us &&
            (state.state == BlockRequestState::QUEUED || state.state == BlockRequestState::ISSUED)) {
            static_cast<void>(block_model_cancel(queue.model, state.token, -ETIMEDOUT));
        }
        if (state.state == BlockRequestState::ISSUED && state.cancel_reason != 0 && !queue.cancel_notified[i]) {
            queue.cancel_notified[i] = true;
            cancel_tokens[cancel_count] = state.token;
            cancel_reasons[cancel_count++] = state.cancel_reason;
        }
        if (block_model_detail::unfinished(state) && state.cancel_reason != 0) post_required = true;
    }
    queue.lock.unlock_irqrestore(flags);
    for (size_t i = 0; i < cancel_count; ++i) {
        if (queue.ops.cancel != nullptr) queue.ops.cancel(queue.device, cancel_tokens[i], cancel_reasons[i]);
    }
    SERVICE_PROVIDER(extra_poll && queue.native && !post_required);
    finish_completions(queue);

    for (size_t attempt = 0; attempt < BLOCK_MODEL_MAX_REQUESTS; ++attempt) {
        flags = queue.lock.lock_irqsave();
#ifdef WOS_ASYNC_BLOCK_IO
        // Before saturation, admission increments submitted and only callback
        // retirement increments completed. Equality proves no unfinished owner;
        // changing either counter's lifetime requires revisiting this proof.
        if (attempt == 0 && !extra_poll && !post_required && queue.native && queue.model.admission_open && !queue.model.resetting &&
            !queue.closing && !queue.caller_driven && queue.model.generation == GENERATION && queue.provider_completion_owner == nullptr &&
            queue.model.stats.submitted != UINT64_MAX && queue.model.stats.submitted == queue.model.stats.completed) {
            queue.lock.unlock_irqrestore(flags);
            return true;
        }
#endif
        BlockRequestToken token{};
        bool const SELECTED = block_model_next_issue(queue.model, token, queue.limits.depth);
        BlockRequest* request = SELECTED ? queue.owners[token.slot] : nullptr;
        if (request != nullptr && request->description.op == BlockRequestOperation::FLUSH) increment(queue.flushes);
        queue.lock.unlock_irqrestore(flags);
        if (request == nullptr) break;
        // Accepted staging and every rejection, including EAGAIN, need the
        // following provider pass before this service relinquishes ownership.
        post_required = true;
        int const RESULT = queue.native ? queue.ops.submit(queue.device, *request) : legacy_issue(queue, *request);
        if (queue.native && RESULT == -EAGAIN) {
            flags = queue.lock.lock_irqsave();
            // Provider took no ownership; cancellation can race admission.
            auto* state = block_model_detail::lookup(queue.model, token);
            if (state != nullptr && state->state == BlockRequestState::ISSUED) {
                queue.cancel_notified[token.slot] = false;
                static_cast<void>(block_model_defer_issue(queue.model, token));
            }
            queue.lock.unlock_irqrestore(flags);
            break;
        }
        if (!queue.native || RESULT != 0) {
            static_cast<void>(block_request_complete(queue.device, token, RESULT, RESULT == 0 ? request->description.bytes : 0));
            finish_completions(queue);
        }
    }
    if (!post_required) {
        flags = queue.lock.lock_irqsave();
        post_required = !queue.native || !queue.model.admission_open || queue.model.resetting || queue.closing || queue.caller_driven ||
                        queue.model.generation != GENERATION;
        for (auto const& state : queue.model.requests) {
            if (block_model_detail::unfinished(state) && state.cancel_reason != 0) post_required = true;
        }
        queue.lock.unlock_irqrestore(flags);
    }
    // PRE may retire transport while reset callbacks are still COMPLETING.
    // POST retries reset reopen after those callbacks; healthy passes with no
    // submit attempt need only PRE. Retained staged owners progress next PRE.
    if (post_required) SERVICE_PROVIDER();
    finish_completions(queue);
    return true;
}

auto wait_poll_context(const BlockRequestQueue& queue) -> bool {
    if (!queue.native || queue.limits.depth <= 1) return false;
    auto* task = safe_syscall_task();
    return task != nullptr && !task->has_interrupting_signal_pending() && !sched::kernel_threads_shutdown_requested();
}

auto wait_poll_allowed(BlockRequestQueue& queue, BlockRequest& request, uint64_t now) -> bool {
    if (!wait_poll_context(queue)) return false;
    uint64_t const FLAGS = queue.lock.lock_irqsave();
    auto* state = block_model_detail::lookup(queue.model, request.token);
    bool const ALLOWED = queue.native && queue.limits.depth > 1 && queue.model.admission_open && !queue.model.resetting && !queue.closing &&
                         !queue.caller_driven && state != nullptr && state->state == BlockRequestState::ISSUED &&
                         state->cancel_reason == 0 && queue.owners[request.token.slot] == &request &&
                         (request.deadline_us == 0 || now < request.deadline_us);
    queue.lock.unlock_irqrestore(FLAGS);
    return ALLOWED;
}

auto worker_loop() -> bool {
    auto* current = sched::get_current_task();
    BlockRequestQueue* own = nullptr;
    // Creation publishes the fully initialized task/queue pair before posting;
    // no ambiguous global launch pointer or per-thread storage is needed.
    for (auto& queue : queues) {
        uint64_t const FLAGS = queue.lock.lock_irqsave();
        bool const MATCHES = queue.worker == current;
        queue.lock.unlock_irqrestore(FLAGS);
        if (MATCHES) {
            own = &queue;
            break;
        }
    }
    if (own == nullptr) return false;
    for (;;) {
        // Hold through the check-to-park edge: a shutdown wake can race the
        // empty check, but cooperative exit must not bypass worker ref cleanup.
        auto const HOLD = sched::hold_kernel_shutdown();
        size_t pending = 0;
        service_queue(*own);
        uint64_t const IRQF = own->lock.lock_irqsave();
        pending = unfinished_locked(*own);
        bool const EXIT = pending == 0 && sched::kernel_threads_shutdown_requested();
        if (EXIT) {
            own->caller_driven = true;
            own->worker = nullptr;
        }
        own->lock.unlock_irqrestore(IRQF);
        if (EXIT) {
            current->release();
            return true;
        }
        if (pending != 0) {
            sched::kern_sleep_us(SERVICE_INTERVAL_US);
            continue;
        }
        sched::kern_block();
    }
}

void worker_entry() {
    if (!worker_loop()) return;
    sched::exit_current_kernel_thread();
}

void start_worker(BlockRequestQueue& queue) {
    uint64_t flags = queue.lock.lock_irqsave();
    if (queue.worker != nullptr || queue.worker_starting || !queue.model.admission_open || sched::kernel_threads_shutdown_requested()) {
        queue.lock.unlock_irqrestore(flags);
        return;
    }
    queue.worker_starting = true;
    queue.lock.unlock_irqrestore(flags);
    auto* worker = sched::task::Task::create_kernel_thread("blockio", worker_entry);
    if (worker != nullptr && !worker->try_acquire_lifetime_ref()) ker::mod::dbg::panic_handler("blockio: invalid new worker lifetime");
    flags = queue.lock.lock_irqsave();
    queue.worker = worker;
    queue.lock.unlock_irqrestore(flags);
    bool const POSTED = worker != nullptr && sched::post_task_balanced(worker);
    flags = queue.lock.lock_irqsave();
    if (!POSTED) {
        queue.worker = nullptr;
        queue.caller_driven = true;
    } else if (queue.worker == worker) {
        queue.caller_driven = false;
    }
    queue.worker_starting = false;
    queue.lock.unlock_irqrestore(flags);
    if (!POSTED && worker != nullptr) {
        worker->release();
        sched::task::destroy_unpublished_user_thread(worker);
    }
    if (POSTED) wake_queue(queue);
    // Allocation failure leaves the explicit service/sync path operational.
}
}  // namespace

auto block_queue_configure(BlockDevice* device, const BlockQueueLimits& limits, const BlockAsyncOps* ops) -> int {
    if (device == nullptr || device->is_partition || device->block_size == 0 || limits.depth == 0 ||
        limits.depth > BLOCK_MODEL_MAX_REQUESTS || limits.max_requests == 0 || limits.max_requests > BLOCK_MODEL_MAX_REQUESTS ||
        limits.depth > limits.max_requests || limits.max_transfer_bytes == 0 || limits.max_transfer_bytes > BLOCK_MODEL_MAX_REQUEST_BYTES ||
        limits.max_segments == 0 || limits.max_segments > BLOCK_MODEL_MAX_SG || limits.max_inflight_bytes < limits.max_transfer_bytes ||
        (ops != nullptr && (ops->submit == nullptr || ops->service == nullptr || ops->cancel == nullptr)) ||
        (ops == nullptr && limits.depth != 1))
        return -EINVAL;
    uint64_t const FLAGS = registry_lock.lock_irqsave();
    BlockRequestQueue* selected = nullptr;
    for (auto& queue : queues) {
        if (queue.device == device) {
            registry_lock.unlock_irqrestore(FLAGS);
            return -EALREADY;
        }
        if (selected == nullptr && queue.device == nullptr) selected = &queue;
    }
    if (selected != nullptr) {
        selected->limits = limits;
        selected->model.limits = {limits.max_requests, limits.max_inflight_bytes, limits.max_transfer_bytes};
        selected->native = ops != nullptr;
        if (ops != nullptr) selected->ops = *ops;
        selected->device = device;
    }
    registry_lock.unlock_irqrestore(FLAGS);
    if (selected == nullptr) return -ENOSPC;
    if (workers_started.load(std::memory_order_acquire)) start_worker(*selected);
    return 0;
}

void block_request_start_workers() {
    workers_started.store(true, std::memory_order_release);
    for (auto& queue : queues) {
        uint64_t const FLAGS = registry_lock.lock_irqsave();
        bool const CONFIGURED = queue.device != nullptr;
        registry_lock.unlock_irqrestore(FLAGS);
        if (CONFIGURED) start_worker(queue);
    }
}

auto block_queue_configured(const BlockDevice* device) -> bool { return find_queue(device) != nullptr; }
auto block_queue_available(const BlockDevice* device) -> bool {
    auto* queue = find_queue(device);
    if (queue == nullptr) return false;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    bool const AVAILABLE = !queue->caller_driven;
    queue->lock.unlock_irqrestore(FLAGS);
    return AVAILABLE;
}
auto block_queue_limits(const BlockDevice* device) -> BlockQueueLimits {
    auto* queue = find_queue(device);
    return queue != nullptr ? queue->limits : BlockQueueLimits{};
}

static auto submit_request(BlockDevice* device, BlockRequest& request, BlockRequestOperation operation, uint64_t lba, size_t count,
                           std::span<const BlockSegment> segments, uint64_t timeout_us, BlockCompletion callback, void* callback_arg,
                           bool caller_driven, bool wake_worker) -> int {
    if (device == nullptr || request.queue != nullptr || timeout_us == 0) return -EINVAL;
    auto* queue = find_queue(device);
    if (queue == nullptr) return -EOPNOTSUPP;
    if (operation == BlockRequestOperation::WRITE && block_device_is_read_only(device)) return -EROFS;
    BlockRequestDescription description{operation, lba, count, 0};
    if (operation == BlockRequestOperation::FLUSH) {
        if (lba != 0 || count != 0 || !segments.empty()) return -EINVAL;
    } else {
        int result = block_model_validate_io(lba, count, device->block_size, device->total_blocks, queue->limits.max_transfer_bytes,
                                             description.bytes);
        if (result != 0) return result;
        auto const* translated = device;
        for (size_t depth = 0; translated->is_partition; ++depth) {
            if (depth >= MAX_QUEUES || translated->parent_disk == nullptr || translated->parent_disk->block_size != translated->block_size)
                return -EINVAL;
            uint64_t root_lba = 0;
            result = block_model_translate_partition(description.lba, count, translated->partition_start_lba, translated->total_blocks,
                                                     translated->parent_disk->total_blocks, root_lba);
            if (result != 0) return result;
            description.lba = root_lba;
            translated = translated->parent_disk;
        }
        if (segments.empty() || segments.size() > queue->limits.max_segments) return -EINVAL;
        std::array<BlockScatterSegment, BLOCK_MODEL_MAX_SG> validation{};
        for (size_t i = 0; i < segments.size(); ++i) {
            auto const ADDRESS = reinterpret_cast<uint64_t>(segments[i].data);
            if (segments[i].data == nullptr || ADDRESS < ker::mod::mm::addr::get_hhdm_offset()) return -EFAULT;
            validation[i] = {ADDRESS, segments[i].size};
        }
        result = block_model_validate_sg(validation.data(), segments.size(), description.bytes,
                                         {queue->limits.max_segments, queue->limits.max_transfer_bytes, 1, UINT64_MAX});
        if (result != 0) return result;
    }
    uint64_t const NOW = ker::mod::time::get_us();
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    if (queue->caller_driven && !caller_driven) {
        queue->lock.unlock_irqrestore(FLAGS);
        return -ESHUTDOWN;
    }
    int const RESULT = block_model_submit(queue->model, description, request.token);
    if (RESULT == 0) {
        request.description = description;
        std::copy(segments.begin(), segments.end(), request.segments.begin());
        request.segment_count = segments.size();
        request.deadline_us = sched::saturating_deadline_us(NOW, timeout_us);
        request.submitted_us = NOW;
        request.status = 0;
        request.bytes = 0;
        request.completed.store(false, std::memory_order_relaxed);
        request.callback = callback;
        request.callback_arg = callback_arg;
        request.queue = queue;
        request.waiter = nullptr;
        queue->owners[request.token.slot] = &request;
        queue->cancel_notified[request.token.slot] = false;
    }
    queue->lock.unlock_irqrestore(FLAGS);
    if (RESULT == 0 && wake_worker) wake_queue(*queue);
    return RESULT;
}

auto block_request_submit(BlockDevice* device, BlockRequest& request, BlockRequestOperation operation, uint64_t lba, size_t count,
                          std::span<const BlockSegment> segments, uint64_t timeout_us, BlockCompletion callback, void* callback_arg)
    -> int {
    return submit_request(device, request, operation, lba, count, segments, timeout_us, callback, callback_arg, false, true);
}

auto block_request_submit_for_wait(BlockDevice* device, BlockRequest& request, BlockRequestOperation operation, uint64_t lba, size_t count,
                                   std::span<const BlockSegment> segments, uint64_t timeout_us) -> int {
    return submit_request(device, request, operation, lba, count, segments, timeout_us, nullptr, nullptr, false, false);
}

namespace {
auto complete_request(BlockDevice* device, BlockRequestToken token, int status, size_t bytes, bool from_service) -> bool {
    auto* queue = find_queue(device);
    if (queue == nullptr) return false;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    bool const CLAIMED = block_model_claim_completion(queue->model, token, status, bytes, true);
    bool const INLINE = CLAIMED && from_service && queue->provider_completion_owner != nullptr && sched::can_query_current_task() &&
                        queue->provider_completion_owner == sched::get_current_task();
    queue->lock.unlock_irqrestore(FLAGS);
    if (CLAIMED && !INLINE) wake_queue(*queue);
    return CLAIMED;
}
}  // namespace

auto block_request_complete(BlockDevice* device, BlockRequestToken token, int status, size_t bytes) -> bool {
    return complete_request(device, token, status, bytes, false);
}

auto block_request_complete_from_service(BlockDevice* device, BlockRequestToken token, int status, size_t bytes) -> bool {
    return complete_request(device, token, status, bytes, true);
}

auto block_request_cancel(BlockRequest& request, int reason) -> int {
    auto* queue = request.queue;
    if (queue == nullptr || reason >= 0) return -EINVAL;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    auto const RESULT = block_model_cancel(queue->model, request.token, reason);
    queue->lock.unlock_irqrestore(FLAGS);
    if (RESULT == BlockCancelResult::REJECTED) return -EALREADY;
    wake_queue(*queue);
    return 0;
}

auto block_request_wait(BlockRequest& request) -> int {
    auto const SHUTDOWN_HOLD = sched::hold_kernel_shutdown();
    auto* queue = request.queue;
    if (queue == nullptr) return -EINVAL;
    auto* waiter = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (waiter != nullptr && !waiter->try_acquire()) waiter = nullptr;
    uint64_t flags = queue->lock.lock_irqsave();
    if (request.waiter != nullptr) {
        queue->lock.unlock_irqrestore(flags);
        if (waiter != nullptr) waiter->release();
        return -EBUSY;
    }
    request.waiter = waiter;
    queue->lock.unlock_irqrestore(flags);
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
    BlockWaitDiagnostics diagnostics{};
#endif
    while (!request.completed.load(std::memory_order_acquire)) {
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        increment(diagnostics.bursts);
        bool exit_counted = false;
        size_t successful_extra_passes = 0;
#endif
        bool const POLLING = wait_poll_context(*queue);
        uint64_t const POLL_STARTED = POLLING ? ker::mod::time::get_us() : 0;
        bool const SERVICED = service_queue(*queue);
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        if (!POLLING) {
            increment(diagnostics.no_context);
            exit_counted = true;
        } else if (!SERVICED) {
            increment(diagnostics.mandatory_busy);
            exit_counted = true;
        }
#endif
        uint64_t last_poll_us = POLL_STARTED;
        for (size_t pass = 0; POLLING && SERVICED && pass < WAIT_POLL_EXTRA_PASSES && !request.completed.load(std::memory_order_acquire);
             ++pass) {
            if (!wait_poll_allowed(*queue, request, last_poll_us))
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
            {
                increment(diagnostics.disallowed);
                exit_counted = true;
                break;
            }
#else
                break;
#endif
            uint64_t const NOW = ker::mod::time::get_us();
            // Bound starts of complete service passes. A provider reset or
            // callback may itself take longer; never renew for unrelated work.
            // Sample after qualification so its guard delay uses the budget.
            if (NOW < last_poll_us || NOW - POLL_STARTED >= WAIT_POLL_BUDGET_US || (request.deadline_us != 0 && NOW >= request.deadline_us))
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
            {
                if (NOW < last_poll_us)
                    increment(diagnostics.clock_regression);
                else if (NOW - POLL_STARTED >= WAIT_POLL_BUDGET_US)
                    increment(diagnostics.budget);
                else
                    increment(diagnostics.deadline);
                exit_counted = true;
                break;
            }
#else
                break;
#endif
            last_poll_us = NOW;
            if (!service_queue(*queue, true))
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
            {
                increment(diagnostics.extra_busy);
                exit_counted = true;
                break;
            }
#else
                break;
#endif
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
            ++successful_extra_passes;
            increment(diagnostics.extra_passes);
#endif
        }
        if (!request.completed.load(std::memory_order_acquire))
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        {
            if (!exit_counted && successful_extra_passes == WAIT_POLL_EXTRA_PASSES) {
                increment(diagnostics.count_cap);
                // This one additional sample belongs only to an observed cap
                // exit. A late completion can still race the existing pause.
                uint64_t const END = ker::mod::time::get_us();
                if (END >= last_poll_us) {
                    uint64_t const ELAPSED = END - POLL_STARTED;
                    increment(diagnostics.cap_elapsed_us, ELAPSED);
                    diagnostics.cap_elapsed_max_us = std::max(diagnostics.cap_elapsed_max_us, ELAPSED);
                    if (ELAPSED < WAIT_POLL_BUDGET_US) increment(diagnostics.count_cap_under_budget);
                }
            }
            increment(diagnostics.park_attempts);
            pause_task(WAIT_RETRY_INTERVAL_US);
        } else if (!exit_counted) {
            increment(diagnostics.completed);
        }
#else
            pause_task(WAIT_RETRY_INTERVAL_US);
#endif
    }
    flags = queue->lock.lock_irqsave();
    request.waiter = nullptr;
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
    merge_wait_diagnostics(queue->wait_diagnostics, diagnostics);
#endif
    queue->lock.unlock_irqrestore(flags);
    if (waiter != nullptr) waiter->release();
    return request.status;
}

auto block_request_release(BlockRequest& request) -> int {
    auto* queue = request.queue;
    if (queue == nullptr) return -EINVAL;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    bool const RELEASED =
        request.waiter == nullptr && request.completed.load(std::memory_order_acquire) && block_model_release(queue->model, request.token);
    if (RELEASED) {
        queue->owners[request.token.slot] = nullptr;
        request.queue = nullptr;
    }
    queue->lock.unlock_irqrestore(FLAGS);
    return RELEASED ? 0 : -EBUSY;
}

void block_queue_wake(BlockDevice* device) {
    if (auto* queue = find_queue(device); queue != nullptr) wake_queue(*queue);
}
void block_queue_service(BlockDevice* device) {
    if (auto* queue = find_queue(device); queue != nullptr) service_queue(*queue);
}
void block_queue_wait_for_capacity(BlockDevice* device) {
    auto const HOLD = sched::hold_kernel_shutdown();
    block_queue_service(device);
    pause_task(100);
}
void block_queue_close(BlockDevice* device) {
    auto* queue = find_queue(device);
    if (queue == nullptr) return;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    queue->closing = true;
    queue->model.admission_open = false;
    queue->lock.unlock_irqrestore(FLAGS);
    wake_queue(*queue);
}

auto block_queue_drain(BlockDevice* device) -> int {
    auto const HOLD = sched::hold_kernel_shutdown();
    auto* queue = find_queue(device);
    if (queue == nullptr) return 0;
    for (;;) {
        service_queue(*queue);
        uint64_t const FLAGS = queue->lock.lock_irqsave();
        size_t const PENDING = unfinished_locked(*queue);
        queue->lock.unlock_irqrestore(FLAGS);
        if (PENDING == 0) return 0;
        pause_task();
    }
}

auto block_queue_begin_reset(BlockDevice* device, int reason) -> int {
    auto* queue = find_queue(device);
    if (queue == nullptr) return -ENODEV;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    int const RESULT = block_model_begin_reset(queue->model, reason);
    queue->lock.unlock_irqrestore(FLAGS);
    wake_queue(*queue);
    return RESULT;
}
auto block_queue_finish_reset(BlockDevice* device, bool reopen) -> int {
    auto* queue = find_queue(device);
    if (queue == nullptr) return -ENODEV;
    uint64_t const FLAGS = queue->lock.lock_irqsave();
    int const RESULT = block_model_finish_reset(queue->model, reopen && !queue->closing);
    queue->lock.unlock_irqrestore(FLAGS);
    if (RESULT == 0) wake_queue(*queue);
    return RESULT;
}

auto block_request_sync(BlockDevice* device, BlockRequestOperation operation, uint64_t lba, size_t count, void* buffer) -> int {
    auto const HOLD = sched::hold_kernel_shutdown();
    if (device == nullptr) return -EINVAL;
    auto* queue = find_queue(device);
    if (queue == nullptr) return -EOPNOTSUPP;
    if (operation != BlockRequestOperation::FLUSH && (buffer == nullptr || device->block_size == 0)) return -EINVAL;
    if (operation != BlockRequestOperation::FLUSH && (lba > device->total_blocks || count > device->total_blocks - lba)) return -EINVAL;
    if (operation != BlockRequestOperation::FLUSH && count > SIZE_MAX / device->block_size) return -EOVERFLOW;
    if (operation != BlockRequestOperation::FLUSH && count == 0) return 0;
    auto const ADDRESS = reinterpret_cast<uint64_t>(buffer);
    bool const USER = operation != BlockRequestOperation::FLUSH && ADDRESS < ker::mod::mm::addr::get_hhdm_offset();
    auto* caller = USER && sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (USER && caller == nullptr) return -EFAULT;
    size_t const MAX_BLOCKS = std::max(size_t{1}, queue->limits.max_transfer_bytes / device->block_size);
    size_t remaining = count;
    auto* cursor = static_cast<std::byte*>(buffer);
    do {
        size_t const BLOCKS = operation == BlockRequestOperation::FLUSH ? 0 : std::min(remaining, MAX_BLOCKS);
        size_t const BYTES = BLOCKS * device->block_size;
        std::byte* staging = USER ? new (std::nothrow) std::byte[BYTES] : nullptr;
        if (USER && staging == nullptr) return -ENOMEM;
        if (USER && operation == BlockRequestOperation::WRITE &&
            !ker::mod::sys::usercopy::copy_from_task_mapped(*caller, reinterpret_cast<uint64_t>(cursor), staging, BYTES)) {
            delete[] staging;
            return -EFAULT;
        }
        BlockRequest request;
        BlockSegment segment{USER ? staging : cursor, BYTES};
        auto const SEGMENTS =
            operation == BlockRequestOperation::FLUSH ? std::span<const BlockSegment>{} : std::span<const BlockSegment>{&segment, 1};
        int result = 0;
        for (;;) {
            result = submit_request(device, request, operation, operation == BlockRequestOperation::FLUSH ? 0 : lba, BLOCKS, SEGMENTS,
                                    30'000'000, nullptr, nullptr, true, false);
            if (result != -EAGAIN) break;
            service_queue(*queue);
            pause_task();
        }
        if (result == 0) {
            result = block_request_wait(request);
            int const RELEASE = block_request_release(request);
            if (result == 0) result = RELEASE;
        }
        if (result == 0 && USER && operation == BlockRequestOperation::READ &&
            !ker::mod::sys::usercopy::copy_to_task_mapped(*caller, reinterpret_cast<uint64_t>(cursor), staging, BYTES))
            result = -EFAULT;
        delete[] staging;
        if (result != 0) return result;
        if (operation == BlockRequestOperation::FLUSH) break;
        remaining -= BLOCKS;
        lba += BLOCKS;
        cursor += BYTES;
    } while (remaining != 0);
    return 0;
}

auto block_queue_snapshots(std::span<BlockQueueSnapshot> output) -> size_t {
    size_t count = 0;
    for (auto& queue : queues) {
        uint64_t const REGISTRY_FLAGS = registry_lock.lock_irqsave();
        auto const* device = queue.device;
        registry_lock.unlock_irqrestore(REGISTRY_FLAGS);
        if (device == nullptr || count >= output.size()) continue;
        auto& snapshot = output[count++];
        uint64_t const FLAGS = queue.lock.lock_irqsave();
        snapshot = {};
        snapshot.name = device->name;
        snapshot.limits = queue.limits;
        snapshot.stats = queue.model.stats;
        snapshot.generation = queue.model.generation;
        snapshot.owned_requests = queue.model.owned_requests;
        snapshot.owned_bytes = queue.model.owned_bytes;
        snapshot.admission_open = queue.model.admission_open;
        snapshot.async_admission_open = queue.model.admission_open && !queue.caller_driven;
        snapshot.worker_present = queue.worker != nullptr && !queue.worker_starting;
        snapshot.caller_driven = queue.caller_driven;
        snapshot.flushes = queue.flushes;
        snapshot.latency_us = queue.latency_us;
        snapshot.max_latency_us = queue.max_latency_us;
        snapshot.latency_histogram = queue.latency_histogram;
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        snapshot.wait_diagnostics = queue.wait_diagnostics;
#endif
        for (auto const& state : queue.model.requests) {
            if (state.state == BlockRequestState::QUEUED) {
                ++snapshot.queued;
                snapshot.queued_bytes += state.bytes;
            }
            if (state.state == BlockRequestState::ISSUED) {
                ++snapshot.issued;
                snapshot.issued_bytes += state.bytes;
            }
            if (state.state == BlockRequestState::COMPLETING) {
                ++snapshot.completing;
                snapshot.completing_bytes += state.bytes;
            }
        }
        queue.lock.unlock_irqrestore(FLAGS);
    }
    return count;
}
}  // namespace ker::dev
