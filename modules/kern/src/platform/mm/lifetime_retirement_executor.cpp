#include <cerrno>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <utility>

namespace ker::mod::mm::file_mapping {
constinit LifetimeRetirementExecutor LifetimeRetirementExecutor::singleton_{};

namespace {
void increment_saturated(uint64_t& value) noexcept {
    if (value != UINT64_MAX) ++value;
}
}  // namespace

RetirementCompletion::RetirementCompletion(RetirementCompletion&& other) noexcept
    : executor_(std::exchange(other.executor_, nullptr)),
      ticket_(std::exchange(other.ticket_, nullptr)),
      generation_(std::exchange(other.generation_, 0)) {}

auto RetirementCompletion::operator=(RetirementCompletion&& other) noexcept -> RetirementCompletion& {
    if (this != &other) {
        reset();
        executor_ = std::exchange(other.executor_, nullptr);
        ticket_ = std::exchange(other.ticket_, nullptr);
        generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
}

RetirementCompletion::~RetirementCompletion() { reset(); }

auto RetirementCompletion::query() const noexcept -> RetirementReturn {
    if (executor_ == nullptr) return {RetirementReturnState::EMPTY, 0};
    return executor_->query_completion(ticket_, generation_);
}

void RetirementCompletion::reset() noexcept {
    auto* const EXECUTOR = std::exchange(executor_, nullptr);
    auto* const TICKET = std::exchange(ticket_, nullptr);
    uint64_t const GENERATION = std::exchange(generation_, 0);
    if (EXECUTOR != nullptr) EXECUTOR->release_completion(TICKET, GENERATION);
}

auto LifetimeRetirementExecutor::contains_ticket(const LifetimeRetirementTicket* ticket) const noexcept -> bool {
    uintptr_t const ADDRESS = reinterpret_cast<uintptr_t>(ticket);
    uintptr_t const BASE = reinterpret_cast<uintptr_t>(tickets_.data());
    return ADDRESS >= BASE && ADDRESS - BASE < sizeof(tickets_) && (ADDRESS - BASE) % sizeof(LifetimeRetirementTicket) == 0;
}

auto LifetimeRetirementExecutor::observe_adapter(void* context, LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument,
                                                 RetirementCompletion& empty) noexcept -> bool {
    return static_cast<LifetimeRetirementExecutor*>(context)->observe_return(ticket, run, argument, empty);
}

auto LifetimeRetirementExecutor::observe_return(LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument,
                                                RetirementCompletion& empty) noexcept -> bool {
    uint64_t const FLAGS = lock_.lock_irqsave();
    bool const VALID = empty.executor_ == nullptr && empty.ticket_ == nullptr && empty.generation_ == 0 && contains_ticket(&ticket) &&
                       ticket.owner_ == this && ticket.state_ == LifetimeRetirementTicket::State::RESERVED && ticket.generation_ != 0 &&
                       run != nullptr && argument != nullptr && ticket.run_ == run && ticket.argument_ == argument &&
                       ticket.observers_ < OBSERVER_LIMIT && observer_handles_ != UINT64_MAX;
    if (VALID) {
        ++ticket.observers_;
        ++observer_handles_;
        empty.executor_ = this;
        empty.ticket_ = &ticket;
        empty.generation_ = ticket.generation_;
    }
    lock_.unlock_irqrestore(FLAGS);
    return VALID;
}

auto LifetimeRetirementExecutor::query_completion(const LifetimeRetirementTicket* ticket, uint64_t generation) noexcept
    -> RetirementReturn {
    uint64_t const FLAGS = lock_.lock_irqsave();
    RetirementReturn result{RetirementReturnState::INVALID, 0};
    bool const VALID = contains_ticket(ticket) && ticket->owner_ == this && generation != 0 && ticket->generation_ == generation &&
                       ticket->observers_ != 0;
    if (VALID && ticket->state_ == LifetimeRetirementTicket::State::DONE)
        result = {RetirementReturnState::RETURNED, ticket->callback_result_};
    else if (VALID &&
             (ticket->state_ == LifetimeRetirementTicket::State::RESERVED || ticket->state_ == LifetimeRetirementTicket::State::READY ||
              ticket->state_ == LifetimeRetirementTicket::State::RUNNING))
        result = {RetirementReturnState::PENDING, 0};
    else
        note_protocol_error_locked();
    lock_.unlock_irqrestore(FLAGS);
    return result;
}

void LifetimeRetirementExecutor::recycle_completed_locked(LifetimeRetirementTicket& ticket) noexcept {
    if (ticket.state_ != LifetimeRetirementTicket::State::DONE || ticket.observers_ != 0) {
        note_protocol_error_locked();
        return;
    }
    if (ticket.generation_ >= GENERATION_LIMIT) {
        ticket.state_ = LifetimeRetirementTicket::State::EXHAUSTED;
        ticket.next_ = nullptr;
        ++exhausted_slots_;
    } else {
        ticket.state_ = LifetimeRetirementTicket::State::FREE;
        ticket.next_ = free_;
        free_ = &ticket;
    }
}

void LifetimeRetirementExecutor::release_completion(LifetimeRetirementTicket* ticket, uint64_t generation) noexcept {
    uint64_t const FLAGS = lock_.lock_irqsave();
    bool const VALID =
        contains_ticket(ticket) && ticket->owner_ == this && generation != 0 && ticket->generation_ == generation &&
        ticket->observers_ != 0 && observer_handles_ != 0 &&
        (ticket->state_ == LifetimeRetirementTicket::State::RESERVED || ticket->state_ == LifetimeRetirementTicket::State::READY ||
         ticket->state_ == LifetimeRetirementTicket::State::RUNNING || ticket->state_ == LifetimeRetirementTicket::State::DONE);
    if (!VALID) {
        note_protocol_error_locked();
        lock_.unlock_irqrestore(FLAGS);
        return;
    }
    --ticket->observers_;
    --observer_handles_;
    if (ticket->state_ == LifetimeRetirementTicket::State::DONE && ticket->observers_ == 0) {
        if (completed_storage_slots_ == 0) {
            note_protocol_error_locked();  // Never underflow or falsely recycle corrupt accounting.
        } else {
            --completed_storage_slots_;
            recycle_completed_locked(*ticket);
        }
    }
    lock_.unlock_irqrestore(FLAGS);
    // No callback/argument, File, Task, allocation, wake or backend access.
}

auto LifetimeRetirementExecutor::initialize_boot() -> bool {
    auto const HOLD = sched::hold_kernel_shutdown();
    auto& queue = singleton_;
    uint64_t flags = queue.lock_.lock_irqsave();
    if (queue.initialized_) {
        bool const AVAILABLE = queue.available_ && queue.admission_open_;
        queue.lock_.unlock_irqrestore(flags);
        return AVAILABLE;
    }
    queue.initialized_ = true;
    for (size_t i = 0; i < queue.tickets_.size(); ++i) {
        auto& ticket = queue.tickets_[i];
        ticket.owner_ = &queue;
        ticket.next_ = queue.free_;
        queue.free_ = &ticket;
    }
    queue.lock_.unlock_irqrestore(flags);

    auto* worker = sched::task::Task::create_kernel_thread("file_retire", worker_entry);
    if (worker == nullptr) return false;
    // Initial scheduler ownership belongs to this exclusively unpublished Task.
    uint32_t expected_refs = 1;
    if (!worker->ref_count.compare_exchange_strong(expected_refs, 2, std::memory_order_acq_rel, std::memory_order_acquire)) {
        // A fresh factory result has its sole scheduler owner. Corrupt lifetime
        // state cannot authorize deletion or an untracked unpublished leak.
        __builtin_trap();
    }
    worker->kernel_thread_context = &queue;
    if (!sched::FileRetirementSchedulerPort::install(*worker, worker_entry, &queue)) {
        worker->kernel_thread_context = nullptr;
        worker->release();
        sched::task::destroy_unpublished_user_thread(worker);
        return false;
    }
    flags = queue.lock_.lock_irqsave();
    queue.worker_ = worker;
    queue.lock_.unlock_irqrestore(flags);
    if (!sched::post_task_balanced(worker)) {
        // Fresh daemon post rollback leaves it wholly unpublished.
        if (!sched::FileRetirementSchedulerPort::remove(*worker)) {
            // An unexpected lease mismatch must not free still-retained Task
            // storage. Preserve ownership and mark incomplete/unclean instead.
            flags = queue.lock_.lock_irqsave();
            queue.note_protocol_error_locked();
            queue.lock_.unlock_irqrestore(flags);
            return false;
        }
        flags = queue.lock_.lock_irqsave();
        queue.worker_ = nullptr;
        queue.lock_.unlock_irqrestore(flags);
        worker->kernel_thread_context = nullptr;
        worker->release();
        sched::task::destroy_unpublished_user_thread(worker);
        return false;
    }
    flags = queue.lock_.lock_irqsave();
    queue.available_ = true;
    // Boot serialization is mandatory. Also refuse admission if shutdown won
    // before opening; the published exact worker can still be explicitly joined.
    queue.admission_open_ = !sched::kernel_threads_shutdown_requested();
    bool const OPEN = queue.admission_open_;
    queue.lock_.unlock_irqrestore(flags);
    return OPEN;
}

auto LifetimeRetirementExecutor::port() noexcept -> const LifetimeRetirementPort* {
    auto& queue = singleton_;
    uint64_t const FLAGS = queue.lock_.lock_irqsave();
    auto const* result = queue.available_ ? &queue.port_ : nullptr;
    queue.lock_.unlock_irqrestore(FLAGS);
    return result;
}

auto LifetimeRetirementExecutor::reserve_adapter(void* context, LifetimeRetirementRun run, void* argument) noexcept
    -> LifetimeRetirementTicket* {
    return static_cast<LifetimeRetirementExecutor*>(context)->reserve(run, argument);
}

void LifetimeRetirementExecutor::submit_adapter(void* context, LifetimeRetirementTicket& ticket) noexcept {
    static_cast<LifetimeRetirementExecutor*>(context)->submit_ready(ticket);
}

auto LifetimeRetirementExecutor::reserve(LifetimeRetirementRun run, void* argument) noexcept -> LifetimeRetirementTicket* {
    if (run == nullptr || argument == nullptr) return nullptr;
    uint64_t const FLAGS = lock_.lock_irqsave();
    // Never increment a max generation, even if a malformed free list exposed
    // an already exhausted slot. Withhold storage before any caller ownership.
    if (available_ && admission_open_) {
        while (free_ != nullptr && free_->generation_ >= GENERATION_LIMIT) {
            auto* exhausted = free_;
            free_ = exhausted->next_;
            exhausted->next_ = nullptr;
            exhausted->state_ = LifetimeRetirementTicket::State::EXHAUSTED;
            ++exhausted_slots_;
        }
    }
    if (!available_ || !admission_open_ || free_ == nullptr) {
        lock_.unlock_irqrestore(FLAGS);
        return nullptr;
    }
    auto* ticket = free_;
    free_ = ticket->next_;
    ticket->next_ = nullptr;
    ticket->run_ = run;
    ticket->argument_ = argument;
    ++ticket->generation_;  // Prior value strictly below GENERATION_LIMIT; no wrap.
    ticket->observers_ = 0;
    ticket->callback_result_ = 0;
    ticket->state_ = LifetimeRetirementTicket::State::RESERVED;
    ++outstanding_;
    lock_.unlock_irqrestore(FLAGS);
    return ticket;
}

void LifetimeRetirementExecutor::note_protocol_error_locked() noexcept { increment_saturated(protocol_errors_); }

void LifetimeRetirementExecutor::submit_ready(LifetimeRetirementTicket& ticket) noexcept {
    uint64_t const FLAGS = lock_.lock_irqsave();
    // Internal caller must own this exact reservation and submit exactly once.
    // Corruption/double submit is retained as unclean, without unsafe logging or
    // pretending the pending reservation completed. Valid late submit cannot fail.
    if (!contains_ticket(&ticket) || ticket.owner_ != this || ticket.state_ != LifetimeRetirementTicket::State::RESERVED ||
        ticket.generation_ == 0 || ticket.run_ == nullptr || ticket.argument_ == nullptr || outstanding_ == 0) {
        note_protocol_error_locked();
        lock_.unlock_irqrestore(FLAGS);
        return;
    }
    ticket.state_ = LifetimeRetirementTicket::State::READY;
    ticket.next_ = nullptr;
    if (tail_ != nullptr)
        tail_->next_ = &ticket;
    else
        head_ = &ticket;
    tail_ = &ticket;
    ++ready_;
    lock_.unlock_irqrestore(FLAGS);
    // No allocation, wake, Task ref, log, backend call or synchronous callback.
    // Callback may already run on another CPU; no further ticket/arg access.
}

auto LifetimeRetirementExecutor::cancel_adapter(void* context, LifetimeRetirementTicket& ticket, LifetimeRetirementRun run,
                                                void* argument) noexcept -> bool {
    return static_cast<LifetimeRetirementExecutor*>(context)->cancel_reserved(ticket, run, argument);
}

auto LifetimeRetirementExecutor::cancel_reserved(LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument) noexcept
    -> bool {
    uint64_t const FLAGS = lock_.lock_irqsave();
    bool const VALID = contains_ticket(&ticket) && ticket.owner_ == this && ticket.state_ == LifetimeRetirementTicket::State::RESERVED &&
                       ticket.generation_ != 0 && ticket.run_ == run && run != nullptr && ticket.argument_ == argument &&
                       argument != nullptr && ticket.observers_ == 0 && outstanding_ != 0;
    if (VALID) {
        --outstanding_;
        ticket.run_ = nullptr;
        ticket.argument_ = nullptr;
        ticket.state_ = LifetimeRetirementTicket::State::DONE;
        increment_saturated(cancelled_);
        recycle_completed_locked(ticket);
    }
    lock_.unlock_irqrestore(FLAGS);
    return VALID;  // No callback, wake, active owner or argument access.
}

void LifetimeRetirementExecutor::close_new_reservations() noexcept {
    auto& queue = singleton_;
    uint64_t const FLAGS = queue.lock_.lock_irqsave();
    queue.admission_open_ = false;
    queue.lock_.unlock_irqrestore(FLAGS);
}

auto LifetimeRetirementExecutor::snapshot_locked() const noexcept -> LifetimeRetirementSnapshot {
    return {initialized_,
            available_,
            admission_open_,
            stop_requested_,
            loop_finished_.load(std::memory_order_acquire),
            outstanding_,
            outstanding_ - ready_ - running_,
            ready_,
            running_,
            completed_,
            cancelled_,
            close_errors_,
            protocol_errors_,
            first_close_result_,
            completed_storage_slots_,
            observer_handles_,
            exhausted_slots_};
}

auto LifetimeRetirementExecutor::snapshot() noexcept -> LifetimeRetirementSnapshot {
    auto& queue = singleton_;
    uint64_t const FLAGS = queue.lock_.lock_irqsave();
    auto const RESULT = queue.snapshot_locked();
    queue.lock_.unlock_irqrestore(FLAGS);
    return RESULT;
}

void LifetimeRetirementExecutor::worker_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        singleton_.drain_loop();
        // Whole callback/idle/queue retirement protected; explicit exit follows
        // only after no further queue access is needed.
    }
    sched::exit_current_kernel_thread();
}

void LifetimeRetirementExecutor::drain_loop() {
    for (;;) {
        LifetimeRetirementTicket* ticket = nullptr;
        LifetimeRetirementRun run = nullptr;
        void* argument = nullptr;
        uint64_t flags = lock_.lock_irqsave();
        if (head_ != nullptr) {
            ticket = head_;
            head_ = ticket->next_;
            if (head_ == nullptr) tail_ = nullptr;
            ticket->next_ = nullptr;
            ticket->state_ = LifetimeRetirementTicket::State::RUNNING;
            --ready_;
            ++running_;
            run = ticket->run_;
            argument = ticket->argument_;
        } else if (stop_requested_ && !admission_open_ && outstanding_ == 0) {
            auto* current = sched::get_current_task();
            current->kernel_thread_context = nullptr;
            lock_.unlock_irqrestore(flags);
            loop_finished_.store(true, std::memory_order_release);  // Final queue access.
            return;
        }
        lock_.unlock_irqrestore(flags);
        if (ticket == nullptr) {
            sched::kern_sleep_us(POLL_US);
            continue;
        }
        // Entry's whole-worker ShutdownOwnership hold spans this entire call,
        // including all nested allocations, backend locks and voluntary yields.
        int const RESULT = run(argument);
        // The callback may have deleted argument and submitted a parent's ticket.
        // Only permanent queue/ticket storage is accessed from this point.
        flags = lock_.lock_irqsave();
        if (RESULT != 0) {
            if (close_errors_ == 0) first_close_result_ = RESULT;
            increment_saturated(close_errors_);
        }
        --running_;
        --outstanding_;
        increment_saturated(completed_);
        ticket->run_ = nullptr;
        ticket->argument_ = nullptr;
        ticket->callback_result_ = RESULT;
        ticket->state_ = LifetimeRetirementTicket::State::DONE;
        if (ticket->observers_ != 0)
            ++completed_storage_slots_;
        else
            recycle_completed_locked(*ticket);
        lock_.unlock_irqrestore(flags);
    }
}

auto LifetimeRetirementExecutor::is_current_worker() noexcept -> bool {
    auto* const CURRENT = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    auto& queue = singleton_;
    uint64_t const FLAGS = queue.lock_.lock_irqsave();
    bool const MATCH = CURRENT != nullptr && CURRENT == queue.worker_;
    queue.lock_.unlock_irqrestore(FLAGS);
    return MATCH;
}

auto LifetimeRetirementExecutor::poll_shutdown() -> LifetimeRetirementDrain {
    if (!sched::can_query_current_task() || sched::get_current_task() == nullptr || sched::preempt_count() != 0 ||
        !sched::interrupts_enabled())
        return {-EPERM, false, snapshot()};
    auto const HOLD = sched::hold_kernel_shutdown();
    auto& queue = singleton_;
    auto* const CURRENT = sched::get_current_task();
    sched::task::Task* join = nullptr;
    uint64_t flags = queue.lock_.lock_irqsave();
    auto observed = queue.snapshot_locked();
    if (CURRENT == queue.worker_) {
        queue.lock_.unlock_irqrestore(flags);
        return {-EDEADLK, false, observed};
    }
    queue.admission_open_ = false;
    if (queue.outstanding_ == 0) queue.stop_requested_ = true;
    observed = queue.snapshot_locked();
    if (queue.worker_ == nullptr) {
        int result = observed.outstanding == 0 && observed.protocol_errors == 0 ? 0 : -EPROTO;
        if (result == 0 && observed.close_errors != 0) result = observed.first_close_result < 0 ? observed.first_close_result : -EIO;
        queue.lock_.unlock_irqrestore(flags);
        return {result, observed.outstanding == 0, observed};
    }
    // Read the retained Task only while the queue still owns it. One controller
    // claims the join under this lock; other polls never carry a borrowed Task
    // pointer through another controller's final release.
    if (!queue.joining_ && observed.loop_finished && queue.worker_->state.load(std::memory_order_acquire) == sched::task::TaskState::DEAD) {
        queue.joining_ = true;
        join = queue.worker_;
    }
    queue.lock_.unlock_irqrestore(flags);
    if (join == nullptr) return {-EAGAIN, false, observed};

    // The queue-owned reference remains held while the separate registry lease
    // is removed. Never nest the queue lock with the scheduler registry lock.
    if (!sched::FileRetirementSchedulerPort::remove(*join)) {
        flags = queue.lock_.lock_irqsave();
        queue.joining_ = false;
        queue.note_protocol_error_locked();
        observed = queue.snapshot_locked();
        queue.lock_.unlock_irqrestore(flags);
        return {-EPROTO, false, observed};
    }
    flags = queue.lock_.lock_irqsave();
    queue.worker_ = nullptr;
    queue.available_ = false;
    queue.joining_ = false;
    observed = queue.snapshot_locked();
    queue.lock_.unlock_irqrestore(flags);
    join->release();  // Last queue Task access; ordinary dead-list GC owns the rest.
    int result = 0;
    if (observed.protocol_errors != 0)
        result = -EPROTO;
    else if (observed.close_errors != 0)
        result = observed.first_close_result < 0 ? observed.first_close_result : -EIO;
    return {result, true, observed};
}

auto LifetimeRetirementExecutor::finish_shutdown(uint64_t timeout_us) -> LifetimeRetirementDrain {
    if (!sched::can_query_current_task() || sched::get_current_task() == nullptr || sched::preempt_count() != 0 ||
        !sched::interrupts_enabled())
        return {-EPERM, false, snapshot()};
    auto const HOLD = sched::hold_kernel_shutdown();
    uint64_t const START = time::get_us();
    for (;;) {
        auto const RESULT = poll_shutdown();
        if (RESULT.joined || RESULT.result != -EAGAIN) return RESULT;
        if (timeout_us == 0 || time::get_us() - START >= timeout_us) return {-ETIMEDOUT, false, snapshot()};
        // No synchronous GC/backend operation is hidden behind this timeout.
        // The owner controller separately retires roots/producers with their
        // actual resumable progress and keeps required providers running.
        sched::kern_sleep_us(POLL_US);
    }
}
}  // namespace ker::mod::mm::file_mapping
