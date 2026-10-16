// Actual registration/cancel/retire/survey/pageout/prepare bodies. Park, IRQ,
// PTE/root lifetime and physical I/O are explicit finite Host endpoints; these
// cases do not implement a scheduler or claim scheduled overlap qualification.
namespace fixture {
void reset_wait(unsigned nodes = 2) {
    reset_current();
    model::domains(nodes);
    model::parks = 0;
    v::readiness_endpoint::snapshot_mode = 0;
    model::runqueues = true;
    model::park_hook = model::wake_hook = {};
    actor.anonymous_swap_wait_generation = 0;
    actor.anonymous_swap_completion_generation = 0;
    actor.deferred_task_switch = actor.wants_block = actor.voluntary = actor.interrupting_signal = actor.wki_execve = false;
    actor.wki_proxy_task_id = 0;
    actor.wakeup_pending = false;
    v::anonymous_swap_generation = 10;
}
auto queue_current(s::task::Task& producer) -> v::AnonymousSwapMapping* {
    auto* r = current_row();
    producer.pagemap = &roots[0];
    uint64_t generation = 0;
    model::check(v::probe_anonymous_swap_fault(&producer, VA, p::PageFault{}, true, generation) == v::AnonymousSwapFaultDisposition::PARK,
                 "actual hardware producer queues independently owned operation");
    model::check(producer.refs == 2 && r->pagein_queued && generation == r->generation, "hardware publication owns exact ref");
    return r;
}
auto join_current(v::AnonymousPageinSnapshot& out) -> int {
    return v::capture_current_anonymous_user_page(actor, VA, true, out, nullptr, true);
}
// A parked caller keeps its C++ Scope alive on its own stack. Producer work
// below executes as a separate supplied DAEMON identity, so that caller-local
// endpoint counter is saved/restored rather than attributed to the worker.
struct WorkerEndpoint {
    s::task::Task* caller{s::current};
    unsigned caller_scopes{model::preparation_scopes};
    s::task::Task worker;
    WorkerEndpoint() {
        s::current = &worker;
        model::preparation_scopes = 0;
    }
    ~WorkerEndpoint() {
        s::current = caller;
        model::preparation_scopes = caller_scopes;
    }
};
void check_retired(s::task::Task& task, uint64_t generation) {
    model::check(task.anonymous_swap_wait_mapping == nullptr && task.anonymous_swap_wait_next == nullptr && task.refs == 1 &&
                     task.anonymous_swap_completion_generation == generation,
                 "producer retires exact node then returns exactly one ref");
}
// Whole actual PAGEOUT is used to create a later real SWAPPED generation;
// no test reimplementation of terminal state, frame/ref or slot transitions.
void pageout_installed() {
    v::readiness_endpoint::source_frame = nullptr;
    auto const physical = static_cast<uint64_t>(v::entries[&roots[0]].frame) << 12;
    for (auto const& block : model::blocks)
        if (block.refs && block.physical == physical) v::readiness_endpoint::source_frame = block.p;
    model::check(v::readiness_endpoint::source_frame != nullptr, "exact current PTE retained source frame");
    v::AnonymousPageoutCandidate candidate;
    model::check(v::snapshot_anonymous_candidate(&actor, 0, candidate) == v::AnonymousCandidateDisposition::SELECTED,
                 "retained installed source for actual next generation");
    model::check(v::pageout_anonymous_candidate(candidate), "actual successor generation PAGEOUT commits");
}
void run_current_wait() {
    // Queue, dequeue-to-operation and in-I/O are three distinct real ownership
    // windows. The second port performs the actual begin before ordinary join.
    for (unsigned phase = 0; phase != 3; ++phase) {
        reset_wait(phase == 1 ? 1 : 2);
        s::task::Task producer;
        auto* r = queue_current(producer);
        auto const generation = r->generation;
        v::AnonymousPageinOperation operation;
        bool begun = phase == 2;
        if (begun) v::begin_anonymous_pagein(v::anonymous_pagein_dequeue(), operation);
        model::park_hook = [&] {
            model::check(actor.refs == 2 && actor.anonymous_swap_wait_mapping == r && model::gates == 1,
                         "ordinary join retains only producer ref and operation gate");
            WorkerEndpoint const worker;
            if (begun)
                v::finish_anonymous_pagein(operation);
            else
                process();
        };
        model::check(prepare_current() == 0 && model::parks == 1 && model::reads == 1 && model::allocations == 1,
                     "queued or PAGEIN ordinary continuation joins without duplicate allocation");
        check_retired(actor, generation);
        check_retired(producer, generation);
        close_installed();
        current_closed();
    }

    reset_wait();
    s::task::Task producer;
    auto* r = queue_current(producer);
    v::AnonymousPageinSnapshot joined;
    model::check(join_current(joined) == static_cast<int>(v::AnonymousCurrentSurvey::JOINED), "actual short survey joins winner");
    process();
    model::check(v::wait_current_anonymous_page(actor, joined) == 0 && model::parks == 0,
                 "completion before first park check needs no fabricated syscall frame");
    close_installed();
    current_closed();

    reset_wait();
    s::task::Task spurious_producer;
    queue_current(spurious_producer);
    model::park_hook = [] { model::park_hook = [] { process(); }; };
    model::check(prepare_current() == 0 && model::parks == 2 && model::reads == 1, "spurious wake repeats actual completion predicate");
    close_installed();
    current_closed();

    // Detach clears all opaque descriptors but leaves pending completion. The
    // head AND null-next tail must remain unavailable until real retirement.
    reset_wait();
    s::task::Task detached_producer;
    r = queue_current(detached_producer);
    model::check(join_current(joined) == 2, "linked second node");
    uint64_t const flags = v::cow_pte_lock.lock_irqsave();
    auto* chain = v::detach_anonymous_swap_waiters_locked(r);
    model::check(chain == &actor && actor.anonymous_swap_wait_mapping == nullptr &&
                     detached_producer.anonymous_swap_wait_mapping == nullptr && !v::anonymous_swap_waiter_idle_locked(actor) &&
                     !v::anonymous_swap_waiter_idle_locked(detached_producer),
                 "detached head and tail cannot be reused before retirement");
    v::cow_pte_lock.unlock_irqrestore(flags);
    auto* const next = actor.anonymous_swap_wait_next;
    auto const refs = actor.refs.load();
    v::cancel_current_anonymous_wait(actor, joined.generation);
    model::check(actor.anonymous_swap_wait_next == next && actor.refs == refs &&
                     actor.anonymous_swap_wait_generation == joined.generation && actor.anonymous_swap_completion_generation == 0,
                 "detached cancellation changes no producer link token ref or generation");
    uint64_t ignored = 0;
    model::check(v::probe_anonymous_swap_fault(&actor, VA, p::PageFault{}, true, ignored) == v::AnonymousSwapFaultDisposition::FAILED,
                 "hardware also refuses detached pending node");
    model::check(join_current(joined) == -EAGAIN, "ordinary also refuses detached pending node");
    s::task::Task replacement;
    replacement.pagemap = &roots[0];
    model::check(v::probe_anonymous_swap_fault(&replacement, VA, p::PageFault{}, true, ignored) == v::AnonymousSwapFaultDisposition::PARK,
                 "independent new list while old chain detached");
    // At canonical wake the old node is retired. Re-registering it is legal,
    // and the old producer must never clear/read the new intrusive links.
    model::wake_hook = [&] {
        model::check(join_current(joined) == 2 && actor.anonymous_swap_wait_next == &replacement && actor.refs == 3,
                     "wake may reuse retired node before prior off-lock ref return");
    };
    v::wake_anonymous_swap_waiters(chain);
    model::check(r->waiters == &actor && actor.anonymous_swap_wait_next == &replacement && actor.refs == 2 && detached_producer.refs == 1,
                 "old retirement leaves newly registered ownership untouched");
    process();
    model::check(replacement.refs == 1, "new generation list returns its own independent ref");
    close_installed();
    current_closed();

    for (unsigned why = 0; why != 5; ++why) {
        reset_wait();
        s::task::Task cancel_producer;
        r = queue_current(cancel_producer);
        model::park_hook = [&, why] {
            if (why == 0) actor.interrupting_signal = true;
            if (why == 1) actor.usercopy_pagemap_closing = true;
            if (why == 2) actor.exit_in_progress = true;
            if (why == 3) model::shutdown = true;
            if (why == 4) actor.deferred_task_switch = true;
        };
        int const expected = why == 0 ? -EINTR : why < 3 ? -ECANCELED : -EWOULDBLOCK;
        model::check(prepare_current() == expected && actor.refs == 1 && actor.anonymous_swap_wait_mapping == nullptr &&
                         actor.anonymous_swap_wait_next == nullptr && actor.anonymous_swap_wait_generation == 0 &&
                         actor.anonymous_swap_completion_generation == 0 && r->waiters == &cancel_producer,
                     "linked cancellation returns only own ref and resets idle generation without completion forgery");
        v::cancel_current_anonymous_wait(actor, r->generation);
        model::check(actor.refs == 1 && v::anonymous_swap_waiter_idle_locked(actor), "linked cancellation is exactly once");
        actor.interrupting_signal = actor.deferred_task_switch = false;
        actor.usercopy_pagemap_closing = actor.exit_in_progress = false;
        model::shutdown = false;
        process();
        check_retired(cancel_producer, cancel_producer.anonymous_swap_wait_generation);
        close_installed();
        current_closed();
    }

    reset_wait();
    // Resolve an opaque node address in the global registry before dereference.
    actor.anonymous_swap_wait_mapping = reinterpret_cast<void*>(1);
    actor.anonymous_swap_wait_generation = 17;
    v::cancel_current_anonymous_wait(actor, 17);
    model::check(
        actor.anonymous_swap_wait_mapping == reinterpret_cast<void*>(1) && actor.refs == 1 && actor.anonymous_swap_wait_generation == 17,
        "unknown opaque descriptor is never dereferenced or released");
    actor.anonymous_swap_wait_mapping = nullptr;
    actor.anonymous_swap_wait_generation = 0;
    current_closed();

    for (auto invalid : {uint64_t{0}, UINT64_MAX}) {
        reset_wait();
        s::task::Task invalid_producer;
        r = queue_current(invalid_producer);
        auto const real_generation = r->generation;
        r->generation = invalid;
        model::check(prepare_current() == -EAGAIN && actor.refs == 1 && model::parks == 0,
                     "ordinary refuses zero and MAX operation generation");
        uint64_t generation = 0;
        model::check(
            v::probe_anonymous_swap_fault(&actor, VA, p::PageFault{}, true, generation) == v::AnonymousSwapFaultDisposition::FAILED &&
                actor.refs == 1,
            "hardware refuses zero and MAX waiter generation");
        r->generation = real_generation;
        process();
        close_installed();
        current_closed();
    }
    reset_wait();
    r = current_row();
    v::anonymous_swap_generation = UINT64_MAX;  // Actual helper's second fetch wraps to zero.
    model::check(fault() == v::AnonymousSwapFaultDisposition::FAILED && !r->pagein_queued && r->frame == nullptr && model::gates == 0 &&
                     model::live() == 0 && v::anonymous_pagein_queue_count == 0 && actor.refs == 1,
                 "invalid new hardware generation refuses before gate frame or queue publication");
    purge();
    current_closed();

    reset_wait();
    r = current_row();
    actor.anonymous_swap_wait_generation = 99;
    actor.anonymous_swap_completion_generation = 0;  // Busy detached tuple, not a descriptor owner.
    model::check(fault() == v::AnonymousSwapFaultDisposition::FAILED && r->pagein_queued && r->frame && actor.refs == 1,
                 "busy hardware Task refusal leaves successfully queued producer drainable");
    actor.anonymous_swap_wait_generation = 0;
    process();
    close_installed();
    current_closed();

    // Source row is produced by the actual full PAGEOUT transaction. During
    // its real write endpoint ordinary capture joins, then actual commit or
    // rollback detaches before flush/frame/slot returns and terminal wake.
    for (bool rollback : {false, true}) {
        reset_wait();
        auto* resident = model::allocate(model::topology.memory[0].base);
        auto const physical = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(resident)));
        v::entries[&roots[0]] = v::pte_from_raw(physical | p::PAGE_PRESENT | p::PAGE_USER | p::PAGE_WRITE);
        v::readiness_endpoint::source_frame = resident;
        v::AnonymousPageoutCandidate candidate;
        model::check(v::snapshot_anonymous_candidate(&actor, 0, candidate) == v::AnonymousCandidateDisposition::SELECTED,
                     "actual pageout receives retained resident frame and root gate");
        v::AnonymousPageinSnapshot pageout_snapshot;
        model::write_hook = [&] {
            model::check(join_current(pageout_snapshot) == 2 && actor.refs == 2 && model::gates == 1,
                         "ordinary joins actual frozen PAGEOUT generation");
        };
        model::wake_hook = [&] {
            model::check(actor.anonymous_swap_wait_mapping == nullptr && actor.anonymous_swap_wait_next == nullptr &&
                             actor.anonymous_swap_completion_generation == pageout_snapshot.generation &&
                             model::live() == (rollback ? 1U : 0U) && model::slot_frees == (rollback ? 1U : 0U),
                         "PAGEOUT commit and rollback notify only after real frame slot returns");
        };
        model::io_error = rollback ? -EIO : 0;
        model::check(v::pageout_anonymous_candidate(candidate) == !rollback, "actual PAGEOUT outcome");
        model::check(
            actor.anonymous_swap_wait_mapping == nullptr && actor.anonymous_swap_completion_generation == pageout_snapshot.generation,
            "PAGEOUT terminal detach and completion cover both outcomes");
        model::write_hook = {};
        model::io_error = 0;
        model::check(v::wait_current_anonymous_page(actor, pageout_snapshot) == 0 && model::parks == 0,
                     "PAGEOUT completion is retirement not claimed PTE success");
        check_retired(actor, pageout_snapshot.generation);
        if (rollback) {
            model::check(v::anonymous_swap_mappings == nullptr && v::entries[&roots[0]].present,
                         "rollback re-survey observes real resident mapping");
            ker::mod::mm::phys::page_ref_dec(resident);
        } else {
            model::check(prepare_current() == 0 && model::reads == 1 && model::allocations == 2,
                         "committed PAGEOUT permits one subsequent ordinary synchronous issue");
            close_installed();
        }
        current_closed();
    }

    reset_wait();
    s::task::Task root_producer;
    queue_current(root_producer);
    model::park_hook = [] {
        process();
        ++model::serial;
    };
    model::check(prepare_current() == -EAGAIN && model::reads == 1 && actor.refs == 1,
                 "post-wake root incarnation change is never accepted as same-root success");
    close_installed();
    current_closed();

    reset_wait();
    s::task::Task failed_producer;
    r = queue_current(failed_producer);
    model::io_error = -EILSEQ;
    model::park_hook = [] { process(); };
    model::check(prepare_current() == -EILSEQ && r->state == v::AnonymousSwapRuntimeState::FAILED && actor.refs == 1,
                 "post-wake FAILED preserves exact stored producer errno");
    purge();
    current_closed();

    reset_wait();
    s::task::Task policy_producer;
    queue_current(policy_producer);
    actor.policy = {2, n::Mode::BIND};
    model::park_hook = [] {
        process();
        actor.policy = {1, n::Mode::BIND};
        model::cpu = 1;
        pageout_installed();
    };
    model::check(prepare_current() == 0 && model::parks == 1 && model::allocations == 2 && model::reads == 2 &&
                     model::blocks.back().physical == model::topology.memory[1].base + 4096,
                 "one join then one issue retains original policy and CPU capture");
    close_installed();
    current_closed();

    reset_wait();
    s::task::Task first_producer, later_producer;
    queue_current(first_producer);
    model::park_hook = [&] {
        process();
        pageout_installed();
        later_producer.pagemap = &roots[0];
        uint64_t generation = 0;
        model::check(
            v::probe_anonymous_swap_fault(&later_producer, VA, p::PageFault{}, true, generation) == v::AnonymousSwapFaultDisposition::PARK,
            "actual later queued generation");
    };
    model::check(prepare_current() == -EAGAIN && model::parks == 1 && actor.refs == 1 && model::reads == 1,
                 "ordinary invocation joins at most one producer generation");
    process();
    close_installed();
    current_closed();

    reset_wait();
    current_row();
    s::task::Task raced_producer;
    model::allocation_hook = [&] {
        raced_producer.pagemap = &roots[0];
        uint64_t generation = 0;
        model::check(
            v::probe_anonymous_swap_fault(&raced_producer, VA, p::PageFault{}, true, generation) == v::AnonymousSwapFaultDisposition::PARK,
            "hardware wins during ordinary private allocation");
    };
    model::park_hook = [] {
        process();
        pageout_installed();
    };
    model::check(prepare_current() == -EAGAIN && model::parks == 1 && model::allocations == 2 && model::reads == 1 && model::live() == 0,
                 "raced synchronous candidate returns before wait and no second issue is attempted");
    purge();
    current_closed();

    // Each admission rejection retains producer authority and does not park.
    for (unsigned kind = 0; kind != 6; ++kind) {
        reset_wait();
        s::task::Task unsafe_producer;
        queue_current(unsafe_producer);
        if (kind == 0) model::runqueues = false;
        if (kind == 1) actor.wants_block = true;
        if (kind == 2) actor.voluntary = true;
        if (kind == 3) actor.wki_proxy_task_id = 1;
        if (kind == 4) actor.wki_execve = true;
        if (kind == 5) actor.deferred_task_switch = true;
        model::check(prepare_current() == -EAGAIN && actor.refs == 1 && model::parks == 0,
                     "unsafe continuation context never joins or steals queued work");
        process();
        close_installed();
        current_closed();
    }
    actor.type = s::task::TaskType::DAEMON;
    actor.syscall_account_start_us = 0;
    s::current = nullptr;
    model::enabled = true;
    std::printf("PASS actual current continuation cases=%u checks=%u\n", model::cases, model::checks);
}
}  // namespace fixture
