// Actual prepare/survey/service/factory transactions, with explicit Host
// Task/current/IF/PTE/physical/SlotIo endpoints. No scheduled-runtime claim.
namespace fixture {
void reset_current() {
    reset();
    actor.type = s::task::TaskType::PROCESS;
    actor.state = s::task::TaskState::ACTIVE;
    actor.syscall_account_start_us = 1;
    actor.usercopy_pagemap_closing = false;
    actor.scheduler_transition_active = false;
    actor.exit_in_progress = false;
    actor.has_exited = false;
    s::current = &actor;
    model::interrupts = true;
    model::enabled = false;  // A tracked marker is authoritative with anonOFF.
    model::missing_leaf = false;
}
auto current_row() -> v::AnonymousSwapMapping* {
    auto* r = row();
    v::entries[&roots[0]] = v::pte_from_raw(v::anonymous_swap_marker_raw(r->original_raw));
    return r;
}
auto prepare_current(bool writable = true, uint64_t address = VA) -> int {
    ker::mod::sys::SyscallPreparationScope scope{actor};
    return v::prepare_current_anonymous_user_page(actor, address, writable, scope);
}
void current_closed() {
    model::check(actor.usercopy_pagemap_accesses == 0 && model::gates == 0 && model::preempt == 0 && model::preparation_scopes == 0 &&
                     model::live() == 0,
                 "ordinary final Task/root/CPU/frame ownership closure");
}
void run_current_preparation() {
    reset_current();
    model::topology.node_count = 0;
    model::check(prepare_current() == 0 && model::allocations == 0 && actor.usercopy_pagemap_accesses == 0,
                 "no tracked rows needs no topology/candidate");
    current_closed();

    reset_current();
    auto* r = current_row();
    actor.policy = {2, n::Mode::BIND};
    r->maintenance_placement = context(n::Mode::BIND, 1, 0);
    model::allocation_hook = [] { actor.policy = {1, n::Mode::BIND}; };
    model::check(prepare_current() == 0 && model::reads == 1 && model::slot_frees == 1 && v::anonymous_swap_mappings == nullptr,
                 "anonOFF tracked page uses actual ordinary service");
    model::check(model::blocks.back().physical == model::topology.memory[1].base, "ordinary copied actor BIND confinement");
    close_installed();  // Checks every actual destination byte and sole ref.
    current_closed();

    reset_current();
    r = current_row();
    model::topology.node_count = 0;
    model::check(prepare_current() == -ENODEV && model::allocations == 0 && r->object != nullptr,
                 "tracked page preserves original slot on missing topology");
    purge();
    current_closed();

    reset_current();
    r = current_row();
    r->pagemap = &roots[1];
    model::topology.node_count = 0;
    model::check(prepare_current() == 0 && model::allocations == 0, "unrelated tracked root cannot add no-work topology failure");
    purge();
    current_closed();

    reset_current();
    r = current_row();
    actor.type = s::task::TaskType::DAEMON;
    model::check(prepare_current() == -EWOULDBLOCK && model::allocations == 0, "core rejects non-PROCESS without fabricated context");
    actor.type = s::task::TaskType::PROCESS;
    s::current = nullptr;
    model::check(prepare_current() == -EWOULDBLOCK && model::allocations == 0, "core rejects foreign current Task");
    s::current = &actor;
    model::check(prepare_current(true, 1ULL << 47) == -EFAULT, "core validates user geometry");
    purge();
    current_closed();

    reset_current();
    r = current_row();
    model::fail_gate = true;
    model::check(prepare_current() == -EAGAIN && model::allocations == 0 && actor.usercopy_pagemap_accesses == 0,
                 "nonwaiting reader refusal drops Task lease");
    model::fail_gate = false;
    purge();
    current_closed();

    for (unsigned kind = 0; kind < 3; ++kind) {
        reset_current();
        r = current_row();
        r->pagein_queued = kind == 0;
        r->state = kind == 1   ? v::AnonymousSwapRuntimeState::PAGEIN
                   : kind == 2 ? v::AnonymousSwapRuntimeState::PAGEOUT
                               : v::AnonymousSwapRuntimeState::SWAPPED;
        model::check(prepare_current() == -EAGAIN && model::allocations == 0 && model::reads == 0,
                     "queued/PAGEIN/PAGEOUT never stolen or falsely completed");
        r->pagein_queued = false;
        r->state = v::AnonymousSwapRuntimeState::SWAPPED;
        purge();
        current_closed();
    }

    reset_current();
    r = current_row();
    r->state = v::AnonymousSwapRuntimeState::FAILED;
    r->error = -ENOSPC;
    model::check(prepare_current() == -ENOSPC && r->object != nullptr && model::slot_frees == 0 && model::reads == 0,
                 "stored FAILED status and retained slot are unchanged");
    purge();
    current_closed();

    reset_current();
    r = current_row();
    r->cancelled = true;
    model::check(prepare_current() == -ECANCELED && model::allocations == 0, "cancelled row is not zero materialized");
    purge();
    current_closed();

    for (unsigned kind = 0; kind < 3; ++kind) {
        reset_current();
        r = current_row();
        if (kind == 0) r->original_raw &= ~p::PAGE_USER;
        if (kind == 1) r->original_raw &= ~(p::PAGE_WRITE | p::PAGE_COW);
        if (kind == 2) r->original_raw |= p::PAGE_SHARED;
        v::entries[&roots[0]] = v::pte_from_raw(v::anonymous_swap_marker_raw(r->original_raw));
        model::check(prepare_current() == -EFAULT && model::allocations == 0, "original user/write/private permissions enforced");
        purge();
        current_closed();
    }

    reset_current();
    r = current_row();
    model::missing_leaf = true;  // Explicit absent/unknown-PS walk endpoint.
    model::check(prepare_current() == -EAGAIN && model::allocations == 0, "no ordinary PS descent or missing-marker acceptance");
    model::missing_leaf = false;
    purge();
    current_closed();

    reset_current();
    r = current_row();
    model::fail_alloc = true;
    model::check(prepare_current() == -ENOMEM && r->state == v::AnonymousSwapRuntimeState::SWAPPED && r->object && model::reads == 0 &&
                     model::slot_frees == 0,
                 "actual pre-issue allocator refusal preserves stored owner");
    model::fail_alloc = false;
    purge();
    current_closed();

    reset_current();
    r = current_row();
    model::allocation_hook = [] { ++v::anonymous_swap_mappings->generation; };
    model::check(prepare_current() == -EAGAIN && model::reads == 0 && model::live() == 0 && r->object,
                 "generation race resets candidate without second issue");
    purge();
    current_closed();

    reset_current();
    r = current_row();
    model::allocation_hook = [] { model::serial = 0; };
    model::check(prepare_current() == -ECANCELED && model::reads == 0 && model::live() == 0,
                 "lost root incarnation never installs or dereferences expired snapshot");
    model::serial = 7;
    purge();
    current_closed();

    reset_current();
    r = current_row();
    model::allocation_hook = [] { v::anonymous_swap_service_closed = true; };
    model::check(prepare_current() == -EWOULDBLOCK && model::reads == 0 && model::live() == 0 && r->object,
                 "ordinary final COW frontier refuses shutdown after allocation");
    // Old two-argument bridge still owns its exact compatibility behavior.
    auto const completion = v::service_anonymous_pagein(snapshot(r), context());
    model::check(completion.status == 0 && model::reads == 1, "compatibility bridge unaffected by new ordinary gate");
    v::anonymous_swap_service_closed = false;
    close_installed();
    current_closed();

    reset_current();
    r = current_row();
    model::io_error = -EILSEQ;
    model::check(prepare_current() == -EILSEQ && r->state == v::AnonymousSwapRuntimeState::FAILED && !r->object && model::slot_frees == 1 &&
                     model::live() == 0,
                 "submitted I/O error preserves existing released-slot FAILED branch");
    purge();
    current_closed();
    actor.type = s::task::TaskType::DAEMON;
    actor.syscall_account_start_us = 0;
    s::current = nullptr;
    model::enabled = true;
    std::printf("PASS actual ordinary preparation cases=%u checks=%u\n", model::cases, model::checks);
}
}  // namespace fixture
