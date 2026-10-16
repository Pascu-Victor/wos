namespace fixture {
namespace epoch = ker::mod::mm::lifecycle_census;
namespace ce = ker::syscall::process::child_events;
namespace task = ker::mod::sched::task;
namespace virt = ker::mod::mm::virt;
namespace fm = ker::mod::mm::file_mapping;

auto child_stamp() -> epoch::Stamp { return ce::selftest_current_child_epoch.snapshot(); }
auto child_view(task::Task& owner) -> ce::SelftestOwnerLifecycleCensus {
    ce::SelftestOwnerLifecycleCensus out{};
    epoch::Stamp before{};
    ce::selftest_current_owner_lifecycle_census_into(owner, out, before);
    return out;
}
auto edges() -> ce::SelftestLifecycleCensus {
    ce::SelftestLifecycleCensus out{};
    epoch::Stamp before{};
    ce::selftest_current_lifecycle_census_into(out, before);
    return out;
}
auto roots() -> virt::SelftestRootCensus {
    virt::SelftestRootCensus out{};
    epoch::Stamp before{};
    virt::selftest_current_root_census_into(out, before);
    return out;
}
void actor(task::Task& value, uint64_t id) {
    value.pid = id;
    value.job.pgid = id;
    value.names.pgid = id;
}
void expect_child_drained() {
    const auto out = edges();
    check(out.complete && epoch::quiescent(out.stamp), "current child census complete after real tails");
    check(out.child_links == 0 && out.tracee_links == 0 && out.queued_events == 0 && out.wait_registrations == 0 && out.event_claims == 0,
          "all actual child edges returned");
}
void reap(task::Task& owner, task::Task& child) {
    check(ce::publish_exit(child), "actual child exit publication");
    for (;;) {
        ce::ClaimedEvent event{};
        const auto result =
            ce::claim_or_register(owner, owner, ce::SELECT_ANY, ce::OPTION_STOPPED | ce::OPTION_CONTINUED, false, event, ce::View::PUBLIC);
        if (result == ce::ProbeResult::NO_CHILD) break;
        check(result == ce::ProbeResult::CLAIMED, "actual queued child result claimed");
        check(ce::commit_claim(owner, owner.child_claim, event, false), "actual consuming commit");
    }
    check(child.refs.load() == 1 && child.child_parent == nullptr, "real child membership refs retired");
}
void disabled_and_epoch_arithmetic() {
    ++cases;
    task::Task owner;
    actor(owner, 10);
    check(!edges().complete && !child_view(owner).complete && !roots().complete, "disabled current-only readers refuse");
    auto legacy = ce::selftest_lifecycle_census();
    check(legacy.complete && epoch::quiescent(legacy.stamp), "legacy reader still enabled independently");
    epoch::Epoch local;
    epoch::Stamp before{}, after{};
    local.snapshot_into(before);
    check(before == local.snapshot(), "actual into snapshot matches old return snapshot");
    local.begin();
    local.snapshot_into(after);
    check(!epoch::quiescent(after) && after.inflight == 1, "actual inflight invalidates complete view");
    local.end();
    check(!epoch::unchanged(before, local.snapshot()), "actual count-restoring epoch rejects ABA");
    epoch::Epoch underflow;
    underflow.end();
    check(!underflow.snapshot().valid, "actual unmatched return poisons arithmetic");
    epoch::Epoch saturation;
    saturation.version_.store(UINT64_MAX);  // Test-only visibility; no global state reset.
    saturation.begin();
    check(!saturation.snapshot().valid, "actual version saturation fails closed");
    ce::selftest_enable_current_lifecycle_census();
    virt::selftest_enable_current_root_census();
    check(edges().complete && roots().complete, "boot-only enable publishes prepared observers");
}
void empty_poll_stability() {
    ++cases;
    task::Task owner;
    actor(owner, 11);
    const auto before = edges();
    const auto old_before = ce::selftest_child_epoch.snapshot();
    for (unsigned i = 0; i < 1000; ++i) {
        ce::ClaimedEvent event{};
        check(ce::claim_or_register(owner, owner, ce::SELECT_ANY, ce::OPTION_NOHANG, false, event, ce::View::PUBLIC) ==
                  ce::ProbeResult::NO_CHILD,
              "actual empty init-style nonblocking probe");
        ce::cancel_wait(owner);
    }
    const auto after = edges();
    check(before.complete && after.complete && epoch::unchanged(before.stamp, after.stamp),
          "empty public polling preserves current owner stamp");
    check(!epoch::unchanged(old_before, ce::selftest_child_epoch.snapshot()), "same polling retains legacy conservative invalidation");
    check(owner.refs.load() == 1 && !owner.child_wait_registration.registered && owner.child_claim.event == nullptr,
          "empty polling does not transfer child refs");
}
void publication_and_rollback() {
    ++cases;
    task::Task parent, child;
    actor(parent, 12);
    actor(child, 13);
    const auto before = child_stamp();
    check(ce::begin_publication(parent, child), "actual pending child membership published");
    const auto pending = child_view(parent);
    check(pending.complete && pending.children == 1 && pending.publishing_children == 1, "actual publishing state represented");
    check(!epoch::unchanged(before, pending.stamp), "actual private child birth invalidates prior pair");
    const auto publishing = pending.stamp;
    check(ce::commit_publication(child), "actual child becomes live");
    const auto live = child_view(parent);
    check(live.live_children == 1 && live.publishing_children == 0 && !epoch::unchanged(publishing, live.stamp),
          "state transition changes epoch even with equal total links");
    reap(parent, child);
    task::Task rollback;
    actor(rollback, 14);
    check(ce::begin_publication(parent, rollback), "actual rollback setup");
    unsigned returns = 0;
    on_task_release = [&] {
        ++returns;
        check(child_stamp().inflight != 0, "publication rollback return remains inflight");
    };
    ce::abort_publication(rollback);
    on_task_release = {};
    check(returns >= 2 && rollback.refs.load() == 1 && parent.refs.load() == 1, "actual rollback releases exact membership owners");
    expect_child_drained();
}
void claims_and_wnowait() {
    ++cases;
    task::Task parent, child;
    actor(parent, 20);
    actor(child, 21);
    check(ce::begin_publication(parent, child) && ce::commit_publication(child), "actual claim setup");
    check(ce::publish_job_control_stop(child, 19), "actual stop event publication");
    const auto before = edges();
    ce::ClaimedEvent event{};
    check(ce::try_claim(parent, parent.child_claim, child.pid, ce::OPTION_STOPPED, event, ce::View::PUBLIC) == ce::ProbeResult::CLAIMED,
          "actual public claim elected");
    check(edges().event_claims == 1 && !epoch::unchanged(before.stamp, edges().stamp), "real claimant owner invalidates pair");
    unsigned returned = 0;
    on_task_release = [&] {
        ++returned;
        check(child_stamp().inflight != 0, "actual claim return tail remains inflight");
    };
    check(ce::commit_claim(parent, parent.child_claim, event, true), "actual WNOWAIT commit");
    on_task_release = {};
    check(returned != 0 && edges().event_claims == 0 && edges().queued_events == 1, "WNOWAIT clears claim but preserves event");
    check(ce::try_claim(parent, parent.child_claim, child.pid, ce::OPTION_STOPPED, event, ce::View::PUBLIC) == ce::ProbeResult::CLAIMED,
          "preserved event claimable again");
    ce::release_claim(parent.child_claim, event);
    check(edges().event_claims == 0 && edges().queued_events == 1, "actual release claim is retryable");
    reap(parent, child);
    expect_child_drained();
}
void waiter_updates_and_notification() {
    ++cases;
    task::Task parent, child, waiter;
    actor(parent, 30);
    actor(child, 31);
    actor(waiter, 32);
    check(ce::begin_publication(parent, child) && ce::commit_publication(child), "actual wait setup");
    ce::ClaimedEvent absent{};
    check(ce::claim_or_register(parent, waiter, child.pid, 0, true, absent, ce::View::PUBLIC) == ce::ProbeResult::WOULD_BLOCK,
          "actual waiter registration");
    const auto registered = child_view(parent);
    check(registered.waiters == 1 && registered.notified_waiters == 0, "actual pending wait row");
    check(ce::claim_or_register(parent, waiter, child.pid, ce::OPTION_STOPPED, true, absent, ce::View::PUBLIC) ==
              ce::ProbeResult::WOULD_BLOCK,
          "actual in-place registration update");
    check(!epoch::unchanged(registered.stamp, child_view(parent).stamp), "in-place registration update invalidates unchanged pair");
    waiter.acquire_refused = true;
    const auto before = child_view(parent);
    check(ce::publish_job_control_stop(child, 19), "actual notification even if wake ref refused");
    const auto after = child_view(parent);
    check(after.waiters == 1 && after.notified_waiters == 1 && !epoch::unchanged(before.stamp, after.stamp),
          "notification write before failed lifetime attempt is observed");
    waiter.acquire_refused = false;
    unsigned returns = 0;
    on_task_release = [&] {
        ++returns;
        check(!waiter.child_wait_registration.registered && child_stamp().inflight != 0, "unlinked registration returns remain inflight");
    };
    unsigned unlocks = 0;
    on_unlock = [&](const void* lock) {
        if (lock == &ce::g_lifecycle_lock.lock_) {
            ++unlocks;
            check(child_stamp().inflight != 0, "unlinked registration stays inflight through real IRQ unlock");
        }
    };
    ce::cancel_wait(waiter);
    on_task_release = {};
    on_unlock = {};
    check(returns >= 2 && unlocks != 0 && waiter.refs.load() == 1, "exact waiter/owner refs released after unlink");
    reap(parent, child);
    expect_child_drained();
}
void tracee_and_reparent_tails() {
    ++cases;
    task::Task parent, child, tracer, reaper;
    actor(parent, 40);
    actor(child, 41);
    actor(tracer, 42);
    actor(reaper, 1);
    check(ce::begin_publication(parent, child) && ce::commit_publication(child), "actual reparent setup");
    check(ce::attach_tracer(child, tracer), "actual tracee ownership");
    check(ce::publish_ptrace_stop(child, 0x7f), "actual ptrace event");
    check(ce::acknowledge_ptrace_stop(child, tracer), "actual ptrace acknowledgement");
    unsigned returns = 0;
    on_task_release = [&] {
        ++returns;
        check(child_stamp().inflight != 0, "real tracee/reparent wake/ref tail held");
    };
    ce::detach_tracer(child);
    ker::mod::sched::reaper = &reaper;
    const auto before = child_stamp();
    ce::prepare_process_exit(parent);
    on_task_release = {};
    ker::mod::sched::reaper = nullptr;
    check(returns != 0 && parent.child_list_head == nullptr && child.child_parent == &reaper,
          "actual host reparent transfers membership with no leftover owner");
    check(!epoch::unchanged(before, child_stamp()) && child_view(reaper).children == 1,
          "same link count across reparent still invalidates semantic epoch");
    reap(reaper, child);
    check(parent.refs.load() == 1 && reaper.refs.load() == 1 && tracer.refs.load() == 1, "actual reparent/tracer owners conserved");
    expect_child_drained();
}
void root_birth_lookup_rollback_and_aba() {
    ++cases;
    virt::check_physical_epoch = true;
    const auto empty = roots();
    for (unsigned i = 0; i < 1000; ++i) {
        auto* root = virt::create_user_root_common(true);
        check(root != nullptr, "actual user-root factory succeeds");
        check(roots().complete && roots().roots.active_roots == 1 && roots().roots.control_pages == 1,
              "actual root/control-page facts match published ownership");
        const auto before = roots();
        const auto old_before = virt::selftest_root_epoch.snapshot();
        for (unsigned n = 0; n < 4; ++n) {
            uint64_t serial = 0;
            check(virt::capture_user_root_incarnation(root, serial) && serial != 0, "actual pure registered-root serial lookup");
        }
        check(epoch::unchanged(before.stamp, roots().stamp), "root access probes preserve semantic stamp");
        check(!epoch::unchanged(old_before, virt::selftest_root_epoch.snapshot()), "root access still invalidates legacy epoch");
        virt::release_pagemap_exclusive_owned(root, 0, nullptr);
        check(roots().complete && roots().roots.active_roots == 0 && roots().roots.control_pages == 0,
              "actual root final return drains controls before observation");
    }
    check(!epoch::unchanged(empty.stamp, roots().stamp), "full root create/retire ABA never restores epoch");
    virt::SelftestRootFactoryOptions options{};
    virt::SelftestRootFactoryOutput output{};
    options.fail_needed_control_chunk = true;
    check(virt::create_user_root_common(true, &options, &output) == nullptr && output.need_chunk_seen && output.control_failure_injected,
          "real NEED_CHUNK precondition reaches supplied local rollback operand");
    check(roots().complete && roots().roots.active_roots == 0 && virt::active_pages.empty(),
          "actual failed root publication returns private root");
    virt::fail_control_after = 0;
    check(virt::create_user_root_common(true) == nullptr && virt::active_pages.empty(),
          "actual control allocation refusal cleans private root");
    virt::fail_control_after = -1;
    virt::fail_root_after = 0;
    const auto old = roots().stamp;
    check(virt::create_user_root_common(true) == nullptr && !epoch::unchanged(old, roots().stamp),
          "real failed birth intent invalidates pair");
    virt::fail_root_after = -1;
    auto* first = virt::create_user_root_common(true);
    uint64_t first_serial = 0;
    check(virt::capture_user_root_incarnation(first, first_serial), "actual first incarnation");
    virt::release_pagemap_exclusive_owned(first, 0, nullptr);
    auto* second = virt::create_user_root_common(true);
    uint64_t second_serial = 0;
    check(virt::capture_user_root_incarnation(second, second_serial) && first == second && second_serial > first_serial,
          "controlled raw-page reuse exercises actual serial ABA fence");
    virt::release_pagemap_exclusive_owned(second, 0, nullptr);
    check(virt::physical_allocations == virt::physical_returns && virt::active_pages.empty(),
          "all factory/control endpoint pages returned");
}
unsigned binding_returns{};
bool tlb_ack{};
fm::FileMappingBinding* expected_binding{};
void binding_return(void*, fm::LifetimeRetirementTicket&) noexcept {
    check(tlb_ack && virt::current_root_stamp().inflight != 0, "detached index obligation spans actual TLB-ref release");
    ++binding_returns;
    delete std::exchange(expected_binding, nullptr);  // Destructive publication endpoint; no post-call dereference allowed.
}
auto binding(fm::FileMappingLifetime& family, virt::PageTable* root, uint64_t serial, uint64_t start) -> fm::FileMappingBinding* {
    static fm::LifetimeRetirementTicket ticket;
    auto* result = new fm::FileMappingBinding(family);
    result->ticket_ = &ticket;
    check(result->configure_unpublished(root, serial, start, start + 4096, 0), "actual owned binding configuration");
    return result;
}
void index_detach_return_and_aba() {
    ++cases;
    auto* root = virt::create_user_root_common(true);
    uint64_t serial = 0;
    check(virt::capture_user_root_incarnation(root, serial), "actual index root incarnation");
    fm::LifetimeRetirementPort port{nullptr, binding_return};
    fm::FileMappingLifetime family{&port};
    auto* prepared = binding(family, root, serial, 0x4000);
    auto* first = prepared;
    auto flags = virt::user_mapping_pin_lock.lock_irqsave();
    check(virt::file_mapping_index.attach_locked(prepared, serial) && prepared == nullptr, "actual PREPARED-to-INDEX transfer");
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    const auto before = roots();
    check(before.complete && before.indexed_bindings == 1, "actual index fact visible");
    fm::FileMappingTlbRetirement retired;
    flags = virt::user_mapping_pin_lock.lock_irqsave();
    check(virt::file_mapping_index.detach_locked(*first, retired), "actual detach transfers exclusive TLB owner");
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    check(!roots().complete && roots().indexed_bindings == 0 && virt::current_root_stamp().inflight == 1,
          "unlinked TLB owner remains busy before physical return");
    expected_binding = first;
    tlb_ack = true;  // Explicit Host ACK endpoint; no hardware shootdown claim.
    retired.release_after_completed_tlb();
    tlb_ack = false;
    check(retired.empty() && expected_binding == nullptr && roots().complete, "actual destructive release returns semantic inflight owner");
    prepared = binding(family, root, serial, 0x4000);
    first = prepared;
    flags = virt::user_mapping_pin_lock.lock_irqsave();
    check(virt::file_mapping_index.attach_locked(prepared, serial), "actual same-count index republication");
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    check(roots().indexed_bindings == before.indexed_bindings && !epoch::unchanged(before.stamp, roots().stamp),
          "count-restoring index ABA rejects old pair");
    flags = virt::user_mapping_pin_lock.lock_irqsave();
    check(virt::file_mapping_index.detach_root_locked(root, serial, retired), "actual root-wide index detach");
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    check(!roots().complete && virt::current_root_stamp().inflight == 1, "root-wide detached batch stays inflight");
    expected_binding = first;
    tlb_ack = true;
    retired.release_after_completed_tlb();
    tlb_ack = false;
    check(binding_returns == 2 && roots().complete && roots().indexed_bindings == 0, "each detached owner returns exactly once");
    virt::release_pagemap_exclusive_owned(root, 0, nullptr);
}
void all_drained() {
    expect_child_drained();
    check(locks == 0 && irq_depth == 0 && !on_task_release && !on_unlock, "all actual lock/return scopes released");
    check(roots().complete && roots().roots.active_roots == 0 && roots().roots.control_pages == 0 && roots().indexed_bindings == 0,
          "root/index facts exactly drained");
    check(virt::active_pages.empty() && virt::physical_allocations == virt::physical_returns, "raw physical endpoints balanced");
    for (auto* page : virt::free_pages) std::free(page);
    virt::free_pages.clear();
}
}  // namespace fixture

int main() {
    fixture::disabled_and_epoch_arithmetic();
    fixture::empty_poll_stability();
    fixture::publication_and_rollback();
    fixture::claims_and_wnowait();
    fixture::waiter_updates_and_notification();
    fixture::tracee_and_reparent_tails();
    fixture::root_birth_lookup_rollback_and_aba();
    fixture::index_detach_return_and_aba();
    fixture::all_drained();
    std::printf("PASS actual semantic cases=%u checks=%u child_writers=1 root_factory=1 index_return=1 explicit_host_endpoints=1\n",
                fixture::cases, fixture::checks);
}
