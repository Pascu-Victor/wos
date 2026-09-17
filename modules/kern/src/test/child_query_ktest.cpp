#include <memory>
#include <net/wki/child_query.hpp>
#include <test/ktest.hpp>

namespace {
namespace query = ker::net::wki::child_query;
namespace events = ker::syscall::process::child_events;
namespace task = ker::mod::sched::task;
using query::Action;
using query::Operation;
using query::Origin;
using query::Outcome;
using query::Phase;
using query::Relay;

constexpr Origin ROOT{7, 11}, MIDDLE{8, 11}, LEAF{7, 12};
constexpr uint64_t CHILD_PID = 0x9101;
constexpr uint64_t GROUP = 77;

struct Queues {
    std::unique_ptr<task::Task[]> tasks = std::make_unique<task::Task[]>(6);
    bool good = true;
    Queues() {
        for (size_t i = 0; i < 3; ++i) {
            auto& parent = owner(i);
            auto& subject = child(i);
            parent.pid = 1;  // Private reaper, never reparents into actual init.
            subject.pid = CHILD_PID;
            parent.type = subject.type = task::TaskType::PROCESS;
            parent.state.store(task::TaskState::EXITING);
            subject.state.store(task::TaskState::EXITING);
            parent.initialize_job_identity({.pgid = GROUP});
            subject.initialize_job_identity({.pgid = GROUP});
            subject.exit_status = static_cast<int32_t>((37 + i) << 8);
            subject.resources.cpu_user_time_us = i + 1;
            good &= events::begin_publication(parent, subject) && events::commit_publication(subject);
        }
    }
    ~Queues() {
        for (size_t i = 0; i < 3; ++i) events::prepare_process_exit(owner(i));
        for (size_t i = 0; i < 6; ++i) KEXPECT_EQ(tasks[i].ref_count.load(), 1U);
    }
    auto owner(size_t index) -> task::Task& { return tasks[index * 2]; }
    auto child(size_t index) -> task::Task& { return tasks[index * 2 + 1]; }
    auto snapshot(size_t index, events::Selector selector, int32_t options = 0) -> events::QueueInspection {
        Origin const NAMESPACE = index == 0 ? ROOT : (index == 1 ? MIDDLE : LEAF);
        return events::inspect_queue(owner(index), selector, options, {NAMESPACE, ROOT, MIDDLE});
    }
};

auto take(Relay& relay) -> Action {
    auto action = relay.take_action();
    KEXPECT_TRUE(action.has_value());
    return action.value_or(Action{});
}
auto result(const Relay& relay, Operation op) -> Outcome {
    auto outcome = relay.outcome(op);
    KEXPECT_TRUE(outcome.has_value());
    return outcome.value_or(Outcome{.error = -EAGAIN});
}
void close_chain(Relay& root, Relay& middle, Relay& leaf) {
    root.close();
    auto const FIRST = take(root);
    middle.close();
    auto const SECOND = take(middle);
    leaf.close();
    KEXPECT_TRUE(middle.accept(SECOND, result(leaf, Operation::CLOSE)));
    KEXPECT_TRUE(root.accept(FIRST, result(middle, Operation::CLOSE)));
    KEXPECT_TRUE(root.phase() == Phase::CLOSED && middle.phase() == Phase::CLOSED && leaf.phase() == Phase::CLOSED);
}
}  // namespace

KTEST(ChildQuery, MultiHopPidCollisionDoesNotClaimEitherChild) {
    Queues queues;
    KEXPECT_TRUE(queues.good && events::publish_exit(queues.child(0)));
    query::Spec const SPEC{.selector = CHILD_PID, .caller_origin = ROOT};
    Relay root, middle, leaf;
    KEXPECT_EQ(root.begin(101, SPEC, queues.snapshot(0, CHILD_PID), true, 2), 0);
    KEXPECT_EQ(middle.begin(102, SPEC, queues.snapshot(1, CHILD_PID), true, 1), 0);
    KEXPECT_EQ(leaf.begin(103, SPEC, queues.snapshot(2, CHILD_PID), false, 0), 0);
    auto const FIRST = take(root);
    auto const SECOND = take(middle);
    KEXPECT_TRUE(!root.take_action() && !root.outcome(Operation::INSPECT));
    KEXPECT_TRUE(root.retry(FIRST) && take(root) == FIRST);
    auto wrong = FIRST;
    ++wrong.query;
    KEXPECT_TRUE(!root.accept(wrong, result(leaf, Operation::INSPECT)));
    KEXPECT_TRUE(middle.accept(SECOND, result(leaf, Operation::INSPECT)));
    KEXPECT_TRUE(root.accept(FIRST, result(middle, Operation::INSPECT)));
    KEXPECT_TRUE(!root.accept(FIRST, result(middle, Operation::INSPECT)) && !root.retry(FIRST));
    auto const DISCOVERY = result(root, Operation::INSPECT);
    KEXPECT_TRUE(DISCOVERY.error == 0 && DISCOVERY.summary.pids.found && DISCOVERY.summary.pids.ambiguous);
    KEXPECT_EQ(DISCOVERY.summary.ready_depth, 0U);
    KEXPECT_EQ(root.validate(events::validate_queue_version(queues.owner(0), root.version())), 0);
    auto const VALIDATE_FIRST = take(root);
    KEXPECT_EQ(middle.validate(events::validate_queue_version(queues.owner(1), middle.version())), 0);
    auto const VALIDATE_SECOND = take(middle);
    KEXPECT_EQ(leaf.validate(events::validate_queue_version(queues.owner(2), leaf.version())), 0);
    KEXPECT_TRUE(middle.accept(VALIDATE_SECOND, result(leaf, Operation::VALIDATE)));
    KEXPECT_TRUE(root.accept(VALIDATE_FIRST, result(middle, Operation::VALIDATE)));
    KEXPECT_TRUE(result(root, Operation::VALIDATE).summary.pids.ambiguous);
    KEXPECT_TRUE(queues.child(0).child_parent_exit_event.claimed_by == nullptr);
    KEXPECT_EQ(queues.owner(0).child_user_time_us, 0ULL);
    close_chain(root, middle, leaf);
    events::Claim claim;
    events::ClaimedEvent event;
    KEXPECT_TRUE(events::try_claim(queues.owner(0), claim, events::SELECT_ANY, 0, event) == events::ProbeResult::CLAIMED);
    KEXPECT_TRUE(events::commit_claim(queues.owner(0), claim, event, false));
    KEXPECT_EQ(queues.owner(0).child_user_time_us, 1ULL);
    events::cancel_claim(claim);
}

KTEST(ChildQuery, AnyChildSelectsDeepReadyQueueAfterValidation) {
    Queues queues;
    KEXPECT_TRUE(queues.good && events::publish_exit(queues.child(2)));
    query::Spec const SPEC{.selector = events::SELECT_ANY, .caller_origin = ROOT};
    Relay root, middle, leaf;
    KEXPECT_EQ(root.begin(201, SPEC, queues.snapshot(0, events::SELECT_ANY), true, 2), 0);
    KEXPECT_EQ(middle.begin(202, SPEC, queues.snapshot(1, events::SELECT_ANY), true, 1), 0);
    KEXPECT_EQ(leaf.begin(203, SPEC, queues.snapshot(2, events::SELECT_ANY), false, 0), 0);
    auto const FIRST = take(root), SECOND = take(middle);
    KEXPECT_TRUE(middle.accept(SECOND, result(leaf, Operation::INSPECT)));
    KEXPECT_TRUE(root.accept(FIRST, result(middle, Operation::INSPECT)));
    auto const DISCOVERY = result(root, Operation::INSPECT);
    KEXPECT_TRUE(!DISCOVERY.summary.pids.found && DISCOVERY.summary.ready_depth == 2);
    KEXPECT_EQ(root.validate(events::validate_queue_version(queues.owner(0), root.version())), 0);
    auto const VALIDATE_FIRST = take(root);
    KEXPECT_EQ(middle.validate(events::validate_queue_version(queues.owner(1), middle.version())), 0);
    auto const VALIDATE_SECOND = take(middle);
    KEXPECT_EQ(leaf.validate(events::validate_queue_version(queues.owner(2), leaf.version())), 0);
    KEXPECT_TRUE(middle.accept(VALIDATE_SECOND, result(leaf, Operation::VALIDATE)));
    KEXPECT_TRUE(root.accept(VALIDATE_FIRST, result(middle, Operation::VALIDATE)));
    KEXPECT_EQ(result(root, Operation::VALIDATE).summary.ready_depth, 2U);
    events::Claim claim;
    events::ClaimedEvent event;
    auto const PROBE = events::try_claim_if_current(queues.owner(2), claim, events::SELECT_ANY, 0, leaf.version(), event);
    KEXPECT_TRUE(PROBE.version == events::QueueVersionStatus::CURRENT && PROBE.probe == events::ProbeResult::CLAIMED);
    KEXPECT_TRUE(events::commit_claim(queues.owner(2), claim, event, false));
    KEXPECT_EQ(queues.owner(2).child_user_time_us, 3ULL);
    events::cancel_claim(claim);
    close_chain(root, middle, leaf);
}

KTEST(ChildQuery, GroupAliasesResolveGloballyBeforeExactProbe) {
    Queues queues;
    for (size_t i = 0; i < 3; ++i) KEXPECT_TRUE(events::publish_exit(queues.child(i)));
    auto const SELECTOR = events::selector_for_process_group(GROUP);
    query::Spec const SPEC{.selector = SELECTOR, .group_alias = true, .caller_origin = ROOT, .own_group = MIDDLE};
    Relay root, middle, leaf;
    KEXPECT_EQ(root.begin(301, SPEC, queues.snapshot(0, SELECTOR), true, 2), 0);
    KEXPECT_EQ(middle.begin(302, SPEC, queues.snapshot(1, SELECTOR), true, 1), 0);
    KEXPECT_EQ(leaf.begin(303, SPEC, queues.snapshot(2, SELECTOR), false, 0), 0);
    auto const FIRST = take(root), SECOND = take(middle);
    KEXPECT_TRUE(middle.accept(SECOND, result(leaf, Operation::INSPECT)));
    KEXPECT_TRUE(root.accept(FIRST, result(middle, Operation::INSPECT)));
    KEXPECT_TRUE(root.chosen_group() == ROOT && middle.chosen_group() == MIDDLE);
    KEXPECT_EQ(result(root, Operation::INSPECT).summary.ready_depth, query::NO_READY);
    KEXPECT_EQ(root.probe(ROOT, queues.snapshot(0, SELECTOR)), 0);
    auto const PROBE_FIRST = take(root);
    KEXPECT_TRUE(PROBE_FIRST.group == ROOT);
    auto const FOREIGN_SELECTOR = events::selector_for_process_group(GROUP, ROOT);
    KEXPECT_EQ(middle.probe(ROOT, queues.snapshot(1, FOREIGN_SELECTOR)), 0);
    auto const PROBE_SECOND = take(middle);
    KEXPECT_EQ(leaf.probe(ROOT, queues.snapshot(2, FOREIGN_SELECTOR)), 0);
    KEXPECT_TRUE(!result(leaf, Operation::PROBE).summary.potential);
    KEXPECT_TRUE(middle.accept(PROBE_SECOND, result(leaf, Operation::PROBE)));
    KEXPECT_TRUE(root.accept(PROBE_FIRST, result(middle, Operation::PROBE)));
    KEXPECT_EQ(result(root, Operation::PROBE).summary.ready_depth, 0U);
    KEXPECT_EQ(root.probe(MIDDLE, queues.snapshot(0, SELECTOR)), -EINVAL);
    KEXPECT_EQ(root.probe(ROOT, queues.snapshot(0, SELECTOR)), 0);
    KEXPECT_TRUE(!root.take_action());
    // A queue change after probing invalidates the whole validated result.
    events::Claim competing;
    events::ClaimedEvent event;
    KEXPECT_TRUE(events::try_claim(queues.owner(1), competing, CHILD_PID, 0, event) == events::ProbeResult::CLAIMED);
    events::release_claim(competing, event);
    KEXPECT_EQ(root.validate(events::validate_queue_version(queues.owner(0), root.version())), 0);
    auto const VALIDATE = take(root);
    KEXPECT_EQ(middle.validate(events::validate_queue_version(queues.owner(1), middle.version())), -EAGAIN);
    KEXPECT_TRUE(root.accept(VALIDATE, result(middle, Operation::VALIDATE)));
    KEXPECT_EQ(result(root, Operation::VALIDATE).error, -EAGAIN);
    KEXPECT_TRUE(queues.child(0).child_parent_exit_event.claimed_by == nullptr);
    close_chain(root, middle, leaf);
}

KTEST(ChildQuery, CancellationWaitsForCleanupAndRejectsLateReplies) {
    Queues queues;
    query::Spec const SPEC{.selector = events::SELECT_ANY, .caller_origin = ROOT};
    Relay relay;
    KEXPECT_EQ(relay.begin(401, SPEC, queues.snapshot(0, events::SELECT_ANY), true, 1), 0);
    auto const DISCOVER = take(relay);
    relay.close();
    auto const CLOSE = take(relay);
    KEXPECT_TRUE(CLOSE.generation != DISCOVER.generation && CLOSE.operation == Operation::CLOSE);
    KEXPECT_TRUE(!relay.accept(DISCOVER, Outcome{}) && !relay.retry(DISCOVER));
    KEXPECT_TRUE(relay.accept(CLOSE, Outcome{.error = -EAGAIN}));
    KEXPECT_TRUE(relay.phase() == Phase::CLOSING && !relay.outcome(Operation::CLOSE));
    KEXPECT_TRUE(take(relay) == CLOSE);
    relay.close();
    KEXPECT_TRUE(!relay.take_action());
    relay.upstream_lost();
    KEXPECT_TRUE(relay.phase() == Phase::CLOSED && result(relay, Operation::CLOSE).error == 0);
    KEXPECT_TRUE(!relay.accept(CLOSE, Outcome{}));
    KEXPECT_EQ(result(relay, Operation::INSPECT).error, -ECANCELED);
}

KTEST(ChildQuery, InvalidDepthAndLostSessionNeverBecomeNoChild) {
    Queues queues;
    query::Spec const SPEC{.selector = events::SELECT_ANY, .caller_origin = ROOT};
    Relay bounded;
    KEXPECT_EQ(bounded.begin(501, SPEC, queues.snapshot(0, events::SELECT_ANY), true, 0), -ELOOP);
    KEXPECT_TRUE(bounded.phase() == Phase::IDLE && !bounded.take_action());
    KEXPECT_EQ(bounded.begin(501, SPEC, queues.snapshot(0, events::SELECT_ANY), true, 1), 0);
    auto const ACTION = take(bounded);
    Outcome malformed{.summary = {.potential = true, .ready_depth = 1}};
    KEXPECT_TRUE(bounded.accept(ACTION, malformed));
    KEXPECT_EQ(result(bounded, Operation::INSPECT).error, -EPROTO);
    bounded.close();
    bounded.upstream_lost();
    KEXPECT_TRUE(bounded.phase() == Phase::CLOSED);
    Relay lost;
    KEXPECT_EQ(lost.begin(502, SPEC, queues.snapshot(0, events::SELECT_ANY), true, 1), 0);
    auto const OLD = take(lost);
    lost.upstream_lost();
    KEXPECT_EQ(result(lost, Operation::INSPECT).error, -EIO);
    KEXPECT_TRUE(!lost.accept(OLD, Outcome{}));
    lost.close();
    KEXPECT_TRUE(lost.phase() == Phase::CLOSED);
    Relay stale;
    query::Spec const GROUP_SPEC{.selector = events::selector_for_process_group(GROUP), .group_alias = true, .caller_origin = ROOT};
    KEXPECT_EQ(stale.begin(503, GROUP_SPEC, queues.snapshot(0, GROUP_SPEC.selector), false, 0), 0);
    events::update_process_group(queues.child(0), GROUP + 1);
    KEXPECT_EQ(stale.probe(ROOT, queues.snapshot(0, GROUP_SPEC.selector)), -EAGAIN);
    KEXPECT_EQ(result(stale, Operation::PROBE).error, -EAGAIN);
    stale.close();
}
