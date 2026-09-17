#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <syscalls_impl/process/child_events.hpp>

namespace ker::net::wki::child_query {

namespace events = ker::syscall::process::child_events;
using Origin = ker::mod::sched::session::Origin;
constexpr uint16_t NO_READY = UINT16_MAX;
constexpr uint16_t MAX_HOPS = 64;

enum class Operation : uint8_t { INSPECT, PROBE, VALIDATE, CLOSE };
enum class Phase : uint8_t { IDLE, INSPECTING, INSPECTED, PROBING, PROBED, VALIDATING, VALIDATED, FAILED, CLOSING, CLOSED };

struct Spec {
    events::Selector selector{};
    bool group_alias{};
    Origin caller_origin{};
    std::optional<Origin> own_group{};
    auto operator==(const Spec&) const -> bool = default;
};
struct Summary {
    events::PidCandidates pids{};
    ker::mod::sched::session::GroupCandidates groups{};
    bool potential{};
    uint16_t ready_depth{NO_READY};
};
struct Outcome {
    int error{};
    Summary summary{};
};
// Local record identity, not a wire capability. The adapter must bind this to
// its exact authenticated upstream session, channel generation and request key.
struct Action {
    uint64_t query{};
    uint8_t generation{};
    Operation operation{Operation::INSPECT};
    Origin group{};
    auto operator==(const Action&) const -> bool = default;
};

[[nodiscard]] constexpr auto physical(Origin origin) -> bool { return origin.node != 0 && origin.boot != 0; }

[[nodiscard]] constexpr auto valid_spec(const Spec& spec) -> bool {
    bool const GROUP = spec.selector.value != events::SELECT_ANY && (spec.selector.value & events::SELECT_PROCESS_GROUP) != 0;
    if (spec.selector.value == 0 || !physical(spec.caller_origin) || (spec.own_group && !physical(*spec.own_group)) ||
        (GROUP && events::selector_for_process_group(spec.selector.value & events::SELECT_PROCESS_GROUP_MASK).value == 0) ||
        (spec.group_alias && (!GROUP || !spec.selector.group_origin.local())) ||
        (!spec.selector.group_origin.local() && !physical(spec.selector.group_origin)))
        return false;
    return true;
}

// Transport-independent continuation. The adapter serializes each instance,
// retains the exact owner/session, and owns its bounded storage through CLOSED.
// No Task/event/channel or syscall-stack pointer is retained here. Every method
// is bounded, nonblocking and allocation-free; none sends or waits on a peer.
class Relay {
   public:
    Relay() = default;
    Relay(const Relay&) = delete;
    auto operator=(const Relay&) -> Relay& = delete;
    Relay(Relay&&) = delete;
    auto operator=(Relay&&) -> Relay& = delete;

    [[nodiscard]] auto begin(uint64_t query, Spec spec, const events::QueueInspection& local, bool upstream, uint16_t remaining_hops)
        -> int {
        if (phase_ != Phase::IDLE) return -EBUSY;
        if (query == 0 || !valid_spec(spec) || remaining_hops > MAX_HOPS) return -EINVAL;
        if (upstream && remaining_hops == 0) return -ELOOP;
        if (local.version.queue == 0 || local.version.groups == 0) return -EINVAL;
        if (local.version.queue == UINT64_MAX || local.version.groups == UINT64_MAX) return -EOVERFLOW;
        query_ = query;
        spec_ = spec;
        version_ = local.version;
        upstream_ = upstream;
        remaining_hops_ = remaining_hops;
        combined_ = summarize(local, false);
        if (upstream_)
            schedule(Operation::INSPECT, Phase::INSPECTING);
        else
            complete(Operation::INSPECT);
        return 0;
    }

    [[nodiscard]] auto pending_action() const -> std::optional<Action> { return pending_; }
    [[nodiscard]] auto phase() const -> Phase { return phase_; }
    [[nodiscard]] auto version() const -> events::QueueVersion { return version_; }
    [[nodiscard]] auto validated_selector() const -> std::optional<events::Selector> {
        if (phase_ != Phase::VALIDATED) return std::nullopt;
        auto selector = spec_.selector;
        if (spec_.group_alias) {
            if (!selected_group_) return std::nullopt;
            selector.group_origin = *selected_group_;
        }
        return selector;
    }

    // Resolve once at the initiating node after the entire INSPECT chain
    // completes. A subtree must accept the initiating node's chosen physical
    // origin even when that origin has no members in the subtree.
    [[nodiscard]] auto chosen_group() const -> std::optional<Origin> {
        auto const DISCOVERY = outcome(Operation::INSPECT);
        if (!spec_.group_alias || !DISCOVERY || DISCOVERY->error != 0) return std::nullopt;
        auto selected = DISCOVERY->summary.groups.resolve(spec_.own_group);
        if (selected && selected->local()) selected = spec_.caller_origin;
        return selected;
    }

    [[nodiscard]] auto probe(Origin selected, const events::QueueInspection& local) -> int {
        if (!spec_.group_alias || !physical(selected)) return -EINVAL;
        if (selected_group_) return *selected_group_ == selected ? terminal_error() : -EINVAL;
        if (phase_ != Phase::INSPECTED) return phase_error();
        if (int const ERROR = check_version(local.version); ERROR != 0) return fail(ERROR);
        selected_group_ = selected;
        combined_ = summarize(local, true);
        if (upstream_)
            schedule(Operation::PROBE, Phase::PROBING);
        else
            complete(Operation::PROBE);
        return 0;
    }

    // The adapter performs local lifecycle validation immediately before this
    // transition. Upstream validation starts only afterwards, so a completed
    // chain shares an interval after the final collect and before validation.
    [[nodiscard]] auto validate(events::QueueVersionStatus status) -> int {
        if (phase_ == Phase::VALIDATING || phase_ == Phase::VALIDATED) return 0;
        if (phase_ != Phase::INSPECTED && phase_ != Phase::PROBED) return phase_error();
        if (spec_.group_alias && phase_ == Phase::INSPECTED) return -EAGAIN;
        if (status != events::QueueVersionStatus::CURRENT)
            return fail(status == events::QueueVersionStatus::EXHAUSTED ? -EOVERFLOW : -EAGAIN);
        if (upstream_)
            schedule(Operation::VALIDATE, Phase::VALIDATING);
        else
            complete(Operation::VALIDATE);
        return 0;
    }

    // Reserve before dropping adapter serialization. A response may race the
    // send completion. Transient failure reissues exactly this operation;
    // completion of a superseded send cannot resurrect it after cancellation.
    [[nodiscard]] auto take_action() -> std::optional<Action> {
        if (!pending_ || in_flight_) return std::nullopt;
        in_flight_ = true;
        issued_ = true;
        return pending_;
    }
    [[nodiscard]] auto retry(const Action& action) -> bool {
        if (!pending_ || *pending_ != action || !in_flight_) return false;
        in_flight_ = false;
        return true;
    }
    [[nodiscard]] auto accept(const Action& action, const Outcome& upstream) -> bool {
        if (!pending_ || *pending_ != action || !issued_) return false;
        if (action.operation == Operation::CLOSE) {
            // Cleanup failure is not permission to reuse the outgoing lane.
            if (upstream.error != 0) {
                in_flight_ = false;
                return true;
            }
            pending_.reset();
            complete(Operation::CLOSE);
            return true;
        }
        pending_.reset();
        if (upstream.error != 0) {
            static_cast<void>(fail(upstream.error < 0 ? upstream.error : -EPROTO));
            return true;
        }
        if (action.operation == Operation::INSPECT || action.operation == Operation::PROBE) {
            if (!valid_summary(upstream.summary, action.operation)) {
                static_cast<void>(fail(-EPROTO));
                return true;
            }
            combined_.pids.merge(upstream.summary.pids);
            combined_.groups.merge(upstream.summary.groups);
            combined_.potential |= upstream.summary.potential;
            if (combined_.ready_depth == NO_READY && upstream.summary.ready_depth != NO_READY)
                combined_.ready_depth = static_cast<uint16_t>(upstream.summary.ready_depth + 1);
        }
        complete(action.operation);
        return true;
    }

    // Keep the upstream record/lane owned until CLOSE is acknowledged, or the
    // exact upstream session becomes terminal. Late earlier replies are ignored.
    void close() {
        if (phase_ == Phase::CLOSING || phase_ == Phase::CLOSED) return;
        pending_.reset();
        if (upstream_)
            schedule(Operation::CLOSE, Phase::CLOSING);
        else
            complete(Operation::CLOSE);
    }
    void upstream_lost() {
        upstream_ = false;
        pending_.reset();
        if (phase_ == Phase::CLOSING)
            complete(Operation::CLOSE);
        else if (phase_ != Phase::CLOSED && phase_ != Phase::IDLE)
            static_cast<void>(fail(-EIO));
    }

    [[nodiscard]] auto outcome(Operation operation) const -> std::optional<Outcome> {
        if (operation > Operation::CLOSE) return std::nullopt;
        if (operation != Operation::CLOSE && (phase_ == Phase::CLOSING || phase_ == Phase::CLOSED)) return Outcome{.error = -ECANCELED};
        if (operation != Operation::CLOSE && phase_ == Phase::FAILED) return Outcome{.error = error_};
        return outcomes_.at(static_cast<size_t>(operation));
    }

   private:
    [[nodiscard]] auto summarize(const events::QueueInspection& local, bool probing) const -> Summary {
        Summary result{};
        if (spec_.group_alias && !probing) {
            result.groups = local.groups;
            result.potential = local.groups.flags != 0;
        } else {
            if (!probing) result.pids = local.pids;
            result.potential = local.potential;
            result.ready_depth = local.ready ? 0 : NO_READY;
        }
        return result;
    }
    [[nodiscard]] auto valid_summary(const Summary& summary, Operation operation) const -> bool {
        using Groups = ker::mod::sched::session::GroupCandidates;
        constexpr uint8_t FLAGS = Groups::LOCAL | Groups::OWN | Groups::OTHER | Groups::AMBIGUOUS;
        if ((summary.pids.ambiguous && !summary.pids.found) || (summary.pids.found && !physical(summary.pids.origin)) ||
            (summary.groups.flags & ~FLAGS) != 0 ||
            ((summary.groups.flags & Groups::AMBIGUOUS) != 0 && (summary.groups.flags & Groups::OTHER) == 0) ||
            ((summary.groups.flags & Groups::OTHER) != 0 && !physical(summary.groups.other)) ||
            ((summary.groups.flags & Groups::OWN) != 0 && !spec_.own_group) ||
            (summary.ready_depth != NO_READY && (!summary.potential || summary.ready_depth >= remaining_hops_)))
            return false;
        bool const ALIAS_DISCOVERY = spec_.group_alias && operation == Operation::INSPECT;
        bool const PID = spec_.selector.value > 0 && spec_.selector.value < events::SELECT_PROCESS_GROUP;
        if (PID && operation == Operation::INSPECT && summary.pids.found != summary.potential) return false;
        if ((!PID || operation == Operation::PROBE) && summary.pids.found) return false;
        if (!ALIAS_DISCOVERY && summary.groups.flags != 0) return false;
        return !ALIAS_DISCOVERY || (summary.ready_depth == NO_READY && summary.potential == (summary.groups.flags != 0));
    }
    [[nodiscard]] auto check_version(events::QueueVersion current) const -> int {
        if (current.queue == UINT64_MAX || current.groups == UINT64_MAX) return -EOVERFLOW;
        return current == version_ ? 0 : -EAGAIN;
    }
    [[nodiscard]] auto terminal_error() const -> int {
        if (phase_ == Phase::FAILED) return error_;
        return phase_ == Phase::CLOSING || phase_ == Phase::CLOSED ? -ECANCELED : 0;
    }
    [[nodiscard]] auto phase_error() const -> int { return terminal_error() != 0 ? terminal_error() : -EAGAIN; }
    auto fail(int error) -> int {
        error_ = error;
        phase_ = Phase::FAILED;
        pending_.reset();
        return error;
    }
    void schedule(Operation operation, Phase phase) {
        // One INSPECT, optional PROBE, one VALIDATE and one CLOSE: at most four
        // generations per instance. The adapter never reuses query identity.
        ++generation_;
        pending_ = Action{query_, generation_, operation, selected_group_.value_or(Origin{})};
        in_flight_ = issued_ = false;
        phase_ = phase;
    }
    void complete(Operation operation) {
        pending_.reset();
        in_flight_ = issued_ = false;
        outcomes_.at(static_cast<size_t>(operation)) = Outcome{.summary = operation == Operation::CLOSE ? Summary{} : combined_};
        switch (operation) {
            case Operation::INSPECT:
                phase_ = Phase::INSPECTED;
                break;
            case Operation::PROBE:
                phase_ = Phase::PROBED;
                break;
            case Operation::VALIDATE:
                phase_ = Phase::VALIDATED;
                break;
            case Operation::CLOSE:
                phase_ = Phase::CLOSED;
                break;
        }
    }

    Phase phase_{Phase::IDLE};
    uint64_t query_{};
    uint8_t generation_{};
    Spec spec_{};
    events::QueueVersion version_{};
    uint16_t remaining_hops_{};
    bool upstream_{};
    bool in_flight_{};
    bool issued_{};
    int error_{};
    std::optional<Origin> selected_group_{};
    std::optional<Action> pending_{};
    Summary combined_{};
    std::array<std::optional<Outcome>, 4> outcomes_{};
};

}  // namespace ker::net::wki::child_query
