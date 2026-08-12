#pragma once

#include <cstdint>

namespace ker::dev::usb::xhci_lifecycle {

// PORTSC fields used by the lifecycle model. These are kept local so the model
// remains independent of the xHCI MMIO/controller implementation.
constexpr uint32_t PORTSC_CCS = uint32_t{1} << 0;
constexpr uint32_t PORTSC_PED = uint32_t{1} << 1;
constexpr uint32_t PORTSC_PR = uint32_t{1} << 4;
constexpr uint32_t PORTSC_PP = uint32_t{1} << 9;
constexpr uint32_t PORTSC_PIC_MASK = uint32_t{3} << 14;
constexpr uint32_t PORTSC_LWS = uint32_t{1} << 16;
constexpr uint32_t PORTSC_CSC = uint32_t{1} << 17;
constexpr uint32_t PORTSC_PEC = uint32_t{1} << 18;
constexpr uint32_t PORTSC_WRC = uint32_t{1} << 19;
constexpr uint32_t PORTSC_OCC = uint32_t{1} << 20;
constexpr uint32_t PORTSC_PRC = uint32_t{1} << 21;
constexpr uint32_t PORTSC_PLC = uint32_t{1} << 22;
constexpr uint32_t PORTSC_CEC = uint32_t{1} << 23;
constexpr uint32_t PORTSC_WCE = uint32_t{1} << 25;
constexpr uint32_t PORTSC_WDE = uint32_t{1} << 26;
constexpr uint32_t PORTSC_WOE = uint32_t{1} << 27;
constexpr uint32_t PORTSC_WPR = uint32_t{1} << 31;

constexpr uint32_t PORTSC_CHANGE_MASK = PORTSC_CSC | PORTSC_PEC | PORTSC_WRC | PORTSC_OCC | PORTSC_PRC | PORTSC_PLC | PORTSC_CEC;
constexpr uint32_t PORTSC_PRESERVED_RW_MASK = PORTSC_PP | PORTSC_PIC_MASK | PORTSC_WCE | PORTSC_WDE | PORTSC_WOE;

// PORTSC has a mixture of ordinary RW fields, RW1CS fields, and write strobes.
// Compose writes from known-safe fields instead of writing a raw register
// snapshot back. In particular, PED, LWS, and WPR must stay zero here.
[[nodiscard]] constexpr auto compose_portsc_write(uint32_t snapshot, uint32_t observed_changes_to_ack, bool request_reset) -> uint32_t {
    uint32_t value = snapshot & PORTSC_PRESERVED_RW_MASK;
    value |= observed_changes_to_ack & snapshot & PORTSC_CHANGE_MASK;
    if (request_reset) {
        value |= PORTSC_PR;
    }
    return value;
}

[[nodiscard]] constexpr auto compose_portsc_ack_write(uint32_t snapshot) -> uint32_t {
    return compose_portsc_write(snapshot, snapshot & PORTSC_CHANGE_MASK, false);
}

// Reset is normally requested after the IRQ path has acknowledged and queued
// its change snapshot. It therefore preserves ordinary RW fields but does not
// acknowledge any newly observed change bit.
[[nodiscard]] constexpr auto compose_portsc_reset_write(uint32_t snapshot) -> uint32_t { return compose_portsc_write(snapshot, 0, true); }

enum class PortState : uint8_t {
    DISCONNECTED,
    RESETTING,
    ENUMERATING,
    CONFIGURED,
    DISCONNECTING,
    FAILED,
    REUSABLE,
};

enum class PortProtocol : uint8_t {
    USB2,
    USB3,
};

enum class PortAction : uint8_t {
    NONE,
    RESET_PORT,
    ENUMERATE,
    ROLLBACK,
};

struct PortChangeBatch {
    // sequence advances for every queued snapshot; generation advances for
    // every observed CSC. Both counters must be preserved when a batch is
    // consumed so duplicate/stale work can be rejected.
    uint64_t sequence{0};
    uint64_t generation{0};
    uint32_t observed_changes{0};
    uint32_t latest_portsc{0};
    PortProtocol protocol{PortProtocol::USB2};
    bool pending{false};
};

struct PortLifecycle {
    PortState state{PortState::DISCONNECTED};
    uint64_t generation{0};
    uint64_t active_generation{0};
    uint64_t last_sequence{0};
    bool connected{false};
    bool enabled{false};
    bool reconnect_pending{false};
    bool quarantined{false};
    PortProtocol protocol{PortProtocol::USB2};
};

struct PortTransition {
    PortLifecycle port{};
    PortAction action{PortAction::NONE};
    bool ignored_stale_snapshot{false};
};

[[nodiscard]] constexpr auto next_counter(uint64_t value) -> uint64_t {
    ++value;
    return value == 0 ? 1 : value;
}

// Counter ordering is valid while a producer cannot get 2^63 observations
// ahead of its consumer, which is vastly beyond a port worker's queue bound.
[[nodiscard]] constexpr auto counter_is_newer(uint64_t candidate, uint64_t reference) -> bool {
    const uint64_t delta = candidate - reference;
    return delta != 0 && delta < (uint64_t{1} << 63);
}

// The caller serializes this value (an IRQ-safe lock or an equivalent atomic
// publication scheme). Multiple snapshots may be folded before task-context
// reconciliation: change bits are ORed, the final level is retained, and each
// CSC advances the physical connection generation.
[[nodiscard]] constexpr auto coalesce_port_status(PortChangeBatch batch, uint32_t snapshot, PortProtocol protocol) -> PortChangeBatch {
    batch.sequence = next_counter(batch.sequence);
    if ((snapshot & PORTSC_CSC) != 0) {
        batch.generation = next_counter(batch.generation);
    }
    batch.observed_changes |= snapshot & PORTSC_CHANGE_MASK;
    batch.latest_portsc = snapshot;
    batch.protocol = protocol;
    batch.pending = true;
    return batch;
}

[[nodiscard]] constexpr auto consume_port_status(PortChangeBatch batch) -> PortChangeBatch {
    batch.observed_changes = 0;
    batch.pending = false;
    return batch;
}

[[nodiscard]] constexpr auto port_connected(uint32_t portsc) -> bool { return (portsc & PORTSC_CCS) != 0; }

[[nodiscard]] constexpr auto port_enabled(uint32_t portsc) -> bool { return (portsc & PORTSC_PED) != 0; }

[[nodiscard]] constexpr auto start_connected_generation(const PortLifecycle& current) -> PortTransition {
    PortTransition result{current, PortAction::NONE, false};
    PortLifecycle& port = result.port;
    port.active_generation = port.generation;
    port.reconnect_pending = false;
    port.quarantined = false;

    if (port.protocol == PortProtocol::USB3 || port.enabled) {
        port.state = PortState::ENUMERATING;
        result.action = PortAction::ENUMERATE;
        return result;
    }

    port.state = PortState::RESETTING;
    result.action = PortAction::RESET_PORT;
    return result;
}

// Reconcile only decides lifecycle work. The IRQ side may call
// coalesce_port_status(), but command submission, allocation, and driver work
// must execute later in a serialized task-context worker.
[[nodiscard]] constexpr auto reconcile_port(PortLifecycle current, const PortChangeBatch& batch) -> PortTransition {
    PortTransition result{current, PortAction::NONE, false};
    if (!batch.pending) {
        return result;
    }
    if (!counter_is_newer(batch.sequence, current.last_sequence)) {
        result.ignored_stale_snapshot = true;
        return result;
    }

    PortLifecycle& port = result.port;
    port.last_sequence = batch.sequence;
    port.generation = batch.generation;
    port.connected = port_connected(batch.latest_portsc);
    port.enabled = port_enabled(batch.latest_portsc);
    port.protocol = batch.protocol;

    const bool connection_changed = (batch.observed_changes & PORTSC_CSC) != 0;
    const bool reset_completed = (batch.observed_changes & PORTSC_PRC) != 0;

    switch (port.state) {
        case PortState::DISCONNECTED:
            if (port.connected) {
                return start_connected_generation(port);
            }
            return result;

        case PortState::RESETTING:
            if (connection_changed) {
                if (!port.connected) {
                    port.state = PortState::DISCONNECTED;
                    port.active_generation = 0;
                    return result;
                }
                return start_connected_generation(port);
            }
            if (!port.connected) {
                port.state = PortState::DISCONNECTED;
                port.active_generation = 0;
                return result;
            }
            if (port.protocol == PortProtocol::USB3) {
                return start_connected_generation(port);
            }
            if (reset_completed) {
                if (port.enabled) {
                    port.state = PortState::ENUMERATING;
                    port.active_generation = port.generation;
                    result.action = PortAction::ENUMERATE;
                } else {
                    port.state = PortState::FAILED;
                    port.reconnect_pending = false;
                    result.action = PortAction::ROLLBACK;
                }
            }
            return result;

        case PortState::ENUMERATING:
        case PortState::CONFIGURED:
            // Any CSC invalidates the active device generation. A batch may
            // contain disconnect+reconnect and end with CCS=1; teardown is
            // still mandatory before the new generation is enumerated.
            if (connection_changed || !port.connected || port.generation != port.active_generation) {
                port.state = PortState::DISCONNECTING;
                port.reconnect_pending = port.connected;
                result.action = PortAction::ROLLBACK;
            }
            return result;

        case PortState::DISCONNECTING:
            if (connection_changed) {
                port.reconnect_pending = port.connected;
            }
            return result;

        case PortState::FAILED:
            if (connection_changed) {
                port.reconnect_pending = port.connected;
            }
            return result;

        case PortState::REUSABLE:
            if (!port.connected) {
                port.state = PortState::DISCONNECTED;
                port.active_generation = 0;
                port.reconnect_pending = false;
                return result;
            }
            return start_connected_generation(port);
    }

    return result;
}

[[nodiscard]] constexpr auto complete_enumeration(PortLifecycle port, uint64_t generation) -> PortTransition {
    PortTransition result{port, PortAction::NONE, false};
    if (port.state != PortState::ENUMERATING) {
        return result;
    }
    if (!port.connected || generation != port.active_generation || generation != port.generation) {
        result.port.state = PortState::DISCONNECTING;
        result.port.reconnect_pending = port.connected;
        result.action = PortAction::ROLLBACK;
        return result;
    }
    result.port.state = PortState::CONFIGURED;
    return result;
}

[[nodiscard]] constexpr auto fail_enumeration(PortLifecycle port, uint64_t generation) -> PortTransition {
    PortTransition result{port, PortAction::NONE, false};
    if (port.state != PortState::ENUMERATING || generation != port.active_generation) {
        return result;
    }
    result.port.state = PortState::FAILED;
    result.port.reconnect_pending = false;
    result.action = PortAction::ROLLBACK;
    return result;
}

// Stage is the last successfully completed publication/ownership step. A
// failed next step rolls back from the current stage.
enum class EnumerationStage : uint8_t {
    NONE,
    SLOT_ENABLED,
    DEVICE_CONTEXT_ALLOCATED,
    DCBAA_PUBLISHED,
    INPUT_CONTEXT_ALLOCATED,
    EP0_RING_ALLOCATED,
    ADDRESSED,
    DESCRIPTORS_READ,
    ENDPOINT_RINGS_ALLOCATED,
    ENDPOINTS_CONFIGURED,
    USB_CONFIGURED,
    DRIVER_BOUND,
    PUBLISHED,
    COUNT,
};

enum class RollbackAction : uint8_t {
    BEGIN,
    CLOSE_ADMISSION,
    DETACH_DRIVER,
    DRAIN_REQUESTS,
    STOP_ENDPOINTS,
    DROP_ENDPOINT_CONTEXTS,
    DISABLE_SLOT,
    CLEAR_DCBAA,
    FREE_ENDPOINT_RINGS,
    FREE_EP0_RING,
    FREE_INPUT_CONTEXT,
    FREE_DEVICE_CONTEXT,
    COMPLETE,
    QUARANTINE,
};

enum class DisableSlotProof : uint8_t {
    NOT_REQUIRED,
    SUCCEEDED,
    UNPROVEN,
};

enum class RollbackDisposition : uint8_t {
    REUSABLE,
    QUARANTINED,
};

[[nodiscard]] constexpr auto rollback_action_mask(RollbackAction action) -> uint32_t { return uint32_t{1} << static_cast<uint8_t>(action); }

struct RollbackPlan {
    uint32_t actions{0};

    [[nodiscard]] constexpr auto contains(RollbackAction action) const -> bool { return (actions & rollback_action_mask(action)) != 0; }
};

struct RollbackDecision {
    RollbackDisposition disposition{RollbackDisposition::REUSABLE};
    // Release actions are returned only after Disable Slot success proves that
    // the controller no longer owns per-slot contexts or rings.
    uint32_t release_actions{0};
};

[[nodiscard]] constexpr auto stage_at_least(EnumerationStage stage, EnumerationStage threshold) -> bool {
    return static_cast<uint8_t>(stage) >= static_cast<uint8_t>(threshold);
}

[[nodiscard]] constexpr auto make_rollback_plan(EnumerationStage stage) -> RollbackPlan {
    RollbackPlan plan{};
    if (stage_at_least(stage, EnumerationStage::PUBLISHED)) {
        plan.actions |= rollback_action_mask(RollbackAction::CLOSE_ADMISSION);
    }
    if (stage_at_least(stage, EnumerationStage::DRIVER_BOUND)) {
        plan.actions |= rollback_action_mask(RollbackAction::DETACH_DRIVER);
    }
    if (stage_at_least(stage, EnumerationStage::ADDRESSED)) {
        plan.actions |= rollback_action_mask(RollbackAction::DRAIN_REQUESTS);
    }
    if (stage_at_least(stage, EnumerationStage::ENDPOINTS_CONFIGURED)) {
        plan.actions |= rollback_action_mask(RollbackAction::STOP_ENDPOINTS);
        plan.actions |= rollback_action_mask(RollbackAction::DROP_ENDPOINT_CONTEXTS);
    }
    if (stage_at_least(stage, EnumerationStage::SLOT_ENABLED)) {
        plan.actions |= rollback_action_mask(RollbackAction::DISABLE_SLOT);
    }
    if (stage_at_least(stage, EnumerationStage::DCBAA_PUBLISHED)) {
        plan.actions |= rollback_action_mask(RollbackAction::CLEAR_DCBAA);
    }
    if (stage_at_least(stage, EnumerationStage::ENDPOINT_RINGS_ALLOCATED)) {
        plan.actions |= rollback_action_mask(RollbackAction::FREE_ENDPOINT_RINGS);
    }
    if (stage_at_least(stage, EnumerationStage::EP0_RING_ALLOCATED)) {
        plan.actions |= rollback_action_mask(RollbackAction::FREE_EP0_RING);
    }
    if (stage_at_least(stage, EnumerationStage::INPUT_CONTEXT_ALLOCATED)) {
        plan.actions |= rollback_action_mask(RollbackAction::FREE_INPUT_CONTEXT);
    }
    if (stage_at_least(stage, EnumerationStage::DEVICE_CONTEXT_ALLOCATED)) {
        plan.actions |= rollback_action_mask(RollbackAction::FREE_DEVICE_CONTEXT);
    }
    return plan;
}

// Actions are ordered so controller-visible resources are relinquished before
// their backing memory. Passing UNPROVEN after DISABLE_SLOT selects quarantine
// and never exposes a clear/free action.
[[nodiscard]] constexpr auto next_rollback_action(const RollbackPlan& plan, RollbackAction completed,
                                                  DisableSlotProof disable_slot_proof = DisableSlotProof::UNPROVEN) -> RollbackAction {
    if (completed == RollbackAction::COMPLETE || completed == RollbackAction::QUARANTINE) {
        return completed;
    }
    if (completed == RollbackAction::DISABLE_SLOT && plan.contains(RollbackAction::DISABLE_SLOT) &&
        disable_slot_proof != DisableSlotProof::SUCCEEDED) {
        return RollbackAction::QUARANTINE;
    }

    const uint8_t first = static_cast<uint8_t>(completed) + 1;
    const uint8_t end = static_cast<uint8_t>(RollbackAction::COMPLETE);
    for (uint8_t candidate = first; candidate < end; ++candidate) {
        const auto action = static_cast<RollbackAction>(candidate);
        if (!plan.contains(action)) {
            continue;
        }
        if (candidate > static_cast<uint8_t>(RollbackAction::DISABLE_SLOT) && plan.contains(RollbackAction::DISABLE_SLOT) &&
            disable_slot_proof != DisableSlotProof::SUCCEEDED) {
            return RollbackAction::QUARANTINE;
        }
        return action;
    }
    return RollbackAction::COMPLETE;
}

[[nodiscard]] constexpr auto decide_rollback_release(const RollbackPlan& plan, DisableSlotProof disable_slot_proof) -> RollbackDecision {
    if (plan.contains(RollbackAction::DISABLE_SLOT) && disable_slot_proof != DisableSlotProof::SUCCEEDED) {
        return {RollbackDisposition::QUARANTINED, 0};
    }

    constexpr uint32_t RELEASE_MASK =
        rollback_action_mask(RollbackAction::CLEAR_DCBAA) | rollback_action_mask(RollbackAction::FREE_ENDPOINT_RINGS) |
        rollback_action_mask(RollbackAction::FREE_EP0_RING) | rollback_action_mask(RollbackAction::FREE_INPUT_CONTEXT) |
        rollback_action_mask(RollbackAction::FREE_DEVICE_CONTEXT);
    return {RollbackDisposition::REUSABLE, plan.actions & RELEASE_MASK};
}

[[nodiscard]] constexpr auto finish_port_rollback(PortLifecycle port, RollbackDisposition disposition) -> PortTransition {
    PortTransition result{port, PortAction::NONE, false};
    result.port.active_generation = 0;
    if (disposition == RollbackDisposition::QUARANTINED) {
        result.port.state = PortState::FAILED;
        result.port.quarantined = true;
        return result;
    }
    result.port.state = PortState::REUSABLE;
    result.port.quarantined = false;
    return result;
}

// A clean record is automatically reused only for an observed newer physical
// generation. Enumeration failures on the same still-connected generation do
// not spin in an unbounded retry loop.
[[nodiscard]] constexpr auto resume_reusable_port(PortLifecycle port) -> PortTransition {
    PortTransition result{port, PortAction::NONE, false};
    if (port.state != PortState::REUSABLE) {
        return result;
    }
    if (!port.connected) {
        result.port.state = PortState::DISCONNECTED;
        result.port.reconnect_pending = false;
        return result;
    }
    if (port.reconnect_pending) {
        return start_connected_generation(port);
    }
    return result;
}

}  // namespace ker::dev::usb::xhci_lifecycle
