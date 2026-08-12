#include <cstdint>
#include <dev/usb/xhci_lifecycle.hpp>
#include <test/ktest.hpp>

namespace {

namespace lifecycle = ker::dev::usb::xhci_lifecycle;

constexpr uint32_t ALL_CHANGES = lifecycle::PORTSC_CHANGE_MASK;
constexpr uint32_t PRESERVED_FIELDS =
    lifecycle::PORTSC_PP | lifecycle::PORTSC_PIC_MASK | lifecycle::PORTSC_WCE | lifecycle::PORTSC_WDE | lifecycle::PORTSC_WOE;

KTEST(XhciLifecyclePortsc, NeutralWritesDoNotTriggerDestructiveFields) {
    constexpr uint32_t SNAPSHOT = lifecycle::PORTSC_CCS | lifecycle::PORTSC_PED | lifecycle::PORTSC_PR | lifecycle::PORTSC_LWS |
                                  lifecycle::PORTSC_WPR | PRESERVED_FIELDS | ALL_CHANGES;

    constexpr uint32_t ACK = lifecycle::compose_portsc_ack_write(SNAPSHOT);
    KEXPECT_EQ(ACK, PRESERVED_FIELDS | ALL_CHANGES);
    KEXPECT_EQ(ACK & (lifecycle::PORTSC_PED | lifecycle::PORTSC_PR | lifecycle::PORTSC_LWS | lifecycle::PORTSC_WPR), 0U);

    constexpr uint32_t RESET = lifecycle::compose_portsc_reset_write(SNAPSHOT);
    KEXPECT_EQ(RESET, PRESERVED_FIELDS | lifecycle::PORTSC_PR);
    KEXPECT_EQ(RESET & (lifecycle::PORTSC_PED | lifecycle::PORTSC_LWS | lifecycle::PORTSC_WPR | ALL_CHANGES), 0U);

    constexpr uint32_t SELECTIVE =
        lifecycle::compose_portsc_write(SNAPSHOT, lifecycle::PORTSC_CSC | lifecycle::PORTSC_PRC | (uint32_t{1} << 24), false);
    KEXPECT_EQ(SELECTIVE, PRESERVED_FIELDS | lifecycle::PORTSC_CSC | lifecycle::PORTSC_PRC);
}

KTEST(XhciLifecyclePort, Usb2ResetCompletionStartsEnumeration) {
    lifecycle::PortChangeBatch changes{};
    changes = lifecycle::coalesce_port_status(changes, lifecycle::PORTSC_CCS | lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB2);

    auto transition = lifecycle::reconcile_port({}, changes);
    KREQUIRE_EQ(transition.port.state, lifecycle::PortState::RESETTING);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::RESET_PORT);
    KEXPECT_EQ(transition.port.generation, 1ULL);

    changes = lifecycle::consume_port_status(changes);
    changes = lifecycle::coalesce_port_status(changes, lifecycle::PORTSC_CCS | lifecycle::PORTSC_PED | lifecycle::PORTSC_PRC,
                                              lifecycle::PortProtocol::USB2);
    transition = lifecycle::reconcile_port(transition.port, changes);

    KEXPECT_EQ(transition.port.state, lifecycle::PortState::ENUMERATING);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::ENUMERATE);
    KEXPECT_EQ(transition.port.active_generation, 1ULL);
    KEXPECT_EQ(transition.port.generation, 1ULL);
}

KTEST(XhciLifecyclePort, Usb3ConnectEnumeratesWithoutReset) {
    auto changes = lifecycle::coalesce_port_status({}, lifecycle::PORTSC_CCS | lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB3);
    const auto transition = lifecycle::reconcile_port({}, changes);

    KEXPECT_EQ(transition.port.state, lifecycle::PortState::ENUMERATING);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::ENUMERATE);
    KEXPECT_EQ(transition.port.active_generation, 1ULL);
}

KTEST(XhciLifecyclePort, RepeatedConnectTransitionIsKasanClean) {
    volatile uint64_t runtime_seed = 1;
    lifecycle::PortLifecycle port{};
    port.protocol = lifecycle::PortProtocol::USB3;
    port.connected = true;

    for (uint64_t generation = runtime_seed; generation < runtime_seed + 32; ++generation) {
        port.generation = generation;
        const auto transition = lifecycle::start_connected_generation(port);
        KREQUIRE_EQ(transition.port.active_generation, generation);
        KREQUIRE_EQ(transition.port.state, lifecycle::PortState::ENUMERATING);
        KREQUIRE_EQ(transition.action, lifecycle::PortAction::ENUMERATE);
    }
}

KTEST(XhciLifecyclePort, DisconnectDuringEnumerationForcesRollback) {
    auto changes = lifecycle::coalesce_port_status({}, lifecycle::PORTSC_CCS | lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB3);
    auto transition = lifecycle::reconcile_port({}, changes);
    KREQUIRE_EQ(transition.port.state, lifecycle::PortState::ENUMERATING);

    changes = lifecycle::consume_port_status(changes);
    changes = lifecycle::coalesce_port_status(changes, lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB3);
    transition = lifecycle::reconcile_port(transition.port, changes);

    KEXPECT_EQ(transition.port.state, lifecycle::PortState::DISCONNECTING);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::ROLLBACK);
    KEXPECT_FALSE(transition.port.connected);
    KEXPECT_EQ(transition.port.generation, 2ULL);
}

KTEST(XhciLifecyclePort, CoalescedReconnectRetiresOldGeneration) {
    auto changes = lifecycle::coalesce_port_status({}, lifecycle::PORTSC_CCS | lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB3);
    auto transition = lifecycle::reconcile_port({}, changes);
    transition = lifecycle::complete_enumeration(transition.port, transition.port.active_generation);
    KREQUIRE_EQ(transition.port.state, lifecycle::PortState::CONFIGURED);

    changes = lifecycle::consume_port_status(changes);
    changes = lifecycle::coalesce_port_status(changes, lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB3);
    changes = lifecycle::coalesce_port_status(changes, lifecycle::PORTSC_CCS | lifecycle::PORTSC_CSC, lifecycle::PortProtocol::USB3);
    transition = lifecycle::reconcile_port(transition.port, changes);

    KREQUIRE_EQ(transition.port.state, lifecycle::PortState::DISCONNECTING);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::ROLLBACK);
    KEXPECT_EQ(transition.port.generation, 3ULL);
    KEXPECT_TRUE(transition.port.reconnect_pending);

    const auto plan = lifecycle::make_rollback_plan(lifecycle::EnumerationStage::PUBLISHED);
    const auto release = lifecycle::decide_rollback_release(plan, lifecycle::DisableSlotProof::SUCCEEDED);
    transition = lifecycle::finish_port_rollback(transition.port, release.disposition);
    KREQUIRE_EQ(transition.port.state, lifecycle::PortState::REUSABLE);

    transition = lifecycle::resume_reusable_port(transition.port);
    KEXPECT_EQ(transition.port.state, lifecycle::PortState::ENUMERATING);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::ENUMERATE);
    KEXPECT_EQ(transition.port.active_generation, 3ULL);
}

struct StageExpectation {
    lifecycle::EnumerationStage stage;
    uint32_t actions;
};

KTEST(XhciLifecycleRollback, EveryFailureStageHasExactReversePlan) {
    constexpr uint32_t CLOSE = lifecycle::rollback_action_mask(lifecycle::RollbackAction::CLOSE_ADMISSION);
    constexpr uint32_t DETACH = lifecycle::rollback_action_mask(lifecycle::RollbackAction::DETACH_DRIVER);
    constexpr uint32_t DRAIN = lifecycle::rollback_action_mask(lifecycle::RollbackAction::DRAIN_REQUESTS);
    constexpr uint32_t STOP = lifecycle::rollback_action_mask(lifecycle::RollbackAction::STOP_ENDPOINTS);
    constexpr uint32_t DROP = lifecycle::rollback_action_mask(lifecycle::RollbackAction::DROP_ENDPOINT_CONTEXTS);
    constexpr uint32_t DISABLE = lifecycle::rollback_action_mask(lifecycle::RollbackAction::DISABLE_SLOT);
    constexpr uint32_t CLEAR = lifecycle::rollback_action_mask(lifecycle::RollbackAction::CLEAR_DCBAA);
    constexpr uint32_t FREE_EPS = lifecycle::rollback_action_mask(lifecycle::RollbackAction::FREE_ENDPOINT_RINGS);
    constexpr uint32_t FREE_EP0 = lifecycle::rollback_action_mask(lifecycle::RollbackAction::FREE_EP0_RING);
    constexpr uint32_t FREE_INPUT = lifecycle::rollback_action_mask(lifecycle::RollbackAction::FREE_INPUT_CONTEXT);
    constexpr uint32_t FREE_DEVICE = lifecycle::rollback_action_mask(lifecycle::RollbackAction::FREE_DEVICE_CONTEXT);

    constexpr StageExpectation EXPECTATIONS[] = {
        {lifecycle::EnumerationStage::NONE, 0},
        {lifecycle::EnumerationStage::SLOT_ENABLED, DISABLE},
        {lifecycle::EnumerationStage::DEVICE_CONTEXT_ALLOCATED, DISABLE | FREE_DEVICE},
        {lifecycle::EnumerationStage::DCBAA_PUBLISHED, DISABLE | CLEAR | FREE_DEVICE},
        {lifecycle::EnumerationStage::INPUT_CONTEXT_ALLOCATED, DISABLE | CLEAR | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::EP0_RING_ALLOCATED, DISABLE | CLEAR | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::ADDRESSED, DISABLE | CLEAR | DRAIN | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::DESCRIPTORS_READ, DISABLE | CLEAR | DRAIN | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::ENDPOINT_RINGS_ALLOCATED, DISABLE | CLEAR | DRAIN | FREE_EPS | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::ENDPOINTS_CONFIGURED,
         DISABLE | CLEAR | DRAIN | STOP | DROP | FREE_EPS | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::USB_CONFIGURED,
         DISABLE | CLEAR | DRAIN | STOP | DROP | FREE_EPS | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::DRIVER_BOUND,
         DETACH | DISABLE | CLEAR | DRAIN | STOP | DROP | FREE_EPS | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
        {lifecycle::EnumerationStage::PUBLISHED,
         CLOSE | DETACH | DISABLE | CLEAR | DRAIN | STOP | DROP | FREE_EPS | FREE_EP0 | FREE_INPUT | FREE_DEVICE},
    };

    for (const auto& expected : EXPECTATIONS) {
        const auto plan = lifecycle::make_rollback_plan(expected.stage);
        KEXPECT_EQ(plan.actions, expected.actions);

        const auto unproven = lifecycle::decide_rollback_release(plan, lifecycle::DisableSlotProof::UNPROVEN);
        if (expected.stage == lifecycle::EnumerationStage::NONE) {
            KEXPECT_EQ(unproven.disposition, lifecycle::RollbackDisposition::REUSABLE);
        } else {
            KEXPECT_EQ(unproven.disposition, lifecycle::RollbackDisposition::QUARANTINED);
        }
        KEXPECT_EQ(unproven.release_actions, 0U);

        const auto proven = lifecycle::decide_rollback_release(plan, lifecycle::DisableSlotProof::SUCCEEDED);
        KEXPECT_EQ(proven.disposition, lifecycle::RollbackDisposition::REUSABLE);
        KEXPECT_EQ(proven.release_actions, expected.actions & (CLEAR | FREE_EPS | FREE_EP0 | FREE_INPUT | FREE_DEVICE));
    }
}

KTEST(XhciLifecycleRollback, DisableMustPrecedeEveryControllerVisibleRelease) {
    const auto plan = lifecycle::make_rollback_plan(lifecycle::EnumerationStage::PUBLISHED);
    auto action = lifecycle::next_rollback_action(plan, lifecycle::RollbackAction::BEGIN);
    KEXPECT_EQ(action, lifecycle::RollbackAction::CLOSE_ADMISSION);
    action = lifecycle::next_rollback_action(plan, action);
    KEXPECT_EQ(action, lifecycle::RollbackAction::DETACH_DRIVER);
    action = lifecycle::next_rollback_action(plan, action);
    KEXPECT_EQ(action, lifecycle::RollbackAction::DRAIN_REQUESTS);
    action = lifecycle::next_rollback_action(plan, action);
    KEXPECT_EQ(action, lifecycle::RollbackAction::STOP_ENDPOINTS);
    action = lifecycle::next_rollback_action(plan, action);
    KEXPECT_EQ(action, lifecycle::RollbackAction::DROP_ENDPOINT_CONTEXTS);
    action = lifecycle::next_rollback_action(plan, action);
    KEXPECT_EQ(action, lifecycle::RollbackAction::DISABLE_SLOT);

    KEXPECT_EQ(lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::UNPROVEN), lifecycle::RollbackAction::QUARANTINE);

    action = lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::SUCCEEDED);
    KEXPECT_EQ(action, lifecycle::RollbackAction::CLEAR_DCBAA);
    action = lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::SUCCEEDED);
    KEXPECT_EQ(action, lifecycle::RollbackAction::FREE_ENDPOINT_RINGS);
    action = lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::SUCCEEDED);
    KEXPECT_EQ(action, lifecycle::RollbackAction::FREE_EP0_RING);
    action = lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::SUCCEEDED);
    KEXPECT_EQ(action, lifecycle::RollbackAction::FREE_INPUT_CONTEXT);
    action = lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::SUCCEEDED);
    KEXPECT_EQ(action, lifecycle::RollbackAction::FREE_DEVICE_CONTEXT);
    action = lifecycle::next_rollback_action(plan, action, lifecycle::DisableSlotProof::SUCCEEDED);
    KEXPECT_EQ(action, lifecycle::RollbackAction::COMPLETE);
}

KTEST(XhciLifecycleRollback, UnprovenDisableQuarantinesPortAndMemory) {
    auto port = lifecycle::PortLifecycle{};
    port.state = lifecycle::PortState::DISCONNECTING;
    port.connected = true;
    port.reconnect_pending = true;

    const auto plan = lifecycle::make_rollback_plan(lifecycle::EnumerationStage::DCBAA_PUBLISHED);
    const auto decision = lifecycle::decide_rollback_release(plan, lifecycle::DisableSlotProof::UNPROVEN);
    KREQUIRE_EQ(decision.disposition, lifecycle::RollbackDisposition::QUARANTINED);
    KEXPECT_EQ(decision.release_actions, 0U);

    const auto transition = lifecycle::finish_port_rollback(port, decision.disposition);
    KEXPECT_EQ(transition.port.state, lifecycle::PortState::FAILED);
    KEXPECT_TRUE(transition.port.quarantined);
    KEXPECT_EQ(transition.action, lifecycle::PortAction::NONE);
}

}  // namespace
