#!/usr/bin/env python3
"""Source invariants for fail-closed WKI resource detach retry."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_DIR = ROOT / "modules" / "kern" / "src" / "net" / "wki"
WKI_CPP = WKI_DIR / "wki.cpp"
WKI_HPP = WKI_DIR / "wki.hpp"
BLOCK_CPP = WKI_DIR / "dev_proxy.cpp"
BLOCK_HPP = WKI_DIR / "dev_proxy.hpp"
VFS_CPP = WKI_DIR / "remote_vfs.cpp"
VFS_HPP = WKI_DIR / "remote_vfs.hpp"
NET_CPP = WKI_DIR / "remote_net.cpp"
NET_HPP = WKI_DIR / "remote_net.hpp"
PEER_CPP = WKI_DIR / "peer.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\s*\([^)]*\)\s*(?:->[^{{]+)?\{{", source, re.DOTALL)
    if match is None:
        fail(f"missing function {name}")
    brace = match.end() - 1
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace : index + 1]
    fail(f"unterminated body for {name}")
    return ""


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        position = source.find(token, cursor)
        if position < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = position + len(token)


def test_deferred_worker_is_public_bounded_and_timer_paced() -> None:
    source = WKI_CPP.read_text()
    header = WKI_HPP.read_text()
    require_tokens(
        header,
        [
            "void wki_deferred_work_notify();",
            "auto wki_peer_remote_boot_epoch_snapshot(uint16_t node_id) -> uint32_t;",
            "auto wki_peer_remote_boot_epoch_invalidated(uint16_t node_id, uint32_t expected_epoch) -> bool;",
        ],
        "public deferred retry support",
    )
    deferred = function_body(source, "process_deferred_blocking_work")
    require_order(
        deferred,
        [
            "wki_dev_proxy_process_pending_detaches()",
            "wki_remote_vfs_process_pending_detaches()",
            "wki_remote_net_process_pending_detaches()",
        ],
        "all detach retries run in task context",
    )
    require_tokens(
        function_body(source, "wki_deferred_work_notify"),
        ["notify_deferred_work()"],
        "public deferred wake wrapper",
    )
    invalidated = function_body(source, "wki_peer_remote_boot_epoch_invalidated")
    require_tokens(
        invalidated,
        ["expected_epoch != 0", "CURRENT_EPOCH != 0", "CURRENT_EPOCH != expected_epoch"],
        "only an exact known boot-epoch change invalidates a reservation",
    )

    workers = [
        (BLOCK_CPP, "wki_dev_proxy_process_pending_detaches", "BLOCK_DETACH_RETRY_BATCH", "BLOCK_DETACH_RETRY_SCAN"),
        (VFS_CPP, "wki_remote_vfs_process_pending_detaches", "VFS_DETACH_RETRY_BATCH", "VFS_DETACH_RETRY_SCAN"),
        (NET_CPP, "wki_remote_net_process_pending_detaches", "NET_DETACH_RETRY_BATCH", "NET_DETACH_RETRY_SCAN"),
    ]
    for path, name, batch, scan in workers:
        body = function_body(path.read_text(), name)
        require_tokens(
            body,
            [f"std::array<", batch, f"scanned < {scan}", "wki_peer_remote_boot_epoch_invalidated"],
            f"bounded rotating worker {name}",
        )
        if "wki_deferred_work_notify" in body:
            fail(f"{name} must let the periodic timer pace failed retries")


def test_block_detach_reservation_survives_teardown() -> None:
    source = BLOCK_CPP.read_text()
    header = BLOCK_HPP.read_text()
    require_tokens(
        header,
        [
            "bool detach_pending = false;",
            "bool detach_retry_in_progress = false;",
            "bool resume_after_detach = false;",
            "bool resume_detach_confirmed = false;",
            "uint32_t binding_peer_boot_epoch = 0;",
            "WkiReliableTxToken detach_tx_token = {};",
            "ProxyBlockState* detach_prev = nullptr;",
            "ProxyBlockState* detach_next = nullptr;",
            "void wki_dev_proxy_process_pending_detaches();",
        ],
        "embedded BLOCK detach reservation",
    )
    stage = function_body(source, "stage_block_detach_locked")
    require_order(
        stage,
        [
            "state->detach_pending = true",
            "state->detach_retry_in_progress = initial_attempt_in_progress",
            "state->detach_attach_cookie = attach_cookie",
            "link_pending_block_detach_locked(state)",
        ],
        "BLOCK stages before sending",
    )
    wrapper = function_body(source, "send_or_defer_block_detach")
    require_order(
        wrapper,
        ["stage_block_detach", "wki_deferred_work_notify"],
        "BLOCK durable staging and task-worker notification",
    )
    if "send_block_detach" in wrapper or "finish_block_detach_attempt" in wrapper:
        fail("BLOCK teardown wrapper must not acquire the tracked-send lifecycle gate")
    erase = function_body(source, "erase_proxy_exact_locked")
    require_tokens(
        erase,
        ["state->ever_published", "state->detach_pending", "state->cleanup_in_progress"],
        "BLOCK pending storage cannot be freed",
    )
    allocator = function_body(source, "allocate_block_attach_cookie_locked")
    require_tokens(
        allocator,
        ["g_pending_detach_head", "state->detach_attach_cookie", "state->detach_incarnation"],
        "BLOCK allocator scans detached tombstones",
    )
    cleanup = function_body(source, "cleanup_failed_block_attach")
    require_order(
        cleanup,
        [
            "s_proxy_lock.lock()",
            "state->cleanup_in_progress = true",
            "state->lock.lock()",
            "stage_block_detach_locked",
            "set_proxy_block_active(state, false)",
            "state->lock.unlock()",
            "s_proxy_lock.unlock()",
            "state->cleanup_in_progress = false",
            "erase_proxy_exact(state)",
            "wki_deferred_work_notify()",
        ],
        "BLOCK cleanup reserves before hiding an unpublished owner binding",
    )
    if "send_or_defer_block_detach" in cleanup:
        fail("BLOCK failed-attach cleanup must not leave a registry-to-staging visibility gap")
    attach = function_body(source, "wki_dev_proxy_attach_block")
    require_tokens(
        attach,
        ["cleanup_failed_block_attach(state, reserved_channel_identity, attach_request_sent)"],
        "BLOCK cancelled post-enqueue attach retires the possible owner binding",
    )
    resume = function_body(source, "wki_dev_proxy_resume_for_peer")
    require_order(
        resume,
        [
            "previous_attach_cookie = p->binding_attach_cookie",
            "p->resume_after_detach = true",
            "send_or_defer_block_detach(p, node_id, p->resource_id, previous_attach_cookie",
            "continue",
            "allocate_block_attach_cookie_locked",
            "p->binding_attach_cookie = attach_cookie",
        ],
        "BLOCK reconnect stages the old exact binding before cookie replacement",
    )
    finish = function_body(source, "finish_block_detach_attempt")
    require_order(
        finish,
        [
            "tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated",
            "if (state->resume_after_detach)",
            "state->resume_detach_confirmed = CAN_RESUME",
            "state->resume_pending = true",
            "PEER->block_resume_pending.store(true, std::memory_order_release)",
            "wki_timer_notify()",
        ],
        "ACKed old-binding detach retriggers BLOCK resume",
    )
    worker = function_body(source, "wki_dev_proxy_process_pending_detaches")
    require_order(
        worker,
        [
            "wki_reliable_tx_status(ATTEMPT.tx_token)",
            "TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED",
            "send_block_detach(",
            "finish_block_detach_attempt",
        ],
        "BLOCK ACK polling and exact retired-generation retry",
    )
    if source.count("send_block_detach(") != 2:
        fail("tracked BLOCK detach send must be limited to builder and task worker")
    attach = function_body(source, "wki_dev_proxy_attach_block")
    require_order(
        attach,
        [
            "s_proxy_lock.lock()",
            "block_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)",
            "return nullptr",
            "allocate_block_attach_cookie_locked",
        ],
        "BLOCK replacement attach is gated behind pending and pre-staging retirement",
    )
    admission_gate = function_body(source, "block_attach_blocked_by_retiring_binding_locked")
    require_tokens(
        admission_gate,
        [
            "g_pending_detach_head",
            "state->detach_owner_node == owner_node",
            "state->cleanup_in_progress",
            "state->epoch_reset_pending",
            "state->resume_pending",
            "state->resume_in_progress",
            "state->resume_after_detach",
            "bool const ACTIVE = proxy_block_active(state)",
            "!ACTIVE",
            "ACTIVE && proxy_block_fenced(state)",
        ],
        "BLOCK attach gate covers in-progress publication, reset/fenced, and claimed retirement intervals",
    )
    ordinary_detach = function_body(source, "wki_dev_proxy_detach_block")
    require_order(
        ordinary_detach,
        [
            "s_proxy_lock.lock()",
            "state->cleanup_in_progress = true",
            "wait_for_block_io_quiescence(state)",
            "s_proxy_lock.lock()",
            "stage_block_detach_locked",
            "set_proxy_block_active(state, false)",
            "unindex_proxy_locked(state)",
            "s_proxy_lock.unlock()",
            "state->cleanup_in_progress = false",
            "wki_deferred_work_notify()",
        ],
        "ordinary BLOCK detach reserves before becoming unroutable and notifies after unlock",
    )
    if "send_or_defer_block_detach" in ordinary_detach:
        fail("ordinary BLOCK detach must stage inside the registry retirement transition")
    reactivate = function_body(source, "wki_dev_proxy_reactivate_resource_observation")
    require_tokens(
        reactivate,
        [
            "s_proxy_lock.lock()",
            "proxy_block_active(state)",
            "proxy_block_fenced(state)",
            "state->resource_generation != resource_generation",
            "block_detach_incarnation_equal(state->binding_incarnation, owner_incarnation)",
            "state->resume_pending = true",
        ],
        "exact BLOCK discovery reactivation",
    )


def test_vfs_detach_reservation_owns_a_lifecycle_ref() -> None:
    source = VFS_CPP.read_text()
    header = VFS_HPP.read_text()
    require_tokens(
        header,
        [
            "bool detach_pending = false;",
            "uint32_t binding_peer_boot_epoch = 0;",
            "WkiReliableTxToken detach_tx_token = {};",
            "ProxyVfsState* detach_prev = nullptr;",
            "ProxyVfsState* detach_next = nullptr;",
            "void wki_remote_vfs_process_pending_detaches();",
            "auto wki_remote_vfs_detach_pending_for_resource(uint16_t owner_node, uint32_t resource_id) -> bool;",
        ],
        "embedded VFS detach reservation",
    )
    stage = function_body(source, "stage_vfs_detach_locked")
    require_order(
        stage,
        ["state->detach_pending = true", "state->lifecycle_refs++", "link_pending_vfs_detach_locked(state)"],
        "VFS pending entry pins proxy storage",
    )
    finish = function_body(source, "finish_vfs_detach_attempt")
    require_order(
        finish,
        [
            "tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated",
            "unlink_pending_vfs_detach_locked(state)",
            "release_pending_ref = true",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "VFS ACK or epoch proof releases only the pending reference",
    )
    discard = function_body(source, "discard_unpublished_proxy")
    require_order(
        discard,
        [
            "s_vfs_lock.lock()",
            "if (state->epoch_reset_pending)",
            "stage_vfs_detach_locked",
            "if (state->detach_pending)",
            "state->epoch_reset_pending = false",
            "state->destroy_when_idle = true",
            "state->mount_released = true",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "epoch-marked unpublished VFS teardown reserves before releasing marker ownership",
    )
    allocator = function_body(source, "allocate_vfs_attach_cookie_locked")
    require_tokens(
        allocator,
        ["g_pending_vfs_detach_head", "proxy->detach_attach_cookie", "proxy->detach_incarnation"],
        "VFS allocator scans pending detach tuples",
    )
    mount = function_body(source, "wki_remote_vfs_mount")
    require_order(
        mount,
        [
            "s_vfs_lock.lock()",
            "vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)",
            "return -EAGAIN",
            "g_vfs_proxies.push_back",
            "if (!state->epoch_reset_pending)",
            "state->active = true",
        ],
        "VFS replacement and in-progress publication cannot cross an epoch marker",
    )
    admission = function_body(source, "vfs_attach_blocked_by_retiring_binding_locked")
    require_tokens(
        admission,
        [
            "vfs_detach_pending_for_resource_locked(owner_node, resource_id)",
            "proxy->owner_node == owner_node",
            "proxy->resource_id == resource_id",
            "proxy->epoch_reset_pending",
        ],
        "VFS admission includes an unstaged epoch marker",
    )
    public_gate = function_body(source, "wki_remote_vfs_detach_pending_for_resource")
    require_order(
        public_gate,
        ["s_vfs_lock.lock()", "vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)", "s_vfs_lock.unlock()"],
        "VFS deferred admission reads pending tuple and epoch-marker ownership under its registry lock",
    )
    idle = function_body(source, "proxy_is_idle_for_resource_release_locked")
    require_tokens(idle, ["!state->active", "!state->epoch_reset_pending", "state->lifecycle_refs == 0"], "VFS marker pins resources and storage")
    wait_failure = mount[mount.find("if (WAIT_RC != 0)") : mount.find("if (ATTACH_STATUS")]
    require_tokens(
        wait_failure,
        ["send_or_defer_vfs_detach(state", "discard_unpublished_proxy(state)"],
        "every VFS post-enqueue wait failure retains and retires its tuple",
    )
    wrapper = function_body(source, "send_or_defer_vfs_detach")
    require_order(wrapper, ["stage_vfs_detach", "wki_deferred_work_notify"], "VFS durable staging and task-worker notification")
    if "send_vfs_detach" in wrapper or "finish_vfs_detach_attempt" in wrapper:
        fail("VFS teardown wrapper must not acquire the tracked-send lifecycle gate")
    worker = function_body(source, "wki_remote_vfs_process_pending_detaches")
    require_order(
        worker,
        [
            "wki_reliable_tx_status(ATTEMPT.tx_token)",
            "TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED",
            "send_vfs_detach(",
            "finish_vfs_detach_attempt",
        ],
        "VFS ACK polling and exact retired-generation retry",
    )
    if source.count("send_vfs_detach(") != 2:
        fail("tracked VFS detach send must be limited to builder and task worker")

    failed_attached = function_body(source, "discard_failed_attached_proxy")
    require_order(
        failed_attached,
        [
            "s_vfs_lock.lock()",
            "state->active || state->epoch_reset_pending",
            "stage_vfs_detach_locked",
            "state->epoch_reset_pending = false",
            "state->active = false",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
        ],
        "failed attached VFS rollback reserves before becoming inactive and notifies after unlock",
    )
    if "send_or_defer_vfs_detach" in failed_attached:
        fail("failed attached VFS rollback must stage inside the registry transition")

    for claim_name in ["claim_vfs_proxy_unmount_by_path", "claim_vfs_proxy_unmount_by_generation"]:
        claim = function_body(source, claim_name)
        require_order(
            claim,
            [
                "s_vfs_lock.lock()",
                "state->active || state->epoch_reset_pending",
                "deactivate_vfs_proxy_locked",
                "stage_vfs_detach_locked",
                "state->epoch_reset_pending = false",
                "s_vfs_lock.unlock()",
            ],
            f"{claim_name} consumes marker ownership only after exact staging",
        )
    require_tokens(
        function_body(source, "find_vfs_proxy_by_mount"),
        ["p->active || p->epoch_reset_pending", "p->mount_configured"],
        "path-based VFS unmount can claim an epoch-marked mount",
    )
    unmount_finish = function_body(source, "finish_vfs_proxy_unmount")
    require_tokens(unmount_finish, ["teardown.detach_staged", "wki_deferred_work_notify()"], "VFS unmount post-lock notification")
    if "send_or_defer_vfs_detach" in unmount_finish:
        fail("normal VFS unmount must not stage after releasing the registry lock")

    marker = function_body(source, "wki_remote_vfs_mark_epoch_reset")
    require_order(
        marker,
        ["s_vfs_lock.lock()", "state->active = false", "state->epoch_reset_pending = true", "s_vfs_lock.unlock()"],
        "VFS RX marker publishes fixed admission ownership",
    )
    for forbidden in ["stage_vfs_detach_locked", "wki_deferred_work_notify", "std::make_unique", "new (std::nothrow)", "push_back"]:
        if forbidden in marker:
            fail(f"VFS RX marker must remain allocation/send-free: found {forbidden}")

    cleanup = function_body(source, "wki_remote_vfs_cleanup_for_peer")
    require_order(
        cleanup,
        [
            "deactivate_vfs_proxy_locked(p, cleanup, false)",
            "if (!owner_reboot_proven)",
            "stage_vfs_detach_locked",
            "if (owner_reboot_proven || p->detach_pending)",
            "p->epoch_reset_pending = false",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
        ],
        "task cleanup consumes marker by exact staging or concrete reboot proof",
    )


def test_net_detach_reservation_is_fail_closed() -> None:
    source = NET_CPP.read_text()
    header = NET_HPP.read_text()
    require_tokens(
        header,
        [
            "bool cleanup_complete = false;",
            "bool detach_pending = false;",
            "uint32_t binding_peer_boot_epoch = 0;",
            "WkiReliableTxToken detach_tx_token = {};",
            "ProxyNetState* detach_prev = nullptr;",
            "ProxyNetState* detach_next = nullptr;",
            "void wki_remote_net_process_pending_detaches();",
            "auto wki_remote_net_detach_pending_for_resource(uint16_t owner_node, uint32_t resource_id) -> bool;",
        ],
        "embedded NET detach reservation",
    )
    stage = function_body(source, "stage_net_detach_locked")
    require_order(
        stage,
        ["state->detach_pending = true", "state->detach_peer_boot_epoch = state->binding_peer_boot_epoch", "link_pending_net_detach_locked"],
        "NET stages its exact legacy tuple",
    )
    if "wki_peer_remote_boot_epoch_snapshot" in stage:
        fail("NET staging must not invert peer_lock against an attach lifecycle lease")
    storage = function_body(source, "erase_unpublished_net_proxy_storage_locked")
    require_tokens(
        storage,
        ["state->ever_published", "state->detach_pending", "!state->cleanup_complete"],
        "NET pending teardown retains unpublished storage",
    )
    allocator = function_body(source, "allocate_net_attach_cookie_locked")
    require_tokens(
        allocator,
        ["g_pending_net_detach_head", "proxy->detach_attach_cookie"],
        "NET allocator scans pending legacy tuples",
    )
    attach = function_body(source, "wki_remote_net_attach")
    require_order(
        attach,
        ["s_net_proxy_lock.lock()", "net_detach_pending_for_resource_locked(owner_node, resource_id)", "return nullptr", "find_net_proxy_by_resource"],
        "NET replacement attach is gated behind pending detach ACK",
    )
    public_gate = function_body(source, "wki_remote_net_detach_pending_for_resource")
    require_order(
        public_gate,
        ["s_net_proxy_lock.lock()", "net_detach_pending_for_resource_locked(owner_node, resource_id)", "s_net_proxy_lock.unlock()"],
        "NET deferred admission reads the detach gate under its registry lock",
    )
    wait_failure = attach[attach.find("if (WAIT_RC != 0)") : attach.find("uint8_t const ATTACH_STATUS")]
    require_order(
        wait_failure,
        ["send_or_defer_net_detach(state", "if (WAIT_RC == WKI_ERR_TIMEOUT)", "retire_unregistered_net_proxy"],
        "every NET post-enqueue wait failure stages detach before teardown",
    )
    unpublish = function_body(source, "unpublish_proxy_netdev")
    require_order(
        unpublish,
        ["route_del_for_dev", "netdev_unregister", "netif_del_for_dev"],
        "NET retirement closes stale-route interface recreation window",
    )
    wrapper = function_body(source, "send_or_defer_net_detach")
    require_order(wrapper, ["stage_net_detach", "wki_deferred_work_notify"], "NET durable staging and task-worker notification")
    if "send_net_detach" in wrapper or "finish_net_detach_attempt" in wrapper:
        fail("NET teardown wrapper must not acquire the tracked-send lifecycle gate")
    finish = function_body(source, "finish_net_detach_attempt")
    require_tokens(
        finish,
        ["tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated", "erase_unpublished_net_proxy_storage_locked(state)"],
        "NET storage retirement requires ACK or epoch proof",
    )
    worker = function_body(source, "wki_remote_net_process_pending_detaches")
    require_order(
        worker,
        [
            "wki_reliable_tx_status(ATTEMPT.tx_token)",
            "TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED",
            "send_net_detach(",
            "finish_net_detach_attempt",
        ],
        "NET ACK polling and exact retired-generation retry",
    )
    if source.count("send_net_detach(") != 2:
        fail("tracked NET detach send must be limited to builder and task worker")

    for detach_name in ["wki_remote_net_detach", "wki_remote_net_detach_resource_generation"]:
        detach = function_body(source, detach_name)
        require_order(
            detach,
            [
                "s_net_proxy_lock.lock()",
                "stage_net_detach_locked",
                "s_net_proxy_lock.unlock()",
                "wait_for_net_proxy_refs_to_drain(state)",
                "wki_deferred_work_notify()",
            ],
            f"{detach_name} reserves before registry unlock and notifies after teardown locks",
        )
        if "send_or_defer_net_detach" in detach:
            fail(f"{detach_name} must stage inside its registry retirement transition")


def test_peer_cleanup_stages_unproven_disconnect_detaches() -> None:
    block = BLOCK_CPP.read_text()
    net = NET_CPP.read_text()
    vfs = VFS_CPP.read_text()
    peer = PEER_CPP.read_text()

    block_cleanup = function_body(block, "wki_dev_proxy_cleanup_epoch_reset_for_peer")
    require_order(
        block_cleanup,
        [
            "if (!owner_reboot_proven)",
            "stage_block_detach_locked",
            "state->binding_attach_cookie = 0",
            "state->binding_incarnation = {}",
            "state->cleanup_in_progress = false",
            "wki_deferred_work_notify()",
        ],
        "BLOCK hard cleanup reserves before discarding an unproven binding",
    )
    require_tokens(
        function_body(block, "wki_dev_proxy_fence_timeout_tick"),
        ["wki_dev_proxy_cleanup_epoch_reset_for_peer(owner_node, true, false)"],
        "BLOCK fence timeout retires with detach rather than pretending reboot",
    )

    net_cleanup = function_body(net, "cleanup_net_proxies_for_peer")
    require_order(
        net_cleanup,
        [
            "if (!owner_reboot_proven)",
            "stage_net_detach_locked",
            "s_net_proxy_lock.unlock()",
            "erase_net_proxy(entry.state)",
            "wki_deferred_work_notify()",
        ],
        "NET peer cleanup reserves before registry removal",
    )

    vfs_cleanup = function_body(vfs, "wki_remote_vfs_cleanup_for_peer")
    require_order(
        vfs_cleanup,
        [
            "deactivate_vfs_proxy_locked(p, cleanup, false)",
            "if (!owner_reboot_proven)",
            "stage_vfs_detach_locked",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
            "release_vfs_proxy_lifecycle_ref(cleanup.state)",
        ],
        "VFS peer cleanup pins and reserves before teardown release",
    )

    proof = function_body(peer, "peer_remote_boot_epoch_change_is_proven_locked")
    require_tokens(
        proof,
        ["peer->remote_boot_epoch != 0", "remote_epoch != 0", "peer->remote_boot_epoch != remote_epoch"],
        "only two distinct known boot epochs prove reboot",
    )
    drain = function_body(peer, "drain_pending_epoch_reset_cleanups")
    require_tokens(
        drain,
        [
            "INVALIDATE_DISCOVERY || !CONNECTED, OWNER_REBOOT_PROVEN",
            "wki_remote_net_cleanup_epoch_reset_for_peer(peer.node_id, OWNER_REBOOT_PROVEN)",
            "wki_remote_vfs_cleanup_for_peer(peer.node_id, OWNER_REBOOT_PROVEN)",
        ],
        "peer cleanup separates local retirement from reboot proof",
    )
    if "OWNER_BOOT_CHANGED || !CONNECTED" in drain:
        fail("disconnect must not be promoted to owner reboot proof")


def test_detach_completion_is_ack_or_epoch_qualified() -> None:
    cases = [
        (BLOCK_CPP, "finish_block_detach_attempt", "wki_dev_proxy_process_pending_detaches", "state->detach_tx_token"),
        (VFS_CPP, "finish_vfs_detach_attempt", "wki_remote_vfs_process_pending_detaches", "state->detach_tx_token"),
        (NET_CPP, "finish_net_detach_attempt", "wki_remote_net_process_pending_detaches", "state->detach_tx_token"),
    ]
    for path, finish_name, worker_name, token_slot in cases:
        source = path.read_text()
        finish = function_body(source, finish_name)
        require_order(
            finish,
            [
                "tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated",
                "unlink_pending_",
                "tx_status == WkiReliableTxStatus::PENDING",
                f"{token_slot} = send_result == WKI_OK ? replacement_token : WkiReliableTxToken{{}}",
            ],
            f"{finish_name} keeps enqueue distinct from acknowledgement",
        )
        if "send_result == WKI_OK || peer_epoch_invalidated" in finish:
            fail(f"{finish_name} must not retire a tuple merely because tracked send enqueued it")

        worker = function_body(source, worker_name)
        require_order(
            worker,
            [
                "wki_reliable_tx_status(ATTEMPT.tx_token)",
                "TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED",
                "send_",
            ],
            f"{worker_name} resends only after exact channel retirement",
        )


def main() -> None:
    test_deferred_worker_is_public_bounded_and_timer_paced()
    test_block_detach_reservation_survives_teardown()
    test_vfs_detach_reservation_owns_a_lifecycle_ref()
    test_net_detach_reservation_is_fail_closed()
    test_peer_cleanup_stages_unproven_disconnect_detaches()
    test_detach_completion_is_ack_or_epoch_qualified()
    print("WKI detach retry source invariants hold")


if __name__ == "__main__":
    main()
