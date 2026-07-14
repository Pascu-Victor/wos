#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
REMOTABLE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remotable.cpp"
DEVFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "devfs.cpp"
DEVFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "devfs.hpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, before: str, after: str, context: str) -> None:
    before_pos = source.rfind(before, 0, source.find(after))
    after_pos = source.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_cleanup_publishes_only_when_it_claims_waiter() -> None:
    cleanup_body = function_body(WKI_CPP.read_text(), "wki_wait_cleanup_for_task")
    require_tokens(
        cleanup_body,
        [
            "uint8_t expected = WkiWaitEntry::PENDING",
            "cur->state.compare_exchange_strong(expected, WkiWaitEntry::CLAIMED",
            "std::memory_order_acq_rel",
            "std::memory_order_acquire",
            "cur->task.store(nullptr, std::memory_order_release)",
            "publish_claimed_wait_entry(cur, WKI_ERR_PEER_FENCED)",
            "else if (expected == WkiWaitEntry::CLAIMED)",
            "claimed_waiter = cur",
            "s_wait_lock.unlock()",
            "wki_quiesce_claimed_op(claimed_waiter)",
        ],
        "WKI task cleanup claim/publish and in-flight claimant quiescence",
    )
    if "cur->state.store(WkiWaitEntry::DONE" in cleanup_body:
        fail("wki_wait_cleanup_for_task must not force DONE after losing the waiter claim")
    if "cur->result =" in cleanup_body:
        fail("wki_wait_cleanup_for_task must publish results only through publish_claimed_wait_entry")

    require_order(
        cleanup_body,
        "s_wait_lock.unlock()",
        "wki_remote_compute_cleanup_for_task(task)",
        "remote-compute cleanup must run after releasing the generic wait lock",
    )
    require_order(
        cleanup_body,
        "wki_remote_compute_cleanup_for_task(task)",
        "wki_remote_vfs_cleanup_for_task(task->pid)",
        "task-owned WKI cleanup ordering",
    )

    quiescence_point = function_body(WKI_CPP.read_text(), "wki_wait_quiescence_point")
    require_tokens(
        quiescence_point,
        [
            "CURRENT_TASK->type != mod::sched::task::TaskType::IDLE",
            "TaskState::ACTIVE",
            "mod::sched::preempt_count() == 0",
            "DAEMON_SHUTDOWN_MUST_ENTER_EXIT",
            "CURRENT_TASK->exit_in_progress",
            "CURRENT_TASK->kernel_shutdown_requested.load(std::memory_order_acquire)",
            "mod::sched::kern_yield()",
            'asm volatile("pause" ::: "memory")',
        ],
        "preemptible process claimant quiescence",
    )
    require_tokens(
        function_body(SCHEDULER_CPP.read_text(), "maybe_exit_current_kernel_thread_for_shutdown"),
        ["task->exit_in_progress", "task->kernel_shutdown_requested.load(std::memory_order_acquire)"],
        "daemon exit quiescence must not recursively re-enter shutdown exit",
    )


def test_done_is_the_final_stack_waiter_access() -> None:
    finish_body = function_body(WKI_CPP.read_text(), "wki_finish_claimed_op")
    task_load = finish_body.find("entry->task.load(std::memory_order_acquire)")
    task_retain = finish_body.find("waiter_task->try_acquire()")
    done_publish = finish_body.find("publish_claimed_wait_entry(entry, result)")
    task_wake = finish_body.find("mod::sched::kern_wake(waiter_task)")
    task_release = finish_body.find("waiter_task->release()")
    if min(task_load, task_retain, done_publish, task_wake, task_release) < 0:
        fail("wki_finish_claimed_op must retain and release its scheduler task around DONE")
    if not task_load < task_retain < done_publish < task_wake < task_release:
        fail("wki_finish_claimed_op must pin task, publish DONE, then wake and release")
    if "entry->" in finish_body[done_publish + len("publish_claimed_wait_entry(entry, result)") :]:
        fail("wki_finish_claimed_op must not dereference a stack waiter after publishing DONE")


def test_ktest_covers_completed_claimed_cleanup() -> None:
    source = WKI_WAIT_KTEST.read_text()
    require_tokens(
        source,
        [
            "TaskCleanupPreservesCompletedClaimedWaiter",
            "wki_claim_op(&wait)",
            "wki_finish_claimed_op(&wait, 77)",
            "wki_wait_cleanup_for_task(&task)",
            "KEXPECT_NULL(wait.task.load(std::memory_order_acquire))",
            "WkiWaitEntry::DONE",
        ],
        "WKI wait cleanup completed-claim KTEST",
    )


def test_resource_withdraw_defers_blocking_unmount() -> None:
    source = REMOTABLE_CPP.read_text()
    wki_source = WKI_CPP.read_text()
    withdraw_body = function_body(source, "handle_resource_withdraw")
    require_tokens(
        withdraw_body,
        ["resource->valid = false", "wki_timer_notify()"],
        "resource withdrawal must publish a fixed-size task-context tombstone",
    )
    if "wki_remote_vfs_unmount(" in withdraw_body:
        fail("resource withdrawal must not unmount remote VFS directly in resource RX handling")
    if "g_pending_vfs_unmount" in source or "queue_vfs_unmount" in source:
        fail("resource withdrawal must not allocate a path-only queue entry on the RX/IRQ stack")

    require_tokens(
        source,
        [
            "REMOTABLE_RX_QUEUE_CAPACITY = WKI_CREDITS_CONTROL",
            "s_resource_rx_lock.lock_irqsave()",
            "WkiRemotableRxAdmission::RETRY",
            "advert.node_id != hdr->src_node",
            "ker::mod::sys::Mutex s_remotable_lock",
            "wki_peer_lifecycle_acquire(peer)",
            "resource_rx_channel_token_matches(pending)",
        ],
        "resource control must cross a bounded authenticated task-context ingress",
    )
    require_tokens(
        wki_source,
        [
            "message_uses_rx_peer_lifecycle(msg, payload, PAYLOAD_LEN)",
            "peer_lifecycle.try_acquire(hdr->src_node)",
            "wki_remotable_admit_rx(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation)",
            "message_uses_rx_peer_lifecycle(RO_MSG, ch->reorder_head->data, ch->reorder_head->len)",
            "wki_remotable_admit_rx(RO_MSG, &ch->reorder_head->hdr",
            "wki_remotable_process_pending_rx()",
            "wki_deferred_work_notify()",
        ],
        "reliable resource RX must admit before ACK in both ordering paths",
    )
    lifecycle_body = function_body(wki_source, "message_uses_rx_peer_lifecycle")
    require_tokens(
        lifecycle_body,
        ["message_uses_compute_lifecycle(type)", "message_uses_deferred_remotable_rx(type)"],
        "resource controls and inline compute must share the receive-time peer lifecycle gate",
    )
    wki_rx_body = function_body(wki_source, "wki_rx")
    require_order(
        wki_rx_body,
        "message_uses_rx_peer_lifecycle(msg, payload, PAYLOAD_LEN)",
        "bool const RELIABLE_RX_ACCEPTED = reliable_rx_peer_accepts(hdr->src_node)",
        "resource peer lifecycle must cover the reliable state gate and channel lookup",
    )
    require_order(
        wki_rx_body,
        "message_uses_rx_peer_lifecycle(RO_MSG, ch->reorder_head->data, ch->reorder_head->len)",
        "ro_remotable_admission = wki_remotable_admit_rx(",
        "reordered resource lifecycle admission must precede queue admission",
    )

    deferred_body = function_body(source, "wki_remotable_process_pending_mounts")
    require_order(
        deferred_body,
        "std::erase_if(g_discovered",
        "wki_remote_vfs_unmount_resource_generation(withdrawn.node_id, withdrawn.resource_id, withdrawn.generation)",
        "deferred worker must retire the tombstone before generation-bound teardown",
    )
    require_order(
        deferred_body,
        "sync_vfs_devfs_resource(withdrawn.node_id, withdrawn.resource_id, withdrawn.generation)",
        "wki_remote_vfs_unmount_resource_generation(withdrawn.node_id, withdrawn.resource_id, withdrawn.generation)",
        "withdrawal must hide the retired devfs generation before blocking mount-reference drain",
    )
    require_tokens(
        deferred_body,
        [
            "pending.resource_generation == withdrawn.generation",
            "wki_remote_vfs_mount(pending.node_id, pending.resource_id, pending.mount_path.data(), pending.resource_generation)",
        ],
        "mount and withdrawal work must preserve the observed resource generation",
    )

    sync_body = function_body(source, "sync_vfs_devfs_resource")
    require_tokens(
        sync_body,
        [
            "devfs_wki_remove_resource(node_id, static_cast<uint16_t>(ResourceType::VFS), resource_id,",
            "retired_generation",
            "snapshot.generation, snapshot.flags",
            "wki_resource_generation_snapshot(node_id, ResourceType::VFS, resource_id) == snapshot_generation",
        ],
        "devfs convergence must fence stale removal and publish exact live generations",
    )

    devfs_source = DEVFS_CPP.read_text()
    require_tokens(
        DEVFS_HPP.read_text(),
        ["uint32_t wki_open_refs = 0;", "bool wki_unlinked = false;"],
        "opened WKI devfs nodes need unlink-safe lifetime pins",
    )
    add_body = function_body(devfs_source, "devfs_wki_add_resource")
    require_tokens(
        add_body,
        [
            "existing_ctx->resource_generation == resource_generation",
            "existing_ctx->peer_hostname == peer_hostname",
            "wki_remove_resource_symlinks(existing_ctx)",
            "existing_ctx->resource_generation = resource_generation",
            "wki_copy_resource_name(existing_ctx, name)",
            "wki_add_resource_symlinks(type_dir_name, existing_ctx)",
        ],
        "devfs identity upsert must preserve opened node/device/context allocations",
    )
    open_body = function_body(devfs_source, "devfs_open_path")
    require_tokens(
        open_body,
        [
            "OptionalWkiLock const WKI_GUARD(WKI_PATH)",
            "node->wki_open_refs++",
            "devfs_file->owns_wki_node_ref = true",
            "wki_release_node_ref(node)",
        ],
        "WKI devfs open must pin nodes across unlink",
    )
    close_body = function_body(devfs_source, "devfs_close")
    require_tokens(
        close_body,
        ["if (devfs_file->owns_wki_node_ref)", "wki_release_node_ref(devfs_file->node)"],
        "WKI devfs close must release node pins",
    )
    unlink_body = function_body(devfs_source, "wki_unlink_node")
    require_tokens(
        unlink_body,
        ["node->wki_unlinked = true", "if (node->wki_open_refs == 0)", "wki_destroy_unlinked_node(node)"],
        "WKI devfs unlink must defer backing-object reclamation",
    )
    resource_ops = devfs_source[devfs_source.find("ker::dev::CharDeviceOps wki_resource_ops") :]
    if "WkiAutoLock const GUARD" not in resource_ops[: resource_ops.find("};")]:
        fail("WKI devfs metadata reads must snapshot under the mutation lock")
    remove_body = function_body(devfs_source, "devfs_wki_remove_resource")
    require_tokens(
        remove_body,
        ["expected_generation != 0", "ctx->resource_generation != expected_generation", "return"],
        "devfs removal must reject a replacement generation",
    )
    peer_remove_body = function_body(devfs_source, "devfs_wki_remove_peer_resources")
    if "std::array<DevFSNode*, 64>" in peer_remove_body or "remove_count < 64" in peer_remove_body:
        fail("peer devfs invalidation must not stop after a fixed 64-node batch")


def main() -> None:
    test_cleanup_publishes_only_when_it_claims_waiter()
    test_done_is_the_final_stack_waiter_access()
    test_ktest_covers_completed_claimed_cleanup()
    test_resource_withdraw_defers_blocking_unmount()
    print("WKI wait cleanup source invariants hold")


if __name__ == "__main__":
    main()
