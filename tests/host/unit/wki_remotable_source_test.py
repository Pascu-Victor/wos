#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTABLE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remotable.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_deferred_retry_deadlines_are_saturating() -> None:
    source = REMOTABLE_CPP.read_text()
    if "#include <net/wki/timer_math.hpp>" not in source:
        fail("remotable retry scheduling must use the shared WKI timer helpers")

    net_requeue = function_body(source, "requeue_net_attach")
    require_order(
        net_requeue,
        [
            "pending.retry_count++",
            "delay_us = std::min(delay_us, NET_AUTO_ATTACH_RETRY_MAX_US)",
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), delay_us)",
            "queue_net_attach_locked",
        ],
        "NET auto-attach retry backoff",
    )

    local_ipv4_defer = function_body(source, "defer_net_attach_for_local_ipv4")
    require_order(
        local_ipv4_defer,
        [
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US)",
            "queue_net_attach_locked",
        ],
        "NET auto-attach local IPv4 defer",
    )

    vfs_detach_defer = function_body(source, "defer_vfs_mount_for_detach")
    require_order(
        vfs_detach_defer,
        [
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), VFS_AUTO_MOUNT_RETRY_BASE_US)",
            "g_pending_vfs_mounts.push_back(pending)",
        ],
        "VFS detach defer",
    )
    if "pending.retry_count++" in vfs_detach_defer:
        fail("VFS detach deferral must not consume the finite auto-mount retry budget")

    net_detach_defer = function_body(source, "defer_net_attach_for_detach")
    require_order(
        net_detach_defer,
        [
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_RETRY_BASE_US)",
            "queue_net_attach_locked",
        ],
        "NET detach defer",
    )
    if "pending.retry_count++" in net_detach_defer:
        fail("NET detach deferral must not consume the finite auto-attach retry budget")

    mount_process = function_body(source, "wki_remotable_process_pending_mounts")
    require_order(
        mount_process,
        [
            "pending.retry_count++",
            "delay_us = std::min(delay_us, VFS_AUTO_MOUNT_RETRY_MAX_US)",
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), delay_us)",
            "g_pending_vfs_mounts.push_back(pending)",
        ],
        "VFS auto-mount retry backoff",
    )

    forbidden = [
        "pending.next_attempt_us = wki_now_us() + delay_us",
        "pending.next_attempt_us = wki_now_us() + NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("remotable retry scheduling must not use wrapping deadline arithmetic: " + ", ".join(present))


def test_pending_net_attach_is_generation_and_epoch_fenced() -> None:
    source = REMOTABLE_CPP.read_text()
    required = [
        "struct PendingNetAttach",
        "uint64_t resource_generation = 0",
        "void queue_net_attach_locked(uint16_t node_id, uint32_t resource_id, uint64_t resource_generation",
        "pending.resource_generation = resource_generation",
    ]
    missing = [token for token in required if token not in source]
    if missing:
        fail("pending NET generation identity is missing: " + ", ".join(missing))

    process = function_body(source, "wki_remotable_process_pending_net_attaches")
    for token in [
        "peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)",
        "defer_net_attach_for_epoch(pending)",
        "wki_remote_net_detach_pending_for_resource(pending.node_id, pending.resource_id)",
        "defer_net_attach_for_detach(pending)",
        "live_resource->generation == pending.resource_generation",
        "wki_resource_generation_is_live(pending.node_id, ResourceType::NET, pending.resource_id,",
        "wki_remote_net_attach(pending.node_id, pending.resource_id, pending.nic_name.data(), pending.resource_generation)",
        "PeerLifecycleLease post_attach_lifecycle",
        "!wki_remote_net_has_proxy(pending.node_id, pending.resource_id)",
    ]:
        if token not in process:
            fail(f"pending NET attach epoch/generation gate is missing {token}")
    require_order(
        process,
        [
            "live_resource->generation == pending.resource_generation",
            "wki_resource_generation_is_live(pending.node_id, ResourceType::NET",
            "wki_remote_net_attach(pending.node_id, pending.resource_id, pending.nic_name.data(), pending.resource_generation)",
        ],
        "NET exact observation before attach",
    )


def test_pending_vfs_mount_waits_for_detach_ack_without_spending_retries() -> None:
    source = REMOTABLE_CPP.read_text()
    process = function_body(source, "wki_remotable_process_pending_mounts")
    require_order(
        process,
        [
            "pending_vfs_mount_is_live_locked(pending)",
            "wki_remote_vfs_detach_pending_for_resource(pending.node_id, pending.resource_id)",
            "defer_vfs_mount_for_detach(pending)",
            "wki_remote_vfs_mount(pending.node_id, pending.resource_id",
            "RET == -EAGAIN",
            "defer_vfs_mount_for_detach(pending)",
            "pending.retry_count++",
        ],
        "VFS detach gate before attach and retry accounting",
    )

    advert = function_body(source, "handle_resource_advert")
    require_order(
        advert,
        [
            "wki_remote_net_detach_resource_generation(adv->node_id, adv->resource_id, retired_net_generation)",
            "devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id, retired_net_generation)",
            "queue_net_attach_locked(adv->node_id, adv->resource_id, devfs_generation",
        ],
        "NET replacement teardown before new publication",
    )

    withdraw = function_body(source, "handle_resource_withdraw")
    require_order(
        withdraw,
        [
            "wki_remote_net_detach_resource_generation(adv->node_id, adv->resource_id, retired_generation)",
            "devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id, retired_generation)",
        ],
        "NET withdrawal exact detach before devfs removal",
    )


def test_pending_vfs_mount_prepares_only_the_local_host_directory() -> None:
    source = REMOTABLE_CPP.read_text()
    process = function_body(source, "wki_remotable_process_pending_mounts")

    if "vfs_mkdir(pending.mount_path.data()" in process:
        fail("nested VFS auto-mount preparation must not recurse through an existing remote host root")

    require_order(
        process,
        [
            "std::copy_n(mount_path, host_dir_len, host_dir.data())",
            "ker::vfs::vfs_mkdir(host_dir.data(), 0755)",
            "wki_remote_vfs_mount(pending.node_id, pending.resource_id, pending.mount_path.data(), pending.resource_generation)",
        ],
        "local host directory preparation before mount-table publication",
    )


def test_same_incarnation_block_advert_revives_exact_generation() -> None:
    source = REMOTABLE_CPP.read_text()

    tombstone = function_body(source, "find_matching_block_tombstone_unlocked")
    require_order(
        tombstone,
        [
            "wki_resource_incarnation_valid(owner_incarnation)",
            "!res.valid",
            "res.resource_type == ResourceType::BLOCK",
            "wki_resource_incarnation_valid(res.owner_incarnation)",
            "wki_resource_incarnation_equal(res.owner_incarnation, owner_incarnation)",
        ],
        "BLOCK tombstone requires an exact negotiated owner identity",
    )

    invalidate = function_body(source, "wki_resources_invalidate_for_peer")
    for token in [
        "resource.resource_type == ResourceType::VFS || resource.resource_type == ResourceType::BLOCK",
        "res.resource_type != ResourceType::VFS && res.resource_type != ResourceType::BLOCK",
    ]:
        if token not in invalidate:
            fail(f"peer invalidation must retain dormant BLOCK observations: missing {token}")

    advert = function_body(source, "handle_resource_advert")
    require_order(
        advert,
        [
            "find_matching_block_tombstone_unlocked(adv->node_id, adv->resource_id, owner_incarnation)",
            "devfs_generation = tombstone->generation",
            "tombstone->valid = true",
            "wki_dev_proxy_reactivate_resource_observation(adv->node_id, adv->resource_id, devfs_generation, owner_incarnation)",
            "PEER->block_resume_pending.store(true, std::memory_order_release)",
            "wki_timer_notify()",
            "return",
            "std::erase_if(g_discovered",
            "res.generation = next_resource_generation_locked()",
        ],
        "same-incarnation BLOCK revival precedes fresh-generation fallback",
    )


def main() -> None:
    test_deferred_retry_deadlines_are_saturating()
    test_pending_net_attach_is_generation_and_epoch_fenced()
    test_pending_vfs_mount_waits_for_detach_ack_without_spending_retries()
    test_pending_vfs_mount_prepares_only_the_local_host_directory()
    test_same_incarnation_block_advert_revives_exact_generation()
    print("WKI remotable source invariants hold")


if __name__ == "__main__":
    main()
