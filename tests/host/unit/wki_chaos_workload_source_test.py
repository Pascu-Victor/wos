#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def require(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        raise AssertionError(f"{context}: missing {', '.join(missing)}")


def main() -> None:
    workload = (ROOT / "modules/kern/src/net/wki/chaos_workload.cpp").read_text()
    workload_hpp = (ROOT / "modules/kern/src/net/wki/chaos_workload.hpp").read_text()
    guest_workload = (ROOT / "modules/testprog/src/wki_chaos_workload.cpp").read_text()
    wki = (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    remotable = (ROOT / "modules/kern/src/net/wki/remotable.cpp").read_text()
    procfs = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    wkictl = (ROOT / "modules/wkictl/src/chaos.cpp").read_text()
    resolver = (ROOT / "modules/wkictl/src/peer_resolver.cpp").read_text()

    require(
        guest_workload,
        [
            "if ((descriptor.revents & POLLNVAL) != 0)",
            "errno = EBADF",
            "if ((descriptor.revents & (events | POLLERR | POLLHUP)) != 0)",
        ],
        "guest workload terminal poll readiness",
    )
    if "descriptor.revents & (POLLERR | POLLNVAL)" in guest_workload:
        raise AssertionError("guest workload must retry I/O after POLLERR to obtain the authoritative result")

    require(
        wki,
        [
            'cmdline_has_token(ker::init::get_kernel_cmdline(), "wki.chaos")',
            'cmdline_has_token(ker::init::get_kernel_cmdline(), "wki.chaos.workload")',
            "wki_chaos_workload_shutdown()",
        ],
        "dual boot gates and teardown",
    )
    require(
        workload_hpp,
        [
            "WKI_CHAOS_WORKLOAD_COMMAND_MAX = 256",
            "WKI_CHAOS_WORKLOAD_SCRATCH_BYTES",
            "wki_chaos_workload_compute_publish_wait(uint64_t absolute_deadline_us)",
        ],
        "bounded adapter header",
    )
    require(
        workload,
        [
            'SCRATCH_RESOURCE_NAME[] = "wki-chaos-scratch"',
            'VFS_ADVERTISED_NAME[] = "/tmp"',
            "std::array<uint8_t, WKI_CHAOS_WORKLOAD_SCRATCH_BYTES>",
            'std::strcmp(verb, "block-rdma-discovered")',
            'std::strcmp(verb, "net-attach-discovered")',
            'std::strcmp(verb, "net-query-discovered")',
            'std::strcmp(verb, "net-detach-discovered")',
            'std::strcmp(verb, "vfs-query-discovered")',
            'std::strcmp(verb, "vfs-unmount-discovered")',
            'std::strcmp(name_token, "name=tmp")',
            "wki_resource_resolve_unique",
            "resource_still_exact",
            "wki_dev_proxy_attach_block",
            "WkiBlockAttachRdmaPolicy::DISABLE",
            "wki_remote_net_attach",
            "wki_remote_net_detach(DEVICE)",
            "clear_managed_net_attachment",
            "managed_net_detach_converged",
            "net_evidence_identity",
            "proxy.binding_peer_boot_epoch == CURRENT_PEER_BOOT_EPOCH",
            ".binding_peer_boot_epoch = proxy.binding_peer_boot_epoch",
            "proxy.binding_peer_boot_epoch == s_managed_net.binding_peer_boot_epoch",
            "wki_remote_vfs_unmount_resource_generation",
            "auto run_vfs_query",
            "return capture_vfs_binding(command)",
            "proxy.rdma_attached != REQUIRE_RDMA",
            "data_slot_bitmap == 0",
            "tag_bitmap == 0",
            "channel_tx_seq_before",
            "channel_tx_seq_after",
            "s_result.restored",
            "s_result.detached",
            "SAFE_WRITE_REJECTION",
            'std::strcmp(verb, "compute-publish")',
            "release_compute_publish_hold()",
            "COMPUTE_PUBLISH_POLL_MAX_US",
            "absolute_deadline_us",
        ],
        "strict exact workload operations",
    )
    if "block_device_find_by_name" in workload or "wki_remote_vfs_unmount(" in workload:
        raise AssertionError("workload bridge must not select an arbitrary device or VFS path")
    if "first_tag" in workload or "last_tag" in workload:
        raise AssertionError("channel sequence counters must not be reported as block-ring tags")
    if "NET_EVIDENCE_IDENTITY.resource_incarnation == 0" not in workload:
        raise AssertionError("NET evidence must preserve the intentional absence of a wire incarnation token")
    poll_body = workload[
        workload.index("auto poll_managed_net_detach") : workload.index("auto clear_managed_net_attachment")
    ]
    clear_body = workload[
        workload.index("auto clear_managed_net_attachment() -> int") : workload.index("auto run_net_query")
    ]
    require(
        poll_body + clear_body,
        [
            "copy_managed_net_result()",
            "snapshot_net_proxy(command",
            "if (!s_managed_net.detach_issued)",
            "s_managed_net.detach_issued = true",
            "s_managed_net.device = nullptr",
            "wki_remote_net_detach(DEVICE)",
            "s_result.detached = managed_net_detach_converged(snapshot_complete, active_matches)",
            "if (s_result.detached)",
            "s_managed_net = {}",
            "return s_result.detached ? 0 : -EBUSY",
        ],
        "bounded exact managed-NET clear",
    )
    if clear_body.count("wki_remote_net_detach(DEVICE)") != 1:
        raise AssertionError("managed-NET clear must issue exact detach at most once")
    if not (
        clear_body.index("s_managed_net.detach_issued = true")
        < clear_body.index("s_managed_net.device = nullptr")
        < clear_body.index("wki_remote_net_detach(DEVICE)")
        < clear_body.index("return poll_managed_net_detach(command)")
    ) or not (
        poll_body.index("s_result.detached = managed_net_detach_converged")
        < poll_body.index("if (s_result.detached)")
        < poll_body.index("s_managed_net = {}")
    ):
        raise AssertionError("managed-NET cleanup receipt must survive until post-detach convergence proof")
    require(
        remotable,
        [
            "auto wki_resource_resolve_unique",
            "g_discovered.size() > WKI_RESOURCE_DIAG_MAX",
            "matches > 1",
            "auto wki_resource_snapshot_exact",
        ],
        "bounded discovery resolution",
    )
    require(
        compute,
        [
            "wki_chaos_workload_compute_publish_wait(deadline_us)",
            "POST_HOLD_SESSION_CURRENT",
            "take_cancelled_task_submit_locked(session, submit->task_id)",
            "POST_HOLD_DEADLINE_EXPIRED",
            "ExecResult const EXEC",
        ],
        "cancel-before-exec publication gate",
    )
    if compute.index("wki_chaos_workload_compute_publish_wait(deadline_us)") > compute.index("ExecResult const EXEC"):
        raise AssertionError("compute hold must run before exec construction")
    require(
        procfs,
        [
            'std::memcpy(buf->d_name.data(), "chaos_workload", 15)',
            "ProcNodeType::WKI_CHAOS_WORKLOAD_FILE",
            "wki_chaos_workload_configure",
            "wki_chaos_workload_snapshot",
            'strcmp(path, "wki/chaos_workload") == 0',
        ],
        "proc workload node",
    )
    require(
        wkictl,
        [
            'WKI_CHAOS_WORKLOAD_PATH = "/proc/wki/chaos_workload"',
            "WKI_CHAOS_WORKLOAD_COMMAND_MAX = 256",
            'std::strcmp(argv[2], "compute-publish-wait") == 0',
            "parse_workload_wait_request",
            "wait_for_compute_publish",
            'u32_field(snapshot, LINE_LEN, "compute_waiters", &waiters)',
            "waiters >= request.waiters",
            "write_command(FD, command.data(), length)",
            "read_workload_snapshot",
            "static_cast<void>(print_workload_result())",
            'std::strncmp(token, "owner_host=", 11)',
            "resolve_peer_hostname(token + 11, &owner)",
            'std::snprintf(resolved_owner.data(), resolved_owner.size(), "owner=%u", owner)',
            "have_owner_selector",
        ],
        "direct argv workload bridge and deterministic barrier",
    )
    require(
        resolver,
        [
            'WKI_PEERS_PATH = "/proc/wki/peers"',
            "WKI_PEER_PROC_SNAPSHOT_CAPACITY - 1",
            "split_peer_row",
            "seen_count >= seen.size()",
            "seen.at(index).node_id == ROW_NODE",
            "local_count != 1",
            "matches != 1",
            "!matching_connected",
        ],
        "strict bounded peer hostname resolver",
    )

    print("WKI chaos workload gate, exact binding, scratch, and direct-argv source checks passed")


if __name__ == "__main__":
    main()
