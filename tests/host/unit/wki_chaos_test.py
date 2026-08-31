#!/usr/bin/env python3

import importlib.util
import json
import socket
import sys
import tempfile
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "scripts" / "test" / "wki_chaos.py"
MATRIX = ROOT / "configs" / "wki_chaos_matrix.json"
CATALOG = ROOT / "configs" / "wki_chaos_scenarios.json"
I1_WORKLOAD = ROOT / "modules" / "testprog" / "src" / "wki_chaos_workload.cpp"


def load_module():
    spec = importlib.util.spec_from_file_location("wki_chaos", RUNNER)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {RUNNER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def test_snapshot_reads_packet_pool_before_other_ssh_observers(module) -> None:
    assert_equal(
        module.SNAPSHOT_SOURCES[0],
        "netdiag",
        "packet-pool snapshot precedes observer-created TCP teardown traffic",
    )
    assert_equal(
        set(module.SNAPSHOT_SOURCES),
        {"chaos", "kipcstat", "netdiag", "peers", "pipes"},
        "snapshot source coverage",
    )


def test_guest_transport_reuses_and_explicitly_closes_control_scope(module) -> None:
    calls = []
    original = module.run_bounded_command

    def record(argv, _timeout_ms, **_kwargs):
        calls.append(argv)
        return {
            "exitCode": 0,
            "stderr": "",
            "stderrBytes": "0",
            "stderrTruncated": False,
            "stdout": "",
            "stdoutBytes": "0",
            "stdoutTruncated": False,
            "timedOut": False,
        }

    module.run_bounded_command = record
    transport = Path("/tmp/fake-wki-chaos-ssh")
    try:
        with module._SshControlScope(transport) as scope:
            argv = module._guest_transport_argv(
                {"host": "wos-0"}, ["/usr/bin/true"], transport
            )
            assert_equal(argv[0], "/usr/bin/env", "SSH control environment wrapper")
            if not argv[1].startswith("WOS_SSH_CONTROL_DIR=/tmp/wos-wki-chaos-ssh-"):
                raise AssertionError(f"unbounded SSH control path: {argv[1]!r}")
            assert_equal(argv[2:], [str(transport), "wos-0", "/usr/bin/true"], "mux command argv")
            assert_equal(scope.hosts, {"wos-0"}, "mux node registry")
    finally:
        module.run_bounded_command = original
    assert_equal(len(calls), 1, "one explicit control-master teardown")
    if "WOS_SSH_CONTROL_COMMAND=exit" not in calls[0]:
        raise AssertionError(f"SSH control master was not explicitly closed: {calls[0]!r}")
    assert_equal(module._ACTIVE_SSH_CONTROL_SCOPE, None, "SSH control scope restoration")


def base_scenario(events: list[dict], *, with_qmp: bool = False) -> dict:
    node = {"id": "0", "host": "wos-0"}
    if with_qmp:
        node.update({"nics": {"wki": "net1"}, "qmp_socket": "state/qmp-vm0.sock"})
    return {
        "events": events,
        "format": "wos.wki-chaos-scenario",
        "name": "unit",
        "nodes": [node],
        "version": 1,
    }


def guest_operation(argv: list[str] | None = None) -> dict:
    return {
        "argv": argv or ["true"],
        "kind": "guest",
        "node": "0",
        "role": "workload",
        "timeout_ms": 1000,
    }


def test_plan_is_seed_stable_and_choice_sensitive(module) -> None:
    scenario = base_scenario(
        [
            {
                "alternatives": [
                    {"kind": "barrier", "label": "left"},
                    {"kind": "barrier", "label": "right"},
                ],
                "group": "choice",
                "id": "choose",
                "phase": "fault",
            }
        ]
    )
    first = module.make_plan(scenario, 1)
    assert_equal(
        module.canonical_json_bytes(first),
        module.canonical_json_bytes(module.make_plan(scenario, 1)),
        "same seed plan bytes",
    )
    different = None
    for seed in range(2, 100):
        candidate = module.make_plan(scenario, seed)
        if candidate["events"][0]["choiceIndex"] != first["events"][0]["choiceIndex"]:
            different = candidate
            break
    if different is None or different["planDigest"] == first["planDigest"]:
        raise AssertionError("seed did not affect a bounded alternative choice")
    module.validate_plan(first)


def test_schema_rejects_shell_duplicates_and_bad_qmp(module) -> None:
    try:
        module.make_plan(
            base_scenario(
                [{"id": "shell", "phase": "workload", "operation": guest_operation(["sh", "-c", "true"])}]
            ),
            0,
        )
    except module.ChaosInputError:
        pass
    else:
        raise AssertionError("shell command was accepted as guest argv")

    try:
        json.loads('{"a":1,"a":2}', object_pairs_hook=module._object_without_duplicates)
    except module.ChaosInputError:
        pass
    else:
        raise AssertionError("duplicate JSON member was accepted")

    qmp_scenario = base_scenario(
        [
            {
                "id": "link",
                "phase": "fault",
                "operation": {
                    "command": "set_link",
                    "kind": "qmp",
                    "nic": "wki",
                    "node": "0",
                    "up": False,
                },
            }
        ],
        with_qmp=True,
    )
    plan = module.make_plan(qmp_scenario, 0)
    assert_equal(
        plan["events"][0]["operation"]["arguments"],
        {"name": "net1", "up": False},
        "logical NIC to QMP netdev mapping",
    )


def test_bounded_command_records_exit_timeout_and_truncation(module) -> None:
    result = module.run_bounded_command(
        [
            sys.executable,
            "-c",
            "import sys; sys.stdout.write('o'*70000); sys.stderr.write('e'*70000); raise SystemExit(7)",
        ],
        5000,
    )
    assert_equal(result["exitCode"], 7, "bounded command exit code")
    assert_equal(result["stdoutTruncated"], True, "stdout truncation")
    assert_equal(result["stderrTruncated"], True, "stderr truncation")
    assert_equal(len(result["stdout"].encode()), module.MAX_COMMAND_OUTPUT_BYTES, "stored stdout bound")

    timed_out = module.run_bounded_command(
        [sys.executable, "-c", "import time; time.sleep(5)"], 20
    )
    assert_equal(timed_out["timedOut"], True, "bounded command timeout")

    snapshot_sized = module.run_bounded_command(
        [sys.executable, "-c", "import sys; sys.stdout.write('s'*140000)"],
        5000,
        stdout_limit_bytes=module.MAX_SNAPSHOT_OUTPUT_BYTES,
    )
    assert_equal(snapshot_sized["stdoutTruncated"], False, "snapshot-sized stdout")
    assert_equal(snapshot_sized["stdoutBytes"], "140000", "snapshot-sized stdout bytes")


def test_guest_poll_retries_direct_argv_and_records_attempts(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        counter = root / "attempts"
        transport = root / "exec_ssh.py"
        make_exec_transport(transport)
        code = (
            "from pathlib import Path;"
            f"p=Path({str(counter)!r});"
            "n=int(p.read_text())+1 if p.exists() else 1;"
            "p.write_text(str(n));"
            "raise SystemExit(0 if n>=3 else 7)"
        )
        scenario = base_scenario(
            [
                {
                    "id": "ready",
                    "phase": "heal",
                    "operation": {
                        "argv": [sys.executable, "-c", code],
                        "attempt_timeout_ms": 1000,
                        "deadline_ms": 5000,
                        "expect_exit": [0],
                        "interval_ms": 1,
                        "kind": "guest_poll",
                        "max_attempts": 5,
                        "node": "0",
                        "role": "invariant",
                    },
                }
            ]
        )
        replay = module.execute_plan(
            module.make_plan(scenario, 3), root / "poll", ssh_script=transport
        )
        result = replay["events"][0]["observation"]["result"]
        assert_equal(replay["outcome"], "passed", "guest poll outcome")
        assert_equal(result["attemptsUsed"], 3, "guest poll bounded retry count")
        assert_equal(
            [attempt["status"] for attempt in result["attempts"]],
            ["failed", "failed", "passed"],
            "guest poll retains every attempt summary",
        )

        large_result, large_success, large_complete = module._poll_guest_argv(
            argv=[sys.executable, "-c", "import sys; sys.stdout.write('n'*140000)"],
            attempt_timeout_ms=1000,
            deadline_ms=5000,
            expect_exit=[0],
            interval_ms=1,
            max_attempts=1,
            node={"host": "unused"},
            result_probe=lambda command_result: (
                len(command_result["stdout"]) == 140000,
                {"bytes": command_result["stdoutBytes"]},
            ),
            ssh_script=transport,
            stdout_limit_bytes=module.MAX_SNAPSHOT_OUTPUT_BYTES,
        )
        assert_equal(
            (large_success, large_complete),
            (True, True),
            "snapshot-sized guest poll",
        )
        assert_equal(
            large_result["attempts"][0]["probe"]["bytes"],
            "140000",
            "snapshot-sized guest poll bytes",
        )


def test_guest_stdout_row_contract_is_bounded_and_relational(module) -> None:
    operation = guest_operation(["/usr/bin/wkictl", "chaos-workload", "block-discovered"])
    operation["stdout_row"] = {
        "equals": {"detached": "1", "status": "0"},
        "negative": ["write_status"],
        "nonzero": ["attach_cookie"],
        "one_of": {"rdma_lane": ["ivshmem", "roce"]},
        "prefix": "wki_chaos_workload",
    }
    plan = module.make_plan(
        base_scenario([{"id": "row", "phase": "workload", "operation": operation}]),
        1,
    )
    contract = plan["events"][0]["operation"]["stdout_row"]
    matched, observation = module._match_stdout_row(
        "wki_chaos_workload status=0 write_status=-74 detached=1 "
        "attach_cookie=9 rdma_lane=roce vfs_lanes=\n",
        contract,
    )
    assert_equal(matched, True, "valid workload row")
    assert_equal(observation["matched"], True, "valid row observation")
    mismatched, bad_observation = module._match_stdout_row(
        "wki_chaos_workload status=0 write_status=0 detached=1 "
        "attach_cookie=0 rdma_lane=message\n",
        contract,
    )
    assert_equal(mismatched, False, "invalid workload row")
    if len(bad_observation["errors"]) != 3:
        raise AssertionError(f"row contract did not retain all mismatches: {bad_observation!r}")


def test_group_reducer_never_splits_pairs(module) -> None:
    events = [
        {"group": "fixed", "id": "setup", "ordinal": 0, "reducible": False},
        {"group": "pair", "id": "partition", "ordinal": 1, "reducible": True},
        {"group": "noise", "id": "duplicate", "ordinal": 2, "reducible": True},
        {"group": "pair", "id": "heal", "ordinal": 3, "reducible": True},
        {"group": "failure", "id": "check", "ordinal": 4, "reducible": True},
    ]

    def predicate(candidate, _removed):
        return any(event["group"] == "failure" for event in candidate)

    minimized, attempts = module.grouped_ddmin(events, predicate, 16)
    assert_equal([event["id"] for event in minimized], ["setup", "check"], "grouped reduction")
    if attempts <= 0 or attempts > 16:
        raise AssertionError(f"invalid bounded reduction attempt count {attempts}")


def snapshot_texts() -> dict[str, str]:
    return {
        "netdiag": (
            "packet_pool capacity=32 baseline=32 active_capacity=32 free=32 used=0 draining=0\n"
            "backlog ready=1 cpus=1 queues=1 queued=0\n"
            "wki_channel peer=0x0001 state=CONNECTED active=1 ack_pending=0 retransmit_count=0 reorder_count=0\n"
            "wki_netdiag_end complete=1\n"
        ),
        "peers": (
            "hostname node_id connected cpus load_pct last_update_us local\n"
            "wos-0 0 1 2 0 10 1\n"
            "wos-1 1 1 2 0 10 0\n"
        ),
        "chaos": (
            "wki_chaos schema=1 supported=1 runtime_control=1 enabled=1 seed=7 rules=0 queued=0 "
            "trace_rows=0 first_event=0 last_event=0 queue_overflow=0 trace_overflow=0 "
            "stream_overflow=0 invalid=0\n"
            "wki_chaos_counters observed=0 matched=0 passed=0 dropped=0 duplicated=0 delayed=0 "
            "reordered=0 corrupted=0 failed=0 released=0\n"
            "wki_chaos_end complete=1\n"
        ),
        "pipes": "pipe active=0 reader_waiters=0 writer_waiters=0 poll_waiters=0 refs=0\n",
        "kipcstat": (
            "exports=0 proxies=0 pump_tasks=0 pending_deliveries=0 proxy_close_queue=0 "
            "local_pipe_active=5 local_pipe_approx_alloc_bytes=1321880 "
            "local_pipe_capacity=1310720 local_pipe_direct_writes=1 "
            "local_pipe_poll_waiters=3 local_pipe_read_closed=0 "
            "local_pipe_reader_waiters=0 local_pipe_write_closed=1 "
            "local_pipe_writer_waiters=0\n"
        ),
    }


def parsed_snapshot(module, texts: dict[str, str] | None = None) -> dict:
    values = texts or snapshot_texts()
    return {
        "complete": True,
        "rows": {
            source: module.parse_snapshot_rows(source, values[source])
            for source in module.SNAPSHOT_SOURCES
        },
    }


def append_netdiag_rows(texts: dict[str, str], rows: str) -> None:
    terminal = "wki_netdiag_end complete=1\n"
    if not texts["netdiag"].endswith(terminal):
        raise AssertionError("netdiag fixture lacks its terminal sentinel")
    texts["netdiag"] = texts["netdiag"].removesuffix(terminal) + rows + terminal


def test_snapshot_invariants_require_complete_stable_gauges(module) -> None:
    baseline = parsed_snapshot(module)
    current = parsed_snapshot(module)
    allow = {"max": {}, "nonzero": [], "packet_delta": 0, "peer_disconnected": []}
    status, issues = module.evaluate_invariants(baseline, current, allow)
    assert_equal((status, issues), ("passed", []), "clean invariant snapshot")
    assert_equal(
        module.invariant_projection(current, allow),
        module.invariant_projection(parsed_snapshot(module), allow),
        "stable invariant projection",
    )

    bad_texts = snapshot_texts()
    bad_texts["kipcstat"] = snapshot_texts()["kipcstat"].replace(
        "pending_deliveries=0", "pending_deliveries=1"
    )
    status, issues = module.evaluate_invariants(baseline, parsed_snapshot(module, bad_texts), allow)
    assert_equal(status, "failed", "nonzero pending work")
    if not any(issue.get("selector") == "kipcstat.pending_deliveries" for issue in issues):
        raise AssertionError(f"pending-delivery invariant was not reported: {issues!r}")

    retained_ipc = snapshot_texts()
    retained_ipc["kipcstat"] = retained_ipc["kipcstat"].replace(
        "exports=0 proxies=0", "exports=1 proxies=1"
    )
    append_netdiag_rows(
        retained_ipc,
        "wki_ipc exports=1 proxies=1 active_pumps=0 pending_deliveries=0\n"
        "wki_ipc_diag_counts exports=1 active_exports=1 proxies=1 active_proxies=1 "
        "pending_deliveries=0 dev_op_work=0 truncated=0\n",
    )
    status, issues = module.evaluate_invariants(
        baseline, parsed_snapshot(module, retained_ipc), allow
    )
    assert_equal(status, "failed", "retained IPC export/proxy ownership")
    retained_selectors = {
        issue.get("selector")
        for issue in issues
        if issue.get("code") == "gauge_nonzero"
    }
    expected_selectors = {
        "kipcstat.exports",
        "kipcstat.proxies",
        "wki_ipc.exports",
        "wki_ipc.proxies",
        "wki_ipc_diag_counts.exports",
        "wki_ipc_diag_counts.active_exports",
        "wki_ipc_diag_counts.proxies",
        "wki_ipc_diag_counts.active_proxies",
    }
    if not expected_selectors.issubset(retained_selectors):
        raise AssertionError(
            f"retained IPC ownership escaped convergence: {sorted(expected_selectors - retained_selectors)!r}"
        )

    no_overflow = snapshot_texts()
    no_overflow["chaos"] = (
        "wki_chaos schema=1 supported=1 runtime_control=1 enabled=1 queued=0\n"
        "wki_chaos_end complete=1\n"
    )
    status, issues = module.evaluate_invariants(baseline, parsed_snapshot(module, no_overflow), allow)
    assert_equal(status, "incomplete", "missing chaos overflow observability")
    if issues[0]["code"] != "chaos_overflow_observability_missing":
        raise AssertionError(f"wrong incomplete issue: {issues!r}")

    changed_pipe = snapshot_texts()
    changed_pipe["kipcstat"] = changed_pipe["kipcstat"].replace(
        "local_pipe_active=5", "local_pipe_active=6"
    )
    status, issues = module.evaluate_invariants(
        baseline, parsed_snapshot(module, changed_pipe), allow
    )
    assert_equal(status, "failed", "local pipe lifecycle returns to baseline")
    if not any(
        issue.get("code") == "gauge_changed_from_baseline"
        and issue.get("selector") == "kipcstat.local_pipe_active"
        for issue in issues
    ):
        raise AssertionError(f"local-pipe baseline delta was not reported: {issues!r}")

    for source, terminal in (
        ("chaos", "wki_chaos_end complete=1\n"),
        ("netdiag", "wki_netdiag_end complete=1\n"),
    ):
        truncated = snapshot_texts()
        truncated[source] = truncated[source].removesuffix(terminal)
        rows = {
            item: module.parse_snapshot_rows(item, truncated[item])
            for item in module.SNAPSHOT_SOURCES
        }
        terminal_issues = module.snapshot_completeness_issues(rows)
        if not any(
            issue.get("code") == "diagnostic_terminal_incomplete"
            and issue.get("source") == source
            for issue in terminal_issues
        ):
            raise AssertionError(
                f"missing terminal sentinel was accepted for {source}: {terminal_issues!r}"
            )


def test_pipe_snapshot_parses_positional_waiter_detail(module) -> None:
    rows = module.parse_snapshot_rows(
        "pipes",
        "owner_records=1 owner_truncated=0\n"
        "pipe=0xffff open_ends=2 poll_waiters=18\n"
        " waiters read_poll 109 110 ...(+16)\n"
        " owner pid=109 fd=4 kind=read cloexec=0 flags=0x0 file_refs=1\n",
    )
    assert_equal(
        rows[2],
        {
            "row": "waiters",
            "values": {"kind": "read_poll", "omitted": "16", "pids": "109,110"},
        },
        "positional pipe waiter detail",
    )
    texts = snapshot_texts()
    texts["pipes"] = (
        "owner_records=1 owner_truncated=0\n"
        "pipe=0xffff open_ends=2 read_refs=1 write_refs=1 poll_waiters=1\n"
        " waiters read_poll 109\n"
        " owner pid=109 fd=4 kind=read cloexec=0 flags=0x0 file_refs=1\n"
    )
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module),
        parsed_snapshot(module, texts),
        {"max": {}, "nonzero": [], "packet_delta": 0, "peer_disconnected": []},
    )
    assert_equal((status, issues), ("passed", []), "persistent local pipe ownership")


def test_peer_snapshot_rejects_duplicate_stable_identities(module) -> None:
    texts = snapshot_texts()
    texts["peers"] = (
        "hostname node_id connected cpus load_pct last_update_us local\n"
        "wos-0 0 1 2 0 10 1\n"
        "wos-1 29187 0 2 0 10 0\n"
        "wos-1 56403 1 2 0 10 0\n"
        "node-7203 0x7203 0 2 0 10 0\n"
    )
    rows = {
        source: module.parse_snapshot_rows(source, texts[source])
        for source in module.SNAPSHOT_SOURCES
    }
    issues = module.snapshot_completeness_issues(rows)
    duplicates = [
        (issue.get("field"), issue.get("value"))
        for issue in issues
        if issue.get("code") == "peer_identity_duplicate"
    ]
    assert_equal(
        duplicates,
        [("hostname", "wos-1"), ("node_id", "0x7203")],
        "duplicate peer identities",
    )


def test_replaced_peer_allowance_requires_exact_epoch_and_cleanup_fence(module) -> None:
    baseline_texts = snapshot_texts()
    append_netdiag_rows(
        baseline_texts,
        "wki_peer_lifecycle peer=0x0001 state=CONNECTED remote_boot_epoch=10 "
        "replacement=0x0000 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n",
    )
    current_texts = snapshot_texts()
    current_texts["peers"] = (
        "hostname node_id connected cpus load_pct last_update_us local\n"
        "wos-0 0 1 2 0 10 1\n"
        "node-0001 1 0 0 0 10 0\n"
        "wos-1 2 1 2 0 10 0\n"
    )
    append_netdiag_rows(
        current_texts,
        "wki_peer_lifecycle peer=0x0001 state=FENCED remote_boot_epoch=10 "
        "replacement=0x0002 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n"
        "wki_peer_lifecycle peer=0x0002 state=CONNECTED remote_boot_epoch=20 "
        "replacement=0x0000 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n",
    )
    allow = module._validate_invariant_allowlist(
        {"peer_replaced": ["wos-1"]}, "allow"
    )
    assert_equal(
        allow["peer_disconnected"], [], "replacement proof is not a broad disconnect allowlist"
    )
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module, baseline_texts),
        parsed_snapshot(module, current_texts),
        allow,
    )
    assert_equal((status, issues), ("passed", []), "fully fenced peer replacement")

    unfenced_texts = dict(current_texts)
    unfenced_texts["netdiag"] = unfenced_texts["netdiag"].replace(
        "replacement=0x0002 lifecycle=0", "replacement=0x0002 lifecycle=1", 1
    )
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module, baseline_texts),
        parsed_snapshot(module, unfenced_texts),
        allow,
    )
    assert_equal(status, "failed", "replacement with active retirement gate")
    if not any(issue.get("code") == "peer_replacement_unproven" for issue in issues):
        raise AssertionError(f"unfenced replacement was accepted: {issues!r}")


def test_repeated_peer_replacement_preserves_proven_tombstones(module) -> None:
    baseline_texts = snapshot_texts()
    baseline_texts["peers"] = (
        "hostname node_id connected cpus load_pct last_update_us local\n"
        "wos-0 0 1 2 0 10 1\n"
        "node-0009 9 0 0 0 10 0\n"
        "wos-1 1 1 2 0 10 0\n"
    )
    append_netdiag_rows(
        baseline_texts,
        "wki_peer_lifecycle peer=0x0009 state=FENCED remote_boot_epoch=90 "
        "replacement=0x0001 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n"
        "wki_peer_lifecycle peer=0x0001 state=CONNECTED remote_boot_epoch=100 "
        "replacement=0x0000 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n",
    )
    current_texts = snapshot_texts()
    current_texts["peers"] = (
        "hostname node_id connected cpus load_pct last_update_us local\n"
        "wos-0 0 1 2 0 10 1\n"
        "node-0009 9 0 0 0 10 0\n"
        "node-0001 1 0 0 0 10 0\n"
        "wos-1 2 1 2 0 10 0\n"
    )
    append_netdiag_rows(
        current_texts,
        "wki_peer_lifecycle peer=0x0009 state=FENCED remote_boot_epoch=90 "
        "replacement=0x0001 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n"
        "wki_peer_lifecycle peer=0x0001 state=FENCED remote_boot_epoch=100 "
        "replacement=0x0002 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n"
        "wki_peer_lifecycle peer=0x0002 state=CONNECTED remote_boot_epoch=200 "
        "replacement=0x0000 lifecycle=0 compute_cleanup=0 vfs_rebind=0 "
        "block_resume=0 invalidate_discovery=0 owner_reboot=0\n",
    )
    allow = module._validate_invariant_allowlist(
        {"peer_replaced": ["wos-1"]}, "allow"
    )
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module, baseline_texts),
        parsed_snapshot(module, current_texts),
        allow,
    )
    assert_equal((status, issues), ("passed", []), "repeated fenced peer replacement")

    reactivated = dict(current_texts)
    reactivated["peers"] = reactivated["peers"].replace(
        "node-0009 9 0 0", "node-0009 9 1 0"
    )
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module, baseline_texts),
        parsed_snapshot(module, reactivated),
        allow,
    )
    assert_equal(status, "failed", "baselined tombstone cannot reactivate")
    if not any(
        issue.get("code") == "baseline_peer_tombstone_reactivated"
        for issue in issues
    ):
        raise AssertionError(f"reactivated tombstone was accepted: {issues!r}")


def test_peer_epoch_probe_follows_rebooted_hostname(module) -> None:
    probe_text = (
        "hostname node_id connected cpus load_pct last_update_us local\n"
        "wos-0 1 1 2 0 10 1\n"
        "node-7203 0x7203 0 0 0 10 0\n"
        "wos-1 0xdc53 1 2 0 10 0\n"
        "packet_pool capacity=32 baseline=32 active_capacity=32 free=32 used=0 draining=0\n"
        "wki_wait entry=0x1 state=0 name= task=1\n"
        "wki_peer_lifecycle peer=0x7203 state=FENCED remote_boot_epoch=100\n"
        "wki_peer_lifecycle peer=0xdc53 state=CONNECTED remote_boot_epoch=200\n"
        "wki_netdiag_end complete=1\n"
    )
    matched, observation = module._match_peer_epoch_probe(
        probe_text,
        peer_host="wos-1",
        baseline_peer_id=0x7203,
        baseline_epoch=100,
    )
    assert_equal(matched, True, "reboot successor peer epoch")
    assert_equal(observation["baselinePeerId"], str(0x7203), "predecessor node ID")
    assert_equal(observation["currentPeerId"], str(0xDC53), "successor node ID")
    assert_equal(observation["currentRemoteBootEpoch"], "200", "successor boot epoch")

    retained_predecessor = probe_text.replace(
        "node-7203 0x7203 0 0 0 10 0",
        "wos-1 0x7203 0 0 0 10 0",
    )
    matched, observation = module._match_peer_epoch_probe(
        retained_predecessor,
        peer_host="wos-1",
        baseline_peer_id=0x7203,
        baseline_epoch=100,
    )
    assert_equal(matched, True, "connected successor with retained fenced predecessor")
    assert_equal(observation["matchingPeerRows"], 2, "diagnostic peer row count")
    assert_equal(observation["connectedMatchingPeerRows"], 1, "unique connected successor")

    ambiguous = retained_predecessor.replace(
        "wos-1 0x7203 0 0 0 10 0",
        "wos-1 0x7203 1 2 0 10 0",
    ).replace(
        "wki_peer_lifecycle peer=0x7203 state=FENCED",
        "wki_peer_lifecycle peer=0x7203 state=CONNECTED",
    )
    matched, observation = module._match_peer_epoch_probe(
        ambiguous,
        peer_host="wos-1",
        baseline_peer_id=0x7203,
        baseline_epoch=100,
    )
    assert_equal(matched, False, "ambiguous connected successor hostname")
    assert_equal(observation["connectedMatchingPeerRows"], 2, "ambiguous peer row count")

    stale_only = retained_predecessor.replace(
        "wos-1 0xdc53 1 2 0 10 0\n", ""
    ).replace(
        "wki_peer_lifecycle peer=0xdc53 state=CONNECTED remote_boot_epoch=200\n", ""
    )
    matched, observation = module._match_peer_epoch_probe(
        stale_only,
        peer_host="wos-1",
        baseline_peer_id=0x7203,
        baseline_epoch=100,
    )
    assert_equal(matched, False, "disconnected predecessor cannot qualify")
    assert_equal(observation["connectedMatchingPeerRows"], 0, "no connected successor")


def test_snapshot_detail_rdma_and_owner_contracts(module) -> None:
    texts = snapshot_texts()
    append_netdiag_rows(texts, (
        "wki_net_proxy owner=0x0001 resource=7 generation=2 ch=3 ch_gen=4 refs=0 "
        "detail_complete=0 op_pending=0 attach_pending=0 detach_pending=0\n"
        "wki_block_proxy owner=0x0001 resource=8 generation=2 owner_boot=5 incarnation=6 "
        "ch=3 ch_gen=4 lifecycle_detail=0 io_detail=1 rdma=1 geometry_valid=1 indices_valid=1\n"
        "wki_server_binding consumer=0x0001 type=block resource=8 owner_boot=5 incarnation=6 "
        "cookie=9 ch=3 ch_gen=4 refs=0 rdma=1 ring_stable=0 geometry_valid=1 indices_valid=1\n"
    ))
    rows = {
        source: module.parse_snapshot_rows(source, texts[source])
        for source in module.SNAPSHOT_SOURCES
    }
    issues = module.snapshot_completeness_issues(rows)
    codes = [issue["code"] for issue in issues]
    if codes.count("diagnostic_detail_incomplete") != 2:
        raise AssertionError(f"missing detail completeness diagnostics: {issues!r}")
    if "rdma_ring_snapshot_unstable" not in codes:
        raise AssertionError(f"missing unstable RDMA ring diagnostic: {issues!r}")

    bad_rdma = snapshot_texts()
    append_netdiag_rows(bad_rdma, (
        "wki_block_proxy owner=0x0001 resource=8 generation=2 owner_boot=5 incarnation=6 "
        "ch=3 ch_gen=4 lifecycle_detail=1 io_detail=1 rdma=1 geometry_valid=0 indices_valid=1\n"
    ))
    allow = {"max": {}, "nonzero": [], "packet_delta": 0, "peer_disconnected": []}
    status, invariant_issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, bad_rdma), allow
    )
    assert_equal(status, "failed", "invalid live RDMA geometry")
    if not any(issue.get("code") == "rdma_ring_invalid" for issue in invariant_issues):
        raise AssertionError(f"missing invalid RDMA geometry issue: {invariant_issues!r}")

    duplicate = snapshot_texts()
    owner_row = (
        "wki_net_proxy owner=0x0001 resource=7 generation=2 ch=3 ch_gen=4 refs=1 "
        "detail_complete=1 op_pending=0 attach_pending=0 detach_pending=0\n"
    )
    append_netdiag_rows(duplicate, owner_row + owner_row)
    duplicate_allow = {
        "max": {},
        "nonzero": ["wki_net_proxy.refs"],
        "packet_delta": 0,
        "peer_disconnected": [],
    }
    status, invariant_issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, duplicate), duplicate_allow
    )
    assert_equal(status, "failed", "duplicate exact net owner")
    if not any(issue.get("code") == "duplicate_owner" for issue in invariant_issues):
        raise AssertionError(f"missing duplicate owner issue: {invariant_issues!r}")


def test_retired_block_proxy_tombstones_are_diagnostics_not_owners(module) -> None:
    tombstones = snapshot_texts()
    retired_row = (
        "wki_block_proxy owner=0x0001 resource=8 generation=2 owner_boot=0 incarnation=0 "
        "ch=56 ch_gen=0 lifecycle_detail=1 io_detail=1 active=0 fenced=1 published=1 "
        "epoch_reset=0 cleanup=0 resume_pending=0 resume_active=0 resume_after_detach=0 "
        "detach_confirmed=0 op_pending=0 op_id=3 op_seq=9 op_waiter=0 attach_pending=0 "
        "attach_waiter=0 binding_cookie=0 detach_pending=0 detach_cookie=0 rdma=0 roce=0 "
        "zone=0 data_slots=0x0 tags=0x0 bulk=0 bulk_max=0\n"
    )
    append_netdiag_rows(tombstones, retired_row + retired_row)
    allow = {"max": {}, "nonzero": [], "packet_delta": 0, "peer_disconnected": []}
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, tombstones), allow
    )
    assert_equal((status, issues), ("passed", []), "quiesced block lifetime tombstones")

    cleanup_owner = dict(tombstones)
    cleanup_owner["netdiag"] = cleanup_owner["netdiag"].replace("cleanup=0", "cleanup=1", 2)
    cleanup_allow = {
        "max": {},
        "nonzero": ["wki_block_proxy.cleanup"],
        "packet_delta": 0,
        "peer_disconnected": [],
    }
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, cleanup_owner), cleanup_allow
    )
    assert_equal(status, "failed", "cleanup-in-progress rows still own their identity")
    if not any(issue.get("code") == "duplicate_owner" for issue in issues):
        raise AssertionError(f"cleanup owners escaped duplicate detection: {issues!r}")


def test_retired_net_proxy_tombstones_are_diagnostics_not_owners(module) -> None:
    tombstones = snapshot_texts()
    retired_rows = (
        "wki_net_proxy owner=0x0001 resource=7 generation=2 ch=3 ch_gen=4 refs=0 "
        "active=0 attaching=0 registered=0 published=1 epoch_reset=0 cleanup_started=1 "
        "cleanup_complete=1 retiring=1 detail_complete=1 op_pending=0 op_id=0 op_seq=0 "
        "op_waiter=0 attach_pending=0 attach_waiter=0 attach_cookie=2 expected_cookie=0 "
        "peer_boot=9 detach_pending=0 detach_retry=0 detach_cookie=0 detach_boot=0 credits=64\n"
        "wki_net_proxy owner=0x0001 resource=7 generation=3 ch=5 ch_gen=6 refs=0 "
        "active=0 attaching=0 registered=0 published=1 epoch_reset=0 cleanup_started=1 "
        "cleanup_complete=1 retiring=1 detail_complete=1 op_pending=0 op_id=0 op_seq=0 "
        "op_waiter=0 attach_pending=0 attach_waiter=0 attach_cookie=3 expected_cookie=0 "
        "peer_boot=9 detach_pending=0 detach_retry=0 detach_cookie=0 detach_boot=0 credits=64\n"
    )
    append_netdiag_rows(tombstones, retired_rows)
    allow = {"max": {}, "nonzero": [], "packet_delta": 0, "peer_disconnected": []}
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, tombstones), allow
    )
    assert_equal((status, issues), ("passed", []), "quiesced NET lifetime tombstones")

    incomplete_cleanup = dict(tombstones)
    incomplete_cleanup["netdiag"] = incomplete_cleanup["netdiag"].replace(
        "cleanup_complete=1", "cleanup_complete=0"
    )
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, incomplete_cleanup), allow
    )
    assert_equal(status, "failed", "incomplete NET cleanup still owns its identity")
    if not any(issue.get("code") == "stale_successor_owner_conflict" for issue in issues):
        raise AssertionError(f"incomplete NET cleanup escaped successor detection: {issues!r}")


def test_ipc_and_compute_owner_identities_follow_proc_lifecycles(module) -> None:
    duplicate = snapshot_texts()
    append_netdiag_rows(duplicate, (
        "wki_ipc_diag kind=export resource=11 type=pipe peer=0x0002 ch=4 "
        "cleanup_epoch=9 active=1\n"
        "wki_ipc_diag kind=export resource=11 type=pipe peer=0x0002 ch=4 "
        "cleanup_epoch=9 active=0\n"
        "wki_ipc_diag kind=proxy resource=11 type=pipe peer=0x0002 ch=4 "
        "cleanup_epoch=9 active=1\n"
        "wki_ipc_diag kind=proxy resource=11 type=pipe peer=0x0002 ch=4 "
        "cleanup_epoch=9 active=0\n"
        "wki_ipc_diag kind=pending_delivery resource=11 peer=0x0002 cleanup_epoch=9 "
        "backlog_bytes=1\n"
        "wki_ipc_diag kind=pending_delivery resource=11 peer=0x0002 cleanup_epoch=9 "
        "backlog_bytes=2\n"
        "wki_compute_task kind=running task=31 peer=0x0002 ch_gen=7 session_epoch=9 "
        "active=1\n"
        "wki_compute_task kind=pending_complete task=31 peer=0x0002 ch_gen=7 "
        "session_epoch=9 active=1\n"
    ))
    allow = {
        "max": {},
        "nonzero": [],
        "packet_delta": 0,
        "peer_disconnected": [],
    }
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, duplicate), allow
    )
    assert_equal(status, "failed", "duplicate IPC and compute lifecycle owners")
    owner_kinds = {
        issue.get("ownerKind") for issue in issues if issue.get("code") == "duplicate_owner"
    }
    assert_equal(
        owner_kinds,
        {
            "wki_compute_task",
            "wki_ipc_diag:export",
            "wki_ipc_diag:pending_delivery",
            "wki_ipc_diag:proxy",
        },
        "exact ownership classes",
    )

    vfs_lanes = snapshot_texts()
    append_netdiag_rows(vfs_lanes, (
        "wki_server_binding consumer=0x0002 type=vfs resource=17 owner_boot=4 "
        "incarnation=8 cookie=3 ch=9 ch_gen=10 refs=0 rdma=0\n"
        "wki_server_binding consumer=0x0002 type=vfs resource=17 owner_boot=4 "
        "incarnation=8 cookie=4 ch=11 ch_gen=12 refs=0 rdma=0\n"
    ))
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, vfs_lanes), allow
    )
    assert_equal((status, issues), ("passed", []), "same-fence VFS server lanes")

    stale_vfs_lane = snapshot_texts()
    append_netdiag_rows(stale_vfs_lane, (
        "wki_server_binding consumer=0x0002 type=vfs resource=17 owner_boot=4 "
        "incarnation=8 cookie=3 ch=9 ch_gen=10 refs=0 rdma=0\n"
        "wki_server_binding consumer=0x0002 type=vfs resource=17 owner_boot=5 "
        "incarnation=9 cookie=4 ch=11 ch_gen=12 refs=0 rdma=0\n"
    ))
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, stale_vfs_lane), allow
    )
    assert_equal(status, "failed", "stale VFS resource fence")
    if not any(
        issue.get("code") == "stale_successor_owner_conflict"
        and issue.get("ownerKind") == "wki_server_binding"
        for issue in issues
    ):
        raise AssertionError(f"stale VFS resource fence was not reported: {issues!r}")

    stale_successor = snapshot_texts()
    append_netdiag_rows(stale_successor, (
        "wki_block_proxy owner=0x0002 resource=17 generation=1 owner_boot=4 "
        "incarnation=8 ch=9 ch_gen=10 lifecycle_detail=1 io_detail=1 rdma=0\n"
        "wki_block_proxy owner=0x0002 resource=17 generation=2 owner_boot=5 "
        "incarnation=9 ch=11 ch_gen=12 lifecycle_detail=1 io_detail=1 rdma=0\n"
        "wki_server_binding consumer=0x0002 type=block resource=17 owner_boot=4 "
        "incarnation=8 cookie=3 ch=9 ch_gen=10 refs=0 rdma=0\n"
        "wki_server_binding consumer=0x0002 type=block resource=17 owner_boot=5 "
        "incarnation=9 cookie=4 ch=11 ch_gen=12 refs=0 rdma=0\n"
        "wki_compute_task kind=running task=31 peer=0x0002 ch_gen=7 session_epoch=9 "
        "active=1\n"
        "wki_compute_task kind=running task=31 peer=0x0002 ch_gen=8 session_epoch=10 "
        "active=1\n"
        "wki_ipc_diag kind=proxy resource=11 type=pipe peer=0x0002 ch=4 "
        "cleanup_epoch=9 active=1\n"
        "wki_ipc_diag kind=proxy resource=11 type=pipe peer=0x0002 ch=4 "
        "cleanup_epoch=10 active=1\n"
        "wki_vfs_proxy owner=0x0002 res_id=23 res_gen=1 lane=0 active=1\n"
        "wki_vfs_proxy owner=0x0002 res_id=23 res_gen=2 lane=0 active=1\n"
        "wki_vfs_proxy owner=0x0002 res_id=23 res_gen=2 lane=1 active=1\n"
    ))
    status, issues = module.evaluate_invariants(
        parsed_snapshot(module), parsed_snapshot(module, stale_successor), allow
    )
    assert_equal(status, "failed", "stale and successor logical owners")
    conflicts = {
        issue.get("ownerKind")
        for issue in issues
        if issue.get("code") == "stale_successor_owner_conflict"
    }
    assert_equal(
        conflicts,
        {
            "wki_block_proxy",
            "wki_compute_task",
            "wki_ipc_diag:proxy",
            "wki_server_binding",
            "wki_vfs_proxy",
        },
        "stale/successor ownership classes",
    )
    vfs_conflicts = [
        issue
        for issue in issues
        if issue.get("code") == "stale_successor_owner_conflict"
        and issue.get("ownerKind") == "wki_vfs_proxy"
    ]
    assert_equal(len(vfs_conflicts), 1, "multi-lane VFS remains legitimate")


def make_fake_snapshot_transport(path: Path) -> None:
    texts = snapshot_texts()
    encoded = json.dumps(texts, sort_keys=True)
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import json,shlex,sys\n"
        f"texts=json.loads({encoded!r})\n"
        "argv=shlex.split(' '.join(sys.argv[2:]))\n"
        "if argv[:3]==['/usr/bin/wkictl','chaos-workload','clear']:\n"
        " print('wki_chaos_workload op=none status=0 detached=1')\n"
        " raise SystemExit(0)\n"
        "source=sys.argv[-1].strip(chr(39)).rsplit('/',1)[-1]\n"
        "if source not in texts: raise SystemExit(0)\n"
        "sys.stdout.write(texts[source])\n"
    )
    path.chmod(0o700)


def make_exec_transport(path: Path) -> None:
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import os,shlex,sys\n"
        "argv=shlex.split(' '.join(sys.argv[2:]))\n"
        "if argv[:3]==['/usr/bin/wkictl','chaos-workload','clear']:\n"
        " print('wki_chaos_workload op=none status=0 detached=1')\n"
        " raise SystemExit(0)\n"
        "if argv and argv[0]=='/usr/bin/wkictl': raise SystemExit(0)\n"
        "os.execvp(argv[0],argv)\n"
    )
    path.chmod(0o700)


def snapshot_commands() -> dict[str, list[str]]:
    return {
        "chaos": ["cat", "/proc/wki/chaos"],
        "kipcstat": ["cat", "/proc/kipcstat"],
        "netdiag": ["cat", "/proc/wki/netdiag"],
        "peers": ["cat", "/proc/wki/peers"],
        "pipes": ["cat", "/proc/wki/pipes"],
    }


def test_live_snapshot_convergence_and_artifacts(module) -> None:
    scenario = base_scenario(
        [
            {
                "id": "baseline",
                "phase": "setup",
                "reducible": False,
                "operation": {
                    "commands": snapshot_commands(),
                    "kind": "snapshot",
                    "label": "baseline",
                    "node": "0",
                    "timeout_ms": 2000,
                },
            },
            {
                "id": "converged",
                "phase": "convergence",
                "reducible": False,
                "operation": {
                    "allow": {},
                    "baseline": "baseline",
                    "commands": snapshot_commands(),
                    "deadline_ms": 10000,
                    "failure_code": "unit.invariant",
                    "interval_ms": 0,
                    "kind": "convergence",
                    "max_samples": 3,
                    "node": "0",
                    "timeout_ms": 2000,
                },
            },
        ]
    )
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        transport = root / "fake_ssh.py"
        make_fake_snapshot_transport(transport)
        output = root / "run"
        replay = module.execute_plan(module.make_plan(scenario, 7), output, ssh_script=transport)
        assert_equal(replay["outcome"], "passed", "first-class convergence outcome")
        assert_equal(replay["evidenceComplete"], True, "first-class convergence evidence")
        assert_equal(
            replay["events"][1]["observation"]["result"]["stableSamples"],
            2,
            "required consecutive stable snapshots",
        )
        manifest = module.load_json(output / "manifest.json")
        paths = {artifact["path"] for artifact in manifest["artifacts"]}
        for expected in (
            "replay.json",
            "telemetry/host.jsonl",
            "telemetry/node-0.jsonl",
            "wosdbg-batch.json",
            "snapshots/baseline/baseline/netdiag.txt",
            "snapshots/converged/sample-02/parsed.json",
        ):
            if expected not in paths:
                raise AssertionError(f"manifest is missing {expected!r}")
        batch = module.load_json(output / "wosdbg-batch.json")
        loaded_paths = {
            call["arguments"]["path"]
            for call in batch["calls"]
            if call["tool"] == "load_log"
        }
        assert_equal(
            loaded_paths,
            {"telemetry/host.jsonl", "telemetry/node-0.jsonl"},
            "WOSDBG batch lanes",
        )
        try:
            module.execute_plan(module.make_plan(scenario, 7), output, ssh_script=transport)
        except module.ChaosInputError:
            pass
        else:
            raise AssertionError("runner overwrote an existing artifact directory")


def test_telemetry_records_are_bounded_for_wosdbg(module) -> None:
    record = module.telemetry_record(
        run_id="sha256:" + "1" * 64,
        scenario="unit",
        kind="wki.chaos.guest_poll",
        payload={
            "evidenceComplete": True,
            "operation": {"kind": "guest_poll", "node": "0", "role": "invariant"},
            "result": {
                "attempts": [{"stdout": "x" * module.MAX_TELEMETRY_LINE_BYTES}],
                "attemptsUsed": 1,
                "deadlineExpired": False,
            },
            "status": "passed",
        },
        node={"id": "0"},
    )
    original = module.canonical_json_bytes(record)
    if len(original) <= module.MAX_TELEMETRY_LINE_BYTES:
        raise AssertionError("telemetry fixture did not exceed the WOSDBG line bound")
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        writer = module.TelemetryWriter(output)
        writer.append("node-0", record)
        writer.close()
        encoded = (output / "telemetry" / "node-0.jsonl").read_bytes()
    if len(encoded) > module.MAX_TELEMETRY_LINE_BYTES:
        raise AssertionError("compacted telemetry exceeds the WOSDBG line bound")
    compact = json.loads(encoded)
    assert_equal(compact["payload"]["status"], "passed", "compacted event status")
    assert_equal(
        compact["payload"]["resultSummary"]["collections"]["attempts"],
        {"items": 1, "type": "list"},
        "compacted poll attempt count",
    )
    assert_equal(
        compact["payload"]["telemetryCompaction"]["originalSha256"],
        "sha256:" + module.hashlib.sha256(original).hexdigest(),
        "compacted record digest",
    )


def test_convergence_uses_bounded_samples_until_invariants_pass(module) -> None:
    baseline = parsed_snapshot(module)
    transient_texts = snapshot_texts()
    transient_texts["netdiag"] = transient_texts["netdiag"].replace(
        "free=32 used=0", "free=31 used=1"
    )
    transient = parsed_snapshot(module, transient_texts)
    clean = parsed_snapshot(module)
    allow = module._validate_invariant_allowlist({}, "allow")
    operation = {
        "allow": allow,
        "baseline": "baseline",
        "deadline_ms": 10_000,
        "interval_ms": 0,
        "max_samples": 4,
    }

    assert_equal(
        module._convergence_sample_interval_ms(
            {"deadline_ms": 10_000, "max_samples": 4}
        ),
        2500,
        "default convergence cadence spans the declared deadline",
    )
    assert_equal(
        module._convergence_sample_interval_ms(operation),
        0,
        "explicit convergence cadence is preserved",
    )

    def run_with(captures):
        pending = list(captures)
        original = module._capture_snapshot

        def capture(**_kwargs):
            value = pending.pop(0)
            return value, {"complete": True, "sample": len(captures) - len(pending)}

        module._capture_snapshot = capture
        try:
            return module._execute_convergence(
                {"id": "converge"},
                operation,
                {},
                Path("unused"),
                Path("unused-ssh"),
                {"baseline": baseline},
            )
        finally:
            module._capture_snapshot = original

    result, status, complete = run_with([transient, transient, clean, clean])
    assert_equal((status, complete), ("passed", True), "bounded convergence recovery")
    assert_equal(len(result["samples"]), 4, "failed stable pair does not stop convergence")

    result, status, complete = run_with([transient] * 4)
    assert_equal((status, complete), ("failed", True), "persistent invariant violation")
    if not any(issue.get("code") == "packet_pool_delta" for issue in result["issues"]):
        raise AssertionError(f"persistent packet delta lost its exact evidence: {result!r}")


def test_async_guest_lifecycle_runs_faults_in_flight_and_forces_cleanup(module) -> None:
    async_events = [
        {
            "group": "job",
            "id": "start",
            "phase": "workload",
            "operation": {
                "argv": [
                    sys.executable,
                    "-c",
                    "import time;print('ready',flush=True);time.sleep(.05);raise SystemExit(3)",
                ],
                "kind": "guest_start",
                "lifetime_ms": 2000,
                "node": "0",
                "process_id": "work",
            },
        },
        {
            "id": "fault",
            "phase": "fault",
            "operation": {"kind": "barrier", "label": "fault-while-running"},
        },
        {
            "group": "job",
            "id": "wait",
            "phase": "heal",
            "operation": {
                "expect_exit": [3],
                "kind": "guest_wait",
                "node": "0",
                "process_id": "work",
                "timeout_ms": 2000,
            },
        },
    ]
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        transport = root / "exec_ssh.py"
        make_exec_transport(transport)
        replay = module.execute_plan(
            module.make_plan(base_scenario(async_events), 9),
            root / "async",
            ssh_script=transport,
        )
        assert_equal(replay["outcome"], "passed", "async in-flight workload")
        wait_result = replay["events"][2]["observation"]["result"]["process"]
        assert_equal(wait_result["exitCode"], 3, "async expected exit")
        assert_equal(wait_result["stdout"], "ready\n", "async bounded stdout")

        marker = root / "cleaned"
        cleanup_events = [
            {
                "group": "job",
                "id": "start",
                "phase": "workload",
                "operation": {
                    "argv": [sys.executable, "-c", "import time;time.sleep(10)"],
                    "cleanup_argv": [
                        sys.executable,
                        "-c",
                        f"from pathlib import Path;Path({str(marker)!r}).write_text('yes')",
                    ],
                    "kind": "guest_start",
                    "lifetime_ms": 10000,
                    "node": "0",
                    "process_id": "work",
                },
            },
            {
                "id": "failure",
                "phase": "fault",
                "operation": guest_operation([sys.executable, "-c", "raise SystemExit(7)"]),
            },
            {
                "group": "job",
                "id": "wait",
                "phase": "heal",
                "operation": {
                    "kind": "guest_wait",
                    "node": "0",
                    "process_id": "work",
                    "timeout_ms": 1000,
                },
            },
        ]
        failed = module.execute_plan(
            module.make_plan(base_scenario(cleanup_events), 10),
            root / "cleanup",
            ssh_script=transport,
        )
        assert_equal(failed["outcome"], "failed", "async early-stop outcome")
        assert_equal(
            failed["failureSignature"]["object"],
            "failure",
            "final cleanup does not erase the first failure",
        )
        assert_equal(failed["events"][2]["observation"]["status"], "not-run", "required wait after stop")
        forced = failed["events"][0]["observation"]["result"]["forcedCleanup"]
        assert_equal(forced["cleanupSucceeded"], True, "async cleanup command")
        assert_equal(marker.read_text(), "yes", "async remote cleanup effect")
        cleanup_argv = [
            step["argv"]
            for node in failed["cleanup"]
            for step in node["steps"]
        ]
        assert_equal(
            cleanup_argv,
            [
                ["/usr/bin/wkictl", "chaos-workload", "clear"],
                ["/usr/bin/wkictl", "chaos", "clear"],
            ],
            "unconditional workload and injector cleanup",
        )
        assert_equal(
            failed["cleanup"][0]["steps"][0]["result"]["attempts"][0]["probe"]["stdoutRow"]["matched"],
            True,
            "final workload cleanup exact detached row",
        )


def test_async_guest_schema_rejects_dangling_duplicate_and_split_groups(module) -> None:
    start = {
        "group": "job",
        "id": "start",
        "phase": "workload",
        "operation": {
            "argv": ["worker"],
            "kind": "guest_start",
            "lifetime_ms": 1000,
            "node": "0",
            "process_id": "work",
        },
    }
    cases = [
        [start],
        [start, {**start, "id": "again"}],
        [
            start,
            {
                "group": "different",
                "id": "wait",
                "phase": "heal",
                "operation": {
                    "kind": "guest_wait",
                    "node": "0",
                    "process_id": "work",
                    "timeout_ms": 1000,
                },
            },
        ],
    ]
    for events in cases:
        try:
            module.make_plan(base_scenario(events), 1)
        except module.ChaosInputError:
            continue
        raise AssertionError(f"invalid async lifecycle was accepted: {events!r}")


def test_truncated_guest_output_makes_run_incomplete(module) -> None:
    events = [
        {
            "id": "bounded-output",
            "phase": "workload",
            "operation": guest_operation(
                [sys.executable, "-c", "print('x' * 70000)"]
            ),
        }
    ]
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        transport = root / "exec_ssh.py"
        make_exec_transport(transport)
        replay = module.execute_plan(
            module.make_plan(base_scenario(events), 11),
            root / "truncated",
            ssh_script=transport,
        )
        assert_equal(replay["outcome"], "incomplete", "truncated run outcome")
        assert_equal(replay["evidenceComplete"], False, "truncated run evidence")
        assert_equal(
            replay["events"][0]["observation"]["status"],
            "incomplete",
            "truncated event status",
        )
        module.validate_replay(replay)


def test_qmp_client_records_interleaved_events(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        socket_path = Path(temporary) / "qmp.sock"
        ready = threading.Event()
        requests: list[dict] = []

        def server() -> None:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(socket_path))
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection, connection.makefile("rwb", buffering=0) as stream:
                    stream.write(b'{"QMP":{"version":{"qemu":{"major":1,"minor":0,"micro":0}},"capabilities":[]}}\r\n')
                    for index in range(2):
                        request = json.loads(stream.readline())
                        requests.append(request)
                        if index == 1:
                            stream.write(b'{"event":"RESET","data":{"reason":"unit"}}\r\n')
                        stream.write(json.dumps({"return": {}, "id": request["id"]}).encode() + b"\r\n")

        thread = threading.Thread(target=server)
        thread.start()
        ready.wait(2)
        with module.QmpClient(socket_path, 2.0) as client:
            response = client.execute("set_link", {"name": "net1", "up": False})
            events, discarded = client.drain_events()
        thread.join(2)
        assert_equal(response.get("return"), {}, "QMP command response")
        assert_equal(requests[1]["execute"], "set_link", "QMP command name")
        assert_equal(requests[1]["arguments"], {"name": "net1", "up": False}, "QMP arguments")
        assert_equal(events[0]["event"], "RESET", "interleaved QMP event")
        assert_equal(discarded, 0, "QMP event overflow")


test_qmp_client_records_interleaved_events.requires_unix_socket = True


def write_lane_result(module, path: Path, scenario: str, lane: str, outcome: str = "passed") -> None:
    evidence_path = path.parent / "evidence.txt"
    evidence_path.write_text(f"{scenario}/{lane}/{outcome}\n")
    data = evidence_path.read_bytes()
    passed = outcome == "passed"
    inventory = [
        {
            "path": "evidence.txt",
            "sha256": "sha256:" + module.hashlib.sha256(data).hexdigest(),
            "size": len(data),
        }
    ]
    if lane in {"host-model", "ktest-model"}:
        check = {
            "evidenceComplete": passed,
            "executionComplete": passed,
            "id": "unit-evidence",
            "passed": passed,
        }
        if lane == "host-model":
            check.update(
                {
                    "argv": ["unit-evidence"],
                    "exitCode": 0 if passed else 1,
                    "stderrPath": "evidence.txt",
                    "stdoutPath": "evidence.txt",
                    "timedOut": False,
                }
            )
        else:
            check["serialPath"] = "evidence.txt"
        catalog = module.validate_scenario_catalog(
            module.load_json(CATALOG), module.load_json(MATRIX)
        )
        result = {
            "catalogDigest": module.canonical_digest(catalog),
            "checks": [check],
            "evidence": inventory,
            "evidenceComplete": passed,
            "executionComplete": passed,
            "format": "wos.wki-chaos-lane-result",
            "generator": {"name": "wos-wki-chaos", "version": 1},
            "lane": lane,
            "outcome": outcome,
            "scenario": scenario,
            "version": 1,
        }
    else:
        result = {
            "artifacts": inventory,
            "completion": {
                "evidenceComplete": passed,
                "executionComplete": passed,
                "outcome": outcome,
            },
            "format": "wos.wki-chaos-run",
            "scenario": {"name": scenario},
            "version": 1,
        }
    path.write_bytes(module.canonical_json_bytes(result))


def test_catalog_readiness_and_real_ktest_evidence(module) -> None:
    matrix = module.load_json(MATRIX)
    catalog = module.validate_scenario_catalog(module.load_json(CATALOG), matrix)
    assert_equal(
        [entry["id"] for entry in catalog["scenarios"]],
        ["C1", "C2", "V1", "V2", "I1", "I2", "N1", "B1", "B2", "X1"],
        "scenario catalog IDs",
    )
    executions = {entry["id"]: entry["execution"] for entry in catalog["scenarios"]}
    live_ids = {"C1", "C2", "V1", "V2", "I1", "I2", "N1", "B1", "B2", "X1"}
    if any(
        execution != ("live-ready" if scenario_id in live_ids else "model-only")
        for scenario_id, execution in executions.items()
    ):
        raise AssertionError(f"catalog readiness set is wrong: {executions!r}")
    report = module.scenario_readiness_report(catalog)
    readiness = {item["id"]: item for item in report["scenarios"]}
    if any(
        (not item["liveReady"] or item["missingAdapters"])
        for scenario_id, item in readiness.items()
        if scenario_id in live_ids
    ):
        raise AssertionError(f"direct workloads are not materially live-ready: {report!r}")
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        try:
            module.materialize_catalog_scenario(catalog, "unknown", root / "unknown.json")
        except module.ChaosInputError as exc:
            if "does not contain" not in str(exc):
                raise AssertionError(f"materializer omitted the unknown ID: {exc}") from exc
        else:
            raise AssertionError("materializer accepted an unknown scenario")

        fixed_seeds = {
            scenario["id"]: int(scenario["fixed_seed"], 0)
            for scenario in module.validate_matrix(matrix)["scenarios"]
        }
        for scenario_id in sorted(live_ids):
            path = root / f"{scenario_id}.json"
            scenario = module.materialize_catalog_scenario(catalog, scenario_id, path)
            assert_equal(scenario["name"], scenario_id, "materialized live scenario")
            assert_equal(module.load_json(path), scenario, "canonical materialized scenario")
            plan = module.make_plan(scenario, fixed_seeds[scenario_id])
            replay = module.execute_plan(plan, root / f"dry-{scenario_id}", dry_run=True)
            assert_equal(replay["outcome"], "dry-run", f"{scenario_id} dry-run")

        c1 = next(entry for entry in catalog["scenarios"] if entry["id"] == "C1")
        serial = root / "serial.log"
        serial.write_text(
            "[0.001] info ktest: === WOS Kernel Self-Test Suite ===\n"
            + "".join(
                f"[0.002] info ktest: PASS  {marker}\n"
                for marker in c1["ktest_passes"]
            )
            + "[0.003] info ktest: === 3 passed, 0 failed ===\n"
        )
        lane_dir = root / "C1" / "ktest-model"
        result = module.qualify_ktest_model(catalog, "C1", lane_dir, serial)
        assert_equal(result["outcome"], "passed", "KTEST evidence qualification")
        loaded = module._load_matrix_lane_result(
            lane_dir / "result.json", "C1", "ktest-model"
        )
        assert_equal(loaded, ("passed", True, True, None), "qualified lane validation")
        (lane_dir / "evidence" / "serial.log").write_text("tampered\n")
        loaded = module._load_matrix_lane_result(
            lane_dir / "result.json", "C1", "ktest-model"
        )
        assert_equal(loaded[0], "incomplete", "tampered lane evidence")

        stale_serial = root / "stale-serial.log"
        stale_serial.write_text(
            serial.read_text()
            + "[1.001] info ktest: === WOS Kernel Self-Test Suite ===\n"
            + f"[1.002] info ktest: RUN   {c1['ktest_passes'][0]}\n"
        )
        stale_lane = root / "C1-stale" / "ktest-model"
        stale_result = module.qualify_ktest_model(
            catalog, "C1", stale_lane, stale_serial
        )
        assert_equal(
            stale_result["outcome"], "incomplete", "newer partial KTEST run"
        )


def test_i1_adapter_and_live_scenario_contract(module) -> None:
    source = I1_WORKLOAD.read_text()
    required_source = [
        'constexpr mode_t MODE_0600 = S_IRUSR | S_IWUSR',
        'constexpr mode_t MODE_0700 = S_IRUSR | S_IWUSR | S_IXUSR',
        'open(path, O_WRONLY | O_CREAT | O_EXCL, MODE_0600)',
        'clock_gettime(CLOCK_MONOTONIC, &now)',
        'ker::process::WKI_TARGET_FLAG_STRICT',
        'execve(executable.data(), arguments.data(), nullptr)',
        '"pipe-open", "before-data", "data-issued", "before-close", "close-issued", "before-discard", "reader-discarded"',
        'write_new_file(advance.data(), CONTENT.data(), CONTENT.size())',
        'constexpr std::array<uint8_t, 3> ACKS{0xB1, 0xB2, 0xB3}',
        'received == expected',
        'require_eof_timeout(options.data_fd, DEADLINE)',
        'errno == EPIPE',
        'prefix.fill(0xD1)',
        'constexpr std::string_view CANCEL = "cancel\\n"',
        'for (const char* name : {"state", "done", "cancel", "metadata"})',
    ]
    missing = [token for token in required_source if token not in source]
    if missing:
        raise AssertionError(f"I1 workload contract is missing {missing!r}")
    for forbidden in ("system(", "popen(", '"/bin/sh"', '"sh -c"'):
        if forbidden in source:
            raise AssertionError(f"I1 workload uses forbidden shell execution: {forbidden}")

    matrix = module.validate_matrix(module.load_json(MATRIX))
    catalog = module.validate_scenario_catalog(module.load_json(CATALOG), matrix)
    i1_entry = next(entry for entry in catalog["scenarios"] if entry["id"] == "I1")
    scenario = i1_entry["live_scenario"]
    if scenario is None:
        raise AssertionError("I1 live scenario is missing")
    plan = module.make_plan(scenario, int("1101000000000001", 16))
    ids = [event["id"] for event in plan["events"]]
    required_order = [
        "drop-data-rule",
        "duplicate-data-rule",
        "advance-before-data",
        "wait-data-issued",
        "drop-data-matched",
        "duplicate-data-matched",
        "reorder-data-rule",
        "advance-data-issued",
        "reorder-data-matched",
        "reorder-data-release",
        "wait-before-close",
        "delay-close-rule",
        "advance-before-close",
        "wait-close-issued",
        "delay-close-matched",
        "delay-close-release",
        "advance-close-issued",
        "wait-before-discard",
        "reader-close-drop-rule",
        "advance-before-discard",
        "wait-reader-discarded",
        "reader-close-drop-matched",
        "advance-reader-discarded",
        "workload-finish",
        "fault-evidence-node0",
        "fault-evidence-node1",
        "clear-node0-final",
    ]
    positions = [ids.index(event_id) for event_id in required_order]
    if positions != sorted(positions):
        raise AssertionError(f"I1 checkpoint/fault order is not causal: {positions!r}")
    grouped = [
        event
        for event in plan["events"]
        if event["group"] == "i1-pipe-data-close"
    ]
    if not grouped or any(not event["reducible"] for event in grouped):
        raise AssertionError("I1 fault/workload events must be one reducer-atomic group")
    start = next(event for event in plan["events"] if event["id"] == "workload-start")
    finish = next(event for event in plan["events"] if event["id"] == "workload-finish")
    assert_equal(start["operation"]["process_id"], "i1-worker", "I1 async start")
    assert_equal(finish["operation"]["process_id"], "i1-worker", "I1 async wait")
    if "cleanup_argv" not in start["operation"]:
        raise AssertionError("I1 async workload lacks bounded cancellation cleanup")
    wait_events = {
        event["id"]: event["operation"]["argv"]
        for event in plan["events"]
        if event["id"].endswith("-matched")
    }
    for event_id, matched in {
        "drop-data-matched": "applied=1",
        "duplicate-data-matched": "applied=1",
        "reorder-data-matched": "applied=2",
        "delay-close-matched": "applied=1",
        "reader-close-drop-matched": "applied=1",
    }.items():
        argv = wait_events[event_id]
        if argv[:3] != ["/usr/bin/wkictl", "chaos", "wait"] or matched not in argv:
            raise AssertionError(f"{event_id} lacks its exact injector barrier: {argv!r}")

    expected_rule_ops = {
        "reader-close-drop-rule": "op=0x0704",
        "drop-data-rule": "op=0x0702",
        "duplicate-data-rule": "op=0x0702",
        "reorder-data-rule": "op=0x0702",
        "delay-close-rule": "op=0x0701",
    }
    for event_id, expected_op in expected_rule_ops.items():
        event = next(event for event in plan["events"] if event["id"] == event_id)
        argv = event["operation"]["argv"]
        actual_ops = [argument for argument in argv if argument.startswith("op=")]
        assert_equal(actual_ops, [expected_op], f"{event_id} exact operation selector")

    expected_fault_ops = {
        "101": "0x0702",
        "102": "0x0702",
        "103": "0x0702",
        "104": "0x0701",
        "201": "0x0704",
    }
    for event in plan["events"]:
        fault = event.get("fault")
        if fault is None or fault.get("rule_id") not in expected_fault_ops:
            continue
        assert_equal(
            fault.get("op"),
            expected_fault_ops[fault["rule_id"]],
            f"{event['id']} fault operation metadata",
        )
    reader_close = next(event for event in plan["events"] if event["id"] == "reader-close-drop-rule")
    assert_equal(reader_close["operation"]["node"], "1", "I1 reader-close injector node")
    if "after=1" in reader_close["operation"]["argv"]:
        raise AssertionError("I1 reader-close rule must drop the first flow-controlled DATA frame")
    reader_close_wait = next(event for event in plan["events"] if event["id"] == "reader-close-drop-matched")
    assert_equal(reader_close_wait["operation"]["node"], "1", "I1 reader-close barrier node")
    discard_advance = next(event for event in plan["events"] if event["id"] == "advance-before-discard")
    assert_equal(discard_advance["operation"]["node"], "0", "I1 discard checkpoint owner")


def test_direct_live_scenario_order_seed_and_evidence_contracts(module) -> None:
    matrix = module.validate_matrix(module.load_json(MATRIX))
    catalog = module.validate_scenario_catalog(module.load_json(CATALOG), matrix)
    live_ids = {"C1", "C2", "V1", "V2", "I1", "I2"}
    fixed_seeds = {item["id"]: int(item["fixed_seed"], 0) for item in matrix["scenarios"]}
    expected_rules = {
        "C1": {"1101": ("tx", "84", None)},
        "C2": {"1201": ("rx", "83", None)},
        "V1": {
            "1301": ("rx", "68", "0x0411"),
            "1302": ("tx", "67", "0x0411"),
            "1303": ("rx", "68", "0x0415"),
            "1304": ("rx", "68", "0x040B"),
            "1305": ("tx", "67", "0x0403"),
        },
        "V2": {
            "1401": ("tx", "67", "0x0411"),
            "1402": ("tx", "67", "0x0403"),
        },
        "I2": {"1501": ("rx", "67", "0x0753")},
    }
    expected_rule_channels = {
        "C1": "3",
        "C2": "3",
        "V1": "dynamic",
        "V2": "dynamic",
        "I2": "3",
    }
    required_order = {
        "C1": [
            "compute-publish-hold", "workload-start", "wait-submit-issued",
            "wait-submit-worker", "advance-submit-issued", "wait-cancel-issued",
            "wait-applied-1101", "compute-publish-release", "advance-cancel-issued",
            "wait-cancel-complete", "workload-finish",
        ],
        "C2": [
            "wait-exit-issued", "wait-applied-1201", "reset-node1",
            "node1-ssh-ready", "peer-epoch-ready", "advance-exit-issued",
            "wait-successor-running", "release-1201", "advance-successor-running",
            "wait-successor-complete", "workload-finish",
        ],
        "V1": [
            "wait-before-write", "rule-1301", "advance-before-write",
            "wait-applied-1301", "release-1301", "wait-write-done",
            "wait-before-utimens", "wait-applied-1303", "release-1303",
            "wait-utimens-done", "wait-before-rename", "wait-applied-1304",
            "release-1304", "wait-rename-done", "wait-before-close",
            "wait-applied-1305", "wait-close-done", "workload-finish",
        ],
        "V2": [
            "wait-lanes-open", "advance-lanes-open", "wait-applied-1401",
            "heal-1401", "wait-operations-done", "wait-before-close",
            "wait-applied-1402", "held-close-evidence", "release-1402",
            "wait-files-closed", "workload-finish",
        ],
        "I2": [
            "wait-control-issued", "wait-applied-1501", "reset-node1",
            "node1-ssh-ready", "peer-epoch-ready", "advance-control-issued",
            "wait-old-session-fenced", "advance-old-session-fenced",
            "wait-successor-issued", "release-1501", "advance-successor-issued",
            "wait-successor-complete", "workload-finish",
        ],
    }
    for entry in catalog["scenarios"]:
        if entry["id"] not in live_ids:
            continue
        scenario = entry["live_scenario"]
        if scenario is None:
            raise AssertionError(f"{entry['id']} lacks a live scenario")
        raw_enables = [
            event["operation"]["argv"]
            for event in scenario["events"]
            if event["id"].startswith("enable-node")
        ]
        if raw_enables != [module.PLAN_SEED_ENABLE_ARGV, module.PLAN_SEED_ENABLE_ARGV]:
            raise AssertionError(f"{entry['id']} does not use the plan-seed placeholder")
        seed = fixed_seeds[entry["id"]]
        plan = module.make_plan(scenario, seed)
        for event in plan["events"]:
            argv = event["operation"].get("argv", [])
            if "--deadline-ms" in argv:
                raise AssertionError(f"{entry['id']} passes an unsupported workload flag: {event['id']}")
        assert_equal(
            module.canonical_json_bytes(plan),
            module.canonical_json_bytes(module.make_plan(scenario, seed)),
            f"{entry['id']} fixed-seed byte stability",
        )
        other = module.make_plan(scenario, seed ^ 1)
        if other["planDigest"] == plan["planDigest"]:
            raise AssertionError(f"{entry['id']} seed did not change its plan")
        enable_argv = [
            event["operation"]["argv"]
            for event in plan["events"]
            if event["id"].startswith("enable-node")
        ]
        expected_enable = ["/usr/bin/wkictl", "chaos", "enable", f"seed={seed}"]
        if enable_argv != [expected_enable, expected_enable]:
            raise AssertionError(f"{entry['id']} plan seed was not injected canonically")
        ids = [event["id"] for event in plan["events"]]
        ordered = [ids.index(event_id) for event_id in required_order.get(entry["id"], [])]
        if ordered != sorted(ordered):
            raise AssertionError(f"{entry['id']} event ordering is not causal: {ordered!r}")
        scenario_groups = {
            event["group"]
            for event in plan["events"]
            if event["reducible"]
        }
        if len(scenario_groups) != 1:
            raise AssertionError(f"{entry['id']} is not one reducer-atomic scenario group")
        evidence = ids.index("fault-evidence-node1")
        clear = ids.index("clear-node0-final")
        converge = ids.index("converge-node0")
        if not evidence < clear < converge:
            raise AssertionError(f"{entry['id']} lacks evidence-before-clear convergence ordering")
        if entry["id"] in {"C2", "I2"}:
            convergence = next(
                event for event in plan["events"] if event["id"] == "converge-node0"
            )
            assert_equal(
                convergence["operation"]["allow"].get("peer_replaced"),
                ["wos-1"],
                f"{entry['id']} exact rebooted-peer retirement proof",
            )

        expected = expected_rules.get(entry["id"], {})
        for rule_id, (direction, message_type, op) in expected.items():
            rule_event = next(event for event in plan["events"] if event["id"] == f"rule-{rule_id}")
            argv = rule_event["operation"]["argv"]
            for required in (
                f"dir={direction}", f"type={message_type}",
                "neighbor_host=wos-1",
            ):
                if required not in argv:
                    raise AssertionError(f"{entry['id']} rule {rule_id} lacks {required!r}")
            if any(token.startswith(("neighbor=", "src=", "dst=")) for token in argv):
                raise AssertionError(f"{entry['id']} rule {rule_id} assumes VM ordinals are WKI IDs")
            actual_ops = [token.removeprefix("op=") for token in argv if token.startswith("op=")]
            assert_equal(actual_ops, [] if op is None else [op], f"{entry['id']} rule {rule_id} op")
            expected_channel = expected_rule_channels[entry["id"]]
            actual_channels = [
                token.removeprefix("channel=") for token in argv if token.startswith("channel=")
            ]
            assert_equal(
                actual_channels,
                [] if expected_channel == "dynamic" else [expected_channel],
                f"{entry['id']} rule {rule_id} channel selector",
            )
            assert_equal(
                rule_event["fault"].get("channel"),
                expected_channel,
                f"{entry['id']} rule {rule_id} channel metadata",
            )
            wait = next(event for event in plan["events"] if event["id"] == f"wait-applied-{rule_id}")
            if not any(token.startswith("applied=") for token in wait["operation"]["argv"]):
                raise AssertionError(f"{entry['id']} rule {rule_id} lacks an applied barrier")

        if entry["id"] == "I2":
            workload = next(event for event in plan["events"] if event["id"] == "workload-start")["operation"]
            assert_equal(workload["argv"][-2:], ["--timeout-ms", "160000"], "I2 reset/recovery workload deadline")
            assert_equal(workload["lifetime_ms"], 220000, "I2 async lifetime")

    invalid = base_scenario(
        [{"id": "bad-seed", "phase": "setup", "operation": guest_operation(["tool", "seed=$PLAN_SEED"])}]
    )
    try:
        module.make_plan(invalid, 1)
    except module.ChaosInputError:
        pass
    else:
        raise AssertionError("plan seed placeholder escaped the exact chaos-enable command")


def test_direct_service_workloads_are_bounded_and_checkpointed(module) -> None:
    source = I1_WORKLOAD.read_text()
    normalized_source = " ".join(source.split())
    required = [
        'constexpr std::array<const char*, 3> C1_CHECKPOINTS{"submit-issued", "cancel-issued", "cancel-complete"}',
        'constexpr std::array<const char*, 4> C2_CHECKPOINTS{"task-running", "exit-issued", "successor-running", "successor-complete"}',
        '"file-open",      "before-write",  "write-done",   "before-utimens", "utimens-done"',
        'constexpr std::array<const char*, 4> V2_CHECKPOINTS{"lanes-open", "operations-done", "before-close", "files-closed"}',
        'constexpr std::array<const char*, 4> I2_CHECKPOINTS{"control-issued", "old-session-fenced", "successor-issued", "successor-complete"}',
        'format_path(out, "/wki/%s/tmp/wki-chaos-%s-%s-%s"',
        'constexpr std::size_t V2_LANE_COUNT = 8',
        'utimensat(AT_FDCWD, original.data(), TIMES.data(), 0)',
        'getpeername(options.data_fd, reinterpret_cast<sockaddr*>(&peer), &peer_length)',
        'spawn_i1_helper(options.target, "I2-socket-getpeername", sockets[0], sockets[1], sockets[1], -1, options.timeout_ms)',
        'spawn_remote_helper(options.target, mode, command->at(0), ready->at(1), command->at(1), ready->at(0), options.timeout_ms)',
        'WTERMSIG(status) != SIGTERM',
        'checkpoint_list(context.scenario).size()',
    ]
    missing = [token for token in required if " ".join(token.split()) not in normalized_source]
    if missing:
        raise AssertionError(f"direct workload contract is missing {missing!r}")
    if "I2-epoll-ctl" in source:
        raise AssertionError("I2 must not target an epoll-owning task that placement policy pins locally")
    for forbidden in ("system(", "popen(", '"/bin/sh"', '"sh -c"'):
        if forbidden in source:
            raise AssertionError(f"direct workload uses forbidden shell execution: {forbidden}")


def test_service_scenarios_use_exact_adapters_and_evidence(module) -> None:
    matrix = module.validate_matrix(module.load_json(MATRIX))
    catalog = module.validate_scenario_catalog(module.load_json(CATALOG), matrix)
    seeds = {entry["id"]: int(entry["fixed_seed"]) for entry in matrix["scenarios"]}
    plans = {}
    for scenario_id in ("N1", "B1", "B2", "X1"):
        entry = next(item for item in catalog["scenarios"] if item["id"] == scenario_id)
        scenario = entry["live_scenario"]
        if scenario is None:
            raise AssertionError(f"{scenario_id} lacks a live scenario")
        plan = module.make_plan(scenario, seeds[scenario_id])
        plans[scenario_id] = plan
        node_count = len(plan["nodes"])
        setup_clears = [
            event
            for event in plan["events"]
            if event["id"].startswith("workload-clear-node")
            and not event["id"].endswith("-final")
        ]
        assert_equal(len(setup_clears), node_count, f"{scenario_id} setup workload clears")
        for event in setup_clears:
            operation = event["operation"]
            assert_equal(
                operation["argv"],
                ["/usr/bin/wkictl", "chaos-workload", "clear"],
                f"{scenario_id} exact setup workload clear",
            )
            assert_equal(
                operation["stdout_row"]["equals"],
                {"detached": "1", "op": "none", "status": "0"},
                f"{scenario_id} setup clear result proof",
            )

    n1 = {event["id"]: event for event in plans["N1"]["events"]}
    n1_rule = n1["rule-1601"]["operation"]["argv"]
    for token in (
        "neighbor_host=wos-1", "src_host=wos-0", "dst_host=wos-2",
        "type=67", "op=0x0303", "after=0", "every=2", "limit=2",
    ):
        if token not in n1_rule:
            raise AssertionError(f"N1 periodic rule lacks {token!r}")
    if "applied=2" not in n1["wait-applied-1601"]["operation"]["argv"]:
        raise AssertionError("N1 lacks its exact periodic applied barrier")
    assert_equal(
        n1["link-down-node1-wki-b"]["operation"]["arguments"],
        {"name": "net2", "up": False},
        "N1 selected routed wki-b link down",
    )
    assert_equal(
        n1["link-up-node1-wki-b"]["operation"]["arguments"],
        {"name": "net2", "up": True},
        "N1 selected routed wki-b link up",
    )

    b1 = {event["id"]: event for event in plans["B1"]["events"]}
    b1_rule = b1["rule-1701"]["operation"]["argv"]
    for token in (
        "type=67", "op=0x0101", "corrupt=payload", "payload_offset=12",
        "payload_xor=0x01", "limit=1",
    ):
        if token not in b1_rule:
            raise AssertionError(f"B1 exact WRITE corruption lacks {token!r}")
    if any(token.startswith("occurrence=") for token in b1_rule):
        raise AssertionError("B1 must select the first matching WRITE rather than the enclosing DEV_OP_REQ stream occurrence")
    b1_row = b1["block-safe-rejection"]["operation"]["stdout_row"]
    assert_equal(b1_row["negative"], ["write_status"], "B1 rejected WRITE proof")
    assert_equal(b1_row["equals"]["rdma"], "0", "B1 message-only proof")
    if "applied=1" not in b1["wait-applied-1701"]["operation"]["argv"]:
        raise AssertionError("B1 lacks applied=1")

    b2 = {event["id"]: event for event in plans["B2"]["events"]}
    sqe = b2["rule-1801"]["operation"]["argv"]
    doorbell = b2["rule-1802"]["operation"]["argv"]
    for token in ("surface=blk_sqe", "action=corrupt", "lane=*", "blk_op=1", "sqe_offset=16", "sqe_xor=0x01"):
        if token not in sqe:
            raise AssertionError(f"B2 SQE rule lacks {token!r}")
    for token in ("surface=blk_doorbell", "action=duplicate", "lane=*", "blk_op=1"):
        if token not in doorbell:
            raise AssertionError(f"B2 doorbell rule lacks {token!r}")
    if any(token.startswith("sqe_") for token in doorbell) or "action=delay" in doorbell:
        raise AssertionError(f"B2 doorbell rule uses an invalid surface contract: {doorbell!r}")
    for event_id in ("block-active-lane-safe-rejection", "block-duplicate-doorbell"):
        contract = b2[event_id]["operation"]["stdout_row"]
        assert_equal(contract["one_of"]["rdma_lane"], ["ivshmem", "roce"], f"{event_id} lane proof")
        for field in ("ring_geometry_valid", "ring_indices_valid", "ring_quiescent"):
            assert_equal(contract["equals"][field], "1", f"{event_id} {field}")

    x1_plan = plans["X1"]
    x1 = {event["id"]: event for event in x1_plan["events"]}
    for event_id in ("c2-start", "i2-start"):
        argv = x1[event_id]["operation"]["argv"]
        if argv[-2:] != ["--timeout-ms", "180000"] or "--deadline-ms" in argv:
            raise AssertionError(f"X1 {event_id} lacks one supported bounded workload timeout: {argv!r}")
    ids = [event["id"] for event in x1_plan["events"]]
    ordered = [
        "c2-start", "c2-task-running", "c2-advance-task-running", "c2-exit-issued",
        "i2-start", "i2-control-issued", "wait-applied-1901", "wait-applied-1902", "reset-node1",
        "node1-ssh-ready", "peer-epoch-ready", "peer-epoch-ready-node2",
        "c2-advance-exit-issued",
        "c2-successor-running", "i2-advance-control-issued",
        "i2-old-session-fenced", "i2-successor-issued", "release-1901",
        "release-1902", "c2-advance-successor-running",
        "c2-successor-complete", "c2-advance-successor-complete", "i2-advance-successor-issued",
        "i2-successor-complete", "i2-advance-successor-complete",
        "c2-finish", "i2-finish",
        "net-old-query-rejected", "net-attach-successor", "post-reset-block",
        "post-reset-vfs-ready", "post-reset-vfs-unmount", "fault-evidence-node2",
        "workload-clear-node0-final", "clear-node0-final", "converge-node0",
    ]
    positions = [ids.index(event_id) for event_id in ordered]
    if positions != sorted(positions):
        raise AssertionError(f"X1 cross-service reset ordering is not causal: {positions!r}")
    for prefix in ("c2", "i2"):
        advance = x1[f"{prefix}-advance-successor-complete"]["operation"]
        assert_equal(advance["argv"][2], "advance", f"X1 {prefix} terminal checkpoint action")
        assert_equal(
            advance["argv"][advance["argv"].index("--checkpoint") + 1],
            "successor-complete",
            f"X1 {prefix} terminal checkpoint identity",
        )
    for node in ("0", "2"):
        assert_equal(
            x1[f"converge-node{node}"]["operation"]["allow"].get("peer_replaced"),
            ["wos-1"],
            f"X1 node {node} exact rebooted-peer retirement proof",
        )
    assert_equal(
        x1["converge-node1"]["operation"]["allow"].get("peer_replaced"),
        [],
        "X1 rebooted owner does not waive its unchanged peers",
    )
    assert_equal(x1["net-old-query-rejected"]["operation"]["expect_exit"], [1], "X1 old NET rejection")
    assert_equal(
        x1["post-reset-block"]["operation"]["stdout_row"]["equals"]["exact_binding"],
        "1",
        "X1 post-reset block exact binding",
    )
    assert_equal(
        x1["post-reset-vfs-ready"]["operation"]["stdout_row"]["equals"],
        {
            "op": "vfs-query-discovered",
            "status": "0",
            "exact_binding": "1",
            "detached": "0",
        },
        "X1 replacement VFS readiness barrier",
    )
    assert_equal(
        x1["post-reset-vfs-unmount"]["operation"]["stdout_row"]["equals"]["detached"],
        "1",
        "X1 exact VFS unmount",
    )


def test_committed_matrix_requires_every_lane(module) -> None:
    raw = module.load_json(MATRIX)
    matrix = module.validate_matrix(raw)
    assert_equal(
        [scenario["id"] for scenario in matrix["scenarios"]],
        ["C1", "C2", "V1", "V2", "I1", "I2", "N1", "B1", "B2", "X1"],
        "committed scenario IDs",
    )
    with tempfile.TemporaryDirectory() as temporary:
        results = Path(temporary)
        catalog = module.load_json(CATALOG)
        report = module.evaluate_matrix(raw, results, catalog)
        assert_equal(report["fullMatrixPassed"], False, "empty matrix result")
        assert_equal(report["outcome"], "incomplete", "empty matrix outcome")
        for scenario in matrix["scenarios"]:
            for lane in scenario["required_lanes"]:
                path = results / scenario["id"] / lane / "result.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                write_lane_result(module, path, scenario["id"], lane)
        passed = module.evaluate_matrix(raw, results, catalog)
        assert_equal(passed["fullMatrixPassed"], True, "complete matrix result")

        skipped_path = results / "B2" / "roce" / "result.json"
        write_lane_result(module, skipped_path, "B2", "roce", "skipped")
        incomplete = module.evaluate_matrix(raw, results, catalog)
        assert_equal(incomplete["fullMatrixPassed"], False, "skipped required lane")
        assert_equal(incomplete["outcome"], "incomplete", "skipped required lane outcome")


def main() -> None:
    module = load_module()
    tests = [
        test_snapshot_reads_packet_pool_before_other_ssh_observers,
        test_guest_transport_reuses_and_explicitly_closes_control_scope,
        test_plan_is_seed_stable_and_choice_sensitive,
        test_schema_rejects_shell_duplicates_and_bad_qmp,
        test_bounded_command_records_exit_timeout_and_truncation,
        test_guest_poll_retries_direct_argv_and_records_attempts,
        test_guest_stdout_row_contract_is_bounded_and_relational,
        test_group_reducer_never_splits_pairs,
        test_snapshot_invariants_require_complete_stable_gauges,
        test_pipe_snapshot_parses_positional_waiter_detail,
        test_peer_snapshot_rejects_duplicate_stable_identities,
        test_replaced_peer_allowance_requires_exact_epoch_and_cleanup_fence,
        test_repeated_peer_replacement_preserves_proven_tombstones,
        test_peer_epoch_probe_follows_rebooted_hostname,
        test_snapshot_detail_rdma_and_owner_contracts,
        test_retired_block_proxy_tombstones_are_diagnostics_not_owners,
        test_retired_net_proxy_tombstones_are_diagnostics_not_owners,
        test_ipc_and_compute_owner_identities_follow_proc_lifecycles,
        test_live_snapshot_convergence_and_artifacts,
        test_telemetry_records_are_bounded_for_wosdbg,
        test_convergence_uses_bounded_samples_until_invariants_pass,
        test_async_guest_lifecycle_runs_faults_in_flight_and_forces_cleanup,
        test_async_guest_schema_rejects_dangling_duplicate_and_split_groups,
        test_truncated_guest_output_makes_run_incomplete,
        test_qmp_client_records_interleaved_events,
        test_catalog_readiness_and_real_ktest_evidence,
        test_i1_adapter_and_live_scenario_contract,
        test_direct_live_scenario_order_seed_and_evidence_contracts,
        test_direct_service_workloads_are_bounded_and_checkpointed,
        test_service_scenarios_use_exact_adapters_and_evidence,
        test_committed_matrix_requires_every_lane,
    ]
    passed = 0
    skipped = 0
    for test in tests:
        if getattr(test, "requires_unix_socket", False):
            try:
                with tempfile.TemporaryDirectory() as temporary:
                    probe_path = Path(temporary) / "probe.sock"
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                        probe.bind(str(probe_path))
            except PermissionError:
                print(f"SKIP {test.__name__}: sandbox denies AF_UNIX bind")
                skipped += 1
                continue
        test(module)
        passed += 1
    print(f"{passed} wki_chaos tests passed, {skipped} skipped")


if __name__ == "__main__":
    main()
