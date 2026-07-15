#!/usr/bin/env python3

import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = ROOT / "scripts" / "bench" / "run_cross_os_benchmark_suite.py"
WOS_SHOWCASE_RUNNER = ROOT / "configs" / "rootfs" / "root" / "wos-showcase" / "run-all.sh"
EXPECTED_LAYOUTS = {
    1: [(32, 32768)],
    2: [(16, 16384), (16, 16384)],
    3: [(11, 10923), (11, 10923), (10, 10922)],
    4: [(8, 8192), (8, 8192), (8, 8192), (8, 8192)],
}


def runtime_remote_stub(
    layout,
    calls=None,
    *,
    hostname_overrides=None,
    vcpu_overrides=None,
    memory_overrides=None,
):
    hostname_overrides = hostname_overrides or {}
    vcpu_overrides = vcpu_overrides or {}
    memory_overrides = memory_overrides or {}

    def remote(host, command, *, timeout=None):
        if calls is not None:
            calls.append((host, command, timeout))
        hostname = host.removesuffix(".wos")
        node_id = int(hostname.removeprefix("wos-"))
        vcpus, memory_mib = layout[node_id]
        if command == "hostname":
            stdout = hostname_overrides.get(hostname, hostname) + "\n"
        elif command == "cat /proc/stat":
            online_vcpus = vcpu_overrides.get(hostname, vcpus)
            stdout = "cpu  1 0 1 1 0 0 0 0 0 0\n" + "".join(
                f"cpu{cpu}  1 0 1 1 0 0 0 0 0 0\n" for cpu in range(online_vcpus)
            )
        elif command == "cat /proc/meminfo":
            mem_total_kib = memory_overrides.get(hostname, (memory_mib - 1) * 1024)
            stdout = f"MemTotal:\t{mem_total_kib} kB\nMemFree:\t1 kB\n"
        else:
            fail(f"unexpected WOS runtime resource command: {command}")
        return subprocess.CompletedProcess([host, command], 0, stdout, "")

    return remote


def fail(message: str) -> None:
    raise AssertionError(message)


def assert_equal(actual, expected, context: str) -> None:
    if actual != expected:
        fail(f"{context}: expected {expected!r}, got {actual!r}")


def load_runner():
    module_name = "cross_os_benchmark_suite_under_test"
    spec = importlib.util.spec_from_file_location(module_name, RUNNER_PATH)
    if spec is None or spec.loader is None:
        fail(f"failed to load {RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        spec.loader.exec_module(module)
    assert_equal(stdout.getvalue(), "", "runner import stdout")
    assert_equal(stderr.getvalue(), "", "runner import stderr")
    return module


def test_committed_cluster_provenance(runner) -> None:
    for node_count, expected_layout in EXPECTED_LAYOUTS.items():
        path = ROOT / "configs" / f"cluster_bench_{node_count}.json"
        provenance, raw = runner.validate_wos_cluster_config(path, node_count)
        assert_equal(provenance["config_validated"], True, f"{path.name} validation flag")
        assert_equal(provenance["runtime_validated"], False, f"{path.name} runtime flag")
        assert_equal(provenance["config_path"], f"configs/{path.name}", f"{path.name} relative path")
        assert_equal(provenance["config_sha256"], hashlib.sha256(raw).hexdigest(), f"{path.name} hash")
        assert_equal(provenance["node_count"], node_count, f"{path.name} node count")
        assert_equal(provenance["total_vcpus"], 32, f"{path.name} aggregate vCPUs")
        assert_equal(provenance["total_memory_mib"], 32768, f"{path.name} aggregate memory")
        actual_layout = [(node["vcpus"], node["memory_mib"]) for node in provenance["nodes"]]
        assert_equal(actual_layout, expected_layout, f"{path.name} normalized resources")
        assert_equal(
            [node["hostname"] for node in provenance["nodes"]],
            [f"wos-{index}" for index in range(node_count)],
            f"{path.name} hostnames",
        )


def test_cluster_mismatch_rejections(runner) -> None:
    try:
        runner.validate_wos_cluster_config(ROOT / "configs" / "cluster_bench_3.json", 2)
    except ValueError as exc:
        if "do not match --num-vms" not in str(exc):
            fail(f"unexpected node-count mismatch diagnostic: {exc}")
    else:
        fail("three-node config was accepted for --num-vms 2")

    source = json.loads((ROOT / "configs" / "cluster_bench_1.json").read_text(encoding="utf-8"))
    broken = copy.deepcopy(source)
    broken["zones"][0]["vm"]["cpus"] = 33
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "wrong-resources.json"
        path.write_text(json.dumps(broken), encoding="utf-8")
        try:
            runner.validate_wos_cluster_config(path, 1)
        except ValueError as exc:
            if "fixed-resource acceptance requires 32" not in str(exc):
                fail(f"unexpected aggregate-resource diagnostic: {exc}")
        else:
            fail("33-vCPU topology was accepted for fixed-resource benchmarking")


def test_runtime_resource_parsing(runner) -> None:
    proc_stat = (
        "cpu  2 0 2 2 0 0 0 0 0 0\n"
        "cpu0  1 0 1 1 0 0 0 0 0 0\n"
        "cpu1\t1 0 1 1 0 0 0 0 0 0\n"
        "cpu-online 2\n"
        " cpu2  0 0 0 0 0 0 0 0 0 0\n"
    )
    assert_equal(runner.parse_wos_online_vcpus(proc_stat), 2, "numbered /proc/stat CPU rows")
    assert_equal(
        runner.parse_wos_mem_total_kib("MemTotal:\t33553408 kB\nMemFree:\t1 kB\n"),
        33553408,
        "WOS MemTotal parsing",
    )

    for invalid in ("", "MemTotal:\t0 kB\n", "MemTotal:\t1 bytes\n"):
        try:
            runner.parse_wos_mem_total_kib(invalid)
        except ValueError:
            pass
        else:
            fail(f"invalid WOS MemTotal was accepted: {invalid!r}")

    invalid_proc_stats = (
        "cpu  1 0 1 1 0 0 0 0 0 0\n",
        "cpu0  1 0 1 1 0 0 0 0 0 0\ncpu0  1 0 1 1 0 0 0 0 0 0\n",
        "cpu0  1 0 1 1 0 0 0 0 0 0\ncpu2  1 0 1 1 0 0 0 0 0 0\n",
        "cpu00  1 0 1 1 0 0 0 0 0 0\n",
    )
    for invalid in invalid_proc_stats:
        try:
            runner.parse_wos_online_vcpus(invalid)
        except ValueError:
            pass
        else:
            fail(f"invalid numbered /proc/stat CPU rows were accepted: {invalid!r}")


def test_runtime_resource_collection(runner) -> None:
    topology, _raw = runner.validate_wos_cluster_config(
        ROOT / "configs" / "cluster_bench_3.json", 3
    )
    hosts = ["wos-0.wos", "wos-1.wos", "wos-2.wos"]
    calls = []
    original_remote = runner.wos_remote_command
    runner.wos_remote_command = runtime_remote_stub(EXPECTED_LAYOUTS[3], calls)
    try:
        resources = runner.collect_wos_runtime_resources(topology, hosts, timeout=4.5)
    finally:
        runner.wos_remote_command = original_remote

    expected_nodes = [
        {
            "hostname": f"wos-{node_id}",
            "online_vcpus": vcpus,
            "mem_total_kib": (memory_mib - 1) * 1024,
        }
        for node_id, (vcpus, memory_mib) in enumerate(EXPECTED_LAYOUTS[3])
    ]
    assert_equal(
        resources,
        {
            "collector": "wos-procfs-v1",
            "memory_semantics": "allocator-visible-usable-kib",
            "nodes": expected_nodes,
            "total_online_vcpus": 32,
            "total_mem_total_kib": sum(node["mem_total_kib"] for node in expected_nodes),
        },
        "runtime resource evidence",
    )
    assert_equal(
        calls,
        [
            (host, command, 4.5)
            for host in hosts
            for command in ("hostname", "cat /proc/stat", "cat /proc/meminfo")
        ],
        "runtime resource probe commands",
    )
    assert_equal(topology["runtime_validated"], False, "collector does not validate early")
    if "runtime_resources" in topology:
        fail("collector mutated topology before the caller attached complete evidence")


def test_runtime_resource_mismatch_rejections(runner) -> None:
    topology, _raw = runner.validate_wos_cluster_config(
        ROOT / "configs" / "cluster_bench_1.json", 1
    )
    original_remote = runner.wos_remote_command

    cases = [
        (
            runtime_remote_stub(
                EXPECTED_LAYOUTS[1], hostname_overrides={"wos-0": "unexpected-host"}
            ),
            "reported hostname",
        ),
        (
            runtime_remote_stub(EXPECTED_LAYOUTS[1], vcpu_overrides={"wos-0": 31}),
            "reports 31 online vCPUs",
        ),
        (
            runtime_remote_stub(EXPECTED_LAYOUTS[1], memory_overrides={"wos-0": 1}),
            "allocator-visible memory within 85%-100%",
        ),
        (
            runtime_remote_stub(
                EXPECTED_LAYOUTS[1], memory_overrides={"wos-0": 32768 * 1024 + 1}
            ),
            "allocator-visible memory within 85%-100%",
        ),
    ]
    try:
        for remote, diagnostic in cases:
            runner.wos_remote_command = remote
            try:
                runner.collect_wos_runtime_resources(topology, ["wos-0.wos"], timeout=1.0)
            except RuntimeError as exc:
                if diagnostic not in str(exc):
                    fail(f"unexpected runtime resource rejection diagnostic: {exc}")
            else:
                fail(f"runtime resource mismatch was accepted: {diagnostic}")

        calls = []
        runner.wos_remote_command = runtime_remote_stub(EXPECTED_LAYOUTS[1], calls)
        try:
            runner.collect_wos_runtime_resources(topology, ["wos-0"], timeout=1.0)
        except RuntimeError as exc:
            if "do not exactly match configured hosts" not in str(exc):
                fail(f"unexpected runtime host-list rejection diagnostic: {exc}")
        else:
            fail("noncanonical WOS runtime host list was accepted")
        assert_equal(calls, [], "host-list mismatch aborts before SSH")
    finally:
        runner.wos_remote_command = original_remote

    assert_equal(topology["runtime_validated"], False, "failed collection runtime flag")
    if "runtime_resources" in topology:
        fail("failed runtime collection attached partial evidence")


def test_mandel_worker_modes(runner) -> None:
    parser = runner.build_parser()

    default_args = parser.parse_args(["--num-vms", "3"])
    assert_equal(runner.mandel_worker_layout(default_args, 3), (24, 8), "legacy default worker layout")
    assert_equal(runner.mandel_worker_source(default_args), "default-per-node", "legacy default worker source")
    assert_equal(
        runner.linux_mandel_worker_args(default_args, 3),
        ["--threads", "8", "--np", "24"],
        "legacy Linux worker args",
    )

    per_node_args = parser.parse_args(["--num-vms", "3", "--mandel-threads", "7"])
    assert_equal(runner.mandel_worker_layout(per_node_args, 3), (21, 7), "explicit per-node layout")
    assert_equal(
        runner.mandel_worker_source(per_node_args),
        "explicit-per-node",
        "explicit per-node worker source",
    )

    total_args = parser.parse_args(["--num-vms", "3", "--mandel-total-workers", "32"])
    assert_equal(runner.mandel_worker_layout(total_args, 3), (32, None), "explicit total worker layout")
    assert_equal(runner.mandel_worker_source(total_args), "explicit-total", "explicit total worker source")
    assert_equal(runner.linux_mandel_worker_args(total_args, 3), ["--np", "32"], "total Linux worker args")

    with contextlib.redirect_stderr(io.StringIO()):
        try:
            parser.parse_args(["--num-vms", "3", "--mandel-threads", "8", "--mandel-total-workers", "32"])
        except SystemExit as exc:
            assert_equal(exc.code, 2, "mutually exclusive worker options exit")
        else:
            fail("per-node and total Mandel worker options were accepted together")


def test_showcase_measurement_parsing(runner) -> None:
    measurements = runner.parse_showcase_measurements(
        '{"benchmark":"duplicate","sequence":1}\n'
        '{"benchmark":"duplicate","sequence":2}\n'
    )
    assert_equal(
        measurements,
        [
            {"benchmark": "duplicate", "sequence": 1},
            {"benchmark": "duplicate", "sequence": 2},
        ],
        "showcase measurement order",
    )
    assert_equal(runner.parse_showcase_measurements(""), [], "empty showcase measurements")

    malformed_inputs = [
        ("not-json\n", "not valid JSON"),
        ("[1, 2, 3]\n", "must contain a JSON object"),
        ('{"valid":true}\n\n', "line 2 is empty"),
    ]
    for text, expected_diagnostic in malformed_inputs:
        try:
            runner.parse_showcase_measurements(text)
        except RuntimeError as exc:
            if expected_diagnostic not in str(exc):
                fail(f"unexpected metrics rejection diagnostic: {exc}")
        else:
            fail(f"invalid showcase metrics were accepted: {text!r}")


def test_wos_showcase_timeout_is_finite_and_nonnegative(runner) -> None:
    parser = runner.build_parser()
    assert_equal(
        parser.parse_args(["--num-vms", "1", "--wos-showcase-timeout", "0"]).wos_showcase_timeout,
        0.0,
        "disabled showcase timeout",
    )
    for invalid in ("-1", "nan", "inf", "-inf"):
        with contextlib.redirect_stderr(io.StringIO()):
            try:
                parser.parse_args(
                    ["--num-vms", "1", "--wos-showcase-timeout", invalid]
                )
            except SystemExit as exc:
                assert_equal(exc.code, 2, f"invalid showcase timeout {invalid}")
            else:
                fail(f"invalid showcase timeout was accepted: {invalid}")


def test_wos_cleanup_cwd_and_timeout_override(runner) -> None:
    original_remote = runner.wos_remote_command
    original_run = runner.run_command

    def fake_remote(_host, command, *, timeout=None):
        del timeout
        if "/proc/[0-9]*/cmdline" not in command:
            fail(f"unexpected cleanup command: {command}")
        if 'readlink "/proc/$pid/cwd"' not in command:
            fail(f"cleanup command does not read process cwd: {command}")
        if "printf '%s\\t'" not in command or "printf '\\t%s\\n'" not in command:
            fail(f"cleanup command does not emit PID-tab-exe-tab-cwd rows: {command}")
        stdout = (
            "101\t/usr/bin/python3\t/tmp/wos-showcase-fixed-0123456789abcdef\n"
            "102\t/usr/bin/git\t/tmp/wos-showcase-fixed-0123456789abcdef/git-clones/job-0\n"
            "103\t/usr/bin/renderbench\t/root\n"
            "104\t/usr/bin/python3\t/root\n"
            "105\t/usr/bin/python3\t/tmp/wos-showcase-fixed-0123456789abcdefx\n"
            "106\t/usr/bin/renderbench-helper\t/root\n"
            "107\t/usr/bin/git\t/tmp/wos-showcase-fixed-0123456789abcdeF\n"
        )
        return subprocess.CompletedProcess([command], 0, stdout, "")

    timeouts = []

    def fake_run(command, *, cwd=None, timeout=None):
        del cwd
        timeouts.append(timeout)
        return subprocess.CompletedProcess(command, 0, "", "")

    runner.wos_remote_command = fake_remote
    runner.run_command = fake_run
    try:
        assert_equal(
            runner.wos_benchmark_pids("wos-0.wos", timeout=2.0),
            [101, 102, 103],
            "fixed-resource stale worker detection",
        )
        args = runner.build_parser().parse_args(["--num-vms", "1"])
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp)
            runner.run_benchmark_command(args, step_dir, ["true"])
            runner.run_benchmark_command(args, step_dir, ["true"], timeout_seconds=321.0)
            runner.run_benchmark_command(args, step_dir, ["true"], timeout_seconds=0.0)
        assert_equal(timeouts, [900.0, 321.0, None], "benchmark timeout override")
    finally:
        runner.wos_remote_command = original_remote
        runner.run_command = original_run


def test_wos_stale_work_root_cleanup(runner) -> None:
    original_remote = runner.wos_remote_command
    commands = []

    def fake_remote(_host, command, *, timeout=None):
        del timeout
        commands.append(command)
        if command == "hostname >/dev/null":
            return subprocess.CompletedProcess([command], 0, "", "")
        if "/proc/[0-9]*/cmdline" in command:
            return subprocess.CompletedProcess([command], 0, "", "")
        if command.startswith("for d in /tmp/wos-showcase-fixed-*"):
            return subprocess.CompletedProcess([command], 0, "", "")
        fail(f"unexpected stale-work-root cleanup command: {command}")

    runner.wos_remote_command = fake_remote
    try:
        args = runner.build_parser().parse_args(["--num-vms", "1"])
        runner.prepare_wos_hosts(args, ["wos-0.wos"])
    finally:
        runner.wos_remote_command = original_remote

    assert_equal(len(commands), 3, "preflight command count")
    cleanup_command = commands[-1]
    required_safety_checks = [
        '[ -d "$d" ] && [ ! -L "$d" ]',
        '[ "${#suffix}" -eq 16 ]',
        '*[!0-9a-f]*',
        'marker="$d/.wos-showcase-owner"',
        '[ -f "$marker" ] && [ ! -L "$marker" ]',
        'bytes=$(wc -c < "$marker"',
        '[ "$bytes" -eq 32 ]',
        '&& printf x',
        '[ "${#owner}" -eq 32 ]',
        'owner_prefix=${owner%????????????????}',
        '[ "$owner_prefix" = "$suffix" ]',
        'rm -rf -- "$d"',
    ]
    for safety_check in required_safety_checks:
        if safety_check not in cleanup_command:
            fail(f"stale-work-root cleanup is missing safety check {safety_check!r}")
    assert_equal(
        cleanup_command.count("*[!0-9a-f]*"),
        2,
        "suffix and owner lowercase-hex checks",
    )

    blocked_commands = []

    def fake_blocked_remote(_host, command, *, timeout=None):
        del timeout
        blocked_commands.append(command)
        if command == "hostname >/dev/null" or command.startswith("kill -"):
            return subprocess.CompletedProcess([command], 0, "", "")
        if "/proc/[0-9]*/cmdline" in command:
            stdout = (
                "201\t/usr/bin/python3\t"
                "/tmp/wos-showcase-fixed-0123456789abcdef/python-sha/job-0\n"
            )
            return subprocess.CompletedProcess([command], 0, stdout, "")
        if command.startswith("for d in /tmp/wos-showcase-fixed-*"):
            fail("stale work roots were removed while a matching process remained")
        fail(f"unexpected blocked-cleanup command: {command}")

    runner.wos_remote_command = fake_blocked_remote
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                runner.prepare_wos_hosts(args, ["wos-0.wos"])
            except RuntimeError as exc:
                if "stale benchmark pids did not exit: 201" not in str(exc):
                    fail(f"unexpected blocked-cleanup diagnostic: {exc}")
            else:
                fail("preflight accepted a fixed-resource process that survived cleanup")
    finally:
        runner.wos_remote_command = original_remote
    if any(command.startswith("for d in /tmp/wos-showcase-fixed-*") for command in blocked_commands):
        fail("stale work-root cleanup ran before matching processes exited")

    barrier_calls = []

    def fake_barrier_remote(host, command, *, timeout=None):
        del timeout
        barrier_calls.append((host, command))
        if command == "hostname >/dev/null":
            return subprocess.CompletedProcess([command], 0, "", "")
        if "/proc/[0-9]*/cmdline" in command:
            return subprocess.CompletedProcess([command], 0, "", "")
        if command.startswith("for d in /tmp/wos-showcase-fixed-*"):
            return subprocess.CompletedProcess([command], 0, "", "")
        fail(f"unexpected multi-host cleanup command: {command}")

    runner.wos_remote_command = fake_barrier_remote
    try:
        runner.prepare_wos_hosts(args, ["wos-0.wos", "wos-1.wos"])
    finally:
        runner.wos_remote_command = original_remote
    scan_indices = [
        index
        for index, (_host, command) in enumerate(barrier_calls)
        if "/proc/[0-9]*/cmdline" in command
    ]
    cleanup_indices = [
        index
        for index, (_host, command) in enumerate(barrier_calls)
        if command.startswith("for d in /tmp/wos-showcase-fixed-*")
    ]
    assert_equal(len(scan_indices), 2, "multi-host process scans")
    assert_equal(len(cleanup_indices), 2, "multi-host root cleanups")
    if max(scan_indices) >= min(cleanup_indices):
        fail("a stale work root was removed before every host crossed the scan barrier")

    failure_calls = []

    def fake_late_failure_remote(host, command, *, timeout=None):
        del timeout
        failure_calls.append((host, command))
        if host == "wos-1.wos" and command == "hostname >/dev/null":
            raise RuntimeError("host unavailable")
        if command == "hostname >/dev/null" or "/proc/[0-9]*/cmdline" in command:
            return subprocess.CompletedProcess([command], 0, "", "")
        if command.startswith("for d in /tmp/wos-showcase-fixed-*"):
            fail("root cleanup ran despite a later unreachable host")
        fail(f"unexpected failed-barrier command: {command}")

    runner.wos_remote_command = fake_late_failure_remote
    try:
        try:
            runner.prepare_wos_hosts(args, ["wos-0.wos", "wos-1.wos"])
        except RuntimeError as exc:
            if "wos-1.wos: unreachable" not in str(exc):
                fail(f"unexpected late-host failure diagnostic: {exc}")
        else:
            fail("preflight accepted an unreachable later host")
    finally:
        runner.wos_remote_command = original_remote
    if any(
        command.startswith("for d in /tmp/wos-showcase-fixed-*")
        for _host, command in failure_calls
    ):
        fail("stale work-root cleanup bypassed the all-host barrier")


def test_wos_showcase_metrics_collection(_runner) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        output_root = tmp_path / "output"
        output_root.mkdir()
        (output_root / "metrics.jsonl").write_text('{"stale":true}\n', encoding="utf-8")

        successful_case = tmp_path / "successful.sh"
        successful_case.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' 'ordinary output' "
            "'{\"benchmark\":\"duplicate\",\"sequence\":1}' "
            "'[\"not-an-object\"]' "
            "'{\"benchmark\":\"duplicate\",\"sequence\":2}'\n",
            encoding="utf-8",
        )
        successful_case.chmod(0o755)

        failed_case = tmp_path / "failed.sh"
        failed_case.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' '{\"benchmark\":\"failed-diagnostic\",\"valid\":true}'\n"
            "exit 7\n",
            encoding="utf-8",
        )
        failed_case.chmod(0o755)

        result = subprocess.run(
            [
                str(WOS_SHOWCASE_RUNNER),
                "--output-root",
                str(output_root),
                str(successful_case),
                str(failed_case),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        assert_equal(result.returncode, 1, "showcase failed-case exit status")
        assert_equal(
            (output_root / "metrics.jsonl").read_text(encoding="utf-8"),
            '{"benchmark":"duplicate","sequence":1}\n'
            '{"benchmark":"duplicate","sequence":2}\n'
            '{"benchmark":"failed-diagnostic","valid":true}\n',
            "showcase collected metrics",
        )
        if f"METRICS_FILE={output_root}/metrics.jsonl" not in result.stdout:
            fail(f"showcase metrics path is missing from output: {result.stdout}")

        rerun = subprocess.run(
            [
                str(WOS_SHOWCASE_RUNNER),
                "--output-root",
                str(output_root),
                str(successful_case),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        assert_equal(rerun.returncode, 0, "showcase successful rerun exit status")
        assert_equal(
            (output_root / "metrics.jsonl").read_text(encoding="utf-8"),
            '{"benchmark":"duplicate","sequence":1}\n'
            '{"benchmark":"duplicate","sequence":2}\n',
            "showcase metrics truncation",
        )


def test_wos_showcase_metrics_plumbing(runner) -> None:
    args = runner.build_parser().parse_args(["--num-vms", "2"])
    suite_remote_root = "/tmp/synthetic-suite"
    host = "wos-1.wos"
    hosts = ["wos-0.wos", host]
    fixed_measurements = [
        {"benchmark": "wos_git_clone", "sequence": 1},
        {"benchmark": "wos_git_checkout", "sequence": 2},
        {"benchmark": "wos_python_sha256", "sequence": 3},
        {"benchmark": "wos_python_json", "sequence": 4},
        {"benchmark": "wos_file_move", "sequence": 5},
    ]
    fetches: list[tuple[str, Path]] = []
    optional_fetches: list[tuple[str, Path]] = []
    failure_events: list[str] = []

    original_prepare = runner.prepare_wos_hosts
    original_run = runner.run_benchmark_command
    original_fetch = runner.fetch_remote_file
    original_optional_fetch = runner.fetch_optional_remote_file

    def fake_fetch(_fetcher, _host, remote_path, local_path, *, timeout):
        del timeout
        fetches.append((remote_path, local_path))
        if remote_path.endswith("/summary.tsv"):
            local_path.write_text("30-bench-wki\tPASS\tlog=synthetic\n", encoding="utf-8")
        elif remote_path.endswith("/metrics.jsonl"):
            local_path.write_text(
                "".join(
                    json.dumps(measurement, separators=(",", ":")) + "\n"
                    for measurement in fixed_measurements
                ),
                encoding="utf-8",
            )
        else:
            fail(f"unexpected showcase fetch: {remote_path}")

    runner.prepare_wos_hosts = lambda _args, _hosts: None
    runner.run_benchmark_command = lambda _args, _step_dir, command, **_kwargs: (
        subprocess.CompletedProcess(command, 0, "synthetic stdout", ""),
        [],
    )
    runner.fetch_remote_file = fake_fetch
    try:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            step = runner.run_wos_showcase(args, suite_dir, suite_remote_root, host, hosts)
            remote_output_root = f"{suite_remote_root}/wos-showcase"
            assert_equal(
                [remote_path for remote_path, _local_path in fetches],
                [f"{remote_output_root}/summary.tsv", f"{remote_output_root}/metrics.jsonl"],
                "successful showcase fetches",
            )
            result = json.loads(
                (suite_dir / "wos-showcase" / "result.json").read_text(encoding="utf-8")
            )
            assert_equal(
                result["measurements"],
                fixed_measurements,
                "showcase result measurements",
            )
            metrics_artifact = suite_dir / "wos-showcase" / "metrics.jsonl"
            if runner.relpath(metrics_artifact) not in step["artifacts"]:
                fail(f"showcase metrics artifact is missing: {step['artifacts']}")
            remote_command = step["command"][-1]
            if f"--output-root {remote_output_root}" not in remote_command:
                fail(f"showcase remote output root is missing from command: {remote_command}")
            if "wos-1.wos,wos-0.wos" not in remote_command:
                fail(f"showcase hosts are not launcher-first: {remote_command}")

        def fake_failed_run(_args, _step_dir, _command, **_kwargs):
            failure_events.append("run-failed")
            raise RuntimeError("synthetic showcase failure")

        def fake_optional_fetch(_fetcher, _host, remote_path, local_path, *, timeout):
            del timeout
            failure_events.append("partial-fetch")
            optional_fetches.append((remote_path, local_path))
            return True

        def fake_failure_cleanup(cleanup_args, cleanup_hosts):
            if cleanup_args.skip_wos_cleanup:
                fail("failure cleanup retained --skip-wos-cleanup")
            assert_equal(cleanup_hosts, hosts, "failure cleanup hosts")
            failure_events.append("all-host-cleanup")

        runner.run_benchmark_command = fake_failed_run
        runner.fetch_optional_remote_file = fake_optional_fetch
        runner.prepare_wos_hosts = fake_failure_cleanup
        with tempfile.TemporaryDirectory() as tmp:
            try:
                runner.run_wos_showcase(args, Path(tmp), suite_remote_root, host, hosts)
            except RuntimeError as exc:
                if "synthetic showcase failure" not in str(exc):
                    fail(f"unexpected failed showcase diagnostic: {exc}")
            else:
                fail("failed WOS showcase was reported as successful")
        assert_equal(
            [remote_path for remote_path, _local_path in optional_fetches],
            [
                f"{suite_remote_root}/wos-showcase/summary.tsv",
                f"{suite_remote_root}/wos-showcase/metrics.jsonl",
            ],
            "failed showcase optional fetches",
        )
        assert_equal(
            [local_path.name for _remote_path, local_path in optional_fetches],
            ["partial-summary.tsv", "partial-metrics.jsonl"],
            "failed showcase partial artifact names",
        )
        assert_equal(
            failure_events,
            ["all-host-cleanup", "run-failed", "all-host-cleanup", "partial-fetch", "partial-fetch"],
            "failed showcase cleanup/fetch ordering",
        )

        failure_events.clear()
        cleanup_attempts = 0

        def fake_incomplete_cleanup(_cleanup_args, _cleanup_hosts):
            nonlocal cleanup_attempts
            cleanup_attempts += 1
            failure_events.append(
                "initial-preflight" if cleanup_attempts == 1 else "cleanup-failed"
            )
            if cleanup_attempts == 2:
                raise RuntimeError("synthetic cleanup failure")

        runner.prepare_wos_hosts = fake_incomplete_cleanup
        cleanup_stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stderr(
            cleanup_stderr
        ):
            try:
                runner.run_wos_showcase(
                    args, Path(tmp), suite_remote_root, host, hosts
                )
            except RuntimeError as exc:
                if str(exc) != "synthetic showcase failure":
                    fail(f"cleanup failure replaced the original exception: {exc}")
                cleanup_note = getattr(exc, "wos_cleanup_error", "")
                if "synthetic cleanup failure" not in cleanup_note:
                    fail(f"cleanup failure was not attached: {cleanup_note!r}")
            else:
                fail("cleanup failure caused a failed showcase to be accepted")
        if "synthetic cleanup failure" not in cleanup_stderr.getvalue():
            fail("cleanup failure was not reported")
        assert_equal(
            failure_events,
            [
                "initial-preflight",
                "run-failed",
                "cleanup-failed",
                "partial-fetch",
                "partial-fetch",
            ],
            "incomplete cleanup/fetch ordering",
        )
    finally:
        runner.prepare_wos_hosts = original_prepare
        runner.run_benchmark_command = original_run
        runner.fetch_remote_file = original_fetch
        runner.fetch_optional_remote_file = original_optional_fetch


def test_mandel_command_composition(runner) -> None:
    parser = runner.build_parser()
    total_args = parser.parse_args(["--num-vms", "3", "--mandel-total-workers", "32", "--mandel-repeat", "0"])
    total_args.remote_suite_name = "test-suite"

    commands: list[list[str]] = []
    original_prepare = runner.prepare_wos_hosts
    original_run = runner.run_benchmark_command
    original_fetch = runner.fetch_remote_file
    original_parse = runner.parse_mandel_report

    def fake_run(_args, _step_dir, command):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", ""), []

    def fake_fetch(_fetcher, _host, _remote_path, local_path, *, timeout):
        del timeout
        local_path.write_text("synthetic report\n", encoding="utf-8")

    runner.prepare_wos_hosts = lambda _args, _hosts: None
    runner.run_benchmark_command = fake_run
    runner.fetch_remote_file = fake_fetch
    runner.parse_mandel_report = lambda _text: {}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            wos_hosts = ["wos-0.wos", "wos-1.wos", "wos-2.wos"]
            wos_step = runner.run_wos_mandelbench(total_args, suite_dir, "/tmp/test-suite", wos_hosts[0], wos_hosts)
            wos_remote = wos_step["command"][-1]
            if "--threads 32" not in wos_remote or "--nodes wos-0.wos,wos-1.wos,wos-2.wos" not in wos_remote:
                fail(f"fixed-total WOS Mandel command is incorrect: {wos_remote}")
            wos_summary = json.loads(
                (suite_dir / "wos-mandelbench" / "result.json").read_text(encoding="utf-8")
            )
            assert_equal(wos_summary["threads"], 32, "fixed-total WOS worker count")
            assert_equal(wos_summary["threads_per_node"], None, "fixed-total WOS per-node count")
            assert_equal(wos_summary["worker_mode"], "total", "fixed-total WOS worker mode")
            assert_equal(wos_summary["worker_source"], "explicit-total", "fixed-total WOS worker source")

            linux_hosts = ["wos-ubuntu-vm0.wos", "wos-ubuntu-vm1.wos", "wos-ubuntu-vm2.wos"]
            linux_step = runner.run_linux_mandelbench(
                total_args, suite_dir, "/tmp/test-suite", linux_hosts, linux_hosts[0]
            )
            linux_command = linux_step["command"]
            if "--threads" in linux_command:
                fail(f"fixed-total Linux Mandel command retained per-node mapping: {linux_command}")
            np_index = linux_command.index("--np")
            assert_equal(linux_command[np_index + 1], "32", "fixed-total Linux MPI ranks")
            linux_summary = json.loads(
                (suite_dir / "linux-mandelbench" / "result.json").read_text(encoding="utf-8")
            )
            assert_equal(linux_summary["threads"], 32, "fixed-total Linux worker count")
            assert_equal(linux_summary["threads_per_node"], None, "fixed-total Linux per-node count")
            assert_equal(linux_summary["worker_mode"], "total", "fixed-total Linux worker mode")
            assert_equal(
                linux_summary["worker_source"],
                "explicit-total",
                "fixed-total Linux worker source",
            )

            default_args = parser.parse_args(["--num-vms", "3", "--mandel-repeat", "0"])
            default_args.remote_suite_name = "test-suite-default"
            default_step = runner.run_linux_mandelbench(
                default_args,
                suite_dir,
                "/tmp/test-suite-default",
                linux_hosts,
                linux_hosts[0],
            )
            default_command = default_step["command"]
            threads_index = default_command.index("--threads")
            np_index = default_command.index("--np")
            assert_equal(default_command[threads_index + 1], "8", "legacy Linux ranks per node")
            assert_equal(default_command[np_index + 1], "24", "legacy Linux total ranks")
    finally:
        runner.prepare_wos_hosts = original_prepare
        runner.run_benchmark_command = original_run
        runner.fetch_remote_file = original_fetch
        runner.parse_mandel_report = original_parse


def test_wos_render_command_and_scene_evidence(runner) -> None:
    parser = runner.build_parser()
    host = "wos-0.wos"
    hosts = [host, "wos-1.wos"]
    duck_digest = "65bf938f54d6073e619e76e007820bbf980cdc3dc0daec0d94830ffc4ae54ab5"
    remote_calls: list[tuple[str, str, float | None]] = []

    original_prepare = runner.prepare_wos_hosts
    original_remote = runner.wos_remote_command
    original_run = runner.run_benchmark_command
    original_fetch = runner.fetch_remote_file
    original_optional_fetch = runner.fetch_optional_remote_file

    def fake_remote(remote_host, command, *, timeout=None):
        remote_calls.append((remote_host, command, timeout))
        return subprocess.CompletedProcess(
            [remote_host, command], 0, f"{duck_digest}  /srv/Duck.glb\n", ""
        )

    def fake_fetch(_fetcher, _host, _remote_path, local_path, *, timeout):
        del timeout
        if local_path.name == "metrics.json":
            local_path.write_text('{"elapsed_seconds":1.0}\n', encoding="utf-8")
        elif local_path.name == "status.json":
            local_path.write_text('{"coordinator_reserve_cpus":0}\n', encoding="utf-8")
        elif local_path.name == "ipc_profile.json":
            local_path.write_text('{"effective_reserve_cpus":0}\n', encoding="utf-8")
        elif local_path.name == "preview.png":
            local_path.write_bytes(b"synthetic preview")
        else:
            fail(f"unexpected render fetch target: {local_path}")

    runner.prepare_wos_hosts = lambda _args, _hosts: None
    runner.wos_remote_command = fake_remote
    runner.run_benchmark_command = lambda _args, _step_dir, command: (
        subprocess.CompletedProcess(command, 0, "", ""),
        [],
    )
    runner.fetch_remote_file = fake_fetch
    runner.fetch_optional_remote_file = (
        lambda _fetcher, _host, _remote_path, _local_path, *, timeout: False
    )
    try:
        optimal_args = parser.parse_args(
            ["--num-vms", "2", "--wos-render-tuning", "optimal"]
        )
        cases = runner.render_cases(optimal_args)
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            builtin_step = runner.run_wos_renderbench(
                optimal_args,
                suite_dir,
                "/tmp/render-evidence",
                host,
                hosts,
                cases[0],
                "node-threads",
            )
            builtin_command = builtin_step["command"]
            reserve_index = builtin_command.index("--coordinator-reserve-cpus")
            assert_equal(
                builtin_command[reserve_index + 1], "0", "optimal node-thread reserve"
            )
            assert_equal(
                builtin_command.count("--coordinator-reserve-cpus"),
                1,
                "optimal node-thread reserve count",
            )
            if "--scene" in builtin_command:
                fail(f"builtin render unexpectedly supplied a scene: {builtin_command}")
            builtin_result = json.loads(
                (suite_dir / "wos-render-default-scene-node-threads" / "result.json").read_text(
                    encoding="utf-8"
                )
            )
            assert_equal(builtin_result["scene_sha256"], None, "builtin scene digest")
            assert_equal(remote_calls, [], "builtin scene hash commands")

            duck_step = runner.run_wos_renderbench(
                optimal_args,
                suite_dir,
                "/tmp/render-evidence",
                host,
                hosts,
                cases[1],
                "process-per-core",
            )
            duck_command = duck_step["command"]
            reserve_index = duck_command.index("--coordinator-reserve-cpus")
            assert_equal(
                duck_command[reserve_index + 1], "0", "optimal process reserve"
            )
            scene_index = duck_command.index("--scene")
            assert_equal(duck_command[scene_index + 1], "/srv/Duck.glb", "Duck scene path")
            duck_result = json.loads(
                (suite_dir / "wos-render-duck-process-per-core" / "result.json").read_text(
                    encoding="utf-8"
                )
            )
            assert_equal(duck_result["scene_sha256"], duck_digest, "Duck scene digest")
            assert_equal(
                remote_calls,
                [(host, "sha256sum -- /srv/Duck.glb", optimal_args.wos_preflight_timeout)],
                "Duck scene hash command",
            )

        manual_args = parser.parse_args(["--num-vms", "2"])
        with tempfile.TemporaryDirectory() as tmp:
            manual_step = runner.run_wos_renderbench(
                manual_args,
                Path(tmp),
                "/tmp/render-manual",
                host,
                hosts,
                runner.render_cases(manual_args)[0],
                "node-threads",
            )
            if "--coordinator-reserve-cpus" in manual_step["command"]:
                fail(f"manual render unexpectedly forced coordinator reserve: {manual_step['command']}")

        runner.wos_remote_command = lambda remote_host, command, *, timeout=None: subprocess.CompletedProcess(
            [remote_host, command], 0, f"{'A' * 64}  /srv/Duck.glb\n", ""
        )
        try:
            runner.wos_remote_file_sha256(host, "/srv/Duck.glb", timeout=1.0)
        except RuntimeError as exc:
            if "malformed output" not in str(exc):
                fail(f"unexpected malformed scene digest diagnostic: {exc}")
        else:
            fail("uppercase WOS scene digest was accepted")
    finally:
        runner.prepare_wos_hosts = original_prepare
        runner.wos_remote_command = original_remote
        runner.run_benchmark_command = original_run
        runner.fetch_remote_file = original_fetch
        runner.fetch_optional_remote_file = original_optional_fetch


def test_optimal_render_rejects_nonzero_coordinator_reserve(runner) -> None:
    original_argv = sys.argv
    try:
        sys.argv = [
            str(RUNNER_PATH),
            "--num-vms",
            "1",
            "--wos-render-tuning",
            "optimal",
            "--wos-coordinator-reserve-cpus",
            "1",
            "--skip-showcase",
            "--skip-mandelbench",
            "--skip-renderbench",
        ]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            try:
                runner.main()
            except SystemExit as exc:
                assert_equal(exc.code, 2, "optimal coordinator conflict exit")
            else:
                fail("optimal render accepted a nonzero coordinator reserve")
        if "incompatible with nonzero --wos-coordinator-reserve-cpus" not in stderr.getvalue():
            fail(f"missing optimal coordinator conflict diagnostic: {stderr.getvalue()}")

        with tempfile.TemporaryDirectory() as tmp:
            sys.argv = [
                str(RUNNER_PATH),
                "--num-vms",
                "1",
                "--os",
                "linux",
                "--wos-render-tuning",
                "optimal",
                "--wos-coordinator-reserve-cpus",
                "0",
                "--results-dir",
                tmp,
                "--skip-showcase",
                "--skip-mandelbench",
                "--skip-renderbench",
            ]
            with contextlib.redirect_stdout(io.StringIO()):
                assert_equal(runner.main(), 0, "optimal explicit zero coordinator reserve")
    finally:
        sys.argv = original_argv


def test_manifest_snapshots_validated_config(runner) -> None:
    source = ROOT / "configs" / "cluster_bench_3.json"
    original_argv = sys.argv
    original_remote = runner.wos_remote_command
    runner.wos_remote_command = runtime_remote_stub(EXPECTED_LAYOUTS[3])
    try:
        with tempfile.TemporaryDirectory() as tmp:
            sys.argv = [
                str(RUNNER_PATH),
                "--num-vms",
                "3",
                "--os",
                "wos",
                "--wos-cluster-config",
                str(source),
                "--mandel-total-workers",
                "32",
                "--results-dir",
                tmp,
                "--skip-showcase",
                "--skip-mandelbench",
                "--skip-renderbench",
            ]
            assert_equal(runner.main(), 0, "empty benchmark suite return code")
            suites = list(Path(tmp).glob("cross-os-suite-*"))
            assert_equal(len(suites), 1, "empty benchmark suite directory count")
            manifest = json.loads((suites[0] / "manifest.json").read_text(encoding="utf-8"))
            topology = manifest["wos_cluster"]
            assert_equal(topology["total_vcpus"], 32, "manifest aggregate vCPUs")
            assert_equal(topology["total_memory_mib"], 32768, "manifest aggregate memory")
            assert_equal(topology["runtime_validated"], True, "manifest runtime flag")
            assert_equal(
                topology["runtime_resources"]["collector"],
                "wos-procfs-v1",
                "manifest runtime collector",
            )
            assert_equal(
                topology["runtime_resources"]["total_online_vcpus"],
                32,
                "manifest runtime aggregate vCPUs",
            )
            assert_equal(
                manifest["mandel_workers"],
                {"mode": "total", "source": "explicit-total", "total": 32, "per_node": None},
                "manifest worker mode",
            )
            assert_equal(
                manifest["source"]["runner_sha256"],
                hashlib.sha256(RUNNER_PATH.read_bytes()).hexdigest(),
                "manifest runner hash",
            )
            snapshot = ROOT / topology["snapshot_file"]
            assert_equal(snapshot.read_bytes(), source.read_bytes(), "manifest topology snapshot")
    finally:
        sys.argv = original_argv
        runner.wos_remote_command = original_remote


def test_linux_only_manifest_preserves_config_only_evidence(runner) -> None:
    source = ROOT / "configs" / "cluster_bench_2.json"
    original_argv = sys.argv
    original_remote = runner.wos_remote_command
    runner.wos_remote_command = lambda *_args, **_kwargs: fail(
        "Linux-only run unexpectedly probed WOS runtime resources"
    )
    try:
        with tempfile.TemporaryDirectory() as tmp:
            sys.argv = [
                str(RUNNER_PATH),
                "--num-vms",
                "2",
                "--os",
                "linux",
                "--wos-cluster-config",
                str(source),
                "--results-dir",
                tmp,
                "--skip-showcase",
                "--skip-mandelbench",
                "--skip-renderbench",
            ]
            assert_equal(runner.main(), 0, "Linux-only empty benchmark suite return code")
            suites = list(Path(tmp).glob("cross-os-suite-*"))
            assert_equal(len(suites), 1, "Linux-only suite directory count")
            topology = json.loads(
                (suites[0] / "manifest.json").read_text(encoding="utf-8")
            )["wos_cluster"]
            assert_equal(topology["runtime_validated"], False, "Linux-only runtime flag")
            if "runtime_resources" in topology:
                fail("Linux-only manifest attached unobserved runtime resources")
    finally:
        sys.argv = original_argv
        runner.wos_remote_command = original_remote


def test_runtime_collection_failure_aborts_before_benchmarks(runner) -> None:
    source = ROOT / "configs" / "cluster_bench_1.json"
    original_argv = sys.argv
    original_remote = runner.wos_remote_command
    original_showcase = runner.run_wos_showcase
    showcase_called = False

    def unexpected_showcase(*_args, **_kwargs):
        nonlocal showcase_called
        showcase_called = True
        fail("WOS showcase started after runtime resource validation failed")

    runner.wos_remote_command = runtime_remote_stub(
        EXPECTED_LAYOUTS[1], vcpu_overrides={"wos-0": 31}
    )
    runner.run_wos_showcase = unexpected_showcase
    try:
        with tempfile.TemporaryDirectory() as tmp:
            sys.argv = [
                str(RUNNER_PATH),
                "--num-vms",
                "1",
                "--os",
                "wos",
                "--wos-cluster-config",
                str(source),
                "--results-dir",
                tmp,
                "--skip-mandelbench",
                "--skip-renderbench",
            ]
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                try:
                    runner.main()
                except SystemExit as exc:
                    assert_equal(exc.code, 2, "runtime resource checkpoint exit status")
                else:
                    fail("runtime resource checkpoint failure did not abort the suite")
            if "WOS runtime resource validation failed" not in stderr.getvalue():
                fail(f"missing runtime resource checkpoint diagnostic: {stderr.getvalue()}")
            assert_equal(showcase_called, False, "benchmark start after checkpoint failure")
            assert_equal(
                list(Path(tmp).glob("cross-os-suite-*")),
                [],
                "suite artifacts created before runtime validation",
            )
    finally:
        sys.argv = original_argv
        runner.wos_remote_command = original_remote
        runner.run_wos_showcase = original_showcase


def test_optional_perf_diagnostics_preserve_invalid_utf8(runner) -> None:
    parser = runner.build_parser()
    args = parser.parse_args(["--num-vms", "1"])
    original_remote_scripts = runner.REMOTE_SCRIPTS

    try:
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp) / "step"
            remote_scripts = Path(tmp) / "remote"
            remote_scripts.mkdir()
            remote_command = remote_scripts / "wos_ssh.sh"
            remote_command.write_text(
                "#!/bin/sh\n"
                "printf 'stdout:\\264\\n'\n"
                "printf 'stderr:\\265\\n' >&2\n",
                encoding="utf-8",
            )
            remote_command.chmod(0o755)
            runner.REMOTE_SCRIPTS = remote_scripts

            cpustat_entries = runner.collect_wos_cpustat(args, step_dir, ["wos-0.wos"], "before")
            report_entries = runner.collect_wos_perf_command(
                args,
                step_dir,
                ["wos-0.wos"],
                "before",
                "wki-report",
                ["wki-report"],
            )

            for entry in [*cpustat_entries, *report_entries]:
                assert_equal(entry["status"], "ok", "diagnostic capture status")
                assert_equal(entry["returncode"], 0, "diagnostic capture return code")
                assert_equal(
                    Path(entry["stdout_file"]).read_text(encoding="utf-8"),
                    "stdout:\\xb4\n",
                    "diagnostic stdout preserves invalid UTF-8",
                )
                assert_equal(
                    Path(entry["stderr_file"]).read_text(encoding="utf-8"),
                    "stderr:\\xb5\n",
                    "diagnostic stderr preserves invalid UTF-8",
                )
                metadata = json.loads(Path(entry["metadata_file"]).read_text(encoding="utf-8"))
                assert_equal(metadata["status"], "ok", "diagnostic metadata status")
                assert_equal(metadata["artifacts"], entry["artifacts"], "diagnostic metadata artifacts")
    finally:
        runner.REMOTE_SCRIPTS = original_remote_scripts


def main() -> None:
    runner = load_runner()
    tests = [
        test_committed_cluster_provenance,
        test_cluster_mismatch_rejections,
        test_runtime_resource_parsing,
        test_runtime_resource_collection,
        test_runtime_resource_mismatch_rejections,
        test_mandel_worker_modes,
        test_showcase_measurement_parsing,
        test_wos_showcase_timeout_is_finite_and_nonnegative,
        test_wos_cleanup_cwd_and_timeout_override,
        test_wos_stale_work_root_cleanup,
        test_wos_showcase_metrics_collection,
        test_wos_showcase_metrics_plumbing,
        test_mandel_command_composition,
        test_wos_render_command_and_scene_evidence,
        test_optimal_render_rejects_nonzero_coordinator_reserve,
        test_manifest_snapshots_validated_config,
        test_linux_only_manifest_preserves_config_only_evidence,
        test_runtime_collection_failure_aborts_before_benchmarks,
        test_optional_perf_diagnostics_preserve_invalid_utf8,
    ]
    for test in tests:
        test(runner)
    print(f"{len(tests)} cross-OS benchmark suite tests passed")


if __name__ == "__main__":
    main()
