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
    host = "wos-0.wos"
    hosts = [host, "wos-1.wos"]
    fetches: list[tuple[str, Path]] = []
    optional_fetches: list[tuple[str, Path]] = []

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
                '{"benchmark":"duplicate","sequence":1}\n'
                '{"benchmark":"duplicate","sequence":2}\n',
                encoding="utf-8",
            )
        else:
            fail(f"unexpected showcase fetch: {remote_path}")

    runner.prepare_wos_hosts = lambda _args, _hosts: None
    runner.run_benchmark_command = lambda _args, _step_dir, command: (
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
                [
                    {"benchmark": "duplicate", "sequence": 1},
                    {"benchmark": "duplicate", "sequence": 2},
                ],
                "showcase result measurements",
            )
            metrics_artifact = suite_dir / "wos-showcase" / "metrics.jsonl"
            if runner.relpath(metrics_artifact) not in step["artifacts"]:
                fail(f"showcase metrics artifact is missing: {step['artifacts']}")
            remote_command = step["command"][-1]
            if f"--output-root {remote_output_root}" not in remote_command:
                fail(f"showcase remote output root is missing from command: {remote_command}")

        def fake_failed_run(_args, _step_dir, _command):
            raise RuntimeError("synthetic showcase failure")

        def fake_optional_fetch(_fetcher, _host, remote_path, local_path, *, timeout):
            del timeout
            optional_fetches.append((remote_path, local_path))
            return True

        runner.run_benchmark_command = fake_failed_run
        runner.fetch_optional_remote_file = fake_optional_fetch
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


def test_manifest_snapshots_validated_config(runner) -> None:
    source = ROOT / "configs" / "cluster_bench_3.json"
    original_argv = sys.argv
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


def main() -> None:
    runner = load_runner()
    tests = [
        test_committed_cluster_provenance,
        test_cluster_mismatch_rejections,
        test_mandel_worker_modes,
        test_showcase_measurement_parsing,
        test_wos_showcase_metrics_collection,
        test_wos_showcase_metrics_plumbing,
        test_mandel_command_composition,
        test_manifest_snapshots_validated_config,
    ]
    for test in tests:
        test(runner)
    print(f"{len(tests)} cross-OS benchmark suite tests passed")


if __name__ == "__main__":
    main()
