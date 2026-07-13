#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
COMPARATOR_PATH = ROOT / "scripts" / "bench" / "compare_fixed_resource_scaling.py"

TOPOLOGY_LAYOUTS = {
    1: (("wos-0", 32, 32768),),
    2: (("wos-0", 16, 16384), ("wos-1", 16, 16384)),
    3: (
        ("wos-0", 11, 10923),
        ("wos-1", 11, 10923),
        ("wos-2", 10, 10922),
    ),
    4: (
        ("wos-0", 8, 8192),
        ("wos-1", 8, 8192),
        ("wos-2", 8, 8192),
        ("wos-3", 8, 8192),
    ),
}

RENDER_CASES = {
    "wos-render-default-scene-node-threads": (
        "default-scene",
        None,
        "node-threads",
        1000,
        1000,
        500,
    ),
    "wos-render-default-scene-process-per-core": (
        "default-scene",
        None,
        "process-per-core",
        1000,
        1000,
        500,
    ),
    "wos-render-duck-node-threads": (
        "duck",
        "/srv/Duck.glb",
        "node-threads",
        4000,
        4000,
        1600,
    ),
    "wos-render-duck-process-per-core": (
        "duck",
        "/srv/Duck.glb",
        "process-per-core",
        4000,
        4000,
        1600,
    ),
}


def fail(message: str) -> None:
    raise AssertionError(message)


def assert_equal(actual, expected, context: str) -> None:
    if actual != expected:
        fail(f"{context}: expected {expected!r}, got {actual!r}")


def load_comparator():
    module_name = "fixed_resource_scaling_under_test"
    spec = importlib.util.spec_from_file_location(module_name, COMPARATOR_PATH)
    if spec is None or spec.loader is None:
        fail(f"failed to load {COMPARATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        spec.loader.exec_module(module)
    assert_equal(stdout.getvalue(), "", "comparator import stdout")
    assert_equal(stderr.getvalue(), "", "comparator import stderr")
    return module


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        fail(f"expected JSON object in {path}")
    return payload


def render_result(elapsed: float, node_count: int, step_name: str) -> dict[str, object]:
    scene, scene_path, placement, width, height, spp = RENDER_CASES[step_name]
    layout = TOPOLOGY_LAYOUTS[node_count]
    host_records: list[dict[str, object]] = []
    for hostname, vcpus, _ in layout:
        if placement == "node-threads":
            configured_slots = 1
            runs = 2
            effective_threads = vcpus
        else:
            configured_slots = vcpus
            runs = vcpus + 1
            effective_threads = 1
        host_records.append(
            {
                "host": f"{hostname}.wos",
                "configured_slots": configured_slots,
                "configured_threads": vcpus,
                "runs": runs,
                "effective_threads": effective_threads,
                "render_ms_avg": 100.0,
                "render_ms_max": 110.0,
                "render_cpu_ms_avg": 95.0,
                "render_cpu_ms_max": 105.0,
                "thread_capacity_ms": 1000.0,
            }
        )

    total_tiles = ((width + 23) // 24) * ((height + 23) // 24)
    total_samples = width * height * spp
    worker_slots = sum(int(record["configured_slots"]) for record in host_records)
    completed_runs = sum(int(record["runs"]) for record in host_records)
    return {
        "benchmark": "renderbench",
        "backend": "ipc",
        "scene": scene,
        "scene_path": scene_path,
        "scene_sha256": None if scene_path is None else "d" * 64,
        "placement": placement,
        "width": width,
        "height": height,
        "spp": spp,
        "max_depth": 6,
        "tile_size": 24,
        "debug_edge_colors": False,
        "debug_render_mode": "pathtraced",
        "debug_constant_tile_us": 0,
        "debug_node_thread_batch_size": 0,
        "coordinator_reserve_cpus": 0,
        "node_worker_reserve_cpus": 0,
        "coordinator_skip_local_worker": False,
        "worker_output_queue_disabled": True,
        "single_thread_worker_queue_disabled": True,
        "live_preview": False,
        "preview_update_interval_seconds": 0.25,
        "final_image_enabled": False,
        "final_image_max_pixels": 0,
        "elapsed_seconds": elapsed,
        "done": True,
        "tiles_done": total_tiles,
        "total_tiles": total_tiles,
        "samples_done": total_samples,
        "total_samples": total_samples,
        "primary_samples": total_samples,
        "ipc_profile": {
            "tiles_done": total_tiles,
            "total_tiles": total_tiles,
            "node_worker_reserve_cpus": 0,
            "worker_output_queue_disabled": True,
            "single_thread_worker_queue_disabled": True,
            "process_persistent_workers": False,
            "persistent_batch_size": 0,
            "read_bytes": total_tiles * 4096,
            "read_calls": total_tiles,
            "worker_slots": worker_slots,
            "completed_runs": completed_runs,
            "effective_reserve_cpus": 0,
            "hosts": host_records,
        },
    }


def participants(node_count: int, total_work_units: int) -> list[dict[str, object]]:
    base, extra = divmod(total_work_units, node_count)
    return [
        {
            "host": f"wos-{index}.wos",
            "runner_host": f"wos-{index}.wos",
            "transport": "local" if index == 0 else "wki",
            "work_units": base + (1 if index < extra else 0),
        }
        for index in range(node_count)
    ]


def showcase_measurement(
    payload: dict[str, object],
    node_count: int,
    total_work_units: int,
    wki_route: str,
) -> dict[str, object]:
    return {
        **payload,
        "placement": "local-baseline" if node_count == 1 else "strict-on",
        "wki_route": wki_route,
        "launcher_host": "wos-0.wos",
        "total_work_units": total_work_units,
        "participants": participants(node_count, total_work_units),
    }


def showcase_measurements(scale: float, node_count: int) -> list[dict[str, object]]:
    return [
        showcase_measurement(
            {
                "benchmark": "wos_distributed_compile",
                "units": 32,
                "total_workers": 32,
                "artifact_digest": "compile-digest",
                "elapsed_seconds": 20.0 * scale,
            },
            node_count,
            32,
            "host-workspace",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_vfsbench_create",
                "path": "/tmp/wos-showcase-vfsbench",
                "iterations": 256,
                "elapsed_seconds": 3.0 * scale,
            },
            node_count,
            256,
            "host-path",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_vfsbench_rename",
                "path": "/tmp/wos-showcase-vfsbench",
                "iterations": 256,
                "elapsed_seconds": 2.5 * scale,
            },
            node_count,
            256,
            "host-path",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_git_clone",
                "repository": "file:///srv/fixture.git",
                "commit": "fixture-commit",
                "checkout_files": 100,
                "elapsed_seconds": 3.0 * scale,
            },
            node_count,
            32,
            "host-workspace",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_git_checkout",
                "commit": "fixture-commit",
                "checkout_files": 100,
                "elapsed_seconds": 2.0 * scale,
            },
            node_count,
            32,
            "host-workspace",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_python_sha256",
                "jobs": 32,
                "rounds": 1000,
                "digest": "python-sha256-digest",
                "elapsed_seconds": 4.0 * scale,
            },
            node_count,
            32,
            "local-runtime",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_python_json",
                "documents": 32,
                "rounds": 1000,
                "digest": "python-json-digest",
                "elapsed_seconds": 5.0 * scale,
            },
            node_count,
            32,
            "local-runtime",
        ),
    ]


def runtime_resources(node_count: int) -> dict[str, object]:
    nodes = [
        {
            "hostname": hostname,
            "online_vcpus": vcpus,
            "mem_total_kib": (memory_mib - 1) * 1024,
        }
        for hostname, vcpus, memory_mib in TOPOLOGY_LAYOUTS[node_count]
    ]
    return {
        "collector": "wos-procfs-v1",
        "memory_semantics": "allocator-visible-usable-kib",
        "nodes": nodes,
        "total_online_vcpus": sum(int(node["online_vcpus"]) for node in nodes),
        "total_mem_total_kib": sum(int(node["mem_total_kib"]) for node in nodes),
    }


def write_run(
    root: Path,
    node_count: int,
    repeat: int,
    *,
    scale: float = 1.0,
    runtime_validated: bool = True,
    include_runtime_resources: bool = True,
    revision: str = "revision-a",
    runner_sha256: str = "runner-hash",
    dirty: bool = False,
    include_showcase: bool = True,
    failed_step: str | None = None,
) -> Path:
    run_dir = root / f"n{node_count}-r{repeat}"
    jitter = (0.99, 1.0, 1.01)[repeat % 3]
    measured_scale = scale * jitter
    steps: list[dict[str, object]] = []

    mandel_path = run_dir / "mandel.json"
    write_json(
        mandel_path,
        {
            "benchmark": "mandelbench",
            "width": 2000,
            "height": 2000,
            "max_iteration": 5000,
            "threads": 32,
            "worker_mode": "total",
            "worker_source": "explicit-total",
            "repeat": 5,
            "hosts": [
                f"{hostname}.wos" for hostname, _, _ in TOPOLOGY_LAYOUTS[node_count]
            ],
            "median": 10.0 * measured_scale,
        },
    )
    steps.append(
        {
            "name": "wos-mandelbench",
            "ok": failed_step != "wos-mandelbench",
            "result_file": mandel_path.name,
        }
    )

    for index, step_name in enumerate(RENDER_CASES):
        render_path = run_dir / f"render-{index}.json"
        write_json(
            render_path,
            render_result(30.0 * measured_scale, node_count, step_name),
        )
        steps.append(
            {
                "name": step_name,
                "ok": failed_step != step_name,
                "result_file": render_path.name,
            }
        )

    if include_showcase:
        showcase_path = run_dir / "showcase.json"
        write_json(
            showcase_path,
            {
                "benchmark": "showcase",
                "measurements": showcase_measurements(measured_scale, node_count),
            },
        )
        steps.append(
            {
                "name": "wos-showcase",
                "ok": failed_step != "wos-showcase",
                "result_file": showcase_path.name,
            }
        )

    config_relative = f"configs/cluster_bench_{node_count}.json"
    config_bytes = (ROOT / config_relative).read_bytes()
    config_sha256 = hashlib.sha256(config_bytes).hexdigest()
    topology_snapshot = run_dir / "wos-cluster-config.json"
    topology_snapshot.write_bytes(config_bytes)
    topology: dict[str, object] = {
        "config_validated": True,
        "config_path": config_relative,
        "config_sha256": config_sha256,
        "snapshot_file": topology_snapshot.name,
        "runtime_validated": runtime_validated,
        "node_count": node_count,
        "total_vcpus": 32,
        "total_memory_mib": 32768,
        "nodes": [
            {
                "hostname": hostname,
                "vcpus": vcpus,
                "memory_mib": memory_mib,
            }
            for hostname, vcpus, memory_mib in TOPOLOGY_LAYOUTS[node_count]
        ],
    }
    if runtime_validated and include_runtime_resources:
        topology["runtime_resources"] = runtime_resources(node_count)

    manifest = {
        "timestamp": f"2026-07-13T{node_count:02d}:{repeat:02d}:00+00:00",
        "num_vms": node_count,
        "wos_hosts": [
            f"{hostname}.wos" for hostname, _, _ in TOPOLOGY_LAYOUTS[node_count]
        ],
        "wos_launcher": "wos-0.wos",
        "source": {
            "revision": revision,
            "dirty": dirty,
            "runner_sha256": runner_sha256,
        },
        "wos_cluster": topology,
        "mandel_workers": {
            "mode": "total",
            "source": "explicit-total",
            "total": 32,
            "per_node": None,
        },
        "steps": steps,
    }
    manifest_path = run_dir / "manifest.json"
    write_json(manifest_path, manifest)
    return manifest_path


def complete_matrix(
    root: Path, *, scales: dict[int, float] | None = None, **kwargs
) -> list[Path]:
    scales = scales or {}
    return [
        write_run(root, node_count, repeat, scale=scales.get(node_count, 1.0), **kwargs)
        for node_count in (1, 2, 3, 4)
        for repeat in range(3)
    ]


def result_path_for(manifest_path: Path, step_name: str) -> Path:
    manifest = read_json(manifest_path)
    steps = manifest.get("steps")
    if not isinstance(steps, list):
        fail(f"manifest has no steps: {manifest_path}")
    for step in steps:
        if isinstance(step, dict) and step.get("name") == step_name:
            raw_path = step.get("result_file")
            if not isinstance(raw_path, str):
                fail(f"step {step_name} has no result file")
            path = Path(raw_path)
            return path if path.is_absolute() else manifest_path.parent / path
    fail(f"manifest has no step {step_name}")


def expect_error(comparator, manifests: list[Path], token: str, **kwargs) -> None:
    try:
        comparator.compare_runs(manifests, **kwargs)
    except comparator.ComparisonError as exc:
        if token not in str(exc):
            fail(f"unexpected comparison diagnostic for {token!r}: {exc}")
    else:
        fail(f"comparison unexpectedly accepted invalid input requiring {token!r}")


def test_exact_two_percent_boundary_and_repeated_medians(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        manifests = complete_matrix(Path(tmp), scales={2: 1.02, 3: 1.02, 4: 1.02})
        result = comparator.compare_runs(manifests)
    assert_equal(result["pass"], True, "exact 2% result")
    assert_equal(
        result["resource_evidence"],
        "configured+runtime-observed",
        "runtime resource evidence",
    )
    measurement_count = sum(
        len(names) for names in comparator.REQUIRED_MEASUREMENTS_BY_FAMILY.values()
    )
    assert_equal(len(result["rows"]), measurement_count * 4, "comparison row count")
    four_node = [row for row in result["rows"] if row["node_count"] == 4]
    if not four_node or any(abs(row["ratio"] - 1.02) > 1e-12 for row in four_node):
        fail(f"four-node ratios are not exactly on the boundary: {four_node}")
    if any(row["samples"] != 3 for row in result["rows"]):
        fail("comparison did not aggregate all three repeated manifests")


def test_ratio_over_boundary_fails(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        manifests = complete_matrix(Path(tmp), scales={4: 1.0201})
        result = comparator.compare_runs(manifests)
    assert_equal(result["pass"], False, "over-boundary result")
    if not any("at 4 nodes" in message for message in result["failures"]):
        fail(f"over-boundary failures are missing: {result['failures']}")


def test_missing_workload_topology_and_repeat_are_reported(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        manifests = [
            write_run(root, node_count, repeat, include_showcase=False)
            for node_count in (1, 2, 3)
            for repeat in range(2)
        ]
        result = comparator.compare_runs(manifests)
    assert_equal(result["pass"], False, "incomplete matrix result")
    messages = "\n".join(result["missing"])
    for token in [
        "missing topology: 4 node(s)",
        "need 3 repeats, found 2",
        "one-node baseline missing workload: distributed-compilation",
        "one-node baseline missing workload: python",
    ]:
        if token not in messages:
            fail(f"incomplete matrix diagnostic is missing {token!r}: {messages}")


def test_partial_comparison_and_manifest_relative_result_paths(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        manifests = complete_matrix(Path(tmp), include_showcase=False)
        first_manifest = read_json(manifests[0])
        first_step = first_manifest["steps"][0]
        if Path(first_step["result_file"]).is_absolute():
            fail("synthetic result path is not manifest-relative")
        result = comparator.compare_runs(
            manifests,
            required_workloads=("mandelbrot", "rendering"),
        )
    assert_equal(result["pass"], True, "Mandel/render-only comparison")
    assert_equal(len(result["rows"]), 20, "Mandel/render-only row count")


def test_config_only_is_provisional_and_nonpassing(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        manifests = complete_matrix(Path(tmp), runtime_validated=False)
        result = comparator.compare_runs(manifests)
    assert_equal(result["pass"], False, "config-only acceptance result")
    assert_equal(
        result["provisional_performance_pass"], True, "config-only provisional result"
    )
    assert_equal(result["resource_evidence"], "config-only", "config-only label")
    if not any(
        "structured runtime CPU/memory observations" in message
        for message in result["missing"]
    ):
        fail(f"runtime evidence diagnostic is missing: {result['missing']}")


def test_bare_runtime_boolean_is_rejected(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        manifests = complete_matrix(root / "bare", include_runtime_resources=False)
        expect_error(
            comparator,
            manifests,
            "runtime_validated requires structured runtime_resources",
        )

        wrong_cpus = complete_matrix(root / "wrong-cpus")
        payload = read_json(wrong_cpus[-1])
        payload["wos_cluster"]["runtime_resources"]["nodes"][-1]["online_vcpus"] = 7
        write_json(wrong_cpus[-1], payload)
        expect_error(comparator, wrong_cpus, "runtime-observed online CPUs")

        impossible_memory = complete_matrix(root / "impossible-memory")
        payload = read_json(impossible_memory[-1])
        payload["wos_cluster"]["runtime_resources"]["nodes"][-1]["mem_total_kib"] = (
            8193 * 1024
        )
        write_json(impossible_memory[-1], payload)
        expect_error(comparator, impossible_memory, "configured-RAM acceptance window")

        implausible_memory = complete_matrix(root / "implausible-memory")
        payload = read_json(implausible_memory[-1])
        payload["wos_cluster"]["runtime_resources"]["nodes"][-1]["mem_total_kib"] = 1
        write_json(implausible_memory[-1], payload)
        expect_error(comparator, implausible_memory, "configured-RAM acceptance window")


def test_provenance_launcher_and_exact_topology_reject(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        duplicates = complete_matrix(root / "duplicates")
        expect_error(
            comparator, [*duplicates, duplicates[0]], "duplicate manifest paths"
        )

        dirty = complete_matrix(root / "dirty")
        payload = read_json(dirty[0])
        payload["source"]["dirty"] = True
        write_json(dirty[0], payload)
        expect_error(comparator, dirty, "clean source tree")

        revision = complete_matrix(root / "revision")
        payload = read_json(revision[-1])
        payload["source"]["revision"] = "revision-b"
        write_json(revision[-1], payload)
        expect_error(comparator, revision, "different source revisions")

        runner = complete_matrix(root / "runner")
        payload = read_json(runner[-1])
        payload["source"]["runner_sha256"] = "other-runner-hash"
        write_json(runner[-1], payload)
        expect_error(comparator, runner, "different benchmark runner hashes")

        copied_timestamp = complete_matrix(root / "timestamp")
        first = read_json(copied_timestamp[0])
        last = read_json(copied_timestamp[-1])
        last["timestamp"] = first["timestamp"]
        write_json(copied_timestamp[-1], last)
        expect_error(comparator, copied_timestamp, "timestamps are not distinct")

        launcher = complete_matrix(root / "launcher")
        payload = read_json(launcher[-1])
        payload["wos_launcher"] = "wos-3.wos"
        write_json(launcher[-1], payload)
        expect_error(comparator, launcher, "requires wos-0 as the fixed launcher")

        workers = complete_matrix(root / "workers")
        payload = read_json(workers[0])
        payload["mandel_workers"] = {
            "mode": "per-node",
            "source": "default-per-node",
            "total": 32,
            "per_node": 32,
        }
        write_json(workers[0], payload)
        expect_error(comparator, workers, "--mandel-total-workers 32")

        topology = complete_matrix(root / "topology")
        payload = read_json(topology[3])
        payload["wos_cluster"]["nodes"][0]["vcpus"] = 15
        payload["wos_cluster"]["nodes"][1]["vcpus"] = 17
        write_json(topology[3], payload)
        expect_error(comparator, topology, "does not match the committed 2-node")

        config_identity = complete_matrix(root / "config-identity")
        payload = read_json(config_identity[-1])
        payload["wos_cluster"]["config_sha256"] = "0" * 64
        write_json(config_identity[-1], payload)
        expect_error(comparator, config_identity, "topology config identity")

        config_snapshot = complete_matrix(root / "config-snapshot")
        payload = read_json(config_snapshot[-1])
        snapshot_file = payload["wos_cluster"]["snapshot_file"]
        if not isinstance(snapshot_file, str):
            fail("synthetic topology snapshot path is missing")
        (config_snapshot[-1].parent / snapshot_file).write_bytes(b"{}\n")
        expect_error(comparator, config_snapshot, "snapshot does not match")

        failed = complete_matrix(root / "failed", failed_step="wos-mandelbench")
        expect_error(comparator, failed, "required step wos-mandelbench failed")


def test_render_matrix_and_per_host_work_are_enforced(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        dynamic_runs = complete_matrix(root / "dynamic-runs")
        dynamic_candidate = result_path_for(
            dynamic_runs[-1], "wos-render-duck-node-threads"
        )
        dynamic_profile = read_json(dynamic_candidate)["ipc_profile"]
        if dynamic_profile["completed_runs"] <= dynamic_profile["worker_slots"]:
            fail("synthetic render profile does not exercise repeated dynamic runs")
        dynamic_result = comparator.compare_runs(
            dynamic_runs, required_workloads=("rendering",)
        )
        assert_equal(dynamic_result["pass"], True, "dynamic render runs comparison")

        incomplete = complete_matrix(root / "incomplete")
        payload = read_json(incomplete[-1])
        payload["steps"] = [
            step
            for step in payload["steps"]
            if step["name"] != "wos-render-duck-process-per-core"
        ]
        write_json(incomplete[-1], payload)
        expect_error(
            comparator,
            incomplete,
            "required render matrix is incomplete",
            required_workloads=("rendering",),
        )

        no_host_work = complete_matrix(root / "no-host-work")
        candidate = result_path_for(
            no_host_work[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["hosts"][-1]["runs"] = 0
        write_json(candidate, payload)
        expect_error(
            comparator,
            no_host_work,
            "has no positive completed work",
            required_workloads=("rendering",),
        )

        wrong_capacity = complete_matrix(root / "wrong-capacity")
        candidate = result_path_for(
            wrong_capacity[-1], "wos-render-default-scene-node-threads"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["hosts"][-1]["effective_threads"] = 7
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_capacity,
            "node-thread capacity",
            required_workloads=("rendering",),
        )

        incomplete_runs = complete_matrix(root / "incomplete-runs")
        candidate = result_path_for(
            incomplete_runs[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["completed_runs"] = (
            payload["ipc_profile"]["worker_slots"] - 1
        )
        write_json(candidate, payload)
        expect_error(
            comparator,
            incomplete_runs,
            "incomplete worker slots",
            required_workloads=("rendering",),
        )

        wrong_configured_slots = complete_matrix(root / "wrong-configured-slots")
        candidate = result_path_for(
            wrong_configured_slots[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["hosts"][-1]["configured_slots"] += 1
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_configured_slots,
            "configured slots do not match worker slots",
            required_workloads=("rendering",),
        )

        wrong_configured_threads = complete_matrix(root / "wrong-configured-threads")
        candidate = result_path_for(
            wrong_configured_threads[-1], "wos-render-duck-node-threads"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["hosts"][-1]["configured_threads"] -= 1
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_configured_threads,
            "node-thread capacity",
            required_workloads=("rendering",),
        )

        missing_scene_digest = complete_matrix(root / "missing-scene-digest")
        candidate = result_path_for(
            missing_scene_digest[-1], "wos-render-duck-node-threads"
        )
        payload = read_json(candidate)
        payload.pop("scene_sha256")
        write_json(candidate, payload)
        expect_error(
            comparator,
            missing_scene_digest,
            "scene-content digest",
            required_workloads=("rendering",),
        )


def test_showcase_routing_participant_coverage_and_python_rows(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        valid = complete_matrix(root / "valid")
        result = comparator.compare_runs(valid, required_workloads=("python",))
        python_rows = [
            row for row in result["rows"] if row["workload"].startswith("python:")
        ]
        assert_equal(
            len(python_rows), 8, "two Python measurements across four topologies"
        )
        assert_equal(
            {row["workload"] for row in python_rows},
            {"python:sha256", "python:json"},
            "Python measurement names",
        )

        bad_routing = complete_matrix(root / "bad-routing")
        candidate = result_path_for(bad_routing[-1], "wos-showcase")
        payload = read_json(candidate)
        payload["measurements"][0]["placement"] = "local-baseline"
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_routing,
            "invalid WKI placement/routing evidence",
            required_workloads=("distributed-compilation",),
        )

        bad_runner = complete_matrix(root / "bad-runner")
        candidate = result_path_for(bad_runner[-1], "wos-showcase")
        payload = read_json(candidate)
        payload["measurements"][0]["participants"][-1]["runner_host"] = "wos-0.wos"
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_runner,
            "invalid WKI work evidence",
            required_workloads=("distributed-compilation",),
        )

        incomplete_coverage = complete_matrix(root / "incomplete-coverage")
        candidate = result_path_for(incomplete_coverage[-1], "wos-showcase")
        payload = read_json(candidate)
        payload["measurements"][0]["participants"].pop()
        write_json(candidate, payload)
        expect_error(
            comparator,
            incomplete_coverage,
            "participants do not match the topology",
            required_workloads=("distributed-compilation",),
        )


def run_cli(comparator, argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        rc = comparator.main(argv)
    return rc, stdout.getvalue(), stderr.getvalue()


def manifest_argv(manifests: list[Path]) -> list[str]:
    argv: list[str] = []
    for manifest in manifests:
        argv.extend(["--manifest", str(manifest)])
    return argv


def test_cli_return_codes_and_machine_readable_result(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        passing = complete_matrix(root / "passing")
        output = root / "comparison.json"
        rc, stdout, stderr = run_cli(
            comparator, [*manifest_argv(passing), "--json-output", str(output)]
        )
        assert_equal(rc, 0, "passing comparator CLI return code")
        result = read_json(output)
        assert_equal(result["pass"], True, "comparator CLI JSON pass")
        if "workload\tnodes\tsamples" not in stdout or stderr:
            fail(f"unexpected passing CLI output: stdout={stdout!r} stderr={stderr!r}")

        failing = complete_matrix(root / "failing", scales={4: 1.03})
        rc, _, stderr = run_cli(comparator, manifest_argv(failing))
        assert_equal(rc, 1, "failed acceptance CLI return code")
        if "FAIL\t" not in stderr:
            fail(f"failed acceptance CLI diagnostic is missing: {stderr!r}")

        malformed = root / "malformed.json"
        write_json(malformed, {"timestamp": "2026-07-13T00:00:00+00:00"})
        rc, _, stderr = run_cli(comparator, ["--manifest", str(malformed)])
        assert_equal(rc, 2, "malformed input CLI return code")
        if "error:" not in stderr:
            fail(f"malformed input CLI diagnostic is missing: {stderr!r}")


def main() -> None:
    comparator = load_comparator()
    tests = [
        test_exact_two_percent_boundary_and_repeated_medians,
        test_ratio_over_boundary_fails,
        test_missing_workload_topology_and_repeat_are_reported,
        test_partial_comparison_and_manifest_relative_result_paths,
        test_config_only_is_provisional_and_nonpassing,
        test_bare_runtime_boolean_is_rejected,
        test_provenance_launcher_and_exact_topology_reject,
        test_render_matrix_and_per_host_work_are_enforced,
        test_showcase_routing_participant_coverage_and_python_rows,
        test_cli_return_codes_and_machine_readable_result,
    ]
    for test in tests:
        test(comparator)
    print(f"{len(tests)} fixed-resource scaling comparator tests passed")


if __name__ == "__main__":
    main()
