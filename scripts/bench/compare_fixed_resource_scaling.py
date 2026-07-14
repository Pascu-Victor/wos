#!/usr/bin/env python3
"""Compare repeated 1-4 node WOS runs against a fixed-resource one-node baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MAX_RATIO = 1.02
DEFAULT_MIN_REPEATS = 3
DEFAULT_NODES = (1, 2, 3, 4)
EXPECTED_TOTAL_VCPUS = 32
EXPECTED_TOTAL_MEMORY_MIB = 32768
EXPECTED_MANDEL_WORKERS = 32
# WOS MemTotal is allocator-visible usable RAM, not QEMU -m. Allow generous
# kernel/firmware reservations while rejecting a materially undersized guest.
MIN_RUNTIME_USABLE_MEMORY_PERCENT = 85
EXPECTED_TOPOLOGY_LAYOUTS = {
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
EXPECTED_TOPOLOGY_CONFIGS = {
    1: (
        "configs/cluster_bench_1.json",
        "aeb2c0d49a532e27baf2f116c6fdb6ec30007481d276720d1bb241ffb04d19d5",
    ),
    2: (
        "configs/cluster_bench_2.json",
        "9f753e84fed7db50ef402f12ec542ae239a3ce5a078140565c492d18d7fda62e",
    ),
    3: (
        "configs/cluster_bench_3.json",
        "cff869463ca6ee6b8cc2564932acac74a2adeb883f174fc29823f578d2a6211a",
    ),
    4: (
        "configs/cluster_bench_4.json",
        "c5d7352e150c8a95de78b1f30370d8ff037e446b8524fc112d3a4c93a41a12c8",
    ),
}
REQUIRED_RENDER_STEPS = (
    "wos-render-default-scene-node-threads",
    "wos-render-default-scene-process-per-core",
    "wos-render-duck-node-threads",
    "wos-render-duck-process-per-core",
)
EXPECTED_RENDER_CASES = {
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
ALL_WORKLOADS = (
    "distributed-compilation",
    "rendering",
    "mandelbrot",
    "file-create",
    "file-rename",
    "git-clone",
    "git-checkout",
    "python",
)


class ComparisonError(ValueError):
    pass


@dataclass(frozen=True)
class Measurement:
    family: str
    name: str
    value: float
    unit: str
    fingerprint: str


@dataclass(frozen=True)
class ShowcaseMetricSpec:
    family: str
    name: str
    metric: str
    fingerprint_fields: tuple[str, ...]
    wki_route: str
    evidence_contract: str | None = None
    count_field: str | None = None
    workspace_route: str | None = None
    participant_digest: bool = False
    expected_work_units: int | None = None
    process_identity: bool = False


JOB_MAP_EVIDENCE_CONTRACT = "wos-showcase-job-map-v1"
JOB_MAP_TOTAL_JOBS = 32
JOB_MAP_GIT_REPOSITORY = "wos-showcase-git-fixture-v1"
JOB_MAP_GIT_FILES = 100
JOB_MAP_GIT_BYTES = 100 * 16 * 1024
JOB_MAP_GIT_COMMIT = "01cc47f97d58c90edec1b043d7288563d82a923d"
JOB_MAP_GIT_TREE = "104396b28c8cbbe7f97e3d6a13704881d700f421"
JOB_MAP_GIT_DIGEST = "20c827ddcaa537afd3379325d782e732190950eaec535af107f4c1580949e1b9"
JOB_MAP_ROUNDS = {
    "quick": 20_000,
    "full": 200_000,
    "stress": 1_000_000,
}
JOB_MAP_PYTHON_DIGESTS = {
    "wos_python_sha256": {
        "quick": "24e31aa0d4cf8694503463fec69aef8b1bfa91a5cc7d2a887b497576d9e33887",
        "full": "da2e3b5fca1be6e37b4fe2bb4e5b118fd1fd090fb77be8528686cb49734007a9",
        "stress": "c670c4081279ba457b212b76af90859fb08c7aea88acf97ff1fcf1bf4ae52e5c",
    },
    "wos_python_json": {
        "quick": "63f0520ef6c8664e16473f9936dd84d09c56c7a0d2a35a8ec3fb519ffb0adade",
        "full": "79332e7c99c2097d7ec9a0603433a159925abbd8c44cec38c258ae7600926672",
        "stress": "1586dfe1209b9d4266e21b333e22eb56d2420c1e246b99b5901ac99bd2f3fb33",
    },
}
SHOWCASE_METADATA_COUNTS = {
    "quick": 32,
    "full": 256,
    "stress": 1000,
}
JOB_MAP_WORK_ROOT_PREFIX = "/tmp/wos-showcase-fixed-"
JOB_MAP_RUNTIME_PATHS = {
    "/root/wos-showcase",
    "/usr",
    "/bin",
    "/lib",
    "/lib64",
    "/libexec",
    "/share",
    "/tmp",
}
JOB_MAP_RUNTIME_PROVENANCE_FIELDS = {
    "helper_sha256",
    "python_sha256",
    "python_hashlib_runtime_sha256",
    "python_json_runtime_sha256",
    "git_sha256",
    "git_upload_pack_sha256",
    "wkictl_sha256",
}
EXPECTED_FIXED_RESOURCE_HELPER_SHA256 = hashlib.sha256(
    (
        ROOT
        / "configs"
        / "rootfs"
        / "root"
        / "wos-showcase"
        / "fixed_resource_workloads.py"
    ).read_bytes()
).hexdigest()
COMPILE_WORKLOAD_ID = "wos-live-cpp-32-tu-v1"
COMPILE_SOURCE_SHA256 = (
    "aa52bc6a7f7f5b58904b6c1d06fb7f813c8567c97470fbe4161a4e691a60c726"
)
COMPILE_COMPILER_PATH = "/usr/bin/clang++"
COMPILE_FLAGS = "-std=c++23 -O2 -fno-ident"
COMPILE_LINK_FLAGS = "-std=c++23 -O2 -Wl,--build-id=none"
COMPILE_CACHE_POLICY = "prewarmed-compiler-source-headers-all-hosts"
COMPILE_LAUNCH_POLICY = "one-controller-per-host-local-tu-workers"


SHOWCASE_METRICS = {
    "wos_distributed_compile": ShowcaseMetricSpec(
        "distributed-compilation",
        "distributed-compilation",
        "elapsed_seconds",
        (
            "workload_id",
            "source_sha256",
            "compiler_path",
            "compiler_version_sha256",
            "compiler_sha256",
            "wkictl_sha256",
            "compile_flags",
            "link_flags",
            "cache_policy",
            "launch_policy",
            "runtime_route",
            "runtime_paths",
            "workspace_route",
            "units",
            "total_workers",
            "artifact_digest",
            "total_work_units",
        ),
        "host-workspace",
        count_field="units",
        expected_work_units=32,
    ),
    "wos_vfsbench_create": ShowcaseMetricSpec(
        "file-create",
        "file-create",
        "elapsed_seconds",
        ("path", "iterations", "total_work_units"),
        "host-path",
        count_field="iterations",
        process_identity=True,
    ),
    "wos_vfsbench_rename": ShowcaseMetricSpec(
        "file-rename",
        "file-rename",
        "elapsed_seconds",
        ("path", "iterations", "total_work_units"),
        "host-path",
        count_field="iterations",
        process_identity=True,
    ),
    "wos_git_clone": ShowcaseMetricSpec(
        "git-clone",
        "git-clone",
        "elapsed_seconds",
        (
            "repository",
            "scale",
            "commit",
            "tree_oid",
            "checkout_files",
            "fixture_bytes",
            "fixture_digest",
            "artifact_digest",
            "cache_policy",
            "runtime_provenance",
            "total_work_units",
        ),
        "host-workspace",
        JOB_MAP_EVIDENCE_CONTRACT,
        workspace_route="host",
        participant_digest=True,
        expected_work_units=JOB_MAP_TOTAL_JOBS,
    ),
    "wos_git_checkout": ShowcaseMetricSpec(
        "git-checkout",
        "git-checkout",
        "elapsed_seconds",
        (
            "scale",
            "commit",
            "tree_oid",
            "checkout_files",
            "fixture_bytes",
            "fixture_digest",
            "artifact_digest",
            "cache_policy",
            "runtime_provenance",
            "total_work_units",
        ),
        "host-workspace",
        JOB_MAP_EVIDENCE_CONTRACT,
        workspace_route="host",
        participant_digest=True,
        expected_work_units=JOB_MAP_TOTAL_JOBS,
    ),
    "wos_python_sha256": ShowcaseMetricSpec(
        "python",
        "python:sha256",
        "elapsed_seconds",
        (
            "scale",
            "jobs",
            "rounds",
            "digest",
            "runtime_provenance",
            "total_work_units",
        ),
        "local-runtime",
        JOB_MAP_EVIDENCE_CONTRACT,
        count_field="jobs",
        participant_digest=True,
        expected_work_units=JOB_MAP_TOTAL_JOBS,
    ),
    "wos_python_json": ShowcaseMetricSpec(
        "python",
        "python:json",
        "elapsed_seconds",
        (
            "scale",
            "documents",
            "rounds",
            "digest",
            "runtime_provenance",
            "total_work_units",
        ),
        "local-runtime",
        JOB_MAP_EVIDENCE_CONTRACT,
        count_field="documents",
        participant_digest=True,
        expected_work_units=JOB_MAP_TOTAL_JOBS,
    ),
}

REQUIRED_MEASUREMENTS_BY_FAMILY = {
    "distributed-compilation": {"distributed-compilation"},
    "rendering": {
        f"rendering:{step.removeprefix('wos-render-')}"
        for step in REQUIRED_RENDER_STEPS
    },
    "mandelbrot": {"mandelbrot"},
    "file-create": {"file-create"},
    "file-rename": {"file-rename"},
    "git-clone": {"git-clone"},
    "git-checkout": {"git-checkout"},
    "python": {"python:sha256", "python:json"},
}


@dataclass(frozen=True)
class Run:
    path: Path
    run_id: str
    node_count: int
    runtime_validated: bool
    revision: str
    runner_sha256: str
    measurements: dict[str, Measurement]


def positive_finite(value: object, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ComparisonError(f"{context}: expected a positive finite number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ComparisonError(f"{context}: expected a positive finite number")
    return parsed


def fingerprint(payload: dict[str, Any], fields: tuple[str, ...], context: str) -> str:
    missing = [field for field in fields if field not in payload]
    if missing:
        raise ComparisonError(
            f"{context}: missing workload parameter(s): {', '.join(missing)}"
        )
    return json.dumps(
        {field: payload[field] for field in fields},
        sort_keys=True,
        separators=(",", ":"),
    )


def resolve_result_path(manifest_path: Path, raw_path: object) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise ComparisonError(f"{manifest_path}: benchmark step has no result_file")
    path = Path(raw_path)
    if path.is_absolute():
        return path
    root_path = ROOT / path
    if root_path.is_file():
        return root_path
    return manifest_path.parent / path


def read_json_object(path: Path, context: str) -> dict[str, Any]:
    if not path.is_file():
        raise ComparisonError(f"{context}: missing JSON file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ComparisonError(
            f"{context}: cannot read JSON object from {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ComparisonError(f"{context}: expected a JSON object in {path}")
    return payload


def load_manifest(path: Path) -> tuple[Path, dict[str, Any]]:
    manifest_path = path.resolve()
    return manifest_path, read_json_object(manifest_path, str(manifest_path))


def validate_run_id(manifest_path: Path, manifest: dict[str, Any]) -> str:
    run_id = manifest.get("timestamp")
    if not isinstance(run_id, str) or not run_id:
        raise ComparisonError(f"{manifest_path}: run timestamp is missing")
    try:
        parsed = datetime.fromisoformat(run_id)
    except ValueError as exc:
        raise ComparisonError(f"{manifest_path}: run timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise ComparisonError(f"{manifest_path}: run timestamp must include a timezone")
    return run_id


def validate_source(manifest_path: Path, manifest: dict[str, Any]) -> tuple[str, str]:
    source = manifest.get("source")
    if not isinstance(source, dict):
        raise ComparisonError(f"{manifest_path}: source provenance is missing")
    revision = source.get("revision")
    runner_sha256 = source.get("runner_sha256")
    if not isinstance(revision, str) or not revision:
        raise ComparisonError(f"{manifest_path}: source revision is missing")
    if source.get("dirty") is not False:
        raise ComparisonError(
            f"{manifest_path}: acceptance input must have a clean source tree"
        )
    if not isinstance(runner_sha256, str) or not runner_sha256:
        raise ComparisonError(f"{manifest_path}: runner hash is missing")
    return revision, runner_sha256


def validate_runtime_resources(
    manifest_path: Path,
    topology: dict[str, Any],
    expected_layout: tuple[tuple[str, int, int], ...],
) -> bool:
    if topology.get("runtime_validated") is not True:
        return False
    runtime = topology.get("runtime_resources")
    if not isinstance(runtime, dict):
        raise ComparisonError(
            f"{manifest_path}: runtime_validated requires structured runtime_resources"
        )
    if (
        runtime.get("collector") != "wos-procfs-v1"
        or runtime.get("memory_semantics") != "allocator-visible-usable-kib"
    ):
        raise ComparisonError(
            f"{manifest_path}: runtime resource collector semantics are missing"
        )
    nodes = runtime.get("nodes")
    if not isinstance(nodes, list) or len(nodes) != len(expected_layout):
        raise ComparisonError(
            f"{manifest_path}: runtime resource node records do not match the topology"
        )
    observed_vcpus: dict[str, int] = {}
    observed_memory_kib: dict[str, int] = {}
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ComparisonError(
                f"{manifest_path}: runtime resource node {index} is not an object"
            )
        hostname = node.get("hostname")
        online_vcpus = node.get("online_vcpus")
        mem_total_kib = node.get("mem_total_kib")
        if (
            not isinstance(hostname, str)
            or not hostname
            or hostname in observed_vcpus
            or isinstance(online_vcpus, bool)
            or not isinstance(online_vcpus, int)
            or online_vcpus <= 0
            or isinstance(mem_total_kib, bool)
            or not isinstance(mem_total_kib, int)
            or mem_total_kib <= 0
        ):
            raise ComparisonError(
                f"{manifest_path}: runtime resource node {index} is invalid"
            )
        observed_vcpus[hostname] = online_vcpus
        observed_memory_kib[hostname] = mem_total_kib
    expected_vcpus = {hostname: vcpus for hostname, vcpus, _ in expected_layout}
    if observed_vcpus != expected_vcpus:
        raise ComparisonError(
            f"{manifest_path}: runtime-observed online CPUs do not match the fixed topology"
        )
    expected_memory_mib = {
        hostname: memory_mib for hostname, _, memory_mib in expected_layout
    }
    for hostname, mem_total_kib in observed_memory_kib.items():
        configured_kib = expected_memory_mib[hostname] * 1024
        minimum_usable_kib = configured_kib * MIN_RUNTIME_USABLE_MEMORY_PERCENT // 100
        if mem_total_kib > configured_kib or mem_total_kib < minimum_usable_kib:
            raise ComparisonError(
                f"{manifest_path}: runtime-observed usable memory for {hostname} is outside the {MIN_RUNTIME_USABLE_MEMORY_PERCENT}%-100% configured-RAM acceptance window"
            )
    if runtime.get("total_online_vcpus") != EXPECTED_TOTAL_VCPUS:
        raise ComparisonError(
            f"{manifest_path}: runtime-observed aggregate online CPUs are not fixed at 32"
        )
    if runtime.get("total_mem_total_kib") != sum(observed_memory_kib.values()):
        raise ComparisonError(
            f"{manifest_path}: runtime-observed aggregate usable memory is inconsistent"
        )
    return True


def validate_topology(
    manifest_path: Path, manifest: dict[str, Any]
) -> tuple[int, bool]:
    node_count = manifest.get("num_vms")
    if (
        isinstance(node_count, bool)
        or not isinstance(node_count, int)
        or node_count <= 0
    ):
        raise ComparisonError(f"{manifest_path}: num_vms must be a positive integer")
    topology = manifest.get("wos_cluster")
    if not isinstance(topology, dict) or topology.get("config_validated") is not True:
        raise ComparisonError(
            f"{manifest_path}: validated WOS topology evidence is missing"
        )
    if topology.get("node_count") != node_count:
        raise ComparisonError(
            f"{manifest_path}: topology node_count does not match num_vms"
        )
    if topology.get("total_vcpus") != EXPECTED_TOTAL_VCPUS:
        raise ComparisonError(
            f"{manifest_path}: topology must total {EXPECTED_TOTAL_VCPUS} vCPUs"
        )
    if topology.get("total_memory_mib") != EXPECTED_TOTAL_MEMORY_MIB:
        raise ComparisonError(
            f"{manifest_path}: topology must total {EXPECTED_TOTAL_MEMORY_MIB} MiB"
        )
    nodes = topology.get("nodes")
    if not isinstance(nodes, list) or len(nodes) != node_count:
        raise ComparisonError(
            f"{manifest_path}: topology node records do not match num_vms"
        )
    hostnames: set[str] = set()
    configured: dict[str, tuple[int, int]] = {}
    total_vcpus = 0
    total_memory_mib = 0
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ComparisonError(
                f"{manifest_path}: topology node {index} is not an object"
            )
        hostname = node.get("hostname")
        vcpus = node.get("vcpus")
        memory_mib = node.get("memory_mib")
        if not isinstance(hostname, str) or not hostname or hostname in hostnames:
            raise ComparisonError(
                f"{manifest_path}: topology node {index} has an invalid or duplicate hostname"
            )
        if isinstance(vcpus, bool) or not isinstance(vcpus, int) or vcpus <= 0:
            raise ComparisonError(
                f"{manifest_path}: topology node {hostname} has invalid vCPUs"
            )
        if (
            isinstance(memory_mib, bool)
            or not isinstance(memory_mib, int)
            or memory_mib <= 0
        ):
            raise ComparisonError(
                f"{manifest_path}: topology node {hostname} has invalid memory"
            )
        hostnames.add(hostname)
        configured[hostname] = (vcpus, memory_mib)
        total_vcpus += vcpus
        total_memory_mib += memory_mib
    if (
        total_vcpus != EXPECTED_TOTAL_VCPUS
        or total_memory_mib != EXPECTED_TOTAL_MEMORY_MIB
    ):
        raise ComparisonError(
            f"{manifest_path}: topology node resources do not match aggregate totals"
        )
    expected_layout = EXPECTED_TOPOLOGY_LAYOUTS.get(node_count)
    if expected_layout is None:
        raise ComparisonError(f"{manifest_path}: acceptance supports only 1-4 nodes")
    expected_config_path, expected_config_sha256 = EXPECTED_TOPOLOGY_CONFIGS[node_count]
    if (
        topology.get("config_path") != expected_config_path
        or topology.get("config_sha256") != expected_config_sha256
    ):
        raise ComparisonError(
            f"{manifest_path}: topology config identity does not match {expected_config_path}"
        )
    snapshot_file = topology.get("snapshot_file")
    snapshot_path = resolve_result_path(manifest_path, snapshot_file)
    try:
        snapshot_sha256 = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ComparisonError(
            f"{manifest_path}: cannot read topology snapshot {snapshot_path}: {exc}"
        ) from exc
    if snapshot_sha256 != expected_config_sha256:
        raise ComparisonError(
            f"{manifest_path}: topology snapshot does not match the pinned config"
        )
    expected = {
        hostname: (vcpus, memory_mib) for hostname, vcpus, memory_mib in expected_layout
    }
    if configured != expected:
        raise ComparisonError(
            f"{manifest_path}: topology does not match the committed {node_count}-node CPU/RAM layout"
        )
    return node_count, validate_runtime_resources(
        manifest_path, topology, expected_layout
    )


def validate_mandel_workers(manifest_path: Path, manifest: dict[str, Any]) -> None:
    workers = manifest.get("mandel_workers")
    if not isinstance(workers, dict):
        raise ComparisonError(f"{manifest_path}: Mandel worker provenance is missing")
    expected = {
        "mode": "total",
        "source": "explicit-total",
        "total": EXPECTED_MANDEL_WORKERS,
        "per_node": None,
    }
    if workers != expected:
        raise ComparisonError(
            f"{manifest_path}: acceptance requires --mandel-total-workers {EXPECTED_MANDEL_WORKERS}"
        )


def result_steps(
    manifest_path: Path, manifest: dict[str, Any]
) -> dict[str, tuple[dict[str, Any], Path]]:
    steps = manifest.get("steps")
    if not isinstance(steps, list):
        raise ComparisonError(f"{manifest_path}: steps must be an array")
    by_name: dict[str, tuple[dict[str, Any], Path]] = {}
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            raise ComparisonError(f"{manifest_path}: step {index} is not an object")
        name = step.get("name")
        if not isinstance(name, str) or not name:
            raise ComparisonError(f"{manifest_path}: step {index} has no name")
        if name in by_name:
            raise ComparisonError(f"{manifest_path}: duplicate step {name!r}")
        result_path = (
            resolve_result_path(manifest_path, step.get("result_file"))
            if step.get("ok") is True
            else Path()
        )
        by_name[name] = (step, result_path)
    return by_name


def extract_mandel(
    manifest_path: Path,
    steps: dict[str, tuple[dict[str, Any], Path]],
    expected_hosts: set[str],
) -> Measurement | None:
    entry = steps.get("wos-mandelbench")
    if entry is None:
        return None
    step, result_path = entry
    if step.get("ok") is not True:
        raise ComparisonError(f"{manifest_path}: required step wos-mandelbench failed")
    result = read_json_object(result_path, f"{manifest_path}: wos-mandelbench")
    fields = (
        "width",
        "height",
        "max_iteration",
        "threads",
        "worker_mode",
        "worker_source",
        "repeat",
    )
    if (
        result.get("worker_mode") != "total"
        or result.get("threads") != EXPECTED_MANDEL_WORKERS
    ):
        raise ComparisonError(
            f"{manifest_path}: Mandel result does not use the fixed total worker budget"
        )
    raw_hosts = result.get("hosts")
    if (
        not isinstance(raw_hosts, list)
        or {
            normalize_wos_hostname(host)
            for host in raw_hosts
            if isinstance(host, str) and host
        }
        != expected_hosts
        or len(raw_hosts) != len(expected_hosts)
    ):
        raise ComparisonError(
            f"{manifest_path}: Mandel target hosts do not match the fixed topology"
        )
    return Measurement(
        family="mandelbrot",
        name="mandelbrot",
        value=positive_finite(result.get("median"), f"{manifest_path}: Mandel median"),
        unit="seconds",
        fingerprint=fingerprint(result, fields, f"{manifest_path}: Mandel result"),
    )


def normalize_wos_hostname(hostname: str) -> str:
    return hostname.removesuffix(".wos")


def expected_wos_hosts(
    manifest_path: Path, manifest: dict[str, Any], node_count: int
) -> tuple[set[str], str]:
    raw_hosts = manifest.get("wos_hosts")
    if not isinstance(raw_hosts, list) or len(raw_hosts) != node_count:
        raise ComparisonError(f"{manifest_path}: wos_hosts does not match num_vms")
    hosts: set[str] = set()
    for raw_host in raw_hosts:
        if not isinstance(raw_host, str) or not raw_host:
            raise ComparisonError(
                f"{manifest_path}: wos_hosts contains an invalid hostname"
            )
        hosts.add(normalize_wos_hostname(raw_host))
    if len(hosts) != node_count:
        raise ComparisonError(
            f"{manifest_path}: wos_hosts contains duplicate hostnames"
        )
    topology = manifest["wos_cluster"]
    topology_hosts = {
        normalize_wos_hostname(node["hostname"]) for node in topology["nodes"]
    }
    if hosts != topology_hosts:
        raise ComparisonError(
            f"{manifest_path}: wos_hosts does not match the validated topology"
        )
    launcher = manifest.get("wos_launcher")
    if not isinstance(launcher, str) or not launcher:
        raise ComparisonError(f"{manifest_path}: wos_launcher is missing")
    normalized_launcher = normalize_wos_hostname(launcher)
    if normalized_launcher != "wos-0" or normalized_launcher not in hosts:
        raise ComparisonError(
            f"{manifest_path}: acceptance requires wos-0 as the fixed launcher"
        )
    return hosts, normalized_launcher


def validate_render_completion(
    manifest_path: Path,
    step_name: str,
    result: dict[str, Any],
    expected_hosts: set[str],
    expected_vcpus: dict[str, int],
) -> None:
    expected_case = EXPECTED_RENDER_CASES.get(step_name)
    if expected_case is None:
        raise ComparisonError(
            f"{manifest_path}: unexpected render acceptance step {step_name}"
        )
    scene, scene_path, placement, width, height, spp = expected_case
    expected_result = {
        "benchmark": "renderbench",
        "backend": "ipc",
        "scene": scene,
        "scene_path": scene_path,
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
    }
    if any(result.get(field) != value for field, value in expected_result.items()):
        raise ComparisonError(
            f"{manifest_path}: {step_name} does not match the fixed render workload"
        )
    scene_sha256 = result.get("scene_sha256")
    if scene == "default-scene":
        valid_scene_digest = scene_sha256 is None and "scene_sha256" in result
    else:
        valid_scene_digest = (
            isinstance(scene_sha256, str)
            and len(scene_sha256) == 64
            and all(character in "0123456789abcdef" for character in scene_sha256)
        )
    if not valid_scene_digest:
        raise ComparisonError(
            f"{manifest_path}: {step_name} has no fixed scene-content digest"
        )
    if result.get("done") is not True:
        raise ComparisonError(f"{manifest_path}: {step_name} did not report completion")
    total_tiles = result.get("total_tiles")
    tiles_done = result.get("tiles_done")
    expected_tiles = ((width + 23) // 24) * ((height + 23) // 24)
    expected_samples = width * height * spp
    if (
        isinstance(total_tiles, bool)
        or not isinstance(total_tiles, int)
        or total_tiles != expected_tiles
        or tiles_done != total_tiles
        or result.get("samples_done") != expected_samples
        or result.get("total_samples") != expected_samples
        or result.get("primary_samples") != expected_samples
    ):
        raise ComparisonError(
            f"{manifest_path}: {step_name} did not complete every tile"
        )
    profile = result.get("ipc_profile")
    if not isinstance(profile, dict):
        raise ComparisonError(f"{manifest_path}: {step_name} has no IPC profile")
    if (
        profile.get("tiles_done") != total_tiles
        or profile.get("total_tiles") != total_tiles
        or profile.get("node_worker_reserve_cpus")
        != result.get("node_worker_reserve_cpus")
        or profile.get("worker_output_queue_disabled")
        != result.get("worker_output_queue_disabled")
        or profile.get("single_thread_worker_queue_disabled")
        != result.get("single_thread_worker_queue_disabled")
        or profile.get("process_persistent_workers") is not False
        or profile.get("persistent_batch_size") != 0
    ):
        raise ComparisonError(
            f"{manifest_path}: {step_name} IPC profile disagrees with the fixed workload"
        )
    for field in ("read_bytes", "read_calls"):
        value = profile.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ComparisonError(
                f"{manifest_path}: {step_name} IPC profile has no positive {field}"
            )
    worker_slots = profile.get("worker_slots")
    completed_runs = profile.get("completed_runs")
    if (
        isinstance(worker_slots, bool)
        or not isinstance(worker_slots, int)
        or worker_slots <= 0
        or isinstance(completed_runs, bool)
        or not isinstance(completed_runs, int)
        or completed_runs < worker_slots
    ):
        raise ComparisonError(
            f"{manifest_path}: {step_name} has incomplete worker slots"
        )
    if profile.get("effective_reserve_cpus") != 0:
        raise ComparisonError(
            f"{manifest_path}: {step_name} did not use the full fixed CPU budget"
        )
    if not isinstance(profile.get("process_persistent_workers"), bool):
        raise ComparisonError(
            f"{manifest_path}: {step_name} has no persistent-worker provenance"
        )
    raw_hosts = profile.get("hosts")
    if not isinstance(raw_hosts, list):
        raise ComparisonError(
            f"{manifest_path}: {step_name} IPC profile has no host records"
        )
    actual_hosts: set[str] = set()
    runs_by_host: dict[str, int] = {}
    configured_slots_by_host: dict[str, int] = {}
    configured_threads_by_host: dict[str, int] = {}
    threads_by_host: dict[str, float] = {}
    for host_record in raw_hosts:
        if not isinstance(host_record, dict) or not isinstance(
            host_record.get("host"), str
        ):
            raise ComparisonError(
                f"{manifest_path}: {step_name} has an invalid IPC host record"
            )
        host = normalize_wos_hostname(host_record["host"])
        configured_slots = host_record.get("configured_slots")
        configured_threads = host_record.get("configured_threads")
        runs = host_record.get("runs")
        effective_threads = host_record.get("effective_threads")
        if (
            isinstance(configured_slots, bool)
            or not isinstance(configured_slots, int)
            or configured_slots <= 0
            or isinstance(configured_threads, bool)
            or not isinstance(configured_threads, int)
            or configured_threads <= 0
        ):
            raise ComparisonError(
                f"{manifest_path}: {step_name} host {host} has no positive configured capacity"
            )
        if (
            isinstance(runs, bool)
            or not isinstance(runs, int)
            or runs <= 0
            or isinstance(effective_threads, bool)
            or not isinstance(effective_threads, (int, float))
            or not math.isfinite(float(effective_threads))
            or float(effective_threads) <= 0
        ):
            raise ComparisonError(
                f"{manifest_path}: {step_name} host {host} has no positive completed work"
            )
        for field in (
            "render_ms_avg",
            "render_ms_max",
            "render_cpu_ms_avg",
            "render_cpu_ms_max",
            "thread_capacity_ms",
        ):
            value = host_record.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ComparisonError(
                    f"{manifest_path}: {step_name} host {host} has no positive {field}"
                )
        actual_hosts.add(host)
        runs_by_host[host] = runs
        configured_slots_by_host[host] = configured_slots
        configured_threads_by_host[host] = configured_threads
        threads_by_host[host] = float(effective_threads)
    if len(actual_hosts) != len(raw_hosts) or actual_hosts != expected_hosts:
        raise ComparisonError(
            f"{manifest_path}: {step_name} worker hosts {sorted(actual_hosts)} do not match expected {sorted(expected_hosts)}"
        )
    if sum(runs_by_host.values()) != completed_runs:
        raise ComparisonError(
            f"{manifest_path}: {step_name} per-host runs do not match completed runs"
        )
    if sum(configured_slots_by_host.values()) != worker_slots:
        raise ComparisonError(
            f"{manifest_path}: {step_name} per-host configured slots do not match worker slots"
        )
    if placement == "node-threads":
        if worker_slots != len(expected_hosts):
            raise ComparisonError(
                f"{manifest_path}: {step_name} must launch one node-thread worker per host"
            )
        for host in expected_hosts:
            if (
                configured_slots_by_host[host] != 1
                or configured_threads_by_host[host] != expected_vcpus[host]
                or threads_by_host[host] != expected_vcpus[host]
            ):
                raise ComparisonError(
                    f"{manifest_path}: {step_name} node-thread capacity for {host} does not match the topology"
                )
    else:
        if worker_slots != EXPECTED_TOTAL_VCPUS:
            raise ComparisonError(
                f"{manifest_path}: {step_name} process-per-core worker total is not fixed at {EXPECTED_TOTAL_VCPUS}"
            )
        for host in expected_hosts:
            if (
                configured_slots_by_host[host] != expected_vcpus[host]
                or configured_threads_by_host[host] != expected_vcpus[host]
                or threads_by_host[host] != 1
            ):
                raise ComparisonError(
                    f"{manifest_path}: {step_name} process-per-core capacity for {host} does not match the topology"
                )


def extract_render(
    manifest_path: Path,
    steps: dict[str, tuple[dict[str, Any], Path]],
    expected_hosts: set[str],
    expected_vcpus: dict[str, int],
) -> list[Measurement]:
    measurements: list[Measurement] = []
    fields = (
        "benchmark",
        "backend",
        "scene",
        "scene_path",
        "scene_sha256",
        "placement",
        "width",
        "height",
        "spp",
        "max_depth",
        "tile_size",
        "debug_edge_colors",
        "debug_render_mode",
        "debug_constant_tile_us",
        "debug_node_thread_batch_size",
        "coordinator_reserve_cpus",
        "node_worker_reserve_cpus",
        "coordinator_skip_local_worker",
        "worker_output_queue_disabled",
        "single_thread_worker_queue_disabled",
        "live_preview",
        "preview_update_interval_seconds",
        "final_image_enabled",
        "final_image_max_pixels",
        "process_persistent_workers",
    )
    found_steps: set[str] = set()
    for step_name, (step, result_path) in sorted(steps.items()):
        if not step_name.startswith("wos-render-"):
            continue
        if step.get("ok") is not True:
            raise ComparisonError(
                f"{manifest_path}: required render step {step_name} failed"
            )
        result = read_json_object(result_path, f"{manifest_path}: {step_name}")
        validate_render_completion(
            manifest_path, step_name, result, expected_hosts, expected_vcpus
        )
        found_steps.add(step_name)
        name = f"rendering:{step_name.removeprefix('wos-render-')}"
        fingerprint_payload = dict(result)
        fingerprint_payload["process_persistent_workers"] = result["ipc_profile"][
            "process_persistent_workers"
        ]
        measurements.append(
            Measurement(
                family="rendering",
                name=name,
                value=positive_finite(
                    result.get("elapsed_seconds"),
                    f"{manifest_path}: {step_name} elapsed_seconds",
                ),
                unit="seconds",
                fingerprint=fingerprint(
                    fingerprint_payload, fields, f"{manifest_path}: {step_name}"
                ),
            )
        )
    missing_steps = sorted(set(REQUIRED_RENDER_STEPS) - found_steps)
    if missing_steps:
        raise ComparisonError(
            f"{manifest_path}: required render matrix is incomplete: {', '.join(missing_steps)}"
        )
    return measurements


def validate_showcase_participants(
    manifest_path: Path,
    benchmark: str,
    payload: dict[str, Any],
    spec: ShowcaseMetricSpec,
    expected_hosts: set[str],
    launcher: str,
) -> None:
    expected_placement = "local-baseline" if len(expected_hosts) == 1 else "strict-on"
    if (
        payload.get("placement") != expected_placement
        or payload.get("wki_route") != spec.wki_route
        or normalize_wos_hostname(str(payload.get("launcher_host", ""))) != launcher
    ):
        raise ComparisonError(
            f"{manifest_path}: {benchmark} has invalid WKI placement/routing evidence"
        )
    total_work_units = payload.get("total_work_units")
    if (
        isinstance(total_work_units, bool)
        or not isinstance(total_work_units, int)
        or total_work_units <= 0
    ):
        raise ComparisonError(
            f"{manifest_path}: {benchmark} has no fixed total work count"
        )
    if (
        spec.expected_work_units is not None
        and total_work_units != spec.expected_work_units
    ):
        raise ComparisonError(
            f"{manifest_path}: {benchmark} does not use the required fixed work total"
        )
    if spec.count_field is not None:
        count = payload.get(spec.count_field)
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count != total_work_units
        ):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} count field disagrees with fixed total work"
            )
    if benchmark == "wos_distributed_compile":
        total_workers = payload.get("total_workers")
        controller_count = payload.get("controller_count")
        runtime_paths = payload.get("runtime_paths")
        workspace_path = payload.get("workspace_path")
        workspace_suffix = (
            workspace_path.removeprefix(JOB_MAP_WORK_ROOT_PREFIX).removesuffix(
                "/distributed-compile"
            )
            if isinstance(workspace_path, str)
            and workspace_path.startswith(JOB_MAP_WORK_ROOT_PREFIX)
            and workspace_path.endswith("/distributed-compile")
            else ""
        )
        if (
            isinstance(total_workers, bool)
            or not isinstance(total_workers, int)
            or total_workers != 32
            or payload.get("workload_id") != COMPILE_WORKLOAD_ID
            or payload.get("source_sha256") != COMPILE_SOURCE_SHA256
            or payload.get("compiler_path") != COMPILE_COMPILER_PATH
            or not valid_lowercase_hex(payload.get("compiler_version_sha256"), 64)
            or not valid_lowercase_hex(payload.get("compiler_sha256"), 64)
            or not valid_lowercase_hex(payload.get("wkictl_sha256"), 64)
            or payload.get("compile_flags") != COMPILE_FLAGS
            or payload.get("link_flags") != COMPILE_LINK_FLAGS
            or payload.get("cache_policy") != COMPILE_CACHE_POLICY
            or payload.get("launch_policy") != COMPILE_LAUNCH_POLICY
            or isinstance(controller_count, bool)
            or not isinstance(controller_count, int)
            or controller_count != len(expected_hosts)
            or not valid_lowercase_hex(payload.get("artifact_digest"), 64)
        ):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} has invalid fixed compile evidence"
            )
        if (
            payload.get("runtime_route") != "local"
            or not isinstance(runtime_paths, list)
            or len(runtime_paths) != len(JOB_MAP_RUNTIME_PATHS)
            or any(not isinstance(path, str) for path in runtime_paths)
            or set(runtime_paths) != JOB_MAP_RUNTIME_PATHS
            or payload.get("workspace_route") != "host"
            or len(workspace_suffix) != 16
            or any(
                character not in "0123456789abcdef" for character in workspace_suffix
            )
            or workspace_path
            != f"{JOB_MAP_WORK_ROOT_PREFIX}{workspace_suffix}/distributed-compile"
        ):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} has invalid compile route evidence"
            )
    elif benchmark in ("wos_vfsbench_create", "wos_vfsbench_rename"):
        if payload.get("path") != "/tmp/wos-showcase-vfsbench":
            raise ComparisonError(
                f"{manifest_path}: {benchmark} has invalid HOST path evidence"
            )
    raw_participants = payload.get("participants")
    if not isinstance(raw_participants, list) or len(raw_participants) != len(
        expected_hosts
    ):
        raise ComparisonError(
            f"{manifest_path}: {benchmark} participants do not match the topology"
        )
    actual_hosts: set[str] = set()
    assigned_work = 0
    work_by_host: dict[str, int] = {}
    for index, participant in enumerate(raw_participants):
        if not isinstance(participant, dict):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} participant {index} is invalid"
            )
        raw_host = participant.get("host")
        raw_runner = participant.get("runner_host")
        work_units = participant.get("work_units")
        if not isinstance(raw_host, str) or not isinstance(raw_runner, str):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} participant {index} has no host identity"
            )
        host = normalize_wos_hostname(raw_host)
        runner = normalize_wos_hostname(raw_runner)
        expected_transport = "local" if host == launcher else "wki"
        if (
            not host
            or host in actual_hosts
            or runner != host
            or participant.get("transport") != expected_transport
            or isinstance(work_units, bool)
            or not isinstance(work_units, int)
            or work_units <= 0
        ):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} participant {index} has invalid WKI work evidence"
            )
        actual_hosts.add(host)
        work_by_host[host] = work_units
        assigned_work += work_units
    if actual_hosts != expected_hosts or assigned_work != total_work_units:
        raise ComparisonError(
            f"{manifest_path}: {benchmark} participant coverage/work does not match the fixed topology"
        )
    base, extra = divmod(total_work_units, len(expected_hosts))
    expected_work_by_host = {
        host: base + (1 if index < extra else 0)
        for index, host in enumerate(sorted(expected_hosts))
    }
    if work_by_host != expected_work_by_host:
        raise ComparisonError(
            f"{manifest_path}: {benchmark} participants do not use the canonical fixed partition"
        )
    if spec.process_identity:
        for index, participant in enumerate(raw_participants):
            host = normalize_wos_hostname(participant["host"])
            spawner = participant.get("spawner_host")
            remote_pid = participant.get("remote_pid")
            if (
                not isinstance(spawner, str)
                or normalize_wos_hostname(spawner) != launcher
                or isinstance(remote_pid, bool)
                or not isinstance(remote_pid, int)
                or (host == launcher and remote_pid != 0)
                or (host != launcher and remote_pid <= 0)
            ):
                raise ComparisonError(
                    f"{manifest_path}: {benchmark} participant {index} has invalid process identity evidence"
                )
    if spec.evidence_contract is not None:
        validate_showcase_job_map(
            manifest_path,
            benchmark,
            payload,
            spec,
            expected_hosts,
            launcher,
            total_work_units,
        )


def valid_lowercase_hex(value: object, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def validate_showcase_job_map(
    manifest_path: Path,
    benchmark: str,
    payload: dict[str, Any],
    spec: ShowcaseMetricSpec,
    expected_hosts: set[str],
    launcher: str,
    total_work_units: int,
) -> None:
    context = f"{manifest_path}: {benchmark}"
    if payload.get("evidence_contract") != spec.evidence_contract:
        raise ComparisonError(f"{context} has no supported job-map evidence contract")
    runtime_provenance = payload.get("runtime_provenance")
    if (
        not isinstance(runtime_provenance, dict)
        or set(runtime_provenance) != JOB_MAP_RUNTIME_PROVENANCE_FIELDS
        or any(
            not valid_lowercase_hex(value, 64) for value in runtime_provenance.values()
        )
        or runtime_provenance.get("helper_sha256")
        != EXPECTED_FIXED_RESOURCE_HELPER_SHA256
    ):
        raise ComparisonError(f"{context} has invalid node-local runtime provenance")
    scale = payload.get("scale")
    if not isinstance(scale, str) or scale not in JOB_MAP_ROUNDS:
        raise ComparisonError(f"{context} has invalid showcase scale evidence")
    if "route_path" not in payload:
        raise ComparisonError(f"{context} is missing observed route-path evidence")
    route_path = payload["route_path"]
    if spec.workspace_route == "host":
        suffix = (
            route_path.removeprefix(JOB_MAP_WORK_ROOT_PREFIX)
            if isinstance(route_path, str)
            else ""
        )
        if (
            not isinstance(route_path, str)
            or not route_path.startswith(JOB_MAP_WORK_ROOT_PREFIX)
            or len(suffix) != 16
            or any(character not in "0123456789abcdef" for character in suffix)
        ):
            raise ComparisonError(f"{context} has invalid HOST workspace evidence")
    elif route_path is not None:
        raise ComparisonError(
            f"{context} unexpectedly routes a local-runtime workspace"
        )

    participants = payload["participants"]
    all_job_ids: list[int] = []
    all_job_digests: dict[int, str] = {}
    ordered_hosts = sorted(expected_hosts)
    base, extra = divmod(total_work_units, len(ordered_hosts))
    expected_assignments: dict[str, tuple[int, list[int]]] = {}
    first_job = 0
    for host_index, host in enumerate(ordered_hosts):
        count = base + (1 if host_index < extra else 0)
        expected_assignments[host] = (
            count,
            list(range(first_job, first_job + count)),
        )
        first_job += count
    required_keys = {
        "host",
        "runner_host",
        "launcher_host",
        "remote_pid",
        "job_remote_pids",
        "strict_target",
        "transport",
        "work_units",
        "completed_work_units",
        "job_ids",
        "job_digests",
        "digest",
        "runtime_route",
        "runtime_paths",
        "workspace_route",
        "workspace_path",
    }
    for index, participant in enumerate(participants):
        if not required_keys.issubset(participant):
            raise ComparisonError(
                f"{context} participant {index} lacks job-map evidence"
            )
        host = normalize_wos_hostname(participant["host"])
        work_units = participant["work_units"]
        completed = participant["completed_work_units"]
        job_ids = participant["job_ids"]
        job_digests = participant["job_digests"]
        remote_pid = participant["remote_pid"]
        job_remote_pids = participant["job_remote_pids"]
        participant_launcher = participant["launcher_host"]
        if (
            not isinstance(participant_launcher, str)
            or not participant_launcher
            or normalize_wos_hostname(participant_launcher) != launcher
            or participant["strict_target"] is not True
            or isinstance(completed, bool)
            or not isinstance(completed, int)
            or completed != work_units
            or not isinstance(job_ids, list)
            or not isinstance(job_digests, list)
            or not isinstance(job_remote_pids, list)
            or len(job_ids) != work_units
            or len(job_digests) != work_units
            or len(job_remote_pids) != work_units
        ):
            raise ComparisonError(
                f"{context} participant {index} has inconsistent completion evidence"
            )
        if any(
            isinstance(job_id, bool) or not isinstance(job_id, int) or job_id < 0
            for job_id in job_ids
        ):
            raise ComparisonError(f"{context} participant {index} has invalid job IDs")
        expected_count, expected_ids = expected_assignments[host]
        if work_units != expected_count or job_ids != expected_ids:
            raise ComparisonError(
                f"{context} participant {index} has the wrong fixed host/job assignment"
            )
        if any(not valid_lowercase_hex(digest, 64) for digest in job_digests):
            raise ComparisonError(
                f"{context} participant {index} has invalid per-job digests"
            )
        if (
            participant["runtime_route"] != "local"
            or not isinstance(participant["runtime_paths"], list)
            or len(participant["runtime_paths"]) != len(JOB_MAP_RUNTIME_PATHS)
            or any(not isinstance(path, str) for path in participant["runtime_paths"])
            or set(participant["runtime_paths"]) != JOB_MAP_RUNTIME_PATHS
            or participant["workspace_route"] != spec.workspace_route
            or participant["workspace_path"] != route_path
        ):
            raise ComparisonError(
                f"{context} participant {index} has invalid route evidence"
            )
        if (
            isinstance(remote_pid, bool)
            or not isinstance(remote_pid, int)
            or any(
                isinstance(pid, bool) or not isinstance(pid, int)
                for pid in job_remote_pids
            )
        ):
            raise ComparisonError(
                f"{context} participant {index} has invalid PID evidence"
            )
        if host == launcher:
            if remote_pid != 0 or any(pid != 0 for pid in job_remote_pids):
                raise ComparisonError(
                    f"{context} local participant has remote PID evidence"
                )
        elif (
            remote_pid <= 0
            or any(pid <= 0 for pid in job_remote_pids)
            or len(set(job_remote_pids)) != len(job_remote_pids)
            or remote_pid in job_remote_pids
        ):
            raise ComparisonError(
                f"{context} remote participant has invalid PID evidence"
            )
        if spec.participant_digest and not valid_lowercase_hex(
            participant.get("digest"), 64
        ):
            raise ComparisonError(
                f"{context} participant {index} has invalid digest evidence"
            )
        participant_digest = hashlib.sha256()
        for job_id, job_digest in zip(job_ids, job_digests, strict=True):
            participant_digest.update(job_id.to_bytes(4, "big"))
            participant_digest.update(bytes.fromhex(job_digest))
            if job_id in all_job_digests:
                raise ComparisonError(f"{context} job map contains a duplicate job ID")
            all_job_digests[job_id] = job_digest
        if participant.get("digest") != participant_digest.hexdigest():
            raise ComparisonError(
                f"{context} participant {index} digest is not bound to its jobs"
            )
        all_job_ids.extend(job_ids)

    if sorted(all_job_ids) != list(range(total_work_units)):
        raise ComparisonError(
            f"{context} job map does not cover each fixed work unit exactly once"
        )
    aggregate_digest = hashlib.sha256()
    for job_id in range(total_work_units):
        aggregate_digest.update(job_id.to_bytes(4, "big"))
        aggregate_digest.update(bytes.fromhex(all_job_digests[job_id]))
    aggregate = aggregate_digest.hexdigest()

    if benchmark in ("wos_git_clone", "wos_git_checkout"):
        if not valid_lowercase_hex(
            payload.get("commit"), 40
        ) or not valid_lowercase_hex(payload.get("tree_oid"), 40):
            raise ComparisonError(f"{context} has invalid Git object identities")
        for field in ("fixture_digest", "artifact_digest"):
            if not valid_lowercase_hex(payload.get(field), 64):
                raise ComparisonError(f"{context} has invalid {field}")
        expected_cache_policy = (
            "warm-source-cold-destination"
            if benchmark == "wos_git_clone"
            else "post-clone-object-cache"
        )
        expected_job_digest = (
            hashlib.sha256(
                f"{payload['commit']}\0{payload['tree_oid']}".encode("ascii")
            ).hexdigest()
            if benchmark == "wos_git_clone"
            else payload.get("fixture_digest")
        )
        if (
            payload.get("commit") != JOB_MAP_GIT_COMMIT
            or payload.get("tree_oid") != JOB_MAP_GIT_TREE
            or payload.get("fixture_digest") != JOB_MAP_GIT_DIGEST
            or payload.get("checkout_files") != JOB_MAP_GIT_FILES
            or isinstance(payload.get("checkout_files"), bool)
            or not isinstance(payload.get("checkout_files"), int)
            or payload.get("fixture_bytes") != JOB_MAP_GIT_BYTES
            or isinstance(payload.get("fixture_bytes"), bool)
            or not isinstance(payload.get("fixture_bytes"), int)
            or payload.get("cache_policy") != expected_cache_policy
            or any(
                job_digest != expected_job_digest
                for job_digest in all_job_digests.values()
            )
            or payload.get("artifact_digest") != aggregate
        ):
            raise ComparisonError(f"{context} has invalid fixed Git workload evidence")
        if benchmark == "wos_git_clone":
            expected_uri = f"file://{route_path}/git-fixture.git"
            if (
                payload.get("repository") != JOB_MAP_GIT_REPOSITORY
                or payload.get("repository_uri") != expected_uri
            ):
                raise ComparisonError(f"{context} has invalid Git repository evidence")
    else:
        rounds = payload.get("rounds")
        expected_digest = JOB_MAP_PYTHON_DIGESTS[benchmark][scale]
        if (
            isinstance(rounds, bool)
            or not isinstance(rounds, int)
            or rounds != JOB_MAP_ROUNDS[scale]
            or not valid_lowercase_hex(payload.get("digest"), 64)
            or payload.get("digest") != aggregate
            or payload.get("digest") != expected_digest
        ):
            raise ComparisonError(f"{context} has invalid fixed Python work evidence")


def validate_git_phase_coherence(
    manifest_path: Path,
    clone: dict[str, Any],
    checkout: dict[str, Any],
) -> None:
    coherent_fields = (
        "scale",
        "commit",
        "tree_oid",
        "checkout_files",
        "fixture_bytes",
        "fixture_digest",
        "route_path",
    )
    if any(clone.get(field) != checkout.get(field) for field in coherent_fields):
        raise ComparisonError(
            f"{manifest_path}: Git clone/checkout phases do not describe one fixed fixture"
        )


def extract_showcase(
    manifest_path: Path,
    steps: dict[str, tuple[dict[str, Any], Path]],
    expected_hosts: set[str],
    launcher: str,
) -> list[Measurement]:
    entry = steps.get("wos-showcase")
    if entry is None:
        return []
    step, result_path = entry
    if step.get("ok") is not True:
        raise ComparisonError(f"{manifest_path}: required step wos-showcase failed")
    result = read_json_object(result_path, f"{manifest_path}: wos-showcase")
    raw_measurements = result.get("measurements", [])
    if not isinstance(raw_measurements, list):
        raise ComparisonError(
            f"{manifest_path}: showcase measurements must be an array"
        )
    measurements: list[Measurement] = []
    seen_names: set[str] = set()
    job_map_payloads: dict[str, dict[str, Any]] = {}
    showcase_scale = result.get("scale")
    if not isinstance(showcase_scale, str) or showcase_scale not in JOB_MAP_ROUNDS:
        raise ComparisonError(f"{manifest_path}: showcase scale evidence is invalid")
    for index, payload in enumerate(raw_measurements):
        if not isinstance(payload, dict):
            raise ComparisonError(
                f"{manifest_path}: showcase measurement {index} is not an object"
            )
        benchmark = payload.get("benchmark")
        if benchmark not in SHOWCASE_METRICS:
            continue
        spec = SHOWCASE_METRICS[benchmark]
        if spec.name in seen_names:
            raise ComparisonError(
                f"{manifest_path}: duplicate showcase measurement for {spec.name}"
            )
        seen_names.add(spec.name)
        if spec.evidence_contract is not None:
            if payload.get("scale") != showcase_scale:
                raise ComparisonError(
                    f"{manifest_path}: {benchmark} scale evidence disagrees with the showcase run"
                )
            job_map_payloads[benchmark] = payload
        if (
            benchmark in ("wos_vfsbench_create", "wos_vfsbench_rename")
            and payload.get("total_work_units")
            != SHOWCASE_METADATA_COUNTS[showcase_scale]
        ):
            raise ComparisonError(
                f"{manifest_path}: {benchmark} does not use the scale-fixed metadata total"
            )
        validate_showcase_participants(
            manifest_path, benchmark, payload, spec, expected_hosts, launcher
        )
        measurements.append(
            Measurement(
                family=spec.family,
                name=spec.name,
                value=positive_finite(
                    payload.get(spec.metric),
                    f"{manifest_path}: {benchmark} {spec.metric}",
                ),
                unit="seconds",
                fingerprint=fingerprint(
                    payload,
                    spec.fingerprint_fields,
                    f"{manifest_path}: {benchmark}",
                ),
            )
        )
    clone = job_map_payloads.get("wos_git_clone")
    checkout = job_map_payloads.get("wos_git_checkout")
    if clone is not None and checkout is not None:
        validate_git_phase_coherence(manifest_path, clone, checkout)
    runtime_provenance = {
        json.dumps(payload.get("runtime_provenance"), sort_keys=True)
        for payload in job_map_payloads.values()
    }
    if len(runtime_provenance) > 1:
        raise ComparisonError(
            f"{manifest_path}: fixed-resource phases used different node-local runtimes"
        )
    return measurements


def load_run(path: Path, required_workloads: set[str]) -> Run:
    manifest_path, manifest = load_manifest(path)
    run_id = validate_run_id(manifest_path, manifest)
    revision, runner_sha256 = validate_source(manifest_path, manifest)
    node_count, runtime_validated = validate_topology(manifest_path, manifest)
    hosts, launcher = expected_wos_hosts(manifest_path, manifest, node_count)
    expected_vcpus = {
        hostname: vcpus for hostname, vcpus, _ in EXPECTED_TOPOLOGY_LAYOUTS[node_count]
    }
    if "mandelbrot" in required_workloads:
        validate_mandel_workers(manifest_path, manifest)
    steps = result_steps(manifest_path, manifest)

    measurements: dict[str, Measurement] = {}
    extracted: list[Measurement] = []
    if "mandelbrot" in required_workloads:
        mandel = extract_mandel(manifest_path, steps, hosts)
        if mandel is not None:
            extracted.append(mandel)
    if "rendering" in required_workloads:
        extracted.extend(extract_render(manifest_path, steps, hosts, expected_vcpus))
    showcase_workloads = {entry.family for entry in SHOWCASE_METRICS.values()}
    if required_workloads & showcase_workloads:
        extracted.extend(extract_showcase(manifest_path, steps, hosts, launcher))
    for measurement in extracted:
        if measurement.name in measurements:
            raise ComparisonError(
                f"{manifest_path}: duplicate measurement {measurement.name}"
            )
        measurements[measurement.name] = measurement
    return Run(
        manifest_path,
        run_id,
        node_count,
        runtime_validated,
        revision,
        runner_sha256,
        measurements,
    )


def compare_runs(
    manifest_paths: list[Path],
    *,
    required_nodes: tuple[int, ...] = DEFAULT_NODES,
    required_workloads: tuple[str, ...] = ALL_WORKLOADS,
    max_ratio: float = DEFAULT_MAX_RATIO,
    min_repeats: int = DEFAULT_MIN_REPEATS,
) -> dict[str, Any]:
    if not manifest_paths:
        raise ComparisonError("at least one manifest is required")
    resolved_manifest_paths = [path.resolve() for path in manifest_paths]
    if len(set(resolved_manifest_paths)) != len(resolved_manifest_paths):
        raise ComparisonError("duplicate manifest paths are not independent repeats")
    if (
        1 not in required_nodes
        or len(set(required_nodes)) != len(required_nodes)
        or any(node < 1 or node > 4 for node in required_nodes)
    ):
        raise ComparisonError(
            "required nodes must be unique and include the one-node baseline"
        )
    if not required_workloads or len(set(required_workloads)) != len(
        required_workloads
    ):
        raise ComparisonError("required workloads must be unique")
    unknown_workloads = set(required_workloads) - set(ALL_WORKLOADS)
    if unknown_workloads:
        raise ComparisonError(
            f"unknown required workload(s): {', '.join(sorted(unknown_workloads))}"
        )
    if not math.isfinite(max_ratio) or max_ratio <= 0:
        raise ComparisonError("max ratio must be a positive finite number")
    if min_repeats <= 0:
        raise ComparisonError("minimum repeats must be positive")

    required_workload_set = set(required_workloads)
    runs = [load_run(path, required_workload_set) for path in resolved_manifest_paths]
    if len({run.run_id for run in runs}) != len(runs):
        raise ComparisonError(
            "manifest timestamps are not distinct independent benchmark runs"
        )
    revisions = {run.revision for run in runs}
    runner_hashes = {run.runner_sha256 for run in runs}
    if len(revisions) != 1:
        raise ComparisonError("manifests use different source revisions")
    if len(runner_hashes) != 1:
        raise ComparisonError("manifests use different benchmark runner hashes")

    runs_by_node = {
        node: [run for run in runs if run.node_count == node] for node in required_nodes
    }
    unexpected_nodes = sorted({run.node_count for run in runs} - set(required_nodes))
    if unexpected_nodes:
        raise ComparisonError(
            f"manifest node count(s) are outside --nodes: {unexpected_nodes}"
        )
    missing: list[str] = []
    for node, node_runs in runs_by_node.items():
        if not node_runs:
            missing.append(f"missing topology: {node} node(s)")
        elif len(node_runs) < min_repeats:
            missing.append(
                f"{node} node(s): need {min_repeats} repeats, found {len(node_runs)}"
            )

    baseline_runs = runs_by_node[1]
    expected_names = set().union(
        *(REQUIRED_MEASUREMENTS_BY_FAMILY[family] for family in required_workloads)
    )
    if baseline_runs:
        for family in required_workloads:
            actual_family_names = {
                name
                for name, item in baseline_runs[0].measurements.items()
                if item.family == family
            }
            if not REQUIRED_MEASUREMENTS_BY_FAMILY[family].issubset(
                actual_family_names
            ):
                missing.append(f"one-node baseline missing workload: {family}")

    fingerprints: dict[str, str] = {}
    units: dict[str, str] = {}
    values: dict[int, dict[str, list[float]]] = {node: {} for node in required_nodes}
    for node, node_runs in runs_by_node.items():
        for run in node_runs:
            actual_names = {
                name
                for name, item in run.measurements.items()
                if item.family in required_workload_set
            }
            if baseline_runs and actual_names != expected_names:
                missing_names = sorted(expected_names - actual_names)
                extra_names = sorted(actual_names - expected_names)
                if missing_names:
                    missing.append(
                        f"{run.path}: missing measurement(s): {', '.join(missing_names)}"
                    )
                if extra_names:
                    missing.append(
                        f"{run.path}: unexpected measurement(s): {', '.join(extra_names)}"
                    )
            for name in expected_names & actual_names:
                item = run.measurements[name]
                prior_fingerprint = fingerprints.setdefault(name, item.fingerprint)
                if prior_fingerprint != item.fingerprint:
                    raise ComparisonError(
                        f"workload parameters differ for {name}: {run.path}"
                    )
                prior_unit = units.setdefault(name, item.unit)
                if prior_unit != item.unit:
                    raise ComparisonError(
                        f"measurement units differ for {name}: {run.path}"
                    )
                values[node].setdefault(name, []).append(item.value)

    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for name in sorted(expected_names):
        baseline_values = values[1].get(name, [])
        if len(baseline_values) < min_repeats:
            continue
        baseline_median = statistics.median(baseline_values)
        for node in required_nodes:
            node_values = values[node].get(name, [])
            if len(node_values) < min_repeats:
                continue
            candidate_median = statistics.median(node_values)
            ratio = candidate_median / baseline_median
            passed = ratio <= max_ratio
            if not passed:
                failures.append(
                    f"{name} at {node} nodes: ratio {ratio:.6f} > {max_ratio:.6f}"
                )
            rows.append(
                {
                    "workload": name,
                    "node_count": node,
                    "samples": len(node_values),
                    "median": candidate_median,
                    "baseline_median": baseline_median,
                    "ratio": ratio,
                    "unit": units[name],
                    "pass": passed,
                }
            )

    runtime_validated_all = bool(runs) and all(run.runtime_validated for run in runs)
    provisional_pass = not missing and not failures
    if not runtime_validated_all:
        missing.append(
            "acceptance requires structured runtime CPU/memory observations for every manifest"
        )
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_revision": next(iter(revisions)),
        "runner_sha256": next(iter(runner_hashes)),
        "required_nodes": list(required_nodes),
        "required_workloads": list(required_workloads),
        "max_candidate_to_baseline_ratio": max_ratio,
        "minimum_repeats": min_repeats,
        "configured_resources": {
            "total_vcpus": EXPECTED_TOTAL_VCPUS,
            "total_memory_mib": EXPECTED_TOTAL_MEMORY_MIB,
        },
        "runtime_validated": runtime_validated_all,
        "resource_evidence": (
            "configured+runtime-observed" if runtime_validated_all else "config-only"
        ),
        "provisional_performance_pass": provisional_pass,
        "rows": rows,
        "missing": sorted(set(missing)),
        "failures": failures,
        "pass": not missing and not failures and runtime_validated_all,
    }


def parse_csv_ints(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected comma-separated positive integers"
        ) from exc
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("expected comma-separated positive integers")
    return parsed


def parse_workloads(value: str) -> tuple[str, ...]:
    parsed = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = set(parsed) - set(ALL_WORKLOADS)
    if not parsed or unknown:
        suffix = f"; unknown: {', '.join(sorted(unknown))}" if unknown else ""
        raise argparse.ArgumentTypeError(
            f"expected known comma-separated workloads{suffix}"
        )
    return parsed


def argparse_positive_float(value: str) -> float:
    try:
        return positive_finite(float(value), "ratio")
    except ComparisonError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def print_text(result: dict[str, Any]) -> None:
    print(f"resource_evidence\t{result['resource_evidence']}")
    print(
        f"provisional_performance_pass\t{str(result['provisional_performance_pass']).lower()}"
    )
    print(f"max_ratio\t{result['max_candidate_to_baseline_ratio']:.6f}")
    print("workload\tnodes\tsamples\tmedian\tbaseline\tratio\tstatus")
    for row in result["rows"]:
        status = "PASS" if row["pass"] else "FAIL"
        print(
            f"{row['workload']}\t{row['node_count']}\t{row['samples']}\t{row['median']:.9g}\t"
            f"{row['baseline_median']:.9g}\t{row['ratio']:.6f}\t{status}"
        )
    for message in result["missing"]:
        print(f"MISSING\t{message}", file=sys.stderr)
    for message in result["failures"]:
        print(f"FAIL\t{message}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        action="append",
        required=True,
        type=Path,
        help="repeat for every suite manifest",
    )
    parser.add_argument(
        "--nodes",
        type=parse_csv_ints,
        default=DEFAULT_NODES,
        help="required node counts; default 1,2,3,4",
    )
    parser.add_argument(
        "--required-workloads",
        type=parse_workloads,
        default=ALL_WORKLOADS,
        help="comma-separated acceptance workloads; default is the complete user-goal set",
    )
    parser.add_argument(
        "--max-ratio", type=argparse_positive_float, default=DEFAULT_MAX_RATIO
    )
    parser.add_argument("--min-repeats", type=positive_int, default=DEFAULT_MIN_REPEATS)
    parser.add_argument("--json-output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = compare_runs(
            args.manifest,
            required_nodes=args.nodes,
            required_workloads=args.required_workloads,
            max_ratio=args.max_ratio,
            min_repeats=args.min_repeats,
        )
    except ComparisonError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print_text(result)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
