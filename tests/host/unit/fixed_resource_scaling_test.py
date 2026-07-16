#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import copy
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


def measurement_for(
    payload: dict[str, object], benchmark: str
) -> dict[str, object]:
    measurements = payload.get("measurements")
    if not isinstance(measurements, list):
        fail("showcase fixture has no measurement list")
    matches = [
        measurement
        for measurement in measurements
        if isinstance(measurement, dict) and measurement.get("benchmark") == benchmark
    ]
    if len(matches) != 1:
        fail(
            f"showcase fixture has {len(matches)} records for benchmark {benchmark!r}"
        )
    return matches[0]


def render_result(elapsed: float, node_count: int, step_name: str) -> dict[str, object]:
    scene, scene_path, placement, width, height, spp = RENDER_CASES[step_name]
    layout = TOPOLOGY_LAYOUTS[node_count]
    host_records: list[dict[str, object]] = []
    for hostname, vcpus, _ in layout:
        if placement == "node-threads":
            configured_slots = 1
            runs = configured_slots
            effective_threads = vcpus
        else:
            configured_slots = vcpus
            runs = configured_slots
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
            "process_persistent_workers": True,
            "persistent_batch_size": 8,
            "fine_grained_process_tail_enabled": placement == "process-per-core"
            and node_count > 1,
            "fine_grained_tail_batches": worker_slots
            if placement == "process-per-core" and node_count > 1
            else 0,
            "fine_grained_tail_tiles": worker_slots
            if placement == "process-per-core" and node_count > 1
            else 0,
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
            "spawner_host": "wos-0.wos",
            "remote_pid": 0 if index == 0 else 100 + index,
            "transport": "local" if index == 0 else "wki",
            "work_units": base + (1 if index < extra else 0),
        }
        for index in range(node_count)
    ]


JOB_MAP_RUNTIME_PATHS = [
    "/root/wos-showcase",
    "/usr",
    "/bin",
    "/lib",
    "/lib64",
    "/libexec",
    "/share",
    "/tmp",
]
FIXED_RESOURCE_HELPER_SHA256 = hashlib.sha256(
    (
        ROOT
        / "configs"
        / "rootfs"
        / "root"
        / "wos-showcase"
        / "fixed_resource_workloads.py"
    ).read_bytes()
).hexdigest()
JOB_MAP_RUNTIME_PROVENANCE = {
    "helper_sha256": FIXED_RESOURCE_HELPER_SHA256,
    "python_sha256": "8" * 64,
    "python_hashlib_runtime_sha256": "b" * 64,
    "python_json_runtime_sha256": "c" * 64,
    "git_sha256": "9" * 64,
    "git_upload_pack_sha256": "a" * 64,
    "wkictl_sha256": "d" * 64,
}
FILE_MOVE_SOURCE_DIGESTS = {
    "quick": "e398a6f80b342307308bc77329f62acc97a545a5afe14d80c993c0efa4c51de8",
    "full": "53df6f06f71f8333e3d0e600d829ec72b41496a0fd9b3867a3f393e1ffa19d16",
    "stress": "d2c6271705a2c566312efbe0add1d25200dbdf154f05db15cd2d44ac1b87d954",
}
FILE_MOVE_ARTIFACT_DIGESTS = {
    "quick": "a94df57463998313e3f775ca221f82a369bf53dc2e9f5475cd730e967c5852bc",
    "full": "301cf0142cb1ed3d74c0d9b919423bc48fc07a8a78932f5dba3b3bd7a95c4ca6",
    "stress": "b61d8aa171a243272b8f00769dce5bb1712e382b0404e187de7a6a18eaa9b6f9",
}
JOB_MAP_QUICK_DIGESTS = {
    "wos_python_sha256": [
        "468bc27e4ee67c2ebf24615181a7e6a0ca41a406f12d467249e34d28fbaee57b",
        "ccc19ded6f5ff3cc2c882865e9f354d29ccea271b741ae675595b17a9216628e",
        "d3f6b7cbcc1b5c1aad9a43677c7d266816fc84c2479ea34c6d28f1ba95fc981e",
        "e87adc6b282f8f6a65b7fc1d6acfe586ae51a2fa759c3569739b6095e3f768c9",
        "00c50a91977a752615deb2728be72b7cf71864d55a2d9ad7ee12d8df4c3eed91",
        "dbfd0c1c3149a93224f113ea4f6733cc0700ff5f161aea1014af9aafb13be9c3",
        "b655fedcd0f6aa173c3c08a7794f1d9881bc7dc08faf0d27d9facd7c768196f1",
        "8458443e2a49202ce68e058a1de45eb35ae34cd7c89d8a4bebd9a46c134c9c81",
        "a76ecd11c936be91a88ca1d8aa42c145e817faa0001a5c793d7e10fb1a26e128",
        "2b39ca819552539c95dc88d58a5bbbf03790318e707213525efd64e0e00413f0",
        "bf9ac176c24ea954ed97411b7f65ae390cd3198d23bad5c96cdb51665c4bca81",
        "f4eddef70745691ad3fd4e415514d8b00c72397368f5690f3e290da89c74c717",
        "feec768afd27e0dcaf7d7275bd38e6b91031cd735940f54e6ccb744e3717337a",
        "9db1b7532379f61aacea948f4bec850d8b828bec1b87bcbc7e3d3bd2ed3ced36",
        "613a30d6d1953734059a09f20b4f7be1d7b533b82cdf3e0070efda82a4b7e87d",
        "37247511d24f2762050cda17811a78c9d73231e9cc3648022cf3fa962e9db766",
        "43bb23df40451f376afba703554c34b034a7922ca2f3bb6f002003bd20ad32b3",
        "aa9cbbd8ecb493768a9ca5582728925f09147297e074abad6bbeaafa7069c9e9",
        "84af72f35365ad3b1cface43002e3df5eaf324481d3910388c7845eae6e9fea8",
        "2e4ec263604b7700a74ffa774e86e8e515e7d0e2bfa3ecd7e2e8f23030a00483",
        "e804be602686bfcc14ea856f54fc7b603d94df3b499b4c0c89f44f873519a9b3",
        "ce37d29b3faf34b815c387c5109403c77b5711f826b4832309c955cfaed60c2e",
        "c7bcb5a2d3120cbc4aba271476e23b2c0cb70e723577f181d54f33164f3af6da",
        "6ad6a7527bb59af8e47935c64f947536a0ab780dd7b478d65f179a39297d8ba8",
        "eb43afaba54b02c44b5179de8e1c49355df071b516b3e263d30ac55144bc0f91",
        "db218618ea22da627b612fe34055f2817750265620b7d1315e15b7dd3d2db9d5",
        "39a987373e533e6f9d35e3c26ed71b414d7a3fc9718ea2b1850c3825fec1b112",
        "367d287f01d1538609859c5d8c13fef60c2c540d9798bee3f11e197c5733a1d3",
        "92752af5ec79247b2d4ae6ecf4e81122fb2661eaefe2b383fd99bc88a491ca89",
        "87e56850695e6574a8639a27a40f745afa5712dba25e8567943f6096e4e6b3b7",
        "f1550787b3ca9c6a2acaa9507e1805f11cd17d38283e62e6b5a2bf825a9f1e88",
        "c7e1b6b48087bfdd7528a0a81d6daec4e6547e1f4859c23e69a670a1e8f733e2",
    ],
    "wos_python_json": [
        "4675c457b9523915ce6a91f1dc0067dd57984894c0002e0638644f1be7db3d5d",
        "f4ae413b470b636e77d6312d7be509f4e642811fb465ba5f9921fc1c732735b3",
        "09142eb8832f63e2c86953a66dd47b3ef425d5e32c25e6595493f32e646e612f",
        "d2ccc620a1efa5674b9c7d13314403b55c78e68881082f99d5228e5e90c23ef8",
        "e4c96710c7a739ab163a6f245af57e8279b05f589facb234809ac032c5f534aa",
        "9e9f9db65718154a52a9575a005efe0add5d718f58e3c568c31075ab0eea7726",
        "8fad2a2182beb46f98cceda1d7b73f3dea205659a70b9f165507de33ef68a7bc",
        "15acaf799a5e28e42b01f8bfd02ffab0a850fb892c73c1d8c7436879d292252b",
        "dd6f001fb82f1028a16dfc72ba5a12445f0a8b87a2acc47134de974e4829aa1a",
        "e1e95ef75bf6f0098e7a3c9de11718cfac29be8dc76c0e8ffa6f9ac84854890d",
        "495abdd0ac7ae82b5f86de09b9e46c39c2b2c0015c48f61004d8d46e51fb263c",
        "9d48dd348c04e90ad40ba38cff7b8d1b4bdba7584fd80549265bf39ec58073cc",
        "4a86c02219319fb632aae5226d10fbc3cd231ebf0302608bbfdd51662ddab5a3",
        "9a972dcdbb8ac619ace2699026eea4977b2e89b32aafbdee6941a609e909d0bb",
        "23dbe8a5b81ac42c62d60bb6483308dd633eba4eac07166b17d0d7ad39966141",
        "dbaddd29b14058bac5409b510fdab87252f064024d3145200804bb1412d0d2a5",
        "aed4b8aee0be3cb8d6ba89963d74faed42226bddc576f1719c303c2e7084c311",
        "549315c1505bf4c95d2292304b5250add8d40384ea48462342c300882896e94c",
        "63896e2aa58d81aff30791c8a2ec9bff578bbdedeea20c278196a6fa1751b4c1",
        "48450000b436f2b55f2f6f407654ab219588e9ff291c28f785da22130a6c6ac4",
        "9106909a4d2a113abbfe1a723932138a24b13dcc12833a1412200755c1ce5b2d",
        "1cd2c3c9e06a4a9dfae6d49254639d964e2454a0e3a67f2a5e296293fe832b47",
        "c04fa9d9cadf5df91d419731a922fb98674ee851c47494ce63c71b756c244a40",
        "5883cad3a051833596d012667b365369a5731eea124fc4ff3d200f2482ccd490",
        "ecbd0f66726843701b69d38d0e3a01e3e1799149433554f3a0efebd1afffd767",
        "9b613be77d16aa70d809108d3051f7d3b36f9d5ac251a5bde652ece28b9ef0ee",
        "2654e3fd62fd6321ad484f904db2d2a9c645972507b8809214ce0797c9e2fc3f",
        "b465de6ee05e8d2a4215d66997edc9b2ec07fec31ed3a934688e826392f3a827",
        "eab3d360435182cfc3f80cb4f3c7d5efe5085395ae7581fcc4b1f9843f037ebf",
        "4def86ec58ce049cb7b7bdf8047dc86551bd4e22c4ca3135e1ea8530d872df99",
        "8ed293bd752d2cb07d2fb7f3fb8a7d9f7f6db48db97be985cb9cff34236e253b",
        "cb96f87f06da25d19d8722627d707e1ec8a0b352c2e3ef1c5052ff52f6aa365d",
    ],
}
JOB_MAP_QUICK_DIGESTS["wos_file_move"] = [
    FILE_MOVE_SOURCE_DIGESTS["quick"]
] * 32


def job_map_participants(
    node_count: int,
    total_work_units: int,
    route_path: str | None,
    payload: dict[str, object],
) -> list[dict[str, object]]:
    benchmark = str(payload["benchmark"])
    base, extra = divmod(total_work_units, node_count)
    result: list[dict[str, object]] = []
    first_job = 0
    for index in range(node_count):
        work_units = base + (1 if index < extra else 0)
        job_ids = list(range(first_job, first_job + work_units))
        if benchmark == "wos_git_clone":
            job_digest = hashlib.sha256(
                f"{payload['commit']}\0{payload['tree_oid']}".encode("ascii")
            ).hexdigest()
            job_digests = [job_digest] * work_units
        elif benchmark == "wos_git_checkout":
            job_digests = [str(payload["fixture_digest"])] * work_units
        else:
            job_digests = [
                JOB_MAP_QUICK_DIGESTS[benchmark][job_id] for job_id in job_ids
            ]
        participant_digest = hashlib.sha256()
        for job_id, job_digest in zip(job_ids, job_digests, strict=True):
            participant_digest.update(job_id.to_bytes(4, "big"))
            participant_digest.update(bytes.fromhex(job_digest))
        remote = index != 0
        participant = {
            "host": f"wos-{index}.wos",
            "runner_host": f"wos-{index}.wos",
            "launcher_host": "wos-0.wos",
            "remote_pid": 100 + index if remote else 0,
            "job_remote_pids": (
                [1000 + job_id for job_id in job_ids]
                if remote
                else [0] * work_units
            ),
            "strict_target": True,
            "transport": "wki" if remote else "local",
            "work_units": work_units,
            "completed_work_units": work_units,
            "job_ids": job_ids,
            "job_digests": job_digests,
            "runtime_route": "local",
            "runtime_paths": JOB_MAP_RUNTIME_PATHS,
            "workspace_route": "host" if route_path is not None else None,
            "workspace_path": route_path,
            "digest": participant_digest.hexdigest(),
        }
        if benchmark == "wos_file_move":
            participant["bytes_moved"] = work_units * 2 * 1024 * 1024
        result.append(participant)
        first_job += work_units
    return result


def showcase_measurement(
    payload: dict[str, object],
    node_count: int,
    total_work_units: int,
    wki_route: str,
    *,
    job_map: bool = False,
) -> dict[str, object]:
    route_path = (
        "/tmp/wos-showcase-fixed-0123456789abcdef"
        if wki_route == "host-workspace"
        else None
    )
    benchmark = str(payload["benchmark"])
    result = {
        **payload,
        "placement": "local-baseline" if node_count == 1 else "strict-on",
        "wki_route": wki_route,
        "launcher_host": "wos-0.wos",
        "total_work_units": total_work_units,
        "participants": (
            job_map_participants(node_count, total_work_units, route_path, payload)
            if job_map
            else participants(node_count, total_work_units)
        ),
    }
    if job_map:
        aggregate_digest = hashlib.sha256()
        for participant in result["participants"]:
            for job_id, job_digest in zip(
                participant["job_ids"], participant["job_digests"], strict=True
            ):
                aggregate_digest.update(job_id.to_bytes(4, "big"))
                aggregate_digest.update(bytes.fromhex(job_digest))
        aggregate = aggregate_digest.hexdigest()
        result.update(
            {
                "evidence_contract": "wos-showcase-job-map-v1",
                "route_path": route_path,
                "runtime_provenance": JOB_MAP_RUNTIME_PROVENANCE,
                "scale": "quick",
            }
        )
        if benchmark in ("wos_file_move", "wos_git_clone", "wos_git_checkout"):
            result["artifact_digest"] = aggregate
        else:
            result["digest"] = aggregate
    if benchmark in ("wos_vfsbench_create", "wos_vfsbench_rename"):
        total_workers = min(32, total_work_units)
        worker_base, worker_extra = divmod(total_workers, node_count)
        result["total_workers"] = total_workers
        for index, participant in enumerate(result["participants"]):
            participant["workers"] = worker_base + (
                1 if index < worker_extra else 0
            )
    return result


def rebind_job_map_digests(
    payload: dict[str, object], job_digests: list[str], aggregate_field: str
) -> None:
    aggregate = hashlib.sha256()
    for participant in payload["participants"]:
        participant_digest = hashlib.sha256()
        first_job = participant["job_ids"][0]
        count = participant["work_units"]
        selected = job_digests[first_job : first_job + count]
        participant["job_digests"] = selected
        for job_id, job_digest in zip(participant["job_ids"], selected, strict=True):
            encoded_job = job_id.to_bytes(4, "big")
            encoded_digest = bytes.fromhex(job_digest)
            participant_digest.update(encoded_job)
            participant_digest.update(encoded_digest)
            aggregate.update(encoded_job)
            aggregate.update(encoded_digest)
        participant["digest"] = participant_digest.hexdigest()
    payload[aggregate_field] = aggregate.hexdigest()


def showcase_measurements(scale: float, node_count: int) -> list[dict[str, object]]:
    return [
        showcase_measurement(
            {
                "benchmark": "wos_distributed_compile",
                "workload_id": "wos-live-cpp-32-tu-v1",
                "source_sha256": "aa52bc6a7f7f5b58904b6c1d06fb7f813c8567c97470fbe4161a4e691a60c726",
                "compiler_path": "/usr/bin/clang++",
                "compiler_version_sha256": "7f5ddecc0cfc8134433d3e5e6df0d6b264fd4e11e95e820c745400907363fa43",
                "compiler_sha256": "6" * 64,
                "wkictl_sha256": "5" * 64,
                "compile_flags": "-std=c++23 -O2 -fno-ident",
                "link_flags": "-std=c++23 -O2 -Wl,--build-id=none",
                "cache_policy": "prewarmed-compiler-source-headers-all-hosts",
                "launch_policy": "one-controller-per-host-local-tu-workers",
                "controller_count": node_count,
                "runtime_route": "local",
                "runtime_paths": JOB_MAP_RUNTIME_PATHS,
                "workspace_route": "host",
                "workspace_path": "/tmp/wos-showcase-fixed-0123456789abcdef/distributed-compile",
                "units": 32,
                "total_workers": 32,
                "artifact_digest": "a" * 64,
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
                "iterations": 32,
                "elapsed_seconds": 3.0 * scale,
            },
            node_count,
            32,
            "host-path",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_vfsbench_rename",
                "path": "/tmp/wos-showcase-vfsbench",
                "iterations": 32,
                "elapsed_seconds": 2.5 * scale,
            },
            node_count,
            32,
            "host-path",
        ),
        showcase_measurement(
            {
                "benchmark": "wos_file_move",
                "workload_id": "wos-showcase-file-move-v1",
                "operation": "stream-copy-read-write-close",
                "source_relative_path": "file-move/source.bin",
                "destination_relative_path": "file-move/destinations",
                "files": 32,
                "bytes_per_file": 2 * 1024 * 1024,
                "total_bytes": 32 * 2 * 1024 * 1024,
                "chunk_bytes": 2 * 1024 * 1024,
                "cache_policy": "warm-shared-source-cold-host-destinations",
                "source_digest": FILE_MOVE_SOURCE_DIGESTS["quick"],
                "artifact_digest": FILE_MOVE_ARTIFACT_DIGESTS["quick"],
                "elapsed_seconds": 3.5 * scale,
            },
            node_count,
            32,
            "host-workspace",
            job_map=True,
        ),
        showcase_measurement(
            {
                "benchmark": "wos_git_clone",
                "repository": "wos-showcase-git-fixture-v1",
                "repository_uri": "file:///tmp/wos-showcase-fixed-0123456789abcdef/git-fixture.git",
                "commit": "01cc47f97d58c90edec1b043d7288563d82a923d",
                "tree_oid": "104396b28c8cbbe7f97e3d6a13704881d700f421",
                "checkout_files": 100,
                "fixture_bytes": 1638400,
                "fixture_digest": "20c827ddcaa537afd3379325d782e732190950eaec535af107f4c1580949e1b9",
                "artifact_digest": "4" * 64,
                "cache_policy": "warm-source-cold-destination",
                "elapsed_seconds": 3.0 * scale,
            },
            node_count,
            32,
            "host-workspace",
            job_map=True,
        ),
        showcase_measurement(
            {
                "benchmark": "wos_git_checkout",
                "commit": "01cc47f97d58c90edec1b043d7288563d82a923d",
                "tree_oid": "104396b28c8cbbe7f97e3d6a13704881d700f421",
                "checkout_files": 100,
                "fixture_bytes": 1638400,
                "fixture_digest": "20c827ddcaa537afd3379325d782e732190950eaec535af107f4c1580949e1b9",
                "artifact_digest": "5" * 64,
                "cache_policy": "post-clone-object-cache",
                "elapsed_seconds": 2.0 * scale,
            },
            node_count,
            32,
            "host-workspace",
            job_map=True,
        ),
        showcase_measurement(
            {
                "benchmark": "wos_python_sha256",
                "jobs": 32,
                "rounds": 20_000,
                "digest": "6" * 64,
                "elapsed_seconds": 4.0 * scale,
            },
            node_count,
            32,
            "local-runtime",
            job_map=True,
        ),
        showcase_measurement(
            {
                "benchmark": "wos_python_json",
                "documents": 32,
                "rounds": 20_000,
                "digest": "7" * 64,
                "elapsed_seconds": 5.0 * scale,
            },
            node_count,
            32,
            "local-runtime",
            job_map=True,
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
                "scale": "quick",
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
        "one-node baseline missing workload: file-movement",
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

        persistent_runs = complete_matrix(root / "persistent-runs")
        persistent_candidate = result_path_for(
            persistent_runs[-1], "wos-render-duck-node-threads"
        )
        persistent_profile = read_json(persistent_candidate)["ipc_profile"]
        assert_equal(
            persistent_profile["completed_runs"],
            persistent_profile["worker_slots"],
            "persistent render process cardinality",
        )
        persistent_result = comparator.compare_runs(
            persistent_runs, required_workloads=("rendering",)
        )
        assert_equal(persistent_result["pass"], True, "persistent render comparison")

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

        repeated_runs = complete_matrix(root / "repeated-runs")
        candidate = result_path_for(
            repeated_runs[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["completed_runs"] += 1
        payload["ipc_profile"]["hosts"][-1]["runs"] += 1
        write_json(candidate, payload)
        expect_error(
            comparator,
            repeated_runs,
            "incomplete worker slots",
            required_workloads=("rendering",),
        )

        nonpersistent = complete_matrix(root / "nonpersistent")
        candidate = result_path_for(
            nonpersistent[-1], "wos-render-duck-node-threads"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["process_persistent_workers"] = False
        write_json(candidate, payload)
        expect_error(
            comparator,
            nonpersistent,
            "IPC profile disagrees with the fixed workload",
            required_workloads=("rendering",),
        )

        zero_batch = complete_matrix(root / "zero-persistent-batch")
        candidate = result_path_for(
            zero_batch[-1], "wos-render-duck-node-threads"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["persistent_batch_size"] = 0
        write_json(candidate, payload)
        expect_error(
            comparator,
            zero_batch,
            "IPC profile disagrees with the fixed workload",
            required_workloads=("rendering",),
        )

        wrong_tail = complete_matrix(root / "wrong-fine-grained-tail")
        candidate = result_path_for(
            wrong_tail[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["fine_grained_process_tail_enabled"] = False
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_tail,
            "IPC profile disagrees with the fixed workload",
            required_workloads=("rendering",),
        )

        wrong_tail_enable = complete_matrix(root / "wrong-fine-grained-tail-enable")
        candidate = result_path_for(
            wrong_tail_enable[0], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["fine_grained_process_tail_enabled"] = True
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_tail_enable,
            "IPC profile disagrees with the fixed workload",
            required_workloads=("rendering",),
        )

        wrong_tail_count = complete_matrix(root / "wrong-fine-grained-tail-count")
        candidate = result_path_for(
            wrong_tail_count[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["fine_grained_tail_batches"] -= 1
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_tail_count,
            "IPC profile disagrees with the fixed workload",
            required_workloads=("rendering",),
        )

        wrong_host_runs = complete_matrix(root / "wrong-host-runs")
        candidate = result_path_for(
            wrong_host_runs[-1], "wos-render-duck-process-per-core"
        )
        payload = read_json(candidate)
        payload["ipc_profile"]["hosts"][0]["runs"] -= 1
        payload["ipc_profile"]["hosts"][-1]["runs"] += 1
        write_json(candidate, payload)
        expect_error(
            comparator,
            wrong_host_runs,
            "exactly one persistent process per configured slot",
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
        measurement_for(payload, "wos_distributed_compile")[
            "placement"
        ] = "local-baseline"
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
        measurement_for(payload, "wos_distributed_compile")["participants"][-1][
            "runner_host"
        ] = "wos-0.wos"
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
        measurement_for(payload, "wos_distributed_compile")["participants"].pop()
        write_json(candidate, payload)
        expect_error(
            comparator,
            incomplete_coverage,
            "participants do not match the topology",
            required_workloads=("distributed-compilation",),
        )

        changed_compile_source = complete_matrix(root / "changed-compile-source")
        candidate = result_path_for(changed_compile_source[0], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")["source_sha256"] = (
            "0" * 64
        )
        write_json(candidate, payload)
        expect_error(
            comparator,
            changed_compile_source,
            "invalid fixed compile evidence",
            required_workloads=("distributed-compilation",),
        )

        bad_compile_cache_policy = complete_matrix(root / "bad-compile-cache-policy")
        candidate = result_path_for(bad_compile_cache_policy[-1], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")[
            "cache_policy"
        ] = "launcher-only-warm-cache"
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_compile_cache_policy,
            "invalid fixed compile evidence",
            required_workloads=("distributed-compilation",),
        )

        bad_compile_launch_policy = complete_matrix(root / "bad-compile-launch-policy")
        candidate = result_path_for(bad_compile_launch_policy[-1], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")[
            "launch_policy"
        ] = "one-wki-launch-per-tu"
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_compile_launch_policy,
            "invalid fixed compile evidence",
            required_workloads=("distributed-compilation",),
        )

        bad_compile_controller_count = complete_matrix(root / "bad-compile-controller-count")
        candidate = result_path_for(bad_compile_controller_count[-1], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")["controller_count"] = 32
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_compile_controller_count,
            "invalid fixed compile evidence",
            required_workloads=("distributed-compilation",),
        )

        changed_compile_binary = complete_matrix(root / "changed-compile-binary")
        candidate = result_path_for(changed_compile_binary[0], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")["compiler_sha256"] = (
            "7" * 64
        )
        write_json(candidate, payload)
        expect_error(
            comparator,
            changed_compile_binary,
            "workload parameters differ for distributed-compilation",
            required_workloads=("distributed-compilation",),
        )

        changed_wkictl_binary = complete_matrix(root / "changed-wkictl-binary")
        candidate = result_path_for(changed_wkictl_binary[0], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")["wkictl_sha256"] = (
            "4" * 64
        )
        write_json(candidate, payload)
        expect_error(
            comparator,
            changed_wkictl_binary,
            "workload parameters differ for distributed-compilation",
            required_workloads=("distributed-compilation",),
        )

        bad_compile_routes = complete_matrix(root / "bad-compile-routes")
        candidate = result_path_for(bad_compile_routes[-1], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")["runtime_route"] = "host"
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_compile_routes,
            "invalid compile route evidence",
            required_workloads=("distributed-compilation",),
        )

        bad_compile_workspace = complete_matrix(root / "bad-compile-workspace")
        candidate = result_path_for(bad_compile_workspace[-1], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_distributed_compile")[
            "workspace_path"
        ] = "/tmp/wos-showcase-fixed-0123456789abcdef/distributed-compile-extra"
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_compile_workspace,
            "invalid compile route evidence",
            required_workloads=("distributed-compilation",),
        )

        bad_metadata_total_workers = complete_matrix(
            root / "bad-metadata-total-workers"
        )
        candidate = result_path_for(
            bad_metadata_total_workers[-1], "wos-showcase"
        )
        payload = read_json(candidate)
        measurement_for(payload, "wos_vfsbench_create")["total_workers"] = 31
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_metadata_total_workers,
            "invalid fixed metadata worker evidence",
            required_workloads=("file-create",),
        )

        bad_metadata_worker_sum = complete_matrix(root / "bad-metadata-worker-sum")
        candidate = result_path_for(bad_metadata_worker_sum[-1], "wos-showcase")
        payload = read_json(candidate)
        rename = measurement_for(payload, "wos_vfsbench_rename")
        for participant, workers in zip(
            rename["participants"], (7, 8, 8, 8), strict=True
        ):
            participant["workers"] = workers
        write_json(candidate, payload)
        expect_error(
            comparator,
            bad_metadata_worker_sum,
            "canonical fixed worker partition",
            required_workloads=("file-rename",),
        )

        excessive_metadata_worker = complete_matrix(
            root / "excessive-metadata-worker"
        )
        candidate = result_path_for(excessive_metadata_worker[-1], "wos-showcase")
        payload = read_json(candidate)
        create = measurement_for(payload, "wos_vfsbench_create")
        for participant, workers in zip(
            create["participants"], (9, 7, 8, 8), strict=True
        ):
            participant["workers"] = workers
        write_json(candidate, payload)
        expect_error(
            comparator,
            excessive_metadata_worker,
            "invalid fixed metadata worker evidence",
            required_workloads=("file-create",),
        )

        for benchmark, workload in (
            ("wos_distributed_compile", "distributed-compilation"),
            ("wos_vfsbench_create", "file-create"),
        ):
            unbalanced = complete_matrix(root / f"unbalanced-{benchmark}")
            candidate = result_path_for(unbalanced[-1], "wos-showcase")
            payload = read_json(candidate)
            measurement = next(
                item
                for item in payload["measurements"]
                if item["benchmark"] == benchmark
            )
            for participant, work_units in zip(
                measurement["participants"], (29, 1, 1, 1), strict=True
            ):
                participant["work_units"] = work_units
            write_json(candidate, payload)
            expect_error(
                comparator,
                unbalanced,
                "canonical fixed partition",
                required_workloads=(workload,),
            )


def test_showcase_job_map_evidence_contract_is_strict(comparator) -> None:
    expected_hosts = {"wos-0", "wos-1", "wos-2"}
    launcher = "wos-0"
    manifest = Path("job-map-fixture.json")
    git_payload = next(
        measurement
        for measurement in showcase_measurements(1.0, 3)
        if measurement["benchmark"] == "wos_git_clone"
    )
    git_spec = comparator.SHOWCASE_METRICS["wos_git_clone"]

    def validate(payload: dict[str, object], benchmark: str, spec) -> None:
        comparator.validate_showcase_participants(
            manifest,
            benchmark,
            payload,
            spec,
            expected_hosts,
            launcher,
        )

    def reject(
        label: str,
        payload: dict[str, object],
        benchmark: str = "wos_git_clone",
        spec=git_spec,
    ) -> None:
        try:
            validate(payload, benchmark, spec)
        except comparator.ComparisonError:
            return
        fail(f"job-map validator accepted {label}")

    validate(git_payload, "wos_git_clone", git_spec)

    mutations = {
        "missing evidence contract": lambda payload: payload.pop("evidence_contract"),
        "incomplete work": lambda payload: payload["participants"][0].__setitem__(
            "completed_work_units", 10
        ),
        "duplicate job ID": lambda payload: payload["participants"][1][
            "job_ids"
        ].__setitem__(0, 0),
        "zero remote job PID": lambda payload: payload["participants"][1][
            "job_remote_pids"
        ].__setitem__(0, 0),
        "missing runtime route": lambda payload: payload["participants"][1][
            "runtime_paths"
        ].pop(),
        "wrong workspace path": lambda payload: payload["participants"][1].__setitem__(
            "workspace_path", "/tmp/not-the-measured-workspace"
        ),
        "non-strict target": lambda payload: payload["participants"][1].__setitem__(
            "strict_target", False
        ),
        "invalid participant digest": lambda payload: payload["participants"][
            1
        ].__setitem__("digest", "A" * 64),
        "changed per-job digest": lambda payload: payload["participants"][1][
            "job_digests"
        ].__setitem__(0, "0" * 64),
        "changed aggregate digest": lambda payload: payload.__setitem__(
            "artifact_digest", "0" * 64
        ),
        "missing top-level route path": lambda payload: payload.pop("route_path"),
        "noncanonical route path": lambda payload: payload.__setitem__(
            "route_path", "/usr"
        ),
        "missing repository URI": lambda payload: payload.pop("repository_uri"),
        "null participant launcher": lambda payload: payload["participants"][
            1
        ].__setitem__("launcher_host", None),
        "wrong helper provenance": lambda payload: payload[
            "runtime_provenance"
        ].__setitem__("helper_sha256", "0" * 64),
    }
    for label, mutate in mutations.items():
        candidate = copy.deepcopy(git_payload)
        mutate(candidate)
        reject(label, candidate)

    swapped = copy.deepcopy(git_payload)
    first = swapped["participants"][0]
    last = swapped["participants"][2]
    for field in (
        "work_units",
        "completed_work_units",
        "job_ids",
        "job_digests",
        "digest",
    ):
        first[field], last[field] = last[field], first[field]
    first["job_remote_pids"] = [0] * first["work_units"]
    last["job_remote_pids"] = list(range(2000, 2000 + last["work_units"]))
    reject("swapped canonical host ranges", swapped)

    for total in (4, 31, 33):
        reduced = showcase_measurement(
            copy.deepcopy(git_payload),
            3 if total != 4 else 4,
            total,
            "host-workspace",
            job_map=True,
        )
        reject(f"fixed total {total}", reduced)

    python_payload = next(
        measurement
        for measurement in showcase_measurements(1.0, 3)
        if measurement["benchmark"] == "wos_python_sha256"
    )
    for invalid_count in (31, 32.0):
        candidate = copy.deepcopy(python_payload)
        candidate["jobs"] = invalid_count
        reject(
            f"Python count {invalid_count!r}",
            candidate,
            "wos_python_sha256",
            comparator.SHOWCASE_METRICS["wos_python_sha256"],
        )

    invalid_rounds = copy.deepcopy(python_payload)
    invalid_rounds["rounds"] = 0
    reject(
        "Python rounds mismatch",
        invalid_rounds,
        "wos_python_sha256",
        comparator.SHOWCASE_METRICS["wos_python_sha256"],
    )

    substituted_git = copy.deepcopy(git_payload)
    substituted_git["commit"] = "a" * 40
    substituted_git["tree_oid"] = "b" * 40
    substituted_git["fixture_digest"] = "c" * 64
    substituted_job_digest = hashlib.sha256(
        f"{substituted_git['commit']}\0{substituted_git['tree_oid']}".encode("ascii")
    ).hexdigest()
    rebind_job_map_digests(
        substituted_git, [substituted_job_digest] * 32, "artifact_digest"
    )
    reject("coherent substituted Git fixture", substituted_git)

    substituted_python = copy.deepcopy(python_payload)
    rebind_job_map_digests(substituted_python, ["0" * 64] * 32, "digest")
    reject(
        "coherent substituted Python outputs",
        substituted_python,
        "wos_python_sha256",
        comparator.SHOWCASE_METRICS["wos_python_sha256"],
    )


def test_file_movement_is_required_fingerprinted_and_strict(comparator) -> None:
    expected_hosts = {"wos-0", "wos-1", "wos-2"}
    launcher = "wos-0"
    manifest = Path("file-movement-fixture.json")
    valid_payload = next(
        measurement
        for measurement in showcase_measurements(1.0, 3)
        if measurement["benchmark"] == "wos_file_move"
    )
    spec = comparator.SHOWCASE_METRICS["wos_file_move"]

    def validate(payload: dict[str, object]) -> None:
        comparator.validate_showcase_participants(
            manifest,
            "wos_file_move",
            payload,
            spec,
            expected_hosts,
            launcher,
        )

    def reject(label: str, mutate) -> None:
        candidate = copy.deepcopy(valid_payload)
        mutate(candidate)
        try:
            validate(candidate)
        except comparator.ComparisonError:
            return
        fail(f"file-movement validator accepted {label}")

    validate(valid_payload)
    for showcase_scale, bytes_per_file in (
        ("quick", 2 * 1024 * 1024),
        ("full", 8 * 1024 * 1024),
        ("stress", 32 * 1024 * 1024),
    ):
        scaled = copy.deepcopy(valid_payload)
        scaled["scale"] = showcase_scale
        scaled["bytes_per_file"] = bytes_per_file
        scaled["total_bytes"] = 32 * bytes_per_file
        scaled["source_digest"] = FILE_MOVE_SOURCE_DIGESTS[showcase_scale]
        for participant in scaled["participants"]:
            participant["bytes_moved"] = participant["work_units"] * bytes_per_file
        rebind_job_map_digests(
            scaled,
            [FILE_MOVE_SOURCE_DIGESTS[showcase_scale]] * 32,
            "artifact_digest",
        )
        assert_equal(
            scaled["artifact_digest"],
            FILE_MOVE_ARTIFACT_DIGESTS[showcase_scale],
            f"{showcase_scale} file-movement aggregate identity",
        )
        validate(scaled)

    mutations = {
        "wrong workload identity": lambda payload: payload.__setitem__(
            "workload_id", "substituted-file-move"
        ),
        "wrong operation": lambda payload: payload.__setitem__(
            "operation", "copy-file-range"
        ),
        "wrong source relative path": lambda payload: payload.__setitem__(
            "source_relative_path", "file-move/other.bin"
        ),
        "wrong destination relative path": lambda payload: payload.__setitem__(
            "destination_relative_path", "file-move/other-destinations"
        ),
        "reduced file count": lambda payload: payload.__setitem__("files", 31),
        "non-integral file count": lambda payload: payload.__setitem__(
            "files", 32.0
        ),
        "wrong bytes per file": lambda payload: payload.__setitem__(
            "bytes_per_file", 1024 * 1024
        ),
        "wrong total bytes": lambda payload: payload.__setitem__(
            "total_bytes", 1
        ),
        "boolean chunk size": lambda payload: payload.__setitem__(
            "chunk_bytes", True
        ),
        "wrong cache policy": lambda payload: payload.__setitem__(
            "cache_policy", "cold-source-cold-destinations"
        ),
        "wrong artifact digest": lambda payload: payload.__setitem__(
            "artifact_digest", "0" * 64
        ),
        "missing participant byte evidence": lambda payload: payload[
            "participants"
        ][1].pop("bytes_moved"),
        "wrong participant byte evidence": lambda payload: payload[
            "participants"
        ][1].__setitem__("bytes_moved", 1),
    }
    for label, mutate in mutations.items():
        reject(label, mutate)

    def substitute_content(payload: dict[str, object]) -> None:
        payload["source_digest"] = "0" * 64
        rebind_job_map_digests(payload, ["0" * 64] * 32, "artifact_digest")

    reject("coherent substituted source and destination content", substitute_content)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        valid = complete_matrix(root / "valid")
        result = comparator.compare_runs(
            valid, required_workloads=("file-movement",)
        )
        assert_equal(result["pass"], True, "file-movement-only acceptance")
        assert_equal(len(result["rows"]), 4, "file-movement topology rows")
        assert_equal(
            {row["workload"] for row in result["rows"]},
            {"file-movement"},
            "file-movement row name",
        )

        distinct_routes = complete_matrix(root / "distinct-routes")
        for index, manifest_path in enumerate(distinct_routes, start=1):
            result_path = result_path_for(manifest_path, "wos-showcase")
            payload = read_json(result_path)
            measurement = measurement_for(payload, "wos_file_move")
            route_path = f"/tmp/wos-showcase-fixed-{index:016x}"
            measurement["route_path"] = route_path
            for participant in measurement["participants"]:
                participant["workspace_path"] = route_path
            write_json(result_path, payload)
        result = comparator.compare_runs(
            distinct_routes, required_workloads=("file-movement",)
        )
        assert_equal(
            result["pass"],
            True,
            "ephemeral file-movement route paths are not fingerprinted",
        )

        missing = complete_matrix(root / "missing")
        result_path = result_path_for(missing[0], "wos-showcase")
        payload = read_json(result_path)
        payload["measurements"] = [
            measurement
            for measurement in payload["measurements"]
            if measurement.get("benchmark") != "wos_file_move"
        ]
        write_json(result_path, payload)
        result = comparator.compare_runs(
            missing, required_workloads=("file-movement",)
        )
        if not any(
            "one-node baseline missing workload: file-movement" in message
            for message in result["missing"]
        ):
            fail(f"missing file-movement diagnostic is absent: {result['missing']}")

        changed_runtime = complete_matrix(root / "changed-runtime")
        result_path = result_path_for(changed_runtime[-1], "wos-showcase")
        payload = read_json(result_path)
        for measurement in payload["measurements"]:
            if measurement.get("evidence_contract") == "wos-showcase-job-map-v1":
                measurement["runtime_provenance"]["python_sha256"] = "e" * 64
        write_json(result_path, payload)
        expect_error(
            comparator,
            changed_runtime,
            "workload parameters differ for file-movement",
            required_workloads=("file-movement",),
        )


def test_showcase_cross_phase_and_outer_evidence_is_coherent(comparator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        incoherent_git = complete_matrix(root / "incoherent-git")
        candidate = result_path_for(incoherent_git[0], "wos-showcase")
        payload = read_json(candidate)
        checkout = next(
            measurement
            for measurement in payload["measurements"]
            if measurement["benchmark"] == "wos_git_checkout"
        )
        checkout["commit"] = "f" * 40
        write_json(candidate, payload)
        expect_error(
            comparator,
            incoherent_git,
            "invalid fixed Git workload evidence",
            required_workloads=("git-clone", "git-checkout"),
        )

        mismatched_scale = complete_matrix(root / "mismatched-scale")
        candidate = result_path_for(mismatched_scale[0], "wos-showcase")
        payload = read_json(candidate)
        payload["scale"] = "stress"
        for measurement in payload["measurements"]:
            if measurement["benchmark"] in (
                "wos_vfsbench_create",
                "wos_vfsbench_rename",
            ):
                measurement["iterations"] = 1000
                measurement["total_work_units"] = 1000
                measurement["participants"][0]["work_units"] = 1000
        write_json(candidate, payload)
        expect_error(
            comparator,
            mismatched_scale,
            "scale evidence disagrees",
            required_workloads=("python",),
        )

        reduced_metadata = complete_matrix(root / "reduced-metadata")
        candidate = result_path_for(reduced_metadata[0], "wos-showcase")
        payload = read_json(candidate)
        create = next(
            measurement
            for measurement in payload["measurements"]
            if measurement["benchmark"] == "wos_vfsbench_create"
        )
        create["iterations"] = 31
        create["total_work_units"] = 31
        create["participants"][0]["work_units"] = 31
        write_json(candidate, payload)
        expect_error(
            comparator,
            reduced_metadata,
            "scale-fixed metadata total",
            required_workloads=("file-create",),
        )

        null_launcher = complete_matrix(root / "null-launcher")
        candidate = result_path_for(null_launcher[1], "wos-showcase")
        payload = read_json(candidate)
        measurement_for(payload, "wos_git_clone")["participants"][0][
            "launcher_host"
        ] = None
        write_json(candidate, payload)
        expect_error(
            comparator,
            null_launcher,
            "inconsistent completion evidence",
            required_workloads=("git-clone",),
        )

        mixed_runtime = complete_matrix(root / "mixed-runtime")
        candidate = result_path_for(mixed_runtime[0], "wos-showcase")
        payload = read_json(candidate)
        python_json = next(
            measurement
            for measurement in payload["measurements"]
            if measurement["benchmark"] == "wos_python_json"
        )
        python_json["runtime_provenance"]["python_sha256"] = "b" * 64
        write_json(candidate, payload)
        expect_error(
            comparator,
            mixed_runtime,
            "different node-local runtimes",
            required_workloads=("python",),
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
        test_showcase_job_map_evidence_contract_is_strict,
        test_file_movement_is_required_fingerprinted_and_strict,
        test_showcase_cross_phase_and_outer_evidence_is_coherent,
        test_cli_return_codes_and_machine_readable_result,
    ]
    for test in tests:
        test(comparator)
    print(f"{len(tests)} fixed-resource scaling comparator tests passed")


if __name__ == "__main__":
    main()
