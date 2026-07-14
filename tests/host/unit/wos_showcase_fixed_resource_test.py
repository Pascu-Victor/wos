#!/usr/bin/env python3

import copy
import hashlib
import importlib.util
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[3]
HELPER = (
    ROOT
    / "configs"
    / "rootfs"
    / "root"
    / "wos-showcase"
    / "fixed_resource_workloads.py"
)
ENTRYPOINT = HELPER.with_name("50-fixed-resource-workloads.sh")
RUN_ALL = HELPER.with_name("run-all.sh")


def fail(message: str) -> None:
    raise AssertionError(message)


def load_helper():
    spec = importlib.util.spec_from_file_location("fixed_resource_workloads", HELPER)
    if spec is None or spec.loader is None:
        fail("cannot load fixed-resource showcase helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def identity(module, host: str, launcher: str, remote_pid: int) -> dict[str, object]:
    return {
        "launcher_host": launcher,
        "runner_host": host,
        "remote_pid": remote_pid,
        "runtime_route": "local",
        "runtime_paths": list(module.LOCAL_ROUTE_PATHS),
    }


def controller_fixture(module, phase: str, hosts: list[str]):
    launcher = hosts[0]
    partitions = module.partition_jobs(hosts)
    workspace = Path("/tmp/wos-showcase-fixed-test")
    workspace_route = "host" if module.phase_uses_host_workspace(phase) else None
    workspace_path = str(workspace) if workspace_route == "host" else None
    controllers = []
    for host_index, (host, start, count) in enumerate(partitions):
        remote = host_index != 0
        controller_pid = 100 + host_index if remote else 0
        jobs = []
        for job_id in range(start, start + count):
            job_pid = 1000 + job_id if remote else 0
            job = {
                "phase": phase,
                "job_id": job_id,
                **identity(module, host, launcher, job_pid),
                "workspace_route": workspace_route,
                "workspace_path": workspace_path,
            }
            if phase == "file-move":
                job["bytes_moved"] = module.FILE_MOVE_BYTES["quick"]
            jobs.append(job)
        controllers.append(
            {
                "phase": phase,
                "target_host": host,
                "job_start": start,
                "job_count": count,
                **identity(module, host, launcher, controller_pid),
                "workspace_route": workspace_route,
                "workspace_path": workspace_path,
                "jobs": jobs,
            }
        )
    return launcher, partitions, workspace, controllers


def test_partition_and_commands(module) -> None:
    hosts = ["wos-0.wos", "wos-1.wos", "wos-2.wos"]
    if [count for _, _, count in module.partition_jobs(hosts)] != [11, 11, 10]:
        fail("three-node fixed-resource partition changed")
    fixture = {
        "repository_uri": "file:///tmp/work/git-fixture.git",
        "commit": "1" * 40,
    }
    command = module.controller_command(
        "git-clone",
        hosts[1],
        hosts[0],
        11,
        11,
        Path("/tmp/work"),
        fixture,
        20_000,
        30.0,
    )
    expected_prefix = [
        "/usr/bin/on",
        hosts[1],
        "/usr/bin/forward",
        "+/tmp/work",
    ]
    if command[:4] != expected_prefix:
        fail(f"Git controller is not strict/HOST-routed: {command}")
    for path in module.LOCAL_ROUTE_PATHS:
        if f"-{path}" not in command:
            fail(f"Git controller lacks LOCAL runtime route for {path}")
    timeout_index = command.index("--timeout-seconds")
    if float(command[timeout_index + 1]) != 25.0:
        fail(f"controller timeout does not reserve cleanup grace: {command}")

    file_move_command = module.controller_command(
        "file-move",
        hosts[1],
        hosts[0],
        11,
        11,
        Path("/tmp/work"),
        {},
        20_000,
        30.0,
        module.FILE_MOVE_BYTES["quick"],
    )
    if file_move_command[:4] != expected_prefix:
        fail(f"file-move controller is not strict/HOST-routed: {file_move_command}")
    if any(f"-{path}" not in file_move_command for path in module.LOCAL_ROUTE_PATHS):
        fail("file-move controller lacks explicit LOCAL runtime routes")
    file_bytes_index = file_move_command.index("--file-bytes")
    if int(file_move_command[file_bytes_index + 1]) != module.FILE_MOVE_BYTES["quick"]:
        fail("file-move controller did not preserve its scale-fixed byte count")

    python_command = module.controller_command(
        "python-sha256",
        hosts[1],
        hosts[0],
        11,
        11,
        Path("/tmp/work"),
        fixture,
        20_000,
        30.0,
    )
    if any(operand.startswith("+/tmp/") for operand in python_command):
        fail(f"Python controller unexpectedly HOST-routes data: {python_command}")
    rotated_hosts = ["wos-2.wos", "wos-0.wos", "wos-1.wos"]
    parsed = module.parse_hosts(",".join(rotated_hosts), "wos-2.wos")
    canonical = sorted(parsed, key=module.normalize_host)
    if [host for host, _, _ in module.partition_jobs(canonical)] != [
        "wos-0.wos",
        "wos-1.wos",
        "wos-2.wos",
    ]:
        fail("nonzero launcher changed the canonical fixed-resource host partition")

    environment = module.deterministic_environment()
    allowed_python = {
        "PYTHONHASHSEED",
        "PYTHONNOUSERSITE",
        "PYTHONDONTWRITEBYTECODE",
    }
    if environment["PATH"] != "/usr/bin:/bin" or any(
        name.startswith("PYTHON") and name not in allowed_python for name in environment
    ):
        fail("fixed-resource worker environment is not isolated")
    expected_git_config = {
        "pack.threads=1",
        "index.threads=1",
        "checkout.workers=1",
        "core.preloadIndex=false",
    }
    if not expected_git_config.issubset(module.GIT_FIXED_CONFIG):
        fail("Git fixed-resource thread controls are incomplete")
    if module.child_timeout_seconds(25.0) != 20.0:
        fail("nested worker timeout does not reserve cleanup grace")

    for phase in (
        "file-move",
        "git-clone",
        "git-checkout",
        "python-sha256",
        "python-json",
    ):
        inner_command = module.inner_job_command(
            SimpleNamespace(
                phase=phase,
                target_host=hosts[1],
                launcher_host=hosts[0],
                timeout_seconds=30.0,
                work_root="/tmp/work",
                file_bytes=module.FILE_MOVE_BYTES["quick"],
                repository_uri=fixture["repository_uri"],
                commit=fixture["commit"],
                rounds=20_000,
            ),
            7,
        )
        if inner_command[:3] != [module.WOS_PYTHON, module.WOS_HELPER, "job"]:
            fail(f"{phase} inner job is not launched directly by Python: {inner_command}")
        if module.WOS_LOCALLY in inner_command:
            fail(f"{phase} inner job retained a redundant local-placement wrapper")

    python_provenance = module.python_workload_provenance()
    if set(python_provenance) != {
        "python_hashlib_runtime_sha256",
        "python_json_runtime_sha256",
    } or any(
        len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        for digest in python_provenance.values()
    ):
        fail(f"Python workload module provenance is invalid: {python_provenance}")
    if module.RUNTIME_PROVENANCE_PATHS.get("wkictl_sha256") != module.WOS_WKICTL:
        fail("WKI control binary is absent from runtime provenance")

    preflight = module.preflight_command(
        "host-workspace",
        hosts[1],
        hosts[0],
        Path("/tmp/wos-showcase-fixed-0123456789abcdef"),
        "0123456789abcdef0123456789abcdef",
        30.0,
    )
    if "+/tmp/wos-showcase-fixed-0123456789abcdef" not in preflight:
        fail("HOST workspace preflight lacks its exact positive route")
    if any(f"-{path}" not in preflight for path in module.LOCAL_ROUTE_PATHS):
        fail("HOST workspace preflight lacks explicit LOCAL runtime routes")


def test_job_map_validation(module) -> None:
    hosts = ["wos-0.wos", "wos-1.wos", "wos-2.wos"]
    launcher, partitions, workspace, controllers = controller_fixture(
        module, "git-clone", hosts
    )
    participants, jobs = module.validate_controller_results(
        "git-clone", controllers, partitions, launcher, workspace
    )
    if sorted(jobs) != list(range(32)):
        fail("validated job map does not cover exactly 0..31")
    if [participant["work_units"] for participant in participants] != [11, 11, 10]:
        fail("participant work map differs from the fixed partition")
    job_digests = {
        job_id: module.sha256_job(job_id, 1) for job_id in range(module.TOTAL_JOBS)
    }
    module.attach_participant_digests(participants, job_digests)
    if any(
        len(participant["job_digests"]) != participant["work_units"]
        for participant in participants
    ):
        fail("participant evidence omits per-job content digests")

    move_launcher, move_partitions, move_workspace, move_controllers = (
        controller_fixture(module, "file-move", hosts)
    )
    move_participants, move_jobs = module.validate_controller_results(
        "file-move",
        move_controllers,
        move_partitions,
        move_launcher,
        move_workspace,
    )
    if sorted(move_jobs) != list(range(module.TOTAL_JOBS)) or any(
        participant["bytes_moved"]
        != participant["work_units"] * module.FILE_MOVE_BYTES["quick"]
        for participant in move_participants
    ):
        fail("file-move job map lost fixed byte evidence")
    invalid_move_bytes = copy.deepcopy(move_controllers)
    invalid_move_bytes[1]["jobs"][0]["bytes_moved"] = 0
    try:
        module.validate_controller_results(
            "file-move",
            invalid_move_bytes,
            move_partitions,
            move_launcher,
            move_workspace,
        )
    except module.WorkloadError:
        pass
    else:
        fail("file-move job map accepted invalid byte evidence")

    mutations = []
    duplicate = copy.deepcopy(controllers)
    duplicate[1]["jobs"][0]["job_id"] = 0
    mutations.append(duplicate)
    local_remote_pid = copy.deepcopy(controllers)
    local_remote_pid[0]["jobs"][0]["remote_pid"] = 1
    mutations.append(local_remote_pid)
    missing_runtime_path = copy.deepcopy(controllers)
    missing_runtime_path[2]["runtime_paths"].pop()
    mutations.append(missing_runtime_path)
    for mutation in mutations:
        try:
            module.validate_controller_results(
                "git-clone", mutation, partitions, launcher, workspace
            )
        except module.WorkloadError:
            pass
        else:
            fail("invalid job-map evidence was accepted")


def test_file_move_copy_and_validation(module) -> None:
    if module.FILE_MOVE_BYTES != {
        "quick": 2 * 1024 * 1024,
        "full": 8 * 1024 * 1024,
        "stress": 32 * 1024 * 1024,
    } or module.FILE_MOVE_CHUNK_BYTES != 2 * 1024 * 1024:
        fail("file-move scale or chunk contract changed")

    class ShortWriter:
        def __init__(self) -> None:
            self.data = bytearray()

        def write(self, data) -> int:
            count = min(3, len(data))
            self.data.extend(data[:count])
            return count

    short_writer = ShortWriter()
    module.write_all(short_writer, b"explicit-short-write-check")
    if bytes(short_writer.data) != b"explicit-short-write-check":
        fail("file-move full-write loop dropped a short-write tail")

    with tempfile.TemporaryDirectory(prefix="wos-file-move-test-") as temporary:
        root = Path(temporary)
        source = root / module.FILE_MOVE_SOURCE_RELATIVE_PATH
        destinations = root / module.FILE_MOVE_DESTINATION_RELATIVE_PATH
        source.parent.mkdir()
        destinations.mkdir()
        payload = bytes(range(251)) * 19
        source.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        fixture = {
            "source_path": source,
            "destination_path": destinations,
            "bytes_per_file": len(payload),
            "source_digest": digest,
        }
        for job_id in range(module.TOTAL_JOBS):
            copied = module.copy_file(
                source, module.file_move_destination(root, job_id), len(payload)
            )
            if copied != len(payload):
                fail("small local file-move copy returned invalid evidence")
        digests = module.validate_file_move_outputs(fixture)
        if set(digests) != set(range(module.TOTAL_JOBS)) or any(
            value != digest for value in digests.values()
        ):
            fail("launcher file-move validation lost exact output evidence")

        try:
            module.copy_file(
                source, module.file_move_destination(root, 0), len(payload)
            )
        except module.WorkloadError:
            pass
        else:
            fail("file-move destination creation was not exclusive")

        module.file_move_destination(root, 0).write_bytes(payload + b"corrupt")
        try:
            module.validate_file_move_outputs(fixture)
        except module.WorkloadError:
            pass
        else:
            fail("launcher validation accepted a corrupt file-move output")

        module.file_move_destination(root, 0).write_bytes(payload)
        source.write_bytes(payload + b"changed")
        try:
            module.validate_file_move_outputs(fixture)
        except module.WorkloadError:
            pass
        else:
            fail("launcher validation accepted a changed file-move source")


def test_entrypoint_and_self_test(module) -> None:
    entrypoint = ENTRYPOINT.read_text(encoding="utf-8")
    if "exec /usr/bin/locally /usr/bin/python3" not in entrypoint:
        fail("fixed-resource coordinator is not launcher-pinned")
    if "50-fixed-resource-workloads.sh" not in RUN_ALL.read_text(encoding="utf-8"):
        fail("fixed-resource workload is absent from the default showcase suite")
    if not os.access(ENTRYPOINT, os.X_OK):
        fail("fixed-resource workload entrypoint is not executable")

    work_root, owner = module.create_work_root()
    try:
        try:
            module.remove_work_root(work_root, owner + "-wrong")
        except module.WorkloadError:
            pass
        else:
            fail("fixed-resource cleanup accepted the wrong ownership token")
        if not work_root.is_dir():
            fail("failed cleanup safety check removed the private workspace")
    finally:
        if work_root.exists():
            module.remove_work_root(work_root, owner)
    with tempfile.TemporaryDirectory() as temporary:
        result = subprocess.run(
            [
                sys.executable,
                str(HELPER),
                "self-test",
                "--timeout-seconds",
                "120",
            ],
            cwd=temporary,
            check=False,
            text=True,
            capture_output=True,
        )
    if (
        result.returncode != 0
        or "fixed-resource workload self-test passed" not in result.stdout
    ):
        fail(
            "fixed-resource self-test failed: "
            f"rc={result.returncode} stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    if module.EVIDENCE_CONTRACT != "wos-showcase-job-map-v1":
        fail("fixed-resource evidence contract changed unexpectedly")


def test_timeout_cleanup(module) -> None:
    started = time.monotonic()
    try:
        module.run_checked(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            timeout_seconds=0.05,
            context="cleanup self-test",
        )
    except module.WorkloadError as exc:
        if "cleanup self-test timed out" not in str(exc):
            fail(f"unexpected timeout diagnostic: {exc}")
    else:
        fail("timed subprocess unexpectedly completed")
    if time.monotonic() - started > 5.0:
        fail("subprocess cleanup exceeded its bounded grace periods")
    if module._ACTIVE_PROCESSES:
        fail("timed subprocess remained registered after cleanup")


def test_timed_job_uses_preflight_evidence(module) -> None:
    original_cwd = Path.cwd()
    original_identity = module.read_proc_identity
    original_verify = module.verify_routes
    work_root, owner = module.create_work_root()
    module.read_proc_identity = lambda: {
        "launcher_host": "wos-0.wos",
        "runner_host": "wos-0.wos",
        "remote_pid": 0,
    }

    def reject_timed_probe(*_args, **_kwargs):
        fail("timed job repeated the untimed route preflight")

    module.verify_routes = reject_timed_probe
    try:
        result = module.run_job(
            SimpleNamespace(
                phase="python-sha256",
                target_host="wos-0.wos",
                launcher_host="wos-0.wos",
                job_id=0,
                work_root=str(work_root),
                repository_uri="",
                commit="",
                rounds=1,
                timeout_seconds=30.0,
            )
        )
        if result.get("runtime_route") != "local" or Path.cwd() != work_root:
            fail("timed job did not inherit preflight routing/cwd evidence")
    finally:
        os.chdir(original_cwd)
        module.read_proc_identity = original_identity
        module.verify_routes = original_verify
        module.remove_work_root(work_root, owner)


def main() -> None:
    module = load_helper()
    test_partition_and_commands(module)
    test_job_map_validation(module)
    test_file_move_copy_and_validation(module)
    test_entrypoint_and_self_test(module)
    test_timeout_cleanup(module)
    test_timed_job_uses_preflight_evidence(module)
    print("6 WOS fixed-resource showcase tests passed")


if __name__ == "__main__":
    main()
