#!/usr/bin/env python3
"""Fixed-total, all-node file-move, Git, and Python showcase workloads for WOS."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Sequence

TOTAL_JOBS = 32
MAX_HOSTS = 4
EVIDENCE_CONTRACT = "wos-showcase-job-map-v1"
FILE_MOVE_WORKLOAD_ID = "wos-showcase-file-move-v1"
FILE_MOVE_OPERATION = "stream-copy-read-write-close"
FILE_MOVE_SOURCE_RELATIVE_PATH = "file-move/source.bin"
FILE_MOVE_DESTINATION_RELATIVE_PATH = "file-move/destinations"
FILE_MOVE_CACHE_POLICY = "warm-shared-source-cold-host-destinations"
FILE_MOVE_CHUNK_BYTES = 2 * 1024 * 1024
FILE_MOVE_BYTES = {
    "quick": 2 * 1024 * 1024,
    "full": 8 * 1024 * 1024,
    "stress": 32 * 1024 * 1024,
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
WORKLOAD_PHASES = (
    "file-move",
    "git-clone",
    "git-checkout",
    "python-sha256",
    "python-json",
)
HOST_WORKSPACE_PHASES = frozenset(("file-move", "git-clone", "git-checkout"))
FIXTURE_ID = "wos-showcase-git-fixture-v1"
FIXTURE_FILES = 100
FIXTURE_FILE_BYTES = 16 * 1024
FIXTURE_COMMIT_DATE = "2000-01-01T00:00:00+0000"
FIXTURE_COMMIT = "01cc47f97d58c90edec1b043d7288563d82a923d"
FIXTURE_TREE = "104396b28c8cbbe7f97e3d6a13704881d700f421"
FIXTURE_DIGEST = "20c827ddcaa537afd3379325d782e732190950eaec535af107f4c1580949e1b9"
WOS_HELPER = "/root/wos-showcase/fixed_resource_workloads.py"
WOS_PYTHON = "/usr/bin/python3"
WOS_GIT = "/usr/bin/git"
WOS_LOCALLY = "/usr/bin/locally"
WOS_ON = "/usr/bin/on"
WOS_FORWARD = "/usr/bin/forward"
WOS_WKICTL = "/usr/bin/wkictl"
LOCAL_ROUTE_PATHS = (
    "/root/wos-showcase",
    "/usr",
    "/bin",
    "/lib",
    "/lib64",
    "/libexec",
    "/share",
    "/tmp",
)
GIT_FIXED_CONFIG = (
    "-c",
    "pack.threads=1",
    "-c",
    "index.threads=1",
    "-c",
    "checkout.workers=1",
    "-c",
    "core.preloadIndex=false",
)
ROUTE_RE = re.compile(r"^vfs-task\[\d+\]: (.+) -> (local|host)$")
OID_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
HOST_RE = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*"
)
ROUND_COUNTS = {
    "quick": 20_000,
    "full": 200_000,
    "stress": 1_000_000,
}
EXPECTED_PYTHON_DIGESTS = {
    "python-sha256": {
        "quick": "24e31aa0d4cf8694503463fec69aef8b1bfa91a5cc7d2a887b497576d9e33887",
        "full": "da2e3b5fca1be6e37b4fe2bb4e5b118fd1fd090fb77be8528686cb49734007a9",
        "stress": "c670c4081279ba457b212b76af90859fb08c7aea88acf97ff1fcf1bf4ae52e5c",
    },
    "python-json": {
        "quick": "63f0520ef6c8664e16473f9936dd84d09c56c7a0d2a35a8ec3fb519ffb0adade",
        "full": "79332e7c99c2097d7ec9a0603433a159925abbd8c44cec38c258ae7600926672",
        "stress": "1586dfe1209b9d4266e21b333e22eb56d2420c1e246b99b5901ac99bd2f3fb33",
    },
}
RUNTIME_PROVENANCE_PATHS = {
    "helper_sha256": WOS_HELPER,
    "python_sha256": WOS_PYTHON,
    "git_sha256": WOS_GIT,
    "git_upload_pack_sha256": "/usr/bin/git-upload-pack",
    "wkictl_sha256": WOS_WKICTL,
}
PYTHON_WORKLOAD_PROVENANCE_FIELDS = {
    "python_hashlib_runtime_sha256",
    "python_json_runtime_sha256",
}
RUNTIME_PROVENANCE_FIELDS = (
    set(RUNTIME_PROVENANCE_PATHS) | PYTHON_WORKLOAD_PROVENANCE_FIELDS
)


class WorkloadError(RuntimeError):
    pass


_ACTIVE_PROCESSES: set[subprocess.Popen[str]] = set()
_HANDLING_SIGNAL = False


def log(message: str) -> None:
    print(f"[fixed-resource] {message}", file=sys.stderr, flush=True)


def normalize_host(host: str) -> str:
    value = host.strip()
    return value[:-4] if value.endswith(".wos") else value


def require_safe_host(host: str, context: str) -> None:
    try:
        encoded = host.encode("ascii")
    except UnicodeEncodeError as exc:
        raise WorkloadError(f"{context} is not an ASCII WOS hostname") from exc
    if not encoded or len(encoded) > 63 or HOST_RE.fullmatch(host) is None:
        raise WorkloadError(f"{context} is not a safe WOS hostname")


def parse_hosts(raw: str, launcher: str) -> list[str]:
    fields = raw.split(",")
    if any(not field.strip() for field in fields):
        raise WorkloadError("host list contains an empty entry")
    hosts = [field.strip() for field in fields]
    if not 1 <= len(hosts) <= MAX_HOSTS:
        raise WorkloadError("fixed-resource workloads require 1..4 hosts")
    normalized = [normalize_host(host) for host in hosts]
    for host in hosts:
        require_safe_host(host, f"host {host!r}")
    for host in normalized:
        require_safe_host(host, f"normalized host {host!r}")
    require_safe_host(launcher, "observed launcher")
    require_safe_host(normalize_host(launcher), "normalized launcher")
    if len(set(normalized)) != len(normalized):
        raise WorkloadError("host list contains duplicate WOS identities")
    if normalized[0] != normalize_host(launcher):
        raise WorkloadError("the observed launcher must be the first requested host")
    return hosts


def partition_jobs(
    hosts: Sequence[str], total: int = TOTAL_JOBS
) -> list[tuple[str, int, int]]:
    if not 1 <= len(hosts) <= MAX_HOSTS or total < len(hosts):
        raise WorkloadError("cannot make a positive fixed-total host partition")
    base, extra = divmod(total, len(hosts))
    result: list[tuple[str, int, int]] = []
    start = 0
    for index, host in enumerate(hosts):
        count = base + (1 if index < extra else 0)
        result.append((host, start, count))
        start += count
    if start != total:
        raise WorkloadError("internal fixed-total partition error")
    return result


def compact_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def parse_json_object(text: str, context: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise WorkloadError(f"{context} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise WorkloadError(f"{context} did not return a JSON object")
    return payload


def summarize_output(text: str, limit: int = 500) -> str:
    compact = " ".join(text.split())
    return compact if len(compact) <= limit else compact[:limit] + "..."


def register_process(process: subprocess.Popen[str]) -> subprocess.Popen[str]:
    _ACTIVE_PROCESSES.add(process)
    return process


def forget_process(process: subprocess.Popen[str]) -> None:
    _ACTIVE_PROCESSES.discard(process)


def signal_process_group(process: subprocess.Popen[str], signum: int) -> None:
    try:
        if hasattr(os, "killpg"):
            os.killpg(process.pid, signum)
        else:
            process.send_signal(signum)
    except OSError:
        pass


def terminate_processes(
    processes: Iterable[subprocess.Popen[str]] | None = None,
) -> bool:
    targets = list(_ACTIVE_PROCESSES if processes is None else processes)
    for process in targets:
        if process.poll() is None:
            signal_process_group(process, signal.SIGTERM)
    deadline = time.monotonic() + 2.0
    for process in targets:
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except (OSError, subprocess.TimeoutExpired):
                pass
    remaining = [process for process in targets if process.poll() is None]
    for process in remaining:
        signal_process_group(process, signal.SIGKILL)
    deadline = time.monotonic() + 2.0
    for process in remaining:
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except (OSError, subprocess.TimeoutExpired):
                pass
    for process in targets:
        if process.poll() is not None:
            forget_process(process)
    return all(process.poll() is not None for process in targets)


def signal_handler(signum: int, _frame: object) -> None:
    global _HANDLING_SIGNAL
    if _HANDLING_SIGNAL:
        raise SystemExit(128 + signum)
    _HANDLING_SIGNAL = True
    terminate_processes()
    raise SystemExit(128 + signum)


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, signal_handler)


def deterministic_environment() -> dict[str, str]:
    env = dict(os.environ)
    for name in tuple(env):
        if name.startswith("GIT_") or name.startswith("PYTHON"):
            del env[name]
    env.update(
        {
            "PATH": "/usr/bin:/bin",
            "LC_ALL": "C",
            "LANG": "C",
            "TZ": "UTC",
            "PYTHONHASHSEED": "0",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "GIT_ALLOW_PROTOCOL": "file",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_DEFAULT_HASH": "sha1",
        }
    )
    return env


def child_timeout_seconds(parent_timeout_seconds: float) -> float:
    if parent_timeout_seconds > 10.0:
        return parent_timeout_seconds - 5.0
    return max(0.01, parent_timeout_seconds * 0.5)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise WorkloadError(f"cannot fingerprint runtime file {path}: {exc}") from exc
    return digest.hexdigest()


def runtime_provenance() -> dict[str, str]:
    provenance = {
        field: sha256_file(Path(path))
        for field, path in RUNTIME_PROVENANCE_PATHS.items()
    }
    provenance.update(python_workload_provenance())
    return provenance


def module_set_sha256(module_names: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for module_name in sorted(set(module_names)):
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise WorkloadError(
                f"cannot import Python workload module {module_name}: {exc}"
            ) from exc
        raw_path = getattr(module, "__file__", None)
        if not isinstance(raw_path, str) or not raw_path:
            raise WorkloadError(
                f"Python workload module {module_name} has no file provenance"
            )
        path = Path(raw_path)
        path_bytes = str(path).encode("utf-8")
        name_bytes = module_name.encode("ascii")
        digest.update(len(name_bytes).to_bytes(4, "big"))
        digest.update(name_bytes)
        digest.update(len(path_bytes).to_bytes(4, "big"))
        digest.update(path_bytes)
        digest.update(bytes.fromhex(sha256_file(path)))
    return digest.hexdigest()


def python_workload_provenance() -> dict[str, str]:
    hashlib_backend = type(hashlib.sha256()).__module__
    json_encoder = importlib.import_module("json.encoder")
    json_decoder = importlib.import_module("json.decoder")
    json_scanner = importlib.import_module("json.scanner")
    json_backends = {
        callback.__module__
        for callback in (
            getattr(json_encoder, "c_make_encoder", None),
            getattr(json_decoder, "c_scanstring", None),
            getattr(json_scanner, "c_make_scanner", None),
        )
        if callback is not None
    }
    return {
        "python_hashlib_runtime_sha256": module_set_sha256(
            ("hashlib", hashlib_backend)
        ),
        "python_json_runtime_sha256": module_set_sha256(
            ("json", "json.encoder", "json.decoder", "json.scanner", *json_backends)
        ),
    }


def spawn(
    command: Sequence[str], *, env: dict[str, str] | None = None
) -> subprocess.Popen[str]:
    try:
        return register_process(
            subprocess.Popen(
                list(command),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                start_new_session=True,
            )
        )
    except OSError as exc:
        raise WorkloadError(f"failed to start {command[0]}: {exc}") from exc


def communicate_many(
    processes: Sequence[tuple[str, subprocess.Popen[str]]], timeout_seconds: float
) -> list[tuple[str, str, str]]:
    deadline = time.monotonic() + timeout_seconds
    outputs: list[tuple[str, str, str]] = []
    process_set = [process for _, process in processes]
    try:
        for context, process in processes:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, timeout_seconds)
            stdout, stderr = process.communicate(timeout=remaining)
            forget_process(process)
            if process.returncode != 0:
                raise WorkloadError(
                    f"{context} failed with status {process.returncode}: {summarize_output(stderr or stdout)}"
                )
            outputs.append((context, stdout, stderr))
    except subprocess.TimeoutExpired as exc:
        stopped = terminate_processes(process_set)
        cleanup = "" if stopped else "; subprocess cleanup did not complete"
        raise WorkloadError(
            f"concurrent subprocesses exceeded {timeout_seconds:g} seconds{cleanup}"
        ) from exc
    except WorkloadError as exc:
        if not terminate_processes(process_set):
            raise WorkloadError(f"{exc}; subprocess cleanup did not complete") from exc
        raise
    return outputs


def run_checked(
    command: Sequence[str],
    *,
    timeout_seconds: float,
    env: dict[str, str] | None = None,
    context: str | None = None,
) -> str:
    label = context or command[0]
    process = spawn(command, env=env)
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        stopped = terminate_processes((process,))
        cleanup = "" if stopped else "; subprocess cleanup did not complete"
        raise WorkloadError(f"{label} timed out{cleanup}") from exc
    finally:
        if process.poll() is not None:
            forget_process(process)
    if process.returncode != 0:
        raise WorkloadError(
            f"{label} failed with status {process.returncode}: {summarize_output(stderr or stdout)}"
        )
    return stdout


def read_proc_identity() -> dict[str, Any]:
    try:
        launcher = Path("/proc/self/wki_launcher").read_text(encoding="utf-8").strip()
        runner = Path("/proc/self/wki_runner").read_text(encoding="utf-8").strip()
        remote_pid_text = (
            Path("/proc/self/wki_remote_pid").read_text(encoding="utf-8").strip()
        )
        remote_pid = int(remote_pid_text, 10)
    except (OSError, ValueError) as exc:
        raise WorkloadError(f"cannot read WKI process identity: {exc}") from exc
    if not launcher or not runner or remote_pid < 0:
        raise WorkloadError("WKI process identity is incomplete")
    return {
        "launcher_host": launcher,
        "runner_host": runner,
        "remote_pid": remote_pid,
    }


def parse_task_routes(text: str) -> dict[str, str]:
    routes: dict[str, str] = {}
    for line in text.splitlines():
        match = ROUTE_RE.fullmatch(line.strip())
        if match is not None:
            routes[match.group(1)] = match.group(2)
    return routes


def verify_routes(work_root: str | None, timeout_seconds: float) -> dict[str, Any]:
    output = run_checked(
        (WOS_LOCALLY, WOS_WKICTL, "vfs", "list"),
        timeout_seconds=timeout_seconds,
        env=deterministic_environment(),
        context="wkictl vfs list",
    )
    routes = parse_task_routes(output)
    missing_local = [path for path in LOCAL_ROUTE_PATHS if routes.get(path) != "local"]
    if missing_local:
        raise WorkloadError(
            f"missing LOCAL runtime route(s): {', '.join(missing_local)}"
        )
    if work_root is not None and routes.get(work_root) != "host":
        raise WorkloadError(f"workspace route is not HOST: {work_root}")
    return {
        "runtime_route": "local",
        "runtime_paths": list(LOCAL_ROUTE_PATHS),
        "workspace_route": "host" if work_root is not None else None,
        "workspace_path": work_root,
    }


def phase_uses_host_workspace(phase: str) -> bool:
    return phase in HOST_WORKSPACE_PHASES


def inherited_route_evidence(phase: str, work_root: str) -> dict[str, Any]:
    workspace_is_host = phase_uses_host_workspace(phase)
    return {
        "runtime_route": "local",
        "runtime_paths": list(LOCAL_ROUTE_PATHS),
        "workspace_route": "host" if workspace_is_host else None,
        "workspace_path": work_root if workspace_is_host else None,
    }


def validate_identity(identity: dict[str, Any], target: str, launcher: str) -> None:
    target_normalized = normalize_host(target)
    launcher_normalized = normalize_host(launcher)
    runner_normalized = normalize_host(str(identity.get("runner_host", "")))
    observed_launcher = normalize_host(str(identity.get("launcher_host", "")))
    remote_pid = identity.get("remote_pid")
    if (
        runner_normalized != target_normalized
        or observed_launcher != launcher_normalized
    ):
        raise WorkloadError(
            f"identity mismatch for {target}: launcher={observed_launcher} runner={runner_normalized}"
        )
    expected_remote = target_normalized != launcher_normalized
    if isinstance(remote_pid, bool) or not isinstance(remote_pid, int):
        raise WorkloadError(f"invalid remote pid evidence for {target}")
    if (expected_remote and remote_pid <= 0) or (
        not expected_remote and remote_pid != 0
    ):
        raise WorkloadError(f"remote pid evidence disagrees with target {target}")


def deterministic_file_bytes(file_index: int, size: int) -> bytes:
    seed = hashlib.sha256(f"{FIXTURE_ID}:{file_index}".encode("ascii")).digest()
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(output[:size])


def deterministic_file_move_chunks(size: int) -> Iterable[bytes]:
    if size <= 0:
        raise WorkloadError("file-move source size must be positive")
    seed = hashlib.sha256(FILE_MOVE_WORKLOAD_ID.encode("ascii")).digest()
    remaining = size
    counter = 0
    carry = b""
    while remaining > 0:
        wanted = min(remaining, FILE_MOVE_CHUNK_BYTES)
        output = bytearray()
        if carry:
            take = min(wanted, len(carry))
            output.extend(carry[:take])
            carry = carry[take:]
        while len(output) < wanted:
            block = hashlib.sha256(seed + counter.to_bytes(8, "big")).digest()
            counter += 1
            take = min(wanted - len(output), len(block))
            output.extend(block[:take])
            carry = block[take:]
        remaining -= len(output)
        yield bytes(output)


def write_all(destination: BinaryIO, data: bytes) -> None:
    remaining = memoryview(data)
    while remaining:
        written = destination.write(remaining)
        if (
            isinstance(written, bool)
            or not isinstance(written, int)
            or written <= 0
            or written > len(remaining)
        ):
            raise WorkloadError("file-move destination returned an invalid write count")
        remaining = remaining[written:]


def copy_file(source_path: Path, destination_path: Path, expected_bytes: int) -> int:
    copied = 0
    try:
        with source_path.open("rb", buffering=0) as source, destination_path.open(
            "xb", buffering=0
        ) as destination:
            while chunk := source.read(FILE_MOVE_CHUNK_BYTES):
                copied += len(chunk)
                if copied > expected_bytes:
                    raise WorkloadError("file-move source is larger than expected")
                write_all(destination, chunk)
    except WorkloadError:
        raise
    except OSError as exc:
        raise WorkloadError(
            f"cannot copy file-move source to {destination_path}: {exc}"
        ) from exc
    if copied != expected_bytes:
        raise WorkloadError(
            f"file-move copied {copied} bytes; expected {expected_bytes}"
        )
    return copied


def create_file_move_fixture(root: Path, scale: str) -> dict[str, Any]:
    try:
        bytes_per_file = FILE_MOVE_BYTES[scale]
        expected_digest = FILE_MOVE_SOURCE_DIGESTS[scale]
    except KeyError as exc:
        raise WorkloadError(f"unknown file-move scale: {scale}") from exc

    source_path = root / FILE_MOVE_SOURCE_RELATIVE_PATH
    destination_path = root / FILE_MOVE_DESTINATION_RELATIVE_PATH
    digest = hashlib.sha256()
    try:
        source_path.parent.mkdir()
        destination_path.mkdir()
        with source_path.open("xb", buffering=0) as source:
            for chunk in deterministic_file_move_chunks(bytes_per_file):
                write_all(source, chunk)
                digest.update(chunk)
    except WorkloadError:
        raise
    except OSError as exc:
        raise WorkloadError(f"cannot create deterministic file-move fixture: {exc}") from exc

    created_digest = digest.hexdigest()
    try:
        created_size = source_path.stat().st_size
    except OSError as exc:
        raise WorkloadError(f"cannot stat file-move source: {exc}") from exc
    # Re-read before timing both to validate the fixture and to make the source
    # cache policy explicit. Remote file contexts are still cold when jobs open it.
    warmed_digest = sha256_file(source_path)
    if (
        created_size != bytes_per_file
        or created_digest != expected_digest
        or warmed_digest != expected_digest
    ):
        raise WorkloadError("deterministic file-move fixture identity changed")
    return {
        "source_path": source_path,
        "destination_path": destination_path,
        "source_relative_path": FILE_MOVE_SOURCE_RELATIVE_PATH,
        "destination_relative_path": FILE_MOVE_DESTINATION_RELATIVE_PATH,
        "bytes_per_file": bytes_per_file,
        "source_digest": expected_digest,
    }


def tree_manifest(root: Path) -> tuple[int, int, str]:
    digest = hashlib.sha256()
    file_count = 0
    byte_count = 0
    for directory, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(name for name in dirnames if name != ".git")
        for filename in sorted(filenames):
            path = Path(directory) / filename
            relative = path.relative_to(root).as_posix()
            data = path.read_bytes()
            path_bytes = relative.encode("utf-8")
            file_digest = hashlib.sha256(data).digest()
            digest.update(len(path_bytes).to_bytes(4, "big"))
            digest.update(path_bytes)
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(file_digest)
            file_count += 1
            byte_count += len(data)
    return file_count, byte_count, digest.hexdigest()


def git_output(
    git: str,
    arguments: Sequence[str],
    *,
    timeout_seconds: float,
    context: str,
) -> str:
    return run_checked(
        (git, *GIT_FIXED_CONFIG, *arguments),
        timeout_seconds=timeout_seconds,
        env=deterministic_environment(),
        context=context,
    ).strip()


def pin_repository_pack_threads(
    root: Path, git: str, timeout_seconds: float, *, bare: bool
) -> None:
    repository_args = ("--git-dir", str(root)) if bare else ("-C", str(root))
    for arguments, context in (
        (
            (*repository_args, "config", "--local", "pack.threads", "1"),
            f"pin pack threads for {root}",
        ),
        (
            (*repository_args, "config", "--local", "--get", "pack.threads"),
            f"verify pack threads for {root}",
        ),
    ):
        value = run_checked(
            (git, *arguments),
            timeout_seconds=timeout_seconds,
            env=deterministic_environment(),
            context=context,
        ).strip()
    if value != "1":
        raise WorkloadError(f"repository pack thread pin did not persist: {root}")


def create_git_fixture(root: Path, git: str, timeout_seconds: float) -> dict[str, Any]:
    seed_root = root / "fixture-seed"
    bare_root = root / "git-fixture.git"
    seed_root.mkdir(parents=True)
    for index in range(FIXTURE_FILES):
        path = seed_root / "tree" / f"{index // 10:02d}" / f"file-{index:03d}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(deterministic_file_bytes(index, FIXTURE_FILE_BYTES))
        path.chmod(0o644)

    file_count, byte_count, manifest_digest = tree_manifest(seed_root)
    if file_count != FIXTURE_FILES or byte_count != FIXTURE_FILES * FIXTURE_FILE_BYTES:
        raise WorkloadError("deterministic Git fixture has an unexpected shape")

    git_output(
        git,
        ("init", "--quiet", str(seed_root)),
        timeout_seconds=timeout_seconds,
        context="git init fixture",
    )
    git_output(
        git,
        ("-C", str(seed_root), "checkout", "--quiet", "-b", "main"),
        timeout_seconds=timeout_seconds,
        context="git create fixture branch",
    )
    pin_repository_pack_threads(seed_root, git, timeout_seconds, bare=False)
    git_output(
        git,
        ("-C", str(seed_root), "add", "--all"),
        timeout_seconds=timeout_seconds,
        context="git add fixture",
    )
    commit_env = deterministic_environment()
    commit_env.update(
        {
            "GIT_AUTHOR_NAME": "WOS Showcase",
            "GIT_AUTHOR_EMAIL": "showcase@wos.invalid",
            "GIT_COMMITTER_NAME": "WOS Showcase",
            "GIT_COMMITTER_EMAIL": "showcase@wos.invalid",
            "GIT_AUTHOR_DATE": FIXTURE_COMMIT_DATE,
            "GIT_COMMITTER_DATE": FIXTURE_COMMIT_DATE,
        }
    )
    run_checked(
        (
            git,
            *GIT_FIXED_CONFIG,
            "-C",
            str(seed_root),
            "-c",
            "commit.gpgSign=false",
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "deterministic WOS showcase fixture",
        ),
        timeout_seconds=timeout_seconds,
        env=commit_env,
        context="git commit fixture",
    )
    commit = git_output(
        git,
        ("-C", str(seed_root), "rev-parse", "HEAD^{commit}"),
        timeout_seconds=timeout_seconds,
        context="git fixture commit identity",
    )
    tree_oid = git_output(
        git,
        ("-C", str(seed_root), "rev-parse", "HEAD^{tree}"),
        timeout_seconds=timeout_seconds,
        context="git fixture tree identity",
    )
    if OID_RE.fullmatch(commit) is None or OID_RE.fullmatch(tree_oid) is None:
        raise WorkloadError(
            "Git fixture did not use deterministic SHA-1 object identities"
        )
    if (
        commit != FIXTURE_COMMIT
        or tree_oid != FIXTURE_TREE
        or manifest_digest != FIXTURE_DIGEST
    ):
        raise WorkloadError("Git fixture identity differs from the fixed workload")

    git_output(
        git,
        ("clone", "--quiet", "--bare", "--no-local", str(seed_root), str(bare_root)),
        timeout_seconds=timeout_seconds,
        context="git create bare fixture",
    )
    git_output(
        git,
        ("--git-dir", str(bare_root), "symbolic-ref", "HEAD", "refs/heads/main"),
        timeout_seconds=timeout_seconds,
        context="git set bare fixture HEAD",
    )
    pin_repository_pack_threads(bare_root, git, timeout_seconds, bare=True)
    git_output(
        git,
        ("--git-dir", str(bare_root), "fsck", "--strict"),
        timeout_seconds=timeout_seconds,
        context="git verify bare fixture",
    )
    shutil.rmtree(seed_root)
    return {
        "repository": FIXTURE_ID,
        "repository_uri": bare_root.resolve().as_uri(),
        "commit": commit,
        "tree_oid": tree_oid,
        "checkout_files": file_count,
        "fixture_bytes": byte_count,
        "fixture_digest": manifest_digest,
    }


def sha256_job(job_id: int, rounds: int) -> str:
    payload = hashlib.sha256(f"wos-python-sha256-v1:{job_id}".encode("ascii")).digest()
    state = hashlib.sha256(payload).digest()
    for index in range(rounds):
        state = hashlib.sha256(state + payload + index.to_bytes(8, "little")).digest()
    return state.hex()


def json_job(job_id: int, rounds: int) -> str:
    document: dict[str, Any] = {
        "benchmark": "wos-python-json-v1",
        "job": job_id,
        "payload": ["alpha", "beta", "gamma", "delta"],
        "round": 0,
        "values": [job_id, job_id + 1, job_id + 2, job_id + 3],
    }
    encoded = b""
    for index in range(rounds):
        document["round"] = index
        encoded = json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        decoded = json.loads(encoded)
        if not isinstance(decoded, dict):
            raise WorkloadError("canonical JSON round trip did not produce an object")
        document = decoded
    return hashlib.sha256(encoded).hexdigest()


def aggregate_digests(job_digests: dict[int, str]) -> str:
    if sorted(job_digests) != list(range(TOTAL_JOBS)):
        raise WorkloadError("job digest set does not cover exactly 0..31")
    digest = hashlib.sha256()
    for job_id in range(TOTAL_JOBS):
        value = job_digests[job_id]
        if SHA256_RE.fullmatch(value) is None:
            raise WorkloadError(f"job {job_id} returned an invalid SHA-256 digest")
        digest.update(job_id.to_bytes(4, "big"))
        digest.update(bytes.fromhex(value))
    return digest.hexdigest()


def job_destination(work_root: Path, job_id: int) -> Path:
    return work_root / "clones" / f"clone-{job_id:03d}"


def file_move_destination(work_root: Path, job_id: int) -> Path:
    return work_root / FILE_MOVE_DESTINATION_RELATIVE_PATH / f"file-{job_id:03d}.bin"


def run_job(args: argparse.Namespace) -> dict[str, Any]:
    os.chdir(args.work_root)
    identity = read_proc_identity()
    validate_identity(identity, args.target_host, args.launcher_host)
    result: dict[str, Any] = {
        "phase": args.phase,
        "job_id": args.job_id,
        **identity,
        **inherited_route_evidence(args.phase, args.work_root),
    }
    env = deterministic_environment()
    if args.phase == "file-move":
        source_path = Path(args.work_root) / FILE_MOVE_SOURCE_RELATIVE_PATH
        destination_path = file_move_destination(Path(args.work_root), args.job_id)
        copied = copy_file(source_path, destination_path, args.file_bytes)
        result.update(
            {
                "bytes_moved": copied,
                "chunk_bytes": FILE_MOVE_CHUNK_BYTES,
                "source_relative_path": FILE_MOVE_SOURCE_RELATIVE_PATH,
                "destination_relative_path": (
                    f"{FILE_MOVE_DESTINATION_RELATIVE_PATH}/file-{args.job_id:03d}.bin"
                ),
            }
        )
    elif args.phase == "git-clone":
        destination = job_destination(Path(args.work_root), args.job_id)
        run_checked(
            (
                WOS_GIT,
                *GIT_FIXED_CONFIG,
                "-c",
                "protocol.file.allow=always",
                "clone",
                "--quiet",
                "--no-local",
                "--no-checkout",
                "--no-tags",
                "--single-branch",
                "--branch",
                "main",
                args.repository_uri,
                str(destination),
            ),
            timeout_seconds=args.timeout_seconds,
            env=env,
            context=f"git clone job {args.job_id}",
        )
    elif args.phase == "git-checkout":
        destination = job_destination(Path(args.work_root), args.job_id)
        run_checked(
            (
                WOS_GIT,
                *GIT_FIXED_CONFIG,
                "-C",
                str(destination),
                "checkout",
                "--quiet",
                "--detach",
                args.commit,
            ),
            timeout_seconds=args.timeout_seconds,
            env=env,
            context=f"git checkout job {args.job_id}",
        )
    elif args.phase == "python-sha256":
        result["digest"] = sha256_job(args.job_id, args.rounds)
        result["rounds"] = args.rounds
    elif args.phase == "python-json":
        result["digest"] = json_job(args.job_id, args.rounds)
        result["rounds"] = args.rounds
    else:
        raise WorkloadError(f"unknown job phase: {args.phase}")
    return result


def inner_job_command(args: argparse.Namespace, job_id: int) -> list[str]:
    command = [
        WOS_PYTHON,
        WOS_HELPER,
        "job",
        "--phase",
        args.phase,
        "--target-host",
        args.target_host,
        "--launcher-host",
        args.launcher_host,
        "--job-id",
        str(job_id),
        "--timeout-seconds",
        str(child_timeout_seconds(args.timeout_seconds)),
    ]
    command += ["--work-root", args.work_root]
    if args.phase == "file-move":
        command += ["--file-bytes", str(args.file_bytes)]
    if args.phase == "git-clone":
        command += ["--repository-uri", args.repository_uri]
    if args.phase == "git-checkout":
        command += ["--commit", args.commit]
    if args.phase in ("python-sha256", "python-json"):
        command += ["--rounds", str(args.rounds)]
    return command


def run_host_worker(args: argparse.Namespace) -> dict[str, Any]:
    os.chdir(args.work_root)
    identity = read_proc_identity()
    validate_identity(identity, args.target_host, args.launcher_host)
    processes: list[tuple[str, subprocess.Popen[str]]] = []
    for job_id in range(args.job_start, args.job_start + args.job_count):
        command = inner_job_command(args, job_id)
        processes.append(
            (
                f"{args.phase} job {job_id}",
                spawn(command, env=deterministic_environment()),
            )
        )
    outputs = communicate_many(processes, args.timeout_seconds)
    jobs: list[dict[str, Any]] = []
    for context, stdout, _stderr in outputs:
        job = parse_json_object(stdout.strip(), context)
        jobs.append(job)
    return {
        "phase": args.phase,
        "target_host": args.target_host,
        "job_start": args.job_start,
        "job_count": args.job_count,
        **identity,
        **inherited_route_evidence(args.phase, args.work_root),
        "jobs": jobs,
    }


def local_route_operands() -> list[str]:
    return [f"-{path}" for path in LOCAL_ROUTE_PATHS]


def controller_command(
    phase: str,
    host: str,
    launcher: str,
    start: int,
    count: int,
    work_root: Path,
    fixture: dict[str, Any],
    rounds: int,
    timeout_seconds: float,
    file_bytes: int = 0,
) -> list[str]:
    command = [WOS_FORWARD]
    if phase_uses_host_workspace(phase):
        command.append(f"+{work_root}")
    command += [
        *local_route_operands(),
        "--",
        WOS_ON,
        host,
        WOS_PYTHON,
        WOS_HELPER,
        "host-worker",
    ]
    command += [
        "--phase",
        phase,
        "--target-host",
        host,
        "--launcher-host",
        launcher,
        "--job-start",
        str(start),
        "--job-count",
        str(count),
        "--timeout-seconds",
        str(child_timeout_seconds(timeout_seconds)),
    ]
    command += ["--work-root", str(work_root)]
    if phase == "file-move":
        command += ["--file-bytes", str(file_bytes)]
    if phase == "git-clone":
        command += ["--repository-uri", str(fixture["repository_uri"])]
    if phase == "git-checkout":
        command += ["--commit", str(fixture["commit"])]
    if phase in ("python-sha256", "python-json"):
        command += ["--rounds", str(rounds)]
    return command


def preflight_command(
    profile: str,
    host: str,
    launcher: str,
    work_root: Path,
    work_root_owner: str,
    timeout_seconds: float,
) -> list[str]:
    command = [WOS_FORWARD]
    if profile == "host-workspace":
        command.append(f"+{work_root}")
    command += [
        *local_route_operands(),
        "--",
        WOS_ON,
        host,
        WOS_PYTHON,
        WOS_HELPER,
        "preflight-host",
    ]
    command += [
        "--profile",
        profile,
        "--target-host",
        host,
        "--launcher-host",
        launcher,
        "--work-root",
        str(work_root),
        "--work-root-owner",
        work_root_owner,
        "--timeout-seconds",
        str(child_timeout_seconds(timeout_seconds)),
    ]
    return command


def run_route_preflight(
    hosts: Sequence[str],
    launcher: str,
    work_root: Path,
    work_root_owner: str,
    timeout_seconds: float,
) -> dict[str, str]:
    expected_provenance: dict[str, str] | None = None
    for profile in ("local-runtime", "host-workspace"):
        processes: list[tuple[str, subprocess.Popen[str]]] = []
        for host in hosts:
            command = preflight_command(
                profile,
                host,
                launcher,
                work_root,
                work_root_owner,
                timeout_seconds,
            )
            processes.append(
                (
                    f"{profile} route preflight {host}",
                    spawn(command, env=deterministic_environment()),
                )
            )
        outputs = communicate_many(processes, timeout_seconds)
        for host, output in zip(hosts, outputs, strict=True):
            context, stdout, _stderr = output
            evidence = parse_json_object(stdout.strip(), context)
            validate_identity(evidence, host, launcher)
            provenance = evidence.get("runtime_provenance")
            expected_workspace_route = "host" if profile == "host-workspace" else None
            expected_workspace_path = (
                str(work_root) if expected_workspace_route == "host" else None
            )
            if (
                evidence.get("profile") != profile
                or evidence.get("target_host") != host
                or evidence.get("runtime_route") != "local"
                or evidence.get("runtime_paths") != list(LOCAL_ROUTE_PATHS)
                or evidence.get("workspace_route") != expected_workspace_route
                or evidence.get("workspace_path") != expected_workspace_path
                or evidence.get("work_root_ready") is not True
                or not isinstance(provenance, dict)
                or set(provenance) != RUNTIME_PROVENANCE_FIELDS
                or any(
                    SHA256_RE.fullmatch(str(value)) is None
                    for value in provenance.values()
                )
            ):
                raise WorkloadError(
                    f"{profile} preflight on {host} returned invalid route evidence"
                )
            normalized_provenance = {
                str(field): str(value) for field, value in provenance.items()
            }
            if expected_provenance is None:
                expected_provenance = normalized_provenance
            elif normalized_provenance != expected_provenance:
                raise WorkloadError(
                    f"{profile} preflight on {host} found mixed node-local runtimes"
                )
    if expected_provenance is None:
        raise WorkloadError("route preflight returned no runtime provenance")
    return expected_provenance


def run_timed_phase(
    phase: str,
    partitions: Sequence[tuple[str, int, int]],
    launcher: str,
    work_root: Path,
    fixture: dict[str, Any],
    rounds: int,
    timeout_seconds: float,
    file_bytes: int = 0,
) -> tuple[float, list[dict[str, Any]]]:
    processes: list[tuple[str, subprocess.Popen[str]]] = []
    started_ns = time.monotonic_ns()
    for host, start, count in partitions:
        command = controller_command(
            phase,
            host,
            launcher,
            start,
            count,
            work_root,
            fixture,
            rounds,
            timeout_seconds,
            file_bytes,
        )
        processes.append(
            (
                f"{phase} controller {host}",
                spawn(command, env=deterministic_environment()),
            )
        )
    outputs = communicate_many(processes, timeout_seconds)
    finished_ns = time.monotonic_ns()
    elapsed = (finished_ns - started_ns) / 1_000_000_000.0
    if elapsed <= 0.0:
        raise WorkloadError(f"{phase} produced a non-positive monotonic duration")
    controllers = [
        parse_json_object(stdout.strip(), context) for context, stdout, _ in outputs
    ]
    return elapsed, controllers


def validate_controller_results(
    phase: str,
    controllers: Sequence[dict[str, Any]],
    partitions: Sequence[tuple[str, int, int]],
    launcher: str,
    work_root: Path,
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    if len(controllers) != len(partitions):
        raise WorkloadError(f"{phase} did not return one controller per host")
    jobs_by_id: dict[int, dict[str, Any]] = {}
    participants: list[dict[str, Any]] = []
    for controller, (host, start, count) in zip(controllers, partitions, strict=True):
        if controller.get("phase") != phase or controller.get("target_host") != host:
            raise WorkloadError(
                f"{phase} controller ordering/target evidence is invalid"
            )
        validate_identity(controller, host, launcher)
        if controller.get("runtime_route") != "local" or controller.get(
            "runtime_paths"
        ) != list(LOCAL_ROUTE_PATHS):
            raise WorkloadError(
                f"{phase} controller on {host} lacks LOCAL runtime evidence"
            )
        expected_workspace_route = "host" if phase_uses_host_workspace(phase) else None
        if controller.get("workspace_route") != expected_workspace_route:
            raise WorkloadError(
                f"{phase} controller on {host} has invalid workspace route evidence"
            )
        expected_workspace_path = (
            str(work_root) if expected_workspace_route == "host" else None
        )
        if (
            "workspace_path" not in controller
            or controller.get("workspace_path") != expected_workspace_path
        ):
            raise WorkloadError(
                f"{phase} controller on {host} routed the wrong workspace"
            )
        raw_jobs = controller.get("jobs")
        if not isinstance(raw_jobs, list) or len(raw_jobs) != count:
            raise WorkloadError(
                f"{phase} controller on {host} returned incomplete work"
            )
        expected_ids = list(range(start, start + count))
        observed_ids: list[int] = []
        job_remote_pids: list[int] = []
        for job in raw_jobs:
            if not isinstance(job, dict) or job.get("phase") != phase:
                raise WorkloadError(
                    f"{phase} controller on {host} returned an invalid job record"
                )
            job_id = job.get("job_id")
            if (
                isinstance(job_id, bool)
                or not isinstance(job_id, int)
                or job_id in jobs_by_id
            ):
                raise WorkloadError(f"{phase} returned a duplicate or invalid job id")
            validate_identity(job, host, launcher)
            if (
                job.get("runtime_route") != "local"
                or job.get("runtime_paths") != list(LOCAL_ROUTE_PATHS)
                or job.get("workspace_route") != expected_workspace_route
            ):
                raise WorkloadError(f"{phase} job {job_id} has invalid route evidence")
            if (
                "workspace_path" not in job
                or job.get("workspace_path") != expected_workspace_path
            ):
                raise WorkloadError(f"{phase} job {job_id} routed the wrong workspace")
            observed_ids.append(job_id)
            jobs_by_id[job_id] = job
            job_remote_pids.append(int(job["remote_pid"]))
        if sorted(observed_ids) != expected_ids:
            raise WorkloadError(
                f"{phase} controller on {host} did not execute its assigned job range"
            )
        transport = (
            "local" if normalize_host(host) == normalize_host(launcher) else "wki"
        )
        controller_remote_pid = int(controller["remote_pid"])
        if transport == "local":
            if controller_remote_pid != 0 or any(job_remote_pids):
                raise WorkloadError(f"{phase} local PID evidence is inconsistent")
        elif (
            controller_remote_pid <= 0
            or any(remote_pid <= 0 for remote_pid in job_remote_pids)
            or len(set(job_remote_pids)) != len(job_remote_pids)
            or controller_remote_pid in job_remote_pids
        ):
            raise WorkloadError(f"{phase} remote PID evidence is inconsistent")
        participant = {
            "host": host,
            "runner_host": controller["runner_host"],
            "launcher_host": controller["launcher_host"],
            "remote_pid": controller_remote_pid,
            "job_remote_pids": job_remote_pids,
            "strict_target": True,
            "transport": transport,
            "work_units": count,
            "completed_work_units": count,
            "job_ids": expected_ids,
            "runtime_route": "local",
            "runtime_paths": list(LOCAL_ROUTE_PATHS),
            "workspace_route": expected_workspace_route,
            "workspace_path": expected_workspace_path,
        }
        if phase == "file-move":
            byte_counts = [job.get("bytes_moved") for job in raw_jobs]
            if any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in byte_counts
            ):
                raise WorkloadError(
                    f"{phase} controller on {host} returned invalid byte evidence"
                )
            participant["bytes_moved"] = sum(byte_counts)
        participants.append(participant)
    if sorted(jobs_by_id) != list(range(TOTAL_JOBS)):
        raise WorkloadError(f"{phase} did not cover exactly 32 unique jobs")
    return participants, jobs_by_id


def validate_clone_outputs(
    work_root: Path,
    fixture: dict[str, Any],
    git: str,
    timeout_seconds: float,
) -> dict[int, str]:
    digests: dict[int, str] = {}
    for job_id in range(TOTAL_JOBS):
        destination = job_destination(work_root, job_id)
        if not destination.is_dir() or not (destination / ".git").is_dir():
            raise WorkloadError(
                f"clone job {job_id} is absent from the launcher workspace"
            )
        visible = sorted(
            path.name for path in destination.iterdir() if path.name != ".git"
        )
        if visible:
            raise WorkloadError(
                f"clone job {job_id} materialized a worktree during --no-checkout"
            )
        commit = git_output(
            git,
            ("-C", str(destination), "rev-parse", "HEAD^{commit}"),
            timeout_seconds=timeout_seconds,
            context=f"validate clone commit {job_id}",
        )
        tree_oid = git_output(
            git,
            ("-C", str(destination), "rev-parse", "HEAD^{tree}"),
            timeout_seconds=timeout_seconds,
            context=f"validate clone tree {job_id}",
        )
        if commit != fixture["commit"] or tree_oid != fixture["tree_oid"]:
            raise WorkloadError(f"clone job {job_id} has the wrong commit/tree")
        digests[job_id] = hashlib.sha256(
            f"{commit}\0{tree_oid}".encode("ascii")
        ).hexdigest()
    return digests


def validate_checkout_outputs(
    work_root: Path,
    fixture: dict[str, Any],
    git: str,
    timeout_seconds: float,
) -> dict[int, str]:
    digests: dict[int, str] = {}
    for job_id in range(TOTAL_JOBS):
        destination = job_destination(work_root, job_id)
        commit = git_output(
            git,
            ("-C", str(destination), "rev-parse", "HEAD^{commit}"),
            timeout_seconds=timeout_seconds,
            context=f"validate checkout commit {job_id}",
        )
        status = git_output(
            git,
            ("-C", str(destination), "status", "--porcelain", "--untracked-files=all"),
            timeout_seconds=timeout_seconds,
            context=f"validate checkout status {job_id}",
        )
        file_count, byte_count, manifest_digest = tree_manifest(destination)
        if (
            commit != fixture["commit"]
            or status
            or file_count != fixture["checkout_files"]
            or byte_count != fixture["fixture_bytes"]
            or manifest_digest != fixture["fixture_digest"]
        ):
            raise WorkloadError(
                f"checkout job {job_id} failed commit/content validation"
            )
        digests[job_id] = manifest_digest
    return digests


def validate_file_move_outputs(
    fixture: dict[str, Any], total_jobs: int = TOTAL_JOBS
) -> dict[int, str]:
    source_path = Path(fixture["source_path"])
    destination_path = Path(fixture["destination_path"])
    bytes_per_file = int(fixture["bytes_per_file"])
    source_digest = str(fixture["source_digest"])
    expected_names = {f"file-{job_id:03d}.bin" for job_id in range(total_jobs)}
    try:
        if (
            source_path.is_symlink()
            or not source_path.is_file()
            or source_path.stat().st_size != bytes_per_file
            or sha256_file(source_path) != source_digest
        ):
            raise WorkloadError("file-move source changed during the timed phase")
        observed_names = {path.name for path in destination_path.iterdir()}
    except WorkloadError:
        raise
    except OSError as exc:
        raise WorkloadError(f"cannot inspect file-move outputs: {exc}") from exc
    if observed_names != expected_names:
        raise WorkloadError("file-move destination set is incomplete or contains extras")

    digests: dict[int, str] = {}
    for job_id in range(total_jobs):
        path = destination_path / f"file-{job_id:03d}.bin"
        try:
            invalid = (
                path.is_symlink()
                or not path.is_file()
                or path.stat().st_size != bytes_per_file
            )
        except OSError as exc:
            raise WorkloadError(f"cannot stat file-move output {job_id}: {exc}") from exc
        if invalid:
            raise WorkloadError(f"file-move output {job_id} has the wrong shape")
        digest = sha256_file(path)
        if digest != source_digest:
            raise WorkloadError(f"file-move output {job_id} failed content validation")
        digests[job_id] = digest
    return digests


def attach_participant_digests(
    participants: list[dict[str, Any]], job_digests: dict[int, str]
) -> None:
    for participant in participants:
        selected = [job_digests[job_id] for job_id in participant["job_ids"]]
        digest = hashlib.sha256()
        for job_id, job_digest in zip(participant["job_ids"], selected, strict=True):
            digest.update(job_id.to_bytes(4, "big"))
            digest.update(bytes.fromhex(job_digest))
        participant["job_digests"] = selected
        participant["digest"] = digest.hexdigest()


def common_measurement(
    benchmark: str,
    elapsed_seconds: float,
    scale: str,
    hosts: Sequence[str],
    launcher: str,
    route: str,
    route_path: str | None,
    participants: list[dict[str, Any]],
    provenance: dict[str, str],
) -> dict[str, Any]:
    return {
        "benchmark": benchmark,
        "evidence_contract": EVIDENCE_CONTRACT,
        "elapsed_seconds": elapsed_seconds,
        "scale": scale,
        "placement": "local-baseline" if len(hosts) == 1 else "strict-on",
        "wki_route": route,
        "route_path": route_path,
        "launcher_host": launcher,
        "total_work_units": TOTAL_JOBS,
        "runtime_provenance": provenance,
        "participants": participants,
    }


def require_safe_work_root(path: Path, owner: str | None = None) -> None:
    suffix = path.name.removeprefix("wos-showcase-fixed-")
    if (
        path.parent != Path("/tmp")
        or len(suffix) != 16
        or any(character not in "0123456789abcdef" for character in suffix)
    ):
        raise WorkloadError(f"unsafe fixed-resource work root: {path}")
    if owner is not None and (
        len(owner) != 32
        or any(character not in "0123456789abcdef" for character in owner)
        or owner[:16] != suffix
    ):
        raise WorkloadError(f"invalid fixed-resource ownership token for {path}")


def verify_work_root_owner(path: Path, owner: str) -> None:
    require_safe_work_root(path, owner)
    owner_path = path / ".wos-showcase-owner"
    if not path.is_dir() or path.is_symlink():
        raise WorkloadError(
            f"fixed-resource work root is not a private directory: {path}"
        )
    try:
        observed_owner = owner_path.read_text(encoding="ascii")
    except OSError as exc:
        raise WorkloadError(f"cannot verify work-root ownership: {path}") from exc
    if observed_owner != owner:
        raise WorkloadError(f"fixed-resource work root has changed ownership: {path}")


def prepare_work_root(path: Path, owner: str) -> None:
    require_safe_work_root(path, owner)
    if path.exists() or path.is_symlink():
        verify_work_root_owner(path, owner)
        return
    owner_path = path / ".wos-showcase-owner"
    created = False
    try:
        path.mkdir(mode=0o700)
        created = True
        owner_path.write_text(owner, encoding="ascii")
    except OSError as exc:
        if created:
            try:
                owner_path.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                path.rmdir()
            except OSError:
                pass
        raise WorkloadError(
            f"cannot prepare private fixed-resource work root: {path}"
        ) from exc


def create_work_root() -> tuple[Path, str]:
    for _attempt in range(100):
        owner = secrets.token_hex(16)
        path = Path("/tmp") / f"wos-showcase-fixed-{owner[:16]}"
        if path.exists() or path.is_symlink():
            continue
        prepare_work_root(path, owner)
        return path, owner
    raise WorkloadError("cannot allocate a private fixed-resource work root")


def remove_work_root(path: Path, owner: str) -> None:
    verify_work_root_owner(path, owner)
    shutil.rmtree(path)


def run_preflight_host(args: argparse.Namespace) -> dict[str, Any]:
    identity = read_proc_identity()
    validate_identity(identity, args.target_host, args.launcher_host)
    work_root = Path(args.work_root)
    if args.profile == "local-runtime":
        prepare_work_root(work_root, args.work_root_owner)
        routed_work_root: str | None = None
    else:
        verify_work_root_owner(work_root, args.work_root_owner)
        routed_work_root = str(work_root)
    os.chdir(work_root)
    routes = verify_routes(routed_work_root, args.timeout_seconds)
    return {
        "profile": args.profile,
        "target_host": args.target_host,
        "work_root_ready": True,
        "runtime_provenance": runtime_provenance(),
        **identity,
        **routes,
    }


def cleanup_host_command(
    host: str,
    launcher: str,
    work_root: Path,
    work_root_owner: str,
    timeout_seconds: float,
) -> list[str]:
    return [
        WOS_FORWARD,
        *local_route_operands(),
        "--",
        WOS_ON,
        host,
        WOS_PYTHON,
        WOS_HELPER,
        "cleanup-host",
        "--target-host",
        host,
        "--launcher-host",
        launcher,
        "--work-root",
        str(work_root),
        "--work-root-owner",
        work_root_owner,
        "--timeout-seconds",
        str(timeout_seconds),
    ]


def cleanup_remote_work_roots(
    hosts: Sequence[str],
    launcher: str,
    work_root: Path,
    work_root_owner: str,
    timeout_seconds: float,
) -> None:
    processes: list[tuple[str, subprocess.Popen[str]]] = []
    for host in hosts:
        if normalize_host(host) == normalize_host(launcher):
            continue
        processes.append(
            (
                f"cleanup fixed-resource work root on {host}",
                spawn(
                    cleanup_host_command(
                        host,
                        launcher,
                        work_root,
                        work_root_owner,
                        timeout_seconds,
                    ),
                    env=deterministic_environment(),
                ),
            )
        )
    if processes:
        communicate_many(processes, timeout_seconds)


def run_cleanup_host(args: argparse.Namespace) -> dict[str, Any]:
    identity = read_proc_identity()
    validate_identity(identity, args.target_host, args.launcher_host)
    work_root = Path(args.work_root)
    os.chdir("/")
    require_safe_work_root(work_root, args.work_root_owner)
    if not work_root.exists() and not work_root.is_symlink():
        return {"removed": False, **identity}
    remove_work_root(work_root, args.work_root_owner)
    return {"removed": True, **identity}


def run_coordinator(args: argparse.Namespace) -> list[dict[str, Any]]:
    coordinator_identity = read_proc_identity()
    launcher = str(coordinator_identity["runner_host"])
    validate_identity(coordinator_identity, launcher, launcher)
    hosts = parse_hosts(args.hosts, launcher)
    partitions = partition_jobs(sorted(hosts, key=normalize_host))
    expected_counts = [count for _, _, count in partitions]
    if expected_counts not in ([32], [16, 16], [11, 11, 10], [8, 8, 8, 8]):
        raise WorkloadError(f"unexpected fixed-resource partition: {expected_counts}")
    rounds = ROUND_COUNTS[args.scale]
    work_root, work_root_owner = create_work_root()
    original_cwd = Path.cwd()
    os.chdir(work_root)
    measurements: list[dict[str, Any]] = []
    try:
        log("validating node-local runtime and HOST workspace routes before timing")
        provenance = run_route_preflight(
            hosts,
            launcher,
            work_root,
            work_root_owner,
            args.timeout_seconds,
        )
        (work_root / "clones").mkdir()
        log(
            f"hosts={','.join(hosts)} partition={expected_counts} total_jobs={TOTAL_JOBS}"
        )

        log("preparing deterministic offline Git fixture outside the timed phases")
        fixture = create_git_fixture(work_root, WOS_GIT, args.timeout_seconds)

        log("running 32 concurrent no-checkout Git clone jobs")
        elapsed, controllers = run_timed_phase(
            "git-clone",
            partitions,
            launcher,
            work_root,
            fixture,
            rounds,
            args.timeout_seconds,
        )
        clone_participants, _ = validate_controller_results(
            "git-clone", controllers, partitions, launcher, work_root
        )
        clone_digests = validate_clone_outputs(
            work_root, fixture, WOS_GIT, args.timeout_seconds
        )
        attach_participant_digests(clone_participants, clone_digests)
        clone_measurement = common_measurement(
            "wos_git_clone",
            elapsed,
            args.scale,
            hosts,
            launcher,
            "host-workspace",
            str(work_root),
            clone_participants,
            provenance,
        )
        clone_measurement.update(
            {
                "repository": fixture["repository"],
                "repository_uri": fixture["repository_uri"],
                "commit": fixture["commit"],
                "tree_oid": fixture["tree_oid"],
                "checkout_files": fixture["checkout_files"],
                "fixture_bytes": fixture["fixture_bytes"],
                "fixture_digest": fixture["fixture_digest"],
                "artifact_digest": aggregate_digests(clone_digests),
                "cache_policy": "warm-source-cold-destination",
            }
        )
        measurements.append(clone_measurement)

        log("running 32 concurrent exact detached Git checkout jobs")
        elapsed, controllers = run_timed_phase(
            "git-checkout",
            partitions,
            launcher,
            work_root,
            fixture,
            rounds,
            args.timeout_seconds,
        )
        checkout_participants, _ = validate_controller_results(
            "git-checkout", controllers, partitions, launcher, work_root
        )
        checkout_digests = validate_checkout_outputs(
            work_root, fixture, WOS_GIT, args.timeout_seconds
        )
        attach_participant_digests(checkout_participants, checkout_digests)
        checkout_measurement = common_measurement(
            "wos_git_checkout",
            elapsed,
            args.scale,
            hosts,
            launcher,
            "host-workspace",
            str(work_root),
            checkout_participants,
            provenance,
        )
        checkout_measurement.update(
            {
                "commit": fixture["commit"],
                "tree_oid": fixture["tree_oid"],
                "checkout_files": fixture["checkout_files"],
                "fixture_bytes": fixture["fixture_bytes"],
                "fixture_digest": fixture["fixture_digest"],
                "artifact_digest": aggregate_digests(checkout_digests),
                "cache_policy": "post-clone-object-cache",
            }
        )
        measurements.append(checkout_measurement)

        for phase, benchmark, count_field in (
            ("python-sha256", "wos_python_sha256", "jobs"),
            ("python-json", "wos_python_json", "documents"),
        ):
            log(f"running {TOTAL_JOBS} concurrent {phase} jobs rounds={rounds}")
            elapsed, controllers = run_timed_phase(
                phase,
                partitions,
                launcher,
                work_root,
                fixture,
                rounds,
                args.timeout_seconds,
            )
            participants, jobs = validate_controller_results(
                phase, controllers, partitions, launcher, work_root
            )
            job_digests: dict[int, str] = {}
            for job_id, job in jobs.items():
                digest = job.get("digest")
                if (
                    SHA256_RE.fullmatch(str(digest)) is None
                    or job.get("rounds") != rounds
                ):
                    raise WorkloadError(
                        f"{phase} job {job_id} returned invalid work evidence"
                    )
                job_digests[job_id] = str(digest)
            attach_participant_digests(participants, job_digests)
            measurement = common_measurement(
                benchmark,
                elapsed,
                args.scale,
                hosts,
                launcher,
                "local-runtime",
                None,
                participants,
                provenance,
            )
            aggregate = aggregate_digests(job_digests)
            if aggregate != EXPECTED_PYTHON_DIGESTS[phase][args.scale]:
                raise WorkloadError(
                    f"{phase} aggregate differs from the fixed workload identity"
                )
            measurement.update(
                {
                    count_field: TOTAL_JOBS,
                    "rounds": rounds,
                    "digest": aggregate,
                }
            )
            measurements.append(measurement)

        # Keep this last: the stress profile leaves 1 GiB of destination data
        # and should not perturb the established Git/Python cache conditions.
        log("preparing deterministic warm file-move source outside the timed phase")
        file_move_fixture = create_file_move_fixture(work_root, args.scale)
        file_bytes = int(file_move_fixture["bytes_per_file"])
        source_digest = str(file_move_fixture["source_digest"])
        log(
            f"running {TOTAL_JOBS} concurrent HOST-workspace file copies "
            f"bytes_per_file={file_bytes}"
        )
        elapsed, controllers = run_timed_phase(
            "file-move",
            partitions,
            launcher,
            work_root,
            file_move_fixture,
            rounds,
            args.timeout_seconds,
            file_bytes,
        )
        file_move_participants, file_move_jobs = validate_controller_results(
            "file-move", controllers, partitions, launcher, work_root
        )
        for job_id, job in file_move_jobs.items():
            expected_destination = (
                f"{FILE_MOVE_DESTINATION_RELATIVE_PATH}/file-{job_id:03d}.bin"
            )
            if (
                job.get("bytes_moved") != file_bytes
                or job.get("chunk_bytes") != FILE_MOVE_CHUNK_BYTES
                or job.get("source_relative_path")
                != FILE_MOVE_SOURCE_RELATIVE_PATH
                or job.get("destination_relative_path") != expected_destination
            ):
                raise WorkloadError(
                    f"file-move job {job_id} returned invalid copy evidence"
                )
        if any(
            participant.get("bytes_moved")
            != participant["work_units"] * file_bytes
            for participant in file_move_participants
        ):
            raise WorkloadError("file-move participant byte evidence is inconsistent")
        file_move_digests = validate_file_move_outputs(file_move_fixture)
        attach_participant_digests(file_move_participants, file_move_digests)
        file_move_artifact_digest = aggregate_digests(file_move_digests)
        if file_move_artifact_digest != FILE_MOVE_ARTIFACT_DIGESTS[args.scale]:
            raise WorkloadError("file-move artifact identity changed")
        file_move_measurement = common_measurement(
            "wos_file_move",
            elapsed,
            args.scale,
            hosts,
            launcher,
            "host-workspace",
            str(work_root),
            file_move_participants,
            provenance,
        )
        file_move_measurement.update(
            {
                "workload_id": FILE_MOVE_WORKLOAD_ID,
                "operation": FILE_MOVE_OPERATION,
                "files": TOTAL_JOBS,
                "bytes_per_file": file_bytes,
                "total_bytes": TOTAL_JOBS * file_bytes,
                "chunk_bytes": FILE_MOVE_CHUNK_BYTES,
                "source_relative_path": FILE_MOVE_SOURCE_RELATIVE_PATH,
                "destination_relative_path": FILE_MOVE_DESTINATION_RELATIVE_PATH,
                "source_digest": source_digest,
                "artifact_digest": file_move_artifact_digest,
                "cache_policy": FILE_MOVE_CACHE_POLICY,
            }
        )
        measurements.append(file_move_measurement)
    finally:
        if not terminate_processes():
            os.chdir(original_cwd)
            raise WorkloadError(
                f"worker processes did not stop; preserving private workspace {work_root}"
            )
        os.chdir(original_cwd)
        cleanup_remote_work_roots(
            hosts,
            launcher,
            work_root,
            work_root_owner,
            args.timeout_seconds,
        )
        remove_work_root(work_root, work_root_owner)
    return measurements


def self_test(args: argparse.Namespace) -> None:
    expected = {
        1: [32],
        2: [16, 16],
        3: [11, 11, 10],
        4: [8, 8, 8, 8],
    }
    for count, expected_counts in expected.items():
        hosts = [f"wos-{index}.wos" for index in range(count)]
        actual = [work for _, _, work in partition_jobs(hosts)]
        if actual != expected_counts:
            raise WorkloadError(f"partition self-test failed for {count} hosts")

    if parse_hosts("wos-0.wos,wos-1.wos,wos-2.wos", "wos-0") != [
        "wos-0.wos",
        "wos-1.wos",
        "wos-2.wos",
    ]:
        raise WorkloadError("valid fixed-resource host list was not preserved")
    for invalid_hosts in (
        "",
        "wos-0,",
        "wos-0,wos-0.wos",
        "wos-0;reboot",
        "../wos-0",
        "wos-1,wos-0",
    ):
        try:
            parse_hosts(invalid_hosts, "wos-0")
        except WorkloadError:
            pass
        else:
            raise WorkloadError(f"unsafe host list was accepted: {invalid_hosts!r}")

    sha_jobs = {job_id: sha256_job(job_id, 3) for job_id in range(TOTAL_JOBS)}
    json_jobs = {job_id: json_job(job_id, 3) for job_id in range(TOTAL_JOBS)}
    if aggregate_digests(sha_jobs) != (
        "dbf88aa86f1f02a94286690c042a5090882a403e9c7ac85ad7f0b27bfffba3ab"
    ):
        raise WorkloadError("SHA-256 workload kernel digest changed")
    if aggregate_digests(json_jobs) != (
        "b36e5f57c0d1c3612803de8a0443de108e612f7afaa811667fda5a940f9927c9"
    ):
        raise WorkloadError("JSON workload kernel digest changed")
    if aggregate_digests(dict(reversed(list(sha_jobs.items())))) != aggregate_digests(
        sha_jobs
    ):
        raise WorkloadError("SHA-256 aggregation is order-dependent")
    if aggregate_digests(dict(reversed(list(json_jobs.items())))) != aggregate_digests(
        json_jobs
    ):
        raise WorkloadError("JSON aggregation is order-dependent")

    for phase, kernel in (
        ("python-sha256", sha256_job),
        ("python-json", json_job),
    ):
        quick_jobs = {
            job_id: kernel(job_id, ROUND_COUNTS["quick"])
            for job_id in range(TOTAL_JOBS)
        }
        if aggregate_digests(quick_jobs) != EXPECTED_PYTHON_DIGESTS[phase]["quick"]:
            raise WorkloadError(f"{phase} quick-scale workload identity changed")

    for scale, file_bytes in FILE_MOVE_BYTES.items():
        digest = hashlib.sha256()
        generated = 0
        for chunk in deterministic_file_move_chunks(file_bytes):
            digest.update(chunk)
            generated += len(chunk)
        source_digest = digest.hexdigest()
        if generated != file_bytes or source_digest != FILE_MOVE_SOURCE_DIGESTS[scale]:
            raise WorkloadError(f"file-move {scale} source identity changed")
        job_digests = {job_id: source_digest for job_id in range(TOTAL_JOBS)}
        if aggregate_digests(job_digests) != FILE_MOVE_ARTIFACT_DIGESTS[scale]:
            raise WorkloadError(f"file-move {scale} artifact identity changed")

    git = args.git or shutil.which("git")
    if not git:
        raise WorkloadError("self-test requires Git")
    with tempfile.TemporaryDirectory(
        prefix="wos-fixed-selftest-", dir="/tmp"
    ) as temporary:
        root = Path(temporary)

        small_source = root / FILE_MOVE_SOURCE_RELATIVE_PATH
        small_destinations = root / FILE_MOVE_DESTINATION_RELATIVE_PATH
        small_source.parent.mkdir()
        small_destinations.mkdir()
        small_payload = bytes(range(256)) * 17
        small_source.write_bytes(small_payload)
        small_digest = hashlib.sha256(small_payload).hexdigest()
        small_fixture = {
            "source_path": small_source,
            "destination_path": small_destinations,
            "bytes_per_file": len(small_payload),
            "source_digest": small_digest,
        }
        for job_id in range(TOTAL_JOBS):
            copied = copy_file(
                small_source, file_move_destination(root, job_id), len(small_payload)
            )
            if copied != len(small_payload):
                raise WorkloadError("small file-move copy self-test failed")
        small_digests = validate_file_move_outputs(small_fixture)
        if any(digest != small_digest for digest in small_digests.values()):
            raise WorkloadError("small file-move validation self-test failed")
        try:
            copy_file(
                small_source,
                file_move_destination(root, 0),
                len(small_payload),
            )
        except WorkloadError:
            pass
        else:
            raise WorkloadError("file-move exclusive destination check failed")
        file_move_destination(root, 0).write_bytes(small_payload + b"corrupt")
        try:
            validate_file_move_outputs(small_fixture)
        except WorkloadError:
            pass
        else:
            raise WorkloadError("file-move corruption self-test was not detected")

        first_root = root / "first"
        second_root = root / "second"
        first_root.mkdir()
        second_root.mkdir()
        first = create_git_fixture(first_root, git, args.timeout_seconds)
        second = create_git_fixture(second_root, git, args.timeout_seconds)
        stable_fields = (
            "commit",
            "tree_oid",
            "checkout_files",
            "fixture_bytes",
            "fixture_digest",
        )
        if any(first[field] != second[field] for field in stable_fields):
            raise WorkloadError("Git fixture identities are not deterministic")
        if (
            first["commit"] != FIXTURE_COMMIT
            or first["tree_oid"] != FIXTURE_TREE
            or first["fixture_digest"] != FIXTURE_DIGEST
        ):
            raise WorkloadError("Git fixture differs from the fixed identity")

        destination = root / "local-clone"
        run_checked(
            (
                git,
                *GIT_FIXED_CONFIG,
                "-c",
                "protocol.file.allow=always",
                "clone",
                "--quiet",
                "--no-local",
                "--no-checkout",
                "--no-tags",
                "--single-branch",
                "--branch",
                "main",
                str(first["repository_uri"]),
                str(destination),
            ),
            timeout_seconds=args.timeout_seconds,
            env=deterministic_environment(),
            context="self-test clone",
        )
        if sorted(path.name for path in destination.iterdir() if path.name != ".git"):
            raise WorkloadError("self-test no-checkout clone materialized files")
        git_output(
            git,
            (
                "-C",
                str(destination),
                "checkout",
                "--quiet",
                "--detach",
                str(first["commit"]),
            ),
            timeout_seconds=args.timeout_seconds,
            context="self-test checkout",
        )
        file_count, byte_count, digest = tree_manifest(destination)
        if (file_count, byte_count, digest) != (
            first["checkout_files"],
            first["fixture_bytes"],
            first["fixture_digest"],
        ):
            raise WorkloadError("self-test checkout content differs from the fixture")
    print("fixed-resource workload self-test passed")


def positive_timeout(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected a positive timeout") from exc
    if not 0.0 < parsed <= 86_400.0:
        raise argparse.ArgumentTypeError("timeout must be in (0, 86400]")
    return parsed


def positive_int(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    coordinator = subparsers.add_parser("coordinator")
    coordinator.add_argument("--hosts", required=True)
    coordinator.add_argument("--scale", choices=tuple(ROUND_COUNTS), required=True)
    coordinator.add_argument("--output-root", required=True)
    coordinator.add_argument("--timeout-seconds", type=positive_timeout, default=1800.0)

    host_worker = subparsers.add_parser("host-worker")
    host_worker.add_argument(
        "--phase",
        choices=WORKLOAD_PHASES,
        required=True,
    )
    host_worker.add_argument("--target-host", required=True)
    host_worker.add_argument("--launcher-host", required=True)
    host_worker.add_argument("--job-start", type=int, required=True)
    host_worker.add_argument("--job-count", type=positive_int, required=True)
    host_worker.add_argument("--work-root", default="")
    host_worker.add_argument("--repository-uri", default="")
    host_worker.add_argument("--commit", default="")
    host_worker.add_argument("--rounds", type=positive_int, default=1)
    host_worker.add_argument("--file-bytes", type=positive_int, default=0)
    host_worker.add_argument("--timeout-seconds", type=positive_timeout, default=1800.0)

    job = subparsers.add_parser("job")
    job.add_argument(
        "--phase",
        choices=WORKLOAD_PHASES,
        required=True,
    )
    job.add_argument("--target-host", required=True)
    job.add_argument("--launcher-host", required=True)
    job.add_argument("--job-id", type=int, required=True)
    job.add_argument("--work-root", default="")
    job.add_argument("--repository-uri", default="")
    job.add_argument("--commit", default="")
    job.add_argument("--rounds", type=positive_int, default=1)
    job.add_argument("--file-bytes", type=positive_int, default=0)
    job.add_argument("--timeout-seconds", type=positive_timeout, default=1800.0)

    preflight_host = subparsers.add_parser("preflight-host")
    preflight_host.add_argument(
        "--profile", choices=("local-runtime", "host-workspace"), required=True
    )
    preflight_host.add_argument("--target-host", required=True)
    preflight_host.add_argument("--launcher-host", required=True)
    preflight_host.add_argument("--work-root", required=True)
    preflight_host.add_argument("--work-root-owner", required=True)
    preflight_host.add_argument(
        "--timeout-seconds", type=positive_timeout, default=1800.0
    )

    cleanup_host = subparsers.add_parser("cleanup-host")
    cleanup_host.add_argument("--target-host", required=True)
    cleanup_host.add_argument("--launcher-host", required=True)
    cleanup_host.add_argument("--work-root", required=True)
    cleanup_host.add_argument("--work-root-owner", required=True)
    cleanup_host.add_argument(
        "--timeout-seconds", type=positive_timeout, default=1800.0
    )

    selftest = subparsers.add_parser("self-test")
    selftest.add_argument("--git")
    selftest.add_argument("--timeout-seconds", type=positive_timeout, default=120.0)
    return parser


def validate_worker_arguments(args: argparse.Namespace) -> None:
    if args.mode == "host-worker":
        if (
            args.job_start < 0
            or args.job_count > TOTAL_JOBS
            or args.job_start + args.job_count > TOTAL_JOBS
        ):
            raise WorkloadError("host worker job range is outside 0..31")
    if args.mode == "job" and not 0 <= args.job_id < TOTAL_JOBS:
        raise WorkloadError("job id is outside 0..31")
    if args.mode in ("host-worker", "job"):
        if not args.work_root:
            raise WorkloadError(f"{args.phase} requires --work-root")
        require_safe_work_root(Path(args.work_root))
        if args.phase == "file-move" and args.file_bytes not in FILE_MOVE_BYTES.values():
            raise WorkloadError("file-move requires a scale-fixed --file-bytes value")
        if args.phase == "git-clone" and not args.repository_uri.startswith("file://"):
            raise WorkloadError("Git clone requires an offline file:// repository URI")
        if args.phase == "git-checkout" and OID_RE.fullmatch(args.commit) is None:
            raise WorkloadError("Git checkout requires an exact SHA-1 commit")


def main(argv: Sequence[str] | None = None) -> int:
    install_signal_handlers()
    args = build_parser().parse_args(argv)
    try:
        validate_worker_arguments(args)
        if args.mode == "coordinator":
            measurements = run_coordinator(args)
            for measurement in measurements:
                print(compact_json(measurement), flush=True)
        elif args.mode == "host-worker":
            print(compact_json(run_host_worker(args)), flush=True)
        elif args.mode == "job":
            print(compact_json(run_job(args)), flush=True)
        elif args.mode == "preflight-host":
            print(compact_json(run_preflight_host(args)), flush=True)
        elif args.mode == "cleanup-host":
            print(compact_json(run_cleanup_host(args)), flush=True)
        elif args.mode == "self-test":
            self_test(args)
        else:
            raise WorkloadError(f"unknown mode: {args.mode}")
    except WorkloadError as exc:
        cleanup = "" if terminate_processes() else "; process cleanup incomplete"
        print(f"fixed-resource workload error: {exc}{cleanup}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
