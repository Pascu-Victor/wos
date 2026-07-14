#!/usr/bin/env python3

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SHOWCASE = (
    ROOT / "configs" / "rootfs" / "root" / "wos-showcase" / "40-live-cpp-distributed.sh"
)


def fail(message: str) -> None:
    raise AssertionError(message)


def write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def install_mocks(root: Path) -> tuple[Path, dict[str, str]]:
    bin_dir = root / "bin"
    bin_dir.mkdir()
    compile_log = root / "compile.log"
    event_log = root / "event.log"
    on_log = root / "on.log"
    prewarm_log = root / "prewarm.log"
    workspace_log = root / "workspace.log"
    live_bin = root / "wos-live-demo"

    write_executable(
        bin_dir / "on",
        """#!/bin/sh
set -eu
host=$1
shift
runner=$host
if [ "${MOCK_MISMATCH_HOST:-}" = "$host" ]; then
    runner=mismatch.wos
fi
if [ "${1:-}" = forward ]; then
    route_record=$host
    launch_kind=probe
    for operand in "$@"; do
        route_record="$route_record|$operand"
        [ "$operand" != -- ] || break
    done
    for operand in "$@"; do
        case "$operand" in
            wos-compile-controller) launch_kind=controller ;;
            wos_compile_prewarm_*) launch_kind=prewarm ;;
        esac
    done
    route_record="$route_record|kind=$launch_kind"
    printf '%s\n' "$route_record" >> "$MOCK_ON_LOG"
    if [ "$launch_kind" = controller ] && [ "${MOCK_FAIL_CONTROLLER_HOST:-}" = "$host" ]; then
        printf 'mock controller launch failure on %s\n' "$host" >&2
        exit 42
    fi
fi
MOCK_RUNNER=$runner
export MOCK_RUNNER
exec "$@"
""",
    )
    write_executable(
        bin_dir / "forward",
        """#!/bin/sh
set -eu
MOCK_VFS_RULES=
route_index=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --) shift; break ;;
        +*|-*)
            operand=$1
            path=${operand#?}
            case "$operand" in
                +*) route=host ;;
                -*) route=local ;;
            esac
            if [ "${MOCK_DROP_ROUTE_PATH:-}" != "$path" ]; then
                MOCK_VFS_RULES="${MOCK_VFS_RULES}vfs-task[$route_index]: $path -> $route
"
                route_index=$((route_index + 1))
            fi
            case "$operand" in
                +/tmp/wos-showcase-fixed-????????????????/distributed-compile)
                    work_root=${path%/distributed-compile}
                    marker=$work_root/.wos-showcase-owner
                    [ -d "$work_root" ] && [ ! -L "$work_root" ] || exit 86
                    [ -f "$marker" ] && [ ! -L "$marker" ] || exit 87
                    owner=$(cat "$marker")
                    [ "${#owner}" -eq 32 ] || exit 88
                    printf '%s|%s\n' "$path" "$owner" >> "$MOCK_WORKSPACE_LOG"
                    ;;
            esac
            shift
            ;;
        *) break ;;
    esac
done
export MOCK_VFS_RULES
exec "$@"
""",
    )
    write_executable(
        bin_dir / "locally",
        """#!/bin/sh
set -eu
MOCK_RUNNER=$MOCK_LAUNCHER
export MOCK_RUNNER
exec "$@"
""",
    )
    write_executable(
        bin_dir / "wkictl",
        """#!/bin/sh
set -eu
[ "${1:-}" = vfs ] && [ "${2:-}" = list ] || exit 89
printf '%s' "${MOCK_VFS_RULES:-}"
""",
    )
    write_executable(
        bin_dir / "sha256sum",
        """#!/bin/sh
set -eu
binary=0
if [ "${1:-}" = -b ]; then
    binary=1
    shift
fi
if [ "$#" -eq 1 ] && [ "$1" = "$MOCK_WKICTL_BIN" ] && \
    [ "${MOCK_WKICTL_MISMATCH_HOST:-}" = "${MOCK_RUNNER:-$MOCK_LAUNCHER}" ]; then
    printf '%s  %s\n' \
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff "$1"
    exit 0
fi
if [ "$binary" -eq 1 ]; then
    exec /usr/bin/sha256sum -b "$@"
fi
exec /usr/bin/sha256sum "$@"
""",
    )
    write_executable(
        bin_dir / "wosid",
        """#!/bin/sh
set -eu
printf 'spawner=%s host=%s pid=%s remote_pid=0\n' \
    "$MOCK_LAUNCHER" "${MOCK_RUNNER:-$MOCK_LAUNCHER}" "$$"
""",
    )
    write_executable(
        bin_dir / "fake-cxx",
        """#!/bin/sh
set -eu
if [ "${1:-}" = --version ]; then
    version='fake-cxx version 1.0'
    if [ "${MOCK_COMPILER_MISMATCH_HOST:-}" = "${MOCK_RUNNER:-$MOCK_LAUNCHER}" ]; then
        version='fake-cxx version 2.0'
    fi
    printf '%s\n' "$version"
    exit 0
fi
out=
compile=0
symbol=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -o)
            out=$2
            shift 2
            ;;
        -c)
            compile=1
            shift
            ;;
        -Dmain=*)
            symbol=${1#-Dmain=}
            shift
            ;;
        *) shift ;;
    esac
done
[ -n "$out" ] || exit 90
if [ -n "$symbol" ]; then
    runner=${MOCK_RUNNER:-$MOCK_LAUNCHER}
    case "$symbol" in
        wos_compile_prewarm_*)
            printf 'prewarm %s %s\n' "$runner" "$symbol" >> "$MOCK_EVENT_LOG"
            printf '%s %s\n' "$runner" "$symbol" >> "$MOCK_PREWARM_LOG"
            if [ "${MOCK_FAIL_PREWARM_HOST:-}" = "$runner" ]; then
                exit 24
            fi
            ;;
        *)
            printf 'timed %s %s\n' "$runner" "$symbol" >> "$MOCK_EVENT_LOG"
            printf '%s %s\n' "$runner" "$symbol" >> "$MOCK_COMPILE_LOG"
            if [ "${MOCK_FAIL_SYMBOL:-}" = "$symbol" ]; then
                printf 'fake-cxx forced failure for %s\n' "$symbol" >&2
                exit 23
            fi
            ;;
    esac
    printf 'object %s\n' "$symbol" > "$out"
elif [ "$compile" -eq 1 ]; then
    printf 'driver object\n' > "$out"
elif [ "$out" = "$MOCK_LIVE_BIN" ]; then
    printf '%s\n' '#!/bin/sh' 'case "${1:-}" in' \
        '  monotonic-ns) date +%s%N ;;' \
        '  *) exit 91 ;;' \
        'esac' > "$out"
    chmod 755 "$out"
else
    printf '%s\n' '#!/bin/sh' \
        'printf "wos-distributed-compile-ok units=32\\n"' > "$out"
    chmod 755 "$out"
fi
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "CXX": str(bin_dir / "fake-cxx"),
            "MOCK_COMPILE_LOG": str(compile_log),
            "MOCK_EVENT_LOG": str(event_log),
            "MOCK_LAUNCHER": "wos-0.wos",
            "MOCK_LIVE_BIN": str(live_bin),
            "MOCK_ON_LOG": str(on_log),
            "MOCK_PREWARM_LOG": str(prewarm_log),
            "MOCK_WKICTL_BIN": str(bin_dir / "wkictl"),
            "MOCK_WORKSPACE_LOG": str(workspace_log),
            "WOS_LIVE_DEMO_BIN": str(live_bin),
            "WOS_LIVE_DISTRIBUTED_COMPILE_ONLY": "1",
        }
    )
    return bin_dir, env


def run_showcase(
    root: Path, hosts: list[str], **extra_env: str
) -> subprocess.CompletedProcess[str]:
    _bin_dir, env = install_mocks(root)
    env["WOS_SHOWCASE_HOSTS"] = ",".join(hosts)
    env.update(extra_env)
    return subprocess.run(
        [str(SHOWCASE)],
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )


def metric_from_stdout(stdout: str) -> dict[str, object]:
    metrics = [
        json.loads(line)
        for line in stdout.splitlines()
        if line.startswith('{"benchmark":"wos_distributed_compile"')
    ]
    if len(metrics) != 1:
        fail(f"expected exactly one compile metric, got {metrics!r}\nstdout:\n{stdout}")
    return metrics[0]


def test_fixed_layouts() -> None:
    layouts = {
        1: [32],
        2: [16, 16],
        3: [11, 11, 10],
        4: [8, 8, 8, 8],
    }
    for node_count, expected_work in layouts.items():
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hosts = [f"wos-{index}.wos" for index in range(node_count)]
            result = run_showcase(root, hosts)
            if result.returncode != 0:
                fail(
                    f"{node_count}-node showcase failed with {result.returncode}:\n"
                    f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )

            metric = metric_from_stdout(result.stdout)
            expected_placement = "local-baseline" if node_count == 1 else "strict-on"
            if metric.get("units") != 32 or metric.get("total_workers") != 32:
                fail(f"invalid fixed compile size: {metric}")
            if metric.get("total_work_units") != 32:
                fail(f"invalid total work evidence: {metric}")
            if metric.get("workload_id") != "wos-live-cpp-32-tu-v1":
                fail(f"invalid workload identity: {metric}")
            if (
                metric.get("source_sha256")
                != "aa52bc6a7f7f5b58904b6c1d06fb7f813c8567c97470fbe4161a4e691a60c726"
            ):
                fail(f"invalid source identity: {metric}")
            if metric.get("compiler_path") != str(root / "bin" / "fake-cxx"):
                fail(f"invalid compiler path: {metric}")
            expected_version_digest = hashlib.sha256(
                b"fake-cxx version 1.0\n"
            ).hexdigest()
            if metric.get("compiler_version_sha256") != expected_version_digest:
                fail(f"invalid compiler version identity: {metric}")
            expected_compiler_digest = hashlib.sha256(
                (root / "bin" / "fake-cxx").read_bytes()
            ).hexdigest()
            if metric.get("compiler_sha256") != expected_compiler_digest:
                fail(f"invalid compiler binary identity: {metric}")
            expected_wkictl_digest = hashlib.sha256(
                (root / "bin" / "wkictl").read_bytes()
            ).hexdigest()
            if metric.get("wkictl_sha256") != expected_wkictl_digest:
                fail(f"invalid wkictl binary identity: {metric}")
            if metric.get("compile_flags") != "-std=c++23 -O2 -fno-ident":
                fail(f"invalid compile flags: {metric}")
            if metric.get("link_flags") != "-std=c++23 -O2 -Wl,--build-id=none":
                fail(f"invalid link flags: {metric}")
            if (
                metric.get("cache_policy")
                != "prewarmed-compiler-source-headers-all-hosts"
            ):
                fail(f"invalid cache policy: {metric}")
            if (
                metric.get("launch_policy")
                != "one-controller-per-host-local-tu-workers"
                or metric.get("controller_count") != node_count
            ):
                fail(f"invalid compile launch policy: {metric}")
            if metric.get("placement") != expected_placement:
                fail(f"invalid placement evidence: {metric}")
            if metric.get("wki_route") != "host-workspace":
                fail(f"invalid workspace routing evidence: {metric}")
            expected_runtime_paths = [
                "/root/wos-showcase",
                "/usr",
                "/bin",
                "/lib",
                "/lib64",
                "/libexec",
                "/share",
                "/tmp",
            ]
            if (
                metric.get("runtime_route") != "local"
                or metric.get("runtime_paths") != expected_runtime_paths
            ):
                fail(f"invalid LOCAL runtime evidence: {metric}")
            workspace = metric.get("workspace_path")
            if metric.get("workspace_route") != "host" or not isinstance(
                workspace, str
            ):
                fail(f"invalid HOST workspace evidence: {metric}")
            if (
                re.fullmatch(
                    r"/tmp/wos-showcase-fixed-[0-9a-f]{16}/distributed-compile",
                    workspace,
                )
                is None
            ):
                fail(f"unsafe private workspace evidence: {metric}")
            if metric.get("launcher_host") != hosts[0]:
                fail(f"invalid launcher evidence: {metric}")
            elapsed = metric.get("elapsed_seconds")
            if not isinstance(elapsed, (int, float)) or elapsed <= 0:
                fail(f"invalid elapsed time: {metric}")
            digest = metric.get("artifact_digest")
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                fail(f"invalid artifact digest: {metric}")

            participants = metric.get("participants")
            if not isinstance(participants, list) or len(participants) != node_count:
                fail(f"invalid participant coverage: {metric}")
            if [
                participant["work_units"] for participant in participants
            ] != expected_work:
                fail(f"invalid work split: {participants}")
            if [participant["host"] for participant in participants] != hosts:
                fail(f"invalid declared hosts: {participants}")
            if [participant["runner_host"] for participant in participants] != hosts:
                fail(f"invalid runner hosts: {participants}")
            expected_transports = ["local"] + ["wki"] * (node_count - 1)
            if [
                participant["transport"] for participant in participants
            ] != expected_transports:
                fail(f"invalid transports: {participants}")

            compile_rows = (
                (root / "compile.log").read_text(encoding="utf-8").splitlines()
            )
            if len(compile_rows) != 32:
                fail(f"expected 32 compiler jobs, got {compile_rows}")
            compile_symbols = [row.split()[1] for row in compile_rows]
            expected_symbols = {
                f"wos_compile_unit_{unit:02d}" for unit in range(32)
            }
            if set(compile_symbols) != expected_symbols:
                fail(f"compiler jobs have missing or duplicate units: {compile_rows}")
            on_rows = (root / "on.log").read_text(encoding="utf-8").splitlines()
            profile = [
                "forward",
                f"+{workspace}",
                *(f"-{path}" for path in expected_runtime_paths),
                "--",
            ]
            expected_on_rows = (
                [
                    "|".join([host, *profile, "kind=probe"])
                    for host in [hosts[0], *hosts]
                ]
                + [
                    "|".join([host, *profile, "kind=prewarm"]) for host in hosts
                ]
                + [
                    "|".join([host, *profile, "kind=controller"])
                    for host in hosts
                ]
            )
            if on_rows != expected_on_rows:
                fail(f"strict on/forward launches differ:\n{on_rows}")
            prewarm_rows = (
                (root / "prewarm.log").read_text(encoding="utf-8").splitlines()
            )
            if [row.split()[0] for row in prewarm_rows] != hosts:
                fail(f"compiler prewarm does not cover canonical hosts: {prewarm_rows}")
            event_rows = (root / "event.log").read_text(encoding="utf-8").splitlines()
            if [row.split()[0] for row in event_rows[:node_count]] != [
                "prewarm"
            ] * node_count or any(
                row.split()[0] != "timed" for row in event_rows[node_count:]
            ):
                fail(f"prewarm did not finish before timed jobs: {event_rows}")
            workspace_rows = (
                (root / "workspace.log").read_text(encoding="utf-8").splitlines()
            )
            if not workspace_rows or any(
                row.split("|", 1)[0] != workspace for row in workspace_rows
            ):
                fail(f"private workspace marker was not observed: {workspace_rows}")
            owner = workspace_rows[0].split("|", 1)[1]
            work_suffix = workspace.split("/")[2].removeprefix("wos-showcase-fixed-")
            if (
                len(owner) != 32
                or owner[:16] != work_suffix
                or not all(character in "0123456789abcdef" for character in owner)
            ):
                fail(f"invalid private workspace owner: {owner}")
            if Path(workspace).parent.exists():
                fail(f"private workspace was not cleaned: {workspace}")


def test_compile_failure_waits_for_all_jobs() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_FAIL_SYMBOL="wos_compile_unit_07",
        )
        if result.returncode == 0:
            fail(f"compiler failure was accepted:\n{result.stdout}")
        if "unit 07 on wos-0.wos failed with rc=23" not in result.stderr:
            fail(f"compiler failure diagnostic is missing:\n{result.stderr}")
        if "fake-cxx forced failure for wos_compile_unit_07" not in result.stderr:
            fail(f"per-unit compiler log was not recovered:\n{result.stderr}")
        compile_rows = (root / "compile.log").read_text(encoding="utf-8").splitlines()
        if len(compile_rows) != 32:
            fail(f"script did not launch/wait the full compile set: {compile_rows}")
        on_rows = (root / "on.log").read_text(encoding="utf-8").splitlines()
        if len(on_rows) != 7 or any(
            not row.endswith("|kind=controller") for row in on_rows[-2:]
        ):
            fail(f"two-host compile did not use one timed controller per host: {on_rows}")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("failed compile emitted an acceptance metric")


def test_controller_failure_preserves_infrastructure_diagnostic() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_FAIL_CONTROLLER_HOST="wos-1.wos",
        )
        if result.returncode == 0:
            fail(f"controller failure was accepted:\n{result.stdout}")
        required = [
            "controller on wos-1.wos reported 0 of 16 unit statuses",
            "controller on wos-1.wos failed with rc=42",
            "mock controller launch failure on wos-1.wos",
        ]
        if any(token not in result.stderr for token in required):
            fail(f"controller failure diagnostic is incomplete:\n{result.stderr}")
        compile_rows = (root / "compile.log").read_text(encoding="utf-8").splitlines()
        if len(compile_rows) != 16:
            fail(f"healthy controller did not finish its assigned range: {compile_rows}")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("failed controller emitted an acceptance metric")


def test_runner_mismatch_is_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_MISMATCH_HOST="wos-1.wos",
        )
        if result.returncode == 0:
            fail(f"runner mismatch was accepted:\n{result.stdout}")
        if "ran on mismatch.wos instead of wos-1.wos" not in result.stderr:
            fail(f"runner mismatch diagnostic is missing:\n{result.stderr}")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("runner mismatch emitted an acceptance metric")


def test_compiler_mismatch_is_rejected_before_timing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_COMPILER_MISMATCH_HOST="wos-1.wos",
        )
        if result.returncode == 0:
            fail(f"compiler mismatch was accepted:\n{result.stdout}")
        if "compiler identity on wos-1.wos differs from launcher" not in result.stderr:
            fail(f"compiler mismatch diagnostic is missing:\n{result.stderr}")
        if (root / "compile.log").exists():
            fail("compiler mismatch launched timed translation-unit jobs")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("compiler mismatch emitted an acceptance metric")


def test_wkictl_mismatch_is_rejected_before_timing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_WKICTL_MISMATCH_HOST="wos-1.wos",
        )
        if result.returncode == 0:
            fail(f"wkictl mismatch was accepted:\n{result.stdout}")
        if "compiler identity on wos-1.wos differs from launcher" not in result.stderr:
            fail(f"wkictl mismatch diagnostic is missing:\n{result.stderr}")
        if (root / "prewarm.log").exists() or (root / "compile.log").exists():
            fail("wkictl mismatch reached prewarm or timed compilation")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("wkictl mismatch emitted an acceptance metric")


def test_source_mismatch_is_rejected_before_timing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "different.cpp"
        source.write_text("auto main() -> int { return 0; }\n", encoding="utf-8")
        result = run_showcase(
            root,
            ["wos-0.wos"],
            WOS_LIVE_DEMO_SRC=str(source),
        )
        if result.returncode == 0:
            fail(f"source mismatch was accepted:\n{result.stdout}")
        if "source fingerprint differs from wos-live-cpp-32-tu-v1" not in result.stderr:
            fail(f"source mismatch diagnostic is missing:\n{result.stderr}")
        if (root / "compile.log").exists():
            fail("source mismatch launched timed translation-unit jobs")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("source mismatch emitted an acceptance metric")


def test_missing_local_runtime_route_is_rejected_before_timing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_DROP_ROUTE_PATH="/lib64",
        )
        if result.returncode == 0:
            fail(f"missing LOCAL runtime route was accepted:\n{result.stdout}")
        if "cannot resolve or fingerprint compiler" not in result.stderr:
            fail(f"route failure diagnostic is missing:\n{result.stderr}")
        if (root / "compile.log").exists():
            fail("invalid runtime profile launched timed translation-unit jobs")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("invalid runtime profile emitted an acceptance metric")


def test_prewarm_failure_is_rejected_before_timing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = run_showcase(
            root,
            ["wos-0.wos", "wos-1.wos"],
            MOCK_FAIL_PREWARM_HOST="wos-1.wos",
        )
        if result.returncode == 0:
            fail(f"compiler prewarm failure was accepted:\n{result.stdout}")
        if "compiler prewarm failed on wos-1.wos" not in result.stderr:
            fail(f"compiler prewarm failure diagnostic is missing:\n{result.stderr}")
        if (root / "compile.log").exists():
            fail("compiler prewarm failure launched timed translation-unit jobs")
        event_rows = (root / "event.log").read_text(encoding="utf-8").splitlines()
        if any(row.startswith("timed ") for row in event_rows):
            fail(f"timed jobs started before prewarm completed: {event_rows}")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("compiler prewarm failure emitted an acceptance metric")


def test_rotated_launcher_uses_canonical_three_node_partition() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        submitted_hosts = ["wos-2.wos", "wos-0.wos", "wos-1.wos"]
        result = run_showcase(
            root,
            submitted_hosts,
            MOCK_LAUNCHER="wos-2.wos",
        )
        if result.returncode != 0:
            fail(
                f"rotated-launcher showcase failed with {result.returncode}:\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        metric = metric_from_stdout(result.stdout)
        participants = metric.get("participants")
        if not isinstance(participants, list):
            fail(f"missing canonical participants: {metric}")
        canonical_hosts = ["wos-0.wos", "wos-1.wos", "wos-2.wos"]
        if [participant["host"] for participant in participants] != canonical_hosts:
            fail(f"host partition is launcher-order dependent: {participants}")
        if [participant["work_units"] for participant in participants] != [11, 11, 10]:
            fail(f"three-node remainder is not canonical: {participants}")
        if [participant["transport"] for participant in participants] != [
            "wki",
            "wki",
            "local",
        ]:
            fail(f"rotated-launcher transport evidence is invalid: {participants}")
        if metric.get("launcher_host") != "wos-2.wos":
            fail(f"rotated launcher identity was lost: {metric}")

        compile_assignments = {}
        for row in (root / "compile.log").read_text(encoding="utf-8").splitlines():
            runner, symbol = row.split()
            compile_assignments[int(symbol.removeprefix("wos_compile_unit_"))] = runner
        expected_assignments = {
            unit: (
                canonical_hosts[0]
                if unit < 11
                else canonical_hosts[1] if unit < 22 else canonical_hosts[2]
            )
            for unit in range(32)
        }
        if compile_assignments != expected_assignments:
            fail(f"timed jobs do not follow canonical partition: {compile_assignments}")


def test_invalid_host_sets_are_rejected() -> None:
    invalid_sets = [
        ["wos-0.wos", "wos-0.wos"],
        ["wos-0.wos", "wos-1", "wos-1.wos"],
        ["wos-0.wos", "wos-1.wos", "wos-2.wos", "wos-3.wos", "wos-4.wos"],
        ["wos-0.wos", "unsafe host"],
    ]
    for hosts in invalid_sets:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_showcase(Path(tmp), hosts)
            if result.returncode == 0:
                fail(f"invalid hosts were accepted: {hosts}\n{result.stdout}")


def main() -> int:
    test_fixed_layouts()
    test_compile_failure_waits_for_all_jobs()
    test_controller_failure_preserves_infrastructure_diagnostic()
    test_runner_mismatch_is_rejected()
    test_compiler_mismatch_is_rejected_before_timing()
    test_wkictl_mismatch_is_rejected_before_timing()
    test_source_mismatch_is_rejected_before_timing()
    test_missing_local_runtime_route_is_rejected_before_timing()
    test_prewarm_failure_is_rejected_before_timing()
    test_rotated_launcher_uses_canonical_three_node_partition()
    test_invalid_host_sets_are_rejected()
    print("11 WOS showcase distributed compile tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
