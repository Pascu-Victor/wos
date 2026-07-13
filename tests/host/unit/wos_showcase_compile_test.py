#!/usr/bin/env python3

import json
import os
from pathlib import Path
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
    on_log = root / "on.log"
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
    printf '%s|%s|%s|%s\n' "$host" "$1" "${2:-}" "${3:-}" >> "$MOCK_ON_LOG"
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
while [ "$#" -gt 0 ]; do
    case "$1" in
        --) shift; break ;;
        +*|-*) shift ;;
        *) break ;;
    esac
done
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
    printf '%s %s\n' "${MOCK_RUNNER:-$MOCK_LAUNCHER}" "$symbol" >> "$MOCK_COMPILE_LOG"
    if [ "${MOCK_FAIL_SYMBOL:-}" = "$symbol" ]; then
        exit 23
    fi
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
            "MOCK_LAUNCHER": "wos-0.wos",
            "MOCK_LIVE_BIN": str(live_bin),
            "MOCK_ON_LOG": str(on_log),
            "WOS_LIVE_COMPILE_WORKSPACE_ROOT": str(root / "workspace"),
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
            if metric.get("placement") != expected_placement:
                fail(f"invalid placement evidence: {metric}")
            if metric.get("wki_route") != "host-workspace":
                fail(f"invalid workspace routing evidence: {metric}")
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
            on_rows = (root / "on.log").read_text(encoding="utf-8").splitlines()
            workspace_operand = f"+{root}/workspace/distributed-compile"
            expected_on_rows = [
                f"{host}|forward|{workspace_operand}|--"
                for host, count in zip(hosts, expected_work, strict=True)
                for _index in range(count)
            ]
            if on_rows != expected_on_rows:
                fail(f"strict on/forward launches differ:\n{on_rows}")


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
        compile_rows = (root / "compile.log").read_text(encoding="utf-8").splitlines()
        if len(compile_rows) != 32:
            fail(f"script did not launch/wait the full compile set: {compile_rows}")
        if '"benchmark":"wos_distributed_compile"' in result.stdout:
            fail("failed compile emitted an acceptance metric")


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
    test_runner_mismatch_is_rejected()
    test_invalid_host_sets_are_rejected()
    print("4 WOS showcase distributed compile tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
