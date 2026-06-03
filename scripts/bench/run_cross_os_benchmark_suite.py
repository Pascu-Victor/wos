#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
BENCH_SCRIPTS = ROOT / "scripts" / "bench"
REMOTE_SCRIPTS = ROOT / "scripts" / "remote"


@dataclass(frozen=True)
class RenderCase:
    name: str
    width: int
    height: int
    spp: int
    wos_scene: str | None
    linux_scene: str | None


def run_command(
    args: list[str],
    *,
    cwd: Path = ROOT,
    input_text: str | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            check=False,
            text=True,
            capture_output=True,
            input=input_text,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        message = [f"command timed out after {timeout:.1f}s: {shlex.join(args)}"]
        if exc.stdout:
            message.append("stdout:")
            message.append(str(exc.stdout).strip())
        else:
            message.append("stdout: (empty)")
        if exc.stderr:
            message.append("stderr:")
            message.append(str(exc.stderr).strip())
        else:
            message.append("stderr: (empty)")
        raise RuntimeError("\n".join(message)) from exc
    if result.returncode != 0:
        message = [f"command failed with exit code {result.returncode}: {shlex.join(args)}"]
        if result.stdout.strip():
            message.append("stdout:")
            message.append(result.stdout.strip())
        else:
            message.append("stdout: (empty)")
        if result.stderr.strip():
            message.append("stderr:")
            message.append(result.stderr.strip())
        else:
            message.append("stderr: (empty)")
        raise RuntimeError("\n".join(message))
    return result


def wos_remote_command(host: str, command: str, *, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    return run_command([str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, command], timeout=timeout)


def wos_benchmark_pids(host: str, *, timeout: float) -> list[int]:
    result = wos_remote_command(host, "ps aux 2>/dev/null || ps", timeout=timeout)
    pids: list[int] = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("PID "):
            continue
        fields = stripped.split(maxsplit=4)
        if len(fields) < 5 or not fields[0].isdigit():
            continue
        command = fields[4].split("\0", 1)[0].split()[0]
        basename = os.path.basename(command)
        if basename in {"renderbench", "testprog"}:
            pids.append(int(fields[0]))
    return pids


def kill_wos_pids(host: str, pids: list[int], signal: str, *, timeout: float) -> None:
    if not pids:
        return
    command = "kill -" + signal + " " + " ".join(str(pid) for pid in pids) + " 2>/dev/null || true"
    wos_remote_command(host, command, timeout=timeout)


def prepare_wos_hosts(args: argparse.Namespace, hosts: list[str]) -> None:
    if args.skip_wos_cleanup:
        return

    failures: list[str] = []
    for host in hosts:
        try:
            wos_remote_command(host, "hostname >/dev/null", timeout=args.wos_preflight_timeout)
        except RuntimeError as exc:
            failures.append(f"{host}: unreachable ({exc})")
            continue

        pids = wos_benchmark_pids(host, timeout=args.wos_preflight_timeout)
        if not pids:
            continue

        print(f"[suite] cleanup {host}: stale benchmark pids {','.join(str(pid) for pid in pids)}")
        kill_wos_pids(host, pids, "TERM", timeout=args.wos_preflight_timeout)
        remaining = wos_benchmark_pids(host, timeout=args.wos_preflight_timeout)
        remaining = [pid for pid in remaining if pid in pids]
        if remaining:
            kill_wos_pids(host, remaining, "KILL", timeout=args.wos_preflight_timeout)
            remaining = wos_benchmark_pids(host, timeout=args.wos_preflight_timeout)
            remaining = [pid for pid in remaining if pid in pids]
        if remaining:
            failures.append(f"{host}: stale benchmark pids did not exit: {','.join(str(pid) for pid in remaining)}")

    if failures:
        raise RuntimeError("WOS preflight failed:\n" + "\n".join(failures))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def relpath(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def step_log_text(result: subprocess.CompletedProcess[str]) -> str:
    return (
        "=== stdout ===\n"
        + (result.stdout if result.stdout else "")
        + "\n=== stderr ===\n"
        + (result.stderr if result.stderr else "")
    )


def fetch_remote_file(fetcher: Path, host: str, remote_path: str, local_path: Path) -> None:
    run_command([str(fetcher), host, remote_path, str(local_path)])


def parse_mandel_report(text: str) -> dict[str, Any]:
    line = ""
    for candidate in text.splitlines():
        stripped = candidate.strip()
        if stripped.startswith("name="):
            line = stripped
    if not line:
        raise RuntimeError("mandelbench report.txt did not contain a summary line")

    out: dict[str, Any] = {"summary_line": line}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if value.replace(".", "", 1).isdigit():
            if "." in value:
                out[key] = float(value)
            else:
                out[key] = int(value)
        else:
            out[key] = value
    return out


def render_cases(args: argparse.Namespace) -> list[RenderCase]:
    return [
        RenderCase(
            name="default-scene",
            width=args.default_render_width,
            height=args.default_render_height,
            spp=args.default_render_spp,
            wos_scene=None,
            linux_scene=None,
        ),
        RenderCase(
            name="duck",
            width=args.duck_render_width,
            height=args.duck_render_height,
            spp=args.duck_render_spp,
            wos_scene=args.wos_duck_scene,
            linux_scene=args.linux_duck_scene,
        ),
    ]


def render_placements(raw: str) -> list[str]:
    placements = [item.strip() for item in raw.split(",") if item.strip()]
    valid = {"node-threads", "process-per-core"}
    invalid = [item for item in placements if item not in valid]
    if invalid:
        raise ValueError(f"invalid render placements: {', '.join(invalid)}")
    if not placements:
        raise ValueError("at least one render placement is required")
    return placements


def append_step(manifest: dict[str, Any], suite_dir: Path, step: dict[str, Any]) -> None:
    manifest["steps"].append(step)
    write_json(suite_dir / "manifest.json", manifest)


def run_wos_mandelbench(
    args: argparse.Namespace,
    suite_dir: Path,
    suite_remote_root: str,
    host: str,
    wos_hosts: list[str],
) -> dict[str, Any]:
    prepare_wos_hosts(args, wos_hosts)

    step_name = "wos-mandelbench"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    remote_work_dir = f"{suite_remote_root}/{step_name}"
    total_workers = args.mandel_threads * args.num_vms
    remote_command = " ".join(
        [
            "mkdir -p",
            shlex.quote(remote_work_dir),
            "&& cd",
            shlex.quote(remote_work_dir),
            "&& /usr/bin/testprog mandelbench",
            "--width",
            shlex.quote(str(args.mandel_width)),
            "--height",
            shlex.quote(str(args.mandel_height)),
            "--max-iter",
            shlex.quote(str(args.mandel_max_iter)),
            "--threads",
            shlex.quote(str(total_workers)),
            "--repeat",
            shlex.quote(str(args.mandel_repeat)),
            "--nodes",
            shlex.quote(",".join(wos_hosts)),
        ]
    )
    result = run_command([str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, remote_command])
    write_text(step_dir / "command.log", step_log_text(result))

    report_local = step_dir / "report.txt"
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_work_dir}/report.txt",
        report_local,
    )
    summary = parse_mandel_report(report_local.read_text(encoding="utf-8"))
    summary.update(
        {
            "benchmark": "mandelbench",
            "os": "wos",
            "host": host,
            "hosts": wos_hosts,
            "width": args.mandel_width,
            "height": args.mandel_height,
            "max_iteration": args.mandel_max_iter,
            "threads": total_workers,
            "threads_per_node": args.mandel_threads,
            "repeat": args.mandel_repeat,
        }
    )
    write_json(step_dir / "result.json", summary)

    artifacts: list[str] = [relpath(report_local), relpath(step_dir / "result.json")]
    for index in range(args.mandel_repeat):
        image_name = f"process_{index:02d}.png"
        local_image = step_dir / image_name
        fetch_remote_file(
            REMOTE_SCRIPTS / "wos_sftp_get.sh",
            host,
            f"{remote_work_dir}/{image_name}",
            local_image,
        )
        artifacts.append(relpath(local_image))

    return {
        "name": step_name,
        "ok": True,
        "os": "wos",
        "host": host,
        "hosts": wos_hosts,
        "command": [str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, remote_command],
        "result_file": relpath(step_dir / "result.json"),
        "artifacts": artifacts,
    }


def run_linux_mandelbench(
    args: argparse.Namespace,
    suite_dir: Path,
    suite_remote_root: str,
    linux_hosts: list[str],
    launcher: str,
) -> dict[str, Any]:
    step_name = "linux-mandelbench"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    output_path = step_dir / "result.json"
    remote_work_dir = f"{suite_remote_root}/{step_name}"
    total_workers = args.mandel_threads * len(linux_hosts)

    command = [
        str(BENCH_SCRIPTS / "run_linux_mpi_benchmark.sh"),
        "--launcher",
        launcher,
        "--hosts",
        ",".join(linux_hosts),
        "--benchmark",
        "mandel",
        "--router-ip",
        args.router_ip,
        "--host-ip",
        args.host_ip,
        "--width",
        str(args.mandel_width),
        "--height",
        str(args.mandel_height),
        "--max-iter",
        str(args.mandel_max_iter),
        "--repeat",
        str(args.mandel_repeat),
        "--threads",
        str(args.mandel_threads),
        "--np",
        str(total_workers),
        "--mandel-output-root",
        remote_work_dir,
        "--output",
        str(output_path),
    ]
    if args.linux_use_host_binary:
        command += ["--use-host-binary"]

    result = run_command(command)
    write_text(step_dir / "command.log", step_log_text(result))

    report_local = step_dir / "report.txt"
    fetch_remote_file(
        REMOTE_SCRIPTS / "linux_sftp_get.sh",
        launcher,
        f"{remote_work_dir}/report.txt",
        report_local,
    )
    summary = parse_mandel_report(report_local.read_text(encoding="utf-8"))
    summary.update(
        {
            "benchmark": "mandelbench",
            "os": "linux",
            "host": launcher,
            "hosts": linux_hosts,
            "width": args.mandel_width,
            "height": args.mandel_height,
            "max_iteration": args.mandel_max_iter,
            "threads": total_workers,
            "threads_per_node": args.mandel_threads,
            "repeat": args.mandel_repeat,
        }
    )
    write_json(step_dir / "result.json", summary)

    artifacts: list[str] = [relpath(report_local), relpath(step_dir / "result.json")]
    for index in range(args.mandel_repeat):
        image_name = f"process_{index:02d}.png"
        local_image = step_dir / image_name
        fetch_remote_file(
            REMOTE_SCRIPTS / "linux_sftp_get.sh",
            launcher,
            f"{remote_work_dir}/{image_name}",
            local_image,
        )
        artifacts.append(relpath(local_image))

    return {
        "name": step_name,
        "ok": True,
        "os": "linux",
        "host": launcher,
        "hosts": linux_hosts,
        "command": command,
        "result_file": relpath(step_dir / "result.json"),
        "artifacts": artifacts,
    }


def run_wos_renderbench(
    args: argparse.Namespace,
    suite_dir: Path,
    suite_remote_root: str,
    host: str,
    wos_hosts: list[str],
    case: RenderCase,
    placement: str,
) -> dict[str, Any]:
    prepare_wos_hosts(args, wos_hosts)

    step_name = f"wos-render-{case.name}-{placement}"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    remote_output_root = f"{suite_remote_root}/renderbench"
    run_id = f"{case.name}-{placement}"

    command = [
        "/usr/bin/renderbench",
        "--backend",
        "ipc",
        "--placement",
        placement,
        "--width",
        str(case.width),
        "--height",
        str(case.height),
        "--spp",
        str(case.spp),
        "--max-depth",
        str(args.render_max_depth),
        "--tile-size",
        str(args.render_tile_size),
        "--output-root",
        remote_output_root,
        "--run-id",
        run_id,
    ]
    if case.wos_scene:
        command += ["--scene", case.wos_scene]
    if args.wos_render_threads is not None:
        command += ["--threads", str(args.wos_render_threads)]
    if args.render_debug_constant_tile_us > 0:
        command += ["--debug-constant-tile-us", str(args.render_debug_constant_tile_us)]

    result = run_command([str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, *command])
    write_text(step_dir / "command.log", step_log_text(result))

    remote_run_dir = f"{remote_output_root}/{run_id}"
    metrics_local = step_dir / "metrics.json"
    status_local = step_dir / "status.json"
    frame_local = step_dir / "frame_000.png"
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_run_dir}/metrics.json",
        metrics_local,
    )
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_run_dir}/status.json",
        status_local,
    )
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_run_dir}/frame_000.png",
        frame_local,
    )

    metrics = json.loads(metrics_local.read_text(encoding="utf-8"))
    status = json.loads(status_local.read_text(encoding="utf-8"))
    merged = {**status, **metrics}
    merged.update(
        {
            "benchmark": "renderbench",
            "os": "wos",
            "host": host,
            "scene": case.name,
            "scene_path": case.wos_scene,
        }
    )
    write_json(step_dir / "result.json", merged)

    return {
        "name": step_name,
        "ok": True,
        "os": "wos",
        "host": host,
        "command": [str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, *command],
        "result_file": relpath(step_dir / "result.json"),
        "artifacts": [
            relpath(metrics_local),
            relpath(status_local),
            relpath(frame_local),
            relpath(step_dir / "result.json"),
        ],
    }


def run_linux_renderbench(
    args: argparse.Namespace,
    suite_dir: Path,
    linux_hosts: list[str],
    launcher: str,
    case: RenderCase,
    placement: str,
) -> dict[str, Any]:
    step_name = f"linux-render-{case.name}-{placement}"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    output_path = step_dir / "result.json"
    run_id = f"{case.name}-{placement}"
    remote_output_root = f"/var/lib/wos-bench/results/tracebench/{args.remote_suite_name}/{step_name}"

    command = [
        str(BENCH_SCRIPTS / "run_linux_mpi_benchmark.sh"),
        "--launcher",
        launcher,
        "--hosts",
        ",".join(linux_hosts),
        "--benchmark",
        "render",
        "--router-ip",
        args.router_ip,
        "--host-ip",
        args.host_ip,
        "--width",
        str(case.width),
        "--height",
        str(case.height),
        "--spp",
        str(case.spp),
        "--max-depth",
        str(args.render_max_depth),
        "--tile-size",
        str(args.render_tile_size),
        "--placement",
        placement,
        "--render-output-root",
        remote_output_root,
        "--run-id",
        run_id,
        "--output",
        str(output_path),
    ]
    if case.linux_scene:
        command += ["--scene", case.linux_scene]
    if args.linux_render_threads is not None:
        command += ["--threads", str(args.linux_render_threads)]
    if args.render_debug_constant_tile_us > 0:
        command += ["--debug-constant-tile-us", str(args.render_debug_constant_tile_us)]
    if args.linux_use_host_binary:
        command += ["--use-host-binary"]

    result = run_command(command)
    write_text(step_dir / "command.log", step_log_text(result))
    artifacts = [relpath(output_path), relpath(step_dir / "command.log")]
    frame_path = step_dir / "result-artifacts" / "frame_000.png"
    if frame_path.exists():
        artifacts.append(relpath(frame_path))

    return {
        "name": step_name,
        "ok": True,
        "os": "linux",
        "host": launcher,
        "hosts": linux_hosts,
        "command": command,
        "result_file": relpath(output_path),
        "artifacts": artifacts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the configured WOS/Linux benchmark suite across already-running VMs."
    )
    parser.add_argument("--num-vms", type=int, required=True)
    parser.add_argument(
        "--os",
        choices=["both", "wos", "linux"],
        default="both",
        help="Choose whether to run both sides of the suite or only one OS.",
    )
    parser.add_argument("--wos-launcher-index", type=int, default=0)
    parser.add_argument("--linux-launcher-index", type=int, default=0)
    parser.add_argument(
        "--results-dir", default="benchmarks/results/cross-os"
    )
    parser.add_argument("--router-ip", default="10.10.0.1")
    parser.add_argument("--host-ip", default="10.10.0.100")
    parser.add_argument("--linux-user", default="user")
    parser.add_argument("--mandel-width", type=int, default=2000)
    parser.add_argument("--mandel-height", type=int, default=2000)
    parser.add_argument("--mandel-max-iter", type=int, default=5000)
    parser.add_argument("--mandel-threads", type=int, default=8, help="Mandelbench worker processes/ranks per node.")
    parser.add_argument("--mandel-repeat", type=int, default=5)
    parser.add_argument(
        "--render-placements",
        default="node-threads,process-per-core",
        help="Comma-separated placements to run for renderbench.",
    )
    parser.add_argument("--default-render-width", type=int, default=1000)
    parser.add_argument("--default-render-height", type=int, default=1000)
    parser.add_argument("--default-render-spp", type=int, default=500)
    parser.add_argument("--duck-render-width", type=int, default=4000)
    parser.add_argument("--duck-render-height", type=int, default=4000)
    parser.add_argument("--duck-render-spp", type=int, default=1600)
    parser.add_argument("--render-max-depth", type=int, default=6)
    parser.add_argument("--render-tile-size", type=int, default=32)
    parser.add_argument(
        "--render-debug-constant-tile-us",
        type=int,
        default=0,
        help="Use renderbench's constant-rate synthetic tile mode with this per-tile delay in microseconds.",
    )
    parser.add_argument("--wos-duck-scene", default="/srv/Duck.glb")
    parser.add_argument("--linux-duck-scene", default="configs/drive/srv/Duck.glb")
    parser.add_argument("--wos-render-threads", type=int)
    parser.add_argument("--linux-render-threads", type=int)
    parser.add_argument("--linux-use-host-binary", action="store_true")
    parser.add_argument(
        "--skip-wos-cleanup",
        action="store_true",
        help="Do not preflight WOS hosts or kill stale WOS benchmark processes before WOS steps.",
    )
    parser.add_argument(
        "--wos-preflight-timeout",
        type=float,
        default=8.0,
        help="Seconds to wait for each WOS preflight or cleanup SSH command.",
    )
    parser.add_argument("--skip-mandelbench", action="store_true")
    parser.add_argument("--skip-renderbench", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.num_vms <= 0:
        parser.error("--num-vms must be greater than zero")
    if not 0 <= args.wos_launcher_index < args.num_vms:
        parser.error("--wos-launcher-index must be within the VM count")
    if not 0 <= args.linux_launcher_index < args.num_vms:
        parser.error("--linux-launcher-index must be within the VM count")
    if args.render_debug_constant_tile_us < 0:
        parser.error("--render-debug-constant-tile-us must be nonnegative")

    try:
        placements = render_placements(args.render_placements)
    except ValueError as exc:
        parser.error(str(exc))

    linux_duck_scene = ROOT / args.linux_duck_scene
    if not args.skip_renderbench and not linux_duck_scene.is_file():
        parser.error(f"Linux Duck scene not found: {linux_duck_scene}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    args.remote_suite_name = f"cross-os-suite-{timestamp}"
    suite_dir = ROOT / args.results_dir / args.remote_suite_name
    suite_dir.mkdir(parents=True, exist_ok=True)
    suite_remote_root = f"/tmp/{args.remote_suite_name}"

    wos_hosts = [f"wos-{index}.wos" for index in range(args.num_vms)]
    linux_hosts = [f"wos-ubuntu-vm{index}.wos" for index in range(args.num_vms)]
    wos_launcher = wos_hosts[args.wos_launcher_index]
    linux_launcher = linux_hosts[args.linux_launcher_index]

    manifest: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "suite_name": args.remote_suite_name,
        "os_selection": args.os,
        "num_vms": args.num_vms,
        "wos_hosts": wos_hosts,
        "linux_hosts": linux_hosts,
        "wos_launcher": wos_launcher,
        "linux_launcher": linux_launcher,
        "steps": [],
    }
    write_json(suite_dir / "manifest.json", manifest)

    failures = 0

    def run_step(name: str, func: Any) -> None:
        nonlocal failures
        print(f"[suite] {name}")
        try:
            step = func()
        except Exception as exc:  # noqa: BLE001
            failures += 1
            error_path = suite_dir / name / "error.txt"
            write_text(error_path, f"{exc}\n")
            append_step(
                manifest,
                suite_dir,
                {
                    "name": name,
                    "ok": False,
                    "error": str(exc),
                    "error_file": relpath(error_path),
                },
            )
            print(f"[suite] {name} failed")
            return

        append_step(manifest, suite_dir, step)
        print(f"[suite] {name} complete")

    run_wos = args.os in {"both", "wos"}
    run_linux = args.os in {"both", "linux"}

    if not args.skip_mandelbench:
        if run_wos:
            run_step(
                "wos-mandelbench",
                lambda: run_wos_mandelbench(args, suite_dir, suite_remote_root, wos_launcher, wos_hosts),
            )
        if run_linux:
            run_step(
                "linux-mandelbench",
                lambda: run_linux_mandelbench(args, suite_dir, suite_remote_root, linux_hosts, linux_launcher),
            )

    if not args.skip_renderbench:
        for case in render_cases(args):
            for placement in placements:
                if run_wos:
                    run_step(
                        f"wos-render-{case.name}-{placement}",
                        lambda case=case, placement=placement: run_wos_renderbench(
                            args,
                            suite_dir,
                            suite_remote_root,
                            wos_launcher,
                            wos_hosts,
                            case,
                            placement,
                        ),
                    )
                if run_linux:
                    run_step(
                        f"linux-render-{case.name}-{placement}",
                        lambda case=case, placement=placement: run_linux_renderbench(
                            args,
                            suite_dir,
                            linux_hosts,
                            linux_launcher,
                            case,
                            placement,
                        ),
                    )

    print(str(suite_dir / "manifest.json"))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
