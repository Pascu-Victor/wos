#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"


def run_command(
    args: list[str], stream: bool = False
) -> subprocess.CompletedProcess[str]:
    if stream:
        result = subprocess.run(args, cwd=ROOT, check=False, text=True)
    else:
        result = subprocess.run(
            args, cwd=ROOT, check=False, capture_output=True, text=True
        )
    if result.returncode != 0:
        message = [
            f"command failed with exit code {result.returncode}: {' '.join(args)}"
        ]
        if stream:
            message.append("(output was streamed to terminal)")
        else:
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


def ping_once(host: str) -> bool:
    result = subprocess.run(
        ["ping", "-c", "1", "-W", "1", host], cwd=ROOT, capture_output=True, text=True
    )
    return result.returncode == 0


def check_lan(router_ip: str, host_ip: str, ubuntu_hosts: list[str]) -> int:
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "host_ip": host_ip,
        "router_ip": router_ip,
        "router_reachable": ping_once(router_ip),
        "ubuntu_hosts": [],
    }
    for host in ubuntu_hosts:
        reachable = False
        try:
            run_command([str(SCRIPTS / "linux_ssh.sh"), host, "true"])
            reachable = True
        except subprocess.CalledProcessError:
            reachable = False
        summary["ubuntu_hosts"].append({"host": host, "ssh_reachable": reachable})

    print(json.dumps(summary))
    return 0 if summary["router_reachable"] else 1


def write_suite_manifest(results_dir: Path, manifest: dict) -> Path:
    results_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        results_dir
        / f"orchestrator-manifest-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="ascii")
    return manifest_path


def run_suite(args: argparse.Namespace) -> int:
    manifest: dict[str, object] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "router_ip": args.router_ip,
        "host_ip": args.host_ip,
        "router_reachable": ping_once(args.router_ip),
        "steps": [],
    }

    if args.wos_fs_host and args.wos_fs_path:
        cmd = [
            str(SCRIPTS / "run_wos_fsbench.sh"),
            args.wos_fs_host,
            "--mode",
            args.wos_fs_mode,
            "--path",
            args.wos_fs_path,
        ]
        if args.wos_fs_mode == "read":
            cmd += ["--read-size", str(args.wos_fs_read_size)]
        cmd += ["--iterations", str(args.wos_fs_iterations)]
        result = run_command(cmd)
        manifest["steps"].append(
            {"kind": "wos-fs", "command": cmd, "stdout": result.stdout.strip()}
        )

    if args.wos_net_server and args.wos_net_client:
        cmd = [
            str(SCRIPTS / "run_wos_netbench_pair.sh"),
            args.wos_net_server,
            args.wos_net_client,
            "--mode",
            args.wos_net_mode,
            "--payload-size",
            str(args.wos_net_payload_size),
        ]
        if args.wos_net_mode == "pingpong":
            cmd += ["--iterations", str(args.wos_net_iterations)]
        else:
            cmd += ["--total-bytes", str(args.wos_net_total_bytes)]
        result = run_command(cmd)
        manifest["steps"].append(
            {"kind": "wos-net", "command": cmd, "stdout": result.stdout.strip()}
        )

    if args.linux_launcher and args.linux_hosts:
        cmd = [
            str(SCRIPTS / "run_linux_mpi_benchmark.sh"),
            "--launcher",
            args.linux_launcher,
            "--hosts",
            args.linux_hosts,
            "--benchmark",
            args.linux_benchmark,
            "--router-ip",
            args.router_ip,
            "--host-ip",
            args.host_ip,
        ]
        if args.linux_benchmark == "net":
            cmd += [
                "--mode",
                args.linux_net_mode,
                "--payload-size",
                str(args.linux_payload_size),
            ]
            if args.linux_net_mode == "pingpong":
                cmd += ["--iterations", str(args.linux_iterations)]
            else:
                cmd += ["--total-bytes", str(args.linux_total_bytes)]
        else:
            cmd += ["--chunk-size", str(args.linux_chunk_size)]
            if args.linux_file_path:
                cmd += ["--file-path", args.linux_file_path]
            cmd += ["--generate-file-bytes", str(args.linux_generate_file_bytes)]
        result = run_command(cmd)
        manifest["steps"].append(
            {"kind": "linux-mpi", "command": cmd, "stdout": result.stdout.strip()}
        )

    manifest_path = write_suite_manifest(ROOT / args.results_dir, manifest)
    print(str(manifest_path))
    return 0


def prepare_linux_cluster(args: argparse.Namespace) -> int:
    cluster_setup_cmd = [str(SCRIPTS / "cluster_setup.py"), "--setup"]
    if args.cluster_config:
        cluster_setup_cmd += ["--config", args.cluster_config]
    print("[linux-up] configuring host topology")
    run_command(cluster_setup_cmd, stream=True)

    launch_cmd = [str(SCRIPTS / "run_linux_cluster.sh"), str(args.num_vms), "--detach"]
    if args.skip:
        launch_cmd += ["--skip", str(args.skip)]
    print("[linux-up] launching Ubuntu VMs in detached mode")
    run_command(launch_cmd, stream=True)

    summary: dict[str, object] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "router_ip": args.router_ip,
        "host_ip": args.host_ip,
        "num_vms": args.num_vms,
        "skip": args.skip,
        "router_reachable": ping_once(args.router_ip),
        "provisioned_hosts": [],
    }

    if args.provision_hosts:
        if args.wait_seconds > 0:
            print(
                f"[linux-up] waiting {args.wait_seconds}s for guest boot before provisioning"
            )
            subprocess.run(["sleep", str(args.wait_seconds)], check=True)
        print("[linux-up] provisioning Ubuntu guests")
        provision_cmd = [
            str(SCRIPTS / "provision_linux_cluster.sh"),
            *args.provision_hosts,
        ]
        run_command(provision_cmd, stream=True)
        summary["provisioned_hosts"] = args.provision_hosts
    else:
        print("[linux-up] no provisioning requested; returning after detached launch")

    print(json.dumps(summary))
    return 0


def stop_linux_cluster() -> int:
    result = run_command([str(SCRIPTS / "run_linux_cluster.sh"), "--stop"])
    if result.stdout.strip():
        print(result.stdout.strip())
    return 0


def linux_cluster_status() -> int:
    result = subprocess.run(
        [str(SCRIPTS / "run_linux_cluster.sh"), "--status"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    return result.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Host-side benchmark orchestrator for WOS and Ubuntu VM benchmarks."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser(
        "check-lan",
        help="Check router reachability and optional Ubuntu SSH reachability.",
    )
    check_parser.add_argument("--router-ip", default="10.10.0.1")
    check_parser.add_argument("--host-ip", default="10.10.0.100")
    check_parser.add_argument("--ubuntu-host", action="append", default=[])

    linux_up_parser = subparsers.add_parser(
        "linux-up",
        help="Set up host networking, launch the Ubuntu cluster, and optionally provision the guests.",
    )
    linux_up_parser.add_argument("--num-vms", type=int, default=2)
    linux_up_parser.add_argument("--skip", type=int, default=0)
    linux_up_parser.add_argument("--cluster-config")
    linux_up_parser.add_argument("--router-ip", default="10.10.0.1")
    linux_up_parser.add_argument("--host-ip", default="10.10.0.100")
    linux_up_parser.add_argument("--wait-seconds", type=int, default=30)
    linux_up_parser.add_argument(
        "--provision-host", action="append", dest="provision_hosts", default=[]
    )

    subparsers.add_parser(
        "linux-down",
        help="Stop Ubuntu VMs previously launched in detached mode.",
    )

    subparsers.add_parser(
        "linux-status",
        help="Show status for Ubuntu VMs launched in detached mode.",
    )

    suite_parser = subparsers.add_parser(
        "suite", help="Run a host-orchestrated benchmark suite and write a manifest."
    )
    suite_parser.add_argument("--router-ip", default="10.10.0.1")
    suite_parser.add_argument("--host-ip", default="10.10.0.100")
    suite_parser.add_argument(
        "--results-dir", default="benchmarks/results/orchestrator"
    )

    suite_parser.add_argument("--wos-fs-host")
    suite_parser.add_argument("--wos-fs-path")
    suite_parser.add_argument("--wos-fs-mode", choices=["read", "stat"], default="read")
    suite_parser.add_argument("--wos-fs-read-size", type=int, default=65536)
    suite_parser.add_argument("--wos-fs-iterations", type=int, default=8)

    suite_parser.add_argument("--wos-net-server")
    suite_parser.add_argument("--wos-net-client")
    suite_parser.add_argument(
        "--wos-net-mode", choices=["pingpong", "stream"], default="pingpong"
    )
    suite_parser.add_argument("--wos-net-payload-size", type=int, default=1024)
    suite_parser.add_argument("--wos-net-iterations", type=int, default=1000)
    suite_parser.add_argument("--wos-net-total-bytes", type=int, default=268435456)

    suite_parser.add_argument("--linux-launcher")
    suite_parser.add_argument("--linux-hosts")
    suite_parser.add_argument(
        "--linux-benchmark", choices=["net", "file"], default="net"
    )
    suite_parser.add_argument(
        "--linux-net-mode", choices=["pingpong", "stream"], default="pingpong"
    )
    suite_parser.add_argument("--linux-payload-size", type=int, default=1024)
    suite_parser.add_argument("--linux-iterations", type=int, default=10000)
    suite_parser.add_argument("--linux-total-bytes", type=int, default=268435456)
    suite_parser.add_argument("--linux-file-path")
    suite_parser.add_argument("--linux-chunk-size", type=int, default=65536)
    suite_parser.add_argument("--linux-generate-file-bytes", type=int, default=67108864)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "check-lan":
        return check_lan(args.router_ip, args.host_ip, args.ubuntu_host)
    if args.command == "linux-up":
        return prepare_linux_cluster(args)
    if args.command == "linux-down":
        return stop_linux_cluster()
    if args.command == "linux-status":
        return linux_cluster_status()
    if args.command == "suite":
        return run_suite(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
