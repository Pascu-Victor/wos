#!/usr/bin/env python3
"""
Build, package, and run WOS kernel selftests in an isolated diagnostic VM.

This script intentionally uses a separate CMake build directory and separate
disk images so the normal WOS VM images are not mutated while selftests run.
"""

from __future__ import annotations

import argparse
import os
import shutil
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLUSTER_SCRIPTS = ROOT / "scripts" / "cluster"
sys.path.insert(0, str(CLUSTER_SCRIPTS))

import cluster_setup  # noqa: E402
import node_setup  # noqa: E402


DIAGNOSTIC_CMAKE_OPTIONS = [
    "-DCMAKE_BUILD_TYPE=Debug",
    "-DWOS_KCFI=ON",
    "-DWOS_KUBSAN=ON",
    "-DWOS_KASAN=ON",
    "-DWOS_KCOV=ON",
    "-DWOS_KCOV_PANIC_TRACE=ON",
    "-DWOS_KCOV_SOURCE_FRIENDLY=ON",
    "-DWOS_SELFTEST=ON",
    "-DWOS_NET_TRACE=ON",
    "-DWOS_NET_PACKET_DEBUG=ON",
    "-DWOS_MM_PROVENANCE_PERF_CONTROL=OFF",
    "-DWOS_MEMACC_FULL_DEFAULT=ON",
    "-DWOS_PHYS_ALLOC_CALLER_STATS=ON",
    "-DWOS_PHYS_LOCK_DEBUG=ON",
    "-DWOS_KMALLOC_DEBUG_INFO=ON",
    "-DWOS_MANDELBENCH_DEBUG=ON",
]

FAST_CMAKE_OPTION_OVERRIDES = {
    "CMAKE_BUILD_TYPE": "RelWithDebInfo",
    "WOS_KCOV_SOURCE_FRIENDLY": "OFF",
}


def diagnostic_cmake_options(fast: bool) -> list[str]:
    if not fast:
        return list(DIAGNOSTIC_CMAKE_OPTIONS)

    options = []
    for option in DIAGNOSTIC_CMAKE_OPTIONS:
        if not option.startswith("-D"):
            options.append(option)
            continue
        name, separator, value = option[2:].partition("=")
        if not separator:
            options.append(option)
            continue
        options.append(f"-D{name}={FAST_CMAKE_OPTION_OVERRIDES.get(name, value)}")
    return options

BUILD_TARGETS = [
    "check_headers",
    "wos_modules",
    "busybox",
    "dropbear",
]

DEFAULT_SOURCE_SYSROOT = ROOT / "toolchain" / "sysroot"


def print_command(cmd: list[str]):
    print("  $ " + " ".join(shlex.quote(part) for part in cmd))


def run_command(cmd: list[str], env: dict[str, str] | None = None):
    print_command(cmd)
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def load_config(path: Path) -> tuple[dict, dict]:
    raw = node_setup.load_json_config(path)
    return raw, node_setup.normalize_node_spec(raw)


def configured_build_dir(raw_config: dict, override: str | None) -> Path:
    if override:
        return Path(override)
    build_cfg = raw_config.get("build", {})
    return Path(build_cfg.get("dir", "build-ktest"))


def path_from_build_config(raw_config: dict, key: str, default: str) -> Path:
    build_cfg = raw_config.get("build", {})
    return Path(build_cfg.get(key, default))


def configured_kernel_cmdline(raw_config: dict, spec: dict, override: str | None) -> str:
    if override is not None:
        return override
    package_cfg = raw_config.get("package", {})
    return str(package_cfg.get("kernel_cmdline", spec.get("kernel_cmdline", "--selftest")))


def abs_path(path: Path) -> Path:
    return path if path.is_absolute() else ROOT / path


def build_roots(raw_config: dict) -> dict[str, Path]:
    return {
        "sysroot": abs_path(path_from_build_config(raw_config, "sysroot", "ktest-data/sysroot")),
        "mlibc_build": abs_path(path_from_build_config(raw_config, "mlibc_build", "ktest-data/mlibc-build")),
        "busybox_build": abs_path(path_from_build_config(raw_config, "busybox_build", "ktest-data/busybox-build")),
        "busybox_install": abs_path(path_from_build_config(raw_config, "busybox_install", "ktest-data/busybox-install")),
        "dropbear_build": abs_path(path_from_build_config(raw_config, "dropbear_build", "ktest-data/dropbear-build")),
    }


def seed_isolated_sysroot(sysroot: Path, reset: bool):
    if reset and sysroot.exists():
        print(f"Resetting isolated sysroot: {sysroot}")
        shutil.rmtree(sysroot)
    if sysroot.exists():
        return
    if not DEFAULT_SOURCE_SYSROOT.exists():
        raise RuntimeError(f"source sysroot not found at {DEFAULT_SOURCE_SYSROOT}")
    print(f"Seeding isolated sysroot: {sysroot}")
    sysroot.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(DEFAULT_SOURCE_SYSROOT, sysroot, symlinks=True)


def configure_build(
    build_dir: Path,
    roots: dict[str, Path],
    extra_cmake_options: list[str],
    generator: str,
    fast: bool,
):
    cmd = [
        "cmake",
        f"-G{generator}",
        "-B",
        str(build_dir),
        ".",
        f"-DWOS_SYSROOT_PATH={roots['sysroot']}",
        f"-DWOS_MLIBC_BUILD_DIR={roots['mlibc_build']}",
        f"-DWOS_BUSYBOX_BUILD_DIR={roots['busybox_build']}",
        f"-DWOS_BUSYBOX_INSTALL_DIR={roots['busybox_install']}",
        f"-DWOS_DROPBEAR_BUILD_DIR={roots['dropbear_build']}",
        "-DWOS_SKIP_LIBCXX_INSTALL=ON",
        *diagnostic_cmake_options(fast),
        *extra_cmake_options,
    ]
    run_command(cmd)


def build_artifacts(build_dir: Path):
    run_command(["cmake", "--build", str(build_dir), "--target", *BUILD_TARGETS])


def package_disks(spec: dict, build_dir: Path, roots: dict[str, Path], kernel_cmdline: str):
    vm_cfg = spec["vm"]
    boot_disk = Path(vm_cfg.get("disk0", "ktest-data/disk.qcow2"))
    rootfs_disk = Path(vm_cfg.get("disk1", "ktest-data/mountfs.qcow2"))

    env = os.environ.copy()
    env["WOS_BUILD_DIR"] = str(build_dir)
    env["WOS_SYSROOT_PATH"] = str(roots["sysroot"])
    env["WOS_BUSYBOX_INSTALL_DIR"] = str(roots["busybox_install"])
    env["WOS_BOOT_DISK"] = str(boot_disk)
    env["WOS_ROOTFS_DISK"] = str(rootfs_disk)
    env["WOS_KERNEL_CMDLINE"] = kernel_cmdline

    print(
        "Packaging isolated KTEST disks "
        f"(boot={boot_disk}, rootfs={rootfs_disk}, cmdline={kernel_cmdline!r})"
    )
    run_command(["scripts/build/create_mountfs_disk.sh"], env=env)
    run_command(["scripts/build/make_image.sh"], env=env)


def main() -> int:
    os.chdir(ROOT)

    parser = argparse.ArgumentParser(
        description="Build, package, and run an isolated WOS kernel selftest VM",
        epilog=(
            "By default this uses build-ktest/ plus ktest-data/ for disks, "
            "sysroot, mlibc, BusyBox, and Dropbear build roots, so the normal "
            "build/, toolchain/sysroot, disk.qcow2, and mountfs.qcow2 artifacts "
            "are not touched."
        ),
    )
    parser.add_argument("--config", default="configs/node.json", help="Single-node VM spec")
    parser.add_argument("--build-dir", help="Override build directory from node config")
    parser.add_argument(
        "--kernel-cmdline",
        help="Override package kernel cmdline (default from node config, usually --selftest)",
    )
    parser.add_argument(
        "--cmake-option",
        action="append",
        default=[],
        help="Additional CMake option for the diagnostic build; may be repeated",
    )
    parser.add_argument("--generator", default="Ninja", help="CMake generator")
    parser.add_argument(
        "--reset-sysroot",
        action="store_true",
        help="Delete and re-seed the isolated KTEST sysroot before building",
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Configure/build the isolated diagnostic artifacts, then stop",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Use RelWithDebInfo and disable KCOV source-friendly codegen for faster selftest runs",
    )
    parser.add_argument("--no-build", action="store_true", help="Skip configure/build")
    parser.add_argument("--no-package", action="store_true", help="Skip disk packaging")
    parser.add_argument("--no-launch", action="store_true", help="Set up topology but do not launch")
    parser.add_argument("--teardown", action="store_true", help="Tear down the single-node topology")
    parser.add_argument(
        "--tcg",
        nargs="?",
        const="",
        default=None,
        metavar="LEVEL",
        help="Launch with TCG instead of KVM. Optional level: int, full",
    )
    parser.add_argument(
        "--debug-node",
        action="store_true",
        help="Launch the KTEST VM paused with a GDB stub",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    raw_config, spec = load_config(config_path)
    build_dir = configured_build_dir(raw_config, args.build_dir)
    roots = build_roots(raw_config)
    kernel_cmdline = configured_kernel_cmdline(raw_config, spec, args.kernel_cmdline)
    cluster_config = node_setup.cluster_config_from_node_spec(spec)

    if args.teardown:
        cluster_setup.ensure_sudo()
        cluster_setup.teardown(cluster_config)
        return 0

    if not args.no_build:
        seed_isolated_sysroot(roots["sysroot"], args.reset_sysroot)
        configure_build(build_dir, roots, args.cmake_option, args.generator, args.fast)
        build_artifacts(build_dir)
    if args.build_only:
        return 0

    if not args.no_package:
        seed_isolated_sysroot(roots["sysroot"], False)
        package_disks(spec, build_dir, roots, kernel_cmdline)

    cluster_setup.ensure_sudo()
    if args.no_launch:
        cluster_setup.setup(cluster_config)
        return 0

    debug_nodes = {node_setup.node_id(spec)} if args.debug_node else None
    cluster_setup.launch(cluster_config, tcg_level=args.tcg, debug_nodes=debug_nodes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
