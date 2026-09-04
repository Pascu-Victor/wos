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
DEBUG_SCRIPTS = ROOT / "scripts" / "debug"
sys.path.insert(0, str(CLUSTER_SCRIPTS))
sys.path.insert(0, str(DEBUG_SCRIPTS))

import cluster_setup  # noqa: E402
import node_setup  # noqa: E402
import wosincident  # noqa: E402


DIAGNOSTIC_CMAKE_OPTIONS = [
    "-DCMAKE_BUILD_TYPE=Debug",
    "-DWOS_KERNEL_LIBCXX_HARDENING_MODE=debug",
    "-DWOS_KCFI=ON",
    "-DWOS_KUBSAN=ON",
    "-DWOS_KASAN=ON",
    "-DWOS_KCOV=ON",
    "-DWOS_KCOV_PANIC_TRACE=ON",
    "-DWOS_KCOV_SOURCE_FRIENDLY=ON",
    "-DWOS_SELFTEST=ON",
    "-DWOS_SCHED_TRANSITION_VALIDATION=ON",
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
    "WOS_KERNEL_LIBCXX_HARDENING_MODE": "fast",
    "WOS_KCOV_SOURCE_FRIENDLY": "OFF",
}

UBTRAP_CMAKE_OPTION_OVERRIDES = {
    "WOS_KUBSAN": "OFF",
    "WOS_KERNEL_UBSAN_TRAP": "ON",
}

def diagnostic_cmake_options(fast: bool, ubtrap: bool) -> list[str]:
    overrides: dict[str, str] = {}
    if fast:
        overrides.update(FAST_CMAKE_OPTION_OVERRIDES)
    if ubtrap:
        overrides.update(UBTRAP_CMAKE_OPTION_OVERRIDES)

    if not overrides:
        return list(DIAGNOSTIC_CMAKE_OPTIONS)

    seen_options: set[str] = set()
    options = []
    for option in DIAGNOSTIC_CMAKE_OPTIONS:
        if not option.startswith("-D"):
            options.append(option)
            continue
        name, separator, value = option[2:].partition("=")
        if not separator:
            options.append(option)
            continue
        seen_options.add(name)
        options.append(f"-D{name}={overrides.get(name, value)}")

    for name, value in overrides.items():
        if name not in seen_options:
            options.append(f"-D{name}={value}")
    return options

BUILD_TARGETS = [
    "check_headers",
    "wos_modules",
    "busybox",
    "dropbear",
    "gnu_make",
    "bash_for_wos",
    "cmake_for_wos",
    "python_for_wos",
    "doom_ascii_for_wos",
    "mlibc_conformance",
]

DEFAULT_SOURCE_SYSROOT = ROOT / "toolchain" / "sysroot"
DEFAULT_HOST_TOOLCHAIN = ROOT / "toolchain" / "host"

ROOT_CMAKE_PATH_OPTIONS = {
    "sysroot": "WOS_SYSROOT_PATH",
    "mlibc_build": "WOS_MLIBC_BUILD_DIR",
    "mlibc_conformance_stage": "WOS_MLIBC_CONFORMANCE_STAGE_DIR",
    "libcxx_build": "WOS_LIBCXX_BUILD_DIR",
    "busybox_build": "WOS_BUSYBOX_BUILD_DIR",
    "busybox_install": "WOS_BUSYBOX_INSTALL_DIR",
    "dropbear_build": "WOS_DROPBEAR_BUILD_DIR",
    "make_build": "WOS_MAKE_BUILD_DIR",
    "bash_build": "WOS_BASH_BUILD_DIR",
    "zlib_build": "WOS_ZLIB_BUILD_DIR",
    "openssl_build": "WOS_OPENSSL_BUILD_DIR",
    "curl_build": "WOS_CURL_BUILD_DIR",
    "git_build": "WOS_GIT_BUILD_DIR",
    "clang_build": "WOS_CLANG_FOR_WOS_BUILD_DIR",
    "ninja_build": "WOS_NINJA_BUILD_DIR",
    "cmake_host_build": "WOS_CMAKE_FOR_HOST_BUILD_DIR",
    "cmake_host_install": "WOS_CMAKE_FOR_HOST_INSTALL_DIR",
    "cmake_build": "WOS_CMAKE_FOR_WOS_BUILD_DIR",
    "python_build": "WOS_PYTHON_BUILD_DIR",
    "meson_build": "WOS_MESON_BUILD_DIR",
    "nasm_build": "WOS_NASM_BUILD_DIR",
    "ncurses_build": "WOS_NCURSES_BUILD_DIR",
    "nano_build": "WOS_NANO_BUILD_DIR",
    "doom_ascii_build": "WOS_DOOM_ASCII_BUILD_DIR",
    "tools_output": "WOS_TOOLS_OUTPUT_DIR",
    "boot_disk": "WOS_BOOT_DISK",
    "rootfs_disk": "WOS_ROOTFS_DISK",
}


def print_command(cmd: list[str]):
    print("  $ " + " ".join(shlex.quote(part) for part in cmd))


def run_command(cmd: list[str], env: dict[str, str] | None = None):
    print_command(cmd)
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def isolated_build_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in ("CFLAGS", "CXXFLAGS", "LDFLAGS"):
        env.pop(name, None)
    return env


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
    build_cfg = raw_config.get("build", {})
    state_root = abs_path(Path(build_cfg.get("state_root", "ktest-data")))

    def state_path(key: str, leaf: str) -> Path:
        configured = build_cfg.get(key)
        return abs_path(Path(configured)) if configured is not None else state_root / leaf

    raw_node = raw_config.get("node", raw_config)
    vm_cfg = raw_node.get("vm", {})
    return {
        "state_root": state_root,
        "sysroot": state_path("sysroot", "sysroot"),
        "mlibc_build": state_path("mlibc_build", "mlibc-build"),
        "mlibc_conformance_stage": state_path("mlibc_conformance_stage", "mlibc-conformance-stage"),
        "libcxx_build": state_path("libcxx_build", "libcxx-build"),
        "busybox_build": state_path("busybox_build", "busybox-build"),
        "busybox_install": state_path("busybox_install", "busybox-install"),
        "dropbear_build": state_path("dropbear_build", "dropbear-build"),
        "make_build": state_path("make_build", "make-build"),
        "bash_build": state_path("bash_build", "bash-build"),
        "zlib_build": state_path("zlib_build", "zlib-build"),
        "openssl_build": state_path("openssl_build", "openssl-build"),
        "curl_build": state_path("curl_build", "curl-build"),
        "git_build": state_path("git_build", "git-build"),
        "clang_build": state_path("clang_build", "clang-wos-build"),
        "ninja_build": state_path("ninja_build", "ninja-build"),
        "cmake_host_build": state_path("cmake_host_build", "cmake-host-build"),
        "cmake_host_install": state_path("cmake_host_install", "host"),
        "cmake_build": state_path("cmake_build", "cmake-wos-build"),
        "python_build": state_path("python_build", "python-build"),
        "meson_build": state_path("meson_build", "meson-build"),
        "nasm_build": state_path("nasm_build", "nasm-build"),
        "ncurses_build": state_path("ncurses_build", "ncurses-build"),
        "nano_build": state_path("nano_build", "nano-build"),
        "doom_ascii_build": state_path("doom_ascii_build", "doom-ascii-build"),
        "tools_output": state_path("tools_output", "tools/bin"),
        "boot_disk": abs_path(Path(vm_cfg["disk0"])) if "disk0" in vm_cfg else state_root / "disk.qcow2",
        "rootfs_disk": abs_path(Path(vm_cfg["disk1"])) if "disk1" in vm_cfg else state_root / "mountfs.qcow2",
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
    ubtrap: bool,
):
    cmd = [
        "cmake",
        f"-G{generator}",
        "-B",
        str(build_dir),
        ".",
        f"-DWOS_STATE_ROOT={roots['state_root']}",
        f"-DWOS_HOST_TOOLCHAIN_PATH={DEFAULT_HOST_TOOLCHAIN}",
        *(f"-D{cmake_name}={roots[root_name]}" for root_name, cmake_name in ROOT_CMAKE_PATH_OPTIONS.items()),
        "-DCMAKE_C_FLAGS:STRING=",
        "-DCMAKE_CXX_FLAGS:STRING=",
        "-DCMAKE_EXE_LINKER_FLAGS:STRING=",
        "-DCMAKE_SHARED_LINKER_FLAGS:STRING=",
        "-DCMAKE_MODULE_LINKER_FLAGS:STRING=",
        "-DWOS_BUILD_BASH_FOR_WOS=ON",
        "-DWOS_BUILD_CMAKE_FOR_HOST=OFF",
        "-DWOS_BUILD_PYTHON_FOR_WOS=ON",
        "-DWOS_BUILD_MLIBC_CONFORMANCE=ON",
        "-DWOS_BUILD_HOST_TOOLS=OFF",
        "-DWOS_SKIP_LIBCXX_INSTALL=ON",
        *diagnostic_cmake_options(fast, ubtrap),
        *extra_cmake_options,
    ]
    run_command(cmd, env=isolated_build_env())


def build_artifacts(build_dir: Path):
    run_command(["cmake", "--build", str(build_dir), "--target", *BUILD_TARGETS], env=isolated_build_env())


def package_disks(build_dir: Path, roots: dict[str, Path], kernel_cmdline: str):
    boot_disk = roots["boot_disk"]
    rootfs_disk = roots["rootfs_disk"]

    env = os.environ.copy()
    env["WOS_BUILD_DIR"] = str(build_dir)
    env["WOS_SYSROOT_PATH"] = str(roots["sysroot"])
    env["WOS_BUSYBOX_INSTALL_DIR"] = str(roots["busybox_install"])
    env["WOS_BOOT_DISK"] = str(boot_disk)
    env["WOS_ROOTFS_DISK"] = str(rootfs_disk)
    env["WOS_KERNEL_CMDLINE"] = kernel_cmdline
    # Give the kernel selftest suite two independent disposable XFS devices.
    # Normal create_mountfs_disk.sh callers retain the single-partition layout.
    env["WOS_KTEST_SECOND_XFS"] = "1"

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
    parser.add_argument("--config", default="configs/node_ktest.json", help="Single-node VM spec")
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
    parser.add_argument(
        "--ubtrap",
        action="store_true",
        help="Use runtime-free trap-mode kernel UBSan instead of report-mode KUBSan",
    )
    parser.add_argument("--no-build", action="store_true", help="Skip configure/build")
    parser.add_argument("--no-package", action="store_true", help="Skip disk packaging")
    parser.add_argument("--no-launch", action="store_true", help="Set up topology but do not launch")
    parser.add_argument(
        "--no-setup",
        action="store_true",
        help="Launch without privileged topology setup; assumes bridge/TAP state already exists",
    )
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
    parser.add_argument(
        "--incident-output",
        metavar="PATH",
        help="Capture launch artifacts into a new .wosincident directory or archive",
    )
    parser.add_argument(
        "--incident-archive",
        action="store_true",
        help="With --incident-output, write deterministic USTAR instead of a directory",
    )
    parser.add_argument(
        "--incident-coverage-manifest",
        action="append",
        default=[],
        metavar="PATH",
        help="Coverage run manifest to import into the incident; may be repeated",
    )
    parser.add_argument(
        "--live-debug-descriptor",
        default="ktest-data/live-debug.json",
        metavar="PATH",
        help=(
            "Publish the private live WOSDBG runtime descriptor at PATH while "
            "the VM runs (default: ktest-data/live-debug.json)"
        ),
    )
    args = parser.parse_args()
    if args.no_setup and (args.teardown or args.no_launch or args.build_only):
        parser.error("--no-setup is only valid when launching")
    if args.incident_archive and not args.incident_output:
        parser.error("--incident-archive requires --incident-output")
    if args.incident_coverage_manifest and not args.incident_output:
        parser.error("incident evidence options require --incident-output")
    if args.incident_output and (args.teardown or args.no_launch or args.build_only):
        parser.error("--incident-output is only valid when launching")

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    raw_config, spec = load_config(config_path)
    build_dir = configured_build_dir(raw_config, args.build_dir)
    roots = build_roots(raw_config)
    kernel_cmdline = configured_kernel_cmdline(raw_config, spec, args.kernel_cmdline)
    cluster_config = node_setup.cluster_config_from_node_spec(spec)

    if args.teardown:
        with cluster_setup.cluster_launch_guard(reject_running_qemus=False):
            cluster_setup.remove_live_debug_descriptor(args.live_debug_descriptor)
            cluster_setup.ensure_sudo()
            cluster_setup.teardown(cluster_config)
        return 0

    incident_snapshots = (
        wosincident.snapshot_node_logs([spec], tcg_level=args.tcg, repo_root=ROOT)
        if args.incident_output
        else None
    )
    run_complete = False
    try:
        if not args.no_build:
            seed_isolated_sysroot(roots["sysroot"], args.reset_sysroot)
            configure_build(build_dir, roots, args.cmake_option, args.generator, args.fast, args.ubtrap)
            build_artifacts(build_dir)
        if args.build_only:
            run_complete = True
            return 0

        if not args.no_package:
            seed_isolated_sysroot(roots["sysroot"], False)
            package_disks(build_dir, roots, kernel_cmdline)

        if args.no_launch:
            cluster_setup.ensure_sudo()
            cluster_setup.setup(cluster_config)
            run_complete = True
            return 0

        if not args.no_setup:
            cluster_setup.ensure_sudo()
        debug_nodes = {node_setup.node_id(spec)} if args.debug_node else None
        cluster_setup.launch(
            cluster_config,
            tcg_level=args.tcg,
            debug_nodes=debug_nodes,
            skip_setup=args.no_setup,
            live_debug_descriptor=args.live_debug_descriptor,
            topology_kind="ktest",
            config_path=config_path,
            build_dir=build_dir,
            symbol_path=build_dir / "modules/kern/wos",
        )
        run_complete = True
        return 0
    finally:
        if args.incident_output:
            exception_type, _exception, _traceback = sys.exc_info()
            wosincident.capture_safely(
                output=Path(args.incident_output),
                archive=args.incident_archive,
                kind="ktest",
                config=raw_config,
                config_path=config_path,
                node_specs=[spec],
                build_dir=build_dir,
                tcg_level=args.tcg,
                profile=wosincident.detect_build_profile(
                    abs_path(build_dir),
                    fallback="RelWithDebInfo" if args.fast else "Debug",
                ),
                coverage_manifests=[Path(path) for path in args.incident_coverage_manifest],
                snapshots=incident_snapshots,
                run_complete=run_complete,
                run_error=(
                    None
                    if exception_type is None
                    else f"{exception_type.__name__} during KTEST execution"
                ),
                repo_root=ROOT,
            )


if __name__ == "__main__":
    raise SystemExit(main())
