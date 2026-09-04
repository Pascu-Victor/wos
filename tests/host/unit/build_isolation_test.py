#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KTEST_SETUP = ROOT / "scripts" / "test" / "ktest_setup.py"
KTEST_CONFIG = ROOT / "configs" / "node_ktest.json"
ROOT_CMAKE = ROOT / "CMakeLists.txt"
MODULES_CMAKE = ROOT / "modules" / "CMakeLists.txt"
TOOLS_CMAKE = ROOT / "tools" / "CMakeLists.txt"


EXPECTED_STATE_SUFFIXES = {
    "sysroot": "sysroot",
    "mlibc_build": "mlibc-build",
    "mlibc_conformance_stage": "mlibc-conformance-stage",
    "libcxx_build": "libcxx-build",
    "busybox_build": "busybox-build",
    "busybox_install": "busybox-install",
    "dropbear_build": "dropbear-build",
    "make_build": "make-build",
    "bash_build": "bash-build",
    "zlib_build": "zlib-build",
    "openssl_build": "openssl-build",
    "curl_build": "curl-build",
    "git_build": "git-build",
    "clang_build": "clang-wos-build",
    "ninja_build": "ninja-build",
    "cmake_host_build": "cmake-host-build",
    "cmake_host_install": "host",
    "cmake_build": "cmake-wos-build",
    "python_build": "python-build",
    "meson_build": "meson-build",
    "nasm_build": "nasm-build",
    "ncurses_build": "ncurses-build",
    "nano_build": "nano-build",
    "doom_ascii_build": "doom-ascii-build",
    "tools_output": "tools/bin",
    "boot_disk": "disk.qcow2",
    "rootfs_disk": "mountfs.qcow2",
}


def load_ktest_module():
    cluster_dir = ROOT / "scripts" / "cluster"
    if str(cluster_dir) not in sys.path:
        sys.path.insert(0, str(cluster_dir))
    spec = importlib.util.spec_from_file_location("ktest_setup", KTEST_SETUP)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {KTEST_SETUP}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        raise AssertionError(f"{context}: missing {', '.join(missing)}")


def test_root_cmake_contract() -> None:
    root_cmake = ROOT_CMAKE.read_text(encoding="utf-8")
    state_pos = root_cmake.find('set(WOS_STATE_ROOT "" CACHE PATH')
    modules_pos = root_cmake.find("add_subdirectory(modules)")
    if state_pos < 0 or modules_pos < 0 or state_pos > modules_pos:
        raise AssertionError("WOS_STATE_ROOT defaults must be defined before modules are configured")

    require_tokens(
        root_cmake,
        [
            'option(WOS_REPRODUCIBLE_BUILD "Enable deterministic build and packaging behavior" OFF)',
            'set(SOURCE_DATE_EPOCH "0" CACHE STRING',
            "WOS_HOST_TOOLCHAIN_ROOT=${WOS_HOST_TOOLCHAIN_PATH}",
            "-DWOS_TARGET_TOOLCHAIN_BIN=${WOS_SYSROOT_PATH}/bin",
            "-DWOS_HOST_TOOLCHAIN_BIN=${WOS_HOST_TOOLCHAIN_PATH}/bin",
            "-DWOS_TOOLS_OUTPUT_DIR=${WOS_TOOLS_OUTPUT_DIR}",
            "WOS_REPRODUCIBLE_BUILD=${WOS_REPRODUCIBLE_BUILD}",
            "SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}",
            "${WOS_BUILD_SCRIPTS_DIR}/update_mountfs_disk.sh",
        ],
        "root isolated-build contract",
    )

    for suffix in EXPECTED_STATE_SUFFIXES.values():
        if suffix in {"tools/bin", "disk.qcow2", "mountfs.qcow2"}:
            continue
        if f'"{suffix}")' not in root_cmake:
            raise AssertionError(f"root CMake does not derive isolated state path {suffix!r}")


def test_tools_and_module_contract() -> None:
    tools_cmake = TOOLS_CMAKE.read_text(encoding="utf-8")
    require_tokens(
        tools_cmake,
        [
            "set(WOS_TARGET_TOOLCHAIN_BIN",
            "set(WOS_HOST_TOOLCHAIN_BIN",
            "set(WOS_HOST_TOOLS_BIN",
            "set(WOS_TOOLS_OUTPUT_DIR",
            "set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${WOS_TOOLS_OUTPUT_DIR})",
            "set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${WOS_TOOLS_OUTPUT_DIR})",
        ],
        "host-tools isolated output contract",
    )

    modules_cmake = MODULES_CMAKE.read_text(encoding="utf-8")
    require_tokens(
        modules_cmake,
        [
            "if(WOS_REPRODUCIBLE_BUILD)",
            "-ffile-prefix-map=${CMAKE_BINARY_DIR}=/wos/build",
            "-fdebug-prefix-map=${CMAKE_BINARY_DIR}=/wos/build",
            "-ffile-prefix-map=${WOS_REPO_ROOT}=/wos/src",
            "-fdebug-prefix-map=${WOS_REPO_ROOT}=/wos/src",
            "-ffile-prefix-map=${WOS_STATE_ROOT}=/wos/state",
            "-fdebug-prefix-map=${WOS_STATE_ROOT}=/wos/state",
        ],
        "module reproducible prefix-map contract",
    )


def test_ktest_root_map(module) -> None:
    raw_config = json.loads(KTEST_CONFIG.read_text(encoding="utf-8"))
    roots = module.build_roots(raw_config)
    expected_state_root = ROOT / "ktest-data"
    if roots["state_root"] != expected_state_root:
        raise AssertionError(f"unexpected KTEST state root: {roots['state_root']}")
    for key, suffix in EXPECTED_STATE_SUFFIXES.items():
        expected = expected_state_root / suffix
        if roots[key] != expected:
            raise AssertionError(f"unexpected KTEST {key}: expected {expected}, got {roots[key]}")

    with tempfile.TemporaryDirectory(prefix="wos-isolation-test-") as tmp:
        state_root = Path(tmp) / "state"
        override = Path(tmp) / "custom-python"
        roots = module.build_roots(
            {"build": {"state_root": str(state_root), "python_build": str(override)}}
        )
        for key, suffix in EXPECTED_STATE_SUFFIXES.items():
            expected = override if key == "python_build" else state_root / suffix
            if roots[key] != expected:
                raise AssertionError(f"state-root derivation failed for {key}: {roots[key]}")


def test_ktest_configure_command(module) -> None:
    raw_config = json.loads(KTEST_CONFIG.read_text(encoding="utf-8"))
    roots = module.build_roots(raw_config)
    captured: list[list[str]] = []
    original = module.run_command
    module.run_command = lambda cmd, env=None: captured.append(cmd)
    try:
        module.configure_build(
            ROOT / "build-ktest",
            roots,
            [],
            "Ninja",
            fast=False,
            ubtrap=False,
        )
    finally:
        module.run_command = original

    if len(captured) != 1:
        raise AssertionError(f"expected one configure command, got {len(captured)}")
    command = captured[0]
    required = {
        f"-DWOS_STATE_ROOT={roots['state_root']}",
        f"-DWOS_HOST_TOOLCHAIN_PATH={module.DEFAULT_HOST_TOOLCHAIN}",
        "-DWOS_BUILD_CMAKE_FOR_HOST=OFF",
        "-DWOS_BUILD_HOST_TOOLS=OFF",
    }
    required.update(
        f"-D{cmake_name}={roots[root_name]}"
        for root_name, cmake_name in module.ROOT_CMAKE_PATH_OPTIONS.items()
    )
    missing = sorted(required.difference(command))
    if missing:
        raise AssertionError(f"KTEST configure omits isolated options: {missing}")


def main() -> None:
    module = load_ktest_module()
    test_root_cmake_contract()
    test_tools_and_module_contract()
    test_ktest_root_map(module)
    test_ktest_configure_command(module)
    print("build isolation invariants hold")


if __name__ == "__main__":
    main()
