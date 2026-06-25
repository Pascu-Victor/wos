#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WOS_TOOLCHAIN = ROOT / "tools" / "wos-toolchain.sh"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def compiler_rt_cmake_block(source: str) -> str:
    match = re.search(
        r"build_compiler_rt\(\)\s*\{(?P<body>.*?)\n\}",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail("tools/wos-toolchain.sh is missing build_compiler_rt")
    return match.group("body")


def test_compiler_rt_runs_real_cmake_checks_without_forced_response_files() -> None:
    source = WOS_TOOLCHAIN.read_text()
    block = compiler_rt_cmake_block(source)

    require_tokens(
        block,
        [
            'env -u LDFLAGS cmake -G Ninja',
            "-DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY",
            "-DCOMPILER_RT_BUILD_SANITIZERS=$build_sanitizers",
            '"${COMPILER_RT_CMAKE_SYSROOT_ARGS[@]}"',
        ],
        "compiler-rt bootstrap CMake configuration",
    )

    forbidden = [
        "CMAKE_NINJA_FORCE_RESPONSE_FILE",
        "COMPILER_RT_LINK_FLAGS",
        "COMPILER_RT_TARGET_HAS_ATOMICS=ON",
        "COMPILER_RT_TARGET_HAS_FCNTL_LCK=ON",
        "COMPILER_RT_TARGET_HAS_FLOCK=ON",
        "COMPILER_RT_TARGET_HAS_UNAME=ON",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("compiler-rt bootstrap must not force or preseed CMake checks: " + ", ".join(present))


def test_compiler_rt_sanitizers_are_built_after_mlibc() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            "build_compiler_rt OFF",
            "ninja && ninja install",
            "build_compiler_rt ON",
            'if [ "$require_sanitizers" = "ON" ]; then',
        ],
        "compiler-rt staged build",
    )

    bootstrap_index = source.index("build_compiler_rt OFF")
    mlibc_install_index = source.index("cd $B/mlibc-build")
    sanitizer_index = source.index("build_compiler_rt ON")
    libcxx_index = source.index("# 6. Build libcxx")
    if not bootstrap_index < mlibc_install_index < sanitizer_index < libcxx_index:
        fail("compiler-rt must build builtins/profile before mlibc and ASAN after mlibc")


def test_native_wos_compiler_rt_does_not_link_with_workspace_sysroot() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            'COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT=$SYSROOT")',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT_COMPILE=$SYSROOT")',
            "unset LDFLAGS\nbuild_compiler_rt OFF",
            "unset LDFLAGS\nbuild_compiler_rt ON",
        ],
        "native WOS compiler-rt sysroot split",
    )


if __name__ == "__main__":
    test_compiler_rt_runs_real_cmake_checks_without_forced_response_files()
    test_compiler_rt_sanitizers_are_built_after_mlibc()
    test_native_wos_compiler_rt_does_not_link_with_workspace_sysroot()
    print("WOS toolchain bootstrap source invariants hold")
