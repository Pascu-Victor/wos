#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BOOTSTRAP = ROOT / "tools" / "bootstrap.sh"
CCACHE_ENV = ROOT / "tools" / "ccache_env.sh"
HOST_TOOLCHAIN = ROOT / "tools" / "host-toolchain.sh"
ROOT_CMAKE = ROOT / "CMakeLists.txt"
BUILD_WOS = ROOT / "scripts" / "dev" / "build_wos.sh"
NINJA_WITH_JOBS = ROOT / "scripts" / "build" / "ninja_with_jobs.sh"
WOS_TOOLCHAIN = ROOT / "tools" / "wos-toolchain.sh"
WOS_LIBCXX_BUILD = ROOT / "scripts" / "build" / "build_libcxx_for_wos.sh"
WOS_CLANG_BUILD = ROOT / "scripts" / "build" / "build_clang_for_wos.sh"
WOS_BUSYBOX_BUILD = ROOT / "scripts" / "build" / "build_busybox.sh"
WOS_CMAKE_BUILD = ROOT / "scripts" / "build" / "build_cmake_for_wos.sh"
WOS_CURL_BUILD = ROOT / "scripts" / "build" / "build_curl_for_wos.sh"
WOS_DROPBEAR_BUILD = ROOT / "scripts" / "build" / "build_dropbear.sh"
WOS_GIT_BUILD = ROOT / "scripts" / "build" / "build_git_for_wos.sh"
WOS_MESON_BUILD = ROOT / "scripts" / "build" / "build_meson_for_wos.sh"
WOS_MLIBC_BUILD = ROOT / "scripts" / "build" / "build_mlibc.sh"
WOS_NASM_BUILD = ROOT / "scripts" / "build" / "build_nasm_for_wos.sh"
WOS_NINJA_BUILD = ROOT / "scripts" / "build" / "build_ninja_for_wos.sh"
WOS_TLS_BUILD = ROOT / "scripts" / "build" / "build_openssl_for_wos.sh"
WOS_PYTHON_BUILD = ROOT / "scripts" / "build" / "build_python_for_wos.sh"
CMAKE_BUILD_UTILITIES = ROOT / "toolchain" / "src" / "cmake" / "Source" / "Modules" / "CMakeBuildUtilities.cmake"
CMAKE_THIRD_PARTY_CHECKS = ROOT / "toolchain" / "src" / "cmake" / "Utilities" / "cmThirdPartyChecks.cmake"
MLIBC_STDIO = ROOT / "toolchain" / "src" / "mlibc" / "options" / "ansi" / "generic" / "stdio.cpp"
MLIBC_SSCANF_TEST = ROOT / "toolchain" / "src" / "mlibc" / "tests" / "ansi" / "sscanf.c"
MLIBC_NAMESER = ROOT / "toolchain" / "src" / "mlibc" / "options" / "bsd" / "generic" / "arpa-nameser.cpp"
MLIBC_RESOLV = ROOT / "toolchain" / "src" / "mlibc" / "options" / "glibc" / "generic" / "resolv.cpp"
MLIBC_SYS_RESOURCE = ROOT / "toolchain" / "src" / "mlibc" / "options" / "posix" / "generic" / "sys-resource.cpp"
WOS_SYSDEPS_CPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "sysdeps.cpp"
WOS_SYSDEPS_HPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "mlibc" / "sysdeps.hpp"
WOS_PROCESS_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "process.h"
WOS_PROCESS_CALLNUMS = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "callnums" / "process.h"
KERNEL_PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
KERNEL_PROCESS_CALLNUMS = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "process.h"
WOS_JOB_HELPER_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    WOS_BUSYBOX_BUILD,
    WOS_CLANG_BUILD,
    ROOT / "scripts" / "build" / "build_cmake_for_host.sh",
    ROOT / "scripts" / "build" / "build_cmake_for_wos.sh",
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    WOS_DROPBEAR_BUILD,
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    ROOT / "scripts" / "build" / "build_make.sh",
    WOS_MLIBC_BUILD,
    WOS_NASM_BUILD,
    WOS_NINJA_BUILD,
    WOS_TLS_BUILD,
    WOS_PYTHON_BUILD,
    ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
    WOS_TOOLCHAIN,
]
WOS_MAKE_JOB_HELPER_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    WOS_BUSYBOX_BUILD,
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    WOS_DROPBEAR_BUILD,
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    ROOT / "scripts" / "build" / "build_make.sh",
    WOS_NASM_BUILD,
    WOS_TLS_BUILD,
    WOS_PYTHON_BUILD,
    ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
]
WOS_NINJA_JOB_HELPER_USERS = [
    WOS_LIBCXX_BUILD,
    WOS_CLANG_BUILD,
    ROOT / "scripts" / "build" / "build_cmake_for_host.sh",
    WOS_CMAKE_BUILD,
    WOS_MLIBC_BUILD,
    WOS_NINJA_BUILD,
    WOS_TOOLCHAIN,
]
WOS_BUSYBOX_TAR_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    ROOT / "scripts" / "build" / "build_make.sh",
    WOS_MESON_BUILD,
    WOS_NASM_BUILD,
    WOS_TLS_BUILD,
    ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
]
WOS_TARBALL_DOWNLOAD_USERS = {
    "Bash": ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    "curl": ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    "Git": ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    "GNU make": ROOT / "scripts" / "build" / "build_make.sh",
    "LibreSSL": WOS_TLS_BUILD,
    "Meson": WOS_MESON_BUILD,
    "NASM": WOS_NASM_BUILD,
    "zlib": ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
}
WOS_SOURCE_COPY_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    WOS_MESON_BUILD,
    WOS_TLS_BUILD,
]


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
            'wos_timed_step "configure" "$detail_label"',
            'wos_run_env_in_dir "$B/compiler-rt-build"',
            "-u LDFLAGS",
            "cmake -G Ninja",
            "-DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY",
            "-DCOMPILER_RT_BUILD_SANITIZERS=$build_sanitizers",
            '"${COMPILER_RT_CMAKE_SYSROOT_ARGS[@]}"',
            '"${COMPILER_RT_CMAKE_CACHE_ARGS[@]}"',
        ],
        "compiler-rt bootstrap CMake configuration",
    )
    require_tokens(
        source,
        [
            "COMPILER_RT_CMAKE_CACHE_ARGS=(",
            "-DCOMPILER_RT_HAS_FPIC_FLAG=1",
            "-DCOMPILER_RT_HAS_FPIE_FLAG=1",
            "-DCOMPILER_RT_HAS_FNO_BUILTIN_FLAG=1",
            "-DCOMPILER_RT_HAS_FNO_SANITIZE_SAFE_STACK_FLAG=1",
            "-DCOMPILER_RT_HAS_WBUILTIN_DECLARATION_MISMATCH_FLAG=False",
            "-DCOMPILER_RT_HAS_ZL_FLAG=False",
            "-DCOMPILER_RT_HAS_ATOMIC_KEYWORD=True",
            "-DCOMPILER_RT_HAS_ASM_LSE=False",
            "-DCOMPILER_RT_HAS_AARCH64_SME=False",
            "-DCXX_SUPPORTS_UNWINDLIB_NONE_FLAG=1",
            "-DC_SUPPORTS_NODEFAULTLIBS_FLAG=1",
        ],
        "compiler-rt bootstrap stable clang capability preseeds",
    )

    forbidden = [
        "CMAKE_NINJA_FORCE_RESPONSE_FILE",
        "COMPILER_RT_LINK_FLAGS",
        "COMPILER_RT_HAS_LIBC=ON",
        "COMPILER_RT_HAS_LIBC=1",
        "COMPILER_RT_HAS_LIBPTHREAD=ON",
        "COMPILER_RT_HAS_LIBPTHREAD=1",
        "COMPILER_RT_HAS_LIBUNWIND=ON",
        "COMPILER_RT_HAS_LIBUNWIND=1",
        "COMPILER_RT_TARGET_HAS_ATOMICS=ON",
        "COMPILER_RT_TARGET_HAS_ATOMICS=1",
        "COMPILER_RT_TARGET_HAS_FCNTL_LCK=ON",
        "COMPILER_RT_TARGET_HAS_FCNTL_LCK=1",
        "COMPILER_RT_TARGET_HAS_FLOCK=ON",
        "COMPILER_RT_TARGET_HAS_FLOCK=1",
        "COMPILER_RT_TARGET_HAS_UNAME=ON",
        "COMPILER_RT_TARGET_HAS_UNAME=1",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("compiler-rt bootstrap must not force response files or runtime target checks: " + ", ".join(present))


def test_compiler_rt_sanitizers_are_built_after_mlibc() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            "build_compiler_rt OFF",
            'wos_timed_step "build" "mlibc"',
            'wos_timed_step "install" "mlibc"',
            'wos_run_in_dir "$B/mlibc-build"',
            'ninja -j"$COMPILER_RT_NINJA_JOBS" install',
            "build_compiler_rt ON",
            'if [ "$require_sanitizers" = "ON" ]; then',
        ],
        "compiler-rt staged build",
    )
    compiler_rt_body = compiler_rt_cmake_block(source)
    if 'ninja -j"$COMPILER_RT_NINJA_JOBS" && ninja -j"$COMPILER_RT_NINJA_JOBS" install' in compiler_rt_body:
        fail("compiler-rt bootstrap must not relink archives in a second immediate Ninja invocation on WOS")

    bootstrap_index = source.index("build_compiler_rt OFF")
    mlibc_install_index = source.index('wos_timed_step "install" "mlibc"')
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
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            'COMPILER_RT_NINJA_JOBS="$WOS_NINJA_JOBS"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT_COMPILE=$SYSROOT")',
            "unset LDFLAGS\nbuild_compiler_rt OFF",
            "unset LDFLAGS\nbuild_compiler_rt ON",
        ],
        "native WOS compiler-rt sysroot split",
    )


def test_wos_toolchain_records_bootstrap_phase_timings() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            'BOOTSTRAP_DETAIL_TSV="${WOS_BOOTSTRAP_DETAIL_TSV:-}"',
            "bootstrap_now_ms()",
            "bootstrap_timestamp_utc()",
            'local epoch="${EPOCHREALTIME:-}"',
            'if [ -n "$epoch" ]; then',
            'TZ=UTC printf -v prefix',
            "bootstrap_detail_header()",
            '"timestamp_utc" "phase" "label" "elapsed_ms" "status"',
            "bootstrap_record_detail()",
            "bootstrap_phase_start()",
            "bootstrap_phase_end()",
            "bootstrap_phase_fail()",
            'bootstrap_record_detail "$phase" "$label" "$((end_ms - start_ms))" "fail:$status"',
            "trap bootstrap_phase_fail ERR",
        ],
        "WOS bootstrap phase timing support",
    )

    phases = re.findall(r"^\s*bootstrap_phase_start\s+([0-9]+)\s+", source, flags=re.MULTILINE)
    expected = [str(phase) for phase in range(1, 19)]
    if phases != expected:
        fail(f"WOS bootstrap must time phases 1 through 18 in order, got {phases}")

    phase_end_count = len(re.findall(r"^\s*bootstrap_phase_end\s*$", source, flags=re.MULTILINE))
    if phase_end_count != 18:
        fail(f"WOS bootstrap must close each timed phase exactly once, got {phase_end_count}")


def test_shared_build_timing_avoids_python_when_epochrealtime_exists() -> None:
    source = CCACHE_ENV.read_text()
    require_tokens(
        source,
        [
            "wos_now_ms()",
            "wos_timestamp_utc()",
            'local epoch="${EPOCHREALTIME:-}"',
            'if [ -n "$epoch" ]; then',
            'TZ=UTC printf -v prefix',
            'printf \'%s\\n\' "$((10#$seconds * 1000 + 10#${fraction:0:3}))"',
            "python3 - <<'PY'",
        ],
        "shared WOS build timing fast path",
    )

    now_start = source.index("wos_now_ms()")
    timestamp_start = source.index("wos_timestamp_utc()")
    record_start = source.index("wos_record_detail()")
    timing_block = source[now_start:record_start]
    if timing_block.count("python3 - <<'PY'") != 2:
        fail("shared timing helpers should keep Python only as a fallback")
    if timing_block.find("python3 - <<'PY'") < timing_block.find('if [ -n "$epoch" ]; then'):
        fail("shared timing helpers must try EPOCHREALTIME before Python")


def test_native_wos_build_defaults_keep_target_ports_enabled() -> None:
    source = ROOT_CMAKE.read_text()
    require_tokens(
        source,
        [
            "set(WOS_PORT_BUILD_DEFAULT ON)",
            "set(WOS_BUILD_CMAKE_FOR_HOST_DEFAULT ${WOS_PORT_BUILD_DEFAULT})",
            "if(WOS_NATIVE_SYSTEM_BUILD)\n    set(WOS_BUILD_CMAKE_FOR_HOST_DEFAULT OFF)\nendif()",
            "set(WOS_BUILD_DISK_IMAGES_DEFAULT ON)",
            "if(WOS_NATIVE_SYSTEM_BUILD)\n    set(WOS_BUILD_DISK_IMAGES_DEFAULT OFF)\nendif()",
            'option(WOS_BUILD_WOSDBG "Build the Qt6-based wosdbg host tool" ${WOS_BUILD_WOSDBG_DEFAULT})',
            'option(WOS_BUILD_DISK_IMAGES "Build boot/rootfs qcow2 images with qemu-img, mtools, and libguestfs" ${WOS_BUILD_DISK_IMAGES_DEFAULT})',
            'option(WOS_BUILD_HOST_TOOLS "Configure and build host-side WOS tools" ${WOS_BUILD_HOST_TOOLS_DEFAULT})',
            "if(WOS_BUILD_HOST_TOOLS)\n    ExternalProject_Add(wos_tools_build",
            'add_custom_target(wos_tools_build_always\n        COMMENT "Host-side WOS tools disabled."\n    )',
            'option(WOS_BUILD_CLANG_FOR_WOS "Build a native WOS clang/lld toolchain and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})',
            'option(WOS_BUILD_CMAKE_FOR_HOST "Build host-side CMake from the WOS fork and install it into toolchain/host" ${WOS_BUILD_CMAKE_FOR_HOST_DEFAULT})',
            'option(WOS_BUILD_CMAKE_FOR_WOS "Build native WOS CMake tools and stage them into the rootfs" ${WOS_PORT_BUILD_DEFAULT})',
            'option(WOS_BUILD_GIT_FOR_WOS "Build native WOS Git and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})',
            'option(WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN "Reuse artifacts already produced by tools/bootstrap.sh instead of rerunning external toolchain/userland port builders" OFF)',
            "if(WOS_BUILD_DISK_IMAGES)\n    add_dependencies(wos_full\n        mountfs_disk\n        boot_image\n    )\nendif()",
            "if(NOT WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)\n    add_custom_target(libcxx DEPENDS ${LIBCXX_STAMP})\nendif()",
        ],
        "native WOS self-hosting build defaults",
    )

    if "set(WOS_PORT_BUILD_DEFAULT OFF)" in source:
        fail("native WOS builds must keep target port/toolchain targets enabled by default")


def test_preseeded_toolchain_mode_validates_bootstrap_outputs() -> None:
    source = ROOT_CMAKE.read_text()
    require_tokens(
        source,
        [
            "wos_add_preseeded_artifact_target",
            "verify_preseeded_artifacts.sh",
            "WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN",
            "wos_add_preseeded_artifact_target(mlibc ${MLIBC_STAMP} mlibc",
            "${WOS_SYSROOT_PATH}/lib/libc.so",
            "wos_add_preseeded_artifact_target(libcxx ${LIBCXX_STAMP} libcxx",
            "${WOS_SYSROOT_PATH}/lib/libc++.so",
            "wos_add_preseeded_artifact_target(busybox ${BUSYBOX_STAMP} busybox",
            "${WOS_BUSYBOX_INSTALL_DIR}/bin/busybox",
            "wos_add_preseeded_artifact_target(dropbear ${DROPBEAR_STAMP} dropbear",
            "${WOS_SYSROOT_PATH}/bin/dropbearmulti",
            "wos_add_preseeded_artifact_target(gnu_make ${GNU_MAKE_STAMP} gnu_make",
            "${WOS_SYSROOT_PATH}/bin/make",
            "wos_add_preseeded_artifact_target(clang_for_wos ${CLANG_FOR_WOS_STAMP} clang_for_wos",
            "${WOS_SYSROOT_PATH}/bin/clang",
            "${WOS_SYSROOT_PATH}/bin/llvm-tblgen",
            "${WOS_SYSROOT_PATH}/bin/llvm-symbolizer",
            "${WOS_SYSROOT_PATH}/bin/llvm-as",
            "${WOS_SYSROOT_PATH}/bin/llc",
            "${WOS_SYSROOT_PATH}/bin/opt",
            "wos_add_preseeded_artifact_target(cmake_for_wos ${CMAKE_FOR_WOS_STAMP} cmake",
            "${WOS_SYSROOT_PATH}/bin/cmake",
            "${WOS_SYSROOT_PATH}/share/cmake-*",
            "wos_add_preseeded_artifact_target(python_for_wos ${PYTHON_FOR_WOS_STAMP} python",
            "${WOS_SYSROOT_PATH}/bin/python3*",
            "wos_add_preseeded_artifact_target(git_for_wos ${GIT_FOR_WOS_STAMP} git",
            "${WOS_SYSROOT_PATH}/libexec/git-core/git-remote-https",
        ],
        "preseeded bootstrap artifact validation",
    )

    clang_preseed_start = source.find("wos_add_preseeded_artifact_target(clang_for_wos")
    clang_preseed_end = source.find("else()", clang_preseed_start)
    if clang_preseed_start < 0 or clang_preseed_end < 0:
        fail("preseeded clang validation block is missing")
    clang_preseed = source[clang_preseed_start:clang_preseed_end]
    if "build_clang_for_wos.sh" in clang_preseed or "COMMAND ${CMAKE_COMMAND} -E touch" in clang_preseed:
        fail("preseeded clang mode must validate existing artifacts instead of rebuilding or blindly touching stamps")


def test_preseeded_toolchain_mode_skips_external_source_scans() -> None:
    source = ROOT_CMAKE.read_text()

    def assert_no_scan_before_preseed(label: str, section_marker: str, preseed_call: str) -> None:
        section_start = source.find(section_marker)
        if section_start < 0:
            fail(f"{label} section is missing")
        preseed_start = source.find(preseed_call, section_start)
        if preseed_start < 0:
            fail(f"{label} preseed validation call is missing")
        prefix = source[section_start:preseed_start]
        forbidden = [
            "wos_collect_external_sources(",
            "wos_glob_recurse(",
        ]
        offenders = [token for token in forbidden if token in prefix]
        if offenders:
            fail(f"{label} preseed configure path must not scan external sources before artifact validation: {', '.join(offenders)}")

    preseeded_ports = [
        ("mlibc", "# --- mlibc:", "wos_add_preseeded_artifact_target(mlibc ${MLIBC_STAMP} mlibc"),
        ("busybox", "# --- busybox:", "wos_add_preseeded_artifact_target(busybox ${BUSYBOX_STAMP} busybox"),
        ("dropbear", "# --- dropbear:", "wos_add_preseeded_artifact_target(dropbear ${DROPBEAR_STAMP} dropbear"),
        ("gnu_make", "# --- gnu_make:", "wos_add_preseeded_artifact_target(gnu_make ${GNU_MAKE_STAMP} gnu_make"),
        ("bash", "# --- bash_for_wos:", "wos_add_preseeded_artifact_target(bash_for_wos ${BASH_FOR_WOS_STAMP} bash"),
        ("zlib", "# --- zlib_for_wos:", "wos_add_preseeded_artifact_target(zlib_for_wos ${ZLIB_FOR_WOS_STAMP} zlib"),
        ("openssl", "# --- openssl_for_wos:", "wos_add_preseeded_artifact_target(openssl_for_wos ${OPENSSL_FOR_WOS_STAMP} openssl"),
        ("curl", "# --- curl_for_wos:", "wos_add_preseeded_artifact_target(curl_for_wos ${CURL_FOR_WOS_STAMP} curl"),
        ("git", "# --- git_for_wos:", "wos_add_preseeded_artifact_target(git_for_wos ${GIT_FOR_WOS_STAMP} git"),
        ("clang", "# --- clang_for_wos:", "wos_add_preseeded_artifact_target(clang_for_wos ${CLANG_FOR_WOS_STAMP} clang_for_wos"),
        ("ninja", "# --- ninja_for_wos:", "wos_add_preseeded_artifact_target(ninja_for_wos ${NINJA_FOR_WOS_STAMP} ninja"),
        ("cmake", "# --- cmake_for_wos:", "wos_add_preseeded_artifact_target(cmake_for_wos ${CMAKE_FOR_WOS_STAMP} cmake"),
        ("python", "# --- python_for_wos:", "wos_add_preseeded_artifact_target(python_for_wos ${PYTHON_FOR_WOS_STAMP} python"),
        ("meson", "# --- meson_for_wos:", "wos_add_preseeded_artifact_target(meson_for_wos ${MESON_FOR_WOS_STAMP} meson"),
        ("nasm", "# --- nasm_for_wos:", "wos_add_preseeded_artifact_target(nasm_for_wos ${NASM_FOR_WOS_STAMP} nasm"),
    ]
    for label, section_marker, preseed_call in preseeded_ports:
        assert_no_scan_before_preseed(label, section_marker, preseed_call)

    require_tokens(
        source,
        [
            "set(WOS_SCAN_CMAKE_PORT_SOURCES OFF)",
            "if(WOS_BUILD_CMAKE_FOR_HOST)\n    set(WOS_SCAN_CMAKE_PORT_SOURCES ON)",
            "elseif(WOS_BUILD_CMAKE_FOR_WOS AND NOT WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN)\n    set(WOS_SCAN_CMAKE_PORT_SOURCES ON)",
            "if(WOS_SCAN_CMAKE_PORT_SOURCES)\n    wos_collect_external_sources(CMAKE_FOR_WOS_SOURCES",
        ],
        "preseeded CMake port source scan guard",
    )


def test_root_toolchain_builds_do_not_use_ninja_console_pool() -> None:
    source = ROOT_CMAKE.read_text()
    forbidden = [
        "USES_TERMINAL",
        "USES_TERMINAL_CONFIGURE",
        "USES_TERMINAL_BUILD",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("root toolchain/image batch builds must not serialize through Ninja's console pool: " + ", ".join(present))


def test_wos_build_jobs_helper_has_self_hostable_fallbacks() -> None:
    source = CCACHE_ENV.read_text()
    require_tokens(
        source,
        [
            'export CCACHE_DIR="${TMPDIR:-/tmp}/wos-ccache"',
            "wos_build_jobs() {",
            'local jobs="${WOS_BUILD_JOBS:-}"',
            "WOS_BUILD_JOBS must be a positive integer",
            'CMAKE_BUILD_PARALLEL_LEVEL',
            "command -v nproc",
            "command -v getconf",
            "getconf _NPROCESSORS_ONLN",
            "command -v python3",
            "os.cpu_count() or 1",
            "jobs=1",
            "wos_detail_file()",
            "WOS_BUILD_DETAIL_TSV",
            "WOS_BOOTSTRAP_DETAIL_TSV",
            "wos_record_detail()",
            "wos_timed_step()",
            "wos_run_in_dir()",
            "wos_run_env_in_dir()",
            "wos_remove_tree()",
            'local attempts="${WOS_REMOVE_TREE_RETRIES:-5}"',
            "refusing to remove unsafe path",
            "WOS_REMOVE_TREE_RETRIES must be a positive integer",
            "wos_copy_tree_entries_excluding()",
            'wos_timed_step "copy_tree"',
            'for entry in "$source_dir"/* "$source_dir"/.[!.]* "$source_dir"/..?*; do',
            'cp -a "$entry" "$dest_dir/"',
            "wos_dir_has_entries()",
            'for entry in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do',
            "wos_refresh_file_mtime()",
            'if touch "$file" 2>/dev/null; then',
            'tmp="$file.wos-mtime.$$"',
            'wos_timed_step "make" "$label" make',
            "wos_make_label()",
            "wos_download_file()",
            'local attempts="${4:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"',
            'local delay="${WOS_SOURCE_DOWNLOAD_RETRY_DELAY:-2}"',
            'local connect_timeout="${WOS_SOURCE_DOWNLOAD_CONNECT_TIMEOUT:-20}"',
            'local low_speed_limit="${WOS_SOURCE_DOWNLOAD_LOW_SPEED_LIMIT:-1}"',
            'local low_speed_time="${WOS_SOURCE_DOWNLOAD_LOW_SPEED_TIME:-60}"',
            "WOS_SOURCE_DOWNLOAD_CONNECT_TIMEOUT must be a positive integer",
            "WOS_SOURCE_DOWNLOAD_LOW_SPEED_LIMIT must be a non-negative integer",
            "WOS_SOURCE_DOWNLOAD_LOW_SPEED_TIME must be a positive integer",
            'curl -fL \\',
            '--connect-timeout "$connect_timeout"',
            '--speed-limit "$low_speed_limit"',
            '--speed-time "$low_speed_time"',
            'rm -f "$dest.tmp"',
            'echo "warning: failed to download $url (status $status)"',
            "wos_prefetch_meson_subprojects()",
            'WOS_MESON_SUBPROJECT_FETCH_RETRIES',
            'WOS_MESON_SUBPROJECT_FETCH_RETRY_DELAY',
            'wos_fetch_meson_git_subproject "$source_dir" "$subproject"',
            'wos_remove_tree "$source_dir/subprojects/$subproject"',
            "wos_fetch_meson_git_subproject()",
            'url="$(wos_wrap_value "$wrap" url)"',
            'revision="$(wos_wrap_value "$wrap" revision)"',
            'local low_speed_limit="${WOS_GIT_HTTP_LOW_SPEED_LIMIT:-1}"',
            'local low_speed_time="${WOS_GIT_HTTP_LOW_SPEED_TIME:-60}"',
            'GIT_HTTP_LOW_SPEED_LIMIT="$low_speed_limit"',
            'GIT_HTTP_LOW_SPEED_TIME="$low_speed_time"',
            'git -C "$dest" fetch --depth 1 origin "$revision" || return 1',
            'wos_copy_tree_entries_excluding "$package_dir" "$dest"',
            "wos_make_jobs() {",
            'local jobs="${WOS_MAKE_JOBS:-}"',
            "WOS_MAKE_JOBS must be a positive integer",
            "wos_make_jobserver_arg() {",
            'local style="${WOS_MAKE_JOBSERVER_STYLE:-}"',
            "style=pipe",
            "--jobserver-style=$style",
            "WOS_MAKE_JOBSERVER_STYLE must be pipe or fifo",
            "wos_make() {",
            'make "$jobserver_arg" -j"$jobs" "$@"',
            "wos_ninja_jobs() {",
            'local jobs="${WOS_NINJA_JOBS:-}"',
            "WOS_NINJA_JOBS must be a positive integer",
            'uname -s 2>/dev/null || printf unknown',
            '= "WOS" ]',
            "/proc/stat",
            "cpu_count",
            "wos_build_jobs",
        ],
        "shared WOS build job helper",
    )


def test_top_level_build_paths_propagate_full_parallelism() -> None:
    build_script = BUILD_WOS.read_text()
    root_cmake = ROOT_CMAKE.read_text()
    ninja_wrapper = NINJA_WITH_JOBS.read_text()

    require_tokens(
        build_script,
        [
            'source "$WORKSPACE_ROOT/tools/ccache_env.sh"',
            'BUILD_JOBS="${WOS_BUILD_JOBS:-${WOS_NINJA_JOBS:-${CMAKE_BUILD_PARALLEL_LEVEL:-}}}"',
            'BUILD_JOBS="$(wos_build_jobs)"',
            'export WOS_BUILD_JOBS="$BUILD_JOBS"',
            'export WOS_NINJA_JOBS="${WOS_NINJA_JOBS:-$BUILD_JOBS}"',
            'export WOS_MAKE_JOBS="${WOS_MAKE_JOBS:-$BUILD_JOBS}"',
            'export CMAKE_BUILD_PARALLEL_LEVEL="$BUILD_JOBS"',
            '--parallel "$BUILD_JOBS"',
        ],
        "top-level Build WOS job propagation",
    )
    require_tokens(
        root_cmake,
        [
            "WOS_LIBCXX_BUILD_DIR=${LIBCXX_BUILD_DIR}",
            "${WOS_BUILD_SCRIPTS_DIR}/build_libcxx_for_wos.sh",
            "DEPENDS ${WOS_BUILD_SCRIPTS_DIR}/build_libcxx_for_wos.sh ${MLIBC_STAMP}",
            "set(WOS_TOOLS_BUILD_PARALLEL_ARGS --parallel)",
            'set(WOS_TOOLS_BUILD_PARALLEL_ARGS --parallel $ENV{WOS_BUILD_JOBS})',
            "BUILD_COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/tools ${WOS_TOOLS_BUILD_PARALLEL_ARGS}",
            "COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/tools ${WOS_TOOLS_BUILD_PARALLEL_ARGS}",
        ],
        "root CMake nested Ninja job propagation",
    )
    require_tokens(
        ninja_wrapper,
        [
            'source "$WORKSPACE_ROOT/tools/ccache_env.sh"',
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            'exec ninja -j"$WOS_NINJA_JOBS" "$@"',
        ],
        "nested Ninja job wrapper",
    )
    libcxx_script = WOS_LIBCXX_BUILD.read_text()
    require_tokens(
        libcxx_script,
        [
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            'ninja -j"$WOS_NINJA_JOBS" -C "$LIBCXX_BUILD" "$@"',
            'if ! wos_timed_step "build" "libcxx_runtime" run_libcxx_ninja; then',
            "reset_libcxx_ninja_state",
            'rm -f "$LIBCXX_BUILD/.ninja_log" "$LIBCXX_BUILD/.ninja_deps"',
            'wos_timed_step "build" "libcxx_runtime_retry" run_libcxx_ninja',
            'wos_timed_step "install" "libcxx_runtime" run_libcxx_ninja install',
        ],
        "libc++ nested Ninja retry keeps full parallelism",
    )
    bootstrap = WOS_TOOLCHAIN.read_text()
    require_tokens(
        bootstrap,
        ['WOS_LIBCXX_BUILD_DIR="$B/libcxx-build" "$B/../scripts/build/build_libcxx_for_wos.sh"'],
        "bootstrap libc++ build wrapper",
    )
    libcxx_config_index = bootstrap.index("$B/src/llvm-project/runtimes")
    libcxx_wrapper_index = bootstrap.index('WOS_LIBCXX_BUILD_DIR="$B/libcxx-build"')
    if libcxx_wrapper_index < libcxx_config_index:
        fail("bootstrap libc++ wrapper must run after configuring the runtimes build")


def test_libcxx_bootstrap_uses_host_tools_and_only_flag_preseeds() -> None:
    bootstrap = WOS_TOOLCHAIN.read_text()
    require_tokens(
        bootstrap,
        [
            'HOST_NINJA="$(command -v ninja)"',
            'HOST_PYTHON="$(command -v python3)"',
            "LIBCXX_CMAKE_HOST_TOOL_ARGS=(",
            '-DCMAKE_MAKE_PROGRAM="$HOST_NINJA"',
            '-DPython3_EXECUTABLE="$HOST_PYTHON"',
            '-DCMAKE_FIND_ROOT_PATH="$SYSROOT"',
            "-DCMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER",
            "-DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY",
            "-DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY",
            "LIBCXX_CMAKE_SUPPORT_CACHE_ARGS=(",
            "-DCXX_SUPPORTS_NOSTDLIBXX_FLAG=1",
            "-DCXX_SUPPORTS_FVISIBILITY_EQ_HIDDEN_FLAG=1",
            "-DCXX_SUPPORTS_WERROR_EQ_RETURN_TYPE_FLAG=1",
            "-DC_SUPPORTS_FUNWIND_TABLES_FLAG=1",
            "-DLLVM_USES_LIBSTDCXX=",
            "-DLINKER_SUPPORTS_COLOR_DIAGNOSTICS=",
            "LIBCXX_RUNTIME_CMAKE_SUPPORT_CACHE_ARGS=(",
            '"${LIBCXX_CMAKE_SUPPORT_CACHE_ARGS[@]}"',
            "-DCXX_SUPPORTS_FNO_EXCEPTIONS_FLAG=1",
            "-DCXX_SUPPORTS_FNO_RTTI_FLAG=1",
            "-DCXX_SUPPORTS_FUNWIND_TABLES_FLAG=1",
            "-DCXX_SUPPORTS_PEDANTIC_FLAG=1",
        ],
        "libc++ bootstrap host-tool selection and flag support preseeds",
    )

    expected_counts = {
        '"${LIBCXX_CMAKE_HOST_TOOL_ARGS[@]}"': 2,
        '"${LIBCXX_CMAKE_SUPPORT_CACHE_ARGS[@]}"': 2,
        '"${LIBCXX_RUNTIME_CMAKE_SUPPORT_CACHE_ARGS[@]}"': 1,
    }
    for token, expected_count in expected_counts.items():
        actual_count = bootstrap.count(token)
        if actual_count != expected_count:
            fail(
                "libc++ bootstrap expected "
                f"{expected_count} occurrences of {token}, found {actual_count}"
            )

    forbidden = [
        "LIBCXXABI_HAS_C_LIB=1",
        "LIBCXXABI_HAS_DL_LIB=1",
        "LIBCXXABI_HAS_CXA_THREAD_ATEXIT_IMPL=1",
        "LIBCXX_HAS_RT_LIB=1",
        "LIBCXX_HAS_ATOMIC_LIB=1",
        "LIBUNWIND_HAS_C_LIB=1",
        "LIBUNWIND_HAS_DL_LIB=1",
        "LIBUNWIND_HAS_PTHREAD_LIB=1",
        "LIBUNWIND_HAS_GCC_S_LIB=1",
        "LIBUNWIND_HAS_GCC_LIB=1",
        "HAVE_FLOCK=1",
        "PICOLIBC=1",
    ]
    present = [token for token in forbidden if token in bootstrap]
    if present:
        fail(
            "libc++ bootstrap must keep library and runtime capability probes live: "
            + ", ".join(present)
        )


def test_mlibc_wrap_dependencies_are_prefetched_with_retries() -> None:
    bootstrap = WOS_TOOLCHAIN.read_text()
    build_script = WOS_MLIBC_BUILD.read_text()
    helper = CCACHE_ENV.read_text()
    require_tokens(
        helper,
        [
            "wos_prefetch_meson_subprojects()",
            "failed to fetch Meson subproject",
            'wos_fetch_meson_git_subproject "$source_dir" "$subproject"',
            'wos_remove_tree "$source_dir/subprojects/$subproject"',
            'GIT_HTTP_LOW_SPEED_LIMIT="$low_speed_limit"',
            'GIT_HTTP_LOW_SPEED_TIME="$low_speed_time"',
            'git -C "$dest" fetch --depth 1 origin "$revision"',
            'git -C "$dest" checkout -f HEAD',
            'git -C "$dest" diff-index --quiet HEAD --',
            "Meson subproject checkout at $dest is incomplete; refetching",
            "Meson subproject checkout at $dest is incomplete after fetch",
            'wos_copy_tree_entries_excluding "$package_dir" "$dest"',
        ],
        "Meson subproject retry helper",
    )
    require_tokens(
        bootstrap,
        [
            'wos_prefetch_meson_subprojects "$B/src/mlibc" freestnd-c-hdrs freestnd-cxx-hdrs frigg',
            'meson_setup_rerunnable "$B/mlibc-build"',
        ],
        "bootstrap mlibc Meson dependency prefetch",
    )
    require_tokens(
        build_script,
        [
            'wos_prefetch_meson_subprojects "$MLIBC_SRC" freestnd-c-hdrs freestnd-cxx-hdrs frigg',
            'meson setup --prefix="$TARGET_SYSROOT"',
        ],
        "incremental mlibc Meson dependency prefetch",
    )


def test_wos_mlibc_priority_sysdeps_are_syscall_backed() -> None:
    sysdeps_header = WOS_SYSDEPS_HPP.read_text()
    sysdeps = WOS_SYSDEPS_CPP.read_text()
    process_header = WOS_PROCESS_H.read_text()
    sys_resource = MLIBC_SYS_RESOURCE.read_text()
    kernel_process = KERNEL_PROCESS_CPP.read_text()
    kernel_callnums = KERNEL_PROCESS_CALLNUMS.read_text()
    mlibc_callnums = WOS_PROCESS_CALLNUMS.read_text()

    require_tokens(
        sysdeps_header,
        [
            "GetPriority,",
            "SetPriority,",
        ],
        "WOS mlibc priority sysdep tags",
    )
    require_tokens(
        sysdeps,
        [
            "int Sysdeps<SetPriority>::operator()(int which, id_t who, int prio)",
            "ker::process::setpriority(which, who, prio)",
            "int Sysdeps<GetPriority>::operator()(int which, id_t who, int *value)",
            "ker::process::getpriority(which, who)",
            "*value = static_cast<int>(r) - 20",
        ],
        "WOS mlibc priority sysdep implementations",
    )
    stub_match = re.search(
        r"Sysdeps<SetPriority>::operator\([^)]*\)\s*\{(?P<body>.*?)\n\}",
        sysdeps,
        flags=re.DOTALL,
    )
    if stub_match and "(void)which;" in stub_match.group("body"):
        fail("WOS mlibc setpriority must not return success without issuing the kernel syscall")

    require_tokens(
        process_header,
        [
            "procmgmt_ops::SETPRIORITY",
            "procmgmt_ops::GETPRIORITY",
            "inline int64_t getpriority(int which, int64_t who)",
        ],
        "WOS mlibc process priority wrappers",
    )
    if "return r - 20;" in process_header:
        fail("raw WOS getpriority wrapper must not decode negative nice values before errno handling")
    require_tokens(
        sys_resource,
        [
            "int getpriority(int which, id_t who)",
            "errno = e;",
            "return -1;",
            "return value;",
        ],
        "generic mlibc getpriority error handling",
    )
    require_tokens(
        kernel_process,
        [
            "WOS_PRIO_KERNEL_ENCODE_BIAS = 20",
            "auto wos_proc_getpriority(int which, int64_t who) -> uint64_t",
            "static_cast<int>(target->sched_nice) + WOS_PRIO_KERNEL_ENCODE_BIAS",
            "case abi::process::procmgmt_ops::GETPRIORITY",
        ],
        "kernel process priority syscalls",
    )
    require_tokens(
        kernel_callnums,
        [
            "SIGPENDING,     // 41",
            "GETPRIORITY,    // 42",
        ],
        "kernel process priority syscall ABI numbering",
    )
    require_tokens(
        mlibc_callnums,
        [
            "SIGPENDING,    // 41",
            "GETPRIORITY,   // 42",
        ],
        "mlibc process priority syscall ABI numbering",
    )


def test_wos_port_build_scripts_use_shared_job_helper() -> None:
    for script in WOS_JOB_HELPER_USERS:
        source = script.read_text()
        require_tokens(
            source,
            ['WOS_BUILD_JOBS="$(wos_build_jobs)"'],
            f"{script.relative_to(ROOT)} shared WOS build job helper",
        )
        forbidden = ["$(nproc)", "`nproc`"]
        present = [token for token in forbidden if token in source]
        if present:
            fail(f"{script.relative_to(ROOT)} must not assume Linux nproc directly: {', '.join(present)}")


def test_gnu_make_port_build_scripts_use_make_job_helper() -> None:
    for script in WOS_MAKE_JOB_HELPER_USERS:
        source = script.read_text()
        require_tokens(
            source,
            ['WOS_MAKE_JOBS="$(wos_make_jobs)"'],
            f"{script.relative_to(ROOT)} shared GNU Make job helper",
        )
        require_tokens(source, ['wos_make "$WOS_MAKE_JOBS"'], f"{script.relative_to(ROOT)} shared GNU Make job helper")
        if re.search(r"(?:host_env\s+)?make\b[^\n]*-j\"\$WOS_BUILD_JOBS\"", source):
            fail(f"{script.relative_to(ROOT)} must use WOS_MAKE_JOBS for GNU Make parallelism")
        if re.search(r"\bmake\b(?:(?!WOS_MAKE_JOBSERVER_ARG).)*-j\"\$WOS_MAKE_JOBS\"", source):
            fail(f"{script.relative_to(ROOT)} must use wos_make or the WOS GNU Make jobserver arg")
        if "host_env make" in source:
            require_tokens(
                source,
                [
                    'WOS_MAKE_JOBSERVER_ARG="$(wos_make_jobserver_arg "$WOS_MAKE_JOBS")"',
                    'host_env make ${WOS_MAKE_JOBSERVER_ARG:+"$WOS_MAKE_JOBSERVER_ARG"}',
                ],
                f"{script.relative_to(ROOT)} host GNU Make jobserver compatibility",
            )


def test_ninja_port_build_scripts_use_ninja_job_helper() -> None:
    for script in WOS_NINJA_JOB_HELPER_USERS:
        source = script.read_text()
        require_tokens(
            source,
            ['WOS_NINJA_JOBS="$(wos_ninja_jobs)"'],
            f"{script.relative_to(ROOT)} shared Ninja job helper",
        )
        forbidden = [
            'ninja -j"$WOS_BUILD_JOBS"',
            'ninja -C "$MLIBC_BUILD" -j"$WOS_BUILD_JOBS"',
            '--parallel "$WOS_BUILD_JOBS"',
        ]
        present = [token for token in forbidden if token in source]
        if present:
            fail(f"{script.relative_to(ROOT)} must use WOS_NINJA_JOBS for Ninja/CMake parallelism")

    bootstrap = WOS_TOOLCHAIN.read_text()
    require_tokens(
        bootstrap,
        ['ninja -j"$WOS_NINJA_JOBS" install-cxx-headers install-cxxabi-headers'],
        "bootstrap libc++ header install parallelism",
    )
    if "ninja install-cxx-headers install-cxxabi-headers" in bootstrap:
        fail("bootstrap libc++ header install must use WOS_NINJA_JOBS")

    clang_source = WOS_CLANG_BUILD.read_text()
    require_tokens(
        clang_source,
        [
            'WOS_LLVM_PARALLEL_LINK_JOBS="${WOS_LLVM_PARALLEL_LINK_JOBS:-$WOS_NINJA_JOBS}"',
            "WOS_LLVM_PARALLEL_LINK_JOBS must be a positive integer",
            '-DLLVM_PARALLEL_LINK_JOBS="$WOS_LLVM_PARALLEL_LINK_JOBS"',
        ],
        "native WOS clang LLVM link parallelism",
    )
    if "-DLLVM_PARALLEL_LINK_JOBS=1" in clang_source:
        fail("native WOS clang build must not force serial LLVM link jobs")


def test_host_toolchain_install_uses_ninja_jobs() -> None:
    source = HOST_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            'source "$WORKSPACE_ROOT/tools/ccache_env.sh"',
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            "-DLLVM_BUILD_TOOLS=ON",
            "-DLLVM_INSTALL_TOOLCHAIN_ONLY=OFF",
            'ninja -j"$WOS_NINJA_JOBS" install',
        ],
        "host toolchain Ninja parallelism",
    )
    if "\nninja install" in source:
        fail("host toolchain install must not use bare ninja install")


def test_wos_port_install_steps_keep_make_parallelism() -> None:
    scripts = [
        WOS_BUSYBOX_BUILD,
        WOS_CURL_BUILD,
        WOS_TLS_BUILD,
        WOS_PYTHON_BUILD,
        ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
    ]
    for script in scripts:
        source = script.read_text()
        if re.search(r'(?m)^\s*(?:if !\s+|yes "" \|\s+)?make\b', source):
            fail(f"{script.relative_to(ROOT)} must use wos_make or host_env make with WOS_MAKE_JOBS")


def test_wos_tar_invocations_use_busybox_compatible_long_options() -> None:
    for script in WOS_BUSYBOX_TAR_USERS:
        source = script.read_text()
        forbidden = ["--strip-components=", "--exclude=", "--exclude ", "-print -quit"]
        present = [token for token in forbidden if token in source]
        if present:
            fail(
                f"{script.relative_to(ROOT)} must avoid BusyBox tar long-option forms "
                f"that are fragile in WOS wrapper copy/extract paths: {', '.join(present)}"
            )


def test_wos_source_copy_wrappers_avoid_busybox_tar_pipelines() -> None:
    for script in WOS_SOURCE_COPY_USERS:
        source = script.read_text()
        require_tokens(
            source,
            [
                "wos_remove_tree",
                "wos_copy_tree_entries_excluding",
            ],
            f"{script.relative_to(ROOT)} source copy helper",
        )
        forbidden = ["tar --exclude", "tar \\\n", "cp -a \"$source_dir/.\""]
        present = [token for token in forbidden if token in source]
        if present:
            fail(
                f"{script.relative_to(ROOT)} must avoid tar-copy pipelines and whole-tree "
                f"copy/remove patterns in WOS source staging: {', '.join(present)}"
            )


def test_gnu_make_script_passes_build_triplet_on_wos() -> None:
    source = (ROOT / "scripts" / "build" / "build_make.sh").read_text()
    require_tokens(
        source,
        [
            'TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'GNU_MAKE_CONFIGURE_BUILD_ARGS=()',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'GNU_MAKE_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            '"${GNU_MAKE_CONFIGURE_BUILD_ARGS[@]}"',
            '--host="$TARGET_ARCH"',
        ],
        "GNU make WOS configure build triplet",
    )


def test_gnu_make_defaults_to_pipe_jobserver_on_wos() -> None:
    source = (ROOT / "scripts" / "build" / "build_make.sh").read_text()
    require_tokens(
        source,
        [
            "patch_default_jobserver_for_wos()",
            'local posixos="$source_dir/src/posixos.c"',
            "default to pipe jobserver on WOS",
            'local needle=\'  if (!style || strcmp (style, "fifo") == 0)\'',
            'local patched="$posixos.wos-jobserver.$$"',
            'while IFS= read -r line || [ -n "$line" ]; do',
            'if [ "$line" = "$needle" ]; then',
            'if (style && strcmp (style, "fifo") == 0)',
            'done <"$posixos" >"$patched"',
            'mv "$patched" "$posixos"',
            "Patching GNU make to default to pipe jobserver on WOS",
            'patch_default_jobserver_for_wos "$MAKE_SOURCE_DIR"',
        ],
        "GNU make WOS default jobserver patch",
    )


def test_gnu_make_script_handles_native_wos_autoconf_probes() -> None:
    source = (ROOT / "scripts" / "build" / "build_make.sh").read_text()
    require_tokens(
        source,
        [
            "rewrite_file_for_mtime()",
            'if touch "$path" 2>/dev/null; then',
            'cp "$path" "$tmp"',
            "refresh_make_release_generated_files()",
            "refresh_make_build_generated_files()",
            'if [ "$HOST_SYSTEM" = "WOS" ]; then\n    refresh_make_release_generated_files "$MAKE_SOURCE_DIR"\nfi',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then\n    refresh_make_build_generated_files "$MAKE_BUILD"\nfi',
            'if [ "$HOST_SYSTEM" = "WOS" ] && { [ ! -x "$MAKE_BUILD/make" ] || [ ! -f "$MAKE_BUILD/config.status" ]; }; then',
            'rm -f "$MAKE_BUILD/Makefile"',
            '[ ! -f "$MAKE_BUILD/config.status" ]',
            "GNU_MAKE_CONFIGURE_CACHE_ARGS=()",
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            "ac_cv_path_SED=/usr/bin/sed",
            '"ac_cv_path_install=/usr/bin/install -c"',
            "ac_cv_path_mkdir=/usr/bin/mkdir",
            "ac_cv_prog_AWK=awk",
            "ac_cv_prog_PERL=perl",
            "ac_cv_path_MSGFMT=:",
            "ac_cv_path_GMSGFMT=:",
            "ac_cv_path_XGETTEXT=:",
            "ac_cv_path_MSGMERGE=:",
            "ac_cv_path_PKG_CONFIG=",
            "ac_cv_path_ac_pt_PKG_CONFIG=",
            "ac_cv_func_mempcpy=yes",
            '"${GNU_MAKE_CONFIGURE_CACHE_ARGS[@]}"',
            "GNU_MAKE_BUILD_ARGS=()",
            "GNU_MAKE_BUILD_ARGS=(MAKEINFO=true)",
            'wos_make "$WOS_MAKE_JOBS" -C "$MAKE_BUILD" "${GNU_MAKE_BUILD_ARGS[@]}"',
        ],
        "GNU make native WOS Autoconf probe handling",
    )


def test_gnu_make_script_preseeds_wos_target_configure_probes() -> None:
    source = (ROOT / "scripts" / "build" / "build_make.sh").read_text()
    config_site_start = source.find("write_make_config_site()")
    config_site_end = source.find("require_file \"$HOST/bin/clang\"", config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("GNU make build script must keep write_make_config_site before host-tool validation")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "write_make_config_site()",
            'tmp_config_site="$(mktemp "$MAKE_BUILD/config.site.XXXXXX")"',
            "ac_cv_func_getloadavg=yes",
            "ac_cv_func_gettimeofday='no (cross-compiling)'",
            "ac_cv_func_mempcpy=yes",
            "ac_cv_func_posix_spawn=yes",
            "ac_cv_func_posix_spawnattr_setsigmask=yes",
            "ac_cv_func_pselect=yes",
            "ac_cv_func_strsignal=yes",
            "ac_cv_header_spawn_h=yes",
            "ac_cv_header_sys_wait_h=yes",
            "ac_cv_struct_st_mtim_nsec=st_mtim.tv_nsec",
            "make_cv_file_timestamp_hi_res=yes",
            "make_cv_job_server=yes",
            "make_cv_posix_spawn=yes",
            "make_cv_synchronous_posix_spawn='no (cross-compiling)'",
            "make_cv_sys_gnu_glob=no",
            "make_cv_union_wait=no",
        ],
        "GNU make WOS target configure preseeds",
    )
    require_tokens(
        source,
        [
            "write_make_config_site",
            'export CONFIG_SITE="$MAKE_BUILD/config.site"',
        ],
        "GNU make config.site export",
    )
    forbidden = [
        "ac_cv_prog_CC=",
        "ac_cv_prog_CXX=",
        "ac_cv_prog_CPP=",
        "ac_cv_prog_AR=",
        "ac_cv_path_install=",
    ]
    present = [token for token in forbidden if token in config_site]
    if present:
        fail("GNU make config.site must not pin host/tool paths: " + ", ".join(present))


def test_bash_script_falls_back_to_target_triplet_on_native_wos() -> None:
    source = (ROOT / "scripts" / "build" / "build_bash_for_wos.sh").read_text()
    require_tokens(
        source,
        [
            "detect_bash_build_triple()",
            'if build_triple="$(sh "$config_guess")"; then',
            'host_system="$(uname -s 2>/dev/null || printf unknown)"',
            'if [ "$host_system" = "WOS" ]; then',
            'printf \'%s\\n\' "$TARGET_ARCH"',
            'BUILD_TRIPLE="$(detect_bash_build_triple "$BASH_WORK/support/config.guess")"',
            '--build="$BUILD_TRIPLE"',
            '--host="$TARGET_ARCH"',
        ],
        "Bash native WOS build triplet fallback",
    )


def test_bash_script_handles_native_wos_autoconf_maintainer_rules() -> None:
    source = (ROOT / "scripts" / "build" / "build_bash_for_wos.sh").read_text()
    require_tokens(
        source,
        [
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            "refresh_bash_release_generated_files()",
            "refresh_bash_build_generated_files()",
            "wos_refresh_file_mtime",
            'if [ "$HOST_SYSTEM" = "WOS" ]; then\n    refresh_bash_release_generated_files "$BASH_WORK"\nfi',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then\n    refresh_bash_build_generated_files "$BASH_WORK"\nfi',
        ],
        "Bash native WOS Autoconf maintainer-rule handling",
    )


def test_bash_script_preseeds_native_wos_tool_probes() -> None:
    source = (ROOT / "scripts" / "build" / "build_bash_for_wos.sh").read_text()
    require_tokens(
        source,
        [
            "BASH_CONFIGURE_CACHE_ARGS=()",
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            '"ac_cv_path_install=/usr/bin/install -c"',
            "ac_cv_path_mkdir=/usr/bin/mkdir",
            "ac_cv_path_SED=/usr/bin/sed",
            '"ac_cv_path_EGREP_TRADITIONAL=/usr/bin/grep -E"',
            "ac_cv_path_MSGFMT=:",
            "ac_cv_path_GMSGFMT=:",
            "ac_cv_path_XGETTEXT=:",
            "ac_cv_path_MSGMERGE=:",
            '"${BASH_CONFIGURE_CACHE_ARGS[@]}"',
        ],
        "Bash native WOS tool-probe configure cache",
    )


def test_bash_script_enables_dev_fd_for_process_substitution() -> None:
    source = (ROOT / "scripts" / "build" / "build_bash_for_wos.sh").read_text()
    require_tokens(
        source,
        [
            "bash_cv_dev_fd=standard",
            "bash_cv_dev_stdin=present",
            "bash_cv_sys_named_pipes=missing",
        ],
        "Bash process substitution /dev/fd configuration",
    )
    if "bash_cv_dev_fd=absent" in source:
        fail("Bash must not be configured as if /dev/fd is absent")


def test_bash_script_preseeds_wos_target_configure_probes() -> None:
    source = (ROOT / "scripts" / "build" / "build_bash_for_wos.sh").read_text()
    config_site_start = source.find("write_config_site()")
    config_site_end = source.find("require_file \"$HOST/bin/clang\"", config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("Bash WOS build script must keep write_config_site before host-tool validation")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "ac_cv_header_sys_random_h=no",
            "ac_cv_header_termios_h=yes",
            "ac_cv_header_sys_mman_h=yes",
            "ac_cv_func_getrandom=no",
            "ac_cv_func_getentropy=yes",
            "ac_cv_func_memfd_create=yes",
            "ac_cv_func_shm_open=yes",
            "ac_cv_func_working_mktime=no",
            "ac_cv_func_chown_works=no",
            "ac_cv_func_mmap_fixed_mapped=no",
            "ac_cv_member_struct_stat_st_atim_tv_nsec=yes",
            "ac_cv_typeof_struct_stat_st_atim_is_struct_timespec=yes",
        ],
        "Bash WOS target feature/configure preseeds",
    )
    require_tokens(
        config_site,
        [
            "ac_cv_sizeof_char=1",
            "ac_cv_sizeof_short=2",
            "ac_cv_sizeof_int=4",
            "ac_cv_sizeof_long=8",
            "ac_cv_sizeof_long_long=8",
            "ac_cv_sizeof_char_p=8",
            "ac_cv_sizeof_size_t=8",
            "ac_cv_type_pid_t=yes",
            "ac_cv_type_uid_t=yes",
            "ac_cv_type_gid_t=yes",
            "ac_cv_type_size_t=yes",
            "ac_cv_type_ssize_t=yes",
            "ac_cv_type_time_t=yes",
            "ac_cv_type_uintptr_t=yes",
        ],
        "Bash WOS target ABI/type preseeds",
    )
    require_tokens(
        config_site,
        [
            "bash_cv_func_sigsetjmp=present",
            "bash_cv_getcwd_malloc=no",
            "bash_cv_struct_winsize_header=ioctl_h",
            "bash_cv_struct_winsize_ioctl=yes",
            "bash_cv_wcwidth_broken=no",
            "bash_cv_wexitstatus_offset=0",
            "bash_cv_std_putenv=yes",
            "bash_cv_std_unsetenv=yes",
        ],
        "Bash WOS runtime behavior preseeds",
    )
    forbidden = [
        "ac_cv_prog_CC=",
        "ac_cv_prog_CPP=",
        "ac_cv_prog_AR=",
        "ac_cv_path_install=",
    ]
    present = [token for token in forbidden if token in config_site]
    if present:
        fail("Bash config.site must not pin host/tool paths: " + ", ".join(present))


def test_nasm_script_uses_release_tarball_without_self_hosted_autogen() -> None:
    source = WOS_NASM_BUILD.read_text()
    require_tokens(
        source,
        [
            'NASM_VERSION="${WOS_NASM_VERSION:-3.02rc9}"',
            "https://www.nasm.us/pub/nasm/releasebuilds/$NASM_VERSION/nasm-$NASM_VERSION.tar.xz",
            "1802d091f4b2c1b3f61ab9d9fca323b8da7674c1ced7d5b770e77604d9c7925b",
            "download_nasm_source()",
            "resolve_nasm_source()",
            "seed_nasm_generated_files()",
            'local fallback_src="$B/src/nasm-$NASM_VERSION"',
            'tar -xJf "$archive" -C "$tmp_dest" --strip-components 1',
            'NASM_SOURCE_DIR="$(resolve_nasm_source)"',
            'require_file "$NASM_SOURCE_DIR/configure"',
            'patch_config_sub_for_wos "$NASM_SOURCE_DIR/autoconf/helpers/config.sub"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'BUILD_TRIPLE="$TARGET_ARCH"',
            'BUILD_TRIPLE="$("$NASM_SOURCE_DIR/autoconf/helpers/config.guess")"',
            '[ "$(cat "$NASM_BUILD/source.path")" != "$NASM_SOURCE_DIR" ]',
            'wos_remove_tree "$NASM_BUILD"',
            'printf \'%s\\n\' "$NASM_SOURCE_DIR" > "$NASM_BUILD/source.path"',
            '"$NASM_SOURCE_DIR/configure" \\',
            "x86/insns.xda",
            "asm/directiv.h",
            'cp -p "$NASM_SOURCE_DIR/$file" "$NASM_BUILD/$file"',
            'wos_refresh_file_mtime "$NASM_BUILD/$file"',
            "editors/nasmtok.json",
            'export CPPFLAGS=""',
            'grep -F -- "-I$TARGET_SYSROOT/include" "$NASM_BUILD/Makefile"',
            'rm -f "$NASM_BUILD/Makefile" "$NASM_BUILD/config.status"',
            "seed_nasm_generated_files",
        ],
        "NASM WOS release source fallback",
    )

    forbidden = ["./autogen.sh", "Generating NASM configure script", 'CPPFLAGS="-I$TARGET_SYSROOT/include"']
    present = [token for token in forbidden if token in source]
    if present:
        fail("NASM WOS build must not require self-hosted Automake/autogen: " + ", ".join(present))


def test_nasm_script_preseeds_wos_target_configure_probes() -> None:
    source = WOS_NASM_BUILD.read_text()
    config_site_start = source.find("write_nasm_config_site()")
    config_site_end = source.find("require_file \"$HOST/bin/clang\"", config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("NASM WOS build script must keep write_nasm_config_site before host-tool validation")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "write_nasm_config_site()",
            'tmp_config_site="$(mktemp "$NASM_BUILD/config.site.XXXXXX")"',
            "ac_cv_func_canonicalize_file_name=yes",
            "ac_cv_func_faccessat=yes",
            "ac_cv_func_mempcpy=yes",
            "ac_cv_func_mmap_fixed_mapped=no",
            "ac_cv_func_strlcpy=yes",
            "ac_cv_header_machine_endian_h=yes",
            "ac_cv_header_stdbit_h=no",
            "ac_cv_prog_cc_c23=-std=gnu23",
            "ac_cv_search_inflate=no",
            "pa_cv_CFLAGS__Werror_unknown_warning_option=yes",
            "pa_cv_CFLAGS__ftrivial_auto_var_init_zero=yes",
            "pa_cv_func___builtin_prefetch=yes",
            "pa_cv_func_htole64=yes",
            "pa_cv_func_snprintf=snprintf",
        ],
        "NASM WOS target configure preseeds",
    )
    require_tokens(
        source,
        [
            "write_nasm_config_site",
            'export CONFIG_SITE="$NASM_BUILD/config.site"',
            '[ "$NASM_BUILD/config.site" -nt "$NASM_BUILD/Makefile" ]',
        ],
        "NASM config.site export and reconfigure trigger",
    )
    forbidden = [
        "ac_cv_prog_CC=",
        "ac_cv_prog_AR=",
        "ac_cv_prog_RANLIB=",
        "ac_cv_prog_STRIP=",
        "ac_cv_path_install=",
        "ac_cv_path_mkdir=",
    ]
    present = [token for token in forbidden if token in config_site]
    if present:
        fail("NASM config.site must not pin host/tool paths: " + ", ".join(present))


def test_cpython_script_uses_target_build_triplet_on_native_wos() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            'TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'patch_config_sub_for_wos "$PYTHON_SRC/config.sub"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'BUILD_TRIPLE="$TARGET_ARCH"',
            'BUILD_TRIPLE="$("$PYTHON_SRC/config.guess")"',
            'PYTHON_HOST_CONFIGURE_BUILD_ARGS=()',
            'PYTHON_CONFIGURE_CACHE_ARGS=()',
            'PYTHON_HOST_CONFIGURE_BUILD_ARGS=(--build="$BUILD_TRIPLE")',
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            '"${PYTHON_CONFIGURE_CACHE_ARGS[@]}"',
            '"${PYTHON_HOST_CONFIGURE_BUILD_ARGS[@]}"',
            "--disable-ipv6",
            'grep -Fq -- "-isystem $HOST/lib/clang/22/include" "$makefile"',
            'grep -Fq -- "-isystem $TARGET_SYSROOT/include" "$makefile"',
            'PYTHON_CPPFLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"',
            '--build="$BUILD_TRIPLE"',
            '--host="$TARGET_ARCH"',
        ],
        "CPython native WOS build triplet handling",
    )


def test_cpython_target_configure_preseeds_wos_runtime_probes() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    config_site_start = source.find("write_config_site()")
    config_site_end = source.find("write_libressl_sigalgs_compat_header()", config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("CPython WOS build script must keep write_config_site before LibreSSL compatibility setup")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "ac_cv_file__dev_ptmx=no",
            "ac_cv_func_getrandom=no",
            "ac_cv_have_decl_PR_SET_VMA_ANON_NAME=no",
            "ac_cv_pthread_is_default=no",
            "ac_cv_kpthread=no",
            "ac_cv_kthread=no",
            "ac_cv_pthread=no",
            "ac_cv_cxx_thread=no",
            "ac_cv_pthread_system_supported=no",
            "ac_cv_posix_semaphores_enabled=yes",
            "ac_cv_broken_sem_getvalue=yes",
            "ac_cv_aligned_required=yes",
            "ac_cv_wchar_t_signed=yes",
            "ac_cv_rshift_extends_sign=yes",
            "ac_cv_computed_gotos=no",
            "ac_cv_broken_nice=no",
            "ac_cv_broken_poll=no",
            "ac_cv_working_tzset=no",
            "ac_cv_broken_mbstowcs=no",
            "ac_cv_have_chflags=no",
            "ac_cv_have_lchflags=no",
            "ac_cv_c_compiler_gnu=yes",
            "ac_cv_cc_name=clang",
            "ac_cv_cc_supports_fstrict_overflow=yes",
            "ac_cv_header_alloca_h=yes",
            "ac_cv_header_sys_epoll_h=yes",
            "ac_cv_header_sys_eventfd_h=no",
            "ac_cv_header_stdatomic_h=no",
            "ac_cv_func_fork=yes",
            "ac_cv_func_posix_spawn=yes",
            "ac_cv_func_socket=yes",
            "ac_cv_func_timerfd_create=no",
            "ac_cv_have_decl_RTLD_NOW=yes",
            "ac_cv_member_struct_stat_st_blksize=yes",
            "ac_cv_type_sockaddr_storage=yes",
        ],
        "CPython target configure WOS runtime probe preseeds",
    )
    require_tokens(
        config_site,
        [
            "ac_cv_sizeof_int=4",
            "ac_cv_sizeof_long=8",
            "ac_cv_sizeof_long_long=8",
            "ac_cv_sizeof_void_p=8",
            "ac_cv_sizeof_size_t=8",
            "ac_cv_sizeof_off_t=8",
            "ac_cv_sizeof_time_t=8",
            "ac_cv_sizeof_pthread_t=8",
            "ac_cv_sizeof_pthread_key_t=8",
            "ac_cv_sizeof_uintptr_t=8",
            "ac_cv_alignof_long=8",
            "ac_cv_alignof_size_t=8",
            "ac_cv_alignof_max_align_t=16",
            "ac_cv_pthread_key_t_is_arithmetic_type=no",
        ],
        "CPython target configure WOS ABI size preseeds",
    )


def test_cpython_selfhost_uses_existing_build_python_when_available() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            "python_source_version()",
            'require_file "$patchlevel" "CPython source is missing Include/patchlevel.h."',
            "PY_MAJOR_VERSION",
            "PY_MINOR_VERSION",
            "python_interpreter_matches_source()",
            'expected = tuple(int(part) for part in sys.argv[1].split("."))',
            "find_compatible_build_python()",
            'local explicit="${WOS_PYTHON_BUILD_PYTHON:-}"',
            'for candidate in "python$expected_version" python3.16 python3 python; do',
            'if [ "$HOST_SYSTEM" = "WOS" ] || [ -n "${WOS_PYTHON_BUILD_PYTHON:-}" ]; then',
            'if ! EXTERNAL_BUILD_PYTHON="$(find_compatible_build_python)"; then',
            'echo "Using existing build Python $EXTERNAL_BUILD_PYTHON for CPython build helpers"',
            'BUILD_PYTHON="$EXTERNAL_BUILD_PYTHON"',
            'grep -Fq "PYTHON_FOR_FREEZE=$BUILD_PYTHON" "$makefile"',
        ],
        "CPython WOS selfhost build Python fallback",
    )


def test_cpython_install_avoids_sysroot_usr_symlink() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            'echo "Installing target CPython with WOS_MAKE_JOBS=$WOS_MAKE_JOBS..."',
            'wos_make "$WOS_MAKE_JOBS" -C "$PYTHON_TARGET_BUILD" \\',
            "--prefix=/usr",
            "--exec-prefix=/usr",
            "prefix= \\",
            "exec_prefix= \\",
            'DESTDIR="$TARGET_SYSROOT"',
            'require_file "$TARGET_SYSROOT/bin/python3"',
        ],
        "CPython native WOS install path",
    )


def test_native_wos_bootstrap_keeps_host_toolchain_shim_discoverable() -> None:
    source = BOOTSTRAP.read_text()
    require_tokens(
        source,
        [
            'local compat_root="$WORKSPACE_ROOT/toolchain/host"',
            'WOS_HOST_TOOLCHAIN_ROOT="$WORKSPACE_ROOT/toolchain/wos-host-shim"',
            'ln -sfn "$(basename "$shim_root")" host',
            'elif [ ! -x "$compat_root/bin/clang" ]; then',
            "llvm-tblgen clang-tblgen",
            "Host toolchain compatibility path",
        ],
        "native WOS bootstrap host-toolchain compatibility shim",
    )


def test_wos_toolchain_uses_shared_busybox_and_dropbear_build_scripts() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            'bootstrap_phase_start 7 "BusyBox for WOS userspace"',
            'bootstrap_phase_start 8 "Dropbear SSH for WOS userspace"',
            'bootstrap_phase_start 9 "GNU make for WOS userspace"',
            'bootstrap_phase_start 10 "Bash for WOS userspace"',
            'bootstrap_phase_start 11 "Ninja for WOS userspace"',
            'bootstrap_phase_start 12 "CMake for WOS userspace"',
            'bootstrap_phase_start 13 "CPython for WOS userspace"',
            'bootstrap_phase_start 14 "Meson for WOS userspace"',
            'bootstrap_phase_start 15 "NASM for WOS userspace"',
            'bootstrap_phase_start 16 "zlib LibreSSL and curl for WOS userspace"',
            'bootstrap_phase_start 17 "Git for WOS userspace"',
            'bootstrap_phase_start 18 "clang/lld for WOS userspace"',
            'WOS_HOST_TOOLCHAIN_ROOT="$HOST" \\',
            'WOS_BUSYBOX_BUILD_DIR="$B/busybox-build" \\',
            'WOS_BUSYBOX_INSTALL_DIR="$B/busybox-install" \\',
            '"$B/../scripts/build/build_busybox.sh"',
            'WOS_DROPBEAR_BUILD_DIR="$B/dropbear-build" \\',
            '"$B/../scripts/build/build_dropbear.sh"',
            'WOS_BASH_BUILD_DIR="$B/bash-build" \\',
            '"$B/../scripts/build/build_bash_for_wos.sh"',
            'WOS_HOST_TOOLCHAIN_ROOT="$HOST" \\',
            'WOS_CLANG_FOR_WOS_BUILD_DIR="$B/clang-wos-build" \\',
            '"$B/../scripts/build/build_clang_for_wos.sh"',
        ],
        "WOS target toolchain BusyBox/Dropbear script delegation",
    )

    forbidden = [
        'HOSTCC="${WOS_CCACHE_PREFIX}/usr/bin/clang"',
        "cp $B/busybox-build/busybox $SYSROOT/bin/busybox",
        "autoconf && autoheader",
        "cp $B/dropbear-build/dropbearmulti $SYSROOT/bin/dropbearmulti",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("WOS target toolchain must not keep stale inline BusyBox/Dropbear build logic: " + ", ".join(present))


def test_native_wos_clang_port_stages_tablegen_for_next_self_host() -> None:
    source = WOS_CLANG_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_COMMON_FLAGS="--sysroot=$TARGET_SYSROOT',
            'TARGET_C_INCLUDE_FLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"',
            'TARGET_C_FLAGS="$TARGET_COMMON_FLAGS $TARGET_C_INCLUDE_FLAGS"',
            'TARGET_CXX_FLAGS="$TARGET_COMMON_FLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1 $TARGET_C_INCLUDE_FLAGS"',
            "collect_wos_llvm_bin_outputs()",
            'ninja -C "$CLANG_BUILD" -j"$WOS_NINJA_JOBS" "${WOS_LLVM_BIN_OUTPUTS[@]}"',
            "install_built_toolset",
            "llvm-symbolizer",
            "llvm-tblgen",
            "clang-tblgen",
            '-DLLVM_TABLEGEN="$HOST/bin/llvm-tblgen"',
            '-DCLANG_TABLEGEN="$HOST/bin/clang-tblgen"',
        ],
        "native WOS clang tool staging",
    )
    if "< <(" in source:
        fail("native WOS clang tool staging must not use Bash process substitution; WOS Bash rejects it")


def test_busybox_and_dropbear_scripts_honor_host_toolchain_override() -> None:
    busybox = WOS_BUSYBOX_BUILD.read_text()
    dropbear = WOS_DROPBEAR_BUILD.read_text()
    require_tokens(
        busybox,
        [
            'export CCACHE_DIR="${TMPDIR:-/tmp}/wos-busybox-ccache"',
            'B="$WORKSPACE_ROOT/toolchain"',
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            "native_host_path() {",
            "find_native_host_tool() {",
            'BB_NATIVE_HOSTCC="$(find_native_host_tool cc || true)"',
            'BB_HOSTCC="${WOS_CCACHE_PREFIX}$BB_NATIVE_HOSTCC"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'if [ "$HOST_SYSTEM" = "WOS" ] && [ -x /usr/bin/clang ]; then',
            'BB_CLANG_RESOURCE_DIR="$(/usr/bin/clang --target=x86_64-pc-wos -print-resource-dir 2>/dev/null || true)"',
            'BB_CC="${WOS_CCACHE_PREFIX}/usr/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT -resource-dir $BB_CLANG_RESOURCE_DIR"',
            'BB_CC="$BB_CC --config=$HOST/bin/x86_64-pc-wos.cfg"',
            "BB_MAKE_SHELL_ARGS=()",
            "BB_MAKE_SHELL_ARGS=(SHELL=/bin/bash CONFIG_SHELL=/bin/bash)",
            'BB_INSTALL="${WOS_BUSYBOX_INSTALL_DIR:-$B/busybox-install}"',
            'BB_CONFIG="${WOS_BUSYBOX_CONFIG:-$WORKSPACE_ROOT/configs/busybox/wos_full.config}"',
        ],
        "BusyBox WOS build script host-toolchain override",
    )
    require_tokens(
        busybox,
        [
            "build_busybox_target()",
            "cleanup_busybox_kbuild_temps()",
            "setup_busybox_kbuild_tools()",
            'BB_KBUILD_TOOLS="$BB_BUILD/wos-kbuild-tools"',
            'if [ "$HOST_SYSTEM" != "WOS" ]; then',
            "#!/usr/bin/python3",
            'PATH="$BB_KBUILD_TOOLS:$PATH"',
            "setup_busybox_kbuild_tools",
            'BusyBox WOS full config not found at $BB_CONFIG',
            'outputmakefile >/tmp/busybox_outputmakefile.log 2>&1',
            'tmp_config="$BB_BUILD/.config.wos"',
            'cp "$BB_CONFIG" "$tmp_config"',
            'cmp -s "$tmp_config" "$BB_BUILD/.config"',
            'mv "$tmp_config" "$BB_BUILD/.config"',
            '"${BB_MAKE_SHELL_ARGS[@]}"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'wos_make "$WOS_MAKE_JOBS" -C "$BB_BUILD" \\',
            "BusyBox build failed; cleaning Kbuild temp/dependency files and retrying once at WOS_MAKE_JOBS=$WOS_MAKE_JOBS",
            "cleanup_busybox_kbuild_temps",
            "build_busybox_target",
            "-name '.*.tmp' -o -name '.*.d'",
            'install >/tmp/busybox_install.log 2>&1',
        ],
        "BusyBox WOS install uses GNU Make jobserver policy",
    )
    assert 'yes "" | wos_make' not in busybox
    assert " allnoconfig " not in busybox
    assert " oldconfig " not in busybox
    require_tokens(
        ROOT_CMAKE.read_text(),
        [
            'set(WOS_BUSYBOX_CONFIG "${CMAKE_SOURCE_DIR}/configs/busybox/wos_full.config" CACHE FILEPATH "Resolved BusyBox WOS config")',
            'WOS_BUSYBOX_CONFIG=${WOS_BUSYBOX_CONFIG}',
            '${WOS_BUILD_SCRIPTS_DIR}/build_busybox.sh ${WOS_BUSYBOX_CONFIG} ${BUSYBOX_SOURCES}',
        ],
        "BusyBox full config CMake dependency",
    )
    require_tokens(
        dropbear,
        [
            'export CCACHE_DIR="${TMPDIR:-/tmp}/wos-dropbear-ccache"',
            'B="$WORKSPACE_ROOT/toolchain"',
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"',
            'DB_BUILD="${WOS_DROPBEAR_BUILD_DIR:-$B/dropbear-build}"',
            'DROPBEAR_CONFIGURE_BUILD_ARGS=()',
            'DROPBEAR_CONFIGURE_CACHE_ARGS=()',
            'uname -s 2>/dev/null || printf unknown',
            'DROPBEAR_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            'ac_cv_func_memcmp_working=yes',
            'ac_cv_func_endutent=no',
            'ac_cv_func_endutxent=no',
            'ac_cv_func_getusershell=no',
            'ac_cv_func_getutent=no',
            'ac_cv_func_getutxent=no',
            'ac_cv_func_getutid=no',
            'ac_cv_func_getutxid=no',
            'ac_cv_func_getutline=no',
            'ac_cv_func_getutxline=no',
            'ac_cv_func_pututline=no',
            'ac_cv_func_pututxline=no',
            'ac_cv_func_setutent=no',
            'ac_cv_func_setutxent=no',
            'ac_cv_func_utmpname=no',
            'ac_cv_func_utmpxname=no',
            '"${DROPBEAR_CONFIGURE_CACHE_ARGS[@]}"',
            '"${DROPBEAR_CONFIGURE_BUILD_ARGS[@]}"',
            '--host="$TARGET_ARCH"',
        ],
        "Dropbear WOS build script host-toolchain override",
    )


def test_dropbear_script_preseeds_wos_target_configure_probes() -> None:
    source = WOS_DROPBEAR_BUILD.read_text()
    config_site_start = source.find("write_dropbear_config_site()")
    config_site_end = source.find('LOCALOPTIONS="$DB_BUILD/localoptions.h"', config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("Dropbear WOS build script must keep write_dropbear_config_site before localoptions setup")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "write_dropbear_config_site()",
            'tmp_config_site="$(mktemp "$DB_BUILD/config.site.XXXXXX")"',
            "ac_cv_func_clock_gettime=yes",
            "ac_cv_func_explicit_bzero=yes",
            "ac_cv_func_getaddrinfo=yes",
            "ac_cv_func_getrandom=no",
            "ac_cv_func_strlcat=yes",
            "ac_cv_func_strlcpy=yes",
            "ac_cv_have_struct_sockaddr_storage=yes",
            "ac_cv_header_sys_random_h=no",
            "ac_cv_header_utmp_h=yes",
            "ac_cv_header_utmpx_h=no",
            "ac_cv_search_openpty='none required'",
            "dropbear_cv_func_have_openpty=yes",
        ],
        "Dropbear WOS target configure preseeds",
    )
    require_tokens(
        config_site,
        [
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            "ac_cv_func_memcmp_working=yes",
            "ac_cv_func_getusershell=no",
            "ac_cv_func_getutxent=no",
            "ac_cv_func_pututxline=no",
            "ac_cv_func_utmpxname=no",
            "ac_cv_func_memcmp_working=no",
            "ac_cv_func_getusershell=yes",
            "ac_cv_func_getutxent=yes",
            "ac_cv_func_pututxline=yes",
            "ac_cv_func_utmpxname=yes",
        ],
        "Dropbear native WOS and Linux-cross configure split",
    )
    require_tokens(
        source,
        [
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            "write_dropbear_config_site",
            'export CONFIG_SITE="$DB_BUILD/config.site"',
            '[ "$DB_BUILD/config.site" -nt "$DB_BUILD/Makefile" ]',
        ],
        "Dropbear config.site export and reconfigure trigger",
    )
    forbidden = [
        "ac_cv_prog_CC=",
        "ac_cv_prog_AR=",
        "ac_cv_prog_RANLIB=",
        "ac_cv_prog_STRIP=",
        "ac_cv_path_install=",
        "ac_cv_build=",
        "ac_cv_host=",
    ]
    present = [token for token in forbidden if token in config_site]
    if present:
        fail("Dropbear config.site must not pin host/tool paths or build tuples: " + ", ".join(present))


def test_mlibc_script_honors_host_toolchain_override_and_job_helper() -> None:
    source = WOS_MLIBC_BUILD.read_text()
    require_tokens(
        source,
        [
            'B="$WORKSPACE_ROOT/toolchain"',
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'WOS_BUILD_JOBS="$(wos_build_jobs)"',
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            'ninja -C "$MLIBC_BUILD" -j"$WOS_NINJA_JOBS"',
            'ninja -C "$MLIBC_BUILD" -j"$WOS_NINJA_JOBS" install',
        ],
        "mlibc WOS build script host-toolchain and job helper",
    )


def test_ninja_script_uses_target_header_stack() -> None:
    source = WOS_NINJA_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_CXX_FLAGS="--sysroot=$TARGET_SYSROOT',
            "-isystem $TARGET_SYSROOT/include/c++/v1",
            "-isystem $HOST/lib/clang/22/include",
            "-isystem $TARGET_SYSROOT/include",
            'cached_cxx="$(sed -n \'s/^CMAKE_CXX_COMPILER:[^=]*=//p\' "$NINJA_BUILD/CMakeCache.txt")"',
            'if [ -n "$cached_cxx" ] && [ "$cached_cxx" != "$HOST/bin/clang++" ]; then',
            'rm -rf "$NINJA_BUILD"',
        ],
        "Ninja WOS build script target header stack",
    )


def test_cmake_script_uses_target_header_stack() -> None:
    source = WOS_CMAKE_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_C_INCLUDE_FLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"',
            'TARGET_C_FLAGS="$TARGET_COMMON_FLAGS $TARGET_C_INCLUDE_FLAGS"',
            'TARGET_CXX_FLAGS="$TARGET_COMMON_FLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1 $TARGET_C_INCLUDE_FLAGS"',
            'TARGET_RELEASE_FLAGS="-O2 -DNDEBUG"',
            'cached_cxx="$(sed -n \'s/^CMAKE_CXX_COMPILER:[^=]*=//p\' "$CMAKE_BUILD/CMakeCache.txt")"',
            'cached_cxx_flags="$(sed -n \'s/^CMAKE_CXX_FLAGS:[^=]*=//p\' "$CMAKE_BUILD/CMakeCache.txt")"',
            'if [ -n "$cached_cxx" ] && [ "$cached_cxx" != "$HOST/bin/clang++" ]; then',
            '*" -isystem $TARGET_SYSROOT/include "*)',
            'if [ -d "$CMAKE_BUILD/CMakeFiles" ] && [ ! -f "$CMAKE_BUILD/CMakeCache.txt" ]; then',
            'wos_remove_tree "$CMAKE_BUILD"',
            '-DCMAKE_INSTALL_PREFIX="$TARGET_SYSROOT"',
            'cmake --install "$CMAKE_BUILD" --prefix "$TARGET_SYSROOT"',
        ],
        "CMake WOS build script target header stack",
    )


def test_native_wos_cmake_configure_preseeds_expensive_checks() -> None:
    cmake_script = WOS_CMAKE_BUILD.read_text()
    build_utilities = CMAKE_BUILD_UTILITIES.read_text()
    third_party_checks = CMAKE_THIRD_PARTY_CHECKS.read_text()

    require_tokens(
        cmake_script,
        [
            "-DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY",
            "-DCMake_CXX17_WORKS=TRUE",
            "-DCMake_HAVE_CXX_MAKE_UNIQUE=TRUE",
            "-DCMake_HAVE_CXX_UNIQUE_PTR=1",
            "-DCMake_HAVE_CXX_FILESYSTEM=TRUE",
            "-DCMake_BUILD_PCH=ON",
            "-DCMAKE_HAVE_LIBC_PTHREAD=1",
            "-DHAVE_UNSETENV=1",
            "-DHAVE_ENVIRON_NOT_REQUIRE_PROTOTYPE=0",
            '-DCMAKE_C_FLAGS_RELEASE="$TARGET_RELEASE_FLAGS"',
            '-DCMAKE_CXX_FLAGS_RELEASE="$TARGET_RELEASE_FLAGS"',
        ],
        "CMake-for-WOS top-level configure preseeds",
    )

    require_tokens(
        build_utilities,
        [
            'if(CMAKE_SYSTEM_NAME STREQUAL "WOS")',
            "set(KWSYS_C_HAS_CLOCK_GETTIME_MONOTONIC_COMPILED 1)",
            "set(KWSYS_C_HAS_SSIZE_T_COMPILED 1)",
            "set(KWSYS_CXX_HAS_BACKTRACE_COMPILED 1)",
            "set(KWSYS_CXX_HAS_CXXABI_COMPILED 1)",
            "set(KWSYS_CXX_HAS_DLADDR_COMPILED 1)",
            "set(KWSYS_CXX_HAS_ENVIRON_IN_STDLIB_H_COMPILED 0)",
            "set(KWSYS_CXX_HAS_GETLOADAVG_COMPILED 1)",
            "set(KWSYS_CXX_HAS_RLIMIT64_COMPILED 0)",
            "set(KWSYS_CXX_HAS_SETENV_COMPILED 1)",
            "set(KWSYS_CXX_HAS_UNSETENV_COMPILED 1)",
            "set(KWSYS_CXX_STAT_HAS_ST_MTIM_COMPILED 1)",
            "set(KWSYS_CXX_STAT_HAS_ST_MTIMESPEC_COMPILED 0)",
            "set(KWSYS_SYS_HAS_IFADDRS_H 1)",
        ],
        "CMake-for-WOS KWSys configure preseeds",
    )

    wos_third_party_match = re.search(
        r'if\(CMAKE_SYSTEM_NAME STREQUAL "WOS"\)(?P<body>.*?)\nendif\(\)',
        third_party_checks,
        flags=re.DOTALL,
    )
    if wos_third_party_match is None:
        fail("CMake third-party checks must include a WOS-specific preseed block")
    wos_third_party_block = wos_third_party_match.group("body")
    crypto_preseeds = [
        "ARCHIVE_CRYPTO_MD5_LIBC",
        "ARCHIVE_CRYPTO_MD5_LIBSYSTEM",
        "ARCHIVE_CRYPTO_RMD160_LIBC",
        "ARCHIVE_CRYPTO_SHA1_LIBC",
        "ARCHIVE_CRYPTO_SHA1_LIBSYSTEM",
        "ARCHIVE_CRYPTO_SHA256_LIBC",
        "ARCHIVE_CRYPTO_SHA256_LIBC2",
        "ARCHIVE_CRYPTO_SHA256_LIBC3",
        "ARCHIVE_CRYPTO_SHA256_LIBSYSTEM",
        "ARCHIVE_CRYPTO_SHA384_LIBC",
        "ARCHIVE_CRYPTO_SHA384_LIBC2",
        "ARCHIVE_CRYPTO_SHA384_LIBC3",
        "ARCHIVE_CRYPTO_SHA384_LIBSYSTEM",
        "ARCHIVE_CRYPTO_SHA512_LIBC",
        "ARCHIVE_CRYPTO_SHA512_LIBC2",
        "ARCHIVE_CRYPTO_SHA512_LIBC3",
        "ARCHIVE_CRYPTO_SHA512_LIBSYSTEM",
    ]
    require_tokens(
        wos_third_party_block,
        [f"set({name} 0)" for name in crypto_preseeds],
        "CMake-for-WOS libarchive crypto preseeds",
    )
    if re.search(r"ARCHIVE_CRYPTO_[A-Z0-9_]+\s+1", wos_third_party_block):
        fail("WOS libarchive crypto preseeds must not invent enabled digest backends")
    require_tokens(
        wos_third_party_block,
        [
            "set(HAVE_ACCEPT4 1)",
            "set(HAVE_SYS_EVENTFD_H 0)",
            "set(HAVE_GETRANDOM 0)",
            "set(HAVE_GETHOSTBYNAME_R_6 1)",
            "set(HAVE_SIZEOF_STRUCT_SOCKADDR_STORAGE TRUE)",
            "set(SIZEOF_STRUCT_SOCKADDR_STORAGE 128)",
            "set(HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1)",
            "set(HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC 0)",
            "set(HAVE_WORKING_FS_IOC_GETFLAGS 0)",
            "set(LIBMD_FOUND 0)",
            "set(SAFE_TO_DEFINE_EXTENSIONS 1)",
        ],
        "CMake-for-WOS broad third-party configure preseeds",
    )
    if "set(LIBMD_FOUND 1)" in wos_third_party_block:
        fail("WOS third-party preseeds must not pin libarchive's inconsistent positive libmd discovery result")


def test_wos_tls_build_is_self_hostable_without_perl() -> None:
    source = WOS_TLS_BUILD.read_text()
    require_tokens(
        source,
        [
            "LIBRESSL_VERSION=\"${WOS_LIBRESSL_VERSION:-4.3.2}\"",
            "https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-$LIBRESSL_VERSION.tar.gz",
            "edf01aee24c65d69e6a9efcb9d44bcda682ff9d4f3bbbd95e794e1dfa90847b5",
            "download_libressl_source",
            "resolve_tls_source",
            "patch_config_sub_for_wos \"$TLS_WORK/config.sub\"",
            "patch_arc4random_for_wos \"$TLS_WORK/crypto/compat/arc4random.h\"",
            "refresh_libressl_release_generated_files",
            "disable_libressl_man_install",
            "SUBDIRS = include crypto ssl tls apps man",
            "SUBDIRS = include crypto ssl tls apps",
            "sed -i 's/| fiwix\\* | mlibc\\* )/| fiwix* | mlibc* | wos* )/'",
            "defined(__WOS__)",
            "#include \"arc4random_netbsd.h\"",
            "aclocal.m4",
            "apps/openssl/Makefile.in",
            'wos_refresh_file_mtime "$TLS_WORK/$file"',
            "write_libressl_config_site",
            'export CONFIG_SITE="$TLS_WORK/config.site"',
            "-D__WOS__=1",
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            "TLS_CONFIGURE_BUILD_ARGS=()",
            "TLS_CONFIGURE_CACHE_ARGS=()",
            'TLS_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            '"${TLS_CONFIGURE_CACHE_ARGS[@]}"',
            '"${TLS_CONFIGURE_BUILD_ARGS[@]}"',
            "remove_cross_libtool_archives",
            "$TARGET_SYSROOT/lib/libssl.la",
            "./configure \\",
            "--host=\"$TARGET_ARCH\"",
            "--prefix=",
            "--libdir=/lib",
            "--disable-shared",
            "--enable-static",
            "--disable-tests",
            "--disable-asm",
            "disable_libressl_man_install",
            "wos_make \"$WOS_MAKE_JOBS\" -C \"$TLS_WORK\"",
            "prefix= \\",
            "exec_prefix= \\",
            "includedir=/include \\",
            "pkgconfigdir=/lib/pkgconfig \\",
            "DESTDIR=\"$TARGET_SYSROOT\" \\",
            "require_file \"$TARGET_SYSROOT/lib/libssl.a\"",
            "require_file \"$TARGET_SYSROOT/lib/libcrypto.a\"",
            "require_file \"$TARGET_SYSROOT/include/openssl/ssl.h\"",
        ],
        "WOS self-hosted TLS build path",
    )

    forbidden = [
        "command -v perl",
        "perl Configure",
        "OPENSSL_TARBALL_URL",
        "OPENSSL_TARBALL_SHA256",
        "--prefix=/usr",
        "--libdir=/usr/lib",
        "DESTDIR=\"$TARGET_SYSROOT\" install",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("WOS TLS build path must avoid Perl/OpenSSL Configure dependency: " + ", ".join(present))


def test_wos_tls_build_preseeds_target_configure_probes() -> None:
    source = WOS_TLS_BUILD.read_text()
    config_site_start = source.find("write_libressl_config_site()")
    config_site_end = source.find('require_file "$HOST/bin/clang"', config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("LibreSSL WOS build script must keep write_libressl_config_site before host-tool validation")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "write_libressl_config_site()",
            'tmp_config_site="$(mktemp "$TLS_WORK/config.site.XXXXXX")"',
            "ac_cv_func_accept4=yes",
            "ac_cv_func_arc4random=no",
            "ac_cv_func_clock_gettime=yes",
            "ac_cv_func_explicit_bzero=yes",
            "ac_cv_func_getentropy=yes",
            "ac_cv_func_memmem=yes",
            "ac_cv_func_reallocarray=yes",
            "ac_cv_func_strlcpy=yes",
            "ac_cv_func_timingsafe_memcmp=no",
            "ac_cv_header_machine_endian_h=yes",
            "ac_cv_header_readpassphrase_h=no",
            "ac_cv_search_clock_gettime='none required'",
            "ac_cv_search_pthread_once='none required'",
            "ac_cv_sizeof_time_t=8",
        ],
        "LibreSSL WOS target configure preseeds",
    )
    require_tokens(
        config_site,
        [
            "am_cv_CC_dependencies_compiler_type=gcc3",
            "am_cv_prog_cc_c_o=yes",
            "am_cv_prog_tar_ustar=gnutar",
            "ax_cv_check_cflags___Werror=yes",
            "lt_cv_deplibs_check_method=unknown",
            "lt_cv_prog_compiler_pic='-fPIC -DPIC'",
            "lt_cv_prog_compiler_pic_works=yes",
            "lt_cv_sharedlib_from_linklib_cmd='printf %s\\n'",
            "lt_cv_to_host_file_cmd=func_convert_file_noop",
        ],
        "LibreSSL Automake/libtool configure preseeds",
    )
    require_tokens(
        source,
        [
            "write_libressl_config_site",
            'export CONFIG_SITE="$TLS_WORK/config.site"',
            '"${TLS_CONFIGURE_CACHE_ARGS[@]}"',
            '"${TLS_CONFIGURE_BUILD_ARGS[@]}"',
        ],
        "LibreSSL config.site export",
    )
    forbidden = [
        "ac_cv_prog_CC=",
        "ac_cv_prog_AR=",
        "ac_cv_prog_RANLIB=",
        "ac_cv_prog_STRIP=",
        "ac_cv_path_install=",
        "ac_cv_path_GREP=",
        "ac_cv_path_SED=",
        "ac_cv_build=",
        "ac_cv_host=",
        "lt_cv_path_LD=",
        "lt_cv_path_NM=",
    ]
    present = [token for token in forbidden if token in config_site]
    if present:
        fail("LibreSSL config.site must not pin host/tool paths or build tuples: " + ", ".join(present))


def test_zlib_install_avoids_sysroot_usr_symlink() -> None:
    source = (ROOT / "scripts" / "build" / "build_zlib_for_wos.sh").read_text()
    require_tokens(
        source,
        [
            '"$ZLIB_SOURCE_DIR/configure" --prefix= --static',
            "prefix= \\",
            "exec_prefix= \\",
            "libdir=/lib \\",
            "includedir=/include \\",
            "pkgconfigdir=/lib/pkgconfig \\",
            'DESTDIR="$TARGET_SYSROOT" \\',
            "install",
        ],
        "zlib WOS install path avoids sysroot /usr symlink",
    )

    forbidden = ['"$ZLIB_SOURCE_DIR/configure" --prefix=/usr --static', 'DESTDIR="$TARGET_SYSROOT" install']
    present = [token for token in forbidden if token in source]
    if present:
        fail("zlib WOS install must avoid installing through the sysroot /usr symlink: " + ", ".join(present))


def test_wos_curl_build_uses_native_triplet_and_real_sysroot_install() -> None:
    source = WOS_CURL_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            "CURL_CONFIGURE_BUILD_ARGS=()",
            "CURL_CONFIGURE_CACHE_ARGS=()",
            'CURL_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            "ac_cv_path_SED=/usr/bin/sed",
            '"ac_cv_path_install=/usr/bin/install -c"',
            "ac_cv_path_lt_DD=/usr/bin/dd",
            "ac_cv_prog_AWK=awk",
            "ac_cv_prog_ac_ct_OBJDUMP=objdump",
            "lt_cv_path_LD=/usr/bin/ld",
            '"lt_cv_path_NM=/usr/bin/nm -B"',
            "lt_cv_path_mainfest_tool=no",
            '"lt_cv_truncate_bin=/usr/bin/dd bs=4096 count=1"',
            "patch_getifaddrs_probe_for_wos",
            'patch_getifaddrs_probe_for_wos "$CURL_WORK/configure"',
            "WOS skips curl getifaddrs run probe",
            'tst_works_getifaddrs=\\"no\\"',
            'if false && test \\"$cross_compiling\\" != \\"yes\\" &&',
            '"${CURL_CONFIGURE_CACHE_ARGS[@]}"',
            '"${CURL_CONFIGURE_BUILD_ARGS[@]}"',
            '--host="$TARGET_ARCH"',
            "--prefix=",
            'wos_copy_tree_entries_excluding "$source_dir" "$CURL_WORK" ".git" ".github" "tests"',
            'wos_copy_tree_entries_excluding "$source_dir/tests" "$CURL_WORK/tests" "data"',
            "for metadata in Makefile.am Makefile.in; do",
            'cp "$source_dir/tests/data/$metadata" "$CURL_WORK/tests/data/"',
            "--bindir=/bin",
            "--libdir=/lib",
            "--includedir=/include",
            "prefix= \\",
            "exec_prefix= \\",
            "bindir=/bin \\",
            "libdir=/lib \\",
            "includedir=/include \\",
            'CURL_TARGET_FLAGS="--target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"',
            'CURL_CFLAGS="$CURL_TARGET_FLAGS -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector"',
            'CURL_CPPFLAGS="$CURL_TARGET_FLAGS -I$TARGET_SYSROOT/include"',
            'CURL_LDFLAGS="$CURL_TARGET_FLAGS -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"',
            'export CC="$HOST/bin/clang"',
            'export CPPFLAGS="$CURL_CPPFLAGS"',
            'DESTDIR="$TARGET_SYSROOT" \\',
            'require_file "$TARGET_SYSROOT/bin/curl"',
            'require_file "$TARGET_SYSROOT/lib/libcurl.a"',
        ],
        "curl native WOS build triplet and install path",
    )

    forbidden = [
        "--prefix=/usr",
        'DESTDIR="$TARGET_SYSROOT" install',
        'export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"',
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("curl WOS install must avoid installing through the sysroot /usr symlink: " + ", ".join(present))


def test_wos_curl_build_preseeds_target_configure_probes() -> None:
    source = WOS_CURL_BUILD.read_text()
    config_site_start = source.find("write_curl_config_site()")
    config_site_end = source.find('require_file "$HOST/bin/clang"', config_site_start)
    if config_site_start < 0 or config_site_end < 0:
        fail("curl WOS build script must keep write_curl_config_site before host-tool validation")
    config_site = source[config_site_start:config_site_end]

    require_tokens(
        config_site,
        [
            "write_curl_config_site()",
            'tmp_config_site="$(mktemp "$CURL_WORK/config.site.XXXXXX")"',
            "ac_cv_func_SSL_set0_wbio=no",
            "ac_cv_func_SSL_set_quic_use_legacy_codepoint=yes",
            "ac_cv_func_accept4=yes",
            "ac_cv_func_eventfd=no",
            "ac_cv_func_pthread_create=yes",
            "ac_cv_header_openssl_ssl_h=yes",
            "ac_cv_header_stdatomic_h=no",
            "ac_cv_lib_ssl_SSL_connect=yes",
            "ac_cv_lib_crypto_HMAC_Update=yes",
            "ac_cv_lib_z_gzread=yes",
            "ac_cv_sizeof_curl_off_t=8",
            "ac_cv_sizeof_curl_socket_t=4",
            "ac_cv_type_struct_sockaddr_storage=yes",
        ],
        "curl WOS target configure preseeds",
    )
    require_tokens(
        config_site,
        [
            "curl_cv_native_windows=no",
            "curl_cv_struct_timeval=yes",
            "lt_cv_prog_compiler_pic_works=yes",
            "lt_cv_to_host_file_cmd=func_convert_file_noop",
        ],
        "curl WOS curl/libtool configure skip-key preseeds",
    )
    require_tokens(
        source,
        [
            "write_curl_config_site",
            'export CONFIG_SITE="$CURL_WORK/config.site"',
            '"${CURL_CONFIGURE_CACHE_ARGS[@]}"',
            '"${CURL_CONFIGURE_BUILD_ARGS[@]}"',
        ],
        "curl config.site export",
    )
    forbidden = [
        "ac_cv_prog_CC=",
        "ac_cv_prog_CPP=",
        "ac_cv_prog_AR=",
        "ac_cv_prog_RANLIB=",
        "ac_cv_prog_STRIP=",
        "ac_cv_path_install=",
        "ac_cv_path_PERL=",
        "ac_cv_build=",
        "ac_cv_host=",
        "curl_cv_func_",
        "curl_cv_writable_argv=",
        "lt_cv_path_LD=",
        "lt_cv_path_NM=",
    ]
    present = [token for token in forbidden if token in config_site]
    if present:
        fail("curl config.site must not pin host/tool paths or build tuples: " + ", ".join(present))


def test_wos_curl_download_errors_stop_before_checksum_or_extract() -> None:
    source = WOS_CURL_BUILD.read_text()
    require_tokens(
        source,
        [
            'CURL_TARBALL_URLS="${WOS_CURL_TARBALL_URLS:-$CURL_TARBALL_URL}"',
            'CURL_DOWNLOAD_ATTEMPTS="${WOS_CURL_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"',
            "download_curl_tarball()",
            'wos_download_file "curl $CURL_VERSION source" "$archive" "$CURL_TARBALL_URLS" "$CURL_DOWNLOAD_ATTEMPTS"',
            'download_curl_tarball "$archive"',
        ],
        "curl source download failure handling",
    )

    forbidden = [
        'curl -L "$CURL_TARBALL_URL" -o "$archive.tmp"',
        'mv "$archive.tmp" "$archive"\n    fi\n\n    echo "$CURL_TARBALL_SHA256',
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("curl source download failures must not continue into checksum/extract: " + ", ".join(present))


def test_wos_tarball_downloads_use_shared_retry_helper() -> None:
    helper = CCACHE_ENV.read_text()
    require_tokens(
        helper,
        [
            "wos_download_file()",
            'distdir="${WOS_SOURCE_DISTDIR:-}"',
            'if [ -n "$distdir" ]; then',
            'echo "Using cached $label from $candidate"',
            'cp "$candidate" "$dest.tmp"',
            'case "$attempts" in',
            'case "$delay" in',
            'case "$connect_timeout" in',
            'case "$low_speed_limit" in',
            'case "$low_speed_time" in',
            '--connect-timeout "$connect_timeout"',
            '--speed-limit "$low_speed_limit"',
            '--speed-time "$low_speed_time"',
            'command -v curl',
            'mv "$dest.tmp" "$dest"',
            'rm -f "$dest.tmp"',
            'echo "ERROR: failed to download $label."',
            'echo "Tried: $urls"',
        ],
        "shared source tarball downloader",
    )

    for label, path in WOS_TARBALL_DOWNLOAD_USERS.items():
        source = path.read_text()
        require_tokens(
            source,
            [
                "_TARBALL_URLS=",
                "_DOWNLOAD_ATTEMPTS=",
                "wos_download_file",
            ],
            f"{label} source download helper use",
        )

        forbidden = [
            'curl -L "$',
            'curl -fL "$',
            'mv "$archive.tmp" "$archive"',
        ]
        present = [token for token in forbidden if token in source]
        if present:
            fail(f"{label} source download must use wos_download_file, found: " + ", ".join(present))


def test_wos_git_install_avoids_sysroot_usr_symlink() -> None:
    source = WOS_GIT_BUILD.read_text()
    require_tokens(
        source,
        [
            '"prefix="',
            '"bindir=/bin"',
            '"gitexecdir=/libexec/git-core"',
            '"template_dir=/share/git-core/templates"',
            '"sysconfdir=/etc"',
            'GIT_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -I. -Icompat/regex -I$TARGET_SYSROOT/include"',
            '"NO_REGEX=NeedsStartEnd"',
            '"WOS_SKIP_TEST_ARTIFACTS=YesPlease"',
            "all:: $(FUZZ_OBJS)",
            "all:: $(TEST_PROGRAMS) $(test_bindir_programs) $(UNIT_TEST_PROGS) $(CLAR_TEST_PROG)",
            "ifndef WOS_SKIP_TEST_ARTIFACTS",
            "templates_makefile",
            "$(TAR) cf -",
            "cp -a blt/.",
            '"DESTDIR=$TARGET_SYSROOT"',
            'require_file "$TARGET_SYSROOT/bin/git"',
            'require_file "$TARGET_SYSROOT/libexec/git-core/git"',
            'require_file "$TARGET_SYSROOT/share/git-core/templates"',
        ],
        "Git WOS install path avoids sysroot /usr symlink",
    )

    forbidden = [
        '"prefix=/usr"',
        '"bindir=/usr/bin"',
        '"gitexecdir=/usr/libexec/git-core"',
        '"template_dir=/usr/share/git-core/templates"',
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("Git WOS install must avoid installing through the sysroot /usr symlink: " + ", ".join(present))


def test_wos_python_ssl_build_handles_libressl_sigalg_gap() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            "PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER",
            "write_libressl_sigalgs_compat_header()",
            "LIBRESSL_VERSION_NUMBER",
            "#define SSL_CTX_set1_client_sigalgs_list(ctx, sigalgslist) (0)",
            "#define SSL_CTX_set1_sigalgs_list(ctx, sigalgslist) (0)",
            'PYTHON_CPPFLAGS="$PYTHON_CPPFLAGS -include $PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER"',
            'CPPFLAGS="$PYTHON_CPPFLAGS"',
        ],
        "CPython _ssl LibreSSL compatibility patch",
    )


def test_mlibc_scanf_float_zero_counts_as_conversion() -> None:
    source = MLIBC_STDIO.read_text()
    require_tokens(
        source,
        [
            "if (c == '0') {\n\t\t\t\t\thandler.consume();\n\t\t\t\t\t++count;",
            "if (c == 'x' || c == 'X') {\n\t\t\t\t\t\tdivisor = 16;\n\t\t\t\t\t\tbase = 16;",
            "\t\t\t\t\t\tcount = 0;",
            "NOMATCH_CHECK(count == 0);",
        ],
        "mlibc scanf floating zero handling",
    )

    tests = MLIBC_SSCANF_TEST.read_text()
    require_tokens(
        tests,
        [
            'assert(sscanf("0", "%lg", &double_value) == 1);',
            'assert(sscanf("-0", "%lg", &double_value) == 1);',
            'assert(sscanf("+0", "%lg", &double_value) == 1);',
            'assert(sscanf("0e0", "%lg", &double_value) == 1);',
        ],
        "mlibc scanf floating zero regression tests",
    )


def test_mlibc_nameser_parser_does_not_panic_on_dns_packets() -> None:
    nameser = MLIBC_NAMESER.read_text()
    require_tokens(
        nameser,
        [
            "int ns_initparse(const unsigned char *msg, int msglen, ns_msg *handle)",
            "int ns_parserr(ns_msg *handle, ns_sect section, int rrnum, ns_rr *rr)",
            "int ns_name_uncompress(",
            "bool has_bytes(const unsigned char *ptr, const unsigned char *eom, size_t count)",
            "handle->_sections[section] = ptr;",
            "if ((label & NS_CMPRSFLGS) == NS_CMPRSFLGS)",
            "return fail_parse();",
        ],
        "mlibc DNS nameser parser",
    )
    if '__ensure(!"Not implemented")' in nameser:
        fail("mlibc nameser parser must reject malformed DNS packets instead of panicking")

    resolv = MLIBC_RESOLV.read_text()
    require_tokens(
        resolv,
        [
            "int dn_expand(",
            "return ns_name_uncompress(msg, eomorig, comp_dn, exp_dn, static_cast<size_t>(length));",
        ],
        "mlibc dn_expand DNS decompression wrapper",
    )


if __name__ == "__main__":
    test_compiler_rt_runs_real_cmake_checks_without_forced_response_files()
    test_compiler_rt_sanitizers_are_built_after_mlibc()
    test_native_wos_compiler_rt_does_not_link_with_workspace_sysroot()
    test_wos_toolchain_records_bootstrap_phase_timings()
    test_shared_build_timing_avoids_python_when_epochrealtime_exists()
    test_native_wos_build_defaults_keep_target_ports_enabled()
    test_preseeded_toolchain_mode_validates_bootstrap_outputs()
    test_preseeded_toolchain_mode_skips_external_source_scans()
    test_root_toolchain_builds_do_not_use_ninja_console_pool()
    test_wos_build_jobs_helper_has_self_hostable_fallbacks()
    test_top_level_build_paths_propagate_full_parallelism()
    test_libcxx_bootstrap_uses_host_tools_and_only_flag_preseeds()
    test_wos_mlibc_priority_sysdeps_are_syscall_backed()
    test_wos_port_build_scripts_use_shared_job_helper()
    test_gnu_make_port_build_scripts_use_make_job_helper()
    test_ninja_port_build_scripts_use_ninja_job_helper()
    test_host_toolchain_install_uses_ninja_jobs()
    test_wos_port_install_steps_keep_make_parallelism()
    test_wos_tar_invocations_use_busybox_compatible_long_options()
    test_wos_source_copy_wrappers_avoid_busybox_tar_pipelines()
    test_gnu_make_script_passes_build_triplet_on_wos()
    test_gnu_make_defaults_to_pipe_jobserver_on_wos()
    test_gnu_make_script_handles_native_wos_autoconf_probes()
    test_gnu_make_script_preseeds_wos_target_configure_probes()
    test_bash_script_falls_back_to_target_triplet_on_native_wos()
    test_bash_script_handles_native_wos_autoconf_maintainer_rules()
    test_bash_script_preseeds_native_wos_tool_probes()
    test_bash_script_enables_dev_fd_for_process_substitution()
    test_bash_script_preseeds_wos_target_configure_probes()
    test_nasm_script_uses_release_tarball_without_self_hosted_autogen()
    test_nasm_script_preseeds_wos_target_configure_probes()
    test_cpython_script_uses_target_build_triplet_on_native_wos()
    test_cpython_target_configure_preseeds_wos_runtime_probes()
    test_cpython_selfhost_uses_existing_build_python_when_available()
    test_cpython_install_avoids_sysroot_usr_symlink()
    test_native_wos_bootstrap_keeps_host_toolchain_shim_discoverable()
    test_wos_toolchain_uses_shared_busybox_and_dropbear_build_scripts()
    test_native_wos_clang_port_stages_tablegen_for_next_self_host()
    test_busybox_and_dropbear_scripts_honor_host_toolchain_override()
    test_dropbear_script_preseeds_wos_target_configure_probes()
    test_mlibc_script_honors_host_toolchain_override_and_job_helper()
    test_ninja_script_uses_target_header_stack()
    test_cmake_script_uses_target_header_stack()
    test_native_wos_cmake_configure_preseeds_expensive_checks()
    test_zlib_install_avoids_sysroot_usr_symlink()
    test_wos_tls_build_is_self_hostable_without_perl()
    test_wos_tls_build_preseeds_target_configure_probes()
    test_wos_curl_build_uses_native_triplet_and_real_sysroot_install()
    test_wos_curl_build_preseeds_target_configure_probes()
    test_wos_curl_download_errors_stop_before_checksum_or_extract()
    test_wos_tarball_downloads_use_shared_retry_helper()
    test_wos_python_ssl_build_handles_libressl_sigalg_gap()
    test_mlibc_scanf_float_zero_counts_as_conversion()
    test_mlibc_nameser_parser_does_not_panic_on_dns_packets()
    print("WOS toolchain bootstrap source invariants hold")
