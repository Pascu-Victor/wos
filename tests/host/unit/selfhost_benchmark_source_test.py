#!/usr/bin/env python3

import json
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SELFHOST_RUNNER = ROOT / "scripts" / "bench" / "run_wos_selfhost_build.sh"
SELFHOST_COMPARE = ROOT / "scripts" / "bench" / "compare_wos_selfhost_reports.py"
ROOTFS_ALIASES = ROOT / "configs" / "rootfs" / "aliases.tsv"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def test_selfhost_runner_covers_acceptance_flow() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"',
            'DEFAULT_LINUX_WORKDIR="/tmp/wos-selfhost-bench"',
            'DEFAULT_WOS_WORKDIR="/root/wos-selfhost-bench"',
            "wos-local",
            "Direct GitHub cloning is the default",
            "benchmark validates source availability and buildability, not Git history",
            "bin/wos-cluster --launch --no-setup",
            "bin/wos-ktest --no-setup",
            "clone_cmd=(git clone)",
            "run_timed_event \"clone\" \"wos_repo\"",
            'local submodule_list="$workdir/submodule-paths.tsv"',
            "git -C \"$checkout\" config --file .gitmodules --get-regexp '^submodule\\..*\\.path$' > \"$submodule_list\"",
            "local submodule_cmd=(git -C \"$checkout\" submodule update --init --recursive)",
            "submodule_cmd+=(--depth 1)",
            "submodule_cmd+=(--jobs \"$jobs\")",
            "run_timed_event \"clone_submodule\" \"$path\"",
            "--full-history",
            "full_history=0",
            "./tools/bootstrap.sh",
            "cmake -GNinja",
            "-DWOS_BUILD_WOSDBG=OFF",
            "-DWOS_BUILD_CMAKE_FOR_HOST=OFF",
            "cmake --build \"$checkout/$build_dir\" --target \"$target\"",
            'DEFAULT_TARGET="wos_full"',
            'mode="${WOS_SELFHOST_MODE:?}"',
            'workdir="$DEFAULT_WOS_WORKDIR"',
            'workdir="$DEFAULT_LINUX_WORKDIR"',
            'WOS_SELFHOST_MODE="linux"',
            'WOS_SELFHOST_MODE="wos"',
            "WOS_SELFHOST_MODE=$(shell_quote wos)",
            'WOS_SELFHOST_FULL_HISTORY="$full_history"',
            "selfhost-report.tsv",
            'printf \'%s\\t%s\\n\' "total" "$total_elapsed" >> "$report"',
        ],
        "WOS self-host benchmark acceptance flow",
    )


def test_selfhost_runner_verifies_toolchain_kernel_and_utilities() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'require_file "toolchain/sysroot/bin/clang"',
            'require_file "toolchain/sysroot/bin/ld.lld"',
            'require_file "toolchain/sysroot/bin/lld"',
            'require_file "toolchain/sysroot/bin/llvm-ar"',
            'require_file "toolchain/sysroot/bin/llvm-ranlib"',
            'require_file "toolchain/sysroot/bin/llvm-nm"',
            'require_file "toolchain/sysroot/bin/llvm-objcopy"',
            'require_file "toolchain/sysroot/bin/llvm-strip"',
            'require_file "toolchain/sysroot/bin/llvm-readelf"',
            'require_file "toolchain/sysroot/bin/llvm-objdump"',
            'require_file "toolchain/sysroot/bin/llvm-tblgen"',
            'require_file "toolchain/sysroot/bin/clang-tblgen"',
            'require_file "toolchain/sysroot/bin/x86_64-pc-wos.cfg"',
            'require_file "toolchain/sysroot/lib/clang"',
            'require_file "toolchain/sysroot/bin/ninja"',
            'require_file "toolchain/sysroot/bin/cmake"',
            'require_file "toolchain/sysroot/bin/ctest"',
            'require_file "toolchain/sysroot/bin/cpack"',
            'require_file "toolchain/sysroot/bin/make"',
            'require_file "toolchain/sysroot/bin/git"',
            'require_file "toolchain/sysroot/bin/git-shell"',
            'require_file "toolchain/sysroot/bin/scalar"',
            'require_file "toolchain/sysroot/libexec/git-core/git-remote-https"',
            'require_file "toolchain/sysroot/bin/curl"',
            'require_file "toolchain/sysroot/etc/ssl/certs/ca-certificates.crt"',
            'require_file "toolchain/sysroot/bin/bash"',
            'require_file "toolchain/sysroot/bin/dropbearmulti"',
            'require_file "toolchain/busybox-install/bin/busybox"',
            'require_file "toolchain/sysroot/bin/meson"',
            'require_file "toolchain/sysroot/bin/ndisasm"',
            'require_any "python" "toolchain/sysroot/bin/python" "toolchain/sysroot/bin/python*"',
            'require_any "python3" "toolchain/sysroot/bin/python3" "toolchain/sysroot/bin/python3.*"',
            'require_file "$build_dir/modules/kern/wos"',
            'require_file "$build_dir/modules/init/init"',
            'require_file "$build_dir/modules/testprog/testprog"',
            'require_file "$build_dir/modules/testd/testd"',
            'require_file "$build_dir/modules/netd/netd"',
            'require_file "$build_dir/modules/httpd/httpd"',
            'require_file "$build_dir/modules/debugserver/debugserver"',
            'require_file "$build_dir/modules/perf/perf"',
            'require_file "$build_dir/modules/top/top"',
            'require_file "$build_dir/modules/memacc/memacc"',
            'require_file "$build_dir/modules/journal/journal"',
            'require_file "$build_dir/modules/journal/libjournal.so"',
            'require_file "$build_dir/modules/wkictl/wkictl"',
            'require_file "$build_dir/modules/powerctl/powerctl"',
            'require_file "$build_dir/modules/renderbench/renderbench"',
            'require_file "$build_dir/modules/strace/strace"',
            'require_file "$build_dir/modules/sftpserver/sftp-server"',
            'require_file "disk.qcow2"',
            'require_file "mountfs.qcow2"',
        ],
        "WOS self-host benchmark artifact coverage",
    )


def test_selfhost_runner_preflights_wos_only_self_host_tools() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "require_wos_selfhost_tools()",
            "sh env make tar sed grep mktemp sha256sum xz yes \\",
            "ld.lld lld llvm-ar llvm-ranlib llvm-nm llvm-objcopy llvm-strip \\",
            "llvm-readelf llvm-objdump llvm-tblgen clang-tblgen",
            'case "$mode" in',
            "wos)",
            "require_wos_selfhost_tools",
            "linux)",
            "unknown self-host mode",
        ],
        "WOS self-host benchmark WOS-mode tool preflight",
    )


def test_selfhost_runner_scrubs_inherited_toolchain_environment() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            'clean_path="${WOS_SELFHOST_CLEAN_PATH:-}"',
            "sanitize_selfhost_environment()",
            "unset CC CXX CPP LD AR AS NM OBJCOPY OBJDUMP RANLIB READELF STRIP",
            "unset CPPFLAGS CFLAGS CXXFLAGS LDFLAGS",
            "unset CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH LIBRARY_PATH LD_LIBRARY_PATH",
            "unset PKG_CONFIG PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR",
            "unset CMAKE_PREFIX_PATH CMAKE_TOOLCHAIN_FILE CMAKE_GENERATOR CMAKE_BUILD_TYPE",
            "unset WOS_WORKSPACE_ROOT",
            "unset WOS_SYSROOT WOS_TARGET_ARCH WOS_TOOLCHAIN_MODE WOS_TOOLCHAIN_ROOT",
            'export PATH="$clean_path"',
            'WOS_SELFHOST_CLEAN_PATH="${WOS_ORIGINAL_PATH:-$PATH}"',
            'WOS_SELFHOST_CLEAN_PATH="/usr/bin:/bin:/usr/sbin:/sbin"',
            "WOS_SELFHOST_CLEAN_PATH=/usr/bin:/bin:/usr/sbin:/sbin",
        ],
        "WOS self-host benchmark environment sanitizer",
    )


def test_rootfs_stages_self_hosting_tablegen_tools() -> None:
    aliases = ROOTFS_ALIASES.read_text()
    require_tokens(
        aliases,
        [
            "copy\ttoolchain/sysroot/bin/llvm-tblgen\t/usr/bin/llvm-tblgen",
            "copy\ttoolchain/sysroot/bin/clang-tblgen\t/usr/bin/clang-tblgen",
        ],
        "WOS rootfs self-hosting tablegen staging",
    )


def test_rootfs_stages_git_helpers_at_configured_runtime_paths() -> None:
    aliases = ROOTFS_ALIASES.read_text()
    require_tokens(
        aliases,
        [
            "copy\ttoolchain/sysroot/libexec/git-core\t/libexec/git-core",
            "copy\ttoolchain/sysroot/share/git-core\t/share/git-core",
            "copy\ttoolchain/sysroot/libexec/git-core\t/usr/libexec/git-core",
            "copy\ttoolchain/sysroot/share/git-core\t/usr/share/git-core",
        ],
        "WOS rootfs Git runtime helper staging",
    )


def test_selfhost_runner_mirror_is_optional_not_default() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "--mirror-file PATH",
            "--mirror-http-prefix U",
            "Rewrite https://github.com/",
            'mirror_file=""',
            'mirror_http_prefix=""',
            'git config --global "url.file://$mirror_file/".insteadOf https://github.com/',
            'git config --global "url.$mirror_http_prefix".insteadOf https://github.com/',
        ],
        "WOS self-host benchmark mirror controls",
    )


def test_selfhost_runner_records_detailed_historical_timings() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "--history-file PATH",
            "selfhost-detail.tsv",
            'history_file="${WOS_SELFHOST_HISTORY_FILE:-}"',
            'history_file="${workdir%/}-history.tsv"',
            "write_timing_header()",
            "record_timing()",
            "run_timed_event()",
            '"run_id" "timestamp_utc" "mode" "phase" "label" "elapsed_ms"',
            '"status" "repo" "commit" "target" "jobs" "full_history"',
            'record_timing "metadata" "checkout_commit" "0" "ok"',
            'run_timed_event "clone" "submodule_init"',
            'run_timed_event "clone" "submodule_status"',
            'record_timing "step" "$name" "$elapsed" "ok"',
            'record_timing "step" "$name" "$elapsed" "fail:$status"',
            'record_timing "step" "total" "$total_elapsed" "ok"',
            'WOS_SELFHOST_HISTORY_FILE="$history_file"',
            "WOS_SELFHOST_HISTORY_FILE=$(shell_quote \"$history_file\")",
        ],
        "WOS self-host benchmark detailed historical timing records",
    )


def test_selfhost_runner_keeps_remote_and_scratch_handling_guarded() -> None:
    source = SELFHOST_RUNNER.read_text()
    require_tokens(
        source,
        [
            "shell_quote()",
            'remote_env="env"',
            'remote_command="payload=\\${TMPDIR:-/tmp}/wos-selfhost-build.\\$\\$.sh"',
            'remote_command+="; cat > \\"\\$payload\\" || exit \\$?"',
            'remote_command+="; $remote_env bash \\"\\$payload\\""',
            'remote_command+="; rm -f \\"\\$payload\\""',
            'selfhost_payload | "$WOS_SSH" "$host" "$remote_command"',
            "/tmp|/tmp/|/var/tmp|/var/tmp/)",
            "/root/wos-selfhost-*|/home/*/wos-selfhost-*)",
            "refusing to replace temporary directory root",
            '[[ "$jobs" =~ ^[1-9][0-9]*$ ]]',
            'reject_whitespace "--history-file" "$history_file"',
        ],
        "WOS self-host benchmark guarded remote and scratch handling",
    )

    skip_block_start = source.find('if [ "$skip_bootstrap" = "1" ]; then')
    skip_block_end = source.find("fi", skip_block_start)
    if skip_block_start < 0 or skip_block_end < 0:
        fail("bootstrap skip branch is missing")
    skip_block = source[skip_block_start:skip_block_end]
    if ">> \"$report\"" in skip_block:
        fail("bootstrap skip branch must leave timing output to time_step")
    if "< <(" in source:
        fail("WOS self-host benchmark must avoid Bash process substitution for WOS shell compatibility")


def test_selfhost_report_comparator_checks_clone_build_and_total() -> None:
    source = SELFHOST_COMPARE.read_text()
    require_tokens(
        source,
        [
            'DEFAULT_STEPS = ("clone_sources", "build_wos", "total")',
            "--max-wos-ratio",
            "ratio = wos_ms / linux_ms",
            "--json-output",
            "return 0 if result[\"pass\"] else 1",
        ],
        "WOS self-host report comparator",
    )

    with tempfile.TemporaryDirectory(prefix="wos-selfhost-compare-") as tmp:
        tmp_path = Path(tmp)
        wos_report = tmp_path / "wos.tsv"
        linux_report = tmp_path / "linux.tsv"
        output_json = tmp_path / "comparison.json"
        wos_report.write_text(
            "clone_sources\t1100\nbootstrap_toolchain\t2000\nconfigure_wos\t300\nbuild_wos\t12000\ntotal\t15400\n",
            encoding="ascii",
        )
        linux_report.write_text(
            "clone_sources\t1000\nbootstrap_toolchain\t1900\nconfigure_wos\t280\nbuild_wos\t10000\ntotal\t13180\n",
            encoding="ascii",
        )
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos",
                str(wos_report),
                "--linux",
                str(linux_report),
                "--max-wos-ratio",
                "1.25",
                "--json-output",
                str(output_json),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            fail(f"self-host comparator rejected acceptable reports: {result.stderr or result.stdout}")
        data = json.loads(output_json.read_text(encoding="ascii"))
        if not data["pass"]:
            fail(f"self-host comparator JSON did not record pass: {data!r}")

        wos_report.write_text("clone_sources\t1400\nbuild_wos\t13000\ntotal\t14400\n", encoding="ascii")
        linux_report.write_text("clone_sources\t1000\nbuild_wos\t10000\ntotal\t11000\n", encoding="ascii")
        result = subprocess.run(
            [
                str(SELFHOST_COMPARE),
                "--wos",
                str(wos_report),
                "--linux",
                str(linux_report),
                "--max-wos-ratio",
                "1.25",
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            fail("self-host comparator accepted WOS reports beyond the ratio threshold")


if __name__ == "__main__":
    test_selfhost_runner_covers_acceptance_flow()
    test_selfhost_runner_verifies_toolchain_kernel_and_utilities()
    test_selfhost_runner_preflights_wos_only_self_host_tools()
    test_selfhost_runner_scrubs_inherited_toolchain_environment()
    test_rootfs_stages_self_hosting_tablegen_tools()
    test_rootfs_stages_git_helpers_at_configured_runtime_paths()
    test_selfhost_runner_mirror_is_optional_not_default()
    test_selfhost_runner_records_detailed_historical_timings()
    test_selfhost_runner_keeps_remote_and_scratch_handling_guarded()
    test_selfhost_report_comparator_checks_clone_build_and_total()
    print("WOS self-host benchmark source invariants hold")
