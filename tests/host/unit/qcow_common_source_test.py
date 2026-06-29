#!/usr/bin/env python3

import subprocess
import tempfile
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
QCOW_COMMON = ROOT / "scripts" / "build" / "qcow_common.sh"


def fail(message: str) -> None:
    raise AssertionError(message)


def run_shell(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", script],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def fake_qemu_img(fake_bin: Path, body: str) -> None:
    path = fake_bin / "qemu-img"
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(0o755)


def test_wos_qcow_run_preserves_failed_command_status() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-qcow-test.") as temp:
        temp_path = Path(temp)
        fake_bin = temp_path / "bin"
        fake_bin.mkdir()
        fake_qemu_img(fake_bin, "exit 0\n")
        disk = temp_path / "disk.qcow2"
        disk.touch()

        shell = textwrap.dedent(
            f"""
            set +e
            export PATH={fake_bin}:$PATH
            export TMPDIR={temp_path}
            source {QCOW_COMMON}
            wos_qcow_run "simulated qcow operation" {disk} sh -c 'printf "hard fail\\n"; exit 37'
            status=$?
            if [ "$status" -ne 37 ]; then
                echo "expected status 37, got $status"
                exit 1
            fi
            """
        )
        result = run_shell(shell)
        if result.returncode != 0:
            fail(result.stdout + result.stderr)


def test_wos_qcow_run_rejects_logged_libguestfs_errors() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-qcow-test.") as temp:
        temp_path = Path(temp)
        fake_bin = temp_path / "bin"
        fake_bin.mkdir()
        fake_qemu_img(fake_bin, "exit 0\n")
        disk = temp_path / "disk.qcow2"
        disk.touch()

        shell = textwrap.dedent(
            f"""
            set +e
            export PATH={fake_bin}:$PATH
            export TMPDIR={temp_path}
            source {QCOW_COMMON}
            wos_qcow_run "simulated qcow operation" {disk} sh -c 'printf "*stdin*:1: libguestfs: error: simulated\\n"'
            status=$?
            if [ "$status" -ne 1 ]; then
                echo "expected status 1, got $status"
                exit 1
            fi
            """
        )
        result = run_shell(shell)
        if result.returncode != 0:
            fail(result.stdout + result.stderr)


def test_libguestfs_prepare_exports_tmpdir_for_supermin() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-qcow-test.") as temp:
        temp_path = Path(temp)
        libguestfs_root = temp_path / "libguestfs"
        shell = textwrap.dedent(
            f"""
            set -e
            unset TMPDIR
            unset LIBGUESTFS_TMPDIR
            unset LIBGUESTFS_CACHEDIR
            export WOS_LIBGUESTFS_ROOT={libguestfs_root}
            source {QCOW_COMMON}
            wos_qcow_prepare_libguestfs_env
            case "$TMPDIR" in
                "$WOS_LIBGUESTFS_ROOT"/wos-libguestfs-tmp.*) ;;
                *) echo "unexpected TMPDIR=$TMPDIR"; exit 1 ;;
            esac
            if [ "$TMPDIR" != "$LIBGUESTFS_TMPDIR" ]; then
                echo "TMPDIR must match LIBGUESTFS_TMPDIR"
                exit 1
            fi
            case "$LIBGUESTFS_CACHEDIR" in
                "$WOS_LIBGUESTFS_ROOT"/wos-libguestfs-cache.*) ;;
                *) echo "unexpected LIBGUESTFS_CACHEDIR=$LIBGUESTFS_CACHEDIR"; exit 1 ;;
            esac
            wos_qcow_cleanup_libguestfs_env
            if [ -n "${{TMPDIR:-}}" ]; then
                echo "cleanup did not restore unset TMPDIR"
                exit 1
            fi
            """
        )
        result = run_shell(shell)
        if result.returncode != 0:
            fail(result.stdout + result.stderr)


def test_libguestfs_prepare_uses_writable_runtime_dir() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-qcow-test.") as temp:
        temp_path = Path(temp)
        libguestfs_root = temp_path / "libguestfs"
        read_only_runtime = temp_path / "readonly-runtime"
        read_only_runtime.mkdir()
        read_only_runtime.chmod(0o500)
        shell = textwrap.dedent(
            f"""
            set -e
            unset TMPDIR
            unset LIBGUESTFS_TMPDIR
            unset LIBGUESTFS_CACHEDIR
            export XDG_RUNTIME_DIR={read_only_runtime}
            export WOS_LIBGUESTFS_ROOT={libguestfs_root}
            source {QCOW_COMMON}
            wos_qcow_prepare_libguestfs_env
            case "$XDG_RUNTIME_DIR" in
                "$WOS_LIBGUESTFS_ROOT"/wos-libguestfs-runtime.*) ;;
                *) echo "unexpected XDG_RUNTIME_DIR=$XDG_RUNTIME_DIR"; exit 1 ;;
            esac
            if [ ! -w "$XDG_RUNTIME_DIR" ]; then
                echo "replacement XDG_RUNTIME_DIR is not writable"
                exit 1
            fi
            wos_qcow_cleanup_libguestfs_env
            if [ "$XDG_RUNTIME_DIR" != "{read_only_runtime}" ]; then
                echo "cleanup did not restore XDG_RUNTIME_DIR"
                exit 1
            fi
            """
        )
        result = run_shell(shell)
        read_only_runtime.chmod(0o700)
        if result.returncode != 0:
            fail(result.stdout + result.stderr)
