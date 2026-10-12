#!/usr/bin/env python3
"""Draft builder for the finite public implicit-stdio fixture; no runtime action."""
import argparse
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    source = args.source.absolute() if args.source else root / "tests/fixtures/implicit_stdio_runtime.cpp"
    output = args.output.absolute()
    if not source.is_file() or source.is_symlink():
        parser.error(f"source must be an existing regular nonsymlink file: {source}")
    if output.exists() or output.is_symlink():
        parser.error(f"output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    sysroot = root / "toolchain/sysroot"
    command = [
        str(root / "toolchain/host/bin/clang++"), "--target=x86_64-pc-wos", f"--sysroot={sysroot}",
        "-std=c++23", "-O2", "-g", "-fPIE", "-fno-stack-protector", "-fno-stack-check",
        "-Wall", "-Wextra", "-Werror", "-fno-exceptions", "-fno-rtti", "-fno-sanitize=safe-stack",
        "-isystem", str(sysroot / "include/c++/v1"), "-isystem", str(sysroot / "include"),
        "-I" + str(root / "modules/kern/src"), str(source), "-fuse-ld=lld", "-L" + str(sysroot / "lib"),
        "-nostdlib++", "--unwindlib=none", "-lc", "-o", str(output),
    ]
    return subprocess.run(command, cwd=root).returncode


if __name__ == "__main__":
    raise SystemExit(main())
