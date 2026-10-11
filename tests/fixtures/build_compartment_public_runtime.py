#!/usr/bin/env python3
"""Build the real WOS compartment probe against the current normal sysroot."""
import argparse
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = args.output.absolute()
    output.parent.mkdir(parents=True, exist_ok=True)
    sysroot = root / 'toolchain/sysroot'
    command = [
        str(root / 'toolchain/host/bin/clang++'), '--target=x86_64-pc-wos',
        f'--sysroot={sysroot}', '-std=c++23', '-O2', '-g', '-fPIE',
        '-fno-stack-protector', '-fno-stack-check', '-Wall', '-Wextra', '-Werror',
        '-fno-exceptions', '-fno-rtti', '-fno-sanitize=safe-stack',
        '-isystem', str(sysroot / 'include/c++/v1'),
        '-isystem', str(sysroot / 'include'), '-I' + str(root / 'modules/kern/src'),
        str(root / 'tests/fixtures/compartment_public_runtime.cpp'), '-fuse-ld=lld',
        '-L' + str(sysroot / 'lib'), '-L' + str(root / 'build/modules/journal'),
        '-nostdlib++', '--unwindlib=none', '-ljournal', '-lc', '-o', str(output),
    ]
    return subprocess.run(command, cwd=root).returncode


if __name__ == '__main__':
    raise SystemExit(main())
