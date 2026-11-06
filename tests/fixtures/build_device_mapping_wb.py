#!/usr/bin/env python3
"""Build the genuine C device-mapping fixture against an explicit WOS sysroot."""
import argparse
import json
from pathlib import Path
import subprocess

from build_compartment_filemap_runtime import elf_gate, pin


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--sysroot", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    sysroot = args.sysroot.resolve(strict=True)
    source = root / "tests/fixtures/device_mapping_wb.c"
    compiler = root / "toolchain/host/bin/clang"
    linker = root / "toolchain/host/bin/ld.lld"
    output = args.output.absolute()
    dependency = Path(str(output) + ".d")
    link_dependency = Path(str(output) + ".link.d")
    receipt = Path(str(output) + ".build.json")
    if not all((sysroot / name).is_dir() for name in ("include", "lib")):
        parser.error("explicit target sysroot must contain include and lib")
    for path in (output, dependency, link_dependency, receipt):
        if path.exists() or path.is_symlink():
            parser.error(f"refusing existing output: {path}")
    inputs = (Path(__file__), root / "tests/fixtures/build_compartment_filemap_runtime.py", source, compiler, linker)
    before = [pin(path) for path in inputs]
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [str(compiler), "--target=x86_64-pc-wos", f"--sysroot={sysroot}", "-std=c17", "-O2", "-g", "-fPIE", "-pie",
               "-fno-stack-protector", "-fno-stack-check", "-Wall", "-Wextra", "-Werror", "-fno-sanitize=safe-stack",
               "-isystem", str(sysroot / "include"), str(source), "-MD", "-MF", str(dependency), "-fuse-ld=lld",
               "-L" + str(sysroot / "lib"), "--unwindlib=none", "-lc", "-Wl,--dynamic-linker=/lib/ld.so",
               "-Wl,--dependency-file=" + str(link_dependency), "-o", str(output)]
    result = subprocess.run(command, cwd=root, timeout=60, check=False)
    after = [pin(path) for path in inputs]
    if before != after:
        raise RuntimeError("source/tool changed during fixture build")
    if result.returncode != 0:
        return result.returncode
    value = {"qualification": "PASS_ARTIFACT_ONLY", "command": command, "exit_code": result.returncode,
             "before": before, "after": after, "outputs": [pin(path) for path in (output, dependency, link_dependency)],
             "elf": elf_gate(output), "runtime_qualified": False}
    with receipt.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
