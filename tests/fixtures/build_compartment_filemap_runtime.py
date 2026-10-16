#!/usr/bin/env python3
"""Build the finite WOS File-map characterization with explicit fresh outputs."""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess


def pin(path):
    path = path.absolute()
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"not a regular input: {path}")
    data = resolved.read_bytes()
    return {"path": str(path), "resolved": str(resolved), "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "symlink": path.is_symlink()}


def elf_gate(path):
    data = path.read_bytes()
    if len(data) < 64 or data[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("expected an ELF64 little-endian artifact")
    fields = struct.unpack_from("<HHIQQQIHHHHHH", data, 16)
    kind, machine, version, entry, phoff = fields[:5]
    if (kind, machine, version) != (3, 62, 1) or fields[8] != 56 or not 1 <= fields[9] <= 32:
        raise ValueError("expected x86_64 PIE with bounded ELF64 program headers")
    count = fields[9]
    if phoff > len(data) or count * 56 > len(data) - phoff:
        raise ValueError("program headers exceed the file")
    segments = [struct.unpack_from("<IIQQQQQQ", data, phoff + i * 56) for i in range(count)]
    for segment in segments:
        if segment[2] > len(data) or segment[5] > len(data) - segment[2] or segment[5] > segment[6]:
            raise ValueError("segment has invalid bounds")
        if segment[0] == 1 and segment[1] & 3 == 3:
            raise ValueError("writable executable LOAD segment")
        if segment[0] == 0x6474E551 and segment[1] & 1:
            raise ValueError("executable stack")
    if len([segment for segment in segments if segment[0] == 0x6474E551]) != 1:
        raise ValueError("expected one explicit non-executable stack segment")
    interpreters = [segment for segment in segments if segment[0] == 3]
    if len(interpreters) != 1:
        raise ValueError("expected exactly one PT_INTERP")
    interp = interpreters[0]
    if data[interp[2]:interp[2] + interp[5]] != b"/lib/ld.so\0":
        raise ValueError("unexpected interpreter")
    if not any(s[0] == 1 and s[1] & 1 and s[3] <= entry < s[3] + s[5] for s in segments):
        raise ValueError("entry is not within file-backed executable bytes")
    dynamic = [segment for segment in segments if segment[0] == 2]
    if len(dynamic) != 1 or dynamic[0][5] % 16:
        raise ValueError("invalid dynamic segment")
    tags = []
    for offset in range(dynamic[0][2], dynamic[0][2] + dynamic[0][5], 16):
        tag, value = struct.unpack_from("<QQ", data, offset)
        if tag == 0:
            break
        tags.append((tag, value))
    else:
        raise ValueError("unterminated dynamic segment")
    strings = [value for tag, value in tags if tag == 5]
    sizes = [value for tag, value in tags if tag == 10]
    if len(strings) != 1 or len(sizes) != 1 or sizes[0] == 0:
        raise ValueError("missing bounded dynamic string table")
    mapped = [s for s in segments if s[0] == 1 and s[3] <= strings[0] and strings[0] - s[3] <= s[5]
              and sizes[0] <= s[5] - (strings[0] - s[3])]
    if len(mapped) != 1:
        raise ValueError("dynamic string table is not file-backed unambiguously")
    string_offset = mapped[0][2] + strings[0] - mapped[0][3]
    table = data[string_offset:string_offset + sizes[0]]
    needed = []
    for tag, value in tags:
        if tag != 1:
            continue
        if value >= len(table) or table.find(b"\0", value) < 0:
            raise ValueError("unbounded NEEDED name")
        needed.append(table[value:table.index(0, value)].decode("ascii"))
    if needed != ["libc.so"]:
        raise ValueError(f"unexpected dependency closure: {needed}")
    return {"type": "ET_DYN", "machine": machine, "entry": entry, "program_headers": count,
            "interpreter": "/lib/ld.so", "needed": needed, "bytes": len(data)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--sysroot", type=Path, help="explicit target sysroot for isolated diagnostic fixtures")
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source = args.source.absolute()
    if source.is_symlink() or not source.is_file() or not (root / "AGENTS.md").is_file():
        parser.error("expected a regular explicit source and the WOS root")
    output = args.output.absolute()
    compiler = root / "toolchain/host/bin/clang++"
    linker = root / "toolchain/host/bin/ld.lld"
    sysroot = args.sysroot.resolve(strict=True) if args.sysroot is not None else root / "toolchain/sysroot"
    if args.sysroot is not None and not all((sysroot / name).is_dir() for name in ("include", "lib")):
        parser.error("explicit target sysroot must contain include and lib directories")
    dependencies = Path(str(output) + ".d")
    link_dependencies = Path(str(output) + ".link.d")
    receipt = Path(str(output) + ".build.json")
    for path in (output, dependencies, link_dependencies, receipt):
        if path.exists() or path.is_symlink():
            parser.error(f"refusing an existing output: {path}")
    before = [pin(path) for path in (Path(__file__), source, compiler, linker)]
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(compiler), "--target=x86_64-pc-wos", f"--sysroot={sysroot}",
        "-std=c++23", "-O2", "-g", "-fPIE", "-pie", "-fno-stack-protector", "-fno-stack-check",
        "-Wall", "-Wextra", "-Werror", "-fno-exceptions", "-fno-rtti", "-fno-sanitize=safe-stack",
        "-isystem", str(sysroot / "include/c++/v1"), "-isystem", str(sysroot / "include"),
        "-I" + str(root / "modules/kern/src"), "-MD", "-MF", str(dependencies), str(source),
        "-fuse-ld=lld", "-L" + str(sysroot / "lib"), "-nostdlib++", "--unwindlib=none", "-lc",
        "-Wl,--dynamic-linker=/lib/ld.so", "-Wl,--dependency-file=" + str(link_dependencies), "-o", str(output),
    ]
    try:
        result = subprocess.run(command, cwd=root, timeout=30, check=False)
    except subprocess.TimeoutExpired:
        return 124
    after = [pin(path) for path in (Path(__file__), source, compiler, linker)]
    if before != after:
        raise ValueError("source or tool identity changed during compilation")
    if result.returncode != 0:
        return result.returncode
    for path in (output, dependencies, link_dependencies):
        if path.is_symlink() or not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"missing regular compiler output: {path}")
    value = {"qualification": "PASS_ARTIFACT_ONLY", "errors": [], "command": command,
             "exit_code": result.returncode, "before": before, "after": after,
             "outputs": [pin(path) for path in (output, dependencies, link_dependencies)],
             "elf": elf_gate(output), "runtime_qualified": False,
             "complete_header_dependency_freshness_qualified": False}
    with receipt.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
