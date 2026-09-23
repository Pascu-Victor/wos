#!/usr/bin/env python3
"""Prove v4 kernel-catalog symbolization at multiple KASLR bases."""

from __future__ import annotations

import argparse
import json
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True

from generate_incident_fixtures import build_coredump, fixed_c_string, sha_build_id


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WOSDBG = REPO_ROOT / "tools" / "build" / "bin" / "wosdbg"
DEFAULT_KERNEL = REPO_ROOT / "build-ktest" / "modules" / "kern" / "wos"
IMAGE_EXACT_MAPPING = 1 << 3
IMAGE_BUILD_ID_PRESENT = 1 << 4
IMAGE_KERNEL = 1 << 5
COREDUMP_V4_HEADER_SIZE = 1872
COREDUMP_V4_SEGMENT_SIZE = 48


class TestFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TestFailure(message)


def elf_layout(image: bytes) -> tuple[int, int, int, int, int, int]:
    require(image[:4] == b"\x7fELF" and image[4] == 2 and image[5] == 1, "kernel is not little-endian ELF64")
    entry = struct.unpack_from("<Q", image, 24)[0]
    phoff = struct.unpack_from("<Q", image, 32)[0]
    phentsize, phnum = struct.unpack_from("<HH", image, 54)
    loads: list[tuple[int, int, int]] = []
    dynamic = 0
    for index in range(phnum):
        offset = phoff + index * phentsize
        require(offset + 56 <= len(image), "kernel program header table is truncated")
        p_type, flags, _, vaddr, _, _, memsz, _ = struct.unpack_from("<IIQQQQQQ", image, offset)
        if p_type == 1:
            loads.append((vaddr, vaddr + memsz, flags))
        elif p_type == 2:
            dynamic = vaddr
    require(loads, "kernel ELF has no PT_LOAD segments")
    executable = [(start, end) for start, end, flags in loads if flags & 1]
    require(executable, "kernel ELF has no executable PT_LOAD segment")
    return entry, min(start for start, _, _ in loads), max(end for _, end, _ in loads), min(x[0] for x in executable), max(x[1] for x in executable), dynamic


def relocated_kernel_dump(kernel: bytes, load_base: int) -> bytes:
    dump = bytearray(build_coredump(4, kernel))
    module_offset = COREDUMP_V4_HEADER_SIZE + COREDUMP_V4_SEGMENT_SIZE
    build_id = bytes.fromhex(sha_build_id(kernel))
    require(0 < len(build_id) <= 32, "kernel build ID is absent or too large for coredump v4")
    entry_offset, image_start, image_end, text_start, text_end, dynamic = elf_layout(kernel)
    entry = load_base + entry_offset

    # The captured first page authenticates the ET_DYN image at its runtime
    # base; the catalog supplies the exact slide and build identity.
    struct.pack_into("<Q", dump, COREDUMP_V4_HEADER_SIZE, load_base)
    module = struct.pack(
        "<7QIB3x32s256s",
        load_base,
        load_base + image_start,
        load_base + image_end,
        load_base + text_start,
        load_base + text_end,
        entry,
        load_base + dynamic,
        IMAGE_EXACT_MAPPING | IMAGE_BUILD_ID_PRESENT | IMAGE_KERNEL,
        len(build_id),
        build_id + bytes(32 - len(build_id)),
        fixed_c_string("/boot/wos", 256),
    )
    dump[module_offset : module_offset + len(module)] = module
    struct.pack_into("<Q", dump, COREDUMP_V4_HEADER_SIZE + 32, 1)
    return bytes(dump)


def invoke(binary: Path, root: Path, kernel: Path, dump: Path, runtime_entry: int) -> dict[str, Any]:
    calls = {
        "calls": [
            {
                "id": "open",
                "tool": "open_coredump",
                "arguments": {"path": str(dump), "kernelElfPath": str(kernel)},
            },
            {
                "id": "resolve",
                "tool": "resolve_address",
                "arguments": {
                    "dumpId": "$open.dumpId",
                    "address": hex(runtime_entry),
                    "moduleFilter": "kernel",
                },
            },
        ],
        "continueOnError": True,
    }
    completed = subprocess.run(
        [str(binary), "--batch", "-"],
        cwd=root,
        input=json.dumps(calls, sort_keys=True),
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    require(
        completed.returncode == 0,
        f"WOSDBG failed ({completed.returncode}): {completed.stderr.strip()} {completed.stdout[:1000]}",
    )
    try:
        output = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise TestFailure(f"WOSDBG returned invalid JSON: {completed.stdout[:200]!r}") from error
    entries = output.get("calls")
    require(isinstance(entries, list) and len(entries) == 2, f"unexpected batch result: {output}")
    results = {entry.get("id"): entry.get("result") for entry in entries}
    require(all(isinstance(result, dict) for result in results.values()), f"non-object result: {results}")
    return results


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wosdbg", type=Path, default=DEFAULT_WOSDBG)
    parser.add_argument("--kernel", type=Path, default=DEFAULT_KERNEL)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    binary = args.wosdbg.resolve()
    kernel_path = args.kernel.resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), f"WOSDBG is not executable: {binary}")
    require(kernel_path.is_file(), f"kernel ELF does not exist: {kernel_path}")
    kernel = kernel_path.read_bytes()
    entry_offset = elf_layout(kernel)[0]

    with tempfile.TemporaryDirectory(prefix="wosdbg-kernel-relocation-") as raw_root:
        root = Path(raw_root)
        (root / "wosdbg.json").write_text(
            json.dumps({"mcp": {"allowedRoots": [str(root), str(REPO_ROOT)]}}, sort_keys=True), encoding="utf-8"
        )
        for index, load_base in enumerate((0xFFFFFFFF92000000, 0xFFFFFFFFB4000000)):
            dump = root / f"kernel-slide-{index}.core"
            dump.write_bytes(relocated_kernel_dump(kernel, load_base))
            results = invoke(binary, root, kernel_path, dump, load_base + entry_offset)
            opened = results["open"]
            resolved = results["resolve"]
            require(opened.get("ok") is True, f"open failed at {load_base:#x}: {opened}")
            require(resolved.get("ok") is True, f"resolve failed at {load_base:#x}: {resolved}")
            address = resolved.get("address")
            module = resolved.get("module")
            require(isinstance(address, dict), f"missing relocated address at {load_base:#x}: {resolved}")
            # Linker boundary symbols intentionally alias the entry point. The
            # resolver may choose any equal-valued ELF symbol as its display
            # label, but DWARF must still identify the executable function.
            require(
                address.get("symbol") == "_start" or address.get("source") == "_start",
                f"wrong relocated entry function at {load_base:#x}: {resolved}",
            )
            require(isinstance(module, dict) and module.get("role") == "kernel", f"wrong module role at {load_base:#x}: {resolved}")
            require(module.get("identityStatus") == "recorded-build-id-match", f"kernel build ID was not authenticated: {resolved}")
            require(int(str(module.get("base")), 16) == load_base, f"wrong module base at {load_base:#x}: {resolved}")
            require(
                int(str(address.get("value")), 16) == load_base + entry_offset,
                f"wrong runtime symbol address at {load_base:#x}: {resolved}",
            )

    print("PASS: WOSDBG coredump kernel symbolization at two KASLR slides")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except TestFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1)
