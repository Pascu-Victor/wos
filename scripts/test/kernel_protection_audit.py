#!/usr/bin/env python3
"""Fail closed when the linked WOS kernel lacks its static protection contract."""

from __future__ import annotations

import argparse
import pathlib
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class LoadSegment:
    start: int
    end: int
    flags: str


def run_readelf(tool: str, *args: str) -> str:
    return subprocess.run([tool, *args], check=True, text=True, stdout=subprocess.PIPE).stdout


def run_tool(tool: str, *args: str) -> str:
    return subprocess.run([tool, *args], check=True, text=True, stdout=subprocess.PIPE).stdout


def load_segments(output: str) -> list[LoadSegment]:
    segments: list[LoadSegment] = []
    pattern = re.compile(
        r"^\s*LOAD\s+\S+\s+(0x[0-9a-fA-F]+)\s+\S+\s+\S+\s+(0x[0-9a-fA-F]+)\s+(.+?)\s+0x[0-9a-fA-F]+\s*$"
    )
    for line in output.splitlines():
        match = pattern.match(line)
        if match is None:
            continue
        start = int(match.group(1), 16)
        size = int(match.group(2), 16)
        segments.append(LoadSegment(start=start, end=start + size, flags="".join(match.group(3).split())))
    return segments


def symbols(output: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[0].rstrip(":").isdigit():
            try:
                result[fields[7]] = int(fields[1], 16)
            except ValueError:
                pass
    return result


def segment_for(segments: list[LoadSegment], address: int) -> LoadSegment | None:
    return next((segment for segment in segments if segment.start <= address < segment.end), None)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("elf", nargs="?", type=pathlib.Path, default=pathlib.Path("build/modules/kern/wos"))
    args = parser.parse_args()

    tool = shutil.which("llvm-readelf") or shutil.which("readelf")
    objdump = shutil.which("llvm-objdump") or shutil.which("objdump")
    if tool is None:
        raise SystemExit("kernel protection audit requires llvm-readelf or readelf")
    if objdump is None:
        raise SystemExit("kernel protection audit requires llvm-objdump or objdump")
    if not args.elf.is_file():
        raise SystemExit(f"kernel ELF does not exist: {args.elf}")

    segments = load_segments(run_readelf(tool, "-W", "-l", str(args.elf)))
    symbol_values = symbols(run_readelf(tool, "-W", "-s", str(args.elf)))
    required = (
        "__kernel_link_base",
        "__kernel_image_start",
        "__kernel_text_start",
        "__kernel_text_end",
        "__kernel_init_start",
        "__kernel_init_end",
        "__kernel_trampoline_start",
        "__kernel_trampoline_end",
        "__kernel_rodata_start",
        "__kernel_rodata_end",
        "__kernel_sanitizer_start",
        "__kernel_sanitizer_end",
        "__kernel_relocation_start",
        "__kernel_relocation_end",
        "__kernel_build_id_start",
        "__kernel_build_id_end",
        "__kernel_data_start",
        "__kernel_dynamic_start",
        "__kernel_dynamic_end",
        "__kernel_bss_start",
        "__kernel_emergency_stack_region_start",
        "__stack_chk_emergency_guard_low",
        "__stack_chk_emergency_stack",
        "__stack_chk_emergency_stack_end",
        "__stack_chk_emergency_guard_high_end",
        "__kernel_emergency_stack_region_end",
        "__kernel_bss_end",
        "__kernel_data_end",
        "__kernel_end",
    )
    missing = [name for name in required if name not in symbol_values]
    errors: list[str] = []
    if missing:
        errors.append("missing linker symbols: " + ", ".join(missing))
    if any("W" in segment.flags and "E" in segment.flags for segment in segments):
        errors.append("an ELF PT_LOAD segment is writable and executable")
    if len(segments) < 3:
        errors.append(f"expected at least three PT_LOAD segments, found {len(segments)}")

    elf_header = run_readelf(tool, "-W", "-h", str(args.elf))
    if not re.search(r"^\s*Type:\s+DYN\b", elf_header, re.MULTILINE):
        errors.append("kernel ELF is not relocatable ET_DYN")
    dynamic = run_readelf(tool, "-W", "-d", str(args.elf))
    if "(RELR)" not in dynamic or "(RELRSZ)" not in dynamic:
        errors.append("kernel dynamic table does not describe packed relative relocations")

    for name in ("__stack_chk_fail", "__stack_chk_fail_local", "wos_stack_protector_bootstrap"):
        if name not in symbol_values:
            errors.append(f"missing stack-protector symbol: {name}")

    disassembly = run_tool(objdump, "-d", str(args.elf))
    guard_loads = len(re.findall(r"%gs:(?:0x40|64)\b", disassembly))
    if guard_loads < 2:
        errors.append(f"expected compiler GS canary loads at offset 0x40, found {guard_loads}")
    if "__stack_chk_fail" not in disassembly:
        errors.append("linked code has no calls to __stack_chk_fail")

    if not missing:
        page_size = 0x1000
        image = symbol_values["__kernel_image_start"]
        link_base = symbol_values["__kernel_link_base"]
        text_start = symbol_values["__kernel_text_start"]
        text_end = symbol_values["__kernel_text_end"]
        init_start = symbol_values["__kernel_init_start"]
        init_end = symbol_values["__kernel_init_end"]
        trampoline_start = symbol_values["__kernel_trampoline_start"]
        trampoline_end = symbol_values["__kernel_trampoline_end"]
        rodata_start = symbol_values["__kernel_rodata_start"]
        rodata_end = symbol_values["__kernel_rodata_end"]
        sanitizer_start = symbol_values["__kernel_sanitizer_start"]
        sanitizer_end = symbol_values["__kernel_sanitizer_end"]
        relocation_start = symbol_values["__kernel_relocation_start"]
        relocation_end = symbol_values["__kernel_relocation_end"]
        build_id_start = symbol_values["__kernel_build_id_start"]
        build_id_end = symbol_values["__kernel_build_id_end"]
        data_start = symbol_values["__kernel_data_start"]
        dynamic_start = symbol_values["__kernel_dynamic_start"]
        dynamic_end = symbol_values["__kernel_dynamic_end"]
        bss_start = symbol_values["__kernel_bss_start"]
        emergency_region_start = symbol_values["__kernel_emergency_stack_region_start"]
        emergency_guard_low = symbol_values["__stack_chk_emergency_guard_low"]
        emergency_stack = symbol_values["__stack_chk_emergency_stack"]
        emergency_stack_end = symbol_values["__stack_chk_emergency_stack_end"]
        emergency_guard_high_end = symbol_values["__stack_chk_emergency_guard_high_end"]
        emergency_region_end = symbol_values["__kernel_emergency_stack_region_end"]
        bss_end = symbol_values["__kernel_bss_end"]
        data_end = symbol_values["__kernel_data_end"]
        kernel_end = symbol_values["__kernel_end"]
        if link_base != 0 or image != 0:
            errors.append("kernel ET_DYN link/image base is not zero")
        if not (
            image <= text_start
            <= init_start < init_end
            <= trampoline_start < trampoline_end
            <= text_end
            <= rodata_start
            <= sanitizer_start <= sanitizer_end
            <= relocation_start <= relocation_end
            <= build_id_start < build_id_end
            <= rodata_end
            <= data_start
            <= dynamic_start < dynamic_end
            <= bss_start
            <= emergency_region_start == emergency_guard_low
            < emergency_stack < emergency_stack_end < emergency_guard_high_end == emergency_region_end
            <= bss_end <= data_end
        ):
            errors.append("kernel linker ranges overlap or are not monotonically ordered")
        if not (
            emergency_stack - emergency_guard_low == page_size
            and emergency_stack_end - emergency_stack == 4 * page_size
            and emergency_guard_high_end - emergency_stack_end == page_size
        ):
            errors.append("stack-canary emergency stack does not have one-page lower/upper guards around a 16 KiB stack")
        if data_end != kernel_end:
            errors.append("__kernel_data_end and __kernel_end differ")
        if any(value % page_size for value in (image, rodata_start, data_start, data_end)):
            errors.append("kernel image/rodata/data boundaries are not page aligned")

        expected = ((text_start, "RE"), (rodata_start, "R"), (data_start, "RW"))
        for address, flags in expected:
            segment = segment_for(segments, address)
            if segment is None:
                errors.append(f"no PT_LOAD covers 0x{address:x}")
            elif segment.flags != flags:
                errors.append(f"PT_LOAD at 0x{address:x} has flags {segment.flags}, expected {flags}")

    if errors:
        for error in errors:
            print(f"kernel-protection-audit: ERROR: {error}", file=sys.stderr)
        return 1
    print(
        f"kernel-protection-audit: PASS: {args.elf} has ordered linker ranges, no writable-executable PT_LOAD, "
        f"and {guard_loads} GS canary loads"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
