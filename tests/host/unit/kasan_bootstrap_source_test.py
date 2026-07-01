#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL_CMAKE = ROOT / "modules" / "kern" / "CMakeLists.txt"
SMT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "smt" / "smt.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def find_matching_brace(source: str, brace: int) -> int:
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail("unterminated braced block")


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|bool|void|uint64_t|void\*)\s+(?:[A-Za-z0-9_:]+::)?{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")
    end = find_matching_brace(source, match.end() - 1)
    return source[match.end() : end]


def cmake_list_body(source: str, name: str) -> str:
    start = source.find(f"set({name}")
    if start < 0:
        fail(f"missing CMake list {name}")
    end = source.find("\n    )", start)
    if end < 0:
        fail(f"unterminated CMake list {name}")
    return source[start:end]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_selftest_ap_bootstrap_is_excluded_from_kasan() -> None:
    body = cmake_list_body(KERNEL_CMAKE.read_text(), "KASAN_EXCLUDED_SRCS")
    entry = "src/platform/smt/smt.cpp"
    index = body.find(entry)
    if index < 0:
        fail("selftest AP bootstrap file is not excluded from KASAN instrumentation")

    nearby_comment = body[max(0, index - 280) : index]
    require_tokens(
        nearby_comment,
        [
            "Secondary CPU bootstrap",
            "IDT",
            "shadow-fault handler",
        ],
        "KASAN AP-bootstrap exclusion comment",
    )


def test_selftest_ap_entry_still_runs_before_idt() -> None:
    body = function_body(SMT_CPP.read_text(), "selftest_secondary_cpu_park")
    require_order(
        body,
        [
            "mm::virt::switch_to_kernel_pagemap();",
            "cpu::enable_fsgsbase();",
            "auto* per_cpu_data = new cpu::PerCpu();",
            "cpu::wrgsbase(PER_CPU_ADDR);",
            "desc::gdt::init_descriptors",
            "desc::idt::idt_init();",
            "selftest_parked_cpu_mask.fetch_or",
        ],
        "selftest AP park entry must finish pre-IDT setup before it can fault safely",
    )


def test_kasan_shadow_fault_primitives_remain_excluded() -> None:
    body = cmake_list_body(KERNEL_CMAKE.read_text(), "KASAN_EXCLUDED_SRCS")
    require_tokens(
        body,
        [
            "src/platform/mm/virt.opt.cpp",
            "src/platform/interrupt/gates.cpp",
            "src/platform/interrupt/idt.cpp",
            "src/sanitizer/kasan.cpp",
            "src/platform/asm/cpu.cpp",
        ],
        "KASAN shadow fault and exception primitives",
    )


def main() -> None:
    test_selftest_ap_bootstrap_is_excluded_from_kasan()
    test_selftest_ap_entry_still_runs_before_idt()
    test_kasan_shadow_fault_primitives_remain_excluded()
    print("KASAN bootstrap source invariants hold")


if __name__ == "__main__":
    main()
