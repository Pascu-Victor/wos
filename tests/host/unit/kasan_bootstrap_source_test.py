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
    cmake = KERNEL_CMAKE.read_text()
    options = cmake_list_body(cmake, "KASAN_EXCLUDED_COMPILE_OPTIONS")
    require_tokens(
        options,
        [
            "-fno-sanitize=all",
            "-fno-sanitize-coverage=trace-pc",
        ],
        "early KASAN/UBSan/KCOV exclusion options",
    )

    body = cmake_list_body(cmake, "KASAN_EXCLUDED_SRCS")
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
            "allocator-driven shadow access",
            "shadow-fault handler",
        ],
        "KASAN AP-bootstrap exclusion comment",
    )


def test_selftest_ap_entry_still_runs_before_idt() -> None:
    body = function_body(SMT_CPP.read_text(), "selftest_secondary_cpu_park")
    before_idt = body.split("desc::idt::idt_init();", 1)[0]
    if "new cpu::PerCpu" in before_idt:
        fail("selftest AP park entry must not allocate PerCpu storage before IDT")
    if "new (std::nothrow) cpu::PerCpu" in before_idt:
        fail("selftest AP park entry must not allocate nothrow PerCpu storage before IDT")
    if "memset" in before_idt:
        fail("selftest AP park entry must not call libc++ memset wrappers before IDT")
    require_order(
        body,
        [
            "mm::virt::switch_to_kernel_pagemap();",
            "cpu::enable_fsgsbase();",
            "auto* per_cpu_data = selftest_ap_per_cpu(cpu_no);",
            "zero_per_cpu(*per_cpu_data);",
            "cpu::wrgsbase(PER_CPU_ADDR);",
            "desc::gdt::init_descriptors",
            "desc::idt::idt_init();",
            "selftest_parked_cpu_mask.fetch_or",
        ],
        "selftest AP park entry must finish pre-IDT setup before it can fault safely",
    )


def test_selftest_ap_context_is_prepared_before_goto_address() -> None:
    smt = SMT_CPP.read_text()
    zero_helper = function_body(smt, "zero_per_cpu")
    if "memset" in zero_helper:
        fail("PerCpu zeroing helper must not call libc++ memset wrappers")
    require_tokens(
        zero_helper,
        [
            "per_cpu_data.syscall_stack = 0;",
            "per_cpu_data.user_rsp = 0;",
            "per_cpu_data.cpu_id = 0;",
            "per_cpu_data.saved_ds = 0;",
            "per_cpu_data.saved_es = 0;",
            "per_cpu_data.syscall_ret_rip = 0;",
            "per_cpu_data.syscall_ret_flags = 0;",
            "per_cpu_data.syscall_entry_tmp = 0;",
        ],
        "PerCpu explicit zeroing helper",
    )

    helper = function_body(smt, "prepare_selftest_ap_context")
    require_order(
        helper,
        [
            "auto* per_cpu_data = new (std::nothrow) cpu::PerCpu();",
            "zero_per_cpu(*per_cpu_data);",
            "kernel_per_cpu_ptrs[cpu_no] = per_cpu_data;",
        ],
        "BSP selftest AP context preparation",
    )

    body = function_body(smt, "park_secondary_cpus_for_selftest")
    require_order(
        body,
        [
            "prepare_selftest_ap_context(i);",
            "expected_mask |= 1ULL << i;",
            "__atomic_store_n(&response->cpus[i]->goto_address",
        ],
        "selftest AP context must be prepared before AP release",
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
    test_selftest_ap_context_is_prepared_before_goto_address()
    test_kasan_shadow_fault_primitives_remain_excluded()
    print("KASAN bootstrap source invariants hold")


if __name__ == "__main__":
    main()
