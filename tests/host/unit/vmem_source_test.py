#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first} before {second}")


def test_nonfixed_mmap_address_selection_is_reserved_before_mapping() -> None:
    source = VMEM_CPP.read_text()
    reserve_body = function_body(source, "reserve_free_mmap_range")
    anon_body = function_body(source, "anon_allocate")
    file_body = function_body(source, "file_allocate")

    require_tokens(
        source,
        [
            "ker::mod::sys::Mutex g_mmap_reserve_lock;",
            "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        ],
        "mmap address reservation surface",
    )
    require_tokens(
        reserve_body,
        [
            "ker::mod::sys::MutexGuard guard(g_mmap_reserve_lock)",
            "uint64_t const VADDR = find_free_range(task, size, hint)",
            "add_shared_vmem_range(task, VADDR, size, 0, 0)",
            "out_vaddr = VADDR",
        ],
        "mmap reservation helper",
    )
    require_order(
        reserve_body,
        "uint64_t const VADDR = find_free_range(task, size, hint)",
        "add_shared_vmem_range(task, VADDR, size, 0, 0)",
        "non-fixed mmap must publish a reservation before mapping work starts",
    )

    for body_name, body in [("anon_allocate", anon_body), ("file_allocate", file_body)]:
        if "vaddr = find_free_range(task, size, hint)" in body:
            fail(f"{body_name} must reserve non-fixed mmap ranges instead of raw find_free_range")
        require_tokens(
            body,
            [
                "reserve_free_mmap_range(task, size, hint, vaddr)",
                "bool const HAS_ADDRESS_RESERVATION = !IS_FIXED;",
            ],
            f"{body_name} non-fixed reservation path",
        )

    require_order(
        file_body,
        "reserve_free_mmap_range(task, size, hint, vaddr)",
        "if (file_mmap_can_share(st, prot))",
        "file mmap must reserve the address before eager file mapping",
    )
    require_order(
        file_body,
        "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        "advance_shared_mmap_cursor(task, vaddr, size)",
        "file mmap reservation must be dropped only after page tables occupy the range",
    )


def main() -> None:
    test_nonfixed_mmap_address_selection_is_reserved_before_mapping()
    print("vmem mmap reservation invariants hold")


if __name__ == "__main__":
    main()
