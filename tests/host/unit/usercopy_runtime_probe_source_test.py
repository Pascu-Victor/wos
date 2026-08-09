#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROBE = ROOT / "modules" / "testprog" / "src" / "usercopy_negative.cpp"
MAIN = ROOT / "modules" / "testprog" / "src" / "main.cpp"
TESTPROG_CMAKE = ROOT / "modules" / "testprog" / "CMakeLists.txt"
MANIFEST = ROOT / "docs" / "syscall_usercopy_manifest.md"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {missing}")


def test_probe_is_opt_in_and_built() -> None:
    require_tokens(
        MAIN.read_text(),
        [
            '#include "usercopy_negative.hpp"',
            'std::strcmp(command, "usercopy-negative") == 0',
            "return run_usercopy_negative();",
        ],
        "testprog usercopy-negative command",
    )
    if "src/usercopy_negative.cpp" not in TESTPROG_CMAKE.read_text():
        fail("testprog does not build the usercopy-negative runtime probe")


def test_probe_covers_pointer_families_and_address_classes() -> None:
    source = PROBE.read_text()
    require_tokens(
        source,
        [
            "callnums::sys_log",
            "callnums::futex",
            "callnums::threading",
            "callnums::process",
            "ptrace::request::GET_REMOTE_INFO",
            "callnums::time",
            "callnums::vfs",
            "callnums::net",
            "callnums::vmem",
            "callnums::shm",
            'InvalidAddress{.name = "null"',
            'InvalidAddress{.name = "user-limit/noncanonical"',
            'InvalidAddress{.name = "kernel-range"',
            'InvalidAddress{.name = "wrapped-range"',
            '"partially-mapped record"',
            '"partially-mapped output"',
            '"read-only output"',
        ],
        "runtime syscall-family/address matrix",
    )

    partial_start = source.find("void probe_partially_mapped_records")
    partial_end = source.find("void probe_readonly_outputs", partial_start)
    if partial_start < 0 or partial_end < 0:
        fail("missing partially-mapped runtime probe")
    partial = source[partial_start:partial_end]
    require_tokens(
        partial,
        [
            "procmgmt_ops::UNAME",
            "std::memset(partial, 'x', MAPPED_PREFIX);",
            '"VFS", "partially-mapped C string"',
            '"VMEM", "partially-mapped C string"',
        ],
        "partially-mapped probe must cross the bytes each syscall actually accesses",
    )
    if "procmgmt_ops::GETHOSTNAME" in partial:
        fail("partially-mapped output probe must not rely on untouched gethostname capacity")

    first_reset = partial.find("std::memset(partial, 'x', MAPPED_PREFIX);", partial.find('"TIME", "partially-mapped output"'))
    vfs_probe = partial.find('"VFS", "partially-mapped C string"')
    second_reset = partial.find("std::memset(partial, 'x', MAPPED_PREFIX);", first_reset + 1)
    vmem_probe = partial.find('"VMEM", "partially-mapped C string"')
    if not (0 <= first_reset < vfs_probe < second_reset < vmem_probe):
        fail("each C-string probe must restore its non-terminating mapped prefix after partial copyout")


def test_probe_exercises_materialization_and_shared_pagemap_races() -> None:
    source = PROBE.read_text()
    require_tokens(
        source,
        [
            "probe_lazy_output(state);",
            "probe_cow_output(state);",
            "fork();",
            '"COW parent isolation"',
            "probe_concurrent_unmap(state);",
            "pthread_create(&worker, nullptr, race_mapper, &context)",
            "MAP_ANONYMOUS | MAP_FIXED",
            "RESULT != 0 && RESULT != -EFAULT",
            '"private segment cleanup"',
            "ker::abi::shm::IPC_RMID",
        ],
        "runtime lazy/COW/race and cleanup contract",
    )
    require_tokens(
        MANIFEST.read_text(),
        [
            "testprog usercopy-negative",
            "user-run runtime evidence",
        ],
        "documented runtime verification command",
    )


def main() -> None:
    test_probe_is_opt_in_and_built()
    test_probe_covers_pointer_families_and_address_classes()
    test_probe_exercises_materialization_and_shared_pagemap_races()
    print("usercopy runtime probe source invariants hold")


if __name__ == "__main__":
    main()
