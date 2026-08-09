#!/usr/bin/env python3

import re
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL = ROOT / "modules" / "kern" / "src"
SYSCALL_IMPL = KERNEL / "syscalls_impl"
EXEC_CPP = SYSCALL_IMPL / "process" / "exec.cpp"
VMEM_CPP = SYSCALL_IMPL / "vmem" / "sys_vmem.cpp"
TASK_CPP = KERNEL / "platform" / "sched" / "task.cpp"
SMT_CPP = KERNEL / "platform" / "smt" / "smt.cpp"
VFS_CORE_CPP = KERNEL / "vfs" / "core.cpp"

REGISTER_POINTER_CAST = re.compile(r"reinterpret_cast<[^>]+>\((?:a[1-6]|arg[1-6])\)")

# These casts are address tokens only. Their consumers are locked below to
# syscall-layer usercopy helpers or to the four VFS byte-I/O bounce entrypoints.
EXPECTED_REGISTER_POINTER_CASTS = {
    "modules/kern/src/syscalls_impl/process/process.cpp": Counter(
        {
            "reinterpret_cast<int32_t*>(a3)": 1,
            "reinterpret_cast<KernelUtsname*>(a2)": 1,
            "reinterpret_cast<const KernelStackT*>(a2)": 1,
            "reinterpret_cast<KernelStackT*>(a3)": 1,
        }
    ),
    "modules/kern/src/syscalls_impl/vfs/sys_vfs.cpp": Counter(
        {
            "reinterpret_cast<void*>(a2)": 2,
            "reinterpret_cast<const void*>(a2)": 2,
            "reinterpret_cast<size_t*>(a4)": 2,
            "reinterpret_cast<off_t*>(a4)": 1,
            "reinterpret_cast<off_t*>(a3)": 1,
            "reinterpret_cast<ker::vfs::Stat*>(a2)": 4,
            "reinterpret_cast<int*>(a3)": 1,
            "reinterpret_cast<const ker::abi::vfs::metadata_batch_header*>(a1)": 1,
            "reinterpret_cast<const MetadataBatchUserEntry*>(a2)": 1,
            "reinterpret_cast<MetadataBatchUserResult*>(a3)": 1,
            "reinterpret_cast<ker::vfs::Stat*>(a3)": 1,
            "reinterpret_cast<char*>(a1)": 1,
            "reinterpret_cast<ker::vfs::EpollEvent*>(a4)": 1,
            "reinterpret_cast<char*>(a2)": 2,
            "reinterpret_cast<uint32_t*>(a4)": 2,
            "reinterpret_cast<ker::vfs::Statvfs*>(a2)": 2,
        }
    ),
}


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


def function_body(source: str, spelling: str) -> str:
    start = source.find(spelling)
    if start < 0:
        fail(f"missing function {spelling}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"missing body for {spelling}")
    return source[brace + 1 : find_matching_brace(source, brace)]


def code_without_comments_or_literals(source: str) -> str:
    pattern = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', flags=re.DOTALL)
    return pattern.sub(lambda match: "\n" * match.group(0).count("\n"), source)


def test_register_to_pointer_conversions_are_closed() -> None:
    boundary_files = sorted(SYSCALL_IMPL.rglob("*.cpp")) + [
        KERNEL / "platform" / "sys" / "syscall.cpp",
        KERNEL / "platform" / "sys" / "signal.cpp",
        KERNEL / "platform" / "debug" / "ptrace.cpp",
    ]
    actual: dict[str, Counter[str]] = {}
    for path in boundary_files:
        casts = Counter(REGISTER_POINTER_CAST.findall(path.read_text()))
        if casts:
            actual[str(path.relative_to(ROOT))] = casts

    if actual != EXPECTED_REGISTER_POINTER_CASTS:
        fail(f"syscall register-to-pointer audit drifted:\nexpected={EXPECTED_REGISTER_POINTER_CASTS}\nactual={actual}")

    for path in boundary_files:
        source = code_without_comments_or_literals(path.read_text())
        relative = path.relative_to(ROOT)
        forbidden = [
            re.compile(r"\([^()]*\*\)\s*(?:a[1-6]|arg[1-6])\b"),
            re.compile(r"\*\s*(?:a[1-6]|arg[1-6])\b"),
            re.compile(r"(?:a[1-6]|arg[1-6])\s*(?:\[|->)"),
        ]
        for pattern in forbidden:
            if pattern.search(source):
                fail(f"{relative} directly casts/dereferences a syscall register with {pattern.pattern}")


def test_raw_translation_exceptions_are_only_unpublished_images() -> None:
    translation_files: dict[str, str] = {}
    for path in sorted(SYSCALL_IMPL.rglob("*.cpp")):
        source = path.read_text()
        if re.search(r"\b(?:translate|get_virt_pointer)\s*\(", source):
            translation_files[str(path.relative_to(ROOT))] = source

    expected_files = {str(EXEC_CPP.relative_to(ROOT)), str(VMEM_CPP.relative_to(ROOT))}
    if set(translation_files) != expected_files:
        fail(f"raw VM access appeared at a syscall boundary: {sorted(set(translation_files) - expected_files)}")

    exec_source = translation_files[str(EXEC_CPP.relative_to(ROOT))]
    expected_exec_calls = {
        "translate(new_task->pagemap, PAGE_VIRT)": 1,
        "translate(new_pagemap, PAGE_VIRT)": 1,
        "translate(new_pagemap, new_thread->fsbase)": 1,
        "translate(new_pagemap, DEST_VADDR)": 1,
        "get_virt_pointer(PAGE_PHYS)": 2,
        "get_virt_pointer(TCB_PADDR)": 1,
        "get_virt_pointer(DEST_PADDR)": 1,
    }
    for call, count in expected_exec_calls.items():
        if exec_source.count(call) != count:
            fail(f"exec raw unpublished-image exception drifted: {call} count={exec_source.count(call)}, expected={count}")
    if exec_source.count("::translate(") != 4 or exec_source.count("::get_virt_pointer(") != 4:
        fail("exec gained a raw VM access outside its enumerated unpublished-image initialization")

    vmem_source = translation_files[str(VMEM_CPP.relative_to(ROOT))]
    table_from_entry = function_body(vmem_source, "auto table_from_entry(")
    if "get_virt_pointer" not in table_from_entry or vmem_source.count("get_virt_pointer(") != 1 or "translate(" in vmem_source:
        fail("VMEM raw access must remain limited to walking kernel page-table frames")


def test_transitive_address_consumers_do_not_raw_access_userspace() -> None:
    smt_set_tcb = function_body(SMT_CPP.read_text(), "auto set_tcb(")
    for forbidden in ["reinterpret_cast", "static_cast<uint64_t*>", "get_virt_pointer", "translate("]:
        if forbidden in smt_set_tcb:
            fail(f"IRQ-disabled set_tcb consumes a raw user address: {forbidden}")

    create_user_thread = function_body(TASK_CPP.read_text(), "Task* Task::create_user_thread(")
    for forbidden in ["get_virt_pointer", "translate(", "PA_ENTRY", "PA_ARG"]:
        if forbidden in create_user_thread:
            fail(f"thread construction raw-reads its syscall-supplied stack: {forbidden}")

    vfs = VFS_CORE_CPP.read_text()
    expected_vfs_bounces = {
        "auto vfs_read(": "vfs_read_user_bounced(*t, f, buf, count",
        "auto vfs_write(": "vfs_write_file(f, buf, count, actual_size)",
        "auto vfs_pread(": "vfs_read_user_bounced(*task, f, buf, count",
        "auto vfs_pwrite(": "vfs_pwrite_user_bounced(*task, f, buf, count",
    }
    for function, bounce in expected_vfs_bounces.items():
        if bounce not in function_body(vfs, function):
            fail(f"{function} no longer routes syscall byte buffers through the bounded VFS usercopy seam")

    write_file = function_body(vfs, "auto vfs_write_file(")
    if "vfs_write_user_bounced(*task, f, buf, count, actual_size)" not in write_file:
        fail("vfs_write must route eligible syscall buffers through vfs_write_user_bounced")


def main() -> None:
    test_register_to_pointer_conversions_are_closed()
    test_raw_translation_exceptions_are_only_unpublished_images()
    test_transitive_address_consumers_do_not_raw_access_userspace()
    print("syscall raw user-access audit is closed")


if __name__ == "__main__":
    main()
