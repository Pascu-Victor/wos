#!/usr/bin/env python3
"""Compile actual managed-huge owners and extracted MM helpers on the Host.

Requires an already configured Host build and its existing GTest libraries.
Root/PUB/locks/address/frame/flush endpoints are explicit C++ test seams. This
does not execute WOS, a buddy allocator, an actual TLB ACK or an SMP workload.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import tempfile
import time

EXPECTED_TESTS = 60
OVERALL_SECONDS = 120
COMMAND_SECONDS = 35
SCRATCH_BYTES = 128 * 1024 * 1024


def pin(path: Path) -> dict:
    path = path.resolve()
    before = path.stat()
    data = path.read_bytes()
    after = path.stat()
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    if identity(before) != identity(after):
        raise RuntimeError(f"input changed while reading: {path}")
    return {
        "path": str(path),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "device": after.st_dev,
        "inode": after.st_ino,
        "mtime_ns": after.st_mtime_ns,
        "ctime_ns": after.st_ctime_ns,
    }


def bounded(text: str, first: str, last: str) -> str:
    if text.count(first) != 1 or text.count(last) != 1:
        raise RuntimeError(f"actual extraction boundary changed: {first!r}, {last!r}")
    return text[text.index(first) : text.index(last) + len(last)]


def compile_flags(row: dict, overlay: Path | None) -> list[str]:
    original = shlex.split(row["command"])
    result = [original[0]]
    if overlay is not None:
        result.append(f"-I{overlay}")
    result.append("-DWOS_HOST_TEST=1")
    index = 1
    while index < len(original):
        argument = original[index]
        if argument in ("-o", "-MF", "-MT", "-MQ"):
            index += 2
        elif argument in ("-c", "-MD", "-MMD", "-MP") or argument == row["file"]:
            index += 1
        else:
            result.append(argument)
            index += 1
    # Match the actual target contract for raw aligned PageTable words.
    return result + ["-fno-strict-aliasing"]


class CppText(str):
    """Unique token sequence replacement tolerates canonical C++ wrapping."""

    @staticmethod
    def pattern(needle):
        tokens = re.findall(
            r'"(?:\\.|[^"\\])*"|//[^\n]*|[A-Za-z_][A-Za-z_0-9]*|0[xX][0-9a-fA-F]+|[0-9]+|::|->|==|!=|>=|<=|&&|\|\||[^\s]',
            needle,
        )
        return r"(?<![A-Za-z_0-9])" + r"\s*".join(re.escape(token) for token in tokens) + r"(?![A-Za-z_0-9])"

    def count(self, needle):
        return len(re.findall(self.pattern(needle), self))

    def replace(self, needle, replacement):
        return CppText(re.sub(self.pattern(needle), lambda _: replacement, self))

    def gtest_cases(self) -> list[dict]:
        """Finite TEST/TEST_F discovery; byte offsets always refer to the original."""
        original = str(self).encode()
        # One search character per UTF-8 byte. Never index the Unicode source
        # with these offsets; retain both units for the source proof below.
        source = original.decode("latin1")
        masked = list(source)
        raw_pattern = re.compile(r'(?:u8|u|U|L)?R"([^ ()\\\t\r\n]{0,16})\(')
        raw_prefix = re.compile(r'(?:u8|u|U|L)?R"')
        literal_pattern = re.compile(r'(?:u8|u|U|L)?(["\'])')
        interesting = re.compile(r"""(?:u8|u|U|L)?(?:R"|["'])|[0-9\xb2\xb3\xb9/\\]""")
        cursor = 0
        while cursor < len(source):
            found = interesting.search(source, cursor)
            if found is None:
                break
            cursor = found.start()
            start = cursor
            if source.startswith("//", cursor):
                cursor = source.find("\n", cursor)
                if cursor < 0:
                    cursor = len(source)
                if source[start:cursor].rstrip("\r").endswith("\\"):
                    raise ValueError("continued line comment is unsupported")
            elif source.startswith("/*", cursor):
                end = source.find("*/", cursor + 2)
                if end < 0:
                    raise ValueError("unterminated C++ comment")
                cursor = end + 2
                if re.search(r"\\\r?\n", source[start:cursor]):
                    raise ValueError("continued block comment is unsupported")
            else:
                raw = raw_pattern.match(source, cursor)
                literal = literal_pattern.match(source, cursor)
                if raw is not None:
                    if any(not 33 <= ord(char) <= 126 for char in raw[1]):
                        raise ValueError("unsupported raw C++ delimiter")
                    end = source.find(")" + raw[1] + '"', raw.end())
                    if end < 0:
                        raise ValueError("unterminated raw C++ literal")
                    cursor = end + len(raw[1]) + 2
                elif raw_prefix.match(source, cursor):
                    raise ValueError("unsupported raw C++ delimiter")
                elif literal is not None:
                    quote = literal[1]
                    cursor = literal.end()
                    while cursor < len(source) and source[cursor] != quote:
                        if source[cursor] == "\\":
                            cursor += 3 if source.startswith("\\\r\n", cursor) else 2
                        elif source[cursor] in "\r\n":
                            raise ValueError("unescaped newline in C++ literal")
                        else:
                            cursor += 1
                    if cursor >= len(source):
                        raise ValueError("unterminated C++ literal")
                    cursor += 1
                else:
                    if source[cursor].isdigit():
                        cursor += 1
                        while cursor < len(source):
                            char = source[cursor]
                            if char.isalnum() or char in "_.":
                                cursor += 1
                            elif char == "'" and cursor + 1 < len(source) and source[cursor + 1].isalnum():
                                cursor += 2
                            else:
                                break
                    else:
                        if source[cursor] == "\\" and source[cursor + 1:cursor + 2] in ("\n", "\r"):
                            raise ValueError("continued C++ code is unsupported")
                        cursor += 1
                    continue
            masked[start:cursor] = [char if char in "\r\n" else " " for char in source[start:cursor]]
        code = "".join(masked)
        directives = list(re.finditer(r"(?m)^[ \t]*#[ \t]*([A-Za-z_][A-Za-z_0-9]*)[^\n]*", code))
        if any(re.search(r"\b(?:TEST(?:_[A-Za-z_0-9]+)?|TYPED_TEST(?:_[A-Za-z_0-9]+)?|GTEST_TEST)\b", item[0]) for item in directives):
            raise ValueError("preprocessor GTest registration is unsupported")
        declarations = list(re.finditer(r"\b(?:TEST(?:_[A-Za-z_][A-Za-z_0-9]*)?|TYPED_TEST(?:_[A-Za-z_][A-Za-z_0-9]*)?|GTEST_TEST_?|INSTANTIATE_TEST(?:_[A-Za-z_][A-Za-z_0-9]*)?|REGISTER_TYPED_TEST(?:_[A-Za-z_][A-Za-z_0-9]*)?)\s*\(", code))
        cases, names = [], set()
        for registration in declarations:
            start = registration.start()
            if cases and start < cases[-1]["byte_end"]:
                raise ValueError("nested or overlapping GTest registration")
            line = code.rfind("\n", 0, start) + 1
            if "#" in code[line:start]:
                raise ValueError("preprocessor GTest registration is unsupported")
            conditional = 0
            for item in directives:
                if item.start() >= start:
                    break
                conditional += (item[1] in ("if", "ifdef", "ifndef")) - (item[1] == "endif")
                if conditional < 0:
                    raise ValueError("unbalanced preprocessor condition")
            if conditional:
                raise ValueError("conditional GTest registration is unsupported")
            declaration = re.match(r"TEST(?:_F)?\s*\(\s*([A-Za-z_][A-Za-z_0-9]*)\s*,\s*([A-Za-z_][A-Za-z_0-9]*)\s*\)\s*\{", code[start:])
            if declaration is None or any(part.startswith("DISABLED_") for part in declaration.groups()):
                raise ValueError("unsupported GTest declaration")
            name = ".".join(declaration.groups())
            if name in names:
                raise ValueError("duplicate GTest case")
            end, depth = start + declaration.end(), 1
            while depth and end < len(code):
                depth += (code[end] == "{") - (code[end] == "}")
                end += 1
            if depth:
                raise ValueError("unbalanced GTest body")
            if any(start < item.start() < end for item in directives):
                raise ValueError("preprocessing within GTest body is unsupported")
            names.add(name)
            cases.append(dict(name=name, byte_start=start, byte_end=end,
                              character_start=len(original[:start].decode()), character_end=len(original[:end].decode()),
                              sha256=hashlib.sha256(original[start:end]).hexdigest()))
        return cases


def control_case_names(test: str, baseline_names: set[str]) -> set[str]:
    terms = test.split(":")
    if not terms or len(terms) != len(set(terms)) or any(
        re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*", term) is None
        or any(part.startswith("DISABLED_") for part in term.split(".")) for term in terms
    ):
        raise ValueError("control requires unique exact positive GTest names")
    names = set(terms)
    if not names <= baseline_names:
        raise ValueError("control case absent from actual complete baseline")
    return names


def verify_control_source(full: str, projection: str, test: str, baseline_names: set[str]) -> dict:
    wanted = control_case_names(test, baseline_names)
    cases = CppText(full).gtest_cases()
    if not wanted <= {case["name"] for case in cases}:
        raise ValueError("selected control case absent from mutant composite")
    original, actual = full.encode(), projection.encode()
    expected = bytearray(original)
    for case in cases:
        case["selected"] = case["name"] in wanted
        if not case["selected"]:
            start, end = case["byte_start"], case["byte_end"]
            expected[start:end] = bytes(char if char in (10, 13) else 32 for char in original[start:end])
    if actual != expected:
        raise ValueError("control projection changed support, selected body or deletion geometry")
    if {case["name"] for case in CppText(projection).gtest_cases()} != wanted:
        raise ValueError("projected GTest inventory differs from exact filter")
    restored = bytearray(actual)
    for case in cases:
        if not case["selected"]:
            start, end = case["byte_start"], case["byte_end"]
            restored[start:end] = original[start:end]
    if restored != original:
        raise ValueError("control projection inverse failed")
    return dict(full_sha256=hashlib.sha256(original).hexdigest(), projected_sha256=hashlib.sha256(actual).hexdigest(),
                bytes=len(original), selected=sorted(wanted), cases=cases, offset_unit="UTF-8 bytes; character offsets also recorded",
                exact_gaps_bodies_inverse=True)


def select_control_source(full: str, test: str, baseline_names: set[str]) -> tuple[str, dict]:
    wanted = control_case_names(test, baseline_names)
    raw = bytearray(full.encode())
    for case in CppText(full).gtest_cases():
        if case["name"] not in wanted:
            start, end = case["byte_start"], case["byte_end"]
            raw[start:end] = bytes(char if char in (10, 13) else 32 for char in raw[start:end])
    projection = raw.decode()
    return projection, verify_control_source(full, projection, test, baseline_names)


def baseline_gtest_names(report: dict) -> set[str]:
    names = [suite["name"] + "." + case["name"] for suite in report["testsuites"] for case in suite["testsuite"]]
    if len(names) != len(set(names)) or len(names) != report["tests"]:
        raise ValueError("actual baseline GTest names are incomplete or duplicated")
    return set(names)


def definition(text: str, anchor: str) -> str:
    if text.count(anchor) != 1:
        raise RuntimeError(f"actual definition changed: {anchor}")
    first = text.index(anchor)
    body = text.index("{", first)
    depth = 1
    end = body + 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    if end < len(text) and text[end] == ";":
        end += 1
    return text[first:end]


def context_overlay(scratch: Path, kernel_file, source_root: Path) -> tuple[Path, str, list[Path], dict]:
    """Actual guards/Task lease methods; only privileged CPU and backend ports are Host seams."""
    overlay = scratch / "context-include"
    sources = {
        name: kernel_file(name)
        for name in (
            "platform/sched/scheduler.hpp",
            "platform/sched/task.cpp",
            "platform/sys/syscall_preparation.hpp",
            "platform/sys/mutex.cpp",
            "syscalls_impl/vmem/sys_vmem.hpp",
            "syscalls_impl/vmem/sys_vmem.cpp",
        )
    }
    scheduler = sources["platform/sched/scheduler.hpp"].read_text()
    guard = definition(scheduler, "struct PreemptGuard {")
    common = """#pragma once
#include <atomic>
#include <cstdint>
namespace ker::mod::sched::task { struct Task; }
namespace host_range {
inline bool interrupts = true, queryable = true, reader_busy = false, exclusive = false;
inline unsigned yields = 0, tries = 0, unlocks = 0, blocking = 0;
inline ker::mod::sched::task::Task* current = nullptr;
inline void (*on_yield)() = nullptr;
void check_yield_boundary();
}
namespace ker::mod::sched {
void preempt_disable(); void preempt_enable(); auto preempt_count() -> uint32_t;
inline auto interrupts_enabled() -> bool { return host_range::interrupts; }
inline auto can_query_current_task() -> bool { return host_range::queryable; }
inline auto get_current_task() -> task::Task* { return host_range::current; }
void kern_yield();
""" + guard + "\n}\n"
    task = """#pragma once
#include <atomic>
#include <cstdint>
#include <platform/mm/paging.hpp>
namespace ker::mod::sched::task {
struct LazyVmemRange;
enum class TaskType { PROCESS, THREAD, DAEMON };
enum class TaskState { ACTIVE, EXITED };
// Explicit layout seam: only fields read by actual extracted methods/guards.
struct Task {
 TaskType type{TaskType::PROCESS}; std::atomic<TaskState> state{TaskState::ACTIVE};
 std::atomic<bool> scheduler_transition_active{false}, exit_in_progress{false}, usercopy_pagemap_closing{false};
 std::atomic<uint32_t> usercopy_pagemap_accesses{0}; uint64_t syscall_account_start_us{1};
 mm::paging::PageTable* pagemap{};
 auto try_acquire_usercopy_pagemap(mm::paging::PageTable*&) -> bool;
 void release_usercopy_pagemap();
};
}
"""
    preparation = sources["platform/sys/syscall_preparation.hpp"].read_text()
    count_cli, count_sti = preparation.count('asm volatile("cli" ::: "memory");'), preparation.count(
        'asm volatile("sti" ::: "memory");'
    )
    if (count_cli, count_sti) != (2, 2):
        raise RuntimeError("privileged preparation endpoints changed")
    preparation = preparation.replace('asm volatile("cli" ::: "memory");', "host_range::interrupts = false;")
    preparation = preparation.replace('asm volatile("sti" ::: "memory");', "host_range::interrupts = true;")
    header = sources["syscalls_impl/vmem/sys_vmem.hpp"].read_text()
    classes = [
        definition(header, f"class {name} {{")
        for name in ("SharedVmemPublicationGuard", "SharedVmemPublicationTryGuard")
    ]
    publication = "#pragma once\nnamespace ker::syscall::vmem {\n" + "\n".join(classes) + "\n}\n"
    for name, text in (
        ("platform/sched/scheduler.hpp", common),
        ("platform/sched/task.hpp", task),
        ("platform/sys/syscall_preparation.hpp", preparation),
        ("syscalls_impl/vmem/sys_vmem.hpp", publication),
    ):
        path = overlay / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    tasks = sources["platform/sched/task.cpp"].read_text()
    methods = (
        "namespace ker::mod::sched::task {\n"
        + "\n".join(
            definition(tasks, anchor)
            for anchor in ("auto Task::try_acquire_usercopy_pagemap(", "void Task::release_usercopy_pagemap()")
        )
        + "\n}\n"
    )
    mutex = sources["platform/sys/mutex.cpp"].read_text()
    mutex_methods = (
        "namespace ker::mod::sys {\n"
        + "\n".join(definition(mutex, anchor) for anchor in ("auto Mutex::try_lock()", "void Mutex::unlock()"))
        + "\n}\n"
    )
    cpp = sources["syscalls_impl/vmem/sys_vmem.cpp"].read_text()
    constructors = (
        "namespace ker::syscall::vmem {\n"
        + "constinit std::atomic<const SharedVmemPublicationGuard*> g_shared_vmem_publication_owner{nullptr};\n"
        + definition(cpp, "[[nodiscard]] auto blocking_publication_owner_matches(") + "\n"
        + "\n".join(
            definition(cpp, anchor)
            for anchor in (
                "SharedVmemPublicationGuard::SharedVmemPublicationGuard()",
                "auto SharedVmemPublicationGuard::acquired()",
                "SharedVmemPublicationGuard::~SharedVmemPublicationGuard()",
                "SharedVmemPublicationTryGuard::SharedVmemPublicationTryGuard()",
                "SharedVmemPublicationTryGuard::~SharedVmemPublicationTryGuard()",
            )
        )
        + "\n}\n"
    )
    mapping = {
        "preempt_guard": guard,
        "task_methods": methods,
        "mutex_methods": mutex_methods,
        "publication_classes": publication,
        "publication_methods": constructors,
        "preparation_host_ports": preparation,
    }
    return overlay, methods + mutex_methods + constructors, list(sources.values()), mapping


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--host-build",
        type=Path,
        required=True,
        help="existing configured Host tree containing compile_commands.json and lib/libgtest*.a",
    )
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument(
        "--kernel-overlay", type=Path, help="optional explicit staged modules/kern/src include/source overlay"
    )
    parser.add_argument("--fork-contract-only", action="store_true", help="run all old positive cases plus the focused fork cases and two fork controls")
    parser.add_argument("--fork-vm-coordinator-only", action="store_true", help="exercise the private actual-owner fork VM composition")
    parser.add_argument("--anonymous-remove-only", action="store_true", help="exercise unwired managed anonymous removal with the retained original positive vectors")
    parser.add_argument("--mremap-policy-only", action="store_true", help="exercise real counted unsupported-PS mremap admission and recovery")
    parser.add_argument("--map-batch-only", action="store_true", help="exercise the unwired whole-batch mapper plus all original positive cases")
    parser.add_argument("--current-huge-map-only", action="store_true", help="exercise actual current Task-local admission and original whole-batch publisher")
    parser.add_argument("--mixed-geometry-only", action="store_true", help="exercise prepared mixed geometry before the ordinary removal continuation")
    parser.add_argument("--mixed-protect-only", action="store_true", help="exercise prepared mixed geometry before the ordinary protection continuation")
    parser.add_argument("--anonymous-fixed-only", action="store_true", help="exercise private prepared anonymous fixed replacement and four new controls")
    parser.add_argument("--file-fixed-only", action="store_true", help="exercise private prepared retained-File fixed replacement and four new controls")
    parser.add_argument("--public-mutation-only", action="store_true", help="exercise actual public mutation dispatch, geometry proof and four source controls")
    parser.add_argument("--public-huge-map-only", action="store_true", help="exercise actual eager private anonymous huge construction and four controls")
    parser.add_argument("--shm-occupancy-only", action="store_true", help="exercise real SHM overlap rejection through PS-aware occupied walkers")
    parser.add_argument("--gtest-main", type=Path)
    parser.add_argument("--gtest", type=Path)
    parser.add_argument("--output", type=Path, help="fresh directory for small logs/results; scratch is always removed")
    options = parser.parse_args()
    selected_profiles = (options.mremap_policy_only, options.fork_contract_only, options.fork_vm_coordinator_only, options.anonymous_remove_only, options.map_batch_only, options.current_huge_map_only, options.mixed_geometry_only, options.mixed_protect_only, options.anonymous_fixed_only, options.file_fixed_only, options.public_mutation_only, options.public_huge_map_only, options.shm_occupancy_only)
    if sum(selected_profiles) > 1:
        parser.error("select at most one explicit managed memory profile")
    overall_seconds = 180 if options.mixed_protect_only or options.anonymous_fixed_only or options.file_fixed_only or options.public_mutation_only or options.public_huge_map_only else OVERALL_SECONDS
    root = options.source_root.resolve()
    host_build = options.host_build.resolve()
    overlay = options.kernel_overlay.resolve() if options.kernel_overlay else None
    support = Path(__file__).resolve().parent
    output = options.output.resolve() if options.output else Path(tempfile.mkdtemp(prefix="managed-huge-result-"))
    if options.output:
        output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    scratch_identity = scratch.lstat()
    started = time.monotonic()
    commands: list[dict] = []
    controls: list[dict] = []
    groups: list[int] = []
    processes: list[subprocess.Popen] = []
    errors: list[str] = []
    selected: list[dict] = []
    emitted: list[dict] = []
    pending_signal = None

    def check_interrupted() -> None:
        if pending_signal is not None:
            raise KeyboardInterrupt(f"received signal {pending_signal}")
    environment = dict(
        os.environ,
        TMPDIR=str(scratch),
        TMP=str(scratch),
        TEMP=str(scratch),
        ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
        UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1",
    )

    def kernel_file(relative: str) -> Path:
        staged = overlay / relative if overlay else None
        return staged if staged is not None and staged.is_file() else root / "modules/kern/src" / relative

    def allocated_bytes() -> int:
        return sum(path.stat().st_blocks * 512 for path in scratch.rglob("*") if path.is_file())

    def run(label: str, arguments: list, expected_success: bool = True) -> None:
        check_interrupted()
        remaining = overall_seconds - (time.monotonic() - started)
        if remaining <= 0 or allocated_bytes() > SCRATCH_BYTES:
            raise RuntimeError("overall time/scratch budget exhausted")
        argv = [str(argument) for argument in arguments]
        log = output / f"{label}.log"
        before = time.monotonic()
        deadline = min(started + overall_seconds, before + COMMAND_SECONDS)
        work_deadline = deadline - 1  # Retain cleanup inside the original command/overall caps.
        if work_deadline <= before:
            raise RuntimeError("command cleanup reserve exhausted")
        with log.open("wb") as stream:
            process = subprocess.Popen(
                argv, cwd=root, env=environment, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True
            )
            groups.append(process.pid)
            processes.append(process)
            try:
                # A signal across Popen return/adoption only sets the pending
                # flag. Check it after both ownership records are installed.
                while True:
                    check_interrupted()
                    remaining = work_deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(argv, COMMAND_SECONDS)
                    if os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None:
                        break
                    time.sleep(min(0.01, remaining))
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"{label}: timed out")
            finally:
                # Keep the leader/SID reserved while the existing owner reaps
                # adopted sanitizer helpers, including on fatal negative cases.
                process_owner.reap_group(process, deadline)
                groups.remove(process.pid)
        code = process.returncode
        check_interrupted()
        try:
            os.killpg(process.pid, 0)
            absent = False
        except ProcessLookupError:
            absent = True
        commands.append(
            {
                "label": label,
                "argv": argv,
                "exit": code,
                "seconds": time.monotonic() - before,
                "leader_reaped": True,
                "owned_group_absent": absent,
                "log": pin(log),
            }
        )
        if not absent or (code == 0) != expected_success:
            raise RuntimeError(f"{label}: unexpected exit {code}")

    saved_signals = {}

    def interrupted(number, _frame):
        nonlocal pending_signal
        if pending_signal is None:
            pending_signal = number

    for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        saved_signals[number] = signal.signal(number, interrupted)

    try:
        module = kernel_file("platform/mm/managed_huge.cpp")
        factory = kernel_file("platform/mm/numa_alloc.cpp")
        virt = kernel_file("platform/mm/virt.opt.cpp")
        header = kernel_file("platform/mm/virt.hpp")
        fixture = support / "managed_huge_test.cpp"
        prelude = support / "managed_huge_resolver_prelude.inc"
        cases = support / "managed_huge_resolver_cases.inc"
        libraries = [
            path.resolve()
            for path in [
                options.gtest_main or host_build / "lib/libgtest_main.a",
                options.gtest or host_build / "lib/libgtest.a",
            ]
        ]
        process_owner_path = support / "file_mmap_registry_runtime_test.py"
        selected = [
            pin(path) for path in [Path(__file__), process_owner_path, module, factory, virt, header, fixture, prelude, cases, *libraries]
        ]
        owner_spec = importlib.util.spec_from_file_location("managed_huge_process_owner", process_owner_path)
        process_owner = importlib.util.module_from_spec(owner_spec)
        owner_spec.loader.exec_module(process_owner)
        process_owner.subreaper()
        if pin(process_owner_path) != selected[1]:
            raise RuntimeError("process-owner source changed during import")

        rows = json.loads((host_build / "compile_commands.json").read_text())
        row = next(
            item
            for item in rows
            if item["file"].endswith("/tests/host/unit/numa_alloc_test.cpp")
            and "numa_alloc_test.dir" in item["command"]
        )
        (output / "actual-host-command-row.json").write_text(json.dumps(row, indent=2) + "\n")
        context, context_methods, context_sources, context_map = context_overlay(scratch, kernel_file, root)
        selected.extend(pin(path) for path in context_sources)
        batch_fixture = None
        if options.map_batch_only or options.current_huge_map_only:
            if options.fork_vm_coordinator_only or options.anonymous_remove_only or options.fork_contract_only:
                raise RuntimeError("batch profile must be selected alone")
            import managed_huge_batch_fixture as batch_composition
            batch_fixture = (batch_composition.CurrentBuilder if options.current_huge_map_only else batch_composition.Builder)(kernel_file, support, definition, bounded)
            selected.extend(pin(path) for path in batch_fixture.inputs)
        policy_fixture = None
        if options.mremap_policy_only:
            import managed_mremap_policy_fixture as policy_composition
            policy_fixture = policy_composition.Builder(kernel_file, support, definition, bounded)
            policy_fixture.extend_context(context)
            selected.extend(pin(path) for path in policy_fixture.inputs)
        coordinator = None
        if options.fork_vm_coordinator_only or options.anonymous_remove_only or options.mixed_geometry_only or options.mixed_protect_only or options.anonymous_fixed_only or options.file_fixed_only or options.public_mutation_only or options.public_huge_map_only or options.shm_occupancy_only:
            if options.shm_occupancy_only:
                import managed_shm_occupancy_fixture as composition
            elif options.mixed_geometry_only or options.mixed_protect_only or options.anonymous_fixed_only or options.file_fixed_only or options.public_mutation_only or options.public_huge_map_only:
                import managed_mixed_geometry_fixture as composition
            elif options.anonymous_remove_only:
                import managed_anonymous_remove_fixture as composition
            else:
                import fork_vm_coordinator_fixture as composition
            coordinator = (composition.PublicHugeMapBuilder if options.public_huge_map_only else composition.PublicMutationBuilder if options.public_mutation_only else composition.FileFixedBuilder if options.file_fixed_only else composition.FixedBuilder if options.anonymous_fixed_only else composition.ProtectBuilder if options.mixed_protect_only else composition.Builder)(kernel_file, support, definition)
            coordinator.extend_context(context)
            selected.extend(pin(path) for path in coordinator.inputs)
        flags = compile_flags(row, overlay)
        flags.insert(1, f"-I{context}")
        selected.append(pin(Path(shutil.which(flags[0]) or flags[0])))
        source = virt.read_text()
        resolver = bounded(source, "// BEGIN managed user-leaf scalar core.", "// END managed user-leaf scalar core.")
        private = bounded(
            source, "// BEGIN private managed publication/split.", "// END private managed publication/split."
        )
        commit = bounded(
            source,
            "auto user_page_pin_commit_write(const UserPagePin& pin) -> bool {",
            "void unpin_user_page(UserPagePin& pin) {",
        ).rsplit("void unpin_user_page", 1)[0]
        index = next(line for line in source.splitlines() if line.startswith("auto index_of("))
        user_pin = definition(header.read_text(), "struct UserPagePin {")
        exclusive = definition(source, "class UserPagemapExclusiveScope {")
        copier = "\n".join(definition(source, anchor) for anchor in (
            "auto valid_fork_placement(", "auto deep_copy_user_pagemap_cow_held(",
            "auto fork_eager_copy_requested()", "auto unpublished_user_root_empty("))
        declaration_start = header.read_text().index("[[nodiscard]] auto deep_copy_current_user_pagemap_prepared(")
        declaration_end = header.read_text().index(";", declaration_start) + 1
        copier += "\n" + header.read_text()[declaration_start:declaration_end]

        epoch = definition(source, "[[gnu::always_inline]] void advance_ps_geometry_epoch_locked(")
        adapters = "\n".join(
            definition(source, anchor)
            for anchor in (
                "auto copy_current_user_pagemap_exclusive(sched::task::Task&",
                "auto acquire_current_range_root(sched::task::Task&",
                "void release_current_range_root(CurrentRangeRootAccess&",
                "auto current_mutation_access_matches(",
                "auto survey_current_range_chunk(",
                "auto validate_current_range_geometry(",
                "auto survey_range_chunk(paging::PageTable*",
                "auto commit_range_split(paging::PageTable*",
            )
        )
        if coordinator is None:
            adapters += (
                "\n} namespace ker::mod::mm::virt {\n"
                + definition(source, "auto get_kernel_pagemap() -> PageTable*")
                + "\n} namespace ker::mod::mm::huge::detail {\n"
            )
        range_cases = support / "managed_huge_range_cases.inc"
        fork_support = support / "managed_huge_fork_support.inc"
        fork_cases = support / "managed_huge_fork_cases.inc"
        selected.extend([pin(fork_support), pin(fork_cases)])
        selected.append(pin(range_cases))
        composite = scratch / "actual-source.cpp"
        if coordinator is not None:
            composite.write_text(coordinator.compose(fixture.read_text(), prelude.read_text(), context_methods,
                fork_support.read_text(), user_pin, exclusive, epoch, index, resolver, private, copier, commit,
                adapters, cases.read_text(), range_cases.read_text(), fork_cases.read_text()))
            public_source = scratch / "actual-public-caller.cpp"
            public_source.write_text(coordinator.public.caller_source())
            shutil.copy2(public_source, output / public_source.name)
        else:
            composite.write_text(
            "\n".join(
                [
                    fixture.read_text(),
                    prelude.read_text(),
                    context_methods,
                    fork_support.read_text(),
                    "namespace ker::mod::mm::virt {",
                    user_pin,
                    exclusive,
                    epoch,
                    index,
                    resolver,
                    private,
                    copier,
                    commit,
                    "} namespace ker::mod::mm::huge::detail {",
                    adapters,
                    "}",
                    "namespace ker::mod::mm::virt {",
                    cases.read_text(),
                    range_cases.read_text(),
                    fork_cases.read_text(),
                ]
            )
        )
        if batch_fixture is not None:
            composite.write_text(batch_fixture.compose(composite.read_text()))
            shutil.copy2(composite, output / "actual-batch-source.cpp")
        elif policy_fixture is None and not options.public_huge_map_only:
            # Newly unwired method has no real batch mapper in legacy profiles.
            # Link-only finite refusal; accidental execution is always a failure.
            with composite.open("a") as stream:
                stream.write("\nnamespace ker::mod::mm::huge::detail {\n"
                             "auto publish_prepared_huge_batch(paging::PageTable*, const ker::syscall::vmem::SharedVmemPublicationGuard&, PreparedHugeMapBatch&) noexcept -> Error {\n"
                             "ADD_FAILURE() << \"current huge-map adapter called in an unselected legacy profile\"; return Error::Unsupported;\n}\n}\n")
        if policy_fixture is not None:
            composite.write_text(policy_fixture.compose(composite.read_text()))
        segments = {
            "resolver": resolver,
            "private": private,
            "commit": commit,
            "index": index,
            "user_pin": user_pin,
            "exclusive": exclusive,
            "copier": copier,
            "epoch": epoch,
            "adapters": adapters,
            **context_map,
            **({"public_huge_mapper": coordinator.batch.mapper, "public_huge_adapter": coordinator.batch.adapter} if options.public_huge_map_only else {}),
            **({"mremap_policy": policy_fixture.manifest_text} if policy_fixture is not None else {}),
            **({"batch_mapper": batch_fixture.mapper, "batch_adapter": batch_fixture.adapter} if batch_fixture is not None else {}),
            **({"current_huge_map_declaration": batch_fixture.current_declaration,
                "current_huge_map_methods": batch_fixture.current_methods} if options.current_huge_map_only else {}),
        }
        if coordinator is not None:
            for path in context.rglob("*"):
                if path.is_file():
                    retained = output / "generated-context" / path.relative_to(context)
                    retained.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, retained)
            (output / "coordinator-source-map.json").write_text(
                json.dumps(
                    {
                        "actual_components": coordinator.extractions,
                        "inputs": [pin(path) for path in coordinator.inputs],
                        "limits": [
                            "Actual Normal owner bodies and full original-exclusive copier in one Host fixture; no WOS runtime/TLB/backend qualification.",
                            "Actual family/binding constructors/counting/index/finish; strict typed fixture seed and queued callback Host port, not permanent-executor qualification.",
                            "Existing54 pin vectors preserve their binding-match refusal port; fork binding attach/query/overlap/detach use real index bodies.",
                            "Host-only metadata allocation ledger; actual target configured/O0 instrumentation requires separate evidence.",
                        ],
                    },
                    indent=2,
                )
                + "\n"
            )
        shutil.copy2(composite, output / "actual-source.cpp")
        (output / "extraction-map.json").write_text(
            json.dumps(
                {
                    "virt": pin(virt),
                    "header": pin(header),
                    "composite": pin(composite),
                    "segments": [
                        {"name": name, "bytes": len(text.encode()), "sha256": hashlib.sha256(text.encode()).hexdigest()}
                        for name, text in segments.items()
                    ],
                },
                indent=2,
            )
            + "\n"
        )

        baseline_cases = CppText(composite.read_text()).gtest_cases()
        baseline_names = {case["name"] for case in baseline_cases}
        expected_count = EXPECTED_TESTS + (policy_fixture.test_count if policy_fixture is not None else 0) + (coordinator.test_count if coordinator is not None else 0) + (batch_fixture.test_count if batch_fixture is not None else 0)
        if len(baseline_names) != expected_count:
            raise RuntimeError("complete lexical GTest inventory differs from required baseline")
        (output / "baseline-case-inventory.json").write_text(json.dumps(baseline_cases, indent=2) + "\n")
        sources = [module, factory, composite] + ([public_source] if coordinator is not None else [])
        dependencies: set[Path] = set()
        for index, path in enumerate(sources):
            depfile = scratch / f"deps-{index}.d"
            run(f"deps-{index}", flags + ["-M", "-MF", depfile, "-MT", "unit", path])
            words = shlex.split(depfile.read_text().replace("\\\n", " ").split(":", 1)[1])
            dependencies.update(Path(word).resolve() for word in words)
        emitted = [pin(path) for path in sorted((dependencies - {composite}) | set(libraries), key=str)]
        objects = []
        for index, path in enumerate(sources):
            obj = scratch / f"unit-{index}.o"
            run(f"compile-{index}", flags + ["-c", path, "-o", obj])
            objects.append(obj)

        def link(label: str, inputs: list[Path]) -> Path:
            binary = scratch / label
            run(f"link-{label}", [flags[0], "-fsanitize=address,undefined", *inputs, *libraries, "-o", binary])
            return binary

        binary = link("current", objects)
        run("run-current", [binary, f"--gtest_output=json:{output / 'current.json'}"])
        tests = json.loads((output / "current.json").read_text())
        if tests["failures"] != 0 or tests["tests"] != expected_count:
            raise RuntimeError("actual range/legacy suite did not pass")
        if baseline_gtest_names(tests) != baseline_names:
            raise RuntimeError("actual complete baseline differs from lexical GTest names")

        control_evidence_bytes = 0
        def mutant(label: str, text: str, test: str, replace_index: int = 2) -> None:
            nonlocal control_evidence_bytes
            selected_cases = control_case_names(test, baseline_names)
            selection = None
            original_source = None
            if replace_index == 2:
                original = str(text)
                text, selection = select_control_source(original, test, baseline_names)
                proof_bytes = len(original.encode()) + len(json.dumps(selection).encode())
                if control_evidence_bytes + proof_bytes > SCRATCH_BYTES:
                    raise RuntimeError("bounded control source evidence exhausted")
                retained = output / f"{label}-full.cpp"
                with retained.open("x") as stream:
                    stream.write(original)
                control_evidence_bytes += proof_bytes
                original_source = pin(retained)
            path = scratch / f"{label}.cpp"
            path.write_text(text)
            obj = scratch / f"{label}.o"
            run(f"compile-{label}", flags + ["-c", path, "-o", obj])
            binary = link(label, [*objects[:replace_index], obj, *objects[replace_index + 1:]])
            controls.append({"label": label, "source": pin(path), "object": pin(obj), "binary": pin(binary),
                             "selected_cases": sorted(selected_cases), "selection": selection, "original_source": original_source})
            run(f"run-{label}", [binary, f"--gtest_filter={test}"], expected_success=False)
            binary.unlink()
            obj.unlink()
            path.unlink()

        actual = CppText(composite.read_text())
        if policy_fixture is not None:
            for label, bad, test in policy_fixture.controls(composite.read_text()):
                mutant(label, bad, test)
        elif options.current_huge_map_only:
            # Reuse the canonical owned command path; only module0 is replaced.
            for label, text, test in batch_fixture.module_controls(CppText(module.read_text())):
                control_case_names(test, baseline_names)
                path = scratch / f"{label}.cpp"
                path.write_text(text)
                obj = scratch / f"{label}.o"
                run(f"compile-{label}", flags + ["-iquote", module.parent, "-c", path, "-o", obj])
                binary = link(label, [obj, *objects[1:]])
                controls.append({"label": label, "source": pin(path), "object": pin(obj), "binary": pin(binary),
                                 "selected_cases": sorted(control_case_names(test, baseline_names))})
                run(f"run-{label}", [binary, f"--gtest_filter={test}"], expected_success=False)
                binary.unlink()
                obj.unlink()
                path.unlink()
        elif batch_fixture is not None:
            for label, bad, test in batch_fixture.controls(composite.read_text()):
                mutant(label, bad, test)
        elif coordinator is not None:
            for label, bad, test in coordinator.controls(composite.read_text()):
                mutant(label, bad, test)
            if not (options.anonymous_remove_only or options.mixed_geometry_only or options.mixed_protect_only or options.anonymous_fixed_only or options.file_fixed_only or options.public_mutation_only or options.public_huge_map_only or options.shm_occupancy_only):
                for label, bad, test in coordinator.public.controls(public_source.read_text()):
                    mutant(label, bad, test, replace_index=3)
        elif options.fork_contract_only:
            owner_check = "if (!exclusive.owns(src)) return false;"
            if composite.read_text().count(owner_check) != 1:
                raise RuntimeError("held-owner control changed")
            mutant("fork-lost-owner", composite.read_text().replace(owner_check, "if (false) return false;"),
                   "ManagedFork.HeldWalkerRejectsDifferentOriginalOwnerBeforeWrites")
            owner_entry = "virt::UserPagemapExclusiveScope source_usercopy_scope(source);"
            if composite.read_text().count(owner_entry) != 1:
                raise RuntimeError("fork reader-order control changed")
            mutant("fork-nested-reader", composite.read_text().replace(owner_entry, owner_entry +
                   "\n    virt::ManagedRootReader nested_reader(source);"),
                   "ManagedFork.FullZeroProofCopiesWithOneExclusiveAndNoReader")
        else:
            needle = (
                "if (level != 2 ||\n                managed_huge_registry.lookup_mapped(key, "
                "address & ~(huge::LEAF_BYTES - 1), raw, result.managed) != huge::Error::None)"
            )
            if actual.count(needle) != 1:
                raise RuntimeError("unknown-PS mutation boundary changed")
            mutant(
                "unknown-ps-authority",
                actual.replace(needle, "if (level != 2)"),
                "ManagedResolver.UnknownTwoMegabyteOneGigabyteAndAbaNeverGrantProvenance",
            )

            needle = "current.mark_dirty();  // CURRENT PS or PML1 slot, never a saved PDE."
            if actual.count(needle) != 1:
                raise RuntimeError("dirty mutation boundary changed")
            replacement = (
                "MutableUserEntrySlot stale; if (managed_pml2_slot_locked(pin.pagemap,pin.user_page,stale)"
                "==Pml2Walk::Slot) stale.mark_dirty();"
            )
            mutant(
                "stale-pde-dirty",
                actual.replace(needle, replacement),
                "ManagedResolver.ExactCurrentLeafDirtyCommitSurvivesPureSplitWithNoRefChange",
            )

            needle = "    flush_pagemap_after_update(root, 0, true);\n    for (uint16_t i = 0; i < storage->descriptor_count; ++i) {"
            if actual.count(needle) != 1:
                raise RuntimeError("ACK mutation boundary changed")
            early = "    for (uint16_t i=0;i<storage->descriptor_count;++i) (void)managed_huge_registry.complete_split_after_ack(storage->descriptors[i].handle);\n"
            bad = actual.replace(needle, early + needle)
            needle = "managed_huge_registry.complete_split_after_ack(d.handle) != huge::Error::None"
            if bad.count(needle) != 1:
                raise RuntimeError("post-ACK mutation boundary changed")
            mutant(
                "capacity-before-ack",
                bad.replace(needle, "false"),
                "ManagedResolver.ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck",
            )

            for label, needle, replacement in (
                (
                    "try-blocks",
                    "acquired_(g_shared_vmem_publication_lock.try_lock())",
                    "acquired_((g_shared_vmem_publication_lock.lock(), true))",
                ),
                ("try-forgets-owner", "acquired_(g_shared_vmem_publication_lock.try_lock())", "acquired_(false)"),
                (
                    "failed-try-unlocks",
                    "if (acquired_) g_shared_vmem_publication_lock.unlock();",
                    "g_shared_vmem_publication_lock.unlock();",
                ),
            ):
                if actual.count(needle) != 1:
                    raise RuntimeError(f"actual try-guard mutation boundary changed: {label}")
                mutant(
                    label,
                    actual.replace(needle, replacement),
                    "ManagedRange.RealTryGuardContentionNestedFailureAndUnlockOwnership",
                )
            needle = "else if (control->ps_geometry_epoch == survey.stamp().epoch) error = Error::None;"
            if actual.count(needle) != 1:
                raise RuntimeError("zero proof mutation boundary changed")
            mutant(
                "zero-ignores-epoch",
                actual.replace(needle, "else error = Error::None;"),
                "ManagedRange.HideRevealAndZeroThenPublicationCannotReuseAnOldProof",
            )
            needle = "        // All512 real tracker invocations precede this exact leaf's cap credit."
            credit = "managed_huge_registry.complete_split_after_ack(d.handle) != huge::Error::None"
            if actual.count(needle) != 1 or actual.count(credit) != 1:
                raise RuntimeError("tracker credit mutation boundary changed")
            early = "        (void)managed_huge_registry.complete_split_after_ack(d.handle);\n"
            marker = "        for (uint64_t page = 0; page < huge::LEAF_PAGES; ++page)\n            owned_frame_track_fresh_normal_mapping"
            if actual.count(marker) != 1:
                raise RuntimeError("tracker loop mutation boundary changed")
            mutant(
                "credit-before-tracking",
                actual.replace(marker, early + marker).replace(credit, "false"),
                "ManagedResolver.ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck",
            )

            needle = "if (!exclusive.owns(root)) return huge::Error::Invalid;"
            if actual.count(needle) != 1:
                raise RuntimeError("typed exclusive mutation boundary changed")
            mutant(
                "nested-exclusive-reader",
                actual.replace(
                    needle, needle + " ManagedRootReader nested(root); if (!nested.valid()) return huge::Error::Again;"
                ),
                "ManagedRange.HeldOriginalExclusiveCommitsWithoutAcquiringAReader",
            )

            def module_mutant(label: str, text: str, test: str) -> None:
                control_case_names(test, baseline_names)
                path = scratch / f"{label}.cpp"
                path.write_text(text)
                obj = scratch / f"{label}.o"
                run(f"compile-{label}", flags + ["-iquote", module.parent, "-c", path, "-o", obj])
                binary = link(label, [obj, objects[1], objects[2]])
                controls.append({"label": label, "source": pin(path), "object": pin(obj), "binary": pin(binary)})
                run(f"run-{label}", [binary, f"--gtest_filter={test}"], expected_success=False)
                binary.unlink()
                obj.unlink()
                path.unlink()

            needle = "g_shared_vmem_publication_owner.store(this, std::memory_order_release);"
            if actual.count(needle) != 1:
                raise RuntimeError("actual blocking held-state boundary changed")
            mutant("blocking-forgets-held", actual.replace(needle, "g_shared_vmem_publication_owner.store(nullptr, std::memory_order_release);"),
                   "ManagedRange.BlockingBorrowUsesRealGuardAfterOrdinaryAcquireBeforeLegacyInterval")

            module_text = CppText(module.read_text())
            needle = ": subject_(&target), preparation_(&preparation), blocking_publication_(&publication) { if (!publication.acquired() || !range_current_context(target, preparation, false)) return;"
            if module_text.count(needle) != 1:
                raise RuntimeError("actual blocking context boundary changed")
            module_mutant("blocking-forgets-context", module_text.replace(needle, ": subject_(&target), preparation_(&preparation), blocking_publication_(&publication) { if (!publication.acquired()) return;"),
                          "ManagedRange.BlockingBorrowRechecksActualContextAndRejectsUnpreparedProof")
            needle = "error = pass(target, preparation, unpublished, final, count.count != 0 ? RangeSurveyMode::Verify : RangeSurveyMode::Count, storage, witness, RangePreparePass::Verify, static_cast<uint8_t>(attempt + 1), count.count);"
            if module_text.count(needle) != 1:
                raise RuntimeError("independent final pass mutation boundary changed")
            module_mutant(
                "omitted-final-pass",
                module_text.replace(needle, "{ error = Error::None; final.count = count.count; }"),
                "ManagedRange.IndependentFinalZeroPassDetectsPublicationAndRebuildsAllDescriptors",
            )

            module_text = CppText(module.read_text())
            for label, needle, replacement, test in (
                ("witness-missing", "*witness = {cursor != nullptr ? cursor->next : 0, cursor != nullptr ? cursor->count : uint16_t{0}, expected_count, error, site, pass, attempt, true, cursor != nullptr && cursor->epoch_changed, false};",
                 "*witness = {};", "ManagedRange.WitnessSeparatesPublicationLeaseReaderAndRootRefusals"),
                ("witness-retains-retry", "if (witness != nullptr) *witness = {};", "if (false) *witness = {};",
                 "ManagedRange.WitnessEpochRetryKeepsOnlyTheLastActualFailureOrClearsOnSuccess"),
            ):
                if label == "witness-retains-retry":
                    # Omit ALL genuine clears in the module, preserving every
                    # error/owner action; a successful retry must reject it.
                    if module_text.count(needle) != 4:
                        raise RuntimeError("actual witness clear boundaries changed")
                elif module_text.count(needle) != 1:
                    raise RuntimeError("actual witness copy boundary changed")
                module_mutant(label, module_text.replace(needle, replacement), test)

            text = module.read_text()
            needle = "(void)owner.span_.release();"
            if text.count(needle) != 1:
                raise RuntimeError("data-transfer mutation boundary changed")
            bad_module = scratch / "drop-adopted-data.cpp"
            control_case_names("ManagedResolver.ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck", baseline_names)
            bad_module.write_text(text.replace(needle, "owner.span_.reset();"))
            obj = scratch / "drop-adopted-data.o"
            run("compile-drop-adopted-data", flags + ["-iquote", module.parent, "-c", bad_module, "-o", obj])
            binary = link("drop-adopted-data", [obj, objects[1], objects[2]])
            run(
                "run-drop-adopted-data",
                [
                    binary,
                    "--gtest_filter=ManagedResolver.ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck",
                ],
                expected_success=False,
            )

        check_interrupted()
    except BaseException as error:
        errors.append(str(error))
    finally:
        try:
            selected_after = [pin(Path(row["path"])) for row in selected]
            emitted_after = [pin(Path(row["path"])) for row in emitted]
            if selected_after != selected or emitted_after != emitted:
                errors.append("actual source/emitted dependency changed")
        except Exception as error:
            selected_after, emitted_after = [], []
            errors.append(f"input resampling: {error}")
        cleanup_deadline = min(started + overall_seconds, time.monotonic() + 1)
        for group in list(groups):
            try:
                os.killpg(group, 0)
            except ProcessLookupError:
                groups.remove(group)
                continue
            except OSError as error:
                errors.append(f"owned group presence unknown: {group}: {error}")
                continue
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError as error:
                errors.append(f"owned group cleanup failed: {group}: {error}")
            errors.append(f"owned group required cleanup: {group}")
        for process in processes:
            try:
                process.wait(timeout=max(0, cleanup_deadline - time.monotonic()))
            except BaseException as error:
                errors.append(f"owned leader reap unproven: {process.pid}: {error}")
        while groups:
            try:
                for group in list(groups):
                    try:
                        os.killpg(group, 0)
                    except ProcessLookupError:
                        groups.remove(group)
                remaining = cleanup_deadline - time.monotonic()
                if not groups or remaining <= 0:
                    break
                time.sleep(min(0.01, remaining))
            except OSError as error:
                errors.append(f"owned group final presence unknown: {error}")
                break
        try:
            removed = allocated_bytes()
            current_scratch = scratch.lstat()
            scratch_owned = (not scratch.is_symlink() and
                             (current_scratch.st_dev, current_scratch.st_ino, current_scratch.st_uid) ==
                             (scratch_identity.st_dev, scratch_identity.st_ino, os.getuid()))
        except OSError as error:
            removed, scratch_owned = 0, False
            errors.append(f"scratch identity/size unproven: {error}")
        all_reaped = all(process.returncode is not None for process in processes)
        if not groups and all_reaped and scratch_owned:
            try:
                shutil.rmtree(scratch)
            except OSError as error:
                errors.append(f"scratch removal failed: {error}")
        else:
            errors.append(f"unproven owned group/leader/scratch identity; preserving {scratch}")
        for number, handler in saved_signals.items():
            signal.signal(number, handler)
        if pending_signal is not None and not any("received signal" in error for error in errors):
            errors.append(f"received signal {pending_signal}")
        result = {
            "qualification": (
                "PASS_ACTUAL_MANAGED_HUGE_HOST_ONLY" if not errors else "FAIL_ACTUAL_MANAGED_HUGE_HOST_ONLY"
            ),
            "errors": errors,
            "seconds": time.monotonic() - started,
            "commands": commands,
            "compiled_controls": controls,
            "input_pins": selected,
            "input_pins_after": selected_after,
            "emitted_inputs_before": emitted,
            "emitted_inputs_after": emitted_after,
            "emitted_input_count": len(emitted),
            "scratch_removed": not scratch.exists(),
            "scratch_allocated_bytes_removed": removed,
            "limits": [
                "Actual C++ source with explicit Host root/PUB/lock/frame/table/flush endpoints.",
                "No Native, PMM/buddy, actual TLB ACK, CPL3/SMP, pressure or goal qualification.",
                "ASan/UBSan enabled; LSan disabled for the configured Host environment.",
            ],
        }
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(
            json.dumps(
                {"result": str(output / "result.json"), "qualification": result["qualification"], "errors": errors}
            )
        )
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
