#!/usr/bin/env python3
"""Run actual range admission, placement, loader probe and cooperation bodies.

Atomic storage retains std::atomic operations/orders. Synchronous hooks inject
legal teardown interleavings; page-table storage, entropy and ELF metadata are
disclosed Host endpoints. This does not execute a native exec/IRQ/teardown path.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[3]


def block(source: str, pattern: str) -> str:
    matches = list(re.finditer(pattern, source, re.M))
    if len(matches) != 1:
        raise ValueError(f"nonunique source definition {pattern}: {len(matches)}")
    first = matches[0].start()
    cursor = source.index("{", first) + 1
    depth = 1
    while depth:
        depth += (source[cursor] == "{") - (source[cursor] == "}")
        cursor += 1
    return source[first:cursor]


def function(source: str, name: str) -> str:
    return block(source, rf"^(?:\[\[nodiscard\]\] )?(?:constexpr )?(?:auto|void) {name}\([^;{{]*\{{")


def once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError(f"nonunique mutation operand: {old}")
    return source.replace(old, new)


def pin(path: Path) -> dict:
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    fields = lambda row: (row.st_dev, row.st_ino, row.st_size, row.st_mtime_ns, row.st_ctime_ns)
    if fields(before) != fields(after):
        raise RuntimeError(f"source changed during read: {path}")
    return dict(path=str(path), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                device=after.st_dev, inode=after.st_ino, mtime_ns=after.st_mtime_ns, ctime_ns=after.st_ctime_ns)


def generate(virt: str, virt_header: str, layout: str, layout_header: str,
             loader: str, loader_header: str, fixture: str) -> dict[str, tuple[str, str | None]]:
    gate_start = virt.index("constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT =")
    gate_end = virt.index("user_pagemap_access_gates{};", gate_start) + len("user_pagemap_access_gates{};")
    # Transport seam only: production bodies below are inserted byte-for-byte.
    gate = virt[gate_start:gate_end].replace("std::atomic<", "host::Atomic<")
    gate += "\n" + function(virt, "user_pagemap_access_gate_index")
    admission = "template <typename WaitForTeardown>\n" + function(virt, "admit_user_pagemap_reader")
    admission += "\n" + function(virt, "release_user_pagemap_access")
    status = re.search(r"enum class UserVirtualRangeStatus[^;]+;", virt_header)
    if status is None:
        raise ValueError("missing actual typed range enum")
    query = status.group() + "\n"
    query += "template <bool StopAtFirst = false>\n" + function(virt, "count_virtual_bytes_in_table")
    query += "\n" + function(virt, "query_user_virtual_range")
    query += "\n" + function(virt, "is_user_virtual_range_free")
    types_start = layout_header.index("enum class ImageRole")
    types_end = layout_header.index("// True only when", types_start)
    layout_types = layout_header[types_start:types_end]
    alias = re.search(r"using ImageRangeQuery =[^;]+;", layout_header)
    if alias is None:
        raise ValueError("missing actual query callback alias")
    layout_types += alias.group()
    candidate = block(layout, r"^struct CandidateSet \{") + ";"
    limit = re.search(r"^constexpr uint64_t MAX_RANDOMIZED_COLLISION_PROBES = [^;]+;", layout, re.M)
    if limit is None:
        raise ValueError("missing actual collision bound")
    placement = limit.group() + "\n" + candidate + "\n"
    placement += "\n".join(function(layout, name) for name in
                            ("is_power_of_two", "align_up_checked", "align_down", "candidate_at", "choose_image_base_with_query"))
    token = block(loader_header, r"^class ElfLayoutCooperation \{") + ";"
    probe = block(loader, r"^struct ElfRangeProbe \{") + ";"
    image = "\n".join(function(loader, name) for name in
                      ("checked_add", "applied_image_range_is_valid", "select_image_load_base"))
    chunks = dict(ACTUAL_GATE=gate, ACTUAL_ADMISSION=admission, ACTUAL_QUERY=query,
                  ACTUAL_LAYOUT_TYPES=layout_types, ACTUAL_LAYOUT=placement,
                  ACTUAL_COOPERATION=token, ACTUAL_PROBE=probe, ACTUAL_IMAGE_SELECTION=image)

    def assemble(overrides: dict[str, str]) -> str:
        result = fixture
        for key, text in (chunks | overrides).items():
            result = once(result, "// " + key, text)
        return result

    variants = {"actual": (assemble({}), None)}
    variants["busy_as_occupied"] = (assemble({"ACTUAL_QUERY": once(query,
        ": UserVirtualRangeStatus::GateBusy;", ": UserVirtualRangeStatus::Occupied;")}), "foreign-busy")
    variants["missing_reader_rollback"] = (assemble({"ACTUAL_ADMISSION": once(admission,
        "        gate.readers.fetch_sub(1, std::memory_order_acq_rel);", "        // missing raced-reader rollback")}), "reader-release")
    variants["capacity_as_busy"] = (assemble({"ACTUAL_QUERY": once(query,
        "? UserVirtualRangeStatus::ReaderUnavailable", "? UserVirtualRangeStatus::GateBusy")}), "capacity-terminal")
    variants["invalid_walk_as_free"] = (assemble({"ACTUAL_QUERY": once(query,
        "if (!VALID) return UserVirtualRangeStatus::Invalid;", "if (!VALID) return UserVirtualRangeStatus::Free;")}), "invalid-walk")
    variants["terminal_advances_cursor"] = (assemble({"ACTUAL_LAYOUT": once(placement,
        "if (STATUS != virt::UserVirtualRangeStatus::Occupied) return false;",
        "if (STATUS == virt::UserVirtualRangeStatus::Free) return false;")}), "selector-terminal")
    variants["entropy_redraw"] = (assemble({"ACTUAL_LAYOUT": once(placement,
        "        uint64_t candidate_bias = 0;",
        "        if (attempt != 0) (void)random::entropy::uniform_u64(CANDIDATES.count, index);\n        uint64_t candidate_bias = 0;")}), "entropy-once")
    variants["cooperation_certifies_free"] = (assemble({"ACTUAL_PROBE": once(probe,
        "                return probe.status;\n            }\n        }",
        "                return probe.status;\n            }\n            probe.status = mod::mm::virt::UserVirtualRangeStatus::Free;\n            return probe.status;\n        }")}), "same-range-rerun")
    variants["third_cooperation"] = (assemble({"ACTUAL_COOPERATION": once(token,
        "uint8_t remaining_{2};", "uint8_t remaining_{3};")}), "shared-budget")
    variants["decrement_after_callback"] = (assemble({"ACTUAL_COOPERATION": once(token,
        "        --remaining_;\n        return cooperate_(context_, root);",
        "        bool const result = cooperate_(context_, root);\n        --remaining_;\n        return result;")}), "token-decrement-before-call")
    variants["busy_wrong_errno"] = (assemble({"ACTUAL_PROBE": once(probe,
        "GateBusy ? -EAGAIN : 0", "GateBusy ? 0 : 0")}), "busy-eagain")
    return variants


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--cxx", default="clang++")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    root = args.source_root.resolve()
    mm = root / "modules/kern/src/platform/mm"
    loader = root / "modules/kern/src/platform/loader"
    fixture = Path(__file__).with_name("elf_layout_contention_cases.inc")
    worker = ROOT / "scripts/test/owned_command.py"
    spec = importlib.util.spec_from_file_location("elf_layout_owned_command", worker)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    # Preserve the multicall C++ driver's argv[0], while pinning target bytes.
    compiler = Path(shutil.which(args.cxx) or args.cxx).absolute()
    source_paths = [mm / "virt.opt.cpp", mm / "virt.hpp", mm / "user_layout.cpp", mm / "user_layout.hpp",
                    loader / "elf_loader.cpp", loader / "elf_loader.hpp"]
    inputs = [Path(__file__).resolve(), fixture, worker, compiler, *source_paths,
              mm / "paging.hpp", root / "modules/defines/defines.hpp"]
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="wos-elf-layout-result-"))
    if args.output:
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    started = time.monotonic()
    deadline = started + 175
    report = dict(kind="HOST_ELF_LAYOUT_CONTENTION", errors=[], commands=[], runtime_qualified=False)
    before = [pin(path) for path in inputs]
    environment = dict(os.environ, TMPDIR=str(scratch), ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                       UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1")
    cancellation_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous_handlers = {number: signal.getsignal(number) for number in cancellation_signals}
    original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, ())

    def cancelled(number, _frame):
        raise KeyboardInterrupt(f"test cancelled by signal {number}")

    for number in cancellation_signals:
        signal.signal(number, cancelled)
    safe_to_prune = True
    try:
        variants = generate(*(path.read_text() for path in source_paths), fixture.read_text())

        def run(label, command, expected, marker=None):
            nonlocal safe_to_prune
            receipt = output / (label + "-owned.json")
            log_path = output / (label + ".log")
            with log_path.open("xb") as log:
                sys.stdout.flush()
                sys.stderr.flush()
                saved_stdout, saved_stderr = os.dup(1), os.dup(2)
                try:
                    safe_to_prune = False
                    os.dup2(log.fileno(), 1)
                    os.dup2(log.fileno(), 2)
                    saved_environment = dict(os.environ)
                    try:
                        os.environ.update(environment)
                        proof, code = owned.run(list(map(str, command)), deadline - 5, receipt)
                    finally:
                        os.environ.clear()
                        os.environ.update(saved_environment)
                    safe_to_prune = (proof["leader_reaped"] and proof["owned_session_absent"]
                                     and proof["adopted_children_absent"] and not proof["errors"])
                finally:
                    os.dup2(saved_stdout, 1)
                    os.dup2(saved_stderr, 2)
                    os.close(saved_stdout)
                    os.close(saved_stderr)
            closed = (safe_to_prune and not proof["timed_out"] and not proof["cancelled"])
            report["commands"].append(dict(label=label, expected_exit=expected, exit=code, closed=closed, receipt=pin(receipt)))
            if code != expected or proof["native_exit"] != expected or not closed:
                raise AssertionError(f"{label}: exit={code}, expected={expected}; see {output}")
            lines = log_path.read_text().splitlines()
            if label == "actual":
                if sum(bool(re.fullmatch(r"ELF_LAYOUT_CONTENTION_PASS checks=[1-9]\d*", line)) for line in lines) != 1:
                    raise AssertionError("actual source omitted its sole complete PASS marker")
            elif marker is not None and lines.count("ELF_LAYOUT_INVARIANT:" + marker) != 1:
                raise AssertionError(f"{label}: unrelated abort cannot satisfy the intended invariant")

        binary = scratch / "check"
        for name, (code, marker) in variants.items():
            cpp = scratch / (name + ".cpp")
            cpp.write_text(code)
            run("compile-" + name, [compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra",
                "-fno-omit-frame-pointer", "-fsanitize=address,undefined", "-fno-sanitize-recover=all",
                "-I" + str(root / "modules/kern/src"), "-I" + str(root / "modules/kern/include"),
                "-I" + str(root / "modules"), cpp, "-o", binary], 0)
            run(name, [binary], 0 if name == "actual" else -signal.SIGABRT, marker)
            # The public owner proved this exact binary's whole forest absent.
            binary.unlink()
    except BaseException as error:
        report["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        signal.pthread_sigmask(signal.SIG_BLOCK, cancellation_signals)
        if safe_to_prune:
            shutil.rmtree(scratch)
        after = [pin(path) for path in inputs]
        report.update(input_pins=before, input_pins_after=after, sources_unchanged=before == after,
                      seconds=time.monotonic() - started, scratch_removed=not scratch.exists())
        if before != after:
            report["errors"].append("source inputs changed during test")
        report["result"] = "PASS" if not report["errors"] else "FAIL"
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)
        signal.pthread_sigmask(signal.SIG_SETMASK, original_mask)
    print(json.dumps(dict(result=report["result"], errors=report["errors"], output=str(output), seconds=report["seconds"])))
    return int(bool(report["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
