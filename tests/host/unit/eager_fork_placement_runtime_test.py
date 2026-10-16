#!/usr/bin/env python3
"""Execute actual eager-fork walker and NUMA factory with disclosed Host endpoints.

Physical/HHDM/ref/TLB/exclusive/root registration are controlled seams. The
full selected walker, validation wrappers and factory are compiled from source.
No WOS scheduler, hardware, actual PMM pressure or public huge/fork proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import tempfile
import time


def function(source: str, signature: str) -> str:
    if source.count(signature) != 1:
        raise RuntimeError(f"nonunique actual function: {signature}")
    start = source.index(signature)
    opening = source.index("{", start)
    depth = 1
    cursor = opening + 1
    while depth:
        if source[cursor] == "{":
            depth += 1
        elif source[cursor] == "}":
            depth -= 1
        cursor += 1
    return source[start:cursor]


def pin(path: Path) -> dict:
    path = path.resolve()
    first = path.stat()
    data = path.read_bytes()
    last = path.stat()
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if identity(first) != identity(last):
        raise RuntimeError(f"input changed during read: {path}")
    return {"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
            "device": last.st_dev, "inode": last.st_ino, "mtime_ns": last.st_mtime_ns, "ctime_ns": last.st_ctime_ns}



def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--virt-source", type=Path, help="explicit staged source; headers remain from source-root")
    parser.add_argument("--factory-source", type=Path, help="explicit staged factory; includes remain from source-root")
    parser.add_argument("--cxx", default="g++", help="Host C++ compiler; ignores WOS target CXX")
    parser.add_argument("--output", type=Path, help="fresh result directory; owned scratch is always removed")
    parser.add_argument("--negative-controls", action="store_true", help="compile two genuine source mutations")
    args = parser.parse_args()
    root = args.source_root.resolve()
    source_path = (args.virt_source or root / "modules/kern/src/platform/mm/virt.opt.cpp").resolve()
    header_path = root / "modules/kern/src/platform/mm/virt.hpp"
    factory_path = (args.factory_source or root / "modules/kern/src/platform/mm/numa_alloc.cpp").resolve()
    cases_path = Path(__file__).with_name("eager_fork_placement_cases.inc")
    compiler = Path(shutil.which(args.cxx) or args.cxx).resolve()
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="eager-fork-result-"))
    if args.output:
        output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    started = time.monotonic()
    commands, errors, groups = [], [], {}
    selected = [Path(__file__), source_path, header_path, factory_path, cases_path, compiler]
    before, emitted = [], []
    environment = dict(os.environ, TMPDIR=str(scratch), ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                       UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1")
    cancellation_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous_handlers = {number: signal.getsignal(number) for number in cancellation_signals}
    launching, deferred_signal = False, None

    def cancelled(number, _frame):
        nonlocal deferred_signal
        if launching:
            deferred_signal = number
            return
        raise KeyboardInterrupt(f"received signal {number}")

    def retire_group(process) -> bool:
        try:
            os.killpg(process.pid, 0)
            return False
        except ProcessLookupError:
            # Do not retain an absent PGID: it can be reused by another job.
            groups.pop(process.pid, None)
            return True

    def stop_group(process) -> None:
        if process.pid not in groups:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        if not retire_group(process):
            raise RuntimeError(f"owned group {process.pid} remains after kill/reap")

    def run(label: str, command: list, expected: int = 0) -> None:
        nonlocal launching, deferred_signal
        remaining = 120 - (time.monotonic() - started)
        if remaining <= 0 or sum(p.stat().st_blocks * 512 for p in scratch.rglob("*") if p.is_file()) > 128 * 1024 * 1024:
            raise RuntimeError("time/scratch budget exhausted")
        log = output / (label + ".log")
        when = time.monotonic()
        process = None
        with log.open("wb") as stream:
            try:
                # Publish ownership before a pending cancellation can run.
                # Defer the handler rather than leaking a blocked signal mask
                # into the compiler or executable child.
                launching = True
                try:
                    process = subprocess.Popen([str(x) for x in command], cwd=root, env=environment,
                                               stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                    groups[process.pid] = process
                finally:
                    launching = False
                if deferred_signal is not None:
                    number, deferred_signal = deferred_signal, None
                    cancelled(number, None)
                code = process.wait(timeout=min(35, remaining))
            except BaseException:
                if process is not None:
                    stop_group(process)
                raise
        absent = retire_group(process)
        commands.append({"label": label, "argv": [str(x) for x in command], "exit": code,
                         "expected_exit": expected, "seconds": time.monotonic() - when,
                         "owned_group_absent": absent, "leader_reaped": True, "log": pin(log)})
        if code != expected or not absent:
            raise RuntimeError(f"{label}: exit {code}, expected {expected}")

    try:
        for number in cancellation_signals:
            signal.signal(number, cancelled)
        before = [pin(path) for path in selected]
        source = source_path.read_text()
        signatures = ["auto valid_fork_placement(", "auto deep_copy_user_pagemap_cow_impl(", "auto fork_eager_copy_requested()",
                      "auto deep_copy_user_pagemap_cow(PageTable* src, PageTable* dst) -> bool",
                      "auto deep_copy_user_pagemap_cow(PageTable* src, PageTable* dst, numa::AllocationContext&",
                      "auto selftest_deep_copy_user_pagemap_eager("]
        bodies = "\n".join(function(source, name) for name in signatures)
        support = cases_path.read_text()
        code = support.replace("// ACTUAL_EXTRACTED_COPIER_INSERTION", bodies)
        code = code.replace("// ACTUAL_FACTORY_INSERTION", factory_path.read_text())
        variants = [("actual", code, 0)]
        if args.negative_controls:
            mutations = [
                ("ambient-bypass", "if (placement != nullptr) {\n                            // Both objects", "if (false) {\n                            // Both objects"),
                ("lost-va-index", "placement->virtual_page_index = VADDR / paging::PAGE_SIZE;", "placement->virtual_page_index = 0;"),
            ]
            for label, original, replacement in mutations:
                if code.count(original) != 1:
                    raise RuntimeError(f"nonunique genuine mutation: {label}")
                variants.append((label, code.replace(original, replacement), 1))
        for label, body, expected in variants:
            cpp, binary, dep = (scratch / (label + suffix) for suffix in (".cpp", ".exe", ".d"))
            cpp.write_text(body)
            flags = [compiler, "-std=c++23", "-O1", "-g", "-fno-strict-aliasing", "-fno-pie", "-no-pie",
                     "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I" + str(root / "modules/kern/src"),
                     "-I" + str(root / "modules"), "-iquote", str(factory_path.parent), "-iquote", str(root / "modules/kern/src/platform/mm"), "-DWOS_SELFTEST=1"]
            run("compile-" + label, flags + ["-MD", "-MF", dep, cpp, "-o", binary])
            dependencies = [Path(x).resolve() for x in shlex.split(dep.read_text().replace("\\\n", " ").split(":", 1)[1])]
            direct = [pin(path) for path in dependencies if path != cpp]
            if not emitted:
                emitted = direct
            elif direct != emitted:
                raise RuntimeError("variant emitted dependency closure differs")
            run("execute-" + label, [binary], expected)
    except BaseException as error:
        errors.append(f"{type(error).__name__}: {error}")
    finally:
        # A second termination signal must not abandon an in-progress cleanup.
        for number in cancellation_signals:
            signal.signal(number, signal.SIG_IGN)
        try:
            for process in list(groups.values()):
                try:
                    stop_group(process)
                except Exception as error:
                    errors.append(f"cleanup: {error}")
            after, emitted_after = [], []
            try:
                after = [pin(Path(row["path"])) for row in before]
                emitted_after = [pin(Path(row["path"])) for row in emitted]
                if before != after or emitted != emitted_after:
                    errors.append("source/dependency changed")
            except Exception as error:
                errors.append(f"input recheck: {error}")
            scratch_bytes = sum(p.stat().st_blocks * 512 for p in scratch.rglob("*") if p.is_file())
            if not groups:
                shutil.rmtree(scratch)
            else:
                errors.append("unproven owned group; preserved " + str(scratch))
            result = {"qualification": "PASS_ACTUAL_EAGER_FORK_AND_FACTORY_HOST_ONLY" if not errors else "FAIL_HOST_ONLY",
                      "errors": errors, "commands": commands, "seconds": time.monotonic() - started,
                      "input_pins": before, "input_pins_after": after, "emitted_inputs": emitted,
                      "emitted_inputs_after": emitted_after, "scratch_removed": not scratch.exists(),
                      "scratch_allocated_bytes_removed": scratch_bytes, "pending_owned_groups": list(groups),
                      "limits": ["Explicit Host physical/ref/TLB/exclusive/root endpoints; no WOS native or scheduled fork.",
                                 "Endpoint exhaustion is controlled Host capacity, not real PMM pressure; current user/CPL3 concurrency is not modeled."]}
            (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps({"result": str(output / "result.json"), "errors": errors, "commands": len(commands)}))
        finally:
            for number, handler in previous_handlers.items():
                signal.signal(number, handler)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
