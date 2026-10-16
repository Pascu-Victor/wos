#!/usr/bin/env python3
"""Compile actual managed-huge owners and extracted MM helpers on the Host.

Requires an already configured Host build and its existing GTest libraries.
Root/PUB/locks/address/frame/flush endpoints are explicit C++ test seams. This
does not execute WOS, a buddy allocator, an actual TLB ACK or an SMP workload.
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
        "path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
        "device": after.st_dev, "inode": after.st_ino,
        "mtime_ns": after.st_mtime_ns, "ctime_ns": after.st_ctime_ns,
    }


def bounded(text: str, first: str, last: str) -> str:
    if text.count(first) != 1 or text.count(last) != 1:
        raise RuntimeError(f"actual extraction boundary changed: {first!r}, {last!r}")
    return text[text.index(first):text.index(last) + len(last)]


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-build", type=Path, required=True,
                        help="existing configured Host tree containing compile_commands.json and lib/libgtest*.a")
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--kernel-overlay", type=Path,
                        help="optional explicit staged modules/kern/src include/source overlay")
    parser.add_argument("--gtest-main", type=Path)
    parser.add_argument("--gtest", type=Path)
    parser.add_argument("--output", type=Path, help="fresh directory for small logs/results; scratch is always removed")
    options = parser.parse_args()
    root = options.source_root.resolve()
    host_build = options.host_build.resolve()
    overlay = options.kernel_overlay.resolve() if options.kernel_overlay else None
    support = Path(__file__).resolve().parent
    output = options.output.resolve() if options.output else Path(tempfile.mkdtemp(prefix="managed-huge-result-"))
    if options.output:
        output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    started = time.monotonic()
    commands: list[dict] = []
    groups: list[int] = []
    processes: list[subprocess.Popen] = []
    errors: list[str] = []
    selected: list[dict] = []
    emitted: list[dict] = []
    environment = dict(os.environ, TMPDIR=str(scratch), TMP=str(scratch), TEMP=str(scratch),
                       ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                       UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1")

    def kernel_file(relative: str) -> Path:
        staged = overlay / relative if overlay else None
        return staged if staged is not None and staged.is_file() else root / "modules/kern/src" / relative

    def allocated_bytes() -> int:
        return sum(path.stat().st_blocks * 512 for path in scratch.rglob("*") if path.is_file())

    def run(label: str, arguments: list, expected_success: bool = True) -> None:
        remaining = OVERALL_SECONDS - (time.monotonic() - started)
        if remaining <= 0 or allocated_bytes() > SCRATCH_BYTES:
            raise RuntimeError("overall time/scratch budget exhausted")
        argv = [str(argument) for argument in arguments]
        log = output / f"{label}.log"
        before = time.monotonic()
        with log.open("wb") as stream:
            process = subprocess.Popen(argv, cwd=root, env=environment, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            groups.append(process.pid)
            processes.append(process)
            try:
                code = process.wait(timeout=min(COMMAND_SECONDS, remaining))
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise RuntimeError(f"{label}: timed out")
        try:
            os.killpg(process.pid, 0)
            absent = False
        except ProcessLookupError:
            absent = True
        commands.append({"label": label, "argv": argv, "exit": code,
                         "seconds": time.monotonic() - before, "leader_reaped": True,
                         "owned_group_absent": absent, "log": pin(log)})
        if not absent or (code == 0) != expected_success:
            raise RuntimeError(f"{label}: unexpected exit {code}")

    try:
        module = kernel_file("platform/mm/managed_huge.cpp")
        factory = kernel_file("platform/mm/numa_alloc.cpp")
        virt = kernel_file("platform/mm/virt.opt.cpp")
        header = kernel_file("platform/mm/virt.hpp")
        fixture = support / "managed_huge_test.cpp"
        prelude = support / "managed_huge_resolver_prelude.inc"
        cases = support / "managed_huge_resolver_cases.inc"
        libraries = [path.resolve() for path in
                     [options.gtest_main or host_build / "lib/libgtest_main.a",
                      options.gtest or host_build / "lib/libgtest.a"]]
        selected = [pin(path) for path in [Path(__file__), module, factory, virt, header, fixture, prelude, cases, *libraries]]

        rows = json.loads((host_build / "compile_commands.json").read_text())
        row = next(item for item in rows if item["file"].endswith("/tests/host/unit/numa_alloc_test.cpp")
                   and "numa_alloc_test.dir" in item["command"])
        (output / "actual-host-command-row.json").write_text(json.dumps(row, indent=2) + "\n")
        flags = compile_flags(row, overlay)
        selected.append(pin(Path(shutil.which(flags[0]) or flags[0])))
        source = virt.read_text()
        resolver = bounded(source, "// BEGIN managed user-leaf scalar core.", "// END managed user-leaf scalar core.")
        private = bounded(source, "// BEGIN private managed publication/split.", "// END private managed publication/split.")
        commit = bounded(source, "auto user_page_pin_commit_write(const UserPagePin& pin) -> bool {",
                         "void unpin_user_page(UserPagePin& pin) {").rsplit("void unpin_user_page", 1)[0]
        index = next(line for line in source.splitlines() if line.startswith("auto index_of("))
        composite = scratch / "actual-source.cpp"
        composite.write_text("\n".join([fixture.read_text(), prelude.read_text(), index, resolver, private, commit, cases.read_text()]))
        segments = {"resolver": resolver, "private": private, "commit": commit, "index": index}
        (output / "extraction-map.json").write_text(json.dumps({
            "virt": pin(virt), "header": pin(header), "composite": pin(composite),
            "segments": [{"name": name, "bytes": len(text.encode()), "sha256": hashlib.sha256(text.encode()).hexdigest()}
                         for name, text in segments.items()],
        }, indent=2) + "\n")

        sources = [module, factory, composite]
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
        if tests["failures"] != 0 or tests["tests"] != 21:
            raise RuntimeError("actual 21-case suite did not pass")

        def mutant(label: str, text: str, test: str) -> None:
            path = scratch / f"{label}.cpp"
            path.write_text(text)
            obj = scratch / f"{label}.o"
            run(f"compile-{label}", flags + ["-c", path, "-o", obj])
            binary = link(label, [objects[0], objects[1], obj])
            run(f"run-{label}", [binary, f"--gtest_filter={test}"], expected_success=False)

        actual = composite.read_text()
        needle = ("if (level != 2 ||\n                managed_huge_registry.lookup_mapped(key, "
                  "address & ~(huge::LEAF_BYTES - 1), raw, result.managed) != huge::Error::None)")
        if actual.count(needle) != 1:
            raise RuntimeError("unknown-PS mutation boundary changed")
        mutant("unknown-ps-authority", actual.replace(needle, "if (level != 2)"),
               "ManagedResolver.UnknownTwoMegabyteOneGigabyteAndAbaNeverGrantProvenance")

        needle = "current.mark_dirty();  // CURRENT PS or PML1 slot, never a saved PDE."
        if actual.count(needle) != 1:
            raise RuntimeError("dirty mutation boundary changed")
        replacement = ("MutableUserEntrySlot stale; if (managed_pml2_slot_locked(pin.pagemap,pin.user_page,stale)"
                       "==Pml2Walk::Slot) stale.mark_dirty();")
        mutant("stale-pde-dirty", actual.replace(needle, replacement),
               "ManagedResolver.ExactCurrentLeafDirtyCommitSurvivesPureSplitWithNoRefChange")

        needle = ("flush_pagemap_after_update(root, 0, true);\n"
                  "    // Only this lexical post-real-flush completion returns managed capacity.")
        if actual.count(needle) != 1:
            raise RuntimeError("ACK mutation boundary changed")
        early = ("for (uint16_t i=0;i<storage->descriptor_count;++i) "
                 "(void)managed_huge_registry.complete_split_after_ack(storage->descriptors[i].handle);\n    ")
        bad = actual.replace(needle, early + needle)
        needle = "managed_huge_registry.complete_split_after_ack(d.handle) != huge::Error::None"
        if bad.count(needle) != 1:
            raise RuntimeError("post-ACK mutation boundary changed")
        mutant("capacity-before-ack", bad.replace(needle, "false"),
               "ManagedResolver.ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck")

        text = module.read_text()
        needle = "(void)owner.span_.release();"
        if text.count(needle) != 1:
            raise RuntimeError("data-transfer mutation boundary changed")
        bad_module = scratch / "drop-adopted-data.cpp"
        bad_module.write_text(text.replace(needle, "owner.span_.reset();"))
        obj = scratch / "drop-adopted-data.o"
        run("compile-drop-adopted-data", flags + ["-iquote", module.parent, "-c", bad_module, "-o", obj])
        binary = link("drop-adopted-data", [obj, objects[1], objects[2]])
        run("run-drop-adopted-data", [binary, "--gtest_filter=ManagedResolver.ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck"],
            expected_success=False)
    except Exception as error:
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
        for group in groups:
            try:
                os.killpg(group, 0)
            except ProcessLookupError:
                continue
            os.killpg(group, signal.SIGKILL)
            errors.append(f"owned group remained after leader reap: {group}")
        for process in processes:
            if process.poll() is None:
                process.wait(timeout=5)
        removed = allocated_bytes()
        shutil.rmtree(scratch)
        result = {
            "qualification": "PASS_ACTUAL_MANAGED_HUGE_HOST_ONLY" if not errors else "FAIL_ACTUAL_MANAGED_HUGE_HOST_ONLY",
            "errors": errors, "seconds": time.monotonic() - started, "commands": commands,
            "input_pins": selected, "input_pins_after": selected_after,
            "emitted_inputs_before": emitted, "emitted_inputs_after": emitted_after,
            "emitted_input_count": len(emitted), "scratch_removed": not scratch.exists(),
            "scratch_allocated_bytes_removed": removed,
            "limits": ["Actual C++ source with explicit Host root/PUB/lock/frame/table/flush endpoints.",
                       "No Native, PMM/buddy, actual TLB ACK, CPL3/SMP, pressure or goal qualification.",
                       "ASan/UBSan enabled; LSan disabled for the configured Host environment."],
        }
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"result": str(output / "result.json"), "qualification": result["qualification"], "errors": errors}))
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
