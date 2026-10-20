#!/usr/bin/env python3
"""Exercise actual paired collector/scanner/MM bodies on real Host allocators.

Physical-zone publication, immutable topology and preliminary reserve totals are
finite endpoints. The capture spy calls the actual MM provider and observes the
actual allocator lock flags; it can inject a real MM hook after preflight. The
registry input is a supplied copy, checked by the actual census predicates.
This grants no kernel scheduling, complete owner census or runtime credit.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
KERNEL = "modules/kern/src/"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def capture(text, signature):
    start = text.index(signature)
    opening = text.index("{", start)
    position, depth = opening + 1, 1
    while depth:
        depth += (text[position] == "{") - (text[position] == "}")
        position += 1
    return text[start:position]


def generate(selected):
    counter = selected(KERNEL + "platform/mm/virt.opt.cpp").read_text()
    counter = counter[counter.index("#ifdef WOS_SELFTEST\nnamespace ker::test::numa_lifecycle::mm_debt {") :]
    collector = selected(KERNEL + "platform/mm/numa_balance.cpp").read_text()
    collector = collector[collector.index("namespace ker::mod::mm::numa::balance {") :]
    call = "::ker::test::numa_lifecycle::mm_debt::snapshot_addressable(mm, capture);"
    assert collector.count(call) == 2
    collector = collector.replace(call, "::fixture::capture_spy(mm, capture);")
    census = selected(KERNEL + "test/numa_lifecycle_census.cpp").read_text()
    predicates = "namespace ker::test::numa_lifecycle {\nnamespace balance=mod::mm::numa::balance;\n"
    predicates += "\n".join(
        capture(census, signature)
        for signature in [
            "auto registry_complete(",
            "auto same_registry_rows(",
            "auto mm_complete(",
            "auto same_mm_debt(",
            "auto mm_settled(",
            "void prepare_mm_capture(",
            "auto coherent_registry_mm(",
            "auto same_physical_ownership_counts(",
            "auto same_physical_counts(",
            "auto same_physical_ownership(",
            "auto same_physical(",
        ]
    )
    predicates += "\n}\n"
    shell = Path(__file__).with_name("numa_coherent_census_cases.inc").read_text()
    for name, body in [("COUNTERS", counter), ("COLLECTORS", collector), ("PREDICATES", predicates)]:
        marker = "/* @" + name + "@ */"
        assert shell.count(marker) == 1
        shell = shell.replace(marker, body)
    return shell


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source_root = (args.source_root or root).resolve(strict=True)

    def selected(relative):
        path = source_root / relative
        return path if path.is_file() else root / relative

    evidence = args.evidence_dir
    if evidence is not None:
        evidence.mkdir(mode=0o700)
    helper_path = root / "tests/host/unit/file_mmap_registry_runtime_test.py"
    owned = load(helper_path, "coherent_owned")
    paths = [Path(__file__), Path(__file__).with_name("numa_coherent_census_cases.inc"), helper_path]
    paths += [
        selected(KERNEL + path)
        for path in [
            "platform/mm/virt.opt.cpp",
            "platform/mm/numa_balance.cpp",
            "platform/mm/numa_balance.hpp",
            "platform/mm/numa_balance_scan.hpp",
            "platform/mm/page_alloc.cpp",
            "platform/mm/page_alloc.hpp",
            "platform/mm/phys.hpp",
            "test/numa_lifecycle_mm_debt.hpp",
            "test/numa_lifecycle_census.cpp",
        ]
    ]
    before = [owned.pin(path) for path in paths]
    result = {
        "pass": False,
        "errors": [],
        "commands": [],
        "source_held": False,
        "scratch_removed": False,
        "preserved_scratch": None,
        "runtime_qualified": False,
        "whole_budget_seconds": 120,
        "test_seams": __doc__,
    }
    scratch, groups, dependencies = None, [], []
    started, handlers = time.monotonic(), {}
    deadline = started + 120
    owned_command_active = False
    pending_signal = None

    def interrupted(number, _):
        nonlocal pending_signal
        if owned_command_active:
            pending_signal = number
            return
        raise KeyboardInterrupt(str(number))

    def command(argv, name, expected=0):
        log = scratch / (name + ".log")
        nonlocal owned_command_active, pending_signal
        # Defer Python unwind through the unchanged bounded command/reap owner.
        owned_command_active = True
        try:
            row = owned.command(argv, log, scratch, deadline, groups)
        finally:
            owned_command_active = False
            if pending_signal is not None:
                number = pending_signal
                pending_signal = None
                raise KeyboardInterrupt("deferred signal " + str(number))
        row.update(name=name, expected=expected)
        result["commands"].append(row)
        if evidence is not None:
            shutil.copyfile(log, evidence / log.name)
        print(log.read_text()[-4096:], end="", flush=True)
        if row["exit"] != expected:
            raise ValueError(f"{name}: expected {expected}, got {row['exit']}")
        return log

    try:
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
        for number in handlers:
            signal.signal(number, interrupted)
        owned.subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-coherent-census-", dir="/tmp"))
        include = scratch / "include"
        # Preserve every copied real header byte. Local quoted includes select
        # the same actual balance/phys/scanner declarations in this overlay.
        for relative in [
            "platform/mm/numa_balance.hpp",
            "platform/mm/numa_balance_scan.hpp",
            "platform/mm/phys.hpp",
            "test/numa_lifecycle_mm_debt.hpp",
        ]:
            destination = include / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(selected(KERNEL + relative), destination)
        # Host-only finite saturation seeds. Every actual Epoch method/body and
        # data layout remains exact; visibility alone exposes its atomic seeds.
        epoch = selected(KERNEL + "platform/mm/lifecycle_census.hpp").read_text()
        opening = epoch.index("class Epoch final {")
        boundary = epoch.index("class Operation final {", opening)
        original = epoch[opening:boundary]
        assert original.count("private:") == 1
        (include / "platform/mm/lifecycle_census.hpp").write_text(
            epoch[:opening] + original.replace("private:", "public:", 1) + epoch[boundary:]
        )
        body = generate(selected)
        variants = {"actual": (body, None)}
        replacements = [
            (
                "stale-preflight",
                "    ::fixture::capture_spy(mm, capture);",
                "    ::fixture::capture_spy(mm, capture, false);",
                1,
                "mutation after ignored preflight must invalidate the short join",
            ),
            (
                "missing-short-join",
                "registry.stamp.version == mm.version_begin",
                "true",
                0,
                "mutation after ignored preflight must invalidate the short join",
            ),
            ("missing-inflight", "value.inflight == 0 && ", "", 0, "active writer must refuse the coherent point"),
            ("missing-quantity", "sizeof(a) - 32", "size_t{0}", 0, "retained debt must fail exact endpoint equality"),
            (
                "missing-physical-free",
                "a.free_pages != b.free_pages || ",
                "",
                0,
                "actual changed physical counts must fail equality",
            ),
        ]
        for name, old, new, ordinal, reason in replacements:
            assert body.count(old) > ordinal
            position = body.index(old)
            for _ in range(ordinal):
                position = body.index(old, position + len(old))
            variant = body[:position] + new + body[position + len(old) :]
            variants[name] = (variant, reason)
        paired = capture(body, "auto selftest_snapshot_with_mm(")
        lock = "for (size_t i = 0; i < scratch.zone_count; ++i) scratch.lock_flags[i] = scratch.zones[i].allocator->lock_irq();"
        assert paired.count(lock) == 1
        variants["capture-before-last-lock"] = (
            body.replace(
                paired, paired.replace(lock, lock.replace("i < scratch.zone_count", "i + 1 < scratch.zone_count")), 1
            ),
            "authoritative capture must hold every allocator lock",
        )
        scan = "    detail::scan_locked(*topology, out, scratch);"
        assert paired.count(scan) == 1
        variants["skip-full-scan"] = (
            body.replace(paired, paired.replace(scan, "    /* omitted actual full scan */"), 1),
            "both unchanged full physical scans complete",
        )
        point = "    ::fixture::capture_spy(mm, capture);"
        position = paired.rindex(point)
        late = paired[:position] + paired[position + len(point) :]
        marker = "    return detail::finish_unlocked(out, scratch);"
        assert late.count(marker) == 1
        late = late.replace(marker, point + "\n" + marker)
        variants["capture-after-unlock"] = (
            body.replace(paired, late, 1),
            "authoritative capture must hold every allocator lock",
        )
        cxx = os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")
        flags = [
            cxx,
            "-std=c++23",
            "-DWOS_SELFTEST=1",
            "-DWOS_HOST_TEST=1",
            "-O1",
            "-g0",
            "-fno-pie",
            "-fsanitize=address,undefined",
            "-fno-omit-frame-pointer",
            "-pthread",
            "-I" + str(include),
            "-I" + str(root / "tests/host/kern_shim"),
            "-I" + str(root / "modules/kern/src"),
            "-I" + str(root / "modules"),
        ]
        allocator = scratch / "allocator.o"
        dep = scratch / "allocator.d"
        command(
            flags
            + [
                "-MD",
                "-MF",
                str(dep),
                "-c",
                str(selected(KERNEL + "platform/mm/page_alloc.cpp")),
                "-o",
                str(allocator),
            ],
            "allocator-compile",
        )
        dependencies.extend(owned.dependency_pins(dep))
        for name, (content, reason) in variants.items():
            source, binary = scratch / (name + ".cpp"), scratch / name
            source.write_text(content)
            dep = scratch / (name + ".d")
            command(
                flags + ["-MD", "-MF", str(dep), str(source), str(allocator), "-no-pie", "-o", str(binary)],
                name + "-compile",
            )
            dependencies.extend(owned.dependency_pins(dep))
            log = command([str(binary)], name + "-run", 0 if name == "actual" else -signal.SIGABRT)
            if reason is not None and reason not in log.read_text():
                raise ValueError("compiled control failed at an unrelated oracle: " + name)
            if evidence is not None:
                shutil.copyfile(source, evidence / source.name)
        result["pass"] = True
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for number in handlers:
            signal.signal(number, signal.SIG_IGN)
        try:
            try:
                result["inputs_before"] = before
                result["inputs_after"] = [owned.pin(path) for path in paths]
                result["source_held"] = before == result["inputs_after"]
                result["dependencies_before"] = dependencies
                result["dependencies_after"] = [owned.pin(Path(row["path"])) for row in dependencies]
                result["emitted_inputs_held"] = dependencies == result["dependencies_after"]
                if not result["source_held"] or not result["emitted_inputs_held"]:
                    result["errors"].append("source or emitted input changed")
            except BaseException as error:
                result["errors"].append("input recheck: " + repr(error))
            result["pending_owned_groups"] = groups[:]
            if scratch is not None:
                if groups:
                    result["preserved_scratch"] = str(scratch)
                    result["errors"].append("unproven group; scratch retained")
                else:
                    try:
                        shutil.rmtree(scratch)
                        result["scratch_removed"] = not scratch.exists()
                    except BaseException as error:
                        result["preserved_scratch"] = str(scratch)
                        result["errors"].append("scratch cleanup: " + repr(error))
            result["seconds"] = time.monotonic() - started
            result["pass"] = result["pass"] and not result["errors"] and result["scratch_removed"]
            if evidence is not None:
                (evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            print(
                json.dumps(
                    {
                        key: result.get(key)
                        for key in [
                            "pass",
                            "errors",
                            "seconds",
                            "source_held",
                            "emitted_inputs_held",
                            "scratch_removed",
                            "preserved_scratch",
                        ]
                    }
                )
            )
        finally:
            for number, handler in handlers.items():
                signal.signal(number, handler)
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
