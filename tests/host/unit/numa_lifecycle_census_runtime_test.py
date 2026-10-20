#!/usr/bin/env python3
"""Run the complete live handler on explicit Host scheduler/provider/transport endpoints.

The handler, wire/scope and owner predicates are actual source. Host endpoints
model owned Tasks, read-only snapshots and mapped-only copies; they grant no
kernel scheduling, physical scan, UID transition or real usercopy qualification.
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


def capture(text, signature, declaration=False):
    start = text.index(signature)
    opening = text.index("{", start)
    position, depth = opening + 1, 1
    while depth:
        depth += (text[position] == "{") - (text[position] == "}")
        position += 1
    if declaration:
        assert text[position] == ";"
        position += 1
    return text[start:position]


def source_text(root, source_root, relative):
    path = source_root / relative
    if not path.is_file():
        path = root / relative
    return path.read_text()


def generate(root, source_root):
    kernel = "modules/kern/src/"

    def text(relative):
        return source_text(root, source_root, kernel + relative)

    def declaration(relative, name):
        return capture(text(relative), "struct " + name + " {", True)

    def clean(value):
        return "\n".join(
            line
            for line in value.split("\n")
            if not line.startswith("#include") and not line.startswith("#pragma once")
        )

    declarations = [
        "namespace ker::mod::mm::file_mapping {"
        + "\n".join(capture(text("platform/mm/unpublished_mapping_retirement.hpp"), "enum class " + name + " :", True)
                    for name in ["UnpublishedRetirementState", "UnpublishedRetirementMode", "UnpublishedRetirementResult"])
        + capture(text("platform/mm/file_mapping_lifetime.hpp"), "enum class RetirementReturnState :", True)
        + "}"
    ] + [
        "namespace ker::mod::acpi::numa {"
        + capture(text("platform/acpi/numa_topology.hpp"), "enum class Provenance :", True)
        + "}"
    ]
    groups = [
        ("ker::mod::mm::lifecycle_storage", "platform/mm/lifecycle_storage.hpp", ["Counts", "Snapshot"]),
        ("ker::vfs", "vfs/fd_reservation.hpp", ["SelftestFdSlot", "SelftestFdCensus"]),
        ("ker::vfs", "vfs/mount.hpp", ["SelftestMountCensus"]),
        ("ker::mod::sched::resource", "platform/sched/resource_process.hpp", ["SelftestPopulationCensus"]),
        ("ker::mod::sched::resource", "platform/sched/service_resource.hpp", ["SelftestServiceCensus"]),
        (
            "ker::syscall::process::child_events",
            "syscalls_impl/process/child_events.hpp",
            ["SelftestLifecycleCensus", "SelftestOwnerLifecycleCensus"],
        ),
        ("ker::syscall::shm", "syscalls_impl/shm/shm.hpp", ["SelftestShmCensus"]),
        ("ker::mod::mm::virt", "platform/mm/virt.hpp", ["UserRootControlStats", "SelftestRootCensus"]),
        (
            "ker::mod::mm::phys",
            "platform/mm/phys.hpp",
            ["AllocStatsSnapshot", "PhysicalOwnerSnapshot", "PhysicalReserveSnapshot", "PhysicalBalanceSnapshot"],
        ),
        ("ker::mod::mm::file_mapping", "platform/mm/lifetime_retirement_executor.hpp", ["LifetimeRetirementSnapshot"]),
    ]
    for namespace, relative, names in groups:
        declarations.append(
            "namespace " + namespace + " {\n" + "\n".join(declaration(relative, name) for name in names) + "\n}"
        )
    declarations.append(
        "namespace ker::mod::mm::numa::balance {\n"
        + capture(text("platform/mm/numa_balance.hpp"), "enum class Error :", True)
        + "\n"
        + "\n".join(declaration("platform/mm/numa_balance.hpp", name) for name in ["Result", "Counts", "Snapshot"])
        + "\n}"
    )
    scalars = "\n".join(
        declaration("test/compartment_lifecycle_census.hpp", name) for name in ["CensusCpuScalars", "CensusScalars"]
    )
    scope = clean(text("test/numa_lifecycle_census.hpp"))
    owner = clean(text("test/lifecycle_owner_census.hpp"))
    body = clean(text("test/numa_lifecycle_census.cpp"))
    fd_readers = "\n".join(
        capture(text("vfs/fd_reservation.cpp"), name)
        for name in [
            "void FdReservation::collect_selftest(",
            "auto selftest_fd_census_parked(",
            "void selftest_fd_census_current(",
        ]
    )
    pop_readers = "\n".join(
        capture(text("platform/sched/resource_process.cpp"), name)
        for name in [
            "void ProcessReservation::collect_selftest(",
            "auto selftest_population_census(",
            "void selftest_population_census_current(",
        ]
    )
    # Host-only visibility/name seam: preserve every actual Epoch method body
    # while allowing finite saturation seeds, never a production reset API.
    boundary_epoch = capture(text("platform/mm/lifecycle_census.hpp"), "class Epoch final {", True)
    import re
    boundary_epoch = re.sub(r"\bEpoch\b", "BoundaryEpoch", boundary_epoch).replace("private:", "public:")
    declarations.append("namespace ker::mod::mm::lifecycle_census {" + boundary_epoch + "}")
    current_native_support = capture(text("platform/sched/task.cpp"), "class CurrentNativeSemanticOperation final {", True)
    native_readers = "constinit mm::lifecycle_census::BoundaryEpoch current_native_semantic_epoch{};\n" + current_native_support + "\n" + "\n".join(
        capture(text("platform/sched/task.cpp"), name)
        for name in [
            "void adjust_native_return_tail(",
            "void selftest_native_debt_snapshot(",
            "void selftest_current_native_debt_snapshot(",
            "void unregister_native_file_mapping_record(",
            "auto native_unpublished_retirements_pending(",
        ]
    )
    retirement = capture(text("platform/sched/task.cpp"), "auto advance_native_unpublished_process(")
    start = retirement.index("#ifdef WOS_SELFTEST\n    adjust_native_return_tail(true);")
    stop = retirement.index("    return Result::COMPLETE;", start)
    native_readers += (
        "\nvoid actual_return_tail(Task* owner, Task* child, mm::file_mapping::UnpublishedMappingRetirement* record, bool detached) {\n"
        + retirement[start:stop]
        + "\n}\n"
    )
    native_readers += "\n" + "\n".join(
        capture(text("platform/sched/task.cpp"), name)
        for name in [
            "void append_native_retirement_locked(",
            "auto advance_native_unpublished_process(",
            "auto begin_native_file_mapping_construction(",
            "void end_native_file_mapping_construction(",
            "void register_native_file_mapping_record(",
            "auto close_native_file_mapping_admission(",
            "auto process_native_unpublished_retirements_slice(",
            "void defer_transferred_native_unpublished_process(",
        ]
    )
    activity_helpers = "namespace ker::test::numa_lifecycle::mm_debt {\n" + "\n".join(
        capture(text("platform/mm/virt.opt.cpp"), signature)
        for signature in ["auto activity_anchor(", "auto activity_delta(", "auto activity_sum(", "void interpret_activity("]
    ) + "\n}\n"
    registry_reader = capture(text("syscalls_impl/vmem/sys_vmem.cpp"), "void selftest_file_registry_snapshot(")
    shell = Path(__file__).with_suffix(".inc").read_text()
    for marker, value in [
        ("@DECLARATIONS@", "\n".join(declarations)),
        ("@SCALARS@", scalars),
        ("@SCOPE@", scope),
        ("@OWNER@", owner),
        ("@HANDLER@", body),
        ("@FD_READERS@", fd_readers),
        ("@POP_READERS@", pop_readers),
        ("@NATIVE_READERS@", native_readers),
        ("@REGISTRY_READER@", registry_reader),
        ("@ACTIVITY_HELPERS@", activity_helpers),
    ]:
        marker = "/* " + marker + " */"
        assert shell.count(marker) == 1
        shell = shell.replace(marker, value)
    return shell


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source_root = (args.source_root or root).resolve(strict=True)
    spec = importlib.util.spec_from_file_location(
        "bounded_process", root / "tests/host/unit/file_mmap_registry_runtime_test.py"
    )
    if spec is None or spec.loader is None:
        raise ValueError("bounded process helper unavailable")
    process = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(process)
    result = {
        "pass": False,
        "errors": [],
        "commands": [],
        "runtime_qualified": False,
        "source_held": False,
        "scratch_removed": False,
        "preserved_scratch": None,
    }
    watched = [
        Path(__file__).resolve(),
        Path(__file__).with_suffix(".inc").resolve(),
        root / "tests/host/unit/file_mmap_registry_runtime_test.py",
    ]
    for relative in [
        "test/numa_lifecycle_census.cpp",
        "test/numa_lifecycle_census.hpp",
        "test/numa_lifecycle_census_wire.hpp",
        "test/numa_lifecycle_mm_debt.hpp",
        "test/lifecycle_owner_census.hpp",
        "test/compartment_lifecycle_census.hpp",
        "platform/mm/lifecycle_storage.hpp",
        "platform/mm/reclaim.hpp",
        "platform/mm/reclaim_policy.hpp",
        "vfs/fd_reservation.hpp",
        "vfs/mount.hpp",
        "platform/sched/resource_process.hpp",
        "platform/sched/service_resource.hpp",
        "syscalls_impl/process/child_events.hpp",
        "syscalls_impl/shm/shm.hpp",
        "platform/mm/virt.hpp",
        "platform/mm/virt.opt.cpp",
        "platform/mm/phys.hpp",
        "platform/mm/lifetime_retirement_executor.hpp",
        "platform/mm/numa_balance.hpp",
        "platform/mm/lifecycle_census.hpp",
        "platform/acpi/numa_topology.hpp",
        "vfs/fd_reservation.cpp",
        "platform/sched/resource_process.cpp",
        "platform/sched/task.cpp",
        "platform/mm/unpublished_mapping_retirement.hpp",
        "platform/mm/file_mapping_lifetime.hpp",
        "syscalls_impl/vmem/sys_vmem.cpp",
    ]:
        candidate = source_root / "modules/kern/src" / relative
        watched.append(candidate if candidate.is_file() else root / "modules/kern/src" / relative)
    before, groups, scratch = [], [], None
    evidence = args.evidence_dir
    if evidence is not None:
        evidence.mkdir(mode=0o700)
    handlers = {sig: signal.getsignal(sig) for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]}

    def interrupted(number, _):
        raise KeyboardInterrupt(str(number))

    started = time.monotonic()
    try:
        for sig in handlers:
            signal.signal(sig, interrupted)
        before = [process.pin(path) for path in watched]
        process.subreaper()
        scratch = Path(tempfile.mkdtemp(prefix="wos-census-handler-", dir="/tmp"))
        source = scratch / "actual.cpp"
        source.write_text(generate(root, source_root))
        include = scratch / "include/platform/sys"
        include.mkdir(parents=True)
        (include / "spinlock.hpp").write_text(
            "#pragma once\n#include <cstdint>\nnamespace ker::mod::sys {\nstruct Spinlock {\n void lock(){fixture::check(fixture::lock_depth==0);++fixture::lock_depth;}\n bool try_lock(){if(fixture::lock_depth)return false;lock();return true;}\n void unlock(){fixture::check(fixture::lock_depth==1);--fixture::lock_depth;fixture::after_native_unlock();}\n uint64_t lock_irqsave(){lock();return 0;}\n void unlock_irqrestore(uint64_t){unlock();}\n};\n}\n"
        )
        compiler = os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")
        binary = scratch / "actual"
        command = [
            compiler,
            "-std=c++23",
            "-DWOS_SELFTEST=1",
            "-DWOS_HOST_TEST=1",
            "-O1",
            "-g0",
            "-fsanitize=address,undefined",
            "-fno-omit-frame-pointer",
            "-I" + str(scratch / "include"),
            "-I" + str(root / "tests/host/kern_shim"),
            "-I" + str(source_root / "modules/kern/src"),
            "-I" + str(root / "modules/kern/src"),
            "-I" + str(root / "modules"),
            str(source),
            "-o",
            str(binary),
        ]
        generated = source.read_text()
        registry_function = capture(generated, "auto same_registry_rows(")
        endpoint_function = capture(generated, "auto prepare_endpoint(")
        mm_function = capture(generated, "auto same_mm_debt(")
        tail_before = "    adjust_native_return_tail(true);"
        assert generated.count(tail_before) == 2
        tail_wrapper = capture(generated, "void actual_return_tail(")
        scope_class = capture(generated, "class CurrentCensusScope final {", True)
        public_scope = scope_class.replace("private:", "public:", 1)
        assert public_scope != scope_class
        policy_presence = "    present(14, driver.pending_mremap_ps_policies != nullptr);"
        assert generated.count(policy_presence) == 1
        baseline_physical = "same_physical_ownership(plain.physical_baseline, plain.physical_b)"
        assert generated.count(baseline_physical) == 1
        variants = {
            "actual": generated,
            "old-baseline-cache-partition": generated.replace(
                baseline_physical, "same_physical(plain.physical_baseline, plain.physical_b)", 1
            ),
            "omit-endpoint-maintenance": generated.replace(
                endpoint_function, endpoint_function[:endpoint_function.index("{")] + "{ return Outcome::ACCEPT; }", 1),
            "omit-mremap-policy-presence": generated.replace(
                policy_presence, "    // deliberately omitted actual mremap policy presence row", 1
            ),
            "forge-current-scope": generated.replace(scope_class, public_scope, 1),
            "omit-real-return-owner": generated.replace(
                tail_wrapper, tail_wrapper.replace(tail_before, "    // deliberately omitted actual return owner", 1), 1
            ),
            "old-pending-reader-equivalence": generated.replace(
                "out.native_retirements_pending = false;",
                "out.native_retirements_pending = sched::task::native_unpublished_retirements_pending();",
                1,
            ),
            "missing-native-tail": generated.replace("value.return_tails == 0", "true", 1),
            "ignore-registry-geometry": generated.replace(
                registry_function, registry_function[: registry_function.index("{")] + "{ return true; }", 1
            ),
            "ignore-mm-baseline": generated.replace(
                mm_function, mm_function[: mm_function.index("{")] + "{ return true; }", 1
            ),
            "missing-semantic-adoption": generated.replace(
                "        semantic.begin();\n#endif\n        native_retirement_queue_head = record->deferred_next;",
                "        // missing adopted operation\n#endif\n        native_retirement_queue_head = record->deferred_next;", 1),
            "accept-saturated-current-native": generated.replace(
                "if (out.stamp.version == UINT64_MAX) out.stamp.valid = false;", "/* omitted saturated-stamp refusal */", 1),
            "old-conservative-current-native": generated.replace(
                capture(generated, "void selftest_current_native_debt_snapshot("),
                capture(generated, "void selftest_current_native_debt_snapshot(").replace("current_native_semantic_epoch.snapshot()", "native_debt_epoch.snapshot()")),
            "fault-in-output": generated.replace(
                "bool const exported = usercopy::copy_to_task_mapped(",
                "bool const exported = usercopy::copy_to_task(",
                1,
            ),
        }
        for name, content in variants.items():
            if name != "actual" and content == generated:
                raise ValueError("control did not alter actual source: " + name)
            variant_source = scratch / (name + ".cpp")
            variant_source.write_text(content)
            variant_binary = scratch / name
            compile_command = command[:]
            compile_command[compile_command.index(str(source))] = str(variant_source)
            compile_command[-1] = str(variant_binary)
            for phase, argv in [("compile", compile_command), ("run", [str(variant_binary)])]:
                log = scratch / (name + "-" + phase + ".log")
                row = process.command(argv, log, scratch, started + 90, groups)
                row["variant"] = name
                row["phase"] = phase
                result["commands"].append(row)
                print(log.read_text()[-4096:], end="", flush=True)
                expected = (
                    0 if phase == "compile" or name in ["actual", "old-pending-reader-equivalence"] else -signal.SIGABRT
                )
                if row["exit"] != expected:
                    raise ValueError(f'actual handler {name}/{phase}: expected {expected}, got {row["exit"]}')
                if phase == "run" and name == "old-baseline-cache-partition":
                    if "physical ownership partition acceptance oracle failed" not in log.read_text():
                        raise ValueError("old baseline control missed the physical partition acceptance oracle")
        result["pass"] = True
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            try:
                result["input_before"] = before
                result["input_after"] = [process.pin(path) for path in watched]
                result["source_held"] = bool(before) and before == result["input_after"]
                if not result["source_held"]:
                    result["errors"].append("source changed during actual handler test")
            except BaseException as error:
                result["errors"].append(f"input recheck: {error}")
            result["owned_groups_pending"] = groups[:]
            if scratch is not None:
                try:
                    if evidence is not None:
                        for path in [*scratch.glob("*.cpp"), *scratch.glob("*.log")]:
                            if path.is_file():
                                shutil.copyfile(path, evidence / path.name)
                except BaseException as error:
                    result["errors"].append(f"evidence copy: {error}")
                if groups:
                    result["errors"].append("owned group remains; preserve scratch")
                    result["preserved_scratch"] = str(scratch)
                else:
                    try:
                        shutil.rmtree(scratch)
                        result["scratch_removed"] = not scratch.exists()
                    except BaseException as error:
                        result["errors"].append(f"scratch cleanup: {error}")
                        result["preserved_scratch"] = str(scratch)
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
        result["seconds"] = time.monotonic() - started
        result["pass"] &= not result["errors"] and result["source_held"] and result["scratch_removed"]
        if evidence is not None:
            (evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
