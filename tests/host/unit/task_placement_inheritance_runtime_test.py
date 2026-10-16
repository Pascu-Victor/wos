#!/usr/bin/env python3
"""Compile actual scheduler placement bodies with explicit Host transport endpoints.

Real RunHeap::contains/storage and scalar Task accessors are captured. Task lifetime,
queue IRQ transport and final existing publishers are bounded Host endpoints; native
private-queue cases separately retain actual kernel Task/RunQueue representation.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]


def generate(root, source_root, capture):
    inputs, fragments = {}, []
    def text(rel):
        path = source_root/rel
        if not path.is_file():
            path = root/rel
        value = path.read_text()
        inputs[str(path)] = hashlib.sha256(value.encode()).hexdigest()
        return value
    def definition(body, anchor, declaration=False):
        result, witness = capture(body, anchor, declaration=declaration)
        fragments.append(witness)
        return result
    scheduler = text("modules/kern/src/platform/sched/scheduler.cpp")
    header = text("modules/kern/src/platform/sched/scheduler.hpp")
    task = text("modules/kern/src/platform/sched/task.hpp")
    heap = text("modules/kern/src/platform/sched/run_heap.cpp")
    text("modules/kern/src/platform/sched/run_heap.hpp")
    snapshot = definition(header, "struct TaskPlacementSnapshot {", True)
    ownership = definition(scheduler, "struct TaskPlacementOwnership {", True)
    accessors = []
    for field in ("cpu", "domain_mask", "domain_id", "cpu_pinned", "domain_hard"):
        accessors.append(definition(task, "auto load_"+field+"()"))
        accessors.append(definition(task, "void store_"+field+"("))
    names = ["inline auto cpu_mask_contains(", "inline auto wait_list_contains_locked(",
             "inline auto runqueue_task_is_reserved_locked(RunQueue* rq, task::Task const* task) -> bool {", "inline auto runqueue_owns_task_locked(",
             "auto owner_cpu_for_task(", "auto hinted_or_scanned_owner_cpu(", "auto task_placement_online_mask(",
             "auto task_placement_subject_is_live(", "auto task_placement_ownership_locked(",
             "auto inherited_task_placement_mask(", "auto observe_task_placement_locked(",
             "auto inherited_task_is_fresh(", "auto initialize_inherited_task_placement_for_cores(",
             "auto inherited_task_placement_matches(", "auto inherited_round_robin_cpu(",
             "auto capture_task_placement_snapshot(", "auto initialize_inherited_task_placement(",
             "auto post_task_balanced_inherited(", "auto post_task_round_robin_inherited("]
    functions = "\n\n".join(definition(scheduler, name) for name in names)
    creation = text("modules/kern/src/syscalls_impl/process/exec.cpp")
    wrappers = "\n".join(definition(creation, anchor) for anchor in [
        "auto wos_proc_exec(", "auto wos_proc_spawn(",
        "auto create_native_compartment_for_subject(mod::sched::task::Task* executor, uint64_t spec_address) -> int64_t {"])
    legacy_initializer = definition(creation, "if (int const ERROR = sched::initialize_inherited_task_placement(new_task, inherited_placement)")
    legacy_publication = definition(creation, "if (!sched::post_task_balanced_inherited(new_task, inherited_placement))")
    native_publication = definition(creation, "if (result == 0) {\n                auto publication_placement")
    shebang_continuation = definition(creation, "[spawn_options, &PARENT_CREDENTIALS, memory_policy, allocation_context, inherited_placement](")
    native_constructor = definition(creation, "explicit NativeCompartmentPreparation(mod::sched::task::Task& owner, const mod::sched::TaskPlacementSnapshot& placement)\n")
    native_member = "const mod::sched::TaskPlacementSnapshot inherited_placement;"
    if creation.count(native_member) != 1:
        raise ValueError("native persistent placement field changed")
    native_initializer = "error = mod::sched::initialize_inherited_task_placement(child, root.inherited_placement);\n            if (error == 0) error = initialize_native_creation_priority(root, *child);"
    if creation.count(native_initializer) != 1:
        raise ValueError("native initializer/refusal ordering changed")
    for anchor, body in [(native_member, native_member), (native_initializer, native_initializer)]:
        fragments.append({"name": anchor, "line": creation.count("\n", 0, creation.index(body)) + 1,
                          "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()})
    preparation = capture(creation, "static auto prepare_native_compartment_inputs_for_owner(")[0]
    native_failure_tail = definition(preparation, "if (error != 0) {")
    shell_path = Path(__file__).with_name("task_placement_inheritance_cases.inc")
    inputs[str(shell_path)] = hashlib.sha256(shell_path.read_bytes()).hexdigest()
    result = shell_path.read_text()
    pieces = {"SNAPSHOT": snapshot, "OWNERSHIP": ownership, "ACCESSORS": "\n".join(accessors),
              "HEAP_ENTRY": definition(heap, "inline auto heap_entry(RunHeap const&"),
              "CONTAINS": definition(heap, "bool RunHeap::contains("), "FUNCTIONS": functions,
              "CREATION_WRAPPERS": wrappers, "LEGACY_INITIALIZER": legacy_initializer, "LEGACY_PUBLICATION": legacy_publication,
              "SHEBANG_CONTINUATION": shebang_continuation,
              "NATIVE_PLACEMENT_MEMBER": native_member, "NATIVE_PLACEMENT_CONSTRUCTOR": native_constructor,
              "NATIVE_INITIALIZER": native_initializer, "NATIVE_FAILURE_TAIL": native_failure_tail,
              "NATIVE_PUBLICATION": native_publication}
    for key, value in pieces.items():
        assert result.count("// INJECT_"+key) == 1
        result = result.replace("// INJECT_"+key, value)
    return result, inputs, fragments


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--host-cxx", default=os.environ.get("HOST_CXX"))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    helper = root/"tests/host/unit/file_mmap_registry_runtime_test.py"
    spec = importlib.util.spec_from_file_location("placement_owned_command", helper)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    compiler = args.host_cxx or shutil.which("g++", path=os.defpath) or shutil.which("clang++", path=os.defpath)
    if not compiler:
        raise ValueError("host compiler unavailable")
    owned.FILE_BUDGET, owned.SCRATCH_BUDGET, owned.LOG_BUDGET = 32 << 20, 128 << 20, 4 << 20
    owned.subreaper()
    scratch = Path(tempfile.mkdtemp(prefix="task-placement-inheritance-"))
    started, groups, commands, errors = time.monotonic(), [], [], []
    deadline = started+120
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    def interrupted(number, _frame):
        raise KeyboardInterrupt(str(number))
    for sig in handlers:
        signal.signal(sig, interrupted)
    result = {}
    last_log = None
    try:
        source, used, fragments = generate(root, args.source_root.resolve(), owned.capture)
        used[str(Path(__file__))] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        used[str(helper)] = hashlib.sha256(helper.read_bytes()).hexdigest()
        before = [owned.pin(Path(path)) for path in sorted(used)]
        before.append(owned.pin(Path(compiler).resolve()))
        variants = {"actual": source}
        changes = {
            "stale-owner": (".owner_cpu = owner_cpu,", ".owner_cpu = subject->load_cpu(),"),
            "missing-owner": ("if (!task_placement_ownership_locked(owner, subject, ownership)) return -EBUSY;",
                              "if ((static_cast<void>(task_placement_ownership_locked(owner, subject, ownership)), false)) return -EBUSY;"),
            "ignored-mask": ("if (cpu_mask_contains(allowed, CPU)) return CPU;", "if ((static_cast<void>(allowed), true)) return CPU;"),
            "wrong-publisher": ("return snapshot.cpu_pinned ? post_task_for_cpu(snapshot.owner_cpu, subject) : post_task_balanced(subject);",
                                "return post_task_balanced(subject);"),
        }
        native_wrapper = owned.capture(source, "auto create_native_compartment_for_subject(mod::sched::task::Task* executor, uint64_t spec_address) -> int64_t {")[0]
        capture_line = "    if (int const ERROR = mod::sched::capture_task_placement_snapshot(executor, inherited_placement); ERROR != 0) return ERROR;"
        if native_wrapper.count(capture_line) != 1:
            raise ValueError("actual native public capture changed")
        changes.update({
            "uncaptured-native-actor": (native_wrapper, native_wrapper.replace(capture_line, "    // Genuine control: omitted actual public capture.")),
            "lost-native-copy": ("inherited_placement(placement) {}", "inherited_placement{} { (void)placement; }"),
            "lost-shebang-copy": ("[spawn_options, &PARENT_CREDENTIALS, memory_policy, allocation_context, inherited_placement](",
                                  "[spawn_options, &PARENT_CREDENTIALS, memory_policy, allocation_context, &inherited_placement]("),
            "old-legacy-publisher": ("!sched::post_task_balanced_inherited(new_task, inherited_placement)", "!(static_cast<void>(inherited_placement), sched::post_task_balanced(new_task))"),
            "ambient-native-publisher": ("!mod::sched::post_task_round_robin_inherited(child, publication_placement, root->inherited_placement.owner_cpu)",
                                          "!(static_cast<void>(publication_placement), mod::sched::post_task_for_cpu(mod::cpu::current_cpu(), child))"),
        })
        for label, (old, new) in changes.items():
            if source.count(old) != 1:
                raise ValueError("genuine mutation anchor is ambiguous: "+label)
            variants[label] = source.replace(old, new)
        def command(argv, label):
            nonlocal last_log
            last_log = scratch/(label+".log")
            mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(handlers))
            try:
                row = owned.command(argv, last_log, scratch, deadline, groups)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
            commands.append(row)
            return row
        dependency_inputs = []
        for label, content in variants.items():
            cpp, binary, dep = scratch/(label+".cpp"), scratch/label, scratch/(label+".d")
            obj = scratch/(label+".o")
            cpp.write_text(content)
            flags = [compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror", "-pthread", "-fno-pie",
                     "-fsanitize=address,undefined", "-I", str(root/"modules/kern/src")]
            compiled = command(flags+["-MD", "-MF", str(dep), "-c", str(cpp), "-o", str(obj)], label+"-compile")
            if compiled["exit"] != 0:
                raise ValueError("actual-source compile failed: "+label)
            deps = owned.dependency_pins(dep)
            dependency_inputs.extend(deps)
            linked = command([compiler, "-pthread", "-fno-pie", "-no-pie", "-fsanitize=address,undefined", str(obj), "-o", str(binary)], label+"-link")
            if linked["exit"] != 0:
                raise ValueError("actual-source link failed: "+label)
            executed = command([str(binary)], label+"-run")
            if label == "actual":
                if executed["exit"] != 0 or "PASS actual placement" not in (scratch/(label+"-run.log")).read_text():
                    raise ValueError("actual-source assertions failed")
            elif executed["exit"] != -signal.SIGABRT or "Assertion" not in (scratch/(label+"-run.log")).read_text():
                raise ValueError("compiled mutant did not reach intended assertion: "+label)
            # Generated sources/binaries are bounded scratch. Capture hashes and
            # current include holds before removing them with all owned scratch.
            for item in deps:
                if owned.pin(Path(item["path"])) != item:
                    raise ValueError("actual emitted compiler input changed")
        after = [owned.pin(Path(item["path"])) for item in before]
        if after != before:
            raise ValueError("source bytes/metadata changed during validation")
        result = dict(source_before=before, source_after=after, fragments=fragments, dependency_inputs=dependency_inputs,
                      variants=list(variants), limitations="Explicit Host Task/IRQ-queue/final publication plus creation credential/NUMA/input/image-core/cleanup endpoints; actual new caller/initializer/post slices only, LSan off; no whole syscall/GS/root/live kernel/SMP/public creation qualification")
    except BaseException as exc:
        errors.append(type(exc).__name__+": "+str(exc))
        if args.evidence_dir is None and last_log is not None:
            try:
                if last_log.is_file():
                    print("Last actual command log: "+last_log.name, file=sys.stderr)
                    print(last_log.read_text(errors="replace")[-32768:], file=sys.stderr)
            except OSError as failure:
                errors.append("failure log: "+repr(failure))
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            if groups:
                errors.append("owned compiler session unresolved; preserve scratch")
            else:
                if args.evidence_dir:
                    args.evidence_dir.mkdir(parents=True, exist_ok=True)
                    for log in scratch.glob("*.log"):
                        shutil.copy2(log, args.evidence_dir/log.name)
                shutil.rmtree(scratch)
        except BaseException as exc:
            errors.append("cleanup: "+repr(exc))
        for sig, original in handlers.items():
            signal.signal(sig, original)
    result.update(errors=errors, commands=commands, seconds=time.monotonic()-started, scratch=str(scratch),
                  scratch_removed=not scratch.exists(), unresolved_groups=groups, qualification="PASS" if not errors else "FAIL")
    if args.evidence_dir:
        args.evidence_dir.mkdir(parents=True, exist_ok=True)
        (args.evidence_dir/"result.json").write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps({key: result[key] for key in ("qualification", "seconds", "errors", "scratch_removed")}))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
