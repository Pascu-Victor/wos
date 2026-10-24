#!/usr/bin/env python3
"""Execute actual scheduler leadership/guard bodies with injected timer decisions.

The atomic transport preserves std::atomic operations/orders and adds synchronous
interrupt boundaries. Dispatch, placement, publication mutex and APIC endpoints
are disclosed Host seams; this does not execute the native RQ, IRQ or ptrace code.
Every negative control must compile and then fail the same runtime invariant.
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
        raise ValueError(f"nonunique actual definition: {pattern}: {len(matches)}")
    start = matches[0].start()
    left = source.index("{", start)
    depth = 1
    cursor = left + 1
    while depth:
        depth += (source[cursor] == "{") - (source[cursor] == "}")
        cursor += 1
    return source[start:cursor]


def once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise ValueError(f"nonunique mutation operand: {old}")
    return text.replace(old, new)


def pin(path: Path) -> dict:
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    fields = lambda row: (row.st_dev, row.st_ino, row.st_size, row.st_mtime_ns, row.st_ctime_ns)
    if fields(before) != fields(after):
        raise RuntimeError(f"source changed during read: {path}")
    return dict(path=str(path), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                device=after.st_dev, inode=after.st_ino, mtime_ns=after.st_mtime_ns, ctime_ns=after.st_ctime_ns)


def generate(source: str, header: str, fixture: str) -> tuple[str, list[str]]:
    definitions = []
    for name in ("queue_reschedule_request", "consume_reschedule_request", "retain_or_reacquire_reschedule_leader",
                 "try_claim_task_placement", "drain_reschedule_leader", "finish_task_placement"):
        body = block(source, rf"^(?:inline )?(?:auto|void) {name}\([^;{{]*\{{")
        if name == "drain_reschedule_leader":
            body = "template <typename Dispatch>\n" + body
        definitions.append(body)
    common = "\n".join(definitions)
    wake = block(source, r"^void reschedule_task_for_cpu\([^;{]*\{")
    placement = block(source, r"^auto apply_task_placement_request\([^;{]*\{")
    guard = block(header, r"^struct PreemptGuard \{") + ";"
    preempt = []
    for name in ("preempt_disable_token_at", "preempt_disable_at", "preempt_disable",
                 "preempt_enable_token_at", "preempt_enable_at", "preempt_enable"):
        preempt.append(block(source, rf"^\[\[clang::no_sanitize\(\"kernel-address\"\)\]\] (?:auto|void) {name}\([^;{{]*\{{"))
    variants = [("actual", wake, placement)]
    declaration = "    PreemptGuard const LEADERSHIP_GUARD;"
    election = "    bool const LEADER = queue_reschedule_request(task, cpu_no);"
    variants.append(("wake_missing", once(wake, declaration, ""), placement))
    variants.append(("wake_late", once(once(wake, declaration, ""), election, election + "\n" + declaration), placement))
    early = once(wake, declaration, "    std::optional<PreemptGuard> leadership_guard(std::in_place);")
    early = once(early, "    drain_reschedule_leader(\n", "    leadership_guard.reset();\n    drain_reschedule_leader(\n")
    variants.append(("wake_early_release", early, placement))
    engage = "        leadership_guard.emplace();"
    claim = "        if (!try_claim_task_placement(subject)) return;"
    variants.append(("placement_missing", wake, once(placement, engage, "")))
    late = once(once(placement, engage, ""), claim, claim + "\n" + engage)
    variants.append(("placement_late", wake, late))
    scoped = once(placement, "    std::optional<PreemptGuard> leadership_guard;", "")
    scoped = once(scoped, "    auto commit = [&] {", "    auto commit = [&] {\n        std::optional<PreemptGuard> leadership_guard;")
    variants.append(("placement_early_release", wake, scoped))
    premature = once(placement, engage, "")
    premature = once(premature, "    if (subject->type == task::TaskType::PROCESS) {",
                     "    leadership_guard.emplace();\n    if (subject->type == task::TaskType::PROCESS) {")
    variants.append(("placement_before_mutex", wake, premature))
    bodies = []
    entries = []
    for name, wake_body, placement_body in variants:
        bodies.append("namespace " + name + " {\nusing namespace ker::mod::sched;\n" + common + "\n" + wake_body + "\n" + placement_body + "\n}\n")
        entries.append('{"' + name + '", ' + name + '::reschedule_task_for_cpu, ' + name + '::apply_task_placement_request}')
    result = fixture.replace("// ACTUAL_PREEMPT_INSERTION", guard + "\n" + "\n".join(preempt))
    result = result.replace("// ACTUAL_LEADERSHIP_INSERTION", "\n".join(bodies))
    result = result.replace("// PROFILE_INSERTION", ",\n".join(entries))
    return result, [item[0] for item in variants]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--cxx", default="clang++")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    root = args.source_root.resolve()
    scheduler = root / "modules/kern/src/platform/sched/scheduler.cpp"
    header = root / "modules/kern/src/platform/sched/scheduler.hpp"
    fixture = Path(__file__).with_name("reschedule_leader_preemption_cases.inc")
    worker = ROOT / "scripts/test/owned_command.py"
    spec = importlib.util.spec_from_file_location("reschedule_owned_command", worker)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    # clang++ is a multicall driver: resolving its symlink to clang changes
    # implicit C++ linkage. Retain argv[0]; pin() still reads the actual target.
    compiler = Path(shutil.which(args.cxx) or args.cxx).absolute()
    inputs = [Path(__file__).resolve(), scheduler, header, fixture, worker, compiler,
              root / "modules/kern/src/platform/sched/preemption_policy.hpp",
              root / "modules/kern/src/platform/sched/frame_class.hpp"]
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="wos-reschedule-result-"))
    if args.output:
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    started = time.monotonic()
    deadline = started + 145
    report = dict(kind="HOST_RESCHEDULE_LEADER_PREEMPTION", errors=[], commands=[], runtime_qualified=False)
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
        code, variants = generate(scheduler.read_text(), header.read_text(), fixture.read_text())
        cpp = scratch / "actual.cpp"
        binary = scratch / "check"
        cpp.write_text(code)
        def run(label, command, expected):
            nonlocal safe_to_prune
            receipt = output / (label + "-owned.json")
            with (output / (label + ".log")).open("wb") as log:
                sys.stdout.flush()
                sys.stderr.flush()
                saved_stdout, saved_stderr = os.dup(1), os.dup(2)
                try:
                    safe_to_prune = False
                    os.dup2(log.fileno(), 1)
                    os.dup2(log.fileno(), 2)
                    # This dedicated test has no other children between phases.
                    # The maintained API owns registration, original signal-mask
                    # restoration, pidfds, cancellation and sole forest reap.
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
            closed = (proof["leader_reaped"] and proof["owned_session_absent"] and proof["adopted_children_absent"]
                      and not proof["errors"] and not proof["timed_out"] and not proof["cancelled"])
            report["commands"].append(dict(label=label, expected_exit=expected, exit=code, closed=closed,
                                            receipt=pin(receipt)))
            if code != expected or proof["native_exit"] != expected or not closed:
                raise AssertionError(f"{label}: exit={code}, expected={expected}; see {output}")
            lines = (output / (label + ".log")).read_text().splitlines()
            if label == "actual":
                markers = [line for line in lines if re.fullmatch(
                    r"actual leadership/guard policy: [1-9]\d* checks, [1-9]\d* injected timers PASS", line)]
                if len(markers) != 1:
                    raise AssertionError("actual source did not emit its sole complete PASS marker")
            elif expected == -signal.SIGABRT:
                marker = ("leadership invariant: sleepable publication acquisition precedes new preemption protection"
                          if label == "placement_before_mutex" else
                          "leadership invariant: timer must not park the executor while it owns foreign reschedule leadership")
                if lines.count(marker) != 1:
                    raise AssertionError(f"{label}: SIGABRT did not prove the intended invariant")
        run("compile", [compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-fno-omit-frame-pointer",
                        "-fsanitize=address,undefined", "-fno-sanitize-recover=all", "-I" + str(root / "modules/kern/src"),
                        cpp, "-o", binary], 0)
        for name in variants:
            run(name, [binary, name], 0 if name == "actual" else -signal.SIGABRT)
    except BaseException as error:
        report["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        signal.pthread_sigmask(signal.SIG_BLOCK, cancellation_signals)
        # The maintained worker has settled every compiler/native forest before
        # returning. No native process receives these private scratch paths.
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
