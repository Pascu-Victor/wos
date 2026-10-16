#!/usr/bin/env python3
"""Exercise actual helper failure bodies on finite raw-syscall/clock/file ports.

No WOS process, child program, credentials, census provider or VM is executed.
The optional explicit predecessor supplies genuine old child/poll controls.
"""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import signal
import sys
import tempfile
import time

ROOT = next(path for path in Path(__file__).resolve().parents if (path / "modules/kern/src").is_dir())
WHOLE_SECONDS, COMMAND_SECONDS = 60, 20


def pin(path):
    invocation = path.absolute()
    alias_before = invocation.lstat()
    path = path.resolve(strict=True)
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    after = path.stat()
    alias_after = invocation.lstat()
    fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(before, field) != getattr(after, field) or
           getattr(alias_before, field) != getattr(alias_after, field) for field in fields):
        raise ValueError("input changed while hashing: " + str(path))
    return dict(path=str(invocation), resolved_path=str(path), bytes=after.st_size, sha256=digest.hexdigest(), dev=after.st_dev,
                inode=after.st_ino, mtime_ns=after.st_mtime_ns, ctime_ns=after.st_ctime_ns,
                invocation_inode=alias_after.st_ino, invocation_dev=alias_after.st_dev,
                invocation_mtime_ns=alias_after.st_mtime_ns, invocation_ctime_ns=alias_after.st_ctime_ns)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--predecessor", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source = (args.source or root / "tests/fixtures/numa_lifecycle_census.cpp").resolve(strict=True)
    shell = Path(__file__).with_suffix(".inc")
    helper_path = root / "tests/host/unit/file_mmap_registry_runtime_test.py"
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("census_helper_owned", helper_path)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)

    def capture(text, anchor, declaration=False):
        # The canonical boundary scanner mistakes numeric separators for char
        # literals. Normalize only its same-length search copy, never C++ bytes.
        search = re.sub(r"(?<=[0-9])'(?=[0-9])", "_", text)
        sample, row = owned.capture(search, anchor, declaration=declaration)
        start = sum(len(line) for line in text.splitlines(keepends=True)[:row["line"] - 1])
        body = text[start:start + len(sample)]
        row.update(bytes=len(body.encode()), sha256=hashlib.sha256(body.encode()).hexdigest())
        return body, row
    watched = [Path(__file__), source, shell, helper_path,
               root / "toolchain/src/mlibc/sysdeps/wos/include/sys/callnums.h"]
    selected_compiler = os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")
    compiler = Path(shutil.which(selected_compiler) or selected_compiler).absolute()
    watched.extend([compiler, compiler.resolve(strict=True)])
    if args.predecessor:
        watched.append(args.predecessor.resolve(strict=True))
    before = [pin(path) for path in watched]
    result = dict(pass_=False, errors=[], commands=[], runtime_qualified=False, source_held=False,
                  scratch_removed=False, preserved_scratch=None, whole_budget_seconds=WHOLE_SECONDS,
                  command_budget_seconds=COMMAND_SECONDS, scratch_budget_bytes=owned.SCRATCH_BUDGET,
                  log_budget_bytes=owned.LOG_BUDGET, file_budget_bytes=owned.FILE_BUDGET,
                  cancellation_limit="Canonical owner still ignores newly arriving signals during its short SIG_IGN reap interval")
    started = time.monotonic()
    deadline = started + WHOLE_SECONDS
    scratch, groups, dependencies, handlers = None, [], [], {}
    active, pending = False, None

    def interrupted(number, _):
        nonlocal pending
        if active:
            if pending is None:
                pending = number
            return
        raise KeyboardInterrupt(str(number))

    def check_pending():
        nonlocal pending
        if pending is not None:
            number, pending = pending, None
            raise KeyboardInterrupt("deferred signal " + str(number))

    def command(argv, label, expected):
        nonlocal active
        check_pending()
        active = True
        try:
            row = owned.command([str(word) for word in argv], scratch / (label + ".log"), scratch,
                                min(deadline, time.monotonic() + COMMAND_SECONDS), groups)
        finally:
            active = False
            check_pending()
        row.update(label=label, expected=expected)
        result["commands"].append(row)
        if row["exit"] != expected:
            raise ValueError(f"{label}: expected {expected}, got {row['exit']}")
        return scratch / (label + ".log")

    try:
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
        for number in handlers:
            signal.signal(number, interrupted)
        if args.evidence_dir:
            args.evidence_dir.mkdir(mode=0o700)
        owned.subreaper()
        active = True
        try:
            scratch = Path(tempfile.mkdtemp(prefix="wos-census-helper-", dir="/tmp"))
        finally:
            active = False
        check_pending()
        actual = source.read_text()
        extracted = []

        def take(anchor, declaration=False):
            body, row = capture(actual, anchor, declaration=declaration)
            extracted.append(row)
            return body

        callnums, row = capture(watched[4].read_text(), "enum class callnums :", declaration=True)
        extracted.append(row)
        storage = actual[actual.index("struct alignas(4096) Transports {"):actual.index("auto control(")]
        child = take("auto child(")
        poll = take("auto poll_census(")
        main_body = take("auto main(")
        begin = main_body.index("    int64_t warm_pid{}, measured_pid{};")
        end = main_body.index("    transport.begin = transport.output;", begin)
        branch = main_body[begin:end]
        result["extractions"] = extracted
        result["storage_source"] = dict(bytes=len(storage.encode()), sha256=hashlib.sha256(storage.encode()).hexdigest())
        result["caller_branch_source"] = dict(bytes=len(branch.encode()), sha256=hashlib.sha256(branch.encode()).hexdigest())
        ports = [("::lseek(", "::fixture::lseek("), ("::write(", "::fixture::write("),
                 ("::fsync(", "::fixture::fsync("), ("::clock_gettime(", "::fixture::clock_gettime("),
                 ("::nanosleep(", "::fixture::nanosleep("), ("::syscall(", "::fixture::census_syscall(")]

        def ported(body):
            for old, new in ports:
                body = body.replace(old, new)
            return body

        replacements = {
            "CALLNUMS": "namespace ker::abi {\n" + callnums + "\n}",
            "ACTUAL_STORAGE_AND_DIAGNOSTIC": ported(storage),
            "ACTUAL_CLOCK_AND_PREFAULT": ported("\n".join(take(anchor) for anchor in
                                            ["auto now_ns(", "auto pause_poll(", "auto time_left(", "void prefault_transport("])),
            "ACTUAL_CHILD": take("struct Child {", True),
            "ACTUAL_CHILD_CALL": child,
            "ACTUAL_CENSUS_CALL": ported(poll),
            "ACTUAL_WARM_BEGIN_BRANCH": branch,
        }
        generated = shell.read_text()
        for name, body in replacements.items():
            marker = "/* @" + name + "@ */"
            if generated.count(marker) != 1:
                raise ValueError("unique actual-body marker: " + name)
            generated = generated.replace(marker, body)
        variants = {"actual": generated}
        for name, old, new in [
            ("lost-child-status", ".wait_status = status,", ".wait_status = 0,"),
            ("stale-transport-header", '"census-transport", result, CENSUS_RESULT)',
             '"census-transport", result, CENSUS_RESULT | HEADER)'),
            ("write-after-failed-seek", "::fixture::lseek(files[4], 0, SEEK_SET) == 0", "true"),
        ]:
            if generated.count(old) != 1:
                raise ValueError("unique genuine control: " + name)
            variants[name] = generated.replace(old, new)
        if args.predecessor:
            old = args.predecessor.read_text()
            for label, anchor, current in [("old-child", "auto child(", child),
                                            ("old-census-poll", "auto poll_census(", ported(poll))]:
                body, provenance = capture(old, anchor)
                body = ported(body)
                if body == current or generated.count(current) != 1:
                    raise ValueError("actual predecessor differs at one whole body: " + label)
                variants[label] = generated.replace(current, body)
                result.setdefault("predecessor_extractions", []).append(dict(label=label, **provenance))
        flags = [str(compiler), "-std=c++23", "-DWOS_SELFTEST=1", "-O1", "-g0", "-fsanitize=address,undefined",
                 "-fno-omit-frame-pointer", "-I" + str(root / "modules/kern/src")]
        for label, content in variants.items():
            cpp, binary, dep = (scratch / (label + suffix) for suffix in [".cpp", "", ".d"])
            cpp.write_text(content)
            command(flags + ["-M", "-MF", dep, "-MT", "unit", cpp], label + "-dependencies", 0)
            discovered = [pin(Path(row["path"])) for row in owned.dependency_pins(dep)]
            dependencies.extend(discovered)
            command(flags + ["-MD", "-MF", dep, cpp, "-o", binary], label + "-compile", 0)
            emitted = [pin(Path(row["path"])) for row in owned.dependency_pins(dep)]
            if {row["path"] for row in emitted} != {row["path"] for row in discovered} or emitted != discovered:
                raise ValueError("compiled inputs differ from held discovery: " + label)
            result.setdefault("dependency_sets", []).append(dict(label=label, discovered=discovered, emitted=emitted))
            log = command([binary], label + "-run", 0 if label == "actual" else 1)
            if label != "actual" and "FAIL:" not in log.read_text():
                raise ValueError("control did not reach its actual assertion oracle: " + label)
            binary.unlink()
        result["pass_"] = True
    except BaseException as error:
        result["errors"].append(type(error).__name__ + ": " + str(error))
    finally:
        active = True
        try:
            try:
                result["inputs_before"] = before
                result["inputs_after"] = [pin(path) for path in watched]
                result["dependencies_before"] = dependencies
                result["dependencies_after"] = [pin(Path(row["path"])) for row in dependencies]
                result["source_held"] = before == result["inputs_after"] and dependencies == result["dependencies_after"]
                if not result["source_held"]:
                    result["errors"].append("source or actual emitted input changed")
                if args.evidence_dir and scratch:
                    for path in [*scratch.glob("*.cpp"), *scratch.glob("*.log")]:
                        shutil.copyfile(path, args.evidence_dir / path.name)
            except BaseException as error:
                result["errors"].append("input/evidence save: " + repr(error))
            result["pending_owned_groups"] = groups[:]
            if scratch:
                if groups:
                    result["preserved_scratch"] = str(scratch)
                    result["errors"].append("unproven owned group; preserve scratch")
                else:
                    try:
                        shutil.rmtree(scratch)
                        result["scratch_removed"] = not scratch.exists()
                    except BaseException as error:
                        result["preserved_scratch"] = str(scratch)
                        result["errors"].append("scratch cleanup: " + repr(error))
        finally:
            for number, handler in handlers.items():
                signal.signal(number, handler)
            active = False
        if pending is not None:
            result["errors"].append("deferred signal " + str(pending))
        result["seconds"] = time.monotonic() - started
        result["pass_"] &= not result["errors"] and result["source_held"] and result["scratch_removed"] and result["seconds"] < WHOLE_SECONDS
        if args.evidence_dir:
            (args.evidence_dir / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({key: result[key] for key in ["pass_", "errors", "seconds", "source_held", "scratch_removed"]}))
    return 0 if result["pass_"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
