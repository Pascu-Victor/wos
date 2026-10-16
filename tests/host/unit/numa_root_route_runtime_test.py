#!/usr/bin/env python3
"""Exercise actual root allocation/return routes with explicit Host endpoints.

The default source is the checkout containing this file. --source-overlay is a
finite read-only staging aid, not an alternate implementation of any function.
The physical/reserve/control-registry endpoints in the C++ fixture are modeled;
this test gives no native PMM, scheduler, TLB or pressure-attribution credit.
"""
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
import traceback

HERE = Path(__file__).resolve().parent
RELATIVE = (
    "modules/kern/src/platform/mm/virt.opt.cpp",
    "modules/kern/src/platform/mm/virt.hpp",
    "modules/kern/src/test/numa_pressure_runtime_ktest.cpp",
)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--cxx", required=True, help="the configured native C++ compiler")
parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[3])
parser.add_argument("--source-overlay", type=Path, help="JSON object: one of three extracted source paths -> absolute file")
parser.add_argument("--evidence-dir", type=Path, help="create a fresh directory for small logs and a receipt")
args = parser.parse_args()
R = args.source_root.resolve(strict=True)
overlay = json.loads(args.source_overlay.read_text()) if args.source_overlay else {}
if not isinstance(overlay, dict) or not set(overlay) <= set(RELATIVE):
    parser.error("overlay contains an unapproved source path")
if any(not isinstance(v, str) or not Path(v).is_absolute() for v in overlay.values()):
    parser.error("overlay files must be absolute paths")
cxx = Path(shutil.which(args.cxx) or args.cxx).resolve(strict=True)
if not cxx.is_file():
    parser.error("native compiler must be an existing file")
E = args.evidence_dir.resolve() if args.evidence_dir else None
if E:
    E.mkdir(parents=True, exist_ok=False)
T = Path(tempfile.mkdtemp(prefix="wos-root-route-"))
started = time.monotonic()
active, commands, errors = {}, [], []
inputs_before, inputs_after, emitted_before, emitted_after = [], [], [], []
registering, pending_signal = False, None
clean = False


def source_path(relative):
    return Path(overlay.get(relative, str(R / relative))).resolve(strict=True)


def pin(path):
    p = Path(path).resolve()
    a = p.stat()
    data = p.read_bytes()
    z = p.stat()
    if (a.st_dev, a.st_ino, a.st_size, a.st_mtime_ns, a.st_ctime_ns) != (z.st_dev, z.st_ino, z.st_size, z.st_mtime_ns, z.st_ctime_ns):
        raise RuntimeError("input drift: " + str(p))
    return dict(path=str(p), bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                device=z.st_dev, inode=z.st_ino, mtime_ns=z.st_mtime_ns, ctime_ns=z.st_ctime_ns)


def definition(text, marker, suffix=""):
    start = text.index(marker)
    first = text.index("{", start)
    depth, quote, line, block, i = 0, None, False, False, first
    while i < len(text):
        c, pair = text[i], text[i:i + 2]
        if line:
            if c == "\n":
                line = False
        elif block:
            if pair == "*/":
                block = False
                i += 1
        elif quote:
            if c == "\\":
                i += 1
            elif c == quote:
                quote = None
        elif pair == "//":
            line = True
            i += 1
        elif pair == "/*":
            block = True
            i += 1
        elif c in ('"', "'"):
            quote = c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                if suffix and text[i + 1:i + 1 + len(suffix)] != suffix:
                    raise RuntimeError("definition suffix: " + marker)
                return text[start:i + 1 + len(suffix)]
        i += 1
    raise RuntimeError("unterminated actual source: " + marker)


def absent(pid):
    try:
        os.killpg(pid, 0)
        return False
    except ProcessLookupError:
        return True


def stop(pid, process):
    if not absent(pid):
        os.killpg(pid, signal.SIGKILL)
    process.wait(timeout=5)
    if absent(pid):
        active.pop(pid, None)
    else:
        raise RuntimeError("owned Host group remains: " + str(pid))


def interrupted(signum, frame):
    global pending_signal
    if registering:
        pending_signal = signum
    else:
        raise KeyboardInterrupt(str(signum))


def run(label, argv, expected=0):
    global registering, pending_signal
    remaining = 120 - (time.monotonic() - started)
    size = sum(p.stat().st_blocks * 512 for p in T.rglob("*") if p.is_file())
    if remaining <= 0 or size > 128 * 1024**2 or shutil.disk_usage(T).free < 40 * 1024**3:
        raise RuntimeError("bounded Host time/storage budget")
    log = T / (label + ".log")
    at = time.monotonic()
    with log.open("wb") as stream:
        # Defer only the Python exception until the exact child is registered.
        # No signal mask is inherited by the compiler or the fixture.
        registering = True
        try:
            process = subprocess.Popen(list(map(str, argv)), cwd=R, stdout=stream,
                stderr=subprocess.STDOUT, start_new_session=True,
                env=dict(os.environ, TMPDIR=str(T), ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                         UBSAN_OPTIONS="halt_on_error=1"))
            active[process.pid] = process
        finally:
            registering = False
        try:
            if pending_signal is not None:
                signum, pending_signal = pending_signal, None
                raise KeyboardInterrupt(str(signum))
            code = process.wait(timeout=min(35, remaining))
        except BaseException:
            stop(process.pid, process)
            raise
    gone = absent(process.pid)
    if gone:
        active.pop(process.pid, None)
    data = log.read_text(errors="replace")
    commands.append(dict(label=label, argv=list(map(str, argv)), exit=code,
        expected_exit=expected, seconds=time.monotonic() - at, group_absent=gone,
        reaped=True, log=pin(log), output=data))
    if code != expected or not gone:
        raise RuntimeError(label + ": " + data[-6000:])
    if expected and ("FAIL " not in data or "ERROR: AddressSanitizer" in data or "runtime error:" in data):
        raise RuntimeError("negative must be a genuine logical assertion")


handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
try:
    for s in handlers:
        signal.signal(s, interrupted)
    cpp, header, fixture = map(source_path, RELATIVE)
    chosen = [Path(__file__), HERE / "numa_root_route_fixture.inc",
              HERE / "numa_root_route_shim/platform/sys/spinlock.hpp", HERE / "numa_root_conservation_cases.inc", cpp, header, fixture, cxx, R / "modules/kern/src/platform/mm/phys.hpp"]
    if args.source_overlay:
        chosen.append(args.source_overlay.resolve())
    inputs_before = [pin(p) for p in chosen]
    source, h, f = cpp.read_text(), header.read_text(), fixture.read_text()
    types = definition(h, "struct PageTablePoolStatsSnapshot", ";") + "\n" + h[h.index("enum class SelftestRootAllocationSource"):h.index("struct SelftestRootCensus")]
    pool_types = source[source.index("constexpr size_t PAGE_TABLE_POOL_CAPACITY"):source.index("constexpr size_t OWNED_FRAME_TABLE_CAPACITY")]
    pool = "\n\n".join(definition(source, marker) for marker in ["auto page_table_pool_proof_begin_locked()", "auto page_table_pool_proof_end(", "void selftest_observe_page_table_pool(", "void get_page_table_pool_stats_snapshot(", "void zero_page_table_for_pool(", "auto try_alloc_page_table_from_pool(",
                 "auto try_release_page_table_to_pool(", "auto alloc_zeroed_page_table("])
    roots = definition(source, "class CurrentRootSemanticOperation", ";") + "\n"
    roots += "\n\n".join(definition(source, marker) for marker in ["auto return_root_to_emergency_reserve(", "void selftest_capture_root_release(",
                  "void release_unregistered_user_root(", "auto create_user_root_common(", "auto create_pagemap()", "auto create_pagemap_may_fail()",
                  "auto selftest_create_pagemap(", "void release_pagemap_exclusive_owned(", "void release_pagemap(PageTable*", "void selftest_release_pagemap("])
    diagnostic = "\n".join(definition(f, marker, ";") for marker in ["enum class RootFrontier", "struct RootRoute {", "struct RootFrontierSnapshot", "struct RootRouteDiagnostics"])
    physical = (R / "modules/kern/src/platform/mm/phys.hpp").read_text()
    physical_types = physical[physical.index("struct PhysicalOwnerSnapshot"):physical.index("enum class PageLookupOwner")]
    physical_types += "\nusing NodeCounterSnapshot = node_counters::Snapshot;\n"
    predicates = "\n".join(definition(f, marker) for marker in ["auto post_clear_accounting_complete(", "auto root_retained_pool_conservation("])
    generated = (HERE / "numa_root_route_fixture.inc").read_text()
    generated = generated.replace("// WOS_TEST_INSERT_ACTUAL_PHYSICAL_TYPES", physical_types)
    generated = generated.replace("// WOS_TEST_INSERT_ACTUAL_CONSERVATION_PREDICATES", predicates)
    generated = generated.replace("// WOS_TEST_INSERT_CONSERVATION_CASES", (HERE / "numa_root_conservation_cases.inc").read_text())
    for marker, value in [("// WOS_TEST_INSERT_ACTUAL_OUTPUT_TYPES", types), ("// WOS_TEST_INSERT_ACTUAL_POOL_TYPES", pool_types), ("// WOS_TEST_INSERT_ACTUAL_POOL_FUNCTIONS", pool),
                          ("// WOS_TEST_INSERT_ACTUAL_ROOT_FUNCTIONS", roots), ("// WOS_TEST_INSERT_ACTUAL_DIAGNOSTIC_TYPES", diagnostic)]:
        if generated.count(marker) != 1: raise RuntimeError("fixture marker")
        generated = generated.replace(marker, value)
    main = T / "actual-root-branches.cpp"; main.write_text(generated)
    mutant1 = T / "mislabel-physical.cpp"
    needle = "*source = SelftestRootAllocationSource::PHYSICAL_ALLOCATOR;"
    if generated.count(needle) != 1: raise RuntimeError("origin mutation boundary")
    mutant1.write_text(generated.replace(needle, "*source = SelftestRootAllocationSource::LEGACY_POOL;"))
    mutant2 = T / "bypass-emergency-return.cpp"
    needle = "if (return_root_to_emergency_reserve(pagemap))"
    if generated.count(needle) != 2: raise RuntimeError("return mutation boundary")
    mutant2.write_text(generated.replace(needle, "if (false && return_root_to_emergency_reserve(pagemap))", 1))
    flags = [cxx, "-std=gnu++23", "-O1", "-DWOS_SELFTEST=1", "-DWOS_HOST_TEST=1",
             "-fsanitize=address,undefined", "-fno-sanitize-recover=all", "-fno-omit-frame-pointer",
             "-Wall", "-Wextra", "-Werror", "-I" + str(HERE / "numa_root_route_shim"),
             "-I" + str(R / "tests/host/kern_shim"), "-I" + str(R / "modules/kern/src"),
             "-I" + str(R / "modules/kern/src/std"), "-I" + str(R / "modules")]
    dep = T / "actual.d"
    run("deps", flags + ["-M", "-MF", dep, "-MT", "unit", main])
    deps = sorted({Path(p).resolve() for p in shlex.split(dep.read_text().replace("\\\n", " ").split(":", 1)[1])})
    emitted_before = [pin(p) for p in deps]
    extra_mutants = []
    for label, needle, replacement in [
        ("accept-saturated-receipt", "if (pool.proof_inflight == 0 || pool.proof_sequence >= UINT64_MAX - 1)", "if (pool.proof_inflight == 0 || pool.proof_sequence == UINT64_MAX)"),
        ("early-transfer-end", "    zero_page_table_for_pool(table);", "    (void)page_table_pool_proof_end(TRACKED);\n    zero_page_table_for_pool(table);"),
        ("ignore-generation", "p->sequence != returned.insertion.completed_sequence", "false"),
        ("ignore-membership", "p->matching_entries != 1", "false"),
        ("old-strict-closure", "check(virt::root_retained_pool_conservation(routes, before, after, a, b), \"exact retained owner accepted\");", "check(old_root_closure(a, b), \"exact retained owner accepted\");"),
    ]:
        if generated.count(needle) != 1: raise RuntimeError("genuine new mutation boundary " + label)
        path = T / (label + ".cpp"); path.write_text(generated.replace(needle, replacement)); extra_mutants.append((label,path,1))
    for label, src, expected in [("actual", main, 0), ("mislabel", mutant1, 1), ("bypass", mutant2, 1), *extra_mutants]:
        binary = T / label
        run("compile-" + label, flags + [src, "-pthread", "-o", binary])
        run("run-" + label, [binary], expected)
    emitted_after = [pin(p) for p in deps]
    inputs_after = [pin(p) for p in chosen]
    if emitted_before != emitted_after or inputs_before != inputs_after:
        raise RuntimeError("source/dependency drift")
except BaseException as error:
    errors.append(type(error).__name__ + ": " + str(error))
    (T / "failure.txt").write_text(traceback.format_exc())
finally:
    for s in handlers:
        signal.signal(s, signal.SIG_IGN)
    try:
        for pid, process in list(active.items()):
            try:
                stop(pid, process)
            except BaseException as error:
                errors.append("stop: " + str(error))
        if inputs_before and not inputs_after:
            try:
                inputs_after = [pin(p["path"]) for p in inputs_before]
                if inputs_before != inputs_after:
                    errors.append("source changed on failed run")
            except BaseException as error:
                errors.append("recheck: " + str(error))
        if E:
            try:
                for p in T.iterdir():
                    if p.is_file() and p.suffix in (".log", ".txt", ".cpp"):
                        shutil.copyfile(p, E / p.name)
            except BaseException as error:
                errors.append("evidence copy: " + str(error))
        size = sum(p.stat().st_blocks * 512 for p in T.rglob("*") if p.is_file())
        if not active:
            try:
                shutil.rmtree(T)
                clean = not T.exists()
            except BaseException as error:
                errors.append("scratch cleanup: " + str(error))
        if active or not clean:
            errors.append("preserved scratch: " + str(T))
        result = dict(qualification="PASS_ACTUAL_ROOT_ROUTE_HOST_ENDPOINTS" if not errors else "FAIL_HOST",
            errors=errors, commands=commands, inputs_before=inputs_before, inputs_after=inputs_after,
            emitted_before=emitted_before, emitted_after=emitted_after, seconds=time.monotonic() - started,
            scratch_removed=clean, scratch_allocated_bytes_removed=size, pending_groups=list(active),
            preserved_scratch=str(T) if not clean else None,
            scope="Actual root/pool/factory/return bodies; explicit physical, emergency, control registry, CPU/CR3 and lock Host endpoints. No PMM pressure, native, SMP or causal attribution.")
        if E:
            (E / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({k: result[k] for k in ("qualification", "errors", "seconds", "scratch_removed")}))
    finally:
        for s, old in handlers.items():
            signal.signal(s, old)
raise SystemExit(bool(errors) or bool(active) or not clean)
