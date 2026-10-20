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


def generate_public_policy(actual, take, shell, callnums):
    """Capture actual guest bodies; endpoints disclose finite Host-only authority."""
    types = actual[actual.index("struct Selection {"):actual.index("// These POD transport bytes")]
    types += actual[actual.index("enum ClockValid :"):actual.index("auto now_ns(")]
    types += take("struct OwnResources {", True)
    types += actual[actual.index("constexpr uint32_t POLICY_MAX_PAGES"):actual.index("auto policy_options(")]
    template = actual[actual.index("template <class T>"):actual.index("enum ClockValid :")]
    helpers = template + "\n".join(take(anchor) for anchor in [
        "auto number(", "auto now_ns(", "auto deadline_after(", "auto before(", "auto raw_error(", "auto query(",
        "auto page_nodes(", "auto set_thread_cpu(", "auto place_thread(", "auto await_cpu(", "auto pin_current_cpu(",
        "auto cpu_still(", "auto map_private(", "auto wait_flag(", "auto nonblocking(", "auto transfer(",
        "auto wait_child(", "auto terminate_and_reap(", "auto close_fd("])
    functions = "\n".join(take(anchor) for anchor in [
        "auto policy_options(", "auto policy_read(", "auto policy_set(", "auto policy_matches(", "auto policy_topology(",
        "auto policy_expected(", "auto policy_change(", "auto policy_mapping(", "void* policy_worker(", "auto policy_usage(", "auto policy_child(",
        "auto policy_pipe(", "auto policy_fork(", "void policy_print(", "auto run_numa_policy("])
    def ports(body):
        body = body.replace("::syscall(", "fixture::resource_call(").replace("std::printf(", "fixture::printf(")
        for name in ("clock_gettime", "mmap", "munmap", "getpid", "getrlimit", "setrlimit", "pipe", "close", "fcntl",
                     "read", "write", "poll", "waitpid", "kill", "pthread_create", "pthread_join"):
            body = re.sub(r"(?<![\w:])" + name + r"\(", "fixture::" + name + "(", body)
        body = body.replace("pid = fork();", "pid = fixture::fork_policy(opts, topo, selected, mode, mask, quota_only, expected_limit, fds[1], deadline);")
        return body.replace("_exit(", "fixture::exit(")
    generated = shell.read_text()
    for marker, value in (("ACTUAL_CALLNUMS", "namespace ker::abi {\n" + callnums + "\n}"),
                          ("ACTUAL_TYPES", types), ("ACTUAL_HELPERS", ports(helpers)), ("ACTUAL_POLICY_FUNCTIONS", ports(functions))):
        token = "/* @" + marker + "@ */"
        if generated.count(token) != 1:
            raise ValueError("unique actual public-policy marker: " + marker)
        generated = generated.replace(token, value)
    variants = {"actual": generated}
    for label, old, new in [
        ("lost-absolute-interleave", "members[(address / PAGE) % count]", "members[((address / PAGE) + 1) % count]"),
        ("lost-thread-inheritance", "inherited.mode != worker.mode || inherited.nodes != worker.mask", "false"),
        ("lost-owned-unmap", "int const RET = fixture::munmap(mapping, static_cast<size_t>(opts.pages) * PAGE);", "int const RET = 0;"),
    ]:
        if generated.count(old) != 1:
            raise ValueError("unique actual public-policy control: " + label)
        variants[label] = generated.replace(old, new)
    return generated, variants


def generate_lifecycle_placement(actual, take, shell):
    """Actual handshake/parent frontiers on explicit finite syscall endpoints."""
    types = actual[actual.index("constexpr size_t PAGE"):actual.index("// These POD transport bytes")]
    types += actual[actual.index("enum ClockValid :"):actual.index("auto now_ns(")]
    types += take("struct ChildRecord {", True) + "\n" + take("enum class ChildPhase :", True)
    types += actual[actual.index("constexpr uint64_t CHILD_READY_MAGIC"):actual.index("struct ChildReady {")]
    types += take("struct ChildReady {", True)
    template = actual[actual.index("template <class T>"):actual.index("enum ClockValid :")]
    helpers = template + "\n".join(take(anchor) for anchor in [
        "auto now_ns(", "auto before(", "auto raw_error(", "auto query(", "auto self_policy(", "auto valid_selection(",
        "auto transfer(", "auto set_thread_cpu(", "auto await_cpu(", "auto lifecycle_child_target(", "auto valid_child_ready(",
        "auto child_ready_rejections(", "void report_ready_failure(",
        "auto child_ready_and_cycle(", "auto receive_child_ready(", "auto terminate_and_reap("])
    caller = take("auto run_numa_lifecycle(")
    begin = caller.index("        ReadyObservation readiness{};")
    end = caller.index('            phase = "parent_pre_copyout";', begin)
    # Keep the whole completed-record validation; omit only its next payload if.
    branch = caller[begin:end]
    suffix = "        if (ok) {\n"
    if not branch.endswith(suffix):
        raise ValueError("exact actual placement-to-payload frontier")
    branch = branch[:-len(suffix)]
    def ports(body):
        body = body.replace("std::fprintf(", "fixture::fprintf(")
        for name in ("clock_gettime", "getpid", "read", "write", "poll", "waitpid", "kill"):
            body = re.sub(r"(?<![\w:])" + name + r"\(", "fixture::" + name + "(", body)
        return body
    generated = shell.read_text()
    for name, value in (("ACTUAL_TYPES", types), ("ACTUAL_HANDSHAKE", ports(helpers)), ("ACTUAL_PARENT_BRANCH", ports(branch))):
        marker = "/* @" + name + "@ */"
        if generated.count(marker) != 1:
            raise ValueError("unique actual placement marker: " + name)
        generated = generated.replace(marker, value)
    variants = {"actual": generated}
    for label, old, new in [
        ("lost-inherited-mask", "ready.inherited_mask == (uint64_t{1} << parent.cpu)", "true"),
        ("lost-controller-placement", "int64_t const PLACE = set_thread_cpu(static_cast<uint64_t>(pid), target.cpu, deadline);", "int64_t const PLACE = 0;"),
        ("lost-child-placement-wait", "int64_t const PLACED = await_cpu(target.cpu, deadline);", "int64_t const PLACED = 0;"),
        ("lost-cycle-alternation", "if ((cycle & 1U) == 0 || topo.selected_count < 2)", "if ((cycle & 1U) != 0 || topo.selected_count < 2)"),
        ("lost-ready-errno-validity", "if (n < 0) observation->valid |= READY_IO_ERRNO;", "observation->valid |= READY_IO_ERRNO;"),
        ("lost-ready-immediate-errno", "observation->io_error = n < 0 ? errno : 0;", "observation->io_error = 0;"),
    ]:
        if generated.count(old) != 1:
            raise ValueError("unique actual placement mutation: " + label)
        variants[label] = generated.replace(old, new)
    return generated, variants


def generate_pressure(actual, take, shell, callnums, wrappers):
    """Actual guest algorithm and actual raw wrappers; finite explicit Host ports."""
    types = actual[actual.index("struct Selection {"):actual.index("// These POD transport bytes")]
    types += actual[actual.index("enum ClockValid :"):actual.index("auto now_ns(")]
    types += actual[actual.index("constexpr uint64_t PRESSURE_MAX_BYTES"):actual.index("uint64_t pressure_hash(")]
    template = actual[actual.index("template <class T>"):actual.index("enum ClockValid :")]
    helpers = template + "\n".join(take(anchor) for anchor in [
        "auto number(", "auto now_ns(", "auto deadline_after(", "auto before(", "auto raw_error(", "auto query(", "auto page_nodes(",
        "auto await_cpu(", "auto pin_current_cpu(", "auto policy_read(", "auto policy_set(", "auto policy_matches(", "auto policy_topology(", "auto policy_usage("])
    functions = "\n".join(take(anchor) for anchor in [
        "uint64_t pressure_hash(", "void pressure_prefault_stack(", "auto pressure_options(", "void pressure_error(", "auto pressure_remaining(", "auto pressure_checkpoint(",
        "auto pressure_verify(", "auto pressure_allocate(", "void pressure_release(", "auto pressure_snapshot(", "auto pressure_write(",
        "void pressure_report(", "auto run_numa_pressure("])
    def ports(body):
        body = body.replace("::syscall(", "fixture::resource_call(").replace("ker::multiproc::", "fixture::")
        for name in ("clock_gettime", "getpid", "getrlimit", "open", "close", "read", "write"):
            body = re.sub(r"(?<![\w:])" + name + r"\(", "fixture::" + name + "(", body)
        return body
    generated = shell.read_text()
    for marker, value in (("ACTUAL_CALLNUMS", "namespace ker::abi {\n" + callnums + "\n}"),
                          ("ACTUAL_PRESSURE_TYPES", types), ("ACTUAL_RAW_WRAPPERS", wrappers),
                          ("ACTUAL_PRESSURE_HELPERS", ports(helpers)), ("ACTUAL_PRESSURE_FUNCTIONS", ports(functions))):
        token = "/* @" + marker + "@ */"
        if generated.count(token) != 1:
            raise ValueError("unique actual pressure marker: " + marker)
        generated = generated.replace(token, value)
    variants = {"actual": generated}
    for name, old, new in [
        ("lost-preferred-foreign", "(foreign ? row.node == state.target : row.node != state.target)", "(foreign ? false : row.node != state.target)"),
        ("lost-bind-node", "(foreign ? row.node == state.target : row.node != state.target)", "(foreign ? row.node == state.target : false)"),
        ("lost-recovery-failure", "if(rc==0)rc=pressure_allocate(s,PAGE,probe_deadline,false,s.recovery_rc,sentinel);", "if(rc==0){(void)pressure_allocate(s,PAGE,probe_deadline,false,s.recovery_rc,sentinel);rc=0;}"),
        ("lost-as-charge", "u.current == state.baseline.current + state.owned_bytes", "true"),
        ("lost-owned-unmap", "entry.release_rc = ker::vmem::free(reinterpret_cast<void*>(entry.address), entry.bytes);", "entry.release_rc = 0;"),
        ("lost-cap-error", "return -E2BIG;", "return -ENOMEM;"),
        ("lost-deadline", "return before(bound) && before(state.deadline);", "return true;"),
        ("lost-output-sentinel", "return signed_result; // Return error code", "*addr = nullptr; return signed_result; // actual raw output control"),
    ]:
        tokens = re.findall(r"[A-Za-z_][A-Za-z_0-9]*|[0-9]+|[^\s]", old)
        pattern = r"(?<![A-Za-z_0-9])" + r"\s*".join(re.escape(token) for token in tokens) + r"(?![A-Za-z_0-9])"
        if len(re.findall(pattern, generated)) != 1:
            raise ValueError("unique actual pressure control: " + name)
        variants[name] = re.sub(pattern, lambda _: new, generated)
    return generated, variants


def generate_public_huge(actual, take, shell, callnums, wrappers, ptrace_types, predecessor):
    """Actual critical guest helpers over explicit finite syscall/clock ports.

    Actual entry/setup/final gates execute over finite endpoints; real phase workloads,
    children/permissions/PTEs do not execute. The old entry is a genuine source control.
    """
    types = actual[actual.index("struct Selection {"):actual.index("// These POD transport bytes")]
    types += take("struct OwnResources {", True)
    types += actual[actual.index("enum ClockValid :"):actual.index("auto now_ns(")]
    types += actual[actual.index("constexpr uint64_t HUGE_BYTES"):actual.index("// END public huge types.")]
    functions = take("template <class T>") + "\n" + "\n".join(take(anchor) for anchor in [
        "auto number(", "auto now_ns(", "auto deadline_after(", "auto before(", "auto raw_error(", "auto close_fd(",
        "auto transfer(", "auto terminate_and_reap(", "auto huge_hold_until(",
        "auto huge_options(", "auto huge_admit(", "auto huge_release(", "auto huge_expected(", "auto huge_serve(", "auto huge_wait(",
        "void report_huge_transfer(", "void report_huge_child_failure(",
        "auto huge_trace(", "auto huge_ptrace_io(", "auto run_numa_huge("])
    # Execute the complete actual post-fork parent branch, including serve, wait,
    # exact cleanup and final reporting. Only its already-created child/pipe
    # context is finite; the child branch and real fork are not executed.
    parent = take("auto huge_child_case(")
    anchor = "    HugeChildObservation observation{};\n"
    if parent.count(anchor) != 1:
        raise ValueError("one actual huge parent observation frontier")
    branch = parent[parent.index(anchor):]
    functions += ("\nauto huge_parent_case(HugeState& s, uint32_t kind, pid_t pid, bool owned, bool attached, "
                  "int (&ready)[2], int (&commands)[2], bool ok, HugeMapping& m) -> bool {\n" + branch)
    if predecessor.count("auto run_numa_huge(") != 1:
        raise ValueError("one genuine predecessor entry definition")
    functions += "\n" + predecessor.replace("auto run_numa_huge(", "auto run_numa_huge_predecessor(")
    functions = functions.replace("std::fprintf(", "fixture::fprintf(")
    for name in ("clock_gettime", "clock_nanosleep", "read", "write", "poll", "kill", "munmap", "waitpid", "close", "getpid", "getrlimit", "policy_read", "policy_set",
                 "policy_matches", "policy_topology", "pin_current_cpu", "huge_bench", "huge_aliases", "huge_mutations",
                 "huge_child_case", "huge_usercopy", "huge_windows", "huge_payload", "huge_byte", "huge_changed"):
        functions = re.sub(r"(?<![\w:])" + name + r"\(", "fixture::" + name + "(", functions)
    functions = functions.replace("ker::multiproc::", "fixture::")
    functions = functions.replace("::syscall(", "fixture::process_call(")
    generated = shell.read_text()
    for key, value in (("ACTUAL_CALLNUMS", "namespace ker::abi {\n" + callnums + "\n}"),
                       ("ACTUAL_PTRACE_TYPES", ptrace_types), ("ACTUAL_HUGE_TYPES", types),
                       ("ACTUAL_HUGE_WRAPPERS", wrappers), ("ACTUAL_HUGE_FUNCTIONS", functions)):
        marker = "/* @" + key + "@ */"
        if generated.count(marker) != 1:
            raise ValueError("unique actual huge marker: " + key)
        generated = generated.replace(marker, value)
    variants = {"actual": generated}
    for label, old, new in [
        ("lost-owned-unmap", "if (fixture::munmap(reinterpret_cast<void*>(interval.address), interval.bytes) == 0)", "if (true)"),
        ("wrong-huge-interleave", "members[(address / HUGE_BYTES) % count]", "members[(address / PAGE) % count]"),
        ("lost-stop-status", "stop ? STOPPED && WSTOPSIG(status) == SIGSTOP", "stop ? true"),
        ("ignored-ptrace-count", "return RC == 0 && io.transferred == buffer.size();", "return RC == 0;"),
        ("lost-huge-ack-observation", "observation != nullptr ? &observation->ack : nullptr", "nullptr"),
        ("lost-huge-precleanup-owner", "observation.precleanup_owned = owned;", "observation.precleanup_owned = false;"),
    ]:
        if generated.count(old) != 1:
            raise ValueError("unique genuine huge control: " + label)
        variants[label] = generated.replace(old, new)
    return generated, variants


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--public-huge", action="store_true", help="actual huge guest critical helpers on explicit finite ports")
    parser.add_argument("--pressure-only", action="store_true", help="exercise actual public pressure workload on finite raw ABI ports")
    parser.add_argument("--workload-wait", action="store_true", help="exercise failure-only testprog wait observations")
    parser.add_argument("--workload-placement", action="store_true", help="exercise actual lifecycle ready/controller placement on finite endpoints")
    parser.add_argument("--public-policy", action="store_true", help="exercise actual healthy public-policy guest bodies on finite ports")
    parser.add_argument("--predecessor", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    if sum((args.workload_wait, args.public_policy, args.workload_placement, args.pressure_only, args.public_huge)) > 1:
        parser.error("select one explicit profile")
    if args.public_huge and args.predecessor is None:
        parser.error("public-huge entry regression requires the explicit genuine predecessor source")
    root = args.root.resolve(strict=True)
    source = (args.source or root / ("modules/testprog/src/numa_locality.cpp" if args.workload_wait or args.public_policy or args.workload_placement or args.pressure_only or args.public_huge else "tests/fixtures/numa_lifecycle_census.cpp")).resolve(strict=True)
    shell = (Path(__file__).with_name("numa_huge_cases.inc") if args.public_huge else
             Path(__file__).with_name("numa_pressure_cases.inc") if args.pressure_only else
             Path(__file__).with_name("numa_lifecycle_placement_test.inc") if args.workload_placement else
             Path(__file__).with_name("numa_policy_cases.inc") if args.public_policy else
             Path(__file__).with_name("numa_lifecycle_wait_diagnostics_test.inc") if args.workload_wait else Path(__file__).with_suffix(".inc"))
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
    if not any((args.workload_wait, args.public_policy, args.workload_placement, args.pressure_only, args.public_huge)):
        watched.append(root / "toolchain/src/mlibc/sysdeps/wos/include/sys/multiproc.h")
    libc_wait = root / "toolchain/src/mlibc/options/posix/generic/sys-wait.cpp"
    if args.workload_wait:
        watched.append(libc_wait)
    if args.public_policy:
        watched.append(Path(__file__).resolve().parents[2] / "fixtures/validate_numa_policy.py")
    if args.pressure_only:
        watched.extend([root / "toolchain/src/mlibc/sysdeps/wos/include/sys/vmem.h",
                        Path(__file__).resolve().parents[2] / "fixtures/validate_numa_pressure.py"])
    if args.public_huge:
        watched.extend([root / "toolchain/src/mlibc/sysdeps/wos/include/sys/vmem.h",
                        root / "toolchain/src/mlibc/sysdeps/wos/include/sys/wos-ptrace.h",
                        root / "toolchain/src/mlibc/abis/wos/vm-flags.h"])
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

        if args.public_huge:
            callnums, row = capture(watched[4].read_text(), "enum class callnums :", declaration=True)
            extracted.append(row)
            raw = (root / "toolchain/src/mlibc/sysdeps/wos/include/sys/vmem.h").read_text()
            wrappers = []
            for anchor in ("static inline int64_t\nallocate(", "static inline int64_t free("):
                body, row = capture(raw, anchor)
                extracted.append(row)
                wrappers.append(body.replace("syscall(", "fixture::vmem_call("))
            ptrace_types = (root / "toolchain/src/mlibc/sysdeps/wos/include/sys/wos-ptrace.h").read_text()
            predecessor, predecessor_row = capture(args.predecessor.read_text(), "auto run_numa_huge(")
            result["predecessor_extractions"] = [predecessor_row]
            generated, variants = generate_public_huge(actual, take, shell, callnums, "\n".join(wrappers), ptrace_types, predecessor)
            result["extractions"] = extracted
            result["profile"] = "public-huge-actual-critical-guest-helpers-finite-ports"
            result["endpoint_limits"] = "Actual full entry/setup/restoration/final gates and complete post-fork parent serve/terminal-wait/cleanup/report branch over finite clock/pipe/wait/kill/policy/affinity/limit/workload endpoints; genuine old entry fails. Real fork/child phase workloads/kernel/PTE/PS/permissions/AS/physical closure not executed"
        elif args.pressure_only:
            callnums, row = capture(watched[4].read_text(), "enum class callnums :", declaration=True)
            extracted.append(row)
            raw = (root / "toolchain/src/mlibc/sysdeps/wos/include/sys/vmem.h").read_text()
            wrappers = []
            for anchor in ("static inline int64_t\nallocate(", "static inline int64_t free(", "static inline int64_t query_numa(", "static inline int64_t set_memory_policy("):
                body, row = capture(raw, anchor)
                extracted.append(row)
                wrappers.append(body.replace("syscall(", "fixture::vmem_call("))
            generated, variants = generate_pressure(actual, take, shell, callnums, "\n".join(wrappers))
            result["extractions"] = extracted
            result["profile"] = "public-pressure-actual-guest-and-raw-wrappers-finite-ports"
            result["endpoint_limits"] = "Finite ABI, mapped bytes, AS and snapshot bytes; no physical allocator, PMM exhaustion or runtime snapshot authority"
        elif args.workload_placement:
            generated, variants = generate_lifecycle_placement(actual, take, shell)
            result["extractions"] = extracted
            result["profile"] = "lifecycle-placement-actual-frontiers-finite-endpoints"
            result["endpoint_limits"] = "Finite clock/pipe/affinity/consuming cleanup and unchanged payload endpoint; no WOS Task, concurrent scheduler or native authority"
        elif args.public_policy:
            callnums, row = capture(watched[4].read_text(), "enum class callnums :", declaration=True)
            extracted.append(row)
            generated, variants = generate_public_policy(actual, take, shell, callnums)
            result["extractions"] = extracted
            result["profile"] = "public-policy-actual-guest-bodies-finite-endpoints"
            result["endpoint_limits"] = "Finite per-Task policy, mapped-byte residency, AS, cooperative placement and child transport; no WOS runtime or physical allocation"
        elif args.workload_wait:
            storage = actual[actual.index("enum ClockValid :"):actual.index("auto now_ns(")]
            caller = take("auto run_numa_lifecycle(")
            begin = caller.index("        int status = -1;")
            end = caller.index("        ++completed;", begin)
            branch = caller[begin:end]
            anchor = "        if (!ok || !reaped || owned || !unmapped || !fds_absent || !mapping_absent || own.fds || own.maps || !own.complete) {"
            if branch.count(anchor) != 1:
                raise ValueError("unique actual postwait failure predicate")
            captured_branch = branch.replace(anchor, """        fixture::capture(status, wait_observation, WAIT_OWNED, READ_CLOSED, CLEANUP_REAP_NEEDED, reaped, owned,
                                 unmapped, fds_absent, MAPPING_QUERY_VALID, MAPPING_QUERY, mapping_absent, own, cleanup_observation);
        """ + anchor)
            readiness_declaration = "        ReadyObservation readiness{};\n"
            if caller.count(readiness_declaration) != 1:
                raise ValueError("unique actual readiness default for unchanged postwait carrier")
            captured_branch = readiness_declaration + captured_branch
            result["caller_branch_source"] = dict(bytes=len(branch.encode()), sha256=hashlib.sha256(branch.encode()).hexdigest())
            result["host_only_capture_insertion"] = anchor
            ports = [("clock_gettime(", "fixture::clock_gettime("), ("waitpid(", "fixture::posix_waitpid("),
                     ("kill(", "fixture::kill("), ("close(", "fixture::close("), ("fcntl(", "fixture::fcntl("),
                     ("munmap(", "fixture::munmap("), ("page_nodes(", "fixture::page_nodes("),
                     ("std::fprintf(", "fixture::fprintf(")]
            def port_wait(body):
                for old, new in ports:
                    body = body.replace(old, new)
                return body
            libc, row = capture(libc_wait.read_text(), "pid_t waitpid(")
            result["libc_frontend_extraction"] = row
            libc = libc.replace("mlibc::sysdep_or_enosys<Waitpid>", "fixture::sysdep_waitpid")
            predecessor = "#define HAVE_WAIT_PREDECESSOR 0\n"
            if args.predecessor:
                old = args.predecessor.read_text()
                bodies = []
                for name in ["auto now_ns(", "auto before(", "auto wait_child(", "auto terminate_and_reap("]:
                    body, row = capture(old, name)
                    bodies.append(port_wait(body))
                    result.setdefault("predecessor_extractions", []).append(row)
                predecessor = "#define HAVE_WAIT_PREDECESSOR 1\nnamespace predecessor {\n" + "\n".join(bodies) + "\n}\n"
            replacements = {
                "ACTUAL_STORAGE": storage,
                "ACTUAL_QUERY_BUFFERS": take("struct QueryBuffers {", True),
                "ACTUAL_CHILD_RECORD": take("struct ChildRecord {", True),
                "ACTUAL_OWN_RESOURCES": take("struct OwnResources {", True),
                "ACTUAL_LIBC_WAITPID": libc,
                "ACTUAL_CLOCK_WAIT_CLEANUP": port_wait("\n".join(take(name) for name in
                    ["auto now_ns(", "auto before(", "auto wait_child(", "auto terminate_and_reap(", "auto close_fd(", "auto confirm_closed(", "void report_ready_failure("])),
                "ACTUAL_PREDECESSOR": predecessor,
                "ACTUAL_POSTWAIT": port_wait(captured_branch),
            }
            generated = shell.read_text()
            for name, body in replacements.items():
                marker = "/* @" + name + "@ */"
                if generated.count(marker) != 1:
                    raise ValueError("unique actual wait marker: " + name)
                generated = generated.replace(marker, body)
            result["extractions"] = extracted
            result["profile"] = "workload-wait-actual-body-ports"
            variants = {"actual": generated}
            for name, old, new in [
                ("lost-status-validity", "observation.status_valid = rc == pid;", "observation.status_valid = true;"),
                ("lost-immediate-errno", "int const ERROR = rc < 0 ? errno : 0;", "int const ERROR = 0;"),
                ("lost-deadline-outcome", "WaitOutcome::DEADLINE : WaitOutcome::CLOCK_ERROR", "WaitOutcome::WAIT_ERROR : WaitOutcome::CLOCK_ERROR"),
                ("lost-cleanup-kill-result", "observation->kill_result = KILL_RESULT;", "observation->kill_result = 0;"),
                ("lost-cleanup-kill-errno", "observation->kill_error = KILL_ERROR;", "observation->kill_error = 0;"),
                ("lost-cleanup-wait-errno", "observation->wait_error = WAIT_ERROR;", "observation->wait_error = 0;"),
                ("lost-cleanup-status-validity", "observation->status_valid = rc == pid;", "observation->status_valid = true;"),
                ("lost-cleanup-status", "observation->status = observation->status_valid ? status : 0;", "observation->status = 0;"),
            ]:
                if generated.count(old) != 1:
                    raise ValueError("unique actual wait control: " + name)
                variants[name] = generated.replace(old, new)
        else:
            callnums, row = capture(watched[4].read_text(), "enum class callnums :", declaration=True)
            extracted.append(row)
            storage = actual[actual.index("struct alignas(4096) Transports {"):actual.index("auto control(")]
            child = take("auto child(")
            poll = take("auto poll_census(")
            main_body = take("auto main(")
            begin = main_body.index("    int64_t warm_pid{}, measured_pid{};")
            end = main_body.index("    transport.begin = transport.output;", begin)
            branch = main_body[begin:end]
            both_end = main_body.index("    transport.end = transport.output;", end) + len("    transport.end = transport.output;")
            both_branch = main_body[begin:both_end]
            startup = main_body[main_body.index("    (void)::umask(0077);"):begin]
            if startup.count("    prefault_transport();") != 1:
                raise ValueError("one actual startup prefault observation")
            startup = startup.replace("    prefault_transport();", "    fixture::main_prefault();\n    prefault_transport();")
            startup = startup.replace("::umask(", "::fixture::umask(").replace("prepare_files()", "fixture::prepare_files()")
            endpoint = actual[actual.index("constexpr size_t ENDPOINT_MAX_OBJECTS"):actual.index("auto prepare_files(")]
            endpoint = endpoint.replace("::fsync(", "::fixture::endpoint_fsync(").replace("::dl_iterate_phdr(", "::fixture::iterate_phdr(")
            result["extractions"] = extracted
            result["storage_source"] = dict(bytes=len(storage.encode()), sha256=hashlib.sha256(storage.encode()).hexdigest())
            result["caller_branch_source"] = dict(bytes=len(branch.encode()), sha256=hashlib.sha256(branch.encode()).hexdigest())
            ports = [("::lseek(", "::fixture::lseek("), ("::write(", "::fixture::write("),
                     ("::fsync(", "::fixture::fsync("), ("::clock_gettime(", "::fixture::clock_gettime("),
                     ("::nanosleep(", "::fixture::nanosleep("), ("::syscall(", "::fixture::census_syscall("),
                     ("ker::multiproc::", "fixture::")]

            def ported(body):
                for old, new in ports:
                    body = body.replace(old, new)
                return body

            replacements = {
                "CALLNUMS": "namespace ker::abi {\n" + callnums + "\n}",
                "ACTUAL_STORAGE_AND_DIAGNOSTIC": ported(storage),
                "ACTUAL_CLOCK_AND_PREFAULT": ported("\n".join(take(anchor) for anchor in
                                                ["auto now_ns(", "auto pause_poll(", "auto time_left(", "void prefault_transport("])),
                "ACTUAL_SUPERVISOR_PLACEMENT": ported("\n".join(take(anchor) for anchor in
                                                               ["auto supervisor_still_here(", "auto pin_supervisor("])),
                "ACTUAL_STARTUP_BRANCH": startup,
                "ACTUAL_ENDPOINT_PREPARATION": ported(endpoint),
                "ACTUAL_CHILD": take("struct Child {", True),
                "ACTUAL_CHILD_CALL": child,
                "ACTUAL_CENSUS_CALL": ported(poll),
                "ACTUAL_WARM_BEGIN_BRANCH": branch,
                "ACTUAL_BOTH_ENDPOINT_BRANCH": both_branch,
            }
            generated = shell.read_text()
            for name, body in replacements.items():
                marker = "/* @" + name + "@ */"
                if generated.count(marker) != 1:
                    raise ValueError("unique actual-body marker: " + name)
                generated = generated.replace(marker, body)
            variants = {"actual": generated}
            for name, old, new in [
                ("omit-endpoint-fsync", "::fixture::endpoint_fsync(fd) != 0", "false"),
                ("lost-child-status", ".wait_status = status,", ".wait_status = 0,"),
                ("stale-transport-header", '"census-transport", result, CENSUS_RESULT)',
                 '"census-transport", result, CENSUS_RESULT | HEADER)'),
                ("write-after-failed-seek", "::fixture::lseek(files[4], 0, SEEK_SET) == 0", "true"),
                ("lost-supervisor-main-pin", 'if (!pin_supervisor()) return finish_failure(73, "supervisor-placement");',
                 'if (false) return finish_failure(73, "supervisor-placement");'),
                ("lost-supervisor-mask-readback",
                 "static_cast<uint64_t>(fixture::getThreadAffinityMask(diagnostic_parent)) == MASK", "true"),
                ("lost-supervisor-busy-recapture", "continue;  // Recapture the current CPU; never request a remote self move.",
                 "return true;  // Finite control: abandon the required EBUSY recapture."),
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
            if args.public_policy and label == "actual":
                decoder_path = Path(__file__).resolve().parents[2] / "fixtures/validate_numa_policy.py"
                decoder_spec = importlib.util.spec_from_file_location("saved_public_policy", decoder_path)
                decoder = importlib.util.module_from_spec(decoder_spec)
                decoder_spec.loader.exec_module(decoder)
                text = log.read_text()
                result["saved_only_decodes"] = []
                for prefix, domains in (("SAVED_POLICY", 2), ("SAVED_UMA", 1)):
                    begin, end = prefix + "_BEGIN\n", prefix + "_END\n"
                    if text.count(begin) != 1 or text.count(end) != 1:
                        raise ValueError("one actual generated guest-output interval")
                    sample = text.split(begin)[1].split(end)[0]
                    result["saved_only_decodes"].append(decoder.decode(sample, 19601, domains, 4))
            binary.unlink()
        if args.pressure_only:
            decoder_path = Path(__file__).resolve().parents[2] / "fixtures/validate_numa_pressure.py"
            decoder_spec = importlib.util.spec_from_file_location("saved_pressure_schema", decoder_path)
            decoder = importlib.util.module_from_spec(decoder_spec)
            decoder_spec.loader.exec_module(decoder)
            result["saved_only_codec_controls"] = decoder.selftest()
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
